//! AWS CodeCommit backend — port of the deployed moto fork's codecommit engine.
//!
//! JSON protocol (`X-Amz-Target: CodeCommit_20150413.<Op>` on
//! `application/x-amz-json-1.1`). Both moto 5.1.23 and the deployed fork carry
//! identical `moto/codecommit` sources (`models.py`/`responses.py`), and every
//! one of the 79 botocore operations has a routed handler there — this port
//! mirrors that surface handler for handler, quirks included:
//!
//! - `CreateCommit` keeps the client's base64 `fileContent` string *as* the
//!   stored bytes (the sidecar-era double-encoding quirk), while `PutFile`
//!   base64-decodes first and stores raw bytes; `GetFile` always re-encodes
//!   on the wire, which is exactly what the compat suite asserts for both
//!   write paths.
//! - A repo holds one flat, un-versioned file map (`repo.files`), so
//!   `GetFolder` reflects files added on any branch and the merge stubs are
//!   enough for the suite.
//! - `GetBlob` always answers empty content; merge/conflict ops return
//!   freshly randomized 40-hex ids; `ListRepositories` always sorts by name.
//!
//! State is per-(account, region), mirroring moto's `CodeCommitBackend` keyed
//! by its `BackendDict`. Contract: tests/compatibility/test_codecommit_compat.py.

use crate::proto::{json_error, now_iso, parse_json_body};
use crate::store::{self, ScopeKey};
use crate::{Request, Response};
use base64::engine::general_purpose::STANDARD as B64;
use base64::Engine as _;
use parking_lot::RwLock;
use serde_json::{json, Map, Value};
use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;
use uuid::Uuid;

// ---------------------------------------------------------------------------
// State
// ---------------------------------------------------------------------------

type Stores = Arc<RwLock<HashMap<ScopeKey, CodeCommitStore>>>;

fn stores() -> Stores {
    store::slot::<CodeCommitStore>("codecommit")
}

/// Per-(account, region) state, mirroring `CodeCommitBackend`.
#[derive(Default)]
struct CodeCommitStore {
    /// repository name -> repository
    repositories: BTreeMap<String, Repository>,
    /// pull request id -> pull request
    pull_requests: BTreeMap<String, PullRequest>,
    /// comment id -> comment
    comments: BTreeMap<String, Comment>,
    /// template name -> approval rule template
    approval_rule_templates: BTreeMap<String, ApprovalRuleTemplate>,
    pull_request_counter: u64,
}

fn with_store(req: &Request, f: impl FnOnce(&mut CodeCommitStore) -> Response) -> Response {
    let slot = stores();
    let mut map = slot.write();
    let st = map.entry((req.account, req.region.clone())).or_default();
    f(st)
}

/// A repository: metadata plus the flat branch/commit/file stores.
struct Repository {
    metadata: Map<String, Value>,
    /// branch name -> commit id (moto's dict keyed by branch name)
    branches: BTreeMap<String, String>,
    /// commit id -> commit
    commits: BTreeMap<String, Commit>,
    /// file path -> file entry (repo-wide, un-versioned — moto's flat map)
    files: BTreeMap<String, FileEntry>,
    tags: BTreeMap<String, String>,
    triggers: Vec<Value>,
}

struct Commit {
    commit_id: String,
    tree_id: String,
    parents: Vec<String>,
    message: String,
    author_name: String,
    author_email: String,
    committer_name: String,
    committer_email: String,
    author_date: String,
    committer_date: String,
}

struct FileEntry {
    file_path: String,
    content: Vec<u8>,
    file_mode: String,
    blob_id: String,
}

struct Comment {
    comment_id: String,
    content: String,
    author_arn: String,
    in_reply_to: Option<String>,
    creation_date: String,
    last_modified_date: String,
    deleted: bool,
    caller_reactions: Vec<String>,
    reaction_counts: BTreeMap<String, i64>,
}

struct ApprovalRule {
    approval_rule_id: String,
    approval_rule_name: String,
    approval_rule_content: String,
    creation_date: String,
    last_modified_date: String,
    rule_content_sha256: String,
}

struct PullRequest {
    pull_request_id: String,
    title: String,
    description: String,
    author_arn: String,
    pull_request_status: String,
    creation_date: String,
    last_activity_date: String,
    /// rule name -> rule
    approval_rules: BTreeMap<String, ApprovalRule>,
    pull_request_targets: Vec<Value>,
    events: Vec<Value>,
    /// ids into the store's comment table (moto keeps the shared objects)
    comment_ids: Vec<String>,
    /// [{approvalState, userArn}] in insertion order
    approval_states: Vec<Value>,
    override_status: String,
    override_user: String,
}

struct ApprovalRuleTemplate {
    approval_rule_template_id: String,
    approval_rule_template_name: String,
    approval_rule_template_content: String,
    approval_rule_template_description: String,
    creation_date: String,
    last_modified_date: String,
    rule_content_sha256: String,
    associated_repositories: Vec<String>,
}

// ---------------------------------------------------------------------------
// Records
// ---------------------------------------------------------------------------

impl Repository {
    fn new(account: u64, region: &str, name: &str, description: &str) -> Self {
        // moto: kmsKeyId only appears when the request carried one, and the
        // default branch is pre-set to "main" with the initial commit.
        let metadata = repo_metadata(account, region, name, description);
        let mut repo = Repository {
            metadata,
            branches: BTreeMap::new(),
            commits: BTreeMap::new(),
            files: BTreeMap::new(),
            tags: BTreeMap::new(),
            triggers: Vec::new(),
        };
        // Create initial commit and main branch
        let initial_commit_id = hex32();
        let initial = Commit::new(
            &initial_commit_id,
            &hex32(),
            Vec::new(),
            "Initial commit",
            "CodeCommit",
            "codecommit@amazon.com",
        );
        repo.commits.insert(initial_commit_id.clone(), initial);
        repo.branches.insert("main".to_string(), initial_commit_id);
        repo
    }

    /// moto stores `repository_metadata` as a mutable dict; the accessors
    /// below read it straight back to the wire.
    fn metadata_json(&self) -> Value {
        Value::Object(self.metadata.clone())
    }
}

/// Build the `repositoryMetadata` dict exactly as moto's `CodeCommit.__init__`.
fn repo_metadata(account: u64, region: &str, name: &str, description: &str) -> Map<String, Value> {
    let now = now_iso();
    let mut m = Map::new();
    m.insert("repositoryName".into(), json!(name));
    m.insert(
        "cloneUrlSsh".into(),
        json!(format!(
            "ssh://git-codecommit.{region}.amazonaws.com/v1/repos/{name}"
        )),
    );
    m.insert(
        "cloneUrlHttp".into(),
        json!(format!(
            "https://git-codecommit.{region}.amazonaws.com/v1/repos/{name}"
        )),
    );
    m.insert("creationDate".into(), json!(now));
    m.insert("lastModifiedDate".into(), json!(now));
    m.insert("repositoryDescription".into(), json!(description));
    m.insert("repositoryId".into(), json!(Uuid::new_v4().to_string()));
    m.insert(
        "Arn".into(),
        json!(format!("arn:aws:codecommit:{region}:{account}:{name}")),
    );
    m.insert("accountId".into(), json!(account.to_string()));
    m.insert("defaultBranch".into(), json!("main"));
    m
}

impl Commit {
    /// All call sites pass already-defaulted author/email values.
    fn new(
        commit_id: &str,
        tree_id: &str,
        parents: Vec<String>,
        message: &str,
        author: &str,
        email: &str,
    ) -> Self {
        Commit {
            commit_id: commit_id.to_string(),
            tree_id: tree_id.to_string(),
            parents,
            message: message.to_string(),
            author_name: author.to_string(),
            author_email: email.to_string(),
            committer_name: author.to_string(),
            committer_email: email.to_string(),
            author_date: now_iso(),
            committer_date: now_iso(),
        }
    }

    fn to_dict(&self) -> Value {
        json!({
            "commitId": self.commit_id,
            "treeId": self.tree_id,
            "parents": self.parents,
            "message": self.message,
            "author": {
                "name": self.author_name,
                "email": self.author_email,
                "date": self.author_date,
            },
            "committer": {
                "name": self.committer_name,
                "email": self.committer_email,
                "date": self.committer_date,
            },
            "additionalData": "",
        })
    }
}

impl FileEntry {
    fn new(file_path: &str, content: Vec<u8>, file_mode: &str) -> Self {
        FileEntry {
            blob_id: sha1_hex(&content),
            file_path: file_path.to_string(),
            content,
            file_mode: file_mode.to_string(),
        }
    }

    /// moto's `FileEntry.to_dict`: path/blob/mode triple.
    fn to_dict(&self) -> Value {
        json!({
            "absolutePath": self.file_path,
            "blobId": self.blob_id,
            "fileMode": self.file_mode,
        })
    }
}

impl Comment {
    fn new(comment_id: &str, content: &str, author_arn: &str, in_reply_to: Option<&str>) -> Self {
        let now = now_iso();
        Comment {
            comment_id: comment_id.to_string(),
            content: content.to_string(),
            author_arn: author_arn.to_string(),
            in_reply_to: in_reply_to.map(String::from),
            creation_date: now.clone(),
            last_modified_date: now,
            deleted: false,
            caller_reactions: Vec::new(),
            reaction_counts: BTreeMap::new(),
        }
    }

    fn to_dict(&self) -> Value {
        let counts: Map<String, Value> = self
            .reaction_counts
            .iter()
            .map(|(k, v)| (k.clone(), json!(v)))
            .collect();
        let mut out = json!({
            "commentId": self.comment_id,
            "content": self.content,
            "authorArn": self.author_arn,
            "creationDate": self.creation_date,
            "lastModifiedDate": self.last_modified_date,
            "deleted": self.deleted,
            "callerReactions": self.caller_reactions,
            "reactionCounts": counts,
        });
        if let Some(reply) = &self.in_reply_to {
            out["inReplyTo"] = json!(reply);
        }
        out
    }
}

impl ApprovalRule {
    fn new(rule_id: &str, name: &str, content: &str) -> Self {
        let now = now_iso();
        ApprovalRule {
            approval_rule_id: rule_id.to_string(),
            approval_rule_name: name.to_string(),
            approval_rule_content: content.to_string(),
            creation_date: now.clone(),
            last_modified_date: now,
            rule_content_sha256: sha256_hex(content),
        }
    }

    fn to_dict(&self) -> Value {
        json!({
            "approvalRuleId": self.approval_rule_id,
            "approvalRuleName": self.approval_rule_name,
            "approvalRuleContent": self.approval_rule_content,
            "ruleContentSha256": self.rule_content_sha256,
            "creationDate": self.creation_date,
            "lastModifiedDate": self.last_modified_date,
            "lastModifiedUser": "",
        })
    }
}

impl PullRequest {
    fn new(
        pull_request_id: &str,
        title: &str,
        description: &str,
        targets: &[Value],
        author_arn: &str,
    ) -> Self {
        let now = now_iso();
        let mut events = Vec::new();
        let mut pull_request_targets = Vec::new();
        for target in targets {
            pull_request_targets.push(json!({
                "repositoryName": target.get("repositoryName").and_then(Value::as_str).unwrap_or(""),
                "sourceReference": target.get("sourceReference").and_then(Value::as_str).unwrap_or(""),
                "destinationReference": target
                    .get("destinationReference")
                    .and_then(Value::as_str)
                    .filter(|d| !d.is_empty())
                    .unwrap_or("main"),
                "destinationCommit": hex32(),
                "sourceCommit": hex32(),
                "mergeBase": hex32(),
                "mergeMetadata": {"isMerged": false},
            }));
        }
        events.push(json!({
            "pullRequestId": pull_request_id,
            "eventDate": now,
            "pullRequestEventType": "PULL_REQUEST_CREATED",
            "actorArn": author_arn,
        }));
        PullRequest {
            pull_request_id: pull_request_id.to_string(),
            title: title.to_string(),
            description: description.to_string(),
            author_arn: author_arn.to_string(),
            pull_request_status: "OPEN".to_string(),
            creation_date: now.clone(),
            last_activity_date: now,
            approval_rules: BTreeMap::new(),
            pull_request_targets,
            events,
            comment_ids: Vec::new(),
            approval_states: Vec::new(),
            override_status: "REVOKE".to_string(),
            override_user: String::new(),
        }
    }

    fn to_dict(&self) -> Value {
        let rules: Vec<Value> = self.approval_rules.values().map(|r| r.to_dict()).collect();
        json!({
            "pullRequestId": self.pull_request_id,
            "title": self.title,
            "description": self.description,
            "pullRequestStatus": self.pull_request_status,
            "creationDate": self.creation_date,
            "lastActivityDate": self.last_activity_date,
            "authorArn": self.author_arn,
            "pullRequestTargets": self.pull_request_targets,
            "approvalRules": rules,
        })
    }
}

impl ApprovalRuleTemplate {
    fn new(name: &str, content: &str, description: &str) -> Self {
        let now = now_iso();
        ApprovalRuleTemplate {
            approval_rule_template_id: Uuid::new_v4().to_string(),
            approval_rule_template_name: name.to_string(),
            approval_rule_template_content: content.to_string(),
            approval_rule_template_description: description.to_string(),
            creation_date: now.clone(),
            last_modified_date: now,
            rule_content_sha256: sha256_hex(content),
            associated_repositories: Vec::new(),
        }
    }

    fn to_dict(&self) -> Value {
        json!({
            "approvalRuleTemplateId": self.approval_rule_template_id,
            "approvalRuleTemplateName": self.approval_rule_template_name,
            "approvalRuleTemplateContent": self.approval_rule_template_content,
            "approvalRuleTemplateDescription": self.approval_rule_template_description,
            "ruleContentSha256": self.rule_content_sha256,
            "creationDate": self.creation_date,
            "lastModifiedDate": self.last_modified_date,
            "lastModifiedUser": "",
        })
    }
}

// ---------------------------------------------------------------------------
// dispatch
// ---------------------------------------------------------------------------

pub fn dispatch(req: &Request) -> Option<Response> {
    if req.service != "codecommit" {
        return None;
    }
    Some(with_store(req, |st| route(req, st)))
}

fn route(req: &Request, st: &mut CodeCommitStore) -> Response {
    let body = parse_json_body(&req.body);
    match req.operation.as_str() {
        // --- repositories ---------------------------------------------------
        "CreateRepository" => create_repository(req, &body, st),
        "GetRepository" => get_repository(req, &body, st),
        "DeleteRepository" => delete_repository(req, &body, st),
        "BatchGetRepositories" => batch_get_repositories(&body, st),
        "ListRepositories" => list_repositories(&body, st),
        "UpdateRepositoryDescription" => update_repository_description(&body, st),
        "UpdateRepositoryName" => update_repository_name(&body, st),
        "UpdateRepositoryEncryptionKey" => update_repository_encryption_key(&body, st),
        // --- branches ---------------------------------------------------------
        "CreateBranch" => create_branch(&body, st),
        "GetBranch" => get_branch(&body, st),
        "DeleteBranch" => delete_branch(&body, st),
        "ListBranches" => list_branches(&body, st),
        "UpdateDefaultBranch" => update_default_branch(&body, st),
        // --- commits ----------------------------------------------------------
        "CreateCommit" => create_commit(&body, st),
        "GetCommit" => get_commit(&body, st),
        "BatchGetCommits" => batch_get_commits(&body, st),
        // --- files --------------------------------------------------------------
        "GetFile" => get_file(&body, st),
        "GetFolder" => get_folder(&body, st),
        "GetBlob" => get_blob(&body, st),
        "PutFile" => put_file(&body, st),
        "DeleteFile" => delete_file(&body, st),
        "GetDifferences" => get_differences(&body, st),
        "ListFileCommitHistory" => list_file_commit_history(&body, st),
        // --- pull requests ------------------------------------------------------
        "CreatePullRequest" => create_pull_request(req, &body, st),
        "GetPullRequest" => get_pull_request(&body, st),
        "ListPullRequests" => list_pull_requests(&body, st),
        "UpdatePullRequestTitle" => update_pull_request_title(&body, st),
        "UpdatePullRequestDescription" => update_pull_request_description(&body, st),
        "UpdatePullRequestStatus" => update_pull_request_status(&body, st),
        "DescribePullRequestEvents" => describe_pull_request_events(&body, st),
        // --- merges ---------------------------------------------------------------
        "MergeBranchesByFastForward" => merge_branches_stub(&body, st),
        "MergeBranchesBySquash" => merge_branches_stub(&body, st),
        "MergeBranchesByThreeWay" => merge_branches_stub(&body, st),
        "CreateUnreferencedMergeCommit" => create_unreferenced_merge_commit(&body, st),
        "MergePullRequestByFastForward"
        | "MergePullRequestBySquash"
        | "MergePullRequestByThreeWay" => merge_pull_request(&body, st),
        "GetMergeOptions" => get_merge_options(&body, st),
        "GetMergeCommit" => get_merge_commit(&body, st),
        "GetMergeConflicts" => get_merge_conflicts(&body, st),
        "BatchDescribeMergeConflicts" => batch_describe_merge_conflicts(&body, st),
        "DescribeMergeConflicts" => describe_merge_conflicts(&body, st),
        // --- comments ---------------------------------------------------------------
        "PostCommentForComparedCommit" => post_comment_for_compared_commit(req, &body, st),
        "PostCommentForPullRequest" => post_comment_for_pull_request(req, &body, st),
        "PostCommentReply" => post_comment_reply(req, &body, st),
        "GetComment" => get_comment(&body, st),
        "UpdateComment" => update_comment(&body, st),
        "DeleteCommentContent" => delete_comment_content(&body, st),
        "GetCommentsForComparedCommit" => get_comments_for_compared_commit(&body, st),
        "GetCommentsForPullRequest" => get_comments_for_pull_request(&body, st),
        "PutCommentReaction" => put_comment_reaction(&body, st),
        "GetCommentReactions" => get_comment_reactions(req, &body, st),
        // --- pull request approval rules ----------------------------------------------
        "CreatePullRequestApprovalRule" => create_pull_request_approval_rule(&body, st),
        "DeletePullRequestApprovalRule" => delete_pull_request_approval_rule(&body, st),
        "UpdatePullRequestApprovalRuleContent" => {
            update_pull_request_approval_rule_content(&body, st)
        }
        "UpdatePullRequestApprovalState" => update_pull_request_approval_state(req, &body, st),
        "GetPullRequestApprovalStates" => get_pull_request_approval_states(&body, st),
        "EvaluatePullRequestApprovalRules" => evaluate_pull_request_approval_rules(&body, st),
        "OverridePullRequestApprovalRules" => override_pull_request_approval_rules(req, &body, st),
        "GetPullRequestOverrideState" => get_pull_request_override_state(&body, st),
        // --- approval rule templates ------------------------------------------------------
        "CreateApprovalRuleTemplate" => create_approval_rule_template(&body, st),
        "GetApprovalRuleTemplate" => get_approval_rule_template(&body, st),
        "DeleteApprovalRuleTemplate" => delete_approval_rule_template(&body, st),
        "ListApprovalRuleTemplates" => list_approval_rule_templates(st),
        "UpdateApprovalRuleTemplateName" => update_approval_rule_template_name(&body, st),
        "UpdateApprovalRuleTemplateContent" => update_approval_rule_template_content(&body, st),
        "UpdateApprovalRuleTemplateDescription" => {
            update_approval_rule_template_description(&body, st)
        }
        "AssociateApprovalRuleTemplateWithRepository" => {
            associate_approval_rule_template_with_repository(&body, st)
        }
        "DisassociateApprovalRuleTemplateFromRepository" => {
            disassociate_approval_rule_template_from_repository(&body, st)
        }
        "BatchAssociateApprovalRuleTemplateWithRepositories" => {
            batch_associate_approval_rule_template_with_repositories(&body, st)
        }
        "BatchDisassociateApprovalRuleTemplateFromRepositories" => {
            batch_disassociate_approval_rule_template_from_repositories(&body, st)
        }
        "ListAssociatedApprovalRuleTemplatesForRepository" => {
            list_associated_approval_rule_templates_for_repository(&body, st)
        }
        "ListRepositoriesForApprovalRuleTemplate" => {
            list_repositories_for_approval_rule_template(&body, st)
        }
        // --- triggers ------------------------------------------------------------------------
        "GetRepositoryTriggers" => get_repository_triggers(&body, st),
        "PutRepositoryTriggers" => put_repository_triggers(&body, st),
        "TestRepositoryTriggers" => test_repository_triggers(&body, st),
        // --- tags -----------------------------------------------------------------------------
        "TagResource" => tag_resource(&body, st),
        "UntagResource" => untag_resource(&body, st),
        "ListTagsForResource" => list_tags_for_resource(&body, st),
        other => json_error(
            501,
            "NotImplemented",
            &format!("The {other} action has not been implemented"),
        ),
    }
}

// ---------------------------------------------------------------------------
// Repo lookups (moto's `_get_repo`), name validation, small helpers
// ---------------------------------------------------------------------------

/// moto's `_is_repository_name_valid`: a name is valid when the `[\w.-]+`
/// regex consumes every character (an empty name is valid too — mirror).
fn is_repository_name_valid(name: &str) -> bool {
    name.is_empty()
        || name
            .chars()
            .all(|c| c.is_alphanumeric() || c == '_' || c == '.' || c == '-')
}

/// moto's `_get_repo` — raises `RepositoryDoesNotExistException` when absent.
fn get_repo<'a>(st: &'a mut CodeCommitStore, name: &str) -> Result<&'a mut Repository, Response> {
    match st.repositories.get_mut(name) {
        Some(repo) => Ok(repo),
        None => Err(repo_does_not_exist(name)),
    }
}

fn caller_arn(account: u64) -> String {
    format!("arn:aws:iam::{account}:root")
}

/// moto's commit id: `str(uuid4()).replace("-", "")` — 32 lowercase hex.
fn hex32() -> String {
    Uuid::new_v4().simple().to_string()
}

fn sha1_hex(data: &[u8]) -> String {
    use sha1::Digest;
    let mut hasher = sha1::Sha1::new();
    hasher.update(data);
    hex::encode(hasher.finalize())
}

fn sha256_hex(data: &str) -> String {
    use sha2::Digest;
    hex::encode(sha2::Sha256::digest(data.as_bytes()))
}

// ---------------------------------------------------------------------------
// Response helpers and error constructors (moto's JsonRESTError twins)
// ---------------------------------------------------------------------------

fn ok(body: Value) -> Response {
    Response::json(200, serde_json::to_vec(&body).unwrap_or_default())
}

fn ok_empty() -> Response {
    ok(json!({}))
}

fn err(code: &str, message: String) -> Response {
    json_error(400, code, &message)
}

fn repo_does_not_exist(name: &str) -> Response {
    err(
        "RepositoryDoesNotExistException",
        format!("{name} does not exist"),
    )
}

fn repo_name_exists(name: &str) -> Response {
    err(
        "RepositoryNameExistsException",
        format!("Repository named {name} already exists"),
    )
}

fn branch_does_not_exist(name: &str) -> Response {
    err(
        "BranchDoesNotExistException",
        format!("Branch named {name} does not exist"),
    )
}

fn branch_name_exists(name: &str) -> Response {
    err(
        "BranchNameExistsException",
        format!("Branch named {name} already exists"),
    )
}

fn pull_request_does_not_exist(id: &str) -> Response {
    err(
        "PullRequestDoesNotExistException",
        format!("Pull request {id} does not exist"),
    )
}

fn comment_does_not_exist(id: &str) -> Response {
    err(
        "CommentDoesNotExistException",
        format!("Comment {id} does not exist"),
    )
}

fn template_does_not_exist(name: &str) -> Response {
    err(
        "ApprovalRuleTemplateDoesNotExistException",
        format!("Approval rule template named {name} does not exist"),
    )
}

fn template_name_already_exists(name: &str) -> Response {
    err(
        "ApprovalRuleTemplateNameAlreadyExistsException",
        format!("Approval rule template named {name} already exists"),
    )
}

fn file_does_not_exist(path: &str) -> Response {
    err(
        "FileDoesNotExistException",
        format!("File {path} does not exist"),
    )
}

fn pr_already_closed(id: &str) -> Response {
    err(
        "PullRequestAlreadyClosedException",
        format!("Pull request {id} is already closed"),
    )
}

fn approval_rule_does_not_exist(name: &str) -> Response {
    err(
        "ApprovalRuleDoesNotExistException",
        format!("Approval rule named {name} does not exist"),
    )
}

fn default_branch_cannot_be_deleted() -> Response {
    err(
        "DefaultBranchCannotBeDeletedException",
        "The default branch cannot be deleted.".to_string(),
    )
}

// ---------------------------------------------------------------------------
// Small body-param readers
// ---------------------------------------------------------------------------

fn s<'a>(body: &'a Value, key: &str) -> &'a str {
    body.get(key).and_then(Value::as_str).unwrap_or("")
}

fn opt_s<'a>(body: &'a Value, key: &str) -> Option<&'a str> {
    body.get(key).and_then(Value::as_str)
}

fn str_list(body: &Value, key: &str) -> Vec<String> {
    body.get(key)
        .and_then(Value::as_array)
        .map(|list| {
            list.iter()
                .filter_map(Value::as_str)
                .map(String::from)
                .collect()
        })
        .unwrap_or_default()
}

// ---------------------------------------------------------------------------
// Repository operations
// ---------------------------------------------------------------------------

fn create_repository(req: &Request, body: &Value, st: &mut CodeCommitStore) -> Response {
    let name = s(body, "repositoryName");
    if !is_repository_name_valid(name) {
        return invalid_repository_name();
    }
    if st.repositories.contains_key(name) {
        return repo_name_exists(name);
    }
    let description = s(body, "repositoryDescription");
    let mut repo = Repository::new(req.account, &req.region, name, description);
    if let Some(kms) = opt_s(body, "kmsKeyId") {
        repo.metadata.insert("kmsKeyId".into(), json!(kms));
    }
    if let Some(tags) = body.get("tags").and_then(Value::as_object) {
        for (k, v) in tags {
            if let Some(v) = v.as_str() {
                repo.tags.insert(k.clone(), v.to_string());
            }
        }
    }
    st.repositories.insert(name.to_string(), repo);
    let metadata_json = st.repositories[name].metadata_json();
    ok(json!({ "repositoryMetadata": metadata_json }))
}

fn get_repository(_req: &Request, body: &Value, st: &mut CodeCommitStore) -> Response {
    let name = s(body, "repositoryName");
    if !is_repository_name_valid(name) {
        return invalid_repository_name();
    }
    match st.repositories.get(name) {
        Some(repo) => ok(json!({ "repositoryMetadata": repo.metadata_json() })),
        None => repo_does_not_exist(name),
    }
}

fn delete_repository(_req: &Request, body: &Value, st: &mut CodeCommitStore) -> Response {
    let name = s(body, "repositoryName");
    if !is_repository_name_valid(name) {
        return invalid_repository_name();
    }
    match st.repositories.remove(name) {
        Some(repo) => match repo.metadata.get("repositoryId").cloned() {
            Some(id) => ok(json!({ "repositoryId": id })),
            None => ok_empty(),
        },
        None => ok_empty(),
    }
}

fn batch_get_repositories(body: &Value, st: &mut CodeCommitStore) -> Response {
    let names = str_list(body, "repositoryNames");
    let mut repositories = Vec::new();
    let mut not_found = Vec::new();
    for name in names {
        match st.repositories.get(&name) {
            Some(repo) => repositories.push(repo.metadata_json()),
            None => not_found.push(json!(name)),
        }
    }
    ok(json!({
        "repositories": repositories,
        "repositoriesNotFound": not_found,
    }))
}

fn list_repositories(body: &Value, st: &mut CodeCommitStore) -> Response {
    let order = body
        .get("order")
        .and_then(Value::as_str)
        .unwrap_or("ascending");
    let descending = order == "descending";
    let mut repos: Vec<Value> = st
        .repositories
        .values()
        .map(|repo| {
            json!({
                "repositoryName": repo.metadata.get("repositoryName").cloned().unwrap_or(json!("")),
                "repositoryId": repo.metadata.get("repositoryId").cloned().unwrap_or(json!("")),
            })
        })
        .collect();
    repos.sort_by(|a, b| {
        let an = a["repositoryName"].as_str().unwrap_or("");
        let bn = b["repositoryName"].as_str().unwrap_or("");
        if descending {
            bn.cmp(an)
        } else {
            an.cmp(bn)
        }
    });
    ok(json!({ "repositories": repos }))
}

fn update_repository_description(body: &Value, st: &mut CodeCommitStore) -> Response {
    let name = s(body, "repositoryName");
    let description = s(body, "repositoryDescription");
    let Ok(repo) = get_repo(st, name) else {
        return repo_does_not_exist(name);
    };
    repo.metadata
        .insert("repositoryDescription".into(), json!(description));
    repo.metadata
        .insert("lastModifiedDate".into(), json!(now_iso()));
    ok_empty()
}

fn update_repository_name(body: &Value, st: &mut CodeCommitStore) -> Response {
    let old_name = s(body, "oldName");
    let new_name = s(body, "newName");
    if !st.repositories.contains_key(old_name) {
        return repo_does_not_exist(old_name);
    }
    if st.repositories.contains_key(new_name) {
        return repo_name_exists(new_name);
    }
    let mut repo = st.repositories.remove(old_name).expect("checked");
    repo.metadata
        .insert("repositoryName".into(), json!(new_name));
    repo.metadata
        .insert("lastModifiedDate".into(), json!(now_iso()));
    st.repositories.insert(new_name.to_string(), repo);
    ok_empty()
}

fn update_repository_encryption_key(body: &Value, st: &mut CodeCommitStore) -> Response {
    let name = s(body, "repositoryName");
    let kms = s(body, "kmsKeyId");
    let Ok(repo) = get_repo(st, name) else {
        return repo_does_not_exist(name);
    };
    repo.metadata.insert("kmsKeyId".into(), json!(kms));
    repo.metadata
        .insert("lastModifiedDate".into(), json!(now_iso()));
    // moto reads originalKmsKeyId *after* the write, so it equals the new id.
    let repository_id = repo
        .metadata
        .get("repositoryId")
        .cloned()
        .unwrap_or(json!(""));
    ok(json!({
        "repositoryId": repository_id,
        "kmsKeyId": kms,
        "originalKmsKeyId": kms,
    }))
}

fn invalid_repository_name() -> Response {
    err(
        "InvalidRepositoryNameException",
        "The repository name is not valid. Repository names can be any valid \
         combination of letters, numbers, periods, underscores, and dashes \
         between 1 and 100 characters in length. Names are case sensitive. \
         For more information, see Limits in the AWS CodeCommit User Guide. "
            .to_string(),
    )
}

// ---------------------------------------------------------------------------
// Branch operations
// ---------------------------------------------------------------------------

fn create_branch(body: &Value, st: &mut CodeCommitStore) -> Response {
    let name = s(body, "repositoryName");
    let branch = s(body, "branchName");
    let commit_id = s(body, "commitId");
    let Ok(repo) = get_repo(st, name) else {
        return repo_does_not_exist(name);
    };
    if repo.branches.contains_key(branch) {
        return branch_name_exists(branch);
    }
    repo.branches
        .insert(branch.to_string(), commit_id.to_string());
    ok_empty()
}

fn get_branch(body: &Value, st: &mut CodeCommitStore) -> Response {
    let name = s(body, "repositoryName");
    let branch = s(body, "branchName");
    let Ok(repo) = get_repo(st, name) else {
        return repo_does_not_exist(name);
    };
    match repo.branches.get(branch) {
        Some(commit_id) => ok(json!({
            "branch": {"branchName": branch, "commitId": commit_id},
        })),
        None => branch_does_not_exist(branch),
    }
}

fn delete_branch(body: &Value, st: &mut CodeCommitStore) -> Response {
    let name = s(body, "repositoryName");
    let branch = s(body, "branchName");
    let Ok(repo) = get_repo(st, name) else {
        return repo_does_not_exist(name);
    };
    let Some(commit_id) = repo.branches.get(branch).cloned() else {
        return branch_does_not_exist(branch);
    };
    let default_branch = repo
        .metadata
        .get("defaultBranch")
        .and_then(Value::as_str)
        .unwrap_or("main");
    if branch == default_branch {
        return default_branch_cannot_be_deleted();
    }
    repo.branches.remove(branch);
    ok(json!({
        "deletedBranch": {"branchName": branch, "commitId": commit_id},
    }))
}

fn list_branches(body: &Value, st: &mut CodeCommitStore) -> Response {
    let name = s(body, "repositoryName");
    let Ok(repo) = get_repo(st, name) else {
        return repo_does_not_exist(name);
    };
    let branches: Vec<Value> = repo.branches.keys().map(|b| json!(b)).collect();
    ok(json!({ "branches": branches }))
}

fn update_default_branch(body: &Value, st: &mut CodeCommitStore) -> Response {
    let name = s(body, "repositoryName");
    let default_branch = s(body, "defaultBranchName");
    let Ok(repo) = get_repo(st, name) else {
        return repo_does_not_exist(name);
    };
    if !repo.branches.contains_key(default_branch) {
        return branch_does_not_exist(default_branch);
    }
    repo.metadata
        .insert("defaultBranch".into(), json!(default_branch));
    ok_empty()
}

// ---------------------------------------------------------------------------
// Commit operations
// ---------------------------------------------------------------------------

/// moto's `CreateCommit` — note the fork keeps the client's base64
/// `fileContent` string as the stored bytes (putFiles) and creates the
/// branch when missing.
fn create_commit(body: &Value, st: &mut CodeCommitStore) -> Response {
    let name = s(body, "repositoryName");
    let branch = s(body, "branchName");
    let Ok(repo) = get_repo(st, name) else {
        return repo_does_not_exist(name);
    };

    let commit_id = hex32();
    let tree_id = hex32();
    let parents: Vec<String> = opt_s(body, "parentCommitId")
        .map(|p| vec![p.to_string()])
        .unwrap_or_default();
    let message = body
        .get("commitMessage")
        .and_then(Value::as_str)
        .unwrap_or("");
    let author_name = opt_s(body, "authorName")
        .filter(|a| !a.is_empty())
        .unwrap_or("Author");
    let email = opt_s(body, "email")
        .filter(|e| !e.is_empty())
        .unwrap_or("author@example.com");
    let commit = Commit::new(&commit_id, &tree_id, parents, message, author_name, email);
    repo.commits.insert(commit_id.clone(), commit);

    let mut files_added = Vec::new();
    let mut files_updated = Vec::new();
    let mut files_deleted = Vec::new();

    if let Some(put_files) = body.get("putFiles").and_then(Value::as_array) {
        for pf in put_files {
            let file_path = pf.get("filePath").and_then(Value::as_str).unwrap_or("");
            // blob members arrive as the base64 string — the fork stores
            // those bytes verbatim (never decodes them here).
            let content = pf
                .get("fileContent")
                .and_then(Value::as_str)
                .unwrap_or("")
                .as_bytes()
                .to_vec();
            let file_mode = pf
                .get("fileMode")
                .and_then(Value::as_str)
                .unwrap_or("NORMAL");
            let was_update = repo.files.contains_key(file_path);
            let entry = FileEntry::new(file_path, content, file_mode);
            repo.files.insert(file_path.to_string(), entry);
            if was_update {
                files_updated.push(repo.files[file_path].to_dict());
            } else {
                files_added.push(repo.files[file_path].to_dict());
            }
        }
    }

    if let Some(delete_files) = body.get("deleteFiles").and_then(Value::as_array) {
        for df in delete_files {
            let file_path = df.get("filePath").and_then(Value::as_str).unwrap_or("");
            if let Some(removed) = repo.files.remove(file_path) {
                // moto reports the removed entry with an empty blob id.
                files_deleted.push(json!({
                    "absolutePath": file_path,
                    "blobId": "",
                    "fileMode": "NORMAL",
                }));
                drop(removed);
            }
        }
    }

    // moto points the branch at the new commit, creating it when missing.
    repo.branches.insert(branch.to_string(), commit_id.clone());

    ok(json!({
        "commitId": commit_id,
        "treeId": tree_id,
        "filesAdded": files_added,
        "filesUpdated": files_updated,
        "filesDeleted": files_deleted,
    }))
}

fn get_commit(body: &Value, st: &mut CodeCommitStore) -> Response {
    let name = s(body, "repositoryName");
    let commit_id = s(body, "commitId");
    let Ok(repo) = get_repo(st, name) else {
        return repo_does_not_exist(name);
    };
    match repo.commits.get(commit_id) {
        Some(commit) => ok(json!({ "commit": commit.to_dict() })),
        // moto returns a stub commit for unknown ids rather than a 404.
        None => {
            let stub = Commit::new(
                commit_id,
                &hex32(),
                Vec::new(),
                "",
                "Unknown",
                "unknown@example.com",
            );
            ok(json!({ "commit": stub.to_dict() }))
        }
    }
}

fn batch_get_commits(body: &Value, st: &mut CodeCommitStore) -> Response {
    let name = s(body, "repositoryName");
    let commit_ids = str_list(body, "commitIds");
    let Ok(repo) = get_repo(st, name) else {
        return repo_does_not_exist(name);
    };
    let mut commits = Vec::new();
    let mut errors = Vec::new();
    for cid in commit_ids {
        match repo.commits.get(&cid) {
            Some(commit) => commits.push(commit.to_dict()),
            None => errors.push(json!(cid)),
        }
    }
    ok(json!({ "commits": commits, "errors": errors }))
}

// ---------------------------------------------------------------------------
// File / tree operations
// ---------------------------------------------------------------------------

/// moto's GetFile commit-id resolution: a branch name wins, any other value is
/// taken as a commit id, and empty falls back to the default branch.
fn resolve_commit_id(repo: &Repository, specifier: Option<&str>) -> String {
    if let Some(spec) = specifier {
        if !spec.is_empty() && repo.branches.contains_key(spec) {
            return repo.branches[spec].clone();
        }
        return spec.to_string();
    }
    let default_branch = repo
        .metadata
        .get("defaultBranch")
        .and_then(Value::as_str)
        .unwrap_or("main");
    repo.branches
        .get(default_branch)
        .cloned()
        .unwrap_or_default()
}

fn get_file(body: &Value, st: &mut CodeCommitStore) -> Response {
    let name = s(body, "repositoryName");
    let file_path = s(body, "filePath");
    let Ok(repo) = get_repo(st, name) else {
        return repo_does_not_exist(name);
    };
    let Some(entry) = repo.files.get(file_path) else {
        return file_does_not_exist(file_path);
    };
    let commit_id = resolve_commit_id(repo, opt_s(body, "commitSpecifier"));
    ok(json!({
        "commitId": commit_id,
        "blobId": entry.blob_id,
        "filePath": file_path,
        "fileMode": entry.file_mode,
        "fileSize": entry.content.len() as i64,
        "fileContent": B64.encode(&entry.content),
    }))
}

fn get_folder(body: &Value, st: &mut CodeCommitStore) -> Response {
    let name = s(body, "repositoryName");
    let folder_path = s(body, "folderPath");
    let Ok(repo) = get_repo(st, name) else {
        return repo_does_not_exist(name);
    };
    let commit_id = resolve_commit_id(repo, opt_s(body, "commitSpecifier"));

    // moto's prefix walk: files directly under the folder land in `files`,
    // deeper paths contribute their first path component to `subFolders`.
    let prefix = if folder_path != "/" {
        format!("{}/", folder_path.trim_end_matches('/'))
    } else {
        "/".to_string()
    };
    let mut files = Vec::new();
    let mut sub_folders = std::collections::BTreeSet::new();
    for (fp, entry) in &repo.files {
        if folder_path == "/" || fp.starts_with(&prefix) {
            let relative = if folder_path != "/" {
                &fp[prefix.len()..]
            } else {
                fp.as_str()
            };
            if let Some(first) = relative.split('/').next() {
                if relative.contains('/') {
                    // root entries stay bare; deeper folders keep the prefix
                    sub_folders.insert(if folder_path != "/" {
                        format!("{prefix}{first}")
                    } else {
                        first.to_string()
                    });
                } else {
                    files.push(entry.to_dict());
                }
            }
        }
    }
    let sub_folders: Vec<Value> = sub_folders
        .into_iter()
        .map(|sf| {
            let relative = sf.split('/').next_back().unwrap_or("").to_string();
            json!({
                "absolutePath": sf,
                "relativePath": relative,
                "treeId": hex32(),
            })
        })
        .collect();
    ok(json!({
        "commitId": commit_id,
        "folderPath": folder_path,
        "files": files,
        "subFolders": sub_folders,
        "symbolicLinks": [],
        "subModules": [],
        "treeId": hex32(),
    }))
}

fn get_blob(body: &Value, st: &mut CodeCommitStore) -> Response {
    let name = s(body, "repositoryName");
    let blob_id = s(body, "blobId");
    if let Err(resp) = get_repo(st, name) {
        return resp;
    }
    ok(json!({
        "blobId": blob_id,
        "content": B64.encode(b""),
    }))
}

/// moto's `PutFile` — fileContent arrives base64 and is decoded before
/// storage, unlike `CreateCommit`.
fn put_file(body: &Value, st: &mut CodeCommitStore) -> Response {
    let name = s(body, "repositoryName");
    let branch = s(body, "branchName");
    let file_path = s(body, "filePath");
    let Ok(repo) = get_repo(st, name) else {
        return repo_does_not_exist(name);
    };
    let raw = match opt_s(body, "fileContent") {
        Some(b64) if !b64.is_empty() => B64.decode(b64).unwrap_or_default(),
        _ => Vec::new(),
    };
    let file_mode = opt_s(body, "fileMode")
        .filter(|m| !m.is_empty())
        .unwrap_or("NORMAL");
    let entry = FileEntry::new(file_path, raw, file_mode);
    let blob_id = entry.blob_id.clone();
    repo.files.insert(file_path.to_string(), entry);

    let commit_id = hex32();
    let tree_id = hex32();
    let parents: Vec<String> = opt_s(body, "parentCommitId")
        .map(|p| vec![p.to_string()])
        .unwrap_or_default();
    let message = body
        .get("commitMessage")
        .and_then(Value::as_str)
        .filter(|m| !m.is_empty())
        .unwrap_or(&format!("Add {file_path}"))
        .to_string();
    let author_name = opt_s(body, "name")
        .filter(|a| !a.is_empty())
        .unwrap_or("Author");
    let email = opt_s(body, "email")
        .filter(|e| !e.is_empty())
        .unwrap_or("author@example.com");
    let commit = Commit::new(&commit_id, &tree_id, parents, &message, author_name, email);
    repo.commits.insert(commit_id.clone(), commit);
    if repo.branches.contains_key(branch) {
        repo.branches.insert(branch.to_string(), commit_id.clone());
    }

    ok(json!({
        "commitId": commit_id,
        "blobId": blob_id,
        "treeId": tree_id,
    }))
}
fn delete_file(body: &Value, st: &mut CodeCommitStore) -> Response {
    let name = s(body, "repositoryName");
    let branch = s(body, "branchName");
    let file_path = s(body, "filePath");
    let Ok(repo) = get_repo(st, name) else {
        return repo_does_not_exist(name);
    };
    let blob_id = repo
        .files
        .remove(file_path)
        .map(|entry| entry.blob_id)
        .unwrap_or_default();

    let commit_id = hex32();
    let tree_id = hex32();
    let parents: Vec<String> = opt_s(body, "parentCommitId")
        .filter(|p| !p.is_empty())
        .map(|p| vec![p.to_string()])
        .unwrap_or_default();
    let message = body
        .get("commitMessage")
        .and_then(Value::as_str)
        .filter(|m| !m.is_empty())
        .unwrap_or(&format!("Delete {file_path}"))
        .to_string();
    let author_name = opt_s(body, "name")
        .filter(|a| !a.is_empty())
        .unwrap_or("Author");
    let email = opt_s(body, "email")
        .filter(|e| !e.is_empty())
        .unwrap_or("author@example.com");
    let commit = Commit::new(&commit_id, &tree_id, parents, &message, author_name, email);
    repo.commits.insert(commit_id.clone(), commit);
    if repo.branches.contains_key(branch) {
        repo.branches.insert(branch.to_string(), commit_id.clone());
    }

    ok(json!({
        "commitId": commit_id,
        "blobId": blob_id,
        "treeId": tree_id,
        "filePath": file_path,
    }))
}

fn get_differences(body: &Value, st: &mut CodeCommitStore) -> Response {
    let name = s(body, "repositoryName");
    if let Err(resp) = get_repo(st, name) {
        return resp;
    }
    ok(json!({ "differences": [] }))
}

fn list_file_commit_history(body: &Value, st: &mut CodeCommitStore) -> Response {
    let name = s(body, "repositoryName");
    if let Err(resp) = get_repo(st, name) {
        return resp;
    }
    ok(json!({ "revisionDag": [] }))
}

// ---------------------------------------------------------------------------
// Pull request operations
// ---------------------------------------------------------------------------

/// Look up a pull request, mapping absence to moto's error.
fn get_pr<'a>(
    st: &'a mut CodeCommitStore,
    pull_request_id: &str,
) -> Result<&'a mut PullRequest, Response> {
    match st.pull_requests.get_mut(pull_request_id) {
        Some(pr) => Ok(pr),
        None => Err(pull_request_does_not_exist(pull_request_id)),
    }
}

fn create_pull_request(req: &Request, body: &Value, st: &mut CodeCommitStore) -> Response {
    let title = s(body, "title");
    let description = body
        .get("description")
        .and_then(Value::as_str)
        .unwrap_or("")
        .to_string();
    let targets = body
        .get("targets")
        .and_then(Value::as_array)
        .cloned()
        .unwrap_or_default();
    st.pull_request_counter += 1;
    let pr_id = st.pull_request_counter.to_string();
    let pr = PullRequest::new(
        &pr_id,
        title,
        &description,
        &targets,
        &caller_arn(req.account),
    );
    st.pull_requests.insert(pr_id.clone(), pr);
    ok(json!({ "pullRequest": st.pull_requests[&pr_id].to_dict() }))
}

fn get_pull_request(body: &Value, st: &mut CodeCommitStore) -> Response {
    let id = s(body, "pullRequestId");
    match st.pull_requests.get(id) {
        Some(pr) => ok(json!({ "pullRequest": pr.to_dict() })),
        None => pull_request_does_not_exist(id),
    }
}

fn list_pull_requests(body: &Value, st: &mut CodeCommitStore) -> Response {
    let name = s(body, "repositoryName");
    let author_arn = opt_s(body, "authorArn")
        .filter(|a| !a.is_empty())
        .map(String::from);
    let status = opt_s(body, "pullRequestStatus")
        .filter(|s| !s.is_empty())
        .map(String::from);
    let Ok(_repo) = get_repo(st, name) else {
        return repo_does_not_exist(name);
    };
    let ids: Vec<Value> = st
        .pull_requests
        .values()
        .filter(|pr| {
            let author_ok = match &author_arn {
                Some(arn) => pr.author_arn == *arn,
                None => true,
            };
            let status_ok = match &status {
                Some(s) => pr.pull_request_status == *s,
                None => true,
            };
            let targets_repo = pr
                .pull_request_targets
                .iter()
                .any(|t| t.get("repositoryName").and_then(Value::as_str) == Some(name));
            author_ok && status_ok && targets_repo
        })
        .map(|pr| json!(pr.pull_request_id))
        .collect();
    ok(json!({ "pullRequestIds": ids }))
}

fn update_pull_request_title(body: &Value, st: &mut CodeCommitStore) -> Response {
    let id = s(body, "pullRequestId");
    let title = s(body, "title");
    let Ok(pr) = get_pr(st, id) else {
        return pull_request_does_not_exist(id);
    };
    pr.title = title.to_string();
    pr.last_activity_date = now_iso();
    ok(json!({ "pullRequest": pr.to_dict() }))
}

fn update_pull_request_description(body: &Value, st: &mut CodeCommitStore) -> Response {
    let id = s(body, "pullRequestId");
    let description = s(body, "description");
    let Ok(pr) = get_pr(st, id) else {
        return pull_request_does_not_exist(id);
    };
    pr.description = description.to_string();
    pr.last_activity_date = now_iso();
    ok(json!({ "pullRequest": pr.to_dict() }))
}

fn update_pull_request_status(body: &Value, st: &mut CodeCommitStore) -> Response {
    let id = s(body, "pullRequestId");
    let status = s(body, "pullRequestStatus");
    let Ok(pr) = get_pr(st, id) else {
        return pull_request_does_not_exist(id);
    };
    if pr.pull_request_status == "CLOSED" {
        return pr_already_closed(id);
    }
    pr.pull_request_status = status.to_string();
    pr.last_activity_date = now_iso();
    pr.events.push(json!({
        "pullRequestId": id,
        "eventDate": pr.last_activity_date,
        "pullRequestEventType": "PULL_REQUEST_STATUS_CHANGED",
        "pullRequestStatusChangedEventMetadata": {"pullRequestStatus": status},
    }));
    ok(json!({ "pullRequest": pr.to_dict() }))
}

fn describe_pull_request_events(body: &Value, st: &mut CodeCommitStore) -> Response {
    let id = s(body, "pullRequestId");
    let event_type = opt_s(body, "pullRequestEventType")
        .filter(|e| !e.is_empty())
        .map(String::from);
    let actor_arn = opt_s(body, "actorArn")
        .filter(|a| !a.is_empty())
        .map(String::from);
    let Ok(pr) = get_pr(st, id) else {
        return pull_request_does_not_exist(id);
    };
    let events: Vec<Value> = pr
        .events
        .iter()
        .filter(|e| {
            event_type
                .as_deref()
                .map(|want| e.get("pullRequestEventType").and_then(Value::as_str) == Some(want))
                .unwrap_or(true)
                && actor_arn
                    .as_deref()
                    .map(|arn| e.get("actorArn").and_then(Value::as_str) == Some(arn))
                    .unwrap_or(true)
        })
        .cloned()
        .collect();
    ok(json!({ "pullRequestEvents": events }))
}

// ---------------------------------------------------------------------------
// Merge operations (moto keeps stub semantics — fresh ids over the flat file
// map are what the compat suite pins down)
// ---------------------------------------------------------------------------

fn merge_branches_stub(body: &Value, st: &mut CodeCommitStore) -> Response {
    let name = s(body, "repositoryName");
    if let Err(resp) = get_repo(st, name) {
        return resp;
    }
    ok(json!({ "commitId": hex32(), "treeId": hex32() }))
}

fn create_unreferenced_merge_commit(body: &Value, st: &mut CodeCommitStore) -> Response {
    let name = s(body, "repositoryName");
    if let Err(resp) = get_repo(st, name) {
        return resp;
    }
    ok(json!({ "commitId": hex32(), "treeId": hex32() }))
}

fn merge_pull_request(body: &Value, st: &mut CodeCommitStore) -> Response {
    let id = s(body, "pullRequestId");
    let Ok(pr) = get_pr(st, id) else {
        return pull_request_does_not_exist(id);
    };
    if pr.pull_request_status == "CLOSED" {
        return pr_already_closed(id);
    }
    pr.pull_request_status = "CLOSED".to_string();
    pr.last_activity_date = now_iso();
    for target in &mut pr.pull_request_targets {
        target["mergeMetadata"] = json!({"isMerged": true, "mergeCommitId": hex32()});
    }
    ok(json!({ "pullRequest": pr.to_dict() }))
}

fn get_merge_options(body: &Value, st: &mut CodeCommitStore) -> Response {
    let name = s(body, "repositoryName");
    let source = s(body, "sourceCommitSpecifier");
    let destination = s(body, "destinationCommitSpecifier");
    if let Err(resp) = get_repo(st, name) {
        return resp;
    }
    ok(json!({
        "mergeOptions": ["FAST_FORWARD_MERGE", "SQUASH_MERGE", "THREE_WAY_MERGE"],
        "sourceCommitId": source,
        "destinationCommitId": destination,
        "baseCommitId": hex32(),
    }))
}

fn get_merge_commit(body: &Value, st: &mut CodeCommitStore) -> Response {
    let name = s(body, "repositoryName");
    let source = s(body, "sourceCommitSpecifier");
    let destination = s(body, "destinationCommitSpecifier");
    if let Err(resp) = get_repo(st, name) {
        return resp;
    }
    ok(json!({
        "sourceCommitId": source,
        "destinationCommitId": destination,
        "baseCommitId": hex32(),
        "mergedCommitId": hex32(),
    }))
}

fn get_merge_conflicts(body: &Value, st: &mut CodeCommitStore) -> Response {
    let name = s(body, "repositoryName");
    let source = s(body, "sourceCommitSpecifier");
    let destination = s(body, "destinationCommitSpecifier");
    if let Err(resp) = get_repo(st, name) {
        return resp;
    }
    ok(json!({
        "mergeable": true,
        "destinationCommitId": destination,
        "sourceCommitId": source,
        "baseCommitId": hex32(),
        "conflictMetadataList": [],
    }))
}

fn batch_describe_merge_conflicts(body: &Value, st: &mut CodeCommitStore) -> Response {
    let name = s(body, "repositoryName");
    let source = s(body, "sourceCommitSpecifier");
    let destination = s(body, "destinationCommitSpecifier");
    if let Err(resp) = get_repo(st, name) {
        return resp;
    }
    ok(json!({
        "conflicts": [],
        "destinationCommitId": destination,
        "sourceCommitId": source,
        "baseCommitId": hex32(),
        "errors": [],
    }))
}

fn describe_merge_conflicts(body: &Value, st: &mut CodeCommitStore) -> Response {
    let name = s(body, "repositoryName");
    let file_path = s(body, "filePath");
    let source = s(body, "sourceCommitSpecifier");
    let destination = s(body, "destinationCommitSpecifier");
    if let Err(resp) = get_repo(st, name) {
        return resp;
    }
    ok(json!({
        "conflictMetadata": {
            "filePath": file_path,
            "fileSizes": {"source": 0, "destination": 0, "base": 0},
            "fileModes": {"source": "NORMAL", "destination": "NORMAL", "base": "NORMAL"},
            "objectTypes": {"source": "FILE", "destination": "FILE", "base": "FILE"},
            "numberOfConflicts": 0,
            "isBinaryFile": {"source": false, "destination": false, "base": false},
            "contentConflict": false,
            "fileModeConflict": false,
            "objectTypeConflict": false,
            "mergeOperations": {"source": "A", "destination": "A"},
        },
        "mergeHunks": [],
        "destinationCommitId": destination,
        "sourceCommitId": source,
        "baseCommitId": hex32(),
    }))
}

// ---------------------------------------------------------------------------
// Comment operations
// ---------------------------------------------------------------------------

fn comment_resp(
    req: &Request,
    body: &Value,
    mut extra: Map<String, Value>,
    store: &mut CodeCommitStore,
) -> Response {
    // Common shape for the two Post* ops: {…extra, "comment": {...}}
    let id = Uuid::new_v4().to_string();
    let content = s(body, "content");
    let comment = Comment::new(&id, content, &caller_arn(req.account), None);
    store.comments.insert(id.clone(), comment);
    extra.insert("comment".into(), store.comments[&id].to_dict());
    ok(Value::Object(extra))
}

fn post_comment_for_compared_commit(
    req: &Request,
    body: &Value,
    store: &mut CodeCommitStore,
) -> Response {
    let name = s(body, "repositoryName");
    if let Err(resp) = get_repo(store, name) {
        return resp;
    }
    let mut fields = Map::new();
    fields.insert("repositoryName".into(), json!(name));
    fields.insert(
        "beforeCommitId".into(),
        json!(opt_s(body, "beforeCommitId").unwrap_or("")),
    );
    fields.insert("afterCommitId".into(), json!(s(body, "afterCommitId")));
    fields.insert("beforeBlobId".into(), json!(""));
    fields.insert("afterBlobId".into(), json!(""));
    fields.insert(
        "location".into(),
        body.get("location").cloned().unwrap_or(json!({})),
    );
    comment_resp(req, body, fields, store)
}

fn post_comment_for_pull_request(
    req: &Request,
    body: &Value,
    st: &mut CodeCommitStore,
) -> Response {
    let id = s(body, "pullRequestId");
    let name = s(body, "repositoryName");
    if let Err(resp) = get_pr(st, id) {
        return resp;
    }
    if let Err(resp) = get_repo(st, name) {
        return resp;
    }
    // moto keeps the shared comment object in the PR's comment list too.
    let comment_id = Uuid::new_v4().to_string();
    let comment = Comment::new(
        &comment_id,
        s(body, "content"),
        &caller_arn(req.account),
        None,
    );
    st.comments.insert(comment_id.clone(), comment);
    if let Some(pr) = st.pull_requests.get_mut(id) {
        pr.comment_ids.push(comment_id.clone());
    }
    let mut fields = Map::new();
    fields.insert("repositoryName".into(), json!(name));
    fields.insert("pullRequestId".into(), json!(id));
    fields.insert("beforeCommitId".into(), json!(s(body, "beforeCommitId")));
    fields.insert("afterCommitId".into(), json!(s(body, "afterCommitId")));
    fields.insert("beforeBlobId".into(), json!(""));
    fields.insert("afterBlobId".into(), json!(""));
    fields.insert(
        "location".into(),
        body.get("location").cloned().unwrap_or(json!({})),
    );
    let comment_json = st.comments[&comment_id].to_dict();
    fields.insert("comment".into(), comment_json);
    ok(Value::Object(fields))
}

fn post_comment_reply(req: &Request, body: &Value, st: &mut CodeCommitStore) -> Response {
    let in_reply_to = s(body, "inReplyTo");
    let content = s(body, "content");
    if !st.comments.contains_key(in_reply_to) {
        return comment_does_not_exist(in_reply_to);
    }
    let comment_id = Uuid::new_v4().to_string();
    let comment = Comment::new(
        &comment_id,
        content,
        &caller_arn(req.account),
        Some(in_reply_to),
    );
    st.comments.insert(comment_id.clone(), comment);
    ok(json!({ "comment": st.comments[&comment_id].to_dict() }))
}

fn get_comment(body: &Value, st: &mut CodeCommitStore) -> Response {
    let id = s(body, "commentId");
    match st.comments.get(id) {
        Some(comment) => ok(json!({ "comment": comment.to_dict() })),
        None => comment_does_not_exist(id),
    }
}

fn update_comment(body: &Value, st: &mut CodeCommitStore) -> Response {
    let id = s(body, "commentId");
    let content = s(body, "content");
    let Some(comment) = st.comments.get_mut(id) else {
        return comment_does_not_exist(id);
    };
    comment.content = content.to_string();
    comment.last_modified_date = now_iso();
    ok(json!({ "comment": comment.to_dict() }))
}

fn delete_comment_content(body: &Value, st: &mut CodeCommitStore) -> Response {
    let id = s(body, "commentId");
    let Some(comment) = st.comments.get_mut(id) else {
        return comment_does_not_exist(id);
    };
    comment.content = String::new();
    comment.deleted = true;
    ok(json!({ "comment": comment.to_dict() }))
}

fn get_comments_for_compared_commit(body: &Value, st: &mut CodeCommitStore) -> Response {
    let name = s(body, "repositoryName");
    if let Err(resp) = get_repo(st, name) {
        return resp;
    }
    // moto's simplified approach: every comment in the backend.
    let comments: Vec<Value> = st.comments.values().map(|c| c.to_dict()).collect();
    ok(json!({ "commentsForComparedCommitData": comments }))
}

fn get_comments_for_pull_request(body: &Value, st: &mut CodeCommitStore) -> Response {
    let id = s(body, "pullRequestId");
    let repository_name = opt_s(body, "repositoryName").unwrap_or("");
    let before = opt_s(body, "beforeCommitId").unwrap_or("");
    let after = opt_s(body, "afterCommitId").unwrap_or("");
    let Ok(pr) = get_pr(st, id) else {
        return pull_request_does_not_exist(id);
    };
    let comment_ids = pr.comment_ids.clone();
    let mut data = Vec::new();
    for cid in &comment_ids {
        let Some(comment) = st.comments.get(cid) else {
            continue;
        };
        data.push(json!({
            "repositoryName": repository_name,
            "pullRequestId": id,
            "beforeCommitId": before,
            "afterCommitId": after,
            "beforeBlobId": "",
            "afterBlobId": "",
            "location": {},
            "comments": [comment.to_dict()],
        }));
    }
    ok(json!({ "commentsForPullRequestData": data }))
}

fn put_comment_reaction(body: &Value, st: &mut CodeCommitStore) -> Response {
    let id = s(body, "commentId");
    let reaction = s(body, "reactionValue");
    let Some(comment) = st.comments.get_mut(id) else {
        return comment_does_not_exist(id);
    };
    let counter = comment
        .reaction_counts
        .entry(reaction.to_string())
        .or_insert(0);
    *counter += 1;
    if !comment.caller_reactions.iter().any(|r| r == reaction) {
        comment.caller_reactions.push(reaction.to_string());
    }
    ok_empty()
}

fn get_comment_reactions(req: &Request, body: &Value, st: &mut CodeCommitStore) -> Response {
    let id = s(body, "commentId");
    let Some(comment) = st.comments.get(id) else {
        return comment_does_not_exist(id);
    };
    let reactions: Vec<Value> = comment
        .reaction_counts
        .keys()
        .map(|emoji| {
            json!({
                "reaction": {"emoji": emoji, "shortCode": emoji, "unicode": emoji},
                "reactionUsers": [caller_arn(req.account)],
                "reactionsFromDeletedUsersCount": 0,
            })
        })
        .collect();
    ok(json!({ "reactionsForComment": reactions }))
}

// ---------------------------------------------------------------------------
// Pull request approval rules
// ---------------------------------------------------------------------------

fn create_pull_request_approval_rule(body: &Value, st: &mut CodeCommitStore) -> Response {
    let id = s(body, "pullRequestId");
    let rule_name = s(body, "approvalRuleName");
    let rule_content = s(body, "approvalRuleContent");
    let Ok(pr) = get_pr(st, id) else {
        return pull_request_does_not_exist(id);
    };
    let rule_id = Uuid::new_v4().to_string();
    let rule = ApprovalRule::new(&rule_id, rule_name, rule_content);
    pr.approval_rules.insert(rule_name.to_string(), rule);
    ok(json!({ "approvalRule": pr.approval_rules[rule_name].to_dict() }))
}

fn delete_pull_request_approval_rule(body: &Value, st: &mut CodeCommitStore) -> Response {
    let id = s(body, "pullRequestId");
    let rule_name = s(body, "approvalRuleName");
    let Ok(pr) = get_pr(st, id) else {
        return pull_request_does_not_exist(id);
    };
    match pr.approval_rules.remove(rule_name) {
        Some(rule) => ok(json!({ "approvalRuleId": rule.approval_rule_id })),
        None => approval_rule_does_not_exist(rule_name),
    }
}

fn update_pull_request_approval_rule_content(body: &Value, st: &mut CodeCommitStore) -> Response {
    let id = s(body, "pullRequestId");
    let rule_name = s(body, "approvalRuleName");
    let new_content = s(body, "newRuleContent");
    let Ok(pr) = get_pr(st, id) else {
        return pull_request_does_not_exist(id);
    };
    let Some(rule) = pr.approval_rules.get_mut(rule_name) else {
        return approval_rule_does_not_exist(rule_name);
    };
    rule.approval_rule_content = new_content.to_string();
    rule.rule_content_sha256 = sha256_hex(new_content);
    rule.last_modified_date = now_iso();
    ok(json!({ "approvalRule": rule.to_dict() }))
}

fn update_pull_request_approval_state(
    req: &Request,
    body: &Value,
    st: &mut CodeCommitStore,
) -> Response {
    let id = s(body, "pullRequestId");
    let approval_state = s(body, "approvalState");
    let Ok(pr) = get_pr(st, id) else {
        return pull_request_does_not_exist(id);
    };
    // moto upserts by the caller's ARN.
    let caller = caller_arn(req.account);
    for state in &mut pr.approval_states {
        if state.get("userArn").and_then(Value::as_str) == Some(caller.as_str()) {
            state["approvalState"] = json!(approval_state);
            return ok_empty();
        }
    }
    pr.approval_states.push(json!({
        "approvalState": approval_state,
        "userArn": caller,
    }));
    ok_empty()
}

fn get_pull_request_approval_states(body: &Value, st: &mut CodeCommitStore) -> Response {
    let id = s(body, "pullRequestId");
    let Ok(pr) = get_pr(st, id) else {
        return pull_request_does_not_exist(id);
    };
    ok(json!({ "approvals": pr.approval_states }))
}

fn evaluate_pull_request_approval_rules(body: &Value, st: &mut CodeCommitStore) -> Response {
    let id = s(body, "pullRequestId");
    let Ok(pr) = get_pr(st, id) else {
        return pull_request_does_not_exist(id);
    };
    let satisfied: Vec<Value> = pr
        .approval_rules
        .values()
        .map(|r| json!(r.approval_rule_name))
        .collect();
    ok(json!({
        "evaluation": {
            "approved": !pr.approval_states.is_empty(),
            "overridden": pr.override_status == "OVERRIDE",
            "approvalRulesSatisfied": satisfied,
            "approvalRulesNotSatisfied": [],
        }
    }))
}

fn override_pull_request_approval_rules(
    req: &Request,
    body: &Value,
    st: &mut CodeCommitStore,
) -> Response {
    let id = s(body, "pullRequestId");
    let override_status = s(body, "overrideStatus");
    let Ok(pr) = get_pr(st, id) else {
        return pull_request_does_not_exist(id);
    };
    pr.override_status = override_status.to_string();
    pr.override_user = caller_arn(req.account);
    ok_empty()
}

fn get_pull_request_override_state(body: &Value, st: &mut CodeCommitStore) -> Response {
    let id = s(body, "pullRequestId");
    let Ok(pr) = get_pr(st, id) else {
        return pull_request_does_not_exist(id);
    };
    ok(json!({
        "overridden": pr.override_status == "OVERRIDE",
        "overrider": pr.override_user,
    }))
}

// ---------------------------------------------------------------------------
// Approval rule template operations
// ---------------------------------------------------------------------------

fn create_approval_rule_template(body: &Value, st: &mut CodeCommitStore) -> Response {
    let name = s(body, "approvalRuleTemplateName");
    let content = s(body, "approvalRuleTemplateContent");
    let description = body
        .get("approvalRuleTemplateDescription")
        .and_then(Value::as_str)
        .unwrap_or("");
    if st.approval_rule_templates.contains_key(name) {
        return template_name_already_exists(name);
    }
    let template = ApprovalRuleTemplate::new(name, content, description);
    st.approval_rule_templates
        .insert(name.to_string(), template);
    ok(json!({
        "approvalRuleTemplate": st.approval_rule_templates[name].to_dict(),
    }))
}

fn get_approval_rule_template(body: &Value, st: &mut CodeCommitStore) -> Response {
    let name = s(body, "approvalRuleTemplateName");
    match st.approval_rule_templates.get(name) {
        Some(t) => ok(json!({ "approvalRuleTemplate": t.to_dict() })),
        None => template_does_not_exist(name),
    }
}

fn delete_approval_rule_template(body: &Value, st: &mut CodeCommitStore) -> Response {
    let name = s(body, "approvalRuleTemplateName");
    match st.approval_rule_templates.remove(name) {
        Some(template) => ok(json!({
            "approvalRuleTemplateId": template.approval_rule_template_id,
        })),
        None => template_does_not_exist(name),
    }
}

fn list_approval_rule_templates(st: &mut CodeCommitStore) -> Response {
    let names: Vec<Value> = st
        .approval_rule_templates
        .keys()
        .map(|n| json!(n))
        .collect();
    ok(json!({ "approvalRuleTemplateNames": names }))
}

fn update_approval_rule_template_name(body: &Value, st: &mut CodeCommitStore) -> Response {
    let old_name = s(body, "oldApprovalRuleTemplateName");
    let new_name = s(body, "newApprovalRuleTemplateName");
    if !st.approval_rule_templates.contains_key(old_name) {
        return template_does_not_exist(old_name);
    }
    if st.approval_rule_templates.contains_key(new_name) {
        return template_name_already_exists(new_name);
    }
    let mut template = st
        .approval_rule_templates
        .remove(old_name)
        .expect("checked");
    template.approval_rule_template_name = new_name.to_string();
    template.last_modified_date = now_iso();
    st.approval_rule_templates
        .insert(new_name.to_string(), template);
    ok(json!({
        "approvalRuleTemplate": st.approval_rule_templates[new_name].to_dict(),
    }))
}

fn update_approval_rule_template_content(body: &Value, st: &mut CodeCommitStore) -> Response {
    let name = s(body, "approvalRuleTemplateName");
    let new_content = s(body, "newRuleContent");
    let Some(template) = st.approval_rule_templates.get_mut(name) else {
        return template_does_not_exist(name);
    };
    template.approval_rule_template_content = new_content.to_string();
    template.rule_content_sha256 = sha256_hex(new_content);
    template.last_modified_date = now_iso();
    ok(json!({ "approvalRuleTemplate": template.to_dict() }))
}

fn update_approval_rule_template_description(body: &Value, st: &mut CodeCommitStore) -> Response {
    let name = s(body, "approvalRuleTemplateName");
    let description = s(body, "approvalRuleTemplateDescription");
    let Some(template) = st.approval_rule_templates.get_mut(name) else {
        return template_does_not_exist(name);
    };
    template.approval_rule_template_description = description.to_string();
    template.last_modified_date = now_iso();
    ok(json!({ "approvalRuleTemplate": template.to_dict() }))
}

fn get_template<'a>(
    st: &'a mut CodeCommitStore,
    name: &str,
) -> Result<&'a mut ApprovalRuleTemplate, Response> {
    match st.approval_rule_templates.get_mut(name) {
        Some(t) => Ok(t),
        None => Err(template_does_not_exist(name)),
    }
}

fn associate_approval_rule_template_with_repository(
    body: &Value,
    st: &mut CodeCommitStore,
) -> Response {
    let template_name = s(body, "approvalRuleTemplateName");
    let repository_name = s(body, "repositoryName");
    let Ok(_template) = get_template(st, template_name) else {
        return template_does_not_exist(template_name);
    };
    let Ok(_repo) = get_repo(st, repository_name) else {
        return repo_does_not_exist(repository_name);
    };
    let Some(template) = st.approval_rule_templates.get_mut(template_name) else {
        return template_does_not_exist(template_name);
    };
    if !template
        .associated_repositories
        .iter()
        .any(|r| r == repository_name)
    {
        template
            .associated_repositories
            .push(repository_name.to_string());
    }
    ok_empty()
}

fn disassociate_approval_rule_template_from_repository(
    body: &Value,
    st: &mut CodeCommitStore,
) -> Response {
    let template_name = s(body, "approvalRuleTemplateName");
    let repository_name = s(body, "repositoryName");
    // moto only validates the template for this operation.
    let Ok(template) = get_template(st, template_name) else {
        return template_does_not_exist(template_name);
    };
    template
        .associated_repositories
        .retain(|r| r != repository_name);
    ok_empty()
}

fn batch_associate_approval_rule_template_with_repositories(
    body: &Value,
    st: &mut CodeCommitStore,
) -> Response {
    let template_name = s(body, "approvalRuleTemplateName");
    let names = str_list(body, "repositoryNames");
    let Ok(_template) = get_template(st, template_name) else {
        return template_does_not_exist(template_name);
    };
    let mut associated = Vec::new();
    let mut errors = Vec::new();
    for name in names {
        if st.repositories.contains_key(name.as_str()) {
            if let Some(template) = st.approval_rule_templates.get_mut(template_name) {
                if !template.associated_repositories.iter().any(|r| r == &name) {
                    template.associated_repositories.push(name.clone());
                }
            }
            associated.push(json!(name));
        } else {
            errors.push(json!({
                "repositoryName": name,
                "errorCode": "RepositoryDoesNotExistException",
                "errorMessage": format!("{name} does not exist"),
            }));
        }
    }
    ok(json!({
        "associatedRepositoryNames": associated,
        "errors": errors,
    }))
}

fn batch_disassociate_approval_rule_template_from_repositories(
    body: &Value,
    st: &mut CodeCommitStore,
) -> Response {
    let template_name = s(body, "approvalRuleTemplateName");
    let names = str_list(body, "repositoryNames");
    let Ok(_template) = get_template(st, template_name) else {
        return template_does_not_exist(template_name);
    };
    let mut disassociated = Vec::new();
    for name in names {
        // moto appends every requested name, associated or not.
        if let Some(template) = st.approval_rule_templates.get_mut(template_name) {
            template.associated_repositories.retain(|r| r != &name);
        }
        disassociated.push(json!(name));
    }
    ok(json!({
        "disassociatedRepositoryNames": disassociated,
        "errors": [],
    }))
}

fn list_associated_approval_rule_templates_for_repository(
    body: &Value,
    st: &mut CodeCommitStore,
) -> Response {
    let name = s(body, "repositoryName");
    if let Err(resp) = get_repo(st, name) {
        return resp;
    }
    let names: Vec<Value> = st
        .approval_rule_templates
        .values()
        .filter(|t| t.associated_repositories.iter().any(|r| r == name))
        .map(|t| json!(t.approval_rule_template_name))
        .collect();
    ok(json!({ "approvalRuleTemplateNames": names }))
}

fn list_repositories_for_approval_rule_template(
    body: &Value,
    st: &mut CodeCommitStore,
) -> Response {
    let template_name = s(body, "approvalRuleTemplateName");
    let Ok(template) = get_template(st, template_name) else {
        return template_does_not_exist(template_name);
    };
    let names: Vec<Value> = template
        .associated_repositories
        .iter()
        .map(|n| json!(n))
        .collect();
    ok(json!({ "repositoryNames": names }))
}

// ---------------------------------------------------------------------------
// Trigger operations
// ---------------------------------------------------------------------------

fn get_repository_triggers(body: &Value, st: &mut CodeCommitStore) -> Response {
    let name = s(body, "repositoryName");
    let Ok(repo) = get_repo(st, name) else {
        return repo_does_not_exist(name);
    };
    ok(json!({
        "repositoryName": name,
        "triggers": repo.triggers,
        "configurationId": Uuid::new_v4().to_string(),
    }))
}

fn put_repository_triggers(body: &Value, st: &mut CodeCommitStore) -> Response {
    let name = s(body, "repositoryName");
    let triggers = body
        .get("triggers")
        .and_then(Value::as_array)
        .cloned()
        .unwrap_or_default();
    let Ok(repo) = get_repo(st, name) else {
        return repo_does_not_exist(name);
    };
    repo.triggers = triggers;
    ok(json!({ "configurationId": Uuid::new_v4().to_string() }))
}

fn test_repository_triggers(body: &Value, st: &mut CodeCommitStore) -> Response {
    let name = s(body, "repositoryName");
    let triggers = body
        .get("triggers")
        .and_then(Value::as_array)
        .cloned()
        .unwrap_or_default();
    if let Err(resp) = get_repo(st, name) {
        return resp;
    }
    let successful: Vec<Value> = triggers
        .iter()
        .map(|t| json!(t.get("name").and_then(Value::as_str).unwrap_or("")))
        .collect();
    ok(json!({
        "successfulExecutions": successful,
        "failedExecutions": [],
    }))
}

// ---------------------------------------------------------------------------
// Tag operations
// ---------------------------------------------------------------------------

fn tag_resource(body: &Value, st: &mut CodeCommitStore) -> Response {
    let arn = s(body, "resourceArn");
    let tags = body
        .get("tags")
        .and_then(Value::as_object)
        .cloned()
        .unwrap_or_default();
    let Some(repo) = st
        .repositories
        .values_mut()
        .find(|r| r.metadata.get("Arn").and_then(Value::as_str) == Some(arn))
    else {
        return ok_empty();
    };
    for (key, value) in tags {
        if let Some(value) = value.as_str() {
            repo.tags.insert(key, value.to_string());
        }
    }
    ok_empty()
}

fn untag_resource(body: &Value, st: &mut CodeCommitStore) -> Response {
    let arn = s(body, "resourceArn");
    let keys = str_list(body, "tagKeys");
    let Some(repo) = st
        .repositories
        .values_mut()
        .find(|r| r.metadata.get("Arn").and_then(Value::as_str) == Some(arn))
    else {
        return ok_empty();
    };
    for key in keys {
        repo.tags.remove(key.as_str());
    }
    ok_empty()
}

fn list_tags_for_resource(body: &Value, st: &mut CodeCommitStore) -> Response {
    let arn = s(body, "resourceArn");
    let tags = st
        .repositories
        .values()
        .find(|r| r.metadata.get("Arn").and_then(Value::as_str) == Some(arn))
        .map(|repo| {
            repo.tags
                .iter()
                .map(|(k, v)| (k.clone(), json!(v)))
                .collect::<Map<String, Value>>()
        })
        .unwrap_or_default();
    ok(json!({ "tags": tags }))
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    // Test flows bind `(status, body)` tuples and discard the status half
    // when only the payload is asserted, matching the ec2 module's precedent.
    #![allow(unused_variables)]
    use super::*;
    use bytes::Bytes;
    use std::sync::atomic::{AtomicU64, Ordering};

    static ACCOUNTS: AtomicU64 = AtomicU64::new(200_000_000_001);

    /// Every test takes a fresh account so the shared store stays isolated.
    fn next_account() -> u64 {
        ACCOUNTS.fetch_add(1, Ordering::SeqCst)
    }

    fn send(account: u64, operation: &str, body: Value) -> (u16, Value) {
        let request = Request {
            service: "codecommit".to_string(),
            operation: operation.to_string(),
            method: "POST".to_string(),
            path: "/".to_string(),
            query_string: String::new(),
            headers: HashMap::new(),
            body: Bytes::from(serde_json::to_vec(&body).unwrap()),
            account,
            region: "us-east-1".to_string(),
        };
        let resp = dispatch(&request).expect("codecommit dispatch");
        (
            resp.status,
            serde_json::from_slice(&resp.body).unwrap_or(json!({})),
        )
    }

    fn make_repo(account: u64, name: &str) -> String {
        let (status, body) = send(
            account,
            "CreateRepository",
            json!({ "repositoryName": name, "repositoryDescription": "d" }),
        );
        assert_eq!(status, 200, "create_repository failed: {body}");
        body["repositoryMetadata"]["repositoryId"]
            .as_str()
            .expect("repositoryId")
            .to_string()
    }

    /// create_commit with a base64-encoded putFiles member, mirroring what
    /// botocore puts on the wire for blob inputs.
    #[test]
    fn repository_lifecycle() {
        let account = next_account();
        let name = "lifecycle-repo";
        assert_eq!(make_repo(account, name).len(), 36);

        let (status, body) = send(
            account,
            "CreateRepository",
            json!({ "repositoryName": name }),
        );
        assert_eq!(status, 400);
        assert_eq!(body["__type"], "RepositoryNameExistsException");

        let (status, body) = send(account, "GetRepository", json!({ "repositoryName": name }));
        assert_eq!(status, 200);
        let meta = &body["repositoryMetadata"];
        assert_eq!(meta["repositoryName"], name);
        assert_eq!(meta["accountId"], account.to_string());
        assert_eq!(meta["defaultBranch"], "main");
        assert_eq!(
            meta["Arn"],
            json!(format!("arn:aws:codecommit:us-east-1:{account}:{name}"))
        );

        let (status, body) = send(
            account,
            "BatchGetRepositories",
            json!({ "repositoryNames": [name, "missing-repo"] }),
        );
        assert_eq!(status, 200);
        assert_eq!(body["repositories"].as_array().unwrap().len(), 1);
        assert_eq!(
            body["repositoriesNotFound"],
            json!(["missing-repo"]),
            "missing repos land in repositoriesNotFound"
        );

        // Renaming keeps the id and drops the old name.
        let (status, body) = send(
            account,
            "UpdateRepositoryName",
            json!({ "oldName": name, "newName": "renamed-repo" }),
        );
        assert_eq!(status, 200, "{body}");
        let (status, body) = send(account, "GetRepository", json!({ "repositoryName": name }));
        assert_eq!(body["__type"], "RepositoryDoesNotExistException");
        let (status, body) = send(
            account,
            "GetRepository",
            json!({ "repositoryName": "renamed-repo" }),
        );
        assert_eq!(status, 200);
        assert!(
            !body["repositoryMetadata"]["repositoryId"]
                .as_str()
                .unwrap()
                .is_empty(),
            "id preserved on rename"
        );

        let (status, body) = send(
            account,
            "DeleteRepository",
            json!({ "repositoryName": "renamed-repo" }),
        );
        assert_eq!(status, 200);
        assert!(body["repositoryId"].as_str().is_some());
    }

    #[test]
    fn invalid_names_rejected() {
        let account = next_account();
        let (status, body) = send(
            account,
            "CreateRepository",
            json!({ "repositoryName": "repo with spaces" }),
        );
        assert_eq!(status, 400);
        assert_eq!(body["__type"], "InvalidRepositoryNameException");
    }

    #[test]
    fn commit_and_file_paths() {
        let account = next_account();
        let name = "files-repo";
        make_repo(account, name);

        // putFiles fileContent arrives as the base64 string botocore sent.
        let b64 = B64.encode(b"Hello World");
        let (status, body) = send(
            account,
            "CreateCommit",
            json!({
                "repositoryName": name,
                "branchName": "main",
                "putFiles": [{"filePath": "README.md", "fileContent": b64}],
                "authorName": "test",
                "email": "t@t.t",
                "commitMessage": "Initial commit",
            }),
        );
        assert_eq!(status, 200, "{body}");
        let commit_id = body["commitId"].as_str().expect("commitId").to_string();
        assert_eq!(commit_id.len(), 32);
        assert_eq!(body["filesAdded"].as_array().unwrap().len(), 1);

        // GetFile returns the stored (base64) bytes re-encoded — the client
        // decodes once to the base64 string, then once more to the content.
        let (status, body) = send(
            account,
            "GetFile",
            json!({ "repositoryName": name, "filePath": "README.md" }),
        );
        assert_eq!(status, 200);
        assert_eq!(body["filePath"], "README.md");
        let wire = body["fileContent"].as_str().expect("fileContent");
        assert_eq!(
            B64.decode(wire).unwrap(),
            B64.encode(b"Hello World").as_bytes()
        );

        // GetFolder at the root shows the file and the subtree entry.
        let (status, body) = send(
            account,
            "GetFolder",
            json!({ "repositoryName": name, "folderPath": "/" }),
        );
        assert_eq!(status, 200);
        assert_eq!(body["folderPath"], "/");
        let files: Vec<&Value> = body["files"].as_array().unwrap().iter().collect();
        assert!(files.iter().any(|f| f["absolutePath"] == "README.md"));

        // Branch commit resolution for GetFile/get_folder.
        let (status, body) = send(
            account,
            "GetBranch",
            json!({ "repositoryName": name, "branchName": "main" }),
        );
        assert_eq!(status, 200);
        assert_eq!(body["branch"]["commitId"], json!(commit_id));

        // Unknown commits come back as stubs.
        let (status, body) = send(
            account,
            "GetCommit",
            json!({ "repositoryName": name, "commitId": "no-such-commit" }),
        );
        assert_eq!(status, 200);
        assert_eq!(body["commit"]["commitId"], "no-such-commit");
        assert_eq!(body["commit"]["author"]["name"], "Unknown");
    }

    #[test]
    fn put_file_and_delete_file() {
        let account = next_account();
        let name = "putfile-repo";
        make_repo(account, name);
        let b64 = B64.encode(b"Hello World");

        // Seed via create_commit so main has a commit to parent onto.
        let (_, body) = send(
            account,
            "CreateCommit",
            json!({
                "repositoryName": name,
                "branchName": "main",
                "putFiles": [{"filePath": "README.md", "fileContent": b64}],
            }),
        );
        let parent = body["commitId"].as_str().unwrap().to_string();

        // PutFile decodes the blob first, so GetFile surfaces raw bytes.
        let (status, body) = send(
            account,
            "PutFile",
            json!({
                "repositoryName": name,
                "branchName": "main",
                "filePath": "newfile.txt",
                "fileContent": B64.encode(b"new file content"),
                "parentCommitId": parent,
            }),
        );
        assert_eq!(status, 200, "{body}");
        assert!(body["commitId"].as_str().is_some());
        assert!(body["blobId"].as_str().is_some());

        let (status, body) = send(
            account,
            "GetFile",
            json!({ "repositoryName": name, "filePath": "newfile.txt" }),
        );
        assert_eq!(status, 200);
        let wire = body["fileContent"].as_str().expect("fileContent");
        assert_eq!(B64.decode(wire).unwrap(), b"new file content");

        // DeleteFile removes the entry and reports it.
        let (status, body) = send(
            account,
            "DeleteFile",
            json!({
                "repositoryName": name,
                "branchName": "main",
                "filePath": "README.md",
                "parentCommitId": body["commitId"].as_str().unwrap_or_default(),
            }),
        );
        assert_eq!(status, 200);
        assert_eq!(body["filePath"], "README.md");
        let (status, body) = send(
            account,
            "GetFile",
            json!({ "repositoryName": name, "filePath": "README.md" }),
        );
        assert_eq!(status, 400);
        assert_eq!(body["__type"], "FileDoesNotExistException");
    }

    #[test]
    fn branches_and_default_branch() {
        let account = next_account();
        let name = "branches-repo";
        make_repo(account, name);
        let (_, body) = send(
            account,
            "CreateCommit",
            json!({
                "repositoryName": name,
                "branchName": "main",
                "putFiles": [],
            }),
        );
        let commit = body["commitId"].as_str().unwrap().to_string();

        let (status, body) = send(
            account,
            "CreateBranch",
            json!({
                "repositoryName": name,
                "branchName": "feature",
                "commitId": json!(commit.clone()),
            }),
        );
        assert_eq!(status, 200);
        let (status, body) = send(
            account,
            "CreateBranch",
            json!({ "repositoryName": name, "branchName": "feature", "commitId": "x" }),
        );
        assert_eq!(body["__type"], "BranchNameExistsException");
        assert_eq!(status, 400);

        let (status, body) = send(account, "ListBranches", json!({ "repositoryName": name }));
        let branches = body["branches"].as_array().unwrap();
        assert!(branches.contains(&json!("main")));
        assert!(branches.contains(&json!("feature")));

        // The default branch refuses deletion.
        let (status, body) = send(
            account,
            "DeleteBranch",
            json!({ "repositoryName": name, "branchName": "main" }),
        );
        assert_eq!(status, 400);
        assert_eq!(body["__type"], "DefaultBranchCannotBeDeletedException");

        let (status, _) = send(
            account,
            "DeleteBranch",
            json!({ "repositoryName": name, "branchName": "feature" }),
        );
        assert_eq!(status, 200);

        let (status, _) = send(
            account,
            "UpdateDefaultBranch",
            json!({ "repositoryName": name, "defaultBranchName": "dev" }),
        );
        let (status, body) = send(
            account,
            "UpdateDefaultBranch",
            json!({ "repositoryName": name, "defaultBranchName": "feature" }),
        );
        assert_eq!(body["__type"], "BranchDoesNotExistException");
        let _ = status;
    }

    #[test]
    fn pull_request_lifecycle() {
        let account = next_account();
        let name = "pr-repo";
        make_repo(account, name);
        let (status, body) = send(
            account,
            "CreatePullRequest",
            json!({
                "title": "T",
                "description": "D",
                "targets": [{
                    "repositoryName": name,
                    "sourceReference": "feature",
                    "destinationReference": "main",
                }],
            }),
        );
        assert_eq!(status, 200);
        let pr_id = body["pullRequest"]["pullRequestId"]
            .as_str()
            .unwrap()
            .to_string();
        assert_eq!(body["pullRequest"]["pullRequestStatus"], "OPEN");
        assert_eq!(
            body["pullRequest"]["pullRequestTargets"][0]["repositoryName"],
            name
        );

        let (status, body) = send(
            account,
            "ListPullRequests",
            json!({ "repositoryName": name, "pullRequestStatus": "OPEN" }),
        );
        assert!(body["pullRequestIds"]
            .as_array()
            .unwrap()
            .contains(&json!(pr_id)));

        // Merge by fast-forward closes the PR.
        let (status, body) = send(
            account,
            "MergePullRequestByFastForward",
            json!({ "pullRequestId": pr_id, "repositoryName": name }),
        );
        assert_eq!(status, 200);
        assert_eq!(body["pullRequest"]["pullRequestStatus"], "CLOSED");
        assert_eq!(
            body["pullRequest"]["pullRequestTargets"][0]["mergeMetadata"]["isMerged"],
            true
        );

        // Already closed: second merge or a status update errors.
        let (status, body) = send(
            account,
            "MergePullRequestByFastForward",
            json!({ "pullRequestId": pr_id, "repositoryName": name }),
        );
        assert_eq!(body["__type"], "PullRequestAlreadyClosedException");

        // MERGE stubs answer fresh ids without touching state.
        let (status, body) = send(
            account,
            "MergeBranchesByThreeWay",
            json!({
                "repositoryName": name,
                "sourceCommitSpecifier": "feature",
                "destinationCommitSpecifier": "main",
            }),
        );
        assert_eq!(status, 200);
        assert!(body["commitId"].as_str().unwrap().len() == 32);
    }

    #[test]
    fn comments_and_reactions() {
        let account = next_account();
        let name = "comments-repo";
        make_repo(account, name);
        let (_, body) = send(
            account,
            "CreateCommit",
            json!({
                "repositoryName": name,
                "branchName": "main",
                "putFiles": [],
            }),
        );
        let commit = json!(body["commitId"].as_str().unwrap().to_string());

        let (status, body) = send(
            account,
            "PostCommentForComparedCommit",
            json!({
                "repositoryName": name,
                "afterCommitId": commit,
                "content": "note",
            }),
        );
        assert_eq!(status, 200);
        let comment_id = body["comment"]["commentId"].as_str().unwrap().to_string();
        assert_eq!(body["comment"]["content"], "note");

        let (status, body) = send(
            account,
            "PutCommentReaction",
            json!({ "commentId": json!(comment_id), "reactionValue": ":thumbsup:" }),
        );
        assert_eq!(status, 200);
        let (status, body) = send(
            account,
            "GetCommentReactions",
            json!({ "commentId": comment_id }),
        );
        let reactions = body["reactionsForComment"].as_array().unwrap();
        assert_eq!(reactions.len(), 1);
        assert_eq!(reactions[0]["reaction"]["emoji"], ":thumbsup:");
        assert_eq!(reactions[0]["reactionUsers"][0], caller_arn(account));

        // Replies chain through inReplyTo.
        let (status, body) = send(
            account,
            "PostCommentReply",
            json!({ "inReplyTo": comment_id, "content": "reply" }),
        );
        assert_eq!(body["comment"]["inReplyTo"], json!(comment_id));

        // Deleting empties the content and flips the flag.
        let (status, body) = send(
            account,
            "DeleteCommentContent",
            json!({ "commentId": comment_id }),
        );
        assert_eq!(body["comment"]["deleted"], true);
        assert_eq!(body["comment"]["content"], "");

        // Unknown comments 400.
        let (status, body) = send(account, "GetComment", json!({ "commentId": "nope" }));
        assert_eq!(body["__type"], "CommentDoesNotExistException");
    }

    #[test]
    fn templates_associations_and_triggers() {
        let account = next_account();
        let (status, body) = send(
            account,
            "CreateApprovalRuleTemplate",
            json!({
                "approvalRuleTemplateName": "tpl",
                "approvalRuleTemplateContent": "approvals: 1",
            }),
        );
        assert_eq!(status, 200);
        assert!(body["approvalRuleTemplate"]["approvalRuleTemplateId"]
            .as_str()
            .is_some());

        let (status, body) = send(
            account,
            "CreateApprovalRuleTemplate",
            json!({
                "approvalRuleTemplateName": "tpl",
                "approvalRuleTemplateContent": "approvals: 1",
            }),
        );
        assert_eq!(
            body["__type"],
            "ApprovalRuleTemplateNameAlreadyExistsException"
        );

        let name = "tpl-repo";
        make_repo(account, name);
        let (status, _) = send(
            account,
            "AssociateApprovalRuleTemplateWithRepository",
            json!({
                "approvalRuleTemplateName": "tpl",
                "repositoryName": name,
            }),
        );
        assert_eq!(status, 200);

        let (status, body) = send(
            account,
            "ListRepositoriesForApprovalRuleTemplate",
            json!({ "approvalRuleTemplateName": "tpl" }),
        );
        assert_eq!(
            body["repositoryNames"],
            json!([name]),
            "association recorded"
        );

        // Triggers round-trip with fresh configuration ids.
        let (status, body) = send(
            account,
            "PutRepositoryTriggers",
            json!({
                "repositoryName": name,
                "triggers": [{"name": "trig", "destinationArn": "arn:aws:sns:us-east-1::t", "events": ["all"]}],
            }),
        );
        assert!(body["configurationId"].as_str().is_some());
        let (status, body) = send(
            account,
            "GetRepositoryTriggers",
            json!({ "repositoryName": name }),
        );
        assert_eq!(body["triggers"][0]["name"], "trig");
        let (status, body) = send(
            account,
            "TestRepositoryTriggers",
            json!({
                "repositoryName": name,
                "triggers": [{"name": "trig"}],
            }),
        );
        assert_eq!(body["successfulExecutions"], json!(["trig"]));
        assert_eq!(body["failedExecutions"], json!([]));
    }

    #[test]
    fn tags_roundtrip() {
        let account = next_account();
        let name = "tags-repo";
        let _id = make_repo(account, name);
        let (_, body) = send(account, "GetRepository", json!({ "repositoryName": name }));
        let arn = body["repositoryMetadata"]["Arn"]
            .as_str()
            .unwrap()
            .to_string();
        assert!(arn.starts_with("arn:aws:codecommit:"));

        let (status, _) = send(
            account,
            "ListTagsForResource",
            json!({ "resourceArn": arn }),
        );
        assert_eq!(status, 200);

        let (status, _) = send(
            account,
            "TagResource",
            json!({ "resourceArn": arn, "tags": {"env": "test"} }),
        );
        assert_eq!(status, 200);
        let (status, body) = send(
            account,
            "ListTagsForResource",
            json!({ "resourceArn": arn }),
        );
        assert_eq!(body["tags"]["env"], "test");

        let (status, _) = send(
            account,
            "UntagResource",
            json!({ "resourceArn": arn, "tagKeys": ["env"] }),
        );
        assert_eq!(status, 200);
        let (status, body) = send(
            account,
            "ListTagsForResource",
            json!({ "resourceArn": arn }),
        );
        assert!(
            body["tags"].as_object().unwrap().get("env").is_none(),
            "tag removed"
        );
    }

    #[test]
    fn approval_rules_on_pr() {
        let account = next_account();
        let name = "rules-repo";
        make_repo(account, name);
        let (_, body) = send(
            account,
            "CreatePullRequest",
            json!({
                "title": "T",
                "targets": [{"repositoryName": name, "sourceReference": "a"}],
            }),
        );
        let pr_id = body["pullRequest"]["pullRequestId"]
            .as_str()
            .unwrap()
            .to_string();

        let (status, body) = send(
            account,
            "CreatePullRequestApprovalRule",
            json!({
                "pullRequestId": pr_id,
                "approvalRuleName": "rule",
                "approvalRuleContent": "approvals: 1",
            }),
        );
        assert_eq!(status, 200);
        assert_eq!(body["approvalRule"]["approvalRuleName"], "rule");

        let (status, body) = send(
            account,
            "EvaluatePullRequestApprovalRules",
            json!({ "pullRequestId": json!(pr_id), "revisionId": "r1" }),
        );
        assert_eq!(body["evaluation"]["approved"], false);
        assert_eq!(
            body["evaluation"]["approvalRulesSatisfied"],
            json!(["rule"])
        );

        let (status, _) = send(
            account,
            "OverridePullRequestApprovalRules",
            json!({
                "pullRequestId": pr_id,
                "revisionId": "r1",
                "overrideStatus": "OVERRIDE",
            }),
        );
        let (status, body) = send(
            account,
            "GetPullRequestOverrideState",
            json!({ "pullRequestId": pr_id, "revisionId": "r1" }),
        );
        assert_eq!(body["overridden"], true);
    }

    #[test]
    fn handles_only_codecommit() {
        let request = Request {
            service: "kms".to_string(),
            operation: "CreateRepository".to_string(),
            method: "POST".to_string(),
            path: "/".to_string(),
            query_string: String::new(),
            headers: HashMap::new(),
            body: Bytes::new(),
            account: 123,
            region: "us-east-1".to_string(),
        };
        assert!(dispatch(&request).is_none());
    }
}
