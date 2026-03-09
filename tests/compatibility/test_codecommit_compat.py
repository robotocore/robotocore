"""Compatibility tests for AWS CodeCommit service."""

import base64
import uuid

import botocore.exceptions
import pytest

from tests.compatibility.conftest import make_client

APPROVAL_RULE_CONTENT = (
    '{"Version": "2018-11-08", "Statements": [{"Type": "Approvers", "NumberOfApprovalsNeeded": 1}]}'
)
APPROVAL_RULE_CONTENT_2 = (
    '{"Version": "2018-11-08", "Statements": [{"Type": "Approvers", "NumberOfApprovalsNeeded": 2}]}'
)


@pytest.fixture
def codecommit():
    return make_client("codecommit")


def _unique(prefix):
    return f"{prefix}-{uuid.uuid4().hex[:8]}"


def _create_repo_with_commit(cc, name=None):
    """Helper: create a repo with an initial commit on 'main'. Returns (name, commit_id)."""
    if name is None:
        name = _unique("repo")
    cc.create_repository(repositoryName=name)
    resp = cc.create_commit(
        repositoryName=name,
        branchName="main",
        putFiles=[{"filePath": "README.md", "fileContent": b"Hello World"}],
        authorName="test",
        email="test@test.com",
        commitMessage="Initial commit",
    )
    return name, resp["commitId"]


def _create_repo_with_feature_branch(cc, name=None):
    """Create repo with main + feature branch."""
    name, main_commit = _create_repo_with_commit(cc, name)
    cc.create_branch(repositoryName=name, branchName="feature", commitId=main_commit)
    resp = cc.create_commit(
        repositoryName=name,
        branchName="feature",
        putFiles=[{"filePath": "feature.txt", "fileContent": b"Feature work"}],
        authorName="test",
        email="test@test.com",
        commitMessage="Feature commit",
        parentCommitId=main_commit,
    )
    return name, main_commit, resp["commitId"]


class TestCodeCommitRepositoryOperations:
    """Tests for CodeCommit repository CRUD operations."""

    def test_create_repository(self, codecommit):
        name = _unique("repo")
        resp = codecommit.create_repository(
            repositoryName=name,
            repositoryDescription="Test repo",
        )
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200
        meta = resp["repositoryMetadata"]
        assert meta["repositoryName"] == name
        assert "repositoryId" in meta
        assert "Arn" in meta
        codecommit.delete_repository(repositoryName=name)

    def test_create_repository_response_fields(self, codecommit):
        """Verify all expected fields in create_repository response."""
        name = _unique("repo")
        try:
            resp = codecommit.create_repository(
                repositoryName=name,
                repositoryDescription="detailed field check",
            )
            meta = resp["repositoryMetadata"]
            assert meta["repositoryName"] == name
            assert meta["repositoryDescription"] == "detailed field check"
            assert meta["accountId"] == "123456789012"
            assert name in meta["cloneUrlHttp"]
            assert name in meta["cloneUrlSsh"]
            assert "codecommit" in meta["Arn"]
            assert "repositoryId" in meta
            assert "creationDate" in meta
            assert "lastModifiedDate" in meta
        finally:
            codecommit.delete_repository(repositoryName=name)

    def test_create_repository_duplicate_name(self, codecommit):
        """Creating a repo with a duplicate name raises RepositoryNameExistsException."""
        name = _unique("repo")
        try:
            codecommit.create_repository(repositoryName=name)
            with pytest.raises(botocore.exceptions.ClientError) as exc_info:
                codecommit.create_repository(repositoryName=name)
            assert "RepositoryNameExistsException" in str(exc_info.value)
        finally:
            codecommit.delete_repository(repositoryName=name)

    def test_get_repository(self, codecommit):
        name = _unique("repo")
        codecommit.create_repository(repositoryName=name)
        resp = codecommit.get_repository(repositoryName=name)
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200
        assert resp["repositoryMetadata"]["repositoryName"] == name
        codecommit.delete_repository(repositoryName=name)

    def test_get_repository_nonexistent(self, codecommit):
        """Getting a nonexistent repo raises RepositoryDoesNotExistException."""
        with pytest.raises(botocore.exceptions.ClientError) as exc_info:
            codecommit.get_repository(repositoryName=_unique("nonexistent"))
        assert "RepositoryDoesNotExistException" in str(exc_info.value)

    def test_get_repository_clone_urls(self, codecommit):
        """Verify clone URLs contain correct region and repo name."""
        name = _unique("repo")
        try:
            codecommit.create_repository(repositoryName=name)
            resp = codecommit.get_repository(repositoryName=name)
            meta = resp["repositoryMetadata"]
            assert "us-east-1" in meta["cloneUrlHttp"]
            assert name in meta["cloneUrlHttp"]
            assert "us-east-1" in meta["cloneUrlSsh"]
            assert name in meta["cloneUrlSsh"]
        finally:
            codecommit.delete_repository(repositoryName=name)

    def test_get_repository_arn_format(self, codecommit):
        """Verify ARN follows expected format."""
        name = _unique("repo")
        try:
            codecommit.create_repository(repositoryName=name)
            resp = codecommit.get_repository(repositoryName=name)
            arn = resp["repositoryMetadata"]["Arn"]
            assert arn.startswith("arn:aws:codecommit:us-east-1:123456789012:")
            assert arn.endswith(name)
        finally:
            codecommit.delete_repository(repositoryName=name)

    def test_delete_repository(self, codecommit):
        name = _unique("repo")
        codecommit.create_repository(repositoryName=name)
        resp = codecommit.delete_repository(repositoryName=name)
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200

    def test_delete_repository_then_get(self, codecommit):
        """After deleting a repo, getting it should raise an error."""
        name = _unique("repo")
        codecommit.create_repository(repositoryName=name)
        codecommit.delete_repository(repositoryName=name)
        with pytest.raises(botocore.exceptions.ClientError) as exc_info:
            codecommit.get_repository(repositoryName=name)
        assert "RepositoryDoesNotExistException" in str(exc_info.value)

    def test_create_repository_no_description(self, codecommit):
        """Creating a repo without a description should work."""
        name = _unique("repo")
        try:
            resp = codecommit.create_repository(repositoryName=name)
            assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200
            assert resp["repositoryMetadata"]["repositoryName"] == name
        finally:
            codecommit.delete_repository(repositoryName=name)

    def test_create_multiple_repositories(self, codecommit):
        """Create multiple repos and verify each has a unique repositoryId."""
        name1 = _unique("repo")
        name2 = _unique("repo")
        try:
            r1 = codecommit.create_repository(repositoryName=name1)
            r2 = codecommit.create_repository(repositoryName=name2)
            id1 = r1["repositoryMetadata"]["repositoryId"]
            id2 = r2["repositoryMetadata"]["repositoryId"]
            assert id1 != id2
            assert r1["repositoryMetadata"]["repositoryName"] == name1
            assert r2["repositoryMetadata"]["repositoryName"] == name2
        finally:
            codecommit.delete_repository(repositoryName=name1)
            codecommit.delete_repository(repositoryName=name2)

    def test_list_repositories(self, codecommit):
        """ListRepositories returns created repos."""
        name = _unique("repo")
        try:
            codecommit.create_repository(repositoryName=name)
            resp = codecommit.list_repositories()
            assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200
            assert "repositories" in resp
            names = [r["repositoryName"] for r in resp["repositories"]]
            assert name in names
        finally:
            codecommit.delete_repository(repositoryName=name)

    def test_batch_get_repositories(self, codecommit):
        """BatchGetRepositories returns metadata for requested repos."""
        name1 = _unique("repo")
        name2 = _unique("repo")
        try:
            codecommit.create_repository(repositoryName=name1)
            codecommit.create_repository(repositoryName=name2)
            resp = codecommit.batch_get_repositories(repositoryNames=[name1, name2])
            assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200
            assert len(resp["repositories"]) == 2
            returned_names = {r["repositoryName"] for r in resp["repositories"]}
            assert name1 in returned_names
            assert name2 in returned_names
        finally:
            codecommit.delete_repository(repositoryName=name1)
            codecommit.delete_repository(repositoryName=name2)

    def test_batch_get_repositories_not_found(self, codecommit):
        """BatchGetRepositories returns repositoriesNotFound for missing repos."""
        resp = codecommit.batch_get_repositories(repositoryNames=[_unique("nonexistent")])
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200
        assert len(resp["repositoriesNotFound"]) == 1

    def test_update_repository_description(self, codecommit):
        """UpdateRepositoryDescription changes the description."""
        name = _unique("repo")
        try:
            codecommit.create_repository(repositoryName=name)
            codecommit.update_repository_description(
                repositoryName=name, repositoryDescription="new desc"
            )
            resp = codecommit.get_repository(repositoryName=name)
            assert resp["repositoryMetadata"]["repositoryDescription"] == "new desc"
        finally:
            codecommit.delete_repository(repositoryName=name)

    def test_update_repository_name(self, codecommit):
        """UpdateRepositoryName renames the repository."""
        old_name = _unique("repo")
        new_name = _unique("repo-renamed")
        try:
            codecommit.create_repository(repositoryName=old_name)
            codecommit.update_repository_name(oldName=old_name, newName=new_name)
            resp = codecommit.get_repository(repositoryName=new_name)
            assert resp["repositoryMetadata"]["repositoryName"] == new_name
            with pytest.raises(botocore.exceptions.ClientError):
                codecommit.get_repository(repositoryName=old_name)
        finally:
            codecommit.delete_repository(repositoryName=new_name)

    def test_update_repository_encryption_key(self, codecommit):
        """UpdateRepositoryEncryptionKey completes without error."""
        name = _unique("repo")
        try:
            codecommit.create_repository(repositoryName=name)
            resp = codecommit.update_repository_encryption_key(
                repositoryName=name,
                kmsKeyId="arn:aws:kms:us-east-1:123456789012:key/12345678-1234-1234-1234-123456789012",
            )
            assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200
        finally:
            codecommit.delete_repository(repositoryName=name)


class TestCodeCommitBranchOperations:
    """Tests for CodeCommit branch operations."""

    def test_get_branch(self, codecommit):
        """GetBranch returns branch info for an existing branch."""
        name, commit_id = _create_repo_with_commit(codecommit)
        try:
            resp = codecommit.get_branch(repositoryName=name, branchName="main")
            assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200
            assert resp["branch"]["branchName"] == "main"
            assert resp["branch"]["commitId"] == commit_id
        finally:
            codecommit.delete_repository(repositoryName=name)

    def test_list_branches(self, codecommit):
        """ListBranches returns all branches in a repo."""
        name, main_commit = _create_repo_with_commit(codecommit)
        try:
            codecommit.create_branch(repositoryName=name, branchName="dev", commitId=main_commit)
            resp = codecommit.list_branches(repositoryName=name)
            assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200
            assert "branches" in resp
            assert "main" in resp["branches"]
            assert "dev" in resp["branches"]
        finally:
            codecommit.delete_repository(repositoryName=name)

    def test_list_branches_single(self, codecommit):
        """ListBranches returns single branch for new repo."""
        name, _ = _create_repo_with_commit(codecommit)
        try:
            resp = codecommit.list_branches(repositoryName=name)
            assert len(resp["branches"]) == 1
            assert resp["branches"][0] == "main"
        finally:
            codecommit.delete_repository(repositoryName=name)


class TestCodeCommitFileOperations:
    """Tests for CodeCommit file and folder operations."""

    def test_get_file(self, codecommit):
        """GetFile returns file content from a repo."""
        name, commit_id = _create_repo_with_commit(codecommit)
        try:
            resp = codecommit.get_file(repositoryName=name, filePath="README.md")
            assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200
            assert resp["filePath"] == "README.md"
            assert base64.b64decode(resp["fileContent"]) == b"Hello World"
            assert resp["commitId"] == commit_id
            assert "blobId" in resp
            assert "fileMode" in resp
            assert "fileSize" in resp
        finally:
            codecommit.delete_repository(repositoryName=name)

    def test_get_folder(self, codecommit):
        """GetFolder returns folder contents."""
        name, commit_id = _create_repo_with_commit(codecommit)
        try:
            resp = codecommit.get_folder(repositoryName=name, folderPath="/")
            assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200
            assert resp["commitId"] == commit_id
            assert resp["folderPath"] == "/"
            assert "files" in resp
            file_names = [f["absolutePath"] for f in resp["files"]]
            assert "README.md" in file_names
        finally:
            codecommit.delete_repository(repositoryName=name)

    def test_get_blob(self, codecommit):
        """GetBlob returns blob content by blobId."""
        name, _ = _create_repo_with_commit(codecommit)
        try:
            # First get the blobId from get_file
            file_resp = codecommit.get_file(repositoryName=name, filePath="README.md")
            blob_id = file_resp["blobId"]
            resp = codecommit.get_blob(repositoryName=name, blobId=blob_id)
            assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200
            assert "content" in resp
        finally:
            codecommit.delete_repository(repositoryName=name)


class TestCodeCommitCommitOperations:
    """Tests for CodeCommit commit operations."""

    def test_get_commit(self, codecommit):
        """GetCommit returns commit details."""
        name, commit_id = _create_repo_with_commit(codecommit)
        try:
            resp = codecommit.get_commit(repositoryName=name, commitId=commit_id)
            assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200
            assert resp["commit"]["commitId"] == commit_id
            assert "message" in resp["commit"]
            assert "author" in resp["commit"]
            assert "committer" in resp["commit"]
            assert "treeId" in resp["commit"]
        finally:
            codecommit.delete_repository(repositoryName=name)

    def test_get_differences(self, codecommit):
        """GetDifferences returns changes between commits."""
        name, main_commit, feature_commit = _create_repo_with_feature_branch(codecommit)
        try:
            resp = codecommit.get_differences(
                repositoryName=name,
                beforeCommitSpecifier=main_commit,
                afterCommitSpecifier=feature_commit,
            )
            assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200
            assert "differences" in resp
            assert isinstance(resp["differences"], list)
        finally:
            codecommit.delete_repository(repositoryName=name)

    def test_list_file_commit_history(self, codecommit):
        """ListFileCommitHistory returns commit history for a file."""
        name, commit_id = _create_repo_with_commit(codecommit)
        try:
            resp = codecommit.list_file_commit_history(repositoryName=name, filePath="README.md")
            assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200
            assert "revisionDag" in resp
        finally:
            codecommit.delete_repository(repositoryName=name)


class TestCodeCommitMergeOperations:
    """Tests for CodeCommit merge operations."""

    def test_get_merge_options(self, codecommit):
        """GetMergeOptions returns available merge strategies."""
        name, main_commit, feature_commit = _create_repo_with_feature_branch(codecommit)
        try:
            resp = codecommit.get_merge_options(
                repositoryName=name,
                sourceCommitSpecifier=feature_commit,
                destinationCommitSpecifier=main_commit,
            )
            assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200
            assert "mergeOptions" in resp
            assert isinstance(resp["mergeOptions"], list)
            assert len(resp["mergeOptions"]) > 0
            assert "sourceCommitId" in resp
            assert "destinationCommitId" in resp
            assert "baseCommitId" in resp
        finally:
            codecommit.delete_repository(repositoryName=name)

    def test_get_merge_conflicts(self, codecommit):
        """GetMergeConflicts returns conflict info (no conflicts expected)."""
        name, main_commit, feature_commit = _create_repo_with_feature_branch(codecommit)
        try:
            resp = codecommit.get_merge_conflicts(
                repositoryName=name,
                sourceCommitSpecifier=feature_commit,
                destinationCommitSpecifier=main_commit,
                mergeOption="THREE_WAY_MERGE",
            )
            assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200
            assert "mergeable" in resp
            assert "conflictMetadataList" in resp
            assert "sourceCommitId" in resp
            assert "destinationCommitId" in resp
        finally:
            codecommit.delete_repository(repositoryName=name)

    def test_get_merge_commit(self, codecommit):
        """GetMergeCommit returns merge commit details."""
        name, main_commit, feature_commit = _create_repo_with_feature_branch(codecommit)
        try:
            resp = codecommit.get_merge_commit(
                repositoryName=name,
                sourceCommitSpecifier=feature_commit,
                destinationCommitSpecifier=main_commit,
            )
            assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200
            assert "sourceCommitId" in resp
            assert "destinationCommitId" in resp
            assert "baseCommitId" in resp
        finally:
            codecommit.delete_repository(repositoryName=name)

    def test_describe_merge_conflicts(self, codecommit):
        """DescribeMergeConflicts returns conflict details (empty when no conflicts)."""
        name, main_commit, feature_commit = _create_repo_with_feature_branch(codecommit)
        try:
            resp = codecommit.describe_merge_conflicts(
                repositoryName=name,
                sourceCommitSpecifier=feature_commit,
                destinationCommitSpecifier=main_commit,
                mergeOption="THREE_WAY_MERGE",
                filePath="feature.txt",
            )
            assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200
            assert "conflictMetadata" in resp
            assert "mergeHunks" in resp
        finally:
            codecommit.delete_repository(repositoryName=name)


class TestCodeCommitPullRequestOperations:
    """Tests for CodeCommit pull request operations."""

    def test_create_and_get_pull_request(self, codecommit):
        """Create a PR and retrieve it with GetPullRequest."""
        name, main_commit, feature_commit = _create_repo_with_feature_branch(codecommit)
        try:
            create_resp = codecommit.create_pull_request(
                title="Test PR",
                description="A test pull request",
                targets=[
                    {
                        "repositoryName": name,
                        "sourceReference": "feature",
                        "destinationReference": "main",
                    }
                ],
            )
            pr_id = create_resp["pullRequest"]["pullRequestId"]
            resp = codecommit.get_pull_request(pullRequestId=pr_id)
            assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200
            assert resp["pullRequest"]["pullRequestId"] == pr_id
            assert resp["pullRequest"]["title"] == "Test PR"
            assert "pullRequestStatus" in resp["pullRequest"]
            assert "pullRequestTargets" in resp["pullRequest"]
        finally:
            codecommit.delete_repository(repositoryName=name)

    def test_list_pull_requests(self, codecommit):
        """ListPullRequests returns PRs for a repo."""
        name, main_commit, feature_commit = _create_repo_with_feature_branch(codecommit)
        try:
            codecommit.create_pull_request(
                title="PR for list",
                targets=[
                    {
                        "repositoryName": name,
                        "sourceReference": "feature",
                        "destinationReference": "main",
                    }
                ],
            )
            resp = codecommit.list_pull_requests(repositoryName=name)
            assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200
            assert "pullRequestIds" in resp
            assert len(resp["pullRequestIds"]) >= 1
        finally:
            codecommit.delete_repository(repositoryName=name)

    def test_list_pull_requests_empty(self, codecommit):
        """ListPullRequests returns empty list for repo with no PRs."""
        name, _ = _create_repo_with_commit(codecommit)
        try:
            resp = codecommit.list_pull_requests(repositoryName=name)
            assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200
            assert "pullRequestIds" in resp
            assert resp["pullRequestIds"] == []
        finally:
            codecommit.delete_repository(repositoryName=name)

    def test_describe_pull_request_events(self, codecommit):
        """DescribePullRequestEvents returns events for a PR."""
        name, main_commit, feature_commit = _create_repo_with_feature_branch(codecommit)
        try:
            create_resp = codecommit.create_pull_request(
                title="PR events test",
                targets=[
                    {
                        "repositoryName": name,
                        "sourceReference": "feature",
                        "destinationReference": "main",
                    }
                ],
            )
            pr_id = create_resp["pullRequest"]["pullRequestId"]
            resp = codecommit.describe_pull_request_events(pullRequestId=pr_id)
            assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200
            assert "pullRequestEvents" in resp
            assert len(resp["pullRequestEvents"]) >= 1
        finally:
            codecommit.delete_repository(repositoryName=name)


class TestCodeCommitCommentOperations:
    """Tests for CodeCommit comment operations."""

    def test_get_comment(self, codecommit):
        """GetComment returns a comment by ID."""
        name, commit_id = _create_repo_with_commit(codecommit)
        try:
            comment_resp = codecommit.post_comment_for_compared_commit(
                repositoryName=name,
                afterCommitId=commit_id,
                content="test comment",
            )
            comment_id = comment_resp["comment"]["commentId"]
            resp = codecommit.get_comment(commentId=comment_id)
            assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200
            assert resp["comment"]["commentId"] == comment_id
            assert resp["comment"]["content"] == "test comment"
            assert "authorArn" in resp["comment"]
            assert "creationDate" in resp["comment"]
        finally:
            codecommit.delete_repository(repositoryName=name)

    def test_get_comment_reactions(self, codecommit):
        """GetCommentReactions returns reactions for a comment."""
        name, commit_id = _create_repo_with_commit(codecommit)
        try:
            comment_resp = codecommit.post_comment_for_compared_commit(
                repositoryName=name,
                afterCommitId=commit_id,
                content="reaction test",
            )
            comment_id = comment_resp["comment"]["commentId"]
            resp = codecommit.get_comment_reactions(commentId=comment_id)
            assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200
            assert "reactionsForComment" in resp
            assert isinstance(resp["reactionsForComment"], list)
        finally:
            codecommit.delete_repository(repositoryName=name)

    def test_get_comments_for_compared_commit(self, codecommit):
        """GetCommentsForComparedCommit returns comments for commit comparison."""
        name, main_commit, feature_commit = _create_repo_with_feature_branch(codecommit)
        try:
            resp = codecommit.get_comments_for_compared_commit(
                repositoryName=name,
                afterCommitId=feature_commit,
            )
            assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200
            assert "commentsForComparedCommitData" in resp
        finally:
            codecommit.delete_repository(repositoryName=name)

    def test_get_comments_for_pull_request(self, codecommit):
        """GetCommentsForPullRequest returns comments for a PR."""
        name, main_commit, feature_commit = _create_repo_with_feature_branch(codecommit)
        try:
            create_resp = codecommit.create_pull_request(
                title="PR comments test",
                targets=[
                    {
                        "repositoryName": name,
                        "sourceReference": "feature",
                        "destinationReference": "main",
                    }
                ],
            )
            pr_id = create_resp["pullRequest"]["pullRequestId"]
            resp = codecommit.get_comments_for_pull_request(pullRequestId=pr_id)
            assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200
            assert "commentsForPullRequestData" in resp
        finally:
            codecommit.delete_repository(repositoryName=name)


class TestCodeCommitApprovalRuleTemplates:
    """Tests for CodeCommit approval rule template operations."""

    def test_list_approval_rule_templates_empty(self, codecommit):
        """ListApprovalRuleTemplates returns list (possibly empty)."""
        resp = codecommit.list_approval_rule_templates()
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200
        assert "approvalRuleTemplateNames" in resp

    def test_create_and_get_approval_rule_template(self, codecommit):
        """Create and retrieve an approval rule template."""
        template_name = _unique("template")
        try:
            codecommit.create_approval_rule_template(
                approvalRuleTemplateName=template_name,
                approvalRuleTemplateContent=APPROVAL_RULE_CONTENT,
            )
            resp = codecommit.get_approval_rule_template(approvalRuleTemplateName=template_name)
            assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200
            assert resp["approvalRuleTemplate"]["approvalRuleTemplateName"] == template_name
            assert "approvalRuleTemplateId" in resp["approvalRuleTemplate"]
            assert "approvalRuleTemplateContent" in resp["approvalRuleTemplate"]
        finally:
            codecommit.delete_approval_rule_template(approvalRuleTemplateName=template_name)

    def test_list_approval_rule_templates_after_create(self, codecommit):
        """ListApprovalRuleTemplates includes created template."""
        template_name = _unique("template")
        try:
            codecommit.create_approval_rule_template(
                approvalRuleTemplateName=template_name,
                approvalRuleTemplateContent=APPROVAL_RULE_CONTENT,
            )
            resp = codecommit.list_approval_rule_templates()
            assert template_name in resp["approvalRuleTemplateNames"]
        finally:
            codecommit.delete_approval_rule_template(approvalRuleTemplateName=template_name)

    def test_list_associated_approval_rule_templates_for_repository(self, codecommit):
        """ListAssociatedApprovalRuleTemplatesForRepository returns associated templates."""
        name = _unique("repo")
        template_name = _unique("template")
        try:
            codecommit.create_repository(repositoryName=name)
            codecommit.create_approval_rule_template(
                approvalRuleTemplateName=template_name,
                approvalRuleTemplateContent=APPROVAL_RULE_CONTENT,
            )
            codecommit.associate_approval_rule_template_with_repository(
                approvalRuleTemplateName=template_name,
                repositoryName=name,
            )
            resp = codecommit.list_associated_approval_rule_templates_for_repository(
                repositoryName=name
            )
            assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200
            assert "approvalRuleTemplateNames" in resp
            assert template_name in resp["approvalRuleTemplateNames"]
        finally:
            codecommit.delete_repository(repositoryName=name)
            codecommit.delete_approval_rule_template(approvalRuleTemplateName=template_name)

    def test_list_repositories_for_approval_rule_template(self, codecommit):
        """ListRepositoriesForApprovalRuleTemplate returns associated repos."""
        name = _unique("repo")
        template_name = _unique("template")
        try:
            codecommit.create_repository(repositoryName=name)
            codecommit.create_approval_rule_template(
                approvalRuleTemplateName=template_name,
                approvalRuleTemplateContent=APPROVAL_RULE_CONTENT,
            )
            codecommit.associate_approval_rule_template_with_repository(
                approvalRuleTemplateName=template_name,
                repositoryName=name,
            )
            resp = codecommit.list_repositories_for_approval_rule_template(
                approvalRuleTemplateName=template_name
            )
            assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200
            assert "repositoryNames" in resp
            assert name in resp["repositoryNames"]
        finally:
            codecommit.delete_repository(repositoryName=name)
            codecommit.delete_approval_rule_template(approvalRuleTemplateName=template_name)


class TestCodeCommitTriggerOperations:
    """Tests for CodeCommit repository trigger operations."""

    def test_get_repository_triggers_empty(self, codecommit):
        """GetRepositoryTriggers returns empty triggers list."""
        name, _ = _create_repo_with_commit(codecommit)
        try:
            resp = codecommit.get_repository_triggers(repositoryName=name)
            assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200
            assert "triggers" in resp
            assert isinstance(resp["triggers"], list)
        finally:
            codecommit.delete_repository(repositoryName=name)


class TestCodeCommitTagOperations:
    """Tests for CodeCommit tag operations."""

    def test_list_tags_for_resource(self, codecommit):
        """ListTagsForResource returns tags for a repo."""
        name = _unique("repo")
        try:
            resp = codecommit.create_repository(repositoryName=name)
            arn = resp["repositoryMetadata"]["Arn"]
            tag_resp = codecommit.list_tags_for_resource(resourceArn=arn)
            assert tag_resp["ResponseMetadata"]["HTTPStatusCode"] == 200
            assert "tags" in tag_resp
        finally:
            codecommit.delete_repository(repositoryName=name)

    def test_list_tags_for_resource_with_tags(self, codecommit):
        """ListTagsForResource returns tags that were set."""
        name = _unique("repo")
        try:
            resp = codecommit.create_repository(
                repositoryName=name,
                tags={"env": "test", "project": "compat"},
            )
            arn = resp["repositoryMetadata"]["Arn"]
            tag_resp = codecommit.list_tags_for_resource(resourceArn=arn)
            assert tag_resp["tags"]["env"] == "test"
            assert tag_resp["tags"]["project"] == "compat"
        finally:
            codecommit.delete_repository(repositoryName=name)
