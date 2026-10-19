//! AWS Glacier backend — port of moto's `glacier` engine (vaults, archives,
//! jobs, vault policies/notifications/lock, data-retrieval policy, provisioned
//! capacity and multipart uploads).
//!
//! Glacier is REST with JSON bodies: 201/202/204 responses carry empty bodies
//! while result members come back via x-amz-* response headers (archiveId,
//! checksum, jobId, uploadId, lockId, capacityId). Archive ids are md5(body)
//! and tree hashes sha256 hex, both like moto. Vault ARNs use the account and
//! region of the request. All state is (account, region) scoped.

use crate::store;
use crate::{Request, Response};
use once_cell::sync::Lazy;
use serde_json::{json, Map, Value};
use sha2::{Digest, Sha256};
use std::collections::HashMap;

static SLOT: Lazy<store::Slot<GlacierState>> = Lazy::new(|| store::slot::<GlacierState>("glacier"));

// Tier-driven job ready delays (moto Job.__init__).
const STANDARD_READY_SECS: i64 = 5;
const EXPEDITED_READY_SECS: i64 = 2;
const BULK_READY_SECS: i64 = 10;

#[derive(Default)]
struct GlacierState {
    vaults: HashMap<String, Vault>,
    data_retrieval_policy: Option<Value>,
    provisioned_capacity: Vec<Value>,
    region: String,
    account: String,
}

struct Vault {
    name: String,
    arn: String,
    creation_date: String,
    archives: HashMap<String, Archive>,
    jobs: HashMap<String, Job>,
    tags: Map<String, Value>,
    notification_configuration: Option<Value>,
    access_policy: Option<String>,
    vault_lock: Option<Value>,
    multipart_uploads: HashMap<String, MultipartUpload>,
}

struct Archive {
    archive_id: String,
    body: Vec<u8>,
    size: usize,
    sha256: String,
    creation_date: String,
    description: String,
}

struct Job {
    job_id: String,
    action: &'static str,
    tier: String,
    vault_arn: String,
    archive_id: Option<String>,
    created_at: chrono::DateTime<chrono::Utc>,
    completion_at: chrono::DateTime<chrono::Utc>,
}

#[derive(Default)]
struct MultipartUpload {
    archive_description: String,
    creation_date: String,
    part_size: i64,
    /// Range header -> body, keyed exactly like moto's part store.
    parts: HashMap<String, Vec<u8>>,
}

fn now_stamp() -> String {
    chrono::Utc::now()
        .format("%Y-%m-%dT%H:%M:%S.000Z")
        .to_string()
}

fn fmt_stamp(t: chrono::DateTime<chrono::Utc>) -> String {
    t.format("%Y-%m-%dT%H:%M:%S.000Z").to_string()
}

fn in_seconds(secs: i64) -> chrono::DateTime<chrono::Utc> {
    chrono::Utc::now() + chrono::Duration::seconds(secs)
}

fn vault_not_found() -> Response {
    crate::proto::json_error(404, "VaultNotFound", "Vault not found")
}

fn resource_not_found(message: &str) -> Response {
    crate::proto::json_error(404, "ResourceNotFoundException", message)
}

fn headers_response(status: u16, headers: Vec<(&str, String)>) -> Response {
    Response {
        status,
        headers: headers
            .into_iter()
            .map(|(k, v)| (k.to_string(), v))
            .collect(),
        body: Vec::new(),
    }
}

/// Hexadecimal md5 digest helper (same as the s3 crate's convention).
fn md5_hex(data: &[u8]) -> String {
    format!("{:x}", md5::compute(data))
}

/// 92 uppercase alphanumeric characters, matching moto's get_job_id length.
fn new_job_id() -> String {
    let mut out = String::new();
    while out.len() < 92 {
        out.push_str(&uuid::Uuid::new_v4().simple().to_string().to_uppercase());
    }
    out.truncate(92);
    out
}

fn job_ready(job: &Job) -> bool {
    chrono::Utc::now() >= job.completion_at
}

fn job_to_dict(job: &Job) -> Value {
    let mut d = json!({
        "Action": job.action,
        "ArchiveId": job.archive_id,
        "ArchiveSizeInBytes": 0,
        "ArchiveSHA256TreeHash": Value::Null,
        "Completed": false,
        "CreationDate": fmt_stamp(job.created_at),
        "InventorySizeInBytes": 0,
        "JobDescription": Value::Null,
        "JobId": job.job_id,
        "RetrievalByteRange": Value::Null,
        "SHA256TreeHash": Value::Null,
        "SNSTopic": Value::Null,
        "StatusCode": "InProgress",
        "StatusMessage": Value::Null,
        "VaultARN": job.vault_arn,
        "Tier": job.tier,
    });
    if job_ready(job) {
        d["Completed"] = json!(true);
        d["CompletionDate"] = json!(fmt_stamp(job.completion_at));
        d["InventorySizeInBytes"] = json!(10000);
        d["StatusCode"] = json!("Succeeded");
    }
    d
}

fn archive_list_item(archive: &Archive) -> Value {
    json!({
        "ArchiveId": archive.archive_id,
        "ArchiveDescription": archive.description,
        "CreationDate": archive.creation_date,
        "Size": archive.size,
        "SHA256TreeHash": archive.sha256,
    })
}

fn vault_to_dict(vault: &Vault) -> Value {
    json!({
        "CreationDate": vault.creation_date,
        "LastInventoryDate": vault.creation_date,
        "NumberOfArchives": vault.archives.len(),
        "SizeInBytes": vault.archives.values().map(|a| a.size).sum::<usize>(),
        "VaultARN": vault.arn,
        "VaultName": vault.name,
    })
}

fn upload_to_dict(upload: &MultipartUpload, upload_id: &str) -> Value {
    json!({
        "ArchiveDescription": upload.archive_description,
        "CreationDate": upload.creation_date,
        "MultipartUploadId": upload_id,
        "PartSizeInBytes": upload.part_size,
        "VaultARN": Value::Null,
    })
}

pub fn dispatch(req: &Request) -> Response {
    let mut guard = SLOT.write();
    let st = guard.entry((req.account, req.region.clone())).or_default();
    if st.region.is_empty() {
        st.region = req.region.clone();
        st.account = req.account.to_string();
    }

    let path = req.path.trim_matches('/');
    let segments: Vec<&str> = path.split('/').collect();
    let method = req.method.as_str();
    let operation = req
        .query_string
        .split('&')
        .find_map(|kv| kv.strip_prefix("operation="))
        .unwrap_or("");
    // Upload endpoints carry raw bytes; everything else is a JSON body.
    let is_binary = matches!(
        (segments.len(), segments.get(4)),
        (5, Some(&"archives")) | (5, Some(&"multipart-uploads")) | (6, Some(&"multipart-uploads"))
    );
    let params = if !is_binary && !req.body.is_empty() {
        crate::proto::parse_json_body(&req.body)
    } else {
        json!({})
    };

    // /{accountId}/policies/data-retrieval
    if segments.len() == 3 && segments[1] == "policies" && segments[2] == "data-retrieval" {
        return match method {
            "GET" => st.get_data_retrieval_policy(),
            "PUT" => st.set_data_retrieval_policy(&params),
            _ => unimplemented(req),
        };
    }
    // /{accountId}/provisioned-capacity
    if segments.len() == 2 && segments[1] == "provisioned-capacity" {
        return match method {
            "GET" => st.list_provisioned_capacity(),
            "POST" => st.purchase_provisioned_capacity(),
            _ => unimplemented(req),
        };
    }
    // /{accountId}/vaults[...] — segment layout: [account, "vaults", ...]
    if segments.len() < 2 || segments[1] != "vaults" {
        return unimplemented(req);
    }
    let vault_name = segments.get(2).copied().unwrap_or("");
    match (segments.len(), method) {
        (2, "GET") => st.list_vaults(),
        (3, "PUT") => st.create_vault(vault_name),
        (3, "DELETE") => st.delete_vault(vault_name),
        (3, "GET") => st.describe_vault(vault_name),
        (4, "GET") => match segments[3] {
            "tags" => st.list_tags_for_vault(vault_name),
            "notification-configuration" => st.get_vault_notifications(vault_name),
            "access-policy" => st.get_vault_access_policy(vault_name),
            "lock-policy" => st.get_vault_lock(vault_name),
            "jobs" => st.list_jobs(vault_name),
            "multipart-uploads" => st.list_multipart_uploads(vault_name),
            _ => unimplemented(req),
        },
        (4, "POST") => match segments[3] {
            "tags" => match operation {
                "add" => st.add_tags_to_vault(vault_name, &params),
                "remove" => st.remove_tags_from_vault(vault_name, &params),
                _ => unimplemented(req),
            },
            "archives" => st.upload_archive(req, vault_name),
            "jobs" => st.initiate_job(req, vault_name, &params),
            "multipart-uploads" => st.initiate_multipart_upload(req, vault_name),
            "lock-policy" => st.initiate_vault_lock(vault_name, &params),
            _ => unimplemented(req),
        },
        (4, "PUT") => match segments[3] {
            "notification-configuration" => st.set_vault_notifications(vault_name, &params),
            "access-policy" => st.set_vault_access_policy(vault_name, &params),
            _ => unimplemented(req),
        },
        (4, "DELETE") => match segments[3] {
            "notification-configuration" => st.delete_vault_notifications(vault_name),
            "access-policy" => st.delete_vault_access_policy(vault_name),
            "lock-policy" => st.abort_vault_lock(vault_name),
            _ => unimplemented(req),
        },
        (5, "DELETE") => match segments[3] {
            "archives" => st.delete_archive(vault_name, segments[4]),
            "multipart-uploads" => st.abort_multipart_upload(vault_name, segments[4]),
            _ => unimplemented(req),
        },
        (5, "GET") => match segments[3] {
            "jobs" => st.describe_job(vault_name, segments[4]),
            "lock-policy" => st.get_vault_lock(vault_name),
            "multipart-uploads" => st.list_parts(vault_name, segments[4]),
            _ => unimplemented(req),
        },
        (5, "PUT") if segments[3] == "multipart-uploads" => {
            st.upload_multipart_part(vault_name, segments[4], req)
        }
        (5, "POST") => match segments[3] {
            "multipart-uploads" => st.complete_multipart_upload(vault_name, segments[4]),
            "lock-policy" => st.complete_vault_lock(vault_name, segments[4]),
            _ => unimplemented(req),
        },
        (6, "GET") if segments[3] == "jobs" && segments[5] == "output" => {
            st.get_job_output(vault_name, segments[4])
        }
        _ => unimplemented(req),
    }
}

fn unimplemented(req: &Request) -> Response {
    crate::proto::json_error(
        501,
        "NotImplemented",
        &format!("The {} action has not been implemented", req.operation),
    )
}

impl GlacierState {
    fn vault_mut(&mut self, name: &str) -> Option<&mut Vault> {
        self.vaults.get_mut(name)
    }

    fn has_vault(&self, name: &str) -> bool {
        self.vaults.contains_key(name)
    }

    fn list_vaults(&self) -> Response {
        let vaults: Vec<Value> = self.vaults.values().map(vault_to_dict).collect();
        Response::json_status(200, &json!({"Marker": Value::Null, "VaultList": vaults}))
    }

    fn create_vault(&mut self, name: &str) -> Response {
        if !self.vaults.contains_key(name) {
            self.vaults.insert(
                name.to_string(),
                Vault {
                    name: name.to_string(),
                    arn: format!(
                        "arn:aws:glacier:{region}:{account}:vaults/{name}",
                        region = self.region,
                        account = self.account
                    ),
                    creation_date: now_stamp(),
                    ..Vault::default()
                },
            );
        }
        Response::new(201, "application/json", "")
    }

    fn delete_vault(&mut self, name: &str) -> Response {
        self.vaults.remove(name);
        Response::new(204, "application/json", "")
    }

    fn describe_vault(&self, name: &str) -> Response {
        match self.vaults.get(name) {
            Some(vault) => Response::json_status(200, &vault_to_dict(vault)),
            None => vault_not_found(),
        }
    }

    fn upload_archive(&mut self, req: &Request, name: &str) -> Response {
        let Some(vault) = self.vault_mut(name) else {
            return vault_not_found();
        };
        let body = req.body.to_vec();
        let archive_id = md5_hex(&body);
        let sha256 = format!("{:x}", Sha256::digest(&body));
        let size = body.len();
        let description = req
            .headers
            .get("x-amz-archive-description")
            .cloned()
            .unwrap_or_default();
        vault.archives.insert(
            archive_id.clone(),
            Archive {
                archive_id: archive_id.clone(),
                body,
                size,
                sha256: sha256.clone(),
                creation_date: now_stamp(),
                description,
            },
        );
        headers_response(
            201,
            vec![
                ("x-amz-archive-id", archive_id),
                ("x-amz-sha256-tree-hash", sha256),
            ],
        )
    }

    fn delete_archive(&mut self, name: &str, archive_id: &str) -> Response {
        match self.vault_mut(name) {
            None => vault_not_found(),
            Some(vault) => {
                if vault.archives.remove(archive_id).is_none() {
                    return resource_not_found(&format!("Archive not found: {archive_id}"));
                }
                Response::new(204, "application/json", "")
            }
        }
    }

    fn list_jobs(&self, name: &str) -> Response {
        match self.vaults.get(name) {
            Some(vault) => {
                let jobs: Vec<Value> = vault.jobs.values().map(job_to_dict).collect();
                Response::json_status(200, &json!({"JobList": jobs, "Marker": Value::Null}))
            }
            None => vault_not_found(),
        }
    }

    fn initiate_job(&mut self, req: &Request, name: &str, params: &Value) -> Response {
        if !self.has_vault(name) {
            return vault_not_found();
        }
        let tier = params
            .get("Tier")
            .and_then(Value::as_str)
            .unwrap_or("Standard")
            .to_string();
        let job_type = params
            .get("Type")
            .and_then(Value::as_str)
            .unwrap_or("inventory-retrieval");
        let job_id = new_job_id();
        let secs = match tier.to_ascii_lowercase().as_str() {
            "expedited" => EXPEDITED_READY_SECS,
            "bulk" => BULK_READY_SECS,
            _ => STANDARD_READY_SECS,
        };
        let vault = self.vaults.get_mut(name).unwrap();
        vault.jobs.insert(
            job_id.clone(),
            Job {
                job_id: job_id.clone(),
                action: if job_type == "archive-retrieval" {
                    "ArchiveRetrieval"
                } else {
                    "InventoryRetrieval"
                },
                tier: tier.clone(),
                vault_arn: vault.arn.clone(),
                archive_id: params
                    .get("ArchiveId")
                    .and_then(Value::as_str)
                    .map(String::from),
                created_at: chrono::Utc::now(),
                completion_at: in_seconds(secs),
            },
        );
        let account = req.path.trim_matches('/').split('/').next().unwrap_or("");
        headers_response(
            202,
            vec![
                ("x-amz-job-id", job_id.clone()),
                (
                    "Location",
                    format!("/{account}/vaults/{name}/jobs/{job_id}"),
                ),
            ],
        )
    }

    fn describe_job(&self, name: &str, job_id: &str) -> Response {
        match self.vaults.get(name) {
            None => vault_not_found(),
            Some(vault) => match vault.jobs.get(job_id) {
                Some(job) => Response::json_status(200, &job_to_dict(job)),
                None => crate::proto::json_error(404, "ResourceNotFoundException", "Job not found"),
            },
        }
    }

    fn get_job_output(&self, name: &str, job_id: &str) -> Response {
        let Some(vault) = self.vaults.get(name) else {
            return vault_not_found();
        };
        let Some(job) = vault.jobs.get(job_id) else {
            return Response::new(404, "text/plain", "404 Not Found");
        };
        if !job_ready(job) {
            return Response::new(404, "text/plain", "404 Not Found");
        }
        if job.action == "InventoryRetrieval" {
            let archive_list: Vec<Value> = vault.archives.values().map(archive_list_item).collect();
            Response::json_status(
                200,
                &json!({
                    "VaultARN": vault.arn,
                    "InventoryDate": fmt_stamp(job.completion_at),
                    "ArchiveList": archive_list,
                }),
            )
        } else {
            match job
                .archive_id
                .as_ref()
                .and_then(|id| vault.archives.get(id))
            {
                Some(archive) => Response::new(
                    200,
                    "application/octet-stream",
                    String::from_utf8_lossy(&archive.body).into_owned(),
                ),
                None => resource_not_found("Archive not found"),
            }
        }
    }

    // --- data retrieval policy ---

    fn get_data_retrieval_policy(&self) -> Response {
        let policy = self
            .data_retrieval_policy
            .clone()
            .unwrap_or_else(|| json!({"Rules": [{"Strategy": "FreeTier"}]}));
        Response::json_status(200, &json!({"Policy": policy}))
    }

    fn set_data_retrieval_policy(&mut self, params: &Value) -> Response {
        self.data_retrieval_policy = Some(params.get("Policy").cloned().unwrap_or(json!({})));
        Response::new(204, "application/json", "")
    }

    // --- tags ---

    fn add_tags_to_vault(&mut self, name: &str, params: &Value) -> Response {
        let Some(vault) = self.vault_mut(name) else {
            return vault_not_found();
        };
        if let Some(tags) = params.get("Tags").and_then(Value::as_object) {
            for (k, v) in tags {
                vault.tags.insert(k.clone(), v.clone());
            }
        }
        Response::new(204, "application/json", "")
    }

    fn remove_tags_from_vault(&mut self, name: &str, params: &Value) -> Response {
        let Some(vault) = self.vault_mut(name) else {
            return vault_not_found();
        };
        if let Some(keys) = params.get("TagKeys").and_then(Value::as_array) {
            for key in keys.iter().filter_map(Value::as_str) {
                vault.tags.remove(key);
            }
        }
        Response::new(204, "application/json", "")
    }

    fn list_tags_for_vault(&self, name: &str) -> Response {
        match self.vaults.get(name) {
            Some(vault) => Response::json_status(200, &json!({"Tags": vault.tags})),
            None => vault_not_found(),
        }
    }

    // --- notifications ---

    fn set_vault_notifications(&mut self, name: &str, params: &Value) -> Response {
        let Some(vault) = self.vault_mut(name) else {
            return vault_not_found();
        };
        vault.notification_configuration = Some(params.clone());
        Response::new(204, "application/json", "")
    }

    fn get_vault_notifications(&self, name: &str) -> Response {
        match self
            .vaults
            .get(name)
            .and_then(|v| v.notification_configuration.clone())
        {
            Some(config) => Response::json_status(200, &config),
            None => resource_not_found("No notification configuration is set for vault"),
        }
    }

    fn delete_vault_notifications(&mut self, name: &str) -> Response {
        if let Some(vault) = self.vault_mut(name) {
            vault.notification_configuration = None;
        }
        Response::new(204, "application/json", "")
    }

    // --- access policy ---

    fn set_vault_access_policy(&mut self, name: &str, params: &Value) -> Response {
        let Some(vault) = self.vault_mut(name) else {
            return vault_not_found();
        };
        vault.access_policy = Some(
            params
                .get("Policy")
                .and_then(Value::as_str)
                .unwrap_or("")
                .to_string(),
        );
        Response::new(204, "application/json", "")
    }

    fn get_vault_access_policy(&self, name: &str) -> Response {
        match self.vaults.get(name).and_then(|v| v.access_policy.clone()) {
            Some(policy) => Response::json_status(200, &json!({"Policy": policy})),
            None => resource_not_found("No access policy is set for vault"),
        }
    }

    fn delete_vault_access_policy(&mut self, name: &str) -> Response {
        if let Some(vault) = self.vault_mut(name) {
            vault.access_policy = None;
        }
        Response::new(204, "application/json", "")
    }

    // --- vault lock ---

    fn initiate_vault_lock(&mut self, name: &str, params: &Value) -> Response {
        let lock_id = uuid::Uuid::new_v4().to_string();
        let Some(vault) = self.vault_mut(name) else {
            return vault_not_found();
        };
        vault.vault_lock = Some(json!({
            "Policy": params.get("Policy").cloned().unwrap_or(json!("")),
            "State": "InProgress",
            "LockId": lock_id,
            "CreationDate": now_stamp(),
            "ExpirationDate": fmt_stamp(in_seconds(24 * 3600)),
        }));
        headers_response(201, vec![("x-amz-lock-id", lock_id)])
    }

    fn get_vault_lock(&self, name: &str) -> Response {
        let Some(vault) = self.vaults.get(name) else {
            return vault_not_found();
        };
        let Some(lock) = &vault.vault_lock else {
            return resource_not_found("No vault lock policy is set for vault");
        };
        let mut out = json!({
            "Policy": lock.get("Policy").cloned().unwrap_or(json!("")),
            "State": lock.get("State").cloned().unwrap_or(json!("")),
            "CreationDate": lock.get("CreationDate").cloned().unwrap_or(json!("")),
        });
        if let Some(exp) = lock.get("ExpirationDate") {
            out["ExpirationDate"] = exp.clone();
        }
        Response::json_status(200, &out)
    }

    fn complete_vault_lock(&mut self, name: &str, lock_id: &str) -> Response {
        let Some(vault) = self.vault_mut(name) else {
            return vault_not_found();
        };
        match &mut vault.vault_lock {
            Some(lock) if lock.get("LockId").and_then(Value::as_str) == Some(lock_id) => {
                lock["State"] = json!("Locked");
                if let Some(map) = lock.as_object_mut() {
                    map.remove("ExpirationDate");
                }
                Response::new(204, "application/json", "")
            }
            _ => resource_not_found("Vault lock not found"),
        }
    }

    fn abort_vault_lock(&mut self, name: &str) -> Response {
        if let Some(vault) = self.vault_mut(name) {
            vault.vault_lock = None;
        }
        Response::new(204, "application/json", "")
    }

    // --- provisioned capacity ---

    fn list_provisioned_capacity(&self) -> Response {
        Response::json_status(
            200,
            &json!({"ProvisionedCapacityList": self.provisioned_capacity}),
        )
    }

    /// Moto appends the PascalCase capacity record and answers with the
    /// camelCase mirror through the x-amz-capacity-id response header.
    fn purchase_provisioned_capacity(&mut self) -> Response {
        let cap_id = uuid::Uuid::new_v4().to_string();
        self.provisioned_capacity.push(json!({
            "CapacityId": cap_id,
            "StartDate": now_stamp(),
            "ExpirationDate": fmt_stamp(in_seconds(30 * 24 * 3600)),
        }));
        headers_response(201, vec![("x-amz-capacity-id", cap_id)])
    }

    // --- multipart uploads ---

    fn initiate_multipart_upload(&mut self, req: &Request, name: &str) -> Response {
        if !self.has_vault(name) {
            return vault_not_found();
        }
        let upload_id = uuid::Uuid::new_v4().to_string();
        let part_size = req
            .headers
            .get("x-amz-part-size")
            .and_then(|v| v.parse::<i64>().ok())
            .unwrap_or(1048576);
        let vault = self.vaults.get_mut(name).unwrap();
        vault.multipart_uploads.insert(
            upload_id.clone(),
            MultipartUpload {
                archive_description: req
                    .headers
                    .get("x-amz-archive-description")
                    .cloned()
                    .unwrap_or_default(),
                creation_date: now_stamp(),
                part_size,
                parts: HashMap::new(),
            },
        );
        let account = req.path.trim_matches('/').split('/').next().unwrap_or("");
        headers_response(
            201,
            vec![
                ("x-amz-multipart-upload-id", upload_id.clone()),
                (
                    "Location",
                    format!("/{account}/vaults/{name}/multipart-uploads/{upload_id}"),
                ),
            ],
        )
    }

    fn upload_multipart_part(&mut self, name: &str, upload_id: &str, req: &Request) -> Response {
        let Some(vault) = self.vaults.get_mut(name) else {
            return vault_not_found();
        };
        let Some(upload) = vault.multipart_uploads.get_mut(upload_id) else {
            return resource_not_found(&format!("Upload not found: {upload_id}"));
        };
        let range = req
            .headers
            .get("content-range")
            .cloned()
            .unwrap_or_default();
        let body = req.body.to_vec();
        let sha256 = format!("{:x}", Sha256::digest(&body));
        upload.parts.insert(range, body);
        headers_response(204, vec![("x-amz-sha256-tree-hash", sha256)])
    }

    fn complete_multipart_upload(&mut self, name: &str, upload_id: &str) -> Response {
        let Some(vault) = self.vaults.get_mut(name) else {
            return vault_not_found();
        };
        let Some(upload) = vault.multipart_uploads.remove(upload_id) else {
            return resource_not_found(&format!("Upload not found: {upload_id}"));
        };
        // Parts are joined in range-header order, mirroring moto.
        let mut ranges: Vec<&String> = upload.parts.keys().collect();
        ranges.sort();
        let mut body: Vec<u8> = Vec::new();
        for range in ranges {
            body.extend_from_slice(&upload.parts[range]);
        }
        let sha256 = format!("{:x}", Sha256::digest(&body));
        let archive_id = md5_hex(&body);
        let size = body.len();
        vault.archives.insert(
            archive_id.clone(),
            Archive {
                archive_id: archive_id.clone(),
                body,
                size,
                sha256: sha256.clone(),
                creation_date: now_stamp(),
                description: upload.archive_description.clone(),
            },
        );
        headers_response(
            201,
            vec![
                ("x-amz-archive-id", archive_id),
                ("x-amz-sha256-tree-hash", sha256),
            ],
        )
    }

    fn abort_multipart_upload(&mut self, name: &str, upload_id: &str) -> Response {
        match self.vault_mut(name) {
            None => vault_not_found(),
            Some(vault) => {
                if vault.multipart_uploads.remove(upload_id).is_none() {
                    return resource_not_found(&format!("Upload not found: {upload_id}"));
                }
                Response::new(204, "application/json", "")
            }
        }
    }

    fn list_multipart_uploads(&self, name: &str) -> Response {
        match self.vaults.get(name) {
            Some(vault) => {
                let uploads: Vec<Value> = vault
                    .multipart_uploads
                    .iter()
                    .map(|(id, upload)| upload_to_dict(upload, id))
                    .collect();
                Response::json_status(200, &json!({"Marker": Value::Null, "UploadsList": uploads}))
            }
            None => vault_not_found(),
        }
    }

    fn list_parts(&self, name: &str, upload_id: &str) -> Response {
        let Some(vault) = self.vaults.get(name) else {
            return vault_not_found();
        };
        let Some(upload) = vault.multipart_uploads.get(upload_id) else {
            return resource_not_found(&format!("Upload not found: {upload_id}"));
        };
        let mut ranges: Vec<&String> = upload.parts.keys().collect();
        ranges.sort();
        let parts: Vec<Value> = ranges
            .iter()
            .map(|range| {
                json!({
                    "RangeInBytes": range,
                    "SHA256TreeHash": format!("{:x}", Sha256::digest(&upload.parts[*range])),
                })
            })
            .collect();
        Response::json_status(
            200,
            &json!({
                "ArchiveDescription": upload.archive_description,
                "CreationDate": upload.creation_date,
                "Marker": Value::Null,
                "MultipartUploadId": upload_id,
                "PartSizeInBytes": upload.part_size,
                "Parts": parts,
                "VaultARN": vault.arn,
            }),
        )
    }
}

impl Default for Vault {
    fn default() -> Self {
        Vault {
            name: String::new(),
            arn: String::new(),
            creation_date: String::new(),
            archives: HashMap::new(),
            jobs: HashMap::new(),
            tags: Map::new(),
            notification_configuration: None,
            access_policy: None,
            vault_lock: None,
            multipart_uploads: HashMap::new(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;

    fn mkreq(
        acct: u64,
        method: &str,
        path: &str,
        body: &[u8],
        headers: &[(&str, &str)],
    ) -> Request {
        let mut header_map = HashMap::new();
        for (k, v) in headers {
            header_map.insert(k.to_string(), v.to_string());
        }
        let mut full_path = path.to_string();
        let mut query_string = String::new();
        if let Some((p, q)) = path.split_once('?') {
            full_path = p.to_string();
            query_string = q.to_string();
        }
        Request {
            service: "glacier".into(),
            operation: String::new(),
            method: method.into(),
            path: full_path,
            query_string,
            headers: header_map,
            body: Bytes::copy_from_slice(body),
            account: acct,
            region: "us-east-1".into(),
        }
    }

    fn json_body(resp: &Response) -> Value {
        serde_json::from_slice(&resp.body).unwrap()
    }

    fn header(resp: &Response, name: &str) -> String {
        resp.headers
            .iter()
            .find(|(k, _)| k.eq_ignore_ascii_case(name))
            .map(|(_, v)| v.clone())
            .unwrap_or_default()
    }

    // Each test owns one account id so the global slot stays isolated.
    fn vault(acct: u64) -> String {
        format!("/{acct}/vaults/vaulter")
    }

    #[test]
    fn vault_crud_and_tags() {
        let acct = 123456789011;
        let dir = vault(acct);
        assert_eq!(dispatch(&mkreq(acct, "PUT", &dir, b"", &[])).status, 201);
        let body = json_body(&dispatch(&mkreq(acct, "GET", &dir, b"", &[])));
        assert_eq!(body["VaultName"], "vaulter");
        assert_eq!(body["NumberOfArchives"], 0);
        assert!(body["VaultARN"]
            .as_str()
            .unwrap()
            .starts_with("arn:aws:glacier:"));
        assert_eq!(
            json_body(&dispatch(&mkreq(
                acct,
                "GET",
                &format!("/{acct}/vaults"),
                b"",
                &[]
            )))["VaultList"]
                .as_array()
                .unwrap()
                .iter()
                .any(|v| v["VaultName"] == "vaulter"),
            true
        );

        let add = dispatch(&mkreq(
            acct,
            "POST",
            &format!("{dir}/tags?operation=add"),
            br#"{"Tags":{"Env":"test"}}"#,
            &[],
        ));
        assert_eq!(add.status, 204);
        let tags = json_body(&dispatch(&mkreq(
            acct,
            "GET",
            &format!("{dir}/tags"),
            b"",
            &[],
        )));
        assert_eq!(tags["Tags"]["Env"], "test");
        let rm = dispatch(&mkreq(
            acct,
            "POST",
            &format!("{dir}/tags?operation=remove"),
            br#"{"TagKeys":["Env"]}"#,
            &[],
        ));
        assert_eq!(rm.status, 204);

        assert_eq!(
            dispatch(&mkreq(
                acct,
                "PUT",
                &format!("{dir}/notification-configuration"),
                br#"{"SNSTopic":"arn:sns","Events":["ArchiveRetrievalCompleted"]}"#,
                &[],
            ))
            .status,
            204
        );
        let notif = json_body(&dispatch(&mkreq(
            acct,
            "GET",
            &format!("{dir}/notification-configuration"),
            b"",
            &[],
        )));
        assert_eq!(notif["SNSTopic"], "arn:sns");
        assert_eq!(dispatch(&mkreq(acct, "DELETE", &dir, b"", &[])).status, 204);
    }

    #[test]
    fn archive_upload_and_delete() {
        let acct = 123456789012;
        let dir = vault(acct);
        dispatch(&mkreq(acct, "PUT", &dir, b"", &[]));
        let up = dispatch(&mkreq(
            acct,
            "POST",
            &format!("{dir}/archives"),
            b"test archive data",
            &[],
        ));
        assert_eq!(up.status, 201);
        let archive_id = header(&up, "x-amz-archive-id");
        assert!(!archive_id.is_empty());
        assert!(!header(&up, "x-amz-sha256-tree-hash").is_empty());
        let del = dispatch(&mkreq(
            acct,
            "DELETE",
            &format!("{dir}/archives/{archive_id}"),
            b"",
            &[],
        ));
        assert_eq!(del.status, 204);
    }

    #[test]
    fn jobs_are_described_and_listed() {
        let acct = 123456789013;
        let dir = vault(acct);
        dispatch(&mkreq(acct, "PUT", &dir, b"", &[]));
        let init = dispatch(&mkreq(
            acct,
            "POST",
            &format!("{dir}/jobs"),
            br#"{"Type":"inventory-retrieval"}"#,
            &[],
        ));
        assert_eq!(init.status, 202);
        let job_id = header(&init, "x-amz-job-id");
        assert_eq!(job_id.len(), 92);

        let desc = json_body(&dispatch(&mkreq(
            acct,
            "GET",
            &format!("{dir}/jobs/{job_id}"),
            b"",
            &[],
        )));
        assert_eq!(desc["JobId"], job_id);
        assert_eq!(desc["Action"], "InventoryRetrieval");
        assert!(desc["StatusCode"].is_string());

        let list = json_body(&dispatch(&mkreq(
            acct,
            "GET",
            &format!("{dir}/jobs"),
            b"",
            &[],
        )));
        assert!(list["JobList"]
            .as_array()
            .unwrap()
            .iter()
            .any(|j| j["JobId"] == job_id));
    }

    #[test]
    fn multipart_upload_flow() {
        let acct = 123456789014;
        let dir = vault(acct);
        dispatch(&mkreq(acct, "PUT", &dir, b"", &[]));
        let init = dispatch(&mkreq(
            acct,
            "POST",
            &format!("{dir}/multipart-uploads"),
            b"",
            &[("x-amz-part-size", "1048576")],
        ));
        assert_eq!(init.status, 201);
        let upload_id = header(&init, "x-amz-multipart-upload-id");

        let part = dispatch(&mkreq(
            acct,
            "PUT",
            &format!("{dir}/multipart-uploads/{upload_id}"),
            &[0x78u8; 1048576],
            &[("content-range", "bytes 0-1048575/*")],
        ));
        assert_eq!(part.status, 204);
        let checksum = header(&part, "x-amz-sha256-tree-hash");
        assert_eq!(checksum.len(), 64);

        let parts = json_body(&dispatch(&mkreq(
            acct,
            "GET",
            &format!("{dir}/multipart-uploads/{upload_id}"),
            b"",
            &[],
        )));
        assert_eq!(parts["Parts"].as_array().unwrap().len(), 1);
        assert_eq!(parts["PartSizeInBytes"], 1048576);

        let complete = dispatch(&mkreq(
            acct,
            "POST",
            &format!("{dir}/multipart-uploads/{upload_id}"),
            b"",
            &[("x-amz-archive-size", "1048576")],
        ));
        assert_eq!(complete.status, 201);
        assert!(!header(&complete, "x-amz-archive-id").is_empty());
        assert!(!header(&complete, "x-amz-sha256-tree-hash").is_empty());
        assert_eq!(header(&complete, "x-amz-sha256-tree-hash"), checksum);
    }

    #[test]
    fn vault_lock_lifecycle() {
        let acct = 123456789015;
        let dir = vault(acct);
        dispatch(&mkreq(acct, "PUT", &dir, b"", &[]));
        let init = dispatch(&mkreq(
            acct,
            "POST",
            &format!("{dir}/lock-policy"),
            br#"{"Policy":"{\"Version\\":..."}"#,
            &[],
        ));
        assert_eq!(init.status, 201);
        let lock_id = header(&init, "x-amz-lock-id");
        assert!(!lock_id.is_empty());
        let lock = json_body(&dispatch(&mkreq(
            acct,
            "GET",
            &format!("{dir}/lock-policy"),
            b"",
            &[],
        )));
        assert_eq!(lock["State"], "InProgress");
        assert_eq!(
            dispatch(&mkreq(
                acct,
                "POST",
                &format!("{dir}/lock-policy/{lock_id}"),
                b"",
                &[]
            ))
            .status,
            204
        );
        let after = json_body(&dispatch(&mkreq(
            acct,
            "GET",
            &format!("{dir}/lock-policy"),
            b"",
            &[],
        )));
        assert_eq!(after["State"], "Locked");
        assert!(after.get("ExpirationDate").is_none());
    }

    #[test]
    fn provisioned_capacity_purchases() {
        let acct = 123456789016;
        let buy = dispatch(&mkreq(
            acct,
            "POST",
            &format!("/{acct}/provisioned-capacity"),
            b"",
            &[],
        ));
        assert_eq!(buy.status, 201);
        assert!(!header(&buy, "x-amz-capacity-id").is_empty());
        let list = dispatch(&mkreq(
            acct,
            "GET",
            &format!("/{acct}/provisioned-capacity"),
            b"",
            &[],
        ));
        let list: Value = serde_json::from_slice(&list.body).unwrap();
        assert!(list["ProvisionedCapacityList"].as_array().unwrap().len() >= 1);
    }

    #[test]
    fn data_retrieval_policy_round_trip() {
        let acct = 123456789017;
        let initial = serde_json::from_slice::<Value>(
            &dispatch(&mkreq(
                acct,
                "GET",
                &format!("/{acct}/policies/data-retrieval"),
                b"",
                &[],
            ))
            .body,
        )
        .unwrap();
        assert!(initial["Policy"].is_object());
        let set = dispatch(&mkreq(
            acct,
            "PUT",
            &format!("/{acct}/policies/data-retrieval"),
            br#"{"Policy":{"Rules":[{"Strategy":"FreeTier"}]}}"#,
            &[],
        ));
        assert_eq!(set.status, 204);
    }

    #[test]
    fn unknown_vault_describe_fails() {
        let acct = 123456789018;
        assert_eq!(
            dispatch(&mkreq(
                acct,
                "GET",
                &format!("/{acct}/vaults/ghost"),
                b"",
                &[]
            ))
            .status,
            404
        );
    }
}
