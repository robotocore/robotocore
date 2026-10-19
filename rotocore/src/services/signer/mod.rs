//! AWS Signer backend (rest-json, api `signer-2017-08-25`).
//!
//! The gateway hands REST-protocol services an empty operation and the raw
//! method + path, so routing matches botocore's requestUri templates from
//! the service model; state is per (account, region) like every other
//! backend.
//!
//! Surface per `probes/signer.json` (moto sidecar era): every one of the 19
//! operations was probed and none is `not_implemented`. 16 returned OK or
//! the handler-proven ResourceNotFoundException; `CancelSigningProfile` and
//! `GetRevocationStatus` recorded probe-side crashes (missing profile /
//! `data-` host-prefix connection error), so they follow the real-AWS
//! semantics the botocore shapes describe; `UntagResource` was a
//! client-side `needs_params` only.

use crate::proto::{json_error, now_iso, parse_json_body, query_map, url_decode};
use crate::store;
use crate::{Request, Response};
use base64::engine::general_purpose::STANDARD as B64;
use base64::Engine as _;
use once_cell::sync::Lazy;
use serde_json::{json, Value};
use sha2::{Digest, Sha256};
use std::collections::{BTreeMap, HashMap};
use uuid::Uuid;

static SLOT: Lazy<store::Slot<State>> = Lazy::new(|| store::slot::<State>("signer"));

#[derive(Default)]
struct State {
    /// profileName -> wire-shaped profile record (doubles as the
    /// GetSigningProfile / ListSigningProfiles body).
    profiles: BTreeMap<String, Value>,
    /// Name -> (revision id, permissions[]). Kept out of the profile record:
    /// permissions are not part of the GetSigningProfile output shape.
    permissions: BTreeMap<String, (u64, Vec<Value>)>,
    /// jobId -> wire-shaped job record (doubles as the DescribeSigningJob body).
    jobs: BTreeMap<String, Value>,
    /// resourceArn -> tags. Like moto, the tag operations key on the ARN the
    /// client passes and never validate that a resource was created.
    tags: HashMap<String, HashMap<String, String>>,
}

/// Dispatch a routed request to the signer backend.
pub fn dispatch(req: &Request) -> Option<Response> {
    if req.service != "signer" {
        return None;
    }
    let params = parse_json_body(&req.body);
    let mut guard = SLOT.write();
    let st = guard.entry((req.account, req.region.clone())).or_default();
    Some(route(st, req, &params))
}

/// Route a signer request by method + path (botocore requestUri templates).
fn route(st: &mut State, req: &Request, body: &Value) -> Response {
    // Percent-decode each captured path segment (profile names, platform ids
    // and resource ARNs arrive URL-encoded), then borrow them for the match.
    let segments: Vec<String> = req
        .path
        .trim_matches('/')
        .split('/')
        .filter(|s| !s.is_empty())
        .map(url_decode)
        .collect();
    let segments: Vec<&str> = segments.iter().map(String::as_str).collect();
    let q = query_map(&req.query_string);
    let account = req.account;
    let region = req.region.as_str();

    match (req.method.as_str(), segments.as_slice()) {
        // --- signing profiles ---------------------------------------------------
        ("GET", ["signing-profiles"]) => st.list_signing_profiles(),
        ("PUT", ["signing-profiles", name]) => st.put_signing_profile(name, body, account, region),
        ("GET", ["signing-profiles", name]) => st.get_signing_profile(name),
        ("DELETE", ["signing-profiles", name]) => st.cancel_signing_profile(name),
        ("GET", ["signing-profiles", name, "permissions"]) => st.list_profile_permissions(name),
        ("POST", ["signing-profiles", name, "permissions"]) => {
            st.add_profile_permission(name, body)
        }
        ("DELETE", ["signing-profiles", name, "permissions", statement_id]) => {
            st.remove_profile_permission(name, statement_id)
        }
        ("PUT", ["signing-profiles", name, "revoke"]) => st.revoke_signing_profile(name),

        // --- signing platforms --------------------------------------------------
        ("GET", ["signing-platforms"]) => list_signing_platforms(),
        ("GET", ["signing-platforms", platform_id]) => get_signing_platform(platform_id),

        // --- signing jobs -------------------------------------------------------
        ("GET", ["signing-jobs"]) => st.list_signing_jobs(),
        ("POST", ["signing-jobs"]) => st.start_signing_job(body, account, region),
        ("POST", ["signing-jobs", "with-payload"]) => st.sign_payload(body, account, region),
        ("GET", ["signing-jobs", job_id]) => st.describe_signing_job(job_id),
        ("PUT", ["signing-jobs", job_id, "revoke"]) => st.revoke_signature(job_id, body),

        // --- revocation lookup --------------------------------------------------
        ("GET", ["revocations"]) => get_revocation_status(&q),

        // --- resource tags ------------------------------------------------------
        ("GET", ["tags", arn]) => st.list_tags_for_resource(arn),
        ("POST", ["tags", arn]) => st.tag_resource(arn, body),
        ("DELETE", ["tags", arn]) => st.untag_resource(arn, &q),

        _ => json_error(
            501,
            "NotImplemented",
            &format!("The {} action has not been implemented", req.operation),
        ),
    }
}

/// Signer ARNs carry a leading slash before the resource path
/// (`arn:aws:signer:region:account:/signing-profiles/name`); the account id
/// sits at colon index 4, which the compat suite relies on.
fn profile_arn(region: &str, account: u64, name: &str) -> String {
    format!("arn:aws:signer:{region}:{account}:/signing-profiles/{name}")
}

fn certificate_arn(region: &str, account: u64) -> String {
    format!(
        "arn:aws:signer:{region}:{account}:/certificates/{}",
        random_id()
    )
}

fn not_found(message: &str) -> Response {
    json_error(404, "ResourceNotFoundException", message)
}

fn ok_json(v: &Value) -> Response {
    Response::json(200, serde_json::to_vec(v).unwrap_or_default())
}

/// Fresh opaque id for jobs and certificates.
fn random_id() -> String {
    Uuid::new_v4().simple().to_string()
}

/// 12-character version label, unique per profile version.
fn profile_version() -> String {
    random_id()[..12].to_string()
}

fn text(v: &Value, key: &str) -> String {
    v.get(key)
        .and_then(Value::as_str)
        .unwrap_or_default()
        .to_string()
}

impl State {
    // ---- signing profiles --------------------------------------------------

    fn put_signing_profile(
        &mut self,
        name: &str,
        body: &Value,
        account: u64,
        region: &str,
    ) -> Response {
        let platform_id = text(body, "platformId");
        let version = profile_version();
        let arn = profile_arn(region, account, name);
        let mut record = json!({
            "profileName": name,
            "platformId": platform_id,
            "profileVersion": version,
            "arn": arn,
            "profileVersionArn": format!("{arn}/{version}"),
            "status": "Active",
        });
        // Pass-through fields the service accepts on update.
        for field in ["signatureValidityPeriod", "overrides", "signingParameters"] {
            if let Some(v) = body.get(field) {
                record[field] = v.clone();
            }
        }
        if let Some(tags) = body.get("tags").and_then(Value::as_object) {
            let tags: HashMap<String, String> = tags
                .iter()
                .filter_map(|(k, v)| v.as_str().map(|s| (k.clone(), s.to_string())))
                .collect();
            self.tags.insert(arn.clone(), tags);
        }
        self.profiles.insert(name.to_string(), record);
        self.permissions
            .entry(name.to_string())
            .or_insert((1, Vec::new()));
        ok_json(&json!({
            "arn": arn,
            "profileVersion": version,
            "profileVersionArn": format!("{arn}/{version}"),
        }))
    }

    fn get_signing_profile(&mut self, name: &str) -> Response {
        let Some(record) = self.profiles.get(name).cloned() else {
            return not_found(&format!("SigningProfile {name} not found"));
        };
        let mut full = record;
        let arn = text(&full, "arn");
        if let Some(tags) = self.tags.get(&arn) {
            full["tags"] = json!(tags);
        }
        ok_json(&full)
    }

    fn cancel_signing_profile(&mut self, name: &str) -> Response {
        match self.profiles.get_mut(name) {
            Some(record) => {
                record["status"] = json!("Canceled");
                ok_json(&json!({}))
            }
            None => not_found(&format!("SigningProfile {name} not found")),
        }
    }

    fn list_signing_profiles(&self) -> Response {
        let profiles: Vec<Value> = self
            .profiles
            .values()
            .map(|r| {
                let mut summary = r.clone();
                let arn = text(r, "arn");
                if let Some(tags) = self.tags.get(&arn) {
                    summary["tags"] = json!(tags);
                }
                summary
            })
            .collect();
        ok_json(&json!({"profiles": profiles, "nextToken": Value::Null}))
    }

    // ---- profile permissions -------------------------------------------------

    fn add_profile_permission(&mut self, name: &str, body: &Value) -> Response {
        if !self.profiles.contains_key(name) {
            return not_found(&format!("SigningProfile {name} not found"));
        }
        let entry = self
            .permissions
            .entry(name.to_string())
            .or_insert_with(|| (1, Vec::new()));
        entry.1.push(json!({
            "action": text(body, "action"),
            "principal": text(body, "principal"),
            "statementId": text(body, "statementId"),
            "profileVersion": text(body, "profileVersion"),
        }));
        entry.0 += 1;
        ok_json(&json!({"revisionId": entry.0.to_string()}))
    }

    fn remove_profile_permission(&mut self, name: &str, statement_id: &str) -> Response {
        if !self.profiles.contains_key(name) {
            return not_found(&format!("SigningProfile {name} not found"));
        }
        let Some(entry) = self.permissions.get_mut(name) else {
            return not_found(&format!("Statement {statement_id} not found"));
        };
        let before = entry.1.len();
        entry.1.retain(|p| text(p, "statementId") != statement_id);
        if entry.1.len() == before {
            return not_found(&format!("Statement {statement_id} not found"));
        }
        entry.0 += 1;
        ok_json(&json!({"revisionId": entry.0.to_string()}))
    }

    fn list_profile_permissions(&self, name: &str) -> Response {
        if !self.profiles.contains_key(name) {
            return not_found(&format!("SigningProfile {name} not found"));
        }
        let (revision, perms) = match self.permissions.get(name) {
            Some(entry) => entry,
            None => &(1, Vec::new()),
        };
        ok_json(&json!({
            "revisionId": revision.to_string(),
            "policySizeBytes": serde_json::to_vec(perms).map(|b| b.len()).unwrap_or(0),
            "permissions": perms,
            "nextToken": Value::Null,
        }))
    }

    // ---- revocation ----------------------------------------------------------

    fn revoke_signing_profile(&mut self, name: &str) -> Response {
        if !self.profiles.contains_key(name) {
            return not_found(&format!("SigningProfile {name} not found"));
        }
        ok_json(&json!({}))
    }

    fn revoke_signature(&mut self, job_id: &str, body: &Value) -> Response {
        let Some(job) = self.jobs.get_mut(job_id) else {
            return not_found(&format!("Signing job {job_id} not found"));
        };
        job["isRevoked"] = json!(true);
        job["status"] = json!("Revoked");
        job["revocationRecord"] = json!({
            "reason": text(body, "reason"),
            "revokedAt": now_iso(),
        });
        ok_json(&json!({}))
    }

    // ---- signing platforms ---------------------------------------------------

    // ---- signing jobs --------------------------------------------------------

    fn start_signing_job(&mut self, body: &Value, account: u64, region: &str) -> Response {
        let profile_name = text(body, "profileName");
        let Some(profile) = self.profiles.get(&profile_name).cloned() else {
            return not_found(&format!("SigningProfile {profile_name} not found"));
        };
        let job_id = random_id();
        let source = body.get("source").cloned().unwrap_or_else(|| json!({}));
        let job = json!({
            "jobId": job_id,
            "jobOwner": account.to_string(),
            "source": source,
            "platformId": text(&profile, "platformId"),
            "profileName": profile_name,
            "profileVersion": text(&profile, "profileVersion"),
            "signingMaterial": {"certificateArn": certificate_arn(region, account)},
            "createdAt": now_iso(),
            "requestedBy": format!("arn:aws:iam::{account}:user/signer"),
            "status": "InProgress",
            "jobInvoker": format!("arn:aws:iam::{account}:user/signer"),
        });
        self.jobs.insert(job_id.clone(), job);
        ok_json(&json!({"jobId": job_id, "jobOwner": account.to_string()}))
    }

    fn describe_signing_job(&self, job_id: &str) -> Response {
        match self.jobs.get(job_id) {
            Some(job) => ok_json(job),
            None => not_found(&format!("Signing job {job_id} not found")),
        }
    }

    fn list_signing_jobs(&self) -> Response {
        let jobs: Vec<Value> = self.jobs.values().cloned().collect();
        ok_json(&json!({"jobs": jobs, "nextToken": Value::Null}))
    }

    fn sign_payload(&mut self, body: &Value, account: u64, _region: &str) -> Response {
        let profile_name = text(body, "profileName");
        if !self.profiles.contains_key(&profile_name) {
            return not_found(&format!("SigningProfile {profile_name} not found"));
        }
        let payload = body
            .get("payload")
            .and_then(Value::as_str)
            .and_then(|p| B64.decode(p).ok())
            .unwrap_or_default();
        let mut hasher = Sha256::new();
        hasher.update(&payload);
        let signature = B64.encode(hasher.finalize());
        ok_json(&json!({
            "jobId": random_id(),
            "jobOwner": account.to_string(),
            "metadata": {},
            "signature": signature,
        }))
    }

    // ---- resource tags ---------------------------------------------------------

    fn tag_resource(&mut self, arn: &str, body: &Value) -> Response {
        if let Some(tags) = body.get("tags").and_then(Value::as_object) {
            let entry = self.tags.entry(arn.to_string()).or_default();
            for (k, v) in tags {
                if let Some(s) = v.as_str() {
                    entry.insert(k.clone(), s.to_string());
                }
            }
        }
        ok_json(&json!({}))
    }

    fn untag_resource(&mut self, arn: &str, q: &BTreeMap<String, Vec<String>>) -> Response {
        let keys = q.get("tagKeys").cloned().unwrap_or_default();
        let entry = self.tags.entry(arn.to_string()).or_default();
        for k in keys {
            entry.remove(&k);
        }
        ok_json(&json!({}))
    }

    fn list_tags_for_resource(&self, arn: &str) -> Response {
        ok_json(&json!({"tags": self.tags.get(arn).cloned().unwrap_or_default()}))
    }
}

// ---- signing platforms -------------------------------------------------------

/// A documented AWS Signer signing platform. Only the `AWSLambda-*` entry is
/// exercised by the compat suites; the rest populate ListSigningPlatforms the
/// way the real service does.
fn platform_table() -> &'static [Platform] {
    static PLATFORMS: Lazy<Vec<Platform>> = Lazy::new(|| {
        vec![
            Platform {
                platform_id: "AWSLambda-SHA384-ECDSA",
                display_name: "AWS Lambda signing platform",
                partner: "AmazonWebServices",
                target: "Lambda",
                category: "AWSLambda",
                max_size_mb: 300,
                revocation_supported: true,
                encryption_algorithm: "ECDSA_SHA384",
                hash_algorithm: "SHA384",
            },
            Platform {
                platform_id: "AWSIoTDeviceManagement-SHA256-ECDSA",
                display_name: "AWS IoT device management signing platform",
                partner: "AmazonWebServices",
                target: "IoTDeviceManagement",
                category: "AWSIoT",
                max_size_mb: 250,
                revocation_supported: false,
                encryption_algorithm: "ECDSA_SHA256",
                hash_algorithm: "SHA256",
            },
            Platform {
                platform_id: "AWSIoTFirmwareUpdate-SHA256-ECDSA",
                display_name: "AWS IoT firmware update signing platform",
                partner: "AmazonWebServices",
                target: "IoTFirmwareUpdate",
                category: "AWSIoT",
                max_size_mb: 200,
                revocation_supported: false,
                encryption_algorithm: "ECDSA_SHA256",
                hash_algorithm: "SHA256",
            },
            Platform {
                platform_id: "AWSIoTFirmwareUpdate-SHA256-RSA",
                display_name: "AWS IoT firmware update signing platform",
                partner: "AmazonWebServices",
                target: "IoTFirmwareUpdate",
                category: "AWSIoT",
                max_size_mb: 200,
                revocation_supported: false,
                encryption_algorithm: "RSA_2048",
                hash_algorithm: "SHA256",
            },
            Platform {
                platform_id: "AmazonFreeRTOS-TI-CC3220SF",
                display_name: "Amazon FreeRTOS signing platform",
                partner: "AmazonFreeRTOS",
                target: "TI_CC3220SF",
                category: "AmazonFreeRTOS",
                max_size_mb: 100,
                revocation_supported: false,
                encryption_algorithm: "ECDSA_SHA256",
                hash_algorithm: "SHA256",
            },
            Platform {
                platform_id: "AmazonFreeRTOS-Default",
                display_name: "Amazon FreeRTOS generic signing platform",
                partner: "AmazonFreeRTOS",
                target: "Default",
                category: "AmazonFreeRTOS",
                max_size_mb: 100,
                revocation_supported: false,
                encryption_algorithm: "ECDSA_SHA256",
                hash_algorithm: "SHA256",
            },
        ]
    });
    &PLATFORMS
}

struct Platform {
    platform_id: &'static str,
    display_name: &'static str,
    partner: &'static str,
    target: &'static str,
    category: &'static str,
    max_size_mb: i64,
    revocation_supported: bool,
    /// Default-able encryption algorithm of the platform's key pairs.
    encryption_algorithm: &'static str,
    hash_algorithm: &'static str,
}

impl Platform {
    fn summary(&self) -> Value {
        json!({
            "platformId": self.platform_id,
            "displayName": self.display_name,
            "partner": self.partner,
            "target": self.target,
            "category": self.category,
            "maxSizeInMB": self.max_size_mb,
            "revocationSupported": self.revocation_supported,
        })
    }

    fn full(&self) -> Value {
        let mut full = self.summary();
        full["signingConfiguration"] = json!({
            "encryptionAlgorithmOptions": {
                "allowedValues": [self.encryption_algorithm],
                "defaultValue": self.encryption_algorithm,
            },
            "hashAlgorithmOptions": {
                "allowedValues": [self.hash_algorithm],
                "defaultValue": self.hash_algorithm,
            },
        });
        full["signingImageFormat"] = json!({
            "supportedFormats": ["JSON"],
            "defaultFormat": "JSON",
        });
        full
    }
}

fn get_signing_platform(platform_id: &str) -> Response {
    match platform_table()
        .iter()
        .find(|p| p.platform_id == platform_id)
    {
        Some(p) => ok_json(&p.full()),
        None => not_found(&format!("Signing platform {platform_id} not found")),
    }
}

fn list_signing_platforms() -> Response {
    let platforms: Vec<Value> = platform_table().iter().map(|p| p.summary()).collect();
    ok_json(&json!({"platforms": platforms, "nextToken": Value::Null}))
}

// ---- revocation lookup -------------------------------------------------------

/// GetRevocationStatus reports confirmed revocations for the supplied
/// certificate hashes. The probe never validated moto's answer (the SDK's
/// `data-` host prefix broke its connection), so the honest default is an
/// empty revoked set — nothing this twin confirms revocation for.
fn get_revocation_status(q: &BTreeMap<String, Vec<String>>) -> Response {
    let _ = q.get("certificateHashes");
    ok_json(&json!({"revokedEntities": []}))
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Each test gets its own account so the process-global slot never
    /// bleeds state between tests.
    fn req(method: &str, path: &str, query: &str, body: Value, account: u64) -> Request {
        Request {
            service: "signer".into(),
            operation: String::new(),
            method: method.into(),
            path: path.into(),
            query_string: query.into(),
            headers: Default::default(),
            body: bytes::Bytes::from(serde_json::to_vec(&body).unwrap_or_default()),
            account,
            region: "us-east-1".into(),
        }
    }

    fn send(req: Request) -> Response {
        dispatch(&req).expect("signer dispatch answers its own requests")
    }

    fn json_body(resp: &Response) -> Value {
        serde_json::from_slice(&resp.body).unwrap()
    }

    fn put_profile(name: &str, account: u64) -> Response {
        send(req(
            "PUT",
            &format!("/signing-profiles/{name}"),
            "",
            json!({"platformId": "AWSLambda-SHA384-ECDSA", "tags": {"env": "test"}}),
            account,
        ))
    }

    #[test]
    fn put_then_get_returns_record() {
        let account = 900_000_000_010;
        let put_resp = put_profile("p-put-get", account);
        assert_eq!(put_resp.status, 200);
        let put_body = json_body(&put_resp);
        let arn = put_body["arn"].as_str().unwrap().to_string();
        assert!(arn.contains("p-put-get"));
        assert_eq!(arn.split(':').nth(4), Some(account.to_string().as_str()));
        assert!(put_body["profileVersion"].as_str().is_some());
        assert!(put_body["profileVersionArn"].as_str().is_some());

        let got = json_body(&send(req(
            "GET",
            "/signing-profiles/p-put-get",
            "",
            json!({}),
            account,
        )));
        assert_eq!(got["profileName"], json!("p-put-get"));
        assert_eq!(got["platformId"], json!("AWSLambda-SHA384-ECDSA"));
        assert_eq!(got["status"], json!("Active"));
        assert_eq!(got["arn"], json!(arn));
        assert_eq!(got["profileVersion"], put_body["profileVersion"]);
        assert_eq!(got["tags"], json!({"env": "test"}));
    }

    #[test]
    fn cancel_marks_profile_canceled() {
        let account = 900_000_000_011;
        put_profile("p-cancel", account);

        let cancel = send(req(
            "DELETE",
            "/signing-profiles/p-cancel",
            "",
            json!({}),
            account,
        ));
        assert_eq!(cancel.status, 200);

        let got = json_body(&send(req(
            "GET",
            "/signing-profiles/p-cancel",
            "",
            json!({}),
            account,
        )));
        assert_eq!(got["status"], json!("Canceled"));
    }

    #[test]
    fn get_missing_profile_is_404() {
        let resp = send(req(
            "GET",
            "/signing-profiles/nope",
            "",
            json!({}),
            900_000_000_012,
        ));
        assert_eq!(resp.status, 404);
        assert_eq!(
            json_body(&resp)["__type"],
            json!("ResourceNotFoundException")
        );
    }

    #[test]
    fn tag_roundtrip_merges_and_removes() {
        let account = 900_000_000_013;
        put_profile("p-tags", account);
        let arn = profile_arn("us-east-1", account, "p-tags");
        let encoded = urlencoding::encode(&arn).to_string();

        let tag = send(req(
            "POST",
            &format!("/tags/{encoded}"),
            "",
            json!({"tags": {"added": "later"}}),
            account,
        ));
        assert_eq!(tag.status, 200);

        let listed = json_body(&send(req(
            "GET",
            &format!("/tags/{encoded}"),
            "",
            json!({}),
            account,
        )));
        assert_eq!(listed["tags"]["env"], json!("test"));
        assert_eq!(listed["tags"]["added"], json!("later"));

        let untag = send(req(
            "DELETE",
            &format!("/tags/{encoded}"),
            "tagKeys=env",
            json!({}),
            account,
        ));
        assert_eq!(untag.status, 200);

        let listed = json_body(&send(req(
            "GET",
            &format!("/tags/{encoded}"),
            "",
            json!({}),
            account,
        )));
        assert!(listed["tags"].get("env").is_none());
        assert_eq!(listed["tags"]["added"], json!("later"));
    }

    #[test]
    fn permissions_add_list_remove() {
        let account = 900_000_000_014;
        put_profile("p-perms", account);

        let missing = send(req(
            "POST",
            "/signing-profiles/ghost/permissions",
            "",
            json!({"action": "signer:StartSigningJob", "principal": "123456789012", "statementId": "s1"}),
            account,
        ));
        assert_eq!(missing.status, 404);

        let add = send(req(
            "POST",
            "/signing-profiles/p-perms/permissions",
            "",
            json!({"action": "signer:StartSigningJob", "principal": "123456789012", "statementId": "s1"}),
            account,
        ));
        assert_eq!(add.status, 200);
        let before_revision = json_body(&add)["revisionId"].clone();
        assert!(before_revision.is_string());

        let listed = json_body(&send(req(
            "GET",
            "/signing-profiles/p-perms/permissions",
            "",
            json!({}),
            account,
        )));
        assert_eq!(listed["permissions"].as_array().unwrap().len(), 1);
        assert_eq!(listed["permissions"][0]["statementId"], json!("s1"));

        let remove = send(req(
            "DELETE",
            "/signing-profiles/p-perms/permissions/s1",
            "",
            json!({}),
            account,
        ));
        assert_eq!(remove.status, 200);
        let after_revision = json_body(&remove)["revisionId"].clone();
        assert!(after_revision.is_string());
        assert_ne!(after_revision, before_revision);

        let listed = json_body(&send(req(
            "GET",
            "/signing-profiles/p-perms/permissions",
            "",
            json!({}),
            account,
        )));
        assert_eq!(listed["revisionId"], after_revision);
        assert_eq!(listed["permissions"].as_array().unwrap().len(), 0);
    }

    #[test]
    fn start_and_describe_job() {
        let account = 900_000_000_015;
        put_profile("p-job", account);

        let started = json_body(&send(req(
            "POST",
            "/signing-jobs",
            "",
            json!({
                "source": {"s3": {"bucketName": "b", "key": "k.zip", "version": "v1"}},
                "profileName": "p-job",
            }),
            account,
        )));
        assert!(started["jobId"].as_str().is_some());
        assert_eq!(started["jobOwner"], json!(account.to_string()));

        let job_id = started["jobId"].as_str().unwrap().to_string();
        let described = json_body(&send(req(
            "GET",
            &format!("/signing-jobs/{job_id}"),
            "",
            json!({}),
            account,
        )));
        assert_eq!(described["status"], json!("InProgress"));
        assert_eq!(described["jobId"], json!(job_id));

        let missing = send(req(
            "GET",
            "/signing-jobs/00000000-0000",
            "",
            json!({}),
            account,
        ));
        assert_eq!(missing.status, 404);
    }

    #[test]
    fn revoke_signature_requires_existing_job() {
        let resp = send(req(
            "PUT",
            "/signing-jobs/no-jobs/revoke",
            "",
            json!({"reason": "test"}),
            900_000_000_016,
        ));
        assert_eq!(resp.status, 404);
        assert_eq!(
            json_body(&resp)["__type"],
            json!("ResourceNotFoundException")
        );
    }

    #[test]
    fn sign_payload_returns_signature() {
        let account = 900_000_000_017;
        put_profile("p-sign", account);

        let resp = send(req(
            "POST",
            "/signing-jobs/with-payload",
            "",
            json!({
                "profileName": "p-sign",
                "payloadFormat": "MessageDigest",
                "payload": B64.encode(b"hello world"),
            }),
            account,
        ));
        assert_eq!(resp.status, 200);
        let body = json_body(&resp);
        assert!(!body["signature"].as_str().unwrap_or_default().is_empty());
        assert!(body["jobId"].as_str().is_some());
    }

    #[test]
    fn platform_list_contains_lambda() {
        let account = 900_000_000_018;
        let body = json_body(&send(req(
            "GET",
            "/signing-platforms",
            "",
            json!({}),
            account,
        )));
        let platforms = body["platforms"].as_array().unwrap();
        assert!(!platforms.is_empty());
        assert!(platforms
            .iter()
            .any(|p| p["platformId"] == json!("AWSLambda-SHA384-ECDSA")));
        assert!(platforms
            .iter()
            .all(|p| p["displayName"].as_str().is_some()));

        let one = json_body(&send(req(
            "GET",
            "/signing-platforms/AWSLambda-SHA384-ECDSA",
            "",
            json!({}),
            account,
        )));
        assert_eq!(one["platformId"], json!("AWSLambda-SHA384-ECDSA"));
        assert!(one["displayName"].as_str().is_some());
        assert!(one["signingConfiguration"].is_object());

        let missing = send(req(
            "GET",
            "/signing-platforms/Nope-123",
            "",
            json!({}),
            account,
        ));
        assert_eq!(missing.status, 404);
    }

    #[test]
    fn revocation_status_is_empty_default() {
        let resp = send(req("GET", "/revocations", "", json!({}), 900_000_000_019));
        assert_eq!(resp.status, 200);
        assert_eq!(json_body(&resp)["revokedEntities"], json!([]));
    }

    #[test]
    fn foreign_service_is_not_claimed() {
        let mut req = req("GET", "/signing-platforms", "", json!({}), 900_000_000_020);
        req.service = "not-signer".into();
        assert!(dispatch(&req).is_none());
    }
}
