//! AWS Textract backend — moto-equivalent (json protocol).
//!
//! Replaces the moto sidecar for `textract` (`X-Amz-Target: Textract.<Op>`
//! on `application/x-amz-json-1.1`). State is per-(account, region) and
//! mirrors moto's `TextractBackend` (moto/textract/models.py): one async-job
//! map per Start/Get pair, plus the adapter / adapter-version / tagging
//! stores. Every payload is moto's fixed fixture — empty `Blocks`, a page
//! count drawn once per process, `JobStatus` "SUCCEEDED" — because moto
//! parses no documents.
//!
//! Surface (matches the deployed fork's `responses.py`, which handles every
//! botocore textract operation; `urls.py` routes the whole service to the
//! same dispatcher): all 25 operations are served. The historical probe's 19
//! `not_implemented` entries were recorded from a stale moto — the deployed
//! module implements them, so per G2 they are in surface. The catch-all
//! fallback stays for any operation outside the fork's handler table.
//!
//! moto quirks kept:
//! * `PAGES` is a class attribute holding `{"Pages": N}`, and every payload
//!   nests it as `{"Pages": TextractBackend.PAGES}` — so the wire shape is
//!   `{"Pages": {"Pages": N}}` (verified against the deployed module in
//!   isolation, no test overrides). One 5..=500 draw per process, shared by
//!   every operation in every backend.
//! * Adapters carry two tag stores: creation tags frozen on the adapter
//!   (surfaced by `GetAdapter`) and the mutable `tagger` map keyed by ARN
//!   (surfaced by `ListTagsForResource`, mutated by Tag/Untag).
//! * Adapter versions are numbered by current count (`str(len+1)`), so a
//!   deleted version's number is reused.

use crate::proto::{json_error, parse_json_body};
use crate::store::{self, ScopeKey};
use crate::{Request, Response};
use chrono::Utc;
use once_cell::sync::Lazy;
use parking_lot::RwLock;
use serde_json::{json, Map, Value};
use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;

// ---------------------------------------------------------------------------
// State
// ---------------------------------------------------------------------------

type Stores = Arc<RwLock<HashMap<ScopeKey, TextractStore>>>;

fn stores() -> Stores {
    store::slot::<TextractStore>("textract")
}

/// Per-(account, region) in-memory state for Amazon Textract.
#[derive(Default)]
struct TextractStore {
    /// job id -> stored result — moto's `async_text_detection_jobs`
    text_detection_jobs: BTreeMap<String, Value>,
    /// job id -> stored result — moto's `async_document_analysis_jobs`
    document_analysis_jobs: BTreeMap<String, Value>,
    /// job id -> stored result — moto's `async_expense_analysis_jobs`
    expense_jobs: BTreeMap<String, Value>,
    /// job id -> stored result — moto's `async_lending_analysis_jobs`
    lending_jobs: BTreeMap<String, Value>,
    /// adapter id -> adapter — moto's `adapters`
    adapters: BTreeMap<String, Adapter>,
    /// resource arn -> tags — moto's `tagger`
    tagger: BTreeMap<String, BTreeMap<String, String>>,
}

fn with_store(req: &Request, f: impl FnOnce(&mut TextractStore) -> Response) -> Response {
    let slot = stores();
    let mut map = slot.write();
    let st = map.entry((req.account, req.region.clone())).or_default();
    f(st)
}

/// moto's `PAGES = mock_random.randint(5, 500)` — evaluated once per
/// process, shared by every register (class attribute on the backend).
static DOCUMENT_PAGES: Lazy<i64> = Lazy::new(|| {
    let draw = uuid::Uuid::new_v4().as_u128() % 496; // randint is inclusive
    (draw as i64) + 5
});

/// moto keeps `ds = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())`.
fn creation_time() -> String {
    Utc::now().format("%Y-%m-%dT%H:%M:%SZ").to_string()
}

/// moto's `TextractAdapter` — `to_dict()` never exposes tags or the ARN.
struct Adapter {
    id: String,
    name: String,
    feature_types: Vec<String>,
    description: String,
    auto_update: String,
    tags: BTreeMap<String, String>,
    creation_time: String,
    arn: String,
    versions: BTreeMap<String, AdapterVersion>,
}

impl Adapter {
    fn to_dict(&self) -> Value {
        json!({
            "AdapterId": self.id,
            "AdapterName": self.name,
            "FeatureTypes": self.feature_types,
            "Description": self.description,
            "AutoUpdate": self.auto_update,
            "CreationTime": self.creation_time,
        })
    }
}

/// moto's `TextractAdapterVersion` — the dataset/KMS/output parameters are
/// stored but `to_dict()` never returns them.
struct AdapterVersion {
    adapter_id: String,
    version: String,
    #[allow(dead_code)] // retained moto-side, never surfaced in to_dict()
    dataset: Value,
    #[allow(dead_code)] // ditto
    kms_key_id: String,
    #[allow(dead_code)] // ditto
    output_config: Value,
    tags: BTreeMap<String, String>,
    creation_time: String,
}

impl AdapterVersion {
    fn to_dict(&self) -> Value {
        json!({
            "AdapterId": self.adapter_id,
            "AdapterVersion": self.version,
            "Status": "ACTIVE",
            "CreationTime": self.creation_time,
        })
    }
}

// ---------------------------------------------------------------------------
// dispatch
// ---------------------------------------------------------------------------

pub fn dispatch(req: &Request) -> Option<Response> {
    if req.service != "textract" {
        return None;
    }
    Some(with_store(req, |st| route(req, st)))
}

fn route(req: &Request, st: &mut TextractStore) -> Response {
    let body = parse_json_body(&req.body);
    match req.operation.as_str() {
        // --- synchronous detection ------------------------------------------
        "DetectDocumentText" => ok(text_result_without_status()),
        // --- async text detection ---------------------------------------------
        "StartDocumentTextDetection" => {
            // moto: `if not document_location` — absent, null and {} all fail.
            if !truthy(body.get("DocumentLocation")) {
                return invalid_parameter();
            }
            let job_id = uuid::Uuid::new_v4().to_string();
            let job = text_job_status_dict();
            // moto keeps the NotificationChannel on the job and publishes an
            // SNS completion event; rotocore has no SNS engine behind this
            // backend, so the channel is accepted but never notified.
            st.text_detection_jobs.insert(job_id.clone(), job);
            ok(json!({ "JobId": job_id }))
        }
        "GetDocumentTextDetection" => {
            let job_id = as_str(&body, "JobId");
            match st.text_detection_jobs.get(job_id) {
                Some(job) => ok(job.clone()),
                None => invalid_job_id(),
            }
        }
        // --- async document analysis -------------------------------------------
        "StartDocumentAnalysis" => {
            // moto: `if not document_location or not feature_types`.
            if !truthy(body.get("DocumentLocation")) || !truthy(body.get("FeatureTypes")) {
                return invalid_parameter();
            }
            let job_id = uuid::Uuid::new_v4().to_string();
            let job = text_job_status_dict();
            st.document_analysis_jobs.insert(job_id.clone(), job);
            ok(json!({ "JobId": job_id }))
        }
        // moto accepts MaxResults/NextToken and ignores them ("Pagination
        // has not yet been implemented").
        "GetDocumentAnalysis" => {
            let job_id = as_str(&body, "JobId");
            match st.document_analysis_jobs.get(job_id) {
                Some(job) => ok(job.clone()),
                None => invalid_job_id(),
            }
        }
        // --- synchronous analysis -------------------------------------------
        "AnalyzeDocument" => ok(json!({
            "Blocks": [],
            "DocumentMetadata": document_metadata(),
            "AnalyzeDocumentModelVersion": "1.0",
        })),
        "AnalyzeExpense" => ok(json!({
            "DocumentMetadata": document_metadata(),
            "ExpenseDocuments": [],
        })),
        "AnalyzeID" => ok(json!({
            "DocumentMetadata": document_metadata(),
            "IdentityDocuments": [],
            "AnalyzeIDModelVersion": "1.0",
        })),
        // --- async expense analysis --------------------------------------------
        "StartExpenseAnalysis" => {
            if !truthy(body.get("DocumentLocation")) {
                return invalid_parameter();
            }
            let job_id = uuid::Uuid::new_v4().to_string();
            st.expense_jobs
                .insert(job_id.clone(), expense_job_status_dict());
            ok(json!({ "JobId": job_id }))
        }
        "GetExpenseAnalysis" => {
            let job_id = as_str(&body, "JobId");
            match st.expense_jobs.get(job_id) {
                Some(job) => ok(job.clone()),
                None => invalid_job_id(),
            }
        }
        // --- async lending analysis -------------------------------------------
        "StartLendingAnalysis" => {
            if !truthy(body.get("DocumentLocation")) {
                return invalid_parameter();
            }
            let job_id = uuid::Uuid::new_v4().to_string();
            st.lending_jobs
                .insert(job_id.clone(), lending_job_status_dict());
            ok(json!({ "JobId": job_id }))
        }
        // moto returns the full job dict for both Get operations.
        "GetLendingAnalysis" | "GetLendingAnalysisSummary" => {
            let job_id = as_str(&body, "JobId");
            match st.lending_jobs.get(job_id) {
                Some(job) => ok(job.clone()),
                None => invalid_job_id(),
            }
        }
        // --- adapter CRUD ------------------------------------------------------
        "CreateAdapter" => {
            let tags = string_map(body.get("Tags"));
            let id = uuid::Uuid::new_v4()
                .simple()
                .to_string()
                .chars()
                .take(12)
                .collect::<String>(); // moto: str(uuid4()).replace("-", "")[:12]
            let arn = format!(
                "arn:aws:textract:{}:{}:adapter/{id}",
                req.region, req.account
            );
            let adapter = Adapter {
                id: id.clone(),
                name: as_str(&body, "AdapterName").to_string(),
                feature_types: string_list(body.get("FeatureTypes")),
                description: as_str(&body, "Description").to_string(),
                auto_update: body
                    .get("AutoUpdate")
                    .and_then(Value::as_str)
                    .unwrap_or("DISABLED")
                    .to_string(),
                tags: tags.clone(),
                creation_time: creation_time(),
                arn: arn.clone(),
                versions: BTreeMap::new(),
            };
            st.adapters.insert(id.clone(), adapter);
            st.tagger.insert(arn, tags);
            ok(json!({ "AdapterId": id }))
        }
        "DeleteAdapter" => match st.adapters.remove(as_str(&body, "AdapterId")) {
            Some(adapter) => {
                st.tagger.remove(&adapter.arn);
                ok(json!({}))
            }
            None => resource_not_found(as_str(&body, "AdapterId")),
        },
        "GetAdapter" => match st.adapters.get(as_str(&body, "AdapterId")) {
            Some(adapter) => {
                let mut result = adapter.to_dict();
                result["Tags"] = to_value(&adapter.tags);
                ok(result)
            }
            None => resource_not_found(as_str(&body, "AdapterId")),
        },
        "UpdateAdapter" => {
            let adapter_id = as_str(&body, "AdapterId").to_string();
            match st.adapters.get_mut(&adapter_id) {
                Some(adapter) => {
                    if let Some(name) = body.get("AdapterName").and_then(Value::as_str) {
                        adapter.name = name.to_string();
                    }
                    if let Some(description) = body.get("Description").and_then(Value::as_str) {
                        adapter.description = description.to_string();
                    }
                    if let Some(auto_update) = body.get("AutoUpdate").and_then(Value::as_str) {
                        adapter.auto_update = auto_update.to_string();
                    }
                    ok(json!({ "AdapterId": adapter_id }))
                }
                None => resource_not_found(&adapter_id),
            }
        }
        // moto ignores MaxResults/NextToken/time filters ("Pagination has not
        // yet been implemented").
        "ListAdapters" => ok(json!({
            "Adapters": st.adapters.values().map(Adapter::to_dict).collect::<Vec<_>>(),
        })),
        // --- adapter versions ---------------------------------------------------
        "CreateAdapterVersion" => {
            let adapter_id = as_str(&body, "AdapterId").to_string();
            match st.adapters.get_mut(&adapter_id) {
                Some(adapter) => {
                    // moto numbers versions by current count.
                    let version = (adapter.versions.len() + 1).to_string();
                    adapter.versions.insert(
                        version.clone(),
                        AdapterVersion {
                            adapter_id: adapter_id.clone(),
                            version: version.clone(),
                            dataset: body.get("DatasetConfig").cloned().unwrap_or(json!({})),
                            kms_key_id: as_str(&body, "KMSKeyId").to_string(),
                            output_config: body.get("OutputConfig").cloned().unwrap_or(json!({})),
                            tags: string_map(body.get("Tags")),
                            creation_time: creation_time(),
                        },
                    );
                    ok(json!({ "AdapterId": adapter_id, "AdapterVersion": version }))
                }
                None => resource_not_found(&adapter_id),
            }
        }
        "DeleteAdapterVersion" => {
            let adapter_id = as_str(&body, "AdapterId").to_string();
            let version = as_str(&body, "AdapterVersion").to_string();
            match st.adapters.get_mut(&adapter_id) {
                Some(adapter) => match adapter.versions.remove(&version) {
                    Some(_) => ok(json!({})),
                    None => resource_not_found(&format!("{adapter_id}/{version}")),
                },
                None => resource_not_found(&adapter_id),
            }
        }
        "GetAdapterVersion" => {
            let adapter_id = as_str(&body, "AdapterId").to_string();
            let version = as_str(&body, "AdapterVersion").to_string();
            match st
                .adapters
                .get(&adapter_id)
                .and_then(|a| a.versions.get(&version))
            {
                Some(av) => {
                    let mut result = av.to_dict();
                    result["Tags"] = to_value(&av.tags);
                    ok(result)
                }
                None => resource_not_found(&format!("{adapter_id}/{version}")),
            }
        }
        "ListAdapterVersions" => {
            let want = as_str(&body, "AdapterId");
            let mut versions = Vec::new();
            for (id, adapter) in &st.adapters {
                if !want.is_empty() && id != want {
                    continue;
                }
                versions.extend(adapter.versions.values().map(AdapterVersion::to_dict));
            }
            ok(json!({ "AdapterVersions": versions }))
        }
        // --- tagging ------------------------------------------------------------
        "TagResource" => {
            if let Value::Object(tags) = body.get("Tags").unwrap_or(&json!({})) {
                let entry = st
                    .tagger
                    .entry(as_str(&body, "ResourceARN").to_string())
                    .or_default();
                for (k, v) in tags {
                    if let Some(s) = v.as_str() {
                        entry.insert(k.clone(), s.to_string());
                    }
                }
            }
            ok(json!({}))
        }
        "UntagResource" => {
            let keys = string_list(body.get("TagKeys"));
            let entry = st
                .tagger
                .entry(as_str(&body, "ResourceARN").to_string())
                .or_default();
            for key in keys {
                entry.remove(&key);
            }
            ok(json!({}))
        }
        "ListTagsForResource" => {
            let tags = st.tagger.get(as_str(&body, "ResourceARN"));
            ok(json!({ "Tags": tags.map(to_value).unwrap_or_else(|| json!({})) }))
        }
        // The fork's urls.py hands the whole service to TextractResponse and
        // every botocore operation has a handler; anything else falls back.
        other => json_error(
            501,
            "NotImplemented",
            &format!("The {other} action has not been implemented"),
        ),
    }
}

// ---------------------------------------------------------------------------
// moto's shared payload builders
// ---------------------------------------------------------------------------

/// moto's `DocumentMetadata: {"Pages": TextractBackend.PAGES}` — `PAGES` is
/// itself `{"Pages": N}`, so the wire shape nests twice.
fn document_metadata() -> Value {
    json!({ "Pages": { "Pages": *DOCUMENT_PAGES } })
}

/// `TextractBackend.detect_document_text` — no JobStatus on the wire.
fn text_result_without_status() -> Value {
    json!({
        "Blocks": [],
        "DetectDocumentTextModelVersion": "1.0",
        "DocumentMetadata": document_metadata(),
    })
}

/// `TextractJob.to_dict()` for engine-started text/analysis jobs (both text
/// detection and document analysis reuse the same dict).
fn text_job_status_dict() -> Value {
    let mut job = text_result_without_status();
    job["JobStatus"] = json!("SUCCEEDED");
    job
}

/// `TextractBackend.start_expense_analysis`'s job payload.
fn expense_job_status_dict() -> Value {
    json!({
        "Blocks": [],
        "DocumentMetadata": document_metadata(),
        "ExpenseDocuments": [],
        "JobStatus": "SUCCEEDED",
    })
}

/// `TextractBackend.start_lending_analysis`'s job payload.
fn lending_job_status_dict() -> Value {
    json!({
        "DocumentMetadata": document_metadata(),
        "Results": [],
        "JobStatus": "SUCCEEDED",
    })
}

// ---------------------------------------------------------------------------
// Response helpers
// ---------------------------------------------------------------------------

fn ok(body: Value) -> Response {
    Response::json(200, serde_json::to_vec(&body).unwrap_or_default())
}

/// moto/textract/exceptions.py: 400 + fixed message.
fn invalid_job_id() -> Response {
    json_error(
        400,
        "InvalidJobIdException",
        "An invalid job identifier was passed.",
    )
}

/// moto/textract/exceptions.py: 400 + the constraint-violation message.
fn invalid_parameter() -> Response {
    json_error(
        400,
        "InvalidParameterException",
        "An input parameter violated a constraint. For example, in synchronous operations, an InvalidParameterException exception occurs when neither of the S3Object or Bytes values are supplied in the Document request parameter. Validate your parameter before calling the API operation again.",
    )
}

/// moto/textract/exceptions.py: 400 + Python `!r` repr of the resource id.
fn resource_not_found(resource_id: &str) -> Response {
    json_error(
        400,
        "ResourceNotFoundException",
        &format!("The resource '{resource_id}' does not exist."),
    )
}

/// Python truthiness innoculated for the optionals: false for null and
/// empty containers (moto's `if not document_location`).
fn truthy(value: Option<&Value>) -> bool {
    match value {
        None | Some(Value::Null) => false,
        Some(Value::Array(items)) => !items.is_empty(),
        Some(Value::Object(obj)) => !obj.is_empty(),
        Some(_) => true,
    }
}

fn as_str<'a>(body: &'a Value, key: &str) -> &'a str {
    body.get(key).and_then(Value::as_str).unwrap_or("")
}

/// botocore sends tag maps as `{str: str}`; null/absent mean "no tags".
fn string_map(value: Option<&Value>) -> BTreeMap<String, String> {
    let mut out = BTreeMap::new();
    if let Some(Value::Object(map)) = value {
        for (k, v) in map {
            if let Some(s) = v.as_str() {
                out.insert(k.clone(), s.to_string());
            }
        }
    }
    out
}

fn to_value(map: &BTreeMap<String, String>) -> Value {
    Value::Object(
        map.iter()
            .map(|(k, v)| (k.clone(), Value::String(v.clone())))
            .collect::<Map<String, Value>>(),
    )
}

fn string_list(value: Option<&Value>) -> Vec<String> {
    match value {
        Some(Value::Array(items)) => items
            .iter()
            .filter_map(Value::as_str)
            .map(str::to_string)
            .collect(),
        _ => Vec::new(),
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
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
            service: "textract".to_string(),
            operation: operation.to_string(),
            method: "POST".to_string(),
            path: "/".to_string(),
            query_string: String::new(),
            headers: HashMap::new(),
            body: Bytes::from(serde_json::to_vec(&body).unwrap()),
            account,
            region: "us-east-1".to_string(),
        };
        let resp = dispatch(&request).expect("textract dispatch");
        (
            resp.status,
            serde_json::from_slice(&resp.body).unwrap_or(json!({})),
        )
    }

    fn s3_location() -> Value {
        json!({ "S3Object": { "Bucket": "test-bucket", "Name": "test.pdf" } })
    }

    #[test]
    fn detect_document_text_fixture() {
        let (status, body) = send(next_account(), "DetectDocumentText", json!({}));
        assert_eq!(status, 200);
        assert_eq!(body["Blocks"], json!([]));
        assert_eq!(body["DetectDocumentTextModelVersion"], "1.0");
        // moto nested-vPages quirk: DocumentMetadata.Pages is PAGES ("Pages": N).
        let pages = body["DocumentMetadata"]["Pages"]["Pages"].as_i64().unwrap();
        assert!((5..=500).contains(&pages), "pages {pages} out of range");
        assert!(body.get("JobStatus").is_none());
    }

    #[test]
    fn document_pages_choice_is_stable_per_process() {
        // moto's PAGES is a class attribute: one draw per process, shared
        // across every operation and backend.
        let (status, first) = send(next_account(), "DetectDocumentText", json!({}));
        assert_eq!(status, 200);
        let (_, second) = send(next_account(), "AnalyzeID", json!({}));
        assert_eq!(
            first["DocumentMetadata"]["Pages"],
            second["DocumentMetadata"]["Pages"]
        );
    }

    #[test]
    fn text_detection_job_roundtrip() {
        let account = next_account();
        let (status, body) = send(
            account,
            "StartDocumentTextDetection",
            json!({ "DocumentLocation": s3_location() }),
        );
        assert_eq!(status, 200);
        let job_id = body["JobId"].as_str().unwrap().to_string();
        assert_eq!(job_id.len(), 36);

        let (status, body) = send(
            account,
            "GetDocumentTextDetection",
            json!({ "JobId": job_id }),
        );
        assert_eq!(status, 200);
        assert_eq!(body["JobStatus"], "SUCCEEDED");
        assert_eq!(body["Blocks"], json!([]));
        assert_eq!(
            body["DocumentMetadata"]["Pages"],
            document_metadata()["Pages"]
        );
    }

    #[test]
    fn text_detection_rejects_missing_or_empty_location() {
        // moto: `not document_location` — absent, null and {} all raise.
        for location in [
            json!({}),
            json!({ "DocumentLocation": Value::Null }),
            json!({ "DocumentLocation": {} }),
        ] {
            let (status, body) = send(next_account(), "StartDocumentTextDetection", location);
            assert_eq!(status, 400);
            assert_eq!(body["__type"], "InvalidParameterException");
        }
    }

    #[test]
    fn get_document_text_detection_unknown_job() {
        let (status, body) = send(
            next_account(),
            "GetDocumentTextDetection",
            json!({ "JobId": "00000000-0000-0000-0000-000000000000" }),
        );
        assert_eq!(status, 400);
        assert_eq!(body["__type"], "InvalidJobIdException");
        assert_eq!(body["message"], "An invalid job identifier was passed.");
    }

    #[test]
    fn document_analysis_job_roundtrip() {
        let account = next_account();
        let (status, body) = send(
            account,
            "StartDocumentAnalysis",
            json!({ "DocumentLocation": s3_location(), "FeatureTypes": ["TABLES"] }),
        );
        assert_eq!(status, 200);
        let job_id = body["JobId"].as_str().unwrap().to_string();

        let (status, body) = send(account, "GetDocumentAnalysis", json!({ "JobId": job_id }));
        assert_eq!(status, 200);
        assert_eq!(body["JobStatus"], "SUCCEEDED");
        assert_eq!(body["Blocks"], json!([]));
        assert!(body["DocumentMetadata"]["Pages"]["Pages"].is_i64());
    }

    #[test]
    fn document_analysis_requires_features_and_location() {
        let location = s3_location();
        for body in [
            json!({ "FeatureTypes": ["TABLES"] }), // no location
            json!({}),                             // neither
            json!({ "DocumentLocation": location, "FeatureTypes": [] }), // empty list
        ] {
            let (status, body) = send(next_account(), "StartDocumentAnalysis", body);
            assert_eq!(status, 400);
            assert_eq!(body["__type"], "InvalidParameterException");
        }
        // A location without features is also rejected...
        let (status, body) = send(
            next_account(),
            "StartDocumentAnalysis",
            json!({ "DocumentLocation": s3_location() }),
        );
        assert_eq!(status, 400);
        assert_eq!(body["__type"], "InvalidParameterException");
    }

    #[test]
    fn job_ids_are_op_type_scoped() {
        // moto keeps a separate map per engine: a text-detection job id is
        // not fetchable from GetDocumentAnalysis.
        let account = next_account();
        let (status, body) = send(
            account,
            "StartDocumentTextDetection",
            json!({ "DocumentLocation": s3_location() }),
        );
        assert_eq!(status, 200);
        let job_id = body["JobId"].as_str().unwrap().to_string();

        let (status, body) = send(account, "GetDocumentAnalysis", json!({ "JobId": job_id }));
        assert_eq!(status, 400);
        assert_eq!(body["__type"], "InvalidJobIdException");
    }

    #[test]
    fn analyze_id_fixture() {
        let (status, body) = send(
            next_account(),
            "AnalyzeID",
            json!({ "DocumentPages": [{ "S3Object": { "Bucket": "test-bucket", "Name": "id.pdf" } }] }),
        );
        assert_eq!(status, 200);
        assert_eq!(
            body["DocumentMetadata"]["Pages"]["Pages"].as_i64().unwrap(),
            *DOCUMENT_PAGES
        );
        assert_eq!(body["IdentityDocuments"], json!([]));
        assert_eq!(body["AnalyzeIDModelVersion"], "1.0");
    }

    #[test]
    fn analyze_document_fixture() {
        let (status, body) = send(
            next_account(),
            "AnalyzeDocument",
            json!({ "Document": s3_location(), "FeatureTypes": ["TABLES"] }),
        );
        assert_eq!(status, 200);
        assert_eq!(body["Blocks"], json!([]));
        assert_eq!(body["AnalyzeDocumentModelVersion"], "1.0");
        assert!(body["DocumentMetadata"]["Pages"]["Pages"].is_i64());
    }

    #[test]
    fn analyze_expense_fixture() {
        let (status, body) = send(
            next_account(),
            "AnalyzeExpense",
            json!({ "Document": s3_location() }),
        );
        assert_eq!(status, 200);
        assert_eq!(body["ExpenseDocuments"], json!([]));
        assert!(body["DocumentMetadata"]["Pages"]["Pages"].is_i64());
        assert!(body.get("JobStatus").is_none());
    }

    #[test]
    fn expense_analysis_roundtrip() {
        let account = next_account();
        let (status, body) = send(
            account,
            "StartExpenseAnalysis",
            json!({ "DocumentLocation": s3_location() }),
        );
        assert_eq!(status, 200);
        let job_id = body["JobId"].as_str().unwrap().to_string();

        let (status, body) = send(account, "GetExpenseAnalysis", json!({ "JobId": job_id }));
        assert_eq!(status, 200);
        assert_eq!(body["JobStatus"], "SUCCEEDED");
        assert_eq!(body["ExpenseDocuments"], json!([]));
        assert!(body["DocumentMetadata"]["Pages"]["Pages"].is_i64());

        // An expense job id is not fetchable from the other engines either.
        let (status, body) = send(account, "GetLendingAnalysis", json!({ "JobId": job_id }));
        assert_eq!(status, 400);
        assert_eq!(body["__type"], "InvalidJobIdException");
    }

    #[test]
    fn expense_analysis_rejects_missing_location() {
        let (status, body) = send(next_account(), "StartExpenseAnalysis", json!({}));
        assert_eq!(status, 400);
        assert_eq!(body["__type"], "InvalidParameterException");
    }

    #[test]
    fn expense_analysis_unknown_job() {
        let (status, body) = send(
            next_account(),
            "GetExpenseAnalysis",
            json!({ "JobId": "00000000000000000000000000000000" }),
        );
        assert_eq!(status, 400);
        assert_eq!(body["__type"], "InvalidJobIdException");
    }

    #[test]
    fn lending_analysis_roundtrip() {
        let account = next_account();
        let (status, body) = send(
            account,
            "StartLendingAnalysis",
            json!({ "DocumentLocation": s3_location() }),
        );
        assert_eq!(status, 200);
        let job_id = body["JobId"].as_str().unwrap().to_string();

        for operation in ["GetLendingAnalysis", "GetLendingAnalysisSummary"] {
            let (status, body) = send(account, operation, json!({ "JobId": job_id }));
            assert_eq!(status, 200, "{operation}");
            assert_eq!(body["JobStatus"], "SUCCEEDED");
            assert_eq!(body["Results"], json!([]));
            assert!(body["DocumentMetadata"]["Pages"]["Pages"].is_i64());
        }

        let (status, body) = send(
            account,
            "GetLendingAnalysisSummary",
            json!({ "JobId": "00000000000000000000000000000000" }),
        );
        assert_eq!(status, 400);
        assert_eq!(body["__type"], "InvalidJobIdException");
    }

    #[test]
    fn lending_analysis_rejects_missing_location() {
        let (status, body) = send(next_account(), "StartLendingAnalysis", json!({}));
        assert_eq!(status, 400);
        assert_eq!(body["__type"], "InvalidParameterException");
    }

    #[test]
    fn adapter_crud_roundtrip() {
        let account = next_account();

        // Create with defaults.
        let (status, body) = send(
            account,
            "CreateAdapter",
            json!({ "AdapterName": "general" }),
        );
        assert_eq!(status, 200);
        let adapter_id = body["AdapterId"].as_str().unwrap().to_string();
        assert_eq!(adapter_id.len(), 12);

        // Get honours the creation parameters.
        let (status, body) = send(account, "GetAdapter", json!({ "AdapterId": adapter_id }));
        assert_eq!(status, 200);
        assert_eq!(body["AdapterId"], adapter_id);
        assert_eq!(body["AdapterName"], "general");
        assert_eq!(body["AutoUpdate"], "DISABLED");
        assert_eq!(body["Description"], "");
        assert!(body["CreationTime"].as_str().unwrap().ends_with('Z'));
        assert_eq!(body["Tags"], json!({}));

        // Update only the fields received (moto takes optional overrides).
        let (status, body) = send(
            account,
            "UpdateAdapter",
            json!({ "AdapterId": adapter_id, "AdapterName": "renamed" }),
        );
        assert_eq!(status, 200);
        assert_eq!(body["AdapterId"], adapter_id);
        // Unset fields survive.
        let (_, body) = send(account, "GetAdapter", json!({ "AdapterId": adapter_id }));
        assert_eq!(body["AdapterName"], "renamed");
        assert_eq!(body["AutoUpdate"], "DISABLED");
        assert_eq!(body["Description"], "");

        // List contains the adapter.
        let (_, body) = send(account, "ListAdapters", json!({}));
        let ids: Vec<&str> = body["Adapters"]
            .as_array()
            .unwrap()
            .iter()
            .map(|a| a["AdapterId"].as_str().unwrap())
            .collect();
        assert!(ids.contains(&adapter_id.as_str()));

        // Delete, then it is gone.
        let (status, body) = send(account, "DeleteAdapter", json!({ "AdapterId": adapter_id }));
        assert_eq!(status, 200);
        assert_eq!(body, json!({}));
        let (status, body) = send(account, "GetAdapter", json!({ "AdapterId": adapter_id }));
        assert_eq!(status, 400);
        assert_eq!(body["__type"], "ResourceNotFoundException");

        // update/delete on unknown ids raise the same exception.
        let (status, body) = send(
            account,
            "UpdateAdapter",
            json!({ "AdapterId": adapter_id, "AdapterName": "ghost" }),
        );
        assert_eq!(status, 400);
        assert_eq!(body["__type"], "ResourceNotFoundException");
    }

    #[test]
    fn adapter_version_crud_roundtrip() {
        let account = next_account();

        // Creating a version under a missing adapter fails first.
        let (status, body) = send(
            account,
            "CreateAdapterVersion",
            json!({ "AdapterId": "no-such-adapter" }),
        );
        assert_eq!(status, 400);
        assert_eq!(body["__type"], "ResourceNotFoundException");

        let (_, body) = send(
            account,
            "CreateAdapter",
            json!({ "AdapterName": "host", "FeatureTypes": ["TABLES"] }),
        );
        let adapter_id = body["AdapterId"].as_str().unwrap().to_string();

        let (status, body) = send(
            account,
            "CreateAdapterVersion",
            json!({
                "AdapterId": adapter_id,
                "DatasetConfig": { "ManifestS3Object": s3_location() },
                "OutputConfig": { "S3Bucket": "out", "S3Prefix": "v/" },
            }),
        );
        assert_eq!(status, 200);
        assert_eq!(body["AdapterId"], adapter_id);
        let version = body["AdapterVersion"].as_str().unwrap().to_string();
        assert_eq!(version, "1"); // moto numbers versions by current count

        // Get returns the ACTIVE version; Tags survive create-tags.
        let (_, body) = send(
            account,
            "GetAdapterVersion",
            json!({ "AdapterId": adapter_id, "AdapterVersion": version }),
        );
        assert_eq!(body["Status"], "ACTIVE");
        assert_eq!(body["AdapterVersion"], version);
        assert_eq!(body["Tags"], json!({}));

        // List honours the adapter filter.
        let (_, body) = send(
            account,
            "ListAdapterVersions",
            json!({ "AdapterId": adapter_id }),
        );
        assert_eq!(body["AdapterVersions"].as_array().unwrap().len(), 1);
        // An unfiltered list also finds it.
        let (_, body) = send(account, "ListAdapterVersions", json!({}));
        assert_eq!(body["AdapterVersions"].as_array().unwrap().len(), 1);

        // A missing version raises for adapter_id/version.
        let (status, body) = send(
            account,
            "GetAdapterVersion",
            json!({ "AdapterId": adapter_id, "AdapterVersion": "99" }),
        );
        assert_eq!(status, 400);
        assert_eq!(body["__type"], "ResourceNotFoundException");

        // Delete, then it is gone.
        let (status, body) = send(
            account,
            "DeleteAdapterVersion",
            json!({ "AdapterId": adapter_id, "AdapterVersion": version }),
        );
        assert_eq!(status, 200);
        assert_eq!(body, json!({}));
        let (status, body) = send(
            account,
            "DeleteAdapterVersion",
            json!({ "AdapterId": adapter_id, "AdapterVersion": version }),
        );
        assert_eq!(status, 400);
        assert_eq!(body["__type"], "ResourceNotFoundException");

        // moto numbers by count: after the delete the next create reuses "1".
        let (_, body) = send(
            account,
            "CreateAdapterVersion",
            json!({ "AdapterId": adapter_id, "DatasetConfig": {} }),
        );
        assert_eq!(body["AdapterVersion"], "1");
    }

    #[test]
    fn tagging_roundtrip() {
        let account = next_account();

        // Creation tags land in the tagger map keyed by the adapter ARN.
        let (_, body) = send(
            account,
            "CreateAdapter",
            json!({ "AdapterName": "tagged", "Tags": { "created-by": "test" } }),
        );
        let adapter_id = body["AdapterId"].as_str().unwrap().to_string();
        let arn = format!("arn:aws:textract:us-east-1:{account}:adapter/{adapter_id}");

        let (status, body) = send(
            account,
            "ListTagsForResource",
            json!({ "ResourceARN": arn }),
        );
        assert_eq!(status, 200);
        assert_eq!(body["Tags"]["created-by"], "test");

        // TagResource merges.
        let (status, _) = send(
            account,
            "TagResource",
            json!({ "ResourceARN": arn, "Tags": { "env": "test", "project": "robotocore" } }),
        );
        assert_eq!(status, 200);
        // UntagResource removes only the listed keys.
        let (status, _) = send(
            account,
            "UntagResource",
            json!({ "ResourceARN": arn, "TagKeys": ["env", "missing"] }),
        );
        assert_eq!(status, 200);
        let (_, body) = send(
            account,
            "ListTagsForResource",
            json!({ "ResourceARN": arn }),
        );
        assert_eq!(
            body["Tags"],
            json!({ "project": "robotocore", "created-by": "test" })
        );

        // Unknown ARNs read empty, per moto's tagger.get(arn, {}).
        let (_, body) = send(
            account,
            "ListTagsForResource",
            json!({ "ResourceARN": "arn:aws:textract:us-east-1:1:adapter/none" }),
        );
        assert_eq!(body["Tags"], json!({}));
    }

    #[test]
    fn adapter_state_is_isolated_per_account() {
        // The default scope stays empty even after another account mutates.
        let other = next_account();
        let (status, body) = send(
            other,
            "CreateAdapter",
            json!({ "AdapterName": "theirs", "FeatureTypes": ["FORMS"] }),
        );
        assert_eq!(status, 200);
        let adapter_id = body["AdapterId"].as_str().unwrap().to_string();

        let account = next_account();
        let (_, body) = send(account, "ListAdapters", json!({}));
        assert_eq!(body["Adapters"], json!([]));

        // Same id in the other account is unknown here.
        let (status, body) = send(account, "GetAdapter", json!({ "AdapterId": adapter_id }));
        assert_eq!(status, 400);
        assert_eq!(body["__type"], "ResourceNotFoundException");
    }
}
