//! AWS Textract backend — moto-equivalent (json protocol).
//!
//! Replaces the moto sidecar for `textract` (`X-Amz-Target: Textract.<Op>`
//! on `application/x-amz-json-1.1`). State is per-(account, region) and
//! mirrors moto's `TextractBackend` (moto/textract): the two async-job maps
//! changed by the Start/Get pairs. Every payload is moto's fixed fixture —
//! empty `Blocks`, a page count drawn once per process, `JobStatus`
//! "SUCCEEDED" — because moto parses no documents (`moto/textract/models.py`).
//!
//! Surface (probes/textract.json): only the six operations below were
//! measurable moto-era (`working`, plus `AnalyzeID` whose probe could not
//! fill `DocumentPages` but whose handler exists in moto). The remaining 19
//! operations recorded 501 from the sidecar and stay 501 here — they are on
//! the G2 ledger, not invented.
//!
//! moto quirk kept: `PAGES` is a class attribute, so the randomized page
//! count is drawn once per process and shared by every call in every
//! backend (`PAGES = mock_random.randint(5, 500)`).

use crate::proto::{json_error, parse_json_body};
use crate::store::{self, ScopeKey};
use crate::{Request, Response};
use once_cell::sync::Lazy;
use parking_lot::RwLock;
use serde_json::{json, Value};
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
        // --- synchronous identity analysis ----------------------------------
        "AnalyzeID" => ok(json!({
            "DocumentMetadata": { "Pages": *DOCUMENT_PAGES },
            "IdentityDocuments": [],
            "AnalyzeIDModelVersion": "1.0",
        })),
        // Everything else recorded 501 moto-era (AnalyzeDocument,
        // AnalyzeExpense, Get/Start ExpenseAnalysis, the Lending pair,
        // adapter CRUD + versions, tagging) — prove nothing further.
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

/// `TextractBackend.detect_document_text` — no JobStatus on the wire.
fn text_result_without_status() -> Value {
    json!({
        "Blocks": [],
        "DetectDocumentTextModelVersion": "1.0",
        "DocumentMetadata": { "Pages": *DOCUMENT_PAGES },
    })
}

/// `TextractJob.to_dict()` for engine-started jobs (both text detection
/// and document analysis reuse the same dict).
fn text_job_status_dict() -> Value {
    let mut job = text_result_without_status();
    job["JobStatus"] = json!("SUCCEEDED");
    job
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
        let pages = body["DocumentMetadata"]["Pages"].as_i64().unwrap();
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
            first["DocumentMetadata"]["Pages"].as_i64().unwrap(),
            second["DocumentMetadata"]["Pages"].as_i64().unwrap()
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
        assert!(body["DocumentMetadata"]["Pages"].is_i64());
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
        assert!(body["DocumentMetadata"]["Pages"].is_i64());
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
            body["DocumentMetadata"]["Pages"].as_i64().unwrap(),
            *DOCUMENT_PAGES
        );
        assert_eq!(body["IdentityDocuments"], json!([]));
        assert_eq!(body["AnalyzeIDModelVersion"], "1.0");
    }

    #[test]
    fn unmeasured_operations_stay_not_implemented() {
        // Probe: not_implemented (501) across the rest of the textract
        // surface — out of the moto-era surface, G2 ledger entries.
        for operation in [
            "AnalyzeDocument",
            "AnalyzeExpense",
            "StartExpenseAnalysis",
            "GetExpenseAnalysis",
            "StartLendingAnalysis",
            "GetLendingAnalysis",
            "GetLendingAnalysisSummary",
            "CreateAdapter",
            "GetAdapter",
            "UpdateAdapter",
            "DeleteAdapter",
            "ListAdapters",
            "CreateAdapterVersion",
            "GetAdapterVersion",
            "ListAdapterVersions",
            "DeleteAdapterVersion",
            "TagResource",
            "UntagResource",
            "ListTagsForResource",
        ] {
            let (status, body) = send(next_account(), operation, json!({}));
            assert_eq!(status, 501, "{operation}");
            assert_eq!(body["__type"], "NotImplemented");
        }
    }
}
