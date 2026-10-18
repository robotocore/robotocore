//! AWS Support backend — ported from moto's `support` service (json proto).
//!
//! X-Amz-Target JSON requests: cases CRUD with ManagedState-style status /
//! severity advancement on describe, Trusted Advisor checks (static resource
//! table) with a status machine on refresh, attachment sets, and the
//! communication/attachment read paths.

use crate::proto::parse_json_body;
use crate::store::{self, ScopeKey};
use crate::{Request, Response};
use once_cell::sync::Lazy;
use serde_json::{json, Value};
use std::collections::HashMap;

static SLOT: Lazy<store::Slot<SupportState>> =
    Lazy::new(|| store::slot::<SupportState>("support"));

static ADVISOR_CHECKS: Lazy<Value> = Lazy::new(|| {
    serde_json::from_slice(include_bytes!("support_checks.json")).expect("advisor checks json")
});

#[derive(Default)]
struct SupportState {
    cases: HashMap<String, Value>,
    case_status: HashMap<String, String>,
    severity_codes: HashMap<String, String>,
    check_status: HashMap<String, String>,
    attachment_sets: HashMap<String, Vec<Value>>,
    attachments: HashMap<String, Value>,
    communications: HashMap<String, Vec<Value>>,
}

pub fn dispatch(req: &Request) -> Response {
    let params = parse_json_body(&req.body);
    let mut guard = SLOT.write();
    let st = guard.entry((req.account, req.region.clone())).or_default();
    match req.operation.as_str() {
        "DescribeTrustedAdvisorChecks" => {
            let body = &*ADVISOR_CHECKS;
            Response::json_status(200, body)
        }
        "RefreshTrustedAdvisorCheck" => st.op_refresh_check(&params),
        "ResolveCase" => st.op_resolve_case(&params),
        "CreateCase" => st.op_create_case(&params),
        "DescribeCases" => st.op_describe_cases(&params),
        "AddAttachmentsToSet" => st.op_add_attachments_to_set(&params),
        "DescribeAttachment" => st.op_describe_attachment(&params),
        "DescribeCreateCaseOptions" => Response::json_status(
            200,
            &json!({
                "languageAvailability": "{\"availability\": [{\"language\": \"en\"}]}",
                "communicationTypeOptions": [{
                    "type": "web",
                    "supportedHours": [{"startTime": "06:00", "endTime": "22:00"}],
                    "datesWithoutSupport": [],
                }],
            }),
        ),
        "DescribeSupportedLanguages" => Response::json_status(
            200,
            &json!({
                "supportedLanguages": [
                    {"code": "en", "language": "English", "display": "English"},
                    {"code": "ja", "language": "Japanese", "display": "Japanese"},
                    {"code": "zh", "language": "Chinese", "display": "Chinese"},
                    {"code": "ko", "language": "Korean", "display": "Korean"},
                ],
            }),
        ),
        "DescribeTrustedAdvisorCheckRefreshStatuses" => {
            let ids: Vec<String> = params
                .get("checkIds")
                .and_then(Value::as_array)
                .map(|a| {
                    a.iter()
                        .filter_map(Value::as_str)
                        .map(String::from)
                        .collect()
                })
                .unwrap_or_default();
            let statuses: Vec<Value> = ids
                .iter()
                .map(|cid| {
                    let status = st
                        .check_status
                        .get(cid)
                        .map(|s| s.as_str())
                        .unwrap_or("none")
                        .to_string();
                    json!({"checkId": cid, "status": status})
                })
                .collect();
            Response::json_status(200, &json!({"statuses": statuses}))
        }
        "DescribeServices" => Response::json_status(
            200,
            &json!({"services": support_services()}),
        ),
        "DescribeSeverityLevels" => Response::json_status(
            200,
            &json!({"severityLevels": support_severity_levels()}),
        ),
        "DescribeTrustedAdvisorCheckResult" => {
            let check_id = params.get("checkId").and_then(Value::as_str).unwrap_or("");
            Response::json_status(
                200,
                &json!({
                    "result": ta_check_entry(check_id)
                }),
            )
        }
        "DescribeTrustedAdvisorCheckSummaries" => {
            let ids: Vec<String> = params
                .get("checkIds")
                .and_then(Value::as_array)
                .map(|a| {
                    a.iter()
                        .filter_map(Value::as_str)
                        .map(String::from)
                        .collect()
                })
                .unwrap_or_default();
            let summaries: Vec<Value> = ids
                .iter()
                .map(|cid| {
                    let mut e = ta_check_entry(cid);
                    e.as_object_mut()
                        .map(|o| o.remove("flaggedResources"));
                    e
                })
                .collect();
            Response::json_status(
                200,
                &json!({"summaries": summaries}),
            )
        }
        "AddCommunicationToCase" => {
            let case_id = params
                .get("caseId")
                .and_then(Value::as_str)
                .unwrap_or_default()
                .to_string();
            let body = params
                .get("communicationBody")
                .and_then(Value::as_str)
                .unwrap_or_default()
                .to_string();
            st.communications
                .entry(case_id.clone())
                .or_default()
                .push(json!({
                    "caseId": case_id,
                    "body": body,
                    "submittedBy": "user@example.com",
                    "timeCreated": now_iso_utc(),
                    "attachmentSet": [],
                }));
            Response::json_status(200, &json!({"result": true}))
        }
        "DescribeCommunications" => {
            let case_id = params
                .get("caseId")
                .and_then(Value::as_str)
                .unwrap_or_default()
                .to_string();
            let mut comms = Vec::new();
            if let Some(case) = st.cases.get(&case_id) {
                comms.push(json!({
                    "caseId": case_id,
                    "body": case.get("communicationBody"),
                    "submittedBy": case.get("submittedBy"),
                    "timeCreated": case.get("timeCreated"),
                    "attachmentSet": [],
                }));
            }
            if let Some(extra) = st.communications.get(&case_id) {
                comms.extend(extra.iter().cloned());
            }
            Response::json_status(
                200,
                &json!({"communications": comms, "nextToken": null}),
            )
        }
        _ => crate::proto::json_error(
            501,
            "NotImplemented",
            &format!("The {} action has not been implemented", req.operation),
        ),
    }
}

fn now_iso_utc() -> String {
    chrono::Utc::now().format("%Y-%m-%dT%H:%M:%SZ").to_string()
}

fn ta_check_entry(check_id: &str) -> Value {
    json!({
        "checkId": check_id,
        "timestamp": now_iso_utc(),
        "status": "ok",
        "resourcesSummary": {
            "resourcesProcessed": 0,
            "resourcesFlagged": 0,
            "resourcesIgnored": 0,
            "resourcesSuppressed": 0,
        },
        "categorySpecificSummary": {},
        "flaggedResources": [],
    })
}

fn support_services() -> Vec<Value> {
    vec![
    json!({
        "code": "amazon-dynamodb",
        "name": "Amazon DynamoDB",
        "categories": [
            {"code": "apis", "name": "APIs"},
            {"code": "other", "name": "Other"},
        ],
    }),
    json!({
        "code": "amazon-s3",
        "name": "Amazon Simple Storage Service",
        "categories": [
            {"code": "general-guidance", "name": "General Guidance"},
            {"code": "other", "name": "Other"},
        ],
    }),
    json!({
        "code": "amazon-ec2",
        "name": "Amazon Elastic Compute Cloud",
        "categories": [
            {"code": "instance-issue", "name": "Instance Issue"},
            {"code": "other", "name": "Other"},
        ],
    }),
    json!({
        "code": "amazon-rds",
        "name": "Amazon Relational Database Service",
        "categories": [{"code": "other", "name": "Other"}],
    }),
    json!({
        "code": "amazon-sqs",
        "name": "Amazon Simple Queue Service",
        "categories": [{"code": "other", "name": "Other"}],
    }),
    json!({
        "code": "general-info",
        "name": "General Info and Getting Started",
        "categories": [{"code": "other", "name": "Other"}],
    })
    ]
}

fn support_severity_levels() -> Vec<Value> {
    vec![
        json!({"code": "low", "name": "Low"}),
        json!({"code": "normal", "name": "Normal"}),
        json!({"code": "high", "name": "High"}),
        json!({"code": "urgent", "name": "Urgent"}),
        json!({"code": "critical", "name": "Critical"}),
    ]
}

/// The ManagedState cycle used by moto for case statuses.
const CASE_TRANSITIONS: &[&str] = &[
    "opened",
    "pending-customer-action",
    "reopened",
    "resolved",
    "unassigned",
    "work-in-progress",
];

fn next_case_status(current: &str) -> String {
    let idx = CASE_TRANSITIONS
        .iter()
        .position(|s| s == &current)
        .map(|i| (i + 1) % CASE_TRANSITIONS.len())
        .unwrap_or(0);
    CASE_TRANSITIONS[idx].to_string()
}

fn next_severity(current: &str) -> String {
    let chain = ["low", "normal", "high", "urgent", "critical"];
    let idx = chain
        .iter()
        .position(|c| c == &current)
        .map(|i| (i + 1) % chain.len())
        .unwrap_or(0);
    chain[idx].to_string()
}

fn now_iso() -> String {
    chrono::Utc::now().format("%Y-%m-%dT%H:%M:%S%.6f").to_string()
}

impl SupportState {
    fn op_refresh_check(&mut self, p: &Value) -> Response {
        let check_id = p
            .get("checkId")
            .and_then(Value::as_str)
            .unwrap_or_default()
            .to_string();
        let status = self
            .check_status
            .get(&check_id)
            .map(|c| c.as_str().to_string());
        let next = match status.as_deref() {
            None => "none",
            Some("none") => "enqueued",
            Some("enqueued") => "processing",
            Some("processing") => "success",
            Some("success") => "abandoned",
            _ => "none",
        };
        self.check_status.insert(check_id.clone(), next.to_string());
        Response::json_status(
            200,
            &json!({
                "status": {
                    "checkId": check_id,
                    "status": next,
                    "millisUntilNextRefreshable": 123,
                }
            }),
        )
    }

    fn op_resolve_case(&mut self, p: &Value) -> Response {
        let case_id = p.get("caseId").and_then(Value::as_str).unwrap_or("");
        let initial_status = match self.case_status.get(case_id).cloned() {
            Some(cur) => {
                let nxt = next_case_status(&cur);
                self.case_status.insert(case_id.to_string(), nxt);
                cur
            }
            None => "unassigned".to_string(),
        };
        Response::json_status(
            200,
            &json!({
                "initialCaseStatus": initial_status,
                "finalCaseStatus": "resolved",
            }),
        )
    }

    fn op_create_case(&mut self, p: &Value) -> Response {
        let id = format!("case-12345678910-2020-{}", Uid::new());
        let lang = p.get("language").cloned().unwrap_or(json!("en"));
        let case = json!({
            "caseId": id,
            "displayId": "foo_display_id",
            "subject": p.get("subject"),
            "serviceCode": p.get("serviceCode"),
            "severityCode": p.get("severityCode").cloned().unwrap_or(json!("low")),
            "categoryCode": p.get("categoryCode"),
            "submittedBy": "moto@moto.com",
            "timeCreated": now_iso(),
            "ccEmailAddresses": p.get("ccEmailAddresses"),
            "language": lang,
            "communicationBody": p.get("communicationBody"),
            "attachmentSetId": p.get("attachmentSetId"),
        });
        self.cases.insert(id.clone(), case);
        self.case_status.insert(id.clone(), "opened".to_string());
        self.severity_codes.insert(id.clone(), "low".to_string());
        Response::json_status(200, &json!({ "caseId": id }))
    }

    fn op_describe_cases(&mut self, p: &Value) -> Response {
        let include_resolved = p
            .get("includeResolvedCases")
            .and_then(Value::as_bool)
            .unwrap_or(false);
        let include_comms = p
            .get("includeCommunications")
            .and_then(Value::as_bool)
            .unwrap_or(true);
        let ids: Vec<String> = p
            .get("caseIdList")
            .and_then(Value::as_array)
            .map(|a| {
                a.iter()
                    .filter_map(Value::as_str)
                    .map(String::from)
                    .collect()
            })
            .unwrap_or_else(|| self.cases.keys().cloned().collect());
        let mut cases = Vec::new();
        for id in ids {
            let Some(case) = self.cases.get(&id) else {
                continue;
            };
            // moto advances status and severity on every describe
            let cur = self.case_status.get(&id).cloned().unwrap_or_default();
            let nxt = next_case_status(&cur);
            self.case_status.insert(id.clone(), nxt.clone());
            let sev = self.severity_codes.get(&id).cloned().unwrap_or_default();
            let sev_next = next_severity(&sev);
            self.severity_codes.insert(id.clone(), sev_next.clone());
            let mut body = case.clone();
            body["status"] = json!(nxt);
            body["severityCode"] = json!(sev_next);
            if include_comms {
                body["recentCommunications"] = json!({
                    "communications": [{
                        "caseId": id,
                        "body": case.get("communicationBody"),
                        "submittedBy": case.get("submittedBy"),
                        "timeCreated": case.get("timeCreated"),
                        "attachmentSet": [{
                            "attachmentId": case.get("attachmentSetId"),
                            "fileName": "support_file.txt",
                        }],
                    }],
                    "nextToken": "foo_next_token",
                });
            }
            if !include_resolved && nxt == "resolved" {
                continue;
            }
            cases.push(sanitize(&body));
        }
        let token = p.get("nextToken").cloned().unwrap_or(json!(null));
        Response::json_status(200, &json!({"cases": cases, "nextToken": token}))
    }

    fn op_add_attachments_to_set(&mut self, p: &Value) -> Response {
        let Some(attachments) = p.get("attachments").and_then(Value::as_array) else {
            return crate::proto::json_error(
                400,
                "ValidationError",
                "1 validation error detected: Value at 'attachments' failed to satisfy constraint: Member must not be null",
            );
        };
        let set_id = p
            .get("attachmentSetId")
            .and_then(Value::as_str)
            .map(String::from)
            .unwrap_or_else(|| Uid::hex32());
        let entry = self.attachment_sets.entry(set_id.clone()).or_default();
        for att in attachments {
            let mut att = att.clone();
            let att_id = format!("{}-{}", Uid::hex32(), att.get("fileName").and_then(Value::as_str).unwrap_or("file"));
            att["attachmentId"] = json!(att_id);
            self.attachments.insert(att_id.clone(), att.clone());
            entry.push(att);
        }
        let resp = json!({
            "attachmentSetId": set_id,
            "expiryTime": now_iso_plus(chrono::Duration::hours(1)),
        });
        Response::json_status(200, &resp)
    }

    fn op_describe_attachment(&mut self, p: &Value) -> Response {
        let id = p
            .get("attachmentId")
            .and_then(Value::as_str)
            .unwrap_or_default();
        let Some(att) = self.attachments.get(id) else {
            return crate::proto::json_error(
                400,
                "AttachmentIdNotFound",
                &format!("Attachment ID not found: {}", id),
            );
        };
        Response::json_status(200, &json!({"attachment": att}))
    }
}

// helper struct to generate ids without pulling a fourth uuid-flow
struct Uid;

impl Uid {
    fn new() -> String {
        let bytes = uuid::Uuid::new_v4();
        bytes
            .as_bytes()
            .iter()
            .map(|b| {
                "0123456789ABCDEFGHIJKLMabcdefghijklm"
                    .as_bytes()[(*b % 37) as usize] as char
            })
            .collect()
    }

    fn hex32() -> String {
        uuid::Uuid::new_v4().simple().to_string()[..32].to_string()
    }
}

fn now_iso_plus(d: chrono::Duration) -> String {
    chrono::Utc::now()
        .checked_add_signed(d)
        .map(|dt| dt.format("%Y-%m-%dT%H:%M:%S%.6f").to_string())
        .unwrap_or_else(now_iso)
}

/// Drop null members (moto's serializer behavior).
fn sanitize(v: &Value) -> Value {
    match v {
        Value::Object(map) => {
            let mut out = serde_json::Map::new();
            for (k, val) in map {
                if !val.is_null() {
                    out.insert(k.clone(), val.clone());
                }
            }
            Value::Object(out)
        }
        other => other.clone(),
    }
}
