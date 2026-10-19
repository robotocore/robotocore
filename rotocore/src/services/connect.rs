//! Amazon Connect backend (rest-json) — the moto-connect semantics in Rust.
//!
//! Replaces the moto bridge for `connect` entirely: every operation in the
//! botocore 2017-08-08 spec (367 ops) is classified against the vendored
//! moto module (`moto/connect`) and served either natively (the 219 ops the
//! moto handlers implement) or denied with byte-equal 501s for moto's own
//! gaps — `The <snake> action has not been implemented` for ops moto's URL
//! table routes but never implemented, `Service connect is not yet
//! implemented` for everything else.
//!
//! State is per (account, region). Instances own per-instance resource
//! stores (agent statuses, flows, queues, users, ...) like moto's
//! `ConnectBackend`; phone numbers and traffic distribution groups are
//! backend-global; tags are keyed by resource ARN (moto's TaggingService).
//!
//! Wire contract (captured from the ground-truth gateway):
//! - 200 bodies: moto's `json.dumps` payload (`", "`/`": "` separators),
//!   content-type mirrors lando's default `application/xml`.
//! - `JsonRESTError` failures: HTTP 400/404/409 per class, body
//!   `{"__type": Code, "message": msg}` and `x-amzn-ErrorType: <Code>`.
//! - Missing required body members hit moto's KeyError path:
//!   500 `{"__type": "InternalError", "message": "'Key'"}`.
//! - 501 deny: `{"__type": "NotImplemented", "message": ...}` over
//!   `application/x-amz-json-1.1`.

use crate::store::{self, Slot};
use crate::{Request, Response};
use once_cell::sync::Lazy;
use serde_json::{json, Map, Value};
use std::collections::{BTreeMap, HashMap};

// ---------------------------------------------------------------------------
// Wire helpers
// ---------------------------------------------------------------------------

fn ok(value: Value) -> Response {
    Response::new(200, "application/xml", py_json(&value).into_bytes())
}

/// `json.dumps` serialization with Python's default separators.
fn py_json(value: &Value) -> String {
    fmt_py(value)
}

fn json_str(s: &str) -> String {
    fmt_py_str(s)
}

fn fmt_py(v: &Value) -> String {
    match v {
        Value::Null => "null".to_string(),
        Value::Bool(b) => b.to_string(),
        Value::Number(n) => n.to_string(),
        Value::String(s) => fmt_py_str(s),
        Value::Array(items) => format!(
            "[{}]",
            items.iter().map(fmt_py).collect::<Vec<_>>().join(", ")
        ),
        Value::Object(map) => format!(
            "{{{}}}",
            map.iter()
                .map(|(k, v)| format!("{}: {}", fmt_py_str(k), fmt_py(v)))
                .collect::<Vec<_>>()
                .join(", ")
        ),
    }
}

/// Python `json.dumps(ensure_ascii=True)` string escaping.
fn fmt_py_str(s: &str) -> String {
    let mut out = String::with_capacity(s.len() + 2);
    out.push('"');
    for c in s.chars() {
        match c {
            '"' => out.push_str("\\\""),
            '\\' => out.push_str("\\\\"),
            '\u{7}' => out.push_str("\\u0007"),
            '\u{8}' => out.push_str("\\b"),
            '\t' => out.push_str("\\t"),
            '\n' => out.push_str("\\n"),
            '\u{b}' => out.push_str("\\u000b"),
            '\u{c}' => out.push_str("\\f"),
            '\r' => out.push_str("\\r"),
            c if (c as u32) < 0x20 => out.push_str(&format!("\\u{:04x}", c as u32)),
            c if (c as u32) < 0x7f => out.push(c),
            c => {
                let mut buf = [0u16; 2];
                for u in c.encode_utf16(&mut buf) {
                    out.push_str(&format!("\\u{:04x}", u));
                }
            }
        }
    }
    out.push('"');
    out
}

fn json_inner(s: &str) -> String {
    let quoted = fmt_py_str(s);
    quoted[1..quoted.len() - 1].to_string()
}

/// moto `JsonRESTError`: status per class, `__type` body, ErrorType header.
fn error_body(status: u16, code: &str, message: &str) -> Response {
    let body = format!(
        "{{\"__type\": {}, \"message\": {}}}",
        json_str(code),
        json_str(message)
    );
    let mut resp = Response::new(status, "application/json", body.into_bytes());
    resp.headers
        .push(("x-amzn-ErrorType".to_string(), code.to_string()));
    resp
}

fn not_found(message: impl Into<String>) -> Response {
    error_body(404, "ResourceNotFoundException", &message.into())
}

fn invalid(message: impl Into<String>) -> Response {
    error_body(400, "InvalidParameterException", &message.into())
}

#[allow(dead_code)] // consumed by the tags/json-error families below
fn duplicate(message: impl Into<String>) -> Response {
    error_body(409, "DuplicateResourceException", &message.into())
}

fn invalid_token(message: impl Into<String>) -> Response {
    error_body(400, "InvalidToken", &message.into())
}

/// moto handler KeyError on a required body member:
/// 500 `{"__type": "InternalError", "message": "'Key'"}`.
fn missing_body_param(key: &str) -> Response {
    error_body(500, "InternalError", &format!("'{key}'"))
}

/// Ops moto routes (URL table) but never implemented.
fn op_not_implemented(op: &str) -> Response {
    Response::new(
        501,
        "application/x-amz-json-1.1",
        format!(
            "{{\"__type\": \"NotImplemented\", \"message\": \"The {} action has not been implemented\"}}",
            json_inner(&to_snake_case(op))
        )
        .into_bytes(),
    )
}

/// Ops with no moto URL routing at all (plus any raw shape the spec table
/// does not name).
fn service_not_implemented() -> Response {
    Response::new(
        501,
        "application/x-amz-json-1.1",
        br#"{"__type": "NotImplemented", "message": "Service connect is not yet implemented"}"#
            .to_vec(),
    )
}

/// Python `datetime.now(timezone.utc).isoformat()` (microseconds, `+00:00`,
/// zero fraction elided).
fn now_iso() -> String {
    let t = chrono::Utc::now().to_rfc3339_opts(chrono::SecondsFormat::Micros, false);
    let Some(dot) = t.find('.') else {
        return t;
    };
    let frac = t[dot + 1..].trim_end_matches("+00:00");
    if frac.chars().all(|c| c == '0') {
        format!("{}+00:00", &t[..dot])
    } else {
        t
    }
}

#[allow(dead_code)] // consumed by the contact-flow family below
fn sha256_hex(input: &str) -> String {
    use sha2::{Digest, Sha256};
    let mut h = Sha256::new();
    h.update(input.as_bytes());
    hex::encode(h.finalize())
}

fn to_snake_case(s: &str) -> String {
    let mut out = String::with_capacity(s.len() + 4);
    let chars: Vec<char> = s.chars().collect();
    for (i, c) in chars.iter().copied().enumerate() {
        if c.is_ascii_uppercase() {
            let next_upper = chars
                .get(i + 1)
                .is_some_and(|n| n.is_ascii_uppercase() || n.is_ascii_digit());
            if i > 0
                && (chars[i - 1].is_ascii_lowercase()
                    || chars[i - 1].is_ascii_digit()
                    || (chars[i - 1].is_ascii_uppercase() && !next_upper && i + 1 < chars.len()))
            {
                out.push('_');
            }
            out.push(c.to_ascii_lowercase());
        } else {
            out.push(c);
        }
    }
    out
}

// ---------------------------------------------------------------------------
// Operation table (botocore connect 2017-08-08 + moto routing classification)
// ---------------------------------------------------------------------------

/// `(method, requestUri, operation, class, greedy, uri-params)` — class `H`
/// (moto handler exists → native port), `H501` (moto routes the URL but the
/// handler is absent), `R501` (no moto URL row at all).
struct OpRow {
    method: &'static str,
    path: &'static str,
    op: &'static str,
    kind: &'static str,
    greedy: bool,
    /// URI parameter names in path order.
    params: &'static [&'static str],
}

macro_rules! rows {
    ($($m:literal, $p:literal, $o:literal, $k:literal, $g:literal, [$($param:literal),*];)* $(;)?) => {{
        vec![$(
            OpRow { method: $m, path: $p, op: $o, kind: $k, greedy: $g, params: &[$($param),*] }
        ),*]
    }};
}

static OPS: Lazy<Vec<OpRow>> = Lazy::new(|| {
    rows![
    // NOTE: rows generated from botocore connect 2017-08-08 with moto's
    // URL-table classification (H = moto handler implemented, H501 = routed
    // but unimplemented in moto, R501 = not routed by moto at all).
        "POST", "evaluation-forms/{InstanceId}/{EvaluationFormId}/activate", "ActivateEvaluationForm", "H", false, ["InstanceId", "EvaluationFormId"];
        "PUT", "analytics-data/instance/{InstanceId}/association", "AssociateAnalyticsDataSet", "H", false, ["InstanceId"];
        "PUT", "instance/{InstanceId}/approved-origin", "AssociateApprovedOrigin", "H", false, ["InstanceId"];
        "PUT", "instance/{InstanceId}/bot", "AssociateBot", "H", false, ["InstanceId"];
        "POST", "contacts/{InstanceId}/{ContactId}/associate-user", "AssociateContactWithUser", "R501", false, ["InstanceId", "ContactId"];
        "PUT", "default-vocabulary/{InstanceId}/{LanguageCode}", "AssociateDefaultVocabulary", "R501", false, ["InstanceId", "LanguageCode"];
        "POST", "email-addresses/{InstanceId}/{EmailAddressId}/associate-alias", "AssociateEmailAddressAlias", "R501", false, ["InstanceId", "EmailAddressId"];
        "PUT", "flow-associations/{InstanceId}", "AssociateFlow", "R501", false, ["InstanceId"];
        "POST", "hours-of-operations/{InstanceId}/{HoursOfOperationId}/associate-hours", "AssociateHoursOfOperations", "R501", false, ["InstanceId", "HoursOfOperationId"];
        "PUT", "instance/{InstanceId}/storage-config", "AssociateInstanceStorageConfig", "H", false, ["InstanceId"];
        "PUT", "instance/{InstanceId}/lambda-function", "AssociateLambdaFunction", "H", false, ["InstanceId"];
        "PUT", "instance/{InstanceId}/lex-bot", "AssociateLexBot", "R501", false, ["InstanceId"];
        "PUT", "phone-number/{PhoneNumberId}/contact-flow", "AssociatePhoneNumberContactFlow", "R501", false, ["PhoneNumberId"];
        "POST", "queues/{InstanceId}/{QueueId}/associate-email-addresses", "AssociateQueueEmailAddresses", "R501", false, ["InstanceId", "QueueId"];
        "POST", "queues/{InstanceId}/{QueueId}/associate-quick-connects", "AssociateQueueQuickConnects", "H", false, ["InstanceId", "QueueId"];
        "POST", "routing-profiles/{InstanceId}/{RoutingProfileId}/associate-queues", "AssociateRoutingProfileQueues", "H", false, ["InstanceId", "RoutingProfileId"];
        "PUT", "instance/{InstanceId}/security-key", "AssociateSecurityKey", "H", false, ["InstanceId"];
        "POST", "associate-security-profiles/{InstanceId}", "AssociateSecurityProfiles", "R501", false, ["InstanceId"];
        "PUT", "traffic-distribution-group/{TrafficDistributionGroupId}/user", "AssociateTrafficDistributionGroupUser", "R501", false, ["TrafficDistributionGroupId"];
        "POST", "users/{InstanceId}/{UserId}/associate-proficiencies", "AssociateUserProficiencies", "R501", false, ["InstanceId", "UserId"];
        "POST", "workspaces/{InstanceId}/{WorkspaceId}/associate", "AssociateWorkspace", "R501", false, ["InstanceId", "WorkspaceId"];
        "PUT", "analytics-data/instance/{InstanceId}/associations", "BatchAssociateAnalyticsDataSet", "R501", false, ["InstanceId"];
        "POST", "data-tables/{InstanceId}/{DataTableId}/values/create", "BatchCreateDataTableValue", "R501", false, ["InstanceId", "DataTableId"];
        "POST", "data-tables/{InstanceId}/{DataTableId}/values/delete", "BatchDeleteDataTableValue", "R501", false, ["InstanceId", "DataTableId"];
        "POST", "data-tables/{InstanceId}/{DataTableId}/values/describe", "BatchDescribeDataTableValue", "R501", false, ["InstanceId", "DataTableId"];
        "POST", "analytics-data/instance/{InstanceId}/associations", "BatchDisassociateAnalyticsDataSet", "R501", false, ["InstanceId"];
        "POST", "attached-files/{InstanceId}", "BatchGetAttachedFileMetadata", "R501", false, ["InstanceId"];
        "POST", "flow-associations-batch/{InstanceId}", "BatchGetFlowAssociation", "R501", false, ["InstanceId"];
        "PUT", "contact/batch/{InstanceId}", "BatchPutContact", "R501", false, ["InstanceId"];
        "POST", "data-tables/{InstanceId}/{DataTableId}/values/update", "BatchUpdateDataTableValue", "R501", false, ["InstanceId", "DataTableId"];
        "POST", "phone-number/claim", "ClaimPhoneNumber", "H", false, [];
        "POST", "attached-files/{InstanceId}/{FileId}", "CompleteAttachedFileUpload", "R501", false, ["InstanceId", "FileId"];
        "PUT", "agent-status/{InstanceId}", "CreateAgentStatus", "H", false, ["InstanceId"];
        "PUT", "contact/create-contact", "CreateContact", "H", false, [];
        "PUT", "contact-flows/{InstanceId}", "CreateContactFlow", "H", false, ["InstanceId"];
        "PUT", "contact-flow-modules/{InstanceId}", "CreateContactFlowModule", "H", false, ["InstanceId"];
        "PUT", "contact-flow-modules/{InstanceId}/{ContactFlowModuleId}/alias", "CreateContactFlowModuleAlias", "H", false, ["InstanceId", "ContactFlowModuleId"];
        "PUT", "contact-flow-modules/{InstanceId}/{ContactFlowModuleId}/version", "CreateContactFlowModuleVersion", "R501", false, ["InstanceId", "ContactFlowModuleId"];
        "PUT", "contact-flows/{InstanceId}/{ContactFlowId}/version", "CreateContactFlowVersion", "R501", false, ["InstanceId", "ContactFlowId"];
        "PUT", "data-tables/{InstanceId}", "CreateDataTable", "H", false, ["InstanceId"];
        "PUT", "data-tables/{InstanceId}/{DataTableId}/attributes", "CreateDataTableAttribute", "H", false, ["InstanceId", "DataTableId"];
        "PUT", "email-addresses/{InstanceId}", "CreateEmailAddress", "H", false, ["InstanceId"];
        "PUT", "evaluation-forms/{InstanceId}", "CreateEvaluationForm", "H", false, ["InstanceId"];
        "PUT", "hours-of-operations/{InstanceId}", "CreateHoursOfOperation", "H", false, ["InstanceId"];
        "PUT", "hours-of-operations/{InstanceId}/{HoursOfOperationId}/overrides", "CreateHoursOfOperationOverride", "H", false, ["InstanceId", "HoursOfOperationId"];
        "PUT", "instance", "CreateInstance", "H", false, [];
        "PUT", "instance/{InstanceId}/integration-associations", "CreateIntegrationAssociation", "H", false, ["InstanceId"];
        "PUT", "notifications/{InstanceId}", "CreateNotification", "H", false, ["InstanceId"];
        "POST", "contact/create-participant", "CreateParticipant", "R501", false, [];
        "POST", "contact/persistent-contact-association/{InstanceId}/{InitialContactId}", "CreatePersistentContactAssociation", "R501", false, ["InstanceId", "InitialContactId"];
        "PUT", "predefined-attributes/{InstanceId}", "CreatePredefinedAttribute", "H", false, ["InstanceId"];
        "PUT", "prompts/{InstanceId}", "CreatePrompt", "H", false, ["InstanceId"];
        "PUT", "push-notification/{InstanceId}/registrations", "CreatePushNotificationRegistration", "R501", false, ["InstanceId"];
        "PUT", "queues/{InstanceId}", "CreateQueue", "H", false, ["InstanceId"];
        "PUT", "quick-connects/{InstanceId}", "CreateQuickConnect", "H", false, ["InstanceId"];
        "PUT", "routing-profiles/{InstanceId}", "CreateRoutingProfile", "H", false, ["InstanceId"];
        "POST", "rules/{InstanceId}", "CreateRule", "H", false, ["InstanceId"];
        "PUT", "security-profiles/{InstanceId}", "CreateSecurityProfile", "H", false, ["InstanceId"];
        "PUT", "instance/{InstanceId}/task/template", "CreateTaskTemplate", "H", false, ["InstanceId"];
        "PUT", "test-cases/{InstanceId}", "CreateTestCase", "H", false, ["InstanceId"];
        "PUT", "traffic-distribution-group", "CreateTrafficDistributionGroup", "H", false, [];
        "PUT", "instance/{InstanceId}/integration-associations/{IntegrationAssociationId}/use-cases", "CreateUseCase", "H", false, ["InstanceId", "IntegrationAssociationId"];
        "PUT", "users/{InstanceId}", "CreateUser", "H", false, ["InstanceId"];
        "PUT", "user-hierarchy-groups/{InstanceId}", "CreateUserHierarchyGroup", "H", false, ["InstanceId"];
        "PUT", "views/{InstanceId}", "CreateView", "H", false, ["InstanceId"];
        "PUT", "views/{InstanceId}/{ViewId}/versions", "CreateViewVersion", "R501", false, ["InstanceId", "ViewId"];
        "POST", "vocabulary/{InstanceId}", "CreateVocabulary", "H", false, ["InstanceId"];
        "PUT", "workspaces/{InstanceId}", "CreateWorkspace", "H", false, ["InstanceId"];
        "PUT", "workspaces/{InstanceId}/{WorkspaceId}/pages", "CreateWorkspacePage", "R501", false, ["InstanceId", "WorkspaceId"];
        "POST", "evaluation-forms/{InstanceId}/{EvaluationFormId}/deactivate", "DeactivateEvaluationForm", "H", false, ["InstanceId", "EvaluationFormId"];
        "DELETE", "attached-files/{InstanceId}/{FileId}", "DeleteAttachedFile", "R501", false, ["InstanceId", "FileId"];
        "DELETE", "contact-evaluations/{InstanceId}/{EvaluationId}", "DeleteContactEvaluation", "H", false, ["InstanceId", "EvaluationId"];
        "DELETE", "contact-flows/{InstanceId}/{ContactFlowId}", "DeleteContactFlow", "H", false, ["InstanceId", "ContactFlowId"];
        "DELETE", "contact-flow-modules/{InstanceId}/{ContactFlowModuleId}", "DeleteContactFlowModule", "H", false, ["InstanceId", "ContactFlowModuleId"];
        "DELETE", "contact-flow-modules/{InstanceId}/{ContactFlowModuleId}/alias/{AliasId}", "DeleteContactFlowModuleAlias", "H", false, ["InstanceId", "ContactFlowModuleId", "AliasId"];
        "DELETE", "contact-flow-modules/{InstanceId}/{ContactFlowModuleId}/version/{ContactFlowModuleVersion}", "DeleteContactFlowModuleVersion", "R501", false, ["InstanceId", "ContactFlowModuleId", "ContactFlowModuleVersion"];
        "DELETE", "contact-flows/{InstanceId}/{ContactFlowId}/version/{ContactFlowVersion}", "DeleteContactFlowVersion", "R501", false, ["InstanceId", "ContactFlowId", "ContactFlowVersion"];
        "DELETE", "data-tables/{InstanceId}/{DataTableId}", "DeleteDataTable", "H", false, ["InstanceId", "DataTableId"];
        "DELETE", "data-tables/{InstanceId}/{DataTableId}/attributes/{AttributeName}", "DeleteDataTableAttribute", "H", false, ["InstanceId", "DataTableId", "AttributeName"];
        "DELETE", "email-addresses/{InstanceId}/{EmailAddressId}", "DeleteEmailAddress", "H", false, ["InstanceId", "EmailAddressId"];
        "DELETE", "evaluation-forms/{InstanceId}/{EvaluationFormId}", "DeleteEvaluationForm", "H", false, ["InstanceId", "EvaluationFormId"];
        "DELETE", "hours-of-operations/{InstanceId}/{HoursOfOperationId}", "DeleteHoursOfOperation", "H", false, ["InstanceId", "HoursOfOperationId"];
        "DELETE", "hours-of-operations/{InstanceId}/{HoursOfOperationId}/overrides/{HoursOfOperationOverrideId}", "DeleteHoursOfOperationOverride", "H", false, ["InstanceId", "HoursOfOperationId", "HoursOfOperationOverrideId"];
        "DELETE", "instance/{InstanceId}", "DeleteInstance", "H", false, ["InstanceId"];
        "DELETE", "instance/{InstanceId}/integration-associations/{IntegrationAssociationId}", "DeleteIntegrationAssociation", "H", false, ["InstanceId", "IntegrationAssociationId"];
        "DELETE", "notifications/{InstanceId}/{NotificationId}", "DeleteNotification", "H", false, ["InstanceId", "NotificationId"];
        "DELETE", "predefined-attributes/{InstanceId}/{Name}", "DeletePredefinedAttribute", "H", false, ["InstanceId", "Name"];
        "DELETE", "prompts/{InstanceId}/{PromptId}", "DeletePrompt", "H", false, ["InstanceId", "PromptId"];
        "DELETE", "push-notification/{InstanceId}/registrations/{RegistrationId}", "DeletePushNotificationRegistration", "R501", false, ["InstanceId", "RegistrationId"];
        "DELETE", "queues/{InstanceId}/{QueueId}", "DeleteQueue", "H", false, ["InstanceId", "QueueId"];
        "DELETE", "quick-connects/{InstanceId}/{QuickConnectId}", "DeleteQuickConnect", "H", false, ["InstanceId", "QuickConnectId"];
        "DELETE", "routing-profiles/{InstanceId}/{RoutingProfileId}", "DeleteRoutingProfile", "H", false, ["InstanceId", "RoutingProfileId"];
        "DELETE", "rules/{InstanceId}/{RuleId}", "DeleteRule", "H", false, ["InstanceId", "RuleId"];
        "DELETE", "security-profiles/{InstanceId}/{SecurityProfileId}", "DeleteSecurityProfile", "H", false, ["InstanceId", "SecurityProfileId"];
        "DELETE", "instance/{InstanceId}/task/template/{TaskTemplateId}", "DeleteTaskTemplate", "H", false, ["InstanceId", "TaskTemplateId"];
        "DELETE", "test-cases/{InstanceId}/{TestCaseId}", "DeleteTestCase", "H", false, ["InstanceId", "TestCaseId"];
        "DELETE", "traffic-distribution-group/{TrafficDistributionGroupId}", "DeleteTrafficDistributionGroup", "H", false, ["TrafficDistributionGroupId"];
        "DELETE", "instance/{InstanceId}/integration-associations/{IntegrationAssociationId}/use-cases/{UseCaseId}", "DeleteUseCase", "H", false, ["InstanceId", "IntegrationAssociationId", "UseCaseId"];
        "DELETE", "users/{InstanceId}/{UserId}", "DeleteUser", "H", false, ["InstanceId", "UserId"];
        "DELETE", "user-hierarchy-groups/{InstanceId}/{HierarchyGroupId}", "DeleteUserHierarchyGroup", "H", false, ["InstanceId", "HierarchyGroupId"];
        "DELETE", "views/{InstanceId}/{ViewId}", "DeleteView", "H", false, ["InstanceId", "ViewId"];
        "DELETE", "views/{InstanceId}/{ViewId}/versions/{ViewVersion}", "DeleteViewVersion", "R501", false, ["InstanceId", "ViewId", "ViewVersion"];
        "POST", "vocabulary-remove/{InstanceId}/{VocabularyId}", "DeleteVocabulary", "H", false, ["InstanceId", "VocabularyId"];
        "DELETE", "workspaces/{InstanceId}/{WorkspaceId}", "DeleteWorkspace", "H", false, ["InstanceId", "WorkspaceId"];
        "DELETE", "workspaces/{InstanceId}/{WorkspaceId}/media", "DeleteWorkspaceMedia", "R501", false, ["InstanceId", "WorkspaceId"];
        "DELETE", "workspaces/{InstanceId}/{WorkspaceId}/pages/{Page}", "DeleteWorkspacePage", "R501", false, ["InstanceId", "WorkspaceId", "Page"];
        "GET", "agent-status/{InstanceId}/{AgentStatusId}", "DescribeAgentStatus", "H", false, ["InstanceId", "AgentStatusId"];
        "GET", "authentication-profiles/{InstanceId}/{AuthenticationProfileId}", "DescribeAuthenticationProfile", "R501", false, ["InstanceId", "AuthenticationProfileId"];
        "GET", "contacts/{InstanceId}/{ContactId}", "DescribeContact", "H", false, ["InstanceId", "ContactId"];
        "GET", "contact-evaluations/{InstanceId}/{EvaluationId}", "DescribeContactEvaluation", "H", false, ["InstanceId", "EvaluationId"];
        "GET", "contact-flows/{InstanceId}/{ContactFlowId}", "DescribeContactFlow", "H", false, ["InstanceId", "ContactFlowId"];
        "GET", "contact-flow-modules/{InstanceId}/{ContactFlowModuleId}", "DescribeContactFlowModule", "H", false, ["InstanceId", "ContactFlowModuleId"];
        "GET", "contact-flow-modules/{InstanceId}/{ContactFlowModuleId}/alias/{AliasId}", "DescribeContactFlowModuleAlias", "H", false, ["InstanceId", "ContactFlowModuleId", "AliasId"];
        "GET", "data-tables/{InstanceId}/{DataTableId}", "DescribeDataTable", "H", false, ["InstanceId", "DataTableId"];
        "GET", "data-tables/{InstanceId}/{DataTableId}/attributes/{AttributeName}", "DescribeDataTableAttribute", "H", false, ["InstanceId", "DataTableId", "AttributeName"];
        "GET", "email-addresses/{InstanceId}/{EmailAddressId}", "DescribeEmailAddress", "H", false, ["InstanceId", "EmailAddressId"];
        "GET", "evaluation-forms/{InstanceId}/{EvaluationFormId}", "DescribeEvaluationForm", "H", false, ["InstanceId", "EvaluationFormId"];
        "GET", "hours-of-operations/{InstanceId}/{HoursOfOperationId}", "DescribeHoursOfOperation", "H", false, ["InstanceId", "HoursOfOperationId"];
        "GET", "hours-of-operations/{InstanceId}/{HoursOfOperationId}/overrides/{HoursOfOperationOverrideId}", "DescribeHoursOfOperationOverride", "H", false, ["InstanceId", "HoursOfOperationId", "HoursOfOperationOverrideId"];
        "GET", "instance/{InstanceId}", "DescribeInstance", "H", false, ["InstanceId"];
        "GET", "instance/{InstanceId}/attribute/{AttributeType}", "DescribeInstanceAttribute", "H", false, ["InstanceId", "AttributeType"];
        "GET", "instance/{InstanceId}/storage-config/{AssociationId}", "DescribeInstanceStorageConfig", "H501", false, ["InstanceId", "AssociationId"];
        "GET", "notifications/{InstanceId}/{NotificationId}", "DescribeNotification", "H", false, ["InstanceId", "NotificationId"];
        "GET", "phone-number/{PhoneNumberId}", "DescribePhoneNumber", "H", false, ["PhoneNumberId"];
        "GET", "predefined-attributes/{InstanceId}/{Name}", "DescribePredefinedAttribute", "H", false, ["InstanceId", "Name"];
        "GET", "prompts/{InstanceId}/{PromptId}", "DescribePrompt", "H", false, ["InstanceId", "PromptId"];
        "GET", "queues/{InstanceId}/{QueueId}", "DescribeQueue", "H", false, ["InstanceId", "QueueId"];
        "GET", "quick-connects/{InstanceId}/{QuickConnectId}", "DescribeQuickConnect", "H", false, ["InstanceId", "QuickConnectId"];
        "GET", "routing-profiles/{InstanceId}/{RoutingProfileId}", "DescribeRoutingProfile", "H", false, ["InstanceId", "RoutingProfileId"];
        "GET", "rules/{InstanceId}/{RuleId}", "DescribeRule", "H", false, ["InstanceId", "RuleId"];
        "GET", "security-profiles/{InstanceId}/{SecurityProfileId}", "DescribeSecurityProfile", "H", false, ["InstanceId", "SecurityProfileId"];
        "GET", "test-cases/{InstanceId}/{TestCaseId}", "DescribeTestCase", "H", false, ["InstanceId", "TestCaseId"];
        "GET", "traffic-distribution-group/{TrafficDistributionGroupId}", "DescribeTrafficDistributionGroup", "H", false, ["TrafficDistributionGroupId"];
        "GET", "users/{InstanceId}/{UserId}", "DescribeUser", "H", false, ["InstanceId", "UserId"];
        "GET", "user-hierarchy-groups/{InstanceId}/{HierarchyGroupId}", "DescribeUserHierarchyGroup", "H", false, ["InstanceId", "HierarchyGroupId"];
        "GET", "user-hierarchy-structure/{InstanceId}", "DescribeUserHierarchyStructure", "H", false, ["InstanceId"];
        "GET", "views/{InstanceId}/{ViewId}", "DescribeView", "H", false, ["InstanceId", "ViewId"];
        "GET", "vocabulary/{InstanceId}/{VocabularyId}", "DescribeVocabulary", "H", false, ["InstanceId", "VocabularyId"];
        "GET", "workspaces/{InstanceId}/{WorkspaceId}", "DescribeWorkspace", "H", false, ["InstanceId", "WorkspaceId"];
        "POST", "analytics-data/instance/{InstanceId}/association", "DisassociateAnalyticsDataSet", "H", false, ["InstanceId"];
        "DELETE", "instance/{InstanceId}/approved-origin", "DisassociateApprovedOrigin", "H", false, ["InstanceId"];
        "POST", "instance/{InstanceId}/bot", "DisassociateBot", "H", false, ["InstanceId"];
        "POST", "email-addresses/{InstanceId}/{EmailAddressId}/disassociate-alias", "DisassociateEmailAddressAlias", "R501", false, ["InstanceId", "EmailAddressId"];
        "DELETE", "flow-associations/{InstanceId}/{ResourceId}/{ResourceType}", "DisassociateFlow", "R501", false, ["InstanceId", "ResourceId", "ResourceType"];
        "POST", "hours-of-operations/{InstanceId}/{HoursOfOperationId}/disassociate-hours", "DisassociateHoursOfOperations", "R501", false, ["InstanceId", "HoursOfOperationId"];
        "DELETE", "instance/{InstanceId}/storage-config/{AssociationId}", "DisassociateInstanceStorageConfig", "H", false, ["InstanceId", "AssociationId"];
        "DELETE", "instance/{InstanceId}/lambda-function", "DisassociateLambdaFunction", "H", false, ["InstanceId"];
        "DELETE", "instance/{InstanceId}/lex-bot", "DisassociateLexBot", "R501", false, ["InstanceId"];
        "DELETE", "phone-number/{PhoneNumberId}/contact-flow", "DisassociatePhoneNumberContactFlow", "R501", false, ["PhoneNumberId"];
        "POST", "queues/{InstanceId}/{QueueId}/disassociate-email-addresses", "DisassociateQueueEmailAddresses", "R501", false, ["InstanceId", "QueueId"];
        "POST", "queues/{InstanceId}/{QueueId}/disassociate-quick-connects", "DisassociateQueueQuickConnects", "H", false, ["InstanceId", "QueueId"];
        "POST", "routing-profiles/{InstanceId}/{RoutingProfileId}/disassociate-queues", "DisassociateRoutingProfileQueues", "H", false, ["InstanceId", "RoutingProfileId"];
        "DELETE", "instance/{InstanceId}/security-key/{AssociationId}", "DisassociateSecurityKey", "H", false, ["InstanceId", "AssociationId"];
        "POST", "disassociate-security-profiles/{InstanceId}", "DisassociateSecurityProfiles", "R501", false, ["InstanceId"];
        "DELETE", "traffic-distribution-group/{TrafficDistributionGroupId}/user", "DisassociateTrafficDistributionGroupUser", "R501", false, ["TrafficDistributionGroupId"];
        "POST", "users/{InstanceId}/{UserId}/disassociate-proficiencies", "DisassociateUserProficiencies", "R501", false, ["InstanceId", "UserId"];
        "POST", "workspaces/{InstanceId}/{WorkspaceId}/disassociate", "DisassociateWorkspace", "R501", false, ["InstanceId", "WorkspaceId"];
        "POST", "users/{InstanceId}/{UserId}/contact", "DismissUserContact", "R501", false, ["InstanceId", "UserId"];
        "POST", "data-tables/{InstanceId}/{DataTableId}/values/evaluate", "EvaluateDataTableValues", "R501", false, ["InstanceId", "DataTableId"];
        "GET", "attached-files/{InstanceId}/{FileId}", "GetAttachedFile", "R501", false, ["InstanceId", "FileId"];
        "GET", "contact/attributes/{InstanceId}/{InitialContactId}", "GetContactAttributes", "H", false, ["InstanceId", "InitialContactId"];
        "POST", "metrics/contact", "GetContactMetrics", "R501", false, [];
        "POST", "metrics/current/{InstanceId}", "GetCurrentMetricData", "R501", false, ["InstanceId"];
        "POST", "metrics/userdata/{InstanceId}", "GetCurrentUserData", "R501", false, ["InstanceId"];
        "GET", "effective-hours-of-operations/{InstanceId}/{HoursOfOperationId}", "GetEffectiveHoursOfOperations", "R501", false, ["InstanceId", "HoursOfOperationId"];
        "GET", "user/federate/{InstanceId}", "GetFederationToken", "R501", false, ["InstanceId"];
        "GET", "flow-associations/{InstanceId}/{ResourceId}/{ResourceType}", "GetFlowAssociation", "R501", false, ["InstanceId", "ResourceId", "ResourceType"];
        "POST", "metrics/historical/{InstanceId}", "GetMetricData", "R501", false, ["InstanceId"];
        "POST", "metrics/data", "GetMetricDataV2", "R501", false, [];
        "GET", "prompts/{InstanceId}/{PromptId}/file", "GetPromptFile", "R501", false, ["InstanceId", "PromptId"];
        "GET", "instance/{InstanceId}/task/template/{TaskTemplateId}", "GetTaskTemplate", "H", false, ["InstanceId", "TaskTemplateId"];
        "GET", "test-cases/{InstanceId}/{TestCaseId}/{TestCaseExecutionId}/summary", "GetTestCaseExecutionSummary", "R501", false, ["InstanceId", "TestCaseId", "TestCaseExecutionId"];
        "GET", "traffic-distribution/{Id}", "GetTrafficDistribution", "R501", false, ["Id"];
        "POST", "phone-number/import", "ImportPhoneNumber", "H501", false, [];
        "POST", "workspaces/{InstanceId}/{WorkspaceId}/media", "ImportWorkspaceMedia", "R501", false, ["InstanceId", "WorkspaceId"];
        "GET", "agent-status/{InstanceId}", "ListAgentStatuses", "H", false, ["InstanceId"];
        "GET", "analytics-data/instance/{InstanceId}/association", "ListAnalyticsDataAssociations", "H", false, ["InstanceId"];
        "GET", "analytics-data/instance/{InstanceId}/datasets", "ListAnalyticsDataLakeDataSets", "R501", false, ["InstanceId"];
        "GET", "instance/{InstanceId}/approved-origins", "ListApprovedOrigins", "H", false, ["InstanceId"];
        "GET", "contact/associated/{InstanceId}", "ListAssociatedContacts", "R501", false, ["InstanceId"];
        "GET", "authentication-profiles-summary/{InstanceId}", "ListAuthenticationProfiles", "R501", false, ["InstanceId"];
        "GET", "instance/{InstanceId}/bots", "ListBots", "H", false, ["InstanceId"];
        "GET", "hours-of-operations/{InstanceId}/{HoursOfOperationId}/hours", "ListChildHoursOfOperations", "R501", false, ["InstanceId", "HoursOfOperationId"];
        "GET", "contact-evaluations/{InstanceId}", "ListContactEvaluations", "H", false, ["InstanceId"];
        "GET", "contact-flow-modules/{InstanceId}/{ContactFlowModuleId}/aliases", "ListContactFlowModuleAliases", "H", false, ["InstanceId", "ContactFlowModuleId"];
        "GET", "contact-flow-modules/{InstanceId}/{ContactFlowModuleId}/versions", "ListContactFlowModuleVersions", "R501", false, ["InstanceId", "ContactFlowModuleId"];
        "GET", "contact-flow-modules-summary/{InstanceId}", "ListContactFlowModules", "H", false, ["InstanceId"];
        "GET", "contact-flows/{InstanceId}/{ContactFlowId}/versions", "ListContactFlowVersions", "H", false, ["InstanceId", "ContactFlowId"];
        "GET", "contact-flows-summary/{InstanceId}", "ListContactFlows", "H", false, ["InstanceId"];
        "GET", "contact/references/{InstanceId}/{ContactId}", "ListContactReferences", "H", false, ["InstanceId", "ContactId"];
        "POST", "data-tables/{InstanceId}/{DataTableId}/attributes", "ListDataTableAttributes", "H", false, ["InstanceId", "DataTableId"];
        "POST", "data-tables/{InstanceId}/{DataTableId}/values/list-primary", "ListDataTablePrimaryValues", "R501", false, ["InstanceId", "DataTableId"];
        "POST", "data-tables/{InstanceId}/{DataTableId}/values/list", "ListDataTableValues", "R501", false, ["InstanceId", "DataTableId"];
        "GET", "data-tables/{InstanceId}", "ListDataTables", "H", false, ["InstanceId"];
        "POST", "default-vocabulary-summary/{InstanceId}", "ListDefaultVocabularies", "H", false, ["InstanceId"];
        "POST", "entity-security-profiles-summary/{InstanceId}", "ListEntitySecurityProfiles", "R501", false, ["InstanceId"];
        "GET", "evaluation-forms/{InstanceId}/{EvaluationFormId}/versions", "ListEvaluationFormVersions", "H", false, ["InstanceId", "EvaluationFormId"];
        "GET", "evaluation-forms/{InstanceId}", "ListEvaluationForms", "H", false, ["InstanceId"];
        "GET", "flow-associations-summary/{InstanceId}", "ListFlowAssociations", "H", false, ["InstanceId"];
        "GET", "hours-of-operations/{InstanceId}/{HoursOfOperationId}/overrides", "ListHoursOfOperationOverrides", "H", false, ["InstanceId", "HoursOfOperationId"];
        "GET", "hours-of-operations-summary/{InstanceId}", "ListHoursOfOperations", "H", false, ["InstanceId"];
        "GET", "instance/{InstanceId}/attributes", "ListInstanceAttributes", "H", false, ["InstanceId"];
        "GET", "instance/{InstanceId}/storage-configs", "ListInstanceStorageConfigs", "H", false, ["InstanceId"];
        "GET", "instance", "ListInstances", "H", false, [];
        "GET", "instance/{InstanceId}/integration-associations", "ListIntegrationAssociations", "H", false, ["InstanceId"];
        "GET", "instance/{InstanceId}/lambda-functions", "ListLambdaFunctions", "H", false, ["InstanceId"];
        "GET", "instance/{InstanceId}/lex-bots", "ListLexBots", "R501", false, ["InstanceId"];
        "GET", "notifications/{InstanceId}", "ListNotifications", "H", false, ["InstanceId"];
        "GET", "phone-numbers-summary/{InstanceId}", "ListPhoneNumbers", "H", false, ["InstanceId"];
        "POST", "phone-number/list", "ListPhoneNumbersV2", "H", false, [];
        "GET", "predefined-attributes/{InstanceId}", "ListPredefinedAttributes", "H", false, ["InstanceId"];
        "GET", "prompts-summary/{InstanceId}", "ListPrompts", "H", false, ["InstanceId"];
        "GET", "queues/{InstanceId}/{QueueId}/email-addresses", "ListQueueEmailAddresses", "R501", false, ["InstanceId", "QueueId"];
        "GET", "queues/{InstanceId}/{QueueId}/quick-connects", "ListQueueQuickConnects", "H", false, ["InstanceId", "QueueId"];
        "GET", "queues-summary/{InstanceId}", "ListQueues", "H", false, ["InstanceId"];
        "GET", "quick-connects/{InstanceId}", "ListQuickConnects", "H", false, ["InstanceId"];
        "POST", "contact/list-real-time-analysis-segments-v2/{InstanceId}/{ContactId}", "ListRealtimeContactAnalysisSegmentsV2", "R501", false, ["InstanceId", "ContactId"];
        "GET", "routing-profiles/{InstanceId}/{RoutingProfileId}/manual-assignment-queues", "ListRoutingProfileManualAssignmentQueues", "R501", false, ["InstanceId", "RoutingProfileId"];
        "GET", "routing-profiles/{InstanceId}/{RoutingProfileId}/queues", "ListRoutingProfileQueues", "H", false, ["InstanceId", "RoutingProfileId"];
        "GET", "routing-profiles-summary/{InstanceId}", "ListRoutingProfiles", "H", false, ["InstanceId"];
        "GET", "rules/{InstanceId}", "ListRules", "H", false, ["InstanceId"];
        "GET", "instance/{InstanceId}/security-keys", "ListSecurityKeys", "H", false, ["InstanceId"];
        "GET", "security-profiles-applications/{InstanceId}/{SecurityProfileId}", "ListSecurityProfileApplications", "H", false, ["InstanceId", "SecurityProfileId"];
        "GET", "security-profiles-flow-modules/{InstanceId}/{SecurityProfileId}", "ListSecurityProfileFlowModules", "R501", false, ["InstanceId", "SecurityProfileId"];
        "GET", "security-profiles-permissions/{InstanceId}/{SecurityProfileId}", "ListSecurityProfilePermissions", "H", false, ["InstanceId", "SecurityProfileId"];
        "GET", "security-profiles-summary/{InstanceId}", "ListSecurityProfiles", "H", false, ["InstanceId"];
        "GET", "tags/{resourceArn}", "ListTagsForResource", "H", false, ["resourceArn"];
        "GET", "instance/{InstanceId}/task/template", "ListTaskTemplates", "H", false, ["InstanceId"];
        "GET", "test-cases/{InstanceId}/{TestCaseId}/{TestCaseExecutionId}/records", "ListTestCaseExecutionRecords", "R501", false, ["InstanceId", "TestCaseId", "TestCaseExecutionId"];
        "GET", "test-case-executions/{InstanceId}", "ListTestCaseExecutions", "R501", false, ["InstanceId"];
        "GET", "test-cases-summary/{InstanceId}", "ListTestCases", "H", false, ["InstanceId"];
        "GET", "traffic-distribution-group/{TrafficDistributionGroupId}/user", "ListTrafficDistributionGroupUsers", "R501", false, ["TrafficDistributionGroupId"];
        "GET", "traffic-distribution-groups", "ListTrafficDistributionGroups", "H", false, [];
        "GET", "instance/{InstanceId}/integration-associations/{IntegrationAssociationId}/use-cases", "ListUseCases", "H", false, ["InstanceId", "IntegrationAssociationId"];
        "GET", "user-hierarchy-groups-summary/{InstanceId}", "ListUserHierarchyGroups", "H", false, ["InstanceId"];
        "GET", "users/{InstanceId}/{UserId}/notifications", "ListUserNotifications", "R501", false, ["InstanceId", "UserId"];
        "GET", "users/{InstanceId}/{UserId}/proficiencies", "ListUserProficiencies", "R501", false, ["InstanceId", "UserId"];
        "GET", "users-summary/{InstanceId}", "ListUsers", "H", false, ["InstanceId"];
        "GET", "views/{InstanceId}/{ViewId}/versions", "ListViewVersions", "R501", false, ["InstanceId", "ViewId"];
        "GET", "views/{InstanceId}", "ListViews", "H", false, ["InstanceId"];
        "GET", "workspaces/{InstanceId}/{WorkspaceId}/media", "ListWorkspaceMedia", "R501", false, ["InstanceId", "WorkspaceId"];
        "GET", "workspaces/{InstanceId}/{WorkspaceId}/pages", "ListWorkspacePages", "R501", false, ["InstanceId", "WorkspaceId"];
        "GET", "workspaces/{InstanceId}", "ListWorkspaces", "H", false, ["InstanceId"];
        "POST", "contact/monitor", "MonitorContact", "R501", false, [];
        "POST", "contact/pause", "PauseContact", "R501", false, [];
        "PUT", "users/{InstanceId}/{UserId}/status", "PutUserStatus", "R501", false, ["InstanceId", "UserId"];
        "DELETE", "phone-number/{PhoneNumberId}", "ReleasePhoneNumber", "H", false, ["PhoneNumberId"];
        "POST", "instance/{InstanceId}/replicate", "ReplicateInstance", "R501", false, ["InstanceId"];
        "POST", "contact/resume", "ResumeContact", "R501", false, [];
        "POST", "contact/resume-recording", "ResumeContactRecording", "R501", false, [];
        "POST", "search-agent-statuses", "SearchAgentStatuses", "H", false, [];
        "POST", "phone-number/search-available", "SearchAvailablePhoneNumbers", "H", false, [];
        "POST", "search-contact-evaluations", "SearchContactEvaluations", "R501", false, [];
        "POST", "search-contact-flow-modules", "SearchContactFlowModules", "H", false, [];
        "POST", "search-contact-flows", "SearchContactFlows", "H", false, [];
        "POST", "search-contacts", "SearchContacts", "R501", false, [];
        "POST", "search-data-tables", "SearchDataTables", "R501", false, [];
        "POST", "search-email-addresses", "SearchEmailAddresses", "R501", false, [];
        "POST", "search-evaluation-forms", "SearchEvaluationForms", "R501", false, [];
        "POST", "search-hours-of-operation-overrides", "SearchHoursOfOperationOverrides", "R501", false, [];
        "POST", "search-hours-of-operations", "SearchHoursOfOperations", "H", false, [];
        "POST", "search-notifications", "SearchNotifications", "R501", false, [];
        "POST", "search-predefined-attributes", "SearchPredefinedAttributes", "H", false, [];
        "POST", "search-prompts", "SearchPrompts", "H", false, [];
        "POST", "search-queues", "SearchQueues", "H", false, [];
        "POST", "search-quick-connects", "SearchQuickConnects", "H", false, [];
        "POST", "search-resource-tags", "SearchResourceTags", "R501", false, [];
        "POST", "search-routing-profiles", "SearchRoutingProfiles", "H", false, [];
        "POST", "search-security-profiles", "SearchSecurityProfiles", "H", false, [];
        "POST", "search-test-cases", "SearchTestCases", "R501", false, [];
        "POST", "search-user-hierarchy-groups", "SearchUserHierarchyGroups", "H", false, [];
        "POST", "search-users", "SearchUsers", "H", false, [];
        "POST", "search-views", "SearchViews", "R501", false, [];
        "POST", "vocabulary-summary/{InstanceId}", "SearchVocabularies", "H", false, ["InstanceId"];
        "POST", "search-workspace-associations", "SearchWorkspaceAssociations", "R501", false, [];
        "POST", "search-workspaces", "SearchWorkspaces", "R501", false, [];
        "POST", "chat-integration-event", "SendChatIntegrationEvent", "R501", false, [];
        "PUT", "instance/{InstanceId}/outbound-email", "SendOutboundEmail", "R501", false, ["InstanceId"];
        "PUT", "attached-files/{InstanceId}", "StartAttachedFileUpload", "R501", false, ["InstanceId"];
        "PUT", "contact/chat", "StartChatContact", "R501", false, [];
        "PUT", "contact-evaluations/{InstanceId}", "StartContactEvaluation", "H", false, ["InstanceId"];
        "POST", "contact/start-contact-media-processing", "StartContactMediaProcessing", "R501", false, [];
        "POST", "contact/start-recording", "StartContactRecording", "R501", false, [];
        "POST", "contact/start-streaming", "StartContactStreaming", "R501", false, [];
        "PUT", "contact/email", "StartEmailContact", "R501", false, [];
        "PUT", "contact/outbound-chat", "StartOutboundChatContact", "R501", false, [];
        "PUT", "contact/outbound-email", "StartOutboundEmailContact", "R501", false, [];
        "PUT", "contact/outbound-voice", "StartOutboundVoiceContact", "R501", false, [];
        "PUT", "contact/screen-sharing", "StartScreenSharing", "R501", false, [];
        "PUT", "contact/task", "StartTaskContact", "R501", false, [];
        "PUT", "test-cases/{InstanceId}/{TestCaseId}/start-execution", "StartTestCaseExecution", "R501", false, ["InstanceId", "TestCaseId"];
        "PUT", "contact/webrtc", "StartWebRTCContact", "R501", false, [];
        "POST", "contact/stop", "StopContact", "H", false, [];
        "POST", "contact/stop-contact-media-processing", "StopContactMediaProcessing", "R501", false, [];
        "POST", "contact/stop-recording", "StopContactRecording", "R501", false, [];
        "POST", "contact/stop-streaming", "StopContactStreaming", "R501", false, [];
        "POST", "test-cases/{InstanceId}/{TestCaseId}/{TestCaseExecutionId}/stop-execution", "StopTestCaseExecution", "R501", false, ["InstanceId", "TestCaseId", "TestCaseExecutionId"];
        "POST", "contact-evaluations/{InstanceId}/{EvaluationId}/submit", "SubmitContactEvaluation", "R501", false, ["InstanceId", "EvaluationId"];
        "POST", "contact/suspend-recording", "SuspendContactRecording", "R501", false, [];
        "POST", "contact/tags", "TagContact", "R501", false, [];
        "POST", "tags/{resourceArn}", "TagResource", "H", false, ["resourceArn"];
        "POST", "contact/transfer", "TransferContact", "R501", false, [];
        "DELETE", "contact/tags/{InstanceId}/{ContactId}", "UntagContact", "R501", false, ["InstanceId", "ContactId"];
        "DELETE", "tags/{resourceArn}", "UntagResource", "H", false, ["resourceArn"];
        "POST", "agent-status/{InstanceId}/{AgentStatusId}", "UpdateAgentStatus", "H", false, ["InstanceId", "AgentStatusId"];
        "POST", "authentication-profiles/{InstanceId}/{AuthenticationProfileId}", "UpdateAuthenticationProfile", "R501", false, ["InstanceId", "AuthenticationProfileId"];
        "POST", "contacts/{InstanceId}/{ContactId}", "UpdateContact", "H", false, ["InstanceId", "ContactId"];
        "POST", "contact/attributes", "UpdateContactAttributes", "H", false, [];
        "POST", "contact-evaluations/{InstanceId}/{EvaluationId}", "UpdateContactEvaluation", "H", false, ["InstanceId", "EvaluationId"];
        "POST", "contact-flows/{InstanceId}/{ContactFlowId}/content", "UpdateContactFlowContent", "H", false, ["InstanceId", "ContactFlowId"];
        "POST", "contact-flows/{InstanceId}/{ContactFlowId}/metadata", "UpdateContactFlowMetadata", "H", false, ["InstanceId", "ContactFlowId"];
        "POST", "contact-flow-modules/{InstanceId}/{ContactFlowModuleId}/alias/{AliasId}", "UpdateContactFlowModuleAlias", "H", false, ["InstanceId", "ContactFlowModuleId", "AliasId"];
        "POST", "contact-flow-modules/{InstanceId}/{ContactFlowModuleId}/content", "UpdateContactFlowModuleContent", "H", false, ["InstanceId", "ContactFlowModuleId"];
        "POST", "contact-flow-modules/{InstanceId}/{ContactFlowModuleId}/metadata", "UpdateContactFlowModuleMetadata", "H", false, ["InstanceId", "ContactFlowModuleId"];
        "POST", "contact-flows/{InstanceId}/{ContactFlowId}/name", "UpdateContactFlowName", "H", false, ["InstanceId", "ContactFlowId"];
        "POST", "contacts/{InstanceId}/{ContactId}/routing-data", "UpdateContactRoutingData", "R501", false, ["InstanceId", "ContactId"];
        "POST", "contact/schedule", "UpdateContactSchedule", "R501", false, [];
        "POST", "data-tables/{InstanceId}/{DataTableId}/attributes/{AttributeName}", "UpdateDataTableAttribute", "H", false, ["InstanceId", "DataTableId", "AttributeName"];
        "POST", "data-tables/{InstanceId}/{DataTableId}", "UpdateDataTableMetadata", "H501", false, ["InstanceId", "DataTableId"];
        "POST", "data-tables/{InstanceId}/{DataTableId}/values/update-primary", "UpdateDataTablePrimaryValues", "R501", false, ["InstanceId", "DataTableId"];
        "POST", "email-addresses/{InstanceId}/{EmailAddressId}", "UpdateEmailAddressMetadata", "H501", false, ["InstanceId", "EmailAddressId"];
        "PUT", "evaluation-forms/{InstanceId}/{EvaluationFormId}", "UpdateEvaluationForm", "H", false, ["InstanceId", "EvaluationFormId"];
        "POST", "hours-of-operations/{InstanceId}/{HoursOfOperationId}", "UpdateHoursOfOperation", "H", false, ["InstanceId", "HoursOfOperationId"];
        "POST", "hours-of-operations/{InstanceId}/{HoursOfOperationId}/overrides/{HoursOfOperationOverrideId}", "UpdateHoursOfOperationOverride", "H", false, ["InstanceId", "HoursOfOperationId", "HoursOfOperationOverrideId"];
        "POST", "instance/{InstanceId}/attribute/{AttributeType}", "UpdateInstanceAttribute", "H", false, ["InstanceId", "AttributeType"];
        "POST", "instance/{InstanceId}/storage-config/{AssociationId}", "UpdateInstanceStorageConfig", "H501", false, ["InstanceId", "AssociationId"];
        "POST", "notifications/{InstanceId}/{NotificationId}", "UpdateNotificationContent", "H501", false, ["InstanceId", "NotificationId"];
        "POST", "contact/update-participant-authentication", "UpdateParticipantAuthentication", "R501", false, [];
        "PUT", "contact/participant-role-config/{InstanceId}/{ContactId}", "UpdateParticipantRoleConfig", "R501", false, ["InstanceId", "ContactId"];
        "PUT", "phone-number/{PhoneNumberId}", "UpdatePhoneNumber", "H", false, ["PhoneNumberId"];
        "PUT", "phone-number/{PhoneNumberId}/metadata", "UpdatePhoneNumberMetadata", "R501", false, ["PhoneNumberId"];
        "POST", "predefined-attributes/{InstanceId}/{Name}", "UpdatePredefinedAttribute", "H", false, ["InstanceId", "Name"];
        "POST", "prompts/{InstanceId}/{PromptId}", "UpdatePrompt", "H", false, ["InstanceId", "PromptId"];
        "POST", "queues/{InstanceId}/{QueueId}/hours-of-operation", "UpdateQueueHoursOfOperation", "H", false, ["InstanceId", "QueueId"];
        "POST", "queues/{InstanceId}/{QueueId}/max-contacts", "UpdateQueueMaxContacts", "H", false, ["InstanceId", "QueueId"];
        "POST", "queues/{InstanceId}/{QueueId}/name", "UpdateQueueName", "H", false, ["InstanceId", "QueueId"];
        "POST", "queues/{InstanceId}/{QueueId}/outbound-caller-config", "UpdateQueueOutboundCallerConfig", "H", false, ["InstanceId", "QueueId"];
        "POST", "queues/{InstanceId}/{QueueId}/outbound-email-config", "UpdateQueueOutboundEmailConfig", "R501", false, ["InstanceId", "QueueId"];
        "POST", "queues/{InstanceId}/{QueueId}/status", "UpdateQueueStatus", "H", false, ["InstanceId", "QueueId"];
        "POST", "quick-connects/{InstanceId}/{QuickConnectId}/config", "UpdateQuickConnectConfig", "H", false, ["InstanceId", "QuickConnectId"];
        "POST", "quick-connects/{InstanceId}/{QuickConnectId}/name", "UpdateQuickConnectName", "H", false, ["InstanceId", "QuickConnectId"];
        "POST", "routing-profiles/{InstanceId}/{RoutingProfileId}/agent-availability-timer", "UpdateRoutingProfileAgentAvailabilityTimer", "R501", false, ["InstanceId", "RoutingProfileId"];
        "POST", "routing-profiles/{InstanceId}/{RoutingProfileId}/concurrency", "UpdateRoutingProfileConcurrency", "H", false, ["InstanceId", "RoutingProfileId"];
        "POST", "routing-profiles/{InstanceId}/{RoutingProfileId}/default-outbound-queue", "UpdateRoutingProfileDefaultOutboundQueue", "H", false, ["InstanceId", "RoutingProfileId"];
        "POST", "routing-profiles/{InstanceId}/{RoutingProfileId}/name", "UpdateRoutingProfileName", "H", false, ["InstanceId", "RoutingProfileId"];
        "POST", "routing-profiles/{InstanceId}/{RoutingProfileId}/queues", "UpdateRoutingProfileQueues", "H", false, ["InstanceId", "RoutingProfileId"];
        "PUT", "rules/{InstanceId}/{RuleId}", "UpdateRule", "H", false, ["InstanceId", "RuleId"];
        "POST", "security-profiles/{InstanceId}/{SecurityProfileId}", "UpdateSecurityProfile", "H", false, ["InstanceId", "SecurityProfileId"];
        "POST", "instance/{InstanceId}/task/template/{TaskTemplateId}", "UpdateTaskTemplate", "H", false, ["InstanceId", "TaskTemplateId"];
        "POST", "test-cases/{InstanceId}/{TestCaseId}", "UpdateTestCase", "H", false, ["InstanceId", "TestCaseId"];
        "PUT", "traffic-distribution/{Id}", "UpdateTrafficDistribution", "R501", false, ["Id"];
        "POST", "users/{InstanceId}/{UserId}/config", "UpdateUserConfig", "R501", false, ["InstanceId", "UserId"];
        "POST", "users/{InstanceId}/{UserId}/hierarchy", "UpdateUserHierarchy", "H", false, ["InstanceId", "UserId"];
        "POST", "user-hierarchy-groups/{InstanceId}/{HierarchyGroupId}/name", "UpdateUserHierarchyGroupName", "H", false, ["InstanceId", "HierarchyGroupId"];
        "POST", "user-hierarchy-structure/{InstanceId}", "UpdateUserHierarchyStructure", "H", false, ["InstanceId"];
        "POST", "users/{InstanceId}/{UserId}/identity-info", "UpdateUserIdentityInfo", "H", false, ["InstanceId", "UserId"];
        "POST", "users/{InstanceId}/{UserId}/notifications/{NotificationId}", "UpdateUserNotificationStatus", "R501", false, ["InstanceId", "UserId", "NotificationId"];
        "POST", "users/{InstanceId}/{UserId}/phone-config", "UpdateUserPhoneConfig", "H", false, ["InstanceId", "UserId"];
        "POST", "users/{InstanceId}/{UserId}/proficiencies", "UpdateUserProficiencies", "R501", false, ["InstanceId", "UserId"];
        "POST", "users/{InstanceId}/{UserId}/routing-profile", "UpdateUserRoutingProfile", "H", false, ["InstanceId", "UserId"];
        "POST", "users/{InstanceId}/{UserId}/security-profiles", "UpdateUserSecurityProfiles", "H", false, ["InstanceId", "UserId"];
        "POST", "views/{InstanceId}/{ViewId}", "UpdateViewContent", "H", false, ["InstanceId", "ViewId"];
        "POST", "views/{InstanceId}/{ViewId}/metadata", "UpdateViewMetadata", "H", false, ["InstanceId", "ViewId"];
        "POST", "workspaces/{InstanceId}/{WorkspaceId}/metadata", "UpdateWorkspaceMetadata", "R501", false, ["InstanceId", "WorkspaceId"];
        "POST", "workspaces/{InstanceId}/{WorkspaceId}/pages/{Page}", "UpdateWorkspacePage", "R501", false, ["InstanceId", "WorkspaceId", "Page"];
        "POST", "workspaces/{InstanceId}/{WorkspaceId}/theme", "UpdateWorkspaceTheme", "R501", false, ["InstanceId", "WorkspaceId"];
        "POST", "workspaces/{InstanceId}/{WorkspaceId}/visibility", "UpdateWorkspaceVisibility", "R501", false, ["InstanceId", "WorkspaceId"];]
});

fn derive_op(method: &str, path: &str) -> Option<&'static OpRow> {
    let segs: Vec<&str> = path
        .trim_matches('/')
        .split('/')
        .filter(|s| !s.is_empty())
        .collect();
    OPS.iter()
        .find(|row| row.method == method && segments_match(row, &segs))
}

fn is_param(seg: &str) -> bool {
    seg.starts_with('{') && seg.ends_with('}')
}

fn segments_match(row: &OpRow, req: &[&str]) -> bool {
    let spec_segs: Vec<&str> = row.path.split('/').filter(|s| !s.is_empty()).collect();
    if row.greedy {
        return req.len() >= spec_segs.len()
            && spec_segs[..spec_segs.len() - 1]
                .iter()
                .zip(req.iter())
                .all(|(s, r)| is_param(s) || s == r);
    }
    spec_segs.len() == req.len()
        && spec_segs
            .iter()
            .zip(req.iter())
            .all(|(s, r)| is_param(s) || s == r)
}

/// Extract URL-decoded URI parameter values for a matched row, keyed by name.
fn path_params(req_path: &str, row: &OpRow) -> HashMap<&'static str, String> {
    let mut out: HashMap<&'static str, String> = HashMap::new();
    let spec_segs: Vec<&str> = row.path.split('/').filter(|s| !s.is_empty()).collect();
    let trimmed = req_path.trim_matches('/');
    let req_segs: Vec<&str> = trimmed.split('/').collect();
    let mut consumed = 0usize;
    for (spec, val) in spec_segs.iter().zip(req_segs.iter()) {
        if !is_param(spec) {
            consumed += 1;
            continue;
        }
        let name = spec.trim_start_matches('{').trim_end_matches('}');
        let value = if row.greedy && *spec == spec_segs[spec_segs.len() - 1] {
            trimmed
                .splitn(consumed + 1, '/')
                .nth(consumed)
                .unwrap_or("")
        } else {
            *val
        };
        consumed += 1;
        if let Some(slot) = row.params.iter().find(|p| **p == name) {
            out.insert(*slot, path_decode(value));
        }
    }
    out
}

/// Path percent-decoding (moto `unquote`): `+` stays a plus.
fn path_decode(s: &str) -> String {
    if !s.contains('%') {
        return s.to_string();
    }
    let b = s.as_bytes();
    let mut out: Vec<u8> = Vec::with_capacity(b.len());
    let mut i = 0;
    while i < b.len() {
        if b[i] == b'%' && i + 2 < b.len() {
            if let (Some(h), Some(l)) = (
                (b[i + 1] as char).to_digit(16),
                (b[i + 2] as char).to_digit(16),
            ) {
                out.push((h * 16 + l) as u8);
                i += 3;
                continue;
            }
        }
        out.push(b[i]);
        i += 1;
    }
    String::from_utf8_lossy(&out).into_owned()
}

// ---------------------------------------------------------------------------
// Request parsing helpers (moto `responses.py` semantics)
// ---------------------------------------------------------------------------

fn body_str(body: &Map<String, Value>, key: &str) -> Result<String, Response> {
    match body.get(key) {
        Some(Value::String(s)) => Ok(s.clone()),
        Some(Value::Null) => Ok("None".to_string()),
        Some(Value::Bool(b)) => Ok(if *b { "True".into() } else { "False".into() }),
        Some(other) => Ok(fmt_py(other)),
        None => Err(missing_body_param(key)),
    }
}

/// `params.get(key, default)` — Java-string-coercion like moto responses.
fn opt_string(body: &Map<String, Value>, key: &str) -> Option<String> {
    body.get(key).map(|v| match v {
        Value::String(s) => s.clone(),
        Value::Null => String::new(),
        Value::Number(n) => n.to_string(),
        Value::Bool(b) => (if *b { "True" } else { "False" }).to_string(),
        other => fmt_py(other),
    })
}

fn body_bool(body: &Map<String, Value>, key: &str, default: bool) -> bool {
    body.get(key).and_then(Value::as_bool).unwrap_or(default)
}

/// `__attrs` — internal per-instance attribute storage ({key: str}).
/// moto keeps instance attributes off the instance dict; the wire output
/// strips them (`strip_attrs`).
fn seed_attrs(inbound: bool, outbound: bool) -> Map<String, Value> {
    let mut attrs = Map::new();
    for (k, v) in [
        ("INBOUND_CALLS", inbound),
        ("OUTBOUND_CALLS", outbound),
        ("CONTACTFLOW_LOGS", false),
        ("CONTACT_LENS", true),
        ("AUTO_RESOLVE_BEST_VOICES", true),
        ("USE_CUSTOM_TTS_VOICES", false),
        ("EARLY_MEDIA", true),
        ("MULTI_PARTY_CONFERENCE", false),
        ("MULTI_PARTY_CHAT_CONFERENCE", false),
        ("HIGH_VOLUME_OUTBOUND", false),
        ("ENHANCED_CONTACT_MONITORING", false),
        ("ENHANCED_CHAT_MONITORING", false),
    ] {
        attrs.insert(k.to_string(), json!(if v { "true" } else { "false" }));
    }
    attrs
}

fn opt_val(body: &Map<String, Value>, key: &str) -> Option<Value> {
    body.get(key).cloned()
}

/// First value (decoded) for `key` in the query string.
fn q1<'a>(query: &'a BTreeMap<String, Vec<String>>, key: &str) -> Option<&'a str> {
    query.get(key).and_then(|v| v.first()).map(String::as_str)
}

fn query_values(query: &BTreeMap<String, Vec<String>>, key: &str) -> Vec<String> {
    query.get(key).cloned().unwrap_or_default()
}

/// moto paginator token: `base64(json.dumps({"uniqueAttributes": "<id>"}))`.
fn encode_page_token(unique_attributes: &str) -> String {
    use base64::Engine;
    base64::engine::general_purpose::STANDARD.encode(
        format!(
            "{{\"uniqueAttributes\": {}}}",
            fmt_py_str(unique_attributes)
        )
        .as_bytes(),
    )
}

fn decode_page_token(token: &str) -> Option<String> {
    use base64::Engine;
    let raw = base64::engine::general_purpose::STANDARD
        .decode(token.trim())
        .ok()?;
    let value: Value = serde_json::from_slice(&raw).ok()?;
    value
        .get("uniqueAttributes")
        .and_then(Value::as_str)
        .map(str::to_string)
}

// ---------------------------------------------------------------------------
// State
// ---------------------------------------------------------------------------

#[derive(Default)]
pub struct ConnectState {
    instances: HashMap<String, Value>,
    #[allow(dead_code)] // phone-number family wires this up in its own commit
    phone_numbers: HashMap<String, Value>,
    #[allow(dead_code)] // traffic-distribution family wires this up in its own commit
    traffic_distribution_groups: HashMap<String, Value>,
    /// moto TaggingService: resource ARN -> tag key/value.
    tags: HashMap<String, Map<String, Value>>,
    /// Per-instance stores: instance_id -> resource_id -> resource JSON.
    analytics_data_associations: HashMap<String, HashMap<String, Value>>,
    agent_statuses: HashMap<String, HashMap<String, Value>>,
    approved_origins: HashMap<String, Vec<String>>,
    bots: HashMap<String, Vec<Value>>,
    contact_attributes: HashMap<String, HashMap<String, Map<String, Value>>>,
    contact_evaluations: HashMap<String, HashMap<String, Value>>,
    contact_flow_module_aliases: HashMap<String, HashMap<String, HashMap<String, Value>>>,
    contact_flow_modules: HashMap<String, HashMap<String, Value>>,
    contact_flows: HashMap<String, HashMap<String, Value>>,
    contacts: HashMap<String, HashMap<String, Value>>,
    data_table_attributes: HashMap<String, HashMap<String, HashMap<String, Value>>>,
    data_tables: HashMap<String, HashMap<String, Value>>,
    email_addresses: HashMap<String, HashMap<String, Value>>,
    evaluation_forms: HashMap<String, HashMap<String, Value>>,
    hours_of_operation_overrides: HashMap<String, HashMap<String, Value>>,
    hours_of_operations: HashMap<String, HashMap<String, Value>>,
    integration_associations: HashMap<String, HashMap<String, Value>>,
    instance_storage_configs: HashMap<String, Vec<Value>>,
    lambda_functions: HashMap<String, Vec<String>>,
    notifications: HashMap<String, HashMap<String, Value>>,
    predefined_attributes: HashMap<String, HashMap<String, Value>>,
    prompts: HashMap<String, HashMap<String, Value>>,
    queue_quick_connect_associations: HashMap<String, HashMap<String, Vec<String>>>,
    queues: HashMap<String, HashMap<String, Value>>,
    quick_connects: HashMap<String, HashMap<String, Value>>,
    routing_profile_queue_associations: HashMap<String, HashMap<String, Vec<Value>>>,
    routing_profiles: HashMap<String, HashMap<String, Value>>,
    rules: HashMap<String, HashMap<String, Value>>,
    security_keys: HashMap<String, Vec<Value>>,
    security_profiles: HashMap<String, HashMap<String, Value>>,
    task_templates: HashMap<String, HashMap<String, Value>>,
    test_cases: HashMap<String, HashMap<String, Value>>,
    use_cases: HashMap<String, HashMap<String, Value>>,
    user_hierarchy_groups: HashMap<String, HashMap<String, Value>>,
    users: HashMap<String, HashMap<String, Value>>,
    vocabularies: HashMap<String, HashMap<String, Value>>,
    views: HashMap<String, HashMap<String, Value>>,
    workspaces: HashMap<String, HashMap<String, Value>>,
}

fn op_state() -> Slot<ConnectState> {
    static SLOT: Lazy<Slot<ConnectState>> = Lazy::new(|| store::slot::<ConnectState>("connect"));
    SLOT.clone()
}

fn write_state<T>(req: &Request, f: impl FnOnce(&mut ConnectState) -> T) -> T {
    let slot = op_state();
    let mut map = slot.write();
    f(map.entry((req.account, req.region.clone())).or_default())
}

/// Tag a resource ARN (moto `TaggingService.convert_dict_to_tags_input`).
fn tag_resource(st: &mut ConnectState, arn: &str, tags: Option<Value>) {
    if let Some(Value::Object(map)) = tags {
        if !map.is_empty() {
            st.tags.insert(arn.to_string(), map);
        }
    }
}

// ---------------------------------------------------------------------------
// dispatch
// ---------------------------------------------------------------------------

pub fn dispatch(req: &Request) -> Response {
    let row = match derive_op(&req.method, &req.path) {
        Some(row) => row,
        None => return service_not_implemented(),
    };
    if row.kind != "H" {
        return if row.kind == "H501" {
            op_not_implemented(row.op)
        } else {
            service_not_implemented()
        };
    }
    let params = path_params(&req.path, row);
    let body = parse_body(&req.body);
    let body = body.as_object().cloned().unwrap_or_default();
    let query = crate::proto::query_map(&req.query_string);
    write_state(req, |st| handle(req, st, row.op, &params, &body, &query))
}

fn parse_body(body: &[u8]) -> Value {
    if body.is_empty() {
        json!({})
    } else {
        serde_json::from_slice(body).unwrap_or_else(|_| json!({}))
    }
}

fn param<'a>(params: &'a HashMap<&'static str, String>, name: &str) -> &'a str {
    params.get(name).map(String::as_str).unwrap_or("")
}

// ---------------------------------------------------------------------------
// Handlers — one arm per moto-implemented op; anything the moto handlers do
// not cover falls through to op_not_implemented (deny-by-default).
// ---------------------------------------------------------------------------

fn handle(
    req: &Request,
    st: &mut ConnectState,
    op: &str,
    params: &HashMap<&'static str, String>,
    body: &Map<String, Value>,
    query: &BTreeMap<String, Vec<String>>,
) -> Response {
    match op {
        // -- Instances ---------------------------------------------------
        "CreateInstance" => op_create_instance(st, body, &req.region, req.account),
        "DescribeInstance" => op_describe_instance(st, param(params, "InstanceId")),
        "ListInstances" => op_list_instances(st, query),
        "DeleteInstance" => op_delete_instance(st, param(params, "InstanceId")),
        "DescribeInstanceAttribute" => op_describe_instance_attribute(
            st,
            param(params, "InstanceId"),
            param(params, "AttributeType"),
        ),
        "UpdateInstanceAttribute" => op_update_instance_attribute(
            st,
            param(params, "InstanceId"),
            param(params, "AttributeType"),
            body,
        ),
        "ListInstanceAttributes" => op_list_instance_attributes(st, param(params, "InstanceId")),
        // -- Instance associations ---------------------------------------
        "AssociateApprovedOrigin" => {
            op_associate_approved_origin(st, param(params, "InstanceId"), body)
        }
        "ListApprovedOrigins" => op_list_approved_origins(st, param(params, "InstanceId")),
        "DisassociateApprovedOrigin" => {
            op_disassociate_approved_origin(st, param(params, "InstanceId"), q1(query, "origin"))
        }
        "AssociateLambdaFunction" => {
            op_associate_lambda_function(st, param(params, "InstanceId"), body)
        }
        "ListLambdaFunctions" => op_list_lambda_functions(st, param(params, "InstanceId")),
        "DisassociateLambdaFunction" => op_disassociate_lambda_function(
            st,
            param(params, "InstanceId"),
            q1(query, "functionArn"),
        ),
        "AssociateBot" => op_associate_bot(st, param(params, "InstanceId"), body),
        "DisassociateBot" => op_disassociate_bot(st, param(params, "InstanceId"), body),
        "ListBots" => op_list_bots(st, param(params, "InstanceId")),
        "AssociateSecurityKey" => op_associate_security_key(st, param(params, "InstanceId"), body),
        "ListSecurityKeys" => op_list_security_keys(st, param(params, "InstanceId")),
        "DisassociateSecurityKey" => op_disassociate_security_key(
            st,
            param(params, "InstanceId"),
            param(params, "AssociationId"),
        ),
        "AssociateInstanceStorageConfig" => {
            op_associate_instance_storage_config(st, param(params, "InstanceId"), body)
        }
        "ListInstanceStorageConfigs" => {
            op_list_instance_storage_configs(st, param(params, "InstanceId"))
        }
        "DisassociateInstanceStorageConfig" => op_disassociate_instance_storage_config(
            st,
            param(params, "InstanceId"),
            param(params, "AssociationId"),
        ),
        "AssociateAnalyticsDataSet" => op_associate_analytics_data_set(
            st,
            param(params, "InstanceId"),
            body,
            req.account,
            &req.region,
        ),
        "DisassociateAnalyticsDataSet" => {
            op_disassociate_analytics_data_set(st, param(params, "InstanceId"), body)
        }
        "ListAnalyticsDataAssociations" => {
            op_list_analytics_data_associations(st, param(params, "InstanceId"), query)
        }
        // -- Tags --------------------------------------------------------
        "TagResource" => op_tag_resource(st, param(params, "resourceArn"), body),
        "UntagResource" => op_untag_resource(st, param(params, "resourceArn"), query),
        "ListTagsForResource" => op_list_tags_for_resource(st, param(params, "resourceArn")),
        // -- Agent statuses ------------------------------------------------
        "CreateAgentStatus" => op_create_agent_status(st, param(params, "InstanceId"), body),
        "DescribeAgentStatus" => op_describe_agent_status(
            st,
            param(params, "InstanceId"),
            param(params, "AgentStatusId"),
        ),
        "UpdateAgentStatus" => op_update_agent_status(
            st,
            param(params, "InstanceId"),
            param(params, "AgentStatusId"),
            body,
        ),
        "ListAgentStatuses" => op_list_agent_statuses(st, param(params, "InstanceId")),
        // -- Security profiles ----------------------------------------------
        "CreateSecurityProfile" => {
            op_create_security_profile(st, param(params, "InstanceId"), body)
        }
        "DescribeSecurityProfile" => op_describe_security_profile(
            st,
            param(params, "InstanceId"),
            param(params, "SecurityProfileId"),
        ),
        "UpdateSecurityProfile" => op_update_security_profile(
            st,
            param(params, "InstanceId"),
            param(params, "SecurityProfileId"),
            body,
        ),
        "DeleteSecurityProfile" => op_delete_security_profile(
            st,
            param(params, "InstanceId"),
            param(params, "SecurityProfileId"),
        ),
        "ListSecurityProfiles" => op_list_security_profiles(st, param(params, "InstanceId")),
        "ListSecurityProfilePermissions" => op_list_security_profile_permissions(
            st,
            param(params, "InstanceId"),
            param(params, "SecurityProfileId"),
        ),
        "ListSecurityProfileApplications" => {
            op_list_security_profile_applications(st, param(params, "InstanceId"))
        }
        // -- Users -----------------------------------------------------------
        "CreateUser" => op_create_user(st, param(params, "InstanceId"), body),
        "DescribeUser" => {
            op_describe_user(st, param(params, "InstanceId"), param(params, "UserId"))
        }
        "UpdateUserIdentityInfo" => {
            let user_id = param(params, "UserId");
            let identity = match opt_val(body, "IdentityInfo") {
                Some(v) => v,
                None => return missing_body_param("IdentityInfo"),
            };
            op_update_user_field(st, param(params, "InstanceId"), user_id, |u| {
                u["IdentityInfo"] = identity
            })
        }
        "UpdateUserPhoneConfig" => {
            let user_id = param(params, "UserId");
            let config = match opt_val(body, "PhoneConfig") {
                Some(v) => v,
                None => return missing_body_param("PhoneConfig"),
            };
            op_update_user_field(st, param(params, "InstanceId"), user_id, |u| {
                u["PhoneConfig"] = config
            })
        }
        "UpdateUserRoutingProfile" => {
            let user_id = param(params, "UserId");
            let rp = match body_str(body, "RoutingProfileId") {
                Ok(v) => v,
                Err(resp) => return resp,
            };
            op_update_user_field(st, param(params, "InstanceId"), user_id, |u| {
                u["RoutingProfileId"] = json!(rp)
            })
        }
        "UpdateUserSecurityProfiles" => {
            let user_id = param(params, "UserId");
            let ids = match opt_val(body, "SecurityProfileIds") {
                Some(v) => v,
                None => return missing_body_param("SecurityProfileIds"),
            };
            op_update_user_field(st, param(params, "InstanceId"), user_id, |u| {
                u["SecurityProfileIds"] = ids
            })
        }
        "UpdateUserHierarchy" => {
            let user_id = param(params, "UserId");
            let hierarchy = opt_val(body, "HierarchyGroupId");
            op_update_user_field(
                st,
                param(params, "InstanceId"),
                user_id,
                |u| match hierarchy {
                    Some(v) if !v.as_str().unwrap_or_default().is_empty() => {
                        u["HierarchyGroupId"] = v;
                    }
                    _ => {
                        u.as_object_mut().map(|m| m.remove("HierarchyGroupId"));
                    }
                },
            )
        }
        "DeleteUser" => op_delete_user(st, param(params, "InstanceId"), param(params, "UserId")),
        "ListUsers" => op_list_users(st, param(params, "InstanceId")),
        // -- User hierarchy groups -------------------------------------------
        "CreateUserHierarchyGroup" => {
            op_create_user_hierarchy_group(st, param(params, "InstanceId"), body)
        }
        "DescribeUserHierarchyGroup" => op_describe_user_hierarchy_group(
            st,
            param(params, "InstanceId"),
            param(params, "HierarchyGroupId"),
        ),
        "DeleteUserHierarchyGroup" => op_delete_user_hierarchy_group(
            st,
            param(params, "InstanceId"),
            param(params, "HierarchyGroupId"),
        ),
        "UpdateUserHierarchyGroupName" => op_update_user_hierarchy_group_name(
            st,
            param(params, "InstanceId"),
            param(params, "HierarchyGroupId"),
            body,
        ),
        "ListUserHierarchyGroups" => op_list_user_hierarchy_groups(st, param(params, "InstanceId")),
        "DescribeUserHierarchyStructure" => op_describe_user_hierarchy_structure(
            st,
            param(params, "InstanceId"),
            &req.region,
            req.account,
        ),
        "UpdateUserHierarchyStructure" => {
            op_update_user_hierarchy_structure(st, param(params, "InstanceId"))
        }
        // -- Contact flows ------------------------------------------------
        "CreateContactFlow" => op_create_contact_flow(st, param(params, "InstanceId"), body),
        "DescribeContactFlow" => op_describe_contact_flow(
            st,
            param(params, "InstanceId"),
            param(params, "ContactFlowId"),
        ),
        "UpdateContactFlowContent" => op_update_contact_flow_content(
            st,
            param(params, "InstanceId"),
            param(params, "ContactFlowId"),
            body,
        ),
        "UpdateContactFlowName" => op_update_contact_flow_name(
            st,
            param(params, "InstanceId"),
            param(params, "ContactFlowId"),
            body,
        ),
        "UpdateContactFlowMetadata" => op_update_contact_flow_metadata(
            st,
            param(params, "InstanceId"),
            param(params, "ContactFlowId"),
            body,
        ),
        "DeleteContactFlow" => op_delete_contact_flow(
            st,
            param(params, "InstanceId"),
            param(params, "ContactFlowId"),
        ),
        "ListContactFlows" => op_list_contact_flows(st, param(params, "InstanceId")),
        "ListContactFlowVersions" => op_list_contact_flow_versions(
            st,
            param(params, "InstanceId"),
            param(params, "ContactFlowId"),
        ),
        // -- Contact flow modules ---------------------------------------------
        "CreateContactFlowModule" => {
            op_create_contact_flow_module(st, param(params, "InstanceId"), body)
        }
        "DescribeContactFlowModule" => op_describe_contact_flow_module(
            st,
            param(params, "InstanceId"),
            param(params, "ContactFlowModuleId"),
        ),
        "UpdateContactFlowModuleContent" => op_update_contact_flow_module_content(
            st,
            param(params, "InstanceId"),
            param(params, "ContactFlowModuleId"),
            body,
        ),
        "UpdateContactFlowModuleMetadata" => op_update_contact_flow_module_metadata(
            st,
            param(params, "InstanceId"),
            param(params, "ContactFlowModuleId"),
            body,
        ),
        "DeleteContactFlowModule" => op_delete_contact_flow_module(
            st,
            param(params, "InstanceId"),
            param(params, "ContactFlowModuleId"),
        ),
        "ListContactFlowModules" => op_list_contact_flow_modules(st, param(params, "InstanceId")),
        // -- Contact flow module aliases --------------------------------------
        "CreateContactFlowModuleAlias" => op_create_contact_flow_module_alias(
            st,
            param(params, "InstanceId"),
            param(params, "ContactFlowModuleId"),
            body,
        ),
        "DescribeContactFlowModuleAlias" => op_describe_contact_flow_module_alias(
            st,
            param(params, "InstanceId"),
            param(params, "ContactFlowModuleId"),
            param(params, "AliasId"),
        ),
        "ListContactFlowModuleAliases" => op_list_contact_flow_module_aliases(
            st,
            param(params, "InstanceId"),
            param(params, "ContactFlowModuleId"),
        ),
        "UpdateContactFlowModuleAlias" => op_update_contact_flow_module_alias(
            st,
            param(params, "InstanceId"),
            param(params, "ContactFlowModuleId"),
            param(params, "AliasId"),
            body,
        ),
        "DeleteContactFlowModuleAlias" => op_delete_contact_flow_module_alias(
            st,
            param(params, "InstanceId"),
            param(params, "ContactFlowModuleId"),
            param(params, "AliasId"),
        ),
        // -- Contacts ----------------------------------------------------------
        "CreateContact" => op_create_contact(st, param(params, "InstanceId"), body),
        "DescribeContact" => {
            op_describe_contact(st, param(params, "InstanceId"), param(params, "ContactId"))
        }
        "UpdateContact" => op_update_contact(
            st,
            param(params, "InstanceId"),
            param(params, "ContactId"),
            body,
        ),
        "StopContact" => {
            let instance_id = match body_str(body, "InstanceId") {
                Ok(v) => v,
                Err(resp) => return resp,
            };
            let contact_id = match body_str(body, "ContactId") {
                Ok(v) => v,
                Err(resp) => return resp,
            };
            op_stop_contact(st, &instance_id, &contact_id)
        }
        // -- Contact attributes -------------------------------------------------
        "GetContactAttributes" => op_get_contact_attributes(
            st,
            param(params, "InstanceId"),
            param(params, "InitialContactId"),
        ),
        "UpdateContactAttributes" => op_update_contact_attributes(st, body),
        // -- Contact evaluations -------------------------------------------------
        "StartContactEvaluation" => {
            op_start_contact_evaluation(st, param(params, "InstanceId"), body)
        }
        "DescribeContactEvaluation" => op_describe_contact_evaluation(
            st,
            param(params, "InstanceId"),
            param(params, "EvaluationId"),
        ),
        "UpdateContactEvaluation" => op_update_contact_evaluation(
            st,
            param(params, "InstanceId"),
            param(params, "EvaluationId"),
            body,
        ),
        "DeleteContactEvaluation" => op_delete_contact_evaluation(
            st,
            param(params, "InstanceId"),
            param(params, "EvaluationId"),
        ),
        "ListContactReferences" => op_list_contact_references(st, param(params, "InstanceId")),
        // -- Flow association list (moto returns []) -------------------------------
        "ListFlowAssociations" => op_list_flow_associations(st, param(params, "InstanceId")),
        // -- Hours of operations ----------------------------------------------
        "CreateHoursOfOperation" => {
            op_create_hours_of_operation(st, param(params, "InstanceId"), body)
        }
        "DescribeHoursOfOperation" => op_describe_hours_of_operation(
            st,
            param(params, "InstanceId"),
            param(params, "HoursOfOperationId"),
        ),
        "UpdateHoursOfOperation" => op_update_hours_of_operation(
            st,
            param(params, "InstanceId"),
            param(params, "HoursOfOperationId"),
            body,
        ),
        "DeleteHoursOfOperation" => op_delete_hours_of_operation(
            st,
            param(params, "InstanceId"),
            param(params, "HoursOfOperationId"),
        ),
        "ListHoursOfOperations" => op_list_hours_of_operations(st, param(params, "InstanceId")),
        // -- Hours of operation overrides --------------------------------------
        "CreateHoursOfOperationOverride" => op_create_hours_of_operation_override(
            st,
            param(params, "InstanceId"),
            param(params, "HoursOfOperationId"),
            body,
        ),
        "DescribeHoursOfOperationOverride" => op_describe_hours_of_operation_override(
            st,
            param(params, "InstanceId"),
            param(params, "HoursOfOperationId"),
            param(params, "HoursOfOperationOverrideId"),
        ),
        "UpdateHoursOfOperationOverride" => op_update_hours_of_operation_override(
            st,
            param(params, "InstanceId"),
            param(params, "HoursOfOperationId"),
            param(params, "HoursOfOperationOverrideId"),
            body,
        ),
        "DeleteHoursOfOperationOverride" => op_delete_hours_of_operation_override(
            st,
            param(params, "InstanceId"),
            param(params, "HoursOfOperationId"),
            param(params, "HoursOfOperationOverrideId"),
        ),
        "ListHoursOfOperationOverrides" => op_list_hours_of_operation_overrides(
            st,
            param(params, "InstanceId"),
            param(params, "HoursOfOperationId"),
        ),
        // -- Queues --------------------------------------------------------------
        "CreateQueue" => op_create_queue(st, param(params, "InstanceId"), body),
        "DescribeQueue" => {
            op_describe_queue(st, param(params, "InstanceId"), param(params, "QueueId"))
        }
        "UpdateQueueName" => op_update_queue_name(
            st,
            param(params, "InstanceId"),
            param(params, "QueueId"),
            body,
        ),
        "UpdateQueueStatus" => op_update_queue_status(
            st,
            param(params, "InstanceId"),
            param(params, "QueueId"),
            body,
        ),
        "UpdateQueueMaxContacts" => op_update_queue_max_contacts(
            st,
            param(params, "InstanceId"),
            param(params, "QueueId"),
            body,
        ),
        "UpdateQueueHoursOfOperation" => op_update_queue_hours_of_operation(
            st,
            param(params, "InstanceId"),
            param(params, "QueueId"),
            body,
        ),
        "UpdateQueueOutboundCallerConfig" => op_update_queue_outbound_caller_config(
            st,
            param(params, "InstanceId"),
            param(params, "QueueId"),
            body,
        ),
        "DeleteQueue" => op_delete_queue(st, param(params, "InstanceId"), param(params, "QueueId")),
        "ListQueues" => op_list_queues(st, param(params, "InstanceId")),
        "ListQueueQuickConnects" => {
            op_list_queue_quick_connects(st, param(params, "InstanceId"), param(params, "QueueId"))
        }
        "AssociateQueueQuickConnects" => op_associate_queue_quick_connects(
            st,
            param(params, "InstanceId"),
            param(params, "QueueId"),
            body,
        ),
        "DisassociateQueueQuickConnects" => op_disassociate_queue_quick_connects(
            st,
            param(params, "InstanceId"),
            param(params, "QueueId"),
            body,
        ),
        // -- Quick connects ------------------------------------------------------
        "CreateQuickConnect" => op_create_quick_connect(st, param(params, "InstanceId"), body),
        "DescribeQuickConnect" => op_describe_quick_connect(
            st,
            param(params, "InstanceId"),
            param(params, "QuickConnectId"),
        ),
        "UpdateQuickConnectName" => op_update_quick_connect_name(
            st,
            param(params, "InstanceId"),
            param(params, "QuickConnectId"),
            body,
        ),
        "UpdateQuickConnectConfig" => op_update_quick_connect_config(
            st,
            param(params, "InstanceId"),
            param(params, "QuickConnectId"),
            body,
        ),
        "DeleteQuickConnect" => op_delete_quick_connect(
            st,
            param(params, "InstanceId"),
            param(params, "QuickConnectId"),
        ),
        "ListQuickConnects" => op_list_quick_connects(st, param(params, "InstanceId")),
        // -- Routing profiles ------------------------------------------------------
        "CreateRoutingProfile" => op_create_routing_profile(st, param(params, "InstanceId"), body),
        "DescribeRoutingProfile" => op_describe_routing_profile(
            st,
            param(params, "InstanceId"),
            param(params, "RoutingProfileId"),
        ),
        "UpdateRoutingProfileName" => op_update_routing_profile_name(
            st,
            param(params, "InstanceId"),
            param(params, "RoutingProfileId"),
            body,
        ),
        "UpdateRoutingProfileConcurrency" => op_update_routing_profile_concurrency(
            st,
            param(params, "InstanceId"),
            param(params, "RoutingProfileId"),
            body,
        ),
        "UpdateRoutingProfileDefaultOutboundQueue" => {
            op_update_routing_profile_default_outbound_queue(
                st,
                param(params, "InstanceId"),
                param(params, "RoutingProfileId"),
                body,
            )
        }
        "DeleteRoutingProfile" => op_delete_routing_profile(
            st,
            param(params, "InstanceId"),
            param(params, "RoutingProfileId"),
        ),
        "ListRoutingProfiles" => op_list_routing_profiles(st, param(params, "InstanceId")),
        "ListRoutingProfileQueues" => op_list_routing_profile_queues(
            st,
            param(params, "InstanceId"),
            param(params, "RoutingProfileId"),
        ),
        "AssociateRoutingProfileQueues" => op_associate_routing_profile_queues(
            st,
            param(params, "InstanceId"),
            param(params, "RoutingProfileId"),
            body,
        ),
        "DisassociateRoutingProfileQueues" => op_disassociate_routing_profile_queues(
            st,
            param(params, "InstanceId"),
            param(params, "RoutingProfileId"),
            body,
        ),
        "UpdateRoutingProfileQueues" => op_update_routing_profile_queues(
            st,
            param(params, "InstanceId"),
            param(params, "RoutingProfileId"),
            body,
        ),
        _ => op_not_implemented(op),
    }
}

// The rest of the file: op family handlers + unit tests.

fn op_create_instance(
    st: &mut ConnectState,
    body: &Map<String, Value>,
    region: &str,
    account: u64,
) -> Response {
    let identity = match body_str(body, "IdentityManagementType") {
        Ok(v) => v,
        Err(resp) => return resp,
    };
    if identity.is_empty() {
        return invalid("IdentityManagementType is a required parameter");
    }
    let alias = opt_string(body, "InstanceAlias");
    let inbound = body_bool(body, "InboundCallsEnabled", false);
    let outbound = body_bool(body, "OutboundCallsEnabled", false);
    let id = uuid::Uuid::new_v4().to_string();
    let arn = format!("arn:aws:connect:{region}:{account}:instance/{id}");
    let access_url = format!(
        "https://{}.my.connect.aws",
        alias.clone().unwrap_or_else(|| id.clone())
    );
    let mut instance = Map::new();
    instance.insert("Id".into(), json!(id));
    instance.insert("Arn".into(), json!(arn.clone()));
    instance.insert("IdentityManagementType".into(), json!(identity));
    instance.insert("CreatedTime".into(), json!(now_iso()));
    instance.insert(
        "ServiceRole".into(),
        json!(format!(
            "arn:aws:iam::{account}:role/aws-service-role/connect.amazonaws.com/AWSServiceRoleForAmazonConnect"
        )),
    );
    instance.insert("InstanceStatus".into(), json!("ACTIVE"));
    instance.insert("InboundCallsEnabled".into(), json!(inbound));
    instance.insert("OutboundCallsEnabled".into(), json!(outbound));
    instance.insert("InstanceAccessUrl".into(), json!(access_url));
    if let Some(a) = &alias {
        instance.insert("InstanceAlias".into(), json!(a));
    }
    if let Some(tags) = opt_val(body, "Tags") {
        instance.insert("Tags".into(), tags.clone());
        tag_resource(st, &arn, Some(tags));
    }
    instance.insert(
        "__attrs".into(),
        Value::Object(seed_attrs(inbound, outbound)),
    );
    st.instances.insert(id.clone(), Value::Object(instance));
    ok(json!({ "Id": id, "Arn": arn }))
}

fn op_describe_instance(st: &mut ConnectState, instance_id: &str) -> Response {
    match st.instances.get(instance_id) {
        Some(inst) => ok(json!({"Instance": strip_attrs(inst)})),
        None => not_found(format!("Instance with id {instance_id} not found")),
    }
}

fn strip_attrs(inst: &Value) -> Value {
    let mut out = inst.as_object().cloned().unwrap_or_default();
    out.remove("__attrs");
    Value::Object(out)
}

/// `to_summary_dict` for ListInstances — no Tags carry-over.
fn instance_summary(inst: &Value) -> Value {
    let mut s = Map::new();
    if let Some(map) = inst.as_object() {
        for f in [
            "Id",
            "Arn",
            "IdentityManagementType",
            "CreatedTime",
            "ServiceRole",
            "InstanceStatus",
            "InboundCallsEnabled",
            "OutboundCallsEnabled",
            "InstanceAccessUrl",
        ] {
            if let Some(v) = map.get(f) {
                s.insert(f.to_string(), v.clone());
            }
        }
        if let Some(v) = map.get("InstanceAlias") {
            s.insert("InstanceAlias".to_string(), v.clone());
        }
    }
    Value::Object(s)
}

fn op_list_instances(st: &mut ConnectState, query: &BTreeMap<String, Vec<String>>) -> Response {
    let max_results = q1(query, "maxResults")
        .and_then(|v| v.parse::<usize>().ok())
        .unwrap_or(10);
    let mut summaries: Vec<Value> = st.instances.values().map(instance_summary).collect();
    // moto sorts by (created_time, id).
    summaries.sort_by(|a, b| {
        let ka = (str_field(a, "CreatedTime"), str_field(a, "Id"));
        let kb = (str_field(b, "CreatedTime"), str_field(b, "Id"));
        ka.cmp(&kb)
    });
    let start = match q1(query, "nextToken") {
        Some(token) => match decode_page_token(token) {
            Some(id) => summaries.iter().position(|s| str_field(s, "Id") == id),
            None => return invalid_token("Invalid token"),
        },
        None => None,
    };
    let start = start.unwrap_or(0);
    let end = (start + max_results).min(summaries.len());
    let mut out = Map::new();
    out.insert(
        "InstanceSummaryList".into(),
        Value::Array(summaries[start..end].to_vec()),
    );
    if end < summaries.len() && end > start {
        let next_id = str_field(&summaries[end], "Id");
        out.insert("NextToken".into(), json!(encode_page_token(&next_id)));
    }
    ok(Value::Object(out))
}

fn str_field(v: &Value, key: &str) -> String {
    v.get(key).and_then(Value::as_str).unwrap_or("").to_string()
}

fn op_delete_instance(st: &mut ConnectState, instance_id: &str) -> Response {
    if st.instances.remove(instance_id).is_none() {
        return not_found(format!("Instance with id {instance_id} not found"));
    }
    // moto pops every per-instance store on delete.
    let _ = st.analytics_data_associations.remove(instance_id);
    let _ = st.agent_statuses.remove(instance_id);
    let _ = st.contact_flows.remove(instance_id);
    let _ = st.contact_flow_modules.remove(instance_id);
    let _ = st.contact_flow_module_aliases.remove(instance_id);
    let _ = st.hours_of_operations.remove(instance_id);
    let _ = st.hours_of_operation_overrides.remove(instance_id);
    let _ = st.prompts.remove(instance_id);
    let _ = st.queues.remove(instance_id);
    let _ = st.queue_quick_connect_associations.remove(instance_id);
    let _ = st.quick_connects.remove(instance_id);
    let _ = st.routing_profiles.remove(instance_id);
    let _ = st.routing_profile_queue_associations.remove(instance_id);
    let _ = st.security_profiles.remove(instance_id);
    let _ = st.users.remove(instance_id);
    let _ = st.user_hierarchy_groups.remove(instance_id);
    let _ = st.vocabularies.remove(instance_id);
    let _ = st.views.remove(instance_id);
    let _ = st.evaluation_forms.remove(instance_id);
    let _ = st.rules.remove(instance_id);
    let _ = st.predefined_attributes.remove(instance_id);
    let _ = st.task_templates.remove(instance_id);
    let _ = st.contacts.remove(instance_id);
    let _ = st.contact_attributes.remove(instance_id);
    let _ = st.contact_evaluations.remove(instance_id);
    let _ = st.email_addresses.remove(instance_id);
    let _ = st.notifications.remove(instance_id);
    let _ = st.workspaces.remove(instance_id);
    let _ = st.data_tables.remove(instance_id);
    let _ = st.data_table_attributes.remove(instance_id);
    let _ = st.test_cases.remove(instance_id);
    let _ = st.integration_associations.remove(instance_id);
    let _ = st.use_cases.remove(instance_id);
    let _ = st.approved_origins.remove(instance_id);
    let _ = st.lambda_functions.remove(instance_id);
    let _ = st.bots.remove(instance_id);
    let _ = st.security_keys.remove(instance_id);
    let _ = st.instance_storage_configs.remove(instance_id);
    ok(json!({}))
}

fn op_describe_instance_attribute(
    st: &mut ConnectState,
    instance_id: &str,
    attr_type: &str,
) -> Response {
    let inst = match st.instances.get(instance_id) {
        Some(i) => i,
        None => return not_found(format!("Instance with id {instance_id} not found")),
    };
    match inst
        .get("__attrs")
        .and_then(|a| a.get(attr_type))
        .and_then(Value::as_str)
    {
        Some(v) => ok(json!({"Attribute": {"AttributeType": attr_type, "Value": v}})),
        None => not_found(format!(
            "Attribute {attr_type} not found for instance {instance_id}"
        )),
    }
}

fn op_update_instance_attribute(
    st: &mut ConnectState,
    instance_id: &str,
    attr_type: &str,
    body: &Map<String, Value>,
) -> Response {
    let value = match body_str(body, "Value") {
        Ok(v) => v,
        Err(resp) => return resp,
    };
    let inst = match st.instances.get_mut(instance_id) {
        Some(i) => i,
        None => return not_found(format!("Instance with id {instance_id} not found")),
    };
    let mut attrs = inst
        .get("__attrs")
        .and_then(Value::as_object)
        .cloned()
        .unwrap_or_default();
    attrs.insert(attr_type.to_string(), json!(value));
    if let Some(map) = inst.as_object_mut() {
        map.insert("__attrs".into(), Value::Object(attrs));
    }
    ok(json!({}))
}

fn op_list_instance_attributes(st: &mut ConnectState, instance_id: &str) -> Response {
    let inst = match st.instances.get(instance_id) {
        Some(i) => i,
        None => return not_found(format!("Instance with id {instance_id} not found")),
    };
    let empty = Map::new();
    let attrs = inst
        .get("__attrs")
        .and_then(Value::as_object)
        .unwrap_or(&empty);
    let rows: Vec<Value> = attrs
        .iter()
        .map(|(k, v)| json!({ "AttributeType": k, "Value": v }))
        .collect();
    ok(json!({ "Attributes": rows }))
}

// ---------------------------------------------------------------------------
// Instance associations (origins, lambda, bots, keys, storage, analytics)
// ---------------------------------------------------------------------------

fn op_associate_approved_origin(
    st: &mut ConnectState,
    instance_id: &str,
    body: &Map<String, Value>,
) -> Response {
    if !st.instances.contains_key(instance_id) {
        return notfound_instance(instance_id);
    }
    let origin = match body_str(body, "Origin") {
        Ok(v) => v,
        Err(resp) => return resp,
    };
    let list = st
        .approved_origins
        .entry(instance_id.to_string())
        .or_default();
    if !list.contains(&origin) {
        list.push(origin);
    }
    ok(json!({}))
}

fn op_list_approved_origins(st: &mut ConnectState, instance_id: &str) -> Response {
    if !st.instances.contains_key(instance_id) {
        return notfound_instance(instance_id);
    }
    ok(json!({"Origins": st.approved_origins.get(instance_id)}))
}

fn op_disassociate_approved_origin(
    st: &mut ConnectState,
    instance_id: &str,
    origin: Option<&str>,
) -> Response {
    if !st.instances.contains_key(instance_id) {
        return notfound_instance(instance_id);
    }
    if let Some(o) = origin {
        if let Some(list) = st.approved_origins.get_mut(instance_id) {
            list.retain(|x| x != o);
        }
    }
    ok(json!({}))
}

fn op_associate_lambda_function(
    st: &mut ConnectState,
    instance_id: &str,
    body: &Map<String, Value>,
) -> Response {
    if !st.instances.contains_key(instance_id) {
        return notfound_instance(instance_id);
    }
    let arn = match body_str(body, "FunctionArn") {
        Ok(v) => v,
        Err(resp) => return resp,
    };
    let list = st
        .lambda_functions
        .entry(instance_id.to_string())
        .or_default();
    if !list.contains(&arn) {
        list.push(arn);
    }
    ok(json!({}))
}

fn op_list_lambda_functions(st: &mut ConnectState, instance_id: &str) -> Response {
    if !st.instances.contains_key(instance_id) {
        return notfound_instance(instance_id);
    }
    ok(json!({"LambdaFunctions": st.lambda_functions.get(instance_id)}))
}

fn op_disassociate_lambda_function(
    st: &mut ConnectState,
    instance_id: &str,
    function_arn: Option<&str>,
) -> Response {
    if !st.instances.contains_key(instance_id) {
        return notfound_instance(instance_id);
    }
    if let Some(arn) = function_arn {
        if let Some(list) = st.lambda_functions.get_mut(instance_id) {
            list.retain(|x| x != arn);
        }
    }
    ok(json!({}))
}

fn op_associate_bot(
    st: &mut ConnectState,
    instance_id: &str,
    body: &Map<String, Value>,
) -> Response {
    if !st.instances.contains_key(instance_id) {
        return notfound_instance(instance_id);
    }
    let mut entry = Map::new();
    for key in ["LexBot", "LexV2Bot"] {
        if let Some(v) = opt_val(body, key) {
            entry.insert(key.to_string(), v);
        }
    }
    st.bots
        .entry(instance_id.to_string())
        .or_default()
        .push(Value::Object(entry));
    ok(json!({}))
}

fn op_disassociate_bot(
    st: &mut ConnectState,
    instance_id: &str,
    body: &Map<String, Value>,
) -> Response {
    if !st.instances.contains_key(instance_id) {
        return notfound_instance(instance_id);
    }
    if let Some(list) = st.bots.get_mut(instance_id) {
        if let Some(lex) = opt_val(body, "LexBot") {
            list.retain(|b| b.get("LexBot") != Some(&lex));
        }
        if let Some(lex) = opt_val(body, "LexV2Bot") {
            list.retain(|b| b.get("LexV2Bot") != Some(&lex));
        }
    }
    ok(json!({}))
}

fn op_list_bots(st: &mut ConnectState, instance_id: &str) -> Response {
    if !st.instances.contains_key(instance_id) {
        return notfound_instance(instance_id);
    }
    ok(json!({"LexBots": st.bots.get(instance_id)}))
}

fn op_associate_security_key(
    st: &mut ConnectState,
    instance_id: &str,
    body: &Map<String, Value>,
) -> Response {
    if !st.instances.contains_key(instance_id) {
        return notfound_instance(instance_id);
    }
    let key = match body_str(body, "Key") {
        Ok(v) => v,
        Err(resp) => return resp,
    };
    let assoc_id = uuid::Uuid::new_v4().to_string();
    st.security_keys
        .entry(instance_id.to_string())
        .or_default()
        .push(json!({
            "AssociationId": assoc_id,
            "Key": key,
            "CreationTime": now_iso(),
        }));
    ok(json!({"AssociationId": assoc_id}))
}

fn op_list_security_keys(st: &mut ConnectState, instance_id: &str) -> Response {
    if !st.instances.contains_key(instance_id) {
        return notfound_instance(instance_id);
    }
    ok(json!({"SecurityKeys": st.security_keys.get(instance_id)}))
}

fn op_disassociate_security_key(
    st: &mut ConnectState,
    instance_id: &str,
    association_id: &str,
) -> Response {
    if !st.instances.contains_key(instance_id) {
        return notfound_instance(instance_id);
    }
    if let Some(list) = st.security_keys.get_mut(instance_id) {
        list.retain(|k| k.get("AssociationId") != Some(&json!(association_id)));
    }
    ok(json!({}))
}

fn op_associate_instance_storage_config(
    st: &mut ConnectState,
    instance_id: &str,
    body: &Map<String, Value>,
) -> Response {
    if !st.instances.contains_key(instance_id) {
        return notfound_instance(instance_id);
    }
    let resource_type = match body_str(body, "ResourceType") {
        Ok(v) => v,
        Err(resp) => return resp,
    };
    let storage_config = match opt_val(body, "StorageConfig") {
        Some(v) => v,
        None => return missing_body_param("StorageConfig"),
    };
    let assoc_id = uuid::Uuid::new_v4().to_string();
    st.instance_storage_configs
        .entry(instance_id.to_string())
        .or_default()
        .push(json!({
            "AssociationId": assoc_id,
            "ResourceType": resource_type,
            "StorageConfig": storage_config,
        }));
    ok(json!({"AssociationId": assoc_id}))
}

fn op_list_instance_storage_configs(st: &mut ConnectState, instance_id: &str) -> Response {
    if !st.instances.contains_key(instance_id) {
        return notfound_instance(instance_id);
    }
    ok(json!({"StorageConfigs": st.instance_storage_configs.get(instance_id)}))
}

fn op_disassociate_instance_storage_config(
    st: &mut ConnectState,
    instance_id: &str,
    association_id: &str,
) -> Response {
    if !st.instances.contains_key(instance_id) {
        return notfound_instance(instance_id);
    }
    if let Some(list) = st.instance_storage_configs.get_mut(instance_id) {
        list.retain(|c| c.get("AssociationId") != Some(&json!(association_id)));
    }
    ok(json!({}))
}

/// Analytics data association — moto's dup message is parameter-shaped.
fn op_associate_analytics_data_set(
    st: &mut ConnectState,
    instance_id: &str,
    body: &Map<String, Value>,
    account: u64,
    region: &str,
) -> Response {
    let data_set_id = match body_str(body, "DataSetId") {
        Ok(v) => v,
        Err(resp) => return resp,
    };
    if !st.instances.contains_key(instance_id) {
        return notfound_instance(instance_id);
    }
    let target_account = opt_string(body, "TargetAccountId").unwrap_or_else(|| account.to_string());
    let existing = st
        .analytics_data_associations
        .entry(instance_id.to_string())
        .or_default();
    if existing.contains_key(&data_set_id) {
        return invalid(format!(
            "Analytics data association for data set {data_set_id} already exists for instance {instance_id}"
        ));
    }
    let resource_share_id = uuid::Uuid::new_v4().to_string();
    let assoc = json!({
        "DataSetId": data_set_id,
        "TargetAccountId": target_account,
        "ResourceShareId": resource_share_id,
        "ResourceShareArn": format!("arn:aws:ram:{region}:{account}:resource-share/{resource_share_id}"),
        "ResourceShareStatus": "ACTIVE",
    });
    existing.insert(data_set_id, assoc.clone());
    ok(assoc)
}

fn op_disassociate_analytics_data_set(
    st: &mut ConnectState,
    instance_id: &str,
    body: &Map<String, Value>,
) -> Response {
    let data_set_id = match body_str(body, "DataSetId") {
        Ok(v) => v,
        Err(resp) => return resp,
    };
    if !st.instances.contains_key(instance_id) {
        return notfound_instance(instance_id);
    }
    let exists = st
        .analytics_data_associations
        .get(instance_id)
        .is_some_and(|m| m.contains_key(&data_set_id));
    if !exists {
        return not_found(format!(
            "Analytics data association for data set {data_set_id} not found for instance {instance_id}"
        ));
    }
    if let Some(map) = st.analytics_data_associations.get_mut(instance_id) {
        map.remove(&data_set_id);
    }
    ok(json!({}))
}

fn op_list_analytics_data_associations(
    st: &mut ConnectState,
    instance_id: &str,
    query: &BTreeMap<String, Vec<String>>,
) -> Response {
    if !st.instances.contains_key(instance_id) {
        return notfound_instance(instance_id);
    }
    let data_set_filter = q1(query, "DataSetId");
    let max_results = q1(query, "maxResults")
        .and_then(|v| v.parse::<usize>().ok())
        .unwrap_or(1000);
    let mut associations: Vec<Value> = st
        .analytics_data_associations
        .get(instance_id)
        .map(|m| m.values().cloned().collect())
        .unwrap_or_default();
    if let Some(ds) = data_set_filter {
        associations.retain(|a| a.get("DataSetId") == Some(&Value::String(ds.to_string())));
    }
    // moto sorts by DataSetId before paging.
    associations.sort_by_key(|a| str_field(a, "DataSetId"));
    let start = match q1(query, "nextToken") {
        Some(token) => match decode_page_token(token) {
            Some(ds_id) => associations
                .iter()
                .position(|a| str_field(a, "DataSetId") == ds_id),
            None => return invalid_token("Invalid token"),
        },
        None => None,
    };
    let start = start.unwrap_or(0);
    let end = (start + max_results).min(associations.len());
    let mut out = Map::new();
    out.insert(
        "Results".into(),
        Value::Array(associations[start..end].to_vec()),
    );
    if end < associations.len() && end > start {
        out.insert(
            "NextToken".into(),
            json!(encode_page_token(&str_field(
                &associations[end],
                "DataSetId"
            ))),
        );
    }
    ok(Value::Object(out))
}

// ---------------------------------------------------------------------------
// Tags (moto TaggingService)
// ---------------------------------------------------------------------------

fn op_tag_resource(
    st: &mut ConnectState,
    resource_arn: &str,
    body: &Map<String, Value>,
) -> Response {
    if let Some(Value::Object(map)) = opt_val(body, "tags") {
        let entry = st.tags.entry(resource_arn.to_string()).or_default();
        for (k, v) in map {
            entry.insert(k, v);
        }
    }
    ok(json!({}))
}

fn op_untag_resource(
    st: &mut ConnectState,
    resource_arn: &str,
    query: &BTreeMap<String, Vec<String>>,
) -> Response {
    if let Some(map) = st.tags.get_mut(resource_arn) {
        for key in query_values(query, "tagKeys") {
            map.remove(&key);
        }
    }
    ok(json!({}))
}

fn op_list_tags_for_resource(st: &mut ConnectState, resource_arn: &str) -> Response {
    let empty = Map::new();
    let tags = st.tags.get(resource_arn).unwrap_or(&empty).clone();
    ok(json!({ "tags": tags }))
}

fn notfound_instance(instance_id: &str) -> Response {
    not_found(format!("Instance with id {instance_id} not found"))
}

// ---------------------------------------------------------------------------
// Agent statuses
// ---------------------------------------------------------------------------

/// moto stamps `instance_arn.split(":")[3]` as the last-modified region.
fn region_of(instance: &Value) -> String {
    instance
        .get("Arn")
        .and_then(Value::as_str)
        .and_then(|arn| arn.split(':').nth(3).map(str::to_string))
        .unwrap_or_default()
}

fn op_create_agent_status(
    st: &mut ConnectState,
    instance_id: &str,
    body: &Map<String, Value>,
) -> Response {
    let instance = match st.instances.get(instance_id) {
        Some(i) => i.clone(),
        None => return notfound_instance(instance_id),
    };
    let name = match body_str(body, "Name") {
        Ok(v) => v,
        Err(resp) => return resp,
    };
    let state = match body_str(body, "State") {
        Ok(v) => v,
        Err(resp) => return resp,
    };
    let agent_status_id = uuid::Uuid::new_v4().to_string();
    let status_arn = format!(
        "{}/agent-status/{agent_status_id}",
        str_field(&instance, "Arn")
    );
    let mut out = Map::new();
    out.insert("AgentStatusARN".into(), json!(status_arn.clone()));
    out.insert("AgentStatusId".into(), json!(agent_status_id));
    out.insert("Name".into(), json!(name));
    out.insert(
        "Description".into(),
        json!(opt_string(body, "Description").unwrap_or_default()),
    );
    out.insert("Type".into(), json!("CUSTOM"));
    out.insert("State".into(), json!(state));
    out.insert(
        "Tags".into(),
        opt_val(body, "Tags").unwrap_or_else(|| json!({})),
    );
    let last_modified = now_iso();
    out.insert("LastModifiedTime".into(), json!(last_modified.clone()));
    out.insert("LastModifiedRegion".into(), json!(region_of(&instance)));
    if let Some(order) = body.get("DisplayOrder").cloned() {
        out.insert("DisplayOrder".into(), order);
    }
    st.agent_statuses
        .entry(instance_id.to_string())
        .or_default()
        .insert(agent_status_id.clone(), Value::Object(out.clone()));
    tag_resource(st, &status_arn, opt_val(body, "Tags"));
    ok(Value::Object(out))
}

fn op_describe_agent_status(
    st: &mut ConnectState,
    instance_id: &str,
    agent_status_id: &str,
) -> Response {
    if !st.instances.contains_key(instance_id) {
        return notfound_instance(instance_id);
    }
    match st
        .agent_statuses
        .get(instance_id)
        .and_then(|m| m.get(agent_status_id))
    {
        Some(s) => ok(json!({"AgentStatus": s.clone()})),
        None => not_found(format!(
            "Agent status {agent_status_id} not found in instance {instance_id}"
        )),
    }
}

fn op_update_agent_status(
    st: &mut ConnectState,
    instance_id: &str,
    agent_status_id: &str,
    body: &Map<String, Value>,
) -> Response {
    if !st.instances.contains_key(instance_id) {
        return notfound_instance(instance_id);
    }
    let status = match st
        .agent_statuses
        .get_mut(instance_id)
        .and_then(|m| m.get_mut(agent_status_id))
    {
        Some(s) => s,
        None => return not_found(format!("Agent status {agent_status_id} not found")),
    };
    if let Some(name) = opt_val(body, "Name") {
        status["Name"] = name;
    }
    if let Some(desc) = opt_val(body, "Description") {
        status["Description"] = desc;
    }
    if let Some(state) = opt_val(body, "State") {
        status["State"] = state;
    }
    if let Some(order) = body.get("DisplayOrder") {
        status["DisplayOrder"] = order.clone();
    }
    if body
        .get("ResetOrderNumber")
        .and_then(Value::as_bool)
        .unwrap_or(false)
    {
        if let Some(m) = status.as_object_mut() {
            m.remove("DisplayOrder");
        }
    }
    status["LastModifiedTime"] = json!(now_iso());
    ok(json!({}))
}

fn op_list_agent_statuses(st: &mut ConnectState, instance_id: &str) -> Response {
    if !st.instances.contains_key(instance_id) {
        return notfound_instance(instance_id);
    }
    let empty = HashMap::new();
    let statuses = st.agent_statuses.get(instance_id).unwrap_or(&empty);
    let rows: Vec<Value> = statuses
        .values()
        .map(|s| {
            let mut m = Map::new();
            for (src, dst) in [
                ("AgentStatusId", "Id"),
                ("AgentStatusARN", "Arn"),
                ("Name", "Name"),
                ("Type", "Type"),
                ("LastModifiedTime", "LastModifiedTime"),
                ("LastModifiedRegion", "LastModifiedRegion"),
            ] {
                if let Some(v) = s.get(src) {
                    m.insert(dst.to_string(), v.clone());
                }
            }
            Value::Object(m)
        })
        .collect();
    ok(json!({"AgentStatusSummaryList": rows}))
}

// ---------------------------------------------------------------------------
// Security profiles
// ---------------------------------------------------------------------------

fn op_create_security_profile(
    st: &mut ConnectState,
    instance_id: &str,
    body: &Map<String, Value>,
) -> Response {
    let instance = match st.instances.get(instance_id) {
        Some(i) => i.clone(),
        None => return notfound_instance(instance_id),
    };
    let name = match body_str(body, "SecurityProfileName") {
        Ok(v) => v,
        Err(resp) => return resp,
    };
    let sp_id = uuid::Uuid::new_v4().to_string();
    let arn = format!("{}/security-profile/{sp_id}", str_field(&instance, "Arn"));
    let mut stored = Map::new();
    stored.insert("Id".into(), json!(sp_id.clone()));
    stored.insert("Arn".into(), json!(arn.clone()));
    stored.insert("SecurityProfileName".into(), json!(name));
    stored.insert(
        "Description".into(),
        json!(opt_string(body, "Description").unwrap_or_default()),
    );
    stored.insert(
        "Tags".into(),
        opt_val(body, "Tags").unwrap_or_else(|| json!({})),
    );
    // moto keeps perms/AC-tags off `to_dict` (list_permissions reads them).
    stored.insert(
        "__permissions".into(),
        opt_val(body, "Permissions").unwrap_or_else(|| json!([])),
    );
    stored.insert(
        "__allowed_access_control_tags".into(),
        opt_val(body, "AllowedAccessControlTags").unwrap_or_else(|| json!({})),
    );
    stored.insert(
        "__tag_restricted_resources".into(),
        opt_val(body, "TagRestrictedResources").unwrap_or_else(|| json!([])),
    );
    stored.insert(
        "TagRestrictedResources".into(),
        opt_val(body, "TagRestrictedResources").unwrap_or_else(|| json!([])),
    );
    stored.insert("LastModifiedTime".into(), json!(now_iso()));
    stored.insert("LastModifiedRegion".into(), json!(region_of(&instance)));
    st.security_profiles
        .entry(instance_id.to_string())
        .or_default()
        .insert(sp_id.clone(), Value::Object(stored));
    tag_resource(st, &arn, opt_val(body, "Tags"));
    ok(json!({"SecurityProfileId": sp_id, "SecurityProfileArn": arn}))
}

fn op_describe_security_profile(st: &mut ConnectState, instance_id: &str, sp_id: &str) -> Response {
    if !st.instances.contains_key(instance_id) {
        return notfound_instance(instance_id);
    }
    match st
        .security_profiles
        .get(instance_id)
        .and_then(|m| m.get(sp_id))
    {
        Some(sp) => ok(json!({"SecurityProfile": sp.clone()})),
        None => not_found(format!(
            "Security profile {sp_id} not found in instance {instance_id}"
        )),
    }
}

fn op_update_security_profile(
    st: &mut ConnectState,
    instance_id: &str,
    sp_id: &str,
    body: &Map<String, Value>,
) -> Response {
    if !st.instances.contains_key(instance_id) {
        return notfound_instance(instance_id);
    }
    let sp = match st
        .security_profiles
        .get_mut(instance_id)
        .and_then(|m| m.get_mut(sp_id))
    {
        Some(sp) => sp,
        None => {
            return not_found(format!("Security profile {sp_id} not found"));
        }
    };
    if let Some(name) = opt_val(body, "SecurityProfileName") {
        sp["SecurityProfileName"] = name;
    }
    if let Some(desc) = opt_val(body, "Description") {
        sp["Description"] = desc;
    }
    if let Some(permissions) = opt_val(body, "Permissions") {
        sp["__permissions"] = permissions;
    }
    if let Some(tags) = opt_val(body, "AllowedAccessControlTags") {
        sp["__allowed_access_control_tags"] = tags;
    }
    if let Some(resources) = opt_val(body, "TagRestrictedResources") {
        sp["__tag_restricted_resources"] = resources;
    }
    sp["LastModifiedTime"] = json!(now_iso());
    ok(json!({}))
}

fn op_delete_security_profile(st: &mut ConnectState, instance_id: &str, sp_id: &str) -> Response {
    if !st.instances.contains_key(instance_id) {
        return notfound_instance(instance_id);
    }
    match st
        .security_profiles
        .get_mut(instance_id)
        .and_then(|m| m.remove(sp_id))
    {
        Some(_) => ok(json!({})),
        None => not_found(format!("Security profile {sp_id} not found")),
    }
}

fn op_list_security_profiles(st: &mut ConnectState, instance_id: &str) -> Response {
    if !st.instances.contains_key(instance_id) {
        return notfound_instance(instance_id);
    }
    let empty = HashMap::new();
    let profiles = st.security_profiles.get(instance_id).unwrap_or(&empty);
    let rows: Vec<Value> = profiles
        .values()
        .map(|sp| {
            json!({
                "Id": sp["Id"],
                "Arn": sp["Arn"],
                "Name": sp["SecurityProfileName"],
                "LastModifiedTime": sp["LastModifiedTime"],
                "LastModifiedRegion": sp["LastModifiedRegion"],
            })
        })
        .collect();
    ok(json!({"SecurityProfileSummaryList": rows}))
}

fn op_list_security_profile_permissions(
    st: &mut ConnectState,
    instance_id: &str,
    sp_id: &str,
) -> Response {
    if !st.instances.contains_key(instance_id) {
        return notfound_instance(instance_id);
    }
    let perms = st
        .security_profiles
        .get(instance_id)
        .and_then(|m| m.get(sp_id))
        .and_then(|sp| sp.get("__permissions").cloned())
        .unwrap_or_else(|| json!([]));
    ok(json!({"Permissions": perms}))
}

fn op_list_security_profile_applications(st: &mut ConnectState, instance_id: &str) -> Response {
    if !st.instances.contains_key(instance_id) {
        return notfound_instance(instance_id);
    }
    ok(json!({"Applications": []}))
}

// ---------------------------------------------------------------------------
// Users
// ---------------------------------------------------------------------------

fn user_default_phone_config() -> Value {
    json!({
        "PhoneType": "SOFT_PHONE",
        "AutoAccept": false,
        "AfterContactWorkTimeLimit": 0,
    })
}

fn op_create_user(st: &mut ConnectState, instance_id: &str, body: &Map<String, Value>) -> Response {
    let instance = match st.instances.get(instance_id) {
        Some(i) => i.clone(),
        None => return notfound_instance(instance_id),
    };
    let username = match body_str(body, "Username") {
        Ok(v) => v,
        Err(resp) => return resp,
    };
    let security_profile_ids = match opt_val(body, "SecurityProfileIds") {
        Some(v) => v,
        None => return missing_body_param("SecurityProfileIds"),
    };
    let routing_profile_id = match body_str(body, "RoutingProfileId") {
        Ok(v) => v,
        Err(resp) => return resp,
    };
    let user_id = uuid::Uuid::new_v4().to_string();
    let arn = format!("{}/agent/{user_id}", str_field(&instance, "Arn"));
    let mut user = Map::new();
    user.insert("Id".into(), json!(user_id.clone()));
    user.insert("Arn".into(), json!(arn.clone()));
    user.insert("Username".into(), json!(username));
    user.insert(
        "IdentityInfo".into(),
        opt_val(body, "IdentityInfo").unwrap_or_else(|| json!({})),
    );
    user.insert(
        "PhoneConfig".into(),
        opt_val(body, "PhoneConfig")
            .filter(|v| !v.is_null())
            .unwrap_or_else(user_default_phone_config),
    );
    user.insert("SecurityProfileIds".into(), security_profile_ids);
    user.insert("RoutingProfileId".into(), json!(routing_profile_id));
    user.insert(
        "Tags".into(),
        opt_val(body, "Tags").unwrap_or_else(|| json!({})),
    );
    user.insert("LastModifiedTime".into(), json!(now_iso()));
    user.insert("LastModifiedRegion".into(), json!(region_of(&instance)));
    if let Some(dir_id) = opt_val(body, "DirectoryUserId").filter(|v| !v.is_null()) {
        user.insert("DirectoryUserId".into(), dir_id);
    }
    if let Some(hg) = opt_val(body, "HierarchyGroupId").filter(|v| !v.is_null()) {
        user.insert("HierarchyGroupId".into(), hg);
    }
    st.users
        .entry(instance_id.to_string())
        .or_default()
        .insert(user_id.clone(), Value::Object(user));
    tag_resource(st, &arn, opt_val(body, "Tags"));
    ok(json!({"UserId": user_id, "UserArn": arn}))
}

fn op_describe_user(st: &mut ConnectState, instance_id: &str, user_id: &str) -> Response {
    if !st.instances.contains_key(instance_id) {
        return notfound_instance(instance_id);
    }
    match st.users.get(instance_id).and_then(|m| m.get(user_id)) {
        Some(u) => ok(json!({"User": u.clone()})),
        None => not_found(format!(
            "User {user_id} not found in instance {instance_id}"
        )),
    }
}

fn op_update_user_field(
    st: &mut ConnectState,
    instance_id: &str,
    user_id: &str,
    apply: impl FnOnce(&mut Value),
) -> Response {
    if !st.instances.contains_key(instance_id) {
        return notfound_instance(instance_id);
    }
    let user = match st
        .users
        .get_mut(instance_id)
        .and_then(|m| m.get_mut(user_id))
    {
        Some(u) => u,
        None => return not_found(format!("User {user_id} not found")),
    };
    apply(user);
    user["LastModifiedTime"] = json!(now_iso());
    ok(json!({}))
}

fn op_delete_user(st: &mut ConnectState, instance_id: &str, user_id: &str) -> Response {
    if !st.instances.contains_key(instance_id) {
        return notfound_instance(instance_id);
    }
    match st
        .users
        .get_mut(instance_id)
        .and_then(|m| m.remove(user_id))
    {
        Some(_) => ok(json!({})),
        None => not_found(format!("User {user_id} not found")),
    }
}

fn op_list_users(st: &mut ConnectState, instance_id: &str) -> Response {
    if !st.instances.contains_key(instance_id) {
        return notfound_instance(instance_id);
    }
    let empty = HashMap::new();
    let users = st.users.get(instance_id).unwrap_or(&empty);
    let rows: Vec<Value> = users
        .values()
        .map(|u| {
            json!({
                "Id": u["Id"],
                "Arn": u["Arn"],
                "Username": u["Username"],
            })
        })
        .collect();
    ok(json!({"UserSummaryList": rows}))
}

// ---------------------------------------------------------------------------
// User hierarchy groups + structure
// ---------------------------------------------------------------------------

fn op_create_user_hierarchy_group(
    st: &mut ConnectState,
    instance_id: &str,
    body: &Map<String, Value>,
) -> Response {
    let instance = match st.instances.get(instance_id) {
        Some(i) => i.clone(),
        None => return notfound_instance(instance_id),
    };
    let name = match body_str(body, "Name") {
        Ok(v) => v,
        Err(resp) => return resp,
    };
    let group_id = uuid::Uuid::new_v4().to_string();
    let arn = format!("{}/agent-group/{group_id}", str_field(&instance, "Arn"));
    let mut group = Map::new();
    group.insert("Id".into(), json!(group_id.clone()));
    group.insert("Arn".into(), json!(arn.clone()));
    group.insert("Name".into(), json!(name));
    group.insert("LevelId".into(), json!("1"));
    group.insert(
        "Tags".into(),
        opt_val(body, "Tags").unwrap_or_else(|| json!({})),
    );
    group.insert("LastModifiedTime".into(), json!(now_iso()));
    group.insert("LastModifiedRegion".into(), json!(region_of(&instance)));
    st.user_hierarchy_groups
        .entry(instance_id.to_string())
        .or_default()
        .insert(group_id.clone(), Value::Object(group));
    tag_resource(st, &arn, opt_val(body, "Tags"));
    ok(json!({"HierarchyGroupId": group_id, "HierarchyGroupArn": arn}))
}

fn op_describe_user_hierarchy_group(
    st: &mut ConnectState,
    instance_id: &str,
    group_id: &str,
) -> Response {
    if !st.instances.contains_key(instance_id) {
        return notfound_instance(instance_id);
    }
    match st
        .user_hierarchy_groups
        .get(instance_id)
        .and_then(|m| m.get(group_id))
    {
        Some(g) => ok(json!({"HierarchyGroup": g.clone()})),
        None => not_found(format!(
            "Hierarchy group {group_id} not found in instance {instance_id}"
        )),
    }
}

fn op_delete_user_hierarchy_group(
    st: &mut ConnectState,
    instance_id: &str,
    group_id: &str,
) -> Response {
    if !st.instances.contains_key(instance_id) {
        return notfound_instance(instance_id);
    }
    match st
        .user_hierarchy_groups
        .get_mut(instance_id)
        .and_then(|m| m.remove(group_id))
    {
        Some(_) => ok(json!({})),
        None => not_found(format!("Hierarchy group {group_id} not found")),
    }
}

fn op_update_user_hierarchy_group_name(
    st: &mut ConnectState,
    instance_id: &str,
    group_id: &str,
    body: &Map<String, Value>,
) -> Response {
    if !st.instances.contains_key(instance_id) {
        return notfound_instance(instance_id);
    }
    let name = match body_str(body, "Name") {
        Ok(v) => v,
        Err(resp) => return resp,
    };
    let group = match st
        .user_hierarchy_groups
        .get_mut(instance_id)
        .and_then(|m| m.get_mut(group_id))
    {
        Some(g) => g,
        None => return not_found(format!("Hierarchy group {group_id} not found")),
    };
    group["Name"] = json!(name);
    group["LastModifiedTime"] = json!(now_iso());
    ok(json!({}))
}

fn op_list_user_hierarchy_groups(st: &mut ConnectState, instance_id: &str) -> Response {
    if !st.instances.contains_key(instance_id) {
        return notfound_instance(instance_id);
    }
    let empty = HashMap::new();
    let groups = st.user_hierarchy_groups.get(instance_id).unwrap_or(&empty);
    let rows: Vec<Value> = groups
        .values()
        .map(|g| {
            json!({
                "Id": g["Id"],
                "Arn": g["Arn"],
                "Name": g["Name"],
                "LastModifiedTime": g["LastModifiedTime"],
                "LastModifiedRegion": g["LastModifiedRegion"],
            })
        })
        .collect();
    ok(json!({"UserHierarchyGroupSummaryList": rows}))
}

fn op_describe_user_hierarchy_structure(
    st: &mut ConnectState,
    instance_id: &str,
    region: &str,
    account: u64,
) -> Response {
    if !st.instances.contains_key(instance_id) {
        return notfound_instance(instance_id);
    }
    let level = |n: u32, name: &str| {
        let arn = format!(
            "arn:aws:connect:{region}:{account}:instance/{instance_id}/agent-group-type/level-{n}"
        );
        json!({"Id": n.to_string(), "Arn": arn, "Name": name})
    };
    ok(json!({
        "HierarchyStructure": {
            "LevelOne": level(1, "Organization"),
            "LevelTwo": level(2, "Division"),
            "LevelThree": level(3, "Department"),
            "LevelFour": level(4, "Team"),
            "LevelFive": level(5, "Agent"),
        }
    }))
}

fn op_update_user_hierarchy_structure(st: &mut ConnectState, instance_id: &str) -> Response {
    if !st.instances.contains_key(instance_id) {
        return notfound_instance(instance_id);
    }
    ok(json!({}))
}

// ---------------------------------------------------------------------------
// Contact flows
// ---------------------------------------------------------------------------

fn instance_arn_or_404(st: &ConnectState, instance_id: &str) -> Option<String> {
    st.instances.get(instance_id).map(|i| str_field(i, "Arn"))
}

fn op_create_contact_flow(
    st: &mut ConnectState,
    instance_id: &str,
    body: &Map<String, Value>,
) -> Response {
    let instance_arn = match instance_arn_or_404(st, instance_id) {
        Some(a) => a,
        None => return notfound_instance(instance_id),
    };
    let name = match body_str(body, "Name") {
        Ok(v) => v,
        Err(resp) => return resp,
    };
    let flow_type = match body_str(body, "Type") {
        Ok(v) => v,
        Err(resp) => return resp,
    };
    let content = match body_str(body, "Content") {
        Ok(v) => v,
        Err(resp) => return resp,
    };
    let flow_id = uuid::Uuid::new_v4().to_string();
    let arn = format!("{instance_arn}/contact-flow/{flow_id}");
    let status = opt_string(body, "Status").unwrap_or_else(|| "PUBLISHED".into());
    let mut flow = Map::new();
    flow.insert("Arn".into(), json!(arn.clone()));
    flow.insert("Id".into(), json!(flow_id.clone()));
    flow.insert("Name".into(), json!(name));
    flow.insert("Type".into(), json!(flow_type));
    flow.insert("State".into(), json!("ACTIVE"));
    flow.insert("Status".into(), json!(status));
    flow.insert(
        "Description".into(),
        json!(opt_string(body, "Description").unwrap_or_default()),
    );
    flow.insert("Content".into(), json!(content.clone()));
    flow.insert(
        "Tags".into(),
        opt_val(body, "Tags").unwrap_or_else(|| json!({})),
    );
    flow.insert("FlowContentSha256".into(), json!(sha256_hex(&content)));
    st.contact_flows
        .entry(instance_id.to_string())
        .or_default()
        .insert(flow_id.clone(), Value::Object(flow));
    tag_resource(st, &arn, opt_val(body, "Tags"));
    ok(json!({"ContactFlowId": flow_id, "ContactFlowArn": arn}))
}

fn op_describe_contact_flow(st: &mut ConnectState, instance_id: &str, flow_id: &str) -> Response {
    if !st.instances.contains_key(instance_id) {
        return notfound_instance(instance_id);
    }
    match st
        .contact_flows
        .get(instance_id)
        .and_then(|m| m.get(flow_id))
    {
        Some(f) => ok(json!({"ContactFlow": f.clone()})),
        None => not_found(format!(
            "Contact flow {flow_id} not found in instance {instance_id}"
        )),
    }
}

fn op_update_contact_flow_content(
    st: &mut ConnectState,
    instance_id: &str,
    flow_id: &str,
    body: &Map<String, Value>,
) -> Response {
    if !st.instances.contains_key(instance_id) {
        return notfound_instance(instance_id);
    }
    let content = match body_str(body, "Content") {
        Ok(v) => v,
        Err(resp) => return resp,
    };
    let flow = match st
        .contact_flows
        .get_mut(instance_id)
        .and_then(|m| m.get_mut(flow_id))
    {
        Some(f) => f,
        None => return not_found(format!("Contact flow {flow_id} not found")),
    };
    flow["Content"] = json!(content.clone());
    flow["FlowContentSha256"] = json!(sha256_hex(&content));
    ok(json!({}))
}

fn op_update_contact_flow_name(
    st: &mut ConnectState,
    instance_id: &str,
    flow_id: &str,
    body: &Map<String, Value>,
) -> Response {
    if !st.instances.contains_key(instance_id) {
        return notfound_instance(instance_id);
    }
    let flow = match st
        .contact_flows
        .get_mut(instance_id)
        .and_then(|m| m.get_mut(flow_id))
    {
        Some(f) => f,
        None => return not_found(format!("Contact flow {flow_id} not found")),
    };
    if let Some(name) = opt_val(body, "Name") {
        flow["Name"] = name;
    }
    if let Some(desc) = opt_val(body, "Description") {
        flow["Description"] = desc;
    }
    ok(json!({}))
}

fn op_update_contact_flow_metadata(
    st: &mut ConnectState,
    instance_id: &str,
    flow_id: &str,
    body: &Map<String, Value>,
) -> Response {
    if !st.instances.contains_key(instance_id) {
        return notfound_instance(instance_id);
    }
    let flow = match st
        .contact_flows
        .get_mut(instance_id)
        .and_then(|m| m.get_mut(flow_id))
    {
        Some(f) => f,
        None => return not_found(format!("Contact flow {flow_id} not found")),
    };
    if let Some(name) = opt_val(body, "Name") {
        flow["Name"] = name;
    }
    if let Some(desc) = opt_val(body, "Description") {
        flow["Description"] = desc;
    }
    if let Some(state) = opt_val(body, "ContactFlowState") {
        flow["State"] = state;
    }
    ok(json!({}))
}

fn op_delete_contact_flow(st: &mut ConnectState, instance_id: &str, flow_id: &str) -> Response {
    if !st.instances.contains_key(instance_id) {
        return notfound_instance(instance_id);
    }
    match st
        .contact_flows
        .get_mut(instance_id)
        .and_then(|m| m.remove(flow_id))
    {
        Some(_) => ok(json!({})),
        None => not_found(format!("Contact flow {flow_id} not found")),
    }
}

fn op_list_contact_flows(st: &mut ConnectState, instance_id: &str) -> Response {
    if !st.instances.contains_key(instance_id) {
        return notfound_instance(instance_id);
    }
    let empty = HashMap::new();
    let flows = st.contact_flows.get(instance_id).unwrap_or(&empty);
    let rows: Vec<Value> = flows
        .values()
        .map(|f| {
            json!({
                "Id": f["Id"],
                "Arn": f["Arn"],
                "Name": f["Name"],
                "ContactFlowType": f["Type"],
                "ContactFlowState": f["State"],
                "ContactFlowStatus": f["Status"],
            })
        })
        .collect();
    ok(json!({"ContactFlowSummaryList": rows}))
}

fn op_list_contact_flow_versions(
    st: &mut ConnectState,
    instance_id: &str,
    flow_id: &str,
) -> Response {
    if !st.instances.contains_key(instance_id) {
        return notfound_instance(instance_id);
    }
    let rows: Vec<Value> = st
        .contact_flows
        .get(instance_id)
        .and_then(|m| m.get(flow_id))
        .map(|f| {
            vec![json!({
                "Arn": f["Arn"],
                "Id": f["Id"],
                "Name": f["Name"],
                "Version": 1,
            })]
        })
        .unwrap_or_default();
    ok(json!({"ContactFlowVersionSummaryList": rows}))
}

// ---------------------------------------------------------------------------
// Contact flow modules
// ---------------------------------------------------------------------------

fn op_create_contact_flow_module(
    st: &mut ConnectState,
    instance_id: &str,
    body: &Map<String, Value>,
) -> Response {
    let instance_arn = match instance_arn_or_404(st, instance_id) {
        Some(a) => a,
        None => return notfound_instance(instance_id),
    };
    let name = match body_str(body, "Name") {
        Ok(v) => v,
        Err(resp) => return resp,
    };
    let content = match body_str(body, "Content") {
        Ok(v) => v,
        Err(resp) => return resp,
    };
    let module_id = uuid::Uuid::new_v4().to_string();
    let arn = format!("{instance_arn}/flow-module/{module_id}");
    let mut stored = Map::new();
    stored.insert("Arn".into(), json!(arn.clone()));
    stored.insert("Id".into(), json!(module_id.clone()));
    stored.insert("Name".into(), json!(name));
    stored.insert("Content".into(), json!(content.clone()));
    stored.insert(
        "Description".into(),
        json!(opt_string(body, "Description").unwrap_or_default()),
    );
    stored.insert("State".into(), json!("ACTIVE"));
    stored.insert("Status".into(), json!("PUBLISHED"));
    stored.insert(
        "Tags".into(),
        opt_val(body, "Tags").unwrap_or_else(|| json!({})),
    );
    stored.insert(
        "FlowModuleContentSha256".into(),
        json!(sha256_hex(&content)),
    );
    st.contact_flow_modules
        .entry(instance_id.to_string())
        .or_default()
        .insert(module_id.clone(), Value::Object(stored));
    tag_resource(st, &arn, opt_val(body, "Tags"));
    ok(json!({"Id": module_id, "Arn": arn}))
}

fn op_describe_contact_flow_module(
    st: &mut ConnectState,
    instance_id: &str,
    module_id: &str,
) -> Response {
    if !st.instances.contains_key(instance_id) {
        return notfound_instance(instance_id);
    }
    match st
        .contact_flow_modules
        .get(instance_id)
        .and_then(|m| m.get(module_id))
    {
        Some(m) => ok(json!({"ContactFlowModule": m.clone()})),
        None => not_found(format!(
            "Contact flow module {module_id} not found in instance {instance_id}"
        )),
    }
}

fn op_update_contact_flow_module_content(
    st: &mut ConnectState,
    instance_id: &str,
    module_id: &str,
    body: &Map<String, Value>,
) -> Response {
    if !st.instances.contains_key(instance_id) {
        return notfound_instance(instance_id);
    }
    let content = match body_str(body, "Content") {
        Ok(v) => v,
        Err(resp) => return resp,
    };
    let module = match st
        .contact_flow_modules
        .get_mut(instance_id)
        .and_then(|m| m.get_mut(module_id))
    {
        Some(m) => m,
        None => return not_found(format!("Contact flow module {module_id} not found")),
    };
    module["Content"] = json!(content.clone());
    module["FlowModuleContentSha256"] = json!(sha256_hex(&content));
    ok(json!({}))
}

fn op_update_contact_flow_module_metadata(
    st: &mut ConnectState,
    instance_id: &str,
    module_id: &str,
    body: &Map<String, Value>,
) -> Response {
    if !st.instances.contains_key(instance_id) {
        return notfound_instance(instance_id);
    }
    let module = match st
        .contact_flow_modules
        .get_mut(instance_id)
        .and_then(|m| m.get_mut(module_id))
    {
        Some(m) => m,
        None => return not_found(format!("Contact flow module {module_id} not found")),
    };
    if let Some(name) = opt_val(body, "Name") {
        module["Name"] = name;
    }
    if let Some(desc) = opt_val(body, "Description") {
        module["Description"] = desc;
    }
    if let Some(state) = opt_val(body, "State") {
        module["State"] = state;
    }
    ok(json!({}))
}

fn op_delete_contact_flow_module(
    st: &mut ConnectState,
    instance_id: &str,
    module_id: &str,
) -> Response {
    if !st.instances.contains_key(instance_id) {
        return notfound_instance(instance_id);
    }
    match st
        .contact_flow_modules
        .get_mut(instance_id)
        .and_then(|m| m.remove(module_id))
    {
        Some(_) => ok(json!({})),
        None => not_found(format!("Contact flow module {module_id} not found")),
    }
}

fn op_list_contact_flow_modules(st: &mut ConnectState, instance_id: &str) -> Response {
    if !st.instances.contains_key(instance_id) {
        return notfound_instance(instance_id);
    }
    let empty = HashMap::new();
    let modules = st.contact_flow_modules.get(instance_id).unwrap_or(&empty);
    let rows: Vec<Value> = modules
        .values()
        .map(|m| {
            json!({
                "Id": m["Id"],
                "Arn": m["Arn"],
                "Name": m["Name"],
                "State": m["State"],
            })
        })
        .collect();
    ok(json!({"ContactFlowModulesSummaryList": rows}))
}

// ---------------------------------------------------------------------------
// Contact flow module aliases
// ---------------------------------------------------------------------------

fn op_create_contact_flow_module_alias(
    st: &mut ConnectState,
    instance_id: &str,
    module_id: &str,
    body: &Map<String, Value>,
) -> Response {
    let instance_arn = match instance_arn_or_404(st, instance_id) {
        Some(a) => a,
        None => return notfound_instance(instance_id),
    };
    let name = match body_str(body, "AliasName") {
        Ok(v) => v,
        Err(resp) => return resp,
    };
    let version = match body_str(body, "ContactFlowModuleVersion") {
        Ok(v) => v,
        Err(resp) => return resp,
    };
    let alias_id = uuid::Uuid::new_v4().to_string();
    let arn = format!("{instance_arn}/flow-module/{module_id}/alias/{alias_id}");
    let mut alias = Map::new();
    alias.insert("AliasId".into(), json!(alias_id.clone()));
    alias.insert("AliasArn".into(), json!(arn.clone()));
    alias.insert("ContactFlowModuleId".into(), json!(module_id));
    alias.insert("Name".into(), json!(name));
    alias.insert("TargetContactFlowModuleVersion".into(), json!(version));
    alias.insert(
        "Description".into(),
        json!(opt_string(body, "Description").unwrap_or_default()),
    );
    let stamp = now_iso();
    alias.insert("CreateTimestamp".into(), json!(stamp.clone()));
    alias.insert("ModifiedTimestamp".into(), json!(stamp));
    // moto does not require the parent module to exist.
    st.contact_flow_module_aliases
        .entry(instance_id.to_string())
        .or_default()
        .entry(module_id.to_string())
        .or_default()
        .insert(alias_id.clone(), Value::Object(alias));
    ok(json!({"Id": alias_id, "ContactFlowModuleArn": arn}))
}

fn op_describe_contact_flow_module_alias(
    st: &mut ConnectState,
    instance_id: &str,
    module_id: &str,
    alias_id: &str,
) -> Response {
    if !st.instances.contains_key(instance_id) {
        return notfound_instance(instance_id);
    }
    match st
        .contact_flow_module_aliases
        .get(instance_id)
        .and_then(|m| m.get(module_id))
        .and_then(|a| a.get(alias_id))
    {
        Some(a) => ok(json!({"ContactFlowModuleAlias": a.clone()})),
        None => not_found(format!("Contact flow module alias {alias_id} not found")),
    }
}

fn op_list_contact_flow_module_aliases(
    st: &mut ConnectState,
    instance_id: &str,
    module_id: &str,
) -> Response {
    if !st.instances.contains_key(instance_id) {
        return notfound_instance(instance_id);
    }
    let rows: Vec<Value> = st
        .contact_flow_module_aliases
        .get(instance_id)
        .and_then(|m| m.get(module_id))
        .map(|a| a.values().cloned().collect())
        .unwrap_or_default();
    ok(json!({"ContactFlowModuleAliasSummaryList": rows}))
}

fn op_update_contact_flow_module_alias(
    st: &mut ConnectState,
    instance_id: &str,
    module_id: &str,
    alias_id: &str,
    body: &Map<String, Value>,
) -> Response {
    if !st.instances.contains_key(instance_id) {
        return notfound_instance(instance_id);
    }
    let alias = match st
        .contact_flow_module_aliases
        .get_mut(instance_id)
        .and_then(|m| m.get_mut(module_id))
        .and_then(|a| a.get_mut(alias_id))
    {
        Some(a) => a,
        None => return not_found(format!("Contact flow module alias {alias_id} not found")),
    };
    if let Some(name) = opt_val(body, "Name") {
        alias["Name"] = name;
    }
    if let Some(version) = opt_val(body, "TargetContactFlowModuleVersion") {
        alias["TargetContactFlowModuleVersion"] = version;
    }
    if let Some(desc) = opt_val(body, "Description") {
        alias["Description"] = desc;
    }
    alias["ModifiedTimestamp"] = json!(now_iso());
    ok(json!({"ContactFlowModuleAlias": alias.clone()}))
}

fn op_delete_contact_flow_module_alias(
    st: &mut ConnectState,
    instance_id: &str,
    module_id: &str,
    alias_id: &str,
) -> Response {
    if !st.instances.contains_key(instance_id) {
        return notfound_instance(instance_id);
    }
    match st
        .contact_flow_module_aliases
        .get_mut(instance_id)
        .and_then(|m| m.get_mut(module_id))
        .and_then(|a| a.remove(alias_id))
    {
        Some(_) => ok(json!({})),
        None => not_found(format!("Contact flow module alias {alias_id} not found")),
    }
}

// ---------------------------------------------------------------------------
// Contacts
// ---------------------------------------------------------------------------

fn op_create_contact(
    st: &mut ConnectState,
    instance_id: &str,
    body: &Map<String, Value>,
) -> Response {
    let instance_id = if instance_arn_or_404(st, instance_id).is_some() {
        instance_id.to_string()
    } else {
        // moto falls back to the body InstanceId for /contact/create-contact.
        let from_body = opt_string(body, "InstanceId").unwrap_or_default();
        if instance_arn_or_404(st, &from_body).is_none() {
            return notfound_instance(instance_id);
        }
        from_body
    };
    let channel = match body_str(body, "Channel") {
        Ok(v) => v,
        Err(resp) => return resp,
    };
    let initiation_method = match body_str(body, "InitiationMethod") {
        Ok(v) => v,
        Err(resp) => return resp,
    };
    let instance_arn = instance_arn_or_404(st, &instance_id).unwrap_or_default();
    let contact_id = uuid::Uuid::new_v4().to_string();
    let arn = format!("{instance_arn}/contact/{contact_id}");
    let stamp = now_iso();
    let mut contact = Map::new();
    contact.insert("Id".into(), json!(contact_id.clone()));
    contact.insert("Arn".into(), json!(arn.clone()));
    contact.insert("Channel".into(), json!(channel));
    contact.insert("InitiationMethod".into(), json!(initiation_method));
    contact.insert(
        "Name".into(),
        json!(opt_string(body, "Name").unwrap_or_default()),
    );
    contact.insert(
        "Description".into(),
        json!(opt_string(body, "Description").unwrap_or_default()),
    );
    contact.insert("InitiationTimestamp".into(), json!(stamp.clone()));
    contact.insert("LastUpdateTimestamp".into(), json!(stamp));
    if let Some(rc) =
        opt_val(body, "RelatedContactId").filter(|v| !v.as_str().unwrap_or_default().is_empty())
    {
        contact.insert("RelatedContactId".into(), rc);
    }
    if let Some(sa) =
        opt_val(body, "SegmentAttributes").filter(|v| v.as_object().is_some_and(|m| !m.is_empty()))
    {
        contact.insert("SegmentAttributes".into(), sa);
    }
    if let Some(ui) = opt_val(body, "UserInfo").filter(|v| !v.is_null()) {
        contact.insert("UserInfo".into(), ui);
    }
    st.contacts
        .entry(instance_id)
        .or_default()
        .insert(contact_id.clone(), Value::Object(contact));
    ok(json!({"ContactId": contact_id, "ContactArn": arn}))
}

fn op_describe_contact(st: &mut ConnectState, instance_id: &str, contact_id: &str) -> Response {
    if !st.instances.contains_key(instance_id) {
        return notfound_instance(instance_id);
    }
    match st.contacts.get(instance_id).and_then(|m| m.get(contact_id)) {
        Some(c) => ok(json!({"Contact": c.clone()})),
        None => not_found(format!("Contact {contact_id} not found")),
    }
}

fn op_update_contact(
    st: &mut ConnectState,
    instance_id: &str,
    contact_id: &str,
    body: &Map<String, Value>,
) -> Response {
    if !st.instances.contains_key(instance_id) {
        return notfound_instance(instance_id);
    }
    let contact = match st
        .contacts
        .get_mut(instance_id)
        .and_then(|m| m.get_mut(contact_id))
    {
        Some(c) => c,
        None => return not_found(format!("Contact {contact_id} not found")),
    };
    if let Some(name) = opt_val(body, "Name") {
        contact["Name"] = name;
    }
    if let Some(desc) = opt_val(body, "Description") {
        contact["Description"] = desc;
    }
    contact["LastUpdateTimestamp"] = json!(now_iso());
    ok(json!({}))
}

fn op_stop_contact(st: &mut ConnectState, instance_id: &str, contact_id: &str) -> Response {
    if !st.instances.contains_key(instance_id) {
        return notfound_instance(instance_id);
    }
    match st
        .contacts
        .get_mut(instance_id)
        .and_then(|m| m.remove(contact_id))
    {
        Some(_) => ok(json!({})),
        None => not_found(format!("Contact {contact_id} not found")),
    }
}

// ---------------------------------------------------------------------------
// Contact attributes
// ---------------------------------------------------------------------------

fn op_get_contact_attributes(
    st: &mut ConnectState,
    instance_id: &str,
    contact_id: &str,
) -> Response {
    if !st.instances.contains_key(instance_id) {
        return notfound_instance(instance_id);
    }
    let attrs = st
        .contact_attributes
        .get(instance_id)
        .and_then(|i| i.get(contact_id))
        .cloned()
        .unwrap_or_default();
    ok(json!({"Attributes": attrs}))
}

fn op_update_contact_attributes(st: &mut ConnectState, body: &Map<String, Value>) -> Response {
    let instance_id = match body_str(body, "InstanceId") {
        Ok(v) => v,
        Err(resp) => return resp,
    };
    let initial_contact_id = match body_str(body, "InitialContactId") {
        Ok(v) => v,
        Err(resp) => return resp,
    };
    if !st.instances.contains_key(&instance_id) {
        return notfound_instance(&instance_id);
    }
    if let Some(Value::Object(attrs)) = opt_val(body, "Attributes") {
        let entry = st
            .contact_attributes
            .entry(instance_id)
            .or_default()
            .entry(initial_contact_id)
            .or_default();
        for (k, v) in attrs {
            entry.insert(k, v);
        }
    }
    ok(json!({}))
}

// ---------------------------------------------------------------------------
// Contact evaluations
// ---------------------------------------------------------------------------

fn op_start_contact_evaluation(
    st: &mut ConnectState,
    instance_id: &str,
    body: &Map<String, Value>,
) -> Response {
    let instance_arn = match instance_arn_or_404(st, instance_id) {
        Some(a) => a,
        None => return notfound_instance(instance_id),
    };
    let contact_id = match body_str(body, "ContactId") {
        Ok(v) => v,
        Err(resp) => return resp,
    };
    let form_id = match body_str(body, "EvaluationFormId") {
        Ok(v) => v,
        Err(resp) => return resp,
    };
    let evaluation_id = uuid::Uuid::new_v4().to_string();
    let arn = format!("{instance_arn}/contact-evaluation/{evaluation_id}");
    let stamp = now_iso();
    let mut evaluation = Map::new();
    evaluation.insert("EvaluationId".into(), json!(evaluation_id.clone()));
    evaluation.insert("EvaluationArn".into(), json!(arn.clone()));
    evaluation.insert("ContactId".into(), json!(contact_id));
    evaluation.insert("EvaluationFormId".into(), json!(form_id));
    evaluation.insert(
        "Status".into(),
        json!(opt_string(body, "Status").unwrap_or_else(|| "DRAFT".into())),
    );
    evaluation.insert(
        "Answers".into(),
        opt_val(body, "Answers").unwrap_or_else(|| json!({})),
    );
    if let Some(notes) = opt_val(body, "Notes").filter(|v| !v.is_null()) {
        evaluation.insert("Notes".into(), notes);
    }
    evaluation.insert("CreateTimestamp".into(), json!(stamp.clone()));
    evaluation.insert("ModifiedTimestamp".into(), json!(stamp));
    st.contact_evaluations
        .entry(instance_id.to_string())
        .or_default()
        .insert(evaluation_id.clone(), Value::Object(evaluation));
    ok(json!({"EvaluationId": evaluation_id, "EvaluationArn": arn}))
}

fn op_describe_contact_evaluation(
    st: &mut ConnectState,
    instance_id: &str,
    evaluation_id: &str,
) -> Response {
    if !st.instances.contains_key(instance_id) {
        return notfound_instance(instance_id);
    }
    match st
        .contact_evaluations
        .get(instance_id)
        .and_then(|m| m.get(evaluation_id))
    {
        Some(e) => ok(json!({"ContactEvaluation": e.clone()})),
        None => not_found(format!("Contact evaluation {evaluation_id} not found")),
    }
}

fn op_update_contact_evaluation(
    st: &mut ConnectState,
    instance_id: &str,
    evaluation_id: &str,
    body: &Map<String, Value>,
) -> Response {
    if !st.instances.contains_key(instance_id) {
        return notfound_instance(instance_id);
    }
    let evaluation = match st
        .contact_evaluations
        .get_mut(instance_id)
        .and_then(|m| m.get_mut(evaluation_id))
    {
        Some(e) => e,
        None => return not_found(format!("Contact evaluation {evaluation_id} not found")),
    };
    if let Some(status) = opt_val(body, "Status") {
        evaluation["Status"] = status;
    }
    if let Some(answers) = opt_val(body, "Answers") {
        evaluation["Answers"] = answers;
    }
    if let Some(notes) = opt_val(body, "Notes") {
        evaluation["Notes"] = notes;
    }
    evaluation["ModifiedTimestamp"] = json!(now_iso());
    ok(json!({"ContactEvaluation": evaluation.clone()}))
}

fn op_delete_contact_evaluation(
    st: &mut ConnectState,
    instance_id: &str,
    evaluation_id: &str,
) -> Response {
    if !st.instances.contains_key(instance_id) {
        return notfound_instance(instance_id);
    }
    match st
        .contact_evaluations
        .get_mut(instance_id)
        .and_then(|m| m.remove(evaluation_id))
    {
        Some(_) => ok(json!({})),
        None => not_found(format!("Contact evaluation {evaluation_id} not found")),
    }
}

fn op_list_contact_references(st: &mut ConnectState, instance_id: &str) -> Response {
    if !st.instances.contains_key(instance_id) {
        return notfound_instance(instance_id);
    }
    ok(json!({"ReferenceSummaryList": []}))
}

fn op_list_flow_associations(st: &mut ConnectState, instance_id: &str) -> Response {
    if !st.instances.contains_key(instance_id) {
        return notfound_instance(instance_id);
    }
    ok(json!({"FlowAssociationSummaryList": []}))
}

// ---------------------------------------------------------------------------
// Hours of operations
// ---------------------------------------------------------------------------

fn op_create_hours_of_operation(
    st: &mut ConnectState,
    instance_id: &str,
    body: &Map<String, Value>,
) -> Response {
    let instance = match st.instances.get(instance_id) {
        Some(i) => i.clone(),
        None => return notfound_instance(instance_id),
    };
    let name = match body_str(body, "Name") {
        Ok(v) => v,
        Err(resp) => return resp,
    };
    let time_zone = match body_str(body, "TimeZone") {
        Ok(v) => v,
        Err(resp) => return resp,
    };
    let config = match opt_val(body, "Config") {
        Some(v) => v,
        None => return missing_body_param("Config"),
    };
    let hoo_id = uuid::Uuid::new_v4().to_string();
    let arn = format!("{}/operating-hours/{hoo_id}", str_field(&instance, "Arn"));
    let mut stored = Map::new();
    stored.insert("HoursOfOperationId".into(), json!(hoo_id.clone()));
    stored.insert("HoursOfOperationArn".into(), json!(arn.clone()));
    stored.insert("Name".into(), json!(name));
    stored.insert(
        "Description".into(),
        json!(opt_string(body, "Description").unwrap_or_default()),
    );
    stored.insert("TimeZone".into(), json!(time_zone));
    stored.insert("Config".into(), config);
    stored.insert(
        "Tags".into(),
        opt_val(body, "Tags").unwrap_or_else(|| json!({})),
    );
    stored.insert("LastModifiedTime".into(), json!(now_iso()));
    stored.insert("LastModifiedRegion".into(), json!(region_of(&instance)));
    st.hours_of_operations
        .entry(instance_id.to_string())
        .or_default()
        .insert(hoo_id.clone(), Value::Object(stored));
    tag_resource(st, &arn, opt_val(body, "Tags"));
    ok(json!({"HoursOfOperationId": hoo_id, "HoursOfOperationArn": arn}))
}

fn op_describe_hours_of_operation(
    st: &mut ConnectState,
    instance_id: &str,
    hoo_id: &str,
) -> Response {
    if !st.instances.contains_key(instance_id) {
        return notfound_instance(instance_id);
    }
    match st
        .hours_of_operations
        .get(instance_id)
        .and_then(|m| m.get(hoo_id))
    {
        Some(h) => ok(json!({"HoursOfOperation": h.clone()})),
        None => not_found(format!(
            "Hours of operation {hoo_id} not found in instance {instance_id}"
        )),
    }
}

fn op_update_hours_of_operation(
    st: &mut ConnectState,
    instance_id: &str,
    hoo_id: &str,
    body: &Map<String, Value>,
) -> Response {
    if !st.instances.contains_key(instance_id) {
        return notfound_instance(instance_id);
    }
    let hoo = match st
        .hours_of_operations
        .get_mut(instance_id)
        .and_then(|m| m.get_mut(hoo_id))
    {
        Some(h) => h,
        None => return not_found(format!("Hours of operation {hoo_id} not found")),
    };
    if let Some(name) = opt_val(body, "Name") {
        hoo["Name"] = name;
    }
    if let Some(desc) = opt_val(body, "Description") {
        hoo["Description"] = desc;
    }
    if let Some(tz) = opt_val(body, "TimeZone") {
        hoo["TimeZone"] = tz;
    }
    if let Some(config) = opt_val(body, "Config") {
        hoo["Config"] = config;
    }
    hoo["LastModifiedTime"] = json!(now_iso());
    ok(json!({}))
}

fn op_delete_hours_of_operation(
    st: &mut ConnectState,
    instance_id: &str,
    hoo_id: &str,
) -> Response {
    if !st.instances.contains_key(instance_id) {
        return notfound_instance(instance_id);
    }
    match st
        .hours_of_operations
        .get_mut(instance_id)
        .and_then(|m| m.remove(hoo_id))
    {
        Some(_) => ok(json!({})),
        None => not_found(format!("Hours of operation {hoo_id} not found")),
    }
}

fn op_list_hours_of_operations(st: &mut ConnectState, instance_id: &str) -> Response {
    if !st.instances.contains_key(instance_id) {
        return notfound_instance(instance_id);
    }
    let empty = HashMap::new();
    let hoos = st.hours_of_operations.get(instance_id).unwrap_or(&empty);
    let rows: Vec<Value> = hoos
        .values()
        .map(|h| {
            json!({
                "Id": h["HoursOfOperationId"],
                "Arn": h["HoursOfOperationArn"],
                "Name": h["Name"],
                "LastModifiedTime": h["LastModifiedTime"],
                "LastModifiedRegion": h["LastModifiedRegion"],
            })
        })
        .collect();
    ok(json!({"HoursOfOperationSummaryList": rows}))
}

// ---------------------------------------------------------------------------
// Hours of operation overrides (keyed instance:hours_of_operation_id)
// ---------------------------------------------------------------------------

fn op_create_hours_of_operation_override(
    st: &mut ConnectState,
    instance_id: &str,
    hoo_id: &str,
    body: &Map<String, Value>,
) -> Response {
    if !st.instances.contains_key(instance_id) {
        return notfound_instance(instance_id);
    }
    let name = match body_str(body, "Name") {
        Ok(v) => v,
        Err(resp) => return resp,
    };
    let override_id = uuid::Uuid::new_v4().to_string();
    let mut stored = Map::new();
    stored.insert(
        "HoursOfOperationOverrideId".into(),
        json!(override_id.clone()),
    );
    stored.insert("Name".into(), json!(name));
    stored.insert(
        "Description".into(),
        json!(opt_string(body, "Description").unwrap_or_default()),
    );
    stored.insert(
        "Config".into(),
        opt_val(body, "Config").unwrap_or_else(|| json!([])),
    );
    stored.insert(
        "EffectiveFrom".into(),
        json!(opt_string(body, "EffectiveFrom").unwrap_or_default()),
    );
    stored.insert(
        "EffectiveTill".into(),
        json!(opt_string(body, "EffectiveTill").unwrap_or_default()),
    );
    let key = format!("{instance_id}:{hoo_id}");
    st.hours_of_operation_overrides
        .entry(key)
        .or_default()
        .insert(override_id.clone(), Value::Object(stored));
    ok(json!({"HoursOfOperationOverrideId": override_id}))
}

fn op_describe_hours_of_operation_override(
    st: &mut ConnectState,
    instance_id: &str,
    hoo_id: &str,
    override_id: &str,
) -> Response {
    if !st.instances.contains_key(instance_id) {
        return notfound_instance(instance_id);
    }
    let key = format!("{instance_id}:{hoo_id}");
    match st
        .hours_of_operation_overrides
        .get(&key)
        .and_then(|m| m.get(override_id))
    {
        Some(o) => ok(json!({"HoursOfOperationOverride": o.clone()})),
        None => not_found(format!(
            "Hours of operation override {override_id} not found"
        )),
    }
}

fn op_update_hours_of_operation_override(
    st: &mut ConnectState,
    instance_id: &str,
    hoo_id: &str,
    override_id: &str,
    body: &Map<String, Value>,
) -> Response {
    if !st.instances.contains_key(instance_id) {
        return notfound_instance(instance_id);
    }
    let key = format!("{instance_id}:{hoo_id}");
    let override_row = match st
        .hours_of_operation_overrides
        .get_mut(&key)
        .and_then(|m| m.get_mut(override_id))
    {
        Some(o) => o,
        None => {
            return not_found(format!(
                "Hours of operation override {override_id} not found"
            ))
        }
    };
    if let Some(name) = opt_val(body, "Name") {
        override_row["Name"] = name;
    }
    if let Some(desc) = opt_val(body, "Description") {
        override_row["Description"] = desc;
    }
    if let Some(config) = opt_val(body, "Config") {
        override_row["Config"] = config;
    }
    if let Some(from) = opt_val(body, "EffectiveFrom") {
        override_row["EffectiveFrom"] = from;
    }
    if let Some(till) = opt_val(body, "EffectiveTill") {
        override_row["EffectiveTill"] = till;
    }
    ok(json!({}))
}

fn op_delete_hours_of_operation_override(
    st: &mut ConnectState,
    instance_id: &str,
    hoo_id: &str,
    override_id: &str,
) -> Response {
    if !st.instances.contains_key(instance_id) {
        return notfound_instance(instance_id);
    }
    let key = format!("{instance_id}:{hoo_id}");
    match st
        .hours_of_operation_overrides
        .get_mut(&key)
        .and_then(|m| m.remove(override_id))
    {
        Some(_) => ok(json!({})),
        None => not_found(format!(
            "Hours of operation override {override_id} not found"
        )),
    }
}

fn op_list_hours_of_operation_overrides(
    st: &mut ConnectState,
    instance_id: &str,
    hoo_id: &str,
) -> Response {
    if !st.instances.contains_key(instance_id) {
        return notfound_instance(instance_id);
    }
    let key = format!("{instance_id}:{hoo_id}");
    let rows: Vec<Value> = st
        .hours_of_operation_overrides
        .get(&key)
        .map(|m| m.values().cloned().collect())
        .unwrap_or_default();
    ok(json!({"HoursOfOperationOverrideList": rows}))
}

// ---------------------------------------------------------------------------
// Queues
// ---------------------------------------------------------------------------

fn op_create_queue(
    st: &mut ConnectState,
    instance_id: &str,
    body: &Map<String, Value>,
) -> Response {
    let instance = match st.instances.get(instance_id) {
        Some(i) => i.clone(),
        None => return notfound_instance(instance_id),
    };
    let name = match body_str(body, "Name") {
        Ok(v) => v,
        Err(resp) => return resp,
    };
    let hours_of_operation_id = match body_str(body, "HoursOfOperationId") {
        Ok(v) => v,
        Err(resp) => return resp,
    };
    let queue_id = uuid::Uuid::new_v4().to_string();
    let arn = format!("{}/queue/{queue_id}", str_field(&instance, "Arn"));
    let mut stored = Map::new();
    stored.insert("Name".into(), json!(name));
    stored.insert("QueueArn".into(), json!(arn.clone()));
    stored.insert("QueueId".into(), json!(queue_id.clone()));
    stored.insert(
        "Description".into(),
        json!(opt_string(body, "Description").unwrap_or_default()),
    );
    stored.insert(
        "OutboundCallerConfig".into(),
        opt_val(body, "OutboundCallerConfig").unwrap_or_else(|| json!({})),
    );
    stored.insert("HoursOfOperationId".into(), json!(hours_of_operation_id));
    // moto: max_contacts or 0
    let max_contacts = body.get("MaxContacts").and_then(Value::as_i64).unwrap_or(0);
    stored.insert("MaxContacts".into(), json!(max_contacts));
    stored.insert("Status".into(), json!("ENABLED"));
    stored.insert(
        "Tags".into(),
        opt_val(body, "Tags").unwrap_or_else(|| json!({})),
    );
    stored.insert("LastModifiedTime".into(), json!(now_iso()));
    stored.insert("LastModifiedRegion".into(), json!(region_of(&instance)));
    st.queues
        .entry(instance_id.to_string())
        .or_default()
        .insert(queue_id.clone(), Value::Object(stored));
    tag_resource(st, &arn, opt_val(body, "Tags"));
    ok(json!({"QueueId": queue_id, "QueueArn": arn}))
}

fn op_describe_queue(st: &mut ConnectState, instance_id: &str, queue_id: &str) -> Response {
    if !st.instances.contains_key(instance_id) {
        return notfound_instance(instance_id);
    }
    match st.queues.get(instance_id).and_then(|m| m.get(queue_id)) {
        Some(q) => ok(json!({"Queue": q.clone()})),
        None => not_found(format!(
            "Queue {queue_id} not found in instance {instance_id}"
        )),
    }
}

fn queue_at<'a>(
    st: &'a mut ConnectState,
    instance_id: &str,
    queue_id: &str,
) -> Result<&'a mut Value, Response> {
    let present = st
        .queues
        .get(instance_id)
        .is_some_and(|m| m.contains_key(queue_id));
    if !present {
        return Err(not_found(format!("Queue {queue_id} not found")));
    }
    let queue = st
        .queues
        .get_mut(instance_id)
        .and_then(|m| m.get_mut(queue_id))
        .unwrap();
    Ok(queue)
}

fn op_update_queue_name(
    st: &mut ConnectState,
    instance_id: &str,
    queue_id: &str,
    body: &Map<String, Value>,
) -> Response {
    if !st.instances.contains_key(instance_id) {
        return notfound_instance(instance_id);
    }
    let queue = match queue_at(st, instance_id, queue_id) {
        Ok(q) => q,
        Err(resp) => return resp,
    };
    if let Some(name) = opt_val(body, "Name") {
        queue["Name"] = name;
    }
    if let Some(desc) = opt_val(body, "Description") {
        queue["Description"] = desc;
    }
    queue["LastModifiedTime"] = json!(now_iso());
    ok(json!({}))
}

fn op_update_queue_status(
    st: &mut ConnectState,
    instance_id: &str,
    queue_id: &str,
    body: &Map<String, Value>,
) -> Response {
    if !st.instances.contains_key(instance_id) {
        return notfound_instance(instance_id);
    }
    let status = match body_str(body, "Status") {
        Ok(v) => v,
        Err(resp) => return resp,
    };
    let queue = match queue_at(st, instance_id, queue_id) {
        Ok(q) => q,
        Err(resp) => return resp,
    };
    queue["Status"] = json!(status);
    queue["LastModifiedTime"] = json!(now_iso());
    ok(json!({}))
}

fn op_update_queue_max_contacts(
    st: &mut ConnectState,
    instance_id: &str,
    queue_id: &str,
    body: &Map<String, Value>,
) -> Response {
    if !st.instances.contains_key(instance_id) {
        return notfound_instance(instance_id);
    }
    let max_contacts = match body.get("MaxContacts") {
        Some(Value::Number(n)) => n.as_i64().unwrap_or(0),
        Some(other) => other
            .as_str()
            .unwrap_or_default()
            .parse::<i64>()
            .unwrap_or(0),
        None => return missing_body_param("MaxContacts"),
    };
    let queue = match queue_at(st, instance_id, queue_id) {
        Ok(q) => q,
        Err(resp) => return resp,
    };
    queue["MaxContacts"] = json!(max_contacts);
    queue["LastModifiedTime"] = json!(now_iso());
    ok(json!({}))
}

fn op_update_queue_hours_of_operation(
    st: &mut ConnectState,
    instance_id: &str,
    queue_id: &str,
    body: &Map<String, Value>,
) -> Response {
    if !st.instances.contains_key(instance_id) {
        return notfound_instance(instance_id);
    }
    let hoo_id = match body_str(body, "HoursOfOperationId") {
        Ok(v) => v,
        Err(resp) => return resp,
    };
    let queue = match queue_at(st, instance_id, queue_id) {
        Ok(q) => q,
        Err(resp) => return resp,
    };
    queue["HoursOfOperationId"] = json!(hoo_id);
    queue["LastModifiedTime"] = json!(now_iso());
    ok(json!({}))
}

fn op_update_queue_outbound_caller_config(
    st: &mut ConnectState,
    instance_id: &str,
    queue_id: &str,
    body: &Map<String, Value>,
) -> Response {
    if !st.instances.contains_key(instance_id) {
        return notfound_instance(instance_id);
    }
    let config = match opt_val(body, "OutboundCallerConfig") {
        Some(v) => v,
        None => return missing_body_param("OutboundCallerConfig"),
    };
    let queue = match queue_at(st, instance_id, queue_id) {
        Ok(q) => q,
        Err(resp) => return resp,
    };
    queue["OutboundCallerConfig"] = config;
    queue["LastModifiedTime"] = json!(now_iso());
    ok(json!({}))
}

fn op_delete_queue(st: &mut ConnectState, instance_id: &str, queue_id: &str) -> Response {
    if !st.instances.contains_key(instance_id) {
        return notfound_instance(instance_id);
    }
    match st
        .queues
        .get_mut(instance_id)
        .and_then(|m| m.remove(queue_id))
    {
        Some(_) => ok(json!({})),
        None => not_found(format!("Queue {queue_id} not found")),
    }
}

fn op_list_queues(st: &mut ConnectState, instance_id: &str) -> Response {
    if !st.instances.contains_key(instance_id) {
        return notfound_instance(instance_id);
    }
    let empty = HashMap::new();
    let queues = st.queues.get(instance_id).unwrap_or(&empty);
    let rows: Vec<Value> = queues
        .values()
        .map(|q| {
            json!({
                "Id": q["QueueId"],
                "Arn": q["QueueArn"],
                "Name": q["Name"],
                "QueueType": "STANDARD",
                "LastModifiedTime": q["LastModifiedTime"],
                "LastModifiedRegion": q["LastModifiedRegion"],
            })
        })
        .collect();
    ok(json!({"QueueSummaryList": rows}))
}

fn op_list_queue_quick_connects(
    st: &mut ConnectState,
    instance_id: &str,
    queue_id: &str,
) -> Response {
    if !st.instances.contains_key(instance_id) {
        return notfound_instance(instance_id);
    }
    let assoc_ids = st
        .queue_quick_connect_associations
        .get(instance_id)
        .and_then(|m| m.get(queue_id))
        .cloned()
        .unwrap_or_default();
    let empty = HashMap::new();
    let qcs = st.quick_connects.get(instance_id).unwrap_or(&empty);
    // moto: no explicit associations -> every quick connect
    let targets: Vec<&Value> = if assoc_ids.is_empty() {
        qcs.values().collect()
    } else {
        assoc_ids.iter().filter_map(|id| qcs.get(id)).collect()
    };
    let rows: Vec<Value> = targets
        .iter()
        .map(|qc| {
            json!({
                "QuickConnectId": qc["QuickConnectId"],
                "Name": qc["Name"],
                "QuickConnectType": qc["QuickConnectConfig"]["QuickConnectType"],
            })
        })
        .collect();
    ok(json!({"QuickConnectSummaryList": rows}))
}

fn op_associate_queue_quick_connects(
    st: &mut ConnectState,
    instance_id: &str,
    queue_id: &str,
    body: &Map<String, Value>,
) -> Response {
    if !st.instances.contains_key(instance_id) {
        return notfound_instance(instance_id);
    }
    let ids = match opt_val(body, "QuickConnectIds") {
        Some(Value::Array(a)) => a
            .iter()
            .filter_map(Value::as_str)
            .map(str::to_string)
            .collect::<Vec<_>>(),
        _ => return missing_body_param("QuickConnectIds"),
    };
    let entry = st
        .queue_quick_connect_associations
        .entry(instance_id.to_string())
        .or_default()
        .entry(queue_id.to_string())
        .or_default();
    for id in ids {
        if !entry.contains(&id) {
            entry.push(id);
        }
    }
    ok(json!({}))
}

fn op_disassociate_queue_quick_connects(
    st: &mut ConnectState,
    instance_id: &str,
    queue_id: &str,
    body: &Map<String, Value>,
) -> Response {
    if !st.instances.contains_key(instance_id) {
        return notfound_instance(instance_id);
    }
    if let Some(Value::Array(ids)) = opt_val(body, "QuickConnectIds") {
        if let Some(entry) = st
            .queue_quick_connect_associations
            .get_mut(instance_id)
            .and_then(|m| m.get_mut(queue_id))
        {
            for id in ids {
                if let Some(id) = id.as_str() {
                    entry.retain(|x| x != id);
                }
            }
        }
    }
    ok(json!({}))
}

// ---------------------------------------------------------------------------
// Quick connects
// ---------------------------------------------------------------------------

fn op_create_quick_connect(
    st: &mut ConnectState,
    instance_id: &str,
    body: &Map<String, Value>,
) -> Response {
    let instance = match st.instances.get(instance_id) {
        Some(i) => i.clone(),
        None => return notfound_instance(instance_id),
    };
    let name = match body_str(body, "Name") {
        Ok(v) => v,
        Err(resp) => return resp,
    };
    let config = match opt_val(body, "QuickConnectConfig") {
        Some(v) => v,
        None => return missing_body_param("QuickConnectConfig"),
    };
    let qc_id = uuid::Uuid::new_v4().to_string();
    let arn = format!(
        "{}/transfer-destination/{qc_id}",
        str_field(&instance, "Arn")
    );
    let mut stored = Map::new();
    stored.insert("QuickConnectARN".into(), json!(arn.clone()));
    stored.insert("QuickConnectId".into(), json!(qc_id.clone()));
    stored.insert("Name".into(), json!(name));
    stored.insert(
        "Description".into(),
        json!(opt_string(body, "Description").unwrap_or_default()),
    );
    stored.insert("QuickConnectConfig".into(), config);
    stored.insert(
        "Tags".into(),
        opt_val(body, "Tags").unwrap_or_else(|| json!({})),
    );
    stored.insert("LastModifiedTime".into(), json!(now_iso()));
    stored.insert("LastModifiedRegion".into(), json!(region_of(&instance)));
    st.quick_connects
        .entry(instance_id.to_string())
        .or_default()
        .insert(qc_id.clone(), Value::Object(stored.clone()));
    tag_resource(st, &arn, opt_val(body, "Tags"));
    ok(json!({"QuickConnectARN": arn, "QuickConnectId": qc_id}))
}

fn op_describe_quick_connect(st: &mut ConnectState, instance_id: &str, qc_id: &str) -> Response {
    if !st.instances.contains_key(instance_id) {
        return notfound_instance(instance_id);
    }
    match st
        .quick_connects
        .get(instance_id)
        .and_then(|m| m.get(qc_id))
    {
        Some(qc) => ok(json!({"QuickConnect": qc.clone()})),
        None => not_found(format!(
            "Quick connect {qc_id} not found in instance {instance_id}"
        )),
    }
}

fn op_update_quick_connect_name(
    st: &mut ConnectState,
    instance_id: &str,
    qc_id: &str,
    body: &Map<String, Value>,
) -> Response {
    if !st.instances.contains_key(instance_id) {
        return notfound_instance(instance_id);
    }
    let qc = match st
        .quick_connects
        .get_mut(instance_id)
        .and_then(|m| m.get_mut(qc_id))
    {
        Some(qc) => qc,
        None => return not_found(format!("Quick connect {qc_id} not found")),
    };
    if let Some(name) = opt_val(body, "Name") {
        qc["Name"] = name;
    }
    if let Some(desc) = opt_val(body, "Description") {
        qc["Description"] = desc;
    }
    qc["LastModifiedTime"] = json!(now_iso());
    ok(json!({}))
}

fn op_update_quick_connect_config(
    st: &mut ConnectState,
    instance_id: &str,
    qc_id: &str,
    body: &Map<String, Value>,
) -> Response {
    if !st.instances.contains_key(instance_id) {
        return notfound_instance(instance_id);
    }
    let config = match opt_val(body, "QuickConnectConfig") {
        Some(v) => v,
        None => return missing_body_param("QuickConnectConfig"),
    };
    let qc = match st
        .quick_connects
        .get_mut(instance_id)
        .and_then(|m| m.get_mut(qc_id))
    {
        Some(qc) => qc,
        None => return not_found(format!("Quick connect {qc_id} not found")),
    };
    qc["QuickConnectConfig"] = config;
    qc["LastModifiedTime"] = json!(now_iso());
    ok(json!({}))
}

fn op_delete_quick_connect(st: &mut ConnectState, instance_id: &str, qc_id: &str) -> Response {
    if !st.instances.contains_key(instance_id) {
        return notfound_instance(instance_id);
    }
    match st
        .quick_connects
        .get_mut(instance_id)
        .and_then(|m| m.remove(qc_id))
    {
        Some(_) => ok(json!({})),
        None => not_found(format!("Quick connect {qc_id} not found")),
    }
}

fn op_list_quick_connects(st: &mut ConnectState, instance_id: &str) -> Response {
    if !st.instances.contains_key(instance_id) {
        return notfound_instance(instance_id);
    }
    let empty = HashMap::new();
    let qcs = st.quick_connects.get(instance_id).unwrap_or(&empty);
    let rows: Vec<Value> = qcs
        .values()
        .map(|qc| {
            json!({
                "QuickConnectId": qc["QuickConnectId"],
                "QuickConnectARN": qc["QuickConnectARN"],
                "Name": qc["Name"],
                "QuickConnectType": qc["QuickConnectConfig"]["QuickConnectType"],
                "LastModifiedTime": qc["LastModifiedTime"],
                "LastModifiedRegion": qc["LastModifiedRegion"],
            })
        })
        .collect();
    ok(json!({"QuickConnectSummaryList": rows}))
}

// ---------------------------------------------------------------------------
// Routing profiles
// ---------------------------------------------------------------------------

fn op_create_routing_profile(
    st: &mut ConnectState,
    instance_id: &str,
    body: &Map<String, Value>,
) -> Response {
    let instance = match st.instances.get(instance_id) {
        Some(i) => i.clone(),
        None => return notfound_instance(instance_id),
    };
    let name = match body_str(body, "Name") {
        Ok(v) => v,
        Err(resp) => return resp,
    };
    let description = match body_str(body, "Description") {
        Ok(v) => v,
        Err(resp) => return resp,
    };
    let default_outbound_queue_id = match body_str(body, "DefaultOutboundQueueId") {
        Ok(v) => v,
        Err(resp) => return resp,
    };
    let media_concurrencies = match opt_val(body, "MediaConcurrencies") {
        Some(v) => v,
        None => return missing_body_param("MediaConcurrencies"),
    };
    let rp_id = uuid::Uuid::new_v4().to_string();
    let arn = format!("{}/routing-profile/{rp_id}", str_field(&instance, "Arn"));
    let mut stored = Map::new();
    stored.insert("InstanceId".into(), json!(instance_id));
    stored.insert("Name".into(), json!(name));
    stored.insert("RoutingProfileArn".into(), json!(arn.clone()));
    stored.insert("RoutingProfileId".into(), json!(rp_id.clone()));
    stored.insert("Description".into(), json!(description));
    stored.insert("MediaConcurrencies".into(), media_concurrencies);
    stored.insert(
        "DefaultOutboundQueueId".into(),
        json!(default_outbound_queue_id),
    );
    stored.insert(
        "Tags".into(),
        opt_val(body, "Tags").unwrap_or_else(|| json!({})),
    );
    stored.insert("NumberOfAssociatedQueues".into(), json!(0));
    stored.insert("LastModifiedTime".into(), json!(now_iso()));
    stored.insert("LastModifiedRegion".into(), json!(region_of(&instance)));
    st.routing_profiles
        .entry(instance_id.to_string())
        .or_default()
        .insert(rp_id.clone(), Value::Object(stored));
    tag_resource(st, &arn, opt_val(body, "Tags"));
    ok(json!({"RoutingProfileArn": arn, "RoutingProfileId": rp_id}))
}

fn op_describe_routing_profile(st: &mut ConnectState, instance_id: &str, rp_id: &str) -> Response {
    if !st.instances.contains_key(instance_id) {
        return notfound_instance(instance_id);
    }
    match st
        .routing_profiles
        .get(instance_id)
        .and_then(|m| m.get(rp_id))
    {
        Some(rp) => ok(json!({"RoutingProfile": rp.clone()})),
        None => not_found(format!(
            "Routing profile {rp_id} not found in instance {instance_id}"
        )),
    }
}

fn routing_profile_at<'a>(
    st: &'a mut ConnectState,
    instance_id: &str,
    rp_id: &str,
) -> Result<&'a mut Value, Response> {
    let present = st
        .routing_profiles
        .get(instance_id)
        .is_some_and(|m| m.contains_key(rp_id));
    if !present {
        return Err(not_found(format!("Routing profile {rp_id} not found")));
    }
    let rp = st
        .routing_profiles
        .get_mut(instance_id)
        .and_then(|m| m.get_mut(rp_id))
        .unwrap();
    Ok(rp)
}

fn op_update_routing_profile_name(
    st: &mut ConnectState,
    instance_id: &str,
    rp_id: &str,
    body: &Map<String, Value>,
) -> Response {
    if !st.instances.contains_key(instance_id) {
        return notfound_instance(instance_id);
    }
    let rp = match routing_profile_at(st, instance_id, rp_id) {
        Ok(rp) => rp,
        Err(resp) => return resp,
    };
    if let Some(name) = opt_val(body, "Name") {
        rp["Name"] = name;
    }
    if let Some(desc) = opt_val(body, "Description") {
        rp["Description"] = desc;
    }
    rp["LastModifiedTime"] = json!(now_iso());
    ok(json!({}))
}

fn op_update_routing_profile_concurrency(
    st: &mut ConnectState,
    instance_id: &str,
    rp_id: &str,
    body: &Map<String, Value>,
) -> Response {
    if !st.instances.contains_key(instance_id) {
        return notfound_instance(instance_id);
    }
    let concurrencies = match opt_val(body, "MediaConcurrencies") {
        Some(v) => v,
        None => return missing_body_param("MediaConcurrencies"),
    };
    let rp = match routing_profile_at(st, instance_id, rp_id) {
        Ok(rp) => rp,
        Err(resp) => return resp,
    };
    rp["MediaConcurrencies"] = concurrencies;
    rp["LastModifiedTime"] = json!(now_iso());
    ok(json!({}))
}

fn op_update_routing_profile_default_outbound_queue(
    st: &mut ConnectState,
    instance_id: &str,
    rp_id: &str,
    body: &Map<String, Value>,
) -> Response {
    if !st.instances.contains_key(instance_id) {
        return notfound_instance(instance_id);
    }
    let queue_id = match body_str(body, "DefaultOutboundQueueId") {
        Ok(v) => v,
        Err(resp) => return resp,
    };
    let rp = match routing_profile_at(st, instance_id, rp_id) {
        Ok(rp) => rp,
        Err(resp) => return resp,
    };
    rp["DefaultOutboundQueueId"] = json!(queue_id);
    rp["LastModifiedTime"] = json!(now_iso());
    ok(json!({}))
}

fn op_delete_routing_profile(st: &mut ConnectState, instance_id: &str, rp_id: &str) -> Response {
    if !st.instances.contains_key(instance_id) {
        return notfound_instance(instance_id);
    }
    match st
        .routing_profiles
        .get_mut(instance_id)
        .and_then(|m| m.remove(rp_id))
    {
        Some(_) => ok(json!({})),
        None => not_found(format!("Routing profile {rp_id} not found")),
    }
}

fn op_list_routing_profiles(st: &mut ConnectState, instance_id: &str) -> Response {
    if !st.instances.contains_key(instance_id) {
        return notfound_instance(instance_id);
    }
    let empty = HashMap::new();
    let rps = st.routing_profiles.get(instance_id).unwrap_or(&empty);
    let rows: Vec<Value> = rps
        .values()
        .map(|rp| {
            json!({
                "Id": rp["RoutingProfileId"],
                "Arn": rp["RoutingProfileArn"],
                "Name": rp["Name"],
                "LastModifiedTime": rp["LastModifiedTime"],
                "LastModifiedRegion": rp["LastModifiedRegion"],
            })
        })
        .collect();
    ok(json!({"RoutingProfileSummaryList": rows}))
}

fn op_list_routing_profile_queues(
    st: &mut ConnectState,
    instance_id: &str,
    rp_id: &str,
) -> Response {
    if !st.instances.contains_key(instance_id) {
        return notfound_instance(instance_id);
    }
    let rows: Vec<Value> = st
        .routing_profile_queue_associations
        .get(instance_id)
        .and_then(|m| m.get(rp_id))
        .cloned()
        .unwrap_or_default();
    ok(json!({"RoutingProfileQueueConfigSummaryList": rows}))
}

fn op_associate_routing_profile_queues(
    st: &mut ConnectState,
    instance_id: &str,
    rp_id: &str,
    body: &Map<String, Value>,
) -> Response {
    if !st.instances.contains_key(instance_id) {
        return notfound_instance(instance_id);
    }
    // moto checks the routing profile first
    if routing_profile_at(st, instance_id, rp_id).is_err() {
        return not_found(format!("Routing profile {rp_id} not found"));
    }
    let configs = match opt_val(body, "QueueConfigs") {
        Some(Value::Array(a)) => a.clone(),
        _ => return missing_body_param("QueueConfigs"),
    };
    let entry = st
        .routing_profile_queue_associations
        .entry(instance_id.to_string())
        .or_default()
        .entry(rp_id.to_string())
        .or_default();
    entry.extend(configs);
    let count = entry.len();
    drop(entry);
    if let Ok(rp) = routing_profile_at(st, instance_id, rp_id) {
        rp["NumberOfAssociatedQueues"] = json!(count);
    }
    ok(json!({}))
}

fn op_disassociate_routing_profile_queues(
    st: &mut ConnectState,
    instance_id: &str,
    rp_id: &str,
    body: &Map<String, Value>,
) -> Response {
    if !st.instances.contains_key(instance_id) {
        return notfound_instance(instance_id);
    }
    if routing_profile_at(st, instance_id, rp_id).is_err() {
        return not_found(format!("Routing profile {rp_id} not found"));
    }
    let references = match opt_val(body, "QueueReferences") {
        Some(v) => v,
        None => return missing_body_param("QueueReferences"),
    };
    let remove_ids: Vec<String> = references
        .as_array()
        .map(|a| {
            a.iter()
                .filter_map(|r| r.get("QueueId"))
                .filter_map(Value::as_str)
                .map(str::to_string)
                .collect()
        })
        .unwrap_or_default();
    let entry = st
        .routing_profile_queue_associations
        .entry(instance_id.to_string())
        .or_default()
        .entry(rp_id.to_string())
        .or_default();
    entry.retain(|cfg| {
        let qid = cfg
            .get("QueueReference")
            .and_then(|r| r.get("QueueId"))
            .and_then(Value::as_str)
            .unwrap_or_default();
        !remove_ids.iter().any(|id| id == qid)
    });
    let count = entry.len();
    drop(entry);
    if let Ok(rp) = routing_profile_at(st, instance_id, rp_id) {
        rp["NumberOfAssociatedQueues"] = json!(count);
    }
    ok(json!({}))
}

fn op_update_routing_profile_queues(
    st: &mut ConnectState,
    instance_id: &str,
    rp_id: &str,
    body: &Map<String, Value>,
) -> Response {
    if !st.instances.contains_key(instance_id) {
        return notfound_instance(instance_id);
    }
    if routing_profile_at(st, instance_id, rp_id).is_err() {
        return not_found(format!("Routing profile {rp_id} not found"));
    }
    let configs = match opt_val(body, "QueueConfigs") {
        Some(Value::Array(a)) => a.clone(),
        _ => return missing_body_param("QueueConfigs"),
    };
    st.routing_profile_queue_associations
        .entry(instance_id.to_string())
        .or_default()
        .insert(rp_id.to_string(), configs.clone());
    if let Ok(rp) = routing_profile_at(st, instance_id, rp_id) {
        rp["NumberOfAssociatedQueues"] = json!(configs.len());
    }
    ok(json!({}))
}
// ---------------------------------------------------------------------------
// Unit tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    const ACCOUNT: u64 = 123456789012;
    const REGION: &str = "us-east-1";

    fn request(method: &str, path: &str, body: Value, query: &str) -> Response {
        let req = Request {
            service: "connect".to_string(),
            operation: String::new(),
            method: method.to_string(),
            path: path.to_string(),
            query_string: query.to_string(),
            headers: HashMap::new(),
            body: serde_json::to_vec(&body).unwrap_or_default().into(),
            account: ACCOUNT,
            region: REGION.to_string(),
        };
        dispatch(&req)
    }

    fn body_of(resp: &Response) -> Value {
        serde_json::from_slice(&resp.body).unwrap_or(Value::Null)
    }

    fn status_of(resp: &Response) -> u16 {
        resp.status
    }

    fn make_instance(extra: Value) -> (String, String) {
        let mut body = json!({"IdentityManagementType": "CONNECT_MANAGED"});
        if let (Some(dst), Some(src)) = (body.as_object_mut(), extra.as_object()) {
            for (k, v) in src {
                dst.insert(k.clone(), v.clone());
            }
        }
        let resp = request("PUT", "/instance", body, "");
        assert_eq!(status_of(&resp), 200, "create failed: {:?}", resp.status);
        let out = body_of(&resp);
        (
            out["Id"].as_str().unwrap().to_string(),
            out["Arn"].as_str().unwrap().to_string(),
        )
    }

    // -- Instances -----------------------------------------------------------

    #[test]
    fn create_describe_delete_instance_cycle() {
        let (id, arn) = make_instance(json!({}));
        assert!(id.len() == 36);
        assert!(arn.starts_with(&format!("arn:aws:connect:us-east-1:{ACCOUNT}:instance/")));
        assert!(arn.ends_with(&id));

        let desc = request("GET", &format!("/instance/{id}"), json!({}), "");
        let inst = &body_of(&desc)["Instance"];
        assert_eq!(inst["Id"], id.as_str());
        assert_eq!(inst["Arn"], arn.as_str());
        assert_eq!(inst["IdentityManagementType"], "CONNECT_MANAGED");

        let deleted = request("DELETE", &format!("/instance/{id}"), json!({}), "");
        assert_eq!(status_of(&deleted), 200);
        let after = request("GET", &format!("/instance/{id}"), json!({}), "");
        assert_eq!(status_of(&after), 404);
        assert_eq!(body_of(&after)["__type"], "ResourceNotFoundException");
    }

    #[test]
    fn list_instances_paginates_by_created_time() {
        let (id1, _) = make_instance(json!({}));
        let (id2, _) = make_instance(json!({}));
        assert_ne!(id1, id2);

        let page = request("GET", "/instance", json!({}), "maxResults=1");
        let out = body_of(&page);
        assert_eq!(out["InstanceSummaryList"].as_array().unwrap().len(), 1);
        let token = out["NextToken"].as_str().unwrap().to_string();
        assert!(!token.is_empty());

        let page2 = request(
            "GET",
            "/instance",
            json!({}),
            &format!("maxResults=1&nextToken={token}"),
        );
        let out2 = body_of(&page2);
        let second = &out2["InstanceSummaryList"].as_array().unwrap()[0];
        assert_ne!(
            second["Id"].as_str(),
            out["InstanceSummaryList"][0]["Id"].as_str()
        );

        // whole set within one page
        let all = request("GET", "/instance", json!({}), "maxResults=100");
        let val_listed = body_of(&all);
        let listed = val_listed["InstanceSummaryList"].as_array().unwrap();
        assert!(listed.iter().any(|s| s["Id"] == id1.as_str()));
        assert!(listed.iter().any(|s| s["Id"] == id2.as_str()));
    }

    #[test]
    fn describe_missing_instance_404() {
        let resp = request(
            "GET",
            "/instance/00000000-0000-0000-0000-000000000000",
            json!({}),
            "",
        );
        assert_eq!(status_of(&resp), 404);
        assert_eq!(body_of(&resp)["__type"], "ResourceNotFoundException");
        assert_eq!(
            body_of(&resp)["message"],
            "Instance with id 00000000-0000-0000-0000-000000000000 not found"
        );
    }

    #[test]
    fn create_requires_identity_management_type() {
        let resp = request("PUT", "/instance", json!({"InboundCallsEnabled": true}), "");
        // moto: responses.py indexes the member directly -> KeyError -> 500
        assert_eq!(status_of(&resp), 500);
        assert_eq!(body_of(&resp)["__type"], "InternalError");
        assert_eq!(body_of(&resp)["message"], "'IdentityManagementType'");

        let empty = request(
            "PUT",
            "/instance",
            json!({"IdentityManagementType": ""}),
            "",
        );
        assert_eq!(status_of(&empty), 400);
        assert_eq!(body_of(&empty)["__type"], "InvalidParameterException");
    }

    #[test]
    fn instance_attributes_lifecycle() {
        let (id, _) = make_instance(json!({}));
        let desc = request(
            "GET",
            &format!("/instance/{id}/attribute/CONTACTFLOW_LOGS"),
            json!({}),
            "",
        );
        let attr = &body_of(&desc)["Attribute"];
        assert_eq!(attr["AttributeType"], "CONTACTFLOW_LOGS");
        assert_eq!(attr["Value"], "false");

        let updated = request(
            "POST",
            &format!("/instance/{id}/attribute/CONTACTFLOW_LOGS"),
            json!({"Value": "true"}),
            "",
        );
        assert_eq!(status_of(&updated), 200);

        let listed = request("GET", &format!("/instance/{id}/attributes"), json!({}), "");
        let val_attrs = body_of(&listed);
        let attrs = val_attrs["Attributes"].as_array().unwrap();
        assert!(attrs
            .iter()
            .any(|a| a["AttributeType"] == "CONTACTFLOW_LOGS" && a["Value"] == "true"));

        let missing = request(
            "GET",
            &format!("/instance/{id}/attribute/NOT_A_TYPE"),
            json!({}),
            "",
        );
        assert_eq!(status_of(&missing), 404);
    }

    // -- Instance associations ------------------------------------------------

    #[test]
    fn approved_origins_roundtrip() {
        let (id, _) = make_instance(json!({}));
        let put = request(
            "PUT",
            &format!("/instance/{id}/approved-origin"),
            json!({"Origin": "https://example.com"}),
            "",
        );
        assert_eq!(status_of(&put), 200);

        let list = request(
            "GET",
            &format!("/instance/{id}/approved-origins"),
            json!({}),
            "",
        );
        let val_origins = body_of(&list);
        let origins = val_origins["Origins"].as_array().unwrap();
        assert!(origins.iter().any(|o| o == "https://example.com"));

        let del = request(
            "DELETE",
            &format!("/instance/{id}/approved-origin"),
            json!({}),
            "origin=https%3A%2F%2Fexample.com",
        );
        assert_eq!(status_of(&del), 200);
        let list = request(
            "GET",
            &format!("/instance/{id}/approved-origins"),
            json!({}),
            "",
        );
        assert!(body_of(&list)["Origins"].as_array().unwrap().is_empty());
    }

    #[test]
    fn lambda_function_association_roundtrip() {
        let (id, _) = make_instance(json!({}));
        let put = request(
            "PUT",
            &format!("/instance/{id}/lambda-function"),
            json!({"FunctionArn": "arn:aws:lambda:us-east-1:123456789012:function:f"}),
            "",
        );
        assert_eq!(status_of(&put), 200);
        let list = request(
            "GET",
            &format!("/instance/{id}/lambda-functions"),
            json!({}),
            "",
        );
        let val_funcs = body_of(&list);
        let funcs = val_funcs["LambdaFunctions"].as_array().unwrap();
        assert_eq!(funcs[0], "arn:aws:lambda:us-east-1:123456789012:function:f");

        let del = request(
            "DELETE",
            &format!("/instance/{id}/lambda-function"),
            json!({}),
            "functionArn=arn%3Aaws%3Alambda%3Aus-east-1%3A123456789012%3Afunction%3Af",
        );
        assert_eq!(status_of(&del), 200);
        let list = request(
            "GET",
            &format!("/instance/{id}/lambda-functions"),
            json!({}),
            "",
        );
        assert!(body_of(&list)["LambdaFunctions"]
            .as_array()
            .unwrap()
            .is_empty());
    }

    #[test]
    fn security_key_roundtrip() {
        let (id, _) = make_instance(json!({}));
        let put = request(
            "PUT",
            &format!("/instance/{id}/security-key"),
            json!({"Key": "k-1"}),
            "",
        );
        let assoc = body_of(&put)["AssociationId"].as_str().unwrap().to_string();
        assert_eq!(assoc.len(), 36);

        let list = request(
            "GET",
            &format!("/instance/{id}/security-keys"),
            json!({}),
            "",
        );
        let val_keys = body_of(&list);
        let keys = val_keys["SecurityKeys"].as_array().unwrap();
        assert_eq!(keys[0]["Key"], "k-1");

        let del = request(
            "DELETE",
            &format!("/instance/{id}/security-key/{assoc}"),
            json!({}),
            "",
        );
        assert_eq!(status_of(&del), 200);
        let list = request(
            "GET",
            &format!("/instance/{id}/security-keys"),
            json!({}),
            "",
        );
        assert!(body_of(&list)["SecurityKeys"]
            .as_array()
            .unwrap()
            .is_empty());
    }

    #[test]
    fn analytics_data_association_roundtrip() {
        let (id, _) = make_instance(json!({}));
        let put = request(
            "PUT",
            &format!("/analytics-data/instance/{id}/association"),
            json!({"DataSetId": "ds-1"}),
            "",
        );
        let out = body_of(&put);
        assert_eq!(out["DataSetId"], "ds-1");
        assert_eq!(out["TargetAccountId"], ACCOUNT.to_string());
        assert_eq!(out["ResourceShareStatus"], "ACTIVE");
        assert!(out["ResourceShareArn"]
            .as_str()
            .unwrap()
            .contains("resource-share/"));

        // duplicate
        let dup = request(
            "PUT",
            &format!("/analytics-data/instance/{id}/association"),
            json!({"DataSetId": "ds-1"}),
            "",
        );
        assert_eq!(status_of(&dup), 400);

        // absent association on wrong data set (DisassociateAnalyticsDataSet)
        let wrong = request(
            "POST",
            &format!("/analytics-data/instance/{id}/association"),
            json!({"DataSetId": "nope"}),
            "",
        );
        assert_eq!(status_of(&wrong), 404);

        // sorted list + filter
        request(
            "PUT",
            &format!("/analytics-data/instance/{id}/association"),
            json!({"DataSetId": "aa-1"}),
            "",
        );
        let list = request(
            "GET",
            &format!("/analytics-data/instance/{id}/association"),
            json!({}),
            "",
        );
        let val_results = body_of(&list);
        let results = val_results["Results"].as_array().unwrap();
        assert_eq!(results[0]["DataSetId"], "aa-1");
        assert_eq!(results[1]["DataSetId"], "ds-1");

        let list = request(
            "GET",
            &format!("/analytics-data/instance/{id}/association"),
            json!({}),
            "DataSetId=ds-1",
        );
        let val_filtered = body_of(&list);
        let filtered = val_filtered["Results"].as_array().unwrap();
        assert_eq!(filtered.len(), 1);

        // disassociate (BatchDisassociateAnalyticsDataSet at the plural path is
        // moto-unrouted -> 501; the real op posts to the singular path)
        let del = request(
            "POST",
            &format!("/analytics-data/instance/{id}/association"),
            json!({"DataSetId": "ds-1"}),
            "",
        );
        assert_eq!(status_of(&del), 200);
        let batch = request(
            "POST",
            &format!("/analytics-data/instance/{id}/associations"),
            json!({"DataSetId": "ds-1"}),
            "",
        );
        assert_eq!(batch.status, 501);
        let list = request(
            "GET",
            &format!("/analytics-data/instance/{id}/association"),
            json!({}),
            "",
        );
        assert_eq!(body_of(&list)["Results"].as_array().unwrap().len(), 1);
    }

    // -- Tags -----------------------------------------------------------------

    #[test]
    fn tag_resource_roundtrip() {
        let (id, arn) = make_instance(json!({"Tags": {"a": "b"}}));
        let desc = request("GET", &format!("/instance/{id}"), json!({}), "");
        assert_eq!(body_of(&desc)["Instance"]["Tags"]["a"], "b");

        let list = request(
            "GET",
            &format!("/tags/{}", url_encoded(&arn)),
            json!({}),
            "",
        );
        assert_eq!(body_of(&list)["tags"]["a"], "b");

        let tag = request(
            "POST",
            &format!("/tags/{}", url_encoded(&arn)),
            json!({"tags": {"k": "v"}}),
            "",
        );
        assert_eq!(status_of(&tag), 200);
        let list = request(
            "GET",
            &format!("/tags/{}", url_encoded(&arn)),
            json!({}),
            "",
        );
        let val_tags = body_of(&list);
        let tags = val_tags["tags"].as_object().unwrap();
        assert_eq!(tags.len(), 2);

        let untag = request(
            "DELETE",
            &format!("/tags/{}", url_encoded(&arn)),
            json!({}),
            "tagKeys=k&tagKeys=a",
        );
        assert_eq!(status_of(&untag), 200);
        let list = request(
            "GET",
            &format!("/tags/{}", url_encoded(&arn)),
            json!({}),
            "",
        );
        assert!(body_of(&list)["tags"].as_object().unwrap().is_empty());
    }

    fn url_encoded(arn: &str) -> String {
        arn.replace(':', "%3A").replace('/', "%2F")
    }

    // -- Agent statuses --------------------------------------------------------

    #[test]
    fn agent_status_lifecycle() {
        let (id, _) = make_instance(json!({}));
        let create = request(
            "PUT",
            &format!("/agent-status/{id}"),
            json!({"Name": "Available", "State": "ENABLED", "Description": "d"}),
            "",
        );
        let created = body_of(&create);
        let sid = created["AgentStatusId"].as_str().unwrap().to_string();
        assert_eq!(sid.len(), 36);
        assert!(created["AgentStatusARN"]
            .as_str()
            .unwrap()
            .ends_with(&format!("/agent-status/{sid}")));

        let desc = request("GET", &format!("/agent-status/{id}/{sid}"), json!({}), "");
        let agent_status = body_of(&desc);
        let status = &agent_status["AgentStatus"];
        assert_eq!(status["Name"], "Available");
        assert_eq!(status["State"], "ENABLED");
        assert_eq!(status["Type"], "CUSTOM");
        assert_eq!(status["Description"], "d");

        let listed = request("GET", &format!("/agent-status/{id}"), json!({}), "");
        let summary = body_of(&listed);
        let rows = summary["AgentStatusSummaryList"].as_array().unwrap();
        assert_eq!(rows[0]["Id"], sid.as_str());
        assert_eq!(rows[0]["Name"], "Available");

        let update = request(
            "POST",
            &format!("/agent-status/{id}/{sid}"),
            json!({"Name": "Busy", "State": "DISABLED"}),
            "",
        );
        assert_eq!(status_of(&update), 200);
        let desc = request("GET", &format!("/agent-status/{id}/{sid}"), json!({}), "");
        let updated = body_of(&desc);
        assert_eq!(updated["AgentStatus"]["Name"], "Busy");
        assert_eq!(updated["AgentStatus"]["State"], "DISABLED");

        let missing = request("GET", &format!("/agent-status/{id}/fake"), json!({}), "");
        assert_eq!(status_of(&missing), 404);
    }

    // -- Security profiles -----------------------------------------------------

    #[test]
    fn security_profile_lifecycle() {
        let (id, _) = make_instance(json!({}));
        let create = request(
            "PUT",
            &format!("/security-profiles/{id}"),
            json!({"SecurityProfileName": "Agent", "Permissions": ["p-1"]}),
            "",
        );
        let created = body_of(&create);
        let sp_id = created["SecurityProfileId"].as_str().unwrap().to_string();
        assert!(created["SecurityProfileArn"]
            .as_str()
            .unwrap()
            .ends_with(&format!("/security-profile/{sp_id}")));

        let desc = request(
            "GET",
            &format!("/security-profiles/{id}/{sp_id}"),
            json!({}),
            "",
        );
        let described = body_of(&desc);
        let sp = &described["SecurityProfile"];
        assert_eq!(sp["SecurityProfileName"], "Agent");
        // moto's to_dict omits Permissions (list_security_profile_permissions
        // is the only reader).
        assert!(sp.get("Permissions").is_none());

        let perms = request(
            "GET",
            &format!("/security-profiles-permissions/{id}/{sp_id}"),
            json!({}),
            "",
        );
        let perm_body = body_of(&perms);
        assert_eq!(perm_body["Permissions"][0], "p-1");

        let listed = request(
            "GET",
            &format!("/security-profiles-summary/{id}"),
            json!({}),
            "",
        );
        let list_body = body_of(&listed);
        assert_eq!(list_body["SecurityProfileSummaryList"][0]["Name"], "Agent");

        let delete = request(
            "DELETE",
            &format!("/security-profiles/{id}/{sp_id}"),
            json!({}),
            "",
        );
        assert_eq!(status_of(&delete), 200);
        let missing = request(
            "GET",
            &format!("/security-profiles/{id}/{sp_id}"),
            json!({}),
            "",
        );
        assert_eq!(status_of(&missing), 404);

        // applications list exists but moto always returns []
        let apps = request(
            "GET",
            &format!("/security-profiles-applications/{id}/{sp_id}"),
            json!({}),
            "",
        );
        // moto 404s: the profile is gone -> ResourceNotFound translated by list
        assert_eq!(apps.status, 200);
        assert!(body_of(&apps)["Applications"]
            .as_array()
            .unwrap_or(&vec![])
            .is_empty());
    }

    // -- Users -----------------------------------------------------------------

    #[test]
    fn user_lifecycle_and_updates() {
        let (id, _) = make_instance(json!({}));
        let create = request(
            "PUT",
            &format!("/users/{id}"),
            json!({"Username": "agent-1", "SecurityProfileIds": ["sp-1"], "RoutingProfileId": "rp-1"}),
            "",
        );
        let created = body_of(&create);
        let user_id = created["UserId"].as_str().unwrap().to_string();
        assert!(created["UserArn"]
            .as_str()
            .unwrap()
            .ends_with(&format!("/agent/{user_id}")));

        let desc = request("GET", &format!("/users/{id}/{user_id}"), json!({}), "");
        let described = body_of(&desc);
        let user = &described["User"];
        assert_eq!(user["Username"], "agent-1");
        assert_eq!(user["RoutingProfileId"], "rp-1");
        assert_eq!(user["PhoneConfig"]["PhoneType"], "SOFT_PHONE");
        assert_eq!(user["PhoneConfig"]["AutoAccept"], false);
        assert_eq!(user["IdentityInfo"], serde_json::json!({}));

        let upd = request(
            "POST",
            &format!("/users/{id}/{user_id}/routing-profile"),
            json!({"RoutingProfileId": "rp-2"}),
            "",
        );
        assert_eq!(status_of(&upd), 200);
        let upd2 = request(
            "POST",
            &format!("/users/{id}/{user_id}/identity-info"),
            json!({"IdentityInfo": {"FirstName": "Ann"}}),
            "",
        );
        assert_eq!(status_of(&upd2), 200);
        let upd3 = request(
            "POST",
            &format!("/users/{id}/{user_id}/security-profiles"),
            json!({"SecurityProfileIds": ["sp-2"]}),
            "",
        );
        assert_eq!(status_of(&upd3), 200);
        let upd4 = request(
            "POST",
            &format!("/users/{id}/{user_id}/hierarchy"),
            json!({"HierarchyGroupId": "hg-1"}),
            "",
        );
        assert_eq!(status_of(&upd4), 200);
        let desc = request("GET", &format!("/users/{id}/{user_id}"), json!({}), "");
        let described = body_of(&desc);
        let user = &described["User"];
        assert_eq!(user["RoutingProfileId"], "rp-2");
        assert_eq!(user["IdentityInfo"]["FirstName"], "Ann");
        assert_eq!(user["SecurityProfileIds"][0], "sp-2");
        assert_eq!(user["HierarchyGroupId"], "hg-1");

        // clearing hierarchy removes the field (moto params.get(None))
        let upd5 = request(
            "POST",
            &format!("/users/{id}/{user_id}/hierarchy"),
            json!({}),
            "",
        );
        assert_eq!(status_of(&upd5), 200);
        let desc = request("GET", &format!("/users/{id}/{user_id}"), json!({}), "");
        let described = body_of(&desc);
        assert!(described["User"]["HierarchyGroupId"].is_null());

        let listed = request("GET", &format!("/users-summary/{id}"), json!({}), "");
        let list_body = body_of(&listed);
        assert_eq!(list_body["UserSummaryList"][0]["Username"], "agent-1");

        let delete = request("DELETE", &format!("/users/{id}/{user_id}"), json!({}), "");
        assert_eq!(status_of(&delete), 200);
        let missing = request("GET", &format!("/users/{id}/{user_id}"), json!({}), "");
        assert_eq!(status_of(&missing), 404);

        // phone config update (explicit body value)
        let create2 = request(
            "PUT",
            &format!("/users/{id}"),
            json!({"Username": "u2", "SecurityProfileIds": [], "RoutingProfileId": "rp-1"}),
            "",
        );
        let created2 = body_of(&create2);
        let uid2 = created2["UserId"].as_str().unwrap().to_string();
        let upd = request(
            "POST",
            &format!("/users/{id}/{uid2}/phone-config"),
            json!({"PhoneConfig": {"PhoneType": "DESK_PHONE"}}),
            "",
        );
        assert_eq!(status_of(&upd), 200);
        let desc = request("GET", &format!("/users/{id}/{uid2}"), json!({}), "");
        let described = body_of(&desc);
        assert_eq!(described["User"]["PhoneConfig"]["PhoneType"], "DESK_PHONE");
    }

    #[test]
    fn user_hierarchy_group_lifecycle() {
        let (id, _) = make_instance(json!({}));
        let create = request(
            "PUT",
            &format!("/user-hierarchy-groups/{id}"),
            json!({"Name": "team-a"}),
            "",
        );
        let created = body_of(&create);
        let gid = created["HierarchyGroupId"].as_str().unwrap().to_string();
        assert!(created["HierarchyGroupArn"]
            .as_str()
            .unwrap()
            .ends_with(&format!("/agent-group/{gid}")));

        let desc = request(
            "GET",
            &format!("/user-hierarchy-groups/{id}/{gid}"),
            json!({}),
            "",
        );
        let described = body_of(&desc);
        let group = &described["HierarchyGroup"];
        assert_eq!(group["Name"], "team-a");
        assert_eq!(group["LevelId"], "1");

        let upd = request(
            "POST",
            &format!("/user-hierarchy-groups/{id}/{gid}/name"),
            json!({"Name": "team-b"}),
            "",
        );
        assert_eq!(status_of(&upd), 200);
        let desc = request(
            "GET",
            &format!("/user-hierarchy-groups/{id}/{gid}"),
            json!({}),
            "",
        );
        let described = body_of(&desc);
        assert_eq!(described["HierarchyGroup"]["Name"], "team-b");

        let listed = request(
            "GET",
            &format!("/user-hierarchy-groups-summary/{id}"),
            json!({}),
            "",
        );
        let list_body = body_of(&listed);
        assert_eq!(
            list_body["UserHierarchyGroupSummaryList"][0]["Name"],
            "team-b"
        );

        let structure = request(
            "GET",
            &format!("/user-hierarchy-structure/{id}"),
            json!({}),
            "",
        );
        let structure_body = body_of(&structure);
        assert_eq!(
            structure_body["HierarchyStructure"]["LevelOne"]["Name"],
            "Organization"
        );
        assert_eq!(
            structure_body["HierarchyStructure"]["LevelFive"]["Name"],
            "Agent"
        );

        let update_structure = request(
            "POST",
            &format!("/user-hierarchy-structure/{id}"),
            json!({"HierarchyStructure": {}}),
            "",
        );
        assert_eq!(status_of(&update_structure), 200);

        let delete = request(
            "DELETE",
            &format!("/user-hierarchy-groups/{id}/{gid}"),
            json!({}),
            "",
        );
        assert_eq!(status_of(&delete), 200);
        let missing = request(
            "GET",
            &format!("/user-hierarchy-groups/{id}/{gid}"),
            json!({}),
            "",
        );
        assert_eq!(status_of(&missing), 404);
    }

    // -- Contact flows --------------------------------------------------------

    #[test]
    fn contact_flow_lifecycle() {
        let (id, _) = make_instance(json!({}));
        let content = r#"{"Version":"2019-10-30","StartAction":"a1","Actions":[]}"#;
        let create = request(
            "PUT",
            &format!("/contact-flows/{id}"),
            json!({"Name": "F", "Type": "CONTACT_FLOW", "Content": content}),
            "",
        );
        let created = body_of(&create);
        let fid = created["ContactFlowId"].as_str().unwrap().to_string();
        assert!(created["ContactFlowArn"]
            .as_str()
            .unwrap()
            .ends_with(&format!("/contact-flow/{fid}")));

        let desc = request("GET", &format!("/contact-flows/{id}/{fid}"), json!({}), "");
        let described = body_of(&desc);
        let flow = &described["ContactFlow"];
        assert_eq!(flow["Name"], "F");
        assert_eq!(flow["Type"], "CONTACT_FLOW");
        assert_eq!(flow["State"], "ACTIVE");
        assert_eq!(flow["Status"], "PUBLISHED");
        // sha256 of the stored content
        assert_eq!(flow["FlowContentSha256"].as_str().unwrap().len(), 64);

        let upd_content = request(
            "POST",
            &format!("/contact-flows/{id}/{fid}/content"),
            json!({"Content": "{\"Version\":\"2019-10-30\"}"}),
            "",
        );
        assert_eq!(status_of(&upd_content), 200);
        let upd_name = request(
            "POST",
            &format!("/contact-flows/{id}/{fid}/name"),
            json!({"Name": "F2", "Description": "d"}),
            "",
        );
        assert_eq!(status_of(&upd_name), 200);
        let upd_meta = request(
            "POST",
            &format!("/contact-flows/{id}/{fid}/metadata"),
            json!({"ContactFlowState": "ARCHIVED"}),
            "",
        );
        assert_eq!(status_of(&upd_meta), 200);
        let versions = request(
            "GET",
            &format!("/contact-flows/{id}/{fid}/versions"),
            json!({}),
            "",
        );
        let versions_body = body_of(&versions);
        assert!(versions_body["ContactFlowVersionSummaryList"][0]["Version"]
            .as_i64()
            .is_some());

        let listed = request(
            "GET",
            &format!("/contact-flows-summary/{id}"),
            json!({}),
            "",
        );
        let list_body = body_of(&listed);
        let rows = list_body["ContactFlowSummaryList"].as_array().unwrap();
        assert_eq!(rows[0]["Name"], "F2");
        assert_eq!(rows[0]["ContactFlowState"], "ARCHIVED");

        let del = request(
            "DELETE",
            &format!("/contact-flows/{id}/{fid}"),
            json!({}),
            "",
        );
        assert_eq!(status_of(&del), 200);
        let after = request("GET", &format!("/contact-flows/{id}/{fid}"), json!({}), "");
        assert_eq!(status_of(&after), 404);
    }

    #[test]
    fn contact_flow_module_and_alias_lifecycle() {
        let (id, _) = make_instance(json!({}));
        let create = request(
            "PUT",
            &format!("/contact-flow-modules/{id}"),
            json!({"Name": "M", "Content": "{}"}),
            "",
        );
        let created = body_of(&create);
        let mid = created["Id"].as_str().unwrap().to_string();
        assert!(created["Arn"]
            .as_str()
            .unwrap()
            .ends_with(&format!("/flow-module/{mid}")));

        let desc = request(
            "GET",
            &format!("/contact-flow-modules/{id}/{mid}"),
            json!({}),
            "",
        );
        let described = body_of(&desc);
        assert_eq!(described["ContactFlowModule"]["Name"], "M");
        assert_eq!(described["ContactFlowModule"]["Status"], "PUBLISHED");

        let upd = request(
            "POST",
            &format!("/contact-flow-modules/{id}/{mid}/metadata"),
            json!({"Name": "M2"}),
            "",
        );
        assert_eq!(status_of(&upd), 200);
        let upd_content = request(
            "POST",
            &format!("/contact-flow-modules/{id}/{mid}/content"),
            json!({"Content": "{\"x\":1}"}),
            "",
        );
        assert_eq!(status_of(&upd_content), 200);

        let listed = request(
            "GET",
            &format!("/contact-flow-modules-summary/{id}"),
            json!({}),
            "",
        );
        let listed_body = body_of(&listed);
        assert_eq!(
            listed_body["ContactFlowModulesSummaryList"][0]["Name"],
            "M2"
        );

        // alias lifecycle (moto does not require the parent module to exist)
        let alias = request(
            "PUT",
            &format!("/contact-flow-modules/{id}/nonexistent-module/alias"),
            json!({"AliasName": "A", "ContactFlowModuleVersion": 1}),
            "",
        );
        let alias_body = body_of(&alias);
        let aid = alias_body["Id"].as_str().unwrap().to_string();
        assert!(alias_body["ContactFlowModuleArn"]
            .as_str()
            .unwrap()
            .ends_with(&format!("/alias/{aid}")));

        let desc = request(
            "GET",
            &format!("/contact-flow-modules/{id}/nonexistent-module/alias/{aid}"),
            json!({}),
            "",
        );
        let described = body_of(&desc);
        assert_eq!(described["ContactFlowModuleAlias"]["Name"], "A");
        assert_eq!(
            described["ContactFlowModuleAlias"]["TargetContactFlowModuleVersion"],
            "1"
        );

        let upd = request(
            "POST",
            &format!("/contact-flow-modules/{id}/nonexistent-module/alias/{aid}"),
            json!({"Name": "A2", "TargetContactFlowModuleVersion": 2}),
            "",
        );
        assert_eq!(status_of(&upd), 200);
        let updated = body_of(&upd);
        assert_eq!(updated["ContactFlowModuleAlias"]["Name"], "A2");
        // update stores the raw JSON member (number); create stringifies.
        assert_eq!(
            updated["ContactFlowModuleAlias"]["TargetContactFlowModuleVersion"],
            2
        );

        let del = request(
            "DELETE",
            &format!("/contact-flow-modules/{id}/nonexistent-module/alias/{aid}"),
            json!({}),
            "",
        );
        assert_eq!(status_of(&del), 200);
        let gone = request(
            "GET",
            &format!("/contact-flow-modules/{id}/nonexistent-module/alias/{aid}"),
            json!({}),
            "",
        );
        assert_eq!(status_of(&gone), 404);

        let del_module = request(
            "DELETE",
            &format!("/contact-flow-modules/{id}/{mid}"),
            json!({}),
            "",
        );
        assert_eq!(status_of(&del_module), 200);
        let gone_module = request(
            "GET",
            &format!("/contact-flow-modules/{id}/{mid}"),
            json!({}),
            "",
        );
        assert_eq!(status_of(&gone_module), 404);
    }

    #[test]
    fn contact_lifecycle() {
        let (id, _) = make_instance(json!({}));
        let create = request(
            "PUT",
            "/contact/create-contact",
            json!({"InstanceId": id.clone(), "Channel": "VOICE", "InitiationMethod": "INBOUND"}),
            "",
        );
        let created = body_of(&create);
        let cid = created["ContactId"].as_str().unwrap().to_string();
        // /contact/create-contact has no InstanceId path segment: the body
        // InstanceId is the only locator (moto falls back to it).
        assert_eq!(cid.len(), 36);
        assert!(created["ContactArn"]
            .as_str()
            .unwrap()
            .ends_with(&format!("/contact/{cid}")));

        let desc = request("GET", &format!("/contacts/{id}/{cid}"), json!({}), "");
        let described = body_of(&desc);
        let contact = &described["Contact"];
        assert_eq!(contact["Channel"], "VOICE");
        assert_eq!(contact["InitiationMethod"], "INBOUND");
        assert!(contact["InitiationTimestamp"].as_str().is_some());

        let upd = request(
            "POST",
            &format!("/contacts/{id}/{cid}"),
            json!({"Name": "changed"}),
            "",
        );
        assert_eq!(status_of(&upd), 200);
        let desc = request("GET", &format!("/contacts/{id}/{cid}"), json!({}), "");
        let described = body_of(&desc);
        assert_eq!(described["Contact"]["Name"], "changed");

        // contact attributes ride on a separate store
        let upd_attrs = request(
            "POST",
            "/contact/attributes",
            json!({"InstanceId": id.clone(), "InitialContactId": cid.clone(), "Attributes": {"foo": "bar"}}),
            "",
        );
        assert_eq!(status_of(&upd_attrs), 200);
        let get_attrs = request(
            "GET",
            &format!("/contact/attributes/{id}/{cid}"),
            json!({}),
            "",
        );
        let attrs_body = body_of(&get_attrs);
        assert_eq!(attrs_body["Attributes"]["foo"], "bar");

        // stop removes the contact
        let stop = request(
            "POST",
            "/contact/stop",
            json!({"InstanceId": id.clone(), "ContactId": cid.clone()}),
            "",
        );
        assert_eq!(status_of(&stop), 200);
        let after = request("GET", &format!("/contacts/{id}/{cid}"), json!({}), "");
        assert_eq!(status_of(&after), 404);
    }

    #[test]
    fn contact_evaluation_lifecycle() {
        let (id, _) = make_instance(json!({}));
        let contact = request(
            "PUT",
            "/contact/create-contact",
            json!({"InstanceId": id.clone(), "Channel": "VOICE", "InitiationMethod": "INBOUND"}),
            "",
        );
        let contact_id = body_of(&contact)["ContactId"].as_str().unwrap().to_string();
        // moto takes any EvaluationFormId at evaluation start (no FK check)
        let create = request(
            "PUT",
            &format!("/contact-evaluations/{id}"),
            json!({"ContactId": contact_id.clone(), "EvaluationFormId": "form-1"}),
            "",
        );
        let created = body_of(&create);
        let eid = created["EvaluationId"].as_str().unwrap().to_string();
        assert!(created["EvaluationArn"]
            .as_str()
            .unwrap()
            .ends_with(&format!("/contact-evaluation/{eid}")));

        let desc = request(
            "GET",
            &format!("/contact-evaluations/{id}/{eid}"),
            json!({}),
            "",
        );
        // moto presents the evaluation under "ContactEvaluation"
        let described = body_of(&desc);
        assert!(described.get("ContactEvaluation").is_some());
        assert_eq!(
            described["ContactEvaluation"]["ContactId"],
            contact_id.as_str()
        );
        assert_eq!(described["ContactEvaluation"]["EvaluationFormId"], "form-1");
        assert_eq!(described["ContactEvaluation"]["Status"], "DRAFT");

        let upd = request(
            "POST",
            &format!("/contact-evaluations/{id}/{eid}"),
            json!({"Status": "SUBMITTED"}),
            "",
        );
        assert_eq!(status_of(&upd), 200);
        let updated = body_of(&upd);
        assert_eq!(updated["ContactEvaluation"]["Status"], "SUBMITTED");

        let del = request(
            "DELETE",
            &format!("/contact-evaluations/{id}/{eid}"),
            json!({}),
            "",
        );
        assert_eq!(status_of(&del), 200);
        let after = request(
            "GET",
            &format!("/contact-evaluations/{id}/{eid}"),
            json!({}),
            "",
        );
        assert_eq!(status_of(&after), 404);
    }

    // -- Hours of operations ----------------------------------------------

    #[test]
    fn hours_of_operation_lifecycle() {
        let (id, _) = make_instance(json!({}));
        let create = request(
            "PUT",
            &format!("/hours-of-operations/{id}"),
            json!({"Name": "BH", "TimeZone": "America/New_York", "Config": [
                {"Day": "MONDAY", "StartTime": {"Hours": 9, "Minutes": 0}, "EndTime": {"Hours": 17, "Minutes": 0}}
            ]}),
            "",
        );
        let created = body_of(&create);
        let hid = created["HoursOfOperationId"].as_str().unwrap().to_string();
        assert!(created["HoursOfOperationArn"]
            .as_str()
            .unwrap()
            .ends_with(&format!("/operating-hours/{hid}")));

        let desc = request(
            "GET",
            &format!("/hours-of-operations/{id}/{hid}"),
            json!({}),
            "",
        );
        let described = body_of(&desc);
        let hoo = &described["HoursOfOperation"];
        assert_eq!(hoo["Name"], "BH");
        assert_eq!(hoo["TimeZone"], "America/New_York");
        assert_eq!(hoo["Config"][0]["Day"], "MONDAY");

        let upd = request(
            "POST",
            &format!("/hours-of-operations/{id}/{hid}"),
            json!({"Name": "BH2"}),
            "",
        );
        assert_eq!(status_of(&upd), 200);

        let listed = request(
            "GET",
            &format!("/hours-of-operations-summary/{id}"),
            json!({}),
            "",
        );
        let list_body = body_of(&listed);
        assert_eq!(list_body["HoursOfOperationSummaryList"][0]["Name"], "BH2");

        let del = request(
            "DELETE",
            &format!("/hours-of-operations/{id}/{hid}"),
            json!({}),
            "",
        );
        assert_eq!(status_of(&del), 200);
        let after = request(
            "GET",
            &format!("/hours-of-operations/{id}/{hid}"),
            json!({}),
            "",
        );
        assert_eq!(status_of(&after), 404);
    }

    #[test]
    fn hours_of_operation_override_lifecycle() {
        let (id, _) = make_instance(json!({}));
        let create = request(
            "PUT",
            &format!("/hours-of-operations/{id}/hoo-1/overrides"),
            json!({"Name": "Holiday", "EffectiveFrom": "2020-01-01", "EffectiveTill": "2020-12-31",
                   "Config": [{"Day": "MONDAY"}]}),
            "",
        );
        let created = body_of(&create);
        let oid = created["HoursOfOperationOverrideId"]
            .as_str()
            .unwrap()
            .to_string();
        assert_eq!(oid.len(), 36);

        let desc = request(
            "GET",
            &format!("/hours-of-operations/{id}/hoo-1/overrides/{oid}"),
            json!({}),
            "",
        );
        let described = body_of(&desc);
        let override_row = &described["HoursOfOperationOverride"];
        assert_eq!(override_row["Name"], "Holiday");
        assert_eq!(override_row["EffectiveFrom"], "2020-01-01");
        assert_eq!(override_row["EffectiveTill"], "2020-12-31");
        assert_eq!(override_row["Config"][0]["Day"], "MONDAY");

        let upd = request(
            "POST",
            &format!("/hours-of-operations/{id}/hoo-1/overrides/{oid}"),
            json!({"Name": "Holiday2"}),
            "",
        );
        assert_eq!(status_of(&upd), 200);

        let listed = request(
            "GET",
            &format!("/hours-of-operations/{id}/hoo-1/overrides"),
            json!({}),
            "",
        );
        let list_body = body_of(&listed);
        let rows = list_body["HoursOfOperationOverrideList"]
            .as_array()
            .unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0]["Name"], "Holiday2");

        // overrides live under instance+hours key: another hoo is empty
        let other = request(
            "GET",
            &format!("/hours-of-operations/{id}/other-hoo/overrides"),
            json!({}),
            "",
        );
        let other_body = body_of(&other);
        assert_eq!(
            other_body["HoursOfOperationOverrideList"]
                .as_array()
                .unwrap()
                .len(),
            0
        );

        let del = request(
            "DELETE",
            &format!("/hours-of-operations/{id}/hoo-1/overrides/{oid}"),
            json!({}),
            "",
        );
        assert_eq!(status_of(&del), 200);
        let after = request(
            "GET",
            &format!("/hours-of-operations/{id}/hoo-1/overrides/{oid}"),
            json!({}),
            "",
        );
        assert_eq!(status_of(&after), 404);
    }

    // -- Queues ---------------------------------------------------------------

    #[test]
    fn queue_lifecycle_and_updates() {
        let (id, _) = make_instance(json!({}));
        let create = request(
            "PUT",
            &format!("/queues/{id}"),
            json!({"Name": "Q1", "HoursOfOperationId": "hoo-1", "MaxContacts": 10}),
            "",
        );
        let created = body_of(&create);
        let qid = created["QueueId"].as_str().unwrap().to_string();
        assert!(created["QueueArn"]
            .as_str()
            .unwrap()
            .ends_with(&format!("/queue/{qid}")));

        let desc = request("GET", &format!("/queues/{id}/{qid}"), json!({}), "");
        let described = body_of(&desc);
        let queue = &described["Queue"];
        assert_eq!(queue["Name"], "Q1");
        assert_eq!(queue["HoursOfOperationId"], "hoo-1");
        assert_eq!(queue["MaxContacts"], 10);
        assert_eq!(queue["Status"], "ENABLED");
        assert_eq!(queue["OutboundCallerConfig"], serde_json::json!({}));

        let rename = request(
            "POST",
            &format!("/queues/{id}/{qid}/name"),
            json!({"Name": "Q2", "Description": "d"}),
            "",
        );
        assert_eq!(status_of(&rename), 200);
        let status = request(
            "POST",
            &format!("/queues/{id}/{qid}/status"),
            json!({"Status": "DEFERRED"}),
            "",
        );
        assert_eq!(status_of(&status), 200);
        let maxc = request(
            "POST",
            &format!("/queues/{id}/{qid}/max-contacts"),
            json!({"MaxContacts": 20}),
            "",
        );
        assert_eq!(status_of(&maxc), 200);
        let hoo = request(
            "POST",
            &format!("/queues/{id}/{qid}/hours-of-operation"),
            json!({"HoursOfOperationId": "hoo-2"}),
            "",
        );
        assert_eq!(status_of(&hoo), 200);
        let cfg = request(
            "POST",
            &format!("/queues/{id}/{qid}/outbound-caller-config"),
            json!({"OutboundCallerConfig": {"OutboundCallerIdName": "calls"}}),
            "",
        );
        assert_eq!(status_of(&cfg), 200);

        let desc = request("GET", &format!("/queues/{id}/{qid}"), json!({}), "");
        let described = body_of(&desc);
        let queue = &described["Queue"];
        assert_eq!(queue["Name"], "Q2");
        assert_eq!(queue["Description"], "d");
        assert_eq!(queue["Status"], "DEFERRED");
        assert_eq!(queue["MaxContacts"], 20);
        assert_eq!(queue["HoursOfOperationId"], "hoo-2");
        assert_eq!(
            queue["OutboundCallerConfig"]["OutboundCallerIdName"],
            "calls"
        );

        let listed = request("GET", &format!("/queues-summary/{id}"), json!({}), "");
        let list_body = body_of(&listed);
        let rows = list_body["QueueSummaryList"].as_array().unwrap();
        assert_eq!(rows[0]["QueueType"], "STANDARD");
        assert_eq!(rows[0]["Name"], "Q2");

        let del = request("DELETE", &format!("/queues/{id}/{qid}"), json!({}), "");
        assert_eq!(status_of(&del), 200);
        let after = request("GET", &format!("/queues/{id}/{qid}"), json!({}), "");
        assert_eq!(status_of(&after), 404);
    }

    // -- Quick connects ---------------------------------------------------------

    #[test]
    fn quick_connect_lifecycle_and_queue_association() {
        let (id, _) = make_instance(json!({}));
        let config = json!({"QuickConnectType": "PHONE_NUMBER",
                            "QuickConnectConfig": {"PhoneNumber": "+15551001"}});
        let create = request(
            "PUT",
            &format!("/quick-connects/{id}"),
            json!({"Name": "QC1", "QuickConnectConfig": config}),
            "",
        );
        let created = body_of(&create);
        let qcid1 = created["QuickConnectId"].as_str().unwrap().to_string();
        assert!(created["QuickConnectARN"]
            .as_str()
            .unwrap()
            .ends_with(&format!("/transfer-destination/{qcid1}")));

        let create2 = request(
            "PUT",
            &format!("/quick-connects/{id}"),
            json!({"Name": "QC2", "QuickConnectConfig": {"QuickConnectType": "QUEUE"}}),
            "",
        );
        let qcid2 = body_of(&create2)["QuickConnectId"]
            .as_str()
            .unwrap()
            .to_string();

        let desc = request(
            "GET",
            &format!("/quick-connects/{id}/{qcid1}"),
            json!({}),
            "",
        );
        let described = body_of(&desc);
        assert_eq!(described["QuickConnect"]["Name"], "QC1");
        assert_eq!(
            described["QuickConnect"]["QuickConnectConfig"]["QuickConnectType"],
            "PHONE_NUMBER"
        );

        let listed = request("GET", &format!("/quick-connects/{id}"), json!({}), "");
        let list_body = body_of(&listed);
        let rows = list_body["QuickConnectSummaryList"].as_array().unwrap();
        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0]["QuickConnectType"], "PHONE_NUMBER");

        // queue association
        let queue = request(
            "PUT",
            &format!("/queues/{id}"),
            json!({"Name": "QA", "HoursOfOperationId": "hoo-1"}),
            "",
        );
        let qid = body_of(&queue)["QueueId"].as_str().unwrap().to_string();
        let assoc = request(
            "POST",
            &format!("/queues/{id}/{qid}/associate-quick-connects"),
            json!({"QuickConnectIds": [qcid1.clone()]}),
            "",
        );
        assert_eq!(status_of(&assoc), 200);
        let listed = request(
            "GET",
            &format!("/queues/{id}/{qid}/quick-connects"),
            json!({}),
            "",
        );
        let assoc_body = body_of(&listed);
        let rows = assoc_body["QuickConnectSummaryList"].as_array().unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0]["Name"], "QC1");

        let disassoc = request(
            "POST",
            &format!("/queues/{id}/{qid}/disassociate-quick-connects"),
            json!({"QuickConnectIds": [qcid1]}),
            "",
        );
        assert_eq!(status_of(&disassoc), 200);

        // config update
        let upd = request(
            "POST",
            &format!("/quick-connects/{id}/{qcid2}/config"),
            json!({"QuickConnectConfig": {"QuickConnectType": "USER"}}),
            "",
        );
        assert_eq!(status_of(&upd), 200);
        let desc = request(
            "GET",
            &format!("/quick-connects/{id}/{qcid2}"),
            json!({}),
            "",
        );
        let described = body_of(&desc);
        assert_eq!(
            described["QuickConnect"]["QuickConnectConfig"]["QuickConnectType"],
            "USER"
        );

        let del = request(
            "DELETE",
            &format!("/quick-connects/{id}/{qcid2}"),
            json!({}),
            "",
        );
        assert_eq!(status_of(&del), 200);
        let after = request(
            "GET",
            &format!("/quick-connects/{id}/{qcid2}"),
            json!({}),
            "",
        );
        assert_eq!(status_of(&after), 404);
    }

    // -- Routing profiles ---------------------------------------------------------

    #[test]
    fn routing_profile_lifecycle_and_queue_association() {
        let (id, _) = make_instance(json!({}));
        let create = request(
            "PUT",
            &format!("/routing-profiles/{id}"),
            json!({"Name": "RP1", "Description": "d", "DefaultOutboundQueueId": "q-1",
                   "MediaConcurrencies": [{"Channel": "VOICE", "Concurrency": 1}]}),
            "",
        );
        let created = body_of(&create);
        let rpid = created["RoutingProfileId"].as_str().unwrap().to_string();
        assert!(created["RoutingProfileArn"]
            .as_str()
            .unwrap()
            .ends_with(&format!("/routing-profile/{rpid}")));

        let desc = request(
            "GET",
            &format!("/routing-profiles/{id}/{rpid}"),
            json!({}),
            "",
        );
        let described = body_of(&desc);
        let rp = &described["RoutingProfile"];
        assert_eq!(rp["Name"], "RP1");
        assert_eq!(rp["InstanceId"], id.as_str());
        assert_eq!(rp["NumberOfAssociatedQueues"], 0);
        assert_eq!(rp["DefaultOutboundQueueId"], "q-1");
        assert_eq!(rp["MediaConcurrencies"][0]["Channel"], "VOICE");

        let upd_name = request(
            "POST",
            &format!("/routing-profiles/{id}/{rpid}/name"),
            json!({"Name": "RP2"}),
            "",
        );
        assert_eq!(status_of(&upd_name), 200);
        let upd_conc = request(
            "POST",
            &format!("/routing-profiles/{id}/{rpid}/concurrency"),
            json!({"MediaConcurrencies": [{"Channel": "CHAT", "Concurrency": 2}]}),
            "",
        );
        assert_eq!(status_of(&upd_conc), 200);
        let upd_q = request(
            "POST",
            &format!("/routing-profiles/{id}/{rpid}/default-outbound-queue"),
            json!({"DefaultOutboundQueueId": "q-2"}),
            "",
        );
        assert_eq!(status_of(&upd_q), 200);

        let listed = request(
            "GET",
            &format!("/routing-profiles-summary/{id}"),
            json!({}),
            "",
        );
        let list_body = body_of(&listed);
        assert_eq!(list_body["RoutingProfileSummaryList"][0]["Name"], "RP2");

        // queue association
        let assoc = request(
            "POST",
            &format!("/routing-profiles/{id}/{rpid}/associate-queues"),
            json!({"QueueConfigs": [
                {"QueueReference": {"Channel": "VOICE", "QueueId": "q-1"}, "QueuePriority": 1},
                {"QueueReference": {"Channel": "CHAT", "QueueId": "q-2"}, "QueuePriority": 2}
            ]}),
            "",
        );
        assert_eq!(status_of(&assoc), 200);
        let listed_q = request(
            "GET",
            &format!("/routing-profiles/{id}/{rpid}/queues"),
            json!({}),
            "",
        );
        let listed_q_body = body_of(&listed_q);
        let rows = listed_q_body["RoutingProfileQueueConfigSummaryList"]
            .as_array()
            .unwrap();
        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0]["QueueReference"]["QueueId"], "q-1");

        let disassoc = request(
            "POST",
            &format!("/routing-profiles/{id}/{rpid}/disassociate-queues"),
            json!({"QueueReferences": [{"QueueId": "q-1"}]}),
            "",
        );
        assert_eq!(status_of(&disassoc), 200);
        let listed_q = request(
            "GET",
            &format!("/routing-profiles/{id}/{rpid}/queues"),
            json!({}),
            "",
        );
        let after_body = body_of(&listed_q);
        let rows = after_body["RoutingProfileQueueConfigSummaryList"]
            .as_array()
            .unwrap();
        assert_eq!(rows.len(), 1);

        let update_qs = request(
            "POST",
            &format!("/routing-profiles/{id}/{rpid}/queues"),
            json!({"QueueConfigs": [
                {"QueueReference": {"Channel": "TASK", "QueueId": "q-3"}, "QueuePriority": 1}
            ]}),
            "",
        );
        assert_eq!(status_of(&update_qs), 200);

        let desc = request(
            "GET",
            &format!("/routing-profiles/{id}/{rpid}"),
            json!({}),
            "",
        );
        let described = body_of(&desc);
        assert_eq!(described["RoutingProfile"]["NumberOfAssociatedQueues"], 1);
        assert_eq!(described["RoutingProfile"]["DefaultOutboundQueueId"], "q-2");
        assert_eq!(
            described["RoutingProfile"]["MediaConcurrencies"][0]["Channel"],
            "CHAT"
        );

        let del = request(
            "DELETE",
            &format!("/routing-profiles/{id}/{rpid}"),
            json!({}),
            "",
        );
        assert_eq!(status_of(&del), 200);
        let after = request(
            "GET",
            &format!("/routing-profiles/{id}/{rpid}"),
            json!({}),
            "",
        );
        assert_eq!(status_of(&after), 404);
    }

    // -- deny-by-default -----------------------------------------------------

    #[test]
    fn unimplemented_and_unknown_ops() {
        // moto routes this URL but has no handler
        let resp = request("GET", "/instance/i/storage-config/a", json!({}), "");
        assert_eq!(resp.status, 501);
        let body = String::from_utf8_lossy(&resp.body);
        assert!(
            body.contains("The describe_instance_storage_config action has not been implemented"),
            "{body}"
        );

        // moto does not route this at all
        let resp = request("PUT", "/contact/chat", json!({}), "");
        assert_eq!(resp.status, 501);
        let body = String::from_utf8_lossy(&resp.body);
        assert!(
            body.contains("Service connect is not yet implemented"),
            "{body}"
        );

        // unregistered URL shape
        let resp = request("POST", "/bogus/path", json!({}), "");
        assert_eq!(resp.status, 501);
        let body = String::from_utf8_lossy(&resp.body);
        assert!(
            body.contains("Service connect is not yet implemented"),
            "{body}"
        );
    }
}
