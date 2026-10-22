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
    // URL-table classification (H = a native port answers in-process —
    // originally the moto handlers, extended by the botocore-shape
    // instance-family port; H501 = routed but unimplemented in moto,
    // R501 = not routed by moto at all).
        "POST", "evaluation-forms/{InstanceId}/{EvaluationFormId}/activate", "ActivateEvaluationForm", "H", false, ["InstanceId", "EvaluationFormId"];
        "PUT", "analytics-data/instance/{InstanceId}/association", "AssociateAnalyticsDataSet", "H", false, ["InstanceId"];
        "PUT", "instance/{InstanceId}/approved-origin", "AssociateApprovedOrigin", "H", false, ["InstanceId"];
        "PUT", "instance/{InstanceId}/bot", "AssociateBot", "H", false, ["InstanceId"];
        "POST", "contacts/{InstanceId}/{ContactId}/associate-user", "AssociateContactWithUser", "H", false, ["InstanceId", "ContactId"];
        "PUT", "default-vocabulary/{InstanceId}/{LanguageCode}", "AssociateDefaultVocabulary", "H", false, ["InstanceId", "LanguageCode"];
        "POST", "email-addresses/{InstanceId}/{EmailAddressId}/associate-alias", "AssociateEmailAddressAlias", "H", false, ["InstanceId", "EmailAddressId"];
        "PUT", "flow-associations/{InstanceId}", "AssociateFlow", "H", false, ["InstanceId"];
        "POST", "hours-of-operations/{InstanceId}/{HoursOfOperationId}/associate-hours", "AssociateHoursOfOperations", "H", false, ["InstanceId", "HoursOfOperationId"];
        "PUT", "instance/{InstanceId}/storage-config", "AssociateInstanceStorageConfig", "H", false, ["InstanceId"];
        "PUT", "instance/{InstanceId}/lambda-function", "AssociateLambdaFunction", "H", false, ["InstanceId"];
        "PUT", "instance/{InstanceId}/lex-bot", "AssociateLexBot", "H", false, ["InstanceId"];
        "PUT", "phone-number/{PhoneNumberId}/contact-flow", "AssociatePhoneNumberContactFlow", "H", false, ["PhoneNumberId"];
        "POST", "queues/{InstanceId}/{QueueId}/associate-email-addresses", "AssociateQueueEmailAddresses", "H", false, ["InstanceId", "QueueId"];
        "POST", "queues/{InstanceId}/{QueueId}/associate-quick-connects", "AssociateQueueQuickConnects", "H", false, ["InstanceId", "QueueId"];
        "POST", "routing-profiles/{InstanceId}/{RoutingProfileId}/associate-queues", "AssociateRoutingProfileQueues", "H", false, ["InstanceId", "RoutingProfileId"];
        "PUT", "instance/{InstanceId}/security-key", "AssociateSecurityKey", "H", false, ["InstanceId"];
        "POST", "associate-security-profiles/{InstanceId}", "AssociateSecurityProfiles", "H", false, ["InstanceId"];
        "PUT", "traffic-distribution-group/{TrafficDistributionGroupId}/user", "AssociateTrafficDistributionGroupUser", "H", false, ["TrafficDistributionGroupId"];
        "POST", "users/{InstanceId}/{UserId}/associate-proficiencies", "AssociateUserProficiencies", "H", false, ["InstanceId", "UserId"];
        "POST", "workspaces/{InstanceId}/{WorkspaceId}/associate", "AssociateWorkspace", "H", false, ["InstanceId", "WorkspaceId"];
        "PUT", "analytics-data/instance/{InstanceId}/associations", "BatchAssociateAnalyticsDataSet", "H", false, ["InstanceId"];
        "POST", "data-tables/{InstanceId}/{DataTableId}/values/create", "BatchCreateDataTableValue", "H", false, ["InstanceId", "DataTableId"];
        "POST", "data-tables/{InstanceId}/{DataTableId}/values/delete", "BatchDeleteDataTableValue", "H", false, ["InstanceId", "DataTableId"];
        "POST", "data-tables/{InstanceId}/{DataTableId}/values/describe", "BatchDescribeDataTableValue", "H", false, ["InstanceId", "DataTableId"];
        "POST", "analytics-data/instance/{InstanceId}/associations", "BatchDisassociateAnalyticsDataSet", "H", false, ["InstanceId"];
        "POST", "attached-files/{InstanceId}", "BatchGetAttachedFileMetadata", "H", false, ["InstanceId"];
        "POST", "flow-associations-batch/{InstanceId}", "BatchGetFlowAssociation", "H", false, ["InstanceId"];
        "PUT", "contact/batch/{InstanceId}", "BatchPutContact", "H", false, ["InstanceId"];
        "POST", "data-tables/{InstanceId}/{DataTableId}/values/update", "BatchUpdateDataTableValue", "H", false, ["InstanceId", "DataTableId"];
        "POST", "phone-number/claim", "ClaimPhoneNumber", "H", false, [];
        "POST", "attached-files/{InstanceId}/{FileId}", "CompleteAttachedFileUpload", "H", false, ["InstanceId", "FileId"];
        "PUT", "agent-status/{InstanceId}", "CreateAgentStatus", "H", false, ["InstanceId"];
        "PUT", "contact/create-contact", "CreateContact", "H", false, [];
        "PUT", "contact-flows/{InstanceId}", "CreateContactFlow", "H", false, ["InstanceId"];
        "PUT", "contact-flow-modules/{InstanceId}", "CreateContactFlowModule", "H", false, ["InstanceId"];
        "PUT", "contact-flow-modules/{InstanceId}/{ContactFlowModuleId}/alias", "CreateContactFlowModuleAlias", "H", false, ["InstanceId", "ContactFlowModuleId"];
        "PUT", "contact-flow-modules/{InstanceId}/{ContactFlowModuleId}/version", "CreateContactFlowModuleVersion", "H", false, ["InstanceId", "ContactFlowModuleId"];
        "PUT", "contact-flows/{InstanceId}/{ContactFlowId}/version", "CreateContactFlowVersion", "H", false, ["InstanceId", "ContactFlowId"];
        "PUT", "data-tables/{InstanceId}", "CreateDataTable", "H", false, ["InstanceId"];
        "PUT", "data-tables/{InstanceId}/{DataTableId}/attributes", "CreateDataTableAttribute", "H", false, ["InstanceId", "DataTableId"];
        "PUT", "email-addresses/{InstanceId}", "CreateEmailAddress", "H", false, ["InstanceId"];
        "PUT", "evaluation-forms/{InstanceId}", "CreateEvaluationForm", "H", false, ["InstanceId"];
        "PUT", "hours-of-operations/{InstanceId}", "CreateHoursOfOperation", "H", false, ["InstanceId"];
        "PUT", "hours-of-operations/{InstanceId}/{HoursOfOperationId}/overrides", "CreateHoursOfOperationOverride", "H", false, ["InstanceId", "HoursOfOperationId"];
        "PUT", "instance", "CreateInstance", "H", false, [];
        "PUT", "instance/{InstanceId}/integration-associations", "CreateIntegrationAssociation", "H", false, ["InstanceId"];
        "PUT", "notifications/{InstanceId}", "CreateNotification", "H", false, ["InstanceId"];
        "POST", "contact/create-participant", "CreateParticipant", "H", false, [];
        "POST", "contact/persistent-contact-association/{InstanceId}/{InitialContactId}", "CreatePersistentContactAssociation", "H", false, ["InstanceId", "InitialContactId"];
        "PUT", "predefined-attributes/{InstanceId}", "CreatePredefinedAttribute", "H", false, ["InstanceId"];
        "PUT", "prompts/{InstanceId}", "CreatePrompt", "H", false, ["InstanceId"];
        "PUT", "push-notification/{InstanceId}/registrations", "CreatePushNotificationRegistration", "H", false, ["InstanceId"];
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
        "PUT", "views/{InstanceId}/{ViewId}/versions", "CreateViewVersion", "H", false, ["InstanceId", "ViewId"];
        "POST", "vocabulary/{InstanceId}", "CreateVocabulary", "H", false, ["InstanceId"];
        "PUT", "workspaces/{InstanceId}", "CreateWorkspace", "H", false, ["InstanceId"];
        "PUT", "workspaces/{InstanceId}/{WorkspaceId}/pages", "CreateWorkspacePage", "H", false, ["InstanceId", "WorkspaceId"];
        "POST", "evaluation-forms/{InstanceId}/{EvaluationFormId}/deactivate", "DeactivateEvaluationForm", "H", false, ["InstanceId", "EvaluationFormId"];
        "DELETE", "attached-files/{InstanceId}/{FileId}", "DeleteAttachedFile", "H", false, ["InstanceId", "FileId"];
        "DELETE", "contact-evaluations/{InstanceId}/{EvaluationId}", "DeleteContactEvaluation", "H", false, ["InstanceId", "EvaluationId"];
        "DELETE", "contact-flows/{InstanceId}/{ContactFlowId}", "DeleteContactFlow", "H", false, ["InstanceId", "ContactFlowId"];
        "DELETE", "contact-flow-modules/{InstanceId}/{ContactFlowModuleId}", "DeleteContactFlowModule", "H", false, ["InstanceId", "ContactFlowModuleId"];
        "DELETE", "contact-flow-modules/{InstanceId}/{ContactFlowModuleId}/alias/{AliasId}", "DeleteContactFlowModuleAlias", "H", false, ["InstanceId", "ContactFlowModuleId", "AliasId"];
        "DELETE", "contact-flow-modules/{InstanceId}/{ContactFlowModuleId}/version/{ContactFlowModuleVersion}", "DeleteContactFlowModuleVersion", "H", false, ["InstanceId", "ContactFlowModuleId", "ContactFlowModuleVersion"];
        "DELETE", "contact-flows/{InstanceId}/{ContactFlowId}/version/{ContactFlowVersion}", "DeleteContactFlowVersion", "H", false, ["InstanceId", "ContactFlowId", "ContactFlowVersion"];
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
        "DELETE", "push-notification/{InstanceId}/registrations/{RegistrationId}", "DeletePushNotificationRegistration", "H", false, ["InstanceId", "RegistrationId"];
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
        "DELETE", "views/{InstanceId}/{ViewId}/versions/{ViewVersion}", "DeleteViewVersion", "H", false, ["InstanceId", "ViewId", "ViewVersion"];
        "POST", "vocabulary-remove/{InstanceId}/{VocabularyId}", "DeleteVocabulary", "H", false, ["InstanceId", "VocabularyId"];
        "DELETE", "workspaces/{InstanceId}/{WorkspaceId}", "DeleteWorkspace", "H", false, ["InstanceId", "WorkspaceId"];
        "DELETE", "workspaces/{InstanceId}/{WorkspaceId}/media", "DeleteWorkspaceMedia", "H", false, ["InstanceId", "WorkspaceId"];
        "DELETE", "workspaces/{InstanceId}/{WorkspaceId}/pages/{Page}", "DeleteWorkspacePage", "H", false, ["InstanceId", "WorkspaceId", "Page"];
        "GET", "agent-status/{InstanceId}/{AgentStatusId}", "DescribeAgentStatus", "H", false, ["InstanceId", "AgentStatusId"];
        "GET", "authentication-profiles/{InstanceId}/{AuthenticationProfileId}", "DescribeAuthenticationProfile", "H", false, ["InstanceId", "AuthenticationProfileId"];
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
        "GET", "instance/{InstanceId}/storage-config/{AssociationId}", "DescribeInstanceStorageConfig", "H", false, ["InstanceId", "AssociationId"];
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
        "POST", "email-addresses/{InstanceId}/{EmailAddressId}/disassociate-alias", "DisassociateEmailAddressAlias", "H", false, ["InstanceId", "EmailAddressId"];
        "DELETE", "flow-associations/{InstanceId}/{ResourceId}/{ResourceType}", "DisassociateFlow", "H", false, ["InstanceId", "ResourceId", "ResourceType"];
        "POST", "hours-of-operations/{InstanceId}/{HoursOfOperationId}/disassociate-hours", "DisassociateHoursOfOperations", "H", false, ["InstanceId", "HoursOfOperationId"];
        "DELETE", "instance/{InstanceId}/storage-config/{AssociationId}", "DisassociateInstanceStorageConfig", "H", false, ["InstanceId", "AssociationId"];
        "DELETE", "instance/{InstanceId}/lambda-function", "DisassociateLambdaFunction", "H", false, ["InstanceId"];
        "DELETE", "instance/{InstanceId}/lex-bot", "DisassociateLexBot", "H", false, ["InstanceId"];
        "DELETE", "phone-number/{PhoneNumberId}/contact-flow", "DisassociatePhoneNumberContactFlow", "H", false, ["PhoneNumberId"];
        "POST", "queues/{InstanceId}/{QueueId}/disassociate-email-addresses", "DisassociateQueueEmailAddresses", "H", false, ["InstanceId", "QueueId"];
        "POST", "queues/{InstanceId}/{QueueId}/disassociate-quick-connects", "DisassociateQueueQuickConnects", "H", false, ["InstanceId", "QueueId"];
        "POST", "routing-profiles/{InstanceId}/{RoutingProfileId}/disassociate-queues", "DisassociateRoutingProfileQueues", "H", false, ["InstanceId", "RoutingProfileId"];
        "DELETE", "instance/{InstanceId}/security-key/{AssociationId}", "DisassociateSecurityKey", "H", false, ["InstanceId", "AssociationId"];
        "POST", "disassociate-security-profiles/{InstanceId}", "DisassociateSecurityProfiles", "H", false, ["InstanceId"];
        "DELETE", "traffic-distribution-group/{TrafficDistributionGroupId}/user", "DisassociateTrafficDistributionGroupUser", "H", false, ["TrafficDistributionGroupId"];
        "POST", "users/{InstanceId}/{UserId}/disassociate-proficiencies", "DisassociateUserProficiencies", "H", false, ["InstanceId", "UserId"];
        "POST", "workspaces/{InstanceId}/{WorkspaceId}/disassociate", "DisassociateWorkspace", "H", false, ["InstanceId", "WorkspaceId"];
        "POST", "users/{InstanceId}/{UserId}/contact", "DismissUserContact", "H", false, ["InstanceId", "UserId"];
        "POST", "data-tables/{InstanceId}/{DataTableId}/values/evaluate", "EvaluateDataTableValues", "H", false, ["InstanceId", "DataTableId"];
        "GET", "attached-files/{InstanceId}/{FileId}", "GetAttachedFile", "H", false, ["InstanceId", "FileId"];
        "GET", "contact/attributes/{InstanceId}/{InitialContactId}", "GetContactAttributes", "H", false, ["InstanceId", "InitialContactId"];
        "POST", "metrics/contact", "GetContactMetrics", "H", false, [];
        "POST", "metrics/current/{InstanceId}", "GetCurrentMetricData", "H", false, ["InstanceId"];
        "POST", "metrics/userdata/{InstanceId}", "GetCurrentUserData", "H", false, ["InstanceId"];
        "GET", "effective-hours-of-operations/{InstanceId}/{HoursOfOperationId}", "GetEffectiveHoursOfOperations", "H", false, ["InstanceId", "HoursOfOperationId"];
        "GET", "user/federate/{InstanceId}", "GetFederationToken", "H", false, ["InstanceId"];
        "GET", "flow-associations/{InstanceId}/{ResourceId}/{ResourceType}", "GetFlowAssociation", "H", false, ["InstanceId", "ResourceId", "ResourceType"];
        "POST", "metrics/historical/{InstanceId}", "GetMetricData", "H", false, ["InstanceId"];
        "POST", "metrics/data", "GetMetricDataV2", "H", false, [];
        "GET", "prompts/{InstanceId}/{PromptId}/file", "GetPromptFile", "H", false, ["InstanceId", "PromptId"];
        "GET", "instance/{InstanceId}/task/template/{TaskTemplateId}", "GetTaskTemplate", "H", false, ["InstanceId", "TaskTemplateId"];
        "GET", "test-cases/{InstanceId}/{TestCaseId}/{TestCaseExecutionId}/summary", "GetTestCaseExecutionSummary", "H", false, ["InstanceId", "TestCaseId", "TestCaseExecutionId"];
        "GET", "traffic-distribution/{Id}", "GetTrafficDistribution", "H", false, ["Id"];
        "POST", "phone-number/import", "ImportPhoneNumber", "H", false, [];
        "POST", "workspaces/{InstanceId}/{WorkspaceId}/media", "ImportWorkspaceMedia", "H", false, ["InstanceId", "WorkspaceId"];
        "GET", "agent-status/{InstanceId}", "ListAgentStatuses", "H", false, ["InstanceId"];
        "GET", "analytics-data/instance/{InstanceId}/association", "ListAnalyticsDataAssociations", "H", false, ["InstanceId"];
        "GET", "analytics-data/instance/{InstanceId}/datasets", "ListAnalyticsDataLakeDataSets", "H", false, ["InstanceId"];
        "GET", "instance/{InstanceId}/approved-origins", "ListApprovedOrigins", "H", false, ["InstanceId"];
        "GET", "contact/associated/{InstanceId}", "ListAssociatedContacts", "H", false, ["InstanceId"];
        "GET", "authentication-profiles-summary/{InstanceId}", "ListAuthenticationProfiles", "H", false, ["InstanceId"];
        "GET", "instance/{InstanceId}/bots", "ListBots", "H", false, ["InstanceId"];
        "GET", "hours-of-operations/{InstanceId}/{HoursOfOperationId}/hours", "ListChildHoursOfOperations", "H", false, ["InstanceId", "HoursOfOperationId"];
        "GET", "contact-evaluations/{InstanceId}", "ListContactEvaluations", "H", false, ["InstanceId"];
        "GET", "contact-flow-modules/{InstanceId}/{ContactFlowModuleId}/aliases", "ListContactFlowModuleAliases", "H", false, ["InstanceId", "ContactFlowModuleId"];
        "GET", "contact-flow-modules/{InstanceId}/{ContactFlowModuleId}/versions", "ListContactFlowModuleVersions", "H", false, ["InstanceId", "ContactFlowModuleId"];
        "GET", "contact-flow-modules-summary/{InstanceId}", "ListContactFlowModules", "H", false, ["InstanceId"];
        "GET", "contact-flows/{InstanceId}/{ContactFlowId}/versions", "ListContactFlowVersions", "H", false, ["InstanceId", "ContactFlowId"];
        "GET", "contact-flows-summary/{InstanceId}", "ListContactFlows", "H", false, ["InstanceId"];
        "GET", "contact/references/{InstanceId}/{ContactId}", "ListContactReferences", "H", false, ["InstanceId", "ContactId"];
        "POST", "data-tables/{InstanceId}/{DataTableId}/attributes", "ListDataTableAttributes", "H", false, ["InstanceId", "DataTableId"];
        "POST", "data-tables/{InstanceId}/{DataTableId}/values/list-primary", "ListDataTablePrimaryValues", "H", false, ["InstanceId", "DataTableId"];
        "POST", "data-tables/{InstanceId}/{DataTableId}/values/list", "ListDataTableValues", "H", false, ["InstanceId", "DataTableId"];
        "GET", "data-tables/{InstanceId}", "ListDataTables", "H", false, ["InstanceId"];
        "POST", "default-vocabulary-summary/{InstanceId}", "ListDefaultVocabularies", "H", false, ["InstanceId"];
        "POST", "entity-security-profiles-summary/{InstanceId}", "ListEntitySecurityProfiles", "H", false, ["InstanceId"];
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
        "GET", "instance/{InstanceId}/lex-bots", "ListLexBots", "H", false, ["InstanceId"];
        "GET", "notifications/{InstanceId}", "ListNotifications", "H", false, ["InstanceId"];
        "GET", "phone-numbers-summary/{InstanceId}", "ListPhoneNumbers", "H", false, ["InstanceId"];
        "POST", "phone-number/list", "ListPhoneNumbersV2", "H", false, [];
        "GET", "predefined-attributes/{InstanceId}", "ListPredefinedAttributes", "H", false, ["InstanceId"];
        "GET", "prompts-summary/{InstanceId}", "ListPrompts", "H", false, ["InstanceId"];
        "GET", "queues/{InstanceId}/{QueueId}/email-addresses", "ListQueueEmailAddresses", "H", false, ["InstanceId", "QueueId"];
        "GET", "queues/{InstanceId}/{QueueId}/quick-connects", "ListQueueQuickConnects", "H", false, ["InstanceId", "QueueId"];
        "GET", "queues-summary/{InstanceId}", "ListQueues", "H", false, ["InstanceId"];
        "GET", "quick-connects/{InstanceId}", "ListQuickConnects", "H", false, ["InstanceId"];
        "POST", "contact/list-real-time-analysis-segments-v2/{InstanceId}/{ContactId}", "ListRealtimeContactAnalysisSegmentsV2", "H", false, ["InstanceId", "ContactId"];
        "GET", "routing-profiles/{InstanceId}/{RoutingProfileId}/manual-assignment-queues", "ListRoutingProfileManualAssignmentQueues", "H", false, ["InstanceId", "RoutingProfileId"];
        "GET", "routing-profiles/{InstanceId}/{RoutingProfileId}/queues", "ListRoutingProfileQueues", "H", false, ["InstanceId", "RoutingProfileId"];
        "GET", "routing-profiles-summary/{InstanceId}", "ListRoutingProfiles", "H", false, ["InstanceId"];
        "GET", "rules/{InstanceId}", "ListRules", "H", false, ["InstanceId"];
        "GET", "instance/{InstanceId}/security-keys", "ListSecurityKeys", "H", false, ["InstanceId"];
        "GET", "security-profiles-applications/{InstanceId}/{SecurityProfileId}", "ListSecurityProfileApplications", "H", false, ["InstanceId", "SecurityProfileId"];
        "GET", "security-profiles-flow-modules/{InstanceId}/{SecurityProfileId}", "ListSecurityProfileFlowModules", "H", false, ["InstanceId", "SecurityProfileId"];
        "GET", "security-profiles-permissions/{InstanceId}/{SecurityProfileId}", "ListSecurityProfilePermissions", "H", false, ["InstanceId", "SecurityProfileId"];
        "GET", "security-profiles-summary/{InstanceId}", "ListSecurityProfiles", "H", false, ["InstanceId"];
        "GET", "tags/{resourceArn}", "ListTagsForResource", "H", false, ["resourceArn"];
        "GET", "instance/{InstanceId}/task/template", "ListTaskTemplates", "H", false, ["InstanceId"];
        "GET", "test-cases/{InstanceId}/{TestCaseId}/{TestCaseExecutionId}/records", "ListTestCaseExecutionRecords", "H", false, ["InstanceId", "TestCaseId", "TestCaseExecutionId"];
        "GET", "test-case-executions/{InstanceId}", "ListTestCaseExecutions", "H", false, ["InstanceId"];
        "GET", "test-cases-summary/{InstanceId}", "ListTestCases", "H", false, ["InstanceId"];
        "GET", "traffic-distribution-group/{TrafficDistributionGroupId}/user", "ListTrafficDistributionGroupUsers", "H", false, ["TrafficDistributionGroupId"];
        "GET", "traffic-distribution-groups", "ListTrafficDistributionGroups", "H", false, [];
        "GET", "instance/{InstanceId}/integration-associations/{IntegrationAssociationId}/use-cases", "ListUseCases", "H", false, ["InstanceId", "IntegrationAssociationId"];
        "GET", "user-hierarchy-groups-summary/{InstanceId}", "ListUserHierarchyGroups", "H", false, ["InstanceId"];
        "GET", "users/{InstanceId}/{UserId}/notifications", "ListUserNotifications", "H", false, ["InstanceId", "UserId"];
        "GET", "users/{InstanceId}/{UserId}/proficiencies", "ListUserProficiencies", "H", false, ["InstanceId", "UserId"];
        "GET", "users-summary/{InstanceId}", "ListUsers", "H", false, ["InstanceId"];
        "GET", "views/{InstanceId}/{ViewId}/versions", "ListViewVersions", "H", false, ["InstanceId", "ViewId"];
        "GET", "views/{InstanceId}", "ListViews", "H", false, ["InstanceId"];
        "GET", "workspaces/{InstanceId}/{WorkspaceId}/media", "ListWorkspaceMedia", "H", false, ["InstanceId", "WorkspaceId"];
        "GET", "workspaces/{InstanceId}/{WorkspaceId}/pages", "ListWorkspacePages", "H", false, ["InstanceId", "WorkspaceId"];
        "GET", "workspaces/{InstanceId}", "ListWorkspaces", "H", false, ["InstanceId"];
        "POST", "contact/monitor", "MonitorContact", "H", false, [];
        "POST", "contact/pause", "PauseContact", "H", false, [];
        "PUT", "users/{InstanceId}/{UserId}/status", "PutUserStatus", "H", false, ["InstanceId", "UserId"];
        "DELETE", "phone-number/{PhoneNumberId}", "ReleasePhoneNumber", "H", false, ["PhoneNumberId"];
        "POST", "instance/{InstanceId}/replicate", "ReplicateInstance", "H", false, ["InstanceId"];
        "POST", "contact/resume", "ResumeContact", "H", false, [];
        "POST", "contact/resume-recording", "ResumeContactRecording", "H", false, [];
        "POST", "search-agent-statuses", "SearchAgentStatuses", "H", false, [];
        "POST", "phone-number/search-available", "SearchAvailablePhoneNumbers", "H", false, [];
        "POST", "search-contact-evaluations", "SearchContactEvaluations", "H", false, [];
        "POST", "search-contact-flow-modules", "SearchContactFlowModules", "H", false, [];
        "POST", "search-contact-flows", "SearchContactFlows", "H", false, [];
        "POST", "search-contacts", "SearchContacts", "H", false, [];
        "POST", "search-data-tables", "SearchDataTables", "H", false, [];
        "POST", "search-email-addresses", "SearchEmailAddresses", "H", false, [];
        "POST", "search-evaluation-forms", "SearchEvaluationForms", "H", false, [];
        "POST", "search-hours-of-operation-overrides", "SearchHoursOfOperationOverrides", "H", false, [];
        "POST", "search-hours-of-operations", "SearchHoursOfOperations", "H", false, [];
        "POST", "search-notifications", "SearchNotifications", "H", false, [];
        "POST", "search-predefined-attributes", "SearchPredefinedAttributes", "H", false, [];
        "POST", "search-prompts", "SearchPrompts", "H", false, [];
        "POST", "search-queues", "SearchQueues", "H", false, [];
        "POST", "search-quick-connects", "SearchQuickConnects", "H", false, [];
        "POST", "search-resource-tags", "SearchResourceTags", "H", false, [];
        "POST", "search-routing-profiles", "SearchRoutingProfiles", "H", false, [];
        "POST", "search-security-profiles", "SearchSecurityProfiles", "H", false, [];
        "POST", "search-test-cases", "SearchTestCases", "H", false, [];
        "POST", "search-user-hierarchy-groups", "SearchUserHierarchyGroups", "H", false, [];
        "POST", "search-users", "SearchUsers", "H", false, [];
        "POST", "search-views", "SearchViews", "H", false, [];
        "POST", "vocabulary-summary/{InstanceId}", "SearchVocabularies", "H", false, ["InstanceId"];
        "POST", "search-workspace-associations", "SearchWorkspaceAssociations", "H", false, [];
        "POST", "search-workspaces", "SearchWorkspaces", "H", false, [];
        "POST", "chat-integration-event", "SendChatIntegrationEvent", "H", false, [];
        "PUT", "instance/{InstanceId}/outbound-email", "SendOutboundEmail", "H", false, ["InstanceId"];
        "PUT", "attached-files/{InstanceId}", "StartAttachedFileUpload", "H", false, ["InstanceId"];
        "PUT", "contact/chat", "StartChatContact", "H", false, [];
        "PUT", "contact-evaluations/{InstanceId}", "StartContactEvaluation", "H", false, ["InstanceId"];
        "POST", "contact/start-contact-media-processing", "StartContactMediaProcessing", "H", false, [];
        "POST", "contact/start-recording", "StartContactRecording", "H", false, [];
        "POST", "contact/start-streaming", "StartContactStreaming", "H", false, [];
        "PUT", "contact/email", "StartEmailContact", "H", false, [];
        "PUT", "contact/outbound-chat", "StartOutboundChatContact", "H", false, [];
        "PUT", "contact/outbound-email", "StartOutboundEmailContact", "H", false, [];
        "PUT", "contact/outbound-voice", "StartOutboundVoiceContact", "H", false, [];
        "PUT", "contact/screen-sharing", "StartScreenSharing", "H", false, [];
        "PUT", "contact/task", "StartTaskContact", "H", false, [];
        "PUT", "test-cases/{InstanceId}/{TestCaseId}/start-execution", "StartTestCaseExecution", "H", false, ["InstanceId", "TestCaseId"];
        "PUT", "contact/webrtc", "StartWebRTCContact", "H", false, [];
        "POST", "contact/stop", "StopContact", "H", false, [];
        "POST", "contact/stop-contact-media-processing", "StopContactMediaProcessing", "H", false, [];
        "POST", "contact/stop-recording", "StopContactRecording", "H", false, [];
        "POST", "contact/stop-streaming", "StopContactStreaming", "H", false, [];
        "POST", "test-cases/{InstanceId}/{TestCaseId}/{TestCaseExecutionId}/stop-execution", "StopTestCaseExecution", "H", false, ["InstanceId", "TestCaseId", "TestCaseExecutionId"];
        "POST", "contact-evaluations/{InstanceId}/{EvaluationId}/submit", "SubmitContactEvaluation", "H", false, ["InstanceId", "EvaluationId"];
        "POST", "contact/suspend-recording", "SuspendContactRecording", "H", false, [];
        "POST", "contact/tags", "TagContact", "H", false, [];
        "POST", "tags/{resourceArn}", "TagResource", "H", false, ["resourceArn"];
        "POST", "contact/transfer", "TransferContact", "H", false, [];
        "DELETE", "contact/tags/{InstanceId}/{ContactId}", "UntagContact", "H", false, ["InstanceId", "ContactId"];
        "DELETE", "tags/{resourceArn}", "UntagResource", "H", false, ["resourceArn"];
        "POST", "agent-status/{InstanceId}/{AgentStatusId}", "UpdateAgentStatus", "H", false, ["InstanceId", "AgentStatusId"];
        "POST", "authentication-profiles/{InstanceId}/{AuthenticationProfileId}", "UpdateAuthenticationProfile", "H", false, ["InstanceId", "AuthenticationProfileId"];
        "POST", "contacts/{InstanceId}/{ContactId}", "UpdateContact", "H", false, ["InstanceId", "ContactId"];
        "POST", "contact/attributes", "UpdateContactAttributes", "H", false, [];
        "POST", "contact-evaluations/{InstanceId}/{EvaluationId}", "UpdateContactEvaluation", "H", false, ["InstanceId", "EvaluationId"];
        "POST", "contact-flows/{InstanceId}/{ContactFlowId}/content", "UpdateContactFlowContent", "H", false, ["InstanceId", "ContactFlowId"];
        "POST", "contact-flows/{InstanceId}/{ContactFlowId}/metadata", "UpdateContactFlowMetadata", "H", false, ["InstanceId", "ContactFlowId"];
        "POST", "contact-flow-modules/{InstanceId}/{ContactFlowModuleId}/alias/{AliasId}", "UpdateContactFlowModuleAlias", "H", false, ["InstanceId", "ContactFlowModuleId", "AliasId"];
        "POST", "contact-flow-modules/{InstanceId}/{ContactFlowModuleId}/content", "UpdateContactFlowModuleContent", "H", false, ["InstanceId", "ContactFlowModuleId"];
        "POST", "contact-flow-modules/{InstanceId}/{ContactFlowModuleId}/metadata", "UpdateContactFlowModuleMetadata", "H", false, ["InstanceId", "ContactFlowModuleId"];
        "POST", "contact-flows/{InstanceId}/{ContactFlowId}/name", "UpdateContactFlowName", "H", false, ["InstanceId", "ContactFlowId"];
        "POST", "contacts/{InstanceId}/{ContactId}/routing-data", "UpdateContactRoutingData", "H", false, ["InstanceId", "ContactId"];
        "POST", "contact/schedule", "UpdateContactSchedule", "H", false, [];
        "POST", "data-tables/{InstanceId}/{DataTableId}/attributes/{AttributeName}", "UpdateDataTableAttribute", "H", false, ["InstanceId", "DataTableId", "AttributeName"];
        "POST", "data-tables/{InstanceId}/{DataTableId}", "UpdateDataTableMetadata", "H", false, ["InstanceId", "DataTableId"];
        "POST", "data-tables/{InstanceId}/{DataTableId}/values/update-primary", "UpdateDataTablePrimaryValues", "H", false, ["InstanceId", "DataTableId"];
        "POST", "email-addresses/{InstanceId}/{EmailAddressId}", "UpdateEmailAddressMetadata", "H", false, ["InstanceId", "EmailAddressId"];
        "PUT", "evaluation-forms/{InstanceId}/{EvaluationFormId}", "UpdateEvaluationForm", "H", false, ["InstanceId", "EvaluationFormId"];
        "POST", "hours-of-operations/{InstanceId}/{HoursOfOperationId}", "UpdateHoursOfOperation", "H", false, ["InstanceId", "HoursOfOperationId"];
        "POST", "hours-of-operations/{InstanceId}/{HoursOfOperationId}/overrides/{HoursOfOperationOverrideId}", "UpdateHoursOfOperationOverride", "H", false, ["InstanceId", "HoursOfOperationId", "HoursOfOperationOverrideId"];
        "POST", "instance/{InstanceId}/attribute/{AttributeType}", "UpdateInstanceAttribute", "H", false, ["InstanceId", "AttributeType"];
        "POST", "instance/{InstanceId}/storage-config/{AssociationId}", "UpdateInstanceStorageConfig", "H", false, ["InstanceId", "AssociationId"];
        "POST", "notifications/{InstanceId}/{NotificationId}", "UpdateNotificationContent", "H", false, ["InstanceId", "NotificationId"];
        "POST", "contact/update-participant-authentication", "UpdateParticipantAuthentication", "H", false, [];
        "PUT", "contact/participant-role-config/{InstanceId}/{ContactId}", "UpdateParticipantRoleConfig", "R501", false, ["InstanceId", "ContactId"];
        "PUT", "phone-number/{PhoneNumberId}", "UpdatePhoneNumber", "H", false, ["PhoneNumberId"];
        "PUT", "phone-number/{PhoneNumberId}/metadata", "UpdatePhoneNumberMetadata", "H", false, ["PhoneNumberId"];
        "POST", "predefined-attributes/{InstanceId}/{Name}", "UpdatePredefinedAttribute", "H", false, ["InstanceId", "Name"];
        "POST", "prompts/{InstanceId}/{PromptId}", "UpdatePrompt", "H", false, ["InstanceId", "PromptId"];
        "POST", "queues/{InstanceId}/{QueueId}/hours-of-operation", "UpdateQueueHoursOfOperation", "H", false, ["InstanceId", "QueueId"];
        "POST", "queues/{InstanceId}/{QueueId}/max-contacts", "UpdateQueueMaxContacts", "H", false, ["InstanceId", "QueueId"];
        "POST", "queues/{InstanceId}/{QueueId}/name", "UpdateQueueName", "H", false, ["InstanceId", "QueueId"];
        "POST", "queues/{InstanceId}/{QueueId}/outbound-caller-config", "UpdateQueueOutboundCallerConfig", "H", false, ["InstanceId", "QueueId"];
        "POST", "queues/{InstanceId}/{QueueId}/outbound-email-config", "UpdateQueueOutboundEmailConfig", "H", false, ["InstanceId", "QueueId"];
        "POST", "queues/{InstanceId}/{QueueId}/status", "UpdateQueueStatus", "H", false, ["InstanceId", "QueueId"];
        "POST", "quick-connects/{InstanceId}/{QuickConnectId}/config", "UpdateQuickConnectConfig", "H", false, ["InstanceId", "QuickConnectId"];
        "POST", "quick-connects/{InstanceId}/{QuickConnectId}/name", "UpdateQuickConnectName", "H", false, ["InstanceId", "QuickConnectId"];
        "POST", "routing-profiles/{InstanceId}/{RoutingProfileId}/agent-availability-timer", "UpdateRoutingProfileAgentAvailabilityTimer", "H", false, ["InstanceId", "RoutingProfileId"];
        "POST", "routing-profiles/{InstanceId}/{RoutingProfileId}/concurrency", "UpdateRoutingProfileConcurrency", "H", false, ["InstanceId", "RoutingProfileId"];
        "POST", "routing-profiles/{InstanceId}/{RoutingProfileId}/default-outbound-queue", "UpdateRoutingProfileDefaultOutboundQueue", "H", false, ["InstanceId", "RoutingProfileId"];
        "POST", "routing-profiles/{InstanceId}/{RoutingProfileId}/name", "UpdateRoutingProfileName", "H", false, ["InstanceId", "RoutingProfileId"];
        "POST", "routing-profiles/{InstanceId}/{RoutingProfileId}/queues", "UpdateRoutingProfileQueues", "H", false, ["InstanceId", "RoutingProfileId"];
        "PUT", "rules/{InstanceId}/{RuleId}", "UpdateRule", "H", false, ["InstanceId", "RuleId"];
        "POST", "security-profiles/{InstanceId}/{SecurityProfileId}", "UpdateSecurityProfile", "H", false, ["InstanceId", "SecurityProfileId"];
        "POST", "instance/{InstanceId}/task/template/{TaskTemplateId}", "UpdateTaskTemplate", "H", false, ["InstanceId", "TaskTemplateId"];
        "POST", "test-cases/{InstanceId}/{TestCaseId}", "UpdateTestCase", "H", false, ["InstanceId", "TestCaseId"];
        "PUT", "traffic-distribution/{Id}", "UpdateTrafficDistribution", "H", false, ["Id"];
        "POST", "users/{InstanceId}/{UserId}/config", "UpdateUserConfig", "H", false, ["InstanceId", "UserId"];
        "POST", "users/{InstanceId}/{UserId}/hierarchy", "UpdateUserHierarchy", "H", false, ["InstanceId", "UserId"];
        "POST", "user-hierarchy-groups/{InstanceId}/{HierarchyGroupId}/name", "UpdateUserHierarchyGroupName", "H", false, ["InstanceId", "HierarchyGroupId"];
        "POST", "user-hierarchy-structure/{InstanceId}", "UpdateUserHierarchyStructure", "H", false, ["InstanceId"];
        "POST", "users/{InstanceId}/{UserId}/identity-info", "UpdateUserIdentityInfo", "H", false, ["InstanceId", "UserId"];
        "POST", "users/{InstanceId}/{UserId}/notifications/{NotificationId}", "UpdateUserNotificationStatus", "H", false, ["InstanceId", "UserId", "NotificationId"];
        "POST", "users/{InstanceId}/{UserId}/phone-config", "UpdateUserPhoneConfig", "H", false, ["InstanceId", "UserId"];
        "POST", "users/{InstanceId}/{UserId}/proficiencies", "UpdateUserProficiencies", "H", false, ["InstanceId", "UserId"];
        "POST", "users/{InstanceId}/{UserId}/routing-profile", "UpdateUserRoutingProfile", "H", false, ["InstanceId", "UserId"];
        "POST", "users/{InstanceId}/{UserId}/security-profiles", "UpdateUserSecurityProfiles", "H", false, ["InstanceId", "UserId"];
        "POST", "views/{InstanceId}/{ViewId}", "UpdateViewContent", "H", false, ["InstanceId", "ViewId"];
        "POST", "views/{InstanceId}/{ViewId}/metadata", "UpdateViewMetadata", "H", false, ["InstanceId", "ViewId"];
        "POST", "workspaces/{InstanceId}/{WorkspaceId}/metadata", "UpdateWorkspaceMetadata", "H", false, ["InstanceId", "WorkspaceId"];
        "POST", "workspaces/{InstanceId}/{WorkspaceId}/pages/{Page}", "UpdateWorkspacePage", "H", false, ["InstanceId", "WorkspaceId", "Page"];
        "POST", "workspaces/{InstanceId}/{WorkspaceId}/theme", "UpdateWorkspaceTheme", "H", false, ["InstanceId", "WorkspaceId"];
        "POST", "workspaces/{InstanceId}/{WorkspaceId}/visibility", "UpdateWorkspaceVisibility", "H", false, ["InstanceId", "WorkspaceId"];]
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
    // Instance-family ops the surface wave wires up (moto absent; botocore
    // shapes are the contract).
    /// instance -> flow_id -> created versions (1 = seeded live version).
    contact_flow_versions: HashMap<String, HashMap<String, Vec<i64>>>,
    contact_flow_module_versions: HashMap<String, HashMap<String, Vec<i64>>>,
    view_versions: HashMap<String, HashMap<String, Vec<i64>>>,
    /// resource-id -> flow arn per instance (AssociateFlow family).
    flow_associations: HashMap<String, HashMap<String, Map<String, Value>>>,
    /// user id -> registration record per instance (push notifications).
    push_notification_registrations: HashMap<String, HashMap<String, Value>>,
    /// instance -> user -> proficiencies [({AttributeName, AttributeValue, Level})]
    user_proficiencies: HashMap<String, HashMap<String, Vec<Value>>>,
    /// instance -> entity arn -> associated security profile ids.
    entity_security_profiles: HashMap<String, HashMap<String, Vec<Value>>>,
    /// instance -> queue id -> associated email address ids.
    queue_email_associations: HashMap<String, HashMap<String, Vec<String>>>,
    /// instance -> email id -> alias config (AssociateEmailAddressAlias).
    email_aliases: HashMap<String, HashMap<String, Map<String, Value>>>,
    /// instance -> workspace -> resource-association records
    workspace_associations: HashMap<String, HashMap<String, Vec<Value>>>,
    /// instance -> {page: page record} per workspace
    workspace_pages: HashMap<String, HashMap<String, HashMap<String, Value>>>,
    /// instance -> [media records] per workspace
    workspace_media: HashMap<String, HashMap<String, Vec<Value>>>,
    /// instance -> attached file id -> file record.
    attached_files: HashMap<String, HashMap<String, Value>>,
    /// instance -> data table -> {values: [...], primary-values: [...]}.
    data_table_values: HashMap<String, HashMap<String, Map<String, Value>>>,
    /// instance -> language code -> vocabulary id (default vocabulary).
    default_vocabularies: HashMap<String, Map<String, Value>>,
    /// instance -> phone-number id -> {"ContactFlowId": ...} (contact flow link).
    phone_contact_flows: HashMap<String, Map<String, Value>>,
    /// instance -> authentication profile id -> profile record.
    authentication_profiles: HashMap<String, HashMap<String, Value>>,
    /// instance -> user id -> notification id -> notification record.
    user_notifications: HashMap<String, HashMap<String, HashMap<String, Value>>>,
    /// instance -> hours-of-operation id -> associated parent ids.
    child_hours_of_operations: HashMap<String, HashMap<String, Vec<String>>>,
    /// test case id -> [execution records] per instance.
    test_case_executions: HashMap<String, HashMap<String, Vec<Value>>>,
    /// contact id -> streaming id per instance (StartContactStreaming).
    contact_streaming_ids: HashMap<String, Map<String, Value>>,
    /// traffic-distribution group id -> [users] (AssociateTDGUser family).
    traffic_distribution_group_users: HashMap<String, Vec<Value>>,
    /// instance -> user id -> runtime fields (PutUserStatus, UpdateUserConfig).
    user_runtime: HashMap<String, HashMap<String, Map<String, Value>>>,
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
        // -- Phone numbers ----------------------------------------------------
        "ClaimPhoneNumber" => op_claim_phone_number(st, body, &req.region, req.account),
        "DescribePhoneNumber" => op_describe_phone_number(st, param(params, "PhoneNumberId")),
        "UpdatePhoneNumber" => op_update_phone_number(st, param(params, "PhoneNumberId"), body),
        "ReleasePhoneNumber" => op_release_phone_number(st, param(params, "PhoneNumberId")),
        "ListPhoneNumbers" => op_list_phone_numbers(st, param(params, "InstanceId")),
        "ListPhoneNumbersV2" => op_list_phone_numbers_v2(st),
        "SearchAvailablePhoneNumbers" => op_search_available_phone_numbers(st, body),
        // -- Prompts ------------------------------------------------------------
        "CreatePrompt" => op_create_prompt(st, param(params, "InstanceId"), body),
        "DescribePrompt" => {
            op_describe_prompt(st, param(params, "InstanceId"), param(params, "PromptId"))
        }
        "UpdatePrompt" => op_update_prompt(
            st,
            param(params, "InstanceId"),
            param(params, "PromptId"),
            body,
        ),
        "DeletePrompt" => {
            op_delete_prompt(st, param(params, "InstanceId"), param(params, "PromptId"))
        }
        "ListPrompts" => op_list_prompts(st, param(params, "InstanceId")),
        // -- Views ---------------------------------------------------------------
        "CreateView" => op_create_view(st, param(params, "InstanceId"), body),
        "DescribeView" => {
            op_describe_view(st, param(params, "InstanceId"), param(params, "ViewId"))
        }
        "UpdateViewContent" => op_update_view_content(
            st,
            param(params, "InstanceId"),
            param(params, "ViewId"),
            body,
        ),
        "UpdateViewMetadata" => op_update_view_metadata(
            st,
            param(params, "InstanceId"),
            param(params, "ViewId"),
            body,
        ),
        "DeleteView" => op_delete_view(st, param(params, "InstanceId"), param(params, "ViewId")),
        "ListViews" => op_list_views(st, param(params, "InstanceId")),
        // -- Vocabularies -----------------------------------------------------------
        "CreateVocabulary" => op_create_vocabulary(st, param(params, "InstanceId"), body),
        "DescribeVocabulary" => op_describe_vocabulary(
            st,
            param(params, "InstanceId"),
            param(params, "VocabularyId"),
        ),
        "DeleteVocabulary" => op_delete_vocabulary(
            st,
            param(params, "InstanceId"),
            param(params, "VocabularyId"),
        ),
        "SearchVocabularies" => op_search_vocabularies(st, param(params, "InstanceId"), body),
        "ListDefaultVocabularies" => op_list_default_vocabularies(st, param(params, "InstanceId")),
        // -- Rules -----------------------------------------------------------------
        "CreateRule" => op_create_rule(st, param(params, "InstanceId"), body),
        "DescribeRule" => {
            op_describe_rule(st, param(params, "InstanceId"), param(params, "RuleId"))
        }
        "UpdateRule" => op_update_rule(
            st,
            param(params, "InstanceId"),
            param(params, "RuleId"),
            body,
        ),
        "DeleteRule" => op_delete_rule(st, param(params, "InstanceId"), param(params, "RuleId")),
        "ListRules" => op_list_rules(st, param(params, "InstanceId")),
        // -- Evaluation forms --------------------------------------------------------
        "CreateEvaluationForm" => op_create_evaluation_form(st, param(params, "InstanceId"), body),
        "DescribeEvaluationForm" => op_describe_evaluation_form(
            st,
            param(params, "InstanceId"),
            param(params, "EvaluationFormId"),
        ),
        "UpdateEvaluationForm" => op_update_evaluation_form(
            st,
            param(params, "InstanceId"),
            param(params, "EvaluationFormId"),
            body,
        ),
        "ActivateEvaluationForm" => op_activate_evaluation_form(
            st,
            param(params, "InstanceId"),
            param(params, "EvaluationFormId"),
            true,
        ),
        "DeactivateEvaluationForm" => op_activate_evaluation_form(
            st,
            param(params, "InstanceId"),
            param(params, "EvaluationFormId"),
            false,
        ),
        "DeleteEvaluationForm" => op_delete_evaluation_form(
            st,
            param(params, "InstanceId"),
            param(params, "EvaluationFormId"),
        ),
        "ListEvaluationForms" => op_list_evaluation_forms(st, param(params, "InstanceId")),
        "ListEvaluationFormVersions" => op_list_evaluation_form_versions(
            st,
            param(params, "InstanceId"),
            param(params, "EvaluationFormId"),
        ),
        // -- Predefined attributes -----------------------------------------------------
        "CreatePredefinedAttribute" => {
            op_create_predefined_attribute(st, param(params, "InstanceId"), body)
        }
        "DescribePredefinedAttribute" => {
            op_describe_predefined_attribute(st, param(params, "InstanceId"), param(params, "Name"))
        }
        "UpdatePredefinedAttribute" => op_update_predefined_attribute(
            st,
            param(params, "InstanceId"),
            param(params, "Name"),
            body,
        ),
        "DeletePredefinedAttribute" => {
            op_delete_predefined_attribute(st, param(params, "InstanceId"), param(params, "Name"))
        }
        "ListPredefinedAttributes" => {
            op_list_predefined_attributes(st, param(params, "InstanceId"))
        }
        // -- Task templates ---------------------------------------------------------------
        "CreateTaskTemplate" => op_create_task_template(st, param(params, "InstanceId"), body),
        "GetTaskTemplate" => op_get_task_template(
            st,
            param(params, "InstanceId"),
            param(params, "TaskTemplateId"),
        ),
        "UpdateTaskTemplate" => op_update_task_template(
            st,
            param(params, "InstanceId"),
            param(params, "TaskTemplateId"),
            body,
        ),
        "DeleteTaskTemplate" => op_delete_task_template(
            st,
            param(params, "InstanceId"),
            param(params, "TaskTemplateId"),
        ),
        "ListTaskTemplates" => op_list_task_templates(st, param(params, "InstanceId")),
        // -- Integration associations + use cases ------------------------------------------
        "CreateIntegrationAssociation" => {
            op_create_integration_association(st, param(params, "InstanceId"), body)
        }
        "ListIntegrationAssociations" => {
            op_list_integration_associations(st, param(params, "InstanceId"))
        }
        "CreateUseCase" => op_create_use_case(
            st,
            param(params, "InstanceId"),
            param(params, "IntegrationAssociationId"),
            body,
        ),
        "DeleteUseCase" => {
            op_delete_use_case(st, param(params, "InstanceId"), param(params, "UseCaseId"))
        }
        "ListUseCases" => op_list_use_cases(
            st,
            param(params, "InstanceId"),
            param(params, "IntegrationAssociationId"),
        ),
        // -- Email addresses -------------------------------------------------------------
        "CreateEmailAddress" => op_create_email_address(st, param(params, "InstanceId"), body),
        "DescribeEmailAddress" => op_describe_email_address(
            st,
            param(params, "InstanceId"),
            param(params, "EmailAddressId"),
        ),
        "DeleteEmailAddress" => op_delete_email_address(
            st,
            param(params, "InstanceId"),
            param(params, "EmailAddressId"),
        ),
        // -- Notifications ---------------------------------------------------------------
        "CreateNotification" => op_create_notification(st, param(params, "InstanceId"), body),
        "DescribeNotification" => op_describe_notification(
            st,
            param(params, "InstanceId"),
            param(params, "NotificationId"),
        ),
        "DeleteNotification" => op_delete_notification(
            st,
            param(params, "InstanceId"),
            param(params, "NotificationId"),
        ),
        "ListNotifications" => op_list_notifications(st, param(params, "InstanceId")),
        // -- Workspaces ---------------------------------------------------------------------
        "CreateWorkspace" => op_create_workspace(st, param(params, "InstanceId"), body),
        "DescribeWorkspace" => op_describe_workspace(
            st,
            param(params, "InstanceId"),
            param(params, "WorkspaceId"),
        ),
        "DeleteWorkspace" => op_delete_workspace(
            st,
            param(params, "InstanceId"),
            param(params, "WorkspaceId"),
        ),
        "ListWorkspaces" => op_list_workspaces(st, param(params, "InstanceId")),
        // -- Test cases --------------------------------------------------------------------
        "CreateTestCase" => op_create_test_case(st, param(params, "InstanceId"), body),
        "DescribeTestCase" => {
            op_describe_test_case(st, param(params, "InstanceId"), param(params, "TestCaseId"))
        }
        "UpdateTestCase" => op_update_test_case(
            st,
            param(params, "InstanceId"),
            param(params, "TestCaseId"),
            body,
        ),
        "DeleteTestCase" => {
            op_delete_test_case(st, param(params, "InstanceId"), param(params, "TestCaseId"))
        }
        "ListTestCases" => op_list_test_cases(st, param(params, "InstanceId")),
        // -- Data tables --------------------------------------------------------------------
        "CreateDataTable" => op_create_data_table(st, param(params, "InstanceId"), body),
        "DescribeDataTable" => op_describe_data_table(
            st,
            param(params, "InstanceId"),
            param(params, "DataTableId"),
        ),
        "DeleteDataTable" => op_delete_data_table(
            st,
            param(params, "InstanceId"),
            param(params, "DataTableId"),
        ),
        "ListDataTables" => op_list_data_tables(st, param(params, "InstanceId")),
        "CreateDataTableAttribute" => op_create_data_table_attribute(
            st,
            param(params, "InstanceId"),
            param(params, "DataTableId"),
            body,
        ),
        "DescribeDataTableAttribute" => op_describe_data_table_attribute(
            st,
            param(params, "InstanceId"),
            param(params, "DataTableId"),
            param(params, "AttributeName"),
        ),
        "ListDataTableAttributes" => op_list_data_table_attributes(
            st,
            param(params, "InstanceId"),
            param(params, "DataTableId"),
        ),
        "UpdateDataTableAttribute" => op_update_data_table_attribute(
            st,
            param(params, "InstanceId"),
            param(params, "DataTableId"),
            param(params, "AttributeName"),
            body,
        ),
        "DeleteDataTableAttribute" => op_delete_data_table_attribute(
            st,
            param(params, "InstanceId"),
            param(params, "DataTableId"),
            param(params, "AttributeName"),
        ),
        // -- Traffic distribution groups ----------------------------------------------------
        "CreateTrafficDistributionGroup" => {
            op_create_traffic_distribution_group(st, body, &req.region, req.account)
        }
        "DescribeTrafficDistributionGroup" => {
            op_describe_traffic_distribution_group(st, param(params, "TrafficDistributionGroupId"))
        }
        "DeleteTrafficDistributionGroup" => {
            op_delete_traffic_distribution_group(st, param(params, "TrafficDistributionGroupId"))
        }
        "ListTrafficDistributionGroups" => op_list_traffic_distribution_groups(st, query),
        "ListContactEvaluations" => op_list_evaluations(st, param(params, "InstanceId")),
        // -- Search ops (moto ignores SearchCriteria, returns everything) --
        "SearchUsers" => {
            let instance_id = match body_str(body, "InstanceId") {
                Ok(v) => v,
                Err(resp) => return resp,
            };
            op_search(st, &instance_id, "Users", |st2, iid| {
                st2.users.get(iid).map(|m| m.values().cloned().collect())
            })
        }
        "SearchAgentStatuses" => {
            let instance_id = match body_str(body, "InstanceId") {
                Ok(v) => v,
                Err(resp) => return resp,
            };
            op_search(st, &instance_id, "AgentStatuses", |st2, iid| {
                st2.agent_statuses
                    .get(iid)
                    .map(|m| m.values().cloned().collect())
            })
        }
        "SearchContactFlows" => {
            let instance_id = match body_str(body, "InstanceId") {
                Ok(v) => v,
                Err(resp) => return resp,
            };
            op_search(st, &instance_id, "ContactFlows", |st2, iid| {
                st2.contact_flows
                    .get(iid)
                    .map(|m| m.values().cloned().collect())
            })
        }
        "SearchContactFlowModules" => {
            let instance_id = match body_str(body, "InstanceId") {
                Ok(v) => v,
                Err(resp) => return resp,
            };
            op_search(st, &instance_id, "ContactFlowModules", |st2, iid| {
                st2.contact_flow_modules
                    .get(iid)
                    .map(|m| m.values().cloned().collect())
            })
        }
        "SearchQuickConnects" => {
            let instance_id = match body_str(body, "InstanceId") {
                Ok(v) => v,
                Err(resp) => return resp,
            };
            op_search(st, &instance_id, "QuickConnects", |st2, iid| {
                st2.quick_connects
                    .get(iid)
                    .map(|m| m.values().cloned().collect())
            })
        }
        "SearchPrompts" => {
            let instance_id = match body_str(body, "InstanceId") {
                Ok(v) => v,
                Err(resp) => return resp,
            };
            op_search(st, &instance_id, "Prompts", |st2, iid| {
                st2.prompts.get(iid).map(|m| m.values().cloned().collect())
            })
        }
        "SearchRoutingProfiles" => {
            let instance_id = match body_str(body, "InstanceId") {
                Ok(v) => v,
                Err(resp) => return resp,
            };
            op_search(st, &instance_id, "RoutingProfiles", |st2, iid| {
                st2.routing_profiles
                    .get(iid)
                    .map(|m| m.values().cloned().collect())
            })
        }
        "SearchSecurityProfiles" => {
            let instance_id = match body_str(body, "InstanceId") {
                Ok(v) => v,
                Err(resp) => return resp,
            };
            op_search(st, &instance_id, "SecurityProfiles", |st2, iid| {
                st2.security_profiles
                    .get(iid)
                    .map(|m| m.values().cloned().collect())
            })
        }
        "SearchHoursOfOperations" => {
            let instance_id = match body_str(body, "InstanceId") {
                Ok(v) => v,
                Err(resp) => return resp,
            };
            op_search(st, &instance_id, "HoursOfOperations", |st2, iid| {
                st2.hours_of_operations
                    .get(iid)
                    .map(|m| m.values().cloned().collect())
            })
        }
        "SearchUserHierarchyGroups" => {
            let instance_id = match body_str(body, "InstanceId") {
                Ok(v) => v,
                Err(resp) => return resp,
            };
            op_search(st, &instance_id, "UserHierarchyGroups", |st2, iid| {
                st2.user_hierarchy_groups
                    .get(iid)
                    .map(|m| m.values().cloned().collect())
            })
        }
        "SearchQueues" => {
            let instance_id = match body_str(body, "InstanceId") {
                Ok(v) => v,
                Err(resp) => return resp,
            };
            op_search(st, &instance_id, "Queues", |st2, iid| {
                st2.queues.get(iid).map(|m| {
                    m.values()
                        .map(|q| {
                            let mut q2 = q.clone();
                            q2["QueueType"] = json!("STANDARD");
                            q2
                        })
                        .collect()
                })
            })
        }
        "SearchPredefinedAttributes" => {
            let instance_id = match body_str(body, "InstanceId") {
                Ok(v) => v,
                Err(resp) => return resp,
            };
            op_search(st, &instance_id, "PredefinedAttributes", |st2, iid| {
                st2.predefined_attributes
                    .get(iid)
                    .map(|m| m.values().map(pa_to_dict).collect())
            })
        }

        // -- Surface wave: instance-family ops (botocore shapes; moto absent) --
        "AssociateContactWithUser" => op_associate_contact_with_user(
            st,
            param(params, "InstanceId"),
            param(params, "ContactId"),
            body,
        ),
        "AssociateDefaultVocabulary" => op_associate_default_vocabulary(
            st,
            param(params, "InstanceId"),
            param(params, "LanguageCode"),
            body,
        ),
        "AssociateEmailAddressAlias" => op_associate_email_alias(
            st,
            param(params, "InstanceId"),
            param(params, "EmailAddressId"),
            body,
            true,
        ),
        "AssociateFlow" => op_associate_flow(st, param(params, "InstanceId"), body),
        "AssociateHoursOfOperations" => op_associate_hours_of_operations(
            st,
            param(params, "InstanceId"),
            param(params, "HoursOfOperationId"),
            body,
        ),
        "AssociateLexBot" => op_associate_lex_bot(st, param(params, "InstanceId"), body),
        "AssociatePhoneNumberContactFlow" => {
            op_associate_phone_contact_flow(st, param(params, "PhoneNumberId"), body)
        }
        "AssociateQueueEmailAddresses" => op_associate_queue_emails(
            st,
            param(params, "InstanceId"),
            param(params, "QueueId"),
            body,
            true,
        ),
        "AssociateSecurityProfiles" => {
            op_associate_security_profiles(st, param(params, "InstanceId"), body, true)
        }
        "AssociateTrafficDistributionGroupUser" => {
            op_associate_tdg_user(st, body, param(params, "TrafficDistributionGroupId"), true)
        }
        "AssociateUserProficiencies" => op_update_user_proficiencies_inner(
            st,
            param(params, "InstanceId"),
            param(params, "UserId"),
            body,
            true,
        ),
        "AssociateWorkspace" => op_associate_workspace(
            st,
            param(params, "InstanceId"),
            param(params, "WorkspaceId"),
            body,
            true,
        ),
        "BatchAssociateAnalyticsDataSet" => {
            op_batch_associate_analytics_data_set(st, param(params, "InstanceId"), body, true)
        }
        "BatchCreateDataTableValue" => op_batch_data_table_values(
            st,
            param(params, "InstanceId"),
            body,
            param(params, "DataTableId"),
            "create",
        ),
        "BatchDeleteDataTableValue" => op_batch_data_table_values(
            st,
            param(params, "InstanceId"),
            body,
            param(params, "DataTableId"),
            "delete",
        ),
        "BatchDescribeDataTableValue" => op_batch_data_table_values(
            st,
            param(params, "InstanceId"),
            body,
            param(params, "DataTableId"),
            "describe",
        ),
        "BatchDisassociateAnalyticsDataSet" => {
            op_batch_associate_analytics_data_set(st, param(params, "InstanceId"), body, false)
        }
        "BatchGetAttachedFileMetadata" => {
            op_batch_get_attached_file_metadata(st, param(params, "InstanceId"), body)
        }
        "BatchGetFlowAssociation" => {
            op_batch_get_flow_association(st, param(params, "InstanceId"), body)
        }
        "BatchPutContact" => op_batch_put_contact(st, param(params, "InstanceId"), body),
        "BatchUpdateDataTableValue" => op_batch_data_table_values(
            st,
            param(params, "InstanceId"),
            body,
            param(params, "DataTableId"),
            "update",
        ),
        "CompleteAttachedFileUpload" => op_complete_attached_file_upload(
            st,
            param(params, "InstanceId"),
            param(params, "FileId"),
        ),
        "CreateContactFlowModuleVersion" => op_create_contact_flow_module_version(
            st,
            param(params, "InstanceId"),
            param(params, "ContactFlowModuleId"),
        ),
        "CreateContactFlowVersion" => op_create_contact_flow_version(
            st,
            param(params, "InstanceId"),
            param(params, "ContactFlowId"),
        ),
        "CreateParticipant" => op_create_participant(
            st,
            param(params, "InstanceId"),
            param(params, "ContactId"),
            body,
        ),
        "CreatePersistentContactAssociation" => op_create_persistent_contact_association(
            st,
            param(params, "InstanceId"),
            param(params, "InitialContactId"),
            body,
        ),
        "CreatePushNotificationRegistration" => {
            op_create_push_notification_registration(st, param(params, "InstanceId"), body)
        }
        "CreateViewVersion" => op_create_view_version(
            st,
            param(params, "InstanceId"),
            param(params, "ViewId"),
            body,
        ),
        "CreateWorkspacePage" => op_create_workspace_page(
            st,
            param(params, "InstanceId"),
            param(params, "WorkspaceId"),
            body,
        ),
        "DeleteAttachedFile" => {
            op_delete_attached_file(st, param(params, "InstanceId"), param(params, "FileId"))
        }
        "DeleteContactFlowModuleVersion" => op_delete_contact_flow_module_version(
            st,
            param(params, "InstanceId"),
            param(params, "ContactFlowModuleId"),
            param(params, "ContactFlowModuleVersion"),
        ),
        "DeleteContactFlowVersion" => op_delete_contact_flow_version(
            st,
            param(params, "InstanceId"),
            param(params, "ContactFlowId"),
            param(params, "ContactFlowVersion"),
        ),
        "DeleteIntegrationAssociation" => op_delete_integration_association(
            st,
            param(params, "InstanceId"),
            param(params, "IntegrationAssociationId"),
        ),
        "DeletePushNotificationRegistration" => op_delete_push_notification_registration(
            st,
            param(params, "InstanceId"),
            param(params, "RegistrationId"),
        ),
        "DeleteViewVersion" => op_delete_view_version(
            st,
            param(params, "InstanceId"),
            param(params, "ViewId"),
            param(params, "ViewVersion"),
        ),
        "DeleteWorkspaceMedia" => op_delete_workspace_media(
            st,
            param(params, "InstanceId"),
            param(params, "WorkspaceId"),
            query,
        ),
        "DeleteWorkspacePage" => op_delete_workspace_page(
            st,
            param(params, "InstanceId"),
            param(params, "WorkspaceId"),
            param(params, "Page"),
        ),
        "DescribeAuthenticationProfile" => op_describe_authentication_profile(
            st,
            param(params, "InstanceId"),
            param(params, "AuthenticationProfileId"),
        ),
        "DescribeInstanceStorageConfig" => op_describe_instance_storage_config(
            st,
            param(params, "InstanceId"),
            param(params, "AssociationId"),
        ),
        "DisassociateEmailAddressAlias" => op_associate_email_alias(
            st,
            param(params, "InstanceId"),
            param(params, "EmailAddressId"),
            body,
            false,
        ),
        "DisassociateFlow" => op_disassociate_flow(
            st,
            param(params, "InstanceId"),
            param(params, "ResourceId"),
            param(params, "ResourceType"),
        ),
        "DisassociateHoursOfOperations" => op_disassociate_hours_of_operations(
            st,
            param(params, "InstanceId"),
            param(params, "HoursOfOperationId"),
            body,
        ),
        "DisassociateLexBot" => op_disassociate_lex_bot(st, param(params, "InstanceId"), query),
        "DisassociatePhoneNumberContactFlow" => {
            op_disassociate_phone_contact_flow(st, param(params, "PhoneNumberId"), body)
        }
        "DisassociateQueueEmailAddresses" => op_associate_queue_emails(
            st,
            param(params, "InstanceId"),
            param(params, "QueueId"),
            body,
            false,
        ),
        "DisassociateSecurityProfiles" => {
            op_associate_security_profiles(st, param(params, "InstanceId"), body, false)
        }
        "DisassociateTrafficDistributionGroupUser" => {
            op_associate_tdg_user(st, body, param(params, "TrafficDistributionGroupId"), false)
        }
        "DisassociateUserProficiencies" => op_disassociate_user_proficiencies(
            st,
            param(params, "InstanceId"),
            param(params, "UserId"),
            body,
        ),
        "DisassociateWorkspace" => op_associate_workspace(
            st,
            param(params, "InstanceId"),
            param(params, "WorkspaceId"),
            body,
            false,
        ),
        "DismissUserContact" => op_dismiss_user_contact(
            st,
            param(params, "InstanceId"),
            param(params, "UserId"),
            body,
        ),
        "EvaluateDataTableValues" => op_evaluate_data_table_values(
            st,
            param(params, "InstanceId"),
            body,
            param(params, "DataTableId"),
        ),
        "GetAttachedFile" => {
            op_get_attached_file(st, param(params, "InstanceId"), param(params, "FileId"))
        }
        "GetContactMetrics" => op_get_contact_metrics(st, body),
        "GetCurrentMetricData" => op_get_current_metric_data(st, param(params, "InstanceId"), body),
        "GetCurrentUserData" => op_get_current_user_data(st, param(params, "InstanceId"), body),
        "GetEffectiveHoursOfOperations" => op_get_effective_hours_of_operations(
            st,
            param(params, "InstanceId"),
            param(params, "HoursOfOperationId"),
            query,
        ),
        "GetFederationToken" => op_get_federation_token(st, param(params, "InstanceId")),
        "GetFlowAssociation" => op_get_flow_association(
            st,
            param(params, "InstanceId"),
            param(params, "ResourceId"),
            param(params, "ResourceType"),
        ),
        "GetMetricData" => op_get_metric_data(st, param(params, "InstanceId"), body),
        "GetMetricDataV2" => op_get_metric_data_v2(st, param(params, "InstanceId"), body),
        "GetPromptFile" => {
            op_get_prompt_file(st, param(params, "InstanceId"), param(params, "PromptId"))
        }
        "GetTestCaseExecutionSummary" => op_get_test_case_execution_summary(
            st,
            param(params, "InstanceId"),
            param(params, "TestCaseId"),
            param(params, "TestCaseExecutionId"),
        ),
        "GetTrafficDistribution" => op_get_traffic_distribution(st, param(params, "Id")),
        "ImportPhoneNumber" => op_import_phone_number(st, body),
        "ImportWorkspaceMedia" => op_import_workspace_media(
            st,
            param(params, "InstanceId"),
            param(params, "WorkspaceId"),
            body,
        ),
        "ListAnalyticsDataLakeDataSets" => {
            op_list_analytics_data_lake_data_sets(st, param(params, "InstanceId"))
        }
        "ListAssociatedContacts" => {
            op_list_associated_contacts(st, param(params, "InstanceId"), query)
        }
        "ListAuthenticationProfiles" => {
            op_list_authentication_profiles(st, param(params, "InstanceId"))
        }
        "ListChildHoursOfOperations" => op_list_child_hours_of_operations(
            st,
            param(params, "InstanceId"),
            param(params, "HoursOfOperationId"),
        ),
        "ListContactFlowModuleVersions" => op_list_contact_flow_module_versions(
            st,
            param(params, "InstanceId"),
            param(params, "ContactFlowModuleId"),
        ),
        "ListDataTablePrimaryValues" => op_list_data_table_values(
            st,
            param(params, "InstanceId"),
            body,
            param(params, "DataTableId"),
            "primary",
        ),
        "ListDataTableValues" => op_list_data_table_values(
            st,
            param(params, "InstanceId"),
            body,
            param(params, "DataTableId"),
            "values",
        ),
        "ListEntitySecurityProfiles" => {
            op_list_entity_security_profiles(st, param(params, "InstanceId"), body)
        }
        "ListLexBots" => op_list_lex_bots(st, param(params, "InstanceId")),
        "ListQueueEmailAddresses" => {
            op_list_queue_emails(st, param(params, "InstanceId"), param(params, "QueueId"))
        }
        "ListRealtimeContactAnalysisSegmentsV2" => op_list_realtime_contact_analysis_segments_v2(
            st,
            param(params, "InstanceId"),
            param(params, "ContactId"),
            body,
        ),
        "ListRoutingProfileManualAssignmentQueues" => {
            op_list_routing_profile_manual_assignment_queues(
                st,
                param(params, "InstanceId"),
                param(params, "RoutingProfileId"),
            )
        }
        "ListSecurityProfileFlowModules" => op_list_security_profile_flow_modules(
            st,
            param(params, "InstanceId"),
            param(params, "SecurityProfileId"),
        ),
        "ListTestCaseExecutionRecords" => op_list_test_case_execution_records(
            st,
            param(params, "InstanceId"),
            param(params, "TestCaseId"),
            param(params, "TestCaseExecutionId"),
        ),
        "ListTestCaseExecutions" => op_list_test_case_executions(st, param(params, "InstanceId")),
        "ListTrafficDistributionGroupUsers" => {
            op_list_tdg_users(st, param(params, "TrafficDistributionGroupId"), query)
        }
        "ListUserNotifications" => op_list_user_notifications(
            st,
            param(params, "InstanceId"),
            param(params, "UserId"),
            query,
        ),
        "ListUserProficiencies" => {
            op_list_user_proficiencies(st, param(params, "InstanceId"), param(params, "UserId"))
        }
        "ListViewVersions" => {
            op_list_view_versions(st, param(params, "InstanceId"), param(params, "ViewId"))
        }
        "ListWorkspaceMedia" => op_list_workspace_media(
            st,
            param(params, "InstanceId"),
            param(params, "WorkspaceId"),
        ),
        "ListWorkspacePages" => op_list_workspace_pages(
            st,
            param(params, "InstanceId"),
            param(params, "WorkspaceId"),
        ),
        "MonitorContact" => op_monitor_contact(st, body),
        "PauseContact" => op_pause_contact(st, body, true),
        "PutUserStatus" => op_put_user_status(
            st,
            param(params, "InstanceId"),
            param(params, "UserId"),
            body,
        ),
        "ReplicateInstance" => op_replicate_instance(
            st,
            param(params, "InstanceId"),
            body,
            &req.region,
            req.account,
        ),
        "ResumeContact" => op_pause_contact(st, body, false),
        "ResumeContactRecording" => op_set_contact_recording(st, body, "RESUMED"),
        "SearchContactEvaluations" => {
            op_search_contact_evaluations(st, param(params, "InstanceId"), body)
        }
        "SearchContacts" => op_search_contacts(st, param(params, "InstanceId"), body),
        "SearchDataTables" => {
            let instance_id = match body_str(body, "InstanceId") {
                Ok(v) => v,
                Err(resp) => return resp,
            };
            op_search(st, &instance_id, "DataTables", |st2, iid| {
                st2.data_tables
                    .get(iid)
                    .map(|m: &HashMap<String, Value>| m.values().cloned().collect())
            })
        }
        "SearchEmailAddresses" => {
            let instance_id = match body_str(body, "InstanceId") {
                Ok(v) => v,
                Err(resp) => return resp,
            };
            op_search(st, &instance_id, "EmailAddresses", |st2, iid| {
                st2.email_addresses
                    .get(iid)
                    .map(|m| m.values().cloned().collect())
            })
        }
        "SearchEvaluationForms" => {
            let instance_id = match body_str(body, "InstanceId") {
                Ok(v) => v,
                Err(resp) => return resp,
            };
            op_search(
                st,
                &instance_id,
                "EvaluationFormSearchSummaryList",
                |st2, iid| {
                    st2.evaluation_forms
                        .get(iid)
                        .map(|m| m.values().map(eval_search_summary).collect())
                },
            )
        }
        "SearchHoursOfOperationOverrides" => {
            let instance_id = match body_str(body, "InstanceId") {
                Ok(v) => v,
                Err(resp) => return resp,
            };
            op_search(st, &instance_id, "HoursOfOperationOverrides", |st2, iid| {
                st2.hours_of_operation_overrides
                    .get(iid)
                    .map(|m| m.values().cloned().collect())
            })
        }
        "SearchNotifications" => {
            let instance_id = match body_str(body, "InstanceId") {
                Ok(v) => v,
                Err(resp) => return resp,
            };
            op_search(st, &instance_id, "Notifications", |st2, iid| {
                st2.notifications
                    .get(iid)
                    .map(|m| m.values().cloned().collect())
            })
        }
        "SearchResourceTags" => op_search_resource_tags(st, body),
        "SearchTestCases" => {
            let instance_id = match body_str(body, "InstanceId") {
                Ok(v) => v,
                Err(resp) => return resp,
            };
            op_search(st, &instance_id, "TestCases", |st2, iid| {
                st2.test_cases
                    .get(iid)
                    .map(|m| m.values().cloned().collect())
            })
        }
        "SearchViews" => {
            let instance_id = match body_str(body, "InstanceId") {
                Ok(v) => v,
                Err(resp) => return resp,
            };
            op_search(st, &instance_id, "Views", |st2, iid| {
                st2.views.get(iid).map(|m| m.values().cloned().collect())
            })
        }
        "SearchWorkspaceAssociations" => {
            let instance_id = match body_str(body, "InstanceId") {
                Ok(v) => v,
                Err(resp) => return resp,
            };
            op_search_workspace_associations(st, &instance_id, body)
        }
        "SearchWorkspaces" => {
            let instance_id = match body_str(body, "InstanceId") {
                Ok(v) => v,
                Err(resp) => return resp,
            };
            op_search(st, &instance_id, "Workspaces", |st2, iid| {
                st2.workspaces
                    .get(iid)
                    .map(|m| m.values().cloned().collect())
            })
        }
        "SendChatIntegrationEvent" => op_send_chat_integration_event(st, body),
        "SendOutboundEmail" => op_send_outbound_email(st, param(params, "InstanceId"), body),
        "StartAttachedFileUpload" => {
            op_start_attached_file_upload(st, param(params, "InstanceId"), body)
        }
        "StartChatContact" => op_start_chat_contact(st, body),
        "StartContactMediaProcessing" => op_contact_media_processing(st, body, true),
        "StartContactRecording" => op_set_contact_recording(st, body, "STARTED"),
        "StartContactStreaming" => {
            op_start_contact_streaming(st, param(params, "InstanceId"), body)
        }
        "StartEmailContact" => op_start_email_contact(st, body, false),
        "StartOutboundChatContact" => op_start_outbound_chat_contact(st, body),
        "StartOutboundEmailContact" => op_start_email_contact(st, body, true),
        "StartOutboundVoiceContact" => op_start_outbound_voice_contact(st, body),
        "StartScreenSharing" => op_start_screen_sharing(st, body),
        "StartTaskContact" => op_start_task_contact(st, body),
        "StartTestCaseExecution" => op_start_test_case_execution(
            st,
            param(params, "InstanceId"),
            param(params, "TestCaseId"),
        ),
        "StartWebRTCContact" => op_start_webrtc_contact(st, body),
        "StopContactMediaProcessing" => op_contact_media_processing(st, body, false),
        "StopContactRecording" => op_set_contact_recording(st, body, "STOPPED"),
        "StopContactStreaming" => op_stop_contact_streaming(st, body),
        "StopTestCaseExecution" => op_stop_test_case_execution(
            st,
            param(params, "InstanceId"),
            param(params, "TestCaseId"),
            param(params, "TestCaseExecutionId"),
        ),
        "SubmitContactEvaluation" => op_submit_contact_evaluation(
            st,
            param(params, "InstanceId"),
            param(params, "EvaluationId"),
        ),
        "SuspendContactRecording" => op_set_contact_recording(st, body, "SUSPENDED"),
        "TagContact" => op_tag_contact(st, body),
        "TransferContact" => op_transfer_contact(st, body),
        "UntagContact" => op_untag_contact(
            st,
            param(params, "InstanceId"),
            param(params, "ContactId"),
            body,
        ),
        "UpdateAuthenticationProfile" => op_update_authentication_profile(
            st,
            param(params, "InstanceId"),
            param(params, "AuthenticationProfileId"),
            body,
        ),
        "UpdateContactRoutingData" => op_update_contact_routing_data(
            st,
            param(params, "InstanceId"),
            param(params, "ContactId"),
            body,
        ),
        "UpdateContactSchedule" => op_update_contact_schedule(st, body),
        "UpdateDataTableMetadata" => op_update_data_table_metadata(
            st,
            param(params, "InstanceId"),
            param(params, "DataTableId"),
            body,
        ),
        "UpdateDataTablePrimaryValues" => op_update_data_table_primary_values(
            st,
            param(params, "InstanceId"),
            param(params, "DataTableId"),
            body,
        ),
        "UpdateEmailAddressMetadata" => op_update_email_address_metadata(
            st,
            param(params, "InstanceId"),
            param(params, "EmailAddressId"),
            body,
        ),
        "UpdateInstanceStorageConfig" => op_update_instance_storage_config(
            st,
            param(params, "InstanceId"),
            param(params, "AssociationId"),
            body,
        ),
        "UpdateNotificationContent" => op_update_notification_content(
            st,
            param(params, "InstanceId"),
            param(params, "NotificationId"),
            body,
        ),
        "UpdateParticipantAuthentication" => op_update_participant_authentication(st, body),
        "UpdatePhoneNumberMetadata" => {
            op_update_phone_number_metadata(st, param(params, "PhoneNumberId"), body)
        }
        "UpdateQueueOutboundEmailConfig" => op_update_queue_outbound_email_config(
            st,
            param(params, "InstanceId"),
            param(params, "QueueId"),
            body,
        ),
        "UpdateRoutingProfileAgentAvailabilityTimer" => {
            op_update_routing_profile_agent_availability_timer(
                st,
                param(params, "InstanceId"),
                param(params, "RoutingProfileId"),
                body,
            )
        }
        "UpdateTrafficDistribution" => {
            op_update_traffic_distribution(st, param(params, "Id"), body)
        }
        "UpdateUserConfig" => op_update_user_config(
            st,
            param(params, "InstanceId"),
            param(params, "UserId"),
            body,
        ),
        "UpdateUserNotificationStatus" => op_update_user_notification_status(
            st,
            param(params, "InstanceId"),
            param(params, "UserId"),
            param(params, "NotificationId"),
            body,
        ),
        "UpdateUserProficiencies" => op_update_user_proficiencies_inner(
            st,
            param(params, "InstanceId"),
            param(params, "UserId"),
            body,
            false,
        ),
        "UpdateWorkspaceMetadata" => op_update_workspace_field(
            st,
            param(params, "InstanceId"),
            param(params, "WorkspaceId"),
            body,
            None,
        ),
        "UpdateWorkspacePage" => op_update_workspace_page(
            st,
            param(params, "InstanceId"),
            param(params, "WorkspaceId"),
            param(params, "Page"),
            body,
        ),
        "UpdateWorkspaceTheme" => op_update_workspace_field(
            st,
            param(params, "InstanceId"),
            param(params, "WorkspaceId"),
            body,
            Some("WorkspaceTheme"),
        ),
        "UpdateWorkspaceVisibility" => op_update_workspace_field(
            st,
            param(params, "InstanceId"),
            param(params, "WorkspaceId"),
            body,
            Some("Visibility"),
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
    ok(json!({"Origins": st.approved_origins.get(instance_id).cloned().unwrap_or_default()}))
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
    ok(
        json!({"LambdaFunctions": st.lambda_functions.get(instance_id).cloned().unwrap_or_default()}),
    )
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
    ok(json!({"LexBots": st.bots.get(instance_id).cloned().unwrap_or_default()}))
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
    ok(json!({"SecurityKeys": st.security_keys.get(instance_id).cloned().unwrap_or_default()}))
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
    ok(
        json!({"StorageConfigs": st.instance_storage_configs.get(instance_id).cloned().unwrap_or_default()}),
    )
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
// Phone numbers (moto global store; all instances visible)
// ---------------------------------------------------------------------------

fn rand_digits(n: usize) -> String {
    let mut out = String::with_capacity(n);
    let mut seed = uuid::Uuid::new_v4().as_u128();
    while out.len() < n {
        out.push_str(&(seed % 10).to_string());
        seed >>= 4;
        if seed == 0 {
            seed = uuid::Uuid::new_v4().as_u128();
        }
    }
    out[..n].to_string()
}

fn op_claim_phone_number(
    st: &mut ConnectState,
    body: &Map<String, Value>,
    region: &str,
    account: u64,
) -> Response {
    let _ = (region, account);
    let phone_number = match body_str(body, "PhoneNumber") {
        Ok(v) => v,
        Err(resp) => return resp,
    };
    let instance_id = opt_string(body, "InstanceId")
        .filter(|v| !v.is_empty())
        .unwrap_or_else(|| {
            opt_string(body, "TargetArn")
                .unwrap_or_default()
                .rsplit('/')
                .next()
                .unwrap_or("")
                .to_string()
        });
    let instance = match st.instances.get(&instance_id) {
        Some(i) => i.clone(),
        None => return notfound_instance(&instance_id),
    };
    let instance_arn = str_field(&instance, "Arn");
    let duplicate_claim = st
        .phone_numbers
        .values()
        .any(|pn| pn.get("PhoneNumber") == Some(&Value::String(phone_number.clone())));
    if duplicate_claim {
        return duplicate(format!("Phone number {phone_number} is already claimed"));
    }
    let country = opt_string(body, "PhoneNumberCountryCode").unwrap_or_else(|| "US".into());
    let number_type = opt_string(body, "PhoneNumberType").unwrap_or_else(|| "DID".into());
    let phone_number_id = uuid::Uuid::new_v4().to_string();
    let arn = format!("{instance_arn}/phone-number/{phone_number_id}");
    let mut stored = Map::new();
    stored.insert("PhoneNumberId".into(), json!(phone_number_id.clone()));
    stored.insert("PhoneNumberArn".into(), json!(arn.clone()));
    stored.insert("PhoneNumber".into(), json!(phone_number));
    stored.insert("PhoneNumberCountryCode".into(), json!(country));
    stored.insert("PhoneNumberType".into(), json!(number_type));
    stored.insert(
        "PhoneNumberDescription".into(),
        json!(opt_string(body, "PhoneNumberDescription").unwrap_or_default()),
    );
    stored.insert("TargetArn".into(), json!(instance_arn));
    stored.insert("InstanceId".into(), json!(instance_id));
    stored.insert(
        "Tags".into(),
        opt_val(body, "Tags").unwrap_or_else(|| json!({})),
    );
    stored.insert("PhoneNumberStatus".into(), json!({"Status": "CLAIMED"}));
    st.phone_numbers
        .insert(phone_number_id.clone(), Value::Object(stored));
    tag_resource(st, &arn, opt_val(body, "Tags"));
    ok(json!({"PhoneNumberId": phone_number_id, "PhoneNumberArn": arn}))
}

fn op_describe_phone_number(st: &mut ConnectState, phone_number_id: &str) -> Response {
    match st.phone_numbers.get(phone_number_id) {
        Some(pn) => ok(json!({"ClaimedPhoneNumberSummary": pn.clone()})),
        None => not_found(format!("Phone number {phone_number_id} not found")),
    }
}

fn op_update_phone_number(
    st: &mut ConnectState,
    phone_number_id: &str,
    body: &Map<String, Value>,
) -> Response {
    let target_arn = opt_string(body, "TargetArn").filter(|v| !v.is_empty());
    let referenced_instance = opt_string(body, "InstanceId").filter(|v| !v.is_empty());
    let resolved_target = match (&target_arn, &referenced_instance) {
        (Some(t), _) => Some(t.clone()),
        (None, Some(instance_id)) => {
            if !st.instances.contains_key(instance_id) {
                return notfound_instance(instance_id);
            }
            Some(instance_arn_of(st, instance_id))
        }
        _ => None,
    };
    let pn = match st.phone_numbers.get_mut(phone_number_id) {
        Some(pn) => pn,
        None => return not_found(format!("Phone number {phone_number_id} not found")),
    };
    if let Some(target) = resolved_target {
        pn["TargetArn"] = json!(target);
        pn["InstanceId"] = json!(target.rsplit('/').next().unwrap_or(""));
    }
    ok(json!({
        "PhoneNumberId": pn["PhoneNumberId"].clone(),
        "PhoneNumberArn": pn["PhoneNumberArn"].clone(),
    }))
}

fn instance_arn_of(st: &ConnectState, instance_id: &str) -> String {
    st.instances
        .get(instance_id)
        .map(|i| str_field(i, "Arn"))
        .unwrap_or_default()
}

fn op_release_phone_number(st: &mut ConnectState, phone_number_id: &str) -> Response {
    match st.phone_numbers.remove(phone_number_id) {
        Some(_) => ok(json!({})),
        None => not_found(format!("Phone number {phone_number_id} not found")),
    }
}

fn op_list_phone_numbers(st: &mut ConnectState, instance_id: &str) -> Response {
    if !st.instances.contains_key(instance_id) {
        return notfound_instance(instance_id);
    }
    let rows: Vec<Value> = st
        .phone_numbers
        .values()
        .filter(|pn| pn.get("InstanceId") == Some(&Value::String(instance_id.to_string())))
        .map(|pn| {
            json!({
                "PhoneNumber": pn["PhoneNumber"],
                "PhoneNumberType": pn["PhoneNumberType"],
                "PhoneNumberCountryCode": pn["PhoneNumberCountryCode"],
                "PhoneNumberId": pn["PhoneNumberId"],
                "PhoneNumberArn": pn["PhoneNumberArn"],
            })
        })
        .collect();
    ok(json!({"PhoneNumberSummaryList": rows}))
}

fn op_list_phone_numbers_v2(st: &mut ConnectState) -> Response {
    let rows: Vec<Value> = st
        .phone_numbers
        .values()
        .map(|pn| {
            json!({
                "PhoneNumberId": pn["PhoneNumberId"],
                "PhoneNumberArn": pn["PhoneNumberArn"],
                "PhoneNumber": pn["PhoneNumber"],
                "PhoneNumberCountryCode": pn["PhoneNumberCountryCode"],
                "PhoneNumberType": pn["PhoneNumberType"],
                "TargetArn": pn["TargetArn"],
            })
        })
        .collect();
    ok(json!({"ListPhoneNumbersSummaryList": rows}))
}

fn op_search_available_phone_numbers(st: &mut ConnectState, body: &Map<String, Value>) -> Response {
    let _ = st;
    let country = match body_str(body, "PhoneNumberCountryCode") {
        Ok(v) => v,
        Err(resp) => return resp,
    };
    let number_type = match body_str(body, "PhoneNumberType") {
        Ok(v) => v,
        Err(resp) => return resp,
    };
    match body_str(body, "TargetArn") {
        Ok(_) => {}
        Err(resp) => return resp,
    }
    let prefix = opt_string(body, "PhoneNumberPrefix").unwrap_or_default();
    let count = body
        .get("MaxResults")
        .and_then(Value::as_i64)
        .unwrap_or(10)
        .clamp(1, 25) as usize;
    let rows: Vec<Value> = (0..count)
        .map(|i| {
            let number = if country == "US" && prefix.len() == 3 {
                format!(
                    "+1{prefix}{}",
                    rand_digits(if (i as u128).is_multiple_of(7) { 3 } else { 4 })
                )
                .chars()
                .take(12)
                .collect::<String>()
            } else if country == "GB" {
                format!("+4420{}{}", rand_digits(4), i)
            } else {
                format!("+1{}", rand_digits(10))
            };
            json!({
                "PhoneNumber": number,
                "PhoneNumberCountryCode": country,
                "PhoneNumberType": number_type,
            })
        })
        .collect();
    ok(json!({"AvailableNumbersList": rows}))
}

// ---------------------------------------------------------------------------
// Prompts
// ---------------------------------------------------------------------------

fn op_create_prompt(
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
    let s3_uri = match body_str(body, "S3Uri") {
        Ok(v) => v,
        Err(resp) => return resp,
    };
    let prompt_id = uuid::Uuid::new_v4().to_string();
    let arn = format!("{}/prompt/{prompt_id}", str_field(&instance, "Arn"));
    let mut stored = Map::new();
    stored.insert("PromptARN".into(), json!(arn.clone()));
    stored.insert("PromptId".into(), json!(prompt_id.clone()));
    stored.insert("Name".into(), json!(name));
    // moto keeps the s3 URI on the model; to_dict never serializes it
    stored.insert("__s3_uri".into(), json!(s3_uri));
    stored.insert(
        "Description".into(),
        json!(opt_string(body, "Description").unwrap_or_default()),
    );
    stored.insert(
        "Tags".into(),
        opt_val(body, "Tags").unwrap_or_else(|| json!({})),
    );
    stored.insert("LastModifiedTime".into(), json!(now_iso()));
    stored.insert("LastModifiedRegion".into(), json!(region_of(&instance)));
    st.prompts
        .entry(instance_id.to_string())
        .or_default()
        .insert(prompt_id.clone(), Value::Object(stored));
    tag_resource(st, &arn, opt_val(body, "Tags"));
    ok(json!({"PromptARN": arn, "PromptId": prompt_id}))
}

fn op_describe_prompt(st: &mut ConnectState, instance_id: &str, prompt_id: &str) -> Response {
    if !st.instances.contains_key(instance_id) {
        return notfound_instance(instance_id);
    }
    match st.prompts.get(instance_id).and_then(|m| m.get(prompt_id)) {
        Some(p) => ok(json!({"Prompt": p.clone()})),
        None => not_found(format!(
            "Prompt {prompt_id} not found in instance {instance_id}"
        )),
    }
}

fn op_update_prompt(
    st: &mut ConnectState,
    instance_id: &str,
    prompt_id: &str,
    body: &Map<String, Value>,
) -> Response {
    if !st.instances.contains_key(instance_id) {
        return notfound_instance(instance_id);
    }
    let prompt = match st
        .prompts
        .get_mut(instance_id)
        .and_then(|m| m.get_mut(prompt_id))
    {
        Some(p) => p,
        None => return not_found(format!("Prompt {prompt_id} not found")),
    };
    if let Some(name) = opt_val(body, "Name") {
        prompt["Name"] = name;
    }
    if let Some(desc) = opt_val(body, "Description") {
        prompt["Description"] = desc;
    }
    if let Some(uri) = opt_val(body, "S3Uri") {
        prompt["__s3_uri"] = uri;
    }
    prompt["LastModifiedTime"] = json!(now_iso());
    ok(json!({
        "PromptARN": prompt["PromptARN"].clone(),
        "PromptId": prompt["PromptId"].clone(),
    }))
}

fn op_delete_prompt(st: &mut ConnectState, instance_id: &str, prompt_id: &str) -> Response {
    if !st.instances.contains_key(instance_id) {
        return notfound_instance(instance_id);
    }
    match st
        .prompts
        .get_mut(instance_id)
        .and_then(|m| m.remove(prompt_id))
    {
        Some(_) => ok(json!({})),
        None => not_found(format!("Prompt {prompt_id} not found")),
    }
}

fn op_list_prompts(st: &mut ConnectState, instance_id: &str) -> Response {
    if !st.instances.contains_key(instance_id) {
        return notfound_instance(instance_id);
    }
    let empty = HashMap::new();
    let prompts = st.prompts.get(instance_id).unwrap_or(&empty);
    let rows: Vec<Value> = prompts
        .values()
        .map(|p| {
            json!({
                "Id": p["PromptId"],
                "Arn": p["PromptARN"],
                "Name": p["Name"],
                "LastModifiedTime": p["LastModifiedTime"],
                "LastModifiedRegion": p["LastModifiedRegion"],
            })
        })
        .collect();
    ok(json!({"PromptSummaryList": rows}))
}

// ---------------------------------------------------------------------------
// Views
// ---------------------------------------------------------------------------

fn op_create_view(st: &mut ConnectState, instance_id: &str, body: &Map<String, Value>) -> Response {
    let instance_arn = match instance_arn_or_404(st, instance_id) {
        Some(a) => a,
        None => return notfound_instance(instance_id),
    };
    let name = match body_str(body, "Name") {
        Ok(v) => v,
        Err(resp) => return resp,
    };
    let status = match body_str(body, "Status") {
        Ok(v) => v,
        Err(resp) => return resp,
    };
    let content = match opt_val(body, "Content") {
        Some(v) => v,
        None => return missing_body_param("Content"),
    };
    let view_id = uuid::Uuid::new_v4().to_string();
    let arn = format!("{instance_arn}/view/{view_id}");
    let mut stored = Map::new();
    stored.insert("Id".into(), json!(view_id.clone()));
    stored.insert("Arn".into(), json!(arn.clone()));
    stored.insert("Name".into(), json!(name));
    stored.insert("Status".into(), json!(status));
    stored.insert("Type".into(), json!("CUSTOMER_MANAGED"));
    stored.insert(
        "Description".into(),
        json!(opt_string(body, "Description").unwrap_or_default()),
    );
    stored.insert("Version".into(), json!(1));
    stored.insert("Content".into(), content);
    stored.insert(
        "Tags".into(),
        opt_val(body, "Tags").unwrap_or_else(|| json!({})),
    );
    st.views
        .entry(instance_id.to_string())
        .or_default()
        .insert(view_id.clone(), Value::Object(stored.clone()));
    tag_resource(st, &arn, opt_val(body, "Tags"));
    // moto returns the create response whole-view
    ok(json!({"View": stored}))
}

fn op_describe_view(st: &mut ConnectState, instance_id: &str, view_id: &str) -> Response {
    if !st.instances.contains_key(instance_id) {
        return notfound_instance(instance_id);
    }
    match st.views.get(instance_id).and_then(|m| m.get(view_id)) {
        Some(v) => ok(json!({"View": v.clone()})),
        None => not_found(format!(
            "View {view_id} not found in instance {instance_id}"
        )),
    }
}

fn op_update_view_content(
    st: &mut ConnectState,
    instance_id: &str,
    view_id: &str,
    body: &Map<String, Value>,
) -> Response {
    if !st.instances.contains_key(instance_id) {
        return notfound_instance(instance_id);
    }
    let content = match opt_val(body, "Content") {
        Some(v) => v,
        None => return missing_body_param("Content"),
    };
    let view = match st
        .views
        .get_mut(instance_id)
        .and_then(|m| m.get_mut(view_id))
    {
        Some(v) => v,
        None => return not_found(format!("View {view_id} not found")),
    };
    view["Content"] = content;
    let version = view["Version"].as_i64().unwrap_or(0) + 1;
    view["Version"] = json!(version);
    if let Some(status) = opt_val(body, "Status").filter(|v| !v.is_null()) {
        view["Status"] = status;
    }
    ok(json!({"View": view.clone()}))
}

fn op_update_view_metadata(
    st: &mut ConnectState,
    instance_id: &str,
    view_id: &str,
    body: &Map<String, Value>,
) -> Response {
    if !st.instances.contains_key(instance_id) {
        return notfound_instance(instance_id);
    }
    let view = match st
        .views
        .get_mut(instance_id)
        .and_then(|m| m.get_mut(view_id))
    {
        Some(v) => v,
        None => return not_found(format!("View {view_id} not found")),
    };
    if let Some(name) = opt_val(body, "Name") {
        view["Name"] = name;
    }
    if let Some(desc) = opt_val(body, "Description") {
        view["Description"] = desc;
    }
    ok(json!({}))
}

fn op_delete_view(st: &mut ConnectState, instance_id: &str, view_id: &str) -> Response {
    if !st.instances.contains_key(instance_id) {
        return notfound_instance(instance_id);
    }
    match st
        .views
        .get_mut(instance_id)
        .and_then(|m| m.remove(view_id))
    {
        Some(_) => ok(json!({})),
        None => not_found(format!("View {view_id} not found")),
    }
}

fn op_list_views(st: &mut ConnectState, instance_id: &str) -> Response {
    if !st.instances.contains_key(instance_id) {
        return notfound_instance(instance_id);
    }
    let empty = HashMap::new();
    let views = st.views.get(instance_id).unwrap_or(&empty);
    let rows: Vec<Value> = views
        .values()
        .map(|v| {
            json!({
                "Id": v["Id"],
                "Arn": v["Arn"],
                "Name": v["Name"],
                "Type": v["Type"],
                "Status": v["Status"],
                "Description": v["Description"],
            })
        })
        .collect();
    ok(json!({"ViewsSummaryList": rows}))
}

// ---------------------------------------------------------------------------
// Vocabularies
// ---------------------------------------------------------------------------

fn op_create_vocabulary(
    st: &mut ConnectState,
    instance_id: &str,
    body: &Map<String, Value>,
) -> Response {
    let instance = match st.instances.get(instance_id) {
        Some(i) => i.clone(),
        None => return notfound_instance(instance_id),
    };
    let name = match body_str(body, "VocabularyName") {
        Ok(v) => v,
        Err(resp) => return resp,
    };
    let language_code = match body_str(body, "LanguageCode") {
        Ok(v) => v,
        Err(resp) => return resp,
    };
    let content = match body_str(body, "Content") {
        Ok(v) => v,
        Err(resp) => return resp,
    };
    let vocab_id = uuid::Uuid::new_v4().to_string();
    let arn = format!("{}/vocabulary/{vocab_id}", str_field(&instance, "Arn"));
    let mut stored = Map::new();
    stored.insert("Name".into(), json!(name));
    stored.insert("Id".into(), json!(vocab_id.clone()));
    stored.insert("Arn".into(), json!(arn.clone()));
    stored.insert("LanguageCode".into(), json!(language_code));
    stored.insert("State".into(), json!("ACTIVE"));
    stored.insert("LastModifiedTime".into(), json!(now_iso()));
    stored.insert("Content".into(), json!(content));
    stored.insert(
        "Tags".into(),
        opt_val(body, "Tags").unwrap_or_else(|| json!({})),
    );
    st.vocabularies
        .entry(instance_id.to_string())
        .or_default()
        .insert(vocab_id.clone(), Value::Object(stored.clone()));
    tag_resource(st, &arn, opt_val(body, "Tags"));
    ok(json!({"VocabularyArn": arn, "VocabularyId": vocab_id, "State": "ACTIVE"}))
}

fn op_describe_vocabulary(st: &mut ConnectState, instance_id: &str, vocab_id: &str) -> Response {
    if !st.instances.contains_key(instance_id) {
        return notfound_instance(instance_id);
    }
    match st
        .vocabularies
        .get(instance_id)
        .and_then(|m| m.get(vocab_id))
    {
        Some(v) => ok(json!({"Vocabulary": v.clone()})),
        None => not_found(format!(
            "Vocabulary {vocab_id} not found in instance {instance_id}"
        )),
    }
}

fn op_delete_vocabulary(st: &mut ConnectState, instance_id: &str, vocab_id: &str) -> Response {
    if !st.instances.contains_key(instance_id) {
        return notfound_instance(instance_id);
    }
    match st
        .vocabularies
        .get_mut(instance_id)
        .and_then(|m| m.remove(vocab_id))
    {
        Some(v) => ok(json!({
            "VocabularyArn": v["Arn"].clone(),
            "VocabularyId": v["Id"].clone(),
            "State": "DELETE_IN_PROGRESS",
        })),
        None => not_found(format!("Vocabulary {vocab_id} not found")),
    }
}

fn op_search_vocabularies(
    st: &mut ConnectState,
    instance_id: &str,
    body: &Map<String, Value>,
) -> Response {
    if !st.instances.contains_key(instance_id) {
        return notfound_instance(instance_id);
    }
    let state = opt_string(body, "State");
    let name_prefix = opt_string(body, "NameStartsWith");
    let language_code = opt_string(body, "LanguageCode");
    let empty = HashMap::new();
    let vocabs = st.vocabularies.get(instance_id).unwrap_or(&empty);
    let rows: Vec<Value> = vocabs
        .values()
        .filter(|v| match &state {
            Some(s) => v.get("State") == Some(&Value::String(s.clone())),
            None => true,
        })
        .filter(|v| match &name_prefix {
            Some(p) => str_field(v, "Name").starts_with(p),
            None => true,
        })
        .filter(|v| match &language_code {
            Some(c) => v.get("LanguageCode") == Some(&Value::String(c.clone())),
            None => true,
        })
        .cloned()
        .collect();
    ok(json!({"VocabularySummaryList": rows}))
}

fn op_list_default_vocabularies(st: &mut ConnectState, instance_id: &str) -> Response {
    if !st.instances.contains_key(instance_id) {
        return notfound_instance(instance_id);
    }
    let empty = HashMap::new();
    let vocabs = st.vocabularies.get(instance_id).unwrap_or(&empty);
    let rows: Vec<Value> = vocabs
        .values()
        .map(|v| {
            json!({
                "InstanceId": instance_id,
                "LanguageCode": v["LanguageCode"],
                "VocabularyId": v["Id"],
                "VocabularyName": v["Name"],
            })
        })
        .collect();
    ok(json!({"DefaultVocabularyList": rows}))
}

// ---------------------------------------------------------------------------
// Rules
// ---------------------------------------------------------------------------

fn op_create_rule(st: &mut ConnectState, instance_id: &str, body: &Map<String, Value>) -> Response {
    let instance_arn = match instance_arn_or_404(st, instance_id) {
        Some(a) => a,
        None => return notfound_instance(instance_id),
    };
    let name = match body_str(body, "Name") {
        Ok(v) => v,
        Err(resp) => return resp,
    };
    let trigger = match opt_val(body, "TriggerEventSource") {
        Some(v) => v,
        None => return missing_body_param("TriggerEventSource"),
    };
    let function = match body_str(body, "Function") {
        Ok(v) => v,
        Err(resp) => return resp,
    };
    let actions = match opt_val(body, "Actions") {
        Some(v) => v,
        None => return missing_body_param("Actions"),
    };
    let publish_status = match body_str(body, "PublishStatus") {
        Ok(v) => v,
        Err(resp) => return resp,
    };
    let rule_id = uuid::Uuid::new_v4().to_string();
    let arn = format!("{instance_arn}/rule/{rule_id}");
    let stamp = now_iso();
    let mut stored = Map::new();
    stored.insert("RuleId".into(), json!(rule_id.clone()));
    stored.insert("RuleArn".into(), json!(arn.clone()));
    stored.insert("Name".into(), json!(name));
    stored.insert("TriggerEventSource".into(), trigger);
    stored.insert("Function".into(), json!(function));
    stored.insert("Actions".into(), actions);
    stored.insert("PublishStatus".into(), json!(publish_status));
    stored.insert("Tags".into(), json!({}));
    stored.insert("CreatedTime".into(), json!(stamp.clone()));
    stored.insert("LastUpdatedTime".into(), json!(stamp));
    st.rules
        .entry(instance_id.to_string())
        .or_default()
        .insert(rule_id.clone(), Value::Object(stored));
    ok(json!({"RuleArn": arn, "RuleId": rule_id}))
}

fn op_describe_rule(st: &mut ConnectState, instance_id: &str, rule_id: &str) -> Response {
    if !st.instances.contains_key(instance_id) {
        return notfound_instance(instance_id);
    }
    match st.rules.get(instance_id).and_then(|m| m.get(rule_id)) {
        Some(r) => ok(json!({"Rule": r.clone()})),
        None => not_found(format!(
            "Rule {rule_id} not found in instance {instance_id}"
        )),
    }
}

fn op_update_rule(
    st: &mut ConnectState,
    instance_id: &str,
    rule_id: &str,
    body: &Map<String, Value>,
) -> Response {
    if !st.instances.contains_key(instance_id) {
        return notfound_instance(instance_id);
    }
    let name = match body_str(body, "Name") {
        Ok(v) => v,
        Err(resp) => return resp,
    };
    let function = match body_str(body, "Function") {
        Ok(v) => v,
        Err(resp) => return resp,
    };
    let actions = match opt_val(body, "Actions") {
        Some(v) => v,
        None => return missing_body_param("Actions"),
    };
    let publish_status = match body_str(body, "PublishStatus") {
        Ok(v) => v,
        Err(resp) => return resp,
    };
    let rule = match st
        .rules
        .get_mut(instance_id)
        .and_then(|m| m.get_mut(rule_id))
    {
        Some(r) => r,
        None => return not_found(format!("Rule {rule_id} not found")),
    };
    rule["Name"] = json!(name);
    rule["Function"] = json!(function);
    rule["Actions"] = actions;
    rule["PublishStatus"] = json!(publish_status);
    rule["LastUpdatedTime"] = json!(now_iso());
    ok(json!({}))
}

fn op_delete_rule(st: &mut ConnectState, instance_id: &str, rule_id: &str) -> Response {
    if !st.instances.contains_key(instance_id) {
        return notfound_instance(instance_id);
    }
    match st
        .rules
        .get_mut(instance_id)
        .and_then(|m| m.remove(rule_id))
    {
        Some(_) => ok(json!({})),
        None => not_found(format!("Rule {rule_id} not found")),
    }
}

fn op_list_rules(st: &mut ConnectState, instance_id: &str) -> Response {
    if !st.instances.contains_key(instance_id) {
        return notfound_instance(instance_id);
    }
    let empty = HashMap::new();
    let rules = st.rules.get(instance_id).unwrap_or(&empty);
    let rows: Vec<Value> = rules
        .values()
        .map(|r| {
            let summaries: Vec<Value> = r
                .get("Actions")
                .and_then(Value::as_array)
                .map(|actions| {
                    actions
                        .iter()
                        .map(|x| {
                            json!({"ActionType": x
                                .get("ActionType")
                                .cloned()
                                .unwrap_or_else(|| json!("UNKNOWN"))})
                        })
                        .collect()
                })
                .unwrap_or_default();
            json!({
                "RuleId": r["RuleId"],
                "RuleArn": r["RuleArn"],
                "Name": r["Name"],
                "PublishStatus": r["PublishStatus"],
                "ActionSummaries": summaries,
                "CreatedTime": r["CreatedTime"],
                "LastUpdatedTime": r["LastUpdatedTime"],
            })
        })
        .collect();
    ok(json!({"RuleSummaryList": rows}))
}

// ---------------------------------------------------------------------------
// Evaluation forms
// ---------------------------------------------------------------------------

fn op_create_evaluation_form(
    st: &mut ConnectState,
    instance_id: &str,
    body: &Map<String, Value>,
) -> Response {
    let instance_arn = match instance_arn_or_404(st, instance_id) {
        Some(a) => a,
        None => return notfound_instance(instance_id),
    };
    let title = match body_str(body, "Title") {
        Ok(v) => v,
        Err(resp) => return resp,
    };
    let form_id = uuid::Uuid::new_v4().to_string();
    let arn = format!("{instance_arn}/evaluation-form/{form_id}");
    let stamp = now_iso();
    let mut stored = Map::new();
    stored.insert("EvaluationFormId".into(), json!(form_id.clone()));
    stored.insert("EvaluationFormArn".into(), json!(arn.clone()));
    stored.insert("Title".into(), json!(title));
    stored.insert(
        "Description".into(),
        json!(opt_string(body, "Description").unwrap_or_default()),
    );
    stored.insert("Status".into(), json!("DRAFT"));
    stored.insert(
        "Items".into(),
        opt_val(body, "Items").unwrap_or_else(|| json!([])),
    );
    stored.insert("EvaluationFormVersion".into(), json!(1));
    stored.insert("CreatedTime".into(), json!(stamp.clone()));
    stored.insert("LastModifiedTime".into(), json!(stamp));
    stored.insert("Tags".into(), json!({}));
    if let Some(s) = opt_val(body, "ScoringStrategy") {
        stored.insert("ScoringStrategy".into(), s);
    }
    st.evaluation_forms
        .entry(instance_id.to_string())
        .or_default()
        .insert(form_id.clone(), Value::Object(stored.clone()));
    ok(json!({"EvaluationFormId": form_id, "EvaluationFormArn": arn}))
}

fn op_describe_evaluation_form(
    st: &mut ConnectState,
    instance_id: &str,
    form_id: &str,
) -> Response {
    if !st.instances.contains_key(instance_id) {
        return notfound_instance(instance_id);
    }
    match st
        .evaluation_forms
        .get(instance_id)
        .and_then(|m| m.get(form_id))
    {
        Some(f) => ok(json!({"EvaluationForm": f.clone()})),
        None => not_found(format!(
            "Evaluation form {form_id} not found in instance {instance_id}"
        )),
    }
}

fn op_update_evaluation_form(
    st: &mut ConnectState,
    instance_id: &str,
    form_id: &str,
    body: &Map<String, Value>,
) -> Response {
    if !st.instances.contains_key(instance_id) {
        return notfound_instance(instance_id);
    }
    let title = match body_str(body, "Title") {
        Ok(v) => v,
        Err(resp) => return resp,
    };
    let form = match st
        .evaluation_forms
        .get_mut(instance_id)
        .and_then(|m| m.get_mut(form_id))
    {
        Some(f) => f,
        None => return not_found(format!("Evaluation form {form_id} not found")),
    };
    form["Title"] = json!(title);
    form["Description"] = json!(opt_string(body, "Description").unwrap_or_default());
    if let Some(items) = opt_val(body, "Items") {
        form["Items"] = items;
    }
    if let Some(s) = opt_val(body, "ScoringStrategy") {
        form["ScoringStrategy"] = s;
    }
    let version = form["EvaluationFormVersion"].as_i64().unwrap_or(0) + 1;
    form["EvaluationFormVersion"] = json!(version);
    form["LastModifiedTime"] = json!(now_iso());
    ok(json!({
        "EvaluationFormId": form["EvaluationFormId"].clone(),
        "EvaluationFormArn": form["EvaluationFormArn"].clone(),
        "EvaluationFormVersion": version,
    }))
}

fn op_activate_evaluation_form(
    st: &mut ConnectState,
    instance_id: &str,
    form_id: &str,
    activate: bool,
) -> Response {
    if !st.instances.contains_key(instance_id) {
        return notfound_instance(instance_id);
    }
    let form = match st
        .evaluation_forms
        .get_mut(instance_id)
        .and_then(|m| m.get_mut(form_id))
    {
        Some(f) => f,
        None => return not_found(format!("Evaluation form {form_id} not found")),
    };
    form["Status"] = json!(if activate { "ACTIVE" } else { "DRAFT" });
    form["LastModifiedTime"] = json!(now_iso());
    ok(json!({
        "EvaluationFormId": form["EvaluationFormId"].clone(),
        "EvaluationFormArn": form["EvaluationFormArn"].clone(),
        "EvaluationFormVersion": form["EvaluationFormVersion"].clone(),
    }))
}

fn op_delete_evaluation_form(st: &mut ConnectState, instance_id: &str, form_id: &str) -> Response {
    if !st.instances.contains_key(instance_id) {
        return notfound_instance(instance_id);
    }
    match st
        .evaluation_forms
        .get_mut(instance_id)
        .and_then(|m| m.remove(form_id))
    {
        Some(_) => ok(json!({})),
        None => not_found(format!("Evaluation form {form_id} not found")),
    }
}

fn op_list_evaluation_forms(st: &mut ConnectState, instance_id: &str) -> Response {
    if !st.instances.contains_key(instance_id) {
        return notfound_instance(instance_id);
    }
    let empty = HashMap::new();
    let forms = st.evaluation_forms.get(instance_id).unwrap_or(&empty);
    let rows: Vec<Value> = forms
        .values()
        .map(|f| {
            json!({
                "EvaluationFormId": f["EvaluationFormId"],
                "EvaluationFormArn": f["EvaluationFormArn"],
                "Title": f["Title"],
                "Status": f["Status"],
                "CreatedTime": f["CreatedTime"],
                "LastModifiedTime": f["LastModifiedTime"],
                "LatestVersion": f["EvaluationFormVersion"],
            })
        })
        .collect();
    ok(json!({"EvaluationFormSummaryList": rows}))
}

fn op_list_evaluation_form_versions(
    st: &mut ConnectState,
    instance_id: &str,
    form_id: &str,
) -> Response {
    if !st.instances.contains_key(instance_id) {
        return notfound_instance(instance_id);
    }
    let rows: Vec<Value> = st
        .evaluation_forms
        .get(instance_id)
        .and_then(|m| m.get(form_id))
        .map(|f| {
            vec![json!({
                "EvaluationFormArn": f["EvaluationFormArn"],
                "EvaluationFormId": f["EvaluationFormId"],
                "EvaluationFormVersion": f["EvaluationFormVersion"],
                "CreatedTime": f["CreatedTime"],
                "LastModifiedTime": f["LastModifiedTime"],
                "Status": f["Status"],
            })]
        })
        .unwrap_or_default();
    ok(json!({"EvaluationFormVersionSummaryList": rows}))
}

// ---------------------------------------------------------------------------
// Predefined attributes
// ---------------------------------------------------------------------------

fn op_create_predefined_attribute(
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
    let values = opt_val(body, "Values").unwrap_or_else(|| json!({}));
    let values_present = values.as_object().map(|m| !m.is_empty()).unwrap_or(true);
    let mut stored = Map::new();
    stored.insert("Name".into(), json!(name.clone()));
    stored.insert("Values".into(), values);
    stored.insert("LastModifiedTime".into(), json!(now_iso()));
    stored.insert(
        "LastModifiedRegion".into(),
        json!(instance_arn.split(':').nth(3).unwrap_or("")),
    );
    stored.insert("__values_present".into(), json!(values_present));
    st.predefined_attributes
        .entry(instance_id.to_string())
        .or_default()
        .insert(name.clone(), Value::Object(stored));
    ok(json!({}))
}

/// moto's `to_dict` skips `Values` only when it keys off a truthy values map.
fn pa_to_dict(v: &Value) -> Value {
    let mut out = Map::new();
    for f in ["Name", "LastModifiedTime", "LastModifiedRegion"] {
        out.insert(f.to_string(), v[f].clone());
    }
    let has_values = v
        .get("Values")
        .and_then(Value::as_object)
        .map(|m| !m.is_empty())
        .unwrap_or(false);
    if has_values {
        out.insert("Values".into(), v["Values"].clone());
    }
    let _ = v.get("__values_present").and_then(Value::as_bool);
    Value::Object(out)
}

fn op_describe_predefined_attribute(
    st: &mut ConnectState,
    instance_id: &str,
    name: &str,
) -> Response {
    if !st.instances.contains_key(instance_id) {
        return notfound_instance(instance_id);
    }
    match st
        .predefined_attributes
        .get(instance_id)
        .and_then(|m| m.get(name))
    {
        Some(a) => ok(json!({"PredefinedAttribute": pa_to_dict(a)})),
        None => not_found(format!("Predefined attribute {name} not found")),
    }
}

fn op_update_predefined_attribute(
    st: &mut ConnectState,
    instance_id: &str,
    name: &str,
    body: &Map<String, Value>,
) -> Response {
    if !st.instances.contains_key(instance_id) {
        return notfound_instance(instance_id);
    }
    let values = match opt_val(body, "Values") {
        Some(v) => v,
        None => return missing_body_param("Values"),
    };
    let attr = match st
        .predefined_attributes
        .get_mut(instance_id)
        .and_then(|m| m.get_mut(name))
    {
        Some(a) => a,
        None => return not_found(format!("Predefined attribute {name} not found")),
    };
    attr["Values"] = values;
    attr["LastModifiedTime"] = json!(now_iso());
    ok(json!({}))
}

fn op_delete_predefined_attribute(
    st: &mut ConnectState,
    instance_id: &str,
    name: &str,
) -> Response {
    if !st.instances.contains_key(instance_id) {
        return notfound_instance(instance_id);
    }
    match st
        .predefined_attributes
        .get_mut(instance_id)
        .and_then(|m| m.remove(name))
    {
        Some(_) => ok(json!({})),
        None => not_found(format!("Predefined attribute {name} not found")),
    }
}

fn op_list_predefined_attributes(st: &mut ConnectState, instance_id: &str) -> Response {
    if !st.instances.contains_key(instance_id) {
        return notfound_instance(instance_id);
    }
    let empty = HashMap::new();
    let attrs = st.predefined_attributes.get(instance_id).unwrap_or(&empty);
    let rows: Vec<Value> = attrs
        .values()
        .map(|a| {
            json!({
                "Name": a["Name"],
                "LastModifiedTime": a["LastModifiedTime"],
                "LastModifiedRegion": a["LastModifiedRegion"],
            })
        })
        .collect();
    ok(json!({"PredefinedAttributeSummaryList": rows}))
}

// ---------------------------------------------------------------------------
// Task templates
// ---------------------------------------------------------------------------

fn task_template_to_dict(v: &Value) -> Value {
    let mut out = Map::new();
    for f in [
        "Id",
        "Arn",
        "Name",
        "Description",
        "Fields",
        "Defaults",
        "Constraints",
        "Status",
        "Tags",
        "CreatedTime",
        "LastModifiedTime",
        "InstanceId",
    ] {
        out.insert(f.to_string(), v[f].clone());
    }
    let has_cf = v
        .get("ContactFlowId")
        .and_then(Value::as_str)
        .map(|s| !s.is_empty())
        .unwrap_or(false);
    if has_cf {
        out.insert("ContactFlowId".into(), v["ContactFlowId"].clone());
    }
    Value::Object(out)
}

fn task_template_mut<'a>(
    st: &'a mut ConnectState,
    instance_id: &str,
    template_id: &str,
) -> Result<&'a mut Value, Response> {
    if !st.instances.contains_key(instance_id) {
        return Err(notfound_instance(instance_id));
    }
    match st
        .task_templates
        .get_mut(instance_id)
        .and_then(|m| m.get_mut(template_id))
    {
        Some(t) => Ok(t),
        None => Err(not_found(format!("Task template {template_id} not found"))),
    }
}

fn op_create_task_template(
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
    let template_id = uuid::Uuid::new_v4().to_string();
    let arn = format!("{instance_arn}/task-template/{template_id}");
    let stamp = now_iso();
    let mut stored = Map::new();
    stored.insert("Id".into(), json!(template_id.clone()));
    stored.insert("Arn".into(), json!(arn.clone()));
    stored.insert("Name".into(), json!(name));
    stored.insert(
        "Description".into(),
        json!(opt_string(body, "Description").unwrap_or_default()),
    );
    stored.insert(
        "Fields".into(),
        opt_val(body, "Fields").unwrap_or_else(|| json!([])),
    );
    stored.insert(
        "Defaults".into(),
        opt_val(body, "Defaults").unwrap_or_else(|| json!({})),
    );
    stored.insert(
        "Constraints".into(),
        opt_val(body, "Constraints").unwrap_or_else(|| json!({})),
    );
    stored.insert(
        "Status".into(),
        json!(opt_string(body, "Status").unwrap_or_else(|| "ACTIVE".into())),
    );
    stored.insert(
        "Tags".into(),
        opt_val(body, "Tags").unwrap_or_else(|| json!({})),
    );
    stored.insert("CreatedTime".into(), json!(stamp.clone()));
    stored.insert("LastModifiedTime".into(), json!(stamp));
    stored.insert(
        "InstanceId".into(),
        json!(instance_arn.rsplit('/').next().unwrap_or("")),
    );
    if let Some(cf) =
        opt_val(body, "ContactFlowId").filter(|v| !v.as_str().unwrap_or_default().is_empty())
    {
        stored.insert("ContactFlowId".into(), cf);
    }
    st.task_templates
        .entry(instance_id.to_string())
        .or_default()
        .insert(template_id.clone(), Value::Object(stored));
    ok(json!({"Id": template_id, "Arn": arn}))
}

fn op_get_task_template(st: &mut ConnectState, instance_id: &str, template_id: &str) -> Response {
    if !st.instances.contains_key(instance_id) {
        return notfound_instance(instance_id);
    }
    match st
        .task_templates
        .get(instance_id)
        .and_then(|m| m.get(template_id))
    {
        Some(t) => ok(task_template_to_dict(t)),
        None => not_found(format!("Task template {template_id} not found")),
    }
}

fn op_update_task_template(
    st: &mut ConnectState,
    instance_id: &str,
    template_id: &str,
    body: &Map<String, Value>,
) -> Response {
    let template = match task_template_mut(st, instance_id, template_id) {
        Ok(t) => t,
        Err(resp) => return resp,
    };
    if let Some(name) = opt_val(body, "Name") {
        template["Name"] = name;
    }
    if let Some(desc) = opt_val(body, "Description") {
        template["Description"] = desc;
    }
    if let Some(fields) = opt_val(body, "Fields") {
        template["Fields"] = fields;
    }
    if let Some(defaults) = opt_val(body, "Defaults") {
        template["Defaults"] = defaults;
    }
    if let Some(constraints) = opt_val(body, "Constraints") {
        template["Constraints"] = constraints;
    }
    if let Some(cf) = opt_val(body, "ContactFlowId") {
        template["ContactFlowId"] = cf;
    }
    if let Some(status) = opt_val(body, "Status") {
        template["Status"] = status;
    }
    template["LastModifiedTime"] = json!(now_iso());
    ok(task_template_to_dict(template))
}

fn op_delete_task_template(
    st: &mut ConnectState,
    instance_id: &str,
    template_id: &str,
) -> Response {
    if !st.instances.contains_key(instance_id) {
        return notfound_instance(instance_id);
    }
    match st
        .task_templates
        .get_mut(instance_id)
        .and_then(|m| m.remove(template_id))
    {
        Some(_) => ok(json!({})),
        None => not_found(format!("Task template {template_id} not found")),
    }
}

fn op_list_task_templates(st: &mut ConnectState, instance_id: &str) -> Response {
    if !st.instances.contains_key(instance_id) {
        return notfound_instance(instance_id);
    }
    let empty = HashMap::new();
    let templates = st.task_templates.get(instance_id).unwrap_or(&empty);
    let rows: Vec<Value> = templates
        .values()
        .map(|t| {
            json!({
                "Id": t["Id"],
                "Arn": t["Arn"],
                "Name": t["Name"],
                "Description": t["Description"],
                "Status": t["Status"],
                "CreatedTime": t["CreatedTime"],
                "LastModifiedTime": t["LastModifiedTime"],
            })
        })
        .collect();
    ok(json!({"TaskTemplates": rows}))
}

// ---------------------------------------------------------------------------
// Integration associations + use cases
// ---------------------------------------------------------------------------

fn op_create_integration_association(
    st: &mut ConnectState,
    instance_id: &str,
    body: &Map<String, Value>,
) -> Response {
    let instance = match st.instances.get(instance_id) {
        Some(i) => i.clone(),
        None => return notfound_instance(instance_id),
    };
    let integration_type = match body_str(body, "IntegrationType") {
        Ok(v) => v,
        Err(resp) => return resp,
    };
    let integration_arn = match body_str(body, "IntegrationArn") {
        Ok(v) => v,
        Err(resp) => return resp,
    };
    let assoc_id = uuid::Uuid::new_v4().to_string();
    let arn = format!(
        "{}/integration-association/{assoc_id}",
        str_field(&instance, "Arn")
    );
    let mut stored = Map::new();
    stored.insert("IntegrationAssociationId".into(), json!(assoc_id.clone()));
    stored.insert("IntegrationAssociationArn".into(), json!(arn.clone()));
    stored.insert(
        "InstanceId".into(),
        json!(str_field(&instance, "Arn").rsplit('/').next().unwrap_or("")),
    );
    stored.insert("IntegrationType".into(), json!(integration_type));
    stored.insert("IntegrationArn".into(), json!(integration_arn));
    stored.insert(
        "SourceApplicationUrl".into(),
        json!(opt_string(body, "SourceApplicationUrl").unwrap_or_default()),
    );
    stored.insert(
        "SourceApplicationName".into(),
        json!(opt_string(body, "SourceApplicationName").unwrap_or_default()),
    );
    stored.insert(
        "SourceType".into(),
        json!(opt_string(body, "SourceType").unwrap_or_default()),
    );
    st.integration_associations
        .entry(instance_id.to_string())
        .or_default()
        .insert(assoc_id.clone(), Value::Object(stored));
    tag_resource(st, &arn, opt_val(body, "Tags"));
    ok(json!({"IntegrationAssociationId": assoc_id, "IntegrationAssociationArn": arn}))
}

fn op_list_integration_associations(st: &mut ConnectState, instance_id: &str) -> Response {
    if !st.instances.contains_key(instance_id) {
        return notfound_instance(instance_id);
    }
    let empty = HashMap::new();
    let assocs = st
        .integration_associations
        .get(instance_id)
        .unwrap_or(&empty);
    // moto mirrors the to_dict entries in the summary list
    let rows: Vec<Value> = assocs.values().cloned().collect();
    ok(json!({"IntegrationAssociationSummaryList": rows}))
}

fn op_create_use_case(
    st: &mut ConnectState,
    instance_id: &str,
    integration_association_id: &str,
    body: &Map<String, Value>,
) -> Response {
    let instance = match st.instances.get(instance_id) {
        Some(i) => i.clone(),
        None => return notfound_instance(instance_id),
    };
    let use_case_type = match body_str(body, "UseCaseType") {
        Ok(v) => v,
        Err(resp) => return resp,
    };
    let uc_id = uuid::Uuid::new_v4().to_string();
    let arn = format!("{}/use-case/{uc_id}", str_field(&instance, "Arn"));
    let mut stored = Map::new();
    stored.insert("UseCaseId".into(), json!(uc_id.clone()));
    stored.insert("UseCaseArn".into(), json!(arn.clone()));
    stored.insert("UseCaseType".into(), json!(use_case_type));
    stored.insert(
        "__integration_association_id".into(),
        json!(integration_association_id),
    );
    st.use_cases
        .entry(instance_id.to_string())
        .or_default()
        .insert(uc_id.clone(), Value::Object(stored));
    ok(json!({"UseCaseId": uc_id, "UseCaseArn": arn}))
}

fn op_delete_use_case(st: &mut ConnectState, instance_id: &str, use_case_id: &str) -> Response {
    if !st.instances.contains_key(instance_id) {
        return notfound_instance(instance_id);
    }
    match st
        .use_cases
        .get_mut(instance_id)
        .and_then(|m| m.remove(use_case_id))
    {
        Some(_) => ok(json!({})),
        None => not_found(format!("Use case {use_case_id} not found")),
    }
}

fn op_list_use_cases(
    st: &mut ConnectState,
    instance_id: &str,
    integration_association_id: &str,
) -> Response {
    if !st.instances.contains_key(instance_id) {
        return notfound_instance(instance_id);
    }
    let empty = HashMap::new();
    let ucs = st.use_cases.get(instance_id).unwrap_or(&empty);
    let rows: Vec<Value> = ucs
        .values()
        .filter(|uc| {
            uc.get("__integration_association_id")
                == Some(&Value::String(integration_association_id.to_string()))
        })
        .map(|uc| {
            json!({
                "UseCaseId": uc["UseCaseId"],
                "UseCaseArn": uc["UseCaseArn"],
                "UseCaseType": uc["UseCaseType"],
            })
        })
        .collect();
    ok(json!({"UseCaseSummaryList": rows}))
}

// ---------------------------------------------------------------------------
// Email addresses
// ---------------------------------------------------------------------------

fn op_create_email_address(
    st: &mut ConnectState,
    instance_id: &str,
    body: &Map<String, Value>,
) -> Response {
    let instance = match st.instances.get(instance_id) {
        Some(i) => i.clone(),
        None => return notfound_instance(instance_id),
    };
    let email_address = match body_str(body, "EmailAddress") {
        Ok(v) => v,
        Err(resp) => return resp,
    };
    let email_id = uuid::Uuid::new_v4().to_string();
    let arn = format!("{}/email-address/{email_id}", str_field(&instance, "Arn"));
    let stamp = now_iso();
    let mut stored = Map::new();
    stored.insert("EmailAddressId".into(), json!(email_id.clone()));
    stored.insert("EmailAddressArn".into(), json!(arn.clone()));
    stored.insert("EmailAddress".into(), json!(email_address));
    stored.insert(
        "DisplayName".into(),
        json!(opt_string(body, "DisplayName").unwrap_or_default()),
    );
    stored.insert(
        "Description".into(),
        json!(opt_string(body, "Description").unwrap_or_default()),
    );
    stored.insert(
        "Tags".into(),
        opt_val(body, "Tags").unwrap_or_else(|| json!({})),
    );
    stored.insert("CreateTimestamp".into(), json!(stamp.clone()));
    stored.insert("ModifiedTimestamp".into(), json!(stamp));
    st.email_addresses
        .entry(instance_id.to_string())
        .or_default()
        .insert(email_id.clone(), Value::Object(stored));
    tag_resource(st, &arn, opt_val(body, "Tags"));
    ok(json!({"EmailAddressId": email_id, "EmailAddressArn": arn}))
}

fn op_describe_email_address(st: &mut ConnectState, instance_id: &str, email_id: &str) -> Response {
    if !st.instances.contains_key(instance_id) {
        return notfound_instance(instance_id);
    }
    match st
        .email_addresses
        .get(instance_id)
        .and_then(|m| m.get(email_id))
    {
        // moto returns the bare to_dict for DescribeEmailAddress
        Some(e) => ok(e.clone()),
        None => not_found(format!("Email address {email_id} not found")),
    }
}

fn op_delete_email_address(st: &mut ConnectState, instance_id: &str, email_id: &str) -> Response {
    if !st.instances.contains_key(instance_id) {
        return notfound_instance(instance_id);
    }
    match st
        .email_addresses
        .get_mut(instance_id)
        .and_then(|m| m.remove(email_id))
    {
        Some(_) => ok(json!({})),
        None => not_found(format!("Email address {email_id} not found")),
    }
}

// ---------------------------------------------------------------------------
// Notifications
// ---------------------------------------------------------------------------

fn op_create_notification(
    st: &mut ConnectState,
    instance_id: &str,
    body: &Map<String, Value>,
) -> Response {
    let instance = match st.instances.get(instance_id) {
        Some(i) => i.clone(),
        None => return notfound_instance(instance_id),
    };
    let recipients = match opt_val(body, "Recipients") {
        Some(v) => v,
        None => return missing_body_param("Recipients"),
    };
    let content = match opt_val(body, "Content") {
        Some(v) => v,
        None => return missing_body_param("Content"),
    };
    let notification_id = uuid::Uuid::new_v4().to_string();
    let arn = format!(
        "{}/notification/{notification_id}",
        str_field(&instance, "Arn")
    );
    let stamp = now_iso();
    let mut stored = Map::new();
    stored.insert("Id".into(), json!(notification_id.clone()));
    stored.insert("Arn".into(), json!(arn.clone()));
    stored.insert("Content".into(), content);
    stored.insert("Recipients".into(), recipients);
    stored.insert("CreatedAt".into(), json!(stamp.clone()));
    stored.insert("LastModifiedTime".into(), json!(stamp.clone()));
    stored.insert("LastModifiedRegion".into(), json!(region_of(&instance)));
    stored.insert(
        "Tags".into(),
        opt_val(body, "Tags").unwrap_or_else(|| json!({})),
    );
    if let Some(priority) = opt_val(body, "Priority").filter(|v| !v.is_null()) {
        stored.insert("Priority".into(), priority);
    }
    if let Some(expires) = opt_val(body, "ExpiresAt").filter(|v| !v.is_null()) {
        stored.insert("ExpiresAt".into(), expires);
    }
    st.notifications
        .entry(instance_id.to_string())
        .or_default()
        .insert(notification_id.clone(), Value::Object(stored));
    ok(json!({"NotificationId": notification_id, "NotificationArn": arn}))
}

fn op_describe_notification(
    st: &mut ConnectState,
    instance_id: &str,
    notification_id: &str,
) -> Response {
    if !st.instances.contains_key(instance_id) {
        return notfound_instance(instance_id);
    }
    match st
        .notifications
        .get(instance_id)
        .and_then(|m| m.get(notification_id))
    {
        // moto returns the bare to_dict for DescribeNotification
        Some(n) => ok(n.clone()),
        None => not_found(format!("Notification {notification_id} not found")),
    }
}

fn op_delete_notification(
    st: &mut ConnectState,
    instance_id: &str,
    notification_id: &str,
) -> Response {
    if !st.instances.contains_key(instance_id) {
        return notfound_instance(instance_id);
    }
    match st
        .notifications
        .get_mut(instance_id)
        .and_then(|m| m.remove(notification_id))
    {
        Some(_) => ok(json!({})),
        None => not_found(format!("Notification {notification_id} not found")),
    }
}

fn op_list_notifications(st: &mut ConnectState, instance_id: &str) -> Response {
    if !st.instances.contains_key(instance_id) {
        return notfound_instance(instance_id);
    }
    let empty = HashMap::new();
    let rows: Vec<Value> = st
        .notifications
        .get(instance_id)
        .unwrap_or(&empty)
        .values()
        .cloned()
        .collect();
    ok(json!({"NotificationSummaryList": rows}))
}

// ---------------------------------------------------------------------------
// Workspaces
// ---------------------------------------------------------------------------

fn op_create_workspace(
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
    let workspace_id = uuid::Uuid::new_v4().to_string();
    let arn = format!("{}/workspace/{workspace_id}", str_field(&instance, "Arn"));
    let stamp = now_iso();
    let mut stored = Map::new();
    stored.insert("WorkspaceId".into(), json!(workspace_id.clone()));
    stored.insert("WorkspaceArn".into(), json!(arn.clone()));
    stored.insert("Name".into(), json!(name));
    stored.insert(
        "Description".into(),
        json!(opt_string(body, "Description").unwrap_or_default()),
    );
    stored.insert(
        "Tags".into(),
        opt_val(body, "Tags").unwrap_or_else(|| json!({})),
    );
    stored.insert("CreateTimestamp".into(), json!(stamp.clone()));
    stored.insert("ModifiedTimestamp".into(), json!(stamp));
    st.workspaces
        .entry(instance_id.to_string())
        .or_default()
        .insert(workspace_id.clone(), Value::Object(stored));
    tag_resource(st, &arn, opt_val(body, "Tags"));
    ok(json!({"WorkspaceId": workspace_id, "WorkspaceArn": arn}))
}

fn op_describe_workspace(st: &mut ConnectState, instance_id: &str, workspace_id: &str) -> Response {
    if !st.instances.contains_key(instance_id) {
        return notfound_instance(instance_id);
    }
    match st
        .workspaces
        .get(instance_id)
        .and_then(|m| m.get(workspace_id))
    {
        Some(w) => ok(json!({"Workspace": w.clone()})),
        None => not_found(format!("Workspace {workspace_id} not found")),
    }
}

fn op_delete_workspace(st: &mut ConnectState, instance_id: &str, workspace_id: &str) -> Response {
    if !st.instances.contains_key(instance_id) {
        return notfound_instance(instance_id);
    }
    match st
        .workspaces
        .get_mut(instance_id)
        .and_then(|m| m.remove(workspace_id))
    {
        Some(_) => ok(json!({})),
        None => not_found(format!("Workspace {workspace_id} not found")),
    }
}

fn op_list_workspaces(st: &mut ConnectState, instance_id: &str) -> Response {
    if !st.instances.contains_key(instance_id) {
        return notfound_instance(instance_id);
    }
    let empty = HashMap::new();
    let rows: Vec<Value> = st
        .workspaces
        .get(instance_id)
        .unwrap_or(&empty)
        .values()
        .cloned()
        .collect();
    ok(json!({"WorkspaceSummaryList": rows}))
}

// ---------------------------------------------------------------------------
// Test cases
// ---------------------------------------------------------------------------

fn op_create_test_case(
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
    let case_id = uuid::Uuid::new_v4().to_string();
    let arn = format!("{}/test-case/{case_id}", str_field(&instance, "Arn"));
    let stamp = now_iso();
    let mut stored = Map::new();
    stored.insert("TestCaseId".into(), json!(case_id.clone()));
    stored.insert("TestCaseArn".into(), json!(arn.clone()));
    stored.insert("Name".into(), json!(name));
    stored.insert(
        "Status".into(),
        json!(opt_string(body, "Status").unwrap_or_else(|| "DRAFT".into())),
    );
    stored.insert(
        "Description".into(),
        json!(opt_string(body, "Description").unwrap_or_default()),
    );
    stored.insert(
        "Tags".into(),
        opt_val(body, "Tags").unwrap_or_else(|| json!({})),
    );
    stored.insert("CreateTimestamp".into(), json!(stamp.clone()));
    stored.insert("ModifiedTimestamp".into(), json!(stamp));
    st.test_cases
        .entry(instance_id.to_string())
        .or_default()
        .insert(case_id.clone(), Value::Object(stored));
    ok(json!({"TestCaseId": case_id, "TestCaseArn": arn}))
}

fn op_describe_test_case(st: &mut ConnectState, instance_id: &str, case_id: &str) -> Response {
    if !st.instances.contains_key(instance_id) {
        return notfound_instance(instance_id);
    }
    match st.test_cases.get(instance_id).and_then(|m| m.get(case_id)) {
        Some(c) => ok(json!({"TestCase": c.clone()})),
        None => not_found(format!("Test case {case_id} not found")),
    }
}

fn op_update_test_case(
    st: &mut ConnectState,
    instance_id: &str,
    case_id: &str,
    body: &Map<String, Value>,
) -> Response {
    if !st.instances.contains_key(instance_id) {
        return notfound_instance(instance_id);
    }
    let case = match st
        .test_cases
        .get_mut(instance_id)
        .and_then(|m| m.get_mut(case_id))
    {
        Some(c) => c,
        None => return not_found(format!("Test case {case_id} not found")),
    };
    if let Some(name) = opt_val(body, "Name") {
        case["Name"] = name;
    }
    if let Some(status) = opt_val(body, "Status") {
        case["Status"] = status;
    }
    if let Some(desc) = opt_val(body, "Description") {
        case["Description"] = desc;
    }
    if let Some(tags) = opt_val(body, "Tags") {
        case["Tags"] = tags;
    }
    case["ModifiedTimestamp"] = json!(now_iso());
    ok(json!({"TestCase": case.clone()}))
}

fn op_delete_test_case(st: &mut ConnectState, instance_id: &str, case_id: &str) -> Response {
    if !st.instances.contains_key(instance_id) {
        return notfound_instance(instance_id);
    }
    match st
        .test_cases
        .get_mut(instance_id)
        .and_then(|m| m.remove(case_id))
    {
        Some(_) => ok(json!({})),
        None => not_found(format!("Test case {case_id} not found")),
    }
}

fn op_list_test_cases(st: &mut ConnectState, instance_id: &str) -> Response {
    if !st.instances.contains_key(instance_id) {
        return notfound_instance(instance_id);
    }
    let empty = HashMap::new();
    let rows: Vec<Value> = st
        .test_cases
        .get(instance_id)
        .unwrap_or(&empty)
        .values()
        .cloned()
        .collect();
    ok(json!({"TestCaseSummaryList": rows}))
}

// ---------------------------------------------------------------------------
// Data tables + attributes
// ---------------------------------------------------------------------------

fn op_create_data_table(
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
    let table_id = uuid::Uuid::new_v4().to_string();
    let arn = format!("{}/data-table/{table_id}", str_field(&instance, "Arn"));
    let stamp = now_iso();
    let mut stored = Map::new();
    stored.insert("DataTableId".into(), json!(table_id.clone()));
    stored.insert("DataTableArn".into(), json!(arn.clone()));
    stored.insert("Name".into(), json!(name));
    stored.insert(
        "Schema".into(),
        opt_val(body, "Schema").unwrap_or_else(|| json!({})),
    );
    stored.insert(
        "Tags".into(),
        opt_val(body, "Tags").unwrap_or_else(|| json!({})),
    );
    stored.insert("CreateTimestamp".into(), json!(stamp.clone()));
    stored.insert("ModifiedTimestamp".into(), json!(stamp));
    st.data_tables
        .entry(instance_id.to_string())
        .or_default()
        .insert(table_id.clone(), Value::Object(stored));
    tag_resource(st, &arn, opt_val(body, "Tags"));
    ok(json!({
        "Id": table_id,
        "Arn": arn,
        "LockVersion": {"DataTable": "1", "Attribute": "1", "PrimaryValues": "1", "Value": "1"},
    }))
}

fn op_describe_data_table(st: &mut ConnectState, instance_id: &str, table_id: &str) -> Response {
    if !st.instances.contains_key(instance_id) {
        return notfound_instance(instance_id);
    }
    match st
        .data_tables
        .get(instance_id)
        .and_then(|m| m.get(table_id))
    {
        Some(t) => ok(json!({"DataTable": t.clone()})),
        None => not_found(format!("Data table {table_id} not found")),
    }
}

fn op_delete_data_table(st: &mut ConnectState, instance_id: &str, table_id: &str) -> Response {
    if !st.instances.contains_key(instance_id) {
        return notfound_instance(instance_id);
    }
    match st
        .data_tables
        .get_mut(instance_id)
        .and_then(|m| m.remove(table_id))
    {
        Some(_) => {
            st.data_table_attributes
                .get_mut(instance_id)
                .and_then(|m| m.remove(table_id));
            ok(json!({}))
        }
        None => not_found(format!("Data table {table_id} not found")),
    }
}

fn op_list_data_tables(st: &mut ConnectState, instance_id: &str) -> Response {
    if !st.instances.contains_key(instance_id) {
        return notfound_instance(instance_id);
    }
    let empty = HashMap::new();
    let rows: Vec<Value> = st
        .data_tables
        .get(instance_id)
        .unwrap_or(&empty)
        .values()
        .cloned()
        .collect();
    ok(json!({"DataTableSummaryList": rows}))
}

fn op_create_data_table_attribute(
    st: &mut ConnectState,
    instance_id: &str,
    table_id: &str,
    body: &Map<String, Value>,
) -> Response {
    if !st.instances.contains_key(instance_id) {
        return notfound_instance(instance_id);
    }
    if st
        .data_tables
        .get(instance_id)
        .and_then(|m| m.get(table_id))
        .is_none()
    {
        return not_found(format!("Data table {table_id} not found"));
    }
    let name = match body_str(body, "Name") {
        Ok(v) => v,
        Err(resp) => return resp,
    };
    let value_type = match body_str(body, "ValueType") {
        Ok(v) => v,
        Err(resp) => return resp,
    };
    let attribute_id = uuid::Uuid::new_v4().to_string();
    let stamp = now_iso();
    let mut stored = Map::new();
    stored.insert("AttributeName".into(), json!(name.clone()));
    stored.insert("DataType".into(), json!(value_type));
    stored.insert(
        "Required".into(),
        json!(body
            .get("Required")
            .and_then(Value::as_bool)
            .unwrap_or(false)),
    );
    if let Some(dv) = opt_val(body, "DefaultValue").filter(|v| !v.is_null()) {
        stored.insert("DefaultValue".into(), dv);
    }
    stored.insert("CreateTimestamp".into(), json!(stamp.clone()));
    stored.insert("ModifiedTimestamp".into(), json!(stamp));
    st.data_table_attributes
        .entry(instance_id.to_string())
        .or_default()
        .entry(table_id.to_string())
        .or_default()
        .insert(name.clone(), Value::Object(stored));
    ok(json!({"Name": name, "AttributeId": attribute_id, "LockVersion": {}}))
}

fn data_table_attr_to_dict(a: &Value) -> Value {
    let mut out = Map::new();
    for f in [
        "AttributeName",
        "DataType",
        "Required",
        "CreateTimestamp",
        "ModifiedTimestamp",
    ] {
        out.insert(f.to_string(), a[f].clone());
    }
    if let Some(dv) = a.get("DefaultValue").filter(|v| !v.is_null()) {
        out.insert("DefaultValue".into(), dv.clone());
    }
    Value::Object(out)
}

fn op_describe_data_table_attribute(
    st: &mut ConnectState,
    instance_id: &str,
    table_id: &str,
    attribute_name: &str,
) -> Response {
    if !st.instances.contains_key(instance_id) {
        return notfound_instance(instance_id);
    }
    match st
        .data_table_attributes
        .get(instance_id)
        .and_then(|m| m.get(table_id))
        .and_then(|a| a.get(attribute_name))
    {
        Some(a) => ok(json!({"DataTableAttribute": data_table_attr_to_dict(a)})),
        None => not_found(format!("Data table attribute {attribute_name} not found")),
    }
}

fn op_list_data_table_attributes(
    st: &mut ConnectState,
    instance_id: &str,
    table_id: &str,
) -> Response {
    if !st.instances.contains_key(instance_id) {
        return notfound_instance(instance_id);
    }
    let rows: Vec<Value> = st
        .data_table_attributes
        .get(instance_id)
        .and_then(|m| m.get(table_id))
        .map(|a| a.values().map(data_table_attr_to_dict).collect())
        .unwrap_or_default();
    ok(json!({"DataTableAttributeSummaryList": rows}))
}

fn op_update_data_table_attribute(
    st: &mut ConnectState,
    instance_id: &str,
    table_id: &str,
    attribute_name: &str,
    body: &Map<String, Value>,
) -> Response {
    if !st.instances.contains_key(instance_id) {
        return notfound_instance(instance_id);
    }
    let attr = match st
        .data_table_attributes
        .get_mut(instance_id)
        .and_then(|m| m.get_mut(table_id))
        .and_then(|a| a.get_mut(attribute_name))
    {
        Some(a) => a,
        None => return not_found(format!("Data table attribute {attribute_name} not found")),
    };
    if let Some(dt) = opt_val(body, "DataType") {
        attr["DataType"] = dt;
    }
    if let Some(dv) = opt_val(body, "DefaultValue") {
        attr["DefaultValue"] = dv;
    }
    if let Some(required) = opt_val(body, "Required") {
        attr["Required"] = required;
    }
    attr["ModifiedTimestamp"] = json!(now_iso());
    ok(json!({"DataTableAttribute": data_table_attr_to_dict(attr)}))
}

fn op_delete_data_table_attribute(
    st: &mut ConnectState,
    instance_id: &str,
    table_id: &str,
    attribute_name: &str,
) -> Response {
    if !st.instances.contains_key(instance_id) {
        return notfound_instance(instance_id);
    }
    match st
        .data_table_attributes
        .get_mut(instance_id)
        .and_then(|m| m.get_mut(table_id))
        .and_then(|a| a.remove(attribute_name))
    {
        Some(_) => ok(json!({})),
        None => not_found(format!("Data table attribute {attribute_name} not found")),
    }
}

// ---------------------------------------------------------------------------
// Traffic distribution groups (global store)
// ---------------------------------------------------------------------------

fn op_create_traffic_distribution_group(
    st: &mut ConnectState,
    body: &Map<String, Value>,
    region: &str,
    account: u64,
) -> Response {
    let name = match body_str(body, "Name") {
        Ok(v) => v,
        Err(resp) => return resp,
    };
    let instance_id = opt_string(body, "InstanceId").unwrap_or_default();
    let tdg_id = uuid::Uuid::new_v4().to_string();
    let arn = format!("arn:aws:connect:{region}:{account}:traffic-distribution-group/{tdg_id}");
    let mut stored = Map::new();
    stored.insert("Id".into(), json!(tdg_id.clone()));
    stored.insert("Arn".into(), json!(arn.clone()));
    stored.insert("Name".into(), json!(name));
    stored.insert(
        "Description".into(),
        json!(opt_string(body, "Description").unwrap_or_default()),
    );
    stored.insert(
        "InstanceArn".into(),
        json!(format!(
            "arn:aws:connect:{region}:{account}:instance/{instance_id}"
        )),
    );
    stored.insert("Status".into(), json!("ACTIVE"));
    stored.insert(
        "Tags".into(),
        opt_val(body, "Tags").unwrap_or_else(|| json!({})),
    );
    stored.insert("IsDefault".into(), json!(false));
    st.traffic_distribution_groups
        .insert(tdg_id.clone(), Value::Object(stored));
    ok(json!({"Id": tdg_id, "Arn": arn}))
}

fn op_describe_traffic_distribution_group(st: &mut ConnectState, tdg_id: &str) -> Response {
    match st.traffic_distribution_groups.get(tdg_id) {
        Some(t) => ok(json!({"TrafficDistributionGroup": t.clone()})),
        None => not_found(format!("Traffic distribution group {tdg_id} not found")),
    }
}

fn op_delete_traffic_distribution_group(st: &mut ConnectState, tdg_id: &str) -> Response {
    match st.traffic_distribution_groups.remove(tdg_id) {
        Some(_) => ok(json!({})),
        None => not_found(format!("Traffic distribution group {tdg_id} not found")),
    }
}

fn op_list_traffic_distribution_groups(
    st: &mut ConnectState,
    query: &BTreeMap<String, Vec<String>>,
) -> Response {
    let instance_filter = q1(query, "instanceId");
    let rows: Vec<Value> = st
        .traffic_distribution_groups
        .values()
        .filter(|tdg| match instance_filter {
            Some(instance_id) => {
                let suffix = format!("instance/{instance_id}");
                str_field(tdg, "InstanceArn").ends_with(&suffix)
            }
            None => true,
        })
        .cloned()
        .collect();
    ok(json!({"TrafficDistributionGroupSummaryList": rows}))
}

// ---------------------------------------------------------------------------
// Search ops + list-contact-evaluations
// ---------------------------------------------------------------------------

/// moto `search_X(instance_id)`: instance-guarded full dump with
/// `ApproximateTotalCount`. (SearchCriteria is stored, never applied.)
fn op_search(
    st: &mut ConnectState,
    instance_id: &str,
    field: &str,
    collect: impl FnOnce(&mut ConnectState, &str) -> Option<Vec<Value>>,
) -> Response {
    if !st.instances.contains_key(instance_id) {
        return notfound_instance(instance_id);
    }
    let rows: Vec<Value> = collect(st, instance_id).unwrap_or_default();
    ok(json!({ field: rows, "ApproximateTotalCount": rows.len() }))
}

/// moto `list_contact_evaluations` returns an empty summary list.
fn op_list_evaluations(st: &mut ConnectState, instance_id: &str) -> Response {
    if !st.instances.contains_key(instance_id) {
        return notfound_instance(instance_id);
    }
    ok(json!({"EvaluationSummaryList": []}))
}

// ---------------------------------------------------------------------------
// Instance-family ops (surface wave) — moto is absent for every op in this
// section; the botocore wire model is the contract: 2xx happy path with all
// required output members synthesized, plus the modeled 4xx the module's
// existing families give on the same fills.
// ---------------------------------------------------------------------------

fn notfound_flow_association(resource_id: &str) -> Response {
    not_found(format!("Flow association {resource_id} not found"))
}

// -- Flow associations (AssociateFlow family) ------------------------------

fn op_associate_flow(
    st: &mut ConnectState,
    instance_id: &str,
    body: &Map<String, Value>,
) -> Response {
    let resource_id = match body_str(body, "ResourceId") {
        Ok(v) => v,
        Err(resp) => return resp,
    };
    let flow_id = match body_str(body, "FlowId") {
        Ok(v) => v,
        Err(resp) => return resp,
    };
    let resource_type = match body_str(body, "ResourceType") {
        Ok(v) => v,
        Err(resp) => return resp,
    };
    if !st.instances.contains_key(instance_id) {
        return notfound_instance(instance_id);
    }
    let mut stored = Map::new();
    stored.insert("ResourceId".into(), json!(resource_id.clone()));
    stored.insert("FlowId".into(), json!(flow_id.clone()));
    stored.insert("ResourceType".into(), json!(resource_type.clone()));
    st.flow_associations
        .entry(instance_id.to_string())
        .or_default()
        .insert(resource_id.clone(), stored.clone());
    ok(json!({}))
}

fn op_get_flow_association(
    st: &mut ConnectState,
    instance_id: &str,
    resource_id: &str,
    resource_type: &str,
) -> Response {
    if !st.instances.contains_key(instance_id) {
        return notfound_instance(instance_id);
    }
    match st
        .flow_associations
        .get(instance_id)
        .and_then(|m| m.get(resource_id))
    {
        Some(assoc) => ok(logical_assoc(assoc, resource_type)),
        None => notfound_flow_association(resource_id),
    }
}

fn logical_assoc(assoc: &Map<String, Value>, resource_type: &str) -> Value {
    let mut out = Map::new();
    out.insert("ResourceId".into(), json!(assoc["ResourceId"].clone()));
    out.insert("FlowId".into(), json!(assoc["FlowId"].clone()));
    // The URI ResourceType shadows the stored one at query time.
    out.insert("ResourceType".into(), json!(resource_type.to_string()));
    Value::Object(out)
}

fn op_disassociate_flow(
    st: &mut ConnectState,
    instance_id: &str,
    resource_id: &str,
    _resource_type: &str,
) -> Response {
    if !st.instances.contains_key(instance_id) {
        return notfound_instance(instance_id);
    }
    let present = st
        .flow_associations
        .get(instance_id)
        .is_some_and(|m| m.contains_key(resource_id));
    if !present {
        return notfound_flow_association(resource_id);
    }
    if let Some(map) = st.flow_associations.get_mut(instance_id) {
        map.remove(resource_id);
    }
    ok(json!({}))
}

fn op_batch_get_flow_association(
    st: &mut ConnectState,
    instance_id_param: &str,
    body: &Map<String, Value>,
) -> Response {
    let instance_id = instance_id_param.to_string();
    let resource_ids = match opt_val(body, "ResourceIds") {
        Some(Value::Array(v)) => v.clone(),
        Some(_) => return invalid("ResourceIds must be a list"),
        None => return missing_body_param("ResourceIds"),
    };
    let resource_type = opt_string(body, "ResourceType").unwrap_or_default();
    if !st.instances.contains_key(&instance_id) {
        return notfound_instance(&instance_id);
    }
    let associations = st
        .flow_associations
        .get(&instance_id)
        .cloned()
        .unwrap_or_default();
    let rows: Vec<Value> = resource_ids
        .iter()
        .filter_map(|rid| rid.as_str())
        .filter_map(|rid| associations.get(rid))
        .map(|a| {
            // honour the stored association type when the probe omits it
            let stored = a
                .get("ResourceType")
                .and_then(Value::as_str)
                .unwrap_or(&resource_type);
            logical_assoc(a, stored)
        })
        .collect();
    ok(json!({"FlowAssociationSummaryList": rows}))
}

include!("connect_f5_handlers_s.rs");

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

        // disassociate (the batches answered) — modeled 200 with Deleted+Errors
        let batch = request(
            "POST",
            &format!("/analytics-data/instance/{id}/associations"),
            json!({"DataSetIds": ["ds-1"]}),
            "",
        );
        assert_eq!(batch.status, 200);
        let out = body_of(&batch);
        assert_eq!(out["Deleted"].as_array().unwrap().len(), 1);
        assert_eq!(out["Errors"].as_array().unwrap().len(), 0);
        let listed = request(
            "GET",
            &format!("/analytics-data/instance/{id}/association"),
            json!({}),
            "",
        );
        assert_eq!(body_of(&listed)["Results"].as_array().unwrap().len(), 1);
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
        let qc1_row = rows
            .iter()
            .find(|r| r["Name"] == "QC1")
            .expect("QC1 in list");
        assert_eq!(qc1_row["QuickConnectType"], "PHONE_NUMBER");

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

    // -- Phone numbers ---------------------------------------------------------

    #[test]
    fn phone_number_claim_describe_release() {
        let (id, arn) = make_instance(json!({}));
        let claim = request(
            "POST",
            "/phone-number/claim",
            json!({"PhoneNumber": "+15551234567", "InstanceId": id.clone()}),
            "",
        );
        let created = body_of(&claim);
        let pnid = created["PhoneNumberId"].as_str().unwrap().to_string();
        assert!(created["PhoneNumberArn"]
            .as_str()
            .unwrap()
            .ends_with(&format!("/phone-number/{pnid}")));

        let desc = request("GET", &format!("/phone-number/{pnid}"), json!({}), "");
        let described = body_of(&desc);
        let pn = &described["ClaimedPhoneNumberSummary"];
        assert_eq!(pn["PhoneNumber"], "+15551234567");
        assert_eq!(pn["PhoneNumberCountryCode"], "US");
        assert_eq!(pn["PhoneNumberType"], "DID");
        assert_eq!(pn["TargetArn"], arn.as_str());
        assert_eq!(pn["InstanceId"], id.as_str());
        assert_eq!(pn["PhoneNumberStatus"]["Status"], "CLAIMED");

        // duplicate claim fails
        let dup = request(
            "POST",
            "/phone-number/claim",
            json!({"PhoneNumber": "+15551234567", "InstanceId": id.clone()}),
            "",
        );
        assert_eq!(dup.status, 409);

        // unknown instance
        let bad = request(
            "POST",
            "/phone-number/claim",
            json!({"PhoneNumber": "+15559999", "InstanceId": "nope"}),
            "",
        );
        assert_eq!(bad.status, 404);

        // v2 list is global
        let v2 = request("POST", "/phone-number/list", json!({}), "");
        let v2_body = body_of(&v2);
        let rows = v2_body["ListPhoneNumbersSummaryList"].as_array().unwrap();
        assert!(rows.iter().any(|r| r["PhoneNumberId"] == pnid.as_str()));

        // per-instance list
        let listed = request(
            "GET",
            &format!("/phone-numbers-summary/{id}"),
            json!({}),
            "",
        );
        let listed_body = body_of(&listed);
        assert_eq!(
            listed_body["PhoneNumberSummaryList"]
                .as_array()
                .unwrap()
                .len(),
            1
        );

        // release
        let rel = request("DELETE", &format!("/phone-number/{pnid}"), json!({}), "");
        assert_eq!(status_of(&rel), 200);
        let after = request("GET", &format!("/phone-number/{pnid}"), json!({}), "");
        assert_eq!(status_of(&after), 404);
    }

    #[test]
    fn search_available_phone_numbers() {
        let (id, _) = make_instance(json!({}));
        let search = request(
            "POST",
            "/phone-number/search-available",
            json!({"TargetArn": format!("arn:aws:connect:us-east-1:123456789012:instance/{id}"),
                   "PhoneNumberCountryCode": "US", "PhoneNumberType": "DID"}),
            "",
        );
        let out = body_of(&search);
        let rows = out["AvailableNumbersList"].as_array().unwrap();
        assert_eq!(rows.len(), 10);
        assert!(rows[0]["PhoneNumber"].as_str().unwrap().starts_with("+1"));
        assert_eq!(rows[0]["PhoneNumberCountryCode"], "US");

        let search = request(
            "POST",
            "/phone-number/search-available",
            json!({"TargetArn": "arn:aws:connect:us-east-1:123456789012:instance/x",
                   "PhoneNumberCountryCode": "US", "PhoneNumberType": "DID", "MaxResults": 25}),
            "",
        );
        assert_eq!(
            body_of(&search)["AvailableNumbersList"]
                .as_array()
                .unwrap()
                .len(),
            25
        );
    }

    // -- Prompts -----------------------------------------------------------------

    #[test]
    fn prompt_lifecycle() {
        let (id, _) = make_instance(json!({}));
        let create = request(
            "PUT",
            &format!("/prompts/{id}"),
            json!({"Name": "P1", "S3Uri": "s3://bucket/p.wav"}),
            "",
        );
        let created = body_of(&create);
        let pid = created["PromptId"].as_str().unwrap().to_string();
        assert!(created["PromptARN"]
            .as_str()
            .unwrap()
            .ends_with(&format!("/prompt/{pid}")));

        let desc = request("GET", &format!("/prompts/{id}/{pid}"), json!({}), "");
        let described = body_of(&desc);
        assert_eq!(described["Prompt"]["Name"], "P1");
        // moto's Prompt.to_dict serializes PromptARN/PromptId fields
        assert!(described["Prompt"].get("PromptARN").is_some());
        assert!(described["Prompt"].get("S3Uri").is_none());

        let upd = request(
            "POST",
            &format!("/prompts/{id}/{pid}"),
            json!({"Name": "P2", "Description": "d"}),
            "",
        );
        let upd_body = body_of(&upd);
        assert_eq!(upd_body["PromptId"], pid.as_str());

        let listed = request("GET", &format!("/prompts-summary/{id}"), json!({}), "");
        let list_body = body_of(&listed);
        assert_eq!(list_body["PromptSummaryList"][0]["Name"], "P2");

        let del = request("DELETE", &format!("/prompts/{id}/{pid}"), json!({}), "");
        assert_eq!(status_of(&del), 200);
        let after = request("GET", &format!("/prompts/{id}/{pid}"), json!({}), "");
        assert_eq!(status_of(&after), 404);
    }

    // -- Views --------------------------------------------------------------------

    #[test]
    fn view_lifecycle() {
        let (id, _) = make_instance(json!({}));
        let content = json!({"Title": "V", "Actions": [], "Type": {"Classifier": []}});
        let create = request(
            "PUT",
            &format!("/views/{id}"),
            json!({"Name": "V1", "Status": "SAVED", "Content": content}),
            "",
        );
        let created = body_of(&create);
        let view = &created["View"];
        let vid = view["Id"].as_str().unwrap().to_string();
        assert!(view["Arn"]
            .as_str()
            .unwrap()
            .ends_with(&format!("/view/{vid}")));
        assert_eq!(view["Version"], 1);
        assert_eq!(view["Type"], "CUSTOMER_MANAGED");

        let desc = request("GET", &format!("/views/{id}/{vid}"), json!({}), "");
        let described = body_of(&desc);
        assert_eq!(described["View"]["Name"], "V1");
        assert_eq!(described["View"]["Content"]["Title"], "V");

        let upd = request(
            "POST",
            &format!("/views/{id}/{vid}"),
            json!({"Content": {"Title": "V2"}}),
            "",
        );
        let upd_body = body_of(&upd);
        assert_eq!(upd_body["View"]["Version"], 2);
        assert_eq!(upd_body["View"]["Content"]["Title"], "V2");

        let listed = request("GET", &format!("/views/{id}"), json!({}), "");
        let list_body = body_of(&listed);
        assert_eq!(list_body["ViewsSummaryList"][0]["Name"], "V1");

        let del = request("DELETE", &format!("/views/{id}/{vid}"), json!({}), "");
        assert_eq!(status_of(&del), 200);
        let after = request("GET", &format!("/views/{id}/{vid}"), json!({}), "");
        assert_eq!(status_of(&after), 404);
    }

    // -- Vocabularies ------------------------------------------------------------------

    #[test]
    fn vocabulary_lifecycle_and_search() {
        let (id, _) = make_instance(json!({}));
        let create = request(
            "POST",
            &format!("/vocabulary/{id}"),
            json!({"VocabularyName": "vocab-1", "LanguageCode": "en-US", "Content": "a b c"}),
            "",
        );
        let created = body_of(&create);
        let vid = created["VocabularyId"].as_str().unwrap().to_string();
        assert_eq!(created["State"], "ACTIVE");
        assert!(created["VocabularyArn"]
            .as_str()
            .unwrap()
            .ends_with(&format!("/vocabulary/{vid}")));

        let desc = request("GET", &format!("/vocabulary/{id}/{vid}"), json!({}), "");
        let described = body_of(&desc);
        assert_eq!(described["Vocabulary"]["LanguageCode"], "en-US");
        assert_eq!(described["Vocabulary"]["State"], "ACTIVE");

        let search = request(
            "POST",
            &format!("/vocabulary-summary/{id}"),
            json!({"State": "ACTIVE", "LanguageCode": "en-US", "NameStartsWith": "bad"}),
            "",
        );
        let search_body = body_of(&search);
        assert!(search_body["VocabularySummaryList"]
            .as_array()
            .unwrap()
            .is_empty());

        let search2 = request(
            "POST",
            &format!("/vocabulary-summary/{id}"),
            json!({"LanguageCode": "en-US"}),
            "",
        );
        let search2_body = body_of(&search2);
        assert_eq!(
            search2_body["VocabularySummaryList"]
                .as_array()
                .unwrap()
                .len(),
            1
        );

        let missing_desc = request("GET", "/vocabulary/no/voc", json!({}), "");
        assert_eq!(missing_desc.status, 404);

        let del = request(
            "POST",
            &format!("/vocabulary-remove/{id}/{vid}"),
            json!({}),
            "",
        );
        let del_body = body_of(&del);
        assert_eq!(del_body["State"], "DELETE_IN_PROGRESS");

        let after = request("GET", &format!("/vocabulary/{id}/{vid}"), json!({}), "");
        assert_eq!(status_of(&after), 404);

        // default vocabularies list after removal is empty
        let dv = request(
            "POST",
            &format!("/default-vocabulary-summary/{id}"),
            json!({}),
            "",
        );
        let dv_body = body_of(&dv);
        assert!(dv_body["DefaultVocabularyList"]
            .as_array()
            .unwrap()
            .is_empty());
    }

    // -- Rules ----------------------------------------------------------------------------

    #[test]
    fn rule_lifecycle() {
        let (id, _) = make_instance(json!({}));
        let create = request(
            "POST",
            &format!("/rules/{id}"),
            json!({"Name": "R1", "TriggerEventSource": {"EventSourceName": "OnPostCallAnalysis"},
                   "Function": "{}", "Actions": [{"ActionType": "ASSIST"}], "PublishStatus": "DRAFT"}),
            "",
        );
        let created = body_of(&create);
        let rid = created["RuleId"].as_str().unwrap().to_string();
        assert!(created["RuleArn"]
            .as_str()
            .unwrap()
            .ends_with(&format!("/rule/{rid}")));

        let desc = request("GET", &format!("/rules/{id}/{rid}"), json!({}), "");
        let described = body_of(&desc);
        let rule = &described["Rule"];
        assert_eq!(rule["Name"], "R1");
        assert_eq!(rule["PublishStatus"], "DRAFT");
        assert_eq!(
            rule["TriggerEventSource"]["EventSourceName"],
            "OnPostCallAnalysis"
        );

        let upd = request(
            "PUT",
            &format!("/rules/{id}/{rid}"),
            json!({"Name": "R2", "Function": "{}", "Actions": [{"ActionType": "CREATE_TASK"}],
                   "PublishStatus": "ACTIVE"}),
            "",
        );
        assert_eq!(status_of(&upd), 200);

        let listed = request("GET", &format!("/rules/{id}"), json!({}), "");
        let list_body = body_of(&listed);
        let rows = list_body["RuleSummaryList"].as_array().unwrap();
        assert_eq!(rows[0]["Name"], "R2");
        assert_eq!(rows[0]["PublishStatus"], "ACTIVE");
        assert_eq!(rows[0]["ActionSummaries"][0]["ActionType"], "CREATE_TASK");

        let del = request("DELETE", &format!("/rules/{id}/{rid}"), json!({}), "");
        assert_eq!(status_of(&del), 200);
        let after = request("GET", &format!("/rules/{id}/{rid}"), json!({}), "");
        assert_eq!(status_of(&after), 404);
    }

    // -- Evaluation forms ---------------------------------------------------------------------

    #[test]
    fn evaluation_form_lifecycle() {
        let (id, _) = make_instance(json!({}));
        let create = request(
            "PUT",
            &format!("/evaluation-forms/{id}"),
            json!({"Title": "EF", "Items": [{"Section": {"Title": "s", "RefId": "r", "Items": []}}]}),
            "",
        );
        let created = body_of(&create);
        let fid = created["EvaluationFormId"].as_str().unwrap().to_string();
        assert!(created["EvaluationFormArn"]
            .as_str()
            .unwrap()
            .ends_with(&format!("/evaluation-form/{fid}")));

        let desc = request(
            "GET",
            &format!("/evaluation-forms/{id}/{fid}"),
            json!({}),
            "",
        );
        let described = body_of(&desc);
        let form = &described["EvaluationForm"];
        assert_eq!(form["Title"], "EF");
        assert_eq!(form["Status"], "DRAFT");
        assert!(form["Items"][0]["Section"].is_object());

        let upd = request(
            "PUT",
            &format!("/evaluation-forms/{id}/{fid}"),
            json!({"Title": "EF2"}),
            "",
        );
        let upd_body = body_of(&upd);
        assert_eq!(upd_body["EvaluationFormVersion"], 2);

        let listed = request("GET", &format!("/evaluation-forms/{id}"), json!({}), "");
        let list_body = body_of(&listed);
        let rows = list_body["EvaluationFormSummaryList"].as_array().unwrap();
        assert_eq!(rows[0]["Title"], "EF2");
        assert_eq!(rows[0]["LatestVersion"], 2);

        let versions = request(
            "GET",
            &format!("/evaluation-forms/{id}/{fid}/versions"),
            json!({}),
            "",
        );
        let versions_body = body_of(&versions);
        assert_eq!(
            versions_body["EvaluationFormVersionSummaryList"][0]["EvaluationFormVersion"],
            2
        );

        let activate = request(
            "POST",
            &format!("/evaluation-forms/{id}/{fid}/activate"),
            json!({"EvaluationFormVersion": 2}),
            "",
        );
        let activate_body = body_of(&activate);
        assert_eq!(activate_body["EvaluationFormVersion"], 2);

        let desc = request(
            "GET",
            &format!("/evaluation-forms/{id}/{fid}"),
            json!({}),
            "",
        );
        let described = body_of(&desc);
        assert_eq!(described["EvaluationForm"]["Status"], "ACTIVE");

        let deactivate = request(
            "POST",
            &format!("/evaluation-forms/{id}/{fid}/deactivate"),
            json!({"EvaluationFormVersion": 2}),
            "",
        );
        assert_eq!(status_of(&deactivate), 200);

        let del = request(
            "DELETE",
            &format!("/evaluation-forms/{id}/{fid}"),
            json!({}),
            "",
        );
        assert_eq!(status_of(&del), 200);
        let after = request(
            "GET",
            &format!("/evaluation-forms/{id}/{fid}"),
            json!({}),
            "",
        );
        assert_eq!(status_of(&after), 404);
    }

    #[test]
    fn predefined_attribute_lifecycle() {
        let (id, _) = make_instance(json!({}));
        let create = request(
            "PUT",
            &format!("/predefined-attributes/{id}"),
            json!({"Name": "trade", "Values": {"Text": "x"}}),
            "",
        );
        assert_eq!(status_of(&create), 200);

        let desc = request(
            "GET",
            &format!("/predefined-attributes/{id}/trade"),
            json!({}),
            "",
        );
        let described = body_of(&desc);
        let attr = &described["PredefinedAttribute"];
        assert_eq!(attr["Name"], "trade");
        assert_eq!(attr["Values"]["Text"], "x");

        let listed = request(
            "GET",
            &format!("/predefined-attributes/{id}"),
            json!({}),
            "",
        );
        let list_body = body_of(&listed);
        let rows = list_body["PredefinedAttributeSummaryList"]
            .as_array()
            .unwrap();
        assert_eq!(rows[0]["Name"], "trade");
        assert!(rows[0].get("Values").is_none());

        let upd = request(
            "POST",
            &format!("/predefined-attributes/{id}/trade"),
            json!({"Values": {"Text": "y"}}),
            "",
        );
        assert_eq!(status_of(&upd), 200);

        let del = request(
            "DELETE",
            &format!("/predefined-attributes/{id}/trade"),
            json!({}),
            "",
        );
        assert_eq!(status_of(&del), 200);
        let after = request(
            "GET",
            &format!("/predefined-attributes/{id}/trade"),
            json!({}),
            "",
        );
        assert_eq!(status_of(&after), 404);
    }

    #[test]
    fn task_template_lifecycle() {
        let (id, _) = make_instance(json!({}));
        let create = request(
            "PUT",
            &format!("/instance/{id}/task/template"),
            json!({"Name": "T1", "Fields": [{"Type": "NAME"}], "ContactFlowId": "cf-1"}),
            "",
        );
        let created = body_of(&create);
        let tid = created["Id"].as_str().unwrap().to_string();
        assert!(created["Arn"]
            .as_str()
            .unwrap()
            .ends_with(&format!("/task-template/{tid}")));

        let got = request(
            "GET",
            &format!("/instance/{id}/task/template/{tid}"),
            json!({}),
            "",
        );
        let got_body = body_of(&got);
        // moto returns the bare template for GetTaskTemplate
        assert_eq!(got_body["Name"], "T1");
        assert_eq!(got_body["ContactFlowId"], "cf-1");
        assert_eq!(got_body["Status"], "ACTIVE");
        assert_eq!(got_body["Fields"][0]["Type"], "NAME");

        let listed = request(
            "GET",
            &format!("/instance/{id}/task/template"),
            json!({}),
            "",
        );
        let list_body = body_of(&listed);
        assert_eq!(list_body["TaskTemplates"].as_array().unwrap().len(), 1);

        let upd = request(
            "POST",
            &format!("/instance/{id}/task/template/{tid}"),
            json!({"Name": "T2", "Status": "INACTIVE"}),
            "",
        );
        let upd_body = body_of(&upd);
        assert_eq!(upd_body["Name"], "T2");
        assert_eq!(upd_body["Status"], "INACTIVE");

        let del = request(
            "DELETE",
            &format!("/instance/{id}/task/template/{tid}"),
            json!({}),
            "",
        );
        assert_eq!(status_of(&del), 200);
        let after = request(
            "GET",
            &format!("/instance/{id}/task/template/{tid}"),
            json!({}),
            "",
        );
        assert_eq!(status_of(&after), 404);
    }

    #[test]
    fn integration_association_and_use_case_lifecycle() {
        let (id, _) = make_instance(json!({}));
        let create = request(
            "PUT",
            &format!("/instance/{id}/integration-associations"),
            json!({"IntegrationType": "WISDOM", "IntegrationArn": "arn:aws:wisdom:..."}),
            "",
        );
        let created = body_of(&create);
        let ia = created["IntegrationAssociationId"]
            .as_str()
            .unwrap()
            .to_string();

        let listed = request(
            "GET",
            &format!("/instance/{id}/integration-associations"),
            json!({}),
            "",
        );
        let list_body = body_of(&listed);
        let rows = list_body["IntegrationAssociationSummaryList"]
            .as_array()
            .unwrap();
        assert_eq!(rows[0]["IntegrationType"], "WISDOM");
        assert_eq!(rows[0]["IntegrationAssociationId"], ia.as_str());

        let use_case = request(
            "PUT",
            &format!("/instance/{id}/integration-associations/{ia}/use-cases"),
            json!({"UseCaseType": "RECOMMENDATIONS"}),
            "",
        );
        let uc_body = body_of(&use_case);
        let ucid = uc_body["UseCaseId"].as_str().unwrap().to_string();

        let listed_uc = request(
            "GET",
            &format!("/instance/{id}/integration-associations/{ia}/use-cases"),
            json!({}),
            "",
        );
        let listed_uc_body = body_of(&listed_uc);
        let uc_rows = listed_uc_body["UseCaseSummaryList"].as_array().unwrap();
        assert_eq!(uc_rows.len(), 1);
        assert_eq!(uc_rows[0]["UseCaseType"], "RECOMMENDATIONS");

        // different integration association filters out
        let listed_uc2 = request(
            "GET",
            &format!("/instance/{id}/integration-associations/other/use-cases"),
            json!({}),
            "",
        );
        assert_eq!(
            body_of(&listed_uc2)["UseCaseSummaryList"]
                .as_array()
                .unwrap()
                .len(),
            0
        );

        let del_uc = request(
            "DELETE",
            &format!("/instance/{id}/integration-associations/{ia}/use-cases/{ucid}"),
            json!({}),
            "",
        );
        assert_eq!(status_of(&del_uc), 200);

        // moto's urls.py never routed the bare integration-association DELETE ->
        // 501 Service-not-implemented (byte-parity with the sidecar)
        let del = request(
            "DELETE",
            &format!("/instance/{id}/integration-associations/{ia}"),
            json!({}),
            "",
        );
        assert_eq!(del.status, 200);
        assert_eq!(body_of(&del), json!({}));

        let listed = request(
            "GET",
            &format!("/instance/{id}/integration-associations"),
            json!({}),
            "",
        );
        assert_eq!(
            body_of(&listed)["IntegrationAssociationSummaryList"]
                .as_array()
                .unwrap()
                .len(),
            0
        );
    }

    #[test]
    fn email_address_lifecycle() {
        let (id, _) = make_instance(json!({}));
        let create = request(
            "PUT",
            &format!("/email-addresses/{id}"),
            json!({"EmailAddress": "team@example.com", "DisplayName": "Team"}),
            "",
        );
        let created = body_of(&create);
        let eid = created["EmailAddressId"].as_str().unwrap().to_string();

        let desc = request(
            "GET",
            &format!("/email-addresses/{id}/{eid}"),
            json!({}),
            "",
        );
        // moto returns the bare to_dict
        let described = body_of(&desc);
        assert_eq!(described["EmailAddressId"], eid.as_str());
        assert_eq!(described["EmailAddress"], "team@example.com");
        assert!(described.get("AliasConfigurations").is_none());
        assert!(described.get("Tags").is_some());

        let del = request(
            "DELETE",
            &format!("/email-addresses/{id}/{eid}"),
            json!({}),
            "",
        );
        assert_eq!(status_of(&del), 200);
        let after = request(
            "GET",
            &format!("/email-addresses/{id}/{eid}"),
            json!({}),
            "",
        );
        assert_eq!(status_of(&after), 404);
    }

    #[test]
    fn notification_lifecycle() {
        let (id, _) = make_instance(json!({}));
        let create = request(
            "PUT",
            &format!("/notifications/{id}"),
            json!({"Recipients": ["u1"], "Content": {"Action": "x"}, "Priority": 3}),
            "",
        );
        let created = body_of(&create);
        let nid = created["NotificationId"].as_str().unwrap().to_string();
        assert!(created["NotificationArn"]
            .as_str()
            .unwrap()
            .ends_with(&format!("/notification/{nid}")));

        let desc = request("GET", &format!("/notifications/{id}/{nid}"), json!({}), "");
        let described = body_of(&desc);
        // moto returns the bare to_dict
        assert_eq!(described["Id"], nid.as_str());
        assert_eq!(described["Priority"], 3);
        assert!(described.get("Notification").is_none());

        let listed = request("GET", &format!("/notifications/{id}"), json!({}), "");
        let list_body = body_of(&listed);
        assert_eq!(
            list_body["NotificationSummaryList"]
                .as_array()
                .unwrap()
                .len(),
            1
        );

        let del = request(
            "DELETE",
            &format!("/notifications/{id}/{nid}"),
            json!({}),
            "",
        );
        assert_eq!(status_of(&del), 200);
        let after = request("GET", &format!("/notifications/{id}/{nid}"), json!({}), "");
        assert_eq!(status_of(&after), 404);
    }

    #[test]
    fn workspace_lifecycle() {
        let (id, _) = make_instance(json!({}));
        let create = request(
            "PUT",
            &format!("/workspaces/{id}"),
            json!({"Name": "W1", "Description": "d"}),
            "",
        );
        let created = body_of(&create);
        let wid = created["WorkspaceId"].as_str().unwrap().to_string();
        assert!(created["WorkspaceArn"]
            .as_str()
            .unwrap()
            .ends_with(&format!("/workspace/{wid}")));

        let desc = request("GET", &format!("/workspaces/{id}/{wid}"), json!({}), "");
        let described = body_of(&desc);
        assert_eq!(described["Workspace"]["Name"], "W1");

        let listed = request("GET", &format!("/workspaces/{id}"), json!({}), "");
        let list_body = body_of(&listed);
        assert_eq!(
            list_body["WorkspaceSummaryList"].as_array().unwrap().len(),
            1
        );

        let del = request("DELETE", &format!("/workspaces/{id}/{wid}"), json!({}), "");
        assert_eq!(status_of(&del), 200);
        let after = request("GET", &format!("/workspaces/{id}/{wid}"), json!({}), "");
        assert_eq!(status_of(&after), 404);
    }

    #[test]
    fn test_case_lifecycle() {
        let (id, _) = make_instance(json!({}));
        let create = request(
            "PUT",
            &format!("/test-cases/{id}"),
            json!({"Name": "TC1"}),
            "",
        );
        let created = body_of(&create);
        let tcid = created["TestCaseId"].as_str().unwrap().to_string();
        assert!(created["TestCaseArn"]
            .as_str()
            .unwrap()
            .ends_with(&format!("/test-case/{tcid}")));

        let desc = request("GET", &format!("/test-cases/{id}/{tcid}"), json!({}), "");
        let described = body_of(&desc);
        assert_eq!(described["TestCase"]["Name"], "TC1");
        assert_eq!(described["TestCase"]["Status"], "DRAFT");

        let upd = request(
            "POST",
            &format!("/test-cases/{id}/{tcid}"),
            json!({"Name": "TC2", "Status": "ACTIVE"}),
            "",
        );
        let upd_body = body_of(&upd);
        assert_eq!(upd_body["TestCase"]["Name"], "TC2");

        let listed = request("GET", &format!("/test-cases-summary/{id}"), json!({}), "");
        let list_body = body_of(&listed);
        assert_eq!(
            list_body["TestCaseSummaryList"].as_array().unwrap().len(),
            1
        );

        let del = request("DELETE", &format!("/test-cases/{id}/{tcid}"), json!({}), "");
        assert_eq!(status_of(&del), 200);
        let after = request("GET", &format!("/test-cases/{id}/{tcid}"), json!({}), "");
        assert_eq!(status_of(&after), 404);
    }

    #[test]
    fn data_table_lifecycle() {
        let (id, _) = make_instance(json!({}));
        let create = request(
            "PUT",
            &format!("/data-tables/{id}"),
            json!({"Name": "DT1"}),
            "",
        );
        let created = body_of(&create);
        let tid = created["Id"].as_str().unwrap().to_string();
        assert!(created["LockVersion"].is_object());
        assert_eq!(created["LockVersion"]["DataTable"], "1");

        let desc = request("GET", &format!("/data-tables/{id}/{tid}"), json!({}), "");
        let described = body_of(&desc);
        assert_eq!(described["DataTable"]["Name"], "DT1");
        assert!(described["DataTable"].get("LockVersion").is_none());

        // attribute lifecycle
        let attr = request(
            "PUT",
            &format!("/data-tables/{id}/{tid}/attributes"),
            json!({"Name": "priority", "ValueType": "TEXT", "Required": true}),
            "",
        );
        let attr_body = body_of(&attr);
        assert_eq!(attr_body["Name"], "priority");
        assert!(attr_body["LockVersion"].is_object());

        let attr_desc = request(
            "GET",
            &format!("/data-tables/{id}/{tid}/attributes/priority"),
            json!({}),
            "",
        );
        let attr_desc_body = body_of(&attr_desc);
        assert_eq!(attr_desc_body["DataTableAttribute"]["DataType"], "TEXT");
        assert_eq!(attr_desc_body["DataTableAttribute"]["Required"], true);

        let attr_listed = request(
            "POST",
            &format!("/data-tables/{id}/{tid}/attributes"),
            json!({}),
            "",
        );
        let attr_listed_body = body_of(&attr_listed);
        assert_eq!(
            attr_listed_body["DataTableAttributeSummaryList"]
                .as_array()
                .unwrap()
                .len(),
            1
        );

        let upd = request(
            "POST",
            &format!("/data-tables/{id}/{tid}/attributes/priority"),
            json!({"DefaultValue": "p1"}),
            "",
        );
        let upd_body = body_of(&upd);
        assert_eq!(upd_body["DataTableAttribute"]["DefaultValue"], "p1");

        let del_attr = request(
            "DELETE",
            &format!("/data-tables/{id}/{tid}/attributes/priority"),
            json!({}),
            "",
        );
        assert_eq!(status_of(&del_attr), 200);
        let gone = request(
            "GET",
            &format!("/data-tables/{id}/{tid}/attributes/priority"),
            json!({}),
            "",
        );
        assert_eq!(status_of(&gone), 404);

        // table delete cascades attributes
        request(
            "POST",
            &format!("/data-tables/{id}/{tid}/attributes"),
            json!({"Name": "priority", "ValueType": "TEXT"}),
            "",
        );
        let del = request("DELETE", &format!("/data-tables/{id}/{tid}"), json!({}), "");
        assert_eq!(status_of(&del), 200);
        let after = request("GET", &format!("/data-tables/{id}/{tid}"), json!({}), "");
        assert_eq!(status_of(&after), 404);

        // recreate and confirm attributes were purged with the table
        let create2 = request(
            "PUT",
            &format!("/data-tables/{id}"),
            json!({"Name": "DT2"}),
            "",
        );
        let tid2 = body_of(&create2)["Id"].as_str().unwrap().to_string();
        let attr_listed2 = request(
            "POST",
            &format!("/data-tables/{id}/{tid2}/attributes"),
            json!({}),
            "",
        );
        assert!(body_of(&attr_listed2)["DataTableAttributeSummaryList"]
            .as_array()
            .unwrap()
            .is_empty());
    }

    #[test]
    fn traffic_distribution_group_lifecycle() {
        let (id, _) = make_instance(json!({}));
        let create = request(
            "PUT",
            "/traffic-distribution-group",
            json!({"Name": "TDG", "InstanceId": id.clone()}),
            "",
        );
        let created = body_of(&create);
        let tid = created["Id"].as_str().unwrap().to_string();
        assert!(created["Arn"]
            .as_str()
            .unwrap()
            .contains("traffic-distribution-group/"));

        let desc = request(
            "GET",
            &format!("/traffic-distribution-group/{tid}"),
            json!({}),
            "",
        );
        let described = body_of(&desc);
        let group = &described["TrafficDistributionGroup"];
        assert_eq!(group["Id"], tid.as_str());
        assert_eq!(group["IsDefault"], false);
        assert_eq!(group["Status"], "ACTIVE");
        assert!(group["InstanceArn"]
            .as_str()
            .unwrap()
            .ends_with(&id.as_str()));

        let listed = request(
            "GET",
            "/traffic-distribution-groups",
            json!({}),
            &format!("instanceId={id}"),
        );
        let list_body = body_of(&listed);
        assert_eq!(
            list_body["TrafficDistributionGroupSummaryList"]
                .as_array()
                .unwrap()
                .len(),
            1
        );
        let listed = request(
            "GET",
            "/traffic-distribution-groups",
            json!({}),
            "instanceId=other",
        );
        assert_eq!(
            body_of(&listed)["TrafficDistributionGroupSummaryList"]
                .as_array()
                .unwrap()
                .len(),
            0
        );

        let del = request(
            "DELETE",
            &format!("/traffic-distribution-group/{tid}"),
            json!({}),
            "",
        );
        assert_eq!(status_of(&del), 200);
        let after = request(
            "GET",
            &format!("/traffic-distribution-group/{tid}"),
            json!({}),
            "",
        );
        assert_eq!(status_of(&after), 404);
    }

    // -- deny-by-default -----------------------------------------------------

    #[test]
    fn unimplemented_and_unknown_ops() {
        // the storage-config describe family is now armed: unknown ids land in
        // the modeled ResourceNotFound
        let resp = request(
            "GET",
            "/instance/i/storage-config/a",
            json!({}),
            "ResourceType=S3",
        );
        assert_eq!(resp.status, 404);
        let body = String::from_utf8_lossy(&resp.body);
        assert!(body.contains("ResourceNotFoundException"), "{body}");

        // start-chat is armed; missing inputs land in the modeled 500-key
        // color the moto KeyError contract uses (not a 501)
        let resp = request("PUT", "/contact/chat", json!({}), "");
        assert_eq!(resp.status, 500);
        let body = String::from_utf8_lossy(&resp.body);
        assert!(body.contains("InternalError"), "{body}");

        // unregistered URL shape
        let resp = request("POST", "/bogus/path", json!({}), "");
        assert_eq!(resp.status, 501);
        let body = String::from_utf8_lossy(&resp.body);
        assert!(
            body.contains("Service connect is not yet implemented"),
            "{body}"
        );
    }

    fn status_owned(resp: Response) -> u16 {
        resp.status
    }

    fn list_owned(resp: Response, field: &str) -> usize {
        let body = body_of(&resp);
        body[field]
            .as_array()
            .unwrap_or_else(|| panic!("missing list field {field} in {body}"))
            .len()
    }

    // -- Instance-family ops (surface wave) ----------------------------------

    fn make_flow(instance: &str) -> String {
        let resp = request(
            "PUT",
            &format!("/contact-flows/{instance}"),
            json!({"Name": "flow", "Type": "CONTACT_FLOW", "Content": "{}"}),
            "",
        );
        assert_eq!(status_of(&resp), 200);
        body_of(&resp)["ContactFlowId"]
            .as_str()
            .unwrap()
            .to_string()
    }

    #[test]
    fn flow_associations_round_trip() {
        let (id, _) = make_instance(json!({}));
        let flow = make_flow(&id);
        let resource = "h1".to_string();

        let assoc = request(
            "PUT",
            &format!("/flow-associations/{id}"),
            json!({"ResourceId": resource, "FlowId": flow, "ResourceType": "HOURS_OF_OPERATION"}),
            "",
        );
        assert_eq!(status_of(&assoc), 200);

        let get = request(
            "GET",
            &format!("/flow-associations/{id}/{resource}/HOURS_OF_OPERATION"),
            json!({}),
            "",
        );
        assert_eq!(status_of(&get), 200);
        let got = body_of(&get);
        assert_eq!(got["FlowId"], flow.as_str());
        assert_eq!(got["ResourceType"], "HOURS_OF_OPERATION");

        let batch = request(
            "POST",
            &format!("/flow-associations-batch/{id}"),
            json!({"ResourceIds": [resource]}),
            "",
        );
        assert_eq!(status_of(&batch), 200);
        let batch_body = body_of(&batch);
        let rows = batch_body["FlowAssociationSummaryList"].as_array().unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0]["ResourceType"], "HOURS_OF_OPERATION");

        let dis = request(
            "DELETE",
            &format!("/flow-associations/{id}/{resource}/HOURS_OF_OPERATION"),
            json!({}),
            "",
        );
        assert_eq!(status_of(&dis), 200);
        let gone = request(
            "DELETE",
            &format!("/flow-associations/{id}/{resource}/HOURS_OF_OPERATION"),
            json!({}),
            "",
        );
        assert_eq!(status_of(&gone), 404);
    }

    #[test]
    fn data_table_values_round_trip() {
        let (id, _) = make_instance(json!({}));
        let table = request(
            "PUT",
            &format!("/data-tables/{id}"),
            json!({"Name": "t"}),
            "",
        );
        let table_id = body_of(&table)["Id"].as_str().unwrap().to_string();
        let base = format!("/data-tables/{id}/{table_id}");

        let created = request(
            "POST",
            &format!("{base}/values/create"),
            json!({"Values": [{"PrimaryValues": [{"AttributeName": "sku", "Value": "v1"}], "AttributeName": "price", "Value": "10"}]}),
            "",
        );
        let out = body_of(&created);
        assert_eq!(out["Successful"].as_array().unwrap().len(), 1);
        assert_eq!(out["Failed"].as_array().unwrap().len(), 0);

        let dup = request(
            "POST",
            &format!("{base}/values/create"),
            json!({"Values": [{"PrimaryValues": [{"AttributeName": "sku", "Value": "v1"}], "AttributeName": "price", "Value": "11"}]}),
            "",
        );
        assert_eq!(body_of(&dup)["Failed"].as_array().unwrap().len(), 1);

        let described = request(
            "POST",
            &format!("{base}/values/describe"),
            json!({"Values": [{"PrimaryValues": [{"AttributeName": "sku", "Value": "v1"}], "AttributeName": "price"}]}),
            "",
        );
        let described_body = body_of(&described);
        let rows = described_body["Successful"].as_array().unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0]["Value"], "10");
        assert!(rows[0]["RecordId"].as_str().is_some());

        let updated = request(
            "POST",
            &format!("{base}/values/update"),
            json!({"Values": [{"PrimaryValues": [{"AttributeName": "sku", "Value": "v1"}], "AttributeName": "price", "Value": "12"}]}),
            "",
        );
        assert_eq!(body_of(&updated)["Successful"].as_array().unwrap().len(), 1);

        let evaluated = request(
            "POST",
            &format!("{base}/values/evaluate"),
            json!({"Values": [{"PrimaryValues": [{"AttributeName": "sku", "Value": "v1"}], "AttributeName": "price"}]}),
            "",
        );
        let evaluated_body = body_of(&evaluated);
        let rows = evaluated_body["Values"].as_array().unwrap();
        assert!(rows[0]["Found"].as_bool().unwrap());

        let listed = request(
            "POST",
            &format!("{base}/values/list"),
            json!({"InstanceId": id, "DataTableId": table_id}),
            "",
        );
        assert_eq!(list_check(&listed, "Values"), 1);
        let primary = request(
            "POST",
            &format!("{base}/values/list-primary"),
            json!({"InstanceId": id, "DataTableId": table_id}),
            "",
        );
        assert_eq!(list_check(&primary, "PrimaryValuesList"), 1);

        let deleted = request(
            "POST",
            &format!("{base}/values/delete"),
            json!({"Values": [{"PrimaryValues": [{"AttributeName": "sku", "Value": "v1"}], "AttributeName": "price"}]}),
            "",
        );
        assert_eq!(body_of(&deleted)["Successful"].as_array().unwrap().len(), 1);

        let meta = request(
            "POST",
            &format!("/{base}"),
            json!({"Name": "t2", "ValueLockLevel": "CELL", "TimeZone": "UTC"}),
            "",
        );
        assert!(body_of(&meta)["LockVersion"].is_object());
    }

    fn list_check(resp: &Response, field: &str) -> usize {
        let body = body_of(resp);
        body[field]
            .as_array()
            .unwrap_or_else(|| panic!("missing list field {field} in {body}"))
            .len()
    }

    #[test]
    fn attached_files_round_trip() {
        let (id, _) = make_instance(json!({}));
        let base = format!("/attached-files/{id}");
        let instance_arn = format!("arn:aws:connect:us-east-1:{ACCOUNT}:instance/{id}");
        let started = request(
            "PUT",
            &base,
            json!({"FileName": "a.txt", "FileSizeInBytes": 12, "FileUseCaseType": "ATTACHMENT", "AssociatedResourceArn": instance_arn.clone()}),
            "",
        );
        assert_eq!(status_of(&started), 200);
        let file_id = body_of(&started)["FileId"].as_str().unwrap().to_string();
        assert_eq!(body_of(&started)["FileStatus"], "PROCESSING");

        assert_eq!(
            status_owned(request("POST", &format!("{base}/{file_id}"), json!({}), "")),
            200
        );

        let got = request(
            "GET",
            &format!("{base}/{file_id}"),
            json!({}),
            "AssociatedResourceArn=x",
        );
        assert_eq!(status_of(&got), 200);
        let got_file = body_of(&got);
        assert_eq!(got_file["FileSizeInBytes"], 12);
        assert_eq!(got_file["FileStatus"], "APPROVED");
        assert!(got_file["DownloadUrlMetadata"].is_object());

        let batch = request(
            "POST",
            &base,
            json!({"FileIds": [file_id, "missing"], "AssociatedResourceArn": instance_arn.clone()}),
            "",
        );
        let out = body_of(&batch);
        assert_eq!(out["Files"].as_array().unwrap().len(), 1);
        assert_eq!(out["Errors"].as_array().unwrap().len(), 1);

        assert_eq!(
            status_owned(request(
                "DELETE",
                &format!("{base}/{file_id}"),
                json!({}),
                ""
            )),
            200
        );
        assert_eq!(
            status_owned(request("GET", &format!("{base}/{file_id}"), json!({}), "")),
            404
        );
    }

    #[test]
    fn workspace_family_round_trip() {
        let (id, _) = make_instance(json!({}));
        let ws = request(
            "PUT",
            &format!("/workspaces/{id}"),
            json!({"Name": "ws"}),
            "",
        );
        assert_eq!(status_of(&ws), 200);
        let ws_id = body_of(&ws)["WorkspaceId"].as_str().unwrap().to_string();
        let base = format!("/workspaces/{id}/{ws_id}");

        assert_eq!(
            status_owned(request(
                "PUT",
                &format!("{base}/pages"),
                json!({"ResourceArn": "arn:apps:app", "Page": "home", "Slug": "slug"}),
                ""
            )),
            200
        );
        assert_eq!(
            list_owned(
                request("GET", &format!("{base}/pages"), json!({}), ""),
                "WorkspacePageList"
            ),
            1
        );
        assert_eq!(
            status_owned(request(
                "POST",
                &format!("{base}/pages/home"),
                json!({"Slug": "s2"}),
                ""
            )),
            200
        );
        assert_eq!(
            status_owned(request(
                "DELETE",
                &format!("{base}/pages/home"),
                json!({}),
                ""
            )),
            200
        );
        assert_eq!(
            status_owned(request(
                "DELETE",
                &format!("{base}/pages/home"),
                json!({}),
                ""
            )),
            404
        );

        assert_eq!(
            status_owned(request(
                "POST",
                &format!("{base}/media"),
                json!({"MediaType": "IMAGE_LOGO_LIGHT_HORIZONTAL", "MediaSource": "u"}),
                ""
            )),
            200
        );
        assert_eq!(
            list_owned(
                request("GET", &format!("{base}/media"), json!({}), ""),
                "Media"
            ),
            1
        );
        assert_eq!(
            status_owned(request(
                "DELETE",
                &format!("{base}/media"),
                json!({}),
                "mediaType=IMAGE_LOGO_LIGHT_HORIZONTAL",
            )),
            200
        );

        let assoc = request(
            "POST",
            &format!("{base}/associate"),
            json!({"ResourceArns": ["arn:apps:app"]}),
            "",
        );
        assert_eq!(status_of(&assoc), 200);
        let out = body_of(&assoc);
        assert_eq!(out["SuccessfulList"].as_array().unwrap().len(), 1);
        let associations = request(
            "POST",
            "/search-workspace-associations",
            json!({"InstanceId": id}),
            "",
        );
        assert_eq!(list_check(&associations, "WorkspaceAssociations"), 1);

        // disassociate removes it again
        assert_eq!(
            status_owned(request(
                "POST",
                &format!("{base}/disassociate"),
                json!({"ResourceArns": ["arn:apps:app"]}),
                ""
            )),
            200
        );

        let metadata = request(
            "POST",
            &format!("{base}/metadata"),
            json!({"Name": "ws2", "Title": "T"}),
            "",
        );
        assert_eq!(status_of(&metadata), 200);
        let described = request("POST", "/search-workspaces", json!({"InstanceId": id}), "");
        assert_eq!(
            body_of(&described)["Workspaces"].as_array().unwrap()[0]["Name"],
            "ws2"
        );
    }
}
