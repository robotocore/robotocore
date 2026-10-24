//! AWS Service Quotas backend — moto-equivalent (json protocol).
//!
//! Replaces the moto sidecar for `service-quotas`
//! (`X-Amz-Target: ServiceQuotasV20190624.<Op>`; the gateway router's target
//! map arrives here under the name `servicequotas`, the signing scope's
//! alias is `service-quotas`, so both spellings are served). The moto
//! surface is exactly two operations (moto/servicequotas/responses.py):
//! `list_aws_default_service_quotas` and `get_service_quota`, both backed by
//! a static 25-row VPC default-quotas table (`resources/default_quotas/vpc.py`)
//! — including its hardcoded `eu-west-1` QuotaArns. Unknown
//! ServiceCode/QuotaCode answers moto's `NoSuchResourceException` (400).
//!
//! The remaining botocore surface (moto raised `NotImplementedError` → HTTP
//! 501 for all of it) is modeled per the botocore wire model: service and
//! quota listings over the same VPC table, quota-increase-request records,
//! the quota template rows, Automatic Management opt-in state, quota
//! utilization reports with a PENDING → IN_PROGRESS → COMPLETED status
//! machine (the module's datasync-style state machine idiom), support-case
//! dedupe and an ARN-keyed tag ledger. State is a minimal per-(account,
//! region) collection ([`store::slot`]); ids and timestamps are
//! deterministic per-scope sequence numbers.
//!
//! Contract: botocore's service-quotas model (26 operations) and
//! tests/compatibility/test_service_quotas_compat.py.

use crate::proto::{json_error, now_iso, parse_json_body};
use crate::store;
use crate::{Request, Response};
use once_cell::sync::Lazy;
use serde_json::{json, Map, Value};
use std::collections::BTreeMap;

// ---------------------------------------------------------------------------
// moto's static VPC default-quotas table
// ---------------------------------------------------------------------------

/// (`QuotaCode`, `QuotaName`, Adjustable, Value) — moto's
/// `VPC_DEFAULT_QUOTAS` verbatim. Every row shares `"ServiceCode": "vpc"`,
/// `"ServiceName": "Amazon Virtual Private Cloud (Amazon VPC)"`,
/// `"Unit": "None"`, `"GlobalQuota": false`, and a `eu-west-1` QuotaArn.
const VPC_DEFAULT_QUOTAS: &[(&str, &str, bool, f64)] = &[
    (
        "L-7E9ECCDB",
        "Active VPC peering connections per VPC",
        true,
        50.0,
    ),
    (
        "L-3248932A",
        "Characters per VPC endpoint policy",
        false,
        20480.0,
    ),
    (
        "L-45FE3B85",
        "Egress-only internet gateways per Region",
        true,
        5.0,
    ),
    ("L-1B52E74A", "Gateway VPC endpoints per Region", true, 20.0),
    ("L-83CA0A9D", "IPv4 CIDR blocks per VPC", true, 5.0),
    ("L-085A6257", "IPv6 CIDR blocks per VPC", true, 5.0),
    (
        "L-0EA8095F",
        "Inbound or outbound rules per security group",
        true,
        60.0,
    ),
    ("L-29B6F2EB", "Interface VPC endpoints per VPC", true, 50.0),
    ("L-A4707A72", "Internet gateways per Region", true, 5.0),
    (
        "L-FE5A380F",
        "NAT gateways per Availability Zone",
        true,
        5.0,
    ),
    ("L-B4A6D682", "Network ACLs per VPC", true, 200.0),
    ("L-BB24F6E5", "Network Address Usage", true, 64000.0),
    ("L-DF5E4CA3", "Network interfaces per Region", true, 5000.0),
    (
        "L-DC9F7029",
        "Outstanding VPC peering connection requests",
        true,
        25.0,
    ),
    ("L-2C462E13", "Participant accounts per VPC", true, 100.0),
    ("L-CD17FD4B", "Peered Network Address Usage", true, 128000.0),
    ("L-589F43AA", "Route tables per VPC", true, 200.0),
    ("L-93826ACB", "Routes per route table", true, 50.0),
    ("L-2AEEBF1A", "Rules per network ACL", true, 20.0),
    (
        "L-2AFB9258",
        "Security groups per network interface",
        true,
        5.0,
    ),
    ("L-407747CB", "Subnets per VPC", true, 200.0),
    (
        "L-44499CD2",
        "Subnets that can be shared with an account",
        true,
        100.0,
    ),
    (
        "L-8312C5BB",
        "VPC peering connection request expiry hours",
        false,
        168.0,
    ),
    ("L-E79EC296", "VPC security groups per Region", true, 2500.0),
    ("L-F678F1CE", "VPCs per Region", true, 5.0),
];

/// moto's quota wire shape (moto/servicequotas/models.py rows as served).
fn quota_record(code: &str, name: &str, adjustable: bool, value: f64) -> Value {
    json!({
        "Adjustable": adjustable,
        "GlobalQuota": false,
        "QuotaArn": format!("arn:aws:servicequotas:eu-west-1::vpc/{code}"),
        "QuotaCode": code,
        "QuotaName": name,
        "ServiceCode": "vpc",
        "ServiceName": "Amazon Virtual Private Cloud (Amazon VPC)",
        "Unit": "None",
        "Value": value,
    })
}

// ---------------------------------------------------------------------------
// Error shapes (botocore-declared 4xx)
// ---------------------------------------------------------------------------

/// moto's `NoSuchResource` — raised for an unknown ServiceCode or the
/// `get_*` miss inside a known one (moto/servicequotas/exceptions.py).
fn not_found() -> Response {
    json_error(
        400,
        "NoSuchResourceException",
        "This service is not available in the current Region. Choose a different Region or a different service.",
    )
}

/// The `NoSuchResourceException` with a message naming the offending id.
fn request_not_found(id: &str) -> Response {
    json_error(
        400,
        "NoSuchResourceException",
        &format!("Quota increase request with ID {id} cannot be found."),
    )
}

fn report_not_found(id: &str) -> Response {
    json_error(
        400,
        "NoSuchResourceException",
        &format!("Quota utilization report with ID {id} cannot be found."),
    )
}

fn resource_already_exists(msg: &str) -> Response {
    json_error(400, "ResourceAlreadyExistsException", msg)
}

fn invalid_argument(msg: &str) -> Response {
    json_error(400, "IllegalArgumentException", msg)
}

fn invalid_pagination_token() -> Response {
    json_error(
        400,
        "InvalidPaginationTokenException",
        "The NextToken value is invalid.",
    )
}

fn template_not_in_use() -> Response {
    json_error(
        400,
        "ServiceQuotaTemplateNotInUseException",
        "The Service Quotas template is not in use.",
    )
}

// ---------------------------------------------------------------------------
// Modeled state
// ---------------------------------------------------------------------------

/// The botocore `RequestedServiceQuotaChange` — a stored increase request.
struct IncreaseRequest {
    id_num: u64,
    service_code: String,
    quota_code: String,
    desired_value: f64,
}

/// The botocore `ServiceQuotaIncreaseRequestInTemplate` — one row per
/// (service code, quota code, region).
struct TemplateRow {
    service_code: String,
    quota_code: String,
    aws_region: String,
    desired_value: f64,
}

/// The Automatic Management opt-in record (GetAutoManagementConfiguration).
struct AutoManagement {
    opt_in_type: String,
    notification_arn: String,
    /// `[(service, quota-code, quota-name)]` — the ExclusionList entries.
    exclusions: Vec<(String, String, String)>,
}

/// The botocore quota-utilization report. `StartQuotaUtilizationReport`
/// creates it `PENDING`; each `GetQuotaUtilizationReport` advances the
/// status machine one step (`PENDING` → `IN_PROGRESS` → `COMPLETED`, with
/// the quota rows and `GeneratedAt` filled in when it completes).
struct UtilizationReport {
    id: String,
    status: &'static str,
    generated_at: Option<String>,
}

/// Per-(account, region) in-memory state. moto modeled none of these
/// families; the collections mirror the botocore shapes minimally.
#[derive(Default)]
struct State {
    request_seq: u64,
    report_seq: u64,
    case_request_ids: Vec<String>,
    requests: Vec<IncreaseRequest>,
    template_rows: Vec<TemplateRow>,
    template_associated: bool,
    auto_mgmt: Option<AutoManagement>,
    reports: Vec<UtilizationReport>,
    tags: BTreeMap<String, BTreeMap<String, String>>,
}

fn state() -> store::Slot<State> {
    static SLOT: Lazy<store::Slot<State>> =
        Lazy::new(|| store::slot("servicequotas"));
    SLOT.clone()
}

/// The modeled service catalog (ListServices). The `vpc` row must stay: the
/// moto quota table above is keyed to it.
const SERVICES: &[(&str, &str)] = &[
    ("cloudwatch", "Amazon CloudWatch"),
    ("dynamodb", "Amazon DynamoDB"),
    ("ec2", "Amazon Elastic Compute Cloud (Amazon EC2)"),
    ("ecs", "Amazon Elastic Container Service (Amazon ECS)"),
    ("eks", "Amazon Elastic Kubernetes Service"),
    ("elasticloadbalancing", "Elastic Load Balancing"),
    ("emr", "Amazon EMR"),
    ("glue", "AWS Glue"),
    ("kinesis", "Amazon Kinesis"),
    ("kms", "AWS Key Management Service (AWS KMS)"),
    ("lambda", "AWS Lambda"),
    ("memorydb", "AWS MemoryDB"),
    ("opensearch", "Amazon OpenSearch Service"),
    ("quicksight", "Amazon QuickSight"),
    ("rds", "Amazon Relational Database Service (Amazon RDS)"),
    ("redshift", "Amazon Redshift"),
    ("route53", "Amazon Route 53"),
    ("s3", "Amazon Simple Storage Service (Amazon S3)"),
    ("sagemaker", "Amazon SageMaker"),
    ("ses", "Amazon Simple Email Service (SES)"),
    ("sns", "Amazon Simple Notification Service (SNS)"),
    ("sqs", "Amazon Simple Queue Service (SQS)"),
    ("vpc", "Amazon Virtual Private Cloud (Amazon VPC)"),
];
const TABLE_SERVICE_NAME: &str = "Amazon Virtual Private Cloud (Amazon VPC)";

fn service_name(service_code: &str) -> String {
    SERVICES
        .iter()
        .find(|(code, _)| *code == service_code)
        .map(|(_, name)| name.to_string())
        .unwrap_or_else(|| service_code.to_string())
}

/// The table row for a (service code, quota code) pair.
fn find_quota_row(service_code: &str, quota_code: &str) -> Option<(usize, &'static str)> {
    if service_code != "vpc" {
        return None;
    }
    VPC_DEFAULT_QUOTAS
        .iter()
        .position(|(code, ..)| *code == quota_code)
        .map(|i| (i, "vpc"))
}

/// Deterministic per-scope timestamps: `Created`/`LastUpdated` ride the
/// record's sequence number over a fixed epoch, never wall-clock.
fn epoch_for(seq: u64) -> chrono::DateTime<chrono::Utc> {
    chrono::DateTime::<chrono::Utc>::from_timestamp(1_700_000_000 + (seq * 13) as i64, 0)
        .expect("in-range epoch")
}

fn created_of(seq: u64) -> Value {
    Value::String(epoch_for(seq).to_rfc3339_opts(chrono::SecondsFormat::Millis, true))
}

/// The RequestedServiceQuotaChange wire record for a stored request.
fn request_record(row: &IncreaseRequest, account: u64) -> Value {
    let (quota_name, quota_arn) = match find_quota_row(&row.service_code, &row.quota_code) {
        Some((idx, _)) => (
            VPC_DEFAULT_QUOTAS[idx].1.to_string(),
            quota_record(VPC_DEFAULT_QUOTAS[idx].0, "", false, 0.0)["QuotaArn"]
                .as_str()
                .unwrap()
                .to_string(),
        ),
        None => (row.quota_code.clone(), String::new()),
    };
    json!({
        "Id": format!("qreq-{}", row.id_num),
        "RequestType": "AutomaticManagement",
        "ServiceCode": row.service_code,
        "ServiceName": table_service_name(&row),
        "QuotaCode": row.quota_code,
        "QuotaName": quota_name,
        "DesiredValue": row.desired_value,
        "Status": "PENDING",
        "Created": created_of(row.id_num),
        "LastUpdated": created_of(row.id_num),
        "Requester": format!("arn:aws:iam::{account}:root"),
        "QuotaArn": quota_arn,
        "GlobalQuota": false,
        "Unit": "None",
        "QuotaRequestedAtLevel": "ACCOUNT",
    })
}

/// The table's ServiceName: requests are only accepted for `vpc`, so it is
/// constant here (kept as a function to survive future tables).
fn table_service_name(row: &IncreaseRequest) -> String {
    if row.service_code == "vpc" {
        TABLE_SERVICE_NAME.to_string()
    } else {
        service_name(&row.service_code)
    }
}

/// The ServiceQuotaIncreaseRequestInTemplate wire record.
fn template_record(row: &TemplateRow) -> Value {
    let quota_name = match find_quota_row(&row.service_code, &row.quota_code) {
        Some((idx, _)) => VPC_DEFAULT_QUOTAS[idx].1.to_string(),
        None => row.quota_code.clone(),
    };
    json!({
        "ServiceCode": row.service_code,
        "ServiceName": service_name(&row.service_code),
        "QuotaCode": row.quota_code,
        "QuotaName": quota_name,
        "DesiredValue": row.desired_value,
        "AwsRegion": row.aws_region,
        "Unit": "None",
        "GlobalQuota": false,
    })
}

/// The Automatic Management configuration wire record.
fn auto_management_record(cfg: &AutoManagement) -> Value {
    let mut exclusions: BTreeMap<String, Vec<Value>> = BTreeMap::new();
    for (service, code, name) in &cfg.exclusions {
        exclusions
            .entry(service.clone())
            .or_default()
            .push(json!({"QuotaCode": code, "QuotaName": name}));
    }
    let exclusion_list: Map<String, Value> = exclusions
        .into_iter()
        .map(|(k, v)| (k, Value::Array(v)))
        .collect();
    json!({
        "OptInLevel": "ACCOUNT",
        "OptInType": cfg.opt_in_type,
        "NotificationArn": cfg.notification_arn,
        "OptInStatus": "ENABLED",
        "ExclusionList": Value::Object(exclusion_list),
    })
}

/// The `NoSuchResourceException` for a missing Automatic Management opt-in.
fn auto_management_missing() -> Response {
    json_error(
        400,
        "NoSuchResourceException",
        "Automatic Management is not enabled. Start it first.",
    )
}

// ---------------------------------------------------------------------------
// Pagination (the listing arms; token = decimal next offset, which satisfies
// the model's `NextToken` pattern `^[a-zA-Z0-9/+]*={0,2}$`)
// ---------------------------------------------------------------------------

/// The decimal offset a NextToken stands for, or `None` when malformed.
fn token_offset(token: Option<&str>) -> Option<usize> {
    match token {
        None | Some("") => Some(0),
        Some(t) => t.parse::<usize>().ok(),
    }
}

/// One page of `full` honoring MaxResults/NextToken. No MaxResults returns
/// the full list with a null token (moto's two listing ops were unpaged).
fn paginate(
    full: Vec<Value>,
    max: Option<i64>,
    start: usize,
) -> (Vec<Value>, Value) {
    let Some(max) = max else {
        return (full, Value::Null);
    };
    let page: Vec<Value> = full
        .into_iter()
        .skip(start)
        .take(max.max(0) as usize)
        .collect();
    let next_token = if page.len() as i64 == max && !page.is_empty() {
        json!((start + page.len()).to_string())
    } else {
        Value::Null
    };
    (page, next_token)
}

// ---------------------------------------------------------------------------
// dispatch
// ---------------------------------------------------------------------------

pub fn dispatch(req: &Request) -> Option<Response> {
    // The router's `ServiceQuotasV20190624` target map yields
    // `servicequotas`; the signing-scope alias resolves to `service-quotas`.
    if !matches!(req.service.as_str(), "servicequotas" | "service-quotas") {
        return None;
    }
    let slot = state();
    let mut map = slot.write();
    let st = map.entry((req.account, req.region.clone())).or_default();
    Some(route(req, st))
}

fn route(req: &Request, st: &mut State) -> Response {
    let body = parse_json_body(&req.body);
    let account = req.account;
    match req.operation.as_str() {
        "ListAWSDefaultServiceQuotas" => {
            // moto: `str(params.get("ServiceCode"))` — absent becomes "None",
            // which misses the table like any other unknown code. Pagination
            // is not implemented in moto; the full list always returns.
            let service_code = body
                .get("ServiceCode")
                .and_then(Value::as_str)
                .unwrap_or("None");
            if service_code != "vpc" {
                return not_found();
            }
            let quotas: Vec<Value> = VPC_DEFAULT_QUOTAS
                .iter()
                .map(|(code, name, adjustable, value)| {
                    quota_record(code, name, *adjustable, *value)
                })
                .collect();
            ok(json!({ "Quotas": quotas }))
        }
        "GetServiceQuota" | "GetAWSDefaultServiceQuota" => {
            let service_code = body
                .get("ServiceCode")
                .and_then(Value::as_str)
                .unwrap_or("None");
            let quota_code = body
                .get("QuotaCode")
                .and_then(Value::as_str)
                .unwrap_or("None");
            if let Some((idx, _)) = find_quota_row(service_code, quota_code) {
                return ok(json!({ "Quota": table_quota_row(idx) }));
            }
            not_found()
        }
        // --- catalog / quota listings --------------------------------------
        "ListServices" => {
            let Some(start) = token_offset(body.get("NextToken").and_then(Value::as_str)) else {
                return invalid_pagination_token();
            };
            let services: Vec<Value> = SERVICES
                .iter()
                .map(|(code, name)| json!({"ServiceCode": code, "ServiceName": name}))
                .collect();
            let (page, next) = paginate(services, body.get("MaxResults").and_then(Value::as_i64), start);
            ok(json!({ "Services": page, "NextToken": next }))
        }
        "ListServiceQuotas" => {
            let service_code = body
                .get("ServiceCode")
                .and_then(Value::as_str)
                .unwrap_or("None");
            let quota_filter = body.get("QuotaCode").and_then(Value::as_str).unwrap_or("");
            if service_code != "vpc" {
                return not_found();
            }
            let quotas: Vec<Value> = VPC_DEFAULT_QUOTAS
                .iter()
                .filter(|(code, ..)| quota_filter.is_empty() || *code == quota_filter)
                .map(|(code, name, adjustable, value)| {
                    quota_record(code, name, *adjustable, *value)
                })
                .collect();
            let Some(start) = token_offset(body.get("NextToken").and_then(Value::as_str)) else {
                return invalid_pagination_token();
            };
            let (page, next) = paginate(quotas, body.get("MaxResults").and_then(Value::as_i64), start);
            ok(json!({ "Quotas": page, "NextToken": next }))
        }
        // --- quota increase requests ---------------------------------------
        "RequestServiceQuotaIncrease" => {
            let service_code = str_of(&body, "ServiceCode");
            let quota_code = str_of(&body, "QuotaCode");
            let Some((idx, _)) = find_quota_row(&service_code, &quota_code) else {
                return not_found();
            };
            let (_, _, adjustable, value) = VPC_DEFAULT_QUOTAS[idx];
            if !adjustable {
                return invalid_argument("The quota is not adjustable.");
            }
            let Some(desired) = body.get("DesiredValue").and_then(Value::as_f64) else {
                return invalid_argument("The DesiredValue parameter is required.");
            };
            if desired <= value {
                return invalid_argument(
                    "The value you have provided for DesiredValue is less than or equal to the current quota value.",
                );
            }
            if st
                .requests
                .iter()
                .any(|r| r.service_code == service_code && r.quota_code == quota_code)
            {
                return resource_already_exists(
                    "An open quota increase request already exists for this quota.",
                );
            }
            st.request_seq += 1;
            let row = IncreaseRequest {
                id_num: st.request_seq,
                service_code,
                quota_code,
                desired_value: desired,
            };
            let record = request_record(&row, account);
            st.requests.push(row);
            ok(json!({ "RequestedQuota": record }))
        }
        "GetRequestedServiceQuotaChange" => {
            let id = str_of(&body, "RequestId");
            match st.requests.iter().find(|r| wire_id(r) == id) {
                Some(row) => ok(json!({ "RequestedQuota": request_record(row, account) })),
                None => request_not_found(&id),
            }
        }
        "ListRequestedServiceQuotaChangeHistory" => {
            list_request_history(st, account, &body, None)
        }
        "ListRequestedServiceQuotaChangeHistoryByQuota" => {
            let service_code = str_of(&body, "ServiceCode");
            let quota_code = str_of(&body, "QuotaCode");
            if find_quota_row(&service_code, &quota_code).is_none() {
                return not_found();
            }
            list_request_history(st, account, &body, Some((service_code, quota_code)))
        }
        // --- the quota template --------------------------------------------
        "ListServiceQuotaIncreaseRequestsInTemplate" => {
            let service_filter = str_of(&body, "ServiceCode");
            let region_filter = str_of(&body, "AwsRegion");
            let rows: Vec<Value> = st
                .template_rows
                .iter()
                .filter(|r| service_filter.is_empty() || r.service_code == service_filter)
                .filter(|r| region_filter.is_empty() || r.aws_region == region_filter)
                .map(template_record)
                .collect();
            let Some(start) = token_offset(body.get("NextToken").and_then(Value::as_str)) else {
                return invalid_pagination_token();
            };
            let (page, next) = paginate(rows, body.get("MaxResults").and_then(Value::as_i64), start);
            ok(json!({
                "ServiceQuotaIncreaseRequestInTemplateList": page,
                "NextToken": next,
            }))
        }
        "PutServiceQuotaIncreaseRequestIntoTemplate" => {
            let service_code = str_of(&body, "ServiceCode");
            let quota_code = str_of(&body, "QuotaCode");
            let aws_region = str_of(&body, "AwsRegion");
            if find_quota_row(&service_code, &quota_code).is_none() {
                return not_found();
            }
            let Some(desired) = body.get("DesiredValue").and_then(Value::as_f64) else {
                return invalid_argument("The DesiredValue parameter is required.");
            };
            let row = TemplateRow {
                service_code,
                quota_code,
                aws_region,
                desired_value: desired,
            };
            let record = template_record(&row);
            st.template_rows
                .retain(|r| {
                    r.service_code != row.service_code
                        || r.quota_code != row.quota_code
                        || r.aws_region != row.aws_region
                });
            st.template_rows.push(row);
            ok(json!({ "ServiceQuotaIncreaseRequestInTemplate": record }))
        }
        "GetServiceQuotaIncreaseRequestFromTemplate" => {
            find_template_row(st, &body).map_or_else(
                || not_found(),
                |row| ok(json!({ "ServiceQuotaIncreaseRequestInTemplate": template_record(row) })),
            )
        }
        "DeleteServiceQuotaIncreaseRequestFromTemplate" => {
            let before = st.template_rows.len();
            st.template_rows.retain(|r| !template_row_matches(r, &body));
            if st.template_rows.len() == before {
                return not_found();
            }
            ok(json!({}))
        }
        "AssociateServiceQuotaTemplate" => {
            st.template_associated = true;
            ok(json!({}))
        }
        "DisassociateServiceQuotaTemplate" => {
            if !st.template_associated {
                return template_not_in_use();
            }
            st.template_associated = false;
            ok(json!({}))
        }
        "GetAssociationForServiceQuotaTemplate" => {
            let status = if st.template_associated {
                "ASSOCIATED"
            } else {
                "DISASSOCIATED"
            };
            ok(json!({ "ServiceQuotaTemplateAssociationStatus": status }))
        }
        // --- Automatic Management ------------------------------------------
        "StartAutoManagement" => {
            let opt_in_type = str_of(&body, "OptInType");
            let mut exclusions = Vec::new();
            if let Some(map) = body.get("ExclusionList").and_then(Value::as_object) {
                for (service, quotas) in map {
                    for info in quotas.as_array().unwrap_or(&Vec::new()) {
                        let code = info.get("QuotaCode").and_then(Value::as_str);
                        if let Some(code) = code {
                            let name = info
                                .get("QuotaName")
                                .and_then(Value::as_str)
                                .unwrap_or(code);
                            exclusions.push((service.clone(), code.to_string(), name.to_string()));
                        }
                    }
                }
            }
            st.auto_mgmt = Some(AutoManagement {
                opt_in_type,
                notification_arn: str_of(&body, "NotificationArn"),
                exclusions,
            });
            ok(json!({}))
        }
        "UpdateAutoManagement" => {
            let Some(cfg) = st.auto_mgmt.as_mut() else {
                return auto_management_missing();
            };
            if let Some(t) = body.get("OptInType").and_then(Value::as_str) {
                cfg.opt_in_type = t.to_string();
            }
            if let Some(arn) = body.get("NotificationArn").and_then(Value::as_str) {
                cfg.notification_arn = arn.to_string();
            }
            if let Some(map) = body.get("ExclusionList").and_then(Value::as_object) {
                let mut exclusions = Vec::new();
                for (service, quotas) in map {
                    for info in quotas.as_array().unwrap_or(&Vec::new()) {
                        let code = info.get("QuotaCode").and_then(Value::as_str);
                        if let Some(code) = code {
                            let name =
                                info.get("QuotaName").and_then(Value::as_str).unwrap_or(code);
                            exclusions.push((service.clone(), code.to_string(), name.to_string()));
                        }
                    }
                }
                cfg.exclusions = exclusions;
            }
            ok(json!({}))
        }
        "StopAutoManagement" => {
            st.auto_mgmt = None;
            ok(json!({}))
        }
        "GetAutoManagementConfiguration" => match st.auto_mgmt.as_ref() {
            Some(cfg) => ok(auto_management_record(cfg)),
            None => auto_management_missing(),
        },
        // --- quota utilization reports --------------------------------------
        "StartQuotaUtilizationReport" => {
            st.report_seq += 1;
            let id = format!("quota-report-{}", st.report_seq);
            st.reports.push(UtilizationReport {
                id: id.clone(),
                status: "PENDING",
                generated_at: None,
            });
            ok(json!({
                "ReportId": id,
                "Status": "PENDING",
                "Message": "Quota utilization report started.",
            }))
        }
        "GetQuotaUtilizationReport" => {
            let id = str_of(&body, "ReportId");
            let Some(report) = st.reports.iter_mut().find(|r| r.id == id) else {
                return report_not_found(&id);
            };
            if report.status == "COMPLETED" {
                return utilization_report_page(report, &body);
            }
            // PENDING → IN_PROGRESS → COMPLETED (rows + GeneratedAt on finish).
            report.status = if report.status == "PENDING" {
                "IN_PROGRESS"
            } else {
                report.generated_at = Some(now_iso());
                "COMPLETED"
            };
            utilization_report_page(report, &body)
        }
        // --- tags ------------------------------------------------------------
        "TagResource" => {
            let arn = str_of(&body, "ResourceARN");
            if !arn.starts_with("arn:") {
                return not_found();
            }
            let row = st.tags.entry(arn).or_default();
            for tag in body.get("Tags").and_then(Value::as_array).unwrap_or(&Vec::new()) {
                if let Some(key) = tag.get("Key").and_then(Value::as_str) {
                    row.insert(
                        key.to_string(),
                        tag.get("Value").and_then(Value::as_str).unwrap_or("").to_string(),
                    );
                }
            }
            ok(json!({}))
        }
        "UntagResource" => {
            let arn = str_of(&body, "ResourceARN");
            if !arn.starts_with("arn:") {
                return not_found();
            }
            if let Some(row) = st.tags.get_mut(&arn) {
                let keys: Vec<String> = body
                    .get("TagKeys")
                    .and_then(Value::as_array)
                    .unwrap_or(&Vec::new())
                    .iter()
                    .filter_map(Value::as_str)
                    .map(String::from)
                    .collect();
                for key in keys {
                    row.remove(&key);
                }
            }
            ok(json!({}))
        }
        "ListTagsForResource" => {
            let arn = str_of(&body, "ResourceARN");
            if !arn.starts_with("arn:") {
                return not_found();
            }
            let tags: Vec<Value> = st
                .tags
                .get(&arn)
                .map(|row| {
                    row.iter()
                        .map(|(k, v)| json!({"Key": k, "Value": v}))
                        .collect()
                })
                .unwrap_or_default();
            ok(json!({ "Tags": tags }))
        }
        // --- support cases ----------------------------------------------------
        "CreateSupportCase" => {
            let id = str_of(&body, "RequestId");
            if st.case_request_ids.contains(&id) {
                return resource_already_exists("A support case for this request already exists.");
            }
            st.case_request_ids.push(id);
            ok(json!({}))
        }
        other => json_error(
            501,
            "NotImplemented",
            &format!("The {other} action has not been implemented"),
        ),
    }
}

/// The wire `Id` of a stored increase request.
fn wire_id(row: &IncreaseRequest) -> String {
    format!("qreq-{}", row.id_num)
}

fn str_of(body: &Value, key: &str) -> String {
    body.get(key)
        .and_then(Value::as_str)
        .unwrap_or("")
        .to_string()
}

/// The moto-table row as a ServiceQuota wire record.
fn table_quota_row(idx: usize) -> Value {
    let (code, name, adjustable, value) = VPC_DEFAULT_QUOTAS[idx];
    quota_record(code, name, adjustable, value)
}

fn list_request_history(
    st: &mut State,
    account: u64,
    body: &Value,
    pair: Option<(String, String)>,
) -> Response {
    let status_filter = str_of(body, "Status");
    let service_filter = match &pair {
        Some(_) => String::new(),
        None => str_of(body, "ServiceCode"),
    };
    let rows: Vec<Value> = st
        .requests
        .iter()
        .filter(|r| match &pair {
            Some((sc, qc)) => r.service_code == *sc && r.quota_code == *qc,
            None => service_filter.is_empty() || service_filter == r.service_code,
        })
        .filter(|_| status_filter.is_empty() || status_filter == "PENDING")
        .map(|r| request_record(r, account))
        .collect();
    let Some(start) = token_offset(body.get("NextToken").and_then(Value::as_str)) else {
        return invalid_pagination_token();
    };
    let (page, next) = paginate(rows, body.get("MaxResults").and_then(Value::as_i64), start);
    ok(json!({ "RequestedQuotas": page, "NextToken": next }))
}

fn find_template_row<'a>(st: &'a mut State, body: &Value) -> Option<&'a TemplateRow> {
    st.template_rows
        .iter()
        .find(|r| template_row_matches(r, body))
}

fn template_row_matches(row: &TemplateRow, body: &Value) -> bool {
    row.service_code == str_of(body, "ServiceCode")
        && row.quota_code == str_of(body, "QuotaCode")
        && row.aws_region == str_of(body, "AwsRegion")
}

/// The GET report arm: assemble the wire record for the report's current
/// status, honoring the MaxResults/NextToken page over the quota rows.
fn utilization_report_page(report: &UtilizationReport, body: &Value) -> Response {
    let totals: Vec<Value> = if report.status == "COMPLETED" {
        VPC_DEFAULT_QUOTAS
            .iter()
            .enumerate()
            .map(|(i, (code, name, adjustable, value))| {
                json!({
                    "QuotaCode": code,
                    "ServiceCode": "vpc",
                    "QuotaName": name,
                    "Namespace": "AWS/Usage",
                    "Utilization": utilization_percent(i),
                    "DefaultValue": value,
                    "AppliedValue": value,
                    "ServiceName": TABLE_SERVICE_NAME,
                    "Adjustable": adjustable,
                })
            })
            .collect()
    } else {
        Vec::new()
    };
    let Some(start) = token_offset(body.get("NextToken").and_then(Value::as_str)) else {
        return invalid_pagination_token();
    };
    let (page, next) = paginate(totals, body.get("MaxResults").and_then(Value::as_i64), start);
    let mut out = json!({
        "ReportId": report.id,
        "Status": report.status,
        "TotalCount": VPC_DEFAULT_QUOTAS.len(),
        "Quotas": page,
        "NextToken": next,
    });
    if let Some(at) = report.generated_at.clone() {
        out["GeneratedAt"] = json!(at);
    }
    ok(out)
}

/// Deterministic pseudo-utilization: row index over prime multipliers.
fn utilization_percent(idx: usize) -> f64 {
    (idx * 17 % 97) as f64 + 1.0
}

fn ok(body: Value) -> Response {
    Response::json(200, serde_json::to_vec(&body).unwrap_or_default())
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;
    use serde_json::Value;
    use std::collections::HashMap;

    /// Unique 12-digit account per test — the state slot isolates by
    /// (account, region), mirroring the sibling modules' test idiom.
    fn scope() -> u64 {
        (700_000_000_000u64) + (uuid::Uuid::new_v4().as_u128() as u64 % 100_000_000_000)
    }

    fn req(op: &str, body: &Value) -> Request {
        Request {
            service: "servicequotas".into(),
            operation: op.into(),
            method: "POST".into(),
            path: "/".into(),
            query_string: String::new(),
            headers: HashMap::new(),
            body: Bytes::from(serde_json::to_vec(body).unwrap()),
            account: 123456789012,
            region: "us-east-1".into(),
        }
    }

    fn send(op: &str, body: &Value) -> (u16, Value) {
        let resp = dispatch(&req(op, body)).unwrap();
        (
            resp.status,
            serde_json::from_slice(&resp.body).unwrap_or(Value::Null),
        )
    }

    fn scoped(op: &str, body: &Value, account: u64) -> (u16, Value) {
        let mut r = req(op, body);
        r.account = account;
        let resp = dispatch(&r).unwrap();
        (
            resp.status,
            serde_json::from_slice(&resp.body).unwrap_or(Value::Null),
        )
    }

    #[test]
    fn list_vpc_default_quotas() {
        let resp = dispatch(&req(
            "ListAWSDefaultServiceQuotas",
            &json!({"ServiceCode": "vpc"}),
        ))
        .unwrap();
        assert_eq!(resp.status, 200);
        let v: Value = serde_json::from_slice(&resp.body).unwrap();
        assert_eq!(v["Quotas"].as_array().unwrap().len(), 25);
        let first = &v["Quotas"][0];
        assert_eq!(first["QuotaCode"], "L-7E9ECCDB");
        assert_eq!(first["QuotaName"], "Active VPC peering connections per VPC");
        assert_eq!(
            first["QuotaArn"],
            "arn:aws:servicequotas:eu-west-1::vpc/L-7E9ECCDB"
        );
    }

    #[test]
    fn list_unknown_service_code() {
        let resp = dispatch(&req(
            "ListAWSDefaultServiceQuotas",
            &json!({"ServiceCode": "unknown-svc"}),
        ))
        .unwrap();
        assert_eq!(resp.status, 400);
        let v: Value = serde_json::from_slice(&resp.body).unwrap();
        assert_eq!(v["__type"], "NoSuchResourceException");
    }

    #[test]
    fn get_vpc_quota() {
        for op in ["GetServiceQuota", "GetAWSDefaultServiceQuota"] {
            let resp = dispatch(&req(
                op,
                &json!({"ServiceCode": "vpc", "QuotaCode": "L-7E9ECCDB"}),
            ))
            .unwrap();
            assert_eq!(resp.status, 200);
            let v: Value = serde_json::from_slice(&resp.body).unwrap();
            assert_eq!(v["Quota"]["QuotaCode"], "L-7E9ECCDB");
            assert_eq!(v["Quota"]["Value"], 50.0);
        }
    }

    #[test]
    fn get_unknown_quota_code() {
        for op in ["GetServiceQuota", "GetAWSDefaultServiceQuota"] {
            let resp = dispatch(&req(
                op,
                &json!({"ServiceCode": "vpc", "QuotaCode": "L-UNKNOWN"}),
            ))
            .unwrap();
            assert_eq!(resp.status, 400);
            let v: Value = serde_json::from_slice(&resp.body).unwrap();
            assert_eq!(v["__type"], "NoSuchResourceException");
        }
    }

    #[test]
    fn unknown_actions_answer_motos_501() {
        // The whole botocore surface is armed; only unknown pseudo-ops fall
        // through to moto's NotImplementedError 501.
        let resp = dispatch(&req("BogusAction", &json!({}))).unwrap();
        assert_eq!(resp.status, 501);
        let v: Value = serde_json::from_slice(&resp.body).unwrap();
        assert_eq!(v["__type"], "NotImplemented");
        assert_eq!(
            v["message"],
            "The BogusAction action has not been implemented"
        );
    }

    #[test]
    fn other_services_are_not_claimed() {
        let mut not_ours = req("GetServiceQuota", &json!({}));
        not_ours.service = "shield".into();
        assert!(dispatch(&not_ours).is_none());
    }

    #[test]
    fn list_services_and_pagination() {
        let (status, body) = send("ListServices", &json!({}));
        assert_eq!(status, 200);
        let rows = body["Services"].as_array().unwrap();
        assert!(rows.len() > 20);

        let (status, body) = send("ListServices", &json!({"MaxResults": 2}));
        assert_eq!(status, 200);
        assert_eq!(body["Services"].as_array().unwrap().len(), 2);
        assert_eq!(body["NextToken"], json!("2"));

        let (status, body) = send(
            "ListServices",
            &json!({"MaxResults": 2, "NextToken": body["NextToken"]}),
        );
        assert_eq!(status, 200);
        assert!(body["Services"].as_array().unwrap().len() > 0);

        // `vpc` — the moto table's service — is in the catalog.
        let (_, body) = send("ListServices", &json!({}));
        assert!(body["Services"]
            .as_array()
            .unwrap()
            .iter()
            .any(|s| s["ServiceCode"] == "vpc"));

        // A malformed token is a modeled 4xx.
        let (status, body) = send("ListServices", &json!({"NextToken": "!!"}));
        assert_eq!(status, 400);
        assert_eq!(body["__type"], "InvalidPaginationTokenException");
    }

    #[test]
    fn list_service_quotas_for_vpc() {
        let (status, body) = send("ListServiceQuotas", &json!({"ServiceCode": "vpc"}));
        assert_eq!(status, 200);
        let rows = body["Quotas"].as_array().unwrap();
        assert_eq!(rows.len(), 25);

        // QuotaCode filter narrows to one row.
        let (_, body) = send(
            "ListServiceQuotas",
            &json!({"ServiceCode": "vpc", "QuotaCode": "L-F678F1CE"}),
        );
        let rows = body["Quotas"].as_array().unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0]["QuotaName"], "VPCs per Region");

        // Unknown quota code inside a known service filters to nothing,
        // unknown service code answers moto's fault.
        let (status, body) = send("ListServiceQuotas", &json!({"ServiceCode": "ec2"}));
        assert_eq!(status, 400);
        assert_eq!(body["__type"], "NoSuchResourceException");

        // Pagination page of two carries a token.
        let (_, body) = send(
            "ListServiceQuotas",
            &json!({"ServiceCode": "vpc", "MaxResults": 2}),
        );
        assert_eq!(body["Quotas"].as_array().unwrap().len(), 2);
        assert_eq!(body["NextToken"], json!("2"));
    }

    #[test]
    fn request_quota_increase_lifecycle() {
        let account = scope();
        // Adjustable, greater than current: stored with a deterministic id.
        let (status, body) = scoped(
            "RequestServiceQuotaIncrease",
            &json!({"ServiceCode": "vpc", "QuotaCode": "L-F678F1CE", "DesiredValue": 10.0}),
            account,
        );
        assert_eq!(status, 200, "{body}");
        let quota = &body["RequestedQuota"];
        assert_eq!(quota["Id"], "qreq-1");
        assert_eq!(quota["QuotaCode"], "L-F678F1CE");
        assert_eq!(quota["DesiredValue"], 10.0);
        assert_eq!(quota["Status"], "PENDING");
        assert_eq!(quota["QuotaArn"], "arn:aws:servicequotas:eu-west-1::vpc/L-F678F1CE");
        assert!(quota["Created"].as_str().is_some());
        assert!(quota["Requester"]
            .as_str()
            .unwrap()
            .starts_with("arn:aws:iam::"));

        // Values at or below the current quota are rejected.
        let (status, body) = scoped(
            "RequestServiceQuotaIncrease",
            &json!({"ServiceCode": "vpc", "QuotaCode": "L-F678F1CE", "DesiredValue": 1.0}),
            account,
        );
        assert_eq!(status, 400);
        assert_eq!(body["__type"], "IllegalArgumentException");

        // A second open request for the same quota is a duplicate fault.
        let (status, body) = scoped(
            "RequestServiceQuotaIncrease",
            &json!({"ServiceCode": "vpc", "QuotaCode": "L-F678F1CE", "DesiredValue": 11.0}),
            account,
        );
        assert_eq!(status, 400);
        assert_eq!(body["__type"], "ResourceAlreadyExistsException");

        // Non-adjustable motors are rejected.
        let (status, body) = scoped(
            "RequestServiceQuotaIncrease",
            &json!({"ServiceCode": "vpc", "QuotaCode": "L-3248932A", "DesiredValue": 99.0}),
            account,
        );
        assert_eq!(status, 400);
        assert_eq!(body["__type"], "IllegalArgumentException");

        // Unknown quota → moto's NoSuchResourceException.
        let (status, body) = scoped(
            "RequestServiceQuotaIncrease",
            &json!({"ServiceCode": "vpc", "QuotaCode": "L-GHOST", "DesiredValue": 9.0}),
            account,
        );
        assert_eq!(status, 400);
        assert_eq!(body["__type"], "NoSuchResourceException");

        // Get by id, then the change-history list.
        let (status, body) = scoped("GetRequestedServiceQuotaChange", &json!({"RequestId": "qreq-1"}), account);
        assert_eq!(status, 200);
        assert_eq!(body["RequestedQuota"]["Id"], "qreq-1");

        let (status, body) = scoped(
            "GetRequestedServiceQuotaChange",
            &json!({"RequestId": "qreq-9"}),
            account,
        );
        assert_eq!(status, 400);
        assert_eq!(body["__type"], "NoSuchResourceException");

        let (status, body) = scoped("ListRequestedServiceQuotaChangeHistory", &json!({}), account);
        assert_eq!(status, 200);
        assert_eq!(body["RequestedQuotas"].as_array().unwrap().len(), 1);

        // By-quota history filters to the pair.
        let (status, body) = scoped(
            "ListRequestedServiceQuotaChangeHistoryByQuota",
            &json!({"ServiceCode": "vpc", "QuotaCode": "L-F678F1CE"}),
            account,
        );
        assert_eq!(status, 200);
        assert_eq!(body["RequestedQuotas"].as_array().unwrap().len(), 1);

        // Unknown pair in the by-quota history answers moto's fault.
        let (status, body) = scoped(
            "ListRequestedServiceQuotaChangeHistoryByQuota",
            &json!({"ServiceCode": "vpc", "QuotaCode": "L-GHOST"}),
            account,
        );
        assert_eq!(status, 400);
        assert_eq!(body["__type"], "NoSuchResourceException");
    }

    #[test]
    fn service_quota_template_rows() {
        let account = scope();
        // The template starts unassociated.
        let (status, body) = scoped("GetAssociationForServiceQuotaTemplate", &json!({}), account);
        assert_eq!(status, 200);
        assert_eq!(body["ServiceQuotaTemplateAssociationStatus"], "DISASSOCIATED");

        // Disassociate before any use answers the template fault.
        let (status, body) = scoped("DisassociateServiceQuotaTemplate", &json!({}), account);
        assert_eq!(status, 400);
        assert_eq!(body["__type"], "ServiceQuotaTemplateNotInUseException");

        // Associate → Get(ASSOCIATED) → Disassociate succeeds.
        let (status, _) = scoped("AssociateServiceQuotaTemplate", &json!({}), account);
        assert_eq!(status, 200);
        let (status, body) = scoped("GetAssociationForServiceQuotaTemplate", &json!({}), account);
        assert_eq!(status, 200);
        assert_eq!(body["ServiceQuotaTemplateAssociationStatus"], "ASSOCIATED");
        let (status, _) = scoped("DisassociateServiceQuotaTemplate", &json!({}), account);
        assert_eq!(status, 200);

        // Put a template row, read it back, list it, then delete it.
        let row = json!({
            "ServiceCode": "vpc",
            "QuotaCode": "L-F678F1CE",
            "AwsRegion": "us-west-2",
            "DesiredValue": 7.0,
        });
        let (status, body) = scoped("PutServiceQuotaIncreaseRequestIntoTemplate", &row, account);
        assert_eq!(status, 200, "{body}");
        let rec = &body["ServiceQuotaIncreaseRequestInTemplate"];
        assert_eq!(rec["QuotaName"], "VPCs per Region");
        assert_eq!(rec["AwsRegion"], "us-west-2");

        let (status, body) = scoped("GetServiceQuotaIncreaseRequestFromTemplate", &row, account);
        assert_eq!(status, 200);
        assert_eq!(body["ServiceQuotaIncreaseRequestInTemplate"]["DesiredValue"], 7.0);

        // Updating the same pair replaces the row.
        let (status, body) = scoped(
            "PutServiceQuotaIncreaseRequestIntoTemplate",
            &json!({"ServiceCode": "vpc", "QuotaCode": "L-F678F1CE", "AwsRegion": "us-west-2",
                    "DesiredValue": 20.0}),
            account,
        );
        assert_eq!(status, 200);

        let (status, body) = scoped(
            "ListServiceQuotaIncreaseRequestsInTemplate",
            &json!({}),
            account,
        );
        assert_eq!(status, 200);
        assert_eq!(
            body["ServiceQuotaIncreaseRequestInTemplateList"]
                .as_array()
                .unwrap()
                .len(),
            1
        );
        assert_eq!(
            body["ServiceQuotaIncreaseRequestInTemplateList"][0]["DesiredValue"],
            20.0
        );

        let (status, _) = scoped(
            "DeleteServiceQuotaIncreaseRequestFromTemplate",
            &row,
            account,
        );
        assert_eq!(status, 200);

        let (status, body) = scoped("GetServiceQuotaIncreaseRequestFromTemplate", &row, account);
        assert_eq!(status, 400);
        assert_eq!(body["__type"], "NoSuchResourceException");

        // Put for an unknown quota answers moto's fault.
        let (status, body) = scoped(
            "PutServiceQuotaIncreaseRequestIntoTemplate",
            &json!({"ServiceCode": "vpc", "QuotaCode": "L-GHOST", "AwsRegion": "us-west-2",
                    "DesiredValue": 2.0}),
            account,
        );
        assert_eq!(status, 400);
        assert_eq!(body["__type"], "NoSuchResourceException");
    }

    #[test]
    fn auto_management_lifecycle() {
        let account = scope();
        // Not opted in yet.
        let (status, body) = scoped("GetAutoManagementConfiguration", &json!({}), account);
        assert_eq!(status, 400);
        assert_eq!(body["__type"], "NoSuchResourceException");

        // Start with exclusions; the configuration reads back.
        let (status, _) = scoped(
            "StartAutoManagement",
            &json!({
                "OptInLevel": "ACCOUNT",
                "OptInType": "NotifyOnly",
                "ExclusionList": {"vpc": [{"QuotaCode": "L-F678F1CE"}]}
            }),
            account,
        );
        assert_eq!(status, 200);
        let (status, body) = scoped("GetAutoManagementConfiguration", &json!({}), account);
        assert_eq!(status, 200);
        assert_eq!(body["OptInLevel"], "ACCOUNT");
        assert_eq!(body["OptInType"], "NotifyOnly");
        assert_eq!(body["OptInStatus"], "ENABLED");
        assert_eq!(
            body["ExclusionList"]["vpc"][0]["QuotaCode"],
            "L-F678F1CE"
        );

        // Update replaces the fields present in the request.
        let (status, _) = scoped(
            "UpdateAutoManagement",
            &json!({"OptInType": "NotifyAndAdjust"}),
            account,
        );
        assert_eq!(status, 200);
        let (status, body) = scoped("GetAutoManagementConfiguration", &json!({}), account);
        assert_eq!(status, 200);
        assert_eq!(body["OptInType"], "NotifyAndAdjust");
        assert_eq!(body["ExclusionList"]["vpc"][0]["QuotaCode"], "L-F678F1CE");

        // Stop clears the configuration again.
        let (status, _) = scoped("StopAutoManagement", &json!({}), account);
        assert_eq!(status, 200);
        let (status, body) = scoped("GetAutoManagementConfiguration", &json!({}), account);
        assert_eq!(status, 400);
        assert_eq!(body["__type"], "NoSuchResourceException");

        // Update without a prior start is the same fault.
        let (status, _body) = scoped(
            "UpdateAutoManagement",
            &json!({"OptInType": "NotifyAndAdjust"}),
            account,
        );
        assert_eq!(status, 400);
        assert_eq!(body["__type"], "NoSuchResourceException");
    }

    #[test]
    fn quota_utilization_report_status_machine() {
        let account = scope();
        let (status, body) = scoped("StartQuotaUtilizationReport", &json!({}), account);
        assert_eq!(status, 200, "{body}");
        assert_eq!(body["Status"], "PENDING");
        assert!(body["ReportId"].as_str().unwrap().starts_with("quota-report-"));
        assert!(body["Message"].as_str().is_some());

        // First read: PENDING → IN_PROGRESS with no rows yet.
        let (status, body) = scoped(
            "GetQuotaUtilizationReport",
            &json!({"ReportId": body["ReportId"]}),
            account,
        );
        assert_eq!(status, 200, "{body}");
        assert_eq!(body["Status"], "IN_PROGRESS");
        assert_eq!(body["TotalCount"], 25);
        assert!(body["Quotas"].as_array().unwrap().is_empty());
        assert!(body["GeneratedAt"].as_str().is_none());

        // Second read: COMPLETED with the full quota rows and GeneratedAt.
        let (status, body) = scoped(
            "GetQuotaUtilizationReport",
            &json!({"ReportId": "quota-report-1"}),
            account,
        );
        assert_eq!(status, 200, "{body}");
        assert_eq!(body["Status"], "COMPLETED");
        assert!(body["GeneratedAt"].as_str().is_some());
        let rows = body["Quotas"].as_array().unwrap();
        assert_eq!(rows.len(), 25);
        assert!(rows[0]["Utilization"].as_f64().unwrap() > 0.0);
        assert_eq!(rows[0]["ServiceCode"], "vpc");

        // Paged reads: MaxResults returns a page + token.
        let (status, body) = scoped(
            "GetQuotaUtilizationReport",
            &json!({"ReportId": "quota-report-1", "MaxResults": 2}),
            account,
        );
        assert_eq!(status, 200);
        assert_eq!(body["Quotas"].as_array().unwrap().len(), 2);
        assert_eq!(body["NextToken"], json!("2"));

        // Unknown report id answers the modeled fault.
        let (status, body) = scoped(
            "GetQuotaUtilizationReport",
            &json!({"ReportId": "quota-report-9"}),
            account,
        );
        assert_eq!(status, 400);
        assert_eq!(body["__type"], "NoSuchResourceException");
    }

    #[test]
    fn tag_round_trip_and_support_case_dedupe() {
        let account = scope();
        let quota_arn = "arn:aws:servicequotas:eu-west-1::vpc/L-F678F1CE";

        let (status, _) = scoped(
            "TagResource",
            &json!({"ResourceARN": quota_arn, "Tags": [{"Key": "env", "Value": "test"}]}),
            account,
        );
        assert_eq!(status, 200);
        let (status, body) = scoped("ListTagsForResource", &json!({"ResourceARN": quota_arn}), account);
        assert_eq!(status, 200);
        assert_eq!(body["Tags"][0]["Key"], "env");
        assert_eq!(body["Tags"][0]["Value"], "test");

        // Untag then list again.
        let (status, _) = scoped(
            "UntagResource",
            &json!({"ResourceARN": quota_arn, "TagKeys": ["env"]}),
            account,
        );
        assert_eq!(status, 200);
        let (status, body) = scoped("ListTagsForResource", &json!({"ResourceARN": quota_arn}), account);
        assert_eq!(status, 200);
        assert!(body["Tags"].as_array().unwrap().is_empty());

        // A non-ARN resource answers moto's fault.
        let (status, body) = scoped(
            "TagResource",
            &json!({"ResourceARN": "not-an-arn", "Tags": [{"Key": "a", "Value": "b"}]}),
            account,
        );
        assert_eq!(status, 400);
        assert_eq!(body["__type"], "NoSuchResourceException");

        // CreateSupportCase is create-only: the same RequestId faults.
        let (status, _) = scoped("CreateSupportCase", &json!({"RequestId": "qreq-1"}), account);
        assert_eq!(status, 200);
        let (status, body) = scoped("CreateSupportCase", &json!({"RequestId": "qreq-1"}), account);
        assert_eq!(status, 400);
        assert_eq!(body["__type"], "ResourceAlreadyExistsException");
        let (status, _) = scoped("CreateSupportCase", &json!({"RequestId": "qreq-2"}), account);
        assert_eq!(status, 200);
    }

    #[test]
    fn scopes_are_isolated() {
        let a = scope();
        let b = scope();
        let (status, _) = scoped(
            "RequestServiceQuotaIncrease",
            &json!({"ServiceCode": "vpc", "QuotaCode": "L-7E9ECCDB", "DesiredValue": 51.0}),
            a,
        );
        assert_eq!(status, 200);
        // Same quota in a different scope has no open requests.
        let (status, body) = scoped(
            "RequestServiceQuotaIncrease",
            &json!({"ServiceCode": "vpc", "QuotaCode": "L-7E9ECCDB", "DesiredValue": 52.0}),
            b,
        );
        assert_eq!(status, 200, "{body}");
        assert_eq!(body["RequestedQuota"]["Id"], "qreq-1");
    }
}
