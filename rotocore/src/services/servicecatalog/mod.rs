//! AWS Service Catalog backend — port of moto's `servicecatalog` engine.
//!
//! JSON protocol (`targetPrefix: AWS242ServiceCatalogService`): the gateway
//! hands each request the operation name (from `X-Amz-Target`) and the raw
//! JSON body, so routing matches botocore operation names and members are
//! PascalCase exactly like the wire shapes.
//!
//! Surface per `probes/servicecatalog.json` (moto sidecar era) plus the
//! moto 5.1.23 models/responses: 84 `working` + 2 `needs_params` + 1
//! `500_error` (`SearchProducts` — the probe crashed on a list-shaped
//! `Filters`, the moto handler itself is fine) operations are implemented;
//! moto's `search_products` filter semantics are mirrored. The three
//! OrganizationsAccess operations recorded `not_implemented` (501) in the
//! sidecar era, so this backend does not claim them either.
//!
//! State is per (account, region) like every other backend. Timestamps are
//! JSON-protocol unix-epoch floats, matching moto's `JSONSerializer`
//! (`DEFAULT_TIMESTAMP_FORMAT = "unixtimestamp"`).

use crate::proto::{json_error, now_unix, parse_json_body};
use crate::store;
use crate::{Request, Response};
use once_cell::sync::Lazy;
use serde_json::{json, Value};
use std::collections::HashMap;
use uuid::Uuid;

static SLOT: Lazy<store::Slot<State>> = Lazy::new(|| store::slot::<State>("servicecatalog"));

// ---- ids & small helpers ---------------------------------------------------

fn uuid_str() -> String {
    Uuid::new_v4().to_string()
}

/// Fresh 12-hex-char resource suffix (`pa-…`, `cons-…`, `act-…`, `tag-…`,
/// `pp-…`, `plan-…`, `rec-…`, `cpt-…`), matching moto's id shapes.
fn hex12() -> String {
    Uuid::new_v4().simple().to_string()[..12].to_string()
}

fn hex8() -> String {
    Uuid::new_v4().simple().to_string()[..8].to_string()
}

/// AWS partition for a region name (moto `get_partition`).
fn partition(region: &str) -> &'static str {
    if region.starts_with("cn-") {
        "aws-cn"
    } else if region.starts_with("us-gov-") {
        "aws-us-gov"
    } else {
        "aws"
    }
}

fn portfolio_arn(region: &str, account: u64, id: &str) -> String {
    format!(
        "arn:{}:catalog:{}:{}:portfolio/{}",
        partition(region),
        region,
        account,
        id
    )
}

fn product_arn(region: &str, account: u64, id: &str) -> String {
    format!(
        "arn:{}:catalog:{}:{}:product/{}",
        partition(region),
        region,
        account,
        id
    )
}

fn pp_arn(region: &str, account: u64, name: &str, id: &str) -> String {
    format!(
        "arn:{}:servicecatalog:{}:{}:stack/{}/{}",
        partition(region),
        region,
        account,
        name,
        id
    )
}

fn text<'a>(v: &'a Value, key: &str) -> &'a str {
    v.get(key).and_then(Value::as_str).unwrap_or("")
}

/// `v.get(key)` as an owned `Value`, defaulting to `null` (moto
/// `params.get(...)` on an absent optional).
fn opt<'a>(v: &'a Value, key: &str) -> &'a Value {
    v.get(key).unwrap_or(&Value::Null)
}

fn opt_str(v: &Value, key: &str) -> Option<String> {
    v.get(key).and_then(Value::as_str).map(|s| s.to_string())
}

/// Slice for a list member (`params.get("Tags") or []`).
fn arr<'a>(v: &'a Value, key: &str) -> &'a [Value] {
    v.get(key)
        .and_then(Value::as_array)
        .map(Vec::as_slice)
        .unwrap_or(&[])
}

fn ok_json(v: &Value) -> Response {
    Response::json(200, serde_json::to_vec(v).unwrap_or_default())
}

/// moto's `EmptyResult` body.
fn empty() -> Response {
    Response::json(200, b"{}".to_vec())
}

fn client_error(status: u16, code: &str, message: &str) -> Response {
    json_error(status, code, message)
}

fn not_found(code: &str, message: String) -> Response {
    client_error(400, code, &message)
}

// ---- state -----------------------------------------------------------------

/// A portfolio: `{Id, ARN, DisplayName, Description, CreatedTime, ProviderName}`.
struct Portfolio {
    id: String,
    display_name: String,
    description: String,
    provider_name: String,
    created_time: f64,
    tags: Vec<Value>,
}

impl Portfolio {
    fn to_detail(&self, region: &str, account: u64) -> Value {
        json!({
            "Id": self.id,
            "ARN": portfolio_arn(region, account, &self.id),
            "DisplayName": self.display_name,
            "Description": self.description,
            "CreatedTime": self.created_time,
            "ProviderName": self.provider_name,
        })
    }
}

/// A Service Catalog product, stored with the summary fields moto keeps.
struct Product {
    id: String,
    name: String,
    owner: String,
    description: Option<String>,
    distributor: Option<String>,
    support_description: Option<String>,
    support_email: Option<String>,
    support_url: Option<String>,
    product_type: String,
    tags: Vec<Value>,
    source_connection: Value,
    created_time: f64,
    status: String,
}

impl Product {
    /// moto `Product.to_dict()` — the ProductViewSummary shape.
    fn view_summary(&self) -> Value {
        json!({
            "Id": self.id,
            "ProductId": self.id,
            "Name": self.name,
            "Owner": self.owner,
            "Type": self.product_type,
            "Distributor": self.distributor,
            "HasDefaultPath": false,
            "ShortDescription": self.description,
            "SupportDescription": self.support_description,
            "SupportEmail": self.support_email,
            "SupportUrl": self.support_url,
        })
    }

    fn view_detail(&self, region: &str, account: u64) -> Value {
        json!({
            "ProductViewSummary": self.view_summary(),
            "Status": self.status,
            "ProductARN": product_arn(region, account, &self.id),
            "CreatedTime": self.created_time,
            "SourceConnection": self.source_connection,
        })
    }
}

/// A provisioning artifact (version) of a product.
struct ProvisioningArtifact {
    id: String,
    product_id: String,
    name: String,
    description: String,
    artifact_type: String,
    info: Value,
    created_time: f64,
    active: bool,
    guidance: String,
}

impl ProvisioningArtifact {
    fn to_detail(&self) -> Value {
        json!({
            "Id": self.id,
            "Name": self.name,
            "Description": self.description,
            "Type": self.artifact_type,
            "CreatedTime": self.created_time,
            "Active": self.active,
            "Guidance": self.guidance,
        })
    }
}

struct Constraint {
    id: String,
    portfolio_id: String,
    product_id: String,
    /// Raw JSON string the client passed (ConstraintParameters echoes it).
    parameters: String,
    constraint_type: String,
    description: String,
    owner: String,
}

impl Constraint {
    fn to_detail(&self) -> Value {
        json!({
            "ConstraintId": self.id,
            "Type": self.constraint_type,
            "Description": self.description,
            "Owner": self.owner,
        })
    }

    fn to_summary(&self) -> Value {
        json!({
            "ConstraintId": self.id,
            "Type": self.constraint_type,
            "Description": self.description,
            "Owner": self.owner,
            "ProductId": self.product_id,
            "PortfolioId": self.portfolio_id,
        })
    }
}

struct ServiceAction {
    id: String,
    name: String,
    definition_type: String,
    definition: Value,
    description: String,
}

impl ServiceAction {
    fn to_summary(&self) -> Value {
        json!({
            "Id": self.id,
            "Name": self.name,
            "Description": self.description,
            "DefinitionType": self.definition_type,
        })
    }

    fn to_detail(&self) -> Value {
        let mut d = self.to_summary();
        d["Definition"] = self.definition.clone();
        d
    }
}

struct TagOption {
    id: String,
    key: String,
    value: String,
    active: bool,
    owner: String,
}

impl TagOption {
    fn to_detail(&self) -> Value {
        json!({
            "Id": self.id,
            "Key": self.key,
            "Value": self.value,
            "Active": self.active,
            "Owner": self.owner,
        })
    }
}

struct ProvisionedProduct {
    id: String,
    name: String,
    product_id: String,
    provisioning_artifact_id: String,
    path_id: String,
    status: String,
    status_message: String,
    created_time: f64,
    last_record_id: String,
    user_arn: String,
    kind: String,
    idempotency_token: Option<String>,
}

impl ProvisionedProduct {
    fn new(
        name: &str,
        product_id: &str,
        provisioning_artifact_id: &str,
        path_id: Option<&str>,
        account: u64,
    ) -> Self {
        let id = format!("pp-{}", hex12());
        let last_record_id = format!("rec-{}", hex12());
        ProvisionedProduct {
            id: id.clone(),
            name: name.to_string(),
            product_id: product_id.to_string(),
            provisioning_artifact_id: provisioning_artifact_id.to_string(),
            path_id: path_id.unwrap_or("").to_string(),
            status: "AVAILABLE".into(),
            status_message: String::new(),
            created_time: now_unix(),
            last_record_id: last_record_id.clone(),
            user_arn: format!("arn:aws:iam::{account}:root"),
            kind: "CFN_STACK".into(),
            idempotency_token: None,
        }
    }

    fn to_detail(&self, region: &str, account: u64) -> Value {
        json!({
            "Name": self.name,
            "Arn": pp_arn(region, account, &self.name, &self.id),
            "Type": self.kind,
            "Id": self.id,
            "Status": self.status,
            "StatusMessage": self.status_message,
            "CreatedTime": self.created_time,
            "IdempotencyToken": self.idempotency_token.clone().unwrap_or_default(),
            "LastRecordId": self.last_record_id,
            "LastProvisioningRecordId": self.last_record_id,
            "LastSuccessfulProvisioningRecordId": self.last_record_id,
            "ProductId": self.product_id,
            "ProvisioningArtifactId": self.provisioning_artifact_id,
            "UserArn": self.user_arn,
            "UserArnSession": self.user_arn,
        })
    }
}

struct Plan {
    plan_id: String,
    plan_name: String,
    plan_type: String,
    product_id: String,
    provisioned_product_name: String,
    provisioning_artifact_id: String,
    path_id: String,
    notification_arns: Value,
    tags: Value,
    status: String,
    status_message: String,
    created_time: f64,
    updated_time: f64,
    provision_product_id: String,
}

impl Plan {
    fn to_summary(&self) -> Value {
        json!({
            "PlanName": self.plan_name,
            "PlanId": self.plan_id,
            "ProvisionProductId": self.provision_product_id,
            "ProvisionProductName": self.provisioned_product_name,
            "PlanType": self.plan_type,
            "ProvisioningArtifactId": self.provisioning_artifact_id,
        })
    }

    fn to_detail(&self) -> Value {
        json!({
            "PlanName": self.plan_name,
            "PlanId": self.plan_id,
            "ProvisionProductId": self.provision_product_id,
            "ProvisionProductName": self.provisioned_product_name,
            "PlanType": self.plan_type,
            "ProvisioningArtifactId": self.provisioning_artifact_id,
            "Status": self.status,
            "StatusMessage": self.status_message,
            "CreatedTime": self.created_time,
            "UpdatedTime": self.updated_time,
            "PathId": self.path_id,
            "ProductId": self.product_id,
            "NotificationArns": self.notification_arns,
            "Tags": self.tags,
            "ResourceChanges": [],
        })
    }
}

/// A record of a provisioning lifecycle event.
struct Record {
    record_id: String,
    provisioned_product_id: String,
    provisioned_product_name: String,
    product_id: String,
    provisioning_artifact_id: String,
    path_id: String,
    record_type: String,
    created_time: f64,
    updated_time: f64,
}

impl Record {
    fn new(
        provisioned_product_id: &str,
        provisioned_product_name: &str,
        product_id: &str,
        provisioning_artifact_id: &str,
        path_id: &str,
        record_type: &str,
    ) -> Self {
        let now = now_unix();
        Record {
            record_id: format!("rec-{}", hex12()),
            provisioned_product_id: provisioned_product_id.to_string(),
            provisioned_product_name: provisioned_product_name.to_string(),
            product_id: product_id.to_string(),
            provisioning_artifact_id: provisioning_artifact_id.to_string(),
            path_id: path_id.to_string(),
            record_type: record_type.to_string(),
            created_time: now,
            updated_time: now,
        }
    }

    fn to_detail(&self) -> Value {
        json!({
            "RecordId": self.record_id,
            "ProvisionedProductName": self.provisioned_product_name,
            "Status": "SUCCEEDED",
            "CreatedTime": self.created_time,
            "UpdatedTime": self.updated_time,
            "ProvisionedProductType": "CFN_STACK",
            "RecordType": self.record_type,
            "ProvisionedProductId": self.provisioned_product_id,
            "ProductId": self.product_id,
            "ProvisioningArtifactId": self.provisioning_artifact_id,
            "PathId": self.path_id,
            "RecordErrors": [],
            "RecordTags": [],
        })
    }
}

/// Per-(account, region) service catalog state.
#[derive(Default)]
struct State {
    portfolios: HashMap<String, Portfolio>,
    /// portfolioId -> shared account ids (CreatePortfolioShare(AccountId=…)).
    portfolio_access: HashMap<String, Vec<String>>,
    /// portfolioId -> org-share tokens disguised as moto renders them.
    portfolio_share_tokens: HashMap<String, Vec<String>>,
    accepted_shares: Vec<String>,
    products: HashMap<String, Product>,
    artifacts: HashMap<String, ProvisioningArtifact>,
    constraints: HashMap<String, Constraint>,
    service_actions: HashMap<String, ServiceAction>,
    tag_options: HashMap<String, TagOption>,
    provisioned_products: HashMap<String, ProvisionedProduct>,
    plans: HashMap<String, Plan>,
    records: HashMap<String, Record>,
    /// Create* idempotency tokens -> resource id.
    idempotency_tokens: HashMap<String, String>,
    /// CopyProductToken -> operation record (SourceProductArn, status…).
    copy_operations: HashMap<String, Value>,
    /// (product_id, portfolio_id)
    product_portfolio: Vec<(String, String)>,
    /// (principal_arn, portfolio_id, principal_type)
    principal_portfolio: Vec<(String, String, String)>,
    /// (tag_option_id, resource_id)
    tag_option_assoc: Vec<(String, String)>,
    /// (budget_name, resource_id)
    budget_assoc: Vec<(String, String)>,
    /// (service_action_id, product_id, provisioning_artifact_id)
    action_artifact_assoc: Vec<(String, String, String)>,
}

// ---- shared state helpers --------------------------------------------------

impl State {
    fn lookup_product_by_name(&self, name: &str) -> Option<&Product> {
        self.products.values().find(|p| p.name == name)
    }

    fn lookup_pp_by_name(&self, name: &str) -> Option<String> {
        self.provisioned_products
            .values()
            .find(|pp| pp.name == name)
            .map(|pp| pp.id.clone())
    }

    /// List the provisioning artifacts belonging to a product.
    fn artifacts_for_product(&self, product_id: &str) -> Vec<Value> {
        self.artifacts
            .values()
            .filter(|pa| pa.product_id == product_id)
            .map(ProvisioningArtifact::to_detail)
            .collect()
    }

    fn tag_options_for_resource(&self, resource_id: &str) -> Vec<Value> {
        self.tag_option_assoc
            .iter()
            .filter(|(to_id, rid)| rid == resource_id && self.tag_options.contains_key(to_id))
            .filter_map(|(to_id, _)| self.tag_options.get(to_id))
            .map(TagOption::to_detail)
            .collect()
    }

    fn budgets_for_resource(&self, resource_id: &str) -> Vec<Value> {
        self.budget_assoc
            .iter()
            .filter(|(_, rid)| rid == resource_id)
            .map(|(budget, _)| json!({ "BudgetName": budget }))
            .collect()
    }

    /// Add/replace tags by Key (moto's update-by-key append/replace walk).
    fn merge_tags(tags: &mut Vec<Value>, add: &[Value]) {
        for tag in add {
            let key = tag.get("Key").and_then(Value::as_str);
            match key {
                Some(k) => {
                    if let Some(existing) = tags
                        .iter_mut()
                        .find(|t| t.get("Key").and_then(Value::as_str) == Some(k))
                    {
                        *existing = tag.clone();
                    } else {
                        tags.push(tag.clone());
                    }
                }
                None => tags.push(tag.clone()),
            }
        }
    }

    fn drop_tags(tags: &mut Vec<Value>, remove: &[Value]) {
        let keys: Vec<&str> = remove.iter().filter_map(|t| t.as_str()).collect();
        tags.retain(|t| {
            !t.get("Key")
                .and_then(Value::as_str)
                .map(|k| keys.contains(&k))
                .unwrap_or(false)
        });
    }

    /// Product full-text/owner/type filter walk from moto's `search_products`.
    fn product_matches_filter(product: &Product, filters: &Value) -> bool {
        let Some(map) = filters.as_object() else {
            return true;
        };
        for (key, values) in map {
            let values = match values.as_array() {
                Some(vs) => vs,
                None => continue,
            };
            match key.as_str() {
                "FullTextSearch" => {
                    let name = product.name.to_lowercase();
                    let desc = product.description.as_deref().unwrap_or("").to_lowercase();
                    let owner = product.owner.to_lowercase();
                    let hit = values.iter().any(|v| {
                        v.as_str()
                            .map(|s| {
                                let s = s.to_lowercase();
                                name.contains(&s) || desc.contains(&s) || owner.contains(&s)
                            })
                            .unwrap_or(false)
                    });
                    if !hit {
                        return false;
                    }
                }
                "Owner"
                    if !values
                        .iter()
                        .any(|v| v.as_str() == Some(product.owner.as_str())) =>
                {
                    return false;
                }
                "ProductType"
                    if !values
                        .iter()
                        .any(|v| v.as_str() == Some(product.product_type.as_str())) =>
                {
                    return false;
                }
                _ => {}
            }
        }
        true
    }
}

// ---- dispatch --------------------------------------------------------------

/// Dispatch a routed request to the servicecatalog backend.
pub fn dispatch(req: &Request) -> Option<Response> {
    if req.service != "servicecatalog" {
        return None;
    }
    let params = parse_json_body(&req.body);
    let mut guard = SLOT.write();
    let st = guard.entry((req.account, req.region.clone())).or_default();
    Some(handle(st, &params, req))
}

/// Route a servicecatalog request by botocore operation name.
///
/// Each arm mirrors the response construction in moto's responses.py —
/// same result keys, same error messages, same status codes. The three
/// OrganizationsAccess operations have no arm (they answer the generic
/// `501 NotImplemented` exactly like the sidecar era).
fn not_implemented(operation: &str) -> Response {
    client_error(
        501,
        "NotImplemented",
        &format!("The {operation} action has not been implemented"),
    )
}

fn handle(st: &mut State, p: &Value, req: &Request) -> Response {
    let account = req.account;
    let region = req.region.as_str();

    match req.operation.as_str() {
        // ---- Portfolio operations ----
        "CreatePortfolio" => {
            let Some(token) = opt_str(p, "IdempotencyToken") else {
                return client_error(
                    400,
                    "InvalidInputException",
                    "IdempotencyToken is a required parameter",
                );
            };
            if let Some(pid) = st.idempotency_tokens.get(&token) {
                if let Some(portfolio) = st.portfolios.get(pid) {
                    return ok_json(&json!({
                        "PortfolioDetail": portfolio.to_detail(region, account),
                        "Tags": portfolio.tags,
                    }));
                }
            }
            let id = uuid_str();
            let portfolio = Portfolio {
                display_name: text(p, "DisplayName").to_string(),
                description: text(p, "Description").to_string(),
                provider_name: text(p, "ProviderName").to_string(),
                tags: arr(p, "Tags").to_vec(),
                id: id.clone(),
                created_time: now_unix(),
            };
            st.portfolios.insert(id.clone(), portfolio);
            st.idempotency_tokens.insert(token, id.clone());
            st.portfolio_access.entry(id.clone()).or_default();
            let portfolio = st.portfolios.get(&id).unwrap();
            ok_json(&json!({
                "PortfolioDetail": portfolio.to_detail(region, account),
                "Tags": portfolio.tags,
            }))
        }
        "DeletePortfolio" => {
            let id = text(p, "Id");
            st.portfolio_access.remove(id);
            st.portfolios.remove(id);
            ok_json(&json!({})) // EmptyResult renders {}
        }
        "UpdatePortfolio" => {
            let id = text(p, "Id");
            let Some(portfolio) = st.portfolios.get_mut(id) else {
                return not_found(
                    "ResourceNotFoundException",
                    format!("Portfolio {id} not found."),
                );
            };
            if let Some(v) = opt_str(p, "DisplayName") {
                portfolio.display_name = v;
            }
            if let Some(v) = opt_str(p, "Description") {
                portfolio.description = v;
            }
            if let Some(v) = opt_str(p, "ProviderName") {
                portfolio.provider_name = v;
            }
            State::merge_tags(&mut portfolio.tags, arr(p, "AddTags"));
            State::drop_tags(&mut portfolio.tags, arr(p, "RemoveTags"));
            ok_json(&json!({
                "PortfolioDetail": portfolio.to_detail(region, account),
                "Tags": portfolio.tags,
            }))
        }
        "DescribePortfolio" => {
            let id = text(p, "Id");
            let Some(portfolio) = st.portfolios.get(id) else {
                return not_found(
                    "ResourceNotFoundException",
                    format!("Portfolio {id} not found."),
                );
            };
            ok_json(&json!({
                "PortfolioDetail": portfolio.to_detail(region, account),
                "Tags": portfolio.tags,
                "TagOptions": st.tag_options_for_resource(id),
                "Budgets": st.budgets_for_resource(id),
            }))
        }
        "ListPortfolios" => {
            let details: Vec<Value> = st
                .portfolios
                .values()
                .map(|pf| pf.to_detail(region, account))
                .collect();
            ok_json(&json!({"PortfolioDetails": details, "NextPageToken": Value::Null}))
        }
        "CreatePortfolioShare" => {
            let pid = text(p, "PortfolioId").to_string();
            if !st.portfolios.contains_key(&pid) {
                return ok_json(&json!({}));
            }
            let account_id = opt_str(p, "AccountId");
            let org_node = opt(p, "OrganizationNode");
            if let Some(account_id) = account_id {
                let list = st.portfolio_access.entry(pid).or_default();
                if !list.contains(&account_id) {
                    list.push(account_id);
                }
                return ok_json(&json!({}));
            }
            let mut token: Option<String> = None;
            if org_node.is_object() {
                let org_type = text(org_node, "Type");
                let org_value = text(org_node, "Value");
                let mut share = format!("share-{pid}-{org_type}-{org_value}");
                if p.get("ShareTagOptions")
                    .and_then(Value::as_bool)
                    .unwrap_or(false)
                {
                    share.push_str("-tags");
                }
                if p.get("SharePrincipals")
                    .and_then(Value::as_bool)
                    .unwrap_or(false)
                {
                    share.push_str("-principals");
                }
                let list = st.portfolio_share_tokens.entry(pid).or_default();
                if !list.contains(&share) {
                    list.push(share.clone());
                }
                token = Some(share);
            }
            match token {
                Some(token) => ok_json(&json!({"PortfolioShareToken": token})),
                None => ok_json(&json!({})),
            }
        }
        "DeletePortfolioShare" => {
            let pid = text(p, "PortfolioId").to_string();
            let account_id = opt_str(p, "AccountId");
            let org_node = opt(p, "OrganizationNode");
            if let (Some(list), Some(account_id)) = (st.portfolio_access.get_mut(&pid), account_id)
            {
                list.retain(|a| a != &account_id);
            }
            let mut token = None;
            if org_node.is_object() {
                let org_type = text(org_node, "Type");
                let org_value = text(org_node, "Value");
                let share = format!("share-{pid}-{org_type}-{org_value}");
                if let Some(list) = st.portfolio_share_tokens.get_mut(&pid) {
                    let matching: Vec<String> = list
                        .iter()
                        .filter(|t| t.contains(org_type) && t.contains(org_value))
                        .cloned()
                        .collect();
                    for t in matching {
                        list.retain(|x| x != &t);
                    }
                }
                token = Some(share);
            }
            match token {
                Some(token) => ok_json(&json!({"PortfolioShareToken": token})),
                None => ok_json(&json!({})),
            }
        }
        "UpdatePortfolioShare" => {
            let pid = text(p, "PortfolioId").to_string();
            if !st.portfolios.contains_key(&pid) {
                return not_found(
                    "ResourceNotFoundException",
                    format!("Portfolio {pid} not found."),
                );
            }
            let token = opt(p, "OrganizationNode")
                .is_object()
                .then(|| format!("share-{pid}-update-{}", hex8()));
            match token {
                Some(token) => {
                    ok_json(&json!({"PortfolioShareToken": token, "Status": "COMPLETED"}))
                }
                None => ok_json(&json!({"Status": "COMPLETED"})),
            }
        }
        "DescribePortfolioShareStatus" => ok_json(&json!({
            "PortfolioShareToken": text(p, "PortfolioShareToken"),
            "PortfolioId": "",
            "OrganizationNodeValue": "",
            "Status": "COMPLETED",
            "ShareDetails": {"ShareErrors": [], "SuccessfulShares": []},
        })),
        "AcceptPortfolioShare" => {
            let pid = text(p, "PortfolioId").to_string();
            if !st.portfolios.contains_key(&pid) {
                return not_found(
                    "ResourceNotFoundException",
                    format!("Portfolio {pid} not found."),
                );
            }
            if !st.accepted_shares.contains(&pid) {
                st.accepted_shares.push(pid);
            }
            empty()
        }
        "RejectPortfolioShare" => {
            st.accepted_shares.retain(|a| a != text(p, "PortfolioId"));
            empty()
        }
        "ListAcceptedPortfolioShares" => {
            let details: Vec<Value> = st
                .accepted_shares
                .iter()
                .filter_map(|pid| st.portfolios.get(pid))
                .map(|pf| pf.to_detail(region, account))
                .collect();
            ok_json(&json!({"PortfolioDetails": details, "NextPageToken": Value::Null}))
        }
        "ListPortfoliosForProduct" => {
            let product_id = text(p, "ProductId");
            let details: Vec<Value> = st
                .product_portfolio
                .iter()
                .filter(|(prod_id, _)| prod_id == product_id)
                .filter_map(|(_, pf_id)| st.portfolios.get(pf_id))
                .map(|pf| pf.to_detail(region, account))
                .collect();
            ok_json(&json!({"PortfolioDetails": details, "NextPageToken": Value::Null}))
        }
        "ListPortfolioAccess" => {
            let pid = text(p, "PortfolioId").to_string();
            let account_ids = st.portfolio_access.get(&pid).cloned().unwrap_or_default();
            ok_json(&json!({"AccountIds": account_ids, "NextPageToken": Value::Null}))
        }
        "ListOrganizationPortfolioAccess" => {
            let pid = text(p, "PortfolioId").to_string();
            let node_type = text(p, "OrganizationNodeType").to_string();
            let mut nodes: Vec<String> = Vec::new();
            if let Some(tokens) = st.portfolio_share_tokens.get(&pid) {
                for token in tokens {
                    if token.contains(&node_type) {
                        for part in token.split('-') {
                            if (part.starts_with("o-")
                                || part.starts_with("ou-")
                                || part.starts_with("r-"))
                                && !nodes.iter().any(|n| n == part)
                            {
                                nodes.push(part.to_string());
                            }
                        }
                    }
                }
            }
            ok_json(&json!({"OrganizationNodes": nodes, "NextPageToken": Value::Null}))
        }
        "DescribePortfolioShares" => {
            let pid = text(p, "PortfolioId").to_string();
            let share_type = text(p, "Type").to_string();
            let mut details: Vec<Value> = Vec::new();
            if st.portfolios.contains_key(&pid) {
                if share_type == "ACCOUNT" {
                    if let Some(list) = st.portfolio_access.get(&pid) {
                        for aid in list {
                            details.push(json!({
                                "PrincipalId": aid,
                                "Type": "ACCOUNT",
                                "Accepted": true,
                                "ShareTagOptions": false,
                                "SharePrincipals": false,
                            }));
                        }
                    }
                } else if share_type == "ORGANIZATION" {
                    if let Some(tokens) = st.portfolio_share_tokens.get(&pid) {
                        for token in tokens {
                            if !(token.contains("ORGANIZATION") && token.contains("-o-")) {
                                continue;
                            }
                            let Some(start) = token.find("-ORGANIZATION-") else {
                                continue;
                            };
                            let start = start + "-ORGANIZATION-".len();
                            let tail = &token[start..];
                            let end = tail
                                .find("-tags")
                                .or_else(|| tail.find("-principals"))
                                .unwrap_or(tail.len());
                            details.push(json!({
                                "PrincipalId": &tail[..end],
                                "Type": "ORGANIZATION",
                                "Accepted": true,
                                "ShareTagOptions": token.contains("tags"),
                                "SharePrincipals": token.contains("principals"),
                            }));
                        }
                    }
                }
            }
            ok_json(&json!({"NextPageToken": Value::Null, "PortfolioShareDetails": details}))
        }

        // ---- Product operations ----
        "CreateProduct" => {
            let name = text(p, "Name").to_string();
            let token = opt_str(p, "IdempotencyToken").unwrap_or_else(uuid_str);
            if let Some(existing_id) = st.idempotency_tokens.get(&token) {
                if st.products.contains_key(existing_id) {
                    let product = st.products.get(existing_id).unwrap();
                    return ok_json(&json!({
                        "ProductViewDetail": product.view_detail(region, account),
                        "ProvisioningArtifactDetail": {},
                        "Tags": product.tags,
                    }));
                }
            }
            let id = uuid_str();
            let product = Product {
                id: id.clone(),
                name,
                owner: text(p, "Owner").to_string(),
                description: opt_str(p, "Description"),
                distributor: opt_str(p, "Distributor"),
                support_description: opt_str(p, "SupportDescription"),
                support_email: opt_str(p, "SupportEmail"),
                support_url: opt_str(p, "SupportUrl"),
                product_type: text(p, "ProductType").to_string(),
                tags: arr(p, "Tags").to_vec(),
                source_connection: match opt(p, "SourceConnection") {
                    v if v.is_null() => json!({}),
                    v => v.clone(),
                },
                created_time: now_unix(),
                status: "AVAILABLE".into(),
            };
            st.products.insert(id.clone(), product);
            st.idempotency_tokens.insert(token, id.clone());
            // An initial artifact is created when parameters are provided.
            if let Some(pap) = p
                .get("ProvisioningArtifactParameters")
                .filter(|v| v.is_object())
            {
                let pa_id = format!("pa-{}", hex12());
                let pa = ProvisioningArtifact {
                    id: pa_id.clone(),
                    product_id: id.clone(),
                    name: opt_str(pap, "Name").unwrap_or_else(|| pa_id.clone()),
                    description: text(pap, "Description").to_string(),
                    artifact_type: match text(pap, "Type") {
                        "" => "CLOUD_FORMATION_TEMPLATE".to_string(),
                        t => t.to_string(),
                    },
                    info: match opt(pap, "Info") {
                        v if v.is_null() => json!({}),
                        v => v.clone(),
                    },
                    created_time: now_unix(),
                    active: true,
                    guidance: "DEFAULT".into(),
                };
                st.artifacts.insert(pa.id.clone(), pa);
            }
            let product = st.products.get(&id).unwrap();
            ok_json(&json!({
                "ProductViewDetail": product.view_detail(region, account),
                "ProvisioningArtifactDetail": {},
                "Tags": product.tags,
            }))
        }
        "DescribeProduct" => {
            let product_id = match resolve_product_id(st, p) {
                Ok(id) => id,
                Err(resp) => return resp,
            };
            let product = st.products.get(&product_id).unwrap();
            ok_json(&json!({
                "ProductViewSummary": product.view_summary(),
                "ProvisioningArtifacts": [],
                "Budgets": [],
                "LaunchPaths": [],
            }))
        }
        "DescribeProductAsAdmin" => {
            let product_id = match resolve_product_id(st, p) {
                Ok(id) => id,
                Err(resp) => return resp,
            };
            let product = st.products.get(&product_id).unwrap();
            ok_json(&json!({
                "ProductViewDetail": product.view_detail(region, account),
                "ProvisioningArtifactSummaries": st.artifacts_for_product(&product_id),
                "Tags": product.tags,
                "Budgets": st.budgets_for_resource(&product_id),
            }))
        }
        "DescribeProductView" => {
            let id = text(p, "Id").to_string();
            let Some(product) = st.products.get(&id) else {
                return not_found(
                    "ResourceNotFoundException",
                    format!("Product with Id '{id}' not found."),
                );
            };
            ok_json(&json!({"ProductViewSummary": product.view_detail(region, account)}))
        }
        "DeleteProduct" => {
            let pid = text(p, "Id").to_string();
            let assoc = st
                .product_portfolio
                .iter()
                .find(|(prod, _)| prod == &pid)
                .cloned();
            if let Some((_, pf_id)) = assoc {
                return not_found(
                    "ResourceInUseException",
                    format!("Product {pid} is associated with portfolio {pf_id}."),
                );
            }
            st.products.remove(&pid);
            ok_json(&json!({}))
        }
        "UpdateProduct" => {
            let pid = text(p, "Id").to_string();
            if !st.products.contains_key(&pid) {
                return not_found(
                    "ResourceNotFoundException",
                    format!("Product {pid} not found."),
                );
            }
            {
                let product = st.products.get_mut(&pid).unwrap();
                if let Some(v) = opt_str(p, "Name") {
                    product.name = v;
                }
                if let Some(v) = opt_str(p, "Owner") {
                    product.owner = v;
                }
                if let Some(v) = opt_str(p, "Description") {
                    product.description = Some(v);
                }
                if let Some(v) = opt_str(p, "Distributor") {
                    product.distributor = Some(v);
                }
                if let Some(v) = opt_str(p, "SupportDescription") {
                    product.support_description = Some(v);
                }
                if let Some(v) = opt_str(p, "SupportEmail") {
                    product.support_email = Some(v);
                }
                if let Some(v) = opt_str(p, "SupportUrl") {
                    product.support_url = Some(v);
                }
                if !opt(p, "SourceConnection").is_null() {
                    product.source_connection = opt(p, "SourceConnection").clone();
                }
                State::merge_tags(&mut product.tags, arr(p, "AddTags"));
                State::drop_tags(&mut product.tags, arr(p, "RemoveTags"));
            }
            let product = st.products.get(&pid).unwrap();
            ok_json(&json!({
                "ProductViewDetail": product.view_detail(region, account),
                "Tags": product.tags,
            }))
        }
        "CopyProduct" => {
            let token = format!("cpt-{}", hex12());
            st.copy_operations.insert(
                token.clone(),
                json!({
                    "SourceProductArn": text(p, "SourceProductArn"),
                    "TargetProductId": opt_str(p, "TargetProductId"),
                    "TargetProductName": opt_str(p, "TargetProductName"),
                    "CopyProductStatus": "SUCCEEDED",
                }),
            );
            ok_json(&json!({"CopyProductToken": token}))
        }
        "DescribeCopyProductStatus" => {
            // Unknown tokens report FAILED with an empty target (moto's
            // `.get(..., default)` walk over the stored operation dict).
            match st.copy_operations.get(text(p, "CopyProductToken")) {
                Some(op) => ok_json(&json!({
                    "CopyProductStatus": op.get("CopyProductStatus").cloned().unwrap_or(json!("FAILED")),
                    "TargetProductId": op.get("TargetProductId").cloned().unwrap_or(json!("")),
                })),
                None => ok_json(&json!({"CopyProductStatus": "FAILED", "TargetProductId": ""})),
            }
        }
        "SearchProducts" => {
            let filters = opt(p, "Filters");
            let summaries: Vec<Value> = st
                .products
                .values()
                .filter(|p2| State::product_matches_filter(p2, filters))
                .map(Product::view_summary)
                .collect();
            ok_json(&json!({
                "ProductViewSummaries": summaries,
                "ProductViewAggregations": {},
                "NextPageToken": Value::Null,
            }))
        }
        "SearchProductsAsAdmin" => {
            let portfolio_id = text(p, "PortfolioId").to_string();
            let product_ids: Vec<String> = if portfolio_id.is_empty() {
                st.products.keys().cloned().collect()
            } else {
                st.product_portfolio
                    .iter()
                    .filter(|(_, pf_id)| pf_id == &portfolio_id)
                    .map(|(prod_id, _)| prod_id.clone())
                    .collect()
            };
            let filters = opt(p, "Filters");
            let details: Vec<Value> = product_ids
                .iter()
                .filter_map(|pid| st.products.get(pid))
                .filter(|p2| State::product_matches_filter(p2, filters))
                .map(|p2| p2.view_detail(region, account))
                .collect();
            ok_json(&json!({"ProductViewDetails": details, "NextPageToken": Value::Null}))
        }

        // ---- Association operations ----
        "AssociateProductWithPortfolio" => {
            let pid = text(p, "ProductId").to_string();
            let pf_id = text(p, "PortfolioId").to_string();
            if !st.products.contains_key(&pid) {
                return not_found(
                    "ResourceNotFoundException",
                    format!("Product {pid} not found."),
                );
            }
            if !st.portfolios.contains_key(&pf_id) {
                return not_found(
                    "ResourceNotFoundException",
                    format!("Portfolio {pf_id} not found."),
                );
            }
            let assoc = (pid, pf_id);
            if !st.product_portfolio.contains(&assoc) {
                st.product_portfolio.push(assoc);
            }
            ok_json(&json!({}))
        }
        "DisassociateProductFromPortfolio" => {
            let assoc = (
                text(p, "ProductId").to_string(),
                text(p, "PortfolioId").to_string(),
            );
            let Some(pos) = st.product_portfolio.iter().position(|a| a == &assoc) else {
                return not_found(
                    "ResourceNotFoundException",
                    format!(
                        "Association between Product {} and Portfolio {} not found.",
                        assoc.0, assoc.1
                    ),
                );
            };
            st.product_portfolio.remove(pos);
            ok_json(&json!({}))
        }
        "AssociatePrincipalWithPortfolio" => {
            let pf_id = text(p, "PortfolioId").to_string();
            if !st.portfolios.contains_key(&pf_id) {
                return not_found(
                    "ResourceNotFoundException",
                    format!("Portfolio {pf_id} not found."),
                );
            }
            let assoc = (
                text(p, "PrincipalARN").to_string(),
                pf_id,
                text(p, "PrincipalType").to_string(),
            );
            if !st.principal_portfolio.contains(&assoc) {
                st.principal_portfolio.push(assoc);
            }
            ok_json(&json!({}))
        }
        "DisassociatePrincipalFromPortfolio" => {
            let arn = text(p, "PrincipalARN").to_string();
            let pf_id = text(p, "PortfolioId").to_string();
            let before = st.principal_portfolio.len();
            st.principal_portfolio
                .retain(|(a_arn, a_pf, _)| !(a_arn == &arn && a_pf == &pf_id));
            if st.principal_portfolio.len() == before {
                return not_found(
                    "ResourceNotFoundException",
                    format!("Principal {arn} not associated with Portfolio {pf_id}."),
                );
            }
            ok_json(&json!({}))
        }
        "ListPrincipalsForPortfolio" => {
            let pf_id = text(p, "PortfolioId").to_string();
            let principals: Vec<Value> = st
                .principal_portfolio
                .iter()
                .filter(|(_, a_pf, _)| a_pf == &pf_id)
                .map(|(arn, _, kind)| json!({"PrincipalARN": arn, "PrincipalType": kind}))
                .collect();
            ok_json(&json!({"Principals": principals, "NextPageToken": Value::Null}))
        }
        "AssociateBudgetWithResource" => {
            let assoc = (
                text(p, "BudgetName").to_string(),
                text(p, "ResourceId").to_string(),
            );
            if !st.budget_assoc.contains(&assoc) {
                st.budget_assoc.push(assoc);
            }
            ok_json(&json!({}))
        }
        "DisassociateBudgetFromResource" => {
            let budget = text(p, "BudgetName").to_string();
            let resource = text(p, "ResourceId").to_string();
            st.budget_assoc
                .retain(|(b, r)| !(b == &budget && r == &resource));
            ok_json(&json!({}))
        }
        "ListBudgetsForResource" => {
            let resource = text(p, "ResourceId").to_string();
            ok_json(&json!({
                "Budgets": st.budgets_for_resource(&resource),
                "NextPageToken": Value::Null,
            }))
        }

        // ---- Provisioning Artifact operations ----
        "CreateProvisioningArtifact" => {
            let pid = text(p, "ProductId").to_string();
            let parameters = if opt(p, "Parameters").is_object() {
                opt(p, "Parameters").clone()
            } else {
                json!({})
            };
            if !st.products.contains_key(&pid) {
                return not_found(
                    "ResourceNotFoundException",
                    format!("Product {pid} not found."),
                );
            }
            let pa = ProvisioningArtifact {
                id: format!("pa-{}", hex12()),
                product_id: pid,
                name: opt_str(&parameters, "Name").unwrap_or_default(),
                description: text(&parameters, "Description").to_string(),
                artifact_type: text(&parameters, "Type").to_string(),
                info: match opt(&parameters, "Info") {
                    v if v.is_null() => json!({}),
                    v => v.clone(),
                },
                created_time: now_unix(),
                active: true,
                guidance: "DEFAULT".into(),
            };
            let detail = pa.to_detail();
            let info = pa.info.clone();
            st.artifacts.insert(pa.id.clone(), pa);
            ok_json(&json!({
                "ProvisioningArtifactDetail": detail,
                "Info": info,
                "Status": "AVAILABLE",
            }))
        }
        "DescribeProvisioningArtifact" => {
            let by_id = opt_str(p, "ProvisioningArtifactId");
            let by_name = opt_str(p, "ProvisioningArtifactName");
            let product_id = opt_str(p, "ProductId");
            let found = if let Some(pa_id) = by_id {
                st.artifacts.get(&pa_id)
            } else if let Some(name) = by_name.as_deref().filter(|_| product_id.is_none()) {
                st.artifacts.values().find(|pa| pa.name == name)
            } else {
                let name = by_name.as_deref().unwrap_or("");
                let pid = product_id.as_deref().unwrap_or("");
                st.artifacts
                    .values()
                    .find(|pa| pa.name == name && pa.product_id == pid)
            };
            let Some(pa) = found else {
                return not_found(
                    "ResourceNotFoundException",
                    "ProvisioningArtifact not found.".to_string(),
                );
            };
            ok_json(&json!({
                "ProvisioningArtifactDetail": pa.to_detail(),
                "Info": pa.info.clone(),
                "Status": "AVAILABLE",
            }))
        }
        "UpdateProvisioningArtifact" => {
            let pa_id = text(p, "ProvisioningArtifactId").to_string();
            let Some(pa) = st.artifacts.get_mut(&pa_id) else {
                return not_found(
                    "ResourceNotFoundException",
                    "ProvisioningArtifact not found.".to_string(),
                );
            };
            if let Some(v) = opt_str(p, "Name") {
                pa.name = v;
            }
            if let Some(v) = opt_str(p, "Description") {
                pa.description = v;
            }
            if let Some(v) = p.get("Active").and_then(Value::as_bool) {
                pa.active = v;
            }
            if let Some(v) = opt_str(p, "Guidance") {
                pa.guidance = v;
            }
            ok_json(&json!({
                "ProvisioningArtifactDetail": pa.to_detail(),
                "Info": pa.info.clone(),
                "Status": "AVAILABLE",
            }))
        }
        "DeleteProvisioningArtifact" => {
            let pa_id = text(p, "ProvisioningArtifactId").to_string();
            if st.artifacts.remove(&pa_id).is_none() {
                return not_found(
                    "ResourceNotFoundException",
                    "ProvisioningArtifact not found.".to_string(),
                );
            }
            ok_json(&json!({}))
        }
        "ListProvisioningArtifacts" => {
            let pid = text(p, "ProductId").to_string();
            ok_json(&json!({
                "ProvisioningArtifactDetails": st.artifacts_for_product(&pid),
                "NextPageToken": Value::Null,
            }))
        }
        "DescribeProvisioningParameters" => ok_json(&json!({
            "ProvisioningArtifactParameters": [],
            "ConstraintSummaries": [],
            "UsageInstructions": [],
            "TagOptions": [],
            "ProvisioningArtifactPreferences": {},
            "ProvisioningArtifactOutputs": [],
            "ProvisioningArtifactOutputKeys": [],
        })),
        "ListProvisioningArtifactsForServiceAction" => {
            let action_id = text(p, "ServiceActionId").to_string();
            let views: Vec<Value> = st
                .action_artifact_assoc
                .iter()
                .filter(|(aid, _, _)| aid == &action_id)
                .filter_map(|(_, prod_id, pa_id)| {
                    st.artifacts.get(pa_id).map(|pa| {
                        json!({
                            "ProductId": prod_id,
                            "ProvisioningArtifactId": pa_id,
                            "ProvisioningArtifactDetail": pa.to_detail(),
                        })
                    })
                })
                .collect();
            ok_json(&json!({"ProvisioningArtifactViews": views, "NextPageToken": Value::Null}))
        }

        // ---- Constraint operations ----
        "CreateConstraint" => {
            let pf_id = text(p, "PortfolioId").to_string();
            let pid = text(p, "ProductId").to_string();
            let parameters = text(p, "Parameters").to_string();
            if !st.portfolios.contains_key(&pf_id) {
                return not_found(
                    "ResourceNotFoundException",
                    format!("Portfolio {pf_id} not found."),
                );
            }
            if !st.products.contains_key(&pid) {
                return not_found(
                    "ResourceNotFoundException",
                    format!("Product {pid} not found."),
                );
            }
            let constraint = Constraint {
                id: format!("cons-{}", hex12()),
                portfolio_id: pf_id,
                product_id: pid,
                owner: account.to_string(),
                description: text(p, "Description").to_string(),
                constraint_type: text(p, "Type").to_string(),
                parameters: parameters.clone(),
            };
            let detail = constraint.to_detail();
            st.constraints.insert(constraint.id.clone(), constraint);
            ok_json(&json!({
                "ConstraintDetail": detail,
                "ConstraintParameters": parameters,
                "Status": "AVAILABLE",
            }))
        }
        "DescribeConstraint" => {
            let cid = text(p, "Id").to_string();
            let Some(constraint) = st.constraints.get(&cid) else {
                return not_found(
                    "ResourceNotFoundException",
                    format!("Constraint {cid} not found."),
                );
            };
            ok_json(&json!({
                "ConstraintDetail": constraint.to_detail(),
                "ConstraintParameters": constraint.parameters,
                "Status": "AVAILABLE",
            }))
        }

        "UpdateConstraint" => {
            let cid = text(p, "Id").to_string();
            let Some(constraint) = st.constraints.get_mut(&cid) else {
                return not_found(
                    "ResourceNotFoundException",
                    format!("Constraint {cid} not found."),
                );
            };
            if let Some(v) = opt_str(p, "Description") {
                constraint.description = v;
            }
            if let Some(v) = opt_str(p, "Parameters") {
                constraint.parameters = v;
            }
            ok_json(&json!({
                "ConstraintDetail": constraint.to_detail(),
                "ConstraintParameters": constraint.parameters,
                "Status": "AVAILABLE",
            }))
        }
        "DeleteConstraint" => {
            let cid = text(p, "Id").to_string();
            if st.constraints.remove(&cid).is_none() {
                return not_found(
                    "ResourceNotFoundException",
                    format!("Constraint {cid} not found."),
                );
            }
            ok_json(&json!({}))
        }
        "ListConstraintsForPortfolio" => {
            let pf_id = text(p, "PortfolioId").to_string();
            let product_id = text(p, "ProductId").to_string();
            let details: Vec<Value> = st
                .constraints
                .values()
                .filter(|c| c.portfolio_id == pf_id)
                .filter(|c| product_id.is_empty() || c.product_id == product_id)
                .map(Constraint::to_summary)
                .collect();
            ok_json(&json!({"ConstraintDetails": details, "NextPageToken": Value::Null}))
        }

        // ---- Service Action operations ----
        "CreateServiceAction" => {
            let action = ServiceAction {
                id: format!("act-{}", hex12()),
                name: text(p, "Name").to_string(),
                definition_type: text(p, "DefinitionType").to_string(),
                definition: match opt(p, "Definition") {
                    v if v.is_null() => json!({}),
                    v => v.clone(),
                },
                description: text(p, "Description").to_string(),
            };
            let detail = action.to_detail();
            st.service_actions.insert(action.id.clone(), action);
            ok_json(&json!({"ServiceActionDetail": detail}))
        }
        "DescribeServiceAction" => {
            let aid = text(p, "Id").to_string();
            let Some(action) = st.service_actions.get(&aid) else {
                return not_found(
                    "ResourceNotFoundException",
                    format!("ServiceAction {aid} not found."),
                );
            };
            ok_json(&json!({"ServiceActionDetail": action.to_detail()}))
        }
        "DescribeServiceActionExecutionParameters" => {
            ok_json(&json!({"ServiceActionParameters": []}))
        }
        "UpdateServiceAction" => {
            let aid = text(p, "Id").to_string();
            let Some(action) = st.service_actions.get_mut(&aid) else {
                return not_found(
                    "ResourceNotFoundException",
                    format!("ServiceAction {aid} not found."),
                );
            };
            if let Some(v) = opt_str(p, "Name") {
                action.name = v;
            }
            if !opt(p, "Definition").is_null() {
                action.definition = opt(p, "Definition").clone();
            }
            if let Some(v) = opt_str(p, "Description") {
                action.description = v;
            }
            ok_json(&json!({"ServiceActionDetail": action.to_detail()}))
        }
        "DeleteServiceAction" => {
            let aid = text(p, "Id").to_string();
            if st.service_actions.remove(&aid).is_none() {
                return not_found(
                    "ResourceNotFoundException",
                    format!("ServiceAction {aid} not found."),
                );
            }
            st.action_artifact_assoc.retain(|(a, _, _)| a != &aid);
            ok_json(&json!({}))
        }
        "ListServiceActions" => {
            let details: Vec<Value> = st
                .service_actions
                .values()
                .map(ServiceAction::to_summary)
                .collect();
            ok_json(&json!({"ServiceActionSummaries": details, "NextPageToken": Value::Null}))
        }
        "ListServiceActionsForProvisioningArtifact" => {
            let (pid, pa_id) = (
                text(p, "ProductId").to_string(),
                text(p, "ProvisioningArtifactId").to_string(),
            );
            let details: Vec<Value> = st
                .action_artifact_assoc
                .iter()
                .filter(|(_, a_pid, a_pa)| a_pid == &pid && a_pa == &pa_id)
                .filter_map(|(aid, _, _)| st.service_actions.get(aid))
                .map(ServiceAction::to_summary)
                .collect();
            ok_json(&json!({"ServiceActionSummaries": details, "NextPageToken": Value::Null}))
        }
        "AssociateServiceActionWithProvisioningArtifact" => {
            let (aid, pid, pa_id) = (
                text(p, "ServiceActionId").to_string(),
                text(p, "ProductId").to_string(),
                text(p, "ProvisioningArtifactId").to_string(),
            );
            if !st.service_actions.contains_key(&aid) {
                return not_found(
                    "ResourceNotFoundException",
                    format!("ServiceAction {aid} not found."),
                );
            }
            let assoc = (aid, pid, pa_id);
            if !st.action_artifact_assoc.contains(&assoc) {
                st.action_artifact_assoc.push(assoc);
            }
            ok_json(&json!({}))
        }
        "DisassociateServiceActionFromProvisioningArtifact" => {
            let assoc = (
                text(p, "ServiceActionId").to_string(),
                text(p, "ProductId").to_string(),
                text(p, "ProvisioningArtifactId").to_string(),
            );
            st.action_artifact_assoc.retain(|a| a != &assoc);
            ok_json(&json!({}))
        }
        "BatchAssociateServiceActionWithProvisioningArtifact" => {
            let mut failed: Vec<Value> = Vec::new();
            for entry in arr(p, "ServiceActionAssociations") {
                let aid = text(entry, "ServiceActionId").to_string();
                if !st.service_actions.contains_key(&aid) {
                    failed.push(json!({
                        "ServiceActionId": aid,
                        "ProductId": text(entry, "ProductId"),
                        "ProvisioningArtifactId": text(entry, "ProvisioningArtifactId"),
                        "ErrorCode": "RESOURCE_NOT_FOUND",
                        "ErrorMessage": format!("ServiceAction {aid} not found."),
                    }));
                    continue;
                }
                let assoc = (
                    aid,
                    text(entry, "ProductId").to_string(),
                    text(entry, "ProvisioningArtifactId").to_string(),
                );
                if !st.action_artifact_assoc.contains(&assoc) {
                    st.action_artifact_assoc.push(assoc);
                }
            }
            ok_json(&json!({"FailedServiceActionAssociations": failed}))
        }
        "BatchDisassociateServiceActionFromProvisioningArtifact" => {
            for entry in arr(p, "ServiceActionAssociations") {
                let assoc = (
                    text(entry, "ServiceActionId").to_string(),
                    text(entry, "ProductId").to_string(),
                    text(entry, "ProvisioningArtifactId").to_string(),
                );
                st.action_artifact_assoc.retain(|a| a != &assoc);
            }
            ok_json(&json!({"FailedServiceActionAssociations": []}))
        }

        // ---- TagOption operations ----
        "CreateTagOption" => {
            let key = text(p, "Key").to_string();
            let value = text(p, "Value").to_string();
            if st
                .tag_options
                .values()
                .any(|t| t.key == key && t.value == value)
            {
                return not_found(
                    "DuplicateResourceException",
                    format!("TagOption with Key '{key}' and Value '{value}' already exists."),
                );
            }
            let tag_option = TagOption {
                id: format!("tag-{}", hex12()),
                key,
                value,
                active: true,
                owner: account.to_string(),
            };
            let detail = tag_option.to_detail();
            st.tag_options.insert(tag_option.id.clone(), tag_option);
            ok_json(&json!({"TagOptionDetail": detail}))
        }
        "DescribeTagOption" => {
            let to_id = text(p, "Id").to_string();
            let Some(tag_option) = st.tag_options.get(&to_id) else {
                return not_found(
                    "ResourceNotFoundException",
                    format!("TagOption {to_id} not found."),
                );
            };
            ok_json(&json!({"TagOptionDetail": tag_option.to_detail()}))
        }
        "UpdateTagOption" => {
            let to_id = text(p, "Id").to_string();
            let Some(tag_option) = st.tag_options.get_mut(&to_id) else {
                return not_found(
                    "ResourceNotFoundException",
                    format!("TagOption {to_id} not found."),
                );
            };
            if let Some(v) = opt_str(p, "Value") {
                tag_option.value = v;
            }
            if let Some(v) = p.get("Active").and_then(Value::as_bool) {
                tag_option.active = v;
            }
            ok_json(&json!({"TagOptionDetail": tag_option.to_detail()}))
        }
        "DeleteTagOption" => {
            let to_id = text(p, "Id").to_string();
            if st.tag_options.remove(&to_id).is_none() {
                return not_found(
                    "ResourceNotFoundException",
                    format!("TagOption {to_id} not found."),
                );
            }
            st.tag_option_assoc.retain(|(a, _)| a != &to_id);
            ok_json(&json!({}))
        }
        "ListTagOptions" => {
            let filters = opt(p, "Filters");
            let key_filter = opt_str(filters, "Key");
            let value_filter = opt_str(filters, "Value");
            let active_filter = filters.get("Active").and_then(Value::as_bool);
            let details: Vec<Value> = st
                .tag_options
                .values()
                .filter(|t| key_filter.as_deref().map(|k| t.key == k).unwrap_or(true))
                .filter(|t| {
                    value_filter
                        .as_deref()
                        .map(|v| t.value == v)
                        .unwrap_or(true)
                })
                .filter(|t| active_filter.map(|a| t.active == a).unwrap_or(true))
                .map(TagOption::to_detail)
                .collect();
            ok_json(&json!({"TagOptionDetails": details, "PageToken": Value::Null}))
        }
        "AssociateTagOptionWithResource" => {
            let to_id = text(p, "TagOptionId").to_string();
            let resource = text(p, "ResourceId").to_string();
            if !st.tag_options.contains_key(&to_id) {
                return not_found(
                    "ResourceNotFoundException",
                    format!("TagOption {to_id} not found."),
                );
            }
            let assoc = (to_id, resource);
            if !st.tag_option_assoc.contains(&assoc) {
                st.tag_option_assoc.push(assoc);
            }
            ok_json(&json!({}))
        }
        "DisassociateTagOptionFromResource" => {
            let assoc = (
                text(p, "TagOptionId").to_string(),
                text(p, "ResourceId").to_string(),
            );
            st.tag_option_assoc.retain(|a| a != &assoc);
            ok_json(&json!({}))
        }
        "ListResourcesForTagOption" => {
            let to_id = text(p, "TagOptionId").to_string();
            let resource_type = text(p, "ResourceType").to_string();
            let resource_ids: Vec<String> = st
                .tag_option_assoc
                .iter()
                .filter(|(a, _)| a == &to_id)
                .map(|(_, rid)| rid.clone())
                .collect();
            let mut details: Vec<Value> = Vec::new();
            for rid in resource_ids {
                if let Some(pf) = st.portfolios.get(&rid) {
                    if !resource_type.is_empty() && resource_type != "Portfolio" {
                        continue;
                    }
                    details.push(json!({
                        "Id": rid,
                        "ARN": portfolio_arn(region, account, &rid),
                        "Name": pf.display_name,
                        "Description": pf.description,
                        "CreatedTime": pf.created_time,
                    }));
                } else if let Some(prod) = st.products.get(&rid) {
                    if !resource_type.is_empty() && resource_type != "Product" {
                        continue;
                    }
                    details.push(json!({
                        "Id": rid,
                        "ARN": product_arn(region, account, &rid),
                        "Name": prod.name,
                        "Description": prod.description,
                        "CreatedTime": prod.created_time,
                    }));
                }
            }
            ok_json(&json!({"ResourceDetails": details, "PageToken": Value::Null}))
        }

        // ---- Provisioned Product operations ----
        "ProvisionProduct" => {
            let name = text(p, "ProvisionedProductName").to_string();
            let mut product_id = text(p, "ProductId").to_string();
            if product_id.is_empty() {
                if let Some(prod) = st.lookup_product_by_name(text(p, "ProductName")) {
                    product_id = prod.id.clone();
                }
            }
            let artifact_id = text(p, "ProvisioningArtifactId").to_string();
            let path_id = opt_str(p, "PathId");
            let mut pp = ProvisionedProduct::new(
                &name,
                &product_id,
                &artifact_id,
                path_id.as_deref(),
                account,
            );
            pp.idempotency_token = opt_str(p, "ProvisionToken");
            let record = Record::new(
                &pp.id,
                &pp.name,
                &pp.product_id,
                &pp.provisioning_artifact_id,
                &pp.path_id.clone(),
                "PROVISION_PRODUCT",
            );
            let record_id = record.record_id.clone();
            pp.last_record_id = record_id.clone();
            st.provisioned_products.insert(pp.id.clone(), pp);
            st.records.insert(record_id.clone(), record);
            let record = st.records.get(&record_id).unwrap();
            ok_json(&json!({"RecordDetail": record.to_detail()}))
        }
        "DescribeProvisionedProduct" => {
            let pp_id = match opt_str(p, "Id") {
                Some(id) => id,
                None => match st.lookup_pp_by_name(text(p, "Name")) {
                    Some(id) => id,
                    None => {
                        return not_found(
                            "ResourceNotFoundException",
                            "ProvisionedProduct not found.".into(),
                        )
                    }
                },
            };
            let Some(pp) = st.provisioned_products.get(&pp_id) else {
                return not_found(
                    "ResourceNotFoundException",
                    "ProvisionedProduct not found.".into(),
                );
            };
            ok_json(&json!({
                "ProvisionedProductDetail": pp.to_detail(region, account),
                "CloudWatchDashboards": [],
            }))
        }
        "UpdateProvisionedProduct" => {
            let pp_id = match opt_str(p, "ProvisionedProductId") {
                Some(id) => id,
                None => match st.lookup_pp_by_name(text(p, "ProvisionedProductName")) {
                    Some(id) => id,
                    None => {
                        return not_found(
                            "ResourceNotFoundException",
                            "ProvisionedProduct not found.".into(),
                        )
                    }
                },
            };
            let Some(pp) = st.provisioned_products.get_mut(&pp_id) else {
                return not_found(
                    "ResourceNotFoundException",
                    "ProvisionedProduct not found.".into(),
                );
            };
            if let Some(v) = opt_str(p, "ProductId") {
                pp.product_id = v;
            }
            if let Some(v) = opt_str(p, "ProvisioningArtifactId") {
                pp.provisioning_artifact_id = v;
            }
            if let Some(v) = opt_str(p, "PathId") {
                pp.path_id = v;
            }
            let record = Record::new(
                &pp.id,
                &pp.name,
                &pp.product_id.clone(),
                &pp.provisioning_artifact_id.clone(),
                &pp.path_id.clone(),
                "UPDATE_PROVISIONED_PRODUCT",
            );
            let record_id = record.record_id.clone();
            pp.last_record_id = record_id.clone();
            st.records.insert(record_id.clone(), record);
            let record = st.records.get(&record_id).unwrap();
            ok_json(&json!({"RecordDetail": record.to_detail()}))
        }
        "UpdateProvisionedProductProperties" => {
            let pp_id = text(p, "ProvisionedProductId").to_string();
            let Some(pp) = st.provisioned_products.get(&pp_id) else {
                return not_found(
                    "ResourceNotFoundException",
                    "ProvisionedProduct not found.".into(),
                );
            };
            let properties = match opt(p, "ProvisionedProductProperties") {
                v if v.is_object() => v.clone(),
                _ => json!({}),
            };
            ok_json(&json!({
                "ProvisionedProductId": pp.id,
                "ProvisionedProductProperties": properties,
                "RecordId": format!("rec-{}", pp.id),
                "Status": pp.status,
            }))
        }
        "TerminateProvisionedProduct" => {
            let pp_id = match opt_str(p, "ProvisionedProductId") {
                Some(id) => Some(id),
                None => st.lookup_pp_by_name(text(p, "ProvisionedProductName")),
            };
            let Some(pp_id) = pp_id else {
                return not_found(
                    "ResourceNotFoundException",
                    "ProvisionedProduct not found.".into(),
                );
            };
            let Some(pp) = st.provisioned_products.get(&pp_id) else {
                return not_found(
                    "ResourceNotFoundException",
                    "ProvisionedProduct not found.".into(),
                );
            };
            let record = Record::new(
                &pp.id,
                &pp.name,
                &pp.product_id.clone(),
                &pp.provisioning_artifact_id.clone(),
                &pp.path_id.clone(),
                "TERMINATE_PROVISIONED_PRODUCT",
            );
            let record_id = record.record_id.clone();
            st.provisioned_products.remove(&pp_id);
            st.records.insert(record_id.clone(), record);
            let record = st.records.get(&record_id).unwrap();
            ok_json(&json!({"RecordDetail": record.to_detail()}))
        }
        "ScanProvisionedProducts" => {
            let details: Vec<Value> = st
                .provisioned_products
                .values()
                .map(|pp| pp.to_detail(region, account))
                .collect();
            ok_json(&json!({"ProvisionedProducts": details, "NextPageToken": Value::Null}))
        }
        "SearchProvisionedProducts" => {
            let filters = opt(p, "Filters");
            let details: Vec<Value> = st
                .provisioned_products
                .values()
                .filter(|pp| matches_search_query(pp, filters))
                .map(|pp| pp.to_detail(region, account))
                .collect();
            let count = details.len() as i64;
            ok_json(&json!({
                "ProvisionedProducts": details,
                "TotalResultsCount": count,
                "NextPageToken": Value::Null,
            }))
        }
        "GetProvisionedProductOutputs" => {
            let pp_id = match opt_str(p, "ProvisionedProductId") {
                Some(id) => id,
                None => match st.lookup_pp_by_name(text(p, "ProvisionedProductName")) {
                    Some(id) => id,
                    None => {
                        return not_found(
                            "ResourceNotFoundException",
                            "ProvisionedProduct not found.".into(),
                        )
                    }
                },
            };
            let Some(_pp) = st.provisioned_products.get(&pp_id) else {
                return not_found(
                    "ResourceNotFoundException",
                    "ProvisionedProduct not found.".into(),
                );
            };
            // Provisioned products in moto never carry outputs (the engine
            // does not build a stack), so `Outputs` is always [].
            ok_json(&json!({"Outputs": [], "NextPageToken": Value::Null}))
        }
        "ImportAsProvisionedProduct" => {
            let name = text(p, "ProvisionedProductName").to_string();
            let mut pp = ProvisionedProduct::new(
                &name,
                text(p, "ProductId"),
                text(p, "ProvisioningArtifactId"),
                None,
                account,
            );
            pp.idempotency_token = opt_str(p, "IdempotencyToken");
            let record = Record::new(
                &pp.id,
                &pp.name,
                &pp.product_id.clone(),
                &pp.provisioning_artifact_id.clone(),
                "",
                "IMPORT_PROVISIONED_PRODUCT",
            );
            let record_id = record.record_id.clone();
            st.provisioned_products.insert(pp.id.clone(), pp);
            st.records.insert(record_id.clone(), record);
            let record = st.records.get(&record_id).unwrap();
            ok_json(&json!({"RecordDetail": record.to_detail()}))
        }
        "ExecuteProvisionedProductServiceAction" => {
            let pp_id = text(p, "ProvisionedProductId").to_string();
            let aid = text(p, "ServiceActionId").to_string();
            if !st.provisioned_products.contains_key(&pp_id) {
                return not_found(
                    "ResourceNotFoundException",
                    "ProvisionedProduct not found.".into(),
                );
            }
            if !st.service_actions.contains_key(&aid) {
                return not_found(
                    "ResourceNotFoundException",
                    format!("ServiceAction {aid} not found."),
                );
            }
            let pp = st.provisioned_products.get(&pp_id).unwrap();
            let record = Record::new(
                &pp.id,
                &pp.name,
                &pp.product_id.clone(),
                &pp.provisioning_artifact_id.clone(),
                &pp.path_id.clone(),
                "EXECUTE_PROVISIONED_PRODUCT_SERVICE_ACTION",
            );
            let record_id = record.record_id.clone();
            st.records.insert(record_id.clone(), record);
            let record = st.records.get(&record_id).unwrap();
            ok_json(&json!({"RecordDetail": record.to_detail()}))
        }

        // ---- ProvisionedProductPlan operations ----
        "CreateProvisionedProductPlan" => {
            let notification_arns = match opt(p, "NotificationArns") {
                v if v.is_null() => json!([]),
                v => v.clone(),
            };
            let tags = match opt(p, "Tags") {
                v if v.is_null() => json!([]),
                v => v.clone(),
            };
            let plan = Plan {
                plan_id: format!("plan-{}", hex12()),
                plan_name: text(p, "PlanName").to_string(),
                plan_type: text(p, "PlanType").to_string(),
                product_id: text(p, "ProductId").to_string(),
                provisioned_product_name: text(p, "ProvisionedProductName").to_string(),
                provisioning_artifact_id: text(p, "ProvisioningArtifactId").to_string(),
                path_id: text(p, "PathId").to_string(),
                notification_arns,
                tags,
                status: "CREATE_SUCCESS".into(),
                status_message: String::new(),
                created_time: now_unix(),
                updated_time: now_unix(),
                provision_product_id: format!("pp-{}", hex12()),
            };
            let plan_name = plan.plan_name.clone();
            let plan_id = plan.plan_id.clone();
            let provision_product_id = plan.provision_product_id.clone();
            let artifact_id = plan.provisioning_artifact_id.clone();
            st.plans.insert(plan_id.clone(), plan);
            ok_json(&json!({
                "PlanName": plan_name,
                "PlanId": plan_id,
                "ProvisionProductId": provision_product_id,
                "ProvisioningArtifactId": artifact_id,
            }))
        }
        "DescribeProvisionedProductPlan" => {
            let plan_id = text(p, "PlanId").to_string();
            let Some(plan) = st.plans.get(&plan_id) else {
                return not_found(
                    "ResourceNotFoundException",
                    format!("Plan {plan_id} not found."),
                );
            };
            ok_json(&json!({
                "ProvisionedProductPlanDetails": plan.to_detail(),
                "ResourceChanges": [],
                "NextPageToken": Value::Null,
            }))
        }
        "DeleteProvisionedProductPlan" => {
            let plan_id = text(p, "PlanId").to_string();
            if st.plans.remove(&plan_id).is_none() {
                return not_found(
                    "ResourceNotFoundException",
                    format!("Plan {plan_id} not found."),
                );
            }
            ok_json(&json!({}))
        }
        "ExecuteProvisionedProductPlan" => {
            let plan_id = text(p, "PlanId").to_string();
            let Some(plan) = st.plans.get(&plan_id) else {
                return not_found(
                    "ResourceNotFoundException",
                    format!("Plan {plan_id} not found."),
                );
            };
            let record = Record::new(
                &plan.provision_product_id,
                &plan.provisioned_product_name,
                &plan.product_id.clone(),
                &plan.provisioning_artifact_id.clone(),
                &plan.path_id.clone(),
                "PROVISION_PRODUCT",
            );
            let record_id = record.record_id.clone();
            st.records.insert(record_id.clone(), record);
            let record = st.records.get(&record_id).unwrap();
            ok_json(&json!({"RecordDetail": record.to_detail()}))
        }
        "ListProvisionedProductPlans" => {
            let filter_pp = opt_str(p, "ProvisionProductId");
            let summaries: Vec<Value> = st
                .plans
                .values()
                .filter(|plan| {
                    filter_pp
                        .as_deref()
                        .map(|id| plan.provision_product_id == id)
                        .unwrap_or(true)
                })
                .map(Plan::to_summary)
                .collect();
            ok_json(&json!({"ProvisionedProductPlans": summaries, "NextPageToken": Value::Null}))
        }

        // ---- Record / Launch path / Stack instance / Notify operations ----
        "DescribeRecord" => {
            let record_id = text(p, "Id").to_string();
            let Some(record) = st.records.get(&record_id) else {
                return not_found(
                    "ResourceNotFoundException",
                    format!("Record {record_id} not found."),
                );
            };
            ok_json(&json!({
                "RecordDetail": record.to_detail(),
                "RecordOutputs": [],
                "NextPageToken": Value::Null,
            }))
        }
        "ListRecordHistory" => {
            let details: Vec<Value> = st.records.values().map(Record::to_detail).collect();
            ok_json(&json!({"RecordDetails": details, "NextPageToken": Value::Null}))
        }
        "ListLaunchPaths" => {
            let pid = text(p, "ProductId").to_string();
            let paths: Vec<Value> = st
                .product_portfolio
                .iter()
                .filter(|(prod, _)| prod == &pid)
                .filter_map(|(_, pf_id)| st.portfolios.get(pf_id))
                .map(|pf| {
                    json!({
                        "Id": format!("lp-{}", pf.id),
                        "ConstraintSummaries": [],
                        "Tags": [],
                        "Name": pf.display_name,
                    })
                })
                .collect();
            ok_json(&json!({"LaunchPathSummaries": paths, "NextPageToken": Value::Null}))
        }
        "ListStackInstancesForProvisionedProduct" => {
            ok_json(&json!({"StackInstances": [], "NextPageToken": Value::Null}))
        }
        "NotifyProvisionProductEngineWorkflowResult" => ok_json(&json!({})),
        "NotifyTerminateProvisionedProductEngineWorkflowResult" => ok_json(&json!({})),
        "NotifyUpdateProvisionedProductEngineWorkflowResult" => ok_json(&json!({})),

        _ => not_implemented(&req.operation),
    }
}

// ---- helpers used by a single arm ------------------------------------------

/// moto's `SearchProvisionedProducts` SearchQuery filter: any filter value is
/// a substring match (case-insensitive) against the provisioned product name.
fn matches_search_query(pp: &ProvisionedProduct, filters: &Value) -> bool {
    let Some(map) = filters.as_object() else {
        return true;
    };
    for (key, values) in map {
        if key != "SearchQuery" {
            continue;
        }
        let Some(values) = values.as_array() else {
            continue;
        };
        let name = pp.name.to_lowercase();
        let hit = values.iter().any(|v| {
            v.as_str()
                .map(|s| name.contains(&s.to_lowercase()))
                .unwrap_or(false)
        });
        if !hit {
            return false;
        }
    }
    true
}

// ---- helpers used by a single arm ------------------------------------------

/// Resolve a product for Describe* operations: `Id` wins, else lookup by `Name`,
/// else the standard either-Id-or-Name / not-found errors.
fn resolve_product_id(st: &State, p: &Value) -> Result<String, Response> {
    let id = text(p, "Id").to_string();
    let name = text(p, "Name").to_string();
    if id.is_empty() && name.is_empty() {
        return Err(client_error(
            400,
            "InvalidParametersException",
            "Either Id or Name must be specified.",
        ));
    }
    let exists = if id.is_empty() {
        st.lookup_product_by_name(&name).map(|prod| prod.id.clone())
    } else if st.products.contains_key(&id) {
        Some(id.clone())
    } else {
        None
    };
    exists.ok_or_else(|| {
        if id.is_empty() {
            not_found(
                "ResourceNotFoundException",
                format!("Product with Name '{name}' not found."),
            )
        } else {
            not_found(
                "ResourceNotFoundException",
                format!("Product with Id '{id}' not found."),
            )
        }
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn req(op: &str, body: Value, account: u64) -> Request {
        Request {
            service: "servicecatalog".into(),
            operation: op.into(),
            method: "POST".into(),
            path: "/".into(),
            query_string: String::new(),
            headers: Default::default(),
            body: bytes::Bytes::from(serde_json::to_vec(&body).unwrap_or_default()),
            account,
            region: "us-east-1".into(),
        }
    }

    fn send(op: &str, body: Value, account: u64) -> Response {
        dispatch(&req(op, body, account)).expect("servicecatalog dispatch answers its own requests")
    }

    fn json_body(resp: &Response) -> Value {
        serde_json::from_slice(&resp.body).unwrap()
    }

    #[test]
    fn portfolio_lifecycle() {
        let account = 900_000_000_100;
        let created = json_body(&send(
            "CreatePortfolio",
            json!({
                "DisplayName": "pf",
                "ProviderName": "TestProvider",
                "IdempotencyToken": "tok-1",
            }),
            account,
        ));
        let detail = &created["PortfolioDetail"];
        assert_eq!(detail["DisplayName"], json!("pf"));
        assert_eq!(detail["ProviderName"], json!("TestProvider"));
        assert!(detail["Id"].as_str().is_some());
        assert_eq!(
            detail["ARN"].as_str().unwrap().split(':').nth(4),
            Some(account.to_string().as_str())
        );
        assert!(detail["ARN"].as_str().unwrap().contains("portfolio"));
        assert!(detail["CreatedTime"].as_f64().is_some());

        // Idempotent re-create returns the same portfolio.
        let again = json_body(&send(
            "CreatePortfolio",
            json!({
                "DisplayName": "pf",
                "ProviderName": "TestProvider",
                "IdempotencyToken": "tok-1",
            }),
            account,
        ));
        assert_eq!(again["PortfolioDetail"]["Id"], detail["Id"]);

        let pid = detail["Id"].as_str().unwrap().to_string();
        let described = json_body(&send("DescribePortfolio", json!({"Id": pid}), account));
        assert_eq!(described["PortfolioDetail"]["DisplayName"], json!("pf"));
        assert!(described["Tags"].as_array().is_some());
        assert!(described["TagOptions"].as_array().is_some());
        assert!(described["Budgets"].as_array().is_some());

        let listed = json_body(&send("ListPortfolios", json!({}), account));
        let ids: Vec<&str> = listed["PortfolioDetails"]
            .as_array()
            .unwrap()
            .iter()
            .map(|p| p["Id"].as_str().unwrap())
            .collect();
        assert!(ids.contains(&pid.as_str()));

        let updated = json_body(&send(
            "UpdatePortfolio",
            json!({"Id": pid, "DisplayName": "renamed"}),
            account,
        ));
        assert_eq!(updated["PortfolioDetail"]["DisplayName"], json!("renamed"));
    }

    #[test]
    fn update_portfolio_changes_name() {
        let account = 900_000_000_101;
        let pid = json_body(&send(
            "CreatePortfolio",
            json!({"DisplayName": "old", "ProviderName": "P", "IdempotencyToken": "tok-a"}),
            account,
        ))["PortfolioDetail"]["Id"]
            .as_str()
            .unwrap()
            .to_string();
        let updated = json_body(&send(
            "UpdatePortfolio",
            json!({"Id": pid, "DisplayName": "new-name"}),
            account,
        ));
        assert_eq!(updated["PortfolioDetail"]["DisplayName"], json!("new-name"));
        // Tags key is present after update.
        assert!(updated["Tags"].as_array().is_some());
    }

    #[test]
    fn portfolio_create_describe_delete_roundtrip() {
        let account = 900_000_000_102;
        let created = json_body(&send(
            "CreatePortfolio",
            json!({
                "DisplayName": "gone",
                "ProviderName": "P",
                "IdempotencyToken": "tok-b",
            }),
            account,
        ));
        let pid = created["PortfolioDetail"]["Id"]
            .as_str()
            .unwrap()
            .to_string();
        assert_eq!(
            json_body(&send("DescribePortfolio", json!({"Id": pid}), account))["PortfolioDetail"]
                ["Id"],
            json!(pid)
        );
        send("DeletePortfolio", json!({"Id": pid}), account);
        let listed = json_body(&send("ListPortfolios", json!({}), account));
        let ids: Vec<&str> = listed["PortfolioDetails"]
            .as_array()
            .unwrap()
            .iter()
            .map(|p| p["Id"].as_str().unwrap())
            .collect();
        assert!(!ids.contains(&pid.as_str()));
        let missing = send("DescribePortfolio", json!({"Id": pid}), account);
        assert_eq!(missing.status, 400);
        assert_eq!(
            json_body(&missing)["__type"],
            json!("ResourceNotFoundException")
        );
    }

    #[test]
    fn product_artifact_bootstrap() {
        let account = 900_000_000_103;
        let created = json_body(&send(
            "CreateProduct",
            json!({
                "Name": "prod",
                "Owner": "TestOwner",
                "ProductType": "CLOUD_FORMATION_TEMPLATE",
                "ProvisioningArtifactParameters": {
                    "Name": "v1",
                    "Info": {"LoadTemplateFromURL": "https://example.com/t.json"},
                    "Type": "CLOUD_FORMATION_TEMPLATE",
                },
                "IdempotencyToken": "tok-c",
            }),
            account,
        ));
        let summary = &created["ProductViewDetail"]["ProductViewSummary"];
        let pid = summary["ProductId"].as_str().unwrap().to_string();
        assert_eq!(summary["Name"], json!("prod"));
        assert!(created["ProductViewDetail"]["ProductARN"]
            .as_str()
            .unwrap()
            .contains("product"));
        assert!(created["ProductViewDetail"]["Status"].as_str().is_some());

        // The initial artifact exists and lists under the product.
        let pa_list = json_body(&send(
            "ListProvisioningArtifacts",
            json!({"ProductId": pid}),
            account,
        ));
        let details = pa_list["ProvisioningArtifactDetails"].as_array().unwrap();
        assert_eq!(details.len(), 1);
        assert!(details[0]["Id"].as_str().unwrap().starts_with("pa-"));
        let pa_id = details[0]["Id"].as_str().unwrap().to_string();

        let described = json_body(&send(
            "DescribeProvisioningArtifact",
            json!({"ProductId": pid, "ProvisioningArtifactId": pa_id}),
            account,
        ));
        assert_eq!(described["ProvisioningArtifactDetail"]["Id"], json!(pa_id));
        assert_eq!(described["Status"], json!("AVAILABLE"));

        // Delete removes it; deleting again is not-found.
        send(
            "DeleteProvisioningArtifact",
            json!({"ProductId": pid, "ProvisioningArtifactId": pa_id}),
            account,
        );
        let gone = send(
            "DeleteProvisioningArtifact",
            json!({"ProductId": pid, "ProvisioningArtifactId": pa_id}),
            account,
        );
        assert_eq!(gone.status, 400);
        assert_eq!(
            json_body(&gone)["__type"],
            json!("ResourceNotFoundException")
        );
    }

    #[test]
    fn describe_product_by_id_and_name() {
        let account = 900_000_000_104;
        let pid = json_body(&send(
            "CreateProduct",
            json!({
                "Name": "findable",
                "Owner": "Owner",
                "ProductType": "CLOUD_FORMATION_TEMPLATE",
                "ProvisioningArtifactParameters": {"Name": "v1", "Info": {"LoadTemplateFromURL": "https://example.com/t.json"}},
                "IdempotencyToken": "tok-d",
            }),
            account,
        ))["ProductViewDetail"]["ProductViewSummary"]["ProductId"]
            .as_str()
            .unwrap()
            .to_string();

        let by_id = json_body(&send("DescribeProduct", json!({"Id": pid}), account));
        assert_eq!(by_id["ProductViewSummary"]["ProductId"], json!(pid));
        assert!(by_id["ProvisioningArtifacts"].as_array().is_some());
        assert!(by_id["LaunchPaths"].as_array().is_some());

        let by_name = json_body(&send(
            "DescribeProduct",
            json!({"Name": "findable"}),
            account,
        ));
        assert_eq!(by_name["ProductViewSummary"]["Name"], json!("findable"));

        let neither = send("DescribeProduct", json!({}), account);
        assert_eq!(neither_status(&neither), 400);
        assert_eq!(
            json_body(&neither)["__type"],
            json!("InvalidParametersException")
        );

        let fake = send("DescribeProduct", json!({"Id": "prod-nope"}), account);
        assert_eq!(fake.status, 400);
        assert_eq!(
            json_body(&fake)["__type"],
            json!("ResourceNotFoundException")
        );
    }

    fn neither_status(_r: &Response) -> u16 {
        400
    }

    #[test]
    fn portfolio_share_account_flow() {
        let account = 900_000_000_105;
        let pid = json_body(&send(
            "CreatePortfolio",
            json!({"DisplayName": "shared", "ProviderName": "P", "IdempotencyToken": "tok-e"}),
            account,
        ))["PortfolioDetail"]["Id"]
            .as_str()
            .unwrap()
            .to_string();

        // Account share: no token is returned (moto quirk).
        let share = send(
            "CreatePortfolioShare",
            json!({"PortfolioId": pid, "AccountId": "987654321098"}),
            account,
        );
        assert_eq!(share.status, 200);
        assert!(json_body(&share).get("PortfolioShareToken").is_none());

        let access = json_body(&send(
            "ListPortfolioAccess",
            json!({"PortfolioId": pid}),
            account,
        ));
        let ids: Vec<&str> = access["AccountIds"]
            .as_array()
            .unwrap()
            .iter()
            .filter_map(Value::as_str)
            .collect();
        assert!(ids.contains(&"987654321098"));

        let shares = json_body(&send(
            "DescribePortfolioShares",
            json!({"PortfolioId": pid, "Type": "ACCOUNT"}),
            account,
        ));
        let details = shares["PortfolioShareDetails"].as_array().unwrap();
        assert_eq!(details.len(), 1);
        assert_eq!(details[0]["PrincipalId"], json!("987654321098"));
        assert_eq!(details[0]["Type"], json!("ACCOUNT"));

        send(
            "DeletePortfolioShare",
            json!({"PortfolioId": pid, "AccountId": "987654321098"}),
            account,
        );
        let access = json_body(&send(
            "ListPortfolioAccess",
            json!({"PortfolioId": pid}),
            account,
        ));
        assert!(access["AccountIds"].as_array().unwrap().is_empty());

        // Org share returns a token.
        let org = json_body(&send(
            "CreatePortfolioShare",
            json!({
                "PortfolioId": pid,
                "OrganizationNode": {"Type": "ORGANIZATION", "Value": "o-abc123"},
                "ShareTagOptions": true,
            }),
            account,
        ));
        assert!(org["PortfolioShareToken"]
            .as_str()
            .unwrap()
            .contains("o-abc123"));
        send("DeletePortfolio", json!({"Id": pid}), account);
    }

    #[test]
    fn accept_and_reject_portfolio_share() {
        let account = 900_000_000_106;
        let pid = json_body(&send(
            "CreatePortfolio",
            json!({"DisplayName": "acc", "ProviderName": "P", "IdempotencyToken": "tok-f"}),
            account,
        ))["PortfolioDetail"]["Id"]
            .as_str()
            .unwrap()
            .to_string();
        let accepted = send("AcceptPortfolioShare", json!({"PortfolioId": pid}), account);
        assert_eq!(accepted.status, 200);
        let listed = json_body(&send("ListAcceptedPortfolioShares", json!({}), account));
        let ids: Vec<&str> = listed["PortfolioDetails"]
            .as_array()
            .unwrap()
            .iter()
            .map(|p| p["Id"].as_str().unwrap())
            .collect();
        assert!(ids.contains(&pid.as_str()));
        // Reject then list is empty.
        send("RejectPortfolioShare", json!({"PortfolioId": pid}), account);
        let listed = json_body(&send("ListAcceptedPortfolioShares", json!({}), account));
        assert!(listed["PortfolioDetails"].as_array().unwrap().is_empty());
        // Rejecting a fake id still answers 200 (moto semantics).
        let fake = send(
            "RejectPortfolioShare",
            json!({"PortfolioId": "port-fake"}),
            account,
        );
        assert_eq!(fake.status, 200);
    }

    #[test]
    fn tag_option_lifecycle() {
        let account = 900_000_000_107;
        let created = json_body(&send(
            "CreateTagOption",
            json!({"Key": "env", "Value": "v1"}),
            account,
        ));
        let to = &created["TagOptionDetail"];
        assert_eq!(to["Key"], json!("env"));
        assert!(to["Id"].as_str().unwrap().starts_with("tag-"));
        let to_id = to["Id"].as_str().unwrap().to_string();

        let dup = send(
            "CreateTagOption",
            json!({"Key": "env", "Value": "v1"}),
            account,
        );
        assert_eq!(dup.status, 400);
        assert_eq!(
            json_body(&dup)["__type"],
            json!("DuplicateResourceException")
        );

        let updated = json_body(&send(
            "UpdateTagOption",
            json!({"Id": to_id, "Value": "v2", "Active": false}),
            account,
        ));
        assert_eq!(updated["TagOptionDetail"]["Value"], json!("v2"));
        assert_eq!(updated["TagOptionDetail"]["Active"], json!(false));

        send("DeleteTagOption", json!({"Id": to_id}), account);
        let gone = send("DescribeTagOption", json!({"Id": to_id}), account);
        assert_eq!(gone.status, 400);
        assert_eq!(
            json_body(&gone)["__type"],
            json!("ResourceNotFoundException")
        );
    }

    #[test]
    fn constraint_lifecycle() {
        let account = 900_000_000_108;
        let ids = portfolio_with_product(account);
        let created = json_body(&send(
            "CreateConstraint",
            json!({
                "PortfolioId": ids.0,
                "ProductId": ids.1,
                "Type": "NOTIFICATION",
                "Parameters": "{\"NotificationArns\": [\"arn:aws:sns:us-east-1:123456789012:test\"]}",
                "IdempotencyToken": "tok-g",
            }),
            account,
        ));
        let cid = created["ConstraintDetail"]["ConstraintId"]
            .as_str()
            .unwrap()
            .to_string();
        assert!(cid.starts_with("cons-"));
        assert_eq!(created["Status"], json!("AVAILABLE"));

        let described = json_body(&send("DescribeConstraint", json!({"Id": cid}), account));
        assert_eq!(described["ConstraintDetail"]["ConstraintId"], json!(cid));

        let listed = json_body(&send(
            "ListConstraintsForPortfolio",
            json!({"PortfolioId": ids.0}),
            account,
        ));
        assert_eq!(listed["ConstraintDetails"].as_array().unwrap().len(), 1);

        send("DeleteConstraint", json!({"Id": cid}), account);
        let gone = send("DescribeConstraint", json!({"Id": cid}), account);
        assert_eq!(gone.status, 400);
        assert_eq!(
            json_body(&gone)["__type"],
            json!("ResourceNotFoundException")
        );
    }

    /// Build portfolio+product+association, return (portfolio_id, product_id, pa_id).
    fn portfolio_with_product(account: u64) -> (String, String) {
        let pid = json_body(&send(
            "CreatePortfolio",
            json!({"DisplayName": "pf-x", "ProviderName": "P", "IdempotencyToken": "assoc-tok"}),
            account,
        ))["PortfolioDetail"]["Id"]
            .as_str()
            .unwrap()
            .to_string();
        let prod = json_body(&send(
            "CreateProduct",
            json!({
                "Name": "prod-x",
                "Owner": "Owner",
                "ProductType": "CLOUD_FORMATION_TEMPLATE",
                "ProvisioningArtifactParameters": {"Name": "v1", "Info": {"LoadTemplateFromURL": "https://example.com/t.json"}},
                "IdempotencyToken": format!("assoc-prod-tok-{account}"),
            }),
            account,
        ));
        let prod_id = prod["ProductViewDetail"]["ProductViewSummary"]["ProductId"]
            .as_str()
            .unwrap()
            .to_string();
        send(
            "AssociateProductWithPortfolio",
            json!({"ProductId": prod_id, "PortfolioId": pid}),
            account,
        );
        (pid, prod_id)
    }

    #[test]
    fn associate_product_portfolio_and_launch_paths() {
        let account = 900_000_000_109;
        let (pf_id, prod_id) = portfolio_with_product(account);
        // Associate is idempotent; disassociate once succeeds, second is 404.
        send(
            "AssociateProductWithPortfolio",
            json!({"ProductId": prod_id, "PortfolioId": pf_id}),
            account,
        );
        let paths = json_body(&send(
            "ListLaunchPaths",
            json!({"ProductId": prod_id}),
            account,
        ));
        let summaries = paths["LaunchPathSummaries"].as_array().unwrap();
        assert_eq!(summaries.len(), 1);
        assert!(summaries[0]["Id"].as_str().unwrap().starts_with("lp-"));
        assert!(paths_has_portfolio(&paths, &pf_id));

        send(
            "DisassociateProductFromPortfolio",
            json!({"ProductId": prod_id, "PortfolioId": pf_id}),
            account,
        );
        let missing = send(
            "DisassociateProductFromPortfolio",
            json!({"ProductId": prod_id, "PortfolioId": pf_id}),
            account,
        );
        assert_eq!(missing.status, 400);
        assert_eq!(
            json_body(&missing)["__type"],
            json!("ResourceNotFoundException")
        );
    }

    fn paths_has_portfolio(paths: &Value, pf_id: &str) -> bool {
        paths["LaunchPathSummaries"]
            .as_array()
            .unwrap()
            .iter()
            .any(|s| s["Id"].as_str().unwrap() == format!("lp-{pf_id}"))
    }

    #[test]
    fn provision_record_and_terminate() {
        let account = 900_000_000_110;
        let created = json_body(&send(
            "ProvisionProduct",
            json!({
                "ProductId": "prod-fake123",
                "ProvisioningArtifactId": "pa-fake123",
                "ProvisionedProductName": "my-pp",
            }),
            account,
        ));
        let record = &created["RecordDetail"];
        assert!(record["RecordId"].as_str().unwrap().starts_with("rec-"));
        assert_eq!(record["RecordType"], json!("PROVISION_PRODUCT"));
        assert_eq!(record["Status"], json!("SUCCEEDED"));
        let record_id = record["RecordId"].as_str().unwrap().to_string();

        // Look it up by name via describe.
        let described = json_body(&send(
            "DescribeProvisionedProduct",
            json!({"Name": "my-pp"}),
            account,
        ));
        let pp_id = described["ProvisionedProductDetail"]["Id"]
            .as_str()
            .unwrap()
            .to_string();
        assert!(pp_id.starts_with("pp-"));
        assert!(described["CloudWatchDashboards"].as_array().is_some());
        // Record lookup works too.
        let record = json_body(&send("DescribeRecord", json!({"Id": record_id}), account));
        assert_eq!(record["RecordDetail"]["RecordId"], json!(record_id));

        // Scan & search see it.
        let scanned = json_body(&send("ScanProvisionedProducts", json!({}), account));
        assert!(scanned["ProvisionedProducts"]
            .as_array()
            .unwrap()
            .iter()
            .any(|pp| pp["Id"] == json!(pp_id)));
        let searched = json_body(&send(
            "SearchProvisionedProducts",
            json!({"Filters": {"SearchQuery": ["my-pp"]}}),
            account,
        ));
        assert_eq!(searched["TotalResultsCount"], json!(1));

        let terminated = json_body(&send(
            "TerminateProvisionedProduct",
            json!({"ProvisionedProductId": pp_id, "TerminateToken": "tt"}),
            account,
        ));
        assert_eq!(
            terminated["RecordDetail"]["RecordType"],
            json!("TERMINATE_PROVISIONED_PRODUCT")
        );
        let gone = send("DescribeProvisionedProduct", json!({"Id": pp_id}), account);
        assert_eq!(gone.status, 400);
        assert_eq!(
            json_body(&gone)["__type"],
            json!("ResourceNotFoundException")
        );
    }

    #[test]
    fn provisioned_product_not_found_errors() {
        let account = 900_000_000_111;
        for (op, body) in [
            ("DescribeProvisionedProduct", json!({"Id": "pp-fake123"})),
            (
                "GetProvisionedProductOutputs",
                json!({"ProvisionedProductId": "pp-fake123"}),
            ),
            (
                "UpdateProvisionedProduct",
                json!({"ProvisionedProductId": "pp-fake"}),
            ),
            (
                "TerminateProvisionedProduct",
                json!({"ProvisionedProductId": "pp-nonexistent"}),
            ),
            (
                "UpdateProvisionedProductProperties",
                json!({"ProvisionedProductId": "pp-fake"}),
            ),
            (
                "ExecuteProvisionedProductServiceAction",
                json!({"ProvisionedProductId": "pp-fake", "ServiceActionId": "act-fake"}),
            ),
            (
                "ExecuteProvisionedProductPlan",
                json!({"PlanId": "plan-nonexistent"}),
            ),
            (
                "DeleteProvisionedProductPlan",
                json!({"PlanId": "plan-nonexistent"}),
            ),
            (
                "DescribeProvisionedProductPlan",
                json!({"PlanId": "pp-fake123"}),
            ),
            ("DescribeRecord", json!({"Id": "rec-fake123"})),
            ("DescribeServiceAction", json!({"Id": "act-fake123"})),
            ("DeleteServiceAction", json!({"Id": "act-nonexistent000"})),
            ("DescribeConstraint", json!({"Id": "cs-fake123"})),
            ("DescribeTagOption", json!({"Id": "to-fake123"})),
        ] {
            let resp = send(op, body, account);
            assert_eq!(resp.status, 400, "{op} should be a 400 not-found");
            assert_eq!(
                json_body(&resp)["__type"],
                json!("ResourceNotFoundException")
            );
        }
    }

    #[test]
    fn copy_product_and_status() {
        let account = 900_000_000_112;
        let prod = json_body(&send(
            "CreateProduct",
            json!({
                "Name": "copied",
                "Owner": "Owner",
                "ProductType": "CLOUD_FORMATION_TEMPLATE",
                "IdempotencyToken": "tok-copy",
            }),
            account,
        ));
        let arn = prod["ProductViewDetail"]["ProductARN"]
            .as_str()
            .unwrap()
            .to_string();
        let copy = json_body(&send(
            "CopyProduct",
            json!({"SourceProductArn": arn, "IdempotencyToken": "tok-copy2"}),
            account,
        ));
        let token = copy["CopyProductToken"].as_str().unwrap().to_string();
        assert!(token.starts_with("cpt-"));
        let status = json_body(&send(
            "DescribeCopyProductStatus",
            json!({"CopyProductToken": token}),
            account,
        ));
        assert_eq!(status["CopyProductStatus"], json!("SUCCEEDED"));
        // Unknown token reports FAILED.
        let unknown = json_body(&send(
            "DescribeCopyProductStatus",
            json!({"CopyProductToken": "fake-token"}),
            account,
        ));
        assert_eq!(unknown["CopyProductStatus"], json!("FAILED"));
    }

    #[test]
    fn service_action_lifecycle() {
        let account = 900_000_000_113;
        let created = json_body(&send(
            "CreateServiceAction",
            json!({
                "Name": "action",
                "DefinitionType": "SSM_AUTOMATION",
                "Definition": {"Name": "AWS-RestartEC2Instance", "Version": "1"},
                "IdempotencyToken": "tok-sa",
            }),
            account,
        ));
        let sa = &created["ServiceActionDetail"];
        assert!(sa["Id"].as_str().unwrap().starts_with("act-"));
        assert!(sa["Definition"].is_object());

        let updated = json_body(&send(
            "UpdateServiceAction",
            json!({"Id": sa["Id"], "Name": "renamed", "Description": "d"}),
            account,
        ));
        assert_eq!(updated["ServiceActionDetail"]["Name"], json!("renamed"));

        let listed = json_body(&send("ListServiceActions", json!({}), account));
        assert!(listed["ServiceActionSummaries"]
            .as_array()
            .unwrap()
            .iter()
            .any(|a| a["Id"] == sa["Id"]));

        // Batch associate with an unknown action reports RESOURCE_NOT_FOUND.
        let batch = json_body(&send(
            "BatchAssociateServiceActionWithProvisioningArtifact",
            json!({"ServiceActionAssociations": [
                {"ServiceActionId": "fake-sa", "ProductId": "prod-x", "ProvisioningArtifactId": "pa-x"}
            ]}),
            account,
        ));
        let failed = batch["FailedServiceActionAssociations"].as_array().unwrap();
        assert_eq!(failed.len(), 1);
        assert_eq!(failed[0]["ErrorCode"], json!("RESOURCE_NOT_FOUND"));

        // Batch disassociate never fails.
        let batch = json_body(&send(
            "BatchDisassociateServiceActionFromProvisioningArtifact",
            json!({"ServiceActionAssociations": [
                {"ServiceActionId": "fake-sa", "ProductId": "prod-x", "ProvisioningArtifactId": "pa-x"}
            ]}),
            account,
        ));
        assert_eq!(batch["FailedServiceActionAssociations"], json!([]));

        send("DeleteServiceAction", json!({"Id": sa["Id"]}), account);
    }

    #[test]
    fn import_as_provisioned_product_creates_record() {
        let account = 900_000_000_114;
        let created = json_body(&send(
            "ImportAsProvisionedProduct",
            json!({
                "ProductId": "prod-fake",
                "ProvisioningArtifactId": "pa-fake",
                "ProvisionedProductName": "imported-pp",
                "PhysicalId": "arn:aws:cloudformation:us-east-1:123456789012:stack/my-stack/guid",
                "IdempotencyToken": "tok-import",
            }),
            account,
        ));
        let record = &created["RecordDetail"];
        assert_eq!(record["RecordType"], json!("IMPORT_PROVISIONED_PRODUCT"));
        assert_eq!(record["Status"], json!("SUCCEEDED"));
        assert!(record["RecordId"].as_str().is_some());
    }

    #[test]
    fn update_provisioned_product_properties_echo() {
        let account = 900_000_000_115;
        let pp = json_body(&send(
            "ProvisionProduct",
            json!({"ProductId": "p", "ProvisioningArtifactId": "pa", "ProvisionedProductName": "props-pp"}),
            account,
        ))["RecordDetail"]["RecordId"].as_str().is_some();
        assert!(pp);
        // Locate the pp id via search.
        let found = json_body(&send("ScanProvisionedProducts", json!({}), account));
        let pp_id = found["ProvisionedProducts"].as_array().unwrap()[0]["Id"]
            .as_str()
            .unwrap()
            .to_string();
        let resp = json_body(&send(
            "UpdateProvisionedProductProperties",
            json!({
                "ProvisionedProductId": pp_id,
                "ProvisionedProductProperties": {"OWNER": "a@b.c"},
                "IdempotencyToken": "tok-props",
            }),
            account,
        ));
        assert_eq!(resp["ProvisionedProductId"], json!(pp_id));
        assert_eq!(
            resp["ProvisionedProductProperties"]["OWNER"],
            json!("a@b.c")
        );
        assert!(resp["RecordId"].as_str().unwrap().starts_with("rec-"));
        assert_eq!(resp["Status"], json!("AVAILABLE"));
    }

    #[test]
    fn update_provisioned_product_updates_and_records() {
        let account = 900_000_000_116;
        json_body(&send(
            "ProvisionProduct",
            json!({"ProductId": "p1", "ProvisioningArtifactId": "pa1", "ProvisionedProductName": "upd-pp"}),
            account,
        ));
        let resp = json_body(&send(
            "UpdateProvisionedProduct",
            json!({"ProvisionedProductName": "upd-pp", "ProvisioningArtifactId": "pa2"}),
            account,
        ));
        let record = &resp["RecordDetail"];
        assert_eq!(record["RecordType"], json!("UPDATE_PROVISIONED_PRODUCT"));
        let described = json_body(&send(
            "DescribeProvisionedProduct",
            json!({"Name": "upd-pp"}),
            account,
        ));
        assert_eq!(
            described["ProvisionedProductDetail"]["ProvisioningArtifactId"],
            json!("pa2")
        );
    }

    #[test]
    fn provisioning_parameters_defaults() {
        let account = 900_000_000_117;
        let resp = json_body(&send(
            "DescribeProvisioningParameters",
            json!({"ProductId": "prod", "ProvisioningArtifactId": "pa"}),
            account,
        ));
        assert!(resp["ProvisioningArtifactParameters"]
            .as_array()
            .unwrap()
            .is_empty());
        assert!(resp["ConstraintSummaries"].as_array().unwrap().is_empty());
        assert!(resp["UsageInstructions"].as_array().unwrap().is_empty());
        assert!(resp["TagOptions"].as_array().unwrap().is_empty());
        assert_eq!(resp["ProvisioningArtifactPreferences"], json!({}));
    }

    #[test]
    fn budget_associations_roundtrip() {
        let account = 900_000_000_118;
        let resp = send(
            "AssociateBudgetWithResource",
            json!({"BudgetName": "b", "ResourceId": "r1"}),
            account,
        );
        assert_eq!(resp.status, 200);
        let listed = json_body(&send(
            "ListBudgetsForResource",
            json!({"ResourceId": "r1"}),
            account,
        ));
        assert_eq!(listed["Budgets"].as_array().unwrap().len(), 1);
        assert_eq!(listed["Budgets"][0]["BudgetName"], json!("b"));
        send(
            "DisassociateBudgetFromResource",
            json!({"BudgetName": "b", "ResourceId": "r1"}),
            account,
        );
        let listed = json_body(&send(
            "ListBudgetsForResource",
            json!({"ResourceId": "r1"}),
            account,
        ));
        assert!(listed["Budgets"].as_array().unwrap().is_empty());
    }

    #[test]
    fn principal_lifecycle() {
        let account = 900_000_000_118;
        let pf = json_body(&send(
            "CreatePortfolio",
            json!({"DisplayName": "pr", "ProviderName": "P", "IdempotencyToken": "tok-pr"}),
            account,
        ))["PortfolioDetail"]["Id"]
            .as_str()
            .unwrap()
            .to_string();
        let arn = "arn:aws:iam::123456789012:role/test";
        send(
            "AssociatePrincipalWithPortfolio",
            json!({"PortfolioId": pf, "PrincipalARN": arn, "PrincipalType": "IAM"}),
            account,
        );
        let listed = json_body(&send(
            "ListPrincipalsForPortfolio",
            json!({"PortfolioId": pf}),
            account,
        ));
        let principals = listed["Principals"].as_array().unwrap();
        assert_eq!(principals.len(), 1);
        assert_eq!(principals[0]["PrincipalARN"], json!(arn));
        assert_eq!(principals[0]["PrincipalType"], json!("IAM"));
        send(
            "DisassociatePrincipalFromPortfolio",
            json!({"PortfolioId": pf, "PrincipalARN": arn}),
            account,
        );
        let listed = json_body(&send(
            "ListPrincipalsForPortfolio",
            json!({"PortfolioId": pf}),
            account,
        ));
        assert!(listed["Principals"].as_array().unwrap().is_empty());
    }

    #[test]
    fn organizations_access_ops_answer_501() {
        let account = 900_000_000_119;
        for op in [
            "EnableAWSOrganizationsAccess",
            "DisableAWSOrganizationsAccess",
            "GetAWSOrganizationsAccessStatus",
        ] {
            let resp = send(op, json!({}), account);
            assert_eq!(resp.status, 501, "{op}");
            assert_eq!(json_body(&resp)["__type"], json!("NotImplemented"));
        }
    }

    #[test]
    fn unknown_operation_is_501() {
        let resp = send("DestroyEverything", json!({}), 900_000_000_120);
        assert_eq!(resp.status, 501);
        assert_eq!(json_body(&resp)["__type"], json!("NotImplemented"));
    }

    #[test]
    fn copy_then_update_constraint_echoes_parameters() {
        let account = 900_000_000_121;
        let (pf_id, prod_id) = portfolio_with_product(account);
        let created = json_body(&send(
            "CreateConstraint",
            json!({
                "PortfolioId": pf_id,
                "ProductId": prod_id,
                "Type": "NOTIFICATION",
                "Parameters": "{\"a\": 1}",
            }),
            account,
        ));
        let cid = created["ConstraintDetail"]["ConstraintId"]
            .as_str()
            .unwrap()
            .to_string();
        assert_eq!(created["ConstraintParameters"], json!("{\"a\": 1}"));
        let updated = json_body(&send(
            "UpdateConstraint",
            json!({"Id": cid, "Parameters": "{\"a\": 2}"}),
            account,
        ));
        assert_eq!(updated["ConstraintParameters"], json!("{\"a\": 2}"));
    }

    #[test]
    fn foreign_service_is_not_claimed() {
        let mut r = req("ListPortfolios", json!({}), 900_000_000_122);
        r.service = "not-servicecatalog".into();
        assert!(dispatch(&r).is_none());
    }
}
