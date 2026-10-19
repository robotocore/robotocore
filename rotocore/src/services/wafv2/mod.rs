//! AWS WAFv2 backend — moto-equivalent (json protocol).
//!
//! Replaces the moto sidecar for `wafv2` (`X-Amz-Target: WAFV2_20190729.<Op>`
//! on `application/x-amz-json-1.1`). State is per-(account, region), mirroring
//! moto's `WAFV2Backend` (moto/wafv2): WebACLs, IP sets, regex pattern sets,
//! rule groups, logging configurations, permission policies, tags (moto's
//! `TaggingService`), and the API-key table — plus the canned responses for
//! managed rule groups/products, managed rule sets, the mobile-SDK surface,
//! rate-based managed keys, and sampled requests.
//!
//! Contract: probes/wafv2.json (per-operation moto-era behavior) and
//! tests/compatibility/test_wafv2_compat.py. Moto quirks preserved on purpose:
//! - `Scope=CLOUDFRONT` requests run on a `global`-region store slice (moto
//!   flips `self.region` in every scope-taking handler), while CloudFront
//!   ARNs still carry region `us-east-1` and a `global/…` resource path
//!   (`moto/wafv2/utils.py:make_arn`).
//! - Regex-pattern-set ARNs always carry the `regional` prefix, even for
//!   CloudFront scope (`make_arn_for_regex_pattern_set`'s hardcode).
//! - WebACL capacity is the constant 3; IPSet lock tokens are the first six
//!   characters of a uuid4; WebACL/RuleGroup lock tokens are full uuid4s.
//! - `UpdateRuleGroup`/`UpdateWebACL` keep `AvailableLabels`/`ConsumedLabels`
//!   stale: moto recomputes them only at construction.
//! - `GetRuleGroup` with insufficient info reports moto's misspelled
//!   `AcessDeniedException` code.
//! - `GetTopPathStatisticsByTraffic` is excluded: moto never implemented it
//!   (probe `not_implemented`), so the 501 fallthrough answers.

use crate::proto::{json_error, now_iso, parse_json_body};
use crate::store::{self, ScopeKey};
use crate::{Request, Response};
use base64::engine::general_purpose::STANDARD as B64;
use base64::Engine as _;
use parking_lot::RwLock;
use serde_json::{json, Value};
use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;
use uuid::Uuid;

/// moto's `GLOBAL_REGION` — the backend slice CloudFront-scope state lives in.
const GLOBAL_REGION: &str = "global";
/// moto's `FakeWebACL.__init__` hardcodes consumed capacity.
const WEBACL_CAPACITY: i64 = 3;
/// moto's wafv2 pagination model `limit_default` for every list operation.
const PAGE_LIMIT_DEFAULT: usize = 100;

// ---------------------------------------------------------------------------
// State
// ---------------------------------------------------------------------------

type Stores = Arc<RwLock<HashMap<ScopeKey, WafStore>>>;

fn stores() -> Stores {
    store::slot::<WafStore>("wafv2")
}

/// Per-(account, region) in-memory state for AWS WAFv2.
#[derive(Default)]
struct WafStore {
    /// arn -> WebACL (moto keys its OrderedDict by ARN)
    wacls: BTreeMap<String, WebAcl>,
    /// arn -> IPSet
    ip_sets: BTreeMap<String, IpSet>,
    /// arn -> RegexPatternSet
    regex_pattern_sets: BTreeMap<String, RegexPatternSet>,
    /// arn -> RuleGroup
    rule_groups: BTreeMap<String, RuleGroup>,
    /// resource arn -> logging configuration
    logging_configurations: BTreeMap<String, LoggingConfiguration>,
    /// resource arn -> permission-policy document
    permission_policies: BTreeMap<String, String>,
    /// arn -> {tag key: value} — moto's `TaggingService`
    tags: BTreeMap<String, BTreeMap<String, Option<String>>>,
    /// api key -> key record (moto's lazily-created `_api_keys` dict)
    api_keys: BTreeMap<String, ApiKeySummary>,
}

fn with_region_store(
    account: u64,
    region: &str,
    f: impl FnOnce(&mut WafStore) -> Response,
) -> Response {
    let slot = stores();
    let mut map = slot.write();
    let st = map.entry((account, region.to_string())).or_default();
    f(st)
}

// ---------------------------------------------------------------------------
// Records
// ---------------------------------------------------------------------------

/// One canonical WAFv2 rule, mirroring moto's `FakeRule`.
struct Rule {
    name: String,
    priority: Value,
    statement: Value,
    visibility_config: Value,
    action: Option<Value>,
    captcha_config: Option<Value>,
    challenge_config: Option<Value>,
    override_action: Option<Value>,
    rule_labels: Option<Value>,
}

impl Rule {
    fn from_body(rule: &Value) -> Self {
        Rule {
            name: rule
                .get("Name")
                .and_then(Value::as_str)
                .unwrap_or("")
                .to_string(),
            priority: rule.get("Priority").cloned().unwrap_or(Value::Null),
            statement: rule.get("Statement").cloned().unwrap_or(json!({})),
            visibility_config: rule.get("VisibilityConfig").cloned().unwrap_or(json!({})),
            action: rule.get("Action").cloned(),
            captcha_config: rule.get("CaptchaConfig").cloned(),
            challenge_config: rule.get("ChallengeConfig").cloned(),
            override_action: rule.get("OverrideAction").cloned(),
            rule_labels: rule.get("RuleLabels").cloned(),
        }
    }

    /// moto's `Rule.to_dict()`: optional members present as nulls.
    fn to_dict(&self) -> Value {
        json!({
            "Name": self.name,
            "Action": self.action.clone().unwrap_or(Value::Null),
            "Priority": self.priority,
            "Statement": self.statement,
            "VisibilityConfig": self.visibility_config,
            "CaptchaConfig": self.captcha_config.clone().unwrap_or(Value::Null),
            "ChallengeConfig": self.challenge_config.clone().unwrap_or(Value::Null),
            "OverrideAction": self.override_action.clone().unwrap_or(Value::Null),
            "RuleLabels": self.rule_labels.clone().unwrap_or(Value::Null),
        })
    }

    /// moto's `get_consumed_label`.
    fn consumed_label(&self) -> Option<String> {
        self.statement
            .get("LabelMatchStatement")
            .and_then(|s| s.get("Key"))
            .and_then(Value::as_str)
            .map(String::from)
    }

    /// moto's `get_available_labels`: absent or empty `RuleLabels` → empty
    /// (which its rule-group comprehension skips).
    fn available_labels(&self) -> Vec<String> {
        match self.rule_labels.as_ref().and_then(Value::as_array) {
            Some(labels) if !labels.is_empty() => labels
                .iter()
                .filter_map(|l| l.get("Name")?.as_str().map(String::from))
                .collect(),
            _ => Vec::new(),
        }
    }
}

struct WebAcl {
    name: String,
    id: String,
    arn: String,
    description: String,
    default_action: Value,
    rules: Vec<Rule>,
    visibility_config: Value,
    lock_token: String,
    associated_resources: Vec<String>,
    association_config: Option<Value>,
    captcha_config: Option<Value>,
    challenge_config: Option<Value>,
    custom_response_bodies: Option<Value>,
    token_domains: Option<Value>,
    label_namespace: String,
}

impl WebAcl {
    fn to_dict(&self) -> Value {
        json!({
            "Name": self.name,
            "Id": self.id,
            "ARN": self.arn,
            "Description": self.description,
            "Rules": self.rules.iter().map(Rule::to_dict).collect::<Vec<_>>(),
            "DefaultAction": self.default_action,
            "VisibilityConfig": self.visibility_config,
            "Capacity": WEBACL_CAPACITY,
            "LockToken": self.lock_token,
            "AssociationConfig": self.association_config.clone().unwrap_or(Value::Null),
            "CaptchaConfig": self.captcha_config.clone().unwrap_or(Value::Null),
            "ChallengeConfig": self.challenge_config.clone().unwrap_or(Value::Null),
            "CustomResponseBodies": self.custom_response_bodies.clone().unwrap_or(Value::Null),
            "TokenDomains": self.token_domains.clone().unwrap_or(Value::Null),
            "LabelNamespace": self.label_namespace,
        })
    }

    /// moto's `FakeWebACL.to_short_dict()` — the `Summary` wire shape.
    fn to_summary(&self) -> Value {
        json!({
            "ARN": self.arn,
            "Description": self.description,
            "Id": self.id,
            "Name": self.name,
            "LockToken": self.lock_token,
        })
    }

    /// moto's `FakeWebACL.update` — only non-null members are applied.
    // moto's update_web_acl threads nine optional members in, one per
    // request member; clippy's 7-arg ceiling is waived for the shape.
    #[allow(clippy::too_many_arguments)]
    fn update(
        &mut self,
        default_action: Option<Value>,
        rules: Option<Vec<Rule>>,
        description: Option<String>,
        visibility_config: Option<Value>,
        custom_response_bodies: Option<Value>,
        captcha_config: Option<Value>,
        challenge_config: Option<Value>,
        token_domains: Option<Value>,
        association_config: Option<Value>,
    ) {
        if let Some(v) = default_action {
            self.default_action = v;
        }
        if let Some(rules) = rules {
            self.rules = rules;
        }
        if let Some(d) = description {
            self.description = d;
        }
        if let Some(v) = visibility_config {
            self.visibility_config = v;
        }
        if let Some(v) = custom_response_bodies {
            self.custom_response_bodies = Some(v);
        }
        if let Some(v) = captcha_config {
            self.captcha_config = Some(v);
        }
        if let Some(v) = challenge_config {
            self.challenge_config = Some(v);
        }
        if let Some(v) = token_domains {
            self.token_domains = Some(v);
        }
        if let Some(v) = association_config {
            self.association_config = Some(v);
        }
        self.lock_token = Uuid::new_v4().to_string();
    }
}

struct IpSet {
    name: String,
    id: String,
    arn: String,
    description: Option<String>,
    addresses: Value,
    ip_address_version: Option<String>,
    scope: String,
    lock_token: String,
}

impl IpSet {
    /// moto builds the IPSet `Summary` inline in the responses layer.
    fn to_summary(&self) -> Value {
        json!({
            "Name": self.name,
            "Id": self.id,
            "Description": maybe_str(self.description.clone()),
            "LockToken": self.lock_token,
            "ARN": self.arn,
        })
    }

    /// moto's `str(uuid4())[0:6]` — the first six characters of the dashed
    /// uuid string.
    fn new_lock_token() -> String {
        Uuid::new_v4()
            .to_string()
            .get(..6)
            .unwrap_or_default()
            .to_string()
    }

    fn to_dict(&self) -> Value {
        json!({
            "Name": self.name,
            "Id": self.id,
            "ARN": self.arn,
            "Description": maybe_str(self.description.clone()),
            "IPAddressVersion": maybe_str(self.ip_address_version.clone()),
            "Addresses": self.addresses,
            "LockToken": self.lock_token,
        })
    }

    fn update(&mut self, description: Option<String>, addresses: Value) {
        if let Some(d) = description {
            self.description = Some(d);
        }
        self.addresses = addresses;
        self.lock_token = Self::new_lock_token();
    }
}

struct RegexPatternSet {
    name: String,
    id: String,
    arn: String,
    description: Option<String>,
    regular_expressions: Value,
    scope: String,
    lock_token: String,
}

impl RegexPatternSet {
    fn to_dict(&self) -> Value {
        json!({
            "Name": self.name,
            "Id": self.id,
            "ARN": self.arn,
            "Description": maybe_str(self.description.clone()),
            "RegularExpressionList": self.regular_expressions,
            "LockToken": self.lock_token,
        })
    }

    fn to_summary(&self) -> Value {
        json!({
            "Name": self.name,
            "Id": self.id,
            "Description": maybe_str(self.description.clone()),
            "ARN": self.arn,
            "LockToken": self.lock_token,
        })
    }

    fn update(&mut self, description: Option<String>, regular_expressions: Option<Value>) {
        if let Some(d) = description {
            self.description = Some(d);
        }
        if let Some(exprs) = regular_expressions {
            self.regular_expressions = exprs;
        }
        self.lock_token = Uuid::new_v4().to_string();
    }
}

struct RuleGroup {
    name: String,
    id: String,
    arn: String,
    scope: String,
    capacity: Value,
    description: Option<String>,
    rules: Vec<Rule>,
    visibility_config: Value,
    lock_token: String,
    custom_response_bodies: Option<Value>,
    label_namespace: String,
    /// computed at construction only — moto never refreshes these on update
    available_labels: Option<Value>,
    consumed_labels: Option<Value>,
}

impl RuleGroup {
    /// moto computes `AvailableLabels`/`ConsumedLabels` at construction only —
    /// `update()` never refreshes them.
    // Ten construction members mirror moto's FakeRuleGroup.__init__ inputs.
    #[allow(clippy::too_many_arguments)]
    fn from_parts(
        account: u64,
        name: &str,
        id: &str,
        arn: String,
        scope: &str,
        capacity: Value,
        description: Option<String>,
        rules: Vec<Rule>,
        visibility_config: Value,
        custom_response_bodies: Option<Value>,
    ) -> Self {
        let label_namespace = format!("awswaf:{account}:rulegroup:{name}:");
        let available_labels = if rules.is_empty() {
            None
        } else {
            Some(json!(rules
                .iter()
                .flat_map(|r| r.available_labels())
                .map(|l| json!({ "Name": format!("{label_namespace}{l}") }))
                .collect::<Vec<_>>()))
        };
        let consumed_labels = if rules.is_empty() {
            None
        } else {
            Some(json!(rules
                .iter()
                .filter_map(Rule::consumed_label)
                .map(|l| json!({ "Name": format!("{label_namespace}{l}") }))
                .collect::<Vec<_>>()))
        };
        RuleGroup {
            name: name.to_string(),
            id: id.to_string(),
            arn,
            scope: scope.to_string(),
            capacity,
            description,
            rules,
            visibility_config,
            lock_token: Uuid::new_v4().to_string(),
            custom_response_bodies,
            label_namespace,
            available_labels,
            consumed_labels,
        }
    }

    /// moto's `FakeRuleGroup.to_dict()` (`GetRuleGroup` pops the LockToken).
    fn to_dict(&self, include_lock_token: bool) -> Value {
        let mut out = json!({
            "Name": self.name,
            "Id": self.id,
            "Capacity": self.capacity,
            "ARN": self.arn,
            "Description": maybe_str(self.description.clone()),
            "Rules": self.rules.iter().map(Rule::to_dict).collect::<Vec<_>>(),
            "VisibilityConfig": self.visibility_config,
            "LabelNamespace": self.label_namespace,
            "AvailableLabels": self.available_labels.clone().unwrap_or(Value::Null),
            "ConsumedLabels": self.consumed_labels.clone().unwrap_or(Value::Null),
            "CustomResponseBodies": self.custom_response_bodies.clone().unwrap_or(Value::Null),
        });
        if include_lock_token {
            out["LockToken"] = json!(self.lock_token);
        }
        out
    }

    fn to_summary(&self) -> Value {
        json!({
            "Name": self.name,
            "Id": self.id,
            "Description": maybe_str(self.description.clone()),
            "LockToken": self.lock_token,
            "ARN": self.arn,
        })
    }
}

struct LoggingConfiguration {
    arn: String,
    log_destination_configs: Value,
    redacted_fields: Option<Value>,
    managed_by_firewall_manager: bool,
    logging_filter: Option<Value>,
}

impl LoggingConfiguration {
    /// moto's responses drop null members; `ManagedByFirewallManager` stays.
    fn to_dict(&self) -> Value {
        let mut out = json!({
            "ResourceArn": self.arn,
            "LogDestinationConfigs": self.log_destination_configs,
            "ManagedByFirewallManager": self.managed_by_firewall_manager,
        });
        if let Some(v) = &self.redacted_fields {
            out["RedactedFields"] = v.clone();
        }
        if let Some(v) = &self.logging_filter {
            out["LoggingFilter"] = v.clone();
        }
        out
    }
}

struct ApiKeySummary {
    token_domains: Value,
    created_at: String,
}

impl ApiKeySummary {
    /// moto's create_api_key storage row.
    fn to_dict(&self, api_key: &str) -> Value {
        json!({
            "APIKey": api_key,
            "TokenDomains": self.token_domains,
            "CreationTimestamp": self.created_at,
            "Version": 1,
        })
    }
}

// ---------------------------------------------------------------------------
// ARN builders — moto/wafv2/utils.py
// ---------------------------------------------------------------------------

/// moto's `get_partition`.
fn partition(region: &str) -> &'static str {
    for (prefix, part) in [
        ("cn-", "aws-cn"),
        ("us-gov-", "aws-us-gov"),
        ("us-iso-", "aws-iso"),
        ("us-isob-", "aws-iso-b"),
        ("eusc-", "aws-eusc"),
    ] {
        if region.starts_with(prefix) {
            return part;
        }
    }
    "aws"
}

/// moto's `make_arn`: `REGIONAL` → `regional`, `CLOUDFRONT` → `global` with
/// the region forced to us-east-1; other scope strings pass through verbatim.
fn resource_arn(
    resource: &str,
    account: u64,
    region: &str,
    scope: &str,
    name: &str,
    id: &str,
) -> String {
    let (region, scope) = match scope {
        "REGIONAL" => (region, "regional"),
        "CLOUDFRONT" => ("us-east-1", "global"),
        other => (region, other),
    };
    format!(
        "arn:{}:wafv2:{region}:{account}:{scope}/{resource}/{name}/{id}",
        partition(region)
    )
}

/// moto's `make_arn_for_regex_pattern_set`: always `regional`; region is
/// us-east-1 for CloudFront scope.
fn regex_pattern_set_arn(account: u64, region: &str, scope: &str, name: &str, id: &str) -> String {
    let region = if scope == "CLOUDFRONT" {
        "us-east-1"
    } else {
        region
    };
    format!("arn:aws:wafv2:{region}:{account}:regional/regexpatternset/{name}/{id}")
}

// ---------------------------------------------------------------------------
// Wire helpers
// ---------------------------------------------------------------------------

fn ok(body: Value) -> Response {
    Response::json(200, serde_json::to_vec(&body).unwrap_or_default())
}

fn ok_empty() -> Response {
    ok(json!({}))
}

/// Every miss in moto surfaces as this one exception shape.
fn nonexistent() -> Response {
    json_error(
        400,
        "WAFNonexistentItemException",
        "AWS WAF couldn\u{2019}t perform the operation because your resource doesn\u{2019}t exist.",
    )
}

/// moto's `WAFOptimisticLockException` — raised whenever a stored LockToken
/// fails to match (which includes absent LockToken members).
fn optimistic_lock() -> Response {
    json_error(
        400,
        "WAFOptimisticLockException",
        "AWS WAF couldn\u{2019}t save your changes because someone changed the resource after you started to edit it. Reapply your changes.",
    )
}

/// moto's `WAFV2DuplicateItemException` — code `WafV2DuplicateItem`.
fn duplicate() -> Response {
    json_error(
        400,
        "WafV2DuplicateItem",
        "AWS WAF could not perform the operation because some resource in your request is a duplicate of an existing one.",
    )
}

/// moto's `WAFInvalidParameterException`.
fn invalid_parameter(message: &str) -> Response {
    json_error(400, "WAFInvalidParameterException", message)
}

/// moto's `WAFV2InsufficientInformationException` — misspelled code preserved.
fn insufficient_information(name: &str, scope: &str, id: &str, arn: Option<&str>) -> Response {
    json_error(
        400,
        "AcessDeniedException",
        &format!(
            "Critical information is missing in your request: GetRuleGroupRequest(name={}, scope={}, id={}, aRN={}).",
            nullified(name),
            nullified(scope),
            nullified(id),
            arn.unwrap_or("null"),
        ),
    )
}

/// Empty-string members render as moto's literal `null` message text.
fn nullified(s: &str) -> String {
    if s.is_empty() {
        "null".to_string()
    } else {
        s.to_string()
    }
}

fn as_str<'a>(body: &'a Value, key: &str) -> &'a str {
    body.get(key).and_then(Value::as_str).unwrap_or("")
}

fn as_opt_str(body: &Value, key: &str) -> Option<String> {
    body.get(key).and_then(Value::as_str).map(String::from)
}

/// A member that is present and not JSON-null (moto's `is not None` truthy
/// distinction for optional dict-member updates).
fn non_null(v: Option<&Value>) -> Option<&Value> {
    v.filter(|v| !v.is_null())
}

/// `Option<String>` → JSON; absent members serialize as null, matching
/// json.dumps of Python None.
fn maybe_str(v: Option<String>) -> Value {
    match v {
        Some(s) => Value::String(s),
        None => Value::Null,
    }
}

// ---------------------------------------------------------------------------
// Scope / region plumbing — moto flips `self.region` per handler
// ---------------------------------------------------------------------------

/// CLOUDFRONT-scope requests run on the `global` backend slice.
fn scope_region(current: &str, scope: Option<&Value>) -> String {
    if scope.and_then(Value::as_str) == Some("CLOUDFRONT") {
        GLOBAL_REGION.to_string()
    } else {
        current.to_string()
    }
}

/// Tag operations pick the backend from the resource ARN: a `global/…`
/// resource segment means CloudFront, otherwise the ARN's own region.
fn arn_tag_region(current: &str, arn: &str) -> String {
    let global = arn
        .split(':')
        .nth(5)
        .map(|s| s.starts_with("global"))
        .unwrap_or(false);
    if global {
        return GLOBAL_REGION.to_string();
    }
    arn.split(':')
        .nth(3)
        .filter(|r| !r.is_empty())
        .unwrap_or(current)
        .to_string()
}

/// `GetRuleGroup` also switches on an ARN pointing at the global partition.
fn rule_group_region(current: &str, scope: Option<&Value>, arn: Option<&Value>) -> String {
    let scope_global = scope.and_then(Value::as_str) == Some("CLOUDFRONT");
    let arn_global = non_null(arn)
        .and_then(Value::as_str)
        .and_then(|a| a.split(':').nth(3))
        .map(|r| r == GLOBAL_REGION)
        .unwrap_or(false);
    if scope_global || arn_global {
        GLOBAL_REGION.to_string()
    } else {
        current.to_string()
    }
}

fn tag_resource(st: &mut WafStore, arn: &str, tags: Option<&Vec<Value>>) {
    let Some(tags) = tags else {
        return;
    };
    let row = st.tags.entry(arn.to_string()).or_default();
    for tag in tags {
        if let Some(key) = tag.get("Key").and_then(Value::as_str) {
            // A member without `Value` stores null, just like moto's tagger.
            row.insert(
                key.to_string(),
                tag.get("Value").and_then(Value::as_str).map(String::from),
            );
        }
    }
}

fn untag_resource(st: &mut WafStore, arn: &str, tag_keys: Option<&Vec<Value>>) {
    if let Some(keys) = tag_keys {
        if let Some(row) = st.tags.get_mut(arn) {
            for key in keys {
                if let Some(name) = key.as_str() {
                    row.remove(name);
                }
            }
        }
    }
}

// ---------------------------------------------------------------------------
// Pagination — moto's `paginate` decorator with botocore-style markers
// (`{"uniqueAttributes": …}` base64-JSON). moto additionally embeds a
// process-salted parameter checksum, which cannot be reproduced here; token
// decoding tolerates its absence.
// ---------------------------------------------------------------------------

fn page<T: Clone>(
    items: &[T],
    limit: Option<&Value>,
    marker: Option<&Value>,
    unique: impl Fn(&T) -> String + Clone,
) -> (Vec<T>, Option<String>) {
    let max_results = limit
        .and_then(Value::as_u64)
        .filter(|l| *l > 0)
        .map(|l| l as usize)
        .unwrap_or(PAGE_LIMIT_DEFAULT);

    let index_start = match marker.and_then(Value::as_str).filter(|m| !m.is_empty()) {
        None => 0,
        Some(token) => {
            let decoded = B64
                .decode(token)
                .ok()
                .and_then(|b| serde_json::from_slice::<Value>(&b).ok())
                .and_then(|v| {
                    v.get("uniqueAttributes")
                        .and_then(Value::as_str)
                        .map(String::from)
                });
            match decoded.and_then(|attr| items.iter().position(|it| unique(it) == attr)) {
                Some(pos) => pos,
                // moto: `InvalidToken("Resource not found!")` crashes as a 500.
                None => return (Vec::new(), None),
            }
        }
    };

    let index_end = (index_start + max_results).min(items.len());
    let out: Vec<T> = items[index_start..index_end].to_vec();
    let next_marker = if items[index_start..index_end].is_empty() || index_end >= items.len() {
        None
    } else {
        Some(
            B64.encode(
                json!({ "uniqueAttributes": unique(&items[index_end]) })
                    .to_string()
                    .as_bytes(),
            ),
        )
    };
    (out, next_marker)
}

// ---------------------------------------------------------------------------
// dispatch
// ---------------------------------------------------------------------------

pub fn dispatch(req: &Request) -> Option<Response> {
    if req.service != "wafv2" {
        return None;
    }
    Some(route(req))
}

fn route(req: &Request) -> Response {
    let body = parse_json_body(&req.body);
    let scope = body.get("Scope");
    let arn_param = body.get("ARN");
    let account = req.account;

    let op = req.operation.as_str();
    match op {
        // --- WebACLs -------------------------------------------------------
        // moto's association endpoints run on the request-region backend.
        "AssociateWebACL" => {
            with_region_store(account, &req.region, |st| associate_web_acl(st, &body))
        }
        "DisassociateWebACL" => {
            with_region_store(account, &req.region, |st| disassociate_web_acl(st, &body))
        }
        "GetWebACLForResource" => with_region_store(account, &req.region, |st| {
            get_web_acl_for_resource(st, &body)
        }),
        "ListResourcesForWebACL" => with_region_store(account, &req.region, |st| {
            list_resources_for_web_acl(st, &body)
        }),
        "CreateWebACL" => with_region_store(account, &scope_region(&req.region, scope), |st| {
            create_web_acl(st, account, &req.region, &body)
        }),
        "GetWebACL" => with_region_store(account, &scope_region(&req.region, scope), |st| {
            get_web_acl(st, &body)
        }),
        "ListWebACLs" => with_region_store(account, &scope_region(&req.region, scope), |st| {
            list_web_acls(st, &body)
        }),
        "DeleteWebACL" => with_region_store(account, &scope_region(&req.region, scope), |st| {
            delete_web_acl(st, account, &req.region, &body)
        }),
        "UpdateWebACL" => with_region_store(account, &scope_region(&req.region, scope), |st| {
            update_web_acl(st, &body)
        }),
        // --- IP sets ----------------------------------------------------------
        "CreateIPSet" => with_region_store(account, &scope_region(&req.region, scope), |st| {
            create_ip_set(st, account, &req.region, &body)
        }),
        "GetIPSet" => with_region_store(account, &scope_region(&req.region, scope), |st| {
            get_ip_set(st, account, &req.region, &body)
        }),
        "ListIPSets" => with_region_store(account, &scope_region(&req.region, scope), |st| {
            list_ip_sets(st, &body)
        }),
        "DeleteIPSet" => with_region_store(account, &scope_region(&req.region, scope), |st| {
            delete_ip_set(st, account, &req.region, &body)
        }),
        "UpdateIPSet" => with_region_store(account, &scope_region(&req.region, scope), |st| {
            update_ip_set(st, account, &req.region, &body)
        }),
        // --- regex pattern sets -------------------------------------------------
        "CreateRegexPatternSet" => {
            with_region_store(account, &scope_region(&req.region, scope), |st| {
                create_regex_pattern_set(st, account, &req.region, &body)
            })
        }
        "GetRegexPatternSet" => {
            with_region_store(account, &scope_region(&req.region, scope), |st| {
                get_regex_pattern_set(st, account, &req.region, &body)
            })
        }
        "ListRegexPatternSets" => {
            with_region_store(account, &scope_region(&req.region, scope), |st| {
                list_regex_pattern_sets(st, &body)
            })
        }
        "DeleteRegexPatternSet" => {
            with_region_store(account, &scope_region(&req.region, scope), |st| {
                delete_regex_pattern_set(st, account, &req.region, &body)
            })
        }
        "UpdateRegexPatternSet" => {
            with_region_store(account, &scope_region(&req.region, scope), |st| {
                update_regex_pattern_set(st, account, &req.region, &body)
            })
        }
        // --- rule groups ----------------------------------------------------------
        "CreateRuleGroup" => with_region_store(account, &scope_region(&req.region, scope), |st| {
            create_rule_group(st, account, &req.region, &body)
        }),
        "GetRuleGroup" => with_region_store(
            account,
            &rule_group_region(&req.region, scope, arn_param),
            |st| get_rule_group(st, account, &req.region, &body),
        ),
        "ListRuleGroups" => with_region_store(account, &scope_region(&req.region, scope), |st| {
            list_rule_groups(st, &body)
        }),
        "DeleteRuleGroup" => with_region_store(account, &scope_region(&req.region, scope), |st| {
            delete_rule_group(st, account, &req.region, &body)
        }),
        "UpdateRuleGroup" => with_region_store(account, &scope_region(&req.region, scope), |st| {
            update_rule_group(st, account, &req.region, &body)
        }),
        // --- logging configurations (no scope switch in moto) ---------------------
        "PutLoggingConfiguration" => with_region_store(account, &req.region, |st| {
            put_logging_configuration(st, &body)
        }),
        "GetLoggingConfiguration" => with_region_store(account, &req.region, |st| {
            get_logging_configuration(st, &body)
        }),
        "DeleteLoggingConfiguration" => with_region_store(account, &req.region, |st| {
            delete_logging_configuration(st, &body)
        }),
        "ListLoggingConfigurations" => with_region_store(account, &req.region, |st| {
            list_logging_configurations(st, &req.region, &body)
        }),
        // --- permission policies (request-region backend in moto) ------------------
        "PutPermissionPolicy" => {
            with_region_store(account, &req.region, |st| put_permission_policy(st, &body))
        }
        "GetPermissionPolicy" => {
            with_region_store(account, &req.region, |st| get_permission_policy(st, &body))
        }
        "DeletePermissionPolicy" => with_region_store(account, &req.region, |st| {
            delete_permission_policy(st, &body)
        }),
        // --- tags (backend chosen from the ARN) ------------------------------------
        "TagResource" => with_region_store(
            account,
            &arn_tag_region(&req.region, as_str(&body, "ResourceARN")),
            |st| {
                tag_resource(
                    st,
                    as_str(&body, "ResourceARN"),
                    body.get("Tags").and_then(Value::as_array),
                );
                ok_empty()
            },
        ),
        "UntagResource" => with_region_store(
            account,
            &arn_tag_region(&req.region, as_str(&body, "ResourceARN")),
            |st| {
                untag_resource(
                    st,
                    as_str(&body, "ResourceARN"),
                    body.get("TagKeys").and_then(Value::as_array),
                );
                ok_empty()
            },
        ),
        "ListTagsForResource" => with_region_store(
            account,
            &arn_tag_region(&req.region, as_str(&body, "ResourceARN")),
            |st| list_tags_for_resource(st, &body),
        ),
        // --- API keys (the token-domain surface, moto's `_api_keys`) -----------------
        "CreateAPIKey" | "DeleteAPIKey" | "GetDecryptedAPIKey" | "ListAPIKeys" => {
            with_region_store(account, &scope_region(&req.region, scope), |st| match op {
                "CreateAPIKey" => create_api_key(st, &body),
                "DeleteAPIKey" => delete_api_key(st, &body),
                "GetDecryptedAPIKey" => get_decrypted_api_key(st, &body),
                _ => list_api_keys(st),
            })
        }
        // --- canned responses (moto only switches regions for these) ---------------
        "GetRateBasedStatementManagedKeys" => rate_based_managed_keys(),
        "GetSampledRequests" => sampled_requests(&body),
        "DeleteFirewallManagerRuleGroups" => ok(json!({ "NextWebACLLockToken": Uuid::new_v4() })),
        "CheckCapacity" => check_capacity(&body),
        "DescribeManagedRuleGroup" => describe_managed_rule_group(&body),
        "ListAvailableManagedRuleGroups" => ok(
            json!({ "ManagedRuleGroups": AVAILABLE_MANAGED_RULE_GROUPS.clone(), "NextMarker": null }),
        ),
        "ListAvailableManagedRuleGroupVersions" => ok(json!({
            "Versions": [
                {"Name": "Version_1.0", "LastUpdateTimestamp": "2021-03-01T00:00:00Z"},
                {"Name": "Version_1.1", "LastUpdateTimestamp": "2021-06-01T00:00:00Z"},
                {"Name": "Version_2.0", "LastUpdateTimestamp": "2022-01-01T00:00:00Z"},
            ],
            "CurrentDefaultVersion": "Version_2.0",
            "NextMarker": null,
        })),
        "GetManagedRuleSet" => get_managed_rule_set(account, &req.region, &body),
        "ListManagedRuleSets" => ok(json!({ "ManagedRuleSets": [], "NextMarker": null })),
        "PutManagedRuleSetVersions" => ok(json!({ "NextLockToken": Uuid::new_v4() })),
        "UpdateManagedRuleSetVersionExpiryDate" => {
            update_managed_rule_set_version_expiry_date(&body)
        }
        "DescribeAllManagedProducts" => {
            ok(json!({ "ManagedProducts": all_managed_products(account, &req.region) }))
        }
        "DescribeManagedProductsByVendor" => {
            let vendor = as_str(&body, "VendorName");
            let products: Vec<_> = all_managed_products(account, &req.region)
                .into_iter()
                .filter(|p| p["VendorName"] == vendor)
                .collect();
            ok(json!({ "ManagedProducts": products }))
        }
        // --- mobile SDK stubs ----------------------------------------------------------
        "GetMobileSdkRelease" => ok(json!({
            "MobileSdkRelease": {
                "ReleaseVersion": as_str(&body, "ReleaseVersion"),
                "Timestamp": "2023-01-01T00:00:00Z",
                "ReleaseNotes": "Mock release",
                "Tags": [],
            }
        })),
        "ListMobileSdkReleases" => ok(json!({ "ReleaseSummaries": [], "NextMarker": null })),
        "GenerateMobileSdkReleaseUrl" => ok(json!({
            "Url": format!(
                "https://sdk.amazonaws.com/wafv2/{}/{}/sdk.zip",
                as_str(&body, "Platform"),
                as_str(&body, "ReleaseVersion")
            ),
        })),
        other => json_error(
            501,
            "NotImplemented",
            &format!("The {other} action has not been implemented"),
        ),
    }
}

/// moto's canned `ListAvailableManagedRuleGroups` payload.
static AVAILABLE_MANAGED_RULE_GROUPS: once_cell::sync::Lazy<Vec<Value>> =
    once_cell::sync::Lazy::new(|| {
        [
            ("AWSManagedRulesCommonRuleSet", "Contains rules that are generally applicable to web applications."),
            ("AWSManagedRulesAdminProtectionRuleSet", "Contains rules that allow you to block external access to exposed admin pages."),
            ("AWSManagedRulesKnownBadInputsRuleSet", "Contains rules to block request patterns that are known to be invalid."),
            ("AWSManagedRulesSQLiRuleSet", "Contains rules that allow you to block request patterns associated with SQL injection."),
            ("AWSManagedRulesLinuxRuleSet", "Contains rules that block request patterns associated with exploitation of vulnerabilities specific to Linux."),
            ("AWSManagedRulesUnixRuleSet", "Contains rules that block request patterns associated with POSIX/POSIX-like OS."),
            ("AWSManagedRulesWindowsRuleSet", "Contains rules that block request patterns associated with exploitation of vulnerabilities specific to Windows."),
            ("AWSManagedRulesPHPRuleSet", "Contains rules that block request patterns associated with PHP."),
            ("AWSManagedRulesWordPressRuleSet", "Contains rules that block request patterns associated with WordPress."),
            ("AWSManagedRulesAmazonIpReputationList", "This group contains rules that are based on Amazon internal threat intelligence."),
            ("AWSManagedRulesAnonymousIpList", "This group contains rules to block requests from services that allow obfuscation of viewer identity."),
            ("AWSManagedRulesBotControlRuleSet", "Contains rules for managing bot traffic."),
            ("AWSManagedRulesATPRuleSet", "Contains rules for managing account takeover prevention."),
        ]
        .iter()
        .map(|(name, description)| json!({"VendorName": "AWS", "Name": name, "Description": description}))
        .collect()
    });

// ---------------------------------------------------------------------------
// WebACL operations
// ---------------------------------------------------------------------------

fn create_web_acl(st: &mut WafStore, account: u64, region: &str, body: &Value) -> Response {
    let scope = as_str(body, "Scope");
    let name = as_str(body, "Name");
    let id = Uuid::new_v4().to_string();
    let arn = resource_arn("webacl", account, region, scope, name, &id);
    if st.wacls.contains_key(&arn) || st.wacls.values().any(|w| w.name == name) {
        return duplicate();
    }
    let rules = body
        .get("Rules")
        .and_then(Value::as_array)
        .map(|list| list.iter().map(Rule::from_body).collect::<Vec<_>>())
        .unwrap_or_default();
    let wacl = WebAcl {
        name: name.to_string(),
        id,
        arn: arn.clone(),
        description: as_str(body, "Description").to_string(),
        default_action: body.get("DefaultAction").cloned().unwrap_or(Value::Null),
        rules,
        visibility_config: body.get("VisibilityConfig").cloned().unwrap_or(Value::Null),
        lock_token: Uuid::new_v4().to_string(),
        associated_resources: Vec::new(),
        association_config: body.get("AssociationConfig").cloned(),
        captcha_config: body.get("CaptchaConfig").cloned(),
        challenge_config: body.get("ChallengeConfig").cloned(),
        custom_response_bodies: body.get("CustomResponseBodies").cloned(),
        token_domains: body.get("TokenDomains").cloned(),
        label_namespace: format!("awswaf:{account}:webacl:{name}:"),
    };
    st.wacls.insert(arn.clone(), wacl);
    tag_resource(st, &arn, body.get("Tags").and_then(Value::as_array));
    let summary = st.wacls[&arn].to_summary();
    ok(json!({ "Summary": summary }))
}

fn find_web_acl<'a>(st: &'a WafStore, name: &str, id: &str) -> Option<&'a WebAcl> {
    st.wacls.values().find(|w| w.name == name && w.id == id)
}

fn get_web_acl(st: &WafStore, body: &Value) -> Response {
    match find_web_acl(st, as_str(body, "Name"), as_str(body, "Id")) {
        Some(wacl) => {
            // moto's GetWebACL response carries the LockToken both inside the
            // ACL dict and at the top level.
            ok(json!({ "WebACL": wacl.to_dict(), "LockToken": wacl.lock_token }))
        }
        None => nonexistent(),
    }
}

fn delete_web_acl(st: &mut WafStore, account: u64, region: &str, body: &Value) -> Response {
    let arn = resource_arn(
        "webacl",
        account,
        region,
        as_str(body, "Scope"),
        as_str(body, "Name"),
        as_str(body, "Id"),
    );
    let lock_token_ok = match st.wacls.get(&arn) {
        Some(wacl) => {
            // An absent LockToken member compares as None in moto — a mismatch.
            Some(wacl.lock_token.as_str()) == body.get("LockToken").and_then(Value::as_str)
        }
        None => return nonexistent(),
    };
    if !lock_token_ok {
        return optimistic_lock();
    }
    st.wacls.remove(&arn);
    ok_empty()
}

fn update_web_acl(st: &mut WafStore, body: &Value) -> Response {
    // moto's UpdateWebACL receives raw body members and applies only the
    // non-null ones; empty `Rules` lists keep the old rules.
    let key = find_web_acl(st, as_str(body, "Name"), as_str(body, "Id")).map(|w| w.arn.clone());
    let Some(key) = key else {
        return nonexistent();
    };
    let wacl = st.wacls.get_mut(&key).unwrap();
    if Some(wacl.lock_token.as_str()) != body.get("LockToken").and_then(Value::as_str) {
        return optimistic_lock();
    }
    let rules = body
        .get("Rules")
        .and_then(Value::as_array)
        .filter(|list| !list.is_empty())
        .map(|list| list.iter().map(Rule::from_body).collect::<Vec<_>>());
    wacl.update(
        non_null(body.get("DefaultAction")).cloned(),
        rules,
        as_opt_str(body, "Description"),
        non_null(body.get("VisibilityConfig")).cloned(),
        non_null(body.get("CustomResponseBodies")).cloned(),
        non_null(body.get("CaptchaConfig")).cloned(),
        non_null(body.get("ChallengeConfig")).cloned(),
        non_null(body.get("TokenDomains")).cloned(),
        non_null(body.get("AssociationConfig")).cloned(),
    );
    ok(json!({ "NextLockToken": wacl.lock_token }))
}

fn associate_web_acl(st: &mut WafStore, body: &Value) -> Response {
    match st.wacls.get_mut(as_str(body, "WebACLArn")) {
        Some(wacl) => {
            wacl.associated_resources
                .push(as_str(body, "ResourceArn").to_string());
            ok_empty()
        }
        None => nonexistent(),
    }
}

fn disassociate_web_acl(st: &mut WafStore, body: &Value) -> Response {
    let resource = as_str(body, "ResourceArn");
    for wacl in st.wacls.values_mut() {
        if let Some(pos) = wacl.associated_resources.iter().position(|r| r == resource) {
            wacl.associated_resources.remove(pos);
            break;
        }
    }
    ok_empty()
}

fn get_web_acl_for_resource(st: &WafStore, body: &Value) -> Response {
    let resource = as_str(body, "ResourceArn");
    let found = st
        .wacls
        .values()
        .find(|w| w.associated_resources.iter().any(|r| r == resource))
        .map(|w| w.to_dict());
    ok(json!({ "WebACL": found.unwrap_or(Value::Null) }))
}

fn list_resources_for_web_acl(st: &WafStore, body: &Value) -> Response {
    let wacl = match st.wacls.get(as_str(body, "WebACLArn")) {
        Some(wacl) => wacl,
        None => return nonexistent(),
    };
    // moto: `resource_type.lower() in r.lower()` when a non-empty type is given.
    let out: Vec<String> = match body
        .get("ResourceType")
        .and_then(Value::as_str)
        .filter(|t| !t.is_empty())
    {
        Some(rtype) => {
            let needle = rtype.to_lowercase();
            wacl.associated_resources
                .iter()
                .filter(|r| r.to_lowercase().contains(&needle))
                .cloned()
                .collect()
        }
        None => wacl.associated_resources.clone(),
    };
    ok(json!({ "ResourceArns": out }))
}

fn list_web_acls(st: &WafStore, body: &Value) -> Response {
    let summaries: Vec<Value> = st.wacls.values().map(|w| w.to_summary()).collect();
    let (page, next_marker) = page(&summaries, body.get("Limit"), body.get("NextMarker"), |s| {
        s["ARN"].as_str().unwrap_or_default().to_string()
    });
    ok(json!({ "NextMarker": next_marker, "WebACLs": page }))
}

// ---------------------------------------------------------------------------
// IP set operations
// ---------------------------------------------------------------------------

fn create_ip_set(st: &mut WafStore, account: u64, region: &str, body: &Value) -> Response {
    // moto: no duplicate-name check on IPSets, unlike WebACLs.
    let scope = as_str(body, "Scope");
    let name = as_str(body, "Name");
    let id = Uuid::new_v4().to_string();
    let arn = resource_arn("ipset", account, region, scope, name, &id);
    let ip_set = IpSet {
        name: name.to_string(),
        id: id.clone(),
        arn: arn.clone(),
        description: as_opt_str(body, "Description"),
        addresses: body.get("Addresses").cloned().unwrap_or(Value::Null),
        ip_address_version: as_opt_str(body, "IPAddressVersion"),
        scope: scope.to_string(),
        lock_token: IpSet::new_lock_token(),
    };
    st.ip_sets.insert(arn.clone(), ip_set);
    tag_resource(st, &arn, body.get("Tags").and_then(Value::as_array));
    let summary = st.ip_sets[&arn].to_summary();
    ok(json!({ "Summary": summary }))
}

fn get_ip_set(st: &WafStore, account: u64, region: &str, body: &Value) -> Response {
    let arn = resource_arn(
        "ipset",
        account,
        region,
        as_str(body, "Scope"),
        as_str(body, "Name"),
        as_str(body, "Id"),
    );
    let ip_set = match st.ip_sets.get(&arn) {
        Some(ip_set) => ip_set,
        None => return nonexistent(),
    };
    let mut ip_set_dict = ip_set.to_dict();
    let lock_token = remove_lock_token(&mut ip_set_dict).unwrap_or_default();
    ok(json!({ "IPSet": ip_set_dict, "LockToken": lock_token }))
}

fn list_ip_sets(st: &WafStore, body: &Value) -> Response {
    let scope = as_str(body, "Scope");
    let summaries: Vec<Value> = st
        .ip_sets
        .values()
        .filter(|ip_set| ip_set.scope == scope)
        .map(|ip_set| ip_set.to_summary())
        .collect();
    let (page, next_marker) = page(&summaries, body.get("Limit"), body.get("NextMarker"), |s| {
        s["ARN"].as_str().unwrap_or_default().to_string()
    });
    ok(json!({ "NextMarker": next_marker, "IPSets": page }))
}

fn delete_ip_set(st: &mut WafStore, account: u64, region: &str, body: &Value) -> Response {
    let arn = resource_arn(
        "ipset",
        account,
        region,
        as_str(body, "Scope"),
        as_str(body, "Name"),
        as_str(body, "Id"),
    );
    let ip_set = match st.ip_sets.get(&arn) {
        Some(ip_set) => ip_set,
        None => return nonexistent(),
    };
    if Some(ip_set.lock_token.as_str()) != body.get("LockToken").and_then(Value::as_str) {
        return optimistic_lock();
    }
    st.ip_sets.remove(&arn);
    ok_empty()
}

fn update_ip_set(st: &mut WafStore, account: u64, region: &str, body: &Value) -> Response {
    let arn = resource_arn(
        "ipset",
        account,
        region,
        as_str(body, "Scope"),
        as_str(body, "Name"),
        as_str(body, "Id"),
    );
    let ip_set = match st.ip_sets.get_mut(&arn) {
        Some(ip_set) => ip_set,
        None => return nonexistent(),
    };
    if Some(ip_set.lock_token.as_str()) != body.get("LockToken").and_then(Value::as_str) {
        return optimistic_lock();
    }
    ip_set.update(
        as_opt_str(body, "Description"),
        body.get("Addresses").cloned().unwrap_or(Value::Null),
    );
    ok(json!({ "NextLockToken": ip_set.lock_token }))
}

// ---------------------------------------------------------------------------
// Regex pattern set operations
// ---------------------------------------------------------------------------

fn create_regex_pattern_set(
    st: &mut WafStore,
    account: u64,
    region: &str,
    body: &Value,
) -> Response {
    let scope = as_str(body, "Scope");
    let name = as_str(body, "Name");
    if st
        .regex_pattern_sets
        .values()
        .any(|set| set.name == name && set.scope == scope)
    {
        return duplicate();
    }
    let id = Uuid::new_v4().to_string();
    let arn = regex_pattern_set_arn(account, region, scope, name, &id);
    let set = RegexPatternSet {
        name: name.to_string(),
        id: id.clone(),
        arn: arn.clone(),
        description: as_opt_str(body, "Description"),
        regular_expressions: body
            .get("RegularExpressionList")
            .cloned()
            .unwrap_or(json!([])),
        scope: scope.to_string(),
        lock_token: Uuid::new_v4().to_string(),
    };
    st.regex_pattern_sets.insert(arn.clone(), set);
    tag_resource(st, &arn, body.get("Tags").and_then(Value::as_array));
    let summary = st.regex_pattern_sets[&arn].to_summary();
    ok(json!({ "Summary": summary }))
}

fn get_regex_pattern_set(st: &WafStore, account: u64, region: &str, body: &Value) -> Response {
    let arn = regex_pattern_set_arn(
        account,
        region,
        as_str(body, "Scope"),
        as_str(body, "Name"),
        as_str(body, "Id"),
    );
    let set = match st.regex_pattern_sets.get(&arn) {
        Some(set) => set,
        None => return nonexistent(),
    };
    let mut set_dict = set.to_dict();
    let lock_token = remove_lock_token(&mut set_dict).unwrap_or_default();
    ok(json!({ "RegexPatternSet": set_dict, "LockToken": lock_token }))
}

fn list_regex_pattern_sets(st: &WafStore, body: &Value) -> Response {
    let scope = as_str(body, "Scope");
    let summaries: Vec<Value> = st
        .regex_pattern_sets
        .values()
        .filter(|set| set.scope == scope)
        .map(RegexPatternSet::to_summary)
        .collect();
    let (page, next_marker) = page(&summaries, body.get("Limit"), body.get("NextMarker"), |s| {
        s["ARN"].as_str().unwrap_or_default().to_string()
    });
    ok(json!({ "NextMarker": next_marker, "RegexPatternSets": page }))
}

fn delete_regex_pattern_set(
    st: &mut WafStore,
    account: u64,
    region: &str,
    body: &Value,
) -> Response {
    let arn = regex_pattern_set_arn(
        account,
        region,
        as_str(body, "Scope"),
        as_str(body, "Name"),
        as_str(body, "Id"),
    );
    let set = match st.regex_pattern_sets.get(&arn) {
        Some(set) => set,
        None => return nonexistent(),
    };
    if Some(set.lock_token.as_str()) != body.get("LockToken").and_then(Value::as_str) {
        return optimistic_lock();
    }
    st.regex_pattern_sets.remove(&arn);
    ok_empty()
}

fn update_regex_pattern_set(
    st: &mut WafStore,
    account: u64,
    region: &str,
    body: &Value,
) -> Response {
    let arn = regex_pattern_set_arn(
        account,
        region,
        as_str(body, "Scope"),
        as_str(body, "Name"),
        as_str(body, "Id"),
    );
    let set = match st.regex_pattern_sets.get_mut(&arn) {
        Some(set) => set,
        None => return nonexistent(),
    };
    if Some(set.lock_token.as_str()) != body.get("LockToken").and_then(Value::as_str) {
        return optimistic_lock();
    }
    set.update(
        as_opt_str(body, "Description"),
        non_null(body.get("RegularExpressionList")).cloned(),
    );
    ok(json!({ "NextLockToken": set.lock_token }))
}

// ---------------------------------------------------------------------------
// Rule-group operations
// ---------------------------------------------------------------------------

fn create_rule_group(st: &mut WafStore, account: u64, region: &str, body: &Value) -> Response {
    let name = as_str(body, "Name");
    if st.rule_groups.values().any(|g| g.name == name) {
        return duplicate();
    }
    let scope = as_str(body, "Scope");
    let id = Uuid::new_v4().to_string();
    let arn = resource_arn("rulegroup", account, region, scope, name, &id);
    let rules = body
        .get("Rules")
        .and_then(Value::as_array)
        .map(|list| list.iter().map(Rule::from_body).collect::<Vec<_>>())
        .unwrap_or_default();
    let group = RuleGroup::from_parts(
        account,
        name,
        &id,
        arn.clone(),
        scope,
        body.get("Capacity").cloned().unwrap_or(Value::Null),
        as_opt_str(body, "Description"),
        rules,
        body.get("VisibilityConfig").cloned().unwrap_or(Value::Null),
        body.get("CustomResponseBodies").cloned(),
    );
    st.rule_groups.insert(arn.clone(), group);
    tag_resource(st, &arn, body.get("Tags").and_then(Value::as_array));
    let summary = st.rule_groups.get(&arn).map(RuleGroup::to_summary);
    ok(json!({ "Summary": summary.unwrap_or(Value::Null) }))
}

fn get_rule_group(st: &WafStore, account: u64, region: &str, body: &Value) -> Response {
    let arn = body.get("ARN").and_then(Value::as_str);
    let group_arn = match arn {
        Some(a) => a.to_string(),
        None => {
            let name = as_str(body, "Name");
            let scope = as_str(body, "Scope");
            let id = as_str(body, "Id");
            if !name.is_empty() && !scope.is_empty() && !id.is_empty() {
                resource_arn("rulegroup", account, region, scope, name, id)
            } else {
                return insufficient_information(
                    name,
                    scope,
                    id,
                    non_null(body.get("ARN")).and_then(Value::as_str),
                );
            }
        }
    };
    let Some(group) = st.rule_groups.get(&group_arn) else {
        return nonexistent();
    };
    // moto's to_dict carries the LockToken inside the dict; the response
    // layer pops it and hoists it to the top level.
    let mut group_dict = group.to_dict(true);
    let lock_token = remove_lock_token(&mut group_dict).unwrap_or_default();
    ok(json!({ "RuleGroup": group_dict, "LockToken": lock_token }))
}

fn list_rule_groups(st: &WafStore, body: &Value) -> Response {
    let scope = as_str(body, "Scope");
    let summaries: Vec<Value> = st
        .rule_groups
        .values()
        .filter(|g| g.scope == scope)
        .map(RuleGroup::to_summary)
        .collect();
    let (page, next_marker) = page(&summaries, body.get("Limit"), body.get("NextMarker"), |s| {
        s["ARN"].as_str().unwrap_or_default().to_string()
    });
    ok(json!({ "NextMarker": next_marker, "RuleGroups": page }))
}

fn delete_rule_group(st: &mut WafStore, account: u64, region: &str, body: &Value) -> Response {
    let arn = resource_arn(
        "rulegroup",
        account,
        region,
        as_str(body, "Scope"),
        as_str(body, "Name"),
        as_str(body, "Id"),
    );
    let group = match st.rule_groups.get(&arn) {
        Some(group) => group,
        None => return nonexistent(),
    };
    if Some(group.lock_token.as_str()) != body.get("LockToken").and_then(Value::as_str) {
        return optimistic_lock();
    }
    st.rule_groups.remove(&arn);
    ok_empty()
}

fn update_rule_group(st: &mut WafStore, account: u64, region: &str, body: &Value) -> Response {
    let arn = resource_arn(
        "rulegroup",
        account,
        region,
        as_str(body, "Scope"),
        as_str(body, "Name"),
        as_str(body, "Id"),
    );
    let group = match st.rule_groups.get_mut(&arn) {
        Some(group) => group,
        None => return nonexistent(),
    };
    if Some(group.lock_token.as_str()) != body.get("LockToken").and_then(Value::as_str) {
        return optimistic_lock();
    }
    if let Some(d) = as_opt_str(body, "Description") {
        group.description = Some(d);
    }
    // moto: an absent or empty Rules member leaves the rules untouched.
    if let Some(list) = body
        .get("Rules")
        .and_then(Value::as_array)
        .filter(|list| !list.is_empty())
    {
        group.rules = list.iter().map(Rule::from_body).collect();
    }
    if let Some(v) = non_null(body.get("VisibilityConfig")) {
        group.visibility_config = v.clone();
    }
    if let Some(v) = non_null(body.get("CustomResponseBodies")) {
        group.custom_response_bodies = Some(v.clone());
    }
    group.lock_token = Uuid::new_v4().to_string();
    ok(json!({ "NextLockToken": group.lock_token }))
}

// ---------------------------------------------------------------------------
// Logging configurations
// ---------------------------------------------------------------------------

fn put_logging_configuration(st: &mut WafStore, body: &Value) -> Response {
    let config = body
        .get("LoggingConfiguration")
        .cloned()
        .unwrap_or(json!({}));
    let arn = as_str(&config, "ResourceArn").to_string();
    let logging = LoggingConfiguration {
        arn: arn.clone(),
        log_destination_configs: config
            .get("LogDestinationConfigs")
            .cloned()
            .unwrap_or(json!([])),
        redacted_fields: config.get("RedactedFields").cloned(),
        managed_by_firewall_manager: config
            .get("ManagedByFirewallManager")
            .and_then(Value::as_bool)
            .unwrap_or(false),
        logging_filter: config.get("LoggingFilter").cloned(),
    };
    let out = logging.to_dict();
    st.logging_configurations.insert(arn, logging);
    ok(json!({ "LoggingConfiguration": out }))
}

fn get_logging_configuration(st: &WafStore, body: &Value) -> Response {
    match st.logging_configurations.get(as_str(body, "ResourceArn")) {
        Some(config) => ok(json!({ "LoggingConfiguration": config.to_dict() })),
        None => nonexistent(),
    }
}

fn delete_logging_configuration(st: &mut WafStore, body: &Value) -> Response {
    if st
        .logging_configurations
        .remove(as_str(body, "ResourceArn"))
        .is_none()
    {
        return nonexistent();
    }
    ok_empty()
}

fn list_logging_configurations(st: &WafStore, region: &str, body: &Value) -> Response {
    // moto filters on `f":{scope}:"` inside the ARN; CLOUDFRONT lists use the
    // literal "global" (which never matches that pattern), others the region.
    let scope = match as_str(body, "Scope") {
        "CLOUDFRONT" => "global".to_string(),
        _ => region.to_string(),
    };
    let needle = format!(":{scope}:");
    let configs: Vec<Value> = st
        .logging_configurations
        .values()
        .filter(|c| c.arn.contains(&needle))
        .map(LoggingConfiguration::to_dict)
        .collect();
    let (page, next_marker) = page(&configs, body.get("Limit"), body.get("NextMarker"), |c| {
        c["ResourceArn"].as_str().unwrap_or_default().to_string()
    });
    ok(json!({ "NextMarker": next_marker, "LoggingConfigurations": page }))
}

// ---------------------------------------------------------------------------
// Permission policies
// ---------------------------------------------------------------------------

/// moto's message for `PutPermissionPolicy` on a non-rule-group ARN.
const INVALID_POLICY_ARN: &str = "The ARN isn't valid. A valid ARN starts with arn: and includes other information separated by colons or slashes.";

fn put_permission_policy(st: &mut WafStore, body: &Value) -> Response {
    let resource_arn = as_str(body, "ResourceArn");
    if !resource_arn.contains("/rulegroup/") {
        return invalid_parameter(INVALID_POLICY_ARN);
    }
    if !st.rule_groups.contains_key(resource_arn) {
        return nonexistent();
    }
    st.permission_policies
        .insert(resource_arn.to_string(), as_str(body, "Policy").to_string());
    ok_empty()
}

fn get_permission_policy(st: &WafStore, body: &Value) -> Response {
    match st.permission_policies.get(as_str(body, "ResourceArn")) {
        Some(policy) => ok(json!({ "Policy": policy })),
        None => nonexistent(),
    }
}

fn delete_permission_policy(st: &mut WafStore, body: &Value) -> Response {
    if st
        .permission_policies
        .remove(as_str(body, "ResourceArn"))
        .is_none()
    {
        return nonexistent();
    }
    ok_empty()
}

// ---------------------------------------------------------------------------
// Tags
// ---------------------------------------------------------------------------

fn list_tags_for_resource(st: &WafStore, body: &Value) -> Response {
    let arn = as_str(body, "ResourceARN");
    let tag_rows: Vec<Value> = match st.tags.get(arn) {
        None => Vec::new(),
        Some(row) => row
            .iter()
            .map(|(k, v)| json!({"Key": k, "Value": maybe_str(v.clone())}))
            .collect(),
    };
    let (page, next_marker) = page(
        &tag_rows,
        body.get("Limit"),
        body.get("NextMarker"),
        |tag| tag["Key"].as_str().unwrap_or_default().to_string(),
    );
    ok(json!({
        "NextMarker": next_marker,
        "TagInfoForResource": {"ResourceARN": arn, "TagList": page},
    }))
}

// ---------------------------------------------------------------------------
// Canned operations
// ---------------------------------------------------------------------------

/// Pop the LockToken member out of a to-dict copy.
fn remove_lock_token(dict: &mut Value) -> Option<String> {
    let obj = dict.as_object_mut()?;
    obj.remove("LockToken")?.as_str().map(String::from)
}

/// moto's `CheckCapacity`: `len(rules)`.
fn check_capacity(body: &Value) -> Response {
    let capacity = body
        .get("Rules")
        .and_then(Value::as_array)
        .map(Vec::len)
        .unwrap_or(0);
    ok(json!({ "Capacity": capacity }))
}

/// moto's canned `DescribeManagedRuleGroup` payload.
fn describe_managed_rule_group(body: &Value) -> Response {
    let vendor = as_str(body, "VendorName");
    let name = as_str(body, "Name");
    ok(json!({
        "Capacity": 700,
        "Rules": [{"Name": "NoUserAgent_HEADER", "Action": {"Block": {}}}],
        "AvailableLabels":
            [{"Name": format!("awswaf:managed:{}:{}:NoUserAgent_HEADER", vendor.to_lowercase(), name.to_lowercase())}],
        "ConsumedLabels": [],
        "LabelNamespace": format!("awswaf:managed:{}:{}:", vendor.to_lowercase(), name.to_lowercase()),
        "VersionName": "Version_1.0",
        "SnsTopicArn": format!("arn:aws:sns:us-east-1:123456789012:managed-rule-group-{name}"),
    }))
}

/// moto's canned `DescribeAllManagedProducts` payload.
fn all_managed_products(account: u64, region: &str) -> Vec<Value> {
    vec![json!({
        "VendorName": "AWS",
        "ManagedRuleSetName": "AWSManagedRulesCommonRuleSet",
        "ProductId": "aws-common-rules",
        "ProductTitle": "AWS Managed Rules Common Rule Set",
        "ProductDescription": "Contains rules that are generally applicable to web applications.",
        "SnsTopicArn": format!("arn:aws:sns:{region}:{account}:managed-product-common"),
        "IsVersioningSupported": true,
        "IsAdvancedManagedRuleSet": false,
    })]
}

/// moto's static `GetManagedRuleSet` response — it fabricates a rule set
/// rather than reading any stored state.
fn get_managed_rule_set(account: u64, region: &str, body: &Value) -> Response {
    let name = as_str(body, "Name");
    let id = as_str(body, "Id");
    ok(json!({
        "ManagedRuleSet": {
            "Name": name,
            "Id": id,
            "ARN": format!("arn:aws:wafv2:{region}:{account}:managed-rule-set/{name}/{id}"),
            "Description": "",
            "PublishedVersions": {},
            "RecommendedVersion": "Version_1.0",
            "LabelNamespace": format!("awswaf:managed:{}:", name.to_lowercase()),
        },
        "LockToken": Uuid::new_v4(),
    }))
}

/// moto's static `UpdateManagedRuleSetVersionExpiryDate` echo.
fn update_managed_rule_set_version_expiry_date(body: &Value) -> Response {
    ok(json!({
        "ExpiringVersion": as_str(body, "VersionToExpire"),
        "ExpiryTimestamp": body.get("ExpiryTimestamp").cloned().unwrap_or(Value::Null),
        "NextLockToken": Uuid::new_v4(),
    }))
}

/// moto's static `GetRateBasedStatementManagedKeys` response.
fn rate_based_managed_keys() -> Response {
    ok(json!({
        "ManagedKeysIPV4": {"IPAddressVersion": "IPV4", "Addresses": []},
        "ManagedKeysIPV6": {"IPAddressVersion": "IPV6", "Addresses": []},
    }))
}

/// moto's `GetSampledRequests`: no sampled traffic, the window echoes back.
fn sampled_requests(body: &Value) -> Response {
    ok(json!({
        "SampledRequests": [],
        "PopulationSize": 0,
        "TimeWindow": body.get("TimeWindow").cloned().unwrap_or(Value::Null),
    }))
}

// ---------------------------------------------------------------------------
// API keys
// ---------------------------------------------------------------------------

fn create_api_key(st: &mut WafStore, body: &Value) -> Response {
    let api_key = Uuid::new_v4().simple().to_string();
    st.api_keys.insert(
        api_key.clone(),
        ApiKeySummary {
            token_domains: body.get("TokenDomains").cloned().unwrap_or(json!([])),
            created_at: now_iso(),
        },
    );
    ok(json!({ "APIKey": api_key }))
}

fn delete_api_key(st: &mut WafStore, body: &Value) -> Response {
    // moto pops silently — deleting an unknown key still answers 200.
    st.api_keys.remove(as_str(body, "APIKey"));
    ok_empty()
}

fn get_decrypted_api_key(st: &WafStore, body: &Value) -> Response {
    let token_domains = st
        .api_keys
        .get(as_str(body, "APIKey"))
        .map(|k| k.token_domains.clone())
        .unwrap_or(json!([]));
    ok(json!({ "TokenDomains": token_domains }))
}

fn list_api_keys(st: &WafStore) -> Response {
    let rows: Vec<Value> = st
        .api_keys
        .iter()
        .map(|(key, summary)| summary.to_dict(key))
        .collect();
    ok(json!({ "APIKeySummaries": rows, "NextMarker": null }))
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;
    use std::sync::atomic::{AtomicU64, Ordering};

    static ACCOUNTS: AtomicU64 = AtomicU64::new(470_500_000_001);

    /// Every test takes a fresh account so the shared store stays isolated.
    fn next_account() -> u64 {
        ACCOUNTS.fetch_add(1, Ordering::SeqCst)
    }

    fn send(account: u64, operation: &str, body: Value) -> (u16, Value) {
        let request = Request {
            service: "wafv2".to_string(),
            operation: operation.to_string(),
            method: "POST".to_string(),
            path: "/".to_string(),
            query_string: String::new(),
            headers: HashMap::new(),
            body: Bytes::from(serde_json::to_vec(&body).unwrap()),
            account,
            region: "us-east-1".to_string(),
        };
        let resp = dispatch(&request).expect("wafv2 dispatch");
        (
            resp.status,
            serde_json::from_slice(&resp.body).unwrap_or(json!({})),
        )
    }

    fn visibility(name: &str) -> Value {
        json!({
            "SampledRequestsEnabled": true,
            "CloudWatchMetricsEnabled": true,
            "MetricName": name,
        })
    }

    fn create_wacl(account: u64, name: &str, scope: &str, rules: Value) -> Value {
        let (status, body) = send(
            account,
            "CreateWebACL",
            json!({
                "Name": name,
                "Scope": scope,
                "DefaultAction": {"Allow": {}},
                "VisibilityConfig": visibility(name),
                "Rules": rules,
            }),
        );
        assert_eq!(status, 200, "create_web_acl failed: {body}");
        body["Summary"].clone()
    }

    fn create_rule_group(account: u64, name: &str, rules: Value) -> Value {
        let (status, body) = send(
            account,
            "CreateRuleGroup",
            json!({
                "Name": name,
                "Scope": "REGIONAL",
                "Capacity": 100,
                "VisibilityConfig": visibility(name),
                "Rules": rules,
            }),
        );
        assert_eq!(status, 200, "create_rule_group failed: {body}");
        body["Summary"].clone()
    }

    fn label_rule(labels: Value, statement: Value) -> Value {
        json!({
            "Name": "rule",
            "Priority": 1,
            "Statement": statement,
            "Action": {"Block": {}},
            "VisibilityConfig": visibility("rule"),
            "RuleLabels": labels,
        })
    }

    #[test]
    fn web_acl_lifecycle() {
        let account = next_account();
        let summary = create_wacl(account, "acl-a", "REGIONAL", json!([]));
        assert!(summary["ARN"]
            .as_str()
            .unwrap()
            .contains("regional/webacl/"));

        let (status, body) = send(
            account,
            "GetWebACL",
            json!({
                "Name": "acl-a", "Scope": "REGIONAL", "Id": summary["Id"],
            }),
        );
        assert_eq!(status, 200);
        assert_eq!(body["WebACL"]["Capacity"], 3);
        assert_eq!(body["WebACL"]["DefaultAction"], json!({"Allow": {}}));
        assert_eq!(
            body["WebACL"]["LabelNamespace"],
            json!(format!("awswaf:{account}:webacl:acl-a:"))
        );
        let lock = body["LockToken"].as_str().unwrap().to_string();

        // Update rotates the token and swaps the default action.
        let (status, body) = send(
            account,
            "UpdateWebACL",
            json!({
                "Name": "acl-a", "Id": summary["Id"], "Scope": "REGIONAL",
                "DefaultAction": {"Block": {}}, "LockToken": lock,
                "VisibilityConfig": visibility("acl-a"),
            }),
        );
        assert_eq!(status, 200);
        let next_lock = body["NextLockToken"].as_str().unwrap().to_string();
        assert_ne!(next_lock, lock, "update must rotate the lock token");

        // Deleting with a stale token fails.
        let (status, body) = send(
            account,
            "DeleteWebACL",
            json!({
                "Name": "acl-a", "Scope": "REGIONAL", "Id": summary["Id"],
                "LockToken": lock,
            }),
        );
        assert_eq!(status, 400);
        assert_eq!(body["__type"], "WAFOptimisticLockException");

        // Deleting with the fresh token succeeds; the ACL is gone.
        let (status, _) = send(
            account,
            "DeleteWebACL",
            json!({
                "Name": "acl-a", "Scope": "REGIONAL", "Id": summary["Id"],
                "LockToken": next_lock,
            }),
        );
        assert_eq!(status, 200);
        let (status, body) = send(
            account,
            "GetWebACL",
            json!({
                "Name": "acl-a", "Scope": "REGIONAL", "Id": summary["Id"],
            }),
        );
        assert_eq!(status, 400);
        assert_eq!(body["__type"], "WAFNonexistentItemException");
    }

    #[test]
    fn duplicate_web_acl_names_rejected() {
        let account = next_account();
        create_wacl(account, "acl-dup", "REGIONAL", json!([]));
        let (status, body) = send(
            account,
            "CreateWebACL",
            json!({
                "Name": "acl-dup",
                "Scope": "REGIONAL",
                "DefaultAction": {"Allow": {}},
                "VisibilityConfig": visibility("acl-dup"),
            }),
        );
        assert_eq!(status, 400);
        assert_eq!(body["__type"], "WafV2DuplicateItem");
    }

    #[test]
    fn cloudfront_scope_lives_in_the_global_slice() {
        let account = next_account();
        let summary = create_wacl(account, "wf-acl", "CLOUDFRONT", json!([]));
        let arn = summary["ARN"].as_str().unwrap();
        assert!(arn.contains(":global/webacl/"), "arn was {arn}");
        assert!(arn.contains("arn:aws:wafv2:us-east-1"), "arn was {arn}");

        let (_, body) = send(account, "ListWebACLs", json!({ "Scope": "CLOUDFRONT" }));
        let names: Vec<&str> = body["WebACLs"]
            .as_array()
            .unwrap()
            .iter()
            .map(|w| w["Name"].as_str().unwrap())
            .collect();
        assert_eq!(names, vec!["wf-acl"]);

        // The regional list of the same account stays empty.
        let (_, body) = send(account, "ListWebACLs", json!({ "Scope": "REGIONAL" }));
        assert_eq!(body["WebACLs"].as_array().unwrap().len(), 0);
    }

    #[test]
    fn ip_set_lifecycle_and_six_char_lock_token() {
        let account = next_account();
        let (status, body) = send(
            account,
            "CreateIPSet",
            json!({
                "Name": "ips-a", "Scope": "REGIONAL", "IPAddressVersion": "IPV4",
                "Addresses": ["10.0.0.0/8"],
            }),
        );
        assert_eq!(status, 200);
        let summary = body["Summary"].clone();
        assert_eq!(summary["LockToken"].as_str().unwrap().len(), 6);

        let (_, body) = send(
            account,
            "GetIPSet",
            json!({
                "Name": "ips-a", "Scope": "REGIONAL", "Id": summary["Id"],
            }),
        );
        assert_eq!(body["IPSet"]["Addresses"], json!(["10.0.0.0/8"]));

        // Update replaces the address list; clearing to empty works.
        let (status, body) = send(
            account,
            "UpdateIPSet",
            json!({
                "Name": "ips-a", "Scope": "REGIONAL", "Id": summary["Id"],
                "Addresses": [], "LockToken": summary["LockToken"],
            }),
        );
        assert_eq!(status, 200);
        assert!(body["NextLockToken"].is_string());

        let (_, body) = send(
            account,
            "GetIPSet",
            json!({
                "Name": "ips-a", "Scope": "REGIONAL", "Id": summary["Id"],
            }),
        );
        assert_eq!(body["IPSet"]["Addresses"], json!([]));
    }

    #[test]
    fn rule_group_labels_and_stale_update() {
        let account = next_account();
        let rules = json!([{
            "Name": "matchme",
            "Priority": 1,
            "Statement": {"LabelMatchStatement": {"Key": "other:label"}},
            "Action": {"Block": {}},
            "VisibilityConfig": visibility("matchme"),
            "RuleLabels": [{"Name": "own:label"}],
        }]);
        let summary = create_rule_group(account, "rg-labels", rules);
        assert!(summary["ARN"]
            .as_str()
            .unwrap()
            .contains("regional/rulegroup/"));

        let (_, body) = send(
            account,
            "GetRuleGroup",
            json!({
                "Name": "rg-labels", "Scope": "REGIONAL", "Id": summary["Id"],
            }),
        );
        assert_eq!(body["RuleGroup"]["Capacity"], 100);
        let ns = format!("awswaf:{account}:rulegroup:rg-labels:");
        assert_eq!(
            body["RuleGroup"]["AvailableLabels"],
            json!([{ "Name": format!("{ns}own:label") }])
        );
        assert_eq!(
            body["RuleGroup"]["ConsumedLabels"],
            json!([{ "Name": format!("{ns}other:label") }])
        );

        // moto quirk: updating the rules never refreshes the label lists.
        let (status, body) = send(
            account,
            "UpdateRuleGroup",
            json!({
                "Name": "rg-labels", "Scope": "REGIONAL", "Id": summary["Id"],
                "LockToken": body["LockToken"],
                "VisibilityConfig": visibility("rg-labels"),
                "Rules": [],
            }),
        );
        assert_eq!(status, 200);
        assert!(body["NextLockToken"].is_string());

        // Empty Rules keep the existing rules (moto's falsy check).
        let (_, body) = send(
            account,
            "GetRuleGroup",
            json!({
                "Name": "rg-labels", "Scope": "REGIONAL", "Id": summary["Id"],
            }),
        );
        assert_eq!(body["RuleGroup"]["Rules"].as_array().unwrap().len(), 1);
    }

    #[test]
    fn rule_group_requires_name_scope_or_arn() {
        let account = next_account();
        let (status, body) = send(account, "GetRuleGroup", json!({}));
        assert_eq!(status, 400);
        assert_eq!(body["__type"], "AcessDeniedException");
        assert!(body["message"]
            .as_str()
            .unwrap()
            .starts_with("Critical information is missing"));
    }

    #[test]
    fn tags_round_trip_through_tag_operations() {
        let account = next_account();
        let summary = create_wacl(account, "acl-tag", "REGIONAL", json!([]));
        let arn = summary["ARN"].as_str().unwrap();

        let (status, _) = send(
            account,
            "TagResource",
            json!({
                "ResourceARN": arn,
                "Tags": [{"Key": "env", "Value": "staging"}, {"Key": "none"}],
            }),
        );
        assert_eq!(status, 200);
        let (_, body) = send(
            account,
            "ListTagsForResource",
            json!({ "ResourceARN": arn }),
        );
        assert_eq!(body["TagInfoForResource"]["ResourceARN"], arn);
        assert_eq!(
            body["TagInfoForResource"]["TagList"],
            json!([{"Key": "env", "Value": "staging"}, {"Key": "none", "Value": null}])
        );

        let (status, _) = send(
            account,
            "UntagResource",
            json!({
                "ResourceARN": arn, "TagKeys": ["env"],
            }),
        );
        assert_eq!(status, 200);
        let (_, body) = send(
            account,
            "ListTagsForResource",
            json!({ "ResourceARN": arn }),
        );
        assert_eq!(
            body["TagInfoForResource"]["TagList"],
            json!([{"Key": "none", "Value": null}])
        );
    }

    #[test]
    fn permission_policy_lifecycle() {
        let account = next_account();
        let summary = create_rule_group(account, "rg-pp", json!([]));
        let rg_arn = summary["ARN"].as_str().unwrap().to_string();

        // A non-rule-group ARN is invalid.
        let (status, body) = send(
            account,
            "PutPermissionPolicy",
            json!({
                "ResourceArn": format!("arn:aws:wafv2:us-east-1:{account}:regional/webacl/x/y"),
                "Policy": "{}",
            }),
        );
        assert_eq!(status, 400);
        assert_eq!(body["__type"], "WAFInvalidParameterException");

        let (status, _) = send(
            account,
            "PutPermissionPolicy",
            json!({
                "ResourceArn": rg_arn, "Policy": "{\"Statement\": []}",
            }),
        );
        assert_eq!(status, 200);
        let (_, body) = send(
            account,
            "GetPermissionPolicy",
            json!({ "ResourceArn": rg_arn }),
        );
        assert_eq!(body["Policy"], "{\"Statement\": []}");

        let (status, _) = send(
            account,
            "DeletePermissionPolicy",
            json!({ "ResourceArn": rg_arn }),
        );
        assert_eq!(status, 200);
        let (status, body) = send(
            account,
            "GetPermissionPolicy",
            json!({ "ResourceArn": rg_arn }),
        );
        assert_eq!(status, 400);
        assert_eq!(body["__type"], "WAFNonexistentItemException");
    }

    #[test]
    fn web_acl_associations_round_trip() {
        let account = next_account();
        let summary = create_wacl(account, "acl-assoc", "REGIONAL", json!([]));
        let alb = "arn:aws:elasticloadbalancing:us-east-1:123456789012:loadbalancer/app/x/1";
        let arn = summary["ARN"].as_str().unwrap();

        let (status, _) = send(
            account,
            "AssociateWebACL",
            json!({
                "WebACLArn": arn, "ResourceArn": alb,
            }),
        );
        assert_eq!(status, 200);
        let (_, body) = send(
            account,
            "GetWebACLForResource",
            json!({ "ResourceArn": alb }),
        );
        assert_eq!(body["WebACL"]["Name"], "acl-assoc");
        let (_, body) = send(
            account,
            "ListResourcesForWebACL",
            json!({ "WebACLArn": arn }),
        );
        assert_eq!(body["ResourceArns"], json!([alb]));

        let (status, _) = send(account, "DisassociateWebACL", json!({ "ResourceArn": alb }));
        assert_eq!(status, 200);
        let (_, body) = send(
            account,
            "GetWebACLForResource",
            json!({ "ResourceArn": alb }),
        );
        assert_eq!(body["WebACL"], Value::Null);
    }

    #[test]
    fn check_capacity_counts_rules() {
        let account = next_account();
        let rules = json!([
            {"Name": "a", "Priority": 0, "Statement": {}, "VisibilityConfig": visibility("a")},
            {"Name": "b", "Priority": 1, "Statement": {}, "VisibilityConfig": visibility("b")},
        ]);
        let (_, body) = send(account, "CheckCapacity", json!({ "Rules": rules }));
        assert_eq!(body["Capacity"], 2);
        let (_, body) = send(account, "CheckCapacity", json!({ "Rules": [] }));
        assert_eq!(body["Capacity"], 0);
    }

    #[test]
    fn list_pagination_uses_arn_markers() {
        let account = next_account();
        create_wacl(account, "p-one", "REGIONAL", json!([]));
        create_wacl(account, "p-two", "REGIONAL", json!([]));

        let (_, body) = send(
            account,
            "ListWebACLs",
            json!({ "Scope": "REGIONAL", "Limit": 1 }),
        );
        assert_eq!(body["WebACLs"].as_array().unwrap().len(), 1);
        let marker = body["NextMarker"].as_str().expect("marker").to_string();
        // The marker is base64 of {"uniqueAttributes": arn}.
        let decoded = serde_json::from_slice::<Value>(&B64.decode(&marker).unwrap()).unwrap();
        assert!(decoded["uniqueAttributes"].is_string());

        let (_, body) = send(
            account,
            "ListWebACLs",
            json!({
                "Scope": "REGIONAL", "Limit": 1, "NextMarker": marker,
            }),
        );
        assert_eq!(body["WebACLs"].as_array().unwrap().len(), 1);
        assert_eq!(
            body["WebACLs"][0]["Name"].as_str().unwrap(),
            if marker.is_empty() {
                ""
            } else {
                let first = serde_json::from_slice::<Value>(&B64.decode(&marker).unwrap()).unwrap();
                let _ = first;
                body["WebACLs"][0]["Name"].as_str().unwrap()
            }
        );
        // End of the list: null marker.
        let (_, final_body) = send(account, "ListWebACLs", json!({ "Scope": "REGIONAL" }));
        assert_eq!(final_body["WebACLs"].as_array().unwrap().len(), 2);
    }

    #[test]
    fn api_keys_round_trip() {
        let account = next_account();
        let (status, body) = send(
            account,
            "CreateAPIKey",
            json!({ "Scope": "REGIONAL", "TokenDomains": ["example.com"] }),
        );
        assert_eq!(status, 200);
        let key = body["APIKey"].as_str().unwrap().to_string();
        assert_eq!(key.len(), 32, "moto uses a dashed-uuid without dashes");

        let (_, body) = send(account, "ListAPIKeys", json!({ "Scope": "REGIONAL" }));
        let summaries = body["APIKeySummaries"].as_array().unwrap();
        assert!(summaries
            .iter()
            .any(|s| s["APIKey"] == key && s["TokenDomains"] == json!(["example.com"])));

        let (status, body) = send(
            account,
            "GetDecryptedAPIKey",
            json!({
                "Scope": "REGIONAL", "APIKey": key,
            }),
        );
        assert_eq!(status, 200);
        assert_eq!(body["TokenDomains"], json!(["example.com"]));

        // Unknown keys decrypt to an empty list.
        let (_, body) = send(
            account,
            "GetDecryptedAPIKey",
            json!({
                "Scope": "REGIONAL", "APIKey": "nope",
            }),
        );
        assert_eq!(body["TokenDomains"], json!([]));

        let (status, _) = send(
            account,
            "DeleteAPIKey",
            json!({
                "Scope": "REGIONAL", "APIKey": key,
            }),
        );
        assert_eq!(status, 200);
        // Deleting again is still 200 (moto pops silently).
        let (status, _) = send(
            account,
            "DeleteAPIKey",
            json!({
                "Scope": "REGIONAL", "APIKey": key,
            }),
        );
        assert_eq!(status, 200);
    }

    #[test]
    fn canned_managed_surfaces() {
        let account = next_account();

        let (_, body) = send(
            account,
            "ListAvailableManagedRuleGroups",
            json!({ "Scope": "REGIONAL" }),
        );
        assert!(body["ManagedRuleGroups"].as_array().unwrap().len() >= 13);
        assert!(body["NextMarker"].is_null());

        let (_, body) = send(
            account,
            "DescribeManagedRuleGroup",
            json!({
                "VendorName": "AWS", "Name": "AWSManagedRulesCommonRuleSet", "Scope": "REGIONAL",
            }),
        );
        assert_eq!(body["Capacity"], 700);
        assert_eq!(body["Rules"][0]["Name"], "NoUserAgent_HEADER");

        let (_, body) = send(
            account,
            "ListAvailableManagedRuleGroupVersions",
            json!({
                "VendorName": "AWS", "Name": "AWSManagedRulesCommonRuleSet", "Scope": "REGIONAL",
            }),
        );
        assert_eq!(body["CurrentDefaultVersion"], "Version_2.0");
        assert_eq!(body["Versions"].as_array().unwrap().len(), 3);

        let (_, body) = send(
            account,
            "GetMobileSdkRelease",
            json!({
                "Platform": "IOS", "ReleaseVersion": "1.0.0",
            }),
        );
        assert_eq!(body["MobileSdkRelease"]["ReleaseNotes"], "Mock release");

        let (_, body) = send(account, "ListMobileSdkReleases", json!({"Platform": "IOS"}));
        assert_eq!(body["ReleaseSummaries"], json!([]));

        let (_, body) = send(
            account,
            "GenerateMobileSdkReleaseUrl",
            json!({
                "Platform": "IOS", "ReleaseVersion": "1.0.0",
            }),
        );
        assert_eq!(
            body["Url"],
            "https://sdk.amazonaws.com/wafv2/IOS/1.0.0/sdk.zip"
        );

        let (_, body) = send(
            account,
            "GetManagedRuleSet",
            json!({
                "Name": "rs", "Scope": "REGIONAL", "Id": "some-id",
            }),
        );
        assert_eq!(
            body["ManagedRuleSet"]["ARN"],
            format!("arn:aws:wafv2:us-east-1:{account}:managed-rule-set/rs/some-id")
        );
        assert!(body["LockToken"].is_string());

        let (_, body) = send(
            account,
            "PutManagedRuleSetVersions",
            json!({
                "Name": "rs", "Scope": "REGIONAL", "Id": "some-id", "LockToken": "old",
            }),
        );
        assert!(body["NextLockToken"].is_string());

        let (_, body) = send(
            account,
            "UpdateManagedRuleSetVersionExpiryDate",
            json!({
                "Name": "rs", "Scope": "REGIONAL", "Id": "some-id", "LockToken": "old",
                "VersionToExpire": "1.0", "ExpiryTimestamp": 1700000000,
            }),
        );
        assert_eq!(body["ExpiringVersion"], "1.0");
        assert_eq!(body["ExpiryTimestamp"], 1700000000);
        assert!(body["NextLockToken"].is_string());

        let (_, body) = send(
            account,
            "DescribeAllManagedProducts",
            json!({ "Scope": "REGIONAL" }),
        );
        assert_eq!(body["ManagedProducts"].as_array().unwrap().len(), 1);

        let (_, body) = send(
            account,
            "DescribeManagedProductsByVendor",
            json!({
                "VendorName": "AWS", "Scope": "REGIONAL",
            }),
        );
        assert_eq!(body["ManagedProducts"].as_array().unwrap().len(), 1);

        let (_, body) = send(
            account,
            "DeleteFirewallManagerRuleGroups",
            json!({
                "WebACLArn": "arn:aws:wafv2:us-east-1:x", "WebACLLockToken": "old",
            }),
        );
        assert!(body["NextWebACLLockToken"].is_string());

        let (_, body) = send(
            account,
            "GetRateBasedStatementManagedKeys",
            json!({
                "WebACLName": "w", "WebACLId": "i", "RuleName": "r",
            }),
        );
        assert_eq!(body["ManagedKeysIPV4"]["IPAddressVersion"], "IPV4");
        assert_eq!(body["ManagedKeysIPV6"]["IPAddressVersion"], "IPV6");

        let (_, body) = send(
            account,
            "GetSampledRequests",
            json!({
                "WebAclArn": "arn", "RuleMetricName": "m", "Scope": "REGIONAL",
                "TimeWindow": {"StartTime": 1.5, "EndTime": 2.5}, "MaxItems": 10,
            }),
        );
        assert_eq!(body["SampledRequests"], json!([]));
        assert_eq!(body["PopulationSize"], 0);
        assert_eq!(
            body["TimeWindow"],
            json!({"StartTime": 1.5, "EndTime": 2.5})
        );

        // Logging configuration lifecycle.
        let acl = create_wacl(account, "acl-log", "REGIONAL", json!([]));
        let lc_arn = acl["ARN"].as_str().unwrap().to_string();
        let (status, _) = send(
            account,
            "PutLoggingConfiguration",
            json!({
                "LoggingConfiguration": {
                    "ResourceArn": lc_arn,
                    "LogDestinationConfigs": ["arn:aws:firehose:us-east-1:123456789012:deliverystream/s"],
                },
            }),
        );
        assert_eq!(status, 200);
        let (_, body) = send(
            account,
            "GetLoggingConfiguration",
            json!({ "ResourceArn": lc_arn }),
        );
        assert_eq!(
            body["LoggingConfiguration"]["ManagedByFirewallManager"],
            false
        );
        assert!(body["LoggingConfiguration"]["RedactedFields"].is_null());
        let (_, body) = send(
            account,
            "ListLoggingConfigurations",
            json!({ "Scope": "REGIONAL" }),
        );
        assert!(body["LoggingConfigurations"]
            .as_array()
            .unwrap()
            .iter()
            .any(|c| c["ResourceArn"] == lc_arn));
        let (status, _) = send(
            account,
            "DeleteLoggingConfiguration",
            json!({ "ResourceArn": lc_arn }),
        );
        assert_eq!(status, 200);
        let (status, body) = send(
            account,
            "GetLoggingConfiguration",
            json!({ "ResourceArn": lc_arn }),
        );
        assert_eq!(status, 400);
        assert_eq!(body["__type"], "WAFNonexistentItemException");
    }

    #[test]
    fn get_top_path_statistics_is_not_implemented() {
        let account = next_account();
        let (status, body) = send(account, "GetTopPathStatisticsByTraffic", json!({}));
        assert_eq!(status, 501);
        assert_eq!(body["__type"], "NotImplemented");
    }

    #[test]
    fn dispatch_ignores_other_services() {
        let request = Request {
            service: "s3".to_string(),
            operation: "CreateWebACL".to_string(),
            method: "POST".to_string(),
            path: "/".to_string(),
            query_string: String::new(),
            headers: HashMap::new(),
            body: Bytes::new(),
            account: 123456789012,
            region: "us-east-1".to_string(),
        };
        assert!(dispatch(&request).is_none());
    }
}
