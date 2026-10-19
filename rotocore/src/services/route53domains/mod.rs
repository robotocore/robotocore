//! Amazon Route 53 Domains backend — moto-equivalent (json protocol).
//!
//! Replaces the moto sidecar for `route53domains`
//! (`X-Amz-Target: Route53Domains_v20140515.<Op>` on
//! `application/x-amz-json-1.1`). State is per-(account, region), mirroring
//! the moto 5.1.23 fork's `Route53DomainsBackend` (moto/route53domains):
//! domains (contacts, nameservers, privacy flags, expiry timestamps), a
//! *permanent* operation log (the duplicate-request check compares against
//! every stored operation, all types included), domain tags, pending
//! internal transfers, per-domain transfer locks, DNSSEC key records, and
//! two paginated lists (moto's `Paginator` with base64 `uniqueAttributes`
//! markers and a limit default of 20).
//!
//! Contract: the botocore service model (35 operations, every one routed
//! by the fork's `responses.py`) and
//! tests/compatibility/test_route53domains_compat.py. There is no
//! probes/route53domains.json, so the fork source is the reference for
//! every response — including its quirks: `DurationInYears=1` always fails
//! the 1-10-years expiry window (moto's two `datetime.now()` calls straddle
//! the exact one-year boundary), `ListOperations` sorts `ASC` in reverse,
//! and `GetDomainSuggestions` ignores `OnlyAvailable`.
//!
//! Documented deviations (invisible to the compat suite): moto creates a
//! hosted zone in the route53 backend on `RegisterDomain` — nothing on
//! this service's surface reads it, so the cross-module side effect is
//! skipped; moto's pagination token carries a Python `hash()` checksum
//! that cannot be reproduced outside CPython, so our markers bind on
//! `uniqueAttributes` alone (round-trips through this server work); name
//! validation mirrors the fork's `VALID_DOMAIN_REGEX` without a regex
//! dependency (hand-rolled prefix matcher); IP parsing uses Rust's std
//! parser (Python's `ip_address` additionally accepts bare integers).

mod data;

use self::data::{CONTACT_TYPES, COUNTRY_CODES, SUPPORTED_TLDS};

use std::collections::HashMap;
use std::net::IpAddr;
use std::sync::Arc;

use base64::engine::general_purpose::STANDARD as B64;
use base64::Engine;
use parking_lot::RwLock;
use serde_json::{json, Map, Value};
use uuid::Uuid;

use crate::proto::{json_error, now_unix, parse_json_body};
use crate::store::{self, ScopeKey};
use crate::{Request, Response};

const MAX_DOMAINS: usize = 200;
/// Page limit when `MaxItems` is absent or zero (moto's `limit_default`).
const PAGE_DEFAULT: usize = 20;
/// One day in epoch seconds; a "year" is 365 days (moto's `timedelta(days=365*n)`).
const DAY: f64 = 86_400.0;
const YEAR: f64 = 365.0;

const DOMAIN_OPERATION_STATUSES: &[&str] =
    &["SUBMITTED", "IN_PROGRESS", "ERROR", "SUCCESSFUL", "FAILED"];

const DOMAIN_OPERATION_TYPES: &[&str] = &[
    "REGISTER_DOMAIN",
    "DELETE_DOMAIN",
    "TRANSFER_IN_DOMAIN",
    "UPDATE_DOMAIN_CONTACT",
    "UPDATE_NAMESERVER",
    "CHANGE_PRIVACY_PROTECTION",
    "DOMAIN_LOCK",
    "ENABLE_AUTORENEW",
    "DISABLE_AUTORENEW",
    "ADD_DNSSEC",
    "REMOVE_DNSSEC",
    "EXPIRE_DOMAIN",
    "TRANSFER_OUT_DOMAIN",
    "CHANGE_DOMAIN_OWNER",
    "RENEW_DOMAIN",
    "PUSH_DOMAIN",
    "INTERNAL_TRANSFER_OUT_DOMAIN",
    "INTERNAL_TRANSFER_IN_DOMAIN",
];

/// moto's default nameserver set for newly registered/transfered domains.
const DEFAULT_NAMESERVERS: &[&str] = &[
    "ns-2048.awscdn-64.net",
    "ns-2051.awscdn-67.net",
    "ns-2050.awscdn-66.net",
    "ns-2049.awscdn-65.net",
];

/// Suffix chain `GetDomainSuggestions` walks (fork's `suffixes` list).
const SUGGESTION_SUFFIXES: &[&str] = &[
    "online", "site", "tech", "store", "shop", "app", "dev", "io", "co", "net",
];

// ---------------------------------------------------------------------------
// State
// ---------------------------------------------------------------------------

type Stores = Arc<RwLock<HashMap<ScopeKey, Store>>>;

fn stores() -> Stores {
    store::slot::<Store>("route53domains")
}

/// Per-(account, region) state for Route 53 Domains.
#[derive(Default)]
struct Store {
    /// Monotonic token giving every record its insertion order (moto's
    /// `dict` iteration order is what the List* generators walk).
    next_seq: u64,
    /// domain name -> record (moto's `__domains` dict)
    domains: HashMap<String, Domain>,
    /// operation id -> record (moto's `__operations` dict)
    operations: HashMap<String, Operation>,
    /// domain name -> TagList (moto's `__domain_tags`)
    tags: HashMap<String, Vec<(String, Value)>>,
    /// domain name -> pending internal transfer (moto's `__pending_transfers`)
    pending_transfers: HashMap<String, PendingTransfer>,
    /// domain name -> transfer-lock state (moto's `__domain_transfer_locks`)
    transfer_locks: HashMap<String, bool>,
    /// domain name -> DNSSEC key records (moto's `__dnssec_keys`)
    dnssec_keys: HashMap<String, Vec<Value>>,
}

impl Store {
    fn next_seq(&mut self) -> u64 {
        let seq = self.next_seq;
        self.next_seq += 1;
        seq
    }

    /// Log an operation (`Route53DomainsOperation.validate` + insert).
    fn record_operation(&mut self, domain_name: &str, op_type: &str) -> String {
        let id = Uuid::new_v4().to_string();
        let ts = now_unix();
        let op = Operation {
            seq: self.next_seq(),
            id: id.clone(),
            domain_name: domain_name.to_string(),
            status: "SUCCESSFUL".to_string(),
            op_type: op_type.to_string(),
            submitted_ts: ts,
            updated_ts: ts,
            message: None,
        };
        self.operations.insert(id.clone(), op);
        id
    }

    /// moto's `__validate_duplicate_operations`: one stored op with the
    /// same (domain, type) pair — any type family, ever — rejects.
    fn has_operation(&self, domain_name: &str, op_type: &str) -> bool {
        self.operations
            .values()
            .any(|o| o.domain_name == domain_name && o.op_type == op_type)
    }

    /// Insertion-ordered values (moto walks its dicts in insertion order).
    fn ordered_domains(&self) -> Vec<&Domain> {
        let mut list: Vec<&Domain> = self.domains.values().collect();
        list.sort_by_key(|d| d.seq);
        list
    }

    fn ordered_operations(&self) -> Vec<&Operation> {
        let mut list: Vec<&Operation> = self.operations.values().collect();
        list.sort_by_key(|o| o.seq);
        list
    }

    /// moto's `_get_domain_or_raise`: name errors first, then ownership.
    fn get_domain_or_raise(&self, domain_name: &str) -> Result<&Domain, Response> {
        domain_name_ok(domain_name)?;
        match self.domains.get(domain_name) {
            Some(domain) => Ok(domain),
            None => Err(invalid_input(&[
                "Domain is not associated with this account".to_string(),
            ])),
        }
    }

    /// Map over a domain known to exist (call `get_domain_or_raise` first,
    /// like moto does, for the error semantics).
    fn update_domain(&mut self, domain_name: &str, f: impl FnOnce(&mut Domain)) {
        if let Some(domain) = self.domains.get_mut(domain_name) {
            f(domain);
        }
    }
}

// ---------------------------------------------------------------------------
// Records
// ---------------------------------------------------------------------------

struct Domain {
    /// Moto's dict preserves insertion order; `seq` keeps it for the lists.
    seq: u64,
    name: String,
    nameservers: Vec<NameServer>,
    auto_renew: bool,
    admin_contact: Value,
    registrant_contact: Value,
    tech_contact: Value,
    admin_privacy: bool,
    registrant_privacy: bool,
    tech_privacy: bool,
    creation_ts: f64,
    updated_ts: f64,
    expiration_ts: f64,
}

// Moto's domain also keeps `ExtraParams` — nothing reads it and it never
// reaches the wire, so the field is dropped here.

/// moto's `Route53Domain.to_json`: null members stay on the wire.
fn domain_to_json(domain: &Domain) -> Value {
    json!({
        "DomainName": domain.name,
        "Nameservers": domain.nameservers.iter().map(NameServer::to_json).collect::<Vec<_>>(),
        "AutoRenew": domain.auto_renew,
        "AdminContact": contact_to_json(&domain.admin_contact),
        "RegistrantContact": contact_to_json(&domain.registrant_contact),
        "TechContact": contact_to_json(&domain.tech_contact),
        "AdminPrivacy": domain.admin_privacy,
        "RegistrantPrivacy": domain.registrant_privacy,
        "TechPrivacy": domain.tech_privacy,
        "RegistrarName": "GANDI SAS",
        "WhoIsServer": "whois.gandi.net",
        "RegistrarUrl": "http://www.gandi.net",
        "AbuseContactEmail": "abuse@support.gandi.net",
        "AbuseContactPhone": Value::Null,
        "RegistryDomainId": "",
        "CreationDate": domain.creation_ts,
        "UpdateDate": domain.updated_ts,
        "ExpirationDate": domain.expiration_ts,
        "Reseller": Value::Null,
        "DnsSec": "",
        "StatusList": ["SUCCEEDED"],
        "DnsSecKeys": Value::Null,
        "BillingContact": contact_to_json(&domain.admin_contact),
    })
}

struct NameServer {
    name: String,
    glue_ips: Vec<String>,
}

impl NameServer {
    /// moto's `NameServer.to_json`: `GlueIps` only when non-empty.
    fn to_json(&self) -> Value {
        if self.glue_ips.is_empty() {
            json!({ "Name": self.name })
        } else {
            json!({ "Name": self.name, "GlueIps": self.glue_ips })
        }
    }
}

struct Operation {
    seq: u64,
    id: String,
    domain_name: String,
    status: String,
    op_type: String,
    submitted_ts: f64,
    updated_ts: f64,
    message: Option<String>,
}

impl Operation {
    /// moto's `Route53DomainsOperation.to_json`; `StatusFlag` is always
    /// null in the fork and `Message` only appears when set.
    fn to_json(&self) -> Value {
        let mut out = json!({
            "OperationId": self.id,
            "Status": self.status,
            "StatusFlag": Value::Null,
            "DomainName": self.domain_name,
            "LastUpdatedDate": self.updated_ts,
            "SubmittedDate": self.submitted_ts,
            "Type": self.op_type,
        });
        if let Some(msg) = &self.message {
            out["Message"] = json!(msg);
        }
        out
    }
}

/// Pending cross-account transfer (moto's `__pending_transfers` row;
/// moto also stores the target account id but nothing reads it).
struct PendingTransfer {
    password: String,
}

// ---------------------------------------------------------------------------
// dispatch
// ---------------------------------------------------------------------------

pub fn dispatch(req: &Request) -> Option<Response> {
    if req.service != "route53domains" {
        return None;
    }
    Some(with_store(req, |st| route(req, st)))
}

fn with_store(req: &Request, f: impl FnOnce(&mut Store) -> Response) -> Response {
    let slot = stores();
    let mut map = slot.write();
    let st = map.entry((req.account, req.region.clone())).or_default();
    f(st)
}

fn route(req: &Request, st: &mut Store) -> Response {
    let body = parse_json_body(&req.body);
    match req.operation.as_str() {
        "RegisterDomain" => register_domain(st, &body),
        "TransferDomain" => transfer_domain(st, &body),
        "DeleteDomain" => delete_domain(st, &body),
        "GetDomainDetail" => get_domain_detail(st, &body),
        "ListDomains" => list_domains(st, &body),
        "ListOperations" => list_operations(st, &body),
        "GetOperationDetail" => get_operation_detail(st, &body),
        "UpdateDomainNameservers" => update_domain_nameservers(st, &body),
        "CheckDomainAvailability" => check_domain_availability(st, &body),
        "CheckDomainTransferability" => ok(json!({
            "Transferability": {"Transferable": "TRANSFERABLE"},
            "Message": "",
        })),
        "GetDomainSuggestions" => get_domain_suggestions(&body),
        "TransferDomainToAnotherAwsAccount" => transfer_to_account(st, &body),
        "AcceptDomainTransferFromAnotherAwsAccount" => accept_transfer(st, &body),
        "RejectDomainTransferFromAnotherAwsAccount" => {
            finish_transfer(st, &body, "INTERNAL_TRANSFER_IN_DOMAIN")
        }
        "CancelDomainTransferToAnotherAwsAccount" => {
            finish_transfer(st, &body, "INTERNAL_TRANSFER_OUT_DOMAIN")
        }
        "EnableDomainAutoRenew" => set_auto_renew(st, &body, true),
        "DisableDomainAutoRenew" => set_auto_renew(st, &body, false),
        "EnableDomainTransferLock" => set_transfer_lock(st, &body, true),
        "DisableDomainTransferLock" => set_transfer_lock(st, &body, false),
        "UpdateDomainContact" => update_domain_contact(st, &body),
        "UpdateDomainContactPrivacy" => update_domain_contact_privacy(st, &body),
        "GetContactReachabilityStatus" => ok(json!({
            "domainName": as_str(&body, "domainName"),
            "status": "DONE",
        })),
        "ResendContactReachabilityEmail" => ok(json!({
            "domainName": as_str(&body, "domainName"),
            "emailAddress": "registrant@example.com",
            "isAlreadyVerified": true,
        })),
        "ResendOperationAuthorization" => resend_operation_authorization(st, &body),
        "RetrieveDomainAuthCode" => retrieve_domain_auth_code(st, &body),
        "RenewDomain" => renew_domain(st, &body),
        "ListTagsForDomain" => ok(json!({
            "TagList": tag_list_wire(st, as_str(&body, "DomainName")),
        })),
        "UpdateTagsForDomain" => update_tags_for_domain(st, &body),
        "DeleteTagsForDomain" => delete_tags_for_domain(st, &body),
        "ListPrices" => list_prices(&body),
        "AssociateDelegationSignerToDomain" => associate_delegation_signer(st, &body),
        "DisassociateDelegationSignerFromDomain" => disassociate_delegation_signer(st, &body),
        "PushDomain" => push_domain(st, &body),
        "ViewBilling" => ok(json!({"NextPageMarker": Value::Null, "BillingRecords": []})),
        other => json_error(
            501,
            "NotImplemented",
            &format!("The {other} action has not been implemented"),
        ),
    }
}

// ---------------------------------------------------------------------------
// Registration / transfer / deletion / detail
// ---------------------------------------------------------------------------

fn register_domain(st: &mut Store, body: &Value) -> Response {
    if st.domains.len() == MAX_DOMAINS {
        return domain_limit();
    }
    let domain_name = as_str(body, "DomainName");
    if st.has_operation(domain_name, "REGISTER_DOMAIN") {
        return duplicate_request();
    }
    let spec = match parse_domain_spec(body, false) {
        Ok(spec) => spec,
        Err(resp) => return resp,
    };
    match build_domain(st, &spec) {
        Ok(domain) => {
            let id = st.record_operation(domain_name, "REGISTER_DOMAIN");
            st.transfer_locks.insert(domain_name.to_string(), true);
            st.domains.insert(domain_name.to_string(), domain);
            ok(json!({ "OperationId": id }))
        }
        Err(resp) => resp,
    }
}

fn transfer_domain(st: &mut Store, body: &Value) -> Response {
    let domain_name = as_str(body, "DomainName");
    if st.has_operation(domain_name, "TRANSFER_IN_DOMAIN") {
        return duplicate_request();
    }
    let spec = match parse_domain_spec(body, true) {
        Ok(spec) => spec,
        Err(resp) => return resp,
    };
    match build_domain(st, &spec) {
        Ok(domain) => {
            let id = st.record_operation(domain_name, "TRANSFER_IN_DOMAIN");
            st.transfer_locks.insert(domain_name.to_string(), true);
            st.domains.insert(domain_name.to_string(), domain);
            ok(json!({ "OperationId": id }))
        }
        Err(resp) => resp,
    }
}

/// Shared parsing for RegisterDomain / TransferDomain — duration, the
/// three required contacts (validated in moto's argument-evaluation
/// order), the privacy flags, and (transfer only) nameservers.
fn parse_domain_spec(body: &Value, with_nameservers: bool) -> Result<DomainSpec<'_>, Response> {
    let Some(duration) = as_int(body, "DurationInYears") else {
        // Required member; moto crashes on `timedelta(days=365 * None)`.
        return Err(internal_error(
            "int() argument must be a string or a number, not 'NoneType'",
        ));
    };
    let admin = contact_entry(body, "AdminContact")?;
    let registrant = contact_entry(body, "RegistrantContact")?;
    let tech = contact_entry(body, "TechContact")?;
    let nameservers = if with_nameservers {
        // TransferDomain carries Nameservers; absent/empty → defaults.
        let raw = body
            .get("Nameservers")
            .and_then(Value::as_array)
            .filter(|list| !list.is_empty());
        raw.map(|list| list.to_vec())
    } else {
        None
    };
    Ok(DomainSpec {
        domain_name: as_str(body, "DomainName"),
        duration_in_years: duration,
        auto_renew: as_bool(body, "AutoRenew", true),
        admin,
        registrant,
        tech,
        admin_privacy: as_bool(body, "PrivacyProtectAdminContact", true),
        registrant_privacy: as_bool(body, "PrivacyProtectRegistrantContact", true),
        tech_privacy: as_bool(body, "PrivacyProtectTechContact", true),
        nameservers,
    })
}

/// Validated required-contact member; moto crashes on a missing dict
/// (`NoneType.get`) — a 500 registered here for symmetry.
fn contact_entry(body: &Value, key: &str) -> Result<Value, Response> {
    let contact = match body.get(key) {
        Some(contact) if contact.is_object() => contact,
        _ => return Err(internal_error("'NoneType' object has no attribute 'get'")),
    };
    validate_contact(contact)?;
    Ok(contact.clone())
}

/// The deviation-free copy of moto's expiry arithmetic: `expiration` is
/// derived from a first `now()`, while the window check runs against a
/// second `now()` that always comes strictly later (quantized clocks
/// included) — exactly what moto's two `datetime.now()` calls guarantee.
fn later_now(prior: f64) -> f64 {
    let now = now_unix();
    if now > prior {
        now
    } else {
        prior + 1e-3
    }
}

/// Input bundle for building a `Domain` (moto's `Route53Domain.validate`).
struct DomainSpec<'a> {
    domain_name: &'a str,
    duration_in_years: i64,
    auto_renew: bool,
    admin: Value,
    registrant: Value,
    tech: Value,
    admin_privacy: bool,
    registrant_privacy: bool,
    tech_privacy: bool,
    /// Raw `Nameservers` member; None/empty → moto's defaults.
    nameservers: Option<Vec<Value>>,
}

fn build_domain(st: &mut Store, spec: &DomainSpec<'_>) -> Result<Domain, Response> {
    let mut errors: Vec<String> = Vec::new();
    // UnsupportedTLD propagates out of `Route53Domain.validate` directly.
    collect_domain_name(spec.domain_name, &mut errors)?;
    let nameservers = match &spec.nameservers {
        Some(raw) => match validate_nameserver_list(raw) {
            Ok(servers) => servers,
            Err(msgs) => {
                errors.extend(msgs);
                default_nameservers()
            }
        },
        None => default_nameservers(),
    };
    let now1 = now_unix();
    let expiration_ts = now1 + YEAR * DAY * spec.duration_in_years as f64;
    let time_left = expiration_ts - later_now(now1);
    if time_left < YEAR * DAY || time_left > 10.0 * YEAR * DAY {
        errors.push("ExpirationDate must by between 1 and 10 years from now".to_string());
    }
    if !errors.is_empty() {
        return Err(invalid_input(&errors));
    }
    Ok(Domain {
        seq: st.next_seq(),
        name: spec.domain_name.to_string(),
        nameservers,
        auto_renew: spec.auto_renew,
        admin_contact: spec.admin.clone(),
        registrant_contact: spec.registrant.clone(),
        tech_contact: spec.tech.clone(),
        admin_privacy: spec.admin_privacy,
        registrant_privacy: spec.registrant_privacy,
        tech_privacy: spec.tech_privacy,
        creation_ts: now1,
        updated_ts: now1,
        expiration_ts,
    })
}

/// moto's `nameservers or [defaults]`.
fn default_nameservers() -> Vec<NameServer> {
    DEFAULT_NAMESERVERS
        .iter()
        .map(|name| NameServer {
            name: (*name).to_string(),
            glue_ips: Vec::new(),
        })
        .collect()
}

fn delete_domain(st: &mut Store, body: &Value) -> Response {
    let domain_name = as_str(body, "DomainName");
    // moto dups the request before validating the name.
    if st.has_operation(domain_name, "DELETE_DOMAIN") {
        return duplicate_request();
    }
    match domain_name_ok(domain_name) {
        Ok(()) => {}
        Err(resp) => return resp,
    }
    if !st.domains.contains_key(domain_name) {
        return invalid_input(&[format!(
            "Domain {domain_name} isn't registered in the current account"
        )]);
    }
    let id = st.record_operation(domain_name, "DELETE_DOMAIN");
    st.domains.remove(domain_name);
    st.tags.remove(domain_name);
    st.transfer_locks.remove(domain_name);
    st.dnssec_keys.remove(domain_name);
    ok(json!({ "OperationId": id }))
}

/// moto's `GetDomainDetail` — `_get_domain_or_raise` then `to_json`.
fn get_domain_detail(st: &mut Store, body: &Value) -> Response {
    let domain_name = as_str(body, "DomainName");
    match st.get_domain_or_raise(domain_name) {
        Ok(domain) => ok(domain_to_json(domain)),
        Err(resp) => resp,
    }
}

// ---------------------------------------------------------------------------
// Lists + pagination
// ---------------------------------------------------------------------------

/// One `ListDomains` filter condition (moto's `DomainsFilter`).
struct DomainsFilter {
    name: String,
    operator: String,
    /// Single value; multi-value lists are rejected at validation.
    value: String,
    /// Expiry filters parse their epoch seconds once, up front.
    expiry: Option<f64>,
}

impl DomainsFilter {
    fn matches(&self, domain: &Domain) -> bool {
        if self.name == "DomainName" {
            return domain.name.starts_with(&self.value);
        }
        let Some(expiry) = self.expiry else {
            return false;
        };
        match self.operator.as_str() {
            "GE" => domain.expiration_ts >= expiry,
            "LE" => domain.expiration_ts <= expiry,
            // Unreachable post-validation (moto's `any(...)` yields nothing).
            _ => false,
        }
    }
}

/// `DomainsFilter.validate`: field/operator enums, exactly one value, and
/// the operator/field pairing constraints. A non-numeric Expiry value
/// crashes moto mid-filter — folded into a 500 here.
fn validate_filter(v: &Value) -> Result<DomainsFilter, Response> {
    let mut errors = Vec::new();
    // A missing member renders python's `None` in moto's messages.
    let name = v
        .get("Name")
        .and_then(Value::as_str)
        .map_or_else(|| "None".to_string(), str::to_string);
    let operator = as_str(v, "Operator").to_string();
    let values = as_str_list(v.get("Values"));
    if !["DomainName", "Expiry"].contains(&name.as_str()) {
        errors.push(format!("Cannot filter by field {name}"));
    }
    if !["LE", "GE", "BEGINS_WITH"].contains(&operator.as_str()) {
        errors.push(format!("Invalid filter operator {operator}"));
    }
    if values.len() != 1 {
        errors.push("Multiple filter values are not currently supported".to_string());
    }
    if name == "DomainName" && operator != "BEGINS_WITH" {
        errors.push(format!(
            "Operator {operator} cannot be used with the DomainName filter"
        ));
    }
    if name == "Expiry" && !matches!(operator.as_str(), "GE" | "LE") {
        errors.push(format!(
            "Operator {operator} cannot be used with the Expiry filter"
        ));
    }
    if !errors.is_empty() {
        return Err(invalid_input(&errors));
    }
    let expiry = if name == "Expiry" {
        match values[0].parse::<f64>() {
            Ok(x) => Some(x),
            // moto's `fromtimestamp(float(value))` ValueError.
            Err(_) => {
                return Err(internal_error(
                    "ValueError: could not convert string to float",
                ))
            }
        }
    } else {
        None
    };
    Ok(DomainsFilter {
        name,
        operator,
        value: values[0].clone(),
        expiry,
    })
}

/// `DomainsSortCondition.validate` — field enum + `ASC`/`DES` order.
fn validate_sort_condition(v: &Value) -> Result<(String, String), Response> {
    let mut errors = Vec::new();
    let name = v
        .get("Name")
        .and_then(Value::as_str)
        .map_or_else(|| "None".to_string(), str::to_string);
    let order = as_str(v, "SortOrder").to_string();
    if !["DomainName", "Expiry"].contains(&name.as_str()) {
        errors.push(format!("Cannot sort by field {name}"));
    }
    if !["ASC", "DES"].contains(&order.as_str()) {
        errors.push(format!("Invalid sort order {order}"));
    }
    if !errors.is_empty() {
        return Err(invalid_input(&errors));
    }
    Ok((name, order))
}

/// moto's `list_domains`: filters, an optional sort (only with matching
/// filter fields), pagination, then the per-domain wire summary.
fn list_domains(st: &mut Store, body: &Value) -> Response {
    let mut filters = Vec::new();
    if let Some(items) = body.get("FilterConditions").and_then(Value::as_array) {
        for item in items {
            match validate_filter(item) {
                Ok(filter) => filters.push(filter),
                Err(resp) => return resp,
            }
        }
    }
    let sort_condition = match body.get("SortCondition").filter(|v| truthy(v)) {
        Some(sort_value) => match validate_sort_condition(sort_value) {
            Ok(sort) => Some(sort),
            Err(resp) => return resp,
        },
        None => None,
    };
    // moto: `if sort and filter_fields and sort.name not in filter_fields`.
    if let Some((sort_name, _)) = &sort_condition {
        let filter_fields: Vec<&str> = filters.iter().map(|f| f.name.as_str()).collect();
        if !filter_fields.is_empty() && !filter_fields.contains(&sort_name.as_str()) {
            return invalid_input(&[String::from(
                "Sort condition must be the same as the filter condition",
            )]);
        }
    }
    let mut domains: Vec<&Domain> = st
        .ordered_domains()
        .into_iter()
        .filter(|d| filters.iter().all(|f| f.matches(d)))
        .collect();
    if let Some((name, order)) = &sort_condition {
        let descending = order == "DES";
        if name == "DomainName" {
            domains.sort_by(|a, b| a.name.cmp(&b.name));
        } else {
            domains.sort_by(|a, b| {
                a.expiration_ts
                    .partial_cmp(&b.expiration_ts)
                    .unwrap_or(std::cmp::Ordering::Equal)
            });
        }
        if descending {
            domains.reverse();
        }
    }
    let (page, marker) = match paginate(domains, |d| d.name.as_str(), body) {
        Ok(result) => result,
        Err(resp) => return resp,
    };
    let summaries: Vec<Value> = page
        .iter()
        .map(|d| {
            json!({
                "DomainName": d.name,
                "AutoRenew": d.auto_renew,
                "Expiry": d.expiration_ts,
                "TransferLock": true,
            })
        })
        .collect();
    ok(json!({"Domains": summaries, "NextPageMarker": marker}))
}

/// moto's `list_operations`: status/type validation, the submitted-since
/// cutoff, ordering (the fork sorts `ASC` in reverse), pagination keyed
/// on the operation id.
fn list_operations(st: &mut Store, body: &Value) -> Response {
    let submitted_since = as_int(body, "SubmittedSince");
    let statuses = as_str_list(body.get("Status"));
    let types = as_str_list(body.get("Type"));
    let sort_by = as_str(body, "SortBy");
    let sort_order = as_str(body, "SortOrder");
    let mut errors = Vec::new();
    if statuses
        .iter()
        .any(|status| !DOMAIN_OPERATION_STATUSES.contains(&status.as_str()))
    {
        errors.push("Status is invalid".to_string());
    }
    if types
        .iter()
        .any(|op_type| !DOMAIN_OPERATION_TYPES.contains(&op_type.as_str()))
    {
        errors.push("Type is invalid".to_string());
    }
    if !errors.is_empty() {
        return invalid_input(&errors);
    }
    let mut operations: Vec<&Operation> = st
        .ordered_operations()
        .into_iter()
        .filter(|op| {
            (statuses.is_empty() || statuses.contains(&op.status))
                && (types.is_empty() || types.contains(&op.op_type))
                && submitted_since.is_none_or(|since| op.submitted_ts >= since as f64)
        })
        .collect();
    if sort_by == "SubmittedDate" {
        operations.sort_by(|a, b| a.submitted_ts.total_cmp(&b.submitted_ts));
        // moto quirk: `reverse=sort_order == ASCENDING`.
        if sort_order == "ASC" {
            operations.reverse();
        }
    }
    let (page, marker) = match paginate(operations, |op| op.id.as_str(), body) {
        Ok(result) => result,
        Err(resp) => return resp,
    };
    let items: Vec<Value> = page.iter().map(|op| op.to_json()).collect();
    ok(json!({"Operations": items, "NextPageMarker": marker}))
}

fn get_operation_detail(st: &mut Store, body: &Value) -> Response {
    let op_id = as_str(body, "OperationId");
    match st.operations.get(op_id) {
        Some(op) => ok(op.to_json()),
        None => invalid_input(&[format!("Operation with id {op_id} doesn't exist")]),
    }
}

// ---------------------------------------------------------------------------
// Nameservers
// ---------------------------------------------------------------------------

/// `NameServer.validate_dict` for `TransferDomain.Nameservers` /
/// `UpdateDomainNameservers` entries.
fn validate_nameserver(data: &Value) -> Result<NameServer, Vec<String>> {
    let mut errors = Vec::new();
    // botocore makes `Name` required; a missing name falls into the
    // invalid-hostname error instead of moto's TypeError crash.
    let name = as_str(data, "Name").to_string();
    let glue_ips = as_str_list(data.get("GlueIps"));
    if !is_valid_domain(&name) {
        errors.push(format!("{name} is not a valid host name"));
    }
    let mut v4 = 0usize;
    let mut v6 = 0usize;
    for ip in &glue_ips {
        match ip.parse::<IpAddr>() {
            Ok(IpAddr::V4(_)) => v4 += 1,
            Ok(IpAddr::V6(_)) => v6 += 1,
            Err(_) => errors.push(format!("{ip} is not a valid IP address")),
        }
    }
    if v4 > 1 {
        errors.push("GlueIps list must include only 1 IPv4 address".to_string());
    }
    if v6 > 1 {
        errors.push("GlueIps list must include only 1 IPv6 address".to_string());
    }
    if !errors.is_empty() {
        return Err(errors);
    }
    Ok(NameServer { name, glue_ips })
}

/// moto's `[NameServer.validate_dict(obj) for obj ...]` — aborts at the
/// first invalid entry (ValidationException inside a list comprehension).
fn validate_nameserver_list(raw: &[Value]) -> Result<Vec<NameServer>, Vec<String>> {
    let mut servers = Vec::new();
    for item in raw {
        servers.push(validate_nameserver(item)?);
    }
    Ok(servers)
}

fn update_domain_nameservers(st: &mut Store, body: &Value) -> Response {
    let domain_name = as_str(body, "DomainName");
    let mut errors = Vec::new();
    // collect_domain_name: `UnsupportedTLD` raises on the spot.
    if let Err(resp) = collect_domain_name(domain_name, &mut errors) {
        return resp;
    }
    let Some(nameservers) = body.get("Nameservers").and_then(Value::as_array) else {
        // Required member; moto crashes on `len(None)`.
        return internal_error("object of type 'NoneType' has no len()");
    };
    if nameservers.is_empty() {
        errors.push("Must supply nameservers".to_string());
    }
    // moto wraps the comprehension in try/except: the first invalid entry
    // records its errors and clears `servers` (the glue check then no-ops).
    let mut servers = Vec::new();
    for item in nameservers {
        match validate_nameserver(item) {
            Ok(server) => servers.push(server),
            Err(mut msgs) => {
                errors.append(&mut msgs);
                servers.clear();
                break;
            }
        }
    }
    for server in &servers {
        if server.name.contains(domain_name) && server.glue_ips.is_empty() {
            errors.push(format!(
                "Must supply glue IPs for name server {} because it is a subdomain of the domain",
                server.name
            ));
        }
    }
    if !errors.is_empty() {
        return invalid_input(&errors);
    }
    if !st.domains.contains_key(domain_name) {
        return invalid_input(&[format!(
            "Domain {domain_name} is not registered to the current AWS account"
        )]);
    }
    // moto dups the request only after the domain is confirmed owned.
    if st.has_operation(domain_name, "UPDATE_NAMESERVER") {
        return duplicate_request();
    }
    let id = st.record_operation(domain_name, "UPDATE_NAMESERVER");
    st.update_domain(domain_name, |domain| domain.nameservers = servers);
    ok(json!({ "OperationId": id }))
}

// ---------------------------------------------------------------------------
// Availability / suggestions / prices
// ---------------------------------------------------------------------------

fn check_domain_availability(st: &mut Store, body: &Value) -> Response {
    let domain_name = as_str(body, "DomainName");
    ok(json!({
        "Availability":
            if st.domains.contains_key(domain_name) { "UNAVAILABLE" } else { "AVAILABLE" },
    }))
}

/// `GetDomainSuggestions` cycles the suffix list until `SuggestionCount`
/// suggestions exist (or the suffixes run out). `OnlyAvailable` is ignored
/// and every suggestion is `AVAILABLE` (fork behavior).
fn get_domain_suggestions(body: &Value) -> Response {
    let Some(count) = as_int(body, "SuggestionCount") else {
        // Required member; moto's fork crashes on `len([]) >= None`.
        return internal_error("'>=' not supported between instances of 'int' and 'NoneType'");
    };
    let base = as_str(body, "DomainName").split('.').next().unwrap_or("");
    let cap = count.max(0) as usize;
    let mut suggestions = Vec::new();
    for suffix in SUGGESTION_SUFFIXES {
        if suggestions.len() >= cap {
            break;
        }
        suggestions.push(json!({
            "DomainName": format!("{base}.{suffix}"),
            "Availability": "AVAILABLE",
        }));
    }
    ok(json!({ "SuggestionsList": suggestions }))
}

fn list_prices(body: &Value) -> Response {
    let tlds: Vec<String> = match body.get("Tld").and_then(Value::as_str) {
        Some(tld) => vec![tld.to_string()],
        None => ["com", "net", "org", "io", "info"]
            .iter()
            .map(|t| (*t).to_string())
            .collect(),
    };
    let price = |amount: f64| json!({"Price": amount, "Currency": "USD"});
    let prices: Vec<Value> = tlds
        .into_iter()
        .map(|tld| {
            json!({
                "Name": tld,
                "RegistrationPrice": price(12.0),
                "TransferPrice": price(12.0),
                "RenewalPrice": price(12.0),
                "ChangeOwnershipPrice": price(12.0),
                "RestorationPrice": price(80.0),
            })
        })
        .collect();
    ok(json!({"Prices": prices, "NextPageMarker": Value::Null}))
}

// ---------------------------------------------------------------------------
// Internal transfers
// ---------------------------------------------------------------------------

fn transfer_to_account(st: &mut Store, body: &Value) -> Response {
    let domain_name = as_str(body, "DomainName");
    if let Err(resp) = st.get_domain_or_raise(domain_name) {
        return resp;
    }
    // moto: `str(uuid4())[:12]` — 8 hex digits, the dash, then 3 more.
    let uuid = Uuid::new_v4().to_string();
    let password = uuid[..12].to_string();
    let id = st.record_operation(domain_name, "INTERNAL_TRANSFER_OUT_DOMAIN");
    st.pending_transfers.insert(
        domain_name.to_string(),
        PendingTransfer {
            password: password.clone(),
        },
    );
    ok(json!({ "OperationId": id, "Password": password }))
}

fn accept_transfer(st: &mut Store, body: &Value) -> Response {
    let domain_name = as_str(body, "DomainName");
    let Some(pending) = st
        .pending_transfers
        .get(domain_name)
        .map(|row| row.password.clone())
    else {
        return invalid_input(&[format!("No pending transfer for domain {domain_name}")]);
    };
    if pending != as_str(body, "Password") {
        return invalid_input(&["Invalid password for transfer".to_string()]);
    }
    st.pending_transfers.remove(domain_name);
    let id = st.record_operation(domain_name, "INTERNAL_TRANSFER_IN_DOMAIN");
    ok(json!({ "OperationId": id }))
}

/// Reject / cancel pop any pending transfer unguarded, like moto.
fn finish_transfer(st: &mut Store, body: &Value, op_type: &str) -> Response {
    let domain_name = as_str(body, "DomainName");
    st.pending_transfers.remove(domain_name);
    let id = st.record_operation(domain_name, op_type);
    ok(json!({ "OperationId": id }))
}

// ---------------------------------------------------------------------------
// Auto-renew / transfer lock / contacts / privacy
// ---------------------------------------------------------------------------

fn set_auto_renew(st: &mut Store, body: &Value, on: bool) -> Response {
    let domain_name = as_str(body, "DomainName");
    if let Err(resp) = st.get_domain_or_raise(domain_name) {
        return resp;
    }
    st.update_domain(domain_name, |domain| domain.auto_renew = on);
    ok_empty()
}

fn set_transfer_lock(st: &mut Store, body: &Value, on: bool) -> Response {
    let domain_name = as_str(body, "DomainName");
    if let Err(resp) = st.get_domain_or_raise(domain_name) {
        return resp;
    }
    st.transfer_locks.insert(domain_name.to_string(), on);
    let id = st.record_operation(domain_name, "DOMAIN_LOCK");
    ok(json!({ "OperationId": id }))
}

/// One contact field of `UpdateDomainContact`: only truthy dicts are
/// validated and applied (moto's `if admin_contact:`).
fn contact_update(body: &Value, key: &str) -> Result<Option<Value>, Response> {
    match body.get(key).filter(|v| truthy(v)) {
        None => Ok(None),
        Some(contact) => {
            validate_contact(contact)?;
            Ok(Some(contact.clone()))
        }
    }
}

fn update_domain_contact(st: &mut Store, body: &Value) -> Response {
    let domain_name = as_str(body, "DomainName");
    if let Err(resp) = st.get_domain_or_raise(domain_name) {
        return resp;
    }
    let admin = match contact_update(body, "AdminContact") {
        Ok(v) => v,
        Err(resp) => return resp,
    };
    let registrant = match contact_update(body, "RegistrantContact") {
        Ok(v) => v,
        Err(resp) => return resp,
    };
    let tech = match contact_update(body, "TechContact") {
        Ok(v) => v,
        Err(resp) => return resp,
    };
    st.update_domain(domain_name, |domain| {
        if let Some(contact) = admin.clone() {
            domain.admin_contact = contact;
        }
        if let Some(contact) = registrant.clone() {
            domain.registrant_contact = contact;
        }
        if let Some(contact) = tech.clone() {
            domain.tech_contact = contact;
        }
    });
    let id = st.record_operation(domain_name, "UPDATE_DOMAIN_CONTACT");
    ok(json!({ "OperationId": id }))
}

fn update_domain_contact_privacy(st: &mut Store, body: &Value) -> Response {
    let domain_name = as_str(body, "DomainName");
    if let Err(resp) = st.get_domain_or_raise(domain_name) {
        return resp;
    }
    // moto's `if admin_privacy is not None:` — a present member applies,
    // an absent (or null) one keeps the stored value.
    let admin = privacy_value(body, "AdminPrivacy");
    let registrant = privacy_value(body, "RegistrantPrivacy");
    let tech = privacy_value(body, "TechPrivacy");
    st.update_domain(domain_name, |domain| {
        if let Some(v) = admin {
            domain.admin_privacy = v;
        }
        if let Some(v) = registrant {
            domain.registrant_privacy = v;
        }
        if let Some(v) = tech {
            domain.tech_privacy = v;
        }
    });
    let id = st.record_operation(domain_name, "CHANGE_PRIVACY_PROTECTION");
    ok(json!({ "OperationId": id }))
}

/// `AdminPrivacy`-style boolean members: `None` means "leave unchanged".
fn privacy_value(body: &Value, key: &str) -> Option<bool> {
    match body.get(key) {
        Some(Value::Bool(b)) => Some(*b),
        Some(Value::String(s)) if s.eq_ignore_ascii_case("true") => Some(true),
        Some(Value::String(s)) if s.eq_ignore_ascii_case("false") => Some(false),
        _ => None,
    }
}

fn resend_operation_authorization(st: &mut Store, body: &Value) -> Response {
    let op_id = as_str(body, "OperationId");
    if !st.operations.contains_key(op_id) {
        return invalid_input(&[format!("Operation with id {op_id} doesn't exist")]);
    }
    ok_empty()
}

fn retrieve_domain_auth_code(st: &mut Store, body: &Value) -> Response {
    let domain_name = as_str(body, "DomainName");
    if let Err(resp) = st.get_domain_or_raise(domain_name) {
        return resp;
    }
    let uuid = Uuid::new_v4().to_string();
    ok(json!({ "AuthCode": uuid[..12].to_string() }))
}

/// moto's `RenewDomain`: `$ DurationInYears or 1` (absent and zero become
/// one; the fork ignores `CurrentExpiryYear`).
fn renew_domain(st: &mut Store, body: &Value) -> Response {
    let domain_name = as_str(body, "DomainName");
    if let Err(resp) = st.get_domain_or_raise(domain_name) {
        return resp;
    }
    let duration = match as_int(body, "DurationInYears") {
        Some(0) | None => 1,
        Some(n) => n,
    };
    st.update_domain(domain_name, |domain| {
        domain.expiration_ts += YEAR * DAY * duration as f64;
    });
    let id = st.record_operation(domain_name, "RENEW_DOMAIN");
    ok(json!({ "OperationId": id }))
}

// ---------------------------------------------------------------------------
// Tags
// ---------------------------------------------------------------------------

/// Render a domain's tag rows as `{Key, Value}` objects (the wire shape).
fn tag_list_wire(st: &Store, domain_name: &str) -> Vec<Value> {
    st.tags
        .get(domain_name)
        .map(|row| {
            row.iter()
                .map(|(key, value)| json!({"Key": key, "Value": value}))
                .collect()
        })
        .unwrap_or_default()
}

/// Tags merge in python-dict order: existing keys update in place, new
/// keys append at the end. No ownership check (fork behavior).
fn update_tags_for_domain(st: &mut Store, body: &Value) -> Response {
    let domain_name = as_str(body, "DomainName").to_string();
    let Some(tags) = body.get("TagsToUpdate") else {
        return ok_empty();
    };
    let incoming: Vec<(String, Value)> = tags
        .as_array()
        .map(|list| {
            list.iter()
                .map(|tag| {
                    let key = as_str(tag, "Key").to_string();
                    let value = tag.get("Value").map_or_else(|| json!(""), |v| v.clone());
                    (key, value)
                })
                .collect()
        })
        .unwrap_or_default();
    let row = st.tags.entry(domain_name).or_default();
    for (key, value) in incoming {
        match row.iter().position(|(k, _)| *k == key) {
            Some(pos) => row[pos].1 = value,
            None => row.push((key, value)),
        }
    }
    ok_empty()
}

fn delete_tags_for_domain(st: &mut Store, body: &Value) -> Response {
    let domain_name = as_str(body, "DomainName").to_string();
    let keys = as_str_list(body.get("TagsToDelete"));
    let row = st.tags.entry(domain_name).or_default();
    row.retain(|(key, _)| !keys.contains(key));
    ok_empty()
}

// ---------------------------------------------------------------------------
// DNSSEC / push
// ---------------------------------------------------------------------------

fn associate_delegation_signer(st: &mut Store, body: &Value) -> Response {
    let domain_name = as_str(body, "DomainName");
    if let Err(resp) = st.get_domain_or_raise(domain_name) {
        return resp;
    }
    let key_id = Uuid::new_v4().to_string();
    let signing = body
        .get("SigningAttributes")
        .and_then(Value::as_object)
        .cloned()
        .unwrap_or_default();
    let mut record = Map::new();
    record.insert("id".to_string(), json!(key_id));
    for (key, value) in signing {
        record.insert(key, value);
    }
    st.dnssec_keys
        .entry(domain_name.to_string())
        .or_default()
        .push(Value::Object(record));
    let id = st.record_operation(domain_name, "ADD_DNSSEC");
    ok(json!({ "OperationId": id }))
}

fn disassociate_delegation_signer(st: &mut Store, body: &Value) -> Response {
    let domain_name = as_str(body, "DomainName");
    let signer_id = as_str(body, "Id").to_string();
    if let Err(resp) = st.get_domain_or_raise(domain_name) {
        return resp;
    }
    let row = st.dnssec_keys.entry(domain_name.to_string()).or_default();
    row.retain(|key| key.get("id").and_then(Value::as_str) != Some(signer_id.as_str()));
    let id = st.record_operation(domain_name, "REMOVE_DNSSEC");
    ok(json!({ "OperationId": id }))
}

fn push_domain(st: &mut Store, body: &Value) -> Response {
    let domain_name = as_str(body, "DomainName");
    if let Err(resp) = st.get_domain_or_raise(domain_name) {
        return resp;
    }
    let id = st.record_operation(domain_name, "PUSH_DOMAIN");
    st.domains.remove(domain_name);
    ok(json!({ "OperationId": id }))
}

// ---------------------------------------------------------------------------
// Names / contacts
// ---------------------------------------------------------------------------

/// The fork's `VALID_DOMAIN_REGEX` under `re.match` (leftmost prefix
/// match): a run of `label '.'` segments, then a final label. Hand-rolled
/// to honor the prefix semantics without a regex dep — trailing junk like
/// `example.com!` still matches while `a.b-.c` fails on the second dot.
fn is_valid_domain(s: &str) -> bool {
    fn alnum(b: u8) -> bool {
        b.is_ascii_lowercase() || b.is_ascii_digit()
    }
    fn label_char(b: u8) -> bool {
        alnum(b) || b == b'-'
    }
    /// `[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?` — non-empty, ≤ 63 chars,
    /// first and last alphanumeric, interior hyphens allowed.
    fn label_valid(seg: &[u8]) -> bool {
        !seg.is_empty()
            && seg.len() <= 63
            && alnum(seg[0])
            && alnum(seg[seg.len() - 1])
            && seg.iter().all(|b| label_char(*b))
    }
    let bytes = s.as_bytes();
    match bytes.first() {
        Some(b) if alnum(*b) => {}
        _ => return false,
    }
    // Walk forward to the first dot; every intermediate char must be a
    // label char (labels cannot span dots), and the final label must start
    // with an alphanumeric right after it.
    let mut dot = 1;
    loop {
        if dot >= bytes.len() {
            return false; // no dot at all, or a bare label
        }
        let b = bytes[dot];
        if b == b'.' {
            return label_valid(&bytes[..dot]) && bytes.get(dot + 1).copied().is_some_and(alnum);
        }
        if !label_char(b) {
            return false;
        }
        dot += 1;
    }
}

/// `Route53Domain.validate_domain_name`: appends "Invalid domain name" or
/// raises `UnsupportedTLD` on the spot.
fn collect_domain_name(domain_name: &str, errors: &mut Vec<String>) -> Result<(), Response> {
    if !is_valid_domain(domain_name) {
        errors.push("Invalid domain name".to_string());
        return Ok(());
    }
    let tld = domain_name.rsplit('.').next().unwrap_or("");
    if !SUPPORTED_TLDS.contains(&tld) {
        return Err(unsupported_tld(tld));
    }
    Ok(())
}

/// The raising variant used outside `Route53Domain.validate` (delete,
/// nameservers, detail): name errors become a single InvalidInput.
fn domain_name_ok(domain_name: &str) -> Result<(), Response> {
    if !is_valid_domain(domain_name) {
        return Err(invalid_input(&["Invalid domain name".to_string()]));
    }
    let tld = domain_name.rsplit('.').next().unwrap_or("");
    if !SUPPORTED_TLDS.contains(&tld) {
        return Err(unsupported_tld(tld));
    }
    Ok(())
}

/// `Route53DomainsContactDetail.validate_dict` — length caps (255), the
/// contact-type/organization coupling, the country-code table and the
/// `+<digits>.<digits>` phone shape.
fn validate_contact(contact: &Value) -> Result<(), Response> {
    let mut errors = Vec::new();
    for field in [
        "AddressLine1",
        "AddressLine2",
        "City",
        "Email",
        "Fax",
        "FirstName",
        "LastName",
        "State",
        "ZipCode",
    ] {
        if let Some(Value::String(text)) = contact.get(field) {
            if text.chars().count() > 255 {
                errors.push(format!("Length of {field} is more than 255"));
            }
        }
    }
    if let Some(contact_type) = contact
        .get("ContactType")
        .and_then(Value::as_str)
        .filter(|t| !t.is_empty())
    {
        if CONTACT_TYPES.contains(&contact_type) {
            if contact_type != "PERSON"
                && contact
                    .get("OrganizationName")
                    .and_then(Value::as_str)
                    .is_none_or(str::is_empty)
            {
                errors.push(
                    "Must supply OrganizationName when ContactType is not PERSON".to_string(),
                );
            }
        } else {
            errors.push(format!("Invalid contact type {contact_type}"));
        }
    }
    if let Some(code) = contact
        .get("CountryCode")
        .and_then(Value::as_str)
        .filter(|code| !code.is_empty())
    {
        if !COUNTRY_CODES.contains(&code) {
            errors.push(format!("CountryCode {code} is invalid"));
        }
    }
    if let Some(phone) = contact
        .get("PhoneNumber")
        .and_then(Value::as_str)
        .filter(|phone| !phone.is_empty())
    {
        if !valid_phone_number(phone) {
            errors.push("PhoneNumber is in an invalid format".to_string());
        }
    }
    if errors.is_empty() {
        Ok(())
    } else {
        Err(invalid_input(&errors))
    }
}

/// `\+\d*\.\d+$` under `re.match` — optional digits, one dot, digits to
/// the end.
fn valid_phone_number(s: &str) -> bool {
    let Some(rest) = s.as_bytes().strip_prefix(b"+") else {
        return false;
    };
    let Some(dot) = rest.iter().position(|b| *b == b'.') else {
        return false;
    };
    let before = &rest[..dot];
    let after = &rest[dot + 1..];
    before.iter().all(u8::is_ascii_digit)
        && !after.is_empty()
        && after.iter().all(u8::is_ascii_digit)
}

/// moto's `Route53DomainsContactDetail.to_json`: fixed key set, nulls
/// dropped, the caller's values rendered verbatim otherwise.
fn contact_to_json(contact: &Value) -> Value {
    /// moto's literal key order in `to_json`.
    const KEYS: &[&str] = &[
        "FirstName",
        "LastName",
        "ContactType",
        "OrganizationName",
        "AddressLine1",
        "AddressLine2",
        "City",
        "State",
        "CountryCode",
        "ZipCode",
        "PhoneNumber",
        "Email",
        "Fax",
        "ExtraParams",
    ];
    let mut out = Map::new();
    for key in KEYS {
        match contact.get(*key) {
            Some(Value::Null) | None => {}
            Some(value) => {
                out.insert((*key).to_string(), value.clone());
            }
        }
    }
    Value::Object(out)
}

// ---------------------------------------------------------------------------
// Pagination (moto's `Paginator` + botocore's TokenEncoder/Decoder)
// ---------------------------------------------------------------------------

/// moto's `Paginator.paginate`: an optional starting token (the first item
/// with a matching attribute — items before it drop off), a page cut, and
/// the next marker derived from the first excluded item.
fn paginate<'a, T>(
    items: Vec<&'a T>,
    key_of: impl Fn(&'a T) -> &'a str,
    body: &Value,
) -> Result<(Vec<&'a T>, Option<String>), Response> {
    // moto: `max_results if max_results else limit_default` — zero/absent
    // both take the default.
    let max_results = match as_int(body, "MaxItems") {
        Some(limit) if limit > 0 => limit as usize,
        _ => PAGE_DEFAULT,
    };
    let start = match body.get("Marker").and_then(Value::as_str) {
        None | Some("") => 0,
        Some(raw) => {
            let attribute = decode_marker(raw)?;
            match items.iter().position(|item| key_of(item) == attribute) {
                Some(index) => index,
                None => return Err(invalid_token("Resource not found!")),
            }
        }
    };
    let end = (start + max_results).min(items.len());
    let page: Vec<&'a T> = items[start..end].to_vec();
    let next = if !page.is_empty() && end < items.len() {
        Some(encode_marker(key_of(items[end])))
    } else {
        None
    };
    Ok((page, next))
}

/// botocore's `TokenEncoder.encode`: JSON + base64.
fn encode_marker(attribute: &str) -> String {
    B64.encode(serde_json::to_vec(&json!({ "uniqueAttributes": attribute })).unwrap_or_default())
}

/// botocore's `TokenDecoder.decode` — undecodable markers raise
/// `InvalidToken` like moto's decoder; a token without `uniqueAttributes`
/// carries no usable position (`InvalidToken`, 400).
fn decode_marker(marker: &str) -> Result<String, Response> {
    let bytes = B64
        .decode(marker)
        .map_err(|_| invalid_token("Invalid token"))?;
    let parsed: Value =
        serde_json::from_slice(&bytes).map_err(|_| invalid_token("Invalid token"))?;
    match parsed.get("uniqueAttributes").and_then(Value::as_str) {
        Some(attribute) => Ok(attribute.to_string()),
        None => Err(invalid_token("Input inconsistent with page token")),
    }
}

// ---------------------------------------------------------------------------
// Response helpers
// ---------------------------------------------------------------------------

fn ok(body: Value) -> Response {
    Response::json(200, serde_json::to_vec(&body).unwrap_or_default())
}

fn ok_empty() -> Response {
    ok(json!({}))
}

fn invalid_input(errors: &[String]) -> Response {
    let msg = format!(
        "The requested item is not acceptable.\n\t{}",
        errors.join("\n\t")
    );
    json_error(400, "InvalidInput", &msg)
}

fn unsupported_tld(tld: &str) -> Response {
    json_error(
        400,
        "UnsupportedTLD",
        &format!("Amazon Route53 does not support the top-level domain (TLD) `.{tld}`."),
    )
}

fn duplicate_request() -> Response {
    json_error(
        400,
        "DuplicateRequest",
        "The request is already in progress for the domain.",
    )
}

fn domain_limit() -> Response {
    json_error(
        400,
        "DomainLimitExceeded",
        "The number of registered domains has exceeded the allowed threshold for this account. \
         If you want to register more domains please request a higher quota",
    )
}

fn invalid_token(message: &str) -> Response {
    json_error(400, "InvalidToken", &format!("Invalid Token: {message}"))
}

fn internal_error(message: &str) -> Response {
    json_error(500, "InternalError", message)
}

fn as_str<'a>(body: &'a Value, key: &str) -> &'a str {
    body.get(key).and_then(Value::as_str).unwrap_or("")
}

fn as_int(body: &Value, key: &str) -> Option<i64> {
    match body.get(key) {
        Some(Value::Number(n)) => n.as_i64().or_else(|| n.as_f64().map(|f| f as i64)),
        Some(Value::String(s)) => s.parse::<i64>().ok(),
        _ => None,
    }
}

/// moto's `_get_bool_param(param, if_none)`: booleans plus true/false
/// strings; everything else (absent, null) falls back to `default`.
fn as_bool(body: &Value, key: &str, default: bool) -> bool {
    match body.get(key) {
        Some(Value::Bool(b)) => *b,
        Some(Value::String(s)) if s.eq_ignore_ascii_case("true") => true,
        Some(Value::String(s)) if s.eq_ignore_ascii_case("false") => false,
        _ => default,
    }
}

fn as_str_list(value: Option<&Value>) -> Vec<String> {
    value
        .and_then(Value::as_array)
        .map(|list| {
            list.iter()
                .filter_map(Value::as_str)
                .map(str::to_string)
                .collect()
        })
        .unwrap_or_default()
}

/// Python truthiness for filter/sort objects: false for null and empty
/// containers, true otherwise.
fn truthy(value: &Value) -> bool {
    match value {
        Value::Null => false,
        Value::Array(a) => !a.is_empty(),
        Value::Object(o) => !o.is_empty(),
        _ => true,
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

    static ACCOUNTS: AtomicU64 = AtomicU64::new(300_000_000_001);

    /// Every test takes a fresh account so the shared store stays isolated.
    fn next_account() -> u64 {
        ACCOUNTS.fetch_add(1, Ordering::SeqCst)
    }

    fn send(account: u64, operation: &str, body: Value) -> (u16, Value) {
        let request = Request {
            service: "route53domains".to_string(),
            operation: operation.to_string(),
            method: "POST".to_string(),
            path: "/".to_string(),
            query_string: String::new(),
            headers: std::collections::HashMap::new(),
            body: Bytes::from(serde_json::to_vec(&body).unwrap()),
            account,
            region: "us-east-1".to_string(),
        };
        let resp = dispatch(&request).expect("route53domains dispatch");
        (
            resp.status,
            serde_json::from_slice(&resp.body).unwrap_or(json!({})),
        )
    }

    fn contact() -> Value {
        json!({
            "FirstName": "Test",
            "LastName": "User",
            "ContactType": "PERSON",
            "AddressLine1": "123 Main St",
            "City": "Seattle",
            "State": "WA",
            "CountryCode": "US",
            "ZipCode": "98101",
            "PhoneNumber": "+1.2065551234",
            "Email": "test@example.com",
        })
    }

    fn unique_domain(tag: &str) -> String {
        let hex = Uuid::new_v4().simple().to_string();
        format!("{tag}{}.com", &hex[..12])
    }

    fn register(account: u64, domain: &str, duration: i64) -> String {
        let (status, body) = send(
            account,
            "RegisterDomain",
            json!({
                "DomainName": domain,
                "DurationInYears": duration,
                "AdminContact": contact(),
                "RegistrantContact": contact(),
                "TechContact": contact(),
            }),
        );
        assert_eq!(status, 200, "register failed: {body}");
        body["OperationId"]
            .as_str()
            .expect("OperationId")
            .to_string()
    }

    #[test]
    fn domain_name_matcher_mirrors_the_regex() {
        // Valid (prefix match — trailing junk is fine).
        for name in [
            "example.com",
            "ab-c.com",
            "0-0.123",
            "a.b.c",
            "example.co.uk!",
        ] {
            assert!(is_valid_domain(name), "{name} should match");
        }
        // Invalid: missing labels, hyphen ends, dots, caps, bare labels.
        for name in [
            "", "a", "a-.com", "ab-.com", "a..b", "A.com", "a.", "bad-.com",
        ] {
            assert!(!is_valid_domain(name), "{name} should not match");
        }
    }

    #[test]
    fn register_and_get_detail() {
        let account = next_account();
        let domain = unique_domain("d1-");
        register(account, &domain, 2);

        let (status, detail) = send(account, "GetDomainDetail", json!({ "DomainName": domain }));
        assert_eq!(status, 200);
        assert_eq!(detail["DomainName"], json!(domain));
        assert_eq!(detail["AutoRenew"], json!(true));
        assert_eq!(detail["AdminPrivacy"], json!(true));
        assert_eq!(detail["RegistrarName"], "GANDI SAS");
        assert_eq!(detail["AbuseContactEmail"], "abuse@support.gandi.net");
        assert_eq!(detail["Reseller"], Value::Null);
        assert!(detail["ExpirationDate"].as_f64().is_some());
        let nameservers = detail["Nameservers"].as_array().unwrap();
        assert_eq!(nameservers.len(), 4);
        assert_eq!(nameservers[0]["Name"], "ns-2048.awscdn-64.net");
        assert!(nameservers[0].get("GlueIps").is_none());
    }

    #[test]
    fn register_duplicate_is_rejected_and_operation_log_is_permanent() {
        let account = next_account();
        let domain = unique_domain("dup-");
        let op_id = register(account, &domain, 2);

        let (status, body) = send(
            account,
            "RegisterDomain",
            json!({
                "DomainName": domain,
                "DurationInYears": 2,
                "AdminContact": contact(),
                "RegistrantContact": contact(),
                "TechContact": contact(),
            }),
        );
        assert_eq!(status, 400);
        assert_eq!(body["__type"], "DuplicateRequest");

        // The logged operation keeps its exact wire shape.
        let (status, detail) = send(
            account,
            "GetOperationDetail",
            json!({ "OperationId": op_id }),
        );
        assert_eq!(status, 200);
        assert_eq!(detail["OperationId"], json!(op_id));
        assert_eq!(detail["Status"], "SUCCESSFUL");
        assert_eq!(detail["Type"], "REGISTER_DOMAIN");
        assert_eq!(detail["StatusFlag"], Value::Null);
        assert_eq!(detail["DomainName"], json!(domain));

        // A bogus id maps to the fork's InvalidInput message.
        let (status, body) = send(
            account,
            "GetOperationDetail",
            json!({ "OperationId": "missing-id" }),
        );
        assert_eq!(status, 400);
        assert_eq!(body["__type"], "InvalidInput");
        assert_eq!(
            body["message"],
            "The requested item is not acceptable.\n\tOperation with id missing-id doesn't exist"
        );
    }

    #[test]
    fn delete_domain_paths() {
        let account = next_account();
        let domain = unique_domain("del-");
        register(account, &domain, 2);

        // Unregistered names answer the fork's ownership message.
        let (status, body) = send(
            account,
            "DeleteDomain",
            json!({ "DomainName": unique_domain("new-") }),
        );
        assert_eq!(status, 400);
        assert_eq!(body["__type"], "InvalidInput");
        assert!(body["message"]
            .as_str()
            .unwrap()
            .contains("isn't registered"));

        let (status, body) = send(account, "DeleteDomain", json!({ "DomainName": domain }));
        assert_eq!(status, 200);
        assert!(body["OperationId"].as_str().is_some());

        // Deleting again dups against the logged DELETE op (fork behavior:
        // every op type is compared against the permanent log).
        let (status, body) = send(account, "DeleteDomain", json!({ "DomainName": domain }));
        assert_eq!(status, 400);
        assert_eq!(body["__type"], "DuplicateRequest");
    }

    #[test]
    fn one_year_transfer_fails_like_the_fork() {
        let account = next_account();
        let domain = unique_domain("tr1-");
        let body_all = |years: i64, op: &str| {
            send(
                account,
                op,
                json!({
                    "DomainName": domain,
                    "DurationInYears": years,
                    "AdminContact": contact(),
                    "RegistrantContact": contact(),
                    "TechContact": contact(),
                }),
            )
        };
        // duration 1 straddles moto's 1-year expiry window → InvalidInput.
        let (status, body) = body_all(1, "TransferDomain");
        assert_eq!(status, 400);
        assert_eq!(body["__type"], "InvalidInput");
        assert!(body["message"]
            .as_str()
            .unwrap()
            .contains("ExpirationDate must by between 1 and 10 years from now"));

        // duration 2 registers the domain through the transfer op.
        let (status, _body) = body_all(2, "TransferDomain");
        assert_eq!(status, 200);
        // TransferDomain ignores the Nameservers member by default → 4.
        register(account, &domain, 2);
        let (status, detail) = send(account, "GetDomainDetail", json!({ "DomainName": domain }));
        assert_eq!(status, 200);
        assert_eq!(detail["Nameservers"].as_array().unwrap().len(), 4);
    }

    #[test]
    fn availability_transferability_suggestions_and_prices() {
        let account = next_account();
        let domain = unique_domain("av-");
        let (status, body) = send(
            account,
            "CheckDomainAvailability",
            json!({ "DomainName": domain }),
        );
        assert_eq!(status, 200);
        assert_eq!(body["Availability"], "AVAILABLE");

        register(account, &domain, 2);
        let (_, body) = send(
            account,
            "CheckDomainAvailability",
            json!({ "DomainName": domain }),
        );
        assert_eq!(body["Availability"], "UNAVAILABLE");

        let (_, body) = send(
            account,
            "CheckDomainTransferability",
            json!({ "DomainName": domain }),
        );
        assert_eq!(body["Transferability"]["Transferable"], "TRANSFERABLE");

        let (_, body) = send(
            account,
            "GetDomainSuggestions",
            json!({ "DomainName": domain, "SuggestionCount": 3, "OnlyAvailable": true }),
        );
        let suggestions = body["SuggestionsList"].as_array().unwrap();
        assert_eq!(suggestions.len(), 3);
        assert_eq!(suggestions[0]["Availability"], "AVAILABLE");

        // Suffixes cycle in fork order: online, site, tech...
        assert!(suggestions[0]["DomainName"]
            .as_str()
            .unwrap()
            .ends_with(".online"));

        let (_, body) = send(account, "ListPrices", json!({}));
        assert_eq!(body["Prices"].as_array().unwrap().len(), 5);
        let (_, body) = send(account, "ListPrices", json!({ "Tld": "com" }));
        let prices = body["Prices"].as_array().unwrap();
        assert_eq!(prices.len(), 1);
        assert_eq!(prices[0]["RegistrationPrice"]["Currency"], "USD");

        let (_, body) = send(account, "ViewBilling", json!({}));
        assert_eq!(body["BillingRecords"], json!([]));
        assert_eq!(body["NextPageMarker"], Value::Null);
    }

    #[test]
    fn tags_round_trip() {
        let account = next_account();
        let domain = unique_domain("tag-");
        register(account, &domain, 2);

        let (status, _) = send(
            account,
            "UpdateTagsForDomain",
            json!({
                "DomainName": domain,
                "TagsToUpdate": [
                    {"Key": "env", "Value": "test"},
                    {"Key": "team", "Value": "devs"},
                ],
            }),
        );
        assert_eq!(status, 200);

        let (_, body) = send(
            account,
            "ListTagsForDomain",
            json!({ "DomainName": domain }),
        );
        let tags = body["TagList"].as_array().unwrap();
        assert_eq!(tags.len(), 2);
        assert_eq!(tags[0]["Key"], "env");
        assert_eq!(tags[0]["Value"], "test");

        let (status, _) = send(
            account,
            "DeleteTagsForDomain",
            json!({ "DomainName": domain, "TagsToDelete": ["env"] }),
        );
        assert_eq!(status, 200);
        let (_, body) = send(
            account,
            "ListTagsForDomain",
            json!({ "DomainName": domain }),
        );
        let tags = body["TagList"].as_array().unwrap();
        assert_eq!(tags.len(), 1);
        assert_eq!(tags[0]["Key"], "team");

        // Unregistered names still accept tags (fork behavior).
        let (status, _) = send(
            account,
            "UpdateTagsForDomain",
            json!({
                "DomainName": unique_domain("no-"),
                "TagsToUpdate": [{"Key": "k", "Value": "v"}],
            }),
        );
        assert_eq!(status, 200);
    }

    #[test]
    fn list_domains_paginates_with_markers() {
        let account = next_account();
        let registered = vec![
            unique_domain("pg1-"),
            unique_domain("pg2-"),
            unique_domain("pg3-"),
        ];
        for domain in &registered {
            register(account, domain, 2);
        }

        let (status, page1) = send(account, "ListDomains", json!({ "MaxItems": 2 }));
        assert_eq!(status, 200);
        let got: Vec<String> = page1["Domains"]
            .as_array()
            .unwrap()
            .iter()
            .map(|d| d["DomainName"].as_str().unwrap().to_string())
            .collect();
        assert_eq!(got, registered[0..2]);
        assert_eq!(page1["Domains"][0]["TransferLock"], json!(true));
        let marker = page1["NextPageMarker"]
            .as_str()
            .expect("marker")
            .to_string();

        let (status, page2) = send(
            account,
            "ListDomains",
            json!({ "MaxItems": 2, "Marker": marker }),
        );
        assert_eq!(status, 200);
        let got: Vec<String> = page2["Domains"]
            .as_array()
            .unwrap()
            .iter()
            .map(|d| d["DomainName"].as_str().unwrap().to_string())
            .collect();
        assert_eq!(got, registered[2..3]);
        assert!(page2["NextPageMarker"].is_null());

        // A marker naming a gone item raises moto's InvalidToken.
        let (status, body) = send(
            account,
            "ListDomains",
            json!({ "Marker": encode_marker("not-there.com") }),
        );
        assert_eq!(status, 400);
        assert_eq!(body["__type"], "InvalidToken");

        // DomainName filters: prefix match.
        let (status, body) = send(
            account,
            "ListDomains",
            json!({
                "FilterConditions": [
                    {"Name": "DomainName", "Operator": "BEGINS_WITH", "Values": [&registered[0]]}
                ]
            }),
        );
        assert_eq!(status, 200);
        assert_eq!(body["Domains"].as_array().unwrap().len(), 1);

        // A filter/sort pairing mismatch is InvalidInput.
        let (status, body) = send(
            account,
            "ListDomains",
            json!({
                "FilterConditions": [
                    {"Name": "DomainName", "Operator": "BEGINS_WITH", "Values": ["x"]}
                ],
                "SortCondition": {"Name": "Expiry", "SortOrder": "ASC"},
            }),
        );
        assert_eq!(status, 400);
        assert_eq!(body["__type"], "InvalidInput");
    }

    #[test]
    fn list_operations_filters_and_sorts() {
        let account = next_account();
        let domain = unique_domain("op-");
        register(account, &domain, 2);

        let (_, body) = send(
            account,
            "ListOperations",
            json!({ "Status": ["SUCCESSFUL"], "Type": ["REGISTER_DOMAIN"] }),
        );
        let ops = body["Operations"].as_array().unwrap();
        assert!(!ops.is_empty());
        assert_eq!(ops[0]["Type"], "REGISTER_DOMAIN");

        // An unknown status value is InvalidInput.
        let (status, body) = send(
            account,
            "ListOperations",
            json!({ "Status": ["NOT_A_STATUS"] }),
        );
        assert_eq!(status, 400);
        assert_eq!(
            body["message"],
            "The requested item is not acceptable.\n\tStatus is invalid"
        );

        // The fork's quirk: ASC sorts in reverse (newest first).
        let second_domain = unique_domain("op2-");
        register(account, &second_domain, 2);
        let (_, body) = send(
            account,
            "ListOperations",
            json!({ "Status": ["SUCCESSFUL"], "Type": ["REGISTER_DOMAIN"], "SortBy": "SubmittedDate", "SortOrder": "ASC" }),
        );
        let ops = body["Operations"].as_array().unwrap();
        assert!(ops.len() >= 2);
        assert_eq!(ops[ops.len() - 1]["Type"], "REGISTER_DOMAIN");
    }

    #[test]
    fn internal_transfer_flow() {
        let account = next_account();
        let domain = unique_domain("xfr-");
        register(account, &domain, 2);

        let (status, body) = send(
            account,
            "TransferDomainToAnotherAwsAccount",
            json!({ "DomainName": domain, "AccountId": "987654321012" }),
        );
        assert_eq!(status, 200);
        let password = body["Password"].as_str().expect("Password").to_string();
        assert_eq!(password.len(), 12);
        assert!(password.contains('-'));
        let _ = body["OperationId"].as_str().expect("OperationId");

        // A wrong password is rejected...
        let (status, body) = send(
            account,
            "AcceptDomainTransferFromAnotherAwsAccount",
            json!({ "DomainName": domain, "Password": "wrong-password" }),
        );
        assert_eq!(status, 400);
        assert_eq!(body["__type"], "InvalidInput");
        assert_eq!(
            body["message"],
            "The requested item is not acceptable.\n\tInvalid password for transfer"
        );

        // ...and the correct one completes the transfer.
        let (status, _) = send(
            account,
            "AcceptDomainTransferFromAnotherAwsAccount",
            json!({ "DomainName": domain, "Password": password }),
        );
        assert_eq!(status, 200);

        // An unknown domain cannot start a transfer.
        let (status, body) = send(
            account,
            "TransferDomainToAnotherAwsAccount",
            json!({ "DomainName": unique_domain("cap-"), "AccountId": "987654321012" }),
        );
        assert_eq!(status, 400);
        assert_eq!(body["__type"], "InvalidInput");

        // Cancel pops any pending transfer without error.
        let (status, body) = send(
            account,
            "CancelDomainTransferToAnotherAwsAccount",
            json!({ "DomainName": domain }),
        );
        assert_eq!(status, 200);
        assert!(body["OperationId"].as_str().is_some());

        // Reject behaves the same.
        let (status, _) = send(
            account,
            "RejectDomainTransferFromAnotherAwsAccount",
            json!({ "DomainName": domain }),
        );
        assert_eq!(status, 200);
    }

    #[test]
    fn contact_and_tld_validation() {
        let account = next_account();
        let mut bad_phone = contact();
        bad_phone["PhoneNumber"] = json!("2065551234");
        let (status, body) = send(
            account,
            "RegisterDomain",
            json!({
                "DomainName": unique_domain("ph-"),
                "DurationInYears": 2,
                "AdminContact": bad_phone,
                "RegistrantContact": bad_phone,
                "TechContact": bad_phone,
            }),
        );
        assert_eq!(status, 400);
        assert_eq!(body["__type"], "InvalidInput");
        assert!(body["message"]
            .as_str()
            .unwrap()
            .contains("PhoneNumber is in an invalid format"));

        // Unsupported TLD is raised even when the name is well-formed.
        let (status, body) = send(
            account,
            "RegisterDomain",
            json!({
                "DomainName": unique_domain("wp-").replace(".com", ".xyzzy"),
                "DurationInYears": 2,
                "AdminContact": contact(),
                "RegistrantContact": contact(),
                "TechContact": contact(),
            }),
        );
        assert_eq!(status, 400);
        assert_eq!(body["__type"], "UnsupportedTLD");
        assert!(body["message"].as_str().unwrap().contains(".xyzzy`"));
    }

    #[test]
    fn auto_renew_lock_and_nameservers() {
        let account = next_account();
        let domain = unique_domain("ns-");
        register(account, &domain, 2);

        // Auto-renew toggles survive a detail read.
        let (status, _) = send(
            account,
            "DisableDomainAutoRenew",
            json!({ "DomainName": domain }),
        );
        assert_eq!(status, 200);
        let (_, detail) = send(account, "GetDomainDetail", json!({ "DomainName": domain }));
        assert_eq!(detail["AutoRenew"], json!(false));
        let (status, _) = send(
            account,
            "EnableDomainAutoRenew",
            json!({ "DomainName": domain }),
        );
        assert_eq!(status, 200);

        // Transfer locks hand back an operation id (type DOMAIN_LOCK).
        let (status, body) = send(
            account,
            "EnableDomainTransferLock",
            json!({ "DomainName": domain }),
        );
        assert_eq!(status, 200);
        assert!(body["OperationId"].as_str().is_some());

        // Nameserver updates replace the stored set.
        let (status, _) = send(
            account,
            "UpdateDomainNameservers",
            json!({
                "DomainName": domain,
                "Nameservers": [
                    {"Name": "ns1.example.com", "GlueIps": ["192.0.2.1"]},
                    {"Name": "ns2.example.com"},
                ],
            }),
        );
        assert_eq!(status, 200);
        let (_, detail) = send(account, "GetDomainDetail", json!({ "DomainName": domain }));
        let nameservers = detail["Nameservers"].as_array().unwrap();
        assert_eq!(nameservers.len(), 2);
        assert_eq!(nameservers[0]["GlueIps"], json!(["192.0.2.1"]));
        assert!(nameservers[1].get("GlueIps").is_none());

        // A subdomain-of-domain nameserver needs glue IPs.
        let (status, body) = send(
            account,
            "UpdateDomainNameservers",
            json!({
                "DomainName": domain,
                "Nameservers": [{"Name": format!("ns.{domain}")}],
            }),
        );
        assert_eq!(status, 400);
        assert!(body["message"]
            .as_str()
            .unwrap()
            .contains("Must supply glue IPs"));

        // Unregistered domains answer the fork's ownership message.
        let (status, _) = send(
            account,
            "UpdateDomainNameservers",
            json!({
                "DomainName": unique_domain("nn-"),
                "Nameservers": [{"Name": "ns1.example.com"}],
            }),
        );
        assert_eq!(status, 400);
    }

    #[test]
    fn contacts_privacy_handshake_and_authcode() {
        let account = next_account();
        let domain = unique_domain("hr-");
        register(account, &domain, 2);

        let mut updated = contact();
        updated["FirstName"] = json!("Updated");
        updated["ContactType"] = json!("COMPANY"); // requires OrganizationName
        let (status, body) = send(
            account,
            "RegisterDomain",
            json!({
                "DomainName": unique_domain("or-"),
                "DurationInYears": 2,
                "AdminContact": updated,
                "RegistrantContact": updated,
                "TechContact": updated,
            }),
        );
        assert_eq!(status, 400);
        assert!(body["message"]
            .as_str()
            .unwrap()
            .contains("Must supply OrganizationName when ContactType is not PERSON"));

        let (status, _) = send(
            account,
            "UpdateDomainContact",
            json!({ "DomainName": domain, "AdminContact": {
                "FirstName": "Second", "LastName": "User", "ContactType": "PERSON",
            } }),
        );
        assert_eq!(status, 200);
        let (_, detail) = send(account, "GetDomainDetail", json!({ "DomainName": domain }));
        assert_eq!(detail["AdminContact"]["FirstName"], "Second");
        // The empty rendered BillingContact mirrors admin contact fields...
        assert!(detail["BillingContact"].get("FirstName").is_some());

        let (status, _) = send(
            account,
            "UpdateDomainContactPrivacy",
            json!({
                "DomainName": domain,
                "AdminPrivacy": false,
                "RegistrantPrivacy": false,
                "TechPrivacy": false,
            }),
        );
        assert_eq!(status, 200);
        let (_, detail) = send(account, "GetDomainDetail", json!({ "DomainName": domain }));
        assert_eq!(detail["AdminPrivacy"], json!(false));
        assert_eq!(detail["RegistrantPrivacy"], json!(false));

        // Reachability endpoints answer lowercase-key shapes.
        let (_, body) = send(
            account,
            "GetContactReachabilityStatus",
            json!({ "domainName": domain }),
        );
        assert_eq!(body["status"], "DONE");
        assert_eq!(body["domainName"], json!(domain));
        let (_, body) = send(
            account,
            "ResendContactReachabilityEmail",
            json!({ "domainName": domain }),
        );
        assert_eq!(body["isAlreadyVerified"], json!(true));
        assert_eq!(body["emailAddress"], "registrant@example.com");

        // Auth codes take moto's 12-char uuid slice shape.
        let (status, body) = send(
            account,
            "RetrieveDomainAuthCode",
            json!({ "DomainName": domain }),
        );
        assert_eq!(status, 200);
        assert_eq!(body["AuthCode"].as_str().unwrap().len(), 12);

        // Renew extends the domain expiry (never shorter).
        let (status, _) = send(
            account,
            "RenewDomain",
            json!({ "DomainName": domain, "CurrentExpiryYear": 2027, "DurationInYears": 1 }),
        );
        assert_eq!(status, 200);
    }

    #[test]
    fn dnssec_ops_log_operations() {
        let account = next_account();
        let domain = unique_domain("dns-");
        register(account, &domain, 2);

        let (status, body) = send(
            account,
            "AssociateDelegationSignerToDomain",
            json!({
                "DomainName": domain,
                "SigningAttributes": {"Algorithm": 13, "Flags": 257, "PublicKey": "dGVzdA=="},
            }),
        );
        assert_eq!(status, 200);
        assert!(body["OperationId"].as_str().is_some());

        let (status, _) = send(
            account,
            "DisassociateDelegationSignerFromDomain",
            json!({ "DomainName": domain, "Id": "fake-signer-id" }),
        );
        assert_eq!(status, 200);

        // An unregistered domain answers the ownership error.
        let (status, body) = send(
            account,
            "AssociateDelegationSignerToDomain",
            json!({
                "DomainName": "nonexistent-domain-12345.com",
                "SigningAttributes": {"Algorithm": 13},
            }),
        );
        assert_eq!(status, 400);
        assert_eq!(body["__type"], "InvalidInput");
        assert_eq!(
            body["message"],
            "The requested item is not acceptable.\n\tDomain is not associated with this account"
        );
    }

    #[test]
    fn push_domain_removes_the_record() {
        let account = next_account();
        let domain = unique_domain("push-");
        register(account, &domain, 2);

        let (status, _) = send(
            account,
            "PushDomain",
            json!({ "DomainName": domain, "Target": "ANOTHER_REGISTRAR" }),
        );
        assert_eq!(status, 200);

        // The record is gone; the domain answers as unowned.
        let (status, _) = send(account, "GetDomainDetail", json!({ "DomainName": domain }));
        assert_eq!(status, 400);

        // Pushing a missing domain is the fork's InvalidInput.
        let (status, _) = send(
            account,
            "PushDomain",
            json!({ "DomainName": domain, "Target": "ANOTHER_REGISTRAR" }),
        );
        assert_eq!(status, 400);
    }

    #[test]
    fn unsupported_service_is_not_armed() {
        let request = Request {
            service: "route53resolver".to_string(),
            operation: "SomeOp".to_string(),
            method: "POST".to_string(),
            path: "/".to_string(),
            query_string: String::new(),
            headers: std::collections::HashMap::new(),
            body: Bytes::new(),
            account: next_account(),
            region: "us-east-1".to_string(),
        };
        // The service guard routes only its own service...
        assert!(super::dispatch(&request).is_none());
        assert!(handles_service_compatible("route53domains"));
    }

    fn handles_service_compatible(name: &str) -> bool {
        crate::services::handles_service(name)
    }
}
