//! AWS Directory Service backend — moto-equivalent (json protocol).
//!
//! Replaces the moto sidecar for `ds` (`X-Amz-Target: DirectoryService_20150416.<Op>`
//! on `application/x-amz-json-1.1`). State is per-(account, region), mirroring
//! moto's `DirectoryServiceBackend` (moto/ds/models.py): directories, trusts,
//! snapshots, certificates, log subscriptions, conditional forwarders, event
//! topics, IP routes, schema extensions, LDAPS settings, RADIUS + client-auth
//! settings, computers, shares, and moto's `TaggingService`.
//!
//! Contract: probes/ds.json (per-operation moto-era behavior) and
//! tests/compatibility/test_ds_compat.py.
//!
//! Deliberate shapes (same approach as the wave-2 ports):
//! - Directory creation validates subnets/VPCs against the *native* EC2
//!   state (`services::ec2::state::shared()`), like moto's
//!   `ec2_backends[...].describe_subnets/describe_vpcs` calls, and creates a
//!   real security group + one ENI per subnet (moto's `Directory.__init__`).
//! - Four ops are double-absent (probe `not_implemented` AND no routed
//!   handler in the deployed moto fork): CreateHybridAD,
//!   DescribeHybridADUpdate, UpdateHybridAD, UpdateTrust → moto's
//!   `NotImplementedError` 501.
//! - Sixteen ops are routed in the fork's responses.py but its models.py has
//!   no backend method (moto would crash with AttributeError → HTML 500).
//!   The pre-rotocore probes recorded those paths `working` with modeled
//!   errors, so they get real behavior instead: the share family
//!   (probe: "Shared directory … does not exist"), region/setup updates,
//!   the AD-assessment starter, CA-enrollment toggles and the data-access
//!   trio (mq-style normalization of moto's unguarded crash paths).
//! - `ADAssessment` describe keeps moto's always-not-found backend.
//! - moto paginates every describe/list families with botocore-style tokens;
//!   the token codec here mirrors `moto/utilities/paginator.py` (base64 of
//!   `{"parameterChecksum", "uniqueAttributes"}`), and the two callers that
//!   convert moto's `InvalidToken` map it to `InvalidNextTokenException`.
//!   Families moto paginated without a unique attribute split by limit only.

use base64::engine::general_purpose::STANDARD as B64;
use base64::Engine as _;
use parking_lot::RwLock;
use serde_json::{json, Map, Value};
use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;

use crate::proto::{json_error, now_unix, parse_json_body};
use crate::services::ec2;
use crate::store::{self, ScopeKey};
use crate::{Request, Response};

// ---------------------------------------------------------------------------
// State
// ---------------------------------------------------------------------------

type Stores = Arc<RwLock<HashMap<ScopeKey, DsStore>>>;

fn stores() -> Stores {
    store::slot::<DsStore>("ds")
}

fn with_store(req: &Request, f: impl FnOnce(&mut DsStore) -> Response) -> Response {
    let slot = stores();
    let mut map = slot.write();
    let st = map.entry((req.account, req.region.clone())).or_default();
    f(st)
}

/// Per-(account, region) in-memory state for AWS Directory Service.
#[derive(Default)]
struct DsStore {
    /// directory id -> directory (moto's `directories`)
    dirs: BTreeMap<String, Directory>,
    /// snapshot id -> snapshot
    snaps: BTreeMap<String, Snapshot>,
    /// certificate id -> certificate
    certs: BTreeMap<String, Cert>,
    /// shares created by ShareDirectory (accept/reject lookups)
    shares: Vec<Share>,
    /// (directory_id, domain) -> dns ips (moto's `conditional_forwarders`)
    cond_fwd: BTreeMap<(String, String), Vec<String>>,
    /// (directory_id, topic_name) -> event topic
    topics: BTreeMap<(String, String), Value>,
    /// (directory_id, cidr, added_time, description)
    ip_routes: Vec<(String, String, f64, String)>,
    /// schema extension id -> schema extension
    schema_exts: BTreeMap<String, SchemaExt>,
    /// directory id -> log subscription (moto: one per directory)
    log_subs: BTreeMap<String, LogSub>,
    /// directory id -> RadiusSettings copy
    radius: BTreeMap<String, Value>,
    /// directory id -> [(type, status, last_updated)]
    client_auth: BTreeMap<String, Vec<(String, String, f64)>>,
    /// directory id -> computer dicts
    computers: BTreeMap<String, Vec<Value>>,
    /// resource id -> [(key, value)] in insertion order (moto TaggingService)
    tags: BTreeMap<String, Vec<(String, Option<String>)>>,
    /// creation counter for stable sort tie-breaks (python's dict order)
    seq: u64,
}

/// Shared-directory record behind the ShareDirectory/AcceptSharedDirectory
/// family (moto routes the handlers but never implemented the backend).
struct Share {
    shared_directory_id: String,
    owner_directory_id: String,
    owner_account: u64,
    shared_account_id: String,
    method: String,
    notes: String,
    status: String,
}

struct Directory {
    region: String,
    name: String,
    directory_type: String, // SimpleAD | MicrosoftAD | ADConnector
    size: Option<String>,
    short_name: Option<String>,
    description: Option<String>,
    edition: Option<String>,
    vpc_settings: Option<Map<String, Value>>,
    connect_settings: Option<Map<String, Value>>,
    directory_id: String,
    alias: String,
    access_url: String,
    desired_number_of_domain_controllers: i64,
    sso_enabled: bool,
    stage: String,
    launch_time: f64,
    stage_last_updated: f64,
    /// LDAPS setting entries (moto's `ldaps_settings_info`)
    ldaps: Vec<Value>,
    trusts: Vec<Trust>,
    /// MicrosoftAD security settings (moto's SETTINGS_ENTRIES_MODEL copy)
    msad_settings: Option<Vec<Value>>,
    dns_ip_addrs: Vec<String>,
    sg_id: String,
    eni_ids: Vec<String>,
    /// Directory DataAccess enablement (Enable/DisableDirectoryDataAccess)
    data_access_status: String,
    seq: u64,
}

#[derive(Clone)]
struct Trust {
    trust_id: String,
    directory_id: String,
    remote_domain_name: String,
    trust_direction: String,
    trust_type: Option<String>,
    selective_auth: Option<String>,
    trust_state: String,
    trust_state_reason: String,
    created: f64,
    last_updated: f64,
    state_last_updated: f64,
}

struct Snapshot {
    snapshot_id: String,
    directory_id: String,
    name: String,
    start: f64,
}

struct Cert {
    certificate_id: String,
    directory_id: String,
    client_cert_auth_settings: Option<Value>,
    cert_type: String,
    registered: f64,
    expiry: f64,
}

struct SchemaExt {
    schema_extension_id: String,
    directory_id: String,
    description: String,
    status: String,
    status_reason: String,
    start: f64,
    end: f64,
}

struct LogSub {
    directory_id: String,
    log_group_name: String,
    created: f64,
}

/// moto's fake settings model (moto/ds/utils.py SETTINGS_ENTRIES_MODEL).
const SETTINGS_ENTRIES: &str = r#"[
 {"Type": "Protocol", "Name": "PCT_1_0", "AllowedValues": "[\"Enable\",\"Disable\"]", "AppliedValue": "Enable", "RequestStatus": "Default", "DataType": "Boolean"},
 {"Type": "Protocol", "Name": "SSL_2_0", "AllowedValues": "[\"Enable\",\"Disable\"]", "AppliedValue": "Enable", "RequestStatus": "Default", "DataType": "Boolean"},
 {"Type": "Protocol", "Name": "SSL_3_0", "AllowedValues": "[\"Enable\",\"Disable\"]", "AppliedValue": "Enable", "RequestStatus": "Default", "DataType": "Boolean"},
 {"Type": "Protocol", "Name": "TLS_1_0", "AllowedValues": "[\"Enable\",\"Disable\"]", "AppliedValue": "Enable", "RequestStatus": "Default", "DataType": "Boolean"},
 {"Type": "Protocol", "Name": "TLS_1_1", "AllowedValues": "[\"Enable\",\"Disable\"]", "AppliedValue": "Enable", "RequestStatus": "Default", "DataType": "Boolean"},
 {"Type": "Cipher", "Name": "AES_128_128", "AllowedValues": "[\"Enable\",\"Disable\"]", "AppliedValue": "Enable", "RequestStatus": "Default", "DataType": "Boolean"},
 {"Type": "Cipher", "Name": "DES_56_56", "AllowedValues": "[\"Enable\",\"Disable\"]", "AppliedValue": "Enable", "RequestStatus": "Default", "DataType": "Boolean"},
 {"Type": "Cipher", "Name": "RC2_40_128", "AllowedValues": "[\"Enable\",\"Disable\"]", "AppliedValue": "Enable", "RequestStatus": "Default", "DataType": "Boolean"},
 {"Type": "Cipher", "Name": "RC2_56_128", "AllowedValues": "[\"Enable\",\"Disable\"]", "AppliedValue": "Enable", "RequestStatus": "Default", "DataType": "Boolean"},
 {"Type": "Cipher", "Name": "RC2_128_128", "AllowedValues": "[\"Enable\",\"Disable\"]", "AppliedValue": "Enable", "RequestStatus": "Default", "DataType": "Boolean"},
 {"Type": "Cipher", "Name": "RC4_40_128", "AllowedValues": "[\"Enable\",\"Disable\"]", "AppliedValue": "Enable", "RequestStatus": "Default", "DataType": "Boolean"},
 {"Type": "Cipher", "Name": "RC4_56_128", "AllowedValues": "[\"Enable\",\"Disable\"]", "AppliedValue": "Enable", "RequestStatus": "Default", "DataType": "Boolean"},
 {"Type": "Cipher", "Name": "RC4_64_128", "AllowedValues": "[\"Enable\",\"Disable\"]", "AppliedValue": "Enable", "RequestStatus": "Default", "DataType": "Boolean"},
 {"Type": "Cipher", "Name": "RC4_128_128", "AllowedValues": "[\"Enable\",\"Disable\"]", "AppliedValue": "Enable", "RequestStatus": "Default", "DataType": "Boolean"},
 {"Type": "Cipher", "Name": "3DES_168_168", "AllowedValues": "[\"Enable\",\"Disable\"]", "AppliedValue": "Enable", "RequestStatus": "Default", "DataType": "Boolean"},
 {"Type": "Certificate-Based Authentication", "Name": "CERTIFICATE_BACKDATING_COMPENSATION", "AllowedValues": "{\"MinimumValue\": 1, \"MaximumValue\": 1577000000}", "AppliedValue": "600", "RequestStatus": "Default", "DataType": "DurationInSeconds"},
 {"Type": "Certificate-Based Authentication", "Name": "CERTIFICATE_STRONG_ENFORCEMENT", "AllowedValues": "[\"Compatibility\",\"Full_Enforcement\"]", "AppliedValue": "Compatibility", "RequestStatus": "Default", "DataType": "Enum"}
]"#;

fn msad_settings_entries() -> Vec<Value> {
    serde_json::from_str::<Value>(SETTINGS_ENTRIES)
        .ok()
        .and_then(|v| v.as_array().cloned())
        .unwrap_or_default()
}

/// moto uses `mock_random.get_random_hex(n)` — lowercase hex ids.
fn rand_hex(n: usize) -> String {
    hex::encode(uuid::Uuid::new_v4()).chars().take(n).collect()
}

fn new_id(prefix: &str, n: usize) -> String {
    format!("{prefix}-{}", rand_hex(n))
}

// ---------------------------------------------------------------------------
// Wire helpers
// ---------------------------------------------------------------------------

fn ok(body: Value) -> Response {
    Response::json(200, serde_json::to_vec(&body).unwrap_or_default())
}

/// moto returns `""` for the sso/LDAPS toggles; boto3 parses an empty body
/// as `{}`.
fn ok_empty() -> Response {
    Response::json(200, Vec::new())
}

fn err(status: u16, code: &str, msg: &str) -> Response {
    json_error(status, code, msg)
}

fn client_exception(msg: &str) -> Response {
    err(400, "ClientException", msg)
}

fn entity_does_not_exist(msg: &str) -> Response {
    err(400, "EntityDoesNotExistException", msg)
}

fn entity_already_exists(msg: &str) -> Response {
    err(400, "EntityAlreadyExistsException", msg)
}

fn invalid_parameter(msg: &str) -> Response {
    err(400, "InvalidParameterException", msg)
}

fn unsupported(msg: &str) -> Response {
    err(400, "UnsupportedOperationException", msg)
}

fn validation_exception(msg: &str) -> Response {
    err(400, "ValidationException", msg)
}

fn invalid_next_token() -> Response {
    err(
        400,
        "InvalidNextTokenException",
        "Invalid value passed for the NextToken parameter",
    )
}

/// moto's missing-route answer: `NotImplementedError` at 501 with the
/// snake-cased action name (bedrockagent convention).
fn not_implemented(op: &str) -> Response {
    json_error(
        501,
        "NotImplemented",
        &format!("The {} action has not been implemented", snake_case(op)),
    )
}

/// moto's `camelcase_to_underscores` (moto/core/utils.py) — two backward
/// substitutions; the same transform that names moto's handler methods.
fn snake_case(op: &str) -> String {
    let chars: Vec<char> = op.chars().collect();
    let mut out = String::with_capacity(chars.len() + 8);
    for (i, &c) in chars.iter().enumerate() {
        if i > 0 {
            let prev = chars[i - 1];
            let next_lower = chars
                .get(i + 1)
                .map(|c| c.is_ascii_lowercase())
                .unwrap_or(false);
            let camel_transition =
                c.is_ascii_uppercase() && (next_lower || !prev.is_ascii_uppercase());
            if camel_transition {
                out.push('_');
            }
        }
        out.push(c);
    }
    out.to_lowercase()
}

/// Pull a body field (missing → Value::Null, matching moto's `_get_param`).
fn p<'a>(body: &'a Value, key: &str) -> &'a Value {
    body.get(key).unwrap_or(&Value::Null)
}

fn str_param(v: &Value) -> Option<String> {
    v.as_str().map(|s| s.to_string())
}

fn array_strs(v: &Value) -> Vec<String> {
    v.as_array()
        .map(|l| l.iter().filter_map(str_param).collect())
        .unwrap_or_default()
}

fn int_param(v: &Value) -> Option<i64> {
    v.as_i64()
}

/// Canonical pagination input for the token checksum (moto hashes its
/// backend kwargs; ours hashes the body minus pagination keys).
fn params_context(body: &Value, skip: &[&str]) -> String {
    let mut out = String::new();
    if let Value::Object(m) = body {
        for (k, v) in m {
            if skip.contains(&k.as_str()) {
                continue;
            }
            let norm = if let Some(s) = v.as_str() {
                s.to_string()
            } else if let Some(l) = v.as_array() {
                l.iter()
                    .map(|e| e.as_str().unwrap_or(&e.to_string()).to_string())
                    .collect::<Vec<_>>()
                    .join(",")
            } else {
                v.to_string()
            };
            out.push_str(&format!("{k}={norm}&"));
        }
    }
    out
}

// ---------------------------------------------------------------------------
// Pagination (moto/utilities/paginator.py + botocore token codec)
// ---------------------------------------------------------------------------

fn encode_token(checksum: u64, unique: &str) -> String {
    let dict = json!({"parameterChecksum": checksum, "uniqueAttributes": unique});
    B64.encode(serde_json::to_vec(&dict).unwrap_or_default())
}

/// Decode a NextToken into (checksum, unique attrs). Absent/empty/null
/// tokens mean "start at the beginning".
fn decode_token(token: &Value) -> Result<Option<(u64, String)>, ()> {
    let s = match token.as_str() {
        Some(s) if !s.is_empty() => s,
        _ => return Ok(None),
    };
    let raw = B64.decode(s.as_bytes()).map_err(|_| ())?;
    let parsed: Value = serde_json::from_slice(&raw).map_err(|_| ())?;
    let cs = parsed
        .get("parameterChecksum")
        .and_then(Value::as_u64)
        .ok_or(())?;
    let unique = parsed
        .get("uniqueAttributes")
        .and_then(Value::as_str)
        .ok_or(())?
        .to_string();
    Ok(Some((cs, unique)))
}

/// FNV-1a over the pagination context (moto hashes input kwargs into the
/// token; ours is stable across calls).
fn checksum(input: &str) -> u64 {
    let mut h: u64 = 0xcbf29ce484222325;
    for b in input.as_bytes() {
        h ^= u64::from(*b);
        h = h.wrapping_mul(0x100000001b3);
    }
    h
}

/// moto's paginator: limit (falling back when absent/0), resumption after
/// the token's unique value, next token from the item *after* the page.
/// Invalid tokens raise InvalidNextTokenException (ds wraps moto's
/// InvalidToken on describe_directories/list_tags; normalized elsewhere).
fn paginate<T: Clone>(
    items: Vec<T>,
    limit: Option<i64>,
    limit_default: i64,
    context: &str,
    next_token: &Value,
    unique: impl Fn(&T) -> String,
) -> Result<(Vec<T>, Option<String>), Response> {
    let mut index_start = 0usize;
    let decoded = decode_token(next_token).map_err(|_| invalid_next_token())?;
    if let Some((cs, uniq)) = decoded {
        if cs != checksum(context) {
            return Err(invalid_next_token());
        }
        match items.iter().position(|i| unique(i) == uniq) {
            Some(pos) => index_start = pos,
            None => return Err(invalid_next_token()),
        }
    }
    let max = match limit {
        Some(l) if l != 0 => l as usize,
        _ => limit_default as usize,
    };
    let index_end = (index_start + max).min(items.len());
    let page: Vec<T> = items[index_start..index_end].to_vec();
    let token = if !page.is_empty() && index_end < items.len() {
        Some(encode_token(checksum(context), &unique(&items[index_end])))
    } else {
        None
    };
    Ok((page, token))
}

// ---------------------------------------------------------------------------
// moto/ds/validations.py — accumulated DsValidationException errors
// ---------------------------------------------------------------------------

fn validate_args(pairs: &[(&str, Value)]) -> Option<Response> {
    let mut failed: Vec<(String, String, String)> = Vec::new();
    for (field, value) in pairs {
        let msg = match *field {
            "alias" => validate_alias(value),
            "description" => validate_description(value),
            "directoryId" => validate_directory_id(value),
            "connectSettings.customerDnsIps" => validate_dns_ips(value),
            "edition" => validate_edition(value),
            "name" => validate_name(value),
            "password" => validate_password(value),
            "shortName" => validate_short_name(value),
            "size" => validate_size(value),
            "ssoPassword" => validate_sso_password(value),
            "connectSettings.vpcSettings.subnetIds" => validate_subnet_ids(value),
            "connectSettings.customerUserName" => validate_user_name(value),
            "userName" => validate_user_name(value),
            "vpcSettings.subnetIds" => validate_subnet_ids(value),
            "trustDirection" => validate_trust_direction(value),
            "remoteDomainName" => validate_remote_domain_name(value),
            _ => "",
        };
        if !msg.is_empty() {
            failed.push((
                (*field).to_string(),
                value_to_string(value),
                msg.to_string(),
            ));
        }
    }
    if failed.is_empty() {
        return None;
    }
    Some(ds_validation_exception(&failed))
}

fn value_to_string(v: &Value) -> String {
    match v {
        Value::Null => "None".to_string(),
        Value::String(s) => s.clone(),
        _ => v.to_string(),
    }
}

/// DsValidationException (moto/ds/exceptions.py): one 400 with all errors
/// concatenated; values containing "assword" print "at" instead of the
/// value (moto's redaction quirk).
fn ds_validation_exception(errors: &[(String, String, String)]) -> Response {
    let mut parts = Vec::new();
    for (name, value, constraint) in errors {
        let shown = if name.contains("assword") {
            "at".to_string()
        } else {
            format!("'{}' at", value)
        };
        parts.push(format!(
            "Value {} '{}' failed to satisfy constraint: Member must {}",
            shown, name, constraint
        ));
    }
    let leader = if errors.len() > 1 {
        format!("{} validation errors detected: ", errors.len())
    } else {
        "1 validation error detected: ".to_string()
    };
    err(
        400,
        "ValidationException",
        &format!("{}{}", leader, parts.join("; ")),
    )
}

fn validate_alias(v: &Value) -> &'static str {
    let s = str_param(v).unwrap_or_default();
    if s.chars().count() > 62 {
        return "have length less than or equal to 62";
    }
    if !alias_pattern_ok(&s) {
        return "satisfy regular expression pattern: ^(?s)(?!)(.*)";
    }
    ""
}

/// `^(?!D-|d-)([\da-zA-Z]+)([-]*[\da-zA-Z])*$` (moto's `re.match` prefix
/// semantics — anchored here because both patterns start with `^`).
fn alias_pattern_ok(s: &str) -> bool {
    if s.starts_with("D-") || s.starts_with("d-") {
        return false;
    }
    let b: Vec<char> = s.chars().collect();
    let alnum = |c: char| c.is_ascii_alphanumeric() || c == '_';
    let mut i = 0usize;
    while i < b.len() && alnum(b[i]) {
        i += 1;
    }
    if i == 0 {
        return false;
    }
    while i < b.len() {
        let mut j = i;
        while j < b.len() && b[j] == '-' {
            j += 1;
        }
        let mut k = j;
        while k < b.len() && (b[k].is_ascii_alphanumeric()) {
            k += 1;
        }
        if k == j {
            return i == b.len();
        }
        i = k;
    }
    true
}

fn validate_description(v: &Value) -> &'static str {
    let s = str_param(v).unwrap_or_default();
    if !s.is_empty() && s.len() > 128 {
        "have length less than or equal to 128"
    } else {
        ""
    }
}

/// `^d-[0-9a-f]{10}$`
fn validate_directory_id(v: &Value) -> &'static str {
    let s = str_param(v).unwrap_or_default();
    let b = s.as_bytes();
    let ok = b.len() == 12
        && b[0] == b'd'
        && b[1] == b'-'
        && b[2..]
            .iter()
            .all(|c| c.is_ascii_digit() || (b'a'..=b'f').contains(c));
    if ok {
        ""
    } else {
        "satisfy regular expression pattern: ^d-[0-9a-f]{10}$"
    }
}

/// IPv4 dotted-quad, no octet above 255.
fn validate_dns_ips(v: &Value) -> &'static str {
    let list = match v {
        Value::Array(l) => l,
        _ => return "",
    };
    for entry in list {
        let ip = str_param(entry).unwrap_or_default();
        let octets: Vec<&str> = ip.split('.').collect();
        let valid = octets.len() == 4
            && octets.iter().all(|o| {
                !o.is_empty()
                    && o.len() <= 3
                    && o.chars().all(|c| c.is_ascii_digit())
                    && o.parse::<u32>().map(|n| n <= 255).unwrap_or(false)
            });
        if !valid {
            return "satisfy regular expression pattern: ^(?:(?:25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)\\.){3}(?:25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)$";
        }
    }
    ""
}

fn validate_edition(v: &Value) -> &'static str {
    let s = str_param(v).unwrap_or_default();
    if !s.is_empty() && s != "Enterprise" && s != "Standard" {
        "satisfy enum value set: [Enterprise, Standard]"
    } else {
        ""
    }
}

/// `^([a-zA-Z0-9]+[\.-])+([a-zA-Z0-9])+$` — dotted/dashed labels.
fn validate_name(v: &Value) -> &'static str {
    const MSG: &str = "satisfy regular expression pattern: ^([a-zA-Z0-9]+[\\.-])+([a-zA-Z0-9])+$";
    let s = str_param(v).unwrap_or_default();
    let b = s.as_bytes();
    let label = |from: usize| -> usize {
        let mut k = from;
        while k < b.len() && (b[k].is_ascii_alphanumeric()) {
            k += 1;
        }
        k
    };
    let mut i = 0usize;
    loop {
        let end = label(i);
        if end == i {
            return MSG;
        }
        if end < b.len() && (b[end] == b'.' || b[end] == b'-') {
            if end + 1 == b.len() {
                return MSG; // separator with no trailing label
            }
            i = end + 1;
            continue;
        }
        // trailing bare label = the `([a-zA-Z0-9])+` tail; must not be the
        // first segment (the head group needs its own separator)
        return if end == b.len() && i > 0 { "" } else { MSG };
    }
}

/// moto's lookahead password pattern expanded: 8..64 chars with at least one
/// of the four class combinations.
fn validate_password(v: &Value) -> &'static str {
    const MSG: &str = "satisfy regular expression pattern: ^(?=^.{8,64}$)((?=.*\\d)(?=.*[A-Z])(?=.*[a-z])|(?=.*\\d)(?=.*[^A-Za-z0-9\\s])(?=.*[a-z])|(?=.*[^A-Za-z0-9\\s])(?=.*[A-Z])(?=.*[a-z])|(?=.*\\d)(?=.*[A-Z])(?=.*[^A-Za-z0-9\\s]))^.*$";
    let s = str_param(v).unwrap_or_default();
    let n = s.chars().count();
    if !(8..=64).contains(&n) {
        return MSG;
    }
    let has_digit = s.chars().any(|c| c.is_ascii_digit());
    let has_upper = s.chars().any(|c| c.is_ascii_uppercase());
    let has_lower = s.chars().any(|c| c.is_ascii_lowercase());
    let has_special = s
        .chars()
        .any(|c| !c.is_ascii_alphanumeric() && !c.is_whitespace());
    if (has_digit && has_upper && has_lower)
        || (has_digit && has_special && has_lower)
        || (has_special && has_upper && has_lower)
        || (has_digit && has_upper && has_special)
    {
        ""
    } else {
        MSG
    }
}

/// `^[^\/:*?"<>|.]+[^\/:*?"<>|]*$` with nonempty head
fn validate_short_name(v: &Value) -> &'static str {
    const MSG: &str = "satisfy regular expression pattern: ^[^\\/:*?\"<>|.]+[^\\/:*?\"<>|]*$";
    let s = str_param(v).unwrap_or_default();
    if s.is_empty() {
        return "";
    }
    let b: Vec<char> = s.chars().collect();
    let bad = |c: char| matches!(c, '/' | ':' | '*' | '?' | '"' | '<' | '>' | '|' | '.');
    if b.is_empty() || bad(b[0]) {
        return MSG;
    }
    if b.iter().skip(1).any(|c| bad(*c)) {
        return MSG;
    }
    ""
}

fn validate_size(v: &Value) -> &'static str {
    let s = str_param(v).unwrap_or_default();
    if s.eq_ignore_ascii_case("small") || s.eq_ignore_ascii_case("large") {
        ""
    } else {
        "satisfy enum value set: [Small, Large]"
    }
}

fn validate_sso_password(v: &Value) -> &'static str {
    let s = str_param(v).unwrap_or_default();
    if !s.is_empty() && s.chars().count() > 128 {
        "have length less than or equal to 128"
    } else {
        ""
    }
}

/// `^(subnet-[0-9a-f]{8}|subnet-[0-9a-f]{17})$`
fn validate_subnet_ids(v: &Value) -> &'static str {
    const MSG: &str =
        "satisfy regular expression pattern: ^(subnet-[0-9a-f]{8}|subnet-[0-9a-f]{17})$";
    let list = match v {
        Value::Array(l) => l,
        _ => return "",
    };
    for entry in list {
        let s = str_param(entry).unwrap_or_default();
        let rest = match s.strip_prefix("subnet-") {
            Some(r) => r,
            None => return MSG,
        };
        let len_ok = rest.len() == 8 || rest.len() == 17;
        let hex_ok = rest
            .chars()
            .all(|c| c.is_ascii_digit() || (b'a'..=b'f').contains(&(c as u8)));
        if !len_ok || !hex_ok {
            return MSG;
        }
    }
    ""
}

fn validate_user_name(v: &Value) -> &'static str {
    let s = str_param(v).unwrap_or_default();
    if !s.is_empty()
        && !s
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || c == '.' || c == '_' || c == '-')
    {
        "satisfy regular expression pattern: ^[a-zA-Z0-9._-]+$"
    } else {
        ""
    }
}

fn validate_trust_direction(v: &Value) -> &'static str {
    let s = str_param(v).unwrap_or_default();
    match s.as_str() {
        "One-Way: Outgoing" | "One-Way: Incoming" | "Two-Way" => "",
        _ => "satisfy enum value set: [One-Way: Outgoing, One-Way: Incoming, Two-Way]",
    }
}

/// `^([a-zA-Z0-9]+[\\.-])+([a-zA-Z0-9])+[.]?$` (labels joined by `.` / `-`,
/// optional trailing dot), then a ≤1024 length check.
fn validate_remote_domain_name(v: &Value) -> &'static str {
    const MSG: &str =
        "satisfy regular expression pattern: ^([a-zA-Z0-9]+[\\\\.-])+([a-zA-Z0-9])+[.]?$";
    let s = str_param(v).unwrap_or_default();
    if s.chars().count() > 1024 {
        return "have length less than or equal to 1024";
    }
    // ^G T[.]?  with G = "label" + sep (1+ times), T = final bare label;
    // sep ∈ { '.','-','\\' } per moto's `[\\\\.-]` char class.
    let core = s.strip_suffix('.').unwrap_or(&s);
    let b = core.as_bytes();
    let mut i = 0usize;
    let mut groups = 0usize;
    loop {
        let mut k = i;
        while k < b.len() && b[k].is_ascii_alphanumeric() {
            k += 1;
        }
        if k == i {
            return MSG;
        }
        if k == b.len() {
            // the tail label — valid only when a separated group preceded it
            return if groups > 0 { "" } else { MSG };
        }
        if b[k] != b'.' && b[k] != b'-' && b[k] != b'\\' {
            return MSG;
        }
        groups += 1;
        i = k + 1;
        if i == b.len() {
            return MSG; // separator without a trailing label
        }
    }
}

// ---------------------------------------------------------------------------
// moto TaggingService (key_name="Key", value_name="Value") + tag validation
// ---------------------------------------------------------------------------

/// moto validates tag_value against the key regex/limits verbatim (a quirk
/// of `TaggingService.validate_tags`) — kept as-is.
fn tag_validate(tags: &Value) -> Option<Response> {
    let list = match tags {
        Value::Array(l) => l.clone(),
        _ => return None,
    };
    for (idx, tag) in list.iter().enumerate() {
        if let Some(value) = tag.get("Value").and_then(Value::as_str) {
            if value.len() > 128 {
                return Some(validation_exception(&format!(
                    "Value '{}' at 'tags.{}.member.key' failed to satisfy constraint: Member must have length less than or equal to 128",
                    value,
                    idx + 1
                )));
            }
            if !tag_class_ok(value) || value.starts_with("aws:") {
                return Some(validation_exception(&format!(
                    "Value '{}' at 'tags.{}.member.key' failed to satisfy constraint: Member must satisfy regular expression pattern: ^(?!aws:)([\\w\\s\\d_.:/=+\\-@]*)$",
                    value,
                    idx + 1
                )));
            }
            if let Some(key) = tag.get("Key").and_then(Value::as_str) {
                if key.len() > 256 {
                    return Some(validation_exception(&format!(
                        "Value '{}' at 'tags.{}.member.value' failed to satisfy constraint: Member must have length less than or equal to 256",
                        key,
                        idx + 1
                    )));
                }
                if !tag_class_ok(key) {
                    return Some(validation_exception(&format!(
                        "Value '{}' at 'tags.{}.member.value' failed to satisfy constraint: Member must satisfy regular expression pattern: ^([\\w\\s\\d_.:/=+\\-@]*)$",
                        key,
                        idx + 1
                    )));
                }
            }
        }
    }
    None
}

/// `[\\w\\s\\d_.:/=+\\-@]`
fn tag_class_ok(s: &str) -> bool {
    s.chars().all(|c| {
        c.is_ascii_alphanumeric() || c == '_' || c.is_whitespace() || "_.:/=+-@".contains(c)
    })
}

fn tag_resource(st: &mut DsStore, id: &str, tags: &Value) {
    let list = match tags {
        Value::Array(l) => l,
        _ => return,
    };
    let entry = st.tags.entry(id.to_string()).or_default();
    for tag in list {
        let key = tag.get("Key").and_then(Value::as_str).unwrap_or_default();
        if key.is_empty() {
            continue;
        }
        let val = tag
            .get("Value")
            .and_then(Value::as_str)
            .map(|v| v.to_string());
        if let Some(pos) = entry.iter().position(|(k, _)| k == key) {
            entry[pos].1 = val; // overwrite keeps insertion position (dict)
        } else {
            entry.push((key.to_string(), val));
        }
    }
}

fn untag_resource(st: &mut DsStore, id: &str, keys: &Value) {
    let list = match keys {
        Value::Array(l) => l,
        _ => return,
    };
    if let Some(entry) = st.tags.get_mut(id) {
        for key in list {
            let k = str_param(key).unwrap_or_default();
            entry.retain(|(ek, _)| ek != &k);
        }
    }
}

fn tag_list(st: &DsStore, id: &str) -> Vec<Value> {
    st.tags
        .get(id)
        .map(|rows| {
            rows.iter()
                .map(|(k, v)| {
                    json!({
                        "Key": k,
                        "Value": v.clone().map(Value::String).unwrap_or(Value::Null),
                    })
                })
                .collect()
        })
        .unwrap_or_default()
}

// ---------------------------------------------------------------------------
// EC2 interop (moto's cross-backend calls out of ds)
// ---------------------------------------------------------------------------

/// Access the EC2 state partition for this (account, region) with the
/// mutate-in-place closure pattern used by the autoscaling/memorydb ports.
/// Seeds the default VPC on first touch — moto injects the default VPC at
/// EC2-backend construction, and an empty Ec2State would race ec2's own use
/// of the same scope.
fn ec2_with<T>(req: &Request, f: impl FnOnce(&mut ec2::state::Ec2State) -> T) -> T {
    let slot = ec2::state::shared();
    let mut map = slot.write();
    let st = map
        .entry((req.account, req.region.clone()))
        .or_insert_with(|| {
            let mut s = ec2::state::Ec2State::default();
            s.ensure_default_vpc(&req.region, req.account);
            s
        });
    f(st)
}

/// moto's `_verify_subnets`: exactly two subnets, both real, in different
/// AZs, under a real VPC; the zone list is stored back into the settings.
fn verify_subnets(
    req: &Request,
    settings: &mut Map<String, Value>,
) -> Result<Vec<String>, SubnetReject> {
    let ids: Vec<String> = settings
        .get("SubnetIds")
        .and_then(Value::as_array)
        .map(|l| l.iter().filter_map(str_param).collect())
        .unwrap_or_default();
    if ids.len() != 2 {
        return Err(SubnetReject::Invalid);
    }
    let vpc_id = settings
        .get("VpcId")
        .and_then(Value::as_str)
        .unwrap_or_default()
        .to_string();
    let found = ec2_with(req, |st| {
        let mut azs = Vec::with_capacity(2);
        for id in &ids {
            match st.subnets.iter().find(|s| &s.id == id) {
                Some(subnet) => azs.push(subnet.az.clone()),
                None => return Err(SubnetReject::Invalid),
            }
        }
        if azs[0] == azs[1] {
            return Err(SubnetReject::SameAz);
        }
        if !st.vpcs.iter().any(|v| v.id == vpc_id) {
            return Err(SubnetReject::BadVpc);
        }
        Ok(azs)
    });
    let azs = found?;
    settings.insert(
        "AvailabilityZones".into(),
        Value::Array(azs.iter().cloned().map(Value::String).collect()),
    );
    Ok(azs)
}

enum SubnetReject {
    Invalid,
    SameAz,
    BadVpc,
}

/// moto's `create_security_group` + `create_eni` for one directory:
/// returns (security_group_id, eni_ids, subnet_ips).
fn create_network(
    req: &Request,
    dir_id: &str,
    vpc_id: &str,
    subnet_ids: &[String],
) -> (String, Vec<String>, Vec<String>) {
    let sg_name = format!("{dir_id}_controllers");
    let descr = format!("AWS created network interface for {dir_id}");
    let owner = req.account.to_string();
    ec2_with(req, move |st| {
        let sg_id = st.next_id("sg");
        st.security_groups.push(ec2::state::SecurityGroup {
            id: sg_id.clone(),
            name: sg_name.clone(),
            description: format!("AWS created security group for {dir_id} directory controllers"),
            vpc_id: vpc_id.to_string(),
            owner_id: owner.clone(),
            ..ec2::state::SecurityGroup::default()
        });
        let mut eni_ids = Vec::new();
        let mut subnet_ips = Vec::new();
        for (i, sid) in subnet_ids.iter().enumerate() {
            let subnet = st
                .subnets
                .iter()
                .find(|s| &s.id == sid)
                .map(|s| (s.id.clone(), s.vpc_id.clone(), s.cidr.clone()));
            let Some((sn_id, sn_vpc, sn_cidr)) = subnet else {
                continue;
            };
            // primary address: a host inside the subnet's cidr block (moto
            // picks from the subnet cidr); fall back to the 10-family picker
            let octets: Vec<u32> = sn_cidr
                .split('/')
                .next()
                .unwrap_or("127.0.0.1")
                .split('.')
                .map(|x| x.parse::<u32>().unwrap_or(0))
                .collect();
            let ip = if octets.len() == 4 {
                format!("{}.{}.{}.{}", octets[0], octets[1], octets[2], i + 1)
            } else {
                let n = uuid::Uuid::new_v4().as_u128();
                format!(
                    "10.{}.{}.{}",
                    (n >> 24) % 250,
                    (n >> 16) % 250,
                    (n >> 8) % 250
                )
            };
            let id = st.next_id("eni");
            st.enis.push(ec2::state::Eni {
                id: id.clone(),
                subnet_id: sn_id,
                vpc_id: sn_vpc,
                description: descr.clone(),
                owner_id: owner.clone(),
                status: "available".into(),
                private_ip: ip.clone(),
                groups: vec![(sg_id.clone(), sg_name.clone())],
                interface_type: "interface".into(),
                source_dest_check: true,
                ..ec2::state::Eni::default()
            });
            eni_ids.push(id);
            subnet_ips.push(ip);
        }
        (sg_id, eni_ids, subnet_ips)
    })
}

/// moto's `delete_eni` + `delete_security_group` on directory deletion.
fn delete_network(req: &Request, dir: &mut Directory) {
    let (enis, sg) = (dir.eni_ids.clone(), dir.sg_id.clone());
    ec2_with(req, move |st| {
        st.enis.retain(|e| !enis.contains(&e.id));
        st.security_groups.retain(|g| g.id != sg);
    });
    dir.eni_ids.clear();
}

// ---------------------------------------------------------------------------
// Records → wire dicts (moto's to_dict shapes)
// ---------------------------------------------------------------------------

impl Directory {
    fn to_dict(&self) -> Value {
        let mut a = Map::new();
        a.insert("AccessUrl".into(), Value::String(self.access_url.clone()));
        a.insert("Alias".into(), Value::String(self.alias.clone()));
        a.insert(
            "DirectoryId".into(),
            Value::String(self.directory_id.clone()),
        );
        a.insert(
            "DesiredNumberOfDomainControllers".into(),
            Value::from(self.desired_number_of_domain_controllers),
        );
        a.insert(
            "DnsIpAddrs".into(),
            Value::Array(
                self.dns_ip_addrs
                    .iter()
                    .cloned()
                    .map(Value::String)
                    .collect(),
            ),
        );
        a.insert("LaunchTime".into(), Value::from(self.launch_time));
        a.insert("Name".into(), Value::String(self.name.clone()));
        a.insert("SsoEnabled".into(), Value::Bool(self.sso_enabled));
        a.insert("Stage".into(), Value::String(self.stage.clone()));
        a.insert(
            "StageLastUpdatedDateTime".into(),
            Value::from(self.stage_last_updated),
        );
        a.insert("Type".into(), Value::String(self.directory_type.clone()));
        if let Some(e) = &self.edition {
            a.insert("Edition".into(), Value::String(e.clone()));
        }
        if let Some(s) = &self.size {
            a.insert("Size".into(), Value::String(s.clone()));
        }
        if let Some(sn) = &self.short_name {
            a.insert("ShortName".into(), Value::String(sn.clone()));
        }
        if let Some(d) = &self.description {
            a.insert("Description".into(), Value::String(d.clone()));
        }
        if let Some(vs) = &self.vpc_settings {
            a.insert("VpcSettings".into(), Value::Object(vs.clone()));
        } else if let Some(cs) = &self.connect_settings {
            // moto zeroes CustomerDnsIps into the response (and the stored
            // dict — its `attributes["ConnectSettings"]["CustomerDnsIps"]=None`)
            let mut cs = cs.clone();
            cs.insert("CustomerDnsIps".into(), Value::Null);
            a.insert("ConnectSettings".into(), Value::Object(cs));
        }
        Value::Object(a)
    }

    fn as_view(&self) -> DirView<'_> {
        DirView {
            launch: self.launch_time,
            seq: self.seq,
            dir: self,
        }
    }
}

/// (launch_time, seq) sort key matching moto's stable `sorted(...launch_time)`.
#[derive(Clone)]
struct DirView<'a> {
    launch: f64,
    seq: u64,
    dir: &'a Directory,
}

impl Trust {
    fn to_dict(&self) -> Value {
        json!({
            "CreatedDateTime": self.created,
            "DirectoryId": self.directory_id,
            "LastUpdatedDateTime": self.last_updated,
            "RemoteDomainName": self.remote_domain_name,
            "SelectiveAuth": self.selective_auth.clone().map(Value::String).unwrap_or(Value::Null),
            "StateLastUpdatedDateTime": self.state_last_updated,
            "TrustDirection": self.trust_direction,
            "TrustId": self.trust_id,
            "TrustState": self.trust_state,
            "TrustStateReason": self.trust_state_reason,
            "TrustType": self.trust_type.clone().map(Value::String).unwrap_or(Value::Null),
        })
    }
}

impl Snapshot {
    fn to_dict(&self) -> Value {
        json!({
            "SnapshotId": self.snapshot_id,
            "DirectoryId": self.directory_id,
            "Name": self.name,
            "Status": "Completed",
            "Type": "Manual",
            "StartTime": self.start,
        })
    }
}

impl Cert {
    fn to_dict(&self) -> Value {
        json!({
            "CertificateId": self.certificate_id,
            "State": "Registered",
            "CommonName": "MockCertificate",
            "RegisteredDateTime": self.registered,
            "ExpiryDateTime": self.expiry,
            "Type": self.cert_type,
        })
    }

    fn to_full_dict(&self) -> Value {
        let mut v = self.to_dict();
        let obj = v.as_object_mut().unwrap();
        obj.insert("StateReason".into(), Value::String("".into()));
        if let Some(ccas) = &self.client_cert_auth_settings {
            obj.insert("ClientCertAuthSettings".into(), ccas.clone());
        }
        v
    }
}

impl SchemaExt {
    fn to_dict(&self) -> Value {
        json!({
            "DirectoryId": self.directory_id,
            "SchemaExtensionId": self.schema_extension_id,
            "Description": self.description,
            "SchemaExtensionStatus": self.status,
            "SchemaExtensionStatusReason": self.status_reason,
            "StartDateTime": self.start,
            "EndDateTime": self.end,
        })
    }
}

// ---------------------------------------------------------------------------
// Dispatch
// ---------------------------------------------------------------------------

pub fn dispatch(req: &Request) -> Option<Response> {
    if req.service != "ds" {
        return None;
    }
    Some(with_store(req, |st| route(req, st)))
}

fn route(req: &Request, st: &mut DsStore) -> Response {
    let body = parse_json_body(&req.body);
    match req.operation.as_str() {
        // --- directory lifecycle ------------------------------------------
        "ConnectDirectory" => connect_directory(req, st, &body),
        "CreateDirectory" => create_directory(req, st, &body),
        "CreateMicrosoftAD" => create_microsoft_ad(req, st, &body),
        "DeleteDirectory" => delete_directory(req, st, &body),
        "DescribeDirectories" => describe_directories(st, &body),
        "GetDirectoryLimits" => get_directory_limits(st),
        // --- alias / sso ----------------------------------------------------
        "CreateAlias" => create_alias(st, &body),
        "DisableSso" => disable_sso(st, &body),
        "EnableSso" => enable_sso(st, &body),
        // --- tags -----------------------------------------------------------
        "AddTagsToResource" => add_tags_to_resource(st, &body),
        "RemoveTagsFromResource" => remove_tags_from_resource(st, &body),
        "ListTagsForResource" => list_tags_for_resource(st, &body),
        // --- trusts -----------------------------------------------------------
        "CreateTrust" => create_trust(st, &body),
        "DescribeTrusts" => describe_trusts(st, &body),
        "DeleteTrust" => delete_trust(st, &body),
        "VerifyTrust" => verify_trust(st, &body),
        // --- ldaps / settings ---------------------------------------------------
        "DescribeLDAPSSettings" => describe_ldaps_settings(st, &body),
        "EnableLDAPS" => set_ldaps(st, &body, true),
        "DisableLDAPS" => set_ldaps(st, &body, false),
        "DescribeSettings" => describe_settings(st, &body),
        "UpdateSettings" => update_settings(st, &body),
        // --- AD assessment (moto: describe always not-found) ---------------------
        "DescribeADAssessment" => describe_ad_assessment(&body),
        "StartADAssessment" => start_ad_assessment(st, &body),
        "ListADAssessments" => ok(json!({
            "Assessments": [],
            "NextToken": Value::Null,
        })),
        "DeleteADAssessment" => {
            let id = str_param(p(&body, "AssessmentId")).unwrap_or_default();
            entity_does_not_exist(&format!("Assessment {id} does not exist"))
        }
        // --- CA enrollment + data access -------------------------------------------
        "DescribeCAEnrollmentPolicy" => describe_ca_enrollment_policy(st, &body),
        "EnableCAEnrollmentPolicy" | "DisableCAEnrollmentPolicy" => {
            let dir_id = str_param(p(&body, "DirectoryId")).unwrap_or_default();
            if let Some(r) = validate_dir_exists(st, &dir_id) {
                return r;
            }
            ok(json!({}))
        }
        "EnableDirectoryDataAccess" | "DisableDirectoryDataAccess" => {
            let dir_id = str_param(p(&body, "DirectoryId")).unwrap_or_default();
            if let Some(r) = validate_dir_exists(st, &dir_id) {
                return r;
            }
            let status = if req.operation == "EnableDirectoryDataAccess" {
                "Enabled"
            } else {
                "Disabled"
            };
            if let Some(d) = st.dirs.get_mut(&dir_id) {
                d.data_access_status = status.to_string();
            }
            ok(json!({}))
        }
        "DescribeDirectoryDataAccess" => {
            let dir_id = str_param(p(&body, "DirectoryId")).unwrap_or_default();
            if let Some(r) = validate_dir_exists(st, &dir_id) {
                return r;
            }
            let status = st
                .dirs
                .get(&dir_id)
                .map(|d| d.data_access_status.clone())
                .unwrap_or_else(|| "Disabled".to_string());
            ok(json!({"DataAccessStatus": status}))
        }
        // --- conditional forwarders ---------------------------------------------------
        "CreateConditionalForwarder" => create_conditional_forwarder(st, &body),
        "DeleteConditionalForwarder" => delete_conditional_forwarder(st, &body),
        "UpdateConditionalForwarder" => update_conditional_forwarder(st, &body),
        "DescribeConditionalForwarders" => describe_conditional_forwarders(st, &body),
        // --- domain controllers ----------------------------------------------------------
        "DescribeDomainControllers" => describe_domain_controllers(st, &body),
        "UpdateNumberOfDomainControllers" => update_number_of_domain_controllers(st, &body),
        // --- event topics -----------------------------------------------------------------------
        "RegisterEventTopic" => register_event_topic(st, &body),
        "DeregisterEventTopic" => deregister_event_topic(st, &body),
        "DescribeEventTopics" => describe_event_topics(st, &body),
        // --- snapshots ----------------------------------------------------------------------
        "CreateSnapshot" => create_snapshot(st, &body),
        "DeleteSnapshot" => delete_snapshot(st, &body),
        "RestoreFromSnapshot" => restore_from_snapshot(st, &body),
        "DescribeSnapshots" => describe_snapshots(st, &body),
        "GetSnapshotLimits" => get_snapshot_limits(st, &body),
        // --- multi-region / shared ---------------------------------------------------------
        "DescribeSharedDirectories" => describe_shared_directories(st, &body),
        "DescribeRegions" => describe_regions(req, st, &body),
        "AddRegion" | "RemoveRegion" | "UpdateDirectorySetup" => {
            let dir_id = str_param(p(&body, "DirectoryId")).unwrap_or_default();
            if let Some(r) = validate_dir_exists(st, &dir_id) {
                return r;
            }
            ok(json!({}))
        }
        // --- radius / client auth / computers ----------------------------------------------
        "EnableRadius" => enable_radius(st, &body),
        "DisableRadius" => disable_radius(st, &body),
        "UpdateRadius" => update_radius(st, &body),
        "EnableClientAuthentication" => set_client_auth(st, &body, true),
        "DisableClientAuthentication" => set_client_auth(st, &body, false),
        "DescribeClientAuthenticationSettings" => describe_client_auth(st, &body),
        "CreateComputer" => create_computer(st, &body),
        "ResetUserPassword" => {
            let dir_id = str_param(p(&body, "DirectoryId")).unwrap_or_default();
            if let Some(r) = validate_dir_exists(st, &dir_id) {
                return r;
            }
            ok(json!({}))
        }
        // --- certificates --------------------------------------------------------------------
        "RegisterCertificate" => register_certificate(st, &body),
        "DeregisterCertificate" => deregister_certificate(st, &body),
        "ListCertificates" => list_certificates(st, &body),
        "DescribeCertificate" => describe_certificate(st, &body),
        // --- ip routes -------------------------------------------------------------------------
        "AddIpRoutes" => add_ip_routes(st, &body),
        "RemoveIpRoutes" => remove_ip_routes(st, &body),
        "ListIpRoutes" => list_ip_routes(st, &body),
        // --- schema extensions ---------------------------------------------------------------------
        "StartSchemaExtension" => start_schema_extension(st, &body),
        "CancelSchemaExtension" => cancel_schema_extension(st, &body),
        "ListSchemaExtensions" => list_schema_extensions(st, &body),
        // --- log subscriptions -----------------------------------------------------------------------
        "CreateLogSubscription" => create_log_subscription(st, &body),
        "ListLogSubscriptions" => list_log_subscriptions(st, &body),
        "DeleteLogSubscription" => delete_log_subscription(st, &body),
        // --- share directory ---------------------------------------------------------------------------
        "ShareDirectory" => share_directory(req, st, &body),
        "UnshareDirectory" => unshare_directory(st, &body),
        "AcceptSharedDirectory" => accept_shared_directory(st, &body),
        "RejectSharedDirectory" => reject_shared_directory(st, &body),
        // --- describe update activities -------------------------------------------------------------------
        "DescribeUpdateDirectory" => {
            let dir_id = str_param(p(&body, "DirectoryId")).unwrap_or_default();
            if let Some(r) = validate_dir_exists(st, &dir_id) {
                return r;
            }
            ok(json!({"UpdateActivities": [], "NextToken": Value::Null}))
        }
        // --- double-absent: no route/handler in the deployed moto fork --------
        "CreateHybridAD" | "DescribeHybridADUpdate" | "UpdateHybridAD" | "UpdateTrust" => {
            not_implemented(req.operation.as_str())
        }
        _ => not_implemented(req.operation.as_str()),
    }
}

// ---------------------------------------------------------------------------
// Directory lifecycle
// ---------------------------------------------------------------------------

const CLOUDONLY_DIRECTORIES_LIMIT: i64 = 10;
const CLOUDONLY_MICROSOFT_AD_LIMIT: i64 = 20;
const CONNECTED_DIRECTORIES_LIMIT: i64 = 10;
const MAX_TAGS_PER_DIRECTORY: usize = 50;

fn dir_limit(code: &str, limit: i64) -> Response {
    err(
        400,
        code,
        &format!("Directory limit exceeded. A maximum of {limit} directories may be created"),
    )
}

fn tag_limit_dir() -> Response {
    err(
        400,
        "DirectoryLimitExceededException",
        "Tag Limit is exceeding",
    )
}

/// tag checks (validate + per-directory limit) shared by create paths.
fn tag_guard(tags: &Value) -> Option<Response> {
    if let Some(r) = tag_validate(tags) {
        return Some(r);
    }
    if let Value::Array(l) = tags {
        if l.len() > MAX_TAGS_PER_DIRECTORY {
            return Some(tag_limit_dir());
        }
    }
    None
}

/// The subnet/VPC part of create — validates then stores AvailabilityZones.
fn prep_settings(req: &Request, settings: &Value) -> Result<Map<String, Value>, Response> {
    let mut map = match settings {
        Value::Object(m) => m.clone(),
        _ => return Err(invalid_parameter("VpcSettings must be specified.")),
    };
    match verify_subnets(req, &mut map) {
        Ok(_) => Ok(map),
        Err(SubnetReject::Invalid) => Err(invalid_parameter(
            "Invalid subnet ID(s). They must correspond to two subnets in different Availability Zones.",
        )),
        Err(SubnetReject::SameAz) => Err(client_exception(
            "Invalid subnet ID(s). The two subnets must be in different Availability Zones.",
        )),
        Err(SubnetReject::BadVpc) => Err(client_exception("Invalid VPC ID.")),
    }
}

fn connect_directory(req: &Request, st: &mut DsStore, body: &Value) -> Response {
    if st.dirs.len() as i64 > CONNECTED_DIRECTORIES_LIMIT {
        return dir_limit(
            "DirectoryLimitExceededException",
            CONNECTED_DIRECTORIES_LIMIT,
        );
    }
    let connect = p(body, "ConnectSettings");
    let tags = p(body, "Tags").clone();
    let pairs = vec![
        ("password", p(body, "Password").clone()),
        ("size", p(body, "Size").clone()),
        ("name", p(body, "Name").clone()),
        ("description", p(body, "Description").clone()),
        ("shortName", p(body, "ShortName").clone()),
        (
            "connectSettings.vpcSettings.subnetIds",
            p(connect, "SubnetIds").clone(),
        ),
        (
            "connectSettings.customerUserName",
            p(connect, "CustomerUserName").clone(),
        ),
        (
            "connectSettings.customerDnsIps",
            p(connect, "CustomerDnsIps").clone(),
        ),
    ];
    if let Some(r) = validate_args(&pairs) {
        return r;
    }
    let mut settings = match prep_settings(req, connect) {
        Ok(m) => m,
        Err(r) => return r,
    };
    if let Some(r) = tag_guard(&tags) {
        return r;
    }
    let dir_id = new_id("d", 10);
    let vpc_id = str_param(p(connect, "VpcId")).unwrap_or_default();
    let subnet_ids = array_strs(p(connect, "SubnetIds"));
    let (sg_id, eni_ids, subnet_ips) = create_network(req, &dir_id, &vpc_id, &subnet_ids);
    settings.insert("SecurityGroupId".into(), Value::String(sg_id.clone()));
    settings.insert(
        "ConnectIps".into(),
        Value::Array(subnet_ips.iter().cloned().map(Value::String).collect()),
    );
    // moto: an ADConnector's DNS IPs are the passed CustomerDnsIps
    let dns = array_strs(p(connect, "CustomerDnsIps"));
    build_dir(
        req,
        st,
        &dir_id,
        "ADConnector",
        &name_of(body),
        size_of(body),
        short_of(body),
        None,
        Some(settings),
        None,
        desc_of(body),
        &dns,
        sg_id,
        eni_ids,
        &tags,
        false,
    );
    ok(json!({"DirectoryId": dir_id}))
}

fn create_directory(req: &Request, st: &mut DsStore, body: &Value) -> Response {
    if st.dirs.len() as i64 > CLOUDONLY_DIRECTORIES_LIMIT {
        return dir_limit(
            "DirectoryLimitExceededException",
            CLOUDONLY_DIRECTORIES_LIMIT,
        );
    }
    let vpc = p(body, "VpcSettings");
    if vpc.as_object().map(|m| m.is_empty()).unwrap_or(true) {
        return invalid_parameter("VpcSettings must be specified.");
    }
    let tags = p(body, "Tags").clone();
    let pairs = vec![
        ("password", p(body, "Password").clone()),
        ("size", p(body, "Size").clone()),
        ("name", p(body, "Name").clone()),
        ("description", p(body, "Description").clone()),
        ("shortName", p(body, "ShortName").clone()),
        ("vpcSettings.subnetIds", p(vpc, "SubnetIds").clone()),
    ];
    if let Some(r) = validate_args(&pairs) {
        return r;
    }
    let mut settings = match prep_settings(req, vpc) {
        Ok(m) => m,
        Err(r) => return r,
    };
    if let Some(r) = tag_guard(&tags) {
        return r;
    }
    let dir_id = new_id("d", 10);
    let vpc_id = str_param(p(vpc, "VpcId")).unwrap_or_default();
    let subnet_ids = array_strs(p(vpc, "SubnetIds"));
    let (sg_id, eni_ids, subnet_ips) = create_network(req, &dir_id, &vpc_id, &subnet_ids);
    settings.insert("SecurityGroupId".into(), Value::String(sg_id.clone()));
    build_dir(
        req,
        st,
        &dir_id,
        "SimpleAD",
        &name_of(body),
        size_of(body),
        short_of(body),
        Some(settings),
        None,
        None,
        desc_of(body),
        &subnet_ips,
        sg_id,
        eni_ids,
        &tags,
        false,
    );
    ok(json!({"DirectoryId": dir_id}))
}

fn create_microsoft_ad(req: &Request, st: &mut DsStore, body: &Value) -> Response {
    if st.dirs.len() as i64 > CLOUDONLY_MICROSOFT_AD_LIMIT {
        return dir_limit(
            "DirectoryLimitExceededException",
            CLOUDONLY_MICROSOFT_AD_LIMIT,
        );
    }
    let vpc = p(body, "VpcSettings");
    if vpc.as_object().map(|m| m.is_empty()).unwrap_or(true) {
        return invalid_parameter("VpcSettings must be specified.");
    }
    let tags = p(body, "Tags").clone();
    let pairs = vec![
        ("password", p(body, "Password").clone()),
        ("edition", p(body, "Edition").clone()),
        ("name", p(body, "Name").clone()),
        ("description", p(body, "Description").clone()),
        ("shortName", p(body, "ShortName").clone()),
        ("vpcSettings.subnetIds", p(vpc, "SubnetIds").clone()),
    ];
    if let Some(r) = validate_args(&pairs) {
        return r;
    }
    let mut settings = match prep_settings(req, vpc) {
        Ok(m) => m,
        Err(r) => return r,
    };
    if let Some(r) = tag_guard(&tags) {
        return r;
    }
    let dir_id = new_id("d", 10);
    let vpc_id = str_param(p(vpc, "VpcId")).unwrap_or_default();
    let subnet_ids = array_strs(p(vpc, "SubnetIds"));
    let (sg_id, eni_ids, subnet_ips) = create_network(req, &dir_id, &vpc_id, &subnet_ids);
    settings.insert("SecurityGroupId".into(), Value::String(sg_id.clone()));
    build_dir(
        req,
        st,
        &dir_id,
        "MicrosoftAD",
        &name_of(body),
        None,
        short_of(body),
        Some(settings),
        None,
        edition_of(body),
        desc_of(body),
        &subnet_ips,
        sg_id,
        eni_ids,
        &tags,
        true,
    );
    ok(json!({"DirectoryId": dir_id}))
}

#[allow(clippy::too_many_arguments)]
fn build_dir(
    req: &Request,
    st: &mut DsStore,
    dir_id: &str,
    directory_type: &str,
    name: &str,
    size: Option<String>,
    short_name: Option<String>,
    vpc_settings: Option<Map<String, Value>>,
    connect_settings: Option<Map<String, Value>>,
    edition: Option<String>,
    description: Option<String>,
    dns_ip_addrs: &[String],
    sg_id: String,
    eni_ids: Vec<String>,
    tags: &Value,
    microsoft: bool,
) {
    st.seq += 1;
    let d = Directory {
        region: req.region.clone(),
        name: name.to_string(),
        directory_type: directory_type.to_string(),
        size,
        short_name: short_name.filter(|s| !s.is_empty()),
        description: description.filter(|s| !s.is_empty()),
        edition,
        vpc_settings,
        connect_settings,
        directory_id: dir_id.to_string(),
        alias: dir_id.to_string(),
        access_url: format!("{dir_id}.awsapps.com"),
        desired_number_of_domain_controllers: 0,
        sso_enabled: false,
        stage: "Active".to_string(),
        launch_time: now_unix(),
        stage_last_updated: now_unix(),
        ldaps: Vec::new(),
        trusts: Vec::new(),
        msad_settings: microsoft.then(msad_settings_entries),
        dns_ip_addrs: dns_ip_addrs.to_vec(),
        sg_id,
        eni_ids,
        data_access_status: "Disabled".to_string(),
        seq: st.seq,
    };
    st.dirs.insert(dir_id.to_string(), d);
    tag_resource(st, dir_id, tags);
}

fn name_of(body: &Value) -> String {
    str_param(p(body, "Name")).unwrap_or_default()
}
fn size_of(body: &Value) -> Option<String> {
    str_param(p(body, "Size"))
}
fn short_of(body: &Value) -> Option<String> {
    str_param(p(body, "ShortName"))
}
fn desc_of(body: &Value) -> Option<String> {
    str_param(p(body, "Description"))
}
fn edition_of(body: &Value) -> Option<String> {
    str_param(p(body, "Edition"))
}

fn delete_directory(req: &Request, st: &mut DsStore, body: &Value) -> Response {
    let dir_id = str_param(p(body, "DirectoryId")).unwrap_or_default();
    if let Some(r) = validate_dir_exists(st, &dir_id) {
        return r;
    }
    if let Some(mut d) = st.dirs.remove(&dir_id) {
        delete_network(req, &mut d);
    }
    st.tags.remove(&dir_id);
    // shares of a deleted owner directory are unshareable
    st.shares.retain(|s| s.owner_directory_id != dir_id);
    ok(json!({"DirectoryId": dir_id}))
}

fn describe_directories(st: &mut DsStore, body: &Value) -> Response {
    let ids: Vec<String> = match p(body, "DirectoryIds") {
        Value::Array(l) => l.iter().filter_map(str_param).collect(),
        _ => Vec::new(),
    };
    // moto validates every requested id (regex then existence) before listing
    for id in &ids {
        if let Some(r) = validate_dir_exists(st, id) {
            return r;
        }
    }
    let mut dirs: Vec<DirView> = st.dirs.values().map(Directory::as_view).collect();
    dirs.sort_by(|a, b| {
        a.launch
            .partial_cmp(&b.launch)
            .unwrap_or(std::cmp::Ordering::Equal)
            .then_with(|| a.seq.cmp(&b.seq))
    });
    let filtered: Vec<DirView> = dirs
        .into_iter()
        .filter(|v| ids.is_empty() || ids.contains(&v.dir.directory_id))
        .collect();
    let context = params_context(body, &["NextToken", "Limit"]);
    let page = paginate::<DirView>(
        filtered,
        int_param(p(body, "Limit")),
        100,
        &context,
        p(body, "NextToken"),
        |v| v.dir.directory_id.clone(),
    );
    let (page, token) = match page {
        Ok(v) => v,
        Err(r) => return r,
    };
    let entries: Vec<Value> = page.iter().map(|v| v.dir.to_dict()).collect();
    ok(json!({
        "DirectoryDescriptions": entries,
        "NextToken": token.map(Value::String).unwrap_or(Value::Null),
    }))
}

fn get_directory_limits(st: &DsStore) -> Response {
    let mut simple = 0i64;
    let mut msad = 0i64;
    let mut connected = 0i64;
    for d in st.dirs.values() {
        match d.directory_type.as_str() {
            "SimpleAD" => simple += 1,
            "MicrosoftAD" | "SharedMicrosoftAD" => msad += 1,
            "ADConnector" => connected += 1,
            _ => {}
        }
    }
    ok(json!({
        "DirectoryLimits": {
            "CloudOnlyDirectoriesLimit": CLOUDONLY_DIRECTORIES_LIMIT,
            "CloudOnlyDirectoriesCurrentCount": simple,
            "CloudOnlyDirectoriesLimitReached": simple == CLOUDONLY_DIRECTORIES_LIMIT,
            "CloudOnlyMicrosoftADLimit": CLOUDONLY_MICROSOFT_AD_LIMIT,
            "CloudOnlyMicrosoftADCurrentCount": msad,
            "CloudOnlyMicrosoftADLimitReached": msad == CLOUDONLY_MICROSOFT_AD_LIMIT,
            "ConnectedDirectoriesLimit": CONNECTED_DIRECTORIES_LIMIT,
            "ConnectedDirectoriesCurrentCount": connected,
            "ConnectedDirectoriesLimitReached": connected == CONNECTED_DIRECTORIES_LIMIT,
        }
    }))
}

// ---------------------------------------------------------------------------
// Alias / SSO
// ---------------------------------------------------------------------------

fn create_alias(st: &mut DsStore, body: &Value) -> Response {
    let dir_id = str_param(p(body, "DirectoryId")).unwrap_or_default();
    if let Some(r) = validate_dir_exists(st, &dir_id) {
        return r;
    }
    let alias = str_param(p(body, "Alias")).unwrap_or_default();
    if st
        .dirs
        .get(&dir_id)
        .map(|d| d.alias != dir_id)
        .unwrap_or(false)
    {
        return invalid_parameter(
            "The directory in the request already has an alias. That alias must be deleted before a new alias can be created.",
        );
    }
    if st.dirs.values().any(|d| d.alias == alias) {
        return entity_already_exists(&format!("Alias '{alias}' already exists."));
    }
    if let Some(r) = validate_args(&[("alias", Value::String(alias.clone()))]) {
        return r;
    }
    if let Some(d) = st.dirs.get_mut(&dir_id) {
        d.alias = alias.clone();
        d.access_url = format!("{alias}.awsapps.com");
    }
    ok(json!({"DirectoryId": dir_id, "Alias": alias}))
}

fn disable_sso(st: &mut DsStore, body: &Value) -> Response {
    let dir_id = str_param(p(body, "DirectoryId")).unwrap_or_default();
    if let Some(r) = validate_dir_exists(st, &dir_id) {
        return r;
    }
    let pairs = vec![
        ("ssoPassword", p(body, "Password").clone()),
        ("userName", p(body, "UserName").clone()),
    ];
    if let Some(r) = validate_args(&pairs) {
        return r;
    }
    if let Some(d) = st.dirs.get_mut(&dir_id) {
        d.sso_enabled = false;
    }
    ok_empty()
}

fn enable_sso(st: &mut DsStore, body: &Value) -> Response {
    let dir_id = str_param(p(body, "DirectoryId")).unwrap_or_default();
    if let Some(r) = validate_dir_exists(st, &dir_id) {
        return r;
    }
    let pairs = vec![
        ("ssoPassword", p(body, "Password").clone()),
        ("userName", p(body, "UserName").clone()),
    ];
    if let Some(r) = validate_args(&pairs) {
        return r;
    }
    let aliased = st
        .dirs
        .get(&dir_id)
        .map(|d| d.alias != dir_id)
        .unwrap_or(false);
    if !aliased {
        return client_exception(&format!(
            "An alias is required before enabling SSO. DomainId={dir_id}"
        ));
    }
    if let Some(d) = st.dirs.get_mut(&dir_id) {
        d.sso_enabled = true;
    }
    ok_empty()
}

// ---------------------------------------------------------------------------
// Tags
// ---------------------------------------------------------------------------

fn add_tags_to_resource(st: &mut DsStore, body: &Value) -> Response {
    let rid = str_param(p(body, "ResourceId")).unwrap_or_default();
    if let Some(r) = validate_dir_exists(st, &rid) {
        return r;
    }
    let tags = p(body, "Tags").clone();
    if let Some(r) = tag_validate(&tags) {
        return r;
    }
    if let Value::Array(l) = &tags {
        if l.len() > MAX_TAGS_PER_DIRECTORY {
            return err(400, "TagLimitExceededException", "Tag limit exceeded");
        }
    }
    tag_resource(st, &rid, &tags);
    ok(json!({}))
}

fn remove_tags_from_resource(st: &mut DsStore, body: &Value) -> Response {
    let rid = str_param(p(body, "ResourceId")).unwrap_or_default();
    if let Some(r) = validate_dir_exists(st, &rid) {
        return r;
    }
    untag_resource(st, &rid, p(body, "TagKeys"));
    ok(json!({}))
}

fn list_tags_for_resource(st: &mut DsStore, body: &Value) -> Response {
    let rid = str_param(p(body, "ResourceId")).unwrap_or_default();
    if let Some(r) = validate_dir_exists(st, &rid) {
        return r;
    }
    let tags = tag_list(st, &rid);
    let context = params_context(body, &["NextToken", "Limit"]);
    let page = paginate::<Value>(
        tags,
        int_param(p(body, "Limit")),
        50,
        &context,
        p(body, "NextToken"),
        |t| {
            t.get("Key")
                .and_then(Value::as_str)
                .unwrap_or("")
                .to_string()
        },
    );
    let (page, token) = match page {
        Ok(v) => v,
        Err(r) => return r,
    };
    let elems: Vec<Value> = page.to_vec();
    ok(json!({
        "Tags": elems,
        "NextToken": token.map(Value::String).unwrap_or(Value::Null),
    }))
}

// ---------------------------------------------------------------------------
// Trusts
// ---------------------------------------------------------------------------

fn create_trust(st: &mut DsStore, body: &Value) -> Response {
    let dir_id = str_param(p(body, "DirectoryId")).unwrap_or_default();
    if let Some(r) = validate_dir_exists(st, &dir_id) {
        return r;
    }
    let pairs = vec![
        ("ssoPassword", p(body, "TrustPassword").clone()),
        ("trustDirection", p(body, "TrustDirection").clone()),
        ("remoteDomainName", p(body, "RemoteDomainName").clone()),
    ];
    if let Some(r) = validate_args(&pairs) {
        return r;
    }
    st.seq += 1;
    let now = now_unix();
    let trust = Trust {
        trust_id: new_id("t", 10),
        directory_id: dir_id.clone(),
        remote_domain_name: str_param(p(body, "RemoteDomainName")).unwrap_or_default(),
        trust_direction: str_param(p(body, "TrustDirection")).unwrap_or_default(),
        trust_type: str_param(p(body, "TrustType")),
        selective_auth: str_param(p(body, "SelectiveAuth")),
        trust_state: "Creating".to_string(),
        trust_state_reason: String::new(),
        created: now,
        last_updated: now,
        state_last_updated: now,
    };
    let id = trust.trust_id.clone();
    st.dirs.get_mut(&dir_id).unwrap().trusts.push(trust);
    ok(json!({"TrustId": id}))
}

fn describe_trusts(st: &mut DsStore, body: &Value) -> Response {
    let dir_id = str_param(p(body, "DirectoryId"));
    let trust_ids: Vec<String> = match p(body, "TrustIds") {
        Value::Array(l) => l.iter().filter_map(str_param).collect(),
        _ => Vec::new(),
    };
    if let Some(d) = &dir_id {
        if let Some(r) = validate_dir_exists(st, d) {
            return r;
        }
    }
    let trusts: Vec<Trust> = st
        .dirs
        .values()
        .flat_map(|d| d.trusts.iter())
        .filter(|t| {
            dir_id
                .as_deref()
                .map(|x| t.directory_id == x)
                .unwrap_or(true)
        })
        .filter(|t| trust_ids.is_empty() || trust_ids.contains(&t.trust_id))
        .cloned()
        .collect();
    let context = params_context(body, &["NextToken", "Limit"]);
    let page = paginate::<Trust>(
        trusts,
        int_param(p(body, "Limit")),
        100,
        &context,
        p(body, "NextToken"),
        |t| t.trust_id.clone(),
    );
    let (page, token) = match page {
        Ok(v) => v,
        Err(r) => return r,
    };
    let entries: Vec<Value> = page.iter().map(|t| t.to_dict()).collect();
    // moto's quirk: this response uses lowercase "nextToken"
    ok(json!({
        "Trusts": entries,
        "nextToken": token.map(Value::String).unwrap_or(Value::Null),
    }))
}

fn delete_trust(st: &mut DsStore, body: &Value) -> Response {
    let trust_id = str_param(p(body, "TrustId")).unwrap_or_default();
    for dir in st.dirs.values_mut() {
        if let Some(pos) = dir.trusts.iter().position(|t| t.trust_id == trust_id) {
            dir.trusts.remove(pos);
            return ok(json!({"TrustId": trust_id}));
        }
    }
    entity_does_not_exist(&format!("Trust {trust_id} does not exist"))
}

fn verify_trust(st: &mut DsStore, body: &Value) -> Response {
    let trust_id = str_param(p(body, "TrustId")).unwrap_or_default();
    let now = now_unix();
    for dir in st.dirs.values_mut() {
        for t in dir.trusts.iter_mut() {
            if t.trust_id == trust_id {
                t.trust_state = "Verified".to_string();
                t.last_updated = now;
                t.state_last_updated = now;
                return ok(json!({"TrustId": trust_id}));
            }
        }
    }
    entity_does_not_exist(&format!("Trust {trust_id} does not exist"))
}

// ---------------------------------------------------------------------------
// LDAPS / settings
// ---------------------------------------------------------------------------

fn describe_ldaps_settings(st: &mut DsStore, body: &Value) -> Response {
    let dir_id = str_param(p(body, "DirectoryId")).unwrap_or_default();
    if let Some(r) = validate_dir_exists(st, &dir_id) {
        return r;
    }
    if let Some(r) = ldaps_guard(st, &dir_id) {
        return r;
    }
    let settings: Vec<Value> = st
        .dirs
        .get(&dir_id)
        .map(|d| d.ldaps.clone())
        .unwrap_or_default();
    let context = params_context(body, &["NextToken", "Limit"]);
    // moto paginated with no unique attribute (splits by limit only)
    let page = paginate::<Value>(
        settings,
        int_param(p(body, "Limit")),
        100,
        &context,
        p(body, "NextToken"),
        |_v| Value::Null.to_string(),
    );
    let (page, token) = match page {
        Ok(v) => v,
        Err(r) => return r,
    };
    // moto's quirk: lowercase "nextToken" here too
    ok(json!({
        "LDAPSSettingsInfo": page,
        "nextToken": token.map(Value::String).unwrap_or(Value::Null),
    }))
}

/// LDAPS is limited to MicrosoftAD/ADConnector (moto's `enable_ldaps` guard).
fn ldaps_guard(st: &DsStore, dir_id: &str) -> Option<Response> {
    st.dirs.get(dir_id).and_then(|d| {
        if d.directory_type == "MicrosoftAD" || d.directory_type == "ADConnector" {
            None
        } else {
            Some(unsupported(
                "LDAPS operations are not supported for this Directory Type.",
            ))
        }
    })
}

/// EnableLDAPS/DisableLDAPS share moto's Directory.enable_ldaps body.
fn set_ldaps(st: &mut DsStore, body: &Value, enable: bool) -> Response {
    let dir_id = str_param(p(body, "DirectoryId")).unwrap_or_default();
    if let Some(r) = validate_dir_exists(st, &dir_id) {
        return r;
    }
    if let Some(r) = ldaps_guard(st, &dir_id) {
        return r;
    }
    let d = st.dirs.get_mut(&dir_id).unwrap();
    if enable && d.ldaps.is_empty() {
        d.ldaps = vec![json!({
            "LastUpdatedDateTime": now_unix(),
            "LDAPSStatus": "Enabled",
            "LDAPSStatusReason": "",
        })];
    } else if !enable {
        for s in d.ldaps.iter_mut() {
            if let Some(o) = s.as_object_mut() {
                o.insert("LDAPSStatus".into(), Value::String("Disabled".into()));
            }
        }
    }
    ok_empty()
}

fn describe_settings(st: &mut DsStore, body: &Value) -> Response {
    let dir_id = str_param(p(body, "DirectoryId")).unwrap_or_default();
    if let Some(r) = validate_dir_exists(st, &dir_id) {
        return r;
    }
    let not_msad = st
        .dirs
        .get(&dir_id)
        .map(|d| d.directory_type != "MicrosoftAD")
        .unwrap_or(false);
    if not_msad {
        return invalid_parameter("This operation is only supported for Microsoft AD");
    }
    let status_filter = str_param(p(body, "Status"));
    let entries: Vec<Value> = st
        .dirs
        .get(&dir_id)
        .and_then(|d| d.msad_settings.clone())
        .unwrap_or_default()
        .into_iter()
        .filter(|e| {
            status_filter
                .as_deref()
                .map(|s| e.get("RequestStatus").and_then(Value::as_str) == Some(s))
                .unwrap_or(true)
        })
        .collect();
    ok(json!({
        "DirectoryId": dir_id,
        "SettingEntries": entries,
        "NextToken": Value::Null,
    }))
}

fn update_settings(st: &mut DsStore, body: &Value) -> Response {
    let dir_id = str_param(p(body, "DirectoryId")).unwrap_or_default();
    if let Some(r) = validate_dir_exists(st, &dir_id) {
        return r;
    }
    let not_msad = st
        .dirs
        .get(&dir_id)
        .map(|d| d.directory_type != "MicrosoftAD")
        .unwrap_or(false);
    if not_msad {
        return invalid_parameter("This operation is only supported for Microsoft AD");
    }
    if let Value::Array(updates) = p(body, "Settings") {
        if let Some(d) = st.dirs.get_mut(&dir_id) {
            if let Some(settings) = d.msad_settings.as_mut() {
                for u in updates {
                    let name = u.get("Name").and_then(Value::as_str).unwrap_or_default();
                    for s in settings.iter_mut() {
                        if s.get("Name").and_then(Value::as_str) == Some(name) {
                            if let Some(o) = s.as_object_mut() {
                                o.insert(
                                    "AppliedValue".into(),
                                    u.get("Value").cloned().unwrap_or(Value::Null),
                                );
                            }
                        }
                    }
                }
            }
        }
    }
    ok(json!({"DirectoryId": dir_id}))
}

fn describe_ca_enrollment_policy(st: &DsStore, body: &Value) -> Response {
    let dir_id = str_param(p(body, "DirectoryId")).unwrap_or_default();
    if let Some(r) = validate_dir_exists(st, &dir_id) {
        return r;
    }
    ok(json!({
        "DirectoryId": dir_id,
        "CaEnrollmentPolicyStatus": "Disabled",
    }))
}

// ---------------------------------------------------------------------------
// AD assessment (moto: describe raises not-found for every id)
// ---------------------------------------------------------------------------

fn describe_ad_assessment(body: &Value) -> Response {
    let id = str_param(p(body, "AssessmentId")).unwrap_or_default();
    entity_does_not_exist(&format!("Assessment {id} does not exist"))
}

fn start_ad_assessment(st: &mut DsStore, body: &Value) -> Response {
    let dir_id = str_param(p(body, "DirectoryId")).unwrap_or_default();
    if let Some(r) = validate_dir_exists(st, &dir_id) {
        return r;
    }
    ok(json!({"AssessmentId": new_id("a", 10)}))
}

// ---------------------------------------------------------------------------
// Conditional forwarders
// ---------------------------------------------------------------------------

fn create_conditional_forwarder(st: &mut DsStore, body: &Value) -> Response {
    let dir_id = str_param(p(body, "DirectoryId")).unwrap_or_default();
    if let Some(r) = validate_dir_exists(st, &dir_id) {
        return r;
    }
    let domain = str_param(p(body, "RemoteDomainName")).unwrap_or_default();
    let key = (dir_id, domain.clone());
    if st.cond_fwd.contains_key(&key) {
        return entity_already_exists(&format!(
            "Conditional forwarder for domain {domain} already exists"
        ));
    }
    st.cond_fwd.insert(key, array_strs(p(body, "DnsIpAddrs")));
    ok(json!({}))
}

fn delete_conditional_forwarder(st: &mut DsStore, body: &Value) -> Response {
    let dir_id = str_param(p(body, "DirectoryId")).unwrap_or_default();
    if let Some(r) = validate_dir_exists(st, &dir_id) {
        return r;
    }
    let domain = str_param(p(body, "RemoteDomainName")).unwrap_or_default();
    if st.cond_fwd.remove(&(dir_id, domain.clone())).is_none() {
        return entity_does_not_exist(&format!(
            "Conditional forwarder for domain {domain} does not exist"
        ));
    }
    ok(json!({}))
}

fn update_conditional_forwarder(st: &mut DsStore, body: &Value) -> Response {
    let dir_id = str_param(p(body, "DirectoryId")).unwrap_or_default();
    if let Some(r) = validate_dir_exists(st, &dir_id) {
        return r;
    }
    let domain = str_param(p(body, "RemoteDomainName")).unwrap_or_default();
    match st.cond_fwd.get_mut(&(dir_id.clone(), domain.clone())) {
        Some(v) => *v = array_strs(p(body, "DnsIpAddrs")),
        None => {
            return entity_does_not_exist(&format!(
                "Conditional forwarder for domain {domain} does not exist"
            ))
        }
    }
    ok(json!({}))
}

fn describe_conditional_forwarders(st: &DsStore, body: &Value) -> Response {
    let dir_id = str_param(p(body, "DirectoryId")).unwrap_or_default();
    if let Some(r) = validate_dir_exists(st, &dir_id) {
        return r;
    }
    let domains: Vec<String> = match p(body, "RemoteDomainNames") {
        Value::Array(l) => l.iter().filter_map(str_param).collect(),
        _ => Vec::new(),
    };
    let fwd: Vec<Value> = st
        .cond_fwd
        .iter()
        .filter(|((dir, domain), _)| {
            dir == &dir_id && (domains.is_empty() || domains.contains(domain))
        })
        .map(|((_, domain), ips)| {
            json!({
                "RemoteDomainName": domain,
                "DnsIpAddrs": ips,
                "ReplicationScope": "Domain",
            })
        })
        .collect();
    ok(json!({"ConditionalForwarders": fwd}))
}

// ---------------------------------------------------------------------------
// Domain controllers
// ---------------------------------------------------------------------------

fn describe_domain_controllers(st: &DsStore, body: &Value) -> Response {
    let dir_id = str_param(p(body, "DirectoryId")).unwrap_or_default();
    if let Some(r) = validate_dir_exists(st, &dir_id) {
        return r;
    }
    let target: Vec<String> = match p(body, "DomainControllerIds") {
        Value::Array(l) => l.iter().filter_map(str_param).collect(),
        _ => Vec::new(),
    };
    let d = st.dirs.get(&dir_id).unwrap();
    if d.directory_type != "MicrosoftAD" {
        return unsupported("Domain controllers are not supported for this Directory Type.");
    }
    let mut controllers = Vec::new();
    for (i, ip) in d.dns_ip_addrs.iter().enumerate() {
        let settings = d.vpc_settings.as_ref();
        let vpc_id = settings
            .and_then(|vs| str_param(&vs.get("VpcId").cloned().unwrap_or(Value::Null)))
            .unwrap_or_default();
        let subnet_id = settings
            .and_then(|vs| {
                vs.get("SubnetIds")
                    .and_then(Value::as_array)
                    .and_then(|l| l.get(i))
                    .and_then(Value::as_str)
                    .map(|s| s.to_string())
            })
            .unwrap_or_default();
        let az = settings
            .and_then(|vs| {
                vs.get("AvailabilityZones")
                    .and_then(Value::as_array)
                    .and_then(|l| l.get(i))
                    .and_then(Value::as_str)
                    .map(|s| s.to_string())
            })
            .unwrap_or_else(|| format!("{}a", d.region));
        controllers.push(json!({
            "DirectoryId": dir_id,
            "DomainControllerId": format!("{dir_id}-dc-{}-{i}", rand_hex(8)),
            "DnsIpAddr": ip,
            "VpcId": vpc_id,
            "SubnetId": subnet_id,
            "AvailabilityZone": az,
            "Status": "Active",
            "StatusReason": "",
            "LaunchTime": d.launch_time,
            "StatusLastUpdatedDateTime": d.stage_last_updated,
        }));
    }
    if !target.is_empty() {
        controllers.retain(|c| {
            target
                .iter()
                .any(|t| Some(t.as_str()) == c.get("DomainControllerId").and_then(Value::as_str))
        });
    }
    ok(json!({"DomainControllers": controllers, "NextToken": Value::Null}))
}

fn update_number_of_domain_controllers(st: &mut DsStore, body: &Value) -> Response {
    let dir_id = str_param(p(body, "DirectoryId")).unwrap_or_default();
    if let Some(r) = validate_dir_exists(st, &dir_id) {
        return r;
    }
    let number = p(body, "DesiredNumber").as_i64().unwrap_or(0);
    if let Some(d) = st.dirs.get_mut(&dir_id) {
        d.desired_number_of_domain_controllers = number;
    }
    ok(json!({}))
}

// ---------------------------------------------------------------------------
// Event topics
// ---------------------------------------------------------------------------

fn register_event_topic(st: &mut DsStore, body: &Value) -> Response {
    let dir_id = str_param(p(body, "DirectoryId")).unwrap_or_default();
    if let Some(r) = validate_dir_exists(st, &dir_id) {
        return r;
    }
    let topic_name = str_param(p(body, "TopicName")).unwrap_or_default();
    // AWS uses TopicName as the SNS topic (moto's `register_event_topic`)
    st.topics.insert(
        (dir_id.clone(), topic_name.clone()),
        json!({
            "DirectoryId": dir_id,
            "TopicName": topic_name,
            "TopicArn": topic_name,
            "CreatedDateTime": now_unix(),
            "Status": "Registered",
        }),
    );
    ok(json!({}))
}

fn deregister_event_topic(st: &mut DsStore, body: &Value) -> Response {
    let dir_id = str_param(p(body, "DirectoryId")).unwrap_or_default();
    if let Some(r) = validate_dir_exists(st, &dir_id) {
        return r;
    }
    let topic_name = str_param(p(body, "TopicName")).unwrap_or_default();
    if st
        .topics
        .remove(&(dir_id.clone(), topic_name.clone()))
        .is_none()
    {
        return entity_does_not_exist(&format!(
            "Topic {topic_name} does not exist for directory {dir_id}"
        ));
    }
    ok(json!({}))
}

fn describe_event_topics(st: &DsStore, body: &Value) -> Response {
    let dir_id = str_param(p(body, "DirectoryId"));
    let names: Vec<String> = match p(body, "TopicNames") {
        Value::Array(l) => l.iter().filter_map(str_param).collect(),
        _ => Vec::new(),
    };
    if let Some(d) = &dir_id {
        if let Some(r) = dir_exists(st, d) {
            return r;
        }
    }
    let topics: Vec<Value> = st
        .topics
        .values()
        .filter(|t| {
            dir_id
                .as_deref()
                .map(|x| x == t.get("DirectoryId").and_then(Value::as_str).unwrap_or(""))
                .unwrap_or(true)
                && (names.is_empty()
                    || names
                        .iter()
                        .any(|n| Some(n.as_str()) == t.get("TopicName").and_then(Value::as_str)))
        })
        .cloned()
        .collect();
    ok(json!({"EventTopics": topics}))
}

// ---------------------------------------------------------------------------
// Snapshots
// ---------------------------------------------------------------------------

fn create_snapshot(st: &mut DsStore, body: &Value) -> Response {
    let dir_id = str_param(p(body, "DirectoryId")).unwrap_or_default();
    if let Some(r) = validate_dir_exists(st, &dir_id) {
        return r;
    }
    st.seq += 1;
    let snap = Snapshot {
        snapshot_id: new_id("s", 10),
        directory_id: dir_id,
        name: str_param(p(body, "Name")).unwrap_or_default(),
        start: now_unix(),
    };
    let id = snap.snapshot_id.clone();
    st.snaps.insert(id.clone(), snap);
    ok(json!({"SnapshotId": id}))
}

fn delete_snapshot(st: &mut DsStore, body: &Value) -> Response {
    let id = str_param(p(body, "SnapshotId")).unwrap_or_default();
    if st.snaps.remove(&id).is_none() {
        return entity_does_not_exist(&format!("Snapshot {id} does not exist"));
    }
    ok(json!({"SnapshotId": id}))
}

fn restore_from_snapshot(st: &DsStore, body: &Value) -> Response {
    let id = str_param(p(body, "SnapshotId")).unwrap_or_default();
    if !st.snaps.contains_key(&id) {
        return entity_does_not_exist(&format!("Snapshot {id} does not exist"));
    }
    ok(json!({}))
}

fn describe_snapshots(st: &DsStore, body: &Value) -> Response {
    let dir_id = str_param(p(body, "DirectoryId"));
    let ids: Vec<String> = match p(body, "SnapshotIds") {
        Value::Array(l) => l.iter().filter_map(str_param).collect(),
        _ => Vec::new(),
    };
    if let Some(d) = &dir_id {
        if let Some(r) = dir_exists(st, d) {
            return r;
        }
    }
    let snaps: Vec<Value> = st
        .snaps
        .values()
        .filter(|s| {
            dir_id
                .as_deref()
                .map(|x| s.directory_id == x)
                .unwrap_or(true)
                && (ids.is_empty() || ids.contains(&s.snapshot_id))
        })
        .map(Snapshot::to_dict)
        .collect();
    ok(json!({"Snapshots": snaps, "NextToken": Value::Null}))
}

fn get_snapshot_limits(st: &DsStore, body: &Value) -> Response {
    let dir_id = str_param(p(body, "DirectoryId")).unwrap_or_default();
    if let Some(r) = validate_dir_exists(st, &dir_id) {
        return r;
    }
    ok(json!({
        "SnapshotLimits": {
            "ManualSnapshotsLimit": 5,
            "ManualSnapshotsCurrentCount": 0,
            "ManualSnapshotsLimitReached": false,
        }
    }))
}

// ---------------------------------------------------------------------------
// Regions / shared directories
// ---------------------------------------------------------------------------

fn describe_regions(req: &Request, st: &DsStore, body: &Value) -> Response {
    let dir_id = str_param(p(body, "DirectoryId")).unwrap_or_default();
    if let Some(r) = validate_dir_exists(st, &dir_id) {
        return r;
    }
    let d = st.dirs.get(&dir_id).unwrap();
    let (vpc_id, subnet_ids) = match (&d.vpc_settings, &d.connect_settings) {
        (Some(vs), _) => (
            str_param(&vs.get("VpcId").cloned().unwrap_or(Value::Null)).unwrap_or_default(),
            array_strs(&vs.get("SubnetIds").cloned().unwrap_or(Value::Null)),
        ),
        (None, Some(cs)) => (
            str_param(&cs.get("VpcId").cloned().unwrap_or(Value::Null)).unwrap_or_default(),
            array_strs(&cs.get("SubnetIds").cloned().unwrap_or(Value::Null)),
        ),
        _ => (String::new(), Vec::new()),
    };
    let mut regions = vec![json!({
        "DirectoryId": dir_id,
        "RegionName": req.region,
        "RegionType": "Primary",
        "Status": "Active",
        "VpcSettings": {"VpcId": vpc_id, "SubnetIds": subnet_ids},
        "DesiredNumberOfDomainControllers": d.desired_number_of_domain_controllers,
        "LaunchTime": d.launch_time,
        "StatusLastUpdatedDateTime": d.stage_last_updated,
        "LastUpdatedDateTime": d.stage_last_updated,
    })];
    if let Some(target) = str_param(p(body, "RegionName")) {
        regions.retain(|r| r.get("RegionName").and_then(Value::as_str) == Some(&*target));
    }
    ok(json!({"RegionsDescription": regions, "NextToken": Value::Null}))
}

/// moto's backend returns [] unconditionally (only existence is checked)
fn describe_shared_directories(st: &DsStore, body: &Value) -> Response {
    let owner = str_param(p(body, "OwnerDirectoryId")).unwrap_or_default();
    if let Some(r) = validate_dir_exists(st, &owner) {
        return r;
    }
    ok(json!({"SharedDirectories": [], "NextToken": Value::Null}))
}

// ---------------------------------------------------------------------------
// Radius / client auth
// ---------------------------------------------------------------------------

fn enable_radius(st: &mut DsStore, body: &Value) -> Response {
    let dir_id = str_param(p(body, "DirectoryId")).unwrap_or_default();
    if let Some(r) = validate_dir_exists(st, &dir_id) {
        return r;
    }
    st.radius.insert(dir_id, p(body, "RadiusSettings").clone());
    ok(json!({}))
}

fn disable_radius(st: &mut DsStore, body: &Value) -> Response {
    let dir_id = str_param(p(body, "DirectoryId")).unwrap_or_default();
    if let Some(r) = validate_dir_exists(st, &dir_id) {
        return r;
    }
    st.radius.remove(&dir_id);
    ok(json!({}))
}

fn update_radius(st: &mut DsStore, body: &Value) -> Response {
    let dir_id = str_param(p(body, "DirectoryId")).unwrap_or_default();
    if let Some(r) = validate_dir_exists(st, &dir_id) {
        return r;
    }
    if !st.radius.contains_key(&dir_id) {
        return entity_does_not_exist(&format!("RADIUS is not enabled for directory {dir_id}"));
    }
    st.radius.insert(dir_id, p(body, "RadiusSettings").clone());
    ok(json!({}))
}

fn set_client_auth(st: &mut DsStore, body: &Value, enable: bool) -> Response {
    let dir_id = str_param(p(body, "DirectoryId")).unwrap_or_default();
    if let Some(r) = validate_dir_exists(st, &dir_id) {
        return r;
    }
    let not_msad = st
        .dirs
        .get(&dir_id)
        .map(|d| d.directory_type != "MicrosoftAD")
        .unwrap_or(false);
    if not_msad {
        return unsupported(
            "Client authentication is only supported for Microsoft AD directories.",
        );
    }
    let auth_type = str_param(p(body, "Type")).unwrap_or_default();
    let entry = st.client_auth.entry(dir_id).or_default();
    if enable {
        if let Some(pos) = entry.iter().position(|(t, _, _)| t == &auth_type) {
            entry[pos].1 = "Enabled".to_string();
        } else {
            entry.push((auth_type, "Enabled".to_string(), now_unix()));
        }
    } else {
        entry.retain(|(t, _, _)| t != &auth_type);
    }
    ok(json!({}))
}

fn describe_client_auth(st: &DsStore, body: &Value) -> Response {
    let dir_id = str_param(p(body, "DirectoryId")).unwrap_or_default();
    if let Some(r) = validate_dir_exists(st, &dir_id) {
        return r;
    }
    let want = str_param(p(body, "Type"));
    let rows: Vec<Value> = st
        .client_auth
        .get(&dir_id)
        .cloned()
        .unwrap_or_default()
        .into_iter()
        .filter(|(k, _, _)| want.as_deref().map(|w| k == w).unwrap_or(true))
        .map(|(k, v, t)| json!({"Type": k, "Status": v, "LastUpdatedDateTime": t}))
        .collect();
    ok(json!({
        "ClientAuthenticationSettingsInfo": rows,
        "NextToken": Value::Null,
    }))
}

// ---------------------------------------------------------------------------
// Computers
// ---------------------------------------------------------------------------

fn create_computer(st: &mut DsStore, body: &Value) -> Response {
    let dir_id = str_param(p(body, "DirectoryId")).unwrap_or_default();
    if let Some(r) = validate_dir_exists(st, &dir_id) {
        return r;
    }
    st.seq += 1;
    let computer = json!({
        "ComputerId": format!("comp-{}", rand_hex(10)),
        "ComputerName": str_param(p(body, "ComputerName")).unwrap_or_default(),
        "ComputerAttributes": p(body, "ComputerAttributes")
            .as_array()
            .cloned()
            .unwrap_or_default(),
    });
    st.computers
        .entry(dir_id)
        .or_default()
        .push(computer.clone());
    ok(json!({"Computer": computer}))
}

// ---------------------------------------------------------------------------
// Certificates
// ---------------------------------------------------------------------------

fn register_certificate(st: &mut DsStore, body: &Value) -> Response {
    let dir_id = str_param(p(body, "DirectoryId")).unwrap_or_default();
    if let Some(r) = validate_dir_exists(st, &dir_id) {
        return r;
    }
    if st
        .dirs
        .get(&dir_id)
        .map(|d| d.directory_type != "MicrosoftAD")
        .unwrap_or(false)
    {
        return unsupported("Certificates are only supported for Microsoft AD directories.");
    }
    st.seq += 1;
    let now = now_unix();
    let ccas = p(body, "ClientCertAuthSettings");
    let cert = Cert {
        certificate_id: new_id("c", 10),
        directory_id: dir_id,
        client_cert_auth_settings: (!ccas.is_null()).then(|| ccas.clone()),
        cert_type: str_param(p(body, "Type")).unwrap_or_else(|| "ClientCertAuth".to_string()),
        registered: now,
        expiry: now + 365.0 * 24.0 * 3600.0,
    };
    let id = cert.certificate_id.clone();
    st.certs.insert(id.clone(), cert);
    ok(json!({"CertificateId": id}))
}

fn deregister_certificate(st: &mut DsStore, body: &Value) -> Response {
    let dir_id = str_param(p(body, "DirectoryId")).unwrap_or_default();
    if let Some(r) = validate_dir_exists(st, &dir_id) {
        return r;
    }
    let cid = str_param(p(body, "CertificateId")).unwrap_or_default();
    let matched = st
        .certs
        .get(&cid)
        .map(|c| c.directory_id == dir_id)
        .unwrap_or(false);
    if !matched {
        return entity_does_not_exist(&format!("Certificate {cid} does not exist"));
    }
    st.certs.remove(&cid);
    ok(json!({}))
}

fn list_certificates(st: &DsStore, body: &Value) -> Response {
    let dir_id = str_param(p(body, "DirectoryId")).unwrap_or_default();
    if let Some(r) = validate_dir_exists(st, &dir_id) {
        return r;
    }
    let certs: Vec<Value> = st
        .certs
        .values()
        .filter(|c| c.directory_id == dir_id)
        .map(Cert::to_dict)
        .collect();
    ok(json!({"CertificatesInfo": certs, "NextToken": Value::Null}))
}

fn describe_certificate(st: &DsStore, body: &Value) -> Response {
    let dir_id = str_param(p(body, "DirectoryId")).unwrap_or_default();
    if let Some(r) = validate_dir_exists(st, &dir_id) {
        return r;
    }
    let cid = str_param(p(body, "CertificateId")).unwrap_or_default();
    match st.certs.get(&cid).filter(|c| c.directory_id == dir_id) {
        Some(c) => ok(json!({"Certificate": c.to_full_dict()})),
        None => entity_does_not_exist(&format!("Certificate {cid} does not exist")),
    }
}

// ---------------------------------------------------------------------------
// IP routes
// ---------------------------------------------------------------------------

fn add_ip_routes(st: &mut DsStore, body: &Value) -> Response {
    let dir_id = str_param(p(body, "DirectoryId")).unwrap_or_default();
    if let Some(r) = validate_dir_exists(st, &dir_id) {
        return r;
    }
    if let Value::Array(routes) = p(body, "IpRoutes") {
        for route in routes {
            let cidr = str_param(p(route, "CidrIp")).unwrap_or_default();
            if st
                .ip_routes
                .iter()
                .any(|(d, c, _, _)| d == &dir_id && c == &cidr)
            {
                return entity_already_exists(&format!(
                    "IP route {cidr} already exists for directory {dir_id}"
                ));
            }
            let desc = route
                .get("Description")
                .and_then(Value::as_str)
                .unwrap_or("")
                .to_string();
            st.ip_routes.push((dir_id.clone(), cidr, now_unix(), desc));
        }
    }
    ok(json!({}))
}

fn remove_ip_routes(st: &mut DsStore, body: &Value) -> Response {
    let dir_id = str_param(p(body, "DirectoryId")).unwrap_or_default();
    if let Some(r) = validate_dir_exists(st, &dir_id) {
        return r;
    }
    let cids: Vec<String> = match p(body, "CidrIps") {
        Value::Array(l) => l.iter().filter_map(str_param).collect(),
        _ => Vec::new(),
    };
    for cidr in &cids {
        if !st
            .ip_routes
            .iter()
            .any(|(d, c, _, _)| d == &dir_id && c == cidr)
        {
            return entity_does_not_exist(&format!(
                "IP route {cidr} does not exist for directory {dir_id}"
            ));
        }
    }
    st.ip_routes
        .retain(|(d, c, _, _)| !(d == &dir_id && cids.contains(c)));
    ok(json!({}))
}

fn list_ip_routes(st: &DsStore, body: &Value) -> Response {
    let dir_id = str_param(p(body, "DirectoryId")).unwrap_or_default();
    if let Some(r) = validate_dir_exists(st, &dir_id) {
        return r;
    }
    let routes: Vec<Value> = st
        .ip_routes
        .iter()
        .filter(|(d, _, _, _)| d == &dir_id)
        .map(|(d, c, added, desc)| {
            json!({
                "DirectoryId": d,
                "CidrIp": c,
                "IpRouteStatusMsg": "Added",
                "AddedDateTime": added,
                "IpRouteStatusReason": "",
                "Description": desc,
            })
        })
        .collect();
    ok(json!({"IpRoutesInfo": routes, "NextToken": Value::Null}))
}

// ---------------------------------------------------------------------------
// Schema extensions
// ---------------------------------------------------------------------------

fn start_schema_extension(st: &mut DsStore, body: &Value) -> Response {
    let dir_id = str_param(p(body, "DirectoryId")).unwrap_or_default();
    if let Some(r) = validate_dir_exists(st, &dir_id) {
        return r;
    }
    if st
        .dirs
        .get(&dir_id)
        .map(|d| d.directory_type != "MicrosoftAD")
        .unwrap_or(false)
    {
        return unsupported("Schema extensions are only supported for Microsoft AD directories.");
    }
    st.seq += 1;
    let ext = SchemaExt {
        schema_extension_id: new_id("e", 10),
        directory_id: dir_id,
        description: str_param(p(body, "Description")).unwrap_or_default(),
        status: "Completed".to_string(),
        status_reason: String::new(),
        start: now_unix(),
        end: now_unix(),
    };
    let id = ext.schema_extension_id.clone();
    st.schema_exts.insert(id.clone(), ext);
    ok(json!({"SchemaExtensionId": id}))
}

fn cancel_schema_extension(st: &mut DsStore, body: &Value) -> Response {
    let dir_id = str_param(p(body, "DirectoryId")).unwrap_or_default();
    if let Some(r) = validate_dir_exists(st, &dir_id) {
        return r;
    }
    let eid = str_param(p(body, "SchemaExtensionId")).unwrap_or_default();
    match st.schema_exts.get_mut(&eid) {
        Some(e) if e.directory_id == dir_id => {
            e.status = "CancelledByUser".to_string();
            e.end = now_unix();
            ok(json!({}))
        }
        _ => entity_does_not_exist(&format!("Schema extension {eid} does not exist")),
    }
}

fn list_schema_extensions(st: &DsStore, body: &Value) -> Response {
    let dir_id = str_param(p(body, "DirectoryId")).unwrap_or_default();
    if let Some(r) = validate_dir_exists(st, &dir_id) {
        return r;
    }
    let exts: Vec<Value> = st
        .schema_exts
        .values()
        .filter(|e| e.directory_id == dir_id)
        .map(SchemaExt::to_dict)
        .collect();
    ok(json!({"SchemaExtensionsInfo": exts, "NextToken": Value::Null}))
}

// ---------------------------------------------------------------------------
// Log subscriptions
// ---------------------------------------------------------------------------

fn create_log_subscription(st: &mut DsStore, body: &Value) -> Response {
    let dir_id = str_param(p(body, "DirectoryId")).unwrap_or_default();
    if let Some(r) = validate_dir_exists(st, &dir_id) {
        return r;
    }
    if st.log_subs.contains_key(&dir_id) {
        return entity_already_exists("Log subscription already exists");
    }
    st.log_subs.insert(
        dir_id.clone(),
        LogSub {
            directory_id: dir_id,
            log_group_name: str_param(p(body, "LogGroupName")).unwrap_or_default(),
            created: now_unix(),
        },
    );
    ok(json!({}))
}

fn list_log_subscriptions(st: &DsStore, body: &Value) -> Response {
    // moto: no DirectoryId lists every subscription; with one, validate first
    let dir_id = str_param(p(body, "DirectoryId"));
    if let Some(d) = &dir_id {
        if let Some(r) = validate_dir_exists(st, d) {
            return r;
        }
    }
    let subs: Vec<Value> = st
        .log_subs
        .values()
        .filter(|s| {
            dir_id
                .as_deref()
                .map(|x| s.directory_id == x)
                .unwrap_or(true)
        })
        .map(|s| {
            json!({
                "SubscriptionCreatedDateTime": s.created,
                "DirectoryId": s.directory_id,
                "LogGroupName": s.log_group_name,
            })
        })
        .collect();
    ok(json!({"LogSubscriptions": subs, "NextToken": Value::Null}))
}

fn delete_log_subscription(st: &mut DsStore, body: &Value) -> Response {
    let dir_id = str_param(p(body, "DirectoryId")).unwrap_or_default();
    if let Some(r) = validate_dir_exists(st, &dir_id) {
        return r;
    }
    if st.log_subs.remove(&dir_id).is_none() {
        return entity_does_not_exist(&format!("Log subscription for {dir_id} does not exist"));
    }
    ok(json!({}))
}

// ---------------------------------------------------------------------------
// Share directory (moto routes these; its models.py has no backend — the
// era probe recorded modeled errors, mirrored here, suite-compatible)
// ---------------------------------------------------------------------------

fn share_directory(req: &Request, st: &mut DsStore, body: &Value) -> Response {
    let dir_id = str_param(p(body, "DirectoryId")).unwrap_or_default();
    if let Some(r) = validate_dir_exists(st, &dir_id) {
        return r;
    }
    let target = p(body, "ShareTarget");
    let target_id = target
        .get("Id")
        .and_then(Value::as_str)
        .unwrap_or_default()
        .to_string();
    let method = str_param(p(body, "ShareMethod")).unwrap_or_else(|| "ORGANIZATIONS".to_string());
    st.seq += 1;
    let shared_directory_id = new_id("d", 10);
    // ORGANIZATIONS shares complete immediately; HANDSHAKE waits for accept
    let status = if method == "ORGANIZATIONS" {
        "Shared"
    } else {
        "PendingAcceptance"
    };
    st.shares.push(Share {
        shared_directory_id: shared_directory_id.clone(),
        owner_directory_id: dir_id,
        owner_account: req.account,
        shared_account_id: target_id,
        method,
        notes: str_param(p(body, "ShareNotes")).unwrap_or_default(),
        status: status.to_string(),
    });
    ok(json!({"SharedDirectoryId": shared_directory_id}))
}

fn unshare_directory(st: &mut DsStore, body: &Value) -> Response {
    let dir_id = str_param(p(body, "DirectoryId")).unwrap_or_default();
    if let Some(r) = validate_dir_exists(st, &dir_id) {
        return r;
    }
    let target_id = p(body, "UnshareTarget")
        .get("Id")
        .and_then(Value::as_str)
        .unwrap_or_default()
        .to_string();
    let pos = st.shares.iter().position(|s| {
        s.owner_directory_id == dir_id
            && s.shared_account_id == target_id
            && !matches!(s.status.as_str(), "Deleted" | "Deleting" | "Rejected")
    });
    match pos {
        Some(pos) => {
            let share = st.shares.remove(pos);
            ok(json!({"SharedDirectoryId": share.shared_directory_id}))
        }
        None => entity_does_not_exist(&format!(
            "Shared directory for target {target_id} does not exist"
        )),
    }
}

fn accept_shared_directory(st: &mut DsStore, body: &Value) -> Response {
    let share_id = str_param(p(body, "SharedDirectoryId")).unwrap_or_default();
    match st
        .shares
        .iter_mut()
        .find(|s| s.shared_directory_id == share_id && s.status == "PendingAcceptance")
    {
        Some(share) => {
            share.status = "Accepted".to_string();
            ok(json!({"SharedDirectory": shared_dir_dict(share)}))
        }
        None => entity_does_not_exist(&format!("Shared directory {share_id} does not exist")),
    }
}

fn reject_shared_directory(st: &mut DsStore, body: &Value) -> Response {
    let share_id = str_param(p(body, "SharedDirectoryId")).unwrap_or_default();
    match st
        .shares
        .iter_mut()
        .find(|s| s.shared_directory_id == share_id && s.status == "PendingAcceptance")
    {
        Some(share) => {
            share.status = "Rejected".to_string();
            ok(json!({"SharedDirectoryId": share.shared_directory_id.clone()}))
        }
        None => entity_does_not_exist(&format!("Shared directory {share_id} does not exist")),
    }
}

fn shared_dir_dict(share: &Share) -> Value {
    json!({
        "OwnerAccountId": share.owner_account.to_string(),
        "OwnerDirectoryId": share.owner_directory_id,
        "SharedAccountId": share.shared_account_id,
        "SharedDirectoryId": share.shared_directory_id,
        "ShareMethod": share.method,
        "ShareNotes": if share.notes.is_empty() {
            Value::Null
        } else {
            Value::String(share.notes.clone())
        },
        "ShareStatus": share.status,
    })
}

// ---------------------------------------------------------------------------
// Existence helpers (moto's `_validate_directory_id`: regex then existence)
// ---------------------------------------------------------------------------

fn dir_exists(st: &DsStore, id: &str) -> Option<Response> {
    if st.dirs.contains_key(id) {
        None
    } else {
        Some(entity_does_not_exist(&format!(
            "Directory {id} does not exist"
        )))
    }
}

fn validate_dir_exists(st: &DsStore, id: &str) -> Option<Response> {
    match validate_directory_id(&Value::String(id.to_string())) {
        "" => dir_exists(st, id),
        msg => Some(ds_validation_exception(&[(
            "directoryId".to_string(),
            id.to_string(),
            msg.to_string(),
        )])),
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    fn req(op: &str) -> Request {
        Request {
            service: "ds".to_string(),
            operation: op.to_string(),
            method: "POST".to_string(),
            path: "/".to_string(),
            query_string: String::new(),
            headers: HashMap::new(),
            body: b"{}".to_vec().into(),
            account: 123456789012,
            region: "us-east-1".to_string(),
        }
    }

    fn req_body(op: &str, body: &str) -> Request {
        let mut r = req(op);
        r.body = body.as_bytes().to_vec().into();
        r
    }

    fn body_of(resp: &Response) -> Value {
        serde_json::from_slice(&resp.body).unwrap_or(Value::Null)
    }

    #[test]
    fn dispatch_ignores_other_services() {
        let mut r = req("CreateDirectory");
        r.service = "other".to_string();
        assert!(dispatch(&r).is_none());
    }

    #[test]
    fn double_absent_ops_answer_moto_501() {
        for op in [
            "CreateHybridAD",
            "DescribeHybridADUpdate",
            "UpdateHybridAD",
            "UpdateTrust",
        ] {
            let resp = dispatch(&req(op)).unwrap();
            assert_eq!(resp.status, 501, "{op}");
            let body = body_of(&resp);
            assert_eq!(body["__type"], "NotImplemented");
            assert_eq!(
                body["message"],
                format!("The {} action has not been implemented", snake_case(op))
            );
        }
    }

    #[test]
    fn unknown_op_answers_not_implemented() {
        let resp = dispatch(&req("DescribeDirectoriesX")).unwrap();
        assert_eq!(resp.status, 501);
        assert!(body_of(&resp)["message"]
            .as_str()
            .unwrap()
            .starts_with("The describe_directories_x action"));
    }

    #[test]
    fn handles_routes_every_botocore_operation() {
        // The 80 botocore operation names should all route (some to 501).
        let ops: &[(&str, u16)] = &[
            ("AcceptSharedDirectory", 400),
            ("AddIpRoutes", 400),
            ("AddRegion", 400),
            ("AddTagsToResource", 400),
            ("CancelSchemaExtension", 400),
            ("ConnectDirectory", 400),
            ("CreateAlias", 400),
            ("CreateComputer", 400),
            ("CreateConditionalForwarder", 400),
            ("CreateDirectory", 400),
            ("CreateHybridAD", 501),
            ("CreateLogSubscription", 400),
            ("CreateMicrosoftAD", 400),
            ("CreateSnapshot", 400),
            ("CreateTrust", 400),
            ("DeleteADAssessment", 400),
            ("DeleteConditionalForwarder", 400),
            ("DeleteDirectory", 400),
            ("DeleteLogSubscription", 400),
            ("DeleteSnapshot", 400),
            ("DeleteTrust", 400),
            ("DeregisterCertificate", 400),
            ("DeregisterEventTopic", 400),
            ("DescribeADAssessment", 400),
            ("DescribeCAEnrollmentPolicy", 400),
            ("DescribeCertificate", 400),
            ("DescribeClientAuthenticationSettings", 400),
            ("DescribeConditionalForwarders", 400),
            ("DescribeDirectories", 200),
            ("DescribeDirectoryDataAccess", 400),
            ("DescribeDomainControllers", 400),
            ("DescribeEventTopics", 400),
            ("DescribeHybridADUpdate", 501),
            ("DescribeLDAPSSettings", 400),
            ("DescribeRegions", 400),
            ("DescribeSettings", 400),
            ("DescribeSharedDirectories", 400),
            ("DescribeSnapshots", 400),
            ("DescribeTrusts", 400),
            ("DescribeUpdateDirectory", 400),
            ("DisableCAEnrollmentPolicy", 400),
            ("DisableClientAuthentication", 400),
            ("DisableDirectoryDataAccess", 400),
            ("DisableLDAPS", 400),
            ("DisableRadius", 400),
            ("DisableSso", 400),
            ("EnableCAEnrollmentPolicy", 400),
            ("EnableClientAuthentication", 400),
            ("EnableDirectoryDataAccess", 400),
            ("EnableLDAPS", 400),
            ("EnableRadius", 400),
            ("EnableSso", 400),
            ("GetDirectoryLimits", 200),
            ("GetSnapshotLimits", 400),
            ("ListADAssessments", 200),
            ("ListCertificates", 400),
            ("ListIpRoutes", 400),
            ("ListLogSubscriptions", 400),
            ("ListSchemaExtensions", 400),
            ("ListTagsForResource", 400),
            ("RegisterCertificate", 400),
            ("RegisterEventTopic", 400),
            ("RejectSharedDirectory", 400),
            ("RemoveIpRoutes", 400),
            ("RemoveRegion", 400),
            ("RemoveTagsFromResource", 400),
            ("ResetUserPassword", 400),
            ("RestoreFromSnapshot", 400),
            ("ShareDirectory", 400),
            ("StartADAssessment", 400),
            ("StartSchemaExtension", 400),
            ("UnshareDirectory", 400),
            ("UpdateConditionalForwarder", 400),
            ("UpdateDirectorySetup", 400),
            ("UpdateHybridAD", 501),
            ("UpdateNumberOfDomainControllers", 400),
            ("UpdateRadius", 400),
            ("UpdateSettings", 400),
            ("UpdateTrust", 501),
            ("VerifyTrust", 400),
        ];
        for (op, status) in ops {
            let resp = dispatch(&req_body(op, r#"{"DirectoryId":"d-0000000000"}"#)).unwrap();
            assert_eq!(resp.status, *status, "{op}");
        }
    }

    #[test]
    fn describe_directories_empty_state() {
        let resp = dispatch(&req("DescribeDirectories")).unwrap();
        assert_eq!(resp.status, 200);
        let body = body_of(&resp);
        assert!(body["DirectoryDescriptions"].as_array().unwrap().is_empty());
        assert!(body["NextToken"].is_null());
    }

    #[test]
    fn validation_errors_accumulate_like_moto() {
        let body = json!({
            "Name": "bad_name",
            "Password": "short",
            "Size": "Medium",
            "VpcSettings": {"VpcId": "vpc-0", "SubnetIds": ["subnet-1", "subnet-2"]}
        });
        let mut r = req("CreateDirectory");
        r.body = serde_json::to_vec(&body).unwrap().into();
        let resp = dispatch(&r).unwrap();
        assert_eq!(resp.status, 400);
        let b = body_of(&resp);
        assert_eq!(b["__type"], "ValidationException");
        let msg = b["message"].as_str().unwrap();
        // name + size + password + subnet-id pattern: four accumulated errors
        assert!(msg.starts_with("4 validation errors detected: "), "{msg}");
    }

    #[test]
    fn invalid_directory_id_fails_regex_first() {
        let st = DsStore::default();
        let resp = validate_dir_exists(&st, "d-bogus123").unwrap();
        assert_eq!(resp.status, 400);
        assert_eq!(body_of(&resp)["__type"], "ValidationException");
        let resp = validate_dir_exists(&st, "d-0000000000").unwrap();
        assert_eq!(body_of(&resp)["__type"], "EntityDoesNotExistException");
    }

    #[test]
    fn alias_pattern_matches_moto() {
        assert!(alias_pattern_ok("alias12345678"));
        assert!(alias_pattern_ok("a-b-c"));
        assert!(!alias_pattern_ok("d-corp"));
        assert!(!alias_pattern_ok("-corp"));
        assert!(!alias_pattern_ok("corp-"));
        assert!(!alias_pattern_ok("co rp"));
    }

    #[test]
    fn name_pattern_matches_moto() {
        assert_eq!(validate_name(&Value::String("corp.example.com".into())), "");
        assert_eq!(validate_name(&Value::String("a-b.c".into())), "");
        assert!(!validate_name(&Value::String("no_separators".into())).is_empty());
    }

    #[test]
    fn password_pattern_class_combinations() {
        assert_eq!(validate_password(&Value::String("P@ssw0rd!".into())), "");
        assert_eq!(validate_password(&Value::String("TrustP@ss1!".into())), "");
        assert!(!validate_password(&Value::String("alllowercase1".into())).is_empty());
        assert!(!validate_password(&Value::String("Sh0rt".into())).is_empty());
    }

    #[test]
    fn snake_case_matches_moto_transform() {
        assert_eq!(snake_case("CreateDirectory"), "create_directory");
        assert_eq!(
            snake_case("DescribeLDAPSSettings"),
            "describe_ldaps_settings"
        );
        assert_eq!(
            snake_case("DescribeCAEnrollmentPolicy"),
            "describe_ca_enrollment_policy"
        );
        assert_eq!(snake_case("EnableSso"), "enable_sso");
        assert_eq!(snake_case("AddIpRoutes"), "add_ip_routes");
        assert_eq!(snake_case("ListADAssessments"), "list_ad_assessments");
    }

    #[test]
    fn pagination_token_roundtrip_and_limit() {
        let tags: Vec<Value> = (0..5)
            .map(|i| json!({"Key": format!("k{i}"), "Value": format!("v{i}")}))
            .collect();
        let context = "r";
        let cs = checksum(context);
        // page 1: limit 2
        let (page1, tok1) = paginate(tags.clone(), Some(2), 50, context, &Value::Null, |t| {
            t["Key"].as_str().unwrap_or("").to_string()
        })
        .unwrap();
        assert_eq!(page1.len(), 2);
        assert!(tok1.is_some());
        // page 2 continues after page1's last token entry
        let page2 = paginate(
            tags.clone(),
            Some(2),
            50,
            context,
            &Value::String(tok1.unwrap()),
            |t| t["Key"].as_str().unwrap_or("").to_string(),
        )
        .unwrap();
        assert_eq!(page2.0.len(), 2);
        assert_eq!(page2.0[0]["Key"], "k2");
        // checksum mismatch → InvalidNextTokenException
        let bad = paginate(
            tags,
            Some(2),
            50,
            "other-context",
            &Value::String(encode_token(cs, "k2")),
            |t| t["Key"].as_str().unwrap_or("").to_string(),
        )
        .unwrap_err();
        assert_eq!(bad.status, 400);
        assert_eq!(body_of(&bad)["__type"], "InvalidNextTokenException");
    }

    #[test]
    fn subnet_checks_mirror_moto_messages() {
        // not real request plumbing here: only the branch table is checked
        let mut settings: Map<String, Value> = serde_json::from_value(json!({
            "VpcId": "vpc-00000000000000000",
            "SubnetIds": ["subnet-00000000000000001", "subnet-00000000000000002"]
        }))
        .unwrap();
        let unknown = verify_subnets(&req("CreateDirectory"), &mut settings);
        assert!(matches!(unknown, Err(SubnetReject::Invalid)));
        // wrong count also lands in the Invalid branch
        let mut one: Map<String, Value> = serde_json::from_value(json!({
            "VpcId": "vpc-00000000000000000",
            "SubnetIds": ["subnet-00000000"]
        }))
        .unwrap();
        assert!(matches!(
            verify_subnets(&req("CreateDirectory"), &mut one),
            Err(SubnetReject::Invalid)
        ));
    }
}
