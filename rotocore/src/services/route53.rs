//! Route 53 — native REST-XML backend.
//!
//! Replaces the python-era native provider (the old tree's
//! `src/robotocore/services/route53/`) that itself wrapped moto for the
//! `/2013-04-01` REST-XML surface: hosted zones, resource record sets,
//! health checks, reusable delegation sets, tags, CIDR collections, query
//! logging configs, VPC association + authorizations, traffic policies /
//! instances, key signing keys, hosted-zone DNSSEC, geo locations, checker
//! IP ranges, TestDNSAnswer and the limit/counter operations.
//!
//! Wire shapes mirror moto's `route53` templates plus the botocore shapes
//! the old provider rendered verbatim (the pytest compat suite
//! `tests/compatibility/test_route53_compat.py` is the spec). Like moto,
//! Route 53 state is per account only — the service is global, so the
//! "(account, region)" slot is keyed with a fixed global region label.

use std::collections::{BTreeMap, HashMap};

use crate::proto::{query_map, url_decode, xml_escape};
use crate::store;
use crate::{Request, Response};
use quick_xml::events::Event;
use quick_xml::Reader;

const XMLNS: &str = "https://route53.amazonaws.com/doc/2013-04-01/";
/// route53 is a global service: moto keys it per partition, we key per
/// account under this fixed region label.
const GLOBAL: &str = "global";
/// Prefix returned in zone/change ids on the wire (moto's wire format).
const ZONE_PREFIX: &str = "/hostedzone/";
const CHANGE_PREFIX: &str = "/change/";

// ---------------------------------------------------------------------------
// XML tree (same minimal element tree as cloudfront's REST-XML parser)
// ---------------------------------------------------------------------------

/// A minimal element tree for REST-XML request bodies. Namespaces are not
/// modelled (botocore only ever sets a single xmlns attribute on the root).
#[derive(Debug, Default, Clone)]
struct El {
    name: String,
    text: String,
    children: Vec<El>,
}

impl El {
    fn child(&self, name: &str) -> Option<&El> {
        self.children.iter().find(|c| c.name == name)
    }

    /// Follow a chain of child names (`el.sub(&["A", "B"])` is `el > A > B`).
    fn sub(&self, path: &[&str]) -> Option<&El> {
        match path {
            [] => Some(self),
            [head, tail @ ..] => self.child(head).and_then(|c| c.sub(tail)),
        }
    }

    /// Element text (trimmed).
    fn as_str(&self) -> &str {
        self.text.trim()
    }

    /// First text of the named child, if any.
    fn text_of(&self, name: &str) -> Option<&str> {
        self.child(name).map(El::as_str)
    }

    /// Text of the named child or a default.
    fn text_or(&self, name: &str, default: &str) -> String {
        self.text_of(name).unwrap_or(default).to_string()
    }

    /// All child nodes named `name`.
    fn components<'a>(&'a self, name: &'a str) -> impl Iterator<Item = &'a El> + 'a {
        self.children.iter().filter(move |c| c.name == name)
    }

    /// All child nodes named `name` at a nested path (`ChangeBatch > Changes`).
    fn els_at<'a>(&'a self, path: &[&str], name: &'a str) -> Vec<&'a El> {
        match self.sub(path) {
            Some(node) => node.children.iter().filter(|c| c.name == name).collect(),
            None => Vec::new(),
        }
    }

    /// Text values of all children named `name` at a nested path.
    fn texts_at(&self, path: &[&str], name: &str) -> Vec<String> {
        self.els_at(path, name)
            .iter()
            .map(|c| c.as_str().to_string())
            .collect()
    }

    /// Text of the named child parsed as an integer, only when present.
    fn int_of(&self, name: &str) -> Option<i64> {
        self.child(name)
            .and_then(|e| e.as_str().parse::<i64>().ok())
    }
}

fn parse_xml(body: &[u8]) -> Option<El> {
    if body.is_empty() {
        return None;
    }
    let mut reader = Reader::from_reader(body);
    let mut stack: Vec<El> = Vec::new();
    let mut top: Option<El> = None;
    let mut buf = Vec::new();
    loop {
        match reader.read_event_into(&mut buf) {
            Ok(Event::Start(e)) => stack.push(El {
                name: std::str::from_utf8(e.local_name().as_ref())
                    .unwrap_or("")
                    .to_string(),
                text: String::new(),
                children: Vec::new(),
            }),
            Ok(Event::Empty(e)) => {
                let el = El {
                    name: std::str::from_utf8(e.local_name().as_ref())
                        .unwrap_or("")
                        .to_string(),
                    text: String::new(),
                    children: Vec::new(),
                };
                match stack.last_mut() {
                    Some(node) => node.children.push(el),
                    None => {
                        top = Some(el);
                        break;
                    }
                }
            }
            Ok(Event::Text(t)) => {
                if let (Some(node), Ok(text)) = (stack.last_mut(), t.unescape()) {
                    if !text.trim().is_empty() {
                        node.text.push_str(&text);
                    }
                }
            }
            Ok(Event::CData(t)) => {
                if let Some(node) = stack.last_mut() {
                    node.text.push_str(&String::from_utf8_lossy(t.as_ref()));
                }
            }
            Ok(Event::End(_)) => {
                let Some(done) = stack.pop() else {
                    break;
                };
                match stack.last_mut() {
                    Some(parent) => parent.children.push(done),
                    None => {
                        top = Some(done);
                        break;
                    }
                }
            }
            Ok(Event::Eof) => break,
            Err(_) => break,
            _ => {}
        }
        buf.clear();
    }
    top
}

// ---------------------------------------------------------------------------
// Wire helpers
// ---------------------------------------------------------------------------

fn esc(s: &str) -> String {
    xml_escape(s)
}

fn el(name: &str, content: impl AsRef<str>) -> String {
    format!("<{name}>{}</{name}>", esc(content.as_ref()))
}

/// Wraps a pre-rendered block (never re-escaped).
fn wrap(name: &str, inner: impl AsRef<str>) -> String {
    format!("<{name}>{}</{name}>", inner.as_ref())
}

fn resp(status: u16, body: impl Into<Vec<u8>>) -> Response {
    Response::xml(status, body.into())
}

fn resp_with(status: u16, headers: Vec<(&str, String)>, body: impl Into<Vec<u8>>) -> Response {
    let mut r = resp(status, body);
    for (k, v) in headers {
        r.headers.push((k.to_string(), v));
    }
    r
}

/// REST-XML error envelope as botocore's rest-xml serializer expects it.
fn xml_error(status: u16, code: &str, message: &str) -> Response {
    let body = wrap("Error", {
        let mut inner = wrap("Type", "Sender");
        inner += &el("Code", code);
        inner += &el("Message", message);
        inner
    });
    resp(
        status,
        format!(
            "<?xml version=\"1.0\" encoding=\"utf-8\"?>\n<ErrorResponse xmlns=\"{XMLNS}\">{body}<RequestId>{}</RequestId></ErrorResponse>\n",
            uuid::Uuid::new_v4()
        ),
    )
}

fn bad_input(code: &str, message: &str) -> Response {
    xml_error(400, code, message)
}

/// Wraps pre-rendered body_xml in `<NameResponse xmlns>` + ResponseMetadata.
fn response_xml(root: &str, inner: &str) -> String {
    format!(
        "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n<{root}Response xmlns=\"{XMLNS}\">{inner}<ResponseMetadata><RequestId>{}</RequestId></ResponseMetadata></{root}Response>",
        uuid::Uuid::new_v4()
    )
}

/// moto's `random_id`: first char from the letter set, rest uppercase alnum.
fn random_id(length: usize) -> String {
    const FIRST: &[u8] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZ";
    const REST: &[u8] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789";
    let bytes = uuid::Uuid::new_v4().as_bytes().to_vec();
    let mut out = String::with_capacity(length);
    out.push(FIRST[bytes[0] as usize % FIRST.len()] as char);
    for i in 1..length {
        let b = bytes[i % bytes.len()];
        out.push(REST[b as usize % REST.len()] as char);
    }
    out
}

/// A 22-character hosted-zone id (moto's `Z1RWWTK7…` shape).
fn new_zone_id() -> String {
    format!("Z{}", random_id(21))
}

/// A `/change/…` id like moto's fake change infos.
fn new_change_id() -> String {
    format!("{CHANGE_PREFIX}{}", random_id(13))
}

fn now_secs_iso() -> String {
    crate::proto::now_iso()
}

/// The first value of a query parameter as an owned string.
fn first(query: &BTreeMap<String, Vec<String>>, key: &str) -> Option<String> {
    query.get(key).and_then(|v| v.first().cloned())
}

/// The first value of a query parameter, dereferenced.
fn first_str<'a>(query: &'a BTreeMap<String, Vec<String>>, key: &str) -> Option<&'a str> {
    query.get(key).and_then(|v| v.first().map(String::as_str))
}

// ---------------------------------------------------------------------------
// State
// ---------------------------------------------------------------------------

#[derive(Clone, Debug)]
struct Vpc {
    vpc_id: String,
    vpc_region: String,
}

impl Vpc {
    fn to_xml(&self) -> String {
        wrap(
            "VPC",
            el("VPCId", &self.vpc_id).clone() + &el("VPCRegion", &self.vpc_region),
        )
    }
}

#[derive(Clone, Debug)]
struct AliasTarget {
    hosted_zone_id: String,
    dns_name: String,
    evaluate_target_health: bool,
}

#[derive(Clone, Debug, Default)]
struct RrSet {
    /// Record name with the trailing dot, as listed on the wire.
    name: String,
    type_: String,
    ttl: Option<i64>,
    records: Option<Vec<String>>,
    set_identifier: Option<String>,
    weight: Option<i64>,
    region: Option<String>,
    health_check_id: Option<String>,
    alias_target: Option<AliasTarget>,
    failover: Option<String>,
    geo_location: Option<BTreeMap<String, String>>,
    multi_value_answer: Option<bool>,
}

impl RrSet {
    /// The reverse-domain sort key moto lists rrsets by.
    fn sort_key(&self) -> (String, String) {
        (reverse_domain(&self.name), self.type_.clone())
    }

    fn to_xml(&self) -> String {
        let mut out = el("Name", &self.name) + &el("Type", &self.type_);
        if let Some(si) = &self.set_identifier {
            out += &el("SetIdentifier", si);
        }
        // moto renders Weight/TTL only when truthy (zero is omitted).
        if self.weight.unwrap_or(0) != 0 {
            out += &el("Weight", self.weight.unwrap().to_string());
        }
        if let Some(region) = &self.region {
            out += &el("Region", region);
        }
        // Alias records have no TTL of their own — Route53 inherits it from
        // the aliased target — so the TTL member is never rendered for them
        // (real AWS behavior; moto 5.2.2 #10029). Even an explicitly supplied
        // TTL is dropped, matching list_resource_record_sets().
        if self.alias_target.is_none() && self.ttl.unwrap_or(0) != 0 {
            out += &el("TTL", self.ttl.unwrap().to_string());
        }
        if let Some(failover) = &self.failover {
            out += &el("Failover", failover);
        }
        if let Some(mva) = self.multi_value_answer {
            out += &el("MultiValueAnswer", bool_text(mva));
        }
        if let Some(geo) = &self.geo_location {
            let mut geo_xml = String::new();
            for key in ["ContinentCode", "CountryCode", "SubdivisionCode"] {
                if let Some(v) = geo.get(key) {
                    geo_xml += &el(key, v);
                }
            }
            out += &wrap("GeoLocation", geo_xml);
        }
        if let Some(alias) = &self.alias_target {
            out += &wrap(
                "AliasTarget",
                el("HostedZoneId", &alias.hosted_zone_id)
                    + &el("DNSName", &alias.dns_name)
                    + &el(
                        "EvaluateTargetHealth",
                        bool_text(alias.evaluate_target_health),
                    ),
            );
        } else if let Some(records) = &self.records {
            let mut rrs = String::new();
            for v in records {
                rrs += &wrap("ResourceRecord", format!("<Value><![CDATA[{v}]]></Value>"));
            }
            out += &wrap("ResourceRecords", rrs);
        }
        if let Some(hc) = &self.health_check_id {
            out += &el("HealthCheckId", hc);
        }
        wrap("ResourceRecordSet", out)
    }
}

#[derive(Clone, Debug)]
struct DelegationSet {
    /// Zone-owned default delegation sets carry an empty id (moto quirk).
    id: String,
    caller_reference: String,
    name_servers: Vec<String>,
}

impl DelegationSet {
    fn to_xml(&self) -> String {
        wrap(
            "DelegationSet",
            el("Id", &self.id)
                + &el("CallerReference", &self.caller_reference)
                + &wrap(
                    "NameServers",
                    self.name_servers
                        .iter()
                        .map(|n| el("NameServer", n))
                        .collect::<String>(),
                ),
        )
    }
}

#[derive(Clone, Debug)]
struct Zone {
    id: String,
    name: String,
    caller_reference: String,
    comment: Option<String>,
    private_zone: bool,
    rrsets: Vec<RrSet>,
    delegation_set: DelegationSet,
    vpcs: Vec<Vpc>,
    dnssec_enabled: bool,
}

impl Zone {
    /// Every entry of `<HostedZone>` shared by create/get/list responses.
    fn base_xml(&self) -> String {
        let comment = match &self.comment {
            Some(c) => el("Comment", c),
            None => String::new(),
        };
        let config = wrap(
            "Config",
            comment + &el("PrivateZone", bool_text(self.private_zone)),
        );
        el("Id", format!("{ZONE_PREFIX}{}", self.id))
            + &el("Name", &self.name)
            + &el("CallerReference", &self.caller_reference)
            + &el("ResourceRecordSetCount", self.rrsets.len().to_string())
            + &config
    }
}

/// `<ChangeInfo>` with a fresh id, INSYNC like moto's fake changes.
fn change_info() -> String {
    wrap(
        "ChangeInfo",
        el("Id", new_change_id()) + &el("Status", "INSYNC") + &el("SubmittedAt", now_secs_iso()),
    )
}

#[derive(Clone, Debug)]
struct HealthCheck {
    id: String,
    caller_reference: String,
    type_: String,
    ip_address: Option<String>,
    port: Option<i64>,
    resource_path: Option<String>,
    fqdn: Option<String>,
    search_string: Option<String>,
    request_interval: Option<i64>,
    failure_threshold: Option<i64>,
    health_threshold: Option<i64>,
    measure_latency: bool,
    inverted: bool,
    disabled: bool,
    enable_sni: bool,
    children: Vec<String>,
    regions: Vec<String>,
    version: i64,
}

impl HealthCheck {
    fn to_xml(&self) -> String {
        // Mirrors moto's `health_check_config` dict: CALCULATED checks carry
        // the health threshold, everything else the endpoint probe fields.
        let mut cfg = el("Type", &self.type_)
            + &opt_el("ResourcePath", self.resource_path.as_deref())
            + &opt_el("FullyQualifiedDomainName", self.fqdn.as_deref())
            + &opt_el("SearchString", self.search_string.as_deref())
            + &el("Inverted", bool_text(self.inverted))
            + &el("Disabled", bool_text(self.disabled))
            + &wrap(
                "ChildHealthChecks",
                self.children
                    .iter()
                    .map(|c| el("ChildHealthCheck", c))
                    .collect::<String>(),
            )
            + &el("EnableSNI", bool_text(self.enable_sni))
            + &wrap(
                "Regions",
                self.regions
                    .iter()
                    .map(|r| el("Region", r))
                    .collect::<String>(),
            );
        if self.type_ == "CALCULATED" {
            cfg += &el(
                "HealthThreshold",
                self.health_threshold.unwrap_or(0).to_string(),
            );
        } else {
            cfg += &opt_el("IPAddress", self.ip_address.as_deref());
            cfg += &opt_el("Port", self.port.map(|v| v.to_string()).as_deref());
            cfg += &opt_el(
                "RequestInterval",
                self.request_interval.map(|v| v.to_string()).as_deref(),
            );
            cfg += &opt_el(
                "FailureThreshold",
                self.failure_threshold.map(|v| v.to_string()).as_deref(),
            );
            cfg += &el("MeasureLatency", bool_text(self.measure_latency));
        }
        wrap(
            "HealthCheck",
            el("Id", &self.id)
                + &el("CallerReference", &self.caller_reference)
                + &wrap("HealthCheckConfig", cfg)
                + &el("HealthCheckVersion", self.version.to_string()),
        )
    }
}

#[derive(Clone, Debug)]
struct QueryLoggingConfig {
    id: String,
    hosted_zone_id: String,
    cloudwatch_logs_log_group_arn: String,
}

impl QueryLoggingConfig {
    fn to_xml(&self) -> String {
        wrap(
            "QueryLoggingConfig",
            el("Id", &self.id)
                + &el("HostedZoneId", &self.hosted_zone_id)
                + &el(
                    "CloudWatchLogsLogGroupArn",
                    &self.cloudwatch_logs_log_group_arn,
                ),
        )
    }
}

#[derive(Clone, Debug)]
struct CidrCollection {
    id: String,
    name: String,
    version: i64,
    arn: String,
    /// location name → CIDR blocks
    locations: BTreeMap<String, Vec<String>>,
}

#[derive(Clone, Debug)]
struct TrafficPolicy {
    id: String,
    name: String,
    /// version → (document, comment)
    versions: BTreeMap<i64, (String, String)>,
}

impl TrafficPolicy {
    fn version_xml(&self, version: i64) -> String {
        let Some((document, comment)) = self.versions.get(&version) else {
            return String::new();
        };
        // The old provider always rendered the comment element, even empty.
        let fields = el("Id", &self.id)
            + &el("Name", &self.name)
            + &el("Version", version.to_string())
            + &el("Type", "A")
            + &wrap(
                "Document",
                // The traffic-policy document is JSON text; render it verbatim
                // (no XML escaping) like the old provider did.
                document.clone(),
            )
            + &el("Comment", comment);
        wrap("TrafficPolicy", fields)
    }
}

#[derive(Clone, Debug)]
struct TrafficPolicyInstance {
    id: String,
    hosted_zone_id: String,
    name: String,
    ttl: i64,
    traffic_policy_id: String,
    traffic_policy_version: i64,
    traffic_policy_type: String,
    state: String,
    message: String,
}

impl TrafficPolicyInstance {
    fn to_xml(&self) -> String {
        wrap(
            "TrafficPolicyInstance",
            el("Id", &self.id)
                + &el("HostedZoneId", &self.hosted_zone_id)
                + &el("Name", &self.name)
                + &el("TTL", self.ttl.to_string())
                + &el("TrafficPolicyId", &self.traffic_policy_id)
                + &el(
                    "TrafficPolicyVersion",
                    self.traffic_policy_version.to_string(),
                )
                + &el("TrafficPolicyType", &self.traffic_policy_type)
                + &el("State", &self.state)
                + &el("Message", &self.message),
        )
    }
}

#[derive(Clone, Debug)]
struct KeySigningKey {
    name: String,
    status: String,
    created_date: String,
}

impl KeySigningKey {
    fn to_xml(&self) -> String {
        wrap(
            "KeySigningKey",
            el("Name", &self.name)
                + &el("Flag", 257.to_string())
                + &el("SigningAlgorithmMnemonic", "ECDSAP256SHA256")
                + &el("SigningAlgorithmType", 13.to_string())
                + &el("DigestAlgorithmMnemonic", "SHA-256")
                + &el("DigestAlgorithmType", 2.to_string())
                + &el("KeyTag", 1.to_string())
                + &el("DigestValue", "abc123")
                + &el("PublicKey", "publickey==")
                + &el("DSRecord", "example DS record")
                + &el("DNSKEYRecord", "example DNSKEY record")
                + &el("Status", &self.status)
                + &el("StatusMessage", "")
                + &el("CreatedDate", self.created_date.as_str())
                + &el("LastModifiedDate", now_secs_iso()),
        )
    }
}

#[derive(Default)]
struct Backend {
    zones: Vec<Zone>,
    health_checks: Vec<HealthCheck>,
    /// resource id → tags (mirrors moto's TaggingService)
    resource_tags: BTreeMap<String, BTreeMap<String, String>>,
    query_logging_configs: HashMap<String, QueryLoggingConfig>,
    delegation_sets: Vec<DelegationSet>,
    cidr_collections: Vec<CidrCollection>,
    /// zone id → authorized VPCs
    vpc_auths: BTreeMap<String, Vec<Vpc>>,
    traffic_policies: Vec<TrafficPolicy>,
    traffic_instances: Vec<TrafficPolicyInstance>,
    /// (zone id, ksk name) → key signing key
    key_signing_keys: BTreeMap<(String, String), KeySigningKey>,
}

fn with_state<T>(account: u64, f: impl FnOnce(&mut Backend) -> T) -> T {
    let slot = store::slot::<Backend>("route53");
    let mut map = slot.write();
    f(map.entry((account, GLOBAL.to_string())).or_default())
}

// ---------------------------------------------------------------------------
// Small value helpers
// ---------------------------------------------------------------------------

fn bool_text(b: bool) -> &'static str {
    if b {
        "true"
    } else {
        "false"
    }
}

/// Renders `<Elem>value</Elem>` only when `value` is `Some` (and non-empty).
fn opt_el(name: &str, value: Option<&str>) -> String {
    match value {
        Some(v) if !v.is_empty() => el(name, v),
        _ => String::new(),
    }
}

/// Route53's reverse-domain sorting key (`www.example.com.` → `com.example.www`).
fn reverse_domain(name: &str) -> String {
    let cleaned = name.strip_suffix('.').unwrap_or(name);
    let mut parts: Vec<&str> = cleaned.split('.').collect();
    parts.reverse();
    parts.join(".")
}

/// Accepts bare zone ids and `/hostedzone/…` prefixed ones.
fn zone_ref(id: &str) -> String {
    id.trim()
        .strip_prefix(ZONE_PREFIX)
        .unwrap_or(id)
        .to_string()
}

/// Record names compared without the trailing dot.
fn bare_name(name: &str) -> String {
    name.trim().strip_suffix('.').unwrap_or(name).to_string()
}

/// Default AWS 4 nameserver set (moto's DelegationSet defaults).
fn default_name_servers() -> Vec<String> {
    vec![
        "ns-2048.awsdns-64.com".to_string(),
        "ns-2049.awsdns-65.net".to_string(),
        "ns-2050.awsdns-66.org".to_string(),
        "ns-2051.awsdns-67.co.uk".to_string(),
    ]
}

fn non_empty(v: Option<&str>) -> Option<String> {
    v.filter(|s| !s.is_empty()).map(String::from)
}

fn no_such_zone(id: &str) -> Response {
    bad_input(
        "NoSuchHostedZone",
        &format!("No hosted zone found with ID: {id}"),
    )
}

fn no_such_health_check(id: &str) -> Response {
    bad_input(
        "NoSuchHealthCheck",
        &format!("A health check with id {id} does not exist."),
    )
}

fn no_such_delegation_set(id: &str) -> Response {
    xml_error(404, "NoSuchDelegationSet", id)
}

fn no_such_query_logging_config() -> Response {
    bad_input(
        "NoSuchQueryLoggingConfig",
        "The query logging configuration does not exist",
    )
}

fn no_such_traffic_policy(id: &str) -> Response {
    xml_error(
        404,
        "NoSuchTrafficPolicy",
        &format!("No traffic policy found with ID {id}"),
    )
}

fn no_such_traffic_policy_instance(id: &str) -> Response {
    xml_error(
        404,
        "NoSuchTrafficPolicyInstance",
        &format!("No traffic policy instance found with ID {id}"),
    )
}

fn no_such_cidr_collection(id: &str) -> Response {
    xml_error(
        404,
        "NoSuchCidrCollectionException",
        &format!("The CIDR collection with ID {id} does not exist."),
    )
}

// ---------------------------------------------------------------------------
// Wire entry
// ---------------------------------------------------------------------------

/// Wire entry point — routes by method + path segments (rest-xml).
pub fn dispatch(req: &Request) -> Response {
    let mut segs: Vec<String> = req
        .path
        .split('/')
        .filter(|s| !s.is_empty())
        .map(url_decode)
        .collect();
    match segs.first() {
        Some(v) if v == "2013-04-01" => {
            segs.remove(0);
        }
        _ => return unhandled(req),
    }
    let segs: Vec<&str> = segs.iter().map(String::as_str).collect();
    let query = query_map(&req.query_string);
    with_state(req.account, |backend| route(req, backend, &segs, &query))
}

fn unhandled(_req: &Request) -> Response {
    xml_error(501, "NotImplemented", "Operation is not implemented")
}

// ---------------------------------------------------------------------------
// Routing
// ---------------------------------------------------------------------------

fn route(
    req: &Request,
    backend: &mut Backend,
    segs: &[&str],
    query: &BTreeMap<String, Vec<String>>,
) -> Response {
    let method = req.method.as_str();
    match (method, segs) {
        // ---------------------------------------------------- hosted zones
        ("POST", ["hostedzone"]) => h_create_hosted_zone(req, backend),
        ("GET", ["hostedzone"]) => h_list_hosted_zones(backend, query),
        ("GET" | "POST", ["hostedzone", id]) => {
            if method == "POST" {
                h_update_hosted_zone_comment(req, backend, id)
            } else {
                h_get_hosted_zone(backend, id)
            }
        }
        ("DELETE", ["hostedzone", id]) => h_delete_hosted_zone(backend, id),
        ("GET", ["hostedzone", id, "rrset"]) => h_list_rrsets(backend, id, query),
        ("POST", ["hostedzone", id, "rrset"]) => h_change_rrsets(req, backend, id),
        ("GET", ["hostedzone", id, "dnssec"]) => h_get_dnssec(backend, id),
        ("POST", ["hostedzone", id, "associatevpc"]) => h_associate_vpc(req, backend, id),
        ("POST", ["hostedzone", id, "disassociatevpc"]) => h_disassociate_vpc(req, backend, id),
        ("POST", ["hostedzone", id, "authorizevpcassociation"]) => {
            h_create_vpc_auth(req, backend, id)
        }
        ("GET", ["hostedzone", id, "authorizevpcassociation"]) => h_list_vpc_auths(backend, id),
        ("POST", ["hostedzone", id, "deauthorizevpcassociation"]) => {
            h_delete_vpc_auth(req, backend, id)
        }
        ("POST", ["hostedzone", id, "enable-dnssec"]) => h_dot_dnssec(backend, id, true),
        ("POST", ["hostedzone", id, "disable-dnssec"]) => h_dot_dnssec(backend, id, false),
        ("POST", ["hostedzone", id, "features"]) => h_update_hosted_zone_features(backend, id),
        ("GET", ["hostedzonesbyname"]) => h_list_hosted_zones_by_name(backend, query),
        ("GET", ["hostedzonesbyvpc"]) => h_list_hosted_zones_by_vpc(req, backend, query),
        ("GET", ["hostedzonecount"]) => h_get_hosted_zone_count(backend),
        // ---------------------------------------------------- health checks
        ("POST", ["healthcheck"]) => h_create_health_check(req, backend),
        ("GET", ["healthcheck"]) => h_list_health_checks(backend),
        ("GET" | "POST", ["healthcheck", id]) => {
            if method == "POST" {
                h_update_health_check(req, backend, id)
            } else {
                h_get_health_check(backend, id)
            }
        }
        ("DELETE", ["healthcheck", id]) => h_delete_health_check(backend, id),
        ("GET", ["healthcheck", id, "status"]) => h_get_health_check_status(backend, id),
        ("GET", ["healthcheck", id, "lastfailurereason"]) => {
            h_get_health_check_last_failure(backend, id)
        }
        ("GET", ["healthcheckcount"]) => h_get_health_check_count(backend),
        // ---------------------------------------------------- tags
        ("POST", ["tags", resource_type]) => h_list_tags_for_resources(req, backend, resource_type),
        ("GET" | "POST", ["tags", resource_type, id]) => {
            if method == "POST" {
                h_change_tags_for_resource(req, backend, resource_type, id)
            } else {
                h_list_tags_for_resource(backend, resource_type, id)
            }
        }
        // ---------------------------------------------------- delegation sets
        ("POST", ["delegationset"]) => h_create_reusable_delegation_set(req, backend),
        ("GET", ["delegationset"]) => h_list_reusable_delegation_sets(backend),
        ("GET", ["delegationset", id]) => h_get_reusable_delegation_set(backend, id),
        ("DELETE", ["delegationset", id]) => h_delete_reusable_delegation_set(backend, id),
        // ---------------------------------------------------- limits
        ("GET", ["accountlimit", limit_type]) => h_get_account_limit(backend, limit_type),
        ("GET", ["hostedzonelimit", id, limit_type]) => {
            h_get_hosted_zone_limit(backend, id, limit_type)
        }
        ("GET", ["reusabledelegationsetlimit", id, limit_type]) => {
            h_get_reusable_delegation_set_limit(backend, id, limit_type)
        }
        // ---------------------------------------------------- changes
        ("GET", ["change", id]) => h_get_change(id),
        // ---------------------------------------------------- query logging
        ("POST", ["queryloggingconfig"]) => h_create_query_logging_config(req, backend),
        ("GET", ["queryloggingconfig"]) => h_list_query_logging_configs(backend, query),
        ("GET", ["queryloggingconfig", id]) => h_get_query_logging_config(backend, id),
        ("DELETE", ["queryloggingconfig", id]) => h_delete_query_logging_config(backend, id),
        // ---------------------------------------------------- CIDR collections
        ("POST", ["cidrcollection"]) => h_create_cidr_collection(req, backend),
        ("GET", ["cidrcollection"]) => h_list_cidr_collections(backend),
        ("POST", ["cidrcollection", id]) => h_change_cidr_collection(req, backend, id),
        ("GET", ["cidrcollection", id]) => h_list_cidr_locations(backend, id),
        ("DELETE", ["cidrcollection", id]) => h_delete_cidr_collection(backend, id),
        ("GET", ["cidrcollection", id, "cidrblocks"]) => h_list_cidr_blocks(backend, id, query),
        // ---------------------------------------------------- key signing keys
        ("POST", ["keysigningkey"]) => h_create_key_signing_key(req, backend),
        ("POST", ["keysigningkey", zone_id, ksk_name, "activate"]) => {
            h_activate_key_signing_key(backend, zone_id, ksk_name, true)
        }
        ("POST", ["keysigningkey", zone_id, ksk_name, "deactivate"]) => {
            h_activate_key_signing_key(backend, zone_id, ksk_name, false)
        }
        ("DELETE", ["keysigningkey", zone_id, ksk_name]) => {
            h_delete_key_signing_key(backend, zone_id, ksk_name)
        }
        // ---------------------------------------------------- traffic policies
        ("POST", ["trafficpolicy"]) => h_create_traffic_policy(req, backend),
        ("GET", ["trafficpolicies"]) => h_list_traffic_policies(backend),
        ("POST", ["trafficpolicy", id]) => h_create_traffic_policy_version(req, backend, id),
        ("GET", ["trafficpolicies", id, "versions"]) => h_list_traffic_policy_versions(backend, id),
        ("GET", ["trafficpolicy", id, version]) => h_get_traffic_policy(backend, id, version),
        ("POST", ["trafficpolicy", id, version]) => {
            h_update_traffic_policy_comment(req, backend, id, version)
        }
        ("DELETE", ["trafficpolicy", id, version]) => h_delete_traffic_policy(backend, id, version),
        // ---------------------------------------------------- traffic instances
        ("POST", ["trafficpolicyinstance"]) => h_create_traffic_policy_instance(req, backend),
        ("GET", ["trafficpolicyinstances"]) => h_list_traffic_policy_instances(backend),
        ("GET", ["trafficpolicyinstances", "hostedzone"]) => {
            h_list_traffic_policy_instances_by_zone(backend, query)
        }
        ("GET", ["trafficpolicyinstances", "trafficpolicy"]) => {
            h_list_traffic_policy_instances_by_policy(backend, query)
        }
        ("GET", ["trafficpolicyinstancecount"]) => h_get_traffic_policy_instance_count(backend),
        ("GET" | "POST", ["trafficpolicyinstance", id]) => {
            if method == "POST" {
                h_update_traffic_policy_instance(req, backend, id)
            } else {
                h_get_traffic_policy_instance(backend, id)
            }
        }
        ("DELETE", ["trafficpolicyinstance", id]) => h_delete_traffic_policy_instance(backend, id),
        // ---------------------------------------------------- misc probes
        ("GET", ["testdnsanswer"]) => h_test_dns_answer(backend, query),
        ("GET", ["checkeripranges"]) => h_get_checker_ip_ranges(),
        ("GET", ["geolocation"]) => h_get_geo_location(query),
        ("GET", ["geolocations"]) => h_list_geo_locations(),
        _ => unhandled(req),
    }
}

// ---------------------------------------------------------------------------
// Hosted zones
// ---------------------------------------------------------------------------

#[allow(clippy::too_many_arguments)]
fn create_zone(
    backend: &mut Backend,
    name: String,
    caller_reference: String,
    comment: Option<String>,
    private_zone: bool,
    vpc: Option<Vpc>,
    delegation_set_id: Option<String>,
) -> Zone {
    // moto reuses a named delegation set's servers when the zone names one;
    // zone-owned default sets carry an empty id with the default nameservers.
    let delegation_set = match delegation_set_id {
        Some(id) => {
            let servers = backend
                .delegation_sets
                .iter()
                .find(|d| d.id == id)
                .map(|d| d.name_servers.clone())
                .unwrap_or_default();
            DelegationSet {
                id,
                caller_reference: format!("DelSet_{name}"),
                name_servers: if servers.is_empty() {
                    default_name_servers()
                } else {
                    servers
                },
            }
        }
        None => DelegationSet {
            id: String::new(),
            caller_reference: format!("DelSet_{name}"),
            name_servers: default_name_servers(),
        },
    };
    // Unique 22-character Z id (moto's HostedZoneIdentifier.generate).
    let id = loop {
        let candidate = new_zone_id();
        if !backend.zones.iter().any(|z| z.id == candidate) {
            break candidate;
        }
    };
    let ns_servers = delegation_set.name_servers.clone();
    // Public zones auto-create NS + SOA records pointing at the nameservers.
    let zone = Zone {
        id,
        name: name.clone(),
        caller_reference,
        comment,
        private_zone,
        rrsets: vec![
            RrSet {
                name: name.clone(),
                type_: "NS".to_string(),
                ttl: Some(172800),
                records: Some(ns_servers.clone()),
                ..RrSet::default()
            },
            RrSet {
                name: name.clone(),
                type_: "SOA".to_string(),
                ttl: Some(900),
                records: Some(vec![format!(
                    "{}. hostmaster.example.com. 1 7200 900 1209600 86400",
                    ns_servers[0]
                )]),
                ..RrSet::default()
            },
        ],
        delegation_set,
        vpcs: vpc.map(|v| vec![v]).unwrap_or_default(),
        dnssec_enabled: false,
    };
    backend.zones.push(zone.clone());
    zone
}

fn h_create_hosted_zone(req: &Request, backend: &mut Backend) -> Response {
    let Some(body) = parse_xml(req.body.as_ref()) else {
        return bad_input("InvalidInput", "Missing request body");
    };
    let mut name = body.text_or("Name", "");
    if !name.ends_with('.') {
        name.push('.');
    }
    let comment = body
        .sub(&["HostedZoneConfig"])
        .and_then(|c| c.text_of("Comment"))
        .filter(|s| !s.is_empty())
        .map(String::from);
    let private_zone = body
        .sub(&["HostedZoneConfig"])
        .and_then(|c| c.text_of("PrivateZone"))
        .map(|v| v == "true")
        .unwrap_or(false);
    // VPC is honored only for private zones (moto only reads it then).
    let vpc = if private_zone {
        body.sub(&["VPC"]).map(|v| Vpc {
            vpc_id: v.text_or("VPCId", ""),
            vpc_region: v.text_or("VPCRegion", "us-east-1"),
        })
    } else {
        None
    };
    let delegation_set_id = body.text_of("DelegationSetId").map(String::from);
    let zone = create_zone(
        backend,
        name,
        body.text_or("CallerReference", ""),
        comment,
        private_zone,
        vpc,
        delegation_set_id,
    );
    let location = format!(
        "https://route53.amazonaws.com/2013-04-01/hostedzone/{}",
        zone.id
    );
    let mut inner = String::new();
    if zone.private_zone && !zone.vpcs.is_empty() {
        inner += &zone.vpcs[0].to_xml();
    }
    inner += &wrap("HostedZone", zone.base_xml());
    if !zone.private_zone {
        inner += &zone.delegation_set.to_xml();
    }
    inner += &change_info();
    resp_with(
        201,
        vec![("Location", location)],
        response_xml("CreateHostedZone", &inner),
    )
}

fn h_list_hosted_zones(backend: &mut Backend, query: &BTreeMap<String, Vec<String>>) -> Response {
    let max_items = first_str(query, "maxitems")
        .and_then(|v| v.parse::<usize>().ok())
        .unwrap_or(100);
    let marker = first(query, "marker").unwrap_or_default();
    let all = &backend.zones;
    let start = all.iter().position(|z| z.id == marker).map_or(0, |p| p + 1);
    let page: Vec<&Zone> = all.iter().skip(start).take(max_items).collect();
    let is_truncated = start + page.len() < all.len();
    let next_marker = page.last().map(|z| z.id.clone()).unwrap_or_default();
    let zones_xml = page
        .iter()
        .map(|z| wrap("HostedZone", z.base_xml()))
        .collect::<String>();
    let mut inner = wrap("HostedZones", zones_xml)
        + &el("Marker", &marker)
        + &el("MaxItems", max_items.to_string())
        + &el("IsTruncated", bool_text(is_truncated));
    if is_truncated {
        inner += &el("NextMarker", &next_marker);
    }
    resp(200, response_xml("ListHostedZones", &inner))
}

fn h_list_hosted_zones_by_name(
    backend: &mut Backend,
    query: &BTreeMap<String, Vec<String>>,
) -> Response {
    let dnsname = first(query, "dnsname");
    let mut zones: Vec<&Zone> = backend.zones.iter().collect();
    match &dnsname {
        Some(dn) => {
            let target = format!("{dn}.");
            zones.retain(|z| z.name == target);
        }
        None => {
            // Sort by domain components reversed (boto3's documented order).
            zones.sort_by_key(|z| reverse_domain(&z.name));
        }
    }
    let zones_xml = zones
        .iter()
        .map(|z| wrap("HostedZone", z.base_xml()))
        .collect::<String>();
    let inner = opt_el("DNSName", dnsname.as_deref())
        + &wrap("HostedZones", zones_xml)
        + &el("IsTruncated", "false")
        + &el("MaxItems", "100");
    resp(200, response_xml("ListHostedZonesByName", &inner))
}

fn h_list_hosted_zones_by_vpc(
    req: &Request,
    backend: &mut Backend,
    query: &BTreeMap<String, Vec<String>>,
) -> Response {
    let vpc_id = first_str(query, "vpcid").unwrap_or("").to_string();
    let summaries = backend
        .zones
        .iter()
        .filter(|z| z.private_zone)
        .filter(|z| z.vpcs.iter().any(|v| v.vpc_id == vpc_id))
        .map(|z| {
            wrap(
                "HostedZoneSummary",
                el("HostedZoneId", &z.id)
                    + &el("Name", &z.name)
                    + &wrap("Owner", el("OwningAccount", req.account.to_string())),
            )
        })
        .collect::<String>();
    let inner = wrap("HostedZoneSummaries", summaries) + &el("MaxItems", "100");
    resp(200, response_xml("ListHostedZonesByVPC", &inner))
}

fn h_get_hosted_zone_count(backend: &mut Backend) -> Response {
    let inner = el("HostedZoneCount", backend.zones.len().to_string());
    resp(200, response_xml("GetHostedZoneCount", &inner))
}

fn h_get_hosted_zone(backend: &mut Backend, id: &str) -> Response {
    let Some(zone) = backend.zones.iter().find(|z| z.id == zone_ref(id)).cloned() else {
        return no_such_zone(id);
    };
    let mut inner = wrap("HostedZone", zone.base_xml());
    if zone.private_zone {
        inner += &wrap(
            "VPCs",
            zone.vpcs.iter().map(|v| v.to_xml()).collect::<String>(),
        );
    } else {
        inner += &zone.delegation_set.to_xml();
    }
    resp(200, response_xml("GetHostedZone", &inner))
}

fn find_zone_mut<'a>(backend: &'a mut Backend, id: &str) -> Result<&'a mut Zone, Response> {
    let wanted = zone_ref(id);
    match backend.zones.iter_mut().find(|z| z.id == wanted) {
        Some(z) => Ok(z),
        None => Err(no_such_zone(id)),
    }
}

fn h_delete_hosted_zone(backend: &mut Backend, id: &str) -> Response {
    let zone = match find_zone_mut(backend, id) {
        Ok(z) => z,
        Err(resp) => return resp,
    };
    if zone
        .rrsets
        .iter()
        .any(|r| r.type_ != "NS" && r.type_ != "SOA")
    {
        return bad_input(
            "HostedZoneNotEmpty",
            "The hosted zone contains resource records that are not SOA or NS records.",
        );
    }
    let wanted = zone_ref(id);
    backend.zones.retain(|z| z.id != wanted);
    resp(200, response_xml("DeleteHostedZone", &change_info()))
}

fn h_update_hosted_zone_comment(req: &Request, backend: &mut Backend, id: &str) -> Response {
    let comment = parse_xml(req.body.as_ref())
        .and_then(|b| b.text_of("Comment").map(String::from))
        .filter(|c| !c.is_empty());
    let zone = match find_zone_mut(backend, id) {
        Ok(z) => z,
        Err(resp) => return resp,
    };
    zone.comment = comment;
    let zone_xml = zone.base_xml();
    resp(
        200,
        response_xml("UpdateHostedZoneComment", &wrap("HostedZone", zone_xml)),
    )
}

// ---------------------------------------------------------------------------
// Resource record sets
// ---------------------------------------------------------------------------

fn h_list_rrsets(
    backend: &mut Backend,
    id: &str,
    query: &BTreeMap<String, Vec<String>>,
) -> Response {
    let start_name = first(query, "name");
    let start_type = first(query, "type");
    if start_type.is_some() && start_name.is_none() {
        return bad_input("InvalidInput", "The input is not valid");
    }
    let max_items = first_str(query, "maxitems")
        .and_then(|v| v.parse::<usize>().ok())
        .unwrap_or(300);
    let zone = match find_zone_mut(backend, id) {
        Ok(z) => z,
        Err(resp) => return resp,
    };
    let mut sorted: Vec<&RrSet> = zone.rrsets.iter().collect();
    sorted.sort_by_key(|r| r.sort_key());
    if let Some(start_name) = &start_name {
        let start_type = start_type.as_deref().unwrap_or("");
        let srev = reverse_domain(start_name);
        // moto: drop while rrset sorts strictly before the start marker.
        while let Some(first) = sorted.first() {
            let rrev = reverse_domain(&first.name);
            if rrev < srev || (rrev == srev && first.type_.as_str() < start_type) {
                sorted.remove(0);
            } else {
                break;
            }
        }
    }
    let page: Vec<&&RrSet> = sorted.iter().take(max_items).collect();
    let is_truncated = sorted.len() > max_items;
    let records_xml = page.iter().map(|r| r.to_xml()).collect::<String>();
    let next: Option<&&RrSet> = if is_truncated {
        sorted.get(max_items)
    } else {
        None
    };
    let inner = wrap("ResourceRecordSets", records_xml)
        + &opt_el("NextRecordName", next.map(|r| r.name.as_str()))
        + &opt_el("NextRecordType", next.map(|r| r.type_.as_str()))
        + &el("MaxItems", max_items.to_string())
        + &el("IsTruncated", bool_text(is_truncated));
    resp(200, response_xml("ListResourceRecordSets", &inner))
}

/// Parses one `<Change>` element into an (action, record) pair.
fn parse_change(change: &El) -> Result<(String, RrSet), Response> {
    let action = change.text_or("Action", "").to_string();
    let Some(rr) = change.child("ResourceRecordSet") else {
        return Ok((
            action,
            RrSet {
                type_: String::new(),
                ..RrSet::default()
            },
        ));
    };
    let records = rr
        .sub(&["ResourceRecords"])
        .map(|rrs| {
            rrs.components("ResourceRecord")
                .filter_map(|r| r.text_of("Value").map(String::from))
                .filter(|v| !v.is_empty())
                .collect::<Vec<String>>()
        })
        .filter(|records| !records.is_empty());
    let rr = RrSet {
        name: rr.text_or("Name", ""),
        type_: rr.text_or("Type", ""),
        ttl: rr.int_of("TTL"),
        records,
        set_identifier: non_empty(rr.text_of("SetIdentifier")),
        weight: rr.int_of("Weight"),
        region: non_empty(rr.text_of("Region")),
        health_check_id: non_empty(rr.text_of("HealthCheckId")),
        alias_target: rr.sub(&["AliasTarget"]).map(|a| AliasTarget {
            hosted_zone_id: a.text_or("HostedZoneId", ""),
            dns_name: a.text_or("DNSName", ""),
            evaluate_target_health: a
                .text_of("EvaluateTargetHealth")
                .map(|v| v == "true")
                .unwrap_or(false),
        }),
        failover: non_empty(rr.text_of("Failover")),
        geo_location: rr.sub(&["GeoLocation"]).map(|g| {
            g.children
                .iter()
                .map(|c| (c.name.clone(), c.as_str().to_string()))
                .collect::<BTreeMap<String, String>>()
        }),
        multi_value_answer: rr.text_of("MultiValueAnswer").map(|v| v == "true"),
    };
    Ok((action, rr))
}

/// Applies one CRUD action to a zone's rrsets (moto's change semantics).
fn apply_rrset_change(zone: &mut Zone, action: &str, rr: &RrSet) -> Result<(), Response> {
    match action {
        "CREATE" | "UPSERT" => {
            let new_name = if rr.name.ends_with('.') {
                rr.name.clone()
            } else {
                format!("{}.", rr.name)
            };
            let mut stored = rr.clone();
            stored.name = new_name.clone();
            if action == "CREATE"
                && zone.rrsets.iter().any(|r| {
                    r.name == new_name
                        && r.type_ == rr.type_
                        && r.set_identifier == rr.set_identifier
                })
            {
                return Err(bad_input(
                    "InvalidChangeBatch",
                    &format!(
                        "Tried to create resource record set [name='{new_name}', type='{}'] but it already exists",
                        rr.type_
                    ),
                ));
            }
            zone.rrsets.retain(|r| {
                !(r.name == new_name
                    && r.type_ == rr.type_
                    && r.set_identifier == rr.set_identifier)
            });
            zone.rrsets.push(stored);
        }
        "DELETE" => {
            let wanted = bare_name(&rr.name);
            match &rr.set_identifier {
                Some(si) => zone
                    .rrsets
                    .retain(|r| r.set_identifier.as_deref() != Some(si)),
                None => zone.rrsets.retain(|r| {
                    !(bare_name(&r.name) == wanted && (rr.type_.is_empty() || r.type_ == rr.type_))
                }),
            }
        }
        other => {
            return Err(bad_input(
                "InvalidInput",
                &format!(
                    "Invalid XML ; cvc-enumeration-valid: Value '{other}' is not facet-valid with respect to enumeration '[CREATE, DELETE, UPSERT]'. It must be a value from the enumeration."
                ),
            ));
        }
    }
    Ok(())
}

fn h_change_rrsets(req: &Request, backend: &mut Backend, id: &str) -> Response {
    let Some(body) = parse_xml(req.body.as_ref()) else {
        return bad_input("InvalidInput", "Missing request body");
    };
    let mut changes = Vec::new();
    for change in body.els_at(&["ChangeBatch", "Changes"], "Change") {
        match parse_change(change) {
            Ok(change) => changes.push(change),
            Err(resp) => return resp,
        }
    }
    let zone = match find_zone_mut(backend, id) {
        Ok(z) => z,
        Err(resp) => return resp,
    };
    for (action, rr) in &changes {
        if let Err(resp) = apply_rrset_change(zone, action, rr) {
            return resp;
        }
    }
    resp(
        200,
        response_xml("ChangeResourceRecordSets", &change_info()),
    )
}

// ---------------------------------------------------------------------------
// VPC association
// ---------------------------------------------------------------------------

fn h_associate_vpc(req: &Request, backend: &mut Backend, id: &str) -> Response {
    let Some(body) = parse_xml(req.body.as_ref()) else {
        return bad_input("InvalidInput", "Missing request body");
    };
    let vpc_id = body
        .sub(&["VPC"])
        .map(|v| v.text_or("VPCId", ""))
        .unwrap_or_default();
    let zone = match find_zone_mut(backend, id) {
        Ok(z) => z,
        Err(resp) => return resp,
    };
    if !zone.private_zone {
        return bad_input(
            "PublicZoneVPCAssociation",
            "You're trying to associate a VPC with a public hosted zone. Amazon Route 53 doesn't support associating a VPC with a public hosted zone.",
        );
    }
    zone.vpcs.push(Vpc {
        vpc_id,
        vpc_region: "us-east-1".to_string(),
    });
    resp(
        200,
        response_xml("AssociateVPCWithHostedZone", &change_info()),
    )
}

fn h_disassociate_vpc(req: &Request, backend: &mut Backend, id: &str) -> Response {
    let vpc_id = match parse_xml(req.body.as_ref()) {
        Some(body) => body
            .sub(&["VPC"])
            .map_or(String::new(), |v| v.text_or("VPCId", "")),
        None => String::new(),
    };
    let zone = match find_zone_mut(backend, id) {
        Ok(z) => z,
        Err(resp) => return resp,
    };
    if zone.vpcs.len() <= 1 {
        return bad_input(
            "LastVPCAssociation",
            "The VPC that you're trying to disassociate from the private hosted zone is the last VPC that is associated with the hosted zone. Amazon Route 53 doesn't support disassociating the last VPC from a hosted zone.",
        );
    }
    zone.vpcs.retain(|v| v.vpc_id != vpc_id);
    resp(
        200,
        response_xml("DisassociateVPCFromHostedZone", &change_info()),
    )
}

// ---------------------------------------------------------------------------
// VPC association authorizations
// ---------------------------------------------------------------------------

fn parsed_vpc(body: &El) -> Vpc {
    body.sub(&["VPC"])
        .map(|v| Vpc {
            vpc_id: v.text_or("VPCId", ""),
            vpc_region: v.text_or("VPCRegion", "us-east-1"),
        })
        .unwrap_or(Vpc {
            vpc_id: String::new(),
            vpc_region: "us-east-1".to_string(),
        })
}

fn h_create_vpc_auth(req: &Request, backend: &mut Backend, id: &str) -> Response {
    let Some(body) = parse_xml(req.body.as_ref()) else {
        return bad_input("InvalidInput", "Missing request body");
    };
    let vpc = parsed_vpc(&body);
    let want = zone_ref(id);
    match find_zone_mut(backend, &want) {
        Ok(_) => {}
        Err(resp) => return resp,
    }
    backend
        .vpc_auths
        .entry(want.clone())
        .or_default()
        .push(vpc.clone());
    let inner = el("HostedZoneId", &want) + &vpc.to_xml();
    resp(
        200,
        response_xml("CreateVPCAssociationAuthorization", &inner),
    )
}

fn h_list_vpc_auths(backend: &mut Backend, id: &str) -> Response {
    let want = zone_ref(id);
    match find_zone_mut(backend, &want) {
        Ok(_) => {}
        Err(resp) => return resp,
    }
    let auths = backend.vpc_auths.get(&want).cloned().unwrap_or_default();
    let inner = el("HostedZoneId", &want)
        + &wrap("VPCs", auths.iter().map(|v| v.to_xml()).collect::<String>());
    resp(
        200,
        response_xml("ListVPCAssociationAuthorizations", &inner),
    )
}

fn h_delete_vpc_auth(req: &Request, backend: &mut Backend, id: &str) -> Response {
    let Some(body) = parse_xml(req.body.as_ref()) else {
        return bad_input("InvalidInput", "Missing request body");
    };
    let vpc = parsed_vpc(&body);
    let want = zone_ref(id);
    match find_zone_mut(backend, &want) {
        Ok(_) => {}
        Err(resp) => return resp,
    }
    if let Some(list) = backend.vpc_auths.get_mut(&want) {
        list.retain(|v| !(v.vpc_id == vpc.vpc_id && v.vpc_region == vpc.vpc_region));
    }
    resp(200, response_xml("DeleteVPCAssociationAuthorization", ""))
}

// ---------------------------------------------------------------------------
// Health checks
// ---------------------------------------------------------------------------

fn h_create_health_check(req: &Request, backend: &mut Backend) -> Response {
    let Some(body) = parse_xml(req.body.as_ref()) else {
        return bad_input("InvalidInput", "Missing request body");
    };
    let empty = El::default();
    let cfg = body.sub(&["HealthCheckConfig"]).unwrap_or(&empty);
    let hc = HealthCheck {
        id: uuid::Uuid::new_v4().to_string(),
        caller_reference: body.text_or("CallerReference", ""),
        type_: cfg.text_or("Type", "HTTP"),
        ip_address: non_empty(cfg.text_of("IPAddress")),
        port: cfg.int_of("Port"),
        resource_path: non_empty(cfg.text_of("ResourcePath")),
        fqdn: non_empty(cfg.text_of("FullyQualifiedDomainName")),
        search_string: non_empty(cfg.text_of("SearchString")),
        request_interval: cfg.int_of("RequestInterval"),
        failure_threshold: cfg.int_of("FailureThreshold"),
        health_threshold: cfg.int_of("HealthThreshold"),
        measure_latency: cfg
            .text_of("MeasureLatency")
            .map(|v| v == "true")
            .unwrap_or(false),
        inverted: cfg
            .text_of("Inverted")
            .map(|v| v == "true")
            .unwrap_or(false),
        disabled: cfg
            .text_of("Disabled")
            .map(|v| v == "true")
            .unwrap_or(false),
        enable_sni: cfg
            .text_of("EnableSNI")
            .map(|v| v == "true")
            .unwrap_or(false),
        children: cfg.texts_at(&["ChildHealthChecks"], "ChildHealthCheck"),
        regions: cfg.texts_at(&["Regions"], "Region"),
        version: 1,
    };
    let xml = hc.to_xml();
    backend.health_checks.push(hc);
    resp(200, response_xml("CreateHealthCheck", &xml))
}

fn get_health_check_mut<'a>(
    backend: &'a mut Backend,
    id: &str,
) -> Result<&'a mut HealthCheck, Response> {
    match backend.health_checks.iter_mut().find(|h| h.id == id) {
        Some(hc) => Ok(hc),
        None => Err(no_such_health_check(id)),
    }
}

fn h_get_health_check(backend: &mut Backend, id: &str) -> Response {
    let hc = match get_health_check_mut(backend, id) {
        Ok(h) => h,
        Err(resp) => return resp,
    };
    let clone = hc.clone();
    resp(200, response_xml("GetHealthCheck", &clone.to_xml()))
}

fn h_list_health_checks(backend: &mut Backend) -> Response {
    let checks_xml = backend
        .health_checks
        .iter()
        .map(|h| h.to_xml())
        .collect::<String>();
    let count = backend.health_checks.len();
    let inner = wrap("HealthChecks", checks_xml)
        + &wrap("Marker", "")
        + &el("IsTruncated", "false")
        + &el("MaxItems", count.to_string());
    resp(200, response_xml("ListHealthChecks", &inner))
}

fn h_delete_health_check(backend: &mut Backend, id: &str) -> Response {
    backend.health_checks.retain(|h| h.id != id);
    resp(200, response_xml("DeleteHealthCheck", ""))
}

fn h_update_health_check(req: &Request, backend: &mut Backend, id: &str) -> Response {
    let Some(body) = parse_xml(req.body.as_ref()) else {
        return bad_input("InvalidInput", "Missing request body");
    };
    let hc = match get_health_check_mut(backend, id) {
        Ok(h) => h,
        Err(resp) => return resp,
    };
    // moto updates only the fields that are present with truthy values.
    if let Some(v) = non_empty(body.text_of("IPAddress")) {
        hc.ip_address = Some(v);
    }
    if let Some(v) = body.int_of("Port") {
        hc.port = Some(v);
    }
    if let Some(v) = non_empty(body.text_of("ResourcePath")) {
        hc.resource_path = Some(v);
    }
    if let Some(v) = non_empty(body.text_of("FullyQualifiedDomainName")) {
        hc.fqdn = Some(v);
    }
    if let Some(v) = non_empty(body.text_of("SearchString")) {
        hc.search_string = Some(v);
    }
    if let Some(v) = body.int_of("FailureThreshold") {
        hc.failure_threshold = Some(v);
    }
    if let Some(v) = body.int_of("HealthThreshold") {
        hc.health_threshold = Some(v);
    }
    let clone = hc.clone();
    resp(200, response_xml("UpdateHealthCheck", &clone.to_xml()))
}

fn h_get_health_check_status(backend: &mut Backend, id: &str) -> Response {
    if let Err(resp) = get_health_check_mut(backend, id) {
        return resp;
    }
    let inner = wrap(
        "HealthCheckObservations",
        wrap(
            "HealthCheckObservation",
            el("IPAddress", "127.0.13.37")
                + &el("Region", "us-east-1")
                + &wrap(
                    "StatusReport",
                    el("CheckedTime", now_secs_iso())
                        + &el(
                            "Status",
                            "Success: HTTP Status Code: 200. Resolved IP: 127.0.13.37. OK",
                        ),
                ),
        ),
    );
    resp(200, response_xml("GetHealthCheckStatus", &inner))
}

fn h_get_health_check_last_failure(backend: &mut Backend, id: &str) -> Response {
    if let Err(resp) = get_health_check_mut(backend, id) {
        return resp;
    }
    resp(
        200,
        response_xml(
            "GetHealthCheckLastFailureReason",
            "<HealthCheckObservations/>",
        ),
    )
}

fn h_get_health_check_count(backend: &mut Backend) -> Response {
    let inner = el("HealthCheckCount", backend.health_checks.len().to_string());
    resp(200, response_xml("GetHealthCheckCount", &inner))
}

// ---------------------------------------------------------------------------
// Tags
// ---------------------------------------------------------------------------

fn tags_xml(tags: Option<&BTreeMap<String, String>>) -> String {
    match tags {
        Some(map) => map
            .iter()
            .map(|(k, v)| wrap("Tag", el("Key", k) + &el("Value", v)))
            .collect::<String>(),
        None => String::new(),
    }
}

fn h_change_tags_for_resource(
    req: &Request,
    backend: &mut Backend,
    resource_type: &str,
    resource_id: &str,
) -> Response {
    let Some(body) = parse_xml(req.body.as_ref()) else {
        return bad_input("InvalidInput", "Missing request body");
    };
    if resource_type == "hostedzone" {
        if resource_id.len() > 32 {
            return bad_input(
                "InvalidInput",
                &format!(
                    "1 validation error detected: Value '{resource_id}' at 'resourceId' failed to satisfy constraint: Member must have length less than or equal to 32"
                ),
            );
        }
        if !backend.zones.iter().any(|z| z.id == zone_ref(resource_id)) {
            return no_such_zone(resource_id);
        }
    }
    let add_tags = body
        .els_at(&["AddTags"], "Tag")
        .iter()
        .map(|tag| (tag.text_or("Key", ""), tag.text_or("Value", "")))
        .collect::<Vec<(String, String)>>();
    let remove_keys = body.texts_at(&["RemoveTagKeys"], "Key");
    let entry = backend
        .resource_tags
        .entry(resource_id.to_string())
        .or_default();
    for (k, v) in add_tags {
        entry.insert(k, v);
    }
    for k in remove_keys {
        entry.remove(&k);
    }
    resp(200, response_xml("ChangeTagsForResource", ""))
}

fn h_list_tags_for_resource(
    backend: &mut Backend,
    resource_type: &str,
    resource_id: &str,
) -> Response {
    let tags = tags_xml(backend.resource_tags.get(resource_id));
    let inner = wrap(
        "ResourceTagSet",
        el("ResourceType", resource_type) + &el("ResourceId", resource_id) + &wrap("Tags", tags),
    );
    resp(200, response_xml("ListTagsForResource", &inner))
}

fn h_list_tags_for_resources(
    req: &Request,
    backend: &mut Backend,
    resource_type: &str,
) -> Response {
    let Some(body) = parse_xml(req.body.as_ref()) else {
        return bad_input("InvalidInput", "Missing request body");
    };
    let ids = body.texts_at(&["ResourceIds"], "ResourceId");
    let mut sets = String::new();
    for id in ids {
        sets += &wrap(
            "ResourceTagSet",
            el("ResourceType", resource_type)
                + &el("ResourceId", &id)
                + &wrap("Tags", tags_xml(backend.resource_tags.get(&id))),
        );
    }
    resp(
        200,
        response_xml("ListTagsForResources", &wrap("ResourceTagSets", sets)),
    )
}

// ---------------------------------------------------------------------------
// Change tracking
// ---------------------------------------------------------------------------

fn h_get_change(id: &str) -> Response {
    let inner = wrap(
        "ChangeInfo",
        el("Id", format!("{CHANGE_PREFIX}{}", zone_ref(id)))
            + &el("Status", "INSYNC")
            + &el("SubmittedAt", now_secs_iso()),
    );
    resp(200, response_xml("GetChange", &inner))
}

// ---------------------------------------------------------------------------
// Reusable delegation sets
// ---------------------------------------------------------------------------

fn h_create_reusable_delegation_set(req: &Request, backend: &mut Backend) -> Response {
    let caller_reference = parse_xml(req.body.as_ref())
        .map(|b| b.text_or("CallerReference", ""))
        .unwrap_or_default();
    let ds = DelegationSet {
        id: random_id(5),
        caller_reference,
        name_servers: default_name_servers(),
    };
    backend.delegation_sets.push(ds.clone());
    let location = format!("https://route53.amazonaws.com/delegationset/{}", ds.id);
    resp_with(
        201,
        vec![("Location", location)],
        response_xml("CreateReusableDelegationSet", &ds.to_xml()),
    )
}

fn h_list_reusable_delegation_sets(backend: &mut Backend) -> Response {
    let sets_xml = backend
        .delegation_sets
        .iter()
        .map(|d| d.to_xml())
        .collect::<String>();
    let inner = wrap("DelegationSets", sets_xml)
        + &wrap("Marker", "")
        + &el("IsTruncated", "false")
        + &el("MaxItems", "100");
    resp(200, response_xml("ListReusableDelegationSets", &inner))
}

fn find_delegation_set_mut<'a>(
    backend: &'a mut Backend,
    id: &str,
) -> Result<&'a mut DelegationSet, Response> {
    match backend.delegation_sets.iter_mut().find(|d| d.id == id) {
        Some(ds) => Ok(ds),
        None => Err(no_such_delegation_set(id)),
    }
}

fn h_get_reusable_delegation_set(backend: &mut Backend, id: &str) -> Response {
    let clone = match find_delegation_set_mut(backend, id) {
        Ok(ds) => ds.clone(),
        Err(resp) => return resp,
    };
    resp(
        200,
        response_xml("GetReusableDelegationSet", &clone.to_xml()),
    )
}

fn h_delete_reusable_delegation_set(backend: &mut Backend, id: &str) -> Response {
    if find_delegation_set_mut(backend, id).is_err() {
        return no_such_delegation_set(id);
    }
    backend.delegation_sets.retain(|d| d.id != id);
    resp(200, response_xml("DeleteReusableDelegationSet", ""))
}

// ---------------------------------------------------------------------------
// Limits
// ---------------------------------------------------------------------------

/// Renders a `<Limit><Type>…</Type><Value>…</Value></Limit>…<Count>` body.
fn limit_xml(limit_type: &str, value: i64, count: usize) -> String {
    wrap(
        "Limit",
        el("Type", limit_type) + &el("Value", value.to_string()),
    ) + &el("Count", count.to_string())
}

fn account_limit_value(limit_type: &str) -> i64 {
    match limit_type {
        "MAX_HEALTH_CHECKS_BY_OWNER" => 200,
        "MAX_HOSTED_ZONES_BY_OWNER" => 500,
        "MAX_REUSABLE_DELEGATION_SETS_BY_OWNER" => 200,
        "MAX_TRAFFIC_POLICIES_BY_OWNER" => 200,
        "MAX_TRAFFIC_POLICY_INSTANCES_BY_OWNER" => 5,
        _ => 100,
    }
}

fn h_get_account_limit(backend: &mut Backend, limit_type: &str) -> Response {
    let count = match limit_type {
        "MAX_HEALTH_CHECKS_BY_OWNER" => backend.health_checks.len(),
        "MAX_HOSTED_ZONES_BY_OWNER" => backend.zones.len(),
        "MAX_REUSABLE_DELEGATION_SETS_BY_OWNER" => backend.delegation_sets.len(),
        _ => 0,
    };
    let inner = limit_xml(limit_type, account_limit_value(limit_type), count);
    resp(200, response_xml("GetAccountLimit", &inner))
}

fn hosted_zone_limit_value(limit_type: &str) -> i64 {
    match limit_type {
        "MAX_RRSETS_BY_ZONE" => 10000,
        "MAX_VPCS_ASSOCIATED_BY_ZONE" => 100,
        _ => 100,
    }
}

fn h_get_hosted_zone_limit(backend: &mut Backend, id: &str, limit_type: &str) -> Response {
    let count = {
        let zone = match find_zone_mut(backend, id) {
            Ok(z) => z,
            Err(resp) => return resp,
        };
        match limit_type {
            "MAX_RRSETS_BY_ZONE" => zone.rrsets.len(),
            "MAX_VPCS_ASSOCIATED_BY_ZONE" => zone.vpcs.len(),
            _ => 0,
        }
    };
    let inner = limit_xml(limit_type, hosted_zone_limit_value(limit_type), count);
    resp(200, response_xml("GetHostedZoneLimit", &inner))
}

fn reusable_delegation_set_limit_value(limit_type: &str) -> i64 {
    match limit_type {
        "MAX_ZONES_BY_REUSABLE_DELEGATION_SET" => 1000,
        _ => 100,
    }
}

fn h_get_reusable_delegation_set_limit(
    backend: &mut Backend,
    id: &str,
    limit_type: &str,
) -> Response {
    if find_delegation_set_mut(backend, id).is_err() {
        return no_such_delegation_set(id);
    }
    let inner = limit_xml(
        limit_type,
        reusable_delegation_set_limit_value(limit_type),
        0,
    );
    resp(200, response_xml("GetReusableDelegationSetLimit", &inner))
}

// ---------------------------------------------------------------------------
// Query logging configs
// ---------------------------------------------------------------------------

fn h_create_query_logging_config(req: &Request, backend: &mut Backend) -> Response {
    let Some(body) = parse_xml(req.body.as_ref()) else {
        return bad_input("InvalidInput", "Missing request body");
    };
    // The old native provider deliberately skips moto's cross-service
    // (CloudWatch Logs) validation; it always provisions the config.
    let cfg = QueryLoggingConfig {
        id: uuid::Uuid::new_v4().to_string(),
        hosted_zone_id: body.text_or("HostedZoneId", ""),
        cloudwatch_logs_log_group_arn: body.text_or("CloudWatchLogsLogGroupArn", ""),
    };
    let location = format!(
        "https://route53.amazonaws.com/2013-04-01/queryloggingconfig/{}",
        cfg.id
    );
    let xml = cfg.to_xml();
    backend.query_logging_configs.insert(cfg.id.clone(), cfg);
    resp_with(
        201,
        vec![("Location", location)],
        response_xml("CreateQueryLoggingConfig", &xml),
    )
}

fn h_get_query_logging_config(backend: &mut Backend, id: &str) -> Response {
    match backend.query_logging_configs.get(id) {
        Some(cfg) => {
            let clone = cfg.clone();
            resp(200, response_xml("GetQueryLoggingConfig", &clone.to_xml()))
        }
        None => no_such_query_logging_config(),
    }
}

fn h_delete_query_logging_config(backend: &mut Backend, id: &str) -> Response {
    if backend.query_logging_configs.remove(id).is_none() {
        return no_such_query_logging_config();
    }
    resp(200, response_xml("DeleteQueryLoggingConfig", ""))
}

fn h_list_query_logging_configs(
    backend: &mut Backend,
    query: &BTreeMap<String, Vec<String>>,
) -> Response {
    let zone = first(query, "hostedzoneid");
    let cfgs_xml = backend
        .query_logging_configs
        .values()
        .filter(|cfg| zone.as_deref().is_none_or(|z| z == cfg.hosted_zone_id))
        .map(|cfg| cfg.to_xml())
        .collect::<String>();
    let inner = wrap("QueryLoggingConfigs", cfgs_xml);
    resp(200, response_xml("ListQueryLoggingConfigs", &inner))
}

// ---------------------------------------------------------------------------
// CIDR collections
// ---------------------------------------------------------------------------

fn h_create_cidr_collection(req: &Request, backend: &mut Backend) -> Response {
    let Some(body) = parse_xml(req.body.as_ref()) else {
        return bad_input("InvalidInput", "Missing request body");
    };
    let id = uuid::Uuid::new_v4().to_string();
    let arn = format!("arn:aws:route53:::cidrcollection/{id}");
    let location = format!("https://route53.amazonaws.com/2013-04-01/cidrcollection/{id}");
    let collection = CidrCollection {
        id,
        name: body.text_or("Name", ""),
        version: 1,
        arn,
        locations: BTreeMap::new(),
    };
    let inner = wrap(
        "Collection",
        el("Arn", &collection.arn)
            + &el("Id", &collection.id)
            + &el("Name", &collection.name)
            + &el("Version", collection.version.to_string()),
    ) + &el("Location", &location);
    backend.cidr_collections.push(collection);
    resp_with(
        201,
        vec![("Location", location)],
        response_xml("CreateCidrCollection", &inner),
    )
}

fn cidr_collection_xml(c: &CidrCollection) -> String {
    wrap(
        "CidrCollection",
        el("Arn", &c.arn)
            + &el("Id", &c.id)
            + &el("Name", &c.name)
            + &el("Version", c.version.to_string()),
    )
}

fn h_list_cidr_collections(backend: &mut Backend) -> Response {
    let collections_xml = backend
        .cidr_collections
        .iter()
        .map(cidr_collection_xml)
        .collect::<String>();
    let inner = wrap("CidrCollections", collections_xml);
    resp(200, response_xml("ListCidrCollections", &inner))
}

fn h_delete_cidr_collection(backend: &mut Backend, id: &str) -> Response {
    if !backend.cidr_collections.iter().any(|c| c.id == id) {
        return no_such_cidr_collection(id);
    }
    backend.cidr_collections.retain(|c| c.id != id);
    resp(200, response_xml("DeleteCidrCollection", ""))
}

fn h_change_cidr_collection(req: &Request, backend: &mut Backend, id: &str) -> Response {
    let Some(body) = parse_xml(req.body.as_ref()) else {
        return bad_input("InvalidInput", "Missing request body");
    };
    let changes: Vec<(String, String, Vec<String>)> = body
        .els_at(&["Changes"], "member")
        .iter()
        .map(|change| {
            (
                change.text_or("Action", ""),
                change.text_or("LocationName", ""),
                change.texts_at(&["CidrList"], "Cidr"),
            )
        })
        .collect();
    let collection = match backend.cidr_collections.iter_mut().find(|c| c.id == id) {
        Some(c) => c,
        None => return no_such_cidr_collection(id),
    };
    for (action, location, cidrs) in changes {
        match action.as_str() {
            "PUT" => {
                let loc = collection.locations.entry(location.clone()).or_default();
                for cidr in cidrs {
                    if !loc.contains(&cidr) {
                        loc.push(cidr);
                    }
                }
            }
            "DELETE_IF_EXISTS" => {
                if let Some(loc) = collection.locations.get_mut(&location) {
                    for cidr in &cidrs {
                        loc.retain(|c| c != cidr);
                    }
                    if loc.is_empty() {
                        collection.locations.remove(&location);
                    }
                }
            }
            _ => {}
        }
    }
    collection.version += 1;
    let inner = el("Id", collection.version.to_string());
    resp(200, response_xml("ChangeCidrCollection", &inner))
}

fn h_list_cidr_blocks(
    backend: &mut Backend,
    id: &str,
    query: &BTreeMap<String, Vec<String>>,
) -> Response {
    let location = first(query, "location");
    let collection = match backend.cidr_collections.iter().find(|c| c.id == id) {
        Some(c) => c,
        None => return no_such_cidr_collection(id),
    };
    let mut blocks = String::new();
    match &location {
        Some(location_name) => {
            let Some(cidrs) = collection.locations.get(location_name) else {
                return xml_error(
                    404,
                    "NoSuchCidrLocationException",
                    &format!("The CIDR location with name '{location_name}' does not exist."),
                );
            };
            for cidr in cidrs {
                blocks += &wrap(
                    "CidrBlock",
                    el("CidrBlock", cidr) + &el("LocationName", location_name),
                );
            }
        }
        None => {
            for (loc, cidrs) in &collection.locations {
                for cidr in cidrs {
                    blocks += &wrap(
                        "CidrBlock",
                        el("CidrBlock", cidr) + &el("LocationName", loc),
                    );
                }
            }
        }
    }
    let inner = wrap("CidrBlocks", blocks);
    resp(200, response_xml("ListCidrBlocks", &inner))
}

fn h_list_cidr_locations(backend: &mut Backend, id: &str) -> Response {
    let collection = match backend.cidr_collections.iter().find(|c| c.id == id) {
        Some(c) => c,
        None => return no_such_cidr_collection(id),
    };
    let locations_xml = collection
        .locations
        .keys()
        .map(|loc| wrap("LocationSummary", el("LocationName", loc)))
        .collect::<String>();
    let inner = wrap("CidrLocations", locations_xml);
    resp(200, response_xml("ListCidrLocations", &inner))
}

// ---------------------------------------------------------------------------
// Traffic policies
// ---------------------------------------------------------------------------

fn h_create_traffic_policy(req: &Request, backend: &mut Backend) -> Response {
    let Some(body) = parse_xml(req.body.as_ref()) else {
        return bad_input("InvalidInput", "Missing request body");
    };
    let policy = TrafficPolicy {
        id: uuid::Uuid::new_v4().to_string(),
        name: body.text_or("Name", ""),
        versions: BTreeMap::from([(
            1,
            (body.text_or("Document", ""), body.text_or("Comment", "")),
        )]),
    };
    let location = format!(
        "https://route53.amazonaws.com/2013-04-01/trafficpolicy/{}/1",
        policy.id
    );
    let xml = policy.version_xml(1);
    backend.traffic_policies.push(policy);
    resp_with(
        201,
        vec![("Location", location)],
        response_xml("CreateTrafficPolicy", &xml),
    )
}

fn find_policy_mut<'a>(
    backend: &'a mut Backend,
    id: &str,
) -> Result<&'a mut TrafficPolicy, Response> {
    match backend.traffic_policies.iter_mut().find(|p| p.id == id) {
        Some(p) => Ok(p),
        None => Err(no_such_traffic_policy(id)),
    }
}

fn h_get_traffic_policy(backend: &mut Backend, id: &str, version: &str) -> Response {
    let Ok(version) = version.parse::<i64>() else {
        return no_such_traffic_policy(id);
    };
    let policy = match find_policy_mut(backend, id) {
        Ok(p) => p,
        Err(resp) => return resp,
    };
    if !policy.versions.contains_key(&version) {
        return no_such_traffic_policy(id);
    }
    let xml = policy.version_xml(version);
    resp(200, response_xml("GetTrafficPolicy", &xml))
}

fn h_delete_traffic_policy(backend: &mut Backend, id: &str, version: &str) -> Response {
    let Ok(version) = version.parse::<i64>() else {
        return no_such_traffic_policy(id);
    };
    let policy = match find_policy_mut(backend, id) {
        Ok(p) => p,
        Err(resp) => return resp,
    };
    if !policy.versions.contains_key(&version) {
        return no_such_traffic_policy(id);
    }
    policy.versions.remove(&version);
    if policy.versions.is_empty() {
        backend.traffic_policies.retain(|p| p.id != id);
    }
    resp(200, response_xml("DeleteTrafficPolicy", ""))
}

fn h_update_traffic_policy_comment(
    req: &Request,
    backend: &mut Backend,
    id: &str,
    version: &str,
) -> Response {
    let Some(body) = parse_xml(req.body.as_ref()) else {
        return bad_input("InvalidInput", "Missing request body");
    };
    let Ok(version) = version.parse::<i64>() else {
        return no_such_traffic_policy(id);
    };
    let policy = match find_policy_mut(backend, id) {
        Ok(p) => p,
        Err(resp) => return resp,
    };
    if !policy.versions.contains_key(&version) {
        return no_such_traffic_policy(id);
    }
    let comment = body.text_or("Comment", "");
    let document = policy.versions[&version].0.clone();
    policy.versions.insert(version, (document, comment));
    let xml = policy.version_xml(version);
    resp(200, response_xml("UpdateTrafficPolicyComment", &xml))
}

fn h_create_traffic_policy_version(req: &Request, backend: &mut Backend, id: &str) -> Response {
    let Some(body) = parse_xml(req.body.as_ref()) else {
        return bad_input("InvalidInput", "Missing request body");
    };
    let document = body.text_or("Document", "");
    let comment = body.text_or("Comment", "");
    let policy = match find_policy_mut(backend, id) {
        Ok(p) => p,
        Err(resp) => return resp,
    };
    let next = policy.versions.keys().next_back().unwrap_or(&1) + 1;
    policy.versions.insert(next, (document, comment));
    let xml = policy.version_xml(next);
    let location = format!("https://route53.amazonaws.com/2013-04-01/trafficpolicy/{id}/{next}");
    resp_with(
        201,
        vec![("Location", location.clone())],
        response_xml("CreateTrafficPolicyVersion", &xml),
    )
}

fn h_list_traffic_policy_versions(backend: &mut Backend, id: &str) -> Response {
    let policy = match find_policy_mut(backend, id) {
        Ok(p) => p,
        Err(resp) => return resp,
    };
    let versions_xml = policy
        .versions
        .keys()
        .copied()
        .map(|v| policy.version_xml(v))
        .collect::<String>();
    let inner = wrap("TrafficPolicies", versions_xml)
        + &el("IsTruncated", "false")
        + &el("MaxItems", "100")
        + &wrap("TrafficPolicyVersionMarker", "");
    resp(200, response_xml("ListTrafficPolicyVersions", &inner))
}

fn h_list_traffic_policies(backend: &mut Backend) -> Response {
    let policies_xml = backend
        .traffic_policies
        .iter()
        .map(|p| {
            wrap(
                "TrafficPolicySummary",
                el("Id", &p.id)
                    + &el("Name", &p.name)
                    + &el(
                        "Version",
                        p.versions.keys().next_back().unwrap_or(&1).to_string(),
                    ),
            )
        })
        .collect::<String>();
    let inner = wrap("TrafficPolicySummaries", policies_xml)
        + &el("IsTruncated", "false")
        + &wrap("TrafficPolicyIdMarker", "")
        + &el("MaxItems", "100");
    resp(200, response_xml("ListTrafficPolicies", &inner))
}

// ---------------------------------------------------------------------------
// Traffic policy instances
// ---------------------------------------------------------------------------

fn h_create_traffic_policy_instance(req: &Request, backend: &mut Backend) -> Response {
    let Some(body) = parse_xml(req.body.as_ref()) else {
        return bad_input("InvalidInput", "Missing request body");
    };
    let instance = TrafficPolicyInstance {
        id: uuid::Uuid::new_v4().to_string(),
        hosted_zone_id: body.text_or("HostedZoneId", ""),
        name: body.text_or("Name", ""),
        ttl: body.int_of("TTL").unwrap_or(60),
        traffic_policy_id: body.text_or("TrafficPolicyId", ""),
        traffic_policy_version: body.int_of("TrafficPolicyVersion").unwrap_or(1),
        traffic_policy_type: "A".to_string(),
        state: "Applied".to_string(),
        message: String::new(),
    };
    let location = format!(
        "https://route53.amazonaws.com/2013-04-01/trafficpolicyinstance/{}",
        instance.id
    );
    let xml = instance.to_xml();
    backend.traffic_instances.push(instance);
    resp_with(
        201,
        vec![("Location", location.clone())],
        response_xml("CreateTrafficPolicyInstance", &xml),
    )
}

fn find_instance_mut<'a>(
    backend: &'a mut Backend,
    id: &str,
) -> Result<&'a mut TrafficPolicyInstance, Response> {
    match backend.traffic_instances.iter_mut().find(|i| i.id == id) {
        Some(i) => Ok(i),
        None => Err(no_such_traffic_policy_instance(id)),
    }
}

fn h_get_traffic_policy_instance(backend: &mut Backend, id: &str) -> Response {
    let clone = match find_instance_mut(backend, id) {
        Ok(i) => i.clone(),
        Err(resp) => return resp,
    };
    resp(
        200,
        response_xml("GetTrafficPolicyInstance", &clone.to_xml()),
    )
}

fn h_delete_traffic_policy_instance(backend: &mut Backend, id: &str) -> Response {
    if find_instance_mut(backend, id).is_err() {
        return no_such_traffic_policy_instance(id);
    }
    backend.traffic_instances.retain(|i| i.id != id);
    resp(200, response_xml("DeleteTrafficPolicyInstance", ""))
}

fn h_update_traffic_policy_instance(req: &Request, backend: &mut Backend, id: &str) -> Response {
    let Some(body) = parse_xml(req.body.as_ref()) else {
        return bad_input("InvalidInput", "Missing request body");
    };
    let inst = match find_instance_mut(backend, id) {
        Ok(i) => i,
        Err(resp) => return resp,
    };
    if let Some(v) = body.int_of("TTL") {
        inst.ttl = v;
    }
    if let Some(v) = non_empty(body.text_of("TrafficPolicyId")) {
        inst.traffic_policy_id = v;
    }
    if let Some(v) = body.int_of("TrafficPolicyVersion") {
        inst.traffic_policy_version = v;
    }
    let clone = inst.clone();
    resp(
        200,
        response_xml("UpdateTrafficPolicyInstance", &clone.to_xml()),
    )
}

fn h_list_traffic_policy_instances(backend: &mut Backend) -> Response {
    let instances_xml = backend
        .traffic_instances
        .iter()
        .map(|i| i.to_xml())
        .collect::<String>();
    let inner = wrap("TrafficPolicyInstances", instances_xml)
        + &el("IsTruncated", "false")
        + &el("MaxItems", "100");
    resp(200, response_xml("ListTrafficPolicyInstances", &inner))
}

fn traffic_policy_instances_by(
    instances: &[TrafficPolicyInstance],
    filter: impl Fn(&TrafficPolicyInstance) -> bool,
) -> String {
    let instances_xml = instances
        .iter()
        .filter(|i| filter(i))
        .map(|i| i.to_xml())
        .collect::<String>();
    wrap("TrafficPolicyInstances", instances_xml)
        + &el("IsTruncated", "false")
        + &el("MaxItems", "100")
}

fn h_list_traffic_policy_instances_by_zone(
    backend: &mut Backend,
    query: &BTreeMap<String, Vec<String>>,
) -> Response {
    let zone_id = first_str(query, "id").unwrap_or("").to_string();
    let inner =
        traffic_policy_instances_by(&backend.traffic_instances, |i| i.hosted_zone_id == zone_id);
    resp(
        200,
        response_xml("ListTrafficPolicyInstancesByHostedZone", &inner),
    )
}

fn h_list_traffic_policy_instances_by_policy(
    backend: &mut Backend,
    query: &BTreeMap<String, Vec<String>>,
) -> Response {
    let policy_id = first_str(query, "id").unwrap_or("").to_string();
    let policy_version = first(query, "version").and_then(|v| v.parse::<i64>().ok());
    let inner = traffic_policy_instances_by(&backend.traffic_instances, |i| {
        i.traffic_policy_id == policy_id
            && policy_version.is_none_or(|v| i.traffic_policy_version == v)
    });
    resp(
        200,
        response_xml("ListTrafficPolicyInstancesByPolicy", &inner),
    )
}

fn h_get_traffic_policy_instance_count(backend: &mut Backend) -> Response {
    let inner = el(
        "TrafficPolicyInstanceCount",
        backend.traffic_instances.len().to_string(),
    );
    resp(200, response_xml("GetTrafficPolicyInstanceCount", &inner))
}

// ---------------------------------------------------------------------------
// Key signing keys + hosted-zone DNSSEC
// ---------------------------------------------------------------------------

fn h_create_key_signing_key(req: &Request, backend: &mut Backend) -> Response {
    let Some(body) = parse_xml(req.body.as_ref()) else {
        return bad_input("InvalidInput", "Missing request body");
    };
    let zone_id = body.text_or("HostedZoneId", "");
    let ksk = KeySigningKey {
        name: body.text_or("Name", ""),
        status: body.text_or("Status", "INACTIVE"),
        created_date: now_secs_iso(),
    };
    let location = format!(
        "https://route53.amazonaws.com/2013-04-01/hostedzone/{zone_id}/keysigningkeys/{}",
        ksk.name
    );
    let inner = change_info() + &ksk.to_xml() + &el("Location", &location);
    backend
        .key_signing_keys
        .insert((zone_id, ksk.name.clone()), ksk);
    resp_with(
        201,
        vec![("Location", location.clone())],
        response_xml("CreateKeySigningKey", &inner),
    )
}

/// Activate/Deactivate share the same wire shape; `activate` picks the status.
fn h_activate_key_signing_key(
    backend: &mut Backend,
    zone_id: &str,
    ksk_name: &str,
    activate: bool,
) -> Response {
    match backend
        .key_signing_keys
        .get_mut(&(zone_id.to_string(), ksk_name.to_string()))
    {
        Some(ksk) => {
            ksk.status = if activate { "ACTIVE" } else { "INACTIVE" }.to_string();
            let op = if activate {
                "ActivateKeySigningKey"
            } else {
                "DeactivateKeySigningKey"
            };
            resp(200, response_xml(op, &change_info()))
        }
        None => xml_error(
            404,
            "NoSuchKeySigningKey",
            &format!("No key signing key found with name {ksk_name} in zone {zone_id}"),
        ),
    }
}

fn h_delete_key_signing_key(backend: &mut Backend, zone_id: &str, ksk_name: &str) -> Response {
    if backend
        .key_signing_keys
        .remove(&(zone_id.to_string(), ksk_name.to_string()))
        .is_none()
    {
        return xml_error(
            404,
            "NoSuchKeySigningKey",
            &format!("No key signing key found with name {ksk_name} in zone {zone_id}"),
        );
    }
    resp(200, response_xml("DeleteKeySigningKey", &change_info()))
}

fn h_dot_dnssec(backend: &mut Backend, id: &str, enable: bool) -> Response {
    let want = zone_ref(id);
    let Some(zone) = backend.zones.iter_mut().find(|z| z.id == want) else {
        return no_such_zone(id);
    };
    zone.dnssec_enabled = enable;
    let op = if enable {
        "EnableHostedZoneDNSSEC"
    } else {
        "DisableHostedZoneDNSSEC"
    };
    resp(200, response_xml(op, &change_info()))
}

fn h_get_dnssec(backend: &mut Backend, id: &str) -> Response {
    let want = zone_ref(id);
    let Some(zone) = backend.zones.iter().find(|z| z.id == want).cloned() else {
        return no_such_zone(id);
    };
    let serve = if zone.dnssec_enabled {
        "SIGNING"
    } else {
        "NOT_SIGNING"
    };
    let ksks_xml = backend
        .key_signing_keys
        .iter()
        .filter(|(k, _)| k.0 == want)
        .map(|(_, ksk)| ksk.to_xml())
        .collect::<String>();
    let inner = wrap("Status", el("ServeSignature", serve)) + &wrap("KeySigningKeys", ksks_xml);
    resp(200, response_xml("GetDNSSEC", &inner))
}

fn h_update_hosted_zone_features(backend: &mut Backend, id: &str) -> Response {
    let want = zone_ref(id);
    match find_zone_mut(backend, &want) {
        Ok(_) => resp(
            200,
            response_xml("UpdateHostedZoneFeatures", &el("HostedZoneId", &want)),
        ),
        Err(resp) => resp,
    }
}

// ---------------------------------------------------------------------------
// Probe helpers: TestDNSAnswer, CheckerIpRanges, GeoLocation
// ---------------------------------------------------------------------------

fn h_test_dns_answer(backend: &mut Backend, query: &BTreeMap<String, Vec<String>>) -> Response {
    let record_name = first_str(query, "recordname").unwrap_or("").to_string();
    let record_type = first_str(query, "recordtype").unwrap_or("A").to_string();
    let hosted_zone_id = first_str(query, "hostedzoneid").unwrap_or("").to_string();

    // Mirror the old provider: resolve the record values when the zone and
    // record exist; fall back to a canned answer otherwise.
    let mut record_data: Vec<String> = Vec::new();
    let zone = backend
        .zones
        .iter()
        .find(|z| z.id == zone_ref(&hosted_zone_id))
        .cloned();
    if let Some(zone) = zone {
        for rr in &zone.rrsets {
            let matches = (rr.name == record_name || rr.name == format!("{record_name}."))
                && rr.type_ == record_type;
            if matches {
                if let Some(records) = &rr.records {
                    record_data.extend(records.iter().cloned());
                }
            }
        }
    }
    if record_data.is_empty() {
        record_data = if record_type == "A" {
            vec!["127.0.0.1".to_string()]
        } else {
            vec![record_name.clone()]
        };
    }
    let records_xml = record_data
        .iter()
        .map(|r| el("RecordData", r))
        .collect::<String>();
    let inner = el("Nameserver", "ns-1.awsdns-01.com")
        + &el("RecordName", &record_name)
        + &el("RecordType", &record_type)
        + &el("ResponseCode", "NOERROR")
        + &el("Protocol", "UDP")
        + &records_xml;
    resp(200, response_xml("TestDNSAnswer", &inner))
}

fn h_get_checker_ip_ranges() -> Response {
    let ranges_xml = [
        "54.183.255.128/26",
        "54.228.16.0/26",
        "176.34.159.192/26",
        "54.232.40.64/26",
    ]
    .iter()
    .map(|r| el("member", *r))
    .collect::<String>();
    resp(
        200,
        response_xml("GetCheckerIpRanges", &wrap("CheckerIpRanges", ranges_xml)),
    )
}

const CONTINENT_NAMES: &[(&str, &str)] = &[
    ("AF", "Africa"),
    ("AN", "Antarctica"),
    ("AS", "Asia"),
    ("EU", "Europe"),
    ("NA", "North America"),
    ("OC", "Oceania"),
    ("SA", "South America"),
];

const COUNTRY_NAMES: &[(&str, &str)] = &[
    ("US", "United States"),
    ("GB", "United Kingdom"),
    ("CA", "Canada"),
    ("DE", "Germany"),
    ("FR", "France"),
    ("JP", "Japan"),
    ("AU", "Australia"),
    ("BR", "Brazil"),
    ("IN", "India"),
    ("CN", "China"),
    ("MX", "Mexico"),
    ("IT", "Italy"),
    ("ES", "Spain"),
    ("KR", "Republic of Korea"),
    ("NL", "Netherlands"),
    ("SE", "Sweden"),
    ("NO", "Norway"),
    ("DK", "Denmark"),
    ("FI", "Finland"),
    ("IE", "Ireland"),
    ("NZ", "New Zealand"),
    ("SG", "Singapore"),
];

/// lookup helper used by the geo tables above (returns the code when unknown).
fn geo_name(table: &[(&str, &str)], code: &str) -> String {
    table
        .iter()
        .find(|(c, _)| *c == code)
        .map(|(_, n)| n.to_string())
        .unwrap_or_else(|| code.to_string())
}

/// Renders a single `<GeoLocationDetails>` block for the query codes given.
///
/// `subdivision` is echoed with the country code, matching the old
/// provider's passthrough behavior for subdivision lookups.
fn geo_details_xml(continent: &str, country: &str, subdivision: &str) -> String {
    let mut parts = String::new();
    if !continent.is_empty() {
        parts += &el("ContinentCode", continent);
        parts += &el("ContinentName", geo_name(CONTINENT_NAMES, continent));
    }
    if !country.is_empty() {
        parts += &el("CountryCode", country);
        parts += &el("CountryName", geo_name(COUNTRY_NAMES, country));
    }
    if !subdivision.is_empty() {
        parts += &el("SubdivisionCode", subdivision);
    }
    wrap("GeoLocationDetails", parts)
}

fn h_get_geo_location(query: &BTreeMap<String, Vec<String>>) -> Response {
    let continent = first_str(query, "continentcode").unwrap_or("").to_string();
    let country = first_str(query, "countrycode").unwrap_or("").to_string();
    let subdivision = first_str(query, "subdivisioncode")
        .unwrap_or("")
        .to_string();
    resp(
        200,
        response_xml(
            "GetGeoLocation",
            &geo_details_xml(&continent, &country, &subdivision),
        ),
    )
}

fn h_list_geo_locations() -> Response {
    let list = CONTINENT_NAMES
        .iter()
        .map(|(code, name)| {
            wrap(
                "GeoLocationDetails",
                el("ContinentCode", code) + &el("ContinentName", name),
            )
        })
        .collect::<String>();
    let inner =
        wrap("GeoLocationDetailsList", list) + &el("IsTruncated", "false") + &el("MaxItems", "100");
    resp(200, response_xml("ListGeoLocations", &inner))
}

// ---------------------------------------------------------------------------
// Unit tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    fn test_req(account: u64, method: &str, path: &str, body: &str) -> Request {
        Request {
            service: "route53".to_string(),
            operation: String::new(),
            method: method.to_string(),
            path: format!("/2013-04-01/{path}"),
            query_string: String::new(),
            headers: std::collections::HashMap::new(),
            body: body.to_string().into(),
            account,
            region: "us-east-1".to_string(),
        }
    }

    #[test]
    fn parse_change_extracts_records() {
        let body = parse_xml(
            br#"<ChangeResourceRecordSetsRequest xmlns="https://route53.amazonaws.com/doc/2013-04-01/"><ChangeBatch><Changes><Change><Action>CREATE</Action><ResourceRecordSet><Name>a.x.test.com</Name><Type>A</Type><TTL>60</TTL><ResourceRecords><ResourceRecord><Value>10.0.0.5</Value></ResourceRecord></ResourceRecords></ResourceRecordSet></Change></Changes></ChangeBatch></ChangeResourceRecordSetsRequest>"#,
        );
        let Some(body) = body else {
            panic!("body should parse");
        };
        let changes: Vec<&El> = body.els_at(&["ChangeBatch", "Changes"], "Change");
        assert_eq!(changes.len(), 1, "one Change element expected");
        let (action, rr) = parse_change(changes[0]).unwrap();
        assert_eq!(action, "CREATE");
        assert_eq!(rr.name, "a.x.test.com");
        assert_eq!(rr.ttl, Some(60));
        assert_eq!(
            rr.records,
            Some(vec!["10.0.0.5".to_string()]),
            "records must be extracted"
        );
    }

    #[test]
    fn unknown_path_is_a_501() {
        let resp = dispatch(&test_req(1, "GET", "unknownpath", ""));
        assert_eq!(resp.status, 501);
    }

    #[test]
    fn hosted_zone_lifecycle_round_trip() {
        // Unique account ids keep the process-global state isolated per test.
        let account =
            10_000_000_000_000 + (uuid::Uuid::new_v4().as_u128() % 1_000_000_000_000) as u64;
        let create = dispatch(&test_req(
            account,
            "POST",
            "hostedzone",
            r#"<CreateHostedZoneRequest xmlns="https://route53.amazonaws.com/doc/2013-04-01/"><Name>zz.test.com</Name><CallerReference>r1</CallerReference><HostedZoneConfig><PrivateZone>false</PrivateZone></HostedZoneConfig></CreateHostedZoneRequest>"#,
        ));
        assert_eq!(create.status, 201);
        let create_body = String::from_utf8(create.body.clone()).unwrap();
        let id = create_body
            .split("<Id>/hostedzone/")
            .nth(1)
            .unwrap()
            .split("</Id>")
            .next()
            .unwrap()
            .to_string();
        assert!(create_body.contains("<Name>zz.test.com.</Name>"));
        assert!(create_body.contains("DelegationSet"));
        assert!(create_body.contains("NameServer"));

        let change = dispatch(&test_req(
            account,
            "POST",
            &format!("hostedzone/{id}/rrset/"),
            r#"<ChangeResourceRecordSetsRequest xmlns="https://route53.amazonaws.com/doc/2013-04-01/"><ChangeBatch><Changes><Change><Action>UPSERT</Action><ResourceRecordSet><Name>a.zz.test.com</Name><Type>A</Type><TTL>60</TTL><ResourceRecords><ResourceRecord><Value>1.2.3.4</Value></ResourceRecord></ResourceRecords></ResourceRecordSet></Change></Changes></ChangeBatch></ChangeResourceRecordSetsRequest>"#,
        ));
        assert_eq!(change.status, 200);

        let list = dispatch(&test_req(
            account,
            "GET",
            &format!("hostedzone/{id}/rrset"),
            "",
        ));
        let list_body = String::from_utf8(list.body).unwrap();
        assert!(list_body.contains("a.zz.test.com."));
        assert!(list_body.contains("<Type>SOA</Type>"));
        assert!(list_body.contains("<Type>NS</Type>"));

        // Mirrors the suite's cleanup: drop the record, then the zone
        // (DeleteHostedZone rejects zones with non-NS/SOA records).
        let cleanup = dispatch(&test_req(
            account,
            "POST",
            &format!("hostedzone/{id}/rrset/"),
            r#"<ChangeResourceRecordSetsRequest xmlns="https://route53.amazonaws.com/doc/2013-04-01/"><ChangeBatch><Changes><Change><Action>DELETE</Action><ResourceRecordSet><Name>a.zz.test.com</Name><Type>A</Type><TTL>60</TTL><ResourceRecords><ResourceRecord><Value>1.2.3.4</Value></ResourceRecord></ResourceRecords></ResourceRecordSet></Change></Changes></ChangeBatch></ChangeResourceRecordSetsRequest>"#,
        ));
        assert_eq!(cleanup.status, 200);

        let delete = dispatch(&test_req(
            account,
            "DELETE",
            &format!("hostedzone/{id}"),
            "",
        ));
        assert_eq!(delete.status, 200);
    }

    #[test]
    fn change_tags_then_list_tags() {
        let account =
            11_000_000_000_000 + (uuid::Uuid::new_v4().as_u128() % 1_000_000_000_000) as u64;
        let create = dispatch(&test_req(
            account,
            "POST",
            "hostedzone",
            r#"<CreateHostedZoneRequest xmlns="https://route53.amazonaws.com/doc/2013-04-01/"><Name>tag.test.com</Name><CallerReference>r2</CallerReference></CreateHostedZoneRequest>"#,
        ));
        let id = String::from_utf8(create.body)
            .unwrap()
            .split("<Id>/hostedzone/")
            .nth(1)
            .unwrap()
            .split("</Id>")
            .next()
            .unwrap()
            .to_string();

        let add = dispatch(&test_req(
            account,
            "POST",
            &format!("tags/hostedzone/{id}"),
            r#"<ChangeTagsForResourceRequest xmlns="https://route53.amazonaws.com/doc/2013-04-01/"><AddTags><Tag><Key>k</Key><Value>v</Value></Tag></AddTags></ChangeTagsForResourceRequest>"#,
        ));
        assert_eq!(add.status, 200);
        let list = dispatch(&test_req(
            account,
            "GET",
            &format!("tags/hostedzone/{id}"),
            "",
        ));
        assert!(String::from_utf8(list.body)
            .unwrap()
            .contains("<Key>k</Key>"));
    }

    #[test]
    fn route53_alias_no_ttl() {
        // Alias records have no TTL of their own: Route53 inherits it from
        // the alias target, so list_resource_record_sets() must never return
        // a TTL member for them (moto 5.2.2 #10029,
        // test_alias_record_set_omits_ttl). Even an explicitly supplied TTL
        // is not echoed back for an alias set.
        let account =
            12_000_000_000_000 + (uuid::Uuid::new_v4().as_u128() % 1_000_000_000_000) as u64;
        let create = dispatch(&test_req(
            account,
            "POST",
            "hostedzone",
            r#"<CreateHostedZoneRequest xmlns="https://route53.amazonaws.com/doc/2013-04-01/"><Name>aliasttl.test.com</Name><CallerReference>alias-ttl</CallerReference></CreateHostedZoneRequest>"#,
        ));
        assert_eq!(create.status, 201);
        let id = String::from_utf8(create.body.clone())
            .unwrap()
            .split("<Id>/hostedzone/")
            .nth(1)
            .unwrap()
            .split("</Id>")
            .next()
            .unwrap()
            .to_string();

        let change = dispatch(&test_req(
            account,
            "POST",
            &format!("hostedzone/{id}/rrset/"),
            r#"<ChangeResourceRecordSetsRequest xmlns="https://route53.amazonaws.com/doc/2013-04-01/"><ChangeBatch><Changes><Change><Action>CREATE</Action><ResourceRecordSet><Name>target.aliasttl.test.com</Name><Type>A</Type><TTL>60</TTL><ResourceRecords><ResourceRecord><Value>10.0.0.5</Value></ResourceRecord></ResourceRecords></ResourceRecordSet></Change><Change><Action>CREATE</Action><ResourceRecordSet><Name>alias.aliasttl.test.com</Name><Type>A</Type><TTL>60</TTL><AliasTarget><HostedZoneId>/hostedzone/ID</HostedZoneId><DNSName>target.aliasttl.test.com</DNSName><EvaluateTargetHealth>false</EvaluateTargetHealth></AliasTarget></ResourceRecordSet></Change></Changes></ChangeBatch></ChangeResourceRecordSetsRequest>"#,
        ));
        assert_eq!(change.status, 200);

        let list = dispatch(&test_req(
            account,
            "GET",
            &format!("hostedzone/{id}/rrset"),
            "",
        ));
        assert_eq!(list.status, 200);
        let body = String::from_utf8(list.body).unwrap();
        let alias_chunks: Vec<String> = body
            .split("<ResourceRecordSet>")
            .skip(1)
            .map(|chunk| {
                chunk
                    .split("</ResourceRecordSet>")
                    .next()
                    .unwrap()
                    .to_string()
            })
            .collect();

        let alias_chunk = alias_chunks.iter().find(|chunk| {
            chunk.contains("<Name>alias.aliasttl.test.com.</Name>")
                && chunk.contains("<AliasTarget>")
        });
        let Some(alias_chunk) = alias_chunk else {
            panic!("alias set not listed: {body}");
        };
        assert!(
            !alias_chunk.contains("<TTL>"),
            "alias set must not render a TTL: {alias_chunk}"
        );

        // Non-alias sets (and the zone's default SOA) keep their TTLs.
        let target_chunk = alias_chunks
            .iter()
            .find(|chunk| chunk.contains("<Name>target.aliasttl.test.com.</Name>"))
            .expect("alias target set listed");
        assert!(target_chunk.contains("<TTL>60</TTL>"));
        let soa_chunk = alias_chunks
            .iter()
            .find(|chunk| chunk.contains("<Type>SOA</Type>"))
            .expect("SOA set listed");
        assert!(soa_chunk.contains("<TTL>900</TTL>"));
    }
}
