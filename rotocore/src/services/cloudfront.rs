//! CloudFront — native REST-XML backend.
//!
//! Replaces the moto sidecar for `cloudfront` on the `/2020-05-31` REST-XML
//! surface (distributions, origin access controls / identities, public keys,
//! key groups, CloudFront Functions, cache / response-headers / origin-request
//! policies, invalidations, tags, field-level encryption, continuous
//! deployment policies, streaming distributions, monitoring subscriptions,
//! realtime log configs, key-value stores, VPC origins, trust stores, anycast
//! IP lists, connection groups / functions, distribution tenants, resource
//! policies and the newer stub operations).
//!
//! Wire shapes mirror moto's cloudfront templates verbatim (the pytest
//! compat suite `tests/compatibility/test_cloudfront_compat.py` is the spec);
//! request bodies are the XML roots botocore emits for the `2020-05-31`
//! operations (single-member payloads, or `<XxxRequest>` wrappers).

use crate::proto::{query_map, query_first, xml_escape};
use crate::store;
use crate::{Request, Response};
use quick_xml::events::Event;
use quick_xml::Reader;
use std::collections::{BTreeMap, HashMap};

const XMLNS: &str = "http://cloudfront.amazonaws.com/doc/2020-05-31/";
/// moto keys cloudfront state per partition, not per region.
const PARTITION: &str = "aws";

// ---------------------------------------------------------------------------
// XML tree
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
    fn text_or(&self, name: &str, default: &'static str) -> String {
        self.text_of(name).unwrap_or(default).to_string()
    }

    fn components<'a>(&'a self, name: &'a str) -> impl Iterator<Item = &'a El> + '_ {
        self.children.iter().filter(move |c| c.name == name)
    }

    /// All child nodes named `name` at a nested path (`Items > CNAME`).
    fn els_at<'a>(&'a self, path: &[&str], name: &'a str) -> Vec<&'a El> {
        match self.sub(path) {
            Some(node) => node
                .children
                .iter()
                .filter(|c| c.name == name)
                .collect(),
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

    /// The (trimmed) text of this element as an owned String.
    fn clone_text(&self) -> String {
        self.as_str().to_string()
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
            Ok(Event::Eof) | Err(_) => break,
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

fn empty_resp(status: u16) -> Response {
    resp(status, "")
}

/// moto/CloudFront rest-xml error envelope (model-driven serializer output).
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

/// The first character comes from the letter set (moto's `random_id`).
fn random_upper(length: usize) -> String {
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

fn random_domain(length: usize) -> String {
    const FIRST: &[u8] = b"abcdefghijklmnopqrstuvwxyz";
    const REST: &[u8] = b"abcdefghijklmnopqrstuvwxyz0123456789";
    let bytes = uuid::Uuid::new_v4().as_bytes().to_vec();
    let mut out = String::with_capacity(length);
    out.push(FIRST[bytes[0] as usize % FIRST.len()] as char);
    for i in 1..length {
        let b = bytes[i % bytes.len()];
        out.push(REST[b as usize % REST.len()] as char);
    }
    out
}

fn now_millis_iso() -> String {
    crate::proto::now_iso()
}

// ---------------------------------------------------------------------------
// State
// ---------------------------------------------------------------------------

#[derive(Clone)]
struct Distribution {
    id: String,
    arn: String,
    /// ManagedState transition: `InProgress` until the first read advances it.
    status: String,
    last_modified_time: String,
    domain_name: String,
    etag: String,
    /// Caller reference (moto's config default when unset in the request).
    caller_reference: String,
    config: El,
    aliases: Vec<String>,
    web_acl_id: String,
}

impl Distribution {
    fn advance(&mut self) {
        if self.status == "InProgress" {
            self.status = "Deployed".to_string();
        }
    }
}

#[derive(Clone)]
struct Invalidation {
    id: String,
    dist_id: String,
    status: String,
    create_time: String,
    paths: Vec<String>,
    caller_ref: String,
}

#[derive(Clone)]
struct OriginAccessControl {
    id: String,
    name: String,
    description: String,
    signing_protocol: String,
    signing_behaviour: String,
    origin_type: String,
    etag: String,
}

impl OriginAccessControl {
    fn update(&mut self, config: &El) {
        if let Some(name) = config.text_of("Name") {
            self.name = name.to_string();
        }
        if let Some(description) = config.text_of("Description") {
            self.description = description.to_string();
        }
        if let Some(protocol) = config.text_of("SigningProtocol") {
            self.signing_protocol = protocol.to_string();
        }
        if let Some(behavior) = config.text_of("SigningBehavior") {
            self.signing_behaviour = behavior.to_string();
        }
        if let Some(origin_type) = config.text_of("OriginAccessControlOriginType") {
            self.origin_type = origin_type.to_string();
        }
    }
}

#[derive(Clone)]
struct OriginAccessIdentity {
    id: String,
    s3_canonical_user_id: String,
    caller_reference: String,
    comment: String,
    etag: String,
}

impl OriginAccessIdentity {
    fn update(&mut self, caller_reference: &str, comment: &str) {
        self.caller_reference = caller_reference.to_string();
        self.comment = comment.to_string();
        self.etag = random_upper(13);
    }
}

#[derive(Clone)]
struct PublicKey {
    id: String,
    caller_ref: String,
    name: String,
    encoded_key: String,
    created: String,
    etag: String,
}

#[derive(Clone)]
struct KeyGroup {
    id: String,
    name: String,
    items: Vec<String>,
    etag: String,
}

impl KeyGroup {
    fn update(&mut self, name: &str, items: Vec<String>) {
        self.name = name.to_string();
        self.items = items;
        self.etag = random_upper(14);
    }
}

#[derive(Clone)]
struct CloudFrontFunction {
    name: String,
    function_code: String,
    function_config: El,
    status: String,
    stage: String,
    created_time: String,
    last_modified_time: String,
    etag: String,
    function_arn: String,
}

impl CloudFrontFunction {
    fn update(&mut self, function_code: &str, function_config: El) {
        self.function_code = function_code.to_string();
        self.function_config = function_config;
        self.last_modified_time = now_millis_iso();
        self.etag = random_upper(14);
    }

    fn publish(&mut self) {
        self.status = "UNASSOCIATED".to_string();
        self.stage = "LIVE".to_string();
        self.etag = random_upper(14);
    }
}

#[derive(Clone)]
struct CachePolicy {
    id: String,
    name: String,
    comment: String,
    default_ttl: String,
    max_ttl: String,
    min_ttl: String,
    last_modified_time: String,
    etag: String,
}

impl CachePolicy {
    fn new(id: String, config: &El) -> Self {
        Self {
            id,
            name: config.text_of("Name").unwrap_or_default().to_string(),
            comment: config.text_of("Comment").unwrap_or_default().to_string(),
            default_ttl: config.text_of("DefaultTTL").unwrap_or("86400").to_string(),
            max_ttl: config.text_of("MaxTTL").unwrap_or("31536000").to_string(),
            min_ttl: config.text_of("MinTTL").unwrap_or("0").to_string(),
            last_modified_time: now_millis_iso(),
            etag: random_upper(14),
        }
    }

    fn update(&mut self, config: &El) {
        if let Some(name) = config.text_of("Name") {
            self.name = name.to_string();
        }
        if let Some(comment) = config.text_of("Comment") {
            self.comment = comment.to_string();
        }
        if let Some(ttl) = config.text_of("DefaultTTL") {
            self.default_ttl = ttl.to_string();
        }
        if let Some(ttl) = config.text_of("MaxTTL") {
            self.max_ttl = ttl.to_string();
        }
        if let Some(ttl) = config.text_of("MinTTL") {
            self.min_ttl = ttl.to_string();
        }
        self.last_modified_time = now_millis_iso();
        self.etag = random_upper(14);
    }
}

#[derive(Clone)]
struct ResponseHeadersPolicy {
    id: String,
    name: String,
    comment: String,
    last_modified_time: String,
    etag: String,
}

impl ResponseHeadersPolicy {
    fn update(&mut self, config: &El) {
        if let Some(name) = config.text_of("Name") {
            self.name = name.to_string();
        }
        if let Some(comment) = config.text_of("Comment") {
            self.comment = comment.to_string();
        }
        self.last_modified_time = now_millis_iso();
        self.etag = random_upper(14);
    }
}

#[derive(Clone)]
struct OriginRequestPolicy {
    id: String,
    name: String,
    comment: String,
    headers_behavior: String,
    cookies_behavior: String,
    query_strings_behavior: String,
    last_modified_time: String,
    etag: String,
}

impl OriginRequestPolicy {
    fn update(&mut self, config: &El) {
        if let Some(name) = config.text_of("Name") {
            self.name = name.to_string();
        }
        if let Some(comment) = config.text_of("Comment") {
            self.comment = comment.to_string();
        }
        if let Some(v) = config.child("HeadersConfig").and_then(|c| c.text_of("HeaderBehavior")) {
            self.headers_behavior = v.to_string();
        }
        if let Some(v) = config.child("CookiesConfig").and_then(|c| c.text_of("CookieBehavior")) {
            self.cookies_behavior = v.to_string();
        }
        if let Some(v) = config
            .child("QueryStringsConfig")
            .and_then(|c| c.text_of("QueryStringBehavior"))
        {
            self.query_strings_behavior = v.to_string();
        }
        self.last_modified_time = now_millis_iso();
        self.etag = random_upper(14);
    }
}

#[derive(Clone)]
struct FleConfig {
    id: String,
    caller_reference: String,
    comment: String,
    last_modified_time: String,
    etag: String,
}

impl FleConfig {
    fn update(&mut self, config: &El) {
        if let Some(v) = config.text_of("CallerReference") {
            self.caller_reference = v.to_string();
        }
        if let Some(v) = config.text_of("Comment") {
            self.comment = v.to_string();
        }
        self.last_modified_time = now_millis_iso();
        self.etag = random_upper(14);
    }
}

#[derive(Clone)]
struct FleProfile {
    id: String,
    name: String,
    caller_reference: String,
    comment: String,
    last_modified_time: String,
    etag: String,
}

impl FleProfile {
    fn update(&mut self, config: &El) {
        if let Some(v) = config.text_of("Name") {
            self.name = v.to_string();
        }
        if let Some(v) = config.text_of("CallerReference") {
            self.caller_reference = v.to_string();
        }
        if let Some(v) = config.text_of("Comment") {
            self.comment = v.to_string();
        }
        self.last_modified_time = now_millis_iso();
        self.etag = random_upper(14);
    }
}

#[derive(Clone)]
struct CdpPolicy {
    id: String,
    enabled: String,
    last_modified_time: String,
    etag: String,
}

impl CdpPolicy {
    fn update(&mut self, config: &El) {
        if let Some(v) = config.text_of("Enabled") {
            self.enabled = v.to_string();
        }
        self.last_modified_time = now_millis_iso();
        self.etag = random_upper(14);
    }
}

#[derive(Clone)]
struct RealtimeLogConfig {
    name: String,
    arn: String,
    sampling_rate: i64,
    end_points: Vec<(String, String, String)>, // (StreamType, RoleARN, StreamARN)
    fields: Vec<String>,
}

impl RealtimeLogConfig {
    fn update(
        &mut self,
        sampling_rate: Option<i64>,
        end_points: Option<Vec<(String, String, String)>>,
        fields: Option<Vec<String>>,
    ) {
        if let Some(rate) = sampling_rate {
            self.sampling_rate = rate;
        }
        if let Some(eps) = end_points {
            self.end_points = eps;
        }
        if let Some(fields) = fields {
            self.fields = fields;
        }
    }
}

#[derive(Clone)]
struct AnycastIpList {
    id: String,
    name: String,
    status: String,
    arn: String,
    ip_count: i64,
    last_modified_time: String,
    etag: String,
}

#[derive(Clone)]
struct VpcOrigin {
    id: String,
    account_id: u64,
    arn: String,
    status: String,
    created_time: String,
    last_modified_time: String,
    endpoint_config: El,
    etag: String,
}

impl VpcOrigin {
    fn update(&mut self, config: El) {
        self.endpoint_config = config;
        self.last_modified_time = now_millis_iso();
        self.etag = random_upper(14);
    }
}

#[derive(Clone)]
struct KeyValueStore {
    id: String,
    name: String,
    comment: String,
    arn: String,
    status: String,
    last_modified_time: String,
    etag: String,
}

impl KeyValueStore {
    fn update(&mut self, comment: &str) {
        self.comment = comment.to_string();
        self.last_modified_time = now_millis_iso();
        self.etag = random_upper(14);
    }
}

#[derive(Clone)]
struct TrustStore {
    id: String,
    name: String,
    status: String,
    arn: String,
    number_of_ca_certificates: i64,
    last_modified_time: String,
    etag: String,
}

impl TrustStore {
    fn update(&mut self) {
        self.last_modified_time = now_millis_iso();
        self.etag = random_upper(14);
    }
}

#[derive(Clone)]
struct ConnectionGroup {
    id: String,
    name: String,
    arn: String,
    status: String,
    created_time: String,
    last_modified_time: String,
    etag: String,
}

#[derive(Clone)]
struct ConnectionFunction {
    id: String,
    name: String,
    connection_function_arn: String,
    status: String,
    stage: String,
    created_time: String,
    last_modified_time: String,
    etag: String,
}

#[derive(Clone)]
struct DistributionTenant {
    id: String,
    name: String,
    distribution_id: String,
    arn: String,
    status: String,
    created_time: String,
    last_modified_time: String,
    etag: String,
}

#[derive(Clone)]
struct StreamingDistribution {
    id: String,
    arn: String,
    status: String,
    last_modified_time: String,
    domain_name: String,
    etag: String,
    config: El,
}

impl StreamingDistribution {
    fn advance(&mut self) {
        if self.status == "InProgress" {
            self.status = "Deployed".to_string();
        }
    }
}

#[derive(Default)]
struct Backend {
    distributions: Vec<Distribution>,
    invalidations: HashMap<String, Vec<Invalidation>>,
    origin_access_controls: Vec<OriginAccessControl>,
    origin_access_identities: Vec<OriginAccessIdentity>,
    public_keys: Vec<PublicKey>,
    key_groups: Vec<KeyGroup>,
    functions: Vec<CloudFrontFunction>,
    cache_policies: Vec<CachePolicy>,
    response_headers_policies: Vec<ResponseHeadersPolicy>,
    origin_request_policies: Vec<OriginRequestPolicy>,
    streaming_distributions: Vec<StreamingDistribution>,
    fle_configs: Vec<FleConfig>,
    fle_profiles: Vec<FleProfile>,
    cdp_policies: Vec<CdpPolicy>,
    /// distribution id → RealtimeMetricsSubscriptionStatus
    monitoring: HashMap<String, String>,
    realtime_log_configs: Vec<RealtimeLogConfig>,
    anycast_ip_lists: Vec<AnycastIpList>,
    vpc_origins: Vec<VpcOrigin>,
    key_value_stores: Vec<KeyValueStore>,
    trust_stores: Vec<TrustStore>,
    connection_groups: Vec<ConnectionGroup>,
    connection_functions: Vec<ConnectionFunction>,
    distribution_tenants: Vec<DistributionTenant>,
    /// ARN → tags (mirrors moto's TaggingService)
    tags: HashMap<String, BTreeMap<String, String>>,
}

fn with_state<T>(account: u64, f: impl FnOnce(&mut Backend) -> T) -> T {
    let slot = store::slot::<Backend>("cloudfront");
    let mut map = slot.write();
    f(map.entry((account, PARTITION.to_string())).or_default())
}

/// Wire entry point — routes by method + path (REST-XML has no Action param).
pub fn dispatch(req: &Request) -> Response {
    let mut segs: Vec<String> = req
        .path
        .split('/')
        .filter(|s| !s.is_empty())
        .map(crate::proto::url_decode)
        .collect();
    match segs.first() {
        Some(v) if v == "2020-05-31" => {
            segs.remove(0);
        }
        _ => return unhandled(req),
    }
    let segs: Vec<&str> = segs.iter().map(String::as_str).collect();
    let query = query_map(&req.query_string);
    let if_match = req.headers.get("if-match").cloned().unwrap_or_default();
    with_state(req.account, |backend| {
        route(req, backend, &segs, &query, &if_match)
    })
}

// ---------------------------------------------------------------------------
// Routing
// ---------------------------------------------------------------------------

fn unhandled(_req: &Request) -> Response {
    xml_error(501, "NotImplemented", "Operation is not implemented")
}

fn not_implemented() -> Response {
    xml_error(501, "NotImplemented", "Operation is not implemented")
}

fn route(
    req: &Request,
    backend: &mut Backend,
    segs: &[&str],
    query: &BTreeMap<String, Vec<String>>,
    if_match: &str,
) -> Response {
    let account = req.account;
    let method = req.method.as_str();
    let so = |i: usize| -> &str { segs.get(i).copied().unwrap_or("") };
    let seg1 = so(0);
    let seg2 = so(1);
    let last = segs.last().copied().unwrap_or("");

    if seg1 == "tagging" {
        return handle_tagging(req, backend, query);
    }

    match (method, segs) {
        // ------------------------------------------------ distributions
        ("POST", ["distribution"]) => h_create_distribution(account, backend, req),
        ("GET", ["distribution"]) => h_list_distributions(backend),
        ("GET", ["distribution", _]) => h_get_distribution(backend, last),
        ("DELETE", ["distribution", _]) => h_delete_distribution(backend, last, if_match),
        ("GET", ["distribution", _, "config"]) => {
            h_get_distribution_config(backend, seg2)
        }
        ("PUT", ["distribution", _, "config"]) => {
            h_update_distribution(account, backend, req, seg2, if_match)
        }
        ("POST", ["distribution", _, "invalidation"]) => {
            h_create_invalidation(backend, seg2, req)
        }
        ("GET", ["distribution", _, "invalidation"]) => h_list_invalidations(backend, seg2),
        ("GET", ["distribution", _, "invalidation", _]) => {
            h_get_invalidation(backend, so(segs.len() - 3), so(segs.len() - 1))
        }
        ("PUT", ["distribution", _, "associate-alias"]) => {
            h_associate_alias(backend, seg2, first(query, "Alias"))
        }
        ("PUT", ["distribution", _, "associate-web-acl"]) => empty_resp(200),
        ("PUT", ["distribution", _, "disassociate-web-acl"]) => empty_resp(200),
        ("POST", ["distribution", _, "copy"]) => {
            h_not_found_distribution()
        }
        ("PUT", ["distribution", _, "promote-staging-config"]) => {
            h_update_distribution_with_staging_config(backend, seg2)
        }
        ("PUT", ["distributions", _, "monitoring-subscription"]) => not_implemented(),
        ("GET" | "POST" | "DELETE", ["distributions", _, "monitoring-subscription"]) => {
            match method {
                "POST" => h_create_monitoring_subscription(backend, seg2, req),
                "GET" => h_get_monitoring_subscription(backend, seg2),
                "DELETE" => h_delete_monitoring_subscription(backend, seg2),
                _ => not_implemented(),
            }
        }
        ("GET", ["distributionsByWebACLId", _]) => {
            h_list_distributions_by_web_acl_id(backend, last)
        }
        ("GET", ["distributionsByCachePolicyId", _]) => distribution_id_list(&[]),
        ("GET", ["distributionsByOriginRequestPolicyId", _]) => {
            distribution_id_list(&[])
        }
        ("GET", ["distributionsByResponseHeadersPolicyId", _]) => distribution_id_list(&[]),
        ("GET", ["distributionsByKeyGroupId", id]) => {
            h_list_distributions_by_key_group(backend, id)
        }
        ("GET", ["distributionsByAnycastIpListId", _]) => distribution_id_list(&[]),
        ("GET", ["distributionsByVpcOriginId", _]) => distribution_id_list(&[]),
        ("GET", ["distributionsByConnectionMode", _]) => distribution_list_empty(),
        ("GET", ["distributionsByOwnedResource", _]) => distribution_list_empty(),
        ("POST", ["distributionsByRealtimeLogConfig"]) => {
            h_list_distributions_by_realtime_log_config(backend, req)
        }
        ("POST", ["distributionsByConnectionFunction"]) => distribution_id_list(&[]),
        ("POST", ["distributionsByTrustStore"]) => distribution_id_list(&[]),
        ("GET", ["conflicting-alias"]) => {
            h_list_conflicting_aliases(backend, account, first(query, "DistributionId"), first(query, "Alias"))
        }
        // ------------------------------------------------ origin access controls
        ("POST", ["origin-access-control"]) => h_create_oac(backend, req),
        ("GET", ["origin-access-control"]) => h_list_oac(backend),
        ("GET", ["origin-access-control", id]) => h_get_oac(backend, id),
        ("GET", ["origin-access-control", _, "config"]) => h_get_oac_config(backend, seg2),
        ("PUT", ["origin-access-control", _, "config"]) => h_update_oac(backend, seg2, req),
        ("DELETE", ["origin-access-control", _]) => h_delete_oac(backend, last),
        // ------------------------------------------------ origin access identities
        ("POST", ["origin-access-identity", "cloudfront"]) => h_create_oai(backend, req),
        ("GET", ["origin-access-identity", "cloudfront"]) => h_list_oais(backend),
        ("GET", ["origin-access-identity", "cloudfront", _]) => h_get_oai(backend, last),
        ("GET", ["origin-access-identity", "cloudfront", _, "config"]) => {
            h_get_oai_config(backend, seg2)
        }
        ("PUT", ["origin-access-identity", "cloudfront", _, "config"]) => {
            h_update_oai(backend, seg2, req)
        }
        ("DELETE", ["origin-access-identity", "cloudfront", _]) => h_delete_oai(backend, last),
        // ------------------------------------------------ public keys
        ("POST", ["public-key"]) => h_create_public_key(backend, req),
        ("GET", ["public-key"]) => h_list_public_keys(backend),
        ("GET", ["public-key", _]) => h_get_public_key(backend, last),
        ("GET", ["public-key", _, "config"]) => h_get_public_key_config(backend, seg2),
        ("PUT", ["public-key", _, "config"]) => h_update_public_key(backend, seg2),
        ("DELETE", ["public-key", _]) => h_delete_public_key(backend, last),
        // ------------------------------------------------ key groups
        ("POST", ["key-group"]) => h_create_key_group(backend, req),
        ("GET", ["key-group"]) => h_list_key_groups(backend),
        ("GET", ["key-group", _]) => h_get_key_group(backend, last),
        ("GET", ["key-group", _, "config"]) => h_get_key_group_config(backend, seg2),
        ("PUT", ["key-group", _]) => h_update_key_group(backend, last, req),
        ("DELETE", ["key-group", _]) => h_delete_key_group(backend, last),
        // ------------------------------------------------ functions
        ("POST", ["function"]) => h_create_function(account, backend, req),
        ("GET", ["function"]) => h_list_functions(backend),
        ("GET", ["function", _]) => h_get_function_sig(backend, last),
        ("GET", ["function", _, "describe"]) => h_describe_function(backend, seg2),
        ("PUT", ["function", _]) => h_update_function(account, backend, req, last),
        ("DELETE", ["function", _]) => h_delete_function(backend, last),
        ("POST", ["function", _, "publish"]) => h_publish_function(backend, seg2),
        ("POST", ["function", _, "test"]) => h_test_function(backend, seg2),
        // ------------------------------------------------ cache policies
        ("POST", ["cache-policy"]) => h_create_cache_policy(backend, req),
        ("GET", ["cache-policy"]) => h_list_cache_policies(backend),
        ("GET", ["cache-policy", _]) => h_get_cache_policy(backend, last),
        ("GET", ["cache-policy", _, "config"]) => h_get_cache_policy_config(backend, seg2),
        ("PUT", ["cache-policy", _]) => h_update_cache_policy(backend, last, req),
        ("DELETE", ["cache-policy", _]) => h_delete_cache_policy(backend, last),
        // ------------------------------------------------ response headers policies
        ("POST", ["response-headers-policy"]) => h_create_rhp(backend, req),
        ("GET", ["response-headers-policy"]) => h_list_rhp(backend),
        ("GET", ["response-headers-policy", _]) => h_get_rhp(backend, last),
        ("GET", ["response-headers-policy", _, "config"]) => h_get_rhp_config(backend, seg2),
        ("PUT", ["response-headers-policy", _]) => h_update_rhp(backend, last, req),
        ("DELETE", ["response-headers-policy", _]) => h_delete_rhp(backend, last),
        // ------------------------------------------------ origin request policies
        ("POST", ["origin-request-policy"]) => h_create_orp(backend, req),
        ("GET", ["origin-request-policy"]) => h_list_orp(backend),
        ("GET", ["origin-request-policy", _]) => h_get_orp(backend, last),
        ("GET", ["origin-request-policy", _, "config"]) => h_get_orp_config(backend, seg2),
        ("PUT", ["origin-request-policy", _]) => h_update_orp(backend, last, req),
        ("DELETE", ["origin-request-policy", _]) => h_delete_orp(backend, last),
        // ------------------------------------------------ field level encryption
        ("POST", ["field-level-encryption"]) => h_create_fle(backend, req),
        ("GET", ["field-level-encryption"]) => h_list_fle(backend),
        ("GET", ["field-level-encryption", _]) => h_get_fle(backend, last),
        ("GET", ["field-level-encryption", _, "config"]) => h_get_fle_config(backend, seg2),
        ("PUT", ["field-level-encryption", _, "config"]) => h_update_fle(backend, seg2, req),
        ("DELETE", ["field-level-encryption", _]) => h_delete_fle(backend, last),
        ("POST", ["field-level-encryption-profile"]) => h_create_flep(backend, req),
        ("GET", ["field-level-encryption-profile"]) => h_list_flep(backend),
        ("GET", ["field-level-encryption-profile", _]) => h_get_flep(backend, last),
        ("GET", ["field-level-encryption-profile", _, "config"]) => {
            h_get_flep_config(backend, seg2)
        }
        ("PUT", ["field-level-encryption-profile", _, "config"]) => {
            h_update_flep(backend, seg2, req)
        }
        ("DELETE", ["field-level-encryption-profile", _]) => h_delete_flep(backend, last),
        // ------------------------------------------------ continuous deployment
        ("POST", ["continuous-deployment-policy"]) => h_create_cdp(backend, req),
        ("GET", ["continuous-deployment-policy"]) => h_list_cdp(backend),
        ("GET", ["continuous-deployment-policy", _]) => h_get_cdp(backend, last),
        ("GET", ["continuous-deployment-policy", _, "config"]) => {
            h_get_cdp_config(backend, seg2)
        }
        ("PUT", ["continuous-deployment-policy", _]) => h_update_cdp(backend, last, req),
        ("DELETE", ["continuous-deployment-policy", _]) => h_delete_cdp(backend, last),
        // ------------------------------------------------ realtime log configs
        ("POST", ["realtime-log-config"]) => h_create_rlc(account, backend, req),
        ("GET", ["realtime-log-config"]) => h_list_rlc(backend),
        ("PUT", ["realtime-log-config"]) => h_update_rlc(account, backend, req),
        ("POST", ["get-realtime-log-config"]) => h_get_rlc(account, backend, req),
        ("POST", ["delete-realtime-log-config"]) => h_delete_rlc(backend, req),
        // ------------------------------------------------ streaming distributions
        ("POST", ["streaming-distribution"]) => h_create_streaming(account, backend, req),
        ("GET", ["streaming-distribution"]) => h_list_streaming(backend),
        ("GET", ["streaming-distribution", _]) => h_get_streaming(backend, last),
        ("GET", ["streaming-distribution", _, "config"]) => {
            h_get_streaming_config(backend, seg2)
        }
        ("PUT", ["streaming-distribution", _, "config"]) => {
            h_update_streaming(backend, seg2, req)
        }
        ("DELETE", ["streaming-distribution", _]) => h_delete_streaming(backend, last),
        // ------------------------------------------------ key value stores
        ("POST", ["key-value-store"]) => h_create_kvs(account, backend, req),
        ("GET", ["key-value-store"]) => h_list_kvs(backend),
        ("GET", ["key-value-store", _]) => h_describe_kvs(backend, last),
        ("PUT", ["key-value-store", _]) => h_update_kvs(backend, last, req),
        ("DELETE", ["key-value-store", _]) => h_delete_kvs(backend, last),
        // ------------------------------------------------ vpc origins
        ("POST", ["vpc-origin"]) => h_create_vpc_origin(account, backend, req),
        ("GET", ["vpc-origin"]) => h_list_vpc_origins(backend),
        ("GET", ["vpc-origin", _]) => h_get_vpc_origin(backend, last),
        ("PUT", ["vpc-origin", _]) => h_update_vpc_origin(backend, last, req),
        ("DELETE", ["vpc-origin", _]) => h_delete_vpc_origin(backend, last),
        // ------------------------------------------------ trust stores
        ("POST", ["trust-store"]) => h_create_trust_store(account, backend, req),
        ("POST", ["trust-stores"]) => h_list_trust_stores(backend),
        ("GET", ["trust-store", _]) => h_get_trust_store(backend, last),
        ("PUT", ["trust-store", _]) => h_update_trust_store(backend, last),
        ("DELETE", ["trust-store", _]) => h_delete_trust_store(backend, last),
        // ------------------------------------------------ anycast ip lists
        ("POST", ["anycast-ip-list"]) => h_create_anycast(account, backend, req),
        ("GET", ["anycast-ip-list"]) => h_list_anycast(backend),
        ("GET", ["anycast-ip-list", _]) => h_get_anycast(backend, last),
        ("PUT", ["anycast-ip-list", _]) => h_get_anycast(backend, last),
        ("DELETE", ["anycast-ip-list", _]) => h_delete_anycast(backend, last),
        // ------------------------------------------------ resource policies
        ("POST", ["get-resource-policy"]) => h_get_resource_policy(),
        ("POST", ["put-resource-policy"]) => h_get_resource_policy(),
        ("POST", ["delete-resource-policy"]) => empty_resp_status(204),
        // ------------------------------------------------ connection groups
        ("POST", ["connection-group"]) => h_create_connection_group(account, backend, req),
        ("POST", ["connection-groups"]) => h_list_connection_groups(backend),
        ("GET", ["connection-group"]) => h_get_connection_group_by_routing_endpoint(),
        ("GET", ["connection-group", _]) => h_get_connection_group(backend, last),
        ("PUT", ["connection-group", _]) => h_update_connection_group(backend, last),
        ("DELETE", ["connection-group", _]) => h_delete_connection_group(backend, last),
        // ------------------------------------------------ connection functions
        ("POST", ["connection-function"]) => h_create_connection_function(account, backend, req),
        ("POST", ["connection-functions"]) => h_list_connection_functions(backend),
        ("GET", ["connection-function", _]) => h_get_connection_function(backend, last),
        ("GET", ["connection-function", _, "describe"]) => {
            h_get_connection_function(backend, seg2)
        }
        ("PUT", ["connection-function", _]) => h_update_connection_function(backend, last),
        ("DELETE", ["connection-function", _]) => h_delete_connection_function(backend, last),
        ("POST", ["connection-function", _, "publish"]) => {
            h_publish_connection_function(backend, seg2)
        }
        ("POST", ["connection-function", _, "test"]) => h_test_connection_function(),
        // ------------------------------------------------ distribution tenants
        ("POST", ["distribution-tenant"]) => h_create_tenant(account, backend, req),
        ("POST", ["distribution-tenants"]) => h_list_tenants(backend),
        ("GET", ["distribution-tenant"]) => h_get_tenant_by_domain(),
        ("GET", ["distribution-tenant", _]) => h_get_tenant(backend, last),
        ("PUT", ["distribution-tenant", _]) => h_update_tenant(backend, last),
        ("DELETE", ["distribution-tenant", _]) => h_delete_tenant(backend, last),
        ("GET" | "POST", ["distribution-tenant", _, "invalidation"]) => {
            if method == "POST" {
                h_create_tenant_invalidation()
            } else {
                h_list_tenant_invalidations()
            }
        }
        ("GET", ["distribution-tenant", _, "invalidation", _]) => {
            h_get_tenant_invalidation(so(segs.len() - 1))
        }
        ("POST", ["distribution-tenants-by-customization"]) => h_list_tenants(backend),
        // ------------------------------------------------ domain + misc stubs
        ("POST", ["domain-conflicts"]) => resp(
            200,
            format!("<?xml version=\"1.0\"?>\n<DomainConflicts xmlns=\"{XMLNS}\"></DomainConflicts>"),
        ),
        ("POST", ["domain-association"]) => empty_resp(200),
        ("POST", ["verify-dns-configuration"]) => resp(
            200,
            format!("<?xml version=\"1.0\"?>\n<VerifyDnsConfigurationResult xmlns=\"{XMLNS}\"></VerifyDnsConfigurationResult>"),
        ),
        ("GET", ["managed-certificate", _]) => resp(
            200,
            format!(
                "<?xml version=\"1.0\"?>\n<ManagedCertificateDetails xmlns=\"{XMLNS}\">{issued}</ManagedCertificateDetails>",
                issued = wrap("CertificateStatus", "ISSUED")
            ),
        ),
        _ => not_implemented(),
    }
}

// ---------------------------------------------------------------------------
// Small query/table helpers
// ---------------------------------------------------------------------------

fn first<'a>(query: &'a BTreeMap<String, Vec<String>>, key: &str) -> Option<&'a str> {
    query.get(key).and_then(|v| v.first()).map(String::as_str)
}

fn body_xml(req: &Request) -> El {
    parse_xml(&req.body).unwrap_or_default()
}

/// Resolves the handler's config element: botocore wraps multi-member inputs
/// in `<XxxRequest>`, single-member payloads arrive bare.
fn config_of<'a>(root: &'a El, wrapper: &str) -> &'a El {
    root.child(wrapper).unwrap_or(root)
}

/// Extract the create/update config element and, for the `WithTags` variants,
/// the caller's tag list. Mirrors moto's `create_distribution` parsing.
fn config_and_tags<'a>(
    root: &'a El,
    config_member: &str,
) -> (&'a El, Vec<(String, String)>) {
    let with_tags_root = if root.name == "DistributionConfigWithTags" {
        Some(root)
    } else {
        root.child("DistributionConfigWithTags")
    };
    if let Some(with_tags) = with_tags_root {
        let tags = with_tags
            .sub(&["Tags", "Items"])
            .map(|items| {
                items
                    .children
                    .iter()
                    .filter(|c| c.name == "Tag")
                    .map(|tag| {
                        (
                            tag.text_of("Key").unwrap_or_default().to_string(),
                            tag.text_of("Value").unwrap_or_default().to_string(),
                        )
                    })
                    .collect()
            })
            .unwrap_or_default();
        let config = root
            .sub(&["DistributionConfigWithTags", "DistributionConfig"])
            .unwrap_or(root);
        return (config, tags);
    }
    let streaming_root = if root.name == "StreamingDistributionConfigWithTags" {
        Some(root)
    } else {
        root.child("StreamingDistributionConfigWithTags")
    };
    if let Some(with_tags) = streaming_root {
        let tags = with_tags
            .sub(&["Tags", "Items"])
            .map(|items| {
                items
                    .components("Tag")
                    .map(|tag| {
                        (
                            tag.text_of("Key").unwrap_or_default().to_string(),
                            tag.text_of("Value").unwrap_or_default().to_string(),
                        )
                    })
                    .collect()
            })
            .unwrap_or_default();
        let config = with_tags.child("StreamingDistributionConfig").unwrap_or(root);
        return (config, tags);
    }
    let bare = if root.name == config_member {
        root
    } else {
        root.child(config_member).unwrap_or(root)
    };
    (bare, Vec::new())
}

fn xml_decl(body: &str) -> String {
    format!("<?xml version=\"1.0\"?>\n{body}")
}

/// `<Tags><Items><Tag>...` doc used by ListTagsForResource.
fn render_tags(backend: &Backend, arn: &str) -> String {
    let mut items = String::new();
    if let Some(tags) = backend.tags.get(arn) {
        for (key, value) in tags {
            items += &wrap("Tag", el("Key", key) + &el("Value", value));
        }
    }
    wrap("Tags", &wrap("Items", &items))
}

// ---------------------------------------------------------------------------
// Distributions
// ---------------------------------------------------------------------------

fn distribution_exists_with_ref<'a>(backend: &'a Backend, caller_reference: &str) -> Option<&'a Distribution> {
    backend
        .distributions
        .iter()
        .find(|d| caller_reference_of(&d.config).as_deref() == Some(caller_reference))
}

fn caller_reference_of(config: &El) -> Option<String> {
    config.text_of("CallerReference").map(str::to_string)
}

fn new_distribution(account: u64, config: El) -> Result<Distribution, Response> {
    // Validate the default target origin (moto's OriginDoesNotExist) and the
    // S3-domain shape for S3 origins (moto's DomainNameNotAnS3Bucket).
    let target = config
        .sub(&["DefaultCacheBehavior", "TargetOriginId"])
        .map(El::as_str)
        .unwrap_or("");
    let origin_ids = config.texts_at(&["Origins", "Items"], "Id");
    if !origin_ids.is_empty() && !origin_ids.iter().any(|i| i == target) {
        return Err(xml_error(
            404,
            "NoSuchOrigin",
            "One or more of your origins or origin groups do not exist.",
        ));
    }
    for origin in config.els_at(&["Origins", "Items"], "Origin") {
        if origin.child("S3OriginConfig").is_some()
            && !origin
                .text_of("DomainName")
                .unwrap_or("")
                .ends_with("amazonaws.com")
        {
            return Err(xml_error(
                400,
                "InvalidArgument",
                "The parameter Origin DomainName does not refer to a valid S3 bucket.",
            ));
        }
    }

    let id = random_upper(13);
    let arn = format!("arn:aws:cloudfront:{account}:distribution/{id}");
    let domain_name = format!("{}.cloudfront.net", random_domain(13));
    Ok(Distribution {
        id,
        arn,
        status: "InProgress".to_string(),
        last_modified_time: "2021-11-27T10:34:26.802Z".to_string(),
        domain_name,
        etag: random_upper(13),
        caller_reference: config
            .text_of("CallerReference")
            .unwrap_or(&uuid::Uuid::new_v4().to_string())
            .to_string(),
        aliases: config.texts_at(&["Aliases", "Items"], "CNAME"),
        web_acl_id: config.text_of("WebACLId").unwrap_or("").to_string(),
        config,
    })
}

fn save_tags(backend: &mut Backend, arn: &str, tags: Vec<(String, String)>) {
    if tags.is_empty() {
        return;
    }
    let entry = backend.tags.entry(arn.to_string()).or_default();
    for (key, value) in tags {
        entry.insert(key, value);
    }
}

fn h_create_distribution(account: u64, backend: &mut Backend, req: &Request) -> Response {
    let root = body_xml(req);
    let (config_el, tags) = config_and_tags(&root, "DistributionConfig");
    let caller_reference = config_el
        .text_of("CallerReference")
        .unwrap_or(&uuid::Uuid::new_v4().to_string())
        .to_string();
    if distribution_exists_with_ref(backend, &caller_reference).is_some() {
        let existing = distribution_exists_with_ref(backend, &caller_reference)
            .map(|d| d.id.clone())
            .unwrap_or_default();
        return xml_error(
            400,
            "DistributionAlreadyExists",
            &format!("The caller reference that you are using to create a distribution is associated with another distribution. Already exists: {existing}"),
        );
    }
    let dist = match new_distribution(account, config_el.clone()) {
        Ok(d) => d,
        Err(resp) => return resp,
    };
    save_tags(backend, &dist.arn, tags);
    let etag = dist.etag.clone();
    let location =
        format!("https://cloudfront.amazonaws.com/2020-05-31/distribution/{}", dist.id);
    let body = render_dist_doc(&dist, "CreateDistributionResult");
    backend.distributions.push(dist);
    resp_with(200, vec![("ETag", etag), ("Location", location)], xml_decl(&body))
}

fn h_list_distributions(backend: &mut Backend) -> Response {
    for dist in &mut backend.distributions {
        dist.advance();
    }
    let mut items = String::new();
    for dist in &backend.distributions {
        items += &wrap("DistributionSummary", &render_dist_entry(dist));
    }
    let items_block = if backend.distributions.is_empty() {
        String::new()
    } else {
        wrap("Items", &items)
    };
    let body = format!(
        "<DistributionList xmlns=\"{XMLNS}\"><Marker></Marker><MaxItems>100</MaxItems><IsTruncated>false</IsTruncated><Quantity>{}</Quantity>{items_block}</DistributionList>",
        backend.distributions.len()
    );
    resp(200, xml_decl(&body))
}

fn h_get_distribution(backend: &mut Backend, id: &str) -> Response {
    match tokenized_distribution(backend, id) {
        Some(dist) => {
            let etag = dist.etag.clone();
            dist.advance();
            let body = render_dist_doc(dist, "Distribution");
            resp_with(200, vec![("ETag", etag)], xml_decl(&body))
        }
        None => xml_error(
            404,
            "NoSuchDistribution",
            "The specified distribution does not exist.",
        ),
    }
}

/// Borrow helper: advance + keep reference (moto's `dist.advance()`).
fn tokenized_distribution<'a>(backend: &'a mut Backend, id: &str) -> Option<&'a mut Distribution> {
    let len = backend.distributions.len();
    for i in 0..len {
        if backend.distributions[i].id == id {
            return Some(&mut backend.distributions[i]);
        }
    }
    None
}

fn h_get_distribution_config(backend: &mut Backend, id: &str) -> Response {
    match tokenized_distribution(backend, id) {
        Some(dist) => {
            dist.advance();
            let etag = dist.etag.clone();
            let body = wrap("DistributionConfig", &render_dist_config(dist));
            resp_with(200, vec![("ETag", etag)], xml_decl(&body))
        }
        None => xml_error(
            404,
            "NoSuchDistribution",
            "The specified distribution does not exist.",
        ),
    }
}

fn h_delete_distribution(backend: &mut Backend, id: &str, if_match: &str) -> Response {
    if if_match.trim().is_empty() {
        return xml_error(
            400,
            "InvalidIfMatchVersion",
            "The If-Match version is missing or not valid for the resource.",
        );
    }
    match backend.distributions.iter().position(|d| d.id == id) {
        Some(pos) => {
            backend.distributions.remove(pos);
            empty_resp(204)
        }
        None => xml_error(
            404,
            "NoSuchDistribution",
            "The specified distribution does not exist.",
        ),
    }
}

fn h_not_found_distribution() -> Response {
    xml_error(
        404,
        "NoSuchDistribution",
        "The specified distribution does not exist.",
    )
}

fn h_update_distribution(
    account: u64,
    backend: &mut Backend,
    req: &Request,
    id: &str,
    if_match: &str,
) -> Response {
    if if_match.trim().is_empty() {
        return xml_error(
            400,
            "InvalidIfMatchVersion",
            "The If-Match version is missing or not valid for the resource.",
        );
    }
    if tokenized_distribution(backend, id).is_none() {
        return h_not_found_distribution();
    }
    let root = body_xml(req);
    let (config_el, _) = config_and_tags(&root, "DistributionConfig");
    let mut new_dist = match new_distribution(account, config_el.clone()) {
        Ok(d) => d,
        Err(resp) => return resp,
    };
    let dist = tokenized_distribution(backend, id).expect("checked above");
    new_dist.id = dist.id.clone();
    new_dist.arn = dist.arn.clone();
    new_dist.etag = dist.etag.clone();
    new_dist.status = dist.status.clone();
    new_dist.last_modified_time = dist.last_modified_time.clone();
    new_dist.domain_name = dist.domain_name.clone();
    *dist = new_dist;
    let body = render_dist_doc(dist, "Distribution");
    let etag = dist.etag.clone();
    let location = format!("https://cloudfront.amazonaws.com/2020-05-31/distribution/{id}");
    resp_with(
        200,
        vec![("ETag", etag), ("Location", location)],
        xml_decl(&body),
    )
}

fn h_update_distribution_with_staging_config(backend: &mut Backend, id: &str) -> Response {
    match tokenized_distribution(backend, id) {
        Some(dist) => {
            dist.advance();
            let etag = dist.etag.clone();
            let body = render_dist_doc(dist, "Distribution");
            resp_with(200, vec![("ETag", etag)], xml_decl(&body))
        }
        None => h_not_found_distribution(),
    }
}

// ---------------------------------------------------------------------------
// Distribution rendering (mirrors moto's DIST_* templates)
// ---------------------------------------------------------------------------

fn render_dist_doc(dist: &Distribution, wrapper: &str) -> String {
    format!(
        "<{wrapper} xmlns=\"{XMLNS}\">{inner}</{wrapper}>",
        inner = render_dist_inner(dist)
    )
}

fn render_dist_inner(dist: &Distribution) -> String {
    render_dist_meta(dist)
        + ACTIVETRUSTED_BLOCK
        + &wrap("DistributionConfig", &render_dist_config(dist))
        + "<AliasICPRecordals></AliasICPRecordals>"
}

fn render_dist_entry(dist: &Distribution) -> String {
    render_dist_meta(dist) + &render_dist_config(dist)
}

const ACTIVETRUSTED_BLOCK: &str = "<ActiveTrustedSigners><Enabled>False</Enabled><Quantity>0</Quantity><Items></Items></ActiveTrustedSigners>\
<ActiveTrustedKeyGroups><Enabled>False</Enabled><Quantity>0</Quantity><Items></Items></ActiveTrustedKeyGroups>";

fn render_dist_meta(dist: &Distribution) -> String {
    el("Id", &dist.id)
        + &el("ARN", &dist.arn)
        + &el("Status", &dist.status)
        + &el("LastModifiedTime", &dist.last_modified_time)
        + "<InProgressInvalidationBatches>0</InProgressInvalidationBatches>"
        + &el("DomainName", &dist.domain_name)
}

/// Text or the moto-style `None` fallback for policy-id fields.
fn policy_id_text(behavior: &El, name: &str) -> String {
    match behavior.text_of(name) {
        Some("") | None => "None".to_string(),
        Some(v) => v.to_string(),
    }
}

fn lower_yes_no(v: &str) -> &'static str {
    if v.eq_ignore_ascii_case("true") {
        "true"
    } else {
        "false"
    }
}

fn render_dist_config(dist: &Distribution) -> String {
    let config = &dist.config;
    let mut out = String::new();

    out += &el("CallerReference", &dist.caller_reference);

    // Aliases
    let mut alias_items = String::new();
    for alias in &dist.aliases {
        alias_items += &wrap("CNAME", alias);
    }
    out += &wrap(
        "Aliases",
        el("Quantity", &dist.aliases.len().to_string()) + &wrap("Items", &alias_items),
    );

    out += &el("DefaultRootObject", &config.text_or("DefaultRootObject", ""));

    // Origins
    let origins = config.els_at(&["Origins", "Items"], "Origin");
    let mut origin_items = String::new();
    for origin in &origins {
        origin_items += &wrap("Origin", &render_origin(origin));
    }
    out += &wrap(
        "Origins",
        el("Quantity", &origins.len().to_string()) + &wrap("Items", &origin_items),
    );

    out += "<OriginGroups><Quantity>0</Quantity></OriginGroups>";

    // Default cache behavior
    if let Some(behavior) = config.child("DefaultCacheBehavior") {
        out += &wrap("DefaultCacheBehavior", &render_behavior(behavior, true));
    }

    // Cache behaviors
    let behaviors = config.els_at(&["CacheBehaviors", "Items"], "CacheBehavior");
    let mut behavior_items = String::new();
    for (i, behavior) in behaviors.iter().enumerate() {
        let _ = i;
        behavior_items += &wrap("CacheBehavior", &render_behavior(behavior, false));
    }
    out += &wrap(
        "CacheBehaviors",
        el("Quantity", &behaviors.len().to_string())
            + &if behaviors.is_empty() {
                String::new()
            } else {
                wrap("Items", &behavior_items)
            },
    );

    // Custom error responses (moto never parses them)
    out += "<CustomErrorResponses><Quantity>0</Quantity></CustomErrorResponses>";

    out += &el("Comment", &config.text_or("Comment", ""));

    // Logging
    let logging = config.child("Logging");
    out += &wrap(
        "Logging",
        &format!(
            "{enabled}{cookies}{bucket}{prefix}",
            enabled = el(
                "Enabled",
                logging.and_then(|l| l.text_of("Enabled")).unwrap_or("False")
            ),
            cookies = el(
                "IncludeCookies",
                logging
                    .and_then(|l| l.text_of("IncludeCookies"))
                    .unwrap_or("False")
            ),
            bucket = el(
                "Bucket",
                logging.and_then(|l| l.text_of("Bucket")).unwrap_or("")
            ),
            prefix = el(
                "Prefix",
                logging.and_then(|l| l.text_of("Prefix")).unwrap_or("")
            ),
        ),
    );

    out += &el("PriceClass", &config.text_or("PriceClass", "PriceClass_All"));

    out += &el(
        "Enabled",
        config.text_of("Enabled").unwrap_or("False"),
    );

    // ViewerCertificate
    let cert = config.child("ViewerCertificate");
    let default_cert = match cert.and_then(|c| c.text_of("CloudFrontDefaultCertificate")) {
        None => "true",
        Some(text) => lower_yes_no(text),
    };
    out += &wrap(
        "ViewerCertificate",
        &format!(
            "<CloudFrontDefaultCertificate>{default_cert}</CloudFrontDefaultCertificate>{iam}{acm}{ssl}{min}{cert_el}{source}",
            iam = el(
                "IAMCertificateId",
                cert.and_then(|c| c.text_of("IAMCertificateId")).unwrap_or("")
            ),
            acm = el(
                "ACMCertificateArn",
                cert.and_then(|c| c.text_of("ACMCertificateArn")).unwrap_or("")
            ),
            ssl = el(
                "SSLSupportMethod",
                cert.and_then(|c| c.text_of("SSLSupportMethod")).unwrap_or("sni-only")
            ),
            min = el(
                "MinimumProtocolVersion",
                cert.and_then(|c| c.text_of("MinimumProtocolVersion"))
                    .unwrap_or("TLSv1")
            ),
            cert_el = el(
                "Certificate",
                cert.and_then(|c| c.text_of("Certificate")).unwrap_or("")
            ),
            source = wrap("CertificateSource", "cloudfront"),
        ),
    );

    // Restrictions
    let geo = config.sub(&["Restrictions", "GeoRestriction"]);
    let locations: Vec<String> = geo
        .map(|g| g.texts_at(&["Items"], "Location"))
        .unwrap_or_default();
    let mut geo_items = String::new();
    for location in &locations {
        geo_items += &wrap("Location", location);
    }
    out += &wrap(
        "Restrictions",
        &wrap(
            "GeoRestriction",
            el(
                "RestrictionType",
                geo.and_then(|g| g.text_of("RestrictionType")).unwrap_or("none")
            ) + &el("Quantity", &locations.len().to_string())
                + &if locations.is_empty() {
                    String::new()
                } else {
                    wrap("Items", &geo_items)
                },
        ),
    );

    out += &el("WebACLId", &config.text_or("WebACLId", ""));
    out += &el("HttpVersion", &config.text_or("HttpVersion", "http2"));
    out += &el(
        "IsIPV6Enabled",
        lower_yes_no(config.text_of("IsIPV6Enabled").unwrap_or("true")),
    );

    out
}

fn render_origin(origin: &El) -> String {
    let mut out = el("Id", &origin.text_or("Id", ""));
    out += &el("DomainName", &origin.text_or("DomainName", ""));
    out += &el("OriginPath", &origin.text_or("OriginPath", ""));

    // Custom headers
    let headers = origin.els_at(&["CustomHeaders", "Items"], "OriginCustomHeader");
    let mut header_items = String::new();
    for header in &headers {
        header_items += &wrap(
            "OriginCustomHeader",
            el("HeaderName", &header.text_or("HeaderName", ""))
                + &el("HeaderValue", &header.text_or("HeaderValue", "")),
        );
    }
    out += &wrap(
        "CustomHeaders",
        el("Quantity", &headers.len().to_string()) + &wrap("Items", &header_items),
    );

    if origin.child("S3OriginConfig").is_some() {
        out += &wrap(
            "S3OriginConfig",
            &el(
                "OriginAccessIdentity",
                &origin
                    .sub(&["S3OriginConfig", "OriginAccessIdentity"])
                    .map(El::as_str)
                    .unwrap_or(""),
            ),
        );
    }

    if let Some(custom) = origin.child("CustomOriginConfig") {
        let protocols: Vec<String> = custom.texts_at(&["OriginSslProtocols", "Items"], "SslProtocol");
        let mut protocol_items = String::new();
        for protocol in &protocols {
            protocol_items += &wrap("SslProtocol", protocol);
        }
        out += &wrap(
            "CustomOriginConfig",
            el("HTTPPort", &custom.text_or("HTTPPort", "None"))
                + &el("HTTPSPort", &custom.text_or("HTTPSPort", "None"))
                + &el(
                    "OriginProtocolPolicy",
                    &custom.text_or("OriginProtocolPolicy", "None")
                )
                + &wrap(
                    "OriginSslProtocols",
                    el("Quantity", &protocols.len().to_string())
                        + &wrap("Items", &protocol_items),
                )
                + &el("OriginReadTimeout", &custom.text_or("OriginReadTimeout", "30"))
                + &el(
                    "OriginKeepaliveTimeout",
                    &custom.text_or("OriginKeepaliveTimeout", "5"),
                ),
        );
    }

    out += &el("ConnectionAttempts", &origin.text_or("ConnectionAttempts", "3"));
    out += &el("ConnectionTimeout", &origin.text_or("ConnectionTimeout", "10"));

    out += &match origin.child("OriginShield") {
        Some(shield) => {
            let inner = el("Enabled", &shield.text_or("Enabled", "false"))
                + &el(
                    "OriginShieldRegion",
                    &shield.text_or("OriginShieldRegion", ""),
                );
            wrap("OriginShield", &inner)
        }
        None => wrap("OriginShield", "<Enabled>false</Enabled>"),
    };

    out
}

fn is_true(text: Option<&str>) -> bool {
    matches!(text, Some(t) if t.eq_ignore_ascii_case("true"))
}

fn render_behavior(behavior: &El, default_cache_behavior: bool) -> String {
    let mut out = String::new();
    if !default_cache_behavior {
        out += &el("PathPattern", &behavior.text_or("PathPattern", ""));
    }
    out += &el("TargetOriginId", &behavior.text_or("TargetOriginId", ""));

    // Trusted signers
    let signers = behavior.els_at(&["TrustedSigners", "Items"], "AwsAccountNumber");
    let mut signer_items = String::new();
    for signer in &signers {
        signer_items += &wrap("AwsAccountNumber", signer.as_str());
    }
    out += &wrap(
        "TrustedSigners",
        el(
            "Enabled",
            if signers.is_empty() { "false" } else { "true" },
        ) + &el("Quantity", &signers.len().to_string())
            + &wrap("Items", &signer_items),
    );

    // Trusted key groups
    let groups = behavior.els_at(&["TrustedKeyGroups", "Items"], "KeyGroup");
    let mut group_items = String::new();
    for group in &groups {
        group_items += &wrap("KeyGroup", group.as_str());
    }
    out += &wrap(
        "TrustedKeyGroups",
        el("Enabled", if groups.is_empty() { "false" } else { "true" })
            + &el("Quantity", &groups.len().to_string())
            + &wrap("Items", &group_items),
    );

    out += &el(
        "ViewerProtocolPolicy",
        &behavior.text_or("ViewerProtocolPolicy", ""),
    );

    // Allowed methods (defaults differ for the default cache behavior)
    let methods: Vec<String> = behavior.texts_at(&["AllowedMethods", "Items"], "Method");
    let default_methods: &[&str] = if default_cache_behavior {
        &["HEAD", "GET"]
    } else {
        &[]
    };
    let default_cached: &[&str] = if default_cache_behavior {
        &["GET", "HEAD"]
    } else {
        &[]
    };
    let cached = behavior.texts_at(&["AllowedMethods", "CachedMethods", "Items"], "Method");
    let cached = if cached.is_empty() {
        default_cached.iter().map(|s| s.to_string()).collect::<Vec<_>>()
    } else {
        cached
    };
    let methods = if methods.is_empty() {
        default_methods.iter().map(|s| s.to_string()).collect::<Vec<_>>()
    } else {
        methods
    };
    let mut method_items = String::new();
    for method in &methods {
        method_items += &wrap("Method", method);
    }
    let mut cached_items = String::new();
    for method in &cached {
        cached_items += &wrap("Method", method);
    }
    out += &wrap(
        "AllowedMethods",
        el("Quantity", &methods.len().to_string())
            + &wrap("Items", &method_items)
            + &wrap(
                "CachedMethods",
                el("Quantity", &cached.len().to_string()) + &wrap("Items", &cached_items),
            ),
    );

    out += "<SmoothStreaming>True</SmoothStreaming>";
    // moto renders lowercase `true`/`false` on the default cache behavior but
    // the raw Python bool (`True`/`False`) on additional cache behaviors.
    let compress_on = behavior
        .text_of("Compress")
        .map(|t| t.eq_ignore_ascii_case("true"))
        .unwrap_or(true);
    out += &if default_cache_behavior {
        el(
            "Compress",
            if compress_on { "true" } else { "false" },
        )
    } else {
        el(
            "Compress",
            if compress_on { "True" } else { "False" },
        )
    };

    out += "<LambdaFunctionAssociations><Quantity>0</Quantity></LambdaFunctionAssociations>";
    out += "<FunctionAssociations><Quantity>0</Quantity></FunctionAssociations>";

    out += &el(
        "FieldLevelEncryptionId",
        &behavior.text_or("FieldLevelEncryptionId", ""),
    );
    out += &el(
        "RealtimeLogConfigArn",
        &behavior.text_or("RealtimeLogConfigArn", ""),
    );
    if default_cache_behavior {
        out += &el("CachePolicyId", &policy_id_text(behavior, "CachePolicyId"));
        out += &el(
            "OriginRequestPolicyId",
            &policy_id_text(behavior, "OriginRequestPolicyId"),
        );
        out += &el(
            "ResponseHeadersPolicyId",
            &policy_id_text(behavior, "ResponseHeadersPolicyId"),
        );
    } else {
        out += &el("CachePolicyId", &behavior.text_or("CachePolicyId", ""));
        out += &el(
            "OriginRequestPolicyId",
            &behavior.text_or("OriginRequestPolicyId", ""),
        );
        out += &el(
            "ResponseHeadersPolicyId",
            &behavior.text_or("ResponseHeadersPolicyId", ""),
        );
    }

    out += &render_forwarded_values(behavior);

    out += &el("MinTTL", &behavior.text_or("MinTTL", "0"));
    out += &el("DefaultTTL", &behavior.text_or("DefaultTTL", "0"));
    out += &el("MaxTTL", &behavior.text_or("MaxTTL", "0"));

    out
}

fn render_forwarded_values(behavior: &El) -> String {
    let forwarded = behavior.child("ForwardedValues");
    let mut forwarded_out = el(
        "QueryString",
        forwarded
            .and_then(|fv| fv.text_of("QueryString"))
            .unwrap_or("false"),
    );
    let cookies = forwarded.and_then(|fv| fv.child("Cookies"));
    let whitelisted: Vec<&El> = forwarded
        .as_ref()
        .map(|fv| fv.els_at(&["Cookies", "WhitelistedNames", "Items"], "Name"))
        .unwrap_or_default();
    let mut white_items = String::new();
    for name in &whitelisted {
        white_items += &wrap("Name", name.as_str());
    }
    forwarded_out += &wrap(
        "Cookies",
        el(
            "Forward",
            cookies
                .and_then(|c| c.text_of("Forward"))
                .unwrap_or("none"),
        ) + &wrap(
            "WhitelistedNames",
            el("Quantity", &whitelisted.len().to_string())
                + &wrap("Items", &white_items),
        ),
    );
    forwarded_out += "<Headers><Quantity>0</Quantity><Items></Items></Headers>";
    forwarded_out += "<QueryStringCacheKeys><Quantity>0</Quantity><Items></Items></QueryStringCacheKeys>";
    wrap("ForwardedValues", &forwarded_out)
}

// ---------------------------------------------------------------------------
// Invalidations
// ---------------------------------------------------------------------------

fn h_create_invalidation(backend: &mut Backend, dist_id: &str, req: &Request) -> Response {
    if tokenized_distribution(backend, dist_id).is_none() {
        return xml_error(
            404,
            "NoSuchDistribution",
            "The specified distribution does not exist.",
        );
    }
    let root = body_xml(req);
    let batch = config_of(&root, "InvalidationBatch");
    let paths = batch.texts_at(&["Paths", "Items"], "Path");
    let caller_ref = batch.text_of("CallerReference").unwrap_or("").to_string();
    let invalidation = Invalidation {
        id: random_upper(13),
        dist_id: dist_id.to_string(),
        status: "COMPLETED".to_string(),
        create_time: now_millis_iso(),
        paths,
        caller_ref,
    };
    let mut location = format!("https://cloudfront.amazonaws.com/2020-05-31/distribution/{dist_id}");
    location
        .push_str(&format!("/invalidation/{}", invalidation.id));
    backend
        .invalidations
        .entry(dist_id.to_string())
        .or_default()
        .push(invalidation);
    let inv = backend
        .invalidations
        .get(dist_id)
        .and_then(|list| list.last())
        .expect("just inserted");
    let body = render_invalidation(inv);
    resp_with(200, vec![("Location", location)], xml_decl(&body))
}

fn render_invalidation(inv: &Invalidation) -> String {
    let mut paths = String::new();
    for path in &inv.paths {
        paths += &wrap("Path", path);
    }
    wrap(
        "Invalidation",
        el("Id", &inv.id)
            + &el("Status", &inv.status)
            + &el("CreateTime", &inv.create_time)
            + &wrap(
                "InvalidationBatch",
                el("CallerReference", &inv.caller_ref)
                    + &wrap(
                        "Paths",
                        el("Quantity", &inv.paths.len().to_string())
                            + &wrap("Items", &paths),
                    ),
            ),
    )
}

fn h_list_invalidations(backend: &mut Backend, dist_id: &str) -> Response {
    let invalidations: Vec<Invalidation> = backend
        .invalidations
        .get(dist_id)
        .map(|list| list.iter().map(|i| i.clone()).collect())
        .unwrap_or_default();
    let mut items = String::new();
    for inv in &invalidations {
        items += &wrap(
            "InvalidationSummary",
            el("CreateTime", &inv.create_time) + &el("Id", &inv.id) + &el("Status", &inv.status),
        );
    }
    let items_block = if invalidations.is_empty() {
        String::new()
    } else {
        wrap("Items", &items)
    };
    let body = wrap(
        "InvalidationList",
        wrap("IsTruncated", "false")
            + &items_block
            + "<Marker></Marker><MaxItems>100</MaxItems>"
            + &el("Quantity", &invalidations.len().to_string()),
    );
    resp(200, xml_decl(&body))
}

fn h_get_invalidation(backend: &mut Backend, dist_id: &str, inv_id: &str) -> Response {
    if tokenized_distribution(backend, dist_id).is_none() {
        return xml_error(
            404,
            "NoSuchDistribution",
            "The specified distribution does not exist.",
        );
    }
    match backend
        .invalidations
        .get(dist_id)
        .cloned()
        .unwrap_or_default()
        .into_iter()
        .find(|inv| inv.id == inv_id)
    {
        Some(inv) => resp(200, xml_decl(&render_invalidation(&inv))),
        None => xml_error(
            404,
            "NoSuchInvalidation",
            "The specified invalidation does not exist.",
        ),
    }
}

// ---------------------------------------------------------------------------
// Tags
// ---------------------------------------------------------------------------

fn handle_tagging(req: &Request, backend: &mut Backend, query: &BTreeMap<String, Vec<String>>) -> Response {
    let resource = first(query, "Resource").unwrap_or("");
    match (req.method.as_str(), first(query, "Operation")) {
        ("POST", Some("Tag")) => {
            let root = body_xml(req);
            let tags = root
                .sub(&["Items"])
                .map(|items| {
                    items
                        .components("Tag")
                        .map(|tag| {
                            (
                                tag.text_of("Key").unwrap_or_default().to_string(),
                                tag.text_of("Value").unwrap_or_default().to_string(),
                            )
                        })
                        .collect()
                })
                .unwrap_or_default();
            save_tags(backend, resource, tags);
            empty_resp(200)
        }
        ("POST", Some("Untag")) => {
            let root = body_xml(req);
            let keys = root.texts_at(&["Items"], "Key");
            if let Some(tags) = backend.tags.get_mut(resource) {
                for key in keys {
                    tags.remove(&key);
                }
            }
            empty_resp(200)
        }
        (_, _) => resp(200, xml_decl(&render_tags(backend, resource))),
    }
}

// ---------------------------------------------------------------------------
// Origin access controls
// ---------------------------------------------------------------------------

fn h_create_oac(backend: &mut Backend, req: &Request) -> Response {
    let root = body_xml(req);
    let config = config_of(&root, "OriginAccessControlConfig");
    let control = OriginAccessControl {
        id: random_upper(13),
        name: config.text_of("Name").unwrap_or_default().to_string(),
        description: config.text_of("Description").unwrap_or_default().to_string(),
        signing_protocol: config.text_of("SigningProtocol").unwrap_or_default().to_string(),
        signing_behaviour: config
            .text_of("SigningBehavior")
            .unwrap_or_default()
            .to_string(),
        origin_type: config
            .text_of("OriginAccessControlOriginType")
            .unwrap_or_default()
            .to_string(),
        etag: random_upper(13),
    };
    backend.origin_access_controls.push(control);
    let control = backend.origin_access_controls.last().expect("just inserted");
    resp(200, xml_decl(&wrap("OriginAccessControl", &render_oac(control))))
}

fn oac_config_no_id(control: &OriginAccessControl) -> String {
    let mut config_out = el("Name", &control.name);
    if !control.description.is_empty() {
        config_out += &el("Description", &control.description);
    }
    config_out += &el("SigningProtocol", &control.signing_protocol);
    config_out += &el("SigningBehavior", &control.signing_behaviour);
    config_out += &el("OriginAccessControlOriginType", &control.origin_type);
    config_out
}

fn render_oac(control: &OriginAccessControl) -> String {
    el("Id", &control.id)
        + &wrap("OriginAccessControlConfig", &oac_config_no_id(control))
}

fn h_get_oac(backend: &mut Backend, id: &str) -> Response {
    match backend.origin_access_controls.iter().find(|c| c.id == id) {
        Some(control) => resp_with(
            200,
            vec![("ETag", control.etag.clone())],
            xml_decl(&wrap("OriginAccessControl", &render_oac(control))),
        ),
        None => xml_error(
            404,
            "NoSuchOriginAccessControl",
            "The specified origin access control does not exist.",
        ),
    }
}

fn h_update_oac(backend: &mut Backend, id: &str, req: &Request) -> Response {
    let root = body_xml(req);
    let config_el = config_of(&root, "OriginAccessControlConfig");
    match backend.origin_access_controls.iter_mut().find(|c| c.id == id) {
        Some(control) => {
            control.update(config_el);
            let etag = control.etag.clone();
            let body = render_oac(control);
            resp_with(
                200,
                vec![("ETag", etag)],
                xml_decl(&wrap("OriginAccessControl", &body)),
            )
        }
        None => xml_error(
            404,
            "NoSuchOriginAccessControl",
            "The specified origin access control does not exist.",
        ),
    }
}

fn h_delete_oac(backend: &mut Backend, id: &str) -> Response {
    match backend.origin_access_controls.iter().position(|c| c.id == id) {
        Some(pos) => {
            backend.origin_access_controls.remove(pos);
            resp(200, "{}")
        }
        None => xml_error(
            404,
            "NoSuchOriginAccessControl",
            "The specified origin access control does not exist.",
        ),
    }
}


// ---------------------------------------------------------------------------
// Origin access identities (legacy OAI)
// ---------------------------------------------------------------------------

fn h_create_oai(backend: &mut Backend, req: &Request) -> Response {
    let root = body_xml(req);
    let config = config_of(&root, "CloudFrontOriginAccessIdentityConfig");
    let mut canonical = [0u8; 64];
    let bytes = uuid::Uuid::new_v4().as_bytes().to_vec();
    for (i, slot) in canonical.iter_mut().enumerate() {
        const CHARS: &[u8] = b"abcdefghijklmnopqrstuvwxyz0123456789";
        *slot = CHARS[bytes[i % bytes.len()] as usize % CHARS.len()];
    }
    let oai = OriginAccessIdentity {
        id: random_upper(13),
        s3_canonical_user_id: String::from_utf8_lossy(&canonical).into_owned(),
        caller_reference: config.text_of("CallerReference").unwrap_or_default().to_string(),
        comment: config.text_of("Comment").unwrap_or_default().to_string(),
        etag: random_upper(13),
    };
    let location =
        format!("https://cloudfront.amazonaws.com/2020-05-31/origin-access-identity/cloudfront/{}", oai.id);
    backend.origin_access_identities.push(oai);
    let oai = backend.origin_access_identities.last().expect("just inserted");
    let body = render_oai(oai);
    resp_with(
        201,
        vec![("ETag", oai.etag.clone()), ("Location", location)],
        xml_decl(&body),
    )
}

fn render_oai(oai: &OriginAccessIdentity) -> String {
    wrap(
        "CloudFrontOriginAccessIdentity",
        el("Id", &oai.id)
            + &el("S3CanonicalUserId", &oai.s3_canonical_user_id)
            + &wrap(
                "CloudFrontOriginAccessIdentityConfig",
                el("CallerReference", &oai.caller_reference)
                    + &el("Comment", &oai.comment),
            ),
    )
}

fn oai_by_id_mut<'a>(backend: &'a mut Backend, id: &str) -> Option<&'a mut OriginAccessIdentity> {
    let pos = backend
        .origin_access_identities
        .iter()
        .position(|o| o.id == id)?;
    Some(&mut backend.origin_access_identities[pos])
}

fn h_get_oai(backend: &mut Backend, id: &str) -> Response {
    match backend.origin_access_identities.iter().find(|o| o.id == id) {
        Some(oai) => resp_with(
            200,
            vec![("ETag", oai.etag.clone())],
            xml_decl(&render_oai(oai)),
        ),
        None => xml_error(
            404,
            "NoSuchCloudFrontOriginAccessIdentity",
            "The specified origin access identity does not exist.",
        ),
    }
}

fn h_get_oai_config(backend: &mut Backend, id: &str) -> Response {
    match backend.origin_access_identities.iter().find(|o| o.id == id) {
        Some(oai) => resp_with(
            200,
            vec![("ETag", oai.etag.clone())],
            xml_decl(&wrap(
                "CloudFrontOriginAccessIdentityConfig",
                el("CallerReference", &oai.caller_reference) + &el("Comment", &oai.comment),
            )),
        ),
        None => xml_error(
            404,
            "NoSuchCloudFrontOriginAccessIdentity",
            "The specified origin access identity does not exist.",
        ),
    }
}

fn h_update_oai(backend: &mut Backend, id: &str, req: &Request) -> Response {
    let root = body_xml(req);
    let config = config_of(&root, "CloudFrontOriginAccessIdentityConfig");
    match oai_by_id_mut(backend, id) {
        Some(oai) => {
            oai.update(
                config.text_of("CallerReference").unwrap_or_default(),
                config.text_of("Comment").unwrap_or_default(),
            );
            let etag = oai.etag.clone();
            let body = render_oai(oai);
            resp_with(200, vec![("ETag", etag)], xml_decl(&body))
        }
        None => xml_error(
            404,
            "NoSuchCloudFrontOriginAccessIdentity",
            "The specified origin access identity does not exist.",
        ),
    }
}

fn h_delete_oai(backend: &mut Backend, id: &str) -> Response {
    match backend.origin_access_identities.iter().position(|o| o.id == id) {
        Some(pos) => {
            backend.origin_access_identities.remove(pos);
            empty_resp(204)
        }
        None => xml_error(
            404,
            "NoSuchCloudFrontOriginAccessIdentity",
            "The specified origin access identity does not exist.",
        ),
    }
}

fn h_list_oais(backend: &mut Backend) -> Response {
    let oais = &backend.origin_access_identities;
    let mut items = String::new();
    for oai in oais {
        items += &wrap(
            "CloudFrontOriginAccessIdentitySummary",
            el("Id", &oai.id)
                + &el("S3CanonicalUserId", &oai.s3_canonical_user_id)
                + &el("Comment", &oai.comment),
        );
    }
    let items_block = if oais.is_empty() {
        String::new()
    } else {
        wrap("Items", &items)
    };
    let body = wrap(
        "CloudFrontOriginAccessIdentityList",
        "<Marker></Marker><MaxItems>100</MaxItems><IsTruncated>false</IsTruncated>".to_string()
            + &el("Quantity", &oais.len().to_string())
            + &items_block,
    );
    resp(200, xml_decl(&body))
}

// ---------------------------------------------------------------------------
// Public keys
// ---------------------------------------------------------------------------

fn h_create_public_key(backend: &mut Backend, req: &Request) -> Response {
    let root = body_xml(req);
    let config = config_of(&root, "PublicKeyConfig");
    // moto appends a trailing newline when the XML roundtrip dropped it
    let mut encoded_key = config.text_of("EncodedKey").unwrap_or_default().to_string();
    if !encoded_key.ends_with('\n') {
        encoded_key.push('\n');
    }
    let key = PublicKey {
        id: random_upper(14),
        caller_ref: config.text_of("CallerReference").unwrap_or_default().to_string(),
        name: config.text_of("Name").unwrap_or_default().to_string(),
        encoded_key,
        created: now_millis_iso(),
        etag: random_upper(14),
    };
    let location = format!("https://cloudfront.amazonaws.com/2020-05-31/public-key/{}", key.id);
    backend.public_keys.push(key);
    let key = backend.public_keys.last().expect("just inserted");
    let body = render_public_key(key);
    resp_with(
        201,
        vec![("ETag", key.etag.clone()), ("Location", location)],
        xml_decl(&body),
    )
}

fn render_public_key(key: &PublicKey) -> String {
    wrap(
        "PublicKey",
        el("Id", &key.id)
            + &el("CreatedTime", &key.created)
            + &wrap(
                "PublicKeyConfig",
                el("CallerReference", &key.caller_ref)
                    + &el("Name", &key.name)
                    + &el("EncodedKey", &key.encoded_key)
                    + "<Comment></Comment>",
            ),
    )
}

fn public_key_by_id_mut<'a>(backend: &'a mut Backend, id: &str) -> Option<&'a mut PublicKey> {
    let pos = backend.public_keys.iter().position(|k| k.id == id)?;
    Some(&mut backend.public_keys[pos])
}

fn h_get_public_key(backend: &mut Backend, id: &str) -> Response {
    match backend.public_keys.iter().find(|k| k.id == id) {
        Some(key) => resp_with(
            200,
            vec![("ETag", key.etag.clone())],
            xml_decl(&render_public_key(key)),
        ),
        None => xml_error(
            404,
            "NoSuchPublicKey",
            "The specified public key does not exist.",
        ),
    }
}

fn h_get_public_key_config(backend: &mut Backend, id: &str) -> Response {
    match backend.public_keys.iter().find(|k| k.id == id) {
        Some(key) => resp_with(
            200,
            vec![("ETag", key.etag.clone())],
            xml_decl(&wrap(
                "PublicKeyConfig",
                el("CallerReference", &key.caller_ref)
                    + &el("Name", &key.name)
                    + &el("EncodedKey", &key.encoded_key)
                    + "<Comment></Comment>",
            )),
        ),
        None => xml_error(
            404,
            "NoSuchPublicKey",
            "The specified public key does not exist.",
        ),
    }
}

fn h_update_public_key(backend: &mut Backend, id: &str) -> Response {
    match public_key_by_id_mut(backend, id) {
        Some(key) => {
            key.etag = random_upper(14);
            let etag = key.etag.clone();
            let body = render_public_key(key);
            resp_with(200, vec![("ETag", etag)], xml_decl(&body))
        }
        None => xml_error(
            404,
            "NoSuchPublicKey",
            "The specified public key does not exist.",
        ),
    }
}

fn h_delete_public_key(backend: &mut Backend, id: &str) -> Response {
    backend.public_keys.retain(|k| k.id != id);
    resp(204, "{}")
}

fn h_list_public_keys(backend: &mut Backend) -> Response {
    let keys = &backend.public_keys;
    let mut items = String::new();
    for key in keys {
        items += &wrap(
            "PublicKeySummary",
            el("Id", &key.id)
                + &el("Name", &key.name)
                + &el("CreatedTime", &key.created)
                + &el("EncodedKey", &key.encoded_key)
                + "<Comment></Comment>",
        );
    }
    let items_block = if keys.is_empty() {
        String::new()
    } else {
        wrap("Items", &items)
    };
    let body = wrap(
        "PublicKeyList",
        "<MaxItems>100</MaxItems>".to_string()
            + &el("Quantity", &keys.len().to_string())
            + &items_block,
    );
    resp(200, xml_decl(&body))
}

// ---------------------------------------------------------------------------
// Key groups
// ---------------------------------------------------------------------------

fn h_create_key_group(backend: &mut Backend, req: &Request) -> Response {
    let root = body_xml(req);
    let config = config_of(&root, "KeyGroupConfig");
    let group = KeyGroup {
        id: random_upper(14),
        name: config.text_of("Name").unwrap_or_default().to_string(),
        items: config
            .sub(&["Items"])
            .map(|items| items.components("PublicKey").map(El::clone_text).collect())
            .unwrap_or_default(),
        etag: random_upper(14),
    };
    let location = format!("https://cloudfront.amazonaws.com/2020-05-31/key-group/{}", group.id);
    backend.key_groups.push(group);
    let group = backend.key_groups.last().expect("just inserted");
    let body = render_key_group(group);
    resp_with(
        201,
        vec![("ETag", group.etag.clone()), ("Location", location)],
        xml_decl(&body),
    )
}

fn render_key_group(group: &KeyGroup) -> String {
    let mut items = String::new();
    for item in &group.items {
        items += &wrap("PublicKey", item);
    }
    wrap(
        "KeyGroup",
        el("Id", &group.id)
            + &wrap(
                "KeyGroupConfig",
                el("Name", &group.name) + &wrap("Items", &items),
            ),
    )
}

fn h_get_key_group(backend: &mut Backend, id: &str) -> Response {
    match backend.key_groups.iter().find(|g| g.id == id) {
        Some(group) => resp_with(
            200,
            vec![("ETag", group.etag.clone())],
            xml_decl(&render_key_group(group)),
        ),
        None => xml_error(
            404,
            "NoSuchResource",
            "The specified key group does not exist.",
        ),
    }
}

fn h_get_key_group_config(backend: &mut Backend, id: &str) -> Response {
    match backend.key_groups.iter().find(|g| g.id == id) {
        Some(group) => {
            let mut items = String::new();
            for item in &group.items {
                items += &wrap("PublicKey", item);
            }
            resp_with(
                200,
                vec![("ETag", group.etag.clone())],
                xml_decl(&wrap(
                    "KeyGroupConfig",
                    el("Name", &group.name) + &wrap("Items", &items),
                )),
            )
        }
        None => xml_error(
            404,
            "NoSuchResource",
            "The specified key group does not exist.",
        ),
    }
}

fn h_update_key_group(backend: &mut Backend, id: &str, req: &Request) -> Response {
    let root = body_xml(req);
    let config = config_of(&root, "KeyGroupConfig");
    match backend.key_groups.iter_mut().find(|g| g.id == id) {
        Some(group) => {
            group.update(
                config.text_of("Name").unwrap_or_default(),
                config
                    .sub(&["Items"])
                    .map(|items| items.components("PublicKey").map(El::clone_text).collect())
                    .unwrap_or_default(),
            );
            let etag = group.etag.clone();
            let body = render_key_group(group);
            resp_with(200, vec![("ETag", etag)], xml_decl(&body))
        }
        None => xml_error(
            404,
            "NoSuchResource",
            "The specified key group does not exist.",
        ),
    }
}

fn h_delete_key_group(backend: &mut Backend, id: &str) -> Response {
    match backend.key_groups.iter().position(|g| g.id == id) {
        Some(pos) => {
            backend.key_groups.remove(pos);
            empty_resp(204)
        }
        None => xml_error(
            404,
            "NoSuchResource",
            "The specified key group does not exist.",
        ),
    }
}

fn h_list_key_groups(backend: &mut Backend) -> Response {
    let groups = &backend.key_groups;
    let mut items = String::new();
    for group in groups {
        items += &wrap("KeyGroupSummary", &render_key_group(group));
    }
    let items_block = if groups.is_empty() {
        String::new()
    } else {
        wrap("Items", &items)
    };
    let body = wrap(
        "KeyGroupList",
        "<MaxItems>100</MaxItems>".to_string()
            + &el("Quantity", &groups.len().to_string())
            + &items_block,
    );
    resp(200, xml_decl(&body))
}

fn h_list_oac(backend: &mut Backend) -> Response {
    let controls = &backend.origin_access_controls;
    let mut items = String::new();
    for control in controls {
        items += &wrap(
            "OriginAccessControlSummary",
            el("Id", &control.id)
                + &el("Name", &control.name)
                + &if control.description.is_empty() {
                    String::new()
                } else {
                    el("Description", &control.description)
                }
                + &el("SigningProtocol", &control.signing_protocol)
                + &el("SigningBehavior", &control.signing_behaviour)
                + &el("OriginAccessControlOriginType", &control.origin_type),
        );
    }
    let items_block = if controls.is_empty() {
        String::new()
    } else {
        wrap("Items", &items)
    };
    let body = wrap(
        "OriginAccessControlList",
        &wrap("Items", &items).as_str(),
    );
    let _ = items_block;
    resp(200, xml_decl(&body))
}

fn h_get_oac_config(backend: &mut Backend, id: &str) -> Response {
    match backend.origin_access_controls.iter().find(|c| c.id == id) {
        Some(control) => resp_with(
            200,
            vec![("ETag", control.etag.clone())],
            xml_decl(&wrap(
                "OriginAccessControlConfig",
                &oac_config_no_id(control),
            )),
        ),
        None => xml_error(
            404,
            "NoSuchOriginAccessControl",
            "The specified origin access control does not exist.",
        ),
    }
}

// ---------------------------------------------------------------------------
// CloudFront Functions
// ---------------------------------------------------------------------------

fn h_create_function(account: u64, backend: &mut Backend, req: &Request) -> Response {
    let root = body_xml(req);
    let request = config_of(&root, "CreateFunctionRequest");
    let name = request.text_of("Name").unwrap_or_default().to_string();
    if backend.functions.iter().any(|f| f.name == name) {
        return xml_error(
            400,
            "FunctionAlreadyExists",
            "A function with the same name already exists.",
        );
    }
    let func = CloudFrontFunction {
        name: name.clone(),
        function_code: request.text_of("FunctionCode").unwrap_or_default().to_string(),
        function_config: request.child("FunctionConfig").cloned().unwrap_or_default(),
        status: "UNPUBLISHED".to_string(),
        stage: "DEVELOPMENT".to_string(),
        created_time: now_millis_iso(),
        last_modified_time: now_millis_iso(),
        etag: random_upper(14),
        function_arn: format!("arn:aws:cloudfront::{account}:function/{name}"),
    };
    let location = format!("https://cloudfront.amazonaws.com/2020-05-31/function/{name}");
    backend.functions.push(func);
    let func = backend.functions.last().expect("just inserted");
    let body = render_function_summary(func);
    resp_with(
        201,
        vec![("ETag", func.etag.clone()), ("Location", location)],
        xml_decl(&body),
    )
}

fn render_function_summary(func: &CloudFrontFunction) -> String {
    wrap(
        "FunctionSummary",
        el("Name", &func.name)
            + &el("Status", &func.status)
            + &wrap(
                "FunctionConfig",
                el("Comment", &func.function_config.text_or("Comment", ""))
                    + &el(
                        "Runtime",
                        &func
                            .function_config
                            .text_of("Runtime")
                            .unwrap_or("cloudfront-js-1.0"),
                    ),
            )
            + &wrap(
                "FunctionMetadata",
                el("FunctionARN", &func.function_arn)
                    + &el("Stage", &func.stage)
                    + &el("CreatedTime", &func.created_time)
                    + &el("LastModifiedTime", &func.last_modified_time),
            ),
    )
}

fn function_by_name_mut<'a>(backend: &'a mut Backend, name: &str) -> Option<&'a mut CloudFrontFunction> {
    let pos = backend.functions.iter().position(|f| f.name == name)?;
    Some(&mut backend.functions[pos])
}

fn h_describe_function(backend: &mut Backend, name: &str) -> Response {
    match backend.functions.iter().find(|f| f.name == name) {
        Some(func) => resp_with(
            200,
            vec![("ETag", func.etag.clone())],
            xml_decl(&render_function_summary(func)),
        ),
        None => xml_error(
            404,
            "NoSuchFunctionExists",
            "The specified function does not exist.",
        ),
    }
}

fn h_get_function_sig(backend: &mut Backend, name: &str) -> Response {
    match backend.functions.iter().find(|f| f.name == name) {
        Some(func) => {
            let body = func.function_code.clone().into_bytes();
            resp_with(
                200,
                vec![
                    ("ETag", func.etag.clone()),
                    ("Content-Type", "application/octet-stream".to_string()),
                ],
                body,
            )
        }
        None => xml_error(
            404,
            "NoSuchFunctionExists",
            "The specified function does not exist.",
        ),
    }
}

fn h_update_function(account: u64, backend: &mut Backend, req: &Request, name: &str) -> Response {
    let root = body_xml(req);
    let request = config_of(&root, "UpdateFunctionRequest");
    match function_by_name_mut(backend, name) {
        Some(func) => {
            func.update(
                &request.text_of("FunctionCode").unwrap_or_default(),
                request.child("FunctionConfig").cloned().unwrap_or_default(),
            );
            let etag = func.etag.clone();
            let _ = account;
            let body = render_function_summary(func);
            resp_with(200, vec![("ETtag", etag)], xml_decl(&body))
        }
        None => xml_error(
            404,
            "NoSuchFunctionExists",
            "The specified function does not exist.",
        ),
    }
}

fn h_delete_function(backend: &mut Backend, name: &str) -> Response {
    match backend.functions.iter().position(|f| f.name == name) {
        Some(pos) => {
            backend.functions.remove(pos);
            empty_resp(204)
        }
        None => xml_error(
            404,
            "NoSuchFunctionExists",
            "The specified function does not exist.",
        ),
    }
}

fn h_publish_function(backend: &mut Backend, name: &str) -> Response {
    match function_by_name_mut(backend, name) {
        Some(func) => {
            func.publish();
            let etag = func.etag.clone();
            let body = render_function_summary(func);
            resp_with(200, vec![("ETag", etag)], xml_decl(&body))
        }
        None => xml_error(
            404,
            "NoSuchFunctionExists",
            "The specified function does not exist.",
        ),
    }
}

fn h_test_function(backend: &mut Backend, name: &str) -> Response {
    if backend.functions.iter().any(|f| f.name == name) {
        let body = wrap(
            "TestResult",
            wrap(
                "FunctionSummary",
                el("Name", name) + &el("Status", "UNASSOCIATED"),
            ) + &el("ComputeUtilization", "12")
                + "<FunctionExecutionLogs></FunctionExecutionLogs>"
                + &el("FunctionErrorMessage", "")
                + &el("FunctionOutput", "{\"response\":{\"statusCode\":200}}"),
        );
        resp(200, xml_decl(&body))
    } else {
        xml_error(
            404,
            "NoSuchFunctionExists",
            "The specified function does not exist.",
        )
    }
}

fn h_list_functions(backend: &mut Backend) -> Response {
    let functions = &backend.functions;
    let mut items = String::new();
    for func in functions {
        items += &render_function_summary(func);
    }
    let items_block = if functions.is_empty() {
        String::new()
    } else {
        wrap("Items", &items)
    };
    let body = wrap(
        "FunctionList",
        "<MaxItems>100</MaxItems>".to_string()
            + &el("Quantity", &functions.len().to_string())
            + &items_block,
    );
    resp(200, xml_decl(&body))
}

// ---------------------------------------------------------------------------
// Cache / response headers / origin request policies
// ---------------------------------------------------------------------------

fn h_create_cache_policy(backend: &mut Backend, req: &Request) -> Response {
    let root = body_xml(req);
    let config_el = config_of(&root, "CachePolicyConfig");
    let policy = CachePolicy::new(random_upper(14), config_el);
    let location = format!("https://cloudfront.amazonaws.com/2020-05-31/cache-policy/{}", policy.id);
    backend.cache_policies.push(policy);
    let policy = backend.cache_policies.last().expect("just inserted");
    let body = render_cache_policy(policy);
    resp_with(
        201,
        vec![("ETag", policy.etag.clone()), ("Location", location)],
        xml_decl(&body),
    )
}

fn render_cache_policy(policy: &CachePolicy) -> String {
    wrap(
        "CachePolicy",
        el("Id", &policy.id)
            + &el("LastModifiedTime", &policy.last_modified_time)
            + &wrap(
                "CachePolicyConfig",
                el("Name", &policy.name)
                    + &el("Comment", &policy.comment)
                    + &el("DefaultTTL", &policy.default_ttl)
                    + &el("MaxTTL", &policy.max_ttl)
                    + &el("MinTTL", &policy.min_ttl),
            ),
    )
}

fn h_get_cache_policy(backend: &mut Backend, id: &str) -> Response {
    match backend.cache_policies.iter().find(|p| p.id == id) {
        Some(policy) => resp_with(
            200,
            vec![("ETag", policy.etag.clone())],
            xml_decl(&render_cache_policy(policy)),
        ),
        None => xml_error(
            404,
            "NoSuchCachePolicy",
            "The specified cache policy does not exist.",
        ),
    }
}

fn h_get_cache_policy_config(backend: &mut Backend, id: &str) -> Response {
    match backend.cache_policies.iter().find(|p| p.id == id) {
        Some(policy) => resp_with(
            200,
            vec![("ETag", policy.etag.clone())],
            xml_decl(&wrap(
                "CachePolicyConfig",
                el("Name", &policy.name)
                    + &el("Comment", &policy.comment)
                    + &el("DefaultTTL", &policy.default_ttl)
                    + &el("MaxTTL", &policy.max_ttl)
                    + &el("MinTTL", &policy.min_ttl),
            )),
        ),
        None => xml_error(
            404,
            "NoSuchCachePolicy",
            "The specified cache policy does not exist.",
        ),
    }
}

fn h_update_cache_policy(backend: &mut Backend, id: &str, req: &Request) -> Response {
    let root = body_xml(req);
    let config_el = config_of(&root, "CachePolicyConfig");
    match backend.cache_policies.iter_mut().find(|p| p.id == id) {
        Some(policy) => {
            policy.update(config_el);
            let etag = policy.etag.clone();
            let body = render_cache_policy(policy);
            resp_with(
                200,
                vec![("ETag", etag)],
                xml_decl(&body),
            )
        }
        None => xml_error(
            404,
            "NoSuchCachePolicy",
            "The specified cache policy does not exist.",
        ),
    }
}

fn h_delete_cache_policy(backend: &mut Backend, id: &str) -> Response {
    match backend.cache_policies.iter().position(|p| p.id == id) {
        Some(pos) => {
            backend.cache_policies.remove(pos);
            empty_resp(204)
        }
        None => xml_error(
            404,
            "NoSuchCachePolicy",
            "The specified cache policy does not exist.",
        ),
    }
}

fn h_list_cache_policies(backend: &mut Backend) -> Response {
    let policies = &backend.cache_policies;
    let mut items = String::new();
    for policy in policies {
        items += &wrap(
            "CachePolicySummary",
            wrap("Type", "custom") + &wrap("CachePolicy", &render_cache_policy(policy)),
        );
    }
    let items_block = if policies.is_empty() {
        String::new()
    } else {
        wrap("Items", &items)
    };
    let body = wrap(
        "CachePolicyList",
        "<MaxItems>100</MaxItems>".to_string()
            + &el("Quantity", &policies.len().to_string())
            + &items_block,
    );
    resp(200, xml_decl(&body))
}

fn h_create_rhp(backend: &mut Backend, req: &Request) -> Response {
    let root = body_xml(req);
    let config_el = config_of(&root, "ResponseHeadersPolicyConfig");
    let policy = ResponseHeadersPolicy {
        id: random_upper(14),
        name: config_el.text_of("Name").unwrap_or_default().to_string(),
        comment: config_el.text_of("Comment").unwrap_or_default().to_string(),
        last_modified_time: now_millis_iso(),
        etag: random_upper(14),
    };
    let location = format!(
        "https://cloudfront.amazonaws.com/2020-05-31/response-headers-policy/{}",
        policy.id
    );
    backend.response_headers_policies.push(policy);
    let policy = backend.response_headers_policies.last().expect("just inserted");
    let body = render_rhp(policy);
    resp_with(
        201,
        vec![("ETag", policy.etag.clone()), ("Location", location)],
        xml_decl(&body),
    )
}

fn render_rhp(policy: &ResponseHeadersPolicy) -> String {
    wrap(
        "ResponseHeadersPolicy",
        el("Id", &policy.id)
            + &el("LastModifiedTime", &policy.last_modified_time)
            + &wrap(
                "ResponseHeadersPolicyConfig",
                el("Name", &policy.name) + &el("Comment", &policy.comment),
            ),
    )
}

fn h_get_rhp(backend: &mut Backend, id: &str) -> Response {
    match backend.response_headers_policies.iter().find(|p| p.id == id) {
        Some(policy) => resp_with(
            200,
            vec![("ETag", policy.etag.clone())],
            xml_decl(&render_rhp(policy)),
        ),
        None => xml_error(
            404,
            "NoSuchResponseHeadersPolicy",
            "The specified response headers policy does not exist.",
        ),
    }
}

fn h_get_rhp_config(backend: &mut Backend, id: &str) -> Response {
    match backend.response_headers_policies.iter().find(|p| p.id == id) {
        Some(policy) => resp_with(
            200,
            vec![("ETag", policy.etag.clone())],
            xml_decl(&wrap(
                "ResponseHeadersPolicyConfig",
                el("Name", &policy.name) + &el("Comment", &policy.comment),
            )),
        ),
        None => xml_error(
            404,
            "NoSuchResponseHeadersPolicy",
            "The specified response headers policy does not exist.",
        ),
    }
}

fn h_update_rhp(backend: &mut Backend, id: &str, req: &Request) -> Response {
    let root = body_xml(req);
    let config_el = config_of(&root, "ResponseHeadersPolicyConfig");
    match backend.response_headers_policies.iter_mut().find(|p| p.id == id) {
        Some(policy) => {
            policy.update(config_el);
            let etag = policy.etag.clone();
            let body = render_rhp(policy);
            resp_with(200, vec![("ETag", etag)], xml_decl(&body))
        }
        None => xml_error(
            404,
            "NoSuchResponseHeadersPolicy",
            "The specified response headers policy does not exist.",
        ),
    }
}

fn h_delete_rhp(backend: &mut Backend, id: &str) -> Response {
    match backend.response_headers_policies.iter().position(|p| p.id == id) {
        Some(pos) => {
            backend.response_headers_policies.remove(pos);
            empty_resp(204)
        }
        None => xml_error(
            404,
            "NoSuchResponseHeadersPolicy",
            "The specified response headers policy does not exist.",
        ),
    }
}

fn h_list_rhp(backend: &mut Backend) -> Response {
    let policies = &backend.response_headers_policies;
    let mut items = String::new();
    for policy in policies {
        items += &wrap(
            "ResponseHeadersPolicySummary",
            wrap("Type", "custom") + &wrap("ResponseHeadersPolicy", &render_rhp(policy)),
        );
    }
    let items_block = if policies.is_empty() {
        String::new()
    } else {
        wrap("Items", &items)
    };
    let body = wrap(
        "ResponseHeadersPolicyList",
        "<MaxItems>100</MaxItems>".to_string() + &el("Quantity", &policies.len().to_string())
             + &items_block,
    );
    resp(200, xml_decl(&body))
}

fn h_create_orp(backend: &mut Backend, req: &Request) -> Response {
    let root = body_xml(req);
    let config_el = config_of(&root, "OriginRequestPolicyConfig");
    let policy = OriginRequestPolicy {
        id: random_upper(14),
        name: config_el.text_of("Name").unwrap_or_default().to_string(),
        comment: config_el.text_of("Comment").unwrap_or_default().to_string(),
        headers_behavior: config_el
            .sub(&["HeadersConfig"])
            .and_then(|c| c.text_of("HeaderBehavior"))
            .unwrap_or("none")
            .to_string(),
        cookies_behavior: config_el
            .sub(&["CookiesConfig"])
            .and_then(|c| c.text_of("CookieBehavior"))
            .unwrap_or("none")
            .to_string(),
        query_strings_behavior: config_el
            .sub(&["QueryStringsConfig"])
            .and_then(|c| c.text_of("QueryStringBehavior"))
            .unwrap_or("none")
            .to_string(),
        last_modified_time: now_millis_iso(),
        etag: random_upper(14),
    };
    let location = format!(
        "https://cloudfront.amazonaws.com/2020-05-31/origin-request-policy/{}",
        policy.id
    );
    backend.origin_request_policies.push(policy);
    let policy = backend.origin_request_policies.last().expect("just inserted");
    let body = render_orp(policy);
    resp_with(
        201,
        vec![("ETag", policy.etag.clone()), ("Location", location)],
        xml_decl(&body),
    )
}

fn render_orp(policy: &OriginRequestPolicy) -> String {
    wrap(
        "OriginRequestPolicy",
        el("Id", &policy.id)
            + &el("LastModifiedTime", &policy.last_modified_time)
            + &wrap(
                "OriginRequestPolicyConfig",
                el("Name", &policy.name)
                    + &el("Comment", &policy.comment)
                    + &wrap("HeadersConfig", &el("HeaderBehavior", &policy.headers_behavior))
                    + &wrap("CookiesConfig", &el("CookieBehavior", &policy.cookies_behavior))
                    + &wrap(
                        "QueryStringsConfig",
                        &el("QueryStringBehavior", &policy.query_strings_behavior),
                    ),
            ),
    )
}

fn h_get_orp(backend: &mut Backend, id: &str) -> Response {
    match backend.origin_request_policies.iter().find(|p| p.id == id) {
        Some(policy) => resp_with(
            200,
            vec![("ETag", policy.etag.clone())],
            xml_decl(&render_orp(policy)),
        ),
        None => xml_error(
            404,
            "NoSuchOriginRequestPolicy",
            "The specified origin request policy does not exist.",
        ),
    }
}

fn h_get_orp_config(backend: &mut Backend, id: &str) -> Response {
    match backend.origin_request_policies.iter().find(|p| p.id == id) {
        Some(policy) => resp_with(
            200,
            vec![("ETag", policy.etag.clone())],
            xml_decl(&wrap(
                "OriginRequestPolicyConfig",
                el("Name", &policy.name)
                    + &el("Comment", &policy.comment)
                    + &wrap("HeadersConfig", &el("HeaderBehavior", &policy.headers_behavior))
                    + &wrap("CookiesConfig", &el("CookieBehavior", &policy.cookies_behavior))
                    + &wrap(
                        "QueryStringsConfig",
                        &el("QueryStringBehavior", &policy.query_strings_behavior),
                    ),
            )),
        ),
        None => xml_error(
            404,
            "NoSuchOriginRequestPolicy",
            "The specified origin request policy does not exist.",
        ),
    }
}

fn h_update_orp(backend: &mut Backend, id: &str, req: &Request) -> Response {
    let root = body_xml(req);
    let config_el = config_of(&root, "OriginRequestPolicyConfig");
    match backend.origin_request_policies.iter_mut().find(|p| p.id == id) {
        Some(policy) => {
            policy.update(config_el);
            let etag = policy.etag.clone();
            let body = render_orp(policy);
            resp_with(200, vec![("ETag", etag)], xml_decl(&body))
        }
        None => xml_error(
            404,
            "NoSuchOriginRequestPolicy",
            "The specified origin request policy does not exist.",
        ),
    }
}

fn h_delete_orp(backend: &mut Backend, id: &str) -> Response {
    match backend.origin_request_policies.iter().position(|p| p.id == id) {
        Some(pos) => {
            backend.origin_request_policies.remove(pos);
            empty_resp(204)
        }
        None => xml_error(
            404,
            "NoSuchOriginRequestPolicy",
            "The specified origin request policy does not exist.",
        ),
    }
}

fn h_list_orp(backend: &mut Backend) -> Response {
    let policies = &backend.origin_request_policies;
    let mut items = String::new();
    for policy in policies {
        items += &wrap(
            "OriginRequestPolicySummary",
            wrap("Type", "custom")
                + &wrap(
                    "OriginRequestPolicy",
                    el("Id", &policy.id)
                        + &el("LastModifiedTime", &policy.last_modified_time)
                        + &wrap(
                            "OriginRequestPolicyConfig",
                            el("Name", &policy.name) + &el("Comment", &policy.comment),
                        ),
                ),
        );
    }
    let items_block = if policies.is_empty() {
        String::new()
    } else {
        wrap("Items", &items)
    };
    let body = wrap(
        "OriginRequestPolicyList",
        "<MaxItems>100</MaxItems>".to_string() + &el("Quantity", &policies.len().to_string())
             + &items_block,
    );
    resp(200, xml_decl(&body))
}

// ---------------------------------------------------------------------------
// Field-level encryption + profiles
// ---------------------------------------------------------------------------

fn h_create_fle(backend: &mut Backend, req: &Request) -> Response {
    let root = body_xml(req);
    let config_el = config_of(&root, "FieldLevelEncryptionConfig");
    let fle = FleConfig {
        id: random_upper(14),
        caller_reference: config_el
            .text_of("CallerReference")
            .unwrap_or(&uuid::Uuid::new_v4().to_string())
            .to_string(),
        comment: config_el.text_of("Comment").unwrap_or_default().to_string(),
        last_modified_time: now_millis_iso(),
        etag: random_upper(14),
    };
    let location = format!(
        "https://cloudfront.amazonaws.com/2020-05-31/field-level-encryption/{}",
        fle.id
    );
    backend.fle_configs.push(fle);
    let fle = backend.fle_configs.last().expect("just inserted");
    let body = render_fle(fle);
    resp_with(201, vec![("ETag", fle.etag.clone()), ("Location", location)], xml_decl(&body))
}

fn render_fle(fle: &FleConfig) -> String {
    wrap(
        "FieldLevelEncryption",
        el("Id", &fle.id)
            + &el("LastModifiedTime", &fle.last_modified_time)
            + &wrap(
                "FieldLevelEncryptionConfig",
                el("CallerReference", &fle.caller_reference)
                    + &el("Comment", &fle.comment),
            ),
    )
}

fn h_get_fle(backend: &mut Backend, id: &str) -> Response {
    match backend.fle_configs.iter().find(|f| f.id == id) {
        Some(fle) => resp_with(
            200,
            vec![("ETag", fle.etag.clone())],
            xml_decl(&render_fle(fle)),
        ),
        None => xml_error(
            404,
            "NoSuchFieldLevelEncryptionConfig",
            "The specified field-level encryption configuration does not exist.",
        ),
    }
}

fn h_get_fle_config(backend: &mut Backend, id: &str) -> Response {
    match backend.fle_configs.iter().find(|f| f.id == id) {
        Some(fle) => resp_with(
            200,
            vec![("ETag", fle.etag.clone())],
            xml_decl(&wrap(
                "FieldLevelEncryptionConfig",
                el("CallerReference", &fle.caller_reference)
                    + &el("Comment", &fle.comment),
            )),
        ),
        None => xml_error(
            404,
            "NoSuchFieldLevelEncryptionConfig",
            "The specified field-level encryption configuration does not exist.",
        ),
    }
}

fn h_update_fle(backend: &mut Backend, id: &str, req: &Request) -> Response {
    let root = body_xml(req);
    let config_el = config_of(&root, "FieldLevelEncryptionConfig");
    match backend.fle_configs.iter_mut().find(|f| f.id == id) {
        Some(fle) => {
            fle.update(config_el);
            let etag = fle.etag.clone();
            let body = render_fle(fle);
            resp_with(200, vec![("ETag", etag)], xml_decl(&body))
        }
        None => xml_error(
            404,
            "NoSuchFieldLevelEncryptionConfig",
            "The specified field-level encryption configuration does not exist.",
        ),
    }
}

fn h_delete_fle(backend: &mut Backend, id: &str) -> Response {
    match backend.fle_configs.iter().position(|f| f.id == id) {
        Some(pos) => {
            backend.fle_configs.remove(pos);
            empty_resp(204)
        }
        None => xml_error(
            404,
            "NoSuchFieldLevelEncryptionConfig",
            "The specified field-level encryption configuration does not exist.",
        ),
    }
}

fn h_list_fle(backend: &mut Backend) -> Response {
    let configs = &backend.fle_configs;
    let mut items = String::new();
    for fle in configs {
        items += &wrap(
            "FieldLevelEncryptionSummary",
            el("Id", &fle.id)
                + &el("LastModifiedTime", &fle.last_modified_time)
                + &el("Comment", &fle.comment),
        );
    }
    let items_block = if configs.is_empty() {
        String::new()
    } else {
        wrap("Items", &items)
    };
    let body = wrap(
        "FieldLevelEncryptionList",
        "<MaxItems>100</MaxItems>".to_string() + &el("Quantity", &configs.len().to_string())
             + &items_block,
    );
    resp(200, xml_decl(&body))
}

fn h_create_flep(backend: &mut Backend, req: &Request) -> Response {
    let root = body_xml(req);
    let config_el = config_of(&root, "FieldLevelEncryptionProfileConfig");
    let profile = FleProfile {
        id: random_upper(14),
        name: config_el.text_of("Name").unwrap_or_default().to_string(),
        caller_reference: config_el
            .text_of("CallerReference")
            .unwrap_or(&uuid::Uuid::new_v4().to_string())
            .to_string(),
        comment: config_el.text_of("Comment").unwrap_or_default().to_string(),
        last_modified_time: now_millis_iso(),
        etag: random_upper(14),
    };
    let location = format!(
        "https://cloudfront.amazonaws.com/2020-05-31/field-level-encryption-profile/{}",
        profile.id
    );
    backend.fle_profiles.push(profile);
    let profile = backend.fle_profiles.last().expect("just inserted");
    let body = render_flep(profile);
    resp_with(
        201,
        vec![("ETag", profile.etag.clone()), ("Location", location)],
        xml_decl(&body),
    )
}

fn render_flep(profile: &FleProfile) -> String {
    wrap(
        "FieldLevelEncryptionProfile",
        el("Id", &profile.id)
            + &el("LastModifiedTime", &profile.last_modified_time)
            + &wrap(
                "FieldLevelEncryptionProfileConfig",
                el("Name", &profile.name)
                    + &el("CallerReference", &profile.caller_reference)
                    + &el("Comment", &profile.comment),
            ),
    )
}

fn h_get_flep(backend: &mut Backend, id: &str) -> Response {
    match backend.fle_profiles.iter().find(|p| p.id == id) {
        Some(profile) => resp_with(
            200,
            vec![("ETag", profile.etag.clone())],
            xml_decl(&render_flep(profile)),
        ),
        None => xml_error(
            404,
            "NoSuchFieldLevelEncryptionProfile",
            "The specified field-level encryption profile does not exist.",
        ),
    }
}

fn h_get_flep_config(backend: &mut Backend, id: &str) -> Response {
    match backend.fle_profiles.iter().find(|p| p.id == id) {
        Some(profile) => resp_with(
            200,
            vec![("ETag", profile.etag.clone())],
            xml_decl(&wrap(
                "FieldLevelEncryptionProfileConfig",
                el("Name", &profile.name)
                    + &el("CallerReference", &profile.caller_reference)
                    + &el("Comment", &profile.comment),
            )),
        ),
        None => xml_error(
            404,
            "NoSuchFieldLevelEncryptionProfile",
            "The specified field-level encryption profile does not exist.",
        ),
    }
}

fn h_update_flep(backend: &mut Backend, id: &str, req: &Request) -> Response {
    let root = body_xml(req);
    let config_el = config_of(&root, "FieldLevelEncryptionProfileConfig");
    match backend.fle_profiles.iter_mut().find(|p| p.id == id) {
        Some(profile) => {
            profile.update(config_el);
            let etag = profile.etag.clone();
            let body = render_flep(profile);
            resp_with(
                200,
                vec![("ETag", etag)],
                xml_decl(&body),
            )
        }
        None => xml_error(
            404,
            "NoSuchFieldLevelEncryptionProfile",
            "The specified field-level encryption profile does not exist.",
        ),
    }
}

fn h_delete_flep(backend: &mut Backend, id: &str) -> Response {
    match backend.fle_profiles.iter().position(|p| p.id == id) {
        Some(pos) => {
            backend.fle_profiles.remove(pos);
            empty_resp(204)
        }
        None => xml_error(
            404,
            "NoSuchFieldLevelEncryptionProfile",
            "The specified field-level encryption profile does not exist.",
        ),
    }
}

fn h_list_flep(backend: &mut Backend) -> Response {
    let profiles = &backend.fle_profiles;
    let mut items = String::new();
    for profile in profiles {
        items += &wrap(
            "FieldLevelEncryptionProfileSummary",
            el("Id", &profile.id)
                + &el("LastModifiedTime", &profile.last_modified_time)
                + &el("Name", &profile.name)
                + &el("Comment", &profile.comment),
        );
    }
    let items_block = if profiles.is_empty() {
        String::new()
    } else {
        wrap("Items", &items)
    };
    let body = wrap(
        "FieldLevelEncryptionProfileList",
        "<MaxItems>100</MaxItems>".to_string() + &el("Quantity", &profiles.len().to_string())
             + &items_block,
    );
    resp(200, xml_decl(&body))
}

// ---------------------------------------------------------------------------
// Continuous deployment policies
// ---------------------------------------------------------------------------

fn h_create_cdp(backend: &mut Backend, req: &Request) -> Response {
    let root = body_xml(req);
    let config_el = config_of(&root, "ContinuousDeploymentPolicyConfig");
    let policy = CdpPolicy {
        id: random_upper(14),
        enabled: config_el.text_of("Enabled").unwrap_or("false").to_string(),
        last_modified_time: now_millis_iso(),
        etag: random_upper(14),
    };
    let location = format!(
        "https://cloudfront.amazonaws.com/2020-05-31/continuous-deployment-policy/{}",
        policy.id
    );
    backend.cdp_policies.push(policy);
    let policy = backend.cdp_policies.last().expect("just inserted");
    let body = render_cdp(policy);
    resp_with(
        201,
        vec![("ETag", policy.etag.clone()), ("Location", location)],
        xml_decl(&body),
    )
}

fn render_cdp(policy: &CdpPolicy) -> String {
    wrap(
        "ContinuousDeploymentPolicy",
        el("Id", &policy.id)
            + &el("LastModifiedTime", &policy.last_modified_time)
            + &wrap(
                "ContinuousDeploymentPolicyConfig",
                "<StagingDistributionDnsNames><Quantity>0</Quantity></StagingDistributionDnsNames>".to_string()
                    + &el("Enabled", &policy.enabled),
            ),
    )
}

fn h_get_cdp(backend: &mut Backend, id: &str) -> Response {
    match backend.cdp_policies.iter().find(|p| p.id == id) {
        Some(policy) => resp_with(
            200,
            vec![("ETag", policy.etag.clone())],
            xml_decl(&render_cdp(policy)),
        ),
        None => xml_error(
            404,
            "NoSuchContinuousDeploymentPolicy",
            "The specified continuous deployment policy does not exist.",
        ),
    }
}

fn h_get_cdp_config(backend: &mut Backend, id: &str) -> Response {
    match backend.cdp_policies.iter().find(|p| p.id == id) {
        Some(policy) => resp_with(
            200,
            vec![("ETag", policy.etag.clone())],
            xml_decl(&wrap(
                "ContinuousDeploymentPolicyConfig",
                "<StagingDistributionDnsNames><Quantity>0</Quantity></StagingDistributionDnsNames>".to_string()
                    + &el("Enabled", &policy.enabled),
            )),
        ),
        None => xml_error(
            404,
            "NoSuchContinuousDeploymentPolicy",
            "The specified continuous deployment policy does not exist.",
        ),
    }
}

fn h_update_cdp(backend: &mut Backend, id: &str, req: &Request) -> Response {
    let root = body_xml(req);
    let config_el = config_of(&root, "ContinuousDeploymentPolicyConfig");
    match backend.cdp_policies.iter_mut().find(|p| p.id == id) {
        Some(policy) => {
            policy.update(config_el);
            let etag = policy.etag.clone();
            let body = render_cdp(policy);
            resp_with(
                200,
                vec![("ETag", etag)],
                xml_decl(&body),
            )
        }
        None => xml_error(
            404,
            "NoSuchContinuousDeploymentPolicy",
            "The specified continuous deployment policy does not exist.",
        ),
    }
}

fn h_delete_cdp(backend: &mut Backend, id: &str) -> Response {
    match backend.cdp_policies.iter().position(|p| p.id == id) {
        Some(pos) => {
            backend.cdp_policies.remove(pos);
            empty_resp(204)
        }
        None => xml_error(
            404,
            "NoSuchContinuousDeploymentPolicy",
            "The specified continuous deployment policy does not exist.",
        ),
    }
}

fn h_list_cdp(backend: &mut Backend) -> Response {
    let policies = &backend.cdp_policies;
    let mut items = String::new();
    for policy in policies {
        items += &wrap(
            "ContinuousDeploymentPolicySummary",
            &wrap("ContinuousDeploymentPolicy", &render_cdp(policy)),
        );
    }
    let items_block = if policies.is_empty() {
        String::new()
    } else {
        wrap("Items", &items)
    };
    let body = wrap(
        "ContinuousDeploymentPolicyList",
        "<MaxItems>100</MaxItems>".to_string()
            + &el("Quantity", &policies.len().to_string())
            + &items_block,
    );
    resp(200, xml_decl(&body))
}

// ---------------------------------------------------------------------------
// Monitoring subscriptions
// ---------------------------------------------------------------------------

fn h_create_monitoring_subscription(backend: &mut Backend, dist_id: &str, req: &Request) -> Response {
    if tokenized_distribution(backend, dist_id).is_none() {
        return xml_error(
            404,
            "NoSuchDistribution",
            "The specified distribution does not exist.",
        );
    }
    let root = body_xml(req);
    let status = root
        .sub(&["MonitoringSubscription", "RealtimeMetricsSubscriptionConfig"])
        .and_then(|cfg| cfg.text_of("RealtimeMetricsSubscriptionStatus"))
        .unwrap_or("Disabled")
        .to_string();
    backend.monitoring.insert(dist_id.to_string(), status.clone());
    let body = wrap(
        "MonitoringSubscription",
        &wrap(
            "RealtimeMetricsSubscriptionConfig",
            &el("RealtimeMetricsSubscriptionStatus", &status),
        ),
    );
    resp(200, xml_decl(&body))
}

fn h_get_monitoring_subscription(backend: &mut Backend, dist_id: &str) -> Response {
    match backend.monitoring.get(dist_id) {
        Some(status) => {
            let body = wrap(
                "MonitoringSubscription",
                &wrap(
                    "RealtimeMetricsSubscriptionConfig",
                    &el("RealtimeMetricsSubscriptionStatus", status),
                ),
            );
            resp(200, xml_decl(&body))
        }
        None => xml_error(
            404,
            "NoSuchMonitoringSubscription",
            "The specified monitoring subscription does not exist.",
        ),
    }
}

fn h_delete_monitoring_subscription(backend: &mut Backend, dist_id: &str) -> Response {
    match backend.monitoring.remove(dist_id) {
        Some(_) => empty_resp(200),
        None => xml_error(
            404,
            "NoSuchMonitoringSubscription",
            "The specified monitoring subscription does not exist.",
        ),
    }
}

// ---------------------------------------------------------------------------
// Realtime log configs
// ---------------------------------------------------------------------------

fn parse_realtime_body(req: &Request) -> El {
    body_xml(req)
}

fn parse_rlc_endpoints(body: &El) -> Vec<(String, String, String)> {
    body.sub(&["EndPoints"])
        .map(|eps| {
            eps.components("member")
                .map(|ep| {
                    (
                        ep.text_of("StreamType").unwrap_or("Kinesis").to_string(),
                        ep.sub(&["KinesisStreamConfig"])
                            .and_then(|k| k.text_of("RoleARN"))
                            .unwrap_or("")
                            .to_string(),
                        ep.sub(&["KinesisStreamConfig"])
                            .and_then(|k| k.text_of("StreamARN"))
                            .unwrap_or("")
                            .to_string(),
                    )
                })
                .collect()
        })
        .unwrap_or_default()
}

fn parse_rlc_fields(body: &El) -> Vec<String> {
    body.sub(&["Fields"])
        .map(|fs| fs.components("Field").map(El::clone_text).collect())
        .unwrap_or_default()
}

fn h_create_rlc(account: u64, backend: &mut Backend, req: &Request) -> Response {
    let body = parse_realtime_body(req);
    let sampling_rate = body
        .text_of("SamplingRate")
        .and_then(|s| s.parse::<i64>().ok())
        .unwrap_or(100);
    let config = RealtimeLogConfig {
        name: body.text_of("Name").unwrap_or_default().to_string(),
        arn: format!(
            "arn:aws:cloudfront::{account}:realtime-log-config/{}",
            body.text_of("Name").unwrap_or_default()
        ),
        sampling_rate,
        end_points: parse_rlc_endpoints(&body),
        fields: parse_rlc_fields(&body),
    };
    backend.realtime_log_configs.push(config);
    let config = backend.realtime_log_configs.last().expect("just inserted");
    let inner = render_rlc_inner(config);
    resp_with(
        201,
        vec![],
        xml_decl(&wrap("CreateRealtimeLogConfigResult", &inner)),
    )
}

fn render_rlc_inner(config: &RealtimeLogConfig) -> String {
    let mut endpoints = String::new();
    for (stream_type, role, stream) in &config.end_points {
        endpoints += &wrap(
            "member",
            el("StreamType", stream_type)
                + &wrap(
                    "KinesisStreamConfig",
                    el("RoleARN", role) + &el("StreamARN", stream),
                ),
        );
    }
    let mut field_items = String::new();
    for field in &config.fields {
        field_items += &wrap("member", field);
    }
    wrap(
        "RealtimeLogConfig",
        el("ARN", &config.arn)
            + &el("Name", &config.name)
            + &el("SamplingRate", &config.sampling_rate.to_string())
            + &wrap("EndPoints", &endpoints)
            + &wrap("Fields", &field_items),
    )
}

fn find_rlc<'a>(backend: &'a mut Backend, name: Option<&str>, arn: Option<&str>) -> Option<&'a mut RealtimeLogConfig> {
    if let Some(name) = name {
        if let Some(pos) = backend.realtime_log_configs.iter().position(|c| c.name == name) {
            return Some(&mut backend.realtime_log_configs[pos]);
        }
    }
    if let Some(arn) = arn {
        if let Some(pos) = backend.realtime_log_configs.iter().position(|c| c.arn == arn) {
            return Some(&mut backend.realtime_log_configs[pos]);
        }
    }
    None
}

fn h_get_rlc(_account: u64, backend: &mut Backend, req: &Request) -> Response {
    let body = parse_realtime_body(req);
    match find_rlc(backend, body.text_of("Name"), body.text_of("ARN")) {
        Some(config) => {
            let inner = render_rlc_inner(config);
            resp(200, xml_decl(&wrap("GetRealtimeLogConfigResult", &inner)))
        }
        None => xml_error(
            404,
            "NoSuchRealtimeLogConfig",
            "The specified real-time log configuration does not exist.",
        ),
    }
}

fn h_update_rlc(_account: u64, backend: &mut Backend, req: &Request) -> Response {
    let body = parse_realtime_body(req);
    let sampling_rate = body.text_of("SamplingRate").and_then(|s| s.parse::<i64>().ok());
    let end_points = if body.child("EndPoints").is_some() {
        Some(parse_rlc_endpoints(&body))
    } else {
        None
    };
    let fields = if body.child("Fields").is_some() {
        Some(parse_rlc_fields(&body))
    } else {
        None
    };
    match find_rlc(backend, body.text_of("Name"), body.text_of("ARN")) {
        Some(config) => {
            config.update(sampling_rate, end_points, fields);
            let inner = render_rlc_inner(config);
            resp(200, xml_decl(&wrap("UpdateRealtimeLogConfigResult", &inner)))
        }
        None => xml_error(
            404,
            "NoSuchRealtimeLogConfig",
            "The specified real-time log configuration does not exist.",
        ),
    }
}

fn h_delete_rlc(backend: &mut Backend, req: &Request) -> Response {
    let body = parse_realtime_body(req);
    let pos = {
        let name = body.text_of("Name");
        let arn = body.text_of("ARN");
        backend.realtime_log_configs.iter().position(|c| {
            name.map(|n| c.name == n).unwrap_or(false)
                || arn.map(|a| c.arn == a).unwrap_or(false)
        })
    };
    match pos {
        Some(pos) => {
            backend.realtime_log_configs.remove(pos);
            empty_resp(204)
        }
        None => xml_error(
            404,
            "NoSuchRealtimeLogConfig",
            "The specified real-time log configuration does not exist.",
        ),
    }
}

fn h_list_rlc(backend: &mut Backend) -> Response {
    let configs = &backend.realtime_log_configs;
    let mut items = String::new();
    for config in configs {
        items += &wrap(
            "member",
            el("ARN", &config.arn)
                + &el("Name", &config.name)
                + &el("SamplingRate", &config.sampling_rate.to_string()),
        );
    }
    let items_block = if configs.is_empty() {
        String::new()
    } else {
        wrap("Items", &items)
    };
    let body = wrap(
        "RealtimeLogConfigs",
        "<MaxItems>100</MaxItems><IsTruncated>false</IsTruncated>".to_string() + &items_block,
    );
    resp(200, xml_decl(&body))
}

// ---------------------------------------------------------------------------
// Streaming distributions
// ---------------------------------------------------------------------------

fn h_create_streaming(account: u64, backend: &mut Backend, req: &Request) -> Response {
    let root = body_xml(req);
    let (config_el, tags) = config_and_tags(&root, "StreamingDistributionConfig");
    let id = random_upper(13);
    let dist = StreamingDistribution {
        id: id.clone(),
        arn: format!("arn:aws:cloudfront:{account}:streaming-distribution/{id}"),
        status: "InProgress".to_string(),
        last_modified_time: now_millis_iso(),
        domain_name: format!("{}.cloudfront.net", random_domain(13)),
        etag: random_upper(13),
        config: config_el.clone(),
    };
    if !tags.is_empty() {
        save_tags(backend, &dist.arn, tags);
    }
    let location = format!(
        "https://cloudfront.amazonaws.com/2020-05-31/streaming-distribution/{id}"
    );
    backend.streaming_distributions.push(dist);
    let dist = backend.streaming_distributions.last().expect("just inserted");
    let body = render_streaming_doc(dist);
    resp_with(
        201,
        vec![("ETag", dist.etag.clone()), ("Location", location)],
        xml_decl(&body),
    )
}

fn streaming_config_inner(dist: &StreamingDistribution) -> String {
    let config = &dist.config;
    let s3_origin = config.child("S3Origin");
    let trusted = config.child("TrustedSigners");
    let signers: Vec<String> = trusted
        .map(|t| t.texts_at(&["Items"], "AwsAccountNumber"))
        .unwrap_or_default();
    let mut signer_items = String::new();
    for signer in &signers {
        signer_items += &wrap("AwsAccountNumber", signer);
    }
    let aliases = config.texts_at(&["Aliases", "Items"], "CNAME");
    let mut alias_items = String::new();
    for alias in &aliases {
        alias_items += &wrap("CNAME", alias);
    }
    let logging = config.child("Logging");
    el(
            "CallerReference",
            config
                .text_of("CallerReference")
                .unwrap_or(&uuid::Uuid::new_v4().to_string()),
        ) + &wrap(
            "S3Origin",
            el(
                "DomainName",
                s3_origin.and_then(|s| s.text_of("DomainName")).unwrap_or(""),
            ) + &el(
                "OriginAccessIdentity",
                s3_origin.and_then(|s| s.text_of("OriginAccessIdentity")).unwrap_or(""),
            ),
        )
        + &wrap(
            "Aliases",
            el("Quantity", &aliases.len().to_string())
                + &if aliases.is_empty() {
                    String::new()
                } else {
                    wrap("Items", &alias_items)
                },
        )
        + &el("Comment", &config.text_or("Comment", ""))
        + &wrap(
            "Logging",
            el(
                "Enabled",
                logging.and_then(|l| l.text_of("Enabled")).unwrap_or("False"),
            ) + &el(
                "Bucket",
                logging.and_then(|l| l.text_of("Bucket")).unwrap_or(""),
            ) + &el(
                "Prefix",
                logging.and_then(|l| l.text_of("Prefix")).unwrap_or(""),
            ),
        )
        + &wrap(
            "TrustedSigners",
            el(
                "Enabled",
                if trusted
                    .and_then(|t| t.text_of("Enabled"))
                    .map(|t| t.eq_ignore_ascii_case("true"))
                    .unwrap_or(false)
                {
                    "true"
                } else {
                    "false"
                },
            ) + &el("Quantity", &signers.len().to_string())
                + &if signers.is_empty() {
                    String::new()
                } else {
                    wrap("Items", &signer_items)
                },
        )
        + &el(
            "PriceClass",
            config.text_of("PriceClass").unwrap_or("PriceClass_All"),
        )
        + &el("Enabled", &config.text_or("Enabled", "false"))
}

fn render_streaming_doc(dist: &StreamingDistribution) -> String {
    wrap(
        "StreamingDistribution",
        el("Id", &dist.id)
            + &el("ARN", &dist.arn)
            + &el("Status", &dist.status)
            + &el("LastModifiedTime", &dist.last_modified_time)
            + &el("DomainName", &dist.domain_name)
            + "<ActiveTrustedSigners><Enabled>false</Enabled><Quantity>0</Quantity></ActiveTrustedSigners>"
            + &wrap("StreamingDistributionConfig", &streaming_config_inner(dist)),
    )
}

fn h_get_streaming(backend: &mut Backend, id: &str) -> Response {
    match backend.streaming_distributions.iter().find(|d| d.id == id) {
        Some(dist) => {
            let etag = dist.etag.clone();
            let mut dist = dist.clone();
            dist.advance();
            resp_with(
                200,
                vec![("ETag", etag)],
                xml_decl(&render_streaming_doc(&dist)),
            )
        }
        None => xml_error(
            404,
            "NoSuchStreamingDistribution",
            "The specified streaming distribution does not exist.",
        ),
    }
}

fn h_get_streaming_config(backend: &mut Backend, id: &str) -> Response {
    match backend.streaming_distributions.iter().find(|d| d.id == id) {
        Some(dist) => {
            let etag = dist.etag.clone();
            let mut dist = dist.clone();
            dist.advance();
            resp_with(
                200,
                vec![("ETag", etag)],
                xml_decl(&wrap(
                    "StreamingDistributionConfig",
                    &streaming_config_inner(&dist),
                )),
            )
        }
        None => xml_error(
            404,
            "NoSuchStreamingDistribution",
            "The specified streaming distribution does not exist.",
        ),
    }
}

fn h_update_streaming(backend: &mut Backend, id: &str, req: &Request) -> Response {
    let root = body_xml(req);
    let config_el = config_of(&root, "StreamingDistributionConfig");
    let pos = match backend.streaming_distributions.iter().position(|d| d.id == id) {
        Some(pos) => pos,
        None => {
            return xml_error(
                404,
                "NoSuchStreamingDistribution",
                "The specified streaming distribution does not exist.",
            )
        }
    };
    let dist = &mut backend.streaming_distributions[pos];
    dist.config = config_el.clone();
    dist.advance();
    let etag = dist.etag.clone();
    let body = render_streaming_doc(dist);
    resp_with(200, vec![("ETag", etag)], xml_decl(&body))
}

fn h_delete_streaming(backend: &mut Backend, id: &str) -> Response {
    match backend.streaming_distributions.iter().position(|d| d.id == id) {
        Some(pos) => {
            backend.streaming_distributions.remove(pos);
            empty_resp(204)
        }
        None => xml_error(
            404,
            "NoSuchStreamingDistribution",
            "The specified streaming distribution does not exist.",
        ),
    }
}

fn h_list_streaming(backend: &mut Backend) -> Response {
    for dist in &mut backend.streaming_distributions {
        dist.advance();
    }
    let mut items = String::new();
    for dist in &backend.streaming_distributions {
        items += &wrap(
            "StreamingDistributionSummary",
            el("Id", &dist.id)
                + &el("ARN", &dist.arn)
                + &el("Status", &dist.status)
                + &el("LastModifiedTime", &dist.last_modified_time)
                + &el("DomainName", &dist.domain_name)
                + &streaming_config_inner(dist),
        );
    }
    let items_block = if backend.streaming_distributions.is_empty() {
        String::new()
    } else {
        wrap("Items", &items)
    };
    let body = wrap(
        "StreamingDistributionList",
        "<Marker></Marker><MaxItems>100</MaxItems><IsTruncated>false</IsTruncated>".to_string()
            + &el("Quantity", &backend.streaming_distributions.len().to_string())
            + &items_block,
    );
    resp(200, xml_decl(&body))
}

// ---------------------------------------------------------------------------
// Key-value stores
// ---------------------------------------------------------------------------

fn h_create_kvs(account: u64, backend: &mut Backend, req: &Request) -> Response {
    let root = body_xml(req);
    let request = config_of(&root, "CreateKeyValueStoreRequest");
    let name = request.text_of("Name").unwrap_or_default().to_string();
    let kvs = KeyValueStore {
        id: random_upper(14),
        name: name.clone(),
        comment: request.text_of("Comment").unwrap_or_default().to_string(),
        arn: format!("arn:aws:cloudfront::{account}:key-value-store/{name}"),
        status: "READY".to_string(),
        last_modified_time: now_millis_iso(),
        etag: random_upper(14),
    };
    let location = format!("https://cloudfront.amazonaws.com/2020-05-31/key-value-store/{name}");
    backend.key_value_stores.push(kvs);
    let kvs = backend.key_value_stores.last().expect("just inserted");
    let body = render_kvs(kvs);
    resp_with(
        201,
        vec![("ETag", kvs.etag.clone()), ("Location", location)],
        xml_decl(&body),
    )
}

fn render_kvs(kvs: &KeyValueStore) -> String {
    wrap(
        "KeyValueStore",
        el("Name", &kvs.name)
            + &el("Id", &kvs.id)
            + &el("Comment", &kvs.comment)
            + &el("ARN", &kvs.arn)
            + &el("Status", &kvs.status)
            + &el("LastModifiedTime", &kvs.last_modified_time),
    )
}

fn h_describe_kvs(backend: &mut Backend, name: &str) -> Response {
    match backend.key_value_stores.iter().find(|k| k.name == name) {
        Some(kvs) => resp_with(
            200,
            vec![("ETag", kvs.etag.clone())],
            xml_decl(&render_kvs(kvs)),
        ),
        None => xml_error(
            404,
            "NoSuchResource",
            &format!("The key value store {name} does not exist."),
        ),
    }
}

fn h_update_kvs(backend: &mut Backend, name: &str, req: &Request) -> Response {
    let root = body_xml(req);
    let request = config_of(&root, "UpdateKeyValueStoreRequest");
    match backend.key_value_stores.iter_mut().find(|k| k.name == name) {
        Some(kvs) => {
            kvs.update(&request.text_of("Comment").unwrap_or_default());
            let etag = kvs.etag.clone();
            let body = render_kvs(kvs);
            resp_with(200, vec![("ETag", etag)], xml_decl(&body))
        }
        None => xml_error(
            404,
            "NoSuchResource",
            &format!("The key value store {name} does not exist."),
        ),
    }
}

fn h_delete_kvs(backend: &mut Backend, name: &str) -> Response {
    backend.key_value_stores.retain(|k| k.name != name);
    resp(204, "{}")
}

fn h_list_kvs(backend: &mut Backend) -> Response {
    let stores = &backend.key_value_stores;
    let mut items = String::new();
    for kvs in stores {
        items += &render_kvs(kvs);
    }
    let items_block = if stores.is_empty() {
        String::new()
    } else {
        wrap("Items", &items)
    };
    let body = wrap(
        "KeyValueStoreList",
        "<MaxItems>100</MaxItems>".to_string()
            + &el("Quantity", &stores.len().to_string())
            + &items_block,
    );
    resp(200, xml_decl(&body))
}

// ---------------------------------------------------------------------------
// VPC origins
// ---------------------------------------------------------------------------

fn h_create_vpc_origin(account: u64, backend: &mut Backend, req: &Request) -> Response {
    let root = body_xml(req);
    let request = config_of(&root, "CreateVpcOriginRequest");
    let config_el = request
        .child("VpcOriginEndpointConfig")
        .cloned()
        .unwrap_or_default();
    let id = random_upper(14);
    let vo = VpcOrigin {
        id: id.clone(),
        account_id: account,
        arn: format!("arn:aws:cloudfront::{account}:vpcorigin/{id}"),
        status: "Deployed".to_string(),
        created_time: now_millis_iso(),
        last_modified_time: now_millis_iso(),
        endpoint_config: config_el.clone(),
        etag: random_upper(14),
    };
    backend.vpc_origins.push(vo);
    let vo = backend.vpc_origins.last().expect("just inserted");
    let body = render_vpc_origin(vo, account);
    resp_with(
        202,
        vec![("ETag", vo.etag.clone())],
        xml_decl(&body),
    )
}

fn vpc_config_values(vo: &VpcOrigin) -> String {
    el(
        "Name",
        &vo.endpoint_config
            .text_of("Name")
            .unwrap_or_default()
            .to_string(),
    ) + &el(
        "Arn",
        &vo.endpoint_config
            .text_of("Arn")
            .unwrap_or_default()
            .to_string(),
    ) + &el(
        "HTTPPort",
        &vo.endpoint_config.text_of("HTTPPort").unwrap_or("80"),
    ) + &el(
        "HTTPSPort",
        &vo.endpoint_config.text_of("HTTPSPort").unwrap_or("443"),
    ) + &el(
        "OriginProtocolPolicy",
        &vo.endpoint_config
            .text_of("OriginProtocolPolicy")
            .unwrap_or("https-only"),
    ) + "<OriginSslProtocols><Quantity>0</Quantity><Items/></OriginSslProtocols>"
}

fn render_vpc_origin(vo: &VpcOrigin, account: u64) -> String {
    let _ = account;
    wrap(
        "VpcOrigin",
        el("Id", &vo.id)
            + &el("Arn", &vo.arn)
            + &el("AccountId", &vo.account_id.to_string())
            + &el("Status", &vo.status)
            + &el("CreatedTime", &vo.created_time)
            + &el("LastModifiedTime", &vo.last_modified_time)
            + &wrap("VpcOriginEndpointConfig", &vpc_config_values(vo)),
    )
}

fn h_get_vpc_origin(backend: &mut Backend, id: &str) -> Response {
    match backend.vpc_origins.iter().find(|v| v.id == id) {
        Some(vo) => {
            let account = vo.account_id;
            resp_with(
                200,
                vec![("ETag", vo.etag.clone())],
                xml_decl(&render_vpc_origin(vo, account)),
            )
        }
        None => xml_error(
            404,
            "NoSuchResource",
            &format!("The VPC origin {id} does not exist."),
        ),
    }
}

fn h_update_vpc_origin(backend: &mut Backend, id: &str, req: &Request) -> Response {
    let root = body_xml(req);
    let request = config_of(&root, "UpdateVpcOriginRequest");
    match backend.vpc_origins.iter_mut().find(|v| v.id == id) {
        Some(vo) => {
            vo.update(request.child("VpcOriginEndpointConfig").cloned().unwrap_or_default());
            let etag = vo.etag.clone();
            let account = vo.account_id;
            let body = render_vpc_origin(vo, account);
            resp_with(200, vec![("ETag", etag)], xml_decl(&body))
        }
        None => xml_error(
            404,
            "NoSuchResource",
            &format!("The VPC origin {id} does not exist."),
        ),
    }
}

fn h_delete_vpc_origin(backend: &mut Backend, id: &str) -> Response {
    backend.vpc_origins.retain(|v| v.id != id);
    empty_resp(202)
}

fn h_list_vpc_origins(backend: &mut Backend) -> Response {
    let origins = &backend.vpc_origins;
    let mut items = String::new();
    for vo in origins {
        items += &wrap(
            "VpcOriginSummary",
            el("Id", &vo.id)
                + &el(
                    "Name",
                    &vo.endpoint_config.text_of("Name").unwrap_or_default(),
                )
                + &el("Status", &vo.status)
                + &el("CreatedTime", &vo.created_time)
                + &el("LastModifiedTime", &vo.last_modified_time)
                + &el("Arn", &vo.arn)
                + &el("AccountId", &vo.account_id.to_string())
                + &el(
                    "OriginEndpointArn",
                    &vo.endpoint_config.text_of("Arn").unwrap_or_default(),
                ),
        );
    }
    let items_block = if origins.is_empty() {
        String::new()
    } else {
        wrap("Items", &items)
    };
    let body = wrap(
        "VpcOriginList",
        "<Marker></Marker><MaxItems>100</MaxItems><IsTruncated>false</IsTruncated>".to_string()
            + &el("Quantity", &origins.len().to_string())
            + &items_block,
    );
    resp(200, xml_decl(&body))
}

// ---------------------------------------------------------------------------
// Trust stores
// ---------------------------------------------------------------------------

fn h_create_trust_store(account: u64, backend: &mut Backend, req: &Request) -> Response {
    let root = body_xml(req);
    let request = config_of(&root, "CreateTrustStoreRequest");
    let id = random_upper(14);
    let ts = TrustStore {
        id: id.clone(),
        name: request.text_of("Name").unwrap_or_default().to_string(),
        status: "DEPLOYED".to_string(),
        arn: format!("arn:aws:cloudfront::{account}:trust-store/{id}"),
        number_of_ca_certificates: 0,
        last_modified_time: now_millis_iso(),
        etag: random_upper(14),
    };
    backend.trust_stores.push(ts);
    let ts = backend.trust_stores.last().expect("just inserted");
    let body = render_trust_store(ts);
    resp_with(201, vec![("ETag", ts.etag.clone())], xml_decl(&body))
}

fn render_trust_store(ts: &TrustStore) -> String {
    wrap(
        "TrustStore",
        el("Id", &ts.id)
            + &el("Arn", &ts.arn)
            + &el("Name", &ts.name)
            + &el("Status", &ts.status)
            + &el("NumberOfCaCertificates", &ts.number_of_ca_certificates.to_string())
            + &el("LastModifiedTime", &ts.last_modified_time),
    )
}

fn h_get_trust_store(backend: &mut Backend, id: &str) -> Response {
    match backend.trust_stores.iter().find(|t| t.id == id) {
        Some(ts) => resp_with(
            200,
            vec![("ETag", ts.etag.clone())],
            xml_decl(&render_trust_store(ts)),
        ),
        None => xml_error(
            404,
            "NoSuchResource",
            &format!("The trust store {id} does not exist."),
        ),
    }
}

fn h_update_trust_store(backend: &mut Backend, id: &str) -> Response {
    match backend.trust_stores.iter_mut().find(|t| t.id == id) {
        Some(ts) => {
            ts.update();
            let etag = ts.etag.clone();
            let body = render_trust_store(ts);
            resp_with(200, vec![("ETag", etag)], xml_decl(&body))
        }
        None => xml_error(
            404,
            "NoSuchResource",
            &format!("The trust store {id} does not exist."),
        ),
    }
}

fn h_delete_trust_store(backend: &mut Backend, id: &str) -> Response {
    backend.trust_stores.retain(|t| t.id != id);
    empty_resp(204)
}

fn h_list_trust_stores(backend: &mut Backend) -> Response {
    let stores = &backend.trust_stores;
    let mut items = String::new();
    for ts in stores {
        items += &wrap(
            "TrustStoreSummary",
            el("Id", &ts.id)
                + &el("Arn", &ts.arn)
                + &el("Name", &ts.name)
                + &el("Status", &ts.status)
                + &el("LastModifiedTime", &ts.last_modified_time),
        );
    }
    let body = wrap("TrustStoreList", &items);
    resp(200, xml_decl(&body))
}

// ---------------------------------------------------------------------------
// Anycast IP lists
// ---------------------------------------------------------------------------

fn h_create_anycast(account: u64, backend: &mut Backend, req: &Request) -> Response {
    let root = body_xml(req);
    let request = config_of(&root, "CreateAnycastIpListRequest");
    let id = random_upper(14);
    let aip = AnycastIpList {
        id: id.clone(),
        name: request.text_of("Name").unwrap_or_default().to_string(),
        status: "Deployed".to_string(),
        arn: format!("arn:aws:cloudfront::{account}:anycast-ip-list/{id}"),
        ip_count: 0,
        last_modified_time: now_millis_iso(),
        etag: random_upper(14),
    };
    backend.anycast_ip_lists.push(aip);
    let aip = backend.anycast_ip_lists.last().expect("just inserted");
    let body = render_anycast(aip);
    resp_with(201, vec![("ETag", aip.etag.clone())], xml_decl(&body))
}

fn render_anycast(aip: &AnycastIpList) -> String {
    wrap(
        "AnycastIpList",
        el("Id", &aip.id)
            + &el("Name", &aip.name)
            + &el("Status", &aip.status)
            + &el("Arn", &aip.arn)
            + "<AnycastIps/>"
            + &el("IpCount", &aip.ip_count.to_string())
            + &el("LastModifiedTime", &aip.last_modified_time),
    )
}

fn h_get_anycast(backend: &mut Backend, id: &str) -> Response {
    match backend.anycast_ip_lists.iter().find(|a| a.id == id) {
        Some(aip) => resp_with(
            200,
            vec![("ETag", aip.etag.clone())],
            xml_decl(&render_anycast(aip)),
        ),
        None => xml_error(
            404,
            "NoSuchResource",
            &format!("The anycast IP list {id} does not exist."),
        ),
    }
}

fn h_delete_anycast(backend: &mut Backend, id: &str) -> Response {
    backend.anycast_ip_lists.retain(|a| a.id != id);
    empty_resp(204)
}

fn h_list_anycast(backend: &mut Backend) -> Response {
    let lists = &backend.anycast_ip_lists;
    let mut items = String::new();
    for aip in lists {
        items += &wrap(
            "AnycastIpListSummary",
            el("Id", &aip.id)
                + &el("Name", &aip.name)
                + &el("Status", &aip.status)
                + &el("Arn", &aip.arn)
                + &el("IpCount", &aip.ip_count.to_string())
                + &el("LastModifiedTime", &aip.last_modified_time),
        );
    }
    let items_block = if lists.is_empty() {
        String::new()
    } else {
        wrap("Items", &items)
    };
    let body = wrap(
        "AnycastIpLists",
        "<Marker></Marker><MaxItems>100</MaxItems><IsTruncated>false</IsTruncated>".to_string()
            + &el("Quantity", &lists.len().to_string())
            + &items_block,
    );
    resp(200, xml_decl(&body))
}

// ---------------------------------------------------------------------------
// Resource policies
// ---------------------------------------------------------------------------

fn h_get_resource_policy() -> Response {
    resp_with(
        200,
        vec![("ETag", random_upper(14))],
        xml_decl(&wrap(
            "GetResourcePolicyResult",
            "",
        )),
    )
}

// ---------------------------------------------------------------------------
// Connection groups / functions
// ---------------------------------------------------------------------------

fn h_create_connection_group(account: u64, backend: &mut Backend, req: &Request) -> Response {
    let root = body_xml(req);
    let request = config_of(&root, "CreateConnectionGroupRequest");
    let id = random_upper(14);
    let cg = ConnectionGroup {
        id: id.clone(),
        name: request.text_of("Name").unwrap_or_default().to_string(),
        arn: format!("arn:aws:cloudfront::{account}:connection-group/{id}"),
        status: "Deployed".to_string(),
        created_time: now_millis_iso(),
        last_modified_time: now_millis_iso(),
        etag: random_upper(14),
    };
    backend.connection_groups.push(cg);
    let cg = backend.connection_groups.last().expect("just inserted");
    let body = render_connection_group(cg);
    resp_with(201, vec![("ETag", cg.etag.clone())], xml_decl(&body))
}

fn render_connection_group(cg: &ConnectionGroup) -> String {
    wrap(
        "ConnectionGroup",
        el("Id", &cg.id)
            + &el("Name", &cg.name)
            + &el("Arn", &cg.arn)
            + &el("CreatedTime", &cg.created_time)
            + &el("LastModifiedTime", &cg.last_modified_time)
            + &el("Status", &cg.status)
            + "<Enabled>true</Enabled><IsDefault>false</IsDefault>",
    )
}

fn h_get_connection_group(backend: &mut Backend, id: &str) -> Response {
    match backend.connection_groups.iter().find(|c| c.id == id) {
        Some(cg) => resp_with(
            200,
            vec![("ETag", cg.etag.clone())],
            xml_decl(&render_connection_group(cg)),
        ),
        None => xml_error(
            404,
            "NoSuchResource",
            &format!("The connection group {id} does not exist."),
        ),
    }
}

/// moto answers this stub with a 200 envelope whose body is the error doc.
fn h_get_connection_group_by_routing_endpoint() -> Response {
    resp(
        200,
        format!(
            "<?xml version=\"1.0\"?>\n<ErrorResponse xmlns=\"{XMLNS}\">{inner}</ErrorResponse>",
            inner = wrap(
                "Error",
                wrap("Type", "Sender")
                    + &el("Code", "NoSuchResource")
                    + &el("Message", "The specified resource does not exist.")
            )
        ),
    )
}

fn h_get_tenant_by_domain() -> Response {
    h_get_connection_group_by_routing_endpoint()
}

fn h_update_connection_group(backend: &mut Backend, id: &str) -> Response {
    match backend.connection_groups.iter_mut().find(|c| c.id == id) {
        Some(cg) => {
            cg.etag = random_upper(14);
            let etag = cg.etag.clone();
            let body = render_connection_group(cg);
            resp_with(200, vec![("ETag", etag)], xml_decl(&body))
        }
        None => xml_error(
            404,
            "NoSuchResource",
            &format!("The connection group {id} does not exist."),
        ),
    }
}

fn h_delete_connection_group(backend: &mut Backend, id: &str) -> Response {
    backend.connection_groups.retain(|c| c.id != id);
    empty_resp(204)
}

fn h_list_connection_groups(backend: &mut Backend) -> Response {
    let groups = &backend.connection_groups;
    let mut items = String::new();
    for cg in groups {
        items += &wrap(
            "ConnectionGroupSummary",
            el("Id", &cg.id)
                + &el("Name", &cg.name)
                + &el("Arn", &cg.arn)
                + &el("CreatedTime", &cg.created_time)
                + &el("LastModifiedTime", &cg.last_modified_time)
                + &el("Status", &cg.status),
        );
    }
    let body = wrap("ConnectionGroups", &items);
    resp(200, xml_decl(&body))
}

// ---------------------------------------------------------------------------

fn h_create_connection_function(account: u64, backend: &mut Backend, req: &Request) -> Response {
    let root = body_xml(req);
    let request = config_of(&root, "CreateConnectionFunctionRequest");
    let name = request.text_of("Name").unwrap_or_default().to_string();
    let id = random_upper(14);
    let cf = ConnectionFunction {
        id: id.clone(),
        name: name.clone(),
        connection_function_arn: format!("arn:aws:cloudfront::{account}:connection-function/{name}"),
        status: "UNASSOCIATED".to_string(),
        stage: "DEVELOPMENT".to_string(),
        created_time: now_millis_iso(),
        last_modified_time: now_millis_iso(),
        etag: random_upper(14),
    };
    backend.connection_functions.push(cf);
    let cf = backend.connection_functions.iter().find(|f| f.name == name).expect("just inserted");
    let body = wrap("ConnectionFunctionSummary", &render_connection_function_fields(cf));
    resp_with(201, vec![("ETag", cf.etag.clone())], xml_decl(&body))
}

fn render_connection_function(cf: &ConnectionFunction) -> String {
    wrap(
        "ConnectionFunctionSummary",
        el("Name", &cf.name)
            + &el("Id", &cf.id)
            + &el("ConnectionFunctionArn", &cf.connection_function_arn)
            + &el("Status", &cf.status)
            + &el("Stage", &cf.stage)
            + &el("CreatedTime", &cf.created_time)
            + &el("LastModifiedTime", &cf.last_modified_time),
    )
}

fn h_get_connection_function(backend: &mut Backend, name: &str) -> Response {
    match backend.connection_functions.iter().find(|f| f.name == name) {
        Some(cf) => resp_with(
            200,
            vec![("ETag", cf.etag.clone())],
            xml_decl(&render_connection_function(cf)),
        ),
        None => xml_error(
            404,
            "NoSuchResource",
            &format!("The connection function {name} does not exist."),
        ),
    }
}

fn h_update_connection_function(backend: &mut Backend, name: &str) -> Response {
    match backend.connection_functions.iter_mut().find(|f| f.name == name) {
        Some(cf) => {
            cf.etag = random_upper(14);
            let etag = cf.etag.clone();
            let body = wrap("ConnectionFunctionSummary", &render_connection_function_fields(cf));
            resp_with(200, vec![("ETag", etag)], xml_decl(&body))
        }
        None => xml_error(
            404,
            "NoSuchResource",
            &format!("The connection function {name} does not exist."),
        ),
    }
}

fn h_publish_connection_function(backend: &mut Backend, name: &str) -> Response {
    match backend.connection_functions.iter_mut().find(|f| f.name == name) {
        Some(cf) => {
            cf.stage = "LIVE".to_string();
            cf.etag = random_upper(14);
            let etag = cf.etag.clone();
            let body = wrap("ConnectionFunctionSummary", &render_connection_function_fields(cf));
            resp_with(200, vec![("ETag", etag)], xml_decl(&body))
        }
        None => xml_error(
            404,
            "NoSuchResource",
            &format!("The connection function {name} does not exist."),
        ),
    }
}

fn h_delete_connection_function(backend: &mut Backend, name: &str) -> Response {
    backend.connection_functions.retain(|f| f.name != name);
    empty_resp(204)
}

fn h_test_connection_function() -> Response {
    resp(
        200,
        format!(
            "<?xml version=\"1.0\"?>\n<TestResult xmlns=\"{XMLNS}\">{inner}</TestResult>",
            inner = wrap(
                "FunctionOutput",
                "{\"response\":{\"statusCode\":200}}"
            ) + &el("ComputeUtilization", "12")
        ),
    )
}

fn h_list_connection_functions(backend: &mut Backend) -> Response {
    let funcs = &backend.connection_functions;
    let mut items = String::new();
    for cf in funcs {
        items += &wrap("ConnectionFunctionSummary", &render_connection_function_fields(cf));
    }
    let body = wrap("ConnectionFunctions", &items);
    resp(200, xml_decl(&body))
}

fn render_connection_function_fields(cf: &ConnectionFunction) -> String {
    el("Name", &cf.name)
        + &el("Id", &cf.id)
        + &el("ConnectionFunctionArn", &cf.connection_function_arn)
        + &el("Status", &cf.status)
        + &el("Stage", &cf.stage)
        + &el("CreatedTime", &cf.created_time)
        + &el("LastModifiedTime", &cf.last_modified_time)
}

// ---------------------------------------------------------------------------
// Distribution tenants
// ---------------------------------------------------------------------------

fn h_create_tenant(account: u64, backend: &mut Backend, req: &Request) -> Response {
    let root = body_xml(req);
    let request = config_of(&root, "CreateDistributionTenantRequest");
    let id = random_upper(14);
    let tenant = DistributionTenant {
        id: id.clone(),
        name: request.text_of("Name").unwrap_or_default().to_string(),
        distribution_id: request
            .text_of("DistributionId")
            .unwrap_or_default()
            .to_string(),
        arn: format!("arn:aws:cloudfront::{account}:distribution-tenant/{id}"),
        status: "Deployed".to_string(),
        created_time: now_millis_iso(),
        last_modified_time: now_millis_iso(),
        etag: random_upper(14),
    };
    backend.distribution_tenants.push(tenant);
    let tenant = backend.distribution_tenants.last().expect("just inserted");
    let body = render_tenant(tenant);
    resp_with(201, vec![("ETag", tenant.etag.clone())], xml_decl(&body))
}

fn render_tenant(tenant: &DistributionTenant) -> String {
    wrap(
        "DistributionTenant",
        el("Id", &tenant.id)
            + &el("DistributionId", &tenant.distribution_id)
            + &el("Name", &tenant.name)
            + &el("Arn", &tenant.arn)
            + &el("Status", &tenant.status)
            + "<Enabled>true</Enabled>"
            + &el("CreatedTime", &tenant.created_time)
            + &el("LastModifiedTime", &tenant.last_modified_time),
    )
}

fn h_get_tenant(backend: &mut Backend, id: &str) -> Response {
    match backend.distribution_tenants.iter().find(|t| t.id == id) {
        Some(tenant) => resp_with(
            200,
            vec![("ETag", tenant.etag.clone())],
            xml_decl(&render_tenant(tenant)),
        ),
        None => xml_error(
            404,
            "NoSuchResource",
            &format!("The distribution tenant {id} does not exist."),
        ),
    }
}

fn h_update_tenant(backend: &mut Backend, id: &str) -> Response {
    match backend.distribution_tenants.iter_mut().find(|t| t.id == id) {
        Some(tenant) => {
            tenant.etag = random_upper(14);
            let etag = tenant.etag.clone();
            let body = render_tenant(tenant);
            resp_with(200, vec![("ETag", etag)], xml_decl(&body))
        }
        None => xml_error(
            404,
            "NoSuchResource",
            &format!("The distribution tenant {id} does not exist."),
        ),
    }
}

fn h_delete_tenant(backend: &mut Backend, id: &str) -> Response {
    backend.distribution_tenants.retain(|t| t.id != id);
    empty_resp(204)
}

fn h_list_tenants(backend: &mut Backend) -> Response {
    let tenants = &backend.distribution_tenants;
    let mut items = String::new();
    for tenant in tenants {
        items += &wrap(
            "DistributionTenantSummary",
            el("Id", &tenant.id)
                + &el("Name", &tenant.name)
                + &el("Arn", &tenant.arn)
                + &el("Status", &tenant.status),
        );
    }
    let body = wrap("DistributionTenantList", &items);
    resp(200, xml_decl(&body))
}

fn h_create_tenant_invalidation() -> Response {
    let body = wrap(
        "Invalidation",
        el("Id", &random_upper(13))
            + &el("Status", "COMPLETED")
            + &el("CreateTime", "2021-01-01T00:00:00.000Z")
            + &wrap(
                "InvalidationBatch",
                "<Paths><Quantity>0</Quantity></Paths>".to_string()
                    + &el("CallerReference", "ref"),
            ),
    );
    resp_with(
        201,
        vec![],
        format!("<?xml version=\"1.0\"?>\n<Invalidation xmlns=\"{XMLNS}\">{body}</Invalidation>"),
    )
}

fn h_get_tenant_invalidation(inv_id: &str) -> Response {
    let body = wrap(
        "Invalidation",
        el("Id", inv_id)
            + &el("Status", "COMPLETED")
            + &el("CreateTime", "2021-01-01T00:00:00.000Z")
            + &wrap(
                "InvalidationBatch",
                "<Paths><Quantity>0</Quantity></Paths>".to_string()
                    + &el("CallerReference", "ref"),
            ),
    );
    resp(
        200,
        format!("<?xml version=\"1.0\"?>\n<Invalidation xmlns=\"{XMLNS}\">{body}</Invalidation>"),
    )
}

fn h_list_tenant_invalidations() -> Response {
    resp(
        200,
        format!(
            "<?xml version=\"1.0\"?>\n<InvalidationList xmlns=\"{XMLNS}\">{inner}</InvalidationList>",
            inner = "<Marker></Marker><MaxItems>100</MaxItems><IsTruncated>false</IsTruncated><Quantity>0</Quantity>"
        ),
    )
}

// ---------------------------------------------------------------------------
// Query helpers + remaining list stubs
// ---------------------------------------------------------------------------

fn h_associate_alias(backend: &mut Backend, dist_id: &str, alias: Option<&str>) -> Response {
    match tokenized_distribution(backend, dist_id) {
        Some(dist) => {
            if let Some(alias) = alias {
                if !dist.aliases.iter().any(|a| a == alias) {
                    dist.aliases.push(alias.to_string());
                }
            }
            empty_resp(200)
        }
        None => xml_error(
            404,
            "NoSuchDistribution",
            "The specified distribution does not exist.",
        ),
    }
}

fn h_list_distributions_by_web_acl_id(backend: &mut Backend, web_acl_id: &str) -> Response {
    for dist in &mut backend.distributions {
        dist.advance();
    }
    let matching: Vec<&Distribution> = backend
        .distributions
        .iter()
        .filter(|d| d.web_acl_id == web_acl_id)
        .collect();
    distribution_list(&matching)
}

fn distribution_list(distributions: &[&Distribution]) -> Response {
    let mut items = String::new();
    for dist in distributions {
        items += &wrap("DistributionSummary", &render_dist_entry(dist));
    }
    let items_block = if distributions.is_empty() {
        String::new()
    } else {
        wrap("Items", &items)
    };
    let body = wrap(
        "DistributionList",
        "<Marker></Marker><MaxItems>100</MaxItems><IsTruncated>false</IsTruncated>".to_string() + &el("Quantity", &distributions.len().to_string())
             + &items_block,
    );
    resp(200, xml_decl(&body))
}

fn distribution_list_empty() -> Response {
    distribution_list(&[])
}

fn distribution_id_list(ids: &[String]) -> Response {
    let mut items = String::new();
    for id in ids {
        items += &wrap("DistributionId", id);
    }
    let items_block = if ids.is_empty() {
        String::new()
    } else {
        wrap("Items", &items)
    };
    let body = wrap(
        "DistributionIdList",
        "<Marker></Marker><MaxItems>100</MaxItems><IsTruncated>false</IsTruncated>".to_string()
            + &el("Quantity", &ids.len().to_string())
            + &items_block,
    );
    resp(200, xml_decl(&body))
}

fn h_list_distributions_by_key_group(backend: &mut Backend, key_group_id: &str) -> Response {
    let mut ids: Vec<String> = Vec::new();
    for dist in &mut backend.distributions {
        dist.advance();
        let default_groups = dist
            .config
            .child("DefaultCacheBehavior")
            .map(|c| c.texts_at(&["TrustedKeyGroups", "Items"], "KeyGroup"))
            .unwrap_or_default();
        if default_groups.iter().any(|g| g == key_group_id) {
            ids.push(dist.id.clone());
            continue;
        }
        for behavior in dist.config.els_at(&["CacheBehaviors", "Items"], "CacheBehavior") {
            if behavior
                .texts_at(&["TrustedKeyGroups", "Items"], "KeyGroup")
                .iter()
                .any(|g| g == key_group_id)
            {
                ids.push(dist.id.clone());
                break;
            }
        }
    }
    distribution_id_list(&ids)
}

fn h_list_distributions_by_realtime_log_config(backend: &mut Backend, req: &Request) -> Response {
    let body = parse_realtime_body(req);
    let arn = body.text_of("RealtimeLogConfigArn").unwrap_or("").to_string();
    for dist in &mut backend.distributions {
        dist.advance();
    }
    let matching: Vec<&Distribution> = backend
        .distributions
        .iter()
        .filter(|d| {
            let behavior = d.config.child("DefaultCacheBehavior");
            let realtime = behavior
                .and_then(|b| b.text_of("RealtimeLogConfigArn"))
                .unwrap_or("");
            realtime == arn
        })
        .collect();
    distribution_list(&matching)
}

fn h_list_conflicting_aliases(
    backend: &mut Backend,
    account: u64,
    dist_id: Option<&str>,
    alias: Option<&str>,
) -> Response {
    let needle = alias.unwrap_or("").to_string();
    let mut items = String::new();
    for dist in &backend.distributions {
        if Some(dist.id.as_str()) == dist_id {
            continue;
        }
        for candidate in &dist.aliases {
            if candidate.contains(&needle) || needle.contains(candidate) {
                items += &wrap(
                    "ConflictingAlias",
                    el("Alias", candidate)
                        + &el("DistributionId", &dist.id)
                        + &el("AccountId", &account.to_string()),
                );
            }
        }
    }
    let count = items.matches("<ConflictingAlias>").count();
    let items_block = if items.is_empty() {
        String::new()
    } else {
        wrap("Items", &items)
    };
    let body = wrap(
        "ConflictingAliasesList",
        "<MaxItems>100</MaxItems>".to_string()
            + &el("Quantity", &count.to_string())
            + &items_block,
    );
    resp(200, xml_decl(&body))
}

fn empty_resp_status(status: u16) -> Response {
    empty_resp(status)
}
