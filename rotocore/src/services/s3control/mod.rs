//! AWS S3 Control backend — native REST-XML backend for the `/v20180820/` surface.
//!
//! Replaces the moto sidecar for `s3control` (virtual-host
//! `{account}.s3-control.{region}` requests routed to `/v20180820/...` by the
//! gateway). State is per-(account, region), mirroring moto's
//! `S3ControlBackend` (moto/s3control): public access block, access points,
//! multi-region access points and their async operations, Storage Lens
//! configurations / groups, batch-operations jobs, access grants
//! (instances / locations / grants / resource policy), object lambda access
//! points, access point scopes and job tagging.
//!
//! Contract: probes/s3control.json (per-operation moto-era behavior) and
//! tests/compatibility/test_s3control_compat.py (recorded green against the
//! moto fork). Timestamps mirror moto (`%Y-%m-%dT%H:%M:%S.%f` for access
//! points, `...Z` elsewhere). Bucket-level policy/tagging/versioning/
//! lifecycle/replication delegate to the S3 state over
//! `s3::bridge::get_bucket`, exactly like moto's `s3_backends[...]` calls.
//!
//! Crash-fidelity exception (crash-row triage): moto's
//! `put_bucket_versioning` indexes `xmltodict.parse(body)
//! ["PutBucketVersioningRequest"]`, but botocore sends the
//! `VersioningConfiguration` root — moto's probe-visible answer is the
//! werkzeug HTML 500 for every real-client shape, so that is what the
//! twin serves; only a body with moto's expected root sets versioning.
//!
//! Operations outside moto's surface answer 501 the same way the sidecar
//! did: identity-center association, data access, caller grants, the
//! directory/regional bucket and object-lambda listing/configuration
//! operations, and the bare `bucket/{name}` CRUD routes. The
//! `tags/…` resource-tags routes and access-point `Tags` follow moto's
//! s3 TaggingService, keyed by the full resource ARN. Access points
//! paginate on botocore tokens; garbage tokens answer the modeled
//! InvalidNextTokenException.

use crate::store::{self, ScopeKey};
use crate::{Request, Response};
use base64::Engine as _;
use parking_lot::RwLock;
use serde_json::{json, Value};
use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;

const XMLNS: &str = "http://awss3control.amazonaws.com/doc/2018-08-20/";

// ---------------------------------------------------------------------------
// XML tree (mirrors the cloudfront idiom)
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

    /// Text of the named child or `""`.
    fn text_or(&self, name: &str) -> String {
        self.text_of(name).unwrap_or("").to_string()
    }

    /// All child nodes named `name`.
    fn parts<'a>(&'a self, name: &'a str) -> impl Iterator<Item = &'a El> + 'a {
        self.children.iter().filter(move |c| c.name == name)
    }

    /// Pairs of (key, value) under the current element's `Tag*` children.
    fn tag_pairs(&self) -> Vec<(String, String)> {
        self.parts("Tag")
            .map(|t| (t.text_or("Key"), t.text_or("Value")))
            .collect()
    }
}

/// The request body root: xmltodict hands moto a map keyed by the root
/// element and moto indexes that key, so the root element itself (or a
/// same-named child, when a wrapper exists) is the request payload.
fn req_el<'a>(root: Option<&'a El>, name: &str) -> Option<&'a El> {
    match root {
        Some(el) if el.name == name => Some(el),
        Some(el) => el.child(name),
        None => None,
    }
}

fn parse_xml(body: &[u8]) -> Option<El> {
    if body.is_empty() {
        return None;
    }
    let mut reader = quick_xml::Reader::from_reader(body);
    let mut stack: Vec<El> = Vec::new();
    let mut top: Option<El> = None;
    let mut buf = Vec::new();
    loop {
        match reader.read_event_into(&mut buf) {
            Ok(quick_xml::events::Event::Start(e)) => stack.push(El {
                name: std::str::from_utf8(e.local_name().as_ref())
                    .unwrap_or("")
                    .to_string(),
                text: String::new(),
                children: Vec::new(),
            }),
            Ok(quick_xml::events::Event::Empty(e)) => {
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
            Ok(quick_xml::events::Event::Text(t)) => {
                if let (Some(node), Ok(text)) = (stack.last_mut(), t.unescape()) {
                    if !text.trim().is_empty() {
                        node.text.push_str(&text);
                    }
                }
            }
            Ok(quick_xml::events::Event::CData(t)) => {
                if let Some(node) = stack.last_mut() {
                    node.text
                        .push_str(String::from_utf8_lossy(t.as_ref()).trim());
                }
            }
            Ok(quick_xml::events::Event::End(_)) => {
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
            Ok(quick_xml::events::Event::Eof) => break,
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
    crate::proto::xml_escape(s)
}

fn el(name: &str, content: impl AsRef<str>) -> String {
    format!("<{name}>{}</{name}>", esc(content.as_ref()))
}

/// Raw (never re-escaped) pre-rendered wrapper — for XML blocks that carry
/// markup themselves, like `Established` or nested `Policy` text.
fn raw_el(name: &str, inner: impl AsRef<str>) -> String {
    format!("<{name}>{}</{name}>", inner.as_ref())
}

fn resp(status: u16, body: impl Into<Vec<u8>>) -> Response {
    Response::xml(status, body.into())
}

fn empty_resp(status: u16) -> Response {
    resp(status, "")
}

/// moto's RESTError rendering for s3control: `wrapped_single_error` plus the
/// exception-specific block (CDATA message, Sender, RequestId).
fn xml_error(status: u16, code: &str, message: &str, extra: &str) -> Response {
    let inner = raw_el(
        "Error",
        [
            el("Code", code),
            format!("<Message><![CDATA[{}]]></Message>", esc(message)),
            "<Type>Sender</Type>".to_string(),
            extra.to_string(),
            el("RequestId", uuid::Uuid::new_v4().to_string()),
        ]
        .concat(),
    );
    let outer = raw_el("ErrorResponse", inner);
    resp(
        status,
        format!("<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n{outer}"),
    )
}

/// moto's S3-side flat error envelope for delegated / account-guard errors.
fn s3_error(status: u16, code: &str, message: &str) -> Response {
    resp(
        status,
        raw_el(
            "Error",
            [
                el("Code", code),
                format!("<Message><![CDATA[{}]]></Message>", esc(message)),
                el("RequestId", uuid::Uuid::new_v4().to_string()),
            ]
            .concat(),
        ),
    )
}

fn not_implemented() -> Response {
    xml_error(
        501,
        "NotImplemented",
        "The requested operation is not implemented",
        "",
    )
}

/// moto timestamps: `%Y-%m-%dT%H:%M:%S.%fZ` (microseconds, trailing Z).
fn moto_ts() -> String {
    format!("{}Z", chrono::Utc::now().format("%Y-%m-%dT%H:%M:%S.%6f"))
}

/// Access point creation dates drop the trailing Z (moto's `strftime` call).
fn moto_ts_nozone() -> String {
    chrono::Utc::now()
        .format("%Y-%m-%dT%H:%M:%S.%6f")
        .to_string()
}

/// moto's `get_random_hex(n)`: n lowercase hex digits.
fn random_hex(n: usize) -> String {
    let mut out = String::with_capacity(n);
    while out.len() < n {
        for b in uuid::Uuid::new_v4().as_bytes() {
            out.push_str(&format!("{b:02x}"));
            if out.len() >= n {
                break;
            }
        }
    }
    out.truncate(n);
    out
}

fn access_point_alias(name: &str) -> String {
    format!("{name}-{}-s3alias", random_hex(34))
}

fn mrap_alias(name: &str) -> String {
    format!("{name}-{}.mrap", random_hex(10))
}

/// Root element carrying the s3control XML namespace.
fn xmlns_el(name: &str, inner: impl AsRef<str>) -> String {
    format!("<{name} xmlns=\"{XMLNS}\">{}</{name}>", inner.as_ref())
}

// ---------------------------------------------------------------------------
// State
// ---------------------------------------------------------------------------

type Stores = Arc<RwLock<HashMap<ScopeKey, S3ControlStore>>>;

fn stores() -> Stores {
    store::slot::<S3ControlStore>("s3control")
}

/// Per-(account, region) in-memory state for AWS S3 Control. moto keys the
/// access-grants fields by account and everything else regionally inside one
/// backend; the (account, region) scope covers both partitions for this twin.
#[derive(Default)]
struct S3ControlStore {
    /// Four optional wire strings — moto's `or "false"` default renders
    /// `false` for anything unset at send time.
    public_access_block: Option<[Option<String>; 4]>,
    /// access point name -> access point
    access_points: BTreeMap<String, AccessPoint>,
    /// mrap name -> mrap
    mrap_access_points: BTreeMap<String, Mrap>,
    /// request token arn -> async operation
    mrap_operations: BTreeMap<String, MrapOperation>,
    storage_lens_configs: BTreeMap<String, StorageLensConfiguration>,
    /// one access grants instance per account (moto's dict)
    access_grants_instance: Option<AccessGrantsInstance>,
    access_grants_locations: BTreeMap<String, AccessGrantsLocation>,
    access_grants: BTreeMap<String, AccessGrant>,
    resource_policy: Option<String>,
    storage_lens_groups: BTreeMap<String, StorageLensGroup>,
    object_lambda_access_points: BTreeMap<String, AccessPoint>,
    object_lambda_access_point_policies: BTreeMap<String, String>,
    /// access point name -> scope element (children directly underneath)
    access_point_scopes: BTreeMap<String, El>,
    jobs: BTreeMap<String, Job>,
    job_tags: BTreeMap<String, Vec<(String, String)>>,
    /// arn -> [(key, value)] — moto's TaggingService on the s3 side
    resource_tags: BTreeMap<String, Vec<(String, String)>>,
}

struct AccessPoint {
    name: String,
    alias: String,
    bucket: String,
    created: String,
    arn: String,
    network_origin: String,
    vpc_id: Option<String>,
    /// four raw wire strings; missing keys render `true` (moto's
    /// `pubc.get(key, "true")` default)
    pubc: Option<BTreeMap<String, String>>,
    policy: Option<String>,
}

impl AccessPoint {
    fn pubc_field(&self, key: &str) -> String {
        self.pubc
            .as_ref()
            .and_then(|m| m.get(key))
            .cloned()
            .unwrap_or_else(|| "true".to_string())
    }
}

struct Mrap {
    name: String,
    alias: String,
    created_at: String,
    /// `Some(map)` when the request carried PublicAccessBlock; `None` is
    /// moto's python-bool default dict — rendered as "True" by its template.
    pubc: Option<BTreeMap<String, String>>,
    regions: Vec<(String, String)>, // (bucket, region)
    policy: Option<String>,
}

struct MrapOperation {
    request_token_arn: String,
    name: String,
    operation: String,
    created_at: String,
    /// regions for create; none for delete
    regions: Vec<(String, String)>,
    public_access_block: Option<BTreeMap<String, String>>,
    /// policy text for put-policy
    policy: Option<String>,
}

struct StorageLensConfiguration {
    arn: String,
    /// the raw `<StorageLensConfiguration>` child of the request body
    config: El,
    tags: Option<El>,
}

struct StorageLensGroup {
    name: String,
    arn: String,
    tags: Vec<(String, String)>,
}

struct AccessGrantsInstance {
    instance_id: String,
    instance_arn: String,
    created_at: String,
    identity_center_arn: Option<String>,
}

struct AccessGrantsLocation {
    location_id: String,
    location_arn: String,
    location_scope: String,
    iam_role_arn: String,
    created_at: String,
}

struct AccessGrant {
    grant_id: String,
    grant_arn: String,
    location_id: String,
    permission: String,
    location_scope: String,
    grantee_type: String,
    grantee_identifier: String,
    created_at: String,
}

struct Job {
    job_id: String,
    status: String,
    description: String,
    priority: i64,
    role_arn: String,
    created_at: String,
}

/// moto resolves the MRAP region bucket-by-bucket across every regional S3
/// backend of the account; fall back to its "us-east-1" default when the
/// bridge has no registration in this region.
fn bucket_region(account: u64, bucket: &str, current: &str) -> String {
    if s3::bridge::bucket_exists(account, current, bucket) {
        current.to_string()
    } else {
        "us-east-1".to_string()
    }
}

// ---------------------------------------------------------------------------
// dispatch
// ---------------------------------------------------------------------------

pub fn dispatch(req: &Request) -> Option<Response> {
    if req.service != "s3control" {
        return None;
    }
    let mut segs: Vec<String> = req
        .path
        .split('/')
        .filter(|s| !s.is_empty())
        .map(crate::proto::url_decode)
        .collect();
    if !segs.first().is_some_and(|s| s == "v20180820") {
        return Some(not_implemented());
    }
    segs.remove(0);
    let query = crate::proto::query_map(&req.query_string);
    let slot = stores();
    let mut map = slot.write();
    let st = map.entry((req.account, req.region.clone())).or_default();
    let body = parse_xml(&req.body);
    Some(route(req, st, &segs, &query, body.as_ref()))
}

/// The exact header value of `x-amz-account-id` ("" when absent).
fn account_header(req: &Request) -> String {
    req.headers
        .get("x-amz-account-id")
        .cloned()
        .unwrap_or_default()
}

/// moto's account guard: `AccessDenied` when the header disagrees with the
/// backend's account (WrongPublicAccessBlockAccountIdError).
fn check_account(req: &Request) -> Result<(), Response> {
    if account_header(req) == req.account.to_string() {
        Ok(())
    } else {
        Err(s3_error(403, "AccessDenied", "Access Denied"))
    }
}

fn route(
    req: &Request,
    st: &mut S3ControlStore,
    segs: &[String],
    query: &BTreeMap<String, Vec<String>>,
    body: Option<&El>,
) -> Response {
    let method = req.method.as_str();
    let name_at = |i: usize| -> String { segs.get(i).cloned().unwrap_or_default() };

    match (method, segs) {
        // ------------------------------------------------ public access block
        ("PUT", [c, f]) if *c == "configuration" && *f == "publicAccessBlock" => {
            put_public_access_block(req, st, body)
        }
        ("GET", [c, f]) if *c == "configuration" && *f == "publicAccessBlock" => {
            get_public_access_block(req, st)
        }
        ("DELETE", [c, f]) if *c == "configuration" && *f == "publicAccessBlock" => {
            delete_public_access_block(req, st)
        }
        // ------------------------------------------------------- access points
        ("GET", [p]) if *p == "accesspoint" => list_access_points(st, query),
        ("PUT", [p, _]) if *p == "accesspoint" => create_access_point(req, st, &name_at(1), body),
        ("GET", [p, _]) if *p == "accesspoint" => get_access_point(st, &name_at(1)),
        ("DELETE", [p, _]) if *p == "accesspoint" => {
            st.access_points.remove(&name_at(1));
            empty_resp(204)
        }
        ("PUT", [p, _, l]) if *p == "accesspoint" && *l == "policy" => {
            put_access_point_policy(st, &name_at(1), body)
        }
        ("GET", [p, _, l]) if *p == "accesspoint" && *l == "policy" => {
            get_access_point_policy(st, &name_at(1))
        }
        ("DELETE", [p, _, l]) if *p == "accesspoint" && *l == "policy" => {
            match st.access_points.get_mut(&name_at(1)) {
                Some(ap) => {
                    ap.policy = None;
                    empty_resp(204)
                }
                None => access_point_not_found(&name_at(1)),
            }
        }
        ("GET", [p, _, l]) if *p == "accesspoint" && *l == "policyStatus" => {
            get_access_point_policy_status(st, &name_at(1))
        }
        ("PUT", [p, _, l]) if *p == "accesspoint" && *l == "scope" => {
            put_access_point_scope(st, &name_at(1), body)
        }
        ("GET", [p, _, l]) if *p == "accesspoint" && *l == "scope" => {
            get_access_point_scope(st, &name_at(1))
        }
        ("DELETE", [p, _, l]) if *p == "accesspoint" && *l == "scope" => {
            st.access_point_scopes.remove(&name_at(1));
            empty_resp(204)
        }
        // ------------------------------------------------- multi-region aps
        ("POST", [a, m, o]) if *a == "async-requests" && *m == "mrap" && *o == "create" => {
            create_multi_region_access_point(req, st, body)
        }
        ("POST", [a, m, o]) if *a == "async-requests" && *m == "mrap" && *o == "delete" => {
            delete_multi_region_access_point(req, st, body)
        }
        ("POST", [a, m, o]) if *a == "async-requests" && *m == "mrap" && *o == "put-policy" => {
            put_multi_region_access_point_policy(req, st, body)
        }
        ("GET", [a, m, rest @ ..]) if *a == "async-requests" && *m == "mrap" => {
            describe_multi_region_access_point_operation(req, st, &rest.join("/"))
        }
        // GET /v20180820/mrap/instances is the list; any other name is a detail
        ("GET", [m, s]) if *m == "mrap" && *s == "instances" => {
            list_multi_region_access_points(st, query)
        }
        // {name+} may be an ARN with raw slashes: everything between
        // "instances" and the trailing operation leaf is the name.
        ("GET", [m, i, rest @ ..]) if *m == "mrap" && *i == "instances" => {
            route_mrap_get(st, &segs[2..])
        }
        ("PATCH", [m, i, rest @ ..])
            if *m == "mrap" && *i == "instances" && rest.last().is_some_and(|s| s == "routes") =>
        {
            route_mrap_patch(st, &mrap_name_from(&segs[2..segs.len() - 1]))
        }
        ("GET", [m, _]) if *m == "mrap" => get_multi_region_access_point(st, &name_at(1)),

        // ------------------------------------------------------- storage lens
        ("GET", [s]) if *s == "storagelens" => list_storage_lens_configs(st),
        ("PUT", [s, _]) if *s == "storagelens" => {
            put_storage_lens_config(req, st, &name_at(1), body)
        }
        ("GET", [s, _]) if *s == "storagelens" => get_storage_lens_config(st, &name_at(1)),
        ("DELETE", [s, _]) if *s == "storagelens" => {
            delete_storage_lens_config(req, st, &name_at(1))
        }
        ("PUT", [s, _, t]) if *s == "storagelens" && *t == "tagging" => {
            put_storage_lens_tagging(req, st, &name_at(1), body)
        }
        ("GET", [s, _, t]) if *s == "storagelens" && *t == "tagging" => {
            get_storage_lens_tagging(req, st, &name_at(1))
        }
        ("DELETE", [s, _, t]) if *s == "storagelens" && *t == "tagging" => {
            delete_storage_lens_tagging(req, st, &name_at(1))
        }
        // ------------------------------------------------ storage lens groups
        ("POST", [s]) if *s == "storagelensgroup" => create_storage_lens_group(req, st, body),
        ("PUT", [s, _]) if *s == "storagelensgroup" => {
            update_storage_lens_group(st, &name_at(1), body)
        }
        ("GET", [s]) if *s == "storagelensgroup" => list_storage_lens_groups(st),
        ("GET", [s, _]) if *s == "storagelensgroup" => get_storage_lens_group(st, &name_at(1)),
        ("DELETE", [s, _]) if *s == "storagelensgroup" => {
            delete_storage_lens_group(st, &name_at(1))
        }
        // ------------------------------------------------------------- jobs
        ("GET", [j]) if *j == "jobs" => list_jobs(st, query),
        ("POST", [j]) if *j == "jobs" => create_job(st, body),
        ("GET", [j, _]) if *j == "jobs" => describe_job(st, &name_at(1)),
        ("POST", [j, _, l]) if *j == "jobs" && *l == "priority" => {
            update_job_priority(st, &name_at(1), query)
        }
        ("POST", [j, _, l]) if *j == "jobs" && *l == "status" => {
            update_job_status(st, &name_at(1), query)
        }
        ("PUT", [j, _, l]) if *j == "jobs" && *l == "tagging" => {
            put_job_tagging(st, &name_at(1), body)
        }
        ("GET", [j, _, l]) if *j == "jobs" && *l == "tagging" => get_job_tagging(st, &name_at(1)),
        ("DELETE", [j, _, l]) if *j == "jobs" && *l == "tagging" => {
            delete_job_tagging(st, &name_at(1))
        }
        // ------------------------------------------------------ access grants
        ("POST", [a]) if *a == "accessgrantsinstance" => {
            create_access_grants_instance(req, st, body)
        }
        ("GET", [a]) if *a == "accessgrantsinstance" => get_access_grants_instance(st),
        ("DELETE", [a]) if *a == "accessgrantsinstance" => delete_access_grants_instance(st),
        ("GET", [a]) if *a == "accessgrantsinstances" => list_access_grants_instances(st),
        ("GET", [a, p]) if *a == "accessgrantsinstance" && *p == "prefix" => {
            get_access_grants_instance_for_prefix(st)
        }
        ("PUT", [a, p]) if *a == "accessgrantsinstance" && *p == "resourcepolicy" => {
            put_access_grants_instance_resource_policy(req, st, body)
        }
        ("GET", [a, p]) if *a == "accessgrantsinstance" && *p == "resourcepolicy" => {
            get_access_grants_instance_resource_policy(st)
        }
        ("DELETE", [a, p]) if *a == "accessgrantsinstance" && *p == "resourcepolicy" => {
            delete_access_grants_instance_resource_policy(st)
        }
        ("POST", [a, p]) if *a == "accessgrantsinstance" && *p == "location" => {
            create_access_grants_location(req, st, body)
        }
        ("GET", [a, p]) if *a == "accessgrantsinstance" && *p == "locations" => {
            list_access_grants_locations(st)
        }
        ("GET", [a, p, _]) if *a == "accessgrantsinstance" && *p == "location" => {
            get_access_grants_location(st, &name_at(2))
        }
        ("PUT", [a, p, _]) if *a == "accessgrantsinstance" && *p == "location" => {
            update_access_grants_location(req, st, &name_at(2), body)
        }
        ("DELETE", [a, p, _]) if *a == "accessgrantsinstance" && *p == "location" => {
            delete_access_grants_location(st, &name_at(2))
        }
        ("POST", [a, p]) if *a == "accessgrantsinstance" && *p == "grant" => {
            create_access_grant(req, st, body)
        }
        ("GET", [a, p]) if *a == "accessgrantsinstance" && *p == "grants" => list_access_grants(st),
        ("GET", [a, p, _]) if *a == "accessgrantsinstance" && *p == "grant" => {
            get_access_grant(st, &name_at(2))
        }
        ("DELETE", [a, p, _]) if *a == "accessgrantsinstance" && *p == "grant" => {
            delete_access_grant(st, &name_at(2))
        }
        // -------------------------------------------------- object lambda aps
        ("PUT", [o, _]) if *o == "accesspointforobjectlambda" => {
            create_access_point_for_object_lambda(req, st, &name_at(1), body)
        }
        ("GET", [o, _]) if *o == "accesspointforobjectlambda" => {
            get_access_point_for_object_lambda(st, &name_at(1))
        }
        ("DELETE", [o, _]) if *o == "accesspointforobjectlambda" => {
            st.object_lambda_access_points.remove(&name_at(1));
            st.object_lambda_access_point_policies.remove(&name_at(1));
            empty_resp(204)
        }
        ("PUT", [o, _, l]) if *o == "accesspointforobjectlambda" && *l == "policy" => {
            put_olap_policy(st, &name_at(1), body)
        }
        ("GET", [o, _, l]) if *o == "accesspointforobjectlambda" && *l == "policy" => {
            get_olap_policy(st, &name_at(1))
        }
        ("DELETE", [o, _, l]) if *o == "accesspointforobjectlambda" && *l == "policy" => {
            st.object_lambda_access_point_policies.remove(&name_at(1));
            empty_resp(204)
        }
        ("GET", [o, _, l]) if *o == "accesspointforobjectlambda" && *l == "policyStatus" => {
            get_olap_policy_status()
        }
        // ------------------------------------------------ delegated bucket ops
        ("GET" | "PUT" | "DELETE", [b, _, l]) if *b == "bucket" => {
            bucket_op(req, st, &name_at(1), l)
        }
        // bare /v20180820/bucket/{name} (GetBucket/CreateBucket/DeleteBucket)
        // is outside moto's surface
        (_, [b, _]) if *b == "bucket" => not_implemented(),
        // -------------------------------------------------------- resource tags
        // The tags URI label spans slashes (moto's `(?P<arn>[\w_:%/-]+)`
        // gate), so the key is the whole tail — the full resource ARN.
        ("POST", [t, arn @ ..]) if *t == "tags" => tag_resource(st, &arn.join("/"), body),
        ("GET", [t, arn @ ..]) if *t == "tags" => list_tags_for_resource(st, &arn.join("/")),
        ("DELETE", [t, arn @ ..]) if *t == "tags" => {
            let keys = query.get("tagKeys").cloned().unwrap_or_default();
            untag_resource(st, &arn.join("/"), &keys)
        }
        // ------------------------------------------- ops outside moto's surface
        (_, [a, p]) if *a == "accessgrantsinstance" && *p == "identitycenter" => not_implemented(),
        (_, [a, p]) if *a == "accessgrantsinstance" && *p == "dataaccess" => not_implemented(),
        (_, [a, p, q]) if *a == "accessgrantsinstance" && *p == "caller" => not_implemented(),
        (_, [o]) if *o == "accesspointfordirectory" => not_implemented(),
        (_, [o]) if *o == "accesspointforobjectlambda" => not_implemented(),
        (_, [o, _, l]) if *o == "accesspointforobjectlambda" && *l == "configuration" => {
            not_implemented()
        }
        // --------------------------------------------------------------- else
        _ => not_implemented(),
    }
}

/// GET /v20180820/mrap/instances/{name+}[/leaf] — split name vs leaf.
fn route_mrap_get(st: &S3ControlStore, tail: &[String]) -> Response {
    // tail excludes the leading "instances" segment.
    if tail.is_empty() {
        return not_implemented();
    }
    match tail.last().map(String::as_str) {
        Some("policy") if tail.len() >= 2 => {
            get_mrap_policy(st, &mrap_name_from(&tail[..tail.len() - 1]))
        }
        Some("policystatus") if tail.len() >= 2 => {
            get_mrap_policy_status(st, &mrap_name_from(&tail[..tail.len() - 1]))
        }
        Some("routes") if tail.len() >= 2 => {
            get_mrap_routes(st, &mrap_name_from(&tail[..tail.len() - 1]))
        }
        _ => get_multi_region_access_point(st, &mrap_name_from(tail)),
    }
}

/// PATCH /v20180820/mrap/instances/{name+}/routes.
fn route_mrap_patch(st: &S3ControlStore, name: &str) -> Response {
    submit_mrap_routes(st, name)
}

/// MRAP name out of the trailing path segments (`{name+}` may carry an ARN;
/// moto takes the last `/`-separated piece after `/instances/`).
fn mrap_name_from(segs: &[String]) -> String {
    segs.join("/").rsplit('/').next().unwrap_or("").to_string()
}

// ---------------------------------------------------------------------------
// Public access block
// ---------------------------------------------------------------------------

fn get_public_access_block(req: &Request, st: &S3ControlStore) -> Response {
    if let Err(resp) = check_account(req) {
        return resp;
    }
    match &st.public_access_block {
        Some(fields) => resp(
            200,
            raw_el(
                "PublicAccessBlockConfiguration",
                [
                    el("BlockPublicAcls", fields[0].as_deref().unwrap_or("false")),
                    el("IgnorePublicAcls", fields[1].as_deref().unwrap_or("false")),
                    el("BlockPublicPolicy", fields[2].as_deref().unwrap_or("false")),
                    el(
                        "RestrictPublicBuckets",
                        fields[3].as_deref().unwrap_or("false"),
                    ),
                ]
                .concat(),
            ),
        ),
        None => xml_error(
            404,
            "NoSuchPublicAccessBlockConfiguration",
            "The public access block configuration was not found",
            "",
        ),
    }
}

fn put_public_access_block(req: &Request, st: &mut S3ControlStore, body: Option<&El>) -> Response {
    if let Err(resp) = check_account(req) {
        return resp;
    }
    // moto crashes on an empty body; a real client always sends one, either
    // as the bare configuration element or wrapped in PutPublicAccessBlockRequest.
    let cfg = body.and_then(|b| {
        req_el(Some(b), "PutPublicAccessBlockRequest")
            .and_then(|outer| outer.child("PublicAccessBlockConfiguration"))
            .or_else(|| req_el(Some(b), "PublicAccessBlockConfiguration"))
    });
    let Some(cfg) = cfg else {
        return s3_invalid_request("Must specify at least one configuration.");
    };
    let mut fields: [Option<String>; 4] = [None, None, None, None];
    for (idx, key) in PAB_KEYS.iter().enumerate() {
        fields[idx] = cfg.text_of(key).filter(|t| !t.is_empty()).map(String::from);
    }
    if fields.iter().all(|f| f.is_none()) {
        return s3_invalid_request("Must specify at least one configuration.");
    }
    st.public_access_block = Some(fields);
    // moto answers HTTP 201 with a JSON `{}` body on this operation.
    Response::json(201, b"{}".to_vec())
}

fn delete_public_access_block(req: &Request, st: &mut S3ControlStore) -> Response {
    if let Err(resp) = check_account(req) {
        return resp;
    }
    st.public_access_block = None;
    empty_resp(204)
}

fn s3_invalid_request(message: &str) -> Response {
    s3_error(400, "InvalidRequest", message)
}

const PAB_KEYS: [&str; 4] = [
    "BlockPublicAcls",
    "IgnorePublicAcls",
    "BlockPublicPolicy",
    "RestrictPublicBuckets",
];

// ---------------------------------------------------------------------------
// Access points
// ---------------------------------------------------------------------------

fn access_point_not_found(name: &str) -> Response {
    xml_error(
        404,
        "NoSuchAccessPoint",
        "The specified accesspoint does not exist",
        &el("AccessPointName", name),
    )
}

/// moto 5.2.4's pagination model raises InvalidNextTokenException for a
/// nextToken that is not a botocore token; the ServiceException serializer
/// renders it as the XML ErrorResponse, HTTP 400.
fn invalid_next_token() -> Response {
    xml_error(
        400,
        "InvalidNextTokenException",
        "The nextToken provided is invalid",
        "",
    )
}

/// moto's Paginator raises its own InvalidToken when a decodable page token
/// names an item that is not in the list; that JsonRESTError surfaces as a
/// JSON body, HTTP 400.
fn invalid_token_not_found() -> Response {
    let body = json!({
        "__type": "InvalidToken",
        "message": "Invalid Token: Resource not found!",
    });
    let mut resp = Response::json(400, serde_json::to_vec(&body).unwrap_or_default());
    resp.headers
        .push(("x-amz-ErrorType".to_string(), "InvalidToken:".to_string()));
    resp
}

/// botocore's TokenEncoder shape for moto's nextToken: base64 of
/// `{"uniqueAttributes": "<value>"}`, padding stripped. moto additionally
/// embeds a parameterChecksum derived from Python's salted `hash()`, which
/// is not wire-portable, so this twin mints only the attribute — and
/// likewise ignores the checksum member of an incoming token. Shared with
/// the other paginated lists (vpclattice `list_listeners`, the iot
/// PAGINATION_MODEL lists); multi-attribute tokens join their values with
/// `|` per moto's `Paginator._build_next_token`.
pub(crate) fn encode_s3control_token(value: &str) -> String {
    let encoded = base64::engine::general_purpose::STANDARD
        .encode(json!({ "uniqueAttributes": value }).to_string());
    encoded.trim_end_matches('=').to_string()
}

/// `None` when the token does not decode like a botocore page token
/// (padding optional, JSON object with a `uniqueAttributes` member).
pub(crate) fn decode_s3control_token(token: &str) -> Option<String> {
    let mut padded = token.to_string();
    let rem = padded.len() % 4;
    if rem != 0 {
        padded.push_str(&"=".repeat(4 - rem));
    }
    let decoded = base64::engine::general_purpose::STANDARD
        .decode(padded)
        .ok()?;
    let value: Value = serde_json::from_slice(&decoded).ok()?;
    value
        .get("uniqueAttributes")
        .and_then(Value::as_str)
        .map(String::from)
}

fn access_point_policy_not_found(name: &str) -> Response {
    xml_error(
        404,
        "NoSuchAccessPointPolicy",
        "The specified accesspoint policy does not exist",
        &el("AccessPointName", name),
    )
}

fn create_access_point(
    req: &Request,
    st: &mut S3ControlStore,
    name: &str,
    body: Option<&El>,
) -> Response {
    let Some(body) = body else {
        return not_implemented();
    };
    let bucket = body.text_or("Bucket");
    let vpc_id = body
        .sub(&["VpcConfiguration", "VpcId"])
        .map(El::as_str)
        .filter(|s| !s.is_empty())
        .map(String::from);
    let pubc = body.child("PublicAccessBlockConfiguration").map(|cfg| {
        BTreeMap::from([
            (
                "BlockPublicAcls".to_string(),
                cfg.text_or("BlockPublicAcls"),
            ),
            (
                "IgnorePublicAcls".to_string(),
                cfg.text_or("IgnorePublicAcls"),
            ),
            (
                "BlockPublicPolicy".to_string(),
                cfg.text_or("BlockPublicPolicy"),
            ),
            (
                "RestrictPublicBuckets".to_string(),
                cfg.text_or("RestrictPublicBuckets"),
            ),
        ])
    });
    // moto hard-codes us-east-1 in the access point ARN regardless of region.
    let arn = format!("arn:aws:s3:us-east-1:{}:accesspoint/{name}", req.account);
    let ap = AccessPoint {
        name: name.to_string(),
        alias: access_point_alias(name),
        bucket,
        created: moto_ts_nozone(),
        arn,
        network_origin: if vpc_id.is_some() { "VPC" } else { "Internet" }.to_string(),
        vpc_id,
        pubc,
        policy: None,
    };
    let alias = ap.alias.clone();
    let arn = ap.arn.clone();
    st.access_points.insert(name.to_string(), ap);
    // moto registers request Tags on the s3 TaggingService, keyed by the
    // access point ARN (`if tags:` — nothing stored without them); boto3
    // surfaces them through the ListTagsForResource route, not the
    // get/list access point results (those members have no Tags shape).
    if let Some(tags) = body.child("Tags") {
        let pairs = tags.tag_pairs();
        if !pairs.is_empty() {
            let row = st.resource_tags.entry(arn.clone()).or_default();
            // TaggingService.tag_resource upserts key by key.
            for (key, value) in pairs {
                match row.iter_mut().find(|(k, _)| *k == key) {
                    Some(row) => row.1 = value,
                    None => row.push((key, value)),
                }
            }
        }
    }
    resp(
        200,
        raw_el(
            "CreateAccessPointResult",
            [el("Alias", &alias), el("AccessPointArn", &arn)].concat(),
        ),
    )
}

/// moto's `list_access_points`: optional bucket filter, `maxResults` paging
/// keyed on the access point name, default limit 1000. The 5.2.4
/// pagination model validates `nextToken`: the token names the access
/// point the page starts at, in botocore's token encoding
/// (base64 `{"uniqueAttributes": "<name>"}`); an undecodable token answers
/// the modeled `InvalidNextTokenException`.
fn list_access_points(st: &S3ControlStore, query: &BTreeMap<String, Vec<String>>) -> Response {
    let bucket_filter = query
        .get("bucket")
        .and_then(|v| v.first())
        .cloned()
        .unwrap_or_default();
    let start_name = match query
        .get("nextToken")
        .and_then(|v| v.first())
        .map(String::as_str)
    {
        Some(token) => match decode_s3control_token(token) {
            Some(unique) => unique,
            None => return invalid_next_token(),
        },
        None => String::new(),
    };
    let max = query
        .get("maxResults")
        .and_then(|v| v.first())
        .and_then(|v| v.parse::<usize>().ok())
        .unwrap_or(1000);
    let matching: Vec<&AccessPoint> = st
        .access_points
        .iter()
        .filter(|(_, ap)| bucket_filter.is_empty() || ap.bucket == bucket_filter)
        .map(|(_, ap)| ap)
        .collect();
    // moto's Paginator positions the page at the token's access point; a
    // decodable token naming no access point here raises its InvalidToken
    // ("Resource not found!"), surfacing as the JsonRESTError JSON body.
    let start = if start_name.is_empty() {
        0usize
    } else {
        match matching.iter().position(|ap| ap.name == start_name) {
            Some(pos) => pos,
            None => return invalid_token_not_found(),
        }
    };
    let page: Vec<&&AccessPoint> = matching[start..].iter().take(max).collect();
    let mut out = String::new();
    for ap in page {
        out += &raw_el(
            "AccessPoint",
            [
                el("Name", &ap.name),
                el("NetworkOrigin", &ap.network_origin),
                ap.vpc_id
                    .as_ref()
                    .map(|id| raw_el("VpcConfiguration", el("VpcId", id)))
                    .unwrap_or_default(),
                el("Bucket", &ap.bucket),
                el("AccessPointArn", &ap.arn),
                el("Alias", &ap.alias),
            ]
            .concat(),
        );
    }
    // moto mints the token from `results[index_end]` — the first EXCLUDED
    // access point — only when the page is non-empty and paging continues.
    let mut next_token = String::new();
    if start + max < matching.len() && max > 0 && !out.is_empty() {
        next_token = el(
            "NextToken",
            encode_s3control_token(&matching[start + max].name),
        );
    }
    resp(
        200,
        raw_el(
            "ListAccessPointsResult",
            [raw_el("AccessPointList", out), next_token].concat(),
        ),
    )
}

fn get_access_point(st: &S3ControlStore, name: &str) -> Response {
    let Some(ap) = st.access_points.get(name) else {
        return access_point_not_found(name);
    };
    let vpc = ap
        .vpc_id
        .as_ref()
        .map(|id| raw_el("VpcConfiguration", el("VpcId", id)))
        .unwrap_or_default();
    let endpoints = ENDPOINTS
        .iter()
        .map(|(k, v)| raw_el("entry", [el("key", k), el("value", v)].concat()))
        .collect::<String>();
    resp(
        200,
        raw_el(
            "GetAccessPointResult",
            [
                el("Name", &ap.name),
                el("Bucket", &ap.bucket),
                el("NetworkOrigin", &ap.network_origin),
                vpc,
                raw_el(
                    "PublicAccessBlockConfiguration",
                    [
                        el("BlockPublicAcls", ap.pubc_field("BlockPublicAcls")),
                        el("IgnorePublicAcls", ap.pubc_field("IgnorePublicAcls")),
                        el("BlockPublicPolicy", ap.pubc_field("BlockPublicPolicy")),
                        el(
                            "RestrictPublicBuckets",
                            ap.pubc_field("RestrictPublicBuckets"),
                        ),
                    ]
                    .concat(),
                ),
                el("CreationDate", &ap.created),
                el("Alias", &ap.alias),
                el("AccessPointArn", &ap.arn),
                raw_el("Endpoints", endpoints),
            ]
            .concat(),
        ),
    )
}

const ENDPOINTS: [(&str, &str); 4] = [
    ("ipv4", "s3-accesspoint.us-east-1.amazonaws.com"),
    ("fips", "s3-accesspoint-fips.us-east-1.amazonaws.com"),
    (
        "fips_dualstack",
        "s3-accesspoint-fips.dualstack.us-east-1.amazonaws.com",
    ),
    (
        "dualstack",
        "s3-accesspoint.dualstack.us-east-1.amazonaws.com",
    ),
];

fn put_access_point_policy(st: &mut S3ControlStore, name: &str, body: Option<&El>) -> Response {
    if !st.access_points.contains_key(name) {
        return access_point_not_found(name);
    }
    // moto indexes the policy strictly: a wrapper without <Policy> crashes
    // (KeyError → 500), mirroring the probe-recorded crash shape.
    match body.and_then(|b| b.text_of("Policy")) {
        Some(policy) => {
            st.access_points.get_mut(name).unwrap().policy = Some(policy.to_string());
            resp(200, "")
        }
        None => xml_error(500, "InternalError", "'Policy'", ""),
    }
}

fn get_access_point_policy(st: &S3ControlStore, name: &str) -> Response {
    match st.access_points.get(name) {
        Some(ap) => match &ap.policy {
            Some(policy) => resp(
                200,
                raw_el("GetAccessPointPolicyResult", el("Policy", policy)),
            ),
            None => access_point_policy_not_found(name),
        },
        None => access_point_not_found(name),
    }
}

fn get_access_point_policy_status(st: &S3ControlStore, name: &str) -> Response {
    match st.access_points.get(name) {
        Some(ap) if ap.policy.is_some() => resp(
            200,
            raw_el(
                "GetAccessPointPolicyResult",
                raw_el("PolicyStatus", el("IsPublic", "true")),
            ),
        ),
        Some(_) => access_point_policy_not_found(name),
        None => access_point_not_found(name),
    }
}

fn put_access_point_scope(st: &mut S3ControlStore, name: &str, body: Option<&El>) -> Response {
    let Some(scope) = body
        .and_then(|b| b.child("Scope"))
        .or_else(|| body.filter(|b| b.name == "Scope"))
    else {
        return not_implemented();
    };
    st.access_point_scopes
        .insert(name.to_string(), scope.clone());
    resp(200, "")
}

fn get_access_point_scope(st: &S3ControlStore, name: &str) -> Response {
    let Some(scope) = st.access_point_scopes.get(name) else {
        return resp(200, empty_scope_result());
    };
    let mut inner = String::new();
    for c in &scope.children {
        inner += &raw_el(&c.name, esc(c.as_str()));
    }
    resp(
        200,
        raw_el(
            "GetAccessPointScopeResult",
            if inner.is_empty() {
                String::new()
            } else {
                raw_el("Scope", inner)
            },
        ),
    )
}

fn empty_scope_result() -> String {
    format!(
        "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n<GetAccessPointScopeResult xmlns=\"{XMLNS}\"></GetAccessPointScopeResult>"
    )
}

// ---------------------------------------------------------------------------
// Multi-region access points
// ---------------------------------------------------------------------------

fn mrap_not_found(name: &str) -> Response {
    xml_error(
        404,
        "NoSuchMultiRegionAccessPoint",
        "The specified multi-region access point does not exist",
        &el("Name", name),
    )
}

fn mrap_policy_not_found(name: &str) -> Response {
    xml_error(
        404,
        "NoSuchMultiRegionAccessPointPolicy",
        "The specified multi-region access point policy does not exist",
        &el("Name", name),
    )
}

fn async_request_not_found(token: &str) -> Response {
    xml_error(
        404,
        "NoSuchAsyncRequest",
        "The specified async request does not exist",
        &el("RequestTokenARN", token),
    )
}

fn create_multi_region_access_point(
    req: &Request,
    st: &mut S3ControlStore,
    body: Option<&El>,
) -> Response {
    let Some(details) = body.and_then(|b| b.child("Details")) else {
        return not_implemented();
    };
    let name = details.text_or("Name");
    if name.is_empty() {
        return not_implemented();
    }
    if st.mrap_access_points.contains_key(&name) {
        return xml_error(
            400,
            "InvalidRequest",
            &format!("Multi-Region Access Point {name} already exists"),
            "",
        );
    }
    let mut regions: Vec<(String, String)> = Vec::new();
    if let Some(regions_el) = details.child("Regions") {
        for region in regions_el.parts("Region") {
            let bucket = region.text_or("Bucket");
            let found = bucket_region(req.account, &bucket, &req.region);
            regions.push((bucket, found));
        }
    }
    let pubc = details.child("PublicAccessBlock").map(|cfg| {
        BTreeMap::from([
            (
                "BlockPublicAcls".to_string(),
                cfg.text_or("BlockPublicAcls"),
            ),
            (
                "IgnorePublicAcls".to_string(),
                cfg.text_or("IgnorePublicAcls"),
            ),
            (
                "BlockPublicPolicy".to_string(),
                cfg.text_or("BlockPublicPolicy"),
            ),
            (
                "RestrictPublicBuckets".to_string(),
                cfg.text_or("RestrictPublicBuckets"),
            ),
        ])
    });
    let token = format!(
        "arn:aws:s3:{}:{}:async-request/mrap/createmultiregionaccesspoint/{}",
        req.region,
        req.account,
        random_hex(24)
    );
    st.mrap_operations.insert(
        token.clone(),
        MrapOperation {
            request_token_arn: token.clone(),
            name: name.clone(),
            operation: "CreateMultiRegionAccessPoint".to_string(),
            created_at: moto_ts(),
            regions: regions.clone(),
            public_access_block: pubc.clone(),
            policy: None,
        },
    );
    st.mrap_access_points.insert(
        name.clone(),
        Mrap {
            name,
            alias: mrap_alias(&details.text_or("Name")),
            created_at: moto_ts(),
            pubc,
            regions,
            policy: None,
        },
    );
    resp(
        200,
        xmlns_el(
            "CreateMultiRegionAccessPointResult",
            el("RequestTokenARN", &token),
        ),
    )
}

fn delete_multi_region_access_point(
    req: &Request,
    st: &mut S3ControlStore,
    body: Option<&El>,
) -> Response {
    let Some(name) = body.and_then(|b| b.sub(&["Details", "Name"])) else {
        return not_implemented();
    };
    let name = name.as_str().to_string();
    if !st.mrap_access_points.contains_key(&name) {
        return mrap_not_found(&name);
    }
    st.mrap_access_points.remove(&name);
    let token = format!(
        "arn:aws:s3:{}:{}:async-request/mrap/deletemultiregionaccesspoint/{}",
        req.region,
        req.account,
        random_hex(24)
    );
    st.mrap_operations.insert(
        token.clone(),
        MrapOperation {
            request_token_arn: token.clone(),
            name,
            operation: "DeleteMultiRegionAccessPoint".to_string(),
            created_at: moto_ts(),
            regions: Vec::new(),
            public_access_block: None,
            policy: None,
        },
    );
    resp(
        200,
        xmlns_el(
            "DeleteMultiRegionAccessPointResult",
            el("RequestTokenARN", &token),
        ),
    )
}

fn put_multi_region_access_point_policy(
    req: &Request,
    st: &mut S3ControlStore,
    body: Option<&El>,
) -> Response {
    let Some(details) = body.and_then(|b| b.child("Details")) else {
        return not_implemented();
    };
    let name = details.text_or("Name");
    let policy = details.text_or("Policy");
    let Some(mrap) = st.mrap_access_points.get_mut(&name) else {
        return mrap_not_found(&name);
    };
    mrap.policy = Some(policy.clone());
    let token = format!(
        "arn:aws:s3:{}:{}:async-request/mrap/putmultiregionaccesspointpolicy/{}",
        req.region,
        req.account,
        random_hex(24)
    );
    st.mrap_operations.insert(
        token.clone(),
        MrapOperation {
            request_token_arn: token.clone(),
            name,
            operation: "PutMultiRegionAccessPointPolicy".to_string(),
            created_at: moto_ts(),
            regions: Vec::new(),
            public_access_block: None,
            policy: Some(policy),
        },
    );
    resp(
        200,
        xmlns_el(
            "PutMultiRegionAccessPointPolicyResult",
            el("RequestTokenARN", &token),
        ),
    )
}

fn describe_multi_region_access_point_operation(
    _req: &Request,
    st: &S3ControlStore,
    token: &str,
) -> Response {
    if token.is_empty() {
        return async_request_not_found(token);
    }
    let Some(op) = st.mrap_operations.get(token) else {
        return async_request_not_found(token);
    };
    let mut params = String::new();
    match op.operation.as_str() {
        "CreateMultiRegionAccessPoint" => {
            let pubc = match &op.public_access_block {
                Some(m) => {
                    let block = m
                        .get("BlockPublicAcls")
                        .cloned()
                        .unwrap_or_else(|| "true".to_string());
                    let ignore = m
                        .get("IgnorePublicAcls")
                        .cloned()
                        .unwrap_or_else(|| "true".to_string());
                    let policy = m
                        .get("BlockPublicPolicy")
                        .cloned()
                        .unwrap_or_else(|| "true".to_string());
                    let restrict = m
                        .get("RestrictPublicBuckets")
                        .cloned()
                        .unwrap_or_else(|| "true".to_string());
                    raw_el(
                        "PublicAccessBlock",
                        [
                            el("BlockPublicAcls", block),
                            el("IgnorePublicAcls", ignore),
                            el("BlockPublicPolicy", policy),
                            el("RestrictPublicBuckets", restrict),
                        ]
                        .concat(),
                    )
                }
                // moto's template default only fires when the key is missing;
                // a created request always carries the four booleans, absent
                // PABs render the python-bool "True" (parsed as false).
                None => raw_el(
                    "PublicAccessBlock",
                    PAB_KEYS.iter().map(|k| el(k, "True")).collect::<String>(),
                ),
            };
            let regions = op
                .regions
                .iter()
                .map(|(bucket, _)| raw_el("Region", el("Bucket", bucket)))
                .collect::<String>();
            params = raw_el(
                "CreateMultiRegionAccessPointRequest",
                [el("Name", &op.name), pubc, raw_el("Regions", regions)].concat(),
            );
        }
        "DeleteMultiRegionAccessPoint" => {
            params = raw_el("DeleteMultiRegionAccessPointRequest", el("Name", &op.name));
        }
        "PutMultiRegionAccessPointPolicy" => {
            let policy = op.policy.clone().unwrap_or_default();
            params = raw_el(
                "PutMultiRegionAccessPointPolicyRequest",
                [el("Name", &op.name), el("Policy", &policy)].concat(),
            );
        }
        _ => {}
    }
    let mut details = String::new();
    if op.operation == "CreateMultiRegionAccessPoint" {
        let regions = op
            .regions
            .iter()
            .map(|(_, region)| {
                raw_el(
                    "Region",
                    [el("Name", region), el("RequestStatus", "SUCCEEDED")].concat(),
                )
            })
            .collect::<String>();
        details = raw_el("MultiRegionAccessPointDetails", raw_el("Regions", regions));
    } else if op.operation == "DeleteMultiRegionAccessPoint" {
        details = raw_el("MultiRegionAccessPointDetails", raw_el("Regions", ""));
    }
    resp(
        200,
        xmlns_el(
            "DescribeMultiRegionAccessPointOperationResult",
            raw_el(
                "AsyncOperation",
                [
                    el("CreationTime", &op.created_at),
                    el("Operation", &op.operation),
                    el("RequestTokenARN", &op.request_token_arn),
                    el("RequestStatus", "SUCCEEDED"),
                    raw_el("RequestParameters", params),
                    raw_el("ResponseDetails", details),
                ]
                .concat(),
            ),
        ),
    )
}

fn mrap_dict(
    name: &str,
    alias: &str,
    created_at: &str,
    pubc_map: &Option<BTreeMap<String, String>>,
    regions: &[(String, String)],
) -> String {
    let pubc = match pubc_map {
        Some(m) => PAB_KEYS
            .iter()
            .map(|k| {
                let field = m.get(*k).cloned().unwrap_or_else(|| "true".to_string());
                el(k, field)
            })
            .collect::<String>(),
        None => PAB_KEYS.iter().map(|k| el(k, "True")).collect::<String>(),
    };
    let regions_xml = regions
        .iter()
        .map(|(bucket, region)| {
            raw_el(
                "Region",
                [el("Bucket", bucket), el("Region", region)].concat(),
            )
        })
        .collect::<String>();
    [
        el("Name", name),
        el("Alias", alias),
        el("CreatedAt", created_at),
        raw_el("PublicAccessBlock", pubc),
        el("Status", "READY"),
        raw_el("Regions", regions_xml),
    ]
    .concat()
}

fn get_multi_region_access_point(st: &S3ControlStore, name: &str) -> Response {
    let Some(mrap) = st.mrap_access_points.get(name) else {
        return mrap_not_found(name);
    };
    let inner = mrap_dict(
        &mrap.name,
        &mrap.alias,
        &mrap.created_at,
        &mrap.pubc,
        &mrap.regions,
    );
    resp(
        200,
        xmlns_el(
            "GetMultiRegionAccessPointResult",
            raw_el("AccessPoint", inner),
        ),
    )
}

fn list_multi_region_access_points(
    st: &S3ControlStore,
    query: &BTreeMap<String, Vec<String>>,
) -> Response {
    let after = query
        .get("nextToken")
        .and_then(|v| v.first())
        .cloned()
        .unwrap_or_default();
    let max = query
        .get("maxResults")
        .and_then(|v| v.first())
        .and_then(|v| v.parse::<usize>().ok())
        .unwrap_or(100);
    let mut out = String::new();
    let mut last = String::new();
    let mut count = 0usize;
    for (name, mrap) in &st.mrap_access_points {
        if !after.is_empty() && *name <= after {
            continue;
        }
        if count >= max {
            break;
        }
        let inner = mrap_dict(
            &mrap.name,
            &mrap.alias,
            &mrap.created_at,
            &mrap.pubc,
            &mrap.regions,
        );
        out += &raw_el("AccessPoint", inner);
        last = name.clone();
        count += 1;
    }
    let next_token = if count >= max && !last.is_empty() && count < st.mrap_access_points.len() {
        el("NextToken", &last)
    } else {
        String::new()
    };
    resp(
        200,
        xmlns_el(
            "ListMultiRegionAccessPointsResult",
            [raw_el("AccessPoints", out), next_token].concat(),
        ),
    )
}

fn get_mrap_policy(st: &S3ControlStore, name: &str) -> Response {
    let Some(mrap) = st.mrap_access_points.get(name) else {
        return mrap_not_found(name);
    };
    let Some(policy) = &mrap.policy else {
        return mrap_policy_not_found(name);
    };
    resp(
        200,
        xmlns_el(
            "GetMultiRegionAccessPointPolicyResult",
            raw_el("Policy", raw_el("Established", el("Policy", policy))),
        ),
    )
}

/// moto's `_is_policy_public`: blocked when BlockPublicPolicy is true,
/// otherwise public when any statement uses the `*` principal shape.
fn is_policy_public(mrap: &Mrap) -> bool {
    let block = mrap
        .pubc
        .as_ref()
        .and_then(|m| m.get("BlockPublicPolicy"))
        .map(|v| v == "true" || v == "True")
        .unwrap_or(false);
    if block {
        return false;
    }
    let Some(policy) = &mrap.policy else {
        return false;
    };
    let compact = policy.replace(' ', "");
    compact.contains(r#""Principal":"*""#) || compact.contains(r#""Principal":{"AWS":"*""#)
}

fn get_mrap_policy_status(st: &S3ControlStore, name: &str) -> Response {
    let Some(mrap) = st.mrap_access_points.get(name) else {
        return mrap_not_found(name);
    };
    if mrap.policy.is_none() {
        return mrap_policy_not_found(name);
    }
    let is_public = is_policy_public(mrap);
    resp(
        200,
        xmlns_el(
            "GetMultiRegionAccessPointPolicyStatusResult",
            raw_el(
                "Established",
                el("IsPublic", if is_public { "true" } else { "false" }),
            ),
        ),
    )
}

fn get_mrap_routes(st: &S3ControlStore, name: &str) -> Response {
    let Some(mrap) = st.mrap_access_points.get(name) else {
        return mrap_not_found(name);
    };
    let routes = mrap
        .regions
        .iter()
        .map(|(bucket, region)| {
            raw_el(
                "Route",
                [
                    el("Bucket", bucket),
                    el("Region", region),
                    el("TrafficDialPercentage", "100"),
                ]
                .concat(),
            )
        })
        .collect::<String>();
    resp(
        200,
        xmlns_el(
            "GetMultiRegionAccessPointRoutesResult",
            [el("Mrap", name), raw_el("Routes", routes)].concat(),
        ),
    )
}

fn submit_mrap_routes(st: &S3ControlStore, name: &str) -> Response {
    if !st.mrap_access_points.contains_key(name) {
        return mrap_not_found(name);
    }
    resp(200, "")
}

// ---------------------------------------------------------------------------
// Storage Lens
// ---------------------------------------------------------------------------

fn storage_lens_config_not_found(config_id: &str) -> Response {
    xml_error(
        404,
        "NoSuchConfiguration",
        &format!("The specified configuration does not exist: {config_id}"),
        "",
    )
}

fn put_storage_lens_config(
    req: &Request,
    st: &mut S3ControlStore,
    config_id: &str,
    body: Option<&El>,
) -> Response {
    if let Err(resp) = check_account(req) {
        return resp;
    }
    let config = body.and_then(|b| b.child("StorageLensConfiguration"));
    let tags = body.and_then(|b| b.child("Tags"));
    st.storage_lens_configs.insert(
        config_id.to_string(),
        StorageLensConfiguration {
            arn: format!(
                "arn:aws:s3:us-east-1:{}:storage-lens/{config_id}",
                req.account
            ),
            config: config.cloned().unwrap_or_default(),
            tags: tags.cloned(),
        },
    );
    resp(200, "")
}

fn get_storage_lens_config(st: &S3ControlStore, config_id: &str) -> Response {
    let Some(config) = st.storage_lens_configs.get(config_id) else {
        return storage_lens_config_not_found(config_id);
    };
    resp(200, render_storage_lens_config(&config.config))
}

/// moto renders selected members of the stored configuration; ids and flags
/// come straight back, unknown members stay off the wire.
fn render_storage_lens_config(config: &El) -> String {
    let mut out = String::new();
    if let Some(id) = config.text_of("Id") {
        out += &el("Id", id);
    }
    if let Some(data_export) = config.child("DataExport") {
        out += &raw_el("DataExport", {
            let mut inner = String::new();
            if let Some(suffix) = data_export.child("S3BucketDestination") {
                inner += &raw_el(
                    "S3BucketDestination",
                    [
                        el("AccountId", suffix.text_or("AccountId")),
                        el("Arn", suffix.text_or("Arn")),
                    ]
                    .concat(),
                );
            }
            inner
        });
    }
    out += &el("IsEnabled", config.text_or("IsEnabled"));
    raw_el("StorageLensConfiguration", out)
}

fn delete_storage_lens_config(req: &Request, st: &mut S3ControlStore, config_id: &str) -> Response {
    if let Err(resp) = check_account(req) {
        return resp;
    }
    if st.storage_lens_configs.remove(config_id).is_none() {
        return storage_lens_config_not_found(config_id);
    }
    empty_resp(204)
}

fn list_storage_lens_configs(st: &S3ControlStore) -> Response {
    let mut out = String::new();
    for config in st.storage_lens_configs.values() {
        out += &raw_el(
            "StorageLensConfiguration",
            [
                el("HomeRegion", ""),
                el("Id", config.config.text_or("Id")),
                el("IsEnabled", config.config.text_or("IsEnabled")),
                el("StorageLensArn", &config.arn),
            ]
            .concat(),
        );
    }
    resp(200, raw_el("ListStorageLensConfigurationsResult", out))
}

fn put_storage_lens_tagging(
    req: &Request,
    st: &mut S3ControlStore,
    config_id: &str,
    body: Option<&El>,
) -> Response {
    if let Err(resp) = check_account(req) {
        return resp;
    }
    // moto raises AccessPointNotFound (not the storage lens error) here.
    let Some(config) = st.storage_lens_configs.get_mut(config_id) else {
        return access_point_not_found(config_id);
    };
    config.tags = body.and_then(|b| b.child("Tags")).cloned();
    resp(200, "")
}

fn get_storage_lens_tagging(req: &Request, st: &S3ControlStore, config_id: &str) -> Response {
    if let Err(resp) = check_account(req) {
        return resp;
    }
    // moto raises AccessPointNotFound for a missing configuration here too.
    let Some(config) = st.storage_lens_configs.get(config_id) else {
        return access_point_not_found(config_id);
    };
    let mut inner = String::new();
    if let Some(tags) = &config.tags {
        for tag in tags.parts("Tag") {
            inner += &raw_el(
                "Tag",
                [
                    el("Key", tag.text_or("Key")),
                    el("Value", tag.text_or("Value")),
                ]
                .concat(),
            );
        }
    }
    resp(
        200,
        raw_el(
            "GetStorageLensConfigurationTaggingResult",
            raw_el("Tags", inner),
        ),
    )
}

fn delete_storage_lens_tagging(
    req: &Request,
    st: &mut S3ControlStore,
    config_id: &str,
) -> Response {
    if let Err(resp) = check_account(req) {
        return resp;
    }
    match st.storage_lens_configs.get_mut(config_id) {
        Some(config) => {
            config.tags = None;
            resp(200, "")
        }
        None => storage_lens_config_not_found(config_id),
    }
}

// ---------------------------------------------------------------------------
// Storage Lens Groups
// ---------------------------------------------------------------------------

fn storage_lens_group_not_found(name: &str) -> Response {
    xml_error(
        404,
        "NoSuchStorageLensGroup",
        &format!("The specified Storage Lens group does not exist: {name}"),
        "",
    )
}

fn create_storage_lens_group(
    req: &Request,
    st: &mut S3ControlStore,
    body: Option<&El>,
) -> Response {
    let Some(group) = body.and_then(|b| b.child("StorageLensGroup")) else {
        return not_implemented();
    };
    let name = group.text_or("Name");
    let tags = body
        .and_then(|b| b.child("Tags"))
        .map(|t| t.tag_pairs())
        .unwrap_or_default();
    let arn = format!(
        "arn:aws:s3:{}:{}:storage-lens-group/{name}",
        req.region, req.account
    );
    let group_arn = arn.clone();
    st.storage_lens_groups.insert(
        name.clone(),
        StorageLensGroup {
            name: name.clone(),
            arn,
            tags,
        },
    );
    resp(
        200,
        xmlns_el(
            "CreateStorageLensGroupResult",
            raw_el(
                "StorageLensGroup",
                [el("Name", &name), el("StorageLensGroupArn", &group_arn)].concat(),
            ),
        ),
    )
}

/// moto's list: every group rendered as `StorageLensGroup` members; paging
/// uses a documented single-page model here (limit 100, no maxResults).
fn list_storage_lens_groups(st: &S3ControlStore) -> Response {
    let inner = st
        .storage_lens_groups
        .values()
        .map(|group| {
            raw_el(
                "StorageLensGroup",
                [
                    el("Name", &group.name),
                    el("StorageLensGroupArn", &group.arn),
                ]
                .concat(),
            )
        })
        .collect::<String>();
    resp(
        200,
        xmlns_el(
            "ListStorageLensGroupsResult",
            raw_el("StorageLensGroupList", inner),
        ),
    )
}

fn get_storage_lens_group(st: &S3ControlStore, name: &str) -> Response {
    let Some(group) = st.storage_lens_groups.get(name) else {
        return storage_lens_group_not_found(name);
    };
    resp(
        200,
        xmlns_el(
            "GetStorageLensGroupResult",
            raw_el(
                "StorageLensGroup",
                [
                    el("Name", &group.name),
                    el("StorageLensGroupArn", &group.arn),
                ]
                .concat(),
            ),
        ),
    )
}

fn update_storage_lens_group(st: &mut S3ControlStore, name: &str, body: Option<&El>) -> Response {
    if !st.storage_lens_groups.contains_key(name) {
        return storage_lens_group_not_found(name);
    }
    // moto swaps the stored group content but keeps the keyed name and ARN.
    if let Some(group) = body.and_then(|b| b.child("StorageLensGroup")) {
        let tags = body
            .and_then(|b| b.child("Tags"))
            .map(|t| t.tag_pairs())
            .unwrap_or_default();
        if let Some(existing) = st.storage_lens_groups.get_mut(name) {
            existing.name = group.text_or("Name");
            existing.tags = tags;
        }
    }
    resp(200, "")
}

fn delete_storage_lens_group(st: &mut S3ControlStore, name: &str) -> Response {
    if st.storage_lens_groups.remove(name).is_none() {
        return storage_lens_group_not_found(name);
    }
    empty_resp(204)
}

// ---------------------------------------------------------------------------
// Jobs
// ---------------------------------------------------------------------------

fn job_not_found(job_id: &str) -> Response {
    xml_error(
        404,
        "NoSuchJob",
        &format!("The specified job does not exist: {job_id}"),
        "",
    )
}

fn create_job(st: &mut S3ControlStore, body: Option<&El>) -> Response {
    let Some(body) = body else {
        return not_implemented();
    };
    let priority = body
        .text_of("Priority")
        .and_then(|p| p.parse::<i64>().ok())
        .unwrap_or(0);
    let confirmation = body
        .text_of("ConfirmationRequired")
        .is_some_and(|t| t.eq_ignore_ascii_case("true"));
    let job = Job {
        job_id: uuid::Uuid::new_v4().to_string(),
        status: if confirmation { "Suspended" } else { "Ready" }.to_string(),
        description: body.text_or("Description"),
        priority,
        role_arn: body.text_or("RoleArn"),
        created_at: moto_ts(),
    };
    let job_id = job.job_id.clone();
    st.jobs.insert(job_id.clone(), job);
    resp(200, xmlns_el("CreateJobResult", el("JobId", &job_id)))
}

fn describe_job(st: &S3ControlStore, job_id: &str) -> Response {
    let Some(job) = st.jobs.get(job_id) else {
        return job_not_found(job_id);
    };
    resp(
        200,
        xmlns_el(
            "DescribeJobResult",
            raw_el(
                "Job",
                [
                    el("JobId", &job.job_id),
                    el("Status", &job.status),
                    el("Description", &job.description),
                    el("Priority", job.priority.to_string()),
                    el("RoleArn", &job.role_arn),
                    el("CreationTime", &job.created_at),
                    raw_el(
                        "ProgressSummary",
                        [
                            el("NumberOfTasksSucceeded", "0"),
                            el("NumberOfTasksFailed", "0"),
                            el("TotalNumberOfTasks", "0"),
                        ]
                        .concat(),
                    ),
                ]
                .concat(),
            ),
        ),
    )
}

fn list_jobs(st: &S3ControlStore, query: &BTreeMap<String, Vec<String>>) -> Response {
    let after = query
        .get("nextToken")
        .and_then(|v| v.first())
        .cloned()
        .unwrap_or_default();
    let max = query
        .get("maxResults")
        .and_then(|v| v.first())
        .and_then(|v| v.parse::<usize>().ok())
        .unwrap_or(1000);
    let mut out = String::new();
    let mut last = String::new();
    let mut count = 0usize;
    for (id, job) in &st.jobs {
        if !after.is_empty() && *id <= after {
            continue;
        }
        if count >= max {
            break;
        }
        out += &raw_el(
            "member",
            [
                el("JobId", &job.job_id),
                el("Status", &job.status),
                el("Description", &job.description),
                el("Priority", job.priority.to_string()),
                el("CreationTime", &job.created_at),
            ]
            .concat(),
        );
        last = id.clone();
        count += 1;
    }
    let next_token = if count >= max && !last.is_empty() && count < st.jobs.len() {
        el("NextToken", &last)
    } else {
        String::new()
    };
    resp(
        200,
        xmlns_el("ListJobsResult", [raw_el("Jobs", out), next_token].concat()),
    )
}

fn update_job_priority(
    st: &mut S3ControlStore,
    job_id: &str,
    query: &BTreeMap<String, Vec<String>>,
) -> Response {
    let Some(job) = st.jobs.get_mut(job_id) else {
        return job_not_found(job_id);
    };
    let priority = query
        .get("priority")
        .and_then(|v| v.first())
        .and_then(|v| v.parse::<i64>().ok())
        .unwrap_or(0);
    job.priority = priority;
    resp(
        200,
        xmlns_el(
            "UpdateJobPriorityResult",
            [el("JobId", job_id), el("Priority", priority.to_string())].concat(),
        ),
    )
}

fn update_job_status(
    st: &mut S3ControlStore,
    job_id: &str,
    query: &BTreeMap<String, Vec<String>>,
) -> Response {
    let Some(job) = st.jobs.get_mut(job_id) else {
        return job_not_found(job_id);
    };
    let status = query
        .get("requestedJobStatus")
        .and_then(|v| v.first())
        .cloned()
        .unwrap_or_default();
    if !status.is_empty() {
        job.status = status.clone();
    }
    resp(
        200,
        xmlns_el(
            "UpdateJobStatusResult",
            [el("JobId", job_id), el("Status", &job.status)].concat(),
        ),
    )
}

fn put_job_tagging(st: &mut S3ControlStore, job_id: &str, body: Option<&El>) -> Response {
    if !st.jobs.contains_key(job_id) {
        return job_not_found(job_id);
    }
    let tags = body
        .and_then(|b| b.child("Tags"))
        .map(|t| t.tag_pairs())
        .unwrap_or_default();
    st.job_tags.insert(job_id.to_string(), tags);
    resp(200, "")
}

fn get_job_tagging(st: &S3ControlStore, job_id: &str) -> Response {
    if !st.jobs.contains_key(job_id) {
        return job_not_found(job_id);
    }
    let mut inner = String::new();
    if let Some(tags) = st.job_tags.get(job_id) {
        for (key, value) in tags {
            inner += &raw_el("Tag", [el("Key", key), el("Value", value)].concat());
        }
    }
    resp(200, xmlns_el("GetJobTaggingResult", raw_el("Tags", inner)))
}

fn delete_job_tagging(st: &mut S3ControlStore, job_id: &str) -> Response {
    if !st.jobs.contains_key(job_id) {
        return job_not_found(job_id);
    }
    st.job_tags.remove(job_id);
    resp(200, "")
}

// ---------------------------------------------------------------------------
// Access Grants
// ---------------------------------------------------------------------------

fn access_grants_instance_not_found() -> Response {
    xml_error(
        404,
        "NoSuchAccessGrantsInstance",
        "The Access Grants instance does not exist",
        "",
    )
}

fn access_grants_location_not_found(location_id: &str) -> Response {
    xml_error(
        404,
        "NoSuchAccessGrantsLocation",
        &format!("The specified access grants location does not exist: {location_id}"),
        "",
    )
}

fn access_grant_not_found(grant_id: &str) -> Response {
    xml_error(
        404,
        "NoSuchAccessGrant",
        &format!("The specified access grant does not exist: {grant_id}"),
        "",
    )
}

fn create_access_grants_instance(
    req: &Request,
    st: &mut S3ControlStore,
    body: Option<&El>,
) -> Response {
    let identity_center_arn = req_el(body, "CreateAccessGrantsInstanceRequest")
        .and_then(|b| b.text_of("IdentityCenterArn"))
        .filter(|s| !s.is_empty())
        .map(String::from);
    let instance = AccessGrantsInstance {
        instance_id: random_hex(16),
        instance_arn: format!(
            "arn:aws:s3:{}:{}:access-grants/default",
            req.region, req.account
        ),
        created_at: moto_ts(),
        identity_center_arn,
    };
    st.access_grants_instance = Some(instance);
    let stored = st.access_grants_instance.as_ref().expect("just inserted");
    let identity_center = stored
        .identity_center_arn
        .as_ref()
        .map(|arn| el("IdentityCenterArn", arn))
        .unwrap_or_default();
    resp(
        200,
        xmlns_el(
            "CreateAccessGrantsInstanceResult",
            [
                el("AccessGrantsInstanceId", &stored.instance_id),
                el("AccessGrantsInstanceArn", &stored.instance_arn),
                el("CreatedAt", &stored.created_at),
                identity_center,
            ]
            .concat(),
        ),
    )
}

fn get_access_grants_instance(st: &S3ControlStore) -> Response {
    let Some(instance) = &st.access_grants_instance else {
        return access_grants_instance_not_found();
    };
    let identity_center = instance
        .identity_center_arn
        .as_ref()
        .map(|arn| el("IdentityCenterArn", arn))
        .unwrap_or_default();
    resp(
        200,
        xmlns_el(
            "GetAccessGrantsInstanceResult",
            [
                el("AccessGrantsInstanceId", &instance.instance_id),
                el("AccessGrantsInstanceArn", &instance.instance_arn),
                el("CreatedAt", &instance.created_at),
                identity_center,
            ]
            .concat(),
        ),
    )
}

fn get_access_grants_instance_for_prefix(st: &S3ControlStore) -> Response {
    let Some(instance) = &st.access_grants_instance else {
        return access_grants_instance_not_found();
    };
    resp(
        200,
        xmlns_el(
            "GetAccessGrantsInstanceForPrefixResult",
            [
                el("AccessGrantsInstanceId", &instance.instance_id),
                el("AccessGrantsInstanceArn", &instance.instance_arn),
            ]
            .concat(),
        ),
    )
}

fn list_access_grants_instances(st: &S3ControlStore) -> Response {
    let mut inner = String::new();
    if let Some(instance) = &st.access_grants_instance {
        inner = raw_el(
            "AccessGrantsInstance",
            [
                el("AccessGrantsInstanceId", &instance.instance_id),
                el("AccessGrantsInstanceArn", &instance.instance_arn),
                el("CreatedAt", &instance.created_at),
            ]
            .concat(),
        );
    }
    resp(
        200,
        xmlns_el(
            "ListAccessGrantsInstancesResult",
            raw_el("AccessGrantsInstancesList", inner),
        ),
    )
}

fn delete_access_grants_instance(st: &mut S3ControlStore) -> Response {
    if st.access_grants_instance.is_none() {
        return access_grants_instance_not_found();
    }
    st.access_grants_instance = None;
    st.access_grants.clear();
    st.access_grants_locations.clear();
    st.resource_policy = None;
    resp(200, "")
}

fn put_access_grants_instance_resource_policy(
    _req: &Request,
    st: &mut S3ControlStore,
    body: Option<&El>,
) -> Response {
    if st.access_grants_instance.is_none() {
        return access_grants_instance_not_found();
    }
    let policy = req_el(body, "PutAccessGrantsInstanceResourcePolicyRequest")
        .and_then(|b| b.text_of("Policy"))
        .unwrap_or("")
        .to_string();
    st.resource_policy = Some(policy.clone());
    resp(
        200,
        xmlns_el(
            "PutAccessGrantsInstanceResourcePolicyResult",
            el("Policy", &policy),
        ),
    )
}

fn get_access_grants_instance_resource_policy(st: &S3ControlStore) -> Response {
    if st.access_grants_instance.is_none() {
        return access_grants_instance_not_found();
    }
    let policy = st.resource_policy.clone().unwrap_or_default();
    resp(
        200,
        xmlns_el(
            "GetAccessGrantsInstanceResourcePolicyResult",
            el("Policy", &policy),
        ),
    )
}

fn delete_access_grants_instance_resource_policy(st: &mut S3ControlStore) -> Response {
    if st.access_grants_instance.is_none() {
        return access_grants_instance_not_found();
    }
    st.resource_policy = None;
    resp(200, "")
}

fn create_access_grants_location(
    req: &Request,
    st: &mut S3ControlStore,
    body: Option<&El>,
) -> Response {
    let Some(body) = req_el(body, "CreateAccessGrantsLocationRequest") else {
        return not_implemented();
    };
    let location = AccessGrantsLocation {
        location_id: format!("default-{}", random_hex(16)),
        location_scope: body.text_or("LocationScope"),
        iam_role_arn: body.text_or("IAMRoleArn"),
        location_arn: String::new(),
        created_at: moto_ts(),
    };
    let mut location = location;
    location.location_arn = format!(
        "arn:aws:s3:{}:{}:access-grants/default/location/{}",
        req.region, req.account, location.location_id
    );
    let location_id = location.location_id.clone();
    let fields = location_fields(&location);
    st.access_grants_locations
        .insert(location_id.clone(), location);
    resp(200, xmlns_el("CreateAccessGrantsLocationResult", fields))
}

fn location_fields(location: &AccessGrantsLocation) -> String {
    [
        el("AccessGrantsLocationId", &location.location_id),
        el("AccessGrantsLocationArn", &location.location_arn),
        el("LocationScope", &location.location_scope),
        el("IAMRoleArn", &location.iam_role_arn),
        el("CreatedAt", &location.created_at),
    ]
    .concat()
}

fn get_access_grants_location(st: &S3ControlStore, location_id: &str) -> Response {
    let Some(location) = st.access_grants_locations.get(location_id) else {
        return access_grants_location_not_found(location_id);
    };
    resp(
        200,
        xmlns_el("GetAccessGrantsLocationResult", location_fields(location)),
    )
}

fn list_access_grants_locations(st: &S3ControlStore) -> Response {
    let inner = st
        .access_grants_locations
        .values()
        .map(|location| raw_el("AccessGrantsLocation", location_fields(location)))
        .collect::<String>();
    resp(
        200,
        xmlns_el(
            "ListAccessGrantsLocationsResult",
            raw_el("AccessGrantsLocationsList", inner),
        ),
    )
}

fn delete_access_grants_location(st: &mut S3ControlStore, location_id: &str) -> Response {
    if st.access_grants_locations.remove(location_id).is_none() {
        return access_grants_location_not_found(location_id);
    }
    resp(200, "")
}

fn update_access_grants_location(
    _req: &Request,
    st: &mut S3ControlStore,
    location_id: &str,
    body: Option<&El>,
) -> Response {
    let Some(location) = st.access_grants_locations.get_mut(location_id) else {
        return access_grants_location_not_found(location_id);
    };
    let role = req_el(body, "UpdateAccessGrantsLocationRequest")
        .and_then(|b| b.text_of("IAMRoleArn"))
        .unwrap_or_default();
    location.iam_role_arn = role.to_string();
    resp(
        200,
        xmlns_el(
            "UpdateAccessGrantsLocationResult",
            location_fields(location),
        ),
    )
}

fn create_access_grant(req: &Request, st: &mut S3ControlStore, body: Option<&El>) -> Response {
    let Some(body) = req_el(body, "CreateAccessGrantRequest") else {
        return not_implemented();
    };
    let grantee = body.child("Grantee");
    let grantee_type = grantee
        .and_then(|g| g.text_of("GranteeType"))
        .unwrap_or_default()
        .to_string();
    let grantee_identifier = grantee
        .and_then(|g| g.text_of("GranteeIdentifier"))
        .unwrap_or_default()
        .to_string();
    let grant_id = random_hex(16);
    let grant = AccessGrant {
        grant_id: grant_id.clone(),
        grant_arn: format!(
            "arn:aws:s3:{}:{}:access-grants/default/grant/{grant_id}",
            req.region, req.account
        ),
        location_id: body.text_or("AccessGrantsLocationId"),
        permission: body.text_of("Permission").unwrap_or_default().to_string(),
        location_scope: body
            .sub(&["AccessGrantsLocationConfiguration", "S3SubPrefix"])
            .map(El::as_str)
            .unwrap_or_default()
            .to_string(),
        grantee_type,
        grantee_identifier,
        created_at: moto_ts(),
    };
    st.access_grants.insert(grant_id.clone(), grant);
    let stored = &st.access_grants[&grant_id];
    resp(
        200,
        xmlns_el(
            "CreateAccessGrantResult",
            [
                el("AccessGrantId", &stored.grant_id),
                el("AccessGrantArn", &stored.grant_arn),
                el("AccessGrantsLocationId", &stored.location_id),
                el("Permission", &stored.permission),
                el("CreatedAt", &stored.created_at),
                raw_el(
                    "Grantee",
                    [
                        el("GranteeType", &stored.grantee_type),
                        el("GranteeIdentifier", &stored.grantee_identifier),
                    ]
                    .concat(),
                ),
            ]
            .concat(),
        ),
    )
}

fn get_access_grant(st: &S3ControlStore, grant_id: &str) -> Response {
    let Some(grant) = st.access_grants.get(grant_id) else {
        return access_grant_not_found(grant_id);
    };
    resp(
        200,
        xmlns_el(
            "GetAccessGrantResult",
            [
                el("AccessGrantId", &grant.grant_id),
                el("AccessGrantArn", &grant.grant_arn),
                el("AccessGrantsLocationId", &grant.location_id),
                el("Permission", &grant.permission),
                el("CreatedAt", &grant.created_at),
                raw_el(
                    "Grantee",
                    [
                        el("GranteeType", &grant.grantee_type),
                        el("GranteeIdentifier", &grant.grantee_identifier),
                    ]
                    .concat(),
                ),
                raw_el(
                    "AccessGrantsLocationConfiguration",
                    el("S3SubPrefix", &grant.location_scope),
                ),
            ]
            .concat(),
        ),
    )
}

fn list_access_grants(st: &S3ControlStore) -> Response {
    let inner = st
        .access_grants
        .values()
        .map(|grant| {
            raw_el(
                "AccessGrant",
                [
                    el("AccessGrantId", &grant.grant_id),
                    el("AccessGrantArn", &grant.grant_arn),
                    el("AccessGrantsLocationId", &grant.location_id),
                    el("Permission", &grant.permission),
                    el("CreatedAt", &grant.created_at),
                ]
                .concat(),
            )
        })
        .collect::<String>();
    resp(
        200,
        xmlns_el("ListAccessGrantsResult", raw_el("AccessGrantsList", inner)),
    )
}

fn delete_access_grant(st: &mut S3ControlStore, grant_id: &str) -> Response {
    if st.access_grants.remove(grant_id).is_none() {
        return access_grant_not_found(grant_id);
    }
    resp(200, "")
}

// ---------------------------------------------------------------------------
// Resource tags (moto's TaggingService semantics on the s3control side)
// ---------------------------------------------------------------------------

/// moto returns each ARN's tag list verbatim; unknown resources list empty.
fn list_tags_for_resource(st: &S3ControlStore, arn: &str) -> Response {
    let mut inner = String::new();
    if let Some(row) = st.resource_tags.get(arn) {
        for (key, value) in row {
            inner += &raw_el("Tag", [el("Key", key), el("Value", value)].concat());
        }
    }
    resp(
        200,
        xmlns_el("ListTagsForResourceResult", raw_el("Tags", inner)),
    )
}

fn tag_resource(st: &mut S3ControlStore, arn: &str, body: Option<&El>) -> Response {
    let tags = body
        .and_then(|b| req_el(Some(b), "TagResourceRequest"))
        .and_then(|b| b.child("Tags"))
        .map(|t| t.tag_pairs())
        .unwrap_or_default();
    let row = st.resource_tags.entry(arn.to_string()).or_default();
    row.clear();
    row.extend(tags);
    empty_resp(204)
}

fn untag_resource(st: &mut S3ControlStore, arn: &str, keys: &[String]) -> Response {
    if let Some(row) = st.resource_tags.get_mut(arn) {
        keys.iter().for_each(|key| row.retain(|(k, _)| k != key));
    }
    empty_resp(204)
}

// ---------------------------------------------------------------------------
// Object Lambda access points
// ---------------------------------------------------------------------------

fn create_access_point_for_object_lambda(
    req: &Request,
    st: &mut S3ControlStore,
    name: &str,
    body: Option<&El>,
) -> Response {
    let supporting = body
        .and_then(|b| b.sub(&["Configuration", "SupportingAccessPoint"]))
        .or_else(|| body.and_then(|b| b.sub(&["SupportingAccessPoint"])))
        .map(El::as_str)
        .unwrap_or_default();
    let supporting_bucket = supporting
        .rsplit('/')
        .next()
        .unwrap_or_default()
        .to_string();
    let ap = AccessPoint {
        name: name.to_string(),
        alias: access_point_alias(name),
        bucket: supporting_bucket,
        created: moto_ts_nozone(),
        // moto credits the access point ARN here (not s3-object-lambda).
        arn: format!("arn:aws:s3:us-east-1:{}:accesspoint/{name}", req.account),
        network_origin: "Internet".to_string(),
        vpc_id: None,
        pubc: None,
        policy: None,
    };
    st.object_lambda_access_points.insert(name.to_string(), ap);
    let stored = &st.object_lambda_access_points[name];
    resp(
        200,
        xmlns_el(
            "CreateAccessPointForObjectLambdaResult",
            [
                el("ObjectLambdaAccessPointArn", &stored.arn),
                raw_el(
                    "Alias",
                    [el("Status", "READY"), el("Value", &stored.alias)].concat(),
                ),
            ]
            .concat(),
        ),
    )
}

fn get_access_point_for_object_lambda(st: &S3ControlStore, name: &str) -> Response {
    let Some(ap) = st.object_lambda_access_points.get(name) else {
        return access_point_not_found(name);
    };
    resp(
        200,
        xmlns_el(
            "GetAccessPointForObjectLambdaResult",
            [
                el("Name", &ap.name),
                el("ObjectLambdaAccessPointArn", &ap.arn),
                el("CreationDate", &ap.created),
                raw_el(
                    "Alias",
                    [el("Status", "READY"), el("Value", &ap.alias)].concat(),
                ),
            ]
            .concat(),
        ),
    )
}

fn put_olap_policy(st: &mut S3ControlStore, name: &str, body: Option<&El>) -> Response {
    if !st.object_lambda_access_points.contains_key(name) {
        return access_point_not_found(name);
    }
    let policy = body
        .and_then(|b| b.text_of("Policy"))
        .unwrap_or_default()
        .to_string();
    st.object_lambda_access_point_policies
        .insert(name.to_string(), policy);
    resp(200, "")
}

fn get_olap_policy(st: &S3ControlStore, name: &str) -> Response {
    if !st.object_lambda_access_points.contains_key(name) {
        return access_point_not_found(name);
    }
    let policy = st
        .object_lambda_access_point_policies
        .get(name)
        .cloned()
        .unwrap_or_default();
    resp(
        200,
        raw_el("GetAccessPointPolicyResult", el("Policy", &policy)),
    )
}

fn get_olap_policy_status() -> Response {
    resp(
        200,
        raw_el(
            "GetAccessPointPolicyResult",
            raw_el("PolicyStatus", el("IsPublic", "true")),
        ),
    )
}

// ---------------------------------------------------------------------------
// Delegated bucket-level operations (mirror moto's S3 backend delegation)
// ---------------------------------------------------------------------------

/// NoSuchBucket in moto's flat S3 error envelope, message matching moto.
fn no_such_bucket(bucket: &str) -> Response {
    let mut inner = [
        el("Code", "NoSuchBucket"),
        "<Message><![CDATA[The specified bucket does not exist]]></Message>".to_string(),
        el("BucketName", bucket),
        el("RequestId", uuid::Uuid::new_v4().to_string()),
    ]
    .concat();
    inner = raw_el("Error", inner);
    resp(
        404,
        format!("<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n{inner}"),
    )
}

fn bucket_op(req: &Request, st: &mut S3ControlStore, bucket: &str, leaf_pair: &str) -> Response {
    let leaf = leaf_pair.to_string();
    let method = req.method.as_str();
    match (method, leaf.as_str()) {
        ("PUT", "versioning") => {
            // moto: `xmltodict.parse(body)["PutBucketVersioningRequest"]`.
            // botocore sends the `VersioningConfiguration` root, so moto's
            // index KeyErrors and its werkzeug server answers the plain
            // HTML 500 — crash-faithful for every real client shape. Only
            // a body carrying moto's root key passes through and sets
            // versioning (`VersioningConfiguration.Status`, default "").
            match parse_xml(&req.body) {
                Some(el) if el.name == "PutBucketVersioningRequest" => {
                    let status = el
                        .sub(&["VersioningConfiguration", "Status"])
                        .map(|s| s.as_str().to_string())
                        .unwrap_or_default();
                    put_bucket_versioning(req, st, bucket, &status)
                }
                _ => crate::proto::moto_crash_response(),
            }
        }
        ("GET", "versioning") => get_bucket_versioning(req, st, bucket),
        ("PUT", "policy") => put_bucket_policy(req, st, bucket),
        ("GET", "policy") => get_bucket_policy(req, st, bucket),
        ("DELETE", "policy") => {
            let Some(bucket_obj) = s3::bridge::get_bucket(req.account, &req.region, bucket) else {
                return no_such_bucket(bucket);
            };
            *bucket_obj.policy.write() = None;
            empty_resp(204)
        }
        ("PUT", "tagging") => put_bucket_tagging(req, st, bucket),
        ("GET", "tagging") => get_bucket_tagging(req, st, bucket),
        ("DELETE", "tagging") => {
            let Some(bucket_obj) = s3::bridge::get_bucket(req.account, &req.region, bucket) else {
                return no_such_bucket(bucket);
            };
            *bucket_obj.tags_xml.write() = None;
            empty_resp(204)
        }
        ("PUT", "lifecycleconfiguration") => put_bucket_lifecycle(req, st, bucket),
        ("GET", "lifecycleconfiguration") => get_bucket_lifecycle(req, st, bucket),
        ("DELETE", "lifecycleconfiguration") => {
            let Some(bucket_obj) = s3::bridge::get_bucket(req.account, &req.region, bucket) else {
                return no_such_bucket(bucket);
            };
            *bucket_obj.lifecycle_xml.write() = None;
            empty_resp(204)
        }
        ("PUT", "replication") => put_bucket_replication(req, st, bucket),
        ("GET", "replication") => get_bucket_replication(req, st, bucket),
        ("DELETE", "replication") => {
            let Some(bucket_obj) = s3::bridge::get_bucket(req.account, &req.region, bucket) else {
                return no_such_bucket(bucket);
            };
            *bucket_obj.replication_xml.write() = None;
            empty_resp(204)
        }
        _ => not_implemented(),
    }
}

fn put_bucket_policy(req: &Request, _st: &mut S3ControlStore, bucket: &str) -> Response {
    // moto indexes `["PutBucketPolicyRequest"]["Policy"]` strictly: an empty
    // body crashes (500); a wrapper without <Policy> yields an empty policy.
    let Some(body) = parse_xml(&req.body) else {
        return xml_error(500, "InternalError", "'PutBucketPolicyRequest'", "");
    };
    let policy = req_el(Some(&body), "PutBucketPolicyRequest")
        .and_then(|b| b.text_of("Policy"))
        .unwrap_or_default()
        .to_string();
    let Some(bucket_obj) = s3::bridge::get_bucket(req.account, &req.region, bucket) else {
        return no_such_bucket(bucket);
    };
    *bucket_obj.policy.write() = Some(policy);
    resp(200, "")
}

fn get_bucket_policy(req: &Request, _st: &mut S3ControlStore, bucket: &str) -> Response {
    // moto catch-all: a missing bucket or unset policy yields an empty body.
    let policy = s3::bridge::get_bucket(req.account, &req.region, bucket)
        .and_then(|b| b.policy.read().clone())
        .unwrap_or_default();
    resp(
        200,
        xmlns_el("GetBucketPolicyResult", el("Policy", &policy)),
    )
}

fn put_bucket_tagging(req: &Request, _st: &mut S3ControlStore, bucket: &str) -> Response {
    // moto's `get("Tagging", {})` fallback: a wrapper without TagSet writes
    // zero tags and still answers 200.
    let parsed = parse_xml(&req.body);
    if parsed.is_none() {
        return xml_error(500, "InternalError", "'PutBucketTaggingRequest'", "");
    }
    let tags = parsed
        .as_ref()
        .and_then(|b| b.sub(&["PutBucketTaggingRequest", "Tagging", "TagSet"]))
        .or_else(|| parsed.as_ref().and_then(|b| b.sub(&["TagSet"])))
        .map(|set| set.tag_pairs())
        .unwrap_or_default();
    let Some(bucket_obj) = s3::bridge::get_bucket(req.account, &req.region, bucket) else {
        return no_such_bucket(bucket);
    };
    let mut tag_xml = String::new();
    for (key, value) in tags {
        tag_xml += &raw_el("Tag", [el("Key", &key), el("Value", &value)].concat());
    }
    let xml = format!(
        "<Tagging xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\"><TagSet>{tag_xml}</TagSet></Tagging>"
    );
    *bucket_obj.tags_xml.write() = Some(xml);
    resp(200, "")
}

fn get_bucket_tagging(req: &Request, _st: &mut S3ControlStore, bucket: &str) -> Response {
    // moto catches every exception and answers an empty TagSet.
    let xml = s3::bridge::get_bucket(req.account, &req.region, bucket)
        .and_then(|b| b.tags_xml.read().clone());
    let mut inner = String::new();
    if let Some(xml) = xml.and_then(|x| parse_xml(x.as_bytes())) {
        if let Some(set) = xml.child("TagSet") {
            for tag in &set.tag_pairs() {
                inner += &raw_el("Tag", [el("Key", &tag.0), el("Value", &tag.1)].concat());
            }
        }
    }
    resp(
        200,
        xmlns_el("GetBucketTaggingResult", raw_el("TagSet", inner)),
    )
}

fn put_bucket_versioning(
    req: &Request,
    _st: &mut S3ControlStore,
    bucket: &str,
    status: &str,
) -> Response {
    let Some(bucket_obj) = s3::bridge::get_bucket(req.account, &req.region, bucket) else {
        return no_such_bucket(bucket);
    };
    *bucket_obj.versioning.write() = match status {
        "Enabled" => Some(true),
        "Suspended" => Some(false),
        _ => None,
    };
    resp(200, "")
}

fn get_bucket_versioning(req: &Request, _st: &mut S3ControlStore, bucket: &str) -> Response {
    let Some(bucket_obj) = s3::bridge::get_bucket(req.account, &req.region, bucket) else {
        return no_such_bucket(bucket);
    };
    let status = match *bucket_obj.versioning.read() {
        Some(true) => "Enabled",
        Some(false) => "Suspended",
        None => "",
    };
    resp(
        200,
        xmlns_el("GetBucketVersioningResult", el("Status", status)),
    )
}

fn put_bucket_lifecycle(req: &Request, _st: &mut S3ControlStore, bucket: &str) -> Response {
    // moto parses via xmltodict: an empty body crashes (500, per probe). A
    // real client always carries <Rules>.
    let Some(put) = parse_xml(&req.body) else {
        return xml_error(500, "InternalError", "no element found", "");
    };
    let rules = req_el(Some(&put), "PutBucketLifecycleConfigurationRequest")
        .and_then(|b| b.sub(&["LifecycleConfiguration", "Rules"]))
        .or_else(|| put.sub(&["LifecycleConfiguration", "Rules"]))
        .map(|rules| rules.parts("Rule").count())
        .unwrap_or(0);
    let Some(bucket_obj) = s3::bridge::get_bucket(req.account, &req.region, bucket) else {
        return no_such_bucket(bucket);
    };
    let mut rule_xml = String::new();
    for _ in 0..rules {
        rule_xml += "<Rule/>";
    }
    let xml = format!("<LifecycleConfiguration><Rules>{rule_xml}</Rules></LifecycleConfiguration>");
    *bucket_obj.lifecycle_xml.write() = Some(xml);
    resp(200, "")
}

fn get_bucket_lifecycle(req: &Request, _st: &mut S3ControlStore, bucket: &str) -> Response {
    // moto's get_bucket raises NoSuchBucket with no catch here.
    let Some(bucket_obj) = s3::bridge::get_bucket(req.account, &req.region, bucket) else {
        return no_such_bucket(bucket);
    };
    let rules = bucket_obj
        .lifecycle_xml
        .read()
        .clone()
        .and_then(|xml| parse_xml(xml.as_bytes()))
        .map(|cfg| {
            cfg.child("Rules")
                .map(|rules| rules.parts("Rule").count())
                .unwrap_or(0)
        })
        .unwrap_or(0);
    let mut rule_xml = String::new();
    for _ in 0..rules {
        rule_xml += "<Rule/>";
    }
    resp(
        200,
        xmlns_el(
            "GetBucketLifecycleConfigurationResult",
            raw_el("Rules", rule_xml),
        ),
    )
}

fn put_bucket_replication(req: &Request, _st: &mut S3ControlStore, bucket: &str) -> Response {
    // moto indexes `["PutBucketReplicationRequest"]` strictly: an empty body
    // crashes (500); a real client always sends the configuration wrapper.
    let Some(body) = parse_xml(&req.body) else {
        return xml_error(500, "InternalError", "'PutBucketReplicationRequest'", "");
    };
    let mut replication = String::new();
    let rep = body.child("ReplicationConfiguration").cloned();
    if let Some(rep) = rep {
        for c in &rep.children {
            replication += &raw_el(&c.name, esc(c.as_str()));
        }
    }
    let Some(bucket_obj) = s3::bridge::get_bucket(req.account, &req.region, bucket) else {
        return no_such_bucket(bucket);
    };
    *bucket_obj.replication_xml.write() = Some(replication);
    resp(200, "")
}

fn get_bucket_replication(req: &Request, _st: &mut S3ControlStore, bucket: &str) -> Response {
    // moto without a catch: a missing bucket surfaces NoSuchBucket.
    let Some(bucket_obj) = s3::bridge::get_bucket(req.account, &req.region, bucket) else {
        return no_such_bucket(bucket);
    };
    let has = bucket_obj
        .replication_xml
        .read()
        .as_deref()
        .is_some_and(|s| !s.is_empty());
    if has {
        resp(
            200,
            xmlns_el(
                "GetBucketReplicationResult",
                raw_el("ReplicationConfiguration", ""),
            ),
        )
    } else {
        resp(200, xmlns_el("GetBucketReplicationResult", ""))
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;
    use std::collections::HashMap as StdHashMap;

    static ACCOUNTS: std::sync::atomic::AtomicU64 =
        std::sync::atomic::AtomicU64::new(200_000_000_001);

    /// Every test takes a fresh account so the shared store stays isolated.
    fn next_account() -> u64 {
        ACCOUNTS.fetch_add(1, std::sync::atomic::Ordering::SeqCst)
    }

    fn request(account: u64, method: &str, path: &str, body: &str) -> Response {
        request_query(account, method, path, "", body)
    }

    fn request_query(account: u64, method: &str, path: &str, query: &str, body: &str) -> Response {
        let req = Request {
            service: "s3control".to_string(),
            operation: String::new(),
            method: method.to_string(),
            path: path.to_string(),
            query_string: query.to_string(),
            headers: StdHashMap::from([("x-amz-account-id".to_string(), account.to_string())]),
            body: Bytes::from(body.to_string()),
            account,
            region: "us-east-1".to_string(),
        };
        dispatch(&req).expect("s3control dispatch")
    }

    fn status_code_str(resp: &Response) -> String {
        String::from_utf8_lossy(&resp.body).to_string()
    }

    fn xml_find<'a>(doc: &'a str, tag: &str) -> Option<&'a str> {
        let open = format!("<{tag}>");
        let close = format!("</{tag}>");
        let start = doc.find(&open)? + open.len();
        let end = doc[start..].find(&close)? + start;
        Some(&doc[start..end])
    }

    fn xml_has(doc: &str, tag: &str, value: &str) -> bool {
        xml_find(doc, tag).is_some_and(|v| v == value)
    }

    #[test]
    fn public_access_block_round_trip() {
        let account = next_account();
        let body = format!(
            "<PutPublicAccessBlockRequest xmlns=\"{XMLNS}\"><PublicAccessBlockConfiguration>\
             <BlockPublicAcls>true</BlockPublicAcls><IgnorePublicAcls>false</IgnorePublicAcls>\
             </PublicAccessBlockConfiguration></PutPublicAccessBlockRequest>"
        );
        let resp = request(
            account,
            "PUT",
            "/v20180820/configuration/publicAccessBlock",
            &body,
        );
        assert_eq!(resp.status, 201);
        let resp = request(
            account,
            "GET",
            "/v20180820/configuration/publicAccessBlock",
            "",
        );
        assert_eq!(resp.status, 200);
        let doc = status_code_str(&resp);
        assert!(xml_has(&doc, "BlockPublicAcls", "true"));
        assert!(xml_has(&doc, "IgnorePublicAcls", "false"));
        // moto's PublicAccessBlock defaults unset fields to false.
        assert!(xml_has(&doc, "BlockPublicPolicy", "false"));

        assert_eq!(
            request(
                account,
                "DELETE",
                "/v20180820/configuration/publicAccessBlock",
                ""
            )
            .status,
            204
        );
        let resp = request(
            account,
            "GET",
            "/v20180820/configuration/publicAccessBlock",
            "",
        );
        assert_eq!(resp.status, 404);
        assert!(status_code_str(&resp).contains("NoSuchPublicAccessBlockConfiguration"));
    }

    #[test]
    fn access_point_lifecycle() {
        let account = next_account();
        let body = "<CreateAccessPointRequest xmlns=\"http://awss3control.amazonaws.com/doc/2018-08-20/\">\
<Bucket>apbucket9</Bucket></CreateAccessPointRequest>";
        let resp = request(account, "PUT", "/v20180820/accesspoint/ap-flow", body);
        assert_eq!(resp.status, 200, "{}", status_code_str(&resp));
        let doc = status_code_str(&resp);
        assert!(xml_find(&doc, "AccessPointArn").is_some());
        assert!(xml_find(&doc, "Alias").is_some());

        let resp = request(account, "GET", "/v20180820/accesspoint/ap-flow", "");
        assert_eq!(resp.status, 200, "{}", status_code_str(&resp));
        let doc = status_code_str(&resp);
        assert!(xml_has(&doc, "Name", "ap-flow"));
        assert!(xml_has(&doc, "Bucket", "apbucket9"));
        assert!(xml_has(&doc, "NetworkOrigin", "Internet"));
        assert!(xml_find(&doc, "CreationDate").is_some());

        assert_eq!(
            request(account, "DELETE", "/v20180820/accesspoint/ap-flow", "").status,
            204
        );
        let resp = request(account, "GET", "/v20180820/accesspoint/ap-flow", "");
        assert_eq!(resp.status, 404);
        assert!(status_code_str(&resp).contains("NoSuchAccessPoint"));
    }

    #[test]
    fn detect_error_and_alias_helpers() {
        assert_eq!(random_hex(2).len(), 2);
        assert!(random_hex(24).chars().all(|c| c.is_ascii_hexdigit()));
        assert!(access_point_alias("ap").starts_with("ap-"));
        assert!(mrap_alias("m").ends_with(".mrap"));
        let doc = "<a><b>1</b></a>";
        assert_eq!(xml_find(doc, "b"), Some("1"));
        assert!(!xml_has(doc, "c", "1"));
    }

    #[test]
    fn mraps_render_and_delete() {
        let account = next_account();
        let body = format!(
            "<CreateMultiRegionAccessPointRequest xmlns=\"{XMLNS}\"><Details><Name>mr9</Name>\
             <Regions><Region><Bucket>bkt9</Bucket></Region></Regions></Details></CreateMultiRegionAccessPointRequest>"
        );
        let resp = request(
            account,
            "POST",
            "/v20180820/async-requests/mrap/create",
            &body,
        );
        assert_eq!(resp.status, 200, "{}", status_code_str(&resp));
        assert!(status_code_str(&resp).contains("RequestTokenARN"));

        let resp = request(account, "GET", "/v20180820/mrap/instances/mr9", "");
        assert_eq!(resp.status, 200);
        assert!(xml_has(&status_code_str(&resp), "Status", "READY"));
        assert!(status_code_str(&resp).contains("<Region>us-east-1</Region>"));

        let resp = request(account, "GET", "/v20180820/mrap/instances/mr9/routes", "");
        assert!(status_code_str(&resp).contains("TrafficDialPercentage"));

        let body = format!(
            "<DeleteMultiRegionAccessPointRequest xmlns=\"{XMLNS}\"><Details><Name>mr9</Name></Details></DeleteMultiRegionAccessPointRequest>"
        );
        assert_eq!(
            request(
                account,
                "POST",
                "/v20180820/async-requests/mrap/delete",
                &body
            )
            .status,
            200
        );
        let resp = request(account, "GET", "/v20180820/mrap/instances/mr9", "");
        assert_eq!(resp.status, 404);
        assert!(status_code_str(&resp).contains("NoSuchMultiRegionAccessPoint"));
    }

    #[test]
    fn jobs_flow() {
        let account = next_account();
        let body = format!(
            "<CreateJobRequest xmlns=\"{XMLNS}\"><Operation/>\
             <Manifest/>\
             <Priority>42</Priority>\
             <RoleArn>arn:aws:iam::1:role/r</RoleArn>\
             <ConfirmationRequired>false</ConfirmationRequired>\
             </CreateJobRequest>"
        );
        let resp = request(account, "POST", "/v20180820/jobs", &body);
        assert_eq!(resp.status, 200, "{}", status_code_str(&resp));
        let job_id = xml_find(&status_code_str(&resp), "JobId")
            .expect("JobId")
            .to_string();

        let resp = request(account, "GET", &format!("/v20180820/jobs/{job_id}"), "");
        assert_eq!(resp.status, 200);
        assert!(xml_has(&status_code_str(&resp), "Status", "Ready"));
        assert!(xml_has(&status_code_str(&resp), "Priority", "42"));
        assert!(status_code_str(&resp).contains("ProgressSummary"));

        let resp = request_query(
            account,
            "POST",
            &format!("/v20180820/jobs/{job_id}/status"),
            "requestedJobStatus=Cancelled",
            "",
        );
        assert!(xml_has(&status_code_str(&resp), "Status", "Cancelled"));

        let resp = request(account, "GET", "/v20180820/jobs/none-uuid", "");
        assert_eq!(resp.status, 404);
        assert!(status_code_str(&resp).contains("NoSuchJob"));
    }

    #[test]
    fn access_grants_flows() {
        let account = next_account();
        let resp = request(account, "POST", "/v20180820/accessgrantsinstance", "");
        assert_eq!(resp.status, 200, "{}", status_code_str(&resp));
        assert!(status_code_str(&resp).contains("AccessGrantsInstanceArn"));
        assert!(status_code_str(&resp).contains("access-grants/default"));

        let resp = request(account, "GET", "/v20180820/accessgrantsinstance", "");
        assert!(status_code_str(&resp).contains("AccessGrantsInstanceId"));

        let body = format!(
            "<CreateAccessGrantsLocationRequest xmlns=\"{XMLNS}\"><LocationScope>s3://b/</LocationScope>\
             <IAMRoleArn>arn:aws:iam::1:role/role</IAMRoleArn></CreateAccessGrantsLocationRequest>"
        );
        let resp = request(
            account,
            "POST",
            "/v20180820/accessgrantsinstance/location",
            &body,
        );
        assert_eq!(resp.status, 200, "{}", status_code_str(&resp));
        let loc_id = xml_find(&status_code_str(&resp), "AccessGrantsLocationId")
            .expect("location id")
            .to_string();
        assert!(status_code_str(&resp).contains("LocationScope"));

        let body = format!(
            "<CreateAccessGrantRequest xmlns=\"{XMLNS}\"><AccessGrantsLocationId>{loc_id}</AccessGrantsLocationId>\
             <AccessGrantsLocationConfiguration><S3SubPrefix>sub/</S3SubPrefix></AccessGrantsLocationConfiguration>\
             <Grantee><GranteeType>IAM</GranteeType><GranteeIdentifier>id9</GranteeIdentifier></Grantee>\
             <Permission>READ</Permission></CreateAccessGrantRequest>"
        );
        let resp = request(
            account,
            "POST",
            "/v20180820/accessgrantsinstance/grant",
            &body,
        );
        assert_eq!(resp.status, 200, "{}", status_code_str(&resp));
        let grant_id = xml_find(&status_code_str(&resp), "AccessGrantId")
            .expect("grant id")
            .to_string();
        assert!(xml_has(&status_code_str(&resp), "Permission", "READ"));

        let resp = request(
            account,
            "GET",
            &format!("/v20180820/accessgrantsinstance/grant/{grant_id}"),
            "",
        );
        assert!(status_code_str(&resp).contains("S3SubPrefix"));

        assert_eq!(
            request(
                account,
                "DELETE",
                &format!("/v20180820/accessgrantsinstance/grant/{grant_id}"),
                ""
            )
            .status,
            200
        );
        let resp = request(
            account,
            "GET",
            &format!("/v20180820/accessgrantsinstance/grant/{grant_id}"),
            "",
        );
        assert_eq!(resp.status, 404);
        assert!(status_code_str(&resp).contains("NoSuchAccessGrant"));

        assert_eq!(
            request(
                account,
                "DELETE",
                &format!("/v20180820/accessgrantsinstance/location/{loc_id}"),
                ""
            )
            .status,
            200
        );
        let resp = request(
            account,
            "GET",
            &format!("/v20180820/accessgrantsinstance/location/{loc_id}"),
            "",
        );
        assert!(status_code_str(&resp).contains("NoSuchAccessGrantsLocation"));

        assert_eq!(
            request(account, "DELETE", "/v20180820/accessgrantsinstance", "").status,
            200
        );
        let resp = request(account, "GET", "/v20180820/accessgrantsinstance", "");
        assert_eq!(resp.status, 404);
    }

    #[test]
    fn storage_lens_and_not_implemented_surface() {
        let account = next_account();
        let body = format!(
            "<PutStorageLensConfigurationRequest xmlns=\"{XMLNS}\"><StorageLensConfiguration>\
             <Id>sl9</Id><IsEnabled>true</IsEnabled></StorageLensConfiguration>\
             <Tags><Tag><Key>k1</Key><Value>v1</Value></Tag></Tags></PutStorageLensConfigurationRequest>"
        );
        assert_eq!(
            request(account, "PUT", "/v20180820/storagelens/sl9", &body).status,
            200
        );
        let resp = request(account, "GET", "/v20180820/storagelens/sl9", "");
        assert_eq!(resp.status, 200, "{}", status_code_str(&resp));
        assert!(xml_has(&status_code_str(&resp), "IsEnabled", "true"));

        let resp = request(account, "GET", "/v20180820/storagelens", "");
        assert!(status_code_str(&resp).contains("StorageLensArn"));

        let resp = request(account, "GET", "/v20180820/storagelens/sl9/tagging", "");
        // moto's AccessPointNotFound for a configuration whose tags unset
        assert!(resp.status == 200, "{}", status_code_str(&resp));
        assert!(status_code_str(&resp).contains("Tags"));

        let resp = request(account, "DELETE", "/v20180820/storagelens/sl9", "");
        assert_eq!(resp.status, 204);
        let resp = request(account, "DELETE", "/v20180820/storagelens/sl9", "");
        assert_eq!(resp.status, 404);
        assert!(status_code_str(&resp).contains("NoSuchConfiguration"));

        // the unimplemented surface answers 501 like moto.
        for (method, path, body_text) in [
            ("POST", "/v20180820/accessgrantsinstance/identitycenter", ""),
            (
                "DELETE",
                "/v20180820/accessgrantsinstance/identitycenter",
                "",
            ),
            ("GET", "/v20180820/accessgrantsinstance/dataaccess", ""),
            ("GET", "/v20180820/accessgrantsinstance/caller/grants", ""),
            ("GET", "/v20180820/accesspointfordirectory", ""),
            ("GET", "/v20180820/accesspointforobjectlambda", ""),
            (
                "PUT",
                "/v20180820/accesspointforobjectlambda/olap9/configuration",
                "",
            ),
        ] {
            let resp = request(account, method, path, body_text);
            assert_eq!(resp.status, 501, "{method} {path}");
            assert!(status_code_str(&resp).contains("NotImplemented"), "{path}");
        }
    }

    #[test]
    fn queries_path_shapes() {
        let account = next_account();
        assert_eq!(request(account, "GET", "/v20180820/jobs", "").status, 200);
        let resp = request(account, "GET", "/v20180820/accesspoint", "");
        assert_eq!(resp.status, 200, "{}", status_code_str(&resp));
        assert!(status_code_str(&resp).contains("AccessPointList"));
    }

    /// moto's `xmltodict.parse(body)["PutBucketVersioningRequest"]` raises a
    /// bare KeyError on an empty body and escapes to the werkzeug server:
    /// the wire answer is the plain HTML 500 page, no AWS envelope.
    #[test]
    fn put_bucket_versioning_empty_body_is_moto_crash() {
        let account = next_account();
        let resp = request(
            account,
            "PUT",
            "/v20180820/bucket/probe-bucket/versioning",
            "",
        );
        assert_eq!(resp.status, 500);
        assert!(
            resp.headers
                .iter()
                .any(|(k, v)| k == "Content-Type" && v == "text/html; charset=utf-8"),
            "{:?}",
            resp.headers
        );
        let text = status_code_str(&resp);
        assert!(text.starts_with("<!doctype html>"), "{text}");
        assert!(
            text.contains("<title>500 Internal Server Error</title>"),
            "{text}"
        );
    }

    /// botocore sends the `VersioningConfiguration` root, so moto's
    /// `["PutBucketVersioningRequest"]` index KeyErrors: every real-client
    /// PUT versioning shape crashes the same way (an HTML 500, not 200,
    /// even with a valid wrapper).
    #[test]
    fn put_bucket_versioning_real_shape_is_moto_crash() {
        let account = next_account();
        let resp = request(
            account,
            "PUT",
            "/v20180820/bucket/probe-ver-bucket/versioning",
            "<VersioningConfiguration><Status>Enabled</Status></VersioningConfiguration>",
        );
        assert_eq!(resp.status, 500);
        let text = status_code_str(&resp);
        assert!(text.starts_with("<!doctype html>"), "{text}");
    }

    /// moto only sets versioning when the body carries its expected
    /// `PutBucketVersioningRequest` root (measured against moto itself).
    #[test]
    fn put_bucket_versioning_sets_status_via_moto_root() {
        let account = next_account();
        let state = s3::models::S3State::new();
        state.put_bucket(std::sync::Arc::new(s3::models::Bucket::new(
            "probe-ver-bucket".to_string(),
            "us-east-1".to_string(),
        )));
        s3::bridge::register_state(account, "us-east-1", &state);
        let bucket = "/v20180820/bucket/probe-ver-bucket/versioning";
        assert_eq!(
            request(
                account,
                "PUT",
                bucket,
                "<PutBucketVersioningRequest><VersioningConfiguration><Status>Enabled</Status></VersioningConfiguration></PutBucketVersioningRequest>",
            ).status,
            200
        );
        let resp = request(account, "GET", bucket, "");
        assert!(status_code_str(&resp).contains("<Status>Enabled</Status>"));
    }

    /// Percent-encode an ARN the way an SDK URI label would (`:` and `/`).
    fn url_encode_arn(s: &str) -> String {
        s.replace(':', "%3A").replace('/', "%2F")
    }

    #[test]
    fn s3control_access_point_tags() {
        let account = next_account();

        // create_access_point accepts Tags and registers them on the
        // resource-tags route under the access point ARN.
        let create_body = format!(
            "<CreateAccessPointRequest xmlns=\"{XMLNS}\"><Bucket>bucket-1</Bucket>\
             <Tags><Tag><Key>env</Key><Value>test</Value></Tag>\
             <Tag><Key>team</Key><Value>core</Value></Tag></Tags></CreateAccessPointRequest>"
        );
        let resp = request(
            account,
            "PUT",
            "/v20180820/accesspoint/tagged-ap",
            &create_body,
        );
        assert_eq!(resp.status, 200);
        let body = status_code_str(&resp);
        assert!(body.contains("<AccessPointArn>"));
        let arn = xml_find(&body, "AccessPointArn").unwrap().to_string();
        assert!(arn.ends_with(":accesspoint/tagged-ap"), "{arn}");

        let tags = status_code_str(&request(
            account,
            "GET",
            &format!("/v20180820/tags/{}", url_encode_arn(&arn)),
            "",
        ));
        assert!(tags.contains("<Key>env</Key><Value>test</Value>"), "{tags}");
        assert!(
            tags.contains("<Key>team</Key><Value>core</Value>"),
            "{tags}"
        );

        // Re-creating with an overlapping Tags list upserts per key
        // (TaggingService.tag_resource): env's value is replaced, team is
        // kept, the new key is appended.
        let recreate_body = format!(
            "<CreateAccessPointRequest xmlns=\"{XMLNS}\"><Bucket>bucket-1</Bucket>\
             <Tags><Tag><Key>env</Key><Value>prod</Value></Tag>\
             <Tag><Key>extra</Key><Value>yes</Value></Tag></Tags></CreateAccessPointRequest>"
        );
        let resp = request(
            account,
            "PUT",
            "/v20180820/accesspoint/tagged-ap",
            &recreate_body,
        );
        assert_eq!(resp.status, 200);
        let tags = status_code_str(&request(
            account,
            "GET",
            &format!("/v20180820/tags/{}", url_encode_arn(&arn)),
            "",
        ));
        assert!(tags.contains("<Key>env</Key><Value>prod</Value>"), "{tags}");
        assert!(
            tags.contains("<Key>team</Key><Value>core</Value>"),
            "{tags}"
        );
        assert!(
            tags.contains("<Key>extra</Key><Value>yes</Value>"),
            "{tags}"
        );

        // An access point created without Tags carries no tag row.
        let plain = {
            let body = status_code_str(&request(
                account,
                "PUT",
                "/v20180820/accesspoint/plain-ap",
                "<CreateAccessPointRequest xmlns=\"{XMLNS}\"><Bucket>bucket-1</Bucket></CreateAccessPointRequest>",
            ));
            xml_find(&body, "AccessPointArn").unwrap().to_string()
        };
        let plain_arn = format!("arn:aws:s3:us-east-1:{account}:accesspoint/plain-ap");
        assert_eq!(plain, plain_arn);
        let tags = status_code_str(&request(
            account,
            "GET",
            &format!("/v20180820/tags/{}", url_encode_arn(&plain_arn)),
            "",
        ));
        assert!(tags.contains("<ListTagsForResourceResult"), "{tags}");
        assert!(!tags.contains("<Value>"), "{tags}");
    }

    /// moto 5.2.4 paginates list_access_points on botocore tokens and
    /// rejects garbage tokens with the modeled InvalidNextTokenException.
    #[test]
    fn s3control_access_points_next_token() {
        let account = next_account();
        for name in ["pn-a", "pn-b", "pn-c"] {
            let resp = request(
                account,
                "PUT",
                &format!("/v20180820/accesspoint/{name}"),
                "<CreateAccessPointRequest xmlns=\"{XMLNS}\"><Bucket>pn-bucket</Bucket></CreateAccessPointRequest>",
            );
            assert_eq!(resp.status, 200);
        }

        // Page 1: two access points, token pointing at the first excluded.
        let page1 = status_code_str(&request_query(
            account,
            "GET",
            "/v20180820/accesspoint",
            "bucket=pn-bucket&maxResults=2",
            "",
        ));
        assert!(page1.contains("<Name>pn-a</Name>"), "{page1}");
        assert!(page1.contains("<Name>pn-b</Name>"), "{page1}");
        assert!(!page1.contains("<Name>pn-c</Name>"), "{page1}");
        let token = xml_find(&page1, "NextToken").unwrap().to_string();
        assert_eq!(decode_s3control_token(&token), Some("pn-c".to_string()));

        // Page 2 starts AT the token's access point; no NextToken element
        // once paging is done.
        let token_query = format!(
            "bucket=pn-bucket&maxResults=2&nextToken={}",
            url_encode_query_value(&token)
        );
        let page2 = status_code_str(&request_query(
            account,
            "GET",
            "/v20180820/accesspoint",
            &token_query,
            "",
        ));
        assert!(page2.contains("<Name>pn-c</Name>"), "{page2}");
        assert!(!page2.contains("NextToken"), "{page2}");

        // Garbage tokens answer the modeled XML error envelope.
        let resp = request_query(
            account,
            "GET",
            "/v20180820/accesspoint",
            "nextToken=garbage",
            "",
        );
        assert_eq!(resp.status, 400);
        let body = status_code_str(&resp);
        assert!(body.contains("<ErrorResponse>"), "{body}");
        assert!(
            body.contains("<Code>InvalidNextTokenException</Code>"),
            "{body}"
        );
        assert!(body.contains("The nextToken provided is invalid"), "{body}");

        // A decodable token naming no access point raises moto's own
        // InvalidToken (JSON body).
        let unknown = encode_s3control_token("pn-nope");
        let resp = request_query(
            account,
            "GET",
            "/v20180820/accesspoint",
            &format!("nextToken={unknown}"),
            "",
        );
        assert_eq!(resp.status, 400);
        assert!(
            status_code_str(&resp).contains("\"__type\":\"InvalidToken\""),
            "{}",
            status_code_str(&resp)
        );
    }

    /// Percent-encode a query value (base64 tokens carry `+`, `/`, `=`).
    fn url_encode_query_value(s: &str) -> String {
        let mut out = String::new();
        for b in s.bytes() {
            match b {
                b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_' | b'.' | b'~' => {
                    out.push(b as char)
                }
                other => out.push_str(&format!("%{other:02X}")),
            }
        }
        out
    }
}
