//! EC2 service — the query-protocol (XML) backend in pure Rust.
//!
//! Request parsing (flattened EC2 lists `Filter.1.Name=...`), XML rendering
//! and the operation switch live here; resource structs live in [`state`];
//! per-family behavior lives in [`handlers`].

pub mod azones;
pub mod handlers;
pub mod ec2b;
pub mod handlers_compute;
pub mod handlers_misc;
pub mod handlers_net2;
pub mod templates;
pub mod handlers_net;
pub mod state;
pub mod tests;

use crate::store::ScopeKey;
use crate::{Request, Response};
use state::Ec2State;

pub use crate::proto::xml_escape;

pub const EC2_NS: &str = "http://ec2.amazonaws.com/doc/2016-11-15/";
pub const OLD_NS: &str = "http://ec2.amazonaws.com/doc/2013-10-15/";

/// Run `f` with a uniquely-held write lease on the (account, region) state.
/// The guard is acquired exactly once per call — no nested re-acquire.
pub fn with_state<T>(req: &Request, f: impl FnOnce(&mut Ec2State) -> T) -> T {
    let key: ScopeKey = (req.account, req.region.clone());
    let slot = state::shared();
    let mut map = slot.write();
    let st = map.entry(key).or_insert_with(|| {
        let mut s = Ec2State::default();
        s.ensure_default_vpc(&req.region, req.account);
        s
    });
    f(st)
}

/// Flattened EC2 query parameters (as botocore puts them on the wire),
/// URL-decoded once at parse time.
pub struct Params {
    map: Vec<(String, String)>,
}

impl Params {
    pub fn parse(req: &Request) -> Params {
        let mut pairs: Vec<(String, String)> = Vec::new();
        // Body first (form), then query string — query wins only for
        // duplicate keys later, matching moto's form-then-query reads.
        for (k, v) in parse_query(&req.query_string) {
            pairs.push((k, v));
        }
        let body = String::from_utf8_lossy(&req.body);
        for (k, v) in parse_query(&body) {
            pairs.push((k, v));
        }
        Params { map: pairs }
    }

    /// First value of a param (top level, e.g. `VpcId`).
    pub fn get(&self, key: &str) -> Option<&str> {
        self.map
            .iter()
            .rev()
            .find(|(k, _)| k == key)
            .map(|(_, v)| v.as_str())
    }

    pub fn str(&self, key: &str) -> String {
        self.get(key).unwrap_or_default().to_string()
    }

    /// Whether the param appeared with any value (including empty).
    pub fn has(&self, key: &str) -> bool {
        self.map.iter().any(|(k, _)| k == key)
    }

    pub fn bool(&self, key: &str) -> Option<bool> {
        self.get(key)
            .map(|v| matches!(v, "true" | "True" | "1"))
    }

    pub fn i64(&self, key: &str) -> Option<i64> {
        self.get(key).and_then(|v| v.parse::<i64>().ok())
    }

    /// Count of a flat list family, e.g. `instances of `AccountId.1..`.
    pub fn count(&self, base: &str) -> usize {
        let mut n = 0;
        while self.map.iter().any(|(k, _)| *k == format!("{base}.{}", n + 1)) {
            n += 1;
        }
        n
    }

    /// List of flat values `Base.1..N` (non-list-of-struct).
    pub fn list(&self, base: &str) -> Vec<&str> {
        let n = self.count(base);
        (1..=n)
            .map(|i| {
                self.map
                    .iter()
                    .rev()
                    .find(|(k, _)| *k == format!("{base}.{i}"))
                    .map(|(_, v)| v.as_str())
                    .unwrap_or_default()
            })
            .collect()
    }

    /// Indices `i` where a `prefix.{i}.`-prefixed key exists (container lists
    /// like `IpPermissions.1.IpProtocol` -> [0], plus any index having sub-fields).
    pub fn container_indices(&self, prefix: &str) -> Vec<usize> {
        let start = format!("{prefix}.");
        let mut max = 0usize;
        for (k, _) in &self.map {
            if let Some(rest) = k.strip_prefix(&start) {
                if !rest.is_empty() {
                    if let Some((idx, _)) = rest.split_once('.') {
                        if let Ok(v) = idx.parse::<usize>() {
                            max = max.max(v);
                        }
                    }
                }
            }
        }
        (1..=max)
            .filter(|i| self.map.iter().any(|(k, _)| k.starts_with(&format!("{start}{i}."))))
            .map(|i| i - 1)
            .collect()
    }

    /// Sub-field of a container member: `prefix.{i+1}.{field}`.
    pub fn sub(&self, prefix: &str, i: usize, field: &str) -> Option<&str> {
        self.get(&format!("{prefix}.{}.{field}", i + 1))
    }

    pub fn sub_list(&self, prefix: &str, i: usize, field: &str) -> Vec<String> {
        self.list_detached(&format!("{prefix}.{}.{field}", i + 1))
    }

    /// Flat list under a fully-spelled path: `IpPermissions.1.IpRanges.2` etc.
    pub fn list_detached(&self, base: &str) -> Vec<String> {
        let n = self.count(base);
        (1..=n)
            .map(|i| {
                self.get(&format!("{base}.{i}"))
                    .unwrap_or_default()
                    .to_string()
            })
            .collect()
    }

    /// Whether any key starts with this prefix (e.g. presence of `Filter.`).
    pub fn has_prefix(&self, prefix: &str) -> bool {
        self.map.iter().any(|(k, _)| k.starts_with(prefix))
    }
}

/// Minimal form/query parser: `a=b&c=%2Fd` → [(a,b), (c,/d)].
fn parse_query(input: &str) -> Vec<(String, String)> {
    if input.trim().is_empty() {
        return Vec::new();
    }
    let mut out = Vec::new();
    for pair in input.split('&') {
        if pair.is_empty() {
            continue;
        }
        let (k, v) = match pair.split_once('=') {
            Some((k, v)) => (k, v),
            None => (pair, ""),
        };
        out.push((url_decode(k), url_decode(v)));
    }
    out
}

/// Percent-decoding with `+` as space, matching form-encoding semantics.
pub fn url_decode(s: &str) -> String {
    let bytes = s.as_bytes();
    let mut out: Vec<u8> = Vec::with_capacity(bytes.len());
    let mut i = 0;
    while i < bytes.len() {
        match bytes[i] {
            b'%' if i + 2 < bytes.len() => {
                let hi = (bytes[i + 1] as char).to_digit(16);
                let lo = (bytes[i + 2] as char).to_digit(16);
                match (hi, lo) {
                    (Some(h), Some(l)) => {
                        out.push((h << 4 | l) as u8);
                        i += 3;
                    }
                    _ => {
                        out.push(b'%');
                        i += 1;
                    }
                }
            }
            b'+' => {
                out.push(b' ');
                i += 1;
            }
            c => {
                out.push(c);
                i += 1;
            }
        }
    }
    String::from_utf8_lossy(&out).into_owned()
}

/// Render an XML document with the standard EC2 namespace.
pub fn ec2_xml(root: &str, body: &str) -> Response {
    let xml = format!(
        r#"<?xml version="1.0" encoding="UTF-8"?>
<{root} xmlns="{ns}">{body}<ResponseMetadata><RequestId>{rid}</RequestId></ResponseMetadata></{root}>"#,
        root = root,
        ns = EC2_NS,
        rid = uuid::Uuid::new_v4()
    );
    Response::xml(200, xml.into_bytes())
}

/// Render an XML document with no namespace (moto's shapes for many ops).
pub fn xmlns_xml(root: &str, ns: Option<&str>, body: &str) -> Response {
    let ns_attr = ns.map(|n| format!(r#" xmlns="{n}""#)).unwrap_or_default();
    let xml = format!(
        r#"<?xml version="1.0" encoding="UTF-8"?>
<{root}{ns_attr}>{body}<ResponseMetadata><RequestId>{rid}</RequestId></ResponseMetadata></{root}>"#,
        root = root,
        ns_attr = ns_attr,
        rid = uuid::Uuid::new_v4()
    );
    Response::xml(200, xml.into_bytes())
}

/// EC2-styled error response (moto's layout, with the EC2 xmlns).
pub fn ec2_error(status: u16, code: &str, message: &str) -> Response {
    let xml = format!(
        r#"<?xml version="1.0" encoding="UTF-8"?>
<Response xmlns="{ns}"><Errors><Error><Code>{code}</Code><Message>{msg}</Message></Error></Errors><RequestID>{rid}</RequestID></Response>"#,
        ns = EC2_NS,
        code = xml_escape(code),
        msg = xml_escape(message),
        rid = uuid::Uuid::new_v4()
    );
    Response::xml(status, xml.into_bytes())
}

/// moto's 501 for unported ops.
pub fn not_implemented(op: &str) -> Response {
    ec2_error(
        501,
        "NotImplemented",
        &format!("The {op} action has not been implemented"),
    )
}

/// Unknown action (moto errors this way when the URL does not match).
pub fn unknown_action(msg: &str) -> Response {
    ec2_error(400, "UnknownAction", msg)
}

// ---------------------------------------------------------------------------
// Shared rendering helpers
// ---------------------------------------------------------------------------

/// `<tagSet>...</tagSet>` for a `Tags` value; empty when no tags.
pub fn tags_xml(tags: &state::Tags) -> String {
    if tags.is_empty() {
        return "<tagSet/>".to_string();
    }
    let mut s = String::from("<tagSet>");
    for (k, v) in tags {
        s.push_str("<item><key>");
        s.push_str(&xml_escape(k));
        s.push_str("</key><value>");
        s.push_str(&xml_escape(v));
        s.push_str("</value></item>");
    }
    s.push_str("</tagSet>");
    s
}

/// Timestamp as moto emits: `2026-10-18T15:41:32.000Z`.
pub fn now_iso() -> String {
    use chrono::Utc;
    Utc::now().format("%Y-%m-%dT%H:%M:%S%.3fZ").to_string()
}

// ---------------------------------------------------------------------------
// Dispatch
// ---------------------------------------------------------------------------

/// EC2 dispatch entry. Returns any response the op family produces;
/// unknown ops get moto's NotImplemented (501) — never a panic.
pub fn dispatch(req: &Request) -> Response {
    if req.operation.is_empty() {
        return ec2_error(400, "MissingParameter", "The request must contain the parameter Action");
    }
    let params = Params::parse(req);
    let resp = handlers::route(req, &params);
    resp
}
