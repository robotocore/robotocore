//! CloudFormation backend for rotocore.
//!
//! Ports the behavior of the Python provider at
//! `src/robotocore/services/cloudformation/` (stacks, change sets, events,
//! exports) plus the moto-shaped stubs the compatibility suite pins.
//!
//! Resource deployment mirrors to the robotocore server's own native APIs
//! over loopback HTTP (the same service handlers real AWS SDKs hit), the
//! way the Python moto sidecar resolved resources; see `mirror.rs`.

pub mod engine;
pub mod mirror;
pub mod ops;
pub mod store;
pub mod xml;

use crate::{Request, Response};
use serde_json::Value;
use std::collections::HashMap;

/// Dispatch one CloudFormation request. Always returns `Some` — once this
/// module is wired the moto sidecar no longer serves cloudformation.
pub fn dispatch(req: &Request) -> Response {
    ops::handle(req)
}

/// Decode a form-encoded (or query-string) CloudFormation request into flat
/// params, mirroring the Python provider's `parse_qs` input.
pub fn form_params(req: &Request) -> HashMap<String, String> {
    let content_type = req.headers.get("content-type").cloned().unwrap_or_default();
    if content_type.contains("x-www-form-urlencoded") {
        parse_qs(std::str::from_utf8(&req.body).unwrap_or(""))
    } else if !req.query_string.is_empty() {
        parse_qs(&req.query_string)
    } else {
        parse_qs(std::str::from_utf8(&req.body).unwrap_or(""))
    }
}

/// Parse `k=v&k2=v2` percent-decoded, first value wins (parse_qs single mode).
pub fn parse_qs(s: &str) -> HashMap<String, String> {
    let mut out = HashMap::new();
    for pair in s.split('&') {
        if pair.is_empty() {
            continue;
        }
        let (k, v) = match pair.split_once('=') {
            Some((k, v)) => (form_decode(k), form_decode(v)),
            None => (form_decode(pair), String::new()),
        };
        out.entry(k).or_insert(v);
    }
    out
}

/// x-www-form-urlencoded percent decoding: `+` is a space.
fn form_decode(s: &str) -> String {
    let bytes = s.as_bytes();
    let mut out: Vec<u8> = Vec::with_capacity(bytes.len());
    let mut i = 0;
    while i < bytes.len() {
        match bytes[i] {
            b'+' => {
                out.push(b' ');
                i += 1;
            }
            b'%' if i + 2 < bytes.len() => {
                let hex = &s[i + 1..i + 3];
                if let Ok(v) = u8::from_str_radix(hex, 16) {
                    out.push(v);
                    i += 3;
                } else {
                    out.push(b'%');
                    i += 1;
                }
            }
            b => {
                out.push(b);
                i += 1;
            }
        }
    }
    String::from_utf8_lossy(&out).into_owned()
}

/// Account id string (the store keys mirror the Python provider's).
pub fn account_str(req: &Request) -> String {
    req.account.to_string()
}

/// Helper used by ops to read one param.
pub fn param<'a>(params: &'a HashMap<String, String>, key: &str) -> &'a str {
    params.get(key).map(String::as_str).unwrap_or("")
}

/// Iterate indexed members: `Parameters.member.1.ParameterKey` patterns.
pub fn indexed_members(params: &HashMap<String, String>, base: &str, leaf: &str) -> Vec<String> {
    let mut out = Vec::new();
    let mut i = 1;
    while let Some(v) = params.get(&format!("{base}.member.{i}.{leaf}")) {
        out.push(v.clone());
        i += 1;
    }
    out
}

/// Extract a name/value pair list like `Tags.member.N.Key/.Value`.
pub fn key_values(params: &HashMap<String, String>, base: &str) -> Vec<(String, String)> {
    let mut out = Vec::new();
    let mut i = 1;
    while let Some(k) = params.get(&format!("{base}.member.{i}.Key")) {
        let v = params
            .get(&format!("{base}.member.{i}.Value"))
            .cloned()
            .unwrap_or_default();
        out.push((k.clone(), v));
        i += 1;
    }
    out
}

pub fn xml_response_response(action: &str, result: Value) -> Response {
    xml::ok(action, &xml::dict_to_xml(&result))
}

pub fn xml_error(code: &str, message: &str, status: u16) -> Response {
    xml::error(code, message, status)
}
