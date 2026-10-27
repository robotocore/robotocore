#!/usr/bin/env python3
"""Generate a new service crate skeleton from botocore specs."""
import argparse, json, os, sys
from glob import glob
from pathlib import Path

def find_spec(service):
    venv_dirs = [
        "/Users/jackdanger/www/robotocore/.venv/lib/python3.12/site-packages",
    ]
    for venv in venv_dirs:
        pattern = os.path.join(venv, "botocore/data", service, "*", "service-2.json.gz")
        paths = glob(pattern)
        if paths:
            import gzip
            with gzip.open(paths[0]) as f:
                return json.load(f)
    raise FileNotFoundError(f"Spec not found: {service}")

def get_operations(spec, ops_filter=None):
    ops = sorted(spec.get("operations", {}).keys())
    if ops_filter:
        ops = [o for o in ops if o in ops_filter]
    return ops

def generate_crate(service, ops, out_dir):
    spec = find_spec(service)
    meta = spec.get("metadata", {})
    protocol = meta.get("protocol", "query")
    target = meta.get("targetPrefix", "")
    api_version = meta.get("apiVersion", "2020-01-01")
    crate_dir = Path(out_dir) / "crates" / service
    src_dir = crate_dir / "src"
    src_dir.mkdir(parents=True, exist_ok=True)
    svc_camel = "".join(w.capitalize() for w in service.replace("-", "_").split("_"))
    svc_title = service.replace("-", " ").title()

    # Cargo.toml
    (crate_dir / "Cargo.toml").write_text(
        '[package]\nname = "%s"\nversion = "0.1.0"\nedition = "2021"\nlicense = "MIT"\n\n'
        '[dependencies]\nserde = { version = "1.0", features = ["derive"] }\n'
        'serde_json = "1.0"\nbytes = "1.5"\nthiserror = "1.0"\n'
        'uuid = { version = "1.0", features = ["v4", "serde"] }\n'
        'chrono = { version = "0.4", features = ["serde"] }\n'
        'parking_lot = "0.12"\ntracing = "0.1"\nhttp = "1.0"\nbase64 = "0.22"\n' % service
    )

    # lib.rs
    (src_dir / "lib.rs").write_text(
        '//! Native %s service for robotocore.\n'
        '//! Protocol: %s, Target: %s\n\n'
        'pub mod handler;\npub mod models;\npub mod protocol;\n\n'
        'pub use protocol::{AwsRequest, AwsResponse};\n\n'
        'pub struct Default%sHandler {\n'
        '    pub(crate) inner: handler::%sHandler,\n}\n\n'
        'impl Default%sHandler {\n'
        '    pub fn new() -> Self {\n'
        '        Self { inner: handler::%sHandler::new() }\n}\n'
        '    pub fn handle(&self, req: AwsRequest) -> AwsResponse {\n'
        '        self.inner.handle(req)\n}\n}\n\n'
        'impl Default for Default%sHandler {\n'
        '    fn default() -> Self { Self::new() }\n}\n'
        % (svc_title, protocol, target or "N/A",
           svc_camel, svc_camel, svc_camel, svc_camel, svc_camel)
    )

    # protocol.rs
    if protocol in ("json", "rest-json"):
        ct = "application/json" if protocol == "rest-json" else "application/x-amz-json-1.0"
        proto = (
            '//! %s request/response types (%s protocol).\n'
            'use bytes::Bytes;\nuse serde_json::Value;\n\n'
            '#[derive(Debug, Clone)]\n'
            'pub struct AwsRequest {\n'
            '    pub service: String,\n    pub operation: String,\n'
            '    pub account: u64,\n    pub region: String,\n'
            '    pub params: Value,\n    pub body: Bytes,\n}\n\n'
            '#[derive(Debug, Clone)]\n'
            'pub struct AwsResponse {\n'
            '    pub status: u16,\n    pub headers: Vec<(String, String)>,\n'
            '    pub body: String,\n}\n\n'
            'impl AwsResponse {\n'
            '    pub fn json(status: u16, body: Value) -> Self {\n'
            '        Self { status,\n'
            '            headers: vec![\n'
            '                ("Content-Type".to_string(), "%s".to_string()),\n'
            '                ("x-amzn-RequestId".to_string(), uuid::Uuid::new_v4().to_string()),\n'
            '                ("server".to_string(), "robotocore".to_string()),\n'
            '            ],\n'
            '            body: serde_json::to_string(&body).unwrap_or_else(|_| "{}".to_string()),\n}\n}\n\n'
            '    pub fn error(status: u16, code: &str, message: &str) -> Self {\n'
            '        let body = serde_json::json!({ " __type": code, "message": message });\n'
            '        Self { status,\n'
            '            headers: vec![\n'
            '                ("Content-Type".to_string(), "%s".to_string()),\n'
            '                ("x-amzn-RequestId".to_string(), uuid::Uuid::new_v4().to_string()),\n'
            '                ("server".to_string(), "robotocore".to_string()),\n'
            '            ],\n'
            '            body: serde_json::to_string(&body).unwrap_or_else(|_| "{}".to_string()),\n}\n}\n}\n'
            % (service, protocol, ct, ct)
        )
    else:
        proto = (
            '//! %s request/response types (query protocol, XML).\n'
            'use bytes::Bytes;\nuse serde_json::Value;\n\n'
            '#[derive(Debug, Clone)]\n'
            'pub struct AwsRequest {\n'
            '    pub service: String,\n    pub operation: String,\n'
            '    pub account: u64,\n    pub region: String,\n'
            '    pub params: Value,\n    pub body: Bytes,\n    pub query: String,\n}\n\n'
            '#[derive(Debug, Clone)]\n'
            'pub struct AwsResponse {\n'
            '    pub status: u16,\n    pub headers: Vec<(String, String)>,\n'
            '    pub body: String,\n}\n\n'
            'impl AwsResponse {\n'
            '    pub fn xml(status: u16, root: &str, body_xml: String) -> Self {\n'
            '        let full_body = format!(\n'
            '            "<{root}Response xmlns=\\\\\"https://api.aws.amazon.com/doc/%s/\\\\\">{body_xml}</{root}Response>"\n'
            '        );\n'
            '        Self { status,\n'
            '            headers: vec![\n'
            '                ("Content-Type".to_string(), "text/xml".to_string()),\n'
            '                ("x-amzn-RequestId".to_string(), uuid::Uuid::new_v4().to_string()),\n'
            '                ("server".to_string(), "robotocore".to_string()),\n'
            '            ],\n'
            '            body: full_body,\n}\n}\n\n'
            '    pub fn error(status: u16, code: &str, message: &str) -> Self {\n'
            '        let body = format!(\n'
            '            "<ErrorResponse><Error><Code>{}</Code><Message>{}</Message></Error></ErrorResponse>",\n'
            '            code, message\n'
            '        );\n'
            '        Self { status,\n'
            '            headers: vec![\n'
            '                ("Content-Type".to_string(), "text/xml".to_string()),\n'
            '                ("x-amzn-RequestId".to_string(), uuid::Uuid::new_v4().to_string()),\n'
            '                ("server".to_string(), "robotocore".to_string()),\n'
            '            ],\n'
            '            body,\n}\n}\n}\n\n'
            'pub fn get_param(req: &AwsRequest, key: &str) -> Option<String> {\n'
            '    if let Some(v) = req.params.get(key).and_then(|v| v.as_str()) {\n'
            '        return Some(v.to_string());\n}\n'
            '    if !req.query.is_empty() {\n'
            "        for pair in req.query.split('&') {\n"
            '            if let Some((k, v)) = pair.split_once("=") {\n'
            '                if k == key { return Some(v.to_string()); }\n}\n}\n'
            '    }\n'
            '    None\n}\n'
            % (service, api_version)
        )
    (src_dir / "protocol.rs").write_text(proto)

    # models.rs
    (src_dir / "models.rs").write_text(
        '//! %s in-memory state models.\n'
        'use parking_lot::RwLock;\n'
        'use std::collections::HashMap;\nuse std::sync::Arc;\n\n'
        '#[derive(Clone)]\n'
        'pub struct %sState {\n'
        '    pub resources: Arc<RwLock<HashMap<String, serde_json::Value>>>,\n}\n\n'
        'impl %sState {\n'
        '    pub fn new() -> Self {\n'
        '        Self { resources: Arc::new(RwLock::new(HashMap::new())) }\n}\n}\n\n'
        'impl Default for %sState {\n'
        '    fn default() -> Self { Self::new() }\n}\n'
        % (svc_title, svc_camel, svc_camel, svc_camel)
    )

    # handler.rs
    h = [
        '//! %s operation handler.' % svc_title, '',
        'use parking_lot::RwLock;',
        'use serde_json::{json, Value};',
        'use std::collections::HashMap;',
        'use std::sync::Arc;', '',
        'use crate::models::%sState;' % svc_camel,
        'use crate::protocol::{AwsRequest, AwsResponse};', '',
        'pub struct %sHandler {' % svc_camel,
        '    state: RwLock<HashMap<(u64, String), %sState>>,' % svc_camel, '}', '',
        'impl %sHandler {' % svc_camel,
        '    pub fn new() -> Self {',
        '        Self { state: RwLock::new(HashMap::new()) }', '    }', '',
        '    fn get_state(&self, account: u64, region: &str) -> %sState {' % svc_camel,
        '        let mut states = self.state.write();',
        '        states.entry((account, region.to_string())).or_insert_with(%sState::new).clone()' % svc_camel,
        '    }', '',
        '    pub fn handle(&self, req: AwsRequest) -> AwsResponse {',
        '        let op = req.operation.as_str();',
        '        match op {',
    ]
    for op in ops:
        h.append('            "%s" => self.%s(&req),' % (op, op.lower()))
    h.extend([
        '            other => AwsResponse::error(400, "ValidationException",',
        '                &format!("The operation {} is not implemented", other)),',
        '        }', '    }',
    ])
    for op in ops:
        h.extend([
            '',
            '    fn %s(&self, _req: &AwsRequest) -> AwsResponse {' % op.lower(),
            '        AwsResponse::error(501, "NotImplemented", "TODO")', '    }',
        ])
    h.extend([
        '}', '',
        'impl Default for %sHandler {' % svc_camel,
        '    fn default() -> Self { Self::new() }', '}',
    ])
    (src_dir / "handler.rs").write_text("\n".join(h) + "\n")

    print("Generated: crates/%s/ (%s, %d ops)" % (service, protocol, len(ops)))

def main():
    parser = argparse.ArgumentParser(description="Generate a service crate skeleton")
    parser.add_argument("--service", required=True)
    parser.add_argument("--ops", help="Comma-separated operations")
    parser.add_argument("--out", default="/Users/jackdanger/www/robotocore-rust")
    args = parser.parse_args()
    spec = find_spec(args.service)
    ops_filter = args.ops.split(",") if args.ops else None
    ops = get_operations(spec, ops_filter)
    if not ops:
        print("No operations for %s" % args.service, file=sys.stderr)
        sys.exit(1)
    generate_crate(args.service, ops, args.out)

if __name__ == "__main__":
    main()
