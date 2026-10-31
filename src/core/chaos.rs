//! Chaos engineering fault-injection admin API.
//!
//! Port of the python-era twin's `robotocore/chaos/` package + its
//! `gateway/app.py` route handlers, recovered from git history:
//! - `fault_rules.py` → `FaultRule` / `FaultRuleStore` below
//! - `middleware.py` → [`intercept`] (called from `catch_all_handler`
//!   before service dispatch; the python twin hooked it into the handler
//!   chain right after context population)
//! - `app.py chaos_*` handlers → the axum CRUD endpoints below
//!
//! Wire spec (`tests/compatibility/chaos/test_chaos_engineering_compat.py`
//! at `391b54bc^`, implementation at `6448ae45^`):
//!
//! | Route | Behavior |
//! |---|---|
//! | `GET /_robotocore/chaos/rules` | `{"rules": [...]}`, 200 |
//! | `POST /_robotocore/chaos/rules` | 201 `{"status": "created", "rule_id": ...}`; 400 for empty body / invalid JSON / non-object |
//! | `DELETE /_robotocore/chaos/rules/{rule_id}` | 200 `{"status": "deleted", ...}` or 404 `{"error": "Rule not found"}` |
//! | `POST /_robotocore/chaos/rules/clear` | 200 `{"status": "cleared", "count": N}` |
//!
//! Injected faults: `latency_ms` sleeps then dispatch proceeds (capped at
//! `MAX_RULE_LATENCY_MS`, 10s — the sleep is inline); an
//! `error_code` short-circuits with protocol-aware bodies — XML
//! `ErrorResponse` for rest-xml/query/ec2 services, `{"__type": ...}` JSON
//! otherwise — plus the `x-robotocore-chaos: {rule_id}` header.
//!
//! Matching: service glob (fnmatch `*`/`?`/`[..]`), operation regex
//! (`re.search` semantics), exact region, exact account (extension: the
//! body's `account` member, absent from the python rule), probability
//! draw, TTL expiry, `enabled` flag. The store is a process-wide
//! in-memory singleton (not per-account), exactly like the python
//! singleton. First matching rule wins; `match_count` increments on every
//! effective match. An `account`-scoped rule only applies to requests
//! from that account — the compat chaos suite uses this to throttle only
//! itself inside the 12-worker shared-twin lane.

use std::sync::Mutex;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use axum::{
    body::Bytes,
    extract::{Path, Query},
    http::{Method, StatusCode},
    response::{IntoResponse, Response},
    Json,
};
use once_cell::sync::Lazy;
use regex::Regex;
use serde_json::{json, Value};
use uuid::Uuid;

use crate::core::service_registry::SERVICE_REGISTRY;

// ---------------------------------------------------------------------------
// FaultRule — python `FaultRule` (chaos/fault_rules.py)
// ---------------------------------------------------------------------------

#[derive(Clone)]
pub struct FaultRule {
    rule_id: String,
    /// `None`/empty match all services; glob syntax otherwise.
    service: Option<String>,
    /// `None`/empty match all; regex source checked with `search` semantics.
    operation: Option<String>,
    /// Exact region filter; `None`/empty matches all.
    region: Option<String>,
    /// Exact account filter; `None`/empty matches all (`account:<12-digit>`).
    /// A chaos suite in its own account can thus fault-inject without
    /// throttling the parallel lane's other workers that share the twin.
    account: Option<String>,
    error_code: Option<String>,
    error_message: String,
    status_code: i64,
    latency_ms: i64,
    /// Draw per request: fault applies unless the draw exceeds this.
    probability: f64,
    enabled: bool,
    ttl_seconds: Option<f64>,
    created_at: f64,
    match_count: i64,
    op_pattern: Option<Regex>,
}

/// Upper bound on a rule's `latency_ms`. The sleep runs inline before
/// dispatch (python `time.sleep` semantics port), so a caller-supplied
/// wall-clock-like value (e.g. milliseconds pasted as seconds) would pin
/// the request task for hours. 10s keeps chaos injections dramatic but
/// bounded; the compat chaos suite's largest latency (500ms) is unaffected.
pub const MAX_RULE_LATENCY_MS: i64 = 10_000;

impl FaultRule {
    fn now_epoch() -> f64 {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_secs_f64())
            .unwrap_or(0.0)
    }

    /// Build a rule from the admin API's JSON body, mirroring python
    /// `FaultRule.from_dict` defaults:
    /// - `status_code`: explicit value (even 0) wins; otherwise 429 for
    ///   `ThrottlingException`, 500 for anything else
    /// - `error_message`: falls back to `Injected by chaos rule {rule_id}`
    ///   when absent or empty
    /// - `latency_ms` 0, `probability` 1.0 clamped to [0, 1], `enabled` true
    /// - `rule_id`: client-supplied (non-empty) value wins, else `uuid4`
    /// - `created_at`/`match_count` honored when supplied
    ///
    /// `Err` = invalid regex in `operation` (python raised `ValueError`,
    /// which escaped the route handler and answered a plain 500).
    fn from_json(data: &Value) -> Result<Self, ()> {
        let rule_id = data
            .get("rule_id")
            .and_then(Value::as_str)
            .filter(|s| !s.is_empty())
            .map(str::to_string)
            .unwrap_or_else(|| Uuid::new_v4().simple().to_string()[..12].to_string());
        let service = data
            .get("service")
            .and_then(Value::as_str)
            .filter(|s| !s.is_empty())
            .map(str::to_string);
        let operation = data
            .get("operation")
            .and_then(Value::as_str)
            .filter(|s| !s.is_empty())
            .map(str::to_string);
        let op_pattern = match &operation {
            Some(src) => Some(Regex::new(src).map_err(|_| ())?),
            None => None,
        };
        let region = data
            .get("region")
            .and_then(Value::as_str)
            .filter(|s| !s.is_empty())
            .map(str::to_string);
        let account = data
            .get("account")
            .and_then(Value::as_str)
            .filter(|s| !s.is_empty())
            .map(str::to_string);
        let error_code = data
            .get("error_code")
            .and_then(Value::as_str)
            .filter(|s| !s.is_empty())
            .map(str::to_string);
        let status_code = data.get("status_code").and_then(Value::as_i64).unwrap_or(
            if error_code.as_deref() == Some("ThrottlingException") {
                429
            } else {
                500
            },
        );
        let error_message = data
            .get("error_message")
            .and_then(Value::as_str)
            .filter(|s| !s.is_empty())
            .map(str::to_string)
            .unwrap_or_else(|| format!("Injected by chaos rule {rule_id}"));
        // Defensive cap (see MAX_RULE_LATENCY_MS): clamped down, not
        // rejected, so a probe with an absurd latency still gets a rule —
        // python had no cap, values within [0, 10_000] are identical.
        let latency_ms = data
            .get("latency_ms")
            .and_then(Value::as_i64)
            .unwrap_or(0)
            .clamp(0, MAX_RULE_LATENCY_MS);
        // python: `max(0.0, min(1.0, probability))`
        let probability = data
            .get("probability")
            .and_then(Value::as_f64)
            .unwrap_or(1.0)
            .clamp(0.0, 1.0);
        let enabled = data.get("enabled").and_then(Value::as_bool).unwrap_or(true);
        let ttl_seconds = data.get("ttl_seconds").and_then(Value::as_f64);
        // python honored supplied `created_at`/`match_count` for reloads
        let created_at = data
            .get("created_at")
            .and_then(Value::as_f64)
            .unwrap_or_else(Self::now_epoch);
        let match_count = data.get("match_count").and_then(Value::as_i64).unwrap_or(0);

        Ok(Self {
            rule_id,
            service,
            operation,
            region,
            account,
            error_code,
            error_message,
            status_code,
            latency_ms,
            probability,
            enabled,
            ttl_seconds,
            created_at,
            match_count,
            op_pattern,
        })
    }

    fn is_expired(&self) -> bool {
        match self.ttl_seconds {
            None => false,
            Some(ttl) => Self::now_epoch() - self.created_at > ttl,
        }
    }

    /// fnmatch-style service test — `None`/empty matches everything.
    fn service_matches(&self, service: &str) -> bool {
        match self.service.as_deref() {
            None => true,
            Some(pattern) => glob_matches(pattern, service),
        }
    }

    /// Python `FaultRule.matches` — `match_count` increments only when the
    /// rule fully matches (probability draw included).
    fn matches(
        &mut self,
        service: &str,
        operation: Option<&str>,
        region: &str,
        account: u64,
    ) -> bool {
        if !self.enabled || self.is_expired() {
            return false;
        }
        if !self.service_matches(service) {
            return false;
        }
        if let Some(rule_region) = &self.region {
            if !rule_region.is_empty() && rule_region != region {
                return false;
            }
        }
        if let Some(rule_account) = &self.account {
            if rule_account != &account.to_string() {
                return false;
            }
        }
        if let Some(pattern) = &self.op_pattern {
            match operation {
                // python: a rule with an operation filter never matches a
                // request whose detected operation is absent.
                Some(op) if !op.is_empty() => {
                    if !pattern.is_match(op) {
                        return false;
                    }
                }
                _ => return false,
            }
        }
        if random_unit() > self.probability {
            return false;
        }
        self.match_count += 1;
        true
    }

    /// Wire shape of `FaultRule.to_dict` — names/types exactly as the
    /// python twin serialized them for the list endpoint.
    fn to_value(&self) -> Value {
        json!({
            "rule_id": self.rule_id,
            "service": self.service,
            "operation": self.operation,
            "region": self.region,
            "account": self.account,
            "error_code": self.error_code,
            "error_message": self.error_message,
            "status_code": self.status_code,
            "latency_ms": self.latency_ms,
            "probability": self.probability,
            "enabled": self.enabled,
            "ttl_seconds": self.ttl_seconds,
            "created_at": self.created_at,
            "match_count": self.match_count,
        })
    }
}

/// fnmatch-style full-string glob test (`*`, `?`, `[seq]`, `[!seq]`),
/// case-sensitive — POSIX `fnmatch.fnmatch` semantics. Unterminated `[`
/// degrades to a literal bracket, `[]...]` embeds a literal `]`.
fn glob_matches(pattern: &str, text: &str) -> bool {
    let p: Vec<char> = pattern.chars().collect();
    let n = p.len();
    let mut re = String::from("(?s)^");
    let mut i = 0;
    while i < n {
        match p[i] {
            '*' => {
                re.push_str(".*");
                i += 1;
            }
            '?' => {
                re.push('.');
                i += 1;
            }
            '[' => {
                // CPython fnmatch.translate scan: leading `!` negates, a
                // `]` right after `[` (or after `!`) stays literal, and an
                // unterminated bracket is a literal `[`.
                let mut j = i + 1;
                if j < n && p[j] == '!' {
                    j += 1;
                }
                if j < n && p[j] == ']' {
                    j += 1;
                }
                while j < n && p[j] != ']' {
                    j += 1;
                }
                if j >= n {
                    re.push_str("\\[");
                    i += 1;
                } else {
                    let stuff: String = p[i + 1..j].iter().collect();
                    re.push('[');
                    if stuff.is_empty() {
                        // python: "[]"/"[!]" widen to the NUL-exclusive class
                        re.push_str("^\\x00");
                    } else if let Some(rest) = stuff.strip_prefix('!') {
                        re.push('^');
                        re.push_str(&rest.replace('\\', "\\\\"));
                    } else if let Some(rest) = stuff.strip_prefix('^') {
                        // caret right after '[' is literal in fnmatch
                        re.push_str("\\^");
                        re.push_str(&rest.replace('\\', "\\\\"));
                    } else {
                        re.push_str(&stuff.replace('\\', "\\\\"));
                    }
                    re.push(']');
                    i = j + 1;
                }
            }
            c => {
                re.push_str(&regex::escape(&c.to_string()));
                i += 1;
            }
        }
    }
    re.push('$');
    Regex::new(&re).map(|r| r.is_match(text)).unwrap_or(false)
}

/// Uniform float in [0, 1) without adding a rand dependency: derive a u64
/// from a fresh v4 UUID.
fn random_unit() -> f64 {
    let mut n: u64 = 0;
    for b in Uuid::new_v4().as_bytes()[..8].iter() {
        n = (n << 8) | *b as u64;
    }
    n as f64 / u64::MAX as f64
}

// ---------------------------------------------------------------------------
// FaultRuleStore — python `FaultRuleStore` + module singleton
// ---------------------------------------------------------------------------

#[derive(Default)]
pub struct FaultRuleStore {
    rules: Vec<FaultRule>,
}

impl FaultRuleStore {
    pub fn add(&mut self, rule: FaultRule) -> String {
        let rule_id = rule.rule_id.clone();
        self.rules.push(rule);
        rule_id
    }

    pub fn remove(&mut self, rule_id: &str) -> bool {
        let before = self.rules.len();
        self.rules.retain(|r| r.rule_id != rule_id);
        self.rules.len() < before
    }

    pub fn clear(&mut self) -> usize {
        let count = self.rules.len();
        self.rules.clear();
        count
    }

    /// Clear rules that would apply to `account`: rules scoped to it plus
    /// unscoped (global) ones — "undo my account's exposure". Rules scoped
    /// to other accounts survive, so a session-end safety net never clobbers
    /// a concurrently running chaos session on the shared twin.
    pub fn clear_account(&mut self, account: &str) -> usize {
        let before = self.rules.len();
        self.rules
            .retain(|r| !(r.account.as_deref().is_none() || r.account.as_deref() == Some(account)));
        before - self.rules.len()
    }

    pub fn list_rules(&self) -> Vec<Value> {
        self.rules.iter().map(FaultRule::to_value).collect()
    }

    pub fn len(&self) -> usize {
        self.rules.len()
    }

    pub fn is_empty(&self) -> bool {
        self.rules.is_empty()
    }

    /// First matching rule wins, in insertion order.
    pub fn find_matching(
        &mut self,
        service: &str,
        operation: Option<&str>,
        region: &str,
        account: u64,
    ) -> Option<FaultRule> {
        for rule in self.rules.iter_mut() {
            if rule.matches(service, operation, region, account) {
                return Some(rule.clone());
            }
        }
        None
    }
}

/// Process-wide in-memory store — the python twin's module singleton.
/// Same persistence scope as the original: per-session, not per-account.
static FAULT_STORE: Lazy<Mutex<FaultRuleStore>> =
    Lazy::new(|| Mutex::new(FaultRuleStore::default()));

fn lock_store() -> std::sync::MutexGuard<'static, FaultRuleStore> {
    FAULT_STORE.lock().unwrap_or_else(|p| p.into_inner())
}

// ---------------------------------------------------------------------------
// Protocol + S3 REST operation detection
// ---------------------------------------------------------------------------

/// Protocol for a service from the gateway's service table; `None` for
/// unknown services falls back to the JSON error branch, like python's
/// `get_service_protocol` returning `None`.
fn service_protocol(service: &str) -> Option<&'static str> {
    // Tuple layout: (name, type, protocol, description)
    SERVICE_REGISTRY
        .iter()
        .find(|(name, ..)| *name == service)
        .map(|(_, _, protocol, _)| *protocol)
}

/// Python middleware: rest-xml / query / ec2 services take XML error bodies.
fn is_xml_protocol(protocol: &str) -> bool {
    matches!(protocol, "rest-xml" | "query" | "ec2")
}

/// S3 REST operation detection from method/path/query, `None` for anything
/// but s3. Port of `_detect_rest_operation`
/// (`src/robotocore/gateway/handlers.py`), kept case-for-case so chaos
/// rules filtered on S3 operation names behave as they did.
fn detect_s3_rest_operation(
    method: &Method,
    path: &str,
    query_string: &str,
) -> Option<&'static str> {
    const SUBRESOURCE_OPS: &[(&str, &[(&str, &str)])] = &[
        ("acl", &[("GET", "GetBucketAcl"), ("PUT", "PutBucketAcl")]),
        (
            "versioning",
            &[
                ("GET", "GetBucketVersioning"),
                ("PUT", "PutBucketVersioning"),
            ],
        ),
        (
            "tagging",
            &[
                ("GET", "GetBucketTagging"),
                ("PUT", "PutBucketTagging"),
                ("DELETE", "DeleteBucketTagging"),
            ],
        ),
        (
            "lifecycle",
            &[
                ("GET", "GetBucketLifecycleConfiguration"),
                ("PUT", "PutBucketLifecycleConfiguration"),
                ("DELETE", "DeleteBucketLifecycle"),
            ],
        ),
        (
            "cors",
            &[
                ("GET", "GetBucketCors"),
                ("PUT", "PutBucketCors"),
                ("DELETE", "DeleteBucketCors"),
            ],
        ),
        (
            "policy",
            &[
                ("GET", "GetBucketPolicy"),
                ("PUT", "PutBucketPolicy"),
                ("DELETE", "DeleteBucketPolicy"),
            ],
        ),
        (
            "encryption",
            &[
                ("GET", "GetBucketEncryption"),
                ("PUT", "PutBucketEncryption"),
                ("DELETE", "DeleteBucketEncryption"),
            ],
        ),
        ("location", &[("GET", "GetBucketLocation")]),
        (
            "logging",
            &[("GET", "GetBucketLogging"), ("PUT", "PutBucketLogging")],
        ),
        (
            "notification",
            &[
                ("GET", "GetBucketNotificationConfiguration"),
                ("PUT", "PutBucketNotificationConfiguration"),
            ],
        ),
        (
            "replication",
            &[
                ("GET", "GetBucketReplication"),
                ("PUT", "PutBucketReplication"),
                ("DELETE", "DeleteBucketReplication"),
            ],
        ),
        (
            "website",
            &[("GET", "GetBucketWebsite"), ("PUT", "PutBucketWebsite")],
        ),
        ("delete", &[("POST", "DeleteObjects")]),
        ("uploads", &[("GET", "ListMultipartUploads")]),
    ];

    let params: std::collections::HashSet<&str> = query_string
        .split('&')
        .filter(|kv| !kv.is_empty())
        .map(|kv| kv.split('=').next().unwrap_or(""))
        .collect();
    for (param, ops) in SUBRESOURCE_OPS {
        if params.contains(*param) {
            return ops
                .iter()
                .find(|(m, _)| *m == method.as_str())
                .map(|(_, op)| *op);
        }
    }

    let trimmed = path.trim_matches('/');
    if trimmed.is_empty() {
        // Root path
        return if method == Method::GET {
            Some("ListBuckets")
        } else {
            None
        };
    }
    let (bucket, key) = match trimmed.split_once('/') {
        Some((b, rest)) => (Some(b), Some(rest)),
        None => (Some(trimmed), None),
    };
    let method_str = method.as_str();
    match (bucket, key.filter(|k| !k.is_empty())) {
        (Some(_), Some(_)) => match method_str {
            "PUT" => Some("PutObject"),
            "GET" => Some("GetObject"),
            "DELETE" => Some("DeleteObject"),
            "HEAD" => Some("HeadObject"),
            "POST" => Some("PutObject"), // POST upload
            _ => None,
        },
        (Some(_), None) => match method_str {
            "GET" => {
                let is_v2 = query_string.split('&').any(|kv| {
                    let mut parts = kv.splitn(2, '=');
                    parts.next() == Some("list-type") && parts.next() == Some("2")
                });
                Some(if is_v2 {
                    "ListObjectsV2"
                } else {
                    "ListObjects"
                })
            }
            "PUT" => Some("CreateBucket"),
            "DELETE" => Some("DeleteBucket"),
            "HEAD" => Some("HeadBucket"),
            _ => None,
        },
        (None, _) => None,
    }
}

// ---------------------------------------------------------------------------
// Interception — python chaos/middleware.py
// ---------------------------------------------------------------------------

pub struct ChaosIntercept {
    pub response: Response,
    pub rule_id: String,
    pub operation: String,
}

/// Check the fault store and build the injected response, if any. Called
/// from `catch_all_handler` before service dispatch (async context: latency
/// sleeps via tokio, like python's `time.sleep` inside its handler thread).
/// A latency-only rule sleeps and lets dispatch proceed; an `error_code`
/// rule answers with the protocol-aware fault body.
///
/// `operation_detected` is the gateway's target/action-derived operation;
/// S3 requests carry none, so the python-era REST detection fills the gap.
pub async fn intercept(
    service: &str,
    operation_detected: &str,
    method: &Method,
    path: &str,
    query_string: &str,
    region: &str,
    account: u64,
) -> Option<ChaosIntercept> {
    let operation = if operation_detected.is_empty() && service == "s3" {
        detect_s3_rest_operation(method, path, query_string)
            .unwrap_or_default()
            .to_string()
    } else {
        operation_detected.to_string()
    };

    let rule = {
        let mut store = lock_store();
        store.find_matching(
            service,
            (!operation.is_empty()).then_some(operation.as_str()),
            region,
            account,
        )
    }?;

    // python: `time.sleep(rule.latency_ms / 1000.0)`
    if rule.latency_ms > 0 {
        tokio::time::sleep(Duration::from_millis(rule.latency_ms as u64)).await;
    }

    let error_code = rule.error_code.as_ref()?;
    let request_id = Uuid::new_v4().simple().to_string();
    let (content_type, body) = if service_protocol(service)
        .map(is_xml_protocol)
        .unwrap_or(false)
    {
        // XML-protocol services expect XML error responses (admin-only
        // config, but still the one hand-built XML in the request path —
        // escape the interpolated code/message the way the rotocore
        // serializers do).
        let xml_escape = |value: &str| {
            value
                .replace('&', "&amp;")
                .replace('<', "&lt;")
                .replace('>', "&gt;")
                .replace('"', "&quot;")
                .replace('\'', "&apos;")
        };
        (
            "application/xml",
            format!(
                "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\
                 <ErrorResponse><Error>\
                 <Code>{}</Code>\
                 <Message>{}</Message>\
                 </Error>\
                 <RequestId>{}</RequestId>\
                 </ErrorResponse>",
                xml_escape(error_code),
                xml_escape(&rule.error_message),
                xml_escape(&request_id)
            ),
        )
    } else {
        // JSON-protocol services
        (
            "application/json",
            json!({
                "__type": error_code,
                "message": rule.error_message,
                "Message": rule.error_message,
                "RequestId": request_id,
            })
            .to_string(),
        )
    };

    let status = StatusCode::from_u16(rule.status_code.clamp(0, 599) as u16)
        .unwrap_or(StatusCode::INTERNAL_SERVER_ERROR);
    let response = Response::builder()
        .status(status)
        .header("content-type", content_type)
        .header("x-robotocore-chaos", &rule.rule_id)
        .body(axum::body::Body::from(body))
        .unwrap();
    Some(ChaosIntercept {
        response,
        rule_id: rule.rule_id,
        operation,
    })
}

// ---------------------------------------------------------------------------
// Admin CRUD endpoints — python gateway/app.py chaos_* handlers
// ---------------------------------------------------------------------------

pub async fn chaos_list_rules() -> Response {
    let rules = lock_store().list_rules();
    (StatusCode::OK, Json(json!({"rules": rules}))).into_response()
}

pub async fn chaos_add_rule(body: Bytes) -> Response {
    if body.is_empty() {
        return (
            StatusCode::BAD_REQUEST,
            Json(json!({"error": "No rule provided"})),
        )
            .into_response();
    }
    let data: Value = match serde_json::from_slice(&body) {
        Ok(v) => v,
        Err(_) => {
            return (
                StatusCode::BAD_REQUEST,
                Json(json!({"error": "Invalid JSON"})),
            )
                .into_response()
        }
    };
    if !data.is_object() {
        return (
            StatusCode::BAD_REQUEST,
            Json(json!({"error": "Expected JSON object"})),
        )
            .into_response();
    }
    // python: `FaultRule.from_dict(data)` raising ValueError (invalid
    // regex) escaped the route handler — starlette answered a plain 500.
    let rule = match FaultRule::from_json(&data) {
        Ok(rule) => rule,
        Err(_) => {
            return (StatusCode::INTERNAL_SERVER_ERROR, "Internal Server Error").into_response()
        }
    };
    let rule_id = lock_store().add(rule);
    (
        StatusCode::CREATED,
        Json(json!({"status": "created", "rule_id": rule_id})),
    )
        .into_response()
}

pub async fn chaos_delete_rule(Path(rule_id): Path<String>) -> Response {
    let removed = lock_store().remove(&rule_id);
    if removed {
        (
            StatusCode::OK,
            Json(json!({"status": "deleted", "rule_id": rule_id})),
        )
            .into_response()
    } else {
        (
            StatusCode::NOT_FOUND,
            Json(json!({"error": "Rule not found"})),
        )
            .into_response()
    }
}

pub async fn chaos_clear_rules(
    query: Query<std::collections::HashMap<String, String>>,
) -> Response {
    // `?account=X` clears only rules scoped to account X — the compat lane's
    // session-end safety net uses this to clean its own account without
    // clobbering a concurrently running chaos session on the shared twin.
    // The bare call clears everything, like python.
    let count = if let Some(account) = query.get("account").filter(|s| !s.is_empty()) {
        lock_store().clear_account(account)
    } else {
        lock_store().clear()
    };
    (
        StatusCode::OK,
        Json(json!({"status": "cleared", "count": count})),
    )
        .into_response()
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use http_body_util::BodyExt;
    use serde_json::json;

    fn rule_from(v: Value) -> FaultRule {
        FaultRule::from_json(&v).expect("rule should build")
    }

    fn seeded_store(rules: Vec<Value>) -> FaultRuleStore {
        let mut store = FaultRuleStore::default();
        for r in rules {
            store.add(rule_from(r));
        }
        store
    }

    async fn body_value(resp: Response) -> Value {
        let bytes = BodyExt::collect(resp.into_body()).await.unwrap().to_bytes();
        serde_json::from_slice(&bytes).unwrap()
    }

    /// The CRUD/intercept tests drive the process-wide FAULT_STORE — run
    /// them strictly serialized so they don't clear each other's rules.
    /// An async-aware lock, since the guard must span `.await` points.
    static TEST_SEQUENCER: once_cell::sync::Lazy<tokio::sync::Mutex<()>> =
        once_cell::sync::Lazy::new(|| tokio::sync::Mutex::new(()));

    #[test]
    fn defaults_mirror_python() {
        let rule = rule_from(json!({"service": "s3", "error_code": "ThrottlingException"}));
        assert_eq!(rule.status_code, 429);
        assert_eq!(rule.latency_ms, 0);
        assert!((0.999..=1.0).contains(&rule.probability));
        assert!(rule.enabled);
        assert!(rule.ttl_seconds.is_none());
        assert_eq!(
            rule.error_message,
            format!("Injected by chaos rule {}", rule.rule_id)
        );
        assert_eq!(rule.rule_id.len(), 12);

        let rule = rule_from(json!({"service": "s3", "error_code": "InternalError"}));
        assert_eq!(rule.status_code, 500);

        // Explicit status_code (even 0) is preserved — the python
        // test_chaos_bugs fix for `0 or 500` falsy handling.
        let rule = rule_from(json!({"error_code": "ThrottlingException", "status_code": 0}));
        assert_eq!(rule.status_code, 0);
    }

    #[test]
    fn to_value_carries_all_fields() {
        let rule = rule_from(json!({
            "service": "s3",
            "operation": "GetObject",
            "region": "us-east-1",
            "error_code": "SlowDown",
            "error_message": "boom",
            "status_code": 503,
            "latency_ms": 200,
            "probability": 0.75,
            "ttl_seconds": 30.5,
        }));
        let v = rule.to_value();
        for key in [
            "rule_id",
            "service",
            "operation",
            "region",
            "error_code",
            "error_message",
            "status_code",
            "latency_ms",
            "probability",
            "enabled",
            "ttl_seconds",
            "created_at",
            "match_count",
        ] {
            assert!(v.get(key).is_some(), "missing {key}");
        }
        assert_eq!(v["operation"], "GetObject");
        assert_eq!(v["status_code"], 503);
        assert_eq!(v["latency_ms"], 200);
        assert_eq!(v["probability"], 0.75);
        assert_eq!(v["ttl_seconds"], 30.5);
        assert_eq!(v["enabled"], true);
        assert_eq!(v["match_count"], 0);
    }

    #[test]
    fn probability_is_clamped() {
        assert!(rule_from(json!({"probability": 5.0})).probability <= 1.0);
        assert!(rule_from(json!({"probability": -3.0})).probability >= 0.0);
    }

    #[test]
    fn latency_ms_is_capped() {
        // MAX_RULE_LATENCY_MS: the sleep would run inline before dispatch,
        // so a wall-clock-like value (or a careless huge number) must not
        // freeze the shared twin for minutes. Negative values clamp to 0.
        assert_eq!(
            rule_from(json!({"latency_ms": 999_999})).latency_ms,
            MAX_RULE_LATENCY_MS
        );
        assert_eq!(rule_from(json!({"latency_ms": -5})).latency_ms, 0);
        assert_eq!(rule_from(json!({"latency_ms": 500})).latency_ms, 500);
    }

    #[test]
    fn service_glob_matching() {
        assert!(rule_from(json!({"service": "s3"})).service_matches("s3"));
        assert!(!rule_from(json!({"service": "s3"})).service_matches("sqs"));
        let star = rule_from(json!({}));
        assert!(star.service_matches("anything"));
        assert!(rule_from(json!({"service": "s3*"})).service_matches("s3control"));
        assert!(!rule_from(json!({"service": "s3*"})).service_matches("dynamodb"));
        assert!(rule_from(json!({"service": "s?"})).service_matches("s3"));
        assert!(!rule_from(json!({"service": "s?"})).service_matches("sqs"));
        assert!(rule_from(json!({"service": "s[qd]s"})).service_matches("sqs"));
        assert!(!rule_from(json!({"service": "s[qd]s"})).service_matches("s3"));
    }

    #[test]
    fn operation_regex_is_search_semantics() {
        let mut store = seeded_store(vec![
            json!({"service": "s3", "operation": "Create.*", "error_code": "X"}),
        ]);
        assert!(store
            .find_matching("s3", Some("CreateBucket"), "us-east-1", 123456789012)
            .is_some());

        let mut store = seeded_store(vec![
            json!({"service": "s3", "operation": "^Delete.*", "error_code": "X"}),
        ]);
        assert!(store
            .find_matching("s3", Some("CreateBucket"), "us-east-1", 123456789012)
            .is_none());
        assert!(store
            .find_matching("s3", Some("DeleteBucket"), "us-east-1", 123456789012)
            .is_some());

        // A rule with an operation filter does not match op-less requests
        // (S3 root requests carry "ListBuckets" via REST detection first).
        assert!(store
            .find_matching("s3", None, "us-east-1", 123456789012)
            .is_none());
    }

    #[test]
    fn service_rule_matches_opless_requests() {
        let mut store = seeded_store(vec![json!({"service": "s3", "error_code": "X"})]);
        assert!(store
            .find_matching("s3", None, "us-east-1", 123456789012)
            .is_some());
    }

    #[test]
    fn region_filter_is_exact() {
        let mut store = seeded_store(vec![json!({"region": "eu-west-1", "error_code": "X"})]);
        assert!(store
            .find_matching("s3", None, "eu-west-1", 123456789012)
            .is_some());
        assert!(store
            .find_matching("s3", None, "us-east-1", 123456789012)
            .is_none());
    }

    /// Account-scoped rules match only their account; unscoped rules match
    /// any account (python-parity default).
    #[test]
    fn account_filter_is_exact_and_unscoped_matches_all() {
        let mut scoped = seeded_store(vec![json!({
            "service": "s3", "error_code": "X", "account": "999777700001"
        })]);
        assert!(scoped
            .find_matching("s3", None, "us-east-1", 999777700001)
            .is_some());
        assert!(scoped
            .find_matching("s3", None, "us-east-1", 123456789012)
            .is_none());
        // non-numeric access keys resolve to the default account
        assert!(scoped
            .find_matching("s3", None, "us-east-1", 123456789012)
            .is_none());

        let mut unscoped = seeded_store(vec![json!({"service": "s3", "error_code": "X"})]);
        assert!(unscoped
            .find_matching("s3", None, "us-east-1", 999777700001)
            .is_some());
        assert!(unscoped
            .find_matching("s3", None, "us-east-1", 123456789012)
            .is_some());
    }

    #[test]
    fn disabled_rules_never_match() {
        let mut store = seeded_store(vec![
            json!({"service": "s3", "error_code": "X", "enabled": false}),
        ]);
        assert!(store
            .find_matching("s3", None, "us-east-1", 123456789012)
            .is_none());
    }

    #[test]
    fn probability_zero_never_one_always() {
        // 0.0: the draw (in [0,1)) is always > 0 in practice
        for _ in 0..50 {
            let mut store = seeded_store(vec![
                json!({"service": "s3", "error_code": "X", "probability": 0.0}),
            ]);
            assert!(store
                .find_matching("s3", None, "us-east-1", 123456789012)
                .is_none());
        }
        let mut store = seeded_store(vec![
            json!({"service": "s3", "error_code": "X", "probability": 1.0}),
        ]);
        assert!(store
            .find_matching("s3", None, "us-east-1", 123456789012)
            .is_some());
    }

    #[test]
    fn probability_draws_split_outcomes() {
        // p=0.5 over a decent sample must produce both outcomes.
        let mut hits = 0;
        for _ in 0..40 {
            let mut store = seeded_store(vec![
                json!({"service": "s3", "error_code": "X", "probability": 0.5}),
            ]);
            if store
                .find_matching("s3", None, "us-east-1", 123456789012)
                .is_some()
            {
                hits += 1;
            }
        }
        assert!((5..=35).contains(&hits), "hits={hits}");
    }

    #[test]
    fn ttl_expiry_checks_created_at() {
        let mut store = seeded_store(vec![json!({
            "service": "s3", "error_code": "X", "ttl_seconds": 10.0, "created_at": 0.0
        })]);
        assert!(store
            .find_matching("s3", None, "us-east-1", 123456789012)
            .is_none());

        let recent = FaultRule::now_epoch() - 1.0;
        let mut store = seeded_store(vec![json!({
            "service": "s3", "error_code": "X", "ttl_seconds": 10.0, "created_at": recent
        })]);
        assert!(store
            .find_matching("s3", None, "us-east-1", 123456789012)
            .is_some());
    }

    #[test]
    fn first_matching_rule_wins_and_counts() {
        let mut store = seeded_store(vec![
            json!({"service": "s3", "error_code": "First"}),
            json!({"service": "s3", "error_code": "Second"}),
        ]);
        let rule = store
            .find_matching("s3", None, "us-east-1", 123456789012)
            .unwrap();
        assert_eq!(rule.error_code.as_deref(), Some("First"));
        // match_count increments only on the matched rule
        let listed = store.list_rules();
        assert_eq!(listed[0]["match_count"], 1);
        assert_eq!(listed[1]["match_count"], 0);
    }

    #[test]
    fn no_match_leaves_store_untouched() {
        let mut store = seeded_store(vec![json!({"service": "dynamodb", "error_code": "X"})]);
        assert!(store
            .find_matching("s3", None, "us-east-1", 123456789012)
            .is_none());
        assert_eq!(store.list_rules()[0]["match_count"], 0);
    }

    #[test]
    fn add_remove_clear_round_trip() {
        let mut store = FaultRuleStore::default();
        let id1 = store.add(rule_from(json!({"service": "s3", "error_code": "X"})));
        store.add(rule_from(json!({"service": "sqs", "error_code": "X"})));
        assert_eq!(store.len(), 2);
        assert!(store.remove(&id1));
        assert!(!store.remove(&id1));
        assert_eq!(store.clear(), 1);
        assert_eq!(store.clear(), 0);
    }

    #[tokio::test]
    async fn add_rule_endpoint_wire_shape() {
        let _sequenced = TEST_SEQUENCER.lock().await;
        lock_store().clear();
        let resp = chaos_add_rule(Bytes::from_static(
            b"{\"service\": \"dynamodb\", \"error_code\": \"ThrottlingException\"}",
        ))
        .await;
        assert_eq!(resp.status(), StatusCode::CREATED);
        let body = body_value(resp).await;
        assert_eq!(body["status"], "created");
        assert_eq!(body["rule_id"].as_str().unwrap().len(), 12);
        lock_store().clear();
    }

    #[tokio::test]
    async fn add_rule_rejects_bad_bodies() {
        let _sequenced = TEST_SEQUENCER.lock().await;
        for (payload, expected) in [
            (Vec::new(), Some("No rule provided")),
            (b"{oops".to_vec(), Some("Invalid JSON")),
            (b"[1,2]".to_vec(), Some("Expected JSON object")),
        ] {
            let resp = chaos_add_rule(Bytes::from(payload)).await;
            assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
            if let Some(expected) = expected {
                let body = body_value(resp).await;
                assert_eq!(body["error"].as_str(), Some(expected));
            }
        }
        // Invalid regex in operation mirrors the python twin's unhandled
        // ValueError → plain-text 500.
        let resp = chaos_add_rule(Bytes::from_static(
            b"{\"service\": \"s3\", \"operation\": \"(unclosed\", \"error_code\": \"X\"}",
        ))
        .await;
        assert_eq!(resp.status(), StatusCode::INTERNAL_SERVER_ERROR);
    }

    #[tokio::test]
    async fn delete_endpoint_wire_shape() {
        let _sequenced = TEST_SEQUENCER.lock().await;
        lock_store().clear();
        let resp = chaos_add_rule(Bytes::from_static(
            b"{\"service\": \"sqs\", \"error_code\": \"X\", \"status_code\": 500}",
        ))
        .await;
        let id = body_value(resp).await["rule_id"]
            .as_str()
            .unwrap()
            .to_string();

        let del = chaos_delete_rule(Path(id)).await;
        assert_eq!(del.status(), StatusCode::OK);
        let body = body_value(del).await;
        assert_eq!(body["status"], "deleted");

        let resp = chaos_delete_rule(Path("nonexistent-id".to_string())).await;
        assert_eq!(resp.status(), StatusCode::NOT_FOUND);
        let body = body_value(resp).await;
        assert!(body["error"].is_string());
        lock_store().clear();
    }

    #[tokio::test]
    async fn list_and_clear_endpoints() {
        let _sequenced = TEST_SEQUENCER.lock().await;
        lock_store().clear();
        for svc in ["s3-chaos-default", "dynamodb-chaos-default"] {
            chaos_add_rule(Bytes::from(format!(
                "{{\"service\": \"{svc}\", \"error_code\": \"X\"}}"
            )))
            .await;
        }
        let resp = chaos_list_rules().await;
        assert_eq!(resp.status(), StatusCode::OK);
        let rules = body_value(resp).await["rules"].as_array().unwrap().clone();
        assert_eq!(rules.len(), 2);

        let resp = chaos_clear_rules(Query::default()).await;
        assert_eq!(resp.status(), StatusCode::OK);
        let body = body_value(resp).await;
        assert_eq!(body["status"], "cleared");
        assert_eq!(body["count"], 2);
        let resp = chaos_list_rules().await;
        assert_eq!(body_value(resp).await["rules"].as_array().unwrap().len(), 0);

        // clear on empty still answers 0
        let resp = chaos_clear_rules(Query::default()).await;
        assert_eq!(body_value(resp).await["count"], 0);
    }

    /// `?account=X` clears every rule that would apply to account X (rules
    /// scoped to X and unscoped/global ones). Rules scoped to other accounts
    /// survive — the lane's session-end safety net uses this so it never
    /// clobbers a concurrently running chaos session on the shared twin.
    #[test]
    fn clear_account_scopes_removal() {
        let mut store = seeded_store(vec![
            json!({"service": "s3", "error_code": "X", "account": "999777700001"}),
            json!({"service": "sqs", "error_code": "X", "account": "123456789012"}),
            json!({"service": "sns", "error_code": "X"}),
        ]);
        assert_eq!(store.clear_account("123456789012"), 2);
        assert_eq!(store.len(), 1);
        assert_eq!(store.clear_account("123456789012"), 0);
        // the other account's rule survives; the unscoped rule was removed
        assert_eq!(store.rules[0].account.as_deref(), Some("999777700001"));
        assert_eq!(store.clear(), 1);
    }

    #[tokio::test]
    async fn intercept_json_error_and_header() {
        let _sequenced = TEST_SEQUENCER.lock().await;
        lock_store().clear();
        let resp = chaos_add_rule(Bytes::from_static(
            b"{\"service\": \"chaos-json-test\", \"error_code\": \"ThrottlingException\", \"status_code\": 429}",
        ))
        .await;
        let id = body_value(resp).await["rule_id"]
            .as_str()
            .unwrap()
            .to_string();

        let hit = intercept(
            "chaos-json-test",
            "",
            &Method::POST,
            "/",
            "",
            "us-east-1",
            123456789012,
        )
        .await
        .expect("rule should match");
        assert_eq!(hit.response.status(), StatusCode::TOO_MANY_REQUESTS);
        assert_eq!(
            hit.response.headers().get("x-robotocore-chaos").unwrap(),
            id.as_str()
        );
        assert_eq!(
            hit.response.headers().get("content-type").unwrap(),
            "application/json"
        );
        let bytes = BodyExt::collect(hit.response.into_body())
            .await
            .unwrap()
            .to_bytes();
        let text = String::from_utf8(bytes.to_vec()).unwrap();
        assert!(text.contains("__type"));
        assert!(text.contains("ThrottlingException"));
        assert!(text.contains("Injected by chaos rule"));

        let rules = lock_store().list_rules();
        assert_eq!(rules[0]["error_code"], "ThrottlingException");
        assert!(rules[0]["match_count"].as_i64().unwrap() >= 1);
        lock_store().clear();
    }

    #[tokio::test]
    async fn intercept_xml_for_rest_xml_service() {
        let _sequenced = TEST_SEQUENCER.lock().await;
        lock_store().clear();
        // "s3" is rest-xml in the service table — XML error body expected.
        chaos_add_rule(Bytes::from_static(
            b"{\"service\": \"s3\", \"error_code\": \"ServiceUnavailableException\", \"status_code\": 503}",
        ))
        .await;

        let hit = intercept("s3", "", &Method::GET, "/", "", "us-east-1", 123456789012)
            .await
            .expect("rule should match");
        assert_eq!(hit.response.status(), StatusCode::SERVICE_UNAVAILABLE);
        let bytes = BodyExt::collect(hit.response.into_body())
            .await
            .unwrap()
            .to_bytes();
        let text = String::from_utf8(bytes.to_vec()).unwrap();
        assert!(text.starts_with("<?xml"), "body={text}");
        assert!(text.contains("<ErrorResponse>"));
        assert!(text.contains("ServiceUnavailableException"));
        lock_store().clear();
    }

    #[tokio::test]
    async fn latency_only_rule_sleeps_then_dispatches() {
        let _sequenced = TEST_SEQUENCER.lock().await;
        lock_store().clear();
        let id = {
            let mut store = lock_store();
            store.add(rule_from(
                json!({"service": "chaos-lat-test", "latency_ms": 1}),
            ))
        };
        let hit = intercept(
            "chaos-lat-test",
            "",
            &Method::POST,
            "/",
            "",
            "us-east-1",
            123456789012,
        )
        .await;
        assert!(hit.is_none(), "latency-only rules must not short-circuit");
        let rules = lock_store().list_rules();
        assert_eq!(rules[0]["rule_id"], id.as_str());
        assert_eq!(rules[0]["match_count"], 1);
        lock_store().clear();
    }

    #[tokio::test]
    async fn s3_rest_operation_detection() {
        assert_eq!(
            detect_s3_rest_operation(&Method::GET, "/", ""),
            Some("ListBuckets")
        );
        assert_eq!(
            detect_s3_rest_operation(&Method::GET, "/mybucket", ""),
            Some("ListObjects")
        );
        assert_eq!(
            detect_s3_rest_operation(&Method::GET, "/mybucket", "list-type=2"),
            Some("ListObjectsV2")
        );
        assert_eq!(
            detect_s3_rest_operation(&Method::PUT, "/mybucket/key", ""),
            Some("PutObject")
        );
        assert_eq!(
            detect_s3_rest_operation(&Method::GET, "/mybucket/key", ""),
            Some("GetObject")
        );
        assert_eq!(
            detect_s3_rest_operation(&Method::DELETE, "/mybucket", ""),
            Some("DeleteBucket")
        );
        assert_eq!(
            detect_s3_rest_operation(&Method::GET, "/mybucket", "acl"),
            Some("GetBucketAcl")
        );
        assert_eq!(detect_s3_rest_operation(&Method::POST, "/", ""), None);
    }
}
