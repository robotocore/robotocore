//! Introspection endpoints: audit log, runtimes, services, usage analytics.
//!
//! Mirrors the Python server's `/_robotocore/*` admin API surface so drop-in
//! compat tests and dashboards work against the Rust server. Field names,
//! key spellings, and orderings must match `src/robotocore/audit/log.py`,
//! `src/robotocore/audit/analytics.py`, and the endpoints in
//! `src/robotocore/gateway/app.py` — see those when changing.

use once_cell::sync::Lazy;
use parking_lot::RwLock;
use serde_json::json;
use std::collections::{HashMap, VecDeque};

const AUDIT_CAPACITY: usize = 2000;
const RECENT_ERRORS_CAPACITY: usize = 100;

/// One audited request, matching the Python audit log entry shape
/// (see `src/robotocore/audit/log.py::record`).
#[derive(Debug, Clone, serde::Serialize)]
pub struct AuditEntry {
    /// Unix epoch seconds (float).
    pub timestamp: f64,
    pub service: String,
    pub operation: Option<String>,
    pub method: String,
    pub path: String,
    pub status_code: u16,
    pub duration_ms: f64,
    pub account_id: String,
    pub region: String,
    pub error: Option<String>,
}

struct AuditInner {
    entries: VecDeque<AuditEntry>,
    errors_by_status: HashMap<String, u64>,
    recent_errors: VecDeque<AuditEntry>,
}

static AUDIT: Lazy<RwLock<AuditInner>> = Lazy::new(|| {
    RwLock::new(AuditInner {
        entries: VecDeque::new(),
        errors_by_status: HashMap::new(),
        recent_errors: VecDeque::new(),
    })
});

pub fn epoch_now() -> f64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs_f64())
        .unwrap_or(0.0)
}

/// Record one request in the audit ring buffer.
pub fn record(entry: AuditEntry) {
    let mut inner = AUDIT.write();
    if inner.entries.len() >= AUDIT_CAPACITY {
        inner.entries.pop_front();
    }
    if entry.status_code >= 400 {
        *inner
            .errors_by_status
            .entry(entry.status_code.to_string())
            .or_insert(0u64) += 1;
        inner.recent_errors.push_back(entry.clone());
        if inner.recent_errors.len() > RECENT_ERRORS_CAPACITY {
            inner.recent_errors.pop_front();
        }
    }
    inner.entries.push_back(entry);
}

/// The most recent `limit` entries, NEWEST FIRST (Python `recent()` order),
/// with optional exact-match service/operation filters.
pub fn recent_entries(
    limit: usize,
    service: Option<&str>,
    operation: Option<&str>,
) -> Vec<AuditEntry> {
    let inner = AUDIT.read();
    let mut out = Vec::new();
    for e in inner.entries.iter().rev() {
        if let Some(s) = service {
            if e.service != s {
                continue;
            }
        }
        if let Some(o) = operation {
            if e.operation.as_deref() != Some(o) {
                continue;
            }
        }
        out.push(e.clone());
        if out.len() >= limit {
            break;
        }
    }
    out
}

/// Error totals grouped by status code plus the most recent errors,
/// matching Python `get_error_summary()` key names. `recent_errors` carries
/// the LAST 20 of the deque, newest first.
pub fn error_summary() -> serde_json::Value {
    let inner = AUDIT.read();
    let total: u64 = inner.errors_by_status.values().sum();
    let by_status_code: serde_json::Map<String, serde_json::Value> = inner
        .errors_by_status
        .iter()
        .map(|(k, v)| (k.clone(), json!(v)))
        .collect();
    let recent_errors: Vec<serde_json::Value> = inner
        .recent_errors
        .iter()
        .rev()
        .take(20)
        .map(entry_value)
        .collect();
    json!({
        "total_errors": total,
        "by_status_code": by_status_code,
        "by_error_type": {},
        "recent_errors": recent_errors,
    })
}

/// Compact error-listing shape Python emits inside `recent_errors`.
fn entry_value(entry: &AuditEntry) -> serde_json::Value {
    json!({
        "timestamp": entry.timestamp,
        "service": entry.service,
        "operation": entry.operation,
        "status_code": entry.status_code,
        "error_type": serde_json::Value::Null,
    })
}

/// Per-service request counters keyed by registry service names.
static REQUEST_COUNTS: Lazy<RwLock<HashMap<String, u64>>> =
    Lazy::new(|| RwLock::new(HashMap::new()));

pub fn bump_requests(service: &str) {
    let known = crate::core::service_registry::SERVICE_REGISTRY
        .iter()
        .any(|(name, _, _, _)| *name == service);
    if known {
        *REQUEST_COUNTS
            .write()
            .entry(service.to_string())
            .or_insert(0) += 1;
    }
}

pub fn request_count(service: &str) -> u64 {
    REQUEST_COUNTS.read().get(service).copied().unwrap_or(0)
}

static STARTED_AT: Lazy<std::time::Instant> = Lazy::new(std::time::Instant::now);

pub fn uptime_seconds() -> f64 {
    STARTED_AT.elapsed().as_secs_f64()
}
