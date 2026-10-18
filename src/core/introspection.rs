//! Introspection endpoints: audit log, runtimes, services.
//!
//! Mirrors the Python server's `/_robotocore/*` admin API surface so drop-in
//! compat tests and dashboards work against the Rust server.

use once_cell::sync::Lazy;
use parking_lot::RwLock;
use serde_json::json;
use std::collections::{HashMap, VecDeque};

/// One audited request.
#[derive(Debug, Clone, serde::Serialize)]
pub struct AuditEntry {
    pub service: String,
    pub operation: String,
    #[serde(rename = "status_code")]
    pub status_code: u16,
    pub account: u64,
    pub region: String,
    pub timestamp: String,
}

const AUDIT_CAPACITY: usize = 2000;

struct AuditInner {
    entries: VecDeque<AuditEntry>,
    errors_by_status: HashMap<u16, u64>,
    recent_errors: VecDeque<AuditEntry>,
}

static AUDIT: Lazy<RwLock<AuditInner>> = Lazy::new(|| {
    RwLock::new(AuditInner {
        entries: VecDeque::new(),
        errors_by_status: HashMap::new(),
        recent_errors: VecDeque::new(),
    })
});

/// Record one request in the audit ring buffer.
pub fn record(service: &str, operation: &str, status_code: u16, account: u64, region: &str) {
    let entry = AuditEntry {
        service: service.to_string(),
        operation: operation.to_string(),
        status_code,
        account,
        region: region.to_string(),
        timestamp: chrono::Utc::now().to_rfc3339(),
    };
    let mut inner = AUDIT.write();
    if inner.entries.len() >= AUDIT_CAPACITY {
        inner.entries.pop_front();
    }
    if status_code >= 400 {
        *inner.errors_by_status.entry(status_code).or_insert(0u64) += 1;
        inner.recent_errors.push_back(entry.clone());
        if inner.recent_errors.len() > 100 {
            inner.recent_errors.pop_front();
        }
    }
    inner.entries.push_back(entry);
}

/// Recent entries, oldest first, up to `limit`.
pub fn recent_entries(limit: usize) -> Vec<AuditEntry> {
    let inner = AUDIT.read();
    let n = inner.entries.len().saturating_sub(limit);
    inner.entries.iter().skip(n).cloned().collect()
}

/// Error totals grouped by status code plus the most recent errors.
pub fn error_summary() -> serde_json::Value {
    let inner = AUDIT.read();
    let total: u64 = inner.errors_by_status.values().sum();
    let statuses: serde_json::Map<String, serde_json::Value> = inner
        .errors_by_status
        .iter()
        .map(|(k, v)| (k.to_string(), json!(v)))
        .collect();
    let recent: Vec<serde_json::Value> = inner
        .recent_errors
        .iter()
        .take(20)
        .map(|e| {
            json!({
                "service": e.service,
                "operation": e.operation,
                "status_code": e.status_code,
                "account": e.account,
                "region": e.region,
                "timestamp": e.timestamp,
            })
        })
        .collect();
    json!({
        "total_errors": total,
        "by_status": statuses,
        "recent": recent,
    })
}
