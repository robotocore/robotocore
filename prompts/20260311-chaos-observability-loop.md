---
role: assistant
timestamp: "2026-03-11T21:30:00Z"
session: chaos-observability-loop
sequence: 1
---

# Chaos + Observability Feedback Loop

## Human prompt
Implement unified chaos + observability feedback loop: per-request context that flows through all middleware, chaos-audit bridge that records fault injections in the audit log, and a unified timeline API endpoint.

## Implementation decisions

1. **RequestContext as dataclass with contextvars**: Used `contextvars.ContextVar` for async-safe per-request state. The context carries `chaos_applied` list that accumulates events as middleware runs.

2. **Audit log integration via `record()` kwargs**: The existing `AuditLog.record()` uses keyword-only args (service, operation, etc.), not a dict. The bridge calls it with `operation="chaos:{fault_type}"` and `error="chaos_injection:{rule_name}"` to distinguish chaos entries from normal API calls.

3. **Timeline categorization**: Instead of separate storage, chaos events are identified by their `error` field prefix (`chaos_injection:`). This avoids schema changes to the ring buffer.

4. **Minimal middleware changes**: Only added 2 `record_chaos_event()` calls in `chaos/middleware.py` — one for latency injection, one for error injection. Import is inside the function to avoid circular imports.

5. **Route at `/_robotocore/timeline`**: Added between audit and usage routes in management_routes list.
