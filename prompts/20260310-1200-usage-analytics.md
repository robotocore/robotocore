---
session: "agent-a3bdf0a1"
timestamp: "2026-03-10T12:00:00Z"
model: claude-opus-4-6
---

## Human

Implement usage analytics and Stack Insights for robotocore -- API usage
statistics, error rates, and usage patterns. Track per-service and per-operation
stats, time series, error breakdown, client patterns. Add management API
endpoints at /_robotocore/usage/*.

## Assistant

Created `src/robotocore/audit/analytics.py` with a `UsageAnalytics` class that
provides thread-safe in-memory counters for:

- Per-service stats (total, success, error, avg latency)
- Per-operation stats at the same granularity
- Rolling per-minute time series (configurable window, default 60 min)
- Error breakdown by status code and error type
- Client tracking by access key ID

Added 5 management endpoints:
- GET /_robotocore/usage -- overall summary
- GET /_robotocore/usage/services -- per-service breakdown
- GET /_robotocore/usage/services/{service} -- single service detail with ops
- GET /_robotocore/usage/errors -- error breakdown with recent errors
- GET /_robotocore/usage/timeline -- per-minute request counts

Wired recording into the existing `audit_response_handler` in handlers.py so
every AWS request automatically feeds the analytics engine.

Configuration: USAGE_ANALYTICS=1 (default enabled), USAGE_ANALYTICS_WINDOW=60.

Tests: 14 unit tests + 6 integration tests, all passing. Covers counter
increments, latency tracking, error breakdown, client tracking, thread safety,
env var disable, time series bucketing, rolling window pruning, and all
endpoint responses.
