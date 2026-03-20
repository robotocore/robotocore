---
session: "cloudwatch-gaps"
timestamp: "2026-03-20T00:06:47Z"
model: claude-opus-4-6
---

## Human

Implement 12 missing CloudWatch operations in the Moto fork and native provider:
MetricStream CRUD (5 ops), AlarmMuteRule stubs (3 ops), GetMetricWidgetImage,
GetInsightRuleReport, PutManagedInsightRules, DescribeAlarmContributors.
Write compat tests for all.

## Assistant

## Key decisions

**Native provider vs Moto**: CloudWatch has a native provider that intercepts
actions via `_ACTION_MAP` and falls back to Moto. Since the installed Moto comes
from a git remote (not vendor/), new operations must be implemented in the native
provider to be immediately available. Also implemented in vendor/moto for future
fork push.

**MetricStreams**: Full CRUD with in-memory dict store in the native provider.
Streams have state (running/stopped) toggled by Start/StopMetricStreams.

**GetMetricWidgetImage**: Returns a minimal 1x1 transparent PNG. Real rendering
would require a charting library; the stub satisfies the API contract.

**GetInsightRuleReport**: Returns empty Contributors list after verifying the
rule exists (falls back to Moto's insight rule store).

**AlarmMuteRules**: Stub-only since these are a newer CloudWatch feature.
PutAlarmMuteRule accepts input, GetAlarmMuteRule returns ResourceNotFoundException,
DeleteAlarmMuteRule is a no-op.
