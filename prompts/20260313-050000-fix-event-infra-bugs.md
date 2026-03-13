---
session: "7e75731e"
timestamp: "2026-03-13T05:00:00Z"
model: claude-opus-4-6
---

## Human

Implement plan to find and fix every remaining bug in event-driven
infrastructure. 7 confirmed bugs from code review of all 9 event-driven
components.

## Assistant

### Key decisions

**Cross-service account_id parsing (Phase 1)**: All 5 cross-service delivery
functions (S3→SQS, S3→SNS, EB→SQS, EB→SNS, SNS→SQS) parsed region from target
ARNs but ignored account_id, defaulting to "123456789012". Fixed all to parse
both fields. Also re-keyed DynamoDB Streams store by (account_id, region) tuple
and updated all 3 callers (hooks.py, event_source.py, ttl.py).

**Alarm scheduler actions_enabled (Phase 2)**: Removed early return that skipped
metric evaluation entirely. AWS behavior: alarms always evaluate and transition
state; actions_enabled only gates action dispatch.

**Alarm Lambda/EC2 action routing (Phase 3)**: Added _invoke_lambda_action
method and :automate: no-op routing. Previously Lambda ARNs fell through to
_publish_to_sns which silently failed.

**Qualified ARN extraction (Phase 4)**: _extract_function_name now returns
"name:alias" for 8-part ARNs instead of dropping the qualifier.

**Position dict initialization (Phase 5)**: Moved from hasattr checks to proper
__init__ initialization for _kinesis_positions and _dynamo_stream_positions.

### Test approach

18 new unit tests across 3 files, all using mocks to verify the fix logic
directly. Patched at the correct import path (e.g. robotocore.services.sqs.provider._get_store
not the local re-import in events/provider.py).
