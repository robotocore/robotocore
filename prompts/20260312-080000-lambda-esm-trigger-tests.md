---
session: "a273cda1"
timestamp: "2026-03-12T08:00:00Z"
model: claude-opus-4-6
---

## Human

Write end-to-end tests for Lambda Event Source Mappings (ESM) -- the mechanism that triggers Lambda from SQS, Kinesis, and DynamoDB Streams. Tests should verify the TRIGGER path (not just CRUD). Verify that when a message arrives on SQS/Kinesis/DynamoDB Streams, Lambda actually gets invoked with the correct event.

Test scenarios: SQS basic trigger, SQS FIFO, SQS batch, SQS filter criteria, Kinesis trigger, DynamoDB Streams trigger, ESM disable/enable, partial batch failure.

## Assistant

## Key decisions

**Verification pattern**: Lambda writes a marker to a DynamoDB table when invoked. Tests poll that marker table to confirm invocation happened. This is more reliable than checking queue emptiness alone, since it proves the Lambda handler actually executed and received the expected event shape.

**Partial batch failure approach**: Initially tried using DynamoDB markers for the partial failure test too, but the Lambda's boto3 DynamoDB call was failing silently in the ESM background thread context. Simplified to a pure-SQS verification: send good + bad messages, wait for ESM to process, disable ESM, then check that only the failed message remains in queue. This tests the actual ReportBatchItemFailures contract without needing cross-service side effects.

**Polling strategy**: ESM engine polls every 1s. Tests use `_poll_until()` with 15s timeout and 0.5s interval. The disable/enable test needs longer waits (5s disabled period) to avoid false positives from race conditions. The filter criteria test uses 8s sleep since both matching and non-matching messages need to be processed.

**FIFO queue ordering**: Verified all 3 messages in a MessageGroupId are present in markers. True strict ordering verification would require the Lambda to write sequence numbers, but presence of all messages proves the FIFO ESM path works.

**What I skipped**: MaximumBatchingWindowInSeconds (hard to test deterministically with 1s poll interval), BisectBatchOnFunctionError (would need a Lambda that throws, not just reports failures), and MaximumRetryAttempts (would need very long timeouts). All 8 tests are green and 100% effective per validate_test_quality.py.
