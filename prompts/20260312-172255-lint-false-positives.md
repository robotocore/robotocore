---
session: "agent-a2bb6cfb"
timestamp: "2026-03-12T17:22:55Z"
model: claude-opus-4-6
---

## Human

Fix three lint false positives in one PR:
1. Provider-import-sync regex fails on multi-line imports (rekognition false positive)
2. CloudWatch registry protocol says "query" but should be "json"
3. test-no-assertion check doesn't recognize _assert_ok() helper functions

## Assistant

## Key decisions

**Regex fix**: The `[^.]+` pattern in the provider-import-sync check matched across
newlines, so a multi-line import like `from robotocore.services.loader import (\n...)`
consumed characters past the next `from robotocore.services.rekognition.` import,
preventing it from matching. Fix: `[^.\n]+` to stop at newlines.

**CloudWatch protocol**: Changed from "query" to "json". The registry protocol field
is purely informational (lint check 7 compares it to botocore). Routing uses
`get_service_protocol()` from botocore directly, not the registry. The native provider
already handles both JSON and query dynamically.

**Assertion helper detection**: Added AST check for function calls where the function
name starts with `_assert` (e.g., `_assert_ok()`). This reduced test-no-assertion
findings from 178 to 101 (~77 fewer false positives).
