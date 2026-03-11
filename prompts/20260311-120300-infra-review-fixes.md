---
session: ci-fix-wave1
timestamp: "2026-03-11T12:03:00Z"
model: claude-opus-4-6
---

# Fix CI for infra review tests PR

## Human

Fix CI failures on Wave 1 PR #84: add prompt log and fix unit test failures in gateway and infra shards.

## Assistant

## Key decisions

**Chaos rule fields**: Test `test_listed_rule_has_all_fields` expected a fixed set of keys but chaos rules now include `ttl_seconds`. Added it to the expected set.

**Audit log error key**: `test_record_empty_error_string` and `test_record_without_error_no_key` assumed the `error` key is absent when no error occurs. The AuditLog always includes the key (as `None`). Fixed tests to assert `entry.get("error") is None`.

**Audit log duration rounding**: Tests expected 2-decimal rounding but the AuditLog rounds to 3 decimals. Fixed assertions to match actual behavior (1.235, 12.345).

**Chaos middleware sleep**: Tests patched `time.sleep` but the middleware uses `asyncio.sleep` (via `asyncio.run` in sync context). Rewrote tests to patch `asyncio.run` instead.
