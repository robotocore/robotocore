---
session: "a7a068c0"
timestamp: "2026-03-10T16:00:00Z"
model: claude-sonnet-4-6
sequence: 2
---

## Human

check ci

## Assistant

PR #32 (prompt log) — all CI green, automerge pending.
PR #23 (`tier1-e2e-tests`) — `unit: services (3.13)` failing with the same two tests:
- `TestExecutePythonHandlerLayers::test_function_code_overrides_layer` → `error_type == 'Handled'`
- `TestExecutePythonHandlerLayers::test_invalid_layer_zip_skipped` → `error_type == 'Runtime.HandlerNotFound'`

Root cause: the Lambda executor stores helper modules (e.g. `shared`) imported by a handler
under their plain name in `sys.modules`, with no function-scoped prefix. In pytest-xdist workers
tests run sequentially within each process; a prior invocation's `shared` module remains cached
and is returned on the next test's `import shared`, causing either wrong values or errors depending
on whether the old tmpdir still existed.

The `_lambda_*` namespace applied to the top-level handler module (`_lambda_override-fn.lambda_function`)
correctly scopes that entry, but modules imported transitively by the handler keep their plain names.

Fix: added `_clear_plain_modules_for_dir(tmpdir)` — called before every execution — which removes
non-`_lambda_*` modules from `sys.modules` whose `__file__` lives in the current tmpdir. The
`_lambda_*` entries are intentionally preserved so `hot_reload=False` caching still works for the
handler module itself. All 4,050 unit tests pass; `test_hot_reload_disabled_keeps_old_code` also
still passes because the handler module's `_lambda_*` key is untouched.
