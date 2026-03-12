---
session: "a2823338"
timestamp: "2026-03-11T12:00:00Z"
model: claude-opus-4-6
---

## Human

Write comprehensive compat tests for Lambda custom runtimes (provided, provided.al2, provided.al2023). These are used for Go, Rust, and other compiled languages via a `bootstrap` executable. Test scenarios include shell script bootstrap, event processing, env vars, errors, timeouts, logs, isolation, and all three runtime variants.

## Assistant

## Key decisions

**Bootstrap scripts as shell**: Used bash scripts that read JSON from stdin and echo JSON to stdout, matching the subprocess protocol in `base.py:run_subprocess()`. This mirrors how compiled Go/Rust binaries would behave.

**ZipInfo.external_attr for permissions**: Set `info.external_attr = 0o755 << 16` to embed Unix executable permissions in the zip. The executor also does `os.chmod` to add S_IEXEC, so even non-executable zips work -- tested this explicitly.

**Error response shape**: Discovered that `bootstrap not found` and `timeout` errors return `null` payload with the error type in `FunctionError` header (not in the JSON body). Adjusted assertions to check `resp["FunctionError"]` instead of parsing the null payload.

**Transform test uses python3 inside bash**: The transform bootstrap pipes the event through `python3 -c` to parse JSON and extract fields. This works because the host has Python installed and the subprocess inherits the PATH.

**13 tests, 100% effective rate**: All tests contact the server and assert on response content. Validated with `validate_test_quality.py`.
