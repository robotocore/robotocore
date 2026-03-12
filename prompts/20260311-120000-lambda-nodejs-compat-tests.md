---
session: "agent-acb91ff2"
timestamp: "2026-03-11T12:00:00Z"
model: claude-opus-4-6
---

## Human

Write comprehensive compat tests for Node.js Lambda execution in Robotocore. Tests should cover: basic handlers, async handlers, callback handlers, context object, env vars, error handling, JSON parsing, isolation, handler-not-found, timeout, large payloads, console.log, and built-in module usage. Every test must contact the server and assert on response content.

## Assistant

## Key decisions

**Researched the runtime stack first**: Read `node.py` (executor), `bootstrap.js` (the Node.js bootstrap that reads stdin/writes stdout), `base.py` (subprocess runner), and `executor.py` (Python in-process executor for comparison). This revealed:
- Node.js runs as a subprocess: `node bootstrap.js <handler>`
- Event is JSON on stdin, result is JSON on stdout, logs on stderr
- Timeout is subprocess timeout + 2s grace period
- bootstrap.js dispatches to async (<=2 args) or callback (3 args) based on `handlerFunc.length`

**Timeout test needed special handling**: The server returns `FunctionError: Task.TimedOut` with `payload: null` (not a structured error body). The initial test assumed a structured error in the payload, which failed. Fixed to check `FunctionError` header instead and tolerate null payload.

**Used console.error instead of console.log for log capture**: The bootstrap protocol sends handler result on stdout and logs on stderr. Using `console.log` would pollute stdout and break JSON parsing. Used `console.error` for the log capture test to match the protocol.

**Subprocess isolation verified**: Since Node.js runs as a fresh subprocess per invocation (unlike Python's in-process execution), global state like `let counter = 0` resets each time. Wrote a test that confirms counter is always 1 across 3 invocations.

**34 tests, 100% effective rate**: All tests contact the server, all assert on response content. Zero no-contact tests per `validate_test_quality.py`.
