---
session: "agent-aed7d1af"
timestamp: "2026-03-10T20:00:00Z"
model: claude-opus-4-6
---

## Human

Implement a comprehensive diagnostic endpoint for robotocore that returns a full system diagnostic bundle at GET /_robotocore/diagnose. Bundle includes system, server, config, services, state, background_engines, memory, audit, and extensions sections. Section filtering via query param. Security gate: requires DEBUG=1 or ROBOTOCORE_DIAG=1. Write tests first.

## Assistant

## Key decisions

**Module naming**: Created `diagnostics_bundle.py` rather than extending the existing `diagnostics.py` module, which handles error diagnostic headers (x-robotocore-diag). The two have different purposes and audiences.

**Section-based architecture**: Each diagnostic section is a standalone collector function, enabling both modular testing and the `?section=` query parameter filtering. Collectors are registered in a `_COLLECTORS` dict for easy extensibility.

**Config masking**: Any env var key containing SECRET, KEY, PASSWORD, or TOKEN gets masked to `***MASKED***`. This is conservative by design -- better to over-mask than leak credentials.

**Memory collection**: Uses the `resource` module (stdlib) rather than requiring `psutil` as a dependency. Falls back gracefully when `/proc/self/status` isn't available (macOS).

**Security gate**: Returns 403 unless DEBUG=1 or ROBOTOCORE_DIAG is set. This prevents accidental exposure of system internals in production-like deployments.

**Route delegation**: Added a thin `_diagnose_handler` in `app.py` that delegates to the bundle module, keeping the app file's import load light via lazy import.

**Test count**: 40 tests (25 unit + 6 integration-semantic + 4 endpoint access + 5 section filtering).
