---
session: moto-coverage-update
timestamp: "2026-03-11T07:00:00Z"
model: claude-opus-4-6
---

## Human

Update moto dependency to include 64 new operations across 12 services implemented in the jackdanger/moto fork. Update stats and pages build.

## Assistant

## Key decisions

**Moto update**: Updated uv.lock to point to latest moto fork commit with expanded service coverage across 12 services.

**Stats update**: Bumped test count to 16,641 tests, 46 native providers, 7,832 ops implemented.

**Pages build**: Auto-inject coverage stats into the GitHub Pages build script so the dashboard stays current.
