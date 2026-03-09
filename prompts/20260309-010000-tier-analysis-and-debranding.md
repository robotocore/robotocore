---
session: "d5f4ace5"
timestamp: "2026-03-09T01:00:00Z"
model: claude-opus-4-6
sequence: 1
---

## Human

Make sure we have a gap analysis against LocalStack's higher tier offerings.
It should all be quite legible.

## Assistant

### Key decisions

**Tier classification**: Mapped all 147 robotocore services to LocalStack pricing
tiers (community/base/ultimate/not_offered) by fetching their actual pricing page.
Added `tier_analysis()` function to `scripts/analyze_localstack.py` with `--tier-analysis`
CLI flag and updated `Makefile` gap-analysis target to run 3 reports.

**Key finding**: robotocore provides 112 services free that are paid or absent in
LocalStack (1,988 operations). 49 services robotocore has via Moto that LocalStack
doesn't offer at any price.

**Naming inconsistency fix**: Three naming conventions were colliding (botocore names,
LocalStack vendor dir names, robotocore dir names). Standardized on botocore names
throughout the gap report by adding `_name_to_registry` mapping lookup.

## Human

Looks like somehow we merged our robotocore/all-fixes branch of my fork of Moto
to the main branch of my fork. For now let's leave it like that. Find all references
to robotocore/all-fixes and update accordingly in our skills and rules and plans.

## Assistant

### Key decisions

**Branch update**: Changed `pyproject.toml` Moto dependency from
`branch = "robotocore/all-fixes"` to `branch = "master"`. Ran `uv lock
--upgrade-package moto` to re-resolve. Updated `scripts/overnight.sh` push target.
Updated CLAUDE.md Moto fixes workflow documentation. Updated MEMORY.md references.

## Human

Let's not reference localstack anywhere in robotocore except in the gap analysis.

## Assistant

### Key decisions

**Scope**: Swept entire codebase for LocalStack/localstack references. Found ~40
across 20+ files. Categorized each as "remove" (branding/provenance) or "keep"
(functional requirement).

**Kept references** (functional):
- `localstack.extensions` entry point group name in `extensions/compat.py` — third-party
  extensions register under this name, changing it would break them
- Backwards-compat aliases (`LocalStackExtensionAdapter`, `discover_localstack_extensions`,
  `load_localstack_extension_module`) — kept as module-level aliases pointing to renamed classes
- `scripts/analyze_localstack.py` — the gap analysis tool, explicitly excluded per user request

**Removed references** (~30):
- Project description, philosophy, docstrings in `__init__.py`, `app.py`, `handler_chain.py`,
  `moto_bridge.py`, `presigned.py`, `banner.py`
- `vendor/localstack/` documentation in CLAUDE.md
- Test docstrings across 7 parity test files and 3 compat test files
- GHA workflow renamed `localstack-parity.yml` -> `parity.yml`, removed the job that
  ran tests against actual LocalStack

**Directory rename**: `tests/localstack_parity/` -> `tests/parity/` via `git mv`.
Updated GHA workflow path and README.

**Test fix**: `test_wraps_extension` asserted `"LocalStack" in adapter.description`
which broke after the class rename. Changed to `"External extension" in adapter.description`.

**Banner change**: Replaced `"DNS: *.localhost.localstack.cloud resolves to 127.0.0.1"`
with `"DNS: use AWS_ENDPOINT_URL=http://localhost:{port}"` — more useful guidance anyway.
