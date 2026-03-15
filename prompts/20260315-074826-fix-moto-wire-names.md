---
name: Fix Moto wire name mismatches across batch 4-7 services
description: Build script to detect/fix _get_param() key mismatches, fix DataBrew/Athena/Config/Logs
type: session
role: assistant
timestamp: 2026-03-15T07:48:26Z
session: fix-moto-wire-names
sequence: 1
---

## Context

While fixing CI failures on PR #159 (event-trigger-tests), discovered that Moto's
`logs/responses.py` was using wrong JSON keys for all scheduled query operations.
The key insight: botocore wire names (what appears in the HTTP body) differ from
shape names. For JSON protocol services, wire name = `shape.serialization.get("name", shape_name)`.

## Key Findings

1. **CloudWatch Logs scheduled query bug** (the original trigger): All 5 scheduled query
   operations used wrong `_get_param` keys (`queryName`, `arn`, `logGroupNames`,
   `targetConfiguration`) when botocore wire names are `name`, `identifier`,
   `logGroupIdentifiers`, `destinationConfiguration`. Also required `queryLanguage`
   parameter (botocore required field).

2. **Accidental passing tests**: Tests asserting `ResourceNotFoundException` with fake
   ARNs passed accidentally when wrong param key read `None` → item not found →
   same exception, wrong reason. The fix caused these tests to start working correctly.

3. **`_get_param` reads 3 sources**: Body JSON, querystring, AND URI path match named
   groups. This is critical for the checker script to avoid false positives on URI params.

4. **Wire name vs shape name**: For REST-JSON services, wire names are camelCase via
   `serialization.name`. The script must use `shape.serialization.get("name", shape_name)`
   not just `shape_name`.

## Script: scripts/fix_moto_param_names.py

New script that:
- Parses Moto `responses.py` files extracting `_get_param("X")` calls per method
- Maps method names to botocore operation names (snake_to_pascal)
- Checks each param key against botocore wire names (body + URI + querystring)
- Reports CASING BUGS (auto-fixable) and UNKNOWN PARAMS (manual review)
- Strips comment lines before checking (avoids flagging commented-out code)
- `--write` applies casing fixes automatically

## Fixes Applied

Via `vendor/moto` `robotocore/all-fixes` branch, commit `6ca6e832`:

- **DataBrew**: `RecipeVersion` → `recipeVersion` in list_recipes/describe_recipe;
  `Name` → `name` for list_recipe_versions (querystring param); preserve `Name`
  for create_profile_job/create_recipe_job (body param)

- **Athena**: `get_resource_dashboard` was reading `WorkGroup` but botocore input
  shape only has `ResourceARN`; also updated `to_dict()` key from `WorkGroup` to
  `ResourceArn`

- **Config**: `describe_configuration_aggregator_sources_status` was reading `Filters`
  but botocore has `UpdateStatus` instead

- **Logs ScheduledQuery**:
  - Added required `queryLanguage` param (defaults to "CWLI")
  - Fixed `logGroupNames` → `logGroupIdentifiers` in `to_dict()` output
  - Added `queryLanguage` to `to_dict()` output
  - Fixed test `test_create_scheduled_query_conflict` to pass `queryLanguage`

## Lessons

- Global string replace (apply_fixes) is insufficient when different methods need
  different values for the same parameter string. DataBrew needed 3 rounds to converge.
  For complex cases, use targeted per-line edits instead of global replace.

- When a batch of services all have similar patterns, write the tool first (fix_moto_param_names.py),
  then run it to find real bugs vs. false positives before manually editing.

- SES dot-notation (`Message.Subject.Data`) and IoT nested params (inside `topicRulePayload`)
  are intentional patterns, not bugs — the script correctly identifies them as "unknown"
  but they are legitimate Moto implementation choices.

## PRs

- PR #162: fix/logs-scheduled-query — CloudWatch Logs ScheduledQuery fix
- PR #163: fix/wire-name-moto-fixes — DataBrew/Athena/Config wire name fixes + script
