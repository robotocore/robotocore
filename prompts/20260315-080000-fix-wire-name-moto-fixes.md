---
name: Fix Moto wire name mismatches (DataBrew, Athena, Config, Logs)
description: Fix _get_param() key names in Moto responses.py for 4 services + add checker script
type: session
role: assistant
timestamp: 2026-03-15T08:00:00Z
session: fix-wire-name-moto-fixes
sequence: 1
---

## Human Prompt

Fix all Moto _get_param wire name mismatches by writing software that makes the
changes, then running it.

## Changes

### scripts/fix_moto_param_names.py (new)

Script to check/fix `_get_param()` key names in Moto `responses.py` files against
botocore wire names. Features:
- Reads all `_get_param("X")` calls per method (skipping comments)
- Maps method names → botocore operations (snake_to_pascal)
- Uses `all_wire_names` (body + URI + querystring) to avoid false positives on URI params
- Reports CASING BUGS (auto-fixable) and UNKNOWN PARAMS (needs manual review)
- `--write` applies casing fixes automatically

### uv.lock / vendor/moto update (moto commit 6ca6e832)

- **DataBrew**: `RecipeVersion` → `recipeVersion` in list_recipes/describe_recipe;
  `Name` → `name` for list_recipe_versions (querystring param); preserve `Name`
  for create_profile_job/create_recipe_job (body param, wire=`Name`)
- **Athena**: `get_resource_dashboard` reads `ResourceARN` not `WorkGroup` (botocore
  input shape has only `ResourceARN`)
- **Config**: `describe_configuration_aggregator_sources_status` reads `UpdateStatus`
  not `Filters` (botocore has UpdateStatus, no Filters)
- **Logs ScheduledQuery**: add `queryLanguage` param (botocore required), fix
  `to_dict()` to return `logGroupIdentifiers` and `queryLanguage`

## Key Decisions

The global string replace approach in `apply_fixes` is insufficient when different
methods need different values for the same string. DataBrew's `Name` vs `name`
conflict required targeted manual edits for the methods that couldn't be resolved
via global replace. For future service audits, use the script for casing bugs
but do targeted edits for any method-specific conflicts.
