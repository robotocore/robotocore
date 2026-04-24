---
title: "Fix: JSON error handling in all management endpoints"
date: 2026-04-23
phase: fix
tags: [gateway, error-handling, json]
---

## Human

Beam search through the codebase and find other things to fix. The beam search found unhandled `json.loads()` calls in management endpoints: `save_state`, `load_state`, `pods_save`, `pods_load` in `app.py`.

## What was done

Extended the `json.JSONDecodeError` fix from `import_state` to all remaining management endpoints that call `json.loads(body)` without error handling:

- `save_state` (line ~409)
- `load_state` (line ~437)
- `pods_save` (line ~985)
- `pods_load` (line ~1008)

Added matching unit tests for `save_state` and `load_state` invalid JSON cases in `test_internal_endpoints.py`.

## Why

All four endpoints would 500 on malformed JSON. `chaos_add_rule` and `update_config` already had proper handling — these were the remaining gaps found by grepping for unguarded `json.loads(body)`.
