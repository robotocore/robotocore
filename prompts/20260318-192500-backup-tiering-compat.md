---
session: "a64316d4"
timestamp: "2026-03-18T19:25:00Z"
model: claude-sonnet-4-6
tools: [bash, read, edit, write, glob, grep]
---

## Human

Expand compat tests for the backup service. Target the untested-but-working operations
(gap=22). Run chunk_service.py, probe each untested operation, write tests in
tests/compatibility/test_backup_compat.py.

## Assistant

## Key decisions

**Probing first**: Ran chunk_service.py which showed 24 untested ops across 20 chunks.
Probed each op against the live server (port 4566) before writing any tests.

**Most ops return 501**: The bulk of "untested" ops (GetBackupPlan, GetLegalHold,
ListBackupPlanVersions, ListBackupSelections, CreateBackupSelection,
ExportBackupPlanTemplate, ListRecoveryPointsByBackupVault,
ListProtectedResourcesByBackupVault, AssociateBackupVaultMpaApprovalTeam,
CreateRestoreAccessBackupVault, ListRestoreAccessBackupVaults, etc.) all return
HTTP 501 NotImplemented. Per project rules, no tests written for unimplemented ops.

**Already tested**: Many "untested" ops per the chunk script were already covered by
existing tests (PutBackupVaultAccessPolicy, DeleteBackupVaultAccessPolicy,
PutBackupVaultNotifications, DeleteBackupVaultNotifications, UpdateGlobalSettings,
UpdateRegionSettings, ListBackupPlanTemplates, ListFrameworks, GetBackupPlanFromJSON).

**TieringConfiguration CRUD works**: CreateTieringConfiguration,
GetTieringConfiguration, UpdateTieringConfiguration, DeleteTieringConfiguration
all returned valid responses. Added 5 new tests:
- test_create_tiering_configuration_returns_arn
- test_create_tiering_configuration_appears_in_list
- test_get_tiering_configuration_after_create (needed fix: response nests under
  TieringConfiguration key, not at top level)
- test_update_tiering_configuration_returns_name
- test_delete_tiering_configuration_removes_from_list

**Response shape discovery**: GetTieringConfiguration returns
`{"TieringConfiguration": {...}}` not `{"TieringConfigurationName": ..., ...}` at top
level. Caught this by running the test first and fixing the assertion.

All 145 tests pass. Quality gate: 0% no-server-contact rate.
