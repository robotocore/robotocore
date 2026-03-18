---
session: "a087aed1"
timestamp: "2026-03-18T02:31:31Z"
model: claude-sonnet-4-6
---

## Human

Expand compat tests for the `codepipeline` service. Read chunks/codepipeline.json to find untested operations, probe the live server, and add tests to tests/compatibility/test_codepipeline_compat.py for any working operations.

## Assistant

## Key decisions

**Chunks file was stale**: The chunks/codepipeline.json marked all 3 operations as `not_implemented`, but live probing showed all three actually work.

**Operations covered**:
- `PutActionRevision`: tested with a real pipeline (create → put revision → assert pipelineExecutionId returned), plus a nonexistent-pipeline error path
- `ListDeployActionExecutionTargets`: tested via nonexistent pipeline → PipelineNotFoundException pattern
- `UpdateActionType`: tested by creating a custom action type first, then updating it, asserting HTTP 200

**Why the existing chunks were wrong**: The probe tool classifies ops as `not_implemented` when they raise exceptions. `UpdateActionType` raises `ActionTypeNotFoundException` which looks like "not implemented" but the operation itself is fully implemented — it just needs an existing action type. The chunks should be re-probed with smarter classification logic.

**Test placement**: Added `test_list_deploy_action_execution_targets_nonexistent` to the existing `TestCodePipelineMiscOps` class (fits thematically), and created a new `TestCodePipelineActionRevision` class for the two PutActionRevision tests and UpdateActionType.

**Quality gate**: 0% no-server-contact rate, 80.4% effective test rate. All 46 tests pass.
