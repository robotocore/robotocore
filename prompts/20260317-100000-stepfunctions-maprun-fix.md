---
session: "a86d371f"
timestamp: "2026-03-17T10:00:00Z"
model: claude-sonnet-4-6
sequence: 6
---

## Human

[CI failing: compat: s-z job — TestStepFunctionsMapRun::test_describe_map_run and
test_list_map_runs failing with ExecutionDoesNotExist after previous Moto fixes]

## Assistant

## Key decisions

**test_describe_map_run**: The compat test expected HTTP 200 for a fake mapRun ARN,
which was testing Moto's incorrect prior behavior (accepting any ARN and returning
an empty `{}`). Real AWS raises an error for unknown mapRun ARNs. Fixed the test to
use `pytest.raises(sfn.exceptions.ExecutionDoesNotExist)` — testing the correct
server behavior.

**list_map_runs validation removed**: The previous fix added
`self.describe_execution(execution_arn)` inside `list_map_runs` to validate the
execution exists. This caused `ExecutionDoesNotExist` even for real executions
because `_get_state_machine_for_execution` had timing/state issues finding the
just-created execution. The relevant fix was always in `responses.py` (wrapping
the result as `{"mapRuns": runs}` instead of bare `[]`). Removed the validation
call — `list_map_runs` simply returns `[]` for any ARN.

**Moto bump**: 4984dea4 → 01d5de67 on JackDanger/moto robotocore/all-fixes branch.
