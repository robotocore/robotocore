"""Failing tests that expose bugs in the Step Functions provider.

Each test documents a specific bug. DO NOT fix the bugs -- only the tests live here.
"""

import json

import pytest

from robotocore.services.stepfunctions.asl import (
    ASLExecutor,
    _is_ddb_typed,
)
from robotocore.services.stepfunctions.provider import (
    SfnError,
    _create_state_machine,
    _describe_state_machine_for_execution,
    _execution_histories,
    _executions,
    _start_execution,
    _state_machines,
    _stop_execution,
)


def _clear_state():
    _state_machines.clear()
    _executions.clear()
    _execution_histories.clear()


def _make_executor(states: dict, start_at: str = "Start", execution_arn: str = "") -> ASLExecutor:
    return ASLExecutor(
        {"StartAt": start_at, "States": states},
        execution_arn=execution_arn,
    )


# ---------------------------------------------------------------------------
# Bug 1: _is_ddb_typed only checks the first value in the dict
#
# The `return False` on line 652 of asl.py is INSIDE the for loop body,
# causing the function to return after examining only the first dict value.
# A multi-key dict where the first value is not DDB-typed but a later one
# IS will incorrectly return False.
# ---------------------------------------------------------------------------


class TestIsDdbTypedBug:
    def test_is_ddb_typed_checks_all_values(self):
        """_is_ddb_typed should inspect all values, not just the first."""
        # First value has 2 keys so it's not recognized as typed.
        # Second value {"S": "Alice"} IS typed. The function should return True.
        item = {"multi_attr": {"S": "val", "extra": "x"}, "name": {"S": "Alice"}}
        result = _is_ddb_typed(item)
        assert result is True


# ---------------------------------------------------------------------------
# Bug 2: _describe_state_machine_for_execution returns definition as dict
#
# AWS API returns "definition" as a JSON string. The provider returns
# sm.get("definition", "{}") which is the parsed dict. It should use
# sm.get("definition_str") instead.
# ---------------------------------------------------------------------------


class TestDescribeStateMachineForExecutionBug:
    def setup_method(self):
        _clear_state()

    def teardown_method(self):
        _clear_state()

    def test_definition_returned_as_string(self):
        """DescribeStateMachineForExecution should return definition as a JSON string."""
        definition_str = json.dumps(
            {"StartAt": "Pass", "States": {"Pass": {"Type": "Pass", "End": True}}}
        )
        _create_state_machine(
            {"name": "sm1", "definition": definition_str, "roleArn": "r"},
            "us-east-1",
            "123",
        )
        sm_arn = "arn:aws:states:us-east-1:123:stateMachine:sm1"
        _start_execution(
            {"stateMachineArn": sm_arn, "name": "exec1", "input": "{}"},
            "us-east-1",
            "123",
        )
        exec_arn = f"{sm_arn.replace(':stateMachine:', ':execution:')}:exec1"
        result = _describe_state_machine_for_execution(
            {"executionArn": exec_arn}, "us-east-1", "123"
        )
        assert isinstance(result["definition"], str), (
            f"definition should be a JSON string, got {type(result['definition'])}"
        )


# ---------------------------------------------------------------------------
# Bug 3: stop_execution records ExecutionAborted with previousEventId=0
#
# _stop_execution calls history.execution_aborted() without passing the
# previous event ID, so aborted events always have previousEventId=0
# instead of chaining to the last event in the history.
# ---------------------------------------------------------------------------


class TestExecutionHistoryAbortedBug:
    def setup_method(self):
        _clear_state()

    def teardown_method(self):
        _clear_state()

    def test_execution_aborted_has_correct_previous_event_id(self):
        """StopExecution should record ExecutionAborted chained to the last event."""
        definition = json.dumps(
            {"StartAt": "Pass", "States": {"Pass": {"Type": "Pass", "End": True}}}
        )
        _create_state_machine(
            {"name": "sm1", "definition": definition, "roleArn": "r"},
            "us-east-1",
            "123",
        )
        sm_arn = "arn:aws:states:us-east-1:123:stateMachine:sm1"
        start_result = _start_execution(
            {"stateMachineArn": sm_arn, "name": "exec1", "input": "{}"},
            "us-east-1",
            "123",
        )
        exec_arn = start_result["executionArn"]

        _stop_execution({"executionArn": exec_arn}, "us-east-1", "123")

        history = _execution_histories.get(exec_arn)
        assert history is not None, "Execution should have history"
        events = history.get_events()
        aborted_events = [e for e in events if e["type"] == "ExecutionAborted"]
        assert len(aborted_events) == 1
        aborted = aborted_events[0]
        assert aborted["previousEventId"] != 0, (
            "ExecutionAborted should chain to previous event, got previousEventId=0"
        )


# ---------------------------------------------------------------------------
# Bug 4: Choice state skips OutputPath processing
#
# In _execute_state, the Choice branch (line 183) returns immediately:
#   return data, self._execute_choice(state_def, effective_input)
# This bypasses ResultSelector, ResultPath, and OutputPath processing.
# Per the ASL spec, Choice states support InputPath and OutputPath.
# ---------------------------------------------------------------------------


class TestChoiceStateOutputPathBug:
    def test_choice_state_applies_output_path(self):
        """Choice state should apply OutputPath before transitioning."""
        executor = _make_executor(
            {
                "Start": {
                    "Type": "Choice",
                    "InputPath": "$.data",
                    "OutputPath": "$.value",
                    "Choices": [
                        {"Variable": "$.flag", "BooleanEquals": True, "Next": "Done"},
                    ],
                    "Default": "Done",
                },
                "Done": {"Type": "Pass", "End": True},
            }
        )
        result = executor.execute({"data": {"flag": True, "value": "result"}, "extra": "ignored"})
        assert result == "result"


# ---------------------------------------------------------------------------
# Bug 5: _create_state_machine silently overwrites duplicates
#
# AWS returns StateMachineAlreadyExists when creating a state machine with
# a name that already exists. The provider silently overwrites instead.
# ---------------------------------------------------------------------------


class TestCreateDuplicateStateMachine:
    def setup_method(self):
        _clear_state()

    def teardown_method(self):
        _clear_state()

    def test_create_duplicate_raises_error(self):
        """Creating a state machine with a duplicate name should raise an error."""
        definition = json.dumps(
            {"StartAt": "Pass", "States": {"Pass": {"Type": "Pass", "End": True}}}
        )
        _create_state_machine(
            {"name": "sm1", "definition": definition, "roleArn": "r"},
            "us-east-1",
            "123",
        )
        with pytest.raises(SfnError) as exc_info:
            _create_state_machine(
                {"name": "sm1", "definition": definition, "roleArn": "r"},
                "us-east-1",
                "123",
            )
        assert exc_info.value.code == "StateMachineAlreadyExists"


# ---------------------------------------------------------------------------
# Bug 6: Succeed state skips OutputPath processing
#
# In _execute_state, the Succeed branch (line 188) returns immediately:
#   return effective_input, None
# This bypasses OutputPath processing at the bottom of _execute_state.
# Per the ASL spec, Succeed states support InputPath and OutputPath.
# ---------------------------------------------------------------------------


class TestSucceedStateOutputPathBug:
    def test_succeed_state_applies_output_path(self):
        """Succeed state should respect OutputPath."""
        executor = _make_executor(
            {
                "Start": {
                    "Type": "Succeed",
                    "OutputPath": "$.result",
                },
            }
        )
        result = executor.execute({"result": "hello", "extra": "ignored"})
        assert result == "hello"
