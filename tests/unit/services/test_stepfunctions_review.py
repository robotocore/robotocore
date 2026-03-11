"""Failing tests for StepFunctions edge cases discovered during code review.

Each test documents correct AWS behavior that the current implementation
does NOT handle properly. All tests are expected to FAIL.
"""

import json
import time

import pytest

from robotocore.services.stepfunctions.asl import (
    ASLExecutionError,
    ASLExecutor,
    _evaluate_choice_rule,
    _resolve_path,
    _task_tokens,
    _token_lock,
)
from robotocore.services.stepfunctions.provider import (
    SfnError,
    _abort_events,
    _create_state_machine,
    _execution_histories,
    _executions,
    _running_threads,
    _start_execution,
    _state_machines,
)


def _clear_state():
    _state_machines.clear()
    _executions.clear()
    _execution_histories.clear()
    _running_threads.clear()
    _abort_events.clear()
    with _token_lock:
        _task_tokens.clear()


def _make_executor(states: dict, start_at: str = "Start", execution_arn: str = "") -> ASLExecutor:
    return ASLExecutor({"StartAt": start_at, "States": states}, execution_arn=execution_arn)


def _wait_for_execution(exec_arn: str, timeout: float = 5.0):
    thread = _running_threads.get(exec_arn)
    if thread:
        thread.join(timeout=timeout)


# ===========================================================================
# Choice state: complex conditions (And/Or/Not, nested)
# ===========================================================================


class TestChoiceComplexConditions:
    def test_choice_state_with_timestamp_comparison(self):
        """Correct behavior: TimestampGreaterThan should compare ISO 8601 timestamps.
        AWS supports these comparison operators but the implementation doesn't handle them."""
        rule = {
            "Variable": "$.timestamp",
            "TimestampGreaterThan": "2024-01-01T00:00:00Z",
            "Next": "After",
        }
        data = {"timestamp": "2025-06-15T12:00:00Z"}
        # 2025 > 2024, should be True
        assert _evaluate_choice_rule(rule, data) is True

    def test_choice_numeric_equals_path(self):
        """Correct behavior: NumericEqualsPath should compare the variable value
        against a number resolved from a JSONPath. The implementation has
        StringEqualsPath but not NumericEqualsPath."""
        rule = {
            "Variable": "$.count",
            "NumericEqualsPath": "$.threshold",
            "Next": "Match",
        }
        data = {"count": 42, "threshold": 42}
        assert _evaluate_choice_rule(rule, data) is True

    def test_choice_is_timestamp(self):
        """Correct behavior: IsTimestamp should check if the value is a valid
        ISO 8601 timestamp string. Not implemented."""
        rule = {
            "Variable": "$.val",
            "IsTimestamp": True,
            "Next": "Match",
        }
        data = {"val": "2024-01-15T10:30:00Z"}
        assert _evaluate_choice_rule(rule, data) is True

    def test_choice_string_less_than_equals(self):
        """Correct behavior: StringLessThanEquals should be supported.
        Only StringLessThan and StringGreaterThan exist, not the Equals variants."""
        rule = {
            "Variable": "$.name",
            "StringLessThanEquals": "Charlie",
            "Next": "Match",
        }
        data = {"name": "Charlie"}
        assert _evaluate_choice_rule(rule, data) is True

    def test_choice_string_greater_than_equals(self):
        """Correct behavior: StringGreaterThanEquals should be supported."""
        rule = {
            "Variable": "$.name",
            "StringGreaterThanEquals": "Alice",
            "Next": "Match",
        }
        data = {"name": "Alice"}
        assert _evaluate_choice_rule(rule, data) is True


# ===========================================================================
# Parallel state: branches should run concurrently
# ===========================================================================


class TestParallelBranchConcurrency:
    def test_parallel_branches_run_concurrently(self):
        """Correct behavior: Parallel branches should run concurrently, not
        sequentially. The current implementation runs them in a serial for loop."""
        definition = {
            "StartAt": "Parallel",
            "States": {
                "Parallel": {
                    "Type": "Parallel",
                    "Branches": [
                        {
                            "StartAt": "Wait1",
                            "States": {
                                "Wait1": {
                                    "Type": "Wait",
                                    "Seconds": 1,
                                    "Next": "Result1",
                                },
                                "Result1": {
                                    "Type": "Pass",
                                    "Result": "branch1",
                                    "End": True,
                                },
                            },
                        },
                        {
                            "StartAt": "Wait2",
                            "States": {
                                "Wait2": {
                                    "Type": "Wait",
                                    "Seconds": 1,
                                    "Next": "Result2",
                                },
                                "Result2": {
                                    "Type": "Pass",
                                    "Result": "branch2",
                                    "End": True,
                                },
                            },
                        },
                    ],
                    "End": True,
                },
            },
        }
        executor = ASLExecutor(definition)
        start = time.time()
        result = executor.execute({})
        elapsed = time.time() - start
        assert result == ["branch1", "branch2"]
        # If branches run concurrently, elapsed should be ~1s, not ~2s
        assert elapsed < 1.5, f"Parallel branches took {elapsed:.1f}s, suggesting serial execution"


# ===========================================================================
# Wait state edge cases
# ===========================================================================


class TestWaitStateEdgeCases:
    def test_wait_seconds_path_missing_field_should_error(self):
        """Correct behavior: SecondsPath pointing to a non-existent field should
        raise a runtime error, not silently skip the wait."""
        states = {
            "Start": {
                "Type": "Wait",
                "SecondsPath": "$.nonexistent",
                "Next": "Done",
            },
            "Done": {"Type": "Pass", "Result": "done", "End": True},
        }
        executor = _make_executor(states)
        with pytest.raises(ASLExecutionError):
            executor.execute({})


# ===========================================================================
# Error handling: Retry support
# ===========================================================================


class TestRetryBehavior:
    def test_retry_with_max_attempts(self):
        """Correct behavior: Retry should re-execute the state up to MaxAttempts times.
        The current implementation has no Retry support at all -- only Catch."""
        states = {
            "Start": {
                "Type": "Task",
                "Resource": "arn:aws:states:::DOES_NOT_EXIST",
                "Retry": [
                    {
                        "ErrorEquals": ["States.ALL"],
                        "IntervalSeconds": 0,
                        "MaxAttempts": 3,
                        "BackoffRate": 1.0,
                    }
                ],
                "Catch": [{"ErrorEquals": ["States.ALL"], "Next": "Failed"}],
                "End": True,
            },
            "Failed": {"Type": "Pass", "Result": "retries-exhausted", "End": True},
        }
        executor = _make_executor(states)
        # After 3 retries fail, should fall through to Catch -> "Failed" state
        result = executor.execute({})
        assert result == "retries-exhausted"

    def test_retry_with_specific_error_code(self):
        """Correct behavior: Retry should only match specific error codes listed
        in ErrorEquals, not all errors."""
        states = {
            "Start": {
                "Type": "Task",
                "Resource": "arn:aws:states:::DOES_NOT_EXIST",
                "Retry": [
                    {
                        "ErrorEquals": ["Lambda.ServiceException"],
                        "MaxAttempts": 2,
                    }
                ],
                "Catch": [{"ErrorEquals": ["States.ALL"], "Next": "Caught"}],
                "End": True,
            },
            "Caught": {"Type": "Pass", "Result": "caught-non-retryable", "End": True},
        }
        executor = _make_executor(states)
        # The error won't match the Retry ErrorEquals, so should go straight to Catch
        result = executor.execute({})
        assert result == "caught-non-retryable"


# ===========================================================================
# Execution history completeness
# ===========================================================================


class TestExecutionHistoryCompleteness:
    def test_parallel_branches_appear_in_history(self):
        """Correct behavior: Events from parallel branches should appear in
        the parent execution's history. Currently, parallel branches create
        their own ASLExecutor without an execution_arn, so their events are lost."""
        definition = {
            "StartAt": "Par",
            "States": {
                "Par": {
                    "Type": "Parallel",
                    "Branches": [
                        {
                            "StartAt": "B1",
                            "States": {"B1": {"Type": "Pass", "Result": 1, "End": True}},
                        },
                        {
                            "StartAt": "B2",
                            "States": {"B2": {"Type": "Pass", "Result": 2, "End": True}},
                        },
                    ],
                    "End": True,
                },
            },
        }
        exec_arn = "arn:aws:states:us-east-1:123456789012:execution:test:par-history"
        executor = ASLExecutor(definition, execution_arn=exec_arn)
        executor.execute({})

        events = executor.history.get_events()
        event_types = [e["type"] for e in events]
        # AWS includes branch state events in the parent history
        branch_state_events = [
            t for t in event_types if "PassStateEntered" in t or "PassStateExited" in t
        ]
        # There should be at least 2 entered + 2 exited from the two branches
        assert len(branch_state_events) >= 4, f"Branch events missing from history: {event_types}"

    def test_map_iterations_appear_in_history(self):
        """Correct behavior: Map state iterations should appear in history.
        Like Parallel, Map creates child executors without history tracking."""
        definition = {
            "StartAt": "MapState",
            "States": {
                "MapState": {
                    "Type": "Map",
                    "ItemsPath": "$.items",
                    "Iterator": {
                        "StartAt": "Process",
                        "States": {
                            "Process": {"Type": "Pass", "End": True},
                        },
                    },
                    "End": True,
                },
            },
        }
        exec_arn = "arn:aws:states:us-east-1:123456789012:execution:test:map-history"
        executor = ASLExecutor(definition, execution_arn=exec_arn)
        executor.execute({"items": [1, 2, 3]})

        events = executor.history.get_events()
        event_types = [e["type"] for e in events]
        # Each iteration's Pass state should appear in history
        pass_entered_count = sum(1 for t in event_types if t == "PassStateEntered")
        assert pass_entered_count >= 3, f"Expected 3 iteration events, got {pass_entered_count}"


# ===========================================================================
# Input/Output processing edge cases
# ===========================================================================


class TestInputOutputProcessing:
    def test_input_path_null_discards_input(self):
        """Correct behavior: InputPath: null should pass an empty object {} to the
        state, discarding the entire input. With ResultPath on that empty input,
        the result should be placed in the empty object."""
        states = {
            "Start": {
                "Type": "Pass",
                "InputPath": None,
                "Result": "hello",
                "ResultPath": "$.greeting",
                "End": True,
            },
        }
        executor = _make_executor(states)
        result = executor.execute({"existing": "data"})
        # With InputPath: null, effective input is {}. Result "hello" goes to $.greeting.
        # But ResultPath is applied to the ORIGINAL input, not effective input.
        # So output should be {"existing": "data", "greeting": "hello"}
        # OR if applied to effective input: {"greeting": "hello"}
        # The key bug: the current code applies ResultPath to {} but the original
        # data contains "existing" which gets lost.
        assert result == {"greeting": "hello"}

    def test_parameters_with_context_object(self):
        """Correct behavior: Parameters can reference the Context Object via
        $$ prefix (e.g., $$.Execution.Id). The implementation only handles
        $ (input) references, not $$ (context) references."""
        states = {
            "Start": {
                "Type": "Pass",
                "Parameters": {
                    "execId.$": "$$.Execution.Id",
                    "input.$": "$.data",
                },
                "End": True,
            },
        }
        exec_arn = "arn:aws:states:us-east-1:123456789012:execution:test:ctx-test"
        executor = ASLExecutor({"StartAt": "Start", "States": states}, execution_arn=exec_arn)
        result = executor.execute({"data": "hello"})
        # Should have the execution ARN from context and "hello" from input
        assert result["execId"] == exec_arn
        assert result["input"] == "hello"


# ===========================================================================
# Express vs Standard: duplicate execution names
# ===========================================================================


class TestDuplicateExecutionName:
    def setup_method(self):
        _clear_state()

    def test_duplicate_execution_name_rejected_for_standard(self):
        """Correct behavior: Starting two executions with the same name on a STANDARD
        state machine should raise ExecutionAlreadyExists. The current implementation
        overwrites the first execution."""
        definition = json.dumps(
            {
                "StartAt": "Wait",
                "States": {"Wait": {"Type": "Wait", "Seconds": 10, "End": True}},
            }
        )
        _create_state_machine(
            {
                "name": "dup-test",
                "definition": definition,
                "roleArn": "arn:aws:iam::role/test",
                "type": "STANDARD",
            },
            "us-east-1",
            "123456789012",
        )
        sm_arn = "arn:aws:states:us-east-1:123456789012:stateMachine:dup-test"
        _start_execution(
            {"stateMachineArn": sm_arn, "name": "same-name"}, "us-east-1", "123456789012"
        )
        time.sleep(0.1)
        # Second execution with same name should fail
        with pytest.raises(SfnError) as exc_info:
            _start_execution(
                {"stateMachineArn": sm_arn, "name": "same-name"}, "us-east-1", "123456789012"
            )
        assert "AlreadyExists" in exc_info.value.code or "already" in exc_info.value.message.lower()


# ===========================================================================
# Map state: MaxConcurrency order preservation
# ===========================================================================


class TestMapStateConcurrency:
    def test_map_state_with_max_concurrency_preserves_order(self):
        """Correct behavior: Map with MaxConcurrency should still return results
        in the original array order. The current implementation has a bug where
        item 0 (an integer) is passed to ASLExecutor.execute() which wraps non-dict
        input as {}, losing the value."""
        states = {
            "Start": {
                "Type": "Map",
                "ItemsPath": "$.items",
                "MaxConcurrency": 2,
                "Iterator": {
                    "StartAt": "Process",
                    "States": {"Process": {"Type": "Pass", "End": True}},
                },
                "End": True,
            },
        }
        executor = _make_executor(states)
        items = list(range(10))
        result = executor.execute({"items": items})
        assert result == items


# ===========================================================================
# Task state with heartbeat timeouts
# ===========================================================================


class TestHeartbeatTimeout:
    def test_heartbeat_timeout_exceeded_raises_error(self):
        """Correct behavior: If HeartbeatSeconds is specified and no heartbeat is
        received within that time, the task should fail with States.HeartbeatTimeout.
        The implementation uses TimeoutSeconds for the callback wait but ignores
        HeartbeatSeconds entirely."""
        states = {
            "Start": {
                "Type": "Task",
                "Resource": "arn:aws:states:::sqs:sendMessage.waitForTaskCallback",
                "HeartbeatSeconds": 1,
                "TimeoutSeconds": 60,
                "End": True,
            },
        }
        executor = _make_executor(states)
        with pytest.raises(ASLExecutionError) as exc_info:
            executor.execute({"QueueUrl": "http://localhost:4566/queue/test", "MessageBody": "hi"})
        assert exc_info.value.error == "States.HeartbeatTimeout"


# ===========================================================================
# Fail state with dynamic error/cause from input
# ===========================================================================


class TestFailStateDynamic:
    def test_fail_state_with_error_path(self):
        """Correct behavior: Fail state supports ErrorPath and CausePath to
        dynamically resolve Error and Cause from the input. The current
        implementation only uses static Error/Cause strings."""
        states = {
            "Start": {
                "Type": "Fail",
                "ErrorPath": "$.errorCode",
                "CausePath": "$.errorMessage",
            },
        }
        executor = _make_executor(states)
        with pytest.raises(ASLExecutionError) as exc_info:
            executor.execute({"errorCode": "DynamicError", "errorMessage": "Resolved from input"})
        assert exc_info.value.error == "DynamicError"
        assert exc_info.value.cause == "Resolved from input"


# ===========================================================================
# JSONPath edge cases
# ===========================================================================


class TestJSONPathEdgeCases:
    def test_resolve_path_array_wildcard(self):
        """Correct behavior: $.items[*].id should resolve to a list of all id values
        from items array. The implementation doesn't support [*] wildcards."""
        data = {"items": [{"id": 1}, {"id": 2}, {"id": 3}]}
        result = _resolve_path(data, "$.items[*].id")
        assert result == [1, 2, 3]


# ===========================================================================
# Succeed state edge cases
# ===========================================================================


class TestSucceedState:
    def test_succeed_state_applies_input_output_filters(self):
        """Correct behavior: Succeed state should support InputPath and OutputPath
        to filter data before returning. The implementation returns effective_input
        directly without applying OutputPath."""
        states = {
            "Start": {
                "Type": "Succeed",
                "InputPath": "$.result",
                "OutputPath": "$.value",
            },
        }
        executor = _make_executor(states)
        result = executor.execute({"result": {"value": 42, "extra": "ignored"}})
        assert result == 42


# ===========================================================================
# Create state machine: duplicate name validation
# ===========================================================================


class TestCreateStateMachineValidation:
    def setup_method(self):
        _clear_state()

    def test_create_duplicate_state_machine_name_raises_error(self):
        """Correct behavior: Creating a state machine with a name that already
        exists should raise StateMachineAlreadyExists. The current implementation
        silently overwrites."""
        definition = json.dumps({"StartAt": "P", "States": {"P": {"Type": "Pass", "End": True}}})
        _create_state_machine(
            {"name": "my-sm", "definition": definition, "roleArn": "arn:aws:iam::role/test"},
            "us-east-1",
            "123456789012",
        )
        with pytest.raises(SfnError) as exc_info:
            _create_state_machine(
                {"name": "my-sm", "definition": definition, "roleArn": "arn:aws:iam::role/test"},
                "us-east-1",
                "123456789012",
            )
        assert "AlreadyExists" in exc_info.value.code
