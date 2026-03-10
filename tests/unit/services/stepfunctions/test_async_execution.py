"""Tests for Step Functions async execution.

Verifies that StartExecution returns immediately with RUNNING status,
background execution completes, StopExecution aborts, task token callbacks
work, and StartSyncExecution remains synchronous.
"""

import json
import threading
import time

import pytest

from robotocore.services.stepfunctions.provider import (
    SfnError,
    _abort_events,
    _exec_lock,
    _execution_histories,
    _executions,
    _running_threads,
    _state_machines,
    _tags,
    _versions,
)

REGION = "us-east-1"
ACCOUNT_ID = "123456789012"

# Simple pass-through state machine
PASS_DEFINITION = json.dumps(
    {
        "StartAt": "PassState",
        "States": {
            "PassState": {
                "Type": "Pass",
                "Result": {"message": "hello"},
                "End": True,
            }
        },
    }
)

# State machine that waits (simulates a long-running execution)
WAIT_DEFINITION = json.dumps(
    {
        "StartAt": "WaitState",
        "States": {
            "WaitState": {
                "Type": "Wait",
                "Seconds": 2,
                "Next": "Done",
            },
            "Done": {
                "Type": "Pass",
                "Result": {"waited": True},
                "End": True,
            },
        },
    }
)

# State machine that always fails
FAIL_DEFINITION = json.dumps(
    {
        "StartAt": "FailState",
        "States": {
            "FailState": {
                "Type": "Fail",
                "Error": "CustomError",
                "Cause": "Something went wrong",
            }
        },
    }
)

# Express workflow definition
EXPRESS_DEFINITION = json.dumps(
    {
        "StartAt": "PassState",
        "States": {
            "PassState": {
                "Type": "Pass",
                "Result": {"express": True},
                "End": True,
            }
        },
    }
)

# Callback pattern definition
CALLBACK_DEFINITION = json.dumps(
    {
        "StartAt": "CallbackTask",
        "States": {
            "CallbackTask": {
                "Type": "Task",
                "Resource": "arn:aws:lambda:us-east-1:123456789012"
                ":function:noop.waitForTaskCallback",
                "TimeoutSeconds": 5,
                "End": True,
            }
        },
    }
)


def _clear_state():
    """Clear all in-memory state between tests."""
    with _exec_lock:
        _executions.clear()
        _state_machines.clear()
        _execution_histories.clear()
        _tags.clear()
        _versions.clear()
        _running_threads.clear()
        _abort_events.clear()


def _create_sm(name: str, definition: str, sm_type: str = "STANDARD") -> dict:
    from robotocore.services.stepfunctions.provider import _create_state_machine

    return _create_state_machine(
        {
            "name": name,
            "definition": definition,
            "roleArn": "arn:aws:iam::role/test",
            "type": sm_type,
        },
        REGION,
        ACCOUNT_ID,
    )


def _start_exec(sm_arn: str, input_str: str = "{}", name: str | None = None) -> dict:
    from robotocore.services.stepfunctions.provider import _start_execution

    params: dict = {"stateMachineArn": sm_arn, "input": input_str}
    if name:
        params["name"] = name
    return _start_execution(params, REGION, ACCOUNT_ID)


def _start_sync_exec(sm_arn: str, input_str: str = "{}") -> dict:
    from robotocore.services.stepfunctions.provider import _start_sync_execution

    return _start_sync_execution(
        {"stateMachineArn": sm_arn, "input": input_str}, REGION, ACCOUNT_ID
    )


def _describe_exec(exec_arn: str) -> dict:
    from robotocore.services.stepfunctions.provider import _describe_execution

    return _describe_execution({"executionArn": exec_arn}, REGION, ACCOUNT_ID)


def _stop_exec(exec_arn: str, error: str = "", cause: str = "") -> dict:
    from robotocore.services.stepfunctions.provider import _stop_execution

    params: dict = {"executionArn": exec_arn}
    if error:
        params["error"] = error
    if cause:
        params["cause"] = cause
    return _stop_execution(params, REGION, ACCOUNT_ID)


def _wait_for_completion(exec_arn: str, timeout: float = 5.0) -> str:
    """Poll DescribeExecution until status is no longer RUNNING."""
    deadline = time.time() + timeout
    while time.time() < deadline:
        result = _describe_exec(exec_arn)
        if result["status"] != "RUNNING":
            return result["status"]
        time.sleep(0.05)
    return "RUNNING"  # timed out


@pytest.fixture(autouse=True)
def clean_state():
    _clear_state()
    yield
    _clear_state()


class TestAsyncExecution:
    """StartExecution returns immediately, background thread completes execution."""

    def test_start_execution_returns_running(self):
        """StartExecution should return immediately; DescribeExecution shows RUNNING initially."""
        sm = _create_sm("pass-machine", PASS_DEFINITION)
        result = _start_exec(sm["stateMachineArn"])

        assert "executionArn" in result
        assert "startDate" in result

        # The execution may already be done (pass is fast), but the initial store
        # was set to RUNNING before the thread ran
        exec_arn = result["executionArn"]
        desc = _describe_exec(exec_arn)
        assert desc["status"] in ("RUNNING", "SUCCEEDED")

    def test_execution_completes_as_succeeded(self):
        """A simple Pass state machine should eventually reach SUCCEEDED."""
        sm = _create_sm("pass-machine", PASS_DEFINITION)
        result = _start_exec(sm["stateMachineArn"])
        exec_arn = result["executionArn"]

        status = _wait_for_completion(exec_arn)
        assert status == "SUCCEEDED"

        desc = _describe_exec(exec_arn)
        assert desc["status"] == "SUCCEEDED"
        assert desc["output"] is not None
        output = json.loads(desc["output"])
        assert output == {"message": "hello"}
        assert desc["stopDate"] is not None

    def test_execution_fails_properly(self):
        """A Fail state should result in FAILED status."""
        sm = _create_sm("fail-machine", FAIL_DEFINITION)
        result = _start_exec(sm["stateMachineArn"])
        exec_arn = result["executionArn"]

        status = _wait_for_completion(exec_arn)
        assert status == "FAILED"

        desc = _describe_exec(exec_arn)
        assert desc["error"] == "CustomError"
        assert desc["cause"] == "Something went wrong"
        assert desc["stopDate"] is not None

    def test_start_execution_nonexistent_machine(self):
        """StartExecution on nonexistent state machine raises error."""
        with pytest.raises(SfnError, match="StateMachineDoesNotExist"):
            _start_exec("arn:aws:states:us-east-1:123456789012:stateMachine:nope")


class TestStopExecution:
    """StopExecution sets status to ABORTED."""

    def test_stop_running_execution(self):
        """StopExecution should mark execution as ABORTED."""
        # Use wait definition so execution takes a moment
        sm = _create_sm("wait-machine", WAIT_DEFINITION)
        result = _start_exec(sm["stateMachineArn"])
        exec_arn = result["executionArn"]

        # Stop it immediately
        stop_result = _stop_exec(exec_arn, error="UserAbort", cause="Test abort")
        assert "stopDate" in stop_result

        desc = _describe_exec(exec_arn)
        assert desc["status"] == "ABORTED"

    def test_stop_nonexistent_execution(self):
        """StopExecution on nonexistent execution raises error."""
        with pytest.raises(SfnError, match="ExecutionDoesNotExist"):
            _stop_exec("arn:aws:states:us-east-1:123456789012:execution:nope:nope")

    def test_stop_preserves_error_cause(self):
        """StopExecution should record error and cause."""
        sm = _create_sm("wait-machine", WAIT_DEFINITION)
        result = _start_exec(sm["stateMachineArn"])
        exec_arn = result["executionArn"]

        _stop_exec(exec_arn, error="ManualAbort", cause="Operator cancelled")

        desc = _describe_exec(exec_arn)
        assert desc["status"] == "ABORTED"
        assert desc["error"] == "ManualAbort"
        assert desc["cause"] == "Operator cancelled"


class TestStartSyncExecution:
    """StartSyncExecution remains synchronous for EXPRESS workflows."""

    def test_sync_execution_returns_result(self):
        """StartSyncExecution should return full result synchronously."""
        sm = _create_sm("express-machine", EXPRESS_DEFINITION, sm_type="EXPRESS")
        result = _start_sync_exec(sm["stateMachineArn"])

        assert result["status"] == "SUCCEEDED"
        assert result["output"] is not None
        output = json.loads(result["output"])
        assert output == {"express": True}
        assert "startDate" in result
        assert "stopDate" in result

    def test_sync_execution_rejects_standard(self):
        """StartSyncExecution should reject STANDARD workflows."""
        sm = _create_sm("standard-machine", PASS_DEFINITION, sm_type="STANDARD")
        with pytest.raises(SfnError, match="InvalidArn"):
            _start_sync_exec(sm["stateMachineArn"])


class TestTaskTokenCallbacks:
    """SendTaskSuccess and SendTaskFailure signal waiting executions."""

    def test_send_task_success(self):
        """SendTaskSuccess completes a waiting callback task."""
        from robotocore.services.stepfunctions.asl import _task_tokens, _token_lock

        # Create a token manually and signal it
        token = "test-token-success"
        event = threading.Event()
        token_info = {
            "event": event,
            "result": None,
            "status": None,
            "error": None,
            "cause": None,
            "last_heartbeat": time.time(),
        }
        with _token_lock:
            _task_tokens[token] = token_info

        from robotocore.services.stepfunctions.provider import _send_task_success_op

        result = _send_task_success_op(
            {"taskToken": token, "output": '{"done": true}'}, REGION, ACCOUNT_ID
        )
        assert result == {}
        assert token_info["status"] == "SUCCESS"
        assert token_info["result"] == {"done": True}
        assert event.is_set()

    def test_send_task_failure(self):
        """SendTaskFailure fails a waiting callback task."""
        from robotocore.services.stepfunctions.asl import _task_tokens, _token_lock

        token = "test-token-failure"
        event = threading.Event()
        token_info = {
            "event": event,
            "result": None,
            "status": None,
            "error": None,
            "cause": None,
            "last_heartbeat": time.time(),
        }
        with _token_lock:
            _task_tokens[token] = token_info

        from robotocore.services.stepfunctions.provider import _send_task_failure_op

        result = _send_task_failure_op(
            {"taskToken": token, "error": "Oops", "cause": "Bad input"}, REGION, ACCOUNT_ID
        )
        assert result == {}
        assert token_info["status"] == "FAILED"
        assert token_info["error"] == "Oops"
        assert event.is_set()

    def test_send_task_success_unknown_token(self):
        """SendTaskSuccess with unknown token raises TaskDoesNotExist."""
        from robotocore.services.stepfunctions.provider import _send_task_success_op

        with pytest.raises(SfnError, match="TaskDoesNotExist"):
            _send_task_success_op({"taskToken": "nonexistent", "output": "{}"}, REGION, ACCOUNT_ID)

    def test_send_task_failure_unknown_token(self):
        """SendTaskFailure with unknown token raises TaskDoesNotExist."""
        from robotocore.services.stepfunctions.provider import _send_task_failure_op

        with pytest.raises(SfnError, match="TaskDoesNotExist"):
            _send_task_failure_op(
                {"taskToken": "nonexistent", "error": "x", "cause": "y"}, REGION, ACCOUNT_ID
            )


class TestConcurrentExecutions:
    """Multiple executions can run concurrently."""

    def test_multiple_concurrent_executions(self):
        """Launch multiple executions and verify they all complete."""
        sm = _create_sm("pass-machine", PASS_DEFINITION)
        sm_arn = sm["stateMachineArn"]

        exec_arns = []
        for i in range(5):
            result = _start_exec(sm_arn, name=f"exec-{i}")
            exec_arns.append(result["executionArn"])

        # Wait for all to complete
        for arn in exec_arns:
            status = _wait_for_completion(arn)
            assert status == "SUCCEEDED", f"Execution {arn} ended with {status}"

    def test_list_executions_shows_all(self):
        """ListExecutions returns all executions for a state machine."""
        from robotocore.services.stepfunctions.provider import _list_executions

        sm = _create_sm("pass-machine", PASS_DEFINITION)
        sm_arn = sm["stateMachineArn"]

        for i in range(3):
            _start_exec(sm_arn, name=f"exec-{i}")

        # Wait for completion
        time.sleep(1)

        result = _list_executions({"stateMachineArn": sm_arn}, REGION, ACCOUNT_ID)
        assert len(result["executions"]) == 3

    def test_list_executions_status_filter(self):
        """ListExecutions with statusFilter returns matching executions."""
        from robotocore.services.stepfunctions.provider import _list_executions

        sm = _create_sm("fail-machine", FAIL_DEFINITION)
        sm_arn = sm["stateMachineArn"]

        _start_exec(sm_arn, name="fail-1")
        _wait_for_completion(f"{sm_arn.replace(':stateMachine:', ':execution:')}:fail-1")

        result = _list_executions(
            {"stateMachineArn": sm_arn, "statusFilter": "FAILED"}, REGION, ACCOUNT_ID
        )
        assert len(result["executions"]) >= 1
        assert all(e["status"] == "FAILED" for e in result["executions"])


class TestExecutionHistory:
    """GetExecutionHistory returns events for completed executions."""

    def test_history_after_completion(self):
        """Execution history should have events after completion."""
        from robotocore.services.stepfunctions.provider import _get_execution_history

        sm = _create_sm("pass-machine", PASS_DEFINITION)
        result = _start_exec(sm["stateMachineArn"])
        exec_arn = result["executionArn"]

        _wait_for_completion(exec_arn)

        history = _get_execution_history({"executionArn": exec_arn}, REGION, ACCOUNT_ID)
        events = history["events"]
        assert len(events) > 0

        # Should have ExecutionStarted event
        event_types = [e["type"] for e in events]
        assert "ExecutionStarted" in event_types
