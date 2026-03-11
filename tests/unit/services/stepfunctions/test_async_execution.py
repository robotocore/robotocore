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

    def test_history_has_state_entered_and_exited(self):
        """Execution history includes state entered and exited events."""
        from robotocore.services.stepfunctions.provider import _get_execution_history

        sm = _create_sm("pass-machine", PASS_DEFINITION)
        result = _start_exec(sm["stateMachineArn"])
        exec_arn = result["executionArn"]
        _wait_for_completion(exec_arn)

        history = _get_execution_history({"executionArn": exec_arn}, REGION, ACCOUNT_ID)
        event_types = [e["type"] for e in history["events"]]
        assert "PassStateEntered" in event_types
        assert "PassStateExited" in event_types

    def test_history_has_execution_succeeded(self):
        """Successful execution history ends with ExecutionSucceeded."""
        from robotocore.services.stepfunctions.provider import _get_execution_history

        sm = _create_sm("pass-machine", PASS_DEFINITION)
        result = _start_exec(sm["stateMachineArn"])
        exec_arn = result["executionArn"]
        _wait_for_completion(exec_arn)

        history = _get_execution_history({"executionArn": exec_arn}, REGION, ACCOUNT_ID)
        event_types = [e["type"] for e in history["events"]]
        assert "ExecutionSucceeded" in event_types

    def test_history_has_execution_failed_on_failure(self):
        """Failed execution history includes ExecutionFailed."""
        from robotocore.services.stepfunctions.provider import _get_execution_history

        sm = _create_sm("fail-machine", FAIL_DEFINITION)
        result = _start_exec(sm["stateMachineArn"])
        exec_arn = result["executionArn"]
        _wait_for_completion(exec_arn)

        history = _get_execution_history({"executionArn": exec_arn}, REGION, ACCOUNT_ID)
        event_types = [e["type"] for e in history["events"]]
        assert "ExecutionFailed" in event_types

    def test_history_reverse_order(self):
        """GetExecutionHistory with reverseOrder returns events in reverse."""
        from robotocore.services.stepfunctions.provider import _get_execution_history

        sm = _create_sm("pass-machine", PASS_DEFINITION)
        result = _start_exec(sm["stateMachineArn"])
        exec_arn = result["executionArn"]
        _wait_for_completion(exec_arn)

        forward = _get_execution_history({"executionArn": exec_arn}, REGION, ACCOUNT_ID)
        reverse = _get_execution_history(
            {"executionArn": exec_arn, "reverseOrder": True}, REGION, ACCOUNT_ID
        )
        assert len(forward["events"]) == len(reverse["events"])
        assert forward["events"][0]["id"] == reverse["events"][-1]["id"]
        assert forward["events"][-1]["id"] == reverse["events"][0]["id"]

    def test_history_empty_for_unknown_execution(self):
        """GetExecutionHistory for unknown execution returns empty events."""
        from robotocore.services.stepfunctions.provider import _get_execution_history

        result = _get_execution_history(
            {"executionArn": "arn:aws:states:us-east-1:123456789012:execution:x:y"},
            REGION,
            ACCOUNT_ID,
        )
        assert result["events"] == []

    def test_history_event_ids_are_monotonic(self):
        """Event IDs in history should be strictly increasing."""
        from robotocore.services.stepfunctions.provider import _get_execution_history

        sm = _create_sm("pass-machine", PASS_DEFINITION)
        result = _start_exec(sm["stateMachineArn"])
        exec_arn = result["executionArn"]
        _wait_for_completion(exec_arn)

        history = _get_execution_history({"executionArn": exec_arn}, REGION, ACCOUNT_ID)
        ids = [e["id"] for e in history["events"]]
        assert ids == sorted(ids)
        assert len(ids) == len(set(ids)), "Event IDs must be unique"


class TestAsyncExecutionArn:
    """Execution ARN construction and naming."""

    def test_execution_arn_contains_state_machine_name(self):
        """Execution ARN should derive from state machine ARN."""
        sm = _create_sm("my-machine", PASS_DEFINITION)
        result = _start_exec(sm["stateMachineArn"])
        exec_arn = result["executionArn"]
        assert ":execution:my-machine:" in exec_arn

    def test_custom_execution_name_in_arn(self):
        """When a name is provided, it appears in the execution ARN."""
        sm = _create_sm("my-machine", PASS_DEFINITION)
        result = _start_exec(sm["stateMachineArn"], name="custom-run")
        assert result["executionArn"].endswith(":custom-run")

    def test_auto_generated_execution_name(self):
        """When no name is provided, a UUID is generated."""
        sm = _create_sm("my-machine", PASS_DEFINITION)
        result = _start_exec(sm["stateMachineArn"])
        # The ARN should end with a UUID-like string (36 chars)
        name_part = result["executionArn"].rsplit(":", 1)[-1]
        assert len(name_part) == 36  # UUID format


class TestAsyncExecutionInput:
    """Input data is correctly passed through and stored."""

    def test_input_preserved_in_describe(self):
        """DescribeExecution should return the original input."""
        sm = _create_sm("pass-machine", PASS_DEFINITION)
        input_data = json.dumps({"key": "value", "num": 42})
        result = _start_exec(sm["stateMachineArn"], input_str=input_data)
        exec_arn = result["executionArn"]
        _wait_for_completion(exec_arn)

        desc = _describe_exec(exec_arn)
        assert desc["input"] == input_data

    def test_default_empty_input(self):
        """Default input should be '{}'."""
        sm = _create_sm("pass-machine", PASS_DEFINITION)
        result = _start_exec(sm["stateMachineArn"])
        exec_arn = result["executionArn"]
        _wait_for_completion(exec_arn)

        desc = _describe_exec(exec_arn)
        assert desc["input"] == "{}"


class TestStopExecutionEdgeCases:
    """Edge cases for StopExecution."""

    def test_stop_without_error_or_cause(self):
        """StopExecution without error/cause still aborts."""
        sm = _create_sm("wait-machine", WAIT_DEFINITION)
        result = _start_exec(sm["stateMachineArn"])
        exec_arn = result["executionArn"]

        stop_result = _stop_exec(exec_arn)
        assert "stopDate" in stop_result

        desc = _describe_exec(exec_arn)
        assert desc["status"] == "ABORTED"

    def test_stop_sets_abort_event(self):
        """StopExecution should signal the abort event for the background thread."""
        sm = _create_sm("wait-machine", WAIT_DEFINITION)
        result = _start_exec(sm["stateMachineArn"])
        exec_arn = result["executionArn"]

        # The abort event should exist before stop
        with _exec_lock:
            abort_event = _abort_events.get(exec_arn)

        _stop_exec(exec_arn)

        # Abort event should be set (or already cleaned up by thread)
        if abort_event is not None:
            assert abort_event.is_set()

    def test_stop_already_completed_execution(self):
        """Stopping an already-completed execution still sets ABORTED."""
        sm = _create_sm("pass-machine", PASS_DEFINITION)
        result = _start_exec(sm["stateMachineArn"])
        exec_arn = result["executionArn"]
        _wait_for_completion(exec_arn)

        # Now stop the already-completed execution
        _stop_exec(exec_arn, error="LateAbort", cause="Too late")
        desc = _describe_exec(exec_arn)
        assert desc["status"] == "ABORTED"


class TestSyncExecutionEdgeCases:
    """Edge cases for StartSyncExecution."""

    def test_sync_execution_failed_state_machine(self):
        """StartSyncExecution with a Fail state returns FAILED synchronously."""
        sm = _create_sm("express-fail", FAIL_DEFINITION, sm_type="EXPRESS")
        result = _start_sync_exec(sm["stateMachineArn"])

        assert result["status"] == "FAILED"
        assert result["error"] == "CustomError"
        assert result["cause"] == "Something went wrong"

    def test_sync_execution_nonexistent_machine(self):
        """StartSyncExecution on nonexistent state machine raises error."""
        with pytest.raises(SfnError, match="StateMachineDoesNotExist"):
            _start_sync_exec("arn:aws:states:us-east-1:123456789012:stateMachine:ghost")

    def test_sync_execution_arn_uses_express_prefix(self):
        """StartSyncExecution ARN uses :express: instead of :execution:."""
        sm = _create_sm("express-machine", EXPRESS_DEFINITION, sm_type="EXPRESS")
        result = _start_sync_exec(sm["stateMachineArn"])
        assert ":express:" in result["executionArn"]

    def test_sync_execution_stores_in_executions(self):
        """StartSyncExecution stores the execution in _executions for later retrieval."""
        sm = _create_sm("express-machine", EXPRESS_DEFINITION, sm_type="EXPRESS")
        result = _start_sync_exec(sm["stateMachineArn"])
        exec_arn = result["executionArn"]

        desc = _describe_exec(exec_arn)
        assert desc["status"] == "SUCCEEDED"

    def test_sync_execution_has_input_in_response(self):
        """StartSyncExecution response includes the input."""
        sm = _create_sm("express-machine", EXPRESS_DEFINITION, sm_type="EXPRESS")
        input_data = json.dumps({"foo": "bar"})
        result = _start_sync_exec(sm["stateMachineArn"], input_str=input_data)
        assert result["input"] == input_data


class TestListExecutionsExpress:
    """EXPRESS executions should not appear in ListExecutions."""

    def test_express_executions_excluded_from_list(self):
        """ListExecutions should not include EXPRESS executions."""
        from robotocore.services.stepfunctions.provider import _list_executions

        sm = _create_sm("express-machine", EXPRESS_DEFINITION, sm_type="EXPRESS")
        sm_arn = sm["stateMachineArn"]

        _start_sync_exec(sm_arn)
        _start_sync_exec(sm_arn)

        result = _list_executions({"stateMachineArn": sm_arn}, REGION, ACCOUNT_ID)
        assert len(result["executions"]) == 0


class TestTaskTokenHeartbeat:
    """SendTaskHeartbeat updates the heartbeat timestamp."""

    def test_send_task_heartbeat(self):
        """SendTaskHeartbeat updates the last_heartbeat timestamp."""
        from robotocore.services.stepfunctions.asl import _task_tokens, _token_lock
        from robotocore.services.stepfunctions.provider import _send_task_heartbeat_op

        token = "test-token-heartbeat"
        event = threading.Event()
        original_time = time.time() - 100  # old heartbeat
        token_info = {
            "event": event,
            "result": None,
            "status": None,
            "error": None,
            "cause": None,
            "last_heartbeat": original_time,
        }
        with _token_lock:
            _task_tokens[token] = token_info

        result = _send_task_heartbeat_op({"taskToken": token}, REGION, ACCOUNT_ID)
        assert result == {}
        assert token_info["last_heartbeat"] > original_time

        # Clean up
        with _token_lock:
            _task_tokens.pop(token, None)

    def test_send_task_heartbeat_unknown_token(self):
        """SendTaskHeartbeat with unknown token raises TaskDoesNotExist."""
        from robotocore.services.stepfunctions.provider import _send_task_heartbeat_op

        with pytest.raises(SfnError, match="TaskDoesNotExist"):
            _send_task_heartbeat_op({"taskToken": "nonexistent"}, REGION, ACCOUNT_ID)


class TestStateMachineCRUD:
    """State machine create, describe, update, delete, list."""

    def test_create_and_describe(self):
        """CreateStateMachine + DescribeStateMachine round-trip."""
        from robotocore.services.stepfunctions.provider import _describe_state_machine

        sm = _create_sm("test-sm", PASS_DEFINITION)
        sm_arn = sm["stateMachineArn"]
        assert "stateMachineArn" in sm
        assert "creationDate" in sm

        desc = _describe_state_machine({"stateMachineArn": sm_arn}, REGION, ACCOUNT_ID)
        assert desc["name"] == "test-sm"
        assert desc["status"] == "ACTIVE"
        assert desc["type"] == "STANDARD"
        assert json.loads(desc["definition"])["StartAt"] == "PassState"

    def test_describe_nonexistent(self):
        """DescribeStateMachine for nonexistent raises error."""
        from robotocore.services.stepfunctions.provider import _describe_state_machine

        with pytest.raises(SfnError, match="StateMachineDoesNotExist"):
            _describe_state_machine(
                {"stateMachineArn": "arn:aws:states:us-east-1:123456789012:stateMachine:nope"},
                REGION,
                ACCOUNT_ID,
            )

    def test_update_state_machine_definition(self):
        """UpdateStateMachine changes the definition."""
        from robotocore.services.stepfunctions.provider import (
            _describe_state_machine,
            _update_state_machine,
        )

        sm = _create_sm("updatable", PASS_DEFINITION)
        sm_arn = sm["stateMachineArn"]

        new_def = json.dumps({"StartAt": "New", "States": {"New": {"Type": "Succeed"}}})
        result = _update_state_machine(
            {"stateMachineArn": sm_arn, "definition": new_def}, REGION, ACCOUNT_ID
        )
        assert "updateDate" in result

        desc = _describe_state_machine({"stateMachineArn": sm_arn}, REGION, ACCOUNT_ID)
        assert json.loads(desc["definition"])["StartAt"] == "New"

    def test_update_state_machine_role(self):
        """UpdateStateMachine changes the roleArn."""
        from robotocore.services.stepfunctions.provider import (
            _describe_state_machine,
            _update_state_machine,
        )

        sm = _create_sm("updatable-role", PASS_DEFINITION)
        sm_arn = sm["stateMachineArn"]

        _update_state_machine(
            {"stateMachineArn": sm_arn, "roleArn": "arn:aws:iam::role/new-role"},
            REGION,
            ACCOUNT_ID,
        )
        desc = _describe_state_machine({"stateMachineArn": sm_arn}, REGION, ACCOUNT_ID)
        assert desc["roleArn"] == "arn:aws:iam::role/new-role"

    def test_update_nonexistent(self):
        """UpdateStateMachine for nonexistent raises error."""
        from robotocore.services.stepfunctions.provider import _update_state_machine

        with pytest.raises(SfnError, match="StateMachineDoesNotExist"):
            _update_state_machine(
                {
                    "stateMachineArn": "arn:aws:states:us-east-1:123456789012:stateMachine:nope",
                    "definition": "{}",
                },
                REGION,
                ACCOUNT_ID,
            )

    def test_delete_state_machine(self):
        """DeleteStateMachine removes the state machine."""
        from robotocore.services.stepfunctions.provider import (
            _delete_state_machine,
            _describe_state_machine,
        )

        sm = _create_sm("deletable", PASS_DEFINITION)
        sm_arn = sm["stateMachineArn"]

        result = _delete_state_machine({"stateMachineArn": sm_arn}, REGION, ACCOUNT_ID)
        assert result == {}

        with pytest.raises(SfnError, match="StateMachineDoesNotExist"):
            _describe_state_machine({"stateMachineArn": sm_arn}, REGION, ACCOUNT_ID)

    def test_delete_cascades_executions(self):
        """DeleteStateMachine also removes associated executions."""
        from robotocore.services.stepfunctions.provider import _delete_state_machine

        sm = _create_sm("cascade-del", PASS_DEFINITION)
        sm_arn = sm["stateMachineArn"]
        result = _start_exec(sm_arn, name="doomed")
        exec_arn = result["executionArn"]
        _wait_for_completion(exec_arn)

        _delete_state_machine({"stateMachineArn": sm_arn}, REGION, ACCOUNT_ID)

        with pytest.raises(SfnError, match="ExecutionDoesNotExist"):
            _describe_exec(exec_arn)

    def test_list_state_machines(self):
        """ListStateMachines returns all state machines."""
        from robotocore.services.stepfunctions.provider import _list_state_machines

        _create_sm("sm-a", PASS_DEFINITION)
        _create_sm("sm-b", FAIL_DEFINITION)

        result = _list_state_machines({}, REGION, ACCOUNT_ID)
        names = [sm["name"] for sm in result["stateMachines"]]
        assert "sm-a" in names
        assert "sm-b" in names


class TestDescribeStateMachineForExecution:
    """DescribeStateMachineForExecution returns the SM linked to an execution."""

    def test_describe_sm_for_execution(self):
        """DescribeStateMachineForExecution returns correct SM."""
        from robotocore.services.stepfunctions.provider import (
            _describe_state_machine_for_execution,
        )

        sm = _create_sm("for-exec", PASS_DEFINITION)
        sm_arn = sm["stateMachineArn"]
        result = _start_exec(sm_arn)
        exec_arn = result["executionArn"]
        _wait_for_completion(exec_arn)

        desc = _describe_state_machine_for_execution({"executionArn": exec_arn}, REGION, ACCOUNT_ID)
        assert desc["stateMachineArn"] == sm_arn
        assert desc["name"] == "for-exec"
        assert "definition" in desc

    def test_describe_sm_for_nonexistent_execution(self):
        """DescribeStateMachineForExecution with bad exec raises error."""
        from robotocore.services.stepfunctions.provider import (
            _describe_state_machine_for_execution,
        )

        with pytest.raises(SfnError, match="ExecutionDoesNotExist"):
            _describe_state_machine_for_execution(
                {"executionArn": "arn:aws:states:us-east-1:123456789012:execution:nope:nope"},
                REGION,
                ACCOUNT_ID,
            )


class TestTagging:
    """TagResource, UntagResource, ListTagsForResource."""

    def test_tag_and_list(self):
        """Tags can be added and listed."""
        from robotocore.services.stepfunctions.provider import (
            _list_tags_for_resource,
            _tag_resource,
        )

        sm = _create_sm("taggable", PASS_DEFINITION)
        sm_arn = sm["stateMachineArn"]

        _tag_resource(
            {"resourceArn": sm_arn, "tags": [{"key": "env", "value": "dev"}]},
            REGION,
            ACCOUNT_ID,
        )

        tags = _list_tags_for_resource({"resourceArn": sm_arn}, REGION, ACCOUNT_ID)
        assert len(tags["tags"]) == 1
        assert tags["tags"][0]["key"] == "env"
        assert tags["tags"][0]["value"] == "dev"

    def test_tag_overwrite(self):
        """Tagging with same key overwrites the value."""
        from robotocore.services.stepfunctions.provider import (
            _list_tags_for_resource,
            _tag_resource,
        )

        sm = _create_sm("tag-overwrite", PASS_DEFINITION)
        sm_arn = sm["stateMachineArn"]

        _tag_resource(
            {"resourceArn": sm_arn, "tags": [{"key": "env", "value": "dev"}]},
            REGION,
            ACCOUNT_ID,
        )
        _tag_resource(
            {"resourceArn": sm_arn, "tags": [{"key": "env", "value": "prod"}]},
            REGION,
            ACCOUNT_ID,
        )

        tags = _list_tags_for_resource({"resourceArn": sm_arn}, REGION, ACCOUNT_ID)
        assert len(tags["tags"]) == 1
        assert tags["tags"][0]["value"] == "prod"

    def test_untag(self):
        """UntagResource removes specified keys."""
        from robotocore.services.stepfunctions.provider import (
            _list_tags_for_resource,
            _tag_resource,
            _untag_resource,
        )

        sm = _create_sm("untaggable", PASS_DEFINITION)
        sm_arn = sm["stateMachineArn"]

        _tag_resource(
            {
                "resourceArn": sm_arn,
                "tags": [
                    {"key": "env", "value": "dev"},
                    {"key": "team", "value": "platform"},
                ],
            },
            REGION,
            ACCOUNT_ID,
        )
        _untag_resource({"resourceArn": sm_arn, "tagKeys": ["env"]}, REGION, ACCOUNT_ID)

        tags = _list_tags_for_resource({"resourceArn": sm_arn}, REGION, ACCOUNT_ID)
        assert len(tags["tags"]) == 1
        assert tags["tags"][0]["key"] == "team"

    def test_tag_nonexistent_resource(self):
        """TagResource on nonexistent resource raises error."""
        from robotocore.services.stepfunctions.provider import _tag_resource

        with pytest.raises(SfnError, match="ResourceNotFound"):
            _tag_resource(
                {
                    "resourceArn": "arn:aws:states:us-east-1:123456789012:stateMachine:nope",
                    "tags": [{"key": "x", "value": "y"}],
                },
                REGION,
                ACCOUNT_ID,
            )


class TestVersionManagement:
    """PublishStateMachineVersion, ListStateMachineVersions, DeleteStateMachineVersion."""

    def test_publish_version(self):
        """PublishStateMachineVersion creates a version."""
        from robotocore.services.stepfunctions.provider import (
            _publish_state_machine_version,
        )

        sm = _create_sm("versioned", PASS_DEFINITION)
        sm_arn = sm["stateMachineArn"]

        result = _publish_state_machine_version({"stateMachineArn": sm_arn}, REGION, ACCOUNT_ID)
        assert result["stateMachineVersionArn"] == f"{sm_arn}:1"
        assert "creationDate" in result

    def test_publish_increments_version(self):
        """Multiple publishes increment version numbers."""
        from robotocore.services.stepfunctions.provider import (
            _publish_state_machine_version,
        )

        sm = _create_sm("versioned", PASS_DEFINITION)
        sm_arn = sm["stateMachineArn"]

        v1 = _publish_state_machine_version({"stateMachineArn": sm_arn}, REGION, ACCOUNT_ID)
        v2 = _publish_state_machine_version({"stateMachineArn": sm_arn}, REGION, ACCOUNT_ID)
        assert v1["stateMachineVersionArn"].endswith(":1")
        assert v2["stateMachineVersionArn"].endswith(":2")

    def test_list_versions(self):
        """ListStateMachineVersions returns versions in reverse order."""
        from robotocore.services.stepfunctions.provider import (
            _list_state_machine_versions,
            _publish_state_machine_version,
        )

        sm = _create_sm("versioned", PASS_DEFINITION)
        sm_arn = sm["stateMachineArn"]

        _publish_state_machine_version({"stateMachineArn": sm_arn}, REGION, ACCOUNT_ID)
        _publish_state_machine_version({"stateMachineArn": sm_arn}, REGION, ACCOUNT_ID)

        result = _list_state_machine_versions({"stateMachineArn": sm_arn}, REGION, ACCOUNT_ID)
        versions = result["stateMachineVersions"]
        assert len(versions) == 2
        # Reverse order: newest first
        assert versions[0]["stateMachineVersionArn"].endswith(":2")
        assert versions[1]["stateMachineVersionArn"].endswith(":1")

    def test_delete_version(self):
        """DeleteStateMachineVersion removes the version."""
        from robotocore.services.stepfunctions.provider import (
            _delete_state_machine_version,
            _list_state_machine_versions,
            _publish_state_machine_version,
        )

        sm = _create_sm("versioned", PASS_DEFINITION)
        sm_arn = sm["stateMachineArn"]

        _publish_state_machine_version({"stateMachineArn": sm_arn}, REGION, ACCOUNT_ID)
        v2 = _publish_state_machine_version({"stateMachineArn": sm_arn}, REGION, ACCOUNT_ID)

        _delete_state_machine_version(
            {"stateMachineVersionArn": v2["stateMachineVersionArn"]}, REGION, ACCOUNT_ID
        )

        result = _list_state_machine_versions({"stateMachineArn": sm_arn}, REGION, ACCOUNT_ID)
        assert len(result["stateMachineVersions"]) == 1

    def test_delete_invalid_version_arn(self):
        """DeleteStateMachineVersion with bad ARN raises ValidationException."""
        from robotocore.services.stepfunctions.provider import (
            _delete_state_machine_version,
        )

        with pytest.raises(SfnError, match="ValidationException"):
            _delete_state_machine_version(
                {"stateMachineVersionArn": "arn:aws:states:us-east-1:123456789012:stateMachine:x"},
                REGION,
                ACCOUNT_ID,
            )

    def test_publish_nonexistent_machine(self):
        """PublishStateMachineVersion for nonexistent machine raises error."""
        from robotocore.services.stepfunctions.provider import (
            _publish_state_machine_version,
        )

        with pytest.raises(SfnError, match="StateMachineDoesNotExist"):
            _publish_state_machine_version(
                {"stateMachineArn": "arn:aws:states:us-east-1:123456789012:stateMachine:nope"},
                REGION,
                ACCOUNT_ID,
            )


class TestValidateDefinition:
    """ValidateStateMachineDefinition validates ASL JSON."""

    def test_valid_definition(self):
        """Valid JSON passes validation."""
        from robotocore.services.stepfunctions.provider import (
            _validate_state_machine_definition,
        )

        result = _validate_state_machine_definition(
            {"definition": PASS_DEFINITION}, REGION, ACCOUNT_ID
        )
        assert result["result"] == "OK"
        assert result["diagnostics"] == []
        assert result["truncated"] is False

    def test_invalid_json_definition(self):
        """Invalid JSON fails validation."""
        from robotocore.services.stepfunctions.provider import (
            _validate_state_machine_definition,
        )

        result = _validate_state_machine_definition(
            {"definition": "not valid json {"}, REGION, ACCOUNT_ID
        )
        assert result["result"] == "FAIL"
        assert len(result["diagnostics"]) == 1
        assert result["diagnostics"][0]["severity"] == "ERROR"


class TestBackgroundThreadCleanup:
    """Background threads are cleaned up after execution completes."""

    def test_thread_removed_after_success(self):
        """Running thread should be cleaned up after execution succeeds."""
        sm = _create_sm("pass-machine", PASS_DEFINITION)
        result = _start_exec(sm["stateMachineArn"])
        exec_arn = result["executionArn"]
        _wait_for_completion(exec_arn)

        # Give a moment for the finally block to run
        time.sleep(0.1)

        with _exec_lock:
            assert exec_arn not in _running_threads
            assert exec_arn not in _abort_events

    def test_thread_removed_after_failure(self):
        """Running thread should be cleaned up after execution fails."""
        sm = _create_sm("fail-machine", FAIL_DEFINITION)
        result = _start_exec(sm["stateMachineArn"])
        exec_arn = result["executionArn"]
        _wait_for_completion(exec_arn)

        time.sleep(0.1)

        with _exec_lock:
            assert exec_arn not in _running_threads
            assert exec_arn not in _abort_events


class TestStartExecutionInternal:
    """_start_execution_internal for nested step functions."""

    def test_internal_execution_succeeds(self):
        """Internal execution returns output on success."""
        from robotocore.services.stepfunctions.provider import _start_execution_internal

        sm = _create_sm("nested", PASS_DEFINITION)
        result = _start_execution_internal(
            sm["stateMachineArn"], {"input": "data"}, REGION, ACCOUNT_ID
        )
        assert "executionArn" in result
        assert result["output"] == {"message": "hello"}

    def test_internal_execution_nonexistent_machine(self):
        """Internal execution returns error dict for nonexistent machine."""
        from robotocore.services.stepfunctions.provider import _start_execution_internal

        result = _start_execution_internal(
            "arn:aws:states:us-east-1:123456789012:stateMachine:nope",
            {},
            REGION,
            ACCOUNT_ID,
        )
        assert "error" in result

    def test_internal_execution_with_fail_machine(self):
        """Internal execution returns error info on failure."""
        from robotocore.services.stepfunctions.provider import _start_execution_internal

        sm = _create_sm("nested-fail", FAIL_DEFINITION)
        result = _start_execution_internal(sm["stateMachineArn"], {}, REGION, ACCOUNT_ID)
        assert "error" in result
        assert result["error"] == "CustomError"
        assert result["cause"] == "Something went wrong"


class TestWaitStateExecution:
    """Wait state execution completes correctly."""

    def test_wait_state_completes(self):
        """Wait state machine eventually reaches SUCCEEDED."""
        sm = _create_sm("wait-machine", WAIT_DEFINITION)
        result = _start_exec(sm["stateMachineArn"])
        exec_arn = result["executionArn"]

        status = _wait_for_completion(exec_arn, timeout=10.0)
        assert status == "SUCCEEDED"

        desc = _describe_exec(exec_arn)
        output = json.loads(desc["output"])
        assert output == {"waited": True}
