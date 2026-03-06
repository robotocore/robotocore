"""Unit tests for Step Functions provider request handling."""

import json

import pytest
from starlette.requests import Request

from robotocore.services.stepfunctions.provider import (
    SfnError,
    _create_state_machine,
    _delete_state_machine,
    _describe_execution,
    _describe_state_machine,
    _error,
    _executions,
    _get_execution_history,
    _json,
    _list_executions,
    _list_state_machines,
    _start_execution,
    _state_machines,
    _stop_execution,
    _update_state_machine,
    handle_stepfunctions_request,
)


def _make_request(body=b"", headers=None):
    hdrs = headers or {}
    raw_headers = [(k.lower().encode(), v.encode()) for k, v in hdrs.items()]
    scope = {
        "type": "http",
        "method": "POST",
        "path": "/",
        "query_string": b"",
        "headers": raw_headers,
        "root_path": "",
        "scheme": "http",
        "server": ("localhost", 4566),
    }

    async def receive():
        return {"type": "http.request", "body": body}

    return Request(scope, receive)


def _clear_state():
    _state_machines.clear()
    _executions.clear()


_SIMPLE_DEFINITION = json.dumps(
    {
        "StartAt": "Pass",
        "States": {"Pass": {"Type": "Pass", "End": True}},
    }
)


class TestHelpers:
    def test_json_response(self):
        resp = _json(200, {"key": "val"})
        assert resp.status_code == 200
        data = json.loads(resp.body)
        assert data["key"] == "val"

    def test_json_none_response(self):
        resp = _json(204, None)
        assert resp.status_code == 204
        assert resp.body == b""

    def test_error_response(self):
        resp = _error("SomeError", "bad thing", 400)
        assert resp.status_code == 400
        data = json.loads(resp.body)
        assert data["__type"] == "SomeError"
        assert data["message"] == "bad thing"


class TestSfnError:
    def test_attributes(self):
        err = SfnError("MyCode", "msg", 404)
        assert err.code == "MyCode"
        assert err.message == "msg"
        assert err.status == 404

    def test_default_status(self):
        err = SfnError("Code", "msg")
        assert err.status == 400


class TestStateMachineOperations:
    def setup_method(self):
        _clear_state()

    def teardown_method(self):
        _clear_state()

    def test_create_state_machine(self):
        result = _create_state_machine(
            {"name": "sm1", "definition": _SIMPLE_DEFINITION, "roleArn": "r"},
            "us-east-1",
            "123",
        )
        assert "stateMachineArn" in result
        assert "us-east-1" in result["stateMachineArn"]

    def test_describe_state_machine(self):
        _create_state_machine(
            {"name": "sm1", "definition": _SIMPLE_DEFINITION, "roleArn": "r"},
            "us-east-1",
            "123",
        )
        arn = "arn:aws:states:us-east-1:123:stateMachine:sm1"
        result = _describe_state_machine({"stateMachineArn": arn}, "us-east-1", "123")
        assert result["name"] == "sm1"
        assert result["status"] == "ACTIVE"

    def test_describe_nonexistent(self):
        with pytest.raises(SfnError) as exc_info:
            _describe_state_machine({"stateMachineArn": "arn:nope"}, "us-east-1", "123")
        assert exc_info.value.code == "StateMachineDoesNotExist"

    def test_list_state_machines(self):
        _create_state_machine(
            {"name": "sm1", "definition": _SIMPLE_DEFINITION, "roleArn": "r"},
            "us-east-1",
            "123",
        )
        result = _list_state_machines({}, "us-east-1", "123")
        assert len(result["stateMachines"]) == 1
        assert result["stateMachines"][0]["name"] == "sm1"

    def test_delete_state_machine(self):
        _create_state_machine(
            {"name": "sm1", "definition": _SIMPLE_DEFINITION, "roleArn": "r"},
            "us-east-1",
            "123",
        )
        arn = "arn:aws:states:us-east-1:123:stateMachine:sm1"
        _delete_state_machine({"stateMachineArn": arn}, "us-east-1", "123")
        assert arn not in _state_machines

    def test_update_state_machine(self):
        _create_state_machine(
            {"name": "sm1", "definition": _SIMPLE_DEFINITION, "roleArn": "r"},
            "us-east-1",
            "123",
        )
        arn = "arn:aws:states:us-east-1:123:stateMachine:sm1"
        new_def = json.dumps(
            {
                "StartAt": "NewPass",
                "States": {"NewPass": {"Type": "Pass", "End": True}},
            }
        )
        result = _update_state_machine(
            {"stateMachineArn": arn, "definition": new_def, "roleArn": "r2"},
            "us-east-1",
            "123",
        )
        assert "updateDate" in result
        assert _state_machines[arn]["roleArn"] == "r2"

    def test_update_nonexistent(self):
        with pytest.raises(SfnError):
            _update_state_machine({"stateMachineArn": "arn:nope"}, "us-east-1", "123")


class TestExecutionOperations:
    def setup_method(self):
        _clear_state()

    def teardown_method(self):
        _clear_state()

    def test_start_execution(self):
        _create_state_machine(
            {"name": "sm1", "definition": _SIMPLE_DEFINITION, "roleArn": "r"},
            "us-east-1",
            "123",
        )
        arn = "arn:aws:states:us-east-1:123:stateMachine:sm1"
        result = _start_execution(
            {"stateMachineArn": arn, "input": "{}"},
            "us-east-1",
            "123",
        )
        assert "executionArn" in result
        assert "startDate" in result

    def test_start_execution_nonexistent_sm(self):
        with pytest.raises(SfnError):
            _start_execution(
                {"stateMachineArn": "arn:nope", "input": "{}"},
                "us-east-1",
                "123",
            )

    def test_describe_execution(self):
        _create_state_machine(
            {"name": "sm1", "definition": _SIMPLE_DEFINITION, "roleArn": "r"},
            "us-east-1",
            "123",
        )
        sm_arn = "arn:aws:states:us-east-1:123:stateMachine:sm1"
        start = _start_execution(
            {"stateMachineArn": sm_arn, "name": "exec1", "input": "{}"},
            "us-east-1",
            "123",
        )
        result = _describe_execution({"executionArn": start["executionArn"]}, "us-east-1", "123")
        assert result["status"] == "SUCCEEDED"
        assert result["name"] == "exec1"

    def test_describe_execution_not_found(self):
        with pytest.raises(SfnError) as exc_info:
            _describe_execution({"executionArn": "arn:nope"}, "us-east-1", "123")
        assert exc_info.value.code == "ExecutionDoesNotExist"

    def test_stop_execution(self):
        _create_state_machine(
            {"name": "sm1", "definition": _SIMPLE_DEFINITION, "roleArn": "r"},
            "us-east-1",
            "123",
        )
        sm_arn = "arn:aws:states:us-east-1:123:stateMachine:sm1"
        start = _start_execution(
            {"stateMachineArn": sm_arn, "name": "exec1", "input": "{}"},
            "us-east-1",
            "123",
        )
        _stop_execution({"executionArn": start["executionArn"]}, "us-east-1", "123")
        exec_info = _executions[start["executionArn"]]
        assert exec_info["status"] == "ABORTED"

    def test_list_executions(self):
        _create_state_machine(
            {"name": "sm1", "definition": _SIMPLE_DEFINITION, "roleArn": "r"},
            "us-east-1",
            "123",
        )
        sm_arn = "arn:aws:states:us-east-1:123:stateMachine:sm1"
        _start_execution(
            {"stateMachineArn": sm_arn, "input": "{}"},
            "us-east-1",
            "123",
        )
        result = _list_executions({"stateMachineArn": sm_arn}, "us-east-1", "123")
        assert len(result["executions"]) == 1

    def test_list_executions_with_filter(self):
        _create_state_machine(
            {"name": "sm1", "definition": _SIMPLE_DEFINITION, "roleArn": "r"},
            "us-east-1",
            "123",
        )
        sm_arn = "arn:aws:states:us-east-1:123:stateMachine:sm1"
        _start_execution(
            {"stateMachineArn": sm_arn, "input": "{}"},
            "us-east-1",
            "123",
        )
        result = _list_executions(
            {"stateMachineArn": sm_arn, "statusFilter": "FAILED"},
            "us-east-1",
            "123",
        )
        assert len(result["executions"]) == 0

    def test_get_execution_history(self):
        result = _get_execution_history({}, "us-east-1", "123")
        assert result == {"events": []}


@pytest.mark.asyncio
class TestHandleStepFunctionsRequest:
    def setup_method(self):
        _clear_state()

    def teardown_method(self):
        _clear_state()

    async def test_create_state_machine_via_handler(self):
        body = json.dumps(
            {
                "name": "sm1",
                "definition": _SIMPLE_DEFINITION,
                "roleArn": "arn:aws:iam::123:role/r",
            }
        ).encode()
        headers = {
            "x-amz-target": "AWSStepFunctions.CreateStateMachine",
        }
        req = _make_request(body=body, headers=headers)
        resp = await handle_stepfunctions_request(req, "us-east-1", "123456789012")
        assert resp.status_code == 200
        data = json.loads(resp.body)
        assert "stateMachineArn" in data

    async def test_unknown_operation(self):
        body = json.dumps({}).encode()
        headers = {"x-amz-target": "AWSStepFunctions.Bogus"}
        req = _make_request(body=body, headers=headers)
        resp = await handle_stepfunctions_request(req, "us-east-1", "123456789012")
        assert resp.status_code == 400
        data = json.loads(resp.body)
        assert data["__type"] == "UnknownOperation"

    async def test_sfn_error_handling(self):
        body = json.dumps({"stateMachineArn": "arn:nope"}).encode()
        headers = {
            "x-amz-target": "AWSStepFunctions.DescribeStateMachine",
        }
        req = _make_request(body=body, headers=headers)
        resp = await handle_stepfunctions_request(req, "us-east-1", "123456789012")
        assert resp.status_code == 400
        data = json.loads(resp.body)
        assert data["__type"] == "StateMachineDoesNotExist"
