"""Native Step Functions provider with ASL execution.

Wraps Moto for state machine CRUD, uses native ASL interpreter for execution.
Supports STANDARD and EXPRESS workflow types, callback patterns, and execution history.
"""

import json
import threading
import time
import uuid
from typing import Any

from starlette.requests import Request
from starlette.responses import Response

from robotocore.services.stepfunctions.asl import (
    ASLExecutionError,
    ASLExecutor,
    send_task_failure,
    send_task_heartbeat,
    send_task_success,
)

# In-memory execution store
_executions: dict[str, dict] = {}
_state_machines: dict[str, dict] = {}  # arn -> definition
_execution_histories: dict[str, Any] = {}  # exec_arn -> ExecutionHistory
_tags: dict[str, list[dict]] = {}  # resource_arn -> [{"key": ..., "value": ...}]
_exec_lock = threading.Lock()


async def handle_stepfunctions_request(request: Request, region: str, account_id: str) -> Response:
    """Handle Step Functions API request (JSON protocol via X-Amz-Target)."""
    body = await request.body()
    target = request.headers.get("x-amz-target", "")

    # Extract operation: "AWSStepFunctions.CreateStateMachine"
    operation = target.split(".")[-1] if "." in target else target

    params = json.loads(body) if body else {}

    handler = _ACTION_MAP.get(operation)
    if handler is None:
        return _error("UnknownOperation", f"Unknown operation: {operation}", 400)

    try:
        result = handler(params, region, account_id)
        return _json(200, result)
    except SfnError as e:
        return _error(e.code, e.message, e.status)
    except Exception as e:
        return _error("InternalError", str(e), 500)


class SfnError(Exception):
    def __init__(self, code: str, message: str, status: int = 400):
        self.code = code
        self.message = message
        self.status = status


# --- Operations ---


def _create_state_machine(params: dict, region: str, account_id: str) -> dict:
    name = params.get("name", "")
    definition = params.get("definition", "{}")
    role_arn = params.get("roleArn", "")
    sm_type = params.get("type", "STANDARD")

    arn = f"arn:aws:states:{region}:{account_id}:stateMachine:{name}"

    if isinstance(definition, str):
        parsed_def = json.loads(definition)
    else:
        parsed_def = definition

    with _exec_lock:
        _state_machines[arn] = {
            "name": name,
            "arn": arn,
            "definition": parsed_def,
            "definition_str": definition if isinstance(definition, str) else json.dumps(definition),
            "roleArn": role_arn,
            "type": sm_type,
            "status": "ACTIVE",
            "creationDate": time.time(),
        }

    return {
        "stateMachineArn": arn,
        "creationDate": time.time(),
    }


def _delete_state_machine(params: dict, region: str, account_id: str) -> dict:
    arn = params.get("stateMachineArn", "")
    with _exec_lock:
        _state_machines.pop(arn, None)
    return {}


def _describe_state_machine(params: dict, region: str, account_id: str) -> dict:
    arn = params.get("stateMachineArn", "")
    with _exec_lock:
        sm = _state_machines.get(arn)
    if not sm:
        raise SfnError("StateMachineDoesNotExist", f"State machine not found: {arn}")
    return {
        "stateMachineArn": sm["arn"],
        "name": sm["name"],
        "status": sm["status"],
        "definition": sm["definition_str"],
        "roleArn": sm["roleArn"],
        "type": sm["type"],
        "creationDate": sm["creationDate"],
    }


def _list_state_machines(params: dict, region: str, account_id: str) -> dict:
    with _exec_lock:
        machines = list(_state_machines.values())
    return {
        "stateMachines": [
            {
                "stateMachineArn": sm["arn"],
                "name": sm["name"],
                "type": sm["type"],
                "creationDate": sm["creationDate"],
            }
            for sm in machines
        ]
    }


def _update_state_machine(params: dict, region: str, account_id: str) -> dict:
    arn = params.get("stateMachineArn", "")
    with _exec_lock:
        sm = _state_machines.get(arn)
        if not sm:
            raise SfnError("StateMachineDoesNotExist", f"State machine not found: {arn}")
        if "definition" in params:
            definition = params["definition"]
            if isinstance(definition, str):
                sm["definition"] = json.loads(definition)
                sm["definition_str"] = definition
            else:
                sm["definition"] = definition
                sm["definition_str"] = json.dumps(definition)
        if "roleArn" in params:
            sm["roleArn"] = params["roleArn"]
    return {"updateDate": time.time()}


def _start_execution(params: dict, region: str, account_id: str) -> dict:
    sm_arn = params.get("stateMachineArn", "")
    name = params.get("name", str(uuid.uuid4()))
    input_str = params.get("input", "{}")

    with _exec_lock:
        sm = _state_machines.get(sm_arn)
    if not sm:
        raise SfnError("StateMachineDoesNotExist", f"State machine not found: {sm_arn}")

    exec_arn = f"{sm_arn.replace(':stateMachine:', ':execution:')}:{name}"

    input_data = json.loads(input_str) if isinstance(input_str, str) else input_str

    # Execute synchronously
    executor = ASLExecutor(sm["definition"], region, account_id, execution_arn=exec_arn)
    start_time = time.time()

    try:
        output = executor.execute(input_data)
        status = "SUCCEEDED"
        error = None
        cause = None
    except ASLExecutionError as e:
        output = None
        status = "FAILED"
        error = e.error
        cause = e.cause

    exec_info = {
        "executionArn": exec_arn,
        "stateMachineArn": sm_arn,
        "name": name,
        "status": status,
        "startDate": start_time,
        "stopDate": time.time(),
        "input": input_str,
        "output": json.dumps(output) if output is not None else None,
        "error": error,
        "cause": cause,
        "smType": sm.get("type", "STANDARD"),
    }

    with _exec_lock:
        # EXPRESS executions are not stored in ListExecutions
        _executions[exec_arn] = exec_info
        if executor.history:
            _execution_histories[exec_arn] = executor.history

    return {
        "executionArn": exec_arn,
        "startDate": start_time,
    }


def _start_sync_execution(params: dict, region: str, account_id: str) -> dict:
    """StartSyncExecution — for EXPRESS workflows, returns result immediately."""
    sm_arn = params.get("stateMachineArn", "")
    name = params.get("name", str(uuid.uuid4()))
    input_str = params.get("input", "{}")

    with _exec_lock:
        sm = _state_machines.get(sm_arn)
    if not sm:
        raise SfnError("StateMachineDoesNotExist", f"State machine not found: {sm_arn}")

    if sm.get("type") != "EXPRESS":
        raise SfnError(
            "InvalidArn",
            "StartSyncExecution is only supported for EXPRESS state machines",
        )

    exec_arn = f"{sm_arn.replace(':stateMachine:', ':express:')}:{name}"
    input_data = json.loads(input_str) if isinstance(input_str, str) else input_str

    executor = ASLExecutor(sm["definition"], region, account_id, execution_arn=exec_arn)
    start_time = time.time()

    try:
        output = executor.execute(input_data)
        status = "SUCCEEDED"
        error = None
        cause = None
    except ASLExecutionError as e:
        output = None
        status = "FAILED"
        error = e.error
        cause = e.cause

    stop_time = time.time()

    # Store for history retrieval but not in ListExecutions
    exec_info = {
        "executionArn": exec_arn,
        "stateMachineArn": sm_arn,
        "name": name,
        "status": status,
        "startDate": start_time,
        "stopDate": stop_time,
        "input": input_str,
        "output": json.dumps(output) if output is not None else None,
        "error": error,
        "cause": cause,
        "smType": "EXPRESS",
    }

    with _exec_lock:
        _executions[exec_arn] = exec_info
        if executor.history:
            _execution_histories[exec_arn] = executor.history

    result = {
        "executionArn": exec_arn,
        "stateMachineArn": sm_arn,
        "name": name,
        "status": status,
        "startDate": start_time,
        "stopDate": stop_time,
        "input": input_str,
    }
    if output is not None:
        result["output"] = json.dumps(output)
    if error:
        result["error"] = error
        result["cause"] = cause
    return result


def _start_execution_internal(
    sm_arn: str, input_data: Any, region: str, account_id: str
) -> dict:
    """Internal helper for nested Step Functions execution from ASL."""
    with _exec_lock:
        sm = _state_machines.get(sm_arn)
    if not sm:
        return {"error": f"State machine not found: {sm_arn}"}

    name = str(uuid.uuid4())
    exec_arn = f"{sm_arn.replace(':stateMachine:', ':execution:')}:{name}"
    executor = ASLExecutor(sm["definition"], region, account_id, execution_arn=exec_arn)

    try:
        output = executor.execute(input_data if isinstance(input_data, dict) else {})
        return {"executionArn": exec_arn, "output": output}
    except ASLExecutionError as e:
        return {"executionArn": exec_arn, "error": e.error, "cause": e.cause}


def _describe_execution(params: dict, region: str, account_id: str) -> dict:
    exec_arn = params.get("executionArn", "")
    with _exec_lock:
        execution = _executions.get(exec_arn)
    if not execution:
        raise SfnError("ExecutionDoesNotExist", f"Execution not found: {exec_arn}")
    result = {
        "executionArn": execution["executionArn"],
        "stateMachineArn": execution["stateMachineArn"],
        "name": execution["name"],
        "status": execution["status"],
        "startDate": execution["startDate"],
        "stopDate": execution.get("stopDate"),
        "input": execution["input"],
    }
    if execution["output"] is not None:
        result["output"] = execution["output"]
    if execution["error"]:
        result["error"] = execution["error"]
        result["cause"] = execution["cause"]
    return result


def _stop_execution(params: dict, region: str, account_id: str) -> dict:
    exec_arn = params.get("executionArn", "")
    with _exec_lock:
        execution = _executions.get(exec_arn)
        if execution:
            execution["status"] = "ABORTED"
            execution["stopDate"] = time.time()
            # Record abort in history
            history = _execution_histories.get(exec_arn)
            if history:
                history.execution_aborted()
    return {"stopDate": time.time()}


def _list_executions(params: dict, region: str, account_id: str) -> dict:
    sm_arn = params.get("stateMachineArn", "")
    status_filter = params.get("statusFilter")
    with _exec_lock:
        # EXPRESS executions don't appear in ListExecutions
        execs = [
            e
            for e in _executions.values()
            if e["stateMachineArn"] == sm_arn and e.get("smType") != "EXPRESS"
        ]
    if status_filter:
        execs = [e for e in execs if e["status"] == status_filter]
    return {
        "executions": [
            {
                "executionArn": e["executionArn"],
                "stateMachineArn": e["stateMachineArn"],
                "name": e["name"],
                "status": e["status"],
                "startDate": e["startDate"],
                "stopDate": e.get("stopDate"),
            }
            for e in execs
        ]
    }


def _get_execution_history(params: dict, region: str, account_id: str) -> dict:
    exec_arn = params.get("executionArn", "")
    reverse_order = params.get("reverseOrder", False)

    with _exec_lock:
        history = _execution_histories.get(exec_arn)

    if not history:
        return {"events": []}

    return {"events": history.get_events(reverse_order=reverse_order)}


def _send_task_success_op(params: dict, region: str, account_id: str) -> dict:
    """SendTaskSuccess — complete a task waiting for callback."""
    task_token = params.get("taskToken", "")
    output = params.get("output", "{}")

    if isinstance(output, str):
        try:
            output = json.loads(output)
        except json.JSONDecodeError:
            pass

    if not send_task_success(task_token, output):
        raise SfnError("TaskDoesNotExist", f"Task token not found: {task_token}")
    return {}


def _send_task_failure_op(params: dict, region: str, account_id: str) -> dict:
    """SendTaskFailure — fail a task waiting for callback."""
    task_token = params.get("taskToken", "")
    error = params.get("error", "")
    cause = params.get("cause", "")

    if not send_task_failure(task_token, error, cause):
        raise SfnError("TaskDoesNotExist", f"Task token not found: {task_token}")
    return {}


def _send_task_heartbeat_op(params: dict, region: str, account_id: str) -> dict:
    """SendTaskHeartbeat — send heartbeat for a waiting task."""
    task_token = params.get("taskToken", "")

    if not send_task_heartbeat(task_token):
        raise SfnError("TaskDoesNotExist", f"Task token not found: {task_token}")
    return {}


def _tag_resource(params: dict, region: str, account_id: str) -> dict:
    arn = params.get("resourceArn", "")
    new_tags = params.get("tags", [])
    with _exec_lock:
        existing = _tags.get(arn, [])
        # Merge: new tags overwrite existing with same key
        tag_map = {t["key"]: t["value"] for t in existing}
        for t in new_tags:
            tag_map[t["key"]] = t["value"]
        _tags[arn] = [{"key": k, "value": v} for k, v in tag_map.items()]
    return {}


def _untag_resource(params: dict, region: str, account_id: str) -> dict:
    arn = params.get("resourceArn", "")
    keys_to_remove = params.get("tagKeys", [])
    with _exec_lock:
        existing = _tags.get(arn, [])
        _tags[arn] = [t for t in existing if t["key"] not in keys_to_remove]
    return {}


def _list_tags_for_resource(params: dict, region: str, account_id: str) -> dict:
    arn = params.get("resourceArn", "")
    with _exec_lock:
        return {"tags": list(_tags.get(arn, []))}


# --- Helpers ---


def _json(status_code: int, data) -> Response:
    if data is None:
        return Response(content=b"", status_code=status_code)
    return Response(
        content=json.dumps(data),
        status_code=status_code,
        media_type="application/x-amz-json-1.0",
    )


def _error(code: str, message: str, status: int) -> Response:
    body = json.dumps({"__type": code, "message": message})
    return Response(content=body, status_code=status, media_type="application/x-amz-json-1.0")


_ACTION_MAP = {
    "CreateStateMachine": _create_state_machine,
    "DeleteStateMachine": _delete_state_machine,
    "DescribeStateMachine": _describe_state_machine,
    "ListStateMachines": _list_state_machines,
    "UpdateStateMachine": _update_state_machine,
    "StartExecution": _start_execution,
    "StartSyncExecution": _start_sync_execution,
    "DescribeExecution": _describe_execution,
    "StopExecution": _stop_execution,
    "ListExecutions": _list_executions,
    "GetExecutionHistory": _get_execution_history,
    "SendTaskSuccess": _send_task_success_op,
    "SendTaskFailure": _send_task_failure_op,
    "SendTaskHeartbeat": _send_task_heartbeat_op,
    "TagResource": _tag_resource,
    "UntagResource": _untag_resource,
    "ListTagsForResource": _list_tags_for_resource,
}
