"""Native Step Functions provider with ASL execution.

Wraps Moto for state machine CRUD, uses native ASL interpreter for execution.
"""

import json
import threading
import time
import uuid

from starlette.requests import Request
from starlette.responses import Response

from robotocore.services.stepfunctions.asl import ASLExecutionError, ASLExecutor

# In-memory execution store
_executions: dict[str, dict] = {}
_state_machines: dict[str, dict] = {}  # arn -> definition
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

    # Execute synchronously (for STANDARD type, real AWS is async but for testing sync is fine)
    executor = ASLExecutor(sm["definition"], region, account_id)
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
    }

    with _exec_lock:
        _executions[exec_arn] = exec_info

    return {
        "executionArn": exec_arn,
        "startDate": start_time,
    }


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
    return {"stopDate": time.time()}


def _list_executions(params: dict, region: str, account_id: str) -> dict:
    sm_arn = params.get("stateMachineArn", "")
    status_filter = params.get("statusFilter")
    with _exec_lock:
        execs = [e for e in _executions.values() if e["stateMachineArn"] == sm_arn]
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
    # Simplified history
    return {"events": []}


def _tag_resource(params: dict, region: str, account_id: str) -> dict:
    return {}


def _untag_resource(params: dict, region: str, account_id: str) -> dict:
    return {}


def _list_tags_for_resource(params: dict, region: str, account_id: str) -> dict:
    return {"tags": []}


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
    "DescribeExecution": _describe_execution,
    "StopExecution": _stop_execution,
    "ListExecutions": _list_executions,
    "GetExecutionHistory": _get_execution_history,
    "TagResource": _tag_resource,
    "UntagResource": _untag_resource,
    "ListTagsForResource": _list_tags_for_resource,
}
