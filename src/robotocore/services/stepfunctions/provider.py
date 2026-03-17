"""Native Step Functions provider with ASL execution.

Wraps Moto for state machine CRUD, uses native ASL interpreter for execution.
Supports STANDARD and EXPRESS workflow types, callback patterns, and execution history.
"""

import asyncio
import json
import logging
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
from robotocore.services.stepfunctions.history import ExecutionHistory
from robotocore.services.stepfunctions.mock_config import (
    extract_test_case_from_name,
    get_mock_config,
    get_test_case,
)

logger = logging.getLogger(__name__)

# In-memory execution store
_executions: dict[str, dict] = {}
_state_machines: dict[str, dict] = {}  # arn -> definition
_execution_histories: dict[str, Any] = {}  # exec_arn -> ExecutionHistory
_tags: dict[str, list[dict]] = {}  # resource_arn -> [{"key": ..., "value": ...}]
_running_threads: dict[str, threading.Thread] = {}  # exec_arn -> background thread
_abort_events: dict[str, threading.Event] = {}  # exec_arn -> abort signal
_exec_lock = threading.Lock()


def _resolve_mock_test_case(
    sm_name: str, execution_name: str, header_test_case: str | None = None
) -> dict | None:
    """Resolve the mock test case for an execution.

    Test case selection priority:
    1. X-SFN-Mock-Config header value
    2. '#TestCase' suffix in execution name
    3. None (no mock)
    """
    config = get_mock_config()
    if config is None:
        return None

    test_case_name = header_test_case
    if test_case_name is None:
        _, test_case_name = extract_test_case_from_name(execution_name)

    if test_case_name is None:
        return None

    return get_test_case(config, sm_name, test_case_name)


async def handle_stepfunctions_request(request: Request, region: str, account_id: str) -> Response:
    """Handle Step Functions API request (JSON protocol via X-Amz-Target)."""
    body = await request.body()
    target = request.headers.get("x-amz-target", "")

    # Extract operation: "AWSStepFunctions.CreateStateMachine"
    operation = target.split(".")[-1] if "." in target else target

    params = json.loads(body) if body else {}

    # Pass mock config header for StartExecution / StartSyncExecution
    mock_header = request.headers.get("x-sfn-mock-config")
    if mock_header and operation in ("StartExecution", "StartSyncExecution"):
        params["_mockTestCase"] = mock_header

    handler = _ACTION_MAP.get(operation)
    if handler is None:
        from robotocore.providers.moto_bridge import forward_to_moto

        return await forward_to_moto(request, "stepfunctions", account_id=account_id)

    try:
        # StartExecution and StartSyncExecution can block for a long time
        # (state machine execution, including Lambda invocations that call back
        # to the server). Run them in a thread to avoid blocking the event loop.
        if operation in ("StartExecution", "StartSyncExecution"):
            result = await asyncio.to_thread(handler, params, region, account_id)
        else:
            result = handler(params, region, account_id)
        return _json(200, result)
    except SfnError as e:
        return _error(e.code, e.message, e.status)
    except Exception as e:  # noqa: BLE001
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
        if arn in _state_machines:
            raise SfnError(
                "StateMachineAlreadyExists",
                f"State Machine Already Exists: '{arn}'",
            )
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
        # Cascade: clean up executions, histories, and tags for this state machine
        exec_arns_to_remove = [k for k, v in _executions.items() if v["stateMachineArn"] == arn]
        for exec_arn in exec_arns_to_remove:
            _executions.pop(exec_arn, None)
            _execution_histories.pop(exec_arn, None)
        _tags.pop(arn, None)
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
    mock_header = params.pop("_mockTestCase", None)

    with _exec_lock:
        sm = _state_machines.get(sm_arn)
    if not sm:
        raise SfnError("StateMachineDoesNotExist", f"State machine not found: {sm_arn}")

    # Strip test case suffix from execution name for the ARN
    clean_name, _ = extract_test_case_from_name(name)
    exec_arn = f"{sm_arn.replace(':stateMachine:', ':execution:')}:{clean_name}"

    # Resolve mock test case
    mock_test_case = _resolve_mock_test_case(sm["name"], name, mock_header)

    input_data = json.loads(input_str) if isinstance(input_str, str) else input_str
    start_time = time.time()

    # Store execution immediately with RUNNING status
    exec_info = {
        "executionArn": exec_arn,
        "stateMachineArn": sm_arn,
        "name": clean_name,
        "status": "RUNNING",
        "startDate": start_time,
        "stopDate": None,
        "input": input_str,
        "output": None,
        "error": None,
        "cause": None,
        "smType": sm.get("type", "STANDARD"),
    }

    abort_event = threading.Event()

    # Pre-create ExecutionHistory so _stop_execution can record abort even
    # if the background thread hasn't finished yet.
    history = ExecutionHistory(exec_arn)

    with _exec_lock:
        # Check for duplicate execution name on STANDARD workflows
        if exec_arn in _executions and sm.get("type", "STANDARD") == "STANDARD":
            raise SfnError(
                "ExecutionAlreadyExists",
                f"Execution already exists: '{exec_arn}'",
            )
        _executions[exec_arn] = exec_info
        _abort_events[exec_arn] = abort_event
        _execution_histories[exec_arn] = history

    # Launch background thread for STANDARD workflows
    definition = sm["definition"]
    thread = threading.Thread(
        target=_run_execution_background,
        args=(
            exec_arn,
            definition,
            input_data,
            region,
            account_id,
            abort_event,
            mock_test_case,
            history,
        ),
        daemon=True,
        name=f"sfn-exec-{name}",
    )

    with _exec_lock:
        _running_threads[exec_arn] = thread

    thread.start()

    return {
        "executionArn": exec_arn,
        "startDate": start_time,
    }


def _run_execution_background(
    exec_arn: str,
    definition: dict,
    input_data: dict,
    region: str,
    account_id: str,
    abort_event: threading.Event,
    mock_test_case: dict | None = None,
    history: ExecutionHistory | None = None,
) -> None:
    """Execute a state machine in a background thread, updating status on completion."""
    executor = ASLExecutor(
        definition,
        region,
        account_id,
        execution_arn=exec_arn,
        mock_test_case=mock_test_case,
        history=history,
    )

    try:
        output = executor.execute(input_data)

        with _exec_lock:
            execution = _executions.get(exec_arn)
            if execution and execution["status"] == "RUNNING":
                execution["status"] = "SUCCEEDED"
                execution["stopDate"] = time.time()
                execution["output"] = json.dumps(output) if output is not None else None
            if executor.history:
                _execution_histories[exec_arn] = executor.history

    except ASLExecutionError as e:
        with _exec_lock:
            execution = _executions.get(exec_arn)
            if execution and execution["status"] == "RUNNING":
                execution["status"] = "FAILED"
                execution["stopDate"] = time.time()
                execution["error"] = e.error
                execution["cause"] = e.cause
            if executor.history:
                _execution_histories[exec_arn] = executor.history

    except Exception as e:
        logger.exception(f"Unexpected error in execution {exec_arn}")
        with _exec_lock:
            execution = _executions.get(exec_arn)
            if execution and execution["status"] == "RUNNING":
                execution["status"] = "FAILED"
                execution["stopDate"] = time.time()
                execution["error"] = type(e).__name__
                execution["cause"] = str(e)

    finally:
        with _exec_lock:
            _running_threads.pop(exec_arn, None)
            _abort_events.pop(exec_arn, None)


def _start_sync_execution(params: dict, region: str, account_id: str) -> dict:
    """StartSyncExecution — for EXPRESS workflows, returns result immediately."""
    sm_arn = params.get("stateMachineArn", "")
    name = params.get("name", str(uuid.uuid4()))
    input_str = params.get("input", "{}")
    mock_header = params.pop("_mockTestCase", None)

    with _exec_lock:
        sm = _state_machines.get(sm_arn)
    if not sm:
        raise SfnError("StateMachineDoesNotExist", f"State machine not found: {sm_arn}")

    if sm.get("type") != "EXPRESS":
        raise SfnError(
            "InvalidArn",
            "StartSyncExecution is only supported for EXPRESS state machines",
        )

    clean_name, _ = extract_test_case_from_name(name)
    exec_arn = f"{sm_arn.replace(':stateMachine:', ':express:')}:{clean_name}"
    input_data = json.loads(input_str) if isinstance(input_str, str) else input_str

    mock_test_case = _resolve_mock_test_case(sm["name"], name, mock_header)
    executor = ASLExecutor(
        sm["definition"],
        region,
        account_id,
        execution_arn=exec_arn,
        mock_test_case=mock_test_case,
    )
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


def _start_execution_internal(sm_arn: str, input_data: Any, region: str, account_id: str) -> dict:
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
        if not execution:
            raise SfnError("ExecutionDoesNotExist", f"Execution not found: {exec_arn}")
        execution["status"] = "ABORTED"
        execution["stopDate"] = time.time()
        if params.get("error"):
            execution["error"] = params["error"]
        if params.get("cause"):
            execution["cause"] = params["cause"]
        # Signal the background thread to stop
        abort_event = _abort_events.get(exec_arn)
        if abort_event:
            abort_event.set()
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
        except json.JSONDecodeError as exc:
            logger.debug("_send_task_success_op: loads failed (non-fatal): %s", exc)

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


def _validate_resource_arn(arn: str) -> None:
    """Validate that a resource ARN refers to an existing resource.

    Must be called under _exec_lock.
    """
    if arn in _state_machines:
        return
    if arn in _executions:
        return
    raise SfnError("ResourceNotFound", f"Resource not found: {arn}")


def _tag_resource(params: dict, region: str, account_id: str) -> dict:
    arn = params.get("resourceArn", "")
    new_tags = params.get("tags", [])
    with _exec_lock:
        _validate_resource_arn(arn)
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
        _validate_resource_arn(arn)
        existing = _tags.get(arn, [])
        _tags[arn] = [t for t in existing if t["key"] not in keys_to_remove]
    return {}


def _list_tags_for_resource(params: dict, region: str, account_id: str) -> dict:
    arn = params.get("resourceArn", "")
    with _exec_lock:
        return {"tags": list(_tags.get(arn, []))}


# --- Version Management ---

# version storage: sm_arn -> list of version dicts
_versions: dict[str, list[dict]] = {}


def _validate_state_machine_definition(params: dict, region: str, account_id: str) -> dict:
    """ValidateStateMachineDefinition — validate ASL JSON."""
    definition = params.get("definition", "")
    result = "OK"
    diagnostics: list[dict] = []
    try:
        json.loads(definition)
    except Exception:  # noqa: BLE001
        result = "FAIL"
        diagnostics.append(
            {
                "severity": "ERROR",
                "code": "INVALID_JSON_DESCRIPTION",
                "message": "Could not parse the state machine definition.",
            }
        )
    return {"result": result, "diagnostics": diagnostics, "truncated": False}


def _publish_state_machine_version(params: dict, region: str, account_id: str) -> dict:
    """PublishStateMachineVersion — create a numbered version snapshot."""
    sm_arn = params.get("stateMachineArn", "")
    description = params.get("description")

    with _exec_lock:
        sm = _state_machines.get(sm_arn)
        if not sm:
            raise SfnError("StateMachineDoesNotExist", f"State machine not found: {sm_arn}")

        versions = _versions.setdefault(sm_arn, [])
        version_number = len(versions) + 1
        version_arn = f"{sm_arn}:{version_number}"
        now = time.time()
        version_entry = {
            "stateMachineVersionArn": version_arn,
            "creationDate": now,
            "description": description,
        }
        versions.append(version_entry)

    return {
        "stateMachineVersionArn": version_arn,
        "creationDate": now,
    }


def _list_state_machine_versions(params: dict, region: str, account_id: str) -> dict:
    """ListStateMachineVersions — return all versions for a state machine."""
    sm_arn = params.get("stateMachineArn", "")

    with _exec_lock:
        sm = _state_machines.get(sm_arn)
        if not sm:
            raise SfnError("StateMachineDoesNotExist", f"State machine not found: {sm_arn}")
        versions = _versions.get(sm_arn, [])
        items = [
            {
                "stateMachineVersionArn": v["stateMachineVersionArn"],
                "creationDate": v["creationDate"],
            }
            for v in reversed(versions)
        ]
    return {"stateMachineVersions": items}


def _delete_state_machine_version(params: dict, region: str, account_id: str) -> dict:
    """DeleteStateMachineVersion — remove a specific version."""
    version_arn = params.get("stateMachineVersionArn", "")

    # Parse: arn:...:stateMachine:name:version_number
    parts = version_arn.rsplit(":", 1)
    if len(parts) != 2 or not parts[1].isdigit():
        raise SfnError("ValidationException", f"Invalid version ARN: {version_arn}")

    sm_arn = parts[0]
    int(parts[1])

    with _exec_lock:
        versions = _versions.get(sm_arn, [])
        _versions[sm_arn] = [v for v in versions if v["stateMachineVersionArn"] != version_arn]
    return {}


# --- Alias Management ---

_aliases: dict[str, dict] = {}


def _create_state_machine_alias(params: dict, region: str, account_id: str) -> dict:
    name = params.get("name", "")
    description = params.get("description", "")
    routing_config = params.get("routingConfiguration", [])

    if not routing_config:
        raise SfnError("ValidationException", "routingConfiguration is required")

    version_arn = routing_config[0].get("stateMachineVersionArn", "")
    parts = version_arn.rsplit(":", 1)
    if len(parts) != 2 or not parts[1].isnumeric():
        raise SfnError(
            "ValidationException",
            f"Invalid version ARN: {version_arn}",
        )
    sm_arn = parts[0]

    with _exec_lock:
        if sm_arn not in _state_machines:
            raise SfnError(
                "StateMachineDoesNotExist",
                f"State Machine Does Not Exist: '{sm_arn}'",
            )
        sm_versions = _versions.get(sm_arn, [])
        if not any(v["stateMachineVersionArn"] == version_arn for v in sm_versions):
            raise SfnError(
                "ResourceNotFound",
                f"Version does not exist: '{version_arn}'",
            )

        alias_arn = f"{sm_arn}:{name}"
        if alias_arn in _aliases:
            raise SfnError(
                "ConflictException",
                f"State Machine Alias already exists: '{alias_arn}'",
                409,
            )
        now = time.time()
        _aliases[alias_arn] = {
            "stateMachineAliasArn": alias_arn,
            "name": name,
            "description": description,
            "routingConfiguration": routing_config,
            "creationDate": now,
            "updateDate": now,
        }

    return {"stateMachineAliasArn": alias_arn, "creationDate": now}


def _describe_state_machine_alias(params: dict, region: str, account_id: str) -> dict:
    alias_arn = params.get("stateMachineAliasArn", "")
    with _exec_lock:
        alias = _aliases.get(alias_arn)
    if not alias:
        raise SfnError(
            "ResourceNotFound",
            f"Resource not found: '{alias_arn}'",
        )
    return dict(alias)


def _list_state_machine_aliases(params: dict, region: str, account_id: str) -> dict:
    sm_arn = params.get("stateMachineArn", "")
    prefix = sm_arn.rstrip(":") + ":"
    with _exec_lock:
        matches = [
            {
                "stateMachineAliasArn": a["stateMachineAliasArn"],
                "creationDate": a["creationDate"],
            }
            for a in _aliases.values()
            if a["stateMachineAliasArn"].startswith(prefix) and a["stateMachineAliasArn"] != sm_arn
        ]
    return {"stateMachineAliases": matches}


def _update_state_machine_alias(params: dict, region: str, account_id: str) -> dict:
    alias_arn = params.get("stateMachineAliasArn", "")
    with _exec_lock:
        alias = _aliases.get(alias_arn)
        if not alias:
            raise SfnError(
                "ResourceNotFound",
                f"Resource not found: '{alias_arn}'",
            )
        if "description" in params:
            alias["description"] = params["description"]
        if "routingConfiguration" in params:
            alias["routingConfiguration"] = params["routingConfiguration"]
        alias["updateDate"] = time.time()
    return {
        "stateMachineAliasArn": alias_arn,
        "updateDate": alias["updateDate"],
    }


def _delete_state_machine_alias(params: dict, region: str, account_id: str) -> dict:
    alias_arn = params.get("stateMachineAliasArn", "")
    with _exec_lock:
        if alias_arn not in _aliases:
            raise SfnError(
                "ResourceNotFound",
                f"Resource not found: '{alias_arn}'",
            )
        del _aliases[alias_arn]
    return {}


# --- Helpers ---


def _json(status_code: int, data) -> Response:
    if data is None:
        return Response(content=b"", status_code=status_code)
    return Response(
        content=json.dumps(data),
        status_code=status_code,
        media_type="application/x-amz-json-1.0",
    )


def _describe_state_machine_for_execution(params: dict, region: str, account_id: str) -> dict:
    exec_arn = params.get("executionArn", "")
    with _exec_lock:
        execution = _executions.get(exec_arn)
        if not execution:
            raise SfnError("ExecutionDoesNotExist", f"Execution {exec_arn} not found", 400)
        sm_arn = execution.get("stateMachineArn", "")
        sm = _state_machines.get(sm_arn)
        if not sm:
            raise SfnError("StateMachineDoesNotExist", "State machine not found", 400)
    return {
        "stateMachineArn": sm_arn,
        "name": sm.get("name", ""),
        "definition": sm.get("definition_str", "{}"),
        "roleArn": sm.get("roleArn", ""),
        "updateDate": sm.get("creationDate", 0),
    }


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
    "DescribeStateMachineForExecution": _describe_state_machine_for_execution,
    "ValidateStateMachineDefinition": _validate_state_machine_definition,
    "PublishStateMachineVersion": _publish_state_machine_version,
    "ListStateMachineVersions": _list_state_machine_versions,
    "DeleteStateMachineVersion": _delete_state_machine_version,
    "CreateStateMachineAlias": _create_state_machine_alias,
    "DescribeStateMachineAlias": _describe_state_machine_alias,
    "ListStateMachineAliases": _list_state_machine_aliases,
    "UpdateStateMachineAlias": _update_state_machine_alias,
    "DeleteStateMachineAlias": _delete_state_machine_alias,
}
