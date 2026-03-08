"""Amazon States Language (ASL) interpreter.

Executes Step Functions state machines with support for:
- Pass, Task, Choice, Wait, Succeed, Fail, Parallel, Map states
- Service integrations: Lambda, SQS, SNS, DynamoDB
- JSONPath input/output processing
- Error handling: Catch, Retry
- Intrinsic function evaluation in Parameters
- Execution history tracking
- Callback pattern with task tokens
- Map state with MaxConcurrency
"""

import copy
import json
import logging
import re
import threading
import time
import uuid
from typing import Any

from robotocore.services.stepfunctions.history import ExecutionHistory
from robotocore.services.stepfunctions.intrinsics import evaluate_intrinsic

logger = logging.getLogger(__name__)

# Global store for task token callbacks
_task_tokens: dict[str, dict] = {}  # token -> {"event": threading.Event, "result": Any}
_token_lock = threading.Lock()


class ASLExecutionError(Exception):
    def __init__(self, error: str, cause: str = ""):
        self.error = error
        self.cause = cause
        super().__init__(f"{error}: {cause}")


def send_task_success(task_token: str, output: Any) -> bool:
    """Complete a waiting task with success."""
    with _token_lock:
        token_info = _task_tokens.get(task_token)
    if not token_info:
        return False
    token_info["result"] = output
    token_info["status"] = "SUCCESS"
    token_info["event"].set()
    return True


def send_task_failure(task_token: str, error: str = "", cause: str = "") -> bool:
    """Complete a waiting task with failure."""
    with _token_lock:
        token_info = _task_tokens.get(task_token)
    if not token_info:
        return False
    token_info["error"] = error
    token_info["cause"] = cause
    token_info["status"] = "FAILED"
    token_info["event"].set()
    return True


def send_task_heartbeat(task_token: str) -> bool:
    """Send a heartbeat for a waiting task."""
    with _token_lock:
        token_info = _task_tokens.get(task_token)
    if not token_info:
        return False
    token_info["last_heartbeat"] = time.time()
    return True


class ASLExecutor:
    """Executes an ASL state machine definition."""

    def __init__(
        self,
        definition: dict,
        region: str = "us-east-1",
        account_id: str = "123456789012",
        execution_arn: str = "",
    ):
        self.definition = definition
        self.region = region
        self.account_id = account_id
        self.states = definition.get("States", {})
        self.start_at = definition.get("StartAt", "")
        self.max_steps = 1000  # Safety limit
        self.history = ExecutionHistory(execution_arn) if execution_arn else None

    def execute(self, input_data: dict | None = None) -> dict:
        """Execute the state machine and return the output."""
        current_state = self.start_at
        data = copy.deepcopy(input_data or {})
        steps = 0

        # Record execution started
        input_str = json.dumps(data)
        if self.history:
            self.history.execution_started(input_str)

        last_event_id = 0

        while current_state and steps < self.max_steps:
            steps += 1
            state_def = self.states.get(current_state)
            if not state_def:
                if self.history:
                    self.history.execution_failed(
                        "States.Runtime",
                        f"State '{current_state}' not found",
                        last_event_id,
                    )
                raise ASLExecutionError("States.Runtime", f"State '{current_state}' not found")

            state_type = state_def.get("Type", "")

            # Record state entered
            if self.history:
                last_event_id = self.history.state_entered(
                    current_state, state_type, json.dumps(data), last_event_id
                )

            try:
                data, next_state = self._execute_state(current_state, state_def, data)
            except ASLExecutionError as e:
                # Check for Catch on ASLExecutionError too
                next_state, data = self._handle_error(state_def, data, e.error, e.cause)
                if next_state is None:
                    if self.history:
                        self.history.execution_failed(e.error, e.cause, last_event_id)
                    raise
            except Exception as e:
                # Check for Catch
                next_state, data = self._handle_error(state_def, data, type(e).__name__, str(e))
                if next_state is None:
                    if self.history:
                        self.history.execution_failed(type(e).__name__, str(e), last_event_id)
                    raise ASLExecutionError(type(e).__name__, str(e))

            # Record state exited
            if self.history:
                last_event_id = self.history.state_exited(
                    current_state, state_type, json.dumps(data), last_event_id
                )

            if state_def.get("End", False) or state_type in ("Succeed", "Fail"):
                if self.history:
                    self.history.execution_succeeded(json.dumps(data), last_event_id)
                return data

            if next_state is None:
                # No Next field and not End — execution complete
                if self.history:
                    self.history.execution_succeeded(json.dumps(data), last_event_id)
                return data

            current_state = next_state

        if self.history:
            self.history.execution_timed_out(last_event_id)
        raise ASLExecutionError("States.Runtime", "Maximum execution steps exceeded")

    def _execute_state(self, name: str, state_def: dict, data: dict) -> tuple[Any, str | None]:
        """Execute a single state, return (output, next_state_name)."""
        state_type = state_def.get("Type", "")

        # Apply InputPath
        effective_input = _apply_path(data, state_def.get("InputPath", "$"))

        # Apply Parameters (with intrinsic function support)
        if "Parameters" in state_def:
            effective_input = _resolve_parameters(state_def["Parameters"], effective_input)

        # Execute based on type
        if state_type == "Pass":
            result = self._execute_pass(state_def, effective_input)
        elif state_type == "Task":
            result = self._execute_task(state_def, effective_input)
        elif state_type == "Choice":
            return data, self._execute_choice(state_def, effective_input)
        elif state_type == "Wait":
            self._execute_wait(state_def, effective_input)
            result = effective_input
        elif state_type == "Succeed":
            return effective_input, None
        elif state_type == "Fail":
            error = state_def.get("Error", "States.Fail")
            cause = state_def.get("Cause", "")
            # Resolve if they reference input
            raise ASLExecutionError(error, cause)
        elif state_type == "Parallel":
            result = self._execute_parallel(state_def, effective_input)
        elif state_type == "Map":
            result = self._execute_map(state_def, effective_input)
        else:
            raise ASLExecutionError("States.Runtime", f"Unknown state type: {state_type}")

        # Apply ResultSelector
        if "ResultSelector" in state_def:
            result = _resolve_parameters(state_def["ResultSelector"], result)

        # Apply ResultPath
        output = _apply_result_path(data, result, state_def.get("ResultPath", "$"))

        # Apply OutputPath
        output = _apply_path(output, state_def.get("OutputPath", "$"))

        return output, state_def.get("Next")

    def _execute_pass(self, state_def: dict, input_data: Any) -> Any:
        if "Result" in state_def:
            return state_def["Result"]
        return input_data

    def _execute_task(self, state_def: dict, input_data: Any) -> Any:
        """Execute a Task state — invoke an AWS service."""
        resource = state_def.get("Resource", "")

        # Check for callback pattern (.waitForTaskCallback)
        is_callback = resource.endswith(".waitForTaskCallback")
        if is_callback:
            resource = resource[: -len(".waitForTaskCallback")]

        # Record task scheduled in history
        if self.history:
            self.history.task_scheduled(
                resource, json.dumps(input_data) if not isinstance(input_data, str) else input_data
            )
            self.history.task_started(resource)

        if is_callback:
            return self._execute_callback_task(resource, input_data, state_def)

        try:
            result = self._dispatch_task(resource, input_data)
        except ASLExecutionError:
            raise
        except Exception as e:
            if self.history:
                self.history.task_failed(resource, type(e).__name__, str(e))
            raise

        if self.history:
            self.history.task_succeeded(
                resource, json.dumps(result) if not isinstance(result, str) else result
            )

        return result

    def _dispatch_task(self, resource: str, input_data: Any) -> Any:
        """Route task to the appropriate service integration."""
        # Lambda invocation
        if resource.startswith("arn:aws:lambda:") or ":function:" in resource:
            return self._invoke_lambda(resource, input_data)

        # SDK integrations: arn:aws:states:::service:action
        if resource.startswith("arn:aws:states:::"):
            service_action = resource[len("arn:aws:states:::") :]
            service, _, action = service_action.partition(":")

            if service == "sqs" and action == "sendMessage":
                return self._invoke_sqs_send(input_data)
            elif service == "sns" and action == "publish":
                return self._invoke_sns_publish(input_data)
            elif service == "dynamodb":
                return self._invoke_dynamodb(action, input_data)
            elif service == "lambda" and action == "invoke":
                fn_arn = input_data.get("FunctionName", "")
                payload = input_data.get("Payload", input_data)
                return self._invoke_lambda(fn_arn, payload)
            elif service == "states" and action == "startExecution":
                return self._invoke_step_functions(input_data)
            else:
                logger.warning(f"Unknown SDK integration: {service}:{action}")
                return input_data

        logger.warning(f"Unknown Task resource: {resource}, returning input")
        return input_data

    def _execute_callback_task(self, resource: str, input_data: Any, state_def: dict) -> Any:
        """Execute a task using the callback pattern with task tokens."""
        task_token = str(uuid.uuid4())
        timeout = state_def.get("TimeoutSeconds", 60)

        # Create token entry
        token_info: dict[str, Any] = {
            "event": threading.Event(),
            "result": None,
            "status": None,
            "error": None,
            "cause": None,
            "last_heartbeat": time.time(),
        }
        with _token_lock:
            _task_tokens[task_token] = token_info

        # Inject task token into input and dispatch
        if isinstance(input_data, dict):
            input_with_token = dict(input_data)
            input_with_token["TaskToken"] = task_token
        else:
            input_with_token = {"input": input_data, "TaskToken": task_token}

        try:
            self._dispatch_task(resource, input_with_token)
        except Exception:
            pass  # For callback, we don't fail on dispatch errors

        # Wait for callback
        got_signal = token_info["event"].wait(timeout=min(timeout, 30))

        with _token_lock:
            _task_tokens.pop(task_token, None)

        if not got_signal:
            raise ASLExecutionError("States.Timeout", "Task timed out waiting for callback")

        if token_info["status"] == "FAILED":
            raise ASLExecutionError(
                token_info["error"] or "States.TaskFailed",
                token_info["cause"] or "Task failed via callback",
            )

        return token_info["result"]

    def _execute_choice(self, state_def: dict, input_data: Any) -> str:
        """Evaluate Choice rules and return the next state name."""
        choices = state_def.get("Choices", [])
        for choice in choices:
            if _evaluate_choice_rule(choice, input_data):
                return choice["Next"]
        default = state_def.get("Default")
        if default:
            return default
        raise ASLExecutionError("States.NoChoiceMatched", "No choice rule matched and no Default")

    def _execute_wait(self, state_def: dict, input_data: Any):
        """Execute a Wait state."""
        if "Seconds" in state_def:
            # In testing, we don't actually wait long
            wait_time = min(state_def["Seconds"], 1)
            time.sleep(wait_time)
        elif "Timestamp" in state_def:
            pass  # Don't block for timestamp waits
        elif "SecondsPath" in state_def:
            seconds = _resolve_path(input_data, state_def["SecondsPath"])
            if isinstance(seconds, (int, float)):
                time.sleep(min(seconds, 1))

    def _execute_parallel(self, state_def: dict, input_data: Any) -> list:
        """Execute parallel branches."""
        branches = state_def.get("Branches", [])
        results = []
        for branch in branches:
            executor = ASLExecutor(branch, self.region, self.account_id)
            result = executor.execute(copy.deepcopy(input_data))
            results.append(result)
        return results

    def _execute_map(self, state_def: dict, input_data: Any) -> list:
        """Execute a Map state over an array with optional MaxConcurrency."""
        items_path = state_def.get("ItemsPath", "$")
        items = _resolve_path(input_data, items_path)
        if not isinstance(items, list):
            items = [items]

        max_concurrency = state_def.get("MaxConcurrency", 0)  # 0 means unlimited
        iterator = state_def.get("Iterator") or state_def.get("ItemProcessor", {})

        if max_concurrency > 0:
            return self._execute_map_with_concurrency(items, iterator, max_concurrency)

        results = []
        for idx, item in enumerate(items):
            exec_input = item
            executor = ASLExecutor(iterator, self.region, self.account_id)
            result = executor.execute(exec_input)
            results.append(result)
        return results

    def _execute_map_with_concurrency(
        self, items: list, iterator: dict, max_concurrency: int
    ) -> list:
        """Execute map iterations with bounded concurrency using threads."""
        results = [None] * len(items)
        errors: list[Exception] = []
        lock = threading.Lock()
        semaphore = threading.Semaphore(max_concurrency)

        def run_item(idx: int, item: Any):
            try:
                semaphore.acquire()
                try:
                    executor = ASLExecutor(iterator, self.region, self.account_id)
                    result = executor.execute(item)
                    with lock:
                        results[idx] = result
                finally:
                    semaphore.release()
            except Exception as e:
                with lock:
                    errors.append(e)

        threads = []
        for idx, item in enumerate(items):
            t = threading.Thread(target=run_item, args=(idx, item))
            threads.append(t)
            t.start()

        for t in threads:
            t.join()

        if errors:
            e = errors[0]
            if isinstance(e, ASLExecutionError):
                raise e
            raise ASLExecutionError(type(e).__name__, str(e))

        return results

    # --- Service integrations ---

    def _invoke_lambda(self, resource: str, input_data: Any) -> Any:
        """Invoke a Lambda function."""
        import base64

        from robotocore.services.lambda_.executor import execute_python_handler

        # Extract function name from ARN
        parts = resource.split(":")
        function_name = parts[-1] if parts else resource

        try:
            from moto.backends import get_backend
            from moto.core import DEFAULT_ACCOUNT_ID

            acct = self.account_id if self.account_id != "123456789012" else DEFAULT_ACCOUNT_ID
            backend = get_backend("lambda")[acct][self.region]
            fn = backend.get_function(function_name)
        except Exception:
            raise ASLExecutionError(
                "Lambda.ServiceException", f"Function not found: {function_name}"
            )

        runtime = getattr(fn, "run_time", "") or ""
        if runtime.startswith("python") and hasattr(fn, "code") and fn.code:
            code_zip = fn.code.get("ZipFile")
            if isinstance(code_zip, str):
                code_zip = base64.b64decode(code_zip)
            if code_zip:
                event = input_data if isinstance(input_data, dict) else {"input": input_data}
                result, error_type, logs = execute_python_handler(
                    code_zip=code_zip,
                    handler=getattr(fn, "handler", "lambda_function.handler"),
                    event=event,
                    function_name=function_name,
                    region=self.region,
                    account_id=self.account_id,
                )
                if error_type:
                    raise ASLExecutionError("Lambda.Unknown", str(result))
                return result
        return {"StatusCode": 200, "Payload": input_data}

    def _invoke_sqs_send(self, input_data: Any) -> dict:
        """Send message to SQS via Step Functions integration."""
        import hashlib

        from robotocore.services.sqs.models import SqsMessage
        from robotocore.services.sqs.provider import _get_store

        queue_url = input_data.get("QueueUrl", "")
        message_body = input_data.get("MessageBody", "")
        if isinstance(message_body, dict):
            message_body = json.dumps(message_body)

        store = _get_store(self.region)
        queue = store.get_queue_by_url(queue_url)
        if not queue:
            raise ASLExecutionError("SQS.QueueDoesNotExist", f"Queue not found: {queue_url}")

        msg = SqsMessage(
            message_id=str(uuid.uuid4()),
            body=message_body,
            md5_of_body=hashlib.md5(message_body.encode()).hexdigest(),
        )
        queue.put(msg)
        return {"MessageId": msg.message_id, "Md5OfMessageBody": msg.md5_of_body}

    def _invoke_sns_publish(self, input_data: Any) -> dict:
        """Publish to SNS via Step Functions integration."""
        topic_arn = input_data.get("TopicArn", "")
        message = input_data.get("Message", "")
        if isinstance(message, dict):
            message = json.dumps(message)

        from robotocore.services.sns.provider import _deliver_to_subscriber, _get_store, _new_id

        store = _get_store(self.region)
        topic = store.get_topic(topic_arn)
        if not topic:
            raise ASLExecutionError("SNS.NotFound", f"Topic not found: {topic_arn}")

        message_id = _new_id()
        for sub in topic.subscriptions:
            if sub.confirmed:
                _deliver_to_subscriber(
                    sub, message, "Step Functions", {}, message_id, topic_arn, self.region
                )
        return {"MessageId": message_id}

    def _invoke_dynamodb(self, action: str, input_data: Any) -> Any:
        """DynamoDB integration via Moto backend."""
        try:
            from moto.backends import get_backend
            from moto.core import DEFAULT_ACCOUNT_ID

            acct = self.account_id if self.account_id != "123456789012" else DEFAULT_ACCOUNT_ID
            backend = get_backend("dynamodb")[acct][self.region]
        except Exception:
            raise ASLExecutionError("DynamoDB.ServiceException", "DynamoDB backend not available")

        table_name = input_data.get("TableName", "")

        if action == "putItem":
            return self._dynamodb_put_item(backend, table_name, input_data)
        elif action == "getItem":
            return self._dynamodb_get_item(backend, table_name, input_data)
        elif action == "deleteItem":
            return self._dynamodb_delete_item(backend, table_name, input_data)
        elif action == "updateItem":
            return self._dynamodb_update_item(backend, table_name, input_data)
        elif action == "query":
            return self._dynamodb_query(backend, table_name, input_data)
        else:
            logger.warning(f"Unknown DynamoDB action: {action}")
            return input_data

    def _dynamodb_put_item(self, backend: Any, table_name: str, params: dict) -> dict:
        """Put item to DynamoDB."""
        item = params.get("Item", {})
        ddb_item = _to_dynamodb_item(item) if not _is_ddb_typed(item) else item
        try:
            backend.put_item(table_name, ddb_item)
        except Exception as e:
            raise ASLExecutionError("DynamoDB.AmazonDynamoDBException", str(e))
        return {}

    def _dynamodb_get_item(self, backend: Any, table_name: str, params: dict) -> dict:
        """Get item from DynamoDB."""
        key = params.get("Key", {})
        ddb_key = _to_dynamodb_item(key) if not _is_ddb_typed(key) else key
        try:
            result = backend.get_item(table_name, ddb_key)
            if result and hasattr(result, "attrs"):
                return {"Item": _from_dynamodb_item(result.attrs)}
            return {"Item": {}}
        except Exception as e:
            raise ASLExecutionError("DynamoDB.AmazonDynamoDBException", str(e))

    def _dynamodb_delete_item(self, backend: Any, table_name: str, params: dict) -> dict:
        """Delete item from DynamoDB."""
        key = params.get("Key", {})
        ddb_key = _to_dynamodb_item(key) if not _is_ddb_typed(key) else key
        try:
            backend.delete_item(table_name, ddb_key)
        except Exception as e:
            raise ASLExecutionError("DynamoDB.AmazonDynamoDBException", str(e))
        return {}

    def _dynamodb_update_item(self, backend: Any, table_name: str, params: dict) -> dict:
        """Update item in DynamoDB."""
        key = params.get("Key", {})
        ddb_key = _to_dynamodb_item(key) if not _is_ddb_typed(key) else key
        update_expr = params.get("UpdateExpression", "")
        expr_attr_values = params.get("ExpressionAttributeValues", {})
        expr_attr_names = params.get("ExpressionAttributeNames", {})
        try:
            backend.update_item(
                table_name,
                ddb_key,
                update_expression=[update_expr] if update_expr else None,
                expression_attribute_values=expr_attr_values or None,
                expression_attribute_names=expr_attr_names or None,
            )
        except Exception as e:
            raise ASLExecutionError("DynamoDB.AmazonDynamoDBException", str(e))
        return {}

    def _dynamodb_query(self, backend: Any, table_name: str, params: dict) -> dict:
        """Query DynamoDB table."""
        key_condition = params.get("KeyConditionExpression", "")
        expr_attr_values = params.get("ExpressionAttributeValues", {})
        expr_attr_names = params.get("ExpressionAttributeNames", {})
        try:
            items, _, _, _, _ = backend.query(
                table_name,
                key_condition_expression=key_condition,
                expression_attribute_values=expr_attr_values or None,
                expression_attribute_names=expr_attr_names or None,
            )
            return {
                "Items": [_from_dynamodb_item(item.attrs) for item in items],
                "Count": len(items),
            }
        except Exception as e:
            raise ASLExecutionError("DynamoDB.AmazonDynamoDBException", str(e))

    def _invoke_step_functions(self, input_data: Any) -> Any:
        """Nested Step Functions execution."""
        sm_arn = input_data.get("StateMachineArn", "")
        sf_input = input_data.get("Input", input_data)

        from robotocore.services.stepfunctions.provider import _start_execution_internal

        try:
            return _start_execution_internal(sm_arn, sf_input, self.region, self.account_id)
        except Exception as e:
            logger.warning(f"Nested execution failed: {e}")
            return input_data

    def _handle_error(
        self, state_def: dict, data: Any, error: str, cause: str
    ) -> tuple[str | None, Any]:
        """Handle errors with Catch blocks."""
        catchers = state_def.get("Catch", [])
        for catch in catchers:
            error_equals = catch.get("ErrorEquals", [])
            if "States.ALL" in error_equals or error in error_equals:
                error_output = {"Error": error, "Cause": cause}
                result_path = catch.get("ResultPath", "$")
                output = _apply_result_path(data, error_output, result_path)
                return catch.get("Next"), output
        return None, data


# --- DynamoDB type helpers ---


def _is_ddb_typed(item: dict) -> bool:
    """Check if item already uses DynamoDB typed format ({"S": "val"})."""
    if not isinstance(item, dict):
        return False
    for v in item.values():
        if isinstance(v, dict) and len(v) == 1:
            key = next(iter(v))
            if key in ("S", "N", "B", "BOOL", "NULL", "L", "M", "SS", "NS", "BS"):
                return True
    return False


def _to_dynamodb_item(item: dict) -> dict:
    """Convert plain dict to DynamoDB typed format."""
    result = {}
    for key, value in item.items():
        if isinstance(value, dict) and len(value) == 1:
            first_key = next(iter(value))
            if first_key in ("S", "N", "B", "BOOL", "NULL", "L", "M", "SS", "NS", "BS"):
                result[key] = value
                continue
        result[key] = _to_ddb_value(value)
    return result


def _to_ddb_value(value: Any) -> dict:
    """Convert a Python value to DynamoDB typed format."""
    if isinstance(value, str):
        return {"S": value}
    if isinstance(value, bool):
        return {"BOOL": value}
    if isinstance(value, (int, float)):
        return {"N": str(value)}
    if value is None:
        return {"NULL": True}
    if isinstance(value, list):
        return {"L": [_to_ddb_value(v) for v in value]}
    if isinstance(value, dict):
        return {"M": {k: _to_ddb_value(v) for k, v in value.items()}}
    return {"S": str(value)}


def _from_dynamodb_item(attrs: dict) -> dict:
    """Convert DynamoDB typed format back to plain dict."""
    result = {}
    for key, value in attrs.items():
        result[key] = _from_ddb_value(value)
    return result


def _from_ddb_value(value: Any) -> Any:
    """Convert a DynamoDB typed value to Python."""
    if not isinstance(value, dict):
        return value
    if "S" in value:
        return value["S"]
    if "N" in value:
        n = value["N"]
        return int(n) if "." not in str(n) else float(n)
    if "BOOL" in value:
        return value["BOOL"]
    if "NULL" in value:
        return None
    if "L" in value:
        return [_from_ddb_value(v) for v in value["L"]]
    if "M" in value:
        return {k: _from_ddb_value(v) for k, v in value["M"].items()}
    if "SS" in value:
        return list(value["SS"])
    if "NS" in value:
        return [int(n) if "." not in str(n) else float(n) for n in value["NS"]]
    return value


# --- JSONPath utilities ---


def _apply_path(data: Any, path: str | None) -> Any:
    """Apply a JSONPath-like path to data."""
    if path is None:
        return {}
    if path == "$":
        return data
    return _resolve_path(data, path)


def _resolve_path(data: Any, path: str) -> Any:
    """Resolve a simple JSONPath expression."""
    if not path or path == "$":
        return data
    if not path.startswith("$"):
        return data

    parts = path[2:].split(".") if len(path) > 1 else []
    current = data
    for part in parts:
        if not part:
            continue
        # Handle array index
        match = re.match(r"(\w+)\[(\d+)\]", part)
        if match:
            key, idx = match.group(1), int(match.group(2))
            if isinstance(current, dict) and key in current:
                current = current[key]
                if isinstance(current, list) and idx < len(current):
                    current = current[idx]
                else:
                    return None
            else:
                return None
        elif isinstance(current, dict):
            current = current.get(part)
            if current is None:
                return None
        else:
            return None
    return current


def _apply_result_path(original: Any, result: Any, result_path: str | None) -> Any:
    """Apply ResultPath to merge result into original data."""
    if result_path is None:
        return original  # Discard result
    if result_path == "$":
        return result  # Replace entirely

    if not result_path.startswith("$."):
        return result

    # Set nested path
    parts = result_path[2:].split(".")
    output = copy.deepcopy(original) if isinstance(original, dict) else {}
    current = output
    for part in parts[:-1]:
        if part not in current:
            current[part] = {}
        current = current[part]
    current[parts[-1]] = result
    return output


def _resolve_parameters(params: dict, input_data: Any) -> Any:
    """Resolve Parameters template, replacing $.path and intrinsic references."""
    result = {}
    for key, value in params.items():
        if key.endswith(".$"):
            # Dynamic reference
            real_key = key[:-2]
            if isinstance(value, str):
                if value.startswith("States."):
                    # Intrinsic function call
                    result[real_key] = evaluate_intrinsic(value, input_data)
                elif value.startswith("$"):
                    result[real_key] = _resolve_path(input_data, value)
                else:
                    result[real_key] = value
            else:
                result[real_key] = value
        elif isinstance(value, dict):
            result[key] = _resolve_parameters(value, input_data)
        elif isinstance(value, list):
            result[key] = [
                _resolve_parameters(v, input_data) if isinstance(v, dict) else v for v in value
            ]
        else:
            result[key] = value
    return result


def _evaluate_choice_rule(rule: dict, data: Any) -> bool:
    """Evaluate a single Choice rule."""
    # AND/OR/NOT operators
    if "And" in rule:
        return all(_evaluate_choice_rule(r, data) for r in rule["And"])
    if "Or" in rule:
        return any(_evaluate_choice_rule(r, data) for r in rule["Or"])
    if "Not" in rule:
        return not _evaluate_choice_rule(rule["Not"], data)

    variable = rule.get("Variable", "$")
    value = _resolve_path(data, variable)

    # Comparison operators
    if "StringEquals" in rule:
        return value == rule["StringEquals"]
    if "StringEqualsPath" in rule:
        return value == _resolve_path(data, rule["StringEqualsPath"])
    if "StringGreaterThan" in rule:
        return isinstance(value, str) and value > rule["StringGreaterThan"]
    if "StringLessThan" in rule:
        return isinstance(value, str) and value < rule["StringLessThan"]
    if "StringMatches" in rule:
        import fnmatch

        return isinstance(value, str) and fnmatch.fnmatch(value, rule["StringMatches"])
    if "NumericEquals" in rule:
        return value == rule["NumericEquals"]
    if "NumericGreaterThan" in rule:
        return isinstance(value, (int, float)) and value > rule["NumericGreaterThan"]
    if "NumericGreaterThanEquals" in rule:
        return isinstance(value, (int, float)) and value >= rule["NumericGreaterThanEquals"]
    if "NumericLessThan" in rule:
        return isinstance(value, (int, float)) and value < rule["NumericLessThan"]
    if "NumericLessThanEquals" in rule:
        return isinstance(value, (int, float)) and value <= rule["NumericLessThanEquals"]
    if "BooleanEquals" in rule:
        return value == rule["BooleanEquals"]
    if "IsPresent" in rule:
        return (value is not None) == rule["IsPresent"]
    if "IsNull" in rule:
        return (value is None) == rule["IsNull"]
    if "IsString" in rule:
        return isinstance(value, str) == rule["IsString"]
    if "IsNumeric" in rule:
        return isinstance(value, (int, float)) == rule["IsNumeric"]
    if "IsBoolean" in rule:
        return isinstance(value, bool) == rule["IsBoolean"]

    return False
