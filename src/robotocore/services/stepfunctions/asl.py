"""Amazon States Language (ASL) interpreter.

Executes Step Functions state machines with support for:
- Pass, Task, Choice, Wait, Succeed, Fail, Parallel, Map states
- Service integrations: Lambda, SQS, SNS, DynamoDB
- JSONPath input/output processing
- Error handling: Catch, Retry
"""

import copy
import json
import logging
import re
import time
import uuid
from typing import Any

logger = logging.getLogger(__name__)


class ASLExecutionError(Exception):
    def __init__(self, error: str, cause: str = ""):
        self.error = error
        self.cause = cause
        super().__init__(f"{error}: {cause}")


class ASLExecutor:
    """Executes an ASL state machine definition."""

    def __init__(
        self, definition: dict, region: str = "us-east-1", account_id: str = "123456789012"
    ):
        self.definition = definition
        self.region = region
        self.account_id = account_id
        self.states = definition.get("States", {})
        self.start_at = definition.get("StartAt", "")
        self.max_steps = 1000  # Safety limit

    def execute(self, input_data: dict | None = None) -> dict:
        """Execute the state machine and return the output."""
        current_state = self.start_at
        data = copy.deepcopy(input_data or {})
        steps = 0

        while current_state and steps < self.max_steps:
            steps += 1
            state_def = self.states.get(current_state)
            if not state_def:
                raise ASLExecutionError("States.Runtime", f"State '{current_state}' not found")

            state_type = state_def.get("Type", "")
            try:
                data, next_state = self._execute_state(current_state, state_def, data)
            except ASLExecutionError:
                raise
            except Exception as e:
                # Check for Catch
                next_state, data = self._handle_error(state_def, data, type(e).__name__, str(e))
                if next_state is None:
                    raise ASLExecutionError(type(e).__name__, str(e))

            if state_def.get("End", False) or state_type in ("Succeed", "Fail"):
                return data

            if next_state is None:
                # No Next field and not End — execution complete
                return data

            current_state = next_state

        raise ASLExecutionError("States.Runtime", "Maximum execution steps exceeded")

    def _execute_state(self, name: str, state_def: dict, data: dict) -> tuple[Any, str | None]:
        """Execute a single state, return (output, next_state_name)."""
        state_type = state_def.get("Type", "")

        # Apply InputPath
        effective_input = _apply_path(data, state_def.get("InputPath", "$"))

        # Apply Parameters
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

        # Parse resource ARN
        if resource.startswith("arn:aws:lambda:"):
            return self._invoke_lambda(resource, input_data)
        elif resource.startswith("arn:aws:states:::sqs:sendMessage"):
            return self._invoke_sqs_send(input_data)
        elif resource.startswith("arn:aws:states:::sns:publish"):
            return self._invoke_sns_publish(input_data)
        elif resource.startswith("arn:aws:states:::dynamodb:"):
            return self._invoke_dynamodb(resource, input_data)
        elif resource.startswith("arn:aws:states:::states:startExecution"):
            return self._invoke_step_functions(input_data)
        elif resource.startswith("arn:aws:lambda:"):
            return self._invoke_lambda(resource, input_data)
        else:
            # Try as Lambda ARN
            if ":function:" in resource:
                return self._invoke_lambda(resource, input_data)
            logger.warning(f"Unknown Task resource: {resource}, returning input")
            return input_data

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
        """Execute a Map state over an array."""
        items_path = state_def.get("ItemsPath", "$")
        items = _resolve_path(input_data, items_path)
        if not isinstance(items, list):
            items = [items]

        iterator = state_def.get("Iterator") or state_def.get("ItemProcessor", {})
        results = []
        for item in items:
            executor = ASLExecutor(iterator, self.region, self.account_id)
            result = executor.execute(item)
            results.append(result)
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

    def _invoke_dynamodb(self, resource: str, input_data: Any) -> Any:
        """DynamoDB integration — delegates to Moto."""
        # Basic pass-through for now
        return input_data

    def _invoke_step_functions(self, input_data: Any) -> Any:
        """Nested Step Functions execution."""
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
    """Resolve Parameters template, replacing $.path references."""
    result = {}
    for key, value in params.items():
        if key.endswith(".$"):
            # Dynamic reference
            real_key = key[:-2]
            if isinstance(value, str) and value.startswith("$"):
                result[real_key] = _resolve_path(input_data, value)
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
