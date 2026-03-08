"""Execution history events for Step Functions.

Tracks full lifecycle events for each execution, matching the AWS
GetExecutionHistory API response format.
"""

import time
from typing import Any


class ExecutionHistory:
    """Collects events for a single state machine execution."""

    def __init__(self, execution_arn: str):
        self.execution_arn = execution_arn
        self.events: list[dict[str, Any]] = []
        self._event_id = 0

    def _next_id(self) -> int:
        self._event_id += 1
        return self._event_id

    def _add_event(
        self,
        event_type: str,
        previous_event_id: int = 0,
        details: dict | None = None,
    ) -> int:
        event_id = self._next_id()
        event: dict[str, Any] = {
            "timestamp": time.time(),
            "type": event_type,
            "id": event_id,
            "previousEventId": previous_event_id,
        }
        if details:
            # AWS uses a camelCase key for details, e.g. "executionStartedEventDetails"
            detail_key = _detail_key(event_type)
            event[detail_key] = details
        self.events.append(event)
        return event_id

    # --- Execution lifecycle ---

    def execution_started(self, input_data: str, role_arn: str = "") -> int:
        return self._add_event(
            "ExecutionStarted",
            details={"input": input_data, "roleArn": role_arn},
        )

    def execution_succeeded(self, output: str, prev_id: int = 0) -> int:
        return self._add_event(
            "ExecutionSucceeded",
            previous_event_id=prev_id,
            details={"output": output},
        )

    def execution_failed(self, error: str, cause: str, prev_id: int = 0) -> int:
        return self._add_event(
            "ExecutionFailed",
            previous_event_id=prev_id,
            details={"error": error, "cause": cause},
        )

    def execution_aborted(self, prev_id: int = 0) -> int:
        return self._add_event(
            "ExecutionAborted",
            previous_event_id=prev_id,
        )

    def execution_timed_out(self, prev_id: int = 0) -> int:
        return self._add_event(
            "ExecutionTimedOut",
            previous_event_id=prev_id,
        )

    # --- State lifecycle ---

    def state_entered(self, name: str, state_type: str, input_data: str, prev_id: int = 0) -> int:
        event_type = _state_entered_type(state_type)
        return self._add_event(
            event_type,
            previous_event_id=prev_id,
            details={"name": name, "input": input_data},
        )

    def state_exited(self, name: str, state_type: str, output: str, prev_id: int = 0) -> int:
        event_type = _state_exited_type(state_type)
        return self._add_event(
            event_type,
            previous_event_id=prev_id,
            details={"name": name, "output": output},
        )

    # --- Task lifecycle ---

    def task_scheduled(
        self,
        resource: str,
        parameters: str = "{}",
        prev_id: int = 0,
    ) -> int:
        return self._add_event(
            "TaskScheduled",
            previous_event_id=prev_id,
            details={
                "resourceType": _resource_type(resource),
                "resource": resource,
                "parameters": parameters,
            },
        )

    def task_started(self, resource: str, prev_id: int = 0) -> int:
        return self._add_event(
            "TaskStarted",
            previous_event_id=prev_id,
            details={
                "resourceType": _resource_type(resource),
                "resource": resource,
            },
        )

    def task_succeeded(self, resource: str, output: str, prev_id: int = 0) -> int:
        return self._add_event(
            "TaskSucceeded",
            previous_event_id=prev_id,
            details={
                "resourceType": _resource_type(resource),
                "resource": resource,
                "output": output,
            },
        )

    def task_failed(self, resource: str, error: str, cause: str, prev_id: int = 0) -> int:
        return self._add_event(
            "TaskFailed",
            previous_event_id=prev_id,
            details={
                "resourceType": _resource_type(resource),
                "resource": resource,
                "error": error,
                "cause": cause,
            },
        )

    def get_events(self, reverse_order: bool = False) -> list[dict]:
        """Return the list of events, optionally in reverse chronological order."""
        events = list(self.events)
        if reverse_order:
            events.reverse()
        return events


# --- Helpers ---


def _detail_key(event_type: str) -> str:
    """Convert event type to the detail key used by AWS.

    E.g. "ExecutionStarted" -> "executionStartedEventDetails"
    """
    return event_type[0].lower() + event_type[1:] + "EventDetails"


def _state_entered_type(state_type: str) -> str:
    """Map state type to entered event type."""
    mapping = {
        "Task": "TaskStateEntered",
        "Pass": "PassStateEntered",
        "Choice": "ChoiceStateEntered",
        "Wait": "WaitStateEntered",
        "Succeed": "SucceedStateEntered",
        "Fail": "FailStateEntered",
        "Parallel": "ParallelStateEntered",
        "Map": "MapStateEntered",
    }
    return mapping.get(state_type, f"{state_type}StateEntered")


def _state_exited_type(state_type: str) -> str:
    """Map state type to exited event type."""
    mapping = {
        "Task": "TaskStateExited",
        "Pass": "PassStateExited",
        "Choice": "ChoiceStateExited",
        "Wait": "WaitStateExited",
        "Succeed": "SucceedStateExited",
        "Fail": "FailStateExited",
        "Parallel": "ParallelStateExited",
        "Map": "MapStateExited",
    }
    return mapping.get(state_type, f"{state_type}StateExited")


def _resource_type(resource: str) -> str:
    """Extract resource type from resource ARN."""
    if "lambda" in resource:
        return "lambda"
    if "sqs" in resource:
        return "sqs"
    if "sns" in resource:
        return "sns"
    if "dynamodb" in resource:
        return "dynamodb"
    if "states" in resource:
        return "states"
    return "unknown"
