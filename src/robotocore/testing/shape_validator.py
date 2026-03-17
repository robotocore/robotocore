"""Recursive response shape validator against botocore output shapes.

Walks the botocore service model's output shape for an operation and
compares it recursively against an actual response dict, catching:
  - Missing required keys
  - Missing optional keys (warnings)
  - Type mismatches
  - Extra keys not in the model
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

import botocore.session

# Map botocore type names to Python types for validation
BOTOCORE_TO_PYTHON = {
    "string": (str,),
    "integer": (int,),
    "long": (int,),
    "float": (float, int),
    "double": (float, int),
    "boolean": (bool,),
    "timestamp": (str,),  # boto3 deserializes to datetime, but we accept str too
    "blob": (str, bytes),
    "list": (list,),
    "structure": (dict,),
    "map": (dict,),
}


@dataclass
class ShapeViolation:
    """A single mismatch between botocore shape and actual response."""

    path: str  # e.g. "SendMessageResult.MessageId"
    issue: str  # "missing_required" | "missing_optional" | "type_mismatch" | "extra_key"
    expected: str  # expected type or key name
    actual: str | None  # actual type or None if missing
    severity: str  # "error" | "warning" | "info"

    def __str__(self) -> str:
        if self.issue == "missing_required":
            return f"ERROR {self.path}: missing required key (expected {self.expected})"
        elif self.issue == "missing_optional":
            return f"WARN  {self.path}: missing optional key (expected {self.expected})"
        elif self.issue == "type_mismatch":
            return f"ERROR {self.path}: type mismatch (expected {self.expected}, got {self.actual})"
        elif self.issue == "extra_key":
            return f"INFO  {self.path}: extra key not in model"
        return f"{self.severity.upper()} {self.path}: {self.issue}"


@dataclass
class ShapeValidationResult:
    """Result of validating a response against its botocore output shape."""

    service: str
    operation: str
    violations: list[ShapeViolation] = field(default_factory=list)
    skipped: bool = False
    skip_reason: str = ""

    @property
    def errors(self) -> list[ShapeViolation]:
        return [v for v in self.violations if v.severity == "error"]

    @property
    def warnings(self) -> list[ShapeViolation]:
        return [v for v in self.violations if v.severity == "warning"]

    @property
    def passed(self) -> bool:
        return not self.errors

    def summary(self) -> str:
        if self.skipped:
            return f"{self.operation}: SKIP ({self.skip_reason})"
        n_err = len(self.errors)
        n_warn = len(self.warnings)
        if n_err == 0 and n_warn == 0:
            return f"{self.operation}: PASS"
        parts = []
        if n_err:
            parts.append(f"{n_err} error(s)")
        if n_warn:
            parts.append(f"{n_warn} warning(s)")
        status = "FAIL" if n_err else "WARN"
        return f"{self.operation}: {status} — {', '.join(parts)}"


def _python_type_name(value: Any) -> str:
    """Get a human-readable type name for a Python value."""
    if value is None:
        return "null"
    return type(value).__name__


def _botocore_type_label(shape) -> str:
    """Get a label for a botocore shape type."""
    return shape.type_name


def validate_shape(
    shape,
    actual_value: Any,
    path: str = "",
    *,
    check_optional: bool = True,
) -> list[ShapeViolation]:
    """Recursively walk botocore output shape vs actual response.

    Args:
        shape: A botocore Shape object (from operation_model.output_shape)
        actual_value: The actual response value to validate
        path: Dot-separated path for error messages
        check_optional: Whether to report missing optional keys as warnings
    """
    violations: list[ShapeViolation] = []

    if shape is None:
        return violations

    type_name = shape.type_name

    if type_name == "structure":
        _validate_structure(shape, actual_value, path, violations, check_optional)
    elif type_name == "list":
        _validate_list(shape, actual_value, path, violations, check_optional)
    elif type_name == "map":
        _validate_map(shape, actual_value, path, violations, check_optional)
    else:
        _validate_scalar(shape, actual_value, path, violations)

    return violations


def _validate_structure(
    shape,
    actual: Any,
    path: str,
    violations: list[ShapeViolation],
    check_optional: bool,
) -> None:
    """Validate a structure shape against a dict value."""
    if not isinstance(actual, dict):
        violations.append(
            ShapeViolation(
                path=path or "(root)",
                issue="type_mismatch",
                expected="structure",
                actual=_python_type_name(actual),
                severity="error",
            )
        )
        return

    if not hasattr(shape, "members"):
        return

    required = set(getattr(shape, "required_members", []))

    for member_name, member_shape in shape.members.items():
        # Skip members serialized as headers or status code — they're not in the body
        location = member_shape.serialization.get("location", "")
        if location in ("header", "headers", "statusCode"):
            continue

        member_path = f"{path}.{member_name}" if path else member_name

        if member_name not in actual:
            if member_name in required:
                violations.append(
                    ShapeViolation(
                        path=member_path,
                        issue="missing_required",
                        expected=_botocore_type_label(member_shape),
                        actual=None,
                        severity="error",
                    )
                )
            elif check_optional:
                violations.append(
                    ShapeViolation(
                        path=member_path,
                        issue="missing_optional",
                        expected=_botocore_type_label(member_shape),
                        actual=None,
                        severity="warning",
                    )
                )
        else:
            # Recurse into present members
            violations.extend(
                validate_shape(
                    member_shape, actual[member_name], member_path, check_optional=check_optional
                )
            )

    # Check for extra keys not in the model
    model_keys = set(shape.members.keys()) if hasattr(shape, "members") else set()
    for key in actual:
        if key not in model_keys:
            member_path = f"{path}.{key}" if path else key
            violations.append(
                ShapeViolation(
                    path=member_path,
                    issue="extra_key",
                    expected="(not in model)",
                    actual=_python_type_name(actual[key]),
                    severity="info",
                )
            )


def _validate_list(
    shape,
    actual: Any,
    path: str,
    violations: list[ShapeViolation],
    check_optional: bool,
) -> None:
    """Validate a list shape against a list value."""
    if not isinstance(actual, list):
        violations.append(
            ShapeViolation(
                path=path or "(root)",
                issue="type_mismatch",
                expected="list",
                actual=_python_type_name(actual),
                severity="error",
            )
        )
        return

    # Validate first element if present (spot check)
    if actual and hasattr(shape, "member"):
        violations.extend(
            validate_shape(shape.member, actual[0], f"{path}[0]", check_optional=check_optional)
        )


def _validate_map(
    shape,
    actual: Any,
    path: str,
    violations: list[ShapeViolation],
    check_optional: bool,
) -> None:
    """Validate a map shape against a dict value."""
    if not isinstance(actual, dict):
        violations.append(
            ShapeViolation(
                path=path or "(root)",
                issue="type_mismatch",
                expected="map",
                actual=_python_type_name(actual),
                severity="error",
            )
        )
        return

    # Validate first value if present (spot check)
    if actual and hasattr(shape, "value"):
        first_key = next(iter(actual))
        violations.extend(
            validate_shape(
                shape.value, actual[first_key], f"{path}.{first_key}", check_optional=check_optional
            )
        )


def _validate_scalar(
    shape,
    actual: Any,
    path: str,
    violations: list[ShapeViolation],
) -> None:
    """Validate a scalar shape (string, integer, boolean, etc.)."""
    if actual is None:
        # None is acceptable for optional scalars (handled by caller)
        return

    expected_types = BOTOCORE_TO_PYTHON.get(shape.type_name)
    if expected_types is None:
        # Unknown shape type — skip
        return

    # Special case: boto3 deserializes timestamps to datetime objects
    import datetime

    if shape.type_name == "timestamp" and isinstance(actual, datetime.datetime):
        return

    if not isinstance(actual, expected_types):
        violations.append(
            ShapeViolation(
                path=path,
                issue="type_mismatch",
                expected=shape.type_name,
                actual=_python_type_name(actual),
                severity="error",
            )
        )


def validate_operation_response(
    service_name: str,
    operation_name: str,
    response: dict,
    *,
    check_optional: bool = True,
) -> ShapeValidationResult:
    """Validate a boto3 response dict against the botocore output shape.

    Args:
        service_name: AWS service name (e.g. 's3', 'dynamodb')
        operation_name: Operation name (e.g. 'ListBuckets')
        response: The response dict from boto3
        check_optional: Whether to report missing optional keys
    """
    result = ShapeValidationResult(service=service_name, operation=operation_name)

    session = botocore.session.get_session()
    try:
        model = session.get_service_model(service_name)
        op_model = model.operation_model(operation_name)
    except Exception as e:
        result.skipped = True
        result.skip_reason = f"cannot load model: {e}"
        return result

    output_shape = op_model.output_shape
    if output_shape is None:
        # No output shape means the operation returns nothing
        return result

    # Strip ResponseMetadata — it's added by boto3, not part of the model
    body = {k: v for k, v in response.items() if k != "ResponseMetadata"}

    result.violations = validate_shape(output_shape, body, check_optional=check_optional)
    return result


def get_output_shape(service_name: str, operation_name: str):
    """Get the botocore output shape for an operation.

    Returns None if the operation has no output shape.
    """
    session = botocore.session.get_session()
    model = session.get_service_model(service_name)
    op_model = model.operation_model(operation_name)
    return op_model.output_shape
