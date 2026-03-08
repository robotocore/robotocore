"""IAM condition operators for policy evaluation.

Implements all standard IAM condition operators including string, numeric,
date, boolean, IP, ARN, and null checks, plus set operator prefixes
(ForAllValues:, ForAnyValue:) and IfExists suffix.
"""

from __future__ import annotations

import fnmatch
import ipaddress
from datetime import UTC, datetime
from typing import Any


def _to_str(value: Any) -> str:
    if isinstance(value, bool):
        return "true" if value else "false"
    return str(value)


def _to_float(value: Any) -> float:
    return float(value)


def _to_datetime(value: Any) -> datetime:
    if isinstance(value, datetime):
        return value
    s = str(value)
    # Try ISO 8601 formats
    for fmt in (
        "%Y-%m-%dT%H:%M:%S%z",
        "%Y-%m-%dT%H:%M:%SZ",
        "%Y-%m-%dT%H:%M:%S.%f%z",
        "%Y-%m-%dT%H:%M:%S.%fZ",
        "%Y-%m-%dT%H:%M:%S",
    ):
        try:
            dt = datetime.strptime(s, fmt)
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=UTC)
            return dt
        except ValueError:
            continue
    raise ValueError(f"Cannot parse date: {s}")


# ---------------------------------------------------------------------------
# Core operator functions
# ---------------------------------------------------------------------------


def _string_equals(context_val: str, policy_val: str) -> bool:
    return _to_str(context_val) == _to_str(policy_val)


def _string_not_equals(context_val: str, policy_val: str) -> bool:
    return _to_str(context_val) != _to_str(policy_val)


def _string_equals_ignore_case(context_val: str, policy_val: str) -> bool:
    return _to_str(context_val).lower() == _to_str(policy_val).lower()


def _string_not_equals_ignore_case(context_val: str, policy_val: str) -> bool:
    return _to_str(context_val).lower() != _to_str(policy_val).lower()


def _string_like(context_val: str, policy_val: str) -> bool:
    return fnmatch.fnmatch(_to_str(context_val), _to_str(policy_val))


def _string_not_like(context_val: str, policy_val: str) -> bool:
    return not fnmatch.fnmatch(_to_str(context_val), _to_str(policy_val))


def _numeric_equals(context_val: Any, policy_val: Any) -> bool:
    return _to_float(context_val) == _to_float(policy_val)


def _numeric_not_equals(context_val: Any, policy_val: Any) -> bool:
    return _to_float(context_val) != _to_float(policy_val)


def _numeric_less_than(context_val: Any, policy_val: Any) -> bool:
    return _to_float(context_val) < _to_float(policy_val)


def _numeric_less_than_equals(context_val: Any, policy_val: Any) -> bool:
    return _to_float(context_val) <= _to_float(policy_val)


def _numeric_greater_than(context_val: Any, policy_val: Any) -> bool:
    return _to_float(context_val) > _to_float(policy_val)


def _numeric_greater_than_equals(context_val: Any, policy_val: Any) -> bool:
    return _to_float(context_val) >= _to_float(policy_val)


def _date_equals(context_val: Any, policy_val: Any) -> bool:
    return _to_datetime(context_val) == _to_datetime(policy_val)


def _date_not_equals(context_val: Any, policy_val: Any) -> bool:
    return _to_datetime(context_val) != _to_datetime(policy_val)


def _date_less_than(context_val: Any, policy_val: Any) -> bool:
    return _to_datetime(context_val) < _to_datetime(policy_val)


def _date_less_than_equals(context_val: Any, policy_val: Any) -> bool:
    return _to_datetime(context_val) <= _to_datetime(policy_val)


def _date_greater_than(context_val: Any, policy_val: Any) -> bool:
    return _to_datetime(context_val) > _to_datetime(policy_val)


def _date_greater_than_equals(context_val: Any, policy_val: Any) -> bool:
    return _to_datetime(context_val) >= _to_datetime(policy_val)


def _bool_op(context_val: Any, policy_val: Any) -> bool:
    cv = _to_str(context_val).lower()
    pv = _to_str(policy_val).lower()
    return cv == pv


def _ip_address(context_val: Any, policy_val: Any) -> bool:
    addr = ipaddress.ip_address(str(context_val))
    network = ipaddress.ip_network(str(policy_val), strict=False)
    return addr in network


def _not_ip_address(context_val: Any, policy_val: Any) -> bool:
    return not _ip_address(context_val, policy_val)


def _arn_match(arn: str, pattern: str, *, wildcard: bool) -> bool:
    """Match two ARNs section by section.

    When wildcard is True, each section of the pattern supports * globs.
    When wildcard is False, sections must match exactly.

    ARN format: arn:partition:service:region:account:resource
    The resource portion (everything after the 5th colon) may contain
    additional colons, so we rejoin those into a single resource string.
    """
    arn_parts = str(arn).split(":")
    pattern_parts = str(pattern).split(":")
    if len(arn_parts) < 6 or len(pattern_parts) < 6:
        return False
    # Rejoin the resource portion (segments 5+) into a single string
    arn_fixed = arn_parts[:5] + [":".join(arn_parts[5:])]
    pattern_fixed = pattern_parts[:5] + [":".join(pattern_parts[5:])]
    for a, p in zip(arn_fixed, pattern_fixed):
        if wildcard:
            if not fnmatch.fnmatch(a, p):
                return False
        else:
            if a != p:
                return False
    return True


def _arn_equals(context_val: Any, policy_val: Any) -> bool:
    return _arn_match(str(context_val), str(policy_val), wildcard=False)


def _arn_not_equals(context_val: Any, policy_val: Any) -> bool:
    return not _arn_equals(context_val, policy_val)


def _arn_like(context_val: Any, policy_val: Any) -> bool:
    return _arn_match(str(context_val), str(policy_val), wildcard=True)


def _arn_not_like(context_val: Any, policy_val: Any) -> bool:
    return not _arn_like(context_val, policy_val)


# ---------------------------------------------------------------------------
# Operator registry
# ---------------------------------------------------------------------------

CONDITION_OPERATORS: dict[str, Any] = {
    "StringEquals": _string_equals,
    "StringNotEquals": _string_not_equals,
    "StringEqualsIgnoreCase": _string_equals_ignore_case,
    "StringNotEqualsIgnoreCase": _string_not_equals_ignore_case,
    "StringLike": _string_like,
    "StringNotLike": _string_not_like,
    "NumericEquals": _numeric_equals,
    "NumericNotEquals": _numeric_not_equals,
    "NumericLessThan": _numeric_less_than,
    "NumericLessThanEquals": _numeric_less_than_equals,
    "NumericGreaterThan": _numeric_greater_than,
    "NumericGreaterThanEquals": _numeric_greater_than_equals,
    "DateEquals": _date_equals,
    "DateNotEquals": _date_not_equals,
    "DateLessThan": _date_less_than,
    "DateLessThanEquals": _date_less_than_equals,
    "DateGreaterThan": _date_greater_than,
    "DateGreaterThanEquals": _date_greater_than_equals,
    "Bool": _bool_op,
    "IpAddress": _ip_address,
    "NotIpAddress": _not_ip_address,
    "ArnEquals": _arn_equals,
    "ArnNotEquals": _arn_not_equals,
    "ArnLike": _arn_like,
    "ArnNotLike": _arn_not_like,
}


def _evaluate_single_operator(
    operator_name: str,
    condition_key: str,
    policy_values: list[str] | str,
    context_values: dict[str, Any],
    *,
    if_exists: bool = False,
) -> bool:
    """Evaluate a single condition operator against context values.

    Returns True if the condition is satisfied.
    """
    if not isinstance(policy_values, list):
        policy_values = [policy_values]

    # Null operator: checks key presence/absence
    if operator_name == "Null":
        key_present = condition_key in context_values and context_values[condition_key] is not None
        expect_null = _to_str(policy_values[0]).lower() == "true"
        return not key_present if expect_null else key_present

    # IfExists: if the key is not present, the condition is satisfied
    if condition_key not in context_values or context_values[condition_key] is None:
        return bool(if_exists)

    op_fn = CONDITION_OPERATORS.get(operator_name)
    if op_fn is None:
        return False

    ctx_val = context_values[condition_key]

    # Any policy value matching counts as success (OR within a key's values)
    for pv in policy_values:
        if op_fn(ctx_val, pv):
            return True
    return False


def _evaluate_set_operator(
    set_prefix: str,
    operator_name: str,
    condition_key: str,
    policy_values: list[str] | str,
    context_values: dict[str, Any],
    *,
    if_exists: bool = False,
) -> bool:
    """Evaluate ForAllValues: or ForAnyValue: prefixed conditions."""
    if not isinstance(policy_values, list):
        policy_values = [policy_values]

    op_fn = CONDITION_OPERATORS.get(operator_name)
    if op_fn is None:
        return False

    ctx_val = context_values.get(condition_key)
    if ctx_val is None:
        # IfExists: missing key means condition is vacuously satisfied
        if if_exists:
            return True
        # ForAllValues with no context values: vacuously true
        # ForAnyValue with no context values: false
        return set_prefix == "ForAllValues"

    if not isinstance(ctx_val, list):
        ctx_val = [ctx_val]

    if set_prefix == "ForAllValues":
        # Every context value must match at least one policy value
        for cv in ctx_val:
            if not any(op_fn(cv, pv) for pv in policy_values):
                return False
        return True
    elif set_prefix == "ForAnyValue":
        # At least one context value must match at least one policy value
        for cv in ctx_val:
            if any(op_fn(cv, pv) for pv in policy_values):
                return True
        return False
    return False


def evaluate_condition_block(
    condition_block: dict[str, Any],
    context_values: dict[str, Any],
) -> bool:
    """Evaluate an IAM policy Condition block.

    All operators in the block must be satisfied (AND logic).
    Within each operator, all condition keys must be satisfied (AND logic).
    Within each condition key's values, any value match counts (OR logic).

    Returns True if the entire condition block is satisfied.
    """
    if not condition_block:
        return True

    for raw_operator, key_value_map in condition_block.items():
        operator = raw_operator
        if_exists = False
        set_prefix = ""

        # Check for set operator prefixes
        for prefix in ("ForAllValues:", "ForAnyValue:"):
            if operator.startswith(prefix):
                set_prefix = prefix.rstrip(":")
                operator = operator[len(prefix) :]
                break

        # Check for IfExists suffix
        if operator.endswith("IfExists"):
            if_exists = True
            operator = operator[: -len("IfExists")]

        for condition_key, policy_values in key_value_map.items():
            if set_prefix:
                result = _evaluate_set_operator(
                    set_prefix,
                    operator,
                    condition_key,
                    policy_values,
                    context_values,
                    if_exists=if_exists,
                )
            else:
                result = _evaluate_single_operator(
                    operator, condition_key, policy_values, context_values, if_exists=if_exists
                )
            if not result:
                return False

    return True
