"""States.* intrinsic functions for Step Functions ASL.

Implements all intrinsic functions per the AWS States Language specification:
https://docs.aws.amazon.com/step-functions/latest/dg/amazon-states-language-intrinsic-functions.html
"""

import base64
import hashlib
import json
import logging
import random
import re
import uuid
from typing import Any

logger = logging.getLogger(__name__)


class IntrinsicError(Exception):
    """Raised when an intrinsic function call fails."""


def evaluate_intrinsic(expression: str, context: dict | None = None) -> Any:
    """Parse and evaluate a States.* intrinsic function expression.

    Args:
        expression: The intrinsic function call string, e.g. "States.Format('hi {}', $.name)"
        context: The current state input data for resolving JSONPath references.

    Returns:
        The result of evaluating the intrinsic function.
    """
    context = context or {}
    return _eval_expr(expression.strip(), context)


def _eval_expr(expr: str, context: dict) -> Any:
    """Evaluate a single expression which may be a literal, path ref, or function call."""
    expr = expr.strip()

    # Nested intrinsic call
    if expr.startswith("States."):
        return _eval_intrinsic_call(expr, context)

    # JSONPath reference
    if expr.startswith("$.") or expr == "$":
        return _resolve_path(context, expr)

    # String literal
    if (expr.startswith("'") and expr.endswith("'")) or (
        expr.startswith('"') and expr.endswith('"')
    ):
        return expr[1:-1]

    # Boolean
    if expr == "true":
        return True
    if expr == "false":
        return False

    # Null
    if expr == "null":
        return None

    # Number
    try:
        if "." in expr:
            return float(expr)
        return int(expr)
    except ValueError as exc:
        logger.debug("_eval_expr: int failed (non-fatal): %s", exc)

    # JSON array literal
    if expr.startswith("[") and expr.endswith("]"):
        inner = expr[1:-1].strip()
        if not inner:
            return []
        items = _split_args(inner)
        return [_eval_expr(item, context) for item in items]

    # JSON object literal
    if expr.startswith("{") and expr.endswith("}"):
        try:
            return json.loads(expr)
        except json.JSONDecodeError as exc:
            logger.debug("_eval_expr: loads failed (non-fatal): %s", exc)

    return expr


def _eval_intrinsic_call(expr: str, context: dict) -> Any:
    """Parse and evaluate a States.FuncName(args...) call."""
    # Find the function name and argument list
    paren_idx = expr.index("(")
    func_name = expr[:paren_idx].strip()
    # Find matching closing paren
    depth = 0
    end_idx = -1
    for i in range(paren_idx, len(expr)):
        if expr[i] == "(":
            depth += 1
        elif expr[i] == ")":
            depth -= 1
            if depth == 0:
                end_idx = i
                break
    if end_idx == -1:
        raise IntrinsicError(f"Unmatched parenthesis in: {expr}")

    args_str = expr[paren_idx + 1 : end_idx].strip()
    args = _split_args(args_str) if args_str else []
    evaluated_args = [_eval_expr(a, context) for a in args]

    handler = _INTRINSIC_MAP.get(func_name)
    if handler is None:
        raise IntrinsicError(f"Unknown intrinsic function: {func_name}")

    return handler(*evaluated_args)


def _split_args(args_str: str) -> list[str]:
    """Split a comma-separated argument string, respecting nesting and quotes."""
    args = []
    depth = 0
    current = []
    in_single_quote = False
    in_double_quote = False

    for ch in args_str:
        if ch == "'" and not in_double_quote:
            in_single_quote = not in_single_quote
            current.append(ch)
        elif ch == '"' and not in_single_quote:
            in_double_quote = not in_double_quote
            current.append(ch)
        elif in_single_quote or in_double_quote:
            current.append(ch)
        elif ch in "([{":
            depth += 1
            current.append(ch)
        elif ch in ")]}":
            depth -= 1
            current.append(ch)
        elif ch == "," and depth == 0:
            args.append("".join(current).strip())
            current = []
        else:
            current.append(ch)

    if current:
        args.append("".join(current).strip())

    return [a for a in args if a]


def _resolve_path(data: Any, path: str) -> Any:
    """Resolve a JSONPath expression against data."""
    if not path or path == "$":
        return data
    if not path.startswith("$"):
        return data

    parts = path[2:].split(".") if len(path) > 1 else []
    current = data
    for part in parts:
        if not part:
            continue
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


# --- Intrinsic function implementations ---


def _states_format(template: str, *args: Any) -> str:
    """States.Format — string formatting with {} placeholders."""
    if not isinstance(template, str):
        raise IntrinsicError("States.Format: first argument must be a string template")
    result = template
    for arg in args:
        str_arg = json.dumps(arg) if isinstance(arg, (dict, list)) else str(arg)
        result = result.replace("{}", str_arg, 1)
    return result


def _states_string_to_json(s: str) -> Any:
    """States.StringToJson — parse JSON string."""
    if not isinstance(s, str):
        raise IntrinsicError("States.StringToJson: argument must be a string")
    return json.loads(s)


def _states_json_to_string(obj: Any) -> str:
    """States.JsonToString — serialize to JSON string."""
    return json.dumps(obj, separators=(",", ":"))


def _states_array(*items: Any) -> list:
    """States.Array — create array from arguments."""
    return list(items)


def _states_array_partition(array: list, chunk_size: int) -> list[list]:
    """States.ArrayPartition — split array into chunks."""
    if not isinstance(array, list):
        raise IntrinsicError("States.ArrayPartition: first argument must be an array")
    chunk_size = int(chunk_size)
    if chunk_size <= 0:
        raise IntrinsicError("States.ArrayPartition: chunk_size must be positive")
    return [array[i : i + chunk_size] for i in range(0, len(array), chunk_size)]


def _states_array_contains(array: list, value: Any) -> bool:
    """States.ArrayContains — check if array contains value."""
    if not isinstance(array, list):
        raise IntrinsicError("States.ArrayContains: first argument must be an array")
    return value in array


def _states_array_range(start: int, end: int, step: int) -> list[int]:
    """States.ArrayRange — generate range of integers."""
    start, end, step = int(start), int(end), int(step)
    if step == 0:
        raise IntrinsicError("States.ArrayRange: step must not be zero")
    result = []
    current = start
    if step > 0:
        while current <= end:
            result.append(current)
            current += step
    else:
        while current >= end:
            result.append(current)
            current += step
    return result


def _states_array_get_item(array: list, index: int) -> Any:
    """States.ArrayGetItem — get element by index."""
    if not isinstance(array, list):
        raise IntrinsicError("States.ArrayGetItem: first argument must be an array")
    index = int(index)
    if index < 0 or index >= len(array):
        raise IntrinsicError(f"States.ArrayGetItem: index {index} out of bounds")
    return array[index]


def _states_array_length(array: list) -> int:
    """States.ArrayLength — return array length."""
    if not isinstance(array, list):
        raise IntrinsicError("States.ArrayLength: argument must be an array")
    return len(array)


def _states_array_unique(array: list) -> list:
    """States.ArrayUnique — deduplicate array preserving order."""
    if not isinstance(array, list):
        raise IntrinsicError("States.ArrayUnique: argument must be an array")
    seen = []
    result = []
    for item in array:
        # Use JSON serialization for comparison of complex types
        key = json.dumps(item, sort_keys=True) if isinstance(item, (dict, list)) else item
        if key not in seen:
            seen.append(key)
            result.append(item)
    return result


def _states_base64_encode(s: str) -> str:
    """States.Base64Encode — base64 encode a string."""
    if not isinstance(s, str):
        raise IntrinsicError("States.Base64Encode: argument must be a string")
    return base64.b64encode(s.encode("utf-8")).decode("utf-8")


def _states_base64_decode(s: str) -> str:
    """States.Base64Decode — base64 decode to string."""
    if not isinstance(s, str):
        raise IntrinsicError("States.Base64Decode: argument must be a string")
    return base64.b64decode(s).decode("utf-8")


def _states_hash(data: str, algorithm: str) -> str:
    """States.Hash — hash data with specified algorithm."""
    if not isinstance(data, str):
        raise IntrinsicError("States.Hash: first argument must be a string")
    algo_map = {
        "MD5": hashlib.md5,
        "SHA-1": hashlib.sha1,
        "SHA-256": hashlib.sha256,
        "SHA-384": hashlib.sha384,
        "SHA-512": hashlib.sha512,
    }
    hasher = algo_map.get(algorithm)
    if hasher is None:
        raise IntrinsicError(f"States.Hash: unsupported algorithm: {algorithm}")
    return hasher(data.encode("utf-8")).hexdigest()


def _states_json_merge(obj1: Any, obj2: Any, deep: bool = False) -> dict:
    """States.JsonMerge — merge two objects."""
    if not isinstance(obj1, dict) or not isinstance(obj2, dict):
        raise IntrinsicError("States.JsonMerge: arguments must be objects")
    if deep:
        return _deep_merge(obj1, obj2)
    result = dict(obj1)
    result.update(obj2)
    return result


def _deep_merge(a: dict, b: dict) -> dict:
    """Deep merge dict b into dict a."""
    result = dict(a)
    for key, val in b.items():
        if key in result and isinstance(result[key], dict) and isinstance(val, dict):
            result[key] = _deep_merge(result[key], val)
        else:
            result[key] = val
    return result


def _states_math_random(start: int, end: int) -> int:
    """States.MathRandom — random integer in [start, end]."""
    start, end = int(start), int(end)
    return random.randint(start, end)


def _states_math_add(a: Any, b: Any) -> int | float:
    """States.MathAdd — addition."""
    a_num = float(a) if isinstance(a, str) else a
    b_num = float(b) if isinstance(b, str) else b
    result = a_num + b_num
    if isinstance(a_num, int) and isinstance(b_num, int):
        return int(result)
    return result


def _states_string_split(s: str, delimiter: str) -> list[str]:
    """States.StringSplit — split string by delimiter."""
    if not isinstance(s, str):
        raise IntrinsicError("States.StringSplit: first argument must be a string")
    if not isinstance(delimiter, str):
        raise IntrinsicError("States.StringSplit: second argument must be a string")
    if not delimiter:
        return list(s)
    return s.split(delimiter)


def _states_uuid() -> str:
    """States.UUID — generate a UUID v4."""
    return str(uuid.uuid4())


_INTRINSIC_MAP = {
    "States.Format": _states_format,
    "States.StringToJson": _states_string_to_json,
    "States.JsonToString": _states_json_to_string,
    "States.Array": _states_array,
    "States.ArrayPartition": _states_array_partition,
    "States.ArrayContains": _states_array_contains,
    "States.ArrayRange": _states_array_range,
    "States.ArrayGetItem": _states_array_get_item,
    "States.ArrayLength": _states_array_length,
    "States.ArrayUnique": _states_array_unique,
    "States.Base64Encode": _states_base64_encode,
    "States.Base64Decode": _states_base64_decode,
    "States.Hash": _states_hash,
    "States.JsonMerge": _states_json_merge,
    "States.MathRandom": _states_math_random,
    "States.MathAdd": _states_math_add,
    "States.StringSplit": _states_string_split,
    "States.UUID": _states_uuid,
}
