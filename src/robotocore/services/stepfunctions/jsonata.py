"""Basic JSONata expression evaluation for Step Functions.

Provides a minimal JSONata evaluator for Step Functions state definitions
that use JSONata query language instead of JSONPath. Supports:
- Simple field access: field, nested.field
- String concatenation with &
- Conditional expressions: condition ? then : else
- Numeric operations: +, -, *, /
- Function calls mapped to States.* intrinsic functions
- String literals and number literals

This is NOT a complete JSONata implementation — just enough for common
Step Functions patterns.
"""

import json
import logging
import re
from typing import Any

from robotocore.services.stepfunctions.intrinsics import evaluate_intrinsic

logger = logging.getLogger(__name__)


class JSONataError(Exception):
    """Raised when JSONata evaluation fails."""


def evaluate_jsonata(expression: str, data: dict | None = None) -> Any:
    """Evaluate a JSONata expression against the given data.

    Args:
        expression: JSONata expression string.
        data: Input data to evaluate against.

    Returns:
        The result of the expression evaluation.
    """
    data = data or {}
    expr = expression.strip()

    if not expr:
        return None

    return _eval(expr, data)


def _eval(expr: str, data: dict) -> Any:
    """Evaluate a JSONata expression."""
    expr = expr.strip()

    # String literal
    if (expr.startswith('"') and expr.endswith('"')) or (
        expr.startswith("'") and expr.endswith("'")
    ):
        return expr[1:-1]

    # Number literal
    try:
        if "." in expr:
            return float(expr)
        return int(expr)
    except ValueError as exc:
        logger.debug("_eval: int failed (non-fatal): %s", exc)

    # Boolean/null
    if expr == "true":
        return True
    if expr == "false":
        return False
    if expr == "null":
        return None

    # Array literal
    if expr.startswith("[") and expr.endswith("]"):
        inner = expr[1:-1].strip()
        if not inner:
            return []
        items = _split_top_level(inner, ",")
        return [_eval(item.strip(), data) for item in items]

    # Object literal
    if expr.startswith("{") and expr.endswith("}"):
        try:
            return json.loads(expr)
        except json.JSONDecodeError as exc:
            logger.debug("_eval: loads failed (non-fatal): %s", exc)
        return _eval_object_literal(expr[1:-1].strip(), data)

    # Conditional: condition ? then_expr : else_expr
    cond_match = _find_ternary(expr)
    if cond_match:
        cond, then_expr, else_expr = cond_match
        cond_result = _eval(cond.strip(), data)
        if _truthy(cond_result):
            return _eval(then_expr.strip(), data)
        return _eval(else_expr.strip(), data)

    # String concatenation with &
    if "&" in expr and not expr.startswith("$"):
        parts = _split_top_level(expr, "&")
        if len(parts) > 1:
            results = [_eval(p.strip(), data) for p in parts]
            return "".join(_to_string(r) for r in results)

    # Arithmetic: look for +, -, *, / at top level
    for op in ("+", "-"):
        parts = _split_top_level(expr, op)
        if len(parts) > 1:
            left = _eval(parts[0].strip(), data)
            right = _eval(op.join(parts[1:]).strip(), data)
            if isinstance(left, (int, float)) and isinstance(right, (int, float)):
                if op == "+":
                    return left + right
                return left - right

    for op in ("*", "/"):
        parts = _split_top_level(expr, op)
        if len(parts) > 1:
            left = _eval(parts[0].strip(), data)
            right = _eval(op.join(parts[1:]).strip(), data)
            if isinstance(left, (int, float)) and isinstance(right, (int, float)):
                if op == "*":
                    return left * right
                if right == 0:
                    raise JSONataError("Division by zero")
                return left / right

    # Comparison operators
    for op_str, op_fn in [
        (">=", lambda a, b: a >= b),
        ("<=", lambda a, b: a <= b),
        ("!=", lambda a, b: a != b),
        ("=", lambda a, b: a == b),
        (">", lambda a, b: a > b),
        ("<", lambda a, b: a < b),
    ]:
        parts = _split_top_level(expr, op_str)
        if len(parts) == 2:
            left = _eval(parts[0].strip(), data)
            right = _eval(parts[1].strip(), data)
            return op_fn(left, right)

    # Function call: $funcName(args) or States.Func(args)
    if expr.startswith("States."):
        return evaluate_intrinsic(expr, data)

    func_match = re.match(r"\$(\w+)\((.*)\)$", expr, re.DOTALL)
    if func_match:
        func_name = func_match.group(1)
        args_str = func_match.group(2).strip()
        return _eval_function(func_name, args_str, data)

    # Parenthesized expression
    if expr.startswith("(") and expr.endswith(")"):
        return _eval(expr[1:-1], data)

    # JSONPath-style field access: $.field.subfield
    if expr.startswith("$."):
        return _resolve_path(data, expr[2:])

    # Plain $ means root
    if expr == "$":
        return data

    # Bare field access: field.subfield
    return _resolve_path(data, expr)


def _resolve_path(data: Any, path: str) -> Any:
    """Resolve a dot-separated path against data."""
    if not path:
        return data

    parts = path.split(".")
    current = data
    for part in parts:
        if not part:
            continue
        # Array index
        idx_match = re.match(r"(\w+)\[(\d+)\]", part)
        if idx_match:
            key, idx = idx_match.group(1), int(idx_match.group(2))
            if isinstance(current, dict) and key in current:
                current = current[key]
                if isinstance(current, list) and idx < len(current):
                    current = current[idx]
                else:
                    return None
            else:
                return None
        elif isinstance(current, dict):
            if part in current:
                current = current[part]
            else:
                return None
        elif isinstance(current, list):
            # Map over array
            return [_resolve_path(item, part) for item in current if isinstance(item, dict)]
        else:
            return None
    return current


def _find_ternary(expr: str) -> tuple[str, str, str] | None:
    """Find top-level ternary operator ? : in expression."""
    depth = 0
    q_pos = -1
    in_quote = False
    quote_char = None

    for i, ch in enumerate(expr):
        if ch in ('"', "'") and not in_quote:
            in_quote = True
            quote_char = ch
        elif ch == quote_char and in_quote:
            in_quote = False
        elif in_quote:
            continue
        elif ch in "([{":
            depth += 1
        elif ch in ")]}":
            depth -= 1
        elif ch == "?" and depth == 0 and q_pos == -1:
            q_pos = i
        elif ch == ":" and depth == 0 and q_pos >= 0:
            return (expr[:q_pos], expr[q_pos + 1 : i], expr[i + 1 :])

    return None


def _split_top_level(expr: str, delimiter: str) -> list[str]:
    """Split expression by delimiter at the top level only."""
    parts = []
    current = []
    depth = 0
    in_quote = False
    quote_char = None
    i = 0

    while i < len(expr):
        ch = expr[i]
        if ch in ('"', "'") and not in_quote:
            in_quote = True
            quote_char = ch
            current.append(ch)
        elif ch == quote_char and in_quote:
            in_quote = False
            current.append(ch)
        elif in_quote:
            current.append(ch)
        elif ch in "([{":
            depth += 1
            current.append(ch)
        elif ch in ")]}":
            depth -= 1
            current.append(ch)
        elif depth == 0 and expr[i : i + len(delimiter)] == delimiter:
            parts.append("".join(current))
            current = []
            i += len(delimiter)
            continue
        else:
            current.append(ch)
        i += 1

    if current:
        parts.append("".join(current))

    return parts


def _eval_object_literal(inner: str, data: dict) -> dict:
    """Evaluate a JSONata object literal like { "key": expr, ... }."""
    result = {}
    pairs = _split_top_level(inner, ",")
    for pair in pairs:
        pair = pair.strip()
        if not pair:
            continue
        colon_parts = _split_top_level(pair, ":")
        if len(colon_parts) >= 2:
            key = _eval(colon_parts[0].strip(), data)
            val = _eval(":".join(colon_parts[1:]).strip(), data)
            if isinstance(key, str):
                result[key] = val
    return result


def _eval_function(func_name: str, args_str: str, data: dict) -> Any:
    """Evaluate a built-in JSONata function."""
    args = []
    if args_str:
        raw_args = _split_top_level(args_str, ",")
        args = [_eval(a.strip(), data) for a in raw_args]

    func_map = {
        "string": lambda a: _to_string(a[0]) if a else "",
        "number": lambda a: float(a[0]) if a else 0,
        "length": lambda a: len(a[0]) if a else 0,
        "substring": _fn_substring,
        "contains": lambda a: a[1] in a[0] if len(a) >= 2 else False,
        "uppercase": lambda a: str(a[0]).upper() if a else "",
        "lowercase": lambda a: str(a[0]).lower() if a else "",
        "trim": lambda a: str(a[0]).strip() if a else "",
        "sum": lambda a: sum(a[0]) if a and isinstance(a[0], list) else 0,
        "count": lambda a: len(a[0]) if a and isinstance(a[0], list) else 0,
        "append": lambda a: (
            (a[0] if isinstance(a[0], list) else [a[0]])
            + (a[1] if isinstance(a[1], list) else [a[1]])
            if len(a) >= 2
            else []
        ),
        "sort": lambda a: sorted(a[0]) if a and isinstance(a[0], list) else [],
        "reverse": lambda a: list(reversed(a[0])) if a and isinstance(a[0], list) else [],
        "join": lambda a: (
            (a[1] if len(a) > 1 else "").join(a[0]) if a and isinstance(a[0], list) else ""
        ),
        "split": lambda a: str(a[0]).split(a[1] if len(a) > 1 else " ") if a else [],
        "type": lambda a: _jsonata_type(a[0]) if a else "undefined",
        "exists": lambda a: a[0] is not None if a else False,
        "keys": lambda a: list(a[0].keys()) if a and isinstance(a[0], dict) else [],
        "values": lambda a: list(a[0].values()) if a and isinstance(a[0], dict) else [],
        "not": lambda a: not _truthy(a[0]) if a else True,
    }

    handler = func_map.get(func_name)
    if handler:
        return handler(args)

    raise JSONataError(f"Unknown function: ${func_name}")


def _fn_substring(args: list) -> str:
    s = str(args[0]) if args else ""
    start = int(args[1]) if len(args) > 1 else 0
    if len(args) > 2:
        length = int(args[2])
        return s[start : start + length]
    return s[start:]


def _to_string(val: Any) -> str:
    """Convert value to string for concatenation."""
    if val is None:
        return ""
    if isinstance(val, bool):
        return "true" if val else "false"
    if isinstance(val, (dict, list)):
        return json.dumps(val)
    return str(val)


def _truthy(val: Any) -> bool:
    """JSONata truthiness."""
    if val is None:
        return False
    if isinstance(val, bool):
        return val
    if isinstance(val, (int, float)):
        return val != 0
    if isinstance(val, str):
        return len(val) > 0
    if isinstance(val, (list, dict)):
        return len(val) > 0
    return True


def _jsonata_type(val: Any) -> str:
    """Return JSONata type name."""
    if val is None:
        return "null"
    if isinstance(val, bool):
        return "boolean"
    if isinstance(val, (int, float)):
        return "number"
    if isinstance(val, str):
        return "string"
    if isinstance(val, list):
        return "array"
    if isinstance(val, dict):
        return "object"
    return "undefined"
