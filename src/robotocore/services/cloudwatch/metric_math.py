"""Metric math expression evaluator for CloudWatch GetMetricData.

Supports functions: SUM, AVG, MIN, MAX, CEIL, FLOOR, ABS
Supports arithmetic: +, -, *, /
Supports metric references: m1, m2, etc.
"""

import math
import re
from collections.abc import Callable


class MetricMathError(Exception):
    """Error evaluating a metric math expression."""


# Supported functions operating on a list of values (aggregation)
_AGGREGATE_FUNCTIONS: dict[str, Callable[[list[float]], float]] = {
    "SUM": sum,
    "AVG": lambda vs: sum(vs) / len(vs) if vs else 0.0,
    "MIN": lambda vs: min(vs) if vs else 0.0,
    "MAX": lambda vs: max(vs) if vs else 0.0,
}

# Supported scalar functions
_SCALAR_FUNCTIONS: dict[str, Callable[[float], float]] = {
    "CEIL": math.ceil,
    "FLOOR": math.floor,
    "ABS": abs,
}

# Token patterns
_TOKEN_RE = re.compile(
    r"""
    (\d+\.?\d*|\.\d+)   |  # number
    ([A-Z]+)\s*\(        |  # function call
    ([a-zA-Z_]\w*)       |  # identifier (metric ref)
    ([+\-*/])            |  # operator
    (\()                 |  # open paren
    (\))                    # close paren
    """,
    re.VERBOSE,
)


def _tokenize(expression: str) -> list[tuple[str, str]]:
    """Tokenize a metric math expression.

    Returns list of (type, value) tuples where type is one of:
    'number', 'func', 'id', 'op', 'lparen', 'rparen'
    """
    tokens: list[tuple[str, str]] = []
    pos = 0
    expr = expression.strip()

    while pos < len(expr):
        if expr[pos].isspace():
            pos += 1
            continue

        m = _TOKEN_RE.match(expr, pos)
        if not m:
            raise MetricMathError(f"Unexpected character at position {pos}: {expr[pos:]}")

        if m.group(1):
            tokens.append(("number", m.group(1)))
        elif m.group(2):
            tokens.append(("func", m.group(2)))
            tokens.append(("lparen", "("))
        elif m.group(3):
            tokens.append(("id", m.group(3)))
        elif m.group(4):
            tokens.append(("op", m.group(4)))
        elif m.group(5):
            tokens.append(("lparen", "("))
        elif m.group(6):
            tokens.append(("rparen", ")"))

        pos = m.end()

    return tokens


def aggregate_values(values: list[float], period: int, stat: str = "Average") -> list[float]:
    """Aggregate raw metric values into period-aligned buckets.

    For simplicity, returns values as-is since Moto stores pre-aggregated data.
    The stat parameter controls how multiple values within a period are combined.
    """
    if not values:
        return []

    stat_lower = stat.lower()
    if stat_lower == "sum":
        return [sum(values)]
    elif stat_lower == "minimum":
        return [min(values)]
    elif stat_lower == "maximum":
        return [max(values)]
    elif stat_lower == "samplecount":
        return [float(len(values))]
    # Average
    return [sum(values) / len(values)] if values else [0.0]


def evaluate_expression(
    expression: str,
    metric_data: dict[str, list[float]],
) -> list[float]:
    """Evaluate a metric math expression against provided metric data.

    Args:
        expression: The math expression (e.g. "SUM(m1)", "m1 + m2", "AVG(m1) / 100")
        metric_data: Dict mapping metric IDs to their data values

    Returns:
        List of result values
    """
    tokens = _tokenize(expression)
    result, _ = _parse_expression(tokens, 0, metric_data)
    return result


def _parse_expression(
    tokens: list[tuple[str, str]],
    pos: int,
    data: dict[str, list[float]],
) -> tuple[list[float], int]:
    """Parse an additive expression (handles + and -)."""
    left, pos = _parse_term(tokens, pos, data)

    while (
        pos < len(tokens)
        and tokens[pos] == ("op", "+")
        or (pos < len(tokens) and tokens[pos] == ("op", "-"))
    ):
        op = tokens[pos][1]
        pos += 1
        right, pos = _parse_term(tokens, pos, data)
        left = _apply_binary_op(left, right, op)

    return left, pos


def _parse_term(
    tokens: list[tuple[str, str]],
    pos: int,
    data: dict[str, list[float]],
) -> tuple[list[float], int]:
    """Parse a multiplicative expression (handles * and /)."""
    left, pos = _parse_unary(tokens, pos, data)

    while pos < len(tokens) and tokens[pos][0] == "op" and tokens[pos][1] in ("*", "/"):
        op = tokens[pos][1]
        pos += 1
        right, pos = _parse_unary(tokens, pos, data)
        left = _apply_binary_op(left, right, op)

    return left, pos


def _parse_unary(
    tokens: list[tuple[str, str]],
    pos: int,
    data: dict[str, list[float]],
) -> tuple[list[float], int]:
    """Parse a unary expression (handles unary minus)."""
    if pos < len(tokens) and tokens[pos] == ("op", "-"):
        pos += 1
        val, pos = _parse_primary(tokens, pos, data)
        return [-v for v in val], pos
    return _parse_primary(tokens, pos, data)


def _parse_primary(
    tokens: list[tuple[str, str]],
    pos: int,
    data: dict[str, list[float]],
) -> tuple[list[float], int]:
    """Parse a primary expression: number, identifier, function call, or parenthesized expr."""
    if pos >= len(tokens):
        raise MetricMathError("Unexpected end of expression")

    tok_type, tok_val = tokens[pos]

    if tok_type == "number":
        return [float(tok_val)], pos + 1

    if tok_type == "func":
        func_name = tok_val
        # Next token should be lparen (already added by tokenizer)
        pos += 1  # skip the tokenizer-generated lparen
        # Handle empty argument list: FUNC() — the tokenizer produces
        # func, lparen, rparen. After skipping func (above), we're at lparen.
        # Check for lparen immediately followed by rparen.
        if pos + 1 < len(tokens) and tokens[pos][0] == "lparen" and tokens[pos + 1][0] == "rparen":
            pos += 2  # skip lparen and rparen
            result = _apply_function(func_name, [])
            return result, pos
        arg, pos = _parse_expression(tokens, pos, data)
        if pos < len(tokens) and tokens[pos][0] == "rparen":
            pos += 1  # skip rparen
        result = _apply_function(func_name, arg)
        return result, pos

    if tok_type == "id":
        metric_id = tok_val
        if metric_id in data:
            return list(data[metric_id]), pos + 1
        raise MetricMathError(f"Unknown metric ID: {metric_id}")

    if tok_type == "lparen":
        pos += 1
        val, pos = _parse_expression(tokens, pos, data)
        if pos < len(tokens) and tokens[pos][0] == "rparen":
            pos += 1
        return val, pos

    raise MetricMathError(f"Unexpected token: {tok_type}={tok_val}")


def _apply_function(name: str, values: list[float]) -> list[float]:
    """Apply a named function to values."""
    upper = name.upper()

    if upper in _AGGREGATE_FUNCTIONS:
        fn = _AGGREGATE_FUNCTIONS[upper]
        return [fn(values)]

    if upper in _SCALAR_FUNCTIONS:
        fn = _SCALAR_FUNCTIONS[upper]
        return [fn(v) for v in values]

    raise MetricMathError(f"Unknown function: {name}")


def _apply_binary_op(left: list[float], right: list[float], op: str) -> list[float]:
    """Apply a binary operator to two value lists, broadcasting scalars."""
    if len(left) == 1 and len(right) > 1:
        left = left * len(right)
    elif len(right) == 1 and len(left) > 1:
        right = right * len(left)

    max_len = max(len(left), len(right))
    # Pad shorter list with 0
    while len(left) < max_len:
        left.append(0.0)
    while len(right) < max_len:
        right.append(0.0)

    if op == "+":
        return [a + b for a, b in zip(left, right)]
    elif op == "-":
        return [a - b for a, b in zip(left, right)]
    elif op == "*":
        return [a * b for a, b in zip(left, right)]
    elif op == "/":
        return [a / b if b != 0 else 0.0 for a, b in zip(left, right)]
    raise MetricMathError(f"Unknown operator: {op}")
