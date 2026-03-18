"""IoT SQL rule evaluation engine.

Parses and evaluates IoT SQL statements like:
  SELECT * FROM 'topic/test'
  SELECT temperature FROM 'sensors/+' WHERE temperature > 30
  SELECT *, topic() as t, timestamp() as ts FROM 'devices/#'

Supports:
- Topic filters with wildcards (+ single level, # multi-level)
- WHERE clauses with comparison operators (=, !=, <, >, <=, >=)
- WHERE clauses with AND/OR/NOT
- SELECT with field extraction from JSON payloads
- Built-in functions: topic(), timestamp(), clientid()
"""

import logging
import re
import time
from dataclasses import dataclass, field
from typing import Any

logger = logging.getLogger(__name__)


@dataclass
class ParsedRule:
    """A parsed IoT SQL rule."""

    select_fields: list[str]  # ["*"] or ["temperature", "topic() as t"]
    topic_filter: str  # e.g., "sensors/+"
    where_clause: str | None = None  # e.g., "temperature > 30"
    aliases: dict[str, str] = field(default_factory=dict)  # e.g., {"t": "topic()"}


def parse_sql(sql: str) -> ParsedRule:
    """Parse an IoT SQL statement into its components.

    Supports: SELECT <fields> FROM '<topic_filter>' [WHERE <condition>]
    """
    sql = sql.strip()

    # Match: SELECT ... FROM '...' [WHERE ...]
    pattern = re.compile(
        r"SELECT\s+(.+?)\s+FROM\s+'([^']+)'"
        r"(?:\s+WHERE\s+(.+))?",
        re.IGNORECASE | re.DOTALL,
    )
    match = pattern.match(sql)
    if not match:
        raise ValueError(f"Invalid IoT SQL: {sql}")

    select_part = match.group(1).strip()
    topic_filter = match.group(2).strip()
    where_clause = match.group(3).strip() if match.group(3) else None

    # Parse SELECT fields and aliases
    select_fields = []
    aliases: dict[str, str] = {}

    for raw_field in _split_select_fields(select_part):
        raw_field = raw_field.strip()
        # Check for alias: "expr AS alias"
        alias_match = re.match(r"(.+?)\s+[Aa][Ss]\s+(\w+)$", raw_field)
        if alias_match:
            expr = alias_match.group(1).strip()
            alias = alias_match.group(2).strip()
            aliases[alias] = expr
            select_fields.append(raw_field)
        else:
            select_fields.append(raw_field)

    return ParsedRule(
        select_fields=select_fields,
        topic_filter=topic_filter,
        where_clause=where_clause,
        aliases=aliases,
    )


def _split_select_fields(select_part: str) -> list[str]:
    """Split SELECT fields by comma, respecting parentheses."""
    fields = []
    depth = 0
    current = ""
    for ch in select_part:
        if ch == "(":
            depth += 1
            current += ch
        elif ch == ")":
            depth -= 1
            current += ch
        elif ch == "," and depth == 0:
            fields.append(current)
            current = ""
        else:
            current += ch
    if current.strip():
        fields.append(current)
    return fields


def topic_matches(topic_filter: str, topic: str) -> bool:
    """Check if a topic matches a topic filter with wildcards.

    + matches a single level: sensors/+/temp matches sensors/room1/temp
    # matches all remaining levels: sensors/# matches sensors/room1/temp
    """
    filter_parts = topic_filter.split("/")
    topic_parts = topic.split("/")

    fi = 0
    ti = 0
    while fi < len(filter_parts) and ti < len(topic_parts):
        if filter_parts[fi] == "#":
            return True  # # matches everything from here
        if filter_parts[fi] == "+" or filter_parts[fi] == topic_parts[ti]:
            fi += 1
            ti += 1
        else:
            return False

    # Both must be exhausted for a match (unless filter ends with #)
    if fi < len(filter_parts) and filter_parts[fi] == "#":
        return True
    return fi == len(filter_parts) and ti == len(topic_parts)


def evaluate_where(
    where_clause: str,
    payload: dict[str, Any],
    topic: str = "",
    client_id: str = "",
) -> bool:
    """Evaluate a WHERE clause against a message payload.

    Supports: =, !=, <, >, <=, >=, AND, OR, NOT, parentheses.
    """
    if not where_clause:
        return True

    tokens = _tokenize_where(where_clause)
    result, _ = _parse_or_expr(tokens, 0, payload, topic, client_id)
    return result


def _tokenize_where(clause: str) -> list[str]:
    """Tokenize a WHERE clause into meaningful tokens."""
    # Match: strings, numbers, identifiers, operators, parens
    pattern = re.compile(
        r"""
        '(?:[^'\\]|\\.)*'   |  # single-quoted string
        "(?:[^"\\]|\\.)*"    |  # double-quoted string
        \d+\.?\d*            |  # number
        <=|>=|!=|<>|[=<>]   |  # comparison operators
        \(|\)                |  # parentheses
        \w+(?:\(\))?         |  # identifiers and functions like topic()
        \w+\([^)]*\)         |  # functions with args
        """,
        re.VERBOSE,
    )
    tokens = pattern.findall(clause)
    return [t for t in tokens if t.strip()]


def _parse_or_expr(
    tokens: list[str],
    pos: int,
    payload: dict[str, Any],
    topic: str,
    client_id: str,
) -> tuple[bool, int]:
    """Parse OR expression (lowest precedence)."""
    left, pos = _parse_and_expr(tokens, pos, payload, topic, client_id)
    while pos < len(tokens) and tokens[pos].upper() == "OR":
        pos += 1
        right, pos = _parse_and_expr(tokens, pos, payload, topic, client_id)
        left = left or right
    return left, pos


def _parse_and_expr(
    tokens: list[str],
    pos: int,
    payload: dict[str, Any],
    topic: str,
    client_id: str,
) -> tuple[bool, int]:
    """Parse AND expression."""
    left, pos = _parse_not_expr(tokens, pos, payload, topic, client_id)
    while pos < len(tokens) and tokens[pos].upper() == "AND":
        pos += 1
        right, pos = _parse_not_expr(tokens, pos, payload, topic, client_id)
        left = left and right
    return left, pos


def _parse_not_expr(
    tokens: list[str],
    pos: int,
    payload: dict[str, Any],
    topic: str,
    client_id: str,
) -> tuple[bool, int]:
    """Parse NOT expression."""
    if pos < len(tokens) and tokens[pos].upper() == "NOT":
        pos += 1
        result, pos = _parse_not_expr(tokens, pos, payload, topic, client_id)
        return not result, pos
    return _parse_comparison(tokens, pos, payload, topic, client_id)


def _parse_comparison(
    tokens: list[str],
    pos: int,
    payload: dict[str, Any],
    topic: str,
    client_id: str,
) -> tuple[bool, int]:
    """Parse a comparison expression or parenthesized sub-expression."""
    if pos < len(tokens) and tokens[pos] == "(":
        pos += 1  # skip (
        result, pos = _parse_or_expr(tokens, pos, payload, topic, client_id)
        if pos < len(tokens) and tokens[pos] == ")":
            pos += 1  # skip )
        return result, pos

    # Get left value
    left_val, pos = _resolve_value(tokens, pos, payload, topic, client_id)

    # Get operator
    if pos >= len(tokens):
        # Bare value treated as truthy
        return bool(left_val), pos

    op = tokens[pos]
    if op not in ("=", "!=", "<>", "<", ">", "<=", ">="):
        # Not a comparison; treat left as truthy
        return bool(left_val), pos

    pos += 1

    # Get right value
    right_val, pos = _resolve_value(tokens, pos, payload, topic, client_id)

    return _compare(left_val, op, right_val), pos


def _resolve_value(
    tokens: list[str],
    pos: int,
    payload: dict[str, Any],
    topic: str,
    client_id: str,
) -> tuple[Any, int]:
    """Resolve a token to its value."""
    if pos >= len(tokens):
        return None, pos

    token = tokens[pos]

    # String literal
    if (token.startswith("'") and token.endswith("'")) or (
        token.startswith('"') and token.endswith('"')
    ):
        return token[1:-1], pos + 1

    # Number
    if re.match(r"^\d+\.?\d*$", token):
        val = float(token) if "." in token else int(token)
        return val, pos + 1

    # Built-in functions
    lower = token.lower()
    if lower == "topic()" or lower == "topic":
        return topic, pos + 1
    if lower == "timestamp()" or lower == "timestamp":
        return int(time.time() * 1000), pos + 1
    if lower == "clientid()" or lower == "clientid":
        return client_id, pos + 1

    # Field reference - dot notation for nested fields
    val = _get_nested(payload, token)
    return val, pos + 1


def _get_nested(payload: dict[str, Any], field_path: str) -> Any:
    """Get a nested field value using dot notation."""
    parts = field_path.split(".")
    current: Any = payload
    for part in parts:
        if isinstance(current, dict) and part in current:
            current = current[part]
        else:
            return None
    return current


def _compare(left: Any, op: str, right: Any) -> bool:
    """Compare two values with the given operator."""
    if left is None or right is None:
        if op in ("=", "=="):
            return left is None and right is None
        if op in ("!=", "<>"):
            return not (left is None and right is None)
        return False

    # Coerce types for comparison
    left, right = _coerce_types(left, right)

    if op == "=" or op == "==":
        return left == right
    if op == "!=" or op == "<>":
        return left != right
    if op == "<":
        return left < right
    if op == ">":
        return left > right
    if op == "<=":
        return left <= right
    if op == ">=":
        return left >= right
    return False


def _coerce_types(left: Any, right: Any) -> tuple[Any, Any]:
    """Coerce values to compatible types for comparison."""
    if isinstance(left, (int, float)) and isinstance(right, str):
        try:
            right = float(right) if "." in right else int(right)
        except (ValueError, TypeError) as e:
            logger.debug("Type coercion skipped (best-effort): %s", e)
    elif isinstance(right, (int, float)) and isinstance(left, str):
        try:
            left = float(left) if "." in left else int(left)
        except (ValueError, TypeError) as e:
            logger.debug("Type coercion skipped (best-effort): %s", e)
    return left, right


def extract_fields(
    parsed: ParsedRule,
    payload: dict[str, Any],
    topic: str = "",
    client_id: str = "",
) -> dict[str, Any]:
    """Extract fields from the payload based on SELECT clause.

    SELECT * returns the full payload.
    SELECT field1, field2 returns only those fields.
    SELECT *, topic() as t adds the topic to the full payload.
    """
    result: dict[str, Any] = {}

    has_star = False
    for f in parsed.select_fields:
        clean = f.strip()
        # Check for aliased expression
        alias_match = re.match(r"(.+?)\s+[Aa][Ss]\s+(\w+)$", clean)
        if alias_match:
            expr = alias_match.group(1).strip()
            alias = alias_match.group(2).strip()
            result[alias] = _eval_select_expr(expr, payload, topic, client_id)
        elif clean == "*":
            has_star = True
        else:
            val = _eval_select_expr(clean, payload, topic, client_id)
            result[clean] = val

    if has_star:
        # Merge full payload, but let explicit fields override
        merged = dict(payload)
        merged.update(result)
        return merged

    return result


def _eval_select_expr(
    expr: str,
    payload: dict[str, Any],
    topic: str,
    client_id: str,
) -> Any:
    """Evaluate a single SELECT expression."""
    lower = expr.lower().strip()
    if lower == "topic()" or lower == "topic":
        return topic
    if lower == "timestamp()" or lower == "timestamp":
        return int(time.time() * 1000)
    if lower == "clientid()" or lower == "clientid":
        return client_id
    # Field reference
    return _get_nested(payload, expr)


@dataclass
class TopicRule:
    """An IoT topic rule with parsed SQL."""

    rule_name: str
    sql: str
    parsed: ParsedRule
    actions: list[dict[str, Any]]
    error_action: dict[str, Any] | None = None
    enabled: bool = True
    description: str = ""
    rule_arn: str = ""


def evaluate_message(
    rules: list[TopicRule],
    topic: str,
    payload: dict[str, Any],
    client_id: str = "",
) -> list[tuple[TopicRule, dict[str, Any]]]:
    """Evaluate a message against all rules.

    Returns list of (rule, extracted_payload) for rules that match.
    """
    matches: list[tuple[TopicRule, dict[str, Any]]] = []

    for rule in rules:
        if not rule.enabled:
            continue

        # Check topic filter
        if not topic_matches(rule.parsed.topic_filter, topic):
            continue

        # Check WHERE clause
        if not evaluate_where(rule.parsed.where_clause, payload, topic, client_id):
            continue

        # Extract fields
        extracted = extract_fields(rule.parsed, payload, topic, client_id)
        matches.append((rule, extracted))

    return matches
