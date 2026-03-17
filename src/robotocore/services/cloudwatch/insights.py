"""CloudWatch Logs Insights query engine.

Supports: fields, filter, stats (with group-by), sort, limit, parse (regex extraction).
Pipeline executor against in-memory log data.
"""

import logging
import re
import threading
import time
import uuid
from collections import defaultdict
from collections.abc import Callable

# ---------------------------------------------------------------------------
# Query store
# ---------------------------------------------------------------------------

_queries: dict[str, dict] = {}
_query_lock = threading.Lock()

# Query status constants
STATUS_SCHEDULED = "Scheduled"
STATUS_RUNNING = "Running"
STATUS_COMPLETE = "Complete"
STATUS_CANCELLED = "Cancelled"


logger = logging.getLogger(__name__)


def start_query(
    log_group_names: list[str],
    query_string: str,
    start_time: int,
    end_time: int,
    region: str,
    account_id: str,
    limit: int = 1000,
) -> str:
    """Start a Logs Insights query and return its query ID."""
    query_id = str(uuid.uuid4())

    with _query_lock:
        _queries[query_id] = {
            "query_id": query_id,
            "query_string": query_string,
            "log_group_names": log_group_names,
            "start_time": start_time,
            "end_time": end_time,
            "region": region,
            "account_id": account_id,
            "limit": limit,
            "status": STATUS_RUNNING,
            "results": [],
            "statistics": {"recordsMatched": 0.0, "recordsScanned": 0.0, "bytesScanned": 0.0},
            "create_time": time.time(),
        }

    # Execute synchronously (in-memory, fast)
    _execute_query(query_id, region, account_id)
    return query_id


def get_query_results(query_id: str) -> dict:
    """Get results of a Logs Insights query."""
    with _query_lock:
        query = _queries.get(query_id)
        if not query:
            raise InsightsError("ResourceNotFoundException", f"Query {query_id} not found")

        return {
            "status": query["status"],
            "results": query["results"],
            "statistics": query["statistics"],
        }


def stop_query(query_id: str) -> bool:
    """Stop a running query."""
    with _query_lock:
        query = _queries.get(query_id)
        if not query:
            return False
        if query["status"] in (STATUS_RUNNING, STATUS_SCHEDULED):
            query["status"] = STATUS_CANCELLED
        return True


def get_all_queries() -> list[dict]:
    """Return all stored queries (for testing)."""
    with _query_lock:
        return [{"query_id": q["query_id"], "status": q["status"]} for q in _queries.values()]


def clear_queries() -> None:
    """Clear all stored queries (for testing)."""
    with _query_lock:
        _queries.clear()


class InsightsError(Exception):
    """Error in Logs Insights operations."""

    def __init__(self, code: str, message: str):
        self.code = code
        self.message = message
        super().__init__(message)


# ---------------------------------------------------------------------------
# Query parser
# ---------------------------------------------------------------------------


def parse_query(query_string: str) -> list[dict]:
    """Parse a Logs Insights query into a pipeline of commands.

    Returns a list of command dicts, each with a 'type' and command-specific keys.
    Supported commands: fields, filter, stats, sort, limit, parse
    """
    commands: list[dict] = []
    # Split on pipe, handling pipes inside quotes/regex
    parts = _split_pipeline(query_string)

    for part in parts:
        part = part.strip()
        if not part:
            continue
        cmd = _parse_command(part)
        if cmd:
            commands.append(cmd)

    return commands


def _split_pipeline(query: str) -> list[str]:
    """Split query on | delimiters, respecting quotes."""
    parts: list[str] = []
    current: list[str] = []
    in_quote = False
    quote_char = ""

    for ch in query:
        if ch in ('"', "'") and not in_quote:
            in_quote = True
            quote_char = ch
            current.append(ch)
        elif ch == quote_char and in_quote:
            in_quote = False
            quote_char = ""
            current.append(ch)
        elif ch == "|" and not in_quote:
            parts.append("".join(current))
            current = []
        else:
            current.append(ch)

    if current:
        parts.append("".join(current))
    return parts


def _parse_command(text: str) -> dict | None:
    """Parse a single pipeline command."""
    text = text.strip()

    # fields @timestamp, @message
    if text.startswith("fields ") or text.startswith("fields\t"):
        fields = [f.strip().lstrip("@") for f in text[7:].split(",")]
        return {"type": "fields", "fields": fields}

    # filter
    if text.startswith("filter ") or text.startswith("filter\t"):
        return {"type": "filter", "expression": text[7:].strip()}

    # stats count(*) by field
    stats_match = re.match(r"stats\s+(.+?)(?:\s+by\s+(.+))?$", text, re.IGNORECASE)
    if stats_match:
        aggregations_str = stats_match.group(1).strip()
        group_by_str = stats_match.group(2)
        aggregations = _parse_aggregations(aggregations_str)
        group_by = [g.strip().lstrip("@") for g in group_by_str.split(",")] if group_by_str else []
        return {"type": "stats", "aggregations": aggregations, "group_by": group_by}

    # sort field asc/desc
    sort_match = re.match(r"sort\s+(@?\w+)\s*(asc|desc)?", text, re.IGNORECASE)
    if sort_match:
        field = sort_match.group(1).lstrip("@")
        order = (sort_match.group(2) or "desc").lower()
        return {"type": "sort", "field": field, "order": order}

    # limit N
    limit_match = re.match(r"limit\s+(\d+)", text, re.IGNORECASE)
    if limit_match:
        return {"type": "limit", "count": int(limit_match.group(1))}

    # parse @message /regex/ as @field1, @field2
    parse_match = re.match(r'parse\s+(@?\w+)\s+[/"](.+?)[/"]\s+as\s+(.+)', text, re.IGNORECASE)
    if parse_match:
        source = parse_match.group(1).lstrip("@")
        pattern = parse_match.group(2)
        field_names = [f.strip().lstrip("@") for f in parse_match.group(3).split(",")]
        return {
            "type": "parse",
            "source": source,
            "pattern": pattern,
            "fields": field_names,
        }

    return None


def _parse_aggregations(text: str) -> list[dict]:
    """Parse aggregation expressions like 'count(*)', 'avg(field)', 'sum(@duration)'."""
    result: list[dict] = []
    # Split on comma, but respect parentheses
    parts = _split_aggregations(text)
    for part in parts:
        part = part.strip()
        agg_match = re.match(r"(\w+)\s*\(\s*(@?\w*\*?)\s*\)", part)
        if agg_match:
            func = agg_match.group(1).lower()
            field = agg_match.group(2).lstrip("@") or "*"
            result.append({"func": func, "field": field})
        else:
            result.append({"func": "count", "field": "*"})
    return result


def _split_aggregations(text: str) -> list[str]:
    """Split aggregation text on commas, respecting parentheses."""
    parts: list[str] = []
    current: list[str] = []
    depth = 0
    for ch in text:
        if ch == "(":
            depth += 1
            current.append(ch)
        elif ch == ")":
            depth -= 1
            current.append(ch)
        elif ch == "," and depth == 0:
            parts.append("".join(current))
            current = []
        else:
            current.append(ch)
    if current:
        parts.append("".join(current))
    return parts


# ---------------------------------------------------------------------------
# Pipeline executor
# ---------------------------------------------------------------------------

# Stats function implementations
_STATS_FUNCTIONS: dict[str, Callable[[list[float]], float]] = {
    "count": lambda vs: float(len(vs)),
    "avg": lambda vs: sum(vs) / len(vs) if vs else 0.0,
    "min": lambda vs: min(vs) if vs else 0.0,
    "max": lambda vs: max(vs) if vs else 0.0,
    "sum": lambda vs: sum(vs),
}


def execute_pipeline(
    commands: list[dict],
    log_events: list[dict],
) -> list[dict[str, str]]:
    """Execute a parsed Insights query pipeline against log events.

    Each log event is a dict with at least 'timestamp' and 'message' keys.
    Returns list of result rows, each a dict of {field: value}.
    """
    # Convert events to row dicts
    rows: list[dict] = []
    for event in log_events:
        row = {
            "timestamp": str(event.get("timestamp", "")),
            "message": event.get("message", ""),
            "logStream": event.get("logStreamName", ""),
            "ptr": event.get("eventId", ""),
        }
        rows.append(row)

    for cmd in commands:
        cmd_type = cmd["type"]
        if cmd_type == "fields":
            rows = _exec_fields(rows, cmd)
        elif cmd_type == "filter":
            rows = _exec_filter(rows, cmd)
        elif cmd_type == "stats":
            rows = _exec_stats(rows, cmd)
        elif cmd_type == "sort":
            rows = _exec_sort(rows, cmd)
        elif cmd_type == "limit":
            rows = rows[: cmd["count"]]
        elif cmd_type == "parse":
            rows = _exec_parse(rows, cmd)

    # Convert to result format [{field: value}]
    return [{k: str(v) for k, v in row.items()} for row in rows]


def _exec_fields(rows: list[dict], cmd: dict) -> list[dict]:
    """Select only the specified fields."""
    fields = cmd["fields"]
    result = []
    for row in rows:
        new_row = {}
        for f in fields:
            if f in row:
                new_row[f] = row[f]
            else:
                new_row[f] = ""
        result.append(new_row)
    return result


def _exec_filter(rows: list[dict], cmd: dict) -> list[dict]:
    """Filter rows based on the expression."""
    expr = cmd["expression"]
    result = []

    for row in rows:
        if _evaluate_filter(row, expr):
            result.append(row)

    return result


def _evaluate_filter(row: dict, expression: str) -> bool:
    """Evaluate a filter expression against a row.

    Supports:
    - @field like /pattern/  (regex match)
    - @field = "value"       (exact match)
    - @field != "value"      (not equal)
    - @field > N, @field < N, etc. (numeric comparison)
    - Simple string containment: @field like "text"
    """
    expression = expression.strip()

    # like with regex: @field like /pattern/
    like_regex = re.match(r"(@?\w+)\s+like\s+/(.+)/", expression, re.IGNORECASE)
    if like_regex:
        field = like_regex.group(1).lstrip("@")
        pattern = like_regex.group(2)
        val = str(row.get(field, ""))
        return bool(re.search(pattern, val))

    # like with string: @field like "text"
    like_str = re.match(r'(@?\w+)\s+like\s+"([^"]*)"', expression, re.IGNORECASE)
    if like_str:
        field = like_str.group(1).lstrip("@")
        text = like_str.group(2)
        val = str(row.get(field, ""))
        return text in val

    # Exact match: @field = "value"
    eq_match = re.match(r'(@?\w+)\s*=\s*"([^"]*)"', expression)
    if eq_match:
        field = eq_match.group(1).lstrip("@")
        value = eq_match.group(2)
        return str(row.get(field, "")) == value

    # Not equal: @field != "value"
    neq_match = re.match(r'(@?\w+)\s*!=\s*"([^"]*)"', expression)
    if neq_match:
        field = neq_match.group(1).lstrip("@")
        value = neq_match.group(2)
        return str(row.get(field, "")) != value

    # Numeric comparisons: @field > N, @field >= N, @field < N, @field <= N, @field = N
    num_match = re.match(r"(@?\w+)\s*(>=|<=|!=|>|<|=)\s*(-?\d+\.?\d*)", expression)
    if num_match:
        field = num_match.group(1).lstrip("@")
        op = num_match.group(2)
        threshold = float(num_match.group(3))
        try:
            val = float(row.get(field, 0))
        except (ValueError, TypeError):
            return False
        if op == ">":
            return val > threshold
        elif op == ">=":
            return val >= threshold
        elif op == "<":
            return val < threshold
        elif op == "<=":
            return val <= threshold
        elif op == "=":
            return val == threshold
        elif op == "!=":
            return val != threshold

    # Fallback: check if message contains the expression text
    return expression.strip('"').strip("'") in str(row.get("message", ""))


def _exec_stats(rows: list[dict], cmd: dict) -> list[dict]:
    """Execute stats aggregation with optional group-by."""
    aggregations = cmd["aggregations"]
    group_by = cmd.get("group_by", [])

    if group_by:
        groups: dict[tuple, list[dict]] = defaultdict(list)
        for row in rows:
            key = tuple(str(row.get(g, "")) for g in group_by)
            groups[key].append(row)

        result = []
        for key, group_rows in groups.items():
            out_row: dict[str, str] = {}
            for i, g in enumerate(group_by):
                out_row[g] = str(key[i])
            for agg in aggregations:
                agg_val = _compute_aggregation(agg, group_rows)
                label = f"{agg['func']}({agg['field']})"
                out_row[label] = str(agg_val)
            result.append(out_row)
        return result
    else:
        out_row = {}
        for agg in aggregations:
            agg_val = _compute_aggregation(agg, rows)
            label = f"{agg['func']}({agg['field']})"
            out_row[label] = str(agg_val)
        return [out_row]


def _compute_aggregation(agg: dict, rows: list[dict]) -> float:
    """Compute a single aggregation over rows."""
    func_name = agg["func"]
    field = agg["field"]
    func = _STATS_FUNCTIONS.get(func_name, _STATS_FUNCTIONS["count"])

    if field == "*":
        return func([1.0] * len(rows))

    values = []
    for row in rows:
        val = row.get(field)
        if val is not None:
            try:
                values.append(float(val))
            except (ValueError, TypeError) as exc:
                logger.debug("_compute_aggregation: append failed (non-fatal): %s", exc)

    if not values and func_name == "count":
        return 0.0
    return func(values) if values else 0.0


def _exec_sort(rows: list[dict], cmd: dict) -> list[dict]:
    """Sort rows by a field."""
    field = cmd["field"]
    reverse = cmd["order"] == "desc"

    def sort_key(row):
        val = row.get(field, "")
        try:
            return (0, float(val))
        except (ValueError, TypeError):
            return (1, str(val))

    return sorted(rows, key=sort_key, reverse=reverse)


def _exec_parse(rows: list[dict], cmd: dict) -> list[dict]:
    """Extract fields from a source field using a regex pattern."""
    source = cmd["source"]
    pattern = cmd["pattern"]
    field_names = cmd["fields"]

    result = []
    for row in rows:
        new_row = dict(row)
        val = str(row.get(source, ""))
        m = re.search(pattern, val)
        if m:
            groups = m.groups()
            for i, fname in enumerate(field_names):
                if i < len(groups):
                    new_row[fname] = groups[i] if groups[i] is not None else ""
                else:
                    new_row[fname] = ""
        else:
            for fname in field_names:
                new_row[fname] = ""
        result.append(new_row)
    return result


# ---------------------------------------------------------------------------
# Internal query execution
# ---------------------------------------------------------------------------


def _execute_query(query_id: str, region: str, account_id: str) -> None:
    """Execute a stored query against Moto's log backends."""
    with _query_lock:
        query = _queries.get(query_id)
        if not query:
            return

    query_string = query["query_string"]
    log_group_names = query["log_group_names"]
    limit = query.get("limit", 1000)

    # Parse the query
    commands = parse_query(query_string)

    # Collect log events from Moto
    all_events: list[dict] = []
    try:
        from moto.backends import get_backend

        logs_backend = get_backend("logs")[account_id][region]
        for lg_name in log_group_names:
            log_group = logs_backend.groups.get(lg_name)
            if not log_group:
                continue
            for stream in log_group.streams.values():
                for event in stream.events:
                    ts = getattr(event, "timestamp", 0)
                    msg = getattr(event, "message", "")
                    all_events.append(
                        {
                            "timestamp": ts,
                            "message": msg,
                            "logStreamName": stream.log_stream_name,
                            "eventId": getattr(event, "event_id", ""),
                        }
                    )
    except Exception as exc:  # noqa: BLE001
        logger.debug("_execute_query: get failed (non-fatal): %s", exc)

    # Execute pipeline
    results = execute_pipeline(commands, all_events)

    # Apply limit
    if limit and len(results) > limit:
        results = results[:limit]

    # Format results as list of list of {field, value} dicts
    formatted_results = []
    for row in results:
        formatted_row = [{"field": k, "value": v} for k, v in row.items()]
        formatted_results.append(formatted_row)

    with _query_lock:
        query = _queries.get(query_id)
        if query:
            query["results"] = formatted_results
            query["status"] = STATUS_COMPLETE
            query["statistics"] = {
                "recordsMatched": float(len(results)),
                "recordsScanned": float(len(all_events)),
                "bytesScanned": float(sum(len(str(e.get("message", ""))) for e in all_events)),
            }
