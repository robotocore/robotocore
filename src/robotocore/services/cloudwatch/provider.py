"""Native CloudWatch provider with deep features.

Handles composite alarms, metric math, dashboards, and enhanced alarm actions.
Falls back to Moto for standard operations (PutMetricAlarm, PutMetricData, etc.)
Uses query protocol (Action parameter).
"""

import json
import logging
import re
import threading
import time
from collections.abc import Callable
from urllib.parse import parse_qs

from starlette.requests import Request
from starlette.responses import Response

from robotocore.providers.moto_bridge import forward_to_moto
from robotocore.services.cloudwatch.metric_math import (
    MetricMathError,
    aggregate_values,
    evaluate_expression,
)

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# In-memory stores
# ---------------------------------------------------------------------------

# Composite alarms: {region: {alarm_name: CompositeAlarmData}}
_composite_alarms: dict[str, dict[str, dict]] = {}
_composite_lock = threading.Lock()

# Dashboards: {region: {dashboard_name: DashboardData}}
_dashboards: dict[str, dict[str, dict]] = {}
_dashboard_lock = threading.Lock()


class CloudWatchError(Exception):
    def __init__(self, code: str, message: str, status: int = 400):
        self.code = code
        self.message = message
        self.status = status


# ---------------------------------------------------------------------------
# Composite alarm rule parsing
# ---------------------------------------------------------------------------


def parse_alarm_rule(rule: str) -> dict:
    """Parse a composite alarm rule expression.

    Supports:
    - ALARM("alarm-name")
    - OK("alarm-name")
    - INSUFFICIENT_DATA("alarm-name")
    - AND, OR, NOT operators
    - Parenthesized groups

    Returns an AST dict: {"type": "AND"|"OR"|"NOT"|"STATE", ...}
    """
    tokens = _tokenize_rule(rule)
    ast, _ = _parse_or_expr(tokens, 0)
    return ast


def _tokenize_rule(rule: str) -> list[tuple[str, str]]:
    """Tokenize a composite alarm rule into (type, value) pairs."""
    tokens: list[tuple[str, str]] = []
    pos = 0
    rule = rule.strip()

    while pos < len(rule):
        if rule[pos].isspace():
            pos += 1
            continue

        # State functions: ALARM("..."), OK("..."), INSUFFICIENT_DATA("...")
        state_match = re.match(r'(ALARM|OK|INSUFFICIENT_DATA)\s*\(\s*"([^"]+)"\s*\)', rule[pos:])
        if state_match:
            tokens.append(("state_func", f"{state_match.group(1)}:{state_match.group(2)}"))
            pos += state_match.end()
            continue

        # Boolean operators
        if rule[pos:].startswith("AND"):
            tokens.append(("op", "AND"))
            pos += 3
            continue
        if rule[pos:].startswith("OR"):
            tokens.append(("op", "OR"))
            pos += 2
            continue
        if rule[pos:].startswith("NOT"):
            tokens.append(("op", "NOT"))
            pos += 3
            continue
        if rule[pos:].startswith("TRUE"):
            tokens.append(("bool", "TRUE"))
            pos += 4
            continue
        if rule[pos:].startswith("FALSE"):
            tokens.append(("bool", "FALSE"))
            pos += 5
            continue

        if rule[pos] == "(":
            tokens.append(("lparen", "("))
            pos += 1
            continue
        if rule[pos] == ")":
            tokens.append(("rparen", ")"))
            pos += 1
            continue

        raise CloudWatchError(
            "InvalidParameterValue",
            f"Unexpected character in alarm rule at position {pos}: {rule[pos : pos + 10]}",
        )

    return tokens


def _parse_or_expr(tokens: list[tuple[str, str]], pos: int) -> tuple[dict, int]:
    """Parse OR expression (lowest precedence)."""
    left, pos = _parse_and_expr(tokens, pos)

    while pos < len(tokens) and tokens[pos] == ("op", "OR"):
        pos += 1
        right, pos = _parse_and_expr(tokens, pos)
        left = {"type": "OR", "left": left, "right": right}

    return left, pos


def _parse_and_expr(tokens: list[tuple[str, str]], pos: int) -> tuple[dict, int]:
    """Parse AND expression."""
    left, pos = _parse_not_expr(tokens, pos)

    while pos < len(tokens) and tokens[pos] == ("op", "AND"):
        pos += 1
        right, pos = _parse_not_expr(tokens, pos)
        left = {"type": "AND", "left": left, "right": right}

    return left, pos


def _parse_not_expr(tokens: list[tuple[str, str]], pos: int) -> tuple[dict, int]:
    """Parse NOT expression."""
    if pos < len(tokens) and tokens[pos] == ("op", "NOT"):
        pos += 1
        child, pos = _parse_primary_rule(tokens, pos)
        return {"type": "NOT", "child": child}, pos
    return _parse_primary_rule(tokens, pos)


def _parse_primary_rule(tokens: list[tuple[str, str]], pos: int) -> tuple[dict, int]:
    """Parse primary: state function, boolean, or parenthesized expression."""
    if pos >= len(tokens):
        raise CloudWatchError("InvalidParameterValue", "Unexpected end of alarm rule")

    tok_type, tok_val = tokens[pos]

    if tok_type == "state_func":
        state, alarm_name = tok_val.split(":", 1)
        return {"type": "STATE", "state": state, "alarm_name": alarm_name}, pos + 1

    if tok_type == "bool":
        return {"type": "BOOL", "value": tok_val == "TRUE"}, pos + 1

    if tok_type == "lparen":
        pos += 1
        expr, pos = _parse_or_expr(tokens, pos)
        if pos < len(tokens) and tokens[pos][0] == "rparen":
            pos += 1
        return expr, pos

    raise CloudWatchError(
        "InvalidParameterValue",
        f"Unexpected token in alarm rule: {tok_type}={tok_val}",
    )


def evaluate_alarm_rule(rule_ast: dict, alarm_states: dict[str, str]) -> str:
    """Evaluate a parsed composite alarm rule against current alarm states.

    Args:
        rule_ast: Parsed AST from parse_alarm_rule()
        alarm_states: Dict mapping alarm_name -> state ("OK", "ALARM", "INSUFFICIENT_DATA")

    Returns:
        "ALARM" if the rule evaluates to True, "OK" if False.
    """
    result = _eval_node(rule_ast, alarm_states)
    return "ALARM" if result else "OK"


def _eval_node(node: dict, states: dict[str, str]) -> bool:
    """Recursively evaluate an AST node."""
    node_type = node["type"]

    if node_type == "STATE":
        expected_state = node["state"]
        alarm_name = node["alarm_name"]
        actual_state = states.get(alarm_name, "INSUFFICIENT_DATA")
        return actual_state == expected_state

    if node_type == "AND":
        return _eval_node(node["left"], states) and _eval_node(node["right"], states)

    if node_type == "OR":
        return _eval_node(node["left"], states) or _eval_node(node["right"], states)

    if node_type == "NOT":
        return not _eval_node(node["child"], states)

    if node_type == "BOOL":
        return node["value"]

    return False


# ---------------------------------------------------------------------------
# Composite alarm CRUD
# ---------------------------------------------------------------------------


def _get_composite_store(region: str) -> dict[str, dict]:
    with _composite_lock:
        if region not in _composite_alarms:
            _composite_alarms[region] = {}
        return _composite_alarms[region]


def put_composite_alarm(params: dict, region: str, account_id: str) -> dict:
    """Create or update a composite alarm."""
    alarm_name = params.get("AlarmName", "")
    alarm_rule = params.get("AlarmRule", "")

    if not alarm_name:
        raise CloudWatchError("ValidationError", "AlarmName is required")
    if not alarm_rule:
        raise CloudWatchError("ValidationError", "AlarmRule is required")

    # Validate the rule parses
    try:
        rule_ast = parse_alarm_rule(alarm_rule)
    except CloudWatchError:
        raise
    except Exception as e:
        raise CloudWatchError("InvalidParameterValue", f"Invalid AlarmRule: {e}")

    store = _get_composite_store(region)
    alarm_arn = f"arn:aws:cloudwatch:{region}:{account_id}:alarm:{alarm_name}"

    alarm_data = {
        "AlarmName": alarm_name,
        "AlarmArn": alarm_arn,
        "AlarmRule": alarm_rule,
        "AlarmDescription": params.get("AlarmDescription", ""),
        "ActionsEnabled": params.get("ActionsEnabled", True),
        "AlarmActions": params.get("AlarmActions", []),
        "OKActions": params.get("OKActions", []),
        "InsufficientDataActions": params.get("InsufficientDataActions", []),
        "StateValue": "INSUFFICIENT_DATA",
        "StateReason": "Unchecked: Initial state",
        "rule_ast": rule_ast,
    }

    with _composite_lock:
        store[alarm_name] = alarm_data

    return {}


def describe_composite_alarms(params: dict, region: str, account_id: str) -> list[dict]:
    """Return composite alarms, optionally filtered by name prefix or names."""
    store = _get_composite_store(region)
    prefix = params.get("AlarmNamePrefix", "")
    alarm_names = params.get("AlarmNames", [])

    results = []
    with _composite_lock:
        for alarm in store.values():
            if prefix and not alarm["AlarmName"].startswith(prefix):
                continue
            if alarm_names and alarm["AlarmName"] not in alarm_names:
                continue
            results.append({k: v for k, v in alarm.items() if k != "rule_ast"})
    return results


def delete_composite_alarms(alarm_names: list[str], region: str) -> None:
    """Delete composite alarms by name."""
    store = _get_composite_store(region)
    with _composite_lock:
        for name in alarm_names:
            store.pop(name, None)


# ---------------------------------------------------------------------------
# Dashboard CRUD
# ---------------------------------------------------------------------------


def _get_dashboard_store(region: str) -> dict[str, dict]:
    with _dashboard_lock:
        if region not in _dashboards:
            _dashboards[region] = {}
        return _dashboards[region]


def put_dashboard(params: dict, region: str, account_id: str) -> dict:
    """Create or update a dashboard."""
    name = params.get("DashboardName", "")
    body = params.get("DashboardBody", "")

    if not name:
        raise CloudWatchError("InvalidParameterInput", "DashboardName is required")
    if not body:
        raise CloudWatchError("InvalidParameterInput", "DashboardBody is required")

    # Validate JSON body
    try:
        parsed = json.loads(body)
        if not isinstance(parsed, dict):
            raise CloudWatchError("InvalidParameterInput", "DashboardBody must be a JSON object")
        if "widgets" not in parsed:
            raise CloudWatchError(
                "InvalidParameterInput",
                "DashboardBody must contain a 'widgets' key",
            )
        if not isinstance(parsed["widgets"], list):
            raise CloudWatchError(
                "InvalidParameterInput",
                "DashboardBody 'widgets' must be a list",
            )
        if len(parsed["widgets"]) == 0:
            raise CloudWatchError(
                "InvalidParameterInput",
                "DashboardBody 'widgets' must not be empty",
            )
    except json.JSONDecodeError as e:
        raise CloudWatchError("InvalidParameterInput", f"Invalid JSON in DashboardBody: {e}")

    arn = f"arn:aws:cloudwatch::{account_id}:dashboard/{name}"
    store = _get_dashboard_store(region)

    with _dashboard_lock:
        store[name] = {
            "DashboardName": name,
            "DashboardArn": arn,
            "DashboardBody": body,
            "LastModified": time.time(),
            "Size": len(body),
        }

    return {"DashboardValidationMessages": []}


def get_dashboard(params: dict, region: str, account_id: str) -> dict:
    """Get a dashboard by name."""
    name = params.get("DashboardName", "")
    store = _get_dashboard_store(region)

    with _dashboard_lock:
        dash = store.get(name)

    if not dash:
        raise CloudWatchError("ResourceNotFound", f"Dashboard {name} does not exist")

    return {
        "DashboardName": dash["DashboardName"],
        "DashboardArn": dash["DashboardArn"],
        "DashboardBody": dash["DashboardBody"],
    }


def list_dashboards(params: dict, region: str, account_id: str) -> list[dict]:
    """List dashboards, optionally filtered by prefix."""
    prefix = params.get("DashboardNamePrefix", "")
    store = _get_dashboard_store(region)

    results = []
    with _dashboard_lock:
        for dash in store.values():
            if prefix and not dash["DashboardName"].startswith(prefix):
                continue
            results.append(
                {
                    "DashboardName": dash["DashboardName"],
                    "DashboardArn": dash["DashboardArn"],
                    "LastModified": dash["LastModified"],
                    "Size": dash["Size"],
                }
            )
    return results


def delete_dashboards(params: dict, region: str, account_id: str) -> dict:
    """Delete dashboards by name."""
    names = params.get("DashboardNames", [])
    store = _get_dashboard_store(region)

    with _dashboard_lock:
        for name in names:
            if name not in store:
                raise CloudWatchError(
                    "ResourceNotFound",
                    f"Dashboard {name} does not exist",
                )
            store.pop(name, None)

    return {}


# ---------------------------------------------------------------------------
# GetMetricData with metric math
# ---------------------------------------------------------------------------


def get_metric_data(params: dict, region: str, account_id: str) -> dict:
    """Handle GetMetricData with metric math expression support."""
    metric_data_queries = params.get("MetricDataQueries", [])

    # First pass: collect data from Moto for queries with MetricStat
    collected_data: dict[str, list[float]] = {}
    labels: dict[str, str] = {}

    from moto.backends import get_backend

    try:
        cw_backend = get_backend("cloudwatch")[account_id][region]
    except (KeyError, TypeError):
        # Return empty results if backend not available
        return {
            "MetricDataResults": [],
            "Messages": [],
        }

    for query in metric_data_queries:
        query_id = query.get("Id", "")
        label = query.get("Label", query_id)
        labels[query_id] = label

        metric_stat = query.get("MetricStat")
        if metric_stat:
            # Collect from Moto backend
            metric = metric_stat.get("Metric", {})
            stat = metric_stat.get("Stat", "Average")
            period = metric_stat.get("Period", 60)
            namespace = metric.get("Namespace", "")
            metric_name = metric.get("MetricName", "")
            dimensions = metric.get("Dimensions", [])

            values = _collect_metric_values_from_backend(
                cw_backend, namespace, metric_name, dimensions, stat, period
            )
            collected_data[query_id] = values

    # Second pass: evaluate expressions
    results = []
    for query in metric_data_queries:
        query_id = query.get("Id", "")
        label = labels.get(query_id, query_id)
        expression = query.get("Expression")

        if expression:
            try:
                values = evaluate_expression(expression, collected_data)
                collected_data[query_id] = values
            except MetricMathError:
                values = []
        else:
            values = collected_data.get(query_id, [])

        timestamps = list(range(len(values)))
        results.append(
            {
                "Id": query_id,
                "Label": label,
                "Timestamps": timestamps,
                "Values": values,
                "StatusCode": "Complete",
            }
        )

    return {
        "MetricDataResults": results,
        "Messages": [],
    }


def _collect_metric_values_from_backend(
    backend,
    namespace: str,
    metric_name: str,
    dimensions: list[dict],
    stat: str,
    period: int,
) -> list[float]:
    """Collect metric values from Moto's in-memory store."""
    values: list[float] = []

    all_data = backend.metric_data + backend.aws_metric_data
    for datum in all_data:
        if datum.namespace != namespace:
            continue
        if datum.name != metric_name:
            continue
        if dimensions:
            dim_match = True
            expected = {(d["Name"], d["Value"]) for d in dimensions}
            actual = {(d.name, d.value) for d in datum.dimensions}
            if not expected.issubset(actual):
                dim_match = False
            if not dim_match:
                continue
        if hasattr(datum, "value") and datum.value is not None:
            values.append(float(datum.value))

    if values:
        return aggregate_values(values, period, stat)
    return []


# ---------------------------------------------------------------------------
# Enhanced alarm actions
# ---------------------------------------------------------------------------


def dispatch_alarm_actions(
    alarm_name: str,
    alarm_data: dict,
    old_state: str,
    new_state: str,
    reason: str,
    region: str,
    account_id: str,
) -> list[str]:
    """Dispatch actions for alarm state transition.

    Supports SNS, Lambda, and EC2 actions.
    Returns list of action ARNs that were dispatched.
    """
    if new_state == "ALARM":
        action_arns = alarm_data.get("AlarmActions", [])
    elif new_state == "OK":
        action_arns = alarm_data.get("OKActions", [])
    elif new_state == "INSUFFICIENT_DATA":
        action_arns = alarm_data.get("InsufficientDataActions", [])
    else:
        return []

    dispatched = []
    for arn in action_arns:
        try:
            _dispatch_single_action(
                arn, alarm_name, old_state, new_state, reason, region, account_id
            )
            dispatched.append(arn)
        except Exception:
            logger.exception("Failed to dispatch alarm action to %s", arn)

    return dispatched


def _dispatch_single_action(
    arn: str,
    alarm_name: str,
    old_state: str,
    new_state: str,
    reason: str,
    region: str,
    account_id: str,
) -> None:
    """Dispatch a single alarm action."""
    if ":sns:" in arn:
        _dispatch_sns_action(arn, alarm_name, old_state, new_state, reason, account_id, region)
    elif ":lambda:" in arn:
        _dispatch_lambda_action(arn, alarm_name, old_state, new_state, reason, region, account_id)
    elif arn.startswith("arn:aws:automate:") and "ec2:" in arn:
        _dispatch_ec2_action(arn, alarm_name, region, account_id)
    else:
        logger.warning("Unsupported alarm action type: %s", arn)


def _dispatch_sns_action(
    topic_arn: str,
    alarm_name: str,
    old_state: str,
    new_state: str,
    reason: str,
    account_id: str,
    region: str,
) -> None:
    """Publish alarm notification to SNS."""
    arn_match = re.match(r"arn:aws:sns:([^:]+):([^:]+):(.+)", topic_arn)
    if not arn_match:
        return

    sns_region = arn_match.group(1)
    sns_account = arn_match.group(2)

    message = json.dumps(
        {
            "AlarmName": alarm_name,
            "NewStateValue": new_state,
            "OldStateValue": old_state,
            "NewStateReason": reason,
        }
    )

    try:
        from moto.backends import get_backend

        sns_backend = get_backend("sns")[sns_account][sns_region]
        sns_backend.publish(message=message, arn=topic_arn, subject=f"ALARM: {alarm_name}")
    except Exception:
        logger.debug("Failed to publish alarm to SNS %s", topic_arn, exc_info=True)


def _dispatch_lambda_action(
    function_arn: str,
    alarm_name: str,
    old_state: str,
    new_state: str,
    reason: str,
    region: str,
    account_id: str,
) -> None:
    """Invoke a Lambda function as alarm action."""
    try:
        from robotocore.services.lambda_.invoke import invoke_lambda_async

        event = {
            "source": "aws.cloudwatch",
            "alarmName": alarm_name,
            "newState": new_state,
            "oldState": old_state,
            "reason": reason,
        }
        invoke_lambda_async(function_arn, event, region, account_id)
    except Exception:
        logger.debug("Failed to invoke Lambda %s", function_arn, exc_info=True)


def _dispatch_ec2_action(
    arn: str,
    alarm_name: str,
    region: str,
    account_id: str,
) -> None:
    """Simulate EC2 alarm actions (stop, terminate, reboot)."""
    action = ""
    if "ec2:stop" in arn.lower():
        action = "stop"
    elif "ec2:terminate" in arn.lower():
        action = "terminate"
    elif "ec2:reboot" in arn.lower():
        action = "reboot"

    logger.info(
        "Simulated EC2 %s action for alarm %s (arn=%s)",
        action,
        alarm_name,
        arn,
    )


# ---------------------------------------------------------------------------
# Main request handler (query protocol)
# ---------------------------------------------------------------------------


async def handle_cloudwatch_request(request: Request, region: str, account_id: str) -> Response:
    """Handle CloudWatch API requests.

    Intercepts composite alarms, dashboards, GetMetricData with math,
    and enhanced alarm actions. Falls back to Moto for standard operations.
    """
    body = await request.body()
    content_type = request.headers.get("content-type", "")
    amz_target = request.headers.get("x-amz-target", "")
    use_json_protocol = "x-amz-json" in content_type and amz_target

    if use_json_protocol:
        # Modern boto3 sends JSON with X-Amz-Target header
        # e.g. "GraniteServiceVersion20100801.DisableAlarmActions"
        action = amz_target.rsplit(".", 1)[-1] if "." in amz_target else amz_target
        params = json.loads(body.decode()) if body else {}
        params["Action"] = action
    else:
        # Traditional query protocol
        if "x-www-form-urlencoded" in content_type:
            parsed = parse_qs(body.decode(), keep_blank_values=True)
        else:
            parsed = parse_qs(str(request.url.query), keep_blank_values=True)
        params = _flatten_query_params(parsed)
        action = params.get("Action", "")

    # DescribeAlarms: intercept if CompositeAlarm type requested, else Moto
    if action == "DescribeAlarms":
        alarm_types = params.get("AlarmTypes", [])
        if isinstance(alarm_types, str):
            alarm_types = [alarm_types]
        if "CompositeAlarm" in alarm_types:
            composites = describe_composite_alarms(params, region, account_id)
            result = {"CompositeAlarms": composites, "MetricAlarms": []}
            if use_json_protocol:
                return Response(
                    content=json.dumps(result),
                    status_code=200,
                    media_type="application/x-amz-json-1.0",
                )
            return _xml_response("DescribeAlarmsResponse", result)

    # GetMetricStatistics: intercept when ExtendedStatistics requested (Moto doesn't compute them)
    if action == "GetMetricStatistics":
        ext_stats = params.get("ExtendedStatistics", [])
        # Query protocol: ExtendedStatistics.member.1, ExtendedStatistics.member.2 etc.
        if not ext_stats:
            ext_list = []
            i = 1
            while True:
                key = f"ExtendedStatistics.member.{i}"
                if key in params:
                    ext_list.append(params[key])
                    i += 1
                else:
                    break
            if ext_list:
                ext_stats = ext_list
        if ext_stats:
            params["ExtendedStatistics"] = ext_stats
            try:
                result = _handle_get_metric_statistics(params, region, account_id)
                if use_json_protocol:
                    return Response(
                        content=json.dumps(result),
                        status_code=200,
                        media_type="application/x-amz-json-1.0",
                    )
                return _xml_response("GetMetricStatisticsResponse", result)
            except Exception as e:
                logger.error("ExtendedStatistics error: %s", e)

    handler = _ACTION_MAP.get(action)
    if handler is not None:
        try:
            result = handler(params, region, account_id)
            if use_json_protocol:
                return Response(
                    content=json.dumps(result),
                    status_code=200,
                    media_type="application/x-amz-json-1.0",
                )
            return _xml_response(action + "Response", result)
        except CloudWatchError as e:
            if use_json_protocol:
                err_body = json.dumps({"__type": e.code, "message": e.message})
                return Response(
                    content=err_body, status_code=e.status, media_type="application/x-amz-json-1.0"
                )
            return _error_response(e.code, e.message, e.status)
        except Exception as e:
            if use_json_protocol:
                err_body = json.dumps({"__type": "InternalError", "message": str(e)})
                return Response(
                    content=err_body, status_code=500, media_type="application/x-amz-json-1.0"
                )
            return _error_response("InternalError", str(e), 500)

    # Fall back to Moto for everything else
    return await forward_to_moto(request, "cloudwatch")


def _flatten_query_params(parsed: dict) -> dict:
    """Flatten query string parameters, extracting lists and nested structures."""
    params: dict = {}
    for key, values in parsed.items():
        if len(values) == 1:
            params[key] = values[0]
        else:
            params[key] = values

    # Extract list parameters (e.g. AlarmNames.member.1, AlarmNames.member.2)
    list_params: dict[str, dict[int, str]] = {}
    for key, value in list(params.items()):
        list_match = re.match(r"(.+)\.member\.(\d+)(\.(.+))?", key)
        if list_match:
            list_name = list_match.group(1)
            index = int(list_match.group(2))
            sub_key = list_match.group(4)
            if list_name not in list_params:
                list_params[list_name] = {}
            if sub_key:
                if index not in list_params[list_name]:
                    list_params[list_name][index] = {}
                list_params[list_name][index] = {
                    **(
                        list_params[list_name].get(index, {})
                        if isinstance(list_params[list_name].get(index), dict)
                        else {}
                    ),
                    sub_key: value,
                }
            else:
                list_params[list_name][index] = value

    for list_name, indexed in list_params.items():
        sorted_values = [indexed[k] for k in sorted(indexed.keys())]
        params[list_name] = sorted_values

    return params


# ---------------------------------------------------------------------------
# Action handlers
# ---------------------------------------------------------------------------


def _handle_put_composite_alarm(params: dict, region: str, account_id: str) -> dict:
    return put_composite_alarm(params, region, account_id)


def _handle_describe_alarms(params: dict, region: str, account_id: str) -> dict:
    """Describe alarms including composite alarms."""
    alarm_type = params.get("AlarmTypes", "")

    # If specifically requesting CompositeAlarm type, return our composites
    if alarm_type == "CompositeAlarm" or (
        isinstance(alarm_type, list) and "CompositeAlarm" in alarm_type
    ):
        composites = describe_composite_alarms(params, region, account_id)
        return {
            "CompositeAlarms": composites,
            "MetricAlarms": [],
        }

    # Otherwise return composites alongside Moto results
    composites = describe_composite_alarms(params, region, account_id)
    return {
        "CompositeAlarms": composites,
    }


def _handle_delete_alarms(params: dict, region: str, account_id: str) -> dict:
    """Delete alarms — handles both composite and metric alarms."""
    alarm_names = params.get("AlarmNames", [])
    if isinstance(alarm_names, str):
        alarm_names = [alarm_names]

    # Delete any composite alarms with these names
    delete_composite_alarms(alarm_names, region)

    # Also delete from Moto (for metric alarms)
    try:
        from moto.backends import get_backend

        backend = get_backend("cloudwatch")[account_id][region]
        for name in alarm_names:
            backend.delete_alarms([name])
    except Exception:
        pass

    return {}


def _handle_put_dashboard(params: dict, region: str, account_id: str) -> dict:
    return put_dashboard(params, region, account_id)


def _handle_get_dashboard(params: dict, region: str, account_id: str) -> dict:
    return get_dashboard(params, region, account_id)


def _handle_list_dashboards(params: dict, region: str, account_id: str) -> dict:
    entries = list_dashboards(params, region, account_id)
    return {"DashboardEntries": entries}


def _handle_delete_dashboards(params: dict, region: str, account_id: str) -> dict:
    return delete_dashboards(params, region, account_id)


def _handle_get_metric_data(params: dict, region: str, account_id: str) -> dict:
    return get_metric_data(params, region, account_id)


def _handle_enable_alarm_actions(params: dict, region: str, account_id: str) -> dict:
    """Enable actions for the specified alarms."""
    alarm_names = params.get("AlarmNames", [])
    if isinstance(alarm_names, str):
        alarm_names = [alarm_names]
    from moto.backends import get_backend

    backend = get_backend("cloudwatch")[account_id][region]
    for name in alarm_names:
        if name in backend.alarms:
            backend.alarms[name].actions_enabled = True
    return {}


def _handle_disable_alarm_actions(params: dict, region: str, account_id: str) -> dict:
    """Disable actions for the specified alarms."""
    alarm_names = params.get("AlarmNames", [])
    if isinstance(alarm_names, str):
        alarm_names = [alarm_names]
    from moto.backends import get_backend

    backend = get_backend("cloudwatch")[account_id][region]
    for name in alarm_names:
        if name in backend.alarms:
            backend.alarms[name].actions_enabled = False
    return {}


def _handle_describe_alarm_history(params: dict, region: str, account_id: str) -> dict:
    """Return alarm history (stub — returns empty list)."""
    return {"AlarmHistoryItems": []}


# ---------------------------------------------------------------------------
# Response helpers
# ---------------------------------------------------------------------------


def _xml_response(wrapper: str, data: dict) -> Response:
    """Build a simple XML response for query protocol."""
    result_name = wrapper.replace("Response", "Result")
    xml = f'<{wrapper} xmlns="http://monitoring.amazonaws.com/doc/2010-08-01/">'
    xml += f"<{result_name}>"
    xml += _dict_to_xml(data)
    xml += f"</{result_name}>"
    xml += "<ResponseMetadata><RequestId>00000000-0000-0000-0000-000000000000</RequestId>"
    xml += "</ResponseMetadata>"
    xml += f"</{wrapper}>"
    return Response(content=xml, status_code=200, media_type="text/xml")


def _dict_to_xml(data) -> str:
    """Convert a dict/list to XML string."""
    if data is None:
        return ""
    if isinstance(data, dict):
        parts = []
        for key, value in data.items():
            if isinstance(value, list):
                parts.append(f"<{key}>")
                for item in value:
                    parts.append(f"<member>{_dict_to_xml(item)}</member>")
                parts.append(f"</{key}>")
            elif isinstance(value, dict):
                parts.append(f"<{key}>{_dict_to_xml(value)}</{key}>")
            elif isinstance(value, bool):
                parts.append(f"<{key}>{'true' if value else 'false'}</{key}>")
            else:
                parts.append(f"<{key}>{_escape_xml(str(value))}</{key}>")
        return "".join(parts)
    return _escape_xml(str(data))


def _escape_xml(s: str) -> str:
    return s.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")


def _error_response(code: str, message: str, status: int) -> Response:
    xml = f"""<ErrorResponse>
  <Error>
    <Code>{_escape_xml(code)}</Code>
    <Message>{_escape_xml(message)}</Message>
  </Error>
  <RequestId>00000000-0000-0000-0000-000000000000</RequestId>
</ErrorResponse>"""
    return Response(content=xml, status_code=status, media_type="text/xml")


# ---------------------------------------------------------------------------
# ExtendedStatistics (percentiles)
# ---------------------------------------------------------------------------


def _handle_get_metric_statistics(params: dict, region: str, account_id: str) -> dict:
    """GetMetricStatistics with ExtendedStatistics support."""
    from moto.backends import get_backend

    backend = get_backend("cloudwatch")[account_id][region]
    namespace = params.get("Namespace", "")
    metric_name = params.get("MetricName", "")
    extended_stats = params.get("ExtendedStatistics", [])
    if isinstance(extended_stats, str):
        extended_stats = [extended_stats]

    # Collect all raw values from Moto's metric data store
    values = []
    for md in getattr(backend, "metric_data", []):
        if md.namespace == namespace and md.name == metric_name:
            if hasattr(md, "value") and md.value is not None:
                values.append(float(md.value))
            elif hasattr(md, "statistics") and md.statistics:
                # Handle StatisticValues
                count = int(getattr(md.statistics, "sample_count", 1) or 1)
                avg = float(getattr(md.statistics, "sum", 0) or 0) / max(count, 1)
                values.extend([avg] * count)

    if not values:
        return {"Label": metric_name, "Datapoints": []}

    values.sort()
    ext_stats_result = {}
    for stat in extended_stats:
        # Parse percentile: "p50", "p99", "p99.9", etc.
        if stat.startswith("p"):
            try:
                pct = float(stat[1:])
                idx = (pct / 100.0) * (len(values) - 1)
                lower = int(idx)
                upper = min(lower + 1, len(values) - 1)
                frac = idx - lower
                val = values[lower] * (1 - frac) + values[upper] * frac
                ext_stats_result[stat] = val
            except (ValueError, IndexError):
                ext_stats_result[stat] = 0.0

    # Build single aggregated datapoint
    import datetime as dt

    datapoint = {
        "Timestamp": dt.datetime.now(dt.UTC).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "Unit": "None",
        "ExtendedStatistics": ext_stats_result,
    }
    # Try to get unit from first metric
    for md in getattr(backend, "metric_data", []):
        if md.namespace == namespace and md.name == metric_name:
            if hasattr(md, "unit") and md.unit:
                datapoint["Unit"] = md.unit
            break

    return {"Label": metric_name, "Datapoints": [datapoint]}


# ---------------------------------------------------------------------------
# Action map
# ---------------------------------------------------------------------------

_ACTION_MAP: dict[str, Callable] = {
    "PutCompositeAlarm": _handle_put_composite_alarm,
    "DeleteAlarms": _handle_delete_alarms,
    "PutDashboard": _handle_put_dashboard,
    "GetDashboard": _handle_get_dashboard,
    "ListDashboards": _handle_list_dashboards,
    "DeleteDashboards": _handle_delete_dashboards,
    "GetMetricData": _handle_get_metric_data,
    "EnableAlarmActions": _handle_enable_alarm_actions,
    "DisableAlarmActions": _handle_disable_alarm_actions,
    "DescribeAlarmHistory": _handle_describe_alarm_history,
}
