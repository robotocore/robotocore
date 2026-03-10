"""Native IoT provider with rule engine and target dispatch.

Intercepts CreateTopicRule, DeleteTopicRule, GetTopicRule, and ReplaceTopicRule
to maintain a local rule registry with parsed SQL. All other operations are
forwarded to Moto.
"""

import json
import logging
import re
import threading

from starlette.requests import Request
from starlette.responses import Response

from robotocore.services.iot.rule_engine import TopicRule, parse_sql

logger = logging.getLogger(__name__)

# Rule store: key = (account_id, region), value = dict of rule_name -> TopicRule
_rule_stores: dict[tuple[str, str], dict[str, TopicRule]] = {}
_store_lock = threading.Lock()


def _get_rules(region: str, account_id: str) -> dict[str, TopicRule]:
    """Get the rule store for a region/account."""
    key = (account_id, region)
    with _store_lock:
        if key not in _rule_stores:
            _rule_stores[key] = {}
        return _rule_stores[key]


def get_all_rules(region: str, account_id: str) -> list[TopicRule]:
    """Get all rules for a region/account (used by data provider for evaluation)."""
    rules = _get_rules(region, account_id)
    return list(rules.values())


async def handle_iot_request(request: Request, region: str, account_id: str) -> Response:
    """Handle an IoT API request (rest-json protocol)."""
    path = request.url.path
    method = request.method.upper()

    # Route based on path patterns (IoT uses REST-JSON protocol)
    # CreateTopicRule: POST /rules/{ruleName}
    # DeleteTopicRule: DELETE /rules/{ruleName}
    # GetTopicRule: GET /rules/{ruleName}
    # ReplaceTopicRule: PATCH /rules/{ruleName}
    # ListTopicRules: GET /rules

    rule_match = re.match(r"^/rules/([^/?]+)$", path)

    if rule_match:
        rule_name = rule_match.group(1)
        if method == "POST":
            return await _create_topic_rule(request, rule_name, region, account_id)
        if method == "GET":
            return await _get_topic_rule(request, rule_name, region, account_id)
        if method == "DELETE":
            return await _delete_topic_rule(request, rule_name, region, account_id)
        if method == "PATCH":
            return await _replace_topic_rule(request, rule_name, region, account_id)

    if path == "/rules" and method == "GET":
        return await _list_topic_rules(request, region, account_id)

    # Forward everything else to Moto
    from robotocore.providers.moto_bridge import forward_to_moto

    return await forward_to_moto(request, "iot", account_id=account_id)


async def _create_topic_rule(
    request: Request, rule_name: str, region: str, account_id: str
) -> Response:
    """Create a topic rule with SQL parsing."""
    body = await request.body()
    params = json.loads(body) if body else {}

    rule_payload = params.get("topicRulePayload", params)
    sql = rule_payload.get("sql", "")
    actions = rule_payload.get("actions", [])
    error_action = rule_payload.get("errorAction")
    description = rule_payload.get("description", "")
    enabled = not rule_payload.get("ruleDisabled", False)

    if not sql:
        return _error_response("InvalidRequestException", "SQL is required", 400)

    try:
        parsed = parse_sql(sql)
    except ValueError as exc:
        return _error_response("InvalidRequestException", str(exc), 400)

    rule_arn = f"arn:aws:iot:{region}:{account_id}:rule/{rule_name}"

    rule = TopicRule(
        rule_name=rule_name,
        sql=sql,
        parsed=parsed,
        actions=actions,
        error_action=error_action,
        enabled=enabled,
        description=description,
        rule_arn=rule_arn,
    )

    rules = _get_rules(region, account_id)
    rules[rule_name] = rule

    # Also forward to Moto so Get/List work via Moto too
    from robotocore.providers.moto_bridge import forward_to_moto

    await forward_to_moto(request, "iot", account_id=account_id)

    return Response(content=b"", status_code=200, media_type="application/json")


async def _get_topic_rule(
    request: Request, rule_name: str, region: str, account_id: str
) -> Response:
    """Get a topic rule -- try local store first, fall back to Moto."""
    rules = _get_rules(region, account_id)
    rule = rules.get(rule_name)

    if rule:
        result = {
            "rule": {
                "ruleName": rule.rule_name,
                "sql": rule.sql,
                "actions": rule.actions,
                "ruleDisabled": not rule.enabled,
                "description": rule.description,
            },
            "ruleArn": rule.rule_arn,
        }
        if rule.error_action:
            result["rule"]["errorAction"] = rule.error_action
        return Response(
            content=json.dumps(result),
            status_code=200,
            media_type="application/json",
        )

    # Fall back to Moto
    from robotocore.providers.moto_bridge import forward_to_moto

    return await forward_to_moto(request, "iot", account_id=account_id)


async def _delete_topic_rule(
    request: Request, rule_name: str, region: str, account_id: str
) -> Response:
    """Delete a topic rule from local store and Moto."""
    rules = _get_rules(region, account_id)
    rules.pop(rule_name, None)

    # Also forward to Moto
    from robotocore.providers.moto_bridge import forward_to_moto

    return await forward_to_moto(request, "iot", account_id=account_id)


async def _replace_topic_rule(
    request: Request, rule_name: str, region: str, account_id: str
) -> Response:
    """Replace (update) a topic rule."""
    body = await request.body()
    params = json.loads(body) if body else {}

    rule_payload = params.get("topicRulePayload", params)
    sql = rule_payload.get("sql", "")
    actions = rule_payload.get("actions", [])
    error_action = rule_payload.get("errorAction")
    description = rule_payload.get("description", "")
    enabled = not rule_payload.get("ruleDisabled", False)

    if not sql:
        return _error_response("InvalidRequestException", "SQL is required", 400)

    try:
        parsed = parse_sql(sql)
    except ValueError as exc:
        return _error_response("InvalidRequestException", str(exc), 400)

    rule_arn = f"arn:aws:iot:{region}:{account_id}:rule/{rule_name}"

    rule = TopicRule(
        rule_name=rule_name,
        sql=sql,
        parsed=parsed,
        actions=actions,
        error_action=error_action,
        enabled=enabled,
        description=description,
        rule_arn=rule_arn,
    )

    rules = _get_rules(region, account_id)
    rules[rule_name] = rule

    # Forward to Moto
    from robotocore.providers.moto_bridge import forward_to_moto

    await forward_to_moto(request, "iot", account_id=account_id)

    return Response(content=b"", status_code=200, media_type="application/json")


async def _list_topic_rules(request: Request, region: str, account_id: str) -> Response:
    """List topic rules -- forward to Moto."""
    from robotocore.providers.moto_bridge import forward_to_moto

    return await forward_to_moto(request, "iot", account_id=account_id)


def _error_response(code: str, message: str, status: int) -> Response:
    """Return a JSON error response."""
    return Response(
        content=json.dumps({"__type": code, "message": message}),
        status_code=status,
        media_type="application/json",
    )
