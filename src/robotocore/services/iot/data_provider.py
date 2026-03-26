"""Native IoT Data Plane provider.

Handles Publish to MQTT topics with rule evaluation and target dispatch.
Forwards GetThingShadow/UpdateThingShadow to Moto.
"""

import json
import logging
import re

from starlette.requests import Request
from starlette.responses import Response

from robotocore.services.iot.provider import get_all_rules
from robotocore.services.iot.rule_engine import evaluate_message
from robotocore.services.iot.target_dispatch import dispatch_actions

logger = logging.getLogger(__name__)


async def handle_iot_data_request(request: Request, region: str, account_id: str) -> Response:
    """Handle an IoT Data Plane API request (rest-json protocol)."""
    path = request.url.path
    method = request.method.upper()

    # Publish: POST /topics/{topic}
    topic_match = re.match(r"^/topics/(.+)$", path)
    if topic_match and method == "POST":
        topic = topic_match.group(1)
        return await _publish(request, topic, region, account_id)

    # Forward everything else (shadows, etc.) to Moto
    from robotocore.providers.moto_bridge import forward_to_moto

    return await forward_to_moto(request, "iot-data", account_id=account_id)


async def _publish(request: Request, topic: str, region: str, account_id: str) -> Response:
    """Publish a message to an MQTT topic and evaluate rules."""
    body = await request.body()

    # Parse query parameters
    query_params = dict(request.query_params)
    retain = query_params.get("retain", "false").lower() == "true"
    try:
        qos = int(query_params.get("qos", "0"))
    except (ValueError, TypeError):
        qos = 0

    # Store retained message in Moto backend if retain=True
    if retain:
        from moto.iotdata.models import iotdata_backends

        iotdata_backend = iotdata_backends[account_id][region]
        iotdata_backend.publish(topic=topic, payload=body, qos=qos, retain=True)

    # Parse payload for rule evaluation
    try:
        payload = json.loads(body) if body else {}
    except (json.JSONDecodeError, UnicodeDecodeError):
        payload = {"raw": body.decode("utf-8", errors="replace")}

    # Get all rules and evaluate
    rules = get_all_rules(region, account_id)
    matches = evaluate_message(rules, topic, payload)

    # Dispatch to matched rule targets
    for rule, extracted_payload in matches:
        try:
            dispatch_actions(
                actions=rule.actions,
                payload=extracted_payload,
                topic=topic,
                region=region,
                account_id=account_id,
                error_action=rule.error_action,
            )
        except Exception as exc:  # noqa: BLE001
            logger.warning("Failed to dispatch IoT rule '%s': %s", rule.rule_name, exc)

    return Response(content=b"", status_code=200, media_type="application/json")
