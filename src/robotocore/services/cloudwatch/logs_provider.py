"""Native CloudWatch Logs provider.

Wraps Moto for standard operations (CreateLogGroup, CreateLogStream, etc.).
Intercepts PutLogEvents for filter processing.
Handles Insights and filter operations natively.
Uses JSON protocol (X-Amz-Target: Logs_20140328.{Action}).
"""

import json
import logging
from collections.abc import Callable

from starlette.requests import Request
from starlette.responses import Response

from robotocore.providers.moto_bridge import forward_to_moto
from robotocore.services.cloudwatch.filters import (
    get_filter_store,
)
from robotocore.services.cloudwatch.insights import (
    InsightsError,
    get_query_results,
    start_query,
    stop_query,
)

logger = logging.getLogger(__name__)


class LogsError(Exception):
    def __init__(self, code: str, message: str, status: int = 400):
        self.code = code
        self.message = message
        self.status = status


async def handle_logs_request(request: Request, region: str, account_id: str) -> Response:
    """Handle CloudWatch Logs API requests.

    Intercepts Insights, filter operations, and PutLogEvents.
    Falls back to Moto for everything else.
    """
    body = await request.body()
    target = request.headers.get("x-amz-target", "")

    if not target:
        return await forward_to_moto(request, "logs")

    action = target.split(".")[-1]
    params = json.loads(body) if body else {}

    handler = _ACTION_MAP.get(action)
    if handler is not None:
        try:
            result = handler(params, region, account_id)
            return _json_response(200, result)
        except LogsError as e:
            return _error_response(e.code, e.message, e.status)
        except InsightsError as e:
            return _error_response(e.code, e.message, 400)
        except Exception as e:
            return _error_response("InternalError", str(e), 500)

    # PutLogEvents: forward to Moto, then process filters
    if action == "PutLogEvents":
        response = await forward_to_moto(request, "logs")
        if 200 <= response.status_code < 300:
            try:
                from robotocore.services.cloudwatch.filters import process_log_events

                log_group_name = params.get("logGroupName", "")
                log_stream_name = params.get("logStreamName", "")
                events = params.get("logEvents", [])
                process_log_events(log_group_name, log_stream_name, events, region, account_id)
            except Exception:
                logger.debug("Failed to process log filters", exc_info=True)
        return response

    # Fall back to Moto for everything else
    return await forward_to_moto(request, "logs")


# ---------------------------------------------------------------------------
# Insights operations
# ---------------------------------------------------------------------------


def _start_query(params: dict, region: str, account_id: str) -> dict:
    log_group_names = params.get("logGroupNames", [])
    # Support single logGroupName
    if not log_group_names:
        single = params.get("logGroupName")
        if single:
            log_group_names = [single]

    query_string = params.get("queryString", "")
    start_time_val = params.get("startTime", 0)
    end_time_val = params.get("endTime", 0)
    limit = params.get("limit", 1000)

    if not query_string:
        raise LogsError("InvalidParameterException", "queryString is required")

    query_id = start_query(
        log_group_names=log_group_names,
        query_string=query_string,
        start_time=int(start_time_val),
        end_time=int(end_time_val),
        region=region,
        account_id=account_id,
        limit=int(limit),
    )
    return {"queryId": query_id}


def _get_query_results(params: dict, region: str, account_id: str) -> dict:
    query_id = params.get("queryId", "")
    if not query_id:
        raise LogsError("InvalidParameterException", "queryId is required")

    return get_query_results(query_id)


def _stop_query(params: dict, region: str, account_id: str) -> dict:
    query_id = params.get("queryId", "")
    if not query_id:
        raise LogsError("InvalidParameterException", "queryId is required")

    success = stop_query(query_id)
    return {"success": success}


# ---------------------------------------------------------------------------
# Filter operations
# ---------------------------------------------------------------------------


def _put_metric_filter(params: dict, region: str, account_id: str) -> dict:
    store = get_filter_store(region)
    log_group_name = params.get("logGroupName", "")
    filter_name = params.get("filterName", "")
    filter_pattern = params.get("filterPattern", "")
    metric_transformations = params.get("metricTransformations", [])

    if not log_group_name:
        raise LogsError("InvalidParameterException", "logGroupName is required")
    if not filter_name:
        raise LogsError("InvalidParameterException", "filterName is required")

    store.put_metric_filter(
        log_group_name, filter_name, filter_pattern, metric_transformations
    )
    return {}


def _delete_metric_filter(params: dict, region: str, account_id: str) -> dict:
    store = get_filter_store(region)
    log_group_name = params.get("logGroupName", "")
    filter_name = params.get("filterName", "")

    if not store.delete_metric_filter(log_group_name, filter_name):
        raise LogsError(
            "ResourceNotFoundException",
            f"Metric filter {filter_name} not found",
        )
    return {}


def _describe_metric_filters(params: dict, region: str, account_id: str) -> dict:
    store = get_filter_store(region)
    log_group_name = params.get("logGroupName")
    filter_name_prefix = params.get("filterNamePrefix")

    filters = store.describe_metric_filters(log_group_name, filter_name_prefix)
    return {
        "metricFilters": [
            {
                "filterName": mf.filter_name,
                "logGroupName": mf.log_group_name,
                "filterPattern": mf.filter_pattern,
                "metricTransformations": [
                    {
                        "metricName": mt.metric_name,
                        "metricNamespace": mt.metric_namespace,
                        "metricValue": mt.metric_value,
                    }
                    for mt in mf.metric_transformations
                ],
            }
            for mf in filters
        ]
    }


def _put_subscription_filter(params: dict, region: str, account_id: str) -> dict:
    store = get_filter_store(region)
    log_group_name = params.get("logGroupName", "")
    filter_name = params.get("filterName", "")
    filter_pattern = params.get("filterPattern", "")
    destination_arn = params.get("destinationArn", "")
    role_arn = params.get("roleArn", "")

    if not log_group_name:
        raise LogsError("InvalidParameterException", "logGroupName is required")
    if not filter_name:
        raise LogsError("InvalidParameterException", "filterName is required")
    if not destination_arn:
        raise LogsError("InvalidParameterException", "destinationArn is required")

    store.put_subscription_filter(
        log_group_name, filter_name, filter_pattern, destination_arn, role_arn
    )
    return {}


def _delete_subscription_filter(params: dict, region: str, account_id: str) -> dict:
    store = get_filter_store(region)
    log_group_name = params.get("logGroupName", "")
    filter_name = params.get("filterName", "")

    if not store.delete_subscription_filter(log_group_name, filter_name):
        raise LogsError(
            "ResourceNotFoundException",
            f"Subscription filter {filter_name} not found",
        )
    return {}


def _describe_subscription_filters(params: dict, region: str, account_id: str) -> dict:
    store = get_filter_store(region)
    log_group_name = params.get("logGroupName", "")

    filters = store.describe_subscription_filters(log_group_name)
    return {
        "subscriptionFilters": [
            {
                "filterName": sf.filter_name,
                "logGroupName": sf.log_group_name,
                "filterPattern": sf.filter_pattern,
                "destinationArn": sf.destination_arn,
                "roleArn": sf.role_arn,
                "distribution": sf.distribution,
            }
            for sf in filters
        ]
    }


# ---------------------------------------------------------------------------
# Response helpers
# ---------------------------------------------------------------------------


def _json_response(status: int, data: dict) -> Response:
    return Response(
        content=json.dumps(data),
        status_code=status,
        media_type="application/x-amz-json-1.1",
    )


def _error_response(code: str, message: str, status: int) -> Response:
    body = json.dumps({"__type": code, "message": message})
    return Response(content=body, status_code=status, media_type="application/x-amz-json-1.1")


# ---------------------------------------------------------------------------
# Action map
# ---------------------------------------------------------------------------

_ACTION_MAP: dict[str, Callable] = {
    "StartQuery": _start_query,
    "GetQueryResults": _get_query_results,
    "StopQuery": _stop_query,
    "PutMetricFilter": _put_metric_filter,
    "DeleteMetricFilter": _delete_metric_filter,
    "DescribeMetricFilters": _describe_metric_filters,
    "PutSubscriptionFilter": _put_subscription_filter,
    "DeleteSubscriptionFilter": _delete_subscription_filter,
    "DescribeSubscriptionFilters": _describe_subscription_filters,
}
