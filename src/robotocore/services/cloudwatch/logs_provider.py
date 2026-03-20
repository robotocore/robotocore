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

# Valid retention values per AWS API docs
_VALID_RETENTION_DAYS = {
    1,
    3,
    5,
    7,
    14,
    30,
    60,
    90,
    120,
    150,
    180,
    365,
    400,
    545,
    731,
    1096,
    1827,
    2192,
    2557,
    2922,
    3288,
    3653,
}


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
        return await forward_to_moto(request, "logs", account_id=account_id)

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
        except Exception as e:  # noqa: BLE001
            return _error_response("InternalError", str(e), 500)

    # PutRetentionPolicy: validate, then forward to Moto
    if action == "PutRetentionPolicy":
        retention_in_days = params.get("retentionInDays")
        if retention_in_days not in _VALID_RETENTION_DAYS:
            return _error_response(
                "InvalidParameterException",
                "1 validation error detected: Value at 'retentionInDays' failed to satisfy "
                "constraint: Member must satisfy enum value set: "
                "[1, 3, 5, 7, 14, 30, 60, 90, 120, 150, 180, 365, 400, 545, 731, 1096, "
                "1827, 2192, 2557, 2922, 3288, 3653]",
                400,
            )
        return await forward_to_moto(request, "logs", account_id=account_id)

    # FilterLogEvents: handle logStreamNamePrefix natively
    if action == "FilterLogEvents":
        log_stream_name_prefix = params.get("logStreamNamePrefix")
        if log_stream_name_prefix:
            return await _filter_log_events_with_prefix(
                request, params, region, account_id, log_stream_name_prefix
            )
        return await forward_to_moto(request, "logs", account_id=account_id)

    # PutLogEvents: forward to Moto, then process filters
    if action == "PutLogEvents":
        response = await forward_to_moto(request, "logs", account_id=account_id)
        if 200 <= response.status_code < 300:
            try:
                from robotocore.services.cloudwatch.filters import process_log_events

                log_group_name = params.get("logGroupName", "")
                log_stream_name = params.get("logStreamName", "")
                events = params.get("logEvents", [])
                process_log_events(log_group_name, log_stream_name, events, region, account_id)
            except Exception:  # noqa: BLE001
                logger.debug("Failed to process log filters", exc_info=True)
        return response

    # ListTagsForResource: normalize ARN (strip trailing :*) before forwarding
    if action == "ListTagsForResource":
        resource_arn = params.get("resourceArn", "")
        if resource_arn.endswith(":*"):
            resource_arn = resource_arn[:-2]
        try:
            from moto.backends import get_backend  # noqa: I001
            from moto.core import DEFAULT_ACCOUNT_ID

            acct = account_id or DEFAULT_ACCOUNT_ID
            logs_backend = get_backend("logs")[acct][region]
            tags = logs_backend.list_tags_for_resource(resource_arn)
            return _json_response(200, {"tags": tags})
        except Exception:  # noqa: BLE001
            return await forward_to_moto(request, "logs", account_id=account_id)

    # TagResource: normalize ARN (strip trailing :*) before forwarding
    if action == "TagResource":
        resource_arn = params.get("resourceArn", "")
        if resource_arn.endswith(":*"):
            resource_arn = resource_arn[:-2]
        tags = params.get("tags", {})
        try:
            from moto.backends import get_backend  # noqa: I001
            from moto.core import DEFAULT_ACCOUNT_ID

            acct = account_id or DEFAULT_ACCOUNT_ID
            logs_backend = get_backend("logs")[acct][region]
            logs_backend.tag_resource(resource_arn, tags)
            return _json_response(200, {})
        except Exception:  # noqa: BLE001
            return await forward_to_moto(request, "logs", account_id=account_id)

    # UntagResource: normalize ARN (strip trailing :*) before forwarding
    if action == "UntagResource":
        resource_arn = params.get("resourceArn", "")
        if resource_arn.endswith(":*"):
            resource_arn = resource_arn[:-2]
        tag_keys = params.get("tagKeys", [])
        try:
            from moto.backends import get_backend  # noqa: I001
            from moto.core import DEFAULT_ACCOUNT_ID

            acct = account_id or DEFAULT_ACCOUNT_ID
            logs_backend = get_backend("logs")[acct][region]
            logs_backend.untag_resource(resource_arn, tag_keys)
            return _json_response(200, {})
        except Exception:  # noqa: BLE001
            return await forward_to_moto(request, "logs", account_id=account_id)

    # Fall back to Moto for everything else
    return await forward_to_moto(request, "logs", account_id=account_id)


# ---------------------------------------------------------------------------
# FilterLogEvents with logStreamNamePrefix
# ---------------------------------------------------------------------------


async def _filter_log_events_with_prefix(
    request: Request,
    params: dict,
    region: str,
    account_id: str,
    prefix: str,
) -> Response:
    """Handle FilterLogEvents with logStreamNamePrefix by resolving streams first."""
    log_group_name = params.get("logGroupName", "")

    try:
        from moto.backends import get_backend  # noqa: I001
        from moto.core import DEFAULT_ACCOUNT_ID

        acct = account_id or DEFAULT_ACCOUNT_ID
        logs_backend = get_backend("logs")[acct][region]
        if log_group_name not in logs_backend.groups:
            return _error_response(
                "ResourceNotFoundException",
                "The specified log group does not exist.",
                400,
            )
        log_group = logs_backend.groups[log_group_name]
        matching_streams = [name for name in log_group.streams if name.startswith(prefix)]
    except Exception as e:  # noqa: BLE001
        return _error_response("InternalError", str(e), 500)

    # Call Moto's filter_log_events with resolved stream names
    start_time = params.get("startTime")
    end_time = params.get("endTime")
    limit = params.get("limit")
    next_token = params.get("nextToken")
    filter_pattern = params.get("filterPattern", "")
    interleaved = params.get("interleaved", False)

    try:
        events, next_token_out, searched_streams = logs_backend.filter_log_events(
            log_group_name,
            matching_streams,
            start_time,
            end_time,
            limit,
            next_token,
            filter_pattern,
            interleaved,
        )
        return _json_response(
            200,
            {
                "events": events,
                "nextToken": next_token_out,
                "searchedLogStreams": searched_streams,
            },
        )
    except Exception as e:  # noqa: BLE001
        return _error_response("InternalError", str(e), 500)


# ---------------------------------------------------------------------------
# KMS key operations
# ---------------------------------------------------------------------------


def _associate_kms_key(params: dict, region: str, account_id: str) -> dict:
    log_group_name = params.get("logGroupName", "")
    kms_key_id = params.get("kmsKeyId", "")

    if not log_group_name:
        raise LogsError("InvalidParameterException", "logGroupName is required")
    if not kms_key_id:
        raise LogsError("InvalidParameterException", "kmsKeyId is required")

    try:
        from moto.backends import get_backend  # noqa: I001
        from moto.core import DEFAULT_ACCOUNT_ID

        acct = account_id or DEFAULT_ACCOUNT_ID
        logs_backend = get_backend("logs")[acct][region]
        if log_group_name not in logs_backend.groups:
            raise LogsError(
                "ResourceNotFoundException",
                "The specified log group does not exist.",
            )
        logs_backend.groups[log_group_name].kms_key_id = kms_key_id
    except LogsError:
        raise
    except Exception as e:
        raise LogsError("InternalError", str(e), 500) from e

    return {}


def _disassociate_kms_key(params: dict, region: str, account_id: str) -> dict:
    log_group_name = params.get("logGroupName", "")

    if not log_group_name:
        raise LogsError("InvalidParameterException", "logGroupName is required")

    try:
        from moto.backends import get_backend  # noqa: I001
        from moto.core import DEFAULT_ACCOUNT_ID

        acct = account_id or DEFAULT_ACCOUNT_ID
        logs_backend = get_backend("logs")[acct][region]
        if log_group_name not in logs_backend.groups:
            raise LogsError(
                "ResourceNotFoundException",
                "The specified log group does not exist.",
            )
        logs_backend.groups[log_group_name].kms_key_id = None
    except LogsError:
        raise
    except Exception as e:
        raise LogsError("InternalError", str(e), 500) from e

    return {}


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

    store.put_metric_filter(log_group_name, filter_name, filter_pattern, metric_transformations)
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
    "AssociateKmsKey": _associate_kms_key,
    "DisassociateKmsKey": _disassociate_kms_key,
}
