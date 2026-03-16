"""Native Resource Groups Tagging API provider.

Augments Moto's resourcegroupstaggingapi results with data from
native providers (SQS, SNS, etc.) whose tags are stored outside Moto.
"""

import json
import logging

from starlette.requests import Request
from starlette.responses import Response

from robotocore.providers.moto_bridge import forward_to_moto

logger = logging.getLogger(__name__)


async def handle_tagging_request(request: Request, region: str, account_id: str) -> Response:
    """Handle Resource Groups Tagging API requests."""
    body = await request.body()
    target = request.headers.get("x-amz-target", "")
    action = target.split(".")[-1] if target else ""

    if action == "GetResources":
        return await _get_resources(request, body, region, account_id)
    if action == "GetTagKeys":
        return await _get_tag_keys(request, body, region, account_id)

    # Everything else (TagResources, UntagResources, GetTagValues) -> Moto
    return await forward_to_moto(request, "resourcegroupstaggingapi", account_id=account_id)


async def _get_resources(request: Request, body: bytes, region: str, account_id: str) -> Response:
    params = json.loads(body) if body else {}
    tag_filters = params.get("TagFilters", [])
    resource_type_filters = [r.lower() for r in params.get("ResourceTypeFilters", [])]

    # Get Moto results first
    moto_resp = await forward_to_moto(request, "resourcegroupstaggingapi", account_id=account_id)
    moto_body = json.loads(moto_resp.body)
    results = moto_body.get("ResourceTagMappingList", [])

    # Augment with native SQS queues
    if (
        not resource_type_filters
        or "sqs" in resource_type_filters
        or "sqs:queue" in resource_type_filters
    ):
        results.extend(_get_native_sqs_resources(region, tag_filters))

    # Augment with native SNS topics
    if (
        not resource_type_filters
        or "sns" in resource_type_filters
        or "sns:topic" in resource_type_filters
    ):
        results.extend(_get_native_sns_resources(region, tag_filters))

    response = {
        "ResourceTagMappingList": results,
        "PaginationToken": moto_body.get("PaginationToken"),
    }
    return Response(
        content=json.dumps(response),
        status_code=200,
        media_type="application/x-amz-json-1.1",
    )


async def _get_tag_keys(request: Request, body: bytes, region: str, account_id: str) -> Response:
    # Get Moto results
    moto_resp = await forward_to_moto(request, "resourcegroupstaggingapi", account_id=account_id)
    moto_body = json.loads(moto_resp.body)
    tag_keys = set(moto_body.get("TagKeys", []))

    # Add keys from native SQS
    try:
        from robotocore.services.sqs.provider import _get_store as get_sqs_store

        sqs_store = get_sqs_store(region)
        for queue in sqs_store.list_queues():
            tag_keys.update(queue.tags.keys())
    except Exception as exc:
        logger.debug("_get_tag_keys: get_sqs_store failed (non-fatal): %s", exc)

    # Add keys from native SNS
    try:
        from robotocore.services.sns.provider import _get_store as get_sns_store

        sns_store = get_sns_store(region)
        for topic in sns_store.topics.values():
            if hasattr(topic, "tags") and topic.tags:
                tag_keys.update(topic.tags.keys())
    except Exception as exc:
        logger.debug("_get_tag_keys: get_sns_store failed (non-fatal): %s", exc)

    response = {
        "TagKeys": sorted(tag_keys),
        "PaginationToken": moto_body.get("PaginationToken"),
    }
    return Response(
        content=json.dumps(response),
        status_code=200,
        media_type="application/x-amz-json-1.1",
    )


def _get_native_sqs_resources(region: str, tag_filters: list) -> list:
    results = []
    try:
        from robotocore.services.sqs.provider import _get_store as get_sqs_store

        sqs_store = get_sqs_store(region)
        for queue in sqs_store.list_queues():
            if not queue.tags:
                continue
            tags = [{"Key": k, "Value": v} for k, v in queue.tags.items()]
            if not _matches_tag_filters(tags, tag_filters):
                continue
            results.append({"ResourceARN": queue.arn, "Tags": tags})
    except Exception:
        logger.debug("Failed to get native SQS resources for tagging", exc_info=True)
    return results


def _get_native_sns_resources(region: str, tag_filters: list) -> list:
    results = []
    try:
        from robotocore.services.sns.provider import _get_store as get_sns_store

        sns_store = get_sns_store(region)
        for topic in sns_store.topics.values():
            if not hasattr(topic, "tags") or not topic.tags:
                continue
            tags = [{"Key": k, "Value": v} for k, v in topic.tags.items()]
            if not _matches_tag_filters(tags, tag_filters):
                continue
            results.append({"ResourceARN": topic.arn, "Tags": tags})
    except Exception:
        logger.debug("Failed to get native SNS resources for tagging", exc_info=True)
    return results


def _matches_tag_filters(tags: list, tag_filters: list) -> bool:
    if not tag_filters:
        return True
    tag_dict = {t["Key"]: t["Value"] for t in tags}
    for f in tag_filters:
        key = f.get("Key", "")
        values = f.get("Values", [])
        if key not in tag_dict:
            return False
        if values and tag_dict[key] not in values:
            return False
    return True
