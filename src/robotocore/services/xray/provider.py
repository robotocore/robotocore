"""Native X-Ray provider.

Implements sampling rules, groups, encryption config, tagging,
service graph construction, trace summaries, and anomaly-based insights.
Falls back to Moto for trace/telemetry storage operations.
"""

import datetime
import json
import logging
import uuid
from typing import Any

from starlette.requests import Request
from starlette.responses import Response

from robotocore.providers.moto_bridge import forward_to_moto
from robotocore.services.xray.trace_correlation import get_engine

logger = logging.getLogger(__name__)

# In-memory stores (per-account not needed for local dev)
_sampling_rules: dict[str, dict[str, Any]] = {}
_groups: dict[str, dict[str, Any]] = {}
_encryption_config: dict[str, dict[str, Any]] = {}  # region -> config
_tags: dict[str, list[dict[str, str]]] = {}  # ARN -> tags


def _default_encryption_config() -> dict[str, Any]:
    return {"Type": "NONE", "Status": "ACTIVE"}


def _json_response(data: dict, status_code: int = 200) -> Response:
    return Response(
        content=json.dumps(data),
        status_code=status_code,
        media_type="application/x-amz-json-1.1",
    )


async def handle_xray_request(request: Request, region: str, account_id: str) -> Response:
    """Handle X-Ray requests, intercepting operations Moto doesn't implement."""
    path = request.url.path.rstrip("/")

    handler = _PATH_MAP.get(path)
    if handler:
        body = await request.body()
        params = json.loads(body) if body else {}
        result = handler(params, region, account_id)
        if isinstance(result, dict) and result.get("__error__"):
            error_data = {
                "__type": result["code"],
                "Message": result["message"],
            }
            return _json_response(error_data, status_code=result.get("status_code", 400))
        return _json_response(result)

    return await forward_to_moto(request, "xray", account_id=account_id)


def _create_sampling_rule(params: dict, region: str, account_id: str) -> dict:
    rule = params.get("SamplingRule", {})
    rule_name = rule.get("RuleName", f"rule-{uuid.uuid4().hex[:8]}")
    rule_arn = f"arn:aws:xray:{region}:{account_id}:sampling-rule/{rule_name}"

    tags = params.get("Tags", [])

    record = {
        "SamplingRule": {
            "RuleName": rule_name,
            "RuleARN": rule_arn,
            "ResourceARN": rule.get("ResourceARN", "*"),
            "Priority": rule.get("Priority", 1000),
            "FixedRate": rule.get("FixedRate", 0.05),
            "ReservoirSize": rule.get("ReservoirSize", 1),
            "ServiceName": rule.get("ServiceName", "*"),
            "ServiceType": rule.get("ServiceType", "*"),
            "Host": rule.get("Host", "*"),
            "HTTPMethod": rule.get("HTTPMethod", "*"),
            "URLPath": rule.get("URLPath", "*"),
            "Version": rule.get("Version", 1),
            "Attributes": rule.get("Attributes", {}),
        },
        "CreatedAt": 0.0,
        "ModifiedAt": 0.0,
    }
    _sampling_rules[rule_name] = record

    if tags:
        _tags[rule_arn] = list(tags)

    return {"SamplingRuleRecord": record}


def _get_sampling_rules(params: dict, region: str, account_id: str) -> dict:
    # Always include the default rule
    records = list(_sampling_rules.values())
    return {"SamplingRuleRecords": records, "NextToken": None}


def _delete_sampling_rule(params: dict, region: str, account_id: str) -> dict:
    rule_name = params.get("RuleName", "")
    rule_arn = params.get("RuleARN", "")

    record = None
    if rule_name and rule_name in _sampling_rules:
        record = _sampling_rules.pop(rule_name)
        # Clean up tags for the deleted rule
        deleted_arn = record["SamplingRule"].get("RuleARN", "")
        _tags.pop(deleted_arn, None)
    elif rule_arn:
        for name, rec in list(_sampling_rules.items()):
            if rec["SamplingRule"].get("RuleARN") == rule_arn:
                record = _sampling_rules.pop(name)
                _tags.pop(rule_arn, None)
                break

    if record is None:
        return {"SamplingRuleRecord": {}}

    return {"SamplingRuleRecord": record}


def _get_sampling_statistic_summaries(params: dict, region: str, account_id: str) -> dict:
    return {"SamplingStatisticSummaries": [], "NextToken": None}


def _create_group(params: dict, region: str, account_id: str) -> dict:
    group_name = params.get("GroupName", f"group-{uuid.uuid4().hex[:8]}")
    group_arn = f"arn:aws:xray:{region}:{account_id}:group/{group_name}"
    filter_expr = params.get("FilterExpression", "")
    tags = params.get("Tags", [])

    group = {
        "GroupName": group_name,
        "GroupARN": group_arn,
        "FilterExpression": filter_expr,
        "InsightsConfiguration": params.get(
            "InsightsConfiguration",
            {
                "InsightsEnabled": False,
                "NotificationsEnabled": False,
            },
        ),
    }
    _groups[group_name] = group

    if tags:
        _tags[group_arn] = list(tags)

    return {"Group": group}


def _get_group(params: dict, region: str, account_id: str) -> dict:
    group_name = params.get("GroupName", "")
    group_arn = params.get("GroupARN", "")

    if group_name and group_name in _groups:
        return {"Group": _groups[group_name]}
    if group_arn:
        for g in _groups.values():
            if g["GroupARN"] == group_arn:
                return {"Group": g}

    return {"Group": {}}


def _get_groups(params: dict, region: str, account_id: str) -> dict:
    groups = list(_groups.values())
    return {"Groups": groups, "NextToken": None}


def _delete_group(params: dict, region: str, account_id: str) -> dict:
    group_name = params.get("GroupName", "")
    group_arn = params.get("GroupARN", "")

    if group_name and group_name in _groups:
        deleted = _groups.pop(group_name)
        _tags.pop(deleted["GroupARN"], None)
    elif group_arn:
        for name, g in list(_groups.items()):
            if g["GroupARN"] == group_arn:
                _groups.pop(name)
                _tags.pop(group_arn, None)
                break

    return {}


def _get_encryption_config(params: dict, region: str, account_id: str) -> dict:
    config = _encryption_config.get(region, _default_encryption_config())
    return {"EncryptionConfig": dict(config)}


def _put_encryption_config(params: dict, region: str, account_id: str) -> dict:
    config = _encryption_config.setdefault(region, _default_encryption_config())
    enc_type = params.get("Type", "NONE")
    key_id = params.get("KeyId", "")
    config["Type"] = enc_type
    config["Status"] = "ACTIVE"
    if key_id:
        config["KeyId"] = key_id
    elif "KeyId" in config and enc_type == "NONE":
        config.pop("KeyId", None)
    return {"EncryptionConfig": dict(config)}


def _tag_resource(params: dict, region: str, account_id: str) -> dict:
    arn = params.get("ResourceARN", "")
    new_tags = params.get("Tags", [])
    existing = _tags.get(arn, [])

    # Merge: update existing keys, add new ones
    existing_keys = {t["Key"]: i for i, t in enumerate(existing)}
    for tag in new_tags:
        if tag["Key"] in existing_keys:
            existing[existing_keys[tag["Key"]]] = tag
        else:
            existing.append(tag)

    _tags[arn] = existing
    return {}


def _untag_resource(params: dict, region: str, account_id: str) -> dict:
    arn = params.get("ResourceARN", "")
    keys_to_remove = set(params.get("TagKeys", []))
    existing = _tags.get(arn, [])
    _tags[arn] = [t for t in existing if t["Key"] not in keys_to_remove]
    return {}


def _list_tags_for_resource(params: dict, region: str, account_id: str) -> dict:
    arn = params.get("ResourceARN", "")
    tags = _tags.get(arn, [])
    return {"Tags": tags, "NextToken": None}


# Resource policies store: policy_name -> policy dict
_resource_policies: dict[str, dict[str, Any]] = {}


def _put_resource_policy(params: dict, region: str, account_id: str) -> dict:
    policy_name = params.get("PolicyName", "")
    policy_document = params.get("PolicyDocument", "")
    revision_id = str(uuid.uuid4())

    policy = {
        "PolicyName": policy_name,
        "PolicyDocument": policy_document,
        "PolicyRevisionId": revision_id,
        "LastUpdatedTime": 0.0,
    }
    _resource_policies[policy_name] = policy
    return {"ResourcePolicy": policy}


def _list_resource_policies(params: dict, region: str, account_id: str) -> dict:
    policies = list(_resource_policies.values())
    return {"ResourcePolicies": policies, "NextToken": None}


def _delete_resource_policy(params: dict, region: str, account_id: str) -> dict:
    policy_name = params.get("PolicyName", "")
    revision_id = params.get("PolicyRevisionId", "")

    if policy_name in _resource_policies:
        existing = _resource_policies[policy_name]
        if revision_id and existing["PolicyRevisionId"] != revision_id:
            return _error_response(
                "InvalidPolicyRevisionIdException",
                "The provided policy revision id does not match.",
                400,
            )
        _resource_policies.pop(policy_name)
    return {}


def _get_insight_summaries(params: dict, region: str, account_id: str) -> dict:
    start_time = params.get("StartTime", 0)
    end_time = params.get("EndTime", 0)
    engine = get_engine()
    insights = engine.detect_anomalies(start_time, end_time)
    group_arn = params.get("GroupARN")
    group_name = params.get("GroupName")
    # Attach group info if provided
    for insight in insights:
        if group_arn:
            insight["GroupARN"] = group_arn
        if group_name:
            insight["GroupName"] = group_name
    return {"InsightSummaries": insights}


def _get_sampling_targets(params: dict, region: str, account_id: str) -> dict:
    return {
        "SamplingTargetDocuments": [],
        "LastRuleModification": datetime.datetime.now(datetime.UTC).isoformat(),
        "UnprocessedStatistics": [],
    }


def _get_service_graph(params: dict, region: str, account_id: str) -> dict:
    start_time = params.get("StartTime", 0)
    end_time = params.get("EndTime", 0)
    engine = get_engine()
    services = engine.build_service_graph(start_time, end_time)
    return {
        "Services": services,
        "StartTime": start_time,
        "EndTime": end_time,
        "ContainsOldGroupVersions": False,
    }


def _put_trace_segments(params: dict, region: str, account_id: str) -> dict:
    """Intercept PutTraceSegments to feed segments into the correlation engine."""
    docs = params.get("TraceSegmentDocuments", [])
    engine = get_engine()
    unprocessed: list[dict[str, Any]] = []

    for doc in docs:
        try:
            segment = json.loads(doc) if isinstance(doc, str) else doc
            engine.add_segment(segment)
        except (json.JSONDecodeError, TypeError) as exc:
            logger.debug("Failed to parse trace segment: %s", exc)
            unprocessed.append({"Id": None, "ErrorCode": "INVALID_DOCUMENT", "Message": str(exc)})

    return {"UnprocessedTraceSegments": unprocessed}


def _get_time_series_service_statistics(params: dict, region: str, account_id: str) -> dict:
    return {"TimeSeriesServiceStatistics": [], "ContainsOldGroupVersions": False}


def _get_trace_summaries(params: dict, region: str, account_id: str) -> dict:
    start_time = params.get("StartTime", 0)
    end_time = params.get("EndTime", 0)
    filter_expression = params.get("FilterExpression", "")
    engine = get_engine()
    summaries = engine.get_trace_summaries(start_time, end_time, filter_expression)
    return {
        "TraceSummaries": summaries,
        "ApproximateTime": datetime.datetime.now(datetime.UTC).timestamp(),
    }


def _error_response(code: str, message: str, status_code: int = 400) -> dict:
    """Return a dict that handle_xray_request wraps into an error response."""
    return {"__error__": True, "code": code, "message": message, "status_code": status_code}


_PATH_MAP = {
    "/CreateSamplingRule": _create_sampling_rule,
    "/GetSamplingRules": _get_sampling_rules,
    "/DeleteSamplingRule": _delete_sampling_rule,
    "/SamplingStatisticSummaries": _get_sampling_statistic_summaries,
    "/CreateGroup": _create_group,
    "/GetGroup": _get_group,
    "/Groups": _get_groups,
    "/DeleteGroup": _delete_group,
    "/EncryptionConfig": _get_encryption_config,
    "/PutEncryptionConfig": _put_encryption_config,
    "/TagResource": _tag_resource,
    "/UntagResource": _untag_resource,
    "/ListTagsForResource": _list_tags_for_resource,
    "/PutResourcePolicy": _put_resource_policy,
    "/ListResourcePolicies": _list_resource_policies,
    "/DeleteResourcePolicy": _delete_resource_policy,
    "/InsightSummaries": _get_insight_summaries,
    "/SamplingTargets": _get_sampling_targets,
    "/TimeSeriesServiceStatistics": _get_time_series_service_statistics,
    "/TraceSummaries": _get_trace_summaries,
    "/ServiceGraph": _get_service_graph,
    "/TraceSegments": _put_trace_segments,
}
