"""Native Support provider.

Intercepts operations that Moto doesn't implement:
- DescribeServices
- DescribeSeverityLevels
- DescribeTrustedAdvisorCheckResult
- DescribeTrustedAdvisorCheckSummaries
- AddCommunicationToCase
- DescribeCommunications
"""

import datetime
import json
import logging

from starlette.requests import Request
from starlette.responses import Response

from robotocore.providers.moto_bridge import forward_to_moto

# AWS Support service codes (subset of real ones)
_SERVICES = [
    {
        "code": "amazon-dynamodb",
        "name": "Amazon DynamoDB",
        "categories": [
            {"code": "apis", "name": "APIs"},
            {"code": "other", "name": "Other"},
        ],
    },
    {
        "code": "amazon-s3",
        "name": "Amazon Simple Storage Service",
        "categories": [
            {"code": "general-guidance", "name": "General Guidance"},
            {"code": "other", "name": "Other"},
        ],
    },
    {
        "code": "amazon-ec2",
        "name": "Amazon Elastic Compute Cloud",
        "categories": [
            {"code": "instance-issue", "name": "Instance Issue"},
            {"code": "other", "name": "Other"},
        ],
    },
    {
        "code": "amazon-rds",
        "name": "Amazon Relational Database Service",
        "categories": [
            {"code": "other", "name": "Other"},
        ],
    },
    {
        "code": "amazon-sqs",
        "name": "Amazon Simple Queue Service",
        "categories": [
            {"code": "other", "name": "Other"},
        ],
    },
    {
        "code": "general-info",
        "name": "General Info and Getting Started",
        "categories": [
            {"code": "other", "name": "Other"},
        ],
    },
]

_SEVERITY_LEVELS = [
    {"code": "low", "name": "Low"},
    {"code": "normal", "name": "Normal"},
    {"code": "high", "name": "High"},
    {"code": "urgent", "name": "Urgent"},
    {"code": "critical", "name": "Critical"},
]

# In-memory communication store: case_id -> list of communications
_communications: dict[str, list[dict]] = {}


logger = logging.getLogger(__name__)


async def handle_support_request(request: Request, region: str, account_id: str) -> Response:
    """Handle Support requests, intercepting unimplemented operations."""
    target = request.headers.get("x-amz-target", "")
    action = target.split(".")[-1] if "." in target else ""

    handler = _ACTION_MAP.get(action)
    if handler:
        body = await request.body()
        params = json.loads(body) if body else {}
        result = handler(params, region, account_id)
        return Response(
            content=json.dumps(result),
            status_code=200,
            media_type="application/x-amz-json-1.1",
        )

    return await forward_to_moto(request, "support", account_id=account_id)


def _describe_services(params: dict, region: str, account_id: str) -> dict:
    return {"services": _SERVICES}


def _describe_severity_levels(params: dict, region: str, account_id: str) -> dict:
    return {"severityLevels": _SEVERITY_LEVELS}


def _describe_trusted_advisor_check_result(params: dict, region: str, account_id: str) -> dict:
    check_id = params.get("checkId", "")
    return {
        "result": {
            "checkId": check_id,
            "timestamp": datetime.datetime.now(datetime.UTC).strftime("%Y-%m-%dT%H:%M:%SZ"),
            "status": "ok",
            "resourcesSummary": {
                "resourcesProcessed": 0,
                "resourcesFlagged": 0,
                "resourcesIgnored": 0,
                "resourcesSuppressed": 0,
            },
            "categorySpecificSummary": {},
            "flaggedResources": [],
        }
    }


def _describe_trusted_advisor_check_summaries(params: dict, region: str, account_id: str) -> dict:
    check_ids = params.get("checkIds", [])
    summaries = []
    for cid in check_ids:
        summaries.append(
            {
                "checkId": cid,
                "timestamp": datetime.datetime.now(datetime.UTC).strftime("%Y-%m-%dT%H:%M:%SZ"),
                "status": "ok",
                "hasFlaggedResources": False,
                "resourcesSummary": {
                    "resourcesProcessed": 0,
                    "resourcesFlagged": 0,
                    "resourcesIgnored": 0,
                    "resourcesSuppressed": 0,
                },
                "categorySpecificSummary": {},
            }
        )
    return {"summaries": summaries}


def _add_communication_to_case(params: dict, region: str, account_id: str) -> dict:
    case_id = params.get("caseId", "")
    comm_body = params.get("communicationBody", "")

    if case_id not in _communications:
        _communications[case_id] = []
    _communications[case_id].append(
        {
            "caseId": case_id,
            "body": comm_body,
            "submittedBy": "user@example.com",
            "timeCreated": datetime.datetime.now(datetime.UTC).strftime("%Y-%m-%dT%H:%M:%SZ"),
            "attachmentSet": [],
        }
    )
    return {"result": True}


def _describe_communications(params: dict, region: str, account_id: str) -> dict:
    case_id = params.get("caseId", "")
    comms = _communications.get(case_id, [])

    # Also pull initial communication from Moto's case if available
    try:
        from moto.backends import get_backend  # noqa: I001

        backend = get_backend("support")[account_id]["us-east-1"]
        if case_id in backend.cases:
            case = backend.cases[case_id]
            initial_comm = {
                "caseId": case_id,
                "body": case.communication_body,
                "submittedBy": case.submitted_by,
                "timeCreated": case.time_created,
                "attachmentSet": [],
            }
            comms = [initial_comm] + comms
    except Exception as exc:  # noqa: BLE001
        logger.debug("_describe_communications: get_backend failed (non-fatal): %s", exc)

    return {"communications": comms, "nextToken": None}


_ACTION_MAP = {
    "DescribeServices": _describe_services,
    "DescribeSeverityLevels": _describe_severity_levels,
    "DescribeTrustedAdvisorCheckResult": _describe_trusted_advisor_check_result,
    "DescribeTrustedAdvisorCheckSummaries": _describe_trusted_advisor_check_summaries,
    "AddCommunicationToCase": _add_communication_to_case,
    "DescribeCommunications": _describe_communications,
}
