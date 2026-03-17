"""IAM enforcement middleware for the gateway.

Opt-in via ENFORCE_IAM=1 environment variable (off by default).
Extracts credentials from SigV4 headers or presigned URL query params,
gathers the caller's policies from the Moto IAM backend, and evaluates
whether the request is allowed.
"""

from __future__ import annotations

import json
import logging
import re
from typing import Any
from urllib.parse import unquote

from starlette.responses import Response

from robotocore.gateway.handler_chain import RequestContext
from robotocore.services.iam.policy_engine import DENY, IMPLICIT_DENY, evaluate_policy

log = logging.getLogger(__name__)

# Pre-compiled patterns
_CREDENTIAL_RE = re.compile(r"Credential=([A-Za-z0-9]+)/(\d{8})/([^/]+)/([^/]+)/aws4_request")

# Map of service signing names to canonical service prefixes for IAM actions
_SERVICE_ACTION_PREFIX: dict[str, str] = {
    "dynamodb": "dynamodb",
    "dynamodbstreams": "dynamodb",
    "events": "events",
    "firehose": "firehose",
    "kinesis": "kinesis",
    "lambda": "lambda",
    "logs": "logs",
    "monitoring": "cloudwatch",
    "s3": "s3",
    "sns": "sns",
    "sqs": "sqs",
    "states": "states",
    "sts": "sts",
    "iam": "iam",
    "kms": "kms",
    "secretsmanager": "secretsmanager",
    "ssm": "ssm",
    "cloudformation": "cloudformation",
    "apigateway": "apigateway",
    "scheduler": "scheduler",
    "stepfunctions": "states",
    "route53": "route53",
    "ec2": "ec2",
    "ses": "ses",
    "config": "config",
    "cloudwatch": "cloudwatch",
    "redshift": "redshift",
    "acm": "acm",
    "es": "es",
    "opensearch": "es",
    "swf": "swf",
    "transcribe": "transcribe",
    "support": "support",
    "resource-groups": "resource-groups",
}

# In-memory STS session store: {access_key_id: {"role_arn": ..., "account_id": ...}}
_sts_sessions: dict[str, dict[str, str]] = {}


def register_sts_session(access_key_id: str, role_arn: str, account_id: str) -> None:
    """Register an assumed-role session for IAM enforcement."""
    _sts_sessions[access_key_id] = {"role_arn": role_arn, "account_id": account_id}


def clear_sts_sessions() -> None:
    """Clear all STS sessions (for testing)."""
    _sts_sessions.clear()


def extract_credentials(request: Any) -> dict[str, str] | None:
    """Extract AWS credentials from the request.

    Looks at the Authorization header (SigV4) and presigned URL query params.
    Returns dict with 'access_key_id', 'region', 'service' or None.
    """
    # Try Authorization header first
    auth = ""
    if hasattr(request, "headers"):
        auth = request.headers.get("authorization", "")

    match = _CREDENTIAL_RE.search(auth)
    if match:
        return {
            "access_key_id": match.group(1),
            "date": match.group(2),
            "region": match.group(3),
            "service": match.group(4),
        }

    # Try presigned URL query params
    query_params = getattr(request, "query_params", {})
    credential = query_params.get("X-Amz-Credential", "")
    if credential:
        credential = unquote(credential)
        parts = credential.split("/")
        if len(parts) >= 5:
            return {
                "access_key_id": parts[0],
                "date": parts[1],
                "region": parts[2],
                "service": parts[3],
            }

    return None


def build_iam_action(service_name: str, operation: str | None) -> str:
    """Build an IAM action string like 'sqs:SendMessage'.

    Uses the service name and operation from the request context.
    """
    prefix = _SERVICE_ACTION_PREFIX.get(service_name, service_name)
    if operation:
        return f"{prefix}:{operation}"
    return f"{prefix}:*"


def build_resource_arn(
    service_name: str,
    region: str,
    account_id: str,
    request: Any,
) -> str:
    """Build a resource ARN based on the service and request.

    Returns a best-effort ARN for the target resource.
    """
    path = getattr(request, "url", None)
    if path is not None:
        path = str(path.path) if hasattr(path, "path") else str(path)
    else:
        path = "/"

    query_params = getattr(request, "query_params", {})

    if service_name == "s3":
        # S3: arn:aws:s3:::bucket/key
        parts = path.strip("/").split("/", 1)
        bucket = parts[0] if parts[0] else "*"
        key = parts[1] if len(parts) > 1 else ""
        if key:
            return f"arn:aws:s3:::{bucket}/{key}"
        return f"arn:aws:s3:::{bucket}"

    if service_name == "sqs":
        # SQS: arn:aws:sqs:region:account:queue-name
        queue_url = query_params.get("QueueUrl", "")
        if queue_url:
            queue_name = queue_url.rstrip("/").split("/")[-1]
            return f"arn:aws:sqs:{region}:{account_id}:{queue_name}"
        parts = path.strip("/").split("/")
        if len(parts) >= 2:
            return f"arn:aws:sqs:{region}:{account_id}:{parts[-1]}"
        return f"arn:aws:sqs:{region}:{account_id}:*"

    if service_name == "sns":
        # SNS: arn:aws:sns:region:account:topic-name
        topic_arn = query_params.get("TopicArn", "")
        if topic_arn:
            return topic_arn
        return f"arn:aws:sns:{region}:{account_id}:*"

    if service_name in ("lambda", "lambda_"):
        # Lambda: arn:aws:lambda:region:account:function:name
        fn_match = re.search(r"/functions/([^/]+)", path)
        if fn_match:
            fn_name = fn_match.group(1)
            return f"arn:aws:lambda:{region}:{account_id}:function:{fn_name}"
        return f"arn:aws:lambda:{region}:{account_id}:function:*"

    if service_name == "dynamodb":
        # DynamoDB: arn:aws:dynamodb:region:account:table/name
        return f"arn:aws:dynamodb:{region}:{account_id}:table/*"

    # Generic fallback
    return f"arn:aws:{service_name}:{region}:{account_id}:*"


def _gather_policies(access_key_id: str, account_id: str, region: str) -> list[dict]:
    """Gather all IAM policies for the given access key.

    Looks up the user/role in the Moto IAM backend and collects
    inline policies, attached managed policies, and group policies.
    """
    try:
        from moto.backends import get_backend

        iam_backend = get_backend("iam")[account_id]["global"]
    except Exception:  # noqa: BLE001
        return []

    policies: list[dict] = []

    # Check STS sessions for assumed roles
    session = _sts_sessions.get(access_key_id)
    if session:
        role_arn = session["role_arn"]
        for role in iam_backend.roles.values():
            if role.arn == role_arn:
                # Inline policies
                for policy_doc in role.policies.values():
                    if isinstance(policy_doc, str):
                        policies.append(json.loads(policy_doc))
                    else:
                        policies.append(policy_doc)
                # Attached managed policies
                for managed in role.managed_policies.values():
                    doc = managed.document
                    if isinstance(doc, str):
                        policies.append(json.loads(doc))
                    else:
                        policies.append(doc)
                break
        return policies

    # Look up user by access key
    for user in iam_backend.users.values():
        for key in user.access_keys:
            if key.access_key_id == access_key_id:
                # Inline policies
                for policy_doc in user.policies.values():
                    if isinstance(policy_doc, str):
                        policies.append(json.loads(policy_doc))
                    else:
                        policies.append(policy_doc)
                # Attached managed policies
                for managed in user.managed_policies.values():
                    doc = managed.document
                    if isinstance(doc, str):
                        policies.append(json.loads(doc))
                    else:
                        policies.append(doc)
                # Group policies
                for group in iam_backend.groups.values():
                    if user.name in [u.name for u in group.users]:
                        for policy_doc in group.policies.values():
                            if isinstance(policy_doc, str):
                                policies.append(json.loads(policy_doc))
                            else:
                                policies.append(policy_doc)
                        for managed in group.managed_policies.values():
                            doc = managed.document
                            if isinstance(doc, str):
                                policies.append(json.loads(doc))
                            else:
                                policies.append(doc)
                return policies

    return policies


def _build_access_denied_response(
    action: str,
    protocol: str | None,
) -> Response:
    """Build a 403 AccessDenied response in the appropriate format."""
    message = (
        f"User is not authorized to perform: {action} "
        f"with an explicit deny in an identity-based policy"
    )

    if protocol in ("json", "rest-json"):
        body = json.dumps(
            {
                "__type": "AccessDeniedException",
                "Message": message,
            }
        )
        return Response(
            content=body,
            status_code=403,
            media_type="application/x-amz-json-1.1",
        )
    else:
        body = (
            "<ErrorResponse>"
            "<Error>"
            "<Type>Sender</Type>"
            "<Code>AccessDenied</Code>"
            f"<Message>{message}</Message>"
            "</Error>"
            "</ErrorResponse>"
        )
        return Response(
            content=body,
            status_code=403,
            media_type="application/xml",
        )


def _record_to_stream(
    *,
    principal: str,
    action: str,
    resource: str,
    decision: str,
    matched_policies: list[str] | None = None,
    matched_statement: dict | None = None,
    request_id: str = "",
    evaluation_duration_ms: float = 0.0,
) -> None:
    """Record a policy evaluation to the stream if enabled."""
    from robotocore.services.iam.policy_stream import get_policy_stream, is_stream_enabled

    if not is_stream_enabled():
        return

    get_policy_stream().record(
        principal=principal,
        action=action,
        resource=resource,
        decision=decision,
        matched_policies=matched_policies,
        matched_statement=matched_statement,
        request_id=request_id,
        evaluation_duration_ms=evaluation_duration_ms,
    )


def iam_enforcement_handler(context: RequestContext) -> None:
    """Gateway handler that enforces IAM policies on requests.

    Opt-in via ENFORCE_IAM=1 environment variable.
    Skips IAM and STS service requests to avoid bootstrap deadlocks.
    """
    from robotocore.config.runtime import get_runtime_config

    enforce = get_runtime_config().get("ENFORCE_IAM", "0") == "1"

    # Skip IAM/STS to avoid deadlock during credential bootstrap
    if context.service_name in ("iam", "sts"):
        return

    creds = extract_credentials(context.request)
    if creds is None:
        # No credentials - when not enforcing, record as Allow if stream is on
        if not enforce:
            _record_to_stream(
                principal="anonymous",
                action=build_iam_action(context.service_name, context.operation),
                resource=build_resource_arn(
                    context.service_name, context.region, context.account_id, context.request
                ),
                decision="Allow",
                request_id=context.request.headers.get("x-amzn-requestid", ""),
            )
        return

    if not enforce:
        # Not enforcing IAM - record to stream if enabled and return
        _record_to_stream(
            principal=creds["access_key_id"],
            action=build_iam_action(context.service_name, context.operation),
            resource=build_resource_arn(
                context.service_name, context.region, context.account_id, context.request
            ),
            decision="Allow",
            request_id=context.request.headers.get("x-amzn-requestid", ""),
        )
        return

    action = build_iam_action(context.service_name, context.operation)
    resource = build_resource_arn(
        context.service_name, context.region, context.account_id, context.request
    )
    request_id = context.request.headers.get("x-amzn-requestid", "")

    import time as _time

    t0 = _time.monotonic()

    policies = _gather_policies(creds["access_key_id"], context.account_id, context.region)

    if not policies:
        # No policies found - implicit deny
        duration = (_time.monotonic() - t0) * 1000
        _record_to_stream(
            principal=creds["access_key_id"],
            action=action,
            resource=resource,
            decision="Deny",
            request_id=request_id,
            evaluation_duration_ms=duration,
        )
        context.response = _build_access_denied_response(action, context.protocol)
        return

    context_values = {
        "aws:SourceIp": getattr(context.request.client, "host", "127.0.0.1")
        if context.request.client
        else "127.0.0.1",
        "aws:CurrentTime": "2024-01-01T00:00:00Z",
        "aws:username": creds["access_key_id"],
    }

    result = evaluate_policy(policies, action, resource, context_values)
    duration = (_time.monotonic() - t0) * 1000

    decision = "Allow" if result not in (DENY, IMPLICIT_DENY) else "Deny"
    policy_arns = [f"inline-policy-{i}" for i in range(len(policies))]
    _record_to_stream(
        principal=creds["access_key_id"],
        action=action,
        resource=resource,
        decision=decision,
        matched_policies=policy_arns,
        request_id=request_id,
        evaluation_duration_ms=duration,
    )

    if result in (DENY, IMPLICIT_DENY):
        context.response = _build_access_denied_response(action, context.protocol)
