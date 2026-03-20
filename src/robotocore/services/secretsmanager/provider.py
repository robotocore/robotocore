"""Native Secrets Manager provider.

Intercepts operations that Moto doesn't implement or is too strict about:
- RotateSecret: Moto validates Lambda ARN exists — we skip that validation
- ValidateResourcePolicy: Not implemented in Moto
- ReplicateSecretToRegions: Not implemented in Moto

Uses JSON protocol via X-Amz-Target: secretsmanager.{Action}.
"""

import json
import time
import uuid

from starlette.requests import Request
from starlette.responses import Response

from robotocore.providers.moto_bridge import forward_to_moto


async def handle_secretsmanager_request(request: Request, region: str, account_id: str) -> Response:
    """Handle Secrets Manager requests."""
    target = request.headers.get("x-amz-target", "")
    action = target.split(".")[-1] if "." in target else ""

    handler = _ACTION_MAP.get(action)
    if handler:
        body = await request.body()
        params = json.loads(body) if body else {}
        try:
            result = handler(params, region, account_id)
            return Response(
                content=json.dumps(result),
                status_code=200,
                media_type="application/x-amz-json-1.1",
            )
        except _SMError as e:
            return Response(
                content=json.dumps({"__type": e.code, "Message": e.message}),
                status_code=e.status,
                media_type="application/x-amz-json-1.1",
            )

    return await forward_to_moto(request, "secretsmanager", account_id=account_id)


class _SMError(Exception):
    def __init__(self, code: str, message: str, status: int = 400):
        self.code = code
        self.message = message
        self.status = status


def _resolve_secret(backend, secret_id: str):
    """Look up a secret by name, ARN, or partial ARN. Returns None if not found.

    Uses Moto's SecretsStore which already handles name/ARN/partial-ARN resolution.
    """
    if not backend._is_valid_identifier(secret_id):
        return None
    return backend.secrets.get(secret_id)


def _rotate_secret(params: dict, region: str, account_id: str) -> dict:
    """RotateSecret — handle without Lambda validation."""
    from moto.backends import get_backend  # noqa: I001

    backend = get_backend("secretsmanager")[account_id][region]
    secret_id = params.get("SecretId", "")
    rotation_lambda_arn = params.get("RotationLambdaARN", "")
    rotation_rules = params.get("RotationRules", {})
    client_request_token = params.get("ClientRequestToken")

    secret = _resolve_secret(backend, secret_id)
    if secret is None:
        raise _SMError(
            "ResourceNotFoundException", "Secrets Manager can't find the specified secret."
        )

    # Soft-deleted secrets cannot be rotated
    if secret.is_deleted():
        raise _SMError(
            "InvalidRequestException",
            "You tried to perform the operation on a secret that's currently marked deleted.",
        )

    # Validate ClientRequestToken length (32-64 chars) per AWS API spec
    if client_request_token:
        token_len = len(client_request_token)
        if token_len < 32 or token_len > 64:
            raise _SMError(
                "InvalidParameterException",
                "ClientRequestToken must be 32-64 characters long.",
            )

    if rotation_lambda_arn:
        secret.rotation_lambda_arn = rotation_lambda_arn
    if rotation_rules:
        days = rotation_rules.get("AutomaticallyAfterDays", 0)
        if days:
            secret.auto_rotate_after_days = days
            secret.rotation_enabled = True
            secret.next_rotation_date = int(time.time()) + (int(days) * 86400)

    # Create a new version with AWSPENDING stage
    new_version_id = client_request_token or str(uuid.uuid4())
    secret_version = {
        "createdate": int(time.time()),
        "version_id": new_version_id,
        "version_stages": ["AWSPENDING"],
    }
    if hasattr(secret, "secret_string") and secret.secret_string is not None:
        secret_version["secret_string"] = secret.secret_string

    # Remove AWSPENDING from old versions
    if hasattr(secret, "remove_version_stages_from_old_versions"):
        secret.remove_version_stages_from_old_versions(["AWSPENDING"])
    secret.versions[new_version_id] = secret_version
    secret.rotation_requested = True

    return {
        "ARN": secret.arn,
        "Name": secret.name,
        "VersionId": new_version_id,
    }


def _validate_resource_policy(params: dict, region: str, account_id: str) -> dict:
    """ValidateResourcePolicy — always validates successfully."""
    return {
        "PolicyValidationPassed": True,
        "ValidationErrors": [],
    }


def _replicate_secret_to_regions(params: dict, region: str, account_id: str) -> dict:
    """ReplicateSecretToRegions — return simulated replication status."""
    from moto.backends import get_backend  # noqa: I001

    backend = get_backend("secretsmanager")[account_id][region]
    secret_id = params.get("SecretId", "")
    add_regions = params.get("AddReplicaRegions", [])

    secret = _resolve_secret(backend, secret_id)
    if secret is None:
        raise _SMError(
            "ResourceNotFoundException", "Secrets Manager can't find the specified secret."
        )

    # Soft-deleted secrets cannot be replicated
    if secret.is_deleted():
        raise _SMError(
            "InvalidRequestException",
            "You tried to perform the operation on a secret that's currently marked deleted.",
        )

    replication_status = []
    for replica in add_regions:
        replication_status.append(
            {
                "Region": replica.get("Region", ""),
                "Status": "InSync",
                "StatusMessage": "Secret replication successful",
            }
        )

    return {
        "ARN": secret.arn,
        "ReplicationStatus": replication_status,
    }


_ACTION_MAP = {
    "RotateSecret": _rotate_secret,
    "ValidateResourcePolicy": _validate_resource_policy,
    "ReplicateSecretToRegions": _replicate_secret_to_regions,
}
