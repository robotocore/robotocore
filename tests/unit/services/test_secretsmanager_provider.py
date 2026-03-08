"""Error-path tests for SecretsManager native provider.

Phase 3A: Covers ResourceNotFoundException for RotateSecret and
ReplicateSecretToRegions operations.
"""

import json
from unittest.mock import AsyncMock, MagicMock

import pytest

from robotocore.services.secretsmanager.provider import handle_secretsmanager_request


def _make_request(action: str, body: dict | None = None) -> MagicMock:
    req = MagicMock()
    req.headers = {"x-amz-target": f"secretsmanager.{action}"}
    req.method = "POST"
    req.url = MagicMock()
    req.url.path = "/"
    req.query_params = {}
    payload = json.dumps(body or {}).encode()
    req.body = AsyncMock(return_value=payload)
    return req


@pytest.mark.asyncio
class TestRotateSecretErrors:
    async def test_rotate_nonexistent_secret_by_name(self):
        req = _make_request("RotateSecret", {
            "SecretId": "nonexistent-secret-xyz",
        })
        resp = await handle_secretsmanager_request(req, "us-east-1", "123456789012")
        assert resp.status_code == 400
        body = json.loads(resp.body)
        assert body["__type"] == "ResourceNotFoundException"

    async def test_rotate_nonexistent_secret_by_arn(self):
        req = _make_request("RotateSecret", {
            "SecretId": "arn:aws:secretsmanager:us-east-1:123456789012:secret:nonexistent",
        })
        resp = await handle_secretsmanager_request(req, "us-east-1", "123456789012")
        assert resp.status_code == 400
        body = json.loads(resp.body)
        assert body["__type"] == "ResourceNotFoundException"


@pytest.mark.asyncio
class TestReplicateSecretErrors:
    async def test_replicate_nonexistent_secret(self):
        req = _make_request("ReplicateSecretToRegions", {
            "SecretId": "nonexistent-secret-xyz",
            "AddReplicaRegions": [{"Region": "eu-west-1"}],
        })
        resp = await handle_secretsmanager_request(req, "us-east-1", "123456789012")
        assert resp.status_code == 400
        body = json.loads(resp.body)
        assert body["__type"] == "ResourceNotFoundException"


@pytest.mark.asyncio
class TestValidateResourcePolicy:
    async def test_validate_policy_always_succeeds(self):
        req = _make_request("ValidateResourcePolicy", {
            "ResourcePolicy": json.dumps({
                "Version": "2012-10-17",
                "Statement": [{
                    "Effect": "Allow", "Principal": "*",
                    "Action": "secretsmanager:GetSecretValue", "Resource": "*",
                }],
            }),
        })
        resp = await handle_secretsmanager_request(req, "us-east-1", "123456789012")
        assert resp.status_code == 200
        body = json.loads(resp.body)
        assert "ValidationErrors" in body
