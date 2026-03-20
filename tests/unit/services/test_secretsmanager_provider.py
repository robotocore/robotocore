"""Error-path tests for SecretsManager native provider.

Phase 3A: Covers ResourceNotFoundException for RotateSecret and
ReplicateSecretToRegions operations.
"""

import json
from unittest.mock import AsyncMock, MagicMock

import pytest

from robotocore.services.secretsmanager.provider import handle_secretsmanager_request


def _create_test_secret(backend, name: str, secret_string: str = "test-value"):
    """Helper to create a secret in Moto backend with all required positional args."""
    backend.create_secret(
        name=name,
        secret_string=secret_string,
        secret_binary=None,
        description=None,
        tags=None,
        kms_key_id=None,
        client_request_token=None,
        replica_regions=[],
        force_overwrite=False,
    )


def _delete_test_secret(backend, name: str):
    """Helper to force-delete a test secret."""
    backend.delete_secret(
        secret_id=name,
        recovery_window_in_days=None,
        force_delete_without_recovery=True,
    )


def _get_backend():
    from moto.backends import get_backend  # noqa: I001

    return get_backend("secretsmanager")["123456789012"]["us-east-1"]


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
        req = _make_request(
            "RotateSecret",
            {
                "SecretId": "nonexistent-secret-xyz",
            },
        )
        resp = await handle_secretsmanager_request(req, "us-east-1", "123456789012")
        assert resp.status_code == 400
        body = json.loads(resp.body)
        assert body["__type"] == "ResourceNotFoundException"

    async def test_rotate_nonexistent_secret_by_arn(self):
        req = _make_request(
            "RotateSecret",
            {
                "SecretId": "arn:aws:secretsmanager:us-east-1:123456789012:secret:nonexistent",
            },
        )
        resp = await handle_secretsmanager_request(req, "us-east-1", "123456789012")
        assert resp.status_code == 400
        body = json.loads(resp.body)
        assert body["__type"] == "ResourceNotFoundException"


@pytest.mark.asyncio
class TestReplicateSecretErrors:
    async def test_replicate_nonexistent_secret(self):
        req = _make_request(
            "ReplicateSecretToRegions",
            {
                "SecretId": "nonexistent-secret-xyz",
                "AddReplicaRegions": [{"Region": "eu-west-1"}],
            },
        )
        resp = await handle_secretsmanager_request(req, "us-east-1", "123456789012")
        assert resp.status_code == 400
        body = json.loads(resp.body)
        assert body["__type"] == "ResourceNotFoundException"


@pytest.mark.asyncio
class TestRotateDeletedSecret:
    """Categorical bug: native providers must check is_deleted() before operating on secrets."""

    async def test_rotate_deleted_secret_returns_invalid_request(self):
        """RotateSecret on a soft-deleted secret must return InvalidRequestException."""
        backend = _get_backend()
        _create_test_secret(backend, "deleted-test-secret", "hunter2")
        backend.delete_secret(
            secret_id="deleted-test-secret",
            recovery_window_in_days=7,
            force_delete_without_recovery=False,
        )
        secret = backend.secrets.get("deleted-test-secret")
        assert secret is not None
        assert secret.is_deleted()

        req = _make_request("RotateSecret", {"SecretId": "deleted-test-secret"})
        resp = await handle_secretsmanager_request(req, "us-east-1", "123456789012")
        assert resp.status_code == 400
        body = json.loads(resp.body)
        assert body["__type"] == "InvalidRequestException"

        # Cleanup
        _delete_test_secret(backend, "deleted-test-secret")

    async def test_replicate_deleted_secret_returns_invalid_request(self):
        """ReplicateSecretToRegions on a soft-deleted secret must return InvalidRequestException."""
        backend = _get_backend()
        _create_test_secret(backend, "deleted-replicate-test", "hunter2")
        backend.delete_secret(
            secret_id="deleted-replicate-test",
            recovery_window_in_days=7,
            force_delete_without_recovery=False,
        )
        secret = backend.secrets.get("deleted-replicate-test")
        assert secret is not None
        assert secret.is_deleted()

        req = _make_request(
            "ReplicateSecretToRegions",
            {
                "SecretId": "deleted-replicate-test",
                "AddReplicaRegions": [{"Region": "eu-west-1"}],
            },
        )
        resp = await handle_secretsmanager_request(req, "us-east-1", "123456789012")
        assert resp.status_code == 400
        body = json.loads(resp.body)
        assert body["__type"] == "InvalidRequestException"

        # Cleanup
        _delete_test_secret(backend, "deleted-replicate-test")


@pytest.mark.asyncio
class TestRotateSecretHappyPath:
    """Categorical: verify intercepted operations return correct response structure."""

    async def test_rotate_existing_secret_returns_arn_name_version(self):
        backend = _get_backend()
        _create_test_secret(backend, "rotate-happy-test", "secret123")

        req = _make_request(
            "RotateSecret",
            {
                "SecretId": "rotate-happy-test",
                "RotationLambdaARN": "arn:aws:lambda:us-east-1:123456789012:function:myRotator",
                "RotationRules": {"AutomaticallyAfterDays": 30},
            },
        )
        resp = await handle_secretsmanager_request(req, "us-east-1", "123456789012")
        assert resp.status_code == 200
        body = json.loads(resp.body)
        assert "ARN" in body
        assert body["Name"] == "rotate-happy-test"
        assert "VersionId" in body

        # Cleanup
        _delete_test_secret(backend, "rotate-happy-test")

    async def test_rotate_secret_client_request_token_too_short(self):
        """Categorical: validate ClientRequestToken length (32-64 chars)."""
        backend = _get_backend()
        _create_test_secret(backend, "rotate-token-test", "secret123")

        req = _make_request(
            "RotateSecret",
            {
                "SecretId": "rotate-token-test",
                "ClientRequestToken": "short",  # < 32 chars
            },
        )
        resp = await handle_secretsmanager_request(req, "us-east-1", "123456789012")
        assert resp.status_code == 400
        body = json.loads(resp.body)
        assert body["__type"] == "InvalidParameterException"

        # Cleanup
        _delete_test_secret(backend, "rotate-token-test")


@pytest.mark.asyncio
class TestSecretLookupByArn:
    """Categorical: native providers that look up secrets by ARN should use
    Moto's SecretsStore.get() which already handles ARN resolution, not manual loops."""

    async def test_rotate_by_full_arn(self):
        backend = _get_backend()
        _create_test_secret(backend, "arn-lookup-test", "secret123")
        secret = backend.secrets.get("arn-lookup-test")
        arn = secret.arn

        req = _make_request("RotateSecret", {"SecretId": arn})
        resp = await handle_secretsmanager_request(req, "us-east-1", "123456789012")
        assert resp.status_code == 200
        body = json.loads(resp.body)
        assert body["Name"] == "arn-lookup-test"

        # Cleanup
        _delete_test_secret(backend, "arn-lookup-test")

    async def test_replicate_by_full_arn(self):
        backend = _get_backend()
        _create_test_secret(backend, "arn-replicate-test", "secret123")
        secret = backend.secrets.get("arn-replicate-test")
        arn = secret.arn

        req = _make_request(
            "ReplicateSecretToRegions",
            {"SecretId": arn, "AddReplicaRegions": [{"Region": "eu-west-1"}]},
        )
        resp = await handle_secretsmanager_request(req, "us-east-1", "123456789012")
        assert resp.status_code == 200
        body = json.loads(resp.body)
        assert body["ARN"] == arn

        # Cleanup
        _delete_test_secret(backend, "arn-replicate-test")


@pytest.mark.asyncio
class TestValidateResourcePolicy:
    async def test_validate_policy_always_succeeds(self):
        req = _make_request(
            "ValidateResourcePolicy",
            {
                "ResourcePolicy": json.dumps(
                    {
                        "Version": "2012-10-17",
                        "Statement": [
                            {
                                "Effect": "Allow",
                                "Principal": "*",
                                "Action": "secretsmanager:GetSecretValue",
                                "Resource": "*",
                            }
                        ],
                    }
                ),
            },
        )
        resp = await handle_secretsmanager_request(req, "us-east-1", "123456789012")
        assert resp.status_code == 200
        body = json.loads(resp.body)
        assert "ValidationErrors" in body
