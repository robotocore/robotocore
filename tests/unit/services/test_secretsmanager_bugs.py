"""Failing tests for bugs in the SecretsManager native provider.

Each test documents a specific correctness bug. All tests should FAIL against
the current implementation, proving the bug exists.
"""

import json
import time
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


def _create_secret_in_moto(name: str, region: str = "us-east-1", account_id: str = "123456789012"):
    """Create a secret directly in Moto backend and return it."""
    from moto.backends import get_backend

    backend = get_backend("secretsmanager")[account_id][region]
    backend.create_secret(
        name=name,
        secret_string="my-secret-value",
        secret_binary=None,
        description="test secret",
        tags=[],
        kms_key_id=None,
        client_request_token=None,
        replica_regions=[],
        force_overwrite=False,
    )
    return backend.secrets[name]


def _delete_secret_in_moto(name: str, region: str = "us-east-1", account_id: str = "123456789012"):
    """Mark a secret as deleted in the Moto backend."""
    from moto.backends import get_backend

    backend = get_backend("secretsmanager")[account_id][region]
    secret = backend.secrets[name]
    secret.delete(time.time())
    return secret


def _cleanup_secret(name: str, region: str = "us-east-1", account_id: str = "123456789012"):
    """Remove a secret from the Moto backend."""
    from moto.backends import get_backend

    backend = get_backend("secretsmanager")[account_id][region]
    try:
        backend.secrets.pop(name)
    except KeyError:
        pass


@pytest.mark.asyncio
class TestRotateSecretDeletedSecretBug:
    """Bug: _rotate_secret does not check if the secret is marked as deleted.

    AWS and Moto both raise InvalidRequestException when you try to rotate
    a deleted secret. The native provider skips this check and happily
    creates a new AWSPENDING version on a deleted secret.
    """

    async def test_rotate_deleted_secret_should_fail(self):
        name = "test-rotate-deleted-bug"
        try:
            _create_secret_in_moto(name)
            _delete_secret_in_moto(name)

            req = _make_request("RotateSecret", {"SecretId": name})
            resp = await handle_secretsmanager_request(req, "us-east-1", "123456789012")

            # Should return an error for a deleted secret
            assert resp.status_code == 400, (
                f"Expected 400 for deleted secret, got {resp.status_code}"
            )
            body = json.loads(resp.body)
            assert body["__type"] == "InvalidRequestException"
        finally:
            _cleanup_secret(name)


@pytest.mark.asyncio
class TestRotateSecretInProgressBug:
    """Bug: _rotate_secret does not detect an in-progress rotation.

    If AWSPENDING exists on a version that doesn't also have AWSCURRENT,
    it means a previous rotation is still in progress. AWS and Moto raise
    InvalidRequestException. The native provider ignores this.
    """

    async def test_rotate_with_pending_rotation_should_fail(self):
        name = "test-rotate-in-progress-bug"
        try:
            secret = _create_secret_in_moto(name)

            # Simulate an in-progress rotation: add a version with only AWSPENDING
            secret.versions["pending-version-id"] = {
                "createdate": int(time.time()),
                "version_id": "pending-version-id",
                "version_stages": ["AWSPENDING"],
            }

            req = _make_request("RotateSecret", {"SecretId": name})
            resp = await handle_secretsmanager_request(req, "us-east-1", "123456789012")

            # Should detect the in-progress rotation and return error
            assert resp.status_code == 400, (
                f"Expected 400 for in-progress rotation, got {resp.status_code}"
            )
            body = json.loads(resp.body)
            assert body["__type"] == "InvalidRequestException"
        finally:
            _cleanup_secret(name)


@pytest.mark.asyncio
class TestRotateSecretDaysValidationBug:
    """Bug: _rotate_secret does not validate AutomaticallyAfterDays range.

    AWS and Moto validate the range is 1-1000. The native provider accepts
    any value including 0, negative numbers, and values > 1000.
    """

    async def test_rotate_with_zero_days_should_fail(self):
        name = "test-rotate-zero-days-bug"
        try:
            _create_secret_in_moto(name)

            req = _make_request(
                "RotateSecret",
                {
                    "SecretId": name,
                    "RotationRules": {"AutomaticallyAfterDays": 0},
                },
            )
            resp = await handle_secretsmanager_request(req, "us-east-1", "123456789012")

            # AWS/Moto reject 0 days. The native provider should too.
            assert resp.status_code == 400, (
                f"Expected 400 for AutomaticallyAfterDays=0, got {resp.status_code}"
            )
            body = json.loads(resp.body)
            assert body["__type"] == "InvalidParameterException"
        finally:
            _cleanup_secret(name)

    async def test_rotate_with_negative_days_should_fail(self):
        name = "test-rotate-negative-days-bug"
        try:
            _create_secret_in_moto(name)

            req = _make_request(
                "RotateSecret",
                {
                    "SecretId": name,
                    "RotationRules": {"AutomaticallyAfterDays": -5},
                },
            )
            resp = await handle_secretsmanager_request(req, "us-east-1", "123456789012")

            assert resp.status_code == 400, (
                f"Expected 400 for negative days, got {resp.status_code}"
            )
            body = json.loads(resp.body)
            assert body["__type"] == "InvalidParameterException"
        finally:
            _cleanup_secret(name)

    async def test_rotate_with_days_over_1000_should_fail(self):
        name = "test-rotate-over-1000-days-bug"
        try:
            _create_secret_in_moto(name)

            req = _make_request(
                "RotateSecret",
                {
                    "SecretId": name,
                    "RotationRules": {"AutomaticallyAfterDays": 1001},
                },
            )
            resp = await handle_secretsmanager_request(req, "us-east-1", "123456789012")

            assert resp.status_code == 400, f"Expected 400 for days > 1000, got {resp.status_code}"
            body = json.loads(resp.body)
            assert body["__type"] == "InvalidParameterException"
        finally:
            _cleanup_secret(name)


@pytest.mark.asyncio
class TestRotateSecretLambdaArnLengthBug:
    """Bug: _rotate_secret does not validate RotationLambdaARN length.

    AWS and Moto validate the ARN must be <= 2048 characters. The native
    provider accepts any length.
    """

    async def test_rotate_with_oversized_lambda_arn_should_fail(self):
        name = "test-rotate-long-arn-bug"
        try:
            _create_secret_in_moto(name)

            long_arn = "arn:aws:lambda:us-east-1:123456789012:function:" + ("x" * 2048)
            req = _make_request(
                "RotateSecret",
                {
                    "SecretId": name,
                    "RotationLambdaARN": long_arn,
                },
            )
            resp = await handle_secretsmanager_request(req, "us-east-1", "123456789012")

            assert resp.status_code == 400, (
                f"Expected 400 for oversized ARN, got {resp.status_code}"
            )
            body = json.loads(resp.body)
            assert body["__type"] == "InvalidParameterException"
        finally:
            _cleanup_secret(name)


@pytest.mark.asyncio
class TestRotateSecretLastRotationDateBug:
    """Bug: _rotate_secret does not set last_rotation_date.

    When a secret is rotated, AWS sets LastRotatedDate to the current time.
    The native provider sets next_rotation_date but forgets last_rotation_date.
    """

    async def test_rotate_should_set_last_rotation_date(self):
        name = "test-rotate-last-date-bug"
        try:
            secret = _create_secret_in_moto(name)

            assert secret.last_rotation_date is None

            req = _make_request(
                "RotateSecret",
                {
                    "SecretId": name,
                    "RotationRules": {"AutomaticallyAfterDays": 30},
                },
            )
            resp = await handle_secretsmanager_request(req, "us-east-1", "123456789012")
            assert resp.status_code == 200

            # After rotation, last_rotation_date should be set
            assert secret.last_rotation_date is not None, (
                "last_rotation_date should be set after rotation"
            )
        finally:
            _cleanup_secret(name)


@pytest.mark.asyncio
class TestRotateSecretPendingValueBug:
    """Bug: _rotate_secret copies the current secret value to the AWSPENDING version.

    This is incorrect. The point of rotation is to create a NEW secret value.
    The AWSPENDING version should NOT contain the same value as the AWSCURRENT
    version. In AWS, the rotation Lambda is responsible for creating the new
    value via PutSecretValue. The native provider pre-populates it with the
    old value, which defeats the purpose of rotation.
    """

    async def test_pending_version_should_not_have_current_value(self):
        name = "test-rotate-pending-value-bug"
        try:
            secret = _create_secret_in_moto(name)

            req = _make_request(
                "RotateSecret",
                {"SecretId": name},
            )
            resp = await handle_secretsmanager_request(req, "us-east-1", "123456789012")
            assert resp.status_code == 200

            resp_body = json.loads(resp.body)
            new_version_id = resp_body["VersionId"]

            # The AWSPENDING version should NOT contain the original secret value
            pending_version = secret.versions[new_version_id]
            assert "secret_string" not in pending_version, (
                "AWSPENDING version should not contain the current secret's value. "
                "The rotation Lambda is responsible for creating the new value."
            )
        finally:
            _cleanup_secret(name)
