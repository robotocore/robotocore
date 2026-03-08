"""Tests for correctness bugs in the Cognito Identity Provider."""

import base64
import hashlib
import hmac

from robotocore.services.cognito.provider import _secret_hash


class TestSecretHashBug:
    def test_secret_hash_uses_hmac_sha256(self):
        """_secret_hash should use HMAC-SHA256 as AWS requires, not plain SHA-256."""
        username = "testuser"
        client_id = "abc123"
        client_secret = "supersecret"

        # Correct AWS implementation: HMAC-SHA256
        msg = (username + client_id).encode("utf-8")
        expected = base64.b64encode(
            hmac.new(client_secret.encode("utf-8"), msg, hashlib.sha256).digest()
        ).decode("utf-8")

        actual = _secret_hash(username, client_id, client_secret)
        assert actual == expected, (
            f"_secret_hash should use HMAC-SHA256 but got wrong result. "
            f"Expected {expected}, got {actual}"
        )
