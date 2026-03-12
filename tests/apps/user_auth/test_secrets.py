"""
Tests for JWT and OAuth secret management.

Covers: JWT signing key storage and retrieval, key rotation,
OAuth client credentials for multiple providers.
"""

import json


class TestSecrets:
    def test_jwt_signing_key_stored(self, auth, secretsmanager, secrets_prefix):
        """Store a JWT signing key and retrieve it."""
        auth.store_jwt_secret("my-signing-key-2026")
        key = auth.get_jwt_secret()
        assert key == "my-signing-key-2026"

    def test_rotate_jwt_key(self, auth, secretsmanager, secrets_prefix):
        """Rotate JWT key: new key is returned, previous key preserved."""
        auth.store_jwt_secret("original-key")
        auth.rotate_jwt_secret("rotated-key-v2")

        key = auth.get_jwt_secret()
        assert key == "rotated-key-v2"

        # Verify previous key is preserved in the secret
        resp = secretsmanager.get_secret_value(SecretId=f"{secrets_prefix}/jwt")
        data = json.loads(resp["SecretString"])
        assert data["previous_key"] == "original-key"
        assert data["signing_key"] == "rotated-key-v2"
        assert "rotated_at" in data

    def test_oauth_credentials_stored(self, auth, secretsmanager, secrets_prefix):
        """Store and retrieve OAuth credentials for a provider."""
        auth.store_oauth_credentials("google", "google-client-id", "google-client-secret")
        creds = auth.get_oauth_credentials("google")
        assert creds["client_id"] == "google-client-id"
        assert creds["client_secret"] == "google-client-secret"
        assert creds["provider"] == "google"

    def test_multiple_oauth_providers(self, auth):
        """Store credentials for multiple OAuth providers independently."""
        auth.store_oauth_credentials("google", "g-id", "g-secret")
        auth.store_oauth_credentials("github", "gh-id", "gh-secret")
        auth.store_oauth_credentials("facebook", "fb-id", "fb-secret")

        google = auth.get_oauth_credentials("google")
        github = auth.get_oauth_credentials("github")
        facebook = auth.get_oauth_credentials("facebook")

        assert google["client_id"] == "g-id"
        assert github["client_id"] == "gh-id"
        assert facebook["client_id"] == "fb-id"
