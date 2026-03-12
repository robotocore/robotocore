"""
Schema validation tests for the SecretsVault.
"""

import pytest

from .app import ValidationError
from .models import SecretTemplate


class TestValidation:
    def test_valid_db_credentials(self, vault, unique_name, db_credentials):
        """Creating a secret matching the db_credentials template succeeds."""
        secret = vault.create_secret(
            name=f"db-val-{unique_name}",
            namespace="dev",
            secret_type="db_credentials",
            value=db_credentials,
        )
        assert secret.value["host"] == db_credentials["host"]

    def test_missing_required_field_fails(self, vault, unique_name):
        """Creating a secret missing a required field raises ValidationError."""
        incomplete = {
            "host": "db.example.com",
            "port": 5432,
            # missing "username" and "password"
        }
        with pytest.raises(ValidationError, match="Missing required field"):
            vault.create_secret(
                name=f"db-bad-{unique_name}",
                namespace="dev",
                secret_type="db_credentials",
                value=incomplete,
            )

    def test_update_with_invalid_schema_fails(self, vault, unique_name, db_credentials):
        """Updating a secret to an invalid schema raises ValidationError."""
        vault.create_secret(
            name=f"db-upd-val-{unique_name}",
            namespace="dev",
            secret_type="db_credentials",
            value=db_credentials,
        )

        with pytest.raises(ValidationError, match="Missing required field"):
            vault.update_secret(
                name=f"db-upd-val-{unique_name}",
                namespace="dev",
                new_value={"host": "only-host"},
                secret_type="db_credentials",
            )

    def test_register_custom_template(self, vault, unique_name):
        """Register a custom template and validate against it."""
        vault.register_template(
            SecretTemplate(
                type_name="oauth_token",
                required_fields=["client_id", "client_secret", "token_url"],
                field_types={
                    "client_id": "str",
                    "client_secret": "str",
                    "token_url": "str",
                },
            )
        )

        # Valid value
        secret = vault.create_secret(
            name=f"oauth-{unique_name}",
            namespace="dev",
            secret_type="oauth_token",
            value={
                "client_id": "my-app",
                "client_secret": "secret-xyz",
                "token_url": "https://auth.example.com/token",
            },
        )
        assert secret.type == "oauth_token"

        # Invalid value -- missing token_url
        with pytest.raises(ValidationError, match="token_url"):
            vault.create_secret(
                name=f"oauth-bad-{unique_name}",
                namespace="dev",
                secret_type="oauth_token",
                value={"client_id": "my-app", "client_secret": "xyz"},
            )

    def test_validate_api_key_template(self, vault, unique_name, api_key_secret):
        """API key template validates correctly."""
        secret = vault.create_secret(
            name=f"apikey-val-{unique_name}",
            namespace="prod",
            secret_type="api_key",
            value=api_key_secret,
        )
        assert secret.value["service"] == "payment-gateway"

    def test_validate_certificate_template(self, vault, unique_name, certificate_secret):
        """Certificate template validates correctly."""
        secret = vault.create_secret(
            name=f"cert-val-{unique_name}",
            namespace="prod",
            secret_type="certificate",
            value=certificate_secret,
        )
        assert "BEGIN CERTIFICATE" in secret.value["cert_body"]

    def test_wrong_field_type_fails(self, vault, unique_name):
        """A field with the wrong type raises ValidationError."""
        bad_creds = {
            "host": "db.example.com",
            "port": "not-a-number",  # should be int
            "username": "user",
            "password": "pass",
        }
        with pytest.raises(ValidationError, match="expected int"):
            vault.create_secret(
                name=f"db-type-{unique_name}",
                namespace="dev",
                secret_type="db_credentials",
                value=bad_creds,
            )
