"""
CRUD tests for the SecretsVault.
"""

import pytest


class TestCreateAndRead:
    def test_create_secret_and_read_back(self, vault, unique_name, db_credentials):
        """Create a DB credential secret, read it back, verify all fields."""
        secret = vault.create_secret(
            name=f"db-{unique_name}",
            namespace="prod",
            secret_type="db_credentials",
            value=db_credentials,
            tags={"Team": "platform", "Environment": "prod"},
            rotation_days=30,
        )

        assert secret.name == f"db-{unique_name}"
        assert secret.namespace == "prod"
        assert secret.type == "db_credentials"
        assert secret.value["host"] == "db.prod.example.com"
        assert secret.value["port"] == 5432
        assert secret.value["password"] == "s3cur3-pr0d-p@ssw0rd!"
        assert secret.tags["Team"] == "platform"
        assert secret.rotation_days == 30
        assert secret.version_id is not None

        # Read back via get_secret
        retrieved = vault.get_secret(f"db-{unique_name}", "prod")
        assert retrieved.value["host"] == "db.prod.example.com"
        assert retrieved.value["username"] == "app_service"

    def test_create_with_tags_and_list_by_tag(self, vault, unique_name, api_key_secret):
        """Create secrets with tags, list by tag value."""
        vault.create_secret(
            name=f"key1-{unique_name}",
            namespace="dev",
            secret_type="api_key",
            value=api_key_secret,
            tags={"Team": "payments"},
        )
        vault.create_secret(
            name=f"key2-{unique_name}",
            namespace="dev",
            secret_type="api_key",
            value={**api_key_secret, "service": "notifications"},
            tags={"Team": "notifications"},
        )

        payments = vault.list_secrets_by_tag("Team", "payments")
        names = [s["name"] for s in payments]
        assert any(f"key1-{unique_name}" in n for n in names)

    def test_update_secret_value_creates_new_version(self, vault, unique_name, db_credentials):
        """Update a secret, verify new value and version change."""
        vault.create_secret(
            name=f"db-upd-{unique_name}",
            namespace="staging",
            secret_type="db_credentials",
            value=db_credentials,
        )

        new_version = vault.update_secret(
            name=f"db-upd-{unique_name}",
            namespace="staging",
            new_value={**db_credentials, "password": "new-rotated-pw"},
            secret_type="db_credentials",
        )
        assert new_version  # non-empty version ID

        retrieved = vault.get_secret(f"db-upd-{unique_name}", "staging")
        assert retrieved.value["password"] == "new-rotated-pw"

    def test_delete_secret(self, vault, unique_name, api_key_secret):
        """Delete a secret, verify it's gone."""
        vault.create_secret(
            name=f"del-{unique_name}",
            namespace="dev",
            secret_type="api_key",
            value=api_key_secret,
        )

        result = vault.delete_secret(f"del-{unique_name}", "dev", force=True)
        assert result["deleted"] is True

        with pytest.raises(Exception):
            # Should fail -- secret is gone
            vault.get_secret(f"del-{unique_name}", "dev")

    def test_delete_and_restore(self, vault, unique_name, api_key_secret):
        """Schedule deletion, then restore, verify value intact."""
        vault.create_secret(
            name=f"restore-{unique_name}",
            namespace="prod",
            secret_type="api_key",
            value=api_key_secret,
        )

        result = vault.delete_secret(
            f"restore-{unique_name}", "prod", force=False, recovery_window_days=7
        )
        assert result["deletion_date"]  # has a scheduled deletion date

        vault.restore_secret(f"restore-{unique_name}", "prod")
        retrieved = vault.get_secret(f"restore-{unique_name}", "prod")
        assert retrieved.value["key"] == "ak-0123456789abcdef"

    def test_namespace_isolation(self, vault, unique_name, db_credentials):
        """Secrets in different namespaces are isolated."""
        vault.create_secret(
            name=f"db-{unique_name}",
            namespace="dev",
            secret_type="db_credentials",
            value={**db_credentials, "host": "dev-db.example.com"},
        )
        vault.create_secret(
            name=f"db-{unique_name}",
            namespace="prod",
            secret_type="db_credentials",
            value={**db_credentials, "host": "prod-db.example.com"},
        )

        dev_secret = vault.get_secret(f"db-{unique_name}", "dev")
        prod_secret = vault.get_secret(f"db-{unique_name}", "prod")

        assert dev_secret.value["host"] == "dev-db.example.com"
        assert prod_secret.value["host"] == "prod-db.example.com"
