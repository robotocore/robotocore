"""
Audit logging tests for the SecretsVault.
"""


class TestAuditLog:
    def test_access_creates_audit_entry(self, vault, unique_name, db_credentials):
        """Accessing a secret creates an audit log entry."""
        vault.create_secret(
            name=f"db-aud-{unique_name}",
            namespace="prod",
            secret_type="db_credentials",
            value=db_credentials,
        )

        vault.get_secret(f"db-aud-{unique_name}", "prod", accessor="deploy-service")

        entries = vault.get_audit_log(secret_name=f"db-aud-{unique_name}", namespace="prod")
        assert len(entries) >= 1
        assert entries[0].accessor == "deploy-service"
        assert entries[0].secret_name == f"prod/db-aud-{unique_name}"

    def test_multiple_accesses_logged(self, vault, unique_name, api_key_secret):
        """Multiple accesses by different principals are all logged."""
        vault.create_secret(
            name=f"key-aud-{unique_name}",
            namespace="dev",
            secret_type="api_key",
            value=api_key_secret,
        )

        vault.get_secret(f"key-aud-{unique_name}", "dev", accessor="service-a")
        vault.get_secret(f"key-aud-{unique_name}", "dev", accessor="service-b")
        vault.get_secret(f"key-aud-{unique_name}", "dev", accessor="service-a")

        entries = vault.get_audit_log(secret_name=f"key-aud-{unique_name}", namespace="dev")
        assert len(entries) == 3

    def test_query_audit_by_secret_name(self, vault, unique_name, db_credentials):
        """Query audit log filtered by secret name."""
        vault.create_secret(
            name=f"db1-aud-{unique_name}",
            namespace="prod",
            secret_type="db_credentials",
            value=db_credentials,
        )
        vault.create_secret(
            name=f"db2-aud-{unique_name}",
            namespace="prod",
            secret_type="db_credentials",
            value={**db_credentials, "host": "other-db.example.com"},
        )

        vault.get_secret(f"db1-aud-{unique_name}", "prod", accessor="svc")
        vault.get_secret(f"db2-aud-{unique_name}", "prod", accessor="svc")

        entries = vault.get_audit_log(secret_name=f"db1-aud-{unique_name}", namespace="prod")
        assert len(entries) == 1
        assert f"db1-aud-{unique_name}" in entries[0].secret_name

    def test_query_audit_by_accessor(self, vault, unique_name, api_key_secret):
        """Query audit log filtered by accessor principal."""
        vault.create_secret(
            name=f"key-acc-{unique_name}",
            namespace="dev",
            secret_type="api_key",
            value=api_key_secret,
        )

        vault.get_secret(f"key-acc-{unique_name}", "dev", accessor="admin-user")
        vault.get_secret(f"key-acc-{unique_name}", "dev", accessor="read-only-svc")

        admin_entries = vault.get_audit_log(accessor="admin-user")
        assert len(admin_entries) >= 1
        assert all(e.accessor == "admin-user" for e in admin_entries)

    def test_audit_includes_version(self, vault, unique_name, db_credentials):
        """Audit log entries include the version ID that was accessed."""
        vault.create_secret(
            name=f"db-ver-aud-{unique_name}",
            namespace="prod",
            secret_type="db_credentials",
            value=db_credentials,
        )

        vault.get_secret(f"db-ver-aud-{unique_name}", "prod", accessor="ci-pipeline")

        entries = vault.get_audit_log(secret_name=f"db-ver-aud-{unique_name}", namespace="prod")
        assert len(entries) >= 1
        assert entries[0].version_accessed  # non-empty version ID
