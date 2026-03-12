"""
Rotation tests for the SecretsVault.
"""


class TestRotation:
    def test_rotate_db_credentials(self, vault, unique_name, db_credentials):
        """Rotate database credentials, verify new password is different."""
        vault.create_secret(
            name=f"db-rot-{unique_name}",
            namespace="prod",
            secret_type="db_credentials",
            value=db_credentials,
        )

        record = vault.rotate_secret(
            name=f"db-rot-{unique_name}",
            namespace="prod",
            secret_type="db_credentials",
            rotated_by="rotation-lambda",
        )

        assert record.secret_name == f"prod/db-rot-{unique_name}"
        assert record.rotated_by == "rotation-lambda"
        assert record.old_version != record.new_version

        # Verify the new value has a different password
        new_secret = vault.get_secret(f"db-rot-{unique_name}", "prod")
        assert new_secret.value["password"] != db_credentials["password"]
        # Host and username should be preserved
        assert new_secret.value["host"] == db_credentials["host"]
        assert new_secret.value["username"] == db_credentials["username"]

    def test_rotation_history_tracked(self, vault, unique_name, db_credentials):
        """Multiple rotations are recorded in history."""
        vault.create_secret(
            name=f"db-hist-{unique_name}",
            namespace="prod",
            secret_type="db_credentials",
            value=db_credentials,
        )

        vault.rotate_secret(f"db-hist-{unique_name}", "prod", "db_credentials", rotated_by="user-a")
        vault.rotate_secret(f"db-hist-{unique_name}", "prod", "db_credentials", rotated_by="user-b")

        history = vault.get_rotation_history(f"db-hist-{unique_name}", "prod")
        assert len(history) == 2
        rotators = {r.rotated_by for r in history}
        assert "user-a" in rotators
        assert "user-b" in rotators

    def test_previous_version_accessible(self, vault, unique_name, db_credentials):
        """After rotation, old version is still accessible by version ID."""
        vault.create_secret(
            name=f"db-ver-{unique_name}",
            namespace="prod",
            secret_type="db_credentials",
            value=db_credentials,
        )

        vault.rotate_secret(f"db-ver-{unique_name}", "prod", "db_credentials")

        # Current version should have new password
        current = vault.get_secret(f"db-ver-{unique_name}", "prod")
        assert current.value["password"] != db_credentials["password"]

        # Old version should still have original password (by version stage)
        old = vault.get_secret(f"db-ver-{unique_name}", "prod", version_stage="AWSPREVIOUS")
        assert old.value["password"] == db_credentials["password"]

    def test_bulk_rotate(self, vault, unique_name, db_credentials, api_key_secret):
        """Bulk rotate all secrets in a namespace."""
        vault.create_secret(
            name=f"db-bulk-{unique_name}",
            namespace="staging",
            secret_type="db_credentials",
            value=db_credentials,
        )
        vault.create_secret(
            name=f"key-bulk-{unique_name}",
            namespace="staging",
            secret_type="api_key",
            value=api_key_secret,
        )

        records = vault.bulk_rotate("staging", rotated_by="bulk-cron")
        assert len(records) == 2
        assert all(r.rotated_by == "bulk-cron" for r in records)

    def test_emergency_rotation(self, vault, unique_name, db_credentials):
        """Emergency rotation generates new credentials and tags the secret."""
        vault.create_secret(
            name=f"db-emerg-{unique_name}",
            namespace="prod",
            secret_type="db_credentials",
            value=db_credentials,
        )

        record = vault.emergency_rotate(
            name=f"db-emerg-{unique_name}",
            namespace="prod",
            secret_type="db_credentials",
            rotated_by="incident-response",
        )

        assert record.rotated_by == "incident-response"

        # New value should have different password
        current = vault.get_secret(f"db-emerg-{unique_name}", "prod")
        assert current.value["password"] != db_credentials["password"]

    def test_rotation_schedule(self, vault, unique_name, db_credentials):
        """Verify rotation schedule reports last_rotated and next_due."""
        vault.create_secret(
            name=f"db-sched-{unique_name}",
            namespace="prod",
            secret_type="db_credentials",
            value=db_credentials,
        )

        vault.rotate_secret(f"db-sched-{unique_name}", "prod", "db_credentials")

        schedule = vault.get_rotation_schedule(f"db-sched-{unique_name}", "prod", rotation_days=90)
        assert schedule["last_rotated"] is not None
        assert schedule["next_due"] is not None
        assert schedule["next_due"] > schedule["last_rotated"]
        assert schedule["overdue"] is False
