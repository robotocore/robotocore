"""
Secrets Rotation Application Tests

Simulates a secrets management workflow: creating database credentials,
rotating them, managing versions, and handling deletion/restoration.
"""

import json

import pytest


@pytest.fixture
def secret_name(unique_name):
    return f"prod/database/credentials-{unique_name}"


class TestSecretsRotationApp:
    def test_create_and_read_secret(self, secretsmanager, secret_name):
        """Create a secret with JSON value, read it back."""
        secret_value = json.dumps(
            {
                "host": "db.example.com",
                "port": 5432,
                "username": "app_user",
                "password": "s3cur3-p@ss!",
            }
        )

        secretsmanager.create_secret(
            Name=secret_name,
            SecretString=secret_value,
            Description="Production database credentials",
        )

        response = secretsmanager.get_secret_value(SecretId=secret_name)
        retrieved = json.loads(response["SecretString"])
        assert retrieved["host"] == "db.example.com"
        assert retrieved["port"] == 5432
        assert retrieved["username"] == "app_user"
        assert retrieved["password"] == "s3cur3-p@ss!"
        assert response["Name"] == secret_name

        secretsmanager.delete_secret(SecretId=secret_name, ForceDeleteWithoutRecovery=True)

    def test_update_secret(self, secretsmanager, secret_name):
        """Create a secret, update its value, verify new value returned."""
        original = json.dumps({"password": "old-password-123"})
        secretsmanager.create_secret(Name=secret_name, SecretString=original)

        updated = json.dumps({"password": "new-password-456"})
        secretsmanager.update_secret(SecretId=secret_name, SecretString=updated)

        response = secretsmanager.get_secret_value(SecretId=secret_name)
        value = json.loads(response["SecretString"])
        assert value["password"] == "new-password-456"

        secretsmanager.delete_secret(SecretId=secret_name, ForceDeleteWithoutRecovery=True)

    def test_secret_versions(self, secretsmanager, secret_name):
        """Create and update a secret, verify version stages exist."""
        secretsmanager.create_secret(
            Name=secret_name,
            SecretString=json.dumps({"version": 1}),
        )

        secretsmanager.update_secret(
            SecretId=secret_name,
            SecretString=json.dumps({"version": 2}),
        )

        response = secretsmanager.list_secret_version_ids(SecretId=secret_name)
        versions = response["Versions"]
        assert len(versions) >= 1

        # At least one version should have AWSCURRENT stage
        current_versions = [v for v in versions if "AWSCURRENT" in v.get("VersionStages", [])]
        assert len(current_versions) == 1

        secretsmanager.delete_secret(SecretId=secret_name, ForceDeleteWithoutRecovery=True)

    def test_delete_and_restore(self, secretsmanager, secret_name):
        """Create, schedule deletion, restore, and verify value intact."""
        original_value = json.dumps({"api_key": "ak-12345"})
        secretsmanager.create_secret(Name=secret_name, SecretString=original_value)

        # Schedule deletion (minimum 7 days)
        delete_resp = secretsmanager.delete_secret(SecretId=secret_name, RecoveryWindowInDays=7)
        assert "DeletionDate" in delete_resp

        # Restore
        secretsmanager.restore_secret(SecretId=secret_name)

        # Verify value is intact
        response = secretsmanager.get_secret_value(SecretId=secret_name)
        value = json.loads(response["SecretString"])
        assert value["api_key"] == "ak-12345"

        secretsmanager.delete_secret(SecretId=secret_name, ForceDeleteWithoutRecovery=True)

    def test_secret_tags(self, secretsmanager, secret_name):
        """Create secret with tags, list, add, and remove tags."""
        secretsmanager.create_secret(
            Name=secret_name,
            SecretString="placeholder",
            Tags=[
                {"Key": "Environment", "Value": "production"},
                {"Key": "Team", "Value": "platform"},
            ],
        )

        desc = secretsmanager.describe_secret(SecretId=secret_name)
        tags = {t["Key"]: t["Value"] for t in desc.get("Tags", [])}
        assert tags["Environment"] == "production"
        assert tags["Team"] == "platform"

        # Add a tag
        secretsmanager.tag_resource(
            SecretId=secret_name,
            Tags=[{"Key": "CostCenter", "Value": "eng-42"}],
        )

        desc = secretsmanager.describe_secret(SecretId=secret_name)
        tags = {t["Key"]: t["Value"] for t in desc.get("Tags", [])}
        assert tags["CostCenter"] == "eng-42"
        assert len(tags) == 3

        # Remove a tag
        secretsmanager.untag_resource(SecretId=secret_name, TagKeys=["Team"])

        desc = secretsmanager.describe_secret(SecretId=secret_name)
        tags = {t["Key"]: t["Value"] for t in desc.get("Tags", [])}
        assert "Team" not in tags
        assert len(tags) == 2

        secretsmanager.delete_secret(SecretId=secret_name, ForceDeleteWithoutRecovery=True)
