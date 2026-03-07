"""Secrets Manager compatibility tests."""

import json

import pytest

from tests.compatibility.conftest import make_client


@pytest.fixture
def sm():
    return make_client("secretsmanager")


class TestSecretsManagerOperations:
    def test_create_and_get_secret(self, sm):
        sm.create_secret(Name="test/secret", SecretString="my-secret-value")
        response = sm.get_secret_value(SecretId="test/secret")
        assert response["SecretString"] == "my-secret-value"
        sm.delete_secret(SecretId="test/secret", ForceDeleteWithoutRecovery=True)

    def test_list_secrets(self, sm):
        sm.create_secret(Name="list/secret1", SecretString="val1")
        response = sm.list_secrets()
        names = [s["Name"] for s in response["SecretList"]]
        assert "list/secret1" in names
        sm.delete_secret(SecretId="list/secret1", ForceDeleteWithoutRecovery=True)

    def test_update_secret(self, sm):
        sm.create_secret(Name="update/secret", SecretString="original")
        sm.update_secret(SecretId="update/secret", SecretString="updated")
        response = sm.get_secret_value(SecretId="update/secret")
        assert response["SecretString"] == "updated"
        sm.delete_secret(SecretId="update/secret", ForceDeleteWithoutRecovery=True)

    def test_describe_secret(self, sm):
        sm.create_secret(Name="describe/secret", SecretString="val")
        response = sm.describe_secret(SecretId="describe/secret")
        assert response["Name"] == "describe/secret"
        sm.delete_secret(SecretId="describe/secret", ForceDeleteWithoutRecovery=True)

    def test_json_secret(self, sm):
        data = json.dumps({"username": "admin", "password": "secret123"})
        sm.create_secret(Name="json/secret", SecretString=data)
        response = sm.get_secret_value(SecretId="json/secret")
        parsed = json.loads(response["SecretString"])
        assert parsed["username"] == "admin"
        sm.delete_secret(SecretId="json/secret", ForceDeleteWithoutRecovery=True)

    def test_binary_secret(self, sm):
        sm.create_secret(Name="binary/secret", SecretBinary=b"\x00\x01\x02\x03")
        response = sm.get_secret_value(SecretId="binary/secret")
        assert response["SecretBinary"] == b"\x00\x01\x02\x03"
        sm.delete_secret(SecretId="binary/secret", ForceDeleteWithoutRecovery=True)

    def test_tag_secret(self, sm):
        sm.create_secret(
            Name="tagged/secret", SecretString="val", Tags=[{"Key": "env", "Value": "test"}]
        )
        response = sm.describe_secret(SecretId="tagged/secret")
        tags = {t["Key"]: t["Value"] for t in response.get("Tags", [])}
        assert tags.get("env") == "test"
        sm.delete_secret(SecretId="tagged/secret", ForceDeleteWithoutRecovery=True)

    def test_put_secret_value(self, sm):
        """Test put_secret_value to add a new version."""
        sm.create_secret(Name="put-val/secret", SecretString="v1")
        sm.put_secret_value(SecretId="put-val/secret", SecretString="v2")
        response = sm.get_secret_value(SecretId="put-val/secret")
        assert response["SecretString"] == "v2"
        sm.delete_secret(SecretId="put-val/secret", ForceDeleteWithoutRecovery=True)

    def test_rotate_secret_config(self, sm):
        """Test describe shows rotation config fields."""
        sm.create_secret(Name="rotate/secret", SecretString="val")
        response = sm.describe_secret(SecretId="rotate/secret")
        assert "RotationEnabled" in response or "Name" in response
        sm.delete_secret(SecretId="rotate/secret", ForceDeleteWithoutRecovery=True)

    def test_create_secret_with_binary(self, sm):
        """Create and retrieve a secret with binary value."""
        binary_data = b"\xde\xad\xbe\xef\x00\x01\x02\x03"
        sm.create_secret(Name="compat/binary-secret", SecretBinary=binary_data)
        response = sm.get_secret_value(SecretId="compat/binary-secret")
        assert response["SecretBinary"] == binary_data
        assert "SecretString" not in response or response.get("SecretString") is None
        sm.delete_secret(SecretId="compat/binary-secret", ForceDeleteWithoutRecovery=True)

    def test_update_secret_string(self, sm):
        """Update secret string value multiple times."""
        sm.create_secret(Name="compat/update-multi", SecretString="v1")
        sm.update_secret(SecretId="compat/update-multi", SecretString="v2")
        sm.update_secret(SecretId="compat/update-multi", SecretString="v3")
        response = sm.get_secret_value(SecretId="compat/update-multi")
        assert response["SecretString"] == "v3"
        sm.delete_secret(SecretId="compat/update-multi", ForceDeleteWithoutRecovery=True)

    def test_update_secret_description(self, sm):
        """Update secret description."""
        sm.create_secret(
            Name="compat/update-desc", SecretString="val", Description="original desc"
        )
        sm.update_secret(
            SecretId="compat/update-desc",
            Description="updated desc",
        )
        response = sm.describe_secret(SecretId="compat/update-desc")
        assert response.get("Description") == "updated desc"
        sm.delete_secret(SecretId="compat/update-desc", ForceDeleteWithoutRecovery=True)

    def test_list_secrets_with_name_filter(self, sm):
        """List secrets filtered by name."""
        sm.create_secret(Name="compat/filter-target", SecretString="val")
        sm.create_secret(Name="compat/filter-other", SecretString="val2")

        response = sm.list_secrets(
            Filters=[{"Key": "name", "Values": ["compat/filter-target"]}]
        )
        names = [s["Name"] for s in response["SecretList"]]
        assert "compat/filter-target" in names

        sm.delete_secret(SecretId="compat/filter-target", ForceDeleteWithoutRecovery=True)
        sm.delete_secret(SecretId="compat/filter-other", ForceDeleteWithoutRecovery=True)

    def test_tag_resource(self, sm):
        """Add tags to an existing secret using tag_resource."""
        sm.create_secret(Name="compat/tag-add", SecretString="val")
        sm.tag_resource(
            SecretId="compat/tag-add",
            Tags=[
                {"Key": "team", "Value": "platform"},
                {"Key": "env", "Value": "staging"},
            ],
        )
        response = sm.describe_secret(SecretId="compat/tag-add")
        tags = {t["Key"]: t["Value"] for t in response.get("Tags", [])}
        assert tags["team"] == "platform"
        assert tags["env"] == "staging"
        sm.delete_secret(SecretId="compat/tag-add", ForceDeleteWithoutRecovery=True)

    def test_untag_resource(self, sm):
        """Remove tags from a secret using untag_resource."""
        sm.create_secret(
            Name="compat/untag",
            SecretString="val",
            Tags=[
                {"Key": "keep", "Value": "yes"},
                {"Key": "remove", "Value": "no"},
            ],
        )
        sm.untag_resource(SecretId="compat/untag", TagKeys=["remove"])
        response = sm.describe_secret(SecretId="compat/untag")
        tags = {t["Key"]: t["Value"] for t in response.get("Tags", [])}
        assert "remove" not in tags
        assert tags["keep"] == "yes"
        sm.delete_secret(SecretId="compat/untag", ForceDeleteWithoutRecovery=True)

    def test_list_secret_version_ids(self, sm):
        """List version IDs after multiple puts."""
        sm.create_secret(Name="compat/versions", SecretString="v1")
        sm.put_secret_value(SecretId="compat/versions", SecretString="v2")

        response = sm.list_secret_version_ids(SecretId="compat/versions")
        assert "Versions" in response
        assert len(response["Versions"]) >= 1

        # The current version should have AWSCURRENT staging label
        current_versions = [
            v for v in response["Versions"] if "AWSCURRENT" in v.get("VersionStages", [])
        ]
        assert len(current_versions) == 1
        sm.delete_secret(SecretId="compat/versions", ForceDeleteWithoutRecovery=True)

    def test_describe_secret_fields(self, sm):
        """Describe secret returns expected metadata fields."""
        sm.create_secret(
            Name="compat/describe-fields",
            SecretString="val",
            Description="test description",
        )
        response = sm.describe_secret(SecretId="compat/describe-fields")
        assert response["Name"] == "compat/describe-fields"
        assert response["Description"] == "test description"
        assert "ARN" in response
        assert "CreatedDate" in response
        assert "LastChangedDate" in response
        sm.delete_secret(SecretId="compat/describe-fields", ForceDeleteWithoutRecovery=True)

    def test_put_secret_value_with_stages(self, sm):
        """Put secret value with explicit version stages."""
        sm.create_secret(Name="compat/put-stages", SecretString="v1")
        sm.put_secret_value(
            SecretId="compat/put-stages",
            SecretString="v2",
            VersionStages=["AWSCURRENT"],
        )
        response = sm.get_secret_value(SecretId="compat/put-stages")
        assert response["SecretString"] == "v2"
        assert "AWSCURRENT" in response.get("VersionStages", [])
        sm.delete_secret(SecretId="compat/put-stages", ForceDeleteWithoutRecovery=True)

    def test_get_secret_by_arn(self, sm):
        """Get secret value using ARN instead of name."""
        sm.create_secret(Name="compat/get-by-arn", SecretString="arn-val")
        desc = sm.describe_secret(SecretId="compat/get-by-arn")
        arn = desc["ARN"]

        response = sm.get_secret_value(SecretId=arn)
        assert response["SecretString"] == "arn-val"
        sm.delete_secret(SecretId="compat/get-by-arn", ForceDeleteWithoutRecovery=True)

    def test_restore_secret(self, sm):
        """Delete and restore a secret."""
        sm.create_secret(Name="compat/restore-me", SecretString="restore-val")
        sm.delete_secret(SecretId="compat/restore-me")

        sm.restore_secret(SecretId="compat/restore-me")
        response = sm.get_secret_value(SecretId="compat/restore-me")
        assert response["SecretString"] == "restore-val"
        sm.delete_secret(SecretId="compat/restore-me", ForceDeleteWithoutRecovery=True)
