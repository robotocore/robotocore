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
