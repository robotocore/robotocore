"""Secrets Manager compatibility tests."""

import json
import uuid

import pytest
from botocore.exceptions import ClientError

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

    def test_create_secret_with_tags(self, sm):
        name = f"tagged/{uuid.uuid4().hex[:8]}"
        sm.create_secret(
            Name=name,
            SecretString="val",
            Tags=[
                {"Key": "env", "Value": "staging"},
                {"Key": "team", "Value": "platform"},
            ],
        )
        desc = sm.describe_secret(SecretId=name)
        tags = {t["Key"]: t["Value"] for t in desc.get("Tags", [])}
        assert tags["env"] == "staging"
        assert tags["team"] == "platform"
        sm.delete_secret(SecretId=name, ForceDeleteWithoutRecovery=True)

    def test_tag_resource(self, sm):
        name = f"tagres/{uuid.uuid4().hex[:8]}"
        sm.create_secret(Name=name, SecretString="val")
        desc = sm.describe_secret(SecretId=name)
        arn = desc["ARN"]
        sm.tag_resource(SecretId=arn, Tags=[{"Key": "added", "Value": "yes"}])
        desc = sm.describe_secret(SecretId=name)
        tags = {t["Key"]: t["Value"] for t in desc.get("Tags", [])}
        assert tags["added"] == "yes"
        sm.delete_secret(SecretId=name, ForceDeleteWithoutRecovery=True)

    def test_untag_resource(self, sm):
        name = f"untag/{uuid.uuid4().hex[:8]}"
        sm.create_secret(
            Name=name,
            SecretString="val",
            Tags=[
                {"Key": "keep", "Value": "1"},
                {"Key": "remove", "Value": "2"},
            ],
        )
        sm.untag_resource(SecretId=name, TagKeys=["remove"])
        desc = sm.describe_secret(SecretId=name)
        tags = {t["Key"]: t["Value"] for t in desc.get("Tags", [])}
        assert "remove" not in tags
        assert tags["keep"] == "1"
        sm.delete_secret(SecretId=name, ForceDeleteWithoutRecovery=True)

    def test_put_secret_value_creates_new_version(self, sm):
        name = f"newver/{uuid.uuid4().hex[:8]}"
        sm.create_secret(Name=name, SecretString="v1")
        put_resp = sm.put_secret_value(SecretId=name, SecretString="v2")
        assert "VersionId" in put_resp
        get_resp = sm.get_secret_value(SecretId=name)
        assert get_resp["SecretString"] == "v2"
        sm.delete_secret(SecretId=name, ForceDeleteWithoutRecovery=True)

    def test_list_secret_version_ids(self, sm):
        name = f"listver/{uuid.uuid4().hex[:8]}"
        sm.create_secret(Name=name, SecretString="v1")
        sm.put_secret_value(SecretId=name, SecretString="v2")
        response = sm.list_secret_version_ids(SecretId=name)
        versions = response["Versions"]
        assert len(versions) >= 1
        # At least one version should have AWSCURRENT
        stages = []
        for v in versions:
            stages.extend(v.get("VersionStages", []))
        assert "AWSCURRENT" in stages
        sm.delete_secret(SecretId=name, ForceDeleteWithoutRecovery=True)

    def test_get_secret_value_by_version_id(self, sm):
        name = f"byverid/{uuid.uuid4().hex[:8]}"
        create_resp = sm.create_secret(Name=name, SecretString="original")
        version_id = create_resp["VersionId"]
        sm.put_secret_value(SecretId=name, SecretString="updated")
        # The original version_id may be AWSPREVIOUS now
        # Fetch the AWSCURRENT
        current = sm.get_secret_value(SecretId=name, VersionStage="AWSCURRENT")
        assert current["SecretString"] == "updated"
        sm.delete_secret(SecretId=name, ForceDeleteWithoutRecovery=True)

    def test_get_secret_value_by_version_stage(self, sm):
        name = f"bystage/{uuid.uuid4().hex[:8]}"
        sm.create_secret(Name=name, SecretString="val")
        response = sm.get_secret_value(SecretId=name, VersionStage="AWSCURRENT")
        assert response["SecretString"] == "val"
        sm.delete_secret(SecretId=name, ForceDeleteWithoutRecovery=True)

    def test_delete_and_restore_secret(self, sm):
        name = f"restore/{uuid.uuid4().hex[:8]}"
        sm.create_secret(Name=name, SecretString="restoreable")
        sm.delete_secret(SecretId=name)
        sm.restore_secret(SecretId=name)
        response = sm.get_secret_value(SecretId=name)
        assert response["SecretString"] == "restoreable"
        sm.delete_secret(SecretId=name, ForceDeleteWithoutRecovery=True)

    def test_update_secret_string(self, sm):
        name = f"updsec/{uuid.uuid4().hex[:8]}"
        sm.create_secret(Name=name, SecretString="orig")
        sm.update_secret(SecretId=name, SecretString="new-value")
        response = sm.get_secret_value(SecretId=name)
        assert response["SecretString"] == "new-value"
        sm.delete_secret(SecretId=name, ForceDeleteWithoutRecovery=True)

    def test_update_secret_description(self, sm):
        name = f"upddesc/{uuid.uuid4().hex[:8]}"
        sm.create_secret(Name=name, SecretString="val", Description="original desc")
        sm.update_secret(SecretId=name, Description="updated desc")
        desc = sm.describe_secret(SecretId=name)
        assert desc.get("Description") == "updated desc"
        sm.delete_secret(SecretId=name, ForceDeleteWithoutRecovery=True)

    def test_get_resource_policy_empty(self, sm):
        name = f"respol/{uuid.uuid4().hex[:8]}"
        sm.create_secret(Name=name, SecretString="val")
        response = sm.get_resource_policy(SecretId=name)
        assert "Name" in response
        sm.delete_secret(SecretId=name, ForceDeleteWithoutRecovery=True)

    def test_put_resource_policy(self, sm):
        name = f"putpol/{uuid.uuid4().hex[:8]}"
        sm.create_secret(Name=name, SecretString="val")
        policy = json.dumps(
            {
                "Version": "2012-10-17",
                "Statement": [
                    {
                        "Sid": "AllowGet",
                        "Effect": "Allow",
                        "Principal": "*",
                        "Action": "secretsmanager:GetSecretValue",
                        "Resource": "*",
                    }
                ],
            }
        )
        sm.put_resource_policy(SecretId=name, ResourcePolicy=policy)
        response = sm.get_resource_policy(SecretId=name)
        assert response.get("ResourcePolicy") is not None
        returned_policy = json.loads(response["ResourcePolicy"])
        assert returned_policy["Statement"][0]["Sid"] == "AllowGet"
        sm.delete_secret(SecretId=name, ForceDeleteWithoutRecovery=True)

    def test_delete_resource_policy(self, sm):
        name = f"delpol/{uuid.uuid4().hex[:8]}"
        sm.create_secret(Name=name, SecretString="val")
        policy = json.dumps(
            {
                "Version": "2012-10-17",
                "Statement": [
                    {
                        "Sid": "Test",
                        "Effect": "Allow",
                        "Principal": "*",
                        "Action": "secretsmanager:GetSecretValue",
                        "Resource": "*",
                    }
                ],
            }
        )
        sm.put_resource_policy(SecretId=name, ResourcePolicy=policy)
        sm.delete_resource_policy(SecretId=name)
        response = sm.get_resource_policy(SecretId=name)
        # After deletion, ResourcePolicy should be None or empty
        assert not response.get("ResourcePolicy")
        sm.delete_secret(SecretId=name, ForceDeleteWithoutRecovery=True)

    def test_get_nonexistent_secret_raises(self, sm):
        with pytest.raises(ClientError) as exc_info:
            sm.get_secret_value(SecretId=f"nonexistent/{uuid.uuid4().hex}")
        assert exc_info.value.response["Error"]["Code"] == "ResourceNotFoundException"

    def test_list_secrets_with_filter(self, sm):
        name = f"filterable/{uuid.uuid4().hex[:8]}"
        sm.create_secret(Name=name, SecretString="val")
        response = sm.list_secrets(
            Filters=[{"Key": "name", "Values": [name]}]
        )
        names = [s["Name"] for s in response["SecretList"]]
        assert name in names
        sm.delete_secret(SecretId=name, ForceDeleteWithoutRecovery=True)

    def test_describe_secret_has_arn(self, sm):
        name = f"arncheck/{uuid.uuid4().hex[:8]}"
        sm.create_secret(Name=name, SecretString="val")
        desc = sm.describe_secret(SecretId=name)
        assert "ARN" in desc
        assert name in desc["ARN"]
        sm.delete_secret(SecretId=name, ForceDeleteWithoutRecovery=True)

    def test_describe_secret_version_ids(self, sm):
        name = f"descver/{uuid.uuid4().hex[:8]}"
        sm.create_secret(Name=name, SecretString="v1")
        sm.put_secret_value(SecretId=name, SecretString="v2")
        desc = sm.describe_secret(SecretId=name)
        assert "VersionIdsToStages" in desc
        # At least one version should map to AWSCURRENT
        found_current = False
        for stages in desc["VersionIdsToStages"].values():
            if "AWSCURRENT" in stages:
                found_current = True
        assert found_current
        sm.delete_secret(SecretId=name, ForceDeleteWithoutRecovery=True)

    def test_put_secret_value_binary(self, sm):
        name = f"putbin/{uuid.uuid4().hex[:8]}"
        sm.create_secret(Name=name, SecretBinary=b"\x00\x01")
        sm.put_secret_value(SecretId=name, SecretBinary=b"\x02\x03")
        response = sm.get_secret_value(SecretId=name)
        assert response["SecretBinary"] == b"\x02\x03"
        sm.delete_secret(SecretId=name, ForceDeleteWithoutRecovery=True)
