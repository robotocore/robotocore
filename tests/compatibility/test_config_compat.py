"""AWS Config compatibility tests."""

import json

import pytest

from tests.compatibility.conftest import make_client


@pytest.fixture
def config():
    return make_client("config")


@pytest.fixture
def iam():
    return make_client("iam")


class TestConfigOperations:
    def test_put_config_rule(self, config):
        config.put_config_rule(
            ConfigRule={
                "ConfigRuleName": "test-rule",
                "Source": {
                    "Owner": "AWS",
                    "SourceIdentifier": "S3_BUCKET_VERSIONING_ENABLED",
                },
            }
        )
        response = config.describe_config_rules(ConfigRuleNames=["test-rule"])
        assert len(response["ConfigRules"]) == 1
        assert response["ConfigRules"][0]["ConfigRuleName"] == "test-rule"
        config.delete_config_rule(ConfigRuleName="test-rule")

    def test_describe_config_rules(self, config):
        config.put_config_rule(
            ConfigRule={
                "ConfigRuleName": "list-rule",
                "Source": {
                    "Owner": "AWS",
                    "SourceIdentifier": "S3_BUCKET_VERSIONING_ENABLED",
                },
            }
        )
        response = config.describe_config_rules()
        names = [r["ConfigRuleName"] for r in response["ConfigRules"]]
        assert "list-rule" in names
        config.delete_config_rule(ConfigRuleName="list-rule")

    def test_put_configuration_recorder(self, config, iam):
        role = iam.create_role(
            RoleName="config-role",
            AssumeRolePolicyDocument=json.dumps(
                {
                    "Version": "2012-10-17",
                    "Statement": [
                        {
                            "Effect": "Allow",
                            "Principal": {"Service": "config.amazonaws.com"},
                            "Action": "sts:AssumeRole",
                        }
                    ],
                }
            ),
        )
        config.put_configuration_recorder(
            ConfigurationRecorder={
                "name": "default",
                "roleARN": role["Role"]["Arn"],
                "recordingGroup": {"allSupported": True},
            }
        )
        response = config.describe_configuration_recorders()
        assert len(response["ConfigurationRecorders"]) >= 1
        iam.delete_role(RoleName="config-role")

    def test_delete_config_rule(self, config):
        config.put_config_rule(
            ConfigRule={
                "ConfigRuleName": "delete-rule",
                "Source": {
                    "Owner": "AWS",
                    "SourceIdentifier": "S3_BUCKET_VERSIONING_ENABLED",
                },
            }
        )
        config.delete_config_rule(ConfigRuleName="delete-rule")
        response = config.describe_config_rules()
        names = [r["ConfigRuleName"] for r in response["ConfigRules"]]
        assert "delete-rule" not in names
