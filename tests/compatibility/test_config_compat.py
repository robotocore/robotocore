"""AWS Config compatibility tests."""

import json
import time

import pytest
from botocore.exceptions import ClientError

from tests.compatibility.conftest import make_client


@pytest.fixture
def config():
    return make_client("config")


@pytest.fixture
def iam():
    return make_client("iam")


@pytest.fixture
def s3():
    return make_client("s3")


class TestConfigRuleOperations:
    """Test Config rule CRUD operations."""

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


class TestConfigRuleDetails:
    """Test Config rule creation with various fields."""

    def test_put_config_rule_with_scope(self, config):
        config.put_config_rule(
            ConfigRule={
                "ConfigRuleName": "scoped-rule",
                "Source": {
                    "Owner": "AWS",
                    "SourceIdentifier": "S3_BUCKET_VERSIONING_ENABLED",
                },
                "Scope": {
                    "ComplianceResourceTypes": ["AWS::S3::Bucket"],
                },
            }
        )
        response = config.describe_config_rules(ConfigRuleNames=["scoped-rule"])
        rule = response["ConfigRules"][0]
        assert rule["Scope"]["ComplianceResourceTypes"] == ["AWS::S3::Bucket"]
        config.delete_config_rule(ConfigRuleName="scoped-rule")

    def test_put_config_rule_with_input_parameters(self, config):
        config.put_config_rule(
            ConfigRule={
                "ConfigRuleName": "param-rule",
                "Source": {
                    "Owner": "AWS",
                    "SourceIdentifier": "S3_BUCKET_VERSIONING_ENABLED",
                },
                "InputParameters": json.dumps({"maxDays": "90"}),
            }
        )
        response = config.describe_config_rules(ConfigRuleNames=["param-rule"])
        rule = response["ConfigRules"][0]
        params = json.loads(rule["InputParameters"])
        assert params["maxDays"] == "90"
        config.delete_config_rule(ConfigRuleName="param-rule")

    def test_update_config_rule(self, config):
        config.put_config_rule(
            ConfigRule={
                "ConfigRuleName": "update-rule",
                "Source": {
                    "Owner": "AWS",
                    "SourceIdentifier": "S3_BUCKET_VERSIONING_ENABLED",
                },
                "Description": "original",
            }
        )
        config.put_config_rule(
            ConfigRule={
                "ConfigRuleName": "update-rule",
                "Source": {
                    "Owner": "AWS",
                    "SourceIdentifier": "S3_BUCKET_VERSIONING_ENABLED",
                },
                "Description": "updated",
            }
        )
        response = config.describe_config_rules(ConfigRuleNames=["update-rule"])
        assert response["ConfigRules"][0]["Description"] == "updated"
        config.delete_config_rule(ConfigRuleName="update-rule")

    def test_describe_nonexistent_rule(self, config):
        with pytest.raises(ClientError) as exc:
            config.describe_config_rules(ConfigRuleNames=["nonexistent-rule-xyz"])
        assert "NoSuchConfigRuleException" in exc.value.response["Error"]["Code"]

    def test_delete_nonexistent_rule(self, config):
        with pytest.raises(ClientError) as exc:
            config.delete_config_rule(ConfigRuleName="nonexistent-rule-xyz")
        assert "NoSuchConfigRuleException" in exc.value.response["Error"]["Code"]

    def test_put_multiple_config_rules(self, config):
        for i in range(3):
            config.put_config_rule(
                ConfigRule={
                    "ConfigRuleName": f"multi-rule-{i}",
                    "Source": {
                        "Owner": "AWS",
                        "SourceIdentifier": "S3_BUCKET_VERSIONING_ENABLED",
                    },
                }
            )
        response = config.describe_config_rules(
            ConfigRuleNames=["multi-rule-0", "multi-rule-1", "multi-rule-2"]
        )
        names = [r["ConfigRuleName"] for r in response["ConfigRules"]]
        assert "multi-rule-0" in names
        assert "multi-rule-1" in names
        assert "multi-rule-2" in names
        for i in range(3):
            config.delete_config_rule(ConfigRuleName=f"multi-rule-{i}")


class TestConfigurationRecorder:
    """Test configuration recorder operations."""

    def _make_role(self, iam, role_name):
        role = iam.create_role(
            RoleName=role_name,
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
        return role["Role"]["Arn"]

    def test_describe_configuration_recorder(self, config, iam):
        role_arn = self._make_role(iam, "config-rec-role")
        config.put_configuration_recorder(
            ConfigurationRecorder={
                "name": "default",
                "roleARN": role_arn,
                "recordingGroup": {"allSupported": True},
            }
        )
        response = config.describe_configuration_recorders(
            ConfigurationRecorderNames=["default"]
        )
        assert len(response["ConfigurationRecorders"]) == 1
        recorder = response["ConfigurationRecorders"][0]
        assert recorder["name"] == "default"
        assert recorder["roleARN"] == role_arn
        iam.delete_role(RoleName="config-rec-role")

    def test_delete_configuration_recorder(self, config, iam):
        role_arn = self._make_role(iam, "config-del-rec-role")
        config.put_configuration_recorder(
            ConfigurationRecorder={
                "name": "default",
                "roleARN": role_arn,
                "recordingGroup": {"allSupported": True},
            }
        )
        config.delete_configuration_recorder(ConfigurationRecorderName="default")
        response = config.describe_configuration_recorders()
        names = [r["name"] for r in response["ConfigurationRecorders"]]
        assert "default" not in names
        iam.delete_role(RoleName="config-del-rec-role")

    def test_recorder_with_resource_types(self, config, iam):
        role_arn = self._make_role(iam, "config-rt-role")
        config.put_configuration_recorder(
            ConfigurationRecorder={
                "name": "default",
                "roleARN": role_arn,
                "recordingGroup": {
                    "allSupported": False,
                    "resourceTypes": ["AWS::S3::Bucket", "AWS::EC2::Instance"],
                },
            }
        )
        response = config.describe_configuration_recorders(
            ConfigurationRecorderNames=["default"]
        )
        recorder = response["ConfigurationRecorders"][0]
        resource_types = recorder["recordingGroup"]["resourceTypes"]
        assert "AWS::S3::Bucket" in resource_types
        assert "AWS::EC2::Instance" in resource_types
        iam.delete_role(RoleName="config-rt-role")


class TestDeliveryChannel:
    """Test delivery channel CRUD."""

    def _setup_recorder(self, config, iam, s3):
        role = iam.create_role(
            RoleName="config-dc-role",
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
        s3.create_bucket(Bucket="config-bucket-test")

    def _cleanup(self, config, iam, s3):
        try:
            config.delete_delivery_channel(DeliveryChannelName="default")
        except ClientError:
            pass
        try:
            config.delete_configuration_recorder(ConfigurationRecorderName="default")
        except ClientError:
            pass
        try:
            iam.delete_role(RoleName="config-dc-role")
        except ClientError:
            pass
        try:
            s3.delete_bucket(Bucket="config-bucket-test")
        except ClientError:
            pass

    def test_put_delivery_channel(self, config, iam, s3):
        self._setup_recorder(config, iam, s3)
        config.put_delivery_channel(
            DeliveryChannel={
                "name": "default",
                "s3BucketName": "config-bucket-test",
            }
        )
        response = config.describe_delivery_channels()
        assert len(response["DeliveryChannels"]) >= 1
        names = [c["name"] for c in response["DeliveryChannels"]]
        assert "default" in names
        self._cleanup(config, iam, s3)

    def test_describe_delivery_channel(self, config, iam, s3):
        self._setup_recorder(config, iam, s3)
        config.put_delivery_channel(
            DeliveryChannel={
                "name": "default",
                "s3BucketName": "config-bucket-test",
            }
        )
        response = config.describe_delivery_channels(DeliveryChannelNames=["default"])
        assert len(response["DeliveryChannels"]) == 1
        channel = response["DeliveryChannels"][0]
        assert channel["name"] == "default"
        assert channel["s3BucketName"] == "config-bucket-test"
        self._cleanup(config, iam, s3)

    def test_delete_delivery_channel(self, config, iam, s3):
        self._setup_recorder(config, iam, s3)
        config.put_delivery_channel(
            DeliveryChannel={
                "name": "default",
                "s3BucketName": "config-bucket-test",
            }
        )
        config.delete_delivery_channel(DeliveryChannelName="default")
        response = config.describe_delivery_channels()
        names = [c["name"] for c in response["DeliveryChannels"]]
        assert "default" not in names
        try:
            config.delete_configuration_recorder(ConfigurationRecorderName="default")
        except ClientError:
            pass
        try:
            iam.delete_role(RoleName="config-dc-role")
        except ClientError:
            pass
        try:
            s3.delete_bucket(Bucket="config-bucket-test")
        except ClientError:
            pass


class TestConfigCompliance:
    """Test compliance-related operations."""

    def test_describe_compliance_by_config_rule(self, config):
        config.put_config_rule(
            ConfigRule={
                "ConfigRuleName": "compliance-rule",
                "Source": {
                    "Owner": "AWS",
                    "SourceIdentifier": "S3_BUCKET_VERSIONING_ENABLED",
                },
            }
        )
        response = config.describe_compliance_by_config_rule(
            ConfigRuleNames=["compliance-rule"]
        )
        assert "ComplianceByConfigRules" in response
        if response["ComplianceByConfigRules"]:
            rule = response["ComplianceByConfigRules"][0]
            assert rule["ConfigRuleName"] == "compliance-rule"
        config.delete_config_rule(ConfigRuleName="compliance-rule")

    def test_put_evaluations(self, config):
        config.put_config_rule(
            ConfigRule={
                "ConfigRuleName": "eval-rule",
                "Source": {
                    "Owner": "AWS",
                    "SourceIdentifier": "S3_BUCKET_VERSIONING_ENABLED",
                },
            }
        )
        response = config.put_evaluations(
            Evaluations=[
                {
                    "ComplianceResourceType": "AWS::S3::Bucket",
                    "ComplianceResourceId": "my-bucket",
                    "ComplianceType": "NON_COMPLIANT",
                    "OrderingTimestamp": time.time(),
                }
            ],
            ResultToken="test-token",
        )
        assert "FailedEvaluations" in response
        config.delete_config_rule(ConfigRuleName="eval-rule")

    def test_describe_config_rule_evaluation_status(self, config):
        config.put_config_rule(
            ConfigRule={
                "ConfigRuleName": "eval-status-rule",
                "Source": {
                    "Owner": "AWS",
                    "SourceIdentifier": "S3_BUCKET_VERSIONING_ENABLED",
                },
            }
        )
        response = config.describe_config_rule_evaluation_status(
            ConfigRuleNames=["eval-status-rule"]
        )
        assert "ConfigRulesEvaluationStatus" in response
        config.delete_config_rule(ConfigRuleName="eval-status-rule")
