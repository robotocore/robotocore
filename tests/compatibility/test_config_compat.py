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
        response = config.describe_configuration_recorders(ConfigurationRecorderNames=["default"])
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

    def test_start_configuration_recorder(self, config, iam, s3):
        role_arn = self._make_role(iam, "config-start-role")
        s3.create_bucket(Bucket="config-start-bucket")
        config.put_configuration_recorder(
            ConfigurationRecorder={
                "name": "default",
                "roleARN": role_arn,
                "recordingGroup": {"allSupported": True},
            }
        )
        config.put_delivery_channel(
            DeliveryChannel={"name": "default", "s3BucketName": "config-start-bucket"}
        )
        try:
            config.start_configuration_recorder(ConfigurationRecorderName="default")
            resp = config.describe_configuration_recorder_status(
                ConfigurationRecorderNames=["default"]
            )
            assert len(resp["ConfigurationRecordersStatus"]) == 1
            assert resp["ConfigurationRecordersStatus"][0]["recording"] is True
        finally:
            try:
                config.stop_configuration_recorder(ConfigurationRecorderName="default")
            except ClientError:
                pass  # best-effort cleanup
            try:
                config.delete_delivery_channel(DeliveryChannelName="default")
            except ClientError:
                pass  # best-effort cleanup
            try:
                config.delete_configuration_recorder(ConfigurationRecorderName="default")
            except ClientError:
                pass  # best-effort cleanup
            iam.delete_role(RoleName="config-start-role")
            s3.delete_bucket(Bucket="config-start-bucket")

    def test_stop_configuration_recorder(self, config, iam, s3):
        role_arn = self._make_role(iam, "config-stop-role")
        s3.create_bucket(Bucket="config-stop-bucket")
        config.put_configuration_recorder(
            ConfigurationRecorder={
                "name": "default",
                "roleARN": role_arn,
                "recordingGroup": {"allSupported": True},
            }
        )
        config.put_delivery_channel(
            DeliveryChannel={"name": "default", "s3BucketName": "config-stop-bucket"}
        )
        try:
            config.start_configuration_recorder(ConfigurationRecorderName="default")
            config.stop_configuration_recorder(ConfigurationRecorderName="default")
            resp = config.describe_configuration_recorder_status(
                ConfigurationRecorderNames=["default"]
            )
            assert len(resp["ConfigurationRecordersStatus"]) == 1
            assert resp["ConfigurationRecordersStatus"][0]["recording"] is False
        finally:
            try:
                config.delete_delivery_channel(DeliveryChannelName="default")
            except ClientError:
                pass  # best-effort cleanup
            try:
                config.delete_configuration_recorder(ConfigurationRecorderName="default")
            except ClientError:
                pass  # best-effort cleanup
            iam.delete_role(RoleName="config-stop-role")
            s3.delete_bucket(Bucket="config-stop-bucket")

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
        response = config.describe_configuration_recorders(ConfigurationRecorderNames=["default"])
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
            pass  # best-effort cleanup
        try:
            config.delete_configuration_recorder(ConfigurationRecorderName="default")
        except ClientError:
            pass  # best-effort cleanup
        try:
            iam.delete_role(RoleName="config-dc-role")
        except ClientError:
            pass  # best-effort cleanup
        try:
            s3.delete_bucket(Bucket="config-bucket-test")
        except ClientError:
            pass  # best-effort cleanup

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
            pass  # best-effort cleanup
        try:
            iam.delete_role(RoleName="config-dc-role")
        except ClientError:
            pass  # best-effort cleanup
        try:
            s3.delete_bucket(Bucket="config-bucket-test")
        except ClientError:
            pass  # best-effort cleanup


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
        response = config.describe_compliance_by_config_rule(ConfigRuleNames=["compliance-rule"])
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


class TestConfigExtended:
    def test_put_config_rule_custom_lambda(self, config):
        name = "custom-lambda-rule"
        config.put_config_rule(
            ConfigRule={
                "ConfigRuleName": name,
                "Source": {
                    "Owner": "CUSTOM_LAMBDA",
                    "SourceIdentifier": "arn:aws:lambda:us-east-1:123456789012:function:my-rule",
                    "SourceDetails": [
                        {
                            "EventSource": "aws.config",
                            "MessageType": "ConfigurationItemChangeNotification",
                        }
                    ],
                },
            }
        )
        resp = config.describe_config_rules(ConfigRuleNames=[name])
        assert resp["ConfigRules"][0]["ConfigRuleName"] == name
        assert resp["ConfigRules"][0]["Source"]["Owner"] == "CUSTOM_LAMBDA"
        config.delete_config_rule(ConfigRuleName=name)

    def test_put_config_rule_maximum_frequency(self, config):
        name = "freq-rule"
        config.put_config_rule(
            ConfigRule={
                "ConfigRuleName": name,
                "Source": {
                    "Owner": "AWS",
                    "SourceIdentifier": "S3_BUCKET_VERSIONING_ENABLED",
                },
                "MaximumExecutionFrequency": "Six_Hours",
            }
        )
        resp = config.describe_config_rules(ConfigRuleNames=[name])
        assert resp["ConfigRules"][0]["MaximumExecutionFrequency"] == "Six_Hours"
        config.delete_config_rule(ConfigRuleName=name)

    def test_put_config_rule_with_description(self, config):
        name = "desc-rule"
        config.put_config_rule(
            ConfigRule={
                "ConfigRuleName": name,
                "Description": "A test description",
                "Source": {
                    "Owner": "AWS",
                    "SourceIdentifier": "S3_BUCKET_VERSIONING_ENABLED",
                },
            }
        )
        resp = config.describe_config_rules(ConfigRuleNames=[name])
        assert resp["ConfigRules"][0].get("Description") == "A test description"
        config.delete_config_rule(ConfigRuleName=name)

    def test_describe_config_rules_all(self, config):
        names = ["all-rule-1", "all-rule-2"]
        for n in names:
            config.put_config_rule(
                ConfigRule={
                    "ConfigRuleName": n,
                    "Source": {
                        "Owner": "AWS",
                        "SourceIdentifier": "S3_BUCKET_VERSIONING_ENABLED",
                    },
                }
            )
        try:
            resp = config.describe_config_rules()
            found = [r["ConfigRuleName"] for r in resp["ConfigRules"]]
            for n in names:
                assert n in found
        finally:
            for n in names:
                config.delete_config_rule(ConfigRuleName=n)

    def test_config_rule_has_arn(self, config):
        name = "arn-rule"
        config.put_config_rule(
            ConfigRule={
                "ConfigRuleName": name,
                "Source": {
                    "Owner": "AWS",
                    "SourceIdentifier": "S3_BUCKET_VERSIONING_ENABLED",
                },
            }
        )
        resp = config.describe_config_rules(ConfigRuleNames=[name])
        assert "ConfigRuleArn" in resp["ConfigRules"][0]
        assert "config-rule" in resp["ConfigRules"][0]["ConfigRuleArn"]
        config.delete_config_rule(ConfigRuleName=name)

    def test_config_rule_has_id(self, config):
        name = "id-rule"
        config.put_config_rule(
            ConfigRule={
                "ConfigRuleName": name,
                "Source": {
                    "Owner": "AWS",
                    "SourceIdentifier": "S3_BUCKET_VERSIONING_ENABLED",
                },
            }
        )
        resp = config.describe_config_rules(ConfigRuleNames=[name])
        assert "ConfigRuleId" in resp["ConfigRules"][0]
        config.delete_config_rule(ConfigRuleName=name)

    def test_put_aggregation_authorization(self, config):
        resp = config.put_aggregation_authorization(
            AuthorizedAccountId="123456789012",
            AuthorizedAwsRegion="us-east-1",
        )
        assert "AggregationAuthorization" in resp
        config.delete_aggregation_authorization(
            AuthorizedAccountId="123456789012",
            AuthorizedAwsRegion="us-east-1",
        )

    def test_describe_aggregation_authorizations(self, config):
        config.put_aggregation_authorization(
            AuthorizedAccountId="123456789012",
            AuthorizedAwsRegion="us-west-2",
        )
        try:
            resp = config.describe_aggregation_authorizations()
            assert "AggregationAuthorizations" in resp
            accounts = [a["AuthorizedAccountId"] for a in resp["AggregationAuthorizations"]]
            assert "123456789012" in accounts
        finally:
            config.delete_aggregation_authorization(
                AuthorizedAccountId="123456789012",
                AuthorizedAwsRegion="us-west-2",
            )

    def test_put_configuration_aggregator(self, config):
        name = "test-aggregator"
        resp = config.put_configuration_aggregator(
            ConfigurationAggregatorName=name,
            AccountAggregationSources=[
                {
                    "AccountIds": ["123456789012"],
                    "AllAwsRegions": True,
                }
            ],
        )
        assert "ConfigurationAggregator" in resp
        config.delete_configuration_aggregator(ConfigurationAggregatorName=name)

    def test_describe_configuration_aggregators(self, config):
        name = "desc-agg"
        config.put_configuration_aggregator(
            ConfigurationAggregatorName=name,
            AccountAggregationSources=[{"AccountIds": ["123456789012"], "AllAwsRegions": True}],
        )
        try:
            resp = config.describe_configuration_aggregators(ConfigurationAggregatorNames=[name])
            assert len(resp["ConfigurationAggregators"]) == 1
            assert resp["ConfigurationAggregators"][0]["ConfigurationAggregatorName"] == name
        finally:
            config.delete_configuration_aggregator(ConfigurationAggregatorName=name)

    def test_put_retention_configuration(self, config):
        resp = config.put_retention_configuration(RetentionPeriodInDays=365)
        assert "RetentionConfiguration" in resp

    def test_describe_compliance_by_resource(self, config):
        resp = config.describe_compliance_by_resource(ResourceType="AWS::S3::Bucket")
        assert "ComplianceByResources" in resp

    def test_get_compliance_details_by_config_rule(self, config):
        name = "comp-detail-rule"
        config.put_config_rule(
            ConfigRule={
                "ConfigRuleName": name,
                "Source": {
                    "Owner": "AWS",
                    "SourceIdentifier": "S3_BUCKET_VERSIONING_ENABLED",
                },
            }
        )
        try:
            resp = config.get_compliance_details_by_config_rule(ConfigRuleName=name)
            assert "EvaluationResults" in resp
        finally:
            config.delete_config_rule(ConfigRuleName=name)

    def test_tag_and_untag_resource(self, config):
        name = "tag-rule"
        config.put_config_rule(
            ConfigRule={
                "ConfigRuleName": name,
                "Source": {
                    "Owner": "AWS",
                    "SourceIdentifier": "S3_BUCKET_VERSIONING_ENABLED",
                },
            }
        )
        resp = config.describe_config_rules(ConfigRuleNames=[name])
        arn = resp["ConfigRules"][0]["ConfigRuleArn"]
        try:
            config.tag_resource(ResourceArn=arn, Tags=[{"Key": "env", "Value": "test"}])
            tags = config.list_tags_for_resource(ResourceArn=arn)
            tag_map = {t["Key"]: t["Value"] for t in tags.get("Tags", [])}
            assert tag_map.get("env") == "test"
            config.untag_resource(ResourceArn=arn, TagKeys=["env"])
            tags2 = config.list_tags_for_resource(ResourceArn=arn)
            tag_map2 = {t["Key"]: t["Value"] for t in tags2.get("Tags", [])}
            assert "env" not in tag_map2
        finally:
            config.delete_config_rule(ConfigRuleName=name)

    def test_describe_configuration_recorder_status(self, config, iam):
        role = iam.create_role(
            RoleName="config-status-role",
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
        try:
            resp = config.describe_configuration_recorder_status()
            assert "ConfigurationRecordersStatus" in resp
        finally:
            try:
                config.delete_configuration_recorder(ConfigurationRecorderName="default")
            except ClientError:
                pass  # best-effort cleanup
            iam.delete_role(RoleName="config-status-role")

    def test_describe_retention_configurations(self, config):
        config.put_retention_configuration(RetentionPeriodInDays=365)
        resp = config.describe_retention_configurations()
        assert "RetentionConfigurations" in resp
        assert len(resp["RetentionConfigurations"]) >= 1

    def test_list_discovered_resources(self, config, s3):
        bucket_name = "config-disc-res-test"
        s3.create_bucket(Bucket=bucket_name)
        try:
            resp = config.list_discovered_resources(resourceType="AWS::S3::Bucket")
            assert "resourceIdentifiers" in resp
        finally:
            s3.delete_bucket(Bucket=bucket_name)

    def test_get_resource_config_history(self, config, s3):
        bucket_name = "config-hist-test"
        s3.create_bucket(Bucket=bucket_name)
        try:
            resp = config.get_resource_config_history(
                resourceType="AWS::S3::Bucket",
                resourceId=bucket_name,
            )
            assert "configurationItems" in resp
        finally:
            s3.delete_bucket(Bucket=bucket_name)


class TestConfigGapStubs:
    """Tests for newly-stubbed Config operations that return empty results."""

    def test_describe_conformance_packs(self, config):
        resp = config.describe_conformance_packs()
        assert "ConformancePackDetails" in resp

    def test_describe_conformance_pack_status(self, config):
        resp = config.describe_conformance_pack_status()
        assert "ConformancePackStatusDetails" in resp

    def test_describe_organization_config_rules(self, config):
        resp = config.describe_organization_config_rules()
        assert "OrganizationConfigRules" in resp

    def test_describe_organization_conformance_packs(self, config):
        resp = config.describe_organization_conformance_packs()
        assert "OrganizationConformancePacks" in resp

    def test_describe_organization_conformance_pack_statuses(self, config):
        resp = config.describe_organization_conformance_pack_statuses()
        assert "OrganizationConformancePackStatuses" in resp

    def test_describe_pending_aggregation_requests(self, config):
        resp = config.describe_pending_aggregation_requests()
        assert "PendingAggregationRequests" in resp

    def test_describe_retention_configurations(self, config):
        resp = config.describe_retention_configurations()
        assert "RetentionConfigurations" in resp

    def test_get_compliance_details_by_config_rule_stub(self, config):
        resp = config.get_compliance_details_by_config_rule(ConfigRuleName="dummy")
        assert "EvaluationResults" in resp

    def test_get_compliance_details_by_resource_stub(self, config):
        resp = config.get_compliance_details_by_resource(
            ResourceType="AWS::S3::Bucket", ResourceId="dummy"
        )
        assert "EvaluationResults" in resp

    def test_list_conformance_pack_compliance_scores(self, config):
        resp = config.list_conformance_pack_compliance_scores()
        assert "ConformancePackComplianceScores" in resp

    def test_list_stored_queries(self, config):
        resp = config.list_stored_queries()
        assert "StoredQueryMetadata" in resp


class TestConfigAutoCoverage:
    """Auto-generated coverage tests for config."""

    @pytest.fixture
    def client(self):
        return make_client("config")

    def test_describe_delivery_channel_status(self, client):
        """DescribeDeliveryChannelStatus returns a response."""
        resp = client.describe_delivery_channel_status()
        assert "DeliveryChannelsStatus" in resp

    def test_describe_organization_config_rule_statuses(self, client):
        """DescribeOrganizationConfigRuleStatuses returns a response."""
        resp = client.describe_organization_config_rule_statuses()
        assert "OrganizationConfigRuleStatuses" in resp

    def test_get_compliance_summary_by_config_rule(self, client):
        """GetComplianceSummaryByConfigRule returns a response."""
        resp = client.get_compliance_summary_by_config_rule()
        assert "ComplianceSummary" in resp

    def test_get_compliance_summary_by_resource_type(self, client):
        """GetComplianceSummaryByResourceType returns a response."""
        resp = client.get_compliance_summary_by_resource_type()
        assert "ComplianceSummariesByResourceType" in resp

    def test_get_custom_rule_policy(self, client):
        """GetCustomRulePolicy returns a response for a given ConfigRuleName."""
        resp = client.get_custom_rule_policy(ConfigRuleName="nonexistent-rule")
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200

    def test_get_discovered_resource_counts(self, client):
        """GetDiscoveredResourceCounts returns a response."""
        resp = client.get_discovered_resource_counts()
        assert "totalDiscoveredResources" in resp

    def test_list_configuration_recorders(self, client):
        """ListConfigurationRecorders returns a response."""
        resp = client.list_configuration_recorders()
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200

    def test_list_resource_evaluations(self, client):
        """ListResourceEvaluations returns a response."""
        resp = client.list_resource_evaluations()
        assert "ResourceEvaluations" in resp

    def test_start_config_rules_evaluation(self, client):
        """StartConfigRulesEvaluation returns a response."""
        resp = client.start_config_rules_evaluation()
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200


class TestConfigBatchResourceConfig:
    """Tests for batch resource config operations."""

    @pytest.fixture
    def client(self):
        return make_client("config")

    def test_batch_get_resource_config(self, client):
        """BatchGetResourceConfig returns results for known resource keys."""
        resp = client.batch_get_resource_config(
            resourceKeys=[
                {
                    "resourceType": "AWS::S3::Bucket",
                    "resourceId": "nonexistent-bucket-xyz",
                }
            ]
        )
        assert "baseConfigurationItems" in resp
        assert "unprocessedResourceKeys" in resp

    def test_batch_get_aggregate_resource_config(self, client):
        """BatchGetAggregateResourceConfig returns results."""
        agg_name = "batch-agg-test"
        client.put_configuration_aggregator(
            ConfigurationAggregatorName=agg_name,
            AccountAggregationSources=[{"AccountIds": ["123456789012"], "AllAwsRegions": True}],
        )
        try:
            resp = client.batch_get_aggregate_resource_config(
                ConfigurationAggregatorName=agg_name,
                ResourceIdentifiers=[
                    {
                        "SourceAccountId": "123456789012",
                        "SourceRegion": "us-east-1",
                        "ResourceId": "nonexistent-bucket",
                        "ResourceType": "AWS::S3::Bucket",
                    }
                ],
            )
            assert "BaseConfigurationItems" in resp
            assert "UnprocessedResourceIdentifiers" in resp
        finally:
            client.delete_configuration_aggregator(ConfigurationAggregatorName=agg_name)


class TestOrganizationConformancePack:
    """Test OrganizationConformancePack operations."""

    @pytest.fixture
    def client(self):
        return make_client("config")

    def test_delete_organization_conformance_pack_nonexistent(self, client):
        """DeleteOrganizationConformancePack for nonexistent raises error."""
        with pytest.raises(ClientError) as exc:
            client.delete_organization_conformance_pack(
                OrganizationConformancePackName="nonexistent-pack-xyz",
            )
        assert "NoSuchOrganizationConformancePackException" in exc.value.response["Error"]["Code"]

    def test_put_organization_conformance_pack(self, client):
        """PutOrganizationConformancePack creates a pack and returns an ARN."""
        import uuid

        name = f"org-cp-{uuid.uuid4().hex[:8]}"
        try:
            resp = client.put_organization_conformance_pack(
                OrganizationConformancePackName=name,
                TemplateS3Uri="s3://fake-bucket/template.yaml",
            )
            assert "OrganizationConformancePackArn" in resp
            assert name in resp["OrganizationConformancePackArn"]
            # Verify it appears in describe
            desc = client.describe_organization_conformance_packs(
                OrganizationConformancePackNames=[name]
            )
            assert len(desc["OrganizationConformancePacks"]) == 1
            pack = desc["OrganizationConformancePacks"][0]
            assert pack["OrganizationConformancePackName"] == name
        finally:
            client.delete_organization_conformance_pack(OrganizationConformancePackName=name)


class TestRetentionConfiguration:
    """Test RetentionConfiguration operations."""

    @pytest.fixture
    def client(self):
        return make_client("config")

    def test_delete_retention_configuration(self, client):
        """DeleteRetentionConfiguration removes the retention config."""
        put_resp = client.put_retention_configuration(RetentionPeriodInDays=365)
        ret_name = put_resp["RetentionConfiguration"]["Name"]
        client.delete_retention_configuration(RetentionConfigurationName=ret_name)
        resp = client.describe_retention_configurations()
        names = [r["Name"] for r in resp["RetentionConfigurations"]]
        assert ret_name not in names


class TestSelectResourceConfig:
    """Test SelectResourceConfig operation."""

    @pytest.fixture
    def client(self):
        return make_client("config")

    def test_select_resource_config(self, client):
        """SelectResourceConfig executes a query."""
        resp = client.select_resource_config(
            Expression="SELECT resourceId WHERE resourceType = 'AWS::S3::Bucket'",
        )
        assert "Results" in resp


class TestAggregateDiscoveredResources:
    """Test ListAggregateDiscoveredResources operation."""

    @pytest.fixture
    def client(self):
        return make_client("config")

    def test_list_aggregate_discovered_resources(self, client):
        """ListAggregateDiscoveredResources returns resources."""
        agg_name = "test-agg-disc-res"
        client.put_configuration_aggregator(
            ConfigurationAggregatorName=agg_name,
            AccountAggregationSources=[{"AccountIds": ["123456789012"], "AllAwsRegions": True}],
        )
        try:
            resp = client.list_aggregate_discovered_resources(
                ConfigurationAggregatorName=agg_name,
                ResourceType="AWS::S3::Bucket",
            )
            assert "ResourceIdentifiers" in resp
        finally:
            client.delete_configuration_aggregator(ConfigurationAggregatorName=agg_name)


class TestResourceConfig:
    """Test ResourceConfig operations (PutResourceConfig, DeleteResourceConfig)."""

    @pytest.fixture
    def client(self):
        return make_client("config")

    def test_put_resource_config(self, client):
        """PutResourceConfig records a third-party resource."""
        resp = client.put_resource_config(
            ResourceType="MyCustom::Resource::Type",
            SchemaVersionId="1",
            ResourceId="res-001",
            Configuration=json.dumps({"key": "value"}),
        )
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200

    def test_delete_resource_config(self, client):
        """DeleteResourceConfig removes a recorded third-party resource."""
        client.put_resource_config(
            ResourceType="MyCustom::Resource::Type",
            SchemaVersionId="1",
            ResourceId="res-del-001",
            Configuration=json.dumps({"key": "value"}),
        )
        resp = client.delete_resource_config(
            ResourceType="MyCustom::Resource::Type",
            ResourceId="res-del-001",
        )
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200


class TestOrganizationConformancePackDetailedStatus:
    """Test GetOrganizationConformancePackDetailedStatus."""

    @pytest.fixture
    def client(self):
        return make_client("config")

    def test_get_organization_conformance_pack_detailed_status_nonexistent(self, client):
        """GetOrganizationConformancePackDetailedStatus for nonexistent raises error."""
        with pytest.raises(ClientError) as exc:
            client.get_organization_conformance_pack_detailed_status(
                OrganizationConformancePackName="nonexistent-pack-xyz",
            )
        assert "NoSuchOrganizationConformancePackException" in exc.value.response["Error"]["Code"]


class TestConformancePackCRUD:
    """Test ConformancePack create/describe/delete lifecycle."""

    @pytest.fixture
    def client(self):
        return make_client("config")

    def test_put_conformance_pack(self, client):
        """PutConformancePack creates a conformance pack and returns an ARN."""
        import uuid

        name = f"cp-put-{uuid.uuid4().hex[:8]}"
        try:
            resp = client.put_conformance_pack(
                ConformancePackName=name,
                TemplateBody='AWSTemplateFormatVersion: "2010-09-09"\nResources: []',
            )
            assert "ConformancePackArn" in resp
            assert name in resp["ConformancePackArn"]
        finally:
            client.delete_conformance_pack(ConformancePackName=name)

    def test_describe_conformance_packs_by_name(self, client):
        """DescribeConformancePacks returns details for a named pack."""
        import uuid

        name = f"cp-desc-{uuid.uuid4().hex[:8]}"
        client.put_conformance_pack(
            ConformancePackName=name,
            TemplateBody='AWSTemplateFormatVersion: "2010-09-09"\nResources: []',
        )
        try:
            resp = client.describe_conformance_packs(ConformancePackNames=[name])
            assert len(resp["ConformancePackDetails"]) == 1
            assert resp["ConformancePackDetails"][0]["ConformancePackName"] == name
            assert "ConformancePackArn" in resp["ConformancePackDetails"][0]
        finally:
            client.delete_conformance_pack(ConformancePackName=name)

    def test_delete_conformance_pack(self, client):
        """DeleteConformancePack removes the pack."""
        import uuid

        name = f"cp-del-{uuid.uuid4().hex[:8]}"
        client.put_conformance_pack(
            ConformancePackName=name,
            TemplateBody='AWSTemplateFormatVersion: "2010-09-09"\nResources: []',
        )
        client.delete_conformance_pack(ConformancePackName=name)
        resp = client.describe_conformance_packs()
        names = [p["ConformancePackName"] for p in resp["ConformancePackDetails"]]
        assert name not in names


class TestStoredQueryCRUD:
    """Test StoredQuery create/get/list/delete lifecycle."""

    @pytest.fixture
    def client(self):
        return make_client("config")

    def test_put_stored_query(self, client):
        """PutStoredQuery creates a query and returns an ARN."""
        import uuid

        name = f"sq-put-{uuid.uuid4().hex[:8]}"
        try:
            resp = client.put_stored_query(
                StoredQuery={
                    "QueryName": name,
                    "Expression": "SELECT resourceId WHERE resourceType = 'AWS::S3::Bucket'",
                },
            )
            assert "QueryArn" in resp
            assert name in resp["QueryArn"]
        finally:
            client.delete_stored_query(QueryName=name)

    def test_get_stored_query(self, client):
        """GetStoredQuery returns the query details."""
        import uuid

        name = f"sq-get-{uuid.uuid4().hex[:8]}"
        client.put_stored_query(
            StoredQuery={
                "QueryName": name,
                "Expression": "SELECT resourceId WHERE resourceType = 'AWS::EC2::Instance'",
            },
        )
        try:
            resp = client.get_stored_query(QueryName=name)
            assert resp["StoredQuery"]["QueryName"] == name
            assert "Expression" in resp["StoredQuery"]
        finally:
            client.delete_stored_query(QueryName=name)

    def test_list_stored_queries_includes_created(self, client):
        """ListStoredQueries includes a created query."""
        import uuid

        name = f"sq-list-{uuid.uuid4().hex[:8]}"
        client.put_stored_query(
            StoredQuery={
                "QueryName": name,
                "Expression": "SELECT resourceId",
            },
        )
        try:
            resp = client.list_stored_queries()
            query_names = [q["QueryName"] for q in resp["StoredQueryMetadata"]]
            assert name in query_names
        finally:
            client.delete_stored_query(QueryName=name)

    def test_delete_stored_query(self, client):
        """DeleteStoredQuery removes the query."""
        import uuid

        name = f"sq-del-{uuid.uuid4().hex[:8]}"
        client.put_stored_query(
            StoredQuery={
                "QueryName": name,
                "Expression": "SELECT resourceId",
            },
        )
        client.delete_stored_query(QueryName=name)
        resp = client.list_stored_queries()
        query_names = [q["QueryName"] for q in resp["StoredQueryMetadata"]]
        assert name not in query_names


class TestConfigAggregationOperations:
    """Tests for aggregation-related operations."""

    def test_delete_aggregation_authorization(self, config):
        """DeleteAggregationAuthorization removes the authorization."""
        config.put_aggregation_authorization(
            AuthorizedAccountId="123456789012",
            AuthorizedAwsRegion="us-east-1",
        )
        config.delete_aggregation_authorization(
            AuthorizedAccountId="123456789012",
            AuthorizedAwsRegion="us-east-1",
        )
        resp = config.describe_aggregation_authorizations()
        accts = [a["AuthorizedAccountId"] for a in resp.get("AggregationAuthorizations", [])]
        assert "123456789012" not in accts or len(resp.get("AggregationAuthorizations", [])) == 0

    def test_delete_configuration_aggregator(self, config):
        """DeleteConfigurationAggregator removes the aggregator."""
        import uuid

        name = f"agg-{uuid.uuid4().hex[:8]}"
        config.put_configuration_aggregator(
            ConfigurationAggregatorName=name,
            AccountAggregationSources=[
                {
                    "AccountIds": ["123456789012"],
                    "AllAwsRegions": True,
                }
            ],
        )
        config.delete_configuration_aggregator(ConfigurationAggregatorName=name)
        resp = config.describe_configuration_aggregators()
        names = [a["ConfigurationAggregatorName"] for a in resp["ConfigurationAggregators"]]
        assert name not in names

    def test_describe_configuration_recorders_all(self, config):
        """DescribeConfigurationRecorders returns all recorders."""
        resp = config.describe_configuration_recorders()
        assert "ConfigurationRecorders" in resp
        assert isinstance(resp["ConfigurationRecorders"], list)

    def test_describe_delivery_channels_all(self, config):
        """DescribeDeliveryChannels returns all channels."""
        resp = config.describe_delivery_channels()
        assert "DeliveryChannels" in resp
        assert isinstance(resp["DeliveryChannels"], list)

    def test_list_tags_for_resource(self, config):
        """ListTagsForResource returns Tags list for a config rule."""
        import uuid

        rule_name = f"tag-rule-{uuid.uuid4().hex[:8]}"
        config.put_config_rule(
            ConfigRule={
                "ConfigRuleName": rule_name,
                "Source": {
                    "Owner": "AWS",
                    "SourceIdentifier": "S3_BUCKET_VERSIONING_ENABLED",
                },
            },
            Tags=[{"Key": "env", "Value": "test"}],
        )
        try:
            # Get the rule ARN
            rules = config.describe_config_rules(ConfigRuleNames=[rule_name])
            arn = rules["ConfigRules"][0]["ConfigRuleArn"]
            resp = config.list_tags_for_resource(ResourceArn=arn)
            assert "Tags" in resp
            assert isinstance(resp["Tags"], list)
        finally:
            config.delete_config_rule(ConfigRuleName=rule_name)

    def test_get_compliance_details_by_resource(self, config):
        """GetComplianceDetailsByResource returns EvaluationResults."""
        resp = config.get_compliance_details_by_resource(
            ResourceType="AWS::S3::Bucket",
            ResourceId="nonexistent-bucket",
        )
        assert "EvaluationResults" in resp
        assert isinstance(resp["EvaluationResults"], list)

    def test_batch_get_resource_config(self, config):
        """BatchGetResourceConfig returns results."""
        resp = config.batch_get_resource_config(
            resourceKeys=[
                {
                    "resourceType": "AWS::S3::Bucket",
                    "resourceId": "nonexistent",
                }
            ]
        )
        assert "baseConfigurationItems" in resp
        assert "unprocessedResourceKeys" in resp

    def test_describe_conformance_pack_status_all(self, config):
        """DescribeConformancePackStatus returns list."""
        resp = config.describe_conformance_pack_status()
        assert "ConformancePackStatusDetails" in resp
        assert isinstance(resp["ConformancePackStatusDetails"], list)

    def test_get_resource_config_history_not_discovered(self, config):
        """GetResourceConfigHistory for unknown resource raises error."""
        with pytest.raises(ClientError) as exc:
            config.get_resource_config_history(
                resourceType="AWS::S3::Bucket",
                resourceId="nonexistent-bucket-123",
            )
        assert exc.value.response["Error"]["Code"] == "ResourceNotDiscoveredException"

    def test_list_resource_evaluations_empty(self, config):
        """ListResourceEvaluations returns ResourceEvaluations."""
        resp = config.list_resource_evaluations()
        assert "ResourceEvaluations" in resp
        assert isinstance(resp["ResourceEvaluations"], list)

    def test_put_resource_config_and_delete(self, config):
        """PutResourceConfig creates a resource, DeleteResourceConfig removes it."""
        import uuid

        rid = f"res-{uuid.uuid4().hex[:8]}"
        config.put_resource_config(
            ResourceType="AWS::S3::Bucket",
            SchemaVersionId="1",
            ResourceId=rid,
            Configuration='{"bucketName":"test"}',
        )
        # Verify it's discoverable
        resp = config.list_discovered_resources(resourceType="AWS::S3::Bucket")
        assert "resourceIdentifiers" in resp

        config.delete_resource_config(
            ResourceType="AWS::S3::Bucket",
            ResourceId=rid,
        )

    def test_select_resource_config(self, config):
        """SelectResourceConfig with query returns results."""
        resp = config.select_resource_config(
            Expression="SELECT resourceId WHERE resourceType = 'AWS::S3::Bucket'"
        )
        assert "Results" in resp

    def test_put_retention_configuration(self, config):
        """PutRetentionConfiguration creates a configuration."""

        resp = config.put_retention_configuration(RetentionPeriodInDays=365)
        assert "RetentionConfiguration" in resp
        assert resp["RetentionConfiguration"]["RetentionPeriodInDays"] == 365
        # Clean up
        name = resp["RetentionConfiguration"]["Name"]
        config.delete_retention_configuration(RetentionConfigurationName=name)

    def test_describe_retention_configurations_all(self, config):
        """DescribeRetentionConfigurations returns list."""
        resp = config.describe_retention_configurations()
        assert "RetentionConfigurations" in resp
        assert isinstance(resp["RetentionConfigurations"], list)


class TestOrganizationConfigRuleCRUD:
    """Test OrganizationConfigRule create/describe/delete lifecycle."""

    @pytest.fixture
    def client(self):
        return make_client("config")

    def test_put_organization_config_rule(self, client):
        """PutOrganizationConfigRule creates a rule and returns an ARN."""
        import uuid

        name = f"org-rule-{uuid.uuid4().hex[:8]}"
        try:
            resp = client.put_organization_config_rule(
                OrganizationConfigRuleName=name,
                OrganizationManagedRuleMetadata={
                    "RuleIdentifier": "S3_BUCKET_VERSIONING_ENABLED",
                },
            )
            assert "OrganizationConfigRuleArn" in resp
            assert resp["OrganizationConfigRuleArn"].startswith("arn:aws:config:")
        finally:
            client.delete_organization_config_rule(OrganizationConfigRuleName=name)

    def test_describe_organization_config_rules_by_name(self, client):
        """DescribeOrganizationConfigRules returns details for a named rule."""
        import uuid

        name = f"org-rule-{uuid.uuid4().hex[:8]}"
        client.put_organization_config_rule(
            OrganizationConfigRuleName=name,
            OrganizationManagedRuleMetadata={
                "RuleIdentifier": "S3_BUCKET_VERSIONING_ENABLED",
            },
        )
        try:
            resp = client.describe_organization_config_rules(OrganizationConfigRuleNames=[name])
            assert len(resp["OrganizationConfigRules"]) == 1
            rule = resp["OrganizationConfigRules"][0]
            assert rule["OrganizationConfigRuleName"] == name
            assert "OrganizationConfigRuleArn" in rule
        finally:
            client.delete_organization_config_rule(OrganizationConfigRuleName=name)

    def test_describe_organization_config_rule_statuses_by_name(self, client):
        """DescribeOrganizationConfigRuleStatuses returns status for a named rule."""
        import uuid

        name = f"org-rule-{uuid.uuid4().hex[:8]}"
        client.put_organization_config_rule(
            OrganizationConfigRuleName=name,
            OrganizationManagedRuleMetadata={
                "RuleIdentifier": "S3_BUCKET_VERSIONING_ENABLED",
            },
        )
        try:
            resp = client.describe_organization_config_rule_statuses(
                OrganizationConfigRuleNames=[name]
            )
            assert "OrganizationConfigRuleStatuses" in resp
            assert len(resp["OrganizationConfigRuleStatuses"]) >= 1
        finally:
            client.delete_organization_config_rule(OrganizationConfigRuleName=name)

    def test_delete_organization_config_rule(self, client):
        """DeleteOrganizationConfigRule removes the rule."""
        import uuid

        name = f"org-rule-{uuid.uuid4().hex[:8]}"
        client.put_organization_config_rule(
            OrganizationConfigRuleName=name,
            OrganizationManagedRuleMetadata={
                "RuleIdentifier": "S3_BUCKET_VERSIONING_ENABLED",
            },
        )
        client.delete_organization_config_rule(OrganizationConfigRuleName=name)
        resp = client.describe_organization_config_rules()
        names = [r["OrganizationConfigRuleName"] for r in resp["OrganizationConfigRules"]]
        assert name not in names

    def test_delete_organization_config_rule_nonexistent(self, client):
        """DeleteOrganizationConfigRule for nonexistent raises error."""
        with pytest.raises(ClientError) as exc:
            client.delete_organization_config_rule(
                OrganizationConfigRuleName="nonexistent-org-rule-xyz",
            )
        assert "NoSuchOrganizationConfigRuleException" in exc.value.response["Error"]["Code"]


class TestRemediationConfigurationCRUD:
    """Test RemediationConfiguration create/describe/delete lifecycle."""

    @pytest.fixture
    def client(self):
        return make_client("config")

    def _create_rule(self, client):
        import uuid

        name = f"rem-rule-{uuid.uuid4().hex[:8]}"
        client.put_config_rule(
            ConfigRule={
                "ConfigRuleName": name,
                "Source": {
                    "Owner": "AWS",
                    "SourceIdentifier": "S3_BUCKET_VERSIONING_ENABLED",
                },
            }
        )
        return name

    def test_put_remediation_configurations(self, client):
        """PutRemediationConfigurations creates a remediation config."""
        rule_name = self._create_rule(client)
        try:
            resp = client.put_remediation_configurations(
                RemediationConfigurations=[
                    {
                        "ConfigRuleName": rule_name,
                        "TargetType": "SSM_DOCUMENT",
                        "TargetId": "AWS-EnableS3BucketEncryption",
                    }
                ]
            )
            assert "FailedBatches" in resp
            assert isinstance(resp["FailedBatches"], list)
        finally:
            client.delete_remediation_configuration(ConfigRuleName=rule_name)
            client.delete_config_rule(ConfigRuleName=rule_name)

    def test_describe_remediation_configurations(self, client):
        """DescribeRemediationConfigurations returns config for a rule."""
        rule_name = self._create_rule(client)
        client.put_remediation_configurations(
            RemediationConfigurations=[
                {
                    "ConfigRuleName": rule_name,
                    "TargetType": "SSM_DOCUMENT",
                    "TargetId": "AWS-EnableS3BucketEncryption",
                }
            ]
        )
        try:
            resp = client.describe_remediation_configurations(ConfigRuleNames=[rule_name])
            assert len(resp["RemediationConfigurations"]) == 1
            config = resp["RemediationConfigurations"][0]
            assert config["ConfigRuleName"] == rule_name
            assert config["TargetType"] == "SSM_DOCUMENT"
            assert config["TargetId"] == "AWS-EnableS3BucketEncryption"
        finally:
            client.delete_remediation_configuration(ConfigRuleName=rule_name)
            client.delete_config_rule(ConfigRuleName=rule_name)

    def test_delete_remediation_configuration(self, client):
        """DeleteRemediationConfiguration removes the config."""
        rule_name = self._create_rule(client)
        client.put_remediation_configurations(
            RemediationConfigurations=[
                {
                    "ConfigRuleName": rule_name,
                    "TargetType": "SSM_DOCUMENT",
                    "TargetId": "AWS-EnableS3BucketEncryption",
                }
            ]
        )
        client.delete_remediation_configuration(ConfigRuleName=rule_name)
        resp = client.describe_remediation_configurations(ConfigRuleNames=[rule_name])
        assert len(resp["RemediationConfigurations"]) == 0
        client.delete_config_rule(ConfigRuleName=rule_name)


class TestAggregateComplianceOperations:
    """Test aggregate compliance operations requiring a configuration aggregator."""

    @pytest.fixture
    def client(self):
        return make_client("config")

    @pytest.fixture
    def aggregator(self, client):
        import uuid

        name = f"test-agg-{uuid.uuid4().hex[:8]}"
        client.put_configuration_aggregator(
            ConfigurationAggregatorName=name,
            AccountAggregationSources=[
                {
                    "AccountIds": ["123456789012"],
                    "AllAwsRegions": True,
                }
            ],
        )
        yield name
        client.delete_configuration_aggregator(ConfigurationAggregatorName=name)

    def test_describe_aggregate_compliance_by_config_rules(self, client, aggregator):
        """DescribeAggregateComplianceByConfigRules returns compliance list."""
        resp = client.describe_aggregate_compliance_by_config_rules(
            ConfigurationAggregatorName=aggregator
        )
        assert "AggregateComplianceByConfigRules" in resp
        assert isinstance(resp["AggregateComplianceByConfigRules"], list)

    def test_describe_aggregate_compliance_by_conformance_packs(self, client, aggregator):
        """DescribeAggregateComplianceByConformancePacks returns compliance list."""
        resp = client.describe_aggregate_compliance_by_conformance_packs(
            ConfigurationAggregatorName=aggregator
        )
        assert "AggregateComplianceByConformancePacks" in resp
        assert isinstance(resp["AggregateComplianceByConformancePacks"], list)

    def test_get_aggregate_config_rule_compliance_summary(self, client, aggregator):
        """GetAggregateConfigRuleComplianceSummary returns compliance counts."""
        resp = client.get_aggregate_config_rule_compliance_summary(
            ConfigurationAggregatorName=aggregator
        )
        assert "AggregateComplianceCounts" in resp
        assert isinstance(resp["AggregateComplianceCounts"], list)

    def test_get_aggregate_conformance_pack_compliance_summary(self, client, aggregator):
        """GetAggregateConformancePackComplianceSummary returns summaries."""
        resp = client.get_aggregate_conformance_pack_compliance_summary(
            ConfigurationAggregatorName=aggregator
        )
        assert "AggregateConformancePackComplianceSummaries" in resp
        assert isinstance(resp["AggregateConformancePackComplianceSummaries"], list)

    def test_get_aggregate_discovered_resource_counts(self, client, aggregator):
        """GetAggregateDiscoveredResourceCounts returns resource counts."""
        resp = client.get_aggregate_discovered_resource_counts(
            ConfigurationAggregatorName=aggregator
        )
        assert "TotalDiscoveredResources" in resp
        assert isinstance(resp["TotalDiscoveredResources"], int)

    def test_get_aggregate_compliance_details_by_config_rule(self, client, aggregator):
        """GetAggregateComplianceDetailsByConfigRule returns evaluation results."""
        resp = client.get_aggregate_compliance_details_by_config_rule(
            ConfigurationAggregatorName=aggregator,
            ConfigRuleName="nonexistent-rule",
            ComplianceType="NON_COMPLIANT",
            AccountId="123456789012",
            AwsRegion="us-east-1",
        )
        assert "AggregateEvaluationResults" in resp
        assert isinstance(resp["AggregateEvaluationResults"], list)

    def test_list_aggregate_discovered_resources_with_aggregator(self, client, aggregator):
        """ListAggregateDiscoveredResources returns resource identifiers."""
        resp = client.list_aggregate_discovered_resources(
            ConfigurationAggregatorName=aggregator,
            ResourceType="AWS::S3::Bucket",
        )
        assert "ResourceIdentifiers" in resp
        assert isinstance(resp["ResourceIdentifiers"], list)


class TestRemediationExtraOperations:
    """Test remediation exception and execution status operations."""

    @pytest.fixture
    def client(self):
        return make_client("config")

    def test_describe_remediation_exceptions(self, client):
        """DescribeRemediationExceptions returns empty list for a rule."""
        resp = client.describe_remediation_exceptions(ConfigRuleName="nonexistent-rule")
        assert "RemediationExceptions" in resp
        assert isinstance(resp["RemediationExceptions"], list)

    def test_describe_remediation_execution_status(self, client):
        """DescribeRemediationExecutionStatus returns empty list for a rule."""
        resp = client.describe_remediation_execution_status(ConfigRuleName="nonexistent-rule")
        assert "RemediationExecutionStatuses" in resp
        assert isinstance(resp["RemediationExecutionStatuses"], list)


class TestConformancePackComplianceOperations:
    """Test conformance pack compliance operations."""

    @pytest.fixture
    def client(self):
        return make_client("config")

    def test_describe_conformance_pack_compliance_nonexistent(self, client):
        """DescribeConformancePackCompliance raises for nonexistent pack."""
        with pytest.raises(ClientError) as exc:
            client.describe_conformance_pack_compliance(ConformancePackName="nonexistent-pack-xyz")
        assert "NoSuchConformancePackException" in exc.value.response["Error"]["Code"]

    def test_get_conformance_pack_compliance_details_nonexistent(self, client):
        """GetConformancePackComplianceDetails raises for nonexistent pack."""
        with pytest.raises(ClientError) as exc:
            client.get_conformance_pack_compliance_details(
                ConformancePackName="nonexistent-pack-xyz"
            )
        assert "NoSuchConformancePackException" in exc.value.response["Error"]["Code"]

    def test_get_resource_evaluation_summary_nonexistent(self, client):
        """GetResourceEvaluationSummary raises for nonexistent evaluation."""
        with pytest.raises(ClientError) as exc:
            client.get_resource_evaluation_summary(ResourceEvaluationId="fake-eval-id")
        assert "ResourceNotFoundException" in exc.value.response["Error"]["Code"]


class TestConfigAdditionalOps:
    """Additional Config operations."""

    @pytest.fixture
    def client(self):
        return make_client("config")

    def test_delete_evaluation_results_nonexistent(self, client):
        """DeleteEvaluationResults raises for nonexistent config rule."""
        with pytest.raises(ClientError) as exc:
            client.delete_evaluation_results(ConfigRuleName="nonexistent-rule-xyz")
        assert "NoSuchConfigRuleException" in exc.value.response["Error"]["Code"]

    def test_put_external_evaluation(self, client):
        """PutExternalEvaluation requires a valid config rule."""
        with pytest.raises(ClientError) as exc:
            client.put_external_evaluation(
                ConfigRuleName="nonexistent-rule-xyz",
                ExternalEvaluation={
                    "ComplianceResourceType": "AWS::EC2::Instance",
                    "ComplianceResourceId": "i-12345678",
                    "ComplianceType": "COMPLIANT",
                    "OrderingTimestamp": "2026-01-01T00:00:00Z",
                },
            )
        err_code = exc.value.response["Error"]["Code"]
        assert "NoSuchConfigRuleException" in err_code

    def test_start_resource_evaluation(self, client):
        """StartResourceEvaluation returns an evaluation ID."""
        resp = client.start_resource_evaluation(
            ResourceDetails={
                "ResourceId": "i-12345678",
                "ResourceType": "AWS::EC2::Instance",
                "ResourceConfiguration": '{"instanceType":"t2.micro"}',
            },
            EvaluationMode="PROACTIVE",
        )
        assert "ResourceEvaluationId" in resp


class TestConfigAggregateResourceConfig:
    """Tests for GetAggregateResourceConfig."""

    def test_get_aggregate_resource_config(self, config):
        """GetAggregateResourceConfig returns ConfigurationItem for an aggregator."""
        config.put_configuration_aggregator(
            ConfigurationAggregatorName="test-agg-resource",
            AccountAggregationSources=[{"AccountIds": ["123456789012"], "AllAwsRegions": True}],
        )
        try:
            resp = config.get_aggregate_resource_config(
                ConfigurationAggregatorName="test-agg-resource",
                ResourceIdentifier={
                    "SourceAccountId": "123456789012",
                    "SourceRegion": "us-east-1",
                    "ResourceId": "nonexistent-resource",
                    "ResourceType": "AWS::S3::Bucket",
                },
            )
            assert "ConfigurationItem" in resp
        finally:
            config.delete_configuration_aggregator(ConfigurationAggregatorName="test-agg-resource")


class TestConfigOrganizationConfigRuleDetailedStatus:
    """Tests for GetOrganizationConfigRuleDetailedStatus."""

    def test_get_organization_config_rule_detailed_status_nonexistent(self, config):
        """GetOrganizationConfigRuleDetailedStatus raises NoSuchOrganizationConfigRuleException."""
        with pytest.raises(ClientError) as exc:
            config.get_organization_config_rule_detailed_status(
                OrganizationConfigRuleName="nonexistent-org-rule"
            )
        assert "NoSuchOrganizationConfigRuleException" in exc.value.response["Error"]["Code"]


class TestConfigConformancePackComplianceSummary:
    """Tests for GetConformancePackComplianceSummary."""

    def test_get_conformance_pack_compliance_summary_nonexistent(self, config):
        """GetConformancePackComplianceSummary raises error for nonexistent pack."""
        with pytest.raises(ClientError) as exc:
            config.get_conformance_pack_compliance_summary(
                ConformancePackNames=["nonexistent-pack"]
            )
        assert "NoSuchConformancePackException" in exc.value.response["Error"]["Code"]


class TestConfigRemediationExceptions:
    """Tests for remediation exception CRUD operations."""

    def test_describe_remediation_exceptions_empty(self, config):
        """DescribeRemediationExceptions returns empty list for a rule with no exceptions."""
        resp = config.describe_remediation_exceptions(ConfigRuleName="no-such-rule")
        assert "RemediationExceptions" in resp
        assert isinstance(resp["RemediationExceptions"], list)

    def test_put_and_describe_remediation_exceptions(self, config):
        """PutRemediationExceptions adds exceptions, DescribeRemediationExceptions returns them."""
        config.put_config_rule(
            ConfigRule={
                "ConfigRuleName": "remediation-exc-rule",
                "Source": {
                    "Owner": "AWS",
                    "SourceIdentifier": "S3_BUCKET_VERSIONING_ENABLED",
                },
            }
        )
        try:
            put_resp = config.put_remediation_exceptions(
                ConfigRuleName="remediation-exc-rule",
                ResourceKeys=[
                    {"ResourceType": "AWS::S3::Bucket", "ResourceId": "test-bucket-1"},
                    {"ResourceType": "AWS::S3::Bucket", "ResourceId": "test-bucket-2"},
                ],
            )
            assert "FailedBatches" in put_resp

            desc_resp = config.describe_remediation_exceptions(
                ConfigRuleName="remediation-exc-rule"
            )
            assert "RemediationExceptions" in desc_resp
            assert isinstance(desc_resp["RemediationExceptions"], list)
        finally:
            config.delete_config_rule(ConfigRuleName="remediation-exc-rule")

    def test_delete_remediation_exceptions(self, config):
        """DeleteRemediationExceptions removes previously added exceptions."""
        config.put_config_rule(
            ConfigRule={
                "ConfigRuleName": "del-remediation-exc-rule",
                "Source": {
                    "Owner": "AWS",
                    "SourceIdentifier": "S3_BUCKET_VERSIONING_ENABLED",
                },
            }
        )
        try:
            config.put_remediation_exceptions(
                ConfigRuleName="del-remediation-exc-rule",
                ResourceKeys=[
                    {"ResourceType": "AWS::S3::Bucket", "ResourceId": "del-bucket"},
                ],
            )
            del_resp = config.delete_remediation_exceptions(
                ConfigRuleName="del-remediation-exc-rule",
                ResourceKeys=[
                    {"ResourceType": "AWS::S3::Bucket", "ResourceId": "del-bucket"},
                ],
            )
            assert "FailedBatches" in del_resp
        finally:
            config.delete_config_rule(ConfigRuleName="del-remediation-exc-rule")


class TestConfigStartRemediationExecution:
    """Tests for StartRemediationExecution."""

    def test_start_remediation_execution_no_config(self, config):
        """StartRemediationExecution raises error when no remediation config exists."""
        with pytest.raises(ClientError) as exc:
            config.start_remediation_execution(
                ConfigRuleName="nonexistent-rule",
                ResourceKeys=[{"resourceType": "AWS::S3::Bucket", "resourceId": "test-bucket"}],
            )
        assert "NoSuchRemediationConfigurationException" in exc.value.response["Error"]["Code"]


class TestConfigAggregatorSourcesStatus:
    """Tests for DescribeConfigurationAggregatorSourcesStatus."""

    def test_describe_configuration_aggregator_sources_status_nonexistent(self, config):
        """DescribeConfigurationAggregatorSourcesStatus with nonexistent aggregator raises error."""
        with pytest.raises(ClientError) as exc:
            config.describe_configuration_aggregator_sources_status(
                ConfigurationAggregatorName="nonexistent-aggregator-xyz"
            )
        assert exc.value.response["Error"]["Code"] == "NoSuchConfigurationAggregatorException"


class TestConfigGapOperations:
    """Tests for previously unimplemented config gap operations."""

    def test_delete_pending_aggregation_request_succeeds(self, config):
        """DeletePendingAggregationRequest completes without error."""
        resp = config.delete_pending_aggregation_request(
            RequesterAccountId="123456789012",
            RequesterAwsRegion="us-east-1",
        )
        assert "ResponseMetadata" in resp

    def test_delete_service_linked_configuration_recorder_returns_name_arn(self, config):
        """DeleteServiceLinkedConfigurationRecorder returns Arn and Name."""
        resp = config.delete_service_linked_configuration_recorder(
            ServicePrincipal="ec2.amazonaws.com",
        )
        assert "Arn" in resp
        assert "Name" in resp

    def test_deliver_config_snapshot_returns_snapshot_id(self, config):
        """DeliverConfigSnapshot returns a configSnapshotId."""
        resp = config.deliver_config_snapshot(deliveryChannelName="default")
        assert "configSnapshotId" in resp
        assert len(resp["configSnapshotId"]) > 0

    def test_get_organization_custom_rule_policy_returns_policy(self, config):
        """GetOrganizationCustomRulePolicy returns PolicyText key."""
        resp = config.get_organization_custom_rule_policy(
            OrganizationConfigRuleName="test-rule",
        )
        assert "PolicyText" in resp

    def test_put_service_linked_configuration_recorder_returns_name_arn(self, config):
        """PutServiceLinkedConfigurationRecorder returns Arn and Name."""
        resp = config.put_service_linked_configuration_recorder(
            ServicePrincipal="ec2.amazonaws.com",
        )
        assert "Arn" in resp
        assert "Name" in resp

    def test_select_aggregate_resource_config_returns_results(self, config):
        """SelectAggregateResourceConfig returns Results and QueryInfo."""
        resp = config.select_aggregate_resource_config(
            Expression="SELECT *",
            ConfigurationAggregatorName="test-agg",
        )
        assert "Results" in resp
        assert "QueryInfo" in resp

    def test_associate_resource_types_returns_recorder(self, config):
        """AssociateResourceTypes returns ConfigurationRecorder."""
        resp = config.associate_resource_types(
            ConfigurationRecorderArn="arn:aws:config:us-east-1:123456789012:config-recorder/test",
            ResourceTypes=["AWS::EC2::Instance"],
        )
        assert "ConfigurationRecorder" in resp

    def test_disassociate_resource_types_returns_recorder(self, config):
        """DisassociateResourceTypes returns ConfigurationRecorder."""
        resp = config.disassociate_resource_types(
            ConfigurationRecorderArn="arn:aws:config:us-east-1:123456789012:config-recorder/test",
            ResourceTypes=["AWS::EC2::Instance"],
        )
        assert "ConfigurationRecorder" in resp
