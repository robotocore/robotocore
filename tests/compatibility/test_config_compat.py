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
                pass
            try:
                config.delete_delivery_channel(DeliveryChannelName="default")
            except ClientError:
                pass
            try:
                config.delete_configuration_recorder(ConfigurationRecorderName="default")
            except ClientError:
                pass
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
                pass
            try:
                config.delete_configuration_recorder(ConfigurationRecorderName="default")
            except ClientError:
                pass
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
                pass
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
        """GetCustomRulePolicy returns a response."""
        client.get_custom_rule_policy()

    def test_get_discovered_resource_counts(self, client):
        """GetDiscoveredResourceCounts returns a response."""
        resp = client.get_discovered_resource_counts()
        assert "totalDiscoveredResources" in resp

    def test_list_configuration_recorders(self, client):
        """ListConfigurationRecorders returns a response."""
        client.list_configuration_recorders()

    def test_list_resource_evaluations(self, client):
        """ListResourceEvaluations returns a response."""
        resp = client.list_resource_evaluations()
        assert "ResourceEvaluations" in resp

    def test_start_config_rules_evaluation(self, client):
        """StartConfigRulesEvaluation returns a response."""
        client.start_config_rules_evaluation()


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
