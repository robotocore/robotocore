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
