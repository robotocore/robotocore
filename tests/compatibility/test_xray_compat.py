"""X-Ray compatibility tests."""

import uuid

import pytest

from tests.compatibility.conftest import make_client


@pytest.fixture
def xray():
    return make_client("xray")


def _unique(prefix):
    return f"{prefix}-{uuid.uuid4().hex[:8]}"


class TestXRaySamplingRuleOperations:
    def test_create_and_get_sampling_rules(self, xray):
        rule_name = _unique("rule")
        resp = xray.create_sampling_rule(
            SamplingRule={
                "RuleName": rule_name,
                "ResourceARN": "*",
                "Priority": 1000,
                "FixedRate": 0.05,
                "ReservoirSize": 1,
                "ServiceName": "*",
                "ServiceType": "*",
                "Host": "*",
                "HTTPMethod": "*",
                "URLPath": "*",
                "Version": 1,
            }
        )
        created = resp["SamplingRuleRecord"]
        assert created["SamplingRule"]["RuleName"] == rule_name

        # GetSamplingRules should include our rule
        rules_resp = xray.get_sampling_rules()
        rule_names = [r["SamplingRule"]["RuleName"] for r in rules_resp["SamplingRuleRecords"]]
        assert rule_name in rule_names

        # Cleanup
        xray.delete_sampling_rule(RuleName=rule_name)

    def test_delete_sampling_rule(self, xray):
        rule_name = _unique("rule")
        xray.create_sampling_rule(
            SamplingRule={
                "RuleName": rule_name,
                "ResourceARN": "*",
                "Priority": 1001,
                "FixedRate": 0.1,
                "ReservoirSize": 2,
                "ServiceName": "*",
                "ServiceType": "*",
                "Host": "*",
                "HTTPMethod": "*",
                "URLPath": "*",
                "Version": 1,
            }
        )
        del_resp = xray.delete_sampling_rule(RuleName=rule_name)
        assert del_resp["SamplingRuleRecord"]["SamplingRule"]["RuleName"] == rule_name

    def test_get_sampling_statistic_summaries(self, xray):
        resp = xray.get_sampling_statistic_summaries()
        assert "SamplingStatisticSummaries" in resp


class TestXRayGroupOperations:
    def test_create_and_get_group(self, xray):
        group_name = _unique("group")
        resp = xray.create_group(GroupName=group_name)
        group = resp["Group"]
        assert group["GroupName"] == group_name
        group_arn = group["GroupARN"]

        # GetGroup by name
        get_resp = xray.get_group(GroupName=group_name)
        assert get_resp["Group"]["GroupName"] == group_name

        # Cleanup
        xray.delete_group(GroupARN=group_arn)

    def test_get_groups(self, xray):
        group_name = _unique("group")
        resp = xray.create_group(GroupName=group_name)
        group_arn = resp["Group"]["GroupARN"]

        groups_resp = xray.get_groups()
        group_names = [g["GroupName"] for g in groups_resp["Groups"]]
        assert group_name in group_names

        # Cleanup
        xray.delete_group(GroupARN=group_arn)

    def test_delete_group(self, xray):
        group_name = _unique("group")
        resp = xray.create_group(GroupName=group_name)
        group_arn = resp["Group"]["GroupARN"]

        xray.delete_group(GroupARN=group_arn)

        # Verify deleted
        groups_resp = xray.get_groups()
        group_names = [g["GroupName"] for g in groups_resp["Groups"]]
        assert group_name not in group_names

    def test_tag_and_untag_group(self, xray):
        group_name = _unique("group")
        resp = xray.create_group(GroupName=group_name)
        group_arn = resp["Group"]["GroupARN"]

        # Tag
        xray.tag_resource(
            ResourceARN=group_arn,
            Tags=[{"Key": "env", "Value": "test"}],
        )

        # List tags
        tags_resp = xray.list_tags_for_resource(ResourceARN=group_arn)
        tags = {t["Key"]: t["Value"] for t in tags_resp["Tags"]}
        assert tags["env"] == "test"

        # Untag
        xray.untag_resource(ResourceARN=group_arn, TagKeys=["env"])
        tags_resp = xray.list_tags_for_resource(ResourceARN=group_arn)
        tag_keys = [t["Key"] for t in tags_resp["Tags"]]
        assert "env" not in tag_keys

        # Cleanup
        xray.delete_group(GroupARN=group_arn)


class TestXRayEncryptionConfig:
    def test_get_encryption_config(self, xray):
        resp = xray.get_encryption_config()
        config = resp["EncryptionConfig"]
        assert "Type" in config
        assert config["Status"] in ("UPDATING", "ACTIVE")

    def test_put_encryption_config(self, xray):
        resp = xray.put_encryption_config(Type="NONE")
        config = resp["EncryptionConfig"]
        assert config["Type"] == "NONE"
