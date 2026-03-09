"""WAFv2 compatibility tests."""

import uuid

import pytest
from botocore.exceptions import ClientError

from tests.compatibility.conftest import make_client


@pytest.fixture
def wafv2():
    return make_client("wafv2")


def _unique(prefix):
    return f"{prefix}-{uuid.uuid4().hex[:8]}"


class TestWAFv2WebACLOperations:
    """Tests for WAFv2 WebACL create, get, list, update, tags, delete."""

    def test_create_and_get_web_acl(self, wafv2):
        name = _unique("webacl")
        resp = wafv2.create_web_acl(
            Name=name,
            Scope="REGIONAL",
            DefaultAction={"Allow": {}},
            VisibilityConfig={
                "SampledRequestsEnabled": True,
                "CloudWatchMetricsEnabled": True,
                "MetricName": name,
            },
        )
        summary = resp["Summary"]
        assert summary["Name"] == name
        assert "ARN" in summary
        assert "Id" in summary
        assert "LockToken" in summary

        # Get
        get_resp = wafv2.get_web_acl(Name=name, Scope="REGIONAL", Id=summary["Id"])
        acl = get_resp["WebACL"]
        assert acl["Name"] == name
        assert acl["DefaultAction"] == {"Allow": {}}
        assert "LockToken" in get_resp

        # Cleanup
        wafv2.delete_web_acl(
            Name=name, Scope="REGIONAL", Id=summary["Id"], LockToken=get_resp["LockToken"]
        )

    def test_list_web_acls(self, wafv2):
        name = _unique("webacl")
        resp = wafv2.create_web_acl(
            Name=name,
            Scope="REGIONAL",
            DefaultAction={"Allow": {}},
            VisibilityConfig={
                "SampledRequestsEnabled": True,
                "CloudWatchMetricsEnabled": True,
                "MetricName": name,
            },
        )
        summary = resp["Summary"]

        listed = wafv2.list_web_acls(Scope="REGIONAL")
        names = [a["Name"] for a in listed["WebACLs"]]
        assert name in names

        # Cleanup
        get_resp = wafv2.get_web_acl(Name=name, Scope="REGIONAL", Id=summary["Id"])
        wafv2.delete_web_acl(
            Name=name, Scope="REGIONAL", Id=summary["Id"], LockToken=get_resp["LockToken"]
        )

    def test_update_web_acl(self, wafv2):
        name = _unique("webacl")
        resp = wafv2.create_web_acl(
            Name=name,
            Scope="REGIONAL",
            DefaultAction={"Allow": {}},
            VisibilityConfig={
                "SampledRequestsEnabled": True,
                "CloudWatchMetricsEnabled": True,
                "MetricName": name,
            },
        )
        summary = resp["Summary"]

        get_resp = wafv2.get_web_acl(Name=name, Scope="REGIONAL", Id=summary["Id"])
        lock_token = get_resp["LockToken"]

        update_resp = wafv2.update_web_acl(
            Name=name,
            Scope="REGIONAL",
            Id=summary["Id"],
            DefaultAction={"Block": {}},
            LockToken=lock_token,
            VisibilityConfig={
                "SampledRequestsEnabled": True,
                "CloudWatchMetricsEnabled": True,
                "MetricName": name,
            },
        )
        assert "NextLockToken" in update_resp

        # Verify update applied
        get_resp2 = wafv2.get_web_acl(Name=name, Scope="REGIONAL", Id=summary["Id"])
        assert get_resp2["WebACL"]["DefaultAction"] == {"Block": {}}

        # Cleanup
        wafv2.delete_web_acl(
            Name=name,
            Scope="REGIONAL",
            Id=summary["Id"],
            LockToken=get_resp2["LockToken"],
        )

    def test_tag_and_untag_web_acl(self, wafv2):
        name = _unique("webacl")
        resp = wafv2.create_web_acl(
            Name=name,
            Scope="REGIONAL",
            DefaultAction={"Allow": {}},
            VisibilityConfig={
                "SampledRequestsEnabled": True,
                "CloudWatchMetricsEnabled": True,
                "MetricName": name,
            },
        )
        summary = resp["Summary"]
        arn = summary["ARN"]

        # Tag
        wafv2.tag_resource(
            ResourceARN=arn,
            Tags=[{"Key": "env", "Value": "staging"}, {"Key": "team", "Value": "infra"}],
        )

        tags_resp = wafv2.list_tags_for_resource(ResourceARN=arn)
        tag_list = tags_resp["TagInfoForResource"]["TagList"]
        tag_map = {t["Key"]: t["Value"] for t in tag_list}
        assert tag_map["env"] == "staging"
        assert tag_map["team"] == "infra"

        # Untag
        wafv2.untag_resource(ResourceARN=arn, TagKeys=["env"])
        tags_resp2 = wafv2.list_tags_for_resource(ResourceARN=arn)
        tag_list2 = tags_resp2["TagInfoForResource"]["TagList"]
        tag_keys = [t["Key"] for t in tag_list2]
        assert "env" not in tag_keys
        assert "team" in tag_keys

        # Cleanup
        get_resp = wafv2.get_web_acl(Name=name, Scope="REGIONAL", Id=summary["Id"])
        wafv2.delete_web_acl(
            Name=name, Scope="REGIONAL", Id=summary["Id"], LockToken=get_resp["LockToken"]
        )

    def test_delete_web_acl(self, wafv2):
        name = _unique("webacl")
        resp = wafv2.create_web_acl(
            Name=name,
            Scope="REGIONAL",
            DefaultAction={"Allow": {}},
            VisibilityConfig={
                "SampledRequestsEnabled": True,
                "CloudWatchMetricsEnabled": True,
                "MetricName": name,
            },
        )
        summary = resp["Summary"]

        get_resp = wafv2.get_web_acl(Name=name, Scope="REGIONAL", Id=summary["Id"])
        wafv2.delete_web_acl(
            Name=name, Scope="REGIONAL", Id=summary["Id"], LockToken=get_resp["LockToken"]
        )

        # Verify deletion
        with pytest.raises(ClientError) as exc:
            wafv2.get_web_acl(Name=name, Scope="REGIONAL", Id=summary["Id"])
        assert exc.value.response["Error"]["Code"] == "WAFNonexistentItemException"


class TestWAFv2WebACLAssociationOperations:
    """Tests for WAFv2 WebACL association operations."""

    def _create_web_acl(self, wafv2):
        name = _unique("webacl")
        resp = wafv2.create_web_acl(
            Name=name,
            Scope="REGIONAL",
            DefaultAction={"Allow": {}},
            VisibilityConfig={
                "SampledRequestsEnabled": True,
                "CloudWatchMetricsEnabled": True,
                "MetricName": name,
            },
        )
        return name, resp["Summary"]

    def _delete_web_acl(self, wafv2, name, acl_id):
        get_resp = wafv2.get_web_acl(Name=name, Scope="REGIONAL", Id=acl_id)
        wafv2.delete_web_acl(
            Name=name, Scope="REGIONAL", Id=acl_id, LockToken=get_resp["LockToken"]
        )

    def test_associate_web_acl(self, wafv2):
        name, summary = self._create_web_acl(wafv2)
        # Use a fake ALB ARN for association
        resource_arn = (
            "arn:aws:elasticloadbalancing:us-east-1:123456789012"
            ":loadbalancer/app/my-alb/1234567890123456"
        )
        try:
            resp = wafv2.associate_web_acl(WebACLArn=summary["ARN"], ResourceArn=resource_arn)
            assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200
        finally:
            self._delete_web_acl(wafv2, name, summary["Id"])

    def test_disassociate_web_acl(self, wafv2):
        name, summary = self._create_web_acl(wafv2)
        resource_arn = (
            "arn:aws:elasticloadbalancing:us-east-1:123456789012"
            ":loadbalancer/app/my-alb/1234567890123456"
        )
        try:
            wafv2.associate_web_acl(WebACLArn=summary["ARN"], ResourceArn=resource_arn)
            resp = wafv2.disassociate_web_acl(ResourceArn=resource_arn)
            assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200
        finally:
            self._delete_web_acl(wafv2, name, summary["Id"])

    def test_get_web_acl_for_resource(self, wafv2):
        name, summary = self._create_web_acl(wafv2)
        resource_arn = (
            "arn:aws:elasticloadbalancing:us-east-1:123456789012"
            ":loadbalancer/app/my-alb/9876543210987654"
        )
        try:
            wafv2.associate_web_acl(WebACLArn=summary["ARN"], ResourceArn=resource_arn)
            resp = wafv2.get_web_acl_for_resource(ResourceArn=resource_arn)
            assert "WebACL" in resp
            assert resp["WebACL"]["Name"] == name
        finally:
            wafv2.disassociate_web_acl(ResourceArn=resource_arn)
            self._delete_web_acl(wafv2, name, summary["Id"])


class TestWAFv2LoggingConfigurationOperations:
    """Tests for WAFv2 LoggingConfiguration operations."""

    def _create_web_acl(self, wafv2):
        name = _unique("webacl")
        resp = wafv2.create_web_acl(
            Name=name,
            Scope="REGIONAL",
            DefaultAction={"Allow": {}},
            VisibilityConfig={
                "SampledRequestsEnabled": True,
                "CloudWatchMetricsEnabled": True,
                "MetricName": name,
            },
        )
        return name, resp["Summary"]

    def _delete_web_acl(self, wafv2, name, acl_id):
        get_resp = wafv2.get_web_acl(Name=name, Scope="REGIONAL", Id=acl_id)
        wafv2.delete_web_acl(
            Name=name, Scope="REGIONAL", Id=acl_id, LockToken=get_resp["LockToken"]
        )

    def test_put_and_get_logging_configuration(self, wafv2):
        name, summary = self._create_web_acl(wafv2)
        log_dest = "arn:aws:firehose:us-east-1:123456789012:deliverystream/aws-waf-logs-test"
        try:
            put_resp = wafv2.put_logging_configuration(
                LoggingConfiguration={
                    "ResourceArn": summary["ARN"],
                    "LogDestinationConfigs": [log_dest],
                }
            )
            assert "LoggingConfiguration" in put_resp
            assert put_resp["LoggingConfiguration"]["ResourceArn"] == summary["ARN"]

            get_resp = wafv2.get_logging_configuration(ResourceArn=summary["ARN"])
            assert get_resp["LoggingConfiguration"]["ResourceArn"] == summary["ARN"]
            assert log_dest in get_resp["LoggingConfiguration"]["LogDestinationConfigs"]
        finally:
            try:
                wafv2.delete_logging_configuration(ResourceArn=summary["ARN"])
            except Exception:
                pass
            self._delete_web_acl(wafv2, name, summary["Id"])

    def test_delete_logging_configuration(self, wafv2):
        name, summary = self._create_web_acl(wafv2)
        log_dest = "arn:aws:firehose:us-east-1:123456789012:deliverystream/aws-waf-logs-test-del"
        try:
            wafv2.put_logging_configuration(
                LoggingConfiguration={
                    "ResourceArn": summary["ARN"],
                    "LogDestinationConfigs": [log_dest],
                }
            )
            del_resp = wafv2.delete_logging_configuration(ResourceArn=summary["ARN"])
            assert del_resp["ResponseMetadata"]["HTTPStatusCode"] == 200

            # Verify it's gone
            with pytest.raises(ClientError) as exc:
                wafv2.get_logging_configuration(ResourceArn=summary["ARN"])
            assert exc.value.response["Error"]["Code"] in (
                "WAFNonexistentItemException",
                "WAFInternalErrorException",
            )
        finally:
            self._delete_web_acl(wafv2, name, summary["Id"])

    def test_list_logging_configurations(self, wafv2):
        name, summary = self._create_web_acl(wafv2)
        log_dest = "arn:aws:firehose:us-east-1:123456789012:deliverystream/aws-waf-logs-test-list"
        try:
            wafv2.put_logging_configuration(
                LoggingConfiguration={
                    "ResourceArn": summary["ARN"],
                    "LogDestinationConfigs": [log_dest],
                }
            )
            list_resp = wafv2.list_logging_configurations(Scope="REGIONAL")
            assert "LoggingConfigurations" in list_resp
            arns = [lc["ResourceArn"] for lc in list_resp["LoggingConfigurations"]]
            assert summary["ARN"] in arns
        finally:
            try:
                wafv2.delete_logging_configuration(ResourceArn=summary["ARN"])
            except Exception:
                pass
            self._delete_web_acl(wafv2, name, summary["Id"])


class TestWAFv2IPSetOperations:
    """Tests for WAFv2 IPSet create, get, list, update, delete."""

    def test_create_and_get_ip_set(self, wafv2):
        name = _unique("ipset")
        resp = wafv2.create_ip_set(
            Name=name,
            Scope="REGIONAL",
            IPAddressVersion="IPV4",
            Addresses=["10.0.0.0/8"],
        )
        summary = resp["Summary"]
        assert summary["Name"] == name
        assert "Id" in summary
        assert "LockToken" in summary

        get_resp = wafv2.get_ip_set(Name=name, Scope="REGIONAL", Id=summary["Id"])
        ip_set = get_resp["IPSet"]
        assert ip_set["Name"] == name
        assert ip_set["IPAddressVersion"] == "IPV4"
        assert "10.0.0.0/8" in ip_set["Addresses"]

        # Cleanup
        wafv2.delete_ip_set(
            Name=name, Scope="REGIONAL", Id=summary["Id"], LockToken=get_resp["LockToken"]
        )

    def test_list_ip_sets(self, wafv2):
        name = _unique("ipset")
        resp = wafv2.create_ip_set(
            Name=name,
            Scope="REGIONAL",
            IPAddressVersion="IPV4",
            Addresses=["192.168.1.0/24"],
        )
        summary = resp["Summary"]

        listed = wafv2.list_ip_sets(Scope="REGIONAL")
        names = [s["Name"] for s in listed["IPSets"]]
        assert name in names

        # Cleanup
        get_resp = wafv2.get_ip_set(Name=name, Scope="REGIONAL", Id=summary["Id"])
        wafv2.delete_ip_set(
            Name=name, Scope="REGIONAL", Id=summary["Id"], LockToken=get_resp["LockToken"]
        )

    def test_update_ip_set(self, wafv2):
        name = _unique("ipset")
        resp = wafv2.create_ip_set(
            Name=name,
            Scope="REGIONAL",
            IPAddressVersion="IPV4",
            Addresses=["1.2.3.4/32"],
        )
        summary = resp["Summary"]

        get_resp = wafv2.get_ip_set(Name=name, Scope="REGIONAL", Id=summary["Id"])

        update_resp = wafv2.update_ip_set(
            Name=name,
            Scope="REGIONAL",
            Id=summary["Id"],
            Addresses=["1.2.3.4/32", "5.6.7.8/32"],
            LockToken=get_resp["LockToken"],
        )
        assert "NextLockToken" in update_resp

        # Verify update
        get_resp2 = wafv2.get_ip_set(Name=name, Scope="REGIONAL", Id=summary["Id"])
        assert sorted(get_resp2["IPSet"]["Addresses"]) == ["1.2.3.4/32", "5.6.7.8/32"]

        # Cleanup
        wafv2.delete_ip_set(
            Name=name, Scope="REGIONAL", Id=summary["Id"], LockToken=get_resp2["LockToken"]
        )

    def test_delete_ip_set(self, wafv2):
        name = _unique("ipset")
        resp = wafv2.create_ip_set(
            Name=name,
            Scope="REGIONAL",
            IPAddressVersion="IPV4",
            Addresses=["172.16.0.0/12"],
        )
        summary = resp["Summary"]

        get_resp = wafv2.get_ip_set(Name=name, Scope="REGIONAL", Id=summary["Id"])
        wafv2.delete_ip_set(
            Name=name, Scope="REGIONAL", Id=summary["Id"], LockToken=get_resp["LockToken"]
        )

        with pytest.raises(ClientError) as exc:
            wafv2.get_ip_set(Name=name, Scope="REGIONAL", Id=summary["Id"])
        assert exc.value.response["Error"]["Code"] == "WAFNonexistentItemException"


class TestWAFv2RegexPatternSetOperations:
    """Tests for WAFv2 RegexPatternSet create, get, list, delete."""

    def test_create_and_get_regex_pattern_set(self, wafv2):
        name = _unique("regex")
        resp = wafv2.create_regex_pattern_set(
            Name=name,
            Scope="REGIONAL",
            RegularExpressionList=[{"RegexString": "^hello"}, {"RegexString": "world$"}],
        )
        summary = resp["Summary"]
        assert summary["Name"] == name
        assert "Id" in summary

        get_resp = wafv2.get_regex_pattern_set(Name=name, Scope="REGIONAL", Id=summary["Id"])
        rps = get_resp["RegexPatternSet"]
        assert rps["Name"] == name
        regexes = [r["RegexString"] for r in rps["RegularExpressionList"]]
        assert "^hello" in regexes
        assert "world$" in regexes

        # Cleanup
        wafv2.delete_regex_pattern_set(
            Name=name, Scope="REGIONAL", Id=summary["Id"], LockToken=get_resp["LockToken"]
        )

    def test_list_regex_pattern_sets(self, wafv2):
        name = _unique("regex")
        resp = wafv2.create_regex_pattern_set(
            Name=name,
            Scope="REGIONAL",
            RegularExpressionList=[{"RegexString": "test.*"}],
        )
        summary = resp["Summary"]

        listed = wafv2.list_regex_pattern_sets(Scope="REGIONAL")
        names = [s["Name"] for s in listed["RegexPatternSets"]]
        assert name in names

        # Cleanup
        get_resp = wafv2.get_regex_pattern_set(Name=name, Scope="REGIONAL", Id=summary["Id"])
        wafv2.delete_regex_pattern_set(
            Name=name, Scope="REGIONAL", Id=summary["Id"], LockToken=get_resp["LockToken"]
        )

    def test_update_regex_pattern_set(self, wafv2):
        name = _unique("regex")
        resp = wafv2.create_regex_pattern_set(
            Name=name,
            Scope="REGIONAL",
            RegularExpressionList=[{"RegexString": "^hello"}],
        )
        summary = resp["Summary"]

        get_resp = wafv2.get_regex_pattern_set(Name=name, Scope="REGIONAL", Id=summary["Id"])
        update_resp = wafv2.update_regex_pattern_set(
            Name=name,
            Scope="REGIONAL",
            Id=summary["Id"],
            RegularExpressionList=[{"RegexString": "^hello"}, {"RegexString": "^world"}],
            LockToken=get_resp["LockToken"],
        )
        assert "NextLockToken" in update_resp

        # Verify update
        get_resp2 = wafv2.get_regex_pattern_set(Name=name, Scope="REGIONAL", Id=summary["Id"])
        regexes = [r["RegexString"] for r in get_resp2["RegexPatternSet"]["RegularExpressionList"]]
        assert "^hello" in regexes
        assert "^world" in regexes

        # Cleanup
        wafv2.delete_regex_pattern_set(
            Name=name, Scope="REGIONAL", Id=summary["Id"], LockToken=get_resp2["LockToken"]
        )

    def test_delete_regex_pattern_set(self, wafv2):
        name = _unique("regex")
        resp = wafv2.create_regex_pattern_set(
            Name=name,
            Scope="REGIONAL",
            RegularExpressionList=[{"RegexString": "^foo"}],
        )
        summary = resp["Summary"]

        get_resp = wafv2.get_regex_pattern_set(Name=name, Scope="REGIONAL", Id=summary["Id"])
        wafv2.delete_regex_pattern_set(
            Name=name, Scope="REGIONAL", Id=summary["Id"], LockToken=get_resp["LockToken"]
        )

        with pytest.raises(ClientError) as exc:
            wafv2.get_regex_pattern_set(Name=name, Scope="REGIONAL", Id=summary["Id"])
        assert exc.value.response["Error"]["Code"] == "WAFNonexistentItemException"


class TestWAFv2RuleGroupOperations:
    """Tests for WAFv2 RuleGroup create, get, list, delete."""

    def test_create_and_get_rule_group(self, wafv2):
        name = _unique("rulegroup")
        resp = wafv2.create_rule_group(
            Name=name,
            Scope="REGIONAL",
            Capacity=100,
            VisibilityConfig={
                "SampledRequestsEnabled": True,
                "CloudWatchMetricsEnabled": True,
                "MetricName": name,
            },
        )
        summary = resp["Summary"]
        assert summary["Name"] == name
        assert "Id" in summary

        get_resp = wafv2.get_rule_group(Name=name, Scope="REGIONAL", Id=summary["Id"])
        rg = get_resp["RuleGroup"]
        assert rg["Name"] == name
        assert rg["Capacity"] == 100

        # Cleanup
        wafv2.delete_rule_group(
            Name=name, Scope="REGIONAL", Id=summary["Id"], LockToken=get_resp["LockToken"]
        )

    def test_list_rule_groups(self, wafv2):
        name = _unique("rulegroup")
        resp = wafv2.create_rule_group(
            Name=name,
            Scope="REGIONAL",
            Capacity=50,
            VisibilityConfig={
                "SampledRequestsEnabled": True,
                "CloudWatchMetricsEnabled": True,
                "MetricName": name,
            },
        )
        summary = resp["Summary"]

        listed = wafv2.list_rule_groups(Scope="REGIONAL")
        names = [s["Name"] for s in listed["RuleGroups"]]
        assert name in names

        # Cleanup
        get_resp = wafv2.get_rule_group(Name=name, Scope="REGIONAL", Id=summary["Id"])
        wafv2.delete_rule_group(
            Name=name, Scope="REGIONAL", Id=summary["Id"], LockToken=get_resp["LockToken"]
        )

    def test_update_rule_group(self, wafv2):
        name = _unique("rulegroup")
        resp = wafv2.create_rule_group(
            Name=name,
            Scope="REGIONAL",
            Capacity=100,
            VisibilityConfig={
                "SampledRequestsEnabled": True,
                "CloudWatchMetricsEnabled": True,
                "MetricName": name,
            },
        )
        summary = resp["Summary"]

        get_resp = wafv2.get_rule_group(Name=name, Scope="REGIONAL", Id=summary["Id"])
        update_resp = wafv2.update_rule_group(
            Name=name,
            Scope="REGIONAL",
            Id=summary["Id"],
            LockToken=get_resp["LockToken"],
            Rules=[
                {
                    "Name": "block-rule",
                    "Priority": 1,
                    "Statement": {
                        "ByteMatchStatement": {
                            "SearchString": b"bad",
                            "FieldToMatch": {"UriPath": {}},
                            "TextTransformations": [{"Priority": 0, "Type": "NONE"}],
                            "PositionalConstraint": "CONTAINS",
                        }
                    },
                    "Action": {"Block": {}},
                    "VisibilityConfig": {
                        "SampledRequestsEnabled": True,
                        "CloudWatchMetricsEnabled": True,
                        "MetricName": "block-rule",
                    },
                }
            ],
            VisibilityConfig={
                "SampledRequestsEnabled": True,
                "CloudWatchMetricsEnabled": True,
                "MetricName": name,
            },
        )
        assert "NextLockToken" in update_resp

        # Verify update
        get_resp2 = wafv2.get_rule_group(Name=name, Scope="REGIONAL", Id=summary["Id"])
        assert len(get_resp2["RuleGroup"]["Rules"]) == 1
        assert get_resp2["RuleGroup"]["Rules"][0]["Name"] == "block-rule"

        # Cleanup
        wafv2.delete_rule_group(
            Name=name, Scope="REGIONAL", Id=summary["Id"], LockToken=get_resp2["LockToken"]
        )

    def test_delete_rule_group(self, wafv2):
        name = _unique("rulegroup")
        resp = wafv2.create_rule_group(
            Name=name,
            Scope="REGIONAL",
            Capacity=25,
            VisibilityConfig={
                "SampledRequestsEnabled": True,
                "CloudWatchMetricsEnabled": True,
                "MetricName": name,
            },
        )
        summary = resp["Summary"]

        get_resp = wafv2.get_rule_group(Name=name, Scope="REGIONAL", Id=summary["Id"])
        wafv2.delete_rule_group(
            Name=name, Scope="REGIONAL", Id=summary["Id"], LockToken=get_resp["LockToken"]
        )

        with pytest.raises(ClientError) as exc:
            wafv2.get_rule_group(Name=name, Scope="REGIONAL", Id=summary["Id"])
        assert exc.value.response["Error"]["Code"] == "WAFNonexistentItemException"


class TestWAFv2IPSetTagging:
    """Tests for TagResource/UntagResource/ListTagsForResource on IP sets."""

    def test_tag_and_untag_ip_set(self, wafv2):
        name = _unique("tag-ipset")
        resp = wafv2.create_ip_set(
            Name=name,
            Scope="REGIONAL",
            IPAddressVersion="IPV4",
            Addresses=["10.0.0.0/8"],
        )
        summary = resp["Summary"]
        ip_arn = summary["ARN"]
        try:
            wafv2.tag_resource(
                ResourceARN=ip_arn,
                Tags=[{"Key": "env", "Value": "test"}, {"Key": "team", "Value": "sec"}],
            )
            tags_resp = wafv2.list_tags_for_resource(ResourceARN=ip_arn)
            tag_map = {t["Key"]: t["Value"] for t in tags_resp["TagInfoForResource"]["TagList"]}
            assert tag_map["env"] == "test"
            assert tag_map["team"] == "sec"

            wafv2.untag_resource(ResourceARN=ip_arn, TagKeys=["env"])
            tags_resp2 = wafv2.list_tags_for_resource(ResourceARN=ip_arn)
            keys = [t["Key"] for t in tags_resp2["TagInfoForResource"]["TagList"]]
            assert "env" not in keys
            assert "team" in keys
        finally:
            get_resp = wafv2.get_ip_set(Name=name, Scope="REGIONAL", Id=summary["Id"])
            wafv2.delete_ip_set(
                Name=name,
                Scope="REGIONAL",
                Id=summary["Id"],
                LockToken=get_resp["LockToken"],
            )

    def test_create_ip_set_with_tags(self, wafv2):
        name = _unique("tagged-ipset")
        resp = wafv2.create_ip_set(
            Name=name,
            Scope="REGIONAL",
            IPAddressVersion="IPV4",
            Addresses=["172.16.0.0/12"],
            Tags=[{"Key": "created-by", "Value": "compat-test"}],
        )
        summary = resp["Summary"]
        try:
            tags = wafv2.list_tags_for_resource(ResourceARN=summary["ARN"])
            tag_map = {t["Key"]: t["Value"] for t in tags["TagInfoForResource"]["TagList"]}
            assert tag_map["created-by"] == "compat-test"
        finally:
            get_resp = wafv2.get_ip_set(Name=name, Scope="REGIONAL", Id=summary["Id"])
            wafv2.delete_ip_set(
                Name=name,
                Scope="REGIONAL",
                Id=summary["Id"],
                LockToken=get_resp["LockToken"],
            )


class TestWAFv2RegexPatternSetTagging:
    """Tests for tagging on regex pattern sets."""

    def test_tag_and_untag_regex_pattern_set(self, wafv2):
        name = _unique("tag-regex")
        resp = wafv2.create_regex_pattern_set(
            Name=name,
            Scope="REGIONAL",
            RegularExpressionList=[{"RegexString": "^test"}],
        )
        summary = resp["Summary"]
        regex_arn = summary["ARN"]
        try:
            wafv2.tag_resource(
                ResourceARN=regex_arn,
                Tags=[{"Key": "team", "Value": "security"}],
            )
            tags = wafv2.list_tags_for_resource(ResourceARN=regex_arn)
            tag_map = {t["Key"]: t["Value"] for t in tags["TagInfoForResource"]["TagList"]}
            assert tag_map["team"] == "security"

            wafv2.untag_resource(ResourceARN=regex_arn, TagKeys=["team"])
            tags2 = wafv2.list_tags_for_resource(ResourceARN=regex_arn)
            assert len(tags2["TagInfoForResource"]["TagList"]) == 0
        finally:
            get_resp = wafv2.get_regex_pattern_set(Name=name, Scope="REGIONAL", Id=summary["Id"])
            wafv2.delete_regex_pattern_set(
                Name=name,
                Scope="REGIONAL",
                Id=summary["Id"],
                LockToken=get_resp["LockToken"],
            )

    def test_create_regex_pattern_set_with_tags(self, wafv2):
        name = _unique("tagged-regex")
        resp = wafv2.create_regex_pattern_set(
            Name=name,
            Scope="REGIONAL",
            RegularExpressionList=[{"RegexString": ".*"}],
            Tags=[{"Key": "purpose", "Value": "testing"}],
        )
        summary = resp["Summary"]
        try:
            tags = wafv2.list_tags_for_resource(ResourceARN=summary["ARN"])
            tag_map = {t["Key"]: t["Value"] for t in tags["TagInfoForResource"]["TagList"]}
            assert tag_map["purpose"] == "testing"
        finally:
            get_resp = wafv2.get_regex_pattern_set(Name=name, Scope="REGIONAL", Id=summary["Id"])
            wafv2.delete_regex_pattern_set(
                Name=name,
                Scope="REGIONAL",
                Id=summary["Id"],
                LockToken=get_resp["LockToken"],
            )


class TestWAFv2RuleGroupTagging:
    """Tests for tagging on rule groups."""

    def test_tag_and_untag_rule_group(self, wafv2):
        name = _unique("tag-rg")
        resp = wafv2.create_rule_group(
            Name=name,
            Scope="REGIONAL",
            Capacity=100,
            VisibilityConfig={
                "SampledRequestsEnabled": True,
                "CloudWatchMetricsEnabled": True,
                "MetricName": name,
            },
        )
        summary = resp["Summary"]
        rg_arn = summary["ARN"]
        try:
            wafv2.tag_resource(
                ResourceARN=rg_arn,
                Tags=[{"Key": "dept", "Value": "eng"}, {"Key": "env", "Value": "staging"}],
            )
            tags = wafv2.list_tags_for_resource(ResourceARN=rg_arn)
            tag_map = {t["Key"]: t["Value"] for t in tags["TagInfoForResource"]["TagList"]}
            assert tag_map["dept"] == "eng"
            assert tag_map["env"] == "staging"

            wafv2.untag_resource(ResourceARN=rg_arn, TagKeys=["dept"])
            tags2 = wafv2.list_tags_for_resource(ResourceARN=rg_arn)
            keys = [t["Key"] for t in tags2["TagInfoForResource"]["TagList"]]
            assert "dept" not in keys
            assert "env" in keys
        finally:
            get_resp = wafv2.get_rule_group(Name=name, Scope="REGIONAL", Id=summary["Id"])
            wafv2.delete_rule_group(
                Name=name,
                Scope="REGIONAL",
                Id=summary["Id"],
                LockToken=get_resp["LockToken"],
            )

    def test_create_rule_group_with_tags(self, wafv2):
        name = _unique("tagged-rg")
        resp = wafv2.create_rule_group(
            Name=name,
            Scope="REGIONAL",
            Capacity=50,
            VisibilityConfig={
                "SampledRequestsEnabled": True,
                "CloudWatchMetricsEnabled": True,
                "MetricName": name,
            },
            Tags=[{"Key": "managed", "Value": "true"}],
        )
        summary = resp["Summary"]
        try:
            tags = wafv2.list_tags_for_resource(ResourceARN=summary["ARN"])
            tag_map = {t["Key"]: t["Value"] for t in tags["TagInfoForResource"]["TagList"]}
            assert tag_map["managed"] == "true"
        finally:
            get_resp = wafv2.get_rule_group(Name=name, Scope="REGIONAL", Id=summary["Id"])
            wafv2.delete_rule_group(
                Name=name,
                Scope="REGIONAL",
                Id=summary["Id"],
                LockToken=get_resp["LockToken"],
            )


class TestWAFv2PermissionPolicy:
    """Tests for PutPermissionPolicy, GetPermissionPolicy, DeletePermissionPolicy."""

    def test_put_get_delete_permission_policy(self, wafv2):
        import json

        name = _unique("pp-rg")
        resp = wafv2.create_rule_group(
            Name=name,
            Scope="REGIONAL",
            Capacity=100,
            VisibilityConfig={
                "SampledRequestsEnabled": True,
                "CloudWatchMetricsEnabled": True,
                "MetricName": name,
            },
        )
        summary = resp["Summary"]
        rg_arn = summary["ARN"]
        try:
            policy = json.dumps(
                {
                    "Version": "2012-10-17",
                    "Statement": [
                        {
                            "Sid": "AllowAccess",
                            "Effect": "Allow",
                            "Principal": {"AWS": "arn:aws:iam::123456789012:root"},
                            "Action": "wafv2:GetRuleGroup",
                            "Resource": rg_arn,
                        }
                    ],
                }
            )
            put_resp = wafv2.put_permission_policy(ResourceArn=rg_arn, Policy=policy)
            assert put_resp["ResponseMetadata"]["HTTPStatusCode"] == 200

            get_resp = wafv2.get_permission_policy(ResourceArn=rg_arn)
            assert "Policy" in get_resp
            parsed = json.loads(get_resp["Policy"])
            assert parsed["Statement"][0]["Sid"] == "AllowAccess"

            del_resp = wafv2.delete_permission_policy(ResourceArn=rg_arn)
            assert del_resp["ResponseMetadata"]["HTTPStatusCode"] == 200

            with pytest.raises(ClientError) as exc:
                wafv2.get_permission_policy(ResourceArn=rg_arn)
            assert exc.value.response["Error"]["Code"] in (
                "WAFNonexistentItemException",
                "WAFInternalErrorException",
            )
        finally:
            get_rg = wafv2.get_rule_group(Name=name, Scope="REGIONAL", Id=summary["Id"])
            wafv2.delete_rule_group(
                Name=name,
                Scope="REGIONAL",
                Id=summary["Id"],
                LockToken=get_rg["LockToken"],
            )


class TestWAFv2CheckCapacity:
    """Test CheckCapacity operation."""

    def test_check_capacity_with_byte_match(self, wafv2):
        resp = wafv2.check_capacity(
            Scope="REGIONAL",
            Rules=[
                {
                    "Name": "block-bad",
                    "Priority": 1,
                    "Statement": {
                        "ByteMatchStatement": {
                            "SearchString": b"bad",
                            "FieldToMatch": {"UriPath": {}},
                            "TextTransformations": [{"Priority": 0, "Type": "NONE"}],
                            "PositionalConstraint": "CONTAINS",
                        }
                    },
                    "Action": {"Block": {}},
                    "VisibilityConfig": {
                        "SampledRequestsEnabled": True,
                        "CloudWatchMetricsEnabled": True,
                        "MetricName": "block-bad",
                    },
                }
            ],
        )
        assert "Capacity" in resp
        assert resp["Capacity"] >= 1


class TestWAFv2AdditionalOperations:
    """Tests for DescribeManagedRuleGroup and GetPermissionPolicy."""

    def test_describe_managed_rule_group(self, wafv2):
        """DescribeManagedRuleGroup returns info about an AWS managed rule group."""
        resp = wafv2.describe_managed_rule_group(
            VendorName="AWS",
            Name="AWSManagedRulesCommonRuleSet",
            Scope="REGIONAL",
        )
        assert "Capacity" in resp
        assert "Rules" in resp
        assert isinstance(resp["Rules"], list)

    def test_get_permission_policy_nonexistent(self, wafv2):
        """GetPermissionPolicy on a nonexistent ARN returns error."""
        with pytest.raises(ClientError) as exc:
            wafv2.get_permission_policy(
                ResourceArn="arn:aws:wafv2:us-east-1:123456789012:regional/rulegroup/nonexistent/fake-id"
            )
        assert exc.value.response["Error"]["Code"] in (
            "WAFNonexistentItemException",
            "WAFInvalidParameterException",
        )
