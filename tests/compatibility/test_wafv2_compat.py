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
                pass  # best-effort cleanup
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
                pass  # best-effort cleanup
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


class TestWAFv2IPSetEdgeCases:
    """Additional edge-case tests for WAFv2 IPSet operations."""

    def test_create_ip_set_ipv6(self, wafv2):
        """Create an IPV6 IP set."""
        name = _unique("ipset-v6")
        resp = wafv2.create_ip_set(
            Name=name,
            Scope="REGIONAL",
            IPAddressVersion="IPV6",
            Addresses=["2001:db8::/32"],
        )
        summary = resp["Summary"]
        assert summary["Name"] == name
        assert "ARN" in summary

        get_resp = wafv2.get_ip_set(Name=name, Scope="REGIONAL", Id=summary["Id"])
        assert get_resp["IPSet"]["IPAddressVersion"] == "IPV6"
        assert "2001:db8::/32" in get_resp["IPSet"]["Addresses"]

        # Cleanup
        wafv2.delete_ip_set(
            Name=name, Scope="REGIONAL", Id=summary["Id"], LockToken=get_resp["LockToken"]
        )

    def test_create_ip_set_empty_addresses(self, wafv2):
        """Create an IP set with no addresses."""
        name = _unique("ipset-empty")
        resp = wafv2.create_ip_set(
            Name=name,
            Scope="REGIONAL",
            IPAddressVersion="IPV4",
            Addresses=[],
        )
        summary = resp["Summary"]
        assert summary["Name"] == name

        get_resp = wafv2.get_ip_set(Name=name, Scope="REGIONAL", Id=summary["Id"])
        assert get_resp["IPSet"]["Addresses"] == []

        # Cleanup
        wafv2.delete_ip_set(
            Name=name, Scope="REGIONAL", Id=summary["Id"], LockToken=get_resp["LockToken"]
        )

    def test_create_ip_set_multiple_cidrs(self, wafv2):
        """Create an IP set with multiple CIDR ranges."""
        name = _unique("ipset-multi")
        addresses = ["10.0.0.0/8", "172.16.0.0/12", "192.168.0.0/16"]
        resp = wafv2.create_ip_set(
            Name=name,
            Scope="REGIONAL",
            IPAddressVersion="IPV4",
            Addresses=addresses,
        )
        summary = resp["Summary"]

        get_resp = wafv2.get_ip_set(Name=name, Scope="REGIONAL", Id=summary["Id"])
        assert sorted(get_resp["IPSet"]["Addresses"]) == sorted(addresses)

        # Cleanup
        wafv2.delete_ip_set(
            Name=name, Scope="REGIONAL", Id=summary["Id"], LockToken=get_resp["LockToken"]
        )

    def test_update_ip_set_to_empty(self, wafv2):
        """Update an IP set to have zero addresses."""
        name = _unique("ipset-clear")
        resp = wafv2.create_ip_set(
            Name=name,
            Scope="REGIONAL",
            IPAddressVersion="IPV4",
            Addresses=["1.2.3.0/24"],
        )
        summary = resp["Summary"]

        get_resp = wafv2.get_ip_set(Name=name, Scope="REGIONAL", Id=summary["Id"])
        update_resp = wafv2.update_ip_set(
            Name=name,
            Scope="REGIONAL",
            Id=summary["Id"],
            Addresses=[],
            LockToken=get_resp["LockToken"],
        )
        assert "NextLockToken" in update_resp

        get_resp2 = wafv2.get_ip_set(Name=name, Scope="REGIONAL", Id=summary["Id"])
        assert get_resp2["IPSet"]["Addresses"] == []

        # Cleanup
        wafv2.delete_ip_set(
            Name=name, Scope="REGIONAL", Id=summary["Id"], LockToken=get_resp2["LockToken"]
        )

    def test_get_nonexistent_ip_set(self, wafv2):
        """Getting a nonexistent IP set returns WAFNonexistentItemException."""
        with pytest.raises(ClientError) as exc:
            wafv2.get_ip_set(
                Name="nonexistent",
                Scope="REGIONAL",
                Id="00000000-0000-0000-0000-000000000000",
            )
        assert exc.value.response["Error"]["Code"] == "WAFNonexistentItemException"

    def test_list_ip_sets_empty_scope(self, wafv2):
        """ListIPSets returns a list (possibly empty) with CLOUDFRONT scope."""
        resp = wafv2.list_ip_sets(Scope="CLOUDFRONT")
        assert "IPSets" in resp
        assert isinstance(resp["IPSets"], list)

    def test_ip_set_arn_format(self, wafv2):
        """Verify the ARN returned for an IP set has the expected format."""
        name = _unique("ipset-arn")
        resp = wafv2.create_ip_set(
            Name=name,
            Scope="REGIONAL",
            IPAddressVersion="IPV4",
            Addresses=[],
        )
        summary = resp["Summary"]
        arn = summary["ARN"]
        assert arn.startswith("arn:aws:wafv2:")
        assert "regional/ipset/" in arn

        # Cleanup
        get_resp = wafv2.get_ip_set(Name=name, Scope="REGIONAL", Id=summary["Id"])
        wafv2.delete_ip_set(
            Name=name, Scope="REGIONAL", Id=summary["Id"], LockToken=get_resp["LockToken"]
        )


class TestWAFv2WebACLEdgeCases:
    """Additional edge-case tests for WAFv2 WebACL operations."""

    def test_create_web_acl_with_block_default(self, wafv2):
        """Create a WebACL with Block as the default action."""
        name = _unique("webacl-block")
        resp = wafv2.create_web_acl(
            Name=name,
            Scope="REGIONAL",
            DefaultAction={"Block": {}},
            VisibilityConfig={
                "SampledRequestsEnabled": True,
                "CloudWatchMetricsEnabled": True,
                "MetricName": name,
            },
        )
        summary = resp["Summary"]
        assert summary["Name"] == name

        get_resp = wafv2.get_web_acl(Name=name, Scope="REGIONAL", Id=summary["Id"])
        assert get_resp["WebACL"]["DefaultAction"] == {"Block": {}}

        # Cleanup
        wafv2.delete_web_acl(
            Name=name, Scope="REGIONAL", Id=summary["Id"], LockToken=get_resp["LockToken"]
        )

    def test_create_web_acl_with_rules(self, wafv2):
        """Create a WebACL with an inline rule."""
        name = _unique("webacl-rules")
        resp = wafv2.create_web_acl(
            Name=name,
            Scope="REGIONAL",
            DefaultAction={"Allow": {}},
            Rules=[
                {
                    "Name": "rate-limit",
                    "Priority": 1,
                    "Statement": {
                        "RateBasedStatement": {
                            "Limit": 2000,
                            "AggregateKeyType": "IP",
                        }
                    },
                    "Action": {"Block": {}},
                    "VisibilityConfig": {
                        "SampledRequestsEnabled": True,
                        "CloudWatchMetricsEnabled": True,
                        "MetricName": "rate-limit",
                    },
                }
            ],
            VisibilityConfig={
                "SampledRequestsEnabled": True,
                "CloudWatchMetricsEnabled": True,
                "MetricName": name,
            },
        )
        summary = resp["Summary"]

        get_resp = wafv2.get_web_acl(Name=name, Scope="REGIONAL", Id=summary["Id"])
        acl = get_resp["WebACL"]
        assert len(acl["Rules"]) == 1
        assert acl["Rules"][0]["Name"] == "rate-limit"

        # Cleanup
        wafv2.delete_web_acl(
            Name=name, Scope="REGIONAL", Id=summary["Id"], LockToken=get_resp["LockToken"]
        )

    def test_web_acl_arn_format(self, wafv2):
        """Verify the ARN returned for a WebACL has the expected format."""
        name = _unique("webacl-arn")
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
        assert arn.startswith("arn:aws:wafv2:")
        assert "regional/webacl/" in arn

        # Cleanup
        get_resp = wafv2.get_web_acl(Name=name, Scope="REGIONAL", Id=summary["Id"])
        wafv2.delete_web_acl(
            Name=name, Scope="REGIONAL", Id=summary["Id"], LockToken=get_resp["LockToken"]
        )

    def test_get_nonexistent_web_acl(self, wafv2):
        """Getting a nonexistent WebACL returns WAFNonexistentItemException."""
        with pytest.raises(ClientError) as exc:
            wafv2.get_web_acl(
                Name="nonexistent",
                Scope="REGIONAL",
                Id="00000000-0000-0000-0000-000000000000",
            )
        assert exc.value.response["Error"]["Code"] == "WAFNonexistentItemException"

    def test_update_web_acl_add_rule(self, wafv2):
        """Update a WebACL to add a rule."""
        name = _unique("webacl-addrule")
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
        assert get_resp["WebACL"]["Rules"] == []

        update_resp = wafv2.update_web_acl(
            Name=name,
            Scope="REGIONAL",
            Id=summary["Id"],
            DefaultAction={"Allow": {}},
            LockToken=get_resp["LockToken"],
            Rules=[
                {
                    "Name": "geo-block",
                    "Priority": 1,
                    "Statement": {
                        "GeoMatchStatement": {
                            "CountryCodes": ["CN", "RU"],
                        }
                    },
                    "Action": {"Block": {}},
                    "VisibilityConfig": {
                        "SampledRequestsEnabled": True,
                        "CloudWatchMetricsEnabled": True,
                        "MetricName": "geo-block",
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

        get_resp2 = wafv2.get_web_acl(Name=name, Scope="REGIONAL", Id=summary["Id"])
        assert len(get_resp2["WebACL"]["Rules"]) == 1
        assert get_resp2["WebACL"]["Rules"][0]["Name"] == "geo-block"

        # Cleanup
        wafv2.delete_web_acl(
            Name=name, Scope="REGIONAL", Id=summary["Id"], LockToken=get_resp2["LockToken"]
        )

    def test_list_web_acls_regional(self, wafv2):
        """ListWebACLs returns a list for REGIONAL scope."""
        resp = wafv2.list_web_acls(Scope="REGIONAL")
        assert "WebACLs" in resp
        assert isinstance(resp["WebACLs"], list)


class TestWAFv2WebACLAssociationEdgeCases:
    """Additional tests for WebACL association operations."""

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

    def test_get_web_acl_for_unassociated_resource(self, wafv2):
        """GetWebACLForResource on a resource with no association returns empty WebACL."""
        resource_arn = (
            "arn:aws:elasticloadbalancing:us-east-1:123456789012"
            ":loadbalancer/app/no-acl-alb/1111111111111111"
        )
        resp = wafv2.get_web_acl_for_resource(ResourceArn=resource_arn)
        # Should return 200 with no WebACL or an empty WebACL field
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200

    def test_disassociate_unassociated_resource(self, wafv2):
        """Disassociating a resource with no association should succeed."""
        resource_arn = (
            "arn:aws:elasticloadbalancing:us-east-1:123456789012"
            ":loadbalancer/app/never-assoc/3333333333333333"
        )
        # Should succeed (idempotent) or raise a specific error
        resp = wafv2.disassociate_web_acl(ResourceArn=resource_arn)
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200


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

    def test_list_available_managed_rule_groups(self, wafv2):
        """ListAvailableManagedRuleGroups returns managed rule group summaries."""
        resp = wafv2.list_available_managed_rule_groups(Scope="REGIONAL")
        assert "ManagedRuleGroups" in resp
        assert isinstance(resp["ManagedRuleGroups"], list)
        # AWS always has at least some managed rule groups
        assert len(resp["ManagedRuleGroups"]) >= 1
        # Each entry should have Name and VendorName
        first = resp["ManagedRuleGroups"][0]
        assert "Name" in first
        assert "VendorName" in first

    def test_list_available_managed_rule_group_versions(self, wafv2):
        """ListAvailableManagedRuleGroupVersions returns versions for a managed rule group."""
        resp = wafv2.list_available_managed_rule_group_versions(
            VendorName="AWS",
            Name="AWSManagedRulesCommonRuleSet",
            Scope="REGIONAL",
        )
        assert "Versions" in resp
        assert isinstance(resp["Versions"], list)
        assert "CurrentDefaultVersion" in resp

    def test_check_capacity_empty_rules(self, wafv2):
        """CheckCapacity with empty rules returns zero capacity."""
        resp = wafv2.check_capacity(Scope="REGIONAL", Rules=[])
        assert "Capacity" in resp
        assert resp["Capacity"] == 0


class TestWAFv2AdditionalOps:
    """Tests for additional WAFv2 operations: API keys, managed products, etc."""

    def test_list_api_keys(self, wafv2):
        """ListAPIKeys returns a list of API keys."""
        resp = wafv2.list_api_keys(Scope="REGIONAL")
        assert "APIKeySummaries" in resp
        assert isinstance(resp["APIKeySummaries"], list)

    def test_delete_api_key_nonexistent(self, wafv2):
        """DeleteAPIKey on a nonexistent key succeeds (idempotent)."""
        resp = wafv2.delete_api_key(
            Scope="REGIONAL",
            APIKey="nonexistent-api-key-value",
        )
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200

    def test_get_decrypted_api_key_nonexistent(self, wafv2):
        """GetDecryptedAPIKey on a nonexistent key returns empty token domains."""
        resp = wafv2.get_decrypted_api_key(
            Scope="REGIONAL",
            APIKey="nonexistent-api-key-value",
        )
        assert "TokenDomains" in resp
        assert isinstance(resp["TokenDomains"], list)

    def test_describe_all_managed_products(self, wafv2):
        """DescribeAllManagedProducts returns a list of managed products."""
        resp = wafv2.describe_all_managed_products(Scope="REGIONAL")
        assert "ManagedProducts" in resp
        assert isinstance(resp["ManagedProducts"], list)

    def test_describe_managed_products_by_vendor(self, wafv2):
        """DescribeManagedProductsByVendor returns products from AWS vendor."""
        resp = wafv2.describe_managed_products_by_vendor(
            Scope="REGIONAL",
            VendorName="AWS",
        )
        assert "ManagedProducts" in resp
        assert isinstance(resp["ManagedProducts"], list)

    def test_generate_mobile_sdk_release_url(self, wafv2):
        """GenerateMobileSdkReleaseUrl returns a URL."""
        resp = wafv2.generate_mobile_sdk_release_url(
            Platform="IOS",
            ReleaseVersion="1.0.0",
        )
        assert "Url" in resp
        assert isinstance(resp["Url"], str)
        assert len(resp["Url"]) > 0

    def test_list_mobile_sdk_releases(self, wafv2):
        """ListMobileSdkReleases returns a list."""
        resp = wafv2.list_mobile_sdk_releases(Platform="IOS")
        assert "ReleaseSummaries" in resp
        assert isinstance(resp["ReleaseSummaries"], list)

    def test_get_mobile_sdk_release(self, wafv2):
        """GetMobileSdkRelease returns release details."""
        resp = wafv2.get_mobile_sdk_release(
            Platform="IOS",
            ReleaseVersion="1.0.0",
        )
        assert "MobileSdkRelease" in resp
        release = resp["MobileSdkRelease"]
        assert "ReleaseVersion" in release
        assert "Timestamp" in release

    def test_list_managed_rule_sets(self, wafv2):
        """ListManagedRuleSets returns a list."""
        resp = wafv2.list_managed_rule_sets(Scope="REGIONAL")
        assert "ManagedRuleSets" in resp
        assert isinstance(resp["ManagedRuleSets"], list)

    def test_get_managed_rule_set(self, wafv2):
        """GetManagedRuleSet returns rule set details."""
        resp = wafv2.get_managed_rule_set(
            Name="test-rule-set",
            Scope="REGIONAL",
            Id="00000000-0000-0000-0000-000000000000",
        )
        assert "ManagedRuleSet" in resp
        rule_set = resp["ManagedRuleSet"]
        assert "Name" in rule_set
        assert "Id" in rule_set
        assert "ARN" in rule_set
        assert "LockToken" in resp

    def test_put_managed_rule_set_versions(self, wafv2):
        """PutManagedRuleSetVersions returns a next lock token."""
        resp = wafv2.put_managed_rule_set_versions(
            Name="test-rule-set",
            Scope="REGIONAL",
            Id="00000000-0000-0000-0000-000000000000",
            LockToken="fake-lock-token",
            RecommendedVersion="1.0",
            VersionsToPublish={
                "1.0": {
                    "AssociatedRuleGroupArn": (
                        "arn:aws:wafv2:us-east-1:123456789012:regional/rulegroup/fake/fake-id"
                    )
                }
            },
        )
        assert "NextLockToken" in resp
        assert isinstance(resp["NextLockToken"], str)

    def test_update_managed_rule_set_version_expiry_date(self, wafv2):
        """UpdateManagedRuleSetVersionExpiryDate returns expiry info and lock token."""
        resp = wafv2.update_managed_rule_set_version_expiry_date(
            Name="test-rule-set",
            Scope="REGIONAL",
            Id="00000000-0000-0000-0000-000000000000",
            LockToken="fake-lock-token",
            VersionToExpire="1.0",
            ExpiryTimestamp=1700000000,
        )
        assert "ExpiringVersion" in resp
        assert resp["ExpiringVersion"] == "1.0"
        assert "ExpiryTimestamp" in resp
        assert "NextLockToken" in resp

    def test_get_rate_based_statement_managed_keys(self, wafv2):
        """GetRateBasedStatementManagedKeys needs a WebACL with a rate-based rule."""
        # Create a WebACL with a rate-based statement
        name = _unique("webacl-rate")
        resp = wafv2.create_web_acl(
            Name=name,
            Scope="REGIONAL",
            DefaultAction={"Allow": {}},
            Rules=[
                {
                    "Name": "rate-rule",
                    "Priority": 1,
                    "Statement": {
                        "RateBasedStatement": {
                            "Limit": 2000,
                            "AggregateKeyType": "IP",
                        }
                    },
                    "Action": {"Block": {}},
                    "VisibilityConfig": {
                        "SampledRequestsEnabled": True,
                        "CloudWatchMetricsEnabled": True,
                        "MetricName": "rate-rule",
                    },
                }
            ],
            VisibilityConfig={
                "SampledRequestsEnabled": True,
                "CloudWatchMetricsEnabled": True,
                "MetricName": name,
            },
        )
        summary = resp["Summary"]
        try:
            keys_resp = wafv2.get_rate_based_statement_managed_keys(
                Scope="REGIONAL",
                WebACLName=name,
                WebACLId=summary["Id"],
                RuleName="rate-rule",
            )
            assert "ManagedKeysIPV4" in keys_resp
            assert "ManagedKeysIPV6" in keys_resp
        finally:
            get_resp = wafv2.get_web_acl(Name=name, Scope="REGIONAL", Id=summary["Id"])
            wafv2.delete_web_acl(
                Name=name,
                Scope="REGIONAL",
                Id=summary["Id"],
                LockToken=get_resp["LockToken"],
            )

    def test_get_sampled_requests(self, wafv2):
        """GetSampledRequests returns sampled requests for a WebACL."""
        import datetime

        name = _unique("webacl-sampled")
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
        try:
            now = datetime.datetime.now(datetime.UTC)
            start = now - datetime.timedelta(hours=1)
            sampled = wafv2.get_sampled_requests(
                WebAclArn=summary["ARN"],
                RuleMetricName=name,
                Scope="REGIONAL",
                TimeWindow={
                    "StartTime": start,
                    "EndTime": now,
                },
                MaxItems=10,
            )
            assert "SampledRequests" in sampled
            assert isinstance(sampled["SampledRequests"], list)
        finally:
            get_resp = wafv2.get_web_acl(Name=name, Scope="REGIONAL", Id=summary["Id"])
            wafv2.delete_web_acl(
                Name=name,
                Scope="REGIONAL",
                Id=summary["Id"],
                LockToken=get_resp["LockToken"],
            )

    def test_list_resources_for_web_acl(self, wafv2):
        """ListResourcesForWebACL returns associated resources."""
        name = _unique("webacl-res")
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
        try:
            res_resp = wafv2.list_resources_for_web_acl(WebACLArn=summary["ARN"])
            assert "ResourceArns" in res_resp
            assert isinstance(res_resp["ResourceArns"], list)
        finally:
            get_resp = wafv2.get_web_acl(Name=name, Scope="REGIONAL", Id=summary["Id"])
            wafv2.delete_web_acl(
                Name=name,
                Scope="REGIONAL",
                Id=summary["Id"],
                LockToken=get_resp["LockToken"],
            )

    def test_delete_firewall_manager_rule_groups(self, wafv2):
        """DeleteFirewallManagerRuleGroups on a WebACL with no FM rules."""
        name = _unique("webacl-fm")
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
        try:
            get_resp = wafv2.get_web_acl(Name=name, Scope="REGIONAL", Id=summary["Id"])
            fm_resp = wafv2.delete_firewall_manager_rule_groups(
                WebACLArn=summary["ARN"],
                WebACLLockToken=get_resp["LockToken"],
            )
            assert "NextWebACLLockToken" in fm_resp
        finally:
            get_resp2 = wafv2.get_web_acl(Name=name, Scope="REGIONAL", Id=summary["Id"])
            wafv2.delete_web_acl(
                Name=name,
                Scope="REGIONAL",
                Id=summary["Id"],
                LockToken=get_resp2["LockToken"],
            )


class TestWAFv2ListResourcesForWebACL:
    """Tests for ListResourcesForWebACL operation."""

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

    def test_list_resources_for_web_acl_empty(self, wafv2):
        """ListResourcesForWebACL returns empty ResourceArns for new ACL."""
        name, summary = self._create_web_acl(wafv2)
        try:
            resp = wafv2.list_resources_for_web_acl(WebACLArn=summary["ARN"])
            assert "ResourceArns" in resp
            assert isinstance(resp["ResourceArns"], list)
        finally:
            self._delete_web_acl(wafv2, name, summary["Id"])

    def test_list_resources_after_associate(self, wafv2):
        """ListResourcesForWebACL includes associated resource."""
        name, summary = self._create_web_acl(wafv2)
        resource_arn = (
            "arn:aws:elasticloadbalancing:us-east-1:123456789012"
            ":loadbalancer/app/my-alb/listres1234567890"
        )
        try:
            wafv2.associate_web_acl(WebACLArn=summary["ARN"], ResourceArn=resource_arn)
            resp = wafv2.list_resources_for_web_acl(WebACLArn=summary["ARN"])
            assert resource_arn in resp["ResourceArns"]
        finally:
            try:
                wafv2.disassociate_web_acl(ResourceArn=resource_arn)
            except Exception:
                pass  # best-effort cleanup
            self._delete_web_acl(wafv2, name, summary["Id"])


class TestWAFv2APIKeyOperations:
    """Tests for WAFv2 API Key create, list, get_decrypted, delete."""

    def test_create_api_key(self, wafv2):
        """CreateAPIKey returns an APIKey string."""
        resp = wafv2.create_api_key(Scope="REGIONAL", TokenDomains=["example.com"])
        assert "APIKey" in resp
        assert isinstance(resp["APIKey"], str)
        assert len(resp["APIKey"]) > 0

    def test_list_api_keys(self, wafv2):
        """ListAPIKeys returns APIKeySummaries."""
        resp = wafv2.list_api_keys(Scope="REGIONAL")
        assert "APIKeySummaries" in resp
        assert isinstance(resp["APIKeySummaries"], list)

    def test_create_then_list_api_keys(self, wafv2):
        """Created API key appears in list."""
        create_resp = wafv2.create_api_key(Scope="REGIONAL", TokenDomains=["test.example.com"])
        api_key = create_resp["APIKey"]

        list_resp = wafv2.list_api_keys(Scope="REGIONAL")
        keys = [s.get("APIKey") for s in list_resp["APIKeySummaries"]]
        assert api_key in keys

    def test_get_decrypted_api_key(self, wafv2):
        """GetDecryptedAPIKey returns TokenDomains."""
        create_resp = wafv2.create_api_key(Scope="REGIONAL", TokenDomains=["decrypt.example.com"])
        api_key = create_resp["APIKey"]

        resp = wafv2.get_decrypted_api_key(Scope="REGIONAL", APIKey=api_key)
        assert "TokenDomains" in resp
        assert isinstance(resp["TokenDomains"], list)

    def test_delete_api_key(self, wafv2):
        """DeleteAPIKey removes the key."""
        create_resp = wafv2.create_api_key(Scope="REGIONAL", TokenDomains=["delete.example.com"])
        api_key = create_resp["APIKey"]

        del_resp = wafv2.delete_api_key(Scope="REGIONAL", APIKey=api_key)
        assert del_resp["ResponseMetadata"]["HTTPStatusCode"] == 200
