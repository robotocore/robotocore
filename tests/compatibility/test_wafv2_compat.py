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
