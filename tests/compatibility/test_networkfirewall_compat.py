"""Network Firewall compatibility tests."""

import uuid

import pytest
from botocore.exceptions import ClientError

from tests.compatibility.conftest import make_client


@pytest.fixture
def nfw():
    return make_client("network-firewall")


def _unique_name():
    return f"test-fw-{uuid.uuid4().hex[:8]}"


POLICY_ARN = "arn:aws:network-firewall:us-east-1:123456789012:firewall-policy/test"


class TestNetworkFirewallOperations:
    def test_create_firewall(self, nfw):
        """CreateFirewall returns Firewall and FirewallStatus."""
        name = _unique_name()
        resp = nfw.create_firewall(
            FirewallName=name,
            FirewallPolicyArn=POLICY_ARN,
            SubnetMappings=[{"SubnetId": "subnet-12345"}],
            VpcId="vpc-12345",
        )
        assert "Firewall" in resp
        assert "FirewallStatus" in resp
        fw = resp["Firewall"]
        assert fw["FirewallName"] == name
        assert fw["FirewallPolicyArn"] == POLICY_ARN
        assert fw["VpcId"] == "vpc-12345"
        assert fw["SubnetMappings"] == [{"SubnetId": "subnet-12345"}]

    def test_create_firewall_has_arn(self, nfw):
        """CreateFirewall returns a valid FirewallArn."""
        name = _unique_name()
        resp = nfw.create_firewall(
            FirewallName=name,
            FirewallPolicyArn=POLICY_ARN,
            SubnetMappings=[{"SubnetId": "subnet-12345"}],
            VpcId="vpc-12345",
        )
        arn = resp["Firewall"]["FirewallArn"]
        assert arn.startswith("arn:aws:network-firewall:")
        assert name in arn

    def test_create_firewall_status(self, nfw):
        """CreateFirewall returns a FirewallStatus with Status field."""
        name = _unique_name()
        resp = nfw.create_firewall(
            FirewallName=name,
            FirewallPolicyArn=POLICY_ARN,
            SubnetMappings=[{"SubnetId": "subnet-12345"}],
            VpcId="vpc-12345",
        )
        status = resp["FirewallStatus"]
        assert "Status" in status
        assert "ConfigurationSyncStateSummary" in status

    def test_create_firewall_protection_flags(self, nfw):
        """CreateFirewall returns protection flags."""
        name = _unique_name()
        resp = nfw.create_firewall(
            FirewallName=name,
            FirewallPolicyArn=POLICY_ARN,
            SubnetMappings=[{"SubnetId": "subnet-12345"}],
            VpcId="vpc-12345",
        )
        fw = resp["Firewall"]
        assert "DeleteProtection" in fw
        assert "SubnetChangeProtection" in fw
        assert "FirewallPolicyChangeProtection" in fw

    def test_describe_firewall_by_name(self, nfw):
        """DescribeFirewall by name returns the created firewall."""
        name = _unique_name()
        nfw.create_firewall(
            FirewallName=name,
            FirewallPolicyArn=POLICY_ARN,
            SubnetMappings=[{"SubnetId": "subnet-12345"}],
            VpcId="vpc-12345",
        )
        resp = nfw.describe_firewall(FirewallName=name)
        assert "Firewall" in resp
        assert "FirewallStatus" in resp
        assert "UpdateToken" in resp
        assert resp["Firewall"]["FirewallName"] == name

    def test_describe_firewall_by_arn(self, nfw):
        """DescribeFirewall by ARN returns the created firewall."""
        name = _unique_name()
        create_resp = nfw.create_firewall(
            FirewallName=name,
            FirewallPolicyArn=POLICY_ARN,
            SubnetMappings=[{"SubnetId": "subnet-12345"}],
            VpcId="vpc-12345",
        )
        arn = create_resp["Firewall"]["FirewallArn"]
        resp = nfw.describe_firewall(FirewallArn=arn)
        assert resp["Firewall"]["FirewallArn"] == arn
        assert resp["Firewall"]["FirewallName"] == name

    def test_describe_firewall_returns_all_fields(self, nfw):
        """DescribeFirewall returns VpcId, SubnetMappings, and policy ARN."""
        name = _unique_name()
        nfw.create_firewall(
            FirewallName=name,
            FirewallPolicyArn=POLICY_ARN,
            SubnetMappings=[{"SubnetId": "subnet-99999"}],
            VpcId="vpc-99999",
        )
        resp = nfw.describe_firewall(FirewallName=name)
        fw = resp["Firewall"]
        assert fw["VpcId"] == "vpc-99999"
        assert fw["SubnetMappings"] == [{"SubnetId": "subnet-99999"}]
        assert fw["FirewallPolicyArn"] == POLICY_ARN

    def test_describe_firewall_nonexistent(self, nfw):
        """DescribeFirewall for a nonexistent firewall raises ResourceNotFoundException."""
        with pytest.raises(ClientError) as exc_info:
            nfw.describe_firewall(FirewallName="nonexistent-fw-" + uuid.uuid4().hex[:8])
        assert exc_info.value.response["Error"]["Code"] == "ResourceNotFoundException"

    def test_list_firewalls(self, nfw):
        """ListFirewalls returns a Firewalls list."""
        name = _unique_name()
        nfw.create_firewall(
            FirewallName=name,
            FirewallPolicyArn=POLICY_ARN,
            SubnetMappings=[{"SubnetId": "subnet-12345"}],
            VpcId="vpc-12345",
        )
        resp = nfw.list_firewalls()
        assert "Firewalls" in resp
        assert isinstance(resp["Firewalls"], list)
        names = [fw["FirewallName"] for fw in resp["Firewalls"]]
        assert name in names

    def test_list_firewalls_entry_has_arn(self, nfw):
        """Each entry in ListFirewalls has FirewallName and FirewallArn."""
        name = _unique_name()
        nfw.create_firewall(
            FirewallName=name,
            FirewallPolicyArn=POLICY_ARN,
            SubnetMappings=[{"SubnetId": "subnet-12345"}],
            VpcId="vpc-12345",
        )
        resp = nfw.list_firewalls()
        matching = [fw for fw in resp["Firewalls"] if fw["FirewallName"] == name]
        assert len(matching) == 1
        assert "FirewallArn" in matching[0]
        assert matching[0]["FirewallArn"].startswith("arn:aws:network-firewall:")

    def test_create_multiple_firewalls(self, nfw):
        """Creating multiple firewalls, all appear in list_firewalls."""
        names = [_unique_name() for _ in range(3)]
        for n in names:
            nfw.create_firewall(
                FirewallName=n,
                FirewallPolicyArn=POLICY_ARN,
                SubnetMappings=[{"SubnetId": "subnet-12345"}],
                VpcId="vpc-12345",
            )
        resp = nfw.list_firewalls()
        listed_names = {fw["FirewallName"] for fw in resp["Firewalls"]}
        for n in names:
            assert n in listed_names

    def test_describe_logging_configuration(self, nfw):
        """DescribeLoggingConfiguration returns FirewallArn and LoggingConfiguration."""
        name = _unique_name()
        create_resp = nfw.create_firewall(
            FirewallName=name,
            FirewallPolicyArn=POLICY_ARN,
            SubnetMappings=[{"SubnetId": "subnet-12345"}],
            VpcId="vpc-12345",
        )
        arn = create_resp["Firewall"]["FirewallArn"]
        resp = nfw.describe_logging_configuration(FirewallArn=arn)
        assert resp["FirewallArn"] == arn
        assert "LoggingConfiguration" in resp

    def test_update_logging_configuration(self, nfw):
        """UpdateLoggingConfiguration persists the log destination config."""
        name = _unique_name()
        create_resp = nfw.create_firewall(
            FirewallName=name,
            FirewallPolicyArn=POLICY_ARN,
            SubnetMappings=[{"SubnetId": "subnet-12345"}],
            VpcId="vpc-12345",
        )
        arn = create_resp["Firewall"]["FirewallArn"]
        resp = nfw.update_logging_configuration(
            FirewallArn=arn,
            LoggingConfiguration={
                "LogDestinationConfigs": [
                    {
                        "LogType": "ALERT",
                        "LogDestinationType": "CloudWatchLogs",
                        "LogDestination": {"logGroup": "/aws/network-firewall/alerts"},
                    }
                ]
            },
        )
        assert resp["FirewallArn"] == arn
        assert resp["FirewallName"] == name
        configs = resp["LoggingConfiguration"]["LogDestinationConfigs"]
        assert len(configs) == 1
        assert configs[0]["LogType"] == "ALERT"


class TestFirewallPolicyOperations:
    def test_create_firewall_policy(self, nfw):
        """CreateFirewallPolicy returns UpdateToken and FirewallPolicyResponse."""
        name = f"test-policy-{uuid.uuid4().hex[:8]}"
        resp = nfw.create_firewall_policy(
            FirewallPolicyName=name,
            FirewallPolicy={
                "StatelessDefaultActions": ["aws:pass"],
                "StatelessFragmentDefaultActions": ["aws:pass"],
            },
        )
        assert "UpdateToken" in resp
        assert "FirewallPolicyResponse" in resp
        pr = resp["FirewallPolicyResponse"]
        assert pr["FirewallPolicyName"] == name
        assert pr["FirewallPolicyArn"].startswith("arn:aws:network-firewall:")
        assert pr["FirewallPolicyStatus"] == "ACTIVE"

    def test_list_firewall_policies(self, nfw):
        """ListFirewallPolicies returns FirewallPolicies list."""
        name = f"test-policy-{uuid.uuid4().hex[:8]}"
        nfw.create_firewall_policy(
            FirewallPolicyName=name,
            FirewallPolicy={
                "StatelessDefaultActions": ["aws:pass"],
                "StatelessFragmentDefaultActions": ["aws:pass"],
            },
        )
        resp = nfw.list_firewall_policies()
        assert "FirewallPolicies" in resp
        names = [p["Name"] for p in resp["FirewallPolicies"]]
        assert name in names

    def test_describe_firewall_policy(self, nfw):
        """DescribeFirewallPolicy returns UpdateToken, FirewallPolicy, and FirewallPolicyResponse."""  # noqa: E501
        name = f"test-policy-{uuid.uuid4().hex[:8]}"
        nfw.create_firewall_policy(
            FirewallPolicyName=name,
            FirewallPolicy={
                "StatelessDefaultActions": ["aws:pass"],
                "StatelessFragmentDefaultActions": ["aws:pass"],
            },
        )
        resp = nfw.describe_firewall_policy(FirewallPolicyName=name)
        assert "UpdateToken" in resp
        assert "FirewallPolicy" in resp
        assert "FirewallPolicyResponse" in resp
        assert resp["FirewallPolicyResponse"]["FirewallPolicyName"] == name

    def test_update_firewall_policy(self, nfw):
        """UpdateFirewallPolicy returns updated policy response."""
        name = f"test-policy-{uuid.uuid4().hex[:8]}"
        create_resp = nfw.create_firewall_policy(
            FirewallPolicyName=name,
            FirewallPolicy={
                "StatelessDefaultActions": ["aws:pass"],
                "StatelessFragmentDefaultActions": ["aws:pass"],
            },
        )
        resp = nfw.update_firewall_policy(
            FirewallPolicyName=name,
            FirewallPolicy={
                "StatelessDefaultActions": ["aws:drop"],
                "StatelessFragmentDefaultActions": ["aws:drop"],
            },
            UpdateToken=create_resp["UpdateToken"],
        )
        assert "UpdateToken" in resp
        assert "FirewallPolicyResponse" in resp

    def test_delete_firewall_policy(self, nfw):
        """DeleteFirewallPolicy removes the policy."""
        name = f"test-policy-{uuid.uuid4().hex[:8]}"
        nfw.create_firewall_policy(
            FirewallPolicyName=name,
            FirewallPolicy={
                "StatelessDefaultActions": ["aws:pass"],
                "StatelessFragmentDefaultActions": ["aws:pass"],
            },
        )
        resp = nfw.delete_firewall_policy(FirewallPolicyName=name)
        assert "FirewallPolicyResponse" in resp

        # Policy should not be listed after deletion
        list_resp = nfw.list_firewall_policies()
        names = [p["Name"] for p in list_resp["FirewallPolicies"]]
        assert name not in names

    def test_describe_firewall_policy_not_found(self, nfw):
        """DescribeFirewallPolicy raises ResourceNotFoundException for missing policy."""
        with pytest.raises(ClientError) as exc_info:
            nfw.describe_firewall_policy(FirewallPolicyName=f"nonexistent-{uuid.uuid4().hex[:8]}")
        assert exc_info.value.response["Error"]["Code"] == "ResourceNotFoundException"


class TestRuleGroupOperations:
    def test_create_rule_group_stateful(self, nfw):
        """CreateRuleGroup (STATEFUL) returns UpdateToken and RuleGroupResponse."""
        name = f"test-rg-{uuid.uuid4().hex[:8]}"
        resp = nfw.create_rule_group(
            RuleGroupName=name,
            Type="STATEFUL",
            Capacity=100,
        )
        assert "UpdateToken" in resp
        assert "RuleGroupResponse" in resp
        rg = resp["RuleGroupResponse"]
        assert rg["RuleGroupName"] == name
        assert rg["Type"] == "STATEFUL"
        assert rg["Capacity"] == 100
        assert "stateful-rulegroup" in rg["RuleGroupArn"]

    def test_create_rule_group_stateless(self, nfw):
        """CreateRuleGroup (STATELESS) returns correct type in ARN."""
        name = f"test-rg-{uuid.uuid4().hex[:8]}"
        resp = nfw.create_rule_group(
            RuleGroupName=name,
            Type="STATELESS",
            Capacity=100,
        )
        rg = resp["RuleGroupResponse"]
        assert rg["Type"] == "STATELESS"
        assert "stateless-rulegroup" in rg["RuleGroupArn"]

    def test_list_rule_groups(self, nfw):
        """ListRuleGroups returns the created rule groups."""
        name = f"test-rg-{uuid.uuid4().hex[:8]}"
        nfw.create_rule_group(RuleGroupName=name, Type="STATEFUL", Capacity=100)
        resp = nfw.list_rule_groups()
        assert "RuleGroups" in resp
        names = [rg["Name"] for rg in resp["RuleGroups"]]
        assert name in names

    def test_describe_rule_group(self, nfw):
        """DescribeRuleGroup returns UpdateToken, RuleGroup, and RuleGroupResponse."""
        name = f"test-rg-{uuid.uuid4().hex[:8]}"
        nfw.create_rule_group(RuleGroupName=name, Type="STATEFUL", Capacity=100)
        resp = nfw.describe_rule_group(RuleGroupName=name, Type="STATEFUL")
        assert "UpdateToken" in resp
        assert "RuleGroup" in resp
        assert "RuleGroupResponse" in resp
        assert resp["RuleGroupResponse"]["RuleGroupName"] == name

    def test_describe_rule_group_metadata(self, nfw):
        """DescribeRuleGroupMetadata returns metadata fields."""
        name = f"test-rg-{uuid.uuid4().hex[:8]}"
        nfw.create_rule_group(RuleGroupName=name, Type="STATEFUL", Capacity=100)
        resp = nfw.describe_rule_group_metadata(RuleGroupName=name, Type="STATEFUL")
        assert resp["RuleGroupName"] == name
        assert resp["Type"] == "STATEFUL"
        assert resp["Capacity"] == 100

    def test_describe_rule_group_summary(self, nfw):
        """DescribeRuleGroupSummary returns summary fields."""
        name = f"test-rg-{uuid.uuid4().hex[:8]}"
        nfw.create_rule_group(RuleGroupName=name, Type="STATEFUL", Capacity=100)
        resp = nfw.describe_rule_group_summary(RuleGroupName=name, Type="STATEFUL")
        assert resp["RuleGroupName"] == name

    def test_update_rule_group(self, nfw):
        """UpdateRuleGroup returns updated response."""
        name = f"test-rg-{uuid.uuid4().hex[:8]}"
        create_resp = nfw.create_rule_group(RuleGroupName=name, Type="STATEFUL", Capacity=100)
        resp = nfw.update_rule_group(
            RuleGroupName=name,
            Type="STATEFUL",
            RuleGroup={
                "RulesSource": {
                    "RulesSourceList": {
                        "Targets": ["example.com"],
                        "TargetTypes": ["HTTP_HOST"],
                        "GeneratedRulesType": "DENYLIST",
                    }
                }
            },
            UpdateToken=create_resp["UpdateToken"],
        )
        assert "UpdateToken" in resp
        assert "RuleGroupResponse" in resp

    def test_delete_rule_group(self, nfw):
        """DeleteRuleGroup removes the rule group."""
        name = f"test-rg-{uuid.uuid4().hex[:8]}"
        nfw.create_rule_group(RuleGroupName=name, Type="STATEFUL", Capacity=100)
        resp = nfw.delete_rule_group(RuleGroupName=name, Type="STATEFUL")
        assert "RuleGroupResponse" in resp

        # Should not appear in list
        list_resp = nfw.list_rule_groups()
        names = [rg["Name"] for rg in list_resp["RuleGroups"]]
        assert name not in names

    def test_describe_rule_group_not_found(self, nfw):
        """DescribeRuleGroup raises ResourceNotFoundException for missing group."""
        with pytest.raises(ClientError) as exc_info:
            nfw.describe_rule_group(
                RuleGroupName=f"nonexistent-{uuid.uuid4().hex[:8]}",
                Type="STATEFUL",
            )
        assert exc_info.value.response["Error"]["Code"] == "ResourceNotFoundException"


class TestTLSInspectionConfigurationOperations:
    def test_create_tls_inspection_configuration(self, nfw):
        """CreateTLSInspectionConfiguration returns UpdateToken and TLSInspectionConfigurationResponse."""  # noqa: E501
        name = f"test-tls-{uuid.uuid4().hex[:8]}"
        resp = nfw.create_tls_inspection_configuration(
            TLSInspectionConfigurationName=name,
            TLSInspectionConfiguration={"ServerCertificateConfigurations": []},
        )
        assert "UpdateToken" in resp
        assert "TLSInspectionConfigurationResponse" in resp
        tls = resp["TLSInspectionConfigurationResponse"]
        assert tls["TLSInspectionConfigurationName"] == name
        assert tls["TLSInspectionConfigurationArn"].startswith("arn:aws:network-firewall:")
        assert tls["TLSInspectionConfigurationStatus"] == "ACTIVE"

    def test_list_tls_inspection_configurations(self, nfw):
        """ListTLSInspectionConfigurations returns the created configs."""
        name = f"test-tls-{uuid.uuid4().hex[:8]}"
        nfw.create_tls_inspection_configuration(
            TLSInspectionConfigurationName=name,
            TLSInspectionConfiguration={"ServerCertificateConfigurations": []},
        )
        resp = nfw.list_tls_inspection_configurations()
        assert "TLSInspectionConfigurations" in resp
        names = [c["Name"] for c in resp["TLSInspectionConfigurations"]]
        assert name in names

    def test_describe_tls_inspection_configuration(self, nfw):
        """DescribeTLSInspectionConfiguration returns configuration details."""
        name = f"test-tls-{uuid.uuid4().hex[:8]}"
        nfw.create_tls_inspection_configuration(
            TLSInspectionConfigurationName=name,
            TLSInspectionConfiguration={"ServerCertificateConfigurations": []},
        )
        resp = nfw.describe_tls_inspection_configuration(TLSInspectionConfigurationName=name)
        assert "UpdateToken" in resp
        assert "TLSInspectionConfiguration" in resp
        assert "TLSInspectionConfigurationResponse" in resp
        assert resp["TLSInspectionConfigurationResponse"]["TLSInspectionConfigurationName"] == name

    def test_update_tls_inspection_configuration(self, nfw):
        """UpdateTLSInspectionConfiguration returns updated response."""
        name = f"test-tls-{uuid.uuid4().hex[:8]}"
        create_resp = nfw.create_tls_inspection_configuration(
            TLSInspectionConfigurationName=name,
            TLSInspectionConfiguration={"ServerCertificateConfigurations": []},
        )
        resp = nfw.update_tls_inspection_configuration(
            TLSInspectionConfigurationName=name,
            TLSInspectionConfiguration={"ServerCertificateConfigurations": []},
            UpdateToken=create_resp["UpdateToken"],
        )
        assert "UpdateToken" in resp
        assert "TLSInspectionConfigurationResponse" in resp

    def test_delete_tls_inspection_configuration(self, nfw):
        """DeleteTLSInspectionConfiguration removes the config."""
        name = f"test-tls-{uuid.uuid4().hex[:8]}"
        nfw.create_tls_inspection_configuration(
            TLSInspectionConfigurationName=name,
            TLSInspectionConfiguration={"ServerCertificateConfigurations": []},
        )
        resp = nfw.delete_tls_inspection_configuration(TLSInspectionConfigurationName=name)
        assert "TLSInspectionConfigurationResponse" in resp

        # Should not appear in list
        list_resp = nfw.list_tls_inspection_configurations()
        names = [c["Name"] for c in list_resp["TLSInspectionConfigurations"]]
        assert name not in names


class TestTaggingOperations:
    def test_tag_resource_and_list_tags(self, nfw):
        """TagResource adds tags, ListTagsForResource returns them."""
        name = f"test-policy-{uuid.uuid4().hex[:8]}"
        resp = nfw.create_firewall_policy(
            FirewallPolicyName=name,
            FirewallPolicy={
                "StatelessDefaultActions": ["aws:pass"],
                "StatelessFragmentDefaultActions": ["aws:pass"],
            },
        )
        arn = resp["FirewallPolicyResponse"]["FirewallPolicyArn"]
        nfw.tag_resource(ResourceArn=arn, Tags=[{"Key": "env", "Value": "prod"}])
        tags_resp = nfw.list_tags_for_resource(ResourceArn=arn)
        assert "Tags" in tags_resp
        tag_map = {t["Key"]: t["Value"] for t in tags_resp["Tags"]}
        assert tag_map.get("env") == "prod"

    def test_untag_resource(self, nfw):
        """UntagResource removes specified tags."""
        name = f"test-policy-{uuid.uuid4().hex[:8]}"
        resp = nfw.create_firewall_policy(
            FirewallPolicyName=name,
            FirewallPolicy={
                "StatelessDefaultActions": ["aws:pass"],
                "StatelessFragmentDefaultActions": ["aws:pass"],
            },
            Tags=[{"Key": "env", "Value": "test"}, {"Key": "team", "Value": "ops"}],
        )
        arn = resp["FirewallPolicyResponse"]["FirewallPolicyArn"]
        nfw.untag_resource(ResourceArn=arn, TagKeys=["env"])
        tags_resp = nfw.list_tags_for_resource(ResourceArn=arn)
        keys = [t["Key"] for t in tags_resp["Tags"]]
        assert "env" not in keys
        assert "team" in keys


class TestFirewallModificationOperations:
    def test_delete_firewall(self, nfw):
        """DeleteFirewall removes the firewall and returns Firewall + FirewallStatus."""
        name = _unique_name()
        create_resp = nfw.create_firewall(
            FirewallName=name,
            FirewallPolicyArn=POLICY_ARN,
            SubnetMappings=[{"SubnetId": "subnet-12345"}],
            VpcId="vpc-12345",
            DeleteProtection=False,
        )
        arn = create_resp["Firewall"]["FirewallArn"]
        resp = nfw.delete_firewall(FirewallArn=arn)
        assert "Firewall" in resp
        assert "FirewallStatus" in resp
        assert resp["Firewall"]["FirewallName"] == name

    def test_associate_firewall_policy(self, nfw):
        """AssociateFirewallPolicy returns FirewallName and UpdateToken."""
        name = _unique_name()
        create_resp = nfw.create_firewall(
            FirewallName=name,
            FirewallPolicyArn=POLICY_ARN,
            SubnetMappings=[{"SubnetId": "subnet-12345"}],
            VpcId="vpc-12345",
            FirewallPolicyChangeProtection=False,
        )
        arn = create_resp["Firewall"]["FirewallArn"]
        desc = nfw.describe_firewall(FirewallArn=arn)
        token = desc["UpdateToken"]

        resp = nfw.associate_firewall_policy(
            FirewallArn=arn, FirewallPolicyArn=POLICY_ARN, UpdateToken=token
        )
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200
        assert resp["FirewallName"] == name
        assert "UpdateToken" in resp

    def test_associate_and_disassociate_subnets(self, nfw):
        """AssociateSubnets adds a subnet; DisassociateSubnets removes it."""
        name = _unique_name()
        create_resp = nfw.create_firewall(
            FirewallName=name,
            FirewallPolicyArn=POLICY_ARN,
            SubnetMappings=[{"SubnetId": "subnet-12345"}],
            VpcId="vpc-12345",
        )
        arn = create_resp["Firewall"]["FirewallArn"]
        desc = nfw.describe_firewall(FirewallArn=arn)
        token = desc["UpdateToken"]

        assoc_resp = nfw.associate_subnets(
            FirewallArn=arn,
            SubnetMappings=[{"SubnetId": "subnet-99999"}],
            UpdateToken=token,
        )
        assert assoc_resp["ResponseMetadata"]["HTTPStatusCode"] == 200
        assert assoc_resp["FirewallArn"] == arn
        assert "UpdateToken" in assoc_resp

        new_token = assoc_resp["UpdateToken"]
        disassoc_resp = nfw.disassociate_subnets(
            FirewallArn=arn, SubnetIds=["subnet-99999"], UpdateToken=new_token
        )
        assert disassoc_resp["ResponseMetadata"]["HTTPStatusCode"] == 200
        assert disassoc_resp["FirewallArn"] == arn

    def test_describe_firewall_metadata(self, nfw):
        """DescribeFirewallMetadata returns FirewallArn, FirewallPolicyArn, Status."""
        name = _unique_name()
        create_resp = nfw.create_firewall(
            FirewallName=name,
            FirewallPolicyArn=POLICY_ARN,
            SubnetMappings=[{"SubnetId": "subnet-12345"}],
            VpcId="vpc-12345",
        )
        arn = create_resp["Firewall"]["FirewallArn"]
        resp = nfw.describe_firewall_metadata(FirewallArn=arn)
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200
        assert resp["FirewallArn"] == arn
        assert "FirewallPolicyArn" in resp
        assert "Status" in resp

    def test_update_firewall_delete_protection(self, nfw):
        """UpdateFirewallDeleteProtection updates delete protection flag."""
        name = _unique_name()
        create_resp = nfw.create_firewall(
            FirewallName=name,
            FirewallPolicyArn=POLICY_ARN,
            SubnetMappings=[{"SubnetId": "subnet-12345"}],
            VpcId="vpc-12345",
        )
        arn = create_resp["Firewall"]["FirewallArn"]
        desc = nfw.describe_firewall(FirewallArn=arn)
        token = desc["UpdateToken"]

        resp = nfw.update_firewall_delete_protection(
            FirewallArn=arn, DeleteProtection=False, UpdateToken=token
        )
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200
        assert resp["FirewallArn"] == arn
        assert resp["DeleteProtection"] is False
        assert "UpdateToken" in resp

    def test_update_firewall_description(self, nfw):
        """UpdateFirewallDescription sets description on a firewall."""
        name = _unique_name()
        create_resp = nfw.create_firewall(
            FirewallName=name,
            FirewallPolicyArn=POLICY_ARN,
            SubnetMappings=[{"SubnetId": "subnet-12345"}],
            VpcId="vpc-12345",
        )
        arn = create_resp["Firewall"]["FirewallArn"]
        desc = nfw.describe_firewall(FirewallArn=arn)
        token = desc["UpdateToken"]

        resp = nfw.update_firewall_description(
            FirewallArn=arn, Description="compat-test-desc", UpdateToken=token
        )
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200
        assert resp["FirewallArn"] == arn
        assert resp["Description"] == "compat-test-desc"

    def test_update_firewall_policy_change_protection(self, nfw):
        """UpdateFirewallPolicyChangeProtection toggles the policy change protection flag."""
        name = _unique_name()
        create_resp = nfw.create_firewall(
            FirewallName=name,
            FirewallPolicyArn=POLICY_ARN,
            SubnetMappings=[{"SubnetId": "subnet-12345"}],
            VpcId="vpc-12345",
        )
        arn = create_resp["Firewall"]["FirewallArn"]
        desc = nfw.describe_firewall(FirewallArn=arn)
        token = desc["UpdateToken"]

        resp = nfw.update_firewall_policy_change_protection(
            FirewallArn=arn, FirewallPolicyChangeProtection=False, UpdateToken=token
        )
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200
        assert resp["FirewallArn"] == arn
        assert resp["FirewallPolicyChangeProtection"] is False

    def test_update_subnet_change_protection(self, nfw):
        """UpdateSubnetChangeProtection toggles the subnet change protection flag."""
        name = _unique_name()
        create_resp = nfw.create_firewall(
            FirewallName=name,
            FirewallPolicyArn=POLICY_ARN,
            SubnetMappings=[{"SubnetId": "subnet-12345"}],
            VpcId="vpc-12345",
        )
        arn = create_resp["Firewall"]["FirewallArn"]
        desc = nfw.describe_firewall(FirewallArn=arn)
        token = desc["UpdateToken"]

        resp = nfw.update_subnet_change_protection(
            FirewallArn=arn, SubnetChangeProtection=False, UpdateToken=token
        )
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200
        assert resp["FirewallArn"] == arn
        assert resp["SubnetChangeProtection"] is False

    def test_update_firewall_encryption_configuration(self, nfw):
        """UpdateFirewallEncryptionConfiguration sets encryption config on a firewall."""
        name = _unique_name()
        create_resp = nfw.create_firewall(
            FirewallName=name,
            FirewallPolicyArn=POLICY_ARN,
            SubnetMappings=[{"SubnetId": "subnet-12345"}],
            VpcId="vpc-12345",
        )
        arn = create_resp["Firewall"]["FirewallArn"]
        desc = nfw.describe_firewall(FirewallArn=arn)
        token = desc["UpdateToken"]

        resp = nfw.update_firewall_encryption_configuration(
            FirewallArn=arn,
            EncryptionConfiguration={"Type": "AWS_OWNED_KMS_KEY"},
            UpdateToken=token,
        )
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200
        assert resp["FirewallArn"] == arn
        assert "EncryptionConfiguration" in resp


class TestResourcePolicyOperations:
    def test_put_and_describe_resource_policy(self, nfw):
        """PutResourcePolicy stores policy, DescribeResourcePolicy retrieves it."""
        import json as _json

        name = f"test-policy-{uuid.uuid4().hex[:8]}"
        resp = nfw.create_firewall_policy(
            FirewallPolicyName=name,
            FirewallPolicy={
                "StatelessDefaultActions": ["aws:pass"],
                "StatelessFragmentDefaultActions": ["aws:pass"],
            },
        )
        arn = resp["FirewallPolicyResponse"]["FirewallPolicyArn"]
        policy_doc = _json.dumps({"Version": "2012-10-17", "Statement": []})
        nfw.put_resource_policy(ResourceArn=arn, Policy=policy_doc)
        desc_resp = nfw.describe_resource_policy(ResourceArn=arn)
        assert "Policy" in desc_resp
        assert desc_resp["Policy"] == policy_doc

    def test_delete_resource_policy(self, nfw):
        """DeleteResourcePolicy removes the policy."""
        import json as _json

        name = f"test-policy-{uuid.uuid4().hex[:8]}"
        resp = nfw.create_firewall_policy(
            FirewallPolicyName=name,
            FirewallPolicy={
                "StatelessDefaultActions": ["aws:pass"],
                "StatelessFragmentDefaultActions": ["aws:pass"],
            },
        )
        arn = resp["FirewallPolicyResponse"]["FirewallPolicyArn"]
        policy_doc = _json.dumps({"Version": "2012-10-17", "Statement": []})
        nfw.put_resource_policy(ResourceArn=arn, Policy=policy_doc)
        nfw.delete_resource_policy(ResourceArn=arn)

        with pytest.raises(ClientError) as exc_info:
            nfw.describe_resource_policy(ResourceArn=arn)
        assert exc_info.value.response["Error"]["Code"] == "ResourceNotFoundException"


class TestNetworkFirewallUpdateFirewallAnalysisSettings:
    """Tests for UpdateFirewallAnalysisSettings."""

    def test_update_firewall_analysis_settings_not_found(self, nfw):
        """UpdateFirewallAnalysisSettings raises ResourceNotFoundException for unknown firewall."""
        with pytest.raises(ClientError) as exc_info:
            nfw.update_firewall_analysis_settings(
                FirewallName="nonexistent-firewall",
                EnabledAnalysisTypes=["TLS_SNI"],
            )
        assert exc_info.value.response["Error"]["Code"] == "ResourceNotFoundException"

    def test_update_firewall_analysis_settings(self, nfw):
        """UpdateFirewallAnalysisSettings updates analysis settings on an existing firewall."""
        name = f"test-fw-analysis-{uuid.uuid4().hex[:8]}"
        policy_name = f"test-policy-{uuid.uuid4().hex[:8]}"
        policy_resp = nfw.create_firewall_policy(
            FirewallPolicyName=policy_name,
            FirewallPolicy={
                "StatelessDefaultActions": ["aws:pass"],
                "StatelessFragmentDefaultActions": ["aws:pass"],
            },
        )
        policy_arn = policy_resp["FirewallPolicyResponse"]["FirewallPolicyArn"]
        nfw.create_firewall(
            FirewallName=name,
            FirewallPolicyArn=policy_arn,
            VpcId="vpc-12345678",
            SubnetMappings=[{"SubnetId": "subnet-12345678"}],
            DeleteProtection=False,
        )
        try:
            resp = nfw.update_firewall_analysis_settings(
                FirewallName=name,
                EnabledAnalysisTypes=["TLS_SNI"],
            )
            assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200
        finally:
            nfw.delete_firewall(FirewallName=name)
            nfw.delete_firewall_policy(FirewallPolicyName=policy_name)
