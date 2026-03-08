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
