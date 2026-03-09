"""IaC test: cdk - vpc_network.

Deploys a VPC with 2 public subnets and a security group allowing HTTP/SSH.
Validates all resources via the EC2 API.
"""

from pathlib import Path

import pytest

from tests.iac.conftest import make_client

pytestmark = pytest.mark.iac

SCENARIO_DIR = Path(__file__).parent


class TestVpcNetwork:
    """CDK VPC network stack with subnets and security group."""

    @pytest.fixture(autouse=True)
    def deploy(self, cdk_runner):
        """Deploy the CDK app and tear it down after tests."""
        result = cdk_runner.deploy(SCENARIO_DIR, "VpcNetworkStack")
        assert result.returncode == 0, f"cdk deploy failed: {result.stderr}"
        yield
        cdk_runner.destroy(SCENARIO_DIR, "VpcNetworkStack")

    def test_vpc_created(self):
        """Verify VPC exists with expected CIDR block."""
        ec2 = make_client("ec2")
        resp = ec2.describe_vpcs(Filters=[{"Name": "cidr", "Values": ["10.0.0.0/16"]}])
        vpcs = resp["Vpcs"]
        assert len(vpcs) >= 1, "VPC with CIDR 10.0.0.0/16 not found"
        assert vpcs[0]["CidrBlock"] == "10.0.0.0/16"

    def test_subnets_created(self):
        """Verify at least 2 subnets exist in the VPC."""
        ec2 = make_client("ec2")
        # Find the VPC first
        vpc_resp = ec2.describe_vpcs(Filters=[{"Name": "cidr", "Values": ["10.0.0.0/16"]}])
        assert len(vpc_resp["Vpcs"]) >= 1
        vpc_id = vpc_resp["Vpcs"][0]["VpcId"]

        subnets_resp = ec2.describe_subnets(Filters=[{"Name": "vpc-id", "Values": [vpc_id]}])
        subnets = subnets_resp["Subnets"]
        assert len(subnets) >= 2, f"Expected at least 2 subnets, found {len(subnets)}"

    def test_security_group_created(self):
        """Verify security group exists with HTTP and SSH ingress rules."""
        ec2 = make_client("ec2")
        resp = ec2.describe_security_groups(
            Filters=[{"Name": "group-name", "Values": ["vpc-network-web-sg"]}]
        )
        sgs = resp["SecurityGroups"]
        assert len(sgs) >= 1, "Security group 'vpc-network-web-sg' not found"

        ingress = sgs[0]["IpPermissions"]
        ingress_ports = sorted(rule["FromPort"] for rule in ingress if "FromPort" in rule)
        assert 22 in ingress_ports, "SSH port 22 not in ingress rules"
        assert 80 in ingress_ports, "HTTP port 80 not in ingress rules"
