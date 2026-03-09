"""IaC test: pulumi - vpc_network.

Validates VPC, subnets, and security group creation via Pulumi.
"""

from __future__ import annotations

from pathlib import Path

import pytest

from tests.iac.conftest import make_client
from tests.iac.helpers.resource_validator import assert_vpc_exists

pytestmark = pytest.mark.iac

SCENARIO_DIR = Path(__file__).parent


@pytest.fixture(scope="module")
def stack_outputs(pulumi_runner):
    """Deploy the VPC network stack and return Pulumi outputs."""
    result = pulumi_runner.up(SCENARIO_DIR)
    if result.returncode != 0:
        pytest.fail(f"pulumi up failed:\n{result.stderr}")
    yield pulumi_runner.stack_output(SCENARIO_DIR)
    pulumi_runner.destroy(SCENARIO_DIR)


@pytest.fixture(scope="module")
def ec2_client():
    return make_client("ec2")


class TestVpcNetwork:
    """Pulumi VPC network: VPC + 2 subnets + security group."""

    def test_vpc_created(self, stack_outputs, ec2_client):
        vpc_id = stack_outputs["vpc_id"]
        vpc = assert_vpc_exists(ec2_client, vpc_id)
        assert vpc["CidrBlock"] == "10.0.0.0/16"

    def test_subnets_created(self, stack_outputs, ec2_client):
        subnet_ids = stack_outputs["subnet_ids"]
        assert len(subnet_ids) == 2

        resp = ec2_client.describe_subnets(SubnetIds=subnet_ids)
        subnets = resp["Subnets"]
        assert len(subnets) == 2

        azs = sorted(s["AvailabilityZone"] for s in subnets)
        assert azs == ["us-east-1a", "us-east-1b"]

        cidrs = sorted(s["CidrBlock"] for s in subnets)
        assert cidrs == ["10.0.1.0/24", "10.0.2.0/24"]

    def test_security_group_created(self, stack_outputs, ec2_client):
        sg_id = stack_outputs["security_group_id"]

        resp = ec2_client.describe_security_groups(GroupIds=[sg_id])
        sgs = resp["SecurityGroups"]
        assert len(sgs) == 1

        ingress = sgs[0]["IpPermissions"]
        ingress_ports = sorted(r["FromPort"] for r in ingress)
        assert ingress_ports == [22, 80]

        for rule in ingress:
            assert rule["IpProtocol"] == "tcp"
            assert any(ip_range["CidrIp"] == "0.0.0.0/0" for ip_range in rule["IpRanges"])
