"""IaC test: CloudFormation VPC network stack."""

from pathlib import Path

import pytest

from tests.iac.conftest import make_client
from tests.iac.helpers.resource_validator import assert_vpc_exists

pytestmark = pytest.mark.iac

TEMPLATE = (Path(__file__).parent / "template.yaml").read_text()


def _get_output(stack: dict, key: str) -> str:
    """Extract an output value from a CloudFormation stack description."""
    for out in stack.get("Outputs", []):
        if out["OutputKey"] == key:
            return out["OutputValue"]
    raise KeyError(f"Output {key!r} not found in stack outputs")


class TestVpcNetwork:
    def test_deploy_and_validate(self, deploy_stack):
        """Deploy VPC stack, validate all resources, then delete."""
        ec2 = make_client("ec2")

        # Deploy
        stack = deploy_stack("vpc-network", TEMPLATE)
        assert stack["StackStatus"] == "CREATE_COMPLETE"

        # Extract outputs
        vpc_id = _get_output(stack, "VpcId")
        subnet1_id = _get_output(stack, "PublicSubnet1Id")
        subnet2_id = _get_output(stack, "PublicSubnet2Id")
        igw_id = _get_output(stack, "InternetGatewayId")
        sg_id = _get_output(stack, "SecurityGroupId")

        # Validate VPC exists with correct CIDR
        vpc = assert_vpc_exists(ec2, vpc_id)
        assert vpc["CidrBlock"] == "10.0.0.0/16"

        # Validate 2 subnets exist with correct CIDRs
        subnets_resp = ec2.describe_subnets(SubnetIds=[subnet1_id, subnet2_id])
        subnets = subnets_resp["Subnets"]
        assert len(subnets) == 2
        cidrs = sorted(s["CidrBlock"] for s in subnets)
        assert cidrs == ["10.0.1.0/24", "10.0.2.0/24"]

        # Validate IGW is attached to VPC
        igw_resp = ec2.describe_internet_gateways(InternetGatewayIds=[igw_id])
        igws = igw_resp["InternetGateways"]
        assert len(igws) == 1
        attachments = igws[0].get("Attachments", [])
        attached_vpcs = [a["VpcId"] for a in attachments]
        assert vpc_id in attached_vpcs

        # Validate security group rules (HTTP 80, SSH 22)
        sg_resp = ec2.describe_security_groups(GroupIds=[sg_id])
        sgs = sg_resp["SecurityGroups"]
        assert len(sgs) == 1
        ingress = sgs[0]["IpPermissions"]
        ingress_ports = sorted(rule["FromPort"] for rule in ingress if "FromPort" in rule)
        assert 22 in ingress_ports
        assert 80 in ingress_ports
