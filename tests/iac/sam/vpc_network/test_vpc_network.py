"""IaC test: sam - vpc_network."""

from pathlib import Path

import pytest

from tests.iac.conftest import make_client
from tests.iac.helpers.stack_deployer import delete_stack, deploy_and_yield, get_stack_outputs

pytestmark = pytest.mark.iac


@pytest.fixture(scope="module")
def deployed_stack(ensure_server, test_run_id):
    template = (Path(__file__).parent / "template.yaml").read_text()
    stack_name = f"{test_run_id}-sam-vpc-network"
    stack = deploy_and_yield(stack_name, template)
    yield stack
    delete_stack(stack_name)


class TestVpcNetwork:
    def test_stack_created(self, deployed_stack):
        assert deployed_stack["StackStatus"] == "CREATE_COMPLETE"

    def test_vpc_exists(self, deployed_stack, ensure_server):
        outputs = get_stack_outputs(deployed_stack)
        vpc_id = outputs.get("VpcId")
        assert vpc_id is not None, "VpcId output missing"

        ec2 = make_client("ec2")
        resp = ec2.describe_vpcs(VpcIds=[vpc_id])
        vpcs = resp["Vpcs"]
        assert len(vpcs) == 1
        assert vpcs[0]["CidrBlock"] == "10.0.0.0/16"

    def test_subnets_exist(self, deployed_stack, ensure_server):
        outputs = get_stack_outputs(deployed_stack)
        subnet_a_id = outputs.get("SubnetAId")
        subnet_b_id = outputs.get("SubnetBId")
        assert subnet_a_id is not None, "SubnetAId output missing"
        assert subnet_b_id is not None, "SubnetBId output missing"

        ec2 = make_client("ec2")
        resp = ec2.describe_subnets(SubnetIds=[subnet_a_id, subnet_b_id])
        subnets = resp["Subnets"]
        assert len(subnets) == 2
        cidrs = sorted(s["CidrBlock"] for s in subnets)
        assert cidrs == ["10.0.1.0/24", "10.0.2.0/24"]

    def test_security_group_exists(self, deployed_stack, ensure_server):
        outputs = get_stack_outputs(deployed_stack)
        sg_id = outputs.get("SecurityGroupId")
        assert sg_id is not None, "SecurityGroupId output missing"

        ec2 = make_client("ec2")
        resp = ec2.describe_security_groups(GroupIds=[sg_id])
        sgs = resp["SecurityGroups"]
        assert len(sgs) == 1
        ingress = sgs[0]["IpPermissions"]
        assert any(rule.get("FromPort") == 443 and rule.get("ToPort") == 443 for rule in ingress)
