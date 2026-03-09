"""IaC test: Terraform VPC network scenario."""

from __future__ import annotations

import pytest

from tests.iac.conftest import make_client


@pytest.fixture(scope="module")
def vpc_outputs(terraform_dir, tf_runner):
    """Apply the VPC network scenario and return Terraform outputs."""
    result = tf_runner.apply(terraform_dir)
    if result.returncode != 0:
        pytest.fail(f"terraform apply failed:\n{result.stderr}")
    return tf_runner.output(terraform_dir)


@pytest.fixture(scope="module")
def ec2_client():
    return make_client("ec2")


class TestVpcNetwork:
    """Validate VPC network resources created by Terraform."""

    def test_vpc_exists_with_correct_cidr(self, vpc_outputs, ec2_client):
        vpc_id = vpc_outputs["vpc_id"]["value"]
        resp = ec2_client.describe_vpcs(VpcIds=[vpc_id])
        vpcs = resp["Vpcs"]
        assert len(vpcs) == 1
        assert vpcs[0]["CidrBlock"] == "10.0.0.0/16"

    def test_two_subnets_in_correct_azs(self, vpc_outputs, ec2_client):
        subnet_ids = vpc_outputs["subnet_ids"]["value"]
        assert len(subnet_ids) == 2

        resp = ec2_client.describe_subnets(SubnetIds=subnet_ids)
        subnets = resp["Subnets"]
        assert len(subnets) == 2

        azs = sorted(s["AvailabilityZone"] for s in subnets)
        assert azs == ["us-east-1a", "us-east-1b"]

        cidrs = sorted(s["CidrBlock"] for s in subnets)
        assert cidrs == ["10.0.1.0/24", "10.0.2.0/24"]

    def test_internet_gateway_attached(self, vpc_outputs, ec2_client):
        vpc_id = vpc_outputs["vpc_id"]["value"]
        resp = ec2_client.describe_internet_gateways(
            Filters=[{"Name": "attachment.vpc-id", "Values": [vpc_id]}]
        )
        igws = resp["InternetGateways"]
        assert len(igws) == 1
        attachments = igws[0]["Attachments"]
        assert any(a["VpcId"] == vpc_id for a in attachments)

    def test_security_group_ingress_rules(self, vpc_outputs, ec2_client):
        sg_id = vpc_outputs["security_group_id"]["value"]
        resp = ec2_client.describe_security_groups(GroupIds=[sg_id])
        sgs = resp["SecurityGroups"]
        assert len(sgs) == 1

        ingress = sgs[0]["IpPermissions"]
        # Expect two ingress rules: port 80 and port 22
        ingress_ports = sorted(r["FromPort"] for r in ingress)
        assert ingress_ports == [22, 80]

        for rule in ingress:
            assert rule["IpProtocol"] == "tcp"
            assert any(ip_range["CidrIp"] == "0.0.0.0/0" for ip_range in rule["IpRanges"])
