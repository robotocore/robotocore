"""IaC test: serverless - vpc_network (VPC + subnets + security group)."""

from __future__ import annotations

import pytest

from tests.iac.conftest import make_client
from tests.iac.helpers.stack_deployer import delete_stack, deploy_and_yield, get_stack_outputs

pytestmark = pytest.mark.iac

TEMPLATE = """\
AWSTemplateFormatVersion: "2010-09-09"
Description: Serverless VPC network - VPC, subnets, security group

Resources:
  Vpc:
    Type: AWS::EC2::VPC
    Properties:
      CidrBlock: 10.0.0.0/16
      EnableDnsSupport: true
      EnableDnsHostnames: true

  PublicSubnet1:
    Type: AWS::EC2::Subnet
    Properties:
      VpcId: !Ref Vpc
      CidrBlock: 10.0.1.0/24
      AvailabilityZone: us-east-1a

  PublicSubnet2:
    Type: AWS::EC2::Subnet
    Properties:
      VpcId: !Ref Vpc
      CidrBlock: 10.0.2.0/24
      AvailabilityZone: us-east-1b

  AppSecurityGroup:
    Type: AWS::EC2::SecurityGroup
    Properties:
      GroupDescription: Allow HTTP and SSH
      VpcId: !Ref Vpc
      SecurityGroupIngress:
        - IpProtocol: tcp
          FromPort: 80
          ToPort: 80
          CidrIp: 0.0.0.0/0
        - IpProtocol: tcp
          FromPort: 22
          ToPort: 22
          CidrIp: 0.0.0.0/0

Outputs:
  VpcId:
    Value: !Ref Vpc
  Subnet1Id:
    Value: !Ref PublicSubnet1
  Subnet2Id:
    Value: !Ref PublicSubnet2
  SecurityGroupId:
    Value: !Ref AppSecurityGroup
"""


@pytest.fixture(scope="module")
def deployed_stack(ensure_server, test_run_id):
    stack_name = f"{test_run_id}-sls-vpc-network"
    stack = deploy_and_yield(stack_name, TEMPLATE)
    yield stack
    delete_stack(stack_name)


class TestVpcNetwork:
    def test_stack_created(self, deployed_stack):
        assert deployed_stack["StackStatus"] == "CREATE_COMPLETE"

    def test_vpc_exists(self, deployed_stack):
        outputs = get_stack_outputs(deployed_stack)
        ec2 = make_client("ec2")
        vpcs = ec2.describe_vpcs(VpcIds=[outputs["VpcId"]])
        assert len(vpcs["Vpcs"]) == 1
        assert vpcs["Vpcs"][0]["CidrBlock"] == "10.0.0.0/16"

    def test_subnets_exist(self, deployed_stack):
        outputs = get_stack_outputs(deployed_stack)
        ec2 = make_client("ec2")
        subnets = ec2.describe_subnets(SubnetIds=[outputs["Subnet1Id"], outputs["Subnet2Id"]])
        assert len(subnets["Subnets"]) == 2
        cidrs = sorted(s["CidrBlock"] for s in subnets["Subnets"])
        assert cidrs == ["10.0.1.0/24", "10.0.2.0/24"]

    def test_security_group_rules(self, deployed_stack):
        outputs = get_stack_outputs(deployed_stack)
        ec2 = make_client("ec2")
        sgs = ec2.describe_security_groups(GroupIds=[outputs["SecurityGroupId"]])
        assert len(sgs["SecurityGroups"]) == 1
        ingress = sgs["SecurityGroups"][0]["IpPermissions"]
        ports = sorted(r["FromPort"] for r in ingress if "FromPort" in r)
        assert 22 in ports
        assert 80 in ports
