"""Pulumi program: VPC + 2 subnets + security group."""

import pulumi
import pulumi_aws as aws

vpc = aws.ec2.Vpc(
    "app-vpc",
    cidr_block="10.0.0.0/16",
    tags={"Name": "app-vpc"},
)

subnet_a = aws.ec2.Subnet(
    "subnet-a",
    vpc_id=vpc.id,
    cidr_block="10.0.1.0/24",
    availability_zone="us-east-1a",
    tags={"Name": "subnet-a"},
)

subnet_b = aws.ec2.Subnet(
    "subnet-b",
    vpc_id=vpc.id,
    cidr_block="10.0.2.0/24",
    availability_zone="us-east-1b",
    tags={"Name": "subnet-b"},
)

security_group = aws.ec2.SecurityGroup(
    "app-sg",
    vpc_id=vpc.id,
    description="Application security group",
    ingress=[
        aws.ec2.SecurityGroupIngressArgs(
            protocol="tcp",
            from_port=80,
            to_port=80,
            cidr_blocks=["0.0.0.0/0"],
        ),
        aws.ec2.SecurityGroupIngressArgs(
            protocol="tcp",
            from_port=22,
            to_port=22,
            cidr_blocks=["0.0.0.0/0"],
        ),
    ],
    tags={"Name": "app-sg"},
)

pulumi.export("vpc_id", vpc.id)
pulumi.export("subnet_ids", [subnet_a.id, subnet_b.id])
pulumi.export("security_group_id", security_group.id)
