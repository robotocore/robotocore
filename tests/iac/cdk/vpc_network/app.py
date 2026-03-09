"""CDK app: VPC + 2 subnets + security group."""

import aws_cdk as cdk
from aws_cdk import aws_ec2 as ec2


class VpcNetworkStack(cdk.Stack):
    def __init__(self, scope, construct_id, **kwargs):
        super().__init__(scope, construct_id, **kwargs)

        vpc = ec2.Vpc(
            self,
            "Vpc",
            vpc_name="vpc-network-vpc",
            ip_addresses=ec2.IpAddresses.cidr("10.0.0.0/16"),
            max_azs=2,
            subnet_configuration=[
                ec2.SubnetConfiguration(
                    name="Public1",
                    subnet_type=ec2.SubnetType.PUBLIC,
                    cidr_mask=24,
                ),
            ],
            nat_gateways=0,
        )

        sg = ec2.SecurityGroup(
            self,
            "WebSG",
            security_group_name="vpc-network-web-sg",
            vpc=vpc,
            description="Allow HTTP and SSH",
            allow_all_outbound=True,
        )
        sg.add_ingress_rule(ec2.Peer.any_ipv4(), ec2.Port.tcp(80), "Allow HTTP")
        sg.add_ingress_rule(ec2.Peer.any_ipv4(), ec2.Port.tcp(22), "Allow SSH")

        cdk.CfnOutput(self, "VpcId", value=vpc.vpc_id)
        cdk.CfnOutput(self, "SecurityGroupId", value=sg.security_group_id)


app = cdk.App()
VpcNetworkStack(app, "VpcNetworkStack")
app.synth()
