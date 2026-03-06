"""Route53 Resolver compatibility tests."""

import uuid

import pytest

from tests.compatibility.conftest import make_client


@pytest.fixture
def resolver():
    return make_client("route53resolver")


def _uid():
    return uuid.uuid4().hex[:8]


@pytest.fixture
def ec2():
    return make_client("ec2")


class TestRoute53ResolverOperations:
    def test_create_resolver_endpoint(self, resolver, ec2):
        # Create a VPC and subnets first
        vpc = ec2.create_vpc(CidrBlock="10.0.0.0/16")
        vpc_id = vpc["Vpc"]["VpcId"]
        subnet1 = ec2.create_subnet(
            VpcId=vpc_id, CidrBlock="10.0.1.0/24", AvailabilityZone="us-east-1a"
        )
        subnet2 = ec2.create_subnet(
            VpcId=vpc_id, CidrBlock="10.0.2.0/24", AvailabilityZone="us-east-1b"
        )
        sg = ec2.create_security_group(
            GroupName=f"resolver-sg-{_uid()}", Description="test", VpcId=vpc_id
        )

        response = resolver.create_resolver_endpoint(
            CreatorRequestId=_uid(),
            Name=f"test-endpoint-{_uid()}",
            SecurityGroupIds=[sg["GroupId"]],
            Direction="INBOUND",
            IpAddresses=[
                {"SubnetId": subnet1["Subnet"]["SubnetId"]},
                {"SubnetId": subnet2["Subnet"]["SubnetId"]},
            ],
        )
        assert response["ResolverEndpoint"]["Direction"] == "INBOUND"
        endpoint_id = response["ResolverEndpoint"]["Id"]

        # Cleanup
        resolver.delete_resolver_endpoint(ResolverEndpointId=endpoint_id)

    def test_list_resolver_endpoints(self, resolver):
        response = resolver.list_resolver_endpoints()
        assert "ResolverEndpoints" in response

    def test_list_resolver_rules(self, resolver):
        response = resolver.list_resolver_rules()
        assert "ResolverRules" in response
