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

    def test_create_resolver_rule(self, resolver):
        name = f"test-rule-{_uid()}"
        response = resolver.create_resolver_rule(
            CreatorRequestId=_uid(),
            Name=name,
            RuleType="FORWARD",
            DomainName="example.com.",
        )
        assert response["ResolverRule"]["Name"] == name
        rule_id = response["ResolverRule"]["Id"]
        resolver.delete_resolver_rule(ResolverRuleId=rule_id)

    def test_get_resolver_rule(self, resolver):
        name = f"get-rule-{_uid()}"
        create = resolver.create_resolver_rule(
            CreatorRequestId=_uid(),
            Name=name,
            RuleType="FORWARD",
            DomainName="get-test.example.com",
        )
        rule_id = create["ResolverRule"]["Id"]
        response = resolver.get_resolver_rule(ResolverRuleId=rule_id)
        assert response["ResolverRule"]["Name"] == name
        assert "get-test.example.com" in response["ResolverRule"]["DomainName"]
        resolver.delete_resolver_rule(ResolverRuleId=rule_id)

    def test_tag_resolver_rule(self, resolver):
        name = f"tag-rule-{_uid()}"
        create = resolver.create_resolver_rule(
            CreatorRequestId=_uid(),
            Name=name,
            RuleType="FORWARD",
            DomainName="tag-test.example.com.",
        )
        arn = create["ResolverRule"]["Arn"]
        rule_id = create["ResolverRule"]["Id"]
        resolver.tag_resource(
            ResourceArn=arn,
            Tags=[{"Key": "env", "Value": "test"}],
        )
        tags = resolver.list_tags_for_resource(ResourceArn=arn)["Tags"]
        tag_map = {t["Key"]: t["Value"] for t in tags}
        assert tag_map["env"] == "test"
        resolver.delete_resolver_rule(ResolverRuleId=rule_id)
