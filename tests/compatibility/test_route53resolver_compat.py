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

    def test_untag_resource(self, resolver):
        name = f"untag-rule-{_uid()}"
        create = resolver.create_resolver_rule(
            CreatorRequestId=_uid(),
            Name=name,
            RuleType="FORWARD",
            DomainName="untag.example.com.",
        )
        arn = create["ResolverRule"]["Arn"]
        rule_id = create["ResolverRule"]["Id"]
        try:
            resolver.tag_resource(
                ResourceArn=arn,
                Tags=[{"Key": "k1", "Value": "v1"}, {"Key": "k2", "Value": "v2"}],
            )
            resolver.untag_resource(ResourceArn=arn, TagKeys=["k1"])
            tags = resolver.list_tags_for_resource(ResourceArn=arn)["Tags"]
            keys = [t["Key"] for t in tags]
            assert "k1" not in keys
            assert "k2" in keys
        finally:
            resolver.delete_resolver_rule(ResolverRuleId=rule_id)

    def test_associate_resolver_rule(self, resolver, ec2):
        rule = resolver.create_resolver_rule(
            CreatorRequestId=_uid(),
            Name=f"assoc-rule-{_uid()}",
            RuleType="SYSTEM",
            DomainName="assoc.example.com.",
        )
        rule_id = rule["ResolverRule"]["Id"]
        vpc = ec2.create_vpc(CidrBlock="10.80.0.0/16")
        vpc_id = vpc["Vpc"]["VpcId"]
        try:
            resp = resolver.associate_resolver_rule(ResolverRuleId=rule_id, VPCId=vpc_id)
            assoc = resp["ResolverRuleAssociation"]
            assert assoc["ResolverRuleId"] == rule_id
            assert assoc["VPCId"] == vpc_id
            assert "Id" in assoc

            # Cleanup
            resolver.disassociate_resolver_rule(ResolverRuleId=rule_id, VPCId=vpc_id)
        finally:
            resolver.delete_resolver_rule(ResolverRuleId=rule_id)

    def test_get_resolver_rule_association(self, resolver, ec2):
        rule = resolver.create_resolver_rule(
            CreatorRequestId=_uid(),
            Name=f"get-assoc-{_uid()}",
            RuleType="SYSTEM",
            DomainName="getassoc.example.com.",
        )
        rule_id = rule["ResolverRule"]["Id"]
        vpc = ec2.create_vpc(CidrBlock="10.81.0.0/16")
        vpc_id = vpc["Vpc"]["VpcId"]
        try:
            assoc_resp = resolver.associate_resolver_rule(ResolverRuleId=rule_id, VPCId=vpc_id)
            assoc_id = assoc_resp["ResolverRuleAssociation"]["Id"]

            resp = resolver.get_resolver_rule_association(ResolverRuleAssociationId=assoc_id)
            assert resp["ResolverRuleAssociation"]["ResolverRuleId"] == rule_id
            assert resp["ResolverRuleAssociation"]["Status"] == "COMPLETE"

            resolver.disassociate_resolver_rule(ResolverRuleId=rule_id, VPCId=vpc_id)
        finally:
            resolver.delete_resolver_rule(ResolverRuleId=rule_id)

    def test_disassociate_resolver_rule(self, resolver, ec2):
        rule = resolver.create_resolver_rule(
            CreatorRequestId=_uid(),
            Name=f"disassoc-rule-{_uid()}",
            RuleType="SYSTEM",
            DomainName="disassoc.example.com.",
        )
        rule_id = rule["ResolverRule"]["Id"]
        vpc = ec2.create_vpc(CidrBlock="10.82.0.0/16")
        vpc_id = vpc["Vpc"]["VpcId"]
        try:
            resolver.associate_resolver_rule(ResolverRuleId=rule_id, VPCId=vpc_id)
            resp = resolver.disassociate_resolver_rule(ResolverRuleId=rule_id, VPCId=vpc_id)
            assert resp["ResolverRuleAssociation"]["ResolverRuleId"] == rule_id
        finally:
            resolver.delete_resolver_rule(ResolverRuleId=rule_id)

    def test_list_resolver_rule_associations(self, resolver, ec2):
        rule = resolver.create_resolver_rule(
            CreatorRequestId=_uid(),
            Name=f"list-assoc-{_uid()}",
            RuleType="SYSTEM",
            DomainName="listassoc.example.com.",
        )
        rule_id = rule["ResolverRule"]["Id"]
        vpc = ec2.create_vpc(CidrBlock="10.83.0.0/16")
        vpc_id = vpc["Vpc"]["VpcId"]
        try:
            resolver.associate_resolver_rule(ResolverRuleId=rule_id, VPCId=vpc_id)
            resp = resolver.list_resolver_rule_associations()
            assert "ResolverRuleAssociations" in resp
            rule_ids = [a["ResolverRuleId"] for a in resp["ResolverRuleAssociations"]]
            assert rule_id in rule_ids

            resolver.disassociate_resolver_rule(ResolverRuleId=rule_id, VPCId=vpc_id)
        finally:
            resolver.delete_resolver_rule(ResolverRuleId=rule_id)

    def test_create_resolver_query_log_config(self, resolver):
        uid = _uid()
        resp = resolver.create_resolver_query_log_config(
            Name=f"qlc-{uid}",
            DestinationArn=f"arn:aws:s3:::my-bucket-{uid}",
            CreatorRequestId=uid,
        )
        qlc = resp["ResolverQueryLogConfig"]
        assert qlc["Name"] == f"qlc-{uid}"
        assert "Id" in qlc

    def test_get_resolver_query_log_config(self, resolver):
        uid = _uid()
        create = resolver.create_resolver_query_log_config(
            Name=f"get-qlc-{uid}",
            DestinationArn=f"arn:aws:s3:::get-bucket-{uid}",
            CreatorRequestId=uid,
        )
        qlc_id = create["ResolverQueryLogConfig"]["Id"]

        resp = resolver.get_resolver_query_log_config(ResolverQueryLogConfigId=qlc_id)
        assert resp["ResolverQueryLogConfig"]["Name"] == f"get-qlc-{uid}"

    def test_list_resolver_query_log_configs(self, resolver):
        uid = _uid()
        resolver.create_resolver_query_log_config(
            Name=f"list-qlc-{uid}",
            DestinationArn=f"arn:aws:s3:::list-bucket-{uid}",
            CreatorRequestId=uid,
        )
        resp = resolver.list_resolver_query_log_configs()
        assert "ResolverQueryLogConfigs" in resp
        names = [c["Name"] for c in resp["ResolverQueryLogConfigs"]]
        assert f"list-qlc-{uid}" in names

    def test_associate_resolver_query_log_config(self, resolver, ec2):
        uid = _uid()
        qlc = resolver.create_resolver_query_log_config(
            Name=f"assoc-qlc-{uid}",
            DestinationArn=f"arn:aws:s3:::assoc-bucket-{uid}",
            CreatorRequestId=uid,
        )
        qlc_id = qlc["ResolverQueryLogConfig"]["Id"]

        vpc = ec2.create_vpc(CidrBlock="10.84.0.0/16")
        vpc_id = vpc["Vpc"]["VpcId"]

        resp = resolver.associate_resolver_query_log_config(
            ResolverQueryLogConfigId=qlc_id, ResourceId=vpc_id
        )
        assoc = resp["ResolverQueryLogConfigAssociation"]
        assert assoc["ResolverQueryLogConfigId"] == qlc_id
        assert assoc["ResourceId"] == vpc_id
        assert "Id" in assoc

    def test_get_resolver_query_log_config_association(self, resolver, ec2):
        uid = _uid()
        qlc = resolver.create_resolver_query_log_config(
            Name=f"get-assoc-qlc-{uid}",
            DestinationArn=f"arn:aws:s3:::get-assoc-bucket-{uid}",
            CreatorRequestId=uid,
        )
        qlc_id = qlc["ResolverQueryLogConfig"]["Id"]

        vpc = ec2.create_vpc(CidrBlock="10.85.0.0/16")
        vpc_id = vpc["Vpc"]["VpcId"]

        assoc_resp = resolver.associate_resolver_query_log_config(
            ResolverQueryLogConfigId=qlc_id, ResourceId=vpc_id
        )
        assoc_id = assoc_resp["ResolverQueryLogConfigAssociation"]["Id"]

        resp = resolver.get_resolver_query_log_config_association(
            ResolverQueryLogConfigAssociationId=assoc_id
        )
        assert resp["ResolverQueryLogConfigAssociation"]["ResolverQueryLogConfigId"] == qlc_id

    def test_list_resolver_query_log_config_associations(self, resolver, ec2):
        uid = _uid()
        qlc = resolver.create_resolver_query_log_config(
            Name=f"list-assoc-qlc-{uid}",
            DestinationArn=f"arn:aws:s3:::list-assoc-bucket-{uid}",
            CreatorRequestId=uid,
        )
        qlc_id = qlc["ResolverQueryLogConfig"]["Id"]

        vpc = ec2.create_vpc(CidrBlock="10.86.0.0/16")
        vpc_id = vpc["Vpc"]["VpcId"]

        resolver.associate_resolver_query_log_config(
            ResolverQueryLogConfigId=qlc_id, ResourceId=vpc_id
        )

        resp = resolver.list_resolver_query_log_config_associations()
        assert "ResolverQueryLogConfigAssociations" in resp
        config_ids = [
            a["ResolverQueryLogConfigId"] for a in resp["ResolverQueryLogConfigAssociations"]
        ]
        assert qlc_id in config_ids

    def test_list_resolver_dnssec_configs(self, resolver):
        resp = resolver.list_resolver_dnssec_configs()
        assert "ResolverDnssecConfigs" in resp


class TestRoute53ResolverGapStubs:
    """Tests for gap ops: firewall, domain lists, rule groups, resolvers."""

    @pytest.fixture
    def resolver(self):
        return make_client("route53resolver")

    def test_list_firewall_configs(self, resolver):
        resp = resolver.list_firewall_configs()
        assert "FirewallConfigs" in resp

    def test_list_firewall_domain_lists(self, resolver):
        resp = resolver.list_firewall_domain_lists()
        assert "FirewallDomainLists" in resp

    def test_list_firewall_rule_groups(self, resolver):
        resp = resolver.list_firewall_rule_groups()
        assert "FirewallRuleGroups" in resp

    def test_list_firewall_rule_group_associations(self, resolver):
        resp = resolver.list_firewall_rule_group_associations()
        assert "FirewallRuleGroupAssociations" in resp

    def test_list_outpost_resolvers(self, resolver):
        resp = resolver.list_outpost_resolvers()
        assert "OutpostResolvers" in resp

    def test_list_resolver_configs(self, resolver):
        resp = resolver.list_resolver_configs()
        assert "ResolverConfigs" in resp
