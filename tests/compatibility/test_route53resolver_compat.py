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

    def test_get_resolver_endpoint(self, resolver, ec2):
        vpc = ec2.create_vpc(CidrBlock="10.92.0.0/16")
        vpc_id = vpc["Vpc"]["VpcId"]
        subnet1 = ec2.create_subnet(
            VpcId=vpc_id, CidrBlock="10.92.1.0/24", AvailabilityZone="us-east-1a"
        )
        subnet2 = ec2.create_subnet(
            VpcId=vpc_id, CidrBlock="10.92.2.0/24", AvailabilityZone="us-east-1b"
        )
        sg = ec2.create_security_group(
            GroupName=f"get-ep-sg-{_uid()}", Description="test", VpcId=vpc_id
        )
        create_resp = resolver.create_resolver_endpoint(
            CreatorRequestId=_uid(),
            Name=f"get-ep-{_uid()}",
            SecurityGroupIds=[sg["GroupId"]],
            Direction="INBOUND",
            IpAddresses=[
                {"SubnetId": subnet1["Subnet"]["SubnetId"]},
                {"SubnetId": subnet2["Subnet"]["SubnetId"]},
            ],
        )
        endpoint_id = create_resp["ResolverEndpoint"]["Id"]
        try:
            resp = resolver.get_resolver_endpoint(ResolverEndpointId=endpoint_id)
            assert resp["ResolverEndpoint"]["Id"] == endpoint_id
            assert resp["ResolverEndpoint"]["Direction"] == "INBOUND"
        finally:
            resolver.delete_resolver_endpoint(ResolverEndpointId=endpoint_id)

    def test_update_resolver_endpoint(self, resolver, ec2):
        vpc = ec2.create_vpc(CidrBlock="10.93.0.0/16")
        vpc_id = vpc["Vpc"]["VpcId"]
        subnet1 = ec2.create_subnet(
            VpcId=vpc_id, CidrBlock="10.93.1.0/24", AvailabilityZone="us-east-1a"
        )
        subnet2 = ec2.create_subnet(
            VpcId=vpc_id, CidrBlock="10.93.2.0/24", AvailabilityZone="us-east-1b"
        )
        sg = ec2.create_security_group(
            GroupName=f"upd-ep-sg-{_uid()}", Description="test", VpcId=vpc_id
        )
        create_resp = resolver.create_resolver_endpoint(
            CreatorRequestId=_uid(),
            Name=f"upd-ep-{_uid()}",
            SecurityGroupIds=[sg["GroupId"]],
            Direction="INBOUND",
            IpAddresses=[
                {"SubnetId": subnet1["Subnet"]["SubnetId"]},
                {"SubnetId": subnet2["Subnet"]["SubnetId"]},
            ],
        )
        endpoint_id = create_resp["ResolverEndpoint"]["Id"]
        new_name = f"updated-ep-{_uid()}"
        try:
            resp = resolver.update_resolver_endpoint(ResolverEndpointId=endpoint_id, Name=new_name)
            assert resp["ResolverEndpoint"]["Name"] == new_name
        finally:
            resolver.delete_resolver_endpoint(ResolverEndpointId=endpoint_id)

    def test_list_resolver_endpoint_ip_addresses(self, resolver, ec2):
        vpc = ec2.create_vpc(CidrBlock="10.94.0.0/16")
        vpc_id = vpc["Vpc"]["VpcId"]
        subnet1 = ec2.create_subnet(
            VpcId=vpc_id, CidrBlock="10.94.1.0/24", AvailabilityZone="us-east-1a"
        )
        subnet2 = ec2.create_subnet(
            VpcId=vpc_id, CidrBlock="10.94.2.0/24", AvailabilityZone="us-east-1b"
        )
        sg = ec2.create_security_group(
            GroupName=f"list-ip-sg-{_uid()}", Description="test", VpcId=vpc_id
        )
        create_resp = resolver.create_resolver_endpoint(
            CreatorRequestId=_uid(),
            Name=f"list-ip-ep-{_uid()}",
            SecurityGroupIds=[sg["GroupId"]],
            Direction="INBOUND",
            IpAddresses=[
                {"SubnetId": subnet1["Subnet"]["SubnetId"]},
                {"SubnetId": subnet2["Subnet"]["SubnetId"]},
            ],
        )
        endpoint_id = create_resp["ResolverEndpoint"]["Id"]
        try:
            resp = resolver.list_resolver_endpoint_ip_addresses(ResolverEndpointId=endpoint_id)
            assert "IpAddresses" in resp
            assert len(resp["IpAddresses"]) >= 2
        finally:
            resolver.delete_resolver_endpoint(ResolverEndpointId=endpoint_id)

    def test_associate_resolver_endpoint_ip_address(self, resolver, ec2):
        vpc = ec2.create_vpc(CidrBlock="10.95.0.0/16")
        vpc_id = vpc["Vpc"]["VpcId"]
        subnet1 = ec2.create_subnet(
            VpcId=vpc_id, CidrBlock="10.95.1.0/24", AvailabilityZone="us-east-1a"
        )
        subnet2 = ec2.create_subnet(
            VpcId=vpc_id, CidrBlock="10.95.2.0/24", AvailabilityZone="us-east-1b"
        )
        subnet3 = ec2.create_subnet(
            VpcId=vpc_id, CidrBlock="10.95.3.0/24", AvailabilityZone="us-east-1c"
        )
        sg = ec2.create_security_group(
            GroupName=f"assoc-ip-sg-{_uid()}", Description="test", VpcId=vpc_id
        )
        create_resp = resolver.create_resolver_endpoint(
            CreatorRequestId=_uid(),
            Name=f"assoc-ip-ep-{_uid()}",
            SecurityGroupIds=[sg["GroupId"]],
            Direction="INBOUND",
            IpAddresses=[
                {"SubnetId": subnet1["Subnet"]["SubnetId"]},
                {"SubnetId": subnet2["Subnet"]["SubnetId"]},
            ],
        )
        endpoint_id = create_resp["ResolverEndpoint"]["Id"]
        try:
            resp = resolver.associate_resolver_endpoint_ip_address(
                ResolverEndpointId=endpoint_id,
                IpAddress={"SubnetId": subnet3["Subnet"]["SubnetId"]},
            )
            assert resp["ResolverEndpoint"]["Id"] == endpoint_id
            # Verify it was added
            ips = resolver.list_resolver_endpoint_ip_addresses(ResolverEndpointId=endpoint_id)
            assert len(ips["IpAddresses"]) >= 3
        finally:
            resolver.delete_resolver_endpoint(ResolverEndpointId=endpoint_id)

    def test_disassociate_resolver_endpoint_ip_address(self, resolver, ec2):
        vpc = ec2.create_vpc(CidrBlock="10.96.0.0/16")
        vpc_id = vpc["Vpc"]["VpcId"]
        subnet1 = ec2.create_subnet(
            VpcId=vpc_id, CidrBlock="10.96.1.0/24", AvailabilityZone="us-east-1a"
        )
        subnet2 = ec2.create_subnet(
            VpcId=vpc_id, CidrBlock="10.96.2.0/24", AvailabilityZone="us-east-1b"
        )
        subnet3 = ec2.create_subnet(
            VpcId=vpc_id, CidrBlock="10.96.3.0/24", AvailabilityZone="us-east-1c"
        )
        sg = ec2.create_security_group(
            GroupName=f"disassoc-ip-sg-{_uid()}", Description="test", VpcId=vpc_id
        )
        create_resp = resolver.create_resolver_endpoint(
            CreatorRequestId=_uid(),
            Name=f"disassoc-ip-ep-{_uid()}",
            SecurityGroupIds=[sg["GroupId"]],
            Direction="INBOUND",
            IpAddresses=[
                {"SubnetId": subnet1["Subnet"]["SubnetId"]},
                {"SubnetId": subnet2["Subnet"]["SubnetId"]},
                {"SubnetId": subnet3["Subnet"]["SubnetId"]},
            ],
        )
        endpoint_id = create_resp["ResolverEndpoint"]["Id"]
        try:
            # Get the IP address ID for the third subnet
            ips = resolver.list_resolver_endpoint_ip_addresses(ResolverEndpointId=endpoint_id)
            # Find the IP in subnet3
            subnet3_id = subnet3["Subnet"]["SubnetId"]
            ip_to_remove = None
            for ip in ips["IpAddresses"]:
                if ip["SubnetId"] == subnet3_id:
                    ip_to_remove = ip["IpId"]
                    break
            assert ip_to_remove is not None, "Could not find IP for subnet3"

            resp = resolver.disassociate_resolver_endpoint_ip_address(
                ResolverEndpointId=endpoint_id,
                IpAddress={"IpId": ip_to_remove},
            )
            assert resp["ResolverEndpoint"]["Id"] == endpoint_id
        finally:
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

    def test_get_resolver_dnssec_config(self, resolver, ec2):
        vpc = ec2.create_vpc(CidrBlock="10.90.0.0/16")
        vpc_id = vpc["Vpc"]["VpcId"]
        # Must enable DNSSEC first to create the config
        resolver.update_resolver_dnssec_config(ResourceId=vpc_id, Validation="ENABLE")
        resp = resolver.get_resolver_dnssec_config(ResourceId=vpc_id)
        assert "ResolverDNSSECConfig" in resp
        config = resp["ResolverDNSSECConfig"]
        assert config["ResourceId"] == vpc_id

    def test_update_resolver_dnssec_config(self, resolver, ec2):
        vpc = ec2.create_vpc(CidrBlock="10.91.0.0/16")
        vpc_id = vpc["Vpc"]["VpcId"]
        resp = resolver.update_resolver_dnssec_config(
            ResourceId=vpc_id,
            Validation="ENABLE",
        )
        assert "ResolverDNSSECConfig" in resp
        config = resp["ResolverDNSSECConfig"]
        assert config["ResourceId"] == vpc_id


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


class TestRoute53ResolverFirewallDomainList:
    """Tests for firewall domain list CRUD operations."""

    @pytest.fixture
    def resolver(self):
        return make_client("route53resolver")

    def test_create_firewall_domain_list(self, resolver):
        uid = _uid()
        resp = resolver.create_firewall_domain_list(
            CreatorRequestId=uid,
            Name=f"fdl-{uid}",
        )
        fdl = resp["FirewallDomainList"]
        assert fdl["Name"] == f"fdl-{uid}"
        assert "Id" in fdl
        resolver.delete_firewall_domain_list(FirewallDomainListId=fdl["Id"])

    def test_get_firewall_domain_list(self, resolver):
        uid = _uid()
        create = resolver.create_firewall_domain_list(
            CreatorRequestId=uid,
            Name=f"get-fdl-{uid}",
        )
        fdl_id = create["FirewallDomainList"]["Id"]
        try:
            resp = resolver.get_firewall_domain_list(FirewallDomainListId=fdl_id)
            assert resp["FirewallDomainList"]["Id"] == fdl_id
            assert resp["FirewallDomainList"]["Name"] == f"get-fdl-{uid}"
        finally:
            resolver.delete_firewall_domain_list(FirewallDomainListId=fdl_id)

    def test_delete_firewall_domain_list(self, resolver):
        uid = _uid()
        create = resolver.create_firewall_domain_list(
            CreatorRequestId=uid,
            Name=f"del-fdl-{uid}",
        )
        fdl_id = create["FirewallDomainList"]["Id"]
        resp = resolver.delete_firewall_domain_list(FirewallDomainListId=fdl_id)
        assert resp["FirewallDomainList"]["Id"] == fdl_id

    def test_update_firewall_domains(self, resolver):
        uid = _uid()
        create = resolver.create_firewall_domain_list(
            CreatorRequestId=uid,
            Name=f"upd-fdl-{uid}",
        )
        fdl_id = create["FirewallDomainList"]["Id"]
        try:
            resp = resolver.update_firewall_domains(
                FirewallDomainListId=fdl_id,
                Operation="ADD",
                Domains=["example.com.", "blocked.org."],
            )
            assert resp["Id"] == fdl_id
            assert resp["Name"] == f"upd-fdl-{uid}"
        finally:
            resolver.delete_firewall_domain_list(FirewallDomainListId=fdl_id)

    def test_list_firewall_domains(self, resolver):
        uid = _uid()
        create = resolver.create_firewall_domain_list(
            CreatorRequestId=uid,
            Name=f"listdom-fdl-{uid}",
        )
        fdl_id = create["FirewallDomainList"]["Id"]
        try:
            resolver.update_firewall_domains(
                FirewallDomainListId=fdl_id,
                Operation="ADD",
                Domains=["test1.com.", "test2.com."],
            )
            resp = resolver.list_firewall_domains(FirewallDomainListId=fdl_id)
            assert "Domains" in resp
            assert len(resp["Domains"]) >= 2
        finally:
            resolver.delete_firewall_domain_list(FirewallDomainListId=fdl_id)


class TestRoute53ResolverFirewallRuleGroup:
    """Tests for firewall rule group CRUD operations."""

    @pytest.fixture
    def resolver(self):
        return make_client("route53resolver")

    def test_create_firewall_rule_group(self, resolver):
        uid = _uid()
        resp = resolver.create_firewall_rule_group(
            CreatorRequestId=uid,
            Name=f"frg-{uid}",
        )
        frg = resp["FirewallRuleGroup"]
        assert frg["Name"] == f"frg-{uid}"
        assert "Id" in frg
        resolver.delete_firewall_rule_group(FirewallRuleGroupId=frg["Id"])

    def test_get_firewall_rule_group(self, resolver):
        uid = _uid()
        create = resolver.create_firewall_rule_group(
            CreatorRequestId=uid,
            Name=f"get-frg-{uid}",
        )
        frg_id = create["FirewallRuleGroup"]["Id"]
        try:
            resp = resolver.get_firewall_rule_group(FirewallRuleGroupId=frg_id)
            assert resp["FirewallRuleGroup"]["Id"] == frg_id
            assert resp["FirewallRuleGroup"]["Name"] == f"get-frg-{uid}"
        finally:
            resolver.delete_firewall_rule_group(FirewallRuleGroupId=frg_id)

    def test_delete_firewall_rule_group(self, resolver):
        uid = _uid()
        create = resolver.create_firewall_rule_group(
            CreatorRequestId=uid,
            Name=f"del-frg-{uid}",
        )
        frg_id = create["FirewallRuleGroup"]["Id"]
        resp = resolver.delete_firewall_rule_group(FirewallRuleGroupId=frg_id)
        assert resp["FirewallRuleGroup"]["Id"] == frg_id


class TestRoute53ResolverFirewallRules:
    """Tests for firewall rule CRUD operations."""

    @pytest.fixture
    def resolver(self):
        return make_client("route53resolver")

    @pytest.fixture
    def firewall_resources(self, resolver):
        """Create a rule group and domain list for firewall rule tests."""
        uid = _uid()
        fdl = resolver.create_firewall_domain_list(
            CreatorRequestId=f"fdl-{uid}",
            Name=f"fdl-rules-{uid}",
        )
        fdl_id = fdl["FirewallDomainList"]["Id"]
        frg = resolver.create_firewall_rule_group(
            CreatorRequestId=f"frg-{uid}",
            Name=f"frg-rules-{uid}",
        )
        frg_id = frg["FirewallRuleGroup"]["Id"]
        yield {"fdl_id": fdl_id, "frg_id": frg_id}
        resolver.delete_firewall_rule_group(FirewallRuleGroupId=frg_id)
        resolver.delete_firewall_domain_list(FirewallDomainListId=fdl_id)

    def test_create_firewall_rule(self, resolver, firewall_resources):
        resp = resolver.create_firewall_rule(
            CreatorRequestId=_uid(),
            FirewallRuleGroupId=firewall_resources["frg_id"],
            FirewallDomainListId=firewall_resources["fdl_id"],
            Priority=100,
            Action="BLOCK",
            BlockResponse="NODATA",
            Name=f"rule-{_uid()}",
        )
        rule = resp["FirewallRule"]
        assert rule["FirewallRuleGroupId"] == firewall_resources["frg_id"]
        assert rule["Action"] == "BLOCK"
        assert rule["Priority"] == 100

    def test_list_firewall_rules(self, resolver, firewall_resources):
        resolver.create_firewall_rule(
            CreatorRequestId=_uid(),
            FirewallRuleGroupId=firewall_resources["frg_id"],
            FirewallDomainListId=firewall_resources["fdl_id"],
            Priority=100,
            Action="BLOCK",
            BlockResponse="NODATA",
            Name=f"list-rule-{_uid()}",
        )
        resp = resolver.list_firewall_rules(
            FirewallRuleGroupId=firewall_resources["frg_id"],
        )
        assert "FirewallRules" in resp
        assert len(resp["FirewallRules"]) >= 1

    def test_update_firewall_rule(self, resolver, firewall_resources):
        resolver.create_firewall_rule(
            CreatorRequestId=_uid(),
            FirewallRuleGroupId=firewall_resources["frg_id"],
            FirewallDomainListId=firewall_resources["fdl_id"],
            Priority=100,
            Action="BLOCK",
            BlockResponse="NODATA",
            Name=f"upd-rule-{_uid()}",
        )
        resp = resolver.update_firewall_rule(
            FirewallRuleGroupId=firewall_resources["frg_id"],
            FirewallDomainListId=firewall_resources["fdl_id"],
            Priority=200,
            Action="ALLOW",
            Name=f"upd-rule-renamed-{_uid()}",
        )
        rule = resp["FirewallRule"]
        assert rule["Priority"] == 200
        assert rule["Action"] == "ALLOW"

    def test_delete_firewall_rule(self, resolver, firewall_resources):
        resolver.create_firewall_rule(
            CreatorRequestId=_uid(),
            FirewallRuleGroupId=firewall_resources["frg_id"],
            FirewallDomainListId=firewall_resources["fdl_id"],
            Priority=100,
            Action="BLOCK",
            BlockResponse="NODATA",
            Name=f"del-rule-{_uid()}",
        )
        resp = resolver.delete_firewall_rule(
            FirewallRuleGroupId=firewall_resources["frg_id"],
            FirewallDomainListId=firewall_resources["fdl_id"],
        )
        rule = resp["FirewallRule"]
        assert rule["FirewallRuleGroupId"] == firewall_resources["frg_id"]


class TestRoute53ResolverFirewallRuleGroupAssociation:
    """Tests for firewall rule group association operations."""

    @pytest.fixture
    def resolver(self):
        return make_client("route53resolver")

    @pytest.fixture
    def ec2(self):
        return make_client("ec2")

    def test_associate_firewall_rule_group(self, resolver, ec2):
        uid = _uid()
        frg = resolver.create_firewall_rule_group(
            CreatorRequestId=uid,
            Name=f"assoc-frg-{uid}",
        )
        frg_id = frg["FirewallRuleGroup"]["Id"]
        vpc = ec2.create_vpc(CidrBlock="10.100.0.0/16")
        vpc_id = vpc["Vpc"]["VpcId"]
        try:
            resp = resolver.associate_firewall_rule_group(
                CreatorRequestId=_uid(),
                FirewallRuleGroupId=frg_id,
                VpcId=vpc_id,
                Priority=101,
                Name=f"assoc-{uid}",
            )
            assoc = resp["FirewallRuleGroupAssociation"]
            assert assoc["FirewallRuleGroupId"] == frg_id
            assert assoc["VpcId"] == vpc_id
            assert "Id" in assoc
            # Cleanup
            resolver.disassociate_firewall_rule_group(
                FirewallRuleGroupAssociationId=assoc["Id"],
            )
        finally:
            resolver.delete_firewall_rule_group(FirewallRuleGroupId=frg_id)

    def test_get_firewall_rule_group_association(self, resolver, ec2):
        uid = _uid()
        frg = resolver.create_firewall_rule_group(
            CreatorRequestId=uid,
            Name=f"get-assoc-frg-{uid}",
        )
        frg_id = frg["FirewallRuleGroup"]["Id"]
        vpc = ec2.create_vpc(CidrBlock="10.101.0.0/16")
        vpc_id = vpc["Vpc"]["VpcId"]
        try:
            assoc_resp = resolver.associate_firewall_rule_group(
                CreatorRequestId=_uid(),
                FirewallRuleGroupId=frg_id,
                VpcId=vpc_id,
                Priority=102,
                Name=f"get-assoc-{uid}",
            )
            assoc_id = assoc_resp["FirewallRuleGroupAssociation"]["Id"]

            resp = resolver.get_firewall_rule_group_association(
                FirewallRuleGroupAssociationId=assoc_id,
            )
            assert resp["FirewallRuleGroupAssociation"]["Id"] == assoc_id
            assert resp["FirewallRuleGroupAssociation"]["FirewallRuleGroupId"] == frg_id
            # Cleanup
            resolver.disassociate_firewall_rule_group(
                FirewallRuleGroupAssociationId=assoc_id,
            )
        finally:
            resolver.delete_firewall_rule_group(FirewallRuleGroupId=frg_id)

    def test_disassociate_firewall_rule_group(self, resolver, ec2):
        uid = _uid()
        frg = resolver.create_firewall_rule_group(
            CreatorRequestId=uid,
            Name=f"disassoc-frg-{uid}",
        )
        frg_id = frg["FirewallRuleGroup"]["Id"]
        vpc = ec2.create_vpc(CidrBlock="10.102.0.0/16")
        vpc_id = vpc["Vpc"]["VpcId"]
        try:
            assoc_resp = resolver.associate_firewall_rule_group(
                CreatorRequestId=_uid(),
                FirewallRuleGroupId=frg_id,
                VpcId=vpc_id,
                Priority=103,
                Name=f"disassoc-{uid}",
            )
            assoc_id = assoc_resp["FirewallRuleGroupAssociation"]["Id"]

            resp = resolver.disassociate_firewall_rule_group(
                FirewallRuleGroupAssociationId=assoc_id,
            )
            assert resp["FirewallRuleGroupAssociation"]["Id"] == assoc_id
        finally:
            resolver.delete_firewall_rule_group(FirewallRuleGroupId=frg_id)


class TestRoute53ResolverQueryLogConfigCleanup:
    """Tests for query log config delete and disassociate operations."""

    @pytest.fixture
    def resolver(self):
        return make_client("route53resolver")

    @pytest.fixture
    def ec2(self):
        return make_client("ec2")

    def test_delete_resolver_query_log_config(self, resolver):
        uid = _uid()
        create = resolver.create_resolver_query_log_config(
            Name=f"del-qlc-{uid}",
            DestinationArn=f"arn:aws:s3:::del-bucket-{uid}",
            CreatorRequestId=uid,
        )
        qlc_id = create["ResolverQueryLogConfig"]["Id"]
        resp = resolver.delete_resolver_query_log_config(
            ResolverQueryLogConfigId=qlc_id,
        )
        assert resp["ResolverQueryLogConfig"]["Id"] == qlc_id

    def test_disassociate_resolver_query_log_config(self, resolver, ec2):
        uid = _uid()
        qlc = resolver.create_resolver_query_log_config(
            Name=f"disassoc-qlc-{uid}",
            DestinationArn=f"arn:aws:s3:::disassoc-bucket-{uid}",
            CreatorRequestId=uid,
        )
        qlc_id = qlc["ResolverQueryLogConfig"]["Id"]
        vpc = ec2.create_vpc(CidrBlock="10.103.0.0/16")
        vpc_id = vpc["Vpc"]["VpcId"]
        resolver.associate_resolver_query_log_config(
            ResolverQueryLogConfigId=qlc_id,
            ResourceId=vpc_id,
        )
        resp = resolver.disassociate_resolver_query_log_config(
            ResolverQueryLogConfigId=qlc_id,
            ResourceId=vpc_id,
        )
        assoc = resp["ResolverQueryLogConfigAssociation"]
        assert assoc["ResolverQueryLogConfigId"] == qlc_id
        assert assoc["ResourceId"] == vpc_id


class TestRoute53ResolverRuleUpdate:
    """Tests for UpdateResolverRule operation."""

    @pytest.fixture
    def resolver(self):
        return make_client("route53resolver")

    def test_update_resolver_rule(self, resolver):
        uid = _uid()
        create = resolver.create_resolver_rule(
            CreatorRequestId=uid,
            Name=f"upd-rule-{uid}",
            RuleType="SYSTEM",
            DomainName=f"update-{uid}.example.com.",
        )
        rule_id = create["ResolverRule"]["Id"]
        new_name = f"renamed-rule-{uid}"
        try:
            resp = resolver.update_resolver_rule(
                ResolverRuleId=rule_id,
                Config={"Name": new_name},
            )
            assert resp["ResolverRule"]["Id"] == rule_id
            assert resp["ResolverRule"]["Name"] == new_name
        finally:
            resolver.delete_resolver_rule(ResolverRuleId=rule_id)


class TestRoute53ResolverRulePolicy:
    """Tests for Put/Get ResolverRulePolicy operations."""

    @pytest.fixture
    def resolver(self):
        return make_client("route53resolver")

    def test_put_resolver_rule_policy(self, resolver):
        uid = _uid()
        create = resolver.create_resolver_rule(
            CreatorRequestId=uid,
            Name=f"policy-rule-{uid}",
            RuleType="SYSTEM",
            DomainName=f"policy-{uid}.example.com.",
        )
        rule_arn = create["ResolverRule"]["Arn"]
        rule_id = create["ResolverRule"]["Id"]
        policy = (
            '{"Version":"2012-10-17","Statement":[{"Effect":"Allow",'
            '"Principal":{"AWS":"arn:aws:iam::123456789012:root"},'
            '"Action":["route53resolver:GetResolverRule","route53resolver:AssociateResolverRule",'
            '"route53resolver:ListResolverRules"],'
            f'"Resource":"{rule_arn}"}}]}}'
        )
        try:
            resp = resolver.put_resolver_rule_policy(Arn=rule_arn, ResolverRulePolicy=policy)
            assert resp["ReturnValue"] is True
        finally:
            resolver.delete_resolver_rule(ResolverRuleId=rule_id)

    def test_get_resolver_rule_policy(self, resolver):
        uid = _uid()
        create = resolver.create_resolver_rule(
            CreatorRequestId=uid,
            Name=f"get-policy-rule-{uid}",
            RuleType="SYSTEM",
            DomainName=f"get-policy-{uid}.example.com.",
        )
        rule_arn = create["ResolverRule"]["Arn"]
        rule_id = create["ResolverRule"]["Id"]
        policy = (
            '{"Version":"2012-10-17","Statement":[{"Effect":"Allow",'
            '"Principal":{"AWS":"arn:aws:iam::123456789012:root"},'
            '"Action":"route53resolver:GetResolverRule",'
            f'"Resource":"{rule_arn}"}}]}}'
        )
        try:
            resolver.put_resolver_rule_policy(Arn=rule_arn, ResolverRulePolicy=policy)
            resp = resolver.get_resolver_rule_policy(Arn=rule_arn)
            assert "ResolverRulePolicy" in resp
            assert len(resp["ResolverRulePolicy"]) > 0
        finally:
            resolver.delete_resolver_rule(ResolverRuleId=rule_id)


class TestRoute53ResolverFirewallRuleGroupPolicy:
    """Tests for Put/Get FirewallRuleGroupPolicy operations."""

    @pytest.fixture
    def resolver(self):
        return make_client("route53resolver")

    def test_put_firewall_rule_group_policy(self, resolver):
        uid = _uid()
        create = resolver.create_firewall_rule_group(
            CreatorRequestId=uid,
            Name=f"policy-frg-{uid}",
        )
        frg_arn = create["FirewallRuleGroup"]["Arn"]
        frg_id = create["FirewallRuleGroup"]["Id"]
        policy = (
            '{"Version":"2012-10-17","Statement":[{"Effect":"Allow",'
            '"Principal":{"AWS":"arn:aws:iam::123456789012:root"},'
            '"Action":"route53resolver:AssociateFirewallRuleGroup",'
            f'"Resource":"{frg_arn}"}}]}}'
        )
        try:
            resp = resolver.put_firewall_rule_group_policy(
                Arn=frg_arn, FirewallRuleGroupPolicy=policy
            )
            assert resp["ReturnValue"] is True
        finally:
            resolver.delete_firewall_rule_group(FirewallRuleGroupId=frg_id)

    def test_get_firewall_rule_group_policy(self, resolver):
        uid = _uid()
        create = resolver.create_firewall_rule_group(
            CreatorRequestId=uid,
            Name=f"get-policy-frg-{uid}",
        )
        frg_arn = create["FirewallRuleGroup"]["Arn"]
        frg_id = create["FirewallRuleGroup"]["Id"]
        policy = (
            '{"Version":"2012-10-17","Statement":[{"Effect":"Allow",'
            '"Principal":{"AWS":"arn:aws:iam::123456789012:root"},'
            '"Action":"route53resolver:AssociateFirewallRuleGroup",'
            f'"Resource":"{frg_arn}"}}]}}'
        )
        try:
            resolver.put_firewall_rule_group_policy(Arn=frg_arn, FirewallRuleGroupPolicy=policy)
            resp = resolver.get_firewall_rule_group_policy(Arn=frg_arn)
            assert "FirewallRuleGroupPolicy" in resp
            assert len(resp["FirewallRuleGroupPolicy"]) > 0
        finally:
            resolver.delete_firewall_rule_group(FirewallRuleGroupId=frg_id)


class TestRoute53ResolverQueryLogConfigPolicy:
    """Tests for Put/Get ResolverQueryLogConfigPolicy operations."""

    @pytest.fixture
    def resolver(self):
        return make_client("route53resolver")

    def test_put_resolver_query_log_config_policy(self, resolver):
        uid = _uid()
        create = resolver.create_resolver_query_log_config(
            Name=f"policy-qlc-{uid}",
            DestinationArn=f"arn:aws:s3:::policy-bucket-{uid}",
            CreatorRequestId=uid,
        )
        qlc_arn = create["ResolverQueryLogConfig"]["Arn"]
        qlc_id = create["ResolverQueryLogConfig"]["Id"]
        policy = (
            '{"Version":"2012-10-17","Statement":[{"Effect":"Allow",'
            '"Principal":{"AWS":"arn:aws:iam::123456789012:root"},'
            '"Action":"route53resolver:AssociateResolverQueryLogConfig",'
            f'"Resource":"{qlc_arn}"}}]}}'
        )
        try:
            resp = resolver.put_resolver_query_log_config_policy(
                Arn=qlc_arn, ResolverQueryLogConfigPolicy=policy
            )
            assert resp["ReturnValue"] is True
        finally:
            resolver.delete_resolver_query_log_config(ResolverQueryLogConfigId=qlc_id)

    def test_get_resolver_query_log_config_policy(self, resolver):
        uid = _uid()
        create = resolver.create_resolver_query_log_config(
            Name=f"get-policy-qlc-{uid}",
            DestinationArn=f"arn:aws:s3:::get-policy-bucket-{uid}",
            CreatorRequestId=uid,
        )
        qlc_arn = create["ResolverQueryLogConfig"]["Arn"]
        qlc_id = create["ResolverQueryLogConfig"]["Id"]
        policy = (
            '{"Version":"2012-10-17","Statement":[{"Effect":"Allow",'
            '"Principal":{"AWS":"arn:aws:iam::123456789012:root"},'
            '"Action":"route53resolver:AssociateResolverQueryLogConfig",'
            f'"Resource":"{qlc_arn}"}}]}}'
        )
        try:
            resolver.put_resolver_query_log_config_policy(
                Arn=qlc_arn, ResolverQueryLogConfigPolicy=policy
            )
            resp = resolver.get_resolver_query_log_config_policy(Arn=qlc_arn)
            assert "ResolverQueryLogConfigPolicy" in resp
            assert len(resp["ResolverQueryLogConfigPolicy"]) > 0
        finally:
            resolver.delete_resolver_query_log_config(ResolverQueryLogConfigId=qlc_id)
