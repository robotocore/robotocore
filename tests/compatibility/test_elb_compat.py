"""ELB Classic (Elastic Load Balancing v1) compatibility tests."""

import uuid

import pytest

from tests.compatibility.conftest import make_client


@pytest.fixture
def elb():
    return make_client("elb")


def _unique(prefix):
    return f"{prefix}-{uuid.uuid4().hex[:8]}"


@pytest.fixture
def load_balancer(elb):
    """Create a classic load balancer and clean it up after the test."""
    name = _unique("clb")
    elb.create_load_balancer(
        LoadBalancerName=name,
        Listeners=[
            {
                "Protocol": "HTTP",
                "LoadBalancerPort": 80,
                "InstanceProtocol": "HTTP",
                "InstancePort": 80,
            }
        ],
        AvailabilityZones=["us-east-1a"],
    )
    yield name
    try:
        elb.delete_load_balancer(LoadBalancerName=name)
    except Exception:
        pass  # best-effort cleanup


class TestELBClassicLoadBalancerOperations:
    def test_create_and_describe_load_balancer(self, elb):
        name = _unique("clb")
        resp = elb.create_load_balancer(
            LoadBalancerName=name,
            Listeners=[
                {
                    "Protocol": "HTTP",
                    "LoadBalancerPort": 80,
                    "InstanceProtocol": "HTTP",
                    "InstancePort": 80,
                }
            ],
            AvailabilityZones=["us-east-1a"],
        )
        try:
            assert "DNSName" in resp

            desc = elb.describe_load_balancers(LoadBalancerNames=[name])
            lbs = desc["LoadBalancerDescriptions"]
            assert len(lbs) == 1
            assert lbs[0]["LoadBalancerName"] == name
            assert lbs[0]["ListenerDescriptions"][0]["Listener"]["LoadBalancerPort"] == 80
        finally:
            elb.delete_load_balancer(LoadBalancerName=name)

    def test_list_load_balancers(self, elb, load_balancer):
        all_lbs = elb.describe_load_balancers()
        names = [lb["LoadBalancerName"] for lb in all_lbs["LoadBalancerDescriptions"]]
        assert load_balancer in names

    def test_delete_load_balancer(self, elb):
        name = _unique("clb")
        elb.create_load_balancer(
            LoadBalancerName=name,
            Listeners=[
                {
                    "Protocol": "HTTP",
                    "LoadBalancerPort": 80,
                    "InstanceProtocol": "HTTP",
                    "InstancePort": 80,
                }
            ],
            AvailabilityZones=["us-east-1a"],
        )
        elb.delete_load_balancer(LoadBalancerName=name)

        with pytest.raises(elb.exceptions.AccessPointNotFoundException):
            elb.describe_load_balancers(LoadBalancerNames=[name])


class TestELBClassicAttributes:
    def test_describe_load_balancer_attributes(self, elb, load_balancer):
        resp = elb.describe_load_balancer_attributes(LoadBalancerName=load_balancer)
        attrs = resp["LoadBalancerAttributes"]
        assert "CrossZoneLoadBalancing" in attrs
        assert "ConnectionDraining" in attrs
        assert "ConnectionSettings" in attrs

    def test_modify_load_balancer_attributes(self, elb, load_balancer):
        resp = elb.modify_load_balancer_attributes(
            LoadBalancerName=load_balancer,
            LoadBalancerAttributes={
                "CrossZoneLoadBalancing": {"Enabled": True},
            },
        )
        attrs = resp["LoadBalancerAttributes"]
        assert attrs["CrossZoneLoadBalancing"]["Enabled"] is True

        # Verify the change persisted
        desc = elb.describe_load_balancer_attributes(LoadBalancerName=load_balancer)
        assert desc["LoadBalancerAttributes"]["CrossZoneLoadBalancing"]["Enabled"] is True


class TestELBClassicHealthCheck:
    def test_configure_health_check(self, elb, load_balancer):
        resp = elb.configure_health_check(
            LoadBalancerName=load_balancer,
            HealthCheck={
                "Target": "HTTP:80/health",
                "Interval": 30,
                "Timeout": 5,
                "UnhealthyThreshold": 2,
                "HealthyThreshold": 10,
            },
        )
        hc = resp["HealthCheck"]
        assert hc["Target"] == "HTTP:80/health"
        assert hc["Interval"] == 30
        assert hc["Timeout"] == 5
        assert hc["UnhealthyThreshold"] == 2
        assert hc["HealthyThreshold"] == 10

        # Verify via describe
        desc = elb.describe_load_balancers(LoadBalancerNames=[load_balancer])
        lb_hc = desc["LoadBalancerDescriptions"][0]["HealthCheck"]
        assert lb_hc["Target"] == "HTTP:80/health"


class TestELBClassicInstances:
    def test_register_and_deregister_instances(self, elb, load_balancer):
        instance_id = "i-1234567890abcdef0"

        elb.register_instances_with_load_balancer(
            LoadBalancerName=load_balancer,
            Instances=[{"InstanceId": instance_id}],
        )

        # Verify instance is registered
        desc = elb.describe_load_balancers(LoadBalancerNames=[load_balancer])
        instance_ids = [i["InstanceId"] for i in desc["LoadBalancerDescriptions"][0]["Instances"]]
        assert instance_id in instance_ids

        # Deregister
        elb.deregister_instances_from_load_balancer(
            LoadBalancerName=load_balancer,
            Instances=[{"InstanceId": instance_id}],
        )

        desc2 = elb.describe_load_balancers(LoadBalancerNames=[load_balancer])
        instance_ids2 = [i["InstanceId"] for i in desc2["LoadBalancerDescriptions"][0]["Instances"]]
        assert instance_id not in instance_ids2


class TestELBClassicTags:
    def test_add_describe_remove_tags(self, elb, load_balancer):
        # Add tags
        elb.add_tags(
            LoadBalancerNames=[load_balancer],
            Tags=[
                {"Key": "env", "Value": "test"},
                {"Key": "team", "Value": "platform"},
            ],
        )

        # Describe tags
        resp = elb.describe_tags(LoadBalancerNames=[load_balancer])
        tag_desc = resp["TagDescriptions"]
        assert len(tag_desc) == 1
        assert tag_desc[0]["LoadBalancerName"] == load_balancer
        tags = {t["Key"]: t["Value"] for t in tag_desc[0]["Tags"]}
        assert tags["env"] == "test"
        assert tags["team"] == "platform"

        # Remove one tag
        elb.remove_tags(
            LoadBalancerNames=[load_balancer],
            Tags=[{"Key": "env"}],
        )
        resp2 = elb.describe_tags(LoadBalancerNames=[load_balancer])
        tag_keys = [t["Key"] for t in resp2["TagDescriptions"][0]["Tags"]]
        assert "env" not in tag_keys
        assert "team" in tag_keys


class TestELBClassicInstanceHealth:
    def test_describe_instance_health_empty(self, elb, load_balancer):
        resp = elb.describe_instance_health(LoadBalancerName=load_balancer)
        assert "InstanceStates" in resp
        assert resp["InstanceStates"] == []

    def test_describe_instance_health_nonexistent_lb(self, elb):
        from botocore.exceptions import ClientError

        with pytest.raises(ClientError) as exc:
            elb.describe_instance_health(LoadBalancerName="does-not-exist")
        assert exc.value.response["Error"]["Code"] == "LoadBalancerNotFound"


class TestELBClassicListenerSSLCertificate:
    def test_set_load_balancer_listener_ssl_certificate(self, elb):
        name = _unique("clb")
        elb.create_load_balancer(
            LoadBalancerName=name,
            Listeners=[
                {
                    "Protocol": "HTTPS",
                    "LoadBalancerPort": 443,
                    "InstanceProtocol": "HTTP",
                    "InstancePort": 80,
                    "SSLCertificateId": "arn:aws:iam::123456789012:server-certificate/old-cert",
                }
            ],
            AvailabilityZones=["us-east-1a"],
        )
        try:
            new_cert = "arn:aws:iam::123456789012:server-certificate/new-cert"
            elb.set_load_balancer_listener_ssl_certificate(
                LoadBalancerName=name,
                LoadBalancerPort=443,
                SSLCertificateId=new_cert,
            )
            desc = elb.describe_load_balancers(LoadBalancerNames=[name])
            listener = desc["LoadBalancerDescriptions"][0]["ListenerDescriptions"][0]["Listener"]
            assert listener["SSLCertificateId"] == new_cert
        finally:
            elb.delete_load_balancer(LoadBalancerName=name)


class TestELBClassicListeners:
    def test_create_load_balancer_listeners(self, elb, load_balancer):
        elb.create_load_balancer_listeners(
            LoadBalancerName=load_balancer,
            Listeners=[
                {
                    "Protocol": "HTTP",
                    "LoadBalancerPort": 8080,
                    "InstanceProtocol": "HTTP",
                    "InstancePort": 8080,
                }
            ],
        )
        desc = elb.describe_load_balancers(LoadBalancerNames=[load_balancer])
        ports = [
            ld["Listener"]["LoadBalancerPort"]
            for ld in desc["LoadBalancerDescriptions"][0]["ListenerDescriptions"]
        ]
        assert 80 in ports
        assert 8080 in ports


class TestELBClassicPolicies:
    def test_create_lb_cookie_stickiness_policy(self, elb, load_balancer):
        policy_name = _unique("sticky")
        elb.create_lb_cookie_stickiness_policy(
            LoadBalancerName=load_balancer,
            PolicyName=policy_name,
            CookieExpirationPeriod=60,
        )

        # Verify policy exists via describe
        desc = elb.describe_load_balancers(LoadBalancerNames=[load_balancer])
        policies = desc["LoadBalancerDescriptions"][0]["Policies"]["LBCookieStickinessPolicies"]
        policy_names = [p["PolicyName"] for p in policies]
        assert policy_name in policy_names

    def test_describe_load_balancer_policies(self, elb, load_balancer):
        policy_name = _unique("sticky")
        elb.create_lb_cookie_stickiness_policy(
            LoadBalancerName=load_balancer,
            PolicyName=policy_name,
            CookieExpirationPeriod=60,
        )
        resp = elb.describe_load_balancer_policies(LoadBalancerName=load_balancer)
        assert "PolicyDescriptions" in resp
        names = [p["PolicyName"] for p in resp["PolicyDescriptions"]]
        assert policy_name in names

    def test_describe_load_balancer_policies_by_name(self, elb, load_balancer):
        policy_name = _unique("sticky")
        elb.create_lb_cookie_stickiness_policy(
            LoadBalancerName=load_balancer,
            PolicyName=policy_name,
            CookieExpirationPeriod=60,
        )
        resp = elb.describe_load_balancer_policies(
            LoadBalancerName=load_balancer,
            PolicyNames=[policy_name],
        )
        assert len(resp["PolicyDescriptions"]) == 1
        assert resp["PolicyDescriptions"][0]["PolicyName"] == policy_name

    def test_set_load_balancer_policies_of_listener(self, elb, load_balancer):
        policy_name = _unique("sticky")
        elb.create_lb_cookie_stickiness_policy(
            LoadBalancerName=load_balancer,
            PolicyName=policy_name,
            CookieExpirationPeriod=60,
        )

        # Attach policy to listener on port 80
        elb.set_load_balancer_policies_of_listener(
            LoadBalancerName=load_balancer,
            LoadBalancerPort=80,
            PolicyNames=[policy_name],
        )

        # Verify via describe
        desc = elb.describe_load_balancers(LoadBalancerNames=[load_balancer])
        listener_desc = desc["LoadBalancerDescriptions"][0]["ListenerDescriptions"][0]
        assert policy_name in listener_desc["PolicyNames"]

    def test_create_app_cookie_stickiness_policy(self, elb, load_balancer):
        policy_name = _unique("appcookie")
        elb.create_app_cookie_stickiness_policy(
            LoadBalancerName=load_balancer,
            PolicyName=policy_name,
            CookieName="JSESSIONID",
        )
        desc = elb.describe_load_balancers(LoadBalancerNames=[load_balancer])
        policies = desc["LoadBalancerDescriptions"][0]["Policies"]["AppCookieStickinessPolicies"]
        policy_names = [p["PolicyName"] for p in policies]
        assert policy_name in policy_names

    def test_create_load_balancer_policy(self, elb, load_balancer):
        policy_name = _unique("custpol")
        elb.create_load_balancer_policy(
            LoadBalancerName=load_balancer,
            PolicyName=policy_name,
            PolicyTypeName="SSLNegotiationPolicyType",
            PolicyAttributes=[
                {
                    "AttributeName": "Protocol-TLSv1.2",
                    "AttributeValue": "true",
                },
            ],
        )
        resp = elb.describe_load_balancer_policies(
            LoadBalancerName=load_balancer,
            PolicyNames=[policy_name],
        )
        assert len(resp["PolicyDescriptions"]) == 1
        assert resp["PolicyDescriptions"][0]["PolicyName"] == policy_name

    def test_delete_load_balancer_policy(self, elb, load_balancer):
        policy_name = _unique("delpol")
        elb.create_lb_cookie_stickiness_policy(
            LoadBalancerName=load_balancer,
            PolicyName=policy_name,
            CookieExpirationPeriod=60,
        )
        elb.delete_load_balancer_policy(
            LoadBalancerName=load_balancer,
            PolicyName=policy_name,
        )
        resp = elb.describe_load_balancer_policies(LoadBalancerName=load_balancer)
        names = [p["PolicyName"] for p in resp["PolicyDescriptions"]]
        assert policy_name not in names

    def test_set_load_balancer_policies_for_backend_server(self, elb, load_balancer):
        policy_name = _unique("backend")
        elb.create_load_balancer_policy(
            LoadBalancerName=load_balancer,
            PolicyName=policy_name,
            PolicyTypeName="ProxyProtocolPolicyType",
            PolicyAttributes=[
                {
                    "AttributeName": "ProxyProtocol",
                    "AttributeValue": "true",
                },
            ],
        )
        resp = elb.set_load_balancer_policies_for_backend_server(
            LoadBalancerName=load_balancer,
            InstancePort=80,
            PolicyNames=[policy_name],
        )
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200


class TestELBClassicListenerManagement:
    def test_delete_load_balancer_listeners(self, elb, load_balancer):
        # Add a second listener first
        elb.create_load_balancer_listeners(
            LoadBalancerName=load_balancer,
            Listeners=[
                {
                    "Protocol": "HTTP",
                    "LoadBalancerPort": 8080,
                    "InstanceProtocol": "HTTP",
                    "InstancePort": 8080,
                }
            ],
        )
        # Verify it was added
        desc = elb.describe_load_balancers(LoadBalancerNames=[load_balancer])
        ports = [
            ld["Listener"]["LoadBalancerPort"]
            for ld in desc["LoadBalancerDescriptions"][0]["ListenerDescriptions"]
        ]
        assert 8080 in ports

        # Delete the second listener
        elb.delete_load_balancer_listeners(
            LoadBalancerName=load_balancer,
            LoadBalancerPorts=[8080],
        )
        desc2 = elb.describe_load_balancers(LoadBalancerNames=[load_balancer])
        ports2 = [
            ld["Listener"]["LoadBalancerPort"]
            for ld in desc2["LoadBalancerDescriptions"][0]["ListenerDescriptions"]
        ]
        assert 8080 not in ports2
        assert 80 in ports2


class TestELBClassicAvailabilityZones:
    def test_enable_availability_zones_for_load_balancer(self, elb, load_balancer):
        resp = elb.enable_availability_zones_for_load_balancer(
            LoadBalancerName=load_balancer,
            AvailabilityZones=["us-east-1b"],
        )
        assert "us-east-1b" in resp["AvailabilityZones"]
        assert "us-east-1a" in resp["AvailabilityZones"]

    def test_disable_availability_zones_for_load_balancer(self, elb, load_balancer):
        # First enable a second AZ
        elb.enable_availability_zones_for_load_balancer(
            LoadBalancerName=load_balancer,
            AvailabilityZones=["us-east-1b"],
        )
        # Now disable it
        resp = elb.disable_availability_zones_for_load_balancer(
            LoadBalancerName=load_balancer,
            AvailabilityZones=["us-east-1b"],
        )
        assert "us-east-1b" not in resp["AvailabilityZones"]
        assert "us-east-1a" in resp["AvailabilityZones"]


class TestELBClassicSubnetsAndSecurityGroups:
    @pytest.fixture
    def vpc_resources(self):
        """Create a VPC with subnets and security group for ELB tests."""
        ec2 = make_client("ec2")
        vpc = ec2.create_vpc(CidrBlock="10.0.0.0/16")
        vpc_id = vpc["Vpc"]["VpcId"]
        subnet1 = ec2.create_subnet(
            VpcId=vpc_id, CidrBlock="10.0.1.0/24", AvailabilityZone="us-east-1a"
        )
        subnet2 = ec2.create_subnet(
            VpcId=vpc_id, CidrBlock="10.0.2.0/24", AvailabilityZone="us-east-1b"
        )
        sg = ec2.create_security_group(
            GroupName=_unique("elbsg"),
            Description="ELB test SG",
            VpcId=vpc_id,
        )
        yield {
            "vpc_id": vpc_id,
            "subnet1_id": subnet1["Subnet"]["SubnetId"],
            "subnet2_id": subnet2["Subnet"]["SubnetId"],
            "sg_id": sg["GroupId"],
        }
        try:
            ec2.delete_security_group(GroupId=sg["GroupId"])
            ec2.delete_subnet(SubnetId=subnet2["Subnet"]["SubnetId"])
            ec2.delete_subnet(SubnetId=subnet1["Subnet"]["SubnetId"])
            ec2.delete_vpc(VpcId=vpc_id)
        except Exception:
            pass  # best-effort cleanup

    def test_apply_security_groups_to_load_balancer(self, elb, vpc_resources):
        name = _unique("clb-vpc")
        elb.create_load_balancer(
            LoadBalancerName=name,
            Listeners=[
                {
                    "Protocol": "HTTP",
                    "LoadBalancerPort": 80,
                    "InstanceProtocol": "HTTP",
                    "InstancePort": 80,
                }
            ],
            Subnets=[vpc_resources["subnet1_id"]],
            SecurityGroups=[vpc_resources["sg_id"]],
        )
        try:
            resp = elb.apply_security_groups_to_load_balancer(
                LoadBalancerName=name,
                SecurityGroups=[vpc_resources["sg_id"]],
            )
            assert vpc_resources["sg_id"] in resp["SecurityGroups"]
        finally:
            elb.delete_load_balancer(LoadBalancerName=name)

    def test_attach_load_balancer_to_subnets(self, elb, vpc_resources):
        name = _unique("clb-sub")
        elb.create_load_balancer(
            LoadBalancerName=name,
            Listeners=[
                {
                    "Protocol": "HTTP",
                    "LoadBalancerPort": 80,
                    "InstanceProtocol": "HTTP",
                    "InstancePort": 80,
                }
            ],
            Subnets=[vpc_resources["subnet1_id"]],
            SecurityGroups=[vpc_resources["sg_id"]],
        )
        try:
            resp = elb.attach_load_balancer_to_subnets(
                LoadBalancerName=name,
                Subnets=[vpc_resources["subnet2_id"]],
            )
            assert vpc_resources["subnet2_id"] in resp["Subnets"]
        finally:
            elb.delete_load_balancer(LoadBalancerName=name)

    def test_detach_load_balancer_from_subnets(self, elb, vpc_resources):
        name = _unique("clb-det")
        elb.create_load_balancer(
            LoadBalancerName=name,
            Listeners=[
                {
                    "Protocol": "HTTP",
                    "LoadBalancerPort": 80,
                    "InstanceProtocol": "HTTP",
                    "InstancePort": 80,
                }
            ],
            Subnets=[vpc_resources["subnet1_id"]],
            SecurityGroups=[vpc_resources["sg_id"]],
        )
        try:
            # Attach second subnet first
            elb.attach_load_balancer_to_subnets(
                LoadBalancerName=name,
                Subnets=[vpc_resources["subnet2_id"]],
            )
            # Detach it
            resp = elb.detach_load_balancer_from_subnets(
                LoadBalancerName=name,
                Subnets=[vpc_resources["subnet2_id"]],
            )
            assert vpc_resources["subnet2_id"] not in resp["Subnets"]
        finally:
            elb.delete_load_balancer(LoadBalancerName=name)
