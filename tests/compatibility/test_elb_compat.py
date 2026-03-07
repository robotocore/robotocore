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
        pass


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
        instance_ids = [
            i["InstanceId"] for i in desc["LoadBalancerDescriptions"][0]["Instances"]
        ]
        assert instance_id in instance_ids

        # Deregister
        elb.deregister_instances_from_load_balancer(
            LoadBalancerName=load_balancer,
            Instances=[{"InstanceId": instance_id}],
        )

        desc2 = elb.describe_load_balancers(LoadBalancerNames=[load_balancer])
        instance_ids2 = [
            i["InstanceId"] for i in desc2["LoadBalancerDescriptions"][0]["Instances"]
        ]
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
