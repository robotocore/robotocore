"""Compatibility tests for VPC Lattice service."""

import uuid

import pytest
from botocore.exceptions import ClientError

from tests.compatibility.conftest import make_client

POLICY_DOC = (
    '{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":"*",'
    '"Action":"vpc-lattice-svcs:Invoke","Resource":"*"}]}'
)
RESOURCE_POLICY_DOC = (
    '{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":"*",'
    '"Action":"vpc-lattice:CreateServiceNetworkVpcAssociation","Resource":"*"}]}'
)


@pytest.fixture
def vpclattice_client():
    return make_client("vpc-lattice")


@pytest.fixture
def service_name():
    return f"test-svc-{uuid.uuid4().hex[:8]}"


@pytest.fixture
def service_network_name():
    return f"test-net-{uuid.uuid4().hex[:8]}"


@pytest.fixture
def service_network(vpclattice_client):
    """Create a service network and clean up after."""
    name = f"test-sn-{uuid.uuid4().hex[:8]}"
    resp = vpclattice_client.create_service_network(name=name)
    sn_id = resp["id"]
    sn_arn = resp["arn"]
    yield sn_id, sn_arn
    try:
        vpclattice_client.delete_service_network(serviceNetworkIdentifier=sn_id)
    except ClientError:
        pass


class TestVpcLatticeService:
    """Tests for VPC Lattice service operations."""

    def test_create_service(self, vpclattice_client, service_name):
        resp = vpclattice_client.create_service(name=service_name)
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200
        assert resp["name"] == service_name
        assert "id" in resp
        assert "arn" in resp
        assert resp["status"] == "ACTIVE"
        assert "dnsEntry" in resp
        assert "domainName" in resp["dnsEntry"]

    def test_create_service_returns_arn_with_correct_format(self, vpclattice_client, service_name):
        resp = vpclattice_client.create_service(name=service_name)
        arn = resp["arn"]
        assert arn.startswith("arn:aws:vpc-lattice:")
        assert ":service/" in arn

    def test_list_services(self, vpclattice_client, service_name):
        vpclattice_client.create_service(name=service_name)
        resp = vpclattice_client.list_services()
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200
        assert "items" in resp
        names = [s["name"] for s in resp["items"]]
        assert service_name in names

    def test_get_service(self, vpclattice_client, service_name):
        create_resp = vpclattice_client.create_service(name=service_name)
        svc_id = create_resp["id"]

        get_resp = vpclattice_client.get_service(serviceIdentifier=svc_id)
        assert get_resp["ResponseMetadata"]["HTTPStatusCode"] == 200
        assert get_resp["id"] == svc_id
        assert get_resp["name"] == service_name
        assert get_resp["status"] == "ACTIVE"
        assert get_resp["arn"] == create_resp["arn"]

    def test_get_service_has_dns_entry(self, vpclattice_client, service_name):
        create_resp = vpclattice_client.create_service(name=service_name)
        svc_id = create_resp["id"]

        get_resp = vpclattice_client.get_service(serviceIdentifier=svc_id)
        assert "dnsEntry" in get_resp
        assert "domainName" in get_resp["dnsEntry"]


class TestVpcLatticeServiceNetwork:
    """Tests for VPC Lattice service network operations."""

    def test_create_service_network(self, vpclattice_client, service_network_name):
        resp = vpclattice_client.create_service_network(name=service_network_name)
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200
        assert resp["name"] == service_network_name
        assert "id" in resp
        assert "arn" in resp

    def test_create_service_network_arn_format(self, vpclattice_client, service_network_name):
        resp = vpclattice_client.create_service_network(name=service_network_name)
        arn = resp["arn"]
        assert arn.startswith("arn:aws:vpc-lattice:")
        assert ":servicenetwork/" in arn

    def test_list_service_networks(self, vpclattice_client, service_network_name):
        vpclattice_client.create_service_network(name=service_network_name)
        resp = vpclattice_client.list_service_networks()
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200
        assert "items" in resp
        names = [n["name"] for n in resp["items"]]
        assert service_network_name in names


class TestVpcLatticeGetServiceNetwork:
    """Tests for GetServiceNetwork operation."""

    def test_get_service_network(self, vpclattice_client, service_network):
        """GetServiceNetwork returns matching id and arn."""
        sn_id, sn_arn = service_network
        response = vpclattice_client.get_service_network(serviceNetworkIdentifier=sn_id)
        assert response["id"] == sn_id
        assert response["arn"] == sn_arn
        assert "name" in response

    def test_get_service_network_not_found(self, vpclattice_client):
        """GetServiceNetwork raises ResourceNotFoundException for unknown ID."""
        with pytest.raises(ClientError) as exc_info:
            vpclattice_client.get_service_network(serviceNetworkIdentifier="sn-00000000-0000-000")
        assert exc_info.value.response["Error"]["Code"] == "ResourceNotFoundException"


class TestVpcLatticeServiceNetworkVpcAssociation:
    """Tests for CreateServiceNetworkVpcAssociation."""

    def test_create_service_network_vpc_association(self, vpclattice_client, service_network):
        """CreateServiceNetworkVpcAssociation returns id, status, arn."""
        sn_id, _ = service_network
        response = vpclattice_client.create_service_network_vpc_association(
            serviceNetworkIdentifier=sn_id,
            vpcIdentifier="vpc-12345678",
        )
        assert "id" in response
        assert response["status"] in ("ACTIVE", "CREATE_IN_PROGRESS", "CREATING")
        assert "vpc-lattice" in response["arn"]


class TestVpcLatticeTags:
    """Tests for tag operations."""

    def test_list_tags_for_resource(self, vpclattice_client, service_network):
        """ListTagsForResource returns tags dict (empty by default)."""
        _, sn_arn = service_network
        response = vpclattice_client.list_tags_for_resource(resourceArn=sn_arn)
        assert isinstance(response["tags"], dict)

    def test_tag_resource(self, vpclattice_client, service_network):
        """TagResource adds tags to the resource."""
        _, sn_arn = service_network
        vpclattice_client.tag_resource(
            resourceArn=sn_arn, tags={"env": "test", "project": "robotocore"}
        )
        tags_resp = vpclattice_client.list_tags_for_resource(resourceArn=sn_arn)
        assert tags_resp["tags"]["env"] == "test"
        assert tags_resp["tags"]["project"] == "robotocore"

    def test_untag_resource(self, vpclattice_client, service_network):
        """UntagResource removes the specified tag keys."""
        _, sn_arn = service_network
        vpclattice_client.tag_resource(resourceArn=sn_arn, tags={"k1": "v1", "k2": "v2"})
        vpclattice_client.untag_resource(resourceArn=sn_arn, tagKeys=["k1"])
        tags_resp = vpclattice_client.list_tags_for_resource(resourceArn=sn_arn)
        assert "k1" not in tags_resp["tags"]
        assert tags_resp["tags"].get("k2") == "v2"


class TestVpcLatticeResourcePolicy:
    """Tests for resource policy operations."""

    def test_put_resource_policy(self, vpclattice_client, service_network):
        """PutResourcePolicy succeeds without error."""
        _, sn_arn = service_network
        response = vpclattice_client.put_resource_policy(
            resourceArn=sn_arn, policy=RESOURCE_POLICY_DOC
        )
        assert response["ResponseMetadata"]["HTTPStatusCode"] == 200

    def test_get_resource_policy(self, vpclattice_client, service_network):
        """GetResourcePolicy returns the exact policy that was put."""
        _, sn_arn = service_network
        vpclattice_client.put_resource_policy(resourceArn=sn_arn, policy=RESOURCE_POLICY_DOC)
        response = vpclattice_client.get_resource_policy(resourceArn=sn_arn)
        assert "vpc-lattice:CreateServiceNetworkVpcAssociation" in response["policy"]

    def test_get_resource_policy_not_found(self, vpclattice_client):
        """GetResourcePolicy raises ResourceNotFoundException for unknown ARN."""
        fake_arn = "arn:aws:vpc-lattice:us-east-1:123456789012:servicenetwork/sn-00000000"
        with pytest.raises(ClientError) as exc_info:
            vpclattice_client.get_resource_policy(resourceArn=fake_arn)
        assert exc_info.value.response["Error"]["Code"] == "ResourceNotFoundException"

    def test_delete_resource_policy(self, vpclattice_client, service_network):
        """DeleteResourcePolicy succeeds after policy has been put."""
        _, sn_arn = service_network
        vpclattice_client.put_resource_policy(resourceArn=sn_arn, policy=RESOURCE_POLICY_DOC)
        response = vpclattice_client.delete_resource_policy(resourceArn=sn_arn)
        assert response["ResponseMetadata"]["HTTPStatusCode"] == 200

    def test_delete_resource_policy_not_found(self, vpclattice_client):
        """DeleteResourcePolicy raises ResourceNotFoundException for unknown ARN."""
        fake_arn = "arn:aws:vpc-lattice:us-east-1:123456789012:servicenetwork/sn-00000000"
        with pytest.raises(ClientError) as exc_info:
            vpclattice_client.delete_resource_policy(resourceArn=fake_arn)
        assert exc_info.value.response["Error"]["Code"] == "ResourceNotFoundException"


class TestVpcLatticeAuthPolicy:
    """Tests for auth policy operations."""

    def test_put_auth_policy(self, vpclattice_client, service_network):
        """PutAuthPolicy returns the policy content and a state string."""
        sn_id, _ = service_network
        response = vpclattice_client.put_auth_policy(resourceIdentifier=sn_id, policy=POLICY_DOC)
        assert "vpc-lattice-svcs:Invoke" in response["policy"]
        assert response["state"] in ("Active", "Inactive", "ACTIVE", "INACTIVE")

    def test_get_auth_policy(self, vpclattice_client, service_network):
        """GetAuthPolicy returns policy content and state."""
        sn_id, _ = service_network
        vpclattice_client.put_auth_policy(resourceIdentifier=sn_id, policy=POLICY_DOC)
        response = vpclattice_client.get_auth_policy(resourceIdentifier=sn_id)
        assert "vpc-lattice-svcs:Invoke" in response["policy"]
        assert response["state"] in ("Active", "Inactive", "ACTIVE", "INACTIVE")
        assert "createdAt" in response
        assert "lastUpdatedAt" in response

    def test_delete_auth_policy(self, vpclattice_client, service_network):
        """DeleteAuthPolicy succeeds after policy has been put."""
        sn_id, _ = service_network
        vpclattice_client.put_auth_policy(resourceIdentifier=sn_id, policy=POLICY_DOC)
        response = vpclattice_client.delete_auth_policy(resourceIdentifier=sn_id)
        assert response["ResponseMetadata"]["HTTPStatusCode"] == 200


class TestVpcLatticeAccessLogSubscription:
    """Tests for access log subscription operations."""

    def test_create_access_log_subscription(self, vpclattice_client, service_network):
        """CreateAccessLogSubscription returns id, arn, and correct resourceId."""
        sn_id, _ = service_network
        response = vpclattice_client.create_access_log_subscription(
            resourceIdentifier=sn_id,
            destinationArn="arn:aws:logs:us-east-1:123456789012:log-group:test-lg",
        )
        assert response["resourceId"] == sn_id
        assert "accesslogsubscription" in response["arn"]
        assert response["id"].startswith("als-")
        # Cleanup
        vpclattice_client.delete_access_log_subscription(
            accessLogSubscriptionIdentifier=response["id"]
        )

    def test_get_access_log_subscription(self, vpclattice_client, service_network):
        """GetAccessLogSubscription returns matching subscription fields."""
        sn_id, _ = service_network
        created = vpclattice_client.create_access_log_subscription(
            resourceIdentifier=sn_id,
            destinationArn="arn:aws:logs:us-east-1:123456789012:log-group:test-lg-get",
        )
        als_id = created["id"]
        response = vpclattice_client.get_access_log_subscription(
            accessLogSubscriptionIdentifier=als_id
        )
        assert response["id"] == als_id
        assert "test-lg-get" in response["destinationArn"]
        assert "createdAt" in response
        # Cleanup
        vpclattice_client.delete_access_log_subscription(accessLogSubscriptionIdentifier=als_id)

    def test_list_access_log_subscriptions(self, vpclattice_client, service_network):
        """ListAccessLogSubscriptions returns items list for a resource."""
        sn_id, _ = service_network
        created = vpclattice_client.create_access_log_subscription(
            resourceIdentifier=sn_id,
            destinationArn="arn:aws:logs:us-east-1:123456789012:log-group:test-lg-list",
        )
        als_id = created["id"]
        response = vpclattice_client.list_access_log_subscriptions(resourceIdentifier=sn_id)
        assert "items" in response
        ids = [item["id"] for item in response["items"]]
        assert als_id in ids
        # Cleanup
        vpclattice_client.delete_access_log_subscription(accessLogSubscriptionIdentifier=als_id)

    def test_update_access_log_subscription(self, vpclattice_client, service_network):
        """UpdateAccessLogSubscription reflects new destinationArn."""
        sn_id, _ = service_network
        created = vpclattice_client.create_access_log_subscription(
            resourceIdentifier=sn_id,
            destinationArn="arn:aws:logs:us-east-1:123456789012:log-group:test-lg-update",
        )
        als_id = created["id"]
        response = vpclattice_client.update_access_log_subscription(
            accessLogSubscriptionIdentifier=als_id,
            destinationArn="arn:aws:logs:us-east-1:123456789012:log-group:test-lg-updated",
        )
        assert response["id"] == als_id
        assert "test-lg-updated" in response["destinationArn"]
        # Cleanup
        vpclattice_client.delete_access_log_subscription(accessLogSubscriptionIdentifier=als_id)

    def test_delete_access_log_subscription(self, vpclattice_client, service_network):
        """DeleteAccessLogSubscription succeeds with 200 status."""
        sn_id, _ = service_network
        created = vpclattice_client.create_access_log_subscription(
            resourceIdentifier=sn_id,
            destinationArn="arn:aws:logs:us-east-1:123456789012:log-group:test-lg-del",
        )
        als_id = created["id"]
        response = vpclattice_client.delete_access_log_subscription(
            accessLogSubscriptionIdentifier=als_id
        )
        assert response["ResponseMetadata"]["HTTPStatusCode"] == 200
