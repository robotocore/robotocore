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


@pytest.fixture
def service(vpclattice_client):
    """Create a service and clean up after."""
    name = f"test-svc-{uuid.uuid4().hex[:8]}"
    resp = vpclattice_client.create_service(name=name)
    svc_id = resp["id"]
    yield svc_id, resp["arn"], name
    try:
        vpclattice_client.delete_service(serviceIdentifier=svc_id)
    except ClientError:
        pass


class TestVpcLatticeServiceDelete:
    """Tests for DeleteService operation."""

    def test_delete_service(self, vpclattice_client):
        name = f"test-svc-del-{uuid.uuid4().hex[:8]}"
        resp = vpclattice_client.create_service(name=name)
        svc_id = resp["id"]
        del_resp = vpclattice_client.delete_service(serviceIdentifier=svc_id)
        assert del_resp["ResponseMetadata"]["HTTPStatusCode"] == 200
        assert "id" in del_resp

    def test_delete_service_not_found(self, vpclattice_client):
        with pytest.raises(ClientError) as exc_info:
            vpclattice_client.delete_service(serviceIdentifier="svc-00000000000000000")
        assert exc_info.value.response["Error"]["Code"] == "ResourceNotFoundException"


class TestVpcLatticeServiceUpdate:
    """Tests for UpdateService operation."""

    def test_update_service_auth_type(self, vpclattice_client, service):
        svc_id, _, _ = service
        resp = vpclattice_client.update_service(serviceIdentifier=svc_id, authType="AWS_IAM")
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200
        assert resp["authType"] == "AWS_IAM"


class TestVpcLatticeServiceNetworkDelete:
    """Tests for DeleteServiceNetwork operation."""

    def test_delete_service_network(self, vpclattice_client):
        name = f"test-sn-del-{uuid.uuid4().hex[:8]}"
        resp = vpclattice_client.create_service_network(name=name)
        sn_id = resp["id"]
        del_resp = vpclattice_client.delete_service_network(serviceNetworkIdentifier=sn_id)
        assert del_resp["ResponseMetadata"]["HTTPStatusCode"] == 200


class TestVpcLatticeServiceNetworkUpdate:
    """Tests for UpdateServiceNetwork operation."""

    def test_update_service_network_auth_type(self, vpclattice_client, service_network):
        sn_id, _ = service_network
        resp = vpclattice_client.update_service_network(
            serviceNetworkIdentifier=sn_id, authType="AWS_IAM"
        )
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200
        assert resp["authType"] == "AWS_IAM"


class TestVpcLatticeListener:
    """Tests for Listener CRUD operations."""

    def test_create_listener(self, vpclattice_client, service):
        svc_id, _, _ = service
        resp = vpclattice_client.create_listener(
            name="test-listener",
            serviceIdentifier=svc_id,
            protocol="HTTP",
            port=80,
            defaultAction={"fixedResponse": {"statusCode": 200}},
        )
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200
        assert "id" in resp
        assert resp["protocol"] == "HTTP"
        assert resp["port"] == 80
        # cleanup
        vpclattice_client.delete_listener(serviceIdentifier=svc_id, listenerIdentifier=resp["id"])

    def test_get_listener(self, vpclattice_client, service):
        svc_id, _, _ = service
        created = vpclattice_client.create_listener(
            name="test-listener-get",
            serviceIdentifier=svc_id,
            protocol="HTTP",
            port=8080,
            defaultAction={"fixedResponse": {"statusCode": 404}},
        )
        lst_id = created["id"]
        resp = vpclattice_client.get_listener(serviceIdentifier=svc_id, listenerIdentifier=lst_id)
        assert resp["id"] == lst_id
        assert resp["protocol"] == "HTTP"
        vpclattice_client.delete_listener(serviceIdentifier=svc_id, listenerIdentifier=lst_id)

    def test_list_listeners(self, vpclattice_client, service):
        svc_id, _, _ = service
        created = vpclattice_client.create_listener(
            name="test-listener-list",
            serviceIdentifier=svc_id,
            protocol="HTTP",
            port=80,
            defaultAction={"fixedResponse": {"statusCode": 200}},
        )
        lst_id = created["id"]
        resp = vpclattice_client.list_listeners(serviceIdentifier=svc_id)
        assert "items" in resp
        ids = [lst["id"] for lst in resp["items"]]
        assert lst_id in ids
        vpclattice_client.delete_listener(serviceIdentifier=svc_id, listenerIdentifier=lst_id)

    def test_update_listener(self, vpclattice_client, service):
        svc_id, _, _ = service
        created = vpclattice_client.create_listener(
            name="test-listener-update",
            serviceIdentifier=svc_id,
            protocol="HTTP",
            port=80,
            defaultAction={"fixedResponse": {"statusCode": 200}},
        )
        lst_id = created["id"]
        resp = vpclattice_client.update_listener(
            serviceIdentifier=svc_id,
            listenerIdentifier=lst_id,
            defaultAction={"fixedResponse": {"statusCode": 503}},
        )
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200
        vpclattice_client.delete_listener(serviceIdentifier=svc_id, listenerIdentifier=lst_id)

    def test_delete_listener(self, vpclattice_client, service):
        svc_id, _, _ = service
        created = vpclattice_client.create_listener(
            name="test-listener-del",
            serviceIdentifier=svc_id,
            protocol="HTTP",
            port=80,
            defaultAction={"fixedResponse": {"statusCode": 200}},
        )
        lst_id = created["id"]
        resp = vpclattice_client.delete_listener(
            serviceIdentifier=svc_id, listenerIdentifier=lst_id
        )
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200


class TestVpcLatticeRule:
    """Tests for Rule CRUD operations."""

    @pytest.fixture
    def listener(self, vpclattice_client, service):
        svc_id, _, _ = service
        resp = vpclattice_client.create_listener(
            name=f"test-listener-rules-{uuid.uuid4().hex[:8]}",
            serviceIdentifier=svc_id,
            protocol="HTTP",
            port=80,
            defaultAction={"fixedResponse": {"statusCode": 200}},
        )
        lst_id = resp["id"]
        yield svc_id, lst_id
        try:
            vpclattice_client.delete_listener(serviceIdentifier=svc_id, listenerIdentifier=lst_id)
        except ClientError:
            pass

    def test_create_rule(self, vpclattice_client, listener):
        svc_id, lst_id = listener
        resp = vpclattice_client.create_rule(
            listenerIdentifier=lst_id,
            serviceIdentifier=svc_id,
            name="test-rule",
            priority=10,
            action={"fixedResponse": {"statusCode": 200}},
            match={"httpMatch": {"pathMatch": {"match": {"exact": "/test"}}}},
        )
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200
        assert "id" in resp
        assert resp["priority"] == 10
        vpclattice_client.delete_rule(
            serviceIdentifier=svc_id,
            listenerIdentifier=lst_id,
            ruleIdentifier=resp["id"],
        )

    def test_get_rule(self, vpclattice_client, listener):
        svc_id, lst_id = listener
        created = vpclattice_client.create_rule(
            listenerIdentifier=lst_id,
            serviceIdentifier=svc_id,
            name="test-rule-get",
            priority=20,
            action={"fixedResponse": {"statusCode": 200}},
            match={"httpMatch": {"pathMatch": {"match": {"exact": "/get"}}}},
        )
        rule_id = created["id"]
        resp = vpclattice_client.get_rule(
            serviceIdentifier=svc_id,
            listenerIdentifier=lst_id,
            ruleIdentifier=rule_id,
        )
        assert resp["id"] == rule_id
        assert resp["priority"] == 20
        vpclattice_client.delete_rule(
            serviceIdentifier=svc_id,
            listenerIdentifier=lst_id,
            ruleIdentifier=rule_id,
        )

    def test_list_rules(self, vpclattice_client, listener):
        svc_id, lst_id = listener
        created = vpclattice_client.create_rule(
            listenerIdentifier=lst_id,
            serviceIdentifier=svc_id,
            name="test-rule-list",
            priority=30,
            action={"fixedResponse": {"statusCode": 200}},
            match={"httpMatch": {"pathMatch": {"match": {"exact": "/list"}}}},
        )
        rule_id = created["id"]
        resp = vpclattice_client.list_rules(serviceIdentifier=svc_id, listenerIdentifier=lst_id)
        assert "items" in resp
        ids = [r["id"] for r in resp["items"]]
        assert rule_id in ids
        vpclattice_client.delete_rule(
            serviceIdentifier=svc_id,
            listenerIdentifier=lst_id,
            ruleIdentifier=rule_id,
        )

    def test_batch_update_rule(self, vpclattice_client, listener):
        svc_id, lst_id = listener
        created = vpclattice_client.create_rule(
            listenerIdentifier=lst_id,
            serviceIdentifier=svc_id,
            name="test-rule-batch",
            priority=40,
            action={"fixedResponse": {"statusCode": 200}},
            match={"httpMatch": {"pathMatch": {"match": {"exact": "/batch"}}}},
        )
        rule_id = created["id"]
        resp = vpclattice_client.batch_update_rule(
            serviceIdentifier=svc_id,
            listenerIdentifier=lst_id,
            rules=[{"ruleIdentifier": rule_id, "priority": 50}],
        )
        assert "successful" in resp
        assert len(resp["successful"]) == 1
        vpclattice_client.delete_rule(
            serviceIdentifier=svc_id,
            listenerIdentifier=lst_id,
            ruleIdentifier=rule_id,
        )


class TestVpcLatticeTargetGroup:
    """Tests for Target Group CRUD operations."""

    def test_create_target_group(self, vpclattice_client):
        name = f"test-tg-{uuid.uuid4().hex[:8]}"
        resp = vpclattice_client.create_target_group(name=name, type="INSTANCE")
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200
        assert "id" in resp
        assert resp["type"] == "INSTANCE"
        assert resp["status"] == "ACTIVE"
        vpclattice_client.delete_target_group(targetGroupIdentifier=resp["id"])

    def test_get_target_group(self, vpclattice_client):
        name = f"test-tg-get-{uuid.uuid4().hex[:8]}"
        created = vpclattice_client.create_target_group(name=name, type="IP")
        tg_id = created["id"]
        resp = vpclattice_client.get_target_group(targetGroupIdentifier=tg_id)
        assert resp["id"] == tg_id
        assert resp["type"] == "IP"
        vpclattice_client.delete_target_group(targetGroupIdentifier=tg_id)

    def test_list_target_groups(self, vpclattice_client):
        name = f"test-tg-list-{uuid.uuid4().hex[:8]}"
        created = vpclattice_client.create_target_group(name=name, type="INSTANCE")
        tg_id = created["id"]
        resp = vpclattice_client.list_target_groups()
        assert "items" in resp
        ids = [tg["id"] for tg in resp["items"]]
        assert tg_id in ids
        vpclattice_client.delete_target_group(targetGroupIdentifier=tg_id)

    def test_register_and_list_targets(self, vpclattice_client):
        name = f"test-tg-targets-{uuid.uuid4().hex[:8]}"
        created = vpclattice_client.create_target_group(name=name, type="INSTANCE")
        tg_id = created["id"]
        reg_resp = vpclattice_client.register_targets(
            targetGroupIdentifier=tg_id,
            targets=[{"id": "i-12345678", "port": 80}],
        )
        assert "successful" in reg_resp
        assert len(reg_resp["successful"]) == 1
        list_resp = vpclattice_client.list_targets(targetGroupIdentifier=tg_id)
        assert "items" in list_resp
        assert len(list_resp["items"]) >= 1
        vpclattice_client.deregister_targets(
            targetGroupIdentifier=tg_id,
            targets=[{"id": "i-12345678", "port": 80}],
        )
        vpclattice_client.delete_target_group(targetGroupIdentifier=tg_id)

    def test_deregister_targets(self, vpclattice_client):
        name = f"test-tg-dereg-{uuid.uuid4().hex[:8]}"
        created = vpclattice_client.create_target_group(name=name, type="INSTANCE")
        tg_id = created["id"]
        vpclattice_client.register_targets(
            targetGroupIdentifier=tg_id,
            targets=[{"id": "i-87654321", "port": 8080}],
        )
        dereg_resp = vpclattice_client.deregister_targets(
            targetGroupIdentifier=tg_id,
            targets=[{"id": "i-87654321", "port": 8080}],
        )
        assert "successful" in dereg_resp
        assert len(dereg_resp["successful"]) == 1
        vpclattice_client.delete_target_group(targetGroupIdentifier=tg_id)

    def test_delete_target_group(self, vpclattice_client):
        name = f"test-tg-del-{uuid.uuid4().hex[:8]}"
        created = vpclattice_client.create_target_group(name=name, type="INSTANCE")
        tg_id = created["id"]
        resp = vpclattice_client.delete_target_group(targetGroupIdentifier=tg_id)
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200
        assert resp["id"] == tg_id


class TestVpcLatticeServiceNetworkServiceAssociation:
    """Tests for ServiceNetworkServiceAssociation CRUD operations."""

    def test_create_service_network_service_association(
        self, vpclattice_client, service_network, service
    ):
        sn_id, _ = service_network
        svc_id, _, _ = service
        resp = vpclattice_client.create_service_network_service_association(
            serviceNetworkIdentifier=sn_id,
            serviceIdentifier=svc_id,
        )
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200
        assert "id" in resp
        assert resp["status"] in ("ACTIVE", "CREATE_IN_PROGRESS")
        vpclattice_client.delete_service_network_service_association(
            serviceNetworkServiceAssociationIdentifier=resp["id"]
        )

    def test_get_service_network_service_association(
        self, vpclattice_client, service_network, service
    ):
        sn_id, _ = service_network
        svc_id, _, _ = service
        created = vpclattice_client.create_service_network_service_association(
            serviceNetworkIdentifier=sn_id,
            serviceIdentifier=svc_id,
        )
        assoc_id = created["id"]
        resp = vpclattice_client.get_service_network_service_association(
            serviceNetworkServiceAssociationIdentifier=assoc_id
        )
        assert resp["id"] == assoc_id
        assert resp["serviceId"] == svc_id
        vpclattice_client.delete_service_network_service_association(
            serviceNetworkServiceAssociationIdentifier=assoc_id
        )

    def test_list_service_network_service_associations(
        self, vpclattice_client, service_network, service
    ):
        sn_id, _ = service_network
        svc_id, _, _ = service
        created = vpclattice_client.create_service_network_service_association(
            serviceNetworkIdentifier=sn_id,
            serviceIdentifier=svc_id,
        )
        assoc_id = created["id"]
        resp = vpclattice_client.list_service_network_service_associations(
            serviceNetworkIdentifier=sn_id
        )
        assert "items" in resp
        ids = [a["id"] for a in resp["items"]]
        assert assoc_id in ids
        vpclattice_client.delete_service_network_service_association(
            serviceNetworkServiceAssociationIdentifier=assoc_id
        )

    def test_delete_service_network_service_association(
        self, vpclattice_client, service_network, service
    ):
        sn_id, _ = service_network
        svc_id, _, _ = service
        created = vpclattice_client.create_service_network_service_association(
            serviceNetworkIdentifier=sn_id,
            serviceIdentifier=svc_id,
        )
        assoc_id = created["id"]
        resp = vpclattice_client.delete_service_network_service_association(
            serviceNetworkServiceAssociationIdentifier=assoc_id
        )
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200


class TestVpcLatticeServiceNetworkVpcAssociationFull:
    """Tests for full ServiceNetworkVpcAssociation CRUD."""

    def test_get_service_network_vpc_association(self, vpclattice_client, service_network):
        sn_id, _ = service_network
        created = vpclattice_client.create_service_network_vpc_association(
            serviceNetworkIdentifier=sn_id,
            vpcIdentifier="vpc-11111111",
        )
        assoc_id = created["id"]
        resp = vpclattice_client.get_service_network_vpc_association(
            serviceNetworkVpcAssociationIdentifier=assoc_id
        )
        assert resp["id"] == assoc_id
        assert resp["status"] == "ACTIVE"
        vpclattice_client.delete_service_network_vpc_association(
            serviceNetworkVpcAssociationIdentifier=assoc_id
        )

    def test_list_service_network_vpc_associations(self, vpclattice_client, service_network):
        sn_id, _ = service_network
        created = vpclattice_client.create_service_network_vpc_association(
            serviceNetworkIdentifier=sn_id,
            vpcIdentifier="vpc-22222222",
        )
        assoc_id = created["id"]
        resp = vpclattice_client.list_service_network_vpc_associations(
            serviceNetworkIdentifier=sn_id
        )
        assert "items" in resp
        ids = [a["id"] for a in resp["items"]]
        assert assoc_id in ids
        vpclattice_client.delete_service_network_vpc_association(
            serviceNetworkVpcAssociationIdentifier=assoc_id
        )

    def test_update_service_network_vpc_association(self, vpclattice_client, service_network):
        sn_id, _ = service_network
        created = vpclattice_client.create_service_network_vpc_association(
            serviceNetworkIdentifier=sn_id,
            vpcIdentifier="vpc-33333333",
        )
        assoc_id = created["id"]
        resp = vpclattice_client.update_service_network_vpc_association(
            serviceNetworkVpcAssociationIdentifier=assoc_id,
            securityGroupIds=["sg-12345678"],
        )
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200
        vpclattice_client.delete_service_network_vpc_association(
            serviceNetworkVpcAssociationIdentifier=assoc_id
        )

    def test_delete_service_network_vpc_association(self, vpclattice_client, service_network):
        sn_id, _ = service_network
        created = vpclattice_client.create_service_network_vpc_association(
            serviceNetworkIdentifier=sn_id,
            vpcIdentifier="vpc-44444444",
        )
        assoc_id = created["id"]
        resp = vpclattice_client.delete_service_network_vpc_association(
            serviceNetworkVpcAssociationIdentifier=assoc_id
        )
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200


class TestVpcLatticeUpdateRule:
    """Tests for UpdateRule operation."""

    def test_update_rule(self, vpclattice_client, service):
        """UpdateRule changes the priority of a rule."""
        svc_id, _, _ = service
        listener_resp = vpclattice_client.create_listener(
            name=f"test-lst-{uuid.uuid4().hex[:8]}",
            serviceIdentifier=svc_id,
            protocol="HTTP",
            port=80,
            defaultAction={"fixedResponse": {"statusCode": 200}},
        )
        lst_id = listener_resp["id"]
        try:
            rule_resp = vpclattice_client.create_rule(
                listenerIdentifier=lst_id,
                serviceIdentifier=svc_id,
                name="test-rule-update",
                priority=10,
                action={"fixedResponse": {"statusCode": 200}},
                match={"httpMatch": {"pathMatch": {"match": {"exact": "/update"}}}},
            )
            rule_id = rule_resp["id"]
            try:
                update_resp = vpclattice_client.update_rule(
                    serviceIdentifier=svc_id,
                    listenerIdentifier=lst_id,
                    ruleIdentifier=rule_id,
                    priority=15,
                )
                assert update_resp["id"] == rule_id
                assert update_resp["priority"] == 15
            finally:
                vpclattice_client.delete_rule(
                    serviceIdentifier=svc_id,
                    listenerIdentifier=lst_id,
                    ruleIdentifier=rule_id,
                )
        finally:
            vpclattice_client.delete_listener(serviceIdentifier=svc_id, listenerIdentifier=lst_id)


class TestVpcLatticeUpdateTargetGroup:
    """Tests for UpdateTargetGroup operation."""

    def test_update_target_group(self, vpclattice_client):
        """UpdateTargetGroup changes health check configuration."""
        name = f"test-tg-upd-{uuid.uuid4().hex[:8]}"
        created = vpclattice_client.create_target_group(name=name, type="INSTANCE")
        tg_id = created["id"]
        try:
            update_resp = vpclattice_client.update_target_group(
                targetGroupIdentifier=tg_id,
                healthCheck={"enabled": True, "healthCheckIntervalSeconds": 30},
            )
            assert update_resp["id"] == tg_id
            assert "config" in update_resp
        finally:
            vpclattice_client.delete_target_group(targetGroupIdentifier=tg_id)
