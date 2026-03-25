"""Service Catalog AppRegistry compatibility tests."""

import uuid

import pytest

from tests.compatibility.conftest import make_client


@pytest.fixture
def appregistry():
    return make_client("servicecatalog-appregistry")


def _uid(prefix="test"):
    return f"{prefix}-{uuid.uuid4().hex[:8]}"


class TestAppRegistryListOperations:
    def test_list_applications(self, appregistry):
        response = appregistry.list_applications()
        assert "applications" in response
        assert isinstance(response["applications"], list)


class TestAppRegistryApplicationCRUD:
    def test_create_application(self, appregistry):
        name = _uid("app")
        resp = appregistry.create_application(
            name=name,
            clientToken=uuid.uuid4().hex,
        )
        assert "application" in resp
        assert resp["application"]["name"] == name
        assert "id" in resp["application"]


class TestServicecatalogappregistryAutoCoverage:
    """Auto-generated coverage tests for servicecatalogappregistry."""

    @pytest.fixture
    def client(self):
        return make_client("servicecatalog-appregistry")

    def test_get_configuration(self, client):
        """GetConfiguration returns a response."""
        resp = client.get_configuration()
        assert "configuration" in resp


class TestPutConfiguration:
    """Tests for PutConfiguration."""

    @pytest.fixture
    def client(self):
        return make_client("servicecatalog-appregistry")

    def test_put_configuration(self, client):
        """PutConfiguration sets the tag query configuration."""
        client.put_configuration(
            configuration={"tagQueryConfiguration": {"tagKey": "awsApplication"}}
        )
        resp = client.get_configuration()
        assert resp["configuration"]["tagQueryConfiguration"]["tagKey"] == "awsApplication"


class TestListAssociatedResources:
    """Tests for ListAssociatedResources."""

    @pytest.fixture
    def client(self):
        return make_client("servicecatalog-appregistry")

    @pytest.fixture
    def application(self, client):
        """Create an application and return its details."""
        name = f"test-app-{uuid.uuid4().hex[:8]}"
        resp = client.create_application(name=name, clientToken=uuid.uuid4().hex)
        return resp["application"]

    def test_list_associated_resources_empty(self, client, application):
        """ListAssociatedResources returns empty list for new application."""
        resp = client.list_associated_resources(application=application["id"])
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200
        assert "resources" in resp
        assert isinstance(resp["resources"], list)


class TestAssociateResource:
    """Tests for AssociateResource."""

    @pytest.fixture
    def client(self):
        return make_client("servicecatalog-appregistry")

    @pytest.fixture
    def application(self, client):
        """Create an application and return its details."""
        name = f"test-app-{uuid.uuid4().hex[:8]}"
        resp = client.create_application(name=name, clientToken=uuid.uuid4().hex)
        return resp["application"]

    def test_associate_resource_not_found(self, client, application):
        """AssociateResource raises ResourceNotFoundException for nonexistent stack."""
        with pytest.raises(client.exceptions.ResourceNotFoundException):
            client.associate_resource(
                application=application["id"],
                resourceType="CFN_STACK",
                resource="nonexistent-stack-xyz",
            )
