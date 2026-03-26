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


class TestGetUpdateDeleteApplication:
    """Tests for GetApplication, UpdateApplication, DeleteApplication."""

    @pytest.fixture
    def client(self):
        return make_client("servicecatalog-appregistry")

    def test_get_application(self, client):
        name = _uid("app")
        created = client.create_application(name=name, clientToken=uuid.uuid4().hex)
        app_id = created["application"]["id"]
        resp = client.get_application(application=app_id)
        assert resp["id"] == app_id
        assert resp["name"] == name

    def test_update_application(self, client):
        name = _uid("app")
        created = client.create_application(name=name, clientToken=uuid.uuid4().hex)
        app_id = created["application"]["id"]
        new_desc = "updated description"
        resp = client.update_application(application=app_id, description=new_desc)
        assert resp["application"]["description"] == new_desc

    def test_delete_application(self, client):
        name = _uid("app")
        created = client.create_application(name=name, clientToken=uuid.uuid4().hex)
        app_id = created["application"]["id"]
        resp = client.delete_application(application=app_id)
        assert resp["application"]["id"] == app_id
        # Verify it's gone
        with pytest.raises(client.exceptions.ResourceNotFoundException):
            client.get_application(application=app_id)

    def test_get_application_not_found(self, client):
        with pytest.raises(client.exceptions.ResourceNotFoundException):
            client.get_application(application="nonexistent-id-xyz")


class TestAttributeGroupCRUD:
    """Tests for attribute group CRUD operations."""

    @pytest.fixture
    def client(self):
        return make_client("servicecatalog-appregistry")

    def test_create_attribute_group(self, client):
        name = _uid("ag")
        resp = client.create_attribute_group(
            name=name,
            attributes='{"key": "value"}',
            clientToken=uuid.uuid4().hex,
        )
        assert "attributeGroup" in resp
        assert resp["attributeGroup"]["name"] == name
        assert "id" in resp["attributeGroup"]

    def test_get_attribute_group(self, client):
        name = _uid("ag")
        created = client.create_attribute_group(
            name=name,
            attributes='{"key": "value"}',
            clientToken=uuid.uuid4().hex,
        )
        ag_id = created["attributeGroup"]["id"]
        resp = client.get_attribute_group(attributeGroup=ag_id)
        assert resp["id"] == ag_id
        assert resp["name"] == name

    def test_update_attribute_group(self, client):
        name = _uid("ag")
        created = client.create_attribute_group(
            name=name,
            attributes='{"key": "value"}',
            clientToken=uuid.uuid4().hex,
        )
        ag_id = created["attributeGroup"]["id"]
        new_name = _uid("ag-updated")
        resp = client.update_attribute_group(
            attributeGroup=ag_id,
            name=new_name,
        )
        assert resp["attributeGroup"]["name"] == new_name
        assert resp["attributeGroup"]["id"] == ag_id

    def test_delete_attribute_group(self, client):
        name = _uid("ag")
        created = client.create_attribute_group(
            name=name,
            attributes='{"key": "value"}',
            clientToken=uuid.uuid4().hex,
        )
        ag_id = created["attributeGroup"]["id"]
        resp = client.delete_attribute_group(attributeGroup=ag_id)
        assert resp["attributeGroup"]["id"] == ag_id
        with pytest.raises(client.exceptions.ResourceNotFoundException):
            client.get_attribute_group(attributeGroup=ag_id)

    def test_list_attribute_groups(self, client):
        name = _uid("ag")
        client.create_attribute_group(
            name=name,
            attributes='{"key": "value"}',
            clientToken=uuid.uuid4().hex,
        )
        resp = client.list_attribute_groups()
        assert "attributeGroups" in resp
        names = [ag["name"] for ag in resp["attributeGroups"]]
        assert name in names


class TestAttributeGroupAssociations:
    """Tests for associating/disassociating attribute groups."""

    @pytest.fixture
    def client(self):
        return make_client("servicecatalog-appregistry")

    @pytest.fixture
    def app_and_ag(self, client):
        app = client.create_application(
            name=_uid("app"), clientToken=uuid.uuid4().hex
        )["application"]
        ag = client.create_attribute_group(
            name=_uid("ag"),
            attributes='{"env": "test"}',
            clientToken=uuid.uuid4().hex,
        )["attributeGroup"]
        return app, ag

    def test_associate_attribute_group(self, client, app_and_ag):
        app, ag = app_and_ag
        resp = client.associate_attribute_group(
            application=app["id"], attributeGroup=ag["id"]
        )
        assert "applicationArn" in resp
        assert "attributeGroupArn" in resp

    def test_list_associated_attribute_groups(self, client, app_and_ag):
        app, ag = app_and_ag
        client.associate_attribute_group(
            application=app["id"], attributeGroup=ag["id"]
        )
        resp = client.list_associated_attribute_groups(application=app["id"])
        assert "attributeGroups" in resp
        assert ag["arn"] in resp["attributeGroups"]

    def test_disassociate_attribute_group(self, client, app_and_ag):
        app, ag = app_and_ag
        client.associate_attribute_group(
            application=app["id"], attributeGroup=ag["id"]
        )
        resp = client.disassociate_attribute_group(
            application=app["id"], attributeGroup=ag["id"]
        )
        assert "applicationArn" in resp
        # Verify removed
        list_resp = client.list_associated_attribute_groups(application=app["id"])
        assert ag["arn"] not in list_resp["attributeGroups"]

    def test_list_attribute_groups_for_application(self, client, app_and_ag):
        app, ag = app_and_ag
        client.associate_attribute_group(
            application=app["id"], attributeGroup=ag["id"]
        )
        resp = client.list_attribute_groups_for_application(application=app["id"])
        assert "attributeGroupsDetails" in resp
        ids = [d["id"] for d in resp["attributeGroupsDetails"]]
        assert ag["id"] in ids


class TestTaggingOperations:
    """Tests for TagResource, UntagResource, ListTagsForResource."""

    @pytest.fixture
    def client(self):
        return make_client("servicecatalog-appregistry")

    def test_tag_and_list_and_untag_resource(self, client):
        app = client.create_application(
            name=_uid("app"), clientToken=uuid.uuid4().hex
        )["application"]
        arn = app["arn"]
        client.tag_resource(resourceArn=arn, tags={"env": "test", "project": "demo"})
        resp = client.list_tags_for_resource(resourceArn=arn)
        assert resp["tags"]["env"] == "test"
        assert resp["tags"]["project"] == "demo"
        client.untag_resource(resourceArn=arn, tagKeys=["project"])
        resp2 = client.list_tags_for_resource(resourceArn=arn)
        assert "project" not in resp2["tags"]
        assert resp2["tags"]["env"] == "test"


class TestDisassociateResource:
    """Tests for DisassociateResource."""

    @pytest.fixture
    def client(self):
        return make_client("servicecatalog-appregistry")

    def test_disassociate_resource_not_found(self, client):
        """DisassociateResource on nonexistent app raises ResourceNotFoundException."""
        with pytest.raises(client.exceptions.ResourceNotFoundException):
            client.disassociate_resource(
                application="nonexistent-app-xyz",
                resourceType="CFN_STACK",
                resource="some-stack",
            )
