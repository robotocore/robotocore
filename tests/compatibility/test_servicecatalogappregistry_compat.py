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
