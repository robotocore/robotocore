"""Greengrass compatibility tests."""

import pytest

from tests.compatibility.conftest import make_client


@pytest.fixture
def greengrass():
    return make_client("greengrass")


class TestGreengrassOperations:
    def test_list_core_definitions(self, greengrass):
        """ListCoreDefinitions returns a list of definitions."""
        response = greengrass.list_core_definitions()
        assert "Definitions" in response
        assert isinstance(response["Definitions"], list)

    def test_list_device_definitions(self, greengrass):
        """ListDeviceDefinitions returns a list of definitions."""
        response = greengrass.list_device_definitions()
        assert "Definitions" in response
        assert isinstance(response["Definitions"], list)

    def test_list_function_definitions(self, greengrass):
        """ListFunctionDefinitions returns a list of definitions."""
        response = greengrass.list_function_definitions()
        assert "Definitions" in response
        assert isinstance(response["Definitions"], list)

    def test_list_groups(self, greengrass):
        """ListGroups returns a list of groups."""
        response = greengrass.list_groups()
        assert "Groups" in response
        assert isinstance(response["Groups"], list)

    def test_list_resource_definitions(self, greengrass):
        """ListResourceDefinitions returns a list of definitions."""
        response = greengrass.list_resource_definitions()
        assert "Definitions" in response
        assert isinstance(response["Definitions"], list)

    def test_list_subscription_definitions(self, greengrass):
        """ListSubscriptionDefinitions returns a list of definitions."""
        response = greengrass.list_subscription_definitions()
        assert "Definitions" in response
        assert isinstance(response["Definitions"], list)

    def test_list_core_definitions_status_code(self, greengrass):
        """ListCoreDefinitions returns HTTP 200."""
        response = greengrass.list_core_definitions()
        assert response["ResponseMetadata"]["HTTPStatusCode"] == 200

    def test_list_groups_status_code(self, greengrass):
        """ListGroups returns HTTP 200."""
        response = greengrass.list_groups()
        assert response["ResponseMetadata"]["HTTPStatusCode"] == 200
