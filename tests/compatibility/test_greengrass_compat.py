"""Greengrass compatibility tests."""

import pytest
from botocore.exceptions import ClientError

from tests.compatibility.conftest import make_client

CORE_INITIAL_VERSION = {
    "Cores": [
        {
            "CertificateArn": "arn:aws:iot:us-east-1:123456789012:cert/abc",
            "Id": "core1",
            "ThingArn": "arn:aws:iot:us-east-1:123456789012:thing/TestCore",
        }
    ]
}

DEVICE_INITIAL_VERSION = {
    "Devices": [
        {
            "CertificateArn": "arn:aws:iot:us-east-1:123456789012:cert/abc",
            "Id": "dev1",
            "ThingArn": "arn:aws:iot:us-east-1:123456789012:thing/TestDevice",
        }
    ]
}

FUNCTION_INITIAL_VERSION = {
    "Functions": [
        {
            "FunctionArn": "arn:aws:lambda:us-east-1:123456789012:function:TestFunc",
            "Id": "func1",
            "FunctionConfiguration": {"MemorySize": 128, "Timeout": 3},
        }
    ]
}

RESOURCE_INITIAL_VERSION = {
    "Resources": [
        {
            "Id": "res1",
            "Name": "TestResource",
            "ResourceDataContainer": {
                "LocalVolumeResourceData": {
                    "SourcePath": "/src",
                    "DestinationPath": "/dst",
                    "GroupOwnerSetting": {"AutoAddGroupOwner": True},
                }
            },
        }
    ]
}

CONNECTOR_INITIAL_VERSION = {
    "Connectors": [
        {
            "ConnectorArn": "arn:aws:greengrass:us-east-1::/connectors/SNS/versions/1",
            "Id": "conn1",
            "Parameters": {},
        }
    ]
}

LOGGER_INITIAL_VERSION = {
    "Loggers": [
        {
            "Component": "GreengrassSystem",
            "Id": "logger1",
            "Level": "INFO",
            "Space": 1024,
            "Type": "FileSystem",
        }
    ]
}

SUBSCRIPTION_INITIAL_VERSION = {
    "Subscriptions": [
        {
            "Id": "sub1",
            "Source": "cloud",
            "Subject": "topic/test",
            "Target": "cloud",
        }
    ]
}


@pytest.fixture
def greengrass():
    return make_client("greengrass")


class TestGreengrassOperations:
    # --- List operations ---

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

    # --- Group CRUD ---

    def test_create_group(self, greengrass):
        """CreateGroup creates a group and returns its ID."""
        resp = greengrass.create_group(Name="test-create-group")
        try:
            assert "Id" in resp
            assert resp["Name"] == "test-create-group"
        finally:
            greengrass.delete_group(GroupId=resp["Id"])

    def test_get_group(self, greengrass):
        """GetGroup returns a previously created group."""
        resp = greengrass.create_group(Name="test-get-group")
        group_id = resp["Id"]
        try:
            result = greengrass.get_group(GroupId=group_id)
            assert result["Id"] == group_id
            assert result["Name"] == "test-get-group"
        finally:
            greengrass.delete_group(GroupId=group_id)

    def test_update_group(self, greengrass):
        """UpdateGroup updates a group's name."""
        resp = greengrass.create_group(Name="test-update-group")
        group_id = resp["Id"]
        try:
            greengrass.update_group(GroupId=group_id, Name="updated-name")
            result = greengrass.get_group(GroupId=group_id)
            assert result["Name"] == "updated-name"
        finally:
            greengrass.delete_group(GroupId=group_id)

    def test_delete_group(self, greengrass):
        """DeleteGroup removes a group."""
        resp = greengrass.create_group(Name="test-delete-group")
        group_id = resp["Id"]
        greengrass.delete_group(GroupId=group_id)
        with pytest.raises(ClientError) as exc:
            greengrass.get_group(GroupId=group_id)
        assert exc.value.response["Error"]["Code"] in (
            "IdNotFoundException",
            "ResourceNotFoundException",
            "NotFoundException",
        )

    # --- GroupVersion ---

    def test_create_group_version(self, greengrass):
        """CreateGroupVersion creates a version for a group."""
        cd = greengrass.create_core_definition(Name="gv-core", InitialVersion=CORE_INITIAL_VERSION)
        group = greengrass.create_group(Name="test-group-ver")
        try:
            resp = greengrass.create_group_version(
                GroupId=group["Id"],
                CoreDefinitionVersionArn=cd["LatestVersionArn"],
            )
            assert "Version" in resp
            assert resp["ResponseMetadata"]["HTTPStatusCode"] == 201
        finally:
            greengrass.delete_group(GroupId=group["Id"])
            greengrass.delete_core_definition(CoreDefinitionId=cd["Id"])

    def test_get_group_version(self, greengrass):
        """GetGroupVersion returns a group version."""
        cd = greengrass.create_core_definition(Name="gv-core2", InitialVersion=CORE_INITIAL_VERSION)
        group = greengrass.create_group(Name="test-get-gv")
        try:
            gv = greengrass.create_group_version(
                GroupId=group["Id"],
                CoreDefinitionVersionArn=cd["LatestVersionArn"],
            )
            result = greengrass.get_group_version(GroupId=group["Id"], GroupVersionId=gv["Version"])
            assert "Definition" in result
            assert result["Version"] == gv["Version"]
        finally:
            greengrass.delete_group(GroupId=group["Id"])
            greengrass.delete_core_definition(CoreDefinitionId=cd["Id"])

    def test_list_group_versions(self, greengrass):
        """ListGroupVersions returns versions for a group."""
        group = greengrass.create_group(Name="test-list-gv")
        try:
            result = greengrass.list_group_versions(GroupId=group["Id"])
            assert "Versions" in result
            assert isinstance(result["Versions"], list)
        finally:
            greengrass.delete_group(GroupId=group["Id"])

    # --- CoreDefinition CRUD ---

    def test_create_core_definition(self, greengrass):
        """CreateCoreDefinition creates a core definition."""
        resp = greengrass.create_core_definition(
            Name="test-core-def", InitialVersion=CORE_INITIAL_VERSION
        )
        try:
            assert "Id" in resp
            assert resp["Name"] == "test-core-def"
        finally:
            greengrass.delete_core_definition(CoreDefinitionId=resp["Id"])

    def test_get_core_definition(self, greengrass):
        """GetCoreDefinition returns a previously created core definition."""
        resp = greengrass.create_core_definition(
            Name="test-get-core", InitialVersion=CORE_INITIAL_VERSION
        )
        cd_id = resp["Id"]
        try:
            result = greengrass.get_core_definition(CoreDefinitionId=cd_id)
            assert result["Id"] == cd_id
            assert result["Name"] == "test-get-core"
        finally:
            greengrass.delete_core_definition(CoreDefinitionId=cd_id)

    def test_update_core_definition(self, greengrass):
        """UpdateCoreDefinition updates name."""
        resp = greengrass.create_core_definition(
            Name="test-upd-core", InitialVersion=CORE_INITIAL_VERSION
        )
        cd_id = resp["Id"]
        try:
            greengrass.update_core_definition(CoreDefinitionId=cd_id, Name="updated-core")
            result = greengrass.get_core_definition(CoreDefinitionId=cd_id)
            assert result["Name"] == "updated-core"
        finally:
            greengrass.delete_core_definition(CoreDefinitionId=cd_id)

    def test_delete_core_definition(self, greengrass):
        """DeleteCoreDefinition removes a core definition."""
        resp = greengrass.create_core_definition(
            Name="test-del-core", InitialVersion=CORE_INITIAL_VERSION
        )
        cd_id = resp["Id"]
        greengrass.delete_core_definition(CoreDefinitionId=cd_id)
        with pytest.raises(ClientError) as exc:
            greengrass.get_core_definition(CoreDefinitionId=cd_id)
        assert exc.value.response["Error"]["Code"] in (
            "IdNotFoundException",
            "ResourceNotFoundException",
            "NotFoundException",
        )

    # --- CoreDefinitionVersion ---

    def test_get_core_definition_version(self, greengrass):
        """GetCoreDefinitionVersion returns a core definition version."""
        resp = greengrass.create_core_definition(
            Name="test-cdv", InitialVersion=CORE_INITIAL_VERSION
        )
        try:
            result = greengrass.get_core_definition_version(
                CoreDefinitionId=resp["Id"],
                CoreDefinitionVersionId=resp["LatestVersion"],
            )
            assert "Definition" in result
            assert "Cores" in result["Definition"]
        finally:
            greengrass.delete_core_definition(CoreDefinitionId=resp["Id"])

    def test_list_core_definition_versions(self, greengrass):
        """ListCoreDefinitionVersions returns versions."""
        resp = greengrass.create_core_definition(
            Name="test-lcdv", InitialVersion=CORE_INITIAL_VERSION
        )
        try:
            result = greengrass.list_core_definition_versions(CoreDefinitionId=resp["Id"])
            assert "Versions" in result
            assert len(result["Versions"]) >= 1
        finally:
            greengrass.delete_core_definition(CoreDefinitionId=resp["Id"])

    # --- DeviceDefinition CRUD ---

    def test_create_device_definition(self, greengrass):
        """CreateDeviceDefinition creates a device definition."""
        resp = greengrass.create_device_definition(
            Name="test-dev-def", InitialVersion=DEVICE_INITIAL_VERSION
        )
        try:
            assert "Id" in resp
            assert resp["Name"] == "test-dev-def"
        finally:
            greengrass.delete_device_definition(DeviceDefinitionId=resp["Id"])

    def test_get_device_definition(self, greengrass):
        """GetDeviceDefinition returns a device definition."""
        resp = greengrass.create_device_definition(
            Name="test-get-dev", InitialVersion=DEVICE_INITIAL_VERSION
        )
        dd_id = resp["Id"]
        try:
            result = greengrass.get_device_definition(DeviceDefinitionId=dd_id)
            assert result["Id"] == dd_id
            assert result["Name"] == "test-get-dev"
        finally:
            greengrass.delete_device_definition(DeviceDefinitionId=dd_id)

    def test_update_device_definition(self, greengrass):
        """UpdateDeviceDefinition updates name."""
        resp = greengrass.create_device_definition(
            Name="test-upd-dev", InitialVersion=DEVICE_INITIAL_VERSION
        )
        dd_id = resp["Id"]
        try:
            greengrass.update_device_definition(DeviceDefinitionId=dd_id, Name="updated-dev")
            result = greengrass.get_device_definition(DeviceDefinitionId=dd_id)
            assert result["Name"] == "updated-dev"
        finally:
            greengrass.delete_device_definition(DeviceDefinitionId=dd_id)

    def test_delete_device_definition(self, greengrass):
        """DeleteDeviceDefinition removes a device definition."""
        resp = greengrass.create_device_definition(
            Name="test-del-dev", InitialVersion=DEVICE_INITIAL_VERSION
        )
        dd_id = resp["Id"]
        greengrass.delete_device_definition(DeviceDefinitionId=dd_id)
        with pytest.raises(ClientError) as exc:
            greengrass.get_device_definition(DeviceDefinitionId=dd_id)
        assert exc.value.response["Error"]["Code"] in (
            "IdNotFoundException",
            "ResourceNotFoundException",
            "NotFoundException",
        )

    # --- DeviceDefinitionVersion ---

    def test_create_device_definition_version(self, greengrass):
        """CreateDeviceDefinitionVersion creates a new version."""
        resp = greengrass.create_device_definition(
            Name="test-ddv", InitialVersion=DEVICE_INITIAL_VERSION
        )
        try:
            result = greengrass.create_device_definition_version(
                DeviceDefinitionId=resp["Id"],
                Devices=[
                    {
                        "CertificateArn": "arn:aws:iot:us-east-1:123456789012:cert/xyz",
                        "Id": "dev2",
                        "ThingArn": "arn:aws:iot:us-east-1:123456789012:thing/Dev2",
                    }
                ],
            )
            assert "Version" in result
        finally:
            greengrass.delete_device_definition(DeviceDefinitionId=resp["Id"])

    def test_get_device_definition_version(self, greengrass):
        """GetDeviceDefinitionVersion returns a version."""
        resp = greengrass.create_device_definition(
            Name="test-gddv", InitialVersion=DEVICE_INITIAL_VERSION
        )
        try:
            result = greengrass.get_device_definition_version(
                DeviceDefinitionId=resp["Id"],
                DeviceDefinitionVersionId=resp["LatestVersion"],
            )
            assert "Definition" in result
            assert "Devices" in result["Definition"]
        finally:
            greengrass.delete_device_definition(DeviceDefinitionId=resp["Id"])

    def test_list_device_definition_versions(self, greengrass):
        """ListDeviceDefinitionVersions returns versions."""
        resp = greengrass.create_device_definition(
            Name="test-lddv", InitialVersion=DEVICE_INITIAL_VERSION
        )
        try:
            result = greengrass.list_device_definition_versions(DeviceDefinitionId=resp["Id"])
            assert "Versions" in result
            assert len(result["Versions"]) >= 1
        finally:
            greengrass.delete_device_definition(DeviceDefinitionId=resp["Id"])

    # --- FunctionDefinition CRUD ---

    def test_create_function_definition(self, greengrass):
        """CreateFunctionDefinition creates a function definition."""
        resp = greengrass.create_function_definition(
            Name="test-func-def", InitialVersion=FUNCTION_INITIAL_VERSION
        )
        try:
            assert "Id" in resp
            assert resp["Name"] == "test-func-def"
        finally:
            greengrass.delete_function_definition(FunctionDefinitionId=resp["Id"])

    def test_get_function_definition(self, greengrass):
        """GetFunctionDefinition returns a function definition."""
        resp = greengrass.create_function_definition(
            Name="test-get-func", InitialVersion=FUNCTION_INITIAL_VERSION
        )
        fd_id = resp["Id"]
        try:
            result = greengrass.get_function_definition(FunctionDefinitionId=fd_id)
            assert result["Id"] == fd_id
            assert result["Name"] == "test-get-func"
        finally:
            greengrass.delete_function_definition(FunctionDefinitionId=fd_id)

    def test_update_function_definition(self, greengrass):
        """UpdateFunctionDefinition updates name."""
        resp = greengrass.create_function_definition(
            Name="test-upd-func", InitialVersion=FUNCTION_INITIAL_VERSION
        )
        fd_id = resp["Id"]
        try:
            greengrass.update_function_definition(FunctionDefinitionId=fd_id, Name="updated-func")
            result = greengrass.get_function_definition(FunctionDefinitionId=fd_id)
            assert result["Name"] == "updated-func"
        finally:
            greengrass.delete_function_definition(FunctionDefinitionId=fd_id)

    def test_delete_function_definition(self, greengrass):
        """DeleteFunctionDefinition removes a function definition."""
        resp = greengrass.create_function_definition(
            Name="test-del-func", InitialVersion=FUNCTION_INITIAL_VERSION
        )
        fd_id = resp["Id"]
        greengrass.delete_function_definition(FunctionDefinitionId=fd_id)
        with pytest.raises(ClientError) as exc:
            greengrass.get_function_definition(FunctionDefinitionId=fd_id)
        assert exc.value.response["Error"]["Code"] in (
            "IdNotFoundException",
            "ResourceNotFoundException",
            "NotFoundException",
        )

    # --- FunctionDefinitionVersion ---

    def test_create_function_definition_version(self, greengrass):
        """CreateFunctionDefinitionVersion creates a new version."""
        resp = greengrass.create_function_definition(
            Name="test-fdv", InitialVersion=FUNCTION_INITIAL_VERSION
        )
        try:
            result = greengrass.create_function_definition_version(
                FunctionDefinitionId=resp["Id"],
                Functions=[
                    {
                        "FunctionArn": "arn:aws:lambda:us-east-1:123456789012:function:F2",
                        "Id": "func2",
                        "FunctionConfiguration": {"MemorySize": 256, "Timeout": 5},
                    }
                ],
            )
            assert "Version" in result
        finally:
            greengrass.delete_function_definition(FunctionDefinitionId=resp["Id"])

    def test_get_function_definition_version(self, greengrass):
        """GetFunctionDefinitionVersion returns a version."""
        resp = greengrass.create_function_definition(
            Name="test-gfdv", InitialVersion=FUNCTION_INITIAL_VERSION
        )
        try:
            result = greengrass.get_function_definition_version(
                FunctionDefinitionId=resp["Id"],
                FunctionDefinitionVersionId=resp["LatestVersion"],
            )
            assert "Definition" in result
            assert "Functions" in result["Definition"]
        finally:
            greengrass.delete_function_definition(FunctionDefinitionId=resp["Id"])

    def test_list_function_definition_versions(self, greengrass):
        """ListFunctionDefinitionVersions returns versions."""
        resp = greengrass.create_function_definition(
            Name="test-lfdv", InitialVersion=FUNCTION_INITIAL_VERSION
        )
        try:
            result = greengrass.list_function_definition_versions(FunctionDefinitionId=resp["Id"])
            assert "Versions" in result
            assert len(result["Versions"]) >= 1
        finally:
            greengrass.delete_function_definition(FunctionDefinitionId=resp["Id"])

    # --- ResourceDefinition CRUD ---

    def test_create_resource_definition(self, greengrass):
        """CreateResourceDefinition creates a resource definition."""
        resp = greengrass.create_resource_definition(
            Name="test-res-def", InitialVersion=RESOURCE_INITIAL_VERSION
        )
        try:
            assert "Id" in resp
            assert resp["Name"] == "test-res-def"
        finally:
            greengrass.delete_resource_definition(ResourceDefinitionId=resp["Id"])

    def test_get_resource_definition(self, greengrass):
        """GetResourceDefinition returns a resource definition."""
        resp = greengrass.create_resource_definition(
            Name="test-get-res", InitialVersion=RESOURCE_INITIAL_VERSION
        )
        rd_id = resp["Id"]
        try:
            result = greengrass.get_resource_definition(ResourceDefinitionId=rd_id)
            assert result["Id"] == rd_id
            assert result["Name"] == "test-get-res"
        finally:
            greengrass.delete_resource_definition(ResourceDefinitionId=rd_id)

    def test_update_resource_definition(self, greengrass):
        """UpdateResourceDefinition updates name."""
        resp = greengrass.create_resource_definition(
            Name="test-upd-res", InitialVersion=RESOURCE_INITIAL_VERSION
        )
        rd_id = resp["Id"]
        try:
            greengrass.update_resource_definition(ResourceDefinitionId=rd_id, Name="updated-res")
            result = greengrass.get_resource_definition(ResourceDefinitionId=rd_id)
            assert result["Name"] == "updated-res"
        finally:
            greengrass.delete_resource_definition(ResourceDefinitionId=rd_id)

    def test_delete_resource_definition(self, greengrass):
        """DeleteResourceDefinition removes a resource definition."""
        resp = greengrass.create_resource_definition(
            Name="test-del-res", InitialVersion=RESOURCE_INITIAL_VERSION
        )
        rd_id = resp["Id"]
        greengrass.delete_resource_definition(ResourceDefinitionId=rd_id)
        with pytest.raises(ClientError) as exc:
            greengrass.get_resource_definition(ResourceDefinitionId=rd_id)
        assert exc.value.response["Error"]["Code"] in (
            "IdNotFoundException",
            "ResourceNotFoundException",
            "NotFoundException",
        )

    # --- ResourceDefinitionVersion ---

    def test_create_resource_definition_version(self, greengrass):
        """CreateResourceDefinitionVersion creates a new version."""
        resp = greengrass.create_resource_definition(
            Name="test-rdv", InitialVersion=RESOURCE_INITIAL_VERSION
        )
        try:
            result = greengrass.create_resource_definition_version(
                ResourceDefinitionId=resp["Id"],
                Resources=[
                    {
                        "Id": "res2",
                        "Name": "Res2",
                        "ResourceDataContainer": {
                            "LocalVolumeResourceData": {
                                "SourcePath": "/src2",
                                "DestinationPath": "/dst2",
                                "GroupOwnerSetting": {"AutoAddGroupOwner": True},
                            }
                        },
                    }
                ],
            )
            assert "Version" in result
        finally:
            greengrass.delete_resource_definition(ResourceDefinitionId=resp["Id"])

    def test_get_resource_definition_version(self, greengrass):
        """GetResourceDefinitionVersion returns a version."""
        resp = greengrass.create_resource_definition(
            Name="test-grdv", InitialVersion=RESOURCE_INITIAL_VERSION
        )
        try:
            result = greengrass.get_resource_definition_version(
                ResourceDefinitionId=resp["Id"],
                ResourceDefinitionVersionId=resp["LatestVersion"],
            )
            assert "Definition" in result
            assert "Resources" in result["Definition"]
        finally:
            greengrass.delete_resource_definition(ResourceDefinitionId=resp["Id"])

    def test_list_resource_definition_versions(self, greengrass):
        """ListResourceDefinitionVersions returns versions."""
        resp = greengrass.create_resource_definition(
            Name="test-lrdv", InitialVersion=RESOURCE_INITIAL_VERSION
        )
        try:
            result = greengrass.list_resource_definition_versions(ResourceDefinitionId=resp["Id"])
            assert "Versions" in result
            assert len(result["Versions"]) >= 1
        finally:
            greengrass.delete_resource_definition(ResourceDefinitionId=resp["Id"])

    # --- SubscriptionDefinition CRUD ---

    def test_create_subscription_definition(self, greengrass):
        """CreateSubscriptionDefinition creates a subscription definition."""
        resp = greengrass.create_subscription_definition(
            Name="test-sub-def", InitialVersion=SUBSCRIPTION_INITIAL_VERSION
        )
        try:
            assert "Id" in resp
            assert resp["Name"] == "test-sub-def"
        finally:
            greengrass.delete_subscription_definition(SubscriptionDefinitionId=resp["Id"])

    def test_get_subscription_definition(self, greengrass):
        """GetSubscriptionDefinition returns a subscription definition."""
        resp = greengrass.create_subscription_definition(
            Name="test-get-sub", InitialVersion=SUBSCRIPTION_INITIAL_VERSION
        )
        sd_id = resp["Id"]
        try:
            result = greengrass.get_subscription_definition(SubscriptionDefinitionId=sd_id)
            assert result["Id"] == sd_id
            assert result["Name"] == "test-get-sub"
        finally:
            greengrass.delete_subscription_definition(SubscriptionDefinitionId=sd_id)

    def test_update_subscription_definition(self, greengrass):
        """UpdateSubscriptionDefinition updates name."""
        resp = greengrass.create_subscription_definition(
            Name="test-upd-sub", InitialVersion=SUBSCRIPTION_INITIAL_VERSION
        )
        sd_id = resp["Id"]
        try:
            greengrass.update_subscription_definition(
                SubscriptionDefinitionId=sd_id, Name="updated-sub"
            )
            result = greengrass.get_subscription_definition(SubscriptionDefinitionId=sd_id)
            assert result["Name"] == "updated-sub"
        finally:
            greengrass.delete_subscription_definition(SubscriptionDefinitionId=sd_id)

    def test_delete_subscription_definition(self, greengrass):
        """DeleteSubscriptionDefinition removes a subscription definition."""
        resp = greengrass.create_subscription_definition(
            Name="test-del-sub", InitialVersion=SUBSCRIPTION_INITIAL_VERSION
        )
        sd_id = resp["Id"]
        greengrass.delete_subscription_definition(SubscriptionDefinitionId=sd_id)
        with pytest.raises(ClientError) as exc:
            greengrass.get_subscription_definition(SubscriptionDefinitionId=sd_id)
        assert exc.value.response["Error"]["Code"] in (
            "IdNotFoundException",
            "ResourceNotFoundException",
            "NotFoundException",
        )

    # --- SubscriptionDefinitionVersion ---

    def test_get_subscription_definition_version(self, greengrass):
        """GetSubscriptionDefinitionVersion returns a version."""
        resp = greengrass.create_subscription_definition(
            Name="test-gsdv", InitialVersion=SUBSCRIPTION_INITIAL_VERSION
        )
        try:
            result = greengrass.get_subscription_definition_version(
                SubscriptionDefinitionId=resp["Id"],
                SubscriptionDefinitionVersionId=resp["LatestVersion"],
            )
            assert "Definition" in result
            assert "Subscriptions" in result["Definition"]
        finally:
            greengrass.delete_subscription_definition(SubscriptionDefinitionId=resp["Id"])

    def test_list_subscription_definition_versions(self, greengrass):
        """ListSubscriptionDefinitionVersions returns versions."""
        resp = greengrass.create_subscription_definition(
            Name="test-lsdv", InitialVersion=SUBSCRIPTION_INITIAL_VERSION
        )
        try:
            result = greengrass.list_subscription_definition_versions(
                SubscriptionDefinitionId=resp["Id"]
            )
            assert "Versions" in result
            assert len(result["Versions"]) >= 1
        finally:
            greengrass.delete_subscription_definition(SubscriptionDefinitionId=resp["Id"])

    # --- ConnectorDefinition ---

    def test_list_connector_definitions(self, greengrass):
        """ListConnectorDefinitions returns a list of definitions."""
        response = greengrass.list_connector_definitions()
        assert "Definitions" in response
        assert isinstance(response["Definitions"], list)

    def test_get_connector_definition(self, greengrass):
        """GetConnectorDefinition returns a previously created connector definition."""
        resp = greengrass.create_connector_definition(
            Name="test-get-conn", InitialVersion=CONNECTOR_INITIAL_VERSION
        )
        conn_id = resp["Id"]
        try:
            result = greengrass.get_connector_definition(ConnectorDefinitionId=conn_id)
            assert result["Id"] == conn_id
            assert result["Name"] == "test-get-conn"
        finally:
            greengrass.delete_connector_definition(ConnectorDefinitionId=conn_id)

    def test_get_connector_definition_version(self, greengrass):
        """GetConnectorDefinitionVersion returns a connector definition version."""
        resp = greengrass.create_connector_definition(
            Name="test-gcdv", InitialVersion=CONNECTOR_INITIAL_VERSION
        )
        try:
            result = greengrass.get_connector_definition_version(
                ConnectorDefinitionId=resp["Id"],
                ConnectorDefinitionVersionId=resp["LatestVersion"],
            )
            assert "Definition" in result
            assert "Connectors" in result["Definition"]
        finally:
            greengrass.delete_connector_definition(ConnectorDefinitionId=resp["Id"])

    def test_list_connector_definition_versions(self, greengrass):
        """ListConnectorDefinitionVersions returns versions."""
        resp = greengrass.create_connector_definition(
            Name="test-lcdv-conn", InitialVersion=CONNECTOR_INITIAL_VERSION
        )
        try:
            result = greengrass.list_connector_definition_versions(ConnectorDefinitionId=resp["Id"])
            assert "Versions" in result
            assert len(result["Versions"]) >= 1
        finally:
            greengrass.delete_connector_definition(ConnectorDefinitionId=resp["Id"])

    # --- LoggerDefinition ---

    def test_list_logger_definitions(self, greengrass):
        """ListLoggerDefinitions returns a list of definitions."""
        response = greengrass.list_logger_definitions()
        assert "Definitions" in response
        assert isinstance(response["Definitions"], list)

    def test_get_logger_definition(self, greengrass):
        """GetLoggerDefinition returns a previously created logger definition."""
        resp = greengrass.create_logger_definition(
            Name="test-get-logger", InitialVersion=LOGGER_INITIAL_VERSION
        )
        logger_id = resp["Id"]
        try:
            result = greengrass.get_logger_definition(LoggerDefinitionId=logger_id)
            assert result["Id"] == logger_id
            assert result["Name"] == "test-get-logger"
        finally:
            greengrass.delete_logger_definition(LoggerDefinitionId=logger_id)

    def test_get_logger_definition_version(self, greengrass):
        """GetLoggerDefinitionVersion returns a logger definition version."""
        resp = greengrass.create_logger_definition(
            Name="test-gldv", InitialVersion=LOGGER_INITIAL_VERSION
        )
        try:
            result = greengrass.get_logger_definition_version(
                LoggerDefinitionId=resp["Id"],
                LoggerDefinitionVersionId=resp["LatestVersion"],
            )
            assert "Definition" in result
            assert "Loggers" in result["Definition"]
        finally:
            greengrass.delete_logger_definition(LoggerDefinitionId=resp["Id"])

    def test_list_logger_definition_versions(self, greengrass):
        """ListLoggerDefinitionVersions returns versions."""
        resp = greengrass.create_logger_definition(
            Name="test-lldv", InitialVersion=LOGGER_INITIAL_VERSION
        )
        try:
            result = greengrass.list_logger_definition_versions(LoggerDefinitionId=resp["Id"])
            assert "Versions" in result
            assert len(result["Versions"]) >= 1
        finally:
            greengrass.delete_logger_definition(LoggerDefinitionId=resp["Id"])

    # --- Deployment ---

    def test_create_deployment(self, greengrass):
        """CreateDeployment creates a deployment for a group."""
        cd = greengrass.create_core_definition(Name="dep-core", InitialVersion=CORE_INITIAL_VERSION)
        group = greengrass.create_group(Name="test-deploy")
        try:
            gv = greengrass.create_group_version(
                GroupId=group["Id"],
                CoreDefinitionVersionArn=cd["LatestVersionArn"],
            )
            resp = greengrass.create_deployment(
                GroupId=group["Id"],
                GroupVersionId=gv["Version"],
                DeploymentType="NewDeployment",
            )
            assert "DeploymentId" in resp
        finally:
            greengrass.delete_group(GroupId=group["Id"])
            greengrass.delete_core_definition(CoreDefinitionId=cd["Id"])

    def test_get_deployment_status(self, greengrass):
        """GetDeploymentStatus returns status for a deployment."""
        cd = greengrass.create_core_definition(
            Name="dep-core2", InitialVersion=CORE_INITIAL_VERSION
        )
        group = greengrass.create_group(Name="test-dep-status")
        try:
            gv = greengrass.create_group_version(
                GroupId=group["Id"],
                CoreDefinitionVersionArn=cd["LatestVersionArn"],
            )
            dep = greengrass.create_deployment(
                GroupId=group["Id"],
                GroupVersionId=gv["Version"],
                DeploymentType="NewDeployment",
            )
            result = greengrass.get_deployment_status(
                GroupId=group["Id"], DeploymentId=dep["DeploymentId"]
            )
            assert "DeploymentStatus" in result
        finally:
            greengrass.delete_group(GroupId=group["Id"])
            greengrass.delete_core_definition(CoreDefinitionId=cd["Id"])

    def test_list_deployments(self, greengrass):
        """ListDeployments returns deployments for a group."""
        group = greengrass.create_group(Name="test-list-dep")
        try:
            result = greengrass.list_deployments(GroupId=group["Id"])
            assert "Deployments" in result
            assert isinstance(result["Deployments"], list)
        finally:
            greengrass.delete_group(GroupId=group["Id"])

    # --- Role association ---

    def test_associate_role_to_group(self, greengrass):
        """AssociateRoleToGroup associates a role with a group."""
        group = greengrass.create_group(Name="test-role-assoc")
        try:
            resp = greengrass.associate_role_to_group(
                GroupId=group["Id"],
                RoleArn="arn:aws:iam::123456789012:role/TestRole",
            )
            assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200
        finally:
            greengrass.disassociate_role_from_group(GroupId=group["Id"])
            greengrass.delete_group(GroupId=group["Id"])

    def test_get_associated_role(self, greengrass):
        """GetAssociatedRole returns the role associated with a group."""
        group = greengrass.create_group(Name="test-get-role")
        try:
            greengrass.associate_role_to_group(
                GroupId=group["Id"],
                RoleArn="arn:aws:iam::123456789012:role/TestRole",
            )
            result = greengrass.get_associated_role(GroupId=group["Id"])
            assert "RoleArn" in result
            assert "arn:aws:iam:" in result["RoleArn"]
        finally:
            greengrass.disassociate_role_from_group(GroupId=group["Id"])
            greengrass.delete_group(GroupId=group["Id"])

    def test_disassociate_role_from_group(self, greengrass):
        """DisassociateRoleFromGroup removes the role from a group."""
        group = greengrass.create_group(Name="test-disassoc-role")
        try:
            greengrass.associate_role_to_group(
                GroupId=group["Id"],
                RoleArn="arn:aws:iam::123456789012:role/TestRole",
            )
            resp = greengrass.disassociate_role_from_group(GroupId=group["Id"])
            assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200
        finally:
            greengrass.delete_group(GroupId=group["Id"])

    # --- ConnectorDefinitionVersion ---

    def test_create_connector_definition_version(self, greengrass):
        """CreateConnectorDefinitionVersion creates a new version."""
        resp = greengrass.create_connector_definition(
            Name="test-cdv-conn", InitialVersion=CONNECTOR_INITIAL_VERSION
        )
        try:
            result = greengrass.create_connector_definition_version(
                ConnectorDefinitionId=resp["Id"],
                Connectors=[
                    {
                        "ConnectorArn": (
                            "arn:aws:greengrass:us-east-1::/connectors/CloudWatch/versions/1"
                        ),
                        "Id": "conn2",
                        "Parameters": {},
                    }
                ],
            )
            assert "Version" in result
        finally:
            greengrass.delete_connector_definition(ConnectorDefinitionId=resp["Id"])

    def test_update_connector_definition(self, greengrass):
        """UpdateConnectorDefinition updates a connector definition name."""
        resp = greengrass.create_connector_definition(
            Name="test-upd-conn", InitialVersion=CONNECTOR_INITIAL_VERSION
        )
        conn_id = resp["Id"]
        try:
            greengrass.update_connector_definition(
                ConnectorDefinitionId=conn_id, Name="updated-conn"
            )
            result = greengrass.get_connector_definition(ConnectorDefinitionId=conn_id)
            assert result["Name"] == "updated-conn"
        finally:
            greengrass.delete_connector_definition(ConnectorDefinitionId=conn_id)

    # --- ConnectorDefinition (standalone CRUD) ---

    def test_delete_connector_definition(self, greengrass):
        """DeleteConnectorDefinition removes a connector definition."""
        resp = greengrass.create_connector_definition(
            Name="test-del-conn", InitialVersion=CONNECTOR_INITIAL_VERSION
        )
        conn_id = resp["Id"]
        greengrass.delete_connector_definition(ConnectorDefinitionId=conn_id)
        with pytest.raises(ClientError) as exc:
            greengrass.get_connector_definition(ConnectorDefinitionId=conn_id)
        assert exc.value.response["Error"]["Code"] in (
            "IdNotFoundException",
            "ResourceNotFoundException",
            "NotFoundException",
        )

    # --- LoggerDefinition (standalone CRUD) ---

    def test_delete_logger_definition(self, greengrass):
        """DeleteLoggerDefinition removes a logger definition."""
        resp = greengrass.create_logger_definition(
            Name="test-del-logger", InitialVersion=LOGGER_INITIAL_VERSION
        )
        logger_id = resp["Id"]
        greengrass.delete_logger_definition(LoggerDefinitionId=logger_id)
        with pytest.raises(ClientError) as exc:
            greengrass.get_logger_definition(LoggerDefinitionId=logger_id)
        assert exc.value.response["Error"]["Code"] in (
            "IdNotFoundException",
            "ResourceNotFoundException",
            "NotFoundException",
        )

    # --- CoreDefinitionVersion (create) ---

    def test_create_core_definition_version(self, greengrass):
        """CreateCoreDefinitionVersion creates a new version."""
        resp = greengrass.create_core_definition(
            Name="test-ccdv", InitialVersion=CORE_INITIAL_VERSION
        )
        try:
            result = greengrass.create_core_definition_version(
                CoreDefinitionId=resp["Id"],
                Cores=[
                    {
                        "CertificateArn": "arn:aws:iot:us-east-1:123456789012:cert/xyz",
                        "Id": "core2",
                        "ThingArn": "arn:aws:iot:us-east-1:123456789012:thing/Core2",
                    }
                ],
            )
            assert "Version" in result
        finally:
            greengrass.delete_core_definition(CoreDefinitionId=resp["Id"])

    # --- LoggerDefinitionVersion ---

    def test_create_logger_definition_version(self, greengrass):
        """CreateLoggerDefinitionVersion creates a new version."""
        resp = greengrass.create_logger_definition(
            Name="test-cldv", InitialVersion=LOGGER_INITIAL_VERSION
        )
        try:
            result = greengrass.create_logger_definition_version(
                LoggerDefinitionId=resp["Id"],
                Loggers=[
                    {
                        "Component": "Lambda",
                        "Id": "logger2",
                        "Level": "DEBUG",
                        "Space": 2048,
                        "Type": "FileSystem",
                    }
                ],
            )
            assert "Version" in result
        finally:
            greengrass.delete_logger_definition(LoggerDefinitionId=resp["Id"])

    def test_update_logger_definition(self, greengrass):
        """UpdateLoggerDefinition updates a logger definition name."""
        resp = greengrass.create_logger_definition(
            Name="test-upd-logger", InitialVersion=LOGGER_INITIAL_VERSION
        )
        logger_id = resp["Id"]
        try:
            greengrass.update_logger_definition(LoggerDefinitionId=logger_id, Name="updated-logger")
            result = greengrass.get_logger_definition(LoggerDefinitionId=logger_id)
            assert result["Name"] == "updated-logger"
        finally:
            greengrass.delete_logger_definition(LoggerDefinitionId=logger_id)

    # --- SubscriptionDefinitionVersion (create) ---

    def test_create_subscription_definition_version(self, greengrass):
        """CreateSubscriptionDefinitionVersion creates a new version."""
        resp = greengrass.create_subscription_definition(
            Name="test-csdv", InitialVersion=SUBSCRIPTION_INITIAL_VERSION
        )
        try:
            result = greengrass.create_subscription_definition_version(
                SubscriptionDefinitionId=resp["Id"],
                Subscriptions=[
                    {
                        "Id": "sub2",
                        "Source": "cloud",
                        "Subject": "topic/other",
                        "Target": "cloud",
                    }
                ],
            )
            assert "Version" in result
        finally:
            greengrass.delete_subscription_definition(SubscriptionDefinitionId=resp["Id"])

    # --- Group with InitialVersion ---

    def test_create_group_with_initial_version(self, greengrass):
        """CreateGroup with InitialVersion creates both group and version."""
        cd = greengrass.create_core_definition(Name="gi-core", InitialVersion=CORE_INITIAL_VERSION)
        try:
            group = greengrass.create_group(
                Name="test-init-ver",
                InitialVersion={"CoreDefinitionVersionArn": cd["LatestVersionArn"]},
            )
            assert "Id" in group
            assert "LatestVersion" in group
            assert len(group["LatestVersion"]) > 0
            greengrass.delete_group(GroupId=group["Id"])
        finally:
            greengrass.delete_core_definition(CoreDefinitionId=cd["Id"])

    # --- Full GroupVersion with multiple definitions ---

    def test_group_version_with_multiple_definitions(self, greengrass):
        """CreateGroupVersion with core + function + logger defs."""
        cd = greengrass.create_core_definition(
            Name="full-core", InitialVersion=CORE_INITIAL_VERSION
        )
        fd = greengrass.create_function_definition(
            Name="full-func", InitialVersion=FUNCTION_INITIAL_VERSION
        )
        group = greengrass.create_group(Name="full-group")
        try:
            gv = greengrass.create_group_version(
                GroupId=group["Id"],
                CoreDefinitionVersionArn=cd["LatestVersionArn"],
                FunctionDefinitionVersionArn=fd["LatestVersionArn"],
            )
            result = greengrass.get_group_version(GroupId=group["Id"], GroupVersionId=gv["Version"])
            defn = result["Definition"]
            assert "CoreDefinitionVersionArn" in defn
            assert "FunctionDefinitionVersionArn" in defn
        finally:
            greengrass.delete_group(GroupId=group["Id"])
            greengrass.delete_core_definition(CoreDefinitionId=cd["Id"])
            greengrass.delete_function_definition(FunctionDefinitionId=fd["Id"])

    # --- List groups returns created groups ---

    def test_list_groups_contains_created(self, greengrass):
        """ListGroups returns previously created groups."""
        g1 = greengrass.create_group(Name="list-g1")
        g2 = greengrass.create_group(Name="list-g2")
        try:
            resp = greengrass.list_groups()
            ids = {g["Id"] for g in resp["Groups"]}
            assert g1["Id"] in ids
            assert g2["Id"] in ids
        finally:
            greengrass.delete_group(GroupId=g1["Id"])
            greengrass.delete_group(GroupId=g2["Id"])

    # --- Deployment list contains created deployment ---

    def test_list_deployments_contains_created(self, greengrass):
        """ListDeployments returns a deployment after creation."""
        cd = greengrass.create_core_definition(
            Name="deplist-core", InitialVersion=CORE_INITIAL_VERSION
        )
        group = greengrass.create_group(Name="deplist-group")
        try:
            gv = greengrass.create_group_version(
                GroupId=group["Id"],
                CoreDefinitionVersionArn=cd["LatestVersionArn"],
            )
            dep = greengrass.create_deployment(
                GroupId=group["Id"],
                GroupVersionId=gv["Version"],
                DeploymentType="NewDeployment",
            )
            result = greengrass.list_deployments(GroupId=group["Id"])
            dep_ids = [d["DeploymentId"] for d in result["Deployments"]]
            assert dep["DeploymentId"] in dep_ids
        finally:
            greengrass.delete_group(GroupId=group["Id"])
            greengrass.delete_core_definition(CoreDefinitionId=cd["Id"])

    # --- Definition Arn fields ---

    def test_connector_definition_has_arn(self, greengrass):
        """CreateConnectorDefinition returns Arn and LatestVersionArn."""
        resp = greengrass.create_connector_definition(
            Name="arn-conn", InitialVersion=CONNECTOR_INITIAL_VERSION
        )
        try:
            assert "Arn" in resp
            assert "LatestVersionArn" in resp
            assert "greengrass" in resp["Arn"]
        finally:
            greengrass.delete_connector_definition(ConnectorDefinitionId=resp["Id"])

    def test_core_definition_has_arn(self, greengrass):
        """CreateCoreDefinition returns Arn and LatestVersionArn."""
        resp = greengrass.create_core_definition(
            Name="arn-core", InitialVersion=CORE_INITIAL_VERSION
        )
        try:
            assert "Arn" in resp
            assert "LatestVersionArn" in resp
        finally:
            greengrass.delete_core_definition(CoreDefinitionId=resp["Id"])

    def test_device_definition_has_arn(self, greengrass):
        """CreateDeviceDefinition returns Arn and LatestVersionArn."""
        resp = greengrass.create_device_definition(
            Name="arn-dev", InitialVersion=DEVICE_INITIAL_VERSION
        )
        try:
            assert "Arn" in resp
            assert "LatestVersionArn" in resp
        finally:
            greengrass.delete_device_definition(DeviceDefinitionId=resp["Id"])

    def test_function_definition_has_arn(self, greengrass):
        """CreateFunctionDefinition returns Arn and LatestVersionArn."""
        resp = greengrass.create_function_definition(
            Name="arn-func", InitialVersion=FUNCTION_INITIAL_VERSION
        )
        try:
            assert "Arn" in resp
            assert "LatestVersionArn" in resp
        finally:
            greengrass.delete_function_definition(FunctionDefinitionId=resp["Id"])

    def test_logger_definition_has_arn(self, greengrass):
        """CreateLoggerDefinition returns Arn and LatestVersionArn."""
        resp = greengrass.create_logger_definition(
            Name="arn-logger", InitialVersion=LOGGER_INITIAL_VERSION
        )
        try:
            assert "Arn" in resp
            assert "LatestVersionArn" in resp
        finally:
            greengrass.delete_logger_definition(LoggerDefinitionId=resp["Id"])

    def test_resource_definition_has_arn(self, greengrass):
        """CreateResourceDefinition returns Arn and LatestVersionArn."""
        resp = greengrass.create_resource_definition(
            Name="arn-res", InitialVersion=RESOURCE_INITIAL_VERSION
        )
        try:
            assert "Arn" in resp
            assert "LatestVersionArn" in resp
        finally:
            greengrass.delete_resource_definition(ResourceDefinitionId=resp["Id"])

    def test_subscription_definition_has_arn(self, greengrass):
        """CreateSubscriptionDefinition returns Arn and LatestVersionArn."""
        resp = greengrass.create_subscription_definition(
            Name="arn-sub", InitialVersion=SUBSCRIPTION_INITIAL_VERSION
        )
        try:
            assert "Arn" in resp
            assert "LatestVersionArn" in resp
        finally:
            greengrass.delete_subscription_definition(SubscriptionDefinitionId=resp["Id"])

    def test_group_has_arn(self, greengrass):
        """CreateGroup returns Arn."""
        group = greengrass.create_group(Name="arn-group")
        try:
            assert "Arn" in group
            assert "greengrass" in group["Arn"]
        finally:
            greengrass.delete_group(GroupId=group["Id"])

    # --- Multiple versions per definition ---

    def test_connector_multiple_versions(self, greengrass):
        """Creating 2 versions results in 2 items in ListConnectorDefinitionVersions."""
        resp = greengrass.create_connector_definition(
            Name="mv-conn", InitialVersion=CONNECTOR_INITIAL_VERSION
        )
        try:
            greengrass.create_connector_definition_version(
                ConnectorDefinitionId=resp["Id"],
                Connectors=[
                    {
                        "ConnectorArn": (
                            "arn:aws:greengrass:us-east-1::/connectors/CloudWatch/versions/1"
                        ),
                        "Id": "conn2",
                        "Parameters": {},
                    }
                ],
            )
            versions = greengrass.list_connector_definition_versions(
                ConnectorDefinitionId=resp["Id"]
            )
            assert len(versions["Versions"]) == 2
        finally:
            greengrass.delete_connector_definition(ConnectorDefinitionId=resp["Id"])

    def test_core_multiple_versions(self, greengrass):
        """Creating 2 core versions results in 2 items in list."""
        resp = greengrass.create_core_definition(
            Name="mv-core", InitialVersion=CORE_INITIAL_VERSION
        )
        try:
            greengrass.create_core_definition_version(
                CoreDefinitionId=resp["Id"],
                Cores=[
                    {
                        "CertificateArn": "arn:aws:iot:us-east-1:123456789012:cert/xyz",
                        "Id": "core2",
                        "ThingArn": "arn:aws:iot:us-east-1:123456789012:thing/Core2",
                    }
                ],
            )
            versions = greengrass.list_core_definition_versions(CoreDefinitionId=resp["Id"])
            assert len(versions["Versions"]) == 2
        finally:
            greengrass.delete_core_definition(CoreDefinitionId=resp["Id"])

    # --- Update then verify via Get ---

    def test_update_group_name_persists(self, greengrass):
        """UpdateGroup name change is reflected in GetGroup."""
        group = greengrass.create_group(Name="pre-update")
        try:
            greengrass.update_group(GroupId=group["Id"], Name="post-update")
            result = greengrass.get_group(GroupId=group["Id"])
            assert result["Name"] == "post-update"
        finally:
            greengrass.delete_group(GroupId=group["Id"])

    # --- Deployment status ---

    def test_deployment_status_has_type(self, greengrass):
        """GetDeploymentStatus returns DeploymentType."""
        cd = greengrass.create_core_definition(Name="ds-core", InitialVersion=CORE_INITIAL_VERSION)
        group = greengrass.create_group(Name="ds-group")
        try:
            gv = greengrass.create_group_version(
                GroupId=group["Id"],
                CoreDefinitionVersionArn=cd["LatestVersionArn"],
            )
            dep = greengrass.create_deployment(
                GroupId=group["Id"],
                GroupVersionId=gv["Version"],
                DeploymentType="NewDeployment",
            )
            result = greengrass.get_deployment_status(
                GroupId=group["Id"], DeploymentId=dep["DeploymentId"]
            )
            assert "DeploymentType" in result
            assert result["DeploymentType"] == "NewDeployment"
        finally:
            greengrass.delete_group(GroupId=group["Id"])
            greengrass.delete_core_definition(CoreDefinitionId=cd["Id"])
