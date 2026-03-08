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
