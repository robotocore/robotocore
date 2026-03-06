"""Resource Groups compatibility tests."""

import uuid

import pytest

from tests.compatibility.conftest import make_client


@pytest.fixture
def resource_groups():
    return make_client("resource-groups")


def _uid():
    return uuid.uuid4().hex[:8]


RESOURCE_QUERY = {
    "Type": "TAG_FILTERS_1_0",
    "Query": (
        '{"ResourceTypeFilters":["AWS::AllSupported"],'
        '"TagFilters":[{"Key":"env","Values":["test"]}]}'
    ),
}


class TestResourceGroupsOperations:
    def test_create_group(self, resource_groups):
        name = f"test-group-{_uid()}"
        response = resource_groups.create_group(
            Name=name,
            Description="A test resource group",
            ResourceQuery=RESOURCE_QUERY,
        )
        assert response["Group"]["Name"] == name
        assert response["Group"]["Description"] == "A test resource group"
        resource_groups.delete_group(GroupName=name)

    def test_get_group(self, resource_groups):
        name = f"get-group-{_uid()}"
        resource_groups.create_group(
            Name=name,
            Description="Get group test",
            ResourceQuery=RESOURCE_QUERY,
        )
        response = resource_groups.get_group(GroupName=name)
        assert response["Group"]["Name"] == name
        assert response["Group"]["Description"] == "Get group test"
        resource_groups.delete_group(GroupName=name)

    def test_list_groups(self, resource_groups):
        name = f"list-group-{_uid()}"
        resource_groups.create_group(
            Name=name,
            Description="List group test",
            ResourceQuery=RESOURCE_QUERY,
        )
        response = resource_groups.list_groups()
        group_arns = [g["GroupArn"] for g in response["GroupIdentifiers"]]
        assert any(name in arn for arn in group_arns)
        resource_groups.delete_group(GroupName=name)

    def test_update_group(self, resource_groups):
        name = f"update-group-{_uid()}"
        resource_groups.create_group(
            Name=name,
            Description="Original description",
            ResourceQuery=RESOURCE_QUERY,
        )
        response = resource_groups.update_group(
            GroupName=name,
            Description="Updated description",
        )
        assert response["Group"]["Description"] == "Updated description"
        resource_groups.delete_group(GroupName=name)

    def test_delete_group(self, resource_groups):
        name = f"delete-group-{_uid()}"
        resource_groups.create_group(
            Name=name,
            Description="Delete group test",
            ResourceQuery=RESOURCE_QUERY,
        )
        response = resource_groups.delete_group(GroupName=name)
        assert response["Group"]["Name"] == name
