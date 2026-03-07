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

    def test_update_group_query(self, resource_groups):
        name = f"query-group-{_uid()}"
        resource_groups.create_group(
            Name=name, Description="Query update", ResourceQuery=RESOURCE_QUERY
        )
        new_query = {
            "Type": "TAG_FILTERS_1_0",
            "Query": (
                '{"ResourceTypeFilters":["AWS::EC2::Instance"],'
                '"TagFilters":[{"Key":"env","Values":["prod"]}]}'
            ),
        }
        response = resource_groups.update_group_query(GroupName=name, ResourceQuery=new_query)
        assert response["GroupQuery"]["ResourceQuery"]["Type"] == "TAG_FILTERS_1_0"
        resource_groups.delete_group(GroupName=name)


class TestResourceGroupsExtended:
    @pytest.fixture
    def resource_groups(self):
        return make_client("resource-groups")

    def test_create_group_with_tags(self, resource_groups):
        name = f"tag-group-{_uid()}"
        resp = resource_groups.create_group(
            Name=name,
            Description="Tagged group",
            ResourceQuery=RESOURCE_QUERY,
            Tags={"env": "test", "team": "platform"},
        )
        try:
            assert resp["Group"]["Name"] == name
            assert resp["Tags"]["env"] == "test"
            assert resp["Tags"]["team"] == "platform"
        finally:
            resource_groups.delete_group(GroupName=name)

    @pytest.mark.xfail(reason="get_tags not implemented")
    def test_get_tags(self, resource_groups):
        name = f"gettag-group-{_uid()}"
        resp = resource_groups.create_group(
            Name=name,
            Description="Get tags group",
            ResourceQuery=RESOURCE_QUERY,
            Tags={"env": "staging"},
        )
        arn = resp["Group"]["GroupArn"]
        try:
            tags = resource_groups.get_tags(Arn=arn)
            assert tags["Tags"]["env"] == "staging"
        finally:
            resource_groups.delete_group(GroupName=name)

    @pytest.mark.xfail(reason="tag/untag not implemented for resource-groups")
    def test_tag_and_untag_group(self, resource_groups):
        name = f"tagop-group-{_uid()}"
        resp = resource_groups.create_group(
            Name=name,
            Description="Tag op group",
            ResourceQuery=RESOURCE_QUERY,
        )
        arn = resp["Group"]["GroupArn"]
        try:
            resource_groups.tag(Arn=arn, Tags={"new_tag": "value"})
            tags = resource_groups.get_tags(Arn=arn)
            assert tags["Tags"]["new_tag"] == "value"
            resource_groups.untag(Arn=arn, Keys=["new_tag"])
            tags2 = resource_groups.get_tags(Arn=arn)
            assert "new_tag" not in tags2.get("Tags", {})
        finally:
            resource_groups.delete_group(GroupName=name)

    def test_get_group_query(self, resource_groups):
        name = f"query-group-{_uid()}"
        resource_groups.create_group(
            Name=name,
            Description="Query group",
            ResourceQuery=RESOURCE_QUERY,
        )
        try:
            resp = resource_groups.get_group_query(GroupName=name)
            assert resp["GroupQuery"]["ResourceQuery"]["Type"] == "TAG_FILTERS_1_0"
        finally:
            resource_groups.delete_group(GroupName=name)

    def test_list_groups_returns_created(self, resource_groups):
        name = f"listg-{_uid()}"
        resource_groups.create_group(
            Name=name,
            Description="List group",
            ResourceQuery=RESOURCE_QUERY,
        )
        try:
            resp = resource_groups.list_groups()
            names = [g.get("GroupName", g.get("Name", "")) for g in resp.get("GroupIdentifiers", [])]
            assert name in names
        finally:
            resource_groups.delete_group(GroupName=name)

    def test_group_has_arn(self, resource_groups):
        name = f"arng-{_uid()}"
        resp = resource_groups.create_group(
            Name=name,
            Description="Arn group",
            ResourceQuery=RESOURCE_QUERY,
        )
        try:
            assert "GroupArn" in resp["Group"]
            assert name in resp["Group"]["GroupArn"]
        finally:
            resource_groups.delete_group(GroupName=name)

    def test_update_group_description(self, resource_groups):
        name = f"updg-{_uid()}"
        resource_groups.create_group(
            Name=name,
            Description="Original",
            ResourceQuery=RESOURCE_QUERY,
        )
        try:
            resp = resource_groups.update_group(GroupName=name, Description="Updated")
            assert resp["Group"]["Description"] == "Updated"
        finally:
            resource_groups.delete_group(GroupName=name)

    def test_delete_group_returns_group(self, resource_groups):
        name = f"delg-{_uid()}"
        resource_groups.create_group(
            Name=name,
            Description="Delete group",
            ResourceQuery=RESOURCE_QUERY,
        )
        resp = resource_groups.delete_group(GroupName=name)
        assert resp["Group"]["Name"] == name
