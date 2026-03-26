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
            names = [
                g.get("GroupName", g.get("Name", "")) for g in resp.get("GroupIdentifiers", [])
            ]
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

    def test_get_group_configuration(self, resource_groups):
        name = f"gcfg-{_uid()}"
        resource_groups.create_group(
            Name=name,
            Description="Config group",
            ResourceQuery=RESOURCE_QUERY,
        )
        try:
            resp = resource_groups.get_group_configuration(Group=name)
            assert "GroupConfiguration" in resp
        finally:
            resource_groups.delete_group(GroupName=name)


class TestResourceGroupsGapStubs:
    """Tests for gap operations: get_account_settings."""

    @pytest.fixture
    def resource_groups(self):
        return make_client("resource-groups")

    def test_get_account_settings(self, resource_groups):
        resp = resource_groups.get_account_settings()
        assert "AccountSettings" in resp


class TestResourceGroupsAutoCoverage:
    """Auto-generated coverage tests for resource-groups."""

    @pytest.fixture
    def client(self):
        return make_client("resource-groups")

    def test_list_group_resources(self, client):
        """ListGroupResources returns ResourceIdentifiers for an existing group."""
        name = f"list-res-{_uid()}"
        client.create_group(
            Name=name,
            Description="List resources group",
            ResourceQuery=RESOURCE_QUERY,
        )
        try:
            resp = client.list_group_resources(Group=name)
            assert "ResourceIdentifiers" in resp
        finally:
            client.delete_group(GroupName=name)

    def test_list_tag_sync_tasks(self, client):
        """ListTagSyncTasks returns a response."""
        resp = client.list_tag_sync_tasks()
        assert "TagSyncTasks" in resp

    def test_put_group_configuration(self, client):
        """PutGroupConfiguration succeeds for a configuration-based group."""
        name = f"putcfg-{_uid()}"
        client.create_group(
            Name=name,
            Description="Put config group",
            Configuration=[
                {
                    "Type": "AWS::ResourceGroups::Generic",
                    "Parameters": [
                        {"Name": "allowed-resource-types", "Values": ["AWS::EC2::Instance"]}
                    ],
                }
            ],
        )
        try:
            resp = client.put_group_configuration(
                Group=name,
                Configuration=[
                    {
                        "Type": "AWS::ResourceGroups::Generic",
                        "Parameters": [
                            {
                                "Name": "allowed-resource-types",
                                "Values": ["AWS::EC2::Instance"],
                            }
                        ],
                    }
                ],
            )
            assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200
        finally:
            client.delete_group(GroupName=name)

    def test_update_account_settings(self, client):
        """UpdateAccountSettings returns a response."""
        resp = client.update_account_settings()
        assert "AccountSettings" in resp


class TestTagSyncTasks:
    """Tests for tag sync task operations."""

    @pytest.fixture
    def client(self):
        return make_client("resource-groups")

    @pytest.fixture
    def group_with_task(self, client):
        name = f"sync-group-{_uid()}"
        client.create_group(
            Name=name,
            Description="Tag sync test group",
            ResourceQuery=RESOURCE_QUERY,
        )
        resp = client.start_tag_sync_task(
            Group=name,
            TagKey="env",
            TagValue="test",
            RoleArn="arn:aws:iam::123456789012:role/test-role",
        )
        task_arn = resp["TaskArn"]
        yield name, task_arn
        try:
            client.delete_group(GroupName=name)
        except Exception:
            pass

    def test_start_tag_sync_task(self, client):
        """StartTagSyncTask returns task details including TaskArn."""
        name = f"start-sync-{_uid()}"
        client.create_group(
            Name=name,
            Description="Start sync task group",
            ResourceQuery=RESOURCE_QUERY,
        )
        try:
            resp = client.start_tag_sync_task(
                Group=name,
                TagKey="team",
                TagValue="platform",
                RoleArn="arn:aws:iam::123456789012:role/test-role",
            )
            assert "TaskArn" in resp
            assert resp["TagKey"] == "team"
            assert resp["TagValue"] == "platform"
        finally:
            client.delete_group(GroupName=name)

    def test_get_tag_sync_task(self, client, group_with_task):
        """GetTagSyncTask returns task status and metadata."""
        _name, task_arn = group_with_task
        resp = client.get_tag_sync_task(TaskArn=task_arn)
        assert resp["TaskArn"] == task_arn
        assert "Status" in resp
        assert "CreatedAt" in resp

    def test_cancel_tag_sync_task(self, client):
        """CancelTagSyncTask returns HTTP 200."""
        name = f"cancel-sync-{_uid()}"
        client.create_group(
            Name=name,
            Description="Cancel sync task group",
            ResourceQuery=RESOURCE_QUERY,
        )
        try:
            start_resp = client.start_tag_sync_task(
                Group=name,
                TagKey="env",
                TagValue="staging",
                RoleArn="arn:aws:iam::123456789012:role/test-role",
            )
            task_arn = start_resp["TaskArn"]
            cancel_resp = client.cancel_tag_sync_task(TaskArn=task_arn)
            assert cancel_resp["ResponseMetadata"]["HTTPStatusCode"] == 200
        finally:
            client.delete_group(GroupName=name)


class TestGroupResourcesOperations:
    """Tests for GroupResources, UngroupResources, ListGroupingStatuses."""

    @pytest.fixture
    def rg_client(self):
        return make_client("resource-groups")

    def test_group_resources(self, rg_client):
        """GroupResources adds ARNs to a group and returns Succeeded list."""
        name = f"grp-res-{_uid()}"
        rg_client.create_group(
            Name=name, Description="Group for resources", ResourceQuery=RESOURCE_QUERY
        )
        try:
            test_arn = f"arn:aws:ec2:us-east-1:123456789012:instance/i-{_uid()}"
            resp = rg_client.group_resources(Group=name, ResourceArns=[test_arn])
            assert test_arn in resp["Succeeded"]
            assert resp["Failed"] == []
        finally:
            rg_client.delete_group(Group=name)

    def test_ungroup_resources(self, rg_client):
        """UngroupResources removes previously grouped ARNs."""
        name = f"ungrp-res-{_uid()}"
        rg_client.create_group(
            Name=name, Description="Group for ungroup", ResourceQuery=RESOURCE_QUERY
        )
        try:
            test_arn = f"arn:aws:ec2:us-east-1:123456789012:instance/i-{_uid()}"
            rg_client.group_resources(Group=name, ResourceArns=[test_arn])
            resp = rg_client.ungroup_resources(Group=name, ResourceArns=[test_arn])
            assert test_arn in resp["Succeeded"]
            assert resp["Failed"] == []
        finally:
            rg_client.delete_group(Group=name)

    def test_list_grouping_statuses(self, rg_client):
        """ListGroupingStatuses returns group name and status list."""
        name = f"list-grp-status-{_uid()}"
        rg_client.create_group(
            Name=name, Description="Group for statuses", ResourceQuery=RESOURCE_QUERY
        )
        try:
            test_arn = f"arn:aws:ec2:us-east-1:123456789012:instance/i-{_uid()}"
            rg_client.group_resources(Group=name, ResourceArns=[test_arn])
            resp = rg_client.list_grouping_statuses(Group=name)
            assert resp["Group"] == name
            assert "GroupingStatuses" in resp
            arns = [s["ResourceArn"] for s in resp["GroupingStatuses"]]
            assert test_arn in arns
        finally:
            rg_client.delete_group(Group=name)
