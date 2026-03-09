"""QuickSight compatibility tests."""

import uuid

import pytest

from tests.compatibility.conftest import make_client

ACCOUNT_ID = "123456789012"
NAMESPACE = "default"


@pytest.fixture
def quicksight():
    return make_client("quicksight")


class TestQuickSightAccountSettings:
    def test_describe_account_settings(self, quicksight):
        response = quicksight.describe_account_settings(AwsAccountId=ACCOUNT_ID)
        assert response["Status"] == 200
        assert "AccountSettings" in response

    def test_update_account_settings(self, quicksight):
        response = quicksight.update_account_settings(
            AwsAccountId=ACCOUNT_ID,
            DefaultNamespace="default",
        )
        assert response["Status"] == 200


class TestQuickSightDashboards:
    def test_list_dashboards(self, quicksight):
        response = quicksight.list_dashboards(AwsAccountId=ACCOUNT_ID)
        assert response["Status"] == 200
        assert isinstance(response["DashboardSummaryList"], list)

    def test_create_and_describe_dashboard(self, quicksight):
        dash_id = f"test-dash-{uuid.uuid4().hex[:8]}"
        create_resp = quicksight.create_dashboard(
            AwsAccountId=ACCOUNT_ID,
            DashboardId=dash_id,
            Name="Test Dashboard",
            SourceEntity={
                "SourceTemplate": {
                    "Arn": f"arn:aws:quicksight:us-east-1:{ACCOUNT_ID}:template/fake-tmpl",
                    "DataSetReferences": [
                        {
                            "DataSetPlaceholder": "placeholder",
                            "DataSetArn": f"arn:aws:quicksight:us-east-1:{ACCOUNT_ID}:dataset/ds",
                        }
                    ],
                }
            },
        )
        assert create_resp["Status"] in (200, 201, 202)
        assert "DashboardId" in create_resp

        describe_resp = quicksight.describe_dashboard(AwsAccountId=ACCOUNT_ID, DashboardId=dash_id)
        assert describe_resp["Status"] == 200
        assert "Dashboard" in describe_resp
        assert describe_resp["Dashboard"]["DashboardId"] == dash_id

    def test_list_tags_for_resource(self, quicksight):
        dash_id = f"test-dash-{uuid.uuid4().hex[:8]}"
        quicksight.create_dashboard(
            AwsAccountId=ACCOUNT_ID,
            DashboardId=dash_id,
            Name="Tagged Dashboard",
            SourceEntity={
                "SourceTemplate": {
                    "Arn": f"arn:aws:quicksight:us-east-1:{ACCOUNT_ID}:template/fake-tmpl",
                    "DataSetReferences": [
                        {
                            "DataSetPlaceholder": "placeholder",
                            "DataSetArn": f"arn:aws:quicksight:us-east-1:{ACCOUNT_ID}:dataset/ds",
                        }
                    ],
                }
            },
        )
        arn = f"arn:aws:quicksight:us-east-1:{ACCOUNT_ID}:dashboard/{dash_id}"
        tag_resp = quicksight.list_tags_for_resource(ResourceArn=arn)
        assert tag_resp["Status"] == 200
        assert "Tags" in tag_resp


class TestQuickSightDataSources:
    def test_create_and_describe_data_source(self, quicksight):
        ds_id = f"ds-{uuid.uuid4().hex[:8]}"
        create_resp = quicksight.create_data_source(
            AwsAccountId=ACCOUNT_ID,
            DataSourceId=ds_id,
            Name="Test Data Source",
            Type="S3",
            DataSourceParameters={
                "S3Parameters": {
                    "ManifestFileLocation": {"Bucket": "my-bucket", "Key": "manifest.json"}
                }
            },
        )
        assert create_resp["Status"] in (200, 201, 202)
        assert create_resp["DataSourceId"] == ds_id
        assert "Arn" in create_resp

        try:
            describe_resp = quicksight.describe_data_source(
                AwsAccountId=ACCOUNT_ID, DataSourceId=ds_id
            )
            assert describe_resp["Status"] == 200
            assert describe_resp["DataSource"]["DataSourceId"] == ds_id
            assert describe_resp["DataSource"]["Name"] == "Test Data Source"
            assert describe_resp["DataSource"]["Type"] == "S3"
        finally:
            quicksight.delete_data_source(AwsAccountId=ACCOUNT_ID, DataSourceId=ds_id)

    def test_list_data_sources(self, quicksight):
        ds_id = f"ds-{uuid.uuid4().hex[:8]}"
        quicksight.create_data_source(
            AwsAccountId=ACCOUNT_ID,
            DataSourceId=ds_id,
            Name="Listed DS",
            Type="S3",
            DataSourceParameters={
                "S3Parameters": {"ManifestFileLocation": {"Bucket": "b", "Key": "k"}}
            },
        )
        try:
            list_resp = quicksight.list_data_sources(AwsAccountId=ACCOUNT_ID)
            assert list_resp["Status"] == 200
            assert isinstance(list_resp["DataSources"], list)
            ds_ids = [d["DataSourceId"] for d in list_resp["DataSources"]]
            assert ds_id in ds_ids
        finally:
            quicksight.delete_data_source(AwsAccountId=ACCOUNT_ID, DataSourceId=ds_id)

    def test_update_data_source(self, quicksight):
        ds_id = f"ds-{uuid.uuid4().hex[:8]}"
        quicksight.create_data_source(
            AwsAccountId=ACCOUNT_ID,
            DataSourceId=ds_id,
            Name="Original Name",
            Type="S3",
            DataSourceParameters={
                "S3Parameters": {"ManifestFileLocation": {"Bucket": "b", "Key": "k"}}
            },
        )
        try:
            update_resp = quicksight.update_data_source(
                AwsAccountId=ACCOUNT_ID,
                DataSourceId=ds_id,
                Name="Updated Name",
                DataSourceParameters={
                    "S3Parameters": {"ManifestFileLocation": {"Bucket": "b2", "Key": "k2"}}
                },
            )
            assert update_resp["Status"] == 200
            assert update_resp["DataSourceId"] == ds_id

            describe_resp = quicksight.describe_data_source(
                AwsAccountId=ACCOUNT_ID, DataSourceId=ds_id
            )
            assert describe_resp["DataSource"]["Name"] == "Updated Name"
        finally:
            quicksight.delete_data_source(AwsAccountId=ACCOUNT_ID, DataSourceId=ds_id)

    def test_delete_data_source(self, quicksight):
        ds_id = f"ds-{uuid.uuid4().hex[:8]}"
        quicksight.create_data_source(
            AwsAccountId=ACCOUNT_ID,
            DataSourceId=ds_id,
            Name="To Delete",
            Type="S3",
            DataSourceParameters={
                "S3Parameters": {"ManifestFileLocation": {"Bucket": "b", "Key": "k"}}
            },
        )
        delete_resp = quicksight.delete_data_source(AwsAccountId=ACCOUNT_ID, DataSourceId=ds_id)
        assert delete_resp["Status"] == 200
        assert delete_resp["DataSourceId"] == ds_id

    def test_delete_nonexistent_data_source_raises(self, quicksight):
        with pytest.raises(quicksight.exceptions.ClientError) as exc_info:
            quicksight.delete_data_source(AwsAccountId=ACCOUNT_ID, DataSourceId="nonexistent-ds")
        assert "ResourceNotFoundException" in str(exc_info.value)


class TestQuickSightDataSets:
    def test_create_data_set(self, quicksight):
        ds_id = f"ds-{uuid.uuid4().hex[:8]}"
        dset_id = f"dset-{uuid.uuid4().hex[:8]}"
        quicksight.create_data_source(
            AwsAccountId=ACCOUNT_ID,
            DataSourceId=ds_id,
            Name="DS for Dataset",
            Type="S3",
            DataSourceParameters={
                "S3Parameters": {"ManifestFileLocation": {"Bucket": "b", "Key": "k"}}
            },
        )
        try:
            create_resp = quicksight.create_data_set(
                AwsAccountId=ACCOUNT_ID,
                DataSetId=dset_id,
                Name="Test Dataset",
                PhysicalTableMap={
                    "t1": {
                        "S3Source": {
                            "DataSourceArn": (
                                f"arn:aws:quicksight:us-east-1:{ACCOUNT_ID}:datasource/{ds_id}"
                            ),
                            "InputColumns": [{"Name": "col1", "Type": "STRING"}],
                        }
                    }
                },
                ImportMode="SPICE",
            )
            assert create_resp["Status"] in (200, 201, 202)
            assert create_resp["DataSetId"] == dset_id
            assert "Arn" in create_resp
        finally:
            quicksight.delete_data_source(AwsAccountId=ACCOUNT_ID, DataSourceId=ds_id)


class TestQuickSightTags:
    def test_tag_and_untag_resource(self, quicksight):
        dash_id = f"test-dash-{uuid.uuid4().hex[:8]}"
        quicksight.create_dashboard(
            AwsAccountId=ACCOUNT_ID,
            DashboardId=dash_id,
            Name="Tag Test Dashboard",
            SourceEntity={
                "SourceTemplate": {
                    "Arn": f"arn:aws:quicksight:us-east-1:{ACCOUNT_ID}:template/fake-tmpl",
                    "DataSetReferences": [
                        {
                            "DataSetPlaceholder": "placeholder",
                            "DataSetArn": f"arn:aws:quicksight:us-east-1:{ACCOUNT_ID}:dataset/ds",
                        }
                    ],
                }
            },
        )
        arn = f"arn:aws:quicksight:us-east-1:{ACCOUNT_ID}:dashboard/{dash_id}"

        tag_resp = quicksight.tag_resource(
            ResourceArn=arn,
            Tags=[{"Key": "env", "Value": "test"}, {"Key": "team", "Value": "data"}],
        )
        assert tag_resp["Status"] == 200

        list_resp = quicksight.list_tags_for_resource(ResourceArn=arn)
        assert list_resp["Status"] == 200
        tags = {t["Key"]: t["Value"] for t in list_resp["Tags"]}
        assert tags["env"] == "test"
        assert tags["team"] == "data"

        untag_resp = quicksight.untag_resource(ResourceArn=arn, TagKeys=["team"])
        assert untag_resp["Status"] == 200

        list_resp2 = quicksight.list_tags_for_resource(ResourceArn=arn)
        tag_keys = [t["Key"] for t in list_resp2["Tags"]]
        assert "env" in tag_keys
        assert "team" not in tag_keys


class TestQuickSightIngestion:
    def test_create_ingestion(self, quicksight):
        ds_id = f"ds-{uuid.uuid4().hex[:8]}"
        dset_id = f"dset-{uuid.uuid4().hex[:8]}"
        ing_id = f"ing-{uuid.uuid4().hex[:8]}"
        quicksight.create_data_source(
            AwsAccountId=ACCOUNT_ID,
            DataSourceId=ds_id,
            Name="Ingestion DS",
            Type="S3",
            DataSourceParameters={
                "S3Parameters": {"ManifestFileLocation": {"Bucket": "b", "Key": "k"}}
            },
        )
        try:
            quicksight.create_data_set(
                AwsAccountId=ACCOUNT_ID,
                DataSetId=dset_id,
                Name="Ingestion Dataset",
                PhysicalTableMap={
                    "t1": {
                        "S3Source": {
                            "DataSourceArn": (
                                f"arn:aws:quicksight:us-east-1:{ACCOUNT_ID}:datasource/{ds_id}"
                            ),
                            "InputColumns": [{"Name": "col1", "Type": "STRING"}],
                        }
                    }
                },
                ImportMode="SPICE",
            )
            resp = quicksight.create_ingestion(
                AwsAccountId=ACCOUNT_ID,
                DataSetId=dset_id,
                IngestionId=ing_id,
            )
            assert resp["Status"] in (200, 201, 202)
            assert resp["IngestionId"] == ing_id
            assert "IngestionStatus" in resp
        finally:
            quicksight.delete_data_source(AwsAccountId=ACCOUNT_ID, DataSourceId=ds_id)


class TestQuickSightPublicSharingSettings:
    def test_update_public_sharing_settings(self, quicksight):
        response = quicksight.update_public_sharing_settings(
            AwsAccountId=ACCOUNT_ID,
            PublicSharingEnabled=False,
        )
        assert response["Status"] == 200


class TestQuickSightGroups:
    def test_list_groups_empty(self, quicksight):
        response = quicksight.list_groups(AwsAccountId=ACCOUNT_ID, Namespace=NAMESPACE)
        assert response["Status"] == 200
        assert isinstance(response["GroupList"], list)

    def test_create_and_describe_group(self, quicksight):
        group_name = f"test-group-{uuid.uuid4().hex[:8]}"
        create_resp = quicksight.create_group(
            AwsAccountId=ACCOUNT_ID, Namespace=NAMESPACE, GroupName=group_name
        )
        assert create_resp["Status"] == 200
        group = create_resp["Group"]
        assert group["GroupName"] == group_name
        assert "Arn" in group
        assert ACCOUNT_ID in group["Arn"]

        describe_resp = quicksight.describe_group(
            AwsAccountId=ACCOUNT_ID, Namespace=NAMESPACE, GroupName=group_name
        )
        assert describe_resp["Status"] == 200
        assert describe_resp["Group"]["GroupName"] == group_name

        # Cleanup
        quicksight.delete_group(AwsAccountId=ACCOUNT_ID, Namespace=NAMESPACE, GroupName=group_name)

    def test_create_group_appears_in_list(self, quicksight):
        group_name = f"test-group-{uuid.uuid4().hex[:8]}"
        quicksight.create_group(AwsAccountId=ACCOUNT_ID, Namespace=NAMESPACE, GroupName=group_name)

        response = quicksight.list_groups(AwsAccountId=ACCOUNT_ID, Namespace=NAMESPACE)
        group_names = [g["GroupName"] for g in response["GroupList"]]
        assert group_name in group_names

        # Cleanup
        quicksight.delete_group(AwsAccountId=ACCOUNT_ID, Namespace=NAMESPACE, GroupName=group_name)

    def test_delete_group(self, quicksight):
        group_name = f"test-group-{uuid.uuid4().hex[:8]}"
        quicksight.create_group(AwsAccountId=ACCOUNT_ID, Namespace=NAMESPACE, GroupName=group_name)

        delete_resp = quicksight.delete_group(
            AwsAccountId=ACCOUNT_ID, Namespace=NAMESPACE, GroupName=group_name
        )
        assert delete_resp["Status"] == 204

    def test_update_group(self, quicksight):
        group_name = f"test-group-{uuid.uuid4().hex[:8]}"
        quicksight.create_group(AwsAccountId=ACCOUNT_ID, Namespace=NAMESPACE, GroupName=group_name)
        try:
            update_resp = quicksight.update_group(
                AwsAccountId=ACCOUNT_ID,
                Namespace=NAMESPACE,
                GroupName=group_name,
                Description="Updated description",
            )
            assert update_resp["Status"] == 200
            assert update_resp["Group"]["GroupName"] == group_name
            assert update_resp["Group"]["Description"] == "Updated description"
        finally:
            quicksight.delete_group(
                AwsAccountId=ACCOUNT_ID, Namespace=NAMESPACE, GroupName=group_name
            )


class TestQuickSightGroupMemberships:
    def test_create_and_list_group_membership(self, quicksight):
        group_name = f"test-group-{uuid.uuid4().hex[:8]}"
        user_name = f"testuser-{uuid.uuid4().hex[:8]}"
        quicksight.create_group(AwsAccountId=ACCOUNT_ID, Namespace=NAMESPACE, GroupName=group_name)
        quicksight.register_user(
            AwsAccountId=ACCOUNT_ID,
            Namespace=NAMESPACE,
            Email=f"{user_name}@example.com",
            IdentityType="QUICKSIGHT",
            UserRole="READER",
            UserName=user_name,
        )
        try:
            membership_resp = quicksight.create_group_membership(
                AwsAccountId=ACCOUNT_ID,
                Namespace=NAMESPACE,
                GroupName=group_name,
                MemberName=user_name,
            )
            assert membership_resp["Status"] == 200
            assert "GroupMember" in membership_resp
            assert membership_resp["GroupMember"]["MemberName"] == user_name

            list_resp = quicksight.list_group_memberships(
                AwsAccountId=ACCOUNT_ID, Namespace=NAMESPACE, GroupName=group_name
            )
            assert list_resp["Status"] == 200
            member_names = [m["MemberName"] for m in list_resp["GroupMemberList"]]
            assert user_name in member_names
        finally:
            quicksight.delete_group(
                AwsAccountId=ACCOUNT_ID, Namespace=NAMESPACE, GroupName=group_name
            )
            quicksight.delete_user(AwsAccountId=ACCOUNT_ID, Namespace=NAMESPACE, UserName=user_name)

    def test_search_groups(self, quicksight):
        group_name = f"searchgrp-{uuid.uuid4().hex[:8]}"
        quicksight.create_group(AwsAccountId=ACCOUNT_ID, Namespace=NAMESPACE, GroupName=group_name)
        try:
            search_resp = quicksight.search_groups(
                AwsAccountId=ACCOUNT_ID,
                Namespace=NAMESPACE,
                Filters=[
                    {
                        "Operator": "StartsWith",
                        "Name": "GROUP_NAME",
                        "Value": "searchgrp-",
                    }
                ],
            )
            assert search_resp["Status"] == 200
            assert "GroupList" in search_resp
            found_names = [g["GroupName"] for g in search_resp["GroupList"]]
            assert group_name in found_names
        finally:
            quicksight.delete_group(
                AwsAccountId=ACCOUNT_ID, Namespace=NAMESPACE, GroupName=group_name
            )


class TestQuickSightUsers:
    def test_list_users_empty(self, quicksight):
        response = quicksight.list_users(AwsAccountId=ACCOUNT_ID, Namespace=NAMESPACE)
        assert response["Status"] == 200
        assert isinstance(response["UserList"], list)

    def test_register_and_describe_user(self, quicksight):
        user_name = f"testuser-{uuid.uuid4().hex[:8]}"
        register_resp = quicksight.register_user(
            AwsAccountId=ACCOUNT_ID,
            Namespace=NAMESPACE,
            Email=f"{user_name}@example.com",
            IdentityType="QUICKSIGHT",
            UserRole="READER",
            UserName=user_name,
        )
        assert register_resp["Status"] == 200
        user = register_resp["User"]
        assert user["UserName"] == user_name
        assert user["Email"] == f"{user_name}@example.com"
        assert user["Role"] == "READER"
        assert "Arn" in user

        describe_resp = quicksight.describe_user(
            AwsAccountId=ACCOUNT_ID, Namespace=NAMESPACE, UserName=user_name
        )
        assert describe_resp["Status"] == 200
        assert describe_resp["User"]["UserName"] == user_name

        # Cleanup
        quicksight.delete_user(AwsAccountId=ACCOUNT_ID, Namespace=NAMESPACE, UserName=user_name)

    def test_register_user_appears_in_list(self, quicksight):
        user_name = f"testuser-{uuid.uuid4().hex[:8]}"
        quicksight.register_user(
            AwsAccountId=ACCOUNT_ID,
            Namespace=NAMESPACE,
            Email=f"{user_name}@example.com",
            IdentityType="QUICKSIGHT",
            UserRole="READER",
            UserName=user_name,
        )

        response = quicksight.list_users(AwsAccountId=ACCOUNT_ID, Namespace=NAMESPACE)
        user_names = [u["UserName"] for u in response["UserList"]]
        assert user_name in user_names

        # Cleanup
        quicksight.delete_user(AwsAccountId=ACCOUNT_ID, Namespace=NAMESPACE, UserName=user_name)

    def test_delete_user(self, quicksight):
        user_name = f"testuser-{uuid.uuid4().hex[:8]}"
        quicksight.register_user(
            AwsAccountId=ACCOUNT_ID,
            Namespace=NAMESPACE,
            Email=f"{user_name}@example.com",
            IdentityType="QUICKSIGHT",
            UserRole="ADMIN",
            UserName=user_name,
        )

        delete_resp = quicksight.delete_user(
            AwsAccountId=ACCOUNT_ID, Namespace=NAMESPACE, UserName=user_name
        )
        assert delete_resp["Status"] == 204

    def test_update_user(self, quicksight):
        user_name = f"testuser-{uuid.uuid4().hex[:8]}"
        quicksight.register_user(
            AwsAccountId=ACCOUNT_ID,
            Namespace=NAMESPACE,
            Email=f"{user_name}@example.com",
            IdentityType="QUICKSIGHT",
            UserRole="READER",
            UserName=user_name,
        )
        try:
            update_resp = quicksight.update_user(
                AwsAccountId=ACCOUNT_ID,
                Namespace=NAMESPACE,
                UserName=user_name,
                Email=f"{user_name}-updated@example.com",
                Role="ADMIN",
            )
            assert update_resp["Status"] == 200
            assert update_resp["User"]["Role"] == "ADMIN"
            assert update_resp["User"]["Email"] == f"{user_name}-updated@example.com"
        finally:
            quicksight.delete_user(AwsAccountId=ACCOUNT_ID, Namespace=NAMESPACE, UserName=user_name)
