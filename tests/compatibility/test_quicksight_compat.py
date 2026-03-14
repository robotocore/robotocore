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


def _unique(prefix="qs"):
    return f"{prefix}-{uuid.uuid4().hex[:8]}"


class TestQuickSightListOperations:
    """Test List* operations that return empty lists or summaries."""

    def test_list_analyses(self, quicksight):
        response = quicksight.list_analyses(AwsAccountId=ACCOUNT_ID)
        assert response["Status"] == 200
        assert isinstance(response["AnalysisSummaryList"], list)

    def test_list_data_sets(self, quicksight):
        response = quicksight.list_data_sets(AwsAccountId=ACCOUNT_ID)
        assert response["Status"] == 200
        assert isinstance(response["DataSetSummaries"], list)

    def test_list_folders(self, quicksight):
        response = quicksight.list_folders(AwsAccountId=ACCOUNT_ID)
        assert response["Status"] == 200
        assert isinstance(response["FolderSummaryList"], list)

    def test_list_templates(self, quicksight):
        response = quicksight.list_templates(AwsAccountId=ACCOUNT_ID)
        assert response["Status"] == 200
        assert isinstance(response["TemplateSummaryList"], list)

    def test_list_themes(self, quicksight):
        response = quicksight.list_themes(AwsAccountId=ACCOUNT_ID)
        assert response["Status"] == 200
        assert isinstance(response["ThemeSummaryList"], list)

    def test_list_namespaces(self, quicksight):
        response = quicksight.list_namespaces(AwsAccountId=ACCOUNT_ID)
        assert response["Status"] == 200
        assert isinstance(response["Namespaces"], list)

    def test_list_topics(self, quicksight):
        response = quicksight.list_topics(AwsAccountId=ACCOUNT_ID)
        assert response["Status"] == 200
        assert isinstance(response["TopicsSummaries"], list)

    def test_list_vpc_connections(self, quicksight):
        response = quicksight.list_vpc_connections(AwsAccountId=ACCOUNT_ID)
        assert response["Status"] == 200
        assert isinstance(response["VPCConnectionSummaries"], list)


class TestQuickSightDescribeWithFakeIds:
    """Test Describe* operations with nonexistent resource IDs."""

    def test_describe_analysis_not_found(self, quicksight):
        with pytest.raises(quicksight.exceptions.ClientError) as exc_info:
            quicksight.describe_analysis(AwsAccountId=ACCOUNT_ID, AnalysisId="nonexistent-analysis")
        assert "ResourceNotFoundException" in str(exc_info.value)

    def test_describe_analysis_definition_not_found(self, quicksight):
        with pytest.raises(quicksight.exceptions.ClientError) as exc_info:
            quicksight.describe_analysis_definition(
                AwsAccountId=ACCOUNT_ID, AnalysisId="nonexistent-analysis"
            )
        assert "ResourceNotFoundException" in str(exc_info.value)

    def test_describe_analysis_permissions_not_found(self, quicksight):
        with pytest.raises(quicksight.exceptions.ClientError) as exc_info:
            quicksight.describe_analysis_permissions(
                AwsAccountId=ACCOUNT_ID, AnalysisId="nonexistent-analysis"
            )
        assert "ResourceNotFoundException" in str(exc_info.value)

    def test_describe_dashboard_definition_not_found(self, quicksight):
        with pytest.raises(quicksight.exceptions.ClientError) as exc_info:
            quicksight.describe_dashboard_definition(
                AwsAccountId=ACCOUNT_ID, DashboardId="nonexistent-dash"
            )
        assert "ResourceNotFoundException" in str(exc_info.value)

    def test_describe_dashboard_permissions_not_found(self, quicksight):
        with pytest.raises(quicksight.exceptions.ClientError) as exc_info:
            quicksight.describe_dashboard_permissions(
                AwsAccountId=ACCOUNT_ID, DashboardId="nonexistent-dash"
            )
        assert "ResourceNotFoundException" in str(exc_info.value)

    def test_describe_data_set_not_found(self, quicksight):
        with pytest.raises(quicksight.exceptions.ClientError) as exc_info:
            quicksight.describe_data_set(AwsAccountId=ACCOUNT_ID, DataSetId="nonexistent-dataset")
        assert "ResourceNotFoundException" in str(exc_info.value)

    def test_describe_data_set_permissions_not_found(self, quicksight):
        with pytest.raises(quicksight.exceptions.ClientError) as exc_info:
            quicksight.describe_data_set_permissions(
                AwsAccountId=ACCOUNT_ID, DataSetId="nonexistent-dataset"
            )
        assert "ResourceNotFoundException" in str(exc_info.value)

    def test_describe_data_source_permissions_not_found(self, quicksight):
        with pytest.raises(quicksight.exceptions.ClientError) as exc_info:
            quicksight.describe_data_source_permissions(
                AwsAccountId=ACCOUNT_ID, DataSourceId="nonexistent-ds"
            )
        assert "ResourceNotFoundException" in str(exc_info.value)

    def test_describe_folder_not_found(self, quicksight):
        with pytest.raises(quicksight.exceptions.ClientError) as exc_info:
            quicksight.describe_folder(AwsAccountId=ACCOUNT_ID, FolderId="nonexistent-folder")
        assert "ResourceNotFoundException" in str(exc_info.value)

    def test_describe_folder_permissions_not_found(self, quicksight):
        with pytest.raises(quicksight.exceptions.ClientError) as exc_info:
            quicksight.describe_folder_permissions(
                AwsAccountId=ACCOUNT_ID, FolderId="nonexistent-folder"
            )
        assert "ResourceNotFoundException" in str(exc_info.value)

    def test_describe_template_not_found(self, quicksight):
        with pytest.raises(quicksight.exceptions.ClientError) as exc_info:
            quicksight.describe_template(AwsAccountId=ACCOUNT_ID, TemplateId="nonexistent-template")
        assert "ResourceNotFoundException" in str(exc_info.value)

    def test_describe_template_definition_not_found(self, quicksight):
        with pytest.raises(quicksight.exceptions.ClientError) as exc_info:
            quicksight.describe_template_definition(
                AwsAccountId=ACCOUNT_ID, TemplateId="nonexistent-template"
            )
        assert "ResourceNotFoundException" in str(exc_info.value)

    def test_describe_template_permissions_not_found(self, quicksight):
        with pytest.raises(quicksight.exceptions.ClientError) as exc_info:
            quicksight.describe_template_permissions(
                AwsAccountId=ACCOUNT_ID, TemplateId="nonexistent-template"
            )
        assert "ResourceNotFoundException" in str(exc_info.value)

    def test_describe_theme_not_found(self, quicksight):
        with pytest.raises(quicksight.exceptions.ClientError) as exc_info:
            quicksight.describe_theme(AwsAccountId=ACCOUNT_ID, ThemeId="nonexistent-theme")
        assert "ResourceNotFoundException" in str(exc_info.value)

    def test_describe_theme_permissions_not_found(self, quicksight):
        with pytest.raises(quicksight.exceptions.ClientError) as exc_info:
            quicksight.describe_theme_permissions(
                AwsAccountId=ACCOUNT_ID, ThemeId="nonexistent-theme"
            )
        assert "ResourceNotFoundException" in str(exc_info.value)

    def test_describe_vpc_connection_not_found(self, quicksight):
        with pytest.raises(quicksight.exceptions.ClientError) as exc_info:
            quicksight.describe_vpc_connection(
                AwsAccountId=ACCOUNT_ID, VPCConnectionId="nonexistent-vpc"
            )
        assert "ResourceNotFoundException" in str(exc_info.value)


class TestQuickSightAccountDescribe:
    """Test account-level Describe operations."""

    def test_describe_account_subscription(self, quicksight):
        response = quicksight.describe_account_subscription(AwsAccountId=ACCOUNT_ID)
        assert response["Status"] == 200
        assert "AccountInfo" in response

    def test_describe_ip_restriction(self, quicksight):
        response = quicksight.describe_ip_restriction(AwsAccountId=ACCOUNT_ID)
        assert response["Status"] == 200
        assert "RequestId" in response

    def test_describe_namespace_not_found(self, quicksight):
        with pytest.raises(quicksight.exceptions.ClientError) as exc_info:
            quicksight.describe_namespace(AwsAccountId=ACCOUNT_ID, Namespace="nonexistent-ns")
        assert "ResourceNotFoundException" in str(exc_info.value)


class TestQuickSightTemplateOperations:
    """Test Template CRUD operations."""

    def test_create_and_describe_template(self, quicksight):
        tmpl_id = _unique("tmpl")
        create_resp = quicksight.create_template(
            AwsAccountId=ACCOUNT_ID,
            TemplateId=tmpl_id,
            Name="Test Template",
            SourceEntity={
                "SourceAnalysis": {
                    "Arn": f"arn:aws:quicksight:us-east-1:{ACCOUNT_ID}:analysis/fake",
                    "DataSetReferences": [
                        {
                            "DataSetPlaceholder": "ph",
                            "DataSetArn": f"arn:aws:quicksight:us-east-1:{ACCOUNT_ID}:dataset/ds",
                        }
                    ],
                }
            },
        )
        assert create_resp["Status"] in (200, 201, 202)
        assert "TemplateId" in create_resp

        describe_resp = quicksight.describe_template(AwsAccountId=ACCOUNT_ID, TemplateId=tmpl_id)
        assert describe_resp["Status"] == 200
        assert "Template" in describe_resp
        assert describe_resp["Template"]["TemplateId"] == tmpl_id

    def test_list_template_versions(self, quicksight):
        tmpl_id = _unique("tmpl")
        quicksight.create_template(
            AwsAccountId=ACCOUNT_ID,
            TemplateId=tmpl_id,
            Name="Version Template",
            SourceEntity={
                "SourceAnalysis": {
                    "Arn": f"arn:aws:quicksight:us-east-1:{ACCOUNT_ID}:analysis/fake",
                    "DataSetReferences": [
                        {
                            "DataSetPlaceholder": "ph",
                            "DataSetArn": f"arn:aws:quicksight:us-east-1:{ACCOUNT_ID}:dataset/ds",
                        }
                    ],
                }
            },
        )
        resp = quicksight.list_template_versions(AwsAccountId=ACCOUNT_ID, TemplateId=tmpl_id)
        assert resp["Status"] == 200
        assert isinstance(resp["TemplateVersionSummaryList"], list)

    def test_describe_template_permissions_on_existing(self, quicksight):
        tmpl_id = _unique("tmpl")
        quicksight.create_template(
            AwsAccountId=ACCOUNT_ID,
            TemplateId=tmpl_id,
            Name="Perms Template",
            SourceEntity={
                "SourceAnalysis": {
                    "Arn": f"arn:aws:quicksight:us-east-1:{ACCOUNT_ID}:analysis/fake",
                    "DataSetReferences": [
                        {
                            "DataSetPlaceholder": "ph",
                            "DataSetArn": f"arn:aws:quicksight:us-east-1:{ACCOUNT_ID}:dataset/ds",
                        }
                    ],
                }
            },
        )
        resp = quicksight.describe_template_permissions(AwsAccountId=ACCOUNT_ID, TemplateId=tmpl_id)
        assert resp["Status"] == 200
        assert "TemplateId" in resp

    def test_create_and_describe_template_alias(self, quicksight):
        tmpl_id = _unique("tmpl")
        quicksight.create_template(
            AwsAccountId=ACCOUNT_ID,
            TemplateId=tmpl_id,
            Name="Alias Template",
            SourceEntity={
                "SourceAnalysis": {
                    "Arn": f"arn:aws:quicksight:us-east-1:{ACCOUNT_ID}:analysis/fake",
                    "DataSetReferences": [
                        {
                            "DataSetPlaceholder": "ph",
                            "DataSetArn": f"arn:aws:quicksight:us-east-1:{ACCOUNT_ID}:dataset/ds",
                        }
                    ],
                }
            },
        )
        alias_resp = quicksight.create_template_alias(
            AwsAccountId=ACCOUNT_ID,
            TemplateId=tmpl_id,
            AliasName="my-alias",
            TemplateVersionNumber=1,
        )
        assert alias_resp["Status"] in (200, 201)
        assert "TemplateAlias" in alias_resp

        describe_resp = quicksight.describe_template_alias(
            AwsAccountId=ACCOUNT_ID, TemplateId=tmpl_id, AliasName="my-alias"
        )
        assert describe_resp["Status"] == 200
        assert "TemplateAlias" in describe_resp
        assert describe_resp["TemplateAlias"]["AliasName"] == "my-alias"

    def test_list_template_aliases(self, quicksight):
        tmpl_id = _unique("tmpl")
        quicksight.create_template(
            AwsAccountId=ACCOUNT_ID,
            TemplateId=tmpl_id,
            Name="Aliases Template",
            SourceEntity={
                "SourceAnalysis": {
                    "Arn": f"arn:aws:quicksight:us-east-1:{ACCOUNT_ID}:analysis/fake",
                    "DataSetReferences": [
                        {
                            "DataSetPlaceholder": "ph",
                            "DataSetArn": f"arn:aws:quicksight:us-east-1:{ACCOUNT_ID}:dataset/ds",
                        }
                    ],
                }
            },
        )
        resp = quicksight.list_template_aliases(AwsAccountId=ACCOUNT_ID, TemplateId=tmpl_id)
        assert resp["Status"] == 200
        assert isinstance(resp["TemplateAliasList"], list)


class TestQuickSightThemeOperations:
    """Test Theme CRUD and related operations."""

    def test_create_and_describe_theme(self, quicksight):
        theme_id = _unique("theme")
        create_resp = quicksight.create_theme(
            AwsAccountId=ACCOUNT_ID,
            ThemeId=theme_id,
            Name="Test Theme",
            BaseThemeId="CLASSIC",
            Configuration={
                "DataColorPalette": {
                    "Colors": ["#000000", "#FFFFFF"],
                }
            },
        )
        assert create_resp["Status"] in (200, 201, 202)
        assert "ThemeId" in create_resp

        describe_resp = quicksight.describe_theme(AwsAccountId=ACCOUNT_ID, ThemeId=theme_id)
        assert describe_resp["Status"] == 200
        assert "Theme" in describe_resp
        assert describe_resp["Theme"]["ThemeId"] == theme_id

    def test_describe_theme_permissions_on_existing(self, quicksight):
        theme_id = _unique("theme")
        quicksight.create_theme(
            AwsAccountId=ACCOUNT_ID,
            ThemeId=theme_id,
            Name="Perms Theme",
            BaseThemeId="CLASSIC",
            Configuration={"DataColorPalette": {"Colors": ["#000000"]}},
        )
        resp = quicksight.describe_theme_permissions(AwsAccountId=ACCOUNT_ID, ThemeId=theme_id)
        assert resp["Status"] == 200
        assert "ThemeId" in resp

    def test_list_theme_versions(self, quicksight):
        theme_id = _unique("theme")
        quicksight.create_theme(
            AwsAccountId=ACCOUNT_ID,
            ThemeId=theme_id,
            Name="Versions Theme",
            BaseThemeId="CLASSIC",
            Configuration={"DataColorPalette": {"Colors": ["#000000"]}},
        )
        resp = quicksight.list_theme_versions(AwsAccountId=ACCOUNT_ID, ThemeId=theme_id)
        assert resp["Status"] == 200
        assert isinstance(resp["ThemeVersionSummaryList"], list)

    def test_create_and_describe_theme_alias(self, quicksight):
        theme_id = _unique("theme")
        quicksight.create_theme(
            AwsAccountId=ACCOUNT_ID,
            ThemeId=theme_id,
            Name="Alias Theme",
            BaseThemeId="CLASSIC",
            Configuration={"DataColorPalette": {"Colors": ["#000000"]}},
        )
        alias_resp = quicksight.create_theme_alias(
            AwsAccountId=ACCOUNT_ID,
            ThemeId=theme_id,
            AliasName="my-theme-alias",
            ThemeVersionNumber=1,
        )
        assert alias_resp["Status"] in (200, 201)
        assert "ThemeAlias" in alias_resp

        describe_resp = quicksight.describe_theme_alias(
            AwsAccountId=ACCOUNT_ID, ThemeId=theme_id, AliasName="my-theme-alias"
        )
        assert describe_resp["Status"] == 200
        assert "ThemeAlias" in describe_resp
        assert describe_resp["ThemeAlias"]["AliasName"] == "my-theme-alias"

    def test_list_theme_aliases(self, quicksight):
        theme_id = _unique("theme")
        quicksight.create_theme(
            AwsAccountId=ACCOUNT_ID,
            ThemeId=theme_id,
            Name="Aliases Theme",
            BaseThemeId="CLASSIC",
            Configuration={"DataColorPalette": {"Colors": ["#000000"]}},
        )
        resp = quicksight.list_theme_aliases(AwsAccountId=ACCOUNT_ID, ThemeId=theme_id)
        assert resp["Status"] == 200
        assert isinstance(resp["ThemeAliasList"], list)


class TestQuickSightDashboardExtended:
    """Extended dashboard tests: permissions, definitions, versions."""

    def _create_dashboard(self, quicksight):
        dash_id = _unique("dash")
        quicksight.create_dashboard(
            AwsAccountId=ACCOUNT_ID,
            DashboardId=dash_id,
            Name="Test Dashboard",
            SourceEntity={
                "SourceTemplate": {
                    "Arn": f"arn:aws:quicksight:us-east-1:{ACCOUNT_ID}:template/fake-tmpl",
                    "DataSetReferences": [
                        {
                            "DataSetPlaceholder": "ph",
                            "DataSetArn": f"arn:aws:quicksight:us-east-1:{ACCOUNT_ID}:dataset/ds",
                        }
                    ],
                }
            },
        )
        return dash_id

    def test_describe_dashboard_permissions(self, quicksight):
        dash_id = self._create_dashboard(quicksight)
        resp = quicksight.describe_dashboard_permissions(
            AwsAccountId=ACCOUNT_ID, DashboardId=dash_id
        )
        assert resp["Status"] == 200
        assert "DashboardId" in resp

    def test_describe_dashboard_definition(self, quicksight):
        dash_id = self._create_dashboard(quicksight)
        resp = quicksight.describe_dashboard_definition(
            AwsAccountId=ACCOUNT_ID, DashboardId=dash_id
        )
        assert resp["Status"] == 200
        assert "DashboardId" in resp

    def test_list_dashboard_versions(self, quicksight):
        dash_id = self._create_dashboard(quicksight)
        resp = quicksight.list_dashboard_versions(AwsAccountId=ACCOUNT_ID, DashboardId=dash_id)
        assert resp["Status"] == 200
        assert isinstance(resp["DashboardVersionSummaryList"], list)


class TestQuickSightFolderOperations:
    """Test Folder CRUD and permissions."""

    def test_create_and_describe_folder(self, quicksight):
        folder_id = _unique("folder")
        create_resp = quicksight.create_folder(
            AwsAccountId=ACCOUNT_ID,
            FolderId=folder_id,
            Name="Test Folder",
            FolderType="SHARED",
        )
        assert create_resp["Status"] in (200, 201)
        assert "FolderId" in create_resp

        describe_resp = quicksight.describe_folder(AwsAccountId=ACCOUNT_ID, FolderId=folder_id)
        assert describe_resp["Status"] == 200
        assert "Folder" in describe_resp
        assert describe_resp["Folder"]["FolderId"] == folder_id

    def test_describe_folder_permissions(self, quicksight):
        folder_id = _unique("folder")
        quicksight.create_folder(
            AwsAccountId=ACCOUNT_ID,
            FolderId=folder_id,
            Name="Perms Folder",
            FolderType="SHARED",
        )
        resp = quicksight.describe_folder_permissions(AwsAccountId=ACCOUNT_ID, FolderId=folder_id)
        assert resp["Status"] == 200
        assert "FolderId" in resp

    def test_list_folder_members(self, quicksight):
        folder_id = _unique("folder")
        quicksight.create_folder(
            AwsAccountId=ACCOUNT_ID,
            FolderId=folder_id,
            Name="Members Folder",
            FolderType="SHARED",
        )
        resp = quicksight.list_folder_members(AwsAccountId=ACCOUNT_ID, FolderId=folder_id)
        assert resp["Status"] == 200
        assert isinstance(resp["FolderMemberList"], list)


class TestQuickSightGroupMembershipDescribe:
    """Test DescribeGroupMembership."""

    def test_describe_group_membership(self, quicksight):
        group_name = _unique("grp")
        user_name = _unique("user")
        quicksight.create_group(AwsAccountId=ACCOUNT_ID, Namespace=NAMESPACE, GroupName=group_name)
        quicksight.register_user(
            AwsAccountId=ACCOUNT_ID,
            Namespace=NAMESPACE,
            Email=f"{user_name}@example.com",
            IdentityType="QUICKSIGHT",
            UserRole="READER",
            UserName=user_name,
        )
        quicksight.create_group_membership(
            AwsAccountId=ACCOUNT_ID,
            Namespace=NAMESPACE,
            GroupName=group_name,
            MemberName=user_name,
        )
        try:
            resp = quicksight.describe_group_membership(
                AwsAccountId=ACCOUNT_ID,
                Namespace=NAMESPACE,
                GroupName=group_name,
                MemberName=user_name,
            )
            assert resp["Status"] == 200
            assert "GroupMember" in resp
            assert resp["GroupMember"]["MemberName"] == user_name
        finally:
            quicksight.delete_group(
                AwsAccountId=ACCOUNT_ID, Namespace=NAMESPACE, GroupName=group_name
            )
            quicksight.delete_user(AwsAccountId=ACCOUNT_ID, Namespace=NAMESPACE, UserName=user_name)


class TestQuickSightUserGroups:
    """Test ListUserGroups."""

    def test_list_user_groups(self, quicksight):
        group_name = _unique("grp")
        user_name = _unique("user")
        quicksight.create_group(AwsAccountId=ACCOUNT_ID, Namespace=NAMESPACE, GroupName=group_name)
        quicksight.register_user(
            AwsAccountId=ACCOUNT_ID,
            Namespace=NAMESPACE,
            Email=f"{user_name}@example.com",
            IdentityType="QUICKSIGHT",
            UserRole="READER",
            UserName=user_name,
        )
        quicksight.create_group_membership(
            AwsAccountId=ACCOUNT_ID,
            Namespace=NAMESPACE,
            GroupName=group_name,
            MemberName=user_name,
        )
        try:
            resp = quicksight.list_user_groups(
                AwsAccountId=ACCOUNT_ID,
                Namespace=NAMESPACE,
                UserName=user_name,
            )
            assert resp["Status"] == 200
            assert isinstance(resp["GroupList"], list)
            group_names = [g["GroupName"] for g in resp["GroupList"]]
            assert group_name in group_names
        finally:
            quicksight.delete_group(
                AwsAccountId=ACCOUNT_ID, Namespace=NAMESPACE, GroupName=group_name
            )
            quicksight.delete_user(AwsAccountId=ACCOUNT_ID, Namespace=NAMESPACE, UserName=user_name)


class TestQuickSightDataSetExtended:
    """Extended DataSet tests: describe, permissions."""

    def _create_data_set(self, quicksight):
        ds_id = _unique("ds")
        dset_id = _unique("dset")
        quicksight.create_data_source(
            AwsAccountId=ACCOUNT_ID,
            DataSourceId=ds_id,
            Name="DS for Dataset",
            Type="S3",
            DataSourceParameters={
                "S3Parameters": {"ManifestFileLocation": {"Bucket": "b", "Key": "k"}}
            },
        )
        quicksight.create_data_set(
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
        return ds_id, dset_id

    def test_describe_data_set(self, quicksight):
        ds_id, dset_id = self._create_data_set(quicksight)
        try:
            resp = quicksight.describe_data_set(AwsAccountId=ACCOUNT_ID, DataSetId=dset_id)
            assert resp["Status"] == 200
            assert "DataSet" in resp
            assert resp["DataSet"]["DataSetId"] == dset_id
        finally:
            quicksight.delete_data_source(AwsAccountId=ACCOUNT_ID, DataSourceId=ds_id)

    def test_describe_data_set_permissions(self, quicksight):
        ds_id, dset_id = self._create_data_set(quicksight)
        try:
            resp = quicksight.describe_data_set_permissions(
                AwsAccountId=ACCOUNT_ID, DataSetId=dset_id
            )
            assert resp["Status"] == 200
            assert "DataSetId" in resp
        finally:
            quicksight.delete_data_source(AwsAccountId=ACCOUNT_ID, DataSourceId=ds_id)

    def test_describe_data_source_permissions(self, quicksight):
        ds_id = _unique("ds")
        quicksight.create_data_source(
            AwsAccountId=ACCOUNT_ID,
            DataSourceId=ds_id,
            Name="Perms DS",
            Type="S3",
            DataSourceParameters={
                "S3Parameters": {"ManifestFileLocation": {"Bucket": "b", "Key": "k"}}
            },
        )
        try:
            resp = quicksight.describe_data_source_permissions(
                AwsAccountId=ACCOUNT_ID, DataSourceId=ds_id
            )
            assert resp["Status"] == 200
            assert "DataSourceId" in resp
        finally:
            quicksight.delete_data_source(AwsAccountId=ACCOUNT_ID, DataSourceId=ds_id)


class TestQuickSightIngestionDescribe:
    """Test DescribeIngestion and ListIngestions."""

    def _create_ingestion(self, quicksight):
        ds_id = _unique("ds")
        dset_id = _unique("dset")
        ing_id = _unique("ing")
        quicksight.create_data_source(
            AwsAccountId=ACCOUNT_ID,
            DataSourceId=ds_id,
            Name="Ingestion DS",
            Type="S3",
            DataSourceParameters={
                "S3Parameters": {"ManifestFileLocation": {"Bucket": "b", "Key": "k"}}
            },
        )
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
        quicksight.create_ingestion(AwsAccountId=ACCOUNT_ID, DataSetId=dset_id, IngestionId=ing_id)
        return ds_id, dset_id, ing_id

    def test_describe_ingestion(self, quicksight):
        ds_id, dset_id, ing_id = self._create_ingestion(quicksight)
        try:
            resp = quicksight.describe_ingestion(
                AwsAccountId=ACCOUNT_ID, DataSetId=dset_id, IngestionId=ing_id
            )
            assert resp["Status"] == 200
            assert "Ingestion" in resp
            assert resp["Ingestion"]["IngestionId"] == ing_id
        finally:
            quicksight.delete_data_source(AwsAccountId=ACCOUNT_ID, DataSourceId=ds_id)

    def test_list_ingestions(self, quicksight):
        ds_id, dset_id, ing_id = self._create_ingestion(quicksight)
        try:
            resp = quicksight.list_ingestions(AwsAccountId=ACCOUNT_ID, DataSetId=dset_id)
            assert resp["Status"] == 200
            assert isinstance(resp["Ingestions"], list)
            ing_ids = [i["IngestionId"] for i in resp["Ingestions"]]
            assert ing_id in ing_ids
        finally:
            quicksight.delete_data_source(AwsAccountId=ACCOUNT_ID, DataSourceId=ds_id)


class TestQuickSightIAMPolicyAssignment:
    """Test IAM policy assignment operations."""

    def test_list_iam_policy_assignments(self, quicksight):
        resp = quicksight.list_iam_policy_assignments(AwsAccountId=ACCOUNT_ID, Namespace=NAMESPACE)
        assert resp["Status"] == 200
        assert isinstance(resp["IAMPolicyAssignments"], list)

    def test_describe_iam_policy_assignment_not_found(self, quicksight):
        with pytest.raises(quicksight.exceptions.ClientError) as exc_info:
            quicksight.describe_iam_policy_assignment(
                AwsAccountId=ACCOUNT_ID,
                Namespace=NAMESPACE,
                AssignmentName="nonexistent-assignment",
            )
        err_str = str(exc_info.value)
        assert "ResourceNotFoundException" in err_str or "InvalidParameterValue" in err_str


class TestQuickSightDescribeMoreNotFound:
    """Additional Describe operations with nonexistent resources."""

    def test_describe_template_alias_not_found(self, quicksight):
        with pytest.raises(quicksight.exceptions.ClientError) as exc_info:
            quicksight.describe_template_alias(
                AwsAccountId=ACCOUNT_ID,
                TemplateId="nonexistent-tmpl",
                AliasName="nonexistent-alias",
            )
        assert "ResourceNotFoundException" in str(exc_info.value)

    def test_describe_theme_alias_not_found(self, quicksight):
        with pytest.raises(quicksight.exceptions.ClientError) as exc_info:
            quicksight.describe_theme_alias(
                AwsAccountId=ACCOUNT_ID,
                ThemeId="nonexistent-theme",
                AliasName="nonexistent-alias",
            )
        assert "ResourceNotFoundException" in str(exc_info.value)

    def test_describe_ingestion_not_found(self, quicksight):
        with pytest.raises(quicksight.exceptions.ClientError) as exc_info:
            quicksight.describe_ingestion(
                AwsAccountId=ACCOUNT_ID,
                DataSetId="nonexistent-dset",
                IngestionId="nonexistent-ing",
            )
        assert "ResourceNotFoundException" in str(exc_info.value)

    def test_describe_group_membership_not_found(self, quicksight):
        group_name = _unique("grp")
        quicksight.create_group(AwsAccountId=ACCOUNT_ID, Namespace=NAMESPACE, GroupName=group_name)
        try:
            with pytest.raises(quicksight.exceptions.ClientError) as exc_info:
                quicksight.describe_group_membership(
                    AwsAccountId=ACCOUNT_ID,
                    Namespace=NAMESPACE,
                    GroupName=group_name,
                    MemberName="nonexistent-user",
                )
            assert "ResourceNotFoundException" in str(exc_info.value)
        finally:
            quicksight.delete_group(
                AwsAccountId=ACCOUNT_ID, Namespace=NAMESPACE, GroupName=group_name
            )


class TestQuickSightMoreListOps:
    """Additional List operations."""

    def test_list_asset_bundle_export_jobs(self, quicksight):
        resp = quicksight.list_asset_bundle_export_jobs(AwsAccountId=ACCOUNT_ID)
        assert resp["Status"] == 200
        assert isinstance(resp["AssetBundleExportJobSummaryList"], list)

    def test_list_asset_bundle_import_jobs(self, quicksight):
        resp = quicksight.list_asset_bundle_import_jobs(AwsAccountId=ACCOUNT_ID)
        assert resp["Status"] == 200
        assert isinstance(resp["AssetBundleImportJobSummaryList"], list)

    def test_list_iam_policy_assignments_for_user(self, quicksight):
        user_name = _unique("user")
        quicksight.register_user(
            AwsAccountId=ACCOUNT_ID,
            Namespace=NAMESPACE,
            Email=f"{user_name}@example.com",
            IdentityType="QUICKSIGHT",
            UserRole="READER",
            UserName=user_name,
        )
        try:
            resp = quicksight.list_iam_policy_assignments_for_user(
                AwsAccountId=ACCOUNT_ID,
                Namespace=NAMESPACE,
                UserName=user_name,
            )
            assert resp["Status"] == 200
            assert isinstance(resp["ActiveAssignments"], list)
        finally:
            quicksight.delete_user(AwsAccountId=ACCOUNT_ID, Namespace=NAMESPACE, UserName=user_name)


class TestQuickSightAccountCustomization:
    """Test account customization operations."""

    def test_describe_account_customization_not_found(self, quicksight):
        with pytest.raises(quicksight.exceptions.ClientError) as exc_info:
            quicksight.describe_account_customization(AwsAccountId=ACCOUNT_ID)
        assert "ResourceNotFoundException" in str(exc_info.value)


class TestQuickSightTopicOperations:
    """Test Topic describe/list operations."""

    def test_describe_topic_not_found(self, quicksight):
        with pytest.raises(quicksight.exceptions.ClientError) as exc_info:
            quicksight.describe_topic(AwsAccountId=ACCOUNT_ID, TopicId="nonexistent-topic")
        err_str = str(exc_info.value)
        assert "ResourceNotFoundException" in err_str or "InvalidParameterValue" in err_str

    def test_describe_topic_permissions_not_found(self, quicksight):
        with pytest.raises(quicksight.exceptions.ClientError) as exc_info:
            quicksight.describe_topic_permissions(
                AwsAccountId=ACCOUNT_ID, TopicId="nonexistent-topic"
            )
        err_str = str(exc_info.value)
        assert "ResourceNotFoundException" in err_str or "InvalidParameterValue" in err_str


class TestQuickSightListOpsExtended:
    """Additional List operations returning empty lists."""

    def test_list_action_connectors(self, quicksight):
        resp = quicksight.list_action_connectors(AwsAccountId=ACCOUNT_ID)
        assert resp["Status"] == 200
        assert "RequestId" in resp

    def test_list_brands(self, quicksight):
        resp = quicksight.list_brands(AwsAccountId=ACCOUNT_ID)
        assert "Brands" in resp

    def test_list_custom_permissions(self, quicksight):
        resp = quicksight.list_custom_permissions(AwsAccountId=ACCOUNT_ID)
        assert resp["Status"] == 200
        assert isinstance(resp["CustomPermissionsList"], list)

    def test_list_flows(self, quicksight):
        resp = quicksight.list_flows(AwsAccountId=ACCOUNT_ID)
        assert resp["Status"] == 200
        assert "RequestId" in resp

    def test_list_identity_propagation_configs(self, quicksight):
        resp = quicksight.list_identity_propagation_configs(AwsAccountId=ACCOUNT_ID)
        assert resp["Status"] == 200
        assert isinstance(resp["Services"], list)

    def test_list_refresh_schedules(self, quicksight):
        resp = quicksight.list_refresh_schedules(AwsAccountId=ACCOUNT_ID, DataSetId="fake")
        assert resp["Status"] == 200
        assert isinstance(resp["RefreshSchedules"], list)

    def test_list_role_memberships(self, quicksight):
        resp = quicksight.list_role_memberships(
            AwsAccountId=ACCOUNT_ID, Namespace=NAMESPACE, Role="ADMIN"
        )
        assert resp["Status"] == 200
        assert isinstance(resp["MembersList"], list)

    def test_list_self_upgrades(self, quicksight):
        resp = quicksight.list_self_upgrades(AwsAccountId=ACCOUNT_ID, Namespace=NAMESPACE)
        assert resp["Status"] == 200
        assert "RequestId" in resp

    def test_list_topic_refresh_schedules_not_found(self, quicksight):
        with pytest.raises(quicksight.exceptions.ClientError) as exc_info:
            quicksight.list_topic_refresh_schedules(AwsAccountId=ACCOUNT_ID, TopicId="fake")
        assert "ResourceNotFoundException" in str(exc_info.value)

    def test_list_topic_reviewed_answers_not_found(self, quicksight):
        with pytest.raises(quicksight.exceptions.ClientError) as exc_info:
            quicksight.list_topic_reviewed_answers(AwsAccountId=ACCOUNT_ID, TopicId="fake")
        assert "ResourceNotFoundException" in str(exc_info.value)


class TestQuickSightDescribeOpsExtended:
    """Additional Describe operations with fake/nonexistent IDs."""

    def test_describe_account_custom_permission(self, quicksight):
        resp = quicksight.describe_account_custom_permission(AwsAccountId=ACCOUNT_ID)
        assert resp["Status"] == 200
        assert "RequestId" in resp

    def test_describe_action_connector(self, quicksight):
        resp = quicksight.describe_action_connector(
            AwsAccountId=ACCOUNT_ID, ActionConnectorId="fake"
        )
        assert resp["Status"] == 200
        assert "RequestId" in resp

    def test_describe_action_connector_permissions(self, quicksight):
        resp = quicksight.describe_action_connector_permissions(
            AwsAccountId=ACCOUNT_ID, ActionConnectorId="fake"
        )
        assert resp["Status"] == 200
        assert "RequestId" in resp

    def test_describe_asset_bundle_export_job(self, quicksight):
        resp = quicksight.describe_asset_bundle_export_job(
            AwsAccountId=ACCOUNT_ID, AssetBundleExportJobId="fake"
        )
        assert resp["Status"] == 200
        assert "JobStatus" in resp

    def test_describe_asset_bundle_import_job(self, quicksight):
        resp = quicksight.describe_asset_bundle_import_job(
            AwsAccountId=ACCOUNT_ID, AssetBundleImportJobId="fake"
        )
        assert resp["Status"] == 200
        assert "JobStatus" in resp

    def test_describe_brand(self, quicksight):
        resp = quicksight.describe_brand(AwsAccountId=ACCOUNT_ID, BrandId="fake")
        assert "RequestId" in resp

    def test_describe_brand_assignment(self, quicksight):
        resp = quicksight.describe_brand_assignment(AwsAccountId=ACCOUNT_ID)
        assert "RequestId" in resp

    def test_describe_brand_published_version(self, quicksight):
        resp = quicksight.describe_brand_published_version(AwsAccountId=ACCOUNT_ID, BrandId="fake")
        assert "RequestId" in resp

    def test_describe_custom_permissions(self, quicksight):
        resp = quicksight.describe_custom_permissions(
            AwsAccountId=ACCOUNT_ID, CustomPermissionsName="fake"
        )
        assert resp["Status"] == 200
        assert "RequestId" in resp

    def test_describe_dashboard_snapshot_job(self, quicksight):
        resp = quicksight.describe_dashboard_snapshot_job(
            AwsAccountId=ACCOUNT_ID, DashboardId="fake", SnapshotJobId="fake"
        )
        assert resp["Status"] == 200
        assert "JobStatus" in resp

    def test_describe_dashboards_qa_configuration(self, quicksight):
        resp = quicksight.describe_dashboards_qa_configuration(AwsAccountId=ACCOUNT_ID)
        assert resp["Status"] == 200
        assert "RequestId" in resp

    def test_describe_data_set_refresh_properties(self, quicksight):
        resp = quicksight.describe_data_set_refresh_properties(
            AwsAccountId=ACCOUNT_ID, DataSetId="fake"
        )
        assert resp["Status"] == 200
        assert "RequestId" in resp

    def test_describe_default_q_business_application(self, quicksight):
        resp = quicksight.describe_default_q_business_application(AwsAccountId=ACCOUNT_ID)
        assert resp["Status"] == 200
        assert "RequestId" in resp

    def test_describe_key_registration(self, quicksight):
        resp = quicksight.describe_key_registration(AwsAccountId=ACCOUNT_ID)
        assert resp["Status"] == 200
        assert isinstance(resp["KeyRegistration"], list)

    def test_describe_q_personalization_configuration(self, quicksight):
        resp = quicksight.describe_q_personalization_configuration(AwsAccountId=ACCOUNT_ID)
        assert resp["Status"] == 200
        assert "PersonalizationMode" in resp

    def test_describe_quick_sight_q_search_configuration(self, quicksight):
        resp = quicksight.describe_quick_sight_q_search_configuration(AwsAccountId=ACCOUNT_ID)
        assert resp["Status"] == 200
        assert "QSearchStatus" in resp

    def test_describe_refresh_schedule_not_found(self, quicksight):
        with pytest.raises(quicksight.exceptions.ClientError) as exc_info:
            quicksight.describe_refresh_schedule(
                AwsAccountId=ACCOUNT_ID, DataSetId="fake", ScheduleId="fake"
            )
        assert "ResourceNotFoundException" in str(exc_info.value)

    def test_describe_role_custom_permission(self, quicksight):
        resp = quicksight.describe_role_custom_permission(
            AwsAccountId=ACCOUNT_ID, Namespace=NAMESPACE, Role="ADMIN"
        )
        assert resp["Status"] == 200
        assert "RequestId" in resp

    def test_describe_self_upgrade_configuration(self, quicksight):
        resp = quicksight.describe_self_upgrade_configuration(
            AwsAccountId=ACCOUNT_ID, Namespace=NAMESPACE
        )
        assert resp["Status"] == 200
        assert "RequestId" in resp

    def test_describe_topic_refresh(self, quicksight):
        resp = quicksight.describe_topic_refresh(
            AwsAccountId=ACCOUNT_ID, TopicId="fake", RefreshId="fake"
        )
        assert resp["Status"] == 200
        assert "RequestId" in resp

    def test_describe_topic_refresh_schedule_not_found(self, quicksight):
        with pytest.raises(quicksight.exceptions.ClientError) as exc_info:
            quicksight.describe_topic_refresh_schedule(
                AwsAccountId=ACCOUNT_ID, TopicId="fake", DatasetId="fake"
            )
        assert "ResourceNotFoundException" in str(exc_info.value)


class TestQuickSightGetOps:
    """Test Get operations."""

    def test_get_dashboard_embed_url(self, quicksight):
        resp = quicksight.get_dashboard_embed_url(
            AwsAccountId=ACCOUNT_ID, DashboardId="fake", IdentityType="ANONYMOUS"
        )
        assert resp["Status"] == 200
        assert "EmbedUrl" in resp

    def test_get_session_embed_url(self, quicksight):
        resp = quicksight.get_session_embed_url(AwsAccountId=ACCOUNT_ID)
        assert resp["Status"] == 200
        assert "EmbedUrl" in resp

    def test_get_flow_metadata(self, quicksight):
        resp = quicksight.get_flow_metadata(AwsAccountId=ACCOUNT_ID, FlowId="fake")
        assert resp["Status"] == 200
        assert "RequestId" in resp

    def test_get_flow_permissions(self, quicksight):
        resp = quicksight.get_flow_permissions(AwsAccountId=ACCOUNT_ID, FlowId="fake")
        assert resp["Status"] == 200


class TestQuickSightAccountCustomizationCRUD:
    """Test Create/Update/Delete account customization."""

    def test_create_account_customization(self, quicksight):
        resp = quicksight.create_account_customization(
            AwsAccountId=ACCOUNT_ID,
            AccountCustomization={"DefaultTheme": "arn:aws:quicksight::aws:theme/CLASSIC"},
        )
        assert resp["Status"] == 200
        assert "AccountCustomization" in resp

    def test_update_account_customization_not_found(self, quicksight):
        # After delete, update should fail
        quicksight.delete_account_customization(AwsAccountId=ACCOUNT_ID)
        with pytest.raises(quicksight.exceptions.ClientError) as exc_info:
            quicksight.update_account_customization(
                AwsAccountId=ACCOUNT_ID,
                AccountCustomization={"DefaultTheme": "arn:aws:quicksight::aws:theme/MIDNIGHT"},
            )
        assert "ResourceNotFoundException" in str(exc_info.value)

    def test_delete_account_customization(self, quicksight):
        quicksight.create_account_customization(
            AwsAccountId=ACCOUNT_ID,
            AccountCustomization={"DefaultTheme": "arn:aws:quicksight::aws:theme/CLASSIC"},
        )
        resp = quicksight.delete_account_customization(AwsAccountId=ACCOUNT_ID)
        assert resp["Status"] == 200


class TestQuickSightAccountSubscriptionCRUD:
    """Test account subscription create/delete."""

    def test_create_account_subscription(self, quicksight):
        resp = quicksight.create_account_subscription(
            AwsAccountId=ACCOUNT_ID,
            AccountName="test-acct",
            AuthenticationMethod="IAM_AND_QUICKSIGHT",
            Edition="ENTERPRISE",
            NotificationEmail="test@example.com",
        )
        assert resp["Status"] == 200
        assert "SignupResponse" in resp

    def test_delete_account_subscription(self, quicksight):
        resp = quicksight.delete_account_subscription(AwsAccountId=ACCOUNT_ID)
        assert resp["Status"] == 200


class TestQuickSightAnalysisCRUD:
    """Test analysis create/update/delete/restore."""

    def test_create_analysis(self, quicksight):
        aid = _unique("analysis")
        resp = quicksight.create_analysis(
            AwsAccountId=ACCOUNT_ID,
            AnalysisId=aid,
            Name="Test Analysis",
            SourceEntity={
                "SourceTemplate": {
                    "Arn": f"arn:aws:quicksight:us-east-1:{ACCOUNT_ID}:template/fake-tmpl",
                    "DataSetReferences": [
                        {
                            "DataSetPlaceholder": "ph",
                            "DataSetArn": f"arn:aws:quicksight:us-east-1:{ACCOUNT_ID}:dataset/ds",
                        }
                    ],
                }
            },
        )
        assert resp["Status"] in (200, 201, 202)
        assert resp["AnalysisId"] == aid

    def test_update_analysis_not_found(self, quicksight):
        with pytest.raises(quicksight.exceptions.ClientError) as exc_info:
            quicksight.update_analysis(
                AwsAccountId=ACCOUNT_ID,
                AnalysisId="nonexistent",
                Name="X",
                SourceEntity={
                    "SourceTemplate": {
                        "Arn": f"arn:aws:quicksight:us-east-1:{ACCOUNT_ID}:template/t",
                        "DataSetReferences": [
                            {
                                "DataSetPlaceholder": "p",
                                "DataSetArn": (
                                    f"arn:aws:quicksight:us-east-1:{ACCOUNT_ID}:dataset/d"
                                ),
                            }
                        ],
                    }
                },
            )
        assert "ResourceNotFoundException" in str(exc_info.value)

    def test_update_analysis_permissions_not_found(self, quicksight):
        with pytest.raises(quicksight.exceptions.ClientError) as exc_info:
            quicksight.update_analysis_permissions(
                AwsAccountId=ACCOUNT_ID, AnalysisId="nonexistent"
            )
        assert "ResourceNotFoundException" in str(exc_info.value)

    def test_delete_analysis_not_found(self, quicksight):
        with pytest.raises(quicksight.exceptions.ClientError) as exc_info:
            quicksight.delete_analysis(AwsAccountId=ACCOUNT_ID, AnalysisId="nonexistent")
        assert "ResourceNotFoundException" in str(exc_info.value)

    def test_restore_analysis_not_found(self, quicksight):
        with pytest.raises(quicksight.exceptions.ClientError) as exc_info:
            quicksight.restore_analysis(AwsAccountId=ACCOUNT_ID, AnalysisId="nonexistent")
        assert "ResourceNotFoundException" in str(exc_info.value)

    def test_create_and_delete_analysis(self, quicksight):
        aid = _unique("analysis")
        quicksight.create_analysis(
            AwsAccountId=ACCOUNT_ID,
            AnalysisId=aid,
            Name="To Delete",
            SourceEntity={
                "SourceTemplate": {
                    "Arn": f"arn:aws:quicksight:us-east-1:{ACCOUNT_ID}:template/fake-tmpl",
                    "DataSetReferences": [
                        {
                            "DataSetPlaceholder": "ph",
                            "DataSetArn": f"arn:aws:quicksight:us-east-1:{ACCOUNT_ID}:dataset/ds",
                        }
                    ],
                }
            },
        )
        del_resp = quicksight.delete_analysis(AwsAccountId=ACCOUNT_ID, AnalysisId=aid)
        assert del_resp["Status"] == 200


class TestQuickSightBrandCRUD:
    """Test brand create/update/delete operations."""

    def test_create_brand(self, quicksight):
        bid = _unique("brand")
        resp = quicksight.create_brand(
            AwsAccountId=ACCOUNT_ID,
            BrandId=bid,
            BrandDefinition={"BrandName": "TestBrand"},
        )
        assert "RequestId" in resp

    def test_update_brand(self, quicksight):
        resp = quicksight.update_brand(
            AwsAccountId=ACCOUNT_ID,
            BrandId="fake",
            BrandDefinition={"BrandName": "Updated"},
        )
        assert "RequestId" in resp

    def test_delete_brand(self, quicksight):
        resp = quicksight.delete_brand(AwsAccountId=ACCOUNT_ID, BrandId="fake")
        assert "RequestId" in resp

    def test_delete_brand_assignment(self, quicksight):
        resp = quicksight.delete_brand_assignment(AwsAccountId=ACCOUNT_ID)
        assert "RequestId" in resp

    def test_update_brand_assignment(self, quicksight):
        resp = quicksight.update_brand_assignment(
            AwsAccountId=ACCOUNT_ID,
            BrandArn=f"arn:aws:quicksight:us-east-1:{ACCOUNT_ID}:brand/fake",
        )
        assert "RequestId" in resp

    def test_update_brand_published_version(self, quicksight):
        resp = quicksight.update_brand_published_version(
            AwsAccountId=ACCOUNT_ID, BrandId="fake", VersionId="1"
        )
        assert "RequestId" in resp


class TestQuickSightCustomPermissionsCRUD:
    """Test custom permissions create/update/delete."""

    def test_create_custom_permissions(self, quicksight):
        name = _unique("perms")
        resp = quicksight.create_custom_permissions(
            AwsAccountId=ACCOUNT_ID,
            CustomPermissionsName=name,
            Capabilities={"ExportToCsv": "DENY", "ExportToExcel": "DENY"},
        )
        assert resp["Status"] == 200

    def test_update_custom_permissions(self, quicksight):
        resp = quicksight.update_custom_permissions(
            AwsAccountId=ACCOUNT_ID,
            CustomPermissionsName="fake",
            Capabilities={"ExportToCsv": "DENY"},
        )
        assert resp["Status"] == 200

    def test_delete_custom_permissions(self, quicksight):
        resp = quicksight.delete_custom_permissions(
            AwsAccountId=ACCOUNT_ID, CustomPermissionsName="fake"
        )
        assert resp["Status"] == 200


class TestQuickSightDashboardMutations:
    """Test dashboard update/delete operations."""

    def test_update_dashboard_not_found(self, quicksight):
        with pytest.raises(quicksight.exceptions.ClientError) as exc_info:
            quicksight.update_dashboard(
                AwsAccountId=ACCOUNT_ID,
                DashboardId="nonexistent",
                Name="X",
                SourceEntity={
                    "SourceTemplate": {
                        "Arn": f"arn:aws:quicksight:us-east-1:{ACCOUNT_ID}:template/t",
                        "DataSetReferences": [
                            {
                                "DataSetPlaceholder": "p",
                                "DataSetArn": (
                                    f"arn:aws:quicksight:us-east-1:{ACCOUNT_ID}:dataset/d"
                                ),
                            }
                        ],
                    }
                },
            )
        assert "ResourceNotFoundException" in str(exc_info.value)

    def test_delete_dashboard_not_found(self, quicksight):
        with pytest.raises(quicksight.exceptions.ClientError) as exc_info:
            quicksight.delete_dashboard(AwsAccountId=ACCOUNT_ID, DashboardId="nonexistent")
        assert "ResourceNotFoundException" in str(exc_info.value)

    def test_update_dashboard_links_not_found(self, quicksight):
        with pytest.raises(quicksight.exceptions.ClientError) as exc_info:
            quicksight.update_dashboard_links(
                AwsAccountId=ACCOUNT_ID, DashboardId="nonexistent", LinkEntities=[]
            )
        assert "ResourceNotFoundException" in str(exc_info.value)

    def test_update_dashboard_permissions_not_found(self, quicksight):
        with pytest.raises(quicksight.exceptions.ClientError) as exc_info:
            quicksight.update_dashboard_permissions(
                AwsAccountId=ACCOUNT_ID, DashboardId="nonexistent"
            )
        assert "ResourceNotFoundException" in str(exc_info.value)

    def test_update_dashboard_published_version_not_found(self, quicksight):
        with pytest.raises(quicksight.exceptions.ClientError) as exc_info:
            quicksight.update_dashboard_published_version(
                AwsAccountId=ACCOUNT_ID, DashboardId="nonexistent", VersionNumber=1
            )
        assert "ResourceNotFoundException" in str(exc_info.value)

    def test_update_dashboards_qa_configuration(self, quicksight):
        resp = quicksight.update_dashboards_qa_configuration(
            AwsAccountId=ACCOUNT_ID, DashboardsQAStatus="ENABLED"
        )
        assert resp["Status"] == 200


class TestQuickSightDataSetMutations:
    """Test data set update/delete operations."""

    def test_update_data_set_not_found(self, quicksight):
        with pytest.raises(quicksight.exceptions.ClientError) as exc_info:
            quicksight.update_data_set(
                AwsAccountId=ACCOUNT_ID,
                DataSetId="nonexistent",
                Name="X",
                PhysicalTableMap={
                    "t1": {
                        "S3Source": {
                            "DataSourceArn": (
                                f"arn:aws:quicksight:us-east-1:{ACCOUNT_ID}:datasource/ds"
                            ),
                            "InputColumns": [{"Name": "col", "Type": "STRING"}],
                        }
                    }
                },
                ImportMode="SPICE",
            )
        assert "ResourceNotFoundException" in str(exc_info.value)

    def test_update_data_set_permissions_not_found(self, quicksight):
        with pytest.raises(quicksight.exceptions.ClientError) as exc_info:
            quicksight.update_data_set_permissions(AwsAccountId=ACCOUNT_ID, DataSetId="nonexistent")
        assert "ResourceNotFoundException" in str(exc_info.value)

    def test_delete_data_set_not_found(self, quicksight):
        with pytest.raises(quicksight.exceptions.ClientError) as exc_info:
            quicksight.delete_data_set(AwsAccountId=ACCOUNT_ID, DataSetId="nonexistent")
        assert "ResourceNotFoundException" in str(exc_info.value)

    def test_delete_data_set_refresh_properties(self, quicksight):
        resp = quicksight.delete_data_set_refresh_properties(
            AwsAccountId=ACCOUNT_ID, DataSetId="fake"
        )
        assert resp["Status"] == 200

    def test_put_data_set_refresh_properties(self, quicksight):
        resp = quicksight.put_data_set_refresh_properties(
            AwsAccountId=ACCOUNT_ID,
            DataSetId="fake",
            DataSetRefreshProperties={
                "RefreshConfiguration": {
                    "IncrementalRefresh": {
                        "LookbackWindow": {
                            "ColumnName": "col",
                            "Size": 1,
                            "SizeUnit": "DAY",
                        }
                    }
                }
            },
        )
        assert resp["Status"] == 200


class TestQuickSightDataSourceMutations:
    """Test data source permission updates."""

    def test_update_data_source_permissions_not_found(self, quicksight):
        with pytest.raises(quicksight.exceptions.ClientError) as exc_info:
            quicksight.update_data_source_permissions(
                AwsAccountId=ACCOUNT_ID, DataSourceId="nonexistent"
            )
        assert "ResourceNotFoundException" in str(exc_info.value)


class TestQuickSightFolderMutations:
    """Test folder update/delete and membership operations."""

    def test_create_folder_membership(self, quicksight):
        fid = _unique("folder")
        quicksight.create_folder(
            AwsAccountId=ACCOUNT_ID,
            FolderId=fid,
            Name="Test Folder",
            FolderType="SHARED",
        )
        resp = quicksight.create_folder_membership(
            AwsAccountId=ACCOUNT_ID,
            FolderId=fid,
            MemberId="fake-member",
            MemberType="DASHBOARD",
        )
        assert resp["Status"] == 200
        assert "FolderMember" in resp

    def test_update_folder_not_found(self, quicksight):
        with pytest.raises(quicksight.exceptions.ClientError) as exc_info:
            quicksight.update_folder(AwsAccountId=ACCOUNT_ID, FolderId="nonexistent", Name="X")
        assert "ResourceNotFoundException" in str(exc_info.value)

    def test_update_folder_permissions_not_found(self, quicksight):
        with pytest.raises(quicksight.exceptions.ClientError) as exc_info:
            quicksight.update_folder_permissions(AwsAccountId=ACCOUNT_ID, FolderId="nonexistent")
        assert "ResourceNotFoundException" in str(exc_info.value)

    def test_delete_folder_not_found(self, quicksight):
        with pytest.raises(quicksight.exceptions.ClientError) as exc_info:
            quicksight.delete_folder(AwsAccountId=ACCOUNT_ID, FolderId="nonexistent")
        assert "ResourceNotFoundException" in str(exc_info.value)

    def test_delete_folder_membership_not_found(self, quicksight):
        with pytest.raises(quicksight.exceptions.ClientError) as exc_info:
            quicksight.delete_folder_membership(
                AwsAccountId=ACCOUNT_ID,
                FolderId="nonexistent",
                MemberId="fake",
                MemberType="DASHBOARD",
            )
        assert "ResourceNotFoundException" in str(exc_info.value)


class TestQuickSightIAMPolicyAssignmentCRUD:
    """Test IAM policy assignment create/update/delete."""

    def test_create_iam_policy_assignment(self, quicksight):
        name = _unique("assign")
        resp = quicksight.create_iam_policy_assignment(
            AwsAccountId=ACCOUNT_ID,
            Namespace=NAMESPACE,
            AssignmentName=name,
            AssignmentStatus="ENABLED",
        )
        assert resp["Status"] == 200
        assert resp["AssignmentName"] == name

    def test_update_iam_policy_assignment_not_found(self, quicksight):
        with pytest.raises(quicksight.exceptions.ClientError) as exc_info:
            quicksight.update_iam_policy_assignment(
                AwsAccountId=ACCOUNT_ID,
                Namespace=NAMESPACE,
                AssignmentName="nonexistent",
                AssignmentStatus="ENABLED",
            )
        assert "ResourceNotFoundException" in str(exc_info.value)

    def test_delete_iam_policy_assignment_not_found(self, quicksight):
        with pytest.raises(quicksight.exceptions.ClientError) as exc_info:
            quicksight.delete_iam_policy_assignment(
                AwsAccountId=ACCOUNT_ID,
                Namespace=NAMESPACE,
                AssignmentName="nonexistent",
            )
        assert "ResourceNotFoundException" in str(exc_info.value)


class TestQuickSightTemplateMutations:
    """Test template update/delete operations."""

    def test_update_template_not_found(self, quicksight):
        with pytest.raises(quicksight.exceptions.ClientError) as exc_info:
            quicksight.update_template(
                AwsAccountId=ACCOUNT_ID,
                TemplateId="nonexistent",
                SourceEntity={
                    "SourceAnalysis": {
                        "Arn": f"arn:aws:quicksight:us-east-1:{ACCOUNT_ID}:analysis/fake",
                        "DataSetReferences": [
                            {
                                "DataSetPlaceholder": "p",
                                "DataSetArn": (
                                    f"arn:aws:quicksight:us-east-1:{ACCOUNT_ID}:dataset/d"
                                ),
                            }
                        ],
                    }
                },
            )
        assert "ResourceNotFoundException" in str(exc_info.value)

    def test_update_template_alias_not_found(self, quicksight):
        with pytest.raises(quicksight.exceptions.ClientError) as exc_info:
            quicksight.update_template_alias(
                AwsAccountId=ACCOUNT_ID,
                TemplateId="nonexistent",
                AliasName="fake",
                TemplateVersionNumber=1,
            )
        assert "ResourceNotFoundException" in str(exc_info.value)

    def test_update_template_permissions_not_found(self, quicksight):
        with pytest.raises(quicksight.exceptions.ClientError) as exc_info:
            quicksight.update_template_permissions(
                AwsAccountId=ACCOUNT_ID, TemplateId="nonexistent"
            )
        assert "ResourceNotFoundException" in str(exc_info.value)

    def test_delete_template_not_found(self, quicksight):
        with pytest.raises(quicksight.exceptions.ClientError) as exc_info:
            quicksight.delete_template(AwsAccountId=ACCOUNT_ID, TemplateId="nonexistent")
        assert "ResourceNotFoundException" in str(exc_info.value)

    def test_delete_template_alias_not_found(self, quicksight):
        with pytest.raises(quicksight.exceptions.ClientError) as exc_info:
            quicksight.delete_template_alias(
                AwsAccountId=ACCOUNT_ID,
                TemplateId="nonexistent",
                AliasName="fake",
            )
        assert "ResourceNotFoundException" in str(exc_info.value)


class TestQuickSightThemeMutations:
    """Test theme update/delete operations."""

    def test_update_theme_not_found(self, quicksight):
        with pytest.raises(quicksight.exceptions.ClientError) as exc_info:
            quicksight.update_theme(
                AwsAccountId=ACCOUNT_ID,
                ThemeId="nonexistent",
                BaseThemeId="CLASSIC",
            )
        assert "ResourceNotFoundException" in str(exc_info.value)

    def test_update_theme_alias_not_found(self, quicksight):
        with pytest.raises(quicksight.exceptions.ClientError) as exc_info:
            quicksight.update_theme_alias(
                AwsAccountId=ACCOUNT_ID,
                ThemeId="nonexistent",
                AliasName="fake",
                ThemeVersionNumber=1,
            )
        assert "ResourceNotFoundException" in str(exc_info.value)

    def test_update_theme_permissions_not_found(self, quicksight):
        with pytest.raises(quicksight.exceptions.ClientError) as exc_info:
            quicksight.update_theme_permissions(AwsAccountId=ACCOUNT_ID, ThemeId="nonexistent")
        assert "ResourceNotFoundException" in str(exc_info.value)

    def test_delete_theme_not_found(self, quicksight):
        with pytest.raises(quicksight.exceptions.ClientError) as exc_info:
            quicksight.delete_theme(AwsAccountId=ACCOUNT_ID, ThemeId="nonexistent")
        assert "ResourceNotFoundException" in str(exc_info.value)

    def test_delete_theme_alias_not_found(self, quicksight):
        with pytest.raises(quicksight.exceptions.ClientError) as exc_info:
            quicksight.delete_theme_alias(
                AwsAccountId=ACCOUNT_ID,
                ThemeId="nonexistent",
                AliasName="fake",
            )
        assert "ResourceNotFoundException" in str(exc_info.value)


class TestQuickSightTopicCRUD:
    """Test topic create/update/delete operations."""

    def test_create_topic(self, quicksight):
        tid = _unique("topic")
        resp = quicksight.create_topic(
            AwsAccountId=ACCOUNT_ID,
            TopicId=tid,
            Topic={
                "Name": "TestTopic",
                "DataSets": [
                    {
                        "DatasetArn": f"arn:aws:quicksight:us-east-1:{ACCOUNT_ID}:dataset/ds",
                        "DatasetName": "ds",
                    }
                ],
            },
        )
        assert resp["Status"] == 200
        assert resp["TopicId"] == tid

    def test_update_topic_not_found(self, quicksight):
        with pytest.raises(quicksight.exceptions.ClientError) as exc_info:
            quicksight.update_topic(
                AwsAccountId=ACCOUNT_ID,
                TopicId="nonexistent",
                Topic={
                    "Name": "X",
                    "DataSets": [
                        {
                            "DatasetArn": f"arn:aws:quicksight:us-east-1:{ACCOUNT_ID}:dataset/ds",
                            "DatasetName": "ds",
                        }
                    ],
                },
            )
        assert "ResourceNotFoundException" in str(exc_info.value)

    def test_update_topic_permissions_not_found(self, quicksight):
        with pytest.raises(quicksight.exceptions.ClientError) as exc_info:
            quicksight.update_topic_permissions(AwsAccountId=ACCOUNT_ID, TopicId="nonexistent")
        assert "ResourceNotFoundException" in str(exc_info.value)

    def test_batch_create_topic_reviewed_answer_not_found(self, quicksight):
        with pytest.raises(quicksight.exceptions.ClientError) as exc_info:
            quicksight.batch_create_topic_reviewed_answer(
                AwsAccountId=ACCOUNT_ID,
                TopicId="nonexistent",
                Answers=[
                    {
                        "AnswerId": "a1",
                        "DatasetArn": f"arn:aws:quicksight:us-east-1:{ACCOUNT_ID}:dataset/ds",
                        "Question": "test?",
                        "Mir": {},
                    }
                ],
            )
        assert "ResourceNotFoundException" in str(exc_info.value)

    def test_batch_delete_topic_reviewed_answer_not_found(self, quicksight):
        with pytest.raises(quicksight.exceptions.ClientError) as exc_info:
            quicksight.batch_delete_topic_reviewed_answer(
                AwsAccountId=ACCOUNT_ID, TopicId="nonexistent", AnswerIds=["a1"]
            )
        assert "ResourceNotFoundException" in str(exc_info.value)


class TestQuickSightTopicRefreshSchedule:
    """Test topic refresh schedule create/update/delete."""

    def test_create_topic_refresh_schedule_not_found(self, quicksight):
        with pytest.raises(quicksight.exceptions.ClientError) as exc_info:
            quicksight.create_topic_refresh_schedule(
                AwsAccountId=ACCOUNT_ID,
                TopicId="nonexistent",
                DatasetArn=f"arn:aws:quicksight:us-east-1:{ACCOUNT_ID}:dataset/ds",
                RefreshSchedule={"IsEnabled": True, "BasedOnSpiceSchedule": True},
            )
        assert "ResourceNotFoundException" in str(exc_info.value)

    def test_update_topic_refresh_schedule_not_found(self, quicksight):
        with pytest.raises(quicksight.exceptions.ClientError) as exc_info:
            quicksight.update_topic_refresh_schedule(
                AwsAccountId=ACCOUNT_ID,
                TopicId="nonexistent",
                DatasetId="fake",
                RefreshSchedule={"IsEnabled": True, "BasedOnSpiceSchedule": True},
            )
        assert "ResourceNotFoundException" in str(exc_info.value)

    def test_delete_topic_refresh_schedule_not_found(self, quicksight):
        with pytest.raises(quicksight.exceptions.ClientError) as exc_info:
            quicksight.delete_topic_refresh_schedule(
                AwsAccountId=ACCOUNT_ID, TopicId="nonexistent", DatasetId="fake"
            )
        assert "ResourceNotFoundException" in str(exc_info.value)


class TestQuickSightRefreshScheduleCRUD:
    """Test data set refresh schedule create/update/delete."""

    def test_create_refresh_schedule(self, quicksight):
        resp = quicksight.create_refresh_schedule(
            AwsAccountId=ACCOUNT_ID,
            DataSetId="fake-ds",
            Schedule={
                "ScheduleId": _unique("sched"),
                "ScheduleFrequency": {"Interval": "DAILY"},
                "RefreshType": "FULL_REFRESH",
            },
        )
        assert resp["Status"] == 200
        assert "ScheduleId" in resp

    def test_update_refresh_schedule_not_found(self, quicksight):
        with pytest.raises(quicksight.exceptions.ClientError) as exc_info:
            quicksight.update_refresh_schedule(
                AwsAccountId=ACCOUNT_ID,
                DataSetId="nonexistent",
                Schedule={
                    "ScheduleId": "nonexistent",
                    "ScheduleFrequency": {"Interval": "DAILY"},
                    "RefreshType": "FULL_REFRESH",
                },
            )
        assert "ResourceNotFoundException" in str(exc_info.value)

    def test_delete_refresh_schedule(self, quicksight):
        resp = quicksight.delete_refresh_schedule(
            AwsAccountId=ACCOUNT_ID, DataSetId="fake", ScheduleId="fake"
        )
        assert resp["Status"] == 200


class TestQuickSightRoleMembership:
    """Test role membership create/delete operations."""

    def test_create_role_membership(self, quicksight):
        resp = quicksight.create_role_membership(
            AwsAccountId=ACCOUNT_ID,
            Namespace=NAMESPACE,
            Role="ADMIN",
            MemberName="fake-group",
        )
        assert resp["Status"] == 200

    def test_delete_role_membership(self, quicksight):
        resp = quicksight.delete_role_membership(
            AwsAccountId=ACCOUNT_ID,
            Namespace=NAMESPACE,
            Role="ADMIN",
            MemberName="fake",
        )
        assert resp["Status"] == 200

    def test_delete_role_custom_permission(self, quicksight):
        resp = quicksight.delete_role_custom_permission(
            AwsAccountId=ACCOUNT_ID, Namespace=NAMESPACE, Role="ADMIN"
        )
        assert resp["Status"] == 200

    def test_update_role_custom_permission(self, quicksight):
        resp = quicksight.update_role_custom_permission(
            AwsAccountId=ACCOUNT_ID,
            Namespace=NAMESPACE,
            Role="ADMIN",
            CustomPermissionsName="fake",
        )
        assert resp["Status"] == 200


class TestQuickSightGroupMembershipMutations:
    """Test group membership delete."""

    def test_delete_group_membership_not_found(self, quicksight):
        with pytest.raises(quicksight.exceptions.ClientError) as exc_info:
            quicksight.delete_group_membership(
                AwsAccountId=ACCOUNT_ID,
                Namespace=NAMESPACE,
                GroupName="nonexistent",
                MemberName="fake",
            )
        assert "ResourceNotFoundException" in str(exc_info.value)


class TestQuickSightNamespaceMutations:
    """Test namespace delete."""

    def test_delete_namespace_not_found(self, quicksight):
        with pytest.raises(quicksight.exceptions.ClientError) as exc_info:
            quicksight.delete_namespace(AwsAccountId=ACCOUNT_ID, Namespace="nonexistent-ns")
        assert "ResourceNotFoundException" in str(exc_info.value)


class TestQuickSightVPCConnectionMutations:
    """Test VPC connection delete."""

    def test_delete_vpc_connection_not_found(self, quicksight):
        with pytest.raises(quicksight.exceptions.ClientError) as exc_info:
            quicksight.delete_vpc_connection(AwsAccountId=ACCOUNT_ID, VPCConnectionId="nonexistent")
        assert "ResourceNotFoundException" in str(exc_info.value)


class TestQuickSightIngestionMutations:
    """Test ingestion cancel."""

    def test_cancel_ingestion_not_found(self, quicksight):
        with pytest.raises(quicksight.exceptions.ClientError) as exc_info:
            quicksight.cancel_ingestion(
                AwsAccountId=ACCOUNT_ID, DataSetId="fake", IngestionId="fake"
            )
        assert "ResourceNotFoundException" in str(exc_info.value)


class TestQuickSightUserMutations:
    """Test user delete/update operations."""

    def test_delete_user_by_principal_id(self, quicksight):
        resp = quicksight.delete_user_by_principal_id(
            AwsAccountId=ACCOUNT_ID, Namespace=NAMESPACE, PrincipalId="fake"
        )
        assert resp["Status"] == 200

    def test_delete_user_custom_permission_not_found(self, quicksight):
        with pytest.raises(quicksight.exceptions.ClientError) as exc_info:
            quicksight.delete_user_custom_permission(
                AwsAccountId=ACCOUNT_ID, Namespace=NAMESPACE, UserName="nonexistent"
            )
        assert "ResourceNotFoundException" in str(exc_info.value)

    def test_update_user_custom_permission_not_found(self, quicksight):
        with pytest.raises(quicksight.exceptions.ClientError) as exc_info:
            quicksight.update_user_custom_permission(
                AwsAccountId=ACCOUNT_ID,
                Namespace=NAMESPACE,
                UserName="nonexistent",
                CustomPermissionsName="perm",
            )
        assert "ResourceNotFoundException" in str(exc_info.value)


class TestQuickSightEmbedUrls:
    """Test embed URL generation operations."""

    def test_generate_embed_url_for_anonymous_user(self, quicksight):
        resp = quicksight.generate_embed_url_for_anonymous_user(
            AwsAccountId=ACCOUNT_ID,
            Namespace=NAMESPACE,
            AuthorizedResourceArns=[f"arn:aws:quicksight:us-east-1:{ACCOUNT_ID}:dashboard/fake"],
            ExperienceConfiguration={"Dashboard": {"InitialDashboardId": "fake"}},
        )
        assert resp["Status"] == 200
        assert "EmbedUrl" in resp

    def test_generate_embed_url_for_registered_user(self, quicksight):
        resp = quicksight.generate_embed_url_for_registered_user(
            AwsAccountId=ACCOUNT_ID,
            UserArn=f"arn:aws:quicksight:us-east-1:{ACCOUNT_ID}:user/default/fake",
            ExperienceConfiguration={"Dashboard": {"InitialDashboardId": "fake"}},
        )
        assert resp["Status"] == 200
        assert "EmbedUrl" in resp

    def test_generate_embed_url_for_registered_user_with_identity(self, quicksight):
        resp = quicksight.generate_embed_url_for_registered_user_with_identity(
            AwsAccountId=ACCOUNT_ID,
            ExperienceConfiguration={"Dashboard": {"InitialDashboardId": "fake"}},
        )
        assert resp["Status"] == 200
        assert "EmbedUrl" in resp


class TestQuickSightSearchOps:
    """Test search operations."""

    def test_search_folders(self, quicksight):
        resp = quicksight.search_folders(
            AwsAccountId=ACCOUNT_ID,
            Filters=[
                {
                    "Operator": "StringEquals",
                    "Name": "PARENT_FOLDER_ARN",
                    "Value": f"arn:aws:quicksight:us-east-1:{ACCOUNT_ID}:folder/root",
                }
            ],
        )
        assert resp["Status"] == 200
        assert isinstance(resp["FolderSummaryList"], list)

    def test_search_action_connectors(self, quicksight):
        resp = quicksight.search_action_connectors(
            AwsAccountId=ACCOUNT_ID,
            Filters=[
                {
                    "Operator": "StringEquals",
                    "Name": "DISPLAY_NAME",
                    "Value": "test",
                }
            ],
        )
        assert resp["Status"] == 200

    def test_search_flows(self, quicksight):
        resp = quicksight.search_flows(
            AwsAccountId=ACCOUNT_ID,
            Filters=[
                {
                    "Operator": "StringEquals",
                    "Name": "DISPLAY_NAME",
                    "Value": "test",
                }
            ],
        )
        assert resp["Status"] == 200


class TestQuickSightMiscMutations:
    """Test miscellaneous update/delete operations."""

    def test_delete_action_connector(self, quicksight):
        resp = quicksight.delete_action_connector(AwsAccountId=ACCOUNT_ID, ActionConnectorId="fake")
        assert resp["Status"] == 200

    def test_delete_default_q_business_application(self, quicksight):
        resp = quicksight.delete_default_q_business_application(AwsAccountId=ACCOUNT_ID)
        assert resp["Status"] == 200

    def test_delete_identity_propagation_config(self, quicksight):
        resp = quicksight.delete_identity_propagation_config(
            AwsAccountId=ACCOUNT_ID, Service="REDSHIFT"
        )
        assert resp["Status"] == 200

    def test_update_action_connector_permissions(self, quicksight):
        resp = quicksight.update_action_connector_permissions(
            AwsAccountId=ACCOUNT_ID, ActionConnectorId="fake"
        )
        assert resp["Status"] == 200

    def test_update_application_with_token_exchange_grant(self, quicksight):
        resp = quicksight.update_application_with_token_exchange_grant(
            AwsAccountId=ACCOUNT_ID, Namespace=NAMESPACE
        )
        assert resp["Status"] == 200

    def test_update_default_q_business_application(self, quicksight):
        resp = quicksight.update_default_q_business_application(
            AwsAccountId=ACCOUNT_ID, ApplicationId="fake-app"
        )
        assert resp["Status"] == 200

    def test_update_flow_permissions(self, quicksight):
        resp = quicksight.update_flow_permissions(AwsAccountId=ACCOUNT_ID, FlowId="fake")
        assert resp["Status"] == 200

    def test_update_identity_propagation_config(self, quicksight):
        resp = quicksight.update_identity_propagation_config(
            AwsAccountId=ACCOUNT_ID, Service="REDSHIFT"
        )
        assert resp["Status"] == 200

    def test_update_ip_restriction(self, quicksight):
        resp = quicksight.update_ip_restriction(AwsAccountId=ACCOUNT_ID)
        assert resp["Status"] == 200

    def test_update_key_registration(self, quicksight):
        resp = quicksight.update_key_registration(AwsAccountId=ACCOUNT_ID, KeyRegistration=[])
        assert "RequestId" in resp

    def test_update_q_personalization_configuration(self, quicksight):
        resp = quicksight.update_q_personalization_configuration(
            AwsAccountId=ACCOUNT_ID, PersonalizationMode="ENABLED"
        )
        assert resp["Status"] == 200

    def test_update_quick_sight_q_search_configuration(self, quicksight):
        resp = quicksight.update_quick_sight_q_search_configuration(
            AwsAccountId=ACCOUNT_ID, QSearchStatus="ENABLED"
        )
        assert resp["Status"] == 200

    def test_update_spice_capacity_configuration(self, quicksight):
        resp = quicksight.update_spice_capacity_configuration(
            AwsAccountId=ACCOUNT_ID, PurchaseMode="MANUAL"
        )
        assert resp["Status"] == 200

    def test_predict_qa_results(self, quicksight):
        resp = quicksight.predict_qa_results(AwsAccountId=ACCOUNT_ID, QueryText="test query")
        assert resp["Status"] == 200

    def test_start_asset_bundle_import_job(self, quicksight):
        jid = _unique("job")
        resp = quicksight.start_asset_bundle_import_job(
            AwsAccountId=ACCOUNT_ID,
            AssetBundleImportJobId=jid,
            AssetBundleImportSource={"Body": b"fake"},
        )
        assert resp["Status"] == 200
        assert "AssetBundleImportJobId" in resp

    def test_start_dashboard_snapshot_job_schedule(self, quicksight):
        resp = quicksight.start_dashboard_snapshot_job_schedule(
            AwsAccountId=ACCOUNT_ID, DashboardId="fake", ScheduleId="fake"
        )
        assert resp["Status"] == 200

    def test_update_self_upgrade(self, quicksight):
        resp = quicksight.update_self_upgrade(
            AwsAccountId=ACCOUNT_ID,
            Namespace=NAMESPACE,
            UpgradeRequestId="fake",
            Action="APPROVE",
        )
        assert resp["Status"] == 200

    def test_update_self_upgrade_configuration(self, quicksight):
        resp = quicksight.update_self_upgrade_configuration(
            AwsAccountId=ACCOUNT_ID, Namespace=NAMESPACE, SelfUpgradeStatus="OPT_IN"
        )
        assert resp["Status"] == 200


class TestQuickSightAccountCustomPermissionMutations:
    """Test account custom permission delete and update operations."""

    def test_delete_account_custom_permission(self, quicksight):
        resp = quicksight.delete_account_custom_permission(AwsAccountId=ACCOUNT_ID)
        assert resp["Status"] == 200
        assert "RequestId" in resp

    def test_update_account_custom_permission(self, quicksight):
        name = _unique("custperm")
        resp = quicksight.update_account_custom_permission(
            AwsAccountId=ACCOUNT_ID,
            CustomPermissionsName=name,
        )
        assert resp["Status"] == 200
        assert "RequestId" in resp


class TestQuickSightSearchOpsExpanded:
    """Test search operations for analyses, dashboards, datasets, datasources, topics."""

    def test_search_analyses(self, quicksight):
        resp = quicksight.search_analyses(
            AwsAccountId=ACCOUNT_ID,
            Filters=[
                {
                    "Operator": "StringEquals",
                    "Name": "QUICKSIGHT_USER",
                    "Value": "test",
                }
            ],
        )
        assert resp["Status"] == 200
        assert isinstance(resp["AnalysisSummaryList"], list)

    def test_search_dashboards(self, quicksight):
        resp = quicksight.search_dashboards(
            AwsAccountId=ACCOUNT_ID,
            Filters=[
                {
                    "Operator": "StringEquals",
                    "Name": "QUICKSIGHT_USER",
                    "Value": "test",
                }
            ],
        )
        assert resp["Status"] == 200
        assert isinstance(resp["DashboardSummaryList"], list)

    def test_search_data_sets(self, quicksight):
        resp = quicksight.search_data_sets(
            AwsAccountId=ACCOUNT_ID,
            Filters=[
                {
                    "Operator": "StringEquals",
                    "Name": "QUICKSIGHT_USER",
                    "Value": "test",
                }
            ],
        )
        assert resp["Status"] == 200
        assert isinstance(resp["DataSetSummaries"], list)

    def test_search_data_sources(self, quicksight):
        resp = quicksight.search_data_sources(
            AwsAccountId=ACCOUNT_ID,
            Filters=[
                {
                    "Operator": "StringEquals",
                    "Name": "QUICKSIGHT_USER",
                    "Value": "test",
                }
            ],
        )
        assert resp["Status"] == 200
        assert isinstance(resp["DataSourceSummaries"], list)

    def test_search_topics(self, quicksight):
        resp = quicksight.search_topics(
            AwsAccountId=ACCOUNT_ID,
            Filters=[
                {
                    "Operator": "StringEquals",
                    "Name": "QUICKSIGHT_USER",
                    "Value": "test",
                }
            ],
        )
        assert resp["Status"] == 200
        assert "RequestId" in resp


class TestQuickSightVPCConnectionCRUD:
    """Test VPC connection create/update/describe/delete lifecycle."""

    def test_create_vpc_connection(self, quicksight):
        vpc_id = _unique("vpc")
        resp = quicksight.create_vpc_connection(
            AwsAccountId=ACCOUNT_ID,
            VPCConnectionId=vpc_id,
            Name="Test VPC Connection",
            SubnetIds=["subnet-111", "subnet-222"],
            SecurityGroupIds=["sg-111"],
            RoleArn=f"arn:aws:iam::{ACCOUNT_ID}:role/test",
        )
        assert resp["Status"] in (200, 201, 202)
        assert "VPCConnectionId" in resp

    def test_update_vpc_connection(self, quicksight):
        vpc_id = _unique("vpc")
        quicksight.create_vpc_connection(
            AwsAccountId=ACCOUNT_ID,
            VPCConnectionId=vpc_id,
            Name="Original",
            SubnetIds=["subnet-111", "subnet-222"],
            SecurityGroupIds=["sg-111"],
            RoleArn=f"arn:aws:iam::{ACCOUNT_ID}:role/test",
        )
        resp = quicksight.update_vpc_connection(
            AwsAccountId=ACCOUNT_ID,
            VPCConnectionId=vpc_id,
            Name="Updated",
            SubnetIds=["subnet-111", "subnet-222"],
            SecurityGroupIds=["sg-111"],
            RoleArn=f"arn:aws:iam::{ACCOUNT_ID}:role/test",
        )
        assert resp["Status"] == 200
        assert "VPCConnectionId" in resp

    def test_create_and_describe_vpc_connection(self, quicksight):
        vpc_id = _unique("vpc")
        quicksight.create_vpc_connection(
            AwsAccountId=ACCOUNT_ID,
            VPCConnectionId=vpc_id,
            Name="Describe Test",
            SubnetIds=["subnet-111", "subnet-222"],
            SecurityGroupIds=["sg-111"],
            RoleArn=f"arn:aws:iam::{ACCOUNT_ID}:role/test",
        )
        resp = quicksight.describe_vpc_connection(AwsAccountId=ACCOUNT_ID, VPCConnectionId=vpc_id)
        assert resp["Status"] == 200
        assert "VPCConnection" in resp
        assert resp["VPCConnection"]["VPCConnectionId"] == vpc_id


class TestQuickSightAssetBundleExportJob:
    """Test asset bundle export job start/describe."""

    def test_start_asset_bundle_export_job(self, quicksight):
        job_id = _unique("export")
        resp = quicksight.start_asset_bundle_export_job(
            AwsAccountId=ACCOUNT_ID,
            AssetBundleExportJobId=job_id,
            ResourceArns=[f"arn:aws:quicksight:us-east-1:{ACCOUNT_ID}:dashboard/test-dash"],
            ExportFormat="CLOUDFORMATION_JSON",
        )
        assert resp["Status"] == 200
        assert "AssetBundleExportJobId" in resp

    def test_start_and_describe_asset_bundle_export_job(self, quicksight):
        job_id = _unique("export")
        quicksight.start_asset_bundle_export_job(
            AwsAccountId=ACCOUNT_ID,
            AssetBundleExportJobId=job_id,
            ResourceArns=[f"arn:aws:quicksight:us-east-1:{ACCOUNT_ID}:dashboard/test-dash"],
            ExportFormat="CLOUDFORMATION_JSON",
        )
        resp = quicksight.describe_asset_bundle_export_job(
            AwsAccountId=ACCOUNT_ID, AssetBundleExportJobId=job_id
        )
        assert resp["Status"] == 200
        assert "JobStatus" in resp


class TestQuickSightDashboardSnapshotJob:
    """Test dashboard snapshot job start/describe."""

    def test_start_dashboard_snapshot_job(self, quicksight):
        dash_id = _unique("dash")
        snap_id = _unique("snap")
        quicksight.create_dashboard(
            AwsAccountId=ACCOUNT_ID,
            DashboardId=dash_id,
            Name="Snap Dashboard",
            SourceEntity={
                "SourceTemplate": {
                    "Arn": f"arn:aws:quicksight:us-east-1:{ACCOUNT_ID}:template/fake",
                    "DataSetReferences": [
                        {
                            "DataSetPlaceholder": "p",
                            "DataSetArn": f"arn:aws:quicksight:us-east-1:{ACCOUNT_ID}:dataset/ds",
                        }
                    ],
                }
            },
        )
        resp = quicksight.start_dashboard_snapshot_job(
            AwsAccountId=ACCOUNT_ID,
            DashboardId=dash_id,
            SnapshotJobId=snap_id,
            UserConfiguration={"AnonymousUsers": [{}]},
            SnapshotConfiguration={
                "FileGroups": [
                    {
                        "Files": [
                            {
                                "SheetSelections": [
                                    {
                                        "SheetId": "sheet1",
                                        "SelectionScope": "ALL_VISUALS",
                                    }
                                ],
                                "FormatType": "PDF",
                            }
                        ]
                    }
                ]
            },
        )
        assert resp["Status"] == 200
        assert "SnapshotJobId" in resp

    def test_describe_dashboard_snapshot_job_by_id(self, quicksight):
        resp = quicksight.describe_dashboard_snapshot_job(
            AwsAccountId=ACCOUNT_ID,
            DashboardId="fake-dash",
            SnapshotJobId="fake-snap",
        )
        assert resp["Status"] == 200


class TestQuickSightActionConnectorCRUD:
    """Test action connector create/update/describe/delete."""

    def test_create_action_connector(self, quicksight):
        ac_id = _unique("ac")
        resp = quicksight.create_action_connector(
            AwsAccountId=ACCOUNT_ID,
            ActionConnectorId=ac_id,
            Name="Test AC",
            Type="JIRA",
            AuthenticationConfig={
                "AuthenticationType": "BASIC",
                "AuthenticationMetadata": {
                    "BasicAuthConnectionMetadata": {
                        "BaseEndpoint": "https://jira.example.com",
                        "Username": "user",
                        "Password": "pass",
                    }
                },
            },
        )
        assert resp["Status"] == 200

    def test_update_action_connector(self, quicksight):
        ac_id = _unique("ac")
        quicksight.create_action_connector(
            AwsAccountId=ACCOUNT_ID,
            ActionConnectorId=ac_id,
            Name="Original AC",
            Type="JIRA",
            AuthenticationConfig={
                "AuthenticationType": "BASIC",
                "AuthenticationMetadata": {
                    "BasicAuthConnectionMetadata": {
                        "BaseEndpoint": "https://jira.example.com",
                        "Username": "user",
                        "Password": "pass",
                    }
                },
            },
        )
        resp = quicksight.update_action_connector(
            AwsAccountId=ACCOUNT_ID,
            ActionConnectorId=ac_id,
            Name="Updated AC",
            AuthenticationConfig={
                "AuthenticationType": "BASIC",
                "AuthenticationMetadata": {
                    "BasicAuthConnectionMetadata": {
                        "BaseEndpoint": "https://jira2.example.com",
                        "Username": "user2",
                        "Password": "pass2",
                    }
                },
            },
        )
        assert resp["Status"] == 200


class TestQuickSightDeleteTopic:
    """Test topic delete operation."""

    def test_delete_topic(self, quicksight):
        tid = _unique("topic")
        quicksight.create_topic(
            AwsAccountId=ACCOUNT_ID,
            TopicId=tid,
            Topic={
                "Name": "Delete Me",
                "DataSets": [
                    {
                        "DatasetArn": f"arn:aws:quicksight:us-east-1:{ACCOUNT_ID}:dataset/ds",
                        "DatasetName": "ds",
                    }
                ],
            },
        )
        resp = quicksight.delete_topic(AwsAccountId=ACCOUNT_ID, TopicId=tid)
        assert resp["Status"] == 200
        assert resp["TopicId"] == tid

    def test_delete_topic_not_found(self, quicksight):
        with pytest.raises(quicksight.exceptions.ClientError) as exc_info:
            quicksight.delete_topic(AwsAccountId=ACCOUNT_ID, TopicId="nonexistent")
        assert "ResourceNotFoundException" in str(exc_info.value)
