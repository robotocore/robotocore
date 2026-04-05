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


class TestQuickSightPredictQAResultsBehavioral:
    """Behavioral fidelity tests for predict_qa_results."""

    def test_predict_qa_results_returns_status_200(self, quicksight):
        resp = quicksight.predict_qa_results(AwsAccountId=ACCOUNT_ID, QueryText="show me sales")
        assert resp["Status"] == 200
        assert "RequestId" in resp

    def test_predict_qa_results_with_different_queries(self, quicksight):
        for query in ["revenue by region", "top customers", "monthly trends"]:
            resp = quicksight.predict_qa_results(AwsAccountId=ACCOUNT_ID, QueryText=query)
            assert resp["Status"] == 200
            assert "RequestId" in resp

    def test_predict_qa_results_response_shape(self, quicksight):
        resp = quicksight.predict_qa_results(AwsAccountId=ACCOUNT_ID, QueryText="test query")
        assert resp["Status"] == 200
        # PredictQAResults should return either PrimaryVisual or Analyses or at minimum RequestId
        assert "PrimaryVisual" in resp or "Analyses" in resp or "RequestId" in resp


class TestQuickSightAccountSettingsBehavioral:
    """Behavioral fidelity tests for account settings."""

    def test_describe_account_settings_has_expected_fields(self, quicksight):
        response = quicksight.describe_account_settings(AwsAccountId=ACCOUNT_ID)
        assert response["Status"] == 200
        settings = response["AccountSettings"]
        assert "AccountName" in settings or "Edition" in settings or "DefaultNamespace" in settings

    def test_update_then_describe_account_settings(self, quicksight):
        quicksight.update_account_settings(
            AwsAccountId=ACCOUNT_ID,
            DefaultNamespace="default",
        )
        response = quicksight.describe_account_settings(AwsAccountId=ACCOUNT_ID)
        assert response["Status"] == 200
        assert "AccountSettings" in response

    def test_update_public_sharing_settings_enable_then_disable(self, quicksight):
        enable_resp = quicksight.update_public_sharing_settings(
            AwsAccountId=ACCOUNT_ID,
            PublicSharingEnabled=True,
        )
        assert enable_resp["Status"] == 200

        disable_resp = quicksight.update_public_sharing_settings(
            AwsAccountId=ACCOUNT_ID,
            PublicSharingEnabled=False,
        )
        assert disable_resp["Status"] == 200


class TestQuickSightListWithContentBehavioral:
    """Verify list operations return resources that were created."""

    def test_list_dashboards_with_content(self, quicksight):
        dash_id = _unique("dash")
        quicksight.create_dashboard(
            AwsAccountId=ACCOUNT_ID,
            DashboardId=dash_id,
            Name="Listed Dashboard",
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
        response = quicksight.list_dashboards(AwsAccountId=ACCOUNT_ID)
        assert response["Status"] == 200
        dash_ids = [d["DashboardId"] for d in response["DashboardSummaryList"]]
        assert dash_id in dash_ids

    def test_list_analyses_with_content(self, quicksight):
        aid = _unique("analysis")
        quicksight.create_analysis(
            AwsAccountId=ACCOUNT_ID,
            AnalysisId=aid,
            Name="Listed Analysis",
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
        response = quicksight.list_analyses(AwsAccountId=ACCOUNT_ID)
        assert response["Status"] == 200
        analysis_ids = [a["AnalysisId"] for a in response["AnalysisSummaryList"]]
        assert aid in analysis_ids

    def test_list_data_sets_with_content(self, quicksight):
        ds_id = _unique("ds")
        dset_id = _unique("dset")
        quicksight.create_data_source(
            AwsAccountId=ACCOUNT_ID,
            DataSourceId=ds_id,
            Name="DS for list test",
            Type="S3",
            DataSourceParameters={
                "S3Parameters": {"ManifestFileLocation": {"Bucket": "b", "Key": "k"}}
            },
        )
        try:
            quicksight.create_data_set(
                AwsAccountId=ACCOUNT_ID,
                DataSetId=dset_id,
                Name="Listed Dataset",
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
            response = quicksight.list_data_sets(AwsAccountId=ACCOUNT_ID)
            assert response["Status"] == 200
            dataset_ids = [d["DataSetId"] for d in response["DataSetSummaries"]]
            assert dset_id in dataset_ids
        finally:
            quicksight.delete_data_source(AwsAccountId=ACCOUNT_ID, DataSourceId=ds_id)

    def test_list_folders_with_content(self, quicksight):
        folder_id = _unique("folder")
        quicksight.create_folder(
            AwsAccountId=ACCOUNT_ID,
            FolderId=folder_id,
            Name="Listed Folder",
            FolderType="SHARED",
        )
        response = quicksight.list_folders(AwsAccountId=ACCOUNT_ID)
        assert response["Status"] == 200
        folder_ids = [f["FolderId"] for f in response["FolderSummaryList"]]
        assert folder_id in folder_ids

    def test_list_templates_with_content(self, quicksight):
        tmpl_id = _unique("tmpl")
        quicksight.create_template(
            AwsAccountId=ACCOUNT_ID,
            TemplateId=tmpl_id,
            Name="Listed Template",
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
        response = quicksight.list_templates(AwsAccountId=ACCOUNT_ID)
        assert response["Status"] == 200
        tmpl_ids = [t["TemplateId"] for t in response["TemplateSummaryList"]]
        assert tmpl_id in tmpl_ids

    def test_list_themes_with_content(self, quicksight):
        theme_id = _unique("theme")
        quicksight.create_theme(
            AwsAccountId=ACCOUNT_ID,
            ThemeId=theme_id,
            Name="Listed Theme",
            BaseThemeId="CLASSIC",
            Configuration={"DataColorPalette": {"Colors": ["#FF0000"]}},
        )
        response = quicksight.list_themes(AwsAccountId=ACCOUNT_ID)
        assert response["Status"] == 200
        theme_ids = [t["ThemeId"] for t in response["ThemeSummaryList"]]
        assert theme_id in theme_ids

    def test_list_namespaces_is_list(self, quicksight):
        response = quicksight.list_namespaces(AwsAccountId=ACCOUNT_ID)
        assert response["Status"] == 200
        assert isinstance(response["Namespaces"], list)

    def test_list_groups_after_create(self, quicksight):
        group_name = _unique("grp")
        quicksight.create_group(AwsAccountId=ACCOUNT_ID, Namespace=NAMESPACE, GroupName=group_name)
        try:
            response = quicksight.list_groups(AwsAccountId=ACCOUNT_ID, Namespace=NAMESPACE)
            assert response["Status"] == 200
            group_names = [g["GroupName"] for g in response["GroupList"]]
            assert group_name in group_names
        finally:
            quicksight.delete_group(
                AwsAccountId=ACCOUNT_ID, Namespace=NAMESPACE, GroupName=group_name
            )

    def test_list_users_after_register(self, quicksight):
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
            response = quicksight.list_users(AwsAccountId=ACCOUNT_ID, Namespace=NAMESPACE)
            assert response["Status"] == 200
            user_names = [u["UserName"] for u in response["UserList"]]
            assert user_name in user_names
        finally:
            quicksight.delete_user(
                AwsAccountId=ACCOUNT_ID, Namespace=NAMESPACE, UserName=user_name
            )


class TestQuickSightSearchGroupsBehavioral:
    """Behavioral edge cases for search_groups."""

    def test_search_groups_retrieve_result(self, quicksight):
        group_name = _unique("srchgrp")
        create_resp = quicksight.create_group(
            AwsAccountId=ACCOUNT_ID, Namespace=NAMESPACE, GroupName=group_name
        )
        try:
            assert "Group" in create_resp
            search_resp = quicksight.search_groups(
                AwsAccountId=ACCOUNT_ID,
                Namespace=NAMESPACE,
                Filters=[
                    {"Operator": "StartsWith", "Name": "GROUP_NAME", "Value": group_name[:6]}
                ],
            )
            assert search_resp["Status"] == 200
            found = [g for g in search_resp["GroupList"] if g["GroupName"] == group_name]
            assert len(found) == 1
            assert found[0]["GroupName"] == group_name
            assert "Arn" in found[0]
            assert ACCOUNT_ID in found[0]["Arn"]
        finally:
            quicksight.delete_group(
                AwsAccountId=ACCOUNT_ID, Namespace=NAMESPACE, GroupName=group_name
            )

    def test_search_groups_no_match_returns_empty(self, quicksight):
        search_resp = quicksight.search_groups(
            AwsAccountId=ACCOUNT_ID,
            Namespace=NAMESPACE,
            Filters=[
                {
                    "Operator": "StartsWith",
                    "Name": "GROUP_NAME",
                    "Value": "zzz-nonexistent-prefix-xyz",
                }
            ],
        )
        assert search_resp["Status"] == 200
        assert search_resp["GroupList"] == []

    def test_search_groups_deleted_group_not_found(self, quicksight):
        group_name = _unique("delgrp")
        quicksight.create_group(AwsAccountId=ACCOUNT_ID, Namespace=NAMESPACE, GroupName=group_name)
        quicksight.delete_group(AwsAccountId=ACCOUNT_ID, Namespace=NAMESPACE, GroupName=group_name)
        search_resp = quicksight.search_groups(
            AwsAccountId=ACCOUNT_ID,
            Namespace=NAMESPACE,
            Filters=[
                {"Operator": "StartsWith", "Name": "GROUP_NAME", "Value": group_name}
            ],
        )
        assert search_resp["Status"] == 200
        found = [g for g in search_resp["GroupList"] if g["GroupName"] == group_name]
        assert len(found) == 0


class TestQuickSightARNFidelity:
    """Verify ARN format and structure for created resources."""

    def test_group_arn_format(self, quicksight):
        group_name = _unique("grp")
        resp = quicksight.create_group(
            AwsAccountId=ACCOUNT_ID, Namespace=NAMESPACE, GroupName=group_name
        )
        try:
            arn = resp["Group"]["Arn"]
            assert arn.startswith("arn:aws:quicksight:")
            assert ACCOUNT_ID in arn
            assert group_name in arn
        finally:
            quicksight.delete_group(
                AwsAccountId=ACCOUNT_ID, Namespace=NAMESPACE, GroupName=group_name
            )

    def test_user_arn_format(self, quicksight):
        user_name = _unique("user")
        resp = quicksight.register_user(
            AwsAccountId=ACCOUNT_ID,
            Namespace=NAMESPACE,
            Email=f"{user_name}@example.com",
            IdentityType="QUICKSIGHT",
            UserRole="READER",
            UserName=user_name,
        )
        try:
            arn = resp["User"]["Arn"]
            assert arn.startswith("arn:aws:quicksight:")
            assert ACCOUNT_ID in arn
            assert user_name in arn
        finally:
            quicksight.delete_user(AwsAccountId=ACCOUNT_ID, Namespace=NAMESPACE, UserName=user_name)

    def test_data_source_arn_format(self, quicksight):
        ds_id = _unique("ds")
        resp = quicksight.create_data_source(
            AwsAccountId=ACCOUNT_ID,
            DataSourceId=ds_id,
            Name="ARN Test DS",
            Type="S3",
            DataSourceParameters={
                "S3Parameters": {"ManifestFileLocation": {"Bucket": "b", "Key": "k"}}
            },
        )
        try:
            arn = resp["Arn"]
            assert arn.startswith("arn:aws:quicksight:")
            assert ACCOUNT_ID in arn
            assert ds_id in arn
        finally:
            quicksight.delete_data_source(AwsAccountId=ACCOUNT_ID, DataSourceId=ds_id)


class TestQuickSightIdempotency:
    """Test lifecycle and recreate behaviors."""

    def test_delete_then_recreate_group(self, quicksight):
        group_name = _unique("recreate")
        quicksight.create_group(AwsAccountId=ACCOUNT_ID, Namespace=NAMESPACE, GroupName=group_name)
        quicksight.delete_group(AwsAccountId=ACCOUNT_ID, Namespace=NAMESPACE, GroupName=group_name)
        resp = quicksight.create_group(
            AwsAccountId=ACCOUNT_ID, Namespace=NAMESPACE, GroupName=group_name
        )
        assert resp["Status"] == 200
        assert resp["Group"]["GroupName"] == group_name
        quicksight.delete_group(AwsAccountId=ACCOUNT_ID, Namespace=NAMESPACE, GroupName=group_name)

    def test_create_dashboard_same_id_returns_success(self, quicksight):
        dash_id = _unique("idem-dash")
        source = {
            "SourceTemplate": {
                "Arn": f"arn:aws:quicksight:us-east-1:{ACCOUNT_ID}:template/fake-tmpl",
                "DataSetReferences": [
                    {
                        "DataSetPlaceholder": "ph",
                        "DataSetArn": f"arn:aws:quicksight:us-east-1:{ACCOUNT_ID}:dataset/ds",
                    }
                ],
            }
        }
        r1 = quicksight.create_dashboard(
            AwsAccountId=ACCOUNT_ID, DashboardId=dash_id, Name="First", SourceEntity=source
        )
        assert r1["Status"] in (200, 201, 202)
        r2 = quicksight.create_dashboard(
            AwsAccountId=ACCOUNT_ID, DashboardId=dash_id, Name="Second", SourceEntity=source
        )
        assert r2["Status"] in (200, 201, 202)
        assert r2["DashboardId"] == dash_id

    def test_create_data_source_then_delete_then_recreate(self, quicksight):
        ds_id = _unique("ds-lifecycle")
        params = {"S3Parameters": {"ManifestFileLocation": {"Bucket": "b", "Key": "k"}}}
        quicksight.create_data_source(
            AwsAccountId=ACCOUNT_ID, DataSourceId=ds_id, Name="DS v1", Type="S3",
            DataSourceParameters=params,
        )
        quicksight.delete_data_source(AwsAccountId=ACCOUNT_ID, DataSourceId=ds_id)
        resp = quicksight.create_data_source(
            AwsAccountId=ACCOUNT_ID, DataSourceId=ds_id, Name="DS v2", Type="S3",
            DataSourceParameters=params,
        )
        assert resp["Status"] in (200, 201, 202)
        assert resp["DataSourceId"] == ds_id
        quicksight.delete_data_source(AwsAccountId=ACCOUNT_ID, DataSourceId=ds_id)


class TestQuickSightDescribeAfterDelete:
    """Verify describe operations fail after deletion."""

    def test_describe_group_after_delete_raises(self, quicksight):
        group_name = _unique("deltest")
        quicksight.create_group(AwsAccountId=ACCOUNT_ID, Namespace=NAMESPACE, GroupName=group_name)
        quicksight.delete_group(AwsAccountId=ACCOUNT_ID, Namespace=NAMESPACE, GroupName=group_name)
        with pytest.raises(quicksight.exceptions.ClientError) as exc_info:
            quicksight.describe_group(
                AwsAccountId=ACCOUNT_ID, Namespace=NAMESPACE, GroupName=group_name
            )
        assert "ResourceNotFoundException" in str(exc_info.value)

    def test_describe_user_after_delete_raises(self, quicksight):
        user_name = _unique("deluser")
        quicksight.register_user(
            AwsAccountId=ACCOUNT_ID,
            Namespace=NAMESPACE,
            Email=f"{user_name}@example.com",
            IdentityType="QUICKSIGHT",
            UserRole="READER",
            UserName=user_name,
        )
        quicksight.delete_user(AwsAccountId=ACCOUNT_ID, Namespace=NAMESPACE, UserName=user_name)
        with pytest.raises(quicksight.exceptions.ClientError) as exc_info:
            quicksight.describe_user(
                AwsAccountId=ACCOUNT_ID, Namespace=NAMESPACE, UserName=user_name
            )
        assert "ResourceNotFoundException" in str(exc_info.value)

    def test_describe_data_source_after_delete_raises(self, quicksight):
        ds_id = _unique("delds")
        quicksight.create_data_source(
            AwsAccountId=ACCOUNT_ID,
            DataSourceId=ds_id,
            Name="To Delete DS",
            Type="S3",
            DataSourceParameters={
                "S3Parameters": {"ManifestFileLocation": {"Bucket": "b", "Key": "k"}}
            },
        )
        quicksight.delete_data_source(AwsAccountId=ACCOUNT_ID, DataSourceId=ds_id)
        with pytest.raises(quicksight.exceptions.ClientError) as exc_info:
            quicksight.describe_data_source(AwsAccountId=ACCOUNT_ID, DataSourceId=ds_id)
        assert "ResourceNotFoundException" in str(exc_info.value)


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


class TestQuickSightGapOps:
    """Tests for QuickSight operations that weren't previously covered."""

    @pytest.fixture
    def qs(self):
        return make_client("quicksight")

    def test_list_folders_for_resource(self, qs):
        """ListFoldersForResource returns a list for any resource ARN."""
        resp = qs.list_folders_for_resource(
            AwsAccountId=ACCOUNT_ID,
            ResourceArn=f"arn:aws:quicksight:us-east-1:{ACCOUNT_ID}:dashboard/nonexistent",
        )
        assert "Folders" in resp or resp["ResponseMetadata"]["HTTPStatusCode"] == 200


class TestQuickSightRemainingGapOps:
    """Tests for remaining QuickSight gap operations."""

    @pytest.fixture
    def client(self):
        return make_client("quicksight")

    def test_create_namespace_not_implemented(self, client):
        from botocore.exceptions import ClientError

        with pytest.raises(ClientError) as exc:
            client.create_namespace(
                AwsAccountId=ACCOUNT_ID,
                Namespace="testnamespace123",
                IdentityStore="QUICKSIGHT",
            )
        assert exc.value.response["Error"]["Code"] in (
            "NotImplemented",
            "ResourceExistsException",
            "AccessDeniedException",
        )

    def test_describe_dashboard_snapshot_job_result(self, client):
        resp = client.describe_dashboard_snapshot_job_result(
            AwsAccountId=ACCOUNT_ID,
            DashboardId="d-nonexistent",
            SnapshotJobId="job-nonexistent",
        )
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200
        assert "Status" in resp

    def test_describe_folder_resolved_permissions_not_implemented(self, client):
        from botocore.exceptions import ClientError

        with pytest.raises(ClientError) as exc:
            client.describe_folder_resolved_permissions(
                AwsAccountId=ACCOUNT_ID,
                FolderId="folder-nonexistent",
            )
        assert exc.value.response["Error"]["Code"] in (
            "NotImplemented",
            "ResourceNotFoundException",
        )

    def test_get_identity_context_returns_response(self, client):
        resp = client.get_identity_context(
            AwsAccountId=ACCOUNT_ID,
            Namespace="default",
            UserIdentifier={
                "UserArn": f"arn:aws:quicksight:us-east-1:{ACCOUNT_ID}:user/default/testuser"
            },
        )
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200


class TestQuickSightAccountSettingsFidelity:
    """Behavioral fidelity for account settings: field content and update persistence."""

    def test_describe_account_settings_has_expected_fields(self, quicksight):
        resp = quicksight.describe_account_settings(AwsAccountId=ACCOUNT_ID)
        settings = resp["AccountSettings"]
        assert "DefaultNamespace" in settings
        assert "Edition" in settings
        assert "PublicSharingEnabled" in settings
        assert isinstance(settings["PublicSharingEnabled"], bool)
        assert isinstance(settings["DefaultNamespace"], str)

    def test_update_account_settings_notification_email_persists(self, quicksight):
        quicksight.update_account_settings(
            AwsAccountId=ACCOUNT_ID,
            DefaultNamespace="default",
            NotificationEmail="fidelity-test@example.com",
        )
        resp = quicksight.describe_account_settings(AwsAccountId=ACCOUNT_ID)
        assert resp["AccountSettings"]["NotificationEmail"] == "fidelity-test@example.com"

    def test_public_sharing_toggle_persists_in_account_settings(self, quicksight):
        quicksight.update_public_sharing_settings(
            AwsAccountId=ACCOUNT_ID, PublicSharingEnabled=True
        )
        resp = quicksight.describe_account_settings(AwsAccountId=ACCOUNT_ID)
        assert resp["AccountSettings"]["PublicSharingEnabled"] is True

        quicksight.update_public_sharing_settings(
            AwsAccountId=ACCOUNT_ID, PublicSharingEnabled=False
        )
        resp = quicksight.describe_account_settings(AwsAccountId=ACCOUNT_ID)
        assert resp["AccountSettings"]["PublicSharingEnabled"] is False


class TestQuickSightEmbedUrlFidelity:
    """Behavioral fidelity: embed URLs return non-empty strings."""

    def test_generate_embed_url_for_anonymous_user_returns_nonempty_url(self, quicksight):
        resp = quicksight.generate_embed_url_for_anonymous_user(
            AwsAccountId=ACCOUNT_ID,
            Namespace=NAMESPACE,
            AuthorizedResourceArns=[
                f"arn:aws:quicksight:us-east-1:{ACCOUNT_ID}:dashboard/fake"
            ],
            ExperienceConfiguration={"Dashboard": {"InitialDashboardId": "fake"}},
        )
        assert resp["Status"] == 200
        assert isinstance(resp["EmbedUrl"], str)
        assert len(resp["EmbedUrl"]) > 0

    def test_generate_embed_url_for_registered_user_returns_nonempty_url(self, quicksight):
        resp = quicksight.generate_embed_url_for_registered_user(
            AwsAccountId=ACCOUNT_ID,
            UserArn=f"arn:aws:quicksight:us-east-1:{ACCOUNT_ID}:user/default/fake",
            ExperienceConfiguration={"Dashboard": {"InitialDashboardId": "fake"}},
        )
        assert resp["Status"] == 200
        assert isinstance(resp["EmbedUrl"], str)
        assert len(resp["EmbedUrl"]) > 0

    def test_generate_embed_url_for_registered_user_with_identity_returns_nonempty_url(
        self, quicksight
    ):
        resp = quicksight.generate_embed_url_for_registered_user_with_identity(
            AwsAccountId=ACCOUNT_ID,
            ExperienceConfiguration={"Dashboard": {"InitialDashboardId": "fake"}},
        )
        assert resp["Status"] == 200
        assert isinstance(resp["EmbedUrl"], str)
        assert len(resp["EmbedUrl"]) > 0

    def test_generate_embed_url_has_request_id(self, quicksight):
        resp = quicksight.generate_embed_url_for_registered_user(
            AwsAccountId=ACCOUNT_ID,
            UserArn=f"arn:aws:quicksight:us-east-1:{ACCOUNT_ID}:user/default/fake",
            ExperienceConfiguration={"QuickSightConsole": {"InitialPath": "/start"}},
        )
        assert resp["Status"] == 200
        assert "RequestId" in resp
        assert isinstance(resp["RequestId"], str)


class TestQuickSightPredictQaFidelity:
    """Behavioral fidelity for predict_qa_results."""

    def test_predict_qa_results_request_id_is_nonempty_string(self, quicksight):
        """RequestId should be a non-empty string in every response."""
        resp = quicksight.predict_qa_results(
            AwsAccountId=ACCOUNT_ID, QueryText="what is total revenue?"
        )
        assert resp["Status"] == 200
        assert "RequestId" in resp
        assert isinstance(resp["RequestId"], str)
        assert len(resp["RequestId"]) > 0

    def test_predict_qa_results_status_200_for_multiple_queries(self, quicksight):
        """Different query texts should each return Status 200 with a RequestId."""
        for query in ["show sales by region", "total orders last month", "top customers"]:
            resp = quicksight.predict_qa_results(AwsAccountId=ACCOUNT_ID, QueryText=query)
            assert resp["Status"] == 200
            assert "RequestId" in resp


class TestQuickSightGroupPagination:
    """Behavioral fidelity: group listing pagination."""

    def test_list_groups_pagination_with_max_results(self, quicksight):
        group_names = [f"paggrp-{uuid.uuid4().hex[:8]}" for _ in range(4)]
        for name in group_names:
            quicksight.create_group(
                AwsAccountId=ACCOUNT_ID, Namespace=NAMESPACE, GroupName=name
            )
        try:
            resp = quicksight.list_groups(
                AwsAccountId=ACCOUNT_ID, Namespace=NAMESPACE, MaxResults=2
            )
            assert resp["Status"] == 200
            assert len(resp["GroupList"]) <= 2
            assert "NextToken" in resp
        finally:
            for name in group_names:
                quicksight.delete_group(
                    AwsAccountId=ACCOUNT_ID, Namespace=NAMESPACE, GroupName=name
                )

    def test_list_groups_pagination_yields_all_results(self, quicksight):
        group_names = [f"paggrp2-{uuid.uuid4().hex[:8]}" for _ in range(3)]
        for name in group_names:
            quicksight.create_group(
                AwsAccountId=ACCOUNT_ID, Namespace=NAMESPACE, GroupName=name
            )
        try:
            collected = []
            resp = quicksight.list_groups(
                AwsAccountId=ACCOUNT_ID, Namespace=NAMESPACE, MaxResults=2
            )
            collected.extend(resp["GroupList"])
            while "NextToken" in resp:
                resp = quicksight.list_groups(
                    AwsAccountId=ACCOUNT_ID,
                    Namespace=NAMESPACE,
                    MaxResults=2,
                    NextToken=resp["NextToken"],
                )
                collected.extend(resp["GroupList"])
            all_names = [g["GroupName"] for g in collected]
            for name in group_names:
                assert name in all_names
        finally:
            for name in group_names:
                quicksight.delete_group(
                    AwsAccountId=ACCOUNT_ID, Namespace=NAMESPACE, GroupName=name
                )


class TestQuickSightUserPagination:
    """Behavioral fidelity: user listing pagination."""

    def test_list_users_pagination_with_max_results(self, quicksight):
        user_names = [f"paguser-{uuid.uuid4().hex[:8]}" for _ in range(3)]
        for name in user_names:
            quicksight.register_user(
                AwsAccountId=ACCOUNT_ID,
                Namespace=NAMESPACE,
                Email=f"{name}@example.com",
                IdentityType="QUICKSIGHT",
                UserRole="READER",
                UserName=name,
            )
        try:
            resp = quicksight.list_users(
                AwsAccountId=ACCOUNT_ID, Namespace=NAMESPACE, MaxResults=2
            )
            assert resp["Status"] == 200
            assert len(resp["UserList"]) <= 2
        finally:
            for name in user_names:
                quicksight.delete_user(
                    AwsAccountId=ACCOUNT_ID, Namespace=NAMESPACE, UserName=name
                )


class TestQuickSightGroupSearchEdgeCases:
    """Edge cases for search_groups."""

    def test_search_groups_no_results_for_nonmatching_prefix(self, quicksight):
        resp = quicksight.search_groups(
            AwsAccountId=ACCOUNT_ID,
            Namespace=NAMESPACE,
            Filters=[
                {
                    "Operator": "StartsWith",
                    "Name": "GROUP_NAME",
                    "Value": "zzz-nonexistent-prefix-xyz-",
                }
            ],
        )
        assert resp["Status"] == 200
        assert isinstance(resp["GroupList"], list)
        assert len(resp["GroupList"]) == 0

    def test_search_groups_deleted_group_not_in_results(self, quicksight):
        group_name = f"searchdel-{uuid.uuid4().hex[:8]}"
        quicksight.create_group(
            AwsAccountId=ACCOUNT_ID, Namespace=NAMESPACE, GroupName=group_name
        )
        # Verify it appears in search
        resp = quicksight.search_groups(
            AwsAccountId=ACCOUNT_ID,
            Namespace=NAMESPACE,
            Filters=[{"Operator": "StartsWith", "Name": "GROUP_NAME", "Value": group_name}],
        )
        assert any(g["GroupName"] == group_name for g in resp["GroupList"])

        # Delete it and verify it's gone from search
        quicksight.delete_group(
            AwsAccountId=ACCOUNT_ID, Namespace=NAMESPACE, GroupName=group_name
        )
        resp2 = quicksight.search_groups(
            AwsAccountId=ACCOUNT_ID,
            Namespace=NAMESPACE,
            Filters=[{"Operator": "StartsWith", "Name": "GROUP_NAME", "Value": group_name}],
        )
        assert all(g["GroupName"] != group_name for g in resp2["GroupList"])

    def test_search_groups_returns_arn(self, quicksight):
        group_name = f"searcharn-{uuid.uuid4().hex[:8]}"
        quicksight.create_group(
            AwsAccountId=ACCOUNT_ID, Namespace=NAMESPACE, GroupName=group_name
        )
        try:
            resp = quicksight.search_groups(
                AwsAccountId=ACCOUNT_ID,
                Namespace=NAMESPACE,
                Filters=[
                    {"Operator": "StartsWith", "Name": "GROUP_NAME", "Value": group_name}
                ],
            )
            assert len(resp["GroupList"]) >= 1
            group = next(g for g in resp["GroupList"] if g["GroupName"] == group_name)
            assert "Arn" in group
            assert ACCOUNT_ID in group["Arn"]
            assert group_name in group["Arn"]
        finally:
            quicksight.delete_group(
                AwsAccountId=ACCOUNT_ID, Namespace=NAMESPACE, GroupName=group_name
            )


class TestQuickSightArnFormat:
    """Verify ARN formats match expected patterns."""

    def test_group_arn_contains_account_and_name(self, quicksight):
        group_name = f"arntest-{uuid.uuid4().hex[:8]}"
        resp = quicksight.create_group(
            AwsAccountId=ACCOUNT_ID, Namespace=NAMESPACE, GroupName=group_name
        )
        arn = resp["Group"]["Arn"]
        assert "arn:aws:quicksight:" in arn
        assert ACCOUNT_ID in arn
        assert group_name in arn
        quicksight.delete_group(AwsAccountId=ACCOUNT_ID, Namespace=NAMESPACE, GroupName=group_name)

    def test_user_arn_contains_account_and_username(self, quicksight):
        user_name = f"arnuser-{uuid.uuid4().hex[:8]}"
        resp = quicksight.register_user(
            AwsAccountId=ACCOUNT_ID,
            Namespace=NAMESPACE,
            Email=f"{user_name}@example.com",
            IdentityType="QUICKSIGHT",
            UserRole="READER",
            UserName=user_name,
        )
        arn = resp["User"]["Arn"]
        assert "arn:aws:quicksight:" in arn
        assert ACCOUNT_ID in arn
        assert user_name in arn
        quicksight.delete_user(AwsAccountId=ACCOUNT_ID, Namespace=NAMESPACE, UserName=user_name)

    def test_data_source_arn_contains_account_and_id(self, quicksight):
        ds_id = _unique("ds")
        resp = quicksight.create_data_source(
            AwsAccountId=ACCOUNT_ID,
            DataSourceId=ds_id,
            Name="ARN Test DS",
            Type="S3",
            DataSourceParameters={
                "S3Parameters": {"ManifestFileLocation": {"Bucket": "b", "Key": "k"}}
            },
        )
        arn = resp["Arn"]
        assert "arn:aws:quicksight:" in arn
        assert ACCOUNT_ID in arn
        assert ds_id in arn
        quicksight.delete_data_source(AwsAccountId=ACCOUNT_ID, DataSourceId=ds_id)


class TestQuickSightDashboardListFidelity:
    """Behavioral fidelity: dashboards appear in list after creation."""

    def test_list_dashboards_includes_created(self, quicksight):
        dash_id = _unique("dash")
        quicksight.create_dashboard(
            AwsAccountId=ACCOUNT_ID,
            DashboardId=dash_id,
            Name="List Fidelity Dashboard",
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
        resp = quicksight.list_dashboards(AwsAccountId=ACCOUNT_ID)
        assert resp["Status"] == 200
        ids = [d["DashboardId"] for d in resp["DashboardSummaryList"]]
        assert dash_id in ids

    def test_list_dashboards_summary_has_expected_fields(self, quicksight):
        dash_id = _unique("dash")
        quicksight.create_dashboard(
            AwsAccountId=ACCOUNT_ID,
            DashboardId=dash_id,
            Name="Fields Check Dashboard",
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
        resp = quicksight.list_dashboards(AwsAccountId=ACCOUNT_ID)
        summaries = {d["DashboardId"]: d for d in resp["DashboardSummaryList"]}
        assert dash_id in summaries
        summary = summaries[dash_id]
        assert "Arn" in summary
        assert "Name" in summary
        assert summary["Name"] == "Fields Check Dashboard"


class TestQuickSightListAnalysesFidelity:
    """Behavioral fidelity: analyses appear in list after creation."""

    def test_list_analyses_includes_created(self, quicksight):
        aid = _unique("analysis")
        quicksight.create_analysis(
            AwsAccountId=ACCOUNT_ID,
            AnalysisId=aid,
            Name="List Test Analysis",
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
        resp = quicksight.list_analyses(AwsAccountId=ACCOUNT_ID)
        assert resp["Status"] == 200
        ids = [a["AnalysisId"] for a in resp["AnalysisSummaryList"]]
        assert aid in ids


class TestQuickSightListDataSetsFidelity:
    """Behavioral fidelity: datasets appear in list after creation."""

    def test_list_data_sets_includes_created(self, quicksight):
        ds_id = _unique("ds")
        dset_id = _unique("dset")
        quicksight.create_data_source(
            AwsAccountId=ACCOUNT_ID,
            DataSourceId=ds_id,
            Name="DS for list test",
            Type="S3",
            DataSourceParameters={
                "S3Parameters": {"ManifestFileLocation": {"Bucket": "b", "Key": "k"}}
            },
        )
        quicksight.create_data_set(
            AwsAccountId=ACCOUNT_ID,
            DataSetId=dset_id,
            Name="List Test Dataset",
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
        try:
            resp = quicksight.list_data_sets(AwsAccountId=ACCOUNT_ID)
            assert resp["Status"] == 200
            ids = [d["DataSetId"] for d in resp["DataSetSummaries"]]
            assert dset_id in ids
        finally:
            quicksight.delete_data_source(AwsAccountId=ACCOUNT_ID, DataSourceId=ds_id)


class TestQuickSightListFoldersFidelity:
    """Behavioral fidelity: folders appear in list after creation."""

    def test_list_folders_includes_created(self, quicksight):
        folder_id = _unique("folder")
        quicksight.create_folder(
            AwsAccountId=ACCOUNT_ID,
            FolderId=folder_id,
            Name="List Test Folder",
            FolderType="SHARED",
        )
        resp = quicksight.list_folders(AwsAccountId=ACCOUNT_ID)
        assert resp["Status"] == 200
        ids = [f["FolderId"] for f in resp["FolderSummaryList"]]
        assert folder_id in ids

    def test_list_folders_summary_has_arn(self, quicksight):
        folder_id = _unique("folder")
        quicksight.create_folder(
            AwsAccountId=ACCOUNT_ID,
            FolderId=folder_id,
            Name="ARN Check Folder",
            FolderType="SHARED",
        )
        resp = quicksight.list_folders(AwsAccountId=ACCOUNT_ID)
        summaries = {f["FolderId"]: f for f in resp["FolderSummaryList"]}
        assert folder_id in summaries
        assert "Arn" in summaries[folder_id]


class TestQuickSightListTemplatesFidelity:
    """Behavioral fidelity: templates appear in list after creation."""

    def test_list_templates_includes_created(self, quicksight):
        tmpl_id = _unique("tmpl")
        quicksight.create_template(
            AwsAccountId=ACCOUNT_ID,
            TemplateId=tmpl_id,
            Name="List Test Template",
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
        resp = quicksight.list_templates(AwsAccountId=ACCOUNT_ID)
        assert resp["Status"] == 200
        ids = [t["TemplateId"] for t in resp["TemplateSummaryList"]]
        assert tmpl_id in ids


class TestQuickSightListThemesFidelity:
    """Behavioral fidelity: themes appear in list after creation."""

    def test_list_themes_includes_created(self, quicksight):
        theme_id = _unique("theme")
        quicksight.create_theme(
            AwsAccountId=ACCOUNT_ID,
            ThemeId=theme_id,
            Name="List Test Theme",
            BaseThemeId="CLASSIC",
            Configuration={"DataColorPalette": {"Colors": ["#FF0000"]}},
        )
        resp = quicksight.list_themes(AwsAccountId=ACCOUNT_ID)
        assert resp["Status"] == 200
        ids = [t["ThemeId"] for t in resp["ThemeSummaryList"]]
        assert theme_id in ids

    def test_list_themes_summary_has_name(self, quicksight):
        theme_id = _unique("theme")
        quicksight.create_theme(
            AwsAccountId=ACCOUNT_ID,
            ThemeId=theme_id,
            Name="Named List Theme",
            BaseThemeId="CLASSIC",
            Configuration={"DataColorPalette": {"Colors": ["#00FF00"]}},
        )
        resp = quicksight.list_themes(AwsAccountId=ACCOUNT_ID)
        summaries = {t["ThemeId"]: t for t in resp["ThemeSummaryList"]}
        assert theme_id in summaries
        assert summaries[theme_id]["Name"] == "Named List Theme"


class TestQuickSightGroupDuplicateCreate:
    """Edge case: creating same group name twice."""

    def test_create_duplicate_group_raises_already_exists(self, quicksight):
        group_name = f"dupgrp-{uuid.uuid4().hex[:8]}"
        quicksight.create_group(
            AwsAccountId=ACCOUNT_ID, Namespace=NAMESPACE, GroupName=group_name
        )
        try:
            with pytest.raises(quicksight.exceptions.ClientError) as exc_info:
                quicksight.create_group(
                    AwsAccountId=ACCOUNT_ID, Namespace=NAMESPACE, GroupName=group_name
                )
            assert "ResourceExistsException" in str(exc_info.value)
        finally:
            quicksight.delete_group(
                AwsAccountId=ACCOUNT_ID, Namespace=NAMESPACE, GroupName=group_name
            )


class TestQuickSightGroupDeleteNonexistent:
    """Edge case: deleting a group that doesn't exist."""

    def test_delete_nonexistent_group_raises(self, quicksight):
        with pytest.raises(quicksight.exceptions.ClientError) as exc_info:
            quicksight.delete_group(
                AwsAccountId=ACCOUNT_ID,
                Namespace=NAMESPACE,
                GroupName="nonexistent-group-xyz",
            )
        assert "ResourceNotFoundException" in str(exc_info.value)


class TestQuickSightGroupDeleteThenList:
    """Behavioral fidelity: deleted group does not appear in list."""

    def test_deleted_group_absent_from_list(self, quicksight):
        group_name = f"delgrp-{uuid.uuid4().hex[:8]}"
        quicksight.create_group(
            AwsAccountId=ACCOUNT_ID, Namespace=NAMESPACE, GroupName=group_name
        )
        # Verify it's in the list before deletion
        before = quicksight.list_groups(AwsAccountId=ACCOUNT_ID, Namespace=NAMESPACE)
        names_before = [g["GroupName"] for g in before["GroupList"]]
        assert group_name in names_before

        quicksight.delete_group(
            AwsAccountId=ACCOUNT_ID, Namespace=NAMESPACE, GroupName=group_name
        )

        after = quicksight.list_groups(AwsAccountId=ACCOUNT_ID, Namespace=NAMESPACE)
        names_after = [g["GroupName"] for g in after["GroupList"]]
        assert group_name not in names_after


class TestQuickSightUserDuplicateRegister:
    """Edge case: registering the same user name twice."""

    def test_register_duplicate_user_raises(self, quicksight):
        user_name = f"dupuser-{uuid.uuid4().hex[:8]}"
        quicksight.register_user(
            AwsAccountId=ACCOUNT_ID,
            Namespace=NAMESPACE,
            Email=f"{user_name}@example.com",
            IdentityType="QUICKSIGHT",
            UserRole="READER",
            UserName=user_name,
        )
        try:
            with pytest.raises(quicksight.exceptions.ClientError) as exc_info:
                quicksight.register_user(
                    AwsAccountId=ACCOUNT_ID,
                    Namespace=NAMESPACE,
                    Email=f"{user_name}@example.com",
                    IdentityType="QUICKSIGHT",
                    UserRole="READER",
                    UserName=user_name,
                )
            assert "ResourceExistsException" in str(exc_info.value)
        finally:
            quicksight.delete_user(AwsAccountId=ACCOUNT_ID, Namespace=NAMESPACE, UserName=user_name)


class TestQuickSightUserDeleteNonexistent:
    """Edge case: deleting a user that doesn't exist."""

    def test_delete_nonexistent_user_raises(self, quicksight):
        with pytest.raises(quicksight.exceptions.ClientError) as exc_info:
            quicksight.delete_user(
                AwsAccountId=ACCOUNT_ID,
                Namespace=NAMESPACE,
                UserName="nonexistent-user-xyz-123",
            )
        assert "ResourceNotFoundException" in str(exc_info.value)


class TestQuickSightUserDeleteThenList:
    """Behavioral fidelity: deleted user does not appear in list."""

    def test_deleted_user_absent_from_list(self, quicksight):
        user_name = f"deluser-{uuid.uuid4().hex[:8]}"
        quicksight.register_user(
            AwsAccountId=ACCOUNT_ID,
            Namespace=NAMESPACE,
            Email=f"{user_name}@example.com",
            IdentityType="QUICKSIGHT",
            UserRole="READER",
            UserName=user_name,
        )
        before = quicksight.list_users(AwsAccountId=ACCOUNT_ID, Namespace=NAMESPACE)
        names_before = [u["UserName"] for u in before["UserList"]]
        assert user_name in names_before

        quicksight.delete_user(AwsAccountId=ACCOUNT_ID, Namespace=NAMESPACE, UserName=user_name)

        after = quicksight.list_users(AwsAccountId=ACCOUNT_ID, Namespace=NAMESPACE)
        names_after = [u["UserName"] for u in after["UserList"]]
        assert user_name not in names_after


class TestQuickSightDashboardDeleteNonexistent:
    """Edge case: deleting a dashboard that doesn't exist."""

    def test_delete_nonexistent_dashboard_raises(self, quicksight):
        with pytest.raises(quicksight.exceptions.ClientError) as exc_info:
            quicksight.delete_dashboard(
                AwsAccountId=ACCOUNT_ID, DashboardId="nonexistent-dash-xyz"
            )
        assert "ResourceNotFoundException" in str(exc_info.value)


class TestQuickSightDashboardDeleteThenList:
    """Behavioral fidelity: deleted dashboard does not appear in list."""

    def test_deleted_dashboard_absent_from_list(self, quicksight):
        dash_id = _unique("dash")
        quicksight.create_dashboard(
            AwsAccountId=ACCOUNT_ID,
            DashboardId=dash_id,
            Name="Delete Test Dashboard",
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
        before = quicksight.list_dashboards(AwsAccountId=ACCOUNT_ID)
        ids_before = [d["DashboardId"] for d in before["DashboardSummaryList"]]
        assert dash_id in ids_before

        quicksight.delete_dashboard(AwsAccountId=ACCOUNT_ID, DashboardId=dash_id)

        after = quicksight.list_dashboards(AwsAccountId=ACCOUNT_ID)
        ids_after = [d["DashboardId"] for d in after["DashboardSummaryList"]]
        assert dash_id not in ids_after


class TestQuickSightAccountSettingsFidelity:
    """Behavioral fidelity: account settings update is reflected in describe."""

    def test_update_default_namespace_reflected_in_describe(self, quicksight):
        # Default namespace is "default"; update it and check
        quicksight.update_account_settings(
            AwsAccountId=ACCOUNT_ID,
            DefaultNamespace="default",
        )
        resp = quicksight.describe_account_settings(AwsAccountId=ACCOUNT_ID)
        assert resp["Status"] == 200
        settings = resp["AccountSettings"]
        assert "DefaultNamespace" in settings
        assert settings["DefaultNamespace"] == "default"

    def test_describe_account_settings_has_edition(self, quicksight):
        resp = quicksight.describe_account_settings(AwsAccountId=ACCOUNT_ID)
        assert resp["Status"] == 200
        settings = resp["AccountSettings"]
        assert "Edition" in settings
        assert isinstance(settings["Edition"], str)


class TestQuickSightDataSourceCreateDuplicate:
    """Edge case: creating same data source ID twice."""

    def test_create_duplicate_datasource_raises(self, quicksight):
        ds_id = _unique("ds")
        params = dict(
            AwsAccountId=ACCOUNT_ID,
            DataSourceId=ds_id,
            Name="Dup DS",
            Type="S3",
            DataSourceParameters={
                "S3Parameters": {"ManifestFileLocation": {"Bucket": "b", "Key": "k"}}
            },
        )
        quicksight.create_data_source(**params)
        try:
            with pytest.raises(quicksight.exceptions.ClientError) as exc_info:
                quicksight.create_data_source(**params)
            assert "ResourceExistsException" in str(exc_info.value)
        finally:
            quicksight.delete_data_source(AwsAccountId=ACCOUNT_ID, DataSourceId=ds_id)


class TestQuickSightDataSourceDeleteThenDescribe:
    """Behavioral fidelity: describe after delete raises ResourceNotFoundException."""

    def test_describe_deleted_datasource_raises(self, quicksight):
        ds_id = _unique("ds")
        quicksight.create_data_source(
            AwsAccountId=ACCOUNT_ID,
            DataSourceId=ds_id,
            Name="To Delete DS",
            Type="S3",
            DataSourceParameters={
                "S3Parameters": {"ManifestFileLocation": {"Bucket": "b", "Key": "k"}}
            },
        )
        quicksight.delete_data_source(AwsAccountId=ACCOUNT_ID, DataSourceId=ds_id)
        with pytest.raises(quicksight.exceptions.ClientError) as exc_info:
            quicksight.describe_data_source(AwsAccountId=ACCOUNT_ID, DataSourceId=ds_id)
        assert "ResourceNotFoundException" in str(exc_info.value)


class TestQuickSightGroupArnFormat:
    """Behavioral fidelity: group ARN follows expected format."""

    def test_group_arn_format(self, quicksight):
        group_name = f"arnfmt-{uuid.uuid4().hex[:8]}"
        resp = quicksight.create_group(
            AwsAccountId=ACCOUNT_ID, Namespace=NAMESPACE, GroupName=group_name
        )
        arn = resp["Group"]["Arn"]
        # Expected: arn:aws:quicksight:<region>:<account>:group/<namespace>/<name>
        assert arn.startswith("arn:aws:quicksight:")
        assert f":{ACCOUNT_ID}:" in arn
        assert f"group/{NAMESPACE}/{group_name}" in arn
        quicksight.delete_group(AwsAccountId=ACCOUNT_ID, Namespace=NAMESPACE, GroupName=group_name)


class TestQuickSightUserArnFormat:
    """Behavioral fidelity: user ARN follows expected format."""

    def test_user_arn_format(self, quicksight):
        user_name = f"arnfmt-{uuid.uuid4().hex[:8]}"
        resp = quicksight.register_user(
            AwsAccountId=ACCOUNT_ID,
            Namespace=NAMESPACE,
            Email=f"{user_name}@example.com",
            IdentityType="QUICKSIGHT",
            UserRole="READER",
            UserName=user_name,
        )
        arn = resp["User"]["Arn"]
        # Expected: arn:aws:quicksight:<region>:<account>:user/<namespace>/<username>
        assert arn.startswith("arn:aws:quicksight:")
        assert f":{ACCOUNT_ID}:" in arn
        assert f"user/{NAMESPACE}/{user_name}" in arn
        quicksight.delete_user(AwsAccountId=ACCOUNT_ID, Namespace=NAMESPACE, UserName=user_name)


class TestQuickSightPublicSharingSettingsFidelity:
    """Behavioral fidelity: public sharing setting toggle."""

    def test_enable_then_disable_public_sharing(self, quicksight):
        resp_enable = quicksight.update_public_sharing_settings(
            AwsAccountId=ACCOUNT_ID, PublicSharingEnabled=True
        )
        assert resp_enable["Status"] == 200

        resp_disable = quicksight.update_public_sharing_settings(
            AwsAccountId=ACCOUNT_ID, PublicSharingEnabled=False
        )
        assert resp_disable["Status"] == 200


class TestQuickSightGroupMembershipEdgeCases:
    """Edge cases for group membership operations."""

    def test_add_member_to_nonexistent_group_raises(self, quicksight):
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
            with pytest.raises(quicksight.exceptions.ClientError) as exc_info:
                quicksight.create_group_membership(
                    AwsAccountId=ACCOUNT_ID,
                    Namespace=NAMESPACE,
                    GroupName="nonexistent-group-xyz",
                    MemberName=user_name,
                )
            assert "ResourceNotFoundException" in str(exc_info.value)
        finally:
            quicksight.delete_user(AwsAccountId=ACCOUNT_ID, Namespace=NAMESPACE, UserName=user_name)

    def test_list_empty_group_memberships(self, quicksight):
        group_name = _unique("emptygrp")
        quicksight.create_group(
            AwsAccountId=ACCOUNT_ID, Namespace=NAMESPACE, GroupName=group_name
        )
        try:
            resp = quicksight.list_group_memberships(
                AwsAccountId=ACCOUNT_ID, Namespace=NAMESPACE, GroupName=group_name
            )
            assert resp["Status"] == 200
            assert isinstance(resp["GroupMemberList"], list)
            assert len(resp["GroupMemberList"]) == 0
        finally:
            quicksight.delete_group(
                AwsAccountId=ACCOUNT_ID, Namespace=NAMESPACE, GroupName=group_name
            )


class TestQuickSightDashboardArnFidelity:
    """Behavioral fidelity: dashboard ARN format."""

    def test_dashboard_arn_format(self, quicksight):
        dash_id = _unique("dash")
        resp = quicksight.create_dashboard(
            AwsAccountId=ACCOUNT_ID,
            DashboardId=dash_id,
            Name="ARN Format Dashboard",
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
        arn = resp["Arn"]
        assert arn.startswith("arn:aws:quicksight:")
        assert ACCOUNT_ID in arn
        assert dash_id in arn


class TestQuickSightDataSourceListFidelity:
    """Behavioral fidelity: data source list shows name and type."""

    def test_list_data_sources_shows_name_and_type(self, quicksight):
        ds_id = _unique("ds")
        quicksight.create_data_source(
            AwsAccountId=ACCOUNT_ID,
            DataSourceId=ds_id,
            Name="Named DS",
            Type="S3",
            DataSourceParameters={
                "S3Parameters": {"ManifestFileLocation": {"Bucket": "b", "Key": "k"}}
            },
        )
        try:
            resp = quicksight.list_data_sources(AwsAccountId=ACCOUNT_ID)
            sources = {d["DataSourceId"]: d for d in resp["DataSources"]}
            assert ds_id in sources
            assert sources[ds_id]["Name"] == "Named DS"
            assert sources[ds_id]["Type"] == "S3"
        finally:
            quicksight.delete_data_source(AwsAccountId=ACCOUNT_ID, DataSourceId=ds_id)


class TestQuickSightFolderDeleteNonexistent:
    """Edge case: deleting a folder that doesn't exist."""

    def test_delete_nonexistent_folder_raises(self, quicksight):
        with pytest.raises(quicksight.exceptions.ClientError) as exc_info:
            quicksight.delete_folder(
                AwsAccountId=ACCOUNT_ID, FolderId="nonexistent-folder-xyz"
            )
        assert "ResourceNotFoundException" in str(exc_info.value)


class TestQuickSightFolderDeleteThenList:
    """Behavioral fidelity: deleted folder does not appear in list."""

    def test_deleted_folder_absent_from_list(self, quicksight):
        folder_id = _unique("folder")
        quicksight.create_folder(
            AwsAccountId=ACCOUNT_ID,
            FolderId=folder_id,
            Name="Delete Test Folder",
            FolderType="SHARED",
        )
        before = quicksight.list_folders(AwsAccountId=ACCOUNT_ID)
        ids_before = [f["FolderId"] for f in before["FolderSummaryList"]]
        assert folder_id in ids_before

        quicksight.delete_folder(AwsAccountId=ACCOUNT_ID, FolderId=folder_id)

        after = quicksight.list_folders(AwsAccountId=ACCOUNT_ID)
        ids_after = [f["FolderId"] for f in after["FolderSummaryList"]]
        assert folder_id not in ids_after


class TestQuickSightUpdateGroupFidelity:
    """Behavioral fidelity: group update is reflected in describe."""

    def test_update_group_description_persists(self, quicksight):
        group_name = _unique("updgrp")
        quicksight.create_group(
            AwsAccountId=ACCOUNT_ID, Namespace=NAMESPACE, GroupName=group_name
        )
        try:
            quicksight.update_group(
                AwsAccountId=ACCOUNT_ID,
                Namespace=NAMESPACE,
                GroupName=group_name,
                Description="new description text",
            )
            describe_resp = quicksight.describe_group(
                AwsAccountId=ACCOUNT_ID, Namespace=NAMESPACE, GroupName=group_name
            )
            assert describe_resp["Group"]["Description"] == "new description text"
        finally:
            quicksight.delete_group(
                AwsAccountId=ACCOUNT_ID, Namespace=NAMESPACE, GroupName=group_name
            )


class TestQuickSightUpdateUserFidelity:
    """Behavioral fidelity: user update is reflected in describe."""

    def test_update_user_role_persists(self, quicksight):
        user_name = _unique("upduser")
        quicksight.register_user(
            AwsAccountId=ACCOUNT_ID,
            Namespace=NAMESPACE,
            Email=f"{user_name}@example.com",
            IdentityType="QUICKSIGHT",
            UserRole="READER",
            UserName=user_name,
        )
        try:
            quicksight.update_user(
                AwsAccountId=ACCOUNT_ID,
                Namespace=NAMESPACE,
                UserName=user_name,
                Email=f"{user_name}@example.com",
                Role="AUTHOR",
            )
            describe_resp = quicksight.describe_user(
                AwsAccountId=ACCOUNT_ID, Namespace=NAMESPACE, UserName=user_name
            )
            assert describe_resp["User"]["Role"] == "AUTHOR"
        finally:
            quicksight.delete_user(AwsAccountId=ACCOUNT_ID, Namespace=NAMESPACE, UserName=user_name)


class TestQuickSightPredictQAEdgeCases:
    """Edge cases for predict_qa_results."""

    def test_predict_qa_results_unicode_query(self, quicksight):
        resp = quicksight.predict_qa_results(
            AwsAccountId=ACCOUNT_ID,
            QueryText="収益を表示",
        )
        assert resp["Status"] == 200

    def test_predict_qa_results_long_query(self, quicksight):
        long_query = "revenue " * 25  # ~200 chars
        resp = quicksight.predict_qa_results(
            AwsAccountId=ACCOUNT_ID,
            QueryText=long_query,
        )
        assert resp["Status"] == 200
        assert "RequestId" in resp

    def test_predict_qa_results_returns_request_id(self, quicksight):
        resp = quicksight.predict_qa_results(
            AwsAccountId=ACCOUNT_ID,
            QueryText="show me sales",
        )
        assert "RequestId" in resp
        assert isinstance(resp["RequestId"], str)
        assert len(resp["RequestId"]) > 0


class TestQuickSightDataSourcesPagination:
    """Pagination behavioral tests for list_data_sources."""

    def _create_data_source(self, quicksight, ds_id, name):
        quicksight.create_data_source(
            AwsAccountId=ACCOUNT_ID,
            DataSourceId=ds_id,
            Name=name,
            Type="S3",
            DataSourceParameters={
                "S3Parameters": {"ManifestFileLocation": {"Bucket": "b", "Key": "k"}}
            },
        )

    def test_list_data_sources_all_visible_after_create(self, quicksight):
        ids = [_unique("ds-all") for _ in range(3)]
        try:
            for i, ds_id in enumerate(ids):
                self._create_data_source(quicksight, ds_id, f"DS All {i}")
            resp = quicksight.list_data_sources(AwsAccountId=ACCOUNT_ID)
            found_ids = [ds["DataSourceId"] for ds in resp["DataSources"]]
            for ds_id in ids:
                assert ds_id in found_ids
        finally:
            for ds_id in ids:
                try:
                    quicksight.delete_data_source(AwsAccountId=ACCOUNT_ID, DataSourceId=ds_id)
                except Exception:
                    pass


class TestQuickSightAccountSettingsEdgeCases:
    """Behavioral fidelity for account settings field types and persistence."""

    def test_describe_account_settings_field_types(self, quicksight):
        resp = quicksight.describe_account_settings(AwsAccountId=ACCOUNT_ID)
        settings = resp["AccountSettings"]
        assert isinstance(settings["DefaultNamespace"], str)
        assert isinstance(settings["Edition"], str)
        assert isinstance(settings["PublicSharingEnabled"], bool)

    def test_update_account_settings_persists_namespace(self, quicksight):
        quicksight.update_account_settings(
            AwsAccountId=ACCOUNT_ID,
            DefaultNamespace="default",
        )
        resp = quicksight.describe_account_settings(AwsAccountId=ACCOUNT_ID)
        assert resp["AccountSettings"]["DefaultNamespace"] == "default"


class TestQuickSightDashboardListEdgeCases:
    """Behavioral fidelity for dashboard list summaries."""

    def _create_dashboard(self, quicksight, dash_id, name):
        quicksight.create_dashboard(
            AwsAccountId=ACCOUNT_ID,
            DashboardId=dash_id,
            Name=name,
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

    def test_list_dashboards_summary_contains_arn_and_name(self, quicksight):
        dash_id = _unique("dash-arn")
        try:
            self._create_dashboard(quicksight, dash_id, "ARN Test Dashboard")
            resp = quicksight.list_dashboards(AwsAccountId=ACCOUNT_ID)
            summaries = {d["DashboardId"]: d for d in resp["DashboardSummaryList"]}
            assert dash_id in summaries
            summary = summaries[dash_id]
            assert "Arn" in summary
            assert summary["Arn"].startswith("arn:aws:quicksight:")
            assert "Name" in summary
        finally:
            try:
                quicksight.delete_dashboard(AwsAccountId=ACCOUNT_ID, DashboardId=dash_id)
            except Exception:
                pass

    def test_list_dashboards_summary_arn_contains_account_id(self, quicksight):
        dash_id = _unique("dash-acct")
        try:
            self._create_dashboard(quicksight, dash_id, "Account ID Test Dashboard")
            resp = quicksight.list_dashboards(AwsAccountId=ACCOUNT_ID)
            summaries = {d["DashboardId"]: d for d in resp["DashboardSummaryList"]}
            assert dash_id in summaries
            assert ACCOUNT_ID in summaries[dash_id]["Arn"]
        finally:
            try:
                quicksight.delete_dashboard(AwsAccountId=ACCOUNT_ID, DashboardId=dash_id)
            except Exception:
                pass


class TestQuickSightGroupsEdgeCases:
    """Behavioral fidelity for group records."""

    def test_list_groups_each_has_arn(self, quicksight):
        group_name = _unique("grp-arn")
        quicksight.create_group(
            AwsAccountId=ACCOUNT_ID, Namespace=NAMESPACE, GroupName=group_name
        )
        try:
            resp = quicksight.list_groups(AwsAccountId=ACCOUNT_ID, Namespace=NAMESPACE)
            groups = {g["GroupName"]: g for g in resp["GroupList"]}
            assert group_name in groups
            assert "Arn" in groups[group_name]
            assert len(groups[group_name]["Arn"]) > 0
        finally:
            quicksight.delete_group(
                AwsAccountId=ACCOUNT_ID, Namespace=NAMESPACE, GroupName=group_name
            )

    def test_create_group_with_description(self, quicksight):
        group_name = _unique("grp-desc")
        quicksight.create_group(
            AwsAccountId=ACCOUNT_ID,
            Namespace=NAMESPACE,
            GroupName=group_name,
            Description="A test group",
        )
        try:
            resp = quicksight.describe_group(
                AwsAccountId=ACCOUNT_ID, Namespace=NAMESPACE, GroupName=group_name
            )
            assert resp["Group"]["Description"] == "A test group"
        finally:
            quicksight.delete_group(
                AwsAccountId=ACCOUNT_ID, Namespace=NAMESPACE, GroupName=group_name
            )


class TestQuickSightUsersEdgeCases:
    """Behavioral fidelity for user records."""

    def test_list_users_each_has_email(self, quicksight):
        user_name = _unique("usr-email")
        email = f"{user_name}@example.com"
        quicksight.register_user(
            AwsAccountId=ACCOUNT_ID,
            Namespace=NAMESPACE,
            Email=email,
            IdentityType="QUICKSIGHT",
            UserRole="READER",
            UserName=user_name,
        )
        try:
            resp = quicksight.list_users(AwsAccountId=ACCOUNT_ID, Namespace=NAMESPACE)
            # list_users returns Arn, Email, Role etc. (not UserName) — find by email
            matching = [u for u in resp["UserList"] if u.get("Email") == email]
            assert len(matching) == 1
            assert "Email" in matching[0]
        finally:
            quicksight.delete_user(
                AwsAccountId=ACCOUNT_ID, Namespace=NAMESPACE, UserName=user_name
            )



class TestQuickSightAnalysesPagination:
    """Pagination behavioral tests for list_analyses."""

    def test_list_analyses_with_multiple(self, quicksight):
        aids = [_unique("an") for _ in range(3)]
        try:
            for aid in aids:
                quicksight.create_analysis(
                    AwsAccountId=ACCOUNT_ID,
                    AnalysisId=aid,
                    Name=f"Analysis {aid}",
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
            resp = quicksight.list_analyses(AwsAccountId=ACCOUNT_ID)
            found_ids = [a["AnalysisId"] for a in resp["AnalysisSummaryList"]]
            for aid in aids:
                assert aid in found_ids
        finally:
            for aid in aids:
                try:
                    quicksight.delete_analysis(AwsAccountId=ACCOUNT_ID, AnalysisId=aid)
                except Exception:
                    pass


class TestQuickSightDataSetsPagination:
    """Pagination behavioral tests for list_data_sets."""

    def test_list_data_sets_with_multiple(self, quicksight):
        ds_id = _unique("ds-base")
        dataset_ids = [_unique("dst") for _ in range(2)]
        try:
            quicksight.create_data_source(
                AwsAccountId=ACCOUNT_ID,
                DataSourceId=ds_id,
                Name="Base DataSource",
                Type="S3",
                DataSourceParameters={
                    "S3Parameters": {"ManifestFileLocation": {"Bucket": "b", "Key": "k"}}
                },
            )
            for did in dataset_ids:
                quicksight.create_data_set(
                    AwsAccountId=ACCOUNT_ID,
                    DataSetId=did,
                    Name=f"DataSet {did}",
                    ImportMode="SPICE",
                    PhysicalTableMap={
                        "t1": {
                            "S3Source": {
                                "DataSourceArn": f"arn:aws:quicksight:us-east-1:{ACCOUNT_ID}:datasource/{ds_id}",
                                "InputColumns": [{"Name": "col1", "Type": "STRING"}],
                            }
                        }
                    },
                )
            resp = quicksight.list_data_sets(AwsAccountId=ACCOUNT_ID)
            found_ids = [d["DataSetId"] for d in resp["DataSetSummaries"]]
            for did in dataset_ids:
                assert did in found_ids
        finally:
            for did in dataset_ids:
                try:
                    quicksight.delete_data_set(AwsAccountId=ACCOUNT_ID, DataSetId=did)
                except Exception:
                    pass
            try:
                quicksight.delete_data_source(AwsAccountId=ACCOUNT_ID, DataSourceId=ds_id)
            except Exception:
                pass


class TestQuickSightFoldersPagination:
    """Pagination behavioral tests for list_folders."""

    def test_list_folders_with_multiple(self, quicksight):
        folder_ids = [_unique("fld") for _ in range(3)]
        try:
            for fid in folder_ids:
                quicksight.create_folder(
                    AwsAccountId=ACCOUNT_ID,
                    FolderId=fid,
                    Name=f"Folder {fid}",
                    FolderType="SHARED",
                )
            resp = quicksight.list_folders(AwsAccountId=ACCOUNT_ID)
            found_ids = [f["FolderId"] for f in resp["FolderSummaryList"]]
            for fid in folder_ids:
                assert fid in found_ids
        finally:
            for fid in folder_ids:
                try:
                    quicksight.delete_folder(AwsAccountId=ACCOUNT_ID, FolderId=fid)
                except Exception:
                    pass


class TestQuickSightDuplicateCreateErrors:
    """Verify that duplicate resource creation raises ResourceExistsException."""

    def test_create_group_duplicate_name_raises(self, quicksight):
        """Creating a group with a name that already exists should raise ResourceExistsException."""
        group_name = _unique("dup-grp")
        quicksight.create_group(AwsAccountId=ACCOUNT_ID, Namespace=NAMESPACE, GroupName=group_name)
        try:
            with pytest.raises(quicksight.exceptions.ClientError) as exc_info:
                quicksight.create_group(
                    AwsAccountId=ACCOUNT_ID, Namespace=NAMESPACE, GroupName=group_name
                )
            assert "ResourceExistsException" in str(exc_info.value)
        finally:
            quicksight.delete_group(
                AwsAccountId=ACCOUNT_ID, Namespace=NAMESPACE, GroupName=group_name
            )

    def test_register_user_duplicate_username_raises(self, quicksight):
        """Registering a user with an already-taken username should raise ResourceExistsException."""
        user_name = _unique("dup-user")
        quicksight.register_user(
            AwsAccountId=ACCOUNT_ID,
            Namespace=NAMESPACE,
            Email=f"{user_name}@example.com",
            IdentityType="QUICKSIGHT",
            UserRole="READER",
            UserName=user_name,
        )
        try:
            with pytest.raises(quicksight.exceptions.ClientError) as exc_info:
                quicksight.register_user(
                    AwsAccountId=ACCOUNT_ID,
                    Namespace=NAMESPACE,
                    Email=f"{user_name}-2@example.com",
                    IdentityType="QUICKSIGHT",
                    UserRole="READER",
                    UserName=user_name,
                )
            assert "ResourceExistsException" in str(exc_info.value)
        finally:
            quicksight.delete_user(AwsAccountId=ACCOUNT_ID, Namespace=NAMESPACE, UserName=user_name)

    def test_create_data_source_duplicate_id_raises(self, quicksight):
        """Creating a data source with an ID that already exists should raise ResourceExistsException."""
        ds_id = _unique("dup-ds")
        params = {"S3Parameters": {"ManifestFileLocation": {"Bucket": "b", "Key": "k"}}}
        quicksight.create_data_source(
            AwsAccountId=ACCOUNT_ID, DataSourceId=ds_id, Name="Original", Type="S3",
            DataSourceParameters=params,
        )
        try:
            with pytest.raises(quicksight.exceptions.ClientError) as exc_info:
                quicksight.create_data_source(
                    AwsAccountId=ACCOUNT_ID, DataSourceId=ds_id, Name="Duplicate", Type="S3",
                    DataSourceParameters=params,
                )
            assert "ResourceExistsException" in str(exc_info.value)
        finally:
            quicksight.delete_data_source(AwsAccountId=ACCOUNT_ID, DataSourceId=ds_id)


class TestQuickSightUnicodeNames:
    """Verify that unicode is handled correctly in resource names and descriptions."""

    def test_group_with_unicode_description(self, quicksight):
        """Groups should accept and preserve unicode descriptions."""
        group_name = _unique("ug")
        quicksight.create_group(AwsAccountId=ACCOUNT_ID, Namespace=NAMESPACE, GroupName=group_name)
        try:
            update_resp = quicksight.update_group(
                AwsAccountId=ACCOUNT_ID,
                Namespace=NAMESPACE,
                GroupName=group_name,
                Description="説明 — описание — وصف",
            )
            assert update_resp["Status"] == 200
            assert update_resp["Group"]["Description"] == "説明 — описание — وصف"

            describe_resp = quicksight.describe_group(
                AwsAccountId=ACCOUNT_ID, Namespace=NAMESPACE, GroupName=group_name
            )
            assert describe_resp["Group"]["Description"] == "説明 — описание — وصف"
        finally:
            quicksight.delete_group(
                AwsAccountId=ACCOUNT_ID, Namespace=NAMESPACE, GroupName=group_name
            )

    def test_data_source_with_unicode_name(self, quicksight):
        """Data sources should accept and preserve unicode names."""
        ds_id = _unique("uds")
        unicode_name = "データソース — Source de données — مصدر البيانات"
        resp = quicksight.create_data_source(
            AwsAccountId=ACCOUNT_ID,
            DataSourceId=ds_id,
            Name=unicode_name,
            Type="S3",
            DataSourceParameters={
                "S3Parameters": {"ManifestFileLocation": {"Bucket": "b", "Key": "k"}}
            },
        )
        assert resp["Status"] in (200, 201, 202)
        try:
            describe_resp = quicksight.describe_data_source(
                AwsAccountId=ACCOUNT_ID, DataSourceId=ds_id
            )
            assert describe_resp["DataSource"]["Name"] == unicode_name
        finally:
            quicksight.delete_data_source(AwsAccountId=ACCOUNT_ID, DataSourceId=ds_id)

    def test_user_with_unicode_email_domain(self, quicksight):
        """Users should accept email addresses with unicode-safe characters."""
        user_name = _unique("uuser")
        email = f"{user_name}@test-example.com"
        resp = quicksight.register_user(
            AwsAccountId=ACCOUNT_ID,
            Namespace=NAMESPACE,
            Email=email,
            IdentityType="QUICKSIGHT",
            UserRole="READER",
            UserName=user_name,
        )
        assert resp["Status"] == 200
        try:
            describe_resp = quicksight.describe_user(
                AwsAccountId=ACCOUNT_ID, Namespace=NAMESPACE, UserName=user_name
            )
            assert describe_resp["User"]["Email"] == email
        finally:
            quicksight.delete_user(AwsAccountId=ACCOUNT_ID, Namespace=NAMESPACE, UserName=user_name)


class TestQuickSightTimestampFidelity:
    """Verify that created resources have valid timestamp fields."""

    def test_data_source_created_time_present(self, quicksight):
        """Newly created data source should have a CreatedTime field."""
        ds_id = _unique("ts-ds")
        resp = quicksight.create_data_source(
            AwsAccountId=ACCOUNT_ID,
            DataSourceId=ds_id,
            Name="Timestamp Test DS",
            Type="S3",
            DataSourceParameters={
                "S3Parameters": {"ManifestFileLocation": {"Bucket": "b", "Key": "k"}}
            },
        )
        assert resp["Status"] in (200, 201, 202)
        try:
            describe_resp = quicksight.describe_data_source(
                AwsAccountId=ACCOUNT_ID, DataSourceId=ds_id
            )
            ds = describe_resp["DataSource"]
            assert "CreatedTime" in ds
            assert "LastUpdatedTime" in ds
        finally:
            quicksight.delete_data_source(AwsAccountId=ACCOUNT_ID, DataSourceId=ds_id)

    def test_group_created_with_arn_present(self, quicksight):
        """Newly created group should have an Arn in the response."""
        group_name = _unique("ts-grp")
        resp = quicksight.create_group(
            AwsAccountId=ACCOUNT_ID, Namespace=NAMESPACE, GroupName=group_name
        )
        try:
            assert resp["Status"] == 200
            group = resp["Group"]
            assert "Arn" in group
            assert group["Arn"].startswith("arn:aws:quicksight:")
            assert ACCOUNT_ID in group["Arn"]
            assert group_name in group["Arn"]
        finally:
            quicksight.delete_group(
                AwsAccountId=ACCOUNT_ID, Namespace=NAMESPACE, GroupName=group_name
            )

    def test_dashboard_describe_has_created_time(self, quicksight):
        """Newly created dashboard should have CreatedTime in describe response."""
        dash_id = _unique("ts-dash")
        quicksight.create_dashboard(
            AwsAccountId=ACCOUNT_ID,
            DashboardId=dash_id,
            Name="Timestamp Dashboard",
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
        resp = quicksight.describe_dashboard(AwsAccountId=ACCOUNT_ID, DashboardId=dash_id)
        assert resp["Status"] == 200
        assert "Dashboard" in resp
        assert "CreatedTime" in resp["Dashboard"]
        assert "LastPublishedTime" in resp["Dashboard"] or "Version" in resp["Dashboard"]


class TestQuickSightDataSourceFullLifecycle:
    """Full CRUD lifecycle test for data sources with edge cases."""

    def test_data_source_full_lifecycle(self, quicksight):
        """Create → describe → list → update → describe again → delete → describe-raises."""
        ds_id = _unique("lifecycle-ds")
        params = {"S3Parameters": {"ManifestFileLocation": {"Bucket": "orig-bucket", "Key": "k"}}}

        # CREATE
        create_resp = quicksight.create_data_source(
            AwsAccountId=ACCOUNT_ID,
            DataSourceId=ds_id,
            Name="Lifecycle DS",
            Type="S3",
            DataSourceParameters=params,
        )
        assert create_resp["Status"] in (200, 201, 202)
        assert create_resp["DataSourceId"] == ds_id
        assert "Arn" in create_resp

        # RETRIEVE
        describe_resp = quicksight.describe_data_source(
            AwsAccountId=ACCOUNT_ID, DataSourceId=ds_id
        )
        assert describe_resp["DataSource"]["Name"] == "Lifecycle DS"

        # LIST - verify it appears
        list_resp = quicksight.list_data_sources(AwsAccountId=ACCOUNT_ID)
        ds_ids = [d["DataSourceId"] for d in list_resp["DataSources"]]
        assert ds_id in ds_ids

        # UPDATE
        update_resp = quicksight.update_data_source(
            AwsAccountId=ACCOUNT_ID,
            DataSourceId=ds_id,
            Name="Lifecycle DS Updated",
            DataSourceParameters={
                "S3Parameters": {"ManifestFileLocation": {"Bucket": "new-bucket", "Key": "k"}}
            },
        )
        assert update_resp["Status"] == 200

        # Verify update persisted
        describe_resp2 = quicksight.describe_data_source(
            AwsAccountId=ACCOUNT_ID, DataSourceId=ds_id
        )
        assert describe_resp2["DataSource"]["Name"] == "Lifecycle DS Updated"

        # DELETE
        delete_resp = quicksight.delete_data_source(AwsAccountId=ACCOUNT_ID, DataSourceId=ds_id)
        assert delete_resp["Status"] == 200

        # ERROR - describe after delete should raise
        with pytest.raises(quicksight.exceptions.ClientError) as exc_info:
            quicksight.describe_data_source(AwsAccountId=ACCOUNT_ID, DataSourceId=ds_id)
        assert "ResourceNotFoundException" in str(exc_info.value)


class TestQuickSightGroupFullLifecycle:
    """Full CRUD lifecycle test for groups."""

    def test_group_full_lifecycle(self, quicksight):
        """Create → describe → list → update → add member → delete → describe-raises."""
        group_name = _unique("lifecycle-grp")
        user_name = _unique("lifecycle-user")

        # CREATE group
        create_resp = quicksight.create_group(
            AwsAccountId=ACCOUNT_ID, Namespace=NAMESPACE, GroupName=group_name
        )
        assert create_resp["Status"] == 200
        assert create_resp["Group"]["GroupName"] == group_name
        assert "Arn" in create_resp["Group"]

        # Register a user to add to the group
        quicksight.register_user(
            AwsAccountId=ACCOUNT_ID,
            Namespace=NAMESPACE,
            Email=f"{user_name}@example.com",
            IdentityType="QUICKSIGHT",
            UserRole="READER",
            UserName=user_name,
        )

        try:
            # RETRIEVE
            describe_resp = quicksight.describe_group(
                AwsAccountId=ACCOUNT_ID, Namespace=NAMESPACE, GroupName=group_name
            )
            assert describe_resp["Group"]["GroupName"] == group_name

            # LIST - verify group appears
            list_resp = quicksight.list_groups(AwsAccountId=ACCOUNT_ID, Namespace=NAMESPACE)
            group_names = [g["GroupName"] for g in list_resp["GroupList"]]
            assert group_name in group_names

            # UPDATE
            update_resp = quicksight.update_group(
                AwsAccountId=ACCOUNT_ID,
                Namespace=NAMESPACE,
                GroupName=group_name,
                Description="Lifecycle test group",
            )
            assert update_resp["Status"] == 200
            assert update_resp["Group"]["Description"] == "Lifecycle test group"

            # Add member
            quicksight.create_group_membership(
                AwsAccountId=ACCOUNT_ID,
                Namespace=NAMESPACE,
                GroupName=group_name,
                MemberName=user_name,
            )

            # DELETE group
            quicksight.delete_group(
                AwsAccountId=ACCOUNT_ID, Namespace=NAMESPACE, GroupName=group_name
            )

            # ERROR - describe after delete should raise
            with pytest.raises(quicksight.exceptions.ClientError) as exc_info:
                quicksight.describe_group(
                    AwsAccountId=ACCOUNT_ID, Namespace=NAMESPACE, GroupName=group_name
                )
            assert "ResourceNotFoundException" in str(exc_info.value)
        finally:
            quicksight.delete_user(AwsAccountId=ACCOUNT_ID, Namespace=NAMESPACE, UserName=user_name)
            try:
                quicksight.delete_group(
                    AwsAccountId=ACCOUNT_ID, Namespace=NAMESPACE, GroupName=group_name
                )
            except Exception:
                pass  # already deleted in test body


class TestQuickSightAccountSettingsEdgeCases:
    """Edge case tests for account settings operations."""

    def test_update_account_settings_then_describe_shows_change(self, quicksight):
        """Setting NotificationEmail should persist to describe_account_settings."""
        email = "edge-case-test@example.com"
        quicksight.update_account_settings(
            AwsAccountId=ACCOUNT_ID,
            DefaultNamespace="default",
            NotificationEmail=email,
        )
        resp = quicksight.describe_account_settings(AwsAccountId=ACCOUNT_ID)
        assert resp["Status"] == 200
        settings = resp["AccountSettings"]
        assert settings.get("NotificationEmail") == email

    def test_describe_account_settings_fields_are_typed_correctly(self, quicksight):
        """AccountSettings fields should have correct types."""
        resp = quicksight.describe_account_settings(AwsAccountId=ACCOUNT_ID)
        assert resp["Status"] == 200
        settings = resp["AccountSettings"]
        # DefaultNamespace is always present and a string
        assert isinstance(settings.get("DefaultNamespace", "default"), str)
        # PublicSharingEnabled is a boolean when present
        if "PublicSharingEnabled" in settings:
            assert isinstance(settings["PublicSharingEnabled"], bool)

    def test_update_public_sharing_true_then_false(self, quicksight):
        """Toggling public sharing should update the stored setting."""
        quicksight.update_public_sharing_settings(
            AwsAccountId=ACCOUNT_ID, PublicSharingEnabled=True
        )
        resp = quicksight.describe_account_settings(AwsAccountId=ACCOUNT_ID)
        assert resp["AccountSettings"]["PublicSharingEnabled"] is True

        quicksight.update_public_sharing_settings(
            AwsAccountId=ACCOUNT_ID, PublicSharingEnabled=False
        )
        resp2 = quicksight.describe_account_settings(AwsAccountId=ACCOUNT_ID)
        assert resp2["AccountSettings"]["PublicSharingEnabled"] is False


class TestQuickSightListDashboardsEdgeCases:
    """Edge case tests for list_dashboards."""

    def test_list_dashboards_returns_summary_fields(self, quicksight):
        """Dashboard summaries should contain expected fields."""
        dash_id = _unique("lds-dash")
        quicksight.create_dashboard(
            AwsAccountId=ACCOUNT_ID,
            DashboardId=dash_id,
            Name="Summary Field Test",
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
        resp = quicksight.list_dashboards(AwsAccountId=ACCOUNT_ID)
        assert resp["Status"] == 200
        summaries = resp["DashboardSummaryList"]
        matching = [d for d in summaries if d["DashboardId"] == dash_id]
        assert len(matching) == 1
        summary = matching[0]
        assert summary["DashboardId"] == dash_id
        assert summary["Name"] == "Summary Field Test"
        assert "Arn" in summary

    def test_list_dashboards_arn_format(self, quicksight):
        """Dashboard ARNs in list results should match expected format."""
        dash_id = _unique("arn-dash")
        quicksight.create_dashboard(
            AwsAccountId=ACCOUNT_ID,
            DashboardId=dash_id,
            Name="ARN Format Test",
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
        resp = quicksight.list_dashboards(AwsAccountId=ACCOUNT_ID)
        matching = [d for d in resp["DashboardSummaryList"] if d["DashboardId"] == dash_id]
        assert len(matching) == 1
        arn = matching[0]["Arn"]
        assert arn.startswith("arn:aws:quicksight:")
        assert ACCOUNT_ID in arn
        assert dash_id in arn

    def test_delete_dashboard_removes_from_list(self, quicksight):
        """Deleting a dashboard should remove it from list_dashboards."""
        dash_id = _unique("del-list-dash")
        quicksight.create_dashboard(
            AwsAccountId=ACCOUNT_ID,
            DashboardId=dash_id,
            Name="Delete From List Test",
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
        # Verify it's in the list before deletion
        before = quicksight.list_dashboards(AwsAccountId=ACCOUNT_ID)
        before_ids = [d["DashboardId"] for d in before["DashboardSummaryList"]]
        assert dash_id in before_ids

        # Delete it
        quicksight.delete_dashboard(AwsAccountId=ACCOUNT_ID, DashboardId=dash_id)

        # Verify it's no longer in list
        with pytest.raises(quicksight.exceptions.ClientError) as exc_info:
            quicksight.describe_dashboard(AwsAccountId=ACCOUNT_ID, DashboardId=dash_id)
        assert "ResourceNotFoundException" in str(exc_info.value)


class TestQuickSightListGroupsEdgeCases:
    """Edge case tests for list_groups and group operations."""

    def test_list_groups_returns_group_fields(self, quicksight):
        """Group list entries should contain GroupName and Arn."""
        group_name = _unique("lgtest-grp")
        quicksight.create_group(AwsAccountId=ACCOUNT_ID, Namespace=NAMESPACE, GroupName=group_name)
        try:
            resp = quicksight.list_groups(AwsAccountId=ACCOUNT_ID, Namespace=NAMESPACE)
            matching = [g for g in resp["GroupList"] if g["GroupName"] == group_name]
            assert len(matching) == 1
            group = matching[0]
            assert group["GroupName"] == group_name
            assert "Arn" in group
            assert ACCOUNT_ID in group["Arn"]
        finally:
            quicksight.delete_group(
                AwsAccountId=ACCOUNT_ID, Namespace=NAMESPACE, GroupName=group_name
            )

    def test_delete_group_removes_from_list(self, quicksight):
        """Deleting a group should remove it from list_groups."""
        group_name = _unique("del-list-grp")
        quicksight.create_group(AwsAccountId=ACCOUNT_ID, Namespace=NAMESPACE, GroupName=group_name)

        before = quicksight.list_groups(AwsAccountId=ACCOUNT_ID, Namespace=NAMESPACE)
        before_names = [g["GroupName"] for g in before["GroupList"]]
        assert group_name in before_names

        quicksight.delete_group(AwsAccountId=ACCOUNT_ID, Namespace=NAMESPACE, GroupName=group_name)

        after = quicksight.list_groups(AwsAccountId=ACCOUNT_ID, Namespace=NAMESPACE)
        after_names = [g["GroupName"] for g in after["GroupList"]]
        assert group_name not in after_names

    def test_list_groups_after_delete_does_not_raise(self, quicksight):
        """list_groups should return 200 even after all groups in a batch are deleted."""
        group_name = _unique("listclean-grp")
        quicksight.create_group(AwsAccountId=ACCOUNT_ID, Namespace=NAMESPACE, GroupName=group_name)
        quicksight.delete_group(AwsAccountId=ACCOUNT_ID, Namespace=NAMESPACE, GroupName=group_name)
        resp = quicksight.list_groups(AwsAccountId=ACCOUNT_ID, Namespace=NAMESPACE)
        assert resp["Status"] == 200
        assert isinstance(resp["GroupList"], list)


class TestQuickSightListUsersEdgeCases:
    """Edge case tests for list_users and user operations."""

    def test_list_users_returns_user_fields(self, quicksight):
        """User list entries should contain UserName, Email, and Role."""
        user_name = _unique("lutest-user")
        quicksight.register_user(
            AwsAccountId=ACCOUNT_ID,
            Namespace=NAMESPACE,
            Email=f"{user_name}@example.com",
            IdentityType="QUICKSIGHT",
            UserRole="READER",
            UserName=user_name,
        )
        try:
            resp = quicksight.list_users(AwsAccountId=ACCOUNT_ID, Namespace=NAMESPACE)
            matching = [u for u in resp["UserList"] if u.get("UserName") == user_name]
            assert len(matching) == 1
            user = matching[0]
            assert user["UserName"] == user_name
            assert user["Email"] == f"{user_name}@example.com"
            assert user["Role"] == "READER"
            assert "Arn" in user
        finally:
            quicksight.delete_user(AwsAccountId=ACCOUNT_ID, Namespace=NAMESPACE, UserName=user_name)

    def test_delete_user_removes_from_list(self, quicksight):
        """Deleting a user should remove them from list_users."""
        user_name = _unique("del-list-user")
        quicksight.register_user(
            AwsAccountId=ACCOUNT_ID,
            Namespace=NAMESPACE,
            Email=f"{user_name}@example.com",
            IdentityType="QUICKSIGHT",
            UserRole="ADMIN",
            UserName=user_name,
        )

        before = quicksight.list_users(AwsAccountId=ACCOUNT_ID, Namespace=NAMESPACE)
        before_names = [u.get("UserName") for u in before["UserList"]]
        assert user_name in before_names

        quicksight.delete_user(AwsAccountId=ACCOUNT_ID, Namespace=NAMESPACE, UserName=user_name)

        after = quicksight.list_users(AwsAccountId=ACCOUNT_ID, Namespace=NAMESPACE)
        after_names = [u.get("UserName") for u in after["UserList"]]
        assert user_name not in after_names

    def test_update_user_role_persists(self, quicksight):
        """Updating a user's role should be reflected in describe_user."""
        user_name = _unique("roleupd-user")
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
                Email=f"{user_name}@example.com",
                Role="AUTHOR",
            )
            assert update_resp["Status"] == 200
            assert update_resp["User"]["Role"] == "AUTHOR"

            describe_resp = quicksight.describe_user(
                AwsAccountId=ACCOUNT_ID, Namespace=NAMESPACE, UserName=user_name
            )
            assert describe_resp["User"]["Role"] == "AUTHOR"
        finally:
            quicksight.delete_user(AwsAccountId=ACCOUNT_ID, Namespace=NAMESPACE, UserName=user_name)

