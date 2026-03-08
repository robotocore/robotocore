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
