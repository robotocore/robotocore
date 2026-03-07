"""Compatibility tests for AWS Lake Formation service."""

import uuid

import pytest

from .conftest import make_client


@pytest.fixture
def lakeformation():
    return make_client("lakeformation")


@pytest.fixture
def unique_suffix():
    return uuid.uuid4().hex[:8]


class TestLakeFormationListResources:
    def test_list_resources_returns_list(self, lakeformation):
        resp = lakeformation.list_resources()
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200
        assert "ResourceInfoList" in resp


class TestLakeFormationGetDataLakeSettings:
    def test_get_data_lake_settings(self, lakeformation):
        resp = lakeformation.get_data_lake_settings()
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200
        assert "DataLakeSettings" in resp


class TestLakeFormationPutDataLakeSettings:
    def test_put_data_lake_settings(self, lakeformation):
        admin_arn = f"arn:aws:iam::123456789012:user/admin-{uuid.uuid4().hex[:8]}"
        resp = lakeformation.put_data_lake_settings(
            DataLakeSettings={
                "DataLakeAdmins": [
                    {"DataLakePrincipalIdentifier": admin_arn},
                ],
            }
        )
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200

    def test_put_then_get_data_lake_settings(self, lakeformation):
        admin_arn = f"arn:aws:iam::123456789012:user/admin-{uuid.uuid4().hex[:8]}"
        lakeformation.put_data_lake_settings(
            DataLakeSettings={
                "DataLakeAdmins": [
                    {"DataLakePrincipalIdentifier": admin_arn},
                ],
            }
        )
        resp = lakeformation.get_data_lake_settings()
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200
        admins = resp["DataLakeSettings"].get("DataLakeAdmins", [])
        admin_arns = [a["DataLakePrincipalIdentifier"] for a in admins]
        assert admin_arn in admin_arns


class TestLakeFormationRegisterDeregisterResource:
    def test_register_resource(self, lakeformation, unique_suffix):
        resource_arn = f"arn:aws:s3:::test-bucket-{unique_suffix}"
        resp = lakeformation.register_resource(ResourceArn=resource_arn)
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200

    def test_register_then_deregister(self, lakeformation, unique_suffix):
        resource_arn = f"arn:aws:s3:::test-bucket-{unique_suffix}"
        lakeformation.register_resource(ResourceArn=resource_arn)
        resp = lakeformation.deregister_resource(ResourceArn=resource_arn)
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200

    def test_register_appears_in_list(self, lakeformation, unique_suffix):
        resource_arn = f"arn:aws:s3:::test-bucket-{unique_suffix}"
        lakeformation.register_resource(ResourceArn=resource_arn)
        resp = lakeformation.list_resources()
        arns = [r["ResourceArn"] for r in resp["ResourceInfoList"]]
        assert resource_arn in arns
        # cleanup
        lakeformation.deregister_resource(ResourceArn=resource_arn)


class TestLakeFormationPermissions:
    def test_list_permissions(self, lakeformation):
        resp = lakeformation.list_permissions()
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200
        assert "PrincipalResourcePermissions" in resp

    def test_grant_permissions(self, lakeformation, unique_suffix):
        principal_arn = f"arn:aws:iam::123456789012:user/user-{unique_suffix}"
        resp = lakeformation.grant_permissions(
            Principal={"DataLakePrincipalIdentifier": principal_arn},
            Resource={"Database": {"Name": f"db-{unique_suffix}"}},
            Permissions=["ALL"],
        )
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200

    def test_grant_then_revoke_permissions(self, lakeformation, unique_suffix):
        principal_arn = f"arn:aws:iam::123456789012:user/user-{unique_suffix}"
        db_name = f"db-{unique_suffix}"
        lakeformation.grant_permissions(
            Principal={"DataLakePrincipalIdentifier": principal_arn},
            Resource={"Database": {"Name": db_name}},
            Permissions=["ALL"],
        )
        resp = lakeformation.revoke_permissions(
            Principal={"DataLakePrincipalIdentifier": principal_arn},
            Resource={"Database": {"Name": db_name}},
            Permissions=["ALL"],
        )
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200

    def test_grant_permissions_appears_in_list(self, lakeformation, unique_suffix):
        principal_arn = f"arn:aws:iam::123456789012:user/user-{unique_suffix}"
        db_name = f"db-{unique_suffix}"
        lakeformation.grant_permissions(
            Principal={"DataLakePrincipalIdentifier": principal_arn},
            Resource={"Database": {"Name": db_name}},
            Permissions=["ALL"],
        )
        resp = lakeformation.list_permissions()
        principals = [
            p["Principal"]["DataLakePrincipalIdentifier"]
            for p in resp["PrincipalResourcePermissions"]
        ]
        assert principal_arn in principals
