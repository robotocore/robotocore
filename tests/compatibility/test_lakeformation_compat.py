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


class TestLakeformationAutoCoverage:
    """Auto-generated coverage tests for lakeformation."""

    @pytest.fixture
    def client(self):
        return make_client("lakeformation")

    def test_list_data_cells_filter(self, client):
        """ListDataCellsFilter returns a response."""
        resp = client.list_data_cells_filter()
        assert "DataCellsFilters" in resp

    def test_list_lf_tags(self, client):
        """ListLFTags returns a response."""
        resp = client.list_lf_tags()
        assert "LFTags" in resp

    def test_get_lf_tag(self, client):
        """GetLFTag returns tag details after creation."""
        tag_key = f"tag-{uuid.uuid4().hex[:8]}"
        client.create_lf_tag(TagKey=tag_key, TagValues=["val1", "val2"])
        try:
            resp = client.get_lf_tag(TagKey=tag_key)
            assert resp["TagKey"] == tag_key
            assert "TagValues" in resp
        finally:
            client.delete_lf_tag(TagKey=tag_key)

    def test_delete_lf_tag(self, client):
        """DeleteLFTag removes a tag."""
        tag_key = f"tag-{uuid.uuid4().hex[:8]}"
        client.create_lf_tag(TagKey=tag_key, TagValues=["v1"])
        resp = client.delete_lf_tag(TagKey=tag_key)
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200
        # Verify it's gone
        resp = client.list_lf_tags()
        tag_keys = [t["TagKey"] for t in resp.get("LFTags", [])]
        assert tag_key not in tag_keys

    def test_create_lf_tag(self, client):
        """CreateLFTag creates a new LF tag."""
        tag_key = f"tag-{uuid.uuid4().hex[:8]}"
        resp = client.create_lf_tag(TagKey=tag_key, TagValues=["val1", "val2", "val3"])
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200
        # Verify via list
        list_resp = client.list_lf_tags()
        found = [t for t in list_resp.get("LFTags", []) if t["TagKey"] == tag_key]
        assert len(found) == 1
        assert set(found[0]["TagValues"]) == {"val1", "val2", "val3"}
        client.delete_lf_tag(TagKey=tag_key)

    def test_update_lf_tag(self, client):
        """UpdateLFTag adds and removes tag values."""
        tag_key = f"tag-{uuid.uuid4().hex[:8]}"
        client.create_lf_tag(TagKey=tag_key, TagValues=["a", "b", "c"])
        try:
            resp = client.update_lf_tag(
                TagKey=tag_key,
                TagValuesToDelete=["b"],
                TagValuesToAdd=["d"],
            )
            assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200
            get_resp = client.get_lf_tag(TagKey=tag_key)
            assert set(get_resp["TagValues"]) == {"a", "c", "d"}
        finally:
            client.delete_lf_tag(TagKey=tag_key)

    def test_create_lf_tag_appears_in_list(self, client):
        """Created LF tags appear in ListLFTags."""
        tag_key = f"tag-{uuid.uuid4().hex[:8]}"
        client.create_lf_tag(TagKey=tag_key, TagValues=["x"])
        try:
            resp = client.list_lf_tags()
            tag_keys = [t["TagKey"] for t in resp.get("LFTags", [])]
            assert tag_key in tag_keys
        finally:
            client.delete_lf_tag(TagKey=tag_key)


class TestLakeFormationBatchPermissions:
    """Tests for BatchGrantPermissions and BatchRevokePermissions."""

    @pytest.fixture
    def client(self):
        return make_client("lakeformation")

    def test_batch_grant_permissions(self, client):
        """BatchGrantPermissions grants multiple permissions at once."""
        suffix = uuid.uuid4().hex[:8]
        principal_arn = f"arn:aws:iam::123456789012:user/user-{suffix}"
        resp = client.batch_grant_permissions(
            Entries=[
                {
                    "Id": f"entry-{suffix}-1",
                    "Principal": {"DataLakePrincipalIdentifier": principal_arn},
                    "Resource": {"Database": {"Name": f"db1-{suffix}"}},
                    "Permissions": ["ALL"],
                },
                {
                    "Id": f"entry-{suffix}-2",
                    "Principal": {"DataLakePrincipalIdentifier": principal_arn},
                    "Resource": {"Database": {"Name": f"db2-{suffix}"}},
                    "Permissions": ["SELECT"],
                },
            ]
        )
        assert "Failures" in resp

    def test_batch_revoke_permissions(self, client):
        """BatchRevokePermissions revokes previously granted permissions."""
        suffix = uuid.uuid4().hex[:8]
        principal_arn = f"arn:aws:iam::123456789012:user/user-{suffix}"
        # Grant first
        client.batch_grant_permissions(
            Entries=[
                {
                    "Id": f"entry-{suffix}",
                    "Principal": {"DataLakePrincipalIdentifier": principal_arn},
                    "Resource": {"Database": {"Name": f"db-{suffix}"}},
                    "Permissions": ["ALL"],
                },
            ]
        )
        # Revoke
        resp = client.batch_revoke_permissions(
            Entries=[
                {
                    "Id": f"entry-{suffix}",
                    "Principal": {"DataLakePrincipalIdentifier": principal_arn},
                    "Resource": {"Database": {"Name": f"db-{suffix}"}},
                    "Permissions": ["ALL"],
                },
            ]
        )
        assert "Failures" in resp


class TestLakeFormationResourceLFTags:
    """Tests for AddLFTagsToResource, RemoveLFTagsFromResource, GetResourceLFTags."""

    @pytest.fixture
    def client(self):
        return make_client("lakeformation")

    def test_add_lf_tags_to_resource(self, client):
        """AddLFTagsToResource assigns LF tags to a database resource."""
        suffix = uuid.uuid4().hex[:8]
        tag_key = f"tag-{suffix}"
        client.create_lf_tag(TagKey=tag_key, TagValues=["v1", "v2"])
        try:
            resp = client.add_lf_tags_to_resource(
                Resource={"Database": {"Name": f"db-{suffix}"}},
                LFTags=[{"TagKey": tag_key, "TagValues": ["v1"]}],
            )
            assert "Failures" in resp
        finally:
            client.delete_lf_tag(TagKey=tag_key)

    def test_remove_lf_tags_from_resource(self, client):
        """RemoveLFTagsFromResource removes LF tags from a resource."""
        suffix = uuid.uuid4().hex[:8]
        tag_key = f"tag-{suffix}"
        client.create_lf_tag(TagKey=tag_key, TagValues=["v1"])
        try:
            # Add tags to a catalog resource (no database needed)
            resp = client.remove_lf_tags_from_resource(
                Resource={"Catalog": {}},
                LFTags=[{"TagKey": tag_key, "TagValues": ["v1"]}],
            )
            assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200
        finally:
            client.delete_lf_tag(TagKey=tag_key)


class TestLakeFormationDescribeResource:
    """Tests for DescribeResource."""

    @pytest.fixture
    def client(self):
        return make_client("lakeformation")

    def test_describe_registered_resource(self, client):
        """DescribeResource returns info for a registered resource."""
        suffix = uuid.uuid4().hex[:8]
        resource_arn = f"arn:aws:s3:::test-bucket-{suffix}"
        client.register_resource(ResourceArn=resource_arn)
        try:
            resp = client.describe_resource(ResourceArn=resource_arn)
            assert "ResourceInfo" in resp
            assert resp["ResourceInfo"]["ResourceArn"] == resource_arn
        finally:
            client.deregister_resource(ResourceArn=resource_arn)


class TestLakeFormationGetResourceLFTags:
    """Tests for GetResourceLFTags operation."""

    @pytest.fixture
    def client(self):
        return make_client("lakeformation")

    def test_get_resource_lf_tags_catalog(self, client):
        """GetResourceLFTags for the Catalog resource returns a response."""
        resp = client.get_resource_lf_tags(Resource={"Catalog": {}})
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200


class TestLakeFormationAdditionalOps:
    """Tests for additional LakeFormation operations."""

    @pytest.fixture
    def client(self):
        return make_client("lakeformation")

    def test_describe_lake_formation_identity_center_configuration(self, client):
        """DescribeLakeFormationIdentityCenterConfiguration returns catalog info."""
        resp = client.describe_lake_formation_identity_center_configuration()
        assert "CatalogId" in resp

    def test_describe_transaction_not_found(self, client):
        """DescribeTransaction with fake ID raises EntityNotFoundException."""
        from botocore.exceptions import ClientError as BotoClientError

        with pytest.raises(BotoClientError) as exc:
            client.describe_transaction(TransactionId="fake-txn-id-12345")
        assert exc.value.response["Error"]["Code"] == "EntityNotFoundException"

    def test_get_data_cells_filter_not_found(self, client):
        """GetDataCellsFilter with nonexistent filter raises EntityNotFoundException."""
        from botocore.exceptions import ClientError as BotoClientError

        with pytest.raises(BotoClientError) as exc:
            client.get_data_cells_filter(
                TableCatalogId="123456789012",
                DatabaseName="nonexistent-db",
                TableName="nonexistent-tbl",
                Name="nonexistent-filter",
            )
        assert exc.value.response["Error"]["Code"] == "EntityNotFoundException"

    def test_get_data_lake_principal(self, client):
        """GetDataLakePrincipal returns identity info."""
        resp = client.get_data_lake_principal()
        assert "Identity" in resp

    def test_get_effective_permissions_for_path(self, client):
        """GetEffectivePermissionsForPath returns permissions list."""
        resp = client.get_effective_permissions_for_path(
            ResourceArn="arn:aws:s3:::test-bucket-perms"
        )
        assert "Permissions" in resp
        assert isinstance(resp["Permissions"], list)

    def test_get_temporary_glue_table_credentials(self, client):
        """GetTemporaryGlueTableCredentials returns temp credentials."""
        resp = client.get_temporary_glue_table_credentials(
            TableArn="arn:aws:glue:us-east-1:123456789012:table/db/tbl"
        )
        assert "AccessKeyId" in resp
        assert "SecretAccessKey" in resp
        assert "SessionToken" in resp

    def test_list_transactions(self, client):
        """ListTransactions returns a list."""
        resp = client.list_transactions()
        assert "Transactions" in resp
        assert isinstance(resp["Transactions"], list)
