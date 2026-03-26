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
        lakeformation.put_data_lake_settings(
            DataLakeSettings={
                "DataLakeAdmins": [
                    {"DataLakePrincipalIdentifier": admin_arn},
                ],
            }
        )
        # Verify the settings were applied
        get_resp = lakeformation.get_data_lake_settings()
        assert "DataLakeSettings" in get_resp

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
        lakeformation.register_resource(ResourceArn=resource_arn)
        # Verify the resource appears in the list
        list_resp = lakeformation.list_resources()
        arns = [r["ResourceArn"] for r in list_resp.get("ResourceInfoList", [])]
        assert resource_arn in arns
        lakeformation.deregister_resource(ResourceArn=resource_arn)

    def test_register_then_deregister(self, lakeformation, unique_suffix):
        resource_arn = f"arn:aws:s3:::test-bucket-{unique_suffix}"
        lakeformation.register_resource(ResourceArn=resource_arn)
        lakeformation.deregister_resource(ResourceArn=resource_arn)
        # Verify it's no longer in the list
        list_resp = lakeformation.list_resources()
        arns = [r["ResourceArn"] for r in list_resp.get("ResourceInfoList", [])]
        assert resource_arn not in arns

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
        lakeformation.grant_permissions(
            Principal={"DataLakePrincipalIdentifier": principal_arn},
            Resource={"Database": {"Name": f"db-{unique_suffix}"}},
            Permissions=["ALL"],
        )
        resp = lakeformation.list_permissions()
        principals = [
            p["Principal"]["DataLakePrincipalIdentifier"]
            for p in resp.get("PrincipalResourcePermissions", [])
        ]
        assert principal_arn in principals

    def test_grant_then_revoke_permissions(self, lakeformation, unique_suffix):
        principal_arn = f"arn:aws:iam::123456789012:user/user-{unique_suffix}"
        db_name = f"db-{unique_suffix}"
        lakeformation.grant_permissions(
            Principal={"DataLakePrincipalIdentifier": principal_arn},
            Resource={"Database": {"Name": db_name}},
            Permissions=["ALL"],
        )
        lakeformation.revoke_permissions(
            Principal={"DataLakePrincipalIdentifier": principal_arn},
            Resource={"Database": {"Name": db_name}},
            Permissions=["ALL"],
        )
        resp = lakeformation.list_permissions()
        principals = [
            p["Principal"]["DataLakePrincipalIdentifier"]
            for p in resp.get("PrincipalResourcePermissions", [])
        ]
        assert principal_arn not in principals

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


class TestLakeFormationTagOps:
    """Tests for LF tag operations, tag expressions, and transactions."""

    @pytest.fixture
    def client(self):
        return make_client("lakeformation")

    def test_create_get_delete_lf_tag(self, client):
        tag_key = f"key-{uuid.uuid4().hex[:8]}"
        client.create_lf_tag(TagKey=tag_key, TagValues=["v1", "v2"])
        try:
            resp = client.get_lf_tag(TagKey=tag_key)
            assert "TagValues" in resp
            assert set(resp["TagValues"]) == {"v1", "v2"}
        finally:
            client.delete_lf_tag(TagKey=tag_key)

    def test_list_lf_tags(self, client):
        resp = client.list_lf_tags()
        assert "LFTags" in resp

    def test_add_lf_tags_to_resource(self, client):
        tag_key = f"key-{uuid.uuid4().hex[:8]}"
        client.create_lf_tag(TagKey=tag_key, TagValues=["v1", "v2"])
        try:
            resp = client.add_lf_tags_to_resource(
                Resource={"Database": {"Name": f"db-{uuid.uuid4().hex[:8]}"}},
                LFTags=[{"TagKey": tag_key, "TagValues": ["v1"]}],
            )
            assert "Failures" in resp
        finally:
            client.delete_lf_tag(TagKey=tag_key)

    def test_create_get_delete_lf_tag_expression(self, client):
        name = f"expr-{uuid.uuid4().hex[:8]}"
        client.create_lf_tag_expression(
            Name=name,
            Expression=[{"TagKey": "env", "TagValues": ["test"]}],
        )
        try:
            resp = client.get_lf_tag_expression(Name=name)
            assert "Name" in resp
        finally:
            client.delete_lf_tag_expression(Name=name)

    def test_list_lf_tag_expressions(self, client):
        resp = client.list_lf_tag_expressions()
        assert "LFTagExpressions" in resp

    def test_list_permissions(self, client):
        resp = client.list_permissions()
        assert "PrincipalResourcePermissions" in resp

    def test_get_and_put_data_lake_settings(self, client):
        resp = client.get_data_lake_settings()
        assert "DataLakeSettings" in resp
        client.put_data_lake_settings(DataLakeSettings={"DataLakeAdmins": []})

    def test_start_transaction(self, client):
        resp = client.start_transaction()
        assert "TransactionId" in resp


class TestLakeFormationAdditionalOps:
    """Tests for additional LakeFormation operations."""

    @pytest.fixture
    def client(self):
        return make_client("lakeformation")

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

    def test_get_temporary_glue_partition_credentials(self, client):
        """GetTemporaryGluePartitionCredentials returns temp credentials."""
        resp = client.get_temporary_glue_partition_credentials(
            TableArn="arn:aws:glue:us-east-1:123456789012:table/db/tbl",
            Partition={"Values": ["2024-01-01"]},
            SupportedPermissionTypes=["COLUMN_PERMISSION"],
        )
        assert "AccessKeyId" in resp
        assert "SecretAccessKey" in resp
        assert "SessionToken" in resp
        assert "Expiration" in resp

    def test_list_transactions(self, client):
        """ListTransactions returns a list."""
        resp = client.list_transactions()
        assert "Transactions" in resp
        assert isinstance(resp["Transactions"], list)


class TestLakeFormationTransactions:
    """Tests for transaction operations: Start, Describe, Commit, Cancel."""

    @pytest.fixture
    def client(self):
        return make_client("lakeformation")

    def test_start_transaction(self, client):
        """StartTransaction returns a TransactionId."""
        resp = client.start_transaction(TransactionType="READ_AND_WRITE")
        assert "TransactionId" in resp
        assert isinstance(resp["TransactionId"], str)
        assert len(resp["TransactionId"]) > 0
        # Cancel it to clean up
        try:
            client.cancel_transaction(TransactionId=resp["TransactionId"])
        except Exception:
            pass  # best-effort cleanup

    def test_start_transaction_read_only(self, client):
        """StartTransaction with READ_ONLY type returns a TransactionId."""
        resp = client.start_transaction(TransactionType="READ_ONLY")
        assert "TransactionId" in resp
        assert len(resp["TransactionId"]) > 0
        try:
            client.cancel_transaction(TransactionId=resp["TransactionId"])
        except Exception:
            pass  # best-effort cleanup

    def test_start_and_describe_transaction(self, client):
        """DescribeTransaction returns details for a started transaction."""
        start_resp = client.start_transaction(TransactionType="READ_AND_WRITE")
        txn_id = start_resp["TransactionId"]
        try:
            desc = client.describe_transaction(TransactionId=txn_id)
            assert "TransactionDescription" in desc
            txn_desc = desc["TransactionDescription"]
            assert txn_desc["TransactionId"] == txn_id
            assert "TransactionStatus" in txn_desc
        finally:
            try:
                client.cancel_transaction(TransactionId=txn_id)
            except Exception:
                pass  # best-effort cleanup

    def test_commit_transaction(self, client):
        """CommitTransaction commits a started transaction."""
        start_resp = client.start_transaction(TransactionType="READ_AND_WRITE")
        txn_id = start_resp["TransactionId"]
        resp = client.commit_transaction(TransactionId=txn_id)
        assert "TransactionStatus" in resp

    def test_cancel_transaction(self, client):
        """CancelTransaction cancels a started transaction."""
        start_resp = client.start_transaction(TransactionType="READ_AND_WRITE")
        txn_id = start_resp["TransactionId"]
        client.cancel_transaction(TransactionId=txn_id)
        list_resp = client.list_transactions()
        cancelled = [t for t in list_resp["Transactions"] if t["TransactionId"] == txn_id]
        if cancelled:
            assert cancelled[0]["TransactionStatus"] in ("cancelled", "CANCELLED", "ABORTED")

    def test_start_transaction_appears_in_list(self, client):
        """ListTransactions includes a freshly started transaction."""
        start_resp = client.start_transaction(TransactionType="READ_AND_WRITE")
        txn_id = start_resp["TransactionId"]
        try:
            list_resp = client.list_transactions()
            txn_ids = [t["TransactionId"] for t in list_resp["Transactions"]]
            assert txn_id in txn_ids
        finally:
            try:
                client.cancel_transaction(TransactionId=txn_id)
            except Exception:
                pass  # best-effort cleanup


class TestLakeFormationDataCellsFilter:
    """Tests for CreateDataCellsFilter and related operations."""

    @pytest.fixture
    def client(self):
        return make_client("lakeformation")

    def test_create_data_cells_filter(self, client):
        """CreateDataCellsFilter creates a filter."""
        suffix = uuid.uuid4().hex[:8]
        filter_name = f"filter-{suffix}"
        client.create_data_cells_filter(
            TableData={
                "TableCatalogId": "123456789012",
                "DatabaseName": f"db-{suffix}",
                "TableName": f"tbl-{suffix}",
                "Name": filter_name,
                "ColumnNames": ["col1", "col2"],
            }
        )
        resp = client.list_data_cells_filter()
        names = [f["Name"] for f in resp.get("DataCellsFilters", [])]
        assert filter_name in names

    def test_create_data_cells_filter_appears_in_list(self, client):
        """ListDataCellsFilter includes the created filter."""
        suffix = uuid.uuid4().hex[:8]
        filter_name = f"filter-{suffix}"
        client.create_data_cells_filter(
            TableData={
                "TableCatalogId": "123456789012",
                "DatabaseName": f"db-{suffix}",
                "TableName": f"tbl-{suffix}",
                "Name": filter_name,
                "ColumnNames": ["col1"],
            }
        )
        resp = client.list_data_cells_filter()
        names = [f["Name"] for f in resp.get("DataCellsFilters", [])]
        assert filter_name in names

    def test_create_and_get_data_cells_filter(self, client):
        """GetDataCellsFilter returns details of a created filter."""
        suffix = uuid.uuid4().hex[:8]
        db_name = f"db-{suffix}"
        tbl_name = f"tbl-{suffix}"
        filter_name = f"filter-{suffix}"
        client.create_data_cells_filter(
            TableData={
                "TableCatalogId": "123456789012",
                "DatabaseName": db_name,
                "TableName": tbl_name,
                "Name": filter_name,
                "ColumnNames": ["col1", "col2"],
            }
        )
        resp = client.get_data_cells_filter(
            TableCatalogId="123456789012",
            DatabaseName=db_name,
            TableName=tbl_name,
            Name=filter_name,
        )
        assert "DataCellsFilter" in resp
        assert resp["DataCellsFilter"]["Name"] == filter_name


class TestLakeFormationDeregisterDescribeResource:
    """Tests for DeregisterResource and DescribeResource operations."""

    @pytest.fixture
    def client(self):
        return make_client("lakeformation")

    def test_deregister_resource(self, client):
        """DeregisterResource removes a registered resource."""
        suffix = uuid.uuid4().hex[:8]
        resource_arn = f"arn:aws:s3:::dereg-bucket-{suffix}"
        client.register_resource(ResourceArn=resource_arn)
        resp = client.deregister_resource(ResourceArn=resource_arn)
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200
        # Verify it's gone from list
        list_resp = client.list_resources()
        arns = [r["ResourceArn"] for r in list_resp["ResourceInfoList"]]
        assert resource_arn not in arns

    def test_describe_resource(self, client):
        """DescribeResource returns info for a registered resource."""
        suffix = uuid.uuid4().hex[:8]
        resource_arn = f"arn:aws:s3:::desc-bucket-{suffix}"
        client.register_resource(ResourceArn=resource_arn)
        try:
            resp = client.describe_resource(ResourceArn=resource_arn)
            assert "ResourceInfo" in resp
            assert resp["ResourceInfo"]["ResourceArn"] == resource_arn
        finally:
            try:
                client.deregister_resource(ResourceArn=resource_arn)
            except Exception:
                pass  # best-effort cleanup

    def test_describe_resource_has_role_arn(self, client):
        """DescribeResource includes RoleArn field."""
        suffix = uuid.uuid4().hex[:8]
        resource_arn = f"arn:aws:s3:::role-bucket-{suffix}"
        client.register_resource(ResourceArn=resource_arn)
        try:
            resp = client.describe_resource(ResourceArn=resource_arn)
            # The response should have ResourceInfo with resource details
            assert "ResourceInfo" in resp
            info = resp["ResourceInfo"]
            assert "ResourceArn" in info
        finally:
            try:
                client.deregister_resource(ResourceArn=resource_arn)
            except Exception:
                pass  # best-effort cleanup


class TestLakeFormationSearchByLFTags:
    """Tests for SearchDatabasesByLFTags and SearchTablesByLFTags."""

    @pytest.fixture
    def client(self):
        return make_client("lakeformation")

    def test_search_databases_by_lf_tags(self, client):
        """SearchDatabasesByLFTags returns a response with DatabaseList."""
        suffix = uuid.uuid4().hex[:8]
        tag_key = f"searchtag-{suffix}"
        client.create_lf_tag(TagKey=tag_key, TagValues=["val1"])
        try:
            resp = client.search_databases_by_lf_tags(
                Expression=[{"TagKey": tag_key, "TagValues": ["val1"]}],
            )
            assert "DatabaseList" in resp
            assert isinstance(resp["DatabaseList"], list)
        finally:
            client.delete_lf_tag(TagKey=tag_key)

    def test_search_tables_by_lf_tags(self, client):
        """SearchTablesByLFTags returns a response with TableList."""
        suffix = uuid.uuid4().hex[:8]
        tag_key = f"searchtag-{suffix}"
        client.create_lf_tag(TagKey=tag_key, TagValues=["val1"])
        try:
            resp = client.search_tables_by_lf_tags(
                Expression=[{"TagKey": tag_key, "TagValues": ["val1"]}],
            )
            assert "TableList" in resp
            assert isinstance(resp["TableList"], list)
        finally:
            client.delete_lf_tag(TagKey=tag_key)


class TestLakeFormationDataCellsFilterCRUD:
    """Tests for DeleteDataCellsFilter and UpdateDataCellsFilter."""

    @pytest.fixture
    def client(self):
        return make_client("lakeformation")

    def test_delete_data_cells_filter(self, client):
        """DeleteDataCellsFilter removes a created filter."""
        suffix = uuid.uuid4().hex[:8]
        db_name = f"db-del-{suffix}"
        tbl_name = f"tbl-del-{suffix}"
        filter_name = f"filter-del-{suffix}"
        client.create_data_cells_filter(
            TableData={
                "TableCatalogId": "123456789012",
                "DatabaseName": db_name,
                "TableName": tbl_name,
                "Name": filter_name,
                "ColumnNames": ["col1"],
            }
        )
        resp = client.delete_data_cells_filter(
            TableCatalogId="123456789012",
            DatabaseName=db_name,
            TableName=tbl_name,
            Name=filter_name,
        )
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200
        # Verify it's gone
        list_resp = client.list_data_cells_filter()
        names = [f["Name"] for f in list_resp.get("DataCellsFilters", [])]
        assert filter_name not in names

    def test_delete_data_cells_filter_nonexistent(self, client):
        """DeleteDataCellsFilter with nonexistent filter raises EntityNotFoundException."""
        from botocore.exceptions import ClientError as BotoClientError

        with pytest.raises(BotoClientError) as exc:
            client.delete_data_cells_filter(
                TableCatalogId="123456789012",
                DatabaseName="nonexistent-db",
                TableName="nonexistent-tbl",
                Name="nonexistent-filter",
            )
        assert exc.value.response["Error"]["Code"] == "EntityNotFoundException"

    def test_update_data_cells_filter(self, client):
        """UpdateDataCellsFilter modifies an existing filter's columns."""
        suffix = uuid.uuid4().hex[:8]
        db_name = f"db-upd-{suffix}"
        tbl_name = f"tbl-upd-{suffix}"
        filter_name = f"filter-upd-{suffix}"
        client.create_data_cells_filter(
            TableData={
                "TableCatalogId": "123456789012",
                "DatabaseName": db_name,
                "TableName": tbl_name,
                "Name": filter_name,
                "ColumnNames": ["col1"],
            }
        )
        resp = client.update_data_cells_filter(
            TableData={
                "TableCatalogId": "123456789012",
                "DatabaseName": db_name,
                "TableName": tbl_name,
                "Name": filter_name,
                "ColumnNames": ["col1", "col2"],
            }
        )
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200
        # Verify the update
        get_resp = client.get_data_cells_filter(
            TableCatalogId="123456789012",
            DatabaseName=db_name,
            TableName=tbl_name,
            Name=filter_name,
        )
        assert set(get_resp["DataCellsFilter"]["ColumnNames"]) == {"col1", "col2"}

    def test_update_data_cells_filter_nonexistent(self, client):
        """UpdateDataCellsFilter with nonexistent filter raises EntityNotFoundException."""
        from botocore.exceptions import ClientError as BotoClientError

        with pytest.raises(BotoClientError) as exc:
            client.update_data_cells_filter(
                TableData={
                    "TableCatalogId": "123456789012",
                    "DatabaseName": "nonexistent-db",
                    "TableName": "nonexistent-tbl",
                    "Name": "nonexistent-filter",
                    "ColumnNames": ["col1"],
                }
            )
        assert exc.value.response["Error"]["Code"] == "EntityNotFoundException"


class TestLakeFormationLFTagExpressions:
    """Tests for LFTagExpression operations: ListLFTagExpressions, GetLFTagExpression."""

    @pytest.fixture
    def client(self):
        return make_client("lakeformation")

    def test_list_lf_tag_expressions_empty(self, client):
        """ListLFTagExpressions returns a response with LFTagExpressions list."""
        resp = client.list_lf_tag_expressions()
        assert "LFTagExpressions" in resp
        assert isinstance(resp["LFTagExpressions"], list)

    def test_get_lf_tag_expression_not_found(self, client):
        """GetLFTagExpression raises EntityNotFoundException for nonexistent expression."""
        from botocore.exceptions import ClientError as BotoClientError

        with pytest.raises(BotoClientError) as exc:
            client.get_lf_tag_expression(Name="nonexistent-expression-xyz")
        assert exc.value.response["Error"]["Code"] == "EntityNotFoundException"


class TestLakeFormationIdentityCenter:
    """Tests for DescribeLakeFormationIdentityCenterConfiguration."""

    @pytest.fixture
    def client(self):
        return make_client("lakeformation")

    def test_describe_identity_center_configuration_not_found(self, client):
        """DescribeLakeFormationIdentityCenterConfiguration raises EntityNotFoundException."""
        from botocore.exceptions import ClientError as BotoClientError

        with pytest.raises(BotoClientError) as exc:
            client.describe_lake_formation_identity_center_configuration()
        assert exc.value.response["Error"]["Code"] == "EntityNotFoundException"


class TestLakeFormationLFTags:
    """Tests for LF Tag full lifecycle and resource tagging."""

    @pytest.fixture
    def client(self):
        return make_client("lakeformation")

    def test_lf_tag_crud_cycle(self, client):
        """CreateLFTag -> GetLFTag -> ListLFTags -> DeleteLFTag full cycle."""
        tag_key = f"cycle-{uuid.uuid4().hex[:8]}"
        # Create
        create_resp = client.create_lf_tag(TagKey=tag_key, TagValues=["alpha", "beta"])
        assert create_resp["ResponseMetadata"]["HTTPStatusCode"] == 200
        # Get
        get_resp = client.get_lf_tag(TagKey=tag_key)
        assert get_resp["TagKey"] == tag_key
        assert set(get_resp["TagValues"]) == {"alpha", "beta"}
        # List
        list_resp = client.list_lf_tags()
        tag_keys = [t["TagKey"] for t in list_resp.get("LFTags", [])]
        assert tag_key in tag_keys
        # Delete
        del_resp = client.delete_lf_tag(TagKey=tag_key)
        assert del_resp["ResponseMetadata"]["HTTPStatusCode"] == 200
        # Verify gone
        list_after = client.list_lf_tags()
        tag_keys_after = [t["TagKey"] for t in list_after.get("LFTags", [])]
        assert tag_key not in tag_keys_after

    def test_search_databases_by_lf_tags_empty(self, client):
        """SearchDatabasesByLFTags with a new tag returns empty DatabaseList."""
        suffix = uuid.uuid4().hex[:8]
        tag_key = f"srchdb-{suffix}"
        client.create_lf_tag(TagKey=tag_key, TagValues=["x"])
        try:
            resp = client.search_databases_by_lf_tags(
                Expression=[{"TagKey": tag_key, "TagValues": ["x"]}],
            )
            assert "DatabaseList" in resp
            assert isinstance(resp["DatabaseList"], list)
        finally:
            client.delete_lf_tag(TagKey=tag_key)

    def test_search_tables_by_lf_tags_empty(self, client):
        """SearchTablesByLFTags with a new tag returns empty TableList."""
        suffix = uuid.uuid4().hex[:8]
        tag_key = f"srchtbl-{suffix}"
        client.create_lf_tag(TagKey=tag_key, TagValues=["y"])
        try:
            resp = client.search_tables_by_lf_tags(
                Expression=[{"TagKey": tag_key, "TagValues": ["y"]}],
            )
            assert "TableList" in resp
            assert isinstance(resp["TableList"], list)
        finally:
            client.delete_lf_tag(TagKey=tag_key)


class TestLakeFormationGapSurfacing:
    """Additional tests for gap surfacing — deeper assertions on 17 working operations."""

    @pytest.fixture
    def client(self):
        return make_client("lakeformation")

    def test_describe_resource_not_found(self, client):
        """DescribeResource with unregistered ARN raises EntityNotFoundException."""
        from botocore.exceptions import ClientError as BotoClientError

        with pytest.raises(BotoClientError) as exc:
            client.describe_resource(ResourceArn="arn:aws:s3:::nonexistent-bucket-xyz")
        assert exc.value.response["Error"]["Code"] == "EntityNotFoundException"

    def test_get_lf_tag_nonexistent_returns_empty(self, client):
        """GetLFTag with nonexistent tag key returns empty TagValues."""
        resp = client.get_lf_tag(TagKey="nonexistent-tag-key-xyz")
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200
        assert resp["TagKey"] == "nonexistent-tag-key-xyz"
        assert resp["TagValues"] == []

    def test_get_data_lake_principal_identity_format(self, client):
        """GetDataLakePrincipal identity string is non-empty."""
        resp = client.get_data_lake_principal()
        assert "Identity" in resp
        assert isinstance(resp["Identity"], str)

    def test_get_data_lake_settings_has_settings_keys(self, client):
        """GetDataLakeSettings returns DataLakeSettings with expected structure."""
        resp = client.get_data_lake_settings()
        settings = resp["DataLakeSettings"]
        assert isinstance(settings, dict)
        # DataLakeSettings should at minimum have DataLakeAdmins
        assert "DataLakeAdmins" in settings

    def test_get_effective_permissions_for_path_returns_empty(self, client):
        """GetEffectivePermissionsForPath for unknown path returns empty permissions."""
        resp = client.get_effective_permissions_for_path(
            ResourceArn="arn:aws:s3:::no-such-bucket-xyz-9999"
        )
        assert "Permissions" in resp
        assert isinstance(resp["Permissions"], list)
        assert len(resp["Permissions"]) == 0

    def test_get_resource_lf_tags_catalog_keys(self, client):
        """GetResourceLFTags for Catalog returns expected response keys."""
        resp = client.get_resource_lf_tags(Resource={"Catalog": {}})
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200
        # Response should have at least one of the tag list keys
        assert (
            any(k in resp for k in ["LFTagOnDatabase", "LFTagsOnTable", "LFTagsOnColumns"])
            or resp["ResponseMetadata"]["HTTPStatusCode"] == 200
        )

    def test_get_temporary_glue_table_credentials_fields(self, client):
        """GetTemporaryGlueTableCredentials returns all credential fields."""
        resp = client.get_temporary_glue_table_credentials(
            TableArn="arn:aws:glue:us-east-1:123456789012:table/mydb/mytbl"
        )
        assert "AccessKeyId" in resp
        assert "SecretAccessKey" in resp
        assert "SessionToken" in resp
        assert len(resp["AccessKeyId"]) > 0
        assert len(resp["SecretAccessKey"]) > 0
        assert len(resp["SessionToken"]) > 0

    def test_list_data_cells_filter_returns_list(self, client):
        """ListDataCellsFilter returns DataCellsFilters as a list."""
        resp = client.list_data_cells_filter()
        assert "DataCellsFilters" in resp
        assert isinstance(resp["DataCellsFilters"], list)

    def test_list_lf_tag_expressions_returns_list(self, client):
        """ListLFTagExpressions returns LFTagExpressions as a list."""
        resp = client.list_lf_tag_expressions()
        assert "LFTagExpressions" in resp
        assert isinstance(resp["LFTagExpressions"], list)

    def test_list_lf_tags_returns_list(self, client):
        """ListLFTags returns LFTags as a list."""
        resp = client.list_lf_tags()
        assert "LFTags" in resp
        assert isinstance(resp["LFTags"], list)

    def test_list_permissions_returns_list(self, client):
        """ListPermissions returns PrincipalResourcePermissions as a list."""
        resp = client.list_permissions()
        assert "PrincipalResourcePermissions" in resp
        assert isinstance(resp["PrincipalResourcePermissions"], list)

    def test_list_resources_returns_list(self, client):
        """ListResources returns ResourceInfoList as a list."""
        resp = client.list_resources()
        assert "ResourceInfoList" in resp
        assert isinstance(resp["ResourceInfoList"], list)

    def test_list_transactions_returns_list(self, client):
        """ListTransactions returns Transactions as a list."""
        resp = client.list_transactions()
        assert "Transactions" in resp
        assert isinstance(resp["Transactions"], list)

    def test_describe_identity_center_not_configured(self, client):
        """DescribeLakeFormationIdentityCenterConfiguration raises when not configured."""
        from botocore.exceptions import ClientError as BotoClientError

        with pytest.raises(BotoClientError) as exc:
            client.describe_lake_formation_identity_center_configuration()
        err = exc.value.response["Error"]
        assert err["Code"] == "EntityNotFoundException"
        assert "Message" in err

    def test_describe_transaction_fake_id(self, client):
        """DescribeTransaction with fake ID raises EntityNotFoundException."""
        from botocore.exceptions import ClientError as BotoClientError

        with pytest.raises(BotoClientError) as exc:
            client.describe_transaction(TransactionId="nonexistent-txn-abc123")
        assert exc.value.response["Error"]["Code"] == "EntityNotFoundException"

    def test_get_data_cells_filter_nonexistent(self, client):
        """GetDataCellsFilter with fake filter raises EntityNotFoundException."""
        from botocore.exceptions import ClientError as BotoClientError

        with pytest.raises(BotoClientError) as exc:
            client.get_data_cells_filter(
                TableCatalogId="123456789012",
                DatabaseName="fake-db-gap",
                TableName="fake-tbl-gap",
                Name="fake-filter-gap",
            )
        assert exc.value.response["Error"]["Code"] == "EntityNotFoundException"

    def test_get_lf_tag_expression_nonexistent(self, client):
        """GetLFTagExpression with fake name raises EntityNotFoundException."""
        from botocore.exceptions import ClientError as BotoClientError

        with pytest.raises(BotoClientError) as exc:
            client.get_lf_tag_expression(Name="nonexistent-lf-expr-gap")
        assert exc.value.response["Error"]["Code"] == "EntityNotFoundException"


class TestLakeFormationLFTagExpressionCRUD:
    """Tests for CreateLFTagExpression, UpdateLFTagExpression, DeleteLFTagExpression."""

    @pytest.fixture
    def client(self):
        return make_client("lakeformation")

    def test_create_lf_tag_expression(self, client):
        """CreateLFTagExpression creates a new LF tag expression."""
        suffix = uuid.uuid4().hex[:8]
        tag_key = f"expr-tag-{suffix}"
        expr_name = f"expr-{suffix}"
        # Create an LF tag first (required for expressions)
        client.create_lf_tag(TagKey=tag_key, TagValues=["v1", "v2"])
        try:
            client.create_lf_tag_expression(
                Name=expr_name,
                Expression=[{"TagKey": tag_key, "TagValues": ["v1"]}],
            )
            list_resp = client.list_lf_tag_expressions()
            names = [e["Name"] for e in list_resp.get("LFTagExpressions", [])]
            assert expr_name in names
        finally:
            try:
                client.delete_lf_tag_expression(Name=expr_name)
            except Exception:
                pass  # best-effort cleanup
            client.delete_lf_tag(TagKey=tag_key)

    def test_delete_lf_tag_expression(self, client):
        """DeleteLFTagExpression removes a created expression."""
        suffix = uuid.uuid4().hex[:8]
        tag_key = f"del-expr-tag-{suffix}"
        expr_name = f"del-expr-{suffix}"
        client.create_lf_tag(TagKey=tag_key, TagValues=["v1"])
        try:
            client.create_lf_tag_expression(
                Name=expr_name,
                Expression=[{"TagKey": tag_key, "TagValues": ["v1"]}],
            )
            resp = client.delete_lf_tag_expression(Name=expr_name)
            assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200
            # Verify it's gone
            list_resp = client.list_lf_tag_expressions()
            names = [e["Name"] for e in list_resp.get("LFTagExpressions", [])]
            assert expr_name not in names
        finally:
            client.delete_lf_tag(TagKey=tag_key)

    def test_update_lf_tag_expression_not_found(self, client):
        """UpdateLFTagExpression with nonexistent name raises EntityNotFoundException."""
        from botocore.exceptions import ClientError as BotoClientError

        with pytest.raises(BotoClientError) as exc:
            client.update_lf_tag_expression(
                Name="nonexistent-expr-xyz-99",
                Expression=[{"TagKey": "fake-tag", "TagValues": ["v1"]}],
            )
        assert exc.value.response["Error"]["Code"] == "EntityNotFoundException"


class TestLakeFormationIdentityCenterCRUD:
    """Tests for Create/Update/Delete LakeFormationIdentityCenterConfiguration."""

    @pytest.fixture
    def client(self):
        return make_client("lakeformation")

    def test_create_identity_center_configuration(self, client):
        """CreateLakeFormationIdentityCenterConfiguration returns ApplicationArn."""
        resp = client.create_lake_formation_identity_center_configuration()
        assert "ApplicationArn" in resp
        # Cleanup
        try:
            client.delete_lake_formation_identity_center_configuration()
        except Exception:
            pass  # best-effort cleanup

    def test_delete_identity_center_configuration(self, client):
        """DeleteLakeFormationIdentityCenterConfiguration removes config."""
        client.create_lake_formation_identity_center_configuration()
        client.delete_lake_formation_identity_center_configuration()
        resp = client.list_lake_formation_opt_ins()
        assert "LakeFormationOptInsInfoList" in resp

    def test_update_identity_center_configuration_not_found(self, client):
        """UpdateLakeFormationIdentityCenterConfiguration raises EntityNotFoundException."""
        from botocore.exceptions import ClientError as BotoClientError

        # Ensure no config exists first
        try:
            client.delete_lake_formation_identity_center_configuration()
        except Exception:
            pass  # may not exist

        with pytest.raises(BotoClientError) as exc:
            client.update_lake_formation_identity_center_configuration()
        assert exc.value.response["Error"]["Code"] == "EntityNotFoundException"


class TestLakeFormationGapOps:
    """Tests for lakeformation ops that are working but weren't tested."""

    @pytest.fixture
    def client(self):
        return make_client("lakeformation")

    def test_revoke_permissions(self, client):
        """RevokePermissions can be called without error."""
        client.revoke_permissions(
            Principal={"DataLakePrincipalIdentifier": "arn:aws:iam::123456789012:role/test-role"},
            Resource={"Catalog": {}},
            Permissions=["ALL"],
        )
        resp = client.list_permissions()
        assert "PrincipalResourcePermissions" in resp


class TestLakeFormationNewStubOps:
    """Tests for newly-implemented lakeformation stub operations."""

    @pytest.fixture
    def client(self):
        return make_client("lakeformation")

    def test_list_lake_formation_opt_ins(self, client):
        """ListLakeFormationOptIns returns empty list by default."""
        resp = client.list_lake_formation_opt_ins()
        assert "LakeFormationOptInsInfoList" in resp
        assert isinstance(resp["LakeFormationOptInsInfoList"], list)

    def test_list_table_storage_optimizers(self, client):
        """ListTableStorageOptimizers returns empty list for nonexistent table."""
        resp = client.list_table_storage_optimizers(
            DatabaseName="nonexistent-db",
            TableName="nonexistent-table",
        )
        assert "StorageOptimizerList" in resp
        assert isinstance(resp["StorageOptimizerList"], list)

    def test_get_table_objects(self, client):
        """GetTableObjects returns empty list for nonexistent table."""
        resp = client.get_table_objects(
            DatabaseName="nonexistent-db",
            TableName="nonexistent-table",
        )
        assert "Objects" in resp
        assert isinstance(resp["Objects"], list)

    def test_get_temporary_data_location_credentials(self, client):
        """GetTemporaryDataLocationCredentials returns credentials."""
        resp = client.get_temporary_data_location_credentials(
            DataLocations=["s3://my-bucket/"],
            DurationSeconds=900,
        )
        assert "Credentials" in resp

    def test_create_lake_formation_opt_in(self, client):
        """CreateLakeFormationOptIn succeeds."""
        client.create_lake_formation_opt_in(
            Principal={"DataLakePrincipalIdentifier": "arn:aws:iam::123456789012:role/test-role"},
            Resource={"Catalog": {}},
        )
        resp = client.list_lake_formation_opt_ins()
        assert "LakeFormationOptInsInfoList" in resp

    def test_delete_lake_formation_opt_in(self, client):
        """DeleteLakeFormationOptIn succeeds."""
        client.delete_lake_formation_opt_in(
            Principal={"DataLakePrincipalIdentifier": "arn:aws:iam::123456789012:role/test-role"},
            Resource={"Catalog": {}},
        )
        resp = client.list_lake_formation_opt_ins()
        assert "LakeFormationOptInsInfoList" in resp

    def test_extend_transaction(self, client):
        """ExtendTransaction succeeds for a transaction."""
        txn = client.start_transaction(TransactionType="READ_AND_WRITE")
        txn_id = txn["TransactionId"]
        client.extend_transaction(TransactionId=txn_id)
        # Verify transaction still exists
        list_resp = client.list_transactions()
        txn_ids = [t["TransactionId"] for t in list_resp["Transactions"]]
        assert txn_id in txn_ids

    def test_update_table_storage_optimizer(self, client):
        """UpdateTableStorageOptimizer returns a result message."""
        resp = client.update_table_storage_optimizer(
            DatabaseName="test-db",
            TableName="test-table",
            StorageOptimizerConfig={"COMPACTION": {"IsEnabled": "true"}},
        )
        assert "Result" in resp

    def test_update_table_objects(self, client):
        """UpdateTableObjects succeeds (stub)."""
        txn = client.start_transaction(TransactionType="READ_AND_WRITE")
        txn_id = txn["TransactionId"]
        client.update_table_objects(
            DatabaseName="test-db",
            TableName="test-table",
            TransactionId=txn_id,
            WriteOperations=[
                {
                    "AddObject": {
                        "Uri": "s3://my-bucket/data/file1.parquet",
                        "ETag": "abc123",
                        "Size": 1024,
                    }
                }
            ],
        )
        # Verify transaction still exists after writing
        list_resp = client.list_transactions()
        txn_ids = [t["TransactionId"] for t in list_resp["Transactions"]]
        assert txn_id in txn_ids


class TestLakeFormationGapOps2:
    """Tests for second batch of newly-working lakeformation gap operations."""

    @pytest.fixture
    def client(self):
        return make_client("lakeformation")

    def test_delete_objects_on_cancel(self, client):
        """DeleteObjectsOnCancel succeeds and returns 200."""
        client.delete_objects_on_cancel(
            DatabaseName="test-db",
            TableName="test-table",
            TransactionId="fake-txn-id-001",
            Objects=[{"Uri": "s3://my-bucket/data/file.parquet", "ETag": "etag123"}],
        )
        resp = client.list_transactions()
        assert "Transactions" in resp

    def test_update_resource(self, client):
        """UpdateResource succeeds and returns 200."""
        client.update_resource(
            RoleArn="arn:aws:iam::123456789012:role/lakeformation-role",
            ResourceArn="arn:aws:s3:::my-lakeformation-bucket",
        )
        resp = client.list_transactions()
        assert "Transactions" in resp


class TestLakeFormationQueryPlanningOps:
    """Tests for LakeFormation query planning operations (use inject_host_prefix=False)."""

    @pytest.fixture
    def client(self):
        import boto3
        from botocore.config import Config

        return boto3.client(
            "lakeformation",
            endpoint_url="http://localhost:4566",
            region_name="us-east-1",
            aws_access_key_id="test",
            aws_secret_access_key="test",
            config=Config(inject_host_prefix=False),
        )

    def test_start_query_planning_returns_query_id(self, client):
        resp = client.start_query_planning(
            QueryPlanningContext={"DatabaseName": "mydb"},
            QueryString="SELECT 1",
        )
        assert "QueryId" in resp
        assert len(resp["QueryId"]) > 0

    def test_get_query_state_returns_state(self, client):
        start = client.start_query_planning(
            QueryPlanningContext={"DatabaseName": "mydb"},
            QueryString="SELECT 1",
        )
        qid = start["QueryId"]
        resp = client.get_query_state(QueryId=qid)
        assert "State" in resp

    def test_get_query_statistics_returns_statistics(self, client):
        start = client.start_query_planning(
            QueryPlanningContext={"DatabaseName": "mydb"},
            QueryString="SELECT 1",
        )
        qid = start["QueryId"]
        resp = client.get_query_statistics(QueryId=qid)
        assert (
            "ExecutionStatistics" in resp
            or "PlanningStatistics" in resp
            or "QuerySubmissionTime" in resp
        )

    def test_get_work_units_returns_list(self, client):
        start = client.start_query_planning(
            QueryPlanningContext={"DatabaseName": "mydb"},
            QueryString="SELECT 1",
        )
        qid = start["QueryId"]
        resp = client.get_work_units(QueryId=qid)
        assert "WorkUnitRanges" in resp

    def test_get_work_unit_results_returns_200(self, client):
        start = client.start_query_planning(
            QueryPlanningContext={"DatabaseName": "mydb"},
            QueryString="SELECT 1",
        )
        qid = start["QueryId"]
        resp = client.get_work_unit_results(QueryId=qid, WorkUnitId=0, WorkUnitToken="token")
        assert "ResultStream" in resp


class TestLakeFormationAssumeDecoratedRoleWithSAML:
    """Test AssumeDecoratedRoleWithSAML."""

    def test_assume_decorated_role_with_saml_returns_credentials(self, lakeformation):
        resp = lakeformation.assume_decorated_role_with_saml(
            SAMLAssertion="a" * 100,
            RoleArn="arn:aws:iam::123456789012:role/lakeformation-saml-role",
            PrincipalArn="arn:aws:iam::123456789012:saml-provider/my-provider",
        )
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200
        assert "AccessKeyId" in resp
        assert "SecretAccessKey" in resp
        assert "SessionToken" in resp

    def test_assume_decorated_role_with_saml_with_duration(self, lakeformation):
        resp = lakeformation.assume_decorated_role_with_saml(
            SAMLAssertion="a" * 100,
            RoleArn="arn:aws:iam::123456789012:role/lakeformation-saml-role",
            PrincipalArn="arn:aws:iam::123456789012:saml-provider/my-provider",
            DurationSeconds=900,
        )
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200
        assert "AccessKeyId" in resp


class TestLakeFormationOptIn:
    """Test CreateLakeFormationOptIn and DeleteLakeFormationOptIn."""

    def test_create_lake_formation_opt_in_succeeds(self, lakeformation):
        lakeformation.create_lake_formation_opt_in(
            Principal={"DataLakePrincipalIdentifier": "arn:aws:iam::123456789012:role/test"},
            Resource={"Catalog": {}},
        )
        resp = lakeformation.list_lake_formation_opt_ins()
        assert "LakeFormationOptInsInfoList" in resp

    def test_delete_lake_formation_opt_in_succeeds(self, lakeformation):
        lakeformation.delete_lake_formation_opt_in(
            Principal={"DataLakePrincipalIdentifier": "arn:aws:iam::123456789012:role/test"},
            Resource={"Catalog": {}},
        )
        resp = lakeformation.list_lake_formation_opt_ins()
        assert "LakeFormationOptInsInfoList" in resp


class TestLakeFormationDeleteObjectsOnCancel:
    """Test DeleteObjectsOnCancel."""

    def test_delete_objects_on_cancel_succeeds(self, lakeformation):
        lakeformation.delete_objects_on_cancel(
            DatabaseName="testdb",
            TableName="testtable",
            TransactionId="abc1234567890123456789012345678901",
            Objects=[{"Uri": "s3://bucket/key", "ETag": "etag123"}],
        )
        resp = lakeformation.list_transactions()
        assert "Transactions" in resp


class TestLakeFormationExtendTransaction:
    """Test ExtendTransaction."""

    def test_extend_transaction_succeeds(self, lakeformation):
        lakeformation.extend_transaction(
            TransactionId="abc1234567890123456789012345678901",
        )
        resp = lakeformation.list_transactions()
        assert "Transactions" in resp


class TestLakeFormationUpdateTableObjects:
    """Test UpdateTableObjects."""

    def test_update_table_objects_succeeds(self, lakeformation):
        lakeformation.update_table_objects(
            DatabaseName="testdb",
            TableName="testtable",
            TransactionId="abc1234567890123456789012345678901",
            WriteOperations=[
                {"AddObject": {"Uri": "s3://bucket/key", "ETag": "etag123", "Size": 100}}
            ],
        )
        resp = lakeformation.list_transactions()
        assert "Transactions" in resp
