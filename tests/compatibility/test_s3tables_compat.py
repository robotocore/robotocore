"""S3 Tables compatibility tests."""

import uuid

import pytest

from tests.compatibility.conftest import make_client


@pytest.fixture
def s3tables():
    return make_client("s3tables")


@pytest.fixture
def table_bucket(s3tables):
    """Create a table bucket and return its ARN. Cleaned up after test."""
    name = _bucket_name()
    resp = s3tables.create_table_bucket(name=name)
    arn = resp["arn"]
    yield arn
    try:
        s3tables.delete_table_bucket(tableBucketARN=arn)
    except Exception:
        pass


@pytest.fixture
def table_with_ns(s3tables, table_bucket):
    """Create a namespace and table inside a table bucket.

    Returns (bucket_arn, ns, table_name, table_arn).
    """
    ns = _ns_name()
    tbl = f"tbl_{uuid.uuid4().hex[:8]}"
    s3tables.create_namespace(tableBucketARN=table_bucket, namespace=[ns])
    create_resp = s3tables.create_table(
        tableBucketARN=table_bucket,
        namespace=ns,
        name=tbl,
        format="ICEBERG",
    )
    table_arn = create_resp["tableARN"]
    yield table_bucket, ns, tbl, table_arn
    try:
        s3tables.delete_table(tableBucketARN=table_bucket, namespace=ns, name=tbl)
    except Exception:
        pass
    try:
        s3tables.delete_namespace(tableBucketARN=table_bucket, namespace=ns)
    except Exception:
        pass


def _bucket_name():
    """Generate a valid s3tables bucket name (lowercase alphanumeric + hyphens)."""
    return f"test-{uuid.uuid4().hex[:8]}"


def _ns_name():
    """Generate a valid s3tables namespace name (lowercase alphanumeric + underscores only)."""
    return f"ns_{uuid.uuid4().hex[:8]}"


class TestS3TablesOperations:
    def test_list_table_buckets_empty(self, s3tables):
        resp = s3tables.list_table_buckets()
        assert "tableBuckets" in resp

    def test_create_table_bucket(self, s3tables):
        name = _bucket_name()
        resp = s3tables.create_table_bucket(name=name)
        assert "arn" in resp
        assert name in resp["arn"]

    def test_list_table_buckets_after_create(self, s3tables):
        name = _bucket_name()
        s3tables.create_table_bucket(name=name)
        resp = s3tables.list_table_buckets()
        names = [b["name"] for b in resp["tableBuckets"]]
        assert name in names

    def test_get_namespace(self, s3tables):
        bucket_name = _bucket_name()
        bucket_resp = s3tables.create_table_bucket(name=bucket_name)
        bucket_arn = bucket_resp["arn"]
        ns_name = _ns_name()
        try:
            s3tables.create_namespace(tableBucketARN=bucket_arn, namespace=[ns_name])
            resp = s3tables.get_namespace(tableBucketARN=bucket_arn, namespace=ns_name)
            assert "namespace" in resp
            assert resp["namespace"] == [ns_name]
        finally:
            try:
                s3tables.delete_namespace(tableBucketARN=bucket_arn, namespace=ns_name)
            except Exception:
                pass

    def test_delete_namespace(self, s3tables):
        bucket_name = _bucket_name()
        bucket_resp = s3tables.create_table_bucket(name=bucket_name)
        bucket_arn = bucket_resp["arn"]
        ns_name = _ns_name()
        try:
            s3tables.create_namespace(tableBucketARN=bucket_arn, namespace=[ns_name])
            s3tables.delete_namespace(tableBucketARN=bucket_arn, namespace=ns_name)
            with pytest.raises(s3tables.exceptions.ClientError) as exc:
                s3tables.get_namespace(tableBucketARN=bucket_arn, namespace=ns_name)
            assert exc.value.response["Error"]["Code"] in (
                "NotFoundException",
                "ResourceNotFoundException",
                "NoSuchEntity",
            )
        except Exception:
            # Cleanup: try to delete the namespace if it still exists
            try:
                s3tables.delete_namespace(tableBucketARN=bucket_arn, namespace=ns_name)
            except Exception:
                pass
            raise

    # --- GetTableBucket ---
    def test_get_table_bucket(self, s3tables, table_bucket):
        resp = s3tables.get_table_bucket(tableBucketARN=table_bucket)
        assert "arn" in resp
        assert resp["arn"] == table_bucket

    # --- GetTableBucketEncryption ---
    def test_get_table_bucket_encryption(self, s3tables, table_bucket):
        # New bucket has no encryption config yet; server returns NotFoundException
        with pytest.raises(s3tables.exceptions.ClientError) as exc:
            s3tables.get_table_bucket_encryption(tableBucketARN=table_bucket)
        assert exc.value.response["Error"]["Code"] == "NotFoundException"

    # --- GetTableBucketMaintenanceConfiguration ---
    def test_get_table_bucket_maintenance_configuration(self, s3tables, table_bucket):
        # New bucket has no maintenance config; server returns NotFoundException
        with pytest.raises(s3tables.exceptions.ClientError) as exc:
            s3tables.get_table_bucket_maintenance_configuration(tableBucketARN=table_bucket)
        assert exc.value.response["Error"]["Code"] == "NotFoundException"

    # --- GetTableBucketMetricsConfiguration ---
    def test_get_table_bucket_metrics_configuration(self, s3tables, table_bucket):
        # New bucket has no metrics config; server returns NotFoundException
        with pytest.raises(s3tables.exceptions.ClientError) as exc:
            s3tables.get_table_bucket_metrics_configuration(tableBucketARN=table_bucket)
        assert exc.value.response["Error"]["Code"] == "NotFoundException"

    # --- GetTableBucketPolicy ---
    def test_get_table_bucket_policy_not_found(self, s3tables, table_bucket):
        """GetTableBucketPolicy on a bucket with no policy should raise an error."""
        with pytest.raises(s3tables.exceptions.ClientError) as exc:
            s3tables.get_table_bucket_policy(tableBucketARN=table_bucket)
        assert exc.value.response["Error"]["Code"] in (
            "NotFoundException",
            "ResourceNotFoundException",
            "NoSuchBucketPolicy",
        )

    # --- GetTableBucketReplication ---
    def test_get_table_bucket_replication(self, s3tables, table_bucket):
        # New bucket has no replication config; server returns NotFoundException
        with pytest.raises(s3tables.exceptions.ClientError) as exc:
            s3tables.get_table_bucket_replication(tableBucketARN=table_bucket)
        assert exc.value.response["Error"]["Code"] == "NotFoundException"

    # --- GetTableBucketStorageClass ---
    def test_get_table_bucket_storage_class(self, s3tables, table_bucket):
        # New bucket has no storage class config; server returns NotFoundException
        with pytest.raises(s3tables.exceptions.ClientError) as exc:
            s3tables.get_table_bucket_storage_class(tableBucketARN=table_bucket)
        assert exc.value.response["Error"]["Code"] == "NotFoundException"

    # --- ListNamespaces ---
    def test_list_namespaces(self, s3tables, table_bucket):
        resp = s3tables.list_namespaces(tableBucketARN=table_bucket)
        assert "namespaces" in resp

    # --- ListTables ---
    def test_list_tables(self, s3tables, table_with_ns):
        bucket_arn, ns, tbl, _table_arn = table_with_ns
        resp = s3tables.list_tables(tableBucketARN=bucket_arn, namespace=ns)
        assert "tables" in resp
        names = [t["name"] for t in resp["tables"]]
        assert tbl in names

    # --- ListTagsForResource ---
    def test_list_tags_for_resource(self, s3tables, table_bucket):
        resp = s3tables.list_tags_for_resource(resourceArn=table_bucket)
        assert "tags" in resp or "Tags" in resp

    # --- GetTableEncryption ---
    def test_get_table_encryption(self, s3tables, table_with_ns):
        bucket_arn, ns, tbl, _table_arn = table_with_ns
        resp = s3tables.get_table_encryption(tableBucketARN=bucket_arn, namespace=ns, name=tbl)
        assert "encryptionConfiguration" in resp

    # --- GetTableMaintenanceConfiguration ---
    def test_get_table_maintenance_configuration(self, s3tables, table_with_ns):
        bucket_arn, ns, tbl, _table_arn = table_with_ns
        resp = s3tables.get_table_maintenance_configuration(
            tableBucketARN=bucket_arn, namespace=ns, name=tbl
        )
        assert "tableARN" in resp or "configuration" in resp

    # --- GetTableMaintenanceJobStatus ---
    def test_get_table_maintenance_job_status(self, s3tables, table_with_ns):
        bucket_arn, ns, tbl, _table_arn = table_with_ns
        resp = s3tables.get_table_maintenance_job_status(
            tableBucketARN=bucket_arn, namespace=ns, name=tbl
        )
        assert "tableARN" in resp or "status" in resp

    # --- GetTableMetadataLocation ---
    def test_get_table_metadata_location(self, s3tables, table_with_ns):
        bucket_arn, ns, tbl, _table_arn = table_with_ns
        resp = s3tables.get_table_metadata_location(
            tableBucketARN=bucket_arn, namespace=ns, name=tbl
        )
        assert "warehouseLocation" in resp

    # --- GetTablePolicy ---
    def test_get_table_policy_not_found(self, s3tables, table_with_ns):
        bucket_arn, ns, tbl, _table_arn = table_with_ns
        with pytest.raises(s3tables.exceptions.ClientError) as exc:
            s3tables.get_table_policy(tableBucketARN=bucket_arn, namespace=ns, name=tbl)
        assert exc.value.response["Error"]["Code"] in (
            "NotFoundException",
            "ResourceNotFoundException",
            "NoSuchTablePolicy",
        )

    # --- GetTableRecordExpirationConfiguration ---
    def test_get_table_record_expiration_configuration(self, s3tables, table_with_ns):
        _bucket_arn, _ns, _tbl, table_arn = table_with_ns
        resp = s3tables.get_table_record_expiration_configuration(tableArn=table_arn)
        assert "tableARN" in resp or "configuration" in resp

    # --- GetTableRecordExpirationJobStatus ---
    def test_get_table_record_expiration_job_status(self, s3tables, table_with_ns):
        _bucket_arn, _ns, _tbl, table_arn = table_with_ns
        resp = s3tables.get_table_record_expiration_job_status(tableArn=table_arn)
        assert "tableARN" in resp or "status" in resp

    # --- GetTableReplication ---
    def test_get_table_replication(self, s3tables, table_with_ns):
        _bucket_arn, _ns, _tbl, table_arn = table_with_ns
        # Table has no replication config; server returns NotFoundException
        with pytest.raises(s3tables.exceptions.ClientError) as exc:
            s3tables.get_table_replication(tableArn=table_arn)
        assert exc.value.response["Error"]["Code"] == "NotFoundException"

    # --- GetTableReplicationStatus ---
    def test_get_table_replication_status(self, s3tables, table_with_ns):
        _bucket_arn, _ns, _tbl, table_arn = table_with_ns
        resp = s3tables.get_table_replication_status(tableArn=table_arn)
        assert "sourceTableArn" in resp or "destinations" in resp

    # --- GetTableStorageClass ---
    def test_get_table_storage_class(self, s3tables, table_with_ns):
        bucket_arn, ns, tbl, _table_arn = table_with_ns
        resp = s3tables.get_table_storage_class(tableBucketARN=bucket_arn, namespace=ns, name=tbl)
        assert "storageClassConfiguration" in resp
