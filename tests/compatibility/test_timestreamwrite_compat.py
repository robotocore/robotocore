"""Timestream Write compatibility tests."""

import uuid

import pytest

from tests.compatibility.conftest import make_client


@pytest.fixture
def timestream_write():
    return make_client("timestream-write")


@pytest.fixture
def tsw_db(timestream_write):
    """Create a database for testing and clean up after."""
    db_name = f"compat-db-{uuid.uuid4().hex[:8]}"
    timestream_write.create_database(DatabaseName=db_name)
    yield db_name
    try:
        timestream_write.delete_database(DatabaseName=db_name)
    except Exception:
        pass  # already deleted


@pytest.fixture
def tsw_table(timestream_write, tsw_db):
    """Create a table for testing and clean up after."""
    table_name = f"compat-tbl-{uuid.uuid4().hex[:8]}"
    timestream_write.create_table(DatabaseName=tsw_db, TableName=table_name)
    yield table_name
    try:
        timestream_write.delete_table(DatabaseName=tsw_db, TableName=table_name)
    except Exception:
        pass  # already deleted


class TestTimestreamWriteOperations:
    def test_list_databases_empty(self, timestream_write):
        response = timestream_write.list_databases()
        assert response["ResponseMetadata"]["HTTPStatusCode"] == 200
        assert "Databases" in response
        assert isinstance(response["Databases"], list)

    def test_describe_endpoints(self, timestream_write):
        response = timestream_write.describe_endpoints()
        assert response["ResponseMetadata"]["HTTPStatusCode"] == 200
        assert "Endpoints" in response
        assert len(response["Endpoints"]) >= 1
        endpoint = response["Endpoints"][0]
        assert "Address" in endpoint
        assert "CachePeriodInMinutes" in endpoint

    def test_create_list_delete_database(self, timestream_write):
        db_name = f"db-{uuid.uuid4().hex[:8]}"

        # Create
        create_resp = timestream_write.create_database(DatabaseName=db_name)
        assert create_resp["ResponseMetadata"]["HTTPStatusCode"] == 200
        assert create_resp["Database"]["DatabaseName"] == db_name
        assert "Arn" in create_resp["Database"]

        try:
            # List and verify present
            list_resp = timestream_write.list_databases()
            db_names = [db["DatabaseName"] for db in list_resp["Databases"]]
            assert db_name in db_names
        finally:
            # Delete
            delete_resp = timestream_write.delete_database(DatabaseName=db_name)
            assert delete_resp["ResponseMetadata"]["HTTPStatusCode"] == 200

        # Verify deleted
        list_resp = timestream_write.list_databases()
        db_names = [db["DatabaseName"] for db in list_resp["Databases"]]
        assert db_name not in db_names


class TestTimestreamwriteAutoCoverage:
    """Auto-generated coverage tests for timestreamwrite."""

    @pytest.fixture
    def client(self):
        return make_client("timestream-write")

    def test_list_tables(self, client):
        """ListTables returns a list of tables."""
        resp = client.list_tables()
        assert "Tables" in resp
        assert isinstance(resp["Tables"], list)


class TestTimestreamWriteCreateTable:
    """Tests for CreateTable operation."""

    def test_create_table(self, timestream_write, tsw_db):
        """CreateTable returns table details including name and db."""
        table_name = f"ct-{uuid.uuid4().hex[:8]}"
        resp = timestream_write.create_table(DatabaseName=tsw_db, TableName=table_name)
        assert "Table" in resp
        assert resp["Table"]["TableName"] == table_name
        assert resp["Table"]["DatabaseName"] == tsw_db
        # cleanup
        timestream_write.delete_table(DatabaseName=tsw_db, TableName=table_name)


class TestTimestreamWriteDeleteTable:
    """Tests for DeleteTable operation."""

    def test_delete_table(self, timestream_write, tsw_db):
        """DeleteTable removes a table without error."""
        table_name = f"dt-{uuid.uuid4().hex[:8]}"
        timestream_write.create_table(DatabaseName=tsw_db, TableName=table_name)
        resp = timestream_write.delete_table(DatabaseName=tsw_db, TableName=table_name)
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200


class TestTimestreamWriteDescribeDatabase:
    """Tests for DescribeDatabase operation."""

    def test_describe_database(self, timestream_write, tsw_db):
        """DescribeDatabase returns database details."""
        resp = timestream_write.describe_database(DatabaseName=tsw_db)
        assert "Database" in resp
        assert resp["Database"]["DatabaseName"] == tsw_db

    def test_describe_database_not_found(self, timestream_write):
        """DescribeDatabase raises ResourceNotFoundException for missing db."""
        with pytest.raises(timestream_write.exceptions.ResourceNotFoundException):
            timestream_write.describe_database(DatabaseName="nonexistent-db-xyz-compat")


class TestTimestreamWriteDescribeTable:
    """Tests for DescribeTable operation."""

    def test_describe_table(self, timestream_write, tsw_db, tsw_table):
        """DescribeTable returns table details."""
        resp = timestream_write.describe_table(DatabaseName=tsw_db, TableName=tsw_table)
        assert "Table" in resp
        assert resp["Table"]["TableName"] == tsw_table
        assert resp["Table"]["DatabaseName"] == tsw_db

    def test_describe_table_not_found(self, timestream_write, tsw_db):
        """DescribeTable raises ResourceNotFoundException for missing table."""
        with pytest.raises(timestream_write.exceptions.ResourceNotFoundException):
            timestream_write.describe_table(
                DatabaseName=tsw_db, TableName="nonexistent-table-xyz-compat"
            )


class TestTimestreamWriteTagging:
    """Tests for TagResource, ListTagsForResource, and UntagResource."""

    def test_tag_resource(self, timestream_write, tsw_db, tsw_table):
        """TagResource applies tags to a Timestream table resource."""
        describe_resp = timestream_write.describe_table(DatabaseName=tsw_db, TableName=tsw_table)
        arn = describe_resp["Table"]["Arn"]
        resp = timestream_write.tag_resource(
            ResourceARN=arn,
            Tags=[{"Key": "env", "Value": "test"}],
        )
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200

    def test_list_tags_for_resource(self, timestream_write, tsw_db, tsw_table):
        """ListTagsForResource returns tags applied to a resource."""
        describe_resp = timestream_write.describe_table(DatabaseName=tsw_db, TableName=tsw_table)
        arn = describe_resp["Table"]["Arn"]
        timestream_write.tag_resource(
            ResourceARN=arn,
            Tags=[{"Key": "project", "Value": "compat-test"}],
        )
        resp = timestream_write.list_tags_for_resource(ResourceARN=arn)
        assert "Tags" in resp
        tags = {t["Key"]: t["Value"] for t in resp["Tags"]}
        assert tags.get("project") == "compat-test"

    def test_untag_resource(self, timestream_write, tsw_db, tsw_table):
        """UntagResource removes a tag from a resource."""
        describe_resp = timestream_write.describe_table(DatabaseName=tsw_db, TableName=tsw_table)
        arn = describe_resp["Table"]["Arn"]
        timestream_write.tag_resource(
            ResourceARN=arn,
            Tags=[{"Key": "to-remove", "Value": "yes"}],
        )
        resp = timestream_write.untag_resource(ResourceARN=arn, TagKeys=["to-remove"])
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200
        list_resp = timestream_write.list_tags_for_resource(ResourceARN=arn)
        keys = [t["Key"] for t in list_resp["Tags"]]
        assert "to-remove" not in keys


class TestTimestreamWriteUpdateTable:
    """Tests for UpdateTable operation."""

    def test_update_table(self, timestream_write, tsw_db, tsw_table):
        """UpdateTable updates retention properties and returns table details."""
        resp = timestream_write.update_table(
            DatabaseName=tsw_db,
            TableName=tsw_table,
            RetentionProperties={
                "MemoryStoreRetentionPeriodInHours": 48,
                "MagneticStoreRetentionPeriodInDays": 30,
            },
        )
        assert "Table" in resp
        assert resp["Table"]["TableName"] == tsw_table


class TestTimestreamWriteWriteRecords:
    """Tests for WriteRecords operation."""

    def test_write_records(self, timestream_write, tsw_db, tsw_table):
        """WriteRecords writes time-series records to a table."""
        resp = timestream_write.write_records(
            DatabaseName=tsw_db,
            TableName=tsw_table,
            Records=[
                {
                    "Dimensions": [{"Name": "host", "Value": "server-1"}],
                    "MeasureName": "cpu_utilization",
                    "MeasureValue": "75.5",
                    "MeasureValueType": "DOUBLE",
                    "Time": "1609459200000",
                    "TimeUnit": "MILLISECONDS",
                }
            ],
        )
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200


class TestTimestreamWriteBatchLoadTask:
    """Tests for CreateBatchLoadTask, DescribeBatchLoadTask, ListBatchLoadTasks, ResumeBatchLoadTask."""  # noqa: E501

    def _batch_load_params(self, db_name: str, table_name: str) -> dict:
        return {
            "TargetDatabaseName": db_name,
            "TargetTableName": table_name,
            "DataModelConfiguration": {
                "DataModel": {
                    "TimeColumn": "timestamp",
                    "TimeUnit": "SECONDS",
                    "DimensionMappings": [
                        {"SourceColumn": "region", "DestinationColumn": "region"}
                    ],
                    "MultiMeasureMappings": {
                        "TargetMultiMeasureName": "metrics",
                        "MultiMeasureAttributeMappings": [
                            {"SourceColumn": "value", "MeasureValueType": "DOUBLE"}
                        ],
                    },
                }
            },
            "DataSourceConfiguration": {
                "DataSourceS3Configuration": {"BucketName": "my-data-bucket"},
                "DataFormat": "CSV",
            },
            "ReportConfiguration": {
                "ReportS3Configuration": {
                    "BucketName": "my-report-bucket",
                    "EncryptionOption": "SSE_S3",
                }
            },
        }

    def test_create_batch_load_task(self, timestream_write, tsw_db, tsw_table):
        """CreateBatchLoadTask returns a TaskId."""
        resp = timestream_write.create_batch_load_task(**self._batch_load_params(tsw_db, tsw_table))
        assert "TaskId" in resp
        assert resp["TaskId"]

    def test_describe_batch_load_task(self, timestream_write, tsw_db, tsw_table):
        """DescribeBatchLoadTask returns task details including status."""
        create_resp = timestream_write.create_batch_load_task(
            **self._batch_load_params(tsw_db, tsw_table)
        )
        task_id = create_resp["TaskId"]
        desc_resp = timestream_write.describe_batch_load_task(TaskId=task_id)
        assert "BatchLoadTaskDescription" in desc_resp
        task = desc_resp["BatchLoadTaskDescription"]
        assert task["TaskId"] == task_id
        assert "TaskStatus" in task
        assert task["TargetDatabaseName"] == tsw_db
        assert task["TargetTableName"] == tsw_table

    def test_list_batch_load_tasks(self, timestream_write, tsw_db, tsw_table):
        """ListBatchLoadTasks returns a list of tasks."""
        timestream_write.create_batch_load_task(**self._batch_load_params(tsw_db, tsw_table))
        resp = timestream_write.list_batch_load_tasks()
        assert "BatchLoadTasks" in resp
        assert isinstance(resp["BatchLoadTasks"], list)
        assert len(resp["BatchLoadTasks"]) >= 1

    def test_resume_batch_load_task(self, timestream_write, tsw_db, tsw_table):
        """ResumeBatchLoadTask changes task status to IN_PROGRESS."""
        create_resp = timestream_write.create_batch_load_task(
            **self._batch_load_params(tsw_db, tsw_table)
        )
        task_id = create_resp["TaskId"]
        resp = timestream_write.resume_batch_load_task(TaskId=task_id)
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200
        desc_resp = timestream_write.describe_batch_load_task(TaskId=task_id)
        assert desc_resp["BatchLoadTaskDescription"]["TaskStatus"] == "IN_PROGRESS"
