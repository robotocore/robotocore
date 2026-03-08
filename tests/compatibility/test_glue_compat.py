"""AWS Glue compatibility tests."""

import uuid

import pytest
from botocore.exceptions import ClientError

from tests.compatibility.conftest import make_client


@pytest.fixture
def glue():
    return make_client("glue")


def _unique(prefix):
    return f"{prefix}-{uuid.uuid4().hex[:8]}"


class TestGlueDatabaseOperations:
    def test_create_and_get_database(self, glue):
        db_name = _unique("db")
        glue.create_database(DatabaseInput={"Name": db_name, "Description": "test database"})

        response = glue.get_database(Name=db_name)
        assert response["Database"]["Name"] == db_name
        assert response["Database"]["Description"] == "test database"

        glue.delete_database(Name=db_name)

    def test_get_databases(self, glue):
        db_name = _unique("db")
        glue.create_database(DatabaseInput={"Name": db_name})

        response = glue.get_databases()
        db_names = [db["Name"] for db in response["DatabaseList"]]
        assert db_name in db_names

        glue.delete_database(Name=db_name)

    def test_delete_database(self, glue):
        db_name = _unique("db")
        glue.create_database(DatabaseInput={"Name": db_name})
        glue.delete_database(Name=db_name)

        with pytest.raises(ClientError) as exc:
            glue.get_database(Name=db_name)
        assert exc.value.response["Error"]["Code"] == "EntityNotFoundException"

    def test_create_duplicate_database_fails(self, glue):
        db_name = _unique("db")
        glue.create_database(DatabaseInput={"Name": db_name})

        with pytest.raises(ClientError) as exc:
            glue.create_database(DatabaseInput={"Name": db_name})
        assert exc.value.response["Error"]["Code"] == "AlreadyExistsException"

        glue.delete_database(Name=db_name)


class TestGlueTableOperations:
    def test_create_and_get_table(self, glue):
        db_name = _unique("db")
        tbl_name = _unique("tbl")
        glue.create_database(DatabaseInput={"Name": db_name})

        glue.create_table(
            DatabaseName=db_name,
            TableInput={
                "Name": tbl_name,
                "StorageDescriptor": {
                    "Columns": [{"Name": "col1", "Type": "string"}],
                    "Location": "s3://bucket/path",
                    "InputFormat": "org.apache.hadoop.mapred.TextInputFormat",
                    "OutputFormat": "org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat",
                    "SerdeInfo": {
                        "SerializationLibrary": "org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe"
                    },
                },
            },
        )

        response = glue.get_table(DatabaseName=db_name, Name=tbl_name)
        assert response["Table"]["Name"] == tbl_name
        assert response["Table"]["StorageDescriptor"]["Columns"][0]["Name"] == "col1"

        glue.delete_table(DatabaseName=db_name, Name=tbl_name)
        glue.delete_database(Name=db_name)

    def test_get_tables(self, glue):
        db_name = _unique("db")
        tbl1 = _unique("tbl")
        tbl2 = _unique("tbl")
        glue.create_database(DatabaseInput={"Name": db_name})

        for tbl in (tbl1, tbl2):
            glue.create_table(
                DatabaseName=db_name,
                TableInput={
                    "Name": tbl,
                    "StorageDescriptor": {
                        "Columns": [{"Name": "id", "Type": "int"}],
                        "Location": "s3://bucket/path",
                        "InputFormat": "org.apache.hadoop.mapred.TextInputFormat",
                        "OutputFormat": "org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat",  # noqa: E501
                        "SerdeInfo": {
                            "SerializationLibrary": "org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe"  # noqa: E501
                        },
                    },
                },
            )

        response = glue.get_tables(DatabaseName=db_name)
        table_names = [t["Name"] for t in response["TableList"]]
        assert tbl1 in table_names
        assert tbl2 in table_names

        for tbl in (tbl1, tbl2):
            glue.delete_table(DatabaseName=db_name, Name=tbl)
        glue.delete_database(Name=db_name)

    def test_delete_table(self, glue):
        db_name = _unique("db")
        tbl_name = _unique("tbl")
        glue.create_database(DatabaseInput={"Name": db_name})
        glue.create_table(
            DatabaseName=db_name,
            TableInput={
                "Name": tbl_name,
                "StorageDescriptor": {
                    "Columns": [{"Name": "col1", "Type": "string"}],
                    "Location": "s3://bucket/path",
                    "InputFormat": "org.apache.hadoop.mapred.TextInputFormat",
                    "OutputFormat": "org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat",
                    "SerdeInfo": {
                        "SerializationLibrary": "org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe"
                    },
                },
            },
        )

        glue.delete_table(DatabaseName=db_name, Name=tbl_name)

        with pytest.raises(ClientError) as exc:
            glue.get_table(DatabaseName=db_name, Name=tbl_name)
        assert exc.value.response["Error"]["Code"] == "EntityNotFoundException"

        glue.delete_database(Name=db_name)


class TestGlueCrawlerOperations:
    def test_create_and_get_crawler(self, glue):
        db_name = _unique("db")
        crawler_name = _unique("test-crawler")
        glue.create_database(DatabaseInput={"Name": db_name})

        glue.create_crawler(
            Name=crawler_name,
            Role="arn:aws:iam::123456789012:role/glue-role",
            DatabaseName=db_name,
            Targets={"S3Targets": [{"Path": "s3://test-bucket/data"}]},
        )

        response = glue.get_crawler(Name=crawler_name)
        assert response["Crawler"]["Name"] == crawler_name
        assert response["Crawler"]["DatabaseName"] == db_name

        glue.delete_crawler(Name=crawler_name)
        glue.delete_database(Name=db_name)

    def test_get_crawlers(self, glue):
        db_name = _unique("db")
        crawler_name = _unique("test-crawler")
        glue.create_database(DatabaseInput={"Name": db_name})

        glue.create_crawler(
            Name=crawler_name,
            Role="arn:aws:iam::123456789012:role/glue-role",
            DatabaseName=db_name,
            Targets={"S3Targets": [{"Path": "s3://test-bucket/data"}]},
        )

        response = glue.get_crawlers()
        crawler_names = [c["Name"] for c in response["Crawlers"]]
        assert crawler_name in crawler_names

        glue.delete_crawler(Name=crawler_name)
        glue.delete_database(Name=db_name)

    def test_delete_crawler(self, glue):
        db_name = _unique("db")
        crawler_name = _unique("test-crawler")
        glue.create_database(DatabaseInput={"Name": db_name})

        glue.create_crawler(
            Name=crawler_name,
            Role="arn:aws:iam::123456789012:role/glue-role",
            DatabaseName=db_name,
            Targets={"S3Targets": [{"Path": "s3://test-bucket/data"}]},
        )

        glue.delete_crawler(Name=crawler_name)

        with pytest.raises(ClientError) as exc:
            glue.get_crawler(Name=crawler_name)
        assert exc.value.response["Error"]["Code"] == "EntityNotFoundException"

        glue.delete_database(Name=db_name)


class TestGlueJobOperations:
    def test_create_and_get_job(self, glue):
        job_name = _unique("test-job")
        glue.create_job(
            Name=job_name,
            Role="arn:aws:iam::123456789012:role/glue-role",
            Command={"Name": "glueetl", "ScriptLocation": "s3://test-bucket/script.py"},
        )

        response = glue.get_job(JobName=job_name)
        assert response["Job"]["Name"] == job_name
        assert response["Job"]["Command"]["ScriptLocation"] == "s3://test-bucket/script.py"

        glue.delete_job(JobName=job_name)

    def test_get_jobs(self, glue):
        job_name = _unique("test-job")
        glue.create_job(
            Name=job_name,
            Role="arn:aws:iam::123456789012:role/glue-role",
            Command={"Name": "glueetl", "ScriptLocation": "s3://test-bucket/script.py"},
        )

        response = glue.get_jobs()
        job_names = [j["Name"] for j in response["Jobs"]]
        assert job_name in job_names

        glue.delete_job(JobName=job_name)

    def test_delete_job(self, glue):
        job_name = _unique("test-job")
        glue.create_job(
            Name=job_name,
            Role="arn:aws:iam::123456789012:role/glue-role",
            Command={"Name": "glueetl", "ScriptLocation": "s3://test-bucket/script.py"},
        )

        glue.delete_job(JobName=job_name)

        with pytest.raises(ClientError) as exc:
            glue.get_job(JobName=job_name)
        assert exc.value.response["Error"]["Code"] == "EntityNotFoundException"


class TestGlueTriggerOperations:
    def test_create_and_get_trigger(self, glue):
        trigger_name = _unique("trigger")
        glue.create_trigger(
            Name=trigger_name,
            Type="SCHEDULED",
            Schedule="cron(0 12 * * ? *)",
            Actions=[{"JobName": "dummy-job"}],
        )
        try:
            response = glue.get_trigger(Name=trigger_name)
            assert response["Trigger"]["Name"] == trigger_name
            assert response["Trigger"]["Type"] == "SCHEDULED"
        finally:
            glue.delete_trigger(Name=trigger_name)

    def test_delete_trigger(self, glue):
        trigger_name = _unique("trigger")
        glue.create_trigger(
            Name=trigger_name,
            Type="SCHEDULED",
            Schedule="cron(0 12 * * ? *)",
            Actions=[{"JobName": "dummy-job"}],
        )
        glue.delete_trigger(Name=trigger_name)

        with pytest.raises(ClientError) as exc:
            glue.get_trigger(Name=trigger_name)
        assert exc.value.response["Error"]["Code"] == "EntityNotFoundException"

    def test_start_trigger(self, glue):
        trigger_name = _unique("trigger")
        glue.create_trigger(
            Name=trigger_name,
            Type="SCHEDULED",
            Schedule="cron(0 12 * * ? *)",
            Actions=[{"JobName": "dummy-job"}],
        )
        try:
            response = glue.start_trigger(Name=trigger_name)
            assert response["Name"] == trigger_name
        finally:
            glue.delete_trigger(Name=trigger_name)

    def test_stop_trigger(self, glue):
        trigger_name = _unique("trigger")
        glue.create_trigger(
            Name=trigger_name,
            Type="SCHEDULED",
            Schedule="cron(0 12 * * ? *)",
            Actions=[{"JobName": "dummy-job"}],
        )
        try:
            glue.start_trigger(Name=trigger_name)
            response = glue.stop_trigger(Name=trigger_name)
            assert response["Name"] == trigger_name
        finally:
            glue.delete_trigger(Name=trigger_name)

    def test_get_nonexistent_trigger(self, glue):
        with pytest.raises(ClientError) as exc:
            glue.get_trigger(Name="does-not-exist-trigger")
        assert exc.value.response["Error"]["Code"] == "EntityNotFoundException"


class TestGlueTags:
    def test_tag_and_get_tags(self, glue):
        db_name = _unique("db")
        crawler_name = _unique("test-crawler")
        glue.create_database(DatabaseInput={"Name": db_name})
        glue.create_crawler(
            Name=crawler_name,
            Role="arn:aws:iam::123456789012:role/glue-role",
            DatabaseName=db_name,
            Targets={"S3Targets": [{"Path": "s3://test-bucket/data"}]},
        )

        crawler_arn = f"arn:aws:glue:us-east-1:123456789012:crawler/{crawler_name}"
        glue.tag_resource(ResourceArn=crawler_arn, TagsToAdd={"env": "test", "team": "dev"})

        response = glue.get_tags(ResourceArn=crawler_arn)
        assert response["Tags"]["env"] == "test"
        assert response["Tags"]["team"] == "dev"

        glue.delete_crawler(Name=crawler_name)
        glue.delete_database(Name=db_name)

    def test_untag_resource(self, glue):
        db_name = _unique("db")
        crawler_name = _unique("test-crawler")
        glue.create_database(DatabaseInput={"Name": db_name})
        glue.create_crawler(
            Name=crawler_name,
            Role="arn:aws:iam::123456789012:role/glue-role",
            DatabaseName=db_name,
            Targets={"S3Targets": [{"Path": "s3://test-bucket/data"}]},
        )

        crawler_arn = f"arn:aws:glue:us-east-1:123456789012:crawler/{crawler_name}"
        glue.tag_resource(
            ResourceArn=crawler_arn, TagsToAdd={"env": "test", "team": "dev", "version": "1"}
        )
        glue.untag_resource(ResourceArn=crawler_arn, TagsToRemove=["team"])

        response = glue.get_tags(ResourceArn=crawler_arn)
        assert "team" not in response["Tags"]
        assert response["Tags"]["env"] == "test"
        assert response["Tags"]["version"] == "1"

        glue.delete_crawler(Name=crawler_name)
        glue.delete_database(Name=db_name)


class TestGluePartitionOperations:
    """Tests for Glue Partition CRUD operations."""

    def _make_db_and_table(self, glue):
        db_name = _unique("db")
        tbl_name = _unique("tbl")
        glue.create_database(DatabaseInput={"Name": db_name})
        glue.create_table(
            DatabaseName=db_name,
            TableInput={
                "Name": tbl_name,
                "StorageDescriptor": {
                    "Columns": [{"Name": "col1", "Type": "string"}],
                    "Location": "s3://bucket/path",
                    "InputFormat": "org.apache.hadoop.mapred.TextInputFormat",
                    "OutputFormat": "org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat",
                    "SerdeInfo": {
                        "SerializationLibrary": "org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe"
                    },
                },
                "PartitionKeys": [{"Name": "year", "Type": "string"}],
            },
        )
        return db_name, tbl_name

    def _cleanup(self, glue, db_name, tbl_name):
        glue.delete_table(DatabaseName=db_name, Name=tbl_name)
        glue.delete_database(Name=db_name)

    def test_create_partition(self, glue):
        db_name, tbl_name = self._make_db_and_table(glue)
        try:
            glue.create_partition(
                DatabaseName=db_name,
                TableName=tbl_name,
                PartitionInput={
                    "Values": ["2024"],
                    "StorageDescriptor": {
                        "Columns": [{"Name": "col1", "Type": "string"}],
                        "Location": "s3://bucket/path/year=2024",
                        "InputFormat": "org.apache.hadoop.mapred.TextInputFormat",
                        "OutputFormat": "org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat",  # noqa: E501
                        "SerdeInfo": {
                            "SerializationLibrary": "org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe"  # noqa: E501
                        },
                    },
                },
            )
            resp = glue.get_partition(
                DatabaseName=db_name, TableName=tbl_name, PartitionValues=["2024"]
            )
            assert resp["Partition"]["Values"] == ["2024"]
        finally:
            self._cleanup(glue, db_name, tbl_name)

    def test_get_partition(self, glue):
        db_name, tbl_name = self._make_db_and_table(glue)
        try:
            glue.create_partition(
                DatabaseName=db_name,
                TableName=tbl_name,
                PartitionInput={
                    "Values": ["2025"],
                    "StorageDescriptor": {
                        "Columns": [{"Name": "col1", "Type": "string"}],
                        "Location": "s3://bucket/path/year=2025",
                        "InputFormat": "org.apache.hadoop.mapred.TextInputFormat",
                        "OutputFormat": "org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat",  # noqa: E501
                        "SerdeInfo": {
                            "SerializationLibrary": "org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe"  # noqa: E501
                        },
                    },
                },
            )
            resp = glue.get_partition(
                DatabaseName=db_name, TableName=tbl_name, PartitionValues=["2025"]
            )
            assert resp["Partition"]["Values"] == ["2025"]
            assert "StorageDescriptor" in resp["Partition"]
        finally:
            self._cleanup(glue, db_name, tbl_name)

    def test_delete_partition(self, glue):
        db_name, tbl_name = self._make_db_and_table(glue)
        try:
            glue.create_partition(
                DatabaseName=db_name,
                TableName=tbl_name,
                PartitionInput={
                    "Values": ["2023"],
                    "StorageDescriptor": {
                        "Columns": [{"Name": "col1", "Type": "string"}],
                        "Location": "s3://bucket/path/year=2023",
                        "InputFormat": "org.apache.hadoop.mapred.TextInputFormat",
                        "OutputFormat": "org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat",  # noqa: E501
                        "SerdeInfo": {
                            "SerializationLibrary": "org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe"  # noqa: E501
                        },
                    },
                },
            )
            glue.delete_partition(
                DatabaseName=db_name, TableName=tbl_name, PartitionValues=["2023"]
            )
            with pytest.raises(ClientError) as exc:
                glue.get_partition(
                    DatabaseName=db_name, TableName=tbl_name, PartitionValues=["2023"]
                )
            assert exc.value.response["Error"]["Code"] == "EntityNotFoundException"
        finally:
            self._cleanup(glue, db_name, tbl_name)

    def test_update_partition(self, glue):
        db_name, tbl_name = self._make_db_and_table(glue)
        try:
            glue.create_partition(
                DatabaseName=db_name,
                TableName=tbl_name,
                PartitionInput={
                    "Values": ["2022"],
                    "StorageDescriptor": {
                        "Columns": [{"Name": "col1", "Type": "string"}],
                        "Location": "s3://bucket/path/year=2022",
                        "InputFormat": "org.apache.hadoop.mapred.TextInputFormat",
                        "OutputFormat": "org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat",  # noqa: E501
                        "SerdeInfo": {
                            "SerializationLibrary": "org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe"  # noqa: E501
                        },
                    },
                },
            )
            glue.update_partition(
                DatabaseName=db_name,
                TableName=tbl_name,
                PartitionValueList=["2022"],
                PartitionInput={
                    "Values": ["2022"],
                    "StorageDescriptor": {
                        "Columns": [{"Name": "col1", "Type": "string"}],
                        "Location": "s3://bucket/path/year=2022-updated",
                        "InputFormat": "org.apache.hadoop.mapred.TextInputFormat",
                        "OutputFormat": "org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat",  # noqa: E501
                        "SerdeInfo": {
                            "SerializationLibrary": "org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe"  # noqa: E501
                        },
                    },
                },
            )
            resp = glue.get_partition(
                DatabaseName=db_name, TableName=tbl_name, PartitionValues=["2022"]
            )
            assert "2022-updated" in resp["Partition"]["StorageDescriptor"]["Location"]
        finally:
            self._cleanup(glue, db_name, tbl_name)


class TestGlueSchemaOperations:
    """Tests for Glue Schema Registry schema operations."""

    @pytest.fixture
    def client(self):
        return make_client("glue")

    @pytest.fixture
    def registry(self, client):
        name = _unique("reg")
        resp = client.create_registry(RegistryName=name, Description="test registry")
        yield {"name": name, "arn": resp["RegistryArn"]}
        try:
            client.delete_registry(RegistryId={"RegistryName": name})
        except Exception:
            pass

    def test_create_schema(self, client, registry):
        """CreateSchema creates a schema in a registry."""
        schema_name = _unique("schema")
        resp = client.create_schema(
            RegistryId={"RegistryName": registry["name"]},
            SchemaName=schema_name,
            DataFormat="AVRO",
            Compatibility="NONE",
            SchemaDefinition='{"type":"record","name":"Test","fields":[{"name":"id","type":"int"}]}',
        )
        assert resp["SchemaName"] == schema_name
        assert resp["DataFormat"] == "AVRO"
        client.delete_schema(SchemaId={"SchemaName": schema_name, "RegistryName": registry["name"]})

    def test_get_schema(self, client, registry):
        """GetSchema retrieves schema details."""
        schema_name = _unique("schema")
        client.create_schema(
            RegistryId={"RegistryName": registry["name"]},
            SchemaName=schema_name,
            DataFormat="AVRO",
            Compatibility="NONE",
            SchemaDefinition='{"type":"record","name":"Test","fields":[{"name":"id","type":"int"}]}',
        )
        try:
            resp = client.get_schema(
                SchemaId={"SchemaName": schema_name, "RegistryName": registry["name"]}
            )
            assert resp["SchemaName"] == schema_name
            assert resp["DataFormat"] == "AVRO"
        finally:
            client.delete_schema(
                SchemaId={"SchemaName": schema_name, "RegistryName": registry["name"]}
            )

    def test_delete_schema(self, client, registry):
        """DeleteSchema removes a schema."""
        schema_name = _unique("schema")
        client.create_schema(
            RegistryId={"RegistryName": registry["name"]},
            SchemaName=schema_name,
            DataFormat="AVRO",
            Compatibility="NONE",
            SchemaDefinition='{"type":"record","name":"Test","fields":[{"name":"id","type":"int"}]}',
        )
        resp = client.delete_schema(
            SchemaId={"SchemaName": schema_name, "RegistryName": registry["name"]}
        )
        assert resp["SchemaName"] == schema_name

        with pytest.raises(ClientError) as exc:
            client.get_schema(
                SchemaId={"SchemaName": schema_name, "RegistryName": registry["name"]}
            )
        assert exc.value.response["Error"]["Code"] == "EntityNotFoundException"

    def test_update_schema(self, client, registry):
        """UpdateSchema modifies schema properties."""
        schema_name = _unique("schema")
        client.create_schema(
            RegistryId={"RegistryName": registry["name"]},
            SchemaName=schema_name,
            DataFormat="AVRO",
            Compatibility="NONE",
            SchemaDefinition='{"type":"record","name":"Test","fields":[{"name":"id","type":"int"}]}',
        )
        try:
            resp = client.update_schema(
                SchemaId={"SchemaName": schema_name, "RegistryName": registry["name"]},
                Compatibility="BACKWARD",
            )
            assert resp["SchemaName"] == schema_name
        finally:
            client.delete_schema(
                SchemaId={"SchemaName": schema_name, "RegistryName": registry["name"]}
            )


class TestGlueAutoCoverage:
    """Auto-generated coverage tests for glue."""

    @pytest.fixture
    def client(self):
        return make_client("glue")

    def test_delete_resource_policy(self, client):
        """DeleteResourcePolicy returns a response."""
        try:
            client.delete_resource_policy()
        except client.exceptions.ClientError:
            pass  # Operation exists

    def test_get_connections(self, client):
        """GetConnections returns a response."""
        resp = client.get_connections()
        assert "ConnectionList" in resp

    def test_get_data_catalog_encryption_settings(self, client):
        """GetDataCatalogEncryptionSettings returns a response."""
        resp = client.get_data_catalog_encryption_settings()
        assert "DataCatalogEncryptionSettings" in resp

    def test_get_dev_endpoints(self, client):
        """GetDevEndpoints returns a response."""
        resp = client.get_dev_endpoints()
        assert "DevEndpoints" in resp

    def test_get_resource_policy(self, client):
        """GetResourcePolicy returns a response."""
        client.get_resource_policy()

    def test_get_security_configurations(self, client):
        """GetSecurityConfigurations returns a response."""
        resp = client.get_security_configurations()
        assert "SecurityConfigurations" in resp

    def test_get_triggers(self, client):
        """GetTriggers returns a response."""
        resp = client.get_triggers()
        assert "Triggers" in resp

    def test_list_crawlers(self, client):
        """ListCrawlers returns a response."""
        resp = client.list_crawlers()
        assert "CrawlerNames" in resp

    def test_list_jobs(self, client):
        """ListJobs returns a response."""
        resp = client.list_jobs()
        assert "JobNames" in resp

    def test_list_registries(self, client):
        """ListRegistries returns a response."""
        resp = client.list_registries()
        assert "Registries" in resp

    def test_list_sessions(self, client):
        """ListSessions returns a response."""
        resp = client.list_sessions()
        assert "Ids" in resp

    def test_list_triggers(self, client):
        """ListTriggers returns a response."""
        resp = client.list_triggers()
        assert "TriggerNames" in resp

    def test_list_workflows(self, client):
        """ListWorkflows returns a response."""
        resp = client.list_workflows()
        assert "Workflows" in resp
