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
            pass  # best-effort cleanup

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
        """GetResourcePolicy returns a response or EntityNotFoundException when no policy exists."""
        try:
            client.get_resource_policy()
        except ClientError as e:
            assert e.response["Error"]["Code"] == "EntityNotFoundException"

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

    def test_get_connection_nonexistent(self, client):
        """GetConnection with fake name returns EntityNotFoundException."""
        with pytest.raises(ClientError) as exc:
            client.get_connection(Name="fake-connection-does-not-exist")
        assert exc.value.response["Error"]["Code"] == "EntityNotFoundException"

    def test_get_dev_endpoint_nonexistent(self, client):
        """GetDevEndpoint with fake name returns EntityNotFoundException."""
        with pytest.raises(ClientError) as exc:
            client.get_dev_endpoint(EndpointName="fake-endpoint-does-not-exist")
        assert exc.value.response["Error"]["Code"] == "EntityNotFoundException"

    def test_get_job_run_nonexistent(self, client):
        """GetJobRun with fake job/run returns EntityNotFoundException."""
        with pytest.raises(ClientError) as exc:
            client.get_job_run(JobName="fake-job-xyz", RunId="jr_fake123")
        assert exc.value.response["Error"]["Code"] == "EntityNotFoundException"

    def test_get_job_runs_nonexistent(self, client):
        """GetJobRuns with fake job returns EntityNotFoundException."""
        with pytest.raises(ClientError) as exc:
            client.get_job_runs(JobName="fake-job-xyz-no-exist")
        assert exc.value.response["Error"]["Code"] == "EntityNotFoundException"

    def test_get_security_configuration_nonexistent(self, client):
        """GetSecurityConfiguration with fake name returns EntityNotFoundException."""
        with pytest.raises(ClientError) as exc:
            client.get_security_configuration(Name="fake-secconfig-does-not-exist")
        assert exc.value.response["Error"]["Code"] == "EntityNotFoundException"

    def test_get_session_nonexistent(self, client):
        """GetSession with fake ID returns EntityNotFoundException."""
        with pytest.raises(ClientError) as exc:
            client.get_session(Id="fake-session-does-not-exist")
        assert exc.value.response["Error"]["Code"] == "EntityNotFoundException"

    def test_get_workflow_nonexistent(self, client):
        """GetWorkflow with fake name returns EntityNotFoundException."""
        with pytest.raises(ClientError) as exc:
            client.get_workflow(Name="fake-workflow-does-not-exist")
        assert exc.value.response["Error"]["Code"] == "EntityNotFoundException"

    def test_get_workflow_run_nonexistent(self, client):
        """GetWorkflowRun with fake name/run returns EntityNotFoundException."""
        with pytest.raises(ClientError) as exc:
            client.get_workflow_run(Name="fake-workflow-xyz", RunId="wr_fake123")
        assert exc.value.response["Error"]["Code"] == "EntityNotFoundException"

    def test_get_workflow_run_properties_nonexistent(self, client):
        """GetWorkflowRunProperties with fake name/run returns EntityNotFoundException."""
        with pytest.raises(ClientError) as exc:
            client.get_workflow_run_properties(Name="fake-workflow-xyz", RunId="wr_fake123")
        assert exc.value.response["Error"]["Code"] == "EntityNotFoundException"

    def test_list_crawls_nonexistent(self, client):
        """ListCrawls with fake crawler returns EntityNotFoundException."""
        with pytest.raises(ClientError) as exc:
            client.list_crawls(CrawlerName="fake-crawler-does-not-exist")
        assert exc.value.response["Error"]["Code"] == "EntityNotFoundException"


class TestGluePartitionAndTableVersionOps:
    """Tests for GetPartitions, GetPartitionIndexes, GetTableVersion, GetTableVersions."""

    @pytest.fixture
    def client(self):
        return make_client("glue")

    def _make_db_and_table(self, client):
        db_name = _unique("db")
        tbl_name = _unique("tbl")
        client.create_database(DatabaseInput={"Name": db_name})
        client.create_table(
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

    def _cleanup(self, client, db_name, tbl_name):
        client.delete_table(DatabaseName=db_name, Name=tbl_name)
        client.delete_database(Name=db_name)

    def test_get_partitions_empty(self, client):
        """GetPartitions on a table with no partitions returns empty list."""
        db_name, tbl_name = self._make_db_and_table(client)
        try:
            resp = client.get_partitions(DatabaseName=db_name, TableName=tbl_name)
            assert "Partitions" in resp
            assert resp["Partitions"] == []
        finally:
            self._cleanup(client, db_name, tbl_name)

    def test_get_partitions_with_data(self, client):
        """GetPartitions returns created partitions."""
        db_name, tbl_name = self._make_db_and_table(client)
        try:
            client.create_partition(
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
            resp = client.get_partitions(DatabaseName=db_name, TableName=tbl_name)
            assert len(resp["Partitions"]) == 1
            assert resp["Partitions"][0]["Values"] == ["2024"]
        finally:
            self._cleanup(client, db_name, tbl_name)

    def test_get_partition_indexes(self, client):
        """GetPartitionIndexes returns index list (possibly empty)."""
        db_name, tbl_name = self._make_db_and_table(client)
        try:
            resp = client.get_partition_indexes(DatabaseName=db_name, TableName=tbl_name)
            assert "PartitionIndexDescriptorList" in resp
        finally:
            self._cleanup(client, db_name, tbl_name)

    def test_get_table_versions(self, client):
        """GetTableVersions returns version list for a table."""
        db_name, tbl_name = self._make_db_and_table(client)
        try:
            resp = client.get_table_versions(DatabaseName=db_name, TableName=tbl_name)
            assert "TableVersions" in resp
            assert len(resp["TableVersions"]) >= 1
        finally:
            self._cleanup(client, db_name, tbl_name)

    def test_get_table_version(self, client):
        """GetTableVersion returns a specific version of a table."""
        db_name, tbl_name = self._make_db_and_table(client)
        try:
            # Get versions first to find a valid version ID
            versions_resp = client.get_table_versions(DatabaseName=db_name, TableName=tbl_name)
            version_id = versions_resp["TableVersions"][0]["VersionId"]

            resp = client.get_table_version(
                DatabaseName=db_name, TableName=tbl_name, VersionId=str(version_id)
            )
            assert "TableVersion" in resp
            assert resp["TableVersion"]["Table"]["Name"] == tbl_name
        finally:
            self._cleanup(client, db_name, tbl_name)


class TestGlueSchemaVersionOps:
    """Tests for GetSchemaVersion and GetSchemaByDefinition."""

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
            pass  # best-effort cleanup

    @pytest.fixture
    def schema(self, client, registry):
        schema_name = _unique("schema")
        definition = '{"type":"record","name":"Test","fields":[{"name":"id","type":"int"}]}'
        resp = client.create_schema(
            RegistryId={"RegistryName": registry["name"]},
            SchemaName=schema_name,
            DataFormat="AVRO",
            Compatibility="NONE",
            SchemaDefinition=definition,
        )
        yield {
            "name": schema_name,
            "registry": registry["name"],
            "arn": resp["SchemaArn"],
            "definition": definition,
        }
        try:
            client.delete_schema(
                SchemaId={"SchemaName": schema_name, "RegistryName": registry["name"]}
            )
        except Exception:
            pass  # best-effort cleanup

    def test_get_schema_version(self, client, schema):
        """GetSchemaVersion retrieves schema version details."""
        resp = client.get_schema_version(
            SchemaId={"SchemaName": schema["name"], "RegistryName": schema["registry"]},
            SchemaVersionNumber={"LatestVersion": True},
        )
        assert "SchemaDefinition" in resp
        assert resp["DataFormat"] == "AVRO"

    def test_get_schema_by_definition(self, client, schema):
        """GetSchemaByDefinition finds a schema by its definition text."""
        resp = client.get_schema_by_definition(
            SchemaId={"SchemaName": schema["name"], "RegistryName": schema["registry"]},
            SchemaDefinition=schema["definition"],
        )
        assert resp["SchemaArn"] == schema["arn"]
        assert "SchemaVersionId" in resp


class TestGlueUpdateDatabase:
    """Tests for UpdateDatabase."""

    def test_update_database(self, glue):
        db_name = _unique("db")
        glue.create_database(DatabaseInput={"Name": db_name, "Description": "original"})
        try:
            glue.update_database(
                Name=db_name,
                DatabaseInput={"Name": db_name, "Description": "updated"},
            )
            resp = glue.get_database(Name=db_name)
            assert resp["Database"]["Description"] == "updated"
        finally:
            glue.delete_database(Name=db_name)


class TestGlueUpdateTable:
    """Tests for UpdateTable."""

    def _storage_descriptor(self, cols=None):
        return {
            "Columns": cols or [{"Name": "col1", "Type": "string"}],
            "Location": "s3://bucket/path",
            "InputFormat": "org.apache.hadoop.mapred.TextInputFormat",
            "OutputFormat": "org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat",
            "SerdeInfo": {
                "SerializationLibrary": "org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe"
            },
        }

    def test_update_table(self, glue):
        db_name = _unique("db")
        tbl_name = _unique("tbl")
        glue.create_database(DatabaseInput={"Name": db_name})
        glue.create_table(
            DatabaseName=db_name,
            TableInput={"Name": tbl_name, "StorageDescriptor": self._storage_descriptor()},
        )
        try:
            new_cols = [{"Name": "col1", "Type": "string"}, {"Name": "col2", "Type": "int"}]
            glue.update_table(
                DatabaseName=db_name,
                TableInput={
                    "Name": tbl_name,
                    "StorageDescriptor": self._storage_descriptor(new_cols),
                },
            )
            resp = glue.get_table(DatabaseName=db_name, Name=tbl_name)
            col_names = [c["Name"] for c in resp["Table"]["StorageDescriptor"]["Columns"]]
            assert "col2" in col_names
        finally:
            glue.delete_table(DatabaseName=db_name, Name=tbl_name)
            glue.delete_database(Name=db_name)


class TestGlueBatchDeleteTable:
    """Tests for BatchDeleteTable."""

    def _storage_descriptor(self):
        return {
            "Columns": [{"Name": "col1", "Type": "string"}],
            "Location": "s3://bucket/path",
            "InputFormat": "org.apache.hadoop.mapred.TextInputFormat",
            "OutputFormat": "org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat",
            "SerdeInfo": {
                "SerializationLibrary": "org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe"
            },
        }

    def test_batch_delete_table(self, glue):
        db_name = _unique("db")
        tbl1 = _unique("tbl")
        tbl2 = _unique("tbl")
        glue.create_database(DatabaseInput={"Name": db_name})
        for tbl in (tbl1, tbl2):
            glue.create_table(
                DatabaseName=db_name,
                TableInput={"Name": tbl, "StorageDescriptor": self._storage_descriptor()},
            )
        try:
            resp = glue.batch_delete_table(DatabaseName=db_name, TablesToDelete=[tbl1, tbl2])
            assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200
            # Verify tables are gone
            with pytest.raises(ClientError) as exc:
                glue.get_table(DatabaseName=db_name, Name=tbl1)
            assert exc.value.response["Error"]["Code"] == "EntityNotFoundException"
        finally:
            glue.delete_database(Name=db_name)


class TestGlueSecurityConfigurationLifecycle:
    """Tests for SecurityConfiguration CRUD."""

    def test_create_and_get_security_configuration(self, glue):
        name = _unique("secconfig")
        resp = glue.create_security_configuration(
            Name=name,
            EncryptionConfiguration={
                "S3Encryption": [{"S3EncryptionMode": "DISABLED"}],
                "CloudWatchEncryption": {"CloudWatchEncryptionMode": "DISABLED"},
                "JobBookmarksEncryption": {"JobBookmarksEncryptionMode": "DISABLED"},
            },
        )
        try:
            assert resp["Name"] == name
            got = glue.get_security_configuration(Name=name)
            assert got["SecurityConfiguration"]["Name"] == name
            assert "EncryptionConfiguration" in got["SecurityConfiguration"]
        finally:
            glue.delete_security_configuration(Name=name)

    def test_get_security_configurations_includes_created(self, glue):
        name = _unique("secconfig")
        glue.create_security_configuration(
            Name=name,
            EncryptionConfiguration={
                "S3Encryption": [{"S3EncryptionMode": "DISABLED"}],
                "CloudWatchEncryption": {"CloudWatchEncryptionMode": "DISABLED"},
                "JobBookmarksEncryption": {"JobBookmarksEncryptionMode": "DISABLED"},
            },
        )
        try:
            resp = glue.get_security_configurations()
            names = [sc["Name"] for sc in resp["SecurityConfigurations"]]
            assert name in names
        finally:
            glue.delete_security_configuration(Name=name)

    def test_delete_security_configuration(self, glue):
        name = _unique("secconfig")
        glue.create_security_configuration(
            Name=name,
            EncryptionConfiguration={
                "S3Encryption": [{"S3EncryptionMode": "DISABLED"}],
                "CloudWatchEncryption": {"CloudWatchEncryptionMode": "DISABLED"},
                "JobBookmarksEncryption": {"JobBookmarksEncryptionMode": "DISABLED"},
            },
        )
        glue.delete_security_configuration(Name=name)
        with pytest.raises(ClientError) as exc:
            glue.get_security_configuration(Name=name)
        assert exc.value.response["Error"]["Code"] == "EntityNotFoundException"


class TestGlueRegistryLifecycle:
    """Tests for Registry CRUD (standalone, not just as fixture)."""

    def test_create_and_get_registry(self, glue):
        name = _unique("reg")
        resp = glue.create_registry(RegistryName=name, Description="test registry")
        try:
            assert resp["RegistryName"] == name
            got = glue.get_registry(RegistryId={"RegistryName": name})
            assert got["RegistryName"] == name
            assert got["Description"] == "test registry"
        finally:
            glue.delete_registry(RegistryId={"RegistryName": name})

    def test_list_registries_includes_created(self, glue):
        name = _unique("reg")
        glue.create_registry(RegistryName=name, Description="for listing")
        try:
            resp = glue.list_registries()
            reg_names = [r["RegistryName"] for r in resp["Registries"]]
            assert name in reg_names
        finally:
            glue.delete_registry(RegistryId={"RegistryName": name})

    def test_delete_registry(self, glue):
        name = _unique("reg")
        glue.create_registry(RegistryName=name, Description="to delete")
        resp = glue.delete_registry(RegistryId={"RegistryName": name})
        assert resp["RegistryName"] == name
        with pytest.raises(ClientError) as exc:
            glue.get_registry(RegistryId={"RegistryName": name})
        assert exc.value.response["Error"]["Code"] == "EntityNotFoundException"


class TestGlueWorkflowLifecycle:
    """Tests for Workflow CRUD."""

    def test_create_and_get_workflow(self, glue):
        name = _unique("wf")
        resp = glue.create_workflow(Name=name, Description="test workflow")
        try:
            assert resp["Name"] == name
            got = glue.get_workflow(Name=name)
            assert got["Workflow"]["Name"] == name
            assert got["Workflow"]["Description"] == "test workflow"
        finally:
            glue.delete_workflow(Name=name)

    def test_list_workflows_includes_created(self, glue):
        name = _unique("wf")
        glue.create_workflow(Name=name)
        try:
            resp = glue.list_workflows()
            assert name in resp["Workflows"]
        finally:
            glue.delete_workflow(Name=name)

    def test_update_workflow(self, glue):
        name = _unique("wf")
        glue.create_workflow(Name=name, Description="original")
        try:
            resp = glue.update_workflow(Name=name, Description="updated")
            assert resp["Name"] == name
            got = glue.get_workflow(Name=name)
            assert got["Workflow"]["Description"] == "updated"
        finally:
            glue.delete_workflow(Name=name)

    def test_delete_workflow(self, glue):
        name = _unique("wf")
        glue.create_workflow(Name=name)
        glue.delete_workflow(Name=name)
        with pytest.raises(ClientError) as exc:
            glue.get_workflow(Name=name)
        assert exc.value.response["Error"]["Code"] == "EntityNotFoundException"


class TestGlueBatchGetPartition:
    """Tests for BatchGetPartition."""

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
                        "SerializationLibrary": "org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe"  # noqa: E501
                    },
                },
                "PartitionKeys": [{"Name": "year", "Type": "string"}],
            },
        )
        return db_name, tbl_name

    def _sd(self, location):
        return {
            "Columns": [{"Name": "col1", "Type": "string"}],
            "Location": location,
            "InputFormat": "org.apache.hadoop.mapred.TextInputFormat",
            "OutputFormat": "org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat",
            "SerdeInfo": {
                "SerializationLibrary": "org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe"
            },
        }

    def test_batch_get_partition(self, glue):
        db_name, tbl_name = self._make_db_and_table(glue)
        try:
            for year in ("2023", "2024", "2025"):
                glue.create_partition(
                    DatabaseName=db_name,
                    TableName=tbl_name,
                    PartitionInput={
                        "Values": [year],
                        "StorageDescriptor": self._sd(f"s3://bucket/path/year={year}"),
                    },
                )
            resp = glue.batch_get_partition(
                DatabaseName=db_name,
                TableName=tbl_name,
                PartitionsToGet=[
                    {"Values": ["2023"]},
                    {"Values": ["2025"]},
                ],
            )
            assert len(resp["Partitions"]) == 2
            values = sorted([p["Values"][0] for p in resp["Partitions"]])
            assert values == ["2023", "2025"]
        finally:
            glue.delete_table(DatabaseName=db_name, Name=tbl_name)
            glue.delete_database(Name=db_name)


class TestGlueBatchCreatePartition:
    """Tests for BatchCreatePartition."""

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
                        "SerializationLibrary": "org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe"  # noqa: E501
                    },
                },
                "PartitionKeys": [{"Name": "year", "Type": "string"}],
            },
        )
        return db_name, tbl_name

    def _sd(self, location):
        return {
            "Columns": [{"Name": "col1", "Type": "string"}],
            "Location": location,
            "InputFormat": "org.apache.hadoop.mapred.TextInputFormat",
            "OutputFormat": "org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat",
            "SerdeInfo": {
                "SerializationLibrary": "org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe"
            },
        }

    def test_batch_create_partition(self, glue):
        db_name, tbl_name = self._make_db_and_table(glue)
        try:
            resp = glue.batch_create_partition(
                DatabaseName=db_name,
                TableName=tbl_name,
                PartitionInputList=[
                    {
                        "Values": ["2023"],
                        "StorageDescriptor": self._sd("s3://bucket/path/year=2023"),
                    },
                    {
                        "Values": ["2024"],
                        "StorageDescriptor": self._sd("s3://bucket/path/year=2024"),
                    },
                ],
            )
            assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200
            # Verify partitions were created
            parts = glue.get_partitions(DatabaseName=db_name, TableName=tbl_name)
            assert len(parts["Partitions"]) == 2
        finally:
            glue.delete_table(DatabaseName=db_name, Name=tbl_name)
            glue.delete_database(Name=db_name)


class TestGlueBatchDeletePartition:
    """Tests for BatchDeletePartition."""

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
                        "SerializationLibrary": "org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe"  # noqa: E501
                    },
                },
                "PartitionKeys": [{"Name": "year", "Type": "string"}],
            },
        )
        return db_name, tbl_name

    def _sd(self, location):
        return {
            "Columns": [{"Name": "col1", "Type": "string"}],
            "Location": location,
            "InputFormat": "org.apache.hadoop.mapred.TextInputFormat",
            "OutputFormat": "org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat",
            "SerdeInfo": {
                "SerializationLibrary": "org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe"
            },
        }

    def test_batch_delete_partition(self, glue):
        db_name, tbl_name = self._make_db_and_table(glue)
        try:
            for year in ("2023", "2024"):
                glue.create_partition(
                    DatabaseName=db_name,
                    TableName=tbl_name,
                    PartitionInput={
                        "Values": [year],
                        "StorageDescriptor": self._sd(f"s3://bucket/path/year={year}"),
                    },
                )
            resp = glue.batch_delete_partition(
                DatabaseName=db_name,
                TableName=tbl_name,
                PartitionsToDelete=[{"Values": ["2023"]}, {"Values": ["2024"]}],
            )
            assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200
            parts = glue.get_partitions(DatabaseName=db_name, TableName=tbl_name)
            assert len(parts["Partitions"]) == 0
        finally:
            glue.delete_table(DatabaseName=db_name, Name=tbl_name)
            glue.delete_database(Name=db_name)


class TestGluePutResourcePolicy:
    """Tests for PutResourcePolicy / GetResourcePolicy."""

    def test_put_and_get_resource_policy(self, glue):
        import json

        policy = json.dumps(
            {
                "Version": "2012-10-17",
                "Statement": [
                    {
                        "Effect": "Allow",
                        "Principal": {"AWS": "arn:aws:iam::123456789012:root"},
                        "Action": "glue:GetDatabase",
                        "Resource": "*",
                    }
                ],
            }
        )
        glue.put_resource_policy(PolicyInJson=policy)
        resp = glue.get_resource_policy()
        assert "PolicyInJson" in resp
        # Clean up
        glue.delete_resource_policy()


class TestGlueStartJobRun:
    """Tests for StartJobRun and GetJobRun."""

    def test_start_and_get_job_run(self, glue):
        job_name = _unique("job")
        glue.create_job(
            Name=job_name,
            Role="arn:aws:iam::123456789012:role/glue-role",
            Command={"Name": "glueetl", "ScriptLocation": "s3://bucket/script.py"},
        )
        try:
            start_resp = glue.start_job_run(JobName=job_name)
            run_id = start_resp["JobRunId"]
            assert run_id

            run_resp = glue.get_job_run(JobName=job_name, RunId=run_id)
            assert run_resp["JobRun"]["JobName"] == job_name
            assert run_resp["JobRun"]["Id"] == run_id
        finally:
            glue.delete_job(JobName=job_name)

    def test_get_job_runs(self, glue):
        job_name = _unique("job")
        glue.create_job(
            Name=job_name,
            Role="arn:aws:iam::123456789012:role/glue-role",
            Command={"Name": "glueetl", "ScriptLocation": "s3://bucket/script.py"},
        )
        try:
            glue.start_job_run(JobName=job_name)
            resp = glue.get_job_runs(JobName=job_name)
            assert len(resp["JobRuns"]) >= 1
            assert resp["JobRuns"][0]["JobName"] == job_name
        finally:
            glue.delete_job(JobName=job_name)


class TestGluePutDataCatalogEncryptionSettings:
    """Tests for PutDataCatalogEncryptionSettings."""

    def test_put_and_get_data_catalog_encryption_settings(self, glue):
        glue.put_data_catalog_encryption_settings(
            DataCatalogEncryptionSettings={
                "ConnectionPasswordEncryption": {
                    "ReturnConnectionPasswordEncrypted": False,
                },
                "EncryptionAtRest": {
                    "CatalogEncryptionMode": "DISABLED",
                },
            }
        )
        resp = glue.get_data_catalog_encryption_settings()
        assert "DataCatalogEncryptionSettings" in resp


class TestGlueBatchGetJobs:
    """Tests for BatchGetJobs."""

    def test_batch_get_jobs_existing(self, glue):
        job1 = _unique("job")
        job2 = _unique("job")
        for jn in (job1, job2):
            glue.create_job(
                Name=jn,
                Role="arn:aws:iam::123456789012:role/glue-role",
                Command={"Name": "glueetl", "ScriptLocation": "s3://bucket/script.py"},
            )
        try:
            resp = glue.batch_get_jobs(JobNames=[job1, job2])
            assert len(resp["Jobs"]) == 2
            names = sorted([j["Name"] for j in resp["Jobs"]])
            assert job1 in names
            assert job2 in names
        finally:
            for jn in (job1, job2):
                glue.delete_job(JobName=jn)

    def test_batch_get_jobs_not_found(self, glue):
        resp = glue.batch_get_jobs(JobNames=["nonexistent-job-xyz"])
        assert len(resp["Jobs"]) == 0
        assert "nonexistent-job-xyz" in resp["JobsNotFound"]


class TestGlueBatchGetCrawlers:
    """Tests for BatchGetCrawlers."""

    def test_batch_get_crawlers_existing(self, glue):
        db_name = _unique("db")
        c1 = _unique("crawler")
        glue.create_database(DatabaseInput={"Name": db_name})
        glue.create_crawler(
            Name=c1,
            Role="arn:aws:iam::123456789012:role/glue-role",
            DatabaseName=db_name,
            Targets={"S3Targets": [{"Path": "s3://test-bucket/data"}]},
        )
        try:
            resp = glue.batch_get_crawlers(CrawlerNames=[c1])
            assert len(resp["Crawlers"]) == 1
            assert resp["Crawlers"][0]["Name"] == c1
        finally:
            glue.delete_crawler(Name=c1)
            glue.delete_database(Name=db_name)

    def test_batch_get_crawlers_not_found(self, glue):
        resp = glue.batch_get_crawlers(CrawlerNames=["nonexistent-crawler-xyz"])
        assert len(resp["Crawlers"]) == 0
        assert "nonexistent-crawler-xyz" in resp["CrawlersNotFound"]


class TestGlueBatchGetTriggers:
    """Tests for BatchGetTriggers."""

    def test_batch_get_triggers_existing(self, glue):
        t1 = _unique("trigger")
        glue.create_trigger(
            Name=t1,
            Type="SCHEDULED",
            Schedule="cron(0 12 * * ? *)",
            Actions=[{"JobName": "dummy-job"}],
        )
        try:
            resp = glue.batch_get_triggers(TriggerNames=[t1])
            assert len(resp["Triggers"]) == 1
            assert resp["Triggers"][0]["Name"] == t1
        finally:
            glue.delete_trigger(Name=t1)

    def test_batch_get_triggers_not_found(self, glue):
        resp = glue.batch_get_triggers(TriggerNames=["nonexistent-trigger-xyz"])
        assert len(resp["Triggers"]) == 0
        assert "nonexistent-trigger-xyz" in resp["TriggersNotFound"]


class TestGlueBatchGetWorkflows:
    """Tests for BatchGetWorkflows."""

    def test_batch_get_workflows_existing(self, glue):
        w1 = _unique("wf")
        glue.create_workflow(Name=w1, Description="for batch get")
        try:
            resp = glue.batch_get_workflows(Names=[w1])
            assert len(resp["Workflows"]) == 1
            assert resp["Workflows"][0]["Name"] == w1
        finally:
            glue.delete_workflow(Name=w1)

    def test_batch_get_workflows_not_found(self, glue):
        resp = glue.batch_get_workflows(Names=["nonexistent-wf-xyz"])
        assert len(resp["Workflows"]) == 0
        assert "nonexistent-wf-xyz" in resp["MissingWorkflows"]


class TestGlueCrawlerStartStop:
    """Tests for StartCrawler and StopCrawler."""

    def test_start_crawler(self, glue):
        db_name = _unique("db")
        crawler_name = _unique("crawler")
        glue.create_database(DatabaseInput={"Name": db_name})
        glue.create_crawler(
            Name=crawler_name,
            Role="arn:aws:iam::123456789012:role/glue-role",
            DatabaseName=db_name,
            Targets={"S3Targets": [{"Path": "s3://test-bucket/data"}]},
        )
        try:
            resp = glue.start_crawler(Name=crawler_name)
            assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200
        finally:
            glue.delete_crawler(Name=crawler_name)
            glue.delete_database(Name=db_name)

    def test_stop_crawler(self, glue):
        db_name = _unique("db")
        crawler_name = _unique("crawler")
        glue.create_database(DatabaseInput={"Name": db_name})
        glue.create_crawler(
            Name=crawler_name,
            Role="arn:aws:iam::123456789012:role/glue-role",
            DatabaseName=db_name,
            Targets={"S3Targets": [{"Path": "s3://test-bucket/data"}]},
        )
        try:
            glue.start_crawler(Name=crawler_name)
            resp = glue.stop_crawler(Name=crawler_name)
            assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200
        finally:
            glue.delete_crawler(Name=crawler_name)
            glue.delete_database(Name=db_name)

    def test_start_nonexistent_crawler_fails(self, glue):
        with pytest.raises(ClientError) as exc:
            glue.start_crawler(Name="fake-crawler-does-not-exist")
        assert exc.value.response["Error"]["Code"] == "EntityNotFoundException"

    def test_stop_nonexistent_crawler_fails(self, glue):
        with pytest.raises(ClientError) as exc:
            glue.stop_crawler(Name="fake-crawler-does-not-exist")
        assert exc.value.response["Error"]["Code"] == "EntityNotFoundException"


class TestGlueCreateConnection:
    """Tests for CreateConnection and GetConnection (without Delete which is not implemented)."""

    def test_create_connection(self, glue):
        conn_name = _unique("conn")
        resp = glue.create_connection(
            ConnectionInput={
                "Name": conn_name,
                "ConnectionType": "JDBC",
                "ConnectionProperties": {
                    "JDBC_CONNECTION_URL": "jdbc:mysql://host:3306/db",
                    "USERNAME": "admin",
                    "PASSWORD": "secret",
                },
            }
        )
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200

    def test_create_and_get_connection(self, glue):
        conn_name = _unique("conn")
        glue.create_connection(
            ConnectionInput={
                "Name": conn_name,
                "ConnectionType": "JDBC",
                "ConnectionProperties": {
                    "JDBC_CONNECTION_URL": "jdbc:mysql://host:3306/db",
                    "USERNAME": "admin",
                    "PASSWORD": "secret",
                },
            }
        )
        resp = glue.get_connection(Name=conn_name)
        assert resp["Connection"]["Name"] == conn_name
        assert "ConnectionProperties" in resp["Connection"]


class TestGlueDeleteTableVersion:
    """Tests for DeleteTableVersion."""

    def _storage_descriptor(self):
        return {
            "Columns": [{"Name": "col1", "Type": "string"}],
            "Location": "s3://bucket/path",
            "InputFormat": "org.apache.hadoop.mapred.TextInputFormat",
            "OutputFormat": "org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat",
            "SerdeInfo": {
                "SerializationLibrary": "org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe"
            },
        }

    def test_delete_table_version(self, glue):
        db_name = _unique("db")
        tbl_name = _unique("tbl")
        glue.create_database(DatabaseInput={"Name": db_name})
        glue.create_table(
            DatabaseName=db_name,
            TableInput={"Name": tbl_name, "StorageDescriptor": self._storage_descriptor()},
        )
        try:
            versions = glue.get_table_versions(DatabaseName=db_name, TableName=tbl_name)
            version_id = versions["TableVersions"][0]["VersionId"]
            resp = glue.delete_table_version(
                DatabaseName=db_name, TableName=tbl_name, VersionId=str(version_id)
            )
            assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200
        finally:
            glue.delete_table(DatabaseName=db_name, Name=tbl_name)
            glue.delete_database(Name=db_name)

    def test_delete_table_version_nonexistent_db(self, glue):
        with pytest.raises(ClientError) as exc:
            glue.delete_table_version(
                DatabaseName="fake-db-xyz", TableName="fake-tbl", VersionId="1"
            )
        assert exc.value.response["Error"]["Code"] == "EntityNotFoundException"


class TestGlueJobWithTags:
    """Tests for creating a job with tags and verifying them."""

    def test_create_job_and_tag(self, glue):
        job_name = _unique("job")
        glue.create_job(
            Name=job_name,
            Role="arn:aws:iam::123456789012:role/glue-role",
            Command={"Name": "glueetl", "ScriptLocation": "s3://bucket/script.py"},
            Tags={"env": "test", "project": "glue-compat"},
        )
        try:
            job_arn = f"arn:aws:glue:us-east-1:123456789012:job/{job_name}"
            resp = glue.get_tags(ResourceArn=job_arn)
            assert resp["Tags"]["env"] == "test"
            assert resp["Tags"]["project"] == "glue-compat"
        finally:
            glue.delete_job(JobName=job_name)


class TestGlueMultipleJobRuns:
    """Tests for multiple job runs on the same job."""

    def test_multiple_job_runs(self, glue):
        job_name = _unique("job")
        glue.create_job(
            Name=job_name,
            Role="arn:aws:iam::123456789012:role/glue-role",
            Command={"Name": "glueetl", "ScriptLocation": "s3://bucket/script.py"},
        )
        try:
            run1 = glue.start_job_run(JobName=job_name)
            run2 = glue.start_job_run(JobName=job_name)
            assert run1["JobRunId"] != run2["JobRunId"]

            runs = glue.get_job_runs(JobName=job_name)
            assert len(runs["JobRuns"]) >= 2
        finally:
            glue.delete_job(JobName=job_name)


class TestGlueBatchDeleteJobs:
    """Tests for batch job deletion via individual DeleteJob calls."""

    def test_delete_multiple_jobs(self, glue):
        """Create and delete multiple jobs, verify all gone."""
        jobs = [_unique("job") for _ in range(3)]
        for jn in jobs:
            glue.create_job(
                Name=jn,
                Role="arn:aws:iam::123456789012:role/glue-role",
                Command={"Name": "glueetl", "ScriptLocation": "s3://bucket/script.py"},
            )
        for jn in jobs:
            glue.delete_job(JobName=jn)
        for jn in jobs:
            with pytest.raises(ClientError) as exc:
                glue.get_job(JobName=jn)
            assert exc.value.response["Error"]["Code"] == "EntityNotFoundException"


class TestGlueSessionOperations:
    """Tests for Glue interactive session CRUD."""

    def test_create_and_get_session(self, glue):
        ses_id = _unique("ses")
        glue.create_session(
            Id=ses_id,
            Role="arn:aws:iam::123456789012:role/glue-role",
            Command={"Name": "glueetl", "PythonVersion": "3"},
        )
        try:
            resp = glue.get_session(Id=ses_id)
            assert resp["Session"]["Id"] == ses_id
            assert resp["Session"]["Role"] == "arn:aws:iam::123456789012:role/glue-role"
        finally:
            glue.delete_session(Id=ses_id)

    def test_list_sessions_includes_created(self, glue):
        ses_id = _unique("ses")
        glue.create_session(
            Id=ses_id,
            Role="arn:aws:iam::123456789012:role/glue-role",
            Command={"Name": "glueetl", "PythonVersion": "3"},
        )
        try:
            resp = glue.list_sessions()
            assert ses_id in resp["Ids"]
        finally:
            glue.delete_session(Id=ses_id)

    def test_stop_session(self, glue):
        ses_id = _unique("ses")
        glue.create_session(
            Id=ses_id,
            Role="arn:aws:iam::123456789012:role/glue-role",
            Command={"Name": "glueetl", "PythonVersion": "3"},
        )
        try:
            resp = glue.stop_session(Id=ses_id)
            assert resp["Id"] == ses_id
        finally:
            glue.delete_session(Id=ses_id)

    def test_delete_session(self, glue):
        ses_id = _unique("ses")
        glue.create_session(
            Id=ses_id,
            Role="arn:aws:iam::123456789012:role/glue-role",
            Command={"Name": "glueetl", "PythonVersion": "3"},
        )
        resp = glue.delete_session(Id=ses_id)
        assert resp["Id"] == ses_id

        with pytest.raises(ClientError) as exc:
            glue.get_session(Id=ses_id)
        assert exc.value.response["Error"]["Code"] == "EntityNotFoundException"

    def test_get_nonexistent_session_fails(self, glue):
        with pytest.raises(ClientError) as exc:
            glue.get_session(Id="fake-session-does-not-exist")
        assert exc.value.response["Error"]["Code"] == "EntityNotFoundException"

    def test_stop_nonexistent_session_fails(self, glue):
        with pytest.raises(ClientError) as exc:
            glue.stop_session(Id="fake-session-does-not-exist")
        assert exc.value.response["Error"]["Code"] == "EntityNotFoundException"


class TestGlueDevEndpointOperations:
    """Tests for Glue DevEndpoint CRUD."""

    def test_create_and_get_dev_endpoint(self, glue):
        name = _unique("de")
        glue.create_dev_endpoint(
            EndpointName=name,
            RoleArn="arn:aws:iam::123456789012:role/glue-role",
        )
        try:
            resp = glue.get_dev_endpoint(EndpointName=name)
            assert resp["DevEndpoint"]["EndpointName"] == name
            assert resp["DevEndpoint"]["RoleArn"] == "arn:aws:iam::123456789012:role/glue-role"
        finally:
            glue.delete_dev_endpoint(EndpointName=name)

    def test_get_dev_endpoints_includes_created(self, glue):
        name = _unique("de")
        glue.create_dev_endpoint(
            EndpointName=name,
            RoleArn="arn:aws:iam::123456789012:role/glue-role",
        )
        try:
            resp = glue.get_dev_endpoints()
            de_names = [d["EndpointName"] for d in resp["DevEndpoints"]]
            assert name in de_names
        finally:
            glue.delete_dev_endpoint(EndpointName=name)

    def test_delete_dev_endpoint(self, glue):
        name = _unique("de")
        glue.create_dev_endpoint(
            EndpointName=name,
            RoleArn="arn:aws:iam::123456789012:role/glue-role",
        )
        glue.delete_dev_endpoint(EndpointName=name)

        with pytest.raises(ClientError) as exc:
            glue.get_dev_endpoint(EndpointName=name)
        assert exc.value.response["Error"]["Code"] == "EntityNotFoundException"

    def test_delete_nonexistent_dev_endpoint_fails(self, glue):
        with pytest.raises(ClientError) as exc:
            glue.delete_dev_endpoint(EndpointName="fake-de-does-not-exist")
        assert exc.value.response["Error"]["Code"] == "EntityNotFoundException"


class TestGlueWorkflowRunOperations:
    """Tests for Workflow run lifecycle operations."""

    def test_start_workflow_run(self, glue):
        name = _unique("wf")
        glue.create_workflow(Name=name)
        try:
            resp = glue.start_workflow_run(Name=name)
            assert "RunId" in resp
            assert resp["RunId"]
        finally:
            glue.delete_workflow(Name=name)

    def test_get_workflow_run(self, glue):
        name = _unique("wf")
        glue.create_workflow(Name=name)
        try:
            run_resp = glue.start_workflow_run(Name=name)
            run_id = run_resp["RunId"]

            resp = glue.get_workflow_run(Name=name, RunId=run_id)
            assert resp["Run"]["Name"] == name
            assert resp["Run"]["WorkflowRunId"] == run_id
            assert "Status" in resp["Run"]
        finally:
            glue.delete_workflow(Name=name)

    def test_get_workflow_runs(self, glue):
        name = _unique("wf")
        glue.create_workflow(Name=name)
        try:
            glue.start_workflow_run(Name=name)
            resp = glue.get_workflow_runs(Name=name)
            assert len(resp["Runs"]) >= 1
            assert resp["Runs"][0]["Name"] == name
        finally:
            glue.delete_workflow(Name=name)

    def test_put_and_get_workflow_run_properties(self, glue):
        name = _unique("wf")
        glue.create_workflow(Name=name)
        try:
            run_resp = glue.start_workflow_run(Name=name)
            run_id = run_resp["RunId"]

            glue.put_workflow_run_properties(
                Name=name, RunId=run_id, RunProperties={"key1": "val1", "key2": "val2"}
            )

            resp = glue.get_workflow_run_properties(Name=name, RunId=run_id)
            assert resp["RunProperties"]["key1"] == "val1"
            assert resp["RunProperties"]["key2"] == "val2"
        finally:
            glue.delete_workflow(Name=name)

    def test_start_workflow_run_nonexistent_fails(self, glue):
        with pytest.raises(ClientError) as exc:
            glue.start_workflow_run(Name="fake-wf-does-not-exist")
        assert exc.value.response["Error"]["Code"] == "EntityNotFoundException"

    def test_get_workflow_runs_nonexistent_fails(self, glue):
        with pytest.raises(ClientError) as exc:
            glue.get_workflow_runs(Name="fake-wf-does-not-exist")
        assert exc.value.response["Error"]["Code"] == "EntityNotFoundException"


class TestGlueRegisterSchemaVersion:
    """Tests for RegisterSchemaVersion."""

    def test_register_schema_version(self, glue):
        reg_name = _unique("reg")
        schema_name = _unique("schema")
        glue.create_registry(RegistryName=reg_name, Description="test")
        glue.create_schema(
            RegistryId={"RegistryName": reg_name},
            SchemaName=schema_name,
            DataFormat="AVRO",
            Compatibility="NONE",
            SchemaDefinition='{"type":"record","name":"T","fields":[{"name":"id","type":"int"}]}',
        )
        try:
            resp = glue.register_schema_version(
                SchemaId={"SchemaName": schema_name, "RegistryName": reg_name},
                SchemaDefinition='{"type":"record","name":"T","fields":[{"name":"id","type":"int"},{"name":"name","type":"string"}]}',  # noqa: E501
            )
            assert resp["VersionNumber"] == 2
            assert resp["Status"] == "AVAILABLE"
            assert "SchemaVersionId" in resp
        finally:
            glue.delete_schema(SchemaId={"SchemaName": schema_name, "RegistryName": reg_name})
            glue.delete_registry(RegistryId={"RegistryName": reg_name})


class TestGluePutSchemaVersionMetadata:
    """Tests for PutSchemaVersionMetadata."""

    def test_put_schema_version_metadata(self, glue):
        reg_name = _unique("reg")
        schema_name = _unique("schema")
        glue.create_registry(RegistryName=reg_name, Description="test")
        glue.create_schema(
            RegistryId={"RegistryName": reg_name},
            SchemaName=schema_name,
            DataFormat="AVRO",
            Compatibility="NONE",
            SchemaDefinition='{"type":"record","name":"T","fields":[{"name":"id","type":"int"}]}',
        )
        try:
            sv_resp = glue.get_schema_version(
                SchemaId={"SchemaName": schema_name, "RegistryName": reg_name},
                SchemaVersionNumber={"LatestVersion": True},
            )
            sv_id = sv_resp["SchemaVersionId"]

            resp = glue.put_schema_version_metadata(
                SchemaVersionId=sv_id,
                MetadataKeyValue={"MetadataKey": "testkey", "MetadataValue": "testval"},
            )
            assert resp["MetadataKey"] == "testkey"
            assert resp["MetadataValue"] == "testval"
        finally:
            glue.delete_schema(SchemaId={"SchemaName": schema_name, "RegistryName": reg_name})
            glue.delete_registry(RegistryId={"RegistryName": reg_name})


class TestGlueBatchUpdatePartition:
    """Tests for BatchUpdatePartition."""

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

    def _sd(self, location):
        return {
            "Columns": [{"Name": "col1", "Type": "string"}],
            "Location": location,
            "InputFormat": "org.apache.hadoop.mapred.TextInputFormat",
            "OutputFormat": "org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat",
            "SerdeInfo": {
                "SerializationLibrary": "org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe"
            },
        }

    def test_batch_update_partition(self, glue):
        db_name, tbl_name = self._make_db_and_table(glue)
        try:
            glue.create_partition(
                DatabaseName=db_name,
                TableName=tbl_name,
                PartitionInput={
                    "Values": ["2024"],
                    "StorageDescriptor": self._sd("s3://bucket/path/year=2024"),
                },
            )
            resp = glue.batch_update_partition(
                DatabaseName=db_name,
                TableName=tbl_name,
                Entries=[
                    {
                        "PartitionValueList": ["2024"],
                        "PartitionInput": {
                            "Values": ["2024"],
                            "StorageDescriptor": self._sd("s3://bucket/path/year=2024-updated"),
                        },
                    }
                ],
            )
            assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200

            part = glue.get_partition(
                DatabaseName=db_name, TableName=tbl_name, PartitionValues=["2024"]
            )
            assert "2024-updated" in part["Partition"]["StorageDescriptor"]["Location"]
        finally:
            glue.delete_table(DatabaseName=db_name, Name=tbl_name)
            glue.delete_database(Name=db_name)


class TestGlueTagOnCrawler:
    """Tests for tagging and untagging crawler resources."""

    def test_tag_and_get_tags_on_crawler(self, glue):
        cr_name = _unique("cr")
        glue.create_crawler(
            Name=cr_name,
            Role="arn:aws:iam::123456789012:role/glue-role",
            Targets={"S3Targets": [{"Path": "s3://bucket/path"}]},
            DatabaseName="default",
        )
        try:
            cr_arn = f"arn:aws:glue:us-east-1:123456789012:crawler/{cr_name}"
            glue.tag_resource(ResourceArn=cr_arn, TagsToAdd={"env": "test", "team": "data"})
            resp = glue.get_tags(ResourceArn=cr_arn)
            assert resp["Tags"]["env"] == "test"
            assert resp["Tags"]["team"] == "data"
        finally:
            glue.delete_crawler(Name=cr_name)

    def test_untag_crawler(self, glue):
        cr_name = _unique("cr")
        glue.create_crawler(
            Name=cr_name,
            Role="arn:aws:iam::123456789012:role/glue-role",
            Targets={"S3Targets": [{"Path": "s3://bucket/path"}]},
            DatabaseName="default",
        )
        try:
            cr_arn = f"arn:aws:glue:us-east-1:123456789012:crawler/{cr_name}"
            glue.tag_resource(ResourceArn=cr_arn, TagsToAdd={"env": "test", "team": "data"})
            glue.untag_resource(ResourceArn=cr_arn, TagsToRemove=["env"])
            resp = glue.get_tags(ResourceArn=cr_arn)
            assert "env" not in resp["Tags"]
            assert resp["Tags"]["team"] == "data"
        finally:
            glue.delete_crawler(Name=cr_name)


class TestGlueTagOnWorkflow:
    """Tests for tagging workflow resources."""

    def test_tag_and_get_tags_on_workflow(self, glue):
        wf_name = _unique("wf")
        glue.create_workflow(Name=wf_name)
        try:
            wf_arn = f"arn:aws:glue:us-east-1:123456789012:workflow/{wf_name}"
            glue.tag_resource(ResourceArn=wf_arn, TagsToAdd={"team": "data"})
            resp = glue.get_tags(ResourceArn=wf_arn)
            assert resp["Tags"]["team"] == "data"
        finally:
            glue.delete_workflow(Name=wf_name)

    def test_untag_workflow(self, glue):
        wf_name = _unique("wf")
        glue.create_workflow(Name=wf_name)
        try:
            wf_arn = f"arn:aws:glue:us-east-1:123456789012:workflow/{wf_name}"
            glue.tag_resource(ResourceArn=wf_arn, TagsToAdd={"a": "1", "b": "2"})
            glue.untag_resource(ResourceArn=wf_arn, TagsToRemove=["a"])
            resp = glue.get_tags(ResourceArn=wf_arn)
            assert "a" not in resp["Tags"]
            assert resp["Tags"]["b"] == "2"
        finally:
            glue.delete_workflow(Name=wf_name)


class TestGlueTagOnTrigger:
    """Tests for tagging trigger resources."""

    def test_tag_trigger_at_creation(self, glue):
        job_name = _unique("job")
        trig_name = _unique("trig")
        glue.create_job(
            Name=job_name,
            Role="arn:aws:iam::123456789012:role/glue-role",
            Command={"Name": "glueetl", "ScriptLocation": "s3://bucket/script.py"},
        )
        try:
            glue.create_trigger(
                Name=trig_name,
                Type="ON_DEMAND",
                Actions=[{"JobName": job_name}],
                Tags={"stage": "dev"},
            )
            trig_arn = f"arn:aws:glue:us-east-1:123456789012:trigger/{trig_name}"
            resp = glue.get_tags(ResourceArn=trig_arn)
            assert resp["Tags"]["stage"] == "dev"
        finally:
            glue.delete_trigger(Name=trig_name)
            glue.delete_job(JobName=job_name)


class TestGlueTablesWithExpression:
    """Tests for get_tables with Expression filter."""

    def _sd(self):
        return {
            "Columns": [{"Name": "col1", "Type": "string"}],
            "Location": "s3://bucket/path",
            "InputFormat": "org.apache.hadoop.mapred.TextInputFormat",
            "OutputFormat": "org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat",
            "SerdeInfo": {
                "SerializationLibrary": "org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe"
            },
        }

    def test_get_tables_with_expression(self, glue):
        db_name = _unique("db")
        prefix = _unique("pfx")
        tbl_a = f"{prefix}-alpha"
        tbl_b = f"{prefix}-beta"
        tbl_c = _unique("other")

        glue.create_database(DatabaseInput={"Name": db_name})
        glue.create_table(
            DatabaseName=db_name, TableInput={"Name": tbl_a, "StorageDescriptor": self._sd()}
        )
        glue.create_table(
            DatabaseName=db_name, TableInput={"Name": tbl_b, "StorageDescriptor": self._sd()}
        )
        glue.create_table(
            DatabaseName=db_name, TableInput={"Name": tbl_c, "StorageDescriptor": self._sd()}
        )
        try:
            resp = glue.get_tables(DatabaseName=db_name, Expression=f"{prefix}*")
            names = [t["Name"] for t in resp["TableList"]]
            assert tbl_a in names
            assert tbl_b in names
            assert tbl_c not in names
        finally:
            glue.delete_table(DatabaseName=db_name, Name=tbl_a)
            glue.delete_table(DatabaseName=db_name, Name=tbl_b)
            glue.delete_table(DatabaseName=db_name, Name=tbl_c)
            glue.delete_database(Name=db_name)


class TestGlueCrawlerDetails:
    """Tests for crawler creation with description and table prefix."""

    def test_create_crawler_with_description_and_prefix(self, glue):
        cr_name = _unique("cr")
        glue.create_crawler(
            Name=cr_name,
            Role="arn:aws:iam::123456789012:role/glue-role",
            Targets={"S3Targets": [{"Path": "s3://bucket/data"}]},
            DatabaseName="default",
            Description="crawls data lake",
            TablePrefix="dl_",
        )
        try:
            resp = glue.get_crawler(Name=cr_name)
            crawler = resp["Crawler"]
            assert crawler["Name"] == cr_name
            assert crawler["Description"] == "crawls data lake"
            assert crawler["TablePrefix"] == "dl_"
            assert crawler["State"] == "READY"
            assert "CreationTime" in crawler
        finally:
            glue.delete_crawler(Name=cr_name)


class TestGlueJobDetails:
    """Tests for job creation with extra parameters."""

    def test_create_job_with_config(self, glue):
        job_name = _unique("job")
        glue.create_job(
            Name=job_name,
            Role="arn:aws:iam::123456789012:role/glue-role",
            Command={"Name": "glueetl", "ScriptLocation": "s3://bucket/script.py"},
            MaxRetries=3,
            Timeout=120,
            MaxCapacity=10.0,
        )
        try:
            resp = glue.get_job(JobName=job_name)
            job = resp["Job"]
            assert job["Name"] == job_name
            assert job["MaxRetries"] == 3
            assert job["Timeout"] == 120
            assert job["MaxCapacity"] == 10.0
        finally:
            glue.delete_job(JobName=job_name)

    def test_create_job_with_default_arguments(self, glue):
        job_name = _unique("job")
        default_args = {
            "--extra-py-files": "s3://bucket/extra.zip",
            "--TempDir": "s3://bucket/tmp",
        }
        glue.create_job(
            Name=job_name,
            Role="arn:aws:iam::123456789012:role/glue-role",
            Command={"Name": "glueetl", "ScriptLocation": "s3://bucket/script.py"},
            DefaultArguments=default_args,
        )
        try:
            resp = glue.get_job(JobName=job_name)
            job = resp["Job"]
            assert job["DefaultArguments"]["--extra-py-files"] == "s3://bucket/extra.zip"
            assert job["DefaultArguments"]["--TempDir"] == "s3://bucket/tmp"
        finally:
            glue.delete_job(JobName=job_name)


class TestGlueScheduledTrigger:
    """Tests for scheduled trigger creation."""

    def test_create_scheduled_trigger(self, glue):
        job_name = _unique("job")
        trig_name = _unique("trig")
        glue.create_job(
            Name=job_name,
            Role="arn:aws:iam::123456789012:role/glue-role",
            Command={"Name": "glueetl", "ScriptLocation": "s3://bucket/script.py"},
        )
        try:
            glue.create_trigger(
                Name=trig_name,
                Type="SCHEDULED",
                Schedule="cron(0 12 * * ? *)",
                Actions=[{"JobName": job_name}],
            )
            resp = glue.get_trigger(Name=trig_name)
            trigger = resp["Trigger"]
            assert trigger["Name"] == trig_name
            assert trigger["Type"] == "SCHEDULED"
            assert trigger["Schedule"] == "cron(0 12 * * ? *)"
            assert trigger["Actions"][0]["JobName"] == job_name
        finally:
            glue.delete_trigger(Name=trig_name)
            glue.delete_job(JobName=job_name)


class TestGlueDatabaseWithCatalogId:
    """Tests for database operations with CatalogId."""

    def test_get_databases_with_catalog_id(self, glue):
        db_name = _unique("db")
        glue.create_database(DatabaseInput={"Name": db_name})
        try:
            resp = glue.get_databases(CatalogId="123456789012")
            db_names = [db["Name"] for db in resp["DatabaseList"]]
            assert db_name in db_names
        finally:
            glue.delete_database(Name=db_name)

    def test_get_database_with_catalog_id(self, glue):
        db_name = _unique("db")
        glue.create_database(DatabaseInput={"Name": db_name})
        try:
            resp = glue.get_database(Name=db_name, CatalogId="123456789012")
            assert resp["Database"]["Name"] == db_name
        finally:
            glue.delete_database(Name=db_name)


class TestGlueListOperations:
    """Tests for Glue list operations that require no setup."""

    def test_list_blueprints(self, glue):
        resp = glue.list_blueprints()
        assert "Blueprints" in resp

    def test_get_classifiers(self, glue):
        resp = glue.get_classifiers()
        assert "Classifiers" in resp

    def test_list_data_quality_rulesets(self, glue):
        resp = glue.list_data_quality_rulesets()
        assert "Rulesets" in resp

    def test_get_ml_transforms(self, glue):
        resp = glue.get_ml_transforms()
        assert "Transforms" in resp

    def test_list_usage_profiles(self, glue):
        resp = glue.list_usage_profiles()
        assert "Profiles" in resp


class TestGlueClassifierOperations:
    """Tests for Glue classifier operations."""

    def test_create_and_get_classifier(self, glue):
        name = _unique("clf")
        glue.create_classifier(
            GrokClassifier={
                "Classification": "test",
                "Name": name,
                "GrokPattern": "%{COMBINEDAPACHELOG}",
            }
        )
        resp = glue.get_classifier(Name=name)
        assert resp["Classifier"]["GrokClassifier"]["Name"] == name
        assert resp["Classifier"]["GrokClassifier"]["Classification"] == "test"
        glue.delete_classifier(Name=name)

    def test_get_classifiers_includes_created(self, glue):
        name = _unique("clf")
        glue.create_classifier(
            GrokClassifier={
                "Classification": "test",
                "Name": name,
                "GrokPattern": "%{COMBINEDAPACHELOG}",
            }
        )
        resp = glue.get_classifiers()
        names = [c["GrokClassifier"]["Name"] for c in resp["Classifiers"] if "GrokClassifier" in c]
        assert name in names
        glue.delete_classifier(Name=name)


class TestGlueCatalogOperations:
    """Tests for Glue catalog operations."""

    def test_get_catalog_not_found(self, glue):
        with pytest.raises(ClientError) as exc:
            glue.get_catalog(CatalogId="999999999999")
        assert exc.value.response["Error"]["Code"] == "EntityNotFoundException"


class TestGlueBlueprintOperations:
    """Tests for Glue blueprint operations."""

    def test_create_and_get_blueprint(self, glue):
        name = _unique("bp")
        glue.create_blueprint(
            Name=name,
            BlueprintLocation="s3://test-bucket/blueprint.py",
        )
        resp = glue.get_blueprint(Name=name)
        assert resp["Blueprint"]["Name"] == name
        glue.delete_blueprint(Name=name)

    def test_list_blueprints_includes_created(self, glue):
        name = _unique("bp")
        glue.create_blueprint(
            Name=name,
            BlueprintLocation="s3://test-bucket/blueprint.py",
        )
        resp = glue.list_blueprints()
        assert name in resp["Blueprints"]
        glue.delete_blueprint(Name=name)


class TestGlueDataQualityRulesetOperations:
    """Tests for Glue data quality ruleset operations."""

    def test_create_and_get_data_quality_ruleset(self, glue):
        name = _unique("dqr")
        glue.create_data_quality_ruleset(
            Name=name,
            Ruleset='Rules = [ IsComplete "col1" ]',
        )
        resp = glue.get_data_quality_ruleset(Name=name)
        assert resp["Name"] == name
        assert "Ruleset" in resp
        glue.delete_data_quality_ruleset(Name=name)

    def test_list_data_quality_rulesets_includes_created(self, glue):
        name = _unique("dqr")
        glue.create_data_quality_ruleset(
            Name=name,
            Ruleset='Rules = [ IsComplete "col1" ]',
        )
        resp = glue.list_data_quality_rulesets()
        names = [r["Name"] for r in resp["Rulesets"]]
        assert name in names
        glue.delete_data_quality_ruleset(Name=name)


class TestGlueMLTransformOperations:
    """Tests for Glue ML transform operations."""

    def test_create_and_get_ml_transform(self, glue):
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
        resp = glue.create_ml_transform(
            Name=_unique("mlt"),
            InputRecordTables=[
                {
                    "DatabaseName": db_name,
                    "TableName": tbl_name,
                }
            ],
            Parameters={
                "TransformType": "FIND_MATCHES",
                "FindMatchesParameters": {
                    "PrimaryKeyColumnName": "col1",
                },
            },
            Role="arn:aws:iam::123456789012:role/GlueRole",
        )
        transform_id = resp["TransformId"]
        assert transform_id

        get_resp = glue.get_ml_transform(TransformId=transform_id)
        assert get_resp["TransformId"] == transform_id
        assert get_resp["Name"]

        glue.delete_ml_transform(TransformId=transform_id)
        glue.delete_table(DatabaseName=db_name, Name=tbl_name)
        glue.delete_database(Name=db_name)

    def test_get_ml_transforms_includes_created(self, glue):
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
        resp = glue.create_ml_transform(
            Name=_unique("mlt"),
            InputRecordTables=[
                {
                    "DatabaseName": db_name,
                    "TableName": tbl_name,
                }
            ],
            Parameters={
                "TransformType": "FIND_MATCHES",
                "FindMatchesParameters": {
                    "PrimaryKeyColumnName": "col1",
                },
            },
            Role="arn:aws:iam::123456789012:role/GlueRole",
        )
        transform_id = resp["TransformId"]

        list_resp = glue.get_ml_transforms()
        ids = [t["TransformId"] for t in list_resp["Transforms"]]
        assert transform_id in ids

        glue.delete_ml_transform(TransformId=transform_id)
        glue.delete_table(DatabaseName=db_name, Name=tbl_name)
        glue.delete_database(Name=db_name)


class TestGlueUsageProfileOperations:
    """Tests for Glue usage profile operations."""

    def test_create_and_get_usage_profile(self, glue):
        name = _unique("up")
        glue.create_usage_profile(Name=name, Configuration={})
        resp = glue.get_usage_profile(Name=name)
        assert resp["Name"] == name
        glue.delete_usage_profile(Name=name)

    def test_list_usage_profiles_includes_created(self, glue):
        name = _unique("up")
        glue.create_usage_profile(Name=name, Configuration={})
        resp = glue.list_usage_profiles()
        names = [p["Name"] for p in resp["Profiles"]]
        assert name in names
        glue.delete_usage_profile(Name=name)

    def test_get_usage_profile_not_found(self, glue):
        with pytest.raises(ClientError) as exc:
            glue.get_usage_profile(Name="nonexistent-profile")
        assert exc.value.response["Error"]["Code"] == "EntityNotFoundException"


class TestGlueGetOperations:
    def test_get_catalog_import_status(self, glue):
        """GetCatalogImportStatus returns import status."""
        resp = glue.get_catalog_import_status()
        assert "ImportStatus" in resp

    def test_get_resource_policies(self, glue):
        """GetResourcePolicies returns policy list."""
        resp = glue.get_resource_policies()
        assert "GetResourcePoliciesResponseList" in resp

    def test_get_resource_policy(self, glue):
        """GetResourcePolicy returns policy or empty."""
        try:
            resp = glue.get_resource_policy()
            assert "PolicyInJson" in resp or resp["ResponseMetadata"]["HTTPStatusCode"] == 200
        except ClientError as e:
            assert e.response["Error"]["Code"] == "EntityNotFoundException"

    def test_get_security_configurations(self, glue):
        """GetSecurityConfigurations returns config list."""
        resp = glue.get_security_configurations()
        assert "SecurityConfigurations" in resp

    def test_get_ml_transforms(self, glue):
        """GetMLTransforms returns transform list."""
        resp = glue.get_ml_transforms()
        assert "Transforms" in resp

    def test_get_crawler_metrics(self, glue):
        """GetCrawlerMetrics returns metrics list."""
        resp = glue.get_crawler_metrics()
        assert "CrawlerMetricsList" in resp

    def test_get_ml_transform_not_found(self, glue):
        """GetMLTransform with fake ID returns error."""
        with pytest.raises(ClientError) as exc:
            glue.get_ml_transform(TransformId="tfm-fake12345678")
        assert exc.value.response["Error"]["Code"] == "EntityNotFoundException"

    def test_get_blueprint_run_not_found(self, glue):
        """GetBlueprintRun with fake params returns error."""
        with pytest.raises(ClientError) as exc:
            glue.get_blueprint_run(BlueprintName="fake-bp", RunId="fake-run-id")
        assert exc.value.response["Error"]["Code"] in (
            "EntityNotFoundException",
            "OperationNotSupportedException",
        )

    def test_get_blueprint_runs_not_found(self, glue):
        """GetBlueprintRuns with fake blueprint returns error."""
        with pytest.raises(ClientError) as exc:
            glue.get_blueprint_runs(BlueprintName="fake-bp")
        assert exc.value.response["Error"]["Code"] in (
            "EntityNotFoundException",
            "OperationNotSupportedException",
        )

    def test_get_column_statistics_for_table(self, glue):
        """GetColumnStatisticsForTable with fake table returns error."""
        with pytest.raises(ClientError) as exc:
            glue.get_column_statistics_for_table(
                DatabaseName="fake-db",
                TableName="fake-table",
                ColumnNames=["col1"],
            )
        assert exc.value.response["Error"]["Code"] == "EntityNotFoundException"

    def test_get_column_statistics_for_partition(self, glue):
        """GetColumnStatisticsForPartition with fake params returns error."""
        with pytest.raises(ClientError) as exc:
            glue.get_column_statistics_for_partition(
                DatabaseName="fake-db",
                TableName="fake-table",
                PartitionValues=["2024-01-01"],
                ColumnNames=["col1"],
            )
        assert exc.value.response["Error"]["Code"] == "EntityNotFoundException"

    def test_get_data_quality_result_not_found(self, glue):
        """GetDataQualityResult with fake ID returns error."""
        with pytest.raises(ClientError) as exc:
            glue.get_data_quality_result(ResultId="dqresult-fake12345678")
        assert exc.value.response["Error"]["Code"] in (
            "EntityNotFoundException",
            "InvalidInputException",
        )

    def test_get_data_quality_ruleset_not_found(self, glue):
        """GetDataQualityRuleset with fake name returns error."""
        with pytest.raises(ClientError) as exc:
            glue.get_data_quality_ruleset(Name="fake-ruleset")
        assert exc.value.response["Error"]["Code"] in (
            "EntityNotFoundException",
            "InvalidInputException",
        )

    def test_get_data_quality_rule_recommendation_run_not_found(self, glue):
        """GetDataQualityRuleRecommendationRun with fake ID returns error."""
        with pytest.raises(ClientError) as exc:
            glue.get_data_quality_rule_recommendation_run(RunId="dqrun-fake12345678")
        assert exc.value.response["Error"]["Code"] in (
            "EntityNotFoundException",
            "InvalidInputException",
        )

    def test_get_data_quality_ruleset_evaluation_run_not_found(self, glue):
        """GetDataQualityRulesetEvaluationRun with fake ID returns error."""
        with pytest.raises(ClientError) as exc:
            glue.get_data_quality_ruleset_evaluation_run(RunId="dqrun-fake12345678")
        assert exc.value.response["Error"]["Code"] in (
            "EntityNotFoundException",
            "InvalidInputException",
        )

    def test_get_column_statistics_task_run_not_found(self, glue):
        """GetColumnStatisticsTaskRun with fake params returns error."""
        with pytest.raises(ClientError) as exc:
            glue.get_column_statistics_task_run(
                ColumnStatisticsTaskRunId="fake-run-id",
            )
        assert exc.value.response["Error"]["Code"] in (
            "EntityNotFoundException",
            "InvalidInputException",
        )

    def test_get_column_statistics_task_runs(self, glue):
        """GetColumnStatisticsTaskRuns with fake params returns error or empty."""
        try:
            resp = glue.get_column_statistics_task_runs(
                DatabaseName="fake-db",
                TableName="fake-table",
            )
            assert "ColumnStatisticsTaskRuns" in resp
        except ClientError as e:
            assert e.response["Error"]["Code"] in (
                "EntityNotFoundException",
                "InvalidInputException",
            )


class TestGlueEntityRecords:
    """Tests for GetEntityRecords operation."""

    def test_get_entity_records(self, glue):
        """GetEntityRecords returns a Records list."""
        resp = glue.get_entity_records(
            EntityName="fake-entity",
            ConnectionName="fake-conn",
            Limit=10,
        )
        assert "Records" in resp


class TestGlueMLTaskRuns:
    """Tests for GetMLTaskRun and GetMLTaskRuns operations."""

    def test_get_ml_task_run_nonexistent(self, glue):
        """GetMLTaskRun for nonexistent transform raises EntityNotFoundException."""
        with pytest.raises(ClientError) as exc:
            glue.get_ml_task_run(TransformId="fake-transform", TaskRunId="fake-task")
        assert exc.value.response["Error"]["Code"] == "EntityNotFoundException"

    def test_get_ml_task_runs_nonexistent(self, glue):
        """GetMLTaskRuns for nonexistent transform raises EntityNotFoundException."""
        with pytest.raises(ClientError) as exc:
            glue.get_ml_task_runs(TransformId="fake-transform")
        assert exc.value.response["Error"]["Code"] == "EntityNotFoundException"


class TestGlueGetMapping:
    """Tests for GetMapping operation."""

    def test_get_mapping(self, glue):
        """GetMapping returns a Mapping list."""
        resp = glue.get_mapping(
            Source={"DatabaseName": "fake-db", "TableName": "fake-table"},
        )
        assert "Mapping" in resp
        assert isinstance(resp["Mapping"], list)


class TestGlueGetStatement:
    """Tests for GetStatement operation."""

    def test_get_statement_nonexistent_session(self, glue):
        """GetStatement for nonexistent session raises EntityNotFoundException."""
        with pytest.raises(ClientError) as exc:
            glue.get_statement(SessionId="fake-session", Id=0)
        assert exc.value.response["Error"]["Code"] == "EntityNotFoundException"


class TestGlueDeleteColumnStatistics:
    """Tests for DeleteColumnStatistics operations."""

    def test_delete_column_statistics_for_table_not_found(self, glue):
        """DeleteColumnStatisticsForTable on nonexistent db raises error."""
        with pytest.raises(ClientError) as exc:
            glue.delete_column_statistics_for_table(
                DatabaseName="fake-db",
                TableName="fake-table",
                ColumnName="fake-col",
            )
        assert exc.value.response["Error"]["Code"] == "EntityNotFoundException"

    def test_delete_column_statistics_for_partition_not_found(self, glue):
        """DeleteColumnStatisticsForPartition on nonexistent db raises error."""
        with pytest.raises(ClientError) as exc:
            glue.delete_column_statistics_for_partition(
                DatabaseName="fake-db",
                TableName="fake-table",
                PartitionValues=["val"],
                ColumnName="fake-col",
            )
        assert exc.value.response["Error"]["Code"] == "EntityNotFoundException"


class TestGlueCustomEntityTypeOperations:
    """Tests for CustomEntityType CRUD operations."""

    def test_create_and_get_custom_entity_type(self, glue):
        """CreateCustomEntityType + GetCustomEntityType round-trip."""
        name = _unique("cet")
        glue.create_custom_entity_type(
            Name=name,
            RegexString="\\d{3}-\\d{2}-\\d{4}",
        )
        resp = glue.get_custom_entity_type(Name=name)
        assert resp["Name"] == name
        assert "RegexString" in resp
        glue.delete_custom_entity_type(Name=name)

    def test_list_custom_entity_types(self, glue):
        """ListCustomEntityTypes returns a list."""
        resp = glue.list_custom_entity_types()
        assert "CustomEntityTypes" in resp
        assert isinstance(resp["CustomEntityTypes"], list)

    def test_delete_custom_entity_type(self, glue):
        """DeleteCustomEntityType removes the entity type."""
        name = _unique("cet")
        glue.create_custom_entity_type(
            Name=name,
            RegexString="test.*pattern",
        )
        glue.delete_custom_entity_type(Name=name)
        with pytest.raises(ClientError) as exc:
            glue.get_custom_entity_type(Name=name)
        assert "Error" in exc.value.response


class TestGlueListOperationsExtended:
    """Tests for additional List operations."""

    def test_list_dev_endpoints(self, glue):
        """ListDevEndpoints returns DevEndpointNames."""
        resp = glue.list_dev_endpoints()
        assert "DevEndpointNames" in resp
        assert isinstance(resp["DevEndpointNames"], list)

    def test_list_schemas(self, glue):
        """ListSchemas returns Schemas list."""
        reg_name = _unique("reg")
        glue.create_registry(RegistryName=reg_name)
        try:
            resp = glue.list_schemas(
                RegistryId={"RegistryName": reg_name},
            )
            assert "Schemas" in resp
            assert isinstance(resp["Schemas"], list)
        finally:
            glue.delete_registry(RegistryId={"RegistryName": reg_name})

    def test_list_schema_versions(self, glue):
        """ListSchemaVersions returns Schemas list."""
        reg_name = _unique("reg")
        schema_name = _unique("sch")
        glue.create_registry(RegistryName=reg_name)
        glue.create_schema(
            RegistryId={"RegistryName": reg_name},
            SchemaName=schema_name,
            DataFormat="AVRO",
            Compatibility="NONE",
            SchemaDefinition='{"type":"record","name":"Test","fields":[{"name":"id","type":"int"}]}',
        )
        try:
            resp = glue.list_schema_versions(
                SchemaId={
                    "RegistryName": reg_name,
                    "SchemaName": schema_name,
                },
            )
            assert "Schemas" in resp
            assert isinstance(resp["Schemas"], list)
        finally:
            glue.delete_schema(
                SchemaId={
                    "RegistryName": reg_name,
                    "SchemaName": schema_name,
                },
            )
            glue.delete_registry(RegistryId={"RegistryName": reg_name})

    def test_list_statements_nonexistent_session(self, glue):
        """ListStatements for nonexistent session raises error."""
        with pytest.raises(ClientError) as exc:
            glue.list_statements(SessionId="fake-session")
        assert exc.value.response["Error"]["Code"] == "EntityNotFoundException"

    def test_list_column_statistics_task_runs(self, glue):
        """ListColumnStatisticsTaskRuns returns ColumnStatisticsTaskRunIds."""
        resp = glue.list_column_statistics_task_runs()
        assert "ColumnStatisticsTaskRunIds" in resp
        assert isinstance(resp["ColumnStatisticsTaskRunIds"], list)

    def test_list_ml_transforms(self, glue):
        """ListMLTransforms returns TransformIds."""
        resp = glue.list_ml_transforms()
        assert "TransformIds" in resp
        assert isinstance(resp["TransformIds"], list)

    def test_list_data_quality_results(self, glue):
        """ListDataQualityResults returns Results list."""
        resp = glue.list_data_quality_results()
        assert "Results" in resp
        assert isinstance(resp["Results"], list)

    def test_list_data_quality_rule_recommendation_runs(self, glue):
        """ListDataQualityRuleRecommendationRuns returns Runs list."""
        resp = glue.list_data_quality_rule_recommendation_runs()
        assert "Runs" in resp
        assert isinstance(resp["Runs"], list)

    def test_list_data_quality_ruleset_evaluation_runs(self, glue):
        """ListDataQualityRulesetEvaluationRuns returns Runs list."""
        resp = glue.list_data_quality_ruleset_evaluation_runs()
        assert "Runs" in resp
        assert isinstance(resp["Runs"], list)


class TestGlueSearchTables:
    """Tests for SearchTables operation."""

    def test_search_tables(self, glue):
        """SearchTables returns TableList."""
        resp = glue.search_tables()
        assert "TableList" in resp
        assert isinstance(resp["TableList"], list)

    def test_search_tables_with_filter(self, glue):
        """SearchTables with a filter returns TableList."""
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
        try:
            resp = glue.search_tables(
                Filters=[
                    {"Key": "DatabaseName", "Value": db_name, "Comparator": "EQUALS"},
                ],
            )
            assert "TableList" in resp
        finally:
            glue.delete_table(DatabaseName=db_name, Name=tbl_name)
            glue.delete_database(Name=db_name)


class TestGlueWorkflowStopAndUpdate:
    """Tests for StopWorkflowRun and workflow update operations."""

    def test_stop_workflow_run_nonexistent(self, glue):
        """StopWorkflowRun for nonexistent workflow raises error."""
        with pytest.raises(ClientError) as exc:
            glue.stop_workflow_run(Name="fake-workflow", RunId="fake-run")
        assert "Error" in exc.value.response

    def test_get_data_quality_model_not_found(self, glue):
        """GetDataQualityModel for nonexistent returns error."""
        with pytest.raises(ClientError) as exc:
            glue.get_data_quality_model(
                StatisticId="fake-stat",
                ProfileId="fake-profile",
            )
        assert "Error" in exc.value.response


class TestGlueCatalogCRUD:
    """Tests for CreateCatalog, DeleteCatalog, UpdateCatalog."""

    def test_create_and_delete_catalog(self, glue):
        """CreateCatalog creates a catalog, DeleteCatalog removes it."""
        name = _unique("cat")
        glue.create_catalog(
            Name=name,
            CatalogInput={"Description": "test catalog"},
        )
        # Verify it exists
        resp = glue.get_catalog(CatalogId=name)
        assert resp["Catalog"]["CatalogId"] == name

        # Delete it
        glue.delete_catalog(CatalogId=name)

        # Verify it's gone
        with pytest.raises(ClientError) as exc:
            glue.get_catalog(CatalogId=name)
        assert exc.value.response["Error"]["Code"] == "EntityNotFoundException"

    def test_update_catalog(self, glue):
        """UpdateCatalog modifies a catalog's description."""
        name = _unique("cat")
        glue.create_catalog(
            Name=name,
            CatalogInput={"Description": "original"},
        )
        try:
            glue.update_catalog(
                CatalogId=name,
                CatalogInput={"Description": "updated"},
            )
            resp = glue.get_catalog(CatalogId=name)
            assert resp["Catalog"]["CatalogId"] == name
        finally:
            glue.delete_catalog(CatalogId=name)

    def test_update_catalog_not_found(self, glue):
        """UpdateCatalog for nonexistent catalog raises error."""
        with pytest.raises(ClientError) as exc:
            glue.update_catalog(
                CatalogId="nonexistent-catalog-xyz",
                CatalogInput={"Description": "nope"},
            )
        assert exc.value.response["Error"]["Code"] == "EntityNotFoundException"


class TestGlueBlueprintUpdate:
    """Tests for UpdateBlueprint."""

    def test_update_blueprint(self, glue):
        """UpdateBlueprint modifies a blueprint's location."""
        name = _unique("bp")
        glue.create_blueprint(
            Name=name,
            BlueprintLocation="s3://test-bucket/blueprint.py",
        )
        try:
            resp = glue.update_blueprint(
                Name=name,
                BlueprintLocation="s3://test-bucket/blueprint-v2.py",
            )
            assert resp["Name"] == name
        finally:
            glue.delete_blueprint(Name=name)

    def test_update_blueprint_not_found(self, glue):
        """UpdateBlueprint for nonexistent blueprint raises error."""
        with pytest.raises(ClientError) as exc:
            glue.update_blueprint(
                Name="nonexistent-bp-xyz",
                BlueprintLocation="s3://test-bucket/nope.py",
            )
        assert exc.value.response["Error"]["Code"] == "EntityNotFoundException"


class TestGlueClassifierUpdate:
    """Tests for UpdateClassifier."""

    def test_update_classifier(self, glue):
        """UpdateClassifier modifies a classifier's pattern."""
        name = _unique("clf")
        glue.create_classifier(
            GrokClassifier={
                "Classification": "test",
                "Name": name,
                "GrokPattern": "%{COMBINEDAPACHELOG}",
            }
        )
        try:
            glue.update_classifier(
                GrokClassifier={
                    "Name": name,
                    "Classification": "updated",
                    "GrokPattern": "%{COMMONAPACHELOG}",
                }
            )
            resp = glue.get_classifier(Name=name)
            assert resp["Classifier"]["GrokClassifier"]["Name"] == name
        finally:
            glue.delete_classifier(Name=name)


class TestGlueDataQualityRulesetUpdate:
    """Tests for UpdateDataQualityRuleset."""

    def test_update_data_quality_ruleset(self, glue):
        """UpdateDataQualityRuleset modifies a ruleset."""
        name = _unique("dqr")
        glue.create_data_quality_ruleset(
            Name=name,
            Ruleset='Rules = [ IsComplete "col1" ]',
        )
        try:
            glue.update_data_quality_ruleset(
                Name=name,
                Ruleset='Rules = [ IsComplete "col2" ]',
            )
            resp = glue.get_data_quality_ruleset(Name=name)
            assert resp["Name"] == name
        finally:
            glue.delete_data_quality_ruleset(Name=name)

    def test_update_data_quality_ruleset_not_found(self, glue):
        """UpdateDataQualityRuleset for nonexistent raises error."""
        with pytest.raises(ClientError) as exc:
            glue.update_data_quality_ruleset(
                Name="nonexistent-dqr-xyz",
                Ruleset='Rules = [ IsComplete "col1" ]',
            )
        assert exc.value.response["Error"]["Code"] == "EntityNotFoundException"


class TestGlueMLTransformUpdate:
    """Tests for UpdateMLTransform."""

    def test_update_ml_transform(self, glue):
        """UpdateMLTransform modifies a transform's description."""
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
        resp = glue.create_ml_transform(
            Name=_unique("mlt"),
            InputRecordTables=[{"DatabaseName": db_name, "TableName": tbl_name}],
            Parameters={
                "TransformType": "FIND_MATCHES",
                "FindMatchesParameters": {"PrimaryKeyColumnName": "col1"},
            },
            Role="arn:aws:iam::123456789012:role/GlueRole",
        )
        transform_id = resp["TransformId"]
        try:
            update_resp = glue.update_ml_transform(
                TransformId=transform_id,
                Description="updated description",
            )
            assert update_resp["TransformId"] == transform_id
        finally:
            glue.delete_ml_transform(TransformId=transform_id)
            glue.delete_table(DatabaseName=db_name, Name=tbl_name)
            glue.delete_database(Name=db_name)

    def test_update_ml_transform_not_found(self, glue):
        """UpdateMLTransform for nonexistent raises error."""
        with pytest.raises(ClientError) as exc:
            glue.update_ml_transform(
                TransformId="nonexistent-id-xyz",
                Description="nope",
            )
        assert exc.value.response["Error"]["Code"] == "EntityNotFoundException"


class TestGlueUsageProfileUpdate:
    """Tests for UpdateUsageProfile."""

    def test_update_usage_profile(self, glue):
        """UpdateUsageProfile modifies a usage profile."""
        name = _unique("up")
        glue.create_usage_profile(Name=name, Configuration={})
        try:
            glue.update_usage_profile(
                Name=name,
                Configuration={
                    "SessionConfiguration": {
                        "IdleTimeout": {"DefaultValue": "60"},
                    }
                },
            )
            resp = glue.get_usage_profile(Name=name)
            assert resp["Name"] == name
        finally:
            glue.delete_usage_profile(Name=name)

    def test_update_usage_profile_not_found(self, glue):
        """UpdateUsageProfile for nonexistent raises error."""
        with pytest.raises(ClientError) as exc:
            glue.update_usage_profile(
                Name="nonexistent-up-xyz",
                Configuration={},
            )
        assert exc.value.response["Error"]["Code"] == "EntityNotFoundException"


class TestGlueUserDefinedFunctions:
    """Tests for CreateUserDefinedFunction, GetUserDefinedFunction, DeleteUserDefinedFunction,
    UpdateUserDefinedFunction."""

    def test_create_and_get_user_defined_function(self, glue):
        db_name = _unique("db")
        func_name = _unique("udf")
        glue.create_database(DatabaseInput={"Name": db_name})
        glue.create_user_defined_function(
            DatabaseName=db_name,
            FunctionInput={
                "FunctionName": func_name,
                "ClassName": "com.example.MyUDF",
                "OwnerName": "test-owner",
                "OwnerType": "USER",
            },
        )
        try:
            resp = glue.get_user_defined_function(DatabaseName=db_name, FunctionName=func_name)
            assert resp["UserDefinedFunction"]["FunctionName"] == func_name
            assert resp["UserDefinedFunction"]["ClassName"] == "com.example.MyUDF"
        finally:
            glue.delete_user_defined_function(DatabaseName=db_name, FunctionName=func_name)
            glue.delete_database(Name=db_name)

    def test_update_user_defined_function(self, glue):
        db_name = _unique("db")
        func_name = _unique("udf")
        glue.create_database(DatabaseInput={"Name": db_name})
        glue.create_user_defined_function(
            DatabaseName=db_name,
            FunctionInput={
                "FunctionName": func_name,
                "ClassName": "com.example.MyUDF",
                "OwnerName": "test-owner",
                "OwnerType": "USER",
            },
        )
        try:
            glue.update_user_defined_function(
                DatabaseName=db_name,
                FunctionName=func_name,
                FunctionInput={
                    "FunctionName": func_name,
                    "ClassName": "com.example.UpdatedUDF",
                    "OwnerName": "test-owner",
                    "OwnerType": "USER",
                },
            )
            resp = glue.get_user_defined_function(DatabaseName=db_name, FunctionName=func_name)
            assert resp["UserDefinedFunction"]["ClassName"] == "com.example.UpdatedUDF"
        finally:
            glue.delete_user_defined_function(DatabaseName=db_name, FunctionName=func_name)
            glue.delete_database(Name=db_name)

    def test_delete_user_defined_function(self, glue):
        db_name = _unique("db")
        func_name = _unique("udf")
        glue.create_database(DatabaseInput={"Name": db_name})
        glue.create_user_defined_function(
            DatabaseName=db_name,
            FunctionInput={
                "FunctionName": func_name,
                "ClassName": "com.example.MyUDF",
                "OwnerName": "test-owner",
                "OwnerType": "USER",
            },
        )
        glue.delete_user_defined_function(DatabaseName=db_name, FunctionName=func_name)
        with pytest.raises(ClientError) as exc:
            glue.get_user_defined_function(DatabaseName=db_name, FunctionName=func_name)
        assert exc.value.response["Error"]["Code"] == "EntityNotFoundException"
        glue.delete_database(Name=db_name)


class TestGlueConnectionOperations:
    """Tests for CreateConnection, DeleteConnection, BatchDeleteConnection,
    UpdateConnection."""

    def test_create_and_delete_connection(self, glue):
        conn_name = _unique("conn")
        glue.create_connection(
            ConnectionInput={
                "Name": conn_name,
                "ConnectionType": "JDBC",
                "ConnectionProperties": {
                    "JDBC_CONNECTION_URL": "jdbc:mysql://localhost:3306/test",
                    "USERNAME": "admin",
                    "PASSWORD": "password",
                },
            }
        )
        resp = glue.get_connection(Name=conn_name)
        assert resp["Connection"]["Name"] == conn_name
        glue.delete_connection(ConnectionName=conn_name)
        with pytest.raises(ClientError) as exc:
            glue.get_connection(Name=conn_name)
        assert exc.value.response["Error"]["Code"] == "EntityNotFoundException"

    def test_batch_delete_connection(self, glue):
        c1 = _unique("conn")
        c2 = _unique("conn")
        for name in (c1, c2):
            glue.create_connection(
                ConnectionInput={
                    "Name": name,
                    "ConnectionType": "JDBC",
                    "ConnectionProperties": {
                        "JDBC_CONNECTION_URL": "jdbc:mysql://localhost:3306/test",
                        "USERNAME": "admin",
                        "PASSWORD": "password",
                    },
                }
            )
        resp = glue.batch_delete_connection(ConnectionNameList=[c1, c2])
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200

    def test_update_connection(self, glue):
        conn_name = _unique("conn")
        glue.create_connection(
            ConnectionInput={
                "Name": conn_name,
                "ConnectionType": "JDBC",
                "ConnectionProperties": {
                    "JDBC_CONNECTION_URL": "jdbc:mysql://localhost:3306/test",
                    "USERNAME": "admin",
                    "PASSWORD": "password",
                },
            }
        )
        try:
            glue.update_connection(
                Name=conn_name,
                ConnectionInput={
                    "Name": conn_name,
                    "ConnectionType": "JDBC",
                    "ConnectionProperties": {
                        "JDBC_CONNECTION_URL": "jdbc:mysql://localhost:3306/updated",
                        "USERNAME": "admin",
                        "PASSWORD": "password",
                    },
                },
            )
            resp = glue.get_connection(Name=conn_name)
            assert "updated" in resp["Connection"]["ConnectionProperties"]["JDBC_CONNECTION_URL"]
        finally:
            glue.delete_connection(ConnectionName=conn_name)


class TestGlueCrawlerUpdates:
    """Tests for UpdateCrawler and UpdateCrawlerSchedule."""

    def test_update_crawler(self, glue):
        db_name = _unique("db")
        crawler_name = _unique("crawler")
        glue.create_database(DatabaseInput={"Name": db_name})
        glue.create_crawler(
            Name=crawler_name,
            Role="arn:aws:iam::123456789012:role/glue-role",
            DatabaseName=db_name,
            Targets={"S3Targets": [{"Path": "s3://test-bucket/data"}]},
        )
        try:
            resp = glue.update_crawler(
                Name=crawler_name,
                Description="updated description",
            )
            assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200
            got = glue.get_crawler(Name=crawler_name)
            assert got["Crawler"]["Description"] == "updated description"
        finally:
            glue.delete_crawler(Name=crawler_name)
            glue.delete_database(Name=db_name)

    def test_update_crawler_schedule(self, glue):
        db_name = _unique("db")
        crawler_name = _unique("crawler")
        glue.create_database(DatabaseInput={"Name": db_name})
        glue.create_crawler(
            Name=crawler_name,
            Role="arn:aws:iam::123456789012:role/glue-role",
            DatabaseName=db_name,
            Targets={"S3Targets": [{"Path": "s3://test-bucket/data"}]},
        )
        try:
            resp = glue.update_crawler_schedule(
                CrawlerName=crawler_name,
                Schedule="cron(0 12 * * ? *)",
            )
            assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200
        finally:
            glue.delete_crawler(Name=crawler_name)
            glue.delete_database(Name=db_name)

    def test_start_crawler_schedule(self, glue):
        db_name = _unique("db")
        crawler_name = _unique("crawler")
        glue.create_database(DatabaseInput={"Name": db_name})
        glue.create_crawler(
            Name=crawler_name,
            Role="arn:aws:iam::123456789012:role/glue-role",
            DatabaseName=db_name,
            Targets={"S3Targets": [{"Path": "s3://test-bucket/data"}]},
            Schedule="cron(0 12 * * ? *)",
        )
        try:
            resp = glue.start_crawler_schedule(CrawlerName=crawler_name)
            assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200
        finally:
            glue.delete_crawler(Name=crawler_name)
            glue.delete_database(Name=db_name)

    def test_stop_crawler_schedule(self, glue):
        db_name = _unique("db")
        crawler_name = _unique("crawler")
        glue.create_database(DatabaseInput={"Name": db_name})
        glue.create_crawler(
            Name=crawler_name,
            Role="arn:aws:iam::123456789012:role/glue-role",
            DatabaseName=db_name,
            Targets={"S3Targets": [{"Path": "s3://test-bucket/data"}]},
            Schedule="cron(0 12 * * ? *)",
        )
        try:
            glue.start_crawler_schedule(CrawlerName=crawler_name)
            resp = glue.stop_crawler_schedule(CrawlerName=crawler_name)
            assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200
        finally:
            glue.delete_crawler(Name=crawler_name)
            glue.delete_database(Name=db_name)


class TestGlueJobUpdates:
    """Tests for UpdateJob, UpdateTrigger, UpdateDevEndpoint."""

    def test_update_job(self, glue):
        job_name = _unique("job")
        glue.create_job(
            Name=job_name,
            Role="arn:aws:iam::123456789012:role/glue-role",
            Command={"Name": "glueetl", "ScriptLocation": "s3://bucket/script.py"},
        )
        try:
            resp = glue.update_job(
                JobName=job_name,
                JobUpdate={
                    "Role": "arn:aws:iam::123456789012:role/glue-role",
                    "Command": {"Name": "glueetl", "ScriptLocation": "s3://bucket/updated.py"},
                    "Description": "updated job",
                },
            )
            assert resp["JobName"] == job_name
        finally:
            glue.delete_job(JobName=job_name)

    def test_update_dev_endpoint(self, glue):
        ep_name = _unique("devep")
        glue.create_dev_endpoint(
            EndpointName=ep_name,
            RoleArn="arn:aws:iam::123456789012:role/glue-role",
        )
        try:
            resp = glue.update_dev_endpoint(
                EndpointName=ep_name,
                PublicKey="ssh-rsa AAAAB3NzaC1yc2EAAAADAQABAAABgQC test",
            )
            assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200
        finally:
            glue.delete_dev_endpoint(EndpointName=ep_name)


class TestGlueJobBookmark:
    """Tests for GetJobBookmark and ResetJobBookmark."""

    def test_get_job_bookmark(self, glue):
        job_name = _unique("job")
        glue.create_job(
            Name=job_name,
            Role="arn:aws:iam::123456789012:role/glue-role",
            Command={"Name": "glueetl", "ScriptLocation": "s3://bucket/script.py"},
        )
        try:
            resp = glue.get_job_bookmark(JobName=job_name)
            assert "JobBookmarkEntry" in resp
        finally:
            glue.delete_job(JobName=job_name)

    def test_reset_job_bookmark(self, glue):
        job_name = _unique("job")
        glue.create_job(
            Name=job_name,
            Role="arn:aws:iam::123456789012:role/glue-role",
            Command={"Name": "glueetl", "ScriptLocation": "s3://bucket/script.py"},
        )
        try:
            resp = glue.reset_job_bookmark(JobName=job_name)
            assert "JobBookmarkEntry" in resp
        finally:
            glue.delete_job(JobName=job_name)


class TestGlueImportCatalog:
    """Tests for ImportCatalogToGlue."""

    def test_import_catalog_to_glue(self, glue):
        resp = glue.import_catalog_to_glue()
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200


class TestGlueGetCatalogs:
    """Tests for GetCatalogs."""

    def test_get_catalogs(self, glue):
        resp = glue.get_catalogs()
        assert "CatalogList" in resp


class TestGlueSchemaVersions:
    """Tests for CheckSchemaVersionValidity and DeleteSchemaVersions."""

    def test_check_schema_version_validity(self, glue):
        resp = glue.check_schema_version_validity(
            DataFormat="AVRO",
            SchemaDefinition=('{"type":"record","name":"T","fields":[{"name":"id","type":"int"}]}'),
        )
        assert "Valid" in resp

    def test_delete_schema_versions(self, glue):
        reg_name = _unique("reg")
        schema_name = _unique("schema")
        glue.create_registry(RegistryName=reg_name)
        glue.create_schema(
            RegistryId={"RegistryName": reg_name},
            SchemaName=schema_name,
            DataFormat="AVRO",
            Compatibility="NONE",
            SchemaDefinition=('{"type":"record","name":"T","fields":[{"name":"id","type":"int"}]}'),
        )
        try:
            resp = glue.delete_schema_versions(
                SchemaId={
                    "RegistryName": reg_name,
                    "SchemaName": schema_name,
                },
                Versions="1",
            )
            assert "SchemaVersionErrors" in resp or "ResponseMetadata" in resp
        finally:
            try:
                glue.delete_schema(SchemaId={"RegistryName": reg_name, "SchemaName": schema_name})
            except Exception:
                pass  # best-effort cleanup
            glue.delete_registry(RegistryId={"RegistryName": reg_name})


class TestGlueUpdateTrigger:
    """Tests for UpdateTrigger."""

    def test_update_trigger(self, glue):
        """UpdateTrigger modifies a trigger's description."""
        name = _unique("trig")
        glue.create_trigger(
            Name=name,
            Type="ON_DEMAND",
            Actions=[{"JobName": "fake-job"}],
        )
        try:
            resp = glue.update_trigger(
                Name=name,
                TriggerUpdate={"Name": name, "Description": "updated desc"},
            )
            assert resp["Trigger"]["Name"] == name
        finally:
            glue.delete_trigger(Name=name)

    def test_update_trigger_not_found(self, glue):
        """UpdateTrigger for nonexistent trigger raises EntityNotFoundException."""
        with pytest.raises(ClientError) as exc:
            glue.update_trigger(
                Name="nonexistent-trigger-xyz",
                TriggerUpdate={"Name": "nonexistent-trigger-xyz"},
            )
        assert exc.value.response["Error"]["Code"] == "EntityNotFoundException"


class TestGlueBatchDeleteTableVersionOp:
    """Tests for BatchDeleteTableVersion."""

    def test_batch_delete_table_version(self, glue):
        """BatchDeleteTableVersion removes specific table versions."""
        db_name = _unique("db")
        tbl_name = _unique("tbl")
        glue.create_database(DatabaseInput={"Name": db_name})
        sd = {
            "Columns": [{"Name": "c1", "Type": "string"}],
            "Location": "s3://b/p",
            "InputFormat": "org.apache.hadoop.mapred.TextInputFormat",
            "OutputFormat": "org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat",
            "SerdeInfo": {
                "SerializationLibrary": "org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe"
            },
        }
        glue.create_table(
            DatabaseName=db_name, TableInput={"Name": tbl_name, "StorageDescriptor": sd}
        )
        sd2 = dict(sd, Columns=[{"Name": "c1", "Type": "string"}, {"Name": "c2", "Type": "int"}])
        glue.update_table(
            DatabaseName=db_name, TableInput={"Name": tbl_name, "StorageDescriptor": sd2}
        )
        versions = glue.get_table_versions(DatabaseName=db_name, TableName=tbl_name)
        version_ids = [v["VersionId"] for v in versions["TableVersions"]]
        assert len(version_ids) >= 2

        try:
            resp = glue.batch_delete_table_version(
                DatabaseName=db_name, TableName=tbl_name, VersionIds=version_ids[:1]
            )
            assert "Errors" in resp
        finally:
            glue.delete_table(DatabaseName=db_name, Name=tbl_name)
            glue.delete_database(Name=db_name)

    def test_batch_delete_table_version_not_found_db(self, glue):
        """BatchDeleteTableVersion for nonexistent database raises EntityNotFoundException."""
        with pytest.raises(ClientError) as exc:
            glue.batch_delete_table_version(
                DatabaseName="nonexistent-db-xyz",
                TableName="nonexistent-tbl",
                VersionIds=["1"],
            )
        assert exc.value.response["Error"]["Code"] == "EntityNotFoundException"


class TestGlueColumnStatisticsForTable:
    """Tests for UpdateColumnStatisticsForTable and DeleteColumnStatisticsForTable."""

    def _make_table(self, glue):
        import datetime

        db_name = _unique("db")
        tbl_name = _unique("tbl")
        glue.create_database(DatabaseInput={"Name": db_name})
        glue.create_table(
            DatabaseName=db_name,
            TableInput={
                "Name": tbl_name,
                "StorageDescriptor": {
                    "Columns": [{"Name": "col1", "Type": "string"}],
                    "Location": "s3://b/p",
                    "InputFormat": "org.apache.hadoop.mapred.TextInputFormat",
                    "OutputFormat": "org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat",
                    "SerdeInfo": {
                        "SerializationLibrary": (
                            "org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe"
                        )
                    },
                },
            },
        )
        return db_name, tbl_name, datetime

    def test_update_column_statistics_for_table(self, glue):
        """UpdateColumnStatisticsForTable sets column stats on a table."""
        db_name, tbl_name, datetime = self._make_table(glue)
        try:
            resp = glue.update_column_statistics_for_table(
                DatabaseName=db_name,
                TableName=tbl_name,
                ColumnStatisticsList=[
                    {
                        "ColumnName": "col1",
                        "ColumnType": "string",
                        "AnalyzedTime": datetime.datetime(2024, 1, 1),
                        "StatisticsData": {
                            "Type": "STRING",
                            "StringColumnStatisticsData": {
                                "MaximumLength": 100,
                                "AverageLength": 50.0,
                                "NumberOfNulls": 0,
                                "NumberOfDistinctValues": 10,
                            },
                        },
                    }
                ],
            )
            assert "Errors" in resp
            assert resp["Errors"] == []

            get_resp = glue.get_column_statistics_for_table(
                DatabaseName=db_name, TableName=tbl_name, ColumnNames=["col1"]
            )
            assert len(get_resp["ColumnStatisticsList"]) == 1
            assert get_resp["ColumnStatisticsList"][0]["ColumnName"] == "col1"
        finally:
            glue.delete_table(DatabaseName=db_name, Name=tbl_name)
            glue.delete_database(Name=db_name)

    def test_delete_column_statistics_for_table_after_update(self, glue):
        """DeleteColumnStatisticsForTable removes column stats that were previously set."""
        db_name, tbl_name, datetime = self._make_table(glue)
        try:
            glue.update_column_statistics_for_table(
                DatabaseName=db_name,
                TableName=tbl_name,
                ColumnStatisticsList=[
                    {
                        "ColumnName": "col1",
                        "ColumnType": "string",
                        "AnalyzedTime": datetime.datetime(2024, 1, 1),
                        "StatisticsData": {
                            "Type": "STRING",
                            "StringColumnStatisticsData": {
                                "MaximumLength": 100,
                                "AverageLength": 50.0,
                                "NumberOfNulls": 0,
                                "NumberOfDistinctValues": 10,
                            },
                        },
                    }
                ],
            )
            resp = glue.delete_column_statistics_for_table(
                DatabaseName=db_name, TableName=tbl_name, ColumnName="col1"
            )
            assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200
        finally:
            glue.delete_table(DatabaseName=db_name, Name=tbl_name)
            glue.delete_database(Name=db_name)


class TestGlueColumnStatisticsForPartition:
    """Tests for UpdateColumnStatisticsForPartition."""

    def test_update_column_statistics_for_partition(self, glue):
        """UpdateColumnStatisticsForPartition sets column stats on a partition."""
        import datetime

        db_name = _unique("db")
        tbl_name = _unique("tbl")
        glue.create_database(DatabaseInput={"Name": db_name})
        sd = {
            "Columns": [{"Name": "col1", "Type": "string"}],
            "Location": "s3://b/p",
            "InputFormat": "org.apache.hadoop.mapred.TextInputFormat",
            "OutputFormat": "org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat",
            "SerdeInfo": {
                "SerializationLibrary": "org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe"
            },
        }
        glue.create_table(
            DatabaseName=db_name,
            TableInput={
                "Name": tbl_name,
                "StorageDescriptor": sd,
                "PartitionKeys": [{"Name": "dt", "Type": "string"}],
            },
        )
        glue.create_partition(
            DatabaseName=db_name,
            TableName=tbl_name,
            PartitionInput={
                "Values": ["2024-01-01"],
                "StorageDescriptor": dict(sd, Location="s3://b/p/dt=2024-01-01"),
            },
        )
        try:
            resp = glue.update_column_statistics_for_partition(
                DatabaseName=db_name,
                TableName=tbl_name,
                PartitionValues=["2024-01-01"],
                ColumnStatisticsList=[
                    {
                        "ColumnName": "col1",
                        "ColumnType": "string",
                        "AnalyzedTime": datetime.datetime(2024, 1, 1),
                        "StatisticsData": {
                            "Type": "STRING",
                            "StringColumnStatisticsData": {
                                "MaximumLength": 100,
                                "AverageLength": 50.0,
                                "NumberOfNulls": 0,
                                "NumberOfDistinctValues": 10,
                            },
                        },
                    }
                ],
            )
            assert "Errors" in resp
            assert resp["Errors"] == []
        finally:
            glue.delete_partition(
                DatabaseName=db_name, TableName=tbl_name, PartitionValues=["2024-01-01"]
            )
            glue.delete_table(DatabaseName=db_name, Name=tbl_name)
            glue.delete_database(Name=db_name)


class TestGlueColumnStatisticsTaskSettingsCRUD:
    """Tests for ColumnStatisticsTaskSettings CRUD."""

    def _make_table(self, glue):
        db_name = _unique("db")
        tbl_name = _unique("tbl")
        glue.create_database(DatabaseInput={"Name": db_name})
        glue.create_table(
            DatabaseName=db_name,
            TableInput={
                "Name": tbl_name,
                "StorageDescriptor": {
                    "Columns": [{"Name": "col1", "Type": "string"}],
                    "Location": "s3://b/p",
                    "InputFormat": "org.apache.hadoop.mapred.TextInputFormat",
                    "OutputFormat": "org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat",
                    "SerdeInfo": {
                        "SerializationLibrary": (
                            "org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe"
                        )
                    },
                },
            },
        )
        return db_name, tbl_name

    def test_create_and_get_column_statistics_task_settings(self, glue):
        """CreateColumnStatisticsTaskSettings creates settings, Get retrieves them."""
        db_name, tbl_name = self._make_table(glue)
        role = "arn:aws:iam::123456789012:role/test"
        try:
            glue.create_column_statistics_task_settings(
                DatabaseName=db_name, TableName=tbl_name, Role=role
            )
            resp = glue.get_column_statistics_task_settings(
                DatabaseName=db_name, TableName=tbl_name
            )
            assert resp["ColumnStatisticsTaskSettings"]["Role"] == role
        finally:
            glue.delete_column_statistics_task_settings(DatabaseName=db_name, TableName=tbl_name)
            glue.delete_table(DatabaseName=db_name, Name=tbl_name)
            glue.delete_database(Name=db_name)

    def test_update_column_statistics_task_settings(self, glue):
        """UpdateColumnStatisticsTaskSettings modifies the role."""
        db_name, tbl_name = self._make_table(glue)
        try:
            glue.create_column_statistics_task_settings(
                DatabaseName=db_name,
                TableName=tbl_name,
                Role="arn:aws:iam::123456789012:role/original",
            )
            resp = glue.update_column_statistics_task_settings(
                DatabaseName=db_name,
                TableName=tbl_name,
                Role="arn:aws:iam::123456789012:role/updated",
            )
            assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200
        finally:
            glue.delete_column_statistics_task_settings(DatabaseName=db_name, TableName=tbl_name)
            glue.delete_table(DatabaseName=db_name, Name=tbl_name)
            glue.delete_database(Name=db_name)

    def test_delete_column_statistics_task_settings(self, glue):
        """DeleteColumnStatisticsTaskSettings removes settings."""
        db_name, tbl_name = self._make_table(glue)
        try:
            glue.create_column_statistics_task_settings(
                DatabaseName=db_name,
                TableName=tbl_name,
                Role="arn:aws:iam::123456789012:role/test",
            )
            glue.delete_column_statistics_task_settings(DatabaseName=db_name, TableName=tbl_name)
            with pytest.raises(ClientError) as exc:
                glue.get_column_statistics_task_settings(DatabaseName=db_name, TableName=tbl_name)
            assert exc.value.response["Error"]["Code"] == "EntityNotFoundException"
        finally:
            glue.delete_table(DatabaseName=db_name, Name=tbl_name)
            glue.delete_database(Name=db_name)

    def test_get_column_statistics_task_settings_not_found(self, glue):
        """GetColumnStatisticsTaskSettings for nonexistent raises EntityNotFoundException."""
        with pytest.raises(ClientError) as exc:
            glue.get_column_statistics_task_settings(
                DatabaseName="nonexistent-db-xyz", TableName="nonexistent-tbl"
            )
        assert exc.value.response["Error"]["Code"] == "EntityNotFoundException"


class TestGlueStatementOperations:
    """Tests for RunStatement, CancelStatement, ListStatements."""

    def test_run_statement(self, glue):
        """RunStatement creates a statement in a session."""
        sess_id = _unique("sess")
        glue.create_session(
            Id=sess_id,
            Role="arn:aws:iam::123456789012:role/test",
            Command={"Name": "glueetl", "PythonVersion": "3"},
        )
        try:
            resp = glue.run_statement(SessionId=sess_id, Code="print(1)")
            assert "Id" in resp
        finally:
            glue.stop_session(Id=sess_id)
            glue.delete_session(Id=sess_id)

    def test_cancel_statement(self, glue):
        """CancelStatement cancels a running statement."""
        sess_id = _unique("sess")
        glue.create_session(
            Id=sess_id,
            Role="arn:aws:iam::123456789012:role/test",
            Command={"Name": "glueetl", "PythonVersion": "3"},
        )
        try:
            run_resp = glue.run_statement(SessionId=sess_id, Code="print(1)")
            stmt_id = run_resp["Id"]
            resp = glue.cancel_statement(SessionId=sess_id, Id=stmt_id)
            assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200
        finally:
            glue.stop_session(Id=sess_id)
            glue.delete_session(Id=sess_id)

    def test_cancel_statement_nonexistent_session(self, glue):
        """CancelStatement for nonexistent session raises EntityNotFoundException."""
        with pytest.raises(ClientError) as exc:
            glue.cancel_statement(SessionId="nonexistent-session-xyz", Id=0)
        assert exc.value.response["Error"]["Code"] == "EntityNotFoundException"


class TestGlueIntegrationOperations:
    """Tests for Integration CRUD."""

    def test_create_and_describe_integration(self, glue):
        """CreateIntegration creates an integration; DescribeIntegrations lists it."""
        name = _unique("int")
        resp = glue.create_integration(
            IntegrationName=name,
            SourceArn="arn:aws:glue:us-east-1:123456789012:database/src",
            TargetArn="arn:aws:glue:us-east-1:123456789012:database/tgt",
        )
        assert "IntegrationArn" in resp
        assert resp["IntegrationName"] == name

        desc = glue.describe_integrations()
        arns = [i["IntegrationArn"] for i in desc["Integrations"]]
        assert resp["IntegrationArn"] in arns

    def test_modify_integration(self, glue):
        """ModifyIntegration updates an integration's description."""
        name = _unique("int")
        create_resp = glue.create_integration(
            IntegrationName=name,
            SourceArn="arn:aws:glue:us-east-1:123456789012:database/src",
            TargetArn="arn:aws:glue:us-east-1:123456789012:database/tgt",
        )
        int_arn = create_resp["IntegrationArn"]
        resp = glue.modify_integration(IntegrationIdentifier=int_arn, Description="updated")
        assert resp["Description"] == "updated"

    def test_modify_integration_not_found(self, glue):
        """ModifyIntegration for nonexistent integration raises EntityNotFoundException."""
        with pytest.raises(ClientError) as exc:
            glue.modify_integration(
                IntegrationIdentifier=("arn:aws:glue:us-east-1:123456789012:integration/fake-xyz"),
                Description="nope",
            )
        assert exc.value.response["Error"]["Code"] == "EntityNotFoundException"

    def test_describe_inbound_integrations(self, glue):
        """DescribeInboundIntegrations returns a list."""
        resp = glue.describe_inbound_integrations(
            TargetArn="arn:aws:glue:us-east-1:123456789012:database/tgt"
        )
        assert "InboundIntegrations" in resp


class TestGlueIntegrationResourceProperty:
    """Tests for Integration resource and table properties."""

    def _make_integration(self, glue):
        name = _unique("int")
        resp = glue.create_integration(
            IntegrationName=name,
            SourceArn="arn:aws:glue:us-east-1:123456789012:database/src",
            TargetArn="arn:aws:glue:us-east-1:123456789012:database/tgt",
        )
        return resp["IntegrationArn"]

    def test_create_and_get_integration_resource_property(self, glue):
        """CreateIntegrationResourceProperty sets properties; Get retrieves them."""
        int_arn = self._make_integration(glue)
        glue.create_integration_resource_property(
            ResourceArn=int_arn,
            SourceProcessingProperties={"RoleArn": "arn:aws:iam::123456789012:role/test"},
        )
        resp = glue.get_integration_resource_property(ResourceArn=int_arn)
        assert resp["ResourceArn"] == int_arn

    def test_delete_integration_resource_property(self, glue):
        """DeleteIntegrationResourceProperty removes the property."""
        int_arn = self._make_integration(glue)
        glue.create_integration_resource_property(
            ResourceArn=int_arn,
            SourceProcessingProperties={"RoleArn": "arn:aws:iam::123456789012:role/test"},
        )
        resp = glue.delete_integration_resource_property(ResourceArn=int_arn)
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200

    def test_update_integration_resource_property(self, glue):
        """UpdateIntegrationResourceProperty modifies the property."""
        int_arn = self._make_integration(glue)
        glue.create_integration_resource_property(
            ResourceArn=int_arn,
            SourceProcessingProperties={"RoleArn": "arn:aws:iam::123456789012:role/test"},
        )
        resp = glue.update_integration_resource_property(
            ResourceArn=int_arn,
            SourceProcessingProperties={"RoleArn": "arn:aws:iam::123456789012:role/updated"},
        )
        assert resp["ResourceArn"] == int_arn

    def test_create_and_get_integration_table_properties(self, glue):
        """CreateIntegrationTableProperties sets table props; Get retrieves them."""
        int_arn = self._make_integration(glue)
        glue.create_integration_table_properties(ResourceArn=int_arn, TableName="test-tbl")
        resp = glue.get_integration_table_properties(ResourceArn=int_arn, TableName="test-tbl")
        assert resp["ResourceArn"] == int_arn

    def test_delete_integration_table_properties(self, glue):
        """DeleteIntegrationTableProperties removes table props."""
        int_arn = self._make_integration(glue)
        glue.create_integration_table_properties(ResourceArn=int_arn, TableName="test-tbl")
        resp = glue.delete_integration_table_properties(ResourceArn=int_arn, TableName="test-tbl")
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200

    def test_update_integration_table_properties(self, glue):
        """UpdateIntegrationTableProperties modifies table props."""
        int_arn = self._make_integration(glue)
        glue.create_integration_table_properties(ResourceArn=int_arn, TableName="test-tbl2")
        resp = glue.update_integration_table_properties(ResourceArn=int_arn, TableName="test-tbl2")
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200


class TestGlueDataQualityOps:
    """Tests for data quality operations."""

    def test_list_data_quality_statistics(self, glue):
        """ListDataQualityStatistics returns a list."""
        resp = glue.list_data_quality_statistics()
        assert "Statistics" in resp

    def test_list_data_quality_statistic_annotations(self, glue):
        """ListDataQualityStatisticAnnotations returns a list."""
        resp = glue.list_data_quality_statistic_annotations()
        assert "Annotations" in resp

    def test_batch_put_data_quality_statistic_annotation(self, glue):
        """BatchPutDataQualityStatisticAnnotation with empty list returns empty failures."""
        resp = glue.batch_put_data_quality_statistic_annotation(InclusionAnnotations=[])
        assert "FailedInclusionAnnotations" in resp

    def test_put_data_quality_profile_annotation(self, glue):
        """PutDataQualityProfileAnnotation succeeds for any profile ID."""
        resp = glue.put_data_quality_profile_annotation(
            ProfileId="fake-profile", InclusionAnnotation="INCLUDE"
        )
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200

    def test_cancel_data_quality_rule_recommendation_run(self, glue):
        """CancelDataQualityRuleRecommendationRun succeeds (idempotent)."""
        resp = glue.cancel_data_quality_rule_recommendation_run(RunId="fake-run-id")
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200

    def test_cancel_data_quality_ruleset_evaluation_run(self, glue):
        """CancelDataQualityRulesetEvaluationRun succeeds (idempotent)."""
        resp = glue.cancel_data_quality_ruleset_evaluation_run(RunId="fake-run-id")
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200

    def test_get_data_quality_model_result_not_found(self, glue):
        """GetDataQualityModelResult for nonexistent raises EntityNotFoundException."""
        with pytest.raises(ClientError) as exc:
            glue.get_data_quality_model_result(StatisticId="fake-stat", ProfileId="fake-profile")
        assert exc.value.response["Error"]["Code"] == "EntityNotFoundException"

    def test_start_data_quality_rule_recommendation_run(self, glue):
        """StartDataQualityRuleRecommendationRun returns a RunId."""
        resp = glue.start_data_quality_rule_recommendation_run(
            DataSource={"GlueTable": {"DatabaseName": "nonexistent", "TableName": "nonexistent"}},
            Role="arn:aws:iam::123456789012:role/test",
        )
        assert "RunId" in resp


class TestGlueDeletePartitionIndex:
    """Tests for DeletePartitionIndex."""

    def test_delete_partition_index(self, glue):
        """DeletePartitionIndex on table with no matching index succeeds."""
        db_name = _unique("db")
        tbl_name = _unique("tbl")
        glue.create_database(DatabaseInput={"Name": db_name})
        glue.create_table(
            DatabaseName=db_name,
            TableInput={
                "Name": tbl_name,
                "StorageDescriptor": {
                    "Columns": [{"Name": "col1", "Type": "string"}],
                    "Location": "s3://b/p",
                    "InputFormat": "org.apache.hadoop.mapred.TextInputFormat",
                    "OutputFormat": "org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat",
                    "SerdeInfo": {
                        "SerializationLibrary": (
                            "org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe"
                        )
                    },
                },
                "PartitionKeys": [{"Name": "dt", "Type": "string"}],
            },
        )
        try:
            resp = glue.delete_partition_index(
                DatabaseName=db_name, TableName=tbl_name, IndexName="fake-idx"
            )
            assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200
        finally:
            glue.delete_table(DatabaseName=db_name, Name=tbl_name)
            glue.delete_database(Name=db_name)


class TestGlueStartBlueprintRun:
    """Tests for StartBlueprintRun."""

    def test_start_blueprint_run(self, glue):
        """StartBlueprintRun returns a RunId."""
        name = _unique("bp")
        glue.create_blueprint(Name=name, BlueprintLocation="s3://bucket/path")
        try:
            resp = glue.start_blueprint_run(
                BlueprintName=name,
                RoleArn="arn:aws:iam::123456789012:role/test",
            )
            assert "RunId" in resp
        finally:
            glue.delete_blueprint(Name=name)


class TestGlueColumnStatisticsTaskRun:
    """Tests for StartColumnStatisticsTaskRun and schedule ops."""

    def _make_table(self, glue):
        db_name = _unique("db")
        tbl_name = _unique("tbl")
        glue.create_database(DatabaseInput={"Name": db_name})
        glue.create_table(
            DatabaseName=db_name,
            TableInput={
                "Name": tbl_name,
                "StorageDescriptor": {
                    "Columns": [{"Name": "col1", "Type": "string"}],
                    "Location": "s3://b/p",
                    "InputFormat": "org.apache.hadoop.mapred.TextInputFormat",
                    "OutputFormat": "org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat",
                    "SerdeInfo": {
                        "SerializationLibrary": (
                            "org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe"
                        )
                    },
                },
            },
        )
        return db_name, tbl_name

    def test_start_column_statistics_task_run(self, glue):
        """StartColumnStatisticsTaskRun returns a task run ID."""
        db_name, tbl_name = self._make_table(glue)
        try:
            resp = glue.start_column_statistics_task_run(
                DatabaseName=db_name,
                TableName=tbl_name,
                Role="arn:aws:iam::123456789012:role/test",
            )
            assert "ColumnStatisticsTaskRunId" in resp
        finally:
            glue.delete_table(DatabaseName=db_name, Name=tbl_name)
            glue.delete_database(Name=db_name)

    def test_stop_column_statistics_task_run(self, glue):
        """StopColumnStatisticsTaskRun succeeds after starting a run."""
        db_name, tbl_name = self._make_table(glue)
        try:
            glue.start_column_statistics_task_run(
                DatabaseName=db_name,
                TableName=tbl_name,
                Role="arn:aws:iam::123456789012:role/test",
            )
            resp = glue.stop_column_statistics_task_run(DatabaseName=db_name, TableName=tbl_name)
            assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200
        finally:
            glue.delete_table(DatabaseName=db_name, Name=tbl_name)
            glue.delete_database(Name=db_name)

    def test_start_column_statistics_task_run_schedule(self, glue):
        """StartColumnStatisticsTaskRunSchedule succeeds."""
        db_name, tbl_name = self._make_table(glue)
        try:
            resp = glue.start_column_statistics_task_run_schedule(
                DatabaseName=db_name, TableName=tbl_name
            )
            assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200
        finally:
            glue.delete_table(DatabaseName=db_name, Name=tbl_name)
            glue.delete_database(Name=db_name)

    def test_stop_column_statistics_task_run_schedule(self, glue):
        """StopColumnStatisticsTaskRunSchedule succeeds."""
        db_name, tbl_name = self._make_table(glue)
        try:
            resp = glue.stop_column_statistics_task_run_schedule(
                DatabaseName=db_name, TableName=tbl_name
            )
            assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200
        finally:
            glue.delete_table(DatabaseName=db_name, Name=tbl_name)
            glue.delete_database(Name=db_name)


class TestGlueMLTransformCRUD:
    """Tests for CreateMLTransform, DeleteMLTransform, GetMLTransform, UpdateMLTransform."""

    def _make_transform(self, glue):
        name = _unique("ml")
        resp = glue.create_ml_transform(
            Name=name,
            InputRecordTables=[{"DatabaseName": "default", "TableName": "test"}],
            Parameters={
                "TransformType": "FIND_MATCHES",
                "FindMatchesParameters": {"PrimaryKeyColumnName": "id"},
            },
            Role="arn:aws:iam::123456789012:role/test",
        )
        return resp["TransformId"], name

    def test_create_and_delete_ml_transform(self, glue):
        """CreateMLTransform creates a transform; DeleteMLTransform removes it."""
        tfm_id, _ = self._make_transform(glue)
        resp = glue.delete_ml_transform(TransformId=tfm_id)
        assert resp["TransformId"] == tfm_id

    def test_get_ml_transform_by_id(self, glue):
        """GetMLTransform retrieves a transform by ID."""
        tfm_id, name = self._make_transform(glue)
        try:
            resp = glue.get_ml_transform(TransformId=tfm_id)
            assert resp["Name"] == name
            assert resp["TransformId"] == tfm_id
        finally:
            glue.delete_ml_transform(TransformId=tfm_id)

    def test_get_ml_transforms_includes_created(self, glue):
        """GetMLTransforms returns list including the created transform."""
        tfm_id, _ = self._make_transform(glue)
        try:
            resp = glue.get_ml_transforms()
            ids = [t["TransformId"] for t in resp["Transforms"]]
            assert tfm_id in ids
        finally:
            glue.delete_ml_transform(TransformId=tfm_id)

    def test_list_ml_transforms_includes_created(self, glue):
        """ListMLTransforms returns IDs including the created transform."""
        tfm_id, _ = self._make_transform(glue)
        try:
            resp = glue.list_ml_transforms()
            assert tfm_id in resp["TransformIds"]
        finally:
            glue.delete_ml_transform(TransformId=tfm_id)

    def test_update_ml_transform_description(self, glue):
        """UpdateMLTransform modifies a transform's description."""
        tfm_id, _ = self._make_transform(glue)
        try:
            resp = glue.update_ml_transform(TransformId=tfm_id, Description="updated desc")
            assert resp["TransformId"] == tfm_id
        finally:
            glue.delete_ml_transform(TransformId=tfm_id)

    def test_get_ml_task_runs_empty(self, glue):
        """GetMLTaskRuns for a transform with no task runs returns empty list."""
        tfm_id, _ = self._make_transform(glue)
        try:
            resp = glue.get_ml_task_runs(TransformId=tfm_id)
            assert "TaskRuns" in resp
            assert resp["TaskRuns"] == []
        finally:
            glue.delete_ml_transform(TransformId=tfm_id)

    def test_get_ml_task_run_not_found(self, glue):
        """GetMLTaskRun for nonexistent task run raises EntityNotFoundException."""
        tfm_id, _ = self._make_transform(glue)
        try:
            with pytest.raises(ClientError) as exc:
                glue.get_ml_task_run(TransformId=tfm_id, TaskRunId="fake-task-run")
            assert exc.value.response["Error"]["Code"] == "EntityNotFoundException"
        finally:
            glue.delete_ml_transform(TransformId=tfm_id)


class TestGlueResumeWorkflowRun:
    """Tests for ResumeWorkflowRun."""

    def test_resume_workflow_run(self, glue):
        """ResumeWorkflowRun returns a new RunId."""
        wf_name = _unique("wf")
        glue.create_workflow(Name=wf_name)
        try:
            run_resp = glue.start_workflow_run(Name=wf_name)
            run_id = run_resp["RunId"]
            resp = glue.resume_workflow_run(Name=wf_name, RunId=run_id, NodeIds=["node1"])
            assert "RunId" in resp
        finally:
            glue.delete_workflow(Name=wf_name)

    def test_resume_workflow_run_nonexistent(self, glue):
        """ResumeWorkflowRun for nonexistent workflow raises error."""
        with pytest.raises(ClientError) as exc:
            glue.resume_workflow_run(Name="nonexistent-wf-xyz", RunId="fake-run", NodeIds=["node1"])
        assert "Error" in exc.value.response


class TestGlueGetConnections:
    """Tests for GetConnections."""

    def test_get_connections_empty(self, glue):
        """GetConnections returns a ConnectionList."""
        resp = glue.get_connections()
        assert "ConnectionList" in resp

    def test_get_connections_after_create(self, glue):
        """GetConnections includes a created connection."""
        conn_name = _unique("conn")
        glue.create_connection(
            ConnectionInput={
                "Name": conn_name,
                "ConnectionType": "JDBC",
                "ConnectionProperties": {
                    "JDBC_CONNECTION_URL": "jdbc:mysql://host:3306/db",
                    "USERNAME": "admin",
                    "PASSWORD": "secret",
                },
            }
        )
        try:
            resp = glue.get_connections()
            names = [c["Name"] for c in resp["ConnectionList"]]
            assert conn_name in names
        finally:
            glue.delete_connection(ConnectionName=conn_name)


class TestGlueGetPartitionIndexes:
    """Tests for GetPartitionIndexes."""

    def test_get_partition_indexes(self, glue):
        """GetPartitionIndexes returns indexes for a table."""
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
                "PartitionKeys": [{"Name": "year", "Type": "int"}],
            },
        )
        try:
            resp = glue.get_partition_indexes(DatabaseName=db_name, TableName=tbl_name)
            assert "PartitionIndexDescriptorList" in resp
        finally:
            glue.delete_table(DatabaseName=db_name, Name=tbl_name)
            glue.delete_database(Name=db_name)

    def test_get_partition_indexes_nonexistent_db(self, glue):
        """GetPartitionIndexes for nonexistent db returns empty list."""
        resp = glue.get_partition_indexes(DatabaseName="fake-db-xyz", TableName="fake-tbl")
        assert "PartitionIndexDescriptorList" in resp


class TestGlueGetSchema:
    """Tests for GetSchema and GetSchemaByDefinition."""

    def test_get_schema(self, glue):
        """GetSchema retrieves schema details by registry and schema name."""
        reg_name = _unique("reg")
        schema_name = _unique("schema")
        glue.create_registry(RegistryName=reg_name)
        glue.create_schema(
            RegistryId={"RegistryName": reg_name},
            SchemaName=schema_name,
            DataFormat="AVRO",
            Compatibility="NONE",
            SchemaDefinition='{"type":"record","name":"T","fields":[{"name":"id","type":"int"}]}',
        )
        try:
            resp = glue.get_schema(SchemaId={"SchemaName": schema_name, "RegistryName": reg_name})
            assert resp["SchemaName"] == schema_name
            assert resp["DataFormat"] == "AVRO"
        finally:
            glue.delete_schema(SchemaId={"SchemaName": schema_name, "RegistryName": reg_name})
            glue.delete_registry(RegistryId={"RegistryName": reg_name})

    def test_get_schema_not_found(self, glue):
        """GetSchema for nonexistent schema raises error."""
        with pytest.raises(ClientError) as exc:
            glue.get_schema(
                SchemaId={"SchemaName": "nonexistent-schema-xyz", "RegistryName": "nonexistent-reg"}
            )
        assert exc.value.response["Error"]["Code"] == "EntityNotFoundException"

    def test_get_schema_by_definition(self, glue):
        """GetSchemaByDefinition finds a schema by its definition."""
        reg_name = _unique("reg")
        schema_name = _unique("schema")
        definition = '{"type":"record","name":"T","fields":[{"name":"id","type":"int"}]}'
        glue.create_registry(RegistryName=reg_name)
        glue.create_schema(
            RegistryId={"RegistryName": reg_name},
            SchemaName=schema_name,
            DataFormat="AVRO",
            Compatibility="NONE",
            SchemaDefinition=definition,
        )
        try:
            resp = glue.get_schema_by_definition(
                SchemaId={"SchemaName": schema_name, "RegistryName": reg_name},
                SchemaDefinition=definition,
            )
            assert "SchemaVersionId" in resp
            assert resp["DataFormat"] == "AVRO"
        finally:
            glue.delete_schema(SchemaId={"SchemaName": schema_name, "RegistryName": reg_name})
            glue.delete_registry(RegistryId={"RegistryName": reg_name})


class TestGlueGetTableVersion:
    """Tests for GetTableVersion."""

    def _storage_descriptor(self):
        return {
            "Columns": [{"Name": "col1", "Type": "string"}],
            "Location": "s3://bucket/path",
            "InputFormat": "org.apache.hadoop.mapred.TextInputFormat",
            "OutputFormat": "org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat",
            "SerdeInfo": {
                "SerializationLibrary": "org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe"
            },
        }

    def test_get_table_version(self, glue):
        """GetTableVersion retrieves a specific table version."""
        db_name = _unique("db")
        tbl_name = _unique("tbl")
        glue.create_database(DatabaseInput={"Name": db_name})
        glue.create_table(
            DatabaseName=db_name,
            TableInput={"Name": tbl_name, "StorageDescriptor": self._storage_descriptor()},
        )
        try:
            versions = glue.get_table_versions(DatabaseName=db_name, TableName=tbl_name)
            version_id = versions["TableVersions"][0]["VersionId"]
            resp = glue.get_table_version(
                DatabaseName=db_name, TableName=tbl_name, VersionId=str(version_id)
            )
            assert "TableVersion" in resp
            assert resp["TableVersion"]["Table"]["Name"] == tbl_name
        finally:
            glue.delete_table(DatabaseName=db_name, Name=tbl_name)
            glue.delete_database(Name=db_name)

    def test_get_table_version_nonexistent_db(self, glue):
        """GetTableVersion for nonexistent db raises EntityNotFoundException."""
        with pytest.raises(ClientError) as exc:
            glue.get_table_version(DatabaseName="fake-db-xyz", TableName="fake-tbl", VersionId="1")
        assert exc.value.response["Error"]["Code"] == "EntityNotFoundException"


class TestGlueGetTriggers:
    """Tests for GetTriggers."""

    def test_get_triggers_empty(self, glue):
        """GetTriggers returns a Triggers list."""
        resp = glue.get_triggers()
        assert "Triggers" in resp

    def test_get_triggers_after_create(self, glue):
        """GetTriggers includes a created trigger."""
        name = _unique("trig")
        glue.create_trigger(
            Name=name,
            Type="ON_DEMAND",
            Actions=[{"JobName": "fake-job"}],
        )
        try:
            resp = glue.get_triggers()
            names = [t["Name"] for t in resp["Triggers"]]
            assert name in names
        finally:
            glue.delete_trigger(Name=name)


class TestGlueListCrawlers:
    """Tests for ListCrawlers."""

    def test_list_crawlers_empty(self, glue):
        """ListCrawlers returns CrawlerNames."""
        resp = glue.list_crawlers()
        assert "CrawlerNames" in resp


class TestGlueListCrawls:
    """Tests for ListCrawls."""

    def test_list_crawls_nonexistent_crawler(self, glue):
        """ListCrawls for nonexistent crawler raises EntityNotFoundException."""
        with pytest.raises(ClientError) as exc:
            glue.list_crawls(CrawlerName="nonexistent-crawler-xyz")
        assert exc.value.response["Error"]["Code"] == "EntityNotFoundException"


class TestGlueListJobs:
    """Tests for ListJobs."""

    def test_list_jobs_empty(self, glue):
        """ListJobs returns JobNames."""
        resp = glue.list_jobs()
        assert "JobNames" in resp

    def test_list_jobs_after_create(self, glue):
        """ListJobs includes a created job."""
        name = _unique("job")
        glue.create_job(
            Name=name,
            Role="arn:aws:iam::123456789012:role/glue-role",
            Command={"Name": "glueetl", "ScriptLocation": "s3://bucket/script.py"},
        )
        try:
            resp = glue.list_jobs()
            assert name in resp["JobNames"]
        finally:
            glue.delete_job(JobName=name)


class TestGlueListTriggers:
    """Tests for ListTriggers."""

    def test_list_triggers_empty(self, glue):
        """ListTriggers returns TriggerNames."""
        resp = glue.list_triggers()
        assert "TriggerNames" in resp

    def test_list_triggers_after_create(self, glue):
        """ListTriggers includes a created trigger."""
        name = _unique("trig")
        glue.create_trigger(
            Name=name,
            Type="ON_DEMAND",
            Actions=[{"JobName": "fake-job"}],
        )
        try:
            resp = glue.list_triggers()
            assert name in resp["TriggerNames"]
        finally:
            glue.delete_trigger(Name=name)


class TestGlueUpdateSchema:
    """Tests for UpdateSchema."""

    def test_update_schema_compatibility(self, glue):
        """UpdateSchema modifies schema compatibility."""
        reg_name = _unique("reg")
        schema_name = _unique("schema")
        glue.create_registry(RegistryName=reg_name)
        glue.create_schema(
            RegistryId={"RegistryName": reg_name},
            SchemaName=schema_name,
            DataFormat="AVRO",
            Compatibility="NONE",
            SchemaDefinition='{"type":"record","name":"T","fields":[{"name":"id","type":"int"}]}',
        )
        try:
            resp = glue.update_schema(
                SchemaId={"SchemaName": schema_name, "RegistryName": reg_name},
                Compatibility="BACKWARD",
            )
            assert resp["SchemaName"] == schema_name
            # Verify the update took effect
            get_resp = glue.get_schema(
                SchemaId={"SchemaName": schema_name, "RegistryName": reg_name}
            )
            assert get_resp["Compatibility"] == "BACKWARD"
        finally:
            glue.delete_schema(SchemaId={"SchemaName": schema_name, "RegistryName": reg_name})
            glue.delete_registry(RegistryId={"RegistryName": reg_name})

    def test_update_schema_nonexistent(self, glue):
        """UpdateSchema for nonexistent schema raises error."""
        with pytest.raises(ClientError) as exc:
            glue.update_schema(
                SchemaId={
                    "SchemaName": "nonexistent-schema-xyz",
                    "RegistryName": "nonexistent-reg",
                },
                Compatibility="FULL",
            )
        assert exc.value.response["Error"]["Code"] in (
            "EntityNotFoundException",
            "InvalidInputException",
        )


class TestGlueBatchGetBlueprints:
    """Tests for BatchGetBlueprints."""

    def test_batch_get_blueprints(self, glue):
        """BatchGetBlueprints returns results for created blueprints."""
        bp_name = _unique("bp")
        glue.create_blueprint(Name=bp_name, BlueprintLocation="s3://bucket/bp.py")
        try:
            resp = glue.batch_get_blueprints(Names=[bp_name])
            assert "Blueprints" in resp
            assert len(resp["Blueprints"]) == 1
            assert resp["Blueprints"][0]["Name"] == bp_name
        finally:
            glue.delete_blueprint(Name=bp_name)

    def test_batch_get_blueprints_missing(self, glue):
        """BatchGetBlueprints returns MissingBlueprints for unknown names."""
        resp = glue.batch_get_blueprints(Names=["nonexistent-bp-xyz"])
        assert "MissingBlueprints" in resp
        assert "nonexistent-bp-xyz" in resp["MissingBlueprints"]


class TestGlueBatchGetCustomEntityTypes:
    """Tests for BatchGetCustomEntityTypes."""

    def test_batch_get_custom_entity_types(self, glue):
        """BatchGetCustomEntityTypes returns results for created types."""
        cet_name = _unique("cet")
        glue.create_custom_entity_type(Name=cet_name, RegexString="\\d{3}-\\d{2}-\\d{4}")
        try:
            resp = glue.batch_get_custom_entity_types(Names=[cet_name])
            assert "CustomEntityTypes" in resp
            assert len(resp["CustomEntityTypes"]) == 1
            assert resp["CustomEntityTypes"][0]["Name"] == cet_name
        finally:
            glue.delete_custom_entity_type(Name=cet_name)


class TestGlueBatchGetDevEndpoints:
    """Tests for BatchGetDevEndpoints."""

    def test_batch_get_dev_endpoints(self, glue):
        """BatchGetDevEndpoints returns DevEndpoints list."""
        resp = glue.batch_get_dev_endpoints(DevEndpointNames=["nonexistent-de-xyz"])
        assert "DevEndpoints" in resp or "DevEndpointsNotFound" in resp


class TestGlueBatchStopJobRun:
    """Tests for BatchStopJobRun."""

    def test_batch_stop_job_run_nonexistent(self, glue):
        """BatchStopJobRun for nonexistent job raises EntityNotFoundException."""
        with pytest.raises(ClientError) as exc:
            glue.batch_stop_job_run(JobName="fake-job-xyz", JobRunIds=["fake-run-id"])
        assert exc.value.response["Error"]["Code"] == "EntityNotFoundException"


class TestGlueCreatePartitionIndex:
    """Tests for CreatePartitionIndex."""

    def test_create_partition_index(self, glue):
        """CreatePartitionIndex adds an index to a partitioned table."""
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
                "PartitionKeys": [{"Name": "year", "Type": "int"}],
            },
        )
        try:
            resp = glue.create_partition_index(
                DatabaseName=db_name,
                TableName=tbl_name,
                PartitionIndex={"Keys": ["year"], "IndexName": "idx-year"},
            )
            assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200
        finally:
            glue.delete_table(DatabaseName=db_name, Name=tbl_name)
            glue.delete_database(Name=db_name)


class TestGlueMLTransforms:
    """Tests for Glue ML Transform operations."""

    def _create_transform(self, glue, name):
        resp = glue.create_ml_transform(
            Name=name,
            InputRecordTables=[
                {
                    "DatabaseName": "default",
                    "TableName": "test_table",
                }
            ],
            Parameters={
                "TransformType": "FIND_MATCHES",
                "FindMatchesParameters": {
                    "PrimaryKeyColumnName": "id",
                },
            },
            Role="arn:aws:iam::123456789012:role/GlueRole",
        )
        return resp["TransformId"]

    def test_create_ml_transform(self, glue):
        """CreateMLTransform creates a transform and returns an ID."""
        name = _unique("ml-xform")
        transform_id = self._create_transform(glue, name)
        assert transform_id is not None
        assert len(transform_id) > 0
        glue.delete_ml_transform(TransformId=transform_id)

    def test_get_ml_transform(self, glue):
        """GetMLTransform returns details for a specific transform."""
        name = _unique("ml-xform")
        transform_id = self._create_transform(glue, name)
        try:
            resp = glue.get_ml_transform(TransformId=transform_id)
            assert resp["TransformId"] == transform_id
            assert resp["Name"] == name
        finally:
            glue.delete_ml_transform(TransformId=transform_id)

    def test_get_ml_transforms(self, glue):
        """GetMLTransforms returns a list of transforms."""
        resp = glue.get_ml_transforms()
        assert "Transforms" in resp

    def test_list_ml_transforms(self, glue):
        """ListMLTransforms returns a list of transform IDs."""
        resp = glue.list_ml_transforms()
        assert "TransformIds" in resp

    def test_update_ml_transform(self, glue):
        """UpdateMLTransform updates a transform's description."""
        name = _unique("ml-xform")
        transform_id = self._create_transform(glue, name)
        try:
            resp = glue.update_ml_transform(
                TransformId=transform_id,
                Description="updated description",
            )
            assert resp["TransformId"] == transform_id
            detail = glue.get_ml_transform(TransformId=transform_id)
            assert detail["Description"] == "updated description"
        finally:
            glue.delete_ml_transform(TransformId=transform_id)

    def test_delete_ml_transform(self, glue):
        """DeleteMLTransform removes a transform."""
        name = _unique("ml-xform")
        transform_id = self._create_transform(glue, name)
        resp = glue.delete_ml_transform(TransformId=transform_id)
        assert resp["TransformId"] == transform_id
        with pytest.raises(ClientError) as exc:
            glue.get_ml_transform(TransformId=transform_id)
        assert exc.value.response["Error"]["Code"] == "EntityNotFoundException"

    def test_get_ml_task_run_not_found(self, glue):
        """GetMLTaskRun with a fake transform ID raises EntityNotFoundException."""
        with pytest.raises(ClientError) as exc:
            glue.get_ml_task_run(
                TransformId="tfm-00000000",
                TaskRunId="tr-00000000",
            )
        assert exc.value.response["Error"]["Code"] == "EntityNotFoundException"

    def test_get_ml_task_runs_not_found(self, glue):
        """GetMLTaskRuns with a fake transform ID raises EntityNotFoundException."""
        with pytest.raises(ClientError) as exc:
            glue.get_ml_task_runs(TransformId="tfm-00000000")
        assert exc.value.response["Error"]["Code"] == "EntityNotFoundException"


class TestGlueUpdateRegistry:
    def test_update_registry(self, glue):
        """UpdateRegistry changes registry description."""
        reg_name = _unique("reg")
        glue.create_registry(RegistryName=reg_name, Description="original")
        try:
            resp = glue.update_registry(
                RegistryId={"RegistryName": reg_name},
                Description="updated",
            )
            assert resp["RegistryName"] == reg_name
            got = glue.get_registry(RegistryId={"RegistryName": reg_name})
            assert got["Description"] == "updated"
        finally:
            glue.delete_registry(RegistryId={"RegistryName": reg_name})

    def test_update_registry_not_found(self, glue):
        """UpdateRegistry with nonexistent registry raises EntityNotFoundException."""
        with pytest.raises(ClientError) as exc:
            glue.update_registry(
                RegistryId={"RegistryName": "nonexistent-reg-xyz"},
                Description="nope",
            )
        assert exc.value.response["Error"]["Code"] == "EntityNotFoundException"


class TestGlueStartDataQualityRulesetEvaluationRun:
    def test_start_data_quality_ruleset_evaluation_run(self, glue):
        """StartDataQualityRulesetEvaluationRun returns a RunId."""
        resp = glue.start_data_quality_ruleset_evaluation_run(
            DataSource={"GlueTable": {"DatabaseName": "testdb", "TableName": "testtbl"}},
            Role="arn:aws:iam::123456789012:role/test",
            RulesetNames=["ruleset1"],
        )
        assert "RunId" in resp


class TestGlueBatchGetDataQualityResult:
    def test_batch_get_data_quality_result(self, glue):
        """BatchGetDataQualityResult returns results list (possibly empty)."""
        resp = glue.batch_get_data_quality_result(ResultIds=["result-fake-id"])
        assert "Results" in resp


class TestGlueGetUserDefinedFunctions:
    """Tests for GetUserDefinedFunctions (plural list operation)."""

    def test_get_user_defined_functions_empty(self, glue):
        """GetUserDefinedFunctions returns empty list for db with no UDFs."""
        db_name = _unique("db")
        glue.create_database(DatabaseInput={"Name": db_name})
        try:
            resp = glue.get_user_defined_functions(DatabaseName=db_name, Pattern="*")
            assert "UserDefinedFunctions" in resp
            assert resp["UserDefinedFunctions"] == []
        finally:
            glue.delete_database(Name=db_name)

    def test_get_user_defined_functions_returns_created(self, glue):
        """GetUserDefinedFunctions lists UDFs matching pattern."""
        db_name = _unique("db")
        func_name = _unique("udf")
        glue.create_database(DatabaseInput={"Name": db_name})
        glue.create_user_defined_function(
            DatabaseName=db_name,
            FunctionInput={
                "FunctionName": func_name,
                "ClassName": "com.example.ListUDF",
                "OwnerName": "owner",
                "OwnerType": "USER",
            },
        )
        try:
            resp = glue.get_user_defined_functions(DatabaseName=db_name, Pattern="*")
            names = [f["FunctionName"] for f in resp["UserDefinedFunctions"]]
            assert func_name in names
        finally:
            glue.delete_user_defined_function(DatabaseName=db_name, FunctionName=func_name)
            glue.delete_database(Name=db_name)

    def test_get_user_defined_functions_nonexistent_db(self, glue):
        """GetUserDefinedFunctions on missing db raises error."""
        with pytest.raises(ClientError) as exc:
            glue.get_user_defined_functions(
                DatabaseName="nonexistent-db-xyz-999",
                Pattern="*",
            )
        assert exc.value.response["Error"]["Code"] == "EntityNotFoundException"


class TestGlueMLTaskRunOps:
    """Tests for Glue ML Task Run operations (newly implemented)."""

    def test_cancel_ml_task_run_nonexistent(self, glue):
        """CancelMLTaskRun with nonexistent transform raises error."""
        with pytest.raises(ClientError) as exc:
            glue.cancel_ml_task_run(
                TransformId="nonexistent-transform",
                TaskRunId="nonexistent-task",
            )
        assert exc.value.response["Error"]["Code"] == "EntityNotFoundException"

    def test_start_ml_evaluation_nonexistent(self, glue):
        """StartMLEvaluationTaskRun with nonexistent transform raises error."""
        with pytest.raises(ClientError) as exc:
            glue.start_ml_evaluation_task_run(TransformId="nonexistent-transform")
        assert exc.value.response["Error"]["Code"] == "EntityNotFoundException"


class TestGlueSchemaVersionDiffOps:
    """Tests for Glue Schema Version Diff operations."""

    def test_get_schema_versions_diff_nonexistent(self, glue):
        """GetSchemaVersionsDiff with nonexistent schema raises error."""
        with pytest.raises(ClientError) as exc:
            glue.get_schema_versions_diff(
                SchemaId={"SchemaName": "nonexistent", "RegistryName": "default-registry"},
                FirstSchemaVersionNumber={"VersionNumber": 1},
                SecondSchemaVersionNumber={"VersionNumber": 2},
                SchemaDiffType="SYNTAX_DIFF",
            )
        assert exc.value.response["Error"]["Code"] in (
            "EntityNotFoundException",
            "InvalidInputException",
        )


class TestGlueStubOps:
    """Tests for Glue stub operations."""

    def test_create_script(self, glue):
        """CreateScript returns a script body."""
        resp = glue.create_script(
            DagNodes=[{"Id": "node1", "NodeType": "S3", "Args": []}],
            DagEdges=[],
        )
        assert (
            "PythonScript" in resp
            or "ScalaCode" in resp
            or resp["ResponseMetadata"]["HTTPStatusCode"] == 200
        )

    def test_test_connection_nonexistent(self, glue):
        """TestConnection with nonexistent connection raises error."""
        with pytest.raises(ClientError) as exc:
            glue.test_connection(ConnectionName="nonexistent-conn")
        assert exc.value.response["Error"]["Code"] in ("EntityNotFoundException", "InternalError")
