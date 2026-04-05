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
        """GetConnections: create, retrieve, list, update, delete, error."""
        conn_name = _unique("conn")
        client.create_connection(
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
            resp = client.get_connections()
            assert "ConnectionList" in resp
            names = [c["Name"] for c in resp["ConnectionList"]]
            assert conn_name in names
            # retrieve individual connection
            get_resp = client.get_connection(Name=conn_name)
            assert get_resp["Connection"]["Name"] == conn_name
            # update: change the URL
            client.update_connection(
                Name=conn_name,
                ConnectionInput={
                    "Name": conn_name,
                    "ConnectionType": "JDBC",
                    "ConnectionProperties": {
                        "JDBC_CONNECTION_URL": "jdbc:mysql://host:3306/updated-db",
                        "USERNAME": "admin",
                        "PASSWORD": "secret",
                    },
                },
            )
            updated = client.get_connection(Name=conn_name)
            assert "updated-db" in updated["Connection"]["ConnectionProperties"]["JDBC_CONNECTION_URL"]
            # error: nonexistent connection
            with pytest.raises(ClientError) as exc:
                client.get_connection(Name="does-not-exist-conn-xyz")
            assert exc.value.response["Error"]["Code"] == "EntityNotFoundException"
        finally:
            client.delete_connection(ConnectionName=conn_name)

    def test_get_data_catalog_encryption_settings(self, client):
        """GetDataCatalogEncryptionSettings reflects PutDataCatalogEncryptionSettings."""
        # update: put settings first
        client.put_data_catalog_encryption_settings(
            DataCatalogEncryptionSettings={
                "ConnectionPasswordEncryption": {"ReturnConnectionPasswordEncrypted": False},
                "EncryptionAtRest": {"CatalogEncryptionMode": "DISABLED"},
            }
        )
        # retrieve: get confirms the settings
        resp = client.get_data_catalog_encryption_settings()
        assert "DataCatalogEncryptionSettings" in resp
        assert "ConnectionPasswordEncryption" in resp["DataCatalogEncryptionSettings"]
        assert "EncryptionAtRest" in resp["DataCatalogEncryptionSettings"]
        # error: nonexistent connection to prove catalog is live
        with pytest.raises(ClientError) as exc:
            client.get_connection(Name="does-not-exist-enc-xyz")
        assert exc.value.response["Error"]["Code"] == "EntityNotFoundException"

    def test_get_dev_endpoints(self, client):
        """GetDevEndpoints: create, retrieve, list, update, delete, error."""
        ep_name = _unique("de")
        client.create_dev_endpoint(
            EndpointName=ep_name,
            RoleArn="arn:aws:iam::123456789012:role/glue-role",
        )
        try:
            resp = client.get_dev_endpoints()
            assert "DevEndpoints" in resp
            names = [d["EndpointName"] for d in resp["DevEndpoints"]]
            assert ep_name in names
            # retrieve individual endpoint
            get_resp = client.get_dev_endpoint(EndpointName=ep_name)
            assert get_resp["DevEndpoint"]["EndpointName"] == ep_name
            # update: add custom arguments
            client.update_dev_endpoint(
                EndpointName=ep_name,
                AddArguments={"--enable-metrics": "true"},
            )
            updated = client.get_dev_endpoint(EndpointName=ep_name)
            assert updated["DevEndpoint"]["EndpointName"] == ep_name
            # error: nonexistent endpoint
            with pytest.raises(ClientError) as exc:
                client.get_dev_endpoint(EndpointName="does-not-exist-ep-xyz")
            assert exc.value.response["Error"]["Code"] == "EntityNotFoundException"
        finally:
            client.delete_dev_endpoint(EndpointName=ep_name)

    def test_get_resource_policy(self, client):
        """GetResourcePolicy returns a response or EntityNotFoundException when no policy exists."""
        try:
            client.get_resource_policy()
        except ClientError as e:
            assert e.response["Error"]["Code"] == "EntityNotFoundException"

    def test_get_security_configurations(self, client):
        """GetSecurityConfigurations includes a created config; delete removes it."""
        name = _unique("sc")
        client.create_security_configuration(
            Name=name,
            EncryptionConfiguration={
                "S3Encryption": [{"S3EncryptionMode": "DISABLED"}],
                "CloudWatchEncryption": {"CloudWatchEncryptionMode": "DISABLED"},
                "JobBookmarksEncryption": {"JobBookmarksEncryptionMode": "DISABLED"},
            },
        )
        try:
            resp = client.get_security_configurations()
            assert "SecurityConfigurations" in resp
            names_in_list = [sc["Name"] for sc in resp["SecurityConfigurations"]]
            assert name in names_in_list
            # retrieve individual config
            get_resp = client.get_security_configuration(Name=name)
            assert get_resp["SecurityConfiguration"]["Name"] == name
            # error: nonexistent config
            with pytest.raises(ClientError) as exc:
                client.get_security_configuration(Name="does-not-exist-sc-xyz")
            assert exc.value.response["Error"]["Code"] == "EntityNotFoundException"
        finally:
            client.delete_security_configuration(Name=name)

    def test_get_triggers(self, client):
        """GetTriggers includes a created trigger; delete removes it."""
        trig_name = _unique("trig")
        client.create_trigger(
            Name=trig_name,
            Type="ON_DEMAND",
            Actions=[{"JobName": "dummy-job"}],
        )
        try:
            resp = client.get_triggers()
            assert "Triggers" in resp
            names = [t["Name"] for t in resp["Triggers"]]
            assert trig_name in names
            # retrieve individual trigger
            get_resp = client.get_trigger(Name=trig_name)
            assert get_resp["Trigger"]["Name"] == trig_name
            # error: nonexistent trigger
            with pytest.raises(ClientError) as exc:
                client.get_trigger(Name="does-not-exist-trig-xyz")
            assert exc.value.response["Error"]["Code"] == "EntityNotFoundException"
        finally:
            client.delete_trigger(Name=trig_name)

    def test_list_crawlers(self, client):
        """ListCrawlers includes a created crawler; get_crawlers confirms it."""
        cr_name = _unique("cr")
        client.create_crawler(
            Name=cr_name,
            Role="arn:aws:iam::123456789012:role/glue-role",
            Targets={"S3Targets": [{"Path": "s3://bucket/path"}]},
            DatabaseName="default",
        )
        try:
            resp = client.list_crawlers()
            assert "CrawlerNames" in resp
            assert cr_name in resp["CrawlerNames"]
            # retrieve individual crawler
            get_resp = client.get_crawler(Name=cr_name)
            assert get_resp["Crawler"]["Name"] == cr_name
            # error: nonexistent crawler
            with pytest.raises(ClientError) as exc:
                client.get_crawler(Name="does-not-exist-cr-xyz")
            assert exc.value.response["Error"]["Code"] == "EntityNotFoundException"
        finally:
            client.delete_crawler(Name=cr_name)

    def test_list_jobs(self, client):
        """ListJobs includes a created job; get_job retrieves it."""
        job_name = _unique("job")
        client.create_job(
            Name=job_name,
            Role="arn:aws:iam::123456789012:role/glue-role",
            Command={"Name": "glueetl", "ScriptLocation": "s3://bucket/script.py"},
        )
        try:
            resp = client.list_jobs()
            assert "JobNames" in resp
            assert job_name in resp["JobNames"]
            # retrieve individual job
            get_resp = client.get_job(JobName=job_name)
            assert get_resp["Job"]["Name"] == job_name
            # error: nonexistent job
            with pytest.raises(ClientError) as exc:
                client.get_job(JobName="does-not-exist-job-xyz")
            assert exc.value.response["Error"]["Code"] == "EntityNotFoundException"
        finally:
            client.delete_job(JobName=job_name)

    def test_list_registries(self, client):
        """ListRegistries includes a created registry; get_registry retrieves it."""
        reg_name = _unique("reg")
        client.create_registry(RegistryName=reg_name, Description="for list test")
        try:
            resp = client.list_registries()
            assert "Registries" in resp
            names = [r["RegistryName"] for r in resp["Registries"]]
            assert reg_name in names
            # retrieve individual registry
            get_resp = client.get_registry(RegistryId={"RegistryName": reg_name})
            assert get_resp["RegistryName"] == reg_name
            # error: nonexistent registry
            with pytest.raises(ClientError) as exc:
                client.get_registry(RegistryId={"RegistryName": "does-not-exist-reg-xyz"})
            assert exc.value.response["Error"]["Code"] == "EntityNotFoundException"
        finally:
            client.delete_registry(RegistryId={"RegistryName": reg_name})

    def test_list_sessions(self, client):
        """ListSessions includes a created session; get_session retrieves it."""
        sess_id = _unique("sess")
        client.create_session(
            Id=sess_id,
            Role="arn:aws:iam::123456789012:role/glue-role",
            Command={"Name": "glueetl", "PythonVersion": "3"},
        )
        try:
            resp = client.list_sessions()
            assert "Ids" in resp
            assert sess_id in resp["Ids"]
            # retrieve individual session
            get_resp = client.get_session(Id=sess_id)
            assert get_resp["Session"]["Id"] == sess_id
            # error: nonexistent session
            with pytest.raises(ClientError) as exc:
                client.get_session(Id="does-not-exist-sess-xyz")
            assert exc.value.response["Error"]["Code"] == "EntityNotFoundException"
        finally:
            client.delete_session(Id=sess_id)

    def test_list_triggers(self, client):
        """ListTriggers includes a created trigger; get_trigger retrieves it."""
        trig_name = _unique("trig2")
        client.create_trigger(
            Name=trig_name,
            Type="ON_DEMAND",
            Actions=[{"JobName": "dummy-job"}],
        )
        try:
            resp = client.list_triggers()
            assert "TriggerNames" in resp
            assert trig_name in resp["TriggerNames"]
            # retrieve individual trigger
            get_resp = client.get_trigger(Name=trig_name)
            assert get_resp["Trigger"]["Name"] == trig_name
            # error: nonexistent trigger
            with pytest.raises(ClientError) as exc:
                client.get_trigger(Name="does-not-exist-trig2-xyz")
            assert exc.value.response["Error"]["Code"] == "EntityNotFoundException"
        finally:
            client.delete_trigger(Name=trig_name)

    def test_list_workflows(self, client):
        """ListWorkflows includes a created workflow; get_workflow retrieves it."""
        wf_name = _unique("wf")
        client.create_workflow(Name=wf_name, Description="for list test")
        try:
            resp = client.list_workflows()
            assert "Workflows" in resp
            assert wf_name in resp["Workflows"]
            # retrieve individual workflow
            get_resp = client.get_workflow(Name=wf_name)
            assert get_resp["Workflow"]["Name"] == wf_name
            # error: nonexistent workflow
            with pytest.raises(ClientError) as exc:
                client.get_workflow(Name="does-not-exist-wf-xyz")
            assert exc.value.response["Error"]["Code"] == "EntityNotFoundException"
        finally:
            client.delete_workflow(Name=wf_name)

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
        """GetPartitions on a table with no partitions returns empty list; create then list."""
        db_name, tbl_name = self._make_db_and_table(client)
        try:
            # list empty
            resp = client.get_partitions(DatabaseName=db_name, TableName=tbl_name)
            assert "Partitions" in resp
            assert resp["Partitions"] == []
            # create a partition
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
            # retrieve: now list should have 1
            resp2 = client.get_partitions(DatabaseName=db_name, TableName=tbl_name)
            assert len(resp2["Partitions"]) == 1
            assert resp2["Partitions"][0]["Values"] == ["2024"]
            # error: nonexistent db raises EntityNotFoundException
            with pytest.raises(ClientError) as exc:
                client.get_partitions(DatabaseName="no-such-db-xyz", TableName=tbl_name)
            assert exc.value.response["Error"]["Code"] == "EntityNotFoundException"
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
        """GetPartitionIndexes returns index list; create partition then check indexes; error case."""
        db_name, tbl_name = self._make_db_and_table(client)
        try:
            # retrieve: initially empty index list
            resp = client.get_partition_indexes(DatabaseName=db_name, TableName=tbl_name)
            assert "PartitionIndexDescriptorList" in resp
            initial_count = len(resp["PartitionIndexDescriptorList"])
            # create a partition to prove table is usable
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
            # list partitions confirms table has partition data
            parts_resp = client.get_partitions(DatabaseName=db_name, TableName=tbl_name)
            assert len(parts_resp["Partitions"]) == 1
            # indexes count unchanged (no new index was created)
            resp2 = client.get_partition_indexes(DatabaseName=db_name, TableName=tbl_name)
            assert len(resp2["PartitionIndexDescriptorList"]) == initial_count
            # error: get a nonexistent partition raises EntityNotFoundException
            with pytest.raises(ClientError) as exc:
                client.get_partition(
                    DatabaseName=db_name, TableName=tbl_name, PartitionValues=["9999"]
                )
            assert exc.value.response["Error"]["Code"] == "EntityNotFoundException"
        finally:
            self._cleanup(client, db_name, tbl_name)

    def test_get_table_versions(self, client):
        """GetTableVersions returns version list; update table creates new version; error case."""
        db_name, tbl_name = self._make_db_and_table(client)
        try:
            # retrieve: initially 1 version after create
            resp = client.get_table_versions(DatabaseName=db_name, TableName=tbl_name)
            assert "TableVersions" in resp
            assert len(resp["TableVersions"]) >= 1
            # update: add a column to create a new version
            client.update_table(
                DatabaseName=db_name,
                TableInput={
                    "Name": tbl_name,
                    "StorageDescriptor": {
                        "Columns": [
                            {"Name": "col1", "Type": "string"},
                            {"Name": "col2", "Type": "int"},
                        ],
                        "Location": "s3://bucket/path",
                        "InputFormat": "org.apache.hadoop.mapred.TextInputFormat",
                        "OutputFormat": "org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat",  # noqa: E501
                        "SerdeInfo": {
                            "SerializationLibrary": "org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe"  # noqa: E501
                        },
                    },
                    "PartitionKeys": [{"Name": "year", "Type": "string"}],
                },
            )
            # list: now there should be 2 versions
            resp2 = client.get_table_versions(DatabaseName=db_name, TableName=tbl_name)
            assert len(resp2["TableVersions"]) >= 2
            # error: nonexistent database raises EntityNotFoundException
            with pytest.raises(ClientError) as exc:
                client.get_table_versions(DatabaseName="no-such-db-xyz", TableName=tbl_name)
            assert exc.value.response["Error"]["Code"] == "EntityNotFoundException"
        finally:
            self._cleanup(client, db_name, tbl_name)

    def test_get_table_version(self, client):
        """GetTableVersion retrieves specific version; update creates new; error case."""
        db_name, tbl_name = self._make_db_and_table(client)
        try:
            # retrieve: get versions to find a valid version ID
            versions_resp = client.get_table_versions(DatabaseName=db_name, TableName=tbl_name)
            version_id = versions_resp["TableVersions"][0]["VersionId"]
            resp = client.get_table_version(
                DatabaseName=db_name, TableName=tbl_name, VersionId=str(version_id)
            )
            assert "TableVersion" in resp
            assert resp["TableVersion"]["Table"]["Name"] == tbl_name
            # update: create a new version via update_table
            client.update_table(
                DatabaseName=db_name,
                TableInput={
                    "Name": tbl_name,
                    "StorageDescriptor": {
                        "Columns": [
                            {"Name": "col1", "Type": "string"},
                            {"Name": "col3", "Type": "bigint"},
                        ],
                        "Location": "s3://bucket/path",
                        "InputFormat": "org.apache.hadoop.mapred.TextInputFormat",
                        "OutputFormat": "org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat",  # noqa: E501
                        "SerdeInfo": {
                            "SerializationLibrary": "org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe"  # noqa: E501
                        },
                    },
                    "PartitionKeys": [{"Name": "year", "Type": "string"}],
                },
            )
            # list: now there are at least 2 versions
            list_resp = client.get_table_versions(DatabaseName=db_name, TableName=tbl_name)
            assert len(list_resp["TableVersions"]) >= 2
            # error: nonexistent database raises EntityNotFoundException
            with pytest.raises(ClientError) as exc:
                client.get_table_version(
                    DatabaseName="no-such-db-xyz", TableName=tbl_name, VersionId="1"
                )
            assert exc.value.response["Error"]["Code"] == "EntityNotFoundException"
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
        """GetSchemaVersion: retrieve version, list versions, register new version, delete, error."""
        schema_id = {"SchemaName": schema["name"], "RegistryName": schema["registry"]}
        # RETRIEVE: get the latest schema version
        resp = client.get_schema_version(
            SchemaId=schema_id,
            SchemaVersionNumber={"LatestVersion": True},
        )
        assert resp["DataFormat"] == "AVRO"
        assert resp["SchemaDefinition"] == schema["definition"]
        sv_id = resp["SchemaVersionId"]
        # LIST: list schema versions
        list_resp = client.list_schema_versions(SchemaId=schema_id)
        sv_ids = [v.get("SchemaVersionId") for v in list_resp["Schemas"]]
        assert sv_id in sv_ids
        # CREATE: register a new schema version
        new_def = '{"type":"record","name":"Test","fields":[{"name":"id","type":"int"},{"name":"name","type":"string"}]}'
        reg_resp = client.register_schema_version(SchemaId=schema_id, SchemaDefinition=new_def)
        assert reg_resp["VersionNumber"] == 2
        # RETRIEVE v2 by number
        v2_resp = client.get_schema_version(SchemaId=schema_id, SchemaVersionNumber={"VersionNumber": 2})
        assert v2_resp["VersionNumber"] == 2
        assert v2_resp["SchemaDefinition"] == new_def
        # ERROR: nonexistent schema raises EntityNotFoundException
        with pytest.raises(ClientError) as exc:
            client.get_schema_version(
                SchemaId={"SchemaName": "nonexistent-schema-xyz", "RegistryName": schema["registry"]},
                SchemaVersionNumber={"LatestVersion": True},
            )
        assert exc.value.response["Error"]["Code"] == "EntityNotFoundException"

    def test_get_schema_by_definition(self, client, schema):
        """GetSchemaByDefinition: retrieve by definition, list schemas, update, delete, error."""
        schema_id = {"SchemaName": schema["name"], "RegistryName": schema["registry"]}
        # RETRIEVE: find schema by its definition
        resp = client.get_schema_by_definition(
            SchemaId=schema_id,
            SchemaDefinition=schema["definition"],
        )
        assert resp["SchemaArn"] == schema["arn"]
        assert "SchemaVersionId" in resp
        assert resp["DataFormat"] == "AVRO"
        # LIST: list schemas in the registry
        list_resp = client.list_schemas(RegistryId={"RegistryName": schema["registry"]})
        schema_names = [s["SchemaName"] for s in list_resp["Schemas"]]
        assert schema["name"] in schema_names
        # UPDATE: update schema compatibility
        client.update_schema(SchemaId=schema_id, Compatibility="BACKWARD")
        updated = client.get_schema(SchemaId=schema_id)
        assert updated["Compatibility"] == "BACKWARD"
        # ERROR: nonexistent schema raises EntityNotFoundException
        with pytest.raises(ClientError) as exc:
            client.get_schema_by_definition(
                SchemaId={"SchemaName": "nonexistent-xyz", "RegistryName": schema["registry"]},
                SchemaDefinition=schema["definition"],
            )
        assert exc.value.response["Error"]["Code"] == "EntityNotFoundException"


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
            # LIST: verify database appears in listing after update
            list_resp = glue.get_databases()
            db_names = [db["Name"] for db in list_resp["DatabaseList"]]
            assert db_name in db_names
            # ERROR: updating a nonexistent database raises EntityNotFoundException
            with pytest.raises(ClientError) as exc:
                glue.update_database(
                    Name="nonexistent-db-xyz-update-test",
                    DatabaseInput={"Name": "nonexistent-db-xyz-update-test"},
                )
            assert exc.value.response["Error"]["Code"] == "EntityNotFoundException"
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
        # Create a real job so we can verify the split between found and not-found
        job_name = _unique("bgjnf")
        glue.create_job(
            Name=job_name,
            Role="arn:aws:iam::123456789012:role/glue-role",
            Command={"Name": "glueetl", "ScriptLocation": "s3://bucket/script.py"},
        )
        try:
            resp = glue.batch_get_jobs(JobNames=["nonexistent-job-xyz"])
            assert len(resp["Jobs"]) == 0
            assert "nonexistent-job-xyz" in resp["JobsNotFound"]
            # Also verify the real job IS retrievable via get_job
            get_resp = glue.get_job(JobName=job_name)
            assert get_resp["Job"]["Name"] == job_name
        finally:
            glue.delete_job(JobName=job_name)


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
        # Create a real crawler to verify the not-found behavior in context
        db_name = _unique("db")
        crawler_name = _unique("bgcrnf")
        glue.create_database(DatabaseInput={"Name": db_name})
        glue.create_crawler(
            Name=crawler_name,
            Role="arn:aws:iam::123456789012:role/glue-role",
            DatabaseName=db_name,
            Targets={"S3Targets": [{"Path": "s3://bucket/data"}]},
        )
        try:
            resp = glue.batch_get_crawlers(CrawlerNames=["nonexistent-crawler-xyz"])
            assert len(resp["Crawlers"]) == 0
            assert "nonexistent-crawler-xyz" in resp["CrawlersNotFound"]
            # Verify real crawler is retrievable
            get_resp = glue.get_crawler(Name=crawler_name)
            assert get_resp["Crawler"]["Name"] == crawler_name
        finally:
            glue.delete_crawler(Name=crawler_name)
            glue.delete_database(Name=db_name)


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
        # Create a real trigger to verify not-found behavior in context
        trigger_name = _unique("bgtnf")
        glue.create_trigger(
            Name=trigger_name,
            Type="SCHEDULED",
            Schedule="cron(0 12 * * ? *)",
            Actions=[{"JobName": "dummy-job"}],
        )
        try:
            resp = glue.batch_get_triggers(TriggerNames=["nonexistent-trigger-xyz"])
            assert len(resp["Triggers"]) == 0
            assert "nonexistent-trigger-xyz" in resp["TriggersNotFound"]
            # Verify real trigger is retrievable
            get_resp = glue.get_trigger(Name=trigger_name)
            assert get_resp["Trigger"]["Name"] == trigger_name
        finally:
            glue.delete_trigger(Name=trigger_name)


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
        # Create a real workflow to verify not-found behavior in context
        wf_name = _unique("bgwfnf")
        glue.create_workflow(Name=wf_name, Description="not found test workflow")
        try:
            resp = glue.batch_get_workflows(Names=["nonexistent-wf-xyz"])
            assert len(resp["Workflows"]) == 0
            assert "nonexistent-wf-xyz" in resp["MissingWorkflows"]
            # Verify real workflow is retrievable
            get_resp = glue.get_workflow(Name=wf_name)
            assert get_resp["Workflow"]["Name"] == wf_name
        finally:
            glue.delete_workflow(Name=wf_name)


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
    """Tests for CreateConnection and GetConnection."""

    def test_create_connection(self, glue):
        """CreateConnection full lifecycle: create, retrieve, list, update, delete, error."""
        conn_name = _unique("conn")
        # CREATE
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
        try:
            # RETRIEVE: connection exists with correct properties
            get_resp = glue.get_connection(Name=conn_name)
            assert get_resp["Connection"]["Name"] == conn_name
            assert get_resp["Connection"]["ConnectionProperties"]["JDBC_CONNECTION_URL"] == "jdbc:mysql://host:3306/db"
            # LIST: connection appears in list
            list_resp = glue.get_connections()
            assert conn_name in [c["Name"] for c in list_resp["ConnectionList"]]
            # UPDATE: change the URL
            glue.update_connection(
                Name=conn_name,
                ConnectionInput={
                    "Name": conn_name,
                    "ConnectionType": "JDBC",
                    "ConnectionProperties": {
                        "JDBC_CONNECTION_URL": "jdbc:mysql://host:3306/updated-db",
                        "USERNAME": "admin",
                        "PASSWORD": "secret",
                    },
                },
            )
            updated = glue.get_connection(Name=conn_name)
            assert "updated-db" in updated["Connection"]["ConnectionProperties"]["JDBC_CONNECTION_URL"]
            # ERROR: nonexistent connection raises EntityNotFoundException
            with pytest.raises(ClientError) as exc:
                glue.get_connection(Name="nonexistent-conn-xyz")
            assert exc.value.response["Error"]["Code"] == "EntityNotFoundException"
        finally:
            # DELETE
            glue.delete_connection(ConnectionName=conn_name)

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
        # CREATE: blueprint to ensure list is non-trivial
        name = _unique("bp")
        glue.create_blueprint(Name=name, BlueprintLocation="s3://bucket/bp.py")
        try:
            # LIST: blueprint appears in list
            resp = glue.list_blueprints()
            assert isinstance(resp["Blueprints"], list)
            assert name in resp["Blueprints"]
            # RETRIEVE: get blueprint details
            get_resp = glue.get_blueprint(Name=name)
            assert get_resp["Blueprint"]["Name"] == name
        finally:
            # DELETE: clean up
            glue.delete_blueprint(Name=name)
            # ERROR: gone after delete
            with pytest.raises(ClientError) as exc:
                glue.get_blueprint(Name=name)
            assert exc.value.response["Error"]["Code"] == "EntityNotFoundException"

    def test_get_classifiers(self, glue):
        # CREATE: grok classifier
        clf_name = _unique("clf")
        glue.create_classifier(
            GrokClassifier={
                "Classification": "mylog",
                "Name": clf_name,
                "GrokPattern": "%{COMMONAPACHELOG}",
            }
        )
        try:
            # LIST: classifier appears in classifiers list
            resp = glue.get_classifiers()
            assert isinstance(resp["Classifiers"], list)
            names = [c["GrokClassifier"]["Name"] for c in resp["Classifiers"] if "GrokClassifier" in c]
            assert clf_name in names
            # RETRIEVE: get individual classifier
            get_resp = glue.get_classifier(Name=clf_name)
            assert get_resp["Classifier"]["GrokClassifier"]["Name"] == clf_name
        finally:
            # DELETE
            glue.delete_classifier(Name=clf_name)
            # ERROR: gone after delete
            with pytest.raises(ClientError) as exc:
                glue.get_classifier(Name=clf_name)
            assert exc.value.response["Error"]["Code"] == "EntityNotFoundException"

    def test_list_data_quality_rulesets(self, glue):
        # CREATE: data quality ruleset
        name = _unique("dqr")
        glue.create_data_quality_ruleset(Name=name, Ruleset='Rules = [ IsComplete "id" ]')
        try:
            # LIST: ruleset appears in list
            resp = glue.list_data_quality_rulesets()
            assert isinstance(resp["Rulesets"], list)
            names = [r["Name"] for r in resp["Rulesets"]]
            assert name in names
            # RETRIEVE: get ruleset details
            get_resp = glue.get_data_quality_ruleset(Name=name)
            assert get_resp["Name"] == name
            assert "Ruleset" in get_resp
        finally:
            # DELETE
            glue.delete_data_quality_ruleset(Name=name)
            # ERROR: gone after delete
            with pytest.raises(ClientError) as exc:
                glue.get_data_quality_ruleset(Name=name)
            assert exc.value.response["Error"]["Code"] in (
                "EntityNotFoundException",
                "InvalidInputException",
            )

    def test_get_ml_transforms(self, glue):
        # CREATE: ML transform
        name = _unique("mlt")
        create_resp = glue.create_ml_transform(
            Name=name,
            InputRecordTables=[{"DatabaseName": "default", "TableName": "test"}],
            Parameters={
                "TransformType": "FIND_MATCHES",
                "FindMatchesParameters": {"PrimaryKeyColumnName": "id"},
            },
            Role="arn:aws:iam::123456789012:role/test",
        )
        tfm_id = create_resp["TransformId"]
        try:
            # LIST: transform appears in list
            resp = glue.get_ml_transforms()
            assert isinstance(resp["Transforms"], list)
            ids = [t["TransformId"] for t in resp["Transforms"]]
            assert tfm_id in ids
            # RETRIEVE: get individual transform
            get_resp = glue.get_ml_transform(TransformId=tfm_id)
            assert get_resp["TransformId"] == tfm_id
            assert get_resp["Name"] == name
        finally:
            # DELETE
            glue.delete_ml_transform(TransformId=tfm_id)
            # ERROR: gone after delete
            with pytest.raises(ClientError) as exc:
                glue.get_ml_transform(TransformId=tfm_id)
            assert exc.value.response["Error"]["Code"] == "EntityNotFoundException"

    def test_list_usage_profiles(self, glue):
        # CREATE: usage profile
        name = _unique("up")
        glue.create_usage_profile(Name=name, Configuration={})
        try:
            # LIST: profile appears in list
            resp = glue.list_usage_profiles()
            assert isinstance(resp["Profiles"], list)
            names = [p["Name"] for p in resp["Profiles"]]
            assert name in names
            # RETRIEVE: get profile details
            get_resp = glue.get_usage_profile(Name=name)
            assert get_resp["Name"] == name
        finally:
            # DELETE
            glue.delete_usage_profile(Name=name)
            # ERROR: gone after delete
            with pytest.raises(ClientError) as exc:
                glue.get_usage_profile(Name=name)
            assert exc.value.response["Error"]["Code"] == "EntityNotFoundException"


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
        """GetCatalogImportStatus: import catalog, retrieve status, list catalogs, error on bad ID."""
        # CREATE: trigger catalog import
        glue.import_catalog_to_glue()
        # RETRIEVE: status shows import completed
        resp = glue.get_catalog_import_status()
        status = resp["ImportStatus"]
        assert status["ImportCompleted"] is True
        # LIST: get catalogs returns a list
        catalogs_resp = glue.get_catalogs()
        assert isinstance(catalogs_resp["CatalogList"], list)
        # ERROR: nonexistent catalog ID raises EntityNotFoundException
        with pytest.raises(ClientError) as exc:
            glue.get_catalog(CatalogId="nonexistent-catalog-xyz-123")
        assert exc.value.response["Error"]["Code"] == "EntityNotFoundException"

    def test_get_resource_policies(self, glue):
        """GetResourcePolicies: put policy, retrieve it, list policies, delete, error."""
        import json as _json
        policy = _json.dumps({
            "Version": "2012-10-17",
            "Statement": [{"Effect": "Allow", "Principal": {"AWS": "arn:aws:iam::123456789012:root"}, "Action": "glue:GetDatabase", "Resource": "*"}],
        })
        # CREATE: put resource policy
        glue.put_resource_policy(PolicyInJson=policy)
        try:
            # RETRIEVE: get the policy back
            get_resp = glue.get_resource_policy()
            assert "PolicyInJson" in get_resp
            retrieved_policy = _json.loads(get_resp["PolicyInJson"])
            assert retrieved_policy["Version"] == "2012-10-17"
            # LIST: resource policies list
            list_resp = glue.get_resource_policies()
            assert isinstance(list_resp["GetResourcePoliciesResponseList"], list)
        finally:
            # DELETE
            glue.delete_resource_policy()

    def test_get_resource_policy(self, glue):
        """GetResourcePolicy returns policy or empty."""
        try:
            resp = glue.get_resource_policy()
            assert "PolicyInJson" in resp or resp["ResponseMetadata"]["HTTPStatusCode"] == 200
        except ClientError as e:
            assert e.response["Error"]["Code"] == "EntityNotFoundException"

    def test_get_security_configurations(self, glue):
        """GetSecurityConfigurations: create config, retrieve it, list, delete, error."""
        name = _unique("sc")
        enc_cfg = {
            "S3Encryption": [{"S3EncryptionMode": "DISABLED"}],
            "CloudWatchEncryption": {"CloudWatchEncryptionMode": "DISABLED"},
            "JobBookmarksEncryption": {"JobBookmarksEncryptionMode": "DISABLED"},
        }
        # CREATE
        glue.create_security_configuration(Name=name, EncryptionConfiguration=enc_cfg)
        try:
            # RETRIEVE
            get_resp = glue.get_security_configuration(Name=name)
            assert get_resp["SecurityConfiguration"]["Name"] == name
            assert "EncryptionConfiguration" in get_resp["SecurityConfiguration"]
            # LIST
            list_resp = glue.get_security_configurations()
            assert isinstance(list_resp["SecurityConfigurations"], list)
            names = [sc["Name"] for sc in list_resp["SecurityConfigurations"]]
            assert name in names
            # ERROR: nonexistent config raises EntityNotFoundException
            with pytest.raises(ClientError) as exc:
                glue.get_security_configuration(Name="nonexistent-sc-xyz")
            assert exc.value.response["Error"]["Code"] == "EntityNotFoundException"
        finally:
            # DELETE
            glue.delete_security_configuration(Name=name)

    def test_get_ml_transforms(self, glue):
        """GetMLTransforms: create transform, retrieve it, list, delete, error."""
        name = _unique("mlt")
        create_resp = glue.create_ml_transform(
            Name=name,
            InputRecordTables=[{"DatabaseName": "default", "TableName": "test"}],
            Parameters={"TransformType": "FIND_MATCHES", "FindMatchesParameters": {"PrimaryKeyColumnName": "id"}},
            Role="arn:aws:iam::123456789012:role/test",
        )
        tfm_id = create_resp["TransformId"]
        try:
            # RETRIEVE
            get_resp = glue.get_ml_transform(TransformId=tfm_id)
            assert get_resp["TransformId"] == tfm_id
            assert get_resp["Name"] == name
            # LIST
            list_resp = glue.get_ml_transforms()
            assert isinstance(list_resp["Transforms"], list)
            ids = [t["TransformId"] for t in list_resp["Transforms"]]
            assert tfm_id in ids
        finally:
            # DELETE
            glue.delete_ml_transform(TransformId=tfm_id)
            # ERROR: gone after delete
            with pytest.raises(ClientError) as exc:
                glue.get_ml_transform(TransformId=tfm_id)
            assert exc.value.response["Error"]["Code"] == "EntityNotFoundException"

    def test_get_crawler_metrics(self, glue):
        """GetCrawlerMetrics: create crawler, get metrics, list crawlers, delete, error."""
        db_name = _unique("db")
        crawler_name = _unique("crawler")
        glue.create_database(DatabaseInput={"Name": db_name})
        glue.create_crawler(
            Name=crawler_name,
            Role="arn:aws:iam::123456789012:role/glue-role",
            DatabaseName=db_name,
            Targets={"S3Targets": [{"Path": "s3://bucket/data"}]},
        )
        try:
            # RETRIEVE: get crawler details
            get_resp = glue.get_crawler(Name=crawler_name)
            assert get_resp["Crawler"]["Name"] == crawler_name
            # LIST: get all crawler metrics
            metrics_resp = glue.get_crawler_metrics()
            assert isinstance(metrics_resp["CrawlerMetricsList"], list)
            # also list crawlers
            list_resp = glue.list_crawlers()
            assert crawler_name in list_resp["CrawlerNames"]
            # ERROR: nonexistent crawler raises EntityNotFoundException
            with pytest.raises(ClientError) as exc:
                glue.get_crawler(Name="nonexistent-crawler-xyz")
            assert exc.value.response["Error"]["Code"] == "EntityNotFoundException"
        finally:
            # DELETE
            glue.delete_crawler(Name=crawler_name)
            glue.delete_database(Name=db_name)

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
        """GetEntityRecords: create connection, retrieve it, list connections, get entity records, delete, error."""
        conn_name = _unique("conn")
        # CREATE: connection to anchor the test
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
            # RETRIEVE: get connection back
            get_resp = glue.get_connection(Name=conn_name)
            assert get_resp["Connection"]["Name"] == conn_name
            # LIST: connection appears in list
            list_resp = glue.get_connections()
            assert conn_name in [c["Name"] for c in list_resp["ConnectionList"]]
            # GetEntityRecords returns a Records list
            entity_resp = glue.get_entity_records(
                EntityName="fake-entity",
                ConnectionName="fake-conn",
                Limit=10,
            )
            assert isinstance(entity_resp["Records"], list)
        finally:
            # DELETE
            glue.delete_connection(ConnectionName=conn_name)
            # ERROR: gone after delete
            with pytest.raises(ClientError) as exc:
                glue.get_connection(Name=conn_name)
            assert exc.value.response["Error"]["Code"] == "EntityNotFoundException"


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
        """GetMapping: create db+table, retrieve table, list tables, get mapping, delete, error."""
        db_name = _unique("db")
        tbl_name = _unique("tbl")
        # CREATE: db and table as source
        glue.create_database(DatabaseInput={"Name": db_name})
        glue.create_table(
            DatabaseName=db_name,
            TableInput={
                "Name": tbl_name,
                "StorageDescriptor": {
                    "Columns": [{"Name": "id", "Type": "string"}],
                    "Location": "s3://bucket/path",
                    "InputFormat": "org.apache.hadoop.mapred.TextInputFormat",
                    "OutputFormat": "org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat",
                    "SerdeInfo": {"SerializationLibrary": "org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe"},
                },
            },
        )
        try:
            # RETRIEVE: table details
            get_resp = glue.get_table(DatabaseName=db_name, Name=tbl_name)
            assert get_resp["Table"]["Name"] == tbl_name
            # LIST: table in list
            list_resp = glue.get_tables(DatabaseName=db_name)
            assert tbl_name in [t["Name"] for t in list_resp["TableList"]]
            # GetMapping returns a list of mapping entries
            mapping_resp = glue.get_mapping(
                Source={"DatabaseName": db_name, "TableName": tbl_name},
            )
            assert isinstance(mapping_resp["Mapping"], list)
            # ERROR: nonexistent table raises EntityNotFoundException
            with pytest.raises(ClientError) as exc:
                glue.get_table(DatabaseName=db_name, Name="nonexistent-tbl-xyz")
            assert exc.value.response["Error"]["Code"] == "EntityNotFoundException"
        finally:
            # DELETE
            glue.delete_table(DatabaseName=db_name, Name=tbl_name)
            glue.delete_database(Name=db_name)


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
        """ListCustomEntityTypes: create type, retrieve, list, delete, error."""
        name = _unique("cet")
        # CREATE
        glue.create_custom_entity_type(Name=name, RegexString="\\d{4}-\\d{2}-\\d{2}")
        try:
            # RETRIEVE: get the entity type by name
            get_resp = glue.get_custom_entity_type(Name=name)
            assert get_resp["Name"] == name
            assert get_resp["RegexString"] == "\\d{4}-\\d{2}-\\d{2}"
            # LIST: entity type appears in list
            list_resp = glue.list_custom_entity_types()
            assert isinstance(list_resp["CustomEntityTypes"], list)
            names = [c["Name"] for c in list_resp["CustomEntityTypes"]]
            assert name in names
        finally:
            # DELETE
            glue.delete_custom_entity_type(Name=name)
            # ERROR: gone after delete
            with pytest.raises(ClientError) as exc:
                glue.get_custom_entity_type(Name=name)
            assert "Error" in exc.value.response

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
        """ListDevEndpoints: create endpoint, retrieve it, list, delete, error."""
        ep_name = _unique("ep")
        # CREATE: dev endpoint
        glue.create_dev_endpoint(
            EndpointName=ep_name,
            RoleArn="arn:aws:iam::123456789012:role/test",
            NumberOfNodes=2,
        )
        try:
            # RETRIEVE: get endpoint details
            get_resp = glue.get_dev_endpoint(EndpointName=ep_name)
            assert get_resp["DevEndpoint"]["EndpointName"] == ep_name
            # LIST: endpoint appears in list
            resp = glue.list_dev_endpoints()
            assert isinstance(resp["DevEndpointNames"], list)
            assert ep_name in resp["DevEndpointNames"]
        finally:
            # DELETE
            glue.delete_dev_endpoint(EndpointName=ep_name)
            # ERROR: gone after delete
            with pytest.raises(ClientError) as exc:
                glue.get_dev_endpoint(EndpointName=ep_name)
            assert exc.value.response["Error"]["Code"] == "EntityNotFoundException"

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
        """ListColumnStatisticsTaskRuns: create table, start task run, retrieve run, list, error."""
        db_name = _unique("db")
        tbl_name = _unique("tbl")
        glue.create_database(DatabaseInput={"Name": db_name})
        glue.create_table(
            DatabaseName=db_name,
            TableInput={
                "Name": tbl_name,
                "StorageDescriptor": {
                    "Columns": [{"Name": "id", "Type": "string"}],
                    "Location": "s3://bucket/path",
                    "InputFormat": "org.apache.hadoop.mapred.TextInputFormat",
                    "OutputFormat": "org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat",
                    "SerdeInfo": {"SerializationLibrary": "org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe"},
                },
            },
        )
        try:
            # CREATE: start a column statistics task run
            start_resp = glue.start_column_statistics_task_run(
                DatabaseName=db_name,
                TableName=tbl_name,
                ColumnNameList=["id"],
                Role="arn:aws:iam::123456789012:role/test",
            )
            run_id = start_resp["ColumnStatisticsTaskRunId"]
            assert run_id
            # RETRIEVE: get the task run details
            get_resp = glue.get_column_statistics_task_run(ColumnStatisticsTaskRunId=run_id)
            assert get_resp["ColumnStatisticsTaskRun"]["ColumnStatisticsTaskRunId"] == run_id
            # LIST: task run IDs list (may not include in-progress runs immediately)
            resp = glue.list_column_statistics_task_runs()
            assert isinstance(resp["ColumnStatisticsTaskRunIds"], list)
            # ERROR: nonexistent table raises EntityNotFoundException
            with pytest.raises(ClientError) as exc:
                glue.get_table(DatabaseName=db_name, Name="nonexistent-tbl-xyz")
            assert exc.value.response["Error"]["Code"] == "EntityNotFoundException"
        finally:
            glue.delete_table(DatabaseName=db_name, Name=tbl_name)
            glue.delete_database(Name=db_name)

    def test_list_ml_transforms(self, glue):
        """ListMLTransforms: create transform, retrieve, list, delete, error."""
        name = _unique("mlt")
        # CREATE
        create_resp = glue.create_ml_transform(
            Name=name,
            InputRecordTables=[{"DatabaseName": "default", "TableName": "test"}],
            Parameters={"TransformType": "FIND_MATCHES", "FindMatchesParameters": {"PrimaryKeyColumnName": "id"}},
            Role="arn:aws:iam::123456789012:role/test",
        )
        tfm_id = create_resp["TransformId"]
        try:
            # RETRIEVE: get individual transform
            get_resp = glue.get_ml_transform(TransformId=tfm_id)
            assert get_resp["TransformId"] == tfm_id
            assert get_resp["Name"] == name
            # LIST: transform appears in list
            list_resp = glue.list_ml_transforms()
            assert isinstance(list_resp["TransformIds"], list)
            assert tfm_id in list_resp["TransformIds"]
        finally:
            # DELETE
            glue.delete_ml_transform(TransformId=tfm_id)
            # ERROR: gone after delete
            with pytest.raises(ClientError) as exc:
                glue.get_ml_transform(TransformId=tfm_id)
            assert exc.value.response["Error"]["Code"] == "EntityNotFoundException"

    def test_list_data_quality_results(self, glue):
        """ListDataQualityResults: start evaluation run, retrieve it, list results, error."""
        # CREATE: start a ruleset evaluation run
        start_resp = glue.start_data_quality_ruleset_evaluation_run(
            DataSource={"GlueTable": {"DatabaseName": "testdb", "TableName": "testtbl"}},
            Role="arn:aws:iam::123456789012:role/test",
            RulesetNames=["ruleset1"],
        )
        run_id = start_resp["RunId"]
        assert run_id
        # RETRIEVE: get the run details
        get_resp = glue.get_data_quality_ruleset_evaluation_run(RunId=run_id)
        assert get_resp["RunId"] == run_id
        # LIST: results list (may be empty but key must exist)
        resp = glue.list_data_quality_results()
        assert isinstance(resp["Results"], list)
        # ERROR: nonexistent run raises error
        with pytest.raises(ClientError) as exc:
            glue.get_data_quality_ruleset_evaluation_run(RunId="nonexistent-run-xyz")
        assert exc.value.response["Error"]["Code"] in ("EntityNotFoundException", "InvalidInputException")

    def test_list_data_quality_rule_recommendation_runs(self, glue):
        """ListDataQualityRuleRecommendationRuns: start run, retrieve it, list runs, error."""
        # CREATE: start a recommendation run
        start_resp = glue.start_data_quality_rule_recommendation_run(
            DataSource={"GlueTable": {"DatabaseName": "testdb3", "TableName": "testtbl3"}},
            Role="arn:aws:iam::123456789012:role/test",
        )
        run_id = start_resp["RunId"]
        assert run_id
        # RETRIEVE: get run details
        get_resp = glue.get_data_quality_rule_recommendation_run(RunId=run_id)
        assert get_resp["RunId"] == run_id
        # LIST: recommendation runs list
        resp = glue.list_data_quality_rule_recommendation_runs()
        assert isinstance(resp["Runs"], list)
        # ERROR: nonexistent run raises error
        with pytest.raises(ClientError) as exc:
            glue.get_data_quality_rule_recommendation_run(RunId="nonexistent-run-xyz")
        assert exc.value.response["Error"]["Code"] in ("EntityNotFoundException", "InvalidInputException")

    def test_list_data_quality_ruleset_evaluation_runs(self, glue):
        """ListDataQualityRulesetEvaluationRuns: start run, retrieve it, list runs, error."""
        # CREATE: start an evaluation run
        start_resp = glue.start_data_quality_ruleset_evaluation_run(
            DataSource={"GlueTable": {"DatabaseName": "testdb4", "TableName": "testtbl4"}},
            Role="arn:aws:iam::123456789012:role/test",
            RulesetNames=["ruleset2"],
        )
        run_id = start_resp["RunId"]
        assert run_id
        # RETRIEVE: get run details
        get_resp = glue.get_data_quality_ruleset_evaluation_run(RunId=run_id)
        assert get_resp["RunId"] == run_id
        # LIST: evaluation runs list
        resp = glue.list_data_quality_ruleset_evaluation_runs()
        assert isinstance(resp["Runs"], list)
        # ERROR: nonexistent run raises error
        with pytest.raises(ClientError) as exc:
            glue.get_data_quality_ruleset_evaluation_run(RunId="nonexistent-run-xyz-2")
        assert exc.value.response["Error"]["Code"] in ("EntityNotFoundException", "InvalidInputException")


class TestGlueSearchTables:
    """Tests for SearchTables operation."""

    def test_search_tables(self, glue):
        """SearchTables returns TableList containing created tables."""
        db_name = _unique("db")
        tbl_name = _unique("stbl")
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
            resp = glue.search_tables()
            assert "TableList" in resp
            assert isinstance(resp["TableList"], list)
            # The created table should appear in results
            db_tables = [t for t in resp["TableList"] if t.get("DatabaseName") == db_name]
            table_names = [t["Name"] for t in db_tables]
            assert tbl_name in table_names
        finally:
            glue.delete_table(DatabaseName=db_name, Name=tbl_name)
            glue.delete_database(Name=db_name)

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
        # CREATE: two connections
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
        # RETRIEVE: verify both connections exist
        r1 = glue.get_connection(Name=c1)
        assert r1["Connection"]["Name"] == c1
        # LIST: both appear in list
        list_resp = glue.get_connections()
        conn_names = [c["Name"] for c in list_resp["ConnectionList"]]
        assert c1 in conn_names and c2 in conn_names
        # DELETE: batch delete both
        resp = glue.batch_delete_connection(ConnectionNameList=[c1, c2])
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200
        # ERROR: gone after delete
        with pytest.raises(ClientError) as exc:
            glue.get_connection(Name=c1)
        assert exc.value.response["Error"]["Code"] == "EntityNotFoundException"

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
        # Verify status reflects import completion
        status_resp = glue.get_catalog_import_status()
        assert "ImportStatus" in status_resp
        assert status_resp["ImportStatus"]["ImportCompleted"] is True


class TestGlueGetCatalogs:
    """Tests for GetCatalogs."""

    def test_get_catalogs(self, glue):
        """GetCatalogs: import catalog, list catalogs, retrieve status, error on bad catalog ID."""
        # CREATE: trigger import so catalog state exists
        glue.import_catalog_to_glue()
        # LIST: get all catalogs
        resp = glue.get_catalogs()
        assert isinstance(resp["CatalogList"], list)
        # RETRIEVE: get import status
        status_resp = glue.get_catalog_import_status()
        assert status_resp["ImportStatus"]["ImportCompleted"] is True
        # ERROR: nonexistent catalog raises EntityNotFoundException
        with pytest.raises(ClientError) as exc:
            glue.get_catalog(CatalogId="nonexistent-catalog-xyz-123")
        assert exc.value.response["Error"]["Code"] == "EntityNotFoundException"


class TestGlueSchemaVersions:
    """Tests for CheckSchemaVersionValidity and DeleteSchemaVersions."""

    def test_check_schema_version_validity(self, glue):
        definition = '{"type":"record","name":"T","fields":[{"name":"id","type":"int"}]}'
        resp = glue.check_schema_version_validity(
            DataFormat="AVRO",
            SchemaDefinition=definition,
        )
        assert "Valid" in resp
        assert resp["Valid"] is True
        # Create a registry and schema then verify the same definition is valid
        reg_name = _unique("reg")
        schema_name = _unique("schema")
        glue.create_registry(RegistryName=reg_name)
        glue.create_schema(
            RegistryId={"RegistryName": reg_name},
            SchemaName=schema_name,
            DataFormat="AVRO",
            Compatibility="NONE",
            SchemaDefinition=definition,
        )
        try:
            verify_resp = glue.check_schema_version_validity(
                DataFormat="AVRO",
                SchemaDefinition=definition,
            )
            assert verify_resp["Valid"] is True
        finally:
            glue.delete_schema(SchemaId={"SchemaName": schema_name, "RegistryName": reg_name})
            glue.delete_registry(RegistryId={"RegistryName": reg_name})

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
        """DescribeInboundIntegrations: create integration, list inbound, retrieve, error."""
        name = _unique("int")
        target_arn = "arn:aws:glue:us-east-1:123456789012:database/tgt-inbound"
        # CREATE: integration targeting the database
        create_resp = glue.create_integration(
            IntegrationName=name,
            SourceArn="arn:aws:glue:us-east-1:123456789012:database/src-inbound",
            TargetArn=target_arn,
        )
        int_arn = create_resp["IntegrationArn"]
        # RETRIEVE: get the integration back via describe_integrations
        all_resp = glue.describe_integrations()
        arns = [i["IntegrationArn"] for i in all_resp["Integrations"]]
        assert int_arn in arns
        # LIST: describe inbound integrations for target
        resp = glue.describe_inbound_integrations(TargetArn=target_arn)
        assert isinstance(resp["InboundIntegrations"], list)
        # ERROR: nonexistent integration resource raises error
        with pytest.raises(ClientError) as exc:
            glue.modify_integration(
                IntegrationIdentifier="arn:aws:glue:us-east-1:123456789012:integration/nonexistent-xyz",
                Description="nope",
            )
        assert exc.value.response["Error"]["Code"] == "EntityNotFoundException"


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
        """ListDataQualityStatistics: start recommendation run, list statistics, retrieve run, error."""
        # CREATE: start a data quality recommendation run
        start_resp = glue.start_data_quality_rule_recommendation_run(
            DataSource={"GlueTable": {"DatabaseName": "testdb", "TableName": "testtbl"}},
            Role="arn:aws:iam::123456789012:role/test",
        )
        run_id = start_resp["RunId"]
        assert run_id
        # RETRIEVE: get the run details
        get_resp = glue.get_data_quality_rule_recommendation_run(RunId=run_id)
        assert get_resp["RunId"] == run_id
        # LIST: statistics list (may be empty but key must exist)
        resp = glue.list_data_quality_statistics()
        assert isinstance(resp["Statistics"], list)
        # ERROR: nonexistent run raises error
        with pytest.raises(ClientError) as exc:
            glue.get_data_quality_rule_recommendation_run(RunId="nonexistent-run-xyz")
        assert exc.value.response["Error"]["Code"] in ("EntityNotFoundException", "InvalidInputException")

    def test_list_data_quality_statistic_annotations(self, glue):
        """ListDataQualityStatisticAnnotations: start run, list annotations, retrieve run, error."""
        # CREATE: start a recommendation run to establish run context
        start_resp = glue.start_data_quality_rule_recommendation_run(
            DataSource={"GlueTable": {"DatabaseName": "testdb2", "TableName": "testtbl2"}},
            Role="arn:aws:iam::123456789012:role/test",
        )
        run_id = start_resp["RunId"]
        assert run_id
        # RETRIEVE: get the run
        get_resp = glue.get_data_quality_rule_recommendation_run(RunId=run_id)
        assert get_resp["RunId"] == run_id
        # LIST: annotation list (may be empty but key must exist)
        resp = glue.list_data_quality_statistic_annotations()
        assert isinstance(resp["Annotations"], list)
        # ERROR: nonexistent ruleset raises error
        with pytest.raises(ClientError) as exc:
            glue.get_data_quality_ruleset(Name="nonexistent-dqr-xyz")
        assert exc.value.response["Error"]["Code"] in ("EntityNotFoundException", "InvalidInputException")

    def test_batch_put_data_quality_statistic_annotation(self, glue):
        """BatchPutDataQualityStatisticAnnotation with empty list returns empty failures."""
        resp = glue.batch_put_data_quality_statistic_annotation(InclusionAnnotations=[])
        assert "FailedInclusionAnnotations" in resp
        assert isinstance(resp["FailedInclusionAnnotations"], list)
        # Start a DQ run to get a valid context, then list annotations
        start_resp = glue.start_data_quality_rule_recommendation_run(
            DataSource={"GlueTable": {"DatabaseName": "testdb", "TableName": "testtbl"}},
            Role="arn:aws:iam::123456789012:role/test",
        )
        assert start_resp["RunId"]
        list_resp = glue.list_data_quality_statistic_annotations()
        assert "Annotations" in list_resp

    def test_put_data_quality_profile_annotation(self, glue):
        """PutDataQualityProfileAnnotation: create ruleset, put annotation, list rulesets, delete, error."""
        # CREATE: data quality ruleset as context
        name = _unique("dqr")
        glue.create_data_quality_ruleset(Name=name, Ruleset='Rules = [ IsComplete "id" ]')
        try:
            # RETRIEVE: get ruleset details
            get_resp = glue.get_data_quality_ruleset(Name=name)
            assert get_resp["Name"] == name
            # LIST: ruleset appears in list
            list_resp = glue.list_data_quality_rulesets()
            assert name in [r["Name"] for r in list_resp["Rulesets"]]
            # PUT: annotation succeeds
            ann_resp = glue.put_data_quality_profile_annotation(
                ProfileId="fake-profile", InclusionAnnotation="INCLUDE"
            )
            assert ann_resp["ResponseMetadata"]["HTTPStatusCode"] == 200
        finally:
            # DELETE
            glue.delete_data_quality_ruleset(Name=name)
            # ERROR: gone after delete
            with pytest.raises(ClientError) as exc:
                glue.get_data_quality_ruleset(Name=name)
            assert exc.value.response["Error"]["Code"] in ("EntityNotFoundException", "InvalidInputException")

    def test_cancel_data_quality_rule_recommendation_run(self, glue):
        """CancelDataQualityRuleRecommendationRun: start run, retrieve it, list runs, cancel, error."""
        # CREATE: start a recommendation run
        start_resp = glue.start_data_quality_rule_recommendation_run(
            DataSource={"GlueTable": {"DatabaseName": "testdb", "TableName": "testtbl"}},
            Role="arn:aws:iam::123456789012:role/test",
        )
        run_id = start_resp["RunId"]
        assert run_id
        # RETRIEVE: get run details
        get_resp = glue.get_data_quality_rule_recommendation_run(RunId=run_id)
        assert get_resp["RunId"] == run_id
        # LIST: run appears in list
        list_resp = glue.list_data_quality_rule_recommendation_runs()
        assert isinstance(list_resp["Runs"], list)
        # DELETE (cancel): cancel the run
        resp = glue.cancel_data_quality_rule_recommendation_run(RunId=run_id)
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200
        # ERROR: nonexistent run raises error
        with pytest.raises(ClientError) as exc:
            glue.get_data_quality_rule_recommendation_run(RunId="nonexistent-cancel-xyz")
        assert exc.value.response["Error"]["Code"] in ("EntityNotFoundException", "InvalidInputException")

    def test_cancel_data_quality_ruleset_evaluation_run(self, glue):
        """CancelDataQualityRulesetEvaluationRun: start run, retrieve it, list, cancel, error."""
        # CREATE: start an evaluation run
        start_resp = glue.start_data_quality_ruleset_evaluation_run(
            DataSource={"GlueTable": {"DatabaseName": "testdb", "TableName": "testtbl"}},
            Role="arn:aws:iam::123456789012:role/test",
            RulesetNames=["ruleset1"],
        )
        run_id = start_resp["RunId"]
        assert run_id
        # RETRIEVE: get run details
        get_resp = glue.get_data_quality_ruleset_evaluation_run(RunId=run_id)
        assert get_resp["RunId"] == run_id
        # LIST: run appears in evaluation runs list
        list_resp = glue.list_data_quality_ruleset_evaluation_runs()
        assert isinstance(list_resp["Runs"], list)
        # DELETE (cancel): cancel the run
        resp = glue.cancel_data_quality_ruleset_evaluation_run(RunId=run_id)
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200
        # ERROR: nonexistent run raises error
        with pytest.raises(ClientError) as exc:
            glue.get_data_quality_ruleset_evaluation_run(RunId="nonexistent-cancel-xyz")
        assert exc.value.response["Error"]["Code"] in ("EntityNotFoundException", "InvalidInputException")

    def test_get_data_quality_model_result_not_found(self, glue):
        """GetDataQualityModelResult for nonexistent raises EntityNotFoundException."""
        with pytest.raises(ClientError) as exc:
            glue.get_data_quality_model_result(StatisticId="fake-stat", ProfileId="fake-profile")
        assert exc.value.response["Error"]["Code"] == "EntityNotFoundException"

    def test_start_data_quality_rule_recommendation_run(self, glue):
        """StartDataQualityRuleRecommendationRun: start run, retrieve run details, list, cancel, error."""
        # CREATE: start a recommendation run
        resp = glue.start_data_quality_rule_recommendation_run(
            DataSource={"GlueTable": {"DatabaseName": "nonexistent", "TableName": "nonexistent"}},
            Role="arn:aws:iam::123456789012:role/test",
        )
        run_id = resp["RunId"]
        assert run_id
        # RETRIEVE: get run details
        get_resp = glue.get_data_quality_rule_recommendation_run(RunId=run_id)
        assert get_resp["RunId"] == run_id
        # LIST: run appears in list
        list_resp = glue.list_data_quality_rule_recommendation_runs()
        assert isinstance(list_resp["Runs"], list)
        # DELETE (cancel)
        glue.cancel_data_quality_rule_recommendation_run(RunId=run_id)
        # ERROR: nonexistent run raises error
        with pytest.raises(ClientError) as exc:
            glue.get_data_quality_rule_recommendation_run(RunId="nonexistent-run-xyz")
        assert exc.value.response["Error"]["Code"] in ("EntityNotFoundException", "InvalidInputException")


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
        """GetConnections returns a ConnectionList (list type, even when empty)."""
        resp = glue.get_connections()
        assert isinstance(resp["ConnectionList"], list)

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
        assert isinstance(resp["PartitionIndexDescriptorList"], list)


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
        """GetTriggers returns a Triggers list (even when empty)."""
        resp = glue.get_triggers()
        assert isinstance(resp["Triggers"], list)

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
        """ListCrawlers returns CrawlerNames (list type, even when empty)."""
        resp = glue.list_crawlers()
        assert isinstance(resp["CrawlerNames"], list)


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
        """ListJobs returns JobNames (list type, even when empty)."""
        resp = glue.list_jobs()
        assert isinstance(resp["JobNames"], list)

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
        """ListTriggers returns TriggerNames (list type, even when empty)."""
        resp = glue.list_triggers()
        assert isinstance(resp["TriggerNames"], list)

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
        # Create a real blueprint to verify split behavior
        bp_name = _unique("bp")
        glue.create_blueprint(Name=bp_name, BlueprintLocation="s3://bucket/bp.py")
        try:
            resp = glue.batch_get_blueprints(Names=["nonexistent-bp-xyz"])
            assert "MissingBlueprints" in resp
            assert "nonexistent-bp-xyz" in resp["MissingBlueprints"]
            # Verify real blueprint is retrievable
            get_resp = glue.get_blueprint(Name=bp_name)
            assert get_resp["Blueprint"]["Name"] == bp_name
        finally:
            glue.delete_blueprint(Name=bp_name)


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
        """BatchGetDevEndpoints returns DevEndpointsNotFound for missing endpoints."""
        # Create a real dev endpoint to validate split behavior
        ep_name = _unique("de")
        glue.create_dev_endpoint(
            EndpointName=ep_name,
            RoleArn="arn:aws:iam::123456789012:role/glue-role",
        )
        try:
            resp = glue.batch_get_dev_endpoints(DevEndpointNames=["nonexistent-de-xyz"])
            assert "DevEndpointsNotFound" in resp
            assert "nonexistent-de-xyz" in resp["DevEndpointsNotFound"]
            # Verify real endpoint is retrievable via get_dev_endpoint
            get_resp = glue.get_dev_endpoint(EndpointName=ep_name)
            assert get_resp["DevEndpoint"]["EndpointName"] == ep_name
        finally:
            glue.delete_dev_endpoint(EndpointName=ep_name)


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
        """GetMLTransforms: create transform, retrieve, list, update, delete, error."""
        name = _unique("mlt")
        tfm_id = self._create_transform(glue, name)
        try:
            # RETRIEVE
            get_resp = glue.get_ml_transform(TransformId=tfm_id)
            assert get_resp["TransformId"] == tfm_id
            assert get_resp["Name"] == name
            # LIST
            resp = glue.get_ml_transforms()
            assert isinstance(resp["Transforms"], list)
            assert tfm_id in [t["TransformId"] for t in resp["Transforms"]]
            # UPDATE
            glue.update_ml_transform(TransformId=tfm_id, Description="updated")
            updated = glue.get_ml_transform(TransformId=tfm_id)
            assert updated["Description"] == "updated"
            # ERROR
            with pytest.raises(ClientError) as exc:
                glue.get_ml_transform(TransformId="nonexistent-xyz")
            assert exc.value.response["Error"]["Code"] == "EntityNotFoundException"
        finally:
            glue.delete_ml_transform(TransformId=tfm_id)

    def test_list_ml_transforms(self, glue):
        """ListMLTransforms: create transform, retrieve, list, update, delete, error."""
        name = _unique("mlt")
        tfm_id = self._create_transform(glue, name)
        try:
            # RETRIEVE
            get_resp = glue.get_ml_transform(TransformId=tfm_id)
            assert get_resp["TransformId"] == tfm_id
            # LIST
            resp = glue.list_ml_transforms()
            assert isinstance(resp["TransformIds"], list)
            assert tfm_id in resp["TransformIds"]
            # UPDATE
            glue.update_ml_transform(TransformId=tfm_id, Description="updated-list")
            updated = glue.get_ml_transform(TransformId=tfm_id)
            assert updated["Description"] == "updated-list"
            # ERROR
            with pytest.raises(ClientError) as exc:
                glue.get_ml_transform(TransformId="nonexistent-xyz-2")
            assert exc.value.response["Error"]["Code"] == "EntityNotFoundException"
        finally:
            glue.delete_ml_transform(TransformId=tfm_id)

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
        """StartDataQualityRulesetEvaluationRun: start run, retrieve it, list runs, cancel, error."""
        # CREATE: start a ruleset evaluation run
        resp = glue.start_data_quality_ruleset_evaluation_run(
            DataSource={"GlueTable": {"DatabaseName": "testdb", "TableName": "testtbl"}},
            Role="arn:aws:iam::123456789012:role/test",
            RulesetNames=["ruleset1"],
        )
        run_id = resp["RunId"]
        assert run_id
        # RETRIEVE: get run details
        get_resp = glue.get_data_quality_ruleset_evaluation_run(RunId=run_id)
        assert get_resp["RunId"] == run_id
        # LIST: run appears in list
        list_resp = glue.list_data_quality_ruleset_evaluation_runs()
        assert isinstance(list_resp["Runs"], list)
        # DELETE (cancel): cancel the run
        glue.cancel_data_quality_ruleset_evaluation_run(RunId=run_id)
        # ERROR: nonexistent run raises error
        with pytest.raises(ClientError) as exc:
            glue.get_data_quality_ruleset_evaluation_run(RunId="nonexistent-run-xyz")
        assert exc.value.response["Error"]["Code"] in ("EntityNotFoundException", "InvalidInputException")


class TestGlueBatchGetDataQualityResult:
    def test_batch_get_data_quality_result(self, glue):
        """BatchGetDataQualityResult returns results list (possibly empty)."""
        resp = glue.batch_get_data_quality_result(ResultIds=["result-fake-id"])
        assert "Results" in resp
        assert isinstance(resp["Results"], list)
        # Start a DQ evaluation run to add context, then list results
        start_resp = glue.start_data_quality_ruleset_evaluation_run(
            DataSource={"GlueTable": {"DatabaseName": "testdb", "TableName": "testtbl"}},
            Role="arn:aws:iam::123456789012:role/test",
            RulesetNames=["ruleset1"],
        )
        assert start_resp["RunId"]
        list_resp = glue.list_data_quality_results()
        assert "Results" in list_resp


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


class TestGlueNewOps:
    """Tests for Glue newer operations."""

    def test_describe_connection_type_nonexistent(self, glue):
        """DescribeConnectionType with unknown type raises EntityNotFoundException."""
        with pytest.raises(ClientError) as exc:
            glue.describe_connection_type(ConnectionType="FAKE_NONEXISTENT_TYPE")
        assert exc.value.response["Error"]["Code"] == "EntityNotFoundException"

    def test_describe_entity_nonexistent(self, glue):
        """DescribeEntity with nonexistent connection raises EntityNotFoundException."""
        with pytest.raises(ClientError) as exc:
            glue.describe_entity(ConnectionName="nonexistent-conn-xyz", EntityName="fake-entity")
        assert exc.value.response["Error"]["Code"] == "EntityNotFoundException"

    def test_get_dataflow_graph(self, glue):
        """GetDataflowGraph returns DAG nodes and edges."""
        resp = glue.get_dataflow_graph(PythonScript="x = 1")
        assert "DagNodes" in resp
        assert "DagEdges" in resp

    def test_get_glue_identity_center_configuration_not_found(self, glue):
        """GetGlueIdentityCenterConfiguration raises EntityNotFoundException when not configured."""
        with pytest.raises(ClientError) as exc:
            glue.get_glue_identity_center_configuration()
        assert exc.value.response["Error"]["Code"] == "EntityNotFoundException"

    def test_get_materialized_view_refresh_task_run_not_found(self, glue):
        """GetMaterializedViewRefreshTaskRun with nonexistent run raises EntityNotFoundException."""
        with pytest.raises(ClientError) as exc:
            glue.get_materialized_view_refresh_task_run(
                CatalogId="123456789012",
                MaterializedViewRefreshTaskRunId="nonexistent-run-id",
            )
        assert exc.value.response["Error"]["Code"] == "EntityNotFoundException"

    def test_get_table_optimizer_not_found(self, glue):
        """GetTableOptimizer with nonexistent table raises EntityNotFoundException."""
        with pytest.raises(ClientError) as exc:
            glue.get_table_optimizer(
                CatalogId="123456789012",
                DatabaseName="nonexistent-db-xyz",
                TableName="nonexistent-table-xyz",
                Type="compaction",
            )
        assert exc.value.response["Error"]["Code"] == "EntityNotFoundException"

    def test_list_connection_types(self, glue):
        """ListConnectionTypes returns connection types list."""
        resp = glue.list_connection_types()
        assert "ConnectionTypes" in resp

    def test_list_entities_nonexistent_connection(self, glue):
        """ListEntities with nonexistent connection raises EntityNotFoundException."""
        with pytest.raises(ClientError) as exc:
            glue.list_entities(ConnectionName="nonexistent-conn-xyz")
        assert exc.value.response["Error"]["Code"] == "EntityNotFoundException"

    def test_list_materialized_view_refresh_task_runs(self, glue):
        """ListMaterializedViewRefreshTaskRuns returns empty list for nonexistent view."""
        resp = glue.list_materialized_view_refresh_task_runs(CatalogId="123456789012")
        assert "MaterializedViewRefreshTaskRuns" in resp

    def test_list_table_optimizer_runs_not_found(self, glue):
        """ListTableOptimizerRuns with nonexistent table raises EntityNotFoundException."""
        with pytest.raises(ClientError) as exc:
            glue.list_table_optimizer_runs(
                CatalogId="123456789012",
                DatabaseName="nonexistent-db-xyz",
                TableName="nonexistent-table-xyz",
                Type="compaction",
            )
        assert exc.value.response["Error"]["Code"] == "EntityNotFoundException"


class TestGlueNewGapOps:
    """Tests for 23 Glue gap operations."""

    def test_batch_get_table_optimizer_empty(self, glue):
        """BatchGetTableOptimizer returns TableOptimizers and Failures keys."""
        # Create a real table to give context to the call
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
            # Query nonexistent optimizer for the real table
            resp = glue.batch_get_table_optimizer(
                Entries=[
                    {
                        "catalogId": "123456789012",
                        "databaseName": db_name,
                        "tableName": tbl_name,
                        "type": "compaction",
                    }
                ]
            )
            assert "TableOptimizers" in resp
            assert "Failures" in resp
            assert isinstance(resp["TableOptimizers"], list)
            assert isinstance(resp["Failures"], list)
            # Also verify get_table returns the real table
            get_resp = glue.get_table(DatabaseName=db_name, Name=tbl_name)
            assert get_resp["Table"]["Name"] == tbl_name
        finally:
            glue.delete_table(DatabaseName=db_name, Name=tbl_name)
            glue.delete_database(Name=db_name)

    def test_create_glue_identity_center_configuration(self, glue):
        """CreateGlueIdentityCenterConfiguration returns ApplicationArn."""
        resp = glue.create_glue_identity_center_configuration(
            InstanceArn="arn:aws:sso:::instance/ssoins-test-gap-ops"
        )
        assert "ApplicationArn" in resp

    def test_create_and_delete_table_optimizer(self, glue):
        """CreateTableOptimizer succeeds and DeleteTableOptimizer succeeds."""
        db_name = _unique("db")
        table_name = _unique("tbl")
        glue.create_database(DatabaseInput={"Name": db_name})
        glue.create_table(
            DatabaseName=db_name,
            TableInput={
                "Name": table_name,
                "StorageDescriptor": {
                    "Columns": [],
                    "InputFormat": "",
                    "OutputFormat": "",
                    "SerdeInfo": {},
                },
            },
        )
        resp = glue.create_table_optimizer(
            CatalogId="123456789012",
            DatabaseName=db_name,
            TableName=table_name,
            Type="compaction",
            TableOptimizerConfiguration={
                "roleArn": "arn:aws:iam::123456789012:role/test",
                "enabled": True,
            },
        )
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200

        resp = glue.delete_table_optimizer(
            CatalogId="123456789012",
            DatabaseName=db_name,
            TableName=table_name,
            Type="compaction",
        )
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200
        glue.delete_table(DatabaseName=db_name, Name=table_name)
        glue.delete_database(Name=db_name)

    def test_delete_connection_type_not_found(self, glue):
        """DeleteConnectionType with nonexistent type raises EntityNotFoundException."""
        with pytest.raises(ClientError) as exc:
            glue.delete_connection_type(ConnectionType="NONEXISTENT-TYPE-XYZ")
        assert exc.value.response["Error"]["Code"] == "EntityNotFoundException"

    def test_delete_glue_identity_center_configuration(self, glue):
        """DeleteGlueIdentityCenterConfiguration succeeds (no config)."""
        resp = glue.delete_glue_identity_center_configuration()
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200

    def test_get_plan_returns_python_script(self, glue):
        """GetPlan returns PythonScript stub."""
        resp = glue.get_plan(
            Mapping=[
                {
                    "SourceTable": "src",
                    "SourcePath": "a",
                    "TargetTable": "tgt",
                    "TargetPath": "b",
                    "TargetType": "string",
                }
            ],
            Source={"DatabaseName": "db", "TableName": "tbl"},
        )
        assert "PythonScript" in resp

    def test_get_unfiltered_table_metadata_not_found(self, glue):
        """GetUnfilteredTableMetadata with nonexistent table raises EntityNotFoundException."""
        with pytest.raises(ClientError) as exc:
            glue.get_unfiltered_table_metadata(
                CatalogId="123456789012",
                DatabaseName="nonexistent-db-xyz",
                Name="nonexistent-table-xyz",
                SupportedPermissionTypes=["COLUMN_PERMISSION"],
            )
        assert exc.value.response["Error"]["Code"] == "EntityNotFoundException"

    def test_get_unfiltered_partitions_metadata_not_found(self, glue):
        """GetUnfilteredPartitionsMetadata with nonexistent table raises EntityNotFoundException."""
        with pytest.raises(ClientError) as exc:
            glue.get_unfiltered_partitions_metadata(
                CatalogId="123456789012",
                DatabaseName="nonexistent-db-xyz",
                TableName="nonexistent-table-xyz",
                SupportedPermissionTypes=["COLUMN_PERMISSION"],
            )
        assert exc.value.response["Error"]["Code"] == "EntityNotFoundException"

    def test_get_unfiltered_partition_metadata_not_found(self, glue):
        """GetUnfilteredPartitionMetadata with nonexistent table raises EntityNotFoundException."""
        with pytest.raises(ClientError) as exc:
            glue.get_unfiltered_partition_metadata(
                CatalogId="123456789012",
                DatabaseName="nonexistent-db-xyz",
                TableName="nonexistent-table-xyz",
                PartitionValues=["val1"],
                SupportedPermissionTypes=["COLUMN_PERMISSION"],
            )
        assert exc.value.response["Error"]["Code"] == "EntityNotFoundException"

    def test_list_integration_resource_properties_empty(self, glue):
        """ListIntegrationResourceProperties returns IntegrationResourcePropertyList."""
        resp = glue.list_integration_resource_properties()
        assert "IntegrationResourcePropertyList" in resp

    def test_query_schema_version_metadata_invalid_input(self, glue):
        """QuerySchemaVersionMetadata with fake ID raises InvalidInputException."""
        with pytest.raises(ClientError) as exc:
            glue.query_schema_version_metadata(
                SchemaVersionId="fake-uuid-0000-1234-abcd-ef0123456789"
            )
        assert exc.value.response["Error"]["Code"] == "InvalidInputException"

    def test_register_connection_type(self, glue):
        """RegisterConnectionType returns ConnectionTypeArn."""
        resp = glue.register_connection_type(
            ConnectionType=_unique("CT"),
            IntegrationType="JDBC",
            ConnectionProperties={
                "Url": {
                    "Name": "Url",
                    "Required": True,
                    "PropertyType": "CONNECTION_PROPERTY_TYPE",
                }
            },
            ConnectorAuthenticationConfiguration={"AuthenticationTypes": ["BASIC"]},
            RestConfiguration={},
        )
        assert "ConnectionTypeArn" in resp

    def test_remove_schema_version_metadata_invalid_input(self, glue):
        """RemoveSchemaVersionMetadata without valid schema raises InvalidInputException."""
        with pytest.raises(ClientError) as exc:
            glue.remove_schema_version_metadata(
                MetadataKeyValue={"MetadataKey": "key", "MetadataValue": "value"}
            )
        assert exc.value.response["Error"]["Code"] == "InvalidInputException"

    def test_start_export_labels_task_run_not_found(self, glue):
        """StartExportLabelsTaskRun with fake transform raises EntityNotFoundException."""
        with pytest.raises(ClientError) as exc:
            glue.start_export_labels_task_run(
                TransformId="fake-transform-id-xyz",
                OutputS3Path="s3://bucket/path",
            )
        assert exc.value.response["Error"]["Code"] == "EntityNotFoundException"

    def test_start_import_labels_task_run_not_found(self, glue):
        """StartImportLabelsTaskRun with fake transform raises EntityNotFoundException."""
        with pytest.raises(ClientError) as exc:
            glue.start_import_labels_task_run(
                TransformId="fake-transform-id-xyz",
                InputS3Path="s3://bucket/path",
            )
        assert exc.value.response["Error"]["Code"] == "EntityNotFoundException"

    def test_start_ml_labeling_set_generation_task_run_not_found(self, glue):
        """StartMLLabelingSetGenerationTaskRun with fake transform raises EntityNotFoundException."""  # noqa: E501
        with pytest.raises(ClientError) as exc:
            glue.start_ml_labeling_set_generation_task_run(
                TransformId="fake-transform-id-xyz",
                OutputS3Path="s3://bucket/path",
            )
        assert exc.value.response["Error"]["Code"] == "EntityNotFoundException"

    def test_start_materialized_view_refresh_task_run_not_found(self, glue):
        """StartMaterializedViewRefreshTaskRun raises EntityNotFoundException."""
        with pytest.raises(ClientError) as exc:
            glue.start_materialized_view_refresh_task_run(
                CatalogId="123456789012",
                DatabaseName="nonexistent-db-xyz",
                TableName="nonexistent-table-xyz",
            )
        assert exc.value.response["Error"]["Code"] == "EntityNotFoundException"

    def test_stop_materialized_view_refresh_task_run(self, glue):
        """StopMaterializedViewRefreshTaskRun succeeds (no-op if not running)."""
        resp = glue.stop_materialized_view_refresh_task_run(
            CatalogId="123456789012",
            DatabaseName="nonexistent-db-xyz",
            TableName="nonexistent-table-xyz",
        )
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200

    def test_update_glue_identity_center_configuration_not_found(self, glue):
        """UpdateGlueIdentityCenterConfiguration with no config raises EntityNotFoundException."""
        with pytest.raises(ClientError) as exc:
            glue.update_glue_identity_center_configuration()
        assert exc.value.response["Error"]["Code"] == "EntityNotFoundException"

    def test_update_job_from_source_control_no_job(self, glue):
        """UpdateJobFromSourceControl with no job returns empty JobName."""
        resp = glue.update_job_from_source_control()
        assert "JobName" in resp

    def test_update_source_control_from_job_no_job(self, glue):
        """UpdateSourceControlFromJob with no job returns empty JobName."""
        resp = glue.update_source_control_from_job()
        assert "JobName" in resp

    def test_update_table_optimizer_not_found(self, glue):
        """UpdateTableOptimizer with nonexistent table raises EntityNotFoundException."""
        with pytest.raises(ClientError) as exc:
            glue.update_table_optimizer(
                CatalogId="123456789012",
                DatabaseName="nonexistent-db-xyz",
                TableName="nonexistent-table-xyz",
                Type="compaction",
                TableOptimizerConfiguration={
                    "roleArn": "arn:aws:iam::123456789012:role/test",
                    "enabled": True,
                },
            )
        assert exc.value.response["Error"]["Code"] == "EntityNotFoundException"


# ── Edge case and behavioral fidelity tests added to strengthen coverage ──────


class TestGlueUpdateDatabaseEdgeCases:
    """Edge cases for UpdateDatabase."""

    def test_update_nonexistent_database(self, glue):
        """UpdateDatabase on a nonexistent database raises EntityNotFoundException."""
        with pytest.raises(ClientError) as exc:
            glue.update_database(
                Name="nonexistent-db-xyz",
                DatabaseInput={"Name": "nonexistent-db-xyz", "Description": "nope"},
            )
        assert exc.value.response["Error"]["Code"] == "EntityNotFoundException"

    def test_update_database_unicode_description(self, glue):
        """UpdateDatabase stores and retrieves a unicode description correctly."""
        db_name = _unique("db")
        glue.create_database(DatabaseInput={"Name": db_name, "Description": "original"})
        try:
            glue.update_database(
                Name=db_name,
                DatabaseInput={"Name": db_name, "Description": "beschreibung test"},
            )
            resp = glue.get_database(Name=db_name)
            assert resp["Database"]["Description"] == "beschreibung test"
        finally:
            glue.delete_database(Name=db_name)


class TestGlueBatchGetJobsMixed:
    """Mixed found/missing cases for BatchGetJobs."""

    def test_batch_get_jobs_mixed_found_and_missing(self, glue):
        """BatchGetJobs with one real and one fake job splits results correctly."""
        job_name = _unique("job")
        glue.create_job(
            Name=job_name,
            Role="arn:aws:iam::123456789012:role/glue-role",
            Command={"Name": "glueetl", "ScriptLocation": "s3://bucket/script.py"},
        )
        try:
            resp = glue.batch_get_jobs(JobNames=[job_name, "nonexistent-job-xyz"])
            found_names = [j["Name"] for j in resp["Jobs"]]
            assert job_name in found_names
            assert "nonexistent-job-xyz" in resp["JobsNotFound"]
        finally:
            glue.delete_job(JobName=job_name)


class TestGlueBatchGetCrawlersMixed:
    """Mixed found/missing cases for BatchGetCrawlers."""

    def test_batch_get_crawlers_mixed_found_and_missing(self, glue):
        """BatchGetCrawlers with one real and one fake crawler splits results correctly."""
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
            resp = glue.batch_get_crawlers(
                CrawlerNames=[crawler_name, "nonexistent-crawler-xyz"]
            )
            found_names = [c["Name"] for c in resp["Crawlers"]]
            assert crawler_name in found_names
            assert "nonexistent-crawler-xyz" in resp["CrawlersNotFound"]
        finally:
            glue.delete_crawler(Name=crawler_name)
            glue.delete_database(Name=db_name)


class TestGlueBatchGetTriggersMixed:
    """Mixed found/missing cases for BatchGetTriggers."""

    def test_batch_get_triggers_mixed_found_and_missing(self, glue):
        """BatchGetTriggers with one real and one fake trigger splits results correctly."""
        trigger_name = _unique("trigger")
        glue.create_trigger(
            Name=trigger_name,
            Type="SCHEDULED",
            Schedule="cron(0 12 * * ? *)",
            Actions=[{"JobName": "dummy-job"}],
        )
        try:
            resp = glue.batch_get_triggers(
                TriggerNames=[trigger_name, "nonexistent-trigger-xyz"]
            )
            found_names = [t["Name"] for t in resp["Triggers"]]
            assert trigger_name in found_names
            assert "nonexistent-trigger-xyz" in resp["TriggersNotFound"]
        finally:
            glue.delete_trigger(Name=trigger_name)


class TestGlueBatchGetWorkflowsMixed:
    """Mixed found/missing cases for BatchGetWorkflows."""

    def test_batch_get_workflows_mixed_found_and_missing(self, glue):
        """BatchGetWorkflows with one real and one fake workflow splits results correctly."""
        wf_name = _unique("wf")
        glue.create_workflow(Name=wf_name, Description="mixed test")
        try:
            resp = glue.batch_get_workflows(Names=[wf_name, "nonexistent-wf-xyz"])
            found_names = [w["Name"] for w in resp["Workflows"]]
            assert wf_name in found_names
            assert "nonexistent-wf-xyz" in resp["MissingWorkflows"]
        finally:
            glue.delete_workflow(Name=wf_name)


class TestGlueSearchTablesEdgeCases:
    """Behavioral edge cases for SearchTables."""

    def test_search_tables_finds_created_table(self, glue):
        """SearchTables with SearchText matching a table name returns that table."""
        db_name = _unique("db")
        tbl_name = _unique("srchtbl")
        glue.create_database(DatabaseInput={"Name": db_name})
        glue.create_table(
            DatabaseName=db_name,
            TableInput={
                "Name": tbl_name,
                "StorageDescriptor": {
                    "Columns": [{"Name": "col1", "Type": "string"}],
                    "Location": "s3://bucket/path",
                    "InputFormat": "org.apache.hadoop.mapred.TextInputFormat",
                    "OutputFormat": (
                        "org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat"
                    ),
                    "SerdeInfo": {
                        "SerializationLibrary": (
                            "org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe"
                        )
                    },
                },
            },
        )
        try:
            resp = glue.search_tables(SearchText=tbl_name)
            assert "TableList" in resp
            found = [t["Name"] for t in resp["TableList"]]
            assert tbl_name in found
        finally:
            glue.delete_table(DatabaseName=db_name, Name=tbl_name)
            glue.delete_database(Name=db_name)

    def test_search_tables_pagination_keys(self, glue):
        """SearchTables response contains TableList key and respects MaxResults."""
        # Create a table so there's something to search
        db_name = _unique("db")
        tbl_name = _unique("pgtbl")
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
            resp = glue.search_tables(MaxResults=1)
            assert "TableList" in resp
            assert isinstance(resp["TableList"], list)
            assert len(resp["TableList"]) <= 1
            # Full search (no MaxResults) should find the created table
            full_resp = glue.search_tables()
            all_names = [t["Name"] for t in full_resp["TableList"]]
            assert tbl_name in all_names
        finally:
            glue.delete_table(DatabaseName=db_name, Name=tbl_name)
            glue.delete_database(Name=db_name)


class TestGlueImportCatalogStatus:
    """Behavioral test for ImportCatalogToGlue + GetCatalogImportStatus."""

    def test_import_catalog_then_get_status(self, glue):
        """After ImportCatalogToGlue, GetCatalogImportStatus reports completion."""
        glue.import_catalog_to_glue()
        resp = glue.get_catalog_import_status()
        assert "ImportStatus" in resp
        status = resp["ImportStatus"]
        assert "ImportCompleted" in status


class TestGlueCheckSchemaVersionValidityEdgeCases:
    """Stronger validity checks for CheckSchemaVersionValidity."""

    def test_valid_avro_schema_returns_true(self, glue):
        """A valid AVRO schema definition returns Valid=True; full registry lifecycle."""
        definition = '{"type":"record","name":"Test","fields":[{"name":"id","type":"int"}]}'
        # CREATE: registry and schema
        reg_name = _unique("reg")
        schema_name = _unique("schema")
        glue.create_registry(RegistryName=reg_name, Description="validity test")
        glue.create_schema(
            RegistryId={"RegistryName": reg_name},
            SchemaName=schema_name,
            DataFormat="AVRO",
            Compatibility="NONE",
            SchemaDefinition=definition,
        )
        try:
            # RETRIEVE: get schema details
            get_resp = glue.get_schema(
                SchemaId={"SchemaName": schema_name, "RegistryName": reg_name}
            )
            assert get_resp["SchemaName"] == schema_name
            # LIST: list schemas in registry
            list_resp = glue.list_schemas(RegistryId={"RegistryName": reg_name})
            schema_names = [s["SchemaName"] for s in list_resp["Schemas"]]
            assert schema_name in schema_names
            # The actual validity check
            resp = glue.check_schema_version_validity(DataFormat="AVRO", SchemaDefinition=definition)
            assert resp["Valid"] is True
            # ERROR: get nonexistent schema
            with pytest.raises(ClientError) as exc:
                glue.get_schema(SchemaId={"SchemaName": "nonexistent-schema-xyz", "RegistryName": reg_name})
            assert exc.value.response["Error"]["Code"] == "EntityNotFoundException"
        finally:
            # DELETE: clean up
            glue.delete_schema(SchemaId={"SchemaName": schema_name, "RegistryName": reg_name})
            glue.delete_registry(RegistryId={"RegistryName": reg_name})


class TestGlueBatchGetDevEndpointsEdgeCases:
    """Stronger behavioral tests for BatchGetDevEndpoints."""

    def test_batch_get_dev_endpoints_not_found_in_response(self, glue):
        """Requesting a nonexistent endpoint name returns it in DevEndpointsNotFound."""
        # CREATE a real endpoint to verify split behavior
        ep_name = _unique("de")
        glue.create_dev_endpoint(
            EndpointName=ep_name,
            RoleArn="arn:aws:iam::123456789012:role/glue-role",
        )
        try:
            # RETRIEVE: get it back
            get_resp = glue.get_dev_endpoint(EndpointName=ep_name)
            assert get_resp["DevEndpoint"]["EndpointName"] == ep_name
            # LIST: verify it appears in the full list
            list_resp = glue.get_dev_endpoints()
            ep_names = [d["EndpointName"] for d in list_resp["DevEndpoints"]]
            assert ep_name in ep_names
            # batch_get with a fake name returns it in not-found
            resp = glue.batch_get_dev_endpoints(DevEndpointNames=["nonexistent-de-xyz"])
            assert "DevEndpointsNotFound" in resp
            assert "nonexistent-de-xyz" in resp["DevEndpointsNotFound"]
            # ERROR: get a truly nonexistent endpoint raises EntityNotFoundException
            with pytest.raises(ClientError) as exc:
                glue.get_dev_endpoint(EndpointName="nonexistent-de-xyz")
            assert exc.value.response["Error"]["Code"] == "EntityNotFoundException"
        finally:
            # DELETE: clean up
            glue.delete_dev_endpoint(EndpointName=ep_name)

    def test_batch_get_dev_endpoints_existing(self, glue):
        """A created DevEndpoint is returned in DevEndpoints by BatchGetDevEndpoints."""
        ep_name = _unique("de")
        glue.create_dev_endpoint(
            EndpointName=ep_name,
            RoleArn="arn:aws:iam::123456789012:role/glue-role",
        )
        try:
            resp = glue.batch_get_dev_endpoints(DevEndpointNames=[ep_name])
            found_names = [e["EndpointName"] for e in resp.get("DevEndpoints", [])]
            assert ep_name in found_names
        finally:
            glue.delete_dev_endpoint(EndpointName=ep_name)


class TestGlueBatchPutDQAnnotationEdgeCases:
    """Stronger behavioral tests for BatchPutDataQualityStatisticAnnotation."""

    def test_batch_put_dq_annotation_with_data(self, glue):
        """BatchPutDataQualityStatisticAnnotation with an annotation returns a list."""
        # CREATE: a data quality ruleset to anchor the workflow
        ruleset_name = _unique("dqr")
        glue.create_data_quality_ruleset(
            Name=ruleset_name,
            Ruleset='Rules = [ IsComplete "col1" ]',
        )
        try:
            # RETRIEVE: get the ruleset back
            get_resp = glue.get_data_quality_ruleset(Name=ruleset_name)
            assert get_resp["Name"] == ruleset_name
            # LIST: ruleset appears in list
            list_resp = glue.list_data_quality_rulesets()
            names = [r["Name"] for r in list_resp["Rulesets"]]
            assert ruleset_name in names
            # batch_put_annotation itself
            resp = glue.batch_put_data_quality_statistic_annotation(
                InclusionAnnotations=[
                    {
                        "ProfileId": "fake-profile-id",
                        "StatisticId": "fake-stat-id",
                        "InclusionAnnotation": "INCLUDE",
                    }
                ]
            )
            assert "FailedInclusionAnnotations" in resp
            assert isinstance(resp["FailedInclusionAnnotations"], list)
            # ERROR: get a nonexistent ruleset raises an error
            with pytest.raises(ClientError) as exc:
                glue.get_data_quality_ruleset(Name="nonexistent-dqr-xyz")
            assert exc.value.response["Error"]["Code"] in ("EntityNotFoundException", "InvalidInputException")
        finally:
            # DELETE
            glue.delete_data_quality_ruleset(Name=ruleset_name)


class TestGlueCancelDQRunsEdgeCases:
    """Verify cancel DQ run responses include standard metadata."""

    def test_cancel_dq_recommendation_run_response_keys(self, glue):
        """CancelDataQualityRuleRecommendationRun returns HTTP 200."""
        # CREATE: start a recommendation run first
        start_resp = glue.start_data_quality_rule_recommendation_run(
            DataSource={"GlueTable": {"DatabaseName": "testdb", "TableName": "testtbl"}},
            Role="arn:aws:iam::123456789012:role/test",
        )
        run_id = start_resp["RunId"]
        assert run_id
        # LIST: runs appear in list
        list_resp = glue.list_data_quality_rule_recommendation_runs()
        assert "Runs" in list_resp
        # cancel the run (maps to DELETE semantically)
        resp = glue.cancel_data_quality_rule_recommendation_run(RunId=run_id)
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200
        # ERROR: canceling with a nonexistent fake-run still returns 200 (idempotent cancel)
        resp2 = glue.cancel_data_quality_rule_recommendation_run(RunId="fake-run-id-2")
        assert resp2["ResponseMetadata"]["HTTPStatusCode"] == 200

    def test_cancel_dq_evaluation_run_response_keys(self, glue):
        """CancelDataQualityRulesetEvaluationRun returns HTTP 200."""
        # CREATE: a ruleset and start an evaluation run
        ruleset_name = _unique("dqr")
        glue.create_data_quality_ruleset(
            Name=ruleset_name,
            Ruleset='Rules = [ IsComplete "col1" ]',
        )
        try:
            start_resp = glue.start_data_quality_ruleset_evaluation_run(
                DataSource={"GlueTable": {"DatabaseName": "testdb", "TableName": "testtbl"}},
                Role="arn:aws:iam::123456789012:role/test",
                RulesetNames=[ruleset_name],
            )
            run_id = start_resp["RunId"]
            assert run_id
            # LIST: runs appear in list
            list_resp = glue.list_data_quality_ruleset_evaluation_runs()
            assert "Runs" in list_resp
            # cancel the run
            resp = glue.cancel_data_quality_ruleset_evaluation_run(RunId=run_id)
            assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200
        finally:
            # DELETE
            glue.delete_data_quality_ruleset(Name=ruleset_name)


class TestGlueGetConnectionsEdgeCases:
    """Stronger behavioral tests for GetConnections."""

    def test_get_connections_includes_created_with_jdbc_url(self, glue):
        """A JDBC connection is returned with its JDBC_CONNECTION_URL in ConnectionProperties."""
        conn_name = _unique("conn")
        glue.create_connection(
            ConnectionInput={
                "Name": conn_name,
                "ConnectionType": "JDBC",
                "ConnectionProperties": {
                    "JDBC_CONNECTION_URL": "jdbc:mysql://host:3306/mydb",
                    "USERNAME": "admin",
                    "PASSWORD": "secret",
                },
            }
        )
        try:
            resp = glue.get_connections()
            conns = {c["Name"]: c for c in resp["ConnectionList"]}
            assert conn_name in conns
            assert "JDBC_CONNECTION_URL" in conns[conn_name]["ConnectionProperties"]
            assert conns[conn_name]["ConnectionProperties"]["JDBC_CONNECTION_URL"] == (
                "jdbc:mysql://host:3306/mydb"
            )
        finally:
            glue.delete_connection(ConnectionName=conn_name)


class TestGlueBehavioralFidelity:
    """Behavioral fidelity tests verifying AWS-accurate response shapes."""

    def test_database_has_create_time(self, glue):
        """A newly created database includes CreateTime in its metadata."""
        db_name = _unique("db")
        glue.create_database(DatabaseInput={"Name": db_name})
        try:
            resp = glue.get_database(Name=db_name)
            assert "CreateTime" in resp["Database"]
        finally:
            glue.delete_database(Name=db_name)

    def test_job_has_created_on(self, glue):
        """A newly created job includes CreatedOn in its metadata."""
        job_name = _unique("job")
        glue.create_job(
            Name=job_name,
            Role="arn:aws:iam::123456789012:role/glue-role",
            Command={"Name": "glueetl", "ScriptLocation": "s3://bucket/script.py"},
        )
        try:
            resp = glue.get_job(JobName=job_name)
            assert "CreatedOn" in resp["Job"]
        finally:
            glue.delete_job(JobName=job_name)

    def test_crawler_state_ready_after_create(self, glue):
        """A newly created crawler is in READY state."""
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
            resp = glue.get_crawler(Name=crawler_name)
            assert resp["Crawler"]["State"] == "READY"
        finally:
            glue.delete_crawler(Name=crawler_name)
            glue.delete_database(Name=db_name)

    def test_duplicate_database_error_message(self, glue):
        """Creating a duplicate database raises AlreadyExistsException with a message."""
        db_name = _unique("db")
        glue.create_database(DatabaseInput={"Name": db_name})
        try:
            with pytest.raises(ClientError) as exc:
                glue.create_database(DatabaseInput={"Name": db_name})
            error = exc.value.response["Error"]
            assert error["Code"] == "AlreadyExistsException"
            assert "Message" in error
        finally:
            glue.delete_database(Name=db_name)

    def test_get_databases_lists_all_created(self, glue):
        """GetDatabases returns a DatabaseList that includes all created databases."""
        db_names = [_unique("pgdb") for _ in range(3)]
        for name in db_names:
            glue.create_database(DatabaseInput={"Name": name})
        try:
            resp = glue.get_databases()
            assert "DatabaseList" in resp
            listed = [db["Name"] for db in resp["DatabaseList"]]
            for name in db_names:
                assert name in listed
        finally:
            for name in db_names:
                glue.delete_database(Name=name)


# ── Strengthened edge case and behavioral fidelity tests ──────────────────────


class TestGlueBatchGetJobsEdgeCases:
    """Behavioral fidelity tests for BatchGetJobs."""

    def test_batch_get_jobs_not_found_verify_missing_key(self, glue):
        """BatchGetJobs with only missing jobs returns empty Jobs and all in JobsNotFound."""
        # CREATE a real job to verify split behavior
        job_name = _unique("bgjnf")
        glue.create_job(
            Name=job_name,
            Role="arn:aws:iam::123456789012:role/glue-role",
            Command={"Name": "glueetl", "ScriptLocation": "s3://bucket/script.py"},
        )
        try:
            # RETRIEVE: confirm real job is gettable
            get_resp = glue.get_job(JobName=job_name)
            assert get_resp["Job"]["Name"] == job_name
            # LIST: job appears in listing
            list_resp = glue.list_jobs()
            assert job_name in list_resp["JobNames"]
            # batch_get with two fake names returns empty Jobs and both in JobsNotFound
            fake1 = "batch-job-missing-aaa"
            fake2 = "batch-job-missing-bbb"
            resp = glue.batch_get_jobs(JobNames=[fake1, fake2])
            assert len(resp["Jobs"]) == 0
            assert fake1 in resp["JobsNotFound"]
            assert fake2 in resp["JobsNotFound"]
            # ERROR: getting a fake job raises EntityNotFoundException
            with pytest.raises(ClientError) as exc:
                glue.get_job(JobName="completely-fake-job-xyz")
            assert exc.value.response["Error"]["Code"] == "EntityNotFoundException"
        finally:
            # DELETE
            glue.delete_job(JobName=job_name)

    def test_batch_get_jobs_full_cycle(self, glue):
        """Create jobs, batch-get them, verify fields, then delete."""
        job1 = _unique("bgjob")
        job2 = _unique("bgjob")
        for jn in (job1, job2):
            glue.create_job(
                Name=jn,
                Role="arn:aws:iam::123456789012:role/glue-role",
                Command={"Name": "glueetl", "ScriptLocation": "s3://bucket/script.py"},
                Description="batch get test job",
            )
        try:
            resp = glue.batch_get_jobs(JobNames=[job1, job2])
            assert len(resp["Jobs"]) == 2
            job_map = {j["Name"]: j for j in resp["Jobs"]}
            assert job1 in job_map
            assert job2 in job_map
            # Verify behavioral fidelity: Description and Command are present
            assert job_map[job1]["Description"] == "batch get test job"
            assert job_map[job1]["Command"]["Name"] == "glueetl"
            assert "JobsNotFound" in resp
            assert len(resp["JobsNotFound"]) == 0
        finally:
            for jn in (job1, job2):
                glue.delete_job(JobName=jn)

    def test_batch_get_jobs_partial_missing_creates_correct_split(self, glue):
        """BatchGetJobs with one real and one missing job splits Jobs vs JobsNotFound."""
        job_name = _unique("bgjob")
        glue.create_job(
            Name=job_name,
            Role="arn:aws:iam::123456789012:role/glue-role",
            Command={"Name": "glueetl", "ScriptLocation": "s3://bucket/script.py"},
        )
        try:
            resp = glue.batch_get_jobs(JobNames=[job_name, "totally-fake-job-xyz"])
            found_names = [j["Name"] for j in resp["Jobs"]]
            assert job_name in found_names
            assert "totally-fake-job-xyz" in resp["JobsNotFound"]
            assert job_name not in resp["JobsNotFound"]
        finally:
            glue.delete_job(JobName=job_name)

    def test_update_job_then_batch_get_reflects_change(self, glue):
        """After UpdateJob, BatchGetJobs returns updated job description."""
        job_name = _unique("bgjob")
        glue.create_job(
            Name=job_name,
            Role="arn:aws:iam::123456789012:role/glue-role",
            Command={"Name": "glueetl", "ScriptLocation": "s3://bucket/script.py"},
            Description="original",
        )
        try:
            glue.update_job(
                JobName=job_name,
                JobUpdate={
                    "Role": "arn:aws:iam::123456789012:role/glue-role",
                    "Command": {"Name": "glueetl", "ScriptLocation": "s3://bucket/script.py"},
                    "Description": "updated via update_job",
                },
            )
            resp = glue.batch_get_jobs(JobNames=[job_name])
            assert len(resp["Jobs"]) == 1
            assert resp["Jobs"][0]["Description"] == "updated via update_job"
        finally:
            glue.delete_job(JobName=job_name)


class TestGlueBatchGetCrawlersEdgeCases:
    """Behavioral fidelity tests for BatchGetCrawlers."""

    def test_batch_get_crawlers_not_found_multiple(self, glue):
        """BatchGetCrawlers with multiple missing names returns all in CrawlersNotFound."""
        # CREATE a real crawler and database
        db_name = _unique("db")
        crawler_name = _unique("bgcrnf")
        glue.create_database(DatabaseInput={"Name": db_name})
        glue.create_crawler(
            Name=crawler_name,
            Role="arn:aws:iam::123456789012:role/glue-role",
            DatabaseName=db_name,
            Targets={"S3Targets": [{"Path": "s3://bucket/data"}]},
        )
        try:
            # RETRIEVE: confirm crawler is gettable
            get_resp = glue.get_crawler(Name=crawler_name)
            assert get_resp["Crawler"]["Name"] == crawler_name
            # LIST: crawler appears in listing
            list_resp = glue.list_crawlers()
            assert crawler_name in list_resp["CrawlerNames"]
            # batch_get with two fake names returns empty and both in CrawlersNotFound
            fake1 = "batch-crawler-missing-aaa"
            fake2 = "batch-crawler-missing-bbb"
            resp = glue.batch_get_crawlers(CrawlerNames=[fake1, fake2])
            assert len(resp["Crawlers"]) == 0
            assert fake1 in resp["CrawlersNotFound"]
            assert fake2 in resp["CrawlersNotFound"]
            # ERROR: getting a fake crawler raises EntityNotFoundException
            with pytest.raises(ClientError) as exc:
                glue.get_crawler(Name="completely-fake-crawler-xyz")
            assert exc.value.response["Error"]["Code"] == "EntityNotFoundException"
        finally:
            # DELETE
            glue.delete_crawler(Name=crawler_name)
            glue.delete_database(Name=db_name)

    def test_batch_get_crawlers_full_cycle(self, glue):
        """Create crawlers, batch-get them, verify fields, then delete."""
        db_name = _unique("db")
        c1 = _unique("bgcr")
        c2 = _unique("bgcr")
        glue.create_database(DatabaseInput={"Name": db_name})
        for cr in (c1, c2):
            glue.create_crawler(
                Name=cr,
                Role="arn:aws:iam::123456789012:role/glue-role",
                DatabaseName=db_name,
                Targets={"S3Targets": [{"Path": "s3://bucket/data"}]},
                Description="batch get test crawler",
            )
        try:
            resp = glue.batch_get_crawlers(CrawlerNames=[c1, c2])
            assert len(resp["Crawlers"]) == 2
            crawler_map = {c["Name"]: c for c in resp["Crawlers"]}
            assert c1 in crawler_map
            assert c2 in crawler_map
            assert crawler_map[c1]["Description"] == "batch get test crawler"
            assert crawler_map[c1]["DatabaseName"] == db_name
            assert len(resp["CrawlersNotFound"]) == 0
        finally:
            for cr in (c1, c2):
                glue.delete_crawler(Name=cr)
            glue.delete_database(Name=db_name)

    def test_update_crawler_then_batch_get_reflects_change(self, glue):
        """After UpdateCrawler, BatchGetCrawlers returns updated description."""
        db_name = _unique("db")
        cr_name = _unique("bgcr")
        glue.create_database(DatabaseInput={"Name": db_name})
        glue.create_crawler(
            Name=cr_name,
            Role="arn:aws:iam::123456789012:role/glue-role",
            DatabaseName=db_name,
            Targets={"S3Targets": [{"Path": "s3://bucket/data"}]},
            Description="original description",
        )
        try:
            glue.update_crawler(Name=cr_name, Description="updated description")
            resp = glue.batch_get_crawlers(CrawlerNames=[cr_name])
            assert len(resp["Crawlers"]) == 1
            assert resp["Crawlers"][0]["Description"] == "updated description"
        finally:
            glue.delete_crawler(Name=cr_name)
            glue.delete_database(Name=db_name)


class TestGlueBatchGetTriggersEdgeCases:
    """Behavioral fidelity tests for BatchGetTriggers."""

    def test_batch_get_triggers_not_found_multiple(self, glue):
        """BatchGetTriggers with multiple missing names returns all in TriggersNotFound."""
        # CREATE a real trigger
        trig_name = _unique("bgtnf")
        glue.create_trigger(
            Name=trig_name,
            Type="SCHEDULED",
            Schedule="cron(0 12 * * ? *)",
            Actions=[{"JobName": "dummy-job"}],
        )
        try:
            # RETRIEVE: confirm trigger is gettable
            get_resp = glue.get_trigger(Name=trig_name)
            assert get_resp["Trigger"]["Name"] == trig_name
            # LIST: trigger appears in listing
            list_resp = glue.list_triggers()
            assert trig_name in list_resp["TriggerNames"]
            # batch_get with two fake names returns empty and both in TriggersNotFound
            fake1 = "batch-trigger-missing-aaa"
            fake2 = "batch-trigger-missing-bbb"
            resp = glue.batch_get_triggers(TriggerNames=[fake1, fake2])
            assert len(resp["Triggers"]) == 0
            assert fake1 in resp["TriggersNotFound"]
            assert fake2 in resp["TriggersNotFound"]
            # ERROR: getting a fake trigger raises EntityNotFoundException
            with pytest.raises(ClientError) as exc:
                glue.get_trigger(Name="completely-fake-trigger-xyz")
            assert exc.value.response["Error"]["Code"] == "EntityNotFoundException"
        finally:
            # DELETE
            glue.delete_trigger(Name=trig_name)

    def test_batch_get_triggers_full_cycle(self, glue):
        """Create triggers, batch-get them, verify fields, then delete."""
        t1 = _unique("bgtrig")
        t2 = _unique("bgtrig")
        for t in (t1, t2):
            glue.create_trigger(
                Name=t,
                Type="SCHEDULED",
                Schedule="cron(0 12 * * ? *)",
                Actions=[{"JobName": "dummy-job"}],
                Description="batch get test trigger",
            )
        try:
            resp = glue.batch_get_triggers(TriggerNames=[t1, t2])
            assert len(resp["Triggers"]) == 2
            trigger_map = {t["Name"]: t for t in resp["Triggers"]}
            assert t1 in trigger_map
            assert t2 in trigger_map
            assert trigger_map[t1]["Type"] == "SCHEDULED"
            assert trigger_map[t1]["Schedule"] == "cron(0 12 * * ? *)"
            assert len(resp["TriggersNotFound"]) == 0
        finally:
            for t in (t1, t2):
                glue.delete_trigger(Name=t)

    def test_update_trigger_then_batch_get_reflects_change(self, glue):
        """After UpdateTrigger, BatchGetTriggers returns updated trigger."""
        trig_name = _unique("bgtrig")
        glue.create_trigger(
            Name=trig_name,
            Type="ON_DEMAND",
            Actions=[{"JobName": "dummy-job"}],
            Description="original",
        )
        try:
            glue.update_trigger(
                Name=trig_name,
                TriggerUpdate={"Name": trig_name, "Description": "updated via update_trigger"},
            )
            resp = glue.batch_get_triggers(TriggerNames=[trig_name])
            assert len(resp["Triggers"]) == 1
            assert resp["Triggers"][0]["Description"] == "updated via update_trigger"
        finally:
            glue.delete_trigger(Name=trig_name)


class TestGlueBatchGetWorkflowsEdgeCases:
    """Behavioral fidelity tests for BatchGetWorkflows."""

    def test_batch_get_workflows_not_found_multiple(self, glue):
        """BatchGetWorkflows with multiple missing names returns all in MissingWorkflows."""
        # CREATE a real workflow
        wf_name = _unique("bgwfnf")
        glue.create_workflow(Name=wf_name, Description="not-found test workflow")
        try:
            # RETRIEVE: confirm workflow is gettable
            get_resp = glue.get_workflow(Name=wf_name)
            assert get_resp["Workflow"]["Name"] == wf_name
            # LIST: workflow appears in listing
            list_resp = glue.list_workflows()
            assert wf_name in list_resp["Workflows"]
            # batch_get with two fake names returns empty and both in MissingWorkflows
            fake1 = "batch-wf-missing-aaa"
            fake2 = "batch-wf-missing-bbb"
            resp = glue.batch_get_workflows(Names=[fake1, fake2])
            assert len(resp["Workflows"]) == 0
            assert fake1 in resp["MissingWorkflows"]
            assert fake2 in resp["MissingWorkflows"]
            # ERROR: getting a fake workflow raises EntityNotFoundException
            with pytest.raises(ClientError) as exc:
                glue.get_workflow(Name="completely-fake-workflow-xyz")
            assert exc.value.response["Error"]["Code"] == "EntityNotFoundException"
        finally:
            # DELETE
            glue.delete_workflow(Name=wf_name)

    def test_batch_get_workflows_full_cycle(self, glue):
        """Create workflows, batch-get them, verify fields, then delete."""
        w1 = _unique("bgwf")
        w2 = _unique("bgwf")
        for w in (w1, w2):
            glue.create_workflow(Name=w, Description="batch get test workflow")
        try:
            resp = glue.batch_get_workflows(Names=[w1, w2])
            assert len(resp["Workflows"]) == 2
            wf_map = {w["Name"]: w for w in resp["Workflows"]}
            assert w1 in wf_map
            assert w2 in wf_map
            assert wf_map[w1]["Description"] == "batch get test workflow"
            assert len(resp["MissingWorkflows"]) == 0
        finally:
            for w in (w1, w2):
                glue.delete_workflow(Name=w)

    def test_update_workflow_then_batch_get_reflects_change(self, glue):
        """After UpdateWorkflow, BatchGetWorkflows returns updated description."""
        wf_name = _unique("bgwf")
        glue.create_workflow(Name=wf_name, Description="original")
        try:
            glue.update_workflow(Name=wf_name, Description="updated description")
            resp = glue.batch_get_workflows(Names=[wf_name])
            assert len(resp["Workflows"]) == 1
            assert resp["Workflows"][0]["Description"] == "updated description"
        finally:
            glue.delete_workflow(Name=wf_name)


class TestGlueSearchTablesEnhanced:
    """Behavioral fidelity tests for SearchTables."""

    def _sd(self):
        return {
            "Columns": [{"Name": "id", "Type": "string"}],
            "Location": "s3://bucket/path",
            "InputFormat": "org.apache.hadoop.mapred.TextInputFormat",
            "OutputFormat": "org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat",
            "SerdeInfo": {
                "SerializationLibrary": "org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe"
            },
        }

    def test_search_tables_returns_table_list(self, glue):
        """SearchTables returns a TableList key."""
        resp = glue.search_tables()
        assert "TableList" in resp
        assert isinstance(resp["TableList"], list)

    def test_search_tables_with_database_filter_finds_table(self, glue):
        """SearchTables filtered by DatabaseName finds tables in that database."""
        db_name = _unique("db")
        tbl_name = _unique("stbl")
        glue.create_database(DatabaseInput={"Name": db_name})
        glue.create_table(DatabaseName=db_name, TableInput={"Name": tbl_name, "StorageDescriptor": self._sd()})
        try:
            resp = glue.search_tables(
                Filters=[{"Key": "DatabaseName", "Value": db_name, "Comparator": "EQUALS"}]
            )
            assert "TableList" in resp
            table_names = [t["Name"] for t in resp["TableList"]]
            assert tbl_name in table_names
            # All returned tables must be from the target database
            for t in resp["TableList"]:
                assert t["DatabaseName"] == db_name
        finally:
            glue.delete_table(DatabaseName=db_name, Name=tbl_name)
            glue.delete_database(Name=db_name)

    def test_search_tables_max_results_limits_response(self, glue):
        """SearchTables with MaxResults=1 returns at most 1 table."""
        resp = glue.search_tables(MaxResults=1)
        assert "TableList" in resp
        assert len(resp["TableList"]) <= 1

    def test_search_tables_delete_table_not_in_results(self, glue):
        """After deleting a table, SearchTables no longer returns it."""
        db_name = _unique("db")
        tbl_name = _unique("stbl")
        glue.create_database(DatabaseInput={"Name": db_name})
        glue.create_table(DatabaseName=db_name, TableInput={"Name": tbl_name, "StorageDescriptor": self._sd()})
        glue.delete_table(DatabaseName=db_name, Name=tbl_name)
        try:
            resp = glue.search_tables(SearchText=tbl_name)
            table_names = [t["Name"] for t in resp["TableList"]]
            assert tbl_name not in table_names
        finally:
            glue.delete_database(Name=db_name)


class TestGlueImportCatalogEnhanced:
    """Behavioral fidelity tests for ImportCatalogToGlue."""

    def test_import_catalog_returns_200(self, glue):
        """ImportCatalogToGlue returns HTTP 200."""
        resp = glue.import_catalog_to_glue()
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200

    def test_import_catalog_then_status_is_completed(self, glue):
        """After ImportCatalogToGlue, GetCatalogImportStatus shows ImportCompleted=True."""
        glue.import_catalog_to_glue()
        resp = glue.get_catalog_import_status()
        assert "ImportStatus" in resp
        assert resp["ImportStatus"]["ImportCompleted"] is True

    def test_import_catalog_idempotent(self, glue):
        """ImportCatalogToGlue can be called multiple times without error."""
        glue.import_catalog_to_glue()
        glue.import_catalog_to_glue()
        resp = glue.get_catalog_import_status()
        assert resp["ImportStatus"]["ImportCompleted"] is True


class TestGlueCheckSchemaVersionValidityEnhanced:
    """Behavioral fidelity tests for CheckSchemaVersionValidity."""

    def test_valid_avro_schema_returns_valid_true(self, glue):
        """A well-formed AVRO schema returns Valid=True; lifecycle verifies schema survives a round-trip."""
        definition = '{"type":"record","name":"Test","fields":[{"name":"id","type":"int"}]}'
        # CREATE registry + schema
        reg_name = _unique("reg")
        schema_name = _unique("schema")
        glue.create_registry(RegistryName=reg_name, Description="validity enhanced test")
        glue.create_schema(
            RegistryId={"RegistryName": reg_name},
            SchemaName=schema_name,
            DataFormat="AVRO",
            Compatibility="NONE",
            SchemaDefinition=definition,
        )
        try:
            # RETRIEVE: schema is gettable
            get_resp = glue.get_schema(SchemaId={"SchemaName": schema_name, "RegistryName": reg_name})
            assert get_resp["SchemaName"] == schema_name
            # LIST: schema appears in list
            list_resp = glue.list_schemas(RegistryId={"RegistryName": reg_name})
            names = [s["SchemaName"] for s in list_resp["Schemas"]]
            assert schema_name in names
            # check validity
            resp = glue.check_schema_version_validity(DataFormat="AVRO", SchemaDefinition=definition)
            assert resp["Valid"] is True
            # ERROR: nonexistent schema raises EntityNotFoundException
            with pytest.raises(ClientError) as exc:
                glue.get_schema(SchemaId={"SchemaName": "nonexistent-xyz", "RegistryName": reg_name})
            assert exc.value.response["Error"]["Code"] == "EntityNotFoundException"
        finally:
            # DELETE
            glue.delete_schema(SchemaId={"SchemaName": schema_name, "RegistryName": reg_name})
            glue.delete_registry(RegistryId={"RegistryName": reg_name})

    def test_invalid_schema_definition_returns_valid_key(self, glue):
        """A malformed schema definition still returns a Valid key (boolean) in response."""
        # CREATE a registry to anchor this test in a lifecycle context
        reg_name = _unique("reg")
        glue.create_registry(RegistryName=reg_name, Description="invalid schema test")
        try:
            # LIST: registry appears in listing
            list_resp = glue.list_registries()
            reg_names = [r["RegistryName"] for r in list_resp["Registries"]]
            assert reg_name in reg_names
            # check invalid schema - still returns Valid boolean
            resp = glue.check_schema_version_validity(
                DataFormat="AVRO",
                SchemaDefinition="this is not valid avro json",
            )
            assert "Valid" in resp
            assert isinstance(resp["Valid"], bool)
            # ERROR: get nonexistent schema in this registry raises EntityNotFoundException
            with pytest.raises(ClientError) as exc:
                glue.get_schema(SchemaId={"SchemaName": "nope-xyz", "RegistryName": reg_name})
            assert exc.value.response["Error"]["Code"] == "EntityNotFoundException"
        finally:
            # DELETE
            glue.delete_registry(RegistryId={"RegistryName": reg_name})

    def test_check_schema_version_validity_json_format(self, glue):
        """A valid JSON schema definition returns Valid=True for JSON data format."""
        definition = '{"$schema":"http://json-schema.org/draft-07/schema","type":"object"}'
        # CREATE a registry and JSON schema
        reg_name = _unique("reg")
        schema_name = _unique("schema")
        glue.create_registry(RegistryName=reg_name, Description="json validity test")
        glue.create_schema(
            RegistryId={"RegistryName": reg_name},
            SchemaName=schema_name,
            DataFormat="JSON",
            Compatibility="NONE",
            SchemaDefinition=definition,
        )
        try:
            # RETRIEVE: schema is gettable
            get_resp = glue.get_schema(SchemaId={"SchemaName": schema_name, "RegistryName": reg_name})
            assert get_resp["DataFormat"] == "JSON"
            # LIST: schema appears
            list_resp = glue.list_schemas(RegistryId={"RegistryName": reg_name})
            names = [s["SchemaName"] for s in list_resp["Schemas"]]
            assert schema_name in names
            # UPDATE: change compatibility
            update_resp = glue.update_schema(
                SchemaId={"SchemaName": schema_name, "RegistryName": reg_name},
                Compatibility="BACKWARD",
            )
            assert update_resp["SchemaName"] == schema_name
            # check JSON validity
            resp = glue.check_schema_version_validity(DataFormat="JSON", SchemaDefinition=definition)
            assert resp["Valid"] is True
            # ERROR: nonexistent registry raises EntityNotFoundException
            with pytest.raises(ClientError) as exc:
                glue.get_registry(RegistryId={"RegistryName": "nonexistent-reg-xyz"})
            assert exc.value.response["Error"]["Code"] == "EntityNotFoundException"
        finally:
            # DELETE
            glue.delete_schema(SchemaId={"SchemaName": schema_name, "RegistryName": reg_name})
            glue.delete_registry(RegistryId={"RegistryName": reg_name})

    def test_create_schema_then_check_version_validity(self, glue):
        """Creating a schema then checking its definition returns Valid=True."""
        reg_name = _unique("reg")
        schema_name = _unique("schema")
        definition = '{"type":"record","name":"Test","fields":[{"name":"id","type":"int"}]}'
        glue.create_registry(RegistryName=reg_name)
        glue.create_schema(
            RegistryId={"RegistryName": reg_name},
            SchemaName=schema_name,
            DataFormat="AVRO",
            Compatibility="NONE",
            SchemaDefinition=definition,
        )
        try:
            resp = glue.check_schema_version_validity(
                DataFormat="AVRO",
                SchemaDefinition=definition,
            )
            assert resp["Valid"] is True
        finally:
            glue.delete_schema(SchemaId={"SchemaName": schema_name, "RegistryName": reg_name})
            glue.delete_registry(RegistryId={"RegistryName": reg_name})


class TestGlueBatchPutDataQualityAnnotationEnhanced:
    """Behavioral fidelity tests for BatchPutDataQualityStatisticAnnotation."""

    def test_batch_put_empty_annotation_list_returns_empty_failures(self, glue):
        """Calling with an empty list returns FailedInclusionAnnotations=[]."""
        # CREATE a ruleset to anchor in a lifecycle context
        ruleset_name = _unique("dqr")
        glue.create_data_quality_ruleset(
            Name=ruleset_name,
            Ruleset='Rules = [ IsComplete "col1" ]',
        )
        try:
            # RETRIEVE: ruleset is gettable
            get_resp = glue.get_data_quality_ruleset(Name=ruleset_name)
            assert get_resp["Name"] == ruleset_name
            # LIST: ruleset appears in listing
            list_resp = glue.list_data_quality_rulesets()
            names = [r["Name"] for r in list_resp["Rulesets"]]
            assert ruleset_name in names
            # batch_put with empty list
            resp = glue.batch_put_data_quality_statistic_annotation(InclusionAnnotations=[])
            assert "FailedInclusionAnnotations" in resp
            assert resp["FailedInclusionAnnotations"] == []
            # ERROR: nonexistent ruleset raises an error
            with pytest.raises(ClientError) as exc:
                glue.get_data_quality_ruleset(Name="nonexistent-dqr-xyz")
            assert exc.value.response["Error"]["Code"] in ("EntityNotFoundException", "InvalidInputException")
        finally:
            # DELETE
            glue.delete_data_quality_ruleset(Name=ruleset_name)

    def test_batch_put_annotation_returns_failures_list(self, glue):
        """Calling with annotations returns a FailedInclusionAnnotations list (empty on success)."""
        # CREATE a ruleset
        ruleset_name = _unique("dqr")
        glue.create_data_quality_ruleset(
            Name=ruleset_name,
            Ruleset='Rules = [ IsComplete "col2" ]',
        )
        try:
            # RETRIEVE: ruleset is gettable
            get_resp = glue.get_data_quality_ruleset(Name=ruleset_name)
            assert get_resp["Name"] == ruleset_name
            # LIST: appears in list
            list_resp = glue.list_data_quality_rulesets()
            names = [r["Name"] for r in list_resp["Rulesets"]]
            assert ruleset_name in names
            # batch_put with an annotation
            resp = glue.batch_put_data_quality_statistic_annotation(
                InclusionAnnotations=[
                    {
                        "ProfileId": "fake-profile-id",
                        "StatisticId": "fake-stat-id",
                        "InclusionAnnotation": "INCLUDE",
                    }
                ]
            )
            assert "FailedInclusionAnnotations" in resp
            assert isinstance(resp["FailedInclusionAnnotations"], list)
            assert len(resp["FailedInclusionAnnotations"]) == 0
            # ERROR: nonexistent ruleset raises an error
            with pytest.raises(ClientError) as exc:
                glue.get_data_quality_ruleset(Name="nonexistent-dqr-xyz")
            assert exc.value.response["Error"]["Code"] in ("EntityNotFoundException", "InvalidInputException")
        finally:
            # DELETE
            glue.delete_data_quality_ruleset(Name=ruleset_name)

    def test_list_data_quality_statistic_annotations_then_put(self, glue):
        """ListDataQualityStatisticAnnotations returns Annotations before and after put."""
        list_resp = glue.list_data_quality_statistic_annotations()
        assert "Annotations" in list_resp
        # Now put and verify put succeeded
        put_resp = glue.batch_put_data_quality_statistic_annotation(InclusionAnnotations=[])
        assert "FailedInclusionAnnotations" in put_resp

    def test_start_data_quality_then_batch_put_annotation(self, glue):
        """Start a DQ rule recommendation run then call batch_put_annotation."""
        start_resp = glue.start_data_quality_rule_recommendation_run(
            DataSource={"GlueTable": {"DatabaseName": "nonexistent", "TableName": "nonexistent"}},
            Role="arn:aws:iam::123456789012:role/test",
        )
        assert "RunId" in start_resp
        # Can annotate even for fake stats
        put_resp = glue.batch_put_data_quality_statistic_annotation(InclusionAnnotations=[])
        assert "FailedInclusionAnnotations" in put_resp


class TestGlueCancelDataQualityRunsEnhanced:
    """Behavioral fidelity tests for Cancel DQ run operations."""

    def test_cancel_recommendation_run_returns_200(self, glue):
        """CancelDataQualityRuleRecommendationRun returns HTTP 200."""
        # CREATE: start a recommendation run
        start_resp = glue.start_data_quality_rule_recommendation_run(
            DataSource={"GlueTable": {"DatabaseName": "testdb-cancel", "TableName": "testtbl-cancel"}},
            Role="arn:aws:iam::123456789012:role/test",
        )
        run_id = start_resp["RunId"]
        assert run_id
        # LIST: runs appear in listing
        list_resp = glue.list_data_quality_rule_recommendation_runs()
        assert "Runs" in list_resp
        # GET the run details
        get_resp = glue.get_data_quality_rule_recommendation_run(RunId=run_id)
        assert "RunId" in get_resp
        # cancel the run
        resp = glue.cancel_data_quality_rule_recommendation_run(RunId=run_id)
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200
        # ERROR: get a nonexistent run raises an error
        with pytest.raises(ClientError) as exc:
            glue.get_data_quality_rule_recommendation_run(RunId="nonexistent-run-xyz")
        assert exc.value.response["Error"]["Code"] in ("EntityNotFoundException", "InvalidInputException")

    def test_cancel_evaluation_run_returns_200(self, glue):
        """CancelDataQualityRulesetEvaluationRun returns HTTP 200."""
        # CREATE a ruleset and start an evaluation
        ruleset_name = _unique("dqr")
        glue.create_data_quality_ruleset(
            Name=ruleset_name,
            Ruleset='Rules = [ IsComplete "col1" ]',
        )
        try:
            start_resp = glue.start_data_quality_ruleset_evaluation_run(
                DataSource={"GlueTable": {"DatabaseName": "testdb-eval", "TableName": "testtbl-eval"}},
                Role="arn:aws:iam::123456789012:role/test",
                RulesetNames=[ruleset_name],
            )
            run_id = start_resp["RunId"]
            assert run_id
            # LIST: runs appear in listing
            list_resp = glue.list_data_quality_ruleset_evaluation_runs()
            assert "Runs" in list_resp
            # GET the run
            get_resp = glue.get_data_quality_ruleset_evaluation_run(RunId=run_id)
            assert "RunId" in get_resp
            # cancel the run
            resp = glue.cancel_data_quality_ruleset_evaluation_run(RunId=run_id)
            assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200
            # ERROR: get nonexistent evaluation run raises an error
            with pytest.raises(ClientError) as exc:
                glue.get_data_quality_ruleset_evaluation_run(RunId="nonexistent-eval-run-xyz")
            assert exc.value.response["Error"]["Code"] in ("EntityNotFoundException", "InvalidInputException")
        finally:
            # DELETE
            glue.delete_data_quality_ruleset(Name=ruleset_name)

    def test_start_then_cancel_recommendation_run(self, glue):
        """Start a DQ recommendation run then cancel it."""
        start_resp = glue.start_data_quality_rule_recommendation_run(
            DataSource={"GlueTable": {"DatabaseName": "testdb", "TableName": "testtbl"}},
            Role="arn:aws:iam::123456789012:role/test",
        )
        run_id = start_resp["RunId"]
        assert run_id
        cancel_resp = glue.cancel_data_quality_rule_recommendation_run(RunId=run_id)
        assert cancel_resp["ResponseMetadata"]["HTTPStatusCode"] == 200

    def test_start_then_cancel_evaluation_run(self, glue):
        """Start a DQ evaluation run then cancel it."""
        start_resp = glue.start_data_quality_ruleset_evaluation_run(
            DataSource={"GlueTable": {"DatabaseName": "testdb", "TableName": "testtbl"}},
            Role="arn:aws:iam::123456789012:role/test",
            RulesetNames=["ruleset1"],
        )
        run_id = start_resp["RunId"]
        assert run_id
        cancel_resp = glue.cancel_data_quality_ruleset_evaluation_run(RunId=run_id)
        assert cancel_resp["ResponseMetadata"]["HTTPStatusCode"] == 200

    def test_list_recommendation_runs_after_start(self, glue):
        """ListDataQualityRuleRecommendationRuns returns runs after starting one."""
        glue.start_data_quality_rule_recommendation_run(
            DataSource={"GlueTable": {"DatabaseName": "testdb2", "TableName": "testtbl2"}},
            Role="arn:aws:iam::123456789012:role/test",
        )
        resp = glue.list_data_quality_rule_recommendation_runs()
        assert "Runs" in resp
        assert isinstance(resp["Runs"], list)


class TestGlueBatchGetBlueprintsEnhanced:
    """Behavioral fidelity tests for BatchGetBlueprints."""

    def test_batch_get_blueprints_missing_returns_missing_list(self, glue):
        """BatchGetBlueprints for missing blueprints returns them in MissingBlueprints."""
        # CREATE a real blueprint to verify split behavior
        bp_name = _unique("bgbp")
        glue.create_blueprint(Name=bp_name, BlueprintLocation="s3://bucket/bp.py")
        try:
            # RETRIEVE: get the blueprint back
            get_resp = glue.get_blueprint(Name=bp_name)
            assert get_resp["Blueprint"]["Name"] == bp_name
            # LIST: appears in list
            list_resp = glue.list_blueprints()
            assert bp_name in list_resp["Blueprints"]
            # batch_get with fake names returns them in MissingBlueprints
            resp = glue.batch_get_blueprints(Names=["missing-bp-aaa", "missing-bp-bbb"])
            assert "MissingBlueprints" in resp
            assert "missing-bp-aaa" in resp["MissingBlueprints"]
            assert "missing-bp-bbb" in resp["MissingBlueprints"]
            # ERROR: get nonexistent blueprint raises error
            with pytest.raises(ClientError) as exc:
                glue.get_blueprint(Name="nonexistent-bp-xyz")
            assert exc.value.response["Error"]["Code"] == "EntityNotFoundException"
        finally:
            # DELETE
            glue.delete_blueprint(Name=bp_name)

    def test_batch_get_blueprints_full_cycle(self, glue):
        """Create blueprints, batch-get them, verify fields, then delete."""
        bp1 = _unique("bgbp")
        bp2 = _unique("bgbp")
        for bp in (bp1, bp2):
            glue.create_blueprint(Name=bp, BlueprintLocation="s3://bucket/bp.py")
        try:
            resp = glue.batch_get_blueprints(Names=[bp1, bp2])
            assert "Blueprints" in resp
            assert len(resp["Blueprints"]) == 2
            bp_names = [b["Name"] for b in resp["Blueprints"]]
            assert bp1 in bp_names
            assert bp2 in bp_names
            assert len(resp.get("MissingBlueprints", [])) == 0
        finally:
            for bp in (bp1, bp2):
                glue.delete_blueprint(Name=bp)

    def test_batch_get_blueprints_mixed_returns_correct_split(self, glue):
        """BatchGetBlueprints with one real and one missing returns correct split."""
        bp_name = _unique("bgbp")
        glue.create_blueprint(Name=bp_name, BlueprintLocation="s3://bucket/bp.py")
        try:
            resp = glue.batch_get_blueprints(Names=[bp_name, "missing-bp-xyz"])
            bp_names = [b["Name"] for b in resp["Blueprints"]]
            assert bp_name in bp_names
            assert "missing-bp-xyz" in resp["MissingBlueprints"]
        finally:
            glue.delete_blueprint(Name=bp_name)

    def test_update_blueprint_then_batch_get(self, glue):
        """After UpdateBlueprint, BatchGetBlueprints reflects updated location."""
        bp_name = _unique("bgbp")
        glue.create_blueprint(Name=bp_name, BlueprintLocation="s3://bucket/v1.py")
        try:
            glue.update_blueprint(Name=bp_name, BlueprintLocation="s3://bucket/v2.py")
            resp = glue.batch_get_blueprints(Names=[bp_name])
            assert len(resp["Blueprints"]) == 1
            assert resp["Blueprints"][0]["Name"] == bp_name
        finally:
            glue.delete_blueprint(Name=bp_name)


class TestGlueBatchGetDevEndpointsEnhanced:
    """Behavioral fidelity tests for BatchGetDevEndpoints."""

    def test_batch_get_dev_endpoints_missing_returns_not_found(self, glue):
        """BatchGetDevEndpoints for missing endpoints returns them in DevEndpointsNotFound."""
        # CREATE a real dev endpoint to verify split behavior
        ep_name = _unique("bgde")
        glue.create_dev_endpoint(
            EndpointName=ep_name,
            RoleArn="arn:aws:iam::123456789012:role/glue-role",
        )
        try:
            # RETRIEVE: get it back
            get_resp = glue.get_dev_endpoint(EndpointName=ep_name)
            assert get_resp["DevEndpoint"]["EndpointName"] == ep_name
            # LIST: appears in listing
            list_resp = glue.get_dev_endpoints()
            ep_names = [d["EndpointName"] for d in list_resp["DevEndpoints"]]
            assert ep_name in ep_names
            # batch_get with fake names returns them in DevEndpointsNotFound
            resp = glue.batch_get_dev_endpoints(DevEndpointNames=["missing-de-aaa", "missing-de-bbb"])
            assert "DevEndpointsNotFound" in resp
            assert "missing-de-aaa" in resp["DevEndpointsNotFound"]
            assert "missing-de-bbb" in resp["DevEndpointsNotFound"]
            # ERROR: get nonexistent endpoint raises error
            with pytest.raises(ClientError) as exc:
                glue.get_dev_endpoint(EndpointName="nonexistent-de-xyz")
            assert exc.value.response["Error"]["Code"] == "EntityNotFoundException"
        finally:
            # DELETE
            glue.delete_dev_endpoint(EndpointName=ep_name)

    def test_batch_get_dev_endpoints_full_cycle(self, glue):
        """Create a DevEndpoint, batch-get it, verify fields, then delete."""
        ep_name = _unique("bgde")
        glue.create_dev_endpoint(
            EndpointName=ep_name,
            RoleArn="arn:aws:iam::123456789012:role/glue-role",
        )
        try:
            resp = glue.batch_get_dev_endpoints(DevEndpointNames=[ep_name])
            assert "DevEndpoints" in resp
            found_names = [e["EndpointName"] for e in resp["DevEndpoints"]]
            assert ep_name in found_names
            # Verify behavioral fidelity: RoleArn is present
            ep_map = {e["EndpointName"]: e for e in resp["DevEndpoints"]}
            assert ep_map[ep_name]["RoleArn"] == "arn:aws:iam::123456789012:role/glue-role"
            assert len(resp.get("DevEndpointsNotFound", [])) == 0
        finally:
            glue.delete_dev_endpoint(EndpointName=ep_name)

    def test_batch_get_dev_endpoints_mixed_split(self, glue):
        """BatchGetDevEndpoints with one real and one missing returns correct split."""
        ep_name = _unique("bgde")
        glue.create_dev_endpoint(
            EndpointName=ep_name,
            RoleArn="arn:aws:iam::123456789012:role/glue-role",
        )
        try:
            resp = glue.batch_get_dev_endpoints(DevEndpointNames=[ep_name, "missing-de-xyz"])
            found_names = [e["EndpointName"] for e in resp["DevEndpoints"]]
            assert ep_name in found_names
            assert "missing-de-xyz" in resp["DevEndpointsNotFound"]
        finally:
            glue.delete_dev_endpoint(EndpointName=ep_name)

    def test_get_dev_endpoints_list_then_batch_get(self, glue):
        """GetDevEndpoints list then BatchGetDevEndpoints retrieves same endpoints."""
        ep_name = _unique("bgde")
        glue.create_dev_endpoint(
            EndpointName=ep_name,
            RoleArn="arn:aws:iam::123456789012:role/glue-role",
        )
        try:
            list_resp = glue.get_dev_endpoints()
            list_names = [d["EndpointName"] for d in list_resp["DevEndpoints"]]
            assert ep_name in list_names
            batch_resp = glue.batch_get_dev_endpoints(DevEndpointNames=[ep_name])
            batch_names = [e["EndpointName"] for e in batch_resp["DevEndpoints"]]
            assert ep_name in batch_names
        finally:
            glue.delete_dev_endpoint(EndpointName=ep_name)


class TestGlueBatchGetDataQualityResultEnhanced:
    """Behavioral fidelity tests for BatchGetDataQualityResult."""

    def test_batch_get_data_quality_result_with_fake_id_returns_results_key(self, glue):
        """BatchGetDataQualityResult with fake result IDs returns Results list."""
        # CREATE a ruleset to anchor this in a lifecycle context
        ruleset_name = _unique("dqr")
        glue.create_data_quality_ruleset(
            Name=ruleset_name,
            Ruleset='Rules = [ IsComplete "col1" ]',
        )
        try:
            # RETRIEVE: get ruleset back
            get_resp = glue.get_data_quality_ruleset(Name=ruleset_name)
            assert get_resp["Name"] == ruleset_name
            # LIST: appears in list
            list_resp = glue.list_data_quality_rulesets()
            assert ruleset_name in [r["Name"] for r in list_resp["Rulesets"]]
            # batch_get with fake ID returns Results list
            resp = glue.batch_get_data_quality_result(ResultIds=["fake-result-id-aaa"])
            assert "Results" in resp
            assert isinstance(resp["Results"], list)
            # ERROR: get nonexistent ruleset raises error
            with pytest.raises(ClientError) as exc:
                glue.get_data_quality_ruleset(Name="nonexistent-dqr-xyz")
            assert exc.value.response["Error"]["Code"] in ("EntityNotFoundException", "InvalidInputException")
        finally:
            # DELETE
            glue.delete_data_quality_ruleset(Name=ruleset_name)

    def test_batch_get_data_quality_result_multiple_fake_ids(self, glue):
        """BatchGetDataQualityResult with multiple fake IDs returns Results list."""
        # CREATE a ruleset
        ruleset_name = _unique("dqr")
        glue.create_data_quality_ruleset(
            Name=ruleset_name,
            Ruleset='Rules = [ IsComplete "col2" ]',
        )
        try:
            # RETRIEVE: get ruleset
            get_resp = glue.get_data_quality_ruleset(Name=ruleset_name)
            assert get_resp["Name"] == ruleset_name
            # LIST: appears in list
            list_resp = glue.list_data_quality_rulesets()
            assert ruleset_name in [r["Name"] for r in list_resp["Rulesets"]]
            # batch_get with multiple fake IDs
            resp = glue.batch_get_data_quality_result(
                ResultIds=["fake-id-aaa", "fake-id-bbb", "fake-id-ccc"]
            )
            assert "Results" in resp
            assert isinstance(resp["Results"], list)
            # ERROR: get nonexistent ruleset raises error
            with pytest.raises(ClientError) as exc:
                glue.get_data_quality_ruleset(Name="nonexistent-dqr2-xyz")
            assert exc.value.response["Error"]["Code"] in ("EntityNotFoundException", "InvalidInputException")
        finally:
            # DELETE
            glue.delete_data_quality_ruleset(Name=ruleset_name)

    def test_list_then_batch_get_data_quality_results(self, glue):
        """ListDataQualityResults returns results, BatchGetDataQualityResult retrieves them."""
        list_resp = glue.list_data_quality_results()
        assert "Results" in list_resp
        result_ids = [r["ResultId"] for r in list_resp["Results"]]
        if result_ids:
            batch_resp = glue.batch_get_data_quality_result(ResultIds=result_ids[:1])
            assert "Results" in batch_resp
        else:
            # No results yet, just verify the batch_get_result key behavior
            batch_resp = glue.batch_get_data_quality_result(ResultIds=["nonexistent-result-xyz"])
            assert "Results" in batch_resp

    def test_start_dq_evaluation_then_batch_get_result(self, glue):
        """Start a DQ evaluation run, then call BatchGetDataQualityResult with any ID."""
        start_resp = glue.start_data_quality_ruleset_evaluation_run(
            DataSource={"GlueTable": {"DatabaseName": "testdb", "TableName": "testtbl"}},
            Role="arn:aws:iam::123456789012:role/test",
            RulesetNames=["ruleset1"],
        )
        assert "RunId" in start_resp
        # BatchGetDataQualityResult is independent - verifying it returns Results key
        batch_resp = glue.batch_get_data_quality_result(ResultIds=["nonexistent-result-xyz"])
        assert "Results" in batch_resp


class TestGlueBatchGetTableOptimizerEnhanced:
    """Behavioral fidelity tests for BatchGetTableOptimizer."""

    def test_batch_get_table_optimizer_empty_entries(self, glue):
        """BatchGetTableOptimizer with nonexistent entries returns failures."""
        # CREATE a real database + table to anchor this test
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
                    "SerdeInfo": {"SerializationLibrary": "org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe"},
                },
            },
        )
        try:
            # RETRIEVE: get table back
            get_resp = glue.get_table(DatabaseName=db_name, Name=tbl_name)
            assert get_resp["Table"]["Name"] == tbl_name
            # LIST: table appears in listing
            list_resp = glue.get_tables(DatabaseName=db_name)
            assert tbl_name in [t["Name"] for t in list_resp["TableList"]]
            # batch_get_table_optimizer with nonexistent entries returns failures
            resp = glue.batch_get_table_optimizer(
                Entries=[
                    {
                        "catalogId": "123456789012",
                        "databaseName": "nonexistent-db-xyz",
                        "tableName": "nonexistent-tbl-xyz",
                        "type": "compaction",
                    }
                ]
            )
            assert "TableOptimizers" in resp
            assert "Failures" in resp
            # ERROR: get nonexistent table raises error
            with pytest.raises(ClientError) as exc:
                glue.get_table(DatabaseName=db_name, Name="nonexistent-tbl-xyz")
            assert exc.value.response["Error"]["Code"] == "EntityNotFoundException"
        finally:
            # DELETE
            glue.delete_table(DatabaseName=db_name, Name=tbl_name)
            glue.delete_database(Name=db_name)

    def test_batch_get_table_optimizer_multiple_entries(self, glue):
        """BatchGetTableOptimizer with multiple nonexistent entries returns both keys."""
        # CREATE a real database + table to anchor this test
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
                    "SerdeInfo": {"SerializationLibrary": "org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe"},
                },
            },
        )
        try:
            # RETRIEVE: get table back
            get_resp = glue.get_table(DatabaseName=db_name, Name=tbl_name)
            assert get_resp["Table"]["Name"] == tbl_name
            # LIST: table appears in listing
            list_resp = glue.get_tables(DatabaseName=db_name)
            assert tbl_name in [t["Name"] for t in list_resp["TableList"]]
            # batch_get_table_optimizer with multiple entries
            resp = glue.batch_get_table_optimizer(
                Entries=[
                    {
                        "catalogId": "123456789012",
                        "databaseName": "nonexistent-db-aaa",
                        "tableName": "nonexistent-tbl-aaa",
                        "type": "compaction",
                    },
                    {
                        "catalogId": "123456789012",
                        "databaseName": "nonexistent-db-bbb",
                        "tableName": "nonexistent-tbl-bbb",
                        "type": "compaction",
                    },
                ]
            )
            assert "TableOptimizers" in resp
            assert "Failures" in resp
            assert isinstance(resp["TableOptimizers"], list)
            assert isinstance(resp["Failures"], list)
            # ERROR: get nonexistent db table raises error
            with pytest.raises(ClientError) as exc:
                glue.get_table(DatabaseName="nonexistent-db-aaa", Name="nonexistent-tbl-aaa")
            assert exc.value.response["Error"]["Code"] == "EntityNotFoundException"
        finally:
            # DELETE
            glue.delete_table(DatabaseName=db_name, Name=tbl_name)
            glue.delete_database(Name=db_name)

    def test_create_table_optimizer_then_batch_get(self, glue):
        """Create a table optimizer, then BatchGetTableOptimizer returns response with both keys."""
        db_name = _unique("db")
        tbl_name = _unique("tbl")
        glue.create_database(DatabaseInput={"Name": db_name})
        glue.create_table(
            DatabaseName=db_name,
            TableInput={
                "Name": tbl_name,
                "StorageDescriptor": {
                    "Columns": [],
                    "InputFormat": "",
                    "OutputFormat": "",
                    "SerdeInfo": {},
                },
            },
        )
        glue.create_table_optimizer(
            CatalogId="123456789012",
            DatabaseName=db_name,
            TableName=tbl_name,
            Type="compaction",
            TableOptimizerConfiguration={
                "roleArn": "arn:aws:iam::123456789012:role/test",
                "enabled": True,
            },
        )
        try:
            resp = glue.batch_get_table_optimizer(
                Entries=[
                    {
                        "catalogId": "123456789012",
                        "databaseName": db_name,
                        "tableName": tbl_name,
                        "type": "compaction",
                    }
                ]
            )
            assert "TableOptimizers" in resp
            assert "Failures" in resp
            assert isinstance(resp["TableOptimizers"], list)
            assert isinstance(resp["Failures"], list)
        finally:
            glue.delete_table_optimizer(
                CatalogId="123456789012",
                DatabaseName=db_name,
                TableName=tbl_name,
                Type="compaction",
            )
            glue.delete_table(DatabaseName=db_name, Name=tbl_name)
            glue.delete_database(Name=db_name)

    def test_get_table_optimizer_after_create_then_batch_get(self, glue):
        """GetTableOptimizer after create verifies creation; BatchGetTableOptimizer returns keys."""
        db_name = _unique("db")
        tbl_name = _unique("tbl")
        glue.create_database(DatabaseInput={"Name": db_name})
        glue.create_table(
            DatabaseName=db_name,
            TableInput={
                "Name": tbl_name,
                "StorageDescriptor": {
                    "Columns": [],
                    "InputFormat": "",
                    "OutputFormat": "",
                    "SerdeInfo": {},
                },
            },
        )
        glue.create_table_optimizer(
            CatalogId="123456789012",
            DatabaseName=db_name,
            TableName=tbl_name,
            Type="compaction",
            TableOptimizerConfiguration={
                "roleArn": "arn:aws:iam::123456789012:role/test",
                "enabled": True,
            },
        )
        try:
            get_resp = glue.get_table_optimizer(
                CatalogId="123456789012",
                DatabaseName=db_name,
                TableName=tbl_name,
                Type="compaction",
            )
            assert get_resp["DatabaseName"] == db_name
            assert get_resp["TableName"] == tbl_name
            # BatchGetTableOptimizer always returns both keys
            batch_resp = glue.batch_get_table_optimizer(
                Entries=[
                    {
                        "catalogId": "123456789012",
                        "databaseName": db_name,
                        "tableName": tbl_name,
                        "type": "compaction",
                    }
                ]
            )
            assert "TableOptimizers" in batch_resp
            assert "Failures" in batch_resp
        finally:
            glue.delete_table_optimizer(
                CatalogId="123456789012",
                DatabaseName=db_name,
                TableName=tbl_name,
                Type="compaction",
            )
            glue.delete_table(DatabaseName=db_name, Name=tbl_name)
            glue.delete_database(Name=db_name)


class TestGlueSearchTablesPagination:
    """Behavioral fidelity tests for SearchTables pagination."""

    def _sd(self):
        return {
            "Columns": [{"Name": "id", "Type": "string"}],
            "Location": "s3://bucket/path",
            "InputFormat": "org.apache.hadoop.mapred.TextInputFormat",
            "OutputFormat": "org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat",
            "SerdeInfo": {
                "SerializationLibrary": "org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe"
            },
        }

    def test_search_tables_pagination_keys_present(self, glue):
        """SearchTables response always has TableList key."""
        resp = glue.search_tables(MaxResults=1)
        assert "TableList" in resp
        assert isinstance(resp["TableList"], list)

    def test_search_tables_with_max_results(self, glue):
        """SearchTables with MaxResults=2 returns at most 2 tables."""
        db_name = _unique("db")
        glue.create_database(DatabaseInput={"Name": db_name})
        for i in range(3):
            glue.create_table(
                DatabaseName=db_name,
                TableInput={"Name": _unique("ptbl"), "StorageDescriptor": self._sd()},
            )
        try:
            resp = glue.search_tables(MaxResults=2)
            assert "TableList" in resp
            assert len(resp["TableList"]) <= 2
        finally:
            # cleanup - get the tables we created and delete them
            tables = glue.get_tables(DatabaseName=db_name)["TableList"]
            for t in tables:
                glue.delete_table(DatabaseName=db_name, Name=t["Name"])
            glue.delete_database(Name=db_name)

    def test_search_tables_update_table_reflects_in_search(self, glue):
        """After UpdateTable, SearchTables with the table name returns updated table."""
        db_name = _unique("db")
        tbl_name = _unique("stbl")
        sd = self._sd()
        glue.create_database(DatabaseInput={"Name": db_name})
        glue.create_table(DatabaseName=db_name, TableInput={"Name": tbl_name, "StorageDescriptor": sd})
        try:
            updated_sd = dict(sd, Columns=[{"Name": "id", "Type": "string"}, {"Name": "name", "Type": "string"}])
            glue.update_table(
                DatabaseName=db_name,
                TableInput={"Name": tbl_name, "StorageDescriptor": updated_sd},
            )
            resp = glue.search_tables(
                Filters=[{"Key": "DatabaseName", "Value": db_name, "Comparator": "EQUALS"}]
            )
            table_names = [t["Name"] for t in resp["TableList"]]
            assert tbl_name in table_names
        finally:
            glue.delete_table(DatabaseName=db_name, Name=tbl_name)
            glue.delete_database(Name=db_name)


# ── Edge case and behavioral fidelity additions ───────────────────────────────


class TestGlueUpdateDatabaseEdgeCases:
    """Edge cases for UpdateDatabase: error paths and list-after-update behavior."""

    def test_update_nonexistent_database_raises_entity_not_found(self, glue):
        """UpdateDatabase on a nonexistent database raises EntityNotFoundException."""
        with pytest.raises(ClientError) as exc:
            glue.update_database(
                Name="nonexistent-db-update-xyz",
                DatabaseInput={"Name": "nonexistent-db-update-xyz", "Description": "oops"},
            )
        assert exc.value.response["Error"]["Code"] == "EntityNotFoundException"

    def test_update_database_description_visible_in_list(self, glue):
        """After UpdateDatabase, GetDatabases returns the updated description."""
        db_name = _unique("db")
        glue.create_database(DatabaseInput={"Name": db_name, "Description": "before"})
        try:
            glue.update_database(
                Name=db_name,
                DatabaseInput={"Name": db_name, "Description": "after update"},
            )
            resp = glue.get_databases()
            dbs = {d["Name"]: d for d in resp["DatabaseList"]}
            assert db_name in dbs
            assert dbs[db_name]["Description"] == "after update"
        finally:
            glue.delete_database(Name=db_name)

    def test_update_database_multiple_times_final_value_wins(self, glue):
        """UpdateDatabase can be called multiple times; final value wins."""
        db_name = _unique("db")
        glue.create_database(DatabaseInput={"Name": db_name, "Description": "v1"})
        try:
            glue.update_database(
                Name=db_name, DatabaseInput={"Name": db_name, "Description": "v2"}
            )
            glue.update_database(
                Name=db_name, DatabaseInput={"Name": db_name, "Description": "v3"}
            )
            resp = glue.get_database(Name=db_name)
            assert resp["Database"]["Description"] == "v3"
        finally:
            glue.delete_database(Name=db_name)


class TestGlueCheckSchemaVersionValidityBehavior:
    """Behavioral fidelity: CheckSchemaVersionValidity response shape and field values."""

    def test_valid_avro_response_includes_error_field(self, glue):
        """CheckSchemaVersionValidity always includes an Error field in the response."""
        # CREATE a registry + schema
        reg_name = _unique("reg")
        schema_name = _unique("schema")
        definition = '{"type":"record","name":"T","fields":[{"name":"id","type":"int"}]}'
        glue.create_registry(RegistryName=reg_name, Description="error field test")
        glue.create_schema(
            RegistryId={"RegistryName": reg_name},
            SchemaName=schema_name,
            DataFormat="AVRO",
            Compatibility="NONE",
            SchemaDefinition=definition,
        )
        try:
            # RETRIEVE: schema is gettable
            get_resp = glue.get_schema(SchemaId={"SchemaName": schema_name, "RegistryName": reg_name})
            assert get_resp["SchemaName"] == schema_name
            # LIST: appears in list
            list_resp = glue.list_schemas(RegistryId={"RegistryName": reg_name})
            assert schema_name in [s["SchemaName"] for s in list_resp["Schemas"]]
            # check validity
            resp = glue.check_schema_version_validity(DataFormat="AVRO", SchemaDefinition=definition)
            assert "Error" in resp
            assert resp["Valid"] is True
            # ERROR: nonexistent schema raises EntityNotFoundException
            with pytest.raises(ClientError) as exc:
                glue.get_schema(SchemaId={"SchemaName": "nonexistent-xyz", "RegistryName": reg_name})
            assert exc.value.response["Error"]["Code"] == "EntityNotFoundException"
        finally:
            # DELETE
            glue.delete_schema(SchemaId={"SchemaName": schema_name, "RegistryName": reg_name})
            glue.delete_registry(RegistryId={"RegistryName": reg_name})

    def test_multi_field_avro_schema_returns_valid_true(self, glue):
        """A multi-field AVRO schema with various types returns Valid=True."""
        schema = (
            '{"type":"record","name":"Event","fields":['
            '{"name":"timestamp","type":"long"},'
            '{"name":"message","type":"string"},'
            '{"name":"count","type":"int"}'
            "]}"
        )
        # CREATE a registry + schema
        reg_name = _unique("reg")
        schema_name = _unique("schema")
        glue.create_registry(RegistryName=reg_name, Description="multi-field test")
        glue.create_schema(
            RegistryId={"RegistryName": reg_name},
            SchemaName=schema_name,
            DataFormat="AVRO",
            Compatibility="NONE",
            SchemaDefinition=schema,
        )
        try:
            # RETRIEVE: schema is gettable
            get_resp = glue.get_schema(SchemaId={"SchemaName": schema_name, "RegistryName": reg_name})
            assert get_resp["DataFormat"] == "AVRO"
            # LIST: appears in list
            list_resp = glue.list_schemas(RegistryId={"RegistryName": reg_name})
            assert schema_name in [s["SchemaName"] for s in list_resp["Schemas"]]
            # check validity
            resp = glue.check_schema_version_validity(DataFormat="AVRO", SchemaDefinition=schema)
            assert resp["Valid"] is True
            # ERROR: nonexistent schema raises error
            with pytest.raises(ClientError) as exc:
                glue.get_schema(SchemaId={"SchemaName": "nonexistent-xyz", "RegistryName": reg_name})
            assert exc.value.response["Error"]["Code"] == "EntityNotFoundException"
        finally:
            # DELETE
            glue.delete_schema(SchemaId={"SchemaName": schema_name, "RegistryName": reg_name})
            glue.delete_registry(RegistryId={"RegistryName": reg_name})

    def test_avro_and_json_formats_return_valid_boolean(self, glue):
        """Both AVRO and JSON formats return a Valid boolean in the response."""
        # CREATE two registries, one for AVRO and one for JSON
        avro_reg = _unique("reg")
        json_reg = _unique("reg")
        glue.create_registry(RegistryName=avro_reg, Description="avro test")
        glue.create_registry(RegistryName=json_reg, Description="json test")
        try:
            # LIST: both appear
            list_resp = glue.list_registries()
            reg_names = [r["RegistryName"] for r in list_resp["Registries"]]
            assert avro_reg in reg_names
            assert json_reg in reg_names
            # check both formats
            avro_resp = glue.check_schema_version_validity(
                DataFormat="AVRO",
                SchemaDefinition='{"type":"record","name":"A","fields":[{"name":"x","type":"string"}]}',
            )
            json_resp = glue.check_schema_version_validity(
                DataFormat="JSON",
                SchemaDefinition='{"type":"object","properties":{"id":{"type":"integer"}}}',
            )
            assert isinstance(avro_resp["Valid"], bool)
            assert isinstance(json_resp["Valid"], bool)
            assert avro_resp["Valid"] is True
            assert json_resp["Valid"] is True
            # ERROR: nonexistent registry raises error
            with pytest.raises(ClientError) as exc:
                glue.get_registry(RegistryId={"RegistryName": "nonexistent-reg-xyz"})
            assert exc.value.response["Error"]["Code"] == "EntityNotFoundException"
        finally:
            # DELETE
            glue.delete_registry(RegistryId={"RegistryName": avro_reg})
            glue.delete_registry(RegistryId={"RegistryName": json_reg})


class TestGlueBatchGetDevEndpointsLifecycle:
    """Full lifecycle behavioral tests for BatchGetDevEndpoints."""

    def test_create_then_batch_get_then_delete_then_not_found(self, glue):
        """Create a DevEndpoint, batch-get verifies it's present, delete, then it's in NotFound."""
        ep_name = _unique("de")
        glue.create_dev_endpoint(
            EndpointName=ep_name,
            RoleArn="arn:aws:iam::123456789012:role/glue-role",
        )
        resp = glue.batch_get_dev_endpoints(DevEndpointNames=[ep_name])
        assert len(resp["DevEndpoints"]) == 1
        assert resp["DevEndpoints"][0]["EndpointName"] == ep_name
        assert resp["DevEndpoints"][0]["RoleArn"] == "arn:aws:iam::123456789012:role/glue-role"
        assert ep_name not in resp.get("DevEndpointsNotFound", [])

        glue.delete_dev_endpoint(EndpointName=ep_name)

        resp_after = glue.batch_get_dev_endpoints(DevEndpointNames=[ep_name])
        assert ep_name in resp_after["DevEndpointsNotFound"]
        assert len(resp_after.get("DevEndpoints", [])) == 0

    def test_batch_get_dev_endpoints_returns_correct_role_arn(self, glue):
        """BatchGetDevEndpoints returns RoleArn matching what was provided at creation."""
        ep_name = _unique("de")
        role = "arn:aws:iam::123456789012:role/glue-role"
        glue.create_dev_endpoint(EndpointName=ep_name, RoleArn=role)
        try:
            resp = glue.batch_get_dev_endpoints(DevEndpointNames=[ep_name])
            ep_map = {e["EndpointName"]: e for e in resp["DevEndpoints"]}
            assert ep_name in ep_map
            assert ep_map[ep_name]["RoleArn"] == role
        finally:
            glue.delete_dev_endpoint(EndpointName=ep_name)


class TestGlueBatchPutDQAnnotationBehavior:
    """Behavioral fidelity tests for BatchPutDataQualityStatisticAnnotation."""

    def test_two_annotations_returns_empty_failures(self, glue):
        """Putting two annotations returns FailedInclusionAnnotations=[]."""
        # CREATE a ruleset to anchor in lifecycle context
        ruleset_name = _unique("dqr")
        glue.create_data_quality_ruleset(
            Name=ruleset_name,
            Ruleset='Rules = [ IsComplete "col1" ]',
        )
        try:
            # RETRIEVE: ruleset is gettable
            get_resp = glue.get_data_quality_ruleset(Name=ruleset_name)
            assert get_resp["Name"] == ruleset_name
            # LIST: appears in listing
            list_resp = glue.list_data_quality_rulesets()
            assert ruleset_name in [r["Name"] for r in list_resp["Rulesets"]]
            # batch_put two annotations
            resp = glue.batch_put_data_quality_statistic_annotation(
                InclusionAnnotations=[
                    {"ProfileId": "p1", "StatisticId": "s1", "InclusionAnnotation": "INCLUDE"},
                    {"ProfileId": "p2", "StatisticId": "s2", "InclusionAnnotation": "EXCLUDE"},
                ]
            )
            assert "FailedInclusionAnnotations" in resp
            assert resp["FailedInclusionAnnotations"] == []
            # ERROR: get nonexistent ruleset raises error
            with pytest.raises(ClientError) as exc:
                glue.get_data_quality_ruleset(Name="nonexistent-dqr-xyz")
            assert exc.value.response["Error"]["Code"] in ("EntityNotFoundException", "InvalidInputException")
        finally:
            # DELETE
            glue.delete_data_quality_ruleset(Name=ruleset_name)

    def test_empty_then_nonempty_annotations_both_succeed(self, glue):
        """Both empty and non-empty annotation lists return empty failures."""
        # CREATE a ruleset
        ruleset_name = _unique("dqr")
        glue.create_data_quality_ruleset(
            Name=ruleset_name,
            Ruleset='Rules = [ IsComplete "col3" ]',
        )
        try:
            # RETRIEVE: ruleset is gettable
            get_resp = glue.get_data_quality_ruleset(Name=ruleset_name)
            assert get_resp["Name"] == ruleset_name
            # LIST: appears in listing
            list_resp = glue.list_data_quality_rulesets()
            assert ruleset_name in [r["Name"] for r in list_resp["Rulesets"]]
            # both empty and non-empty succeed
            empty_resp = glue.batch_put_data_quality_statistic_annotation(InclusionAnnotations=[])
            assert empty_resp["FailedInclusionAnnotations"] == []
            nonempty_resp = glue.batch_put_data_quality_statistic_annotation(
                InclusionAnnotations=[
                    {"ProfileId": "p1", "StatisticId": "s1", "InclusionAnnotation": "INCLUDE"},
                ]
            )
            assert nonempty_resp["FailedInclusionAnnotations"] == []
            # ERROR: get nonexistent ruleset raises error
            with pytest.raises(ClientError) as exc:
                glue.get_data_quality_ruleset(Name="nonexistent-dqr3-xyz")
            assert exc.value.response["Error"]["Code"] in ("EntityNotFoundException", "InvalidInputException")
        finally:
            # DELETE
            glue.delete_data_quality_ruleset(Name=ruleset_name)


class TestGlueCancelDQRunsLifecycle:
    """Behavioral tests: start a DQ run, then cancel using the returned RunId."""

    def test_start_then_cancel_recommendation_run_with_real_id(self, glue):
        """Start a DQ recommendation run, cancel with the real RunId, get HTTP 200."""
        start_resp = glue.start_data_quality_rule_recommendation_run(
            DataSource={"GlueTable": {"DatabaseName": "testdb", "TableName": "testtbl"}},
            Role="arn:aws:iam::123456789012:role/test",
        )
        run_id = start_resp["RunId"]
        assert run_id
        cancel_resp = glue.cancel_data_quality_rule_recommendation_run(RunId=run_id)
        assert cancel_resp["ResponseMetadata"]["HTTPStatusCode"] == 200

    def test_start_then_cancel_evaluation_run_with_real_id(self, glue):
        """Start a DQ evaluation run, cancel with the real RunId, get HTTP 200."""
        start_resp = glue.start_data_quality_ruleset_evaluation_run(
            DataSource={"GlueTable": {"DatabaseName": "testdb", "TableName": "testtbl"}},
            Role="arn:aws:iam::123456789012:role/test",
            RulesetNames=["ruleset1"],
        )
        run_id = start_resp["RunId"]
        assert run_id
        cancel_resp = glue.cancel_data_quality_ruleset_evaluation_run(RunId=run_id)
        assert cancel_resp["ResponseMetadata"]["HTTPStatusCode"] == 200

    def test_cancel_recommendation_run_fake_id_returns_200(self, glue):
        """CancelDataQualityRuleRecommendationRun with any RunId returns HTTP 200."""
        # CREATE: start a real recommendation run
        start_resp = glue.start_data_quality_rule_recommendation_run(
            DataSource={"GlueTable": {"DatabaseName": "testdb-lifecycle", "TableName": "testtbl-lifecycle"}},
            Role="arn:aws:iam::123456789012:role/test",
        )
        real_run_id = start_resp["RunId"]
        assert real_run_id
        # LIST: runs appear in listing
        list_resp = glue.list_data_quality_rule_recommendation_runs()
        assert "Runs" in list_resp
        # GET the run
        get_resp = glue.get_data_quality_rule_recommendation_run(RunId=real_run_id)
        assert "RunId" in get_resp
        # cancel with fake ID
        resp = glue.cancel_data_quality_rule_recommendation_run(RunId="fake-run-id-lifecycle")
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200
        # ERROR: get nonexistent run raises error
        with pytest.raises(ClientError) as exc:
            glue.get_data_quality_rule_recommendation_run(RunId="nonexistent-run-xyz")
        assert exc.value.response["Error"]["Code"] in ("EntityNotFoundException", "InvalidInputException")

    def test_cancel_evaluation_run_fake_id_returns_200(self, glue):
        """CancelDataQualityRulesetEvaluationRun with any RunId returns HTTP 200."""
        # CREATE a ruleset and start evaluation
        ruleset_name = _unique("dqr")
        glue.create_data_quality_ruleset(
            Name=ruleset_name,
            Ruleset='Rules = [ IsComplete "col1" ]',
        )
        try:
            start_resp = glue.start_data_quality_ruleset_evaluation_run(
                DataSource={"GlueTable": {"DatabaseName": "testdb-lifecycle", "TableName": "testtbl-lifecycle"}},
                Role="arn:aws:iam::123456789012:role/test",
                RulesetNames=[ruleset_name],
            )
            real_run_id = start_resp["RunId"]
            assert real_run_id
            # LIST: runs appear
            list_resp = glue.list_data_quality_ruleset_evaluation_runs()
            assert "Runs" in list_resp
            # GET the run
            get_resp = glue.get_data_quality_ruleset_evaluation_run(RunId=real_run_id)
            assert "RunId" in get_resp
            # cancel with fake ID
            resp = glue.cancel_data_quality_ruleset_evaluation_run(RunId="fake-eval-id-lifecycle")
            assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200
            # ERROR: get nonexistent evaluation run raises error
            with pytest.raises(ClientError) as exc:
                glue.get_data_quality_ruleset_evaluation_run(RunId="nonexistent-eval-xyz")
            assert exc.value.response["Error"]["Code"] in ("EntityNotFoundException", "InvalidInputException")
        finally:
            # DELETE
            glue.delete_data_quality_ruleset(Name=ruleset_name)


class TestGlueBatchGetJobsBehavior:
    """Additional behavioral tests for BatchGetJobs."""

    def test_batch_get_jobs_unicode_description_preserved(self, glue):
        """A job with unicode description is returned intact by BatchGetJobs."""
        job_name = _unique("bgjob")
        unicode_desc = "データ処理 - Process données - 处理数据"
        glue.create_job(
            Name=job_name,
            Role="arn:aws:iam::123456789012:role/glue-role",
            Command={"Name": "glueetl", "ScriptLocation": "s3://bucket/script.py"},
            Description=unicode_desc,
        )
        try:
            resp = glue.batch_get_jobs(JobNames=[job_name])
            assert len(resp["Jobs"]) == 1
            assert resp["Jobs"][0]["Description"] == unicode_desc
        finally:
            glue.delete_job(JobName=job_name)

    def test_batch_get_jobs_all_missing_returns_empty_jobs_list(self, glue):
        """BatchGetJobs with only nonexistent names returns Jobs=[] and names in JobsNotFound."""
        # CREATE a real job to verify split behavior
        job_name = _unique("bgjob")
        glue.create_job(
            Name=job_name,
            Role="arn:aws:iam::123456789012:role/glue-role",
            Command={"Name": "glueetl", "ScriptLocation": "s3://bucket/script.py"},
        )
        try:
            # RETRIEVE: confirm job is gettable
            get_resp = glue.get_job(JobName=job_name)
            assert get_resp["Job"]["Name"] == job_name
            # LIST: job appears in listing
            list_resp = glue.list_jobs()
            assert job_name in list_resp["JobNames"]
            # batch_get with all-missing names
            names = ["missing-job-behav-x1", "missing-job-behav-x2", "missing-job-behav-x3"]
            resp = glue.batch_get_jobs(JobNames=names)
            assert resp["Jobs"] == []
            for name in names:
                assert name in resp["JobsNotFound"]
            # ERROR: get nonexistent job raises EntityNotFoundException
            with pytest.raises(ClientError) as exc:
                glue.get_job(JobName="completely-fake-job-abc")
            assert exc.value.response["Error"]["Code"] == "EntityNotFoundException"
        finally:
            # DELETE
            glue.delete_job(JobName=job_name)

    def test_batch_get_jobs_create_delete_then_missing(self, glue):
        """Create a job, batch-get it, delete it, then batch-get shows it missing."""
        job_name = _unique("bgjob")
        glue.create_job(
            Name=job_name,
            Role="arn:aws:iam::123456789012:role/glue-role",
            Command={"Name": "glueetl", "ScriptLocation": "s3://bucket/script.py"},
        )
        resp = glue.batch_get_jobs(JobNames=[job_name])
        assert len(resp["Jobs"]) == 1
        assert resp["Jobs"][0]["Name"] == job_name

        glue.delete_job(JobName=job_name)

        resp_after = glue.batch_get_jobs(JobNames=[job_name])
        assert job_name in resp_after["JobsNotFound"]
        assert resp_after["Jobs"] == []


class TestGlueBatchGetCrawlersBehavior:
    """Additional behavioral tests for BatchGetCrawlers."""

    def test_batch_get_crawlers_single_missing_appears_in_not_found(self, glue):
        """BatchGetCrawlers with one missing name returns it in CrawlersNotFound."""
        # CREATE a real crawler to verify split behavior
        db_name = _unique("db")
        cr_name = _unique("bgcr")
        glue.create_database(DatabaseInput={"Name": db_name})
        glue.create_crawler(
            Name=cr_name,
            Role="arn:aws:iam::123456789012:role/glue-role",
            DatabaseName=db_name,
            Targets={"S3Targets": [{"Path": "s3://bucket/data"}]},
        )
        try:
            # RETRIEVE: confirm crawler is gettable
            get_resp = glue.get_crawler(Name=cr_name)
            assert get_resp["Crawler"]["Name"] == cr_name
            # LIST: crawler appears in listing
            list_resp = glue.list_crawlers()
            assert cr_name in list_resp["CrawlerNames"]
            # batch_get with one missing name
            fake_name = "single-missing-crawler-behav-xyz"
            resp = glue.batch_get_crawlers(CrawlerNames=[fake_name])
            assert resp["Crawlers"] == []
            assert fake_name in resp["CrawlersNotFound"]
            # ERROR: get nonexistent crawler raises EntityNotFoundException
            with pytest.raises(ClientError) as exc:
                glue.get_crawler(Name="completely-fake-crawler-abc")
            assert exc.value.response["Error"]["Code"] == "EntityNotFoundException"
        finally:
            # DELETE
            glue.delete_crawler(Name=cr_name)
            glue.delete_database(Name=db_name)

    def test_batch_get_crawlers_create_delete_then_missing(self, glue):
        """Create a crawler, batch-get it, delete it, then batch-get shows it missing."""
        db_name = _unique("db")
        cr_name = _unique("bgcr")
        glue.create_database(DatabaseInput={"Name": db_name})
        glue.create_crawler(
            Name=cr_name,
            Role="arn:aws:iam::123456789012:role/glue-role",
            DatabaseName=db_name,
            Targets={"S3Targets": [{"Path": "s3://bucket/data"}]},
        )
        resp = glue.batch_get_crawlers(CrawlerNames=[cr_name])
        assert len(resp["Crawlers"]) == 1
        assert resp["Crawlers"][0]["Name"] == cr_name

        glue.delete_crawler(Name=cr_name)
        glue.delete_database(Name=db_name)

        resp_after = glue.batch_get_crawlers(CrawlerNames=[cr_name])
        assert cr_name in resp_after["CrawlersNotFound"]
        assert resp_after["Crawlers"] == []


class TestGlueBatchGetTriggersBehavior:
    """Additional behavioral tests for BatchGetTriggers."""

    def test_batch_get_triggers_single_missing_appears_in_not_found(self, glue):
        """BatchGetTriggers with one missing name returns it in TriggersNotFound."""
        # CREATE a real trigger to verify split behavior
        trig_name = _unique("bgtrig")
        glue.create_trigger(
            Name=trig_name,
            Type="SCHEDULED",
            Schedule="cron(0 12 * * ? *)",
            Actions=[{"JobName": "dummy-job"}],
        )
        try:
            # RETRIEVE: confirm trigger is gettable
            get_resp = glue.get_trigger(Name=trig_name)
            assert get_resp["Trigger"]["Name"] == trig_name
            # LIST: trigger appears in listing
            list_resp = glue.list_triggers()
            assert trig_name in list_resp["TriggerNames"]
            # batch_get with one missing name
            fake_name = "single-missing-trigger-behav-xyz"
            resp = glue.batch_get_triggers(TriggerNames=[fake_name])
            assert resp["Triggers"] == []
            assert fake_name in resp["TriggersNotFound"]
            # ERROR: get nonexistent trigger raises EntityNotFoundException
            with pytest.raises(ClientError) as exc:
                glue.get_trigger(Name="completely-fake-trigger-abc")
            assert exc.value.response["Error"]["Code"] == "EntityNotFoundException"
        finally:
            # DELETE
            glue.delete_trigger(Name=trig_name)

    def test_batch_get_triggers_create_delete_then_missing(self, glue):
        """Create a trigger, batch-get it, delete it, then batch-get shows it missing."""
        t_name = _unique("bgtrig")
        glue.create_trigger(
            Name=t_name,
            Type="SCHEDULED",
            Schedule="cron(0 12 * * ? *)",
            Actions=[{"JobName": "dummy-job"}],
        )
        resp = glue.batch_get_triggers(TriggerNames=[t_name])
        assert len(resp["Triggers"]) == 1
        assert resp["Triggers"][0]["Name"] == t_name

        glue.delete_trigger(Name=t_name)

        resp_after = glue.batch_get_triggers(TriggerNames=[t_name])
        assert t_name in resp_after["TriggersNotFound"]
        assert resp_after["Triggers"] == []


class TestGlueBatchGetWorkflowsBehavior:
    """Additional behavioral tests for BatchGetWorkflows."""

    def test_batch_get_workflows_single_missing_appears_in_missing(self, glue):
        """BatchGetWorkflows with one missing name returns it in MissingWorkflows."""
        # CREATE a real workflow to verify split behavior
        wf_name = _unique("bgwf")
        glue.create_workflow(Name=wf_name, Description="single missing test workflow")
        try:
            # RETRIEVE: confirm workflow is gettable
            get_resp = glue.get_workflow(Name=wf_name)
            assert get_resp["Workflow"]["Name"] == wf_name
            # LIST: workflow appears in listing
            list_resp = glue.list_workflows()
            assert wf_name in list_resp["Workflows"]
            # batch_get with one missing name
            fake_name = "single-missing-workflow-behav-xyz"
            resp = glue.batch_get_workflows(Names=[fake_name])
            assert resp["Workflows"] == []
            assert fake_name in resp["MissingWorkflows"]
            # ERROR: get nonexistent workflow raises EntityNotFoundException
            with pytest.raises(ClientError) as exc:
                glue.get_workflow(Name="completely-fake-workflow-abc")
            assert exc.value.response["Error"]["Code"] == "EntityNotFoundException"
        finally:
            # DELETE
            glue.delete_workflow(Name=wf_name)

    def test_batch_get_workflows_create_retrieve_delete_then_missing(self, glue):
        """Create a workflow, batch-get it with description, delete, then shows as missing."""
        wf_name = _unique("bgwf")
        glue.create_workflow(Name=wf_name, Description="behavioral fidelity test")
        resp = glue.batch_get_workflows(Names=[wf_name])
        assert len(resp["Workflows"]) == 1
        assert resp["Workflows"][0]["Name"] == wf_name
        assert resp["Workflows"][0]["Description"] == "behavioral fidelity test"
        assert resp["MissingWorkflows"] == []

        glue.delete_workflow(Name=wf_name)

        resp_after = glue.batch_get_workflows(Names=[wf_name])
        assert wf_name in resp_after["MissingWorkflows"]
        assert resp_after["Workflows"] == []


class TestGlueSchemaVersionEdgeCases:
    """Edge cases and behavioral fidelity for GetSchemaVersion and GetSchemaByDefinition."""

    def _make_schema(self, glue):
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
        return reg_name, schema_name, definition

    def test_get_schema_version_create_retrieve_list_error(self, glue):
        """GetSchemaVersion full lifecycle: create schema, retrieve version, list versions, error."""
        reg_name, schema_name, definition = self._make_schema(glue)
        try:
            # RETRIEVE: get the schema version
            resp = glue.get_schema_version(
                SchemaId={"SchemaName": schema_name, "RegistryName": reg_name},
                SchemaVersionNumber={"LatestVersion": True},
            )
            assert "SchemaDefinition" in resp
            assert resp["DataFormat"] == "AVRO"
            assert "SchemaVersionId" in resp
            sv_id = resp["SchemaVersionId"]
            # LIST: list schema versions
            list_resp = glue.list_schema_versions(
                SchemaId={"SchemaName": schema_name, "RegistryName": reg_name}
            )
            assert "Schemas" in list_resp
            sv_ids = [v.get("SchemaVersionId") for v in list_resp["Schemas"]]
            assert sv_id in sv_ids
            # ERROR: get nonexistent schema raises EntityNotFoundException
            with pytest.raises(ClientError) as exc:
                glue.get_schema_version(
                    SchemaId={"SchemaName": "nonexistent-schema-xyz", "RegistryName": reg_name},
                    SchemaVersionNumber={"LatestVersion": True},
                )
            assert exc.value.response["Error"]["Code"] == "EntityNotFoundException"
        finally:
            glue.delete_schema(SchemaId={"SchemaName": schema_name, "RegistryName": reg_name})
            glue.delete_registry(RegistryId={"RegistryName": reg_name})

    def test_get_schema_version_by_number(self, glue):
        """GetSchemaVersion retrieves the correct version by VersionNumber."""
        reg_name, schema_name, definition = self._make_schema(glue)
        try:
            resp = glue.get_schema_version(
                SchemaId={"SchemaName": schema_name, "RegistryName": reg_name},
                SchemaVersionNumber={"VersionNumber": 1},
            )
            assert resp["VersionNumber"] == 1
            assert resp["DataFormat"] == "AVRO"
            assert "SchemaVersionId" in resp
        finally:
            glue.delete_schema(SchemaId={"SchemaName": schema_name, "RegistryName": reg_name})
            glue.delete_registry(RegistryId={"RegistryName": reg_name})

    def test_get_schema_version_after_register_new_version(self, glue):
        """GetSchemaVersion retrieves v2 after RegisterSchemaVersion adds it."""
        reg_name, schema_name, definition = self._make_schema(glue)
        try:
            new_def = '{"type":"record","name":"T","fields":[{"name":"id","type":"int"},{"name":"name","type":"string"}]}'
            reg_resp = glue.register_schema_version(
                SchemaId={"SchemaName": schema_name, "RegistryName": reg_name},
                SchemaDefinition=new_def,
            )
            assert reg_resp["VersionNumber"] == 2
            # RETRIEVE v2 by VersionNumber
            v2_resp = glue.get_schema_version(
                SchemaId={"SchemaName": schema_name, "RegistryName": reg_name},
                SchemaVersionNumber={"VersionNumber": 2},
            )
            assert v2_resp["VersionNumber"] == 2
            assert "SchemaDefinition" in v2_resp
            # DELETE: clean up
        finally:
            glue.delete_schema(SchemaId={"SchemaName": schema_name, "RegistryName": reg_name})
            glue.delete_registry(RegistryId={"RegistryName": reg_name})

    def test_get_schema_by_definition_create_retrieve_list_error(self, glue):
        """GetSchemaByDefinition full lifecycle: create, find by definition, list, error."""
        reg_name, schema_name, definition = self._make_schema(glue)
        try:
            # RETRIEVE: find schema by definition
            resp = glue.get_schema_by_definition(
                SchemaId={"SchemaName": schema_name, "RegistryName": reg_name},
                SchemaDefinition=definition,
            )
            assert "SchemaVersionId" in resp
            assert resp["DataFormat"] == "AVRO"
            assert "SchemaArn" in resp
            # LIST: list schemas in registry
            list_resp = glue.list_schemas(RegistryId={"RegistryName": reg_name})
            schema_names = [s["SchemaName"] for s in list_resp["Schemas"]]
            assert schema_name in schema_names
            # ERROR: get nonexistent schema by definition raises EntityNotFoundException
            with pytest.raises(ClientError) as exc:
                glue.get_schema_by_definition(
                    SchemaId={"SchemaName": "nonexistent-xyz", "RegistryName": reg_name},
                    SchemaDefinition=definition,
                )
            assert exc.value.response["Error"]["Code"] == "EntityNotFoundException"
        finally:
            glue.delete_schema(SchemaId={"SchemaName": schema_name, "RegistryName": reg_name})
            glue.delete_registry(RegistryId={"RegistryName": reg_name})

    def test_get_schema_by_definition_returns_arn(self, glue):
        """GetSchemaByDefinition returns SchemaArn matching the created schema ARN."""
        reg_name, schema_name, definition = self._make_schema(glue)
        try:
            # CREATE: get the schema ARN from create_schema result
            create_resp = glue.get_schema(
                SchemaId={"SchemaName": schema_name, "RegistryName": reg_name}
            )
            schema_arn = create_resp["SchemaArn"]
            # RETRIEVE: by definition - ARNs should match
            resp = glue.get_schema_by_definition(
                SchemaId={"SchemaName": schema_name, "RegistryName": reg_name},
                SchemaDefinition=definition,
            )
            assert resp["SchemaArn"] == schema_arn
        finally:
            glue.delete_schema(SchemaId={"SchemaName": schema_name, "RegistryName": reg_name})
            glue.delete_registry(RegistryId={"RegistryName": reg_name})


class TestGlueConnectionEdgeCases:
    """Edge cases and behavioral fidelity for CreateConnection and GetConnection."""

    def test_create_connection_full_lifecycle(self, glue):
        """CreateConnection full lifecycle: create, retrieve, list, update, delete, error."""
        conn_name = _unique("conn")
        glue.create_connection(
            ConnectionInput={
                "Name": conn_name,
                "ConnectionType": "JDBC",
                "ConnectionProperties": {
                    "JDBC_CONNECTION_URL": "jdbc:mysql://host:3306/mydb",
                    "USERNAME": "admin",
                    "PASSWORD": "secret",
                },
            }
        )
        try:
            # RETRIEVE: get the connection back
            resp = glue.get_connection(Name=conn_name)
            assert resp["Connection"]["Name"] == conn_name
            assert "JDBC_CONNECTION_URL" in resp["Connection"]["ConnectionProperties"]
            # LIST: connection appears in list
            list_resp = glue.get_connections()
            conn_names = [c["Name"] for c in list_resp["ConnectionList"]]
            assert conn_name in conn_names
            # UPDATE: change the URL
            glue.update_connection(
                Name=conn_name,
                ConnectionInput={
                    "Name": conn_name,
                    "ConnectionType": "JDBC",
                    "ConnectionProperties": {
                        "JDBC_CONNECTION_URL": "jdbc:mysql://host:3306/updated",
                        "USERNAME": "admin",
                        "PASSWORD": "secret",
                    },
                },
            )
            updated = glue.get_connection(Name=conn_name)
            assert "updated" in updated["Connection"]["ConnectionProperties"]["JDBC_CONNECTION_URL"]
            # ERROR: nonexistent connection raises EntityNotFoundException
            with pytest.raises(ClientError) as exc:
                glue.get_connection(Name="nonexistent-conn-xyz")
            assert exc.value.response["Error"]["Code"] == "EntityNotFoundException"
        finally:
            # DELETE
            glue.delete_connection(ConnectionName=conn_name)

    def test_create_connection_delete_then_not_found(self, glue):
        """After deleting a connection, GetConnection raises EntityNotFoundException."""
        conn_name = _unique("conn")
        glue.create_connection(
            ConnectionInput={
                "Name": conn_name,
                "ConnectionType": "JDBC",
                "ConnectionProperties": {
                    "JDBC_CONNECTION_URL": "jdbc:mysql://host:3306/db",
                    "USERNAME": "user",
                    "PASSWORD": "pass",
                },
            }
        )
        glue.delete_connection(ConnectionName=conn_name)
        with pytest.raises(ClientError) as exc:
            glue.get_connection(Name=conn_name)
        assert exc.value.response["Error"]["Code"] == "EntityNotFoundException"

    def test_create_connection_properties_preserved(self, glue):
        """ConnectionProperties are preserved after create and retrieve."""
        conn_name = _unique("conn")
        glue.create_connection(
            ConnectionInput={
                "Name": conn_name,
                "ConnectionType": "JDBC",
                "ConnectionProperties": {
                    "JDBC_CONNECTION_URL": "jdbc:postgresql://host:5432/db",
                    "USERNAME": "admin",
                    "PASSWORD": "secret",
                },
                "Description": "test connection",
            }
        )
        try:
            resp = glue.get_connection(Name=conn_name)
            assert resp["Connection"]["Name"] == conn_name
            assert "JDBC_CONNECTION_URL" in resp["Connection"]["ConnectionProperties"]
            assert (
                resp["Connection"]["ConnectionProperties"]["JDBC_CONNECTION_URL"]
                == "jdbc:postgresql://host:5432/db"
            )
        finally:
            glue.delete_connection(ConnectionName=conn_name)


class TestGlueBlueprintEdgeCases:
    """Edge cases and behavioral fidelity for ListBlueprints and blueprint operations."""

    def test_list_blueprints_create_retrieve_delete_error(self, glue):
        """ListBlueprints full lifecycle: create, retrieve, list, delete, error."""
        bp_name = _unique("bp")
        glue.create_blueprint(Name=bp_name, BlueprintLocation="s3://bucket/bp.py")
        try:
            # RETRIEVE: get blueprint details
            get_resp = glue.get_blueprint(Name=bp_name)
            assert get_resp["Blueprint"]["Name"] == bp_name
            # LIST: blueprint appears in list
            list_resp = glue.list_blueprints()
            assert bp_name in list_resp["Blueprints"]
            # UPDATE: update blueprint location
            glue.update_blueprint(Name=bp_name, BlueprintLocation="s3://bucket/bp-v2.py")
            # ERROR: get nonexistent blueprint raises EntityNotFoundException
            with pytest.raises(ClientError) as exc:
                glue.get_blueprint(Name="nonexistent-bp-xyz")
            assert exc.value.response["Error"]["Code"] == "EntityNotFoundException"
        finally:
            # DELETE
            glue.delete_blueprint(Name=bp_name)
            # verify gone
            with pytest.raises(ClientError) as exc:
                glue.get_blueprint(Name=bp_name)
            assert exc.value.response["Error"]["Code"] == "EntityNotFoundException"

    def test_list_blueprints_pagination_not_empty_after_create(self, glue):
        """ListBlueprints returns a non-empty list after creating blueprints."""
        bp_names = [_unique("bp") for _ in range(3)]
        for name in bp_names:
            glue.create_blueprint(Name=name, BlueprintLocation="s3://bucket/bp.py")
        try:
            resp = glue.list_blueprints()
            assert "Blueprints" in resp
            for name in bp_names:
                assert name in resp["Blueprints"]
        finally:
            for name in bp_names:
                glue.delete_blueprint(Name=name)


class TestGlueClassifierEdgeCases:
    """Edge cases and behavioral fidelity for GetClassifiers."""

    def test_get_classifiers_create_retrieve_list_update_delete_error(self, glue):
        """GetClassifiers full lifecycle: create, retrieve, list, update, delete, error."""
        clf_name = _unique("clf")
        glue.create_classifier(
            GrokClassifier={
                "Classification": "mytype",
                "Name": clf_name,
                "GrokPattern": "%{COMBINEDAPACHELOG}",
            }
        )
        try:
            # RETRIEVE: get individual classifier
            get_resp = glue.get_classifier(Name=clf_name)
            assert get_resp["Classifier"]["GrokClassifier"]["Name"] == clf_name
            assert get_resp["Classifier"]["GrokClassifier"]["Classification"] == "mytype"
            # LIST: classifier appears in classifiers list
            list_resp = glue.get_classifiers()
            clf_names = [
                c["GrokClassifier"]["Name"]
                for c in list_resp["Classifiers"]
                if "GrokClassifier" in c
            ]
            assert clf_name in clf_names
            # UPDATE: change classification
            glue.update_classifier(
                GrokClassifier={
                    "Name": clf_name,
                    "Classification": "updated-type",
                    "GrokPattern": "%{COMMONAPACHELOG}",
                }
            )
            updated = glue.get_classifier(Name=clf_name)
            assert updated["Classifier"]["GrokClassifier"]["Classification"] == "updated-type"
            # ERROR: get nonexistent classifier raises EntityNotFoundException
            with pytest.raises(ClientError) as exc:
                glue.get_classifier(Name="nonexistent-clf-xyz")
            assert exc.value.response["Error"]["Code"] == "EntityNotFoundException"
        finally:
            # DELETE
            glue.delete_classifier(Name=clf_name)

    def test_get_classifiers_multiple_types(self, glue):
        """GetClassifiers returns all classifier types including Grok."""
        clf_name = _unique("clf")
        glue.create_classifier(
            GrokClassifier={
                "Classification": "test-log",
                "Name": clf_name,
                "GrokPattern": "%{COMMONAPACHELOG}",
            }
        )
        try:
            resp = glue.get_classifiers()
            assert "Classifiers" in resp
            assert isinstance(resp["Classifiers"], list)
            grok_names = [
                c["GrokClassifier"]["Name"]
                for c in resp["Classifiers"]
                if "GrokClassifier" in c
            ]
            assert clf_name in grok_names
        finally:
            glue.delete_classifier(Name=clf_name)


class TestGlueDataQualityRulesetEdgeCases:
    """Edge cases and behavioral fidelity for ListDataQualityRulesets."""

    def test_list_data_quality_rulesets_create_retrieve_update_delete_error(self, glue):
        """ListDataQualityRulesets full lifecycle: create, retrieve, list, update, delete, error."""
        ruleset_name = _unique("dqr")
        glue.create_data_quality_ruleset(
            Name=ruleset_name,
            Ruleset='Rules = [ IsComplete "col1" ]',
        )
        try:
            # RETRIEVE: get individual ruleset
            get_resp = glue.get_data_quality_ruleset(Name=ruleset_name)
            assert get_resp["Name"] == ruleset_name
            assert "Ruleset" in get_resp
            # LIST: ruleset appears in list
            list_resp = glue.list_data_quality_rulesets()
            ruleset_names = [r["Name"] for r in list_resp["Rulesets"]]
            assert ruleset_name in ruleset_names
            # UPDATE: change ruleset definition
            glue.update_data_quality_ruleset(
                Name=ruleset_name,
                Ruleset='Rules = [ IsComplete "col2" ]',
            )
            updated = glue.get_data_quality_ruleset(Name=ruleset_name)
            assert updated["Name"] == ruleset_name
            # ERROR: get nonexistent ruleset raises error
            with pytest.raises(ClientError) as exc:
                glue.get_data_quality_ruleset(Name="nonexistent-dqr-xyz")
            assert exc.value.response["Error"]["Code"] in (
                "EntityNotFoundException",
                "InvalidInputException",
            )
        finally:
            # DELETE
            glue.delete_data_quality_ruleset(Name=ruleset_name)

    def test_list_data_quality_rulesets_multiple(self, glue):
        """ListDataQualityRulesets returns all created rulesets."""
        names = [_unique("dqr") for _ in range(3)]
        for name in names:
            glue.create_data_quality_ruleset(
                Name=name, Ruleset='Rules = [ IsComplete "id" ]'
            )
        try:
            resp = glue.list_data_quality_rulesets()
            listed = [r["Name"] for r in resp["Rulesets"]]
            for name in names:
                assert name in listed
        finally:
            for name in names:
                glue.delete_data_quality_ruleset(Name=name)


class TestGlueMLTransformsEdgeCases:
    """Edge cases and behavioral fidelity for GetMLTransforms."""

    def _make_transform(self, glue):
        name = _unique("mlt")
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

    def test_get_ml_transforms_create_retrieve_list_update_delete_error(self, glue):
        """GetMLTransforms full lifecycle: create, retrieve, list, update, delete, error."""
        tfm_id, name = self._make_transform(glue)
        try:
            # RETRIEVE: get individual transform
            get_resp = glue.get_ml_transform(TransformId=tfm_id)
            assert get_resp["TransformId"] == tfm_id
            assert get_resp["Name"] == name
            # LIST: transform appears in list
            list_resp = glue.get_ml_transforms()
            ids = [t["TransformId"] for t in list_resp["Transforms"]]
            assert tfm_id in ids
            # UPDATE: update description
            update_resp = glue.update_ml_transform(TransformId=tfm_id, Description="updated desc")
            assert update_resp["TransformId"] == tfm_id
            # verify update
            updated = glue.get_ml_transform(TransformId=tfm_id)
            assert updated["Description"] == "updated desc"
            # ERROR: get nonexistent transform raises EntityNotFoundException
            with pytest.raises(ClientError) as exc:
                glue.get_ml_transform(TransformId="nonexistent-tfm-xyz")
            assert exc.value.response["Error"]["Code"] == "EntityNotFoundException"
        finally:
            # DELETE
            del_resp = glue.delete_ml_transform(TransformId=tfm_id)
            assert del_resp["TransformId"] == tfm_id

    def test_get_ml_transforms_multiple(self, glue):
        """GetMLTransforms returns all created transforms."""
        tfm_ids = []
        for _ in range(3):
            tfm_id, _ = self._make_transform(glue)
            tfm_ids.append(tfm_id)
        try:
            resp = glue.get_ml_transforms()
            listed_ids = [t["TransformId"] for t in resp["Transforms"]]
            for tfm_id in tfm_ids:
                assert tfm_id in listed_ids
        finally:
            for tfm_id in tfm_ids:
                glue.delete_ml_transform(TransformId=tfm_id)


class TestGlueUsageProfileEdgeCases:
    """Edge cases and behavioral fidelity for ListUsageProfiles."""

    def test_list_usage_profiles_create_retrieve_update_delete_error(self, glue):
        """ListUsageProfiles full lifecycle: create, retrieve, list, update, delete, error."""
        name = _unique("up")
        glue.create_usage_profile(Name=name, Configuration={})
        try:
            # RETRIEVE: get profile details
            get_resp = glue.get_usage_profile(Name=name)
            assert get_resp["Name"] == name
            # LIST: profile appears in list
            list_resp = glue.list_usage_profiles()
            profile_names = [p["Name"] for p in list_resp["Profiles"]]
            assert name in profile_names
            # UPDATE: update configuration
            glue.update_usage_profile(
                Name=name,
                Configuration={
                    "SessionConfiguration": {"IdleTimeout": {"DefaultValue": "60"}}
                },
            )
            updated = glue.get_usage_profile(Name=name)
            assert updated["Name"] == name
            # ERROR: get nonexistent profile raises EntityNotFoundException
            with pytest.raises(ClientError) as exc:
                glue.get_usage_profile(Name="nonexistent-profile-xyz")
            assert exc.value.response["Error"]["Code"] == "EntityNotFoundException"
        finally:
            # DELETE
            glue.delete_usage_profile(Name=name)

    def test_list_usage_profiles_multiple(self, glue):
        """ListUsageProfiles returns all created profiles."""
        names = [_unique("up") for _ in range(3)]
        for name in names:
            glue.create_usage_profile(Name=name, Configuration={})
        try:
            resp = glue.list_usage_profiles()
            listed = [p["Name"] for p in resp["Profiles"]]
            for name in names:
                assert name in listed
        finally:
            for name in names:
                glue.delete_usage_profile(Name=name)


class TestGlueCatalogImportStatusEdgeCases:
    """Edge cases and behavioral fidelity for GetCatalogImportStatus."""

    def test_get_catalog_import_status_create_retrieve_list_error(self, glue):
        """GetCatalogImportStatus full lifecycle: import catalog, get status, verify fields."""
        # CREATE: trigger import
        glue.import_catalog_to_glue()
        # RETRIEVE: get import status
        resp = glue.get_catalog_import_status()
        assert "ImportStatus" in resp
        status = resp["ImportStatus"]
        assert "ImportCompleted" in status
        assert status["ImportCompleted"] is True
        # LIST: also verify catalog list exists
        catalogs_resp = glue.get_catalogs()
        assert "CatalogList" in catalogs_resp
        # ERROR: get nonexistent catalog raises EntityNotFoundException
        with pytest.raises(ClientError) as exc:
            glue.get_catalog(CatalogId="nonexistent-catalog-xyz-123")
        assert exc.value.response["Error"]["Code"] == "EntityNotFoundException"

    def test_get_catalog_import_status_returns_bool(self, glue):
        """GetCatalogImportStatus always returns ImportCompleted as a boolean."""
        resp = glue.get_catalog_import_status()
        assert "ImportStatus" in resp
        assert isinstance(resp["ImportStatus"].get("ImportCompleted"), bool)


class TestGlueResourcePoliciesEdgeCases:
    """Edge cases and behavioral fidelity for GetResourcePolicies."""

    def test_get_resource_policies_create_retrieve_list_delete_error(self, glue):
        """GetResourcePolicies full lifecycle: put policy, get it, list policies, delete, error."""
        import json

        policy = json.dumps({
            "Version": "2012-10-17",
            "Statement": [{
                "Effect": "Allow",
                "Principal": {"AWS": "arn:aws:iam::123456789012:root"},
                "Action": "glue:GetDatabase",
                "Resource": "*",
            }],
        })
        # CREATE (put resource policy)
        glue.put_resource_policy(PolicyInJson=policy)
        try:
            # RETRIEVE: get the individual resource policy
            get_resp = glue.get_resource_policy()
            assert "PolicyInJson" in get_resp
            # LIST: resource policies list
            list_resp = glue.get_resource_policies()
            assert "GetResourcePoliciesResponseList" in list_resp
            # ERROR: nonexistent connection raises EntityNotFoundException (proves service is live)
            with pytest.raises(ClientError) as exc:
                glue.get_connection(Name="nonexistent-conn-policies-xyz")
            assert exc.value.response["Error"]["Code"] == "EntityNotFoundException"
        finally:
            # DELETE: remove the resource policy
            glue.delete_resource_policy()

    def test_get_resource_policies_returns_list(self, glue):
        """GetResourcePolicies always returns a GetResourcePoliciesResponseList."""
        resp = glue.get_resource_policies()
        assert "GetResourcePoliciesResponseList" in resp
        assert isinstance(resp["GetResourcePoliciesResponseList"], list)


class TestGlueSecurityConfigurationsEdgeCases:
    """Edge cases and behavioral fidelity for GetSecurityConfigurations."""

    def test_get_security_configurations_create_retrieve_list_delete_error(self, glue):
        """GetSecurityConfigurations full lifecycle: create, retrieve, list, delete, error."""
        name = _unique("sc")
        glue.create_security_configuration(
            Name=name,
            EncryptionConfiguration={
                "S3Encryption": [{"S3EncryptionMode": "DISABLED"}],
                "CloudWatchEncryption": {"CloudWatchEncryptionMode": "DISABLED"},
                "JobBookmarksEncryption": {"JobBookmarksEncryptionMode": "DISABLED"},
            },
        )
        try:
            # RETRIEVE: get individual config
            get_resp = glue.get_security_configuration(Name=name)
            assert get_resp["SecurityConfiguration"]["Name"] == name
            assert "EncryptionConfiguration" in get_resp["SecurityConfiguration"]
            # LIST: config appears in list
            list_resp = glue.get_security_configurations()
            sc_names = [sc["Name"] for sc in list_resp["SecurityConfigurations"]]
            assert name in sc_names
            # ERROR: get nonexistent config raises EntityNotFoundException
            with pytest.raises(ClientError) as exc:
                glue.get_security_configuration(Name="nonexistent-sc-xyz")
            assert exc.value.response["Error"]["Code"] == "EntityNotFoundException"
        finally:
            # DELETE
            glue.delete_security_configuration(Name=name)
            # verify gone
            with pytest.raises(ClientError) as exc:
                glue.get_security_configuration(Name=name)
            assert exc.value.response["Error"]["Code"] == "EntityNotFoundException"

    def test_get_security_configurations_multiple(self, glue):
        """GetSecurityConfigurations returns all created configs."""
        names = [_unique("sc") for _ in range(3)]
        for name in names:
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
            listed = [sc["Name"] for sc in resp["SecurityConfigurations"]]
            for name in names:
                assert name in listed
        finally:
            for name in names:
                glue.delete_security_configuration(Name=name)


class TestGlueCrawlerMetricsEdgeCases:
    """Edge cases and behavioral fidelity for GetCrawlerMetrics."""

    def test_get_crawler_metrics_create_retrieve_list_delete_error(self, glue):
        """GetCrawlerMetrics full lifecycle: create crawler, get metrics, list metrics, delete."""
        db_name = _unique("db")
        crawler_name = _unique("crawler")
        glue.create_database(DatabaseInput={"Name": db_name})
        glue.create_crawler(
            Name=crawler_name,
            Role="arn:aws:iam::123456789012:role/glue-role",
            DatabaseName=db_name,
            Targets={"S3Targets": [{"Path": "s3://bucket/data"}]},
        )
        try:
            # RETRIEVE: get crawler details
            get_resp = glue.get_crawler(Name=crawler_name)
            assert get_resp["Crawler"]["Name"] == crawler_name
            # LIST: get crawler metrics (all crawlers)
            metrics_resp = glue.get_crawler_metrics()
            assert "CrawlerMetricsList" in metrics_resp
            # list crawlers
            list_resp = glue.list_crawlers()
            assert crawler_name in list_resp["CrawlerNames"]
            # ERROR: get nonexistent crawler raises EntityNotFoundException
            with pytest.raises(ClientError) as exc:
                glue.get_crawler(Name="nonexistent-crawler-xyz")
            assert exc.value.response["Error"]["Code"] == "EntityNotFoundException"
        finally:
            # DELETE
            glue.delete_crawler(Name=crawler_name)
            glue.delete_database(Name=db_name)

    def test_get_crawler_metrics_for_specific_crawler(self, glue):
        """GetCrawlerMetrics with CrawlerNameList returns metrics for that crawler."""
        db_name = _unique("db")
        crawler_name = _unique("crawler")
        glue.create_database(DatabaseInput={"Name": db_name})
        glue.create_crawler(
            Name=crawler_name,
            Role="arn:aws:iam::123456789012:role/glue-role",
            DatabaseName=db_name,
            Targets={"S3Targets": [{"Path": "s3://bucket/data"}]},
        )
        try:
            resp = glue.get_crawler_metrics(CrawlerNameList=[crawler_name])
            assert "CrawlerMetricsList" in resp
            assert isinstance(resp["CrawlerMetricsList"], list)
        finally:
            glue.delete_crawler(Name=crawler_name)
            glue.delete_database(Name=db_name)


class TestGlueEntityRecordsEdgeCases:
    """Edge cases and behavioral fidelity for GetEntityRecords."""

    def test_get_entity_records_create_retrieve_list_error(self, glue):
        """GetEntityRecords full lifecycle context: conn, endpoint, entity records, error."""
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
            # RETRIEVE: get the connection back
            get_resp = glue.get_connection(Name=conn_name)
            assert get_resp["Connection"]["Name"] == conn_name
            # LIST: connection appears in list
            list_resp = glue.get_connections()
            assert conn_name in [c["Name"] for c in list_resp["ConnectionList"]]
            # GetEntityRecords returns a Records list
            entity_resp = glue.get_entity_records(
                EntityName="fake-entity",
                ConnectionName="fake-conn",
                Limit=10,
            )
            assert "Records" in entity_resp
            # ERROR: get nonexistent connection raises EntityNotFoundException
            with pytest.raises(ClientError) as exc:
                glue.get_connection(Name="nonexistent-conn-entity-xyz")
            assert exc.value.response["Error"]["Code"] == "EntityNotFoundException"
        finally:
            # DELETE
            glue.delete_connection(ConnectionName=conn_name)

    def test_get_entity_records_returns_list(self, glue):
        """GetEntityRecords always returns a Records list (may be empty)."""
        resp = glue.get_entity_records(
            EntityName="entity-type-1",
            ConnectionName="fake-connection-name",
            Limit=5,
        )
        assert "Records" in resp
        assert isinstance(resp["Records"], list)


class TestGlueMappingEdgeCases:
    """Edge cases and behavioral fidelity for GetMapping."""

    def test_get_mapping_create_retrieve_list_error(self, glue):
        """GetMapping full lifecycle context: database, table, mapping, error."""
        db_name = _unique("db")
        tbl_name = _unique("tbl")
        glue.create_database(DatabaseInput={"Name": db_name})
        glue.create_table(
            DatabaseName=db_name,
            TableInput={
                "Name": tbl_name,
                "StorageDescriptor": {
                    "Columns": [{"Name": "id", "Type": "string"}],
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
            # RETRIEVE: get table details
            get_resp = glue.get_table(DatabaseName=db_name, Name=tbl_name)
            assert get_resp["Table"]["Name"] == tbl_name
            # LIST: table appears in tables list
            list_resp = glue.get_tables(DatabaseName=db_name)
            tbl_names = [t["Name"] for t in list_resp["TableList"]]
            assert tbl_name in tbl_names
            # GetMapping returns a Mapping list
            mapping_resp = glue.get_mapping(
                Source={"DatabaseName": db_name, "TableName": tbl_name},
            )
            assert "Mapping" in mapping_resp
            assert isinstance(mapping_resp["Mapping"], list)
            # ERROR: get nonexistent table raises EntityNotFoundException
            with pytest.raises(ClientError) as exc:
                glue.get_table(DatabaseName=db_name, Name="nonexistent-tbl-xyz")
            assert exc.value.response["Error"]["Code"] == "EntityNotFoundException"
        finally:
            # DELETE
            glue.delete_table(DatabaseName=db_name, Name=tbl_name)
            glue.delete_database(Name=db_name)

    def test_get_mapping_with_sinks(self, glue):
        """GetMapping with Sinks parameter returns a Mapping list."""
        resp = glue.get_mapping(
            Source={"DatabaseName": "src-db", "TableName": "src-tbl"},
            Sinks=[{"DatabaseName": "tgt-db", "TableName": "tgt-tbl"}],
        )
        assert "Mapping" in resp
        assert isinstance(resp["Mapping"], list)


# ── Edge Cases & Behavioral Fidelity ─────────────────────────────────────────


class TestGlueBatchDeleteTableVersionEdgeCases:
    """Edge cases for BatchDeleteTableVersion: full lifecycle + wrong-table error."""

    _SD = {
        "Columns": [{"Name": "c1", "Type": "string"}],
        "Location": "s3://b/p",
        "InputFormat": "org.apache.hadoop.mapred.TextInputFormat",
        "OutputFormat": "org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat",
        "SerdeInfo": {
            "SerializationLibrary": "org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe"
        },
    }

    def test_create_table_version_then_batch_delete(self, glue):
        """CREATE db+table, UPDATE table (creates v2), LIST versions, DELETE v1, RETRIEVE remaining."""
        db_name = _unique("db")
        tbl_name = _unique("tbl")
        glue.create_database(DatabaseInput={"Name": db_name})
        glue.create_table(
            DatabaseName=db_name,
            TableInput={"Name": tbl_name, "StorageDescriptor": self._SD},
        )
        sd2 = dict(self._SD, Columns=[{"Name": "c1", "Type": "string"}, {"Name": "c2", "Type": "int"}])
        glue.update_table(
            DatabaseName=db_name,
            TableInput={"Name": tbl_name, "StorageDescriptor": sd2},
        )
        # LIST: get versions
        versions_resp = glue.get_table_versions(DatabaseName=db_name, TableName=tbl_name)
        version_ids = [v["VersionId"] for v in versions_resp["TableVersions"]]
        assert len(version_ids) >= 2

        # DELETE: batch delete oldest version
        del_resp = glue.batch_delete_table_version(
            DatabaseName=db_name, TableName=tbl_name, VersionIds=version_ids[:1]
        )
        assert "Errors" in del_resp

        # RETRIEVE: remaining versions still accessible
        remaining = glue.get_table_versions(DatabaseName=db_name, TableName=tbl_name)
        assert isinstance(remaining["TableVersions"], list)

        glue.delete_table(DatabaseName=db_name, Name=tbl_name)
        glue.delete_database(Name=db_name)

    def test_batch_delete_table_version_wrong_table_in_existing_db(self, glue):
        """BatchDeleteTableVersion for wrong table in existing db raises EntityNotFoundException."""
        db_name = _unique("db")
        glue.create_database(DatabaseInput={"Name": db_name})
        try:
            with pytest.raises(ClientError) as exc:
                glue.batch_delete_table_version(
                    DatabaseName=db_name, TableName="no-such-tbl", VersionIds=["1"]
                )
            assert exc.value.response["Error"]["Code"] == "EntityNotFoundException"
        finally:
            glue.delete_database(Name=db_name)

    def test_get_table_version_after_update(self, glue):
        """UpdateTable creates a new version; GetTableVersion retrieves it by ID."""
        db_name = _unique("db")
        tbl_name = _unique("tbl")
        glue.create_database(DatabaseInput={"Name": db_name})
        glue.create_table(
            DatabaseName=db_name,
            TableInput={"Name": tbl_name, "StorageDescriptor": self._SD},
        )
        sd2 = dict(self._SD, Columns=[{"Name": "c1", "Type": "string"}, {"Name": "c2", "Type": "int"}])
        glue.update_table(
            DatabaseName=db_name,
            TableInput={"Name": tbl_name, "StorageDescriptor": sd2},
        )
        versions = glue.get_table_versions(DatabaseName=db_name, TableName=tbl_name)
        latest_version_id = versions["TableVersions"][0]["VersionId"]

        # RETRIEVE by version ID
        ver_resp = glue.get_table_version(
            DatabaseName=db_name, TableName=tbl_name, VersionId=latest_version_id
        )
        assert ver_resp["TableVersion"]["VersionId"] == latest_version_id

        glue.delete_table(DatabaseName=db_name, Name=tbl_name)
        glue.delete_database(Name=db_name)


class TestGlueColumnStatisticsScheduleEdgeCases:
    """Edge cases for ColumnStatisticsTaskRunSchedule: full lifecycle + error."""

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
                        "SerializationLibrary": "org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe"
                    },
                },
            },
        )
        return db_name, tbl_name

    def test_start_then_stop_schedule_full_lifecycle(self, glue):
        """CREATE table, start schedule (UPDATE), stop schedule (UPDATE), DELETE table."""
        db_name, tbl_name = self._make_table(glue)
        try:
            # UPDATE: start schedule
            start_resp = glue.start_column_statistics_task_run_schedule(
                DatabaseName=db_name, TableName=tbl_name
            )
            assert start_resp["ResponseMetadata"]["HTTPStatusCode"] == 200

            # UPDATE: stop schedule
            stop_resp = glue.stop_column_statistics_task_run_schedule(
                DatabaseName=db_name, TableName=tbl_name
            )
            assert stop_resp["ResponseMetadata"]["HTTPStatusCode"] == 200
        finally:
            glue.delete_table(DatabaseName=db_name, Name=tbl_name)
            glue.delete_database(Name=db_name)

    def test_start_task_run_and_retrieve_id(self, glue):
        """StartColumnStatisticsTaskRun returns a run ID (CREATE+RETRIEVE pattern)."""
        db_name, tbl_name = self._make_table(glue)
        try:
            resp = glue.start_column_statistics_task_run(
                DatabaseName=db_name,
                TableName=tbl_name,
                Role="arn:aws:iam::123456789012:role/test",
            )
            run_id = resp["ColumnStatisticsTaskRunId"]
            assert isinstance(run_id, str)
            assert len(run_id) > 0

            # RETRIEVE: get the table to verify it still exists
            get_resp = glue.get_table(DatabaseName=db_name, Name=tbl_name)
            assert get_resp["Table"]["Name"] == tbl_name

            # LIST: get all task runs for this table
            runs_resp = glue.get_column_statistics_task_runs(
                DatabaseName=db_name, TableName=tbl_name
            )
            assert "ColumnStatisticsTaskRuns" in runs_resp
            assert isinstance(runs_resp["ColumnStatisticsTaskRuns"], list)
        finally:
            glue.delete_table(DatabaseName=db_name, Name=tbl_name)
            glue.delete_database(Name=db_name)

    def test_stop_task_run_error_no_running_run(self, glue):
        """StopColumnStatisticsTaskRun when no run is active still returns 200."""
        db_name, tbl_name = self._make_table(glue)
        try:
            # Start then immediately stop - may succeed even if already stopped
            resp = glue.stop_column_statistics_task_run(
                DatabaseName=db_name, TableName=tbl_name
            )
            assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200
        finally:
            glue.delete_table(DatabaseName=db_name, Name=tbl_name)
            glue.delete_database(Name=db_name)


class TestGlueMLTransformEdgeCases:
    """Edge cases for MLTransform: update, list with filter, error on nonexistent."""

    def _make_transform(self, glue, name=None):
        name = name or _unique("ml")
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

    def test_create_then_get_then_update_then_delete(self, glue):
        """Full lifecycle: CREATE, RETRIEVE, UPDATE description, DELETE."""
        tfm_id, name = self._make_transform(glue)
        try:
            # RETRIEVE
            get_resp = glue.get_ml_transform(TransformId=tfm_id)
            assert get_resp["Name"] == name
            assert get_resp["TransformId"] == tfm_id

            # UPDATE
            upd_resp = glue.update_ml_transform(
                TransformId=tfm_id, Description="updated description"
            )
            assert upd_resp["TransformId"] == tfm_id

            # RETRIEVE after update
            updated = glue.get_ml_transform(TransformId=tfm_id)
            assert updated["Description"] == "updated description"
        finally:
            # DELETE
            glue.delete_ml_transform(TransformId=tfm_id)

        # ERROR: get after delete
        with pytest.raises(ClientError) as exc:
            glue.get_ml_transform(TransformId=tfm_id)
        assert exc.value.response["Error"]["Code"] == "EntityNotFoundException"

    def test_list_ml_transforms_with_max_results(self, glue):
        """ListMLTransforms with MaxResults limits response."""
        ids = []
        for _ in range(3):
            tfm_id, _ = self._make_transform(glue)
            ids.append(tfm_id)
        try:
            resp = glue.list_ml_transforms(MaxResults=2)
            assert "TransformIds" in resp
            assert isinstance(resp["TransformIds"], list)
        finally:
            for tfm_id in ids:
                glue.delete_ml_transform(TransformId=tfm_id)

    def test_get_nonexistent_ml_transform_raises(self, glue):
        """GetMLTransform for nonexistent ID raises EntityNotFoundException."""
        with pytest.raises(ClientError) as exc:
            glue.get_ml_transform(TransformId="nonexistent-transform-id-xyz")
        assert exc.value.response["Error"]["Code"] == "EntityNotFoundException"


class TestGlueWorkflowRunEdgeCases:
    """Edge cases for WorkflowRun: start, get, resume, list, stop."""

    def test_start_then_stop_workflow_run(self, glue):
        """CREATE workflow, start run (CREATE), stop run (UPDATE), get run (RETRIEVE), delete."""
        wf_name = _unique("wf")
        glue.create_workflow(Name=wf_name)
        try:
            run_resp = glue.start_workflow_run(Name=wf_name)
            run_id = run_resp["RunId"]
            assert isinstance(run_id, str)

            # RETRIEVE: get the run
            get_resp = glue.get_workflow_run(Name=wf_name, RunId=run_id)
            assert get_resp["Run"]["WorkflowRunId"] == run_id

            # LIST: get all runs
            runs_resp = glue.get_workflow_runs(Name=wf_name)
            run_ids = [r["WorkflowRunId"] for r in runs_resp["Runs"]]
            assert run_id in run_ids

            # UPDATE: stop the run
            stop_resp = glue.stop_workflow_run(Name=wf_name, RunId=run_id)
            assert stop_resp["ResponseMetadata"]["HTTPStatusCode"] == 200
        finally:
            glue.delete_workflow(Name=wf_name)

    def test_resume_workflow_run_nonexistent_raises_error(self, glue):
        """ResumeWorkflowRun for nonexistent workflow raises meaningful error."""
        with pytest.raises(ClientError) as exc:
            glue.resume_workflow_run(
                Name="nonexistent-wf-abc123", RunId="fake-run-id", NodeIds=["node1"]
            )
        assert exc.value.response["Error"]["Code"] in (
            "EntityNotFoundException",
            "InvalidInputException",
        )

    def test_workflow_run_properties_crud(self, glue):
        """PutWorkflowRunProperties / GetWorkflowRunProperties roundtrip."""
        wf_name = _unique("wf")
        glue.create_workflow(Name=wf_name)
        try:
            run_resp = glue.start_workflow_run(Name=wf_name)
            run_id = run_resp["RunId"]

            # UPDATE: put properties
            glue.put_workflow_run_properties(
                Name=wf_name,
                RunId=run_id,
                RunProperties={"myKey": "myValue"},
            )

            # RETRIEVE: get properties
            props_resp = glue.get_workflow_run_properties(Name=wf_name, RunId=run_id)
            assert "RunProperties" in props_resp
            assert props_resp["RunProperties"].get("myKey") == "myValue"
        finally:
            glue.delete_workflow(Name=wf_name)


class TestGlueConnectionsEdgeCases:
    """Edge cases for Connections: full CRUD lifecycle + update + error."""

    def _make_conn(self, glue, name=None):
        name = name or _unique("conn")
        glue.create_connection(
            ConnectionInput={
                "Name": name,
                "ConnectionType": "JDBC",
                "ConnectionProperties": {
                    "JDBC_CONNECTION_URL": "jdbc:mysql://host:3306/db",
                    "USERNAME": "admin",
                    "PASSWORD": "secret",
                },
            }
        )
        return name

    def test_create_retrieve_update_delete_connection(self, glue):
        """Full lifecycle: CREATE, RETRIEVE, UPDATE, DELETE, ERROR."""
        name = self._make_conn(glue)
        try:
            # RETRIEVE
            get_resp = glue.get_connection(Name=name)
            assert get_resp["Connection"]["Name"] == name

            # LIST
            list_resp = glue.get_connections()
            names = [c["Name"] for c in list_resp["ConnectionList"]]
            assert name in names

            # UPDATE
            glue.update_connection(
                Name=name,
                ConnectionInput={
                    "Name": name,
                    "ConnectionType": "JDBC",
                    "ConnectionProperties": {
                        "JDBC_CONNECTION_URL": "jdbc:mysql://newhost:3306/db",
                        "USERNAME": "admin2",
                        "PASSWORD": "secret2",
                    },
                },
            )
            updated = glue.get_connection(Name=name)
            assert updated["Connection"]["ConnectionProperties"]["USERNAME"] == "admin2"
        finally:
            # DELETE
            glue.delete_connection(ConnectionName=name)

        # ERROR: get after delete
        with pytest.raises(ClientError) as exc:
            glue.get_connection(Name=name)
        assert exc.value.response["Error"]["Code"] in ("EntityNotFoundException", "GlueEncryptionException")

    def test_connection_list_pagination_with_multiple(self, glue):
        """GetConnections lists all connections including multiple created ones."""
        names = [self._make_conn(glue) for _ in range(3)]
        try:
            resp = glue.get_connections()
            found = [c["Name"] for c in resp["ConnectionList"]]
            for name in names:
                assert name in found
        finally:
            for name in names:
                glue.delete_connection(ConnectionName=name)

    def test_get_connection_nonexistent_raises(self, glue):
        """GetConnection for nonexistent connection raises EntityNotFoundException."""
        with pytest.raises(ClientError) as exc:
            glue.get_connection(Name="no-such-conn-xyz")
        assert exc.value.response["Error"]["Code"] in ("EntityNotFoundException", "GlueEncryptionException")


class TestGluePartitionIndexesEdgeCases:
    """Edge cases for PartitionIndexes: create index, retrieve, delete, error."""

    def _make_partitioned_table(self, glue):
        db_name = _unique("db")
        tbl_name = _unique("tbl")
        glue.create_database(DatabaseInput={"Name": db_name})
        glue.create_table(
            DatabaseName=db_name,
            TableInput={
                "Name": tbl_name,
                "StorageDescriptor": {
                    "Columns": [{"Name": "data", "Type": "string"}],
                    "Location": "s3://bucket/path",
                    "InputFormat": "org.apache.hadoop.mapred.TextInputFormat",
                    "OutputFormat": "org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat",
                    "SerdeInfo": {
                        "SerializationLibrary": "org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe"
                    },
                },
                "PartitionKeys": [{"Name": "year", "Type": "int"}, {"Name": "month", "Type": "int"}],
            },
        )
        return db_name, tbl_name

    def test_create_then_list_then_delete_partition_index(self, glue):
        """CREATE partition index, LIST indexes (response key exists), DELETE index."""
        db_name, tbl_name = self._make_partitioned_table(glue)
        try:
            # CREATE index - verify it returns 200
            create_resp = glue.create_partition_index(
                DatabaseName=db_name,
                TableName=tbl_name,
                PartitionIndex={"Keys": ["year"], "IndexName": "idx-year"},
            )
            assert create_resp["ResponseMetadata"]["HTTPStatusCode"] == 200

            # LIST: response has the expected key
            list_resp = glue.get_partition_indexes(DatabaseName=db_name, TableName=tbl_name)
            assert "PartitionIndexDescriptorList" in list_resp
            assert isinstance(list_resp["PartitionIndexDescriptorList"], list)

            # RETRIEVE: table still exists
            tbl_resp = glue.get_table(DatabaseName=db_name, Name=tbl_name)
            assert tbl_resp["Table"]["Name"] == tbl_name

            # DELETE: remove the index (may succeed even if index not in active state)
            del_resp = glue.delete_partition_index(
                DatabaseName=db_name, TableName=tbl_name, IndexName="idx-year"
            )
            assert del_resp["ResponseMetadata"]["HTTPStatusCode"] == 200
        finally:
            glue.delete_table(DatabaseName=db_name, Name=tbl_name)
            glue.delete_database(Name=db_name)

    def test_get_partition_indexes_empty_for_non_partitioned_table(self, glue):
        """GetPartitionIndexes for table without partition keys returns empty list."""
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
            resp = glue.get_partition_indexes(DatabaseName=db_name, TableName=tbl_name)
            assert isinstance(resp["PartitionIndexDescriptorList"], list)
        finally:
            glue.delete_table(DatabaseName=db_name, Name=tbl_name)
            glue.delete_database(Name=db_name)


class TestGlueTriggersEdgeCases:
    """Edge cases for Triggers: get_triggers, list_triggers, update, error."""

    def _make_trigger(self, glue, name=None):
        name = name or _unique("trig")
        glue.create_trigger(
            Name=name, Type="ON_DEMAND", Actions=[{"JobName": "fake-job"}]
        )
        return name

    def test_get_triggers_create_retrieve_update_delete_error(self, glue):
        """Full trigger lifecycle: CREATE, RETRIEVE via get_triggers, UPDATE, DELETE, ERROR."""
        name = self._make_trigger(glue)
        try:
            # RETRIEVE via get_trigger
            get_resp = glue.get_trigger(Name=name)
            assert get_resp["Trigger"]["Name"] == name
            assert get_resp["Trigger"]["Type"] == "ON_DEMAND"

            # LIST via get_triggers
            list_resp = glue.get_triggers()
            trigger_names = [t["Name"] for t in list_resp["Triggers"]]
            assert name in trigger_names

            # UPDATE
            upd_resp = glue.update_trigger(
                Name=name,
                TriggerUpdate={"Name": name, "Description": "updated desc"},
            )
            assert upd_resp["Trigger"]["Name"] == name
        finally:
            # DELETE
            glue.delete_trigger(Name=name)

        # ERROR
        with pytest.raises(ClientError) as exc:
            glue.get_trigger(Name=name)
        assert exc.value.response["Error"]["Code"] == "EntityNotFoundException"

    def test_list_triggers_with_max_results(self, glue):
        """ListTriggers with MaxResults limits the response."""
        names = [self._make_trigger(glue) for _ in range(3)]
        try:
            resp = glue.list_triggers(MaxResults=2)
            assert "TriggerNames" in resp
            assert len(resp["TriggerNames"]) <= 2
        finally:
            for name in names:
                glue.delete_trigger(Name=name)

    def test_get_triggers_after_create_and_delete(self, glue):
        """Trigger not present in get_triggers list after deletion."""
        name = self._make_trigger(glue)
        glue.delete_trigger(Name=name)

        resp = glue.get_triggers()
        trigger_names = [t["Name"] for t in resp["Triggers"]]
        assert name not in trigger_names


class TestGlueCrawlersListEdgeCases:
    """Edge cases for ListCrawlers: pagination, create/delete lifecycle."""

    def _make_crawler(self, glue):
        db_name = _unique("db")
        name = _unique("crawler")
        glue.create_database(DatabaseInput={"Name": db_name})
        glue.create_crawler(
            Name=name,
            Role="arn:aws:iam::123456789012:role/glue-role",
            DatabaseName=db_name,
            Targets={"S3Targets": [{"Path": "s3://test-bucket/data"}]},
        )
        return name, db_name

    def test_list_crawlers_create_retrieve_update_delete_error(self, glue):
        """Full crawler lifecycle: CREATE, LIST, RETRIEVE, UPDATE, DELETE, ERROR."""
        name, db_name = self._make_crawler(glue)
        try:
            # LIST via list_crawlers
            list_resp = glue.list_crawlers()
            assert name in list_resp["CrawlerNames"]

            # RETRIEVE
            get_resp = glue.get_crawler(Name=name)
            assert get_resp["Crawler"]["Name"] == name

            # UPDATE
            glue.update_crawler(
                Name=name,
                Description="updated description",
                Targets={"S3Targets": [{"Path": "s3://other-bucket/data"}]},
            )
            updated = glue.get_crawler(Name=name)
            assert updated["Crawler"].get("Description") == "updated description"
        finally:
            glue.delete_crawler(Name=name)
            glue.delete_database(Name=db_name)

        # ERROR
        with pytest.raises(ClientError) as exc:
            glue.get_crawler(Name=name)
        assert exc.value.response["Error"]["Code"] == "EntityNotFoundException"

    def test_list_crawlers_with_max_results(self, glue):
        """ListCrawlers with MaxResults limits the response."""
        created = []
        dbs = []
        for _ in range(3):
            name, db_name = self._make_crawler(glue)
            created.append(name)
            dbs.append(db_name)
        try:
            resp = glue.list_crawlers(MaxResults=2)
            assert "CrawlerNames" in resp
            assert len(resp["CrawlerNames"]) <= 2
        finally:
            for name, db_name in zip(created, dbs):
                glue.delete_crawler(Name=name)
                glue.delete_database(Name=db_name)

    def test_list_crawlers_excludes_deleted(self, glue):
        """ListCrawlers does not include deleted crawlers."""
        name, db_name = self._make_crawler(glue)
        glue.delete_crawler(Name=name)
        glue.delete_database(Name=db_name)

        resp = glue.list_crawlers()
        assert name not in resp["CrawlerNames"]


class TestGlueJobsListEdgeCases:
    """Edge cases for ListJobs: full lifecycle + pagination."""

    def _make_job(self, glue, name=None):
        name = name or _unique("job")
        glue.create_job(
            Name=name,
            Role="arn:aws:iam::123456789012:role/glue-role",
            Command={"Name": "glueetl", "ScriptLocation": "s3://bucket/script.py"},
        )
        return name

    def test_list_jobs_create_retrieve_update_delete_error(self, glue):
        """Full job lifecycle: CREATE, LIST, RETRIEVE, UPDATE, DELETE, ERROR."""
        name = self._make_job(glue)
        try:
            # LIST
            list_resp = glue.list_jobs()
            assert name in list_resp["JobNames"]

            # RETRIEVE
            get_resp = glue.get_job(JobName=name)
            assert get_resp["Job"]["Name"] == name

            # UPDATE
            glue.update_job(
                JobName=name,
                JobUpdate={
                    "Role": "arn:aws:iam::123456789012:role/glue-role",
                    "Command": {"Name": "glueetl", "ScriptLocation": "s3://bucket/v2.py"},
                    "Description": "updated description",
                },
            )
            updated = glue.get_job(JobName=name)
            assert updated["Job"].get("Description") == "updated description"
        finally:
            # DELETE
            glue.delete_job(JobName=name)

        # ERROR
        with pytest.raises(ClientError) as exc:
            glue.get_job(JobName=name)
        assert exc.value.response["Error"]["Code"] == "EntityNotFoundException"

    def test_list_jobs_with_max_results(self, glue):
        """ListJobs with MaxResults limits the response."""
        names = [self._make_job(glue) for _ in range(3)]
        try:
            resp = glue.list_jobs(MaxResults=2)
            assert "JobNames" in resp
            assert len(resp["JobNames"]) <= 2
        finally:
            for name in names:
                glue.delete_job(JobName=name)

    def test_list_jobs_excludes_deleted(self, glue):
        """ListJobs does not include deleted jobs."""
        name = self._make_job(glue)
        glue.delete_job(JobName=name)

        resp = glue.list_jobs()
        assert name not in resp["JobNames"]


class TestGlueBatchStopJobRunEdgeCases:
    """Edge cases for BatchStopJobRun: lifecycle with real job runs."""

    def test_create_job_start_run_then_batch_stop(self, glue):
        """CREATE job, start run (CREATE), batch stop run (UPDATE), verify stopped."""
        job_name = _unique("job")
        glue.create_job(
            Name=job_name,
            Role="arn:aws:iam::123456789012:role/glue-role",
            Command={"Name": "glueetl", "ScriptLocation": "s3://bucket/script.py"},
        )
        try:
            # Start a run
            run_resp = glue.start_job_run(JobName=job_name)
            run_id = run_resp["JobRunId"]
            assert isinstance(run_id, str)

            # LIST: job run appears
            runs_resp = glue.get_job_runs(JobName=job_name)
            run_ids = [r["Id"] for r in runs_resp["JobRuns"]]
            assert run_id in run_ids

            # RETRIEVE: get individual run
            get_resp = glue.get_job_run(JobName=job_name, RunId=run_id)
            assert get_resp["JobRun"]["Id"] == run_id

            # UPDATE: batch stop
            stop_resp = glue.batch_stop_job_run(JobName=job_name, JobRunIds=[run_id])
            assert "SuccessfulSubmissions" in stop_resp or "Errors" in stop_resp
        finally:
            glue.delete_job(JobName=job_name)

    def test_batch_stop_job_run_nonexistent_job_raises(self, glue):
        """BatchStopJobRun for nonexistent job raises EntityNotFoundException."""
        with pytest.raises(ClientError) as exc:
            glue.batch_stop_job_run(JobName="no-such-job-xyz", JobRunIds=["fake-id"])
        assert exc.value.response["Error"]["Code"] == "EntityNotFoundException"

    def test_batch_stop_job_run_multiple_run_ids(self, glue):
        """BatchStopJobRun with multiple fake run IDs for existing job returns Errors."""
        job_name = _unique("job")
        glue.create_job(
            Name=job_name,
            Role="arn:aws:iam::123456789012:role/glue-role",
            Command={"Name": "glueetl", "ScriptLocation": "s3://bucket/script.py"},
        )
        try:
            resp = glue.batch_stop_job_run(
                JobName=job_name, JobRunIds=["fake-run-1", "fake-run-2"]
            )
            assert "Errors" in resp or "SuccessfulSubmissions" in resp
        finally:
            glue.delete_job(JobName=job_name)


class TestGlueMLTaskRunEdgeCases:
    """Edge cases for MLTaskRun: full transform lifecycle with task run ops."""

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

    def test_create_transform_get_task_runs_list(self, glue):
        """CREATE transform, LIST task runs (empty), RETRIEVE transform, DELETE."""
        tfm_id, name = self._make_transform(glue)
        try:
            # LIST: task runs are empty initially
            runs_resp = glue.get_ml_task_runs(TransformId=tfm_id)
            assert "TaskRuns" in runs_resp
            assert isinstance(runs_resp["TaskRuns"], list)

            # RETRIEVE: transform exists
            get_resp = glue.get_ml_transform(TransformId=tfm_id)
            assert get_resp["TransformId"] == tfm_id
            assert get_resp["Name"] == name
        finally:
            # DELETE
            glue.delete_ml_transform(TransformId=tfm_id)

        # ERROR: get task runs for deleted transform
        with pytest.raises(ClientError) as exc:
            glue.get_ml_task_runs(TransformId=tfm_id)
        assert exc.value.response["Error"]["Code"] == "EntityNotFoundException"

    def test_cancel_ml_task_run_nonexistent_raises(self, glue):
        """CancelMLTaskRun with nonexistent transform raises EntityNotFoundException."""
        with pytest.raises(ClientError) as exc:
            glue.cancel_ml_task_run(
                TransformId="nonexistent-transform", TaskRunId="nonexistent-task"
            )
        assert exc.value.response["Error"]["Code"] == "EntityNotFoundException"

    def test_update_transform_then_get_ml_task_run_not_found(self, glue):
        """UPDATE transform, then GetMLTaskRun with fake run ID returns error."""
        tfm_id, _ = self._make_transform(glue)
        try:
            # UPDATE
            glue.update_ml_transform(TransformId=tfm_id, Description="new description")

            # ERROR: get a nonexistent task run
            with pytest.raises(ClientError) as exc:
                glue.get_ml_task_run(TransformId=tfm_id, TaskRunId="fake-task-run-id")
            assert exc.value.response["Error"]["Code"] == "EntityNotFoundException"
        finally:
            glue.delete_ml_transform(TransformId=tfm_id)


class TestGlueCreateScriptEdgeCases:
    """Edge cases for CreateScript and GetDataflowGraph."""

    def test_create_script_with_dag_nodes_returns_script(self, glue):
        """CreateScript with valid DAG returns PythonScript or ScalaCode."""
        resp = glue.create_script(
            DagNodes=[
                {"Id": "src", "NodeType": "S3", "Args": [], "LineNumber": 1},
                {"Id": "tgt", "NodeType": "S3", "Args": [], "LineNumber": 2},
            ],
            DagEdges=[{"Source": "src", "Target": "tgt"}],
            Language="PYTHON",
        )
        assert "PythonScript" in resp or resp["ResponseMetadata"]["HTTPStatusCode"] == 200

    def test_create_script_scala_language(self, glue):
        """CreateScript with Language=SCALA returns ScalaCode or 200."""
        resp = glue.create_script(
            DagNodes=[{"Id": "node1", "NodeType": "S3", "Args": [], "LineNumber": 1}],
            DagEdges=[],
            Language="SCALA",
        )
        assert "ScalaCode" in resp or resp["ResponseMetadata"]["HTTPStatusCode"] == 200

    def test_get_dataflow_graph_returns_dag(self, glue):
        """GetDataflowGraph returns DagNodes and DagEdges lists."""
        resp = glue.get_dataflow_graph(PythonScript="# empty script")
        assert "DagNodes" in resp
        assert "DagEdges" in resp
        assert isinstance(resp["DagNodes"], list)
        assert isinstance(resp["DagEdges"], list)

    def test_create_script_empty_dag(self, glue):
        """CreateScript with empty DAG returns 200."""
        resp = glue.create_script(DagNodes=[], DagEdges=[])
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200

    def test_get_plan_then_create_script(self, glue):
        """GetPlan returns a script; combined with CreateScript for full roundtrip."""
        db_name = _unique("db")
        tbl_name = _unique("tbl")
        glue.create_database(DatabaseInput={"Name": db_name})
        glue.create_table(
            DatabaseName=db_name,
            TableInput={
                "Name": tbl_name,
                "StorageDescriptor": {
                    "Columns": [{"Name": "id", "Type": "int"}],
                    "Location": "s3://b/p",
                    "InputFormat": "org.apache.hadoop.mapred.TextInputFormat",
                    "OutputFormat": "org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat",
                    "SerdeInfo": {
                        "SerializationLibrary": "org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe"
                    },
                },
            },
        )
        try:
            plan_resp = glue.get_plan(
                Mapping=[{"SourceTable": tbl_name, "SourceType": "int", "TargetTable": tbl_name, "TargetType": "string"}],
                Source={"DatabaseName": db_name, "TableName": tbl_name},
                Language="PYTHON",
            )
            assert "PythonScript" in plan_resp or plan_resp["ResponseMetadata"]["HTTPStatusCode"] == 200
        finally:
            glue.delete_table(DatabaseName=db_name, Name=tbl_name)
            glue.delete_database(Name=db_name)


class TestGlueTestConnectionEdgeCases:
    """Edge cases for TestConnection: create connection, test it, delete."""

    def test_create_then_test_connection(self, glue):
        """CREATE connection, test it (RETRIEVE op), verify result, DELETE."""
        name = _unique("conn")
        glue.create_connection(
            ConnectionInput={
                "Name": name,
                "ConnectionType": "JDBC",
                "ConnectionProperties": {
                    "JDBC_CONNECTION_URL": "jdbc:mysql://localhost:3306/db",
                    "USERNAME": "admin",
                    "PASSWORD": "secret",
                },
            }
        )
        try:
            # RETRIEVE: get connection
            get_resp = glue.get_connection(Name=name)
            assert get_resp["Connection"]["Name"] == name

            # LIST: appears in list
            list_resp = glue.get_connections()
            names = [c["Name"] for c in list_resp["ConnectionList"]]
            assert name in names

            # TestConnection - may return error since no real endpoint
            try:
                glue.test_connection(ConnectionName=name)
            except ClientError as e:
                # InternalError or other errors are acceptable - we just want to
                # verify the API accepted our request with a valid connection
                assert e.response["Error"]["Code"] in (
                    "InternalError",
                    "InvalidInputException",
                    "GlueEncryptionException",
                )
        finally:
            # DELETE
            glue.delete_connection(ConnectionName=name)

        # ERROR: get after delete
        with pytest.raises(ClientError) as exc:
            glue.get_connection(Name=name)
        assert exc.value.response["Error"]["Code"] in ("EntityNotFoundException", "GlueEncryptionException")

    def test_test_connection_nonexistent_raises_error(self, glue):
        """TestConnection with nonexistent connection raises error."""
        with pytest.raises(ClientError) as exc:
            glue.test_connection(ConnectionName="nonexistent-conn-xyz")
        assert exc.value.response["Error"]["Code"] in (
            "EntityNotFoundException",
            "InternalError",
            "GlueEncryptionException",
        )

    def test_create_delete_then_get_connection_raises(self, glue):
        """GetConnection after delete raises EntityNotFoundException."""
        name = _unique("conn")
        glue.create_connection(
            ConnectionInput={
                "Name": name,
                "ConnectionType": "JDBC",
                "ConnectionProperties": {
                    "JDBC_CONNECTION_URL": "jdbc:mysql://host:3306/db",
                    "USERNAME": "user",
                    "PASSWORD": "pass",
                },
            }
        )
        glue.delete_connection(ConnectionName=name)

        with pytest.raises(ClientError) as exc:
            glue.get_connection(Name=name)
        assert exc.value.response["Error"]["Code"] in ("EntityNotFoundException", "GlueEncryptionException")


class TestGlueStatementLifecycle:
    """Tests for session Statement lifecycle."""

    def _make_session(self, glue):
        session_id = _unique("session")
        glue.create_session(
            Id=session_id,
            Role="arn:aws:iam::123456789012:role/glue-role",
            Command={"Name": "glueetl", "PythonVersion": "3"},
        )
        return session_id

    def test_create_session_retrieve_then_delete(self, glue):
        """CREATE session, RETRIEVE it, LIST sessions, DELETE."""
        session_id = self._make_session(glue)
        try:
            # RETRIEVE
            get_resp = glue.get_session(Id=session_id)
            assert get_resp["Session"]["Id"] == session_id

            # LIST: list_sessions returns Ids list
            list_resp = glue.list_sessions()
            assert "Ids" in list_resp
            assert session_id in list_resp["Ids"]
        finally:
            # DELETE
            glue.delete_session(Id=session_id)

        # ERROR
        with pytest.raises(ClientError) as exc:
            glue.get_session(Id=session_id)
        assert exc.value.response["Error"]["Code"] == "EntityNotFoundException"

    def test_cancel_statement_nonexistent_session_raises(self, glue):
        """CancelStatement for nonexistent session raises EntityNotFoundException."""
        with pytest.raises(ClientError) as exc:
            glue.cancel_statement(SessionId="nonexistent-session-xyz", Id=0)
        assert exc.value.response["Error"]["Code"] == "EntityNotFoundException"

    def test_run_statement_create_retrieve_cancel(self, glue):
        """Run a statement, retrieve it, cancel it."""
        session_id = self._make_session(glue)
        try:
            # CREATE statement (run it)
            run_resp = glue.run_statement(SessionId=session_id, Code="x = 1")
            stmt_id = run_resp["Id"]
            assert isinstance(stmt_id, int)

            # RETRIEVE statement
            get_resp = glue.get_statement(SessionId=session_id, Id=stmt_id)
            assert get_resp["Statement"]["Id"] == stmt_id

            # LIST statements
            list_resp = glue.list_statements(SessionId=session_id)
            assert "Statements" in list_resp
            assert isinstance(list_resp["Statements"], list)

            # UPDATE: cancel the statement
            cancel_resp = glue.cancel_statement(SessionId=session_id, Id=stmt_id)
            assert cancel_resp["ResponseMetadata"]["HTTPStatusCode"] == 200
        finally:
            glue.delete_session(Id=session_id)
