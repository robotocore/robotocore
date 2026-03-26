"""Compatibility tests for Redshift Data API service."""

import uuid

import pytest

from tests.compatibility.conftest import make_client


@pytest.fixture
def redshiftdata():
    return make_client("redshift-data")


class TestRedshiftDataOperations:
    """Tests for Redshift Data API operations."""

    def test_execute_statement(self, redshiftdata):
        resp = redshiftdata.execute_statement(
            ClusterIdentifier="test-cluster",
            Database="dev",
            Sql="SELECT 1",
        )
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200
        assert "Id" in resp
        assert resp["Database"] == "dev"
        assert resp["ClusterIdentifier"] == "test-cluster"

    def test_describe_statement(self, redshiftdata):
        exec_resp = redshiftdata.execute_statement(
            ClusterIdentifier="test-cluster",
            Database="dev",
            Sql="SELECT 1",
        )
        stmt_id = exec_resp["Id"]
        resp = redshiftdata.describe_statement(Id=stmt_id)
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200
        assert resp["Id"] == stmt_id

    def test_describe_statement_not_found(self, redshiftdata):
        with pytest.raises(redshiftdata.exceptions.ResourceNotFoundException):
            redshiftdata.describe_statement(Id=str(uuid.uuid4()))

    def test_list_databases(self, redshiftdata):
        resp = redshiftdata.list_databases(
            Database="dev",
            ClusterIdentifier="test-cluster",
        )
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200
        assert "Databases" in resp
        assert "dev" in resp["Databases"]

    def test_list_schemas(self, redshiftdata):
        resp = redshiftdata.list_schemas(
            Database="dev",
            ClusterIdentifier="test-cluster",
        )
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200
        assert "Schemas" in resp
        assert len(resp["Schemas"]) >= 1

    def test_list_tables(self, redshiftdata):
        resp = redshiftdata.list_tables(
            Database="dev",
            ClusterIdentifier="test-cluster",
        )
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200
        assert "Tables" in resp

    def test_describe_table(self, redshiftdata):
        resp = redshiftdata.describe_table(
            Database="dev",
            ClusterIdentifier="test-cluster",
            Table="users",
        )
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200
        assert "TableName" in resp

    def test_list_statements(self, redshiftdata):
        # Create a statement first
        redshiftdata.execute_statement(
            ClusterIdentifier="test-cluster",
            Database="dev",
            Sql="SELECT 1",
        )
        resp = redshiftdata.list_statements()
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200
        assert "Statements" in resp
        assert len(resp["Statements"]) >= 1

    def test_batch_execute_statement(self, redshiftdata):
        resp = redshiftdata.batch_execute_statement(
            ClusterIdentifier="test-cluster",
            Database="dev",
            Sqls=["SELECT 1", "SELECT 2"],
        )
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200
        assert "Id" in resp

    def test_get_statement_result_v2(self, redshiftdata):
        exec_resp = redshiftdata.execute_statement(
            ClusterIdentifier="test-cluster",
            Database="dev",
            Sql="SELECT 1",
        )
        stmt_id = exec_resp["Id"]
        resp = redshiftdata.get_statement_result_v2(Id=stmt_id)
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200
        assert "Records" in resp

    def test_get_statement_result_v2_not_found(self, redshiftdata):
        with pytest.raises(redshiftdata.exceptions.ResourceNotFoundException):
            redshiftdata.get_statement_result_v2(Id=str(uuid.uuid4()))
