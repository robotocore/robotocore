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

    def test_cancel_statement(self, redshiftdata):
        exec_resp = redshiftdata.execute_statement(
            ClusterIdentifier="test-cluster",
            Database="dev",
            Sql="SELECT 1",
        )
        stmt_id = exec_resp["Id"]
        resp = redshiftdata.cancel_statement(Id=stmt_id)
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200
        assert "Status" in resp

    def test_cancel_statement_not_found(self, redshiftdata):
        with pytest.raises(redshiftdata.exceptions.ResourceNotFoundException):
            redshiftdata.cancel_statement(Id=str(uuid.uuid4()))

    def test_get_statement_result(self, redshiftdata):
        exec_resp = redshiftdata.execute_statement(
            ClusterIdentifier="test-cluster",
            Database="dev",
            Sql="SELECT 1",
        )
        stmt_id = exec_resp["Id"]
        resp = redshiftdata.get_statement_result(Id=stmt_id)
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200
        assert "Records" in resp

    def test_get_statement_result_not_found(self, redshiftdata):
        with pytest.raises(redshiftdata.exceptions.ResourceNotFoundException):
            redshiftdata.get_statement_result(Id=str(uuid.uuid4()))
