"""RDS Data API compatibility tests."""

import pytest

from tests.compatibility.conftest import make_client


@pytest.fixture
def rds():
    return make_client("rds")


@pytest.fixture
def rds_data():
    return make_client("rds-data")


class TestRDSDataOperations:
    def test_execute_statement(self, rds, rds_data):
        # Create a DB instance first — the native provider requires it
        rds.create_db_instance(
            DBInstanceIdentifier="test-db",
            DBInstanceClass="db.t3.micro",
            Engine="postgres",
            MasterUsername="admin",
            MasterUserPassword="password123",
        )
        try:
            response = rds_data.execute_statement(
                resourceArn="arn:aws:rds:us-east-1:123456789012:db:test-db",
                secretArn="arn:aws:secretsmanager:us-east-1:123456789012:secret:test",
                sql="SELECT 1",
            )
            assert response["ResponseMetadata"]["HTTPStatusCode"] == 200
            assert "records" in response
        finally:
            rds.delete_db_instance(
                DBInstanceIdentifier="test-db",
                SkipFinalSnapshot=True,
            )
