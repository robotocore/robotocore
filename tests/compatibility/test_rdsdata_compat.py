"""RDS Data API compatibility tests."""

import pytest

from tests.compatibility.conftest import make_client


@pytest.fixture
def rds_data():
    return make_client("rds-data")


class TestRDSDataOperations:
    def test_execute_statement(self, rds_data):
        response = rds_data.execute_statement(
            resourceArn="arn:aws:rds:us-east-1:123456789012:cluster:test",
            secretArn="arn:aws:secretsmanager:us-east-1:123456789012:secret:test",
            sql="SELECT 1",
        )
        assert response["ResponseMetadata"]["HTTPStatusCode"] == 200
        assert "records" in response
