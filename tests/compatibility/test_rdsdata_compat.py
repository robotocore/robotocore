"""RDS Data API compatibility tests."""

import pytest

from tests.compatibility.conftest import make_client


@pytest.fixture
def rds_data():
    return make_client("rds-data")


@pytest.fixture
def rds():
    return make_client("rds")


@pytest.fixture
def rds_cluster(rds):
    """Create an RDS cluster that RDS Data API requires."""
    cluster_id = "rdsdata-compat-test"
    rds.create_db_cluster(
        DBClusterIdentifier=cluster_id,
        Engine="aurora-mysql",
        MasterUsername="admin",
        MasterUserPassword="password123",
        EnableHttpEndpoint=True,
    )
    yield cluster_id
    try:
        rds.delete_db_cluster(DBClusterIdentifier=cluster_id, SkipFinalSnapshot=True)
    except Exception:
        pass  # best-effort cleanup


class TestRDSDataOperations:
    def test_execute_statement(self, rds_data, rds_cluster):
        response = rds_data.execute_statement(
            resourceArn=f"arn:aws:rds:us-east-1:123456789012:cluster:{rds_cluster}",
            secretArn="arn:aws:secretsmanager:us-east-1:123456789012:secret:test",
            sql="SELECT 1",
        )
        assert response["ResponseMetadata"]["HTTPStatusCode"] == 200
        assert "records" in response
