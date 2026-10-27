"""RDS Data API compatibility tests."""

import pytest
from botocore.exceptions import ClientError

from tests.compatibility.conftest import make_client

SECRET_ARN = "arn:aws:secretsmanager:us-east-1:123456789012:secret:test"


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
            secretArn=SECRET_ARN,
            sql="SELECT 1",
        )
        assert response["ResponseMetadata"]["HTTPStatusCode"] == 200
        assert "records" in response

    def test_execute_statement_moto_default_shape(self, rds_data):
        # moto's RDSDataServiceBackend returns QueryResults(records=[]) when
        # no results are pre-configured: `records` is an empty list and the
        # other four response members are serialized as null (botocore drops
        # them), no matter what the request said.
        response = rds_data.execute_statement(
            resourceArn="arn:aws:rds:us-east-1:123456789012:cluster:not-created",
            secretArn=SECRET_ARN,
            sql="SELECT * FROM anything",
        )
        assert response["ResponseMetadata"]["HTTPStatusCode"] == 200
        assert response["records"] == []
        assert response.get("columnMetadata") is None
        assert response.get("numberOfRecordsUpdated") is None
        assert response.get("generatedFields") is None
        assert response.get("formattedRecords") is None

    def test_execute_statement_validates_nothing(self, rds_data):
        # moto validates none of the input parameters: unknown clusters,
        # unknown transactions and DML statements all yield 200 + records=[],
        # and the full optional field set is accepted.
        response = rds_data.execute_statement(
            resourceArn="arn:aws:rds:eu-west-2:123456789012:cluster:no-such-thing",
            secretArn="arn:aws:secretsmanager:eu-west-2:123456789012:secret:missing",
            sql="INSERT INTO t VALUES (:a)",
            database="mydb",
            schema="public",
            parameters=[
                {"name": "a", "value": {"stringValue": "x"}},
                {"name": "b", "value": {"longValue": 2}},
            ],
            transactionId="abc123transaction",
            includeResultMetadata=True,
            continueAfterTimeout=False,
            formatRecordsAs="JSON",
            resultSetOptions={"decimalReturnType": "DOUBLE", "longReturnType": "LONG"},
        )
        assert response["ResponseMetadata"]["HTTPStatusCode"] == 200
        assert response["records"] == []
        assert response.get("columnMetadata") is None


class TestRDSDataNotImplementedSurface:
    """moto's rdsdata module routes exactly one operation.

    `moto/rdsdata/urls.py` contains a single url_path — `POST /Execute` —
    and `RDSDataServiceResponse` implements only `execute_statement`. The
    rest of the 2018-08-01 botocore surface (including the deprecated
    ExecuteSql) has never had a moto handler, so the rotocore port answers
    the crate's verbatim NotImplemented for them.
    """

    @pytest.mark.parametrize(
        "op,kwargs",
        [
            (
                "batch_execute_statement",
                {
                    "resourceArn": "arn:aws:rds:us-east-1:123456789012:cluster:c",
                    "secretArn": SECRET_ARN,
                    "sql": "SELECT 1",
                },
            ),
            (
                "begin_transaction",
                {
                    "resourceArn": "arn:aws:rds:us-east-1:123456789012:cluster:c",
                    "secretArn": SECRET_ARN,
                },
            ),
            (
                "commit_transaction",
                {
                    "resourceArn": "arn:aws:rds:us-east-1:123456789012:cluster:c",
                    "secretArn": SECRET_ARN,
                    "transactionId": "abc123",
                },
            ),
            (
                "rollback_transaction",
                {
                    "resourceArn": "arn:aws:rds:us-east-1:123456789012:cluster:c",
                    "secretArn": SECRET_ARN,
                    "transactionId": "abc123",
                },
            ),
            (
                "execute_sql",
                {
                    "dbClusterOrInstanceArn": "arn:aws:rds:us-east-1:123456789012:cluster:c",
                    "awsSecretStoreArn": SECRET_ARN,
                    "sqlStatements": "SELECT 1",
                },
            ),
        ],
    )
    def test_unimplemented_ops_answer_not_implemented(self, rds_data, op, kwargs):
        with pytest.raises(ClientError) as exc:
            getattr(rds_data, op)(**kwargs)
        error = exc.value.response["Error"]
        assert error["Code"] == "NotImplemented", op
        assert error["Message"].startswith("The "), op

