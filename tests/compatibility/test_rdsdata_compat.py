"""RDS Data API compatibility tests."""

import pytest

from tests.compatibility.conftest import make_client


@pytest.fixture
def rds_data():
    return make_client("rds-data")


@pytest.fixture
def rds():
    return make_client("rds")


@pytest.fixture(scope="module")
def rds_cluster_module():
    """Create an RDS cluster shared across all tests in this module."""
    rds = make_client("rds")
    cluster_id = "rdsdata-compat-test"
    try:
        rds.create_db_cluster(
            DBClusterIdentifier=cluster_id,
            Engine="aurora-mysql",
            MasterUsername="admin",
            MasterUserPassword="password123",
            EnableHttpEndpoint=True,
        )
    except Exception:
        pass  # already exists from prior run is fine — engine still attached
    yield cluster_id
    try:
        rds.delete_db_cluster(DBClusterIdentifier=cluster_id, SkipFinalSnapshot=True)
    except Exception:
        pass  # best-effort cleanup


@pytest.fixture
def rds_cluster(rds_cluster_module):
    return rds_cluster_module


@pytest.fixture
def cluster_arn(rds_cluster):
    return f"arn:aws:rds:us-east-1:123456789012:cluster:{rds_cluster}"


SECRET_ARN = "arn:aws:secretsmanager:us-east-1:123456789012:secret:test"


class TestExecuteStatement:
    def test_execute_statement_select(self, rds_data, cluster_arn):
        response = rds_data.execute_statement(
            resourceArn=cluster_arn,
            secretArn=SECRET_ARN,
            sql="SELECT 1",
        )
        assert response["ResponseMetadata"]["HTTPStatusCode"] == 200
        assert "records" in response
        assert response["records"] == [[{"longValue": 1}]]

    def test_execute_statement_returns_number_of_records_updated(self, rds_data, cluster_arn):
        response = rds_data.execute_statement(
            resourceArn=cluster_arn,
            secretArn=SECRET_ARN,
            sql="SELECT 1",
        )
        assert "numberOfRecordsUpdated" in response

    def test_execute_statement_with_column_metadata(self, rds_data, cluster_arn):
        response = rds_data.execute_statement(
            resourceArn=cluster_arn,
            secretArn=SECRET_ARN,
            sql="SELECT 42 AS answer",
            includeResultMetadata=True,
        )
        assert "columnMetadata" in response
        assert len(response["columnMetadata"]) == 1
        assert response["columnMetadata"][0]["name"] == "answer"

    def test_execute_statement_create_table(self, rds_data, cluster_arn):
        response = rds_data.execute_statement(
            resourceArn=cluster_arn,
            secretArn=SECRET_ARN,
            sql="CREATE TABLE IF NOT EXISTS rdsdata_test_table (id INTEGER, name TEXT)",
        )
        assert response["ResponseMetadata"]["HTTPStatusCode"] == 200
        assert "numberOfRecordsUpdated" in response

    def test_execute_statement_insert_and_select(self, rds_data, cluster_arn):
        rds_data.execute_statement(
            resourceArn=cluster_arn,
            secretArn=SECRET_ARN,
            sql="CREATE TABLE IF NOT EXISTS rdsdata_insert_test (id INTEGER)",
        )
        insert_response = rds_data.execute_statement(
            resourceArn=cluster_arn,
            secretArn=SECRET_ARN,
            sql="INSERT INTO rdsdata_insert_test VALUES (99)",
        )
        assert insert_response["numberOfRecordsUpdated"] == 1

        select_response = rds_data.execute_statement(
            resourceArn=cluster_arn,
            secretArn=SECRET_ARN,
            sql="SELECT id FROM rdsdata_insert_test WHERE id = 99",
        )
        assert select_response["records"] == [[{"longValue": 99}]]


class TestBatchExecuteStatement:
    def test_batch_execute_statement_empty_param_sets(self, rds_data, cluster_arn):
        response = rds_data.batch_execute_statement(
            resourceArn=cluster_arn,
            secretArn=SECRET_ARN,
            sql="SELECT 1",
            parameterSets=[],
        )
        assert response["ResponseMetadata"]["HTTPStatusCode"] == 200
        assert "updateResults" in response
        assert response["updateResults"] == []

    def test_batch_execute_statement_with_params(self, rds_data, cluster_arn):
        rds_data.execute_statement(
            resourceArn=cluster_arn,
            secretArn=SECRET_ARN,
            sql="CREATE TABLE IF NOT EXISTS rdsdata_batch_test (val INTEGER)",
        )
        response = rds_data.batch_execute_statement(
            resourceArn=cluster_arn,
            secretArn=SECRET_ARN,
            sql="INSERT INTO rdsdata_batch_test VALUES (:val)",
            parameterSets=[
                [{"name": "val", "value": {"longValue": 1}}],
                [{"name": "val", "value": {"longValue": 2}}],
            ],
        )
        assert response["ResponseMetadata"]["HTTPStatusCode"] == 200
        assert "updateResults" in response
        assert len(response["updateResults"]) == 2


class TestTransactions:
    def test_begin_transaction_returns_transaction_id(self, rds_data, cluster_arn):
        response = rds_data.begin_transaction(
            resourceArn=cluster_arn,
            secretArn=SECRET_ARN,
        )
        assert response["ResponseMetadata"]["HTTPStatusCode"] == 200
        assert "transactionId" in response
        assert len(response["transactionId"]) > 0

    def test_commit_transaction(self, rds_data, cluster_arn):
        begin_response = rds_data.begin_transaction(
            resourceArn=cluster_arn,
            secretArn=SECRET_ARN,
        )
        tx_id = begin_response["transactionId"]

        commit_response = rds_data.commit_transaction(
            resourceArn=cluster_arn,
            secretArn=SECRET_ARN,
            transactionId=tx_id,
        )
        assert commit_response["ResponseMetadata"]["HTTPStatusCode"] == 200
        assert "transactionStatus" in commit_response
        assert "Committed" in commit_response["transactionStatus"]

    def test_rollback_transaction(self, rds_data, cluster_arn):
        begin_response = rds_data.begin_transaction(
            resourceArn=cluster_arn,
            secretArn=SECRET_ARN,
        )
        tx_id = begin_response["transactionId"]

        rollback_response = rds_data.rollback_transaction(
            resourceArn=cluster_arn,
            secretArn=SECRET_ARN,
            transactionId=tx_id,
        )
        assert rollback_response["ResponseMetadata"]["HTTPStatusCode"] == 200
        assert "transactionStatus" in rollback_response
        assert rollback_response["transactionStatus"] == "Transaction Rolledback"

    def test_execute_in_transaction(self, rds_data, cluster_arn):
        rds_data.execute_statement(
            resourceArn=cluster_arn,
            secretArn=SECRET_ARN,
            sql="CREATE TABLE IF NOT EXISTS rdsdata_tx_test (val INTEGER)",
        )

        begin_response = rds_data.begin_transaction(
            resourceArn=cluster_arn,
            secretArn=SECRET_ARN,
        )
        tx_id = begin_response["transactionId"]

        rds_data.execute_statement(
            resourceArn=cluster_arn,
            secretArn=SECRET_ARN,
            sql="INSERT INTO rdsdata_tx_test VALUES (42)",
            transactionId=tx_id,
        )

        rds_data.commit_transaction(
            resourceArn=cluster_arn,
            secretArn=SECRET_ARN,
            transactionId=tx_id,
        )

        select_response = rds_data.execute_statement(
            resourceArn=cluster_arn,
            secretArn=SECRET_ARN,
            sql="SELECT val FROM rdsdata_tx_test WHERE val = 42",
        )
        assert select_response["records"] == [[{"longValue": 42}]]


class TestExecuteSql:
    def test_execute_sql_select(self, rds_data, cluster_arn):
        response = rds_data.execute_sql(
            awsSecretStoreArn=SECRET_ARN,
            dbClusterOrInstanceArn=cluster_arn,
            sqlStatements="SELECT 1",
        )
        assert response["ResponseMetadata"]["HTTPStatusCode"] == 200
        assert "sqlStatementResults" in response
        assert len(response["sqlStatementResults"]) == 1

    def test_execute_sql_result_frame(self, rds_data, cluster_arn):
        response = rds_data.execute_sql(
            awsSecretStoreArn=SECRET_ARN,
            dbClusterOrInstanceArn=cluster_arn,
            sqlStatements="SELECT 1",
        )
        result = response["sqlStatementResults"][0]
        assert "resultFrame" in result
        rf = result["resultFrame"]
        assert "resultSetMetadata" in rf
        assert "records" in rf
        assert rf["records"] == [{"values": [{"bigIntValue": 1}]}]

    def test_execute_sql_multiple_statements(self, rds_data, cluster_arn):
        response = rds_data.execute_sql(
            awsSecretStoreArn=SECRET_ARN,
            dbClusterOrInstanceArn=cluster_arn,
            sqlStatements="SELECT 1; SELECT 2",
        )
        assert len(response["sqlStatementResults"]) == 2
        assert response["sqlStatementResults"][0]["resultFrame"]["records"] == [
            {"values": [{"bigIntValue": 1}]}
        ]
        assert response["sqlStatementResults"][1]["resultFrame"]["records"] == [
            {"values": [{"bigIntValue": 2}]}
        ]

    def test_execute_sql_dml(self, rds_data, cluster_arn):
        rds_data.execute_sql(
            awsSecretStoreArn=SECRET_ARN,
            dbClusterOrInstanceArn=cluster_arn,
            sqlStatements="CREATE TABLE IF NOT EXISTS rdsdata_execsql_test (id INTEGER)",
        )
        response = rds_data.execute_sql(
            awsSecretStoreArn=SECRET_ARN,
            dbClusterOrInstanceArn=cluster_arn,
            sqlStatements="INSERT INTO rdsdata_execsql_test VALUES (5)",
        )
        assert response["sqlStatementResults"][0]["numberOfRecordsUpdated"] == 1
