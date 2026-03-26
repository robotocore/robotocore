"""Timestream Query compatibility tests."""

import pytest

from tests.compatibility.conftest import make_client


@pytest.fixture
def timestream_query():
    return make_client("timestream-query")


class TestTimestreamQueryOperations:
    def test_describe_endpoints(self, timestream_query):
        response = timestream_query.describe_endpoints()
        assert response["ResponseMetadata"]["HTTPStatusCode"] == 200
        assert "Endpoints" in response
        assert len(response["Endpoints"]) >= 1
        endpoint = response["Endpoints"][0]
        assert "Address" in endpoint
        assert "CachePeriodInMinutes" in endpoint


class TestTimestreamQueryScheduledQuery:
    """Tests for scheduled query CRUD operations."""

    @pytest.fixture
    def client(self):
        return make_client("timestream-query")

    @pytest.fixture
    def scheduled_query_arn(self, client):
        """Create a scheduled query and return its ARN, deleting after test."""
        resp = client.create_scheduled_query(
            Name="test-query-fixture",
            QueryString="SELECT 1",
            ScheduleConfiguration={"ScheduleExpression": "rate(1 hour)"},
            NotificationConfiguration={
                "SnsConfiguration": {"TopicArn": "arn:aws:sns:us-east-1:123456789012:test-topic"}
            },
            ScheduledQueryExecutionRoleArn="arn:aws:iam::123456789012:role/test-role",
            ErrorReportConfiguration={
                "S3Configuration": {
                    "BucketName": "test-bucket",
                    "EncryptionOption": "SSE_S3",
                }
            },
        )
        arn = resp["Arn"]
        yield arn
        try:
            client.delete_scheduled_query(ScheduledQueryArn=arn)
        except Exception:
            pass

    def test_create_scheduled_query(self, client):
        """CreateScheduledQuery returns an ARN."""
        resp = client.create_scheduled_query(
            Name="test-create-sq",
            QueryString="SELECT 1",
            ScheduleConfiguration={"ScheduleExpression": "rate(2 hours)"},
            NotificationConfiguration={
                "SnsConfiguration": {
                    "TopicArn": "arn:aws:sns:us-east-1:123456789012:test-create-topic"
                }
            },
            ScheduledQueryExecutionRoleArn="arn:aws:iam::123456789012:role/test-role",
            ErrorReportConfiguration={
                "S3Configuration": {
                    "BucketName": "test-bucket",
                    "EncryptionOption": "SSE_S3",
                }
            },
        )
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200
        assert "Arn" in resp
        assert "scheduled-query" in resp["Arn"]
        # Cleanup
        client.delete_scheduled_query(ScheduledQueryArn=resp["Arn"])

    def test_describe_scheduled_query(self, client, scheduled_query_arn):
        """DescribeScheduledQuery returns details of a scheduled query."""
        resp = client.describe_scheduled_query(ScheduledQueryArn=scheduled_query_arn)
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200
        assert "ScheduledQuery" in resp
        sq = resp["ScheduledQuery"]
        assert sq["Arn"] == scheduled_query_arn
        assert sq["Name"] == "test-query-fixture"
        assert sq["QueryString"] == "SELECT 1"
        assert "State" in sq

    def test_describe_scheduled_query_not_found(self, client):
        """DescribeScheduledQuery raises ResourceNotFoundException for unknown ARN."""
        with pytest.raises(client.exceptions.ResourceNotFoundException):
            client.describe_scheduled_query(
                ScheduledQueryArn="arn:aws:timestream:us-east-1:123456789012:scheduled-query/does-not-exist"
            )

    def test_delete_scheduled_query(self, client):
        """DeleteScheduledQuery removes a scheduled query."""
        create_resp = client.create_scheduled_query(
            Name="test-delete-sq",
            QueryString="SELECT 1",
            ScheduleConfiguration={"ScheduleExpression": "rate(1 hour)"},
            NotificationConfiguration={
                "SnsConfiguration": {"TopicArn": "arn:aws:sns:us-east-1:123456789012:delete-topic"}
            },
            ScheduledQueryExecutionRoleArn="arn:aws:iam::123456789012:role/test-role",
            ErrorReportConfiguration={
                "S3Configuration": {
                    "BucketName": "test-bucket",
                    "EncryptionOption": "SSE_S3",
                }
            },
        )
        arn = create_resp["Arn"]
        del_resp = client.delete_scheduled_query(ScheduledQueryArn=arn)
        assert del_resp["ResponseMetadata"]["HTTPStatusCode"] == 200
        # Verify it's gone
        with pytest.raises(client.exceptions.ResourceNotFoundException):
            client.describe_scheduled_query(ScheduledQueryArn=arn)


class TestTimestreamQueryQuery:
    """Tests for the Query operation."""

    @pytest.fixture
    def client(self):
        return make_client("timestream-query")

    def test_query(self, client):
        """Query returns a QueryId and Rows."""
        resp = client.query(QueryString="SELECT 1")
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200
        assert "QueryId" in resp
        assert "Rows" in resp
        assert isinstance(resp["Rows"], list)


class TestTimestreamQueryTagging:
    """Tests for ListTagsForResource on timestream-query resources."""

    @pytest.fixture
    def client(self):
        return make_client("timestream-query")

    @pytest.fixture
    def scheduled_query_arn(self, client):
        """Create a scheduled query and return its ARN."""
        resp = client.create_scheduled_query(
            Name="test-tagging-sq",
            QueryString="SELECT 1",
            ScheduleConfiguration={"ScheduleExpression": "rate(1 hour)"},
            NotificationConfiguration={
                "SnsConfiguration": {
                    "TopicArn": "arn:aws:sns:us-east-1:123456789012:test-tag-topic"
                }
            },
            ScheduledQueryExecutionRoleArn="arn:aws:iam::123456789012:role/test-role",
            ErrorReportConfiguration={
                "S3Configuration": {
                    "BucketName": "test-bucket",
                    "EncryptionOption": "SSE_S3",
                }
            },
        )
        arn = resp["Arn"]
        yield arn
        try:
            client.delete_scheduled_query(ScheduledQueryArn=arn)
        except Exception:
            pass

    def test_list_tags_for_resource(self, client, scheduled_query_arn):
        """ListTagsForResource returns Tags list for a scheduled query."""
        resp = client.list_tags_for_resource(ResourceARN=scheduled_query_arn)
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200
        assert "Tags" in resp
        assert isinstance(resp["Tags"], list)


class TestTimestreamQueryMissingOps:
    """Tests for previously-missing operations: ListScheduledQueries, DescribeAccountSettings,
    PrepareQuery, CancelQuery, ExecuteScheduledQuery."""

    @pytest.fixture
    def client(self):
        return make_client("timestream-query")

    @pytest.fixture
    def scheduled_query_arn(self, client):
        """Create a scheduled query and return its ARN."""
        resp = client.create_scheduled_query(
            Name="test-missing-ops-sq",
            QueryString="SELECT 1",
            ScheduleConfiguration={"ScheduleExpression": "rate(1 hour)"},
            NotificationConfiguration={
                "SnsConfiguration": {
                    "TopicArn": "arn:aws:sns:us-east-1:123456789012:test-missing-topic"
                }
            },
            ScheduledQueryExecutionRoleArn="arn:aws:iam::123456789012:role/test-role",
            ErrorReportConfiguration={
                "S3Configuration": {
                    "BucketName": "test-bucket",
                    "EncryptionOption": "SSE_S3",
                }
            },
        )
        arn = resp["Arn"]
        yield arn
        try:
            client.delete_scheduled_query(ScheduledQueryArn=arn)
        except Exception:
            pass

    def test_list_scheduled_queries(self, client):
        """ListScheduledQueries returns ScheduledQueries list."""
        resp = client.list_scheduled_queries()
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200
        assert "ScheduledQueries" in resp
        assert isinstance(resp["ScheduledQueries"], list)

    def test_list_scheduled_queries_shows_created(self, client, scheduled_query_arn):
        """ListScheduledQueries returns the created scheduled query."""
        resp = client.list_scheduled_queries()
        arns = [sq["Arn"] for sq in resp["ScheduledQueries"]]
        assert scheduled_query_arn in arns

    def test_describe_account_settings(self, client):
        """DescribeAccountSettings returns MaxQueryTCU and QueryPricingModel."""
        resp = client.describe_account_settings()
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200
        assert "MaxQueryTCU" in resp
        assert "QueryPricingModel" in resp

    def test_prepare_query(self, client):
        """PrepareQuery returns QueryString, Columns, and Parameters."""
        resp = client.prepare_query(QueryString="SELECT 1")
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200
        assert "QueryString" in resp
        assert "Columns" in resp
        assert "Parameters" in resp
        assert resp["QueryString"] == "SELECT 1"

    def test_cancel_query(self, client):
        """CancelQuery returns CancellationMessage."""
        resp = client.cancel_query(QueryId="test-query-id-12345")
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200
        assert "CancellationMessage" in resp

    def test_execute_scheduled_query(self, client, scheduled_query_arn):
        """ExecuteScheduledQuery returns 200."""
        resp = client.execute_scheduled_query(
            ScheduledQueryArn=scheduled_query_arn,
            InvocationTime="2024-01-01T00:00:00Z",
        )
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200
