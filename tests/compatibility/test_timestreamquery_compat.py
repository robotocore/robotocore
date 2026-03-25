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
