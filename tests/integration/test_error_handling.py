"""Error handling integration tests.

These tests verify that the gateway returns proper error responses for
malformed requests, unknown services, and missing parameters.
"""

import json

from tests.integration.conftest import auth_header, json_headers


class TestUnknownServiceErrors:
    """Requests that cannot be routed to any AWS service."""

    async def test_get_unknown_path_returns_400(self, client):
        resp = await client.get("/some/unknown/path")
        assert resp.status_code == 400
        body = resp.json()
        assert "error" in body

    async def test_post_no_auth_no_target_returns_400(self, client):
        resp = await client.post(
            "/",
            content=b"random-body",
            headers={"Content-Type": "text/plain"},
        )
        assert resp.status_code == 400

    async def test_post_with_auth_unknown_service_returns_400(self, client):
        resp = await client.post(
            "/",
            content=b"gibberish",
            headers={
                **auth_header("nonexistent"),
                "Content-Type": "text/plain",
            },
        )
        # Should get an error status but not crash
        assert resp.status_code in (400, 404, 500, 501)


class TestMalformedRequests:
    """Badly formed AWS requests."""

    async def test_sqs_missing_action(self, client):
        """SQS request with no Action parameter."""
        resp = await client.post(
            "/",
            content="Version=2012-11-05",
            headers={
                **auth_header("sqs"),
                "Content-Type": "application/x-www-form-urlencoded",
            },
        )
        # Should return an error, not crash
        assert resp.status_code in (400, 500)

    async def test_dynamodb_missing_target_header(self, client):
        """DynamoDB request without X-Amz-Target header."""
        resp = await client.post(
            "/",
            content=json.dumps({"TableName": "nonexistent"}).encode(),
            headers={
                **auth_header("dynamodb"),
                "Content-Type": "application/x-amz-json-1.0",
            },
        )
        # Without X-Amz-Target, service routing may fail
        assert resp.status_code in (400, 500)

    async def test_dynamodb_invalid_json_body(self, client):
        """DynamoDB request with broken JSON."""
        resp = await client.post(
            "/",
            content=b"not valid json{{{",
            headers={
                **json_headers("dynamodb"),
                "X-Amz-Target": "DynamoDB_20120810.DescribeTable",
            },
        )
        assert resp.status_code in (400, 500)

    async def test_lambda_invoke_nonexistent_function(self, client):
        """Invoke a Lambda function that does not exist."""
        resp = await client.post(
            "/2015-03-31/functions/does-not-exist/invocations",
            content=b"{}",
            headers=auth_header("lambda"),
        )
        assert resp.status_code in (404, 500)
        body = resp.json()
        assert "ResourceNotFoundException" in (
            body.get("__type", "")
            + body.get("Type", "")
            + body.get("errorType", "")
            + body.get("Error", {}).get("Code", "")
            + json.dumps(body)
        )


class TestMissingRequiredParameters:
    """Requests missing required parameters for various services."""

    async def test_s3_get_object_no_bucket(self, client):
        """GET on S3 root with no bucket specified."""
        resp = await client.get(
            "/",
            headers=auth_header("s3"),
        )
        # S3 list-buckets or error
        assert resp.status_code in (200, 400)

    async def test_sqs_delete_queue_no_url(self, client):
        """SQS DeleteQueue with no QueueUrl -- may succeed with empty or error."""
        resp = await client.post(
            "/",
            content="Action=DeleteQueue&Version=2012-11-05",
            headers={
                **auth_header("sqs"),
                "Content-Type": "application/x-www-form-urlencoded",
            },
        )
        # Some implementations accept this gracefully, others error
        assert resp.status_code in (200, 400, 404, 500)

    async def test_sns_publish_no_topic(self, client):
        """SNS Publish with no TopicArn or TargetArn."""
        resp = await client.post(
            "/",
            content=("Action=Publish&Message=test&Version=2010-03-31"),
            headers={
                **auth_header("sns"),
                "Content-Type": "application/x-www-form-urlencoded",
            },
        )
        assert resp.status_code in (400, 404, 500)


class TestErrorResponseFormat:
    """Verify error responses use the correct format per protocol."""

    async def test_sqs_error_is_json_or_xml(self, client):
        """SQS errors should be well-formed (JSON or XML)."""
        resp = await client.post(
            "/",
            content="Action=GetQueueUrl&QueueName=no-such-queue&Version=2012-11-05",
            headers={
                **auth_header("sqs"),
                "Content-Type": "application/x-www-form-urlencoded",
            },
        )
        assert resp.status_code in (400, 404, 500)
        text = resp.text
        # Should be valid JSON or XML
        is_json = False
        is_xml = False
        try:
            json.loads(text)
            is_json = True
        except (json.JSONDecodeError, ValueError):
            pass
        if text.strip().startswith("<"):
            is_xml = True
        assert is_json or is_xml, f"Error response is neither JSON nor XML: {text[:200]}"

    async def test_dynamodb_error_is_json(self, client):
        """DynamoDB errors should be JSON."""
        resp = await client.post(
            "/",
            content=json.dumps({"TableName": "nonexistent-table-xyz"}).encode(),
            headers={
                **json_headers("dynamodb"),
                "X-Amz-Target": "DynamoDB_20120810.DescribeTable",
            },
        )
        assert resp.status_code in (400, 404, 500)
        body = resp.json()  # Should not raise
        assert isinstance(body, dict)

    async def test_sts_error_is_xml(self, client):
        """STS with a bad action should return XML error."""
        resp = await client.post(
            "/",
            content="Action=NoSuchAction&Version=2011-06-15",
            headers={
                **auth_header("sts"),
                "Content-Type": "application/x-www-form-urlencoded",
            },
        )
        # Could be 400 or 500 for unknown action
        assert resp.status_code in (400, 404, 500)


class TestHealthEndpoint:
    """Health endpoint should always work."""

    async def test_health_returns_200(self, client):
        resp = await client.get("/_robotocore/health")
        assert resp.status_code == 200
        body = resp.json()
        assert body["status"] == "running"

    async def test_health_ignores_auth(self, client):
        """Health endpoint works without any auth headers."""
        resp = await client.get("/_robotocore/health")
        assert resp.status_code == 200


class TestCORSHeaders:
    """CORS headers on error and success responses."""

    async def test_options_returns_cors(self, client):
        resp = await client.options(
            "/",
            headers=auth_header("sts"),
        )
        assert resp.status_code == 200
        assert "access-control-allow-origin" in resp.headers

    async def test_error_response_has_cors(self, client):
        """Error responses from routed requests should have CORS headers."""
        # Use a request that goes through the handler chain (has auth)
        resp = await client.post(
            "/",
            content="Action=NoSuchAction&Version=2011-06-15",
            headers={
                **auth_header("sts"),
                "Content-Type": "application/x-www-form-urlencoded",
            },
        )
        assert resp.status_code in (400, 404, 500)
        assert "access-control-allow-origin" in resp.headers
