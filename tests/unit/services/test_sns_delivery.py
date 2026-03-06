"""Unit tests for SNS cross-service delivery (Lambda, HTTP/HTTPS)."""

import io
import json
import uuid
import zipfile
from http.server import BaseHTTPRequestHandler, HTTPServer
from threading import Thread
from unittest.mock import patch

from robotocore.services.sns.models import SnsSubscription
from robotocore.services.sns.provider import (
    _deliver_to_http,
    _deliver_to_subscriber,
)


def _make_lambda_zip(code: str) -> bytes:
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w") as zf:
        zf.writestr("lambda_function.py", code)
    return buf.getvalue()


def _make_subscription(
    protocol: str, endpoint: str, topic_arn: str = "arn:aws:sns:us-east-1:123456789012:test-topic"
) -> SnsSubscription:
    sub_id = str(uuid.uuid4())
    return SnsSubscription(
        subscription_arn=f"{topic_arn}:{sub_id}",
        topic_arn=topic_arn,
        protocol=protocol,
        endpoint=endpoint,
        owner="123456789012",
    )


class TestDeliverToLambda:
    """Test SNS -> Lambda delivery via invoke_lambda_async."""

    def test_deliver_to_lambda_calls_invoke_async(self):
        """Verify _deliver_to_lambda dispatches via invoke_lambda_async."""
        topic_arn = "arn:aws:sns:us-east-1:123456789012:test-topic"
        func_arn = "arn:aws:lambda:us-east-1:123456789012:function:my-func"
        sub = _make_subscription("lambda", func_arn, topic_arn)

        with patch("robotocore.services.lambda_.invoke.invoke_lambda_async") as mock_invoke:
            from robotocore.services.sns.provider import _deliver_to_lambda

            _deliver_to_lambda(
                sub,
                "hello world",
                "Test Subject",
                {},
                "msg-123",
                topic_arn,
                "us-east-1",
            )

            mock_invoke.assert_called_once()
            call_args = mock_invoke.call_args

            # First positional arg is the function ARN
            assert call_args[0][0] == func_arn

            # Second positional arg is the event payload
            event = call_args[0][1]
            assert "Records" in event
            assert len(event["Records"]) == 1
            record = event["Records"][0]
            assert record["EventSource"] == "aws:sns"
            assert record["EventVersion"] == "1.0"
            assert record["EventSubscriptionArn"] == sub.subscription_arn
            sns_data = record["Sns"]
            assert sns_data["Type"] == "Notification"
            assert sns_data["MessageId"] == "msg-123"
            assert sns_data["TopicArn"] == topic_arn
            assert sns_data["Subject"] == "Test Subject"
            assert sns_data["Message"] == "hello world"
            assert sns_data["SignatureVersion"] == "1"

    def test_deliver_to_lambda_with_message_attributes(self):
        """Verify message attributes are correctly formatted in the Lambda event."""
        topic_arn = "arn:aws:sns:us-east-1:123456789012:test-topic"
        func_arn = "arn:aws:lambda:us-east-1:123456789012:function:my-func"
        sub = _make_subscription("lambda", func_arn, topic_arn)

        msg_attrs = {
            "color": {"DataType": "String", "StringValue": "red"},
        }

        with patch("robotocore.services.lambda_.invoke.invoke_lambda_async") as mock_invoke:
            from robotocore.services.sns.provider import _deliver_to_lambda

            _deliver_to_lambda(
                sub,
                "test msg",
                None,
                msg_attrs,
                "msg-456",
                topic_arn,
                "us-east-1",
            )

            event = mock_invoke.call_args[0][1]
            sns_attrs = event["Records"][0]["Sns"]["MessageAttributes"]
            assert "color" in sns_attrs
            assert sns_attrs["color"]["Type"] == "String"
            assert sns_attrs["color"]["Value"] == "red"

    def test_deliver_to_lambda_handles_bad_arn(self):
        """Verify graceful handling when Lambda ARN is malformed."""
        topic_arn = "arn:aws:sns:us-east-1:123456789012:test-topic"
        sub = _make_subscription("lambda", "bad-arn", topic_arn)

        with patch("robotocore.services.lambda_.invoke.invoke_lambda_async") as mock_invoke:
            from robotocore.services.sns.provider import _deliver_to_lambda

            # Should still dispatch (invoke module handles errors)
            _deliver_to_lambda(
                sub,
                "msg",
                None,
                {},
                "msg-789",
                topic_arn,
                "us-east-1",
            )
            mock_invoke.assert_called_once()


class TestDeliverToHttp:
    """Test SNS -> HTTP/HTTPS delivery."""

    def test_deliver_to_http_posts_to_endpoint(self):
        """Verify that _deliver_to_http sends a POST to the subscription endpoint."""
        received = []

        class Handler(BaseHTTPRequestHandler):
            def do_POST(self):
                length = int(self.headers.get("Content-Length", 0))
                body = self.rfile.read(length)
                received.append(
                    {
                        "body": json.loads(body),
                        "headers": dict(self.headers),
                    }
                )
                self.send_response(200)
                self.end_headers()

            def log_message(self, *args):
                pass  # Suppress server logging in tests

        server = HTTPServer(("127.0.0.1", 0), Handler)
        port = server.server_address[1]
        thread = Thread(target=server.handle_request, daemon=True)
        thread.start()

        try:
            topic_arn = "arn:aws:sns:us-east-1:123456789012:test-topic"
            endpoint = f"http://127.0.0.1:{port}/notify"
            sub = _make_subscription("http", endpoint, topic_arn)

            _deliver_to_http(
                sub,
                "hello http",
                "HTTP Subject",
                {"key1": {"DataType": "String", "StringValue": "val1"}},
                "msg-http-1",
                topic_arn,
                "us-east-1",
            )

            thread.join(timeout=5)
            assert len(received) == 1
            payload = received[0]["body"]
            assert payload["Type"] == "Notification"
            assert payload["MessageId"] == "msg-http-1"
            assert payload["TopicArn"] == topic_arn
            assert payload["Subject"] == "HTTP Subject"
            assert payload["Message"] == "hello http"
            assert payload["SignatureVersion"] == "1"
            assert "MessageAttributes" in payload
            assert payload["MessageAttributes"]["key1"]["Type"] == "String"
            assert payload["MessageAttributes"]["key1"]["Value"] == "val1"

            # Verify SNS-specific headers (case-insensitive lookup)
            headers_lower = {k.lower(): v for k, v in received[0]["headers"].items()}
            assert headers_lower.get("x-amz-sns-message-type") == "Notification"
            assert headers_lower.get("x-amz-sns-message-id") == "msg-http-1"
            assert headers_lower.get("x-amz-sns-topic-arn") == topic_arn
        finally:
            server.server_close()

    def test_deliver_to_http_handles_connection_error(self):
        """Verify graceful handling when HTTP endpoint is unreachable."""
        topic_arn = "arn:aws:sns:us-east-1:123456789012:test-topic"
        endpoint = "http://127.0.0.1:19999/not-listening"
        sub = _make_subscription("http", endpoint, topic_arn)

        # Should not raise
        _deliver_to_http(
            sub,
            "test",
            None,
            {},
            "msg-err",
            topic_arn,
            "us-east-1",
        )


class TestDeliverToSubscriberRouting:
    """Test that _deliver_to_subscriber routes to the correct delivery method."""

    def test_routes_lambda_protocol(self):
        sub = _make_subscription("lambda", "arn:aws:lambda:us-east-1:123456789012:function:test")
        with patch("robotocore.services.sns.provider._deliver_to_lambda") as mock:
            _deliver_to_subscriber(sub, "msg", None, {}, "id", "topic-arn", "us-east-1")
            mock.assert_called_once()

    def test_routes_http_protocol(self):
        sub = _make_subscription("http", "http://example.com/endpoint")
        with patch("robotocore.services.sns.provider._deliver_to_http") as mock:
            _deliver_to_subscriber(sub, "msg", None, {}, "id", "topic-arn", "us-east-1")
            mock.assert_called_once()

    def test_routes_https_protocol(self):
        sub = _make_subscription("https", "https://example.com/endpoint")
        with patch("robotocore.services.sns.provider._deliver_to_http") as mock:
            _deliver_to_subscriber(sub, "msg", None, {}, "id", "topic-arn", "us-east-1")
            mock.assert_called_once()

    def test_routes_sqs_protocol(self):
        sub = _make_subscription("sqs", "arn:aws:sqs:us-east-1:123456789012:my-queue")
        with patch("robotocore.services.sns.provider._deliver_to_sqs") as mock:
            _deliver_to_subscriber(sub, "msg", None, {}, "id", "topic-arn", "us-east-1")
            mock.assert_called_once()
