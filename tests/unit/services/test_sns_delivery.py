"""Unit tests for SNS cross-service delivery (Lambda, HTTP/HTTPS)."""

import io
import json
import uuid
import zipfile
from http.server import BaseHTTPRequestHandler, HTTPServer
from threading import Thread
from unittest.mock import MagicMock, patch

import pytest

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


def _make_subscription(protocol: str, endpoint: str, topic_arn: str = "arn:aws:sns:us-east-1:123456789012:test-topic") -> SnsSubscription:
    sub_id = str(uuid.uuid4())
    return SnsSubscription(
        subscription_arn=f"{topic_arn}:{sub_id}",
        topic_arn=topic_arn,
        protocol=protocol,
        endpoint=endpoint,
        owner="123456789012",
    )


class TestDeliverToLambda:
    """Test SNS -> Lambda delivery."""

    def test_deliver_to_lambda_calls_executor(self):
        """Verify that _deliver_to_lambda invokes execute_python_handler with correct SNS event."""
        topic_arn = "arn:aws:sns:us-east-1:123456789012:test-topic"
        func_arn = "arn:aws:lambda:us-east-1:123456789012:function:my-func"
        sub = _make_subscription("lambda", func_arn, topic_arn)
        code_zip = _make_lambda_zip('def handler(event, ctx): return event')

        # Mock the Moto backend
        mock_fn = MagicMock()
        mock_fn.run_time = "python3.12"
        mock_fn.code = {"ZipFile": code_zip}
        mock_fn.handler = "lambda_function.handler"
        mock_fn.timeout = 3
        mock_fn.memory_size = 128
        mock_fn.environment_vars = {}
        mock_fn.layers = []

        mock_backend = MagicMock()
        mock_backend.get_function.return_value = mock_fn

        with patch("moto.backends.get_backend") as mock_get_backend, \
             patch("robotocore.services.lambda_.executor.execute_python_handler") as mock_exec:
            mock_get_backend.return_value = {"123456789012": {"us-east-1": mock_backend}}
            mock_exec.return_value = ({"ok": True}, None, "")

            # Import after patching to ensure the local import picks up the mock
            from robotocore.services.sns.provider import _deliver_to_lambda
            _deliver_to_lambda(
                sub, "hello world", "Test Subject",
                {}, "msg-123", topic_arn, "us-east-1",
            )

            mock_exec.assert_called_once()
            call_args = mock_exec.call_args
            # execute_python_handler uses positional args
            event = call_args.kwargs.get("event", call_args[1].get("event") if len(call_args) > 1 and isinstance(call_args[1], dict) else None)
            if event is None:
                # Positional: code_zip, handler, event, ...
                event = call_args[0][2] if len(call_args[0]) > 2 else call_args.kwargs["event"]

            # Verify event structure matches AWS SNS->Lambda format
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
        code_zip = _make_lambda_zip('def handler(event, ctx): return event')

        mock_fn = MagicMock()
        mock_fn.run_time = "python3.12"
        mock_fn.code = {"ZipFile": code_zip}
        mock_fn.handler = "lambda_function.handler"
        mock_fn.timeout = 3
        mock_fn.memory_size = 128
        mock_fn.environment_vars = {}
        mock_fn.layers = []

        mock_backend = MagicMock()
        mock_backend.get_function.return_value = mock_fn

        msg_attrs = {
            "color": {"DataType": "String", "StringValue": "red"},
        }

        with patch("moto.backends.get_backend") as mock_get_backend, \
             patch("robotocore.services.lambda_.executor.execute_python_handler") as mock_exec:
            mock_get_backend.return_value = {"123456789012": {"us-east-1": mock_backend}}
            mock_exec.return_value = ({"ok": True}, None, "")

            from robotocore.services.sns.provider import _deliver_to_lambda
            _deliver_to_lambda(
                sub, "test msg", None,
                msg_attrs, "msg-456", topic_arn, "us-east-1",
            )

            call_args = mock_exec.call_args
            event = call_args.kwargs.get("event", call_args[1].get("event") if len(call_args) > 1 and isinstance(call_args[1], dict) else None)
            if event is None:
                event = call_args[0][2] if len(call_args[0]) > 2 else call_args.kwargs["event"]

            sns_attrs = event["Records"][0]["Sns"]["MessageAttributes"]
            assert "color" in sns_attrs
            assert sns_attrs["color"]["Type"] == "String"
            assert sns_attrs["color"]["Value"] == "red"

    def test_deliver_to_lambda_handles_missing_function(self):
        """Verify graceful handling when Lambda function doesn't exist."""
        topic_arn = "arn:aws:sns:us-east-1:123456789012:test-topic"
        func_arn = "arn:aws:lambda:us-east-1:123456789012:function:nonexistent"
        sub = _make_subscription("lambda", func_arn, topic_arn)

        with patch("moto.backends.get_backend") as mock_get_backend:
            mock_backend = MagicMock()
            mock_backend.get_function.side_effect = Exception("Function not found")
            mock_get_backend.return_value = {"123456789012": {"us-east-1": mock_backend}}

            from robotocore.services.sns.provider import _deliver_to_lambda
            # Should not raise
            _deliver_to_lambda(
                sub, "msg", None, {}, "msg-789", topic_arn, "us-east-1",
            )

    def test_deliver_to_lambda_skips_non_python_runtime(self):
        """Verify non-Python runtimes are skipped gracefully."""
        topic_arn = "arn:aws:sns:us-east-1:123456789012:test-topic"
        func_arn = "arn:aws:lambda:us-east-1:123456789012:function:node-func"
        sub = _make_subscription("lambda", func_arn, topic_arn)

        mock_fn = MagicMock()
        mock_fn.run_time = "nodejs18.x"
        mock_fn.code = {"ZipFile": b"fake"}

        mock_backend = MagicMock()
        mock_backend.get_function.return_value = mock_fn

        with patch("moto.backends.get_backend") as mock_get_backend, \
             patch("robotocore.services.lambda_.executor.execute_python_handler") as mock_exec:
            mock_get_backend.return_value = {"123456789012": {"us-east-1": mock_backend}}

            from robotocore.services.sns.provider import _deliver_to_lambda
            _deliver_to_lambda(
                sub, "msg", None, {}, "msg-skip", topic_arn, "us-east-1",
            )

            # Should NOT have called the executor
            mock_exec.assert_not_called()


class TestDeliverToHttp:
    """Test SNS -> HTTP/HTTPS delivery."""

    def test_deliver_to_http_posts_to_endpoint(self):
        """Verify that _deliver_to_http sends a POST to the subscription endpoint."""
        received = []

        class Handler(BaseHTTPRequestHandler):
            def do_POST(self):
                length = int(self.headers.get("Content-Length", 0))
                body = self.rfile.read(length)
                received.append({
                    "body": json.loads(body),
                    "headers": dict(self.headers),
                })
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
                sub, "hello http", "HTTP Subject",
                {"key1": {"DataType": "String", "StringValue": "val1"}},
                "msg-http-1", topic_arn, "us-east-1",
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
            sub, "test", None, {}, "msg-err", topic_arn, "us-east-1",
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
