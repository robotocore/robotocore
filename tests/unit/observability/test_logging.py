"""Tests for structured JSON logging."""

import json
import logging
import os
from unittest.mock import patch

from robotocore.observability.logging import (
    JsonFormatter,
    log_request,
    log_response,
    setup_logging,
)


class TestJsonFormatter:
    def test_format_basic_record(self):
        formatter = JsonFormatter()
        record = logging.LogRecord(
            name="test",
            level=logging.INFO,
            pathname="test.py",
            lineno=1,
            msg="hello %s",
            args=("world",),
            exc_info=None,
        )
        output = formatter.format(record)
        data = json.loads(output)
        assert data["level"] == "INFO"
        assert data["message"] == "hello world"
        assert data["logger"] == "test"
        assert "timestamp" in data

    def test_format_includes_extra_fields(self):
        formatter = JsonFormatter()
        record = logging.LogRecord(
            name="test",
            level=logging.INFO,
            pathname="test.py",
            lineno=1,
            msg="test",
            args=(),
            exc_info=None,
        )
        record.service = "sqs"
        record.operation = "SendMessage"
        record.request_id = "abc-123"
        record.duration_ms = 42.5
        output = formatter.format(record)
        data = json.loads(output)
        assert data["service"] == "sqs"
        assert data["operation"] == "SendMessage"
        assert data["request_id"] == "abc-123"
        assert data["duration_ms"] == 42.5

    def test_format_excludes_missing_extras(self):
        formatter = JsonFormatter()
        record = logging.LogRecord(
            name="test",
            level=logging.WARNING,
            pathname="test.py",
            lineno=1,
            msg="warn",
            args=(),
            exc_info=None,
        )
        output = formatter.format(record)
        data = json.loads(output)
        assert "service" not in data
        assert "operation" not in data
        assert "request_id" not in data

    def test_format_includes_exception(self):
        formatter = JsonFormatter()
        try:
            raise ValueError("boom")
        except ValueError:
            import sys

            exc_info = sys.exc_info()
        record = logging.LogRecord(
            name="test",
            level=logging.ERROR,
            pathname="test.py",
            lineno=1,
            msg="error",
            args=(),
            exc_info=exc_info,
        )
        output = formatter.format(record)
        data = json.loads(output)
        assert data["exception"] == "boom"

    def test_format_is_valid_json(self):
        formatter = JsonFormatter()
        record = logging.LogRecord(
            name="test.module",
            level=logging.DEBUG,
            pathname="test.py",
            lineno=42,
            msg='a message with special chars: {}<>&"',
            args=(),
            exc_info=None,
        )
        output = formatter.format(record)
        # Should be valid JSON
        parsed = json.loads(output)
        assert isinstance(parsed, dict)

    def test_format_timestamp_is_iso(self):
        formatter = JsonFormatter()
        record = logging.LogRecord(
            name="test",
            level=logging.INFO,
            pathname="test.py",
            lineno=1,
            msg="test",
            args=(),
            exc_info=None,
        )
        output = formatter.format(record)
        data = json.loads(output)
        # ISO format contains T
        assert "T" in data["timestamp"]


class TestSetupLogging:
    def teardown_method(self):
        """Reset root logger after each test."""
        root = logging.getLogger()
        root.handlers.clear()
        root.setLevel(logging.WARNING)

    def test_default_level_is_info(self):
        with patch.dict(os.environ, {}, clear=True):
            os.environ.pop("LOG_LEVEL", None)
            os.environ.pop("LOG_FORMAT", None)
            setup_logging()
        assert logging.getLogger().level == logging.INFO

    def test_custom_level_debug(self):
        with patch.dict(os.environ, {"LOG_LEVEL": "DEBUG"}):
            setup_logging()
        assert logging.getLogger().level == logging.DEBUG

    def test_custom_level_error(self):
        with patch.dict(os.environ, {"LOG_LEVEL": "ERROR"}):
            setup_logging()
        assert logging.getLogger().level == logging.ERROR

    def test_json_format_uses_json_formatter(self):
        with patch.dict(os.environ, {"LOG_FORMAT": "json"}):
            setup_logging()
        root = logging.getLogger()
        assert len(root.handlers) == 1
        assert isinstance(root.handlers[0].formatter, JsonFormatter)

    def test_text_format_is_default(self):
        with patch.dict(os.environ, {}, clear=True):
            os.environ.pop("LOG_FORMAT", None)
            setup_logging()
        root = logging.getLogger()
        assert len(root.handlers) == 1
        assert not isinstance(root.handlers[0].formatter, JsonFormatter)

    def test_invalid_level_defaults_to_info(self):
        with patch.dict(os.environ, {"LOG_LEVEL": "NOTAVALIDLEVEL"}):
            setup_logging()
        assert logging.getLogger().level == logging.INFO


class TestLogRequest:
    def test_logs_when_debug_enabled(self, caplog):
        logger = logging.getLogger("test_request")
        with patch.dict(os.environ, {"DEBUG": "1"}):
            with caplog.at_level(logging.DEBUG, logger="test_request"):
                log_request(
                    logger,
                    method="POST",
                    path="/",
                    headers={"content-type": "application/json"},
                    body_size=42,
                    request_id="req-1",
                )
        assert "POST" in caplog.text
        assert "req-1" in caplog.text

    def test_no_log_when_debug_disabled(self, caplog):
        logger = logging.getLogger("test_request")
        with patch.dict(os.environ, {}, clear=True):
            os.environ.pop("DEBUG", None)
            with caplog.at_level(logging.DEBUG, logger="test_request"):
                log_request(
                    logger,
                    method="POST",
                    path="/",
                    headers={},
                    body_size=0,
                    request_id="req-2",
                )
        assert "req-2" not in caplog.text

    def test_sanitizes_authorization_header(self, caplog):
        logger = logging.getLogger("test_request")
        with patch.dict(os.environ, {"DEBUG": "1"}):
            with caplog.at_level(logging.DEBUG, logger="test_request"):
                log_request(
                    logger,
                    method="GET",
                    path="/",
                    headers={
                        "authorization": "AWS4-HMAC-SHA256 secret-stuff",
                        "content-type": "text/xml",
                    },
                    body_size=0,
                    request_id="req-3",
                )
        assert "secret-stuff" not in caplog.text
        assert "***" in caplog.text

    def test_sanitizes_security_token(self, caplog):
        logger = logging.getLogger("test_request")
        with patch.dict(os.environ, {"DEBUG": "1"}):
            with caplog.at_level(logging.DEBUG, logger="test_request"):
                log_request(
                    logger,
                    method="GET",
                    path="/",
                    headers={
                        "x-amz-security-token": "super-secret-token",
                    },
                    body_size=0,
                    request_id="req-4",
                )
        assert "super-secret-token" not in caplog.text


class TestLogResponse:
    def test_logs_when_debug_enabled(self, caplog):
        logger = logging.getLogger("test_response")
        with patch.dict(os.environ, {"DEBUG": "1"}):
            with caplog.at_level(logging.DEBUG, logger="test_response"):
                log_response(
                    logger,
                    status_code=200,
                    body_size=100,
                    duration_ms=5.3,
                    request_id="resp-1",
                )
        assert "200" in caplog.text
        assert "resp-1" in caplog.text

    def test_no_log_when_debug_disabled(self, caplog):
        logger = logging.getLogger("test_response")
        with patch.dict(os.environ, {}, clear=True):
            os.environ.pop("DEBUG", None)
            with caplog.at_level(logging.DEBUG, logger="test_response"):
                log_response(
                    logger,
                    status_code=200,
                    body_size=100,
                    duration_ms=5.3,
                    request_id="resp-2",
                )
        assert "resp-2" not in caplog.text
