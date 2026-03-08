"""Unit tests for the diagnostic logging module."""

import json
import os
import tempfile

from robotocore.diagnostics import header_value, record, reset


class TestHeaderValue:
    def test_basic_exception(self):
        e = ValueError("bad input")
        val = header_value(e)
        assert val == "ValueError: bad input"

    def test_not_implemented(self):
        e = NotImplementedError("GetWidget not yet implemented")
        val = header_value(e)
        assert val == "NotImplementedError: GetWidget not yet implemented"

    def test_truncates_long_messages(self):
        e = RuntimeError("x" * 1000)
        val = header_value(e)
        assert len(val) <= 512
        assert val.endswith("...")

    def test_strips_newlines(self):
        e = ValueError("line1\nline2\rline3")
        val = header_value(e)
        assert "\n" not in val
        assert "\r" not in val

    def test_empty_message(self):
        e = RuntimeError()
        val = header_value(e)
        assert val == "RuntimeError: "

    def test_key_error(self):
        e = KeyError("missing_field")
        val = header_value(e)
        assert "KeyError" in val
        assert "missing_field" in val

    def test_attribute_error(self):
        e = AttributeError("'NoneType' object has no attribute 'arn'")
        val = header_value(e)
        assert "AttributeError" in val
        assert "arn" in val


class TestRecord:
    def setup_method(self):
        reset()

    def teardown_method(self):
        reset()
        os.environ.pop("ROBOTOCORE_DIAG", None)

    def test_no_env_var_is_silent(self):
        """Without ROBOTOCORE_DIAG, record() is a no-op."""
        os.environ.pop("ROBOTOCORE_DIAG", None)
        # Should not raise
        record(exc=ValueError("test"), service="s3", status=500)

    def test_writes_to_file_when_enabled(self):
        with tempfile.NamedTemporaryFile(mode="w", suffix=".log", delete=False) as f:
            log_path = f.name

        try:
            os.environ["ROBOTOCORE_DIAG"] = log_path
            record(
                exc=ValueError("bad"),
                service="sqs",
                operation="SendMessage",
                method="POST",
                path="/",
                status=500,
            )

            with open(log_path) as f:
                lines = f.readlines()

            # First line is startup marker, second is our record
            assert len(lines) >= 2
            entry = json.loads(lines[-1])
            assert entry["service"] == "sqs"
            assert entry["operation"] == "SendMessage"
            assert entry["status"] == 500
            assert entry["exc_type"] == "ValueError"
            assert entry["exc_msg"] == "bad"
            assert "ValueError" in entry["traceback"]
            assert entry["level"] == "ERROR"
        finally:
            os.unlink(log_path)

    def test_startup_marker(self):
        with tempfile.NamedTemporaryFile(mode="w", suffix=".log", delete=False) as f:
            log_path = f.name

        try:
            os.environ["ROBOTOCORE_DIAG"] = log_path
            record(exc=ValueError("trigger"), service="s3", status=500)

            with open(log_path) as f:
                first_line = f.readline()

            entry = json.loads(first_line)
            assert entry["event"] == "diag_start"
        finally:
            os.unlink(log_path)

    def test_501_logged_as_warn(self):
        with tempfile.NamedTemporaryFile(mode="w", suffix=".log", delete=False) as f:
            log_path = f.name

        try:
            os.environ["ROBOTOCORE_DIAG"] = log_path
            record(
                exc=NotImplementedError("not built"),
                service="ec2",
                operation="DescribeWidget",
                status=501,
            )

            with open(log_path) as f:
                lines = f.readlines()

            entry = json.loads(lines[-1])
            assert entry["level"] == "WARN"
            assert entry["exc_type"] == "NotImplementedError"
        finally:
            os.unlink(log_path)

    def test_multiple_records_append(self):
        with tempfile.NamedTemporaryFile(mode="w", suffix=".log", delete=False) as f:
            log_path = f.name

        try:
            os.environ["ROBOTOCORE_DIAG"] = log_path
            record(exc=ValueError("first"), service="s3", status=500)
            record(exc=KeyError("second"), service="sqs", status=500)

            with open(log_path) as f:
                lines = f.readlines()

            # Startup marker + 2 records
            assert len(lines) == 3
            assert json.loads(lines[1])["exc_msg"] == "first"
            assert json.loads(lines[2])["exc_msg"] == "'second'"
        finally:
            os.unlink(log_path)

    def test_traceback_included(self):
        with tempfile.NamedTemporaryFile(mode="w", suffix=".log", delete=False) as f:
            log_path = f.name

        try:
            os.environ["ROBOTOCORE_DIAG"] = log_path
            try:
                raise RuntimeError("boom")
            except RuntimeError as e:
                record(exc=e, service="lambda", status=500)

            with open(log_path) as f:
                lines = f.readlines()

            entry = json.loads(lines[-1])
            assert "RuntimeError: boom" in entry["traceback"]
            assert "test_traceback_included" in entry["traceback"]
        finally:
            os.unlink(log_path)


class TestDiagHeaderInResponses:
    """Verify the x-robotocore-diag header appears on error responses."""

    def test_error_normalizer_includes_diag_header(self):
        from unittest.mock import MagicMock

        from robotocore.gateway.handler_chain import RequestContext
        from robotocore.gateway.handlers import error_normalizer

        request = MagicMock()
        request.method = "POST"
        request.url.path = "/"
        request.headers = {}
        request.query_params = {}
        ctx = RequestContext(request=request, service_name="dynamodb")
        ctx.protocol = "json"

        error_normalizer(ctx, ValueError("bad table"))
        assert ctx.response is not None
        diag = ctx.response.headers.get("x-robotocore-diag")
        assert diag is not None
        assert "ValueError" in diag
        assert "bad table" in diag

    def test_error_normalizer_xml_includes_diag_header(self):
        from unittest.mock import MagicMock

        from robotocore.gateway.handler_chain import RequestContext
        from robotocore.gateway.handlers import error_normalizer

        request = MagicMock()
        request.method = "POST"
        request.url.path = "/"
        request.headers = {}
        request.query_params = {}
        ctx = RequestContext(request=request, service_name="s3")
        ctx.protocol = "rest-xml"

        error_normalizer(ctx, KeyError("missing_bucket"))
        diag = ctx.response.headers.get("x-robotocore-diag")
        assert "KeyError" in diag

    def test_not_implemented_includes_diag_header(self):
        from unittest.mock import MagicMock

        from robotocore.gateway.handler_chain import RequestContext
        from robotocore.gateway.handlers import error_normalizer

        request = MagicMock()
        request.method = "POST"
        request.url.path = "/"
        request.headers = {}
        request.query_params = {}
        ctx = RequestContext(request=request, service_name="sqs")
        ctx.protocol = "query"

        error_normalizer(ctx, NotImplementedError("PurgeQueue not implemented"))
        assert ctx.response.status_code == 501
        diag = ctx.response.headers.get("x-robotocore-diag")
        assert "NotImplementedError" in diag
        assert "PurgeQueue" in diag


class TestDiagWithProbeScripts:
    """Verify probe scripts can use the diag header for better classification."""

    def test_header_value_usable_for_classification(self):
        """The header value contains enough info to classify errors."""
        # NotImplementedError → "not implemented"
        val = header_value(NotImplementedError("GetWidget"))
        assert "NotImplementedError" in val

        # KeyError → internal bug
        val = header_value(KeyError("missing_field"))
        assert "KeyError" in val

        # AttributeError → internal bug
        val = header_value(AttributeError("'NoneType' has no 'arn'"))
        assert "AttributeError" in val

    def test_header_survives_boto3_error_response(self):
        """Simulate what boto3 sees in e.response["ResponseMetadata"]["HTTPHeaders"]."""
        # boto3 lowercases all header names
        header_name = "x-robotocore-diag".lower()
        assert header_name == "x-robotocore-diag"

        val = header_value(RuntimeError("Moto blew up"))
        # This is what would appear in HTTPHeaders
        headers = {header_name: val}
        assert "RuntimeError: Moto blew up" in headers["x-robotocore-diag"]
