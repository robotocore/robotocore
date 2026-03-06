"""Unit tests for Lambda event source mapping engine."""

from unittest.mock import MagicMock, patch

from robotocore.services.lambda_.event_source import (
    EventSourceEngine,
    _convert_message_attributes,
    _extract_function_name,
    get_engine,
)

# ---------------------------------------------------------------------------
# _extract_function_name
# ---------------------------------------------------------------------------


class TestExtractFunctionName:
    def test_full_arn(self):
        arn = "arn:aws:lambda:us-east-1:123456789012:function:my-fn"
        assert _extract_function_name(arn) == "my-fn"

    def test_short_arn_returns_as_is(self):
        assert _extract_function_name("arn:aws:lambda:us-east-1") == "arn:aws:lambda:us-east-1"

    def test_plain_name(self):
        assert _extract_function_name("my-function") == "my-function"

    def test_arn_with_qualifier(self):
        arn = "arn:aws:lambda:us-east-1:123456789012:function:my-fn:prod"
        assert _extract_function_name(arn) == "my-fn"


# ---------------------------------------------------------------------------
# _convert_message_attributes
# ---------------------------------------------------------------------------


class TestConvertMessageAttributes:
    def test_empty(self):
        assert _convert_message_attributes({}) == {}

    def test_dict_value_passthrough(self):
        attrs = {"key": {"stringValue": "val", "dataType": "String"}}
        assert _convert_message_attributes(attrs) == attrs

    def test_non_dict_value_wrapped(self):
        result = _convert_message_attributes({"key": "value"})
        assert result == {"key": {"stringValue": "value", "dataType": "String"}}

    def test_numeric_value(self):
        result = _convert_message_attributes({"num": 42})
        assert result == {"num": {"stringValue": "42", "dataType": "String"}}


# ---------------------------------------------------------------------------
# get_engine (singleton)
# ---------------------------------------------------------------------------


class TestGetEngine:
    def test_returns_engine(self):
        import robotocore.services.lambda_.event_source as mod

        old = mod._engine
        mod._engine = None
        try:
            engine = get_engine()
            assert isinstance(engine, EventSourceEngine)
        finally:
            mod._engine = old

    def test_returns_same_instance(self):
        import robotocore.services.lambda_.event_source as mod

        old = mod._engine
        mod._engine = None
        try:
            e1 = get_engine()
            e2 = get_engine()
            assert e1 is e2
        finally:
            mod._engine = old


# ---------------------------------------------------------------------------
# EventSourceEngine -- start / stop lifecycle
# ---------------------------------------------------------------------------


class TestEventSourceEngineLifecycle:
    def test_start_creates_thread(self):
        engine = EventSourceEngine()
        with patch.object(engine, "_poll_loop"):
            engine.start()
            assert engine._running is True
            assert engine._thread is not None
            engine.stop()

    def test_start_idempotent(self):
        engine = EventSourceEngine()
        with patch.object(engine, "_poll_loop"):
            engine.start()
            thread1 = engine._thread
            engine.start()
            assert engine._thread is thread1
            engine.stop()

    def test_stop_sets_flag(self):
        engine = EventSourceEngine()
        engine._running = True
        engine.stop()
        assert engine._running is False

    def test_stop_when_not_started(self):
        engine = EventSourceEngine()
        engine.stop()
        assert engine._running is False


# ---------------------------------------------------------------------------
# EventSourceEngine._poll_all_mappings
# ---------------------------------------------------------------------------

_SQS_MAPPING = {
    "State": "Enabled",
    "EventSourceArn": "arn:aws:sqs:us-east-1:123456789012:my-queue",
    "FunctionArn": "arn:aws:lambda:us-east-1:123456789012:function:fn",
    "BatchSize": 5,
    "_region": "us-east-1",
    "_account_id": "123456789012",
}


class TestPollAllMappings:
    @patch("robotocore.services.lambda_.event_source.EventSourceEngine._poll_sqs")
    def test_dispatches_sqs(self, mock_poll_sqs):
        engine = EventSourceEngine()
        with patch(
            "robotocore.services.lambda_.provider.get_event_source_mappings",
            return_value=[_SQS_MAPPING],
        ):
            engine._poll_all_mappings()
        mock_poll_sqs.assert_called_once_with(
            "arn:aws:sqs:us-east-1:123456789012:my-queue",
            "arn:aws:lambda:us-east-1:123456789012:function:fn",
            5,
            "123456789012",
            "us-east-1",
        )

    @patch("robotocore.services.lambda_.event_source.EventSourceEngine._poll_kinesis")
    def test_dispatches_kinesis(self, mock_poll_kinesis):
        engine = EventSourceEngine()
        mapping = {
            "State": "Enabled",
            "EventSourceArn": "arn:aws:kinesis:us-east-1:123456789012:stream/s",
            "FunctionArn": "arn:aws:lambda:us-east-1:123456789012:function:fn",
            "BatchSize": 100,
            "_region": "us-east-1",
            "_account_id": "123456789012",
        }
        with patch(
            "robotocore.services.lambda_.provider.get_event_source_mappings",
            return_value=[mapping],
        ):
            engine._poll_all_mappings()
        mock_poll_kinesis.assert_called_once()

    @patch("robotocore.services.lambda_.event_source.EventSourceEngine._poll_dynamodb_stream")
    def test_dispatches_dynamodb_stream(self, mock_poll_ddb):
        engine = EventSourceEngine()
        mapping = {
            "State": "Enabled",
            "EventSourceArn": "arn:aws:dynamodb:us-east-1:123456789012:table/T/stream/2024-01-01",
            "FunctionArn": "arn:aws:lambda:us-east-1:123456789012:function:fn",
            "BatchSize": 10,
            "_region": "us-east-1",
            "_account_id": "123456789012",
        }
        with patch(
            "robotocore.services.lambda_.provider.get_event_source_mappings",
            return_value=[mapping],
        ):
            engine._poll_all_mappings()
        mock_poll_ddb.assert_called_once()

    @patch("robotocore.services.lambda_.event_source.EventSourceEngine._poll_sqs")
    def test_disabled_mapping_skipped(self, mock_poll_sqs):
        engine = EventSourceEngine()
        mapping = dict(_SQS_MAPPING, State="Disabled")
        with patch(
            "robotocore.services.lambda_.provider.get_event_source_mappings",
            return_value=[mapping],
        ):
            engine._poll_all_mappings()
        mock_poll_sqs.assert_not_called()

    @patch("robotocore.services.lambda_.event_source.EventSourceEngine._poll_sqs")
    def test_exception_in_mapping_does_not_crash(self, mock_poll_sqs):
        mock_poll_sqs.side_effect = RuntimeError("boom")
        engine = EventSourceEngine()
        with patch(
            "robotocore.services.lambda_.provider.get_event_source_mappings",
            return_value=[_SQS_MAPPING],
        ):
            engine._poll_all_mappings()  # Should not raise


# ---------------------------------------------------------------------------
# EventSourceEngine._invoke_lambda
# ---------------------------------------------------------------------------


class TestInvokeLambda:
    def test_function_not_found_returns_false(self):
        mock_get_backend = MagicMock()
        backend = mock_get_backend.return_value.__getitem__.return_value.__getitem__.return_value
        backend.get_function.side_effect = Exception("nope")
        engine = EventSourceEngine()
        with patch("moto.backends.get_backend", mock_get_backend):
            result = engine._invoke_lambda("my-fn", {"Records": []}, "123456789012", "us-east-1")
        assert result is False

    def test_python_success_returns_true(self):
        fn = MagicMock()
        fn.run_time = "python3.12"
        fn.code_bytes = b"fake"
        fn.handler = "h.h"
        fn.timeout = 3
        fn.memory_size = 128
        fn.environment_vars = {}

        mock_get_backend = MagicMock()
        backend = mock_get_backend.return_value.__getitem__.return_value.__getitem__.return_value
        backend.get_function.return_value = fn

        mock_exec = MagicMock(return_value=({"ok": True}, None, ""))
        engine = EventSourceEngine()
        with (
            patch("moto.backends.get_backend", mock_get_backend),
            patch(
                "robotocore.services.lambda_.event_source.execute_python_handler",
                mock_exec,
            ),
        ):
            result = engine._invoke_lambda("my-fn", {"Records": []}, "123456789012", "us-east-1")
        assert result is True

    def test_python_error_returns_false(self):
        fn = MagicMock()
        fn.run_time = "python3.12"
        fn.code_bytes = b"fake"
        fn.handler = "h.h"
        fn.timeout = 3
        fn.memory_size = 128
        fn.environment_vars = {}

        mock_get_backend = MagicMock()
        backend = mock_get_backend.return_value.__getitem__.return_value.__getitem__.return_value
        backend.get_function.return_value = fn

        mock_exec = MagicMock(return_value=(None, "Handled", "error logs"))
        engine = EventSourceEngine()
        with (
            patch("moto.backends.get_backend", mock_get_backend),
            patch(
                "robotocore.services.lambda_.event_source.execute_python_handler",
                mock_exec,
            ),
        ):
            result = engine._invoke_lambda("my-fn", {"Records": []}, "123456789012", "us-east-1")
        assert result is False

    def test_non_python_runtime_returns_true(self):
        fn = MagicMock()
        fn.run_time = "nodejs18.x"
        fn.code_bytes = None
        fn.code = {}
        fn.layers = []

        mock_get_backend = MagicMock()
        backend = mock_get_backend.return_value.__getitem__.return_value.__getitem__.return_value
        backend.get_function.return_value = fn

        engine = EventSourceEngine()
        with patch("moto.backends.get_backend", mock_get_backend):
            result = engine._invoke_lambda("my-fn", {"Records": []}, "123456789012", "us-east-1")
        assert result is True
