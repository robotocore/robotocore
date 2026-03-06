"""Unit tests for Lambda invocation utilities (invoke.py)."""

import io
import zipfile
from unittest.mock import MagicMock, patch

from robotocore.services.lambda_.invoke import (
    _invoke_lambda_sync,
    invoke_lambda_async,
    invoke_lambda_sync,
)

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_zip(files: dict[str, str]) -> bytes:
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w") as zf:
        for name, content in files.items():
            zf.writestr(name, content)
    return buf.getvalue()


def _echo_handler_zip() -> bytes:
    return _make_zip({"lambda_function.py": "def handler(event, context):\n    return event\n"})


def _make_mock_fn(
    runtime="python3.12",
    handler="lambda_function.handler",
    code_bytes=None,
    code=None,
    timeout=3,
    memory_size=128,
    env_vars=None,
    layers=None,
):
    fn = MagicMock()
    fn.run_time = runtime
    fn.handler = handler
    fn.code_bytes = code_bytes
    fn.code = code or {}
    fn.timeout = timeout
    fn.memory_size = memory_size
    fn.environment_vars = env_vars or {}
    fn.layers = layers or []
    return fn


def _patch_backend(fn_or_exc):
    """Create patches for moto.backends.get_backend.

    Returns (p_backend, p_layers, mock_backend_obj).
    """
    mock_backend_obj = MagicMock()
    if isinstance(fn_or_exc, Exception):
        mock_backend_obj.get_function.side_effect = fn_or_exc
    else:
        mock_backend_obj.get_function.return_value = fn_or_exc

    mock_get_backend = MagicMock()
    mock_get_backend.return_value.__getitem__.return_value.__getitem__.return_value = (
        mock_backend_obj
    )

    p_backend = patch("moto.backends.get_backend", mock_get_backend)
    p_layers = patch("robotocore.services.lambda_.executor.get_layer_zips", return_value=[])
    return p_backend, p_layers, mock_backend_obj


# ---------------------------------------------------------------------------
# _invoke_lambda_sync (internal)
# ---------------------------------------------------------------------------


class TestInvokeLambdaSyncInternal:
    def test_successful_invocation(self):
        fn = _make_mock_fn(code_bytes=_echo_handler_zip())
        p1, p2, _ = _patch_backend(fn)
        with p1, p2:
            result, error_type, logs = _invoke_lambda_sync(
                "arn:aws:lambda:us-east-1:123456789012:function:echo",
                {"hello": "world"},
                "us-east-1",
                "123456789012",
                None,
            )
        assert result == {"hello": "world"}
        assert error_type is None

    def test_function_not_found(self):
        p1, p2, _ = _patch_backend(Exception("not found"))
        with p1, p2:
            result, error_type, _ = _invoke_lambda_sync(
                "arn:aws:lambda:us-east-1:123456789012:function:missing",
                {},
                "us-east-1",
                "123456789012",
                None,
            )
        assert error_type == "ResourceNotFoundException"
        assert result is None

    def test_non_python_runtime(self):
        fn = _make_mock_fn(runtime="nodejs18.x")
        p1, p2, _ = _patch_backend(fn)
        with p1, p2:
            _, error_type, _ = _invoke_lambda_sync(
                "arn:aws:lambda:us-east-1:123456789012:function:node-fn",
                {},
                "us-east-1",
                "123456789012",
                None,
            )
        assert error_type == "InvalidRuntime"

    def test_no_code(self):
        fn = _make_mock_fn(code_bytes=None, code={})
        p1, p2, _ = _patch_backend(fn)
        with p1, p2:
            _, error_type, _ = _invoke_lambda_sync(
                "arn:aws:lambda:us-east-1:123456789012:function:no-code",
                {},
                "us-east-1",
                "123456789012",
                None,
            )
        assert error_type == "InvalidCodeException"

    def test_callback_called_on_success(self):
        fn = _make_mock_fn(code_bytes=_echo_handler_zip())
        p1, p2, _ = _patch_backend(fn)
        callback = MagicMock()
        with p1, p2:
            _invoke_lambda_sync(
                "arn:aws:lambda:us-east-1:123456789012:function:echo",
                {"key": "val"},
                "us-east-1",
                "123456789012",
                callback,
            )
        callback.assert_called_once()
        assert callback.call_args[0][0] == {"key": "val"}
        assert callback.call_args[0][1] is None

    def test_callback_exception_does_not_propagate(self):
        fn = _make_mock_fn(code_bytes=_echo_handler_zip())
        p1, p2, _ = _patch_backend(fn)
        callback = MagicMock(side_effect=RuntimeError("callback boom"))
        with p1, p2:
            _, error_type, _ = _invoke_lambda_sync(
                "arn:aws:lambda:us-east-1:123456789012:function:echo",
                {},
                "us-east-1",
                "123456789012",
                callback,
            )
        assert error_type is None

    def test_handler_error_sets_error_type(self):
        code = "def handler(event, context):\n    raise ValueError('oops')\n"
        fn = _make_mock_fn(code_bytes=_make_zip({"lambda_function.py": code}))
        p1, p2, _ = _patch_backend(fn)
        with p1, p2:
            _, error_type, _ = _invoke_lambda_sync(
                "arn:aws:lambda:us-east-1:123456789012:function:err",
                {},
                "us-east-1",
                "123456789012",
                None,
            )
        assert error_type == "Handled"

    def test_code_from_zipfile_field(self):
        """When code_bytes is None, falls back to code['ZipFile'] (base64)."""
        import base64

        encoded = base64.b64encode(_echo_handler_zip()).decode()
        fn = _make_mock_fn(code_bytes=None, code={"ZipFile": encoded})
        p1, p2, _ = _patch_backend(fn)
        with p1, p2:
            result, error_type, _ = _invoke_lambda_sync(
                "arn:aws:lambda:us-east-1:123456789012:function:b64",
                {"x": 1},
                "us-east-1",
                "123456789012",
                None,
            )
        assert result == {"x": 1}
        assert error_type is None


# ---------------------------------------------------------------------------
# invoke_lambda_sync (public -- runs in thread pool)
# ---------------------------------------------------------------------------


class TestInvokeLambdaSync:
    @patch("robotocore.services.lambda_.invoke._invoke_lambda_sync")
    def test_returns_result(self, mock_internal):
        mock_internal.return_value = ({"ok": True}, None, "")
        result, error_type, _ = invoke_lambda_sync(
            "arn:aws:lambda:us-east-1:123456789012:function:fn",
            {},
            "us-east-1",
            "123456789012",
        )
        assert result == {"ok": True}
        assert error_type is None

    def test_timeout_returns_error(self):
        import concurrent.futures

        with patch("robotocore.services.lambda_.invoke._executor") as mock_executor:
            future = MagicMock()
            future.result.side_effect = concurrent.futures.TimeoutError()
            mock_executor.submit.return_value = future
            _, error_type, _ = invoke_lambda_sync(
                "arn:aws:lambda:us-east-1:123456789012:function:fn",
                {},
                "us-east-1",
                "123456789012",
            )
        assert error_type == "TaskTimedOut"

    @patch("robotocore.services.lambda_.invoke._executor")
    def test_generic_exception_returns_service_exception(self, mock_executor):
        future = MagicMock()
        future.result.side_effect = RuntimeError("boom")
        mock_executor.submit.return_value = future
        _, error_type, logs = invoke_lambda_sync(
            "arn:aws:lambda:us-east-1:123456789012:function:fn",
            {},
            "us-east-1",
            "123456789012",
        )
        assert error_type == "ServiceException"
        assert "boom" in logs


# ---------------------------------------------------------------------------
# invoke_lambda_async
# ---------------------------------------------------------------------------


class TestInvokeLambdaAsync:
    @patch("robotocore.services.lambda_.invoke._executor")
    def test_submits_to_thread_pool(self, mock_executor):
        invoke_lambda_async(
            "arn:aws:lambda:us-east-1:123456789012:function:fn",
            {"data": 1},
            "us-east-1",
            "123456789012",
        )
        mock_executor.submit.assert_called_once()
        args = mock_executor.submit.call_args[0]
        assert args[0] is _invoke_lambda_sync
        assert args[1] == "arn:aws:lambda:us-east-1:123456789012:function:fn"
        assert args[2] == {"data": 1}

    @patch("robotocore.services.lambda_.invoke._executor")
    def test_passes_callback(self, mock_executor):
        cb = MagicMock()
        invoke_lambda_async(
            "arn:aws:lambda:us-east-1:123456789012:function:fn",
            {},
            "us-east-1",
            "123456789012",
            callback=cb,
        )
        args = mock_executor.submit.call_args[0]
        assert args[5] is cb


# ---------------------------------------------------------------------------
# ARN parsing in _invoke_lambda_sync
# ---------------------------------------------------------------------------


class TestArnParsing:
    def test_extracts_function_name_from_full_arn(self):
        fn = _make_mock_fn(code_bytes=_echo_handler_zip())
        p1, p2, mock_be = _patch_backend(fn)
        with p1, p2:
            _invoke_lambda_sync(
                "arn:aws:lambda:us-west-2:999888777666:function:my-function",
                {},
                "us-west-2",
                "999888777666",
                None,
            )
        mock_be.get_function.assert_called_with("my-function")

    def test_short_name_used_as_is(self):
        fn = _make_mock_fn(code_bytes=_echo_handler_zip())
        p1, p2, mock_be = _patch_backend(fn)
        with p1, p2:
            _invoke_lambda_sync("just-a-name", {}, "us-east-1", "123456789012", None)
        mock_be.get_function.assert_called_with("just-a-name")
