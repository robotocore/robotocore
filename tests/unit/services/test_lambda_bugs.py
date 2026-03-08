"""Failing tests that expose bugs in the Lambda native provider.

Each test has a docstring explaining the bug it targets. These tests are
expected to FAIL against the current code. Do NOT fix the provider — only
add tests here.
"""

import json
from unittest.mock import MagicMock, patch

import pytest

from robotocore.services.lambda_.event_source import _match_pattern
from robotocore.services.lambda_.provider import (
    _esm_store,
    _provisioned_concurrency,
    handle_lambda_request,
)

# ---------------------------------------------------------------------------
# Helpers (same pattern as existing tests)
# ---------------------------------------------------------------------------


def _make_scope(method: str, path: str, body: bytes = b"", headers: dict | None = None):
    hdrs = headers or {}
    raw_headers = [(k.lower().encode(), v.encode()) for k, v in hdrs.items()]
    return {
        "type": "http",
        "method": method,
        "path": path,
        "query_string": b"",
        "headers": raw_headers,
        "root_path": "",
        "scheme": "http",
        "server": ("localhost", 4566),
    }


def _make_scope_with_qs(
    method: str,
    path: str,
    query_string: str = "",
    body: bytes = b"",
    headers: dict | None = None,
):
    hdrs = headers or {}
    raw_headers = [(k.lower().encode(), v.encode()) for k, v in hdrs.items()]
    return {
        "type": "http",
        "method": method,
        "path": path,
        "query_string": query_string.encode(),
        "headers": raw_headers,
        "root_path": "",
        "scheme": "http",
        "server": ("localhost", 4566),
    }


async def _make_request(method, path, body=b"", headers=None, query_string=""):
    from starlette.requests import Request

    if query_string:
        scope = _make_scope_with_qs(method, path, query_string, body, headers)
    else:
        scope = _make_scope(method, path, body, headers)

    async def receive():
        return {"type": "http.request", "body": body}

    return Request(scope, receive)


# ===========================================================================
# Bug 1: DryRun invocation executes the Lambda function
# ===========================================================================


@pytest.mark.asyncio
class TestDryRunExecutesFunction:
    """Bug: DryRun invocation type should NOT execute the function.

    In real AWS, DryRun only validates that the caller has permission to invoke
    the function. It returns 204 without executing any code. The provider
    executes the function fully (including running user code) and then discards
    the result, which is both incorrect and wasteful.
    """

    @patch("robotocore.services.lambda_.provider._get_moto_backend")
    @patch("robotocore.services.lambda_.provider.get_executor_for_runtime")
    async def test_dryrun_should_not_execute_code(self, mock_get_executor, mock_backend_fn):
        mock_backend = MagicMock()
        mock_fn = MagicMock()
        mock_fn.run_time = "python3.12"
        mock_fn.code = {"ZipFile": "UEsFBgAAAAAAAAAAAAAAAAAAAAAAAA=="}
        mock_fn.code_bytes = b"fake-zip"
        mock_fn.handler = "lambda_function.handler"
        mock_fn.timeout = 3
        mock_fn.memory_size = 128
        mock_fn.environment_vars = {}
        mock_backend.get_function.return_value = mock_fn
        mock_backend_fn.return_value = mock_backend

        mock_executor = MagicMock()
        mock_executor.execute.return_value = ("result", None, "logs")
        mock_get_executor.return_value = mock_executor

        req = await _make_request(
            "POST",
            "/2015-03-31/functions/my-fn/invocations",
            body=b"{}",
            headers={"x-amz-invocation-type": "DryRun"},
        )
        resp = await handle_lambda_request(req, "us-east-1", "123456789012")
        assert resp.status_code == 204
        # The executor should NOT have been called for DryRun
        mock_executor.execute.assert_not_called()


# ===========================================================================
# Bug 2: Invoke with invalid JSON body returns 500 instead of proper error
# ===========================================================================


@pytest.mark.asyncio
class TestInvokeInvalidJsonBody:
    """Bug: Passing non-JSON bytes to Invoke should return InvalidRequestContentException.

    AWS returns a 400 with InvalidRequestContentException when the request
    body is not valid JSON. The provider calls json.loads(body) which raises
    a JSONDecodeError, caught by the generic exception handler, returning 500.
    """

    @patch("robotocore.services.lambda_.provider._get_moto_backend")
    async def test_invalid_json_returns_400_not_500(self, mock_backend_fn):
        mock_backend = MagicMock()
        mock_fn = MagicMock()
        mock_fn.run_time = "python3.12"
        mock_fn.code = {}
        mock_fn.code_bytes = None
        mock_fn.handler = "lambda_function.handler"
        mock_fn.timeout = 3
        mock_fn.memory_size = 128
        mock_fn.environment_vars = {}
        mock_backend.get_function.return_value = mock_fn
        mock_backend_fn.return_value = mock_backend

        req = await _make_request(
            "POST",
            "/2015-03-31/functions/my-fn/invocations",
            body=b"\x80\x81\x82",  # Not valid JSON
        )
        resp = await handle_lambda_request(req, "us-east-1", "123456789012")
        # AWS returns 400 with InvalidRequestContentException, not 500
        assert resp.status_code == 400
        data = json.loads(resp.body)
        assert "InvalidRequestContent" in data["__type"]


# ===========================================================================
# Bug 3: ESM update ignores FunctionName change
# ===========================================================================


@pytest.mark.asyncio
class TestEsmUpdateIgnoresFunctionName:
    """Bug: UpdateEventSourceMapping should allow changing the FunctionName.

    AWS allows updating the target function via the FunctionName field in
    an UpdateEventSourceMapping call. The provider only updates a hardcoded
    list of fields and ignores FunctionName entirely, so the mapping continues
    to point at the old function.
    """

    async def test_update_esm_function_name(self):
        _esm_store.clear()
        _esm_store["update-uuid"] = {
            "UUID": "update-uuid",
            "EventSourceArn": "arn:aws:sqs:us-east-1:123:q",
            "FunctionArn": "arn:aws:lambda:us-east-1:123:function:old-fn",
            "BatchSize": 10,
            "State": "Enabled",
            "_region": "us-east-1",
            "_account_id": "123",
        }

        body = json.dumps({"FunctionName": "new-fn"}).encode()
        req = await _make_request("PUT", "/2015-03-31/event-source-mappings/update-uuid", body)
        resp = await handle_lambda_request(req, "us-east-1", "123456789012")
        assert resp.status_code == 200
        data = json.loads(resp.body)
        # The FunctionArn should have been updated to point at new-fn
        assert "new-fn" in data["FunctionArn"]

        _esm_store.clear()


# ===========================================================================
# Bug 4: Provisioned concurrency accepts $LATEST qualifier
# ===========================================================================


@pytest.mark.asyncio
class TestProvisionedConcurrencyLatestQualifier:
    """Bug: PutProvisionedConcurrencyConfig should reject $LATEST qualifier.

    AWS does not allow provisioned concurrency on $LATEST — only on published
    versions or aliases. The provider accepts $LATEST as the default qualifier
    and happily stores provisioned concurrency for it.
    """

    @patch("robotocore.services.lambda_.provider._get_moto_backend")
    async def test_provisioned_concurrency_rejects_latest(self, mock_backend_fn):
        mock_backend = MagicMock()
        mock_fn = MagicMock()
        mock_fn.get_configuration.return_value = {"FunctionName": "my-fn"}
        mock_backend.get_function.return_value = mock_fn
        mock_backend_fn.return_value = mock_backend

        _provisioned_concurrency.clear()

        body = json.dumps({"ProvisionedConcurrentExecutions": 5}).encode()
        # No Qualifier param means default is $LATEST
        req = await _make_request(
            "PUT",
            "/2015-03-31/functions/my-fn/provisioned-concurrency",
            body,
        )
        resp = await handle_lambda_request(req, "us-east-1", "123456789012")
        # AWS returns 400 InvalidParameterValueException for $LATEST
        assert resp.status_code == 400

        _provisioned_concurrency.clear()


# ===========================================================================
# Bug 5: exists:false filter matches when key is absent but skips remaining
#         pattern keys
# ===========================================================================


class TestFilterCriteriaExistsFalseBug:
    """Bug: exists:false in FilterCriteria short-circuits remaining pattern keys.

    When a record is missing a key that has an [{"exists": false}] filter,
    _match_pattern returns True immediately (line 106) without checking the
    remaining keys in the pattern. This means a pattern like:
        {"missingKey": [{"exists": false}], "presentKey": ["requiredValue"]}
    will match a record that is missing both keys, which is incorrect.
    """

    def test_exists_false_should_not_skip_remaining_keys(self):
        pattern = {
            "missingKey": [{"exists": False}],
            "status": ["active"],
        }
        # Record has neither missingKey nor status — should NOT match
        # because status != "active"
        record = {"other": "data"}
        result = _match_pattern(record, pattern)
        assert result is False


# ===========================================================================
# Bug 6: Error response includes both "Type" and "__type" fields
# ===========================================================================


class TestErrorResponseFormat:
    """Bug: Lambda error responses should not include the "Type" field.

    Real AWS Lambda error responses have the structure:
        {"Message": "...", "__type": "ResourceNotFoundException"}
    or sometimes just "message" (lowercase). They do NOT include a top-level
    "Type" field with value "User". The "Type" field is an error classification
    internal to Lambda (User vs Service) that AWS does not expose in the
    response body. Including it produces responses that don't match the real
    AWS wire format.
    """

    def test_error_response_should_not_have_type_field(self):
        from robotocore.services.lambda_.provider import _error

        resp = _error("ResourceNotFoundException", "Function not found", 404)
        data = json.loads(resp.body)
        # Real AWS doesn't include "Type" in the response body
        assert "Type" not in data
        # But it should have __type and Message
        assert data["__type"] == "ResourceNotFoundException"
        assert data["Message"] == "Function not found"


# ===========================================================================
# Bug 7: Invoke does not support Qualifier query parameter
# ===========================================================================


@pytest.mark.asyncio
class TestInvokeQualifier:
    """Bug: Invoke should support the Qualifier query parameter.

    AWS allows invoking a specific version or alias via:
        POST /functions/{name}/invocations?Qualifier=1
    The provider ignores the Qualifier and always invokes $LATEST.
    The x-amz-executed-version header should reflect the actual version
    invoked, not always "$LATEST".
    """

    @patch("robotocore.services.lambda_.provider._get_moto_backend")
    @patch("robotocore.services.lambda_.provider.get_executor_for_runtime")
    async def test_invoke_with_qualifier_uses_version(self, mock_get_executor, mock_backend_fn):
        mock_backend = MagicMock()
        mock_fn = MagicMock()
        mock_fn.run_time = "python3.12"
        mock_fn.code = {}
        mock_fn.code_bytes = b"fake"
        mock_fn.handler = "lambda_function.handler"
        mock_fn.timeout = 3
        mock_fn.memory_size = 128
        mock_fn.environment_vars = {}
        mock_backend.get_function.return_value = mock_fn
        mock_backend_fn.return_value = mock_backend

        mock_executor = MagicMock()
        mock_executor.execute.return_value = ({"ok": True}, None, "")
        mock_get_executor.return_value = mock_executor

        req = await _make_request(
            "POST",
            "/2015-03-31/functions/my-fn/invocations",
            body=b"{}",
            query_string="Qualifier=1",
        )
        resp = await handle_lambda_request(req, "us-east-1", "123456789012")
        assert resp.status_code == 200
        # The executed-version header should reflect the qualifier, not $LATEST
        assert resp.headers.get("x-amz-executed-version") == "1"


# ===========================================================================
# Bug 8: Event (async) invocation executes Lambda synchronously before
#         returning 202
# ===========================================================================


@pytest.mark.asyncio
class TestAsyncInvocationIsSync:
    """Bug: Event invocation type should return 202 immediately, not after execution.

    AWS Event invocations are fire-and-forget: the API returns 202 immediately
    and the function executes asynchronously. The provider runs the full
    Lambda execution synchronously (including user code), dispatches results,
    and only then returns 202. This means:
    - Slow Lambda functions block the API call
    - Errors in the function code could affect the 202 response
    - The dispatch_async_result is called with the actual result, which is
      correct for destinations, but the timing is wrong

    We verify that the function execution should not block the response.
    """

    @patch("robotocore.services.lambda_.provider._get_moto_backend")
    @patch("robotocore.services.lambda_.provider.get_executor_for_runtime")
    @patch("robotocore.services.lambda_.provider._dispatch_async_result")
    async def test_event_invocation_does_not_return_error_payload(
        self, mock_dispatch, mock_get_executor, mock_backend_fn
    ):
        mock_backend = MagicMock()
        mock_fn = MagicMock()
        mock_fn.run_time = "python3.12"
        mock_fn.code = {}
        mock_fn.code_bytes = b"fake"
        mock_fn.handler = "lambda_function.handler"
        mock_fn.timeout = 3
        mock_fn.memory_size = 128
        mock_fn.environment_vars = {}
        mock_backend.get_function.return_value = mock_fn
        mock_backend_fn.return_value = mock_backend

        # Simulate a function error
        mock_executor = MagicMock()
        mock_executor.execute.return_value = (None, "Handled", "error traceback")
        mock_get_executor.return_value = mock_executor

        req = await _make_request(
            "POST",
            "/2015-03-31/functions/my-fn/invocations",
            body=b"{}",
            headers={"x-amz-invocation-type": "Event"},
        )
        resp = await handle_lambda_request(req, "us-east-1", "123456789012")
        assert resp.status_code == 202
        # AWS does NOT include x-amz-function-error for Event invocations
        # because the error is only reported via destinations/DLQ, not the response
        assert "x-amz-function-error" not in resp.headers
