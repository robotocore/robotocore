"""Semantic tests for Lambda limits integration with the provider layer.

These test the validation functions and concurrency tracker as they would be
used by CreateFunction, UpdateFunctionCode, and Invoke — but without needing
a running server. They test the business logic contracts.
"""

import pytest

from robotocore.services.lambda_.limits import (
    CodeStorageExceededException,
    ConcurrencyTracker,
    InvalidParameterValueException,
    RequestTooLargeException,
    TooManyRequestsException,
    validate_code_size_zipped,
    validate_envvar_size,
    validate_payload_size,
)


class TestCreateFunctionOversizedZip:
    """CreateFunction with oversized zip -> InvalidParameterValueException."""

    def test_oversized_zip_rejected(self, monkeypatch):
        monkeypatch.setenv("LAMBDA_LIMITS_CODE_SIZE_ZIPPED", "1000")
        with pytest.raises(InvalidParameterValueException):
            validate_code_size_zipped(2000)

    def test_valid_zip_accepted(self, monkeypatch):
        monkeypatch.setenv("LAMBDA_LIMITS_CODE_SIZE_ZIPPED", "1000")
        validate_code_size_zipped(999)  # no raise


class TestCreateFunctionOversizedEnvVars:
    """CreateFunction with oversized env vars -> InvalidParameterValueException."""

    def test_oversized_envvars_rejected(self, monkeypatch):
        monkeypatch.setenv("LAMBDA_LIMITS_MAX_FUNCTION_ENVVAR_SIZE_BYTES", "10")
        env = {"A": "x" * 20}
        with pytest.raises(InvalidParameterValueException):
            validate_envvar_size(env)


class TestInvokeOversizedPayload:
    """Invoke with oversized payload -> RequestTooLargeException."""

    def test_sync_oversized(self, monkeypatch):
        monkeypatch.setenv("LAMBDA_LIMITS_MAX_FUNCTION_PAYLOAD_SIZE_BYTES", "100")
        with pytest.raises(RequestTooLargeException):
            validate_payload_size(200, is_async=False)

    def test_async_oversized(self, monkeypatch):
        monkeypatch.setenv("LAMBDA_LIMITS_MAX_FUNCTION_PAYLOAD_ASYNC_BYTES", "50")
        with pytest.raises(RequestTooLargeException):
            validate_payload_size(100, is_async=True)


class TestInvokeAtConcurrencyLimit:
    """Invoke at concurrency limit -> TooManyRequestsException."""

    def test_at_account_limit(self, monkeypatch):
        monkeypatch.setenv("LAMBDA_LIMITS_CONCURRENT_EXECUTIONS", "1")
        tracker = ConcurrencyTracker()
        tracker.acquire("fn1")
        with pytest.raises(TooManyRequestsException):
            tracker.acquire("fn2")

    def test_at_function_reserved_limit(self, monkeypatch):
        monkeypatch.setenv("LAMBDA_LIMITS_CONCURRENT_EXECUTIONS", "100")
        monkeypatch.setenv("LAMBDA_LIMITS_MINIMUM_UNRESERVED_CONCURRENCY", "10")
        tracker = ConcurrencyTracker()
        tracker.set_reserved("fn1", 1)
        tracker.acquire("fn1")
        with pytest.raises(TooManyRequestsException):
            tracker.acquire("fn1")


class TestPutGetDeleteFunctionConcurrency:
    """PutFunctionConcurrency -> GetFunctionConcurrency returns correct value."""

    def test_put_then_get(self, monkeypatch):
        monkeypatch.setenv("LAMBDA_LIMITS_CONCURRENT_EXECUTIONS", "1000")
        tracker = ConcurrencyTracker()
        tracker.set_reserved("fn1", 42)
        assert tracker.get_reserved("fn1") == 42

    def test_delete_removes_reserved(self, monkeypatch):
        monkeypatch.setenv("LAMBDA_LIMITS_CONCURRENT_EXECUTIONS", "1000")
        tracker = ConcurrencyTracker()
        tracker.set_reserved("fn1", 42)
        tracker.delete_reserved("fn1")
        assert tracker.get_reserved("fn1") is None


class TestReservedConcurrencyMinimumUnreserved:
    """Reserved concurrency prevents over-reservation."""

    def test_unreserved_minimum_enforced(self, monkeypatch):
        monkeypatch.setenv("LAMBDA_LIMITS_CONCURRENT_EXECUTIONS", "100")
        monkeypatch.setenv("LAMBDA_LIMITS_MINIMUM_UNRESERVED_CONCURRENCY", "50")
        tracker = ConcurrencyTracker()
        tracker.set_reserved("fn1", 50)
        # Now only 50 left, minimum is 50, so cannot reserve any more
        with pytest.raises(InvalidParameterValueException, match="minimum value"):
            tracker.set_reserved("fn2", 1)


class TestAccountCodeSizeTracking:
    """Total account code size tracking across create/delete."""

    def test_code_size_accumulates(self):
        tracker = ConcurrencyTracker()
        tracker.add_code_size(1000)
        tracker.add_code_size(2000)
        assert tracker.get_total_code_size() == 3000

    def test_code_size_limit_exceeded(self, monkeypatch):
        monkeypatch.setenv("LAMBDA_LIMITS_TOTAL_CODE_SIZE", "5000")
        tracker = ConcurrencyTracker()
        tracker.add_code_size(4000)
        with pytest.raises(CodeStorageExceededException):
            tracker.add_code_size(2000)

    def test_delete_frees_space(self, monkeypatch):
        monkeypatch.setenv("LAMBDA_LIMITS_TOTAL_CODE_SIZE", "5000")
        tracker = ConcurrencyTracker()
        tracker.add_code_size(4000)
        tracker.remove_code_size(3000)
        # Now only 1000 used, can add 3999 more
        tracker.add_code_size(3999)
        assert tracker.get_total_code_size() == 4999
