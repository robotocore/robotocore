"""Unit tests for Lambda concurrency limits and code size validation."""

import threading

import pytest

from robotocore.services.lambda_.limits import (
    CodeStorageExceededException,
    ConcurrencyTracker,
    InvalidParameterValueException,
    RequestTooLargeException,
    TooManyRequestsException,
    get_code_size_zipped_limit,
    get_concurrent_executions_limit,
    get_max_function_envvar_size,
    get_max_payload_async,
    get_max_payload_sync,
    get_minimum_unreserved_concurrency,
    get_total_code_size_limit,
    is_prebuild_images,
    is_synchronous_create,
    validate_code_size_zipped,
    validate_envvar_size,
    validate_payload_size,
)

_MB = 1024 * 1024
_GB = 1024 * _MB


# ---------------------------------------------------------------------------
# Default limit values
# ---------------------------------------------------------------------------


class TestDefaultLimits:
    def test_concurrent_executions_default(self):
        assert get_concurrent_executions_limit() == 1000

    def test_minimum_unreserved_default(self):
        assert get_minimum_unreserved_concurrency() == 100

    def test_total_code_size_default(self):
        assert get_total_code_size_limit() == 80 * _GB

    def test_code_size_zipped_default(self):
        assert get_code_size_zipped_limit() == 50 * _MB

    def test_max_envvar_size_default(self):
        assert get_max_function_envvar_size() == 4096

    def test_max_payload_sync_default(self):
        assert get_max_payload_sync() == 6 * _MB

    def test_max_payload_async_default(self):
        assert get_max_payload_async() == 256 * 1024


class TestCustomLimitsFromEnv:
    def test_concurrent_executions_custom(self, monkeypatch):
        monkeypatch.setenv("LAMBDA_LIMITS_CONCURRENT_EXECUTIONS", "500")
        assert get_concurrent_executions_limit() == 500

    def test_minimum_unreserved_custom(self, monkeypatch):
        monkeypatch.setenv("LAMBDA_LIMITS_MINIMUM_UNRESERVED_CONCURRENCY", "50")
        assert get_minimum_unreserved_concurrency() == 50

    def test_total_code_size_custom(self, monkeypatch):
        monkeypatch.setenv("LAMBDA_LIMITS_TOTAL_CODE_SIZE", "1000000")
        assert get_total_code_size_limit() == 1000000

    def test_invalid_env_falls_back_to_default(self, monkeypatch):
        monkeypatch.setenv("LAMBDA_LIMITS_CONCURRENT_EXECUTIONS", "not-a-number")
        assert get_concurrent_executions_limit() == 1000


# ---------------------------------------------------------------------------
# Concurrent execution tracking
# ---------------------------------------------------------------------------


class TestConcurrencyTracking:
    @pytest.fixture(autouse=True)
    def tracker(self):
        self.t = ConcurrencyTracker()

    def test_acquire_and_release(self):
        self.t.acquire("fn1")
        assert self.t.get_function_count("fn1") == 1
        assert self.t.get_global_count() == 1
        self.t.release("fn1")
        assert self.t.get_function_count("fn1") == 0
        assert self.t.get_global_count() == 0

    def test_multiple_functions(self):
        self.t.acquire("fn1")
        self.t.acquire("fn2")
        assert self.t.get_global_count() == 2
        assert self.t.get_function_count("fn1") == 1
        assert self.t.get_function_count("fn2") == 1

    def test_account_limit_exceeded(self, monkeypatch):
        monkeypatch.setenv("LAMBDA_LIMITS_CONCURRENT_EXECUTIONS", "2")
        self.t.acquire("fn1")
        self.t.acquire("fn2")
        with pytest.raises(TooManyRequestsException, match="Rate Exceeded"):
            self.t.acquire("fn3")

    def test_function_reserved_limit_exceeded(self, monkeypatch):
        monkeypatch.setenv("LAMBDA_LIMITS_CONCURRENT_EXECUTIONS", "1000")
        self.t.set_reserved("fn1", 2)
        self.t.acquire("fn1")
        self.t.acquire("fn1")
        with pytest.raises(TooManyRequestsException, match="reserved concurrency"):
            self.t.acquire("fn1")

    def test_release_below_zero_is_safe(self):
        self.t.release("fn1")
        assert self.t.get_function_count("fn1") == 0
        assert self.t.get_global_count() == 0

    def test_unreserved_pool_exhausted(self, monkeypatch):
        monkeypatch.setenv("LAMBDA_LIMITS_CONCURRENT_EXECUTIONS", "10")
        monkeypatch.setenv("LAMBDA_LIMITS_MINIMUM_UNRESERVED_CONCURRENCY", "1")
        # Reserve 8 for fn1 → only 2 unreserved
        self.t.set_reserved("fn1", 8)
        # Use 2 unreserved slots
        self.t.acquire("fn2")
        self.t.acquire("fn3")
        with pytest.raises(TooManyRequestsException, match="unreserved"):
            self.t.acquire("fn4")


# ---------------------------------------------------------------------------
# Reserved concurrency
# ---------------------------------------------------------------------------


class TestReservedConcurrency:
    @pytest.fixture(autouse=True)
    def tracker(self):
        self.t = ConcurrencyTracker()

    def test_set_get_reserved(self):
        self.t.set_reserved("fn1", 50)
        assert self.t.get_reserved("fn1") == 50

    def test_get_reserved_unset(self):
        assert self.t.get_reserved("fn1") is None

    def test_delete_reserved(self):
        self.t.set_reserved("fn1", 50)
        self.t.delete_reserved("fn1")
        assert self.t.get_reserved("fn1") is None

    def test_minimum_unreserved_check(self, monkeypatch):
        monkeypatch.setenv("LAMBDA_LIMITS_CONCURRENT_EXECUTIONS", "200")
        monkeypatch.setenv("LAMBDA_LIMITS_MINIMUM_UNRESERVED_CONCURRENCY", "100")
        # Can reserve up to 100 (200 - 100 minimum unreserved)
        self.t.set_reserved("fn1", 50)
        self.t.set_reserved("fn2", 50)
        # Trying to reserve 1 more would leave only 99 unreserved
        with pytest.raises(InvalidParameterValueException, match="minimum value"):
            self.t.set_reserved("fn3", 1)

    def test_update_reserved_checks_correctly(self, monkeypatch):
        monkeypatch.setenv("LAMBDA_LIMITS_CONCURRENT_EXECUTIONS", "200")
        monkeypatch.setenv("LAMBDA_LIMITS_MINIMUM_UNRESERVED_CONCURRENCY", "100")
        self.t.set_reserved("fn1", 50)
        # Updating fn1 from 50 to 100 should work (total reserved = 100, unreserved = 100)
        self.t.set_reserved("fn1", 100)
        assert self.t.get_reserved("fn1") == 100


# ---------------------------------------------------------------------------
# Code size validation
# ---------------------------------------------------------------------------


class TestCodeSizeValidation:
    def test_accept_under_limit(self):
        validate_code_size_zipped(1000)  # should not raise

    def test_reject_over_limit(self, monkeypatch):
        monkeypatch.setenv("LAMBDA_LIMITS_CODE_SIZE_ZIPPED", "1000")
        with pytest.raises(InvalidParameterValueException, match="smaller than"):
            validate_code_size_zipped(1001)

    def test_exact_limit_accepted(self, monkeypatch):
        monkeypatch.setenv("LAMBDA_LIMITS_CODE_SIZE_ZIPPED", "1000")
        validate_code_size_zipped(1000)  # should not raise


# ---------------------------------------------------------------------------
# Env var size validation
# ---------------------------------------------------------------------------


class TestEnvVarValidation:
    def test_accept_small_envvars(self):
        validate_envvar_size({"FOO": "bar"})  # should not raise

    def test_reject_oversized_envvars(self, monkeypatch):
        monkeypatch.setenv("LAMBDA_LIMITS_MAX_FUNCTION_ENVVAR_SIZE_BYTES", "10")
        with pytest.raises(InvalidParameterValueException, match="4KB limit"):
            validate_envvar_size({"LONG_KEY": "x" * 20})


# ---------------------------------------------------------------------------
# Payload size validation
# ---------------------------------------------------------------------------


class TestPayloadValidation:
    def test_sync_payload_within_limit(self):
        validate_payload_size(1000, is_async=False)  # should not raise

    def test_sync_payload_over_limit(self, monkeypatch):
        monkeypatch.setenv("LAMBDA_LIMITS_MAX_FUNCTION_PAYLOAD_SIZE_BYTES", "100")
        with pytest.raises(RequestTooLargeException, match="exceeds"):
            validate_payload_size(101, is_async=False)

    def test_async_payload_over_limit(self, monkeypatch):
        monkeypatch.setenv("LAMBDA_LIMITS_MAX_FUNCTION_PAYLOAD_ASYNC_BYTES", "100")
        with pytest.raises(RequestTooLargeException, match="exceeds"):
            validate_payload_size(101, is_async=True)

    def test_async_payload_within_limit(self, monkeypatch):
        monkeypatch.setenv("LAMBDA_LIMITS_MAX_FUNCTION_PAYLOAD_ASYNC_BYTES", "200")
        validate_payload_size(200, is_async=True)  # should not raise


# ---------------------------------------------------------------------------
# Total account code size tracking
# ---------------------------------------------------------------------------


class TestTotalCodeSize:
    @pytest.fixture(autouse=True)
    def tracker(self):
        self.t = ConcurrencyTracker()

    def test_add_code_size(self):
        self.t.add_code_size(1000)
        assert self.t.get_total_code_size() == 1000

    def test_remove_code_size(self):
        self.t.add_code_size(1000)
        self.t.remove_code_size(500)
        assert self.t.get_total_code_size() == 500

    def test_exceed_total_code_size(self, monkeypatch):
        monkeypatch.setenv("LAMBDA_LIMITS_TOTAL_CODE_SIZE", "1000")
        self.t.add_code_size(900)
        with pytest.raises(CodeStorageExceededException, match="limit exceeded"):
            self.t.add_code_size(200)

    def test_remove_below_zero_safe(self):
        self.t.remove_code_size(100)
        assert self.t.get_total_code_size() == 0


# ---------------------------------------------------------------------------
# Provisioned concurrency tracking
# ---------------------------------------------------------------------------


class TestProvisionedConcurrency:
    @pytest.fixture(autouse=True)
    def tracker(self):
        self.t = ConcurrencyTracker()

    def test_set_get_provisioned(self):
        self.t.set_provisioned("fn1:1", 10)
        assert self.t.get_provisioned("fn1:1") == 10

    def test_delete_provisioned(self):
        self.t.set_provisioned("fn1:1", 10)
        self.t.delete_provisioned("fn1:1")
        assert self.t.get_provisioned("fn1:1") is None


# ---------------------------------------------------------------------------
# Thread safety
# ---------------------------------------------------------------------------


class TestThreadSafety:
    def test_concurrent_acquire_release(self):
        tracker = ConcurrencyTracker()
        errors = []

        def worker(fn_key: str, count: int):
            try:
                for _ in range(count):
                    tracker.acquire(fn_key)
                for _ in range(count):
                    tracker.release(fn_key)
            except Exception as e:
                errors.append(e)

        threads = [threading.Thread(target=worker, args=(f"fn{i}", 50)) for i in range(10)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        assert not errors
        assert tracker.get_global_count() == 0

    def test_concurrent_set_reserved(self, monkeypatch):
        monkeypatch.setenv("LAMBDA_LIMITS_CONCURRENT_EXECUTIONS", "10000")
        monkeypatch.setenv("LAMBDA_LIMITS_MINIMUM_UNRESERVED_CONCURRENCY", "100")
        tracker = ConcurrencyTracker()
        errors = []

        def worker(fn_key: str, amount: int):
            try:
                tracker.set_reserved(fn_key, amount)
            except Exception as e:
                errors.append(e)

        threads = [threading.Thread(target=worker, args=(f"fn{i}", 10)) for i in range(50)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        # Some may have failed due to unreserved limit — that's fine
        # But no crashes
        total_reserved = sum(tracker.get_reserved(f"fn{i}") or 0 for i in range(50))
        assert total_reserved <= 9900  # 10000 - 100 minimum unreserved


# ---------------------------------------------------------------------------
# Lifecycle flags
# ---------------------------------------------------------------------------


class TestLifecycleFlags:
    def test_synchronous_create_default_false(self):
        assert is_synchronous_create() is False

    def test_synchronous_create_enabled(self, monkeypatch):
        monkeypatch.setenv("LAMBDA_SYNCHRONOUS_CREATE", "true")
        assert is_synchronous_create() is True

    def test_prebuild_images_default_false(self):
        assert is_prebuild_images() is False

    def test_prebuild_images_enabled(self, monkeypatch):
        monkeypatch.setenv("LAMBDA_PREBUILD_IMAGES", "1")
        assert is_prebuild_images() is True
