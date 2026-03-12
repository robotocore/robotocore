"""Unit tests for Lambda recursion detection enforcement."""

import pytest

from robotocore.services.lambda_.recursion import (
    MAX_RECURSION_DEPTH,
    RecursiveInvocationException,
    check_recursion,
    decrement_depth,
    get_recursion_depth,
    increment_depth,
    reset_all_depths,
)


@pytest.fixture(autouse=True)
def _clean_depths():
    """Reset recursion depth counters before and after each test."""
    reset_all_depths()
    yield
    reset_all_depths()


ACCOUNT = "123456789012"
REGION = "us-east-1"
FUNC = "my-function"


class TestRecursionDepthTracking:
    """Test the basic depth increment/decrement/get operations."""

    def test_initial_depth_is_zero(self):
        assert get_recursion_depth(ACCOUNT, REGION, FUNC) == 0

    def test_increment_returns_new_depth(self):
        assert increment_depth(ACCOUNT, REGION, FUNC) == 1
        assert increment_depth(ACCOUNT, REGION, FUNC) == 2
        assert increment_depth(ACCOUNT, REGION, FUNC) == 3

    def test_decrement_reduces_depth(self):
        increment_depth(ACCOUNT, REGION, FUNC)
        increment_depth(ACCOUNT, REGION, FUNC)
        assert get_recursion_depth(ACCOUNT, REGION, FUNC) == 2

        decrement_depth(ACCOUNT, REGION, FUNC)
        assert get_recursion_depth(ACCOUNT, REGION, FUNC) == 1

    def test_decrement_below_zero_stays_at_zero(self):
        decrement_depth(ACCOUNT, REGION, FUNC)
        assert get_recursion_depth(ACCOUNT, REGION, FUNC) == 0

    def test_decrement_to_zero_cleans_up_key(self):
        increment_depth(ACCOUNT, REGION, FUNC)
        decrement_depth(ACCOUNT, REGION, FUNC)
        assert get_recursion_depth(ACCOUNT, REGION, FUNC) == 0

    def test_different_functions_tracked_independently(self):
        increment_depth(ACCOUNT, REGION, "func-a")
        increment_depth(ACCOUNT, REGION, "func-a")
        increment_depth(ACCOUNT, REGION, "func-b")

        assert get_recursion_depth(ACCOUNT, REGION, "func-a") == 2
        assert get_recursion_depth(ACCOUNT, REGION, "func-b") == 1

    def test_different_regions_tracked_independently(self):
        increment_depth(ACCOUNT, "us-east-1", FUNC)
        increment_depth(ACCOUNT, "us-west-2", FUNC)
        increment_depth(ACCOUNT, "us-west-2", FUNC)

        assert get_recursion_depth(ACCOUNT, "us-east-1", FUNC) == 1
        assert get_recursion_depth(ACCOUNT, "us-west-2", FUNC) == 2

    def test_different_accounts_tracked_independently(self):
        increment_depth("111111111111", REGION, FUNC)
        increment_depth("222222222222", REGION, FUNC)
        increment_depth("222222222222", REGION, FUNC)

        assert get_recursion_depth("111111111111", REGION, FUNC) == 1
        assert get_recursion_depth("222222222222", REGION, FUNC) == 2

    def test_reset_all_depths(self):
        increment_depth(ACCOUNT, REGION, "func-a")
        increment_depth(ACCOUNT, REGION, "func-b")
        reset_all_depths()

        assert get_recursion_depth(ACCOUNT, REGION, "func-a") == 0
        assert get_recursion_depth(ACCOUNT, REGION, "func-b") == 0


class TestCheckRecursion:
    """Test the check_recursion enforcement logic."""

    def test_no_error_below_threshold(self):
        """No exception when depth is below MAX_RECURSION_DEPTH."""
        for _ in range(MAX_RECURSION_DEPTH - 1):
            increment_depth(ACCOUNT, REGION, FUNC)
        # Should not raise — we're at depth 15, threshold is 16
        check_recursion(ACCOUNT, REGION, FUNC, "Terminate")

    def test_error_at_threshold(self):
        """RecursiveInvocationException at exactly MAX_RECURSION_DEPTH."""
        for _ in range(MAX_RECURSION_DEPTH):
            increment_depth(ACCOUNT, REGION, FUNC)
        with pytest.raises(RecursiveInvocationException) as exc_info:
            check_recursion(ACCOUNT, REGION, FUNC, "Terminate")
        assert FUNC in str(exc_info.value)
        assert exc_info.value.depth == MAX_RECURSION_DEPTH

    def test_error_above_threshold(self):
        """RecursiveInvocationException above MAX_RECURSION_DEPTH."""
        for _ in range(MAX_RECURSION_DEPTH + 5):
            increment_depth(ACCOUNT, REGION, FUNC)
        with pytest.raises(RecursiveInvocationException):
            check_recursion(ACCOUNT, REGION, FUNC, "Terminate")

    def test_allow_mode_skips_check(self):
        """No exception when mode is 'Allow', even above threshold."""
        for _ in range(MAX_RECURSION_DEPTH + 10):
            increment_depth(ACCOUNT, REGION, FUNC)
        # Should not raise
        check_recursion(ACCOUNT, REGION, FUNC, "Allow")

    def test_terminate_is_default(self):
        """Default behavior is 'Terminate'."""
        for _ in range(MAX_RECURSION_DEPTH):
            increment_depth(ACCOUNT, REGION, FUNC)
        with pytest.raises(RecursiveInvocationException):
            check_recursion(ACCOUNT, REGION, FUNC)

    def test_zero_depth_always_passes(self):
        """Fresh function with no invocations always passes."""
        check_recursion(ACCOUNT, REGION, FUNC, "Terminate")


class TestRecursiveInvocationException:
    """Test the exception itself."""

    def test_attributes(self):
        exc = RecursiveInvocationException("my-func", 16)
        assert exc.function_name == "my-func"
        assert exc.depth == 16
        assert "my-func" in str(exc)
        assert "16" in str(exc)

    def test_is_exception(self):
        assert issubclass(RecursiveInvocationException, Exception)


class TestMaxRecursionDepthConstant:
    """Verify the threshold matches AWS behavior."""

    def test_max_depth_is_16(self):
        assert MAX_RECURSION_DEPTH == 16
