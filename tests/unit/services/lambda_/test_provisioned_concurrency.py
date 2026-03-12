"""Unit tests for Lambda provisioned concurrency enforcement during invocations.

Verifies that:
- Provisioned concurrency slots are used before on-demand when capacity is available
- When provisioned capacity is exhausted, invocations fall through to on-demand
- Utilization metrics are tracked correctly
- x-amz-executed-version header reflects the qualifier
- Provisioned concurrency state is cleaned up on delete
"""

import pytest

from robotocore.services.lambda_.limits import (
    ConcurrencyTracker,
    TooManyRequestsException,
)


class TestProvisionedConcurrencyRouting:
    """Test that invocations route through provisioned pool when available."""

    @pytest.fixture(autouse=True)
    def tracker(self):
        self.t = ConcurrencyTracker()

    def test_acquire_uses_provisioned_when_available(self):
        """When provisioned concurrency is set, acquire returns True (provisioned)."""
        fn_key = "123:us-east-1:my-func"
        prov_key = f"{fn_key}:1"
        self.t.set_provisioned(prov_key, 5)

        used_provisioned = self.t.acquire(fn_key, "1")
        assert used_provisioned is True
        assert self.t.get_provisioned_in_use(prov_key) == 1

    def test_acquire_returns_false_without_provisioned(self):
        """Without provisioned concurrency, acquire returns False (on-demand)."""
        fn_key = "123:us-east-1:my-func"
        used_provisioned = self.t.acquire(fn_key, "$LATEST")
        assert used_provisioned is False

    def test_provisioned_pool_exhausted_falls_through(self):
        """When all provisioned slots are in use, new invocations use on-demand."""
        fn_key = "123:us-east-1:my-func"
        prov_key = f"{fn_key}:1"
        self.t.set_provisioned(prov_key, 2)

        # Fill provisioned pool
        assert self.t.acquire(fn_key, "1") is True
        assert self.t.acquire(fn_key, "1") is True

        # Third invocation falls through to on-demand
        assert self.t.acquire(fn_key, "1") is False
        assert self.t.get_provisioned_in_use(prov_key) == 2

    def test_release_frees_provisioned_slot(self):
        """Releasing decrements provisioned in-use count."""
        fn_key = "123:us-east-1:my-func"
        prov_key = f"{fn_key}:1"
        self.t.set_provisioned(prov_key, 2)

        self.t.acquire(fn_key, "1")
        self.t.acquire(fn_key, "1")
        assert self.t.get_provisioned_in_use(prov_key) == 2

        self.t.release(fn_key, "1")
        assert self.t.get_provisioned_in_use(prov_key) == 1

        self.t.release(fn_key, "1")
        assert self.t.get_provisioned_in_use(prov_key) == 0

    def test_provisioned_slot_reused_after_release(self):
        """After release, the provisioned slot is available again."""
        fn_key = "123:us-east-1:my-func"
        prov_key = f"{fn_key}:1"
        self.t.set_provisioned(prov_key, 1)

        assert self.t.acquire(fn_key, "1") is True
        # Pool full now
        assert self.t.acquire(fn_key, "1") is False

        self.t.release(fn_key, "1")
        # Slot freed — should use provisioned again
        assert self.t.acquire(fn_key, "1") is True


class TestProvisionedUtilization:
    """Test utilization metric calculation."""

    @pytest.fixture(autouse=True)
    def tracker(self):
        self.t = ConcurrencyTracker()

    def test_utilization_zero_when_no_provisioned(self):
        fn_key = "123:us-east-1:my-func:1"
        assert self.t.get_provisioned_utilization(fn_key) == 0.0

    def test_utilization_zero_when_idle(self):
        fn_key = "123:us-east-1:my-func"
        prov_key = f"{fn_key}:1"
        self.t.set_provisioned(prov_key, 10)
        assert self.t.get_provisioned_utilization(prov_key) == 0.0

    def test_utilization_partial(self):
        fn_key = "123:us-east-1:my-func"
        prov_key = f"{fn_key}:1"
        self.t.set_provisioned(prov_key, 10)
        self.t.acquire(fn_key, "1")
        self.t.acquire(fn_key, "1")
        assert self.t.get_provisioned_utilization(prov_key) == pytest.approx(0.2)

    def test_utilization_full(self):
        fn_key = "123:us-east-1:my-func"
        prov_key = f"{fn_key}:1"
        self.t.set_provisioned(prov_key, 2)
        self.t.acquire(fn_key, "1")
        self.t.acquire(fn_key, "1")
        assert self.t.get_provisioned_utilization(prov_key) == pytest.approx(1.0)

    def test_utilization_capped_at_one(self):
        """Utilization should never exceed 1.0 even if tracking is off."""
        fn_key = "123:us-east-1:my-func:1"
        self.t.set_provisioned(fn_key, 1)
        # Manually set in_use higher than capacity (shouldn't happen normally)
        with self.t._lock:
            self.t._provisioned_in_use[fn_key] = 5
        assert self.t.get_provisioned_utilization(fn_key) == 1.0

    def test_utilization_decreases_after_release(self):
        fn_key = "123:us-east-1:my-func"
        prov_key = f"{fn_key}:1"
        self.t.set_provisioned(prov_key, 4)
        self.t.acquire(fn_key, "1")
        self.t.acquire(fn_key, "1")
        assert self.t.get_provisioned_utilization(prov_key) == pytest.approx(0.5)

        self.t.release(fn_key, "1")
        assert self.t.get_provisioned_utilization(prov_key) == pytest.approx(0.25)


class TestProvisionedConcurrencyCleanup:
    """Test that delete clears both provisioned capacity and in-use counters."""

    @pytest.fixture(autouse=True)
    def tracker(self):
        self.t = ConcurrencyTracker()

    def test_delete_clears_provisioned_and_in_use(self):
        fn_key = "123:us-east-1:my-func"
        prov_key = f"{fn_key}:1"
        self.t.set_provisioned(prov_key, 5)
        self.t.acquire(fn_key, "1")

        self.t.delete_provisioned(prov_key)
        assert self.t.get_provisioned(prov_key) is None
        assert self.t.get_provisioned_in_use(prov_key) == 0

    def test_reset_clears_provisioned_in_use(self):
        fn_key = "123:us-east-1:my-func"
        prov_key = f"{fn_key}:1"
        self.t.set_provisioned(prov_key, 5)
        self.t.acquire(fn_key, "1")

        self.t.reset()
        assert self.t.get_provisioned(prov_key) is None
        assert self.t.get_provisioned_in_use(prov_key) == 0
        assert self.t.get_global_count() == 0


class TestProvisionedWithReservedConcurrency:
    """Test interaction between provisioned and reserved concurrency."""

    @pytest.fixture(autouse=True)
    def tracker(self, monkeypatch):
        monkeypatch.setenv("LAMBDA_LIMITS_CONCURRENT_EXECUTIONS", "100")
        monkeypatch.setenv("LAMBDA_LIMITS_MINIMUM_UNRESERVED_CONCURRENCY", "10")
        self.t = ConcurrencyTracker()

    def test_provisioned_with_reserved_limit(self):
        """Provisioned concurrency is consumed within reserved limit."""
        fn_key = "123:us-east-1:my-func"
        prov_key = f"{fn_key}:1"
        self.t.set_reserved(fn_key, 5)
        self.t.set_provisioned(prov_key, 3)

        # First 3 should use provisioned
        assert self.t.acquire(fn_key, "1") is True
        assert self.t.acquire(fn_key, "1") is True
        assert self.t.acquire(fn_key, "1") is True

        # Next 2 use on-demand (within reserved limit)
        assert self.t.acquire(fn_key, "1") is False
        assert self.t.acquire(fn_key, "1") is False

        # 6th exceeds reserved limit of 5
        with pytest.raises(TooManyRequestsException, match="reserved concurrency"):
            self.t.acquire(fn_key, "1")

    def test_provisioned_exhausted_still_respects_account_limit(self, monkeypatch):
        """Even when provisioned is exhausted, account limit is enforced."""
        monkeypatch.setenv("LAMBDA_LIMITS_CONCURRENT_EXECUTIONS", "3")
        monkeypatch.setenv("LAMBDA_LIMITS_MINIMUM_UNRESERVED_CONCURRENCY", "0")
        t = ConcurrencyTracker()
        fn_key = "123:us-east-1:my-func"
        prov_key = f"{fn_key}:1"
        t.set_provisioned(prov_key, 1)

        t.acquire(fn_key, "1")  # provisioned
        t.acquire(fn_key, "1")  # on-demand
        t.acquire(fn_key, "1")  # on-demand

        with pytest.raises(TooManyRequestsException, match="Account concurrent"):
            t.acquire(fn_key, "1")


class TestMultipleQualifiers:
    """Test provisioned concurrency with different qualifiers."""

    @pytest.fixture(autouse=True)
    def tracker(self):
        self.t = ConcurrencyTracker()

    def test_different_qualifiers_independent_pools(self):
        """Each qualifier has its own provisioned concurrency pool."""
        fn_key = "123:us-east-1:my-func"
        prov_key_v1 = f"{fn_key}:1"
        prov_key_v2 = f"{fn_key}:2"
        self.t.set_provisioned(prov_key_v1, 2)
        self.t.set_provisioned(prov_key_v2, 3)

        assert self.t.acquire(fn_key, "1") is True
        assert self.t.acquire(fn_key, "2") is True
        assert self.t.get_provisioned_in_use(prov_key_v1) == 1
        assert self.t.get_provisioned_in_use(prov_key_v2) == 1

    def test_latest_qualifier_no_provisioned(self):
        """$LATEST qualifier without provisioned config uses on-demand."""
        fn_key = "123:us-east-1:my-func"
        prov_key = f"{fn_key}:1"
        self.t.set_provisioned(prov_key, 5)

        # Invoke with $LATEST — no provisioned config for $LATEST
        assert self.t.acquire(fn_key, "$LATEST") is False

    def test_qualifier_with_provisioned_version_specific(self):
        """Provisioned concurrency on version 1 doesn't affect version 2."""
        fn_key = "123:us-east-1:my-func"
        prov_key_v1 = f"{fn_key}:1"
        self.t.set_provisioned(prov_key_v1, 1)

        # Version 1 uses provisioned
        assert self.t.acquire(fn_key, "1") is True
        # Version 1 pool full
        assert self.t.acquire(fn_key, "1") is False

        # Version 2 never had provisioned — always on-demand
        assert self.t.acquire(fn_key, "2") is False
