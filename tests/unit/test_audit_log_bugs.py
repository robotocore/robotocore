"""Failing tests for bugs found in audit log and resource browser.

Each test documents a specific bug and fails against the current implementation.
"""

import threading
import time
from unittest.mock import MagicMock, patch

from robotocore.audit.log import AuditLog, get_audit_log
from robotocore.resources.browser import (
    _get_backend,
    get_resource_counts,
    get_service_resources,
)


class TestAuditLogNegativeLimit:
    """Bug: recent(limit=-1) returns all entries except the newest.

    When limit is negative, Python slicing `entries[:-1]` removes the last
    element instead of returning an empty list. Since entries are newest-first
    after the reverse, `entries[:-1]` drops the newest entry -- the opposite
    of the intended behavior.

    Expected: negative limit should return an empty list (or raise ValueError).
    Actual: returns all entries except the most recent one.
    """

    def test_negative_limit_should_return_empty(self):
        log = AuditLog(max_size=100)
        log.record(service="s3", operation="Op1")
        log.record(service="s3", operation="Op2")
        log.record(service="s3", operation="Op3")

        result = log.recent(limit=-1)
        # A negative limit should logically return no entries (empty list).
        # But entries[:-1] returns all entries except the last element.
        assert result == [], f"recent(limit=-1) should return empty list, got {len(result)} entries"

    def test_negative_limit_minus_2_drops_two_newest(self):
        log = AuditLog(max_size=100)
        for i in range(5):
            log.record(service="s3", operation=f"Op{i}")

        result = log.recent(limit=-2)
        # Should return empty, but entries[:-2] returns 3 entries
        assert result == [], f"recent(limit=-2) should return empty list, got {len(result)} entries"


class TestAuditLogMaxSizeZero:
    """Bug: AuditLog(max_size=0) ignores the explicit 0 and uses env default.

    Line 17: `size = max_size or int(os.environ.get(..., "1000"))`
    Since 0 is falsy in Python, `0 or 1000` evaluates to 1000.
    An explicit max_size=0 is silently ignored.

    Expected: max_size=0 should create a deque with maxlen=0 (no storage).
    Actual: falls through to default of 1000.
    """

    def test_explicit_zero_max_size_should_store_nothing(self):
        log = AuditLog(max_size=0)
        log.record(service="s3", operation="PutObject")

        entries = log.recent()
        # With max_size=0, the deque should have maxlen=0 and store nothing.
        assert len(entries) == 0, (
            f"AuditLog(max_size=0) should store nothing, but stored {len(entries)} entries"
        )

    def test_explicit_zero_max_size_deque_maxlen(self):
        log = AuditLog(max_size=0)
        # The deque's maxlen should be 0
        assert log._entries.maxlen == 0, (
            f"Expected deque maxlen=0, got maxlen={log._entries.maxlen}"
        )


class TestAuditLogEmptyStringError:
    """Bug: record(error="") silently drops the error field.

    Line 46: `if error:` is falsy for empty string.
    An empty string is a valid (if unusual) error value that should be preserved.

    Expected: error="" should appear in the entry as {"error": ""}.
    Actual: the error key is omitted entirely.
    """

    def test_empty_string_error_should_be_preserved(self):
        log = AuditLog(max_size=100)
        log.record(service="s3", operation="PutObject", error="")

        entry = log.recent()[0]
        # The caller explicitly passed error="", so it should be in the entry.
        assert "error" in entry, "error='' was passed but the 'error' key is missing from the entry"


class TestGetAuditLogSingletonRace:
    """Bug: get_audit_log() singleton initialization is not thread-safe.

    Lines 72-74: Two threads can both see `_log is None` simultaneously,
    each creating a separate AuditLog instance. One instance's entries
    will be lost when the other thread overwrites the global.

    This test demonstrates the race by patching to force interleaving.
    """

    def test_concurrent_init_returns_same_instance(self):
        """Two threads calling get_audit_log() simultaneously should get the same instance."""
        import robotocore.audit.log as log_module

        # Reset the singleton
        original = log_module._log
        log_module._log = None

        instances = []
        barrier = threading.Barrier(2)

        original_init = AuditLog.__init__

        def slow_init(self, *args, **kwargs):
            original_init(self, *args, **kwargs)
            # Force a context switch after init but before assignment
            time.sleep(0.01)

        def get_log():
            barrier.wait()
            instance = get_audit_log()
            instances.append(id(instance))

        try:
            with patch.object(AuditLog, "__init__", slow_init):
                t1 = threading.Thread(target=get_log)
                t2 = threading.Thread(target=get_log)
                t1.start()
                t2.start()
                t1.join(timeout=5)
                t2.join(timeout=5)

            # Both threads should get the same instance
            assert len(instances) == 2
            assert instances[0] == instances[1], (
                f"get_audit_log() returned two different instances: "
                f"{instances[0]} != {instances[1]}"
            )
        finally:
            log_module._log = original


class TestResourceBrowserMultiRegion:
    """Bug: _get_backend only returns one region's backend.

    Resources created in us-west-2 are invisible if us-east-1 also exists,
    because _get_backend returns the us-east-1 backend only.
    get_resource_counts() undercounts when resources exist in multiple regions.
    """

    @patch("robotocore.resources.browser._get_backend")
    @patch("robotocore.services.registry.SERVICE_REGISTRY", {"s3": MagicMock()})
    def test_resource_counts_include_all_regions(self, mock_get_backend):
        """Resources across multiple regions should all be counted.

        _get_backend returns only one region. If the user has 1 bucket in
        us-east-1 and 2 buckets in us-west-2, get_resource_counts reports 1
        instead of 3.
        """
        # Simulate backend returning only us-east-1 (1 bucket)
        # when there should be 3 total across regions
        backend = MagicMock()
        backend.buckets = {"bucket-1": MagicMock()}  # only 1 from us-east-1
        mock_get_backend.return_value = backend

        counts = get_resource_counts()
        # The real total is 3 (1 in us-east-1 + 2 in us-west-2)
        # but _get_backend only sees one region, so we get 1.
        # This test asserts the correct behavior (counting all regions).
        assert counts.get("s3", 0) == 3, (
            f"Expected 3 total S3 buckets across all regions, "
            f"but got {counts.get('s3', 0)} (only one region counted)"
        )

    def test_get_backend_returns_only_one_region(self):
        """_get_backend returns only one region, missing resources in others."""
        from moto.core.base_backend import BackendDict

        useast1_backend = MagicMock()
        useast1_backend.buckets = {"east-bucket": MagicMock()}
        uswest2_backend = MagicMock()
        uswest2_backend.buckets = {"west-bucket-1": MagicMock(), "west-bucket-2": MagicMock()}

        regions = {"us-east-1": useast1_backend, "us-west-2": uswest2_backend}
        account_dict = MagicMock()
        account_dict.__contains__ = lambda self, key: key in regions
        account_dict.__getitem__ = lambda self, key: regions[key]
        account_dict.__iter__ = lambda self: iter(regions)

        backend_dict = MagicMock(spec=BackendDict)
        backend_dict.__contains__ = lambda self, key: key == "123456789012"
        backend_dict.__getitem__ = lambda self, key: account_dict

        with patch("moto.backends.get_backend", return_value=backend_dict):
            result = _get_backend("s3")

        # _get_backend should return backends for ALL regions so callers
        # can aggregate resources. Instead it returns only us-east-1.
        # We expect a list/dict of all region backends, not a single one.
        assert isinstance(result, (list, dict)), (
            f"_get_backend should return all region backends, "
            f"but returned a single backend: {type(result)}"
        )


class TestResourceBrowserListResources:
    """Bug: get_service_resources calls .values() on dicts but iterates
    lists directly. However, it doesn't extract name/arn from list items
    that use different attribute conventions.

    More importantly: get_resource_counts uses len() but silently swallows
    TypeError if the attribute isn't a collection. This means broken
    resource attributes produce silent zero counts instead of errors.
    """

    @patch("robotocore.resources.browser._get_backend")
    def test_non_collection_attribute_silently_ignored(self, mock_get_backend):
        """An attribute that isn't a collection should raise, not be silently ignored."""
        backend = MagicMock()
        # Simulate a backend where 'buckets' is an integer (broken attribute)
        backend.buckets = 42
        mock_get_backend.return_value = backend

        # get_service_resources will try to call .values() on 42, which fails,
        # and the exception is silently swallowed. The caller gets no indication
        # that something is wrong.
        resources = get_service_resources("s3")
        # Should raise an error or return a meaningful indication of the problem,
        # not silently return an empty list
        assert len(resources) > 0 or resources == "error", (
            "Broken resource attribute (int instead of dict) was silently ignored. "
            "Expected an error or non-empty result, got empty list."
        )


class TestAuditLogTimestampPrecision:
    """Bug: timestamp uses time.time() which has platform-dependent precision.

    On some platforms time.time() has only ~15ms resolution.
    Two rapid-fire records could get the same timestamp, making it impossible
    to determine ordering from timestamps alone.

    More practically: the timestamp is a float epoch (seconds), not ISO 8601.
    This makes it hard to use in APIs that return JSON, since float precision
    can be lost during JSON serialization for large epoch values.
    """

    def test_rapid_records_have_distinct_timestamps(self):
        log = AuditLog(max_size=100)
        for _ in range(10):
            log.record(service="s3", operation="PutObject")

        entries = log.recent(limit=10)
        timestamps = [e["timestamp"] for e in entries]
        # Each entry should have a unique timestamp for proper ordering
        assert len(set(timestamps)) == len(timestamps), (
            f"Expected 10 unique timestamps but got {len(set(timestamps))} unique "
            f"out of {len(timestamps)}"
        )


class TestAuditLogDurationNotMeasured:
    """Bug: duration_ms is a caller-provided value, not measured by AuditLog.

    The record() method accepts duration_ms as a parameter with default 0.0.
    If the caller forgets to pass it, the audit log records 0.0 duration.
    There is no mechanism in AuditLog itself to measure actual request duration.

    This means the audit log is only as accurate as its callers -- if any
    caller path forgets to compute and pass duration_ms, the data is wrong.
    """

    def test_duration_defaults_to_zero_not_measured(self):
        log = AuditLog(max_size=100)
        before = time.time()
        time.sleep(0.01)  # Simulate some work
        log.record(service="s3", operation="PutObject")
        after = time.time()

        entry = log.recent()[0]
        elapsed_ms = (after - before) * 1000

        # duration_ms should reflect actual elapsed time, but it's always 0.0
        # when not explicitly provided
        assert entry["duration_ms"] > 0, (
            f"duration_ms should measure actual request time ({elapsed_ms:.1f}ms elapsed) "
            f"but got {entry['duration_ms']}"
        )
