"""
Tests for usage tracking and quota enforcement via CloudWatch.
"""

import pytest

from .app import QuotaExceededError


class TestUsageTracking:
    """Record and retrieve per-tenant usage metrics."""

    def test_record_and_get_api_call_usage(self, platform, tenant_a):
        """Record API call usage and retrieve the sum."""
        platform.record_usage("tenant-a", "ApiCalls", 50)
        platform.record_usage("tenant-a", "ApiCalls", 30)

        total = platform.get_usage("tenant-a", "ApiCalls")
        # The provisioning process also records baseline + per-operation calls,
        # so total should be >= 80.
        assert total >= 80

    def test_record_storage_usage(self, platform, tenant_a):
        """Record storage usage bytes."""
        platform.record_usage("tenant-a", "StorageBytes", 1024 * 1024)
        total = platform.get_usage("tenant-a", "StorageBytes")
        assert total >= 1024 * 1024

    def test_usage_per_tenant_isolation(self, platform, tenant_a, tenant_b):
        """Usage recorded for tenant-a is not visible in tenant-b's metrics."""
        platform.record_usage("tenant-a", "CustomMetric", 999)
        platform.record_usage("tenant-b", "CustomMetric", 1)

        total_a = platform.get_usage("tenant-a", "CustomMetric")
        total_b = platform.get_usage("tenant-b", "CustomMetric")
        assert total_a >= 999
        assert total_b >= 1
        assert total_a != total_b

    def test_platform_wide_metric(self, platform, tenant_a):
        """Platform-wide metrics have no tenant dimension."""
        platform.put_platform_metric("TotalTenants", 5)
        # No assertion on the value -- just verify it doesn't raise.
        # CloudWatch platform metrics are not per-tenant so get_usage won't see them.


class TestQuotaEnforcement:
    """Verify quota checks prevent exceeding plan limits."""

    def test_within_quota_passes(self, platform, tenant_a):
        """check_quota returns True when under the limit."""
        result = platform.check_quota("tenant-a", "ApiCalls", additional=1)
        assert result is True

    def test_exceed_api_call_quota_raises(self, platform, tenant_a):
        """Exceeding the daily API call limit raises QuotaExceededError."""
        # Starter plan: 10,000 API calls/day.  Blast past it.
        platform.record_usage("tenant-a", "ApiCalls", 10_001)

        with pytest.raises(QuotaExceededError):
            platform.check_quota("tenant-a", "ApiCalls", additional=1)

    def test_exceed_storage_quota_raises(self, platform, tenant_a):
        """Exceeding the storage quota raises QuotaExceededError."""
        # Starter plan: 1024 MB = 1,073,741,824 bytes
        over_limit = 1024 * 1024 * 1024 + 1
        platform.record_usage("tenant-a", "StorageBytes", over_limit)

        with pytest.raises(QuotaExceededError):
            platform.check_quota("tenant-a", "StorageBytes", additional=1)

    def test_enterprise_has_higher_quota(self, platform, tenant_b):
        """Enterprise plan allows much more usage before hitting the limit."""
        # Enterprise: 1,000,000 API calls/day
        platform.record_usage("tenant-b", "ApiCalls", 500_000)
        result = platform.check_quota("tenant-b", "ApiCalls", additional=1)
        assert result is True


class TestCrossTenantReporting:
    """Admin aggregate reporting across tenants."""

    def test_aggregate_usage_across_tenants(self, platform, tenant_a, tenant_b):
        """Admin can see usage totals for multiple tenants at once."""
        platform.record_usage("tenant-a", "ApiCalls", 100)
        platform.record_usage("tenant-b", "ApiCalls", 200)

        agg = platform.get_cross_tenant_aggregate("ApiCalls", ["tenant-a", "tenant-b"])
        assert "tenant-a" in agg
        assert "tenant-b" in agg
        assert agg["tenant-a"] >= 100
        assert agg["tenant-b"] >= 200
