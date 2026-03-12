"""
Tests for per-tenant configuration via SSM Parameter Store.
"""


class TestTenantConfig:
    """Read and write tenant configuration."""

    def test_get_config_reflects_plan(self, platform, tenant_a):
        """Config derived from the starter plan has correct limits."""
        config = platform.get_tenant_config("tenant-a")
        assert config.tenant_id == "tenant-a"
        assert config.max_users == 10
        assert config.storage_quota_mb == 1024
        assert "billing" in config.features
        assert "reports" in config.features

    def test_enterprise_plan_config(self, platform, tenant_b):
        """Enterprise plan has higher limits and more features."""
        config = platform.get_tenant_config("tenant-b")
        assert config.max_users == 500
        assert config.storage_quota_mb == 102_400
        assert "sso" in config.features
        assert "audit" in config.features
        assert "custom_branding" in config.features

    def test_set_and_get_config_param(self, platform, tenant_a):
        """Write a custom config param and read it back."""
        platform.set_tenant_config_param("tenant-a", "dark_mode", "enabled")
        val = platform.get_tenant_config_param("tenant-a", "dark_mode")
        assert val == "enabled"

    def test_get_missing_config_param_returns_none(self, platform, tenant_a):
        """Reading a non-existent param returns None."""
        val = platform.get_tenant_config_param("tenant-a", "nonexistent_key")
        assert val is None

    def test_overwrite_config_param(self, platform, tenant_a):
        """Overwriting a param updates the value."""
        platform.set_tenant_config_param("tenant-a", "theme", "light")
        platform.set_tenant_config_param("tenant-a", "theme", "dark")
        val = platform.get_tenant_config_param("tenant-a", "theme")
        assert val == "dark"


class TestFeatureFlags:
    """Feature flag operations per tenant."""

    def test_starter_features_subset_of_enterprise(self, platform, tenant_a, tenant_b):
        """Starter plan features are a strict subset of enterprise features."""
        config_a = platform.get_tenant_config("tenant-a")
        config_b = platform.get_tenant_config("tenant-b")
        assert set(config_a.features) < set(config_b.features)

    def test_feature_flag_update(self, platform, tenant_a):
        """Overwriting features_enabled changes the feature set."""
        platform.set_tenant_config_param("tenant-a", "features_enabled", "billing,reports,sso")
        config = platform.get_tenant_config("tenant-a")
        assert "sso" in config.features
        assert "billing" in config.features

    def test_rate_limits_per_plan(self, platform, tenant_a, tenant_b):
        """Rate limits differ between starter and enterprise."""
        config_a = platform.get_tenant_config("tenant-a")
        config_b = platform.get_tenant_config("tenant-b")
        assert config_a.rate_limits["max_api_calls_per_day"] == 10_000
        assert config_b.rate_limits["max_api_calls_per_day"] == 1_000_000


class TestPlanMigration:
    """Change a tenant's plan tier and verify effects."""

    def test_upgrade_plan(self, platform, tenant_a):
        """Upgrading from starter to pro updates config."""
        platform.change_tenant_plan("tenant-a", "pro")

        config = platform.get_tenant_config("tenant-a")
        assert config.max_users == 50
        assert config.storage_quota_mb == 10_240
        assert "api_access" in config.features
        assert "sso" in config.features

    def test_downgrade_plan(self, platform, tenant_b):
        """Downgrading from enterprise to starter reduces limits."""
        platform.change_tenant_plan("tenant-b", "starter")

        config = platform.get_tenant_config("tenant-b")
        assert config.max_users == 10
        assert "audit" not in config.features

    def test_plan_change_updates_tenant_metadata(self, platform, tenant_a):
        """Plan change is reflected in the tenant's DynamoDB metadata."""
        platform.change_tenant_plan("tenant-a", "enterprise")

        tenant = platform.get_tenant("tenant-a")
        assert tenant.plan == "enterprise"

    def test_plan_change_audit_logged(self, platform, tenant_a):
        """Plan changes appear in the audit log with old and new plan."""
        platform.change_tenant_plan("tenant-a", "pro")

        log = platform.get_audit_log()
        change_entries = [e for e in log if e["action"] == "change_plan"]
        assert len(change_entries) >= 1
        entry = change_entries[-1]
        assert entry["old_plan"] == "starter"
        assert entry["new_plan"] == "pro"

    def test_config_hierarchical_paths(self, platform, tenant_a):
        """Nested SSM paths work for hierarchical config."""
        platform.set_tenant_config_param(
            "tenant-a", "integrations/slack/webhook_url", "https://hooks.slack.example.com/x"
        )
        val = platform.get_tenant_config_param("tenant-a", "integrations/slack/webhook_url")
        assert val == "https://hooks.slack.example.com/x"
