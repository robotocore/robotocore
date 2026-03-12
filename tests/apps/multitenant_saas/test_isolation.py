"""
Tests verifying strict data isolation between tenants.
"""

from .models import TenantEntity


class TestDynamoDBIsolation:
    """DynamoDB partition-key isolation tests."""

    def test_tenant_a_data_invisible_to_tenant_b(self, platform, tenant_a, tenant_b):
        """Querying tenant-b must never return tenant-a entities."""
        platform.put_entity(
            TenantEntity(
                tenant_id="tenant-a",
                entity_key="USER#secret",
                entity_type="USER",
                data={"name": "Alice", "ssn": "123-45-6789"},
            )
        )
        platform.put_entity(
            TenantEntity(
                tenant_id="tenant-b",
                entity_key="USER#1",
                entity_type="USER",
                data={"name": "Bob"},
            )
        )

        b_entities = platform.query_entities("tenant-b")
        b_keys = {e.entity_key for e in b_entities}
        assert "USER#secret" not in b_keys
        for e in b_entities:
            assert e.tenant_id == "tenant-b"

    def test_tenant_b_data_invisible_to_tenant_a(self, platform, tenant_a, tenant_b):
        """Querying tenant-a must never return tenant-b entities."""
        platform.put_entity(
            TenantEntity(
                tenant_id="tenant-b",
                entity_key="PROJECT#confidential",
                entity_type="PROJECT",
                data={"budget": 1_000_000},
            )
        )

        a_entities = platform.query_entities("tenant-a")
        a_keys = {e.entity_key for e in a_entities}
        assert "PROJECT#confidential" not in a_keys

    def test_same_entity_key_different_tenants(self, platform, tenant_a, tenant_b):
        """Two tenants can have entities with the same key without collision."""
        platform.put_entity(
            TenantEntity(
                tenant_id="tenant-a",
                entity_key="USER#1",
                entity_type="USER",
                data={"name": "Alice-A"},
            )
        )
        platform.put_entity(
            TenantEntity(
                tenant_id="tenant-b",
                entity_key="USER#1",
                entity_type="USER",
                data={"name": "Alice-B"},
            )
        )

        entity_a = platform.get_entity("tenant-a", "USER#1")
        entity_b = platform.get_entity("tenant-b", "USER#1")
        assert entity_a.data["name"] == "Alice-A"
        assert entity_b.data["name"] == "Alice-B"

    def test_update_one_tenant_doesnt_affect_other(self, platform, tenant_a, tenant_b):
        """Updating tenant-a's entity leaves tenant-b's identical key untouched."""
        for tid in ("tenant-a", "tenant-b"):
            platform.put_entity(
                TenantEntity(
                    tenant_id=tid,
                    entity_key="CONFIG#main",
                    entity_type="CONFIG",
                    data={"version": 1},
                )
            )

        platform.update_entity("tenant-a", "CONFIG#main", {"version": 2})

        ea = platform.get_entity("tenant-a", "CONFIG#main")
        eb = platform.get_entity("tenant-b", "CONFIG#main")
        assert ea.data["version"] == 2
        assert eb.data["version"] == 1


class TestS3Isolation:
    """S3 prefix-based file isolation tests."""

    def test_tenant_a_files_not_in_tenant_b_listing(self, platform, tenant_a, tenant_b):
        """Listing tenant-b's prefix must not show tenant-a files."""
        platform.upload_file("tenant-a", "reports/q1.csv", b"revenue,100")
        platform.upload_file("tenant-b", "reports/q1.csv", b"revenue,200")

        a_files = platform.list_files("tenant-a")
        b_files = platform.list_files("tenant-b")

        for f in a_files:
            assert f.startswith("tenant-a/")
        for f in b_files:
            assert f.startswith("tenant-b/")

    def test_tenant_file_content_isolation(self, platform, tenant_a, tenant_b):
        """Same path, different tenant, different content."""
        platform.upload_file("tenant-a", "data/config.json", b'{"tier":"starter"}')
        platform.upload_file("tenant-b", "data/config.json", b'{"tier":"enterprise"}')

        content_a = platform.download_file("tenant-a", "data/config.json")
        content_b = platform.download_file("tenant-b", "data/config.json")
        assert b"starter" in content_a
        assert b"enterprise" in content_b


class TestConfigIsolation:
    """SSM config isolation tests."""

    def test_config_values_differ_by_tenant(self, platform, tenant_a, tenant_b):
        """Each tenant's SSM config reflects their own plan."""
        config_a = platform.get_tenant_config("tenant-a")
        config_b = platform.get_tenant_config("tenant-b")
        assert config_a.max_users != config_b.max_users
        assert set(config_a.features) != set(config_b.features)

    def test_setting_config_on_one_tenant_doesnt_affect_other(self, platform, tenant_a, tenant_b):
        """Writing a custom param for tenant-a doesn't appear on tenant-b."""
        platform.set_tenant_config_param("tenant-a", "custom_flag", "true")

        val_b = platform.get_tenant_config_param("tenant-b", "custom_flag")
        assert val_b is None

        val_a = platform.get_tenant_config_param("tenant-a", "custom_flag")
        assert val_a == "true"
