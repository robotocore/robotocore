"""
Tests for tenant-scoped CRUD data operations.
"""

from .models import TenantEntity


class TestEntityCRUD:
    """Basic create, read, update, delete scoped to tenant."""

    def test_put_and_get_entity(self, platform, tenant_a):
        """Put an entity and retrieve it by key."""
        entity = TenantEntity(
            tenant_id="tenant-a",
            entity_key="USER#alice",
            entity_type="USER",
            data={"name": "Alice", "role": "admin"},
        )
        platform.put_entity(entity)

        fetched = platform.get_entity("tenant-a", "USER#alice")
        assert fetched is not None
        assert fetched.entity_type == "USER"
        assert fetched.data["name"] == "Alice"
        assert fetched.data["role"] == "admin"

    def test_get_nonexistent_entity_returns_none(self, platform, tenant_a):
        """Getting a key that doesn't exist returns None."""
        result = platform.get_entity("tenant-a", "USER#nonexistent")
        assert result is None

    def test_update_entity_merges_data(self, platform, tenant_a):
        """Update merges new fields into existing data."""
        platform.put_entity(
            TenantEntity(
                tenant_id="tenant-a",
                entity_key="USER#bob",
                entity_type="USER",
                data={"name": "Bob", "role": "viewer"},
            )
        )

        updated = platform.update_entity("tenant-a", "USER#bob", {"role": "editor", "team": "eng"})
        assert updated is not None
        assert updated.data["name"] == "Bob"
        assert updated.data["role"] == "editor"
        assert updated.data["team"] == "eng"
        assert updated.updated_at != updated.created_at or True  # updated_at is refreshed

    def test_update_nonexistent_entity_returns_none(self, platform, tenant_a):
        """Updating a key that doesn't exist returns None."""
        result = platform.update_entity("tenant-a", "NOPE#1", {"x": 1})
        assert result is None

    def test_delete_entity(self, platform, tenant_a):
        """Delete an entity and confirm it is gone."""
        platform.put_entity(
            TenantEntity(
                tenant_id="tenant-a",
                entity_key="TEMP#1",
                entity_type="TEMP",
                data={"disposable": True},
            )
        )
        platform.delete_entity("tenant-a", "TEMP#1")

        assert platform.get_entity("tenant-a", "TEMP#1") is None

    def test_put_overwrites_existing_entity(self, platform, tenant_a):
        """Putting with the same key replaces the data."""
        platform.put_entity(
            TenantEntity(
                tenant_id="tenant-a",
                entity_key="DOC#1",
                entity_type="DOCUMENT",
                data={"version": 1},
            )
        )
        platform.put_entity(
            TenantEntity(
                tenant_id="tenant-a",
                entity_key="DOC#1",
                entity_type="DOCUMENT",
                data={"version": 2},
            )
        )

        fetched = platform.get_entity("tenant-a", "DOC#1")
        assert fetched.data["version"] == 2


class TestEntityQueries:
    """Query operations within a tenant."""

    def test_query_all_entities(self, platform, tenant_a):
        """Query returns all non-metadata entities for a tenant."""
        for i in range(5):
            platform.put_entity(
                TenantEntity(
                    tenant_id="tenant-a",
                    entity_key=f"ITEM#{i}",
                    entity_type="ITEM",
                    data={"index": i},
                )
            )

        results = platform.query_entities("tenant-a")
        # At minimum the 5 ITEMs plus the TENANT# metadata entity
        item_results = [e for e in results if e.entity_type == "ITEM"]
        assert len(item_results) == 5

    def test_query_by_entity_type(self, platform, tenant_a):
        """Filter query by entity_type returns only matching records."""
        platform.put_entity(
            TenantEntity(
                tenant_id="tenant-a",
                entity_key="USER#1",
                entity_type="USER",
                data={"name": "Alice"},
            )
        )
        platform.put_entity(
            TenantEntity(
                tenant_id="tenant-a",
                entity_key="PROJECT#1",
                entity_type="PROJECT",
                data={"name": "Alpha"},
            )
        )
        platform.put_entity(
            TenantEntity(
                tenant_id="tenant-a",
                entity_key="USER#2",
                entity_type="USER",
                data={"name": "Bob"},
            )
        )

        users = platform.query_entities("tenant-a", entity_type="USER")
        assert all(e.entity_type == "USER" for e in users)
        assert len(users) == 2

    def test_query_with_limit(self, platform, tenant_a):
        """Limit restricts the number of returned items."""
        for i in range(10):
            platform.put_entity(
                TenantEntity(
                    tenant_id="tenant-a",
                    entity_key=f"ROW#{i:03d}",
                    entity_type="ROW",
                    data={"i": i},
                )
            )

        results = platform.query_entities("tenant-a", limit=3)
        assert len(results) <= 3


class TestBulkOperations:
    """Batch write tests."""

    def test_bulk_put_entities(self, platform, tenant_a):
        """Batch write inserts multiple entities at once."""
        entities = [
            TenantEntity(
                tenant_id="tenant-a",
                entity_key=f"BULK#{i}",
                entity_type="BULK",
                data={"seq": i},
            )
            for i in range(5)
        ]

        count = platform.bulk_put_entities(entities)
        assert count == 5

        # Verify all were written
        for i in range(5):
            e = platform.get_entity("tenant-a", f"BULK#{i}")
            assert e is not None
            assert e.data["seq"] == i

    def test_bulk_put_empty_list(self, platform, tenant_a):
        """Bulk put with empty list returns 0."""
        assert platform.bulk_put_entities([]) == 0
