"""Tests for versioned in-memory snapshots in StateManager."""

import threading

import pytest

from robotocore.state.manager import StateManager


@pytest.fixture()
def manager(tmp_path):
    """Create a StateManager with a temp state dir and mock native handlers."""
    mgr = StateManager(state_dir=str(tmp_path))

    # Register a simple native handler for testing
    _store: dict = {"counter": 0}

    def save_fn():
        return dict(_store)

    def load_fn(data):
        _store.clear()
        _store.update(data)

    mgr.register_native_handler("test-service", save_fn, load_fn)

    # Expose the store so tests can mutate state between saves
    mgr._test_store = _store
    return mgr


class TestSaveVersioned:
    def test_first_save_creates_version_1(self, manager):
        result = manager.save_versioned("my-snap")
        assert result["name"] == "my-snap"
        assert result["version"] == 1
        assert result["timestamp"] > 0
        assert result["size"] >= 0
        assert isinstance(result["services"], list)

    def test_second_save_increments_version(self, manager):
        r1 = manager.save_versioned("my-snap")
        assert r1["version"] == 1

        manager._test_store["counter"] = 42
        r2 = manager.save_versioned("my-snap")
        assert r2["version"] == 2
        assert r2["timestamp"] >= r1["timestamp"]

    def test_different_names_independent(self, manager):
        r1 = manager.save_versioned("snap-a")
        r2 = manager.save_versioned("snap-b")
        assert r1["version"] == 1
        assert r2["version"] == 1

    def test_invalid_name_rejected(self, manager):
        with pytest.raises(ValueError, match="empty"):
            manager.save_versioned("")

        with pytest.raises(ValueError, match="path traversal"):
            manager.save_versioned("../evil")

    def test_selective_services(self, manager):
        result = manager.save_versioned("my-snap", services=["test-service"])
        assert "test-service" in result["services"]


class TestLoadVersioned:
    def test_load_latest(self, manager):
        manager._test_store["counter"] = 10
        manager.save_versioned("snap")

        manager._test_store["counter"] = 20
        manager.save_versioned("snap")

        # Mutate state
        manager._test_store["counter"] = 999

        # Load latest (v2) -- should restore counter=20
        result = manager.load_versioned("snap")
        assert result["version"] == 2
        assert manager._test_store["counter"] == 20

    def test_load_specific_version(self, manager):
        manager._test_store["counter"] = 10
        manager.save_versioned("snap")

        manager._test_store["counter"] = 20
        manager.save_versioned("snap")

        # Load v1 -- should restore counter=10
        result = manager.load_versioned("snap", version=1)
        assert result["version"] == 1
        assert manager._test_store["counter"] == 10

    def test_load_nonexistent_name_raises(self, manager):
        with pytest.raises(ValueError, match="not found"):
            manager.load_versioned("nope")

    def test_load_nonexistent_version_raises(self, manager):
        manager.save_versioned("snap")
        with pytest.raises(ValueError, match="version 99 not found"):
            manager.load_versioned("snap", version=99)

    def test_load_returns_metadata(self, manager):
        manager.save_versioned("snap")
        result = manager.load_versioned("snap")
        assert result["name"] == "snap"
        assert result["version"] == 1
        assert "timestamp" in result
        assert "services" in result
        assert "size" in result


class TestListVersioned:
    def test_empty_list(self, manager):
        assert manager.list_versioned() == []

    def test_lists_all_snapshots(self, manager):
        manager.save_versioned("alpha")
        manager.save_versioned("alpha")
        manager.save_versioned("beta")

        result = manager.list_versioned()
        assert len(result) == 2

        alpha = next(s for s in result if s["name"] == "alpha")
        assert alpha["latest"] == 2
        assert alpha["version_count"] == 2
        assert len(alpha["versions"]) == 2

        beta = next(s for s in result if s["name"] == "beta")
        assert beta["latest"] == 1
        assert beta["version_count"] == 1

    def test_version_metadata_correct(self, manager):
        manager.save_versioned("snap")
        result = manager.list_versioned()
        ver = result[0]["versions"][0]
        assert ver["version"] == 1
        assert ver["timestamp"] > 0
        assert isinstance(ver["services"], list)
        assert ver["size"] >= 0


class TestVersionsForSnapshot:
    def test_returns_version_history(self, manager):
        manager.save_versioned("snap")
        manager.save_versioned("snap")
        manager.save_versioned("snap")

        versions = manager.versions_for_snapshot("snap")
        assert len(versions) == 3
        assert [v["version"] for v in versions] == [1, 2, 3]

    def test_nonexistent_raises(self, manager):
        with pytest.raises(ValueError, match="not found"):
            manager.versions_for_snapshot("nope")


class TestDeleteVersioned:
    def test_delete_specific_version(self, manager):
        manager.save_versioned("snap")
        manager.save_versioned("snap")
        manager.save_versioned("snap")

        result = manager.delete_versioned("snap", version=2)
        assert result["deleted_version"] == 2
        assert result["remaining"] == 2

        # Verify version 2 is gone
        versions = manager.versions_for_snapshot("snap")
        assert [v["version"] for v in versions] == [1, 3]

    def test_delete_all_versions(self, manager):
        manager.save_versioned("snap")
        manager.save_versioned("snap")

        result = manager.delete_versioned("snap")
        assert result["deleted_versions"] == 2
        assert result["remaining"] == 0

        # Snapshot should be gone
        with pytest.raises(ValueError, match="not found"):
            manager.versions_for_snapshot("snap")

    def test_delete_last_version_removes_entry(self, manager):
        manager.save_versioned("snap")
        manager.delete_versioned("snap", version=1)

        assert manager.list_versioned() == []

    def test_delete_latest_updates_latest_pointer(self, manager):
        manager.save_versioned("snap")
        manager.save_versioned("snap")
        manager.save_versioned("snap")

        manager.delete_versioned("snap", version=3)

        # Latest should now point to 2
        result = manager.load_versioned("snap")
        assert result["version"] == 2

    def test_delete_nonexistent_name_raises(self, manager):
        with pytest.raises(ValueError, match="not found"):
            manager.delete_versioned("nope")

    def test_delete_nonexistent_version_raises(self, manager):
        manager.save_versioned("snap")
        with pytest.raises(ValueError, match="version 99 not found"):
            manager.delete_versioned("snap", version=99)


class TestThreadSafety:
    def test_concurrent_saves(self, manager):
        """Multiple threads saving to the same snapshot name concurrently."""
        errors = []

        def save_many(n):
            try:
                for _ in range(10):
                    manager.save_versioned("concurrent")
            except Exception as e:
                errors.append(e)

        threads = [threading.Thread(target=save_many, args=(i,)) for i in range(4)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        assert not errors
        versions = manager.versions_for_snapshot("concurrent")
        assert len(versions) == 40  # 4 threads * 10 saves
