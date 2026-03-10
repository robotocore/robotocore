"""State persistence manager -- save and restore emulator state across restarts.

Supports saving all Moto backend state plus native provider state to disk,
and restoring it on startup. This enables "Cloud Pods"-like functionality
where you can snapshot and share emulator state.

Configuration via environment variables:
    ROBOTOCORE_STATE_DIR=/path/to/state         Save/load state directory
    ROBOTOCORE_PERSIST=1                        Enable auto-save on shutdown
    PERSISTENCE=1                               Enable auto-save after mutations
    ROBOTOCORE_RESTORE_SNAPSHOT=<name|latest>    Auto-restore snapshot on startup
"""

import io
import json
import logging
import os
import pickle
import tarfile
import threading
import time
from pathlib import Path

logger = logging.getLogger(__name__)


class StateManager:
    """Manages save/restore of emulator state."""

    def __init__(self, state_dir: str | None = None) -> None:
        self.state_dir = Path(state_dir) if state_dir else None
        self._native_handlers: dict[str, tuple] = {}
        self._last_save_time: float = 0.0
        self._save_lock = threading.Lock()
        self._debounce_interval: float = 1.0  # seconds

    def register_native_handler(
        self,
        service: str,
        save_fn,
        load_fn,
    ) -> None:
        """Register a native provider's save/load functions."""
        self._native_handlers[service] = (save_fn, load_fn)

    def save(
        self,
        path: str | Path | None = None,
        name: str | None = None,
        services: list[str] | None = None,
        compress: bool = False,
    ) -> str:
        """Save all state to disk. Returns the path used.

        Args:
            path: Directory to save to (defaults to state_dir).
            name: Named snapshot — saves under state_dir/snapshots/{name}/.
            services: If provided, only save these services.
            compress: If True, create a .tar.gz archive instead of a directory.
        """
        if name:
            base = Path(path) if path else self.state_dir
            if not base:
                raise ValueError("No state directory configured")
            save_dir = base / "snapshots" / name
            # Prevent path traversal
            if not save_dir.resolve().is_relative_to((base / "snapshots").resolve()):
                raise ValueError(f"Invalid snapshot name (path traversal): {name!r}")
        else:
            save_dir = Path(path) if path else self.state_dir
        if not save_dir:
            raise ValueError("No state directory configured")

        save_dir.mkdir(parents=True, exist_ok=True)
        timestamp = time.strftime("%Y%m%dT%H%M%S")

        # Save Moto backend state
        moto_path = save_dir / "moto_state.pkl"
        self._save_moto_state(moto_path, services=services)

        # Save native provider state
        native_path = save_dir / "native_state.json"
        self._save_native_state(native_path, services=services)

        # Save metadata
        meta = {
            "version": "1.0",
            "timestamp": timestamp,
            "saved_at": time.time(),
            "moto_services": self._list_moto_services(services),
            "native_services": (
                [s for s in self._native_handlers if s in services]
                if services
                else list(self._native_handlers.keys())
            ),
            "name": name,
            "compressed": compress,
        }
        meta_path = save_dir / "metadata.json"
        meta_path.write_text(json.dumps(meta, indent=2))

        if compress:
            archive_path = save_dir.with_suffix(".tar.gz")
            with tarfile.open(archive_path, "w:gz") as tar:
                for file_path in save_dir.iterdir():
                    tar.add(file_path, arcname=file_path.name)
            # Clean up the uncompressed directory
            import shutil

            shutil.rmtree(save_dir)
            self._last_save_time = time.monotonic()
            logger.info("State saved (compressed) to %s", archive_path)
            return str(archive_path)

        self._last_save_time = time.monotonic()
        logger.info("State saved to %s", save_dir)
        return str(save_dir)

    def save_debounced(self) -> bool:
        """Save state with debouncing -- at most once per debounce_interval.

        Returns True if save was performed, False if skipped due to debounce.
        """
        now = time.monotonic()
        if now - self._last_save_time < self._debounce_interval:
            return False

        with self._save_lock:
            # Double-check after acquiring lock
            if time.monotonic() - self._last_save_time < self._debounce_interval:
                return False
            self.save()
            return True

    def list_snapshots(self) -> list[dict]:
        """List all named snapshots (both directory and compressed formats)."""
        if not self.state_dir:
            return []
        snap_dir = self.state_dir / "snapshots"
        if not snap_dir.exists():
            return []
        snapshots = []
        seen_names: set[str] = set()
        for entry in sorted(snap_dir.iterdir()):
            if entry.is_dir():
                meta_path = entry / "metadata.json"
                if meta_path.exists():
                    meta = json.loads(meta_path.read_text())
                    snapshots.append(
                        {
                            "name": entry.name,
                            "timestamp": meta.get("timestamp"),
                            "saved_at": meta.get("saved_at"),
                            "services": meta.get("moto_services", []),
                            "native_services": meta.get("native_services", []),
                            "compressed": False,
                        }
                    )
                else:
                    snapshots.append({"name": entry.name, "compressed": False})
                seen_names.add(entry.name)
            elif entry.name.endswith(".tar.gz"):
                snap_name = entry.name.removesuffix(".tar.gz")
                if snap_name in seen_names:
                    continue
                # Read metadata from the archive
                meta_info = self._read_compressed_metadata(entry)
                info: dict = {"name": snap_name, "compressed": True}
                if meta_info:
                    info["timestamp"] = meta_info.get("timestamp")
                    info["saved_at"] = meta_info.get("saved_at")
                    info["services"] = meta_info.get("moto_services", [])
                    info["native_services"] = meta_info.get("native_services", [])
                snapshots.append(info)
                seen_names.add(snap_name)
        return snapshots

    def load(
        self,
        path: str | Path | None = None,
        name: str | None = None,
        services: list[str] | None = None,
    ) -> bool:
        """Load state from disk. Returns True if successful.

        Args:
            path: Directory to load from (defaults to state_dir).
            name: Named snapshot — loads from state_dir/snapshots/{name}/.
                  Use "latest" to load the most recently saved snapshot.
            services: If provided, only load these services.
        """
        if name:
            base = Path(path) if path else self.state_dir
            if not base:
                return False

            # Handle "latest" — find the most recently saved snapshot
            if name == "latest":
                resolved_name = self._find_latest_snapshot()
                if not resolved_name:
                    logger.warning("No snapshots found for 'latest' restore")
                    return False
                name = resolved_name
                logger.info("Resolved 'latest' snapshot to '%s'", name)

            load_dir = base / "snapshots" / name
            # Prevent path traversal
            if not load_dir.resolve().is_relative_to((base / "snapshots").resolve()):
                raise ValueError(f"Invalid snapshot name (path traversal): {name!r}")

            # Check for compressed format
            archive_path = load_dir.with_suffix(".tar.gz")
            if not load_dir.exists() and archive_path.exists():
                return self._load_compressed(archive_path, services=services)
        else:
            load_dir = Path(path) if path else self.state_dir
        if not load_dir or not load_dir.exists():
            logger.debug("No state directory found at %s", load_dir)
            return False

        meta_path = load_dir / "metadata.json"
        if not meta_path.exists():
            logger.warning("No metadata.json in %s -- skipping load", load_dir)
            return False

        meta = json.loads(meta_path.read_text())
        logger.info(
            "Loading state from %s (saved at %s)",
            load_dir,
            meta.get("timestamp", "unknown"),
        )

        # Load Moto backend state
        moto_path = load_dir / "moto_state.pkl"
        if moto_path.exists():
            self._load_moto_state(moto_path, services=services)

        # Load native provider state
        native_path = load_dir / "native_state.json"
        if native_path.exists():
            self._load_native_state(native_path, services=services)

        logger.info("State loaded successfully")
        return True

    def reset(self) -> None:
        """Reset all state to empty."""
        self._reset_moto_state()
        for service, (_, load_fn) in self._native_handlers.items():
            try:
                load_fn({})
            except Exception:
                logger.debug("Failed to reset native state for %s", service, exc_info=True)
        logger.info("All state reset")

    def export_json(self) -> dict:
        """Export native provider state as a JSON-serializable dict."""
        state: dict = {
            "version": "1.0",
            "exported_at": time.time(),
            "native_state": {},
        }
        for service, (save_fn, _) in self._native_handlers.items():
            try:
                state["native_state"][service] = save_fn()
            except Exception:
                logger.debug("Could not export state for %s", service, exc_info=True)
        return state

    def import_json(self, data: dict) -> None:
        """Import native provider state from a JSON dict."""
        native_state = data.get("native_state", {})
        for service, (_, load_fn) in self._native_handlers.items():
            if service in native_state:
                try:
                    load_fn(native_state[service])
                except Exception:
                    logger.debug(
                        "Could not import state for %s",
                        service,
                        exc_info=True,
                    )
        logger.info("State imported from JSON")

    def export_snapshot_bytes(self, name: str | None = None) -> bytes:
        """Export a snapshot as a compressed tar.gz byte stream.

        If *name* is given, exports that named snapshot. Otherwise exports the
        current live state by performing a temporary save.
        """
        import tempfile

        if name:
            # Check if the snapshot already exists as a compressed archive
            if self.state_dir:
                archive_path = self.state_dir / "snapshots" / f"{name}.tar.gz"
                if archive_path.exists():
                    return archive_path.read_bytes()

            # Check if it exists as a directory, and compress it on the fly
            if self.state_dir:
                snap_dir = self.state_dir / "snapshots" / name
                if snap_dir.exists():
                    return self._compress_directory(snap_dir)

            raise ValueError(f"Snapshot '{name}' not found")

        # No name — save current state to a temp dir and compress it
        with tempfile.TemporaryDirectory() as tmpdir:
            self.save(path=tmpdir)
            return self._compress_directory(Path(tmpdir))

    def import_snapshot_bytes(
        self,
        data: bytes,
        name: str | None = None,
        load_after_import: bool = True,
        services: list[str] | None = None,
    ) -> str:
        """Import a snapshot from compressed tar.gz bytes.

        Args:
            data: The tar.gz archive bytes.
            name: Name to assign to the imported snapshot. If None, uses the
                  name from the archive metadata (or 'imported-<timestamp>').
            load_after_import: If True, immediately load the snapshot state.
            services: If provided, only load these services after import.

        Returns:
            The name of the imported snapshot.
        """
        if not self.state_dir:
            raise ValueError("No state directory configured")

        # Extract to a temp dir first to read metadata
        import tempfile

        with tempfile.TemporaryDirectory() as tmpdir:
            with tarfile.open(fileobj=io.BytesIO(data), mode="r:gz") as tar:
                tar.extractall(tmpdir)  # noqa: S202

            tmp_path = Path(tmpdir)
            meta_path = tmp_path / "metadata.json"
            if meta_path.exists():
                meta = json.loads(meta_path.read_text())
                resolved_name = name or meta.get("name") or f"imported-{int(time.time())}"
            else:
                resolved_name = name or f"imported-{int(time.time())}"

            # Move to snapshots directory
            snap_dir = self.state_dir / "snapshots" / resolved_name
            snap_dir.mkdir(parents=True, exist_ok=True)
            for item in tmp_path.iterdir():
                dest = snap_dir / item.name
                if dest.exists():
                    if dest.is_dir():
                        import shutil

                        shutil.rmtree(dest)
                    else:
                        dest.unlink()
                item.rename(dest)

        logger.info("Imported snapshot as '%s'", resolved_name)

        if load_after_import:
            self.load(name=resolved_name, services=services)

        return resolved_name

    def restore_on_startup(self) -> bool:
        """Check ROBOTOCORE_RESTORE_SNAPSHOT env var and restore if set.

        Returns True if a snapshot was restored.
        """
        snapshot_name = os.environ.get("ROBOTOCORE_RESTORE_SNAPSHOT")
        if not snapshot_name:
            return False

        logger.info("Auto-restoring snapshot: %s", snapshot_name)
        success = self.load(name=snapshot_name)
        if success:
            logger.info("Successfully restored snapshot '%s' on startup", snapshot_name)
        else:
            logger.warning("Failed to restore snapshot '%s' on startup", snapshot_name)
        return success

    def _find_latest_snapshot(self) -> str | None:
        """Find the most recently saved snapshot by saved_at timestamp."""
        snapshots = self.list_snapshots()
        if not snapshots:
            return None
        # Sort by saved_at (most recent first), falling back to name
        snapshots_with_time = [s for s in snapshots if s.get("saved_at")]
        if snapshots_with_time:
            snapshots_with_time.sort(key=lambda s: s["saved_at"], reverse=True)
            return snapshots_with_time[0]["name"]
        # No timestamps — just return last alphabetically
        return snapshots[-1]["name"]

    def _load_compressed(self, archive_path: Path, services: list[str] | None = None) -> bool:
        """Load state from a compressed tar.gz archive."""
        import tempfile

        try:
            with tempfile.TemporaryDirectory() as tmpdir:
                with tarfile.open(archive_path, "r:gz") as tar:
                    tar.extractall(tmpdir)  # noqa: S202

                tmp_path = Path(tmpdir)
                meta_path = tmp_path / "metadata.json"
                if not meta_path.exists():
                    logger.warning("No metadata.json in compressed snapshot %s", archive_path)
                    return False

                meta = json.loads(meta_path.read_text())
                logger.info(
                    "Loading compressed state from %s (saved at %s)",
                    archive_path,
                    meta.get("timestamp", "unknown"),
                )

                moto_path = tmp_path / "moto_state.pkl"
                if moto_path.exists():
                    self._load_moto_state(moto_path, services=services)

                native_path = tmp_path / "native_state.json"
                if native_path.exists():
                    self._load_native_state(native_path, services=services)

            logger.info("Compressed state loaded successfully from %s", archive_path)
            return True
        except Exception:
            logger.warning("Failed to load compressed state from %s", archive_path, exc_info=True)
            return False

    def _compress_directory(self, directory: Path) -> bytes:
        """Compress a directory into tar.gz bytes."""
        buf = io.BytesIO()
        with tarfile.open(fileobj=buf, mode="w:gz") as tar:
            for file_path in directory.iterdir():
                tar.add(file_path, arcname=file_path.name)
        return buf.getvalue()

    def _read_compressed_metadata(self, archive_path: Path) -> dict | None:
        """Read metadata.json from a compressed tar.gz archive without full extraction."""
        try:
            with tarfile.open(archive_path, "r:gz") as tar:
                try:
                    member = tar.getmember("metadata.json")
                    f = tar.extractfile(member)
                    if f:
                        return json.loads(f.read())
                except KeyError:
                    pass
        except Exception:
            logger.debug("Could not read metadata from %s", archive_path, exc_info=True)
        return None

    def _save_moto_state(self, path: Path, services: list[str] | None = None) -> None:
        """Pickle all Moto backends."""
        try:
            from moto.backends import get_backend

            state = {}
            for service_name in self._list_moto_services(services):
                try:
                    backend_dict = get_backend(service_name)
                    # Serialize the backend dict (account -> region -> backend)
                    service_state = {}
                    for account_id in list(backend_dict.keys()):
                        account_state = {}
                        for region in list(backend_dict[account_id].keys()):
                            backend = backend_dict[account_id][region]
                            account_state[region] = backend
                        service_state[account_id] = account_state
                    state[service_name] = service_state
                except Exception:
                    logger.debug(
                        "Could not save Moto state for %s",
                        service_name,
                        exc_info=True,
                    )

            with open(path, "wb") as f:
                pickle.dump(state, f, protocol=pickle.HIGHEST_PROTOCOL)

        except Exception:
            logger.warning("Failed to save Moto state", exc_info=True)

    def _load_moto_state(self, path: Path, services: list[str] | None = None) -> None:
        """Restore Moto backends from pickle."""
        try:
            from moto.backends import get_backend

            with open(path, "rb") as f:
                state = pickle.load(f)  # noqa: S301

            for service_name, service_state in state.items():
                if services and service_name not in services:
                    continue
                try:
                    backend_dict = get_backend(service_name)
                    for account_id, account_state in service_state.items():
                        for region, backend in account_state.items():
                            backend_dict[account_id][region] = backend
                except Exception:
                    logger.debug(
                        "Could not load Moto state for %s",
                        service_name,
                        exc_info=True,
                    )

        except Exception:
            logger.warning("Failed to load Moto state", exc_info=True)

    def _save_native_state(self, path: Path, services: list[str] | None = None) -> None:
        """Save native provider state as JSON."""
        state = {}
        for service, (save_fn, _) in self._native_handlers.items():
            if services and service not in services:
                continue
            try:
                state[service] = save_fn()
            except Exception:
                logger.debug(
                    "Could not save native state for %s",
                    service,
                    exc_info=True,
                )

        path.write_text(json.dumps(state, indent=2, default=str))

    def _load_native_state(self, path: Path, services: list[str] | None = None) -> None:
        """Load native provider state from JSON."""
        state = json.loads(path.read_text())
        for service, (_, load_fn) in self._native_handlers.items():
            if services and service not in services:
                continue
            if service in state:
                try:
                    load_fn(state[service])
                except Exception:
                    logger.debug(
                        "Could not load native state for %s",
                        service,
                        exc_info=True,
                    )

    def _reset_moto_state(self) -> None:
        """Reset all Moto backends."""
        try:
            import moto.core.models as moto_models

            if hasattr(moto_models, "base_decorator"):
                moto_models.base_decorator.reset()
            else:
                from moto.backends import get_backend

                for service_name in self._list_moto_services():
                    try:
                        backend_dict = get_backend(service_name)
                        for account_id in list(backend_dict.keys()):
                            for region in list(backend_dict[account_id].keys()):
                                backend_dict[account_id][region].reset()
                    except Exception:
                        pass
        except Exception:
            logger.debug("Failed to reset Moto state", exc_info=True)

    def _list_moto_services(self, services: list[str] | None = None) -> list[str]:
        """List Moto services that have backends with data."""
        from robotocore.services.registry import SERVICE_REGISTRY

        all_services = sorted(SERVICE_REGISTRY.keys())
        if services:
            return [s for s in all_services if s in services]
        return all_services


# Singleton
_manager: StateManager | None = None


def get_state_manager() -> StateManager:
    """Get the global StateManager instance."""
    global _manager
    if _manager is None:
        state_dir = os.environ.get("ROBOTOCORE_STATE_DIR")
        _manager = StateManager(state_dir=state_dir)
    return _manager
