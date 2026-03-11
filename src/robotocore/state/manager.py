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

from __future__ import annotations

import base64
import io
import json
import logging
import os
import pickle
import tarfile
import threading
import time
from pathlib import Path
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from robotocore.state.hooks import StateHookRegistry

logger = logging.getLogger(__name__)

# Supported metadata versions
_SUPPORTED_VERSIONS = {"1.0"}

# Type tags for JSON round-trip fidelity
_TAG_TUPLE = "__tuple__"
_TAG_SET = "__set__"
_TAG_BYTES = "__bytes__"
_TAG_INT_KEY_DICT = "__int_key_dict__"


class _TypePreservingEncoder(json.JSONEncoder):
    """JSON encoder that tags non-JSON-native types for faithful round-trip."""

    def default(self, o: Any) -> Any:
        if isinstance(o, bytes):
            return {_TAG_BYTES: base64.b64encode(o).decode("ascii")}
        if isinstance(o, set):
            return {_TAG_SET: sorted(self._encode_value(v) for v in o)}
        if isinstance(o, tuple):
            return {_TAG_TUPLE: [self._encode_value(v) for v in o]}
        return super().default(o)

    def _encode_value(self, v: Any) -> Any:
        """Recursively encode a value, handling special types."""
        if isinstance(v, bytes):
            return {_TAG_BYTES: base64.b64encode(v).decode("ascii")}
        if isinstance(v, set):
            return {_TAG_SET: sorted(self._encode_value(x) for x in v)}
        if isinstance(v, tuple):
            return {_TAG_TUPLE: [self._encode_value(x) for x in v]}
        if isinstance(v, dict):
            return self._encode_dict(v)
        if isinstance(v, list):
            return [self._encode_value(x) for x in v]
        return v

    def _encode_dict(self, d: dict) -> dict:
        """Encode a dict, handling integer keys."""
        has_int_keys = any(isinstance(k, int) for k in d)
        if has_int_keys:
            return {_TAG_INT_KEY_DICT: [[k, self._encode_value(v)] for k, v in d.items()]}
        return {str(k): self._encode_value(v) for k, v in d.items()}

    def encode(self, o: Any) -> str:
        if isinstance(o, dict):
            o = self._encode_dict(o)
        elif isinstance(o, (tuple, set, bytes)):
            o = self._encode_value(o)
        return super().encode(o)


def _type_preserving_decode(obj: Any) -> Any:
    """Object hook for JSON decoder that restores tagged types."""
    if isinstance(obj, dict):
        if _TAG_TUPLE in obj:
            return tuple(_type_preserving_decode(v) for v in obj[_TAG_TUPLE])
        if _TAG_SET in obj:
            return {_type_preserving_decode(v) for v in obj[_TAG_SET]}
        if _TAG_BYTES in obj:
            return base64.b64decode(obj[_TAG_BYTES])
        if _TAG_INT_KEY_DICT in obj:
            return {k: _type_preserving_decode(v) for k, v in obj[_TAG_INT_KEY_DICT]}
        return {k: _type_preserving_decode(v) for k, v in obj.items()}
    if isinstance(obj, list):
        return [_type_preserving_decode(v) for v in obj]
    return obj


def _dumps_native(state: dict) -> str:
    """Serialize native state to JSON with type preservation."""
    return _TypePreservingEncoder(indent=2).encode(state)


def _loads_native(text: str) -> dict:
    """Deserialize native state from JSON with type restoration."""
    raw = json.loads(text)
    return _type_preserving_decode(raw)


class _DisallowedClassError(pickle.UnpicklingError):
    """Raised when a pickle contains a disallowed class (security violation)."""


class _RestrictedUnpickler(pickle.Unpickler):
    """Unpickler that rejects dangerous classes.

    Only allows modules that are part of moto, builtins, and standard
    collection types. Rejects arbitrary code execution via __reduce__.
    """

    _ALLOWED_MODULE_PREFIXES = (
        "moto.",
        "builtins",
        "collections",
        "datetime",
        "decimal",
        "copy_reg",
        "copyreg",
        "_codecs",
    )

    # Dangerous callables that should never appear in pickles
    _BLOCKED_NAMES = frozenset(
        {
            "eval",
            "exec",
            "compile",
            "execfile",
            "input",
            "__import__",
            "getattr",
            "setattr",
            "delattr",
            "globals",
            "locals",
            "vars",
            "open",
            "breakpoint",
        }
    )

    def find_class(self, module: str, name: str) -> type:
        if not any(module.startswith(prefix) for prefix in self._ALLOWED_MODULE_PREFIXES):
            raise _DisallowedClassError(f"Disallowed class in pickle: {module}.{name}")
        if name in self._BLOCKED_NAMES:
            raise _DisallowedClassError(f"Disallowed callable in pickle: {module}.{name}")
        return super().find_class(module, name)


def _safe_tar_extract(tar: tarfile.TarFile, dest: str) -> None:
    """Extract tar archive members after validating against path traversal."""
    dest_path = Path(dest).resolve()
    for member in tar.getmembers():
        member_path = (dest_path / member.name).resolve()
        if not member_path.is_relative_to(dest_path):
            raise ValueError(f"Path traversal detected in tar member: {member.name!r}")
    tar.extractall(dest)  # noqa: S202


def _validate_snapshot_name(name: str) -> None:
    """Validate a snapshot name, rejecting empty strings and path separators."""
    if not name:
        raise ValueError("Snapshot name must not be empty")
    if "/" in name or "\\" in name:
        raise ValueError(f"Invalid snapshot name (path traversal): {name!r}")


class StateManager:
    """Manages save/restore of emulator state."""

    def __init__(
        self,
        state_dir: str | None = None,
        hook_registry: StateHookRegistry | None = None,
    ) -> None:
        self.state_dir = Path(state_dir) if state_dir else None
        self._native_handlers: dict[str, tuple] = {}
        self._last_save_time: float = 0.0
        self._save_lock = threading.Lock()
        self._debounce_interval: float = 1.0  # seconds

        if hook_registry is not None:
            self._hooks = hook_registry
        else:
            from robotocore.state.hooks import state_hooks

            self._hooks = state_hooks

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
        from robotocore.state.hooks import HookType

        if name is not None:
            _validate_snapshot_name(name)

        with self._save_lock:
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

            # Fire before-save hook (may abort by raising)
            hook_ctx = {
                "services": services,
                "snapshot_name": name,
                "snapshot_path": str(save_dir),
            }
            self._hooks.fire(HookType.BEFORE_SAVE, hook_ctx)

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
                result_path = str(archive_path)
                hook_ctx["snapshot_path"] = result_path
                self._hooks.fire(HookType.AFTER_SAVE, hook_ctx)
                return result_path

            # If saving uncompressed, clean up any stale .tar.gz with the same name
            if name:
                stale_archive = save_dir.with_suffix(".tar.gz")
                if stale_archive.exists():
                    stale_archive.unlink()

            self._last_save_time = time.monotonic()
            logger.info("State saved to %s", save_dir)
            result_path = str(save_dir)
            hook_ctx["snapshot_path"] = result_path
            self._hooks.fire(HookType.AFTER_SAVE, hook_ctx)
            return result_path

    def save_debounced(self) -> bool:
        """Save state with debouncing -- at most once per debounce_interval.

        Returns True if save was performed, False if skipped due to debounce.
        """
        now = time.monotonic()
        if now - self._last_save_time < self._debounce_interval:
            return False

        # save() already acquires _save_lock, so just call it directly
        # Double-check after potential wait
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
                    try:
                        meta = json.loads(meta_path.read_text())
                    except (json.JSONDecodeError, OSError):
                        logger.warning("Corrupted metadata.json in snapshot %s", entry.name)
                        snapshots.append({"name": entry.name, "compressed": False})
                        seen_names.add(entry.name)
                        continue
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
        from robotocore.state.hooks import HookType

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

        # Fire before-load hook (may abort by raising)
        hook_ctx = {
            "services": services,
            "snapshot_name": name,
            "snapshot_path": str(load_dir),
        }
        self._hooks.fire(HookType.BEFORE_LOAD, hook_ctx)

        try:
            meta = json.loads(meta_path.read_text())
        except (json.JSONDecodeError, OSError):
            logger.warning("Corrupted metadata.json in %s", load_dir)
            return False

        # Validate metadata version
        version = meta.get("version")
        if version not in _SUPPORTED_VERSIONS:
            raise ValueError(
                f"Unsupported snapshot version {version!r} (supported: {_SUPPORTED_VERSIONS})"
            )

        logger.info(
            "Loading state from %s (saved at %s)",
            load_dir,
            meta.get("timestamp", "unknown"),
        )

        had_failure = False

        # Load Moto backend state
        moto_path = load_dir / "moto_state.pkl"
        if moto_path.exists():
            if not self._load_moto_state(moto_path, services=services):
                had_failure = True

        # Load native provider state
        native_path = load_dir / "native_state.json"
        if native_path.exists():
            if not self._load_native_state(native_path, services=services):
                had_failure = True

        if had_failure:
            logger.warning("State load completed with errors")
            return False

        logger.info("State loaded successfully")
        self._hooks.fire(HookType.AFTER_LOAD, hook_ctx)
        return True

    def reset(self, services: list[str] | None = None) -> None:
        """Reset state to empty. If services is provided, only reset those services."""
        from robotocore.state.hooks import HookType

        hook_ctx: dict = {"services": services}
        self._hooks.fire(HookType.BEFORE_RESET, hook_ctx)

        if services:
            # Selective reset: only reset specified services
            self._reset_moto_state(services=services)
            for service, (_, load_fn) in self._native_handlers.items():
                if service not in services:
                    continue
                try:
                    load_fn({})
                except Exception:
                    logger.debug("Failed to reset native state for %s", service, exc_info=True)
            logger.info("State reset for services: %s", services)
        else:
            # Full reset
            self._reset_moto_state()
            for service, (_, load_fn) in self._native_handlers.items():
                try:
                    load_fn({})
                except Exception:
                    logger.debug("Failed to reset native state for %s", service, exc_info=True)
            logger.info("All state reset")

        self._hooks.fire(HookType.AFTER_RESET, hook_ctx)

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
                _safe_tar_extract(tar, tmpdir)

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

    def _load_compressed(
        self,
        archive_path: Path,
        services: list[str] | None = None,
    ) -> bool:
        """Load state from a compressed tar.gz archive."""
        import tempfile

        from robotocore.state.hooks import HookType

        hook_ctx = {
            "services": services,
            "snapshot_name": archive_path.stem.removesuffix(".tar"),
            "snapshot_path": str(archive_path),
        }

        try:
            self._hooks.fire(HookType.BEFORE_LOAD, hook_ctx)

            with tempfile.TemporaryDirectory() as tmpdir:
                with tarfile.open(archive_path, "r:gz") as tar:
                    _safe_tar_extract(tar, tmpdir)

                tmp_path = Path(tmpdir)
                meta_path = tmp_path / "metadata.json"
                if not meta_path.exists():
                    logger.warning("No metadata.json in compressed snapshot %s", archive_path)
                    return False

                meta = json.loads(meta_path.read_text())

                # Validate version
                version = meta.get("version")
                if version not in _SUPPORTED_VERSIONS:
                    raise ValueError(
                        f"Unsupported snapshot version {version!r} "
                        f"(supported: {_SUPPORTED_VERSIONS})"
                    )

                logger.info(
                    "Loading compressed state from %s (saved at %s)",
                    archive_path,
                    meta.get("timestamp", "unknown"),
                )

                had_failure = False

                moto_path = tmp_path / "moto_state.pkl"
                if moto_path.exists():
                    if not self._load_moto_state(moto_path, services=services):
                        had_failure = True

                native_path = tmp_path / "native_state.json"
                if native_path.exists():
                    if not self._load_native_state(native_path, services=services):
                        had_failure = True

            if had_failure:
                logger.warning("Compressed state load completed with errors")
                return False

            logger.info("Compressed state loaded successfully from %s", archive_path)
            self._hooks.fire(HookType.AFTER_LOAD, hook_ctx)
            return True
        except ValueError:
            raise
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
            # Remove corrupt/partial pickle so load doesn't choke on it
            try:
                path.unlink(missing_ok=True)
            except OSError:
                pass

    def _load_moto_state(self, path: Path, services: list[str] | None = None) -> bool:
        """Restore Moto backends from pickle. Returns True on success.

        Raises pickle.UnpicklingError if the pickle contains disallowed classes
        (security violation). Other errors return False.
        """
        try:
            from moto.backends import get_backend

            with open(path, "rb") as f:
                state = _RestrictedUnpickler(f).load()

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

            return True
        except _DisallowedClassError:
            # Security violations must propagate -- don't silently swallow them
            raise
        except Exception:
            logger.warning("Failed to load Moto state", exc_info=True)
            return False

    def _save_native_state(self, path: Path, services: list[str] | None = None) -> None:
        """Save native provider state as type-preserving JSON.

        When saving selectively, merges with existing saved state so that
        previously saved services are not destroyed.
        """
        # Load existing state to merge with (for selective saves)
        existing_state: dict = {}
        if services and path.exists():
            try:
                existing_state = _loads_native(path.read_text())
            except (json.JSONDecodeError, OSError):
                pass

        state = dict(existing_state)
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

        path.write_text(_dumps_native(state))

    def _load_native_state(self, path: Path, services: list[str] | None = None) -> bool:
        """Load native provider state from JSON. Returns True on success."""
        try:
            state = _loads_native(path.read_text())
        except (json.JSONDecodeError, OSError):
            logger.warning("Failed to load native state from %s", path)
            return False

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
        return True

    def _reset_moto_state(self, services: list[str] | None = None) -> None:
        """Reset Moto backends. If services is provided, only reset those."""
        try:
            if not services:
                import moto.core.models as moto_models

                if hasattr(moto_models, "base_decorator"):
                    moto_models.base_decorator.reset()
                    return

            from moto.backends import get_backend

            target_services = (
                self._list_moto_services(services) if services else self._list_moto_services()
            )
            for service_name in target_services:
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
