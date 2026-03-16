"""State persistence manager -- save and restore emulator state across restarts.

Supports saving all Moto backend state plus native provider state to disk,
and restoring it on startup. This enables "Cloud Pods"-like functionality
where you can snapshot and share emulator state.

Configuration via environment variables:
    ROBOTOCORE_STATE_DIR=/path/to/state         Save/load state directory
    ROBOTOCORE_PERSIST=1                        Enable auto-save on shutdown
    PERSISTENCE=1                               Enable auto-save after mutations
    ROBOTOCORE_RESTORE_SNAPSHOT=<name|latest>    Auto-restore snapshot on startup
    SNAPSHOT_SAVE_STRATEGY=<strategy>            on_shutdown|on_request|scheduled|manual
    SNAPSHOT_LOAD_STRATEGY=<strategy>            Load strategy (on_startup|on_request|manual)
    SNAPSHOT_FLUSH_INTERVAL=<seconds>            Interval for scheduled saves (default 15)
"""

from __future__ import annotations

import base64
import copy
import enum
import io
import json
import logging
import os
import pickle
import sys
import tarfile
import threading
import time
from pathlib import Path
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from robotocore.state.hooks import StateHookRegistry

from robotocore.state.change_tracker import ChangeTracker

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
        "re",
        "robotocore.state.manager",
        # Standard library types that Moto backends store on model objects
        "ipaddress",  # EC2/VPC: IPv4Address, IPv4Network, IPv6Network
        "enum",  # Various status enums
        "uuid",  # Resource identifiers
        "pathlib",  # Path objects in some backends
        "json",  # JSONDecodeError etc.
        "zoneinfo",  # Timezone-aware datetimes
        "functools",  # partial, cached_property
        "abc",  # ABCMeta
        "typing",  # Type annotations stored as values
    )

    # Dangerous callables that should never appear in pickles.
    # Note: getattr/setattr/delattr are used legitimately by pickle to
    # reconstruct objects (e.g. Moto's EC2 backend uses __reduce__ with
    # getattr), so they are NOT blocked here.
    _BLOCKED_NAMES = frozenset(
        {
            "eval",
            "exec",
            "compile",
            "execfile",
            "input",
            "__import__",
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


# ---------------------------------------------------------------------------
# Thread-safe pickling: strip/restore threading primitives that Moto stores
# on backend objects (Lock, RLock, Condition, Event, Semaphore, etc.)
# ---------------------------------------------------------------------------


# Sentinel class stored in the pickle stream in place of unpicklable threading objs
class _ThreadingSentinel:
    """Placeholder for a threading primitive stripped during pickling."""

    __slots__ = ("type_name",)

    def __init__(self, type_name: str) -> None:
        self.type_name = type_name

    def __repr__(self) -> str:
        return f"_ThreadingSentinel({self.type_name!r})"


# Map from sentinel type_name -> factory to recreate the object on load
_THREADING_FACTORIES: dict[str, Any] = {
    "Lock": threading.Lock,
    "RLock": threading.RLock,
    "Condition": threading.Condition,
    "Event": threading.Event,
    "Semaphore": threading.Semaphore,
    "BoundedSemaphore": threading.BoundedSemaphore,
    "Barrier": threading.Barrier,
}

# Types we need to intercept (resolved once at import time)
_LOCK_TYPE = type(threading.Lock())
_RLOCK_TYPE = type(threading.RLock())
_THREADING_TYPES: tuple[type, ...] = (
    _LOCK_TYPE,
    _RLOCK_TYPE,
    threading.Condition,
    threading.Event,
    threading.Semaphore,
    threading.BoundedSemaphore,
    threading.Barrier,
)

# Map from actual type -> sentinel type_name
_TYPE_TO_NAME: dict[type, str] = {
    _LOCK_TYPE: "Lock",
    _RLOCK_TYPE: "RLock",
    threading.Condition: "Condition",
    threading.Event: "Event",
    threading.Semaphore: "Semaphore",
    threading.BoundedSemaphore: "BoundedSemaphore",
    threading.Barrier: "Barrier",
}


class _ThreadSafePickler(pickle.Pickler):
    """Pickler that replaces threading primitives with serializable sentinels.

    Moto backend objects (SQS queues, ECR registries, StepFunctions executions)
    store Lock/RLock/Condition objects that cannot be pickled. This pickler
    intercepts them via the ``reducer_override`` hook and emits a
    ``_ThreadingSentinel`` placeholder instead.
    """

    def reducer_override(self, obj: Any) -> Any:
        if isinstance(obj, _THREADING_TYPES):
            type_name = _TYPE_TO_NAME.get(type(obj), "Lock")
            return (  # noqa: E501 – pickle __reduce__ tuple
                _ThreadingSentinel,
                (type_name,),
            )
        # Returning NotImplemented tells pickle to use the default mechanism
        return NotImplemented


def _safe_pickle_dumps(obj: Any) -> bytes:
    """Pickle *obj*, replacing unpicklable threading primitives with sentinels."""
    buf = io.BytesIO()
    _ThreadSafePickler(buf, protocol=pickle.HIGHEST_PROTOCOL).dump(obj)
    return buf.getvalue()


def _restore_threading_objects(obj: Any, _seen: set[int] | None = None) -> Any:
    """Walk a deserialized object graph and replace sentinels with real locks.

    Modifies containers in-place (dicts, lists, object __dict__) and returns
    *obj* for convenience.
    """
    if _seen is None:
        _seen = set()
    obj_id = id(obj)
    if obj_id in _seen:
        return obj
    _seen.add(obj_id)

    if isinstance(obj, _ThreadingSentinel):
        factory = _THREADING_FACTORIES.get(obj.type_name, threading.Lock)
        return factory()

    if isinstance(obj, dict):
        for key in list(obj.keys()):
            val = obj[key]
            if isinstance(val, _ThreadingSentinel):
                factory = _THREADING_FACTORIES.get(val.type_name, threading.Lock)
                obj[key] = factory()
            else:
                _restore_threading_objects(val, _seen)
        return obj

    if isinstance(obj, list):
        for i, val in enumerate(obj):
            if isinstance(val, _ThreadingSentinel):
                factory = _THREADING_FACTORIES.get(val.type_name, threading.Lock)
                obj[i] = factory()
            else:
                _restore_threading_objects(val, _seen)
        return obj

    # Walk instance __dict__ for arbitrary objects
    if hasattr(obj, "__dict__"):
        d = obj.__dict__
        for key in list(d.keys()):
            val = d[key]
            if isinstance(val, _ThreadingSentinel):
                factory = _THREADING_FACTORIES.get(val.type_name, threading.Lock)
                d[key] = factory()
            else:
                _restore_threading_objects(val, _seen)

    return obj


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


class SnapshotSaveStrategy(enum.Enum):
    """When to automatically save state."""

    ON_SHUTDOWN = "on_shutdown"
    ON_REQUEST = "on_request"
    SCHEDULED = "scheduled"
    MANUAL = "manual"


class SnapshotLoadStrategy(enum.Enum):
    """When to automatically load state."""

    ON_STARTUP = "on_startup"
    ON_REQUEST = "on_request"
    MANUAL = "manual"


class InvalidStrategyError(ValueError):
    """Raised when an invalid strategy name is given."""

    pass


def _parse_save_strategy(value: str) -> SnapshotSaveStrategy:
    """Parse a save strategy from an env var string."""
    try:
        return SnapshotSaveStrategy(value)
    except ValueError:
        valid = ", ".join(s.value for s in SnapshotSaveStrategy)
        raise InvalidStrategyError(
            f"Invalid SNAPSHOT_SAVE_STRATEGY: {value!r}. Valid values: {valid}"
        )


def _parse_load_strategy(value: str) -> SnapshotLoadStrategy:
    """Parse a load strategy from an env var string."""
    try:
        return SnapshotLoadStrategy(value)
    except ValueError:
        valid = ", ".join(s.value for s in SnapshotLoadStrategy)
        raise InvalidStrategyError(
            f"Invalid SNAPSHOT_LOAD_STRATEGY: {value!r}. Valid values: {valid}"
        )


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

        # Change tracking
        self.change_tracker = ChangeTracker()

        # Lazy-load tracking for on_request load strategy
        self._lazy_loaded = False

        # Versioned in-memory snapshots: {name: {versions: {N: {...}}, latest: N}}
        self._versioned_snapshots: dict[str, dict[str, Any]] = {}
        self._snapshot_lock = threading.Lock()

        # Parse strategies from env
        save_str = os.environ.get("SNAPSHOT_SAVE_STRATEGY")
        if save_str:
            self.save_strategy = _parse_save_strategy(save_str)
        else:
            self.save_strategy = SnapshotSaveStrategy.ON_SHUTDOWN

        load_str = os.environ.get("SNAPSHOT_LOAD_STRATEGY")
        if load_str:
            self.load_strategy = _parse_load_strategy(load_str)
        else:
            self.load_strategy = SnapshotLoadStrategy.ON_STARTUP

        # Flush interval for scheduled strategy
        self.flush_interval: float = float(os.environ.get("SNAPSHOT_FLUSH_INTERVAL", "15"))

        # Scheduled saver thread
        self._scheduler_stop = threading.Event()
        self._scheduler_thread: threading.Thread | None = None

    # ------------------------------------------------------------------
    # Strategy hooks -- called by the gateway
    # ------------------------------------------------------------------

    def on_mutating_request(self) -> None:
        """Called after a mutating AWS request completes.

        Triggers a save if the save strategy is ON_REQUEST (debounced).
        """
        if self.save_strategy != SnapshotSaveStrategy.ON_REQUEST:
            return
        if not self.change_tracker.is_dirty:
            return
        self.save_debounced()

    def on_first_request(self) -> None:
        """Called on the first incoming request.

        Triggers a lazy load if the load strategy is ON_REQUEST.
        """
        if self.load_strategy != SnapshotLoadStrategy.ON_REQUEST:
            return
        if self._lazy_loaded:
            return
        self._lazy_loaded = True
        self.load()

    def on_shutdown(self) -> None:
        """Called when the server is shutting down.

        Saves state if the save strategy is ON_SHUTDOWN.
        """
        self.stop_scheduled_saver()
        if self.save_strategy != SnapshotSaveStrategy.ON_SHUTDOWN:
            return
        if self.state_dir:
            self.save()

    # ------------------------------------------------------------------
    # Scheduled saver
    # ------------------------------------------------------------------

    def start_scheduled_saver(self) -> None:
        """Start the background thread for scheduled saves."""
        if self._scheduler_thread is not None:
            return
        self._scheduler_stop.clear()
        self._scheduler_thread = threading.Thread(
            target=self._scheduled_saver_loop,
            daemon=True,
            name="snapshot-scheduler",
        )
        self._scheduler_thread.start()
        logger.info("Scheduled snapshot saver started (interval=%.1fs)", self.flush_interval)

    def stop_scheduled_saver(self) -> None:
        """Stop the background scheduled saver thread."""
        if self._scheduler_thread is None:
            return
        self._scheduler_stop.set()
        self._scheduler_thread.join(timeout=5.0)
        self._scheduler_thread = None
        logger.info("Scheduled snapshot saver stopped")

    def _scheduled_saver_loop(self) -> None:
        """Background loop that saves state on interval when dirty."""
        while not self._scheduler_stop.wait(timeout=self.flush_interval):
            self._do_scheduled_save()

    def _do_scheduled_save(self) -> None:
        """Single scheduled save tick: save only if dirty."""
        if not self.change_tracker.is_dirty:
            return
        if not self.state_dir:
            return
        try:
            self.save()
            self.change_tracker.mark_clean()
        except Exception:
            logger.warning("Scheduled save failed", exc_info=True)

    # ------------------------------------------------------------------
    # Core save/load
    # ------------------------------------------------------------------

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
            name: Named snapshot -- saves under state_dir/snapshots/{name}/.
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
            self.change_tracker.mark_clean()
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
            name: Named snapshot -- loads from state_dir/snapshots/{name}/.
                  Use "latest" to load the most recently saved snapshot.
            services: If provided, only load these services.
        """
        from robotocore.state.hooks import HookType

        if name:
            base = Path(path) if path else self.state_dir
            if not base:
                return False

            # Handle "latest" -- find the most recently saved snapshot
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

        # No name -- save current state to a temp dir and compress it
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
        # No timestamps -- just return last alphabetically
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
                except KeyError as exc:
                    logger.debug("_read_compressed_metadata: getmember failed (non-fatal): %s", exc)
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
                    # Verify this service is picklable (threading objects handled)
                    _safe_pickle_dumps(service_state)
                    state[service_name] = service_state
                except Exception:
                    logger.debug(
                        "Could not save Moto state for %s",
                        service_name,
                        exc_info=True,
                    )

            with open(path, "wb") as f:
                _ThreadSafePickler(f, protocol=pickle.HIGHEST_PROTOCOL).dump(state)

        except Exception:
            logger.warning("Failed to save Moto state", exc_info=True)
            # Remove corrupt/partial pickle so load doesn't choke on it
            try:
                path.unlink(missing_ok=True)
            except OSError as exc:
                logger.debug("_save_moto_state: unlink failed (non-fatal): %s", exc)

    def _load_moto_state(self, path: Path, services: list[str] | None = None) -> bool:
        """Restore Moto backends from pickle. Returns True on success.

        Raises pickle.UnpicklingError if the pickle contains disallowed classes
        (security violation). Other errors return False.
        """
        try:
            from moto.backends import get_backend

            with open(path, "rb") as f:
                state = _RestrictedUnpickler(f).load()

            # Restore threading primitives that were replaced with sentinels
            _restore_threading_objects(state)

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
            except (json.JSONDecodeError, OSError) as exc:
                logger.debug("_save_native_state: _loads_native failed (non-fatal): %s", exc)

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
                except Exception as exc:
                    logger.debug("_reset_moto_state: get_backend failed (non-fatal): %s", exc)
        except Exception:
            logger.debug("Failed to reset Moto state", exc_info=True)

    def _list_moto_services(self, services: list[str] | None = None) -> list[str]:
        """List Moto services that have backends with data."""
        from robotocore.services.registry import SERVICE_REGISTRY

        all_services = sorted(SERVICE_REGISTRY.keys())
        if services:
            return [s for s in all_services if s in services]
        return all_services

    # ------------------------------------------------------------------
    # Versioned in-memory snapshots
    # ------------------------------------------------------------------

    def _capture_state(self, services: list[str] | None = None) -> dict[str, Any]:
        """Capture the current emulator state as a serializable dict.

        This pickles Moto backends and collects native handler state, storing
        everything in a dict suitable for in-memory versioned snapshots.
        """
        state: dict[str, Any] = {}

        # Capture Moto state via pickle round-trip into bytes
        try:
            from moto.backends import get_backend

            moto_state: dict = {}
            for service_name in self._list_moto_services(services):
                try:
                    backend_dict = get_backend(service_name)
                    service_state: dict = {}
                    for account_id in list(backend_dict.keys()):
                        account_state: dict = {}
                        for region in list(backend_dict[account_id].keys()):
                            backend = backend_dict[account_id][region]
                            account_state[region] = backend
                        service_state[account_id] = account_state
                    # Verify this service is picklable (threading objects handled)
                    _safe_pickle_dumps(service_state)
                    moto_state[service_name] = service_state
                except Exception:
                    logger.debug("Could not capture Moto state for %s", service_name, exc_info=True)
            state["moto"] = _safe_pickle_dumps(moto_state)
        except Exception:
            logger.debug("Could not capture Moto state", exc_info=True)
            state["moto"] = b""

        # Capture native provider state
        native: dict = {}
        for service, (save_fn, _) in self._native_handlers.items():
            if services and service not in services:
                continue
            try:
                native[service] = copy.deepcopy(save_fn())
            except Exception:
                logger.debug("Could not capture native state for %s", service, exc_info=True)
        state["native"] = native

        return state

    def _restore_state(self, state: dict[str, Any], services: list[str] | None = None) -> None:
        """Restore emulator state from a previously captured dict."""
        # Restore Moto state
        moto_bytes = state.get("moto", b"")
        if moto_bytes:
            try:
                from moto.backends import get_backend

                moto_state = _RestrictedUnpickler(io.BytesIO(moto_bytes)).load()
                # Restore threading primitives that were replaced with sentinels
                _restore_threading_objects(moto_state)
                for service_name, service_state in moto_state.items():
                    if services and service_name not in services:
                        continue
                    try:
                        backend_dict = get_backend(service_name)
                        for account_id, account_state in service_state.items():
                            for region, backend in account_state.items():
                                backend_dict[account_id][region] = backend
                    except Exception:
                        logger.debug(
                            "Could not restore Moto state for %s",
                            service_name,
                            exc_info=True,
                        )
            except Exception:
                logger.debug("Could not restore Moto state", exc_info=True)

        # Restore native provider state
        native = state.get("native", {})
        for service, (_, load_fn) in self._native_handlers.items():
            if services and service not in services:
                continue
            if service in native:
                try:
                    load_fn(copy.deepcopy(native[service]))
                except Exception:
                    logger.debug("Could not restore native state for %s", service, exc_info=True)

    def _estimate_size(self, state: dict[str, Any]) -> int:
        """Estimate size of a captured state dict in bytes."""
        size = len(state.get("moto", b""))
        try:
            native_json = json.dumps(state.get("native", {}), default=str)
            size += len(native_json.encode())
        except Exception:
            size += sys.getsizeof(state.get("native", {}))
        return size

    def save_versioned(
        self,
        name: str,
        services: list[str] | None = None,
    ) -> dict[str, Any]:
        """Save the current state as a new version of a named snapshot.

        Auto-increments the version number. Returns metadata dict with
        name, version, timestamp, services, and size.
        """
        _validate_snapshot_name(name)

        captured = self._capture_state(services=services)
        ts = time.time()
        size = self._estimate_size(captured)

        # Determine which services are included
        included_services: list[str] = []
        if services:
            included_services = list(services)
        else:
            included_services = self._list_moto_services()
            included_services.extend(self._native_handlers.keys())
            included_services = sorted(set(included_services))

        with self._snapshot_lock:
            if name not in self._versioned_snapshots:
                self._versioned_snapshots[name] = {"versions": {}, "latest": 0}

            entry = self._versioned_snapshots[name]
            new_version = entry["latest"] + 1
            entry["versions"][new_version] = {
                "data": captured,
                "timestamp": ts,
                "services": included_services,
                "size": size,
            }
            entry["latest"] = new_version

        logger.info("Saved versioned snapshot '%s' v%d (%d bytes)", name, new_version, size)
        return {
            "name": name,
            "version": new_version,
            "timestamp": ts,
            "services": included_services,
            "size": size,
        }

    def load_versioned(
        self,
        name: str,
        version: int | None = None,
        services: list[str] | None = None,
    ) -> dict[str, Any]:
        """Load a versioned snapshot into the emulator.

        Args:
            name: Snapshot name.
            version: Version number to load. None means latest.
            services: If provided, only restore these services.

        Returns metadata dict. Raises ValueError if not found.
        """
        with self._snapshot_lock:
            if name not in self._versioned_snapshots:
                raise ValueError(f"Snapshot '{name}' not found")

            entry = self._versioned_snapshots[name]

            if version is None:
                version = entry["latest"]

            if version not in entry["versions"]:
                raise ValueError(f"Snapshot '{name}' version {version} not found")

            ver_data = entry["versions"][version]

        self._restore_state(ver_data["data"], services=services)

        logger.info("Loaded versioned snapshot '%s' v%d", name, version)
        return {
            "name": name,
            "version": version,
            "timestamp": ver_data["timestamp"],
            "services": ver_data["services"],
            "size": ver_data["size"],
        }

    def list_versioned(self) -> list[dict[str, Any]]:
        """List all versioned snapshots with their metadata.

        Returns a list of dicts, each with name, latest version, version count,
        and per-version metadata.
        """
        result: list[dict[str, Any]] = []
        with self._snapshot_lock:
            for name, entry in sorted(self._versioned_snapshots.items()):
                versions_meta: list[dict[str, Any]] = []
                for ver_num in sorted(entry["versions"]):
                    ver = entry["versions"][ver_num]
                    versions_meta.append(
                        {
                            "version": ver_num,
                            "timestamp": ver["timestamp"],
                            "services": ver["services"],
                            "size": ver["size"],
                        }
                    )
                result.append(
                    {
                        "name": name,
                        "latest": entry["latest"],
                        "version_count": len(entry["versions"]),
                        "versions": versions_meta,
                    }
                )
        return result

    def versions_for_snapshot(self, name: str) -> list[dict[str, Any]]:
        """Return version history for a specific named snapshot.

        Raises ValueError if the snapshot does not exist.
        """
        with self._snapshot_lock:
            if name not in self._versioned_snapshots:
                raise ValueError(f"Snapshot '{name}' not found")

            entry = self._versioned_snapshots[name]
            versions_meta: list[dict[str, Any]] = []
            for ver_num in sorted(entry["versions"]):
                ver = entry["versions"][ver_num]
                versions_meta.append(
                    {
                        "version": ver_num,
                        "timestamp": ver["timestamp"],
                        "services": ver["services"],
                        "size": ver["size"],
                    }
                )
        return versions_meta

    def delete_versioned(
        self,
        name: str,
        version: int | None = None,
    ) -> dict[str, Any]:
        """Delete a versioned snapshot or a specific version.

        Args:
            name: Snapshot name.
            version: If provided, delete only this version. If None, delete all.

        Returns a summary dict. Raises ValueError if not found.
        """
        with self._snapshot_lock:
            if name not in self._versioned_snapshots:
                raise ValueError(f"Snapshot '{name}' not found")

            entry = self._versioned_snapshots[name]

            if version is not None:
                if version not in entry["versions"]:
                    raise ValueError(f"Snapshot '{name}' version {version} not found")
                del entry["versions"][version]

                # If no versions remain, remove the whole entry
                if not entry["versions"]:
                    del self._versioned_snapshots[name]
                    logger.info("Deleted snapshot '%s' (last version removed)", name)
                    return {"name": name, "deleted_version": version, "remaining": 0}

                # Update latest if we deleted the latest
                if version == entry["latest"]:
                    entry["latest"] = max(entry["versions"])

                remaining = len(entry["versions"])
                logger.info(
                    "Deleted version %d of snapshot '%s' (%d remaining)",
                    version,
                    name,
                    remaining,
                )
                return {
                    "name": name,
                    "deleted_version": version,
                    "remaining": remaining,
                }
            else:
                version_count = len(entry["versions"])
                del self._versioned_snapshots[name]
                logger.info("Deleted all %d versions of snapshot '%s'", version_count, name)
                return {
                    "name": name,
                    "deleted_versions": version_count,
                    "remaining": 0,
                }


# Singleton
_manager: StateManager | None = None


def get_state_manager() -> StateManager:
    """Get the global StateManager instance."""
    global _manager
    if _manager is None:
        state_dir = os.environ.get("ROBOTOCORE_STATE_DIR")
        _manager = StateManager(state_dir=state_dir)
    return _manager
