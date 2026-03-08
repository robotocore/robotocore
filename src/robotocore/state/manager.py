"""State persistence manager -- save and restore emulator state across restarts.

Supports saving all Moto backend state plus native provider state to disk,
and restoring it on startup. This enables "Cloud Pods"-like functionality
where you can snapshot and share emulator state.

Configuration via environment variables:
    ROBOTOCORE_STATE_DIR=/path/to/state    Save/load state directory
    ROBOTOCORE_PERSIST=1                   Enable auto-save on shutdown
    PERSISTENCE=1                          Enable auto-save after mutations
"""

import json
import logging
import os
import pickle
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
    ) -> str:
        """Save all state to disk. Returns the path used.

        Args:
            path: Directory to save to (defaults to state_dir).
            name: Named snapshot — saves under state_dir/snapshots/{name}/.
            services: If provided, only save these services.
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
        }
        meta_path = save_dir / "metadata.json"
        meta_path.write_text(json.dumps(meta, indent=2))

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
        """List all named snapshots."""
        if not self.state_dir:
            return []
        snap_dir = self.state_dir / "snapshots"
        if not snap_dir.exists():
            return []
        snapshots = []
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
                        }
                    )
                else:
                    snapshots.append({"name": entry.name})
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
            services: If provided, only load these services.
        """
        if name:
            base = Path(path) if path else self.state_dir
            if not base:
                return False
            load_dir = base / "snapshots" / name
            # Prevent path traversal
            if not load_dir.resolve().is_relative_to((base / "snapshots").resolve()):
                raise ValueError(f"Invalid snapshot name (path traversal): {name!r}")
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
