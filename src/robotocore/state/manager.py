"""State persistence manager — save and restore emulator state across restarts.

Supports saving all Moto backend state plus native provider state to disk,
and restoring it on startup. This enables "Cloud Pods"-like functionality
where you can snapshot and share emulator state.

Configuration via environment variables:
    ROBOTOCORE_STATE_DIR=/path/to/state    Save/load state directory
    ROBOTOCORE_PERSIST=1                   Enable auto-save on shutdown
"""

import json
import logging
import os
import pickle
import time
from pathlib import Path

logger = logging.getLogger(__name__)


class StateManager:
    """Manages save/restore of emulator state."""

    def __init__(self, state_dir: str | None = None) -> None:
        self.state_dir = Path(state_dir) if state_dir else None
        self._native_handlers: dict[str, tuple] = {}

    def register_native_handler(
        self,
        service: str,
        save_fn,
        load_fn,
    ) -> None:
        """Register a native provider's save/load functions."""
        self._native_handlers[service] = (save_fn, load_fn)

    def save(self, path: str | Path | None = None) -> str:
        """Save all state to disk. Returns the path used."""
        save_dir = Path(path) if path else self.state_dir
        if not save_dir:
            raise ValueError("No state directory configured")

        save_dir.mkdir(parents=True, exist_ok=True)
        timestamp = time.strftime("%Y%m%dT%H%M%S")

        # Save Moto backend state
        moto_path = save_dir / "moto_state.pkl"
        self._save_moto_state(moto_path)

        # Save native provider state
        native_path = save_dir / "native_state.json"
        self._save_native_state(native_path)

        # Save metadata
        meta = {
            "version": "1.0",
            "timestamp": timestamp,
            "saved_at": time.time(),
            "moto_services": self._list_moto_services(),
            "native_services": list(self._native_handlers.keys()),
        }
        meta_path = save_dir / "metadata.json"
        meta_path.write_text(json.dumps(meta, indent=2))

        logger.info("State saved to %s", save_dir)
        return str(save_dir)

    def load(self, path: str | Path | None = None) -> bool:
        """Load state from disk. Returns True if successful."""
        load_dir = Path(path) if path else self.state_dir
        if not load_dir or not load_dir.exists():
            logger.debug("No state directory found at %s", load_dir)
            return False

        meta_path = load_dir / "metadata.json"
        if not meta_path.exists():
            logger.warning("No metadata.json in %s — skipping load", load_dir)
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
            self._load_moto_state(moto_path)

        # Load native provider state
        native_path = load_dir / "native_state.json"
        if native_path.exists():
            self._load_native_state(native_path)

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

    def _save_moto_state(self, path: Path) -> None:
        """Pickle all Moto backends."""
        try:
            from moto.backends import get_backend
            from moto.core import DEFAULT_ACCOUNT_ID

            state = {}
            for service_name in self._list_moto_services():
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
                    logger.debug("Could not save Moto state for %s", service_name, exc_info=True)

            with open(path, "wb") as f:
                pickle.dump(state, f, protocol=pickle.HIGHEST_PROTOCOL)

        except Exception:
            logger.warning("Failed to save Moto state", exc_info=True)

    def _load_moto_state(self, path: Path) -> None:
        """Restore Moto backends from pickle."""
        try:
            from moto.backends import get_backend

            with open(path, "rb") as f:
                state = pickle.load(f)

            for service_name, service_state in state.items():
                try:
                    backend_dict = get_backend(service_name)
                    for account_id, account_state in service_state.items():
                        for region, backend in account_state.items():
                            backend_dict[account_id][region] = backend
                except Exception:
                    logger.debug("Could not load Moto state for %s", service_name, exc_info=True)

        except Exception:
            logger.warning("Failed to load Moto state", exc_info=True)

    def _save_native_state(self, path: Path) -> None:
        """Save native provider state as JSON."""
        state = {}
        for service, (save_fn, _) in self._native_handlers.items():
            try:
                state[service] = save_fn()
            except Exception:
                logger.debug("Could not save native state for %s", service, exc_info=True)

        path.write_text(json.dumps(state, indent=2, default=str))

    def _load_native_state(self, path: Path) -> None:
        """Load native provider state from JSON."""
        state = json.loads(path.read_text())
        for service, (_, load_fn) in self._native_handlers.items():
            if service in state:
                try:
                    load_fn(state[service])
                except Exception:
                    logger.debug("Could not load native state for %s", service, exc_info=True)

    def _reset_moto_state(self) -> None:
        """Reset all Moto backends."""
        try:
            import moto.core.models as moto_models
            if hasattr(moto_models, "base_decorator"):
                moto_models.base_decorator.reset()
            else:
                from moto.core import DEFAULT_ACCOUNT_ID
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

    def _list_moto_services(self) -> list[str]:
        """List Moto services that have backends with data."""
        from robotocore.services.registry import SERVICE_REGISTRY
        return sorted(SERVICE_REGISTRY.keys())


# Singleton
_manager: StateManager | None = None


def get_state_manager() -> StateManager:
    """Get the global StateManager instance."""
    global _manager
    if _manager is None:
        state_dir = os.environ.get("ROBOTOCORE_STATE_DIR")
        _manager = StateManager(state_dir=state_dir)
    return _manager
