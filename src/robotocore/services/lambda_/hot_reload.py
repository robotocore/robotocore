"""Lambda hot reload — mount-based code loading with file change detection.

When LAMBDA_CODE_MOUNT_DIR is set, Lambda looks for function code at
{MOUNT_DIR}/{function_name}/ before using the stored zip. Combined with
LAMBDA_HOT_RELOAD=1, code changes are detected via mtime and the handler
module is re-imported on each invocation.
"""

import logging
import os

logger = logging.getLogger(__name__)

# File extensions to monitor for changes
_WATCHED_EXTENSIONS = frozenset(
    {
        ".py",
        ".js",
        ".mjs",
        ".cjs",
        ".ts",
        ".rb",
        ".java",
        ".cs",
        ".go",
    }
)


def get_mount_dir() -> str | None:
    """Return the configured mount directory, or None if not set."""
    return os.environ.get("LAMBDA_CODE_MOUNT_DIR") or None


def is_hot_reload_enabled() -> bool:
    """Check if hot reload is globally enabled via env var."""
    return os.environ.get("LAMBDA_HOT_RELOAD", "").strip() in ("1", "true", "yes")


def is_hot_reload_for_function(env_vars: dict | None) -> bool:
    """Check if hot reload is enabled for a specific function via its env vars."""
    if not env_vars:
        return False
    return "__ROBOTOCORE_HOT_RELOAD__" in env_vars


def get_mount_path(function_name: str) -> str | None:
    """Return the mount path for a function if it exists, else None.

    Checks {LAMBDA_CODE_MOUNT_DIR}/{function_name}/ for existence.
    """
    mount_dir = get_mount_dir()
    if not mount_dir:
        return None
    path = os.path.join(mount_dir, function_name)
    if os.path.isdir(path):
        return path
    return None


class FileWatcher:
    """Mtime-based file change detection for hot reload.

    Tracks the max mtime of watched files per function directory.
    No external dependencies — uses only os.stat().
    """

    def __init__(self) -> None:
        self._last_mtimes: dict[str, float] = {}

    def check_for_changes(self, function_name: str, code_dir: str) -> bool:
        """Return True if files in code_dir have changed since last check.

        On first call for a given function_name, returns False (no change yet)
        and records the current mtime.

        Detects both forward and backward mtime changes (e.g., git checkout
        can set mtimes to older values).
        """
        current_mtime = self._scan_directory(code_dir)
        last_mtime = self._last_mtimes.get(function_name)

        if last_mtime is None:
            # First time seeing this function — record baseline
            self._last_mtimes[function_name] = current_mtime
            return False

        if current_mtime != last_mtime:
            self._last_mtimes[function_name] = current_mtime
            logger.info(
                "Hot reload: detected changes in %s (mtime %.3f -> %.3f)",
                function_name,
                last_mtime,
                current_mtime,
            )
            return True

        return False

    def invalidate(self, function_name: str) -> None:
        """Remove tracked mtime for a function."""
        self._last_mtimes.pop(function_name, None)

    def _scan_directory(self, path: str) -> float:
        """Recursively find the max mtime of watched files in path.

        Does not follow symlinks to avoid infinite loops from circular links.
        Uses os.lstat to get the mtime of the link target without following chains.
        """
        max_mtime = 0.0
        try:
            for dirpath, dirnames, filenames in os.walk(path, followlinks=False):
                # Skip hidden directories and __pycache__
                dirnames[:] = [d for d in dirnames if not d.startswith(".") and d != "__pycache__"]
                for filename in filenames:
                    _, ext = os.path.splitext(filename)
                    if ext in _WATCHED_EXTENSIONS:
                        try:
                            filepath = os.path.join(dirpath, filename)
                            mtime = os.stat(filepath).st_mtime
                            if mtime > max_mtime:
                                max_mtime = mtime
                        except OSError:
                            pass
        except OSError:
            pass
        return max_mtime


# Module-level singleton
_file_watcher = FileWatcher()


def get_file_watcher() -> FileWatcher:
    """Return the global FileWatcher singleton."""
    return _file_watcher
