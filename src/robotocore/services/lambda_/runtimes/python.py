"""Python runtime executor — runs handler in-process for speed.

Lambda Python handlers are dispatched in-process for performance: there's no
subprocess overhead per invocation. The trade-off is that we cannot swap
interpreter versions per function — the host's Python is what runs the code.

We still accept the AWS runtime identifier (e.g. ``python3.12``) so we can
warn when there's a major-version mismatch between what the function declared
and what the host can offer. Handlers that rely on version-specific syntax or
stdlib behavior will see the warning rather than a silent mismatch.
"""

import logging
import sys

from robotocore.services.lambda_.executor import execute_python_handler

logger = logging.getLogger(__name__)

# Lambda runtime identifier → (major, minor) of the matching CPython.
# Runtimes absent from this map (e.g. python3.7) trigger a warning when used.
_RUNTIME_BINARY: dict[str, tuple[int, int]] = {
    "python3.8": (3, 8),
    "python3.9": (3, 9),
    "python3.10": (3, 10),
    "python3.11": (3, 11),
    "python3.12": (3, 12),
    "python3.13": (3, 13),
}


class PythonExecutor:
    def __init__(self, runtime: str = "") -> None:
        self._runtime = runtime
        self._mismatch_warned = False

    def execute(
        self,
        code_zip: bytes,
        handler: str,
        event: dict,
        function_name: str,
        timeout: int = 3,
        memory_size: int = 128,
        env_vars: dict | None = None,
        region: str = "us-east-1",
        account_id: str = "123456789012",
        layer_zips: list[bytes] | None = None,
        code_dir: str | None = None,
        hot_reload: bool = False,
    ) -> tuple[dict | str | list | None, str | None, str]:
        self._check_version_match()
        return execute_python_handler(
            code_zip=code_zip,
            handler=handler,
            event=event,
            function_name=function_name,
            timeout=timeout,
            memory_size=memory_size,
            env_vars=env_vars,
            region=region,
            account_id=account_id,
            layer_zips=layer_zips,
            code_dir=code_dir,
            hot_reload=hot_reload,
        )

    def _check_version_match(self) -> None:
        """Warn once per executor instance if the host Python doesn't match."""
        if self._mismatch_warned or not self._runtime:
            return
        expected = _RUNTIME_BINARY.get(self._runtime)
        host = (sys.version_info.major, sys.version_info.minor)
        if expected is None:
            logger.warning(
                "Unknown python runtime %r — executing in host Python %d.%d. Supported: %s",
                self._runtime,
                host[0],
                host[1],
                ", ".join(sorted(_RUNTIME_BINARY)),
            )
        elif expected != host:
            logger.warning(
                "Python runtime %r requested but host is Python %d.%d — "
                "handler will run in-process under the host interpreter.",
                self._runtime,
                host[0],
                host[1],
            )
        self._mismatch_warned = True
