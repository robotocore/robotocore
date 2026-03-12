"""Lambda concurrency limits and code size validation.

Tracks concurrent executions per function and globally, validates code/payload sizes,
and enforces reserved/provisioned concurrency constraints. All limit values are
configurable via environment variables.
"""

import os
import threading

# ---------------------------------------------------------------------------
# Configurable limits (env-var overridable)
# ---------------------------------------------------------------------------

_MB = 1024 * 1024
_GB = 1024 * _MB


def _env_int(name: str, default: int) -> int:
    """Read an integer from the environment, falling back to *default*."""
    raw = os.environ.get(name)
    if raw is not None:
        try:
            return int(raw)
        except ValueError:
            pass
    return default


def get_concurrent_executions_limit() -> int:
    return _env_int("LAMBDA_LIMITS_CONCURRENT_EXECUTIONS", 1000)


def get_minimum_unreserved_concurrency() -> int:
    return _env_int("LAMBDA_LIMITS_MINIMUM_UNRESERVED_CONCURRENCY", 100)


def get_total_code_size_limit() -> int:
    return _env_int("LAMBDA_LIMITS_TOTAL_CODE_SIZE", 80 * _GB)


def get_code_size_zipped_limit() -> int:
    return _env_int("LAMBDA_LIMITS_CODE_SIZE_ZIPPED", 50 * _MB)


def get_code_size_unzipped_limit() -> int:
    return _env_int("LAMBDA_LIMITS_CODE_SIZE_UNZIPPED", 250 * _MB)


def get_create_function_request_size_limit() -> int:
    return _env_int("LAMBDA_LIMITS_CREATE_FUNCTION_REQUEST_SIZE", 70 * _MB)


def get_max_function_envvar_size() -> int:
    return _env_int("LAMBDA_LIMITS_MAX_FUNCTION_ENVVAR_SIZE_BYTES", 4096)


def get_max_payload_sync() -> int:
    return _env_int("LAMBDA_LIMITS_MAX_FUNCTION_PAYLOAD_SIZE_BYTES", 6 * _MB)


def get_max_payload_async() -> int:
    return _env_int("LAMBDA_LIMITS_MAX_FUNCTION_PAYLOAD_ASYNC_BYTES", 256 * 1024)


# ---------------------------------------------------------------------------
# Concurrency tracker  (thread-safe)
# ---------------------------------------------------------------------------


class ConcurrencyTracker:
    """Track concurrent Lambda executions per-function and globally.

    Thread-safe: all mutations are protected by a single lock so that the
    global counter stays in sync with per-function counters.
    """

    def __init__(self) -> None:
        self._lock = threading.Lock()
        # function_key -> current concurrent count
        self._function_counts: dict[str, int] = {}
        self._global_count: int = 0

        # function_key -> reserved concurrency (set via PutFunctionConcurrency)
        self._reserved: dict[str, int] = {}

        # function_key -> provisioned concurrency capacity
        self._provisioned: dict[str, int] = {}

        # function_key -> current provisioned concurrency in use
        self._provisioned_in_use: dict[str, int] = {}

        # account-level total code size tracking
        self._total_code_size: int = 0

    # -- concurrent execution tracking --

    def acquire(self, function_key: str, qualifier: str = "$LATEST") -> bool:
        """Increment concurrent execution count. Returns True if the invocation
        was routed through provisioned concurrency, False if on-demand.

        Raises TooManyRequestsException if the account-level limit or function
        reserved limit is reached.
        """
        with self._lock:
            account_limit = get_concurrent_executions_limit()

            # Check account-level limit
            if self._global_count >= account_limit:
                raise TooManyRequestsException(
                    f"Rate Exceeded. Account concurrent execution limit {account_limit} reached."
                )

            # Check if provisioned concurrency is available for this function+qualifier
            prov_key = f"{function_key}:{qualifier}"
            provisioned_capacity = self._provisioned.get(prov_key)
            used_provisioned = False

            if provisioned_capacity is not None and provisioned_capacity > 0:
                prov_in_use = self._provisioned_in_use.get(prov_key, 0)
                if prov_in_use < provisioned_capacity:
                    # Route through provisioned pool
                    self._provisioned_in_use[prov_key] = prov_in_use + 1
                    used_provisioned = True
                # If provisioned is full, fall through to on-demand pool

            # Check per-function reserved concurrency
            reserved = self._reserved.get(function_key)
            if reserved is not None:
                current = self._function_counts.get(function_key, 0)
                if current >= reserved:
                    # If we already allocated provisioned, undo it
                    if used_provisioned:
                        self._provisioned_in_use[prov_key] = (
                            self._provisioned_in_use.get(prov_key, 1) - 1
                        )
                        used_provisioned = False
                    raise TooManyRequestsException(
                        f"Rate Exceeded. Function {function_key} reserved concurrency "
                        f"limit {reserved} reached."
                    )
            else:
                # For unreserved functions, check unreserved pool
                total_reserved = sum(self._reserved.values())
                unreserved_limit = account_limit - total_reserved
                unreserved_in_use = sum(
                    count
                    for key, count in self._function_counts.items()
                    if key not in self._reserved
                )
                if unreserved_in_use >= unreserved_limit:
                    # If we already allocated provisioned, undo it
                    if used_provisioned:
                        self._provisioned_in_use[prov_key] = (
                            self._provisioned_in_use.get(prov_key, 1) - 1
                        )
                        used_provisioned = False
                    raise TooManyRequestsException(
                        "Rate Exceeded. No unreserved concurrency available."
                    )

            self._function_counts[function_key] = self._function_counts.get(function_key, 0) + 1
            self._global_count += 1
            return used_provisioned

    def release(self, function_key: str, qualifier: str = "$LATEST") -> None:
        """Decrement concurrent execution count."""
        with self._lock:
            current = self._function_counts.get(function_key, 0)
            if current > 0:
                self._function_counts[function_key] = current - 1
                self._global_count = max(0, self._global_count - 1)

            # Also release provisioned slot if one is in use
            prov_key = f"{function_key}:{qualifier}"
            prov_in_use = self._provisioned_in_use.get(prov_key, 0)
            if prov_in_use > 0:
                self._provisioned_in_use[prov_key] = prov_in_use - 1

    def get_function_count(self, function_key: str) -> int:
        with self._lock:
            return self._function_counts.get(function_key, 0)

    def get_global_count(self) -> int:
        with self._lock:
            return self._global_count

    # -- reserved concurrency --

    def set_reserved(self, function_key: str, reserved: int) -> None:
        """Set reserved concurrency for a function.

        Raises InvalidParameterValueException if setting this would leave fewer
        than MINIMUM_UNRESERVED_CONCURRENCY unreserved.
        """
        with self._lock:
            account_limit = get_concurrent_executions_limit()
            min_unreserved = get_minimum_unreserved_concurrency()

            # Calculate total reserved *excluding* the current function
            total_reserved = sum(v for k, v in self._reserved.items() if k != function_key)
            new_total = total_reserved + reserved
            remaining = account_limit - new_total

            if remaining < min_unreserved:
                raise InvalidParameterValueException(
                    f"Specified ReservedConcurrentExecutions for function decreases account's "
                    f"UnreservedConcurrentExecution below its minimum value of {min_unreserved}."
                )

            self._reserved[function_key] = reserved

    def get_reserved(self, function_key: str) -> int | None:
        with self._lock:
            return self._reserved.get(function_key)

    def delete_reserved(self, function_key: str) -> None:
        with self._lock:
            self._reserved.pop(function_key, None)

    # -- provisioned concurrency --

    def set_provisioned(self, function_key: str, provisioned: int) -> None:
        with self._lock:
            self._provisioned[function_key] = provisioned

    def get_provisioned(self, function_key: str) -> int | None:
        with self._lock:
            return self._provisioned.get(function_key)

    def delete_provisioned(self, function_key: str) -> None:
        with self._lock:
            self._provisioned.pop(function_key, None)
            self._provisioned_in_use.pop(function_key, None)

    def get_provisioned_in_use(self, function_key: str) -> int:
        """Return number of provisioned concurrency slots currently in use."""
        with self._lock:
            return self._provisioned_in_use.get(function_key, 0)

    def get_provisioned_utilization(self, function_key: str) -> float:
        """Return provisioned concurrency utilization as a fraction (0.0 - 1.0)."""
        with self._lock:
            capacity = self._provisioned.get(function_key, 0)
            if capacity == 0:
                return 0.0
            in_use = self._provisioned_in_use.get(function_key, 0)
            return min(1.0, in_use / capacity)

    # -- account code size tracking --

    def add_code_size(self, size: int) -> None:
        """Add to total account code size. Raises if over limit."""
        with self._lock:
            new_total = self._total_code_size + size
            if new_total > get_total_code_size_limit():
                raise CodeStorageExceededException(
                    f"Code storage limit exceeded. Total: {new_total}, "
                    f"Limit: {get_total_code_size_limit()}"
                )
            self._total_code_size = new_total

    def remove_code_size(self, size: int) -> None:
        with self._lock:
            self._total_code_size = max(0, self._total_code_size - size)

    def get_total_code_size(self) -> int:
        with self._lock:
            return self._total_code_size

    def reset(self) -> None:
        """Reset all state (for testing)."""
        with self._lock:
            self._function_counts.clear()
            self._global_count = 0
            self._reserved.clear()
            self._provisioned.clear()
            self._provisioned_in_use.clear()
            self._total_code_size = 0


# Module-level singleton
_tracker = ConcurrencyTracker()


def get_concurrency_tracker() -> ConcurrencyTracker:
    return _tracker


# ---------------------------------------------------------------------------
# Validation helpers
# ---------------------------------------------------------------------------


def validate_code_size_zipped(size: int) -> None:
    """Raise InvalidParameterValueException if zip exceeds limit."""
    limit = get_code_size_zipped_limit()
    if size > limit:
        raise InvalidParameterValueException(
            f"Unzipped size must be smaller than {limit} bytes. Actual: {size}"
        )


def validate_envvar_size(env_vars: dict[str, str]) -> None:
    """Raise InvalidParameterValueException if total env var size exceeds limit."""
    limit = get_max_function_envvar_size()
    total = sum(len(k) + len(v) for k, v in env_vars.items())
    if total > limit:
        raise InvalidParameterValueException(
            f"Lambda was unable to configure your environment variables because the "
            f"environment variables you have provided exceeded the 4KB limit. "
            f"String measured: {total}, Limit: {limit}"
        )


def validate_payload_size(payload_bytes: int, *, is_async: bool = False) -> None:
    """Raise RequestTooLargeException if payload exceeds limit."""
    limit = get_max_payload_async() if is_async else get_max_payload_sync()
    if payload_bytes > limit:
        raise RequestTooLargeException(
            f"Request payload size ({payload_bytes} bytes) exceeds the "
            f"maximum allowed payload size ({limit} bytes)."
        )


# ---------------------------------------------------------------------------
# Lifecycle flags
# ---------------------------------------------------------------------------


def is_synchronous_create() -> bool:
    """When true, CreateFunction blocks until function is Active."""
    return os.environ.get("LAMBDA_SYNCHRONOUS_CREATE", "").lower() in ("1", "true", "yes")


def is_prebuild_images() -> bool:
    """When true, validate Docker images at creation time."""
    return os.environ.get("LAMBDA_PREBUILD_IMAGES", "").lower() in ("1", "true", "yes")


# ---------------------------------------------------------------------------
# Exception types (match AWS error codes)
# ---------------------------------------------------------------------------


class TooManyRequestsException(Exception):  # noqa: N818
    """Raised when concurrency limit is exceeded."""

    pass


class InvalidParameterValueException(Exception):  # noqa: N818
    """Raised for invalid parameter values (code size, env vars, etc.)."""

    pass


class RequestTooLargeException(Exception):  # noqa: N818
    """Raised when invocation payload exceeds limit."""

    pass


class CodeStorageExceededException(Exception):  # noqa: N818
    """Raised when total account code storage exceeds limit."""

    pass
