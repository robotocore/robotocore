"""Lambda recursion detection — prevents infinite invocation loops.

AWS Lambda recursion detection tracks when a function invokes itself (directly
or via a chain through SQS/SNS/etc). When RecursiveLoop is set to "Terminate"
(the default), invocations are terminated after a threshold (~16 recursive calls).

The recursion depth is tracked per-function using a threading-aware counter.
Each invocation increments the counter on entry and decrements on exit.
Cross-service invocations (via invoke.py) also participate in tracking.
"""

import logging
import threading

logger = logging.getLogger(__name__)

# Maximum recursive invocation depth before termination (matches AWS behavior)
MAX_RECURSION_DEPTH = 16

# Per-thread recursion depth counters: key = (account_id, region, func_name)
_recursion_depths: dict[tuple[str, str, str], int] = {}
_depth_lock = threading.Lock()


class RecursiveInvocationException(Exception):  # noqa: N818 — matches AWS error code
    """Raised when a Lambda function exceeds the recursive invocation limit."""

    def __init__(self, function_name: str, depth: int):
        self.function_name = function_name
        self.depth = depth
        super().__init__(
            f"RecursiveInvocationException: Lambda function {function_name} "
            f"exceeded recursive invocation limit ({depth} >= {MAX_RECURSION_DEPTH})"
        )


def get_recursion_depth(account_id: str, region: str, func_name: str) -> int:
    """Return the current recursion depth for a function."""
    key = (account_id, region, func_name)
    with _depth_lock:
        return _recursion_depths.get(key, 0)


def increment_depth(account_id: str, region: str, func_name: str) -> int:
    """Increment recursion depth and return the new value."""
    key = (account_id, region, func_name)
    with _depth_lock:
        current = _recursion_depths.get(key, 0)
        _recursion_depths[key] = current + 1
        return current + 1


def decrement_depth(account_id: str, region: str, func_name: str) -> None:
    """Decrement recursion depth after invocation completes."""
    key = (account_id, region, func_name)
    with _depth_lock:
        current = _recursion_depths.get(key, 0)
        if current > 0:
            _recursion_depths[key] = current - 1
        if _recursion_depths.get(key, 0) == 0:
            _recursion_depths.pop(key, None)


def check_recursion(
    account_id: str, region: str, func_name: str, recursive_loop: str = "Terminate"
) -> None:
    """Check recursion depth and raise if limit exceeded.

    Args:
        account_id: AWS account ID
        region: AWS region
        func_name: Lambda function name
        recursive_loop: "Terminate" (default) or "Allow"

    Raises:
        RecursiveInvocationException: If depth >= MAX_RECURSION_DEPTH and mode is "Terminate"
    """
    if recursive_loop == "Allow":
        return

    depth = get_recursion_depth(account_id, region, func_name)
    if depth >= MAX_RECURSION_DEPTH:
        logger.warning(
            "Recursive invocation detected for %s (depth=%d, limit=%d)",
            func_name,
            depth,
            MAX_RECURSION_DEPTH,
        )
        raise RecursiveInvocationException(func_name, depth)


def get_recursion_config(account_id: str, region: str, func_name: str) -> str:
    """Get the recursion config for a function from the provider store.

    Returns "Terminate" (default) or "Allow".
    """
    from robotocore.services.lambda_.provider import _recursion_configs, _recursion_lock

    key = (account_id, region, func_name)
    with _recursion_lock:
        return _recursion_configs.get(key, "Terminate")


def reset_all_depths() -> None:
    """Reset all recursion depth counters (for testing)."""
    with _depth_lock:
        _recursion_depths.clear()
