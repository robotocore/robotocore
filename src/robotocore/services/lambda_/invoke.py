"""Shared Lambda invocation utilities for cross-service integrations.

Runs Lambda execution in a thread pool so the ASGI event loop stays free,
preventing deadlocks when Lambda code calls back to the server (e.g., SQS, S3).
"""

import base64
import concurrent.futures
import logging

logger = logging.getLogger(__name__)

# Thread pool for Lambda invocations — allows Lambda code to call back to the server
_executor = concurrent.futures.ThreadPoolExecutor(max_workers=4, thread_name_prefix="lambda-invoke")


def invoke_lambda_async(
    function_arn: str,
    payload: dict,
    region: str,
    account_id: str,
    callback: callable = None,
) -> None:
    """Fire-and-forget Lambda invocation in a background thread.

    Used by SNS, EventBridge, and other services that dispatch to Lambda.
    The invocation runs in a thread pool so the caller's event loop is not blocked.
    """
    _executor.submit(_invoke_lambda_sync, function_arn, payload, region, account_id, callback)


def invoke_lambda_sync(
    function_arn: str,
    payload: dict,
    region: str,
    account_id: str,
) -> tuple[dict | str | None, str | None, str]:
    """Synchronous Lambda invocation in a thread pool, with result.

    Returns (result, error_type, logs). Blocks until complete but runs
    the actual execution in a worker thread.
    """
    future = _executor.submit(_invoke_lambda_sync, function_arn, payload, region, account_id, None)
    try:
        return future.result(timeout=30)
    except concurrent.futures.TimeoutError:
        return None, "TaskTimedOut", "Lambda execution timed out"
    except Exception as e:  # noqa: BLE001
        return None, "ServiceException", str(e)


def _invoke_lambda_sync(
    function_arn: str,
    payload: dict,
    region: str,
    account_id: str,
    callback: callable = None,
) -> tuple[dict | str | None, str | None, str]:
    """Internal: execute a Lambda function synchronously (runs in thread pool)."""
    from robotocore.services.lambda_.executor import get_layer_zips
    from robotocore.services.lambda_.recursion import (
        RecursiveInvocationException,
        check_recursion,
        decrement_depth,
        get_recursion_config,
        increment_depth,
    )

    # Parse function name from ARN
    arn_parts = function_arn.split(":")
    if len(arn_parts) >= 7 and arn_parts[5] == "function":
        function_name = arn_parts[6]
    else:
        function_name = function_arn.split(":")[-1] if ":" in function_arn else function_arn

    # Parse account from ARN
    if len(arn_parts) >= 5:
        account_id = arn_parts[4]

    # Check recursion detection
    recursive_loop = get_recursion_config(account_id, region, function_name)
    try:
        check_recursion(account_id, region, function_name, recursive_loop)
    except RecursiveInvocationException as exc:
        logger.warning("Recursive invocation blocked for %s: %s", function_name, exc)
        return None, "RecursiveInvocationException", str(exc)

    # Track recursion depth
    increment_depth(account_id, region, function_name)

    try:
        try:
            from moto.backends import get_backend  # noqa: I001
            from moto.core import DEFAULT_ACCOUNT_ID

            acct = account_id if account_id != "123456789012" else DEFAULT_ACCOUNT_ID
            backend = get_backend("lambda")[acct][region]
            fn = backend.get_function(function_name)
        except Exception as e:  # noqa: BLE001
            logger.error("Lambda invoke: function not found: %s (%s)", function_name, e)
            return None, "ResourceNotFoundException", f"Function not found: {function_name}"

        runtime = getattr(fn, "run_time", "") or ""

        # Get code zip
        code_zip = getattr(fn, "code_bytes", None)
        if not code_zip:
            raw = (fn.code or {}).get("ZipFile") if hasattr(fn, "code") else None
            if raw:
                code_zip = base64.b64decode(raw) if isinstance(raw, str) else raw

        if not code_zip:
            logger.error("Lambda invoke: no code for %s", function_name)
            return None, "InvalidCodeException", f"No code found for {function_name}"

        handler = getattr(fn, "handler", "lambda_function.handler")
        timeout = int(getattr(fn, "timeout", 3) or 3)
        memory_size = int(getattr(fn, "memory_size", 128) or 128)
        env_vars = getattr(fn, "environment_vars", {}) or {}
        layer_zips = get_layer_zips(fn, account_id, region)

        from robotocore.services.lambda_.docker_executor import get_executor_mode

        if get_executor_mode() == "docker":
            from robotocore.services.lambda_.docker_executor import get_docker_executor

            docker_exec = get_docker_executor()
            result, error_type, logs = docker_exec.execute(
                code_zip=code_zip,
                handler=handler,
                event=payload,
                function_name=function_name,
                runtime=runtime,
                timeout=timeout,
                memory_size=memory_size,
                env_vars=env_vars,
                region=region,
                account_id=account_id,
                layer_zips=layer_zips if layer_zips else None,
            )
        else:
            from robotocore.services.lambda_.runtimes import get_executor_for_runtime

            executor = get_executor_for_runtime(runtime)
            result, error_type, logs = executor.execute(
                code_zip=code_zip,
                handler=handler,
                event=payload,
                function_name=function_name,
                timeout=timeout,
                memory_size=memory_size,
                env_vars=env_vars,
                region=region,
                account_id=account_id,
                layer_zips=layer_zips if layer_zips else None,
            )

        if callback:
            try:
                callback(result, error_type, logs)
            except Exception:
                logger.exception("Lambda invoke callback failed for %s", function_name)

        if error_type:
            logger.warning("Lambda %s returned error %s", function_name, error_type)
        else:
            logger.debug("Lambda %s invoked successfully", function_name)

        return result, error_type, logs
    finally:
        decrement_depth(account_id, region, function_name)
