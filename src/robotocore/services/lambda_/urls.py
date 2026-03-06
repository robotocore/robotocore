"""Lambda Function URLs — native store for URL configs.

Provides CRUD operations for function URL configurations. Moto has basic support
but we maintain a parallel native store for richer control and to support
function URL invocation routing.
"""

import threading
import time
import uuid

# Store: (account_id, region, func_name) -> url config dict
_url_configs: dict[tuple[str, str, str], dict] = {}
_url_lock = threading.Lock()


def create_function_url_config(
    func_name: str,
    region: str,
    account_id: str,
    config: dict,
) -> dict:
    """Create a function URL config. Raises if one already exists."""
    key = (account_id, region, func_name)
    with _url_lock:
        if key in _url_configs:
            raise FunctionUrlConfigExistsError(func_name)

        url_id = uuid.uuid4().hex[:12]
        now = time.strftime("%Y-%m-%dT%H:%M:%S.000+0000", time.gmtime())
        func_arn = f"arn:aws:lambda:{region}:{account_id}:function:{func_name}"

        url_config = {
            "FunctionUrl": f"https://{url_id}.lambda-url.{region}.on.aws/",
            "FunctionArn": func_arn,
            "AuthType": config.get("AuthType", "NONE"),
            "Cors": config.get("Cors", {}),
            "CreationTime": now,
            "LastModifiedTime": now,
            "InvokeMode": config.get("InvokeMode", "BUFFERED"),
        }
        _url_configs[key] = url_config
        return url_config


def get_function_url_config(
    func_name: str,
    region: str,
    account_id: str,
) -> dict:
    """Get a function URL config. Raises if not found."""
    key = (account_id, region, func_name)
    with _url_lock:
        config = _url_configs.get(key)
    if not config:
        raise FunctionUrlConfigNotFoundError(func_name)
    return config


def update_function_url_config(
    func_name: str,
    region: str,
    account_id: str,
    config: dict,
) -> dict:
    """Update a function URL config. Raises if not found."""
    key = (account_id, region, func_name)
    with _url_lock:
        existing = _url_configs.get(key)
        if not existing:
            raise FunctionUrlConfigNotFoundError(func_name)

        if "AuthType" in config:
            existing["AuthType"] = config["AuthType"]
        if "Cors" in config:
            existing["Cors"] = config["Cors"]
        if "InvokeMode" in config:
            existing["InvokeMode"] = config["InvokeMode"]
        existing["LastModifiedTime"] = time.strftime(
            "%Y-%m-%dT%H:%M:%S.000+0000", time.gmtime()
        )
        return dict(existing)


def delete_function_url_config(
    func_name: str,
    region: str,
    account_id: str,
) -> None:
    """Delete a function URL config. Raises if not found."""
    key = (account_id, region, func_name)
    with _url_lock:
        if key not in _url_configs:
            raise FunctionUrlConfigNotFoundError(func_name)
        del _url_configs[key]


def get_all_url_configs() -> list[dict]:
    """Return all function URL configs (for routing)."""
    with _url_lock:
        return list(_url_configs.values())


def find_function_by_url(url_host: str) -> dict | None:
    """Find a function URL config by the URL host prefix."""
    with _url_lock:
        for config in _url_configs.values():
            func_url = config.get("FunctionUrl", "")
            if url_host in func_url:
                return config
    return None


def clear_store() -> None:
    """Clear all URL configs (for testing)."""
    with _url_lock:
        _url_configs.clear()


class FunctionUrlConfigExistsError(Exception):
    """Raised when a function URL config already exists."""

    def __init__(self, func_name: str):
        super().__init__(
            f"Failed to create function url config for the function {func_name}. "
            "Error: FunctionUrlConfig exists for this Lambda function"
        )


class FunctionUrlConfigNotFoundError(Exception):
    """Raised when a function URL config is not found."""

    def __init__(self, func_name: str):
        super().__init__(
            f"The function url config for function {func_name} does not exist"
        )
