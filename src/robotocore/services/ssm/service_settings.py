"""SSM service settings (Get/Update/ResetServiceSetting), which Moto does not store.

Moto's GetServiceSetting always answers the default with an empty value, its
UpdateServiceSetting is a no-op, and passing the setting's ARN as ``SettingId`` (Terraform does on
refresh) yields an ARN nested inside an ARN. Settings are stored per account and region here.
"""

import json
import threading
from datetime import UTC, datetime

from starlette.responses import Response

_lock = threading.Lock()
# (account, region, setting path) -> (value, last-modified epoch)
_settings: dict[tuple[str, str, str], tuple[str, float]] = {}

# Documented AWS defaults for settings Terraform users commonly manage.
_DEFAULTS = {
    "/ssm/parameter-store/high-throughput-enabled": "false",
    "/ssm/parameter-store/default-parameter-tier": "Standard",
    "/ssm/automation/customer-script-log-destination": "CloudWatch",
    "/ssm/managed-instance/activation-tier": "standard",
    "/ssm/opsinsights/opscenter": "Disabled",
    "/ssm/documents/console/public-sharing-permission": "Enable",
}


def _path(setting_id: str) -> str:
    """'/ssm/x' from either the path form or arn:...:servicesetting/ssm/x."""
    if setting_id.startswith("arn:") and ":servicesetting/" in setting_id:
        return "/" + setting_id.split(":servicesetting/", 1)[1].lstrip("/")
    return setting_id


def _json(data: dict, status: int = 200) -> Response:
    return Response(
        content=json.dumps(data), status_code=status, media_type="application/x-amz-json-1.1"
    )


def _setting(path: str, region: str, account_id: str, partition: str = "aws") -> dict:
    with _lock:
        stored = _settings.get((account_id, region, path))
    arn = f"arn:{partition}:ssm:{region}:{account_id}:servicesetting{path}"
    if stored is None:
        return {
            "SettingId": path,
            "SettingValue": _DEFAULTS.get(path, ""),
            "LastModifiedDate": 0,
            "LastModifiedUser": "System",
            "ARN": arn,
            "Status": "Default",
        }
    value, modified = stored
    return {
        "SettingId": path,
        "SettingValue": value,
        "LastModifiedDate": modified,
        "LastModifiedUser": f"arn:{partition}:iam::{account_id}:root",
        "ARN": arn,
        "Status": "Customized",
    }


def handle(action: str, params: dict, region: str, account_id: str) -> Response | None:
    setting_id = params.get("SettingId", "")
    if action not in ("GetServiceSetting", "UpdateServiceSetting", "ResetServiceSetting"):
        return None
    if not setting_id:
        return _json(
            {"__type": "ValidationException", "message": "SettingId is required"}, status=400
        )
    path = _path(setting_id)
    if action == "UpdateServiceSetting":
        with _lock:
            _settings[(account_id, region, path)] = (
                str(params.get("SettingValue", "")),
                datetime.now(UTC).timestamp(),
            )
        return _json({})
    if action == "ResetServiceSetting":
        with _lock:
            _settings.pop((account_id, region, path), None)
    return _json({"ServiceSetting": _setting(path, region, account_id)})
