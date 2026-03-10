"""Native EventBridge Scheduler provider.

Implements schedule and schedule group CRUD with REST-JSON protocol.
Schedules can target Lambda, SQS, SNS, and other AWS services.
"""

import json
import re
import threading
import time
from urllib.parse import unquote

from starlette.requests import Request
from starlette.responses import Response

DEFAULT_ACCOUNT_ID = "123456789012"

_schedules: dict[tuple[str, str], dict[str, dict]] = {}  # (account_id, region) -> name -> schedule
_groups: dict[tuple[str, str], dict[str, dict]] = {}  # (account_id, region) -> name -> group
_lock = threading.Lock()


class SchedulerError(Exception):
    def __init__(self, code: str, message: str, status: int = 400):
        self.code = code
        self.message = message
        self.status = status


def _get_schedules(region: str, account_id: str = DEFAULT_ACCOUNT_ID) -> dict[str, dict]:
    key = (account_id, region)
    with _lock:
        if key not in _schedules:
            _schedules[key] = {}
        return _schedules[key]


def _get_groups(region: str, account_id: str = DEFAULT_ACCOUNT_ID) -> dict[str, dict]:
    key = (account_id, region)
    with _lock:
        if key not in _groups:
            _groups[key] = {}
            # Default group always exists
            _groups[key]["default"] = {
                "Name": "default",
                "Arn": f"arn:aws:scheduler:{region}:{account_id}:schedule-group/default",
                "State": "ACTIVE",
                "CreationDate": time.time(),
                "LastModificationDate": time.time(),
            }
        return _groups[key]


# REST-JSON path patterns
_SCHEDULE_PATH = re.compile(r"^/schedules/([^/?]+)$")
_SCHEDULES_LIST = re.compile(r"^/schedules/?$")
_GROUP_PATH = re.compile(r"^/schedule-groups/([^/?]+)$")
_GROUPS_LIST = re.compile(r"^/schedule-groups/?$")
_TAGS_PATH = re.compile(r"^/tags/(.+)$")


async def handle_scheduler_request(request: Request, region: str, account_id: str) -> Response:
    """Handle an EventBridge Scheduler API request."""
    path = request.url.path
    method = request.method.upper()
    body = await request.body()
    params = json.loads(body) if body else {}

    try:
        # Schedule operations
        m = _SCHEDULE_PATH.match(path)
        if m:
            name = m.group(1)
            if method == "POST":
                return _json_response(_create_schedule(name, params, region, account_id))
            elif method == "GET":
                return _json_response(_get_schedule(name, params, region, account_id))
            elif method == "PUT":
                return _json_response(_update_schedule(name, params, region, account_id))
            elif method == "DELETE":
                return _json_response(_delete_schedule(name, params, region, account_id))

        if _SCHEDULES_LIST.match(path) and method == "GET":
            return _json_response(_list_schedules(request.query_params, region, account_id))

        # Schedule group operations
        m = _GROUP_PATH.match(path)
        if m:
            group_name = m.group(1)
            if method == "POST":
                return _json_response(
                    _create_schedule_group(group_name, params, region, account_id)
                )
            elif method == "GET":
                return _json_response(_get_schedule_group(group_name, region, account_id))
            elif method == "DELETE":
                return _json_response(_delete_schedule_group(group_name, region, account_id))

        if _GROUPS_LIST.match(path) and method == "GET":
            return _json_response(_list_schedule_groups(request.query_params, region, account_id))

        # Tags
        m = _TAGS_PATH.match(path)
        if m:
            resource_arn = unquote(m.group(1))
            if method == "GET":
                return _json_response(
                    {"Tags": _list_tags_scheduler(resource_arn, region, account_id)}
                )
            elif method == "POST":
                new_tags = params.get("Tags", [])
                _tag_resource_scheduler(resource_arn, new_tags, region, account_id)
                return _json_response({})
            elif method == "DELETE":
                tag_keys = request.query_params.getlist("TagKeys")
                _untag_resource_scheduler(resource_arn, tag_keys, region, account_id)
                return _json_response({})

        return _error("InvalidAction", f"Unknown path: {method} {path}", 400)

    except SchedulerError as e:
        return _error(e.code, e.message, e.status)
    except Exception as e:
        return _error("InternalError", str(e), 500)


def _create_schedule(name: str, params: dict, region: str, account_id: str) -> dict:
    schedules = _get_schedules(region, account_id)
    group_name = params.get("GroupName", "default")

    arn = f"arn:aws:scheduler:{region}:{account_id}:schedule/{group_name}/{name}"

    schedule = {
        "Name": name,
        "Arn": arn,
        "GroupName": group_name,
        "ScheduleExpression": params.get("ScheduleExpression", ""),
        "ScheduleExpressionTimezone": params.get("ScheduleExpressionTimezone", "UTC"),
        "FlexibleTimeWindow": params.get("FlexibleTimeWindow", {"Mode": "OFF"}),
        "Target": params.get("Target", {}),
        "State": params.get("State", "ENABLED"),
        "Description": params.get("Description", ""),
        "StartDate": params.get("StartDate"),
        "EndDate": params.get("EndDate"),
        "KmsKeyArn": params.get("KmsKeyArn"),
        "CreationDate": time.time(),
        "LastModificationDate": time.time(),
        "_tags": {t["Key"]: t["Value"] for t in params.get("Tags", [])},
    }

    with _lock:
        schedules[name] = schedule

    return {"ScheduleArn": arn}


def _get_schedule(name: str, params: dict, region: str, account_id: str) -> dict:
    schedules = _get_schedules(region, account_id)
    with _lock:
        schedule = schedules.get(name)
    if not schedule:
        raise SchedulerError("ResourceNotFoundException", f"Schedule {name} does not exist.", 404)
    result = dict(schedule)
    result.pop("_tags", None)
    return result


def _update_schedule(name: str, params: dict, region: str, account_id: str) -> dict:
    schedules = _get_schedules(region, account_id)
    with _lock:
        schedule = schedules.get(name)
        if not schedule:
            raise SchedulerError(
                "ResourceNotFoundException", f"Schedule {name} does not exist.", 404
            )

        if "ScheduleExpression" in params:
            schedule["ScheduleExpression"] = params["ScheduleExpression"]
        if "Target" in params:
            schedule["Target"] = params["Target"]
        if "FlexibleTimeWindow" in params:
            schedule["FlexibleTimeWindow"] = params["FlexibleTimeWindow"]
        if "State" in params:
            schedule["State"] = params["State"]
        if "Description" in params:
            schedule["Description"] = params["Description"]
        schedule["LastModificationDate"] = time.time()

    return {"ScheduleArn": schedule["Arn"]}


def _delete_schedule(name: str, params: dict, region: str, account_id: str) -> dict:
    schedules = _get_schedules(region, account_id)
    with _lock:
        if name not in schedules:
            raise SchedulerError(
                "ResourceNotFoundException", f"Schedule {name} does not exist.", 404
            )
        del schedules[name]
    return {}


def _list_schedules(query_params, region: str, account_id: str) -> dict:
    schedules = _get_schedules(region, account_id)
    group_filter = query_params.get("GroupName")
    name_prefix = query_params.get("NamePrefix")

    with _lock:
        items = list(schedules.values())

    if group_filter:
        items = [s for s in items if s.get("GroupName") == group_filter]
    if name_prefix:
        items = [s for s in items if s["Name"].startswith(name_prefix)]

    summaries = []
    for s in items:
        summaries.append(
            {
                "Name": s["Name"],
                "Arn": s["Arn"],
                "GroupName": s.get("GroupName", "default"),
                "ScheduleExpression": s["ScheduleExpression"],
                "State": s["State"],
                "Target": {"Arn": s["Target"].get("Arn", "")},
                "CreationDate": s["CreationDate"],
                "LastModificationDate": s["LastModificationDate"],
            }
        )

    return {"Schedules": summaries}


def _create_schedule_group(name: str, params: dict, region: str, account_id: str) -> dict:
    groups = _get_groups(region, account_id)
    arn = f"arn:aws:scheduler:{region}:{account_id}:schedule-group/{name}"

    with _lock:
        if name in groups:
            raise SchedulerError("ConflictException", f"Schedule group {name} already exists.", 409)
        groups[name] = {
            "Name": name,
            "Arn": arn,
            "State": "ACTIVE",
            "CreationDate": time.time(),
            "LastModificationDate": time.time(),
        }

    return {"ScheduleGroupArn": arn}


def _get_schedule_group(name: str, region: str, account_id: str = DEFAULT_ACCOUNT_ID) -> dict:
    groups = _get_groups(region, account_id)
    with _lock:
        group = groups.get(name)
    if not group:
        raise SchedulerError(
            "ResourceNotFoundException", f"Schedule group {name} does not exist.", 404
        )
    return dict(group)


def _delete_schedule_group(name: str, region: str, account_id: str = DEFAULT_ACCOUNT_ID) -> dict:
    groups = _get_groups(region, account_id)
    with _lock:
        if name not in groups:
            raise SchedulerError(
                "ResourceNotFoundException", f"Schedule group {name} does not exist.", 404
            )
        if name == "default":
            raise SchedulerError(
                "ValidationException", "Cannot delete the default schedule group.", 400
            )
        del groups[name]
    return {}


def _list_schedule_groups(query_params, region: str, account_id: str = DEFAULT_ACCOUNT_ID) -> dict:
    groups = _get_groups(region, account_id)
    name_prefix = query_params.get("NamePrefix")

    with _lock:
        items = list(groups.values())

    if name_prefix:
        items = [g for g in items if g["Name"].startswith(name_prefix)]

    return {
        "ScheduleGroups": [
            {
                "Name": g["Name"],
                "Arn": g["Arn"],
                "State": g["State"],
                "CreationDate": g["CreationDate"],
                "LastModificationDate": g["LastModificationDate"],
            }
            for g in items
        ]
    }


def _find_resource_by_arn_scheduler(
    resource_arn: str, region: str, account_id: str = DEFAULT_ACCOUNT_ID
) -> dict | None:
    """Find a schedule or group by ARN."""
    schedules = _get_schedules(region, account_id)
    with _lock:
        for s in schedules.values():
            if s.get("Arn") == resource_arn:
                return s
    groups = _get_groups(region, account_id)
    with _lock:
        for g in groups.values():
            if g.get("Arn") == resource_arn:
                return g
    return None


def _list_tags_scheduler(
    resource_arn: str, region: str, account_id: str = DEFAULT_ACCOUNT_ID
) -> list[dict]:
    resource = _find_resource_by_arn_scheduler(resource_arn, region, account_id)
    if resource is None:
        return []
    tags = resource.get("_tags", {})
    return [{"Key": k, "Value": v} for k, v in tags.items()]


def _tag_resource_scheduler(
    resource_arn: str, new_tags: list[dict], region: str, account_id: str = DEFAULT_ACCOUNT_ID
) -> None:
    resource = _find_resource_by_arn_scheduler(resource_arn, region, account_id)
    if resource is None:
        return
    with _lock:
        existing = resource.setdefault("_tags", {})
        for t in new_tags:
            existing[t["Key"]] = t["Value"]


def _untag_resource_scheduler(
    resource_arn: str, tag_keys: list[str], region: str, account_id: str = DEFAULT_ACCOUNT_ID
) -> None:
    resource = _find_resource_by_arn_scheduler(resource_arn, region, account_id)
    if resource is None:
        return
    with _lock:
        tags = resource.get("_tags", {})
        for key in tag_keys:
            tags.pop(key, None)


def _json_response(data: dict, status: int = 200) -> Response:
    return Response(
        content=json.dumps(data, default=str),
        status_code=status,
        media_type="application/json",
    )


def _error(code: str, message: str, status: int) -> Response:
    body = json.dumps({"__type": code, "Message": message})
    return Response(content=body, status_code=status, media_type="application/json")
