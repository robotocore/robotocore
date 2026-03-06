"""Native EventBridge Scheduler provider.

Implements schedule and schedule group CRUD with REST-JSON protocol.
Schedules can target Lambda, SQS, SNS, and other AWS services.
"""

import json
import re
import threading
import time

from starlette.requests import Request
from starlette.responses import Response

_schedules: dict[str, dict[str, dict]] = {}  # region -> name -> schedule
_groups: dict[str, dict[str, dict]] = {}  # region -> name -> group
_lock = threading.Lock()


class SchedulerError(Exception):
    def __init__(self, code: str, message: str, status: int = 400):
        self.code = code
        self.message = message
        self.status = status


def _get_schedules(region: str) -> dict[str, dict]:
    with _lock:
        if region not in _schedules:
            _schedules[region] = {}
        return _schedules[region]


def _get_groups(region: str) -> dict[str, dict]:
    with _lock:
        if region not in _groups:
            _groups[region] = {}
            # Default group always exists
            _groups[region]["default"] = {
                "Name": "default",
                "Arn": f"arn:aws:scheduler:{region}:123456789012:schedule-group/default",
                "State": "ACTIVE",
                "CreationDate": time.time(),
                "LastModificationDate": time.time(),
            }
        return _groups[region]


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
                return _json_response(_get_schedule_group(group_name, region))
            elif method == "DELETE":
                return _json_response(_delete_schedule_group(group_name, region))

        if _GROUPS_LIST.match(path) and method == "GET":
            return _json_response(_list_schedule_groups(request.query_params, region))

        # Tags
        m = _TAGS_PATH.match(path)
        if m:
            if method == "GET":
                return _json_response({"Tags": []})
            elif method == "POST":
                return _json_response({})
            elif method == "DELETE":
                return _json_response({})

        return _error("InvalidAction", f"Unknown path: {method} {path}", 400)

    except SchedulerError as e:
        return _error(e.code, e.message, e.status)
    except Exception as e:
        return _error("InternalError", str(e), 500)


def _create_schedule(name: str, params: dict, region: str, account_id: str) -> dict:
    schedules = _get_schedules(region)
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
    }

    with _lock:
        schedules[name] = schedule

    return {"ScheduleArn": arn}


def _get_schedule(name: str, params: dict, region: str, account_id: str) -> dict:
    schedules = _get_schedules(region)
    with _lock:
        schedule = schedules.get(name)
    if not schedule:
        raise SchedulerError("ResourceNotFoundException", f"Schedule {name} does not exist.", 404)
    result = dict(schedule)
    result.pop("_tags", None)
    return result


def _update_schedule(name: str, params: dict, region: str, account_id: str) -> dict:
    schedules = _get_schedules(region)
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
    schedules = _get_schedules(region)
    with _lock:
        if name not in schedules:
            raise SchedulerError(
                "ResourceNotFoundException", f"Schedule {name} does not exist.", 404
            )
        del schedules[name]
    return {}


def _list_schedules(query_params, region: str, account_id: str) -> dict:
    schedules = _get_schedules(region)
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
    groups = _get_groups(region)
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


def _get_schedule_group(name: str, region: str) -> dict:
    groups = _get_groups(region)
    with _lock:
        group = groups.get(name)
    if not group:
        raise SchedulerError(
            "ResourceNotFoundException", f"Schedule group {name} does not exist.", 404
        )
    return dict(group)


def _delete_schedule_group(name: str, region: str) -> dict:
    groups = _get_groups(region)
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


def _list_schedule_groups(query_params, region: str) -> dict:
    groups = _get_groups(region)
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


def _json_response(data: dict, status: int = 200) -> Response:
    return Response(
        content=json.dumps(data, default=str),
        status_code=status,
        media_type="application/json",
    )


def _error(code: str, message: str, status: int) -> Response:
    body = json.dumps({"__type": code, "Message": message})
    return Response(content=body, status_code=status, media_type="application/json")
