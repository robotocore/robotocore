"""Native ECS/Fargate provider.

JSON protocol via X-Amz-Target: AmazonEC2ContainerServiceV20141113.{Action}.
"""

import json
import threading
import time
import uuid
from collections.abc import Callable

from starlette.requests import Request
from starlette.responses import Response

# ---------------------------------------------------------------------------
# In-memory stores (region-scoped)
# ---------------------------------------------------------------------------

_stores: dict[str, "EcsStore"] = {}
_lock = threading.RLock()


class EcsStore:
    """Per-region in-memory store for ECS resources."""

    def __init__(self, region: str, account_id: str) -> None:
        self.region = region
        self.account_id = account_id
        self.clusters: dict[str, dict] = {}  # cluster_name -> cluster
        self.task_definitions: dict[str, list[dict]] = {}  # family -> [revisions]
        self.services: dict[str, dict[str, dict]] = {}  # cluster -> service_name -> svc
        self.tasks: dict[str, dict[str, dict]] = {}  # cluster -> task_id -> task
        self.tags: dict[str, list[dict]] = {}  # arn -> [tags]
        self.lock = threading.RLock()


def _get_store(
    region: str = "us-east-1", account_id: str = "123456789012"
) -> EcsStore:
    with _lock:
        if region not in _stores:
            _stores[region] = EcsStore(region, account_id)
        return _stores[region]


def _new_id() -> str:
    return str(uuid.uuid4())


# ---------------------------------------------------------------------------
# Error handling
# ---------------------------------------------------------------------------


class EcsError(Exception):
    def __init__(self, code: str, message: str, status: int = 400):
        self.code = code
        self.message = message
        self.status = status


# ---------------------------------------------------------------------------
# Main handler
# ---------------------------------------------------------------------------


async def handle_ecs_request(
    request: Request, region: str, account_id: str
) -> Response:
    """Handle an ECS API request."""
    body = await request.body()
    target = request.headers.get("x-amz-target", "")

    if not target:
        return _error("InvalidAction", "Missing X-Amz-Target header", 400)

    action = target.split(".")[-1]
    params = json.loads(body) if body else {}

    store = _get_store(region, account_id)
    handler = _ACTION_MAP.get(action)
    if handler is None:
        return _error("InvalidAction", f"Unknown action: {action}", 400)

    try:
        result = handler(store, params, region, account_id)
        return _json_response(result)
    except EcsError as e:
        return _error(e.code, e.message, e.status)
    except Exception as e:
        return _error("ServerException", str(e), 500)


# ---------------------------------------------------------------------------
# Cluster CRUD
# ---------------------------------------------------------------------------


def _create_cluster(
    store: EcsStore, params: dict, region: str, account_id: str
) -> dict:
    name = params.get("clusterName", "default")
    arn = f"arn:aws:ecs:{region}:{account_id}:cluster/{name}"

    cluster = {
        "clusterArn": arn,
        "clusterName": name,
        "status": "ACTIVE",
        "registeredContainerInstancesCount": 0,
        "runningTasksCount": 0,
        "pendingTasksCount": 0,
        "activeServicesCount": 0,
        "settings": params.get("settings", []),
        "capacityProviders": params.get("capacityProviders", []),
        "tags": params.get("tags", []),
    }

    with store.lock:
        store.clusters[name] = cluster
        store.services.setdefault(name, {})
        store.tasks.setdefault(name, {})
        if cluster["tags"]:
            store.tags[arn] = cluster["tags"]

    return {"cluster": cluster}


def _describe_clusters(
    store: EcsStore, params: dict, region: str, account_id: str
) -> dict:
    cluster_ids = params.get("clusters", [])
    failures = []
    found = []

    with store.lock:
        for cid in cluster_ids:
            # Accept both ARN and name
            name = cid.split("/")[-1] if "/" in cid else cid
            cluster = store.clusters.get(name)
            if cluster:
                # Update dynamic counts
                cluster["activeServicesCount"] = len(
                    store.services.get(name, {})
                )
                cluster["runningTasksCount"] = sum(
                    1
                    for t in store.tasks.get(name, {}).values()
                    if t["lastStatus"] == "RUNNING"
                )
                found.append(cluster)
            else:
                failures.append({
                    "arn": cid,
                    "reason": "MISSING",
                })

    return {"clusters": found, "failures": failures}


def _list_clusters(
    store: EcsStore, params: dict, region: str, account_id: str
) -> dict:
    with store.lock:
        arns = [c["clusterArn"] for c in store.clusters.values()]
    return {"clusterArns": arns}


def _delete_cluster(
    store: EcsStore, params: dict, region: str, account_id: str
) -> dict:
    cluster_id = params.get("cluster", "")
    name = cluster_id.split("/")[-1] if "/" in cluster_id else cluster_id

    with store.lock:
        cluster = store.clusters.get(name)
        if not cluster:
            raise EcsError(
                "ClusterNotFoundException",
                f"Cluster {cluster_id} not found.",
                404,
            )
        cluster["status"] = "INACTIVE"
        del store.clusters[name]

    return {"cluster": cluster}


# ---------------------------------------------------------------------------
# Task Definitions
# ---------------------------------------------------------------------------


def _register_task_definition(
    store: EcsStore, params: dict, region: str, account_id: str
) -> dict:
    family = params.get("family", "")
    if not family:
        raise EcsError("ClientException", "family is required.")

    with store.lock:
        revisions = store.task_definitions.setdefault(family, [])
        revision = len(revisions) + 1

    td_arn = f"arn:aws:ecs:{region}:{account_id}:task-definition/{family}:{revision}"

    td = {
        "taskDefinitionArn": td_arn,
        "family": family,
        "revision": revision,
        "status": "ACTIVE",
        "containerDefinitions": params.get("containerDefinitions", []),
        "cpu": params.get("cpu", "256"),
        "memory": params.get("memory", "512"),
        "networkMode": params.get("networkMode", "awsvpc"),
        "requiresCompatibilities": params.get("requiresCompatibilities", ["FARGATE"]),
        "executionRoleArn": params.get("executionRoleArn", ""),
        "taskRoleArn": params.get("taskRoleArn", ""),
        "volumes": params.get("volumes", []),
        "tags": params.get("tags", []),
    }

    with store.lock:
        revisions.append(td)
        if td["tags"]:
            store.tags[td_arn] = td["tags"]

    return {"taskDefinition": td, "tags": td.get("tags", [])}


def _describe_task_definition(
    store: EcsStore, params: dict, region: str, account_id: str
) -> dict:
    td_ref = params.get("taskDefinition", "")
    td = _resolve_task_definition(store, td_ref)
    if not td:
        raise EcsError(
            "ClientException",
            f"Unable to describe task definition {td_ref}.",
        )
    return {"taskDefinition": td, "tags": td.get("tags", [])}


def _list_task_definitions(
    store: EcsStore, params: dict, region: str, account_id: str
) -> dict:
    family_prefix = params.get("familyPrefix", "")
    status_filter = params.get("status", "ACTIVE")

    arns = []
    with store.lock:
        for family, revisions in store.task_definitions.items():
            if family_prefix and not family.startswith(family_prefix):
                continue
            for td in revisions:
                if td["status"] == status_filter:
                    arns.append(td["taskDefinitionArn"])

    return {"taskDefinitionArns": arns}


def _list_task_definition_families(
    store: EcsStore, params: dict, region: str, account_id: str
) -> dict:
    family_prefix = params.get("familyPrefix", "")
    status_filter = params.get("status", "ACTIVE")

    families = []
    with store.lock:
        for family, revisions in store.task_definitions.items():
            if family_prefix and not family.startswith(family_prefix):
                continue
            if status_filter == "ACTIVE" and any(td["status"] == "ACTIVE" for td in revisions):
                families.append(family)
            elif status_filter == "INACTIVE" and any(td["status"] == "INACTIVE" for td in revisions):
                families.append(family)
            elif status_filter == "ALL":
                families.append(family)

    return {"families": sorted(families)}


def _deregister_task_definition(
    store: EcsStore, params: dict, region: str, account_id: str
) -> dict:
    td_ref = params.get("taskDefinition", "")
    td = _resolve_task_definition(store, td_ref)
    if not td:
        raise EcsError(
            "ClientException",
            f"Unable to deregister task definition {td_ref}.",
        )
    td["status"] = "INACTIVE"
    return {"taskDefinition": td}


# ---------------------------------------------------------------------------
# Services
# ---------------------------------------------------------------------------


def _create_service(
    store: EcsStore, params: dict, region: str, account_id: str
) -> dict:
    cluster_name = _resolve_cluster_name(params.get("cluster", "default"))
    _require_cluster(store, cluster_name)

    svc_name = params.get("serviceName", "")
    if not svc_name:
        raise EcsError("ClientException", "serviceName is required.")

    svc_arn = f"arn:aws:ecs:{region}:{account_id}:service/{cluster_name}/{svc_name}"

    service = {
        "serviceArn": svc_arn,
        "serviceName": svc_name,
        "clusterArn": store.clusters[cluster_name]["clusterArn"],
        "taskDefinition": params.get("taskDefinition", ""),
        "desiredCount": params.get("desiredCount", 1),
        "runningCount": 0,
        "pendingCount": 0,
        "status": "ACTIVE",
        "launchType": params.get("launchType", "FARGATE"),
        "networkConfiguration": params.get("networkConfiguration", {}),
        "loadBalancers": params.get("loadBalancers", []),
        "createdAt": time.time(),
        "tags": params.get("tags", []),
    }

    with store.lock:
        services = store.services.setdefault(cluster_name, {})
        if svc_name in services:
            raise EcsError(
                "ClientException",
                f"Service {svc_name} already exists.",
            )
        services[svc_name] = service
        if service["tags"]:
            store.tags[svc_arn] = service["tags"]

    return {"service": service}


def _describe_services(
    store: EcsStore, params: dict, region: str, account_id: str
) -> dict:
    cluster_name = _resolve_cluster_name(params.get("cluster", "default"))
    service_ids = params.get("services", [])
    failures = []
    found = []

    with store.lock:
        cluster_services = store.services.get(cluster_name, {})
        for sid in service_ids:
            name = sid.split("/")[-1] if "/" in sid else sid
            svc = cluster_services.get(name)
            if svc:
                found.append(svc)
            else:
                failures.append({"arn": sid, "reason": "MISSING"})

    return {"services": found, "failures": failures}


def _list_services(
    store: EcsStore, params: dict, region: str, account_id: str
) -> dict:
    cluster_name = _resolve_cluster_name(params.get("cluster", "default"))

    with store.lock:
        services = store.services.get(cluster_name, {})
        arns = [s["serviceArn"] for s in services.values()]
    return {"serviceArns": arns}


def _update_service(
    store: EcsStore, params: dict, region: str, account_id: str
) -> dict:
    cluster_name = _resolve_cluster_name(params.get("cluster", "default"))
    svc_name = params.get("service", "")
    svc_name = svc_name.split("/")[-1] if "/" in svc_name else svc_name

    with store.lock:
        services = store.services.get(cluster_name, {})
        svc = services.get(svc_name)
        if not svc:
            raise EcsError(
                "ServiceNotFoundException",
                f"Service {svc_name} not found.",
                404,
            )
        if "desiredCount" in params:
            svc["desiredCount"] = params["desiredCount"]
        if "taskDefinition" in params:
            svc["taskDefinition"] = params["taskDefinition"]

    return {"service": svc}


def _delete_service(
    store: EcsStore, params: dict, region: str, account_id: str
) -> dict:
    cluster_name = _resolve_cluster_name(params.get("cluster", "default"))
    svc_name = params.get("service", "")
    svc_name = svc_name.split("/")[-1] if "/" in svc_name else svc_name

    with store.lock:
        services = store.services.get(cluster_name, {})
        svc = services.get(svc_name)
        if not svc:
            raise EcsError(
                "ServiceNotFoundException",
                f"Service {svc_name} not found.",
                404,
            )
        svc["status"] = "INACTIVE"
        svc["desiredCount"] = 0
        del services[svc_name]

    return {"service": svc}


# ---------------------------------------------------------------------------
# Tasks
# ---------------------------------------------------------------------------


def _run_task(
    store: EcsStore, params: dict, region: str, account_id: str
) -> dict:
    cluster_name = _resolve_cluster_name(params.get("cluster", "default"))
    _require_cluster(store, cluster_name)

    td_ref = params.get("taskDefinition", "")
    td = _resolve_task_definition(store, td_ref)
    if not td:
        raise EcsError("ClientException", f"Task definition {td_ref} not found.")

    count = params.get("count", 1)
    tasks = []

    with store.lock:
        for _ in range(count):
            task_id = _new_id()
            task_arn = (
                f"arn:aws:ecs:{region}:{account_id}:task/{cluster_name}/{task_id}"
            )
            task = {
                "taskArn": task_arn,
                "taskDefinitionArn": td["taskDefinitionArn"],
                "clusterArn": store.clusters[cluster_name]["clusterArn"],
                "lastStatus": "RUNNING",
                "desiredStatus": "RUNNING",
                "cpu": td.get("cpu", "256"),
                "memory": td.get("memory", "512"),
                "launchType": params.get("launchType", "FARGATE"),
                "startedAt": time.time(),
                "createdAt": time.time(),
                "containers": [
                    {
                        "containerArn": (
                            f"arn:aws:ecs:{region}:{account_id}"
                            f":container/{_new_id()}"
                        ),
                        "name": cd.get("name", ""),
                        "lastStatus": "RUNNING",
                    }
                    for cd in td.get("containerDefinitions", [])
                ],
                "tags": params.get("tags", []),
                "overrides": params.get("overrides", {}),
            }
            store.tasks.setdefault(cluster_name, {})[task_id] = task
            if task["tags"]:
                store.tags[task_arn] = task["tags"]
            tasks.append(task)

    return {"tasks": tasks, "failures": []}


def _describe_tasks(
    store: EcsStore, params: dict, region: str, account_id: str
) -> dict:
    cluster_name = _resolve_cluster_name(params.get("cluster", "default"))
    task_ids = params.get("tasks", [])
    found = []
    failures = []

    with store.lock:
        cluster_tasks = store.tasks.get(cluster_name, {})
        for tid in task_ids:
            # Extract task ID from ARN if needed
            task_id = tid.split("/")[-1] if "/" in tid else tid
            task = cluster_tasks.get(task_id)
            if task:
                found.append(task)
            else:
                failures.append({"arn": tid, "reason": "MISSING"})

    return {"tasks": found, "failures": failures}


def _list_tasks(
    store: EcsStore, params: dict, region: str, account_id: str
) -> dict:
    cluster_name = _resolve_cluster_name(params.get("cluster", "default"))
    service_name = params.get("serviceName")
    status_filter = params.get("desiredStatus", "RUNNING")

    with store.lock:
        tasks = store.tasks.get(cluster_name, {})
        arns = []
        for task in tasks.values():
            if task["desiredStatus"] == status_filter:
                if service_name is None:
                    arns.append(task["taskArn"])
                # Service filtering would need task-to-service mapping
                elif not service_name:
                    arns.append(task["taskArn"])

    return {"taskArns": arns}


def _stop_task(
    store: EcsStore, params: dict, region: str, account_id: str
) -> dict:
    cluster_name = _resolve_cluster_name(params.get("cluster", "default"))
    task_id_ref = params.get("task", "")
    task_id = task_id_ref.split("/")[-1] if "/" in task_id_ref else task_id_ref

    with store.lock:
        tasks = store.tasks.get(cluster_name, {})
        task = tasks.get(task_id)
        if not task:
            raise EcsError(
                "InvalidParameterException",
                f"Task {task_id_ref} not found.",
            )
        task["lastStatus"] = "STOPPED"
        task["desiredStatus"] = "STOPPED"
        task["stoppedAt"] = time.time()
        task["stoppedReason"] = params.get("reason", "Task stopped by user")

    return {"task": task}


# ---------------------------------------------------------------------------
# Tagging
# ---------------------------------------------------------------------------


def _tag_resource(
    store: EcsStore, params: dict, region: str, account_id: str
) -> dict:
    arn = params.get("resourceArn", "")
    new_tags = params.get("tags", [])
    with store.lock:
        existing = store.tags.setdefault(arn, [])
        # Merge: overwrite existing keys
        existing_keys = {t["key"]: i for i, t in enumerate(existing)}
        for tag in new_tags:
            if tag["key"] in existing_keys:
                existing[existing_keys[tag["key"]]] = tag
            else:
                existing.append(tag)
    return {}


def _untag_resource(
    store: EcsStore, params: dict, region: str, account_id: str
) -> dict:
    arn = params.get("resourceArn", "")
    keys_to_remove = params.get("tagKeys", [])
    with store.lock:
        existing = store.tags.get(arn, [])
        store.tags[arn] = [t for t in existing if t["key"] not in keys_to_remove]
    return {}


def _list_tags_for_resource(
    store: EcsStore, params: dict, region: str, account_id: str
) -> dict:
    arn = params.get("resourceArn", "")
    with store.lock:
        tags = store.tags.get(arn, [])
    return {"tags": list(tags)}


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------


def _resolve_cluster_name(cluster_ref: str) -> str:
    return cluster_ref.split("/")[-1] if "/" in cluster_ref else cluster_ref


def _require_cluster(store: EcsStore, cluster_name: str) -> None:
    with store.lock:
        if cluster_name not in store.clusters:
            raise EcsError(
                "ClusterNotFoundException",
                f"Cluster {cluster_name} not found.",
                404,
            )


def _resolve_task_definition(store: EcsStore, ref: str) -> dict | None:
    """Resolve a task definition reference (family, family:revision, or ARN)."""
    with store.lock:
        # ARN format: .../family:revision
        if ref.startswith("arn:"):
            parts = ref.split("/")[-1]
        else:
            parts = ref

        if ":" in parts:
            family, rev_str = parts.rsplit(":", 1)
            try:
                rev = int(rev_str)
            except ValueError:
                return None
            revisions = store.task_definitions.get(family, [])
            for td in revisions:
                if td["revision"] == rev:
                    return td
            return None
        else:
            family = parts
            revisions = store.task_definitions.get(family, [])
            # Return latest active revision
            for td in reversed(revisions):
                if td["status"] == "ACTIVE":
                    return td
            return None


# ---------------------------------------------------------------------------
# Response helpers
# ---------------------------------------------------------------------------


def _json_response(data: dict) -> Response:
    return Response(
        content=json.dumps(data, default=str),
        status_code=200,
        media_type="application/x-amz-json-1.1",
    )


def _error(code: str, message: str, status: int) -> Response:
    body = json.dumps({"__type": code, "message": message})
    return Response(
        content=body, status_code=status, media_type="application/x-amz-json-1.1"
    )


# ---------------------------------------------------------------------------
# Action map
# ---------------------------------------------------------------------------

_ACTION_MAP: dict[str, Callable] = {
    "CreateCluster": _create_cluster,
    "DescribeClusters": _describe_clusters,
    "ListClusters": _list_clusters,
    "DeleteCluster": _delete_cluster,
    "RegisterTaskDefinition": _register_task_definition,
    "DescribeTaskDefinition": _describe_task_definition,
    "ListTaskDefinitions": _list_task_definitions,
    "ListTaskDefinitionFamilies": _list_task_definition_families,
    "DeregisterTaskDefinition": _deregister_task_definition,
    "CreateService": _create_service,
    "DescribeServices": _describe_services,
    "ListServices": _list_services,
    "UpdateService": _update_service,
    "DeleteService": _delete_service,
    "RunTask": _run_task,
    "DescribeTasks": _describe_tasks,
    "ListTasks": _list_tasks,
    "StopTask": _stop_task,
    "TagResource": _tag_resource,
    "UntagResource": _untag_resource,
    "ListTagsForResource": _list_tags_for_resource,
}
