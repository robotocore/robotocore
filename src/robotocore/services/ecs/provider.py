"""Native ECS/Fargate provider.

JSON protocol via X-Amz-Target: AmazonEC2ContainerServiceV20141113.{Action}.
"""

import json
import logging
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


logger = logging.getLogger(__name__)


class EcsStore:
    """Per-region in-memory store for ECS resources."""

    def __init__(self, region: str, account_id: str) -> None:
        self.region = region
        self.account_id = account_id
        self.clusters: dict[str, dict] = {}  # cluster_name -> cluster
        self.task_definitions: dict[str, list[dict]] = {}  # family -> [revisions]
        self.services: dict[str, dict[str, dict]] = {}  # cluster -> service_name -> svc
        self.tasks: dict[str, dict[str, dict]] = {}  # cluster -> task_id -> task
        self.container_instances: dict[str, dict[str, dict]] = {}  # cluster -> ci_id -> ci
        self.task_sets: dict[str, dict[str, dict]] = {}  # service_arn -> ts_id -> task_set
        self.attributes: dict[str, list[dict]] = {}  # cluster -> [attributes]
        self.tags: dict[str, list[dict]] = {}  # arn -> [tags]
        self.lock = threading.RLock()


def _get_store(region: str = "us-east-1", account_id: str = "123456789012") -> EcsStore:
    key = f"{account_id}:{region}"
    with _lock:
        if key not in _stores:
            _stores[key] = EcsStore(region, account_id)
        return _stores[key]


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


async def handle_ecs_request(request: Request, region: str, account_id: str) -> Response:
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
        from robotocore.providers.moto_bridge import forward_to_moto

        return await forward_to_moto(request, "ecs", account_id=account_id)

    try:
        result = handler(store, params, region, account_id)
        return _json_response(result)
    except EcsError as e:
        return _error(e.code, e.message, e.status)
    except Exception as e:  # noqa: BLE001
        return _error("ServerException", str(e), 500)


# ---------------------------------------------------------------------------
# Cluster CRUD
# ---------------------------------------------------------------------------


def _create_cluster(store: EcsStore, params: dict, region: str, account_id: str) -> dict:
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


def _describe_clusters(store: EcsStore, params: dict, region: str, account_id: str) -> dict:
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
                cluster["activeServicesCount"] = len(store.services.get(name, {}))
                cluster["runningTasksCount"] = sum(
                    1 for t in store.tasks.get(name, {}).values() if t["lastStatus"] == "RUNNING"
                )
                found.append(cluster)
            else:
                failures.append(
                    {
                        "arn": cid,
                        "reason": "MISSING",
                    }
                )

    return {"clusters": found, "failures": failures}


def _list_clusters(store: EcsStore, params: dict, region: str, account_id: str) -> dict:
    with store.lock:
        arns = [c["clusterArn"] for c in store.clusters.values()]
    return {"clusterArns": arns}


def _delete_cluster(store: EcsStore, params: dict, region: str, account_id: str) -> dict:
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

        # Cascade: clean up child service/task tags, then remove children
        for svc in store.services.get(name, {}).values():
            store.tags.pop(svc.get("serviceArn", ""), None)
        for task in store.tasks.get(name, {}).values():
            store.tags.pop(task.get("taskArn", ""), None)
        store.services.pop(name, None)
        store.tasks.pop(name, None)

        # Clean up cluster's own tags
        store.tags.pop(cluster["clusterArn"], None)

        del store.clusters[name]

    return {"cluster": cluster}


# ---------------------------------------------------------------------------
# Task Definitions
# ---------------------------------------------------------------------------


def _register_task_definition(store: EcsStore, params: dict, region: str, account_id: str) -> dict:
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

        revisions.append(td)
        if td["tags"]:
            store.tags[td_arn] = td["tags"]

    return {"taskDefinition": td, "tags": td.get("tags", [])}


def _describe_task_definition(store: EcsStore, params: dict, region: str, account_id: str) -> dict:
    td_ref = params.get("taskDefinition", "")
    td = _resolve_task_definition(store, td_ref)
    if not td:
        raise EcsError(
            "ClientException",
            f"Unable to describe task definition {td_ref}.",
        )
    return {"taskDefinition": td, "tags": td.get("tags", [])}


def _list_task_definitions(store: EcsStore, params: dict, region: str, account_id: str) -> dict:
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
            elif status_filter == "INACTIVE" and any(
                td["status"] == "INACTIVE" for td in revisions
            ):
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


def _create_service(store: EcsStore, params: dict, region: str, account_id: str) -> dict:
    cluster_name = _resolve_cluster_name(params.get("cluster", "default"))
    _require_cluster(store, cluster_name)

    svc_name = params.get("serviceName", "")
    if not svc_name:
        raise EcsError("ClientException", "serviceName is required.")

    svc_arn = f"arn:aws:ecs:{region}:{account_id}:service/{cluster_name}/{svc_name}"

    now = time.time()
    dep_id = f"ecs-svc/{_new_id().replace('-', '')[:20]}"
    initial_deployment = {
        "id": dep_id,
        "status": "PRIMARY",
        "taskDefinition": params.get("taskDefinition", ""),
        "desiredCount": params.get("desiredCount", 1),
        "runningCount": 0,
        "pendingCount": 0,
        "createdAt": now,
        "updatedAt": now,
    }

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
        "deployments": [initial_deployment],
        "createdAt": now,
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


def _describe_services(store: EcsStore, params: dict, region: str, account_id: str) -> dict:
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


def _list_services(store: EcsStore, params: dict, region: str, account_id: str) -> dict:
    cluster_name = _resolve_cluster_name(params.get("cluster", "default"))

    with store.lock:
        services = store.services.get(cluster_name, {})
        arns = [s["serviceArn"] for s in services.values()]
    return {"serviceArns": arns}


def _update_service(store: EcsStore, params: dict, region: str, account_id: str) -> dict:
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


def _delete_service(store: EcsStore, params: dict, region: str, account_id: str) -> dict:
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
        store.tags.pop(svc.get("serviceArn", ""), None)
        del services[svc_name]

    return {"service": svc}


# ---------------------------------------------------------------------------
# Tasks
# ---------------------------------------------------------------------------


def _run_task(store: EcsStore, params: dict, region: str, account_id: str) -> dict:
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
            task_arn = f"arn:aws:ecs:{region}:{account_id}:task/{cluster_name}/{task_id}"
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
                            f"arn:aws:ecs:{region}:{account_id}:container/{_new_id()}"
                        ),
                        "name": cd.get("name", ""),
                        "image": cd.get("image", ""),
                        "lastStatus": "RUNNING",
                        **({"portMappings": cd["portMappings"]} if "portMappings" in cd else {}),
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


def _describe_tasks(store: EcsStore, params: dict, region: str, account_id: str) -> dict:
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


def _list_tasks(store: EcsStore, params: dict, region: str, account_id: str) -> dict:
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


def _stop_task(store: EcsStore, params: dict, region: str, account_id: str) -> dict:
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


def _tag_resource(store: EcsStore, params: dict, region: str, account_id: str) -> dict:
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
        # Sync inline resource tags
        resource = _find_resource_by_arn(store, arn)
        if resource is not None:
            resource["tags"] = list(existing)
    return {}


def _untag_resource(store: EcsStore, params: dict, region: str, account_id: str) -> dict:
    arn = params.get("resourceArn", "")
    keys_to_remove = set(params.get("tagKeys", []))
    with store.lock:
        existing = store.tags.get(arn, [])
        store.tags[arn] = [t for t in existing if t["key"] not in keys_to_remove]
        # Sync inline resource tags
        resource = _find_resource_by_arn(store, arn)
        if resource is not None:
            resource["tags"] = list(store.tags[arn])
    return {}


def _list_tags_for_resource(store: EcsStore, params: dict, region: str, account_id: str) -> dict:
    arn = params.get("resourceArn", "")
    with store.lock:
        tags = store.tags.get(arn, [])
    return {"tags": list(tags)}


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------


def _find_resource_by_arn(store: EcsStore, arn: str) -> dict | None:
    """Find a resource dict by ARN so inline tags can be synced.

    Must be called while holding store.lock.
    """
    # Cluster
    for cluster in store.clusters.values():
        if cluster.get("clusterArn") == arn:
            return cluster
    # Service
    for svc_map in store.services.values():
        for svc in svc_map.values():
            if svc.get("serviceArn") == arn:
                return svc
    # Task
    for task_map in store.tasks.values():
        for task in task_map.values():
            if task.get("taskArn") == arn:
                return task
    # Task definition
    for revisions in store.task_definitions.values():
        for td in revisions:
            if td.get("taskDefinitionArn") == arn:
                return td
    return None


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


# ---------------------------------------------------------------------------
# Container Instances
# ---------------------------------------------------------------------------


def _register_container_instance(
    store: EcsStore, params: dict, region: str, account_id: str
) -> dict:
    cluster_name = _resolve_cluster_name(params.get("cluster", "default"))
    _require_cluster(store, cluster_name)

    doc_str = params.get("instanceIdentityDocument", "{}")
    doc = json.loads(doc_str) if isinstance(doc_str, str) else doc_str
    ec2_instance_id = doc.get("instanceId", f"i-{_new_id()[:17]}")

    ci_id = _new_id()
    ci_arn = f"arn:aws:ecs:{region}:{account_id}:container-instance/{cluster_name}/{ci_id}"

    ci = {
        "containerInstanceArn": ci_arn,
        "ec2InstanceId": ec2_instance_id,
        "status": "ACTIVE",
        "registeredResources": [
            {"name": "CPU", "type": "INTEGER", "integerValue": 2048},
            {"name": "MEMORY", "type": "INTEGER", "integerValue": 3768},
        ],
        "remainingResources": [
            {"name": "CPU", "type": "INTEGER", "integerValue": 2048},
            {"name": "MEMORY", "type": "INTEGER", "integerValue": 3768},
        ],
        "runningTasksCount": 0,
        "pendingTasksCount": 0,
        "agentConnected": True,
        "attributes": params.get("attributes", []),
        "tags": params.get("tags", []),
    }

    with store.lock:
        store.container_instances.setdefault(cluster_name, {})[ci_id] = ci
        store.clusters[cluster_name]["registeredContainerInstancesCount"] += 1
        if ci["tags"]:
            store.tags[ci_arn] = ci["tags"]

    return {"containerInstance": ci}


def _deregister_container_instance(
    store: EcsStore, params: dict, region: str, account_id: str
) -> dict:
    cluster_name = _resolve_cluster_name(params.get("cluster", "default"))
    _require_cluster(store, cluster_name)

    ci_ref = params.get("containerInstance", "")
    ci_id = ci_ref.split("/")[-1] if "/" in ci_ref else ci_ref

    with store.lock:
        ci_map = store.container_instances.get(cluster_name, {})
        ci = ci_map.get(ci_id)
        if not ci:
            raise EcsError(
                "InvalidParameterException",
                f"Container instance {ci_ref} not found.",
            )
        ci["status"] = "INACTIVE"
        ci["agentConnected"] = False
        store.tags.pop(ci.get("containerInstanceArn", ""), None)
        del ci_map[ci_id]
        store.clusters[cluster_name]["registeredContainerInstancesCount"] = max(
            0, store.clusters[cluster_name]["registeredContainerInstancesCount"] - 1
        )

    return {"containerInstance": ci}


def _describe_container_instances(
    store: EcsStore, params: dict, region: str, account_id: str
) -> dict:
    cluster_name = _resolve_cluster_name(params.get("cluster", "default"))
    ci_refs = params.get("containerInstances", [])
    found = []
    failures = []

    with store.lock:
        ci_map = store.container_instances.get(cluster_name, {})
        for ref in ci_refs:
            ci_id = ref.split("/")[-1] if "/" in ref else ref
            ci = ci_map.get(ci_id)
            if ci:
                found.append(ci)
            else:
                failures.append({"arn": ref, "reason": "MISSING"})

    return {"containerInstances": found, "failures": failures}


def _update_container_instances_state(
    store: EcsStore, params: dict, region: str, account_id: str
) -> dict:
    cluster_name = _resolve_cluster_name(params.get("cluster", "default"))
    _require_cluster(store, cluster_name)

    ci_refs = params.get("containerInstances", [])
    new_status = params.get("status", "ACTIVE")
    found = []
    failures = []

    with store.lock:
        ci_map = store.container_instances.get(cluster_name, {})
        for ref in ci_refs:
            ci_id = ref.split("/")[-1] if "/" in ref else ref
            ci = ci_map.get(ci_id)
            if ci:
                ci["status"] = new_status
                found.append(ci)
            else:
                failures.append({"arn": ref, "reason": "MISSING"})

    return {"containerInstances": found, "failures": failures}


def _list_container_instances(store: EcsStore, params: dict, region: str, account_id: str) -> dict:
    cluster_name = _resolve_cluster_name(params.get("cluster", "default"))
    with store.lock:
        ci_map = store.container_instances.get(cluster_name, {})
        arns = [ci["containerInstanceArn"] for ci in ci_map.values()]
    return {"containerInstanceArns": arns}


# ---------------------------------------------------------------------------
# Task Sets
# ---------------------------------------------------------------------------


def _create_task_set(store: EcsStore, params: dict, region: str, account_id: str) -> dict:
    cluster_name = _resolve_cluster_name(params.get("cluster", "default"))
    _require_cluster(store, cluster_name)

    svc_ref = params.get("service", "")
    svc_name = svc_ref.split("/")[-1] if "/" in svc_ref else svc_ref

    with store.lock:
        services = store.services.get(cluster_name, {})
        svc = services.get(svc_name)
        if not svc:
            raise EcsError("ServiceNotFoundException", f"Service {svc_name} not found.", 404)

        svc_arn = svc["serviceArn"]
        ts_id = _new_id()
        ts_arn = f"arn:aws:ecs:{region}:{account_id}:task-set/{cluster_name}/{svc_name}/{ts_id}"

        task_set = {
            "id": ts_id,
            "taskSetArn": ts_arn,
            "serviceArn": svc_arn,
            "clusterArn": store.clusters[cluster_name]["clusterArn"],
            "taskDefinition": params.get("taskDefinition", ""),
            "status": "ACTIVE",
            "computedDesiredCount": 0,
            "pendingCount": 0,
            "runningCount": 0,
            "stabilityStatus": "STEADY_STATE",
            "scale": params.get("scale", {"value": 100.0, "unit": "PERCENT"}),
            "launchType": params.get("launchType", "FARGATE"),
            "networkConfiguration": params.get("networkConfiguration", {}),
            "loadBalancers": params.get("loadBalancers", []),
            "tags": params.get("tags", []),
            "createdAt": time.time(),
        }

        store.task_sets.setdefault(svc_arn, {})[ts_id] = task_set
        if task_set["tags"]:
            store.tags[ts_arn] = task_set["tags"]

    return {"taskSet": task_set}


def _describe_task_sets(store: EcsStore, params: dict, region: str, account_id: str) -> dict:
    cluster_name = _resolve_cluster_name(params.get("cluster", "default"))
    svc_ref = params.get("service", "")
    svc_name = svc_ref.split("/")[-1] if "/" in svc_ref else svc_ref
    ts_refs = params.get("taskSets", [])

    with store.lock:
        services = store.services.get(cluster_name, {})
        svc = services.get(svc_name)
        if not svc:
            raise EcsError("ServiceNotFoundException", f"Service {svc_name} not found.", 404)

        svc_arn = svc["serviceArn"]
        ts_map = store.task_sets.get(svc_arn, {})

        if ts_refs:
            found = []
            for ref in ts_refs:
                ts_id = ref.split("/")[-1] if "/" in ref else ref
                ts = ts_map.get(ts_id)
                if ts:
                    found.append(ts)
        else:
            found = list(ts_map.values())

    return {"taskSets": found}


def _update_task_set(store: EcsStore, params: dict, region: str, account_id: str) -> dict:
    cluster_name = _resolve_cluster_name(params.get("cluster", "default"))
    svc_ref = params.get("service", "")
    svc_name = svc_ref.split("/")[-1] if "/" in svc_ref else svc_ref
    ts_ref = params.get("taskSet", "")
    ts_id = ts_ref.split("/")[-1] if "/" in ts_ref else ts_ref

    with store.lock:
        services = store.services.get(cluster_name, {})
        svc = services.get(svc_name)
        if not svc:
            raise EcsError("ServiceNotFoundException", f"Service {svc_name} not found.", 404)

        svc_arn = svc["serviceArn"]
        ts_map = store.task_sets.get(svc_arn, {})
        ts = ts_map.get(ts_id)
        if not ts:
            raise EcsError("InvalidParameterException", f"Task set {ts_ref} not found.")

        if "scale" in params:
            ts["scale"] = params["scale"]

    return {"taskSet": ts}


def _delete_task_set(store: EcsStore, params: dict, region: str, account_id: str) -> dict:
    cluster_name = _resolve_cluster_name(params.get("cluster", "default"))
    svc_ref = params.get("service", "")
    svc_name = svc_ref.split("/")[-1] if "/" in svc_ref else svc_ref
    ts_ref = params.get("taskSet", "")
    ts_id = ts_ref.split("/")[-1] if "/" in ts_ref else ts_ref

    with store.lock:
        services = store.services.get(cluster_name, {})
        svc = services.get(svc_name)
        if not svc:
            raise EcsError("ServiceNotFoundException", f"Service {svc_name} not found.", 404)

        svc_arn = svc["serviceArn"]
        ts_map = store.task_sets.get(svc_arn, {})
        ts = ts_map.get(ts_id)
        if not ts:
            raise EcsError("InvalidParameterException", f"Task set {ts_ref} not found.")

        ts["status"] = "INACTIVE"
        store.tags.pop(ts.get("taskSetArn", ""), None)
        del ts_map[ts_id]

    return {"taskSet": ts}


# ---------------------------------------------------------------------------
# Attributes
# ---------------------------------------------------------------------------


def _put_attributes(store: EcsStore, params: dict, region: str, account_id: str) -> dict:
    cluster_name = _resolve_cluster_name(params.get("cluster", "default"))
    _require_cluster(store, cluster_name)

    attrs = params.get("attributes", [])
    with store.lock:
        existing = store.attributes.setdefault(cluster_name, [])
        for attr in attrs:
            # Replace existing attribute with same name+targetId+targetType
            key = (attr.get("name"), attr.get("targetId"), attr.get("targetType"))
            existing[:] = [
                a
                for a in existing
                if (a.get("name"), a.get("targetId"), a.get("targetType")) != key
            ]
            existing.append(attr)

    return {"attributes": attrs}


def _delete_attributes(store: EcsStore, params: dict, region: str, account_id: str) -> dict:
    cluster_name = _resolve_cluster_name(params.get("cluster", "default"))
    _require_cluster(store, cluster_name)

    attrs_to_del = params.get("attributes", [])
    with store.lock:
        existing = store.attributes.get(cluster_name, [])
        for attr in attrs_to_del:
            key = (attr.get("name"), attr.get("targetId"), attr.get("targetType"))
            existing[:] = [
                a
                for a in existing
                if (a.get("name"), a.get("targetId"), a.get("targetType")) != key
            ]

    return {"attributes": attrs_to_del}


def _list_attributes(store: EcsStore, params: dict, region: str, account_id: str) -> dict:
    cluster_name = _resolve_cluster_name(params.get("cluster", "default"))
    target_type = params.get("targetType", "")
    attr_name = params.get("attributeName")

    with store.lock:
        all_attrs = store.attributes.get(cluster_name, [])
        filtered = [a for a in all_attrs if not target_type or a.get("targetType") == target_type]
        if attr_name:
            filtered = [a for a in filtered if a.get("name") == attr_name]

    return {"attributes": filtered}


# ---------------------------------------------------------------------------
# Delete Task Definitions (batch)
# ---------------------------------------------------------------------------


def _delete_task_definitions(store: EcsStore, params: dict, region: str, account_id: str) -> dict:
    td_refs = params.get("taskDefinitions", [])
    deleted = []
    failures = []

    with store.lock:
        for ref in td_refs:
            td = _resolve_task_definition(store, ref)
            if td:
                td["status"] = "DELETE_IN_PROGRESS"
                deleted.append(td)
            else:
                failures.append(
                    {
                        "arn": ref,
                        "reason": (
                            "The specified task definition does not exist. "
                            "Specify a valid account, family, revision and try again."
                        ),
                    }
                )

    return {"taskDefinitions": deleted, "failures": failures}


def _put_cluster_capacity_providers(
    store: EcsStore, params: dict, region: str, account_id: str
) -> dict:
    cluster_name = _resolve_cluster_name(params.get("cluster", "default"))
    capacity_providers = params.get("capacityProviders", [])
    default_strategy = params.get("defaultCapacityProviderStrategy", [])
    with store.lock:
        cluster = store.clusters.get(cluster_name)
        if not cluster:
            raise EcsError("ClusterNotFoundException", f"Cluster {cluster_name} not found", 404)
        cluster["capacityProviders"] = capacity_providers
        cluster["defaultCapacityProviderStrategy"] = default_strategy
    return {"cluster": cluster}


def _error(code: str, message: str, status: int) -> Response:
    body = json.dumps({"__type": code, "message": message})
    return Response(content=body, status_code=status, media_type="application/x-amz-json-1.1")


# ---------------------------------------------------------------------------
# Service Deployments & Revisions
# ---------------------------------------------------------------------------


def _list_service_deployments(store: EcsStore, params: dict, region: str, account_id: str) -> dict:
    cluster_name = _resolve_cluster_name(params.get("cluster", "default"))
    _require_cluster(store, cluster_name)
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

    status_filter = params.get("status")
    deployments = []
    for dep in svc.get("deployments", []):
        dep_arn = (
            f"arn:aws:ecs:{region}:{account_id}:"
            f"service-deployment/{cluster_name}/{svc_name}/{dep['id']}"
        )
        entry = {
            "serviceDeploymentArn": dep_arn,
            "serviceArn": svc["serviceArn"],
            "clusterArn": svc["clusterArn"],
            "status": dep.get("status", "PRIMARY"),
            "createdAt": dep.get("createdAt"),
            "targetServiceRevisionArn": (
                f"arn:aws:ecs:{region}:{account_id}:service-revision/{cluster_name}/{svc_name}:1"
            ),
        }
        if status_filter is None or entry["status"] == status_filter:
            deployments.append(entry)
    return {"serviceDeployments": deployments}


def _describe_service_deployments(
    store: EcsStore, params: dict, region: str, account_id: str
) -> dict:
    dep_arns = params.get("serviceDeploymentArns", [])
    results = []
    failures = []
    for dep_arn in dep_arns:
        found = False
        with store.lock:
            for cl_name, services in store.services.items():
                for svc_name, svc in services.items():
                    for dep in svc.get("deployments", []):
                        expected_arn = (
                            f"arn:aws:ecs:{region}:{account_id}:"
                            f"service-deployment/{cl_name}/{svc_name}/{dep['id']}"
                        )
                        if expected_arn == dep_arn:
                            results.append(
                                {
                                    "serviceDeploymentArn": expected_arn,
                                    "serviceArn": svc["serviceArn"],
                                    "clusterArn": svc["clusterArn"],
                                    "status": dep.get("status", "PRIMARY"),
                                    "taskDefinition": dep.get("taskDefinition", ""),
                                    "desiredCount": dep.get("desiredCount", 0),
                                    "runningCount": dep.get("runningCount", 0),
                                    "createdAt": dep.get("createdAt"),
                                    "updatedAt": dep.get("updatedAt"),
                                }
                            )
                            found = True
                            break
                    if found:
                        break
                if found:
                    break
        if not found:
            failures.append({"arn": dep_arn, "reason": "SERVICE_DEPLOYMENT_NOT_FOUND"})
    return {"serviceDeployments": results, "failures": failures}


def _describe_service_revisions(
    store: EcsStore, params: dict, region: str, account_id: str
) -> dict:
    rev_arns = params.get("serviceRevisionArns", [])
    results = []
    failures = []
    for rev_arn in rev_arns:
        found = False
        try:
            parts = rev_arn.split(":")
            resource = parts[5]
            path_parts = resource.split("/")
            cluster_name = path_parts[1]
            svc_name = path_parts[2]
            with store.lock:
                services = store.services.get(cluster_name, {})
                svc = services.get(svc_name)
                if svc:
                    results.append(
                        {
                            "serviceRevisionArn": rev_arn,
                            "serviceArn": svc["serviceArn"],
                            "clusterArn": svc["clusterArn"],
                            "taskDefinition": svc.get("taskDefinition", ""),
                            "desiredCount": svc.get("desiredCount", 0),
                            "runningCount": svc.get("runningCount", 0),
                            "createdAt": svc.get("createdAt"),
                        }
                    )
                    found = True
        except (IndexError, ValueError) as exc:
            logger.debug("_describe_service_revisions: split failed (non-fatal): %s", exc)
        if not found:
            failures.append({"arn": rev_arn, "reason": "SERVICE_REVISION_NOT_FOUND"})
    return {"serviceRevisions": results, "failures": failures}


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
    "DeleteTaskDefinitions": _delete_task_definitions,
    "CreateService": _create_service,
    "DescribeServices": _describe_services,
    "ListServices": _list_services,
    "UpdateService": _update_service,
    "DeleteService": _delete_service,
    "RunTask": _run_task,
    "DescribeTasks": _describe_tasks,
    "ListTasks": _list_tasks,
    "StopTask": _stop_task,
    "RegisterContainerInstance": _register_container_instance,
    "DeregisterContainerInstance": _deregister_container_instance,
    "DescribeContainerInstances": _describe_container_instances,
    "UpdateContainerInstancesState": _update_container_instances_state,
    "ListContainerInstances": _list_container_instances,
    "CreateTaskSet": _create_task_set,
    "DescribeTaskSets": _describe_task_sets,
    "UpdateTaskSet": _update_task_set,
    "DeleteTaskSet": _delete_task_set,
    "PutAttributes": _put_attributes,
    "DeleteAttributes": _delete_attributes,
    "ListAttributes": _list_attributes,
    "TagResource": _tag_resource,
    "UntagResource": _untag_resource,
    "ListTagsForResource": _list_tags_for_resource,
    "PutClusterCapacityProviders": _put_cluster_capacity_providers,
    "ListServiceDeployments": _list_service_deployments,
    "DescribeServiceDeployments": _describe_service_deployments,
    "DescribeServiceRevisions": _describe_service_revisions,
}
