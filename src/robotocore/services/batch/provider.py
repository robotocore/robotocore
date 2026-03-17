"""Native AWS Batch provider.

REST-like path routing (/v1/createcomputeenvironment, /v1/createjobqueue, etc.).
"""

import json
import re
import threading
import time
import uuid
from collections.abc import Callable

from starlette.requests import Request
from starlette.responses import Response

# ---------------------------------------------------------------------------
# In-memory stores (region-scoped)
# ---------------------------------------------------------------------------

_stores: dict[str, "BatchStore"] = {}
_lock = threading.RLock()


class BatchStore:
    """Per-region in-memory store for AWS Batch resources."""

    def __init__(self, region: str, account_id: str) -> None:
        self.region = region
        self.account_id = account_id
        self.compute_envs: dict[str, dict] = {}  # name -> ce
        self.job_queues: dict[str, dict] = {}  # name -> queue
        self.job_definitions: dict[str, list[dict]] = {}  # name -> [revisions]
        self.jobs: dict[str, dict] = {}  # job_id -> job
        self.tags: dict[str, dict[str, str]] = {}  # arn -> {key: value}
        self.lock = threading.RLock()


def _get_store(region: str = "us-east-1", account_id: str = "123456789012") -> BatchStore:
    key = f"{account_id}:{region}"
    with _lock:
        if key not in _stores:
            _stores[key] = BatchStore(region, account_id)
        return _stores[key]


def _new_id() -> str:
    return str(uuid.uuid4())


# ---------------------------------------------------------------------------
# Error handling
# ---------------------------------------------------------------------------


class BatchError(Exception):
    def __init__(self, code: str, message: str, status: int = 400):
        self.code = code
        self.message = message
        self.status = status


# ---------------------------------------------------------------------------
# Path patterns
# ---------------------------------------------------------------------------

_PATHS: dict[str, Callable] = {}


def _route(pattern: str):
    """Decorator to register a path pattern handler."""
    compiled = re.compile(f"^{pattern}$")

    def decorator(func):
        _PATHS[pattern] = (compiled, func)
        return func

    return decorator


# ---------------------------------------------------------------------------
# Main handler
# ---------------------------------------------------------------------------


async def handle_batch_request(request: Request, region: str, account_id: str) -> Response:
    """Handle an AWS Batch API request."""
    path = request.url.path
    method = request.method.upper()
    body = await request.body()
    params = json.loads(body) if body else {}
    store = _get_store(region, account_id)

    try:
        # Route by path
        handler = _PATH_MAP.get(path)
        if handler:
            result = handler(store, params, region, account_id)
            return _json_response(result)

        # Tags routes with ARN in path
        if path.startswith("/v1/tags/"):
            arn = path[len("/v1/tags/") :]
            if method == "POST":
                return _json_response(_tag_resource(store, arn, params))
            elif method == "DELETE":
                tag_keys = request.query_params.getlist("tagKeys")
                return _json_response(_untag_resource(store, arn, tag_keys))
            elif method == "GET":
                return _json_response(_list_tags_for_resource(store, arn))

        # Fall through to Moto for ops not handled natively
        from robotocore.providers.moto_bridge import forward_to_moto

        return await forward_to_moto(request, "batch", account_id=account_id)

    except BatchError as e:
        return _error(e.code, e.message, e.status)
    except Exception as e:  # noqa: BLE001
        return _error("ServerException", str(e), 500)


# ---------------------------------------------------------------------------
# Compute Environments
# ---------------------------------------------------------------------------


def _create_compute_environment(
    store: BatchStore, params: dict, region: str, account_id: str
) -> dict:
    name = params.get("computeEnvironmentName", "")
    if not name:
        raise BatchError("ClientException", "computeEnvironmentName is required.")

    ce_arn = f"arn:aws:batch:{region}:{account_id}:compute-environment/{name}"

    ce = {
        "computeEnvironmentArn": ce_arn,
        "computeEnvironmentName": name,
        "type": params.get("type", "MANAGED"),
        "state": params.get("state", "ENABLED"),
        "status": "VALID",
        "statusReason": "ComputeEnvironment Healthy",
        "computeResources": params.get("computeResources", {}),
        "serviceRole": params.get("serviceRole", ""),
        "tags": params.get("tags", {}),
    }

    with store.lock:
        if name in store.compute_envs:
            raise BatchError(
                "ClientException",
                f"Compute environment {name} already exists.",
            )
        store.compute_envs[name] = ce
        if ce["tags"]:
            store.tags[ce_arn] = ce["tags"]

    return {
        "computeEnvironmentArn": ce_arn,
        "computeEnvironmentName": name,
    }


def _describe_compute_environments(
    store: BatchStore, params: dict, region: str, account_id: str
) -> dict:
    names = params.get("computeEnvironments", [])

    with store.lock:
        if names:
            envs = []
            for n in names:
                name = n.split("/")[-1] if "/" in n else n
                ce = store.compute_envs.get(name)
                if ce:
                    envs.append(ce)
        else:
            envs = list(store.compute_envs.values())

    return {"computeEnvironments": envs}


def _update_compute_environment(
    store: BatchStore, params: dict, region: str, account_id: str
) -> dict:
    name = params.get("computeEnvironment", "")
    name = name.split("/")[-1] if "/" in name else name

    with store.lock:
        ce = store.compute_envs.get(name)
        if not ce:
            raise BatchError(
                "ClientException",
                f"Compute environment {name} not found.",
            )
        if "state" in params:
            ce["state"] = params["state"]
        if "computeResources" in params:
            ce["computeResources"].update(params["computeResources"])

    return {
        "computeEnvironmentArn": ce["computeEnvironmentArn"],
        "computeEnvironmentName": name,
    }


def _delete_compute_environment(
    store: BatchStore, params: dict, region: str, account_id: str
) -> dict:
    name = params.get("computeEnvironment", "")
    name = name.split("/")[-1] if "/" in name else name

    with store.lock:
        ce = store.compute_envs.get(name)
        if not ce:
            raise BatchError(
                "ClientException",
                f"Compute environment {name} not found.",
            )
        store.tags.pop(ce["computeEnvironmentArn"], None)
        del store.compute_envs[name]
    return {}


# ---------------------------------------------------------------------------
# Job Queues
# ---------------------------------------------------------------------------


def _create_job_queue(store: BatchStore, params: dict, region: str, account_id: str) -> dict:
    name = params.get("jobQueueName", "")
    if not name:
        raise BatchError("ClientException", "jobQueueName is required.")

    queue_arn = f"arn:aws:batch:{region}:{account_id}:job-queue/{name}"

    queue = {
        "jobQueueArn": queue_arn,
        "jobQueueName": name,
        "state": params.get("state", "ENABLED"),
        "status": "VALID",
        "statusReason": "JobQueue Healthy",
        "priority": params.get("priority", 1),
        "computeEnvironmentOrder": params.get("computeEnvironmentOrder", []),
        "tags": params.get("tags", {}),
    }

    with store.lock:
        if name in store.job_queues:
            raise BatchError("ClientException", f"Job queue {name} already exists.")
        store.job_queues[name] = queue
        if queue["tags"]:
            store.tags[queue_arn] = queue["tags"]

    return {"jobQueueArn": queue_arn, "jobQueueName": name}


def _describe_job_queues(store: BatchStore, params: dict, region: str, account_id: str) -> dict:
    names = params.get("jobQueues", [])

    with store.lock:
        if names:
            queues = []
            for n in names:
                name = n.split("/")[-1] if "/" in n else n
                q = store.job_queues.get(name)
                if q:
                    queues.append(q)
        else:
            queues = list(store.job_queues.values())

    return {"jobQueues": queues}


def _update_job_queue(store: BatchStore, params: dict, region: str, account_id: str) -> dict:
    name = params.get("jobQueue", "")
    name = name.split("/")[-1] if "/" in name else name

    with store.lock:
        queue = store.job_queues.get(name)
        if not queue:
            raise BatchError("ClientException", f"Job queue {name} not found.")
        if "state" in params:
            queue["state"] = params["state"]
        if "priority" in params:
            queue["priority"] = params["priority"]
        if "computeEnvironmentOrder" in params:
            queue["computeEnvironmentOrder"] = params["computeEnvironmentOrder"]

    return {
        "jobQueueArn": queue["jobQueueArn"],
        "jobQueueName": name,
    }


def _delete_job_queue(store: BatchStore, params: dict, region: str, account_id: str) -> dict:
    name = params.get("jobQueue", "")
    name = name.split("/")[-1] if "/" in name else name

    with store.lock:
        queue = store.job_queues.get(name)
        if not queue:
            raise BatchError("ClientException", f"Job queue {name} not found.")
        store.tags.pop(queue["jobQueueArn"], None)
        del store.job_queues[name]
    return {}


# ---------------------------------------------------------------------------
# Job Definitions
# ---------------------------------------------------------------------------


def _register_job_definition(store: BatchStore, params: dict, region: str, account_id: str) -> dict:
    name = params.get("jobDefinitionName", "")
    if not name:
        raise BatchError("ClientException", "jobDefinitionName is required.")

    with store.lock:
        revisions = store.job_definitions.setdefault(name, [])
        revision = len(revisions) + 1

        jd_arn = f"arn:aws:batch:{region}:{account_id}:job-definition/{name}:{revision}"

        jd = {
            "jobDefinitionArn": jd_arn,
            "jobDefinitionName": name,
            "revision": revision,
            "status": "ACTIVE",
            "type": params.get("type", "container"),
            "containerProperties": params.get("containerProperties", {}),
            "parameters": params.get("parameters", {}),
            "retryStrategy": params.get("retryStrategy", {"attempts": 1}),
            "timeout": params.get("timeout", {}),
            "tags": params.get("tags", {}),
        }

        revisions.append(jd)
        if jd["tags"]:
            store.tags[jd_arn] = jd["tags"]

    return {
        "jobDefinitionArn": jd_arn,
        "jobDefinitionName": name,
        "revision": revision,
    }


def _describe_job_definitions(
    store: BatchStore, params: dict, region: str, account_id: str
) -> dict:
    names = params.get("jobDefinitions", [])
    name_filter = params.get("jobDefinitionName", "")
    status_filter = params.get("status", "ACTIVE")

    results = []
    with store.lock:
        if names:
            for ref in names:
                jd = _resolve_job_definition(store, ref)
                if jd:
                    results.append(jd)
        elif name_filter:
            for jd in store.job_definitions.get(name_filter, []):
                if jd["status"] == status_filter:
                    results.append(jd)
        else:
            for revisions in store.job_definitions.values():
                for jd in revisions:
                    if jd["status"] == status_filter:
                        results.append(jd)

    return {"jobDefinitions": results}


def _deregister_job_definition(
    store: BatchStore, params: dict, region: str, account_id: str
) -> dict:
    ref = params.get("jobDefinition", "")
    with store.lock:
        jd = _resolve_job_definition(store, ref)
        if not jd:
            raise BatchError("ClientException", f"Job definition {ref} not found.")
        jd["status"] = "INACTIVE"
    return {}


# ---------------------------------------------------------------------------
# Jobs
# ---------------------------------------------------------------------------


_JOB_LIFECYCLE = ["SUBMITTED", "PENDING", "RUNNABLE", "STARTING", "RUNNING", "SUCCEEDED"]


def _submit_job(store: BatchStore, params: dict, region: str, account_id: str) -> dict:
    job_name = params.get("jobName", "")
    job_queue = params.get("jobQueue", "")
    job_def = params.get("jobDefinition", "")

    if not job_name:
        raise BatchError("ClientException", "jobName is required.")

    job_id = _new_id()
    job_arn = f"arn:aws:batch:{region}:{account_id}:job/{job_id}"

    job = {
        "jobArn": job_arn,
        "jobId": job_id,
        "jobName": job_name,
        "jobQueue": job_queue,
        "jobDefinition": job_def,
        "status": "SUBMITTED",
        "statusReason": "",
        "createdAt": int(time.time() * 1000),
        "startedAt": 0,
        "stoppedAt": 0,
        "dependsOn": params.get("dependsOn", []),
        "parameters": params.get("parameters", {}),
        "container": params.get("containerOverrides", {}),
        "tags": params.get("tags", {}),
    }

    with store.lock:
        store.jobs[job_id] = job
        if job["tags"]:
            store.tags[job_arn] = job["tags"]

    # Simulate lifecycle advancement
    _advance_job(store, job_id)

    return {"jobArn": job_arn, "jobId": job_id, "jobName": job_name}


def _describe_jobs(store: BatchStore, params: dict, region: str, account_id: str) -> dict:
    job_ids = params.get("jobs", [])

    with store.lock:
        jobs = []
        for jid in job_ids:
            job = store.jobs.get(jid)
            if job:
                jobs.append(job)

    return {"jobs": jobs}


def _list_jobs(store: BatchStore, params: dict, region: str, account_id: str) -> dict:
    job_queue = params.get("jobQueue", "")
    status_filter = params.get("jobStatus")

    with store.lock:
        summaries = []
        for job in store.jobs.values():
            if job_queue:
                jq = job["jobQueue"]
                # Match by full ARN or by queue name
                jq_name = jq.rsplit("/", 1)[-1] if "/" in jq else jq
                if job_queue != jq and job_queue != jq_name:
                    continue
            if status_filter and job["status"] != status_filter:
                continue
            summaries.append(
                {
                    "jobArn": job["jobArn"],
                    "jobId": job["jobId"],
                    "jobName": job["jobName"],
                    "createdAt": job["createdAt"],
                    "status": job["status"],
                    "statusReason": job["statusReason"],
                }
            )

    return {"jobSummaryList": summaries}


def _terminate_job(store: BatchStore, params: dict, region: str, account_id: str) -> dict:
    job_id = params.get("jobId", "")
    reason = params.get("reason", "Terminated by user")

    with store.lock:
        job = store.jobs.get(job_id)
        if not job:
            raise BatchError("ClientException", f"Job {job_id} not found.")
        job["status"] = "FAILED"
        job["statusReason"] = reason
        job["stoppedAt"] = int(time.time() * 1000)

    return {}


def _cancel_job(store: BatchStore, params: dict, region: str, account_id: str) -> dict:
    job_id = params.get("jobId", "")
    reason = params.get("reason", "Cancelled by user")

    with store.lock:
        job = store.jobs.get(job_id)
        if not job:
            raise BatchError("ClientException", f"Job {job_id} not found.")
        if job["status"] in ("STARTING", "RUNNING"):
            raise BatchError(
                "ClientException",
                "Cannot cancel a job that is already STARTING or RUNNING.",
            )
        job["status"] = "FAILED"
        job["statusReason"] = reason
        job["stoppedAt"] = int(time.time() * 1000)

    return {}


# ---------------------------------------------------------------------------
# Tagging
# ---------------------------------------------------------------------------


def _arn_exists(store: BatchStore, arn: str) -> bool:
    """Check if a resource ARN exists in the store."""
    # Check compute environments
    for ce in store.compute_envs.values():
        if ce["computeEnvironmentArn"] == arn:
            return True
    # Check job queues
    for q in store.job_queues.values():
        if q["jobQueueArn"] == arn:
            return True
    # Check job definitions
    for revisions in store.job_definitions.values():
        for jd in revisions:
            if jd["jobDefinitionArn"] == arn:
                return True
    # Check jobs
    for job in store.jobs.values():
        if job["jobArn"] == arn:
            return True
    return False


def _tag_resource(store: BatchStore, arn: str, params: dict) -> dict:
    new_tags = params.get("tags", {})
    with store.lock:
        if not _arn_exists(store, arn):
            raise BatchError(
                "ClientException",
                f"Resource not found: {arn}",
                404,
            )
        existing = store.tags.setdefault(arn, {})
        existing.update(new_tags)
    return {}


def _untag_resource(store: BatchStore, arn: str, tag_keys: list[str]) -> dict:
    with store.lock:
        if not _arn_exists(store, arn):
            raise BatchError(
                "ClientException",
                f"Resource not found: {arn}",
                404,
            )
        existing = store.tags.get(arn, {})
        for key in tag_keys:
            existing.pop(key, None)
    return {}


def _list_tags_for_resource(store: BatchStore, arn: str) -> dict:
    with store.lock:
        if not _arn_exists(store, arn):
            raise BatchError(
                "ClientException",
                f"Resource not found: {arn}",
                404,
            )
        tags = store.tags.get(arn, {})
    return {"tags": dict(tags)}


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _advance_job(store: BatchStore, job_id: str) -> None:
    """Simulate job lifecycle advancement to SUCCEEDED."""
    with store.lock:
        job = store.jobs.get(job_id)
        if not job:
            return
        # Advance through lifecycle stages instantly
        job["status"] = "SUCCEEDED"
        now = int(time.time() * 1000)
        job["startedAt"] = now
        job["stoppedAt"] = now


def _resolve_job_definition(store: BatchStore, ref: str) -> dict | None:
    """Resolve a job definition reference (name, name:revision, or ARN)."""
    if ref.startswith("arn:"):
        parts = ref.split("/")[-1]
    else:
        parts = ref

    if ":" in parts:
        name, rev_str = parts.rsplit(":", 1)
        try:
            rev = int(rev_str)
        except ValueError:
            return None
        revisions = store.job_definitions.get(name, [])
        for jd in revisions:
            if jd["revision"] == rev:
                return jd
        return None
    else:
        name = parts
        revisions = store.job_definitions.get(name, [])
        for jd in reversed(revisions):
            if jd["status"] == "ACTIVE":
                return jd
        return None


def _json_response(data: dict, status: int = 200) -> Response:
    return Response(
        content=json.dumps(data, default=str),
        status_code=status,
        media_type="application/json",
    )


def _error(code: str, message: str, status: int) -> Response:
    body = json.dumps({"__type": code, "message": message})
    return Response(content=body, status_code=status, media_type="application/json")


# ---------------------------------------------------------------------------
# Path-based routing map
# ---------------------------------------------------------------------------

_PATH_MAP: dict[str, Callable] = {
    "/v1/createcomputeenvironment": _create_compute_environment,
    "/v1/describecomputeenvironments": _describe_compute_environments,
    "/v1/updatecomputeenvironment": _update_compute_environment,
    "/v1/deletecomputeenvironment": _delete_compute_environment,
    "/v1/createjobqueue": _create_job_queue,
    "/v1/describejobqueues": _describe_job_queues,
    "/v1/updatejobqueue": _update_job_queue,
    "/v1/deletejobqueue": _delete_job_queue,
    "/v1/registerjobdefinition": _register_job_definition,
    "/v1/describejobdefinitions": _describe_job_definitions,
    "/v1/deregisterjobdefinition": _deregister_job_definition,
    "/v1/submitjob": _submit_job,
    "/v1/describejobs": _describe_jobs,
    "/v1/listjobs": _list_jobs,
    "/v1/terminatejob": _terminate_job,
    "/v1/canceljob": _cancel_job,
}
