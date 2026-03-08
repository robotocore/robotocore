"""Failing tests that expose bugs in the Batch provider.

Each test documents a specific bug. Do NOT fix the provider — only add tests here.
"""

import json

import pytest
from starlette.requests import Request

from robotocore.services.batch.provider import (
    _stores,
    handle_batch_request,
)

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

REGION = "us-east-1"
ACCOUNT = "123456789012"


def _make_request(method: str, path: str, body: dict | None = None, query: str = ""):
    scope = {
        "type": "http",
        "method": method.upper(),
        "path": path,
        "query_string": query.encode(),
        "headers": [],
    }
    body_bytes = json.dumps(body).encode() if body else b""

    async def receive():
        return {"type": "http.request", "body": body_bytes}

    return Request(scope, receive)


@pytest.fixture(autouse=True)
def _clear_stores():
    _stores.clear()
    yield
    _stores.clear()


async def _create_compute_env(name: str = "test-ce") -> str:
    req = _make_request(
        "POST",
        "/v1/createcomputeenvironment",
        {
            "computeEnvironmentName": name,
            "type": "MANAGED",
        },
    )
    resp = await handle_batch_request(req, REGION, ACCOUNT)
    return json.loads(resp.body)["computeEnvironmentArn"]


async def _create_job_queue(name: str = "test-queue", priority: int = 1) -> str:
    req = _make_request(
        "POST",
        "/v1/createjobqueue",
        {
            "jobQueueName": name,
            "priority": priority,
        },
    )
    resp = await handle_batch_request(req, REGION, ACCOUNT)
    return json.loads(resp.body)["jobQueueArn"]


async def _register_job_def(name: str = "test-def") -> str:
    req = _make_request(
        "POST",
        "/v1/registerjobdefinition",
        {
            "jobDefinitionName": name,
            "type": "container",
            "containerProperties": {"image": "busybox", "vcpus": 1, "memory": 512},
        },
    )
    resp = await handle_batch_request(req, REGION, ACCOUNT)
    return json.loads(resp.body)["jobDefinitionArn"]


async def _submit_job(
    job_name: str = "test-job",
    queue: str = "test-queue",
    job_def: str = "test-def",
) -> str:
    req = _make_request(
        "POST",
        "/v1/submitjob",
        {
            "jobName": job_name,
            "jobQueue": queue,
            "jobDefinition": job_def,
        },
    )
    resp = await handle_batch_request(req, REGION, ACCOUNT)
    return json.loads(resp.body)["jobId"]


# ---------------------------------------------------------------------------
# Bug 1: _submit_job does not validate that the job queue exists
#
# AWS Batch rejects SubmitJob if the referenced job queue doesn't exist.
# The provider accepts any string for jobQueue without checking.
# ---------------------------------------------------------------------------


class TestSubmitJobQueueValidation:
    @pytest.mark.asyncio
    async def test_submit_job_with_nonexistent_queue(self):
        """SubmitJob should fail if the job queue doesn't exist."""
        await _register_job_def("mydef")
        req = _make_request(
            "POST",
            "/v1/submitjob",
            {
                "jobName": "myjob",
                "jobQueue": "nonexistent-queue",
                "jobDefinition": "mydef",
            },
        )
        resp = await handle_batch_request(req, REGION, ACCOUNT)
        # Bug: the provider accepts any string for jobQueue without validation
        # AWS returns a 400 ClientException for nonexistent queues
        assert resp.status_code == 400, (
            f"SubmitJob with nonexistent queue should return 400, got {resp.status_code}"
        )


# ---------------------------------------------------------------------------
# Bug 2: _submit_job does not validate that the job definition exists
#
# AWS Batch rejects SubmitJob if the referenced job definition doesn't exist.
# The provider accepts any string for jobDefinition without checking.
# ---------------------------------------------------------------------------


class TestSubmitJobDefinitionValidation:
    @pytest.mark.asyncio
    async def test_submit_job_with_nonexistent_definition(self):
        """SubmitJob should fail if the job definition doesn't exist."""
        await _create_job_queue("myqueue")
        req = _make_request(
            "POST",
            "/v1/submitjob",
            {
                "jobName": "myjob",
                "jobQueue": "myqueue",
                "jobDefinition": "nonexistent-def",
            },
        )
        resp = await handle_batch_request(req, REGION, ACCOUNT)
        # Bug: the provider accepts any string for jobDefinition without validation
        assert resp.status_code == 400, (
            f"SubmitJob with nonexistent job definition should return 400, got {resp.status_code}"
        )


# ---------------------------------------------------------------------------
# Bug 3: _cancel_job allows cancelling SUCCEEDED jobs
#
# AWS Batch rejects CancelJob for jobs in SUCCEEDED or FAILED status.
# The provider only blocks STARTING and RUNNING, letting SUCCEEDED through
# and marking it as FAILED, which is incorrect.
# ---------------------------------------------------------------------------


class TestCancelJobSucceeded:
    @pytest.mark.asyncio
    async def test_cancel_succeeded_job_should_fail(self):
        """CancelJob on a SUCCEEDED job should return an error."""
        await _create_job_queue("q1")
        await _register_job_def("mydef")
        job_id = await _submit_job("myjob", "q1", "mydef")

        # The job auto-advances to SUCCEEDED
        desc_req = _make_request("POST", "/v1/describejobs", {"jobs": [job_id]})
        desc_resp = await handle_batch_request(desc_req, REGION, ACCOUNT)
        job_data = json.loads(desc_resp.body)["jobs"][0]
        assert job_data["status"] == "SUCCEEDED"

        # Now try to cancel it
        req = _make_request(
            "POST",
            "/v1/canceljob",
            {"jobId": job_id, "reason": "Oops"},
        )
        resp = await handle_batch_request(req, REGION, ACCOUNT)
        # Bug: the provider allows this and marks it as FAILED
        assert resp.status_code == 400, (
            f"CancelJob on SUCCEEDED job should return 400, got {resp.status_code}"
        )


# ---------------------------------------------------------------------------
# Bug 4: _delete_compute_environment doesn't check for referencing job queues
#
# AWS Batch rejects DeleteComputeEnvironment if any job queue references it
# in its computeEnvironmentOrder. The provider just deletes it.
# ---------------------------------------------------------------------------


class TestDeleteComputeEnvWithReferences:
    @pytest.mark.asyncio
    async def test_delete_compute_env_referenced_by_queue(self):
        """DeleteComputeEnvironment should fail if a job queue references it."""
        ce_arn = await _create_compute_env("my-ce")

        # Create a job queue that references this compute environment
        req = _make_request(
            "POST",
            "/v1/createjobqueue",
            {
                "jobQueueName": "my-queue",
                "priority": 1,
                "computeEnvironmentOrder": [{"computeEnvironment": ce_arn, "order": 1}],
            },
        )
        resp = await handle_batch_request(req, REGION, ACCOUNT)
        assert resp.status_code == 200

        # Now try to delete the compute environment
        del_req = _make_request(
            "POST",
            "/v1/deletecomputeenvironment",
            {"computeEnvironment": "my-ce"},
        )
        del_resp = await handle_batch_request(del_req, REGION, ACCOUNT)
        # Bug: provider allows deletion even though a queue references this CE
        assert del_resp.status_code == 400, (
            f"Delete CE referenced by queue should return 400, got {del_resp.status_code}"
        )


# ---------------------------------------------------------------------------
# Bug 5: Job queue priority is stored but not validated
#
# AWS Batch requires priority to be a non-negative integer (0+).
# The provider accepts any value including negative numbers.
# ---------------------------------------------------------------------------


class TestJobQueuePriorityValidation:
    @pytest.mark.asyncio
    async def test_create_job_queue_negative_priority(self):
        """CreateJobQueue should reject negative priority values."""
        req = _make_request(
            "POST",
            "/v1/createjobqueue",
            {
                "jobQueueName": "bad-queue",
                "priority": -5,
            },
        )
        resp = await handle_batch_request(req, REGION, ACCOUNT)
        # Bug: the provider accepts negative priority
        assert resp.status_code == 400, (
            f"CreateJobQueue with negative priority should return 400, got {resp.status_code}"
        )


# ---------------------------------------------------------------------------
# Bug 6: _delete_job_queue doesn't check for active jobs
#
# AWS Batch requires the job queue to be in DISABLED state before deletion.
# The provider allows deleting an ENABLED queue.
# ---------------------------------------------------------------------------


class TestDeleteJobQueueStateCheck:
    @pytest.mark.asyncio
    async def test_delete_enabled_job_queue_should_fail(self):
        """DeleteJobQueue should reject deletion of an ENABLED queue."""
        await _create_job_queue("my-queue")

        # Verify it's ENABLED
        desc_req = _make_request(
            "POST",
            "/v1/describejobqueues",
            {"jobQueues": ["my-queue"]},
        )
        desc_resp = await handle_batch_request(desc_req, REGION, ACCOUNT)
        queue_data = json.loads(desc_resp.body)["jobQueues"][0]
        assert queue_data["state"] == "ENABLED"

        # Try to delete it while ENABLED
        del_req = _make_request(
            "POST",
            "/v1/deletejobqueue",
            {"jobQueue": "my-queue"},
        )
        del_resp = await handle_batch_request(del_req, REGION, ACCOUNT)
        # Bug: provider allows deleting an ENABLED queue
        assert del_resp.status_code == 400, (
            f"Delete ENABLED queue should return 400, got {del_resp.status_code}"
        )
