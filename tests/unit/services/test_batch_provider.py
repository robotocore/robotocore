"""Unit tests for the AWS Batch provider."""

import json

import pytest
from starlette.requests import Request

from robotocore.services.batch.provider import (
    BatchError,
    _error,
    _json_response,
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
    req = _make_request("POST", "/v1/createcomputeenvironment", {
        "computeEnvironmentName": name, "type": "MANAGED",
    })
    resp = await handle_batch_request(req, REGION, ACCOUNT)
    return json.loads(resp.body)["computeEnvironmentArn"]


async def _create_job_queue(name: str = "test-queue") -> str:
    req = _make_request("POST", "/v1/createjobqueue", {
        "jobQueueName": name, "priority": 1,
    })
    resp = await handle_batch_request(req, REGION, ACCOUNT)
    return json.loads(resp.body)["jobQueueArn"]


async def _register_job_def(name: str = "test-def") -> str:
    req = _make_request("POST", "/v1/registerjobdefinition", {
        "jobDefinitionName": name,
        "type": "container",
        "containerProperties": {"image": "busybox", "vcpus": 1, "memory": 512},
    })
    resp = await handle_batch_request(req, REGION, ACCOUNT)
    return json.loads(resp.body)["jobDefinitionArn"]


# ---------------------------------------------------------------------------
# Error / response helpers
# ---------------------------------------------------------------------------


class TestBatchError:
    def test_default_status(self):
        e = BatchError("Code", "msg")
        assert e.status == 400

    def test_custom_status(self):
        e = BatchError("Code", "msg", 500)
        assert e.status == 500


class TestResponseHelpers:
    def test_json_response(self):
        resp = _json_response({"key": "val"})
        assert resp.status_code == 200

    def test_error_response(self):
        resp = _error("Code", "msg", 400)
        assert resp.status_code == 400


# ---------------------------------------------------------------------------
# Compute Environments
# ---------------------------------------------------------------------------


class TestComputeEnvironments:
    @pytest.mark.asyncio
    async def test_create(self):
        req = _make_request("POST", "/v1/createcomputeenvironment", {
            "computeEnvironmentName": "myenv", "type": "MANAGED",
        })
        resp = await handle_batch_request(req, REGION, ACCOUNT)
        assert resp.status_code == 200
        data = json.loads(resp.body)
        assert data["computeEnvironmentName"] == "myenv"

    @pytest.mark.asyncio
    async def test_create_duplicate(self):
        await _create_compute_env("myenv")
        req = _make_request("POST", "/v1/createcomputeenvironment", {
            "computeEnvironmentName": "myenv",
        })
        resp = await handle_batch_request(req, REGION, ACCOUNT)
        assert resp.status_code == 400

    @pytest.mark.asyncio
    async def test_describe(self):
        await _create_compute_env("myenv")
        req = _make_request("POST", "/v1/describecomputeenvironments", {
            "computeEnvironments": ["myenv"],
        })
        resp = await handle_batch_request(req, REGION, ACCOUNT)
        data = json.loads(resp.body)
        assert len(data["computeEnvironments"]) == 1

    @pytest.mark.asyncio
    async def test_describe_all(self):
        await _create_compute_env("env1")
        await _create_compute_env("env2")
        req = _make_request("POST", "/v1/describecomputeenvironments", {})
        resp = await handle_batch_request(req, REGION, ACCOUNT)
        data = json.loads(resp.body)
        assert len(data["computeEnvironments"]) == 2

    @pytest.mark.asyncio
    async def test_update(self):
        await _create_compute_env("myenv")
        req = _make_request("POST", "/v1/updatecomputeenvironment", {
            "computeEnvironment": "myenv", "state": "DISABLED",
        })
        resp = await handle_batch_request(req, REGION, ACCOUNT)
        assert resp.status_code == 200

    @pytest.mark.asyncio
    async def test_delete(self):
        await _create_compute_env("myenv")
        req = _make_request("POST", "/v1/deletecomputeenvironment", {
            "computeEnvironment": "myenv",
        })
        resp = await handle_batch_request(req, REGION, ACCOUNT)
        assert resp.status_code == 200

    @pytest.mark.asyncio
    async def test_delete_nonexistent(self):
        req = _make_request("POST", "/v1/deletecomputeenvironment", {
            "computeEnvironment": "nope",
        })
        resp = await handle_batch_request(req, REGION, ACCOUNT)
        assert resp.status_code == 400


# ---------------------------------------------------------------------------
# Job Queues
# ---------------------------------------------------------------------------


class TestJobQueues:
    @pytest.mark.asyncio
    async def test_create(self):
        req = _make_request("POST", "/v1/createjobqueue", {
            "jobQueueName": "myqueue", "priority": 10,
        })
        resp = await handle_batch_request(req, REGION, ACCOUNT)
        assert resp.status_code == 200
        data = json.loads(resp.body)
        assert data["jobQueueName"] == "myqueue"

    @pytest.mark.asyncio
    async def test_describe(self):
        await _create_job_queue("q1")
        req = _make_request("POST", "/v1/describejobqueues", {
            "jobQueues": ["q1"],
        })
        resp = await handle_batch_request(req, REGION, ACCOUNT)
        data = json.loads(resp.body)
        assert len(data["jobQueues"]) == 1

    @pytest.mark.asyncio
    async def test_update(self):
        await _create_job_queue("q1")
        req = _make_request("POST", "/v1/updatejobqueue", {
            "jobQueue": "q1", "priority": 99,
        })
        resp = await handle_batch_request(req, REGION, ACCOUNT)
        assert resp.status_code == 200

    @pytest.mark.asyncio
    async def test_delete(self):
        await _create_job_queue("q1")
        req = _make_request("POST", "/v1/deletejobqueue", {"jobQueue": "q1"})
        resp = await handle_batch_request(req, REGION, ACCOUNT)
        assert resp.status_code == 200


# ---------------------------------------------------------------------------
# Job Definitions
# ---------------------------------------------------------------------------


class TestJobDefinitions:
    @pytest.mark.asyncio
    async def test_register(self):
        req = _make_request("POST", "/v1/registerjobdefinition", {
            "jobDefinitionName": "mydef", "type": "container",
            "containerProperties": {"image": "busybox"},
        })
        resp = await handle_batch_request(req, REGION, ACCOUNT)
        assert resp.status_code == 200
        data = json.loads(resp.body)
        assert data["revision"] == 1

    @pytest.mark.asyncio
    async def test_revision_auto_increment(self):
        for _ in range(3):
            await _register_job_def("mydef")
        req = _make_request("POST", "/v1/describejobdefinitions", {
            "jobDefinitionName": "mydef",
        })
        resp = await handle_batch_request(req, REGION, ACCOUNT)
        data = json.loads(resp.body)
        assert len(data["jobDefinitions"]) == 3

    @pytest.mark.asyncio
    async def test_describe_by_name(self):
        await _register_job_def("mydef")
        req = _make_request("POST", "/v1/describejobdefinitions", {
            "jobDefinitions": ["mydef:1"],
        })
        resp = await handle_batch_request(req, REGION, ACCOUNT)
        data = json.loads(resp.body)
        assert len(data["jobDefinitions"]) == 1

    @pytest.mark.asyncio
    async def test_deregister(self):
        await _register_job_def("mydef")
        req = _make_request("POST", "/v1/deregisterjobdefinition", {
            "jobDefinition": "mydef:1",
        })
        resp = await handle_batch_request(req, REGION, ACCOUNT)
        assert resp.status_code == 200


# ---------------------------------------------------------------------------
# Jobs
# ---------------------------------------------------------------------------


class TestJobs:
    @pytest.mark.asyncio
    async def test_submit_job(self):
        await _create_job_queue("q1")
        await _register_job_def("mydef")
        req = _make_request("POST", "/v1/submitjob", {
            "jobName": "myjob",
            "jobQueue": "q1",
            "jobDefinition": "mydef",
        })
        resp = await handle_batch_request(req, REGION, ACCOUNT)
        assert resp.status_code == 200
        data = json.loads(resp.body)
        assert "jobId" in data
        assert data["jobName"] == "myjob"

    @pytest.mark.asyncio
    async def test_describe_jobs(self):
        await _create_job_queue("q1")
        await _register_job_def("mydef")
        submit_resp = await handle_batch_request(
            _make_request("POST", "/v1/submitjob", {
                "jobName": "myjob", "jobQueue": "q1", "jobDefinition": "mydef",
            }), REGION, ACCOUNT
        )
        job_id = json.loads(submit_resp.body)["jobId"]

        req = _make_request("POST", "/v1/describejobs", {"jobs": [job_id]})
        resp = await handle_batch_request(req, REGION, ACCOUNT)
        data = json.loads(resp.body)
        assert len(data["jobs"]) == 1
        assert data["jobs"][0]["status"] == "SUCCEEDED"

    @pytest.mark.asyncio
    async def test_list_jobs(self):
        await _create_job_queue("q1")
        await _register_job_def("mydef")
        await handle_batch_request(
            _make_request("POST", "/v1/submitjob", {
                "jobName": "myjob", "jobQueue": "q1", "jobDefinition": "mydef",
            }), REGION, ACCOUNT
        )
        req = _make_request("POST", "/v1/listjobs", {
            "jobQueue": "q1", "jobStatus": "SUCCEEDED",
        })
        resp = await handle_batch_request(req, REGION, ACCOUNT)
        data = json.loads(resp.body)
        assert len(data["jobSummaryList"]) == 1

    @pytest.mark.asyncio
    async def test_terminate_job(self):
        await _create_job_queue("q1")
        await _register_job_def("mydef")
        submit_resp = await handle_batch_request(
            _make_request("POST", "/v1/submitjob", {
                "jobName": "myjob", "jobQueue": "q1", "jobDefinition": "mydef",
            }), REGION, ACCOUNT
        )
        job_id = json.loads(submit_resp.body)["jobId"]

        req = _make_request("POST", "/v1/terminatejob", {
            "jobId": job_id, "reason": "Testing",
        })
        resp = await handle_batch_request(req, REGION, ACCOUNT)
        assert resp.status_code == 200

    @pytest.mark.asyncio
    async def test_cancel_submitted_job(self):
        await _create_job_queue("q1")
        await _register_job_def("mydef")
        # Override status to SUBMITTED for cancel test
        submit_resp = await handle_batch_request(
            _make_request("POST", "/v1/submitjob", {
                "jobName": "myjob", "jobQueue": "q1", "jobDefinition": "mydef",
            }), REGION, ACCOUNT
        )
        job_id = json.loads(submit_resp.body)["jobId"]
        # Set status back to SUBMITTED for cancel test
        store = _stores.get(REGION)
        with store.lock:
            store.jobs[job_id]["status"] = "SUBMITTED"

        req = _make_request("POST", "/v1/canceljob", {
            "jobId": job_id, "reason": "No longer needed",
        })
        resp = await handle_batch_request(req, REGION, ACCOUNT)
        assert resp.status_code == 200


# ---------------------------------------------------------------------------
# Tagging
# ---------------------------------------------------------------------------


class TestTagging:
    @pytest.mark.asyncio
    async def test_tag_resource(self):
        arn = await _create_compute_env("tagged-env")
        req = _make_request(
            "POST", f"/v1/tags/{arn}",
            {"tags": {"env": "test", "team": "eng"}},
        )
        resp = await handle_batch_request(req, REGION, ACCOUNT)
        assert resp.status_code == 200

    @pytest.mark.asyncio
    async def test_list_tags(self):
        arn = await _create_compute_env("tagged-env")
        await handle_batch_request(
            _make_request("POST", f"/v1/tags/{arn}", {"tags": {"env": "test"}}),
            REGION, ACCOUNT
        )
        req = _make_request("GET", f"/v1/tags/{arn}")
        resp = await handle_batch_request(req, REGION, ACCOUNT)
        data = json.loads(resp.body)
        assert data["tags"]["env"] == "test"

    @pytest.mark.asyncio
    async def test_untag_resource(self):
        arn = await _create_compute_env("tagged-env")
        await handle_batch_request(
            _make_request("POST", f"/v1/tags/{arn}", {"tags": {"env": "test"}}),
            REGION, ACCOUNT
        )
        req = _make_request("DELETE", f"/v1/tags/{arn}", query="tagKeys=env")
        resp = await handle_batch_request(req, REGION, ACCOUNT)
        assert resp.status_code == 200


# ---------------------------------------------------------------------------
# Edge cases
# ---------------------------------------------------------------------------


class TestEdgeCases:
    @pytest.mark.asyncio
    async def test_unknown_path(self):
        req = _make_request("POST", "/v1/bogus", {})
        resp = await handle_batch_request(req, REGION, ACCOUNT)
        assert resp.status_code == 400

    @pytest.mark.asyncio
    async def test_missing_job_name(self):
        req = _make_request("POST", "/v1/submitjob", {
            "jobQueue": "q1", "jobDefinition": "mydef",
        })
        resp = await handle_batch_request(req, REGION, ACCOUNT)
        assert resp.status_code == 400
