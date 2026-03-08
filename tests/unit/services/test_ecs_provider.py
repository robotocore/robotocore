"""Unit tests for the ECS provider."""

import json

import pytest
from starlette.requests import Request

from robotocore.services.ecs.provider import (
    EcsError,
    _error,
    _json_response,
    _stores,
    handle_ecs_request,
)

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

REGION = "us-east-1"
ACCOUNT = "123456789012"


def _make_request(action: str, body: dict):
    target = f"AmazonEC2ContainerServiceV20141113.{action}"
    scope = {
        "type": "http",
        "method": "POST",
        "path": "/",
        "query_string": b"",
        "headers": [(b"x-amz-target", target.encode())],
    }
    body_bytes = json.dumps(body).encode()

    async def receive():
        return {"type": "http.request", "body": body_bytes}

    return Request(scope, receive)


@pytest.fixture(autouse=True)
def _clear_stores():
    _stores.clear()
    yield
    _stores.clear()


async def _create_cluster(name: str = "test-cluster") -> str:
    req = _make_request("CreateCluster", {"clusterName": name})
    resp = await handle_ecs_request(req, REGION, ACCOUNT)
    return json.loads(resp.body)["cluster"]["clusterArn"]


async def _register_task_def(family: str = "test-family") -> str:
    req = _make_request(
        "RegisterTaskDefinition",
        {
            "family": family,
            "containerDefinitions": [{"name": "app", "image": "nginx"}],
        },
    )
    resp = await handle_ecs_request(req, REGION, ACCOUNT)
    return json.loads(resp.body)["taskDefinition"]["taskDefinitionArn"]


# ---------------------------------------------------------------------------
# Error / response helpers
# ---------------------------------------------------------------------------


class TestEcsError:
    def test_default_status(self):
        e = EcsError("Code", "msg")
        assert e.status == 400

    def test_custom_status(self):
        e = EcsError("Code", "msg", 404)
        assert e.status == 404


class TestResponseHelpers:
    def test_json_response(self):
        resp = _json_response({"key": "val"})
        assert resp.status_code == 200

    def test_error_response(self):
        resp = _error("Code", "msg", 400)
        assert resp.status_code == 400


# ---------------------------------------------------------------------------
# Clusters
# ---------------------------------------------------------------------------


class TestClusterCrud:
    @pytest.mark.asyncio
    async def test_create_cluster(self):
        req = _make_request("CreateCluster", {"clusterName": "mycluster"})
        resp = await handle_ecs_request(req, REGION, ACCOUNT)
        assert resp.status_code == 200
        data = json.loads(resp.body)
        assert data["cluster"]["clusterName"] == "mycluster"
        assert data["cluster"]["status"] == "ACTIVE"

    @pytest.mark.asyncio
    async def test_describe_clusters(self):
        await _create_cluster("c1")
        req = _make_request("DescribeClusters", {"clusters": ["c1"]})
        resp = await handle_ecs_request(req, REGION, ACCOUNT)
        data = json.loads(resp.body)
        assert len(data["clusters"]) == 1
        assert data["clusters"][0]["clusterName"] == "c1"

    @pytest.mark.asyncio
    async def test_describe_missing_cluster(self):
        req = _make_request("DescribeClusters", {"clusters": ["nope"]})
        resp = await handle_ecs_request(req, REGION, ACCOUNT)
        data = json.loads(resp.body)
        assert len(data["failures"]) == 1

    @pytest.mark.asyncio
    async def test_list_clusters(self):
        await _create_cluster("c1")
        await _create_cluster("c2")
        req = _make_request("ListClusters", {})
        resp = await handle_ecs_request(req, REGION, ACCOUNT)
        data = json.loads(resp.body)
        assert len(data["clusterArns"]) == 2

    @pytest.mark.asyncio
    async def test_delete_cluster(self):
        await _create_cluster("todelete")
        req = _make_request("DeleteCluster", {"cluster": "todelete"})
        resp = await handle_ecs_request(req, REGION, ACCOUNT)
        assert resp.status_code == 200
        data = json.loads(resp.body)
        assert data["cluster"]["status"] == "INACTIVE"

    @pytest.mark.asyncio
    async def test_delete_nonexistent_cluster(self):
        req = _make_request("DeleteCluster", {"cluster": "nope"})
        resp = await handle_ecs_request(req, REGION, ACCOUNT)
        assert resp.status_code == 404


# ---------------------------------------------------------------------------
# Task Definitions
# ---------------------------------------------------------------------------


class TestTaskDefinitions:
    @pytest.mark.asyncio
    async def test_register_task_definition(self):
        req = _make_request(
            "RegisterTaskDefinition",
            {
                "family": "web",
                "containerDefinitions": [{"name": "app", "image": "nginx"}],
            },
        )
        resp = await handle_ecs_request(req, REGION, ACCOUNT)
        assert resp.status_code == 200
        data = json.loads(resp.body)
        assert data["taskDefinition"]["family"] == "web"
        assert data["taskDefinition"]["revision"] == 1

    @pytest.mark.asyncio
    async def test_revision_auto_increment(self):
        for _ in range(3):
            await _register_task_def("web")
        req = _make_request("ListTaskDefinitions", {"familyPrefix": "web"})
        resp = await handle_ecs_request(req, REGION, ACCOUNT)
        data = json.loads(resp.body)
        assert len(data["taskDefinitionArns"]) == 3

    @pytest.mark.asyncio
    async def test_describe_task_definition(self):
        await _register_task_def("web")
        req = _make_request("DescribeTaskDefinition", {"taskDefinition": "web:1"})
        resp = await handle_ecs_request(req, REGION, ACCOUNT)
        assert resp.status_code == 200
        data = json.loads(resp.body)
        assert data["taskDefinition"]["revision"] == 1

    @pytest.mark.asyncio
    async def test_describe_task_def_latest(self):
        await _register_task_def("web")
        await _register_task_def("web")
        req = _make_request("DescribeTaskDefinition", {"taskDefinition": "web"})
        resp = await handle_ecs_request(req, REGION, ACCOUNT)
        data = json.loads(resp.body)
        assert data["taskDefinition"]["revision"] == 2

    @pytest.mark.asyncio
    async def test_deregister_task_definition(self):
        await _register_task_def("web")
        req = _make_request("DeregisterTaskDefinition", {"taskDefinition": "web:1"})
        resp = await handle_ecs_request(req, REGION, ACCOUNT)
        data = json.loads(resp.body)
        assert data["taskDefinition"]["status"] == "INACTIVE"

    @pytest.mark.asyncio
    async def test_list_task_definitions(self):
        await _register_task_def("web")
        await _register_task_def("api")
        req = _make_request("ListTaskDefinitions", {})
        resp = await handle_ecs_request(req, REGION, ACCOUNT)
        data = json.loads(resp.body)
        assert len(data["taskDefinitionArns"]) == 2


# ---------------------------------------------------------------------------
# Services
# ---------------------------------------------------------------------------


class TestServices:
    @pytest.mark.asyncio
    async def test_create_service(self):
        await _create_cluster("c1")
        await _register_task_def("web")
        req = _make_request(
            "CreateService",
            {
                "cluster": "c1",
                "serviceName": "web-svc",
                "taskDefinition": "web",
                "desiredCount": 2,
            },
        )
        resp = await handle_ecs_request(req, REGION, ACCOUNT)
        assert resp.status_code == 200
        data = json.loads(resp.body)
        assert data["service"]["serviceName"] == "web-svc"

    @pytest.mark.asyncio
    async def test_describe_services(self):
        await _create_cluster("c1")
        await _register_task_def("web")
        await handle_ecs_request(
            _make_request(
                "CreateService", {"cluster": "c1", "serviceName": "svc1", "taskDefinition": "web"}
            ),
            REGION,
            ACCOUNT,
        )
        req = _make_request("DescribeServices", {"cluster": "c1", "services": ["svc1"]})
        resp = await handle_ecs_request(req, REGION, ACCOUNT)
        data = json.loads(resp.body)
        assert len(data["services"]) == 1

    @pytest.mark.asyncio
    async def test_list_services(self):
        await _create_cluster("c1")
        await _register_task_def("web")
        await handle_ecs_request(
            _make_request(
                "CreateService", {"cluster": "c1", "serviceName": "svc1", "taskDefinition": "web"}
            ),
            REGION,
            ACCOUNT,
        )
        req = _make_request("ListServices", {"cluster": "c1"})
        resp = await handle_ecs_request(req, REGION, ACCOUNT)
        data = json.loads(resp.body)
        assert len(data["serviceArns"]) == 1

    @pytest.mark.asyncio
    async def test_update_service(self):
        await _create_cluster("c1")
        await _register_task_def("web")
        await handle_ecs_request(
            _make_request(
                "CreateService",
                {
                    "cluster": "c1",
                    "serviceName": "svc1",
                    "taskDefinition": "web",
                    "desiredCount": 1,
                },
            ),
            REGION,
            ACCOUNT,
        )
        req = _make_request(
            "UpdateService", {"cluster": "c1", "service": "svc1", "desiredCount": 5}
        )
        resp = await handle_ecs_request(req, REGION, ACCOUNT)
        data = json.loads(resp.body)
        assert data["service"]["desiredCount"] == 5

    @pytest.mark.asyncio
    async def test_delete_service(self):
        await _create_cluster("c1")
        await _register_task_def("web")
        await handle_ecs_request(
            _make_request(
                "CreateService", {"cluster": "c1", "serviceName": "svc1", "taskDefinition": "web"}
            ),
            REGION,
            ACCOUNT,
        )
        req = _make_request("DeleteService", {"cluster": "c1", "service": "svc1"})
        resp = await handle_ecs_request(req, REGION, ACCOUNT)
        data = json.loads(resp.body)
        assert data["service"]["status"] == "INACTIVE"


# ---------------------------------------------------------------------------
# Tasks
# ---------------------------------------------------------------------------


class TestTasks:
    @pytest.mark.asyncio
    async def test_run_task(self):
        await _create_cluster("c1")
        await _register_task_def("web")
        req = _make_request("RunTask", {"cluster": "c1", "taskDefinition": "web", "count": 2})
        resp = await handle_ecs_request(req, REGION, ACCOUNT)
        assert resp.status_code == 200
        data = json.loads(resp.body)
        assert len(data["tasks"]) == 2

    @pytest.mark.asyncio
    async def test_describe_tasks(self):
        await _create_cluster("c1")
        await _register_task_def("web")
        run_resp = await handle_ecs_request(
            _make_request("RunTask", {"cluster": "c1", "taskDefinition": "web"}), REGION, ACCOUNT
        )
        task_arn = json.loads(run_resp.body)["tasks"][0]["taskArn"]
        task_id = task_arn.split("/")[-1]

        req = _make_request("DescribeTasks", {"cluster": "c1", "tasks": [task_id]})
        resp = await handle_ecs_request(req, REGION, ACCOUNT)
        data = json.loads(resp.body)
        assert len(data["tasks"]) == 1

    @pytest.mark.asyncio
    async def test_list_tasks(self):
        await _create_cluster("c1")
        await _register_task_def("web")
        await handle_ecs_request(
            _make_request("RunTask", {"cluster": "c1", "taskDefinition": "web"}), REGION, ACCOUNT
        )
        req = _make_request("ListTasks", {"cluster": "c1"})
        resp = await handle_ecs_request(req, REGION, ACCOUNT)
        data = json.loads(resp.body)
        assert len(data["taskArns"]) == 1

    @pytest.mark.asyncio
    async def test_stop_task(self):
        await _create_cluster("c1")
        await _register_task_def("web")
        run_resp = await handle_ecs_request(
            _make_request("RunTask", {"cluster": "c1", "taskDefinition": "web"}), REGION, ACCOUNT
        )
        task_arn = json.loads(run_resp.body)["tasks"][0]["taskArn"]
        task_id = task_arn.split("/")[-1]

        req = _make_request("StopTask", {"cluster": "c1", "task": task_id})
        resp = await handle_ecs_request(req, REGION, ACCOUNT)
        data = json.loads(resp.body)
        assert data["task"]["lastStatus"] == "STOPPED"


# ---------------------------------------------------------------------------
# Tagging
# ---------------------------------------------------------------------------


class TestTagging:
    @pytest.mark.asyncio
    async def test_tag_resource(self):
        arn = await _create_cluster("c1")
        req = _make_request(
            "TagResource",
            {
                "resourceArn": arn,
                "tags": [{"key": "env", "value": "test"}],
            },
        )
        resp = await handle_ecs_request(req, REGION, ACCOUNT)
        assert resp.status_code == 200

    @pytest.mark.asyncio
    async def test_list_tags_for_resource(self):
        arn = await _create_cluster("c1")
        await handle_ecs_request(
            _make_request(
                "TagResource",
                {
                    "resourceArn": arn,
                    "tags": [{"key": "env", "value": "test"}],
                },
            ),
            REGION,
            ACCOUNT,
        )
        req = _make_request("ListTagsForResource", {"resourceArn": arn})
        resp = await handle_ecs_request(req, REGION, ACCOUNT)
        data = json.loads(resp.body)
        assert len(data["tags"]) == 1
        assert data["tags"][0]["key"] == "env"

    @pytest.mark.asyncio
    async def test_untag_resource(self):
        arn = await _create_cluster("c1")
        await handle_ecs_request(
            _make_request(
                "TagResource",
                {
                    "resourceArn": arn,
                    "tags": [{"key": "env", "value": "test"}],
                },
            ),
            REGION,
            ACCOUNT,
        )
        await handle_ecs_request(
            _make_request("UntagResource", {"resourceArn": arn, "tagKeys": ["env"]}),
            REGION,
            ACCOUNT,
        )
        req = _make_request("ListTagsForResource", {"resourceArn": arn})
        resp = await handle_ecs_request(req, REGION, ACCOUNT)
        data = json.loads(resp.body)
        assert len(data["tags"]) == 0


# ---------------------------------------------------------------------------
# Edge cases
# ---------------------------------------------------------------------------


class TestEdgeCases:
    @pytest.mark.asyncio
    async def test_missing_target(self):
        scope = {
            "type": "http",
            "method": "POST",
            "path": "/",
            "query_string": b"",
            "headers": [],
        }

        async def receive():
            return {"type": "http.request", "body": b"{}"}

        req = Request(scope, receive)
        resp = await handle_ecs_request(req, REGION, ACCOUNT)
        assert resp.status_code == 400
