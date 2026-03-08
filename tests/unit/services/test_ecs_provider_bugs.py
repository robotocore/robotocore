"""Failing tests that expose bugs in the ECS provider.

Each test documents a specific bug. Do NOT fix the provider — only add tests here.
"""

import json

import pytest
from starlette.requests import Request

from robotocore.services.ecs.provider import (
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


async def _register_task_def(family: str = "test-family", **kwargs) -> str:
    params = {
        "family": family,
        "containerDefinitions": [{"name": "app", "image": "nginx"}],
        **kwargs,
    }
    req = _make_request("RegisterTaskDefinition", params)
    resp = await handle_ecs_request(req, REGION, ACCOUNT)
    return json.loads(resp.body)["taskDefinition"]["taskDefinitionArn"]


# ---------------------------------------------------------------------------
# Bug 1: Container port mappings are dropped when running tasks
#
# When _run_task creates container entries from containerDefinitions, it only
# copies "name" and "lastStatus". The portMappings, image, environment, and
# all other container properties are lost. AWS ECS includes portMappings in
# the task's container descriptions.
# ---------------------------------------------------------------------------


class TestContainerPortMappingsDropped:
    @pytest.mark.asyncio
    async def test_run_task_preserves_container_port_mappings(self):
        """RunTask should include portMappings in the container descriptions."""
        await _create_cluster("c1")
        req = _make_request(
            "RegisterTaskDefinition",
            {
                "family": "web",
                "containerDefinitions": [
                    {
                        "name": "app",
                        "image": "nginx",
                        "portMappings": [
                            {"containerPort": 80, "hostPort": 8080, "protocol": "tcp"}
                        ],
                    }
                ],
            },
        )
        await handle_ecs_request(req, REGION, ACCOUNT)

        run_req = _make_request("RunTask", {"cluster": "c1", "taskDefinition": "web"})
        run_resp = await handle_ecs_request(run_req, REGION, ACCOUNT)
        data = json.loads(run_resp.body)

        container = data["tasks"][0]["containers"][0]
        # Bug: portMappings is missing from the container
        assert "portMappings" in container, "portMappings should be present in container"
        assert container["portMappings"][0]["containerPort"] == 80

    @pytest.mark.asyncio
    async def test_run_task_preserves_container_image(self):
        """RunTask should include the image in the container descriptions."""
        await _create_cluster("c1")
        await _register_task_def("web")

        run_req = _make_request("RunTask", {"cluster": "c1", "taskDefinition": "web"})
        run_resp = await handle_ecs_request(run_req, REGION, ACCOUNT)
        data = json.loads(run_resp.body)

        container = data["tasks"][0]["containers"][0]
        # Bug: image is missing from the container
        assert "image" in container, "image should be present in container"
        assert container["image"] == "nginx"


# ---------------------------------------------------------------------------
# Bug 2: _list_tasks service filtering is broken
#
# In _list_tasks, when serviceName is a non-empty string, neither the
# `if service_name is None` branch nor the `elif not service_name` branch
# matches, so no tasks are ever returned for a specific service.
# ---------------------------------------------------------------------------


class TestListTasksServiceFiltering:
    @pytest.mark.asyncio
    async def test_list_tasks_with_service_name_filter(self):
        """ListTasks with serviceName should return tasks for that service.

        The current code has a logic bug: when serviceName is set to a non-empty
        string, the task is never appended to the results list because:
        - `if service_name is None` is False (it's a string)
        - `elif not service_name` is False (it's a non-empty string)
        So the task ARN is never added.
        """
        await _create_cluster("c1")
        await _register_task_def("web")

        # Create a service
        await handle_ecs_request(
            _make_request(
                "CreateService",
                {
                    "cluster": "c1",
                    "serviceName": "web-svc",
                    "taskDefinition": "web",
                    "desiredCount": 1,
                },
            ),
            REGION,
            ACCOUNT,
        )

        # Run a task
        await handle_ecs_request(
            _make_request("RunTask", {"cluster": "c1", "taskDefinition": "web"}),
            REGION,
            ACCOUNT,
        )

        # List tasks filtering by serviceName — this returns empty due to the bug
        req = _make_request("ListTasks", {"cluster": "c1", "serviceName": "web-svc"})
        resp = await handle_ecs_request(req, REGION, ACCOUNT)
        data = json.loads(resp.body)

        # Bug: returns [] because the service filter logic is broken
        # Even though there's no real task-to-service mapping, at minimum
        # the code should not silently drop all results when serviceName is set.
        # A correct implementation would either:
        # (a) return tasks associated with the service, or
        # (b) raise an error if service filtering is not supported.
        # Currently it returns an empty list, which is wrong.
        assert len(data["taskArns"]) > 0, (
            "ListTasks with serviceName should not silently return empty results"
        )


# ---------------------------------------------------------------------------
# Bug 3: PutClusterCapacityProviders doesn't resolve cluster ARNs
#
# All other cluster operations call _resolve_cluster_name() to handle both
# cluster names and ARNs. PutClusterCapacityProviders uses the raw param
# value, so passing an ARN fails with ClusterNotFoundException.
# ---------------------------------------------------------------------------


class TestPutClusterCapacityProvidersArnResolution:
    @pytest.mark.asyncio
    async def test_put_capacity_providers_with_arn(self):
        """PutClusterCapacityProviders should accept cluster ARNs, not just names."""
        cluster_arn = await _create_cluster("c1")

        # Use the ARN instead of the name — this should work but doesn't
        req = _make_request(
            "PutClusterCapacityProviders",
            {
                "cluster": cluster_arn,
                "capacityProviders": ["FARGATE"],
                "defaultCapacityProviderStrategy": [{"capacityProvider": "FARGATE", "weight": 1}],
            },
        )
        resp = await handle_ecs_request(req, REGION, ACCOUNT)
        # Bug: returns 404 because the ARN is not resolved to a cluster name
        assert resp.status_code == 200, (
            f"PutClusterCapacityProviders should accept ARNs, got {resp.status_code}"
        )
