"""Compatibility tests for AWS App Mesh service."""

import uuid

import pytest
from botocore.exceptions import ClientError

from tests.compatibility.conftest import make_client


@pytest.fixture
def appmesh_client():
    return make_client("appmesh")


@pytest.fixture
def mesh_name(appmesh_client):
    name = f"test-mesh-{uuid.uuid4().hex[:8]}"
    appmesh_client.create_mesh(meshName=name)
    yield name
    try:
        appmesh_client.delete_mesh(meshName=name)
    except Exception:
        pass  # best-effort cleanup


class TestMeshOperations:
    def test_list_meshes(self, appmesh_client, mesh_name):
        resp = appmesh_client.list_meshes()
        mesh_names = [m["meshName"] for m in resp["meshes"]]
        assert mesh_name in mesh_names

    def test_create_mesh(self, appmesh_client):
        name = f"test-mesh-{uuid.uuid4().hex[:8]}"
        resp = appmesh_client.create_mesh(meshName=name)
        mesh = resp["mesh"]
        assert mesh["meshName"] == name
        assert "metadata" in mesh
        assert mesh["metadata"]["version"] >= 1
        # cleanup
        appmesh_client.delete_mesh(meshName=name)

    def test_describe_mesh(self, appmesh_client, mesh_name):
        resp = appmesh_client.describe_mesh(meshName=mesh_name)
        mesh = resp["mesh"]
        assert mesh["meshName"] == mesh_name
        assert "metadata" in mesh
        assert "spec" in mesh
        assert "status" in mesh

    def test_delete_mesh(self, appmesh_client):
        name = f"test-mesh-{uuid.uuid4().hex[:8]}"
        appmesh_client.create_mesh(meshName=name)
        resp = appmesh_client.delete_mesh(meshName=name)
        mesh = resp["mesh"]
        assert mesh["meshName"] == name


class TestVirtualNodeOperations:
    def test_create_virtual_node(self, appmesh_client, mesh_name):
        node_name = f"node-{uuid.uuid4().hex[:8]}"
        resp = appmesh_client.create_virtual_node(
            meshName=mesh_name,
            virtualNodeName=node_name,
            spec={
                "listeners": [],
                "serviceDiscovery": {"dns": {"hostname": "test.local"}},
            },
        )
        node = resp["virtualNode"]
        assert node["virtualNodeName"] == node_name
        assert node["meshName"] == mesh_name
        # cleanup
        appmesh_client.delete_virtual_node(meshName=mesh_name, virtualNodeName=node_name)

    def test_list_virtual_nodes(self, appmesh_client, mesh_name):
        node_name = f"node-{uuid.uuid4().hex[:8]}"
        appmesh_client.create_virtual_node(
            meshName=mesh_name,
            virtualNodeName=node_name,
            spec={
                "listeners": [],
                "serviceDiscovery": {"dns": {"hostname": "list.local"}},
            },
        )
        resp = appmesh_client.list_virtual_nodes(meshName=mesh_name)
        node_names = [n["virtualNodeName"] for n in resp["virtualNodes"]]
        assert node_name in node_names
        # cleanup
        appmesh_client.delete_virtual_node(meshName=mesh_name, virtualNodeName=node_name)

    def test_delete_virtual_node(self, appmesh_client, mesh_name):
        node_name = f"node-{uuid.uuid4().hex[:8]}"
        appmesh_client.create_virtual_node(
            meshName=mesh_name,
            virtualNodeName=node_name,
            spec={
                "listeners": [],
                "serviceDiscovery": {"dns": {"hostname": "del.local"}},
            },
        )
        resp = appmesh_client.delete_virtual_node(meshName=mesh_name, virtualNodeName=node_name)
        node = resp["virtualNode"]
        assert node["virtualNodeName"] == node_name

    def test_describe_virtual_node(self, appmesh_client, mesh_name):
        vn_name = f"node-{uuid.uuid4().hex[:8]}"
        appmesh_client.create_virtual_node(
            meshName=mesh_name,
            virtualNodeName=vn_name,
            spec={
                "listeners": [],
                "serviceDiscovery": {"dns": {"hostname": "desc.local"}},
            },
        )
        try:
            resp = appmesh_client.describe_virtual_node(meshName=mesh_name, virtualNodeName=vn_name)
            node = resp["virtualNode"]
            assert node["virtualNodeName"] == vn_name
            assert "spec" in node
            assert "status" in node
            assert "metadata" in node
        finally:
            appmesh_client.delete_virtual_node(meshName=mesh_name, virtualNodeName=vn_name)

    def test_update_virtual_node(self, appmesh_client, mesh_name):
        vn_name = f"node-{uuid.uuid4().hex[:8]}"
        appmesh_client.create_virtual_node(
            meshName=mesh_name,
            virtualNodeName=vn_name,
            spec={
                "listeners": [],
                "serviceDiscovery": {"dns": {"hostname": "orig.local"}},
            },
        )
        try:
            resp = appmesh_client.update_virtual_node(
                meshName=mesh_name,
                virtualNodeName=vn_name,
                spec={
                    "listeners": [],
                    "serviceDiscovery": {"dns": {"hostname": "updated.local"}},
                },
            )
            node = resp["virtualNode"]
            assert node["virtualNodeName"] == vn_name
            assert node["meshName"] == mesh_name
        finally:
            appmesh_client.delete_virtual_node(meshName=mesh_name, virtualNodeName=vn_name)


class TestVirtualRouterOperations:
    def test_create_virtual_router(self, appmesh_client, mesh_name):
        router_name = f"router-{uuid.uuid4().hex[:8]}"
        resp = appmesh_client.create_virtual_router(
            meshName=mesh_name,
            virtualRouterName=router_name,
            spec={"listeners": [{"portMapping": {"port": 8080, "protocol": "http"}}]},
        )
        router = resp["virtualRouter"]
        assert router["virtualRouterName"] == router_name
        assert router["meshName"] == mesh_name
        # cleanup
        appmesh_client.delete_virtual_router(meshName=mesh_name, virtualRouterName=router_name)

    def test_list_virtual_routers(self, appmesh_client, mesh_name):
        router_name = f"router-{uuid.uuid4().hex[:8]}"
        appmesh_client.create_virtual_router(
            meshName=mesh_name,
            virtualRouterName=router_name,
            spec={"listeners": [{"portMapping": {"port": 9090, "protocol": "http"}}]},
        )
        resp = appmesh_client.list_virtual_routers(meshName=mesh_name)
        router_names = [r["virtualRouterName"] for r in resp["virtualRouters"]]
        assert router_name in router_names
        # cleanup
        appmesh_client.delete_virtual_router(meshName=mesh_name, virtualRouterName=router_name)

    def test_delete_virtual_router(self, appmesh_client, mesh_name):
        router_name = f"router-{uuid.uuid4().hex[:8]}"
        appmesh_client.create_virtual_router(
            meshName=mesh_name,
            virtualRouterName=router_name,
            spec={"listeners": [{"portMapping": {"port": 7070, "protocol": "http"}}]},
        )
        resp = appmesh_client.delete_virtual_router(
            meshName=mesh_name, virtualRouterName=router_name
        )
        router = resp["virtualRouter"]
        assert router["virtualRouterName"] == router_name

    def test_describe_virtual_router(self, appmesh_client, mesh_name):
        rtr_name = f"router-{uuid.uuid4().hex[:8]}"
        appmesh_client.create_virtual_router(
            meshName=mesh_name,
            virtualRouterName=rtr_name,
            spec={"listeners": [{"portMapping": {"port": 6060, "protocol": "http"}}]},
        )
        try:
            resp = appmesh_client.describe_virtual_router(
                meshName=mesh_name, virtualRouterName=rtr_name
            )
            router = resp["virtualRouter"]
            assert router["virtualRouterName"] == rtr_name
            assert "spec" in router
            assert "status" in router
            assert "metadata" in router
        finally:
            appmesh_client.delete_virtual_router(meshName=mesh_name, virtualRouterName=rtr_name)

    def test_update_virtual_router(self, appmesh_client, mesh_name):
        rtr_name = f"router-{uuid.uuid4().hex[:8]}"
        appmesh_client.create_virtual_router(
            meshName=mesh_name,
            virtualRouterName=rtr_name,
            spec={"listeners": [{"portMapping": {"port": 5050, "protocol": "http"}}]},
        )
        try:
            resp = appmesh_client.update_virtual_router(
                meshName=mesh_name,
                virtualRouterName=rtr_name,
                spec={"listeners": [{"portMapping": {"port": 5051, "protocol": "http"}}]},
            )
            router = resp["virtualRouter"]
            assert router["virtualRouterName"] == rtr_name
            assert router["meshName"] == mesh_name
        finally:
            appmesh_client.delete_virtual_router(meshName=mesh_name, virtualRouterName=rtr_name)


class TestMeshAdditionalOperations:
    def test_update_mesh(self, appmesh_client, mesh_name):
        resp = appmesh_client.update_mesh(
            meshName=mesh_name,
            spec={"egressFilter": {"type": "ALLOW_ALL"}},
        )
        mesh = resp["mesh"]
        assert mesh["meshName"] == mesh_name
        assert "spec" in mesh

    def test_tag_resource(self, appmesh_client, mesh_name):
        # Get the mesh ARN
        desc = appmesh_client.describe_mesh(meshName=mesh_name)
        mesh_arn = desc["mesh"]["metadata"]["arn"]
        appmesh_client.tag_resource(
            resourceArn=mesh_arn,
            tags=[{"key": "env", "value": "test"}],
        )
        resp = appmesh_client.list_tags_for_resource(resourceArn=mesh_arn)
        tags = resp["tags"]
        tag_keys = [t["key"] for t in tags]
        assert "env" in tag_keys

    def test_list_tags_for_resource(self, appmesh_client, mesh_name):
        desc = appmesh_client.describe_mesh(meshName=mesh_name)
        mesh_arn = desc["mesh"]["metadata"]["arn"]
        resp = appmesh_client.list_tags_for_resource(resourceArn=mesh_arn)
        assert "tags" in resp


@pytest.fixture
def router_name(appmesh_client, mesh_name):
    name = f"router-{uuid.uuid4().hex[:8]}"
    appmesh_client.create_virtual_router(
        meshName=mesh_name,
        virtualRouterName=name,
        spec={"listeners": [{"portMapping": {"port": 8080, "protocol": "http"}}]},
    )
    yield name
    try:
        appmesh_client.delete_virtual_router(meshName=mesh_name, virtualRouterName=name)
    except Exception:
        pass  # best-effort cleanup


@pytest.fixture
def node_name(appmesh_client, mesh_name):
    name = f"node-{uuid.uuid4().hex[:8]}"
    appmesh_client.create_virtual_node(
        meshName=mesh_name,
        virtualNodeName=name,
        spec={
            "listeners": [{"portMapping": {"port": 8080, "protocol": "http"}}],
            "serviceDiscovery": {"dns": {"hostname": "test.local"}},
        },
    )
    yield name
    try:
        appmesh_client.delete_virtual_node(meshName=mesh_name, virtualNodeName=name)
    except Exception:
        pass  # best-effort cleanup


def _route_spec(node_name):
    return {
        "httpRoute": {
            "match": {"prefix": "/"},
            "action": {"weightedTargets": [{"virtualNode": node_name, "weight": 100}]},
        }
    }


class TestRouteOperations:
    def test_create_route(self, appmesh_client, mesh_name, router_name, node_name):
        route_name = f"route-{uuid.uuid4().hex[:8]}"
        resp = appmesh_client.create_route(
            meshName=mesh_name,
            virtualRouterName=router_name,
            routeName=route_name,
            spec=_route_spec(node_name),
        )
        route = resp["route"]
        assert route["routeName"] == route_name
        assert route["meshName"] == mesh_name
        assert route["virtualRouterName"] == router_name
        # cleanup
        appmesh_client.delete_route(
            meshName=mesh_name, virtualRouterName=router_name, routeName=route_name
        )

    def test_describe_route(self, appmesh_client, mesh_name, router_name, node_name):
        route_name = f"route-{uuid.uuid4().hex[:8]}"
        appmesh_client.create_route(
            meshName=mesh_name,
            virtualRouterName=router_name,
            routeName=route_name,
            spec=_route_spec(node_name),
        )
        try:
            resp = appmesh_client.describe_route(
                meshName=mesh_name, virtualRouterName=router_name, routeName=route_name
            )
            route = resp["route"]
            assert route["routeName"] == route_name
            assert "spec" in route
            assert "status" in route
            assert "metadata" in route
        finally:
            appmesh_client.delete_route(
                meshName=mesh_name, virtualRouterName=router_name, routeName=route_name
            )

    def test_update_route(self, appmesh_client, mesh_name, router_name, node_name):
        route_name = f"route-{uuid.uuid4().hex[:8]}"
        appmesh_client.create_route(
            meshName=mesh_name,
            virtualRouterName=router_name,
            routeName=route_name,
            spec=_route_spec(node_name),
        )
        try:
            resp = appmesh_client.update_route(
                meshName=mesh_name,
                virtualRouterName=router_name,
                routeName=route_name,
                spec={
                    "httpRoute": {
                        "match": {"prefix": "/api"},
                        "action": {"weightedTargets": [{"virtualNode": node_name, "weight": 100}]},
                    }
                },
            )
            route = resp["route"]
            assert route["routeName"] == route_name
            assert route["meshName"] == mesh_name
        finally:
            appmesh_client.delete_route(
                meshName=mesh_name, virtualRouterName=router_name, routeName=route_name
            )

    def test_delete_route(self, appmesh_client, mesh_name, router_name, node_name):
        route_name = f"route-{uuid.uuid4().hex[:8]}"
        appmesh_client.create_route(
            meshName=mesh_name,
            virtualRouterName=router_name,
            routeName=route_name,
            spec=_route_spec(node_name),
        )
        resp = appmesh_client.delete_route(
            meshName=mesh_name, virtualRouterName=router_name, routeName=route_name
        )
        route = resp["route"]
        assert route["routeName"] == route_name

    def test_list_routes(self, appmesh_client, mesh_name, router_name, node_name):
        route_name = f"route-{uuid.uuid4().hex[:8]}"
        appmesh_client.create_route(
            meshName=mesh_name,
            virtualRouterName=router_name,
            routeName=route_name,
            spec=_route_spec(node_name),
        )
        try:
            resp = appmesh_client.list_routes(meshName=mesh_name, virtualRouterName=router_name)
            route_names = [r["routeName"] for r in resp["routes"]]
            assert route_name in route_names
        finally:
            appmesh_client.delete_route(
                meshName=mesh_name, virtualRouterName=router_name, routeName=route_name
            )


class TestErrorPaths:
    """Tests that verify proper error responses for nonexistent resources."""

    def test_list_meshes_empty(self, appmesh_client):
        """ListMeshes returns a list even when no meshes exist."""
        resp = appmesh_client.list_meshes()
        assert "meshes" in resp

    def test_describe_mesh_not_found(self, appmesh_client):
        with pytest.raises(ClientError) as exc_info:
            appmesh_client.describe_mesh(meshName="nonexistent-mesh-xyz")
        assert "NotFound" in exc_info.value.response["Error"]["Code"]

    def test_describe_route_mesh_not_found(self, appmesh_client):
        with pytest.raises(ClientError) as exc_info:
            appmesh_client.describe_route(
                meshName="nonexistent-mesh-xyz",
                virtualRouterName="nonexistent-router",
                routeName="nonexistent-route",
            )
        assert "NotFound" in exc_info.value.response["Error"]["Code"]

    def test_describe_virtual_node_mesh_not_found(self, appmesh_client):
        with pytest.raises(ClientError) as exc_info:
            appmesh_client.describe_virtual_node(
                meshName="nonexistent-mesh-xyz",
                virtualNodeName="nonexistent-node",
            )
        assert "NotFound" in exc_info.value.response["Error"]["Code"]

    def test_describe_virtual_router_mesh_not_found(self, appmesh_client):
        with pytest.raises(ClientError) as exc_info:
            appmesh_client.describe_virtual_router(
                meshName="nonexistent-mesh-xyz",
                virtualRouterName="nonexistent-router",
            )
        assert "NotFound" in exc_info.value.response["Error"]["Code"]

    def test_list_routes_mesh_not_found(self, appmesh_client):
        with pytest.raises(ClientError) as exc_info:
            appmesh_client.list_routes(
                meshName="nonexistent-mesh-xyz",
                virtualRouterName="nonexistent-router",
            )
        assert "NotFound" in exc_info.value.response["Error"]["Code"]

    def test_list_virtual_nodes_mesh_not_found(self, appmesh_client):
        with pytest.raises(ClientError) as exc_info:
            appmesh_client.list_virtual_nodes(meshName="nonexistent-mesh-xyz")
        assert "NotFound" in exc_info.value.response["Error"]["Code"]

    def test_list_virtual_routers_mesh_not_found(self, appmesh_client):
        with pytest.raises(ClientError) as exc_info:
            appmesh_client.list_virtual_routers(meshName="nonexistent-mesh-xyz")
        assert "NotFound" in exc_info.value.response["Error"]["Code"]

    def test_list_tags_for_resource_not_found(self, appmesh_client):
        fake_arn = "arn:aws:appmesh:us-east-1:123456789012:mesh/nonexistent-mesh-xyz"
        with pytest.raises(ClientError) as exc_info:
            appmesh_client.list_tags_for_resource(resourceArn=fake_arn)
        assert "NotFound" in exc_info.value.response["Error"]["Code"]
