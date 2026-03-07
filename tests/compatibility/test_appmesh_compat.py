"""Compatibility tests for AWS App Mesh service."""

import uuid

import pytest

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
        pass


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
        appmesh_client.delete_virtual_node(
            meshName=mesh_name, virtualNodeName=node_name
        )

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
        appmesh_client.delete_virtual_node(
            meshName=mesh_name, virtualNodeName=node_name
        )

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
        resp = appmesh_client.delete_virtual_node(
            meshName=mesh_name, virtualNodeName=node_name
        )
        node = resp["virtualNode"]
        assert node["virtualNodeName"] == node_name


class TestVirtualRouterOperations:
    def test_create_virtual_router(self, appmesh_client, mesh_name):
        router_name = f"router-{uuid.uuid4().hex[:8]}"
        resp = appmesh_client.create_virtual_router(
            meshName=mesh_name,
            virtualRouterName=router_name,
            spec={
                "listeners": [{"portMapping": {"port": 8080, "protocol": "http"}}]
            },
        )
        router = resp["virtualRouter"]
        assert router["virtualRouterName"] == router_name
        assert router["meshName"] == mesh_name
        # cleanup
        appmesh_client.delete_virtual_router(
            meshName=mesh_name, virtualRouterName=router_name
        )

    def test_list_virtual_routers(self, appmesh_client, mesh_name):
        router_name = f"router-{uuid.uuid4().hex[:8]}"
        appmesh_client.create_virtual_router(
            meshName=mesh_name,
            virtualRouterName=router_name,
            spec={
                "listeners": [{"portMapping": {"port": 9090, "protocol": "http"}}]
            },
        )
        resp = appmesh_client.list_virtual_routers(meshName=mesh_name)
        router_names = [r["virtualRouterName"] for r in resp["virtualRouters"]]
        assert router_name in router_names
        # cleanup
        appmesh_client.delete_virtual_router(
            meshName=mesh_name, virtualRouterName=router_name
        )

    def test_delete_virtual_router(self, appmesh_client, mesh_name):
        router_name = f"router-{uuid.uuid4().hex[:8]}"
        appmesh_client.create_virtual_router(
            meshName=mesh_name,
            virtualRouterName=router_name,
            spec={
                "listeners": [{"portMapping": {"port": 7070, "protocol": "http"}}]
            },
        )
        resp = appmesh_client.delete_virtual_router(
            meshName=mesh_name, virtualRouterName=router_name
        )
        router = resp["virtualRouter"]
        assert router["virtualRouterName"] == router_name
