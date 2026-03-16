"""EMR Containers compatibility tests."""

import uuid

import pytest

from tests.compatibility.conftest import make_client


@pytest.fixture
def emr_containers():
    return make_client("emr-containers")


def _unique(prefix):
    return f"{prefix}-{uuid.uuid4().hex[:8]}"


@pytest.fixture
def virtual_cluster(emr_containers):
    namespace = f"ns-{uuid.uuid4().hex[:12]}"
    resp = emr_containers.create_virtual_cluster(
        name=_unique("test-vc"),
        containerProvider={
            "id": "eks-cluster-1",
            "type": "EKS",
            "info": {"eksInfo": {"namespace": namespace}},
        },
    )
    vc_id = resp["id"]
    yield resp
    try:
        emr_containers.delete_virtual_cluster(id=vc_id)
    except Exception:
        pass  # best-effort cleanup


class TestEMRContainersVirtualCluster:
    def test_list_virtual_clusters_empty(self, emr_containers):
        resp = emr_containers.list_virtual_clusters()
        assert "virtualClusters" in resp

    def test_create_virtual_cluster(self, emr_containers):
        namespace = f"ns-{uuid.uuid4().hex[:12]}"
        name = _unique("create-vc")
        resp = emr_containers.create_virtual_cluster(
            name=name,
            containerProvider={
                "id": "eks-cluster-1",
                "type": "EKS",
                "info": {"eksInfo": {"namespace": namespace}},
            },
        )
        assert "id" in resp
        assert resp["name"] == name
        assert "arn" in resp
        # Cleanup
        emr_containers.delete_virtual_cluster(id=resp["id"])

    def test_describe_virtual_cluster(self, emr_containers, virtual_cluster):
        vc_id = virtual_cluster["id"]
        resp = emr_containers.describe_virtual_cluster(id=vc_id)
        vc = resp["virtualCluster"]
        assert vc["id"] == vc_id
        assert vc["name"] == virtual_cluster["name"]
        assert "arn" in vc
        assert "containerProvider" in vc

    def test_delete_virtual_cluster(self, emr_containers):
        namespace = f"ns-{uuid.uuid4().hex[:12]}"
        resp = emr_containers.create_virtual_cluster(
            name=_unique("delete-vc"),
            containerProvider={
                "id": "eks-cluster-1",
                "type": "EKS",
                "info": {"eksInfo": {"namespace": namespace}},
            },
        )
        vc_id = resp["id"]
        delete_resp = emr_containers.delete_virtual_cluster(id=vc_id)
        assert delete_resp["id"] == vc_id

    def test_list_virtual_clusters_includes_created(self, emr_containers, virtual_cluster):
        vc_id = virtual_cluster["id"]
        resp = emr_containers.list_virtual_clusters()
        vc_ids = [vc["id"] for vc in resp["virtualClusters"]]
        assert vc_id in vc_ids
