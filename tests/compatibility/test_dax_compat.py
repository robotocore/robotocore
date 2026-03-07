"""DAX compatibility tests."""

import uuid

import pytest

from tests.compatibility.conftest import make_client


@pytest.fixture
def dax():
    return make_client("dax")


def _unique(prefix):
    return f"{prefix}-{uuid.uuid4().hex[:8]}"


class TestDAXClusterOperations:
    @pytest.fixture
    def cluster(self, dax):
        name = _unique("dax")
        resp = dax.create_cluster(
            ClusterName=name,
            NodeType="dax.r4.large",
            ReplicationFactor=1,
            IamRoleArn="arn:aws:iam::123456789012:role/DAXRole",
        )
        arn = resp["Cluster"]["ClusterArn"]
        yield {"name": name, "arn": arn}
        try:
            dax.delete_cluster(ClusterName=name)
        except Exception:
            pass

    def test_create_cluster(self, dax):
        name = _unique("dax")
        resp = dax.create_cluster(
            ClusterName=name,
            NodeType="dax.r4.large",
            ReplicationFactor=1,
            IamRoleArn="arn:aws:iam::123456789012:role/DAXRole",
        )
        cluster = resp["Cluster"]
        assert cluster["ClusterName"] == name
        assert cluster["Status"] == "creating"
        assert "ClusterArn" in cluster
        dax.delete_cluster(ClusterName=name)

    def test_describe_clusters_empty(self, dax):
        resp = dax.describe_clusters()
        assert "Clusters" in resp

    def test_describe_clusters_filtered(self, dax, cluster):
        resp = dax.describe_clusters(ClusterNames=[cluster["name"]])
        clusters = resp["Clusters"]
        assert len(clusters) == 1
        assert clusters[0]["ClusterName"] == cluster["name"]

    def test_delete_cluster(self, dax):
        name = _unique("dax")
        dax.create_cluster(
            ClusterName=name,
            NodeType="dax.r4.large",
            ReplicationFactor=1,
            IamRoleArn="arn:aws:iam::123456789012:role/DAXRole",
        )
        resp = dax.delete_cluster(ClusterName=name)
        assert resp["Cluster"]["ClusterName"] == name

    def test_list_tags(self, dax, cluster):
        resp = dax.list_tags(ResourceName=cluster["arn"])
        assert "Tags" in resp
        assert isinstance(resp["Tags"], list)
