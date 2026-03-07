"""EKS compatibility tests."""

import uuid

import pytest
from botocore.exceptions import ClientError

from tests.compatibility.conftest import make_client


@pytest.fixture
def eks():
    return make_client("eks")


def _unique(prefix):
    return f"{prefix}-{uuid.uuid4().hex[:8]}"


class TestEKSClusterOperations:
    """Tests for EKS cluster create, describe, list, tags, and delete."""

    def test_create_and_describe_cluster(self, eks):
        name = _unique("cluster")
        resp = eks.create_cluster(
            name=name,
            roleArn="arn:aws:iam::123456789012:role/eks-role",
            resourcesVpcConfig={
                "subnetIds": ["subnet-12345"],
                "securityGroupIds": ["sg-12345"],
            },
        )
        try:
            cluster = resp["cluster"]
            assert cluster["name"] == name
            assert cluster["status"] == "ACTIVE"
            assert "arn" in cluster
            assert cluster["roleArn"] == "arn:aws:iam::123456789012:role/eks-role"

            # Describe should return same info
            desc = eks.describe_cluster(name=name)["cluster"]
            assert desc["name"] == name
            assert desc["status"] == "ACTIVE"
            assert desc["arn"] == cluster["arn"]
            assert desc["roleArn"] == cluster["roleArn"]
        finally:
            eks.delete_cluster(name=name)

    def test_create_cluster_with_tags(self, eks):
        name = _unique("cluster")
        resp = eks.create_cluster(
            name=name,
            roleArn="arn:aws:iam::123456789012:role/eks-role",
            resourcesVpcConfig={
                "subnetIds": ["subnet-12345"],
                "securityGroupIds": ["sg-12345"],
            },
            tags={"env": "test", "project": "robotocore"},
        )
        try:
            cluster = resp["cluster"]
            assert cluster["tags"]["env"] == "test"
            assert cluster["tags"]["project"] == "robotocore"
        finally:
            eks.delete_cluster(name=name)

    def test_create_cluster_returns_expected_fields(self, eks):
        name = _unique("cluster")
        resp = eks.create_cluster(
            name=name,
            roleArn="arn:aws:iam::123456789012:role/eks-role",
            resourcesVpcConfig={
                "subnetIds": ["subnet-12345"],
                "securityGroupIds": ["sg-12345"],
            },
        )
        try:
            cluster = resp["cluster"]
            assert "endpoint" in cluster
            assert "certificateAuthority" in cluster
            assert "kubernetesNetworkConfig" in cluster
            assert "version" in cluster
        finally:
            eks.delete_cluster(name=name)

    def test_list_clusters_includes_created(self, eks):
        name = _unique("cluster")
        eks.create_cluster(
            name=name,
            roleArn="arn:aws:iam::123456789012:role/eks-role",
            resourcesVpcConfig={
                "subnetIds": ["subnet-12345"],
                "securityGroupIds": ["sg-12345"],
            },
        )
        try:
            clusters = eks.list_clusters()["clusters"]
            assert name in clusters
        finally:
            eks.delete_cluster(name=name)

    def test_delete_cluster(self, eks):
        name = _unique("cluster")
        eks.create_cluster(
            name=name,
            roleArn="arn:aws:iam::123456789012:role/eks-role",
            resourcesVpcConfig={
                "subnetIds": ["subnet-12345"],
                "securityGroupIds": ["sg-12345"],
            },
        )
        resp = eks.delete_cluster(name=name)
        assert resp["cluster"]["name"] == name

        # Should not appear in list
        clusters = eks.list_clusters()["clusters"]
        assert name not in clusters

    def test_describe_nonexistent_cluster_raises(self, eks):
        with pytest.raises(ClientError) as exc_info:
            eks.describe_cluster(name="nonexistent-cluster")
        assert exc_info.value.response["Error"]["Code"] == "ResourceNotFoundException"

    def test_delete_nonexistent_cluster_raises(self, eks):
        with pytest.raises(ClientError) as exc_info:
            eks.delete_cluster(name="nonexistent-cluster")
        assert exc_info.value.response["Error"]["Code"] == "ResourceNotFoundException"

    def test_create_duplicate_cluster_raises(self, eks):
        name = _unique("cluster")
        eks.create_cluster(
            name=name,
            roleArn="arn:aws:iam::123456789012:role/eks-role",
            resourcesVpcConfig={
                "subnetIds": ["subnet-12345"],
                "securityGroupIds": ["sg-12345"],
            },
        )
        try:
            with pytest.raises(ClientError) as exc_info:
                eks.create_cluster(
                    name=name,
                    roleArn="arn:aws:iam::123456789012:role/eks-role",
                    resourcesVpcConfig={
                        "subnetIds": ["subnet-12345"],
                        "securityGroupIds": ["sg-12345"],
                    },
                )
            assert exc_info.value.response["Error"]["Code"] == "ResourceInUseException"
        finally:
            eks.delete_cluster(name=name)

    def test_describe_after_delete_raises(self, eks):
        name = _unique("cluster")
        eks.create_cluster(
            name=name,
            roleArn="arn:aws:iam::123456789012:role/eks-role",
            resourcesVpcConfig={
                "subnetIds": ["subnet-12345"],
                "securityGroupIds": ["sg-12345"],
            },
        )
        eks.delete_cluster(name=name)

        with pytest.raises(ClientError) as exc_info:
            eks.describe_cluster(name=name)
        assert exc_info.value.response["Error"]["Code"] == "ResourceNotFoundException"


class TestEKSListOperations:
    """Tests for EKS list operations."""

    def test_list_clusters_empty(self, eks):
        # List clusters; result should be a list (may or may not be empty
        # depending on other test state, but the call should succeed)
        resp = eks.list_clusters()
        assert isinstance(resp["clusters"], list)

    def test_list_clusters_multiple(self, eks):
        names = [_unique("cluster") for _ in range(3)]
        for n in names:
            eks.create_cluster(
                name=n,
                roleArn="arn:aws:iam::123456789012:role/eks-role",
                resourcesVpcConfig={
                    "subnetIds": ["subnet-12345"],
                    "securityGroupIds": ["sg-12345"],
                },
            )
        try:
            clusters = eks.list_clusters()["clusters"]
            for n in names:
                assert n in clusters
        finally:
            for n in names:
                eks.delete_cluster(name=n)
