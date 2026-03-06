"""Redshift compatibility tests."""

import pytest

from tests.compatibility.conftest import make_client


@pytest.fixture
def redshift():
    return make_client("redshift")


class TestRedshiftOperations:
    def test_create_cluster(self, redshift):
        response = redshift.create_cluster(
            ClusterIdentifier="test-cluster",
            NodeType="dc2.large",
            MasterUsername="admin",
            MasterUserPassword="Password1!",
            NumberOfNodes=1,
            ClusterType="single-node",
        )
        cluster = response["Cluster"]
        assert cluster["ClusterIdentifier"] == "test-cluster"
        assert cluster["NodeType"] == "dc2.large"
        assert cluster["MasterUsername"] == "admin"

        # Cleanup
        redshift.delete_cluster(
            ClusterIdentifier="test-cluster",
            SkipFinalClusterSnapshot=True,
        )

    def test_describe_clusters(self, redshift):
        redshift.create_cluster(
            ClusterIdentifier="describe-cluster",
            NodeType="dc2.large",
            MasterUsername="admin",
            MasterUserPassword="Password1!",
            NumberOfNodes=1,
            ClusterType="single-node",
        )
        response = redshift.describe_clusters(ClusterIdentifier="describe-cluster")
        assert len(response["Clusters"]) == 1
        assert response["Clusters"][0]["ClusterIdentifier"] == "describe-cluster"

        # Cleanup
        redshift.delete_cluster(
            ClusterIdentifier="describe-cluster",
            SkipFinalClusterSnapshot=True,
        )

    def test_delete_cluster(self, redshift):
        redshift.create_cluster(
            ClusterIdentifier="delete-cluster",
            NodeType="dc2.large",
            MasterUsername="admin",
            MasterUserPassword="Password1!",
            NumberOfNodes=1,
            ClusterType="single-node",
        )
        response = redshift.delete_cluster(
            ClusterIdentifier="delete-cluster",
            SkipFinalClusterSnapshot=True,
        )
        assert response["Cluster"]["ClusterIdentifier"] == "delete-cluster"

    def test_create_cluster_snapshot(self, redshift):
        redshift.create_cluster(
            ClusterIdentifier="snapshot-cluster",
            NodeType="dc2.large",
            MasterUsername="admin",
            MasterUserPassword="Password1!",
            NumberOfNodes=1,
            ClusterType="single-node",
        )
        response = redshift.create_cluster_snapshot(
            SnapshotIdentifier="test-snapshot",
            ClusterIdentifier="snapshot-cluster",
        )
        assert response["Snapshot"]["SnapshotIdentifier"] == "test-snapshot"
        assert response["Snapshot"]["ClusterIdentifier"] == "snapshot-cluster"

        # Cleanup
        redshift.delete_cluster_snapshot(SnapshotIdentifier="test-snapshot")
        redshift.delete_cluster(
            ClusterIdentifier="snapshot-cluster",
            SkipFinalClusterSnapshot=True,
        )

    def test_create_cluster_parameter_group(self, redshift):
        response = redshift.create_cluster_parameter_group(
            ParameterGroupName="test-param-group",
            ParameterGroupFamily="redshift-1.0",
            Description="Test parameter group",
        )
        assert response["ClusterParameterGroup"]["ParameterGroupName"] == "test-param-group"

        # Verify it shows up in listing
        desc_response = redshift.describe_cluster_parameter_groups(
            ParameterGroupName="test-param-group"
        )
        assert len(desc_response["ParameterGroups"]) == 1

        # Cleanup
        redshift.delete_cluster_parameter_group(ParameterGroupName="test-param-group")
