"""Redshift compatibility tests."""

import uuid

import pytest
from botocore.exceptions import ClientError

from tests.compatibility.conftest import make_client


def _uid():
    return uuid.uuid4().hex[:8]


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

    def test_describe_clusters_empty(self, redshift):
        response = redshift.describe_clusters()
        assert "Clusters" in response

    def test_create_cluster_subnet_group(self, redshift):
        ec2 = make_client("ec2")
        vpc = ec2.create_vpc(CidrBlock="10.200.0.0/16")
        vpc_id = vpc["Vpc"]["VpcId"]
        subnet = ec2.create_subnet(VpcId=vpc_id, CidrBlock="10.200.1.0/24")
        subnet_id = subnet["Subnet"]["SubnetId"]
        name = f"test-sg-{_uid()}"
        response = redshift.create_cluster_subnet_group(
            ClusterSubnetGroupName=name,
            Description="Test subnet group",
            SubnetIds=[subnet_id],
        )
        assert response["ClusterSubnetGroup"]["ClusterSubnetGroupName"] == name
        redshift.delete_cluster_subnet_group(ClusterSubnetGroupName=name)

    def test_create_cluster_with_tags(self, redshift):
        cid = f"tagged-{_uid()}"
        response = redshift.create_cluster(
            ClusterIdentifier=cid,
            NodeType="dc2.large",
            MasterUsername="admin",
            MasterUserPassword="Password1!",
            NumberOfNodes=1,
            ClusterType="single-node",
            Tags=[{"Key": "env", "Value": "test"}],
        )
        tags = {t["Key"]: t["Value"] for t in response["Cluster"].get("Tags", [])}
        assert tags.get("env") == "test"
        redshift.delete_cluster(ClusterIdentifier=cid, SkipFinalClusterSnapshot=True)

    def test_describe_cluster_not_found(self, redshift):
        with pytest.raises(ClientError) as exc:
            redshift.describe_clusters(ClusterIdentifier="nonexistent-cluster-xyz")
        assert "ClusterNotFound" in exc.value.response["Error"]["Code"]

    def test_describe_cluster_snapshots(self, redshift):
        cid = f"snap-{_uid()}"
        redshift.create_cluster(
            ClusterIdentifier=cid,
            NodeType="dc2.large",
            MasterUsername="admin",
            MasterUserPassword="Password1!",
            NumberOfNodes=1,
            ClusterType="single-node",
        )
        snap_name = f"snap-{_uid()}"
        redshift.create_cluster_snapshot(SnapshotIdentifier=snap_name, ClusterIdentifier=cid)
        response = redshift.describe_cluster_snapshots(SnapshotIdentifier=snap_name)
        assert len(response["Snapshots"]) == 1
        assert response["Snapshots"][0]["SnapshotIdentifier"] == snap_name
        redshift.delete_cluster_snapshot(SnapshotIdentifier=snap_name)
        redshift.delete_cluster(ClusterIdentifier=cid, SkipFinalClusterSnapshot=True)
