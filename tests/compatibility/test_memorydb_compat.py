"""MemoryDB compatibility tests."""

import pytest

from tests.compatibility.conftest import make_client


@pytest.fixture
def memorydb():
    return make_client("memorydb")


@pytest.fixture
def ec2_client():
    return make_client("ec2")


class TestMemoryDBOperations:
    def test_describe_clusters(self, memorydb):
        """DescribeClusters returns a list of clusters."""
        response = memorydb.describe_clusters()
        assert "Clusters" in response
        assert isinstance(response["Clusters"], list)

    def test_describe_snapshots(self, memorydb):
        """DescribeSnapshots returns a list of snapshots."""
        response = memorydb.describe_snapshots()
        assert "Snapshots" in response
        assert isinstance(response["Snapshots"], list)

    def test_describe_subnet_groups(self, memorydb):
        """DescribeSubnetGroups returns a list of subnet groups."""
        response = memorydb.describe_subnet_groups()
        assert "SubnetGroups" in response
        assert isinstance(response["SubnetGroups"], list)

    def test_describe_clusters_status_code(self, memorydb):
        """DescribeClusters returns HTTP 200."""
        response = memorydb.describe_clusters()
        assert response["ResponseMetadata"]["HTTPStatusCode"] == 200

    def test_describe_snapshots_status_code(self, memorydb):
        """DescribeSnapshots returns HTTP 200."""
        response = memorydb.describe_snapshots()
        assert response["ResponseMetadata"]["HTTPStatusCode"] == 200

    def test_describe_subnet_groups_status_code(self, memorydb):
        """DescribeSubnetGroups returns HTTP 200."""
        response = memorydb.describe_subnet_groups()
        assert response["ResponseMetadata"]["HTTPStatusCode"] == 200


class TestMemoryDBCluster:
    """Tests for MemoryDB Cluster operations."""

    def test_create_cluster(self, memorydb):
        """CreateCluster creates a cluster and returns its details."""
        name = "compat-test-create-cl"
        try:
            resp = memorydb.create_cluster(
                ClusterName=name, NodeType="db.t4g.small", ACLName="open-access"
            )
            cluster = resp["Cluster"]
            assert cluster["Name"] == name
            assert cluster["NodeType"] == "db.t4g.small"
            assert "ARN" in cluster
            assert "Status" in cluster
        finally:
            memorydb.delete_cluster(ClusterName=name)

    def test_update_cluster(self, memorydb):
        """UpdateCluster modifies cluster description."""
        name = "compat-test-update-cl"
        memorydb.create_cluster(ClusterName=name, NodeType="db.t4g.small", ACLName="open-access")
        try:
            resp = memorydb.update_cluster(ClusterName=name, Description="updated-description")
            cluster = resp["Cluster"]
            assert cluster["Name"] == name
            assert cluster["Description"] == "updated-description"
        finally:
            memorydb.delete_cluster(ClusterName=name)


class TestMemoryDBSnapshot:
    """Tests for MemoryDB Snapshot operations."""

    def test_create_and_delete_snapshot(self, memorydb):
        """CreateSnapshot and DeleteSnapshot work end-to-end."""
        cluster_name = "compat-test-snap-cl"
        snap_name = "compat-test-snap-1"
        memorydb.create_cluster(
            ClusterName=cluster_name, NodeType="db.t4g.small", ACLName="open-access"
        )
        try:
            resp = memorydb.create_snapshot(ClusterName=cluster_name, SnapshotName=snap_name)
            snapshot = resp["Snapshot"]
            assert snapshot["Name"] == snap_name
            assert snapshot["ClusterConfiguration"]["Name"] == cluster_name
            assert "Status" in snapshot

            del_resp = memorydb.delete_snapshot(SnapshotName=snap_name)
            assert del_resp["Snapshot"]["Name"] == snap_name
        finally:
            memorydb.delete_cluster(ClusterName=cluster_name)


class TestMemoryDBSubnetGroup:
    """Tests for MemoryDB SubnetGroup operations."""

    def test_create_and_delete_subnet_group(self, memorydb, ec2_client):
        """CreateSubnetGroup and DeleteSubnetGroup work end-to-end."""
        # Get an existing subnet
        subnets = ec2_client.describe_subnets()["Subnets"]
        assert len(subnets) > 0, "Need at least one subnet"
        subnet_id = subnets[0]["SubnetId"]

        sg_name = "compat-test-sg-1"
        resp = memorydb.create_subnet_group(
            SubnetGroupName=sg_name, SubnetIds=[subnet_id], Description="test group"
        )
        sg = resp["SubnetGroup"]
        assert sg["Name"] == sg_name
        assert sg["Description"] == "test group"
        assert len(sg["Subnets"]) >= 1

        del_resp = memorydb.delete_subnet_group(SubnetGroupName=sg_name)
        assert del_resp["SubnetGroup"]["Name"] == sg_name


class TestMemoryDBTags:
    """Tests for MemoryDB Tag operations."""

    def test_tag_untag_list_tags(self, memorydb):
        """TagResource, ListTags, and UntagResource work on a cluster."""
        name = "compat-test-tag-cl"
        memorydb.create_cluster(ClusterName=name, NodeType="db.t4g.small", ACLName="open-access")
        try:
            # Get cluster ARN
            clusters = memorydb.describe_clusters(ClusterName=name)["Clusters"]
            arn = clusters[0]["ARN"]

            # Tag
            memorydb.tag_resource(ResourceArn=arn, Tags=[{"Key": "env", "Value": "test"}])

            # List tags
            tags_resp = memorydb.list_tags(ResourceArn=arn)
            assert "TagList" in tags_resp
            tag_keys = [t["Key"] for t in tags_resp["TagList"]]
            assert "env" in tag_keys

            # Untag
            memorydb.untag_resource(ResourceArn=arn, TagKeys=["env"])
            tags_after = memorydb.list_tags(ResourceArn=arn)
            tag_keys_after = [t["Key"] for t in tags_after["TagList"]]
            assert "env" not in tag_keys_after
        finally:
            memorydb.delete_cluster(ClusterName=name)
