"""MemoryDB compatibility tests."""

import pytest

from tests.compatibility.conftest import make_client


@pytest.fixture
def memorydb():
    return make_client("memorydb")


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
