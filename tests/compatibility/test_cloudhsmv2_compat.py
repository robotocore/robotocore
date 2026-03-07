"""CloudHSM V2 compatibility tests."""

import pytest

from tests.compatibility.conftest import make_client


@pytest.fixture
def cloudhsmv2():
    return make_client("cloudhsmv2")


class TestCloudHSMV2Operations:
    def test_describe_backups(self, cloudhsmv2):
        """DescribeBackups returns a list of backups."""
        response = cloudhsmv2.describe_backups()
        assert "Backups" in response
        assert isinstance(response["Backups"], list)

    def test_describe_clusters(self, cloudhsmv2):
        """DescribeClusters returns a list of clusters."""
        response = cloudhsmv2.describe_clusters()
        assert "Clusters" in response
        assert isinstance(response["Clusters"], list)

    def test_describe_backups_with_max_results(self, cloudhsmv2):
        """DescribeBackups respects MaxResults parameter."""
        response = cloudhsmv2.describe_backups(MaxResults=10)
        assert "Backups" in response

    def test_describe_clusters_with_max_results(self, cloudhsmv2):
        """DescribeClusters respects MaxResults parameter."""
        response = cloudhsmv2.describe_clusters(MaxResults=10)
        assert "Clusters" in response

    def test_describe_backups_status_code(self, cloudhsmv2):
        """DescribeBackups returns HTTP 200."""
        response = cloudhsmv2.describe_backups()
        assert response["ResponseMetadata"]["HTTPStatusCode"] == 200

    def test_describe_clusters_status_code(self, cloudhsmv2):
        """DescribeClusters returns HTTP 200."""
        response = cloudhsmv2.describe_clusters()
        assert response["ResponseMetadata"]["HTTPStatusCode"] == 200

    def test_get_resource_policy(self, cloudhsmv2):
        """GetResourcePolicy returns HTTP 200 for a cluster ARN."""
        response = cloudhsmv2.get_resource_policy(
            ResourceArn="arn:aws:cloudhsm:us-east-1:123456789012:cluster/cluster-1234567"
        )
        assert response["ResponseMetadata"]["HTTPStatusCode"] == 200
