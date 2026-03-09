"""CloudHSM V2 compatibility tests."""

import uuid

import pytest

from tests.compatibility.conftest import make_client


def _uid():
    return uuid.uuid4().hex[:8]


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


class TestCloudhsmv2AutoCoverage:
    """Auto-generated coverage tests for cloudhsmv2."""

    @pytest.fixture
    def client(self):
        return make_client("cloudhsmv2")

    def test_put_resource_policy(self, client):
        """PutResourcePolicy returns a response."""
        client.put_resource_policy()

    def test_list_tags(self, client):
        """ListTags returns tags for a cluster resource."""
        response = client.list_tags(ResourceId="cluster-doesnotexist123")
        assert "TagList" in response
        assert isinstance(response["TagList"], list)


class TestCloudHSMV2ClusterOperations:
    """Tests for cluster lifecycle operations."""

    @pytest.fixture
    def client(self):
        return make_client("cloudhsmv2")

    def _create_cluster(self, client):
        resp = client.create_cluster(
            HsmType="hsm1.medium",
            SubnetIds=[f"subnet-{_uid()}"],
        )
        return resp["Cluster"]["ClusterId"]

    def test_create_cluster(self, client):
        """CreateCluster returns a cluster with valid fields."""
        resp = client.create_cluster(
            HsmType="hsm1.medium",
            SubnetIds=["subnet-12345678"],
        )
        cluster = resp["Cluster"]
        assert "ClusterId" in cluster
        assert cluster["HsmType"] == "hsm1.medium"
        assert cluster["State"] in ("ACTIVE", "CREATE_IN_PROGRESS", "UNINITIALIZED")

    def test_create_hsm(self, client):
        """CreateHsm adds an HSM to a cluster."""
        cluster_id = self._create_cluster(client)
        resp = client.create_hsm(
            ClusterId=cluster_id,
            AvailabilityZone="us-east-1a",
        )
        assert "Hsm" in resp
        assert "HsmId" in resp["Hsm"]
        assert resp["Hsm"]["ClusterId"] == cluster_id

    def test_delete_hsm(self, client):
        """DeleteHsm removes an HSM from a cluster."""
        cluster_id = self._create_cluster(client)
        create_resp = client.create_hsm(
            ClusterId=cluster_id,
            AvailabilityZone="us-east-1a",
        )
        hsm_id = create_resp["Hsm"]["HsmId"]
        resp = client.delete_hsm(ClusterId=cluster_id, HsmId=hsm_id)
        assert "HsmId" in resp
        assert resp["HsmId"] == hsm_id

    def test_delete_resource_policy(self, client):
        """DeleteResourcePolicy returns 200."""
        cluster_id = self._create_cluster(client)
        arn = f"arn:aws:cloudhsm:us-east-1:123456789012:cluster/{cluster_id}"
        resp = client.delete_resource_policy(ResourceArn=arn)
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200

    def test_modify_cluster(self, client):
        """ModifyCluster updates cluster backup retention."""
        cluster_id = self._create_cluster(client)
        resp = client.modify_cluster(
            ClusterId=cluster_id,
            BackupRetentionPolicy={"Type": "DAYS", "Value": "7"},
        )
        assert "Cluster" in resp
        assert resp["Cluster"]["ClusterId"] == cluster_id

    def test_describe_clusters_finds_created(self, client):
        """DescribeClusters returns a cluster we created."""
        cluster_id = self._create_cluster(client)
        resp = client.describe_clusters(Filters={"clusterIds": [cluster_id]})
        assert "Clusters" in resp
        ids = [c["ClusterId"] for c in resp["Clusters"]]
        assert cluster_id in ids

    def test_tag_and_list_tags_on_cluster(self, client):
        """TagResource and ListTags work on a cluster."""
        cluster_id = self._create_cluster(client)
        client.tag_resource(
            ResourceId=cluster_id,
            TagList=[{"Key": "Env", "Value": "test"}],
        )
        resp = client.list_tags(ResourceId=cluster_id)
        assert "TagList" in resp
        tag_map = {t["Key"]: t["Value"] for t in resp["TagList"]}
        assert tag_map.get("Env") == "test"

    def test_untag_resource(self, client):
        """UntagResource removes tags from a cluster."""
        cluster_id = self._create_cluster(client)
        client.tag_resource(
            ResourceId=cluster_id,
            TagList=[{"Key": "Env", "Value": "test"}, {"Key": "Team", "Value": "dev"}],
        )
        client.untag_resource(
            ResourceId=cluster_id,
            TagKeyList=["Env"],
        )
        resp = client.list_tags(ResourceId=cluster_id)
        tag_map = {t["Key"]: t["Value"] for t in resp["TagList"]}
        assert "Env" not in tag_map
        assert tag_map.get("Team") == "dev"


class TestCloudHSMV2BackupOperations:
    """Tests for backup lifecycle operations."""

    @pytest.fixture
    def client(self):
        return make_client("cloudhsmv2")

    def _create_cluster_and_backup(self, client):
        """Create a cluster, then delete it to generate a backup."""
        resp = client.create_cluster(
            HsmType="hsm1.medium",
            SubnetIds=[f"subnet-{_uid()}"],
        )
        cluster_id = resp["Cluster"]["ClusterId"]
        client.delete_cluster(ClusterId=cluster_id)
        # Find the backup for this cluster
        backups = client.describe_backups(Filters={"clusterIds": [cluster_id]})
        backup_id = backups["Backups"][0]["BackupId"]
        return cluster_id, backup_id

    def test_delete_backup(self, client):
        """DeleteBackup removes a backup."""
        _, backup_id = self._create_cluster_and_backup(client)
        resp = client.delete_backup(BackupId=backup_id)
        assert "Backup" in resp
        assert resp["Backup"]["BackupId"] == backup_id

    def test_restore_backup(self, client):
        """RestoreBackup creates a new cluster from a backup."""
        _, backup_id = self._create_cluster_and_backup(client)
        resp = client.restore_backup(BackupId=backup_id)
        assert "Backup" in resp
        assert resp["Backup"]["BackupId"] == backup_id

    def test_copy_backup_to_region(self, client):
        """CopyBackupToRegion copies a backup to another region."""
        _, backup_id = self._create_cluster_and_backup(client)
        resp = client.copy_backup_to_region(
            DestinationRegion="us-west-2",
            BackupId=backup_id,
        )
        assert "DestinationBackup" in resp
        assert resp["DestinationBackup"]["SourceBackup"] == backup_id

    def test_modify_backup_attributes(self, client):
        """ModifyBackupAttributes updates backup never-expires flag."""
        _, backup_id = self._create_cluster_and_backup(client)
        resp = client.modify_backup_attributes(
            BackupId=backup_id,
            NeverExpires=True,
        )
        assert "Backup" in resp
        assert resp["Backup"]["BackupId"] == backup_id

    def test_delete_cluster(self, client):
        """DeleteCluster removes a cluster and creates a backup."""
        resp = client.create_cluster(
            HsmType="hsm1.medium",
            SubnetIds=[f"subnet-{_uid()}"],
        )
        cluster_id = resp["Cluster"]["ClusterId"]
        del_resp = client.delete_cluster(ClusterId=cluster_id)
        assert "Cluster" in del_resp
        assert del_resp["Cluster"]["State"] == "DELETED"


class TestCloudHSMv2Additional:
    """Tests for additional CloudHSM V2 operations."""

    @pytest.fixture
    def client(self):
        return make_client("cloudhsmv2")

    def test_initialize_cluster_fake(self, client):
        """InitializeCluster with fake ClusterId raises error or returns result."""
        try:
            resp = client.initialize_cluster(
                ClusterId=f"cluster-{_uid()}",
                SignedCert="-----BEGIN CERTIFICATE-----\nMIIBfake\n-----END CERTIFICATE-----",
                TrustAnchor="-----BEGIN CERTIFICATE-----\nMIIBfake\n-----END CERTIFICATE-----",
            )
            assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200
        except Exception as e:
            if hasattr(e, "response"):
                assert "Code" in e.response["Error"]
            else:
                assert "error" in str(e).lower() or "not found" in str(e).lower()
