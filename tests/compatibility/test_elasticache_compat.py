"""ElastiCache compatibility tests."""

import uuid

import pytest
from botocore.exceptions import ClientError

from tests.compatibility.conftest import make_client


@pytest.fixture
def elasticache():
    return make_client("elasticache")


def _unique(prefix):
    return f"{prefix}-{uuid.uuid4().hex[:8]}"


class TestElastiCacheSubnetGroupOperations:
    def test_create_subnet_group(self, elasticache):
        name = _unique("sg")
        resp = elasticache.create_cache_subnet_group(
            CacheSubnetGroupName=name,
            CacheSubnetGroupDescription="test subnet group",
            SubnetIds=["subnet-12345678"],
        )
        group = resp["CacheSubnetGroup"]
        assert group["CacheSubnetGroupName"] == name
        assert group["CacheSubnetGroupDescription"] == "test subnet group"
        assert "ARN" in group

    def test_describe_subnet_groups(self, elasticache):
        name = _unique("sg")
        elasticache.create_cache_subnet_group(
            CacheSubnetGroupName=name,
            CacheSubnetGroupDescription="for describe",
            SubnetIds=["subnet-12345678"],
        )
        desc = elasticache.describe_cache_subnet_groups(CacheSubnetGroupName=name)
        groups = desc["CacheSubnetGroups"]
        assert len(groups) == 1
        assert groups[0]["CacheSubnetGroupName"] == name

    def test_describe_subnet_groups_all(self, elasticache):
        resp = elasticache.describe_cache_subnet_groups()
        assert "CacheSubnetGroups" in resp

    def test_create_subnet_group_duplicate_error(self, elasticache):
        name = _unique("sg")
        elasticache.create_cache_subnet_group(
            CacheSubnetGroupName=name,
            CacheSubnetGroupDescription="first",
            SubnetIds=["subnet-12345678"],
        )
        with pytest.raises(ClientError) as exc:
            elasticache.create_cache_subnet_group(
                CacheSubnetGroupName=name,
                CacheSubnetGroupDescription="duplicate",
                SubnetIds=["subnet-12345678"],
            )
        assert "AlreadyExists" in exc.value.response["Error"]["Code"] or "Duplicate" in str(
            exc.value
        )


class TestElastiCacheDescribeOperations:
    def test_describe_replication_groups_empty(self, elasticache):
        resp = elasticache.describe_replication_groups()
        assert "ReplicationGroups" in resp

    def test_describe_cache_clusters_empty(self, elasticache):
        resp = elasticache.describe_cache_clusters()
        assert "CacheClusters" in resp


class TestElastiCacheTags:
    @pytest.fixture
    def subnet_group_arn(self, elasticache):
        name = _unique("sg")
        resp = elasticache.create_cache_subnet_group(
            CacheSubnetGroupName=name,
            CacheSubnetGroupDescription="for tagging",
            SubnetIds=["subnet-12345678"],
        )
        arn = resp["CacheSubnetGroup"]["ARN"]
        yield arn

    def test_add_and_list_tags(self, elasticache, subnet_group_arn):
        elasticache.add_tags_to_resource(
            ResourceName=subnet_group_arn,
            Tags=[
                {"Key": "env", "Value": "test"},
                {"Key": "project", "Value": "robotocore"},
            ],
        )
        resp = elasticache.list_tags_for_resource(ResourceName=subnet_group_arn)
        tags = {t["Key"]: t["Value"] for t in resp["TagList"]}
        assert tags["env"] == "test"
        assert tags["project"] == "robotocore"

    def test_remove_tags(self, elasticache, subnet_group_arn):
        elasticache.add_tags_to_resource(
            ResourceName=subnet_group_arn,
            Tags=[
                {"Key": "env", "Value": "test"},
                {"Key": "project", "Value": "robotocore"},
            ],
        )
        elasticache.remove_tags_from_resource(
            ResourceName=subnet_group_arn,
            TagKeys=["env"],
        )
        resp = elasticache.list_tags_for_resource(ResourceName=subnet_group_arn)
        tags = {t["Key"]: t["Value"] for t in resp["TagList"]}
        assert "env" not in tags
        assert tags["project"] == "robotocore"


class TestElastiCacheReplicationGroupOperations:
    @pytest.fixture
    def client(self):
        return make_client("elasticache")

    def test_create_and_delete_replication_group(self, client):
        rg_id = _unique("rg")
        resp = client.create_replication_group(
            ReplicationGroupId=rg_id,
            ReplicationGroupDescription="test replication group",
        )
        group = resp["ReplicationGroup"]
        assert group["ReplicationGroupId"] == rg_id
        assert group["Description"] == "test replication group"
        try:
            desc = client.describe_replication_groups(ReplicationGroupId=rg_id)
            assert len(desc["ReplicationGroups"]) == 1
            assert desc["ReplicationGroups"][0]["ReplicationGroupId"] == rg_id
        finally:
            del_resp = client.delete_replication_group(ReplicationGroupId=rg_id)
            assert "ReplicationGroup" in del_resp

    def test_delete_nonexistent_replication_group(self, client):
        with pytest.raises(ClientError) as exc:
            client.delete_replication_group(ReplicationGroupId="does-not-exist")
        assert exc.value.response["Error"]["Code"] in (
            "ReplicationGroupNotFoundFault",
            "ReplicationGroupNotFound",
        )

    def test_create_replication_group_has_status(self, client):
        rg_id = _unique("rg")
        resp = client.create_replication_group(
            ReplicationGroupId=rg_id,
            ReplicationGroupDescription="status check",
        )
        group = resp["ReplicationGroup"]
        assert "Status" in group
        assert "MemberClusters" in group
        client.delete_replication_group(ReplicationGroupId=rg_id)

    def test_describe_nonexistent_replication_group(self, client):
        with pytest.raises(ClientError) as exc:
            client.describe_replication_groups(ReplicationGroupId="does-not-exist-rg")
        assert exc.value.response["Error"]["Code"] in (
            "ReplicationGroupNotFoundFault",
            "ReplicationGroupNotFound",
        )

    def test_create_replication_group_duplicate_error(self, client):
        rg_id = _unique("rg")
        client.create_replication_group(
            ReplicationGroupId=rg_id,
            ReplicationGroupDescription="first",
        )
        try:
            with pytest.raises(ClientError) as exc:
                client.create_replication_group(
                    ReplicationGroupId=rg_id,
                    ReplicationGroupDescription="duplicate",
                )
            assert "AlreadyExists" in exc.value.response["Error"]["Code"] or "Duplicate" in str(
                exc.value
            )
        finally:
            client.delete_replication_group(ReplicationGroupId=rg_id)


class TestElastiCacheUserOperations:
    def test_create_and_describe_user(self, elasticache):
        user_id = _unique("user")
        resp = elasticache.create_user(
            UserId=user_id,
            UserName="testuser",
            Engine="redis",
            AccessString="on ~* +@all",
            NoPasswordRequired=True,
        )
        assert resp["UserId"] == user_id
        assert resp["UserName"] == "testuser"

        desc = elasticache.describe_users(UserId=user_id)
        assert len(desc["Users"]) == 1
        assert desc["Users"][0]["UserId"] == user_id

        elasticache.delete_user(UserId=user_id)

    def test_delete_user(self, elasticache):
        user_id = _unique("user")
        elasticache.create_user(
            UserId=user_id,
            UserName="testuser3",
            Engine="redis",
            AccessString="on ~* +@all",
            NoPasswordRequired=True,
        )
        resp = elasticache.delete_user(UserId=user_id)
        assert resp["UserId"] == user_id

    def test_create_user_has_engine_and_arn(self, elasticache):
        user_id = _unique("user")
        resp = elasticache.create_user(
            UserId=user_id,
            UserName="testengine",
            Engine="redis",
            AccessString="on ~* +@all",
            NoPasswordRequired=True,
        )
        assert resp["Engine"] == "redis"
        assert "ARN" in resp
        assert resp["Status"] in ("active", "creating", "modifying")
        elasticache.delete_user(UserId=user_id)

    def test_describe_users_all(self, elasticache):
        resp = elasticache.describe_users()
        assert "Users" in resp
        assert isinstance(resp["Users"], list)

    def test_delete_nonexistent_user(self, elasticache):
        with pytest.raises(ClientError) as exc:
            elasticache.delete_user(UserId="nonexistent-user-id")
        assert exc.value.response["Error"]["Code"] in (
            "UserNotFound",
            "UserNotFoundFault",
        )

    def test_create_user_duplicate_error(self, elasticache):
        user_id = _unique("user")
        elasticache.create_user(
            UserId=user_id,
            UserName="dupuser",
            Engine="redis",
            AccessString="on ~* +@all",
            NoPasswordRequired=True,
        )
        try:
            with pytest.raises(ClientError) as exc:
                elasticache.create_user(
                    UserId=user_id,
                    UserName="dupuser2",
                    Engine="redis",
                    AccessString="on ~* +@all",
                    NoPasswordRequired=True,
                )
            assert "AlreadyExists" in exc.value.response["Error"]["Code"] or "Duplicate" in str(
                exc.value
            )
        finally:
            elasticache.delete_user(UserId=user_id)


class TestElastiCacheCacheClusterOperations:
    def test_create_and_describe_cache_cluster(self, elasticache):
        cc_id = _unique("cc")
        resp = elasticache.create_cache_cluster(
            CacheClusterId=cc_id,
            NumCacheNodes=1,
            CacheNodeType="cache.t2.micro",
            Engine="redis",
        )
        cluster = resp["CacheCluster"]
        assert cluster["CacheClusterId"] == cc_id
        assert cluster["Engine"] == "redis"
        assert cluster["CacheNodeType"] == "cache.t2.micro"
        assert cluster["NumCacheNodes"] == 1

        desc = elasticache.describe_cache_clusters(CacheClusterId=cc_id)
        assert len(desc["CacheClusters"]) == 1
        assert desc["CacheClusters"][0]["CacheClusterId"] == cc_id

        elasticache.delete_cache_cluster(CacheClusterId=cc_id)

    def test_create_cache_cluster_has_status(self, elasticache):
        cc_id = _unique("cc")
        resp = elasticache.create_cache_cluster(
            CacheClusterId=cc_id,
            NumCacheNodes=1,
            CacheNodeType="cache.t2.micro",
            Engine="redis",
        )
        cluster = resp["CacheCluster"]
        assert "CacheClusterStatus" in cluster
        assert "CacheClusterCreateTime" in cluster or "CacheClusterStatus" in cluster
        elasticache.delete_cache_cluster(CacheClusterId=cc_id)

    def test_create_cache_cluster_memcached(self, elasticache):
        cc_id = _unique("cc")
        resp = elasticache.create_cache_cluster(
            CacheClusterId=cc_id,
            NumCacheNodes=1,
            CacheNodeType="cache.t2.micro",
            Engine="memcached",
        )
        cluster = resp["CacheCluster"]
        assert cluster["Engine"] == "memcached"
        elasticache.delete_cache_cluster(CacheClusterId=cc_id)

    def test_delete_cache_cluster(self, elasticache):
        cc_id = _unique("cc")
        elasticache.create_cache_cluster(
            CacheClusterId=cc_id,
            NumCacheNodes=1,
            CacheNodeType="cache.t2.micro",
            Engine="redis",
        )
        resp = elasticache.delete_cache_cluster(CacheClusterId=cc_id)
        assert "CacheCluster" in resp
        assert resp["CacheCluster"]["CacheClusterId"] == cc_id

    def test_delete_nonexistent_cache_cluster(self, elasticache):
        with pytest.raises(ClientError) as exc:
            elasticache.delete_cache_cluster(CacheClusterId="nonexistent-cc")
        assert exc.value.response["Error"]["Code"] in (
            "CacheClusterNotFound",
            "CacheClusterNotFoundFault",
        )

    def test_describe_nonexistent_cache_cluster(self, elasticache):
        with pytest.raises(ClientError) as exc:
            elasticache.describe_cache_clusters(CacheClusterId="nonexistent-cc")
        assert exc.value.response["Error"]["Code"] in (
            "CacheClusterNotFound",
            "CacheClusterNotFoundFault",
        )

    def test_create_cache_cluster_duplicate_error(self, elasticache):
        cc_id = _unique("cc")
        elasticache.create_cache_cluster(
            CacheClusterId=cc_id,
            NumCacheNodes=1,
            CacheNodeType="cache.t2.micro",
            Engine="redis",
        )
        try:
            with pytest.raises(ClientError) as exc:
                elasticache.create_cache_cluster(
                    CacheClusterId=cc_id,
                    NumCacheNodes=1,
                    CacheNodeType="cache.t2.micro",
                    Engine="redis",
                )
            assert "AlreadyExists" in exc.value.response["Error"]["Code"] or "Duplicate" in str(
                exc.value
            )
        finally:
            elasticache.delete_cache_cluster(CacheClusterId=cc_id)

    def test_create_cache_cluster_with_tags(self, elasticache):
        cc_id = _unique("cc")
        resp = elasticache.create_cache_cluster(
            CacheClusterId=cc_id,
            NumCacheNodes=1,
            CacheNodeType="cache.t2.micro",
            Engine="redis",
            Tags=[{"Key": "env", "Value": "test"}],
        )
        cluster = resp["CacheCluster"]
        assert cluster["CacheClusterId"] == cc_id
        elasticache.delete_cache_cluster(CacheClusterId=cc_id)


class TestElastiCacheSnapshotOperations:
    def test_create_and_describe_snapshot(self, elasticache):
        rg_id = _unique("rg")
        elasticache.create_replication_group(
            ReplicationGroupId=rg_id,
            ReplicationGroupDescription="for snapshot",
        )
        snap_name = _unique("snap")
        resp = elasticache.create_snapshot(
            SnapshotName=snap_name,
            ReplicationGroupId=rg_id,
        )
        assert resp["Snapshot"]["SnapshotName"] == snap_name
        assert resp["Snapshot"]["ReplicationGroupId"] == rg_id

        desc = elasticache.describe_snapshots(SnapshotName=snap_name)
        assert len(desc["Snapshots"]) == 1
        assert desc["Snapshots"][0]["SnapshotName"] == snap_name

        elasticache.delete_snapshot(SnapshotName=snap_name)
        elasticache.delete_replication_group(ReplicationGroupId=rg_id)

    def test_describe_snapshots_all(self, elasticache):
        resp = elasticache.describe_snapshots()
        assert "Snapshots" in resp

    def test_delete_snapshot(self, elasticache):
        rg_id = _unique("rg")
        elasticache.create_replication_group(
            ReplicationGroupId=rg_id,
            ReplicationGroupDescription="for snap del",
        )
        snap_name = _unique("snap")
        elasticache.create_snapshot(
            SnapshotName=snap_name,
            ReplicationGroupId=rg_id,
        )
        resp = elasticache.delete_snapshot(SnapshotName=snap_name)
        assert resp["Snapshot"]["SnapshotName"] == snap_name
        elasticache.delete_replication_group(ReplicationGroupId=rg_id)

    def test_describe_nonexistent_snapshot(self, elasticache):
        with pytest.raises(ClientError) as exc:
            elasticache.describe_snapshots(SnapshotName="nonexistent-snap")
        assert exc.value.response["Error"]["Code"] in (
            "SnapshotNotFoundFault",
            "SnapshotNotFound",
        )

    def test_delete_nonexistent_snapshot(self, elasticache):
        with pytest.raises(ClientError) as exc:
            elasticache.delete_snapshot(SnapshotName="nonexistent-snap")
        assert exc.value.response["Error"]["Code"] in (
            "SnapshotNotFoundFault",
            "SnapshotNotFound",
        )

    def test_describe_snapshots_by_replication_group(self, elasticache):
        rg_id = _unique("rg")
        elasticache.create_replication_group(
            ReplicationGroupId=rg_id,
            ReplicationGroupDescription="for snap by rg",
        )
        snap_name = _unique("snap")
        elasticache.create_snapshot(
            SnapshotName=snap_name,
            ReplicationGroupId=rg_id,
        )
        desc = elasticache.describe_snapshots(ReplicationGroupId=rg_id)
        assert len(desc["Snapshots"]) >= 1
        snap_names = [s["SnapshotName"] for s in desc["Snapshots"]]
        assert snap_name in snap_names

        elasticache.delete_snapshot(SnapshotName=snap_name)
        elasticache.delete_replication_group(ReplicationGroupId=rg_id)

    def test_snapshot_has_status_and_source(self, elasticache):
        rg_id = _unique("rg")
        elasticache.create_replication_group(
            ReplicationGroupId=rg_id,
            ReplicationGroupDescription="for snap fields",
        )
        snap_name = _unique("snap")
        resp = elasticache.create_snapshot(
            SnapshotName=snap_name,
            ReplicationGroupId=rg_id,
        )
        snap = resp["Snapshot"]
        assert "SnapshotStatus" in snap
        assert "SnapshotSource" in snap

        elasticache.delete_snapshot(SnapshotName=snap_name)
        elasticache.delete_replication_group(ReplicationGroupId=rg_id)
