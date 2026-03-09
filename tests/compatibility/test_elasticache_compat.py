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


class TestElastiCacheTagsOnCacheCluster:
    """Tags on cache cluster resources."""

    def test_add_and_list_tags_on_cluster(self, elasticache):
        cc_id = _unique("cc")
        resp = elasticache.create_cache_cluster(
            CacheClusterId=cc_id,
            NumCacheNodes=1,
            CacheNodeType="cache.t2.micro",
            Engine="redis",
        )
        arn = resp["CacheCluster"]["ARN"]
        try:
            elasticache.add_tags_to_resource(
                ResourceName=arn,
                Tags=[
                    {"Key": "env", "Value": "staging"},
                    {"Key": "team", "Value": "infra"},
                ],
            )
            tags_resp = elasticache.list_tags_for_resource(ResourceName=arn)
            tag_map = {t["Key"]: t["Value"] for t in tags_resp["TagList"]}
            assert tag_map["env"] == "staging"
            assert tag_map["team"] == "infra"
        finally:
            elasticache.delete_cache_cluster(CacheClusterId=cc_id)

    def test_remove_tags_from_cluster(self, elasticache):
        cc_id = _unique("cc")
        resp = elasticache.create_cache_cluster(
            CacheClusterId=cc_id,
            NumCacheNodes=1,
            CacheNodeType="cache.t2.micro",
            Engine="redis",
        )
        arn = resp["CacheCluster"]["ARN"]
        try:
            elasticache.add_tags_to_resource(
                ResourceName=arn,
                Tags=[
                    {"Key": "env", "Value": "test"},
                    {"Key": "remove-me", "Value": "yes"},
                ],
            )
            elasticache.remove_tags_from_resource(
                ResourceName=arn,
                TagKeys=["remove-me"],
            )
            tags_resp = elasticache.list_tags_for_resource(ResourceName=arn)
            tag_keys = [t["Key"] for t in tags_resp["TagList"]]
            assert "env" in tag_keys
            assert "remove-me" not in tag_keys
        finally:
            elasticache.delete_cache_cluster(CacheClusterId=cc_id)


class TestElastiCacheTagsOnReplicationGroup:
    """Tags on replication group resources."""

    def test_add_and_list_tags_on_replication_group(self, elasticache):
        rg_id = _unique("rg")
        resp = elasticache.create_replication_group(
            ReplicationGroupId=rg_id,
            ReplicationGroupDescription="tag test",
        )
        arn = resp["ReplicationGroup"]["ARN"]
        try:
            elasticache.add_tags_to_resource(
                ResourceName=arn,
                Tags=[{"Key": "env", "Value": "prod"}],
            )
            tags_resp = elasticache.list_tags_for_resource(ResourceName=arn)
            tag_map = {t["Key"]: t["Value"] for t in tags_resp["TagList"]}
            assert tag_map["env"] == "prod"
        finally:
            elasticache.delete_replication_group(ReplicationGroupId=rg_id)

    def test_remove_tags_from_replication_group(self, elasticache):
        rg_id = _unique("rg")
        resp = elasticache.create_replication_group(
            ReplicationGroupId=rg_id,
            ReplicationGroupDescription="tag remove test",
        )
        arn = resp["ReplicationGroup"]["ARN"]
        try:
            elasticache.add_tags_to_resource(
                ResourceName=arn,
                Tags=[
                    {"Key": "keep", "Value": "yes"},
                    {"Key": "drop", "Value": "yes"},
                ],
            )
            elasticache.remove_tags_from_resource(
                ResourceName=arn,
                TagKeys=["drop"],
            )
            tags_resp = elasticache.list_tags_for_resource(ResourceName=arn)
            tag_keys = [t["Key"] for t in tags_resp["TagList"]]
            assert "keep" in tag_keys
            assert "drop" not in tag_keys
        finally:
            elasticache.delete_replication_group(ReplicationGroupId=rg_id)


class TestElastiCacheTagsOnUser:
    """Tags on user resources."""

    def test_add_and_list_tags_on_user(self, elasticache):
        user_id = _unique("user")
        resp = elasticache.create_user(
            UserId=user_id,
            UserName="taguser",
            Engine="redis",
            AccessString="on ~* +@all",
            NoPasswordRequired=True,
        )
        arn = resp["ARN"]
        try:
            elasticache.add_tags_to_resource(
                ResourceName=arn,
                Tags=[{"Key": "role", "Value": "admin"}],
            )
            tags_resp = elasticache.list_tags_for_resource(ResourceName=arn)
            tag_map = {t["Key"]: t["Value"] for t in tags_resp["TagList"]}
            assert tag_map["role"] == "admin"
        finally:
            elasticache.delete_user(UserId=user_id)

    def test_remove_tags_from_user(self, elasticache):
        user_id = _unique("user")
        resp = elasticache.create_user(
            UserId=user_id,
            UserName="taguser2",
            Engine="redis",
            AccessString="on ~* +@all",
            NoPasswordRequired=True,
        )
        arn = resp["ARN"]
        try:
            elasticache.add_tags_to_resource(
                ResourceName=arn,
                Tags=[
                    {"Key": "keep", "Value": "yes"},
                    {"Key": "drop", "Value": "yes"},
                ],
            )
            elasticache.remove_tags_from_resource(
                ResourceName=arn,
                TagKeys=["drop"],
            )
            tags_resp = elasticache.list_tags_for_resource(ResourceName=arn)
            tag_keys = [t["Key"] for t in tags_resp["TagList"]]
            assert "keep" in tag_keys
            assert "drop" not in tag_keys
        finally:
            elasticache.delete_user(UserId=user_id)


class TestElastiCacheDescribeExtended:
    def test_describe_cache_engine_versions(self, elasticache):
        resp = elasticache.describe_cache_engine_versions()
        assert "CacheEngineVersions" in resp
        assert isinstance(resp["CacheEngineVersions"], list)

    def test_describe_cache_engine_versions_redis(self, elasticache):
        resp = elasticache.describe_cache_engine_versions(Engine="redis")
        assert "CacheEngineVersions" in resp
        for v in resp["CacheEngineVersions"]:
            assert v["Engine"] == "redis"

    def test_describe_cache_parameter_groups(self, elasticache):
        resp = elasticache.describe_cache_parameter_groups()
        assert "CacheParameterGroups" in resp
        assert isinstance(resp["CacheParameterGroups"], list)

    def test_describe_cache_parameters(self, elasticache):
        # Use the default parameter group which always exists
        resp = elasticache.describe_cache_parameters(CacheParameterGroupName="default.redis7")
        assert "Parameters" in resp
        assert isinstance(resp["Parameters"], list)

    def test_describe_events(self, elasticache):
        resp = elasticache.describe_events()
        assert "Events" in resp
        assert isinstance(resp["Events"], list)

    def test_describe_events_with_source_type(self, elasticache):
        resp = elasticache.describe_events(SourceType="cache-cluster")
        assert "Events" in resp
        assert isinstance(resp["Events"], list)

    def test_describe_service_updates(self, elasticache):
        resp = elasticache.describe_service_updates()
        assert "ServiceUpdates" in resp
        assert isinstance(resp["ServiceUpdates"], list)

    def test_describe_update_actions(self, elasticache):
        resp = elasticache.describe_update_actions()
        assert "UpdateActions" in resp
        assert isinstance(resp["UpdateActions"], list)


class TestElastiCacheUserGroupOperations:
    def test_describe_user_groups(self, elasticache):
        resp = elasticache.describe_user_groups()
        assert "UserGroups" in resp
        assert isinstance(resp["UserGroups"], list)


class TestElastiCacheServerlessCaches:
    def test_describe_serverless_caches(self, elasticache):
        resp = elasticache.describe_serverless_caches()
        assert "ServerlessCaches" in resp
        assert isinstance(resp["ServerlessCaches"], list)

    def test_create_and_delete_serverless_cache(self, elasticache):
        name = _unique("sc")
        resp = elasticache.create_serverless_cache(
            ServerlessCacheName=name,
            Engine="redis",
        )
        sc = resp["ServerlessCache"]
        assert sc["ServerlessCacheName"] == name
        assert sc["Engine"] == "redis"
        assert "Status" in sc
        assert "ARN" in sc
        assert "Endpoint" in sc

        del_resp = elasticache.delete_serverless_cache(ServerlessCacheName=name)
        assert del_resp["ServerlessCache"]["ServerlessCacheName"] == name

    def test_modify_serverless_cache(self, elasticache):
        name = _unique("sc")
        elasticache.create_serverless_cache(
            ServerlessCacheName=name,
            Engine="redis",
        )
        try:
            resp = elasticache.modify_serverless_cache(
                ServerlessCacheName=name,
                Description="updated serverless cache",
            )
            assert resp["ServerlessCache"]["ServerlessCacheName"] == name
        finally:
            elasticache.delete_serverless_cache(ServerlessCacheName=name)


class TestElastiCacheCacheParameterGroupOperations:
    """Tests for cache parameter group CRUD."""

    def test_create_and_delete_cache_parameter_group(self, elasticache):
        name = _unique("cpg")
        resp = elasticache.create_cache_parameter_group(
            CacheParameterGroupName=name,
            CacheParameterGroupFamily="redis7",
            Description="test param group",
        )
        group = resp["CacheParameterGroup"]
        assert group["CacheParameterGroupName"] == name
        assert group["CacheParameterGroupFamily"] == "redis7"
        assert group["Description"] == "test param group"

        elasticache.delete_cache_parameter_group(CacheParameterGroupName=name)

    def test_modify_cache_parameter_group(self, elasticache):
        name = _unique("cpg")
        elasticache.create_cache_parameter_group(
            CacheParameterGroupName=name,
            CacheParameterGroupFamily="redis7",
            Description="for modify test",
        )
        try:
            resp = elasticache.modify_cache_parameter_group(
                CacheParameterGroupName=name,
                ParameterNameValues=[
                    {"ParameterName": "activedefrag", "ParameterValue": "yes"},
                ],
            )
            assert resp["CacheParameterGroupName"] == name
        finally:
            elasticache.delete_cache_parameter_group(CacheParameterGroupName=name)

    def test_reset_cache_parameter_group(self, elasticache):
        name = _unique("cpg")
        elasticache.create_cache_parameter_group(
            CacheParameterGroupName=name,
            CacheParameterGroupFamily="redis7",
            Description="for reset test",
        )
        try:
            resp = elasticache.reset_cache_parameter_group(
                CacheParameterGroupName=name,
                ResetAllParameters=True,
            )
            assert resp["CacheParameterGroupName"] == name
        finally:
            elasticache.delete_cache_parameter_group(CacheParameterGroupName=name)


class TestElastiCacheCacheSecurityGroupOperations:
    """Tests for cache security group operations."""

    def test_create_and_describe_cache_security_group(self, elasticache):
        name = _unique("csg")
        resp = elasticache.create_cache_security_group(
            CacheSecurityGroupName=name,
            Description="test security group",
        )
        group = resp["CacheSecurityGroup"]
        assert group["CacheSecurityGroupName"] == name
        assert group["Description"] == "test security group"

        desc = elasticache.describe_cache_security_groups(CacheSecurityGroupName=name)
        assert len(desc["CacheSecurityGroups"]) == 1
        assert desc["CacheSecurityGroups"][0]["CacheSecurityGroupName"] == name

        elasticache.delete_cache_security_group(CacheSecurityGroupName=name)

    def test_describe_cache_security_groups_all(self, elasticache):
        resp = elasticache.describe_cache_security_groups()
        assert "CacheSecurityGroups" in resp
        assert isinstance(resp["CacheSecurityGroups"], list)

    def test_revoke_cache_security_group_ingress(self, elasticache):
        name = _unique("csg")
        elasticache.create_cache_security_group(
            CacheSecurityGroupName=name,
            Description="for revoke test",
        )
        try:
            resp = elasticache.revoke_cache_security_group_ingress(
                CacheSecurityGroupName=name,
                EC2SecurityGroupName="default",
                EC2SecurityGroupOwnerId="123456789012",
            )
            group = resp["CacheSecurityGroup"]
            assert group["CacheSecurityGroupName"] == name
            assert "EC2SecurityGroups" in group
        finally:
            elasticache.delete_cache_security_group(CacheSecurityGroupName=name)


class TestElastiCacheGlobalReplicationGroupOperations:
    """Tests for global replication group operations."""

    def test_describe_global_replication_groups(self, elasticache):
        resp = elasticache.describe_global_replication_groups()
        assert "GlobalReplicationGroups" in resp
        assert isinstance(resp["GlobalReplicationGroups"], list)

    def test_create_and_delete_global_replication_group(self, elasticache):
        rg_id = _unique("rg")
        elasticache.create_replication_group(
            ReplicationGroupId=rg_id,
            ReplicationGroupDescription="for global rg test",
        )
        grg_suffix = _unique("grg")
        try:
            resp = elasticache.create_global_replication_group(
                GlobalReplicationGroupIdSuffix=grg_suffix,
                PrimaryReplicationGroupId=rg_id,
            )
            grg = resp["GlobalReplicationGroup"]
            assert grg_suffix in grg["GlobalReplicationGroupId"]
            assert grg["Status"] == "available"
            assert grg["Engine"] == "redis"
            assert len(grg["Members"]) >= 1
            assert grg["Members"][0]["ReplicationGroupId"] == rg_id

            grg_id = grg["GlobalReplicationGroupId"]

            del_resp = elasticache.delete_global_replication_group(
                GlobalReplicationGroupId=grg_id,
                RetainPrimaryReplicationGroup=True,
            )
            assert del_resp["GlobalReplicationGroup"]["GlobalReplicationGroupId"] == grg_id
        finally:
            elasticache.delete_replication_group(ReplicationGroupId=rg_id)

    def test_modify_global_replication_group(self, elasticache):
        rg_id = _unique("rg")
        elasticache.create_replication_group(
            ReplicationGroupId=rg_id,
            ReplicationGroupDescription="for global modify",
        )
        grg_suffix = _unique("grg")
        try:
            resp = elasticache.create_global_replication_group(
                GlobalReplicationGroupIdSuffix=grg_suffix,
                PrimaryReplicationGroupId=rg_id,
            )
            grg_id = resp["GlobalReplicationGroup"]["GlobalReplicationGroupId"]

            mod_resp = elasticache.modify_global_replication_group(
                GlobalReplicationGroupId=grg_id,
                ApplyImmediately=True,
                GlobalReplicationGroupDescription="updated description",
            )
            assert mod_resp["GlobalReplicationGroup"]["GlobalReplicationGroupId"] == grg_id
            assert (
                mod_resp["GlobalReplicationGroup"]["GlobalReplicationGroupDescription"]
                == "updated description"
            )

            elasticache.delete_global_replication_group(
                GlobalReplicationGroupId=grg_id,
                RetainPrimaryReplicationGroup=True,
            )
        finally:
            elasticache.delete_replication_group(ReplicationGroupId=rg_id)


class TestElastiCacheModifyOperations:
    """Tests for modify operations on existing resources."""

    def test_modify_cache_cluster(self, elasticache):
        cc_id = _unique("cc")
        elasticache.create_cache_cluster(
            CacheClusterId=cc_id,
            NumCacheNodes=1,
            CacheNodeType="cache.t2.micro",
            Engine="redis",
        )
        try:
            resp = elasticache.modify_cache_cluster(
                CacheClusterId=cc_id,
                SnapshotRetentionLimit=5,
            )
            assert resp["CacheCluster"]["CacheClusterId"] == cc_id
        finally:
            elasticache.delete_cache_cluster(CacheClusterId=cc_id)

    def test_modify_cache_subnet_group(self, elasticache):
        name = _unique("sg")
        elasticache.create_cache_subnet_group(
            CacheSubnetGroupName=name,
            CacheSubnetGroupDescription="for modify",
            SubnetIds=["subnet-12345678"],
        )
        resp = elasticache.modify_cache_subnet_group(
            CacheSubnetGroupName=name,
            CacheSubnetGroupDescription="updated description",
        )
        group = resp["CacheSubnetGroup"]
        assert group["CacheSubnetGroupName"] == name
        assert group["CacheSubnetGroupDescription"] == "updated description"

    def test_modify_replication_group(self, elasticache):
        rg_id = _unique("rg")
        elasticache.create_replication_group(
            ReplicationGroupId=rg_id,
            ReplicationGroupDescription="for modify",
        )
        try:
            resp = elasticache.modify_replication_group(
                ReplicationGroupId=rg_id,
                ReplicationGroupDescription="updated desc",
            )
            assert resp["ReplicationGroup"]["ReplicationGroupId"] == rg_id
        finally:
            elasticache.delete_replication_group(ReplicationGroupId=rg_id)

    def test_modify_user(self, elasticache):
        user_id = _unique("user")
        elasticache.create_user(
            UserId=user_id,
            UserName="moduser",
            Engine="redis",
            AccessString="on ~* +@all",
            NoPasswordRequired=True,
        )
        try:
            resp = elasticache.modify_user(
                UserId=user_id,
                AccessString="on ~* +@read",
            )
            assert resp["UserId"] == user_id
        finally:
            elasticache.delete_user(UserId=user_id)


class TestElastiCacheUserGroupCRUD:
    """Tests for user group create/delete operations."""

    def test_create_and_delete_user_group(self, elasticache):
        ug_id = _unique("ug")
        resp = elasticache.create_user_group(
            UserGroupId=ug_id,
            Engine="redis",
            UserIds=["default"],
        )
        assert resp["UserGroupId"] == ug_id
        assert resp["Engine"] == "redis"
        assert "ARN" in resp
        assert "Status" in resp

        del_resp = elasticache.delete_user_group(UserGroupId=ug_id)
        assert del_resp["UserGroupId"] == ug_id

    def test_modify_user_group(self, elasticache):
        ug_id = _unique("ug")
        user_id = _unique("user")
        elasticache.create_user(
            UserId=user_id,
            UserName="uguser",
            Engine="redis",
            AccessString="on ~* +@all",
            NoPasswordRequired=True,
        )
        elasticache.create_user_group(
            UserGroupId=ug_id,
            Engine="redis",
            UserIds=["default"],
        )
        try:
            resp = elasticache.modify_user_group(
                UserGroupId=ug_id,
                UserIdsToAdd=[user_id],
            )
            assert resp["UserGroupId"] == ug_id
        finally:
            elasticache.delete_user_group(UserGroupId=ug_id)
            elasticache.delete_user(UserId=user_id)


class TestElastiCacheServerlessCacheSnapshots:
    """Tests for serverless cache snapshot operations."""

    def test_describe_serverless_cache_snapshots(self, elasticache):
        resp = elasticache.describe_serverless_cache_snapshots()
        assert "ServerlessCacheSnapshots" in resp
        assert isinstance(resp["ServerlessCacheSnapshots"], list)

    def test_create_and_delete_serverless_cache_snapshot(self, elasticache):
        cache_name = _unique("sc")
        elasticache.create_serverless_cache(
            ServerlessCacheName=cache_name,
            Engine="redis",
        )
        snap_name = _unique("scs")
        try:
            resp = elasticache.create_serverless_cache_snapshot(
                ServerlessCacheSnapshotName=snap_name,
                ServerlessCacheName=cache_name,
            )
            snap = resp["ServerlessCacheSnapshot"]
            assert snap["ServerlessCacheSnapshotName"] == snap_name
            assert "ARN" in snap
            assert "Status" in snap

            del_resp = elasticache.delete_serverless_cache_snapshot(
                ServerlessCacheSnapshotName=snap_name,
            )
            assert del_resp["ServerlessCacheSnapshot"]["ServerlessCacheSnapshotName"] == snap_name
        finally:
            elasticache.delete_serverless_cache(ServerlessCacheName=cache_name)


class TestElastiCacheCopySnapshot:
    """Tests for copy snapshot operation."""

    def test_copy_snapshot(self, elasticache):
        rg_id = _unique("rg")
        elasticache.create_replication_group(
            ReplicationGroupId=rg_id,
            ReplicationGroupDescription="for copy snap",
        )
        snap_name = _unique("snap")
        target_name = _unique("snap-copy")
        elasticache.create_snapshot(
            SnapshotName=snap_name,
            ReplicationGroupId=rg_id,
        )
        try:
            resp = elasticache.copy_snapshot(
                SourceSnapshotName=snap_name,
                TargetSnapshotName=target_name,
            )
            assert resp["Snapshot"]["SnapshotName"] == target_name
        finally:
            try:
                elasticache.delete_snapshot(SnapshotName=target_name)
            except ClientError:
                pass
            elasticache.delete_snapshot(SnapshotName=snap_name)
            elasticache.delete_replication_group(ReplicationGroupId=rg_id)
