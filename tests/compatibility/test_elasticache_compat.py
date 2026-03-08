"""ElastiCache compatibility tests."""

import uuid

import pytest

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
        from botocore.exceptions import ClientError

        with pytest.raises(ClientError) as exc:
            client.delete_replication_group(ReplicationGroupId="does-not-exist")
        assert exc.value.response["Error"]["Code"] in (
            "ReplicationGroupNotFoundFault",
            "ReplicationGroupNotFound",
            "ReplicationGroupNotFoundFault",
        )


class TestElasticacheAutoCoverage:
    """Auto-generated coverage tests for elasticache."""

    @pytest.fixture
    def client(self):
        return make_client("elasticache")

    def test_describe_snapshots(self, client):
        """DescribeSnapshots returns a response."""
        resp = client.describe_snapshots()
        assert "Snapshots" in resp

    def test_describe_users(self, client):
        """DescribeUsers returns a response."""
        resp = client.describe_users()
        assert "Users" in resp
