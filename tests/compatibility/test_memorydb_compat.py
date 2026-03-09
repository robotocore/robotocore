"""MemoryDB compatibility tests."""

import uuid

import pytest
from botocore.exceptions import ClientError

from tests.compatibility.conftest import make_client


def _uid():
    return uuid.uuid4().hex[:8]


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


class TestMemoryDBACLs:
    """Tests for MemoryDB ACL operations."""

    def test_describe_acls(self, memorydb):
        """DescribeACLs returns the default open-access ACL."""
        resp = memorydb.describe_acls()
        assert "ACLs" in resp
        names = [a["Name"] for a in resp["ACLs"]]
        assert "open-access" in names

    def test_create_and_delete_acl(self, memorydb):
        """CreateACL creates an ACL, DeleteACL removes it."""
        name = f"test-acl-{_uid()}"
        resp = memorydb.create_acl(ACLName=name)
        assert resp["ACL"]["Name"] == name
        assert resp["ACL"]["Status"] == "active"

        del_resp = memorydb.delete_acl(ACLName=name)
        assert del_resp["ACL"]["Name"] == name

    def test_delete_acl_nonexistent(self, memorydb):
        """DeleteACL for nonexistent ACL raises error."""
        with pytest.raises(ClientError) as exc:
            memorydb.delete_acl(ACLName="nonexistent-acl")
        assert exc.value.response["Error"]["Code"] == "ACLNotFoundFault"


class TestMemoryDBUsers:
    """Tests for MemoryDB User operations."""

    def test_describe_users(self, memorydb):
        """DescribeUsers returns at least the default user."""
        resp = memorydb.describe_users()
        assert "Users" in resp
        names = [u["Name"] for u in resp["Users"]]
        assert "default" in names

    def test_create_and_delete_user(self, memorydb):
        """CreateUser creates a user, DeleteUser removes it."""
        name = f"test-user-{_uid()}"
        resp = memorydb.create_user(
            UserName=name,
            AccessString="on ~* +@all",
            AuthenticationMode={"Type": "no-password"},
        )
        assert resp["User"]["Name"] == name
        assert resp["User"]["Status"] == "active"

        del_resp = memorydb.delete_user(UserName=name)
        assert del_resp["User"]["Name"] == name

    def test_delete_user_nonexistent(self, memorydb):
        """DeleteUser for nonexistent user raises error."""
        with pytest.raises(ClientError) as exc:
            memorydb.delete_user(UserName="nonexistent-user")
        assert exc.value.response["Error"]["Code"] == "UserNotFoundFault"


class TestMemoryDBParameterGroups:
    """Tests for MemoryDB ParameterGroup operations."""

    def test_describe_parameter_groups(self, memorydb):
        """DescribeParameterGroups returns at least default group."""
        resp = memorydb.describe_parameter_groups()
        assert "ParameterGroups" in resp
        assert isinstance(resp["ParameterGroups"], list)

    def test_create_and_delete_parameter_group(self, memorydb):
        """CreateParameterGroup + DeleteParameterGroup roundtrip."""
        name = f"test-pg-{_uid()}"
        resp = memorydb.create_parameter_group(
            ParameterGroupName=name,
            Family="memorydb_redis7",
            Description="test parameter group",
        )
        assert resp["ParameterGroup"]["Name"] == name

        del_resp = memorydb.delete_parameter_group(ParameterGroupName=name)
        assert del_resp["ParameterGroup"]["Name"] == name

    def test_delete_parameter_group_nonexistent(self, memorydb):
        """DeleteParameterGroup for nonexistent raises error."""
        with pytest.raises(ClientError) as exc:
            memorydb.delete_parameter_group(ParameterGroupName="nonexistent-pg")
        assert exc.value.response["Error"]["Code"] == "ParameterGroupNotFoundFault"


class TestMemoryDBListOperations:
    """Tests for MemoryDB list/describe operations."""

    def test_describe_service_updates(self, memorydb):
        """DescribeServiceUpdates returns a list."""
        resp = memorydb.describe_service_updates()
        assert "ServiceUpdates" in resp
        assert isinstance(resp["ServiceUpdates"], list)

    def test_describe_events(self, memorydb):
        """DescribeEvents returns a list."""
        resp = memorydb.describe_events()
        assert "Events" in resp
        assert isinstance(resp["Events"], list)

    def test_describe_engine_versions(self, memorydb):
        """DescribeEngineVersions returns version list."""
        resp = memorydb.describe_engine_versions()
        assert "EngineVersions" in resp
        assert isinstance(resp["EngineVersions"], list)

    def test_describe_reserved_nodes(self, memorydb):
        """DescribeReservedNodes returns a list."""
        resp = memorydb.describe_reserved_nodes()
        assert "ReservedNodes" in resp
        assert isinstance(resp["ReservedNodes"], list)

    def test_describe_reserved_nodes_offerings(self, memorydb):
        """DescribeReservedNodesOfferings returns a list."""
        resp = memorydb.describe_reserved_nodes_offerings()
        assert "ReservedNodesOfferings" in resp
        assert isinstance(resp["ReservedNodesOfferings"], list)


class TestMemoryDBACLOperations:
    """Tests for MemoryDB ACL create, update, and delete operations."""

    def test_create_acl_no_users(self, memorydb):
        """CreateACL without UserNames creates an ACL."""
        name = f"acl-op-{_uid()}"
        resp = memorydb.create_acl(ACLName=name)
        assert resp["ACL"]["Name"] == name
        assert resp["ACL"]["Status"] == "active"
        memorydb.delete_acl(ACLName=name)

    def test_create_and_delete_acl_roundtrip(self, memorydb):
        """CreateACL then DeleteACL removes the ACL."""
        name = f"acl-del-{_uid()}"
        memorydb.create_acl(ACLName=name)
        del_resp = memorydb.delete_acl(ACLName=name)
        assert del_resp["ACL"]["Name"] == name
        # Verify it's gone
        resp = memorydb.describe_acls()
        names = [a["Name"] for a in resp["ACLs"]]
        assert name not in names

    def test_update_acl_add_user(self, memorydb):
        """UpdateACL adds a user to an existing ACL."""
        acl_name = f"acl-upd-{_uid()}"
        user_name = f"user-upd-{_uid()}"
        # Create user and ACL
        memorydb.create_user(
            UserName=user_name,
            AccessString="on ~* +@all",
            AuthenticationMode={"Type": "no-password"},
        )
        memorydb.create_acl(ACLName=acl_name)
        try:
            resp = memorydb.update_acl(ACLName=acl_name, UserNamesToAdd=[user_name])
            assert resp["ACL"]["Name"] == acl_name
        finally:
            memorydb.delete_acl(ACLName=acl_name)
            memorydb.delete_user(UserName=user_name)


class TestMemoryDBUpdates:
    """Tests for MemoryDB update operations on parameter groups and users."""

    def test_update_parameter_group(self, memorydb):
        """UpdateParameterGroup updates parameters on a group."""
        pg_name = f"pg-upd-{_uid()}"
        memorydb.create_parameter_group(
            ParameterGroupName=pg_name,
            Family="memorydb_redis7",
            Description="test pg for update",
        )
        try:
            resp = memorydb.update_parameter_group(
                ParameterGroupName=pg_name,
                ParameterNameValues=[
                    {"ParameterName": "activedefrag", "ParameterValue": "yes"},
                ],
            )
            assert resp["ParameterGroup"]["Name"] == pg_name
        finally:
            memorydb.delete_parameter_group(ParameterGroupName=pg_name)

    def test_update_user(self, memorydb):
        """UpdateUser modifies user access string."""
        user_name = f"user-upd2-{_uid()}"
        memorydb.create_user(
            UserName=user_name,
            AccessString="on ~* +@all",
            AuthenticationMode={"Type": "no-password"},
        )
        try:
            resp = memorydb.update_user(
                UserName=user_name,
                AccessString="on ~app* +@read",
            )
            assert resp["User"]["Name"] == user_name
        finally:
            memorydb.delete_user(UserName=user_name)
