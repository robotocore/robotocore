"""Neptune compatibility tests."""

import uuid

import pytest
from botocore.exceptions import ClientError

from tests.compatibility.conftest import make_client


@pytest.fixture
def neptune():
    return make_client("neptune")


@pytest.fixture
def ec2():
    return make_client("ec2")


def _unique(prefix):
    return f"{prefix}-{uuid.uuid4().hex[:8]}"


@pytest.fixture
def subnet_ids(ec2):
    """Return two subnet IDs in different AZs, creating VPC/subnets if needed."""
    vpcs = ec2.describe_vpcs()["Vpcs"]
    if vpcs:
        vpc_id = vpcs[0]["VpcId"]
    else:
        vpc_id = ec2.create_vpc(CidrBlock="10.0.0.0/16")["Vpc"]["VpcId"]

    subnets = ec2.describe_subnets(Filters=[{"Name": "vpc-id", "Values": [vpc_id]}])["Subnets"]
    if len(subnets) >= 2:
        return [s["SubnetId"] for s in subnets[:2]]

    s1 = ec2.create_subnet(VpcId=vpc_id, CidrBlock="10.0.1.0/24", AvailabilityZone="us-east-1a")[
        "Subnet"
    ]["SubnetId"]
    s2 = ec2.create_subnet(VpcId=vpc_id, CidrBlock="10.0.2.0/24", AvailabilityZone="us-east-1b")[
        "Subnet"
    ]["SubnetId"]
    return [s1, s2]


class TestNeptuneSubnetGroupOperations:
    def test_create_db_subnet_group(self, neptune, subnet_ids):
        name = _unique("nep-sg")
        resp = neptune.create_db_subnet_group(
            DBSubnetGroupName=name,
            DBSubnetGroupDescription="test subnet group",
            SubnetIds=subnet_ids,
        )
        assert resp["DBSubnetGroup"]["DBSubnetGroupName"] == name
        assert resp["DBSubnetGroup"]["DBSubnetGroupDescription"] == "test subnet group"
        neptune.delete_db_subnet_group(DBSubnetGroupName=name)

    def test_describe_db_subnet_groups(self, neptune, subnet_ids):
        name = _unique("nep-sg")
        neptune.create_db_subnet_group(
            DBSubnetGroupName=name,
            DBSubnetGroupDescription="desc",
            SubnetIds=subnet_ids,
        )
        try:
            resp = neptune.describe_db_subnet_groups(DBSubnetGroupName=name)
            groups = resp["DBSubnetGroups"]
            assert len(groups) == 1
            assert groups[0]["DBSubnetGroupName"] == name
        finally:
            neptune.delete_db_subnet_group(DBSubnetGroupName=name)

    def test_describe_db_subnet_groups_all(self, neptune, subnet_ids):
        name = _unique("nep-sg")
        neptune.create_db_subnet_group(
            DBSubnetGroupName=name,
            DBSubnetGroupDescription="desc",
            SubnetIds=subnet_ids,
        )
        try:
            resp = neptune.describe_db_subnet_groups()
            names = [g["DBSubnetGroupName"] for g in resp["DBSubnetGroups"]]
            assert name in names
        finally:
            neptune.delete_db_subnet_group(DBSubnetGroupName=name)

    def test_delete_db_subnet_group(self, neptune, subnet_ids):
        name = _unique("nep-sg")
        neptune.create_db_subnet_group(
            DBSubnetGroupName=name,
            DBSubnetGroupDescription="to delete",
            SubnetIds=subnet_ids,
        )
        neptune.delete_db_subnet_group(DBSubnetGroupName=name)
        # Verify it's gone
        resp = neptune.describe_db_subnet_groups()
        names = [g["DBSubnetGroupName"] for g in resp["DBSubnetGroups"]]
        assert name not in names


class TestNeptuneDescribeOperations:
    def test_describe_db_clusters_empty(self, neptune):
        resp = neptune.describe_db_clusters()
        assert "DBClusters" in resp
        assert isinstance(resp["DBClusters"], list)

    def test_describe_db_instances_empty(self, neptune):
        resp = neptune.describe_db_instances()
        assert "DBInstances" in resp
        assert isinstance(resp["DBInstances"], list)

    def test_describe_db_parameter_groups(self, neptune):
        resp = neptune.describe_db_parameter_groups()
        assert "DBParameterGroups" in resp
        assert isinstance(resp["DBParameterGroups"], list)

    def test_describe_db_cluster_parameter_groups(self, neptune):
        resp = neptune.describe_db_cluster_parameter_groups()
        assert "DBClusterParameterGroups" in resp
        assert isinstance(resp["DBClusterParameterGroups"], list)

    def test_describe_event_subscriptions(self, neptune):
        resp = neptune.describe_event_subscriptions()
        assert "EventSubscriptionsList" in resp
        assert isinstance(resp["EventSubscriptionsList"], list)

    def test_describe_events(self, neptune):
        resp = neptune.describe_events()
        assert "Events" in resp
        assert isinstance(resp["Events"], list)

    def test_describe_global_clusters(self, neptune):
        resp = neptune.describe_global_clusters()
        assert "GlobalClusters" in resp
        assert isinstance(resp["GlobalClusters"], list)


class TestNeptuneDBClusterParameterGroupOperations:
    def test_create_db_cluster_parameter_group(self, neptune):
        name = _unique("nep-cpg")
        resp = neptune.create_db_cluster_parameter_group(
            DBClusterParameterGroupName=name,
            DBParameterGroupFamily="neptune1",
            Description="test cluster param group",
        )
        assert resp["DBClusterParameterGroup"]["DBClusterParameterGroupName"] == name
        assert resp["DBClusterParameterGroup"]["Description"] == "test cluster param group"
        neptune.delete_db_cluster_parameter_group(DBClusterParameterGroupName=name)

    def test_delete_db_cluster_parameter_group(self, neptune):
        name = _unique("nep-cpg")
        neptune.create_db_cluster_parameter_group(
            DBClusterParameterGroupName=name,
            DBParameterGroupFamily="neptune1",
            Description="to delete",
        )
        neptune.delete_db_cluster_parameter_group(DBClusterParameterGroupName=name)
        resp = neptune.describe_db_cluster_parameter_groups()
        names = [g["DBClusterParameterGroupName"] for g in resp["DBClusterParameterGroups"]]
        assert name not in names

    def test_modify_db_cluster_parameter_group(self, neptune):
        name = _unique("nep-cpg")
        neptune.create_db_cluster_parameter_group(
            DBClusterParameterGroupName=name,
            DBParameterGroupFamily="neptune1",
            Description="to modify",
        )
        try:
            resp = neptune.modify_db_cluster_parameter_group(
                DBClusterParameterGroupName=name,
                Parameters=[
                    {
                        "ParameterName": "neptune_query_timeout",
                        "ParameterValue": "240000",
                        "ApplyMethod": "pending-reboot",
                    }
                ],
            )
            assert resp["DBClusterParameterGroupName"] == name
        finally:
            neptune.delete_db_cluster_parameter_group(DBClusterParameterGroupName=name)

    def test_copy_db_cluster_parameter_group(self, neptune):
        src = _unique("nep-cpg-src")
        tgt = _unique("nep-cpg-tgt")
        neptune.create_db_cluster_parameter_group(
            DBClusterParameterGroupName=src,
            DBParameterGroupFamily="neptune1",
            Description="source group",
        )
        try:
            resp = neptune.copy_db_cluster_parameter_group(
                SourceDBClusterParameterGroupIdentifier=src,
                TargetDBClusterParameterGroupIdentifier=tgt,
                TargetDBClusterParameterGroupDescription="copied group",
            )
            assert resp["DBClusterParameterGroup"]["DBClusterParameterGroupName"] == tgt
            assert resp["DBClusterParameterGroup"]["Description"] == "copied group"
        finally:
            neptune.delete_db_cluster_parameter_group(DBClusterParameterGroupName=src)
            try:
                neptune.delete_db_cluster_parameter_group(DBClusterParameterGroupName=tgt)
            except Exception:
                pass  # best-effort cleanup


class TestNeptuneTags:
    def test_add_and_list_tags(self, neptune, subnet_ids):
        name = _unique("nep-tag")
        create_resp = neptune.create_db_subnet_group(
            DBSubnetGroupName=name,
            DBSubnetGroupDescription="for tagging",
            SubnetIds=subnet_ids,
        )
        arn = create_resp["DBSubnetGroup"]["DBSubnetGroupArn"]
        try:
            neptune.add_tags_to_resource(
                ResourceName=arn,
                Tags=[{"Key": "env", "Value": "test"}, {"Key": "team", "Value": "dev"}],
            )
            resp = neptune.list_tags_for_resource(ResourceName=arn)
            tag_map = {t["Key"]: t["Value"] for t in resp["TagList"]}
            assert tag_map["env"] == "test"
            assert tag_map["team"] == "dev"
        finally:
            neptune.delete_db_subnet_group(DBSubnetGroupName=name)

    def test_remove_tags(self, neptune, subnet_ids):
        name = _unique("nep-tag")
        create_resp = neptune.create_db_subnet_group(
            DBSubnetGroupName=name,
            DBSubnetGroupDescription="for tagging",
            SubnetIds=subnet_ids,
        )
        arn = create_resp["DBSubnetGroup"]["DBSubnetGroupArn"]
        try:
            neptune.add_tags_to_resource(
                ResourceName=arn,
                Tags=[{"Key": "env", "Value": "test"}, {"Key": "temp", "Value": "yes"}],
            )
            neptune.remove_tags_from_resource(ResourceName=arn, TagKeys=["temp"])
            resp = neptune.list_tags_for_resource(ResourceName=arn)
            keys = [t["Key"] for t in resp["TagList"]]
            assert "env" in keys
            assert "temp" not in keys
        finally:
            neptune.delete_db_subnet_group(DBSubnetGroupName=name)

    def test_list_tags_empty(self, neptune, subnet_ids):
        name = _unique("nep-tag")
        create_resp = neptune.create_db_subnet_group(
            DBSubnetGroupName=name,
            DBSubnetGroupDescription="no tags",
            SubnetIds=subnet_ids,
        )
        arn = create_resp["DBSubnetGroup"]["DBSubnetGroupArn"]
        try:
            resp = neptune.list_tags_for_resource(ResourceName=arn)
            assert resp["TagList"] == []
        finally:
            neptune.delete_db_subnet_group(DBSubnetGroupName=name)


class TestNeptuneDBClusterOperations:
    """Tests for CreateDBCluster, DescribeDBClusters, StopDBCluster, StartDBCluster."""

    @pytest.fixture(autouse=True)
    def _setup(self, neptune, subnet_ids):
        self.neptune = neptune
        self.subnet_ids = subnet_ids
        self.sg_name = _unique("cl-sg")
        neptune.create_db_subnet_group(
            DBSubnetGroupName=self.sg_name,
            DBSubnetGroupDescription="cluster tests",
            SubnetIds=subnet_ids,
        )
        yield
        neptune.delete_db_subnet_group(DBSubnetGroupName=self.sg_name)

    def test_create_db_cluster(self, neptune):
        cluster_id = _unique("nep-cl")
        resp = neptune.create_db_cluster(
            DBClusterIdentifier=cluster_id,
            Engine="neptune",
            DBSubnetGroupName=self.sg_name,
        )
        try:
            assert resp["DBCluster"]["DBClusterIdentifier"] == cluster_id
            assert resp["DBCluster"]["Engine"] == "neptune"
            assert "Status" in resp["DBCluster"]
        finally:
            neptune.delete_db_cluster(DBClusterIdentifier=cluster_id, SkipFinalSnapshot=True)

    def test_describe_db_clusters_filtered(self, neptune):
        cluster_id = _unique("nep-cl")
        neptune.create_db_cluster(
            DBClusterIdentifier=cluster_id,
            Engine="neptune",
            DBSubnetGroupName=self.sg_name,
        )
        try:
            resp = neptune.describe_db_clusters(DBClusterIdentifier=cluster_id)
            assert len(resp["DBClusters"]) == 1
            assert resp["DBClusters"][0]["DBClusterIdentifier"] == cluster_id
        finally:
            neptune.delete_db_cluster(DBClusterIdentifier=cluster_id, SkipFinalSnapshot=True)

    def test_stop_and_start_db_cluster(self, neptune):
        cluster_id = _unique("nep-cl")
        neptune.create_db_cluster(
            DBClusterIdentifier=cluster_id,
            Engine="neptune",
            DBSubnetGroupName=self.sg_name,
        )
        try:
            resp = neptune.stop_db_cluster(DBClusterIdentifier=cluster_id)
            assert resp["DBCluster"]["DBClusterIdentifier"] == cluster_id

            resp = neptune.start_db_cluster(DBClusterIdentifier=cluster_id)
            assert resp["DBCluster"]["DBClusterIdentifier"] == cluster_id
        finally:
            neptune.delete_db_cluster(DBClusterIdentifier=cluster_id, SkipFinalSnapshot=True)

    def test_add_role_to_db_cluster(self, neptune):
        cluster_id = _unique("nep-cl")
        neptune.create_db_cluster(
            DBClusterIdentifier=cluster_id,
            Engine="neptune",
            DBSubnetGroupName=self.sg_name,
        )
        try:
            neptune.add_role_to_db_cluster(
                DBClusterIdentifier=cluster_id,
                RoleArn="arn:aws:iam::123456789012:role/neptune-role",
            )
            resp = neptune.describe_db_clusters(DBClusterIdentifier=cluster_id)
            roles = resp["DBClusters"][0].get("AssociatedRoles", [])
            assert any(r["RoleArn"] == "arn:aws:iam::123456789012:role/neptune-role" for r in roles)
        finally:
            neptune.delete_db_cluster(DBClusterIdentifier=cluster_id, SkipFinalSnapshot=True)


class TestNeptuneDBClusterModify:
    """Tests for ModifyDBCluster."""

    @pytest.fixture(autouse=True)
    def _setup(self, neptune, subnet_ids):
        self.neptune = neptune
        self.sg_name = _unique("mod-sg")
        self.cluster_id = _unique("mod-cl")
        neptune.create_db_subnet_group(
            DBSubnetGroupName=self.sg_name,
            DBSubnetGroupDescription="modify tests",
            SubnetIds=subnet_ids,
        )
        neptune.create_db_cluster(
            DBClusterIdentifier=self.cluster_id,
            Engine="neptune",
            DBSubnetGroupName=self.sg_name,
        )
        yield
        neptune.delete_db_cluster(DBClusterIdentifier=self.cluster_id, SkipFinalSnapshot=True)
        neptune.delete_db_subnet_group(DBSubnetGroupName=self.sg_name)

    def test_modify_db_cluster_maintenance_window(self, neptune):
        resp = neptune.modify_db_cluster(
            DBClusterIdentifier=self.cluster_id,
            PreferredMaintenanceWindow="mon:03:00-mon:04:00",
            ApplyImmediately=True,
        )
        assert resp["DBCluster"]["DBClusterIdentifier"] == self.cluster_id
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200

    def test_modify_db_cluster_backup_retention(self, neptune):
        resp = neptune.modify_db_cluster(
            DBClusterIdentifier=self.cluster_id,
            BackupRetentionPeriod=7,
            ApplyImmediately=True,
        )
        assert resp["DBCluster"]["DBClusterIdentifier"] == self.cluster_id

    def test_modify_nonexistent_db_cluster(self, neptune):
        with pytest.raises(ClientError) as exc_info:
            neptune.modify_db_cluster(
                DBClusterIdentifier="nonexistent-cluster",
                PreferredMaintenanceWindow="mon:03:00-mon:04:00",
            )
        assert "NotFound" in exc_info.value.response["Error"]["Code"] or "DBCluster" in str(
            exc_info.value
        )


class TestNeptuneDBInstanceOperations:
    """Tests for CreateDBInstance, ModifyDBInstance, RebootDBInstance, DeleteDBInstance."""

    @pytest.fixture(autouse=True)
    def _setup(self, neptune, subnet_ids):
        self.neptune = neptune
        self.sg_name = _unique("inst-sg")
        self.cluster_id = _unique("inst-cl")
        neptune.create_db_subnet_group(
            DBSubnetGroupName=self.sg_name,
            DBSubnetGroupDescription="instance tests",
            SubnetIds=subnet_ids,
        )
        neptune.create_db_cluster(
            DBClusterIdentifier=self.cluster_id,
            Engine="neptune",
            DBSubnetGroupName=self.sg_name,
        )
        yield
        neptune.delete_db_cluster(DBClusterIdentifier=self.cluster_id, SkipFinalSnapshot=True)
        neptune.delete_db_subnet_group(DBSubnetGroupName=self.sg_name)

    def test_create_db_instance(self, neptune):
        inst_id = _unique("nep-inst")
        resp = neptune.create_db_instance(
            DBInstanceIdentifier=inst_id,
            DBInstanceClass="db.r5.large",
            Engine="neptune",
            DBClusterIdentifier=self.cluster_id,
        )
        try:
            assert resp["DBInstance"]["DBInstanceIdentifier"] == inst_id
            assert resp["DBInstance"]["Engine"] == "neptune"
            assert "DBInstanceStatus" in resp["DBInstance"]
        finally:
            neptune.delete_db_instance(DBInstanceIdentifier=inst_id, SkipFinalSnapshot=True)

    def test_modify_db_instance(self, neptune):
        inst_id = _unique("nep-inst")
        neptune.create_db_instance(
            DBInstanceIdentifier=inst_id,
            DBInstanceClass="db.r5.large",
            Engine="neptune",
            DBClusterIdentifier=self.cluster_id,
        )
        try:
            resp = neptune.modify_db_instance(
                DBInstanceIdentifier=inst_id,
                DBInstanceClass="db.r5.xlarge",
                ApplyImmediately=True,
            )
            assert resp["DBInstance"]["DBInstanceIdentifier"] == inst_id
        finally:
            neptune.delete_db_instance(DBInstanceIdentifier=inst_id, SkipFinalSnapshot=True)

    def test_reboot_db_instance(self, neptune):
        inst_id = _unique("nep-inst")
        neptune.create_db_instance(
            DBInstanceIdentifier=inst_id,
            DBInstanceClass="db.r5.large",
            Engine="neptune",
            DBClusterIdentifier=self.cluster_id,
        )
        try:
            resp = neptune.reboot_db_instance(DBInstanceIdentifier=inst_id)
            assert resp["DBInstance"]["DBInstanceIdentifier"] == inst_id
        finally:
            neptune.delete_db_instance(DBInstanceIdentifier=inst_id, SkipFinalSnapshot=True)

    def test_delete_db_instance(self, neptune):
        inst_id = _unique("nep-inst")
        neptune.create_db_instance(
            DBInstanceIdentifier=inst_id,
            DBInstanceClass="db.r5.large",
            Engine="neptune",
            DBClusterIdentifier=self.cluster_id,
        )
        neptune.delete_db_instance(DBInstanceIdentifier=inst_id, SkipFinalSnapshot=True)
        resp = neptune.describe_db_instances()
        ids = [i["DBInstanceIdentifier"] for i in resp["DBInstances"]]
        assert inst_id not in ids


class TestNeptuneDBClusterSnapshotOperations:
    """Tests for CreateDBClusterSnapshot, DescribeDBClusterSnapshots,
    CopyDBClusterSnapshot, DescribeDBClusterSnapshotAttributes,
    ModifyDBClusterSnapshotAttribute, DeleteDBClusterSnapshot."""

    @pytest.fixture(autouse=True)
    def _setup(self, neptune, subnet_ids):
        self.neptune = neptune
        self.sg_name = _unique("snap-sg")
        self.cluster_id = _unique("snap-cl")
        neptune.create_db_subnet_group(
            DBSubnetGroupName=self.sg_name,
            DBSubnetGroupDescription="snapshot tests",
            SubnetIds=subnet_ids,
        )
        neptune.create_db_cluster(
            DBClusterIdentifier=self.cluster_id,
            Engine="neptune",
            DBSubnetGroupName=self.sg_name,
        )
        yield
        neptune.delete_db_cluster(DBClusterIdentifier=self.cluster_id, SkipFinalSnapshot=True)
        neptune.delete_db_subnet_group(DBSubnetGroupName=self.sg_name)

    def test_create_db_cluster_snapshot(self, neptune):
        snap_id = _unique("nep-snap")
        resp = neptune.create_db_cluster_snapshot(
            DBClusterSnapshotIdentifier=snap_id,
            DBClusterIdentifier=self.cluster_id,
        )
        try:
            assert resp["DBClusterSnapshot"]["DBClusterSnapshotIdentifier"] == snap_id
            assert resp["DBClusterSnapshot"]["DBClusterIdentifier"] == self.cluster_id
            assert "Status" in resp["DBClusterSnapshot"]
        finally:
            neptune.delete_db_cluster_snapshot(DBClusterSnapshotIdentifier=snap_id)

    def test_describe_db_cluster_snapshots(self, neptune):
        snap_id = _unique("nep-snap")
        neptune.create_db_cluster_snapshot(
            DBClusterSnapshotIdentifier=snap_id,
            DBClusterIdentifier=self.cluster_id,
        )
        try:
            resp = neptune.describe_db_cluster_snapshots(DBClusterIdentifier=self.cluster_id)
            ids = [s["DBClusterSnapshotIdentifier"] for s in resp["DBClusterSnapshots"]]
            assert snap_id in ids
        finally:
            neptune.delete_db_cluster_snapshot(DBClusterSnapshotIdentifier=snap_id)

    def test_copy_db_cluster_snapshot(self, neptune):
        src = _unique("nep-snap-src")
        tgt = _unique("nep-snap-tgt")
        neptune.create_db_cluster_snapshot(
            DBClusterSnapshotIdentifier=src,
            DBClusterIdentifier=self.cluster_id,
        )
        try:
            resp = neptune.copy_db_cluster_snapshot(
                SourceDBClusterSnapshotIdentifier=src,
                TargetDBClusterSnapshotIdentifier=tgt,
            )
            assert resp["DBClusterSnapshot"]["DBClusterSnapshotIdentifier"] == tgt
        finally:
            try:
                neptune.delete_db_cluster_snapshot(DBClusterSnapshotIdentifier=src)
            except Exception:
                pass  # best-effort cleanup
            try:
                neptune.delete_db_cluster_snapshot(DBClusterSnapshotIdentifier=tgt)
            except Exception:
                pass  # best-effort cleanup

    def test_describe_db_cluster_snapshot_attributes(self, neptune):
        snap_id = _unique("nep-snap")
        neptune.create_db_cluster_snapshot(
            DBClusterSnapshotIdentifier=snap_id,
            DBClusterIdentifier=self.cluster_id,
        )
        try:
            resp = neptune.describe_db_cluster_snapshot_attributes(
                DBClusterSnapshotIdentifier=snap_id
            )
            attrs = resp["DBClusterSnapshotAttributesResult"]
            assert attrs["DBClusterSnapshotIdentifier"] == snap_id
            assert "DBClusterSnapshotAttributes" in attrs
        finally:
            neptune.delete_db_cluster_snapshot(DBClusterSnapshotIdentifier=snap_id)

    def test_modify_db_cluster_snapshot_attribute(self, neptune):
        snap_id = _unique("nep-snap")
        neptune.create_db_cluster_snapshot(
            DBClusterSnapshotIdentifier=snap_id,
            DBClusterIdentifier=self.cluster_id,
        )
        try:
            resp = neptune.modify_db_cluster_snapshot_attribute(
                DBClusterSnapshotIdentifier=snap_id,
                AttributeName="restore",
                ValuesToAdd=["123456789012"],
            )
            attrs = resp["DBClusterSnapshotAttributesResult"]
            assert attrs["DBClusterSnapshotIdentifier"] == snap_id
        finally:
            neptune.delete_db_cluster_snapshot(DBClusterSnapshotIdentifier=snap_id)

    def test_delete_db_cluster_snapshot(self, neptune):
        snap_id = _unique("nep-snap")
        neptune.create_db_cluster_snapshot(
            DBClusterSnapshotIdentifier=snap_id,
            DBClusterIdentifier=self.cluster_id,
        )
        neptune.delete_db_cluster_snapshot(DBClusterSnapshotIdentifier=snap_id)
        resp = neptune.describe_db_cluster_snapshots()
        ids = [s["DBClusterSnapshotIdentifier"] for s in resp["DBClusterSnapshots"]]
        assert snap_id not in ids


class TestNeptuneDBParameterGroupOperations:
    """Tests for CreateDBParameterGroup, DescribeDBParameters,
    ModifyDBParameterGroup, CopyDBParameterGroup, DeleteDBParameterGroup."""

    def test_create_db_parameter_group(self, neptune):
        name = _unique("nep-pg")
        resp = neptune.create_db_parameter_group(
            DBParameterGroupName=name,
            DBParameterGroupFamily="neptune1",
            Description="test param group",
        )
        try:
            assert resp["DBParameterGroup"]["DBParameterGroupName"] == name
            assert resp["DBParameterGroup"]["Description"] == "test param group"
        finally:
            neptune.delete_db_parameter_group(DBParameterGroupName=name)

    def test_describe_db_parameters(self, neptune):
        name = _unique("nep-pg")
        neptune.create_db_parameter_group(
            DBParameterGroupName=name,
            DBParameterGroupFamily="neptune1",
            Description="test",
        )
        try:
            resp = neptune.describe_db_parameters(DBParameterGroupName=name)
            assert "Parameters" in resp
            assert isinstance(resp["Parameters"], list)
        finally:
            neptune.delete_db_parameter_group(DBParameterGroupName=name)

    def test_modify_db_parameter_group(self, neptune):
        name = _unique("nep-pg")
        neptune.create_db_parameter_group(
            DBParameterGroupName=name,
            DBParameterGroupFamily="neptune1",
            Description="test",
        )
        try:
            resp = neptune.modify_db_parameter_group(
                DBParameterGroupName=name,
                Parameters=[
                    {
                        "ParameterName": "neptune_query_timeout",
                        "ParameterValue": "240000",
                        "ApplyMethod": "pending-reboot",
                    }
                ],
            )
            assert resp["DBParameterGroupName"] == name
        finally:
            neptune.delete_db_parameter_group(DBParameterGroupName=name)

    def test_copy_db_parameter_group(self, neptune):
        src = _unique("nep-pg-src")
        tgt = _unique("nep-pg-tgt")
        neptune.create_db_parameter_group(
            DBParameterGroupName=src,
            DBParameterGroupFamily="neptune1",
            Description="source",
        )
        try:
            resp = neptune.copy_db_parameter_group(
                SourceDBParameterGroupIdentifier=src,
                TargetDBParameterGroupIdentifier=tgt,
                TargetDBParameterGroupDescription="copied",
            )
            assert resp["DBParameterGroup"]["DBParameterGroupName"] == tgt
            assert resp["DBParameterGroup"]["Description"] == "copied"
        finally:
            try:
                neptune.delete_db_parameter_group(DBParameterGroupName=src)
            except Exception:
                pass  # best-effort cleanup
            try:
                neptune.delete_db_parameter_group(DBParameterGroupName=tgt)
            except Exception:
                pass  # best-effort cleanup

    def test_delete_db_parameter_group(self, neptune):
        name = _unique("nep-pg")
        neptune.create_db_parameter_group(
            DBParameterGroupName=name,
            DBParameterGroupFamily="neptune1",
            Description="to delete",
        )
        neptune.delete_db_parameter_group(DBParameterGroupName=name)
        resp = neptune.describe_db_parameter_groups()
        names = [g["DBParameterGroupName"] for g in resp["DBParameterGroups"]]
        assert name not in names


class TestNeptuneDescribeClusterParameterGroups:
    """Tests for DescribeDBClusterParameters."""

    def test_describe_db_cluster_parameters(self, neptune):
        name = _unique("nep-cpg")
        neptune.create_db_cluster_parameter_group(
            DBClusterParameterGroupName=name,
            DBParameterGroupFamily="neptune1",
            Description="test",
        )
        try:
            resp = neptune.describe_db_cluster_parameters(DBClusterParameterGroupName=name)
            assert "Parameters" in resp
            assert isinstance(resp["Parameters"], list)
        finally:
            neptune.delete_db_cluster_parameter_group(DBClusterParameterGroupName=name)


class TestNeptuneEventSubscriptionOperations:
    """Tests for CreateEventSubscription, DeleteEventSubscription."""

    def test_create_event_subscription(self, neptune):
        name = _unique("nep-sub")
        resp = neptune.create_event_subscription(
            SubscriptionName=name,
            SnsTopicArn="arn:aws:sns:us-east-1:123456789012:test-topic",
        )
        try:
            sub = resp["EventSubscription"]
            assert sub["CustSubscriptionId"] == name
            assert "SnsTopicArn" in sub
        finally:
            neptune.delete_event_subscription(SubscriptionName=name)

    def test_delete_event_subscription(self, neptune):
        name = _unique("nep-sub")
        neptune.create_event_subscription(
            SubscriptionName=name,
            SnsTopicArn="arn:aws:sns:us-east-1:123456789012:test-topic",
        )
        neptune.delete_event_subscription(SubscriptionName=name)
        resp = neptune.describe_event_subscriptions()
        names = [s["CustSubscriptionId"] for s in resp["EventSubscriptionsList"]]
        assert name not in names


class TestNeptuneGlobalClusterOperations:
    """Tests for CreateGlobalCluster, DeleteGlobalCluster."""

    def test_create_global_cluster(self, neptune):
        gc_id = _unique("nep-gc")
        resp = neptune.create_global_cluster(
            GlobalClusterIdentifier=gc_id,
            Engine="neptune",
        )
        try:
            assert resp["GlobalCluster"]["GlobalClusterIdentifier"] == gc_id
            assert resp["GlobalCluster"]["Engine"] == "neptune"
        finally:
            neptune.delete_global_cluster(GlobalClusterIdentifier=gc_id)

    def test_delete_global_cluster(self, neptune):
        gc_id = _unique("nep-gc")
        neptune.create_global_cluster(
            GlobalClusterIdentifier=gc_id,
            Engine="neptune",
        )
        neptune.delete_global_cluster(GlobalClusterIdentifier=gc_id)
        resp = neptune.describe_global_clusters()
        ids = [g["GlobalClusterIdentifier"] for g in resp["GlobalClusters"]]
        assert gc_id not in ids


class TestNeptuneSubnetGroupModify:
    """Tests for ModifyDBSubnetGroup."""

    def test_modify_db_subnet_group(self, neptune, subnet_ids):
        name = _unique("nep-mod-sg")
        neptune.create_db_subnet_group(
            DBSubnetGroupName=name,
            DBSubnetGroupDescription="original desc",
            SubnetIds=subnet_ids,
        )
        resp = neptune.modify_db_subnet_group(
            DBSubnetGroupName=name,
            DBSubnetGroupDescription="modified desc",
            SubnetIds=subnet_ids,
        )
        assert resp["DBSubnetGroup"]["DBSubnetGroupDescription"] == "modified desc"
        try:
            neptune.delete_db_subnet_group(DBSubnetGroupName=name)
        except Exception:
            pass  # best-effort cleanup


class TestNeptuneOrderableInstances:
    """Tests for DescribeOrderableDBInstanceOptions."""

    def test_describe_orderable_db_instance_options(self, neptune):
        resp = neptune.describe_orderable_db_instance_options(Engine="neptune")
        assert "OrderableDBInstanceOptions" in resp
        assert isinstance(resp["OrderableDBInstanceOptions"], list)
        assert len(resp["OrderableDBInstanceOptions"]) > 0


class TestNeptuneDescribeDBEngineVersions:
    """Tests for DescribeDBEngineVersions."""

    def test_describe_db_engine_versions(self, neptune):
        resp = neptune.describe_db_engine_versions(Engine="neptune")
        assert "DBEngineVersions" in resp
        assert isinstance(resp["DBEngineVersions"], list)
        assert len(resp["DBEngineVersions"]) > 0

    def test_describe_db_engine_versions_has_engine(self, neptune):
        resp = neptune.describe_db_engine_versions(Engine="neptune")
        for ver in resp["DBEngineVersions"]:
            assert ver["Engine"] == "neptune"
            assert "EngineVersion" in ver

    def test_describe_db_engine_versions_has_param_group_family(self, neptune):
        resp = neptune.describe_db_engine_versions(Engine="neptune")
        for ver in resp["DBEngineVersions"]:
            assert "DBParameterGroupFamily" in ver


class TestNeptuneEngineDefaultClusterParameters:
    """Tests for DescribeEngineDefaultClusterParameters."""

    def test_describe_engine_default_cluster_parameters(self, neptune):
        resp = neptune.describe_engine_default_cluster_parameters(DBParameterGroupFamily="neptune1")
        result = resp["EngineDefaults"]
        assert "Parameters" in result
        assert isinstance(result["Parameters"], list)

    def test_describe_engine_default_cluster_parameters_has_family(self, neptune):
        resp = neptune.describe_engine_default_cluster_parameters(DBParameterGroupFamily="neptune1")
        assert resp["EngineDefaults"]["DBParameterGroupFamily"] == "neptune1"


class TestNeptuneEngineDefaultParameters:
    """Tests for DescribeEngineDefaultParameters."""

    def test_describe_engine_default_parameters(self, neptune):
        resp = neptune.describe_engine_default_parameters(DBParameterGroupFamily="neptune1")
        result = resp["EngineDefaults"]
        assert "Parameters" in result
        assert isinstance(result["Parameters"], list)

    def test_describe_engine_default_parameters_has_family(self, neptune):
        resp = neptune.describe_engine_default_parameters(DBParameterGroupFamily="neptune1")
        assert resp["EngineDefaults"]["DBParameterGroupFamily"] == "neptune1"


class TestNeptuneDescribeEventCategories:
    """Tests for DescribeEventCategories."""

    def test_describe_event_categories(self, neptune):
        resp = neptune.describe_event_categories()
        assert "EventCategoriesMapList" in resp
        assert isinstance(resp["EventCategoriesMapList"], list)


class TestNeptuneDescribePendingMaintenanceActions:
    """Tests for DescribePendingMaintenanceActions."""

    def test_describe_pending_maintenance_actions(self, neptune):
        resp = neptune.describe_pending_maintenance_actions()
        assert "PendingMaintenanceActions" in resp
        assert isinstance(resp["PendingMaintenanceActions"], list)


class TestNeptuneDescribeValidDBInstanceModifications:
    """Tests for DescribeValidDBInstanceModifications."""

    @pytest.fixture(autouse=True)
    def _setup(self, neptune, subnet_ids):
        self.neptune = neptune
        self.sg_name = _unique("valid-sg")
        self.cluster_id = _unique("valid-cl")
        self.inst_id = _unique("valid-inst")
        neptune.create_db_subnet_group(
            DBSubnetGroupName=self.sg_name,
            DBSubnetGroupDescription="valid mod tests",
            SubnetIds=subnet_ids,
        )
        neptune.create_db_cluster(
            DBClusterIdentifier=self.cluster_id,
            Engine="neptune",
            DBSubnetGroupName=self.sg_name,
        )
        neptune.create_db_instance(
            DBInstanceIdentifier=self.inst_id,
            DBInstanceClass="db.r5.large",
            Engine="neptune",
            DBClusterIdentifier=self.cluster_id,
        )
        yield
        neptune.delete_db_instance(DBInstanceIdentifier=self.inst_id, SkipFinalSnapshot=True)
        neptune.delete_db_cluster(DBClusterIdentifier=self.cluster_id, SkipFinalSnapshot=True)
        neptune.delete_db_subnet_group(DBSubnetGroupName=self.sg_name)

    def test_describe_valid_db_instance_modifications(self, neptune):
        resp = neptune.describe_valid_db_instance_modifications(DBInstanceIdentifier=self.inst_id)
        msg = resp["ValidDBInstanceModificationsMessage"]
        assert "Storage" in msg
        assert isinstance(msg["Storage"], list)

    def test_describe_valid_db_instance_modifications_nonexistent(self, neptune):
        with pytest.raises(ClientError) as exc_info:
            neptune.describe_valid_db_instance_modifications(
                DBInstanceIdentifier="nonexistent-instance"
            )
        assert "NotFound" in exc_info.value.response["Error"]["Code"]


class TestNeptuneDescribeDBClusterEndpoints:
    """Tests for DescribeDBClusterEndpoints."""

    def test_describe_db_cluster_endpoints_empty(self, neptune):
        resp = neptune.describe_db_cluster_endpoints()
        assert "DBClusterEndpoints" in resp
        assert isinstance(resp["DBClusterEndpoints"], list)


class TestNeptuneDBClusterEndpointOperations:
    """Tests for CreateDBClusterEndpoint with a real cluster."""

    @pytest.fixture(autouse=True)
    def _setup(self, neptune, subnet_ids):
        self.neptune = neptune
        self.sg_name = _unique("ep-sg")
        self.cluster_id = _unique("ep-cl")
        neptune.create_db_subnet_group(
            DBSubnetGroupName=self.sg_name,
            DBSubnetGroupDescription="endpoint tests",
            SubnetIds=subnet_ids,
        )
        neptune.create_db_cluster(
            DBClusterIdentifier=self.cluster_id,
            Engine="neptune",
            DBSubnetGroupName=self.sg_name,
        )
        yield
        neptune.delete_db_cluster(DBClusterIdentifier=self.cluster_id, SkipFinalSnapshot=True)
        neptune.delete_db_subnet_group(DBSubnetGroupName=self.sg_name)

    def test_create_db_cluster_endpoint(self, neptune):
        ep_id = _unique("ep")
        resp = neptune.create_db_cluster_endpoint(
            DBClusterIdentifier=self.cluster_id,
            DBClusterEndpointIdentifier=ep_id,
            EndpointType="READER",
        )
        assert resp["DBClusterEndpointIdentifier"] == ep_id
        assert resp["EndpointType"] == "READER"
        assert resp["DBClusterIdentifier"] == self.cluster_id

    def test_describe_db_cluster_endpoints_filtered(self, neptune):
        ep_id = _unique("ep")
        neptune.create_db_cluster_endpoint(
            DBClusterIdentifier=self.cluster_id,
            DBClusterEndpointIdentifier=ep_id,
            EndpointType="READER",
        )
        resp = neptune.describe_db_cluster_endpoints(DBClusterIdentifier=self.cluster_id)
        ep_ids = [e["DBClusterEndpointIdentifier"] for e in resp["DBClusterEndpoints"]]
        assert ep_id in ep_ids


class TestNeptuneModifyGlobalCluster:
    """Tests for ModifyGlobalCluster and FailoverGlobalCluster."""

    def test_modify_global_cluster(self, neptune):
        gc_id = _unique("gc-mod")
        neptune.create_global_cluster(GlobalClusterIdentifier=gc_id, Engine="neptune")
        try:
            new_id = _unique("gc-new")
            resp = neptune.modify_global_cluster(
                GlobalClusterIdentifier=gc_id,
                NewGlobalClusterIdentifier=new_id,
            )
            assert resp["GlobalCluster"]["GlobalClusterIdentifier"] == new_id
            neptune.delete_global_cluster(GlobalClusterIdentifier=new_id)
        except Exception:
            try:
                neptune.delete_global_cluster(GlobalClusterIdentifier=gc_id)
            except Exception:
                pass  # best-effort cleanup
            raise

    def test_modify_nonexistent_global_cluster(self, neptune):
        with pytest.raises(ClientError) as exc_info:
            neptune.modify_global_cluster(
                GlobalClusterIdentifier="nonexistent-gc",
            )
        assert "NotFound" in exc_info.value.response["Error"]["Code"]

    def test_failover_global_cluster_nonexistent(self, neptune):
        with pytest.raises(ClientError) as exc_info:
            neptune.failover_global_cluster(
                GlobalClusterIdentifier="nonexistent-gc",
                TargetDbClusterIdentifier="arn:aws:rds:us-east-1:123456789012:cluster:fake",
            )
        assert "NotFound" in exc_info.value.response["Error"]["Code"]


class TestNeptuneApplyPendingMaintenanceAction:
    """Tests for ApplyPendingMaintenanceAction."""

    def test_apply_pending_maintenance_action(self, neptune, subnet_ids):
        sg_name = _unique("maint-sg")
        cluster_id = _unique("maint-cl")
        inst_id = _unique("maint-inst")
        neptune.create_db_subnet_group(
            DBSubnetGroupName=sg_name,
            DBSubnetGroupDescription="maintenance tests",
            SubnetIds=subnet_ids,
        )
        neptune.create_db_cluster(
            DBClusterIdentifier=cluster_id,
            Engine="neptune",
            DBSubnetGroupName=sg_name,
        )
        neptune.create_db_instance(
            DBInstanceIdentifier=inst_id,
            DBInstanceClass="db.r5.large",
            Engine="neptune",
            DBClusterIdentifier=cluster_id,
        )
        try:
            resp = neptune.apply_pending_maintenance_action(
                ResourceIdentifier=f"arn:aws:rds:us-east-1:123456789012:db:{inst_id}",
                ApplyAction="system-update",
                OptInType="immediate",
            )
            assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200
            assert "ResourcePendingMaintenanceActions" in resp
        finally:
            neptune.delete_db_instance(DBInstanceIdentifier=inst_id, SkipFinalSnapshot=True)
            neptune.delete_db_cluster(DBClusterIdentifier=cluster_id, SkipFinalSnapshot=True)
            neptune.delete_db_subnet_group(DBSubnetGroupName=sg_name)


class TestNeptuneModifyEventSubscription:
    """Tests for ModifyEventSubscription."""

    def test_modify_event_subscription(self, neptune):
        name = _unique("nep-sub")
        neptune.create_event_subscription(
            SubscriptionName=name,
            SnsTopicArn="arn:aws:sns:us-east-1:123456789012:test-topic",
        )
        try:
            resp = neptune.modify_event_subscription(SubscriptionName=name, Enabled=False)
            assert resp["EventSubscription"]["CustSubscriptionId"] == name
        finally:
            neptune.delete_event_subscription(SubscriptionName=name)

    def test_modify_nonexistent_event_subscription(self, neptune):
        with pytest.raises(ClientError) as exc_info:
            neptune.modify_event_subscription(SubscriptionName="nonexistent-sub")
        assert "NotFound" in exc_info.value.response["Error"]["Code"]

    def test_add_source_identifier_to_subscription(self, neptune):
        """AddSourceIdentifierToSubscription adds a source to an event sub."""
        name = _unique("nep-sub-src")
        neptune.create_event_subscription(
            SubscriptionName=name,
            SnsTopicArn="arn:aws:sns:us-east-1:123456789012:test-topic",
        )
        try:
            resp = neptune.add_source_identifier_to_subscription(
                SubscriptionName=name,
                SourceIdentifier="test-db-instance",
            )
            sub = resp["EventSubscription"]
            assert sub["CustSubscriptionId"] == name
            assert "test-db-instance" in sub.get("SourceIdsList", [])
        finally:
            neptune.delete_event_subscription(SubscriptionName=name)

    def test_remove_source_identifier_from_subscription(self, neptune):
        """RemoveSourceIdentifierFromSubscription removes a source."""
        name = _unique("nep-sub-rm")
        neptune.create_event_subscription(
            SubscriptionName=name,
            SnsTopicArn="arn:aws:sns:us-east-1:123456789012:test-topic",
        )
        try:
            neptune.add_source_identifier_to_subscription(
                SubscriptionName=name,
                SourceIdentifier="test-db-instance",
            )
            resp = neptune.remove_source_identifier_from_subscription(
                SubscriptionName=name,
                SourceIdentifier="test-db-instance",
            )
            sub = resp["EventSubscription"]
            assert sub["CustSubscriptionId"] == name
            assert "test-db-instance" not in sub.get("SourceIdsList", [])
        finally:
            neptune.delete_event_subscription(SubscriptionName=name)


class TestNeptuneDescribeOps:
    """Additional describe/list operations returning empty results."""

    def test_describe_db_cluster_snapshots_empty(self, neptune):
        resp = neptune.describe_db_cluster_snapshots()
        assert "DBClusterSnapshots" in resp
        assert isinstance(resp["DBClusterSnapshots"], list)

    def test_describe_db_subnet_groups_all(self, neptune):
        resp = neptune.describe_db_subnet_groups()
        assert "DBSubnetGroups" in resp
        assert isinstance(resp["DBSubnetGroups"], list)


class TestNeptuneClusterEndpointLifecycle:
    """Tests for ModifyDBClusterEndpoint and DeleteDBClusterEndpoint."""

    @pytest.fixture(autouse=True)
    def _setup(self, neptune, subnet_ids):
        self.neptune = neptune
        self.sg_name = _unique("epl-sg")
        self.cluster_id = _unique("epl-cl")
        neptune.create_db_subnet_group(
            DBSubnetGroupName=self.sg_name,
            DBSubnetGroupDescription="endpoint lifecycle tests",
            SubnetIds=subnet_ids,
        )
        neptune.create_db_cluster(
            DBClusterIdentifier=self.cluster_id,
            Engine="neptune",
            DBSubnetGroupName=self.sg_name,
        )
        yield
        neptune.delete_db_cluster(DBClusterIdentifier=self.cluster_id, SkipFinalSnapshot=True)
        neptune.delete_db_subnet_group(DBSubnetGroupName=self.sg_name)

    def test_modify_db_cluster_endpoint(self, neptune):
        ep_id = _unique("epl-ep")
        neptune.create_db_cluster_endpoint(
            DBClusterIdentifier=self.cluster_id,
            DBClusterEndpointIdentifier=ep_id,
            EndpointType="READER",
        )
        try:
            resp = neptune.modify_db_cluster_endpoint(
                DBClusterEndpointIdentifier=ep_id,
                EndpointType="ANY",
            )
            assert resp["DBClusterEndpointIdentifier"] == ep_id
            assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200
        finally:
            try:
                neptune.delete_db_cluster_endpoint(DBClusterEndpointIdentifier=ep_id)
            except Exception:
                pass  # best-effort cleanup

    def test_delete_db_cluster_endpoint(self, neptune):
        ep_id = _unique("epl-ep")
        neptune.create_db_cluster_endpoint(
            DBClusterIdentifier=self.cluster_id,
            DBClusterEndpointIdentifier=ep_id,
            EndpointType="READER",
        )
        resp = neptune.delete_db_cluster_endpoint(DBClusterEndpointIdentifier=ep_id)
        assert resp["DBClusterEndpointIdentifier"] == ep_id
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200


class TestNeptuneRestoreOps:
    """Tests for RestoreDBClusterFromSnapshot and RestoreDBClusterToPointInTime."""

    @pytest.fixture(autouse=True)
    def _setup(self, neptune, subnet_ids):
        self.neptune = neptune
        self.sg_name = _unique("rst-sg")
        self.cluster_id = _unique("rst-cl")
        neptune.create_db_subnet_group(
            DBSubnetGroupName=self.sg_name,
            DBSubnetGroupDescription="restore tests",
            SubnetIds=subnet_ids,
        )
        neptune.create_db_cluster(
            DBClusterIdentifier=self.cluster_id,
            Engine="neptune",
            DBSubnetGroupName=self.sg_name,
        )
        yield
        neptune.delete_db_cluster(DBClusterIdentifier=self.cluster_id, SkipFinalSnapshot=True)
        neptune.delete_db_subnet_group(DBSubnetGroupName=self.sg_name)

    def test_restore_db_cluster_from_snapshot_nonexistent(self, neptune):
        """RestoreDBClusterFromSnapshot raises error for nonexistent snapshot."""
        with pytest.raises(ClientError) as exc_info:
            neptune.restore_db_cluster_from_snapshot(
                DBClusterIdentifier="fake-restore",
                SnapshotIdentifier="nonexistent-snap",
                Engine="neptune",
            )
        assert "NotFound" in exc_info.value.response["Error"]["Code"]

    def test_restore_db_cluster_to_point_in_time_nonexistent(self, neptune):
        """RestoreDBClusterToPointInTime raises error for nonexistent source cluster."""
        with pytest.raises(ClientError) as exc_info:
            neptune.restore_db_cluster_to_point_in_time(
                DBClusterIdentifier="fake-pit",
                SourceDBClusterIdentifier="nonexistent-source",
                UseLatestRestorableTime=True,
            )
        assert "NotFound" in exc_info.value.response["Error"]["Code"]

    def test_restore_db_cluster_to_point_in_time(self, neptune):
        """RestoreDBClusterToPointInTime restores from an existing cluster."""
        restored_id = _unique("pit-cl")
        try:
            resp = neptune.restore_db_cluster_to_point_in_time(
                DBClusterIdentifier=restored_id,
                SourceDBClusterIdentifier=self.cluster_id,
                UseLatestRestorableTime=True,
            )
            assert resp["DBCluster"]["DBClusterIdentifier"] == restored_id
        finally:
            try:
                neptune.delete_db_cluster(DBClusterIdentifier=restored_id, SkipFinalSnapshot=True)
            except Exception:
                pass  # best-effort cleanup


class TestNeptuneGlobalClusterAdvanced:
    """Tests for RemoveFromGlobalCluster and SwitchoverGlobalCluster."""

    def test_remove_from_global_cluster(self, neptune):
        """RemoveFromGlobalCluster with fake ARN returns 200."""
        resp = neptune.remove_from_global_cluster(
            GlobalClusterIdentifier="fake-gc",
            DbClusterIdentifier="arn:aws:rds:us-east-1:123456789012:cluster:fake",
        )
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200

    def test_switchover_global_cluster_nonexistent(self, neptune):
        """SwitchoverGlobalCluster raises error for nonexistent global cluster."""
        with pytest.raises(ClientError) as exc_info:
            neptune.switchover_global_cluster(
                GlobalClusterIdentifier="nonexistent-gc",
                TargetDbClusterIdentifier=("arn:aws:rds:us-east-1:123456789012:cluster:fake"),
            )
        assert "NotFound" in exc_info.value.response["Error"]["Code"]

    def test_promote_read_replica_db_cluster_nonexistent(self, neptune):
        """PromoteReadReplicaDBCluster raises error for nonexistent cluster."""
        with pytest.raises(ClientError) as exc_info:
            neptune.promote_read_replica_db_cluster(
                DBClusterIdentifier="nonexistent-cluster",
            )
        assert "NotFound" in exc_info.value.response["Error"]["Code"]


class TestNeptuneEventSubscriptionAdvanced:
    """Tests for AddSourceIdentifierToSubscription, RemoveSourceIdentifierFromSubscription."""

    def test_add_source_identifier_to_subscription(self, neptune):
        """AddSourceIdentifierToSubscription adds source to existing subscription."""
        name = _unique("sub-src")
        neptune.create_event_subscription(
            SubscriptionName=name,
            SnsTopicArn="arn:aws:sns:us-east-1:123456789012:test-topic",
        )
        try:
            resp = neptune.add_source_identifier_to_subscription(
                SubscriptionName=name,
                SourceIdentifier="my-cluster-id",
            )
            sub = resp["EventSubscription"]
            assert sub["CustSubscriptionId"] == name
            assert "SourceIdsList" in sub
        finally:
            neptune.delete_event_subscription(SubscriptionName=name)

    def test_remove_source_identifier_from_subscription(self, neptune):
        """RemoveSourceIdentifierFromSubscription removes source from subscription."""
        name = _unique("sub-rm")
        neptune.create_event_subscription(
            SubscriptionName=name,
            SnsTopicArn="arn:aws:sns:us-east-1:123456789012:test-topic",
        )
        try:
            # Add first, then remove
            neptune.add_source_identifier_to_subscription(
                SubscriptionName=name,
                SourceIdentifier="my-cluster-id",
            )
            resp = neptune.remove_source_identifier_from_subscription(
                SubscriptionName=name,
                SourceIdentifier="my-cluster-id",
            )
            sub = resp["EventSubscription"]
            assert sub["CustSubscriptionId"] == name
        finally:
            neptune.delete_event_subscription(SubscriptionName=name)

    def test_add_source_identifier_nonexistent_subscription(self, neptune):
        """AddSourceIdentifierToSubscription raises error for nonexistent subscription."""
        with pytest.raises(ClientError) as exc_info:
            neptune.add_source_identifier_to_subscription(
                SubscriptionName="nonexistent-sub",
                SourceIdentifier="fake-source",
            )
        assert "NotFound" in exc_info.value.response["Error"]["Code"]

    def test_remove_source_identifier_nonexistent_subscription(self, neptune):
        """RemoveSourceIdentifierFromSubscription raises error for nonexistent sub."""
        with pytest.raises(ClientError) as exc_info:
            neptune.remove_source_identifier_from_subscription(
                SubscriptionName="nonexistent-sub",
                SourceIdentifier="fake-source",
            )
        assert "NotFound" in exc_info.value.response["Error"]["Code"]


class TestNeptuneFailoverDBCluster:
    """Tests for FailoverDBCluster."""

    def test_failover_db_cluster_nonexistent(self, neptune):
        """FailoverDBCluster raises error for nonexistent cluster."""
        with pytest.raises(ClientError) as exc_info:
            neptune.failover_db_cluster(DBClusterIdentifier="nonexistent-cluster")
        assert "NotFound" in exc_info.value.response["Error"]["Code"]

    def test_failover_db_cluster_insufficient_instances(self, neptune, subnet_ids):
        """FailoverDBCluster requires at least two instances."""
        sg_name = _unique("fo-sg")
        cluster_id = _unique("fo-cl")
        neptune.create_db_subnet_group(
            DBSubnetGroupName=sg_name,
            DBSubnetGroupDescription="failover tests",
            SubnetIds=subnet_ids,
        )
        neptune.create_db_cluster(
            DBClusterIdentifier=cluster_id,
            Engine="neptune",
            DBSubnetGroupName=sg_name,
        )
        try:
            with pytest.raises(ClientError) as exc_info:
                neptune.failover_db_cluster(DBClusterIdentifier=cluster_id)
            assert "InvalidDBClusterState" in exc_info.value.response["Error"]["Code"]
        finally:
            neptune.delete_db_cluster(DBClusterIdentifier=cluster_id, SkipFinalSnapshot=True)
            neptune.delete_db_subnet_group(DBSubnetGroupName=sg_name)
