"""Neptune compatibility tests."""

import uuid

import pytest

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
                pass


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
                pass
            try:
                neptune.delete_db_cluster_snapshot(DBClusterSnapshotIdentifier=tgt)
            except Exception:
                pass

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
                pass
            try:
                neptune.delete_db_parameter_group(DBParameterGroupName=tgt)
            except Exception:
                pass

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
            pass


class TestNeptuneOrderableInstances:
    """Tests for DescribeOrderableDBInstanceOptions."""

    def test_describe_orderable_db_instance_options(self, neptune):
        resp = neptune.describe_orderable_db_instance_options(Engine="neptune")
        assert "OrderableDBInstanceOptions" in resp
        assert isinstance(resp["OrderableDBInstanceOptions"], list)
        assert len(resp["OrderableDBInstanceOptions"]) > 0
