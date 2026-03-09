"""RDS compatibility tests."""

import uuid

import pytest
from botocore.exceptions import ClientError

from tests.compatibility.conftest import make_client


@pytest.fixture
def rds():
    return make_client("rds")


@pytest.fixture
def ec2():
    return make_client("ec2")


def _unique(prefix):
    return f"{prefix}-{uuid.uuid4().hex[:8]}"


@pytest.fixture
def db_instance(rds):
    """Create a DB instance and clean up after."""
    name = _unique("compat-db")
    rds.create_db_instance(
        DBInstanceIdentifier=name,
        DBInstanceClass="db.t3.micro",
        Engine="mysql",
        MasterUsername="admin",
        MasterUserPassword="password123",
        Tags=[{"Key": "created-by", "Value": "compat-test"}],
    )
    yield name
    try:
        rds.delete_db_instance(DBInstanceIdentifier=name, SkipFinalSnapshot=True)
    except ClientError:
        pass


@pytest.fixture
def subnet_group(rds, ec2):
    """Create a DB subnet group with a VPC and two subnets, clean up after."""
    name = _unique("compat-sg")
    vpc = ec2.create_vpc(CidrBlock="10.88.0.0/16")
    vpc_id = vpc["Vpc"]["VpcId"]
    s1 = ec2.create_subnet(VpcId=vpc_id, CidrBlock="10.88.1.0/24", AvailabilityZone="us-east-1a")
    s2 = ec2.create_subnet(VpcId=vpc_id, CidrBlock="10.88.2.0/24", AvailabilityZone="us-east-1b")
    subnet_ids = [s1["Subnet"]["SubnetId"], s2["Subnet"]["SubnetId"]]
    rds.create_db_subnet_group(
        DBSubnetGroupName=name,
        DBSubnetGroupDescription="compat test subnet group",
        SubnetIds=subnet_ids,
    )
    yield name
    try:
        rds.delete_db_subnet_group(DBSubnetGroupName=name)
    except ClientError:
        pass
    for sid in subnet_ids:
        try:
            ec2.delete_subnet(SubnetId=sid)
        except ClientError:
            pass
    try:
        ec2.delete_vpc(VpcId=vpc_id)
    except ClientError:
        pass


@pytest.fixture
def param_group(rds):
    """Create a DB parameter group and clean up after."""
    name = _unique("compat-pg")
    rds.create_db_parameter_group(
        DBParameterGroupName=name,
        DBParameterGroupFamily="mysql8.0",
        Description="compat test parameter group",
    )
    yield name
    try:
        rds.delete_db_parameter_group(DBParameterGroupName=name)
    except ClientError:
        pass


class TestRDSDBInstanceOperations:
    def test_create_and_describe_db_instance(self, rds, db_instance):
        resp = rds.describe_db_instances(DBInstanceIdentifier=db_instance)
        instances = resp["DBInstances"]
        assert len(instances) == 1
        inst = instances[0]
        assert inst["DBInstanceIdentifier"] == db_instance
        assert inst["Engine"] == "mysql"
        assert inst["DBInstanceClass"] == "db.t3.micro"
        assert inst["MasterUsername"] == "admin"
        assert inst["DBInstanceStatus"] in ("available", "creating")

    def test_list_db_instances(self, rds, db_instance):
        resp = rds.describe_db_instances()
        identifiers = [i["DBInstanceIdentifier"] for i in resp["DBInstances"]]
        assert db_instance in identifiers

    def test_modify_db_instance(self, rds, db_instance):
        resp = rds.modify_db_instance(
            DBInstanceIdentifier=db_instance,
            MasterUserPassword="newpassword456",
        )
        assert resp["DBInstance"]["DBInstanceIdentifier"] == db_instance

    def test_delete_db_instance(self, rds):
        name = _unique("compat-del")
        rds.create_db_instance(
            DBInstanceIdentifier=name,
            DBInstanceClass="db.t3.micro",
            Engine="mysql",
            MasterUsername="admin",
            MasterUserPassword="password123",
        )
        rds.delete_db_instance(DBInstanceIdentifier=name, SkipFinalSnapshot=True)
        # After deletion, describing should fail
        with pytest.raises(ClientError) as exc:
            rds.describe_db_instances(DBInstanceIdentifier=name)
        assert exc.value.response["Error"]["Code"] == "DBInstanceNotFound"

    def test_add_and_list_tags(self, rds, db_instance):
        arn = f"arn:aws:rds:us-east-1:123456789012:db:{db_instance}"
        rds.add_tags_to_resource(
            ResourceName=arn,
            Tags=[
                {"Key": "env", "Value": "test"},
                {"Key": "team", "Value": "backend"},
            ],
        )
        resp = rds.list_tags_for_resource(ResourceName=arn)
        tag_map = {t["Key"]: t["Value"] for t in resp["TagList"]}
        assert tag_map["env"] == "test"
        assert tag_map["team"] == "backend"

    def test_remove_tags(self, rds, db_instance):
        arn = f"arn:aws:rds:us-east-1:123456789012:db:{db_instance}"
        rds.add_tags_to_resource(
            ResourceName=arn,
            Tags=[{"Key": "remove-me", "Value": "yes"}],
        )
        rds.remove_tags_from_resource(ResourceName=arn, TagKeys=["remove-me"])
        resp = rds.list_tags_for_resource(ResourceName=arn)
        keys = [t["Key"] for t in resp["TagList"]]
        assert "remove-me" not in keys

    def test_create_with_tags(self, rds, db_instance):
        arn = f"arn:aws:rds:us-east-1:123456789012:db:{db_instance}"
        resp = rds.list_tags_for_resource(ResourceName=arn)
        tag_map = {t["Key"]: t["Value"] for t in resp["TagList"]}
        assert tag_map.get("created-by") == "compat-test"

    def test_reboot_db_instance(self, rds, db_instance):
        resp = rds.reboot_db_instance(DBInstanceIdentifier=db_instance)
        assert resp["DBInstance"]["DBInstanceIdentifier"] == db_instance

    def test_stop_db_instance(self, rds, db_instance):
        resp = rds.stop_db_instance(DBInstanceIdentifier=db_instance)
        assert resp["DBInstance"]["DBInstanceIdentifier"] == db_instance

    def test_start_db_instance(self, rds, db_instance):
        # Stop first, then start
        rds.stop_db_instance(DBInstanceIdentifier=db_instance)
        resp = rds.start_db_instance(DBInstanceIdentifier=db_instance)
        assert resp["DBInstance"]["DBInstanceIdentifier"] == db_instance


class TestRDSSubnetGroupOperations:
    def test_create_and_describe_subnet_group(self, rds, subnet_group):
        resp = rds.describe_db_subnet_groups(DBSubnetGroupName=subnet_group)
        groups = resp["DBSubnetGroups"]
        assert len(groups) == 1
        grp = groups[0]
        assert grp["DBSubnetGroupName"] == subnet_group
        assert grp["DBSubnetGroupDescription"] == "compat test subnet group"
        assert len(grp["Subnets"]) == 2

    def test_delete_subnet_group(self, rds, ec2):
        name = _unique("compat-sg-del")
        vpc = ec2.create_vpc(CidrBlock="10.77.0.0/16")
        vpc_id = vpc["Vpc"]["VpcId"]
        s1 = ec2.create_subnet(
            VpcId=vpc_id, CidrBlock="10.77.1.0/24", AvailabilityZone="us-east-1a"
        )
        s2 = ec2.create_subnet(
            VpcId=vpc_id, CidrBlock="10.77.2.0/24", AvailabilityZone="us-east-1b"
        )
        rds.create_db_subnet_group(
            DBSubnetGroupName=name,
            DBSubnetGroupDescription="to delete",
            SubnetIds=[s1["Subnet"]["SubnetId"], s2["Subnet"]["SubnetId"]],
        )
        rds.delete_db_subnet_group(DBSubnetGroupName=name)
        with pytest.raises(ClientError) as exc:
            rds.describe_db_subnet_groups(DBSubnetGroupName=name)
        assert exc.value.response["Error"]["Code"] == "DBSubnetGroupNotFoundFault"
        # Cleanup EC2 resources
        for sid in [s1["Subnet"]["SubnetId"], s2["Subnet"]["SubnetId"]]:
            try:
                ec2.delete_subnet(SubnetId=sid)
            except ClientError:
                pass
        try:
            ec2.delete_vpc(VpcId=vpc_id)
        except ClientError:
            pass


class TestRDSParameterGroupOperations:
    def test_create_and_describe_parameter_group(self, rds, param_group):
        resp = rds.describe_db_parameter_groups(DBParameterGroupName=param_group)
        groups = resp["DBParameterGroups"]
        assert len(groups) == 1
        grp = groups[0]
        assert grp["DBParameterGroupName"] == param_group
        assert grp["DBParameterGroupFamily"] == "mysql8.0"
        assert grp["Description"] == "compat test parameter group"

    def test_describe_db_parameters(self, rds, param_group):
        resp = rds.describe_db_parameters(DBParameterGroupName=param_group)
        # Should return a Parameters list (may be empty for a new group)
        assert "Parameters" in resp

    def test_delete_parameter_group(self, rds):
        name = _unique("compat-pg-del")
        rds.create_db_parameter_group(
            DBParameterGroupName=name,
            DBParameterGroupFamily="mysql8.0",
            Description="to delete",
        )
        rds.delete_db_parameter_group(DBParameterGroupName=name)
        # After deletion, describe returns empty list
        resp = rds.describe_db_parameter_groups(DBParameterGroupName=name)
        assert resp["DBParameterGroups"] == []


class TestRDSSnapshotOperations:
    def test_create_and_describe_snapshot(self, rds, db_instance):
        snap_id = _unique("compat-snap")
        rds.create_db_snapshot(
            DBSnapshotIdentifier=snap_id,
            DBInstanceIdentifier=db_instance,
        )
        resp = rds.describe_db_snapshots(DBSnapshotIdentifier=snap_id)
        snaps = resp["DBSnapshots"]
        assert len(snaps) == 1
        snap = snaps[0]
        assert snap["DBSnapshotIdentifier"] == snap_id
        assert snap["DBInstanceIdentifier"] == db_instance
        assert snap["Engine"] == "mysql"
        # Cleanup
        rds.delete_db_snapshot(DBSnapshotIdentifier=snap_id)

    def test_delete_snapshot(self, rds, db_instance):
        snap_id = _unique("compat-snap-del")
        rds.create_db_snapshot(
            DBSnapshotIdentifier=snap_id,
            DBInstanceIdentifier=db_instance,
        )
        rds.delete_db_snapshot(DBSnapshotIdentifier=snap_id)
        with pytest.raises(ClientError) as exc:
            rds.describe_db_snapshots(DBSnapshotIdentifier=snap_id)
        assert "NotFound" in exc.value.response["Error"]["Code"]


class TestRDSDescribeOperations:
    def test_describe_events(self, rds):
        resp = rds.describe_events()
        assert "Events" in resp

    def test_describe_orderable_db_instance_options(self, rds):
        resp = rds.describe_orderable_db_instance_options(Engine="mysql", MaxRecords=20)
        assert "OrderableDBInstanceOptions" in resp


class TestRdsAutoCoverage:
    """Auto-generated coverage tests for rds."""

    @pytest.fixture
    def client(self):
        return make_client("rds")

    def test_describe_blue_green_deployments(self, client):
        """DescribeBlueGreenDeployments returns a response."""
        resp = client.describe_blue_green_deployments()
        assert "BlueGreenDeployments" in resp

    def test_describe_db_cluster_parameter_groups(self, client):
        """DescribeDBClusterParameterGroups returns a response."""
        resp = client.describe_db_cluster_parameter_groups()
        assert "DBClusterParameterGroups" in resp

    def test_describe_db_cluster_snapshots(self, client):
        """DescribeDBClusterSnapshots returns a response."""
        resp = client.describe_db_cluster_snapshots()
        assert "DBClusterSnapshots" in resp

    def test_describe_db_clusters(self, client):
        """DescribeDBClusters returns a response."""
        resp = client.describe_db_clusters()
        assert "DBClusters" in resp

    def test_describe_db_instance_automated_backups(self, client):
        """DescribeDBInstanceAutomatedBackups returns a response."""
        resp = client.describe_db_instance_automated_backups()
        assert "DBInstanceAutomatedBackups" in resp

    def test_describe_db_instances(self, client):
        """DescribeDBInstances returns a response."""
        resp = client.describe_db_instances()
        assert "DBInstances" in resp

    def test_describe_db_parameter_groups(self, client):
        """DescribeDBParameterGroups returns a response."""
        resp = client.describe_db_parameter_groups()
        assert "DBParameterGroups" in resp

    def test_describe_db_proxies(self, client):
        """DescribeDBProxies returns a response."""
        resp = client.describe_db_proxies()
        assert "DBProxies" in resp

    def test_describe_db_security_groups(self, client):
        """DescribeDBSecurityGroups returns a response."""
        resp = client.describe_db_security_groups()
        assert "DBSecurityGroups" in resp

    def test_describe_db_shard_groups(self, client):
        """DescribeDBShardGroups returns a response."""
        resp = client.describe_db_shard_groups()
        assert "DBShardGroups" in resp

    def test_describe_db_snapshots(self, client):
        """DescribeDBSnapshots returns a response."""
        resp = client.describe_db_snapshots()
        assert "DBSnapshots" in resp

    def test_describe_db_subnet_groups(self, client):
        """DescribeDBSubnetGroups returns a response."""
        resp = client.describe_db_subnet_groups()
        assert "DBSubnetGroups" in resp

    def test_describe_event_subscriptions(self, client):
        """DescribeEventSubscriptions returns a response."""
        resp = client.describe_event_subscriptions()
        assert "EventSubscriptionsList" in resp

    def test_describe_export_tasks(self, client):
        """DescribeExportTasks returns a response."""
        resp = client.describe_export_tasks()
        assert "ExportTasks" in resp

    def test_describe_global_clusters(self, client):
        """DescribeGlobalClusters returns a response."""
        resp = client.describe_global_clusters()
        assert "GlobalClusters" in resp


class TestRDSDBClusterParameterGroupOperations:
    @pytest.fixture
    def client(self):
        return make_client("rds")

    def test_create_db_cluster_parameter_group(self, client):
        name = _unique("compat-cpg")
        try:
            resp = client.create_db_cluster_parameter_group(
                DBClusterParameterGroupName=name,
                DBParameterGroupFamily="aurora-mysql8.0",
                Description="compat test cluster param group",
            )
            assert resp["DBClusterParameterGroup"]["DBClusterParameterGroupName"] == name
        finally:
            try:
                client.delete_db_cluster_parameter_group(DBClusterParameterGroupName=name)
            except ClientError:
                pass

    def test_modify_db_cluster_parameter_group(self, client):
        name = _unique("compat-cpg")
        client.create_db_cluster_parameter_group(
            DBClusterParameterGroupName=name,
            DBParameterGroupFamily="aurora-mysql8.0",
            Description="compat test",
        )
        try:
            resp = client.modify_db_cluster_parameter_group(
                DBClusterParameterGroupName=name,
                Parameters=[
                    {
                        "ParameterName": "character_set_server",
                        "ParameterValue": "utf8mb4",
                        "ApplyMethod": "pending-reboot",
                    }
                ],
            )
            assert resp["DBClusterParameterGroupName"] == name
        finally:
            try:
                client.delete_db_cluster_parameter_group(DBClusterParameterGroupName=name)
            except ClientError:
                pass

    def test_delete_db_cluster_parameter_group(self, client):
        name = _unique("compat-cpg")
        client.create_db_cluster_parameter_group(
            DBClusterParameterGroupName=name,
            DBParameterGroupFamily="aurora-mysql8.0",
            Description="to delete",
        )
        client.delete_db_cluster_parameter_group(DBClusterParameterGroupName=name)
        # Verify it's gone - describe should return empty or error
        resp = client.describe_db_cluster_parameter_groups()
        names = [g["DBClusterParameterGroupName"] for g in resp["DBClusterParameterGroups"]]
        assert name not in names

    def test_copy_db_cluster_parameter_group(self, client):
        src = _unique("compat-cpg-src")
        tgt = _unique("compat-cpg-tgt")
        client.create_db_cluster_parameter_group(
            DBClusterParameterGroupName=src,
            DBParameterGroupFamily="aurora-mysql8.0",
            Description="source group",
        )
        try:
            resp = client.copy_db_cluster_parameter_group(
                SourceDBClusterParameterGroupIdentifier=src,
                TargetDBClusterParameterGroupIdentifier=tgt,
                TargetDBClusterParameterGroupDescription="copied group",
            )
            assert resp["DBClusterParameterGroup"]["DBClusterParameterGroupName"] == tgt
        finally:
            for n in [src, tgt]:
                try:
                    client.delete_db_cluster_parameter_group(DBClusterParameterGroupName=n)
                except ClientError:
                    pass

    def test_describe_db_cluster_parameters(self, client):
        name = _unique("compat-cpg")
        client.create_db_cluster_parameter_group(
            DBClusterParameterGroupName=name,
            DBParameterGroupFamily="aurora-mysql8.0",
            Description="compat test",
        )
        try:
            resp = client.describe_db_cluster_parameters(DBClusterParameterGroupName=name)
            assert "Parameters" in resp
        finally:
            try:
                client.delete_db_cluster_parameter_group(DBClusterParameterGroupName=name)
            except ClientError:
                pass


class TestRDSDBParameterGroupCRUDOperations:
    @pytest.fixture
    def client(self):
        return make_client("rds")

    def test_copy_db_parameter_group(self, client):
        src = _unique("compat-pg-src")
        tgt = _unique("compat-pg-tgt")
        client.create_db_parameter_group(
            DBParameterGroupName=src,
            DBParameterGroupFamily="mysql8.0",
            Description="source group",
        )
        try:
            resp = client.copy_db_parameter_group(
                SourceDBParameterGroupIdentifier=src,
                TargetDBParameterGroupIdentifier=tgt,
                TargetDBParameterGroupDescription="copied group",
            )
            assert resp["DBParameterGroup"]["DBParameterGroupName"] == tgt
        finally:
            for n in [src, tgt]:
                try:
                    client.delete_db_parameter_group(DBParameterGroupName=n)
                except ClientError:
                    pass

    def test_modify_db_parameter_group(self, client):
        name = _unique("compat-pg")
        client.create_db_parameter_group(
            DBParameterGroupName=name,
            DBParameterGroupFamily="mysql8.0",
            Description="compat test",
        )
        try:
            resp = client.modify_db_parameter_group(
                DBParameterGroupName=name,
                Parameters=[
                    {
                        "ParameterName": "max_connections",
                        "ParameterValue": "200",
                        "ApplyMethod": "pending-reboot",
                    }
                ],
            )
            assert resp["DBParameterGroupName"] == name
        finally:
            try:
                client.delete_db_parameter_group(DBParameterGroupName=name)
            except ClientError:
                pass


class TestRDSDBClusterOperations:
    @pytest.fixture
    def client(self):
        return make_client("rds")

    def test_create_db_cluster(self, client):
        name = _unique("compat-cl")
        try:
            resp = client.create_db_cluster(
                DBClusterIdentifier=name,
                Engine="aurora-mysql",
                MasterUsername="admin",
                MasterUserPassword="password123!",
            )
            assert resp["DBCluster"]["DBClusterIdentifier"] == name
            assert resp["DBCluster"]["Engine"] == "aurora-mysql"
        finally:
            try:
                client.delete_db_cluster(DBClusterIdentifier=name, SkipFinalSnapshot=True)
            except ClientError:
                pass

    def test_stop_db_cluster(self, client):
        name = _unique("compat-cl")
        client.create_db_cluster(
            DBClusterIdentifier=name,
            Engine="aurora-mysql",
            MasterUsername="admin",
            MasterUserPassword="password123!",
        )
        try:
            resp = client.stop_db_cluster(DBClusterIdentifier=name)
            assert resp["DBCluster"]["DBClusterIdentifier"] == name
        finally:
            try:
                client.delete_db_cluster(DBClusterIdentifier=name, SkipFinalSnapshot=True)
            except ClientError:
                pass

    def test_start_db_cluster(self, client):
        name = _unique("compat-cl")
        client.create_db_cluster(
            DBClusterIdentifier=name,
            Engine="aurora-mysql",
            MasterUsername="admin",
            MasterUserPassword="password123!",
        )
        try:
            client.stop_db_cluster(DBClusterIdentifier=name)
            resp = client.start_db_cluster(DBClusterIdentifier=name)
            assert resp["DBCluster"]["DBClusterIdentifier"] == name
        finally:
            try:
                client.delete_db_cluster(DBClusterIdentifier=name, SkipFinalSnapshot=True)
            except ClientError:
                pass


class TestRDSDBClusterSnapshotOperations:
    @pytest.fixture
    def client(self):
        return make_client("rds")

    @pytest.fixture
    def cluster(self, client):
        name = _unique("compat-cl")
        client.create_db_cluster(
            DBClusterIdentifier=name,
            Engine="aurora-mysql",
            MasterUsername="admin",
            MasterUserPassword="password123!",
        )
        yield name
        try:
            client.delete_db_cluster(DBClusterIdentifier=name, SkipFinalSnapshot=True)
        except ClientError:
            pass

    def test_create_db_cluster_snapshot(self, client, cluster):
        snap = _unique("compat-csnap")
        try:
            resp = client.create_db_cluster_snapshot(
                DBClusterSnapshotIdentifier=snap,
                DBClusterIdentifier=cluster,
            )
            assert resp["DBClusterSnapshot"]["DBClusterSnapshotIdentifier"] == snap
        finally:
            try:
                client.delete_db_cluster_snapshot(DBClusterSnapshotIdentifier=snap)
            except ClientError:
                pass

    def test_delete_db_cluster_snapshot(self, client, cluster):
        snap = _unique("compat-csnap")
        client.create_db_cluster_snapshot(
            DBClusterSnapshotIdentifier=snap,
            DBClusterIdentifier=cluster,
        )
        resp = client.delete_db_cluster_snapshot(DBClusterSnapshotIdentifier=snap)
        assert "DBClusterSnapshot" in resp

    def test_copy_db_cluster_snapshot(self, client, cluster):
        src = _unique("compat-csnap-src")
        tgt = _unique("compat-csnap-tgt")
        client.create_db_cluster_snapshot(
            DBClusterSnapshotIdentifier=src,
            DBClusterIdentifier=cluster,
        )
        try:
            resp = client.copy_db_cluster_snapshot(
                SourceDBClusterSnapshotIdentifier=src,
                TargetDBClusterSnapshotIdentifier=tgt,
            )
            assert resp["DBClusterSnapshot"]["DBClusterSnapshotIdentifier"] == tgt
        finally:
            for s in [src, tgt]:
                try:
                    client.delete_db_cluster_snapshot(DBClusterSnapshotIdentifier=s)
                except ClientError:
                    pass

    def test_describe_db_cluster_snapshot_attributes(self, client, cluster):
        snap = _unique("compat-csnap")
        client.create_db_cluster_snapshot(
            DBClusterSnapshotIdentifier=snap,
            DBClusterIdentifier=cluster,
        )
        try:
            resp = client.describe_db_cluster_snapshot_attributes(DBClusterSnapshotIdentifier=snap)
            assert "DBClusterSnapshotAttributesResult" in resp
        finally:
            try:
                client.delete_db_cluster_snapshot(DBClusterSnapshotIdentifier=snap)
            except ClientError:
                pass

    def test_modify_db_cluster_snapshot_attribute(self, client, cluster):
        snap = _unique("compat-csnap")
        client.create_db_cluster_snapshot(
            DBClusterSnapshotIdentifier=snap,
            DBClusterIdentifier=cluster,
        )
        try:
            resp = client.modify_db_cluster_snapshot_attribute(
                DBClusterSnapshotIdentifier=snap,
                AttributeName="restore",
                ValuesToAdd=["all"],
            )
            assert "DBClusterSnapshotAttributesResult" in resp
        finally:
            try:
                client.delete_db_cluster_snapshot(DBClusterSnapshotIdentifier=snap)
            except ClientError:
                pass


class TestRDSDBSnapshotCRUDOperations:
    @pytest.fixture
    def client(self):
        return make_client("rds")

    @pytest.fixture
    def instance(self, client):
        name = _unique("compat-db")
        client.create_db_instance(
            DBInstanceIdentifier=name,
            DBInstanceClass="db.t3.micro",
            Engine="mysql",
            MasterUsername="admin",
            MasterUserPassword="password123",
        )
        yield name
        try:
            client.delete_db_instance(DBInstanceIdentifier=name, SkipFinalSnapshot=True)
        except ClientError:
            pass

    def test_copy_db_snapshot(self, client, instance):
        src = _unique("compat-snap-src")
        tgt = _unique("compat-snap-tgt")
        client.create_db_snapshot(
            DBSnapshotIdentifier=src,
            DBInstanceIdentifier=instance,
        )
        try:
            resp = client.copy_db_snapshot(
                SourceDBSnapshotIdentifier=src,
                TargetDBSnapshotIdentifier=tgt,
            )
            assert resp["DBSnapshot"]["DBSnapshotIdentifier"] == tgt
        finally:
            for s in [src, tgt]:
                try:
                    client.delete_db_snapshot(DBSnapshotIdentifier=s)
                except ClientError:
                    pass

    def test_describe_db_snapshot_attributes(self, client, instance):
        snap = _unique("compat-snap")
        client.create_db_snapshot(
            DBSnapshotIdentifier=snap,
            DBInstanceIdentifier=instance,
        )
        try:
            resp = client.describe_db_snapshot_attributes(DBSnapshotIdentifier=snap)
            assert "DBSnapshotAttributesResult" in resp
        finally:
            try:
                client.delete_db_snapshot(DBSnapshotIdentifier=snap)
            except ClientError:
                pass

    def test_modify_db_snapshot_attribute(self, client, instance):
        snap = _unique("compat-snap")
        client.create_db_snapshot(
            DBSnapshotIdentifier=snap,
            DBInstanceIdentifier=instance,
        )
        try:
            resp = client.modify_db_snapshot_attribute(
                DBSnapshotIdentifier=snap,
                AttributeName="restore",
                ValuesToAdd=["all"],
            )
            assert "DBSnapshotAttributesResult" in resp
        finally:
            try:
                client.delete_db_snapshot(DBSnapshotIdentifier=snap)
            except ClientError:
                pass


class TestRDSOptionGroupOperations:
    @pytest.fixture
    def client(self):
        return make_client("rds")

    def test_create_option_group(self, client):
        name = _unique("compat-og")
        try:
            resp = client.create_option_group(
                OptionGroupName=name,
                EngineName="mysql",
                MajorEngineVersion="8.0",
                OptionGroupDescription="compat test option group",
            )
            assert resp["OptionGroup"]["OptionGroupName"] == name
        finally:
            try:
                client.delete_option_group(OptionGroupName=name)
            except ClientError:
                pass

    def test_delete_option_group(self, client):
        name = _unique("compat-og")
        client.create_option_group(
            OptionGroupName=name,
            EngineName="mysql",
            MajorEngineVersion="8.0",
            OptionGroupDescription="to delete",
        )
        client.delete_option_group(OptionGroupName=name)
        # Verify deletion by trying to describe the specific group
        with pytest.raises(ClientError) as exc:
            client.describe_option_groups(OptionGroupName=name)
        assert exc.value.response["Error"]["Code"] in (
            "OptionGroupNotFoundFault",
            "InternalError",
        )

    def test_describe_option_group_options(self, client):
        resp = client.describe_option_group_options(EngineName="mysql")
        assert "OptionGroupOptions" in resp


class TestRDSDBSecurityGroupOperations:
    @pytest.fixture
    def client(self):
        return make_client("rds")

    def test_create_db_security_group(self, client):
        name = _unique("compat-dbsg")
        try:
            resp = client.create_db_security_group(
                DBSecurityGroupName=name,
                DBSecurityGroupDescription="compat test security group",
            )
            assert resp["DBSecurityGroup"]["DBSecurityGroupName"] == name
        finally:
            try:
                client.delete_db_security_group(DBSecurityGroupName=name)
            except ClientError:
                pass

    def test_delete_db_security_group(self, client):
        name = _unique("compat-dbsg")
        client.create_db_security_group(
            DBSecurityGroupName=name,
            DBSecurityGroupDescription="to delete",
        )
        client.delete_db_security_group(DBSecurityGroupName=name)
        resp = client.describe_db_security_groups()
        names = [g["DBSecurityGroupName"] for g in resp["DBSecurityGroups"]]
        assert name not in names

    def test_authorize_db_security_group_ingress(self, client):
        name = _unique("compat-dbsg")
        client.create_db_security_group(
            DBSecurityGroupName=name,
            DBSecurityGroupDescription="compat test",
        )
        try:
            resp = client.authorize_db_security_group_ingress(
                DBSecurityGroupName=name,
                CIDRIP="10.0.0.0/24",
            )
            assert resp["DBSecurityGroup"]["DBSecurityGroupName"] == name
        finally:
            try:
                client.delete_db_security_group(DBSecurityGroupName=name)
            except ClientError:
                pass


class TestRDSEventSubscriptionOperations:
    @pytest.fixture
    def client(self):
        return make_client("rds")

    def test_create_event_subscription(self, client):
        name = _unique("compat-esub")
        try:
            resp = client.create_event_subscription(
                SubscriptionName=name,
                SnsTopicArn="arn:aws:sns:us-east-1:123456789012:test-topic",
            )
            assert resp["EventSubscription"]["CustSubscriptionId"] == name
        finally:
            try:
                client.delete_event_subscription(SubscriptionName=name)
            except ClientError:
                pass

    def test_delete_event_subscription(self, client):
        name = _unique("compat-esub")
        client.create_event_subscription(
            SubscriptionName=name,
            SnsTopicArn="arn:aws:sns:us-east-1:123456789012:test-topic",
        )
        resp = client.delete_event_subscription(SubscriptionName=name)
        assert "EventSubscription" in resp


class TestRDSGlobalClusterOperations:
    @pytest.fixture
    def client(self):
        return make_client("rds")

    def test_create_global_cluster(self, client):
        name = _unique("compat-gc")
        try:
            resp = client.create_global_cluster(
                GlobalClusterIdentifier=name,
                Engine="aurora-mysql",
            )
            assert resp["GlobalCluster"]["GlobalClusterIdentifier"] == name
        finally:
            try:
                client.delete_global_cluster(GlobalClusterIdentifier=name)
            except ClientError:
                pass


class TestRDSDBLogFiles:
    @pytest.fixture
    def client(self):
        return make_client("rds")

    def test_describe_db_log_files(self, client):
        name = _unique("compat-db")
        client.create_db_instance(
            DBInstanceIdentifier=name,
            DBInstanceClass="db.t3.micro",
            Engine="mysql",
            MasterUsername="admin",
            MasterUserPassword="password123",
        )
        try:
            resp = client.describe_db_log_files(DBInstanceIdentifier=name)
            assert "DescribeDBLogFiles" in resp
        finally:
            try:
                client.delete_db_instance(DBInstanceIdentifier=name, SkipFinalSnapshot=True)
            except ClientError:
                pass


class TestRDSRoleOperations:
    @pytest.fixture
    def client(self):
        return make_client("rds")

    def test_add_role_to_db_instance(self, client):
        name = _unique("compat-db")
        client.create_db_instance(
            DBInstanceIdentifier=name,
            DBInstanceClass="db.t3.micro",
            Engine="mysql",
            MasterUsername="admin",
            MasterUserPassword="password123",
        )
        try:
            resp = client.add_role_to_db_instance(
                DBInstanceIdentifier=name,
                RoleArn="arn:aws:iam::123456789012:role/test-role",
                FeatureName="s3Import",
            )
            assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200
        finally:
            try:
                client.delete_db_instance(DBInstanceIdentifier=name, SkipFinalSnapshot=True)
            except ClientError:
                pass

    def test_add_role_to_db_cluster(self, client):
        name = _unique("compat-cl")
        client.create_db_cluster(
            DBClusterIdentifier=name,
            Engine="aurora-mysql",
            MasterUsername="admin",
            MasterUserPassword="password123!",
        )
        try:
            resp = client.add_role_to_db_cluster(
                DBClusterIdentifier=name,
                RoleArn="arn:aws:iam::123456789012:role/test-role",
            )
            assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200
        finally:
            try:
                client.delete_db_cluster(DBClusterIdentifier=name, SkipFinalSnapshot=True)
            except ClientError:
                pass


class TestRDSBlueGreenDeploymentOperations:
    @pytest.fixture
    def client(self):
        return make_client("rds")

    @pytest.fixture
    def source_instance(self, client):
        name = _unique("compat-bg-src")
        client.create_db_instance(
            DBInstanceIdentifier=name,
            DBInstanceClass="db.t3.micro",
            Engine="mysql",
            MasterUsername="admin",
            MasterUserPassword="password123",
        )
        yield name
        try:
            client.delete_db_instance(DBInstanceIdentifier=name, SkipFinalSnapshot=True)
        except ClientError:
            pass

    def test_create_blue_green_deployment(self, client, source_instance):
        bg_name = _unique("compat-bg")
        source_arn = f"arn:aws:rds:us-east-1:123456789012:db:{source_instance}"
        try:
            resp = client.create_blue_green_deployment(
                BlueGreenDeploymentName=bg_name,
                Source=source_arn,
            )
            assert "BlueGreenDeployment" in resp
            assert resp["BlueGreenDeployment"]["BlueGreenDeploymentName"] == bg_name
            bg_id = resp["BlueGreenDeployment"]["BlueGreenDeploymentIdentifier"]
        finally:
            try:
                client.delete_blue_green_deployment(BlueGreenDeploymentIdentifier=bg_id)
            except Exception:
                pass

    def test_delete_blue_green_deployment(self, client, source_instance):
        bg_name = _unique("compat-bg")
        source_arn = f"arn:aws:rds:us-east-1:123456789012:db:{source_instance}"
        resp = client.create_blue_green_deployment(
            BlueGreenDeploymentName=bg_name,
            Source=source_arn,
        )
        bg_id = resp["BlueGreenDeployment"]["BlueGreenDeploymentIdentifier"]
        del_resp = client.delete_blue_green_deployment(
            BlueGreenDeploymentIdentifier=bg_id,
        )
        assert "BlueGreenDeployment" in del_resp


class TestRDSRestoreOperations:
    @pytest.fixture
    def client(self):
        return make_client("rds")

    def test_restore_db_cluster_from_snapshot(self, client):
        cluster_name = _unique("compat-cl")
        snap_name = _unique("compat-csnap")
        restored_name = _unique("compat-restored")
        client.create_db_cluster(
            DBClusterIdentifier=cluster_name,
            Engine="aurora-mysql",
            MasterUsername="admin",
            MasterUserPassword="password123!",
        )
        try:
            client.create_db_cluster_snapshot(
                DBClusterSnapshotIdentifier=snap_name,
                DBClusterIdentifier=cluster_name,
            )
            try:
                resp = client.restore_db_cluster_from_snapshot(
                    DBClusterIdentifier=restored_name,
                    SnapshotIdentifier=snap_name,
                    Engine="aurora-mysql",
                )
                assert resp["DBCluster"]["DBClusterIdentifier"] == restored_name
            finally:
                try:
                    client.delete_db_cluster(
                        DBClusterIdentifier=restored_name, SkipFinalSnapshot=True
                    )
                except ClientError:
                    pass
                try:
                    client.delete_db_cluster_snapshot(DBClusterSnapshotIdentifier=snap_name)
                except ClientError:
                    pass
        finally:
            try:
                client.delete_db_cluster(DBClusterIdentifier=cluster_name, SkipFinalSnapshot=True)
            except ClientError:
                pass


class TestRDSModifyOptionGroupOperations:
    @pytest.fixture
    def client(self):
        return make_client("rds")

    def test_modify_option_group(self, client):
        name = _unique("compat-og")
        client.create_option_group(
            OptionGroupName=name,
            EngineName="mysql",
            MajorEngineVersion="8.0",
            OptionGroupDescription="compat test",
        )
        try:
            # Empty options lists should return InvalidParameterValue
            with pytest.raises(ClientError) as exc:
                client.modify_option_group(
                    OptionGroupName=name,
                    OptionsToInclude=[],
                    ApplyImmediately=True,
                )
            assert exc.value.response["Error"]["Code"] == "InvalidParameterValue"
        finally:
            try:
                client.delete_option_group(OptionGroupName=name)
            except ClientError:
                pass


class TestRDSExportTaskOperations:
    @pytest.fixture
    def client(self):
        return make_client("rds")

    def test_cancel_export_task_nonexistent(self, client):
        """CancelExportTask returns error for nonexistent task."""
        with pytest.raises(ClientError) as exc:
            client.cancel_export_task(ExportTaskIdentifier="does-not-exist")
        assert exc.value.response["Error"]["Code"] in (
            "ExportTaskNotFoundFault",
            "ExportTaskNotFound",
        )


class TestRDSGlobalClusterMemberOperations:
    @pytest.fixture
    def client(self):
        return make_client("rds")

    def test_remove_from_global_cluster(self, client):
        """RemoveFromGlobalCluster on a global cluster with a member."""
        gc_name = _unique("compat-gc")
        cl_name = _unique("compat-cl")
        client.create_global_cluster(
            GlobalClusterIdentifier=gc_name,
            Engine="aurora-mysql",
        )
        try:
            client.create_db_cluster(
                DBClusterIdentifier=cl_name,
                Engine="aurora-mysql",
                MasterUsername="admin",
                MasterUserPassword="password123!",
                GlobalClusterIdentifier=gc_name,
            )
            try:
                resp = client.remove_from_global_cluster(
                    GlobalClusterIdentifier=gc_name,
                    DbClusterIdentifier=f"arn:aws:rds:us-east-1:123456789012:cluster:{cl_name}",
                )
                assert "GlobalCluster" in resp
            finally:
                try:
                    client.delete_db_cluster(DBClusterIdentifier=cl_name, SkipFinalSnapshot=True)
                except ClientError:
                    pass
        finally:
            try:
                client.delete_global_cluster(GlobalClusterIdentifier=gc_name)
            except ClientError:
                pass


class TestRDSSwitchoverOperations:
    @pytest.fixture
    def client(self):
        return make_client("rds")

    def test_switchover_blue_green_nonexistent(self, client):
        """SwitchoverBlueGreenDeployment returns error for nonexistent deployment."""
        with pytest.raises(ClientError) as exc:
            client.switchover_blue_green_deployment(
                BlueGreenDeploymentIdentifier="bgd-does-not-exist",
            )
        assert exc.value.response["Error"]["Code"] in (
            "BlueGreenDeploymentNotFoundFault",
            "BlueGreenDeploymentNotFound",
        )


class TestRDSFailoverOperations:
    @pytest.fixture
    def client(self):
        return make_client("rds")

    def test_failover_nonexistent_db_cluster(self, client):
        """FailoverDBCluster returns error for nonexistent cluster."""
        with pytest.raises(ClientError) as exc:
            client.failover_db_cluster(DBClusterIdentifier="does-not-exist")
        assert exc.value.response["Error"]["Code"] == "DBClusterNotFoundFault"


class TestRDSDBProxyCRUD:
    @pytest.fixture
    def client(self):
        return make_client("rds")

    @pytest.fixture
    def ec2_client(self):
        return make_client("ec2")

    @pytest.fixture
    def vpc_subnets(self, ec2_client):
        """Create a VPC with two subnets for DB proxy."""
        vpc = ec2_client.create_vpc(CidrBlock="10.97.0.0/16")
        vpc_id = vpc["Vpc"]["VpcId"]
        s1 = ec2_client.create_subnet(
            VpcId=vpc_id, CidrBlock="10.97.1.0/24", AvailabilityZone="us-east-1a"
        )
        s2 = ec2_client.create_subnet(
            VpcId=vpc_id, CidrBlock="10.97.2.0/24", AvailabilityZone="us-east-1b"
        )
        subnet_ids = [s1["Subnet"]["SubnetId"], s2["Subnet"]["SubnetId"]]
        yield subnet_ids
        for sid in subnet_ids:
            try:
                ec2_client.delete_subnet(SubnetId=sid)
            except ClientError:
                pass
        try:
            ec2_client.delete_vpc(VpcId=vpc_id)
        except ClientError:
            pass

    def test_create_describe_delete_db_proxy(self, client, vpc_subnets):
        """Full CRUD lifecycle for DBProxy."""
        name = _unique("compat-px")
        try:
            resp = client.create_db_proxy(
                DBProxyName=name,
                EngineFamily="MYSQL",
                Auth=[
                    {
                        "AuthScheme": "SECRETS",
                        "SecretArn": "arn:aws:secretsmanager:us-east-1:123456789012:secret:test",
                        "IAMAuth": "DISABLED",
                    }
                ],
                RoleArn="arn:aws:iam::123456789012:role/test-role",
                VpcSubnetIds=vpc_subnets,
            )
            assert resp["DBProxy"]["DBProxyName"] == name
            assert resp["DBProxy"]["EngineFamily"] == "MYSQL"

            # Describe specific proxy
            desc = client.describe_db_proxies(DBProxyName=name)
            assert len(desc["DBProxies"]) == 1
            assert desc["DBProxies"][0]["DBProxyName"] == name

            # Delete
            client.delete_db_proxy(DBProxyName=name)

            # Verify deletion
            desc2 = client.describe_db_proxies()
            names = [p["DBProxyName"] for p in desc2["DBProxies"]]
            assert name not in names
        except Exception:
            try:
                client.delete_db_proxy(DBProxyName=name)
            except ClientError:
                pass
            raise


class TestRDSModifyDBClusterOperations:
    @pytest.fixture
    def client(self):
        return make_client("rds")

    def test_modify_db_cluster(self, client):
        """ModifyDBCluster changes cluster settings."""
        name = _unique("compat-cl")
        client.create_db_cluster(
            DBClusterIdentifier=name,
            Engine="aurora-mysql",
            MasterUsername="admin",
            MasterUserPassword="password123!",
        )
        try:
            resp = client.modify_db_cluster(
                DBClusterIdentifier=name,
                DeletionProtection=False,
            )
            assert resp["DBCluster"]["DBClusterIdentifier"] == name
            assert resp["DBCluster"]["Engine"] == "aurora-mysql"
        finally:
            try:
                client.delete_db_cluster(DBClusterIdentifier=name, SkipFinalSnapshot=True)
            except ClientError:
                pass


class TestRDSModifyDBSubnetGroupOperations:
    @pytest.fixture
    def client(self):
        return make_client("rds")

    @pytest.fixture
    def ec2_client(self):
        return make_client("ec2")

    def test_modify_db_subnet_group(self, client, ec2_client):
        """ModifyDBSubnetGroup updates description and subnets."""
        vpc = ec2_client.create_vpc(CidrBlock="10.96.0.0/16")
        vpc_id = vpc["Vpc"]["VpcId"]
        s1 = ec2_client.create_subnet(
            VpcId=vpc_id, CidrBlock="10.96.1.0/24", AvailabilityZone="us-east-1a"
        )
        s2 = ec2_client.create_subnet(
            VpcId=vpc_id, CidrBlock="10.96.2.0/24", AvailabilityZone="us-east-1b"
        )
        s3 = ec2_client.create_subnet(
            VpcId=vpc_id, CidrBlock="10.96.3.0/24", AvailabilityZone="us-east-1c"
        )
        sid1 = s1["Subnet"]["SubnetId"]
        sid2 = s2["Subnet"]["SubnetId"]
        sid3 = s3["Subnet"]["SubnetId"]
        name = _unique("compat-sg")
        client.create_db_subnet_group(
            DBSubnetGroupName=name,
            DBSubnetGroupDescription="original",
            SubnetIds=[sid1, sid2],
        )
        try:
            resp = client.modify_db_subnet_group(
                DBSubnetGroupName=name,
                DBSubnetGroupDescription="modified desc",
                SubnetIds=[sid1, sid2, sid3],
            )
            grp = resp["DBSubnetGroup"]
            assert grp["DBSubnetGroupName"] == name
            assert grp["DBSubnetGroupDescription"] == "modified desc"
            assert len(grp["Subnets"]) == 3
        finally:
            try:
                client.delete_db_subnet_group(DBSubnetGroupName=name)
            except ClientError:
                pass


class TestRDSGlobalClusterCRUD:
    @pytest.fixture
    def client(self):
        return make_client("rds")

    def test_create_describe_delete_global_cluster(self, client):
        """Full CRUD lifecycle for GlobalCluster."""
        name = _unique("compat-gc")
        resp = client.create_global_cluster(
            GlobalClusterIdentifier=name,
            Engine="aurora-mysql",
        )
        assert resp["GlobalCluster"]["GlobalClusterIdentifier"] == name
        assert resp["GlobalCluster"]["Engine"] == "aurora-mysql"

        # Describe specific cluster
        desc = client.describe_global_clusters(GlobalClusterIdentifier=name)
        matching = [g for g in desc["GlobalClusters"] if g["GlobalClusterIdentifier"] == name]
        assert len(matching) == 1
        assert matching[0]["GlobalClusterIdentifier"] == name

        # Delete
        del_resp = client.delete_global_cluster(GlobalClusterIdentifier=name)
        assert "GlobalCluster" in del_resp

        # Verify gone
        desc2 = client.describe_global_clusters()
        names = [g["GlobalClusterIdentifier"] for g in desc2["GlobalClusters"]]
        assert name not in names


class TestRDSReadReplicaOperations:
    @pytest.fixture
    def client(self):
        return make_client("rds")

    @pytest.fixture
    def source_instance(self, client):
        name = _unique("compat-src")
        client.create_db_instance(
            DBInstanceIdentifier=name,
            DBInstanceClass="db.t3.micro",
            Engine="mysql",
            MasterUsername="admin",
            MasterUserPassword="password123",
        )
        yield name
        try:
            client.delete_db_instance(DBInstanceIdentifier=name, SkipFinalSnapshot=True)
        except ClientError:
            pass

    def test_create_read_replica(self, client, source_instance):
        """CreateDBInstanceReadReplica creates a replica and it appears in DescribeDBInstances."""
        rr_name = _unique("compat-rr")
        try:
            resp = client.create_db_instance_read_replica(
                DBInstanceIdentifier=rr_name,
                SourceDBInstanceIdentifier=source_instance,
            )
            assert resp["DBInstance"]["DBInstanceIdentifier"] == rr_name
            assert resp["DBInstance"]["Engine"] == "mysql"

            # Verify in describe
            desc = client.describe_db_instances(DBInstanceIdentifier=rr_name)
            assert len(desc["DBInstances"]) == 1
            assert desc["DBInstances"][0]["DBInstanceIdentifier"] == rr_name
        finally:
            try:
                client.delete_db_instance(DBInstanceIdentifier=rr_name, SkipFinalSnapshot=True)
            except ClientError:
                pass

    def test_promote_read_replica(self, client, source_instance):
        """PromoteReadReplica promotes a replica to standalone."""
        rr_name = _unique("compat-rr")
        client.create_db_instance_read_replica(
            DBInstanceIdentifier=rr_name,
            SourceDBInstanceIdentifier=source_instance,
        )
        try:
            resp = client.promote_read_replica(DBInstanceIdentifier=rr_name)
            assert resp["DBInstance"]["DBInstanceIdentifier"] == rr_name
        finally:
            try:
                client.delete_db_instance(DBInstanceIdentifier=rr_name, SkipFinalSnapshot=True)
            except ClientError:
                pass


class TestRDSExportTaskCRUD:
    @pytest.fixture
    def client(self):
        return make_client("rds")

    @pytest.fixture
    def snapshot(self, client):
        db_name = _unique("compat-db")
        snap_name = _unique("compat-snap")
        client.create_db_instance(
            DBInstanceIdentifier=db_name,
            DBInstanceClass="db.t3.micro",
            Engine="mysql",
            MasterUsername="admin",
            MasterUserPassword="password123",
        )
        client.create_db_snapshot(
            DBSnapshotIdentifier=snap_name,
            DBInstanceIdentifier=db_name,
        )
        snap_arn = f"arn:aws:rds:us-east-1:123456789012:snapshot:{snap_name}"
        yield snap_arn, snap_name, db_name
        try:
            client.delete_db_snapshot(DBSnapshotIdentifier=snap_name)
        except ClientError:
            pass
        try:
            client.delete_db_instance(DBInstanceIdentifier=db_name, SkipFinalSnapshot=True)
        except ClientError:
            pass

    def test_start_describe_cancel_export_task(self, client, snapshot):
        """Full export task lifecycle: start, describe, cancel."""
        snap_arn, _, _ = snapshot
        task_id = _unique("compat-exp")
        resp = client.start_export_task(
            ExportTaskIdentifier=task_id,
            SourceArn=snap_arn,
            S3BucketName="test-export-bucket",
            IamRoleArn="arn:aws:iam::123456789012:role/export-role",
            KmsKeyId="arn:aws:kms:us-east-1:123456789012:key/test-key",
        )
        assert resp["ExportTaskIdentifier"] == task_id
        assert resp["SourceArn"] == snap_arn

        # Describe
        desc = client.describe_export_tasks(ExportTaskIdentifier=task_id)
        assert len(desc["ExportTasks"]) == 1
        assert desc["ExportTasks"][0]["ExportTaskIdentifier"] == task_id

        # Cancel
        cancel = client.cancel_export_task(ExportTaskIdentifier=task_id)
        assert cancel["ExportTaskIdentifier"] == task_id


class TestRDSClusterTagOperations:
    """Test tagging operations on DB clusters."""

    @pytest.fixture
    def client(self):
        return make_client("rds")

    @pytest.fixture
    def cluster(self, client):
        name = _unique("compat-cl")
        client.create_db_cluster(
            DBClusterIdentifier=name,
            Engine="aurora-mysql",
            MasterUsername="admin",
            MasterUserPassword="password123!",
            Tags=[{"Key": "created-by", "Value": "compat-test"}],
        )
        yield name
        try:
            client.delete_db_cluster(DBClusterIdentifier=name, SkipFinalSnapshot=True)
        except ClientError:
            pass

    def test_cluster_create_with_tags(self, client, cluster):
        """Tags passed at cluster creation time are visible."""
        arn = f"arn:aws:rds:us-east-1:123456789012:cluster:{cluster}"
        resp = client.list_tags_for_resource(ResourceName=arn)
        tag_map = {t["Key"]: t["Value"] for t in resp["TagList"]}
        assert tag_map.get("created-by") == "compat-test"

    def test_cluster_add_and_list_tags(self, client, cluster):
        """AddTagsToResource on a cluster, then ListTagsForResource."""
        arn = f"arn:aws:rds:us-east-1:123456789012:cluster:{cluster}"
        client.add_tags_to_resource(
            ResourceName=arn,
            Tags=[
                {"Key": "env", "Value": "staging"},
                {"Key": "team", "Value": "platform"},
            ],
        )
        resp = client.list_tags_for_resource(ResourceName=arn)
        tag_map = {t["Key"]: t["Value"] for t in resp["TagList"]}
        assert tag_map["env"] == "staging"
        assert tag_map["team"] == "platform"

    def test_cluster_remove_tags(self, client, cluster):
        """RemoveTagsFromResource removes specific tags from a cluster."""
        arn = f"arn:aws:rds:us-east-1:123456789012:cluster:{cluster}"
        client.add_tags_to_resource(
            ResourceName=arn,
            Tags=[{"Key": "remove-me", "Value": "yes"}],
        )
        client.remove_tags_from_resource(ResourceName=arn, TagKeys=["remove-me"])
        resp = client.list_tags_for_resource(ResourceName=arn)
        keys = [t["Key"] for t in resp["TagList"]]
        assert "remove-me" not in keys


class TestRDSRestoreDBClusterToPointInTime:
    """Test RestoreDBClusterToPointInTime."""

    @pytest.fixture
    def client(self):
        return make_client("rds")

    def test_restore_db_cluster_to_point_in_time(self, client):
        """RestoreDBClusterToPointInTime creates a cluster from point-in-time."""
        src = _unique("compat-cl")
        tgt = _unique("compat-cl-pit")
        client.create_db_cluster(
            DBClusterIdentifier=src,
            Engine="aurora-mysql",
            MasterUsername="admin",
            MasterUserPassword="password123!",
        )
        try:
            resp = client.restore_db_cluster_to_point_in_time(
                DBClusterIdentifier=tgt,
                SourceDBClusterIdentifier=src,
                UseLatestRestorableTime=True,
            )
            assert resp["DBCluster"]["DBClusterIdentifier"] == tgt
            assert resp["DBCluster"]["Engine"] == "aurora-mysql"
        finally:
            for name in [tgt, src]:
                try:
                    client.delete_db_cluster(DBClusterIdentifier=name, SkipFinalSnapshot=True)
                except ClientError:
                    pass


class TestRDSDBProxyTargetOperations:
    """Test DBProxy target and target group operations."""

    @pytest.fixture
    def client(self):
        return make_client("rds")

    @pytest.fixture
    def ec2_client(self):
        return make_client("ec2")

    @pytest.fixture
    def proxy(self, client, ec2_client):
        vpc = ec2_client.create_vpc(CidrBlock="10.95.0.0/16")
        vpc_id = vpc["Vpc"]["VpcId"]
        s1 = ec2_client.create_subnet(
            VpcId=vpc_id, CidrBlock="10.95.1.0/24", AvailabilityZone="us-east-1a"
        )
        s2 = ec2_client.create_subnet(
            VpcId=vpc_id, CidrBlock="10.95.2.0/24", AvailabilityZone="us-east-1b"
        )
        subnet_ids = [s1["Subnet"]["SubnetId"], s2["Subnet"]["SubnetId"]]
        name = _unique("compat-px")
        client.create_db_proxy(
            DBProxyName=name,
            EngineFamily="MYSQL",
            Auth=[
                {
                    "AuthScheme": "SECRETS",
                    "SecretArn": "arn:aws:secretsmanager:us-east-1:123456789012:secret:test",
                    "IAMAuth": "DISABLED",
                }
            ],
            RoleArn="arn:aws:iam::123456789012:role/test-role",
            VpcSubnetIds=subnet_ids,
        )
        yield name
        try:
            client.delete_db_proxy(DBProxyName=name)
        except ClientError:
            pass
        for sid in subnet_ids:
            try:
                ec2_client.delete_subnet(SubnetId=sid)
            except ClientError:
                pass
        try:
            ec2_client.delete_vpc(VpcId=vpc_id)
        except ClientError:
            pass

    def test_describe_db_proxy_target_groups(self, client, proxy):
        """DescribeDBProxyTargetGroups returns target groups for a proxy."""
        resp = client.describe_db_proxy_target_groups(DBProxyName=proxy)
        assert "TargetGroups" in resp
        assert isinstance(resp["TargetGroups"], list)

    def test_describe_db_proxy_targets(self, client, proxy):
        """DescribeDBProxyTargets returns targets for a proxy."""
        resp = client.describe_db_proxy_targets(DBProxyName=proxy)
        assert "Targets" in resp
        assert isinstance(resp["Targets"], list)


class TestRDSDescribeEventsFiltered:
    """Test DescribeEvents with filters."""

    @pytest.fixture
    def client(self):
        return make_client("rds")

    def test_describe_events_by_source_type(self, client):
        """DescribeEvents can filter by SourceType."""
        resp = client.describe_events(SourceType="db-instance")
        assert "Events" in resp
        assert isinstance(resp["Events"], list)

    def test_describe_events_by_source_type_cluster(self, client):
        """DescribeEvents can filter by SourceType=db-cluster."""
        resp = client.describe_events(SourceType="db-cluster")
        assert "Events" in resp
        assert isinstance(resp["Events"], list)


class TestRDSOptionGroupDescribeSpecific:
    """Test DescribeOptionGroups with specific group name."""

    @pytest.fixture
    def client(self):
        return make_client("rds")

    def test_describe_option_group_by_name(self, client):
        """DescribeOptionGroups returns a specific group by name."""
        name = _unique("compat-og")
        client.create_option_group(
            OptionGroupName=name,
            EngineName="mysql",
            MajorEngineVersion="8.0",
            OptionGroupDescription="compat test specific describe",
        )
        try:
            resp = client.describe_option_groups(OptionGroupName=name)
            assert len(resp["OptionGroupsList"]) == 1
            og = resp["OptionGroupsList"][0]
            assert og["OptionGroupName"] == name
            assert og["EngineName"] == "mysql"
            assert og["MajorEngineVersion"] == "8.0"
        finally:
            try:
                client.delete_option_group(OptionGroupName=name)
            except ClientError:
                pass


class TestRDSRestoreDBInstanceOperations:
    @pytest.fixture
    def client(self):
        return make_client("rds")

    def test_restore_db_instance_from_snapshot(self, client):
        """RestoreDBInstanceFromDBSnapshot creates an instance from a snapshot."""
        src = _unique("compat-db")
        snap = _unique("compat-snap")
        restored = _unique("compat-rest")
        client.create_db_instance(
            DBInstanceIdentifier=src,
            DBInstanceClass="db.t3.micro",
            Engine="mysql",
            MasterUsername="admin",
            MasterUserPassword="password123",
        )
        client.create_db_snapshot(
            DBSnapshotIdentifier=snap,
            DBInstanceIdentifier=src,
        )
        try:
            resp = client.restore_db_instance_from_db_snapshot(
                DBInstanceIdentifier=restored,
                DBSnapshotIdentifier=snap,
            )
            assert resp["DBInstance"]["DBInstanceIdentifier"] == restored
            assert resp["DBInstance"]["Engine"] == "mysql"
        finally:
            for name in [restored, src]:
                try:
                    client.delete_db_instance(DBInstanceIdentifier=name, SkipFinalSnapshot=True)
                except ClientError:
                    pass
            try:
                client.delete_db_snapshot(DBSnapshotIdentifier=snap)
            except ClientError:
                pass

    def test_restore_db_instance_to_point_in_time(self, client):
        """RestoreDBInstanceToPointInTime creates instance from point-in-time."""
        src = _unique("compat-db")
        tgt = _unique("compat-pit")
        client.create_db_instance(
            DBInstanceIdentifier=src,
            DBInstanceClass="db.t3.micro",
            Engine="mysql",
            MasterUsername="admin",
            MasterUserPassword="password123",
        )
        try:
            resp = client.restore_db_instance_to_point_in_time(
                SourceDBInstanceIdentifier=src,
                TargetDBInstanceIdentifier=tgt,
                UseLatestRestorableTime=True,
            )
            assert resp["DBInstance"]["DBInstanceIdentifier"] == tgt
            assert resp["DBInstance"]["Engine"] == "mysql"
        finally:
            for name in [tgt, src]:
                try:
                    client.delete_db_instance(DBInstanceIdentifier=name, SkipFinalSnapshot=True)
                except ClientError:
                    pass


class TestRDSDBShardGroupOperations:
    """Test DBShardGroup operations."""

    @pytest.fixture
    def client(self):
        return make_client("rds")

    def test_create_db_shard_group(self, client):
        """CreateDBShardGroup creates a shard group for an Aurora Limitless cluster."""
        cluster_name = _unique("compat-cl")
        client.create_db_cluster(
            DBClusterIdentifier=cluster_name,
            Engine="aurora-mysql",
            MasterUsername="admin",
            MasterUserPassword="password123!",
        )
        sg_name = _unique("compat-sg")
        try:
            resp = client.create_db_shard_group(
                DBShardGroupIdentifier=sg_name,
                DBClusterIdentifier=cluster_name,
                MaxACU=100.0,
            )
            assert resp["DBShardGroupIdentifier"] == sg_name
            assert resp["DBClusterIdentifier"] == cluster_name
        finally:
            try:
                client.delete_db_shard_group(DBShardGroupIdentifier=sg_name)
            except ClientError:
                pass
            try:
                client.delete_db_cluster(DBClusterIdentifier=cluster_name, SkipFinalSnapshot=True)
            except ClientError:
                pass


class TestRDSDBProxyTargetRegistration:
    """Test DBProxy target registration/deregistration and modify target group."""

    @pytest.fixture
    def client(self):
        return make_client("rds")

    @pytest.fixture
    def ec2_client(self):
        return make_client("ec2")

    @pytest.fixture
    def proxy_with_instance(self, client, ec2_client):
        """Create a proxy and a DB instance for target registration."""
        vpc = ec2_client.create_vpc(CidrBlock="10.94.0.0/16")
        vpc_id = vpc["Vpc"]["VpcId"]
        s1 = ec2_client.create_subnet(
            VpcId=vpc_id, CidrBlock="10.94.1.0/24", AvailabilityZone="us-east-1a"
        )
        s2 = ec2_client.create_subnet(
            VpcId=vpc_id, CidrBlock="10.94.2.0/24", AvailabilityZone="us-east-1b"
        )
        subnet_ids = [s1["Subnet"]["SubnetId"], s2["Subnet"]["SubnetId"]]
        proxy_name = _unique("compat-px")
        db_name = _unique("compat-db")
        client.create_db_proxy(
            DBProxyName=proxy_name,
            EngineFamily="MYSQL",
            Auth=[
                {
                    "AuthScheme": "SECRETS",
                    "SecretArn": "arn:aws:secretsmanager:us-east-1:123456789012:secret:test",
                    "IAMAuth": "DISABLED",
                }
            ],
            RoleArn="arn:aws:iam::123456789012:role/test-role",
            VpcSubnetIds=subnet_ids,
        )
        client.create_db_instance(
            DBInstanceIdentifier=db_name,
            DBInstanceClass="db.t3.micro",
            Engine="mysql",
            MasterUsername="admin",
            MasterUserPassword="password123",
        )
        yield proxy_name, db_name
        try:
            client.delete_db_proxy(DBProxyName=proxy_name)
        except ClientError:
            pass
        try:
            client.delete_db_instance(DBInstanceIdentifier=db_name, SkipFinalSnapshot=True)
        except ClientError:
            pass
        for sid in subnet_ids:
            try:
                ec2_client.delete_subnet(SubnetId=sid)
            except ClientError:
                pass
        try:
            ec2_client.delete_vpc(VpcId=vpc_id)
        except ClientError:
            pass

    def test_register_and_deregister_db_proxy_targets(self, client, proxy_with_instance):
        """Register and deregister a DB instance as a proxy target."""
        proxy_name, db_name = proxy_with_instance
        resp = client.register_db_proxy_targets(
            DBProxyName=proxy_name,
            DBInstanceIdentifiers=[db_name],
        )
        assert "DBProxyTargets" in resp
        assert isinstance(resp["DBProxyTargets"], list)

        # Deregister
        dereg = client.deregister_db_proxy_targets(
            DBProxyName=proxy_name,
            DBInstanceIdentifiers=[db_name],
        )
        assert dereg["ResponseMetadata"]["HTTPStatusCode"] == 200

    def test_modify_db_proxy_target_group(self, client, proxy_with_instance):
        """ModifyDBProxyTargetGroup updates connection settings."""
        proxy_name, _ = proxy_with_instance
        resp = client.modify_db_proxy_target_group(
            TargetGroupName="default",
            DBProxyName=proxy_name,
            ConnectionPoolConfig={
                "MaxConnectionsPercent": 50,
                "MaxIdleConnectionsPercent": 25,
            },
        )
        assert "DBProxyTargetGroup" in resp
        assert resp["DBProxyTargetGroup"]["TargetGroupName"] == "default"


class TestRDSEventSubscriptionCRUD:
    """Full CRUD lifecycle for event subscriptions."""

    @pytest.fixture
    def client(self):
        return make_client("rds")

    def test_describe_event_subscription_by_name(self, client):
        """DescribeEventSubscriptions returns a specific subscription by name."""
        name = _unique("compat-esub")
        client.create_event_subscription(
            SubscriptionName=name,
            SnsTopicArn="arn:aws:sns:us-east-1:123456789012:test-topic",
        )
        try:
            resp = client.describe_event_subscriptions(SubscriptionName=name)
            subs = resp["EventSubscriptionsList"]
            assert len(subs) == 1
            sub = subs[0]
            assert sub["CustSubscriptionId"] == name
            assert sub["SnsTopicArn"] == "arn:aws:sns:us-east-1:123456789012:test-topic"
        finally:
            try:
                client.delete_event_subscription(SubscriptionName=name)
            except ClientError:
                pass

    def test_event_subscription_lifecycle(self, client):
        """Create → describe → delete → verify gone."""
        name = _unique("compat-esub")
        # Create
        create_resp = client.create_event_subscription(
            SubscriptionName=name,
            SnsTopicArn="arn:aws:sns:us-east-1:123456789012:test-topic",
        )
        assert create_resp["EventSubscription"]["CustSubscriptionId"] == name

        # Describe - should be in the list
        list_resp = client.describe_event_subscriptions()
        sub_names = [s["CustSubscriptionId"] for s in list_resp["EventSubscriptionsList"]]
        assert name in sub_names

        # Delete
        del_resp = client.delete_event_subscription(SubscriptionName=name)
        assert "EventSubscription" in del_resp

        # Verify gone
        list_resp2 = client.describe_event_subscriptions()
        sub_names2 = [s["CustSubscriptionId"] for s in list_resp2["EventSubscriptionsList"]]
        assert name not in sub_names2

    def test_delete_nonexistent_event_subscription(self, client):
        """Deleting a nonexistent event subscription raises an error."""
        with pytest.raises(ClientError) as exc:
            client.delete_event_subscription(SubscriptionName="nonexistent-sub-12345")
        assert exc.value.response["Error"]["Code"] in (
            "SubscriptionNotFound",
            "SubscriptionNotFoundFault",
        )


class TestRDSOptionGroupCRUDLifecycle:
    """Full CRUD lifecycle for option groups."""

    @pytest.fixture
    def client(self):
        return make_client("rds")

    def test_option_group_create_describe_delete(self, client):
        """Create → describe by name → verify fields → delete → verify gone."""
        name = _unique("compat-og")
        # Create
        create_resp = client.create_option_group(
            OptionGroupName=name,
            EngineName="mysql",
            MajorEngineVersion="8.0",
            OptionGroupDescription="lifecycle test",
        )
        assert create_resp["OptionGroup"]["OptionGroupName"] == name
        assert create_resp["OptionGroup"]["EngineName"] == "mysql"

        try:
            # Describe by name
            desc_resp = client.describe_option_groups(OptionGroupName=name)
            groups = desc_resp["OptionGroupsList"]
            assert len(groups) == 1
            og = groups[0]
            assert og["OptionGroupName"] == name
            assert og["EngineName"] == "mysql"
            assert og["MajorEngineVersion"] == "8.0"
            assert og["OptionGroupDescription"] == "lifecycle test"
            assert "OptionGroupArn" in og
            assert isinstance(og["Options"], list)
        finally:
            # Delete
            client.delete_option_group(OptionGroupName=name)

        # Verify gone
        with pytest.raises(ClientError) as exc:
            client.describe_option_groups(OptionGroupName=name)
        assert exc.value.response["Error"]["Code"] in (
            "OptionGroupNotFoundFault",
            "InternalError",
        )

    def test_option_group_arn_format(self, client):
        """Option group ARN follows expected format."""
        name = _unique("compat-og")
        resp = client.create_option_group(
            OptionGroupName=name,
            EngineName="mysql",
            MajorEngineVersion="8.0",
            OptionGroupDescription="arn test",
        )
        try:
            arn = resp["OptionGroup"]["OptionGroupArn"]
            assert arn.startswith("arn:aws:rds:")
            assert f":og:{name}" in arn
        finally:
            try:
                client.delete_option_group(OptionGroupName=name)
            except ClientError:
                pass


class TestRDSTagsCRUD:
    """Comprehensive tags CRUD on parameter groups (lighter-weight than DB instances)."""

    @pytest.fixture
    def client(self):
        return make_client("rds")

    def test_tags_on_parameter_group(self, client):
        """Add, list, and remove tags on a parameter group."""
        name = _unique("compat-pg")
        client.create_db_parameter_group(
            DBParameterGroupName=name,
            DBParameterGroupFamily="mysql8.0",
            Description="tag test",
        )
        arn = f"arn:aws:rds:us-east-1:123456789012:pg:{name}"
        try:
            # Add tags
            client.add_tags_to_resource(
                ResourceName=arn,
                Tags=[
                    {"Key": "env", "Value": "test"},
                    {"Key": "team", "Value": "platform"},
                ],
            )
            # List tags
            resp = client.list_tags_for_resource(ResourceName=arn)
            tag_map = {t["Key"]: t["Value"] for t in resp["TagList"]}
            assert tag_map["env"] == "test"
            assert tag_map["team"] == "platform"

            # Remove one tag
            client.remove_tags_from_resource(ResourceName=arn, TagKeys=["team"])
            resp2 = client.list_tags_for_resource(ResourceName=arn)
            keys = [t["Key"] for t in resp2["TagList"]]
            assert "team" not in keys
            assert "env" in keys
        finally:
            try:
                client.delete_db_parameter_group(DBParameterGroupName=name)
            except ClientError:
                pass

    def test_tags_on_option_group(self, client):
        """Add and list tags on an option group."""
        name = _unique("compat-og")
        resp = client.create_option_group(
            OptionGroupName=name,
            EngineName="mysql",
            MajorEngineVersion="8.0",
            OptionGroupDescription="tag test",
        )
        arn = resp["OptionGroup"]["OptionGroupArn"]
        try:
            client.add_tags_to_resource(
                ResourceName=arn,
                Tags=[{"Key": "purpose", "Value": "testing"}],
            )
            tag_resp = client.list_tags_for_resource(ResourceName=arn)
            tag_map = {t["Key"]: t["Value"] for t in tag_resp["TagList"]}
            assert tag_map["purpose"] == "testing"
        finally:
            try:
                client.delete_option_group(OptionGroupName=name)
            except ClientError:
                pass


class TestRDSAdditionalDescribeOperations:
    """Tests for additional describe/list operations that are working but untested."""

    def test_describe_db_shard_groups(self, rds):
        resp = rds.describe_db_shard_groups()
        assert "DBShardGroups" in resp
        assert isinstance(resp["DBShardGroups"], list)

    def test_describe_db_instance_automated_backups(self, rds):
        resp = rds.describe_db_instance_automated_backups()
        assert "DBInstanceAutomatedBackups" in resp
        assert isinstance(resp["DBInstanceAutomatedBackups"], list)

    def test_describe_orderable_db_instance_options(self, rds):
        resp = rds.describe_orderable_db_instance_options(Engine="mysql")
        assert "OrderableDBInstanceOptions" in resp
        assert isinstance(resp["OrderableDBInstanceOptions"], list)

    def test_describe_db_proxies_empty(self, rds):
        resp = rds.describe_db_proxies()
        assert "DBProxies" in resp
        assert isinstance(resp["DBProxies"], list)

    def test_describe_export_tasks_empty(self, rds):
        resp = rds.describe_export_tasks()
        assert "ExportTasks" in resp
        assert isinstance(resp["ExportTasks"], list)

    def test_describe_blue_green_deployments_empty(self, rds):
        resp = rds.describe_blue_green_deployments()
        assert "BlueGreenDeployments" in resp
        assert isinstance(resp["BlueGreenDeployments"], list)


class TestRDSErrorPathOperations:
    """Tests for operations that return errors for nonexistent resources."""

    def test_failover_nonexistent_db_cluster(self, rds):
        with pytest.raises(ClientError) as exc:
            rds.failover_db_cluster(DBClusterIdentifier="nonexistent-cluster")
        assert exc.value.response["Error"]["Code"] == "DBClusterNotFoundFault"

    def test_promote_read_replica_db_cluster_nonexistent(self, rds):
        with pytest.raises(ClientError) as exc:
            rds.promote_read_replica_db_cluster(DBClusterIdentifier="nonexistent-cluster")
        assert exc.value.response["Error"]["Code"] == "DBClusterNotFoundFault"

    def test_delete_db_proxy_nonexistent(self, rds):
        with pytest.raises(ClientError) as exc:
            rds.delete_db_proxy(DBProxyName="nonexistent-proxy")
        assert exc.value.response["Error"]["Code"] == "DBProxyNotFoundFault"

    def test_describe_db_proxy_target_groups_nonexistent(self, rds):
        with pytest.raises(ClientError) as exc:
            rds.describe_db_proxy_target_groups(DBProxyName="nonexistent-proxy")
        assert exc.value.response["Error"]["Code"] == "DBProxyNotFoundFault"

    def test_describe_db_proxy_targets_nonexistent(self, rds):
        with pytest.raises(ClientError) as exc:
            rds.describe_db_proxy_targets(DBProxyName="nonexistent-proxy")
        assert exc.value.response["Error"]["Code"] == "DBProxyNotFoundFault"

    def test_modify_db_subnet_group_nonexistent(self, rds):
        with pytest.raises(ClientError) as exc:
            rds.modify_db_subnet_group(
                DBSubnetGroupName="nonexistent-sg",
                SubnetIds=["subnet-12345678"],
            )
        assert exc.value.response["Error"]["Code"] in (
            "DBSubnetGroupNotFoundFault",
            "InvalidSubnetID.NotFound",
        )

    def test_delete_db_subnet_group_nonexistent(self, rds):
        with pytest.raises(ClientError) as exc:
            rds.delete_db_subnet_group(DBSubnetGroupName="nonexistent-sg")
        assert exc.value.response["Error"]["Code"] == "DBSubnetGroupNotFoundFault"

    def test_delete_global_cluster_nonexistent(self, rds):
        with pytest.raises(ClientError) as exc:
            rds.delete_global_cluster(GlobalClusterIdentifier="nonexistent-gc")
        assert exc.value.response["Error"]["Code"] == "GlobalClusterNotFoundFault"

    def test_modify_option_group_nonexistent(self, rds):
        with pytest.raises(ClientError) as exc:
            rds.modify_option_group(OptionGroupName="nonexistent-og")
        assert exc.value.response["Error"]["Code"] == "OptionGroupNotFoundFault"

    def test_delete_option_group_nonexistent(self, rds):
        with pytest.raises(ClientError) as exc:
            rds.delete_option_group(OptionGroupName="nonexistent-og")
        assert exc.value.response["Error"]["Code"] == "OptionGroupNotFoundFault"

    def test_cancel_export_task_nonexistent(self, rds):
        with pytest.raises(ClientError) as exc:
            rds.cancel_export_task(ExportTaskIdentifier="nonexistent-task")
        assert exc.value.response["Error"]["Code"] == "ExportTaskNotFound"

    def test_switchover_blue_green_nonexistent(self, rds):
        with pytest.raises(ClientError) as exc:
            rds.switchover_blue_green_deployment(BlueGreenDeploymentIdentifier="nonexistent-bgd")
        assert exc.value.response["Error"]["Code"] == "BlueGreenDeploymentNotFoundFault"

    def test_copy_db_cluster_parameter_group_nonexistent(self, rds):
        with pytest.raises(ClientError) as exc:
            rds.copy_db_cluster_parameter_group(
                SourceDBClusterParameterGroupIdentifier="nonexistent-pg",
                TargetDBClusterParameterGroupIdentifier=_unique("target-pg"),
                TargetDBClusterParameterGroupDescription="copy test",
            )
        assert exc.value.response["Error"]["Code"] == "DBParameterGroupNotFound"

    def test_copy_db_parameter_group_nonexistent(self, rds):
        with pytest.raises(ClientError) as exc:
            rds.copy_db_parameter_group(
                SourceDBParameterGroupIdentifier="nonexistent-pg",
                TargetDBParameterGroupIdentifier=_unique("target-pg"),
                TargetDBParameterGroupDescription="copy test",
            )
        assert exc.value.response["Error"]["Code"] == "DBParameterGroupNotFound"

    def test_copy_db_snapshot_nonexistent(self, rds):
        with pytest.raises(ClientError) as exc:
            rds.copy_db_snapshot(
                SourceDBSnapshotIdentifier="nonexistent-snap",
                TargetDBSnapshotIdentifier=_unique("target-snap"),
            )
        assert exc.value.response["Error"]["Code"] == "DBSnapshotNotFound"

    def test_copy_db_cluster_snapshot_nonexistent(self, rds):
        with pytest.raises(ClientError) as exc:
            rds.copy_db_cluster_snapshot(
                SourceDBClusterSnapshotIdentifier="nonexistent-snap",
                TargetDBClusterSnapshotIdentifier=_unique("target-snap"),
            )
        assert exc.value.response["Error"]["Code"] == "DBClusterSnapshotNotFoundFault"

    def test_copy_option_group_nonexistent(self, rds):
        with pytest.raises(ClientError) as exc:
            rds.copy_option_group(
                SourceOptionGroupIdentifier="nonexistent-og",
                TargetOptionGroupIdentifier=_unique("target-og"),
                TargetOptionGroupDescription="copy test",
            )
        assert exc.value.response["Error"]["Code"] == "OptionGroupNotFoundFault"


class TestRDSDescribeAccountAttributes:
    """Tests for DescribeAccountAttributes."""

    @pytest.fixture
    def client(self):
        return make_client("rds")

    def test_describe_account_attributes(self, client):
        """DescribeAccountAttributes returns quota information."""
        resp = client.describe_account_attributes()
        assert "AccountQuotas" in resp
        assert isinstance(resp["AccountQuotas"], list)
        assert len(resp["AccountQuotas"]) > 0
        # Each quota should have standard fields
        quota = resp["AccountQuotas"][0]
        assert "AccountQuotaName" in quota
        assert "Used" in quota
        assert "Max" in quota


class TestRDSDescribeCertificates:
    """Tests for DescribeCertificates."""

    @pytest.fixture
    def client(self):
        return make_client("rds")

    def test_describe_certificates(self, client):
        """DescribeCertificates returns certificate information."""
        resp = client.describe_certificates()
        assert "Certificates" in resp
        assert isinstance(resp["Certificates"], list)


class TestRDSCopyOptionGroup:
    """Tests for CopyOptionGroup."""

    @pytest.fixture
    def client(self):
        return make_client("rds")

    def test_copy_option_group(self, client):
        """CopyOptionGroup creates a copy of an existing option group."""
        src_name = _unique("compat-og-src")
        tgt_name = _unique("compat-og-tgt")
        # Create source option group
        client.create_option_group(
            OptionGroupName=src_name,
            EngineName="mysql",
            MajorEngineVersion="8.0",
            OptionGroupDescription="source for copy test",
        )
        try:
            resp = client.copy_option_group(
                SourceOptionGroupIdentifier=src_name,
                TargetOptionGroupIdentifier=tgt_name,
                TargetOptionGroupDescription="copied option group",
            )
            assert "OptionGroup" in resp
            assert resp["OptionGroup"]["OptionGroupName"] == tgt_name
            assert resp["OptionGroup"]["OptionGroupDescription"] == "copied option group"
        finally:
            try:
                client.delete_option_group(OptionGroupName=tgt_name)
            except ClientError:
                pass
            try:
                client.delete_option_group(OptionGroupName=src_name)
            except ClientError:
                pass


class TestRDSDescribeDBClusterEndpoints:
    """Tests for DescribeDBClusterEndpoints."""

    @pytest.fixture
    def client(self):
        return make_client("rds")

    def test_describe_db_cluster_endpoints_empty(self, client):
        """DescribeDBClusterEndpoints returns empty list when no clusters exist."""
        resp = client.describe_db_cluster_endpoints()
        assert "DBClusterEndpoints" in resp
        assert isinstance(resp["DBClusterEndpoints"], list)

    def test_describe_db_cluster_endpoints_for_cluster(self, client):
        """DescribeDBClusterEndpoints for a specific cluster returns its endpoints."""
        name = _unique("compat-cl")
        client.create_db_cluster(
            DBClusterIdentifier=name,
            Engine="aurora-mysql",
            MasterUsername="admin",
            MasterUserPassword="password123",
        )
        try:
            resp = client.describe_db_cluster_endpoints(DBClusterIdentifier=name)
            assert "DBClusterEndpoints" in resp
            assert isinstance(resp["DBClusterEndpoints"], list)
        finally:
            try:
                client.delete_db_cluster(DBClusterIdentifier=name, SkipFinalSnapshot=True)
            except ClientError:
                pass


class TestRDSDescribeDBEngineVersions:
    """Tests for DescribeDBEngineVersions."""

    @pytest.fixture
    def client(self):
        return make_client("rds")

    def test_describe_db_engine_versions(self, client):
        """DescribeDBEngineVersions returns engine version list."""
        resp = client.describe_db_engine_versions()
        assert "DBEngineVersions" in resp
        assert isinstance(resp["DBEngineVersions"], list)
        assert len(resp["DBEngineVersions"]) > 0

    def test_describe_db_engine_versions_by_engine(self, client):
        """DescribeDBEngineVersions can filter by engine."""
        resp = client.describe_db_engine_versions(Engine="mysql")
        assert "DBEngineVersions" in resp
        for v in resp["DBEngineVersions"]:
            assert v["Engine"] == "mysql"

    def test_describe_db_engine_versions_fields(self, client):
        """Each engine version has expected fields."""
        resp = client.describe_db_engine_versions()
        version = resp["DBEngineVersions"][0]
        assert "Engine" in version
        assert "EngineVersion" in version
        assert "DBParameterGroupFamily" in version


class TestRDSDescribeEventCategories:
    """Tests for DescribeEventCategories."""

    @pytest.fixture
    def client(self):
        return make_client("rds")

    def test_describe_event_categories(self, client):
        """DescribeEventCategories returns category map."""
        resp = client.describe_event_categories()
        assert "EventCategoriesMapList" in resp
        assert isinstance(resp["EventCategoriesMapList"], list)
        assert len(resp["EventCategoriesMapList"]) > 0

    def test_describe_event_categories_fields(self, client):
        """Each event category entry has expected fields."""
        resp = client.describe_event_categories()
        entry = resp["EventCategoriesMapList"][0]
        assert "SourceType" in entry
        assert "EventCategories" in entry
        assert isinstance(entry["EventCategories"], list)


class TestRDSDescribeEngineDefaultParameters:
    """Tests for DescribeEngineDefaultParameters."""

    @pytest.fixture
    def client(self):
        return make_client("rds")

    def test_describe_engine_default_parameters(self, client):
        """DescribeEngineDefaultParameters returns defaults for a family."""
        resp = client.describe_engine_default_parameters(DBParameterGroupFamily="mysql8.0")
        assert "EngineDefaults" in resp
        assert "Parameters" in resp["EngineDefaults"]
        assert isinstance(resp["EngineDefaults"]["Parameters"], list)


class TestRDSDescribeEngineDefaultClusterParameters:
    """Tests for DescribeEngineDefaultClusterParameters."""

    @pytest.fixture
    def client(self):
        return make_client("rds")

    def test_describe_engine_default_cluster_parameters(self, client):
        """DescribeEngineDefaultClusterParameters returns cluster param defaults."""
        resp = client.describe_engine_default_cluster_parameters(
            DBParameterGroupFamily="aurora-mysql8.0"
        )
        assert "EngineDefaults" in resp
        assert "Parameters" in resp["EngineDefaults"]
        assert isinstance(resp["EngineDefaults"]["Parameters"], list)


class TestRDSDescribeReservedDBInstances:
    """Tests for DescribeReservedDBInstances."""

    @pytest.fixture
    def client(self):
        return make_client("rds")

    def test_describe_reserved_db_instances(self, client):
        """DescribeReservedDBInstances returns empty list when none purchased."""
        resp = client.describe_reserved_db_instances()
        assert "ReservedDBInstances" in resp
        assert isinstance(resp["ReservedDBInstances"], list)


class TestRDSDescribeReservedDBInstancesOfferings:
    """Tests for DescribeReservedDBInstancesOfferings."""

    @pytest.fixture
    def client(self):
        return make_client("rds")

    def test_describe_reserved_db_instances_offerings(self, client):
        """DescribeReservedDBInstancesOfferings returns offerings list."""
        resp = client.describe_reserved_db_instances_offerings()
        assert "ReservedDBInstancesOfferings" in resp
        assert isinstance(resp["ReservedDBInstancesOfferings"], list)


class TestRDSPurchaseReservedDBInstancesOffering:
    """Tests for PurchaseReservedDBInstancesOffering."""

    @pytest.fixture
    def client(self):
        return make_client("rds")

    def test_purchase_nonexistent_offering(self, client):
        """PurchaseReservedDBInstancesOffering with bad ID returns error."""
        with pytest.raises(ClientError) as exc:
            client.purchase_reserved_db_instances_offering(
                ReservedDBInstancesOfferingId="nonexistent-offering"
            )
        assert exc.value.response["Error"]["Code"] == "ReservedDBInstancesOfferingNotFound"


class TestRDSDescribePendingMaintenanceActions:
    """Tests for DescribePendingMaintenanceActions."""

    @pytest.fixture
    def client(self):
        return make_client("rds")

    def test_describe_pending_maintenance_actions(self, client):
        """DescribePendingMaintenanceActions returns empty list by default."""
        resp = client.describe_pending_maintenance_actions()
        assert "PendingMaintenanceActions" in resp
        assert isinstance(resp["PendingMaintenanceActions"], list)


class TestRDSDescribeDBClusterAutomatedBackups:
    """Tests for DescribeDBClusterAutomatedBackups."""

    @pytest.fixture
    def client(self):
        return make_client("rds")

    def test_describe_db_cluster_automated_backups(self, client):
        """DescribeDBClusterAutomatedBackups returns empty list by default."""
        resp = client.describe_db_cluster_automated_backups()
        assert "DBClusterAutomatedBackups" in resp
        assert isinstance(resp["DBClusterAutomatedBackups"], list)


class TestRDSDescribeDBRecommendations:
    """Tests for DescribeDBRecommendations."""

    @pytest.fixture
    def client(self):
        return make_client("rds")

    def test_describe_db_recommendations(self, client):
        """DescribeDBRecommendations returns recommendations list."""
        resp = client.describe_db_recommendations()
        assert "DBRecommendations" in resp
        assert isinstance(resp["DBRecommendations"], list)


class TestRDSDescribeTenantDatabases:
    """Tests for DescribeTenantDatabases."""

    @pytest.fixture
    def client(self):
        return make_client("rds")

    def test_describe_tenant_databases(self, client):
        """DescribeTenantDatabases returns empty list by default."""
        resp = client.describe_tenant_databases()
        assert "TenantDatabases" in resp
        assert isinstance(resp["TenantDatabases"], list)


class TestRDSDescribeDBSnapshotTenantDatabases:
    """Tests for DescribeDBSnapshotTenantDatabases."""

    @pytest.fixture
    def client(self):
        return make_client("rds")

    def test_describe_db_snapshot_tenant_databases(self, client):
        """DescribeDBSnapshotTenantDatabases returns empty list by default."""
        resp = client.describe_db_snapshot_tenant_databases()
        assert "DBSnapshotTenantDatabases" in resp
        assert isinstance(resp["DBSnapshotTenantDatabases"], list)


class TestRDSDescribeIntegrations:
    """Tests for DescribeIntegrations."""

    @pytest.fixture
    def client(self):
        return make_client("rds")

    def test_describe_integrations(self, client):
        """DescribeIntegrations returns empty list by default."""
        resp = client.describe_integrations()
        assert "Integrations" in resp
        assert isinstance(resp["Integrations"], list)


class TestRDSDescribeDBProxyEndpoints:
    """Tests for DescribeDBProxyEndpoints."""

    @pytest.fixture
    def client(self):
        return make_client("rds")

    def test_describe_db_proxy_endpoints(self, client):
        """DescribeDBProxyEndpoints returns empty list by default."""
        resp = client.describe_db_proxy_endpoints()
        assert "DBProxyEndpoints" in resp
        assert isinstance(resp["DBProxyEndpoints"], list)


class TestRDSDescribeValidDBInstanceModifications:
    """Tests for DescribeValidDBInstanceModifications."""

    @pytest.fixture
    def client(self):
        return make_client("rds")

    def test_describe_valid_db_instance_modifications(self, client):
        """DescribeValidDBInstanceModifications returns modification info."""
        name = _unique("compat-db")
        client.create_db_instance(
            DBInstanceIdentifier=name,
            DBInstanceClass="db.t3.micro",
            Engine="mysql",
            MasterUsername="admin",
            MasterUserPassword="password123",
        )
        try:
            resp = client.describe_valid_db_instance_modifications(DBInstanceIdentifier=name)
            assert "ValidDBInstanceModificationsMessage" in resp
        finally:
            try:
                client.delete_db_instance(DBInstanceIdentifier=name, SkipFinalSnapshot=True)
            except ClientError:
                pass


class TestRDSCustomDBEngineVersion:
    """Tests for CustomDBEngineVersion CRUD."""

    @pytest.fixture
    def client(self):
        return make_client("rds")

    def test_create_custom_db_engine_version(self, client):
        """CreateCustomDBEngineVersion creates a custom engine version."""
        engine = "custom-oracle-ee"
        version = f"19.cv_{uuid.uuid4().hex[:6]}"
        try:
            resp = client.create_custom_db_engine_version(
                Engine=engine,
                EngineVersion=version,
                DatabaseInstallationFilesS3BucketName="my-bucket",
            )
            assert resp["Engine"] == engine
            assert resp["EngineVersion"] == version
        finally:
            try:
                client.delete_custom_db_engine_version(Engine=engine, EngineVersion=version)
            except ClientError:
                pass

    def test_delete_custom_db_engine_version(self, client):
        """DeleteCustomDBEngineVersion removes a custom engine version."""
        engine = "custom-oracle-ee"
        version = f"19.cv_{uuid.uuid4().hex[:6]}"
        client.create_custom_db_engine_version(
            Engine=engine,
            EngineVersion=version,
            DatabaseInstallationFilesS3BucketName="my-bucket",
        )
        resp = client.delete_custom_db_engine_version(Engine=engine, EngineVersion=version)
        assert resp["Engine"] == engine
        assert resp["EngineVersion"] == version

    def test_modify_custom_db_engine_version(self, client):
        """ModifyCustomDBEngineVersion updates a custom engine version."""
        engine = "custom-oracle-ee"
        version = f"19.cv_{uuid.uuid4().hex[:6]}"
        client.create_custom_db_engine_version(
            Engine=engine,
            EngineVersion=version,
            DatabaseInstallationFilesS3BucketName="my-bucket",
        )
        try:
            resp = client.modify_custom_db_engine_version(
                Engine=engine,
                EngineVersion=version,
                Description="updated description",
            )
            assert resp["Engine"] == engine
            assert resp["EngineVersion"] == version
        finally:
            try:
                client.delete_custom_db_engine_version(Engine=engine, EngineVersion=version)
            except ClientError:
                pass


class TestRDSIntegration:
    """Tests for Integration CRUD."""

    @pytest.fixture
    def client(self):
        return make_client("rds")

    def test_create_and_describe_integration(self, client):
        """CreateIntegration creates and DescribeIntegrations lists it."""
        int_name = _unique("compat-int")
        try:
            resp = client.create_integration(
                IntegrationName=int_name,
                SourceArn="arn:aws:rds:us-east-1:123456789012:cluster:src-cluster",
                TargetArn=("arn:aws:redshift-serverless:us-east-1:123456789012:namespace/tgt-ns"),
            )
            assert resp["IntegrationName"] == int_name
            assert "IntegrationArn" in resp
            integration_arn = resp["IntegrationArn"]

            desc = client.describe_integrations()
            arns = [i["IntegrationArn"] for i in desc["Integrations"]]
            assert integration_arn in arns
        finally:
            try:
                client.delete_integration(IntegrationIdentifier=integration_arn)
            except (ClientError, UnboundLocalError):
                pass

    def test_delete_integration(self, client):
        """DeleteIntegration removes an integration."""
        int_name = _unique("compat-int")
        resp = client.create_integration(
            IntegrationName=int_name,
            SourceArn="arn:aws:rds:us-east-1:123456789012:cluster:src-cluster",
            TargetArn=("arn:aws:redshift-serverless:us-east-1:123456789012:namespace/tgt-ns"),
        )
        integration_arn = resp["IntegrationArn"]
        del_resp = client.delete_integration(IntegrationIdentifier=integration_arn)
        assert del_resp["IntegrationArn"] == integration_arn

    def test_delete_nonexistent_integration(self, client):
        """DeleteIntegration with nonexistent ARN returns error."""
        with pytest.raises(ClientError) as exc:
            client.delete_integration(
                IntegrationIdentifier=("arn:aws:rds:us-east-1:123456789012:integration:nonexistent")
            )
        assert exc.value.response["Error"]["Code"] == "IntegrationNotFoundFault"

    def test_modify_nonexistent_integration(self, client):
        """ModifyIntegration with nonexistent ARN returns error."""
        with pytest.raises(ClientError) as exc:
            client.modify_integration(
                IntegrationIdentifier=("arn:aws:rds:us-east-1:123456789012:integration:nonexistent")
            )
        assert exc.value.response["Error"]["Code"] == "IntegrationNotFoundFault"


class TestRDSTenantDatabase:
    """Tests for TenantDatabase CRUD."""

    @pytest.fixture
    def client(self):
        return make_client("rds")

    @pytest.fixture
    def db_instance(self, client):
        """Create a DB instance for tenant database tests."""
        name = _unique("compat-db")
        client.create_db_instance(
            DBInstanceIdentifier=name,
            DBInstanceClass="db.t3.micro",
            Engine="mysql",
            MasterUsername="admin",
            MasterUserPassword="password123",
        )
        yield name
        try:
            client.delete_db_instance(DBInstanceIdentifier=name, SkipFinalSnapshot=True)
        except ClientError:
            pass

    def test_create_tenant_database(self, client, db_instance):
        """CreateTenantDatabase creates a tenant database on an instance."""
        resp = client.create_tenant_database(
            DBInstanceIdentifier=db_instance,
            TenantDBName="tenant1",
            MasterUsername="tenantadmin",
            MasterUserPassword="tenantpass123",
        )
        td = resp["TenantDatabase"]
        assert td["DBInstanceIdentifier"] == db_instance
        assert td["TenantDBName"] == "tenant1"
        assert td["MasterUsername"] == "tenantadmin"
        assert "Status" in td

    def test_describe_tenant_databases_after_create(self, client, db_instance):
        """DescribeTenantDatabases lists created tenant databases."""
        client.create_tenant_database(
            DBInstanceIdentifier=db_instance,
            TenantDBName="tenant2",
            MasterUsername="tenantadmin",
            MasterUserPassword="tenantpass123",
        )
        resp = client.describe_tenant_databases()
        names = [t["TenantDBName"] for t in resp["TenantDatabases"]]
        assert "tenant2" in names


class TestRDSDBClusterEndpoint:
    """Tests for DBClusterEndpoint CRUD."""

    @pytest.fixture
    def client(self):
        return make_client("rds")

    @pytest.fixture
    def cluster(self, client):
        """Create a DB cluster for endpoint tests."""
        name = _unique("compat-cl")
        client.create_db_cluster(
            DBClusterIdentifier=name,
            Engine="aurora-mysql",
            MasterUsername="admin",
            MasterUserPassword="password123",
        )
        yield name
        try:
            client.delete_db_cluster(DBClusterIdentifier=name, SkipFinalSnapshot=True)
        except ClientError:
            pass

    def test_create_db_cluster_endpoint(self, client, cluster):
        """CreateDBClusterEndpoint creates a custom endpoint."""
        ep_id = _unique("compat-ep")
        try:
            resp = client.create_db_cluster_endpoint(
                DBClusterIdentifier=cluster,
                DBClusterEndpointIdentifier=ep_id,
                EndpointType="READER",
            )
            assert resp["DBClusterEndpointIdentifier"] == ep_id
            assert resp["DBClusterIdentifier"] == cluster
            assert resp["EndpointType"] == "READER"
            assert "Endpoint" in resp
        finally:
            try:
                client.delete_db_cluster_endpoint(DBClusterEndpointIdentifier=ep_id)
            except ClientError:
                pass

    def test_modify_db_cluster_endpoint(self, client, cluster):
        """ModifyDBClusterEndpoint changes endpoint type."""
        ep_id = _unique("compat-ep")
        client.create_db_cluster_endpoint(
            DBClusterIdentifier=cluster,
            DBClusterEndpointIdentifier=ep_id,
            EndpointType="READER",
        )
        try:
            resp = client.modify_db_cluster_endpoint(
                DBClusterEndpointIdentifier=ep_id,
                EndpointType="ANY",
            )
            assert resp["DBClusterEndpointIdentifier"] == ep_id
        finally:
            try:
                client.delete_db_cluster_endpoint(DBClusterEndpointIdentifier=ep_id)
            except ClientError:
                pass

    def test_delete_db_cluster_endpoint(self, client, cluster):
        """DeleteDBClusterEndpoint removes an endpoint."""
        ep_id = _unique("compat-ep")
        client.create_db_cluster_endpoint(
            DBClusterIdentifier=cluster,
            DBClusterEndpointIdentifier=ep_id,
            EndpointType="READER",
        )
        resp = client.delete_db_cluster_endpoint(DBClusterEndpointIdentifier=ep_id)
        assert resp["DBClusterEndpointIdentifier"] == ep_id


class TestRDSActivityStream:
    """Tests for ActivityStream operations."""

    @pytest.fixture
    def client(self):
        return make_client("rds")

    @pytest.fixture
    def cluster(self, client):
        """Create a DB cluster for activity stream tests."""
        name = _unique("compat-cl")
        client.create_db_cluster(
            DBClusterIdentifier=name,
            Engine="aurora-mysql",
            MasterUsername="admin",
            MasterUserPassword="password123",
        )
        yield name
        try:
            client.delete_db_cluster(DBClusterIdentifier=name, SkipFinalSnapshot=True)
        except ClientError:
            pass

    def test_start_activity_stream(self, client, cluster):
        """StartActivityStream starts streaming for a cluster."""
        resp = client.start_activity_stream(
            ResourceArn=f"arn:aws:rds:us-east-1:123456789012:cluster:{cluster}",
            Mode="async",
            KmsKeyId="alias/aws/rds",
        )
        assert "KmsKeyId" in resp
        assert "KinesisStreamName" in resp
        assert "Status" in resp

    def test_stop_activity_stream(self, client, cluster):
        """StopActivityStream stops streaming for a cluster."""
        client.start_activity_stream(
            ResourceArn=f"arn:aws:rds:us-east-1:123456789012:cluster:{cluster}",
            Mode="async",
            KmsKeyId="alias/aws/rds",
        )
        resp = client.stop_activity_stream(
            ResourceArn=f"arn:aws:rds:us-east-1:123456789012:cluster:{cluster}",
        )
        assert "KmsKeyId" in resp
        assert "KinesisStreamName" in resp
        assert "Status" in resp

    def test_modify_activity_stream(self, client, cluster):
        """ModifyActivityStream modifies stream settings."""
        resp = client.modify_activity_stream(
            ResourceArn=f"arn:aws:rds:us-east-1:123456789012:cluster:{cluster}",
        )
        assert "KmsKeyId" in resp
        assert "KinesisStreamName" in resp
        assert "Status" in resp


class TestRDSEventSubscriptionSourceIds:
    """Tests for Add/RemoveSourceIdentifierToSubscription."""

    @pytest.fixture
    def client(self):
        return make_client("rds")

    @pytest.fixture
    def subscription(self, client):
        """Create an event subscription for source ID tests."""
        name = _unique("compat-sub")
        client.create_event_subscription(
            SubscriptionName=name,
            SnsTopicArn="arn:aws:sns:us-east-1:123456789012:my-topic",
        )
        yield name
        try:
            client.delete_event_subscription(SubscriptionName=name)
        except ClientError:
            pass

    def test_add_source_identifier(self, client, subscription):
        """AddSourceIdentifierToSubscription adds a source."""
        resp = client.add_source_identifier_to_subscription(
            SubscriptionName=subscription,
            SourceIdentifier="my-db-instance",
        )
        assert "EventSubscription" in resp
        assert "my-db-instance" in resp["EventSubscription"]["SourceIdsList"]

    def test_remove_source_identifier(self, client, subscription):
        """RemoveSourceIdentifierFromSubscription removes a source."""
        client.add_source_identifier_to_subscription(
            SubscriptionName=subscription,
            SourceIdentifier="my-db-instance",
        )
        resp = client.remove_source_identifier_from_subscription(
            SubscriptionName=subscription,
            SourceIdentifier="my-db-instance",
        )
        assert "EventSubscription" in resp
        assert "my-db-instance" not in resp["EventSubscription"].get("SourceIdsList", [])


class TestRDSDBShardGroupErrors:
    """Tests for DBShardGroup error handling."""

    @pytest.fixture
    def client(self):
        return make_client("rds")

    def test_delete_nonexistent_db_shard_group(self, client):
        """DeleteDBShardGroup with nonexistent ID returns error."""
        with pytest.raises(ClientError) as exc:
            client.delete_db_shard_group(DBShardGroupIdentifier="nonexistent-shard-group")
        assert exc.value.response["Error"]["Code"] == "DBShardGroupNotFound"

    def test_reboot_nonexistent_db_shard_group(self, client):
        """RebootDBShardGroup with nonexistent ID returns error."""
        with pytest.raises(ClientError) as exc:
            client.reboot_db_shard_group(DBShardGroupIdentifier="nonexistent-shard-group")
        assert exc.value.response["Error"]["Code"] == "DBShardGroupNotFound"

    def test_modify_nonexistent_db_shard_group(self, client):
        """ModifyDBShardGroup with nonexistent ID returns error."""
        with pytest.raises(ClientError) as exc:
            client.modify_db_shard_group(DBShardGroupIdentifier="nonexistent-shard-group")
        assert exc.value.response["Error"]["Code"] == "DBShardGroupNotFound"


class TestRDSModifyGlobalCluster:
    """Tests for ModifyGlobalCluster."""

    @pytest.fixture
    def client(self):
        return make_client("rds")

    def test_modify_nonexistent_global_cluster(self, client):
        """ModifyGlobalCluster with nonexistent ID returns error."""
        with pytest.raises(ClientError) as exc:
            client.modify_global_cluster(GlobalClusterIdentifier="nonexistent-gc")
        assert exc.value.response["Error"]["Code"] == "GlobalClusterNotFoundFault"


class TestRDSSwitchoverGlobalCluster:
    """Tests for SwitchoverGlobalCluster."""

    @pytest.fixture
    def client(self):
        return make_client("rds")

    def test_switchover_nonexistent_global_cluster(self, client):
        """SwitchoverGlobalCluster with nonexistent ID returns error."""
        with pytest.raises(ClientError) as exc:
            client.switchover_global_cluster(
                GlobalClusterIdentifier="nonexistent-gc",
                TargetDbClusterIdentifier=(
                    "arn:aws:rds:us-east-1:123456789012:cluster:nonexistent"
                ),
            )
        assert exc.value.response["Error"]["Code"] == "GlobalClusterNotFoundFault"


class TestRDSDBProxyEndpoint:
    """Tests for DBProxyEndpoint CRUD."""

    @pytest.fixture
    def client(self):
        return make_client("rds")

    @pytest.fixture
    def proxy_with_subnets(self, client):
        """Create a DB proxy for endpoint tests."""
        ec2 = make_client("ec2")
        vpc = ec2.create_vpc(CidrBlock="10.97.0.0/16")
        vpc_id = vpc["Vpc"]["VpcId"]
        s1 = ec2.create_subnet(
            VpcId=vpc_id, CidrBlock="10.97.1.0/24", AvailabilityZone="us-east-1a"
        )
        s2 = ec2.create_subnet(
            VpcId=vpc_id, CidrBlock="10.97.2.0/24", AvailabilityZone="us-east-1b"
        )
        subnet_ids = [s1["Subnet"]["SubnetId"], s2["Subnet"]["SubnetId"]]
        proxy_name = _unique("compat-proxy")
        client.create_db_proxy(
            DBProxyName=proxy_name,
            EngineFamily="MYSQL",
            Auth=[
                {
                    "AuthScheme": "SECRETS",
                    "SecretArn": ("arn:aws:secretsmanager:us-east-1:123456789012:secret:my-secret"),
                    "IAMAuth": "DISABLED",
                }
            ],
            RoleArn="arn:aws:iam::123456789012:role/my-role",
            VpcSubnetIds=subnet_ids,
        )
        yield proxy_name, subnet_ids
        try:
            client.delete_db_proxy(DBProxyName=proxy_name)
        except ClientError:
            pass

    def test_create_db_proxy_endpoint(self, client, proxy_with_subnets):
        """CreateDBProxyEndpoint creates a proxy endpoint."""
        proxy_name, subnet_ids = proxy_with_subnets
        ep_name = _unique("compat-pep")
        try:
            resp = client.create_db_proxy_endpoint(
                DBProxyName=proxy_name,
                DBProxyEndpointName=ep_name,
                VpcSubnetIds=subnet_ids,
            )
            assert "DBProxyEndpoint" in resp
            assert resp["DBProxyEndpoint"]["DBProxyEndpointName"] == ep_name
            assert resp["DBProxyEndpoint"]["DBProxyName"] == proxy_name
        finally:
            try:
                client.delete_db_proxy_endpoint(DBProxyEndpointName=ep_name)
            except ClientError:
                pass

    def test_delete_db_proxy_endpoint(self, client, proxy_with_subnets):
        """DeleteDBProxyEndpoint removes a proxy endpoint."""
        proxy_name, subnet_ids = proxy_with_subnets
        ep_name = _unique("compat-pep")
        client.create_db_proxy_endpoint(
            DBProxyName=proxy_name,
            DBProxyEndpointName=ep_name,
            VpcSubnetIds=subnet_ids,
        )
        resp = client.delete_db_proxy_endpoint(DBProxyEndpointName=ep_name)
        assert "DBProxyEndpoint" in resp
        assert resp["DBProxyEndpoint"]["DBProxyEndpointName"] == ep_name


class TestRDSCRUDBatch2:
    """Batch 2 CRUD operations for RDS."""

    # -- DB INSTANCES --

    def test_create_and_delete_db_instance(self, rds):
        """CreateDBInstance then DeleteDBInstance full lifecycle."""
        name = _unique("crud2-inst")
        try:
            resp = rds.create_db_instance(
                DBInstanceIdentifier=name,
                DBInstanceClass="db.t3.micro",
                Engine="mysql",
                MasterUsername="admin",
                MasterUserPassword="password123",
            )
            assert "DBInstance" in resp
            assert resp["DBInstance"]["DBInstanceIdentifier"] == name
            assert resp["DBInstance"]["Engine"] == "mysql"

            del_resp = rds.delete_db_instance(DBInstanceIdentifier=name, SkipFinalSnapshot=True)
            assert "DBInstance" in del_resp
            assert del_resp["DBInstance"]["DBInstanceIdentifier"] == name
        except ClientError:
            # cleanup if partially created
            try:
                rds.delete_db_instance(DBInstanceIdentifier=name, SkipFinalSnapshot=True)
            except ClientError:
                pass
            raise

    def test_describe_valid_db_instance_modifications(self, rds, db_instance):
        """DescribeValidDBInstanceModifications returns modification options."""
        resp = rds.describe_valid_db_instance_modifications(DBInstanceIdentifier=db_instance)
        assert "ValidDBInstanceModificationsMessage" in resp

    def test_create_db_instance_read_replica(self, rds, db_instance):
        """CreateDBInstanceReadReplica from a source instance."""
        replica_name = _unique("crud2-replica")
        try:
            resp = rds.create_db_instance_read_replica(
                DBInstanceIdentifier=replica_name,
                SourceDBInstanceIdentifier=db_instance,
            )
            assert "DBInstance" in resp
            assert resp["DBInstance"]["DBInstanceIdentifier"] == replica_name
        finally:
            try:
                rds.delete_db_instance(DBInstanceIdentifier=replica_name, SkipFinalSnapshot=True)
            except ClientError:
                pass

    def test_restore_db_instance_from_db_snapshot(self, rds, db_instance):
        """RestoreDBInstanceFromDBSnapshot restores an instance from a snapshot."""
        snap_name = _unique("crud2-snap")
        restored_name = _unique("crud2-restored")
        try:
            rds.create_db_snapshot(
                DBSnapshotIdentifier=snap_name,
                DBInstanceIdentifier=db_instance,
            )
            resp = rds.restore_db_instance_from_db_snapshot(
                DBInstanceIdentifier=restored_name,
                DBSnapshotIdentifier=snap_name,
            )
            assert "DBInstance" in resp
            assert resp["DBInstance"]["DBInstanceIdentifier"] == restored_name
        finally:
            try:
                rds.delete_db_instance(DBInstanceIdentifier=restored_name, SkipFinalSnapshot=True)
            except ClientError:
                pass
            try:
                rds.delete_db_snapshot(DBSnapshotIdentifier=snap_name)
            except ClientError:
                pass

    # -- DB CLUSTERS --

    def test_create_and_delete_db_cluster(self, rds):
        """CreateDBCluster then DeleteDBCluster full lifecycle."""
        name = _unique("crud2-cl")
        try:
            resp = rds.create_db_cluster(
                DBClusterIdentifier=name,
                Engine="aurora-mysql",
                MasterUsername="admin",
                MasterUserPassword="password123",
            )
            assert "DBCluster" in resp
            assert resp["DBCluster"]["DBClusterIdentifier"] == name
            assert resp["DBCluster"]["Engine"] == "aurora-mysql"

            del_resp = rds.delete_db_cluster(DBClusterIdentifier=name, SkipFinalSnapshot=True)
            assert "DBCluster" in del_resp
            assert del_resp["DBCluster"]["DBClusterIdentifier"] == name
        except ClientError:
            try:
                rds.delete_db_cluster(DBClusterIdentifier=name, SkipFinalSnapshot=True)
            except ClientError:
                pass
            raise

    def test_describe_db_cluster_endpoints(self, rds):
        """DescribeDBClusterEndpoints returns endpoints for a cluster."""
        name = _unique("crud2-clep")
        try:
            rds.create_db_cluster(
                DBClusterIdentifier=name,
                Engine="aurora-mysql",
                MasterUsername="admin",
                MasterUserPassword="password123",
            )
            resp = rds.describe_db_cluster_endpoints(DBClusterIdentifier=name)
            assert "DBClusterEndpoints" in resp
            assert isinstance(resp["DBClusterEndpoints"], list)
        finally:
            try:
                rds.delete_db_cluster(DBClusterIdentifier=name, SkipFinalSnapshot=True)
            except ClientError:
                pass

    def test_failover_db_cluster_nonexistent(self, rds):
        """FailoverDBCluster with nonexistent cluster raises error."""
        with pytest.raises(ClientError) as exc:
            rds.failover_db_cluster(DBClusterIdentifier="nonexistent-cluster-xyz-999")
        assert exc.value.response["Error"]["Code"] == "DBClusterNotFoundFault"

    def test_restore_db_cluster_from_snapshot(self, rds):
        """RestoreDBClusterFromSnapshot restores from a cluster snapshot."""
        cl_name = _unique("crud2-clrs")
        snap_name = _unique("crud2-clsnap")
        restored_name = _unique("crud2-clrest")
        try:
            rds.create_db_cluster(
                DBClusterIdentifier=cl_name,
                Engine="aurora-mysql",
                MasterUsername="admin",
                MasterUserPassword="password123",
            )
            rds.create_db_cluster_snapshot(
                DBClusterSnapshotIdentifier=snap_name,
                DBClusterIdentifier=cl_name,
            )
            resp = rds.restore_db_cluster_from_snapshot(
                DBClusterIdentifier=restored_name,
                SnapshotIdentifier=snap_name,
                Engine="aurora-mysql",
            )
            assert "DBCluster" in resp
            assert resp["DBCluster"]["DBClusterIdentifier"] == restored_name
        finally:
            for cname in [restored_name, cl_name]:
                try:
                    rds.delete_db_cluster(DBClusterIdentifier=cname, SkipFinalSnapshot=True)
                except ClientError:
                    pass
            try:
                rds.delete_db_cluster_snapshot(DBClusterSnapshotIdentifier=snap_name)
            except ClientError:
                pass

    # -- PARAMETER GROUPS --

    def test_create_and_delete_db_parameter_group(self, rds):
        """CreateDBParameterGroup, DescribeDBParameters, DeleteDBParameterGroup."""
        name = _unique("crud2-pg")
        try:
            resp = rds.create_db_parameter_group(
                DBParameterGroupName=name,
                DBParameterGroupFamily="mysql8.0",
                Description="crud2 test parameter group",
            )
            assert "DBParameterGroup" in resp
            assert resp["DBParameterGroup"]["DBParameterGroupName"] == name

            params_resp = rds.describe_db_parameters(DBParameterGroupName=name)
            assert "Parameters" in params_resp

            rds.delete_db_parameter_group(DBParameterGroupName=name)
            # Verify it's gone
            with pytest.raises(ClientError):
                rds.describe_db_parameters(DBParameterGroupName=name)
        except Exception:
            try:
                rds.delete_db_parameter_group(DBParameterGroupName=name)
            except ClientError:
                pass
            raise

    def test_create_and_delete_db_cluster_parameter_group(self, rds):
        """CreateDBClusterParameterGroup and DeleteDBClusterParameterGroup."""
        name = _unique("crud2-cpg")
        try:
            resp = rds.create_db_cluster_parameter_group(
                DBClusterParameterGroupName=name,
                DBParameterGroupFamily="aurora-mysql8.0",
                Description="crud2 test cluster parameter group",
            )
            assert "DBClusterParameterGroup" in resp
            assert resp["DBClusterParameterGroup"]["DBClusterParameterGroupName"] == name

            rds.delete_db_cluster_parameter_group(DBClusterParameterGroupName=name)
            with pytest.raises(ClientError):
                rds.describe_db_cluster_parameter_groups(DBClusterParameterGroupName=name)
        except Exception:
            try:
                rds.delete_db_cluster_parameter_group(DBClusterParameterGroupName=name)
            except ClientError:
                pass
            raise

    def test_copy_db_parameter_group(self, rds):
        """CopyDBParameterGroup copies a parameter group."""
        src = _unique("crud2-pgsrc")
        tgt = _unique("crud2-pgtgt")
        try:
            rds.create_db_parameter_group(
                DBParameterGroupName=src,
                DBParameterGroupFamily="mysql8.0",
                Description="source pg",
            )
            resp = rds.copy_db_parameter_group(
                SourceDBParameterGroupIdentifier=src,
                TargetDBParameterGroupIdentifier=tgt,
                TargetDBParameterGroupDescription="copied pg",
            )
            assert "DBParameterGroup" in resp
            assert resp["DBParameterGroup"]["DBParameterGroupName"] == tgt
        finally:
            for pg in [tgt, src]:
                try:
                    rds.delete_db_parameter_group(DBParameterGroupName=pg)
                except ClientError:
                    pass

    def test_copy_db_cluster_parameter_group(self, rds):
        """CopyDBClusterParameterGroup copies a cluster parameter group."""
        src = _unique("crud2-cpgsrc")
        tgt = _unique("crud2-cpgtgt")
        try:
            rds.create_db_cluster_parameter_group(
                DBClusterParameterGroupName=src,
                DBParameterGroupFamily="aurora-mysql8.0",
                Description="source cpg",
            )
            resp = rds.copy_db_cluster_parameter_group(
                SourceDBClusterParameterGroupIdentifier=src,
                TargetDBClusterParameterGroupIdentifier=tgt,
                TargetDBClusterParameterGroupDescription="copied cpg",
            )
            assert "DBClusterParameterGroup" in resp
            assert resp["DBClusterParameterGroup"]["DBClusterParameterGroupName"] == tgt
        finally:
            for cpg in [tgt, src]:
                try:
                    rds.delete_db_cluster_parameter_group(DBClusterParameterGroupName=cpg)
                except ClientError:
                    pass

    # -- SUBNET GROUPS --

    def test_create_and_delete_db_subnet_group(self, rds, ec2):
        """CreateDBSubnetGroup and DeleteDBSubnetGroup lifecycle."""
        name = _unique("crud2-sg")
        vpc = ec2.create_vpc(CidrBlock="10.99.0.0/16")
        vpc_id = vpc["Vpc"]["VpcId"]
        s1 = ec2.create_subnet(
            VpcId=vpc_id, CidrBlock="10.99.1.0/24", AvailabilityZone="us-east-1a"
        )
        s2 = ec2.create_subnet(
            VpcId=vpc_id, CidrBlock="10.99.2.0/24", AvailabilityZone="us-east-1b"
        )
        subnet_ids = [s1["Subnet"]["SubnetId"], s2["Subnet"]["SubnetId"]]
        try:
            resp = rds.create_db_subnet_group(
                DBSubnetGroupName=name,
                DBSubnetGroupDescription="crud2 test subnet group",
                SubnetIds=subnet_ids,
            )
            assert "DBSubnetGroup" in resp
            assert resp["DBSubnetGroup"]["DBSubnetGroupName"] == name

            rds.delete_db_subnet_group(DBSubnetGroupName=name)
            with pytest.raises(ClientError):
                rds.describe_db_subnet_groups(DBSubnetGroupName=name)
        finally:
            try:
                rds.delete_db_subnet_group(DBSubnetGroupName=name)
            except ClientError:
                pass
            for sid in subnet_ids:
                try:
                    ec2.delete_subnet(SubnetId=sid)
                except ClientError:
                    pass
            try:
                ec2.delete_vpc(VpcId=vpc_id)
            except ClientError:
                pass

    # -- SECURITY GROUPS --

    def test_create_and_delete_db_security_group(self, rds):
        """CreateDBSecurityGroup and DeleteDBSecurityGroup lifecycle."""
        name = _unique("crud2-secg")
        try:
            resp = rds.create_db_security_group(
                DBSecurityGroupName=name,
                DBSecurityGroupDescription="crud2 test security group",
            )
            assert "DBSecurityGroup" in resp
            assert resp["DBSecurityGroup"]["DBSecurityGroupName"] == name

            rds.delete_db_security_group(DBSecurityGroupName=name)
            with pytest.raises(ClientError):
                rds.describe_db_security_groups(DBSecurityGroupName=name)
        except Exception:
            try:
                rds.delete_db_security_group(DBSecurityGroupName=name)
            except ClientError:
                pass
            raise

    # -- SNAPSHOTS --

    def test_create_and_delete_db_snapshot(self, rds, db_instance):
        """CreateDBSnapshot and DeleteDBSnapshot lifecycle."""
        snap_name = _unique("crud2-dbsnap")
        try:
            resp = rds.create_db_snapshot(
                DBSnapshotIdentifier=snap_name,
                DBInstanceIdentifier=db_instance,
            )
            assert "DBSnapshot" in resp
            assert resp["DBSnapshot"]["DBSnapshotIdentifier"] == snap_name

            del_resp = rds.delete_db_snapshot(DBSnapshotIdentifier=snap_name)
            assert "DBSnapshot" in del_resp
        except Exception:
            try:
                rds.delete_db_snapshot(DBSnapshotIdentifier=snap_name)
            except ClientError:
                pass
            raise

    def test_create_and_delete_db_cluster_snapshot(self, rds):
        """CreateDBClusterSnapshot and DeleteDBClusterSnapshot lifecycle."""
        cl_name = _unique("crud2-clsnp")
        snap_name = _unique("crud2-clsnap2")
        try:
            rds.create_db_cluster(
                DBClusterIdentifier=cl_name,
                Engine="aurora-mysql",
                MasterUsername="admin",
                MasterUserPassword="password123",
            )
            resp = rds.create_db_cluster_snapshot(
                DBClusterSnapshotIdentifier=snap_name,
                DBClusterIdentifier=cl_name,
            )
            assert "DBClusterSnapshot" in resp
            assert resp["DBClusterSnapshot"]["DBClusterSnapshotIdentifier"] == snap_name

            del_resp = rds.delete_db_cluster_snapshot(DBClusterSnapshotIdentifier=snap_name)
            assert "DBClusterSnapshot" in del_resp
        finally:
            try:
                rds.delete_db_cluster_snapshot(DBClusterSnapshotIdentifier=snap_name)
            except ClientError:
                pass
            try:
                rds.delete_db_cluster(DBClusterIdentifier=cl_name, SkipFinalSnapshot=True)
            except ClientError:
                pass

    # -- PROXY --

    def test_create_and_delete_db_proxy(self, rds, ec2):
        """CreateDBProxy and DeleteDBProxy lifecycle."""
        proxy_name = _unique("crud2-prx")
        vpc = ec2.create_vpc(CidrBlock="10.77.0.0/16")
        vpc_id = vpc["Vpc"]["VpcId"]
        s1 = ec2.create_subnet(
            VpcId=vpc_id, CidrBlock="10.77.1.0/24", AvailabilityZone="us-east-1a"
        )
        s2 = ec2.create_subnet(
            VpcId=vpc_id, CidrBlock="10.77.2.0/24", AvailabilityZone="us-east-1b"
        )
        subnet_ids = [s1["Subnet"]["SubnetId"], s2["Subnet"]["SubnetId"]]
        try:
            resp = rds.create_db_proxy(
                DBProxyName=proxy_name,
                EngineFamily="MYSQL",
                Auth=[
                    {
                        "AuthScheme": "SECRETS",
                        "SecretArn": "arn:aws:secretsmanager:us-east-1:123456789012:secret:test",
                        "IAMAuth": "DISABLED",
                    }
                ],
                RoleArn="arn:aws:iam::123456789012:role/test",
                VpcSubnetIds=subnet_ids,
            )
            assert "DBProxy" in resp
            assert resp["DBProxy"]["DBProxyName"] == proxy_name

            del_resp = rds.delete_db_proxy(DBProxyName=proxy_name)
            assert "DBProxy" in del_resp
            assert del_resp["DBProxy"]["DBProxyName"] == proxy_name
        finally:
            try:
                rds.delete_db_proxy(DBProxyName=proxy_name)
            except ClientError:
                pass
            for sid in subnet_ids:
                try:
                    ec2.delete_subnet(SubnetId=sid)
                except ClientError:
                    pass
            try:
                ec2.delete_vpc(VpcId=vpc_id)
            except ClientError:
                pass

    # -- EVENT SUBSCRIPTION --

    def test_modify_event_subscription(self, rds):
        """ModifyEventSubscription modifies an existing subscription."""
        sub_name = _unique("crud2-evsub")
        try:
            rds.create_event_subscription(
                SubscriptionName=sub_name,
                SnsTopicArn="arn:aws:sns:us-east-1:123456789012:test-topic",
                Enabled=True,
            )
            resp = rds.modify_event_subscription(
                SubscriptionName=sub_name,
                Enabled=False,
            )
            assert "EventSubscription" in resp
            assert resp["EventSubscription"]["CustSubscriptionId"] == sub_name
        finally:
            try:
                rds.delete_event_subscription(SubscriptionName=sub_name)
            except ClientError:
                pass

    # -- ROLES --

    def test_add_role_to_db_cluster(self, rds):
        """AddRoleToDBCluster attaches a role to a cluster."""
        cl_name = _unique("crud2-clrole")
        role_arn = "arn:aws:iam::123456789012:role/test-role"
        try:
            rds.create_db_cluster(
                DBClusterIdentifier=cl_name,
                Engine="aurora-mysql",
                MasterUsername="admin",
                MasterUserPassword="password123",
            )
            rds.add_role_to_db_cluster(
                DBClusterIdentifier=cl_name,
                RoleArn=role_arn,
            )
            desc = rds.describe_db_clusters(DBClusterIdentifier=cl_name)
            roles = desc["DBClusters"][0].get("AssociatedRoles", [])
            role_arns = [r["RoleArn"] for r in roles]
            assert role_arn in role_arns
        finally:
            try:
                rds.delete_db_cluster(DBClusterIdentifier=cl_name, SkipFinalSnapshot=True)
            except ClientError:
                pass

    def test_add_role_to_db_instance(self, rds, db_instance):
        """AddRoleToDBInstance attaches a role to an instance."""
        role_arn = "arn:aws:iam::123456789012:role/test-inst-role"
        feature = "s3Import"
        rds.add_role_to_db_instance(
            DBInstanceIdentifier=db_instance,
            RoleArn=role_arn,
            FeatureName=feature,
        )
        desc = rds.describe_db_instances(DBInstanceIdentifier=db_instance)
        roles = desc["DBInstances"][0].get("AssociatedRoles", [])
        role_arns = [r["RoleArn"] for r in roles]
        assert role_arn in role_arns


class TestRDSApplyPendingMaintenanceAction:
    """Tests for ApplyPendingMaintenanceAction."""

    @pytest.fixture
    def client(self):
        return make_client("rds")

    def test_apply_pending_maintenance_action(self, client):
        """ApplyPendingMaintenanceAction returns a response for a valid resource."""
        resp = client.apply_pending_maintenance_action(
            ResourceIdentifier="arn:aws:rds:us-east-1:123456789012:db:nonexistent-db",
            ApplyAction="system-update",
            OptInType="immediate",
        )
        assert "ResourcePendingMaintenanceActions" in resp


class TestRDSDeleteDBInstanceAutomatedBackup:
    """Tests for DeleteDBInstanceAutomatedBackup."""

    @pytest.fixture
    def client(self):
        return make_client("rds")

    def test_delete_automated_backup_returns_response(self, client):
        """DeleteDBInstanceAutomatedBackup returns a response."""
        resp = client.delete_db_instance_automated_backup(
            DBInstanceAutomatedBackupsArn=(
                "arn:aws:rds:us-east-1:123456789012:auto-backup:ab-nonexistent123"
            ),
        )
        assert "DBInstanceAutomatedBackup" in resp


class TestRDSDeleteTenantDatabase:
    """Tests for DeleteTenantDatabase."""

    @pytest.fixture
    def client(self):
        return make_client("rds")

    def test_delete_nonexistent_tenant_database(self, client):
        """DeleteTenantDatabase with fake ID returns TenantDatabaseNotFound."""
        with pytest.raises(ClientError) as exc:
            client.delete_tenant_database(
                DBInstanceIdentifier="nonexistent-db",
                TenantDBName="nonexistent-tenant",
            )
        assert exc.value.response["Error"]["Code"] == "TenantDatabaseNotFound"


class TestRDSFailoverGlobalCluster:
    """Tests for FailoverGlobalCluster."""

    @pytest.fixture
    def client(self):
        return make_client("rds")

    def test_failover_nonexistent_global_cluster(self, client):
        """FailoverGlobalCluster with fake ID returns GlobalClusterNotFoundFault."""
        with pytest.raises(ClientError) as exc:
            client.failover_global_cluster(
                GlobalClusterIdentifier="nonexistent-global",
                TargetDbClusterIdentifier=(
                    "arn:aws:rds:us-east-1:123456789012:cluster:nonexistent"
                ),
            )
        assert exc.value.response["Error"]["Code"] == "GlobalClusterNotFoundFault"


class TestRDSModifyTenantDatabase:
    """Tests for ModifyTenantDatabase."""

    @pytest.fixture
    def client(self):
        return make_client("rds")

    def test_modify_nonexistent_tenant_database(self, client):
        """ModifyTenantDatabase with fake ID returns TenantDatabaseNotFound."""
        with pytest.raises(ClientError) as exc:
            client.modify_tenant_database(
                DBInstanceIdentifier="nonexistent-db",
                TenantDBName="nonexistent-tenant",
            )
        assert exc.value.response["Error"]["Code"] == "TenantDatabaseNotFound"


class TestRDSClusterLifecycle:
    """Tests for DB cluster lifecycle operations."""

    @pytest.fixture
    def client(self):
        return make_client("rds")

    @pytest.fixture
    def cluster(self, client):
        name = _unique("cl")
        client.create_db_cluster(
            DBClusterIdentifier=name,
            Engine="aurora-mysql",
            MasterUsername="admin",
            MasterUserPassword="password123",
        )
        yield name
        try:
            client.delete_db_cluster(DBClusterIdentifier=name, SkipFinalSnapshot=True)
        except ClientError:
            pass

    def test_create_and_delete_cluster(self, client):
        """Create a cluster and delete it."""
        name = _unique("cl")
        resp = client.create_db_cluster(
            DBClusterIdentifier=name,
            Engine="aurora-mysql",
            MasterUsername="admin",
            MasterUserPassword="password123",
        )
        assert resp["DBCluster"]["DBClusterIdentifier"] == name
        assert resp["DBCluster"]["Engine"] == "aurora-mysql"
        del_resp = client.delete_db_cluster(DBClusterIdentifier=name, SkipFinalSnapshot=True)
        assert del_resp["DBCluster"]["DBClusterIdentifier"] == name

    def test_describe_clusters_with_filter(self, client, cluster):
        """DescribeDBClusters with specific identifier filter."""
        resp = client.describe_db_clusters(DBClusterIdentifier=cluster)
        assert len(resp["DBClusters"]) == 1
        assert resp["DBClusters"][0]["DBClusterIdentifier"] == cluster

    def test_modify_cluster(self, client, cluster):
        """ModifyDBCluster to change backup retention."""
        resp = client.modify_db_cluster(
            DBClusterIdentifier=cluster,
            BackupRetentionPeriod=7,
            ApplyImmediately=True,
        )
        assert resp["DBCluster"]["DBClusterIdentifier"] == cluster

    def test_create_cluster_snapshot(self, client, cluster):
        """Create and describe a cluster snapshot."""
        snap_name = _unique("clsnap")
        resp = client.create_db_cluster_snapshot(
            DBClusterSnapshotIdentifier=snap_name,
            DBClusterIdentifier=cluster,
        )
        assert resp["DBClusterSnapshot"]["DBClusterSnapshotIdentifier"] == snap_name
        desc = client.describe_db_cluster_snapshots(DBClusterSnapshotIdentifier=snap_name)
        assert len(desc["DBClusterSnapshots"]) == 1
        client.delete_db_cluster_snapshot(DBClusterSnapshotIdentifier=snap_name)

    def test_cluster_snapshot_attributes(self, client, cluster):
        """Modify and describe cluster snapshot attributes."""
        snap_name = _unique("clsnap")
        client.create_db_cluster_snapshot(
            DBClusterSnapshotIdentifier=snap_name,
            DBClusterIdentifier=cluster,
        )
        client.modify_db_cluster_snapshot_attribute(
            DBClusterSnapshotIdentifier=snap_name,
            AttributeName="restore",
            ValuesToAdd=["all"],
        )
        resp = client.describe_db_cluster_snapshot_attributes(DBClusterSnapshotIdentifier=snap_name)
        attrs = resp["DBClusterSnapshotAttributesResult"]["DBClusterSnapshotAttributes"]
        restore_attr = [a for a in attrs if a["AttributeName"] == "restore"]
        assert len(restore_attr) == 1
        assert "all" in restore_attr[0]["AttributeValues"]
        client.delete_db_cluster_snapshot(DBClusterSnapshotIdentifier=snap_name)

    def test_copy_cluster_snapshot(self, client, cluster):
        """CopyDBClusterSnapshot creates a copy."""
        snap_name = _unique("clsnap")
        copy_name = _unique("clsnapcopy")
        client.create_db_cluster_snapshot(
            DBClusterSnapshotIdentifier=snap_name,
            DBClusterIdentifier=cluster,
        )
        resp = client.copy_db_cluster_snapshot(
            SourceDBClusterSnapshotIdentifier=snap_name,
            TargetDBClusterSnapshotIdentifier=copy_name,
        )
        assert resp["DBClusterSnapshot"]["DBClusterSnapshotIdentifier"] == copy_name
        client.delete_db_cluster_snapshot(DBClusterSnapshotIdentifier=copy_name)
        client.delete_db_cluster_snapshot(DBClusterSnapshotIdentifier=snap_name)

    def test_stop_and_start_cluster(self, client, cluster):
        """StopDBCluster then StartDBCluster."""
        stop = client.stop_db_cluster(DBClusterIdentifier=cluster)
        assert stop["DBCluster"]["DBClusterIdentifier"] == cluster
        start = client.start_db_cluster(DBClusterIdentifier=cluster)
        assert start["DBCluster"]["DBClusterIdentifier"] == cluster

    def test_failover_cluster_requires_instances(self, client, cluster):
        """FailoverDBCluster with no instances returns InvalidDBClusterStateFault."""
        with pytest.raises(ClientError) as exc:
            client.failover_db_cluster(DBClusterIdentifier=cluster)
        assert exc.value.response["Error"]["Code"] == "InvalidDBClusterStateFault"

    def test_add_role_to_cluster(self, client, cluster):
        """AddRoleToDBCluster returns 200."""
        resp = client.add_role_to_db_cluster(
            DBClusterIdentifier=cluster,
            RoleArn="arn:aws:iam::123456789012:role/test-role",
        )
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200

    def test_cluster_endpoint_lifecycle(self, client, cluster):
        """Create, modify, and delete a cluster endpoint."""
        ep_name = _unique("ep")
        create_resp = client.create_db_cluster_endpoint(
            DBClusterIdentifier=cluster,
            DBClusterEndpointIdentifier=ep_name,
            EndpointType="ANY",
        )
        assert create_resp["DBClusterEndpointIdentifier"] == ep_name
        assert create_resp["EndpointType"] == "ANY"

        mod_resp = client.modify_db_cluster_endpoint(
            DBClusterEndpointIdentifier=ep_name,
            EndpointType="READER",
        )
        assert mod_resp["EndpointType"] == "READER"

        del_resp = client.delete_db_cluster_endpoint(
            DBClusterEndpointIdentifier=ep_name,
        )
        assert del_resp["DBClusterEndpointIdentifier"] == ep_name


class TestRDSDescribeOperationsDeep:
    """Deeper assertions on Describe/List operations — verify response types and structure."""

    @pytest.fixture
    def client(self):
        return make_client("rds")

    def test_describe_db_clusters_returns_list(self, client):
        """DescribeDBClusters returns a list of clusters."""
        resp = client.describe_db_clusters()
        assert isinstance(resp["DBClusters"], list)

    def test_describe_db_instances_returns_list(self, client):
        """DescribeDBInstances returns a list of instances."""
        resp = client.describe_db_instances()
        assert isinstance(resp["DBInstances"], list)

    def test_describe_db_snapshots_returns_list(self, client):
        """DescribeDBSnapshots returns a list of snapshots."""
        resp = client.describe_db_snapshots()
        assert isinstance(resp["DBSnapshots"], list)

    def test_describe_db_cluster_snapshots_returns_list(self, client):
        """DescribeDBClusterSnapshots returns a list."""
        resp = client.describe_db_cluster_snapshots()
        assert isinstance(resp["DBClusterSnapshots"], list)

    def test_describe_db_subnet_groups_returns_list(self, client):
        """DescribeDBSubnetGroups returns a list."""
        resp = client.describe_db_subnet_groups()
        assert isinstance(resp["DBSubnetGroups"], list)

    def test_describe_db_parameter_groups_returns_list(self, client):
        """DescribeDBParameterGroups returns a list with default groups."""
        resp = client.describe_db_parameter_groups()
        groups = resp["DBParameterGroups"]
        assert isinstance(groups, list)
        assert len(groups) > 0

    def test_describe_db_cluster_parameter_groups_returns_list(self, client):
        """DescribeDBClusterParameterGroups returns a list."""
        resp = client.describe_db_cluster_parameter_groups()
        groups = resp["DBClusterParameterGroups"]
        assert isinstance(groups, list)

    def test_describe_db_security_groups_returns_list(self, client):
        """DescribeDBSecurityGroups returns a list."""
        resp = client.describe_db_security_groups()
        assert isinstance(resp["DBSecurityGroups"], list)

    def test_describe_db_engine_versions_returns_list(self, client):
        """DescribeDBEngineVersions returns a non-empty list."""
        resp = client.describe_db_engine_versions()
        versions = resp["DBEngineVersions"]
        assert isinstance(versions, list)
        assert len(versions) > 0

    def test_describe_db_engine_versions_entry_has_engine(self, client):
        """Each engine version entry has an Engine field."""
        resp = client.describe_db_engine_versions()
        versions = resp["DBEngineVersions"]
        assert len(versions) > 0
        assert "Engine" in versions[0]

    def test_describe_orderable_db_instance_options_returns_list(self, client):
        """DescribeOrderableDBInstanceOptions returns a list."""
        resp = client.describe_orderable_db_instance_options(Engine="mysql")
        assert isinstance(resp["OrderableDBInstanceOptions"], list)

    def test_describe_db_proxies_returns_list(self, client):
        """DescribeDBProxies returns a list."""
        resp = client.describe_db_proxies()
        assert isinstance(resp["DBProxies"], list)

    def test_describe_db_proxy_endpoints_returns_list(self, client):
        """DescribeDBProxyEndpoints returns a list."""
        resp = client.describe_db_proxy_endpoints()
        assert isinstance(resp["DBProxyEndpoints"], list)

    def test_describe_reserved_db_instances_returns_list(self, client):
        """DescribeReservedDBInstances returns a list."""
        resp = client.describe_reserved_db_instances()
        assert isinstance(resp["ReservedDBInstances"], list)

    def test_describe_reserved_db_instances_offerings_returns_list(self, client):
        """DescribeReservedDBInstancesOfferings returns a list."""
        resp = client.describe_reserved_db_instances_offerings()
        assert isinstance(resp["ReservedDBInstancesOfferings"], list)

    def test_describe_db_cluster_endpoints_returns_list(self, client):
        """DescribeDBClusterEndpoints returns a list."""
        resp = client.describe_db_cluster_endpoints()
        assert isinstance(resp["DBClusterEndpoints"], list)

    def test_describe_db_cluster_automated_backups_returns_list(self, client):
        """DescribeDBClusterAutomatedBackups returns a list."""
        resp = client.describe_db_cluster_automated_backups()
        assert isinstance(resp["DBClusterAutomatedBackups"], list)

    def test_describe_db_instance_automated_backups_returns_list(self, client):
        """DescribeDBInstanceAutomatedBackups returns a list."""
        resp = client.describe_db_instance_automated_backups()
        assert isinstance(resp["DBInstanceAutomatedBackups"], list)

    def test_describe_db_shard_groups_returns_list(self, client):
        """DescribeDBShardGroups returns a list."""
        resp = client.describe_db_shard_groups()
        assert isinstance(resp["DBShardGroups"], list)

    def test_describe_db_snapshot_tenant_databases_returns_list(self, client):
        """DescribeDBSnapshotTenantDatabases returns a list."""
        resp = client.describe_db_snapshot_tenant_databases()
        assert isinstance(resp["DBSnapshotTenantDatabases"], list)

    def test_describe_db_recommendations_returns_list(self, client):
        """DescribeDBRecommendations returns a list."""
        resp = client.describe_db_recommendations()
        assert isinstance(resp["DBRecommendations"], list)

    def test_describe_db_engine_versions_filter_by_engine(self, client):
        """DescribeDBEngineVersions filtered by engine returns matching entries."""
        resp = client.describe_db_engine_versions(Engine="mysql")
        versions = resp["DBEngineVersions"]
        assert isinstance(versions, list)
        for v in versions:
            assert v["Engine"] == "mysql"

    def test_describe_db_engine_versions_filter_by_postgres(self, client):
        """DescribeDBEngineVersions filtered by postgres returns matching entries."""
        resp = client.describe_db_engine_versions(Engine="postgres")
        versions = resp["DBEngineVersions"]
        assert isinstance(versions, list)
        for v in versions:
            assert v["Engine"] == "postgres"

    def test_describe_option_groups_returns_list(self, client):
        """DescribeOptionGroups returns a list."""
        resp = client.describe_option_groups()
        assert isinstance(resp["OptionGroupsList"], list)

    def test_describe_events_returns_list(self, client):
        """DescribeEvents returns a list."""
        resp = client.describe_events()
        assert isinstance(resp["Events"], list)

    def test_describe_event_subscriptions_returns_list(self, client):
        """DescribeEventSubscriptions returns a list."""
        resp = client.describe_event_subscriptions()
        assert isinstance(resp["EventSubscriptionsList"], list)

    def test_describe_global_clusters_returns_list(self, client):
        """DescribeGlobalClusters returns a list."""
        resp = client.describe_global_clusters()
        assert isinstance(resp["GlobalClusters"], list)

    def test_describe_pending_maintenance_actions_returns_list(self, client):
        """DescribePendingMaintenanceActions returns a list."""
        resp = client.describe_pending_maintenance_actions()
        assert isinstance(resp["PendingMaintenanceActions"], list)

    def test_describe_certificates_returns_list(self, client):
        """DescribeCertificates returns a list."""
        resp = client.describe_certificates()
        assert isinstance(resp["Certificates"], list)

    def test_describe_account_attributes_returns_list(self, client):
        """DescribeAccountAttributes returns a list of quota items."""
        resp = client.describe_account_attributes()
        quotas = resp["AccountQuotas"]
        assert isinstance(quotas, list)
        assert len(quotas) > 0

    def test_describe_account_attributes_has_expected_fields(self, client):
        """DescribeAccountAttributes entries have quota name and values."""
        resp = client.describe_account_attributes()
        quotas = resp["AccountQuotas"]
        assert len(quotas) > 0
        first = quotas[0]
        assert "AccountQuotaName" in first
        assert "Used" in first
        assert "Max" in first


class TestRDSParameterGroupLifecycle:
    """Full lifecycle: create, describe params, modify, copy, delete."""

    @pytest.fixture
    def client(self):
        return make_client("rds")

    def test_parameter_group_create_describe_params(self, client):
        """Create a parameter group and describe its parameters."""
        name = _unique("pg-life")
        try:
            client.create_db_parameter_group(
                DBParameterGroupName=name,
                DBParameterGroupFamily="mysql8.0",
                Description="lifecycle test",
            )
            resp = client.describe_db_parameters(DBParameterGroupName=name)
            assert isinstance(resp["Parameters"], list)
        finally:
            try:
                client.delete_db_parameter_group(DBParameterGroupName=name)
            except ClientError:
                pass

    def test_parameter_group_modify_and_verify(self, client):
        """Create, modify a parameter, then verify modification."""
        name = _unique("pg-mod")
        try:
            client.create_db_parameter_group(
                DBParameterGroupName=name,
                DBParameterGroupFamily="mysql8.0",
                Description="modify test",
            )
            mod_resp = client.modify_db_parameter_group(
                DBParameterGroupName=name,
                Parameters=[
                    {
                        "ParameterName": "max_connections",
                        "ParameterValue": "200",
                        "ApplyMethod": "pending-reboot",
                    }
                ],
            )
            assert mod_resp["DBParameterGroupName"] == name
        finally:
            try:
                client.delete_db_parameter_group(DBParameterGroupName=name)
            except ClientError:
                pass

    def test_parameter_group_copy(self, client):
        """Copy a parameter group and verify the copy exists."""
        src = _unique("pg-src")
        dst = _unique("pg-dst")
        try:
            client.create_db_parameter_group(
                DBParameterGroupName=src,
                DBParameterGroupFamily="mysql8.0",
                Description="source",
            )
            resp = client.copy_db_parameter_group(
                SourceDBParameterGroupIdentifier=src,
                TargetDBParameterGroupIdentifier=dst,
                TargetDBParameterGroupDescription="copy",
            )
            assert resp["DBParameterGroup"]["DBParameterGroupName"] == dst
            # Verify copy appears in describe
            desc = client.describe_db_parameter_groups(DBParameterGroupName=dst)
            assert len(desc["DBParameterGroups"]) == 1
            assert desc["DBParameterGroups"][0]["DBParameterGroupName"] == dst
        finally:
            for n in (dst, src):
                try:
                    client.delete_db_parameter_group(DBParameterGroupName=n)
                except ClientError:
                    pass

    def test_parameter_group_describe_specific(self, client):
        """Describe a specific parameter group by name."""
        name = _unique("pg-desc")
        try:
            client.create_db_parameter_group(
                DBParameterGroupName=name,
                DBParameterGroupFamily="mysql8.0",
                Description="describe test",
            )
            desc = client.describe_db_parameter_groups(DBParameterGroupName=name)
            assert len(desc["DBParameterGroups"]) == 1
            pg = desc["DBParameterGroups"][0]
            assert pg["DBParameterGroupName"] == name
            assert pg["Description"] == "describe test"
            assert pg["DBParameterGroupFamily"] == "mysql8.0"
        finally:
            try:
                client.delete_db_parameter_group(DBParameterGroupName=name)
            except ClientError:
                pass


class TestRDSClusterParameterGroupLifecycle:
    """Full lifecycle for cluster parameter groups."""

    @pytest.fixture
    def client(self):
        return make_client("rds")

    def test_cluster_parameter_group_create_and_describe(self, client):
        """Create a cluster parameter group and describe it."""
        name = _unique("cpg-life")
        try:
            resp = client.create_db_cluster_parameter_group(
                DBClusterParameterGroupName=name,
                DBParameterGroupFamily="aurora-mysql8.0",
                Description="lifecycle test",
            )
            assert resp["DBClusterParameterGroup"]["DBClusterParameterGroupName"] == name
            desc = client.describe_db_cluster_parameter_groups(DBClusterParameterGroupName=name)
            assert len(desc["DBClusterParameterGroups"]) == 1
            assert desc["DBClusterParameterGroups"][0]["DBClusterParameterGroupName"] == name
        finally:
            try:
                client.delete_db_cluster_parameter_group(DBClusterParameterGroupName=name)
            except ClientError:
                pass

    def test_cluster_parameter_group_describe_parameters(self, client):
        """Describe parameters of a cluster parameter group."""
        name = _unique("cpg-params")
        try:
            client.create_db_cluster_parameter_group(
                DBClusterParameterGroupName=name,
                DBParameterGroupFamily="aurora-mysql8.0",
                Description="params test",
            )
            resp = client.describe_db_cluster_parameters(DBClusterParameterGroupName=name)
            assert isinstance(resp["Parameters"], list)
        finally:
            try:
                client.delete_db_cluster_parameter_group(DBClusterParameterGroupName=name)
            except ClientError:
                pass

    def test_cluster_parameter_group_modify(self, client):
        """Modify a cluster parameter group."""
        name = _unique("cpg-mod")
        try:
            client.create_db_cluster_parameter_group(
                DBClusterParameterGroupName=name,
                DBParameterGroupFamily="aurora-mysql8.0",
                Description="modify test",
            )
            resp = client.modify_db_cluster_parameter_group(
                DBClusterParameterGroupName=name,
                Parameters=[
                    {
                        "ParameterName": "character_set_server",
                        "ParameterValue": "utf8",
                        "ApplyMethod": "pending-reboot",
                    }
                ],
            )
            assert resp["DBClusterParameterGroupName"] == name
        finally:
            try:
                client.delete_db_cluster_parameter_group(DBClusterParameterGroupName=name)
            except ClientError:
                pass

    def test_cluster_parameter_group_copy(self, client):
        """Copy a cluster parameter group."""
        src = _unique("cpg-src")
        dst = _unique("cpg-dst")
        try:
            client.create_db_cluster_parameter_group(
                DBClusterParameterGroupName=src,
                DBParameterGroupFamily="aurora-mysql8.0",
                Description="source",
            )
            resp = client.copy_db_cluster_parameter_group(
                SourceDBClusterParameterGroupIdentifier=src,
                TargetDBClusterParameterGroupIdentifier=dst,
                TargetDBClusterParameterGroupDescription="copy",
            )
            assert resp["DBClusterParameterGroup"]["DBClusterParameterGroupName"] == dst
        finally:
            for n in (dst, src):
                try:
                    client.delete_db_cluster_parameter_group(DBClusterParameterGroupName=n)
                except ClientError:
                    pass


class TestRDSSecurityGroupLifecycle:
    """Security group create, authorize, describe, delete lifecycle."""

    @pytest.fixture
    def client(self):
        return make_client("rds")

    def test_security_group_create_authorize_describe(self, client):
        """Create security group, authorize ingress, verify via describe."""
        name = _unique("dbsg-life")
        try:
            create_resp = client.create_db_security_group(
                DBSecurityGroupName=name,
                DBSecurityGroupDescription="lifecycle test",
            )
            assert create_resp["DBSecurityGroup"]["DBSecurityGroupName"] == name
            assert create_resp["DBSecurityGroup"]["DBSecurityGroupDescription"] == "lifecycle test"

            auth_resp = client.authorize_db_security_group_ingress(
                DBSecurityGroupName=name,
                CIDRIP="10.0.0.0/8",
            )
            ip_ranges = auth_resp["DBSecurityGroup"]["IPRanges"]
            assert len(ip_ranges) >= 1
            cidrs = [r["CIDRIP"] for r in ip_ranges]
            assert "10.0.0.0/8" in cidrs

            desc = client.describe_db_security_groups(DBSecurityGroupName=name)
            assert len(desc["DBSecurityGroups"]) == 1
            sg = desc["DBSecurityGroups"][0]
            assert sg["DBSecurityGroupName"] == name
            desc_cidrs = [r["CIDRIP"] for r in sg["IPRanges"]]
            assert "10.0.0.0/8" in desc_cidrs
        finally:
            try:
                client.delete_db_security_group(DBSecurityGroupName=name)
            except ClientError:
                pass


class TestRDSSubnetGroupLifecycleDeep:
    """Subnet group create, modify, describe, delete lifecycle."""

    @pytest.fixture
    def client(self):
        return make_client("rds")

    @pytest.fixture
    def ec2_client(self):
        return make_client("ec2")

    def test_subnet_group_create_and_delete(self, client, ec2_client):
        """Create subnet group and verify it exists, then delete."""
        vpcs = ec2_client.describe_vpcs()["Vpcs"]
        if not vpcs:
            pytest.skip("No VPCs available")
        vpc_id = vpcs[0]["VpcId"]
        subnets = ec2_client.describe_subnets(Filters=[{"Name": "vpc-id", "Values": [vpc_id]}])[
            "Subnets"
        ]
        if not subnets:
            pytest.skip("No subnets available")

        name = _unique("sng-life")
        try:
            create_resp = client.create_db_subnet_group(
                DBSubnetGroupName=name,
                DBSubnetGroupDescription="original",
                SubnetIds=[subnets[0]["SubnetId"]],
            )
            assert create_resp["DBSubnetGroup"]["DBSubnetGroupName"] == name
            assert create_resp["DBSubnetGroup"]["DBSubnetGroupDescription"] == "original"
        finally:
            try:
                client.delete_db_subnet_group(DBSubnetGroupName=name)
            except ClientError:
                pass

    def test_subnet_group_has_vpc_id(self, client, ec2_client):
        """Created subnet group includes VpcId in the response."""
        vpcs = ec2_client.describe_vpcs()["Vpcs"]
        if not vpcs:
            pytest.skip("No VPCs available")
        vpc_id = vpcs[0]["VpcId"]
        subnets = ec2_client.describe_subnets(Filters=[{"Name": "vpc-id", "Values": [vpc_id]}])[
            "Subnets"
        ]
        if not subnets:
            pytest.skip("No subnets available")

        name = _unique("sng-vpc")
        try:
            resp = client.create_db_subnet_group(
                DBSubnetGroupName=name,
                DBSubnetGroupDescription="vpc test",
                SubnetIds=[subnets[0]["SubnetId"]],
            )
            assert "VpcId" in resp["DBSubnetGroup"]
        finally:
            try:
                client.delete_db_subnet_group(DBSubnetGroupName=name)
            except ClientError:
                pass


class TestRDSDescribeWithFilters:
    """Tests that verify describe operations with specific identifier filters."""

    @pytest.fixture
    def client(self):
        return make_client("rds")

    def test_describe_nonexistent_db_instance(self, client):
        """DescribeDBInstances with nonexistent ID returns DBInstanceNotFound."""
        with pytest.raises(ClientError) as exc:
            client.describe_db_instances(DBInstanceIdentifier="nonexistent-db-12345")
        assert exc.value.response["Error"]["Code"] == "DBInstanceNotFound"

    def test_describe_nonexistent_db_cluster(self, client):
        """DescribeDBClusters with nonexistent ID returns DBClusterNotFoundFault."""
        with pytest.raises(ClientError) as exc:
            client.describe_db_clusters(DBClusterIdentifier="nonexistent-cl-12345")
        assert exc.value.response["Error"]["Code"] == "DBClusterNotFoundFault"

    def test_describe_parameter_groups_includes_defaults(self, client):
        """DescribeDBParameterGroups returns default parameter groups."""
        resp = client.describe_db_parameter_groups()
        groups = resp["DBParameterGroups"]
        assert isinstance(groups, list)
        assert len(groups) > 0
        # Default groups have DBParameterGroupFamily set
        assert "DBParameterGroupFamily" in groups[0]

    def test_describe_nonexistent_subnet_group(self, client):
        """DescribeDBSubnetGroups with nonexistent name returns error."""
        with pytest.raises(ClientError) as exc:
            client.describe_db_subnet_groups(DBSubnetGroupName="nonexistent-sng-12345")
        assert "Code" in exc.value.response["Error"]

    def test_describe_nonexistent_security_group(self, client):
        """DescribeDBSecurityGroups with nonexistent name returns error."""
        with pytest.raises(ClientError) as exc:
            client.describe_db_security_groups(DBSecurityGroupName="nonexistent-dbsg-12345")
        assert "Code" in exc.value.response["Error"]

    def test_describe_nonexistent_snapshot(self, client):
        """DescribeDBSnapshots with nonexistent ID returns error."""
        with pytest.raises(ClientError) as exc:
            client.describe_db_snapshots(DBSnapshotIdentifier="nonexistent-snap-12345")
        assert "Code" in exc.value.response["Error"]

    def test_describe_nonexistent_cluster_snapshot(self, client):
        """DescribeDBClusterSnapshots with nonexistent ID returns error."""
        with pytest.raises(ClientError) as exc:
            client.describe_db_cluster_snapshots(
                DBClusterSnapshotIdentifier="nonexistent-clsnap-12345"
            )
        assert "Code" in exc.value.response["Error"]

    def test_describe_db_engine_versions_has_version_field(self, client):
        """DescribeDBEngineVersions entries have EngineVersion field."""
        resp = client.describe_db_engine_versions(Engine="mysql")
        versions = resp["DBEngineVersions"]
        assert len(versions) > 0
        for v in versions:
            assert "EngineVersion" in v
            assert "DBParameterGroupFamily" in v

    def test_describe_event_categories_returns_list(self, client):
        """DescribeEventCategories returns a list of source type events."""
        resp = client.describe_event_categories()
        assert isinstance(resp["EventCategoriesMapList"], list)

    def test_describe_engine_default_parameters_returns_params(self, client):
        """DescribeEngineDefaultParameters returns parameters."""
        resp = client.describe_engine_default_parameters(DBParameterGroupFamily="mysql8.0")
        result = resp["EngineDefaults"]
        assert isinstance(result["Parameters"], list)

    def test_describe_engine_default_cluster_parameters(self, client):
        """DescribeEngineDefaultClusterParameters returns parameters."""
        resp = client.describe_engine_default_cluster_parameters(
            DBParameterGroupFamily="aurora-mysql8.0"
        )
        result = resp["EngineDefaults"]
        assert isinstance(result["Parameters"], list)


class TestRDSInstanceLifecycle:
    """Tests for DB instance lifecycle operations."""

    @pytest.fixture
    def client(self):
        return make_client("rds")

    @pytest.fixture
    def instance(self, client):
        name = _unique("inst")
        client.create_db_instance(
            DBInstanceIdentifier=name,
            DBInstanceClass="db.t3.micro",
            Engine="mysql",
            MasterUsername="admin",
            MasterUserPassword="pass123",
        )
        yield name
        try:
            client.delete_db_instance(DBInstanceIdentifier=name, SkipFinalSnapshot=True)
        except ClientError:
            pass

    def test_create_and_describe_instance(self, client):
        """Create a DB instance and describe it."""
        name = _unique("inst")
        resp = client.create_db_instance(
            DBInstanceIdentifier=name,
            DBInstanceClass="db.t3.micro",
            Engine="mysql",
            MasterUsername="admin",
            MasterUserPassword="pass123",
        )
        assert resp["DBInstance"]["DBInstanceIdentifier"] == name
        assert resp["DBInstance"]["Engine"] == "mysql"
        desc = client.describe_db_instances(DBInstanceIdentifier=name)
        assert len(desc["DBInstances"]) == 1
        assert desc["DBInstances"][0]["DBInstanceIdentifier"] == name
        client.delete_db_instance(DBInstanceIdentifier=name, SkipFinalSnapshot=True)

    def test_modify_instance(self, client, instance):
        """ModifyDBInstance changes the instance class."""
        resp = client.modify_db_instance(
            DBInstanceIdentifier=instance,
            DBInstanceClass="db.t3.small",
            ApplyImmediately=True,
        )
        assert resp["DBInstance"]["DBInstanceIdentifier"] == instance

    def test_reboot_instance(self, client, instance):
        """RebootDBInstance returns the instance."""
        resp = client.reboot_db_instance(DBInstanceIdentifier=instance)
        assert resp["DBInstance"]["DBInstanceIdentifier"] == instance

    def test_stop_and_start_instance(self, client, instance):
        """StopDBInstance then StartDBInstance."""
        stop = client.stop_db_instance(DBInstanceIdentifier=instance)
        assert stop["DBInstance"]["DBInstanceIdentifier"] == instance
        start = client.start_db_instance(DBInstanceIdentifier=instance)
        assert start["DBInstance"]["DBInstanceIdentifier"] == instance

    def test_add_role_to_instance(self, client, instance):
        """AddRoleToDBInstance returns 200."""
        resp = client.add_role_to_db_instance(
            DBInstanceIdentifier=instance,
            RoleArn="arn:aws:iam::123456789012:role/test-role",
            FeatureName="s3Import",
        )
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200

    def test_snapshot_lifecycle(self, client, instance):
        """Create, describe, modify attrs, describe attrs, copy, delete snapshot."""
        snap_name = _unique("snap")
        # Create
        resp = client.create_db_snapshot(
            DBSnapshotIdentifier=snap_name, DBInstanceIdentifier=instance
        )
        assert resp["DBSnapshot"]["DBSnapshotIdentifier"] == snap_name

        # Describe
        desc = client.describe_db_snapshots(DBSnapshotIdentifier=snap_name)
        assert len(desc["DBSnapshots"]) == 1

        # Modify attribute
        client.modify_db_snapshot_attribute(
            DBSnapshotIdentifier=snap_name,
            AttributeName="restore",
            ValuesToAdd=["all"],
        )

        # Describe attributes
        attrs_resp = client.describe_db_snapshot_attributes(DBSnapshotIdentifier=snap_name)
        attrs = attrs_resp["DBSnapshotAttributesResult"]["DBSnapshotAttributes"]
        restore_attr = [a for a in attrs if a["AttributeName"] == "restore"]
        assert len(restore_attr) == 1
        assert "all" in restore_attr[0]["AttributeValues"]

        # Copy
        copy_name = _unique("snapcopy")
        copy_resp = client.copy_db_snapshot(
            SourceDBSnapshotIdentifier=snap_name,
            TargetDBSnapshotIdentifier=copy_name,
        )
        assert copy_resp["DBSnapshot"]["DBSnapshotIdentifier"] == copy_name

        # Cleanup
        client.delete_db_snapshot(DBSnapshotIdentifier=copy_name)
        client.delete_db_snapshot(DBSnapshotIdentifier=snap_name)

    def test_delete_automated_backup(self, client):
        """DeleteDBInstanceAutomatedBackup with fake resource ID."""
        resp = client.delete_db_instance_automated_backup(DbiResourceId="dbi-fake-resource-id")
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200


class TestRDSRestoreOperations2:
    """Tests for DB restore operations."""

    @pytest.fixture
    def client(self):
        return make_client("rds")

    def test_restore_instance_from_snapshot(self, client):
        """RestoreDBInstanceFromDBSnapshot creates instance from snapshot."""
        inst = _unique("inst")
        client.create_db_instance(
            DBInstanceIdentifier=inst,
            DBInstanceClass="db.t3.micro",
            Engine="mysql",
            MasterUsername="admin",
            MasterUserPassword="pass123",
        )
        snap = _unique("snap")
        client.create_db_snapshot(DBSnapshotIdentifier=snap, DBInstanceIdentifier=inst)
        restored = _unique("restored")
        resp = client.restore_db_instance_from_db_snapshot(
            DBInstanceIdentifier=restored, DBSnapshotIdentifier=snap
        )
        assert resp["DBInstance"]["DBInstanceIdentifier"] == restored
        # Cleanup
        client.delete_db_instance(DBInstanceIdentifier=restored, SkipFinalSnapshot=True)
        client.delete_db_snapshot(DBSnapshotIdentifier=snap)
        client.delete_db_instance(DBInstanceIdentifier=inst, SkipFinalSnapshot=True)

    def test_restore_instance_to_point_in_time(self, client):
        """RestoreDBInstanceToPointInTime from source instance."""
        inst = _unique("inst")
        client.create_db_instance(
            DBInstanceIdentifier=inst,
            DBInstanceClass="db.t3.micro",
            Engine="mysql",
            MasterUsername="admin",
            MasterUserPassword="pass123",
        )
        pitr = _unique("pitr")
        resp = client.restore_db_instance_to_point_in_time(
            SourceDBInstanceIdentifier=inst,
            TargetDBInstanceIdentifier=pitr,
            UseLatestRestorableTime=True,
        )
        assert resp["DBInstance"]["DBInstanceIdentifier"] == pitr
        # Cleanup
        client.delete_db_instance(DBInstanceIdentifier=pitr, SkipFinalSnapshot=True)
        client.delete_db_instance(DBInstanceIdentifier=inst, SkipFinalSnapshot=True)

    def test_restore_cluster_from_snapshot(self, client):
        """RestoreDBClusterFromSnapshot creates cluster from snapshot."""
        cl = _unique("cl")
        client.create_db_cluster(
            DBClusterIdentifier=cl,
            Engine="aurora-mysql",
            MasterUsername="admin",
            MasterUserPassword="password123",
        )
        snap = _unique("clsnap")
        client.create_db_cluster_snapshot(DBClusterSnapshotIdentifier=snap, DBClusterIdentifier=cl)
        restored = _unique("rcl")
        resp = client.restore_db_cluster_from_snapshot(
            DBClusterIdentifier=restored,
            SnapshotIdentifier=snap,
            Engine="aurora-mysql",
        )
        assert resp["DBCluster"]["DBClusterIdentifier"] == restored
        # Cleanup
        client.delete_db_cluster(DBClusterIdentifier=restored, SkipFinalSnapshot=True)
        client.delete_db_cluster_snapshot(DBClusterSnapshotIdentifier=snap)
        client.delete_db_cluster(DBClusterIdentifier=cl, SkipFinalSnapshot=True)
