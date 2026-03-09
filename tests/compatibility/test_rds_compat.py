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
