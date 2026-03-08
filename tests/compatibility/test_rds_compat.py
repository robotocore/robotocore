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
