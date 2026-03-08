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
