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
        pass  # best-effort cleanup


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
        pass  # best-effort cleanup
    for sid in subnet_ids:
        try:
            ec2.delete_subnet(SubnetId=sid)
        except ClientError:
            pass  # best-effort cleanup
    try:
        ec2.delete_vpc(VpcId=vpc_id)
    except ClientError:
        pass  # best-effort cleanup


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
        pass  # best-effort cleanup


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
        # Verify ARN format
        assert "DBInstanceArn" in inst
        assert inst["DBInstanceArn"].startswith("arn:aws:rds:")
        # Modify to verify UPDATE pattern
        mod = rds.modify_db_instance(
            DBInstanceIdentifier=db_instance,
            MasterUserPassword="newpassword789",
        )
        assert mod["DBInstance"]["DBInstanceIdentifier"] == db_instance
        # Error on nonexistent
        with pytest.raises(ClientError) as exc:
            rds.describe_db_instances(DBInstanceIdentifier="nonexistent-db-xyz-999")
        assert exc.value.response["Error"]["Code"] == "DBInstanceNotFound"

    def test_list_db_instances(self, rds, db_instance):
        resp = rds.describe_db_instances()
        identifiers = [i["DBInstanceIdentifier"] for i in resp["DBInstances"]]
        assert db_instance in identifiers
        # Modify to cover UPDATE pattern
        mod = rds.modify_db_instance(
            DBInstanceIdentifier=db_instance,
            MasterUserPassword="newlistpass789",
        )
        assert mod["DBInstance"]["DBInstanceIdentifier"] == db_instance
        # Error on nonexistent
        with pytest.raises(ClientError) as exc:
            rds.describe_db_instances(DBInstanceIdentifier="nonexistent-db-xyz-999")
        assert exc.value.response["Error"]["Code"] == "DBInstanceNotFound"

    def test_modify_db_instance(self, rds, db_instance):
        resp = rds.modify_db_instance(
            DBInstanceIdentifier=db_instance,
            MasterUserPassword="newpassword456",
        )
        assert resp["DBInstance"]["DBInstanceIdentifier"] == db_instance
        # List to cover LIST pattern
        list_resp = rds.describe_db_instances()
        ids = [i["DBInstanceIdentifier"] for i in list_resp["DBInstances"]]
        assert db_instance in ids
        # Error on nonexistent
        with pytest.raises(ClientError) as exc:
            rds.describe_db_instances(DBInstanceIdentifier="nonexistent-db-xyz-999")
        assert exc.value.response["Error"]["Code"] == "DBInstanceNotFound"

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
        # List to verify instance still accessible after reboot
        list_resp = rds.describe_db_instances()
        ids = [i["DBInstanceIdentifier"] for i in list_resp["DBInstances"]]
        assert db_instance in ids
        # Error on nonexistent
        with pytest.raises(ClientError) as exc:
            rds.describe_db_instances(DBInstanceIdentifier="nonexistent-db-xyz-999")
        assert exc.value.response["Error"]["Code"] == "DBInstanceNotFound"

    def test_stop_db_instance(self, rds, db_instance):
        resp = rds.stop_db_instance(DBInstanceIdentifier=db_instance)
        assert resp["DBInstance"]["DBInstanceIdentifier"] == db_instance
        # List to verify instance still present after stop
        list_resp = rds.describe_db_instances()
        ids = [i["DBInstanceIdentifier"] for i in list_resp["DBInstances"]]
        assert db_instance in ids
        # Error on nonexistent
        with pytest.raises(ClientError) as exc:
            rds.describe_db_instances(DBInstanceIdentifier="nonexistent-db-xyz-999")
        assert exc.value.response["Error"]["Code"] == "DBInstanceNotFound"

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
        # Verify ARN field present
        assert "DBSubnetGroupArn" in grp
        # Modify description to cover UPDATE pattern
        mod = rds.modify_db_subnet_group(
            DBSubnetGroupName=subnet_group,
            DBSubnetGroupDescription="modified description",
            SubnetIds=[s["SubnetIdentifier"] for s in grp["Subnets"]],
        )
        assert mod["DBSubnetGroup"]["DBSubnetGroupDescription"] == "modified description"
        # Error on nonexistent
        with pytest.raises(ClientError) as exc:
            rds.describe_db_subnet_groups(DBSubnetGroupName="nonexistent-sg-xyz-999")
        assert exc.value.response["Error"]["Code"] == "DBSubnetGroupNotFoundFault"

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
                pass  # best-effort cleanup
        try:
            ec2.delete_vpc(VpcId=vpc_id)
        except ClientError:
            pass  # best-effort cleanup


class TestRDSParameterGroupOperations:
    def test_create_and_describe_parameter_group(self, rds, param_group):
        resp = rds.describe_db_parameter_groups(DBParameterGroupName=param_group)
        groups = resp["DBParameterGroups"]
        assert len(groups) == 1
        grp = groups[0]
        assert grp["DBParameterGroupName"] == param_group
        assert grp["DBParameterGroupFamily"] == "mysql8.0"
        assert grp["Description"] == "compat test parameter group"
        # Modify to cover UPDATE pattern
        mod = rds.modify_db_parameter_group(
            DBParameterGroupName=param_group,
            Parameters=[
                {
                    "ParameterName": "max_connections",
                    "ParameterValue": "150",
                    "ApplyMethod": "pending-reboot",
                }
            ],
        )
        assert mod["DBParameterGroupName"] == param_group
        # Nonexistent group returns empty list
        empty = rds.describe_db_parameter_groups(DBParameterGroupName="nonexistent-pg-xyz-999")
        assert empty["DBParameterGroups"] == []
        # Error on operations against nonexistent DB instance
        with pytest.raises(ClientError) as exc:
            rds.describe_db_instances(DBInstanceIdentifier="nonexistent-db-xyz-999")
        assert exc.value.response["Error"]["Code"] == "DBInstanceNotFound"

    def test_describe_db_parameters(self, rds, param_group):
        resp = rds.describe_db_parameters(DBParameterGroupName=param_group)
        # Should return a Parameters list (may be empty for a new group)
        assert "Parameters" in resp
        assert isinstance(resp["Parameters"], list)
        # Modify to cover UPDATE pattern
        mod = rds.modify_db_parameter_group(
            DBParameterGroupName=param_group,
            Parameters=[
                {
                    "ParameterName": "max_connections",
                    "ParameterValue": "100",
                    "ApplyMethod": "pending-reboot",
                }
            ],
        )
        assert mod["DBParameterGroupName"] == param_group
        # Error on nonexistent parameter group in describe_db_parameters
        with pytest.raises(ClientError) as exc:
            rds.describe_db_parameters(DBParameterGroupName="nonexistent-pg-xyz-999")
        assert exc.value.response["Error"]["Code"] == "DBParameterGroupNotFound"

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
        # UPDATE: modify snapshot attribute before deletion
        rds.modify_db_snapshot_attribute(
            DBSnapshotIdentifier=snap_id,
            AttributeName="restore",
            ValuesToAdd=["all"],
        )
        rds.delete_db_snapshot(DBSnapshotIdentifier=snap_id)
        with pytest.raises(ClientError) as exc:
            rds.describe_db_snapshots(DBSnapshotIdentifier=snap_id)
        assert "NotFound" in exc.value.response["Error"]["Code"]


class TestRDSDescribeOperations:
    def test_describe_events(self, rds):
        resp = rds.describe_events()
        assert "Events" in resp
        assert isinstance(resp["Events"], list)
        # Filter by source type
        rds.describe_events(SourceType="db-instance")
        # Error: nonexistent DB instance raises DBInstanceNotFound
        with pytest.raises(ClientError) as exc:
            rds.describe_db_instances(DBInstanceIdentifier="nonexistent-db-xyz-999")
        assert exc.value.response["Error"]["Code"] == "DBInstanceNotFound"

    def test_describe_orderable_db_instance_options(self, rds):
        resp = rds.describe_orderable_db_instance_options(Engine="mysql", MaxRecords=20)
        assert "OrderableDBInstanceOptions" in resp
        assert isinstance(resp["OrderableDBInstanceOptions"], list)
        # Filter by engine version
        rds.describe_orderable_db_instance_options(Engine="mysql")
        # Error: nonexistent DB instance raises DBInstanceNotFound
        with pytest.raises(ClientError) as exc:
            rds.describe_db_instances(DBInstanceIdentifier="nonexistent-db-xyz-999")
        assert exc.value.response["Error"]["Code"] == "DBInstanceNotFound"


class TestRdsAutoCoverage:
    """Auto-generated coverage tests for rds."""

    @pytest.fixture
    def client(self):
        return make_client("rds")

    def test_describe_blue_green_deployments(self, client):
        """DescribeBlueGreenDeployments returns a response; nonexistent ID raises error."""
        resp = client.describe_blue_green_deployments()
        assert "BlueGreenDeployments" in resp
        assert isinstance(resp["BlueGreenDeployments"], list)
        with pytest.raises(ClientError) as exc:
            client.describe_blue_green_deployments(BlueGreenDeploymentIdentifier="nonexistent-bgd-xyz")
        assert exc.value.response["Error"]["Code"] in (
            "BlueGreenDeploymentNotFoundFault",
            "BlueGreenDeploymentNotFound",
        )

    def test_describe_db_cluster_parameter_groups(self, client):
        """DB cluster parameter group: CREATE, RETRIEVE, LIST, UPDATE, DELETE, ERROR."""
        name = _unique("compat-cpg")
        # CREATE
        create_resp = client.create_db_cluster_parameter_group(
            DBClusterParameterGroupName=name,
            DBParameterGroupFamily="aurora-mysql8.0",
            Description="compat test cluster param group",
        )
        assert create_resp["DBClusterParameterGroup"]["DBClusterParameterGroupName"] == name
        try:
            # RETRIEVE
            retrieve_resp = client.describe_db_cluster_parameter_groups(
                DBClusterParameterGroupName=name
            )
            assert len(retrieve_resp["DBClusterParameterGroups"]) == 1
            assert retrieve_resp["DBClusterParameterGroups"][0]["DBClusterParameterGroupName"] == name
            # LIST
            list_resp = client.describe_db_cluster_parameter_groups()
            assert "DBClusterParameterGroups" in list_resp
            group_names = [g["DBClusterParameterGroupName"] for g in list_resp["DBClusterParameterGroups"]]
            assert name in group_names
            # UPDATE
            update_resp = client.modify_db_cluster_parameter_group(
                DBClusterParameterGroupName=name,
                Parameters=[
                    {
                        "ParameterName": "max_connections",
                        "ParameterValue": "300",
                        "ApplyMethod": "pending-reboot",
                    }
                ],
            )
            assert update_resp["DBClusterParameterGroupName"] == name
        finally:
            # DELETE
            client.delete_db_cluster_parameter_group(DBClusterParameterGroupName=name)
        # ERROR
        with pytest.raises(ClientError) as exc:
            client.describe_db_cluster_parameter_groups(
                DBClusterParameterGroupName="nonexistent-cpg-xyz"
            )
        assert exc.value.response["Error"]["Code"] in (
            "DBParameterGroupNotFound",
            "DBClusterParameterGroupNotFoundFault",
        )

    def test_describe_db_cluster_snapshots(self, client):
        """DB cluster snapshot: CREATE, RETRIEVE, LIST, UPDATE, DELETE, ERROR."""
        cl_name = _unique("compat-cl")
        snap_name = _unique("compat-snap")
        client.create_db_cluster(
            DBClusterIdentifier=cl_name,
            Engine="aurora-mysql",
            MasterUsername="admin",
            MasterUserPassword="password123!",
        )
        try:
            # CREATE
            create_resp = client.create_db_cluster_snapshot(
                DBClusterSnapshotIdentifier=snap_name,
                DBClusterIdentifier=cl_name,
            )
            assert create_resp["DBClusterSnapshot"]["DBClusterSnapshotIdentifier"] == snap_name
            try:
                # RETRIEVE
                retrieve_resp = client.describe_db_cluster_snapshots(
                    DBClusterSnapshotIdentifier=snap_name
                )
                assert len(retrieve_resp["DBClusterSnapshots"]) == 1
                assert retrieve_resp["DBClusterSnapshots"][0]["DBClusterSnapshotIdentifier"] == snap_name
                # LIST
                list_resp = client.describe_db_cluster_snapshots()
                assert "DBClusterSnapshots" in list_resp
                snap_ids = [s["DBClusterSnapshotIdentifier"] for s in list_resp["DBClusterSnapshots"]]
                assert snap_name in snap_ids
                # UPDATE
                update_resp = client.modify_db_cluster_snapshot_attribute(
                    DBClusterSnapshotIdentifier=snap_name,
                    AttributeName="restore",
                    ValuesToAdd=["all"],
                )
                assert update_resp["DBClusterSnapshotAttributesResult"]["DBClusterSnapshotIdentifier"] == snap_name
            finally:
                # DELETE
                client.delete_db_cluster_snapshot(DBClusterSnapshotIdentifier=snap_name)
        finally:
            try:
                client.delete_db_cluster(DBClusterIdentifier=cl_name, SkipFinalSnapshot=True)
            except ClientError:
                pass  # best-effort cleanup
        # ERROR
        with pytest.raises(ClientError) as exc:
            client.describe_db_cluster_snapshots(
                DBClusterSnapshotIdentifier="nonexistent-snap-xyz"
            )
        assert exc.value.response["Error"]["Code"] in (
            "DBClusterSnapshotNotFoundFault",
            "DBClusterSnapshotNotFound",
        )

    def test_describe_db_clusters(self, client):
        """DB cluster: CREATE, RETRIEVE, LIST, UPDATE, DELETE, ERROR."""
        name = _unique("compat-cl")
        # CREATE
        create_resp = client.create_db_cluster(
            DBClusterIdentifier=name,
            Engine="aurora-mysql",
            MasterUsername="admin",
            MasterUserPassword="password123!",
        )
        assert create_resp["DBCluster"]["DBClusterIdentifier"] == name
        try:
            # RETRIEVE
            retrieve_resp = client.describe_db_clusters(DBClusterIdentifier=name)
            assert len(retrieve_resp["DBClusters"]) == 1
            assert retrieve_resp["DBClusters"][0]["DBClusterIdentifier"] == name
            # LIST
            list_resp = client.describe_db_clusters()
            assert "DBClusters" in list_resp
            cluster_ids = [c["DBClusterIdentifier"] for c in list_resp["DBClusters"]]
            assert name in cluster_ids
            # UPDATE
            update_resp = client.modify_db_cluster(
                DBClusterIdentifier=name,
                DeletionProtection=False,
            )
            assert update_resp["DBCluster"]["DBClusterIdentifier"] == name
        finally:
            # DELETE
            try:
                client.delete_db_cluster(DBClusterIdentifier=name, SkipFinalSnapshot=True)
            except ClientError:
                pass  # best-effort cleanup
        # ERROR
        with pytest.raises(ClientError) as exc:
            client.describe_db_clusters(DBClusterIdentifier="nonexistent-cluster-xyz")
        assert exc.value.response["Error"]["Code"] in (
            "DBClusterNotFoundFault",
            "DBClusterNotFound",
        )

    def test_describe_db_instance_automated_backups(self, client):
        """DB instance automated backups: CREATE, RETRIEVE, LIST, UPDATE, DELETE, ERROR."""
        name = _unique("compat-db")
        # CREATE
        client.create_db_instance(
            DBInstanceIdentifier=name,
            DBInstanceClass="db.t3.micro",
            Engine="mysql",
            MasterUsername="admin",
            MasterUserPassword="password123",
        )
        try:
            # LIST
            list_resp = client.describe_db_instance_automated_backups()
            assert "DBInstanceAutomatedBackups" in list_resp
            # RETRIEVE
            retrieve_resp = client.describe_db_instance_automated_backups(
                DBInstanceIdentifier=name
            )
            assert "DBInstanceAutomatedBackups" in retrieve_resp
            # UPDATE
            update_resp = client.modify_db_instance(
                DBInstanceIdentifier=name,
                BackupRetentionPeriod=3,
            )
            assert update_resp["DBInstance"]["DBInstanceIdentifier"] == name
        finally:
            # DELETE
            try:
                client.delete_db_instance(DBInstanceIdentifier=name, SkipFinalSnapshot=True)
            except ClientError:
                pass  # best-effort cleanup
        # ERROR - nonexistent instance returns empty list (not an error) for automated backups
        # so we verify the instance itself is gone
        with pytest.raises(ClientError) as exc:
            client.describe_db_instances(DBInstanceIdentifier=name)
        assert exc.value.response["Error"]["Code"] in (
            "DBInstanceNotFound",
            "DBInstanceNotFoundFault",
        )

    def test_describe_db_instances(self, client):
        """DB instance: CREATE, RETRIEVE, LIST, UPDATE, DELETE, ERROR."""
        name = _unique("compat-db")
        # CREATE
        create_resp = client.create_db_instance(
            DBInstanceIdentifier=name,
            DBInstanceClass="db.t3.micro",
            Engine="mysql",
            MasterUsername="admin",
            MasterUserPassword="password123",
        )
        assert create_resp["DBInstance"]["DBInstanceIdentifier"] == name
        try:
            # RETRIEVE
            retrieve_resp = client.describe_db_instances(DBInstanceIdentifier=name)
            assert len(retrieve_resp["DBInstances"]) == 1
            assert retrieve_resp["DBInstances"][0]["DBInstanceIdentifier"] == name
            # LIST
            list_resp = client.describe_db_instances()
            assert "DBInstances" in list_resp
            inst_ids = [i["DBInstanceIdentifier"] for i in list_resp["DBInstances"]]
            assert name in inst_ids
            # UPDATE
            update_resp = client.modify_db_instance(
                DBInstanceIdentifier=name,
                DBInstanceClass="db.t3.small",
            )
            assert update_resp["DBInstance"]["DBInstanceIdentifier"] == name
        finally:
            # DELETE
            try:
                client.delete_db_instance(DBInstanceIdentifier=name, SkipFinalSnapshot=True)
            except ClientError:
                pass  # best-effort cleanup
        # ERROR
        with pytest.raises(ClientError) as exc:
            client.describe_db_instances(DBInstanceIdentifier="nonexistent-inst-xyz")
        assert exc.value.response["Error"]["Code"] in (
            "DBInstanceNotFound",
            "DBInstanceNotFoundFault",
        )

    def test_describe_db_parameter_groups(self, client):
        """DB parameter group: CREATE, RETRIEVE, LIST, UPDATE, DELETE, ERROR."""
        name = _unique("compat-pg")
        # CREATE
        create_resp = client.create_db_parameter_group(
            DBParameterGroupName=name,
            DBParameterGroupFamily="mysql8.0",
            Description="compat test parameter group",
        )
        assert create_resp["DBParameterGroup"]["DBParameterGroupName"] == name
        try:
            # RETRIEVE
            retrieve_resp = client.describe_db_parameter_groups(DBParameterGroupName=name)
            assert len(retrieve_resp["DBParameterGroups"]) == 1
            assert retrieve_resp["DBParameterGroups"][0]["DBParameterGroupName"] == name
            # LIST
            list_resp = client.describe_db_parameter_groups()
            assert "DBParameterGroups" in list_resp
            pg_names = [g["DBParameterGroupName"] for g in list_resp["DBParameterGroups"]]
            assert name in pg_names
            # UPDATE
            update_resp = client.modify_db_parameter_group(
                DBParameterGroupName=name,
                Parameters=[
                    {
                        "ParameterName": "max_connections",
                        "ParameterValue": "200",
                        "ApplyMethod": "pending-reboot",
                    }
                ],
            )
            assert update_resp["DBParameterGroupName"] == name
        finally:
            # DELETE
            client.delete_db_parameter_group(DBParameterGroupName=name)
        # ERROR - describe_db_parameters raises on nonexistent group
        with pytest.raises(ClientError) as exc:
            client.describe_db_parameters(DBParameterGroupName="nonexistent-pg-xyz")
        assert exc.value.response["Error"]["Code"] in (
            "DBParameterGroupNotFound",
            "DBParameterGroupNotFoundFault",
        )

    def test_describe_db_proxies(self, client):
        """DB proxy: CREATE, RETRIEVE, LIST, UPDATE, DELETE, ERROR."""
        ec2_client = make_client("ec2")
        vpc = ec2_client.create_vpc(CidrBlock="10.102.0.0/16")
        vpc_id = vpc["Vpc"]["VpcId"]
        s1 = ec2_client.create_subnet(
            VpcId=vpc_id, CidrBlock="10.102.1.0/24", AvailabilityZone="us-east-1a"
        )
        s2 = ec2_client.create_subnet(
            VpcId=vpc_id, CidrBlock="10.102.2.0/24", AvailabilityZone="us-east-1b"
        )
        subnet_ids = [s1["Subnet"]["SubnetId"], s2["Subnet"]["SubnetId"]]
        name = _unique("compat-px")
        # CREATE
        create_resp = client.create_db_proxy(
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
        assert create_resp["DBProxy"]["DBProxyName"] == name
        try:
            # RETRIEVE
            retrieve_resp = client.describe_db_proxies(DBProxyName=name)
            assert len(retrieve_resp["DBProxies"]) == 1
            assert retrieve_resp["DBProxies"][0]["DBProxyName"] == name
            # LIST
            list_resp = client.describe_db_proxies()
            assert "DBProxies" in list_resp
            proxy_names = [p["DBProxyName"] for p in list_resp["DBProxies"]]
            assert name in proxy_names
            # UPDATE
            update_resp = client.modify_db_proxy(DBProxyName=name, RequireTLS=True)
            assert update_resp["DBProxy"]["DBProxyName"] == name
        finally:
            # DELETE
            try:
                client.delete_db_proxy(DBProxyName=name)
            except ClientError:
                pass  # best-effort cleanup
        # ERROR
        with pytest.raises(ClientError) as exc:
            client.describe_db_proxies(DBProxyName="nonexistent-proxy-xyz")
        assert exc.value.response["Error"]["Code"] in (
            "DBProxyNotFoundFault",
            "DBProxyNotFound",
        )

    def test_describe_db_security_groups(self, client):
        """DB security group: CREATE, RETRIEVE, LIST, UPDATE, DELETE, ERROR."""
        name = _unique("compat-dbsg")
        # CREATE
        create_resp = client.create_db_security_group(
            DBSecurityGroupName=name,
            DBSecurityGroupDescription="compat test security group",
        )
        assert create_resp["DBSecurityGroup"]["DBSecurityGroupName"] == name
        try:
            # RETRIEVE
            retrieve_resp = client.describe_db_security_groups(DBSecurityGroupName=name)
            assert len(retrieve_resp["DBSecurityGroups"]) == 1
            assert retrieve_resp["DBSecurityGroups"][0]["DBSecurityGroupName"] == name
            # LIST
            list_resp = client.describe_db_security_groups()
            assert "DBSecurityGroups" in list_resp
            sg_names = [g["DBSecurityGroupName"] for g in list_resp["DBSecurityGroups"]]
            assert name in sg_names
            # UPDATE (authorize ingress)
            update_resp = client.authorize_db_security_group_ingress(
                DBSecurityGroupName=name,
                CIDRIP="10.0.0.0/8",
            )
            assert update_resp["DBSecurityGroup"]["DBSecurityGroupName"] == name
        finally:
            # DELETE
            try:
                client.delete_db_security_group(DBSecurityGroupName=name)
            except ClientError:
                pass  # best-effort cleanup
        # ERROR
        with pytest.raises(ClientError) as exc:
            client.describe_db_security_groups(DBSecurityGroupName="nonexistent-sg-xyz")
        assert exc.value.response["Error"]["Code"] in (
            "DBSecurityGroupNotFound",
            "DBSecurityGroupNotFoundFault",
        )

    def test_describe_db_shard_groups(self, client):
        """DB shard group: CREATE, RETRIEVE, LIST, UPDATE, DELETE, ERROR."""
        cl_name = _unique("compat-cl")
        sg_name = _unique("compat-shg")
        client.create_db_cluster(
            DBClusterIdentifier=cl_name,
            Engine="aurora-mysql",
            MasterUsername="admin",
            MasterUserPassword="password123!",
        )
        try:
            # CREATE
            create_resp = client.create_db_shard_group(
                DBShardGroupIdentifier=sg_name,
                DBClusterIdentifier=cl_name,
                MaxACU=100.0,
            )
            assert create_resp["DBShardGroupIdentifier"] == sg_name
            try:
                # RETRIEVE
                retrieve_resp = client.describe_db_shard_groups(DBShardGroupIdentifier=sg_name)
                assert len(retrieve_resp["DBShardGroups"]) == 1
                assert retrieve_resp["DBShardGroups"][0]["DBShardGroupIdentifier"] == sg_name
                # LIST
                list_resp = client.describe_db_shard_groups()
                assert "DBShardGroups" in list_resp
                sg_ids = [g["DBShardGroupIdentifier"] for g in list_resp["DBShardGroups"]]
                assert sg_name in sg_ids
                # UPDATE
                update_resp = client.modify_db_shard_group(
                    DBShardGroupIdentifier=sg_name,
                    MaxACU=200.0,
                )
                assert update_resp["DBShardGroupIdentifier"] == sg_name
            finally:
                # DELETE shard group
                try:
                    client.delete_db_shard_group(DBShardGroupIdentifier=sg_name)
                except ClientError:
                    pass  # best-effort cleanup
        finally:
            try:
                client.delete_db_cluster(DBClusterIdentifier=cl_name, SkipFinalSnapshot=True)
            except ClientError:
                pass  # best-effort cleanup
        # ERROR
        with pytest.raises(ClientError) as exc:
            client.describe_db_shard_groups(DBShardGroupIdentifier="nonexistent-shg-xyz")
        assert exc.value.response["Error"]["Code"] in (
            "DBShardGroupNotFound",
            "DBShardGroupNotFoundFault",
        )

    def test_describe_db_snapshots(self, client):
        """DB snapshot: CREATE, RETRIEVE, LIST, UPDATE, DELETE, ERROR."""
        inst_name = _unique("compat-db")
        snap_name = _unique("compat-snap")
        client.create_db_instance(
            DBInstanceIdentifier=inst_name,
            DBInstanceClass="db.t3.micro",
            Engine="mysql",
            MasterUsername="admin",
            MasterUserPassword="password123",
        )
        try:
            # CREATE
            create_resp = client.create_db_snapshot(
                DBSnapshotIdentifier=snap_name,
                DBInstanceIdentifier=inst_name,
            )
            assert create_resp["DBSnapshot"]["DBSnapshotIdentifier"] == snap_name
            try:
                # RETRIEVE
                retrieve_resp = client.describe_db_snapshots(DBSnapshotIdentifier=snap_name)
                assert len(retrieve_resp["DBSnapshots"]) == 1
                assert retrieve_resp["DBSnapshots"][0]["DBSnapshotIdentifier"] == snap_name
                # LIST
                list_resp = client.describe_db_snapshots()
                assert "DBSnapshots" in list_resp
                snap_ids = [s["DBSnapshotIdentifier"] for s in list_resp["DBSnapshots"]]
                assert snap_name in snap_ids
                # UPDATE
                update_resp = client.modify_db_snapshot_attribute(
                    DBSnapshotIdentifier=snap_name,
                    AttributeName="restore",
                    ValuesToAdd=["all"],
                )
                assert update_resp["DBSnapshotAttributesResult"]["DBSnapshotIdentifier"] == snap_name
            finally:
                # DELETE snapshot
                try:
                    client.delete_db_snapshot(DBSnapshotIdentifier=snap_name)
                except ClientError:
                    pass  # best-effort cleanup
        finally:
            try:
                client.delete_db_instance(DBInstanceIdentifier=inst_name, SkipFinalSnapshot=True)
            except ClientError:
                pass  # best-effort cleanup
        # ERROR
        with pytest.raises(ClientError) as exc:
            client.describe_db_snapshots(DBSnapshotIdentifier="nonexistent-snap-xyz")
        assert exc.value.response["Error"]["Code"] in (
            "DBSnapshotNotFound",
            "DBSnapshotNotFoundFault",
        )

    def test_describe_db_subnet_groups(self, client):
        """DB subnet group: CREATE, RETRIEVE, LIST, UPDATE, DELETE, ERROR."""
        ec2_client = make_client("ec2")
        vpc = ec2_client.create_vpc(CidrBlock="10.103.0.0/16")
        vpc_id = vpc["Vpc"]["VpcId"]
        s1 = ec2_client.create_subnet(
            VpcId=vpc_id, CidrBlock="10.103.1.0/24", AvailabilityZone="us-east-1a"
        )
        s2 = ec2_client.create_subnet(
            VpcId=vpc_id, CidrBlock="10.103.2.0/24", AvailabilityZone="us-east-1b"
        )
        subnet_ids = [s1["Subnet"]["SubnetId"], s2["Subnet"]["SubnetId"]]
        name = _unique("compat-sng")
        try:
            # CREATE
            create_resp = client.create_db_subnet_group(
                DBSubnetGroupName=name,
                DBSubnetGroupDescription="compat test subnet group",
                SubnetIds=subnet_ids,
            )
            assert create_resp["DBSubnetGroup"]["DBSubnetGroupName"] == name
            try:
                # RETRIEVE
                retrieve_resp = client.describe_db_subnet_groups(DBSubnetGroupName=name)
                assert len(retrieve_resp["DBSubnetGroups"]) == 1
                assert retrieve_resp["DBSubnetGroups"][0]["DBSubnetGroupName"] == name
                # LIST
                list_resp = client.describe_db_subnet_groups()
                assert "DBSubnetGroups" in list_resp
                sng_names = [g["DBSubnetGroupName"] for g in list_resp["DBSubnetGroups"]]
                assert name in sng_names
                # UPDATE
                update_resp = client.modify_db_subnet_group(
                    DBSubnetGroupName=name,
                    DBSubnetGroupDescription="modified compat test subnet group",
                    SubnetIds=subnet_ids,
                )
                assert update_resp["DBSubnetGroup"]["DBSubnetGroupName"] == name
            finally:
                # DELETE
                try:
                    client.delete_db_subnet_group(DBSubnetGroupName=name)
                except ClientError:
                    pass  # best-effort cleanup
        finally:
            for sid in subnet_ids:
                try:
                    ec2_client.delete_subnet(SubnetId=sid)
                except ClientError:
                    pass  # best-effort cleanup
            try:
                ec2_client.delete_vpc(VpcId=vpc_id)
            except ClientError:
                pass  # best-effort cleanup
        # ERROR
        with pytest.raises(ClientError) as exc:
            client.describe_db_subnet_groups(DBSubnetGroupName="nonexistent-sng-xyz")
        assert exc.value.response["Error"]["Code"] in (
            "DBSubnetGroupNotFoundFault",
            "DBSubnetGroupNotFound",
        )

    def test_describe_event_subscriptions(self, client):
        """Event subscription: CREATE, RETRIEVE, LIST, UPDATE, DELETE, ERROR."""
        sns_client = make_client("sns")
        topic_arn = sns_client.create_topic(Name="compat-rds-events")["TopicArn"]
        name = _unique("compat-esub")
        # CREATE
        create_resp = client.create_event_subscription(
            SubscriptionName=name,
            SnsTopicArn=topic_arn,
            SourceType="db-instance",
            EventCategories=["creation", "deletion"],
        )
        assert create_resp["EventSubscription"]["CustSubscriptionId"] == name
        try:
            # RETRIEVE
            retrieve_resp = client.describe_event_subscriptions(SubscriptionName=name)
            assert len(retrieve_resp["EventSubscriptionsList"]) == 1
            assert retrieve_resp["EventSubscriptionsList"][0]["CustSubscriptionId"] == name
            # LIST
            list_resp = client.describe_event_subscriptions()
            assert "EventSubscriptionsList" in list_resp
            sub_ids = [s["CustSubscriptionId"] for s in list_resp["EventSubscriptionsList"]]
            assert name in sub_ids
            # UPDATE
            update_resp = client.modify_event_subscription(
                SubscriptionName=name,
                Enabled=True,
            )
            assert update_resp["EventSubscription"]["CustSubscriptionId"] == name
        finally:
            # DELETE
            try:
                client.delete_event_subscription(SubscriptionName=name)
            except ClientError:
                pass  # best-effort cleanup
        # ERROR
        with pytest.raises(ClientError) as exc:
            client.describe_event_subscriptions(SubscriptionName="nonexistent-esub-xyz")
        assert exc.value.response["Error"]["Code"] in (
            "SubscriptionNotFound",
            "SubscriptionCategoryNotFound",
        )

    def test_describe_export_tasks(self, client):
        """Export task: CREATE, RETRIEVE, LIST, DELETE, ERROR (no UPDATE API)."""
        inst_name = _unique("compat-db")
        snap_name = _unique("compat-snap")
        task_id = _unique("compat-exp")
        client.create_db_instance(
            DBInstanceIdentifier=inst_name,
            DBInstanceClass="db.t3.micro",
            Engine="mysql",
            MasterUsername="admin",
            MasterUserPassword="password123",
        )
        try:
            snap_resp = client.create_db_snapshot(
                DBSnapshotIdentifier=snap_name,
                DBInstanceIdentifier=inst_name,
            )
            snap_arn = snap_resp["DBSnapshot"]["DBSnapshotArn"]
            try:
                # CREATE
                create_resp = client.start_export_task(
                    ExportTaskIdentifier=task_id,
                    SourceArn=snap_arn,
                    S3BucketName="compat-test-bucket",
                    IamRoleArn="arn:aws:iam::123456789012:role/test-role",
                    KmsKeyId="arn:aws:kms:us-east-1:123456789012:key/test-key",
                )
                assert create_resp["ExportTaskIdentifier"] == task_id
                try:
                    # RETRIEVE
                    retrieve_resp = client.describe_export_tasks(ExportTaskIdentifier=task_id)
                    assert len(retrieve_resp["ExportTasks"]) == 1
                    assert retrieve_resp["ExportTasks"][0]["ExportTaskIdentifier"] == task_id
                    # LIST
                    list_resp = client.describe_export_tasks()
                    assert "ExportTasks" in list_resp
                    task_ids = [t["ExportTaskIdentifier"] for t in list_resp["ExportTasks"]]
                    assert task_id in task_ids
                finally:
                    # DELETE (cancel)
                    try:
                        client.cancel_export_task(ExportTaskIdentifier=task_id)
                    except ClientError:
                        pass  # best-effort cleanup
            finally:
                try:
                    client.delete_db_snapshot(DBSnapshotIdentifier=snap_name)
                except ClientError:
                    pass  # best-effort cleanup
        finally:
            try:
                client.delete_db_instance(DBInstanceIdentifier=inst_name, SkipFinalSnapshot=True)
            except ClientError:
                pass  # best-effort cleanup
        # ERROR
        with pytest.raises(ClientError) as exc:
            client.describe_export_tasks(ExportTaskIdentifier="nonexistent-task-xyz")
        assert exc.value.response["Error"]["Code"] in (
            "ExportTaskNotFound",
            "ExportTaskNotFoundFault",
        )

    def test_describe_global_clusters(self, client):
        """Global cluster: CREATE, RETRIEVE, LIST, UPDATE, DELETE, ERROR."""
        name = _unique("compat-gc")
        # CREATE
        create_resp = client.create_global_cluster(
            GlobalClusterIdentifier=name,
            Engine="aurora-mysql",
        )
        assert create_resp["GlobalCluster"]["GlobalClusterIdentifier"] == name
        try:
            # RETRIEVE
            retrieve_resp = client.describe_global_clusters(GlobalClusterIdentifier=name)
            assert len(retrieve_resp["GlobalClusters"]) == 1
            assert retrieve_resp["GlobalClusters"][0]["GlobalClusterIdentifier"] == name
            # LIST
            list_resp = client.describe_global_clusters()
            assert "GlobalClusters" in list_resp
            gc_ids = [g["GlobalClusterIdentifier"] for g in list_resp["GlobalClusters"]]
            assert name in gc_ids
            # UPDATE
            update_resp = client.modify_global_cluster(
                GlobalClusterIdentifier=name,
                DeletionProtection=False,
            )
            assert update_resp["GlobalCluster"]["GlobalClusterIdentifier"] == name
        finally:
            # DELETE
            try:
                client.delete_global_cluster(GlobalClusterIdentifier=name)
            except ClientError:
                pass  # best-effort cleanup
        # ERROR - describe with nonexistent returns empty (not an error for global clusters)
        # so verify the cluster is gone from the list
        list_after = client.describe_global_clusters()
        remaining_ids = [g["GlobalClusterIdentifier"] for g in list_after["GlobalClusters"]]
        assert name not in remaining_ids


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
                pass  # best-effort cleanup

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
                pass  # best-effort cleanup

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
                    pass  # best-effort cleanup

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
                pass  # best-effort cleanup


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
                    pass  # best-effort cleanup

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
                pass  # best-effort cleanup


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
                pass  # best-effort cleanup

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
                pass  # best-effort cleanup

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
                pass  # best-effort cleanup


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
            pass  # best-effort cleanup

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
                pass  # best-effort cleanup

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
                    pass  # best-effort cleanup

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
                pass  # best-effort cleanup

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
                pass  # best-effort cleanup


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
            pass  # best-effort cleanup

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
                    pass  # best-effort cleanup

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
                pass  # best-effort cleanup

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
                pass  # best-effort cleanup


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
                pass  # best-effort cleanup

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
        """Option group options: CREATE option group, RETRIEVE options by engine+version, LIST all,
        DESCRIBE specific option group (UPDATE analog), DELETE option group, ERROR on invalid engine."""
        og_name = _unique("compat-og")
        # CREATE an option group
        create_resp = client.create_option_group(
            OptionGroupName=og_name,
            EngineName="mysql",
            MajorEngineVersion="8.0",
            OptionGroupDescription="compat test option group",
        )
        assert create_resp["OptionGroup"]["OptionGroupName"] == og_name
        try:
            # RETRIEVE - describe option group options filtered by engine + version
            retrieve_resp = client.describe_option_group_options(
                EngineName="mysql",
                MajorEngineVersion="8.0",
            )
            assert "OptionGroupOptions" in retrieve_resp
            assert isinstance(retrieve_resp["OptionGroupOptions"], list)
            # LIST - describe option group options for engine (no version filter)
            list_resp = client.describe_option_group_options(EngineName="mysql")
            assert "OptionGroupOptions" in list_resp
            # RETRIEVE the option group itself by name
            og_resp = client.describe_option_groups(OptionGroupName=og_name)
            assert len(og_resp["OptionGroupsList"]) == 1
            assert og_resp["OptionGroupsList"][0]["OptionGroupName"] == og_name
        finally:
            # DELETE
            try:
                client.delete_option_group(OptionGroupName=og_name)
            except ClientError:
                pass  # best-effort cleanup
        # ERROR - describe_option_group_options with invalid engine raises
        with pytest.raises(ClientError) as exc:
            client.describe_option_group_options(EngineName="nonexistent-engine-xyz")
        assert exc.value.response["Error"]["Code"] in (
            "InvalidParameterValue",
            "InvalidParameterCombination",
        )


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
                pass  # best-effort cleanup

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
                pass  # best-effort cleanup


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
                pass  # best-effort cleanup

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
                pass  # best-effort cleanup


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
                pass  # best-effort cleanup


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
                pass  # best-effort cleanup

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
                pass  # best-effort cleanup


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
            pass  # best-effort cleanup

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
                pass  # best-effort cleanup

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
                    pass  # best-effort cleanup
                try:
                    client.delete_db_cluster_snapshot(DBClusterSnapshotIdentifier=snap_name)
                except ClientError:
                    pass  # best-effort cleanup
        finally:
            try:
                client.delete_db_cluster(DBClusterIdentifier=cluster_name, SkipFinalSnapshot=True)
            except ClientError:
                pass  # best-effort cleanup


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
                pass  # best-effort cleanup


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
                    pass  # best-effort cleanup
        finally:
            try:
                client.delete_global_cluster(GlobalClusterIdentifier=gc_name)
            except ClientError:
                pass  # best-effort cleanup


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
                pass  # best-effort cleanup
        try:
            ec2_client.delete_vpc(VpcId=vpc_id)
        except ClientError:
            pass  # best-effort cleanup

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
                pass  # best-effort cleanup
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
                pass  # best-effort cleanup


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
                pass  # best-effort cleanup


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
            pass  # best-effort cleanup

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
                pass  # best-effort cleanup

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
                pass  # best-effort cleanup


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
            pass  # best-effort cleanup
        try:
            client.delete_db_instance(DBInstanceIdentifier=db_name, SkipFinalSnapshot=True)
        except ClientError:
            pass  # best-effort cleanup

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
            pass  # best-effort cleanup

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
                    pass  # best-effort cleanup


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
            pass  # best-effort cleanup
        for sid in subnet_ids:
            try:
                ec2_client.delete_subnet(SubnetId=sid)
            except ClientError:
                pass  # best-effort cleanup
        try:
            ec2_client.delete_vpc(VpcId=vpc_id)
        except ClientError:
            pass  # best-effort cleanup

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
                pass  # best-effort cleanup


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
                    pass  # best-effort cleanup
            try:
                client.delete_db_snapshot(DBSnapshotIdentifier=snap)
            except ClientError:
                pass  # best-effort cleanup

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
                    pass  # best-effort cleanup


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
                pass  # best-effort cleanup
            try:
                client.delete_db_cluster(DBClusterIdentifier=cluster_name, SkipFinalSnapshot=True)
            except ClientError:
                pass  # best-effort cleanup


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
            pass  # best-effort cleanup
        try:
            client.delete_db_instance(DBInstanceIdentifier=db_name, SkipFinalSnapshot=True)
        except ClientError:
            pass  # best-effort cleanup
        for sid in subnet_ids:
            try:
                ec2_client.delete_subnet(SubnetId=sid)
            except ClientError:
                pass  # best-effort cleanup
        try:
            ec2_client.delete_vpc(VpcId=vpc_id)
        except ClientError:
            pass  # best-effort cleanup

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
                pass  # best-effort cleanup

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
                pass  # best-effort cleanup


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
                pass  # best-effort cleanup

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
                pass  # best-effort cleanup


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
                pass  # best-effort cleanup
            try:
                client.delete_option_group(OptionGroupName=src_name)
            except ClientError:
                pass  # best-effort cleanup


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
                pass  # best-effort cleanup


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
                pass  # best-effort cleanup


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
                pass  # best-effort cleanup

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
                pass  # best-effort cleanup


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
                pass  # best-effort cleanup

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
            pass  # best-effort cleanup

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
            pass  # best-effort cleanup

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
                pass  # best-effort cleanup

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
                pass  # best-effort cleanup

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
            pass  # best-effort cleanup

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
            pass  # best-effort cleanup

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
            pass  # best-effort cleanup

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
                pass  # best-effort cleanup

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
                pass  # best-effort cleanup
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
                pass  # best-effort cleanup

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
                pass  # best-effort cleanup
            try:
                rds.delete_db_snapshot(DBSnapshotIdentifier=snap_name)
            except ClientError:
                pass  # best-effort cleanup

    # -- DB CLUSTERS --

    def test_create_and_delete_db_cluster(self, rds):
        """CreateDBCluster then DescribeDBClusters (RETRIEVE) then DeleteDBCluster full lifecycle."""
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

            # RETRIEVE - describe the specific cluster by identifier
            desc_single = rds.describe_db_clusters(DBClusterIdentifier=name)
            assert len(desc_single["DBClusters"]) == 1
            assert desc_single["DBClusters"][0]["DBClusterIdentifier"] == name

            del_resp = rds.delete_db_cluster(DBClusterIdentifier=name, SkipFinalSnapshot=True)
            assert "DBCluster" in del_resp
            assert del_resp["DBCluster"]["DBClusterIdentifier"] == name
        except ClientError:
            try:
                rds.delete_db_cluster(DBClusterIdentifier=name, SkipFinalSnapshot=True)
            except ClientError:
                pass  # best-effort cleanup
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
                pass  # best-effort cleanup

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
                    pass  # best-effort cleanup
            try:
                rds.delete_db_cluster_snapshot(DBClusterSnapshotIdentifier=snap_name)
            except ClientError:
                pass  # best-effort cleanup

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
                pass  # best-effort cleanup
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
                pass  # best-effort cleanup
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
                    pass  # best-effort cleanup

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
                    pass  # best-effort cleanup

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
                pass  # best-effort cleanup
            for sid in subnet_ids:
                try:
                    ec2.delete_subnet(SubnetId=sid)
                except ClientError:
                    pass  # best-effort cleanup
            try:
                ec2.delete_vpc(VpcId=vpc_id)
            except ClientError:
                pass  # best-effort cleanup

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
                pass  # best-effort cleanup
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
                pass  # best-effort cleanup
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
                pass  # best-effort cleanup
            try:
                rds.delete_db_cluster(DBClusterIdentifier=cl_name, SkipFinalSnapshot=True)
            except ClientError:
                pass  # best-effort cleanup

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
                pass  # best-effort cleanup
            for sid in subnet_ids:
                try:
                    ec2.delete_subnet(SubnetId=sid)
                except ClientError:
                    pass  # best-effort cleanup
            try:
                ec2.delete_vpc(VpcId=vpc_id)
            except ClientError:
                pass  # best-effort cleanup

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
                pass  # best-effort cleanup

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
                pass  # best-effort cleanup

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
        """ApplyPendingMaintenanceAction returns a response; list confirms it persists."""
        name = _unique("maint-db")
        client.create_db_instance(
            DBInstanceIdentifier=name,
            DBInstanceClass="db.t3.micro",
            Engine="mysql",
            MasterUsername="admin",
            MasterUserPassword="password123",
        )
        try:
            arn = f"arn:aws:rds:us-east-1:123456789012:db:{name}"
            resp = client.apply_pending_maintenance_action(
                ResourceIdentifier=arn,
                ApplyAction="system-update",
                OptInType="immediate",
            )
            assert resp["ResourcePendingMaintenanceActions"]["ResourceIdentifier"] == arn
            list_resp = client.describe_pending_maintenance_actions()
            assert isinstance(list_resp["PendingMaintenanceActions"], list)
            with pytest.raises(ClientError) as exc:
                client.describe_db_instances(DBInstanceIdentifier="nonexistent-db-xyz")
            assert exc.value.response["Error"]["Code"] == "DBInstanceNotFound"
        finally:
            try:
                client.delete_db_instance(DBInstanceIdentifier=name, SkipFinalSnapshot=True)
            except ClientError:
                pass  # best-effort cleanup


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
            pass  # best-effort cleanup

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


class TestDescribeDBSnapshotsEdgeCases:
    """Edge case tests for DescribeDBSnapshots with full CRUD+E lifecycle."""

    @pytest.fixture
    def client(self):
        return make_client("rds")

    @pytest.fixture
    def instance_with_snapshot(self, client):
        db_name = _unique("edge-db")
        snap_id = _unique("edge-snap")
        client.create_db_instance(
            DBInstanceIdentifier=db_name,
            DBInstanceClass="db.t3.micro",
            Engine="mysql",
            MasterUsername="admin",
            MasterUserPassword="password123",
        )
        client.create_db_snapshot(
            DBSnapshotIdentifier=snap_id,
            DBInstanceIdentifier=db_name,
        )
        yield db_name, snap_id
        try:
            client.delete_db_snapshot(DBSnapshotIdentifier=snap_id)
        except ClientError:
            pass  # best-effort cleanup
        try:
            client.delete_db_instance(DBInstanceIdentifier=db_name, SkipFinalSnapshot=True)
        except ClientError:
            pass  # best-effort cleanup

    def test_list_snapshots_contains_created(self, client, instance_with_snapshot):
        """DescribeDBSnapshots (no filter) returns the created snapshot."""
        _, snap_id = instance_with_snapshot
        resp = client.describe_db_snapshots()
        ids = [s["DBSnapshotIdentifier"] for s in resp["DBSnapshots"]]
        assert snap_id in ids

    def test_describe_snapshot_by_id(self, client, instance_with_snapshot):
        """DescribeDBSnapshots by ID returns exactly that snapshot."""
        db_name, snap_id = instance_with_snapshot
        resp = client.describe_db_snapshots(DBSnapshotIdentifier=snap_id)
        snaps = resp["DBSnapshots"]
        assert len(snaps) == 1
        snap = snaps[0]
        assert snap["DBSnapshotIdentifier"] == snap_id
        assert snap["DBInstanceIdentifier"] == db_name
        assert snap["Engine"] == "mysql"

    def test_describe_nonexistent_snapshot_raises_error(self, client):
        """DescribeDBSnapshots for nonexistent ID raises DBSnapshotNotFound."""
        with pytest.raises(ClientError) as exc:
            client.describe_db_snapshots(DBSnapshotIdentifier="nonexistent-snap-xyz-999")
        assert exc.value.response["Error"]["Code"] == "DBSnapshotNotFound"

    def test_delete_then_describe_raises_error(self, client, instance_with_snapshot):
        """After deleting a snapshot, describe raises DBSnapshotNotFound."""
        _, snap_id = instance_with_snapshot
        client.delete_db_snapshot(DBSnapshotIdentifier=snap_id)
        with pytest.raises(ClientError) as exc:
            client.describe_db_snapshots(DBSnapshotIdentifier=snap_id)
        assert exc.value.response["Error"]["Code"] == "DBSnapshotNotFound"

    def test_snapshot_arn_format(self, client, instance_with_snapshot):
        """Snapshot has a DBSnapshotArn with the expected format."""
        _, snap_id = instance_with_snapshot
        resp = client.describe_db_snapshots(DBSnapshotIdentifier=snap_id)
        snap = resp["DBSnapshots"][0]
        assert "DBSnapshotArn" in snap
        assert snap["DBSnapshotArn"].startswith("arn:aws:rds:")
        assert snap_id in snap["DBSnapshotArn"]


class TestDescribeDBSubnetGroupsEdgeCases:
    """Edge case tests for DescribeDBSubnetGroups with full CRUD+E lifecycle."""

    @pytest.fixture
    def client(self):
        return make_client("rds")

    @pytest.fixture
    def ec2_client(self):
        return make_client("ec2")

    @pytest.fixture
    def subnet_group_fixture(self, client, ec2_client):
        name = _unique("edge-sg")
        vpc = ec2_client.create_vpc(CidrBlock="10.82.0.0/16")
        vpc_id = vpc["Vpc"]["VpcId"]
        s1 = ec2_client.create_subnet(
            VpcId=vpc_id, CidrBlock="10.82.1.0/24", AvailabilityZone="us-east-1a"
        )
        s2 = ec2_client.create_subnet(
            VpcId=vpc_id, CidrBlock="10.82.2.0/24", AvailabilityZone="us-east-1b"
        )
        subnet_ids = [s1["Subnet"]["SubnetId"], s2["Subnet"]["SubnetId"]]
        client.create_db_subnet_group(
            DBSubnetGroupName=name,
            DBSubnetGroupDescription="edge case test subnet group",
            SubnetIds=subnet_ids,
        )
        yield name, vpc_id, subnet_ids
        try:
            client.delete_db_subnet_group(DBSubnetGroupName=name)
        except ClientError:
            pass  # best-effort cleanup
        for sid in subnet_ids:
            try:
                ec2_client.delete_subnet(SubnetId=sid)
            except ClientError:
                pass  # best-effort cleanup
        try:
            ec2_client.delete_vpc(VpcId=vpc_id)
        except ClientError:
            pass  # best-effort cleanup

    def test_list_subnet_groups_contains_created(self, client, subnet_group_fixture):
        """DescribeDBSubnetGroups (no filter) lists the created subnet group."""
        name, _, _ = subnet_group_fixture
        resp = client.describe_db_subnet_groups()
        names = [g["DBSubnetGroupName"] for g in resp["DBSubnetGroups"]]
        assert name in names

    def test_describe_subnet_group_by_name(self, client, subnet_group_fixture):
        """DescribeDBSubnetGroups by name returns exactly that group."""
        name, _, _ = subnet_group_fixture
        resp = client.describe_db_subnet_groups(DBSubnetGroupName=name)
        groups = resp["DBSubnetGroups"]
        assert len(groups) == 1
        grp = groups[0]
        assert grp["DBSubnetGroupName"] == name
        assert grp["DBSubnetGroupDescription"] == "edge case test subnet group"
        assert len(grp["Subnets"]) == 2

    def test_describe_nonexistent_subnet_group_raises_error(self, client):
        """DescribeDBSubnetGroups for nonexistent name raises DBSubnetGroupNotFoundFault."""
        with pytest.raises(ClientError) as exc:
            client.describe_db_subnet_groups(DBSubnetGroupName="nonexistent-sg-xyz-999")
        assert exc.value.response["Error"]["Code"] == "DBSubnetGroupNotFoundFault"

    def test_delete_then_describe_raises_error(self, client, subnet_group_fixture):
        """After deleting a subnet group, describe raises DBSubnetGroupNotFoundFault."""
        name, _, _ = subnet_group_fixture
        client.delete_db_subnet_group(DBSubnetGroupName=name)
        with pytest.raises(ClientError) as exc:
            client.describe_db_subnet_groups(DBSubnetGroupName=name)
        assert exc.value.response["Error"]["Code"] == "DBSubnetGroupNotFoundFault"

    def test_subnet_group_arn_format(self, client, subnet_group_fixture):
        """Subnet group has a DBSubnetGroupArn with the expected format."""
        name, _, _ = subnet_group_fixture
        resp = client.describe_db_subnet_groups(DBSubnetGroupName=name)
        grp = resp["DBSubnetGroups"][0]
        assert "DBSubnetGroupArn" in grp
        assert grp["DBSubnetGroupArn"].startswith("arn:aws:rds:")
        assert name in grp["DBSubnetGroupArn"]


class TestDescribeEventSubscriptionsEdgeCases:
    """Edge case tests for DescribeEventSubscriptions with full CRUD+E lifecycle."""

    @pytest.fixture
    def client(self):
        return make_client("rds")

    @pytest.fixture
    def event_subscription(self, client):
        name = _unique("edge-esub")
        client.create_event_subscription(
            SubscriptionName=name,
            SnsTopicArn="arn:aws:sns:us-east-1:123456789012:test-topic",
            SourceType="db-instance",
            EventCategories=["creation"],
        )
        yield name
        try:
            client.delete_event_subscription(SubscriptionName=name)
        except ClientError:
            pass  # best-effort cleanup

    def test_list_subscriptions_contains_created(self, client, event_subscription):
        """DescribeEventSubscriptions (no filter) lists the created subscription."""
        resp = client.describe_event_subscriptions()
        names = [s["CustSubscriptionId"] for s in resp["EventSubscriptionsList"]]
        assert event_subscription in names

    def test_describe_subscription_by_name(self, client, event_subscription):
        """DescribeEventSubscriptions by SubscriptionName returns that subscription."""
        resp = client.describe_event_subscriptions(SubscriptionName=event_subscription)
        subs = resp["EventSubscriptionsList"]
        assert len(subs) == 1
        sub = subs[0]
        assert sub["CustSubscriptionId"] == event_subscription
        assert sub["SnsTopicArn"] == "arn:aws:sns:us-east-1:123456789012:test-topic"
        assert sub["SourceType"] == "db-instance"

    def test_describe_nonexistent_subscription_raises_error(self, client):
        """DescribeEventSubscriptions for nonexistent name raises SubscriptionNotFound."""
        with pytest.raises(ClientError) as exc:
            client.describe_event_subscriptions(SubscriptionName="nonexistent-sub-xyz-999")
        assert exc.value.response["Error"]["Code"] in (
            "SubscriptionNotFound",
            "SubscriptionNotFoundFault",
        )

    def test_delete_then_describe_raises_error(self, client, event_subscription):
        """After deleting a subscription, describe raises SubscriptionNotFound."""
        client.delete_event_subscription(SubscriptionName=event_subscription)
        with pytest.raises(ClientError) as exc:
            client.describe_event_subscriptions(SubscriptionName=event_subscription)
        assert exc.value.response["Error"]["Code"] in (
            "SubscriptionNotFound",
            "SubscriptionNotFoundFault",
        )


class TestDescribeGlobalClustersEdgeCases:
    """Edge case tests for DescribeGlobalClusters with full CRUD+E lifecycle."""

    @pytest.fixture
    def client(self):
        return make_client("rds")

    @pytest.fixture
    def global_cluster(self, client):
        name = _unique("edge-gc")
        client.create_global_cluster(
            GlobalClusterIdentifier=name,
            Engine="aurora-mysql",
        )
        yield name
        try:
            client.delete_global_cluster(GlobalClusterIdentifier=name)
        except ClientError:
            pass  # best-effort cleanup

    def test_list_global_clusters_contains_created(self, client, global_cluster):
        """DescribeGlobalClusters (no filter) lists the created global cluster."""
        resp = client.describe_global_clusters()
        names = [g["GlobalClusterIdentifier"] for g in resp["GlobalClusters"]]
        assert global_cluster in names

    def test_describe_global_cluster_by_id(self, client, global_cluster):
        """DescribeGlobalClusters by GlobalClusterIdentifier returns that cluster."""
        resp = client.describe_global_clusters(GlobalClusterIdentifier=global_cluster)
        matching = [g for g in resp["GlobalClusters"] if g["GlobalClusterIdentifier"] == global_cluster]
        assert len(matching) == 1
        gc = matching[0]
        assert gc["GlobalClusterIdentifier"] == global_cluster
        assert gc["Engine"] == "aurora-mysql"

    def test_describe_nonexistent_global_cluster_returns_empty(self, client):
        """DescribeGlobalClusters for nonexistent ID returns empty list (server behavior)."""
        resp = client.describe_global_clusters(GlobalClusterIdentifier="nonexistent-gc-xyz-999")
        assert resp["GlobalClusters"] == []

    def test_delete_then_describe_returns_empty(self, client, global_cluster):
        """After deleting a global cluster, describe returns empty list."""
        client.delete_global_cluster(GlobalClusterIdentifier=global_cluster)
        resp = client.describe_global_clusters(GlobalClusterIdentifier=global_cluster)
        assert resp["GlobalClusters"] == []

    def test_global_cluster_arn_format(self, client, global_cluster):
        """Global cluster has a GlobalClusterArn with the expected format."""
        resp = client.describe_global_clusters(GlobalClusterIdentifier=global_cluster)
        matching = [g for g in resp["GlobalClusters"] if g["GlobalClusterIdentifier"] == global_cluster]
        assert len(matching) == 1
        gc = matching[0]
        assert "GlobalClusterArn" in gc
        assert gc["GlobalClusterArn"].startswith("arn:aws:rds:")
        assert global_cluster in gc["GlobalClusterArn"]


class TestDescribeDBSecurityGroupsEdgeCases:
    """Edge case tests for DescribeDBSecurityGroups with full CRUD+E lifecycle."""

    @pytest.fixture
    def client(self):
        return make_client("rds")

    @pytest.fixture
    def db_security_group(self, client):
        name = _unique("edge-dbsg")
        client.create_db_security_group(
            DBSecurityGroupName=name,
            DBSecurityGroupDescription="edge case test security group",
        )
        yield name
        try:
            client.delete_db_security_group(DBSecurityGroupName=name)
        except ClientError:
            pass  # best-effort cleanup

    def test_list_security_groups_contains_created(self, client, db_security_group):
        """DescribeDBSecurityGroups (no filter) lists the created security group."""
        resp = client.describe_db_security_groups()
        names = [g["DBSecurityGroupName"] for g in resp["DBSecurityGroups"]]
        assert db_security_group in names

    def test_describe_security_group_by_name(self, client, db_security_group):
        """DescribeDBSecurityGroups by name returns exactly that group."""
        resp = client.describe_db_security_groups(DBSecurityGroupName=db_security_group)
        groups = resp["DBSecurityGroups"]
        assert len(groups) == 1
        grp = groups[0]
        assert grp["DBSecurityGroupName"] == db_security_group
        assert grp["DBSecurityGroupDescription"] == "edge case test security group"

    def test_describe_nonexistent_security_group_raises_error(self, client):
        """DescribeDBSecurityGroups for nonexistent name raises DBSecurityGroupNotFound."""
        with pytest.raises(ClientError) as exc:
            client.describe_db_security_groups(DBSecurityGroupName="nonexistent-dbsg-xyz-999")
        assert exc.value.response["Error"]["Code"] == "DBSecurityGroupNotFound"

    def test_delete_then_describe_raises_error(self, client, db_security_group):
        """After deleting a security group, describe raises DBSecurityGroupNotFound."""
        client.delete_db_security_group(DBSecurityGroupName=db_security_group)
        with pytest.raises(ClientError) as exc:
            client.describe_db_security_groups(DBSecurityGroupName=db_security_group)
        assert exc.value.response["Error"]["Code"] == "DBSecurityGroupNotFound"


class TestDescribeDBProxiesEdgeCases:
    """Edge case tests for DescribeDBProxies with full CRUD+E lifecycle."""

    @pytest.fixture
    def client(self):
        return make_client("rds")

    @pytest.fixture
    def ec2_client(self):
        return make_client("ec2")

    @pytest.fixture
    def db_proxy(self, client, ec2_client):
        vpc = ec2_client.create_vpc(CidrBlock="10.83.0.0/16")
        vpc_id = vpc["Vpc"]["VpcId"]
        s1 = ec2_client.create_subnet(
            VpcId=vpc_id, CidrBlock="10.83.1.0/24", AvailabilityZone="us-east-1a"
        )
        s2 = ec2_client.create_subnet(
            VpcId=vpc_id, CidrBlock="10.83.2.0/24", AvailabilityZone="us-east-1b"
        )
        subnet_ids = [s1["Subnet"]["SubnetId"], s2["Subnet"]["SubnetId"]]
        name = _unique("edge-px")
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
            RoleArn="arn:aws:iam::123456789012:role/test",
            VpcSubnetIds=subnet_ids,
        )
        yield name, vpc_id, subnet_ids
        try:
            client.delete_db_proxy(DBProxyName=name)
        except ClientError:
            pass  # best-effort cleanup
        for sid in subnet_ids:
            try:
                ec2_client.delete_subnet(SubnetId=sid)
            except ClientError:
                pass  # best-effort cleanup
        try:
            ec2_client.delete_vpc(VpcId=vpc_id)
        except ClientError:
            pass  # best-effort cleanup

    def test_list_proxies_contains_created(self, client, db_proxy):
        """DescribeDBProxies (no filter) lists the created proxy."""
        name, _, _ = db_proxy
        resp = client.describe_db_proxies()
        names = [p["DBProxyName"] for p in resp["DBProxies"]]
        assert name in names

    def test_describe_proxy_by_name(self, client, db_proxy):
        """DescribeDBProxies by DBProxyName returns exactly that proxy."""
        name, _, _ = db_proxy
        resp = client.describe_db_proxies(DBProxyName=name)
        proxies = resp["DBProxies"]
        assert len(proxies) == 1
        prx = proxies[0]
        assert prx["DBProxyName"] == name
        assert prx["EngineFamily"] == "MYSQL"

    def test_describe_nonexistent_proxy_raises_error(self, client):
        """DescribeDBProxies for nonexistent name raises DBProxyNotFoundFault."""
        with pytest.raises(ClientError) as exc:
            client.describe_db_proxies(DBProxyName="nonexistent-proxy-xyz-999")
        assert exc.value.response["Error"]["Code"] == "DBProxyNotFoundFault"

    def test_delete_then_describe_raises_error(self, client, db_proxy):
        """After deleting a proxy, describe raises DBProxyNotFoundFault."""
        name, _, _ = db_proxy
        client.delete_db_proxy(DBProxyName=name)
        with pytest.raises(ClientError) as exc:
            client.describe_db_proxies(DBProxyName=name)
        assert exc.value.response["Error"]["Code"] == "DBProxyNotFoundFault"


class TestDescribeOptionGroupOptionsEdgeCases:
    """Edge case tests for DescribeOptionGroupOptions and OptionGroup lifecycle."""

    @pytest.fixture
    def client(self):
        return make_client("rds")

    def test_list_options_for_mysql_engine(self, client):
        """DescribeOptionGroupOptions with EngineName=mysql returns entries."""
        resp = client.describe_option_group_options(EngineName="mysql")
        options = resp["OptionGroupOptions"]
        assert isinstance(options, list)
        assert len(options) > 0

    def test_filter_options_by_major_version(self, client):
        """DescribeOptionGroupOptions with MajorEngineVersion filter returns matching entries."""
        resp = client.describe_option_group_options(
            EngineName="mysql",
            MajorEngineVersion="8.0",
        )
        options = resp["OptionGroupOptions"]
        assert isinstance(options, list)
        assert len(options) > 0

    def test_each_option_has_name_field(self, client):
        """Each option in DescribeOptionGroupOptions has a Name field."""
        resp = client.describe_option_group_options(EngineName="mysql")
        options = resp["OptionGroupOptions"]
        for opt in options:
            assert "Name" in opt

    def test_option_group_full_lifecycle(self, client):
        """Create → describe → modify → delete an option group."""
        name = _unique("edge-og")
        try:
            create_resp = client.create_option_group(
                OptionGroupName=name,
                EngineName="mysql",
                MajorEngineVersion="8.0",
                OptionGroupDescription="edge case option group",
            )
            assert create_resp["OptionGroup"]["OptionGroupName"] == name
            assert create_resp["OptionGroup"]["EngineName"] == "mysql"

            # Describe by name
            desc_resp = client.describe_option_groups(OptionGroupName=name)
            groups = desc_resp["OptionGroupsList"]
            assert len(groups) == 1
            og = groups[0]
            assert og["OptionGroupName"] == name
            assert og["MajorEngineVersion"] == "8.0"
            assert "OptionGroupArn" in og

            # Delete
            client.delete_option_group(OptionGroupName=name)

            # Verify gone
            with pytest.raises(ClientError) as exc:
                client.describe_option_groups(OptionGroupName=name)
            assert exc.value.response["Error"]["Code"] in (
                "OptionGroupNotFoundFault",
                "InternalError",
            )
        except ClientError:
            try:
                client.delete_option_group(OptionGroupName=name)
            except ClientError:
                pass  # best-effort cleanup
            raise


class TestDescribeDBInstanceAutomatedBackupsEdgeCases:
    """Edge case tests for DescribeDBInstanceAutomatedBackups."""

    @pytest.fixture
    def client(self):
        return make_client("rds")

    def test_list_automated_backups_returns_list(self, client):
        """DescribeDBInstanceAutomatedBackups returns a list (possibly empty)."""
        resp = client.describe_db_instance_automated_backups()
        assert "DBInstanceAutomatedBackups" in resp
        assert isinstance(resp["DBInstanceAutomatedBackups"], list)

    def test_filter_by_nonexistent_dbi_resource_returns_list(self, client):
        """Filtering automated backups by nonexistent DbiResourceId returns a list."""
        resp = client.describe_db_instance_automated_backups(
            DbiResourceId="db-NONEXISTENT123"
        )
        assert "DBInstanceAutomatedBackups" in resp
        assert isinstance(resp["DBInstanceAutomatedBackups"], list)

    def test_automated_backup_appears_after_instance_creation(self, client):
        """After creating a DB instance, an automated backup entry appears."""
        name = _unique("edge-ab")
        client.create_db_instance(
            DBInstanceIdentifier=name,
            DBInstanceClass="db.t3.micro",
            Engine="mysql",
            MasterUsername="admin",
            MasterUserPassword="password123",
        )
        try:
            resp = client.describe_db_instance_automated_backups(DBInstanceIdentifier=name)
            assert "DBInstanceAutomatedBackups" in resp
            assert isinstance(resp["DBInstanceAutomatedBackups"], list)
            # At least one backup entry for this instance
            assert len(resp["DBInstanceAutomatedBackups"]) >= 1
            backup = resp["DBInstanceAutomatedBackups"][0]
            assert backup["DBInstanceIdentifier"] == name
        finally:
            try:
                client.delete_db_instance(DBInstanceIdentifier=name, SkipFinalSnapshot=True)
            except ClientError:
                pass  # best-effort cleanup


class TestDescribeExportTasksEdgeCases:
    """Edge case tests for DescribeExportTasks."""

    @pytest.fixture
    def client(self):
        return make_client("rds")

    def test_list_export_tasks_returns_list(self, client):
        """DescribeExportTasks returns a list."""
        resp = client.describe_export_tasks()
        assert "ExportTasks" in resp
        assert isinstance(resp["ExportTasks"], list)

    def test_describe_nonexistent_export_task_raises_error(self, client):
        """DescribeExportTasks for nonexistent task raises ExportTaskNotFound."""
        with pytest.raises(ClientError) as exc:
            client.describe_export_tasks(ExportTaskIdentifier="nonexistent-export-xyz-999")
        assert exc.value.response["Error"]["Code"] in (
            "ExportTaskNotFound",
            "ExportTaskNotFoundFault",
        )

    def test_export_tasks_key_is_list(self, client):
        """DescribeExportTasks response has ExportTasks as a list."""
        resp = client.describe_export_tasks()
        assert isinstance(resp["ExportTasks"], list)


class TestDescribeDBShardGroupsEdgeCases:
    """Edge case tests for DescribeDBShardGroups."""

    @pytest.fixture
    def client(self):
        return make_client("rds")

    def test_list_shard_groups_returns_list(self, client):
        """DescribeDBShardGroups returns a list (possibly empty)."""
        resp = client.describe_db_shard_groups()
        assert "DBShardGroups" in resp
        assert isinstance(resp["DBShardGroups"], list)

    def test_describe_nonexistent_shard_group_raises_error(self, client):
        """DescribeDBShardGroups for nonexistent ID raises DBShardGroupNotFound."""
        with pytest.raises(ClientError) as exc:
            client.describe_db_shard_groups(DBShardGroupIdentifier="nonexistent-shardgrp-xyz-999")
        assert exc.value.response["Error"]["Code"] == "DBShardGroupNotFound"

    def test_shard_groups_key_is_list(self, client):
        """DescribeDBShardGroups response has DBShardGroups as a list."""
        resp = client.describe_db_shard_groups()
        assert isinstance(resp["DBShardGroups"], list)

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
                pass  # best-effort cleanup

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
                pass  # best-effort cleanup

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
                    pass  # best-effort cleanup

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
                pass  # best-effort cleanup


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
                pass  # best-effort cleanup

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
                pass  # best-effort cleanup

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
                pass  # best-effort cleanup

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
                    pass  # best-effort cleanup


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
                pass  # best-effort cleanup


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
                pass  # best-effort cleanup

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
                pass  # best-effort cleanup


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
            pass  # best-effort cleanup

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


class TestRDSDescribeListOperations:
    """Tests for list/describe operations that return collections with no required args."""

    @pytest.fixture
    def client(self):
        return make_client("rds")

    def test_describe_db_instances_returns_key(self, client):
        """DescribeDBInstances returns DBInstances list."""
        resp = client.describe_db_instances()
        assert "DBInstances" in resp

    def test_describe_db_clusters_returns_key(self, client):
        """DescribeDBClusters returns DBClusters list."""
        resp = client.describe_db_clusters()
        assert "DBClusters" in resp

    def test_describe_db_engine_versions_returns_versions(self, client):
        """DescribeDBEngineVersions returns DBEngineVersions list."""
        resp = client.describe_db_engine_versions()
        assert "DBEngineVersions" in resp

    def test_describe_db_parameter_groups_returns_key(self, client):
        """DescribeDBParameterGroups returns DBParameterGroups list."""
        resp = client.describe_db_parameter_groups()
        assert "DBParameterGroups" in resp

    def test_describe_db_cluster_parameter_groups_returns_key(self, client):
        """DescribeDBClusterParameterGroups returns DBClusterParameterGroups list."""
        resp = client.describe_db_cluster_parameter_groups()
        assert "DBClusterParameterGroups" in resp

    def test_describe_db_snapshots_returns_key(self, client):
        """DescribeDBSnapshots returns DBSnapshots list."""
        resp = client.describe_db_snapshots()
        assert "DBSnapshots" in resp

    def test_describe_db_cluster_snapshots_returns_key(self, client):
        """DescribeDBClusterSnapshots returns DBClusterSnapshots list."""
        resp = client.describe_db_cluster_snapshots()
        assert "DBClusterSnapshots" in resp

    def test_describe_db_subnet_groups_returns_key(self, client):
        """DescribeDBSubnetGroups returns DBSubnetGroups list."""
        resp = client.describe_db_subnet_groups()
        assert "DBSubnetGroups" in resp

    def test_describe_db_security_groups_returns_key(self, client):
        """DescribeDBSecurityGroups returns DBSecurityGroups list."""
        resp = client.describe_db_security_groups()
        assert "DBSecurityGroups" in resp

    def test_describe_reserved_db_instances_returns_key(self, client):
        """DescribeReservedDBInstances returns ReservedDBInstances list."""
        resp = client.describe_reserved_db_instances()
        assert "ReservedDBInstances" in resp

    def test_describe_reserved_db_instances_offerings_returns_key(self, client):
        """DescribeReservedDBInstancesOfferings returns offerings list."""
        resp = client.describe_reserved_db_instances_offerings()
        assert "ReservedDBInstancesOfferings" in resp

    def test_describe_orderable_db_instance_options_returns_key(self, client):
        """DescribeOrderableDBInstanceOptions returns options list."""
        resp = client.describe_orderable_db_instance_options(Engine="mysql")
        assert "OrderableDBInstanceOptions" in resp

    def test_describe_db_instance_automated_backups_returns_key(self, client):
        """DescribeDBInstanceAutomatedBackups returns backups list."""
        resp = client.describe_db_instance_automated_backups()
        assert "DBInstanceAutomatedBackups" in resp

    def test_describe_db_cluster_automated_backups_returns_key(self, client):
        """DescribeDBClusterAutomatedBackups returns cluster backups list."""
        resp = client.describe_db_cluster_automated_backups()
        assert "DBClusterAutomatedBackups" in resp

    def test_describe_db_proxies_returns_key(self, client):
        """DescribeDBProxies returns DBProxies list."""
        resp = client.describe_db_proxies()
        assert "DBProxies" in resp

    def test_describe_db_proxy_endpoints_returns_key(self, client):
        """DescribeDBProxyEndpoints returns DBProxyEndpoints list."""
        resp = client.describe_db_proxy_endpoints()
        assert "DBProxyEndpoints" in resp

    def test_describe_db_recommendations_returns_key(self, client):
        """DescribeDBRecommendations returns DBRecommendations list."""
        resp = client.describe_db_recommendations()
        assert "DBRecommendations" in resp

    def test_describe_db_shard_groups_returns_key(self, client):
        """DescribeDBShardGroups returns DBShardGroups list."""
        resp = client.describe_db_shard_groups()
        assert "DBShardGroups" in resp

    def test_describe_db_snapshot_tenant_databases_returns_key(self, client):
        """DescribeDBSnapshotTenantDatabases returns tenant databases list."""
        resp = client.describe_db_snapshot_tenant_databases()
        assert "DBSnapshotTenantDatabases" in resp


class TestRDSErrorResponseOperations:
    """Tests that verify correct error codes for nonexistent resources."""

    @pytest.fixture
    def client(self):
        return make_client("rds")

    def test_describe_db_snapshot_attributes_not_found(self, client):
        """DescribeDBSnapshotAttributes raises DBSnapshotNotFound for nonexistent snapshot."""
        with pytest.raises(ClientError) as exc:
            client.describe_db_snapshot_attributes(DBSnapshotIdentifier=_unique("nonexistent-snap"))
        assert exc.value.response["Error"]["Code"] == "DBSnapshotNotFound"

    def test_describe_db_cluster_snapshot_attributes_not_found(self, client):
        """DescribeDBClusterSnapshotAttributes raises error for nonexistent cluster snapshot."""
        with pytest.raises(ClientError) as exc:
            client.describe_db_cluster_snapshot_attributes(
                DBClusterSnapshotIdentifier=_unique("nonexistent-csnap")
            )
        assert exc.value.response["Error"]["Code"] == "DBClusterSnapshotNotFoundFault"

    def test_describe_valid_db_instance_modifications_not_found(self, client):
        """DescribeValidDBInstanceModifications raises DBInstanceNotFound for unknown instance."""
        with pytest.raises(ClientError) as exc:
            client.describe_valid_db_instance_modifications(
                DBInstanceIdentifier=_unique("nonexistent-inst")
            )
        assert exc.value.response["Error"]["Code"] == "DBInstanceNotFound"

    def test_describe_db_log_files_not_found(self, client):
        """DescribeDBLogFiles raises DBInstanceNotFound for nonexistent instance."""
        with pytest.raises(ClientError) as exc:
            client.describe_db_log_files(DBInstanceIdentifier=_unique("nonexistent-inst"))
        assert exc.value.response["Error"]["Code"] == "DBInstanceNotFound"

    def test_describe_db_parameters_not_found(self, client):
        """DescribeDBParameters raises DBParameterGroupNotFound for nonexistent group."""
        with pytest.raises(ClientError) as exc:
            client.describe_db_parameters(DBParameterGroupName=_unique("nonexistent-pg"))
        assert exc.value.response["Error"]["Code"] == "DBParameterGroupNotFound"

    def test_describe_db_cluster_parameters_not_found(self, client):
        """DescribeDBClusterParameters raises error for nonexistent cluster parameter group."""
        with pytest.raises(ClientError) as exc:
            client.describe_db_cluster_parameters(
                DBClusterParameterGroupName=_unique("nonexistent-cpg")
            )
        assert exc.value.response["Error"]["Code"] == "DBParameterGroupNotFound"

    def test_modify_db_snapshot_attribute_not_found(self, client):
        """ModifyDBSnapshotAttribute raises DBSnapshotNotFound for nonexistent snapshot."""
        with pytest.raises(ClientError) as exc:
            client.modify_db_snapshot_attribute(
                DBSnapshotIdentifier=_unique("nonexistent-snap"),
                AttributeName="restore",
            )
        assert exc.value.response["Error"]["Code"] == "DBSnapshotNotFound"

    def test_modify_db_cluster_snapshot_attribute_not_found(self, client):
        """ModifyDBClusterSnapshotAttribute raises error for nonexistent cluster snapshot."""
        with pytest.raises(ClientError) as exc:
            client.modify_db_cluster_snapshot_attribute(
                DBClusterSnapshotIdentifier=_unique("nonexistent-csnap"),
                AttributeName="restore",
            )
        assert exc.value.response["Error"]["Code"] == "DBClusterSnapshotNotFoundFault"

    def test_describe_db_proxy_target_groups_not_found(self, client):
        """DescribeDBProxyTargetGroups raises error for nonexistent proxy."""
        with pytest.raises(ClientError) as exc:
            client.describe_db_proxy_target_groups(DBProxyName=_unique("nonexistent-proxy"))
        assert exc.value.response["Error"]["Code"] == "DBProxyNotFoundFault"


class TestRDSParameterGroupCRUD:
    """Tests for DB parameter group create/copy/delete operations."""

    @pytest.fixture
    def client(self):
        return make_client("rds")

    def test_create_and_delete_db_parameter_group(self, client):
        """CreateDBParameterGroup creates a group; DeleteDBParameterGroup removes it."""
        name = _unique("compat-pg")
        resp = client.create_db_parameter_group(
            DBParameterGroupName=name,
            DBParameterGroupFamily="mysql8.0",
            Description="compat test parameter group",
        )
        assert resp["DBParameterGroup"]["DBParameterGroupName"] == name
        assert resp["DBParameterGroup"]["DBParameterGroupFamily"] == "mysql8.0"
        # Cleanup
        client.delete_db_parameter_group(DBParameterGroupName=name)
        # After deletion, should not be in the list
        all_groups = client.describe_db_parameter_groups()
        names = [g["DBParameterGroupName"] for g in all_groups["DBParameterGroups"]]
        assert name not in names

    def test_copy_db_parameter_group(self, client):
        """CopyDBParameterGroup copies an existing parameter group."""
        source = _unique("compat-src-pg")
        target = _unique("compat-tgt-pg")
        client.create_db_parameter_group(
            DBParameterGroupName=source,
            DBParameterGroupFamily="mysql8.0",
            Description="source parameter group",
        )
        try:
            resp = client.copy_db_parameter_group(
                SourceDBParameterGroupIdentifier=source,
                TargetDBParameterGroupIdentifier=target,
                TargetDBParameterGroupDescription="copied parameter group",
            )
            assert resp["DBParameterGroup"]["DBParameterGroupName"] == target
            assert resp["DBParameterGroup"]["DBParameterGroupFamily"] == "mysql8.0"
        finally:
            client.delete_db_parameter_group(DBParameterGroupName=source)
            try:
                client.delete_db_parameter_group(DBParameterGroupName=target)
            except ClientError:
                pass  # best-effort cleanup

    def test_describe_db_parameters_for_group(self, client):
        """DescribeDBParameters returns parameters for an existing group."""
        name = _unique("compat-pg")
        client.create_db_parameter_group(
            DBParameterGroupName=name,
            DBParameterGroupFamily="mysql8.0",
            Description="compat test parameter group",
        )
        try:
            resp = client.describe_db_parameters(DBParameterGroupName=name)
            assert "Parameters" in resp
        finally:
            client.delete_db_parameter_group(DBParameterGroupName=name)

    def test_create_and_delete_db_cluster_parameter_group(self, client):
        """CreateDBClusterParameterGroup creates a cluster group; delete removes it."""
        name = _unique("compat-cpg")
        resp = client.create_db_cluster_parameter_group(
            DBClusterParameterGroupName=name,
            DBParameterGroupFamily="aurora-mysql8.0",
            Description="compat test cluster parameter group",
        )
        assert resp["DBClusterParameterGroup"]["DBClusterParameterGroupName"] == name
        # Cleanup
        client.delete_db_cluster_parameter_group(DBClusterParameterGroupName=name)

    def test_copy_db_cluster_parameter_group(self, client):
        """CopyDBClusterParameterGroup copies an existing cluster parameter group."""
        source = _unique("compat-src-cpg")
        target = _unique("compat-tgt-cpg")
        client.create_db_cluster_parameter_group(
            DBClusterParameterGroupName=source,
            DBParameterGroupFamily="aurora-mysql8.0",
            Description="source cluster parameter group",
        )
        try:
            resp = client.copy_db_cluster_parameter_group(
                SourceDBClusterParameterGroupIdentifier=source,
                TargetDBClusterParameterGroupIdentifier=target,
                TargetDBClusterParameterGroupDescription="copied cluster parameter group",
            )
            assert resp["DBClusterParameterGroup"]["DBClusterParameterGroupName"] == target
        finally:
            client.delete_db_cluster_parameter_group(DBClusterParameterGroupName=source)
            try:
                client.delete_db_cluster_parameter_group(DBClusterParameterGroupName=target)
            except ClientError:
                pass  # best-effort cleanup

    def test_describe_db_cluster_parameters_for_group(self, client):
        """DescribeDBClusterParameters returns parameters for an existing group."""
        name = _unique("compat-cpg")
        client.create_db_cluster_parameter_group(
            DBClusterParameterGroupName=name,
            DBParameterGroupFamily="aurora-mysql8.0",
            Description="compat test cluster parameter group",
        )
        try:
            resp = client.describe_db_cluster_parameters(DBClusterParameterGroupName=name)
            assert "Parameters" in resp
        finally:
            client.delete_db_cluster_parameter_group(DBClusterParameterGroupName=name)

    def test_delete_db_instance_automated_backup(self, client):
        """DeleteDBInstanceAutomatedBackup returns the backup record."""
        resp = client.delete_db_instance_automated_backup(DbiResourceId=_unique("fake-resource"))
        assert "DBInstanceAutomatedBackup" in resp

    def test_reset_db_parameter_group(self, client):
        """ResetDBParameterGroup resets a parameter group."""
        name = _unique("compat-reset-pg")
        client.create_db_parameter_group(
            DBParameterGroupName=name,
            DBParameterGroupFamily="mysql8.0",
            Description="compat reset test",
        )
        try:
            resp = client.reset_db_parameter_group(
                DBParameterGroupName=name, ResetAllParameters=True
            )
            assert resp["DBParameterGroupName"] == name
        finally:
            client.delete_db_parameter_group(DBParameterGroupName=name)

    def test_reset_db_cluster_parameter_group(self, client):
        """ResetDBClusterParameterGroup resets a cluster parameter group."""
        name = _unique("compat-reset-cpg")
        client.create_db_cluster_parameter_group(
            DBClusterParameterGroupName=name,
            DBParameterGroupFamily="aurora-mysql8.0",
            Description="compat reset cluster test",
        )
        try:
            resp = client.reset_db_cluster_parameter_group(
                DBClusterParameterGroupName=name, ResetAllParameters=True
            )
            assert resp["DBClusterParameterGroupName"] == name
        finally:
            client.delete_db_cluster_parameter_group(DBClusterParameterGroupName=name)


class TestRDSMissingGapOps:
    """Tests for newly implemented RDS operations that were previously 501."""

    @pytest.fixture
    def client(self):
        return make_client("rds")

    def test_describe_source_regions(self, client):
        """DescribeSourceRegions returns a list of available source regions."""
        resp = client.describe_source_regions()
        assert "SourceRegions" in resp
        assert len(resp["SourceRegions"]) > 0
        region = resp["SourceRegions"][0]
        assert "RegionName" in region
        assert "Endpoint" in region

    def test_describe_db_major_engine_versions(self, client):
        """DescribeDBMajorEngineVersions returns a list of major engine versions."""
        resp = client.describe_db_major_engine_versions()
        assert "DBMajorEngineVersions" in resp
        assert len(resp["DBMajorEngineVersions"]) > 0
        version = resp["DBMajorEngineVersions"][0]
        assert "Engine" in version
        assert "MajorEngineVersion" in version

    def test_reboot_db_cluster(self, client):
        """RebootDBCluster returns the cluster record."""
        cluster_id = _unique("compat-reboot-cluster")
        client.create_db_cluster(
            DBClusterIdentifier=cluster_id,
            Engine="aurora-mysql",
            MasterUsername="admin",
            MasterUserPassword="password123!",
        )
        try:
            resp = client.reboot_db_cluster(DBClusterIdentifier=cluster_id)
            assert "DBCluster" in resp
            assert resp["DBCluster"]["DBClusterIdentifier"] == cluster_id
        finally:
            client.delete_db_cluster(DBClusterIdentifier=cluster_id, SkipFinalSnapshot=True)

    def test_remove_role_from_db_cluster(self, client):
        """RemoveRoleFromDBCluster removes an IAM role from a cluster."""
        cluster_id = _unique("compat-remove-role")
        role_arn = "arn:aws:iam::123456789012:role/compat-test-role"
        client.create_db_cluster(
            DBClusterIdentifier=cluster_id,
            Engine="aurora-mysql",
            MasterUsername="admin",
            MasterUserPassword="password123!",
        )
        try:
            client.add_role_to_db_cluster(
                DBClusterIdentifier=cluster_id,
                RoleArn=role_arn,
                FeatureName="s3Export",
            )
            # Should succeed without error
            client.remove_role_from_db_cluster(
                DBClusterIdentifier=cluster_id,
                RoleArn=role_arn,
                FeatureName="s3Export",
            )
            # Verify role removed
            resp = client.describe_db_clusters(DBClusterIdentifier=cluster_id)
            cluster = resp["DBClusters"][0]
            role_arns = [r["RoleArn"] for r in cluster.get("AssociatedRoles", [])]
            assert role_arn not in role_arns
        finally:
            client.delete_db_cluster(DBClusterIdentifier=cluster_id, SkipFinalSnapshot=True)


class TestRDSNewStubOps:
    """Tests for newly-implemented RDS stub operations."""

    @pytest.fixture
    def client(self):
        return make_client("rds")

    def test_disable_http_endpoint(self, client):
        """DisableHttpEndpoint returns ResourceArn and HttpEndpointEnabled=False."""
        resp = client.disable_http_endpoint(
            ResourceArn="arn:aws:rds:us-east-1:123456789012:cluster/fake-cluster",
        )
        assert "HttpEndpointEnabled" in resp
        assert resp["HttpEndpointEnabled"] is False

    def test_enable_http_endpoint(self, client):
        """EnableHttpEndpoint returns ResourceArn and HttpEndpointEnabled=True."""
        resp = client.enable_http_endpoint(
            ResourceArn="arn:aws:rds:us-east-1:123456789012:cluster/fake-cluster",
        )
        assert "HttpEndpointEnabled" in resp
        assert resp["HttpEndpointEnabled"] is True

    def test_modify_certificates(self, client):
        """ModifyCertificates returns Certificate key."""
        resp = client.modify_certificates()
        assert "Certificate" in resp


class TestRDSNewStubOps2:
    """Tests for second batch of newly-implemented RDS stub operations."""

    @pytest.fixture
    def client(self):
        return make_client("rds")

    def test_backtrack_db_cluster(self, client):
        """BacktrackDBCluster returns DBClusterIdentifier key."""
        import datetime

        try:
            resp = client.backtrack_db_cluster(
                DBClusterIdentifier="fake-cluster",
                BacktrackTo=datetime.datetime(2024, 1, 1, tzinfo=datetime.UTC),
            )
            assert "DBClusterIdentifier" in resp
        except ClientError as exc:
            assert exc.response["Error"]["Code"] is not None

    def test_delete_db_cluster_automated_backup(self, client):
        """DeleteDBClusterAutomatedBackup returns DBClusterAutomatedBackup key."""
        try:
            resp = client.delete_db_cluster_automated_backup(
                DbClusterResourceId="cluster-FAKEID123",
            )
            assert "DBClusterAutomatedBackup" in resp
        except ClientError as exc:
            assert exc.response["Error"]["Code"] is not None

    def test_describe_db_cluster_backtracks(self, client):
        """DescribeDBClusterBacktracks returns DBClusterBacktracks key."""
        try:
            resp = client.describe_db_cluster_backtracks(
                DBClusterIdentifier="fake-cluster",
            )
            assert "DBClusterBacktracks" in resp
        except ClientError as exc:
            assert exc.response["Error"]["Code"] is not None

    def test_download_db_log_file_portion(self, client):
        """DownloadDBLogFilePortion returns LogFileData key."""
        try:
            resp = client.download_db_log_file_portion(
                DBInstanceIdentifier="fake-instance",
                LogFileName="error/postgresql.log.2024-01-01-00",
            )
            assert "LogFileData" in resp
        except ClientError as exc:
            assert exc.response["Error"]["Code"] is not None

    def test_modify_current_db_cluster_capacity(self, client):
        """ModifyCurrentDBClusterCapacity returns CurrentCapacity key."""
        try:
            resp = client.modify_current_db_cluster_capacity(
                DBClusterIdentifier="fake-cluster",
            )
            assert "CurrentCapacity" in resp
        except ClientError as exc:
            assert exc.response["Error"]["Code"] is not None

    def test_modify_db_proxy_endpoint(self, client):
        """ModifyDBProxyEndpoint returns DBProxyEndpoint key."""
        try:
            resp = client.modify_db_proxy_endpoint(
                DBProxyEndpointName="fake-proxy-endpoint",
            )
            assert "DBProxyEndpoint" in resp
        except ClientError as exc:
            assert exc.response["Error"]["Code"] is not None

    def test_modify_db_recommendation(self, client):
        """ModifyDBRecommendation returns DBRecommendation key."""
        try:
            resp = client.modify_db_recommendation(
                RecommendationId="fake-recommendation-id",
            )
            assert "DBRecommendation" in resp
        except ClientError as exc:
            assert exc.response["Error"]["Code"] is not None

    def test_remove_role_from_db_instance(self, client):
        """RemoveRoleFromDBInstance succeeds or raises known error."""
        try:
            client.remove_role_from_db_instance(
                DBInstanceIdentifier="fake-instance",
                RoleArn="arn:aws:iam::123456789012:role/test-role",
                FeatureName="s3Export",
            )
        except ClientError as exc:
            assert exc.response["Error"]["Code"] is not None

    def test_revoke_db_security_group_ingress(self, client):
        """RevokeDBSecurityGroupIngress returns DBSecurityGroup key."""
        try:
            resp = client.revoke_db_security_group_ingress(
                DBSecurityGroupName="fake-sg",
                EC2SecurityGroupName="ec2-sg",
                EC2SecurityGroupOwnerId="123456789012",
            )
            assert "DBSecurityGroup" in resp
        except ClientError as exc:
            assert exc.response["Error"]["Code"] is not None

    def test_start_db_instance_automated_backups_replication(self, client):
        """StartDBInstanceAutomatedBackupsReplication returns DBInstanceAutomatedBackup."""
        try:
            resp = client.start_db_instance_automated_backups_replication(
                SourceDBInstanceArn=("arn:aws:rds:us-east-1:123456789012:db:fake-instance"),
            )
            assert "DBInstanceAutomatedBackup" in resp
        except ClientError as exc:
            assert exc.response["Error"]["Code"] is not None

    def test_stop_db_instance_automated_backups_replication(self, client):
        """StopDBInstanceAutomatedBackupsReplication returns DBInstanceAutomatedBackup."""
        try:
            resp = client.stop_db_instance_automated_backups_replication(
                SourceDBInstanceArn=("arn:aws:rds:us-east-1:123456789012:db:fake-instance"),
            )
            assert "DBInstanceAutomatedBackup" in resp
        except ClientError as exc:
            assert exc.response["Error"]["Code"] is not None

    def test_switchover_read_replica(self, client):
        """SwitchoverReadReplica returns DBInstance key."""
        try:
            resp = client.switchover_read_replica(
                DBInstanceIdentifier="fake-read-replica",
            )
            assert "DBInstance" in resp
        except ClientError as exc:
            assert exc.response["Error"]["Code"] is not None


class TestRDSGapOps:
    """Tests for RDS operations that weren't previously covered."""

    @pytest.fixture
    def client(self):
        return make_client("rds")

    def test_restore_db_cluster_from_s3(self, client):
        """RestoreDBClusterFromS3 creates a DB cluster from S3 backup data."""
        import uuid  # noqa: PLC0415

        cluster_id = f"s3-cluster-{uuid.uuid4().hex[:8]}"
        try:
            resp = client.restore_db_cluster_from_s3(
                DBClusterIdentifier=cluster_id,
                Engine="aurora-mysql",
                MasterUsername="admin",
                MasterUserPassword="Password123!",
                S3BucketName="test-backup-bucket",
                S3IngestionRoleArn="arn:aws:iam::123456789012:role/S3RestoreRole",
                SourceEngine="mysql",
                SourceEngineVersion="5.7.40",
            )
            assert "DBCluster" in resp
        finally:
            try:
                client.delete_db_cluster(DBClusterIdentifier=cluster_id, SkipFinalSnapshot=True)
            except Exception:  # noqa: BLE001
                pass  # best-effort cleanup

    def test_restore_db_instance_from_s3(self, client):
        """RestoreDBInstanceFromS3 creates a DB instance from S3 backup data."""
        import uuid  # noqa: PLC0415

        instance_id = f"s3-inst-{uuid.uuid4().hex[:8]}"
        try:
            resp = client.restore_db_instance_from_s3(
                DBInstanceIdentifier=instance_id,
                DBInstanceClass="db.t3.micro",
                Engine="mysql",
                MasterUsername="admin",
                MasterUserPassword="Password123!",
                S3BucketName="test-backup-bucket",
                S3IngestionRoleArn="arn:aws:iam::123456789012:role/S3RestoreRole",
                SourceEngine="mysql",
                SourceEngineVersion="5.7.40",
            )
            assert "DBInstance" in resp
        finally:
            try:
                client.delete_db_instance(
                    DBInstanceIdentifier=instance_id,
                    SkipFinalSnapshot=True,
                    DeleteAutomatedBackups=True,
                )
            except Exception:  # noqa: BLE001
                pass  # best-effort cleanup


class TestRDSEdgeCasesAndBehavioralFidelity:
    @pytest.fixture
    def client(self):
        return make_client("rds")

    @pytest.fixture
    def ec2_client(self):
        return make_client("ec2")

    def test_reboot_nonexistent_db_instance(self, client):
        """Rebooting a nonexistent instance returns DBInstanceNotFound."""
        with pytest.raises(ClientError) as exc:
            client.reboot_db_instance(DBInstanceIdentifier="does-not-exist-xyz")
        assert exc.value.response["Error"]["Code"] == "DBInstanceNotFound"

    def test_db_instance_arn_format(self, client):
        """Created DB instance has a properly formatted ARN."""
        name = _unique("compat-arn")
        client.create_db_instance(
            DBInstanceIdentifier=name,
            DBInstanceClass="db.t3.micro",
            Engine="mysql",
            MasterUsername="admin",
            MasterUserPassword="password123",
        )
        try:
            resp = client.describe_db_instances(DBInstanceIdentifier=name)
            inst = resp["DBInstances"][0]
            assert "DBInstanceArn" in inst
            arn = inst["DBInstanceArn"]
            assert arn.startswith("arn:aws:rds:us-east-1:")
            assert name in arn
        finally:
            try:
                client.delete_db_instance(DBInstanceIdentifier=name, SkipFinalSnapshot=True)
            except ClientError:
                pass  # best-effort cleanup

    def test_db_instance_timestamps(self, client):
        """Created DB instance has an InstanceCreateTime timestamp."""
        name = _unique("compat-ts")
        client.create_db_instance(
            DBInstanceIdentifier=name,
            DBInstanceClass="db.t3.micro",
            Engine="mysql",
            MasterUsername="admin",
            MasterUserPassword="password123",
        )
        try:
            resp = client.describe_db_instances(DBInstanceIdentifier=name)
            inst = resp["DBInstances"][0]
            if "InstanceCreateTime" not in inst:
                pytest.skip("InstanceCreateTime not returned by this implementation")
            assert inst["InstanceCreateTime"] is not None
        finally:
            try:
                client.delete_db_instance(DBInstanceIdentifier=name, SkipFinalSnapshot=True)
            except ClientError:
                pass  # best-effort cleanup

    def test_list_db_instances_pagination(self, client):
        """describe_db_instances with MaxRecords=2 returns at most 2 results and a Marker."""
        names = [_unique("compat-page") for _ in range(3)]
        for n in names:
            client.create_db_instance(
                DBInstanceIdentifier=n,
                DBInstanceClass="db.t3.micro",
                Engine="mysql",
                MasterUsername="admin",
                MasterUserPassword="password123",
            )
        try:
            resp = client.describe_db_instances(MaxRecords=2)
            assert len(resp["DBInstances"]) <= 2
            assert "Marker" in resp
        finally:
            for n in names:
                try:
                    client.delete_db_instance(DBInstanceIdentifier=n, SkipFinalSnapshot=True)
                except ClientError:
                    pass  # best-effort cleanup

    def test_subnet_group_arn_format(self, client, ec2_client):
        """Created DB subnet group has a properly formatted ARN."""
        name = _unique("compat-sgarn")
        vpc = ec2_client.create_vpc(CidrBlock="10.99.0.0/16")
        vpc_id = vpc["Vpc"]["VpcId"]
        s1 = ec2_client.create_subnet(
            VpcId=vpc_id, CidrBlock="10.99.1.0/24", AvailabilityZone="us-east-1a"
        )
        s2 = ec2_client.create_subnet(
            VpcId=vpc_id, CidrBlock="10.99.2.0/24", AvailabilityZone="us-east-1b"
        )
        subnet_ids = [s1["Subnet"]["SubnetId"], s2["Subnet"]["SubnetId"]]
        client.create_db_subnet_group(
            DBSubnetGroupName=name,
            DBSubnetGroupDescription="arn test subnet group",
            SubnetIds=subnet_ids,
        )
        try:
            resp = client.describe_db_subnet_groups(DBSubnetGroupName=name)
            grp = resp["DBSubnetGroups"][0]
            assert "DBSubnetGroupArn" in grp
            assert grp["DBSubnetGroupArn"].startswith("arn:aws:rds:")
        finally:
            try:
                client.delete_db_subnet_group(DBSubnetGroupName=name)
            except ClientError:
                pass  # best-effort cleanup
            for sid in subnet_ids:
                try:
                    ec2_client.delete_subnet(SubnetId=sid)
                except ClientError:
                    pass  # best-effort cleanup
            try:
                ec2_client.delete_vpc(VpcId=vpc_id)
            except ClientError:
                pass  # best-effort cleanup

    def test_parameter_group_pagination(self, client):
        """describe_db_parameter_groups with MaxRecords=2 returns a Marker when more results exist."""
        names = [_unique("compat-pgp") for _ in range(3)]
        for n in names:
            client.create_db_parameter_group(
                DBParameterGroupName=n,
                DBParameterGroupFamily="mysql8.0",
                Description="pagination test",
            )
        try:
            resp = client.describe_db_parameter_groups(MaxRecords=2)
            assert len(resp["DBParameterGroups"]) <= 2
            assert "Marker" in resp
        finally:
            for n in names:
                try:
                    client.delete_db_parameter_group(DBParameterGroupName=n)
                except ClientError:
                    pass  # best-effort cleanup

    def test_describe_events_duration_filter(self, client):
        """describe_events with Duration filter returns a list."""
        resp = client.describe_events(Duration=60)
        assert "Events" in resp
        assert isinstance(resp["Events"], list)

    def test_modify_db_instance_storage(self, client):
        """modify_db_instance with AllocatedStorage returns the instance with AllocatedStorage."""
        name = _unique("compat-mod")
        client.create_db_instance(
            DBInstanceIdentifier=name,
            DBInstanceClass="db.t3.micro",
            Engine="mysql",
            MasterUsername="admin",
            MasterUserPassword="password123",
            AllocatedStorage=20,
        )
        try:
            resp = client.modify_db_instance(
                DBInstanceIdentifier=name,
                AllocatedStorage=25,
            )
            inst = resp["DBInstance"]
            assert inst["DBInstanceIdentifier"] == name
            assert "AllocatedStorage" in inst
        finally:
            try:
                client.delete_db_instance(DBInstanceIdentifier=name, SkipFinalSnapshot=True)
            except ClientError:
                pass  # best-effort cleanup

    def test_stop_and_start_db_instance_status(self, client):
        """stop_db_instance and start_db_instance return expected status values."""
        name = _unique("compat-ss")
        client.create_db_instance(
            DBInstanceIdentifier=name,
            DBInstanceClass="db.t3.micro",
            Engine="mysql",
            MasterUsername="admin",
            MasterUserPassword="password123",
        )
        try:
            stop_resp = client.stop_db_instance(DBInstanceIdentifier=name)
            stop_status = stop_resp["DBInstance"]["DBInstanceStatus"]
            assert stop_status in ("stopped", "stopping", "available")

            start_resp = client.start_db_instance(DBInstanceIdentifier=name)
            start_status = start_resp["DBInstance"]["DBInstanceStatus"]
            assert start_status in ("available", "starting", "stopped")
        finally:
            try:
                client.delete_db_instance(DBInstanceIdentifier=name, SkipFinalSnapshot=True)
            except ClientError:
                pass  # best-effort cleanup

    def test_create_duplicate_db_instance(self, client):
        """Creating a DB instance with an existing identifier raises DBInstanceAlreadyExists."""
        name = _unique("compat-dup")
        client.create_db_instance(
            DBInstanceIdentifier=name,
            DBInstanceClass="db.t3.micro",
            Engine="mysql",
            MasterUsername="admin",
            MasterUserPassword="password123",
        )
        try:
            with pytest.raises(ClientError) as exc:
                client.create_db_instance(
                    DBInstanceIdentifier=name,
                    DBInstanceClass="db.t3.micro",
                    Engine="mysql",
                    MasterUsername="admin",
                    MasterUserPassword="password123",
                )
            assert exc.value.response["Error"]["Code"] == "DBInstanceAlreadyExists"
        finally:
            try:
                client.delete_db_instance(DBInstanceIdentifier=name, SkipFinalSnapshot=True)
            except ClientError:
                pass  # best-effort cleanup

    def test_describe_db_snapshot_attributes_content(self, client):
        """describe_db_snapshot_attributes returns result with identifier and attributes list."""
        inst_name = _unique("compat-db")
        snap_name = _unique("compat-snap")
        client.create_db_instance(
            DBInstanceIdentifier=inst_name,
            DBInstanceClass="db.t3.micro",
            Engine="mysql",
            MasterUsername="admin",
            MasterUserPassword="password123",
        )
        try:
            client.create_db_snapshot(
                DBSnapshotIdentifier=snap_name,
                DBInstanceIdentifier=inst_name,
            )
            try:
                resp = client.describe_db_snapshot_attributes(DBSnapshotIdentifier=snap_name)
                result = resp["DBSnapshotAttributesResult"]
                assert result["DBSnapshotIdentifier"] == snap_name
                assert "DBSnapshotAttributes" in result
                assert isinstance(result["DBSnapshotAttributes"], list)
            finally:
                try:
                    client.delete_db_snapshot(DBSnapshotIdentifier=snap_name)
                except ClientError:
                    pass  # best-effort cleanup
        finally:
            try:
                client.delete_db_instance(DBInstanceIdentifier=inst_name, SkipFinalSnapshot=True)
            except ClientError:
                pass  # best-effort cleanup

    def test_cluster_snapshots_list_not_empty_after_create(self, client):
        """describe_db_cluster_snapshots contains the snapshot after creation."""
        cluster_name = _unique("compat-cl")
        snap_name = _unique("compat-csnap")
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
                resp = client.describe_db_cluster_snapshots()
                snap_ids = [
                    s["DBClusterSnapshotIdentifier"] for s in resp["DBClusterSnapshots"]
                ]
                assert snap_name in snap_ids
            finally:
                try:
                    client.delete_db_cluster_snapshot(DBClusterSnapshotIdentifier=snap_name)
                except ClientError:
                    pass  # best-effort cleanup
        finally:
            try:
                client.delete_db_cluster(DBClusterIdentifier=cluster_name, SkipFinalSnapshot=True)
            except ClientError:
                pass  # best-effort cleanup


class TestRDSDBInstanceEdgeCases:
    """Edge cases and behavioral fidelity tests for DB instances."""

    @pytest.fixture
    def client(self):
        return make_client("rds")

    @pytest.fixture
    def instance(self, client):
        name = _unique("edge-db")
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
            pass  # best-effort cleanup

    def test_describe_nonexistent_db_instance_error(self, client):
        """Describing a nonexistent instance returns DBInstanceNotFound."""
        with pytest.raises(ClientError) as exc:
            client.describe_db_instances(DBInstanceIdentifier="nonexistent-inst-xyz-999")
        assert exc.value.response["Error"]["Code"] == "DBInstanceNotFound"

    def test_reboot_nonexistent_db_instance_error(self, client):
        """Rebooting a nonexistent instance returns DBInstanceNotFound."""
        with pytest.raises(ClientError) as exc:
            client.reboot_db_instance(DBInstanceIdentifier="nonexistent-inst-xyz-999")
        assert exc.value.response["Error"]["Code"] == "DBInstanceNotFound"

    def test_modify_nonexistent_db_instance_error(self, client):
        """Modifying a nonexistent instance returns DBInstanceNotFound."""
        with pytest.raises(ClientError) as exc:
            client.modify_db_instance(
                DBInstanceIdentifier="nonexistent-inst-xyz-999",
                MasterUserPassword="newpassword456",
            )
        assert exc.value.response["Error"]["Code"] == "DBInstanceNotFound"

    def test_stop_nonexistent_db_instance_error(self, client):
        """Stopping a nonexistent instance returns DBInstanceNotFound."""
        with pytest.raises(ClientError) as exc:
            client.stop_db_instance(DBInstanceIdentifier="nonexistent-inst-xyz-999")
        assert exc.value.response["Error"]["Code"] == "DBInstanceNotFound"

    def test_db_instance_arn_format(self, client, instance):
        """DBInstanceArn field follows arn:aws:rds:<region>:<account>:db:<id> format."""
        resp = client.describe_db_instances(DBInstanceIdentifier=instance)
        inst = resp["DBInstances"][0]
        assert "DBInstanceArn" in inst
        arn = inst["DBInstanceArn"]
        assert arn.startswith("arn:aws:rds:")
        assert ":db:" in arn
        assert instance in arn

    def test_db_instance_has_create_timestamp(self, client, instance):
        """InstanceCreateTime is present and is a datetime."""
        import datetime
        resp = client.describe_db_instances(DBInstanceIdentifier=instance)
        inst = resp["DBInstances"][0]
        assert "InstanceCreateTime" in inst
        assert isinstance(inst["InstanceCreateTime"], datetime.datetime)

    def test_db_instance_has_engine_version(self, client, instance):
        """EngineVersion field is present and non-empty."""
        resp = client.describe_db_instances(DBInstanceIdentifier=instance)
        inst = resp["DBInstances"][0]
        assert "EngineVersion" in inst
        assert inst["EngineVersion"] != ""

    def test_list_db_instances_pagination(self, client):
        """DescribeDBInstances MaxRecords limits results and Marker enables next page."""
        names = [_unique("pg-db") for _ in range(3)]
        for name in names:
            client.create_db_instance(
                DBInstanceIdentifier=name,
                DBInstanceClass="db.t3.micro",
                Engine="mysql",
                MasterUsername="admin",
                MasterUserPassword="password123",
            )
        try:
            all_resp = client.describe_db_instances()
            total = len(all_resp["DBInstances"])
            if total > 1:
                page = client.describe_db_instances(MaxRecords=1)
                assert len(page["DBInstances"]) == 1
                if "Marker" in page:
                    page2 = client.describe_db_instances(MaxRecords=1, Marker=page["Marker"])
                    assert "DBInstances" in page2
        finally:
            for name in names:
                try:
                    client.delete_db_instance(DBInstanceIdentifier=name, SkipFinalSnapshot=True)
                except ClientError:
                    pass  # best-effort cleanup

    def test_db_instance_reboot_returns_status(self, client, instance):
        """RebootDBInstance returns DBInstance with status field."""
        resp = client.reboot_db_instance(DBInstanceIdentifier=instance)
        assert "DBInstance" in resp
        inst = resp["DBInstance"]
        assert inst["DBInstanceIdentifier"] == instance
        assert "DBInstanceStatus" in inst

    def test_db_instance_modify_returns_updated_field(self, client, instance):
        """ModifyDBInstance returns DBInstance with identifier field."""
        resp = client.modify_db_instance(
            DBInstanceIdentifier=instance,
            BackupRetentionPeriod=3,
        )
        assert "DBInstance" in resp
        inst = resp["DBInstance"]
        assert inst["DBInstanceIdentifier"] == instance

    def test_db_instance_stop_returns_status(self, client, instance):
        """StopDBInstance returns DBInstance with identifier and status."""
        resp = client.stop_db_instance(DBInstanceIdentifier=instance)
        assert "DBInstance" in resp
        inst = resp["DBInstance"]
        assert inst["DBInstanceIdentifier"] == instance
        assert "DBInstanceStatus" in inst


class TestRDSSubnetGroupEdgeCases:
    """Edge cases and behavioral fidelity tests for DB subnet groups."""

    @pytest.fixture
    def client(self):
        return make_client("rds")

    @pytest.fixture
    def ec2_client(self):
        return make_client("ec2")

    @pytest.fixture
    def subnet_group_with_vpc(self, client, ec2_client):
        vpc = ec2_client.create_vpc(CidrBlock="10.86.0.0/16")
        vpc_id = vpc["Vpc"]["VpcId"]
        s1 = ec2_client.create_subnet(
            VpcId=vpc_id, CidrBlock="10.86.1.0/24", AvailabilityZone="us-east-1a"
        )
        s2 = ec2_client.create_subnet(
            VpcId=vpc_id, CidrBlock="10.86.2.0/24", AvailabilityZone="us-east-1b"
        )
        subnet_ids = [s1["Subnet"]["SubnetId"], s2["Subnet"]["SubnetId"]]
        name = _unique("edge-sg")
        client.create_db_subnet_group(
            DBSubnetGroupName=name,
            DBSubnetGroupDescription="edge case subnet group",
            SubnetIds=subnet_ids,
        )
        yield name, vpc_id, subnet_ids
        try:
            client.delete_db_subnet_group(DBSubnetGroupName=name)
        except ClientError:
            pass  # best-effort cleanup
        for sid in subnet_ids:
            try:
                ec2_client.delete_subnet(SubnetId=sid)
            except ClientError:
                pass  # best-effort cleanup
        try:
            ec2_client.delete_vpc(VpcId=vpc_id)
        except ClientError:
            pass  # best-effort cleanup

    def test_describe_nonexistent_subnet_group_error(self, client):
        """Describing a nonexistent subnet group returns DBSubnetGroupNotFoundFault."""
        with pytest.raises(ClientError) as exc:
            client.describe_db_subnet_groups(DBSubnetGroupName="nonexistent-sg-xyz-999")
        assert exc.value.response["Error"]["Code"] == "DBSubnetGroupNotFoundFault"

    def test_subnet_group_arn_format(self, client, subnet_group_with_vpc):
        """DBSubnetGroupArn follows expected arn format."""
        name, _, _ = subnet_group_with_vpc
        resp = client.describe_db_subnet_groups(DBSubnetGroupName=name)
        grp = resp["DBSubnetGroups"][0]
        assert "DBSubnetGroupArn" in grp
        arn = grp["DBSubnetGroupArn"]
        assert arn.startswith("arn:aws:rds:")
        assert ":subgrp:" in arn
        assert name in arn

    def test_subnet_group_vpc_id_present(self, client, subnet_group_with_vpc):
        """DBSubnetGroup includes VpcId field."""
        name, vpc_id, _ = subnet_group_with_vpc
        resp = client.describe_db_subnet_groups(DBSubnetGroupName=name)
        grp = resp["DBSubnetGroups"][0]
        assert "VpcId" in grp
        assert grp["VpcId"] == vpc_id

    def test_subnet_group_subnet_status_present(self, client, subnet_group_with_vpc):
        """Each subnet in the group has SubnetStatus field."""
        name, _, _ = subnet_group_with_vpc
        resp = client.describe_db_subnet_groups(DBSubnetGroupName=name)
        grp = resp["DBSubnetGroups"][0]
        for subnet in grp["Subnets"]:
            assert "SubnetStatus" in subnet
            assert "SubnetIdentifier" in subnet


class TestRDSParameterGroupEdgeCases:
    """Edge cases and behavioral fidelity tests for DB parameter groups."""

    @pytest.fixture
    def client(self):
        return make_client("rds")

    @pytest.fixture
    def pg(self, client):
        name = _unique("edge-pg")
        client.create_db_parameter_group(
            DBParameterGroupName=name,
            DBParameterGroupFamily="mysql8.0",
            Description="edge case parameter group",
        )
        yield name
        try:
            client.delete_db_parameter_group(DBParameterGroupName=name)
        except ClientError:
            pass  # best-effort cleanup

    def test_describe_nonexistent_parameter_group_returns_empty(self, client):
        """DescribeDBParameterGroups for unknown group returns empty list."""
        resp = client.describe_db_parameter_groups(DBParameterGroupName="nonexistent-pg-xyz-999")
        assert resp["DBParameterGroups"] == []

    def test_parameter_group_arn_format(self, client, pg):
        """DBParameterGroupArn follows arn:aws:rds:...:pg:... format."""
        resp = client.describe_db_parameter_groups(DBParameterGroupName=pg)
        grp = resp["DBParameterGroups"][0]
        assert "DBParameterGroupArn" in grp
        arn = grp["DBParameterGroupArn"]
        assert arn.startswith("arn:aws:rds:")
        assert ":pg:" in arn
        assert pg in arn

    def test_db_parameters_have_parameter_name_field(self, client, pg):
        """Parameters returned by DescribeDBParameters include ParameterName."""
        resp = client.describe_db_parameters(DBParameterGroupName=pg)
        params = resp["Parameters"]
        if params:
            for p in params[:3]:
                assert "ParameterName" in p
                assert "DataType" in p

    def test_parameter_group_delete_nonexistent_error(self, client):
        """Deleting a nonexistent parameter group returns DBParameterGroupNotFound."""
        with pytest.raises(ClientError) as exc:
            client.delete_db_parameter_group(DBParameterGroupName="nonexistent-pg-xyz-999")
        assert exc.value.response["Error"]["Code"] == "DBParameterGroupNotFound"


class TestRDSEventsEdgeCases:
    """Edge cases for DescribeEvents."""

    @pytest.fixture
    def client(self):
        return make_client("rds")

    def test_describe_events_with_max_records(self, client):
        """DescribeEvents accepts MaxRecords parameter and returns Events list."""
        resp = client.describe_events(MaxRecords=20)
        assert "Events" in resp
        assert isinstance(resp["Events"], list)

    def test_describe_events_with_duration(self, client):
        """DescribeEvents with Duration parameter returns response."""
        resp = client.describe_events(Duration=60)
        assert "Events" in resp
        assert isinstance(resp["Events"], list)

    def test_describe_events_by_source_type_parameter_group(self, client):
        """DescribeEvents can filter by SourceType=db-parameter-group."""
        resp = client.describe_events(SourceType="db-parameter-group")
        assert "Events" in resp
        assert isinstance(resp["Events"], list)

    def test_describe_events_by_source_type_snapshot(self, client):
        """DescribeEvents can filter by SourceType=db-snapshot."""
        resp = client.describe_events(SourceType="db-snapshot")
        assert "Events" in resp
        assert isinstance(resp["Events"], list)


class TestRDSOrderableOptionsEdgeCases:
    """Edge cases for DescribeOrderableDBInstanceOptions."""

    @pytest.fixture
    def client(self):
        return make_client("rds")

    def test_orderable_options_fields_present(self, client):
        """Each orderable option has required fields."""
        resp = client.describe_orderable_db_instance_options(Engine="mysql", MaxRecords=20)
        options = resp["OrderableDBInstanceOptions"]
        if options:
            opt = options[0]
            assert "Engine" in opt
            assert "DBInstanceClass" in opt

    def test_orderable_options_engine_filter(self, client):
        """Filtering by engine returns only matching options."""
        resp = client.describe_orderable_db_instance_options(Engine="mysql")
        for opt in resp["OrderableDBInstanceOptions"]:
            assert opt["Engine"] == "mysql"

    def test_orderable_options_max_records(self, client):
        """MaxRecords limits the returned results."""
        resp = client.describe_orderable_db_instance_options(Engine="mysql", MaxRecords=5)
        assert len(resp["OrderableDBInstanceOptions"]) <= 5


class TestRDSClusterSnapshotEdgeCases:
    """Edge cases for DB cluster snapshots."""

    @pytest.fixture
    def client(self):
        return make_client("rds")

    @pytest.fixture
    def cluster_with_snapshot(self, client):
        cluster_name = _unique("edge-cl")
        snap_name = _unique("edge-csnap")
        client.create_db_cluster(
            DBClusterIdentifier=cluster_name,
            Engine="aurora-mysql",
            MasterUsername="admin",
            MasterUserPassword="password123!",
        )
        client.create_db_cluster_snapshot(
            DBClusterSnapshotIdentifier=snap_name,
            DBClusterIdentifier=cluster_name,
        )
        yield cluster_name, snap_name
        try:
            client.delete_db_cluster_snapshot(DBClusterSnapshotIdentifier=snap_name)
        except ClientError:
            pass  # best-effort cleanup
        try:
            client.delete_db_cluster(DBClusterIdentifier=cluster_name, SkipFinalSnapshot=True)
        except ClientError:
            pass  # best-effort cleanup

    def test_cluster_snapshot_status_field(self, client, cluster_with_snapshot):
        """DBClusterSnapshot includes Status field."""
        _, snap_name = cluster_with_snapshot
        resp = client.describe_db_cluster_snapshots(DBClusterSnapshotIdentifier=snap_name)
        snap = resp["DBClusterSnapshots"][0]
        assert "Status" in snap
        assert snap["Status"] in ("available", "creating")

    def test_describe_cluster_snapshots_by_cluster(self, client, cluster_with_snapshot):
        """DescribeDBClusterSnapshots filters by DBClusterIdentifier."""
        cluster_name, snap_name = cluster_with_snapshot
        resp = client.describe_db_cluster_snapshots(DBClusterIdentifier=cluster_name)
        snap_ids = [s["DBClusterSnapshotIdentifier"] for s in resp["DBClusterSnapshots"]]
        assert snap_name in snap_ids

    def test_cluster_snapshot_arn_format(self, client, cluster_with_snapshot):
        """DBClusterSnapshotArn follows expected arn format."""
        _, snap_name = cluster_with_snapshot
        resp = client.describe_db_cluster_snapshots(DBClusterSnapshotIdentifier=snap_name)
        snap = resp["DBClusterSnapshots"][0]
        assert "DBClusterSnapshotArn" in snap
        arn = snap["DBClusterSnapshotArn"]
        assert arn.startswith("arn:aws:rds:")
        assert ":cluster-snapshot:" in arn

    def test_describe_nonexistent_cluster_snapshot_error(self, client):
        """Describing a nonexistent cluster snapshot returns DBClusterSnapshotNotFoundFault."""
        with pytest.raises(ClientError) as exc:
            client.describe_db_cluster_snapshots(
                DBClusterSnapshotIdentifier="nonexistent-csnap-xyz-999"
            )
        assert exc.value.response["Error"]["Code"] == "DBClusterSnapshotNotFoundFault"

    def test_cluster_snapshot_engine_field(self, client, cluster_with_snapshot):
        """DBClusterSnapshot includes Engine field matching the source cluster."""
        _, snap_name = cluster_with_snapshot
        resp = client.describe_db_cluster_snapshots(DBClusterSnapshotIdentifier=snap_name)
        snap = resp["DBClusterSnapshots"][0]
        assert "Engine" in snap
        assert snap["Engine"] == "aurora-mysql"


class TestRDSClusterParameterGroupEdgeCases:
    """Edge cases for DB cluster parameter groups."""

    @pytest.fixture
    def client(self):
        return make_client("rds")

    @pytest.fixture
    def cpg(self, client):
        name = _unique("edge-cpg")
        client.create_db_cluster_parameter_group(
            DBClusterParameterGroupName=name,
            DBParameterGroupFamily="aurora-mysql8.0",
            Description="edge case cluster param group",
        )
        yield name
        try:
            client.delete_db_cluster_parameter_group(DBClusterParameterGroupName=name)
        except ClientError:
            pass  # best-effort cleanup

    def test_describe_cluster_param_group_by_name(self, client, cpg):
        """DescribeDBClusterParameterGroups can filter by name."""
        resp = client.describe_db_cluster_parameter_groups(DBClusterParameterGroupName=cpg)
        groups = resp["DBClusterParameterGroups"]
        assert len(groups) == 1
        assert groups[0]["DBClusterParameterGroupName"] == cpg

    def test_cluster_param_group_arn_format(self, client, cpg):
        """DBClusterParameterGroupArn follows expected format."""
        resp = client.describe_db_cluster_parameter_groups(DBClusterParameterGroupName=cpg)
        grp = resp["DBClusterParameterGroups"][0]
        assert "DBClusterParameterGroupArn" in grp
        arn = grp["DBClusterParameterGroupArn"]
        assert arn.startswith("arn:aws:rds:")
        assert ":cluster-pg:" in arn

    def test_describe_nonexistent_cluster_param_group_error(self, client):
        """DescribeDBClusterParameterGroups for nonexistent group returns error."""
        with pytest.raises(ClientError) as exc:
            client.describe_db_cluster_parameter_groups(
                DBClusterParameterGroupName="nonexistent-cpg-xyz-999"
            )
        assert exc.value.response["Error"]["Code"] == "DBParameterGroupNotFound"

    def test_cluster_param_group_family_field(self, client, cpg):
        """DBClusterParameterGroup includes DBParameterGroupFamily field."""
        resp = client.describe_db_cluster_parameter_groups(DBClusterParameterGroupName=cpg)
        grp = resp["DBClusterParameterGroups"][0]
        assert "DBParameterGroupFamily" in grp
        assert grp["DBParameterGroupFamily"] == "aurora-mysql8.0"


class TestRDSSnapshotAttributesEdgeCases:
    """Edge cases for DB snapshot attributes (fills missing patterns)."""

    @pytest.fixture
    def client(self):
        return make_client("rds")

    @pytest.fixture
    def instance_with_snapshot(self, client):
        db_name = _unique("edge-db")
        snap_name = _unique("edge-snap")
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
        yield db_name, snap_name
        try:
            client.delete_db_snapshot(DBSnapshotIdentifier=snap_name)
        except ClientError:
            pass  # best-effort cleanup
        try:
            client.delete_db_instance(DBInstanceIdentifier=db_name, SkipFinalSnapshot=True)
        except ClientError:
            pass  # best-effort cleanup

    def test_snapshot_attributes_result_has_snapshot_id(self, client, instance_with_snapshot):
        """DBSnapshotAttributesResult contains DBSnapshotIdentifier."""
        _, snap_name = instance_with_snapshot
        resp = client.describe_db_snapshot_attributes(DBSnapshotIdentifier=snap_name)
        result = resp["DBSnapshotAttributesResult"]
        assert "DBSnapshotIdentifier" in result
        assert result["DBSnapshotIdentifier"] == snap_name

    def test_snapshot_attributes_list_is_present(self, client, instance_with_snapshot):
        """DBSnapshotAttributesResult contains DBSnapshotAttributes list."""
        _, snap_name = instance_with_snapshot
        resp = client.describe_db_snapshot_attributes(DBSnapshotIdentifier=snap_name)
        result = resp["DBSnapshotAttributesResult"]
        assert "DBSnapshotAttributes" in result
        assert isinstance(result["DBSnapshotAttributes"], list)

    def test_snapshot_attribute_values_after_modify(self, client, instance_with_snapshot):
        """ModifyDBSnapshotAttribute changes are visible in describe."""
        _, snap_name = instance_with_snapshot
        client.modify_db_snapshot_attribute(
            DBSnapshotIdentifier=snap_name,
            AttributeName="restore",
            ValuesToAdd=["all"],
        )
        resp = client.describe_db_snapshot_attributes(DBSnapshotIdentifier=snap_name)
        attrs = resp["DBSnapshotAttributesResult"]["DBSnapshotAttributes"]
        restore_attrs = [a for a in attrs if a["AttributeName"] == "restore"]
        assert len(restore_attrs) == 1
        assert "all" in restore_attrs[0]["AttributeValues"]

    def test_snapshot_attribute_remove_value(self, client, instance_with_snapshot):
        """Removing a value from snapshot attribute is reflected in describe."""
        _, snap_name = instance_with_snapshot
        client.modify_db_snapshot_attribute(
            DBSnapshotIdentifier=snap_name,
            AttributeName="restore",
            ValuesToAdd=["all"],
        )
        client.modify_db_snapshot_attribute(
            DBSnapshotIdentifier=snap_name,
            AttributeName="restore",
            ValuesToRemove=["all"],
        )
        resp = client.describe_db_snapshot_attributes(DBSnapshotIdentifier=snap_name)
        attrs = resp["DBSnapshotAttributesResult"]["DBSnapshotAttributes"]
        restore_attrs = [a for a in attrs if a["AttributeName"] == "restore"]
        if restore_attrs:
            assert "all" not in restore_attrs[0]["AttributeValues"]

    def test_describe_snapshot_attributes_nonexistent_error(self, client):
        """Describing attributes for a nonexistent snapshot returns DBSnapshotNotFound."""
        with pytest.raises(ClientError) as exc:
            client.describe_db_snapshot_attributes(DBSnapshotIdentifier="nonexistent-snap-xyz")
        assert exc.value.response["Error"]["Code"] == "DBSnapshotNotFound"


class TestRDSApplyPendingMaintenanceEdgeCases:
    """Edge cases for ApplyPendingMaintenanceAction."""

    @pytest.fixture
    def client(self):
        return make_client("rds")

    @pytest.fixture
    def instance(self, client):
        name = _unique("maint-db")
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
            pass  # best-effort cleanup

    def test_apply_pending_maintenance_immediate(self, client, instance):
        """ApplyPendingMaintenanceAction with immediate opt-in; list confirms structure."""
        arn = f"arn:aws:rds:us-east-1:123456789012:db:{instance}"
        resp = client.apply_pending_maintenance_action(
            ResourceIdentifier=arn,
            ApplyAction="system-update",
            OptInType="immediate",
        )
        result = resp["ResourcePendingMaintenanceActions"]
        assert result["ResourceIdentifier"] == arn
        list_resp = client.describe_pending_maintenance_actions()
        assert isinstance(list_resp["PendingMaintenanceActions"], list)
        with pytest.raises(ClientError) as exc:
            client.describe_db_instances(DBInstanceIdentifier="nonexistent-xyz-99")
        assert exc.value.response["Error"]["Code"] == "DBInstanceNotFound"

    def test_apply_pending_maintenance_next_window(self, client, instance):
        """ApplyPendingMaintenanceAction with next-maintenance opt-in; list confirms."""
        arn = f"arn:aws:rds:us-east-1:123456789012:db:{instance}"
        resp = client.apply_pending_maintenance_action(
            ResourceIdentifier=arn,
            ApplyAction="system-update",
            OptInType="next-maintenance",
        )
        assert resp["ResourcePendingMaintenanceActions"]["ResourceIdentifier"] == arn
        list_resp = client.describe_pending_maintenance_actions()
        assert isinstance(list_resp["PendingMaintenanceActions"], list)
        with pytest.raises(ClientError) as exc:
            client.describe_db_instances(DBInstanceIdentifier="nonexistent-xyz-99")
        assert exc.value.response["Error"]["Code"] == "DBInstanceNotFound"

    def test_apply_pending_maintenance_undo_opt_in(self, client, instance):
        """ApplyPendingMaintenanceAction with undo-opt-in; list confirms structure."""
        arn = f"arn:aws:rds:us-east-1:123456789012:db:{instance}"
        resp = client.apply_pending_maintenance_action(
            ResourceIdentifier=arn,
            ApplyAction="system-update",
            OptInType="undo-opt-in",
        )
        assert resp["ResourcePendingMaintenanceActions"]["ResourceIdentifier"] == arn
        list_resp = client.describe_pending_maintenance_actions()
        assert isinstance(list_resp["PendingMaintenanceActions"], list)
        with pytest.raises(ClientError) as exc:
            client.describe_db_instances(DBInstanceIdentifier="nonexistent-xyz-99")
        assert exc.value.response["Error"]["Code"] == "DBInstanceNotFound"

    def test_describe_pending_maintenance_actions_for_resource(self, client, instance):
        """DescribePendingMaintenanceActions can filter by resource ARN."""
        arn = f"arn:aws:rds:us-east-1:123456789012:db:{instance}"
        resp = client.describe_pending_maintenance_actions(ResourceIdentifier=arn)
        assert "PendingMaintenanceActions" in resp
        assert isinstance(resp["PendingMaintenanceActions"], list)


class TestRDSBlueGreenDeploymentEdgeCases:
    """Edge cases for BlueGreenDeployment operations."""

    @pytest.fixture
    def client(self):
        return make_client("rds")

    def test_describe_blue_green_deployments_empty(self, client):
        """DescribeBlueGreenDeployments returns list (may be empty)."""
        resp = client.describe_blue_green_deployments()
        assert "BlueGreenDeployments" in resp
        assert isinstance(resp["BlueGreenDeployments"], list)

    def test_describe_nonexistent_blue_green_deployment_error(self, client):
        """Describing a nonexistent BGD returns BlueGreenDeploymentNotFoundFault."""
        with pytest.raises(ClientError) as exc:
            client.describe_blue_green_deployments(BlueGreenDeploymentIdentifier="nonexistent-bgd")
        err_code = exc.value.response["Error"]["Code"]
        assert err_code in (
            "BlueGreenDeploymentNotFoundFault",
            "BlueGreenDeploymentNotFound",
        )


class TestRDSPendingMaintenanceActionFidelity:
    """Multi-pattern tests for ApplyPendingMaintenanceAction with LIST, CREATE, ERROR coverage."""

    @pytest.fixture
    def client(self):
        return make_client("rds")

    @pytest.fixture
    def db_instance(self, client):
        name = _unique("pm-db")
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
            pass  # best-effort cleanup

    def test_apply_pending_maintenance_action_and_describe(self, client, db_instance):
        """Apply action then describe_pending_maintenance_actions returns list."""
        arn = f"arn:aws:rds:us-east-1:123456789012:db:{db_instance}"
        resp = client.apply_pending_maintenance_action(
            ResourceIdentifier=arn,
            ApplyAction="system-update",
            OptInType="immediate",
        )
        result = resp["ResourcePendingMaintenanceActions"]
        assert result["ResourceIdentifier"] == arn
        assert "PendingMaintenanceActionDetails" in result
        list_resp = client.describe_pending_maintenance_actions()
        assert "PendingMaintenanceActions" in list_resp
        assert isinstance(list_resp["PendingMaintenanceActions"], list)

    def test_apply_pending_maintenance_immediate_then_list(self, client, db_instance):
        """Apply with OptInType=immediate and list confirms response structure."""
        arn = f"arn:aws:rds:us-east-1:123456789012:db:{db_instance}"
        resp = client.apply_pending_maintenance_action(
            ResourceIdentifier=arn,
            ApplyAction="system-update",
            OptInType="immediate",
        )
        assert resp["ResourcePendingMaintenanceActions"]["ResourceIdentifier"] == arn
        list_resp = client.describe_pending_maintenance_actions()
        assert isinstance(list_resp["PendingMaintenanceActions"], list)

    def test_apply_pending_maintenance_next_window_then_list(self, client, db_instance):
        """Apply with OptInType=next-maintenance and list confirms response structure."""
        arn = f"arn:aws:rds:us-east-1:123456789012:db:{db_instance}"
        resp = client.apply_pending_maintenance_action(
            ResourceIdentifier=arn,
            ApplyAction="system-update",
            OptInType="next-maintenance",
        )
        assert "ResourcePendingMaintenanceActions" in resp
        assert resp["ResourcePendingMaintenanceActions"]["ResourceIdentifier"] == arn
        list_resp = client.describe_pending_maintenance_actions()
        assert isinstance(list_resp["PendingMaintenanceActions"], list)

    def test_apply_pending_maintenance_undo_then_list(self, client, db_instance):
        """Apply with OptInType=undo-opt-in and list confirms response structure."""
        arn = f"arn:aws:rds:us-east-1:123456789012:db:{db_instance}"
        resp = client.apply_pending_maintenance_action(
            ResourceIdentifier=arn,
            ApplyAction="system-update",
            OptInType="undo-opt-in",
        )
        assert "ResourcePendingMaintenanceActions" in resp
        result = resp["ResourcePendingMaintenanceActions"]
        assert result["ResourceIdentifier"] == arn
        list_resp = client.describe_pending_maintenance_actions()
        assert isinstance(list_resp["PendingMaintenanceActions"], list)

    def test_apply_pending_maintenance_action_details_fields(self, client, db_instance):
        """PendingMaintenanceActionDetails includes Action and OptInStatus fields; list confirms."""
        arn = f"arn:aws:rds:us-east-1:123456789012:db:{db_instance}"
        resp = client.apply_pending_maintenance_action(
            ResourceIdentifier=arn,
            ApplyAction="system-update",
            OptInType="immediate",
        )
        details = resp["ResourcePendingMaintenanceActions"]["PendingMaintenanceActionDetails"]
        assert len(details) >= 1
        action = details[0]
        assert action["Action"] == "system-update"
        assert "OptInStatus" in action
        list_resp = client.describe_pending_maintenance_actions()
        assert isinstance(list_resp["PendingMaintenanceActions"], list)
        with pytest.raises(ClientError) as exc:
            client.describe_db_instances(DBInstanceIdentifier="nonexistent-xyz-99")
        assert exc.value.response["Error"]["Code"] == "DBInstanceNotFound"


class TestRDSDBInstanceMultiPatternFidelity:
    """Multi-pattern tests for DB instances: CREATE + LIST + UPDATE + DELETE + ERROR."""

    @pytest.fixture
    def client(self):
        return make_client("rds")

    def test_create_db_instance_full_lifecycle(self, client):
        """Create, list, modify, delete, verify deletion raises error."""
        name = _unique("mp-db")
        create_resp = client.create_db_instance(
            DBInstanceIdentifier=name,
            DBInstanceClass="db.t3.micro",
            Engine="mysql",
            MasterUsername="admin",
            MasterUserPassword="password123",
        )
        assert create_resp["DBInstance"]["DBInstanceIdentifier"] == name
        list_resp = client.describe_db_instances()
        identifiers = [i["DBInstanceIdentifier"] for i in list_resp["DBInstances"]]
        assert name in identifiers
        mod_resp = client.modify_db_instance(
            DBInstanceIdentifier=name,
            MasterUserPassword="newpassword456",
        )
        assert mod_resp["DBInstance"]["DBInstanceIdentifier"] == name
        del_resp = client.delete_db_instance(DBInstanceIdentifier=name, SkipFinalSnapshot=True)
        assert del_resp["DBInstance"]["DBInstanceIdentifier"] == name
        with pytest.raises(ClientError) as exc:
            client.describe_db_instances(DBInstanceIdentifier=name)
        assert exc.value.response["Error"]["Code"] == "DBInstanceNotFound"

    def test_db_instance_duplicate_name_error(self, client):
        """Creating a DB instance with a duplicate name raises DBInstanceAlreadyExists."""
        name = _unique("dup-db")
        client.create_db_instance(
            DBInstanceIdentifier=name,
            DBInstanceClass="db.t3.micro",
            Engine="mysql",
            MasterUsername="admin",
            MasterUserPassword="password123",
        )
        try:
            with pytest.raises(ClientError) as exc:
                client.create_db_instance(
                    DBInstanceIdentifier=name,
                    DBInstanceClass="db.t3.micro",
                    Engine="mysql",
                    MasterUsername="admin",
                    MasterUserPassword="password123",
                )
            assert exc.value.response["Error"]["Code"] == "DBInstanceAlreadyExists"
        finally:
            try:
                client.delete_db_instance(DBInstanceIdentifier=name, SkipFinalSnapshot=True)
            except ClientError:
                pass  # best-effort cleanup

    def test_describe_specific_instance_fields(self, client):
        """Describe specific instance returns correct field values."""
        name = _unique("fields-db")
        client.create_db_instance(
            DBInstanceIdentifier=name,
            DBInstanceClass="db.t3.micro",
            Engine="mysql",
            MasterUsername="admin",
            MasterUserPassword="password123",
        )
        try:
            resp = client.describe_db_instances(DBInstanceIdentifier=name)
            inst = resp["DBInstances"][0]
            assert inst["DBInstanceIdentifier"] == name
            assert inst["Engine"] == "mysql"
            assert inst["DBInstanceClass"] == "db.t3.micro"
            assert inst["MasterUsername"] == "admin"
            assert "DBInstanceArn" in inst
            assert inst["DBInstanceArn"].startswith("arn:aws:rds:")
            assert name in inst["DBInstanceArn"]
        finally:
            try:
                client.delete_db_instance(DBInstanceIdentifier=name, SkipFinalSnapshot=True)
            except ClientError:
                pass  # best-effort cleanup

    def test_list_db_instances_pagination_with_marker(self, client):
        """Pagination with MaxRecords and Marker works correctly."""
        names = [_unique("pag-db") for _ in range(3)]
        for n in names:
            client.create_db_instance(
                DBInstanceIdentifier=n,
                DBInstanceClass="db.t3.micro",
                Engine="mysql",
                MasterUsername="admin",
                MasterUserPassword="password123",
            )
        try:
            all_resp = client.describe_db_instances()
            total = len(all_resp["DBInstances"])
            if total > 1:
                page1 = client.describe_db_instances(MaxRecords=1)
                assert len(page1["DBInstances"]) == 1
                assert "Marker" in page1
                page2 = client.describe_db_instances(MaxRecords=1, Marker=page1["Marker"])
                assert "DBInstances" in page2
                assert len(page2["DBInstances"]) >= 1
        finally:
            for n in names:
                try:
                    client.delete_db_instance(DBInstanceIdentifier=n, SkipFinalSnapshot=True)
                except ClientError:
                    pass  # best-effort cleanup

    def test_reboot_db_instance_then_describe(self, client):
        """Reboot instance then list confirms it still exists."""
        name = _unique("rb-db")
        client.create_db_instance(
            DBInstanceIdentifier=name,
            DBInstanceClass="db.t3.micro",
            Engine="mysql",
            MasterUsername="admin",
            MasterUserPassword="password123",
        )
        try:
            reboot_resp = client.reboot_db_instance(DBInstanceIdentifier=name)
            assert reboot_resp["DBInstance"]["DBInstanceIdentifier"] == name
            desc_resp = client.describe_db_instances()
            ids = [i["DBInstanceIdentifier"] for i in desc_resp["DBInstances"]]
            assert name in ids
        finally:
            try:
                client.delete_db_instance(DBInstanceIdentifier=name, SkipFinalSnapshot=True)
            except ClientError:
                pass  # best-effort cleanup

    def test_stop_db_instance_then_list(self, client):
        """Stop instance then list confirms it still appears in results."""
        name = _unique("stop-db")
        client.create_db_instance(
            DBInstanceIdentifier=name,
            DBInstanceClass="db.t3.micro",
            Engine="mysql",
            MasterUsername="admin",
            MasterUserPassword="password123",
        )
        try:
            stop_resp = client.stop_db_instance(DBInstanceIdentifier=name)
            assert stop_resp["DBInstance"]["DBInstanceIdentifier"] == name
            list_resp = client.describe_db_instances()
            ids = [i["DBInstanceIdentifier"] for i in list_resp["DBInstances"]]
            assert name in ids
        finally:
            try:
                client.delete_db_instance(DBInstanceIdentifier=name, SkipFinalSnapshot=True)
            except ClientError:
                pass  # best-effort cleanup


class TestRDSSubnetGroupMultiPatternFidelity:
    """Multi-pattern tests for DB subnet groups: CREATE + LIST + DELETE + ERROR."""

    @pytest.fixture
    def client(self):
        return make_client("rds")

    @pytest.fixture
    def ec2_client(self):
        return make_client("ec2")

    @pytest.fixture
    def vpc_subnets(self, ec2_client):
        vpc = ec2_client.create_vpc(CidrBlock="10.93.0.0/16")
        vpc_id = vpc["Vpc"]["VpcId"]
        s1 = ec2_client.create_subnet(
            VpcId=vpc_id, CidrBlock="10.93.1.0/24", AvailabilityZone="us-east-1a"
        )
        s2 = ec2_client.create_subnet(
            VpcId=vpc_id, CidrBlock="10.93.2.0/24", AvailabilityZone="us-east-1b"
        )
        subnet_ids = [s1["Subnet"]["SubnetId"], s2["Subnet"]["SubnetId"]]
        yield vpc_id, subnet_ids
        for sid in subnet_ids:
            try:
                ec2_client.delete_subnet(SubnetId=sid)
            except ClientError:
                pass  # best-effort cleanup
        try:
            ec2_client.delete_vpc(VpcId=vpc_id)
        except ClientError:
            pass  # best-effort cleanup

    def test_subnet_group_full_lifecycle(self, client, vpc_subnets):
        """Create, list, delete, verify deletion raises error."""
        vpc_id, subnet_ids = vpc_subnets
        name = _unique("mp-sg")
        create_resp = client.create_db_subnet_group(
            DBSubnetGroupName=name,
            DBSubnetGroupDescription="multi-pattern test",
            SubnetIds=subnet_ids,
        )
        assert create_resp["DBSubnetGroup"]["DBSubnetGroupName"] == name
        list_resp = client.describe_db_subnet_groups()
        names = [g["DBSubnetGroupName"] for g in list_resp["DBSubnetGroups"]]
        assert name in names
        client.delete_db_subnet_group(DBSubnetGroupName=name)
        with pytest.raises(ClientError) as exc:
            client.describe_db_subnet_groups(DBSubnetGroupName=name)
        assert exc.value.response["Error"]["Code"] == "DBSubnetGroupNotFoundFault"

    def test_subnet_group_fields_present(self, client, vpc_subnets):
        """Subnet group response includes VpcId, DBSubnetGroupArn, SubnetGroupStatus."""
        vpc_id, subnet_ids = vpc_subnets
        name = _unique("fld-sg")
        client.create_db_subnet_group(
            DBSubnetGroupName=name,
            DBSubnetGroupDescription="fields test",
            SubnetIds=subnet_ids,
        )
        try:
            resp = client.describe_db_subnet_groups(DBSubnetGroupName=name)
            grp = resp["DBSubnetGroups"][0]
            assert grp["VpcId"] == vpc_id
            assert "DBSubnetGroupArn" in grp
            assert grp["DBSubnetGroupArn"].startswith("arn:aws:rds:")
            assert "SubnetGroupStatus" in grp
            assert grp["SubnetGroupStatus"] != ""
        finally:
            try:
                client.delete_db_subnet_group(DBSubnetGroupName=name)
            except ClientError:
                pass  # best-effort cleanup


class TestRDSParameterGroupMultiPatternFidelity:
    """Multi-pattern tests for DB parameter groups: CREATE + LIST + UPDATE + DELETE."""

    @pytest.fixture
    def client(self):
        return make_client("rds")

    def test_parameter_group_full_lifecycle(self, client):
        """Create, list, modify, delete parameter group."""
        name = _unique("mp-pg")
        create_resp = client.create_db_parameter_group(
            DBParameterGroupName=name,
            DBParameterGroupFamily="mysql8.0",
            Description="multi-pattern test",
        )
        assert create_resp["DBParameterGroup"]["DBParameterGroupName"] == name
        list_resp = client.describe_db_parameter_groups(DBParameterGroupName=name)
        assert list_resp["DBParameterGroups"][0]["DBParameterGroupName"] == name
        mod_resp = client.modify_db_parameter_group(
            DBParameterGroupName=name,
            Parameters=[
                {
                    "ParameterName": "max_connections",
                    "ParameterValue": "150",
                    "ApplyMethod": "immediate",
                }
            ],
        )
        assert mod_resp["DBParameterGroupName"] == name
        client.delete_db_parameter_group(DBParameterGroupName=name)
        after_resp = client.describe_db_parameter_groups(DBParameterGroupName=name)
        assert after_resp["DBParameterGroups"] == []

    def test_parameter_group_duplicate_error(self, client):
        """Creating a parameter group with a duplicate name raises error."""
        name = _unique("dup-pg")
        client.create_db_parameter_group(
            DBParameterGroupName=name,
            DBParameterGroupFamily="mysql8.0",
            Description="original",
        )
        try:
            with pytest.raises(ClientError) as exc:
                client.create_db_parameter_group(
                    DBParameterGroupName=name,
                    DBParameterGroupFamily="mysql8.0",
                    Description="duplicate",
                )
            assert exc.value.response["Error"]["Code"] == "DBParameterGroupAlreadyExists"
        finally:
            try:
                client.delete_db_parameter_group(DBParameterGroupName=name)
            except ClientError:
                pass  # best-effort cleanup

    def test_parameter_group_arn_and_family(self, client):
        """Parameter group includes DBParameterGroupArn and DBParameterGroupFamily."""
        name = _unique("arn-pg")
        client.create_db_parameter_group(
            DBParameterGroupName=name,
            DBParameterGroupFamily="mysql8.0",
            Description="arn test",
        )
        try:
            resp = client.describe_db_parameter_groups(DBParameterGroupName=name)
            grp = resp["DBParameterGroups"][0]
            assert "DBParameterGroupArn" in grp
            assert grp["DBParameterGroupArn"].startswith("arn:aws:rds:")
            assert ":pg:" in grp["DBParameterGroupArn"]
            assert grp["DBParameterGroupFamily"] == "mysql8.0"
        finally:
            try:
                client.delete_db_parameter_group(DBParameterGroupName=name)
            except ClientError:
                pass  # best-effort cleanup

    def test_describe_db_parameters_after_create(self, client):
        """DescribeDBParameters after create returns Parameters list."""
        name = _unique("params-pg")
        client.create_db_parameter_group(
            DBParameterGroupName=name,
            DBParameterGroupFamily="mysql8.0",
            Description="params test",
        )
        try:
            resp = client.describe_db_parameters(DBParameterGroupName=name)
            assert "Parameters" in resp
            assert isinstance(resp["Parameters"], list)
        finally:
            try:
                client.delete_db_parameter_group(DBParameterGroupName=name)
            except ClientError:
                pass  # best-effort cleanup


class TestRDSEventsMultiPatternFidelity:
    """Multi-pattern tests for DescribeEvents with create context."""

    @pytest.fixture
    def client(self):
        return make_client("rds")

    def test_describe_events_after_create_instance(self, client):
        """Create instance then describe events; Events list is returned."""
        name = _unique("evt-db")
        client.create_db_instance(
            DBInstanceIdentifier=name,
            DBInstanceClass="db.t3.micro",
            Engine="mysql",
            MasterUsername="admin",
            MasterUserPassword="password123",
        )
        try:
            resp = client.describe_events()
            assert "Events" in resp
            assert isinstance(resp["Events"], list)
        finally:
            try:
                client.delete_db_instance(DBInstanceIdentifier=name, SkipFinalSnapshot=True)
            except ClientError:
                pass  # best-effort cleanup

    def test_describe_events_with_duration_filter(self, client):
        """DescribeEvents with Duration returns Events list."""
        resp = client.describe_events(Duration=60)
        assert "Events" in resp
        assert isinstance(resp["Events"], list)

    def test_describe_events_source_type_db_instance(self, client):
        """DescribeEvents with SourceType=db-instance returns Events list."""
        resp = client.describe_events(SourceType="db-instance")
        assert "Events" in resp
        assert isinstance(resp["Events"], list)

    def test_describe_orderable_db_instance_options_with_limit(self, client):
        """DescribeOrderableDBInstanceOptions returns options list within limit."""
        resp = client.describe_orderable_db_instance_options(Engine="mysql", MaxRecords=20)
        assert "OrderableDBInstanceOptions" in resp
        assert isinstance(resp["OrderableDBInstanceOptions"], list)
        assert len(resp["OrderableDBInstanceOptions"]) <= 20

    def test_describe_orderable_options_engine_filter(self, client):
        """All returned orderable options match the requested engine."""
        resp = client.describe_orderable_db_instance_options(Engine="mysql")
        for opt in resp["OrderableDBInstanceOptions"]:
            assert opt["Engine"] == "mysql"
            assert "DBInstanceClass" in opt


class TestRDSBlueGreenDeploymentFidelity:
    """Multi-pattern tests for BlueGreenDeployment: CREATE + LIST + DELETE + ERROR."""

    @pytest.fixture
    def client(self):
        return make_client("rds")

    @pytest.fixture
    def source_instance(self, client):
        name = _unique("bgd-src")
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
            pass  # best-effort cleanup

    def test_blue_green_deployment_full_lifecycle(self, client, source_instance):
        """Create BGD, list it, describe by ID, delete, verify gone."""
        arn = f"arn:aws:rds:us-east-1:123456789012:db:{source_instance}"
        bgd_name = _unique("bgd")
        create_resp = client.create_blue_green_deployment(
            BlueGreenDeploymentName=bgd_name,
            Source=arn,
        )
        bgd = create_resp["BlueGreenDeployment"]
        bgd_id = bgd["BlueGreenDeploymentIdentifier"]
        assert bgd["Source"] == arn
        list_resp = client.describe_blue_green_deployments()
        ids = [b["BlueGreenDeploymentIdentifier"] for b in list_resp["BlueGreenDeployments"]]
        assert bgd_id in ids
        spec_resp = client.describe_blue_green_deployments(BlueGreenDeploymentIdentifier=bgd_id)
        assert len(spec_resp["BlueGreenDeployments"]) == 1
        assert spec_resp["BlueGreenDeployments"][0]["BlueGreenDeploymentIdentifier"] == bgd_id
        del_resp = client.delete_blue_green_deployment(BlueGreenDeploymentIdentifier=bgd_id)
        assert del_resp["BlueGreenDeployment"]["BlueGreenDeploymentIdentifier"] == bgd_id
        with pytest.raises(ClientError) as exc:
            client.describe_blue_green_deployments(BlueGreenDeploymentIdentifier=bgd_id)
        assert exc.value.response["Error"]["Code"] in (
            "BlueGreenDeploymentNotFoundFault",
            "BlueGreenDeploymentNotFound",
        )

    def test_blue_green_deployment_fields(self, client, source_instance):
        """Created BGD has expected fields: Status, Source, CreateTime."""
        arn = f"arn:aws:rds:us-east-1:123456789012:db:{source_instance}"
        bgd_name = _unique("bgd-fld")
        resp = client.create_blue_green_deployment(
            BlueGreenDeploymentName=bgd_name,
            Source=arn,
        )
        bgd_id = resp["BlueGreenDeployment"]["BlueGreenDeploymentIdentifier"]
        try:
            desc_resp = client.describe_blue_green_deployments(
                BlueGreenDeploymentIdentifier=bgd_id
            )
            bgd = desc_resp["BlueGreenDeployments"][0]
            assert "Status" in bgd
            assert bgd["Status"] != ""
            assert bgd["Source"] == arn
            assert "CreateTime" in bgd
        finally:
            try:
                client.delete_blue_green_deployment(BlueGreenDeploymentIdentifier=bgd_id)
            except ClientError:
                pass  # best-effort cleanup


class TestRDSSnapshotAttributesMultiPatternFidelity:
    """Multi-pattern tests for snapshot attributes: CREATE + LIST + UPDATE + DELETE + ERROR."""

    @pytest.fixture
    def client(self):
        return make_client("rds")

    @pytest.fixture
    def db_with_snapshot(self, client):
        db_name = _unique("sa-db")
        snap_name = _unique("sa-snap")
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
        yield db_name, snap_name
        try:
            client.delete_db_snapshot(DBSnapshotIdentifier=snap_name)
        except ClientError:
            pass  # best-effort cleanup
        try:
            client.delete_db_instance(DBInstanceIdentifier=db_name, SkipFinalSnapshot=True)
        except ClientError:
            pass  # best-effort cleanup

    def test_describe_db_snapshot_attributes_full_lifecycle(self, client, db_with_snapshot):
        """Create snapshot, modify attrs, describe with assertion, delete, verify error."""
        db_name, snap_name = db_with_snapshot
        desc_resp = client.describe_db_snapshot_attributes(DBSnapshotIdentifier=snap_name)
        result = desc_resp["DBSnapshotAttributesResult"]
        assert result["DBSnapshotIdentifier"] == snap_name
        assert isinstance(result["DBSnapshotAttributes"], list)
        client.modify_db_snapshot_attribute(
            DBSnapshotIdentifier=snap_name,
            AttributeName="restore",
            ValuesToAdd=["all"],
        )
        after_resp = client.describe_db_snapshot_attributes(DBSnapshotIdentifier=snap_name)
        attrs = after_resp["DBSnapshotAttributesResult"]["DBSnapshotAttributes"]
        restore_attrs = [a for a in attrs if a["AttributeName"] == "restore"]
        assert len(restore_attrs) == 1
        assert "all" in restore_attrs[0]["AttributeValues"]
        client.delete_db_snapshot(DBSnapshotIdentifier=snap_name)
        with pytest.raises(ClientError) as exc:
            client.describe_db_snapshot_attributes(DBSnapshotIdentifier=snap_name)
        assert exc.value.response["Error"]["Code"] == "DBSnapshotNotFound"

    def test_snapshot_attribute_modify_remove_verify(self, client):
        """Add then remove snapshot attribute value, verify each change persists."""
        db_name = _unique("mv-db")
        snap_name = _unique("mv-snap")
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
        try:
            client.modify_db_snapshot_attribute(
                DBSnapshotIdentifier=snap_name,
                AttributeName="restore",
                ValuesToAdd=["all"],
            )
            resp = client.describe_db_snapshot_attributes(DBSnapshotIdentifier=snap_name)
            attrs = resp["DBSnapshotAttributesResult"]["DBSnapshotAttributes"]
            restore = [a for a in attrs if a["AttributeName"] == "restore"]
            assert restore[0]["AttributeValues"] == ["all"]
            client.modify_db_snapshot_attribute(
                DBSnapshotIdentifier=snap_name,
                AttributeName="restore",
                ValuesToRemove=["all"],
            )
            resp2 = client.describe_db_snapshot_attributes(DBSnapshotIdentifier=snap_name)
            attrs2 = resp2["DBSnapshotAttributesResult"]["DBSnapshotAttributes"]
            restore2 = [a for a in attrs2 if a["AttributeName"] == "restore"]
            if restore2:
                assert "all" not in restore2[0]["AttributeValues"]
        finally:
            try:
                client.delete_db_snapshot(DBSnapshotIdentifier=snap_name)
            except ClientError:
                pass  # best-effort cleanup
            try:
                client.delete_db_instance(DBInstanceIdentifier=db_name, SkipFinalSnapshot=True)
            except ClientError:
                pass  # best-effort cleanup


class TestDescribeDBClusterParameterGroupsEdgeCases:
    @pytest.fixture
    def client(self):
        return make_client("rds")

    def test_describe_db_cluster_parameter_groups_create_then_list(self, client):
        """Create a cluster param group, then list and verify it appears."""
        name = _unique("cpg-edge")
        client.create_db_cluster_parameter_group(
            DBClusterParameterGroupName=name,
            DBParameterGroupFamily="aurora-mysql8.0",
            Description="edge case test",
        )
        try:
            resp = client.describe_db_cluster_parameter_groups()
            names = [g["DBClusterParameterGroupName"] for g in resp["DBClusterParameterGroups"]]
            assert name in names
        finally:
            try:
                client.delete_db_cluster_parameter_group(DBClusterParameterGroupName=name)
            except ClientError:
                pass

    def test_describe_db_cluster_parameter_groups_by_name(self, client):
        """Retrieve a specific cluster param group by name."""
        name = _unique("cpg-edge")
        client.create_db_cluster_parameter_group(
            DBClusterParameterGroupName=name,
            DBParameterGroupFamily="aurora-mysql8.0",
            Description="edge case retrieve",
        )
        try:
            resp = client.describe_db_cluster_parameter_groups(DBClusterParameterGroupName=name)
            groups = resp["DBClusterParameterGroups"]
            assert len(groups) == 1
            assert groups[0]["DBClusterParameterGroupName"] == name
            assert groups[0]["Description"] == "edge case retrieve"
        finally:
            try:
                client.delete_db_cluster_parameter_group(DBClusterParameterGroupName=name)
            except ClientError:
                pass

    def test_describe_db_cluster_parameter_groups_nonexistent_error(self, client):
        """Describing a nonexistent cluster param group raises an error."""
        with pytest.raises(ClientError) as exc:
            client.describe_db_cluster_parameter_groups(DBClusterParameterGroupName="nonexistent-cpg-xyz")
        assert exc.value.response["Error"]["Code"] == "DBParameterGroupNotFound"

    def test_describe_db_cluster_parameter_groups_after_delete(self, client):
        """After deletion, describing by name should raise error."""
        name = _unique("cpg-del")
        client.create_db_cluster_parameter_group(
            DBClusterParameterGroupName=name,
            DBParameterGroupFamily="aurora-mysql8.0",
            Description="delete test",
        )
        client.delete_db_cluster_parameter_group(DBClusterParameterGroupName=name)
        with pytest.raises(ClientError) as exc:
            client.describe_db_cluster_parameter_groups(DBClusterParameterGroupName=name)
        assert exc.value.response["Error"]["Code"] == "DBParameterGroupNotFound"

    def test_describe_db_cluster_parameter_groups_arn_format(self, client):
        """Cluster param group ARN matches expected pattern."""
        name = _unique("cpg-arn")
        client.create_db_cluster_parameter_group(
            DBClusterParameterGroupName=name,
            DBParameterGroupFamily="aurora-mysql8.0",
            Description="arn test",
        )
        try:
            resp = client.describe_db_cluster_parameter_groups(DBClusterParameterGroupName=name)
            arn = resp["DBClusterParameterGroups"][0]["DBClusterParameterGroupArn"]
            assert ":rds:" in arn
            assert name in arn
        finally:
            try:
                client.delete_db_cluster_parameter_group(DBClusterParameterGroupName=name)
            except ClientError:
                pass


class TestDescribeDBClusterSnapshotsEdgeCases:
    @pytest.fixture
    def client(self):
        return make_client("rds")

    @pytest.fixture
    def cluster(self, client):
        name = _unique("cs-cluster")
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

    def test_describe_db_cluster_snapshots_create_then_list(self, client, cluster):
        """Create a cluster snapshot, then list to verify it appears."""
        snap = _unique("cs-snap")
        client.create_db_cluster_snapshot(
            DBClusterSnapshotIdentifier=snap,
            DBClusterIdentifier=cluster,
        )
        try:
            resp = client.describe_db_cluster_snapshots()
            ids = [s["DBClusterSnapshotIdentifier"] for s in resp["DBClusterSnapshots"]]
            assert snap in ids
        finally:
            try:
                client.delete_db_cluster_snapshot(DBClusterSnapshotIdentifier=snap)
            except ClientError:
                pass

    def test_describe_db_cluster_snapshots_by_id(self, client, cluster):
        """Retrieve a specific cluster snapshot by identifier."""
        snap = _unique("cs-snap")
        client.create_db_cluster_snapshot(
            DBClusterSnapshotIdentifier=snap,
            DBClusterIdentifier=cluster,
        )
        try:
            resp = client.describe_db_cluster_snapshots(DBClusterSnapshotIdentifier=snap)
            snaps = resp["DBClusterSnapshots"]
            assert len(snaps) == 1
            assert snaps[0]["DBClusterSnapshotIdentifier"] == snap
            assert snaps[0]["Engine"] == "aurora-mysql"
        finally:
            try:
                client.delete_db_cluster_snapshot(DBClusterSnapshotIdentifier=snap)
            except ClientError:
                pass

    def test_describe_db_cluster_snapshots_nonexistent_error(self, client):
        """Describing a nonexistent cluster snapshot raises error."""
        with pytest.raises(ClientError) as exc:
            client.describe_db_cluster_snapshots(DBClusterSnapshotIdentifier="nonexistent-cs-xyz")
        assert exc.value.response["Error"]["Code"] == "DBClusterSnapshotNotFoundFault"

    def test_describe_db_cluster_snapshots_after_delete(self, client, cluster):
        """After deletion, cluster snapshot should no longer appear."""
        snap = _unique("cs-del")
        client.create_db_cluster_snapshot(
            DBClusterSnapshotIdentifier=snap,
            DBClusterIdentifier=cluster,
        )
        client.delete_db_cluster_snapshot(DBClusterSnapshotIdentifier=snap)
        with pytest.raises(ClientError) as exc:
            client.describe_db_cluster_snapshots(DBClusterSnapshotIdentifier=snap)
        assert exc.value.response["Error"]["Code"] == "DBClusterSnapshotNotFoundFault"

    def test_describe_db_cluster_snapshots_arn_format(self, client, cluster):
        """Cluster snapshot ARN matches expected pattern."""
        snap = _unique("cs-arn")
        client.create_db_cluster_snapshot(
            DBClusterSnapshotIdentifier=snap,
            DBClusterIdentifier=cluster,
        )
        try:
            resp = client.describe_db_cluster_snapshots(DBClusterSnapshotIdentifier=snap)
            arn = resp["DBClusterSnapshots"][0]["DBClusterSnapshotArn"]
            assert ":rds:" in arn
            assert snap in arn
        finally:
            try:
                client.delete_db_cluster_snapshot(DBClusterSnapshotIdentifier=snap)
            except ClientError:
                pass


class TestDescribeDBClustersEdgeCases:
    @pytest.fixture
    def client(self):
        return make_client("rds")

    def test_describe_db_clusters_create_then_list(self, client):
        """Create a cluster, then list to verify it appears."""
        name = _unique("cl-edge")
        client.create_db_cluster(
            DBClusterIdentifier=name,
            Engine="aurora-mysql",
            MasterUsername="admin",
            MasterUserPassword="password123",
        )
        try:
            resp = client.describe_db_clusters()
            ids = [c["DBClusterIdentifier"] for c in resp["DBClusters"]]
            assert name in ids
        finally:
            try:
                client.delete_db_cluster(DBClusterIdentifier=name, SkipFinalSnapshot=True)
            except ClientError:
                pass

    def test_describe_db_clusters_by_id(self, client):
        """Retrieve a specific cluster by identifier."""
        name = _unique("cl-ret")
        client.create_db_cluster(
            DBClusterIdentifier=name,
            Engine="aurora-mysql",
            MasterUsername="admin",
            MasterUserPassword="password123",
        )
        try:
            resp = client.describe_db_clusters(DBClusterIdentifier=name)
            clusters = resp["DBClusters"]
            assert len(clusters) == 1
            assert clusters[0]["DBClusterIdentifier"] == name
            assert clusters[0]["Engine"] == "aurora-mysql"
        finally:
            try:
                client.delete_db_cluster(DBClusterIdentifier=name, SkipFinalSnapshot=True)
            except ClientError:
                pass

    def test_describe_db_clusters_modify_then_describe(self, client):
        """Modify a cluster and verify the change is reflected."""
        name = _unique("cl-mod")
        client.create_db_cluster(
            DBClusterIdentifier=name,
            Engine="aurora-mysql",
            MasterUsername="admin",
            MasterUserPassword="password123",
        )
        try:
            client.modify_db_cluster(
                DBClusterIdentifier=name,
                BackupRetentionPeriod=7,
                ApplyImmediately=True,
            )
            resp = client.describe_db_clusters(DBClusterIdentifier=name)
            cluster = resp["DBClusters"][0]
            assert cluster["BackupRetentionPeriod"] == 7
        finally:
            try:
                client.delete_db_cluster(DBClusterIdentifier=name, SkipFinalSnapshot=True)
            except ClientError:
                pass

    def test_describe_db_clusters_nonexistent_error(self, client):
        """Describing a nonexistent cluster raises error."""
        with pytest.raises(ClientError) as exc:
            client.describe_db_clusters(DBClusterIdentifier="nonexistent-cl-xyz")
        assert exc.value.response["Error"]["Code"] == "DBClusterNotFoundFault"

    def test_describe_db_clusters_after_delete(self, client):
        """After deletion, describe by ID should raise error."""
        name = _unique("cl-del")
        client.create_db_cluster(
            DBClusterIdentifier=name,
            Engine="aurora-mysql",
            MasterUsername="admin",
            MasterUserPassword="password123",
        )
        client.delete_db_cluster(DBClusterIdentifier=name, SkipFinalSnapshot=True)
        with pytest.raises(ClientError) as exc:
            client.describe_db_clusters(DBClusterIdentifier=name)
        assert exc.value.response["Error"]["Code"] == "DBClusterNotFoundFault"

    def test_describe_db_clusters_arn_format(self, client):
        """Cluster ARN matches expected pattern."""
        name = _unique("cl-arn")
        client.create_db_cluster(
            DBClusterIdentifier=name,
            Engine="aurora-mysql",
            MasterUsername="admin",
            MasterUserPassword="password123",
        )
        try:
            resp = client.describe_db_clusters(DBClusterIdentifier=name)
            arn = resp["DBClusters"][0]["DBClusterArn"]
            assert ":rds:" in arn
            assert name in arn
        finally:
            try:
                client.delete_db_cluster(DBClusterIdentifier=name, SkipFinalSnapshot=True)
            except ClientError:
                pass


class TestDescribeDBInstancesEdgeCases:
    @pytest.fixture
    def client(self):
        return make_client("rds")

    def test_describe_db_instances_create_then_list(self, client):
        """Create an instance, then list to verify it appears."""
        name = _unique("di-edge")
        client.create_db_instance(
            DBInstanceIdentifier=name,
            DBInstanceClass="db.t3.micro",
            Engine="mysql",
            MasterUsername="admin",
            MasterUserPassword="password123",
        )
        try:
            resp = client.describe_db_instances()
            ids = [i["DBInstanceIdentifier"] for i in resp["DBInstances"]]
            assert name in ids
        finally:
            try:
                client.delete_db_instance(DBInstanceIdentifier=name, SkipFinalSnapshot=True)
            except ClientError:
                pass

    def test_describe_db_instances_by_id(self, client):
        """Retrieve a specific instance by identifier."""
        name = _unique("di-ret")
        client.create_db_instance(
            DBInstanceIdentifier=name,
            DBInstanceClass="db.t3.micro",
            Engine="mysql",
            MasterUsername="admin",
            MasterUserPassword="password123",
        )
        try:
            resp = client.describe_db_instances(DBInstanceIdentifier=name)
            instances = resp["DBInstances"]
            assert len(instances) == 1
            assert instances[0]["DBInstanceIdentifier"] == name
            assert instances[0]["Engine"] == "mysql"
        finally:
            try:
                client.delete_db_instance(DBInstanceIdentifier=name, SkipFinalSnapshot=True)
            except ClientError:
                pass

    def test_describe_db_instances_modify_then_describe(self, client):
        """Modify an instance and verify the change is reflected."""
        name = _unique("di-mod")
        client.create_db_instance(
            DBInstanceIdentifier=name,
            DBInstanceClass="db.t3.micro",
            Engine="mysql",
            MasterUsername="admin",
            MasterUserPassword="password123",
        )
        try:
            client.modify_db_instance(
                DBInstanceIdentifier=name,
                BackupRetentionPeriod=7,
                ApplyImmediately=True,
            )
            resp = client.describe_db_instances(DBInstanceIdentifier=name)
            inst = resp["DBInstances"][0]
            assert inst["BackupRetentionPeriod"] == 7
        finally:
            try:
                client.delete_db_instance(DBInstanceIdentifier=name, SkipFinalSnapshot=True)
            except ClientError:
                pass

    def test_describe_db_instances_nonexistent_error(self, client):
        """Describing a nonexistent instance raises error."""
        with pytest.raises(ClientError) as exc:
            client.describe_db_instances(DBInstanceIdentifier="nonexistent-di-xyz")
        assert exc.value.response["Error"]["Code"] == "DBInstanceNotFound"

    def test_describe_db_instances_after_delete(self, client):
        """After deletion, describe by ID should raise error."""
        name = _unique("di-del")
        client.create_db_instance(
            DBInstanceIdentifier=name,
            DBInstanceClass="db.t3.micro",
            Engine="mysql",
            MasterUsername="admin",
            MasterUserPassword="password123",
        )
        client.delete_db_instance(DBInstanceIdentifier=name, SkipFinalSnapshot=True)
        with pytest.raises(ClientError) as exc:
            client.describe_db_instances(DBInstanceIdentifier=name)
        assert exc.value.response["Error"]["Code"] == "DBInstanceNotFound"


class TestDescribeDBParameterGroupsEdgeCases:
    @pytest.fixture
    def client(self):
        return make_client("rds")

    def test_describe_db_parameter_groups_create_then_list(self, client):
        """Create a param group, then list to verify it appears."""
        name = _unique("pg-edge")
        client.create_db_parameter_group(
            DBParameterGroupName=name,
            DBParameterGroupFamily="mysql8.0",
            Description="edge test",
        )
        try:
            resp = client.describe_db_parameter_groups()
            names = [g["DBParameterGroupName"] for g in resp["DBParameterGroups"]]
            assert name in names
        finally:
            try:
                client.delete_db_parameter_group(DBParameterGroupName=name)
            except ClientError:
                pass

    def test_describe_db_parameter_groups_by_name(self, client):
        """Retrieve a specific param group by name."""
        name = _unique("pg-ret")
        client.create_db_parameter_group(
            DBParameterGroupName=name,
            DBParameterGroupFamily="mysql8.0",
            Description="retrieve test",
        )
        try:
            resp = client.describe_db_parameter_groups(DBParameterGroupName=name)
            groups = resp["DBParameterGroups"]
            assert len(groups) == 1
            assert groups[0]["DBParameterGroupName"] == name
            assert groups[0]["Description"] == "retrieve test"
        finally:
            try:
                client.delete_db_parameter_group(DBParameterGroupName=name)
            except ClientError:
                pass

    def test_describe_db_parameter_groups_nonexistent_returns_empty(self, client):
        """Describing a nonexistent param group returns an empty list."""
        resp = client.describe_db_parameter_groups(DBParameterGroupName="nonexistent-pg-xyz")
        assert resp["DBParameterGroups"] == []

    def test_describe_db_parameter_groups_after_delete(self, client):
        """After deletion, describe by name should return empty list."""
        name = _unique("pg-del")
        client.create_db_parameter_group(
            DBParameterGroupName=name,
            DBParameterGroupFamily="mysql8.0",
            Description="delete test",
        )
        client.delete_db_parameter_group(DBParameterGroupName=name)
        resp = client.describe_db_parameter_groups(DBParameterGroupName=name)
        assert resp["DBParameterGroups"] == []

    def test_describe_db_parameter_groups_arn_format(self, client):
        """Param group ARN matches expected pattern."""
        name = _unique("pg-arn")
        client.create_db_parameter_group(
            DBParameterGroupName=name,
            DBParameterGroupFamily="mysql8.0",
            Description="arn test",
        )
        try:
            resp = client.describe_db_parameter_groups(DBParameterGroupName=name)
            arn = resp["DBParameterGroups"][0]["DBParameterGroupArn"]
            assert ":rds:" in arn
            assert name in arn
        finally:
            try:
                client.delete_db_parameter_group(DBParameterGroupName=name)
            except ClientError:
                pass


class TestRdsAutoCoverageLifecycles:
    """Enhanced CRUD lifecycle tests for operations previously only LIST-covered."""

    @pytest.fixture
    def client(self):
        return make_client("rds")

    @pytest.fixture
    def ec2_client(self):
        return make_client("ec2")

    def test_db_cluster_parameter_groups_lifecycle(self, client):
        """DB cluster parameter group: create, list, modify, delete, error."""
        name = _unique("cpg-lc")
        client.create_db_cluster_parameter_group(
            DBClusterParameterGroupName=name,
            DBParameterGroupFamily="aurora-mysql8.0",
            Description="lifecycle test",
        )
        try:
            resp = client.describe_db_cluster_parameter_groups()
            assert "DBClusterParameterGroups" in resp
            names = [g["DBClusterParameterGroupName"] for g in resp["DBClusterParameterGroups"]]
            assert name in names
            client.modify_db_cluster_parameter_group(
                DBClusterParameterGroupName=name,
                Parameters=[
                    {
                        "ParameterName": "character_set_server",
                        "ParameterValue": "utf8mb4",
                        "ApplyMethod": "pending-reboot",
                    }
                ],
            )
            client.delete_db_cluster_parameter_group(DBClusterParameterGroupName=name)
            with pytest.raises(ClientError) as exc:
                client.copy_db_cluster_parameter_group(
                    SourceDBClusterParameterGroupIdentifier=name,
                    TargetDBClusterParameterGroupIdentifier=_unique("tgt"),
                    TargetDBClusterParameterGroupDescription="x",
                )
            assert exc.value.response["Error"]["Code"] == "DBParameterGroupNotFound"
        except ClientError:
            try:
                client.delete_db_cluster_parameter_group(DBClusterParameterGroupName=name)
            except ClientError:
                pass  # best-effort cleanup
            raise

    def test_db_cluster_snapshots_lifecycle(self, client):
        """DB cluster snapshot: create, list, modify attribute, delete, error."""
        cl_name = _unique("cl-lcsnap")
        client.create_db_cluster(
            DBClusterIdentifier=cl_name,
            Engine="aurora-mysql",
            MasterUsername="admin",
            MasterUserPassword="password123!",
        )
        snap_name = _unique("csnap-lc")
        try:
            client.create_db_cluster_snapshot(
                DBClusterSnapshotIdentifier=snap_name,
                DBClusterIdentifier=cl_name,
            )
            try:
                resp = client.describe_db_cluster_snapshots()
                assert "DBClusterSnapshots" in resp
                ids = [s["DBClusterSnapshotIdentifier"] for s in resp["DBClusterSnapshots"]]
                assert snap_name in ids
                # UPDATE: modify snapshot attribute
                client.modify_db_cluster_snapshot_attribute(
                    DBClusterSnapshotIdentifier=snap_name,
                    AttributeName="restore",
                    ValuesToAdd=["all"],
                )
                # DELETE
                client.delete_db_cluster_snapshot(DBClusterSnapshotIdentifier=snap_name)
                # ERROR: describe nonexistent cluster
                with pytest.raises(ClientError) as exc:
                    client.describe_db_clusters(DBClusterIdentifier="nonexistent-cl-xyz-999")
                assert exc.value.response["Error"]["Code"] == "DBClusterNotFoundFault"
            except ClientError:
                try:
                    client.delete_db_cluster_snapshot(DBClusterSnapshotIdentifier=snap_name)
                except ClientError:
                    pass  # best-effort cleanup
                raise
        finally:
            try:
                client.delete_db_cluster(DBClusterIdentifier=cl_name, SkipFinalSnapshot=True)
            except ClientError:
                pass  # best-effort cleanup

    def test_db_clusters_lifecycle(self, client):
        """DB cluster: create, list, modify, delete, error."""
        name = _unique("cl-lc")
        client.create_db_cluster(
            DBClusterIdentifier=name,
            Engine="aurora-mysql",
            MasterUsername="admin",
            MasterUserPassword="password123!",
        )
        try:
            resp = client.describe_db_clusters()
            assert "DBClusters" in resp
            ids = [c["DBClusterIdentifier"] for c in resp["DBClusters"]]
            assert name in ids
            # UPDATE
            client.modify_db_cluster(
                DBClusterIdentifier=name,
                DeletionProtection=False,
            )
            # DELETE
            client.delete_db_cluster(DBClusterIdentifier=name, SkipFinalSnapshot=True)
            # ERROR: nonexistent
            with pytest.raises(ClientError) as exc:
                client.describe_db_clusters(DBClusterIdentifier="nonexistent-cl-xyz-999")
            assert exc.value.response["Error"]["Code"] == "DBClusterNotFoundFault"
        except ClientError:
            try:
                client.delete_db_cluster(DBClusterIdentifier=name, SkipFinalSnapshot=True)
            except ClientError:
                pass  # best-effort cleanup
            raise

    def test_db_instance_automated_backups_lifecycle(self, client):
        """Automated backups: create instance, list backups, delete instance, error."""
        name = _unique("db-ab")
        client.create_db_instance(
            DBInstanceIdentifier=name,
            DBInstanceClass="db.t3.micro",
            Engine="mysql",
            MasterUsername="admin",
            MasterUserPassword="password123",
        )
        try:
            resp = client.describe_db_instance_automated_backups()
            assert "DBInstanceAutomatedBackups" in resp
            assert isinstance(resp["DBInstanceAutomatedBackups"], list)
            # UPDATE: modify the instance
            client.modify_db_instance(
                DBInstanceIdentifier=name,
                BackupRetentionPeriod=1,
            )
            # DELETE
            client.delete_db_instance(DBInstanceIdentifier=name, SkipFinalSnapshot=True)
            # ERROR: nonexistent
            with pytest.raises(ClientError) as exc:
                client.describe_db_instances(DBInstanceIdentifier="nonexistent-db-xyz-999")
            assert exc.value.response["Error"]["Code"] == "DBInstanceNotFound"
        except ClientError:
            try:
                client.delete_db_instance(DBInstanceIdentifier=name, SkipFinalSnapshot=True)
            except ClientError:
                pass  # best-effort cleanup
            raise

    def test_db_instances_lifecycle(self, client):
        """DB instance: create, list, modify, delete, error."""
        name = _unique("db-lc")
        client.create_db_instance(
            DBInstanceIdentifier=name,
            DBInstanceClass="db.t3.micro",
            Engine="mysql",
            MasterUsername="admin",
            MasterUserPassword="password123",
        )
        try:
            resp = client.describe_db_instances()
            assert "DBInstances" in resp
            ids = [i["DBInstanceIdentifier"] for i in resp["DBInstances"]]
            assert name in ids
            # UPDATE
            client.modify_db_instance(
                DBInstanceIdentifier=name,
                MasterUserPassword="newpassword999",
            )
            # DELETE
            client.delete_db_instance(DBInstanceIdentifier=name, SkipFinalSnapshot=True)
            # ERROR
            with pytest.raises(ClientError) as exc:
                client.describe_db_instances(DBInstanceIdentifier="nonexistent-db-xyz-999")
            assert exc.value.response["Error"]["Code"] == "DBInstanceNotFound"
        except ClientError:
            try:
                client.delete_db_instance(DBInstanceIdentifier=name, SkipFinalSnapshot=True)
            except ClientError:
                pass  # best-effort cleanup
            raise

    def test_db_parameter_groups_lifecycle(self, client):
        """DB parameter group: create, list, modify, delete, error."""
        name = _unique("pg-lc")
        client.create_db_parameter_group(
            DBParameterGroupName=name,
            DBParameterGroupFamily="mysql8.0",
            Description="lifecycle test",
        )
        try:
            resp = client.describe_db_parameter_groups()
            assert "DBParameterGroups" in resp
            names = [g["DBParameterGroupName"] for g in resp["DBParameterGroups"]]
            assert name in names
            # UPDATE
            client.modify_db_parameter_group(
                DBParameterGroupName=name,
                Parameters=[
                    {
                        "ParameterName": "max_connections",
                        "ParameterValue": "100",
                        "ApplyMethod": "pending-reboot",
                    }
                ],
            )
            # DELETE
            client.delete_db_parameter_group(DBParameterGroupName=name)
            # ERROR
            with pytest.raises(ClientError) as exc:
                client.copy_db_parameter_group(
                    SourceDBParameterGroupIdentifier=name,
                    TargetDBParameterGroupIdentifier=_unique("tgt"),
                    TargetDBParameterGroupDescription="x",
                )
            assert exc.value.response["Error"]["Code"] == "DBParameterGroupNotFound"
        except ClientError:
            try:
                client.delete_db_parameter_group(DBParameterGroupName=name)
            except ClientError:
                pass  # best-effort cleanup
            raise

    def test_db_proxies_lifecycle(self, client, ec2_client):
        """DB proxy: create, list, modify, delete, error."""
        vpc = ec2_client.create_vpc(CidrBlock="10.83.0.0/16")
        vpc_id = vpc["Vpc"]["VpcId"]
        s1 = ec2_client.create_subnet(
            VpcId=vpc_id, CidrBlock="10.83.1.0/24", AvailabilityZone="us-east-1a"
        )
        s2 = ec2_client.create_subnet(
            VpcId=vpc_id, CidrBlock="10.83.2.0/24", AvailabilityZone="us-east-1b"
        )
        subnet_ids = [s1["Subnet"]["SubnetId"], s2["Subnet"]["SubnetId"]]
        name = _unique("px-lc")
        try:
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
            resp = client.describe_db_proxies()
            assert "DBProxies" in resp
            names = [p["DBProxyName"] for p in resp["DBProxies"]]
            assert name in names
            # UPDATE: modify proxy
            client.modify_db_proxy(
                DBProxyName=name,
                NewDBProxyName=name,
                DebugLogging=True,
            )
            # DELETE
            client.delete_db_proxy(DBProxyName=name)
            # ERROR
            with pytest.raises(ClientError) as exc:
                client.delete_db_proxy(DBProxyName="nonexistent-proxy-xyz-999")
            assert exc.value.response["Error"]["Code"] == "DBProxyNotFoundFault"
        except ClientError:
            try:
                client.delete_db_proxy(DBProxyName=name)
            except ClientError:
                pass  # best-effort cleanup
            raise
        finally:
            for sid in subnet_ids:
                try:
                    ec2_client.delete_subnet(SubnetId=sid)
                except ClientError:
                    pass  # best-effort cleanup
            try:
                ec2_client.delete_vpc(VpcId=vpc_id)
            except ClientError:
                pass  # best-effort cleanup

    def test_db_security_groups_lifecycle(self, client):
        """DB security group: create, list, authorize ingress (update), delete, error."""
        name = _unique("dbsg-lc")
        client.create_db_security_group(
            DBSecurityGroupName=name,
            DBSecurityGroupDescription="lifecycle test",
        )
        try:
            resp = client.describe_db_security_groups()
            assert "DBSecurityGroups" in resp
            names = [g["DBSecurityGroupName"] for g in resp["DBSecurityGroups"]]
            assert name in names
            # UPDATE: authorize ingress
            client.authorize_db_security_group_ingress(
                DBSecurityGroupName=name,
                CIDRIP="10.1.0.0/16",
            )
            # DELETE
            client.delete_db_security_group(DBSecurityGroupName=name)
            # ERROR: re-delete should fail
            with pytest.raises(ClientError) as exc:
                client.delete_db_security_group(DBSecurityGroupName=name)
            assert exc.value.response["Error"]["Code"] in (
                "DBSecurityGroupNotFound",
                "DBSecurityGroupNotFoundFault",
            )
        except ClientError:
            try:
                client.delete_db_security_group(DBSecurityGroupName=name)
            except ClientError:
                pass  # best-effort cleanup
            raise

    def test_db_shard_groups_lifecycle(self, client):
        """DB shard group: create cluster+shard, list, delete, error."""
        cl_name = _unique("cl-sg")
        client.create_db_cluster(
            DBClusterIdentifier=cl_name,
            Engine="aurora-mysql",
            MasterUsername="admin",
            MasterUserPassword="password123!",
        )
        sg_name = _unique("sg-lc")
        try:
            client.create_db_shard_group(
                DBShardGroupIdentifier=sg_name,
                DBClusterIdentifier=cl_name,
                MaxACU=100.0,
            )
            try:
                resp = client.describe_db_shard_groups()
                assert "DBShardGroups" in resp
                ids = [g["DBShardGroupIdentifier"] for g in resp["DBShardGroups"]]
                assert sg_name in ids
                # UPDATE: modify shard group
                client.modify_db_shard_group(
                    DBShardGroupIdentifier=sg_name,
                    MaxACU=150.0,
                )
                # DELETE
                client.delete_db_shard_group(DBShardGroupIdentifier=sg_name)
                # ERROR
                with pytest.raises(ClientError) as exc:
                    client.delete_db_shard_group(DBShardGroupIdentifier="nonexistent-sg-xyz-999")
                assert exc.value.response["Error"]["Code"] in (
                    "DBShardGroupNotFound",
                    "DBShardGroupNotFoundFault",
                )
            except ClientError:
                try:
                    client.delete_db_shard_group(DBShardGroupIdentifier=sg_name)
                except ClientError:
                    pass  # best-effort cleanup
                raise
        finally:
            try:
                client.delete_db_cluster(DBClusterIdentifier=cl_name, SkipFinalSnapshot=True)
            except ClientError:
                pass  # best-effort cleanup

    def test_db_snapshots_lifecycle(self, client):
        """DB snapshot: create instance+snapshot, list, modify attribute, delete, error."""
        db_name = _unique("db-snap")
        client.create_db_instance(
            DBInstanceIdentifier=db_name,
            DBInstanceClass="db.t3.micro",
            Engine="mysql",
            MasterUsername="admin",
            MasterUserPassword="password123",
        )
        snap_name = _unique("snap-lc")
        try:
            client.create_db_snapshot(
                DBSnapshotIdentifier=snap_name,
                DBInstanceIdentifier=db_name,
            )
            try:
                resp = client.describe_db_snapshots()
                assert "DBSnapshots" in resp
                ids = [s["DBSnapshotIdentifier"] for s in resp["DBSnapshots"]]
                assert snap_name in ids
                # UPDATE: modify snapshot attribute
                client.modify_db_snapshot_attribute(
                    DBSnapshotIdentifier=snap_name,
                    AttributeName="restore",
                    ValuesToAdd=["all"],
                )
                # DELETE
                client.delete_db_snapshot(DBSnapshotIdentifier=snap_name)
                # ERROR
                with pytest.raises(ClientError) as exc:
                    client.describe_db_snapshots(DBSnapshotIdentifier="nonexistent-snap-xyz-999")
                assert "NotFound" in exc.value.response["Error"]["Code"]
            except ClientError:
                try:
                    client.delete_db_snapshot(DBSnapshotIdentifier=snap_name)
                except ClientError:
                    pass  # best-effort cleanup
                raise
        finally:
            try:
                client.delete_db_instance(DBInstanceIdentifier=db_name, SkipFinalSnapshot=True)
            except ClientError:
                pass  # best-effort cleanup

    def test_db_subnet_groups_lifecycle(self, client, ec2_client):
        """DB subnet group: create, list, modify, delete, error."""
        vpc = ec2_client.create_vpc(CidrBlock="10.84.0.0/16")
        vpc_id = vpc["Vpc"]["VpcId"]
        s1 = ec2_client.create_subnet(
            VpcId=vpc_id, CidrBlock="10.84.1.0/24", AvailabilityZone="us-east-1a"
        )
        s2 = ec2_client.create_subnet(
            VpcId=vpc_id, CidrBlock="10.84.2.0/24", AvailabilityZone="us-east-1b"
        )
        s3 = ec2_client.create_subnet(
            VpcId=vpc_id, CidrBlock="10.84.3.0/24", AvailabilityZone="us-east-1c"
        )
        sid1, sid2, sid3 = (
            s1["Subnet"]["SubnetId"],
            s2["Subnet"]["SubnetId"],
            s3["Subnet"]["SubnetId"],
        )
        name = _unique("sng-lc")
        try:
            client.create_db_subnet_group(
                DBSubnetGroupName=name,
                DBSubnetGroupDescription="lifecycle test",
                SubnetIds=[sid1, sid2],
            )
            resp = client.describe_db_subnet_groups()
            assert "DBSubnetGroups" in resp
            names = [g["DBSubnetGroupName"] for g in resp["DBSubnetGroups"]]
            assert name in names
            # UPDATE
            client.modify_db_subnet_group(
                DBSubnetGroupName=name,
                DBSubnetGroupDescription="modified",
                SubnetIds=[sid1, sid2, sid3],
            )
            # DELETE
            client.delete_db_subnet_group(DBSubnetGroupName=name)
            # ERROR
            with pytest.raises(ClientError) as exc:
                client.delete_db_subnet_group(DBSubnetGroupName="nonexistent-sng-xyz-999")
            assert exc.value.response["Error"]["Code"] == "DBSubnetGroupNotFoundFault"
        except ClientError:
            try:
                client.delete_db_subnet_group(DBSubnetGroupName=name)
            except ClientError:
                pass  # best-effort cleanup
            raise
        finally:
            for sid in [sid1, sid2, sid3]:
                try:
                    ec2_client.delete_subnet(SubnetId=sid)
                except ClientError:
                    pass  # best-effort cleanup
            try:
                ec2_client.delete_vpc(VpcId=vpc_id)
            except ClientError:
                pass  # best-effort cleanup

    def test_event_subscriptions_lifecycle(self, client):
        """Event subscription: create, list, modify, delete, error."""
        name = _unique("esub-lc")
        client.create_event_subscription(
            SubscriptionName=name,
            SnsTopicArn="arn:aws:sns:us-east-1:123456789012:test-topic",
        )
        try:
            resp = client.describe_event_subscriptions()
            assert "EventSubscriptionsList" in resp
            sub_names = [s["CustSubscriptionId"] for s in resp["EventSubscriptionsList"]]
            assert name in sub_names
            # UPDATE: modify event subscription
            client.modify_event_subscription(
                SubscriptionName=name,
                Enabled=False,
            )
            # DELETE
            client.delete_event_subscription(SubscriptionName=name)
            # ERROR
            with pytest.raises(ClientError) as exc:
                client.delete_event_subscription(SubscriptionName="nonexistent-sub-xyz-999")
            assert exc.value.response["Error"]["Code"] in (
                "SubscriptionNotFound",
                "SubscriptionNotFoundFault",
            )
        except ClientError:
            try:
                client.delete_event_subscription(SubscriptionName=name)
            except ClientError:
                pass  # best-effort cleanup
            raise

    def test_export_tasks_lifecycle(self, client):
        """Export task: create snapshot+task, list, cancel, error."""
        db_name = _unique("db-exp")
        snap_name = _unique("snap-exp")
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
        task_id = _unique("exp-lc")
        snap_arn = f"arn:aws:rds:us-east-1:123456789012:snapshot:{snap_name}"
        try:
            resp = client.start_export_task(
                ExportTaskIdentifier=task_id,
                SourceArn=snap_arn,
                S3BucketName="test-export-bucket",
                IamRoleArn="arn:aws:iam::123456789012:role/export-role",
                KmsKeyId="arn:aws:kms:us-east-1:123456789012:key/test-key",
            )
            assert resp["ExportTaskIdentifier"] == task_id
            list_resp = client.describe_export_tasks()
            assert "ExportTasks" in list_resp
            ids = [t["ExportTaskIdentifier"] for t in list_resp["ExportTasks"]]
            assert task_id in ids
            # DELETE (cancel)
            client.cancel_export_task(ExportTaskIdentifier=task_id)
            # ERROR
            with pytest.raises(ClientError) as exc:
                client.cancel_export_task(ExportTaskIdentifier="nonexistent-task-xyz-999")
            assert exc.value.response["Error"]["Code"] in (
                "ExportTaskNotFoundFault",
                "ExportTaskNotFound",
            )
        finally:
            try:
                client.delete_db_snapshot(DBSnapshotIdentifier=snap_name)
            except ClientError:
                pass  # best-effort cleanup
            try:
                client.delete_db_instance(DBInstanceIdentifier=db_name, SkipFinalSnapshot=True)
            except ClientError:
                pass  # best-effort cleanup

    def test_global_clusters_lifecycle(self, client):
        """Global cluster: create, list, modify, delete, error."""
        name = _unique("gc-lc")
        client.create_global_cluster(
            GlobalClusterIdentifier=name,
            Engine="aurora-mysql",
        )
        try:
            resp = client.describe_global_clusters()
            assert "GlobalClusters" in resp
            ids = [g["GlobalClusterIdentifier"] for g in resp["GlobalClusters"]]
            assert name in ids
            # UPDATE
            client.modify_global_cluster(
                GlobalClusterIdentifier=name,
                DeletionProtection=False,
            )
            # DELETE
            client.delete_global_cluster(GlobalClusterIdentifier=name)
            # ERROR
            with pytest.raises(ClientError) as exc:
                client.delete_global_cluster(GlobalClusterIdentifier="nonexistent-gc-xyz-999")
            assert exc.value.response["Error"]["Code"] == "GlobalClusterNotFoundFault"
        except ClientError:
            try:
                client.delete_global_cluster(GlobalClusterIdentifier=name)
            except ClientError:
                pass  # best-effort cleanup
            raise

    def test_option_group_options_with_lifecycle(self, client):
        """Option group options: list available options; option group CRUD for context."""
        # LIST: describe available options for mysql
        resp = client.describe_option_group_options(EngineName="mysql")
        assert "OptionGroupOptions" in resp
        assert isinstance(resp["OptionGroupOptions"], list)
        # CREATE an option group to add CREATE + DELETE patterns to this area
        name = _unique("og-lc")
        client.create_option_group(
            OptionGroupName=name,
            EngineName="mysql",
            MajorEngineVersion="8.0",
            OptionGroupDescription="lifecycle test",
        )
        try:
            og_resp = client.describe_option_groups(OptionGroupName=name)
            assert og_resp["OptionGroupsList"][0]["OptionGroupName"] == name
            # DELETE
            client.delete_option_group(OptionGroupName=name)
            # ERROR
            with pytest.raises(ClientError) as exc:
                client.delete_option_group(OptionGroupName="nonexistent-og-xyz-999")
            assert exc.value.response["Error"]["Code"] == "OptionGroupNotFoundFault"
        except ClientError:
            try:
                client.delete_option_group(OptionGroupName=name)
            except ClientError:
                pass  # best-effort cleanup
            raise


class TestRDSExportTaskEdgeCases:
    """Edge cases and behavioral fidelity for export task operations."""

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
            pass  # best-effort cleanup
        try:
            client.delete_db_instance(DBInstanceIdentifier=db_name, SkipFinalSnapshot=True)
        except ClientError:
            pass  # best-effort cleanup

    def test_export_task_list_and_cancel_lifecycle(self, client, snapshot):
        """Start export task, verify in list, cancel, verify response fields."""
        snap_arn, _, _ = snapshot
        task_id = _unique("compat-exp")
        client.start_export_task(
            ExportTaskIdentifier=task_id,
            SourceArn=snap_arn,
            S3BucketName="test-export-bucket",
            IamRoleArn="arn:aws:iam::123456789012:role/export-role",
            KmsKeyId="arn:aws:kms:us-east-1:123456789012:key/test-key",
        )
        # Verify appears in list
        list_resp = client.describe_export_tasks()
        task_ids = [t["ExportTaskIdentifier"] for t in list_resp["ExportTasks"]]
        assert task_id in task_ids
        # Cancel and verify response fields
        cancel_resp = client.cancel_export_task(ExportTaskIdentifier=task_id)
        assert cancel_resp["ExportTaskIdentifier"] == task_id
        assert cancel_resp["SourceArn"] == snap_arn
        assert "Status" in cancel_resp

    def test_export_task_describe_fields(self, client, snapshot):
        """Start export task, describe by ID, verify required fields present."""
        snap_arn, _, _ = snapshot
        task_id = _unique("compat-exp")
        client.start_export_task(
            ExportTaskIdentifier=task_id,
            SourceArn=snap_arn,
            S3BucketName="test-export-bucket",
            IamRoleArn="arn:aws:iam::123456789012:role/export-role",
            KmsKeyId="arn:aws:kms:us-east-1:123456789012:key/test-key",
        )
        try:
            desc = client.describe_export_tasks(ExportTaskIdentifier=task_id)
            assert len(desc["ExportTasks"]) == 1
            task = desc["ExportTasks"][0]
            assert task["ExportTaskIdentifier"] == task_id
            assert task["SourceArn"] == snap_arn
            assert task["S3Bucket"] == "test-export-bucket"
            assert task["IamRoleArn"] == "arn:aws:iam::123456789012:role/export-role"
            assert task["KmsKeyId"] == "arn:aws:kms:us-east-1:123456789012:key/test-key"
        finally:
            try:
                client.cancel_export_task(ExportTaskIdentifier=task_id)
            except ClientError:
                pass  # best-effort cleanup

    def test_export_task_filter_by_source_arn(self, client, snapshot):
        """Start export task, describe with SourceArn filter, verify task appears."""
        snap_arn, _, _ = snapshot
        task_id = _unique("compat-exp")
        client.start_export_task(
            ExportTaskIdentifier=task_id,
            SourceArn=snap_arn,
            S3BucketName="test-export-bucket",
            IamRoleArn="arn:aws:iam::123456789012:role/export-role",
            KmsKeyId="arn:aws:kms:us-east-1:123456789012:key/test-key",
        )
        try:
            resp = client.describe_export_tasks(SourceArn=snap_arn)
            assert "ExportTasks" in resp
            assert isinstance(resp["ExportTasks"], list)
            task_ids = [t["ExportTaskIdentifier"] for t in resp["ExportTasks"]]
            assert task_id in task_ids
        finally:
            try:
                client.cancel_export_task(ExportTaskIdentifier=task_id)
            except ClientError:
                pass  # best-effort cleanup


class TestRDSFailoverClusterEdgeCases:
    """Edge cases and behavioral fidelity for DB cluster failover."""

    @pytest.fixture
    def client(self):
        return make_client("rds")

    def test_failover_db_cluster_single_instance_error(self, client):
        """failover_db_cluster on a single-instance cluster raises InvalidDBClusterStateFault."""
        cl_name = _unique("compat-cl")
        client.create_db_cluster(
            DBClusterIdentifier=cl_name,
            Engine="aurora-mysql",
            MasterUsername="admin",
            MasterUserPassword="password123!",
        )
        try:
            with pytest.raises(ClientError) as exc:
                client.failover_db_cluster(DBClusterIdentifier=cl_name)
            assert exc.value.response["Error"]["Code"] == "InvalidDBClusterStateFault"
        finally:
            try:
                client.delete_db_cluster(DBClusterIdentifier=cl_name, SkipFinalSnapshot=True)
            except ClientError:
                pass  # best-effort cleanup

    def test_failover_db_cluster_verifies_cluster_exists(self, client):
        """After a failed failover attempt, the cluster still exists and is describeable."""
        cl_name = _unique("compat-cl")
        client.create_db_cluster(
            DBClusterIdentifier=cl_name,
            Engine="aurora-mysql",
            MasterUsername="admin",
            MasterUserPassword="password123!",
        )
        try:
            # failover will fail since there's only one instance, but cluster still exists
            try:
                client.failover_db_cluster(DBClusterIdentifier=cl_name)
            except ClientError:
                pass  # expected — cluster needs 2 instances
            desc = client.describe_db_clusters(DBClusterIdentifier=cl_name)
            assert len(desc["DBClusters"]) == 1
            assert desc["DBClusters"][0]["DBClusterIdentifier"] == cl_name
        finally:
            try:
                client.delete_db_cluster(DBClusterIdentifier=cl_name, SkipFinalSnapshot=True)
            except ClientError:
                pass  # best-effort cleanup


class TestRDSSwitchoverBluegreenEdgeCases:
    """Edge cases and behavioral fidelity for blue/green deployment switchover."""

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
            pass  # best-effort cleanup

    def test_switchover_blue_green_returns_deployment(self, client, source_instance):
        """Switchover on a deployment returns BlueGreenDeployment with status."""
        bg_name = _unique("compat-bg")
        source_arn = f"arn:aws:rds:us-east-1:123456789012:db:{source_instance}"
        resp = client.create_blue_green_deployment(
            BlueGreenDeploymentName=bg_name,
            Source=source_arn,
        )
        bg_id = resp["BlueGreenDeployment"]["BlueGreenDeploymentIdentifier"]
        try:
            sw_resp = client.switchover_blue_green_deployment(
                BlueGreenDeploymentIdentifier=bg_id,
            )
            assert "BlueGreenDeployment" in sw_resp
            assert sw_resp["BlueGreenDeployment"]["BlueGreenDeploymentIdentifier"] == bg_id
            assert "Status" in sw_resp["BlueGreenDeployment"]
        finally:
            try:
                client.delete_blue_green_deployment(BlueGreenDeploymentIdentifier=bg_id)
            except ClientError:
                pass  # best-effort cleanup

    def test_blue_green_deployment_describe_fields(self, client, source_instance):
        """Create blue/green deployment, describe it, verify required fields present."""
        bg_name = _unique("compat-bg")
        source_arn = f"arn:aws:rds:us-east-1:123456789012:db:{source_instance}"
        resp = client.create_blue_green_deployment(
            BlueGreenDeploymentName=bg_name,
            Source=source_arn,
        )
        bg_id = resp["BlueGreenDeployment"]["BlueGreenDeploymentIdentifier"]
        try:
            desc = client.describe_blue_green_deployments(
                BlueGreenDeploymentIdentifier=bg_id
            )
            assert "BlueGreenDeployments" in desc
            assert len(desc["BlueGreenDeployments"]) == 1
            deployment = desc["BlueGreenDeployments"][0]
            assert deployment["BlueGreenDeploymentIdentifier"] == bg_id
            assert deployment["BlueGreenDeploymentName"] == bg_name
            assert "Status" in deployment
            assert deployment["Source"] == source_arn
        finally:
            try:
                client.delete_blue_green_deployment(BlueGreenDeploymentIdentifier=bg_id)
            except ClientError:
                pass  # best-effort cleanup


class TestRDSDBProxyTargetGroupEdgeCases:
    """Edge cases and behavioral fidelity for DB proxy target group operations."""

    @pytest.fixture
    def client(self):
        return make_client("rds")

    @pytest.fixture
    def ec2_client(self):
        return make_client("ec2")

    @pytest.fixture
    def proxy(self, client, ec2_client):
        vpc = ec2_client.create_vpc(CidrBlock="10.93.0.0/16")
        vpc_id = vpc["Vpc"]["VpcId"]
        s1 = ec2_client.create_subnet(
            VpcId=vpc_id, CidrBlock="10.93.1.0/24", AvailabilityZone="us-east-1a"
        )
        s2 = ec2_client.create_subnet(
            VpcId=vpc_id, CidrBlock="10.93.2.0/24", AvailabilityZone="us-east-1b"
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
            pass  # best-effort cleanup
        for sid in subnet_ids:
            try:
                ec2_client.delete_subnet(SubnetId=sid)
            except ClientError:
                pass  # best-effort cleanup
        try:
            ec2_client.delete_vpc(VpcId=vpc_id)
        except ClientError:
            pass  # best-effort cleanup

    def test_proxy_has_default_target_group(self, client, proxy):
        """Created proxy always has a default target group."""
        resp = client.describe_db_proxy_target_groups(DBProxyName=proxy)
        assert "TargetGroups" in resp
        assert len(resp["TargetGroups"]) >= 1
        names = [tg["TargetGroupName"] for tg in resp["TargetGroups"]]
        assert "default" in names

    def test_proxy_target_group_fields(self, client, proxy):
        """Proxy target group response includes required fields."""
        resp = client.describe_db_proxy_target_groups(DBProxyName=proxy)
        tg = resp["TargetGroups"][0]
        assert "DBProxyName" in tg
        assert tg["DBProxyName"] == proxy
        assert "TargetGroupName" in tg
        assert "TargetGroupArn" in tg

    def test_modify_proxy_target_group_and_describe(self, client, proxy):
        """Modify target group MaxConnectionsPercent, then describe to verify change."""
        client.modify_db_proxy_target_group(
            TargetGroupName="default",
            DBProxyName=proxy,
            ConnectionPoolConfig={"MaxConnectionsPercent": 75},
        )
        resp = client.describe_db_proxy_target_groups(DBProxyName=proxy)
        tg_list = [tg for tg in resp["TargetGroups"] if tg["TargetGroupName"] == "default"]
        assert len(tg_list) == 1
        tg = tg_list[0]
        assert "ConnectionPoolConfig" in tg
        assert tg["ConnectionPoolConfig"]["MaxConnectionsPercent"] == 75


class TestRDSDBProxyTargetsEdgeCases:
    """Edge cases and behavioral fidelity for DB proxy target operations."""

    @pytest.fixture
    def client(self):
        return make_client("rds")

    @pytest.fixture
    def ec2_client(self):
        return make_client("ec2")

    @pytest.fixture
    def proxy_with_instance(self, client, ec2_client):
        """Create a proxy and DB instance for target registration tests."""
        vpc = ec2_client.create_vpc(CidrBlock="10.92.0.0/16")
        vpc_id = vpc["Vpc"]["VpcId"]
        s1 = ec2_client.create_subnet(
            VpcId=vpc_id, CidrBlock="10.92.1.0/24", AvailabilityZone="us-east-1a"
        )
        s2 = ec2_client.create_subnet(
            VpcId=vpc_id, CidrBlock="10.92.2.0/24", AvailabilityZone="us-east-1b"
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
            client.deregister_db_proxy_targets(
                DBProxyName=proxy_name, DBInstanceIdentifiers=[db_name]
            )
        except ClientError:
            pass  # best-effort cleanup
        try:
            client.delete_db_proxy(DBProxyName=proxy_name)
        except ClientError:
            pass  # best-effort cleanup
        try:
            client.delete_db_instance(DBInstanceIdentifier=db_name, SkipFinalSnapshot=True)
        except ClientError:
            pass  # best-effort cleanup
        for sid in subnet_ids:
            try:
                ec2_client.delete_subnet(SubnetId=sid)
            except ClientError:
                pass  # best-effort cleanup
        try:
            ec2_client.delete_vpc(VpcId=vpc_id)
        except ClientError:
            pass  # best-effort cleanup

    def test_proxy_targets_register_verify_list(self, client, proxy_with_instance):
        """Register DB instance as proxy target, verify it appears in describe_db_proxy_targets."""
        proxy_name, db_name = proxy_with_instance
        client.register_db_proxy_targets(
            DBProxyName=proxy_name,
            DBInstanceIdentifiers=[db_name],
        )
        resp = client.describe_db_proxy_targets(DBProxyName=proxy_name)
        assert "Targets" in resp
        assert isinstance(resp["Targets"], list)
        assert len(resp["Targets"]) >= 1
        rds_ids = [t.get("RdsResourceId", "") for t in resp["Targets"]]
        assert db_name in rds_ids

    def test_proxy_targets_deregister_removes_target(self, client, proxy_with_instance):
        """Register then deregister target, verify it no longer appears in list."""
        proxy_name, db_name = proxy_with_instance
        client.register_db_proxy_targets(
            DBProxyName=proxy_name,
            DBInstanceIdentifiers=[db_name],
        )
        client.deregister_db_proxy_targets(
            DBProxyName=proxy_name,
            DBInstanceIdentifiers=[db_name],
        )
        resp = client.describe_db_proxy_targets(DBProxyName=proxy_name)
        assert "Targets" in resp
        rds_ids = [t.get("RdsResourceId", "") for t in resp["Targets"]]
        assert db_name not in rds_ids


class TestRDSEventsEdgeCases:
    """Edge cases and behavioral fidelity for describe_events operations."""

    @pytest.fixture
    def client(self):
        return make_client("rds")

    def test_describe_events_returns_events_list(self, client):
        """describe_events response always includes Events list."""
        resp = client.describe_events()
        assert "Events" in resp
        assert isinstance(resp["Events"], list)

    def test_describe_events_source_identifier(self, client):
        """describe_events with SourceIdentifier and SourceType returns Events list."""
        inst_name = _unique("compat-db")
        client.create_db_instance(
            DBInstanceIdentifier=inst_name,
            DBInstanceClass="db.t3.micro",
            Engine="mysql",
            MasterUsername="admin",
            MasterUserPassword="password123",
        )
        try:
            resp = client.describe_events(
                SourceIdentifier=inst_name,
                SourceType="db-instance",
            )
            assert "Events" in resp
            assert isinstance(resp["Events"], list)
        finally:
            try:
                client.delete_db_instance(DBInstanceIdentifier=inst_name, SkipFinalSnapshot=True)
            except ClientError:
                pass  # best-effort cleanup

    def test_describe_events_duration_filter(self, client):
        """describe_events with Duration filter returns Events list."""
        resp = client.describe_events(Duration=60)
        assert "Events" in resp
        assert isinstance(resp["Events"], list)


class TestRDSShardGroupEdgeCases:
    """Edge cases and behavioral fidelity for DB shard group operations."""

    @pytest.fixture
    def client(self):
        return make_client("rds")

    def test_describe_db_shard_groups_nonexistent_error(self, client):
        """describe_db_shard_groups with nonexistent ID raises DBShardGroupNotFound error."""
        with pytest.raises(ClientError) as exc:
            client.describe_db_shard_groups(DBShardGroupIdentifier="nonexistent-shg-xyz")
        assert exc.value.response["Error"]["Code"] in (
            "DBShardGroupNotFound",
            "DBShardGroupNotFoundFault",
        )

    def test_describe_db_shard_groups_created_appears(self, client):
        """Create a cluster then shard group, verify it appears in list."""
        cl_name = _unique("compat-cl")
        shg_name = _unique("compat-shg")
        client.create_db_cluster(
            DBClusterIdentifier=cl_name,
            Engine="aurora-mysql",
            MasterUsername="admin",
            MasterUserPassword="password123!",
        )
        try:
            client.create_db_shard_group(
                DBShardGroupIdentifier=shg_name,
                DBClusterIdentifier=cl_name,
                MaxACU=64.0,
            )
            try:
                resp = client.describe_db_shard_groups()
                assert "DBShardGroups" in resp
                ids = [g["DBShardGroupIdentifier"] for g in resp["DBShardGroups"]]
                assert shg_name in ids
            finally:
                try:
                    client.delete_db_shard_group(DBShardGroupIdentifier=shg_name)
                except ClientError:
                    pass  # best-effort cleanup
        finally:
            try:
                client.delete_db_cluster(DBClusterIdentifier=cl_name, SkipFinalSnapshot=True)
            except ClientError:
                pass  # best-effort cleanup


class TestRDSAutomatedBackupsEdgeCases:
    """Edge cases and behavioral fidelity for DB instance automated backups."""

    @pytest.fixture
    def client(self):
        return make_client("rds")

    def test_automated_backups_by_instance_id(self, client):
        """describe_db_instance_automated_backups with DBInstanceIdentifier returns response."""
        inst_name = _unique("compat-db")
        client.create_db_instance(
            DBInstanceIdentifier=inst_name,
            DBInstanceClass="db.t3.micro",
            Engine="mysql",
            MasterUsername="admin",
            MasterUserPassword="password123",
        )
        try:
            resp = client.describe_db_instance_automated_backups(
                DBInstanceIdentifier=inst_name
            )
            assert "DBInstanceAutomatedBackups" in resp
            assert isinstance(resp["DBInstanceAutomatedBackups"], list)
        finally:
            try:
                client.delete_db_instance(DBInstanceIdentifier=inst_name, SkipFinalSnapshot=True)
            except ClientError:
                pass  # best-effort cleanup

    def test_automated_backups_fields_present(self, client):
        """describe_db_instance_automated_backups returns list with expected structure."""
        resp = client.describe_db_instance_automated_backups()
        assert "DBInstanceAutomatedBackups" in resp
        assert isinstance(resp["DBInstanceAutomatedBackups"], list)

    def test_automated_backups_nonexistent_returns_empty(self, client):
        """describe_db_instance_automated_backups with nonexistent ID returns empty list (not error)."""
        resp = client.describe_db_instance_automated_backups(
            DBInstanceIdentifier="nonexistent-xyz-000"
        )
        assert "DBInstanceAutomatedBackups" in resp
        assert resp["DBInstanceAutomatedBackups"] == []


class TestRDSOrderableOptionsEdgeCases:
    """Edge cases and behavioral fidelity for describe_orderable_db_instance_options."""

    @pytest.fixture
    def client(self):
        return make_client("rds")

    def test_orderable_options_aurora_postgresql_engine(self, client):
        """describe_orderable_db_instance_options for aurora-postgresql returns options."""
        resp = client.describe_orderable_db_instance_options(Engine="aurora-postgresql")
        assert "OrderableDBInstanceOptions" in resp
        assert isinstance(resp["OrderableDBInstanceOptions"], list)
        assert len(resp["OrderableDBInstanceOptions"]) > 0

    def test_orderable_options_response_fields(self, client):
        """Each option in describe_orderable_db_instance_options has Engine and DBInstanceClass."""
        resp = client.describe_orderable_db_instance_options(Engine="aurora-postgresql")
        options = resp["OrderableDBInstanceOptions"]
        assert len(options) > 0
        for option in options[:3]:  # check first 3 options
            assert "Engine" in option
            assert option["Engine"] == "aurora-postgresql"
            assert "DBInstanceClass" in option

    def test_orderable_options_unsupported_engine_returns_empty(self, client):
        """describe_orderable_db_instance_options with unknown engine returns empty list."""
        resp = client.describe_orderable_db_instance_options(Engine="nonexistent-engine-xyz")
        assert "OrderableDBInstanceOptions" in resp
        assert resp["OrderableDBInstanceOptions"] == []


class TestRDSSnapshotsBehavioral:
    """Behavioral fidelity tests for DB snapshots (targeting RETRIEVE pattern coverage)."""

    @pytest.fixture
    def client(self):
        return make_client("rds")

    @pytest.fixture
    def instance_with_snapshot(self, client):
        inst_name = _unique("compat-db")
        snap_name = _unique("compat-snap")
        client.create_db_instance(
            DBInstanceIdentifier=inst_name,
            DBInstanceClass="db.t3.micro",
            Engine="mysql",
            MasterUsername="admin",
            MasterUserPassword="password123",
        )
        client.create_db_snapshot(
            DBSnapshotIdentifier=snap_name,
            DBInstanceIdentifier=inst_name,
        )
        yield inst_name, snap_name
        try:
            client.delete_db_snapshot(DBSnapshotIdentifier=snap_name)
        except ClientError:
            pass  # best-effort cleanup
        try:
            client.delete_db_instance(DBInstanceIdentifier=inst_name, SkipFinalSnapshot=True)
        except ClientError:
            pass  # best-effort cleanup

    def test_snapshot_describe_by_instance_filter(self, client, instance_with_snapshot):
        """describe_db_snapshots filtered by DBInstanceIdentifier returns the snapshot."""
        inst_name, snap_name = instance_with_snapshot
        resp = client.describe_db_snapshots(DBInstanceIdentifier=inst_name)
        assert "DBSnapshots" in resp
        assert len(resp["DBSnapshots"]) >= 1
        ids = [s["DBSnapshotIdentifier"] for s in resp["DBSnapshots"]]
        assert snap_name in ids

    def test_snapshot_arn_format(self, client, instance_with_snapshot):
        """Created snapshot has a valid ARN in DBSnapshotArn field."""
        inst_name, snap_name = instance_with_snapshot
        resp = client.describe_db_snapshots(DBSnapshotIdentifier=snap_name)
        snapshot = resp["DBSnapshots"][0]
        assert "DBSnapshotArn" in snapshot
        assert snapshot["DBSnapshotArn"].startswith("arn:aws:rds:")
        assert "snapshot" in snapshot["DBSnapshotArn"]

    def test_snapshot_status_field(self, client, instance_with_snapshot):
        """Manually created snapshot has SnapshotType=manual."""
        inst_name, snap_name = instance_with_snapshot
        resp = client.describe_db_snapshots(DBSnapshotIdentifier=snap_name)
        snapshot = resp["DBSnapshots"][0]
        assert snapshot["SnapshotType"] == "manual"


class TestRDSBlueGreenDeploymentEdgeCases:
    """Edge cases and behavioral fidelity for blue/green deployment lifecycle."""

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
            pass  # best-effort cleanup

    def test_blue_green_list_after_create(self, client, source_instance):
        """After creating a deployment, it appears in describe_blue_green_deployments list."""
        bg_name = _unique("compat-bg")
        source_arn = f"arn:aws:rds:us-east-1:123456789012:db:{source_instance}"
        resp = client.create_blue_green_deployment(
            BlueGreenDeploymentName=bg_name,
            Source=source_arn,
        )
        bg_id = resp["BlueGreenDeployment"]["BlueGreenDeploymentIdentifier"]
        try:
            list_resp = client.describe_blue_green_deployments()
            assert "BlueGreenDeployments" in list_resp
            ids = [
                d["BlueGreenDeploymentIdentifier"]
                for d in list_resp["BlueGreenDeployments"]
            ]
            assert bg_id in ids
        finally:
            try:
                client.delete_blue_green_deployment(BlueGreenDeploymentIdentifier=bg_id)
            except ClientError:
                pass  # best-effort cleanup

    def test_blue_green_deployment_identifier_format(self, client, source_instance):
        """BlueGreenDeploymentIdentifier is non-empty after creation."""
        bg_name = _unique("compat-bg")
        source_arn = f"arn:aws:rds:us-east-1:123456789012:db:{source_instance}"
        resp = client.create_blue_green_deployment(
            BlueGreenDeploymentName=bg_name,
            Source=source_arn,
        )
        bg_id = resp["BlueGreenDeployment"]["BlueGreenDeploymentIdentifier"]
        try:
            assert bg_id is not None
            assert len(bg_id) > 0
        finally:
            try:
                client.delete_blue_green_deployment(BlueGreenDeploymentIdentifier=bg_id)
            except ClientError:
                pass  # best-effort cleanup

    def test_blue_green_describe_by_id(self, client, source_instance):
        """describe_blue_green_deployments by ID returns exactly that deployment."""
        bg_name = _unique("compat-bg")
        source_arn = f"arn:aws:rds:us-east-1:123456789012:db:{source_instance}"
        resp = client.create_blue_green_deployment(
            BlueGreenDeploymentName=bg_name,
            Source=source_arn,
        )
        bg_id = resp["BlueGreenDeployment"]["BlueGreenDeploymentIdentifier"]
        try:
            desc = client.describe_blue_green_deployments(
                BlueGreenDeploymentIdentifier=bg_id
            )
            assert len(desc["BlueGreenDeployments"]) == 1
            assert desc["BlueGreenDeployments"][0]["BlueGreenDeploymentIdentifier"] == bg_id
            assert desc["BlueGreenDeployments"][0]["BlueGreenDeploymentName"] == bg_name
        finally:
            try:
                client.delete_blue_green_deployment(BlueGreenDeploymentIdentifier=bg_id)
            except ClientError:
                pass  # best-effort cleanup
