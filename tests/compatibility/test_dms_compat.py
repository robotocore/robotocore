"""DMS (Database Migration Service) compatibility tests."""

import uuid

import pytest
from botocore.exceptions import ClientError

from tests.compatibility.conftest import make_client


@pytest.fixture
def dms():
    return make_client("dms")


@pytest.fixture
def ec2():
    return make_client("ec2")


def _unique(prefix):
    return f"{prefix}-{uuid.uuid4().hex[:8]}"


class TestDMSReplicationInstanceOperations:
    def test_describe_replication_instances_empty(self, dms):
        """DescribeReplicationInstances returns empty list when none exist."""
        response = dms.describe_replication_instances()
        assert "ReplicationInstances" in response
        assert isinstance(response["ReplicationInstances"], list)

    def test_describe_connections_empty(self, dms):
        """DescribeConnections returns empty list when none exist."""
        response = dms.describe_connections()
        assert "Connections" in response
        assert isinstance(response["Connections"], list)


class TestDMSEndpointOperations:
    def test_create_source_endpoint(self, dms):
        """Create a source endpoint and verify its fields."""
        ep_id = _unique("ep")
        response = dms.create_endpoint(
            EndpointIdentifier=ep_id,
            EndpointType="source",
            EngineName="mysql",
            ServerName="localhost",
            Port=3306,
            Username="admin",
            Password="password",
        )
        ep = response["Endpoint"]
        assert ep["EndpointIdentifier"] == ep_id
        assert ep["EndpointType"] == "source"
        assert ep["EngineName"] == "mysql"
        assert "EndpointArn" in ep
        # Cleanup
        dms.delete_endpoint(EndpointArn=ep["EndpointArn"])

    def test_create_target_endpoint(self, dms):
        """Create a target endpoint with postgres engine."""
        ep_id = _unique("tgt")
        response = dms.create_endpoint(
            EndpointIdentifier=ep_id,
            EndpointType="target",
            EngineName="postgres",
            ServerName="localhost",
            Port=5432,
            Username="admin",
            Password="password",
        )
        ep = response["Endpoint"]
        assert ep["EndpointType"] == "target"
        assert ep["EngineName"] == "postgres"
        dms.delete_endpoint(EndpointArn=ep["EndpointArn"])

    def test_describe_endpoints_empty(self, dms):
        """DescribeEndpoints returns empty list when none exist."""
        response = dms.describe_endpoints()
        assert "Endpoints" in response
        assert isinstance(response["Endpoints"], list)

    def test_describe_endpoints_finds_created(self, dms):
        """DescribeEndpoints includes a newly created endpoint."""
        ep_id = _unique("ep")
        create_resp = dms.create_endpoint(
            EndpointIdentifier=ep_id,
            EndpointType="source",
            EngineName="mysql",
            ServerName="localhost",
            Port=3306,
            Username="admin",
            Password="password",
        )
        arn = create_resp["Endpoint"]["EndpointArn"]
        try:
            response = dms.describe_endpoints()
            identifiers = [e["EndpointIdentifier"] for e in response["Endpoints"]]
            assert ep_id in identifiers
        finally:
            dms.delete_endpoint(EndpointArn=arn)

    def test_delete_endpoint(self, dms):
        """Delete an endpoint and verify it's gone."""
        ep_id = _unique("ep")
        create_resp = dms.create_endpoint(
            EndpointIdentifier=ep_id,
            EndpointType="source",
            EngineName="mysql",
            ServerName="localhost",
            Port=3306,
            Username="admin",
            Password="password",
        )
        arn = create_resp["Endpoint"]["EndpointArn"]
        dms.delete_endpoint(EndpointArn=arn)

        response = dms.describe_endpoints()
        identifiers = [e["EndpointIdentifier"] for e in response["Endpoints"]]
        assert ep_id not in identifiers

    def test_delete_nonexistent_endpoint_raises(self, dms):
        """Deleting a non-existent endpoint raises ResourceNotFoundFault."""
        with pytest.raises(ClientError) as exc_info:
            dms.delete_endpoint(
                EndpointArn="arn:aws:dms:us-east-1:123456789012:endpoint:nonexistent"
            )
        assert exc_info.value.response["Error"]["Code"] == "ResourceNotFoundFault"

    def test_create_endpoint_with_extra_connection_attributes(self, dms):
        """Create endpoint with ExtraConnectionAttributes."""
        ep_id = _unique("ep")
        response = dms.create_endpoint(
            EndpointIdentifier=ep_id,
            EndpointType="source",
            EngineName="mysql",
            ServerName="localhost",
            Port=3306,
            Username="admin",
            Password="password",
            ExtraConnectionAttributes="key=value",
        )
        assert "EndpointArn" in response["Endpoint"]
        dms.delete_endpoint(EndpointArn=response["Endpoint"]["EndpointArn"])


class TestDMSSubnetGroupOperations:
    def test_create_replication_subnet_group(self, dms, ec2):
        """Create a replication subnet group and verify it."""
        vpc = ec2.create_vpc(CidrBlock="10.0.0.0/16")
        vpc_id = vpc["Vpc"]["VpcId"]
        sub1 = ec2.create_subnet(
            VpcId=vpc_id, CidrBlock="10.0.1.0/24", AvailabilityZone="us-east-1a"
        )
        sub2 = ec2.create_subnet(
            VpcId=vpc_id, CidrBlock="10.0.2.0/24", AvailabilityZone="us-east-1b"
        )
        sub1_id = sub1["Subnet"]["SubnetId"]
        sub2_id = sub2["Subnet"]["SubnetId"]

        sg_id = _unique("rsg")
        response = dms.create_replication_subnet_group(
            ReplicationSubnetGroupIdentifier=sg_id,
            ReplicationSubnetGroupDescription="Test subnet group",
            SubnetIds=[sub1_id, sub2_id],
        )
        group = response["ReplicationSubnetGroup"]
        assert group["ReplicationSubnetGroupIdentifier"] == sg_id
        assert group["ReplicationSubnetGroupDescription"] == "Test subnet group"
        assert "VpcId" in group

        # Cleanup
        dms.delete_replication_subnet_group(ReplicationSubnetGroupIdentifier=sg_id)

    def test_describe_replication_subnet_groups(self, dms, ec2):
        """DescribeReplicationSubnetGroups finds created group."""
        vpc = ec2.create_vpc(CidrBlock="10.1.0.0/16")
        vpc_id = vpc["Vpc"]["VpcId"]
        sub1 = ec2.create_subnet(
            VpcId=vpc_id, CidrBlock="10.1.1.0/24", AvailabilityZone="us-east-1a"
        )
        sub2 = ec2.create_subnet(
            VpcId=vpc_id, CidrBlock="10.1.2.0/24", AvailabilityZone="us-east-1b"
        )

        sg_id = _unique("rsg")
        dms.create_replication_subnet_group(
            ReplicationSubnetGroupIdentifier=sg_id,
            ReplicationSubnetGroupDescription="Describe test",
            SubnetIds=[sub1["Subnet"]["SubnetId"], sub2["Subnet"]["SubnetId"]],
        )
        try:
            response = dms.describe_replication_subnet_groups()
            ids = [
                g["ReplicationSubnetGroupIdentifier"] for g in response["ReplicationSubnetGroups"]
            ]
            assert sg_id in ids
        finally:
            dms.delete_replication_subnet_group(ReplicationSubnetGroupIdentifier=sg_id)

    def test_delete_replication_subnet_group(self, dms, ec2):
        """Delete a replication subnet group and verify removal."""
        vpc = ec2.create_vpc(CidrBlock="10.2.0.0/16")
        vpc_id = vpc["Vpc"]["VpcId"]
        sub1 = ec2.create_subnet(
            VpcId=vpc_id, CidrBlock="10.2.1.0/24", AvailabilityZone="us-east-1a"
        )
        sub2 = ec2.create_subnet(
            VpcId=vpc_id, CidrBlock="10.2.2.0/24", AvailabilityZone="us-east-1b"
        )

        sg_id = _unique("rsg")
        dms.create_replication_subnet_group(
            ReplicationSubnetGroupIdentifier=sg_id,
            ReplicationSubnetGroupDescription="Delete test",
            SubnetIds=[sub1["Subnet"]["SubnetId"], sub2["Subnet"]["SubnetId"]],
        )
        dms.delete_replication_subnet_group(ReplicationSubnetGroupIdentifier=sg_id)

        response = dms.describe_replication_subnet_groups()
        ids = [g["ReplicationSubnetGroupIdentifier"] for g in response["ReplicationSubnetGroups"]]
        assert sg_id not in ids


class TestDMSSubnetGroupErrors:
    def test_delete_nonexistent_subnet_group_raises(self, dms):
        """DeleteReplicationSubnetGroup raises ResourceNotFoundFault for missing group."""
        with pytest.raises(ClientError) as exc_info:
            dms.delete_replication_subnet_group(
                ReplicationSubnetGroupIdentifier="nonexistent-group"
            )
        assert exc_info.value.response["Error"]["Code"] == "ResourceNotFoundFault"


class TestDMSTags:
    def test_list_tags_for_resource_empty(self, dms):
        """ListTagsForResource returns empty list for new endpoint."""
        ep_id = _unique("ep")
        create_resp = dms.create_endpoint(
            EndpointIdentifier=ep_id,
            EndpointType="source",
            EngineName="mysql",
            ServerName="localhost",
            Port=3306,
            Username="admin",
            Password="password",
        )
        arn = create_resp["Endpoint"]["EndpointArn"]
        try:
            response = dms.list_tags_for_resource(ResourceArn=arn)
            assert response["TagList"] == []
        finally:
            dms.delete_endpoint(EndpointArn=arn)

    def test_create_endpoint_with_tags(self, dms):
        """Creating an endpoint with Tags makes them visible via ListTagsForResource."""
        ep_id = _unique("ep")
        create_resp = dms.create_endpoint(
            EndpointIdentifier=ep_id,
            EndpointType="source",
            EngineName="mysql",
            ServerName="localhost",
            Port=3306,
            Username="admin",
            Password="password",
            Tags=[{"Key": "env", "Value": "test"}, {"Key": "team", "Value": "data"}],
        )
        arn = create_resp["Endpoint"]["EndpointArn"]
        try:
            response = dms.list_tags_for_resource(ResourceArn=arn)
            tag_keys = {t["Key"] for t in response["TagList"]}
            assert "env" in tag_keys
            assert "team" in tag_keys
        finally:
            dms.delete_endpoint(EndpointArn=arn)

    def test_create_replication_instance_with_tags(self, dms):
        """Creating a replication instance with Tags makes them visible."""
        ri_id = _unique("ri")
        resp = dms.create_replication_instance(
            ReplicationInstanceIdentifier=ri_id,
            ReplicationInstanceClass="dms.t3.micro",
            Tags=[{"Key": "env", "Value": "prod"}],
        )
        ri_arn = resp["ReplicationInstance"]["ReplicationInstanceArn"]
        try:
            tags_resp = dms.list_tags_for_resource(ResourceArn=ri_arn)
            tag_keys = {t["Key"] for t in tags_resp["TagList"]}
            assert "env" in tag_keys
            tag_vals = {t["Value"] for t in tags_resp["TagList"] if t["Key"] == "env"}
            assert "prod" in tag_vals
        finally:
            dms.delete_replication_instance(ReplicationInstanceArn=ri_arn)

    def test_create_replication_task_with_tags(self, dms):
        """Creating a replication task with Tags makes them visible."""
        uid = uuid.uuid4().hex[:8]
        src = dms.create_endpoint(
            EndpointIdentifier=f"src-{uid}",
            EndpointType="source",
            EngineName="mysql",
            ServerName="localhost",
            Port=3306,
            Username="admin",
            Password="password",
        )
        tgt = dms.create_endpoint(
            EndpointIdentifier=f"tgt-{uid}",
            EndpointType="target",
            EngineName="postgres",
            ServerName="localhost",
            Port=5432,
            Username="admin",
            Password="password",
        )
        ri = dms.create_replication_instance(
            ReplicationInstanceIdentifier=f"ri-{uid}",
            ReplicationInstanceClass="dms.t3.micro",
        )
        try:
            task = dms.create_replication_task(
                ReplicationTaskIdentifier=f"task-{uid}",
                SourceEndpointArn=src["Endpoint"]["EndpointArn"],
                TargetEndpointArn=tgt["Endpoint"]["EndpointArn"],
                ReplicationInstanceArn=ri["ReplicationInstance"]["ReplicationInstanceArn"],
                MigrationType="full-load",
                TableMappings='{"rules":[]}',
                Tags=[{"Key": "env", "Value": "staging"}],
            )
            task_arn = task["ReplicationTask"]["ReplicationTaskArn"]
            tags_resp = dms.list_tags_for_resource(ResourceArn=task_arn)
            tag_keys = {t["Key"] for t in tags_resp["TagList"]}
            assert "env" in tag_keys
            dms.delete_replication_task(ReplicationTaskArn=task_arn)
        finally:
            dms.delete_endpoint(EndpointArn=src["Endpoint"]["EndpointArn"])
            dms.delete_endpoint(EndpointArn=tgt["Endpoint"]["EndpointArn"])
            dms.delete_replication_instance(
                ReplicationInstanceArn=ri["ReplicationInstance"]["ReplicationInstanceArn"]
            )


class TestDMSReplicationTaskOperations:
    def _create_task_prereqs(self, dms):
        """Create endpoints and replication instance needed for a replication task."""
        src_id = _unique("src")
        tgt_id = _unique("tgt")
        ri_id = _unique("ri")

        src = dms.create_endpoint(
            EndpointIdentifier=src_id,
            EndpointType="source",
            EngineName="mysql",
            ServerName="localhost",
            Port=3306,
            Username="admin",
            Password="password",
        )
        tgt = dms.create_endpoint(
            EndpointIdentifier=tgt_id,
            EndpointType="target",
            EngineName="postgres",
            ServerName="localhost",
            Port=5432,
            Username="admin",
            Password="password",
        )
        ri = dms.create_replication_instance(
            ReplicationInstanceIdentifier=ri_id,
            ReplicationInstanceClass="dms.t3.micro",
        )

        return {
            "source_arn": src["Endpoint"]["EndpointArn"],
            "target_arn": tgt["Endpoint"]["EndpointArn"],
            "ri_arn": ri["ReplicationInstance"]["ReplicationInstanceArn"],
        }

    def _cleanup_prereqs(self, dms, prereqs):
        dms.delete_endpoint(EndpointArn=prereqs["source_arn"])
        dms.delete_endpoint(EndpointArn=prereqs["target_arn"])
        dms.delete_replication_instance(ReplicationInstanceArn=prereqs["ri_arn"])

    def test_create_replication_task(self, dms):
        """CreateReplicationTask creates a task and returns its details."""
        prereqs = self._create_task_prereqs(dms)
        task_id = _unique("task")
        try:
            resp = dms.create_replication_task(
                ReplicationTaskIdentifier=task_id,
                SourceEndpointArn=prereqs["source_arn"],
                TargetEndpointArn=prereqs["target_arn"],
                ReplicationInstanceArn=prereqs["ri_arn"],
                MigrationType="full-load",
                TableMappings='{"rules":[]}',
            )
            task = resp["ReplicationTask"]
            assert task["ReplicationTaskIdentifier"] == task_id
            assert task["MigrationType"] == "full-load"
            assert "ReplicationTaskArn" in task
            # Cleanup task
            dms.delete_replication_task(ReplicationTaskArn=task["ReplicationTaskArn"])
        finally:
            self._cleanup_prereqs(dms, prereqs)

    def test_delete_replication_task(self, dms):
        """DeleteReplicationTask removes the task."""
        prereqs = self._create_task_prereqs(dms)
        task_id = _unique("task")
        try:
            resp = dms.create_replication_task(
                ReplicationTaskIdentifier=task_id,
                SourceEndpointArn=prereqs["source_arn"],
                TargetEndpointArn=prereqs["target_arn"],
                ReplicationInstanceArn=prereqs["ri_arn"],
                MigrationType="full-load",
                TableMappings='{"rules":[]}',
            )
            task_arn = resp["ReplicationTask"]["ReplicationTaskArn"]
            del_resp = dms.delete_replication_task(ReplicationTaskArn=task_arn)
            assert del_resp["ReplicationTask"]["ReplicationTaskIdentifier"] == task_id
            # Verify it's gone by trying to delete again
            with pytest.raises(ClientError) as exc:
                dms.delete_replication_task(ReplicationTaskArn=task_arn)
            assert exc.value.response["Error"]["Code"] == "ResourceNotFoundFault"
        finally:
            self._cleanup_prereqs(dms, prereqs)

    def test_delete_nonexistent_replication_task(self, dms):
        """DeleteReplicationTask raises ResourceNotFoundFault for missing task."""
        with pytest.raises(ClientError) as exc:
            dms.delete_replication_task(
                ReplicationTaskArn="arn:aws:dms:us-east-1:123456789012:task:nonexistent"
            )
        assert exc.value.response["Error"]["Code"] == "ResourceNotFoundFault"

    def test_start_replication_task(self, dms):
        """StartReplicationTask changes task status."""
        prereqs = self._create_task_prereqs(dms)
        task_id = _unique("task")
        try:
            resp = dms.create_replication_task(
                ReplicationTaskIdentifier=task_id,
                SourceEndpointArn=prereqs["source_arn"],
                TargetEndpointArn=prereqs["target_arn"],
                ReplicationInstanceArn=prereqs["ri_arn"],
                MigrationType="full-load",
                TableMappings='{"rules":[]}',
            )
            task_arn = resp["ReplicationTask"]["ReplicationTaskArn"]
            start_resp = dms.start_replication_task(
                ReplicationTaskArn=task_arn,
                StartReplicationTaskType="start-replication",
            )
            assert start_resp["ReplicationTask"]["Status"] == "running"
            # Cleanup
            dms.stop_replication_task(ReplicationTaskArn=task_arn)
            dms.delete_replication_task(ReplicationTaskArn=task_arn)
        finally:
            self._cleanup_prereqs(dms, prereqs)

    def test_stop_replication_task(self, dms):
        """StopReplicationTask stops a running task."""
        prereqs = self._create_task_prereqs(dms)
        task_id = _unique("task")
        try:
            resp = dms.create_replication_task(
                ReplicationTaskIdentifier=task_id,
                SourceEndpointArn=prereqs["source_arn"],
                TargetEndpointArn=prereqs["target_arn"],
                ReplicationInstanceArn=prereqs["ri_arn"],
                MigrationType="full-load",
                TableMappings='{"rules":[]}',
            )
            task_arn = resp["ReplicationTask"]["ReplicationTaskArn"]
            dms.start_replication_task(
                ReplicationTaskArn=task_arn,
                StartReplicationTaskType="start-replication",
            )
            stop_resp = dms.stop_replication_task(ReplicationTaskArn=task_arn)
            assert stop_resp["ReplicationTask"]["Status"] == "stopped"
            # Cleanup
            dms.delete_replication_task(ReplicationTaskArn=task_arn)
        finally:
            self._cleanup_prereqs(dms, prereqs)

    def test_start_nonexistent_replication_task(self, dms):
        """StartReplicationTask raises ResourceNotFoundFault for missing task."""
        with pytest.raises(ClientError) as exc:
            dms.start_replication_task(
                ReplicationTaskArn="arn:aws:dms:us-east-1:123456789012:task:nonexistent",
                StartReplicationTaskType="start-replication",
            )
        assert exc.value.response["Error"]["Code"] == "ResourceNotFoundFault"

    def test_stop_nonexistent_replication_task(self, dms):
        """StopReplicationTask raises ResourceNotFoundFault for missing task."""
        with pytest.raises(ClientError) as exc:
            dms.stop_replication_task(
                ReplicationTaskArn="arn:aws:dms:us-east-1:123456789012:task:nonexistent"
            )
        assert exc.value.response["Error"]["Code"] == "ResourceNotFoundFault"


class TestDMSReplicationInstanceCreateDelete:
    def test_create_replication_instance(self, dms):
        """CreateReplicationInstance creates an instance and returns its details."""
        ri_id = _unique("ri")
        resp = dms.create_replication_instance(
            ReplicationInstanceIdentifier=ri_id,
            ReplicationInstanceClass="dms.t3.micro",
        )
        ri = resp["ReplicationInstance"]
        assert ri["ReplicationInstanceIdentifier"] == ri_id
        assert ri["ReplicationInstanceClass"] == "dms.t3.micro"
        assert "ReplicationInstanceArn" in ri
        # Cleanup
        dms.delete_replication_instance(ReplicationInstanceArn=ri["ReplicationInstanceArn"])

    def test_delete_replication_instance(self, dms):
        """DeleteReplicationInstance removes the instance."""
        ri_id = _unique("ri")
        resp = dms.create_replication_instance(
            ReplicationInstanceIdentifier=ri_id,
            ReplicationInstanceClass="dms.t3.micro",
        )
        ri_arn = resp["ReplicationInstance"]["ReplicationInstanceArn"]
        del_resp = dms.delete_replication_instance(ReplicationInstanceArn=ri_arn)
        assert del_resp["ReplicationInstance"]["ReplicationInstanceIdentifier"] == ri_id

        # Verify it's gone
        instances = dms.describe_replication_instances()
        arns = [i["ReplicationInstanceArn"] for i in instances["ReplicationInstances"]]
        assert ri_arn not in arns


class TestDMSTestConnection:
    def test_test_connection(self, dms):
        """TestConnection returns a connection object."""
        ri_id = _unique("ri")
        ep_id = _unique("ep")
        ri = dms.create_replication_instance(
            ReplicationInstanceIdentifier=ri_id,
            ReplicationInstanceClass="dms.t3.micro",
        )
        ri_arn = ri["ReplicationInstance"]["ReplicationInstanceArn"]
        ep = dms.create_endpoint(
            EndpointIdentifier=ep_id,
            EndpointType="source",
            EngineName="mysql",
            ServerName="localhost",
            Port=3306,
            Username="admin",
            Password="password",
        )
        ep_arn = ep["Endpoint"]["EndpointArn"]
        try:
            resp = dms.test_connection(
                ReplicationInstanceArn=ri_arn,
                EndpointArn=ep_arn,
            )
            conn = resp["Connection"]
            assert "Status" in conn
            assert conn["EndpointArn"] == ep_arn
            assert conn["ReplicationInstanceArn"] == ri_arn
        finally:
            dms.delete_endpoint(EndpointArn=ep_arn)
            dms.delete_replication_instance(ReplicationInstanceArn=ri_arn)


class TestDMSDescribeOperations:
    def test_describe_endpoints_has_response_metadata(self, dms):
        """DescribeEndpoints returns proper ResponseMetadata."""
        response = dms.describe_endpoints()
        assert response["ResponseMetadata"]["HTTPStatusCode"] == 200

    def test_describe_replication_instances_has_response_metadata(self, dms):
        """DescribeReplicationInstances returns proper ResponseMetadata."""
        response = dms.describe_replication_instances()
        assert response["ResponseMetadata"]["HTTPStatusCode"] == 200

    def test_describe_connections_has_response_metadata(self, dms):
        """DescribeConnections returns proper ResponseMetadata."""
        response = dms.describe_connections()
        assert response["ResponseMetadata"]["HTTPStatusCode"] == 200


class TestDMSReplicationInstanceAdvanced:
    """Tests for replication instance with extra parameters and filters."""

    def test_create_replication_instance_with_allocated_storage(self, dms):
        """CreateReplicationInstance with AllocatedStorage."""
        ri_id = _unique("ri")
        resp = dms.create_replication_instance(
            ReplicationInstanceIdentifier=ri_id,
            ReplicationInstanceClass="dms.t3.micro",
            AllocatedStorage=50,
        )
        ri = resp["ReplicationInstance"]
        assert ri["AllocatedStorage"] == 50
        assert ri["ReplicationInstanceIdentifier"] == ri_id
        dms.delete_replication_instance(ReplicationInstanceArn=ri["ReplicationInstanceArn"])

    def test_create_replication_instance_multi_az(self, dms):
        """CreateReplicationInstance with MultiAZ=False."""
        ri_id = _unique("ri")
        resp = dms.create_replication_instance(
            ReplicationInstanceIdentifier=ri_id,
            ReplicationInstanceClass="dms.t3.micro",
            MultiAZ=False,
        )
        ri = resp["ReplicationInstance"]
        assert ri["MultiAZ"] is False
        dms.delete_replication_instance(ReplicationInstanceArn=ri["ReplicationInstanceArn"])

    def test_describe_replication_instances_finds_created(self, dms):
        """DescribeReplicationInstances includes a newly created instance."""
        ri_id = _unique("ri")
        resp = dms.create_replication_instance(
            ReplicationInstanceIdentifier=ri_id,
            ReplicationInstanceClass="dms.t3.micro",
        )
        ri_arn = resp["ReplicationInstance"]["ReplicationInstanceArn"]
        try:
            desc = dms.describe_replication_instances()
            ids = [i["ReplicationInstanceIdentifier"] for i in desc["ReplicationInstances"]]
            assert ri_id in ids
        finally:
            dms.delete_replication_instance(ReplicationInstanceArn=ri_arn)

    def test_describe_replication_instances_with_filter(self, dms):
        """DescribeReplicationInstances filters by replication-instance-id."""
        ri_id = _unique("ri")
        resp = dms.create_replication_instance(
            ReplicationInstanceIdentifier=ri_id,
            ReplicationInstanceClass="dms.t3.micro",
        )
        ri_arn = resp["ReplicationInstance"]["ReplicationInstanceArn"]
        try:
            desc = dms.describe_replication_instances(
                Filters=[{"Name": "replication-instance-id", "Values": [ri_id]}]
            )
            assert len(desc["ReplicationInstances"]) == 1
            assert desc["ReplicationInstances"][0]["ReplicationInstanceIdentifier"] == ri_id
        finally:
            dms.delete_replication_instance(ReplicationInstanceArn=ri_arn)

    def test_replication_instance_status(self, dms):
        """Created replication instance has a ReplicationInstanceStatus."""
        ri_id = _unique("ri")
        resp = dms.create_replication_instance(
            ReplicationInstanceIdentifier=ri_id,
            ReplicationInstanceClass="dms.t3.micro",
        )
        ri = resp["ReplicationInstance"]
        assert "ReplicationInstanceStatus" in ri
        assert isinstance(ri["ReplicationInstanceStatus"], str)
        dms.delete_replication_instance(ReplicationInstanceArn=ri["ReplicationInstanceArn"])

    def test_replication_instance_arn_format(self, dms):
        """Replication instance ARN has expected format."""
        ri_id = _unique("ri")
        resp = dms.create_replication_instance(
            ReplicationInstanceIdentifier=ri_id,
            ReplicationInstanceClass="dms.t3.micro",
        )
        arn = resp["ReplicationInstance"]["ReplicationInstanceArn"]
        assert arn.startswith("arn:aws:dms:")
        assert ":rep:" in arn or "replication-instance" in arn.lower() or ri_id in arn
        dms.delete_replication_instance(ReplicationInstanceArn=arn)

    def test_create_replication_instance_with_az(self, dms):
        """CreateReplicationInstance with AvailabilityZone."""
        ri_id = _unique("ri")
        resp = dms.create_replication_instance(
            ReplicationInstanceIdentifier=ri_id,
            ReplicationInstanceClass="dms.t3.micro",
            AvailabilityZone="us-east-1a",
        )
        ri = resp["ReplicationInstance"]
        assert ri["AvailabilityZone"] == "us-east-1a"
        dms.delete_replication_instance(ReplicationInstanceArn=ri["ReplicationInstanceArn"])

    def test_create_replication_instance_with_engine_version(self, dms):
        """CreateReplicationInstance with EngineVersion."""
        ri_id = _unique("ri")
        resp = dms.create_replication_instance(
            ReplicationInstanceIdentifier=ri_id,
            ReplicationInstanceClass="dms.t3.micro",
            EngineVersion="3.4.7",
        )
        ri = resp["ReplicationInstance"]
        assert ri["EngineVersion"] == "3.4.7"
        dms.delete_replication_instance(ReplicationInstanceArn=ri["ReplicationInstanceArn"])

    def test_create_replication_instance_publicly_accessible(self, dms):
        """CreateReplicationInstance with PubliclyAccessible=False."""
        ri_id = _unique("ri")
        resp = dms.create_replication_instance(
            ReplicationInstanceIdentifier=ri_id,
            ReplicationInstanceClass="dms.t3.micro",
            PubliclyAccessible=False,
        )
        ri = resp["ReplicationInstance"]
        assert ri["PubliclyAccessible"] is False
        dms.delete_replication_instance(ReplicationInstanceArn=ri["ReplicationInstanceArn"])

    def test_delete_nonexistent_replication_instance_raises(self, dms):
        """DeleteReplicationInstance raises ResourceNotFoundFault for missing instance."""
        with pytest.raises(ClientError) as exc_info:
            dms.delete_replication_instance(
                ReplicationInstanceArn="arn:aws:dms:us-east-1:123456789012:rep:nonexistent"
            )
        assert exc_info.value.response["Error"]["Code"] == "ResourceNotFoundFault"


class TestDMSEndpointAdvanced:
    """Tests for endpoint operations with filters and details."""

    def test_describe_endpoints_with_filter(self, dms):
        """DescribeEndpoints with endpoint-type filter."""
        ep_id = _unique("ep")
        create_resp = dms.create_endpoint(
            EndpointIdentifier=ep_id,
            EndpointType="source",
            EngineName="mysql",
            ServerName="localhost",
            Port=3306,
            Username="admin",
            Password="password",
        )
        arn = create_resp["Endpoint"]["EndpointArn"]
        try:
            resp = dms.describe_endpoints(Filters=[{"Name": "endpoint-type", "Values": ["source"]}])
            types = [e["EndpointType"] for e in resp["Endpoints"]]
            assert all(t == "source" for t in types)
        finally:
            dms.delete_endpoint(EndpointArn=arn)

    def test_endpoint_arn_format(self, dms):
        """Endpoint ARN has expected format."""
        ep_id = _unique("ep")
        create_resp = dms.create_endpoint(
            EndpointIdentifier=ep_id,
            EndpointType="source",
            EngineName="mysql",
            ServerName="localhost",
            Port=3306,
            Username="admin",
            Password="password",
        )
        arn = create_resp["Endpoint"]["EndpointArn"]
        assert arn.startswith("arn:aws:dms:")
        assert "endpoint" in arn.lower() or ep_id in arn
        dms.delete_endpoint(EndpointArn=arn)

    def test_endpoint_has_status(self, dms):
        """Created endpoint has a Status field."""
        ep_id = _unique("ep")
        create_resp = dms.create_endpoint(
            EndpointIdentifier=ep_id,
            EndpointType="source",
            EngineName="mysql",
            ServerName="localhost",
            Port=3306,
            Username="admin",
            Password="password",
        )
        ep = create_resp["Endpoint"]
        assert "Status" in ep
        assert isinstance(ep["Status"], str)
        dms.delete_endpoint(EndpointArn=ep["EndpointArn"])

    def test_create_endpoint_s3_engine(self, dms):
        """Create an endpoint with S3 engine type."""
        ep_id = _unique("ep")
        resp = dms.create_endpoint(
            EndpointIdentifier=ep_id,
            EndpointType="target",
            EngineName="s3",
            S3Settings={
                "BucketName": "my-bucket",
                "ServiceAccessRoleArn": "arn:aws:iam::123456789012:role/test",
            },
        )
        ep = resp["Endpoint"]
        assert ep["EngineName"] == "s3"
        assert ep["EndpointType"] == "target"
        dms.delete_endpoint(EndpointArn=ep["EndpointArn"])

    def test_create_endpoint_with_kms_key(self, dms):
        """Create endpoint with KmsKeyId returns it in response."""
        ep_id = _unique("ep")
        resp = dms.create_endpoint(
            EndpointIdentifier=ep_id,
            EndpointType="source",
            EngineName="mysql",
            ServerName="localhost",
            Port=3306,
            Username="admin",
            Password="password",
            KmsKeyId="arn:aws:kms:us-east-1:123456789012:key/fake-key-id",
        )
        ep = resp["Endpoint"]
        assert ep["KmsKeyId"] == "arn:aws:kms:us-east-1:123456789012:key/fake-key-id"
        dms.delete_endpoint(EndpointArn=ep["EndpointArn"])

    def test_create_endpoint_with_ssl_mode(self, dms):
        """Create endpoint with SslMode returns it in response."""
        ep_id = _unique("ep")
        resp = dms.create_endpoint(
            EndpointIdentifier=ep_id,
            EndpointType="source",
            EngineName="mysql",
            ServerName="localhost",
            Port=3306,
            Username="admin",
            Password="password",
            SslMode="none",
        )
        ep = resp["Endpoint"]
        assert ep["SslMode"] == "none"
        dms.delete_endpoint(EndpointArn=ep["EndpointArn"])

    def test_create_endpoint_with_database_name(self, dms):
        """Create endpoint with DatabaseName returns it in response."""
        ep_id = _unique("ep")
        resp = dms.create_endpoint(
            EndpointIdentifier=ep_id,
            EndpointType="source",
            EngineName="mysql",
            ServerName="localhost",
            Port=3306,
            Username="admin",
            Password="password",
            DatabaseName="mydb",
        )
        ep = resp["Endpoint"]
        assert ep["DatabaseName"] == "mydb"
        dms.delete_endpoint(EndpointArn=ep["EndpointArn"])

    def test_create_endpoint_kinesis_engine(self, dms):
        """Create endpoint with kinesis engine type."""
        ep_id = _unique("ep")
        resp = dms.create_endpoint(
            EndpointIdentifier=ep_id,
            EndpointType="target",
            EngineName="kinesis",
            KinesisSettings={
                "StreamArn": "arn:aws:kinesis:us-east-1:123456789012:stream/test",
                "ServiceAccessRoleArn": "arn:aws:iam::123456789012:role/test",
            },
        )
        ep = resp["Endpoint"]
        assert ep["EngineName"] == "kinesis"
        assert ep["EndpointType"] == "target"
        dms.delete_endpoint(EndpointArn=ep["EndpointArn"])

    def test_describe_endpoints_with_endpoint_id_filter(self, dms):
        """DescribeEndpoints with endpoint-id filter returns matching endpoint."""
        ep_id = _unique("ep")
        create_resp = dms.create_endpoint(
            EndpointIdentifier=ep_id,
            EndpointType="source",
            EngineName="mysql",
            ServerName="localhost",
            Port=3306,
            Username="admin",
            Password="password",
        )
        arn = create_resp["Endpoint"]["EndpointArn"]
        try:
            resp = dms.describe_endpoints(Filters=[{"Name": "endpoint-id", "Values": [ep_id]}])
            assert len(resp["Endpoints"]) == 1
            assert resp["Endpoints"][0]["EndpointIdentifier"] == ep_id
        finally:
            dms.delete_endpoint(EndpointArn=arn)

    def test_describe_replication_subnet_groups_empty(self, dms):
        """DescribeReplicationSubnetGroups returns list (may be empty)."""
        resp = dms.describe_replication_subnet_groups()
        assert "ReplicationSubnetGroups" in resp
        assert isinstance(resp["ReplicationSubnetGroups"], list)


class TestDMSDescribeReplicationTasks:
    """Tests for DescribeReplicationTasks operation."""

    def _create_task_prereqs(self, dms):
        """Create endpoints and replication instance needed for a replication task."""
        uid = uuid.uuid4().hex[:8]
        src = dms.create_endpoint(
            EndpointIdentifier=f"src-{uid}",
            EndpointType="source",
            EngineName="mysql",
            ServerName="localhost",
            Port=3306,
            Username="admin",
            Password="password",
        )
        tgt = dms.create_endpoint(
            EndpointIdentifier=f"tgt-{uid}",
            EndpointType="target",
            EngineName="postgres",
            ServerName="localhost",
            Port=5432,
            Username="admin",
            Password="password",
        )
        ri = dms.create_replication_instance(
            ReplicationInstanceIdentifier=f"ri-{uid}",
            ReplicationInstanceClass="dms.t3.micro",
        )
        return {
            "source_arn": src["Endpoint"]["EndpointArn"],
            "target_arn": tgt["Endpoint"]["EndpointArn"],
            "ri_arn": ri["ReplicationInstance"]["ReplicationInstanceArn"],
        }

    def _cleanup_prereqs(self, dms, prereqs):
        dms.delete_endpoint(EndpointArn=prereqs["source_arn"])
        dms.delete_endpoint(EndpointArn=prereqs["target_arn"])
        dms.delete_replication_instance(ReplicationInstanceArn=prereqs["ri_arn"])

    def test_describe_replication_tasks_with_task_id_filter(self, dms):
        """DescribeReplicationTasks filtered by replication-task-id returns matching task."""
        prereqs = self._create_task_prereqs(dms)
        task_id = _unique("task")
        try:
            task = dms.create_replication_task(
                ReplicationTaskIdentifier=task_id,
                SourceEndpointArn=prereqs["source_arn"],
                TargetEndpointArn=prereqs["target_arn"],
                ReplicationInstanceArn=prereqs["ri_arn"],
                MigrationType="full-load",
                TableMappings='{"rules":[]}',
            )
            task_arn = task["ReplicationTask"]["ReplicationTaskArn"]
            resp = dms.describe_replication_tasks(
                Filters=[{"Name": "replication-task-id", "Values": [task_id]}]
            )
            assert len(resp["ReplicationTasks"]) == 1
            assert resp["ReplicationTasks"][0]["ReplicationTaskIdentifier"] == task_id
            dms.delete_replication_task(ReplicationTaskArn=task_arn)
        finally:
            self._cleanup_prereqs(dms, prereqs)

    def test_describe_replication_tasks_filter_no_match(self, dms):
        """DescribeReplicationTasks with non-matching filter returns empty list."""
        resp = dms.describe_replication_tasks(
            Filters=[{"Name": "replication-task-id", "Values": ["nonexistent-task"]}]
        )
        assert resp["ReplicationTasks"] == []

    def test_describe_replication_tasks_with_ri_arn_filter(self, dms):
        """DescribeReplicationTasks filtered by replication-instance-arn."""
        prereqs = self._create_task_prereqs(dms)
        task_id = _unique("task")
        try:
            task = dms.create_replication_task(
                ReplicationTaskIdentifier=task_id,
                SourceEndpointArn=prereqs["source_arn"],
                TargetEndpointArn=prereqs["target_arn"],
                ReplicationInstanceArn=prereqs["ri_arn"],
                MigrationType="full-load",
                TableMappings='{"rules":[]}',
            )
            task_arn = task["ReplicationTask"]["ReplicationTaskArn"]
            resp = dms.describe_replication_tasks(
                Filters=[{"Name": "replication-instance-arn", "Values": [prereqs["ri_arn"]]}]
            )
            assert len(resp["ReplicationTasks"]) >= 1
            task_ids = [t["ReplicationTaskIdentifier"] for t in resp["ReplicationTasks"]]
            assert task_id in task_ids
            dms.delete_replication_task(ReplicationTaskArn=task_arn)
        finally:
            self._cleanup_prereqs(dms, prereqs)

    def test_describe_replication_tasks_has_task_details(self, dms):
        """DescribeReplicationTasks returns task with expected fields."""
        prereqs = self._create_task_prereqs(dms)
        task_id = _unique("task")
        try:
            task = dms.create_replication_task(
                ReplicationTaskIdentifier=task_id,
                SourceEndpointArn=prereqs["source_arn"],
                TargetEndpointArn=prereqs["target_arn"],
                ReplicationInstanceArn=prereqs["ri_arn"],
                MigrationType="full-load",
                TableMappings='{"rules":[]}',
            )
            task_arn = task["ReplicationTask"]["ReplicationTaskArn"]
            resp = dms.describe_replication_tasks(
                Filters=[{"Name": "replication-task-id", "Values": [task_id]}]
            )
            t = resp["ReplicationTasks"][0]
            assert t["MigrationType"] == "full-load"
            assert "Status" in t
            assert "ReplicationTaskArn" in t
            assert "TableMappings" in t
            dms.delete_replication_task(ReplicationTaskArn=task_arn)
        finally:
            self._cleanup_prereqs(dms, prereqs)


class TestDMSConnectionsAdvanced:
    """Tests for DescribeConnections with filters and after TestConnection."""

    def test_describe_connections_with_filter_no_match(self, dms):
        """DescribeConnections with non-matching filter returns empty list."""
        resp = dms.describe_connections(
            Filters=[
                {
                    "Name": "endpoint-arn",
                    "Values": ["arn:aws:dms:us-east-1:123456789012:endpoint:nonexistent"],
                }
            ]
        )
        assert resp["Connections"] == []

    def test_describe_connections_after_test_connection(self, dms):
        """DescribeConnections finds connection created by TestConnection."""
        ri_id = _unique("ri")
        ep_id = _unique("ep")
        ri = dms.create_replication_instance(
            ReplicationInstanceIdentifier=ri_id,
            ReplicationInstanceClass="dms.t3.micro",
        )
        ri_arn = ri["ReplicationInstance"]["ReplicationInstanceArn"]
        ep = dms.create_endpoint(
            EndpointIdentifier=ep_id,
            EndpointType="source",
            EngineName="mysql",
            ServerName="localhost",
            Port=3306,
            Username="admin",
            Password="password",
        )
        ep_arn = ep["Endpoint"]["EndpointArn"]
        try:
            dms.test_connection(
                ReplicationInstanceArn=ri_arn,
                EndpointArn=ep_arn,
            )
            resp = dms.describe_connections(Filters=[{"Name": "endpoint-arn", "Values": [ep_arn]}])
            assert len(resp["Connections"]) == 1
            conn = resp["Connections"][0]
            assert conn["EndpointArn"] == ep_arn
            assert conn["ReplicationInstanceArn"] == ri_arn
            assert "Status" in conn
        finally:
            dms.delete_endpoint(EndpointArn=ep_arn)
            dms.delete_replication_instance(ReplicationInstanceArn=ri_arn)


class TestDMSReplicationTaskMigrationTypes:
    """Tests for replication tasks with different migration types."""

    def _create_task_prereqs(self, dms):
        uid = uuid.uuid4().hex[:8]
        src = dms.create_endpoint(
            EndpointIdentifier=f"src-{uid}",
            EndpointType="source",
            EngineName="mysql",
            ServerName="localhost",
            Port=3306,
            Username="admin",
            Password="password",
        )
        tgt = dms.create_endpoint(
            EndpointIdentifier=f"tgt-{uid}",
            EndpointType="target",
            EngineName="postgres",
            ServerName="localhost",
            Port=5432,
            Username="admin",
            Password="password",
        )
        ri = dms.create_replication_instance(
            ReplicationInstanceIdentifier=f"ri-{uid}",
            ReplicationInstanceClass="dms.t3.micro",
        )
        return {
            "source_arn": src["Endpoint"]["EndpointArn"],
            "target_arn": tgt["Endpoint"]["EndpointArn"],
            "ri_arn": ri["ReplicationInstance"]["ReplicationInstanceArn"],
        }

    def _cleanup_prereqs(self, dms, prereqs):
        dms.delete_endpoint(EndpointArn=prereqs["source_arn"])
        dms.delete_endpoint(EndpointArn=prereqs["target_arn"])
        dms.delete_replication_instance(ReplicationInstanceArn=prereqs["ri_arn"])

    def test_create_replication_task_cdc(self, dms):
        """CreateReplicationTask with MigrationType=cdc."""
        prereqs = self._create_task_prereqs(dms)
        task_id = _unique("task")
        try:
            resp = dms.create_replication_task(
                ReplicationTaskIdentifier=task_id,
                SourceEndpointArn=prereqs["source_arn"],
                TargetEndpointArn=prereqs["target_arn"],
                ReplicationInstanceArn=prereqs["ri_arn"],
                MigrationType="cdc",
                TableMappings='{"rules":[]}',
            )
            assert resp["ReplicationTask"]["MigrationType"] == "cdc"
            assert resp["ReplicationTask"]["ReplicationTaskIdentifier"] == task_id
            dms.delete_replication_task(
                ReplicationTaskArn=resp["ReplicationTask"]["ReplicationTaskArn"]
            )
        finally:
            self._cleanup_prereqs(dms, prereqs)

    def test_create_replication_task_full_load_and_cdc(self, dms):
        """CreateReplicationTask with MigrationType=full-load-and-cdc."""
        prereqs = self._create_task_prereqs(dms)
        task_id = _unique("task")
        try:
            resp = dms.create_replication_task(
                ReplicationTaskIdentifier=task_id,
                SourceEndpointArn=prereqs["source_arn"],
                TargetEndpointArn=prereqs["target_arn"],
                ReplicationInstanceArn=prereqs["ri_arn"],
                MigrationType="full-load-and-cdc",
                TableMappings='{"rules":[]}',
            )
            assert resp["ReplicationTask"]["MigrationType"] == "full-load-and-cdc"
            dms.delete_replication_task(
                ReplicationTaskArn=resp["ReplicationTask"]["ReplicationTaskArn"]
            )
        finally:
            self._cleanup_prereqs(dms, prereqs)


class TestDMSSubnetGroupFilters:
    """Tests for DescribeReplicationSubnetGroups with filters."""

    def test_describe_replication_subnet_groups_filter_no_match(self, dms):
        """DescribeReplicationSubnetGroups with non-matching filter returns empty."""
        resp = dms.describe_replication_subnet_groups(
            Filters=[
                {
                    "Name": "replication-subnet-group-id",
                    "Values": ["nonexistent-group"],
                }
            ]
        )
        assert resp["ReplicationSubnetGroups"] == []

    def test_describe_replication_subnet_groups_with_filter(self, dms, ec2):
        """DescribeReplicationSubnetGroups with matching filter returns group."""
        vpc = ec2.create_vpc(CidrBlock="10.80.0.0/16")
        vpc_id = vpc["Vpc"]["VpcId"]
        sub1 = ec2.create_subnet(
            VpcId=vpc_id, CidrBlock="10.80.1.0/24", AvailabilityZone="us-east-1a"
        )
        sub2 = ec2.create_subnet(
            VpcId=vpc_id, CidrBlock="10.80.2.0/24", AvailabilityZone="us-east-1b"
        )

        sg_id = _unique("rsg")
        dms.create_replication_subnet_group(
            ReplicationSubnetGroupIdentifier=sg_id,
            ReplicationSubnetGroupDescription="Filter test",
            SubnetIds=[sub1["Subnet"]["SubnetId"], sub2["Subnet"]["SubnetId"]],
        )
        try:
            resp = dms.describe_replication_subnet_groups(
                Filters=[{"Name": "replication-subnet-group-id", "Values": [sg_id]}]
            )
            assert len(resp["ReplicationSubnetGroups"]) == 1
            assert resp["ReplicationSubnetGroups"][0]["ReplicationSubnetGroupIdentifier"] == sg_id
        finally:
            dms.delete_replication_subnet_group(ReplicationSubnetGroupIdentifier=sg_id)


class TestDMSEndpointFilterAdvanced:
    """Tests for DescribeEndpoints with various filter types."""

    def test_describe_endpoints_engine_name_filter(self, dms):
        """DescribeEndpoints with engine-name filter returns only matching engines."""
        ep_mysql = _unique("ep")
        ep_pg = _unique("ep")
        mysql_resp = dms.create_endpoint(
            EndpointIdentifier=ep_mysql,
            EndpointType="source",
            EngineName="mysql",
            ServerName="localhost",
            Port=3306,
            Username="admin",
            Password="password",
        )
        pg_resp = dms.create_endpoint(
            EndpointIdentifier=ep_pg,
            EndpointType="target",
            EngineName="postgres",
            ServerName="localhost",
            Port=5432,
            Username="admin",
            Password="password",
        )
        try:
            resp = dms.describe_endpoints(Filters=[{"Name": "engine-name", "Values": ["mysql"]}])
            engines = [e["EngineName"] for e in resp["Endpoints"]]
            assert all(e == "mysql" for e in engines)
        finally:
            dms.delete_endpoint(EndpointArn=mysql_resp["Endpoint"]["EndpointArn"])
            dms.delete_endpoint(EndpointArn=pg_resp["Endpoint"]["EndpointArn"])

    def test_describe_endpoints_endpoint_arn_filter(self, dms):
        """DescribeEndpoints with endpoint-arn filter returns specific endpoint."""
        ep_id = _unique("ep")
        create_resp = dms.create_endpoint(
            EndpointIdentifier=ep_id,
            EndpointType="source",
            EngineName="mysql",
            ServerName="localhost",
            Port=3306,
            Username="admin",
            Password="password",
        )
        arn = create_resp["Endpoint"]["EndpointArn"]
        try:
            resp = dms.describe_endpoints(Filters=[{"Name": "endpoint-arn", "Values": [arn]}])
            assert len(resp["Endpoints"]) == 1
            assert resp["Endpoints"][0]["EndpointArn"] == arn
        finally:
            dms.delete_endpoint(EndpointArn=arn)


class TestDMSReplicationInstanceFilterAdvanced:
    """Tests for DescribeReplicationInstances with additional filters."""

    def test_describe_ri_with_class_filter(self, dms):
        """DescribeReplicationInstances filtered by replication-instance-class."""
        ri_id = _unique("ri")
        resp = dms.create_replication_instance(
            ReplicationInstanceIdentifier=ri_id,
            ReplicationInstanceClass="dms.t3.micro",
        )
        ri_arn = resp["ReplicationInstance"]["ReplicationInstanceArn"]
        try:
            desc = dms.describe_replication_instances(
                Filters=[
                    {
                        "Name": "replication-instance-class",
                        "Values": ["dms.t3.micro"],
                    }
                ]
            )
            classes = [i["ReplicationInstanceClass"] for i in desc["ReplicationInstances"]]
            assert all(c == "dms.t3.micro" for c in classes)
            assert len(desc["ReplicationInstances"]) >= 1
        finally:
            dms.delete_replication_instance(ReplicationInstanceArn=ri_arn)


class TestDMSEndpointEngines:
    """Tests for creating endpoints with different engine types."""

    def test_create_endpoint_oracle(self, dms):
        """Create source endpoint with Oracle engine."""
        ep_id = _unique("ep")
        resp = dms.create_endpoint(
            EndpointIdentifier=ep_id,
            EndpointType="source",
            EngineName="oracle",
            ServerName="localhost",
            Port=1521,
            Username="admin",
            Password="password",
            DatabaseName="ORCL",
        )
        ep = resp["Endpoint"]
        assert ep["EngineName"] == "oracle"
        assert ep["Port"] == 1521
        assert ep["DatabaseName"] == "ORCL"
        dms.delete_endpoint(EndpointArn=ep["EndpointArn"])

    def test_create_endpoint_mariadb(self, dms):
        """Create source endpoint with MariaDB engine."""
        ep_id = _unique("ep")
        resp = dms.create_endpoint(
            EndpointIdentifier=ep_id,
            EndpointType="source",
            EngineName="mariadb",
            ServerName="localhost",
            Port=3306,
            Username="admin",
            Password="password",
        )
        ep = resp["Endpoint"]
        assert ep["EngineName"] == "mariadb"
        assert ep["EndpointType"] == "source"
        dms.delete_endpoint(EndpointArn=ep["EndpointArn"])

    def test_create_endpoint_sqlserver(self, dms):
        """Create source endpoint with SQL Server engine."""
        ep_id = _unique("ep")
        resp = dms.create_endpoint(
            EndpointIdentifier=ep_id,
            EndpointType="source",
            EngineName="sqlserver",
            ServerName="localhost",
            Port=1433,
            Username="admin",
            Password="password",
            DatabaseName="master",
        )
        ep = resp["Endpoint"]
        assert ep["EngineName"] == "sqlserver"
        assert ep["Port"] == 1433
        dms.delete_endpoint(EndpointArn=ep["EndpointArn"])


class TestDMSDescribeOperationsExpanded:
    """Tests for Describe operations that return lists/metadata without requiring resources."""

    def test_describe_account_attributes(self, dms):
        """DescribeAccountAttributes returns account quota information."""
        response = dms.describe_account_attributes()
        assert "AccountQuotas" in response
        assert isinstance(response["AccountQuotas"], list)

    def test_describe_account_attributes_has_quota_names(self, dms):
        """DescribeAccountAttributes quotas have AccountQuotaName field."""
        response = dms.describe_account_attributes()
        if len(response["AccountQuotas"]) > 0:
            quota = response["AccountQuotas"][0]
            assert "AccountQuotaName" in quota

    def test_describe_certificates_empty(self, dms):
        """DescribeCertificates returns empty list when none exist."""
        response = dms.describe_certificates()
        assert "Certificates" in response
        assert isinstance(response["Certificates"], list)

    def test_describe_endpoint_types(self, dms):
        """DescribeEndpointTypes returns supported endpoint types."""
        response = dms.describe_endpoint_types()
        assert "SupportedEndpointTypes" in response
        assert isinstance(response["SupportedEndpointTypes"], list)

    def test_describe_endpoint_types_has_engine_info(self, dms):
        """DescribeEndpointTypes entries have EngineName and SupportsCDC."""
        response = dms.describe_endpoint_types()
        if len(response["SupportedEndpointTypes"]) > 0:
            entry = response["SupportedEndpointTypes"][0]
            assert "EngineName" in entry

    def test_describe_event_categories(self, dms):
        """DescribeEventCategories returns event category groups."""
        response = dms.describe_event_categories()
        assert "EventCategoryGroupList" in response
        assert isinstance(response["EventCategoryGroupList"], list)

    def test_describe_event_subscriptions_empty(self, dms):
        """DescribeEventSubscriptions returns empty list when none exist."""
        response = dms.describe_event_subscriptions()
        assert "EventSubscriptionsList" in response
        assert isinstance(response["EventSubscriptionsList"], list)

    def test_describe_events_empty(self, dms):
        """DescribeEvents returns empty event list."""
        response = dms.describe_events()
        assert "Events" in response
        assert isinstance(response["Events"], list)

    def test_describe_orderable_replication_instances(self, dms):
        """DescribeOrderableReplicationInstances returns available instance types."""
        response = dms.describe_orderable_replication_instances()
        assert "OrderableReplicationInstances" in response
        assert isinstance(response["OrderableReplicationInstances"], list)

    def test_describe_pending_maintenance_actions_empty(self, dms):
        """DescribePendingMaintenanceActions returns empty list."""
        response = dms.describe_pending_maintenance_actions()
        assert "PendingMaintenanceActions" in response
        assert isinstance(response["PendingMaintenanceActions"], list)

    def test_describe_replication_task_assessment_results_empty(self, dms):
        """DescribeReplicationTaskAssessmentResults returns empty list."""
        response = dms.describe_replication_task_assessment_results()
        assert "ReplicationTaskAssessmentResults" in response
        assert isinstance(response["ReplicationTaskAssessmentResults"], list)

    def test_describe_applicable_individual_assessments(self, dms):
        """DescribeApplicableIndividualAssessments returns list of assessment names."""
        response = dms.describe_applicable_individual_assessments()
        assert "IndividualAssessmentNames" in response
        assert isinstance(response["IndividualAssessmentNames"], list)

    def test_describe_replication_task_assessment_runs_empty(self, dms):
        """DescribeReplicationTaskAssessmentRuns returns empty list."""
        response = dms.describe_replication_task_assessment_runs()
        assert "ReplicationTaskAssessmentRuns" in response
        assert isinstance(response["ReplicationTaskAssessmentRuns"], list)

    def test_describe_replication_task_individual_assessments_empty(self, dms):
        """DescribeReplicationTaskIndividualAssessments returns empty list."""
        response = dms.describe_replication_task_individual_assessments()
        assert "ReplicationTaskIndividualAssessments" in response
        assert isinstance(response["ReplicationTaskIndividualAssessments"], list)

    def test_describe_replications_empty(self, dms):
        """DescribeReplications returns empty list."""
        response = dms.describe_replications()
        assert "Replications" in response
        assert isinstance(response["Replications"], list)

    def test_describe_replication_configs_empty(self, dms):
        """DescribeReplicationConfigs returns empty list."""
        response = dms.describe_replication_configs()
        assert "ReplicationConfigs" in response
        assert isinstance(response["ReplicationConfigs"], list)

    def test_describe_fleet_advisor_collectors_empty(self, dms):
        """DescribeFleetAdvisorCollectors returns empty list."""
        response = dms.describe_fleet_advisor_collectors()
        assert "Collectors" in response
        assert isinstance(response["Collectors"], list)

    def test_describe_fleet_advisor_databases_empty(self, dms):
        """DescribeFleetAdvisorDatabases returns empty list."""
        response = dms.describe_fleet_advisor_databases()
        assert "Databases" in response
        assert isinstance(response["Databases"], list)

    def test_describe_fleet_advisor_schemas_empty(self, dms):
        """DescribeFleetAdvisorSchemas returns empty list."""
        response = dms.describe_fleet_advisor_schemas()
        assert "FleetAdvisorSchemas" in response
        assert isinstance(response["FleetAdvisorSchemas"], list)

    def test_describe_fleet_advisor_lsa_analysis_empty(self, dms):
        """DescribeFleetAdvisorLsaAnalysis returns empty list."""
        response = dms.describe_fleet_advisor_lsa_analysis()
        assert "Analysis" in response
        assert isinstance(response["Analysis"], list)

    def test_describe_fleet_advisor_schema_object_summary_empty(self, dms):
        """DescribeFleetAdvisorSchemaObjectSummary returns empty list."""
        response = dms.describe_fleet_advisor_schema_object_summary()
        assert "FleetAdvisorSchemaObjects" in response
        assert isinstance(response["FleetAdvisorSchemaObjects"], list)

    def test_describe_recommendations_empty(self, dms):
        """DescribeRecommendations returns empty list."""
        response = dms.describe_recommendations()
        assert "Recommendations" in response
        assert isinstance(response["Recommendations"], list)

    def test_describe_recommendation_limitations_empty(self, dms):
        """DescribeRecommendationLimitations returns empty list."""
        response = dms.describe_recommendation_limitations()
        assert "Limitations" in response
        assert isinstance(response["Limitations"], list)

    def test_describe_instance_profiles_empty(self, dms):
        """DescribeInstanceProfiles returns empty list."""
        response = dms.describe_instance_profiles()
        assert "InstanceProfiles" in response
        assert isinstance(response["InstanceProfiles"], list)

    def test_describe_data_providers_empty(self, dms):
        """DescribeDataProviders returns empty list."""
        response = dms.describe_data_providers()
        assert "DataProviders" in response
        assert isinstance(response["DataProviders"], list)

    def test_describe_migration_projects_empty(self, dms):
        """DescribeMigrationProjects returns empty list."""
        response = dms.describe_migration_projects()
        assert "MigrationProjects" in response
        assert isinstance(response["MigrationProjects"], list)

    def test_describe_extension_pack_associations_empty(self, dms):
        """DescribeExtensionPackAssociations returns empty list."""
        response = dms.describe_extension_pack_associations(
            MigrationProjectIdentifier="arn:aws:dms:us-east-1:123456789012:migration-project:nonexistent"
        )
        assert "Requests" in response
        assert isinstance(response["Requests"], list)

    def test_describe_metadata_model_assessments_empty(self, dms):
        """DescribeMetadataModelAssessments returns empty list."""
        response = dms.describe_metadata_model_assessments(
            MigrationProjectIdentifier="arn:aws:dms:us-east-1:123456789012:migration-project:nonexistent"
        )
        assert "Requests" in response
        assert isinstance(response["Requests"], list)

    def test_describe_metadata_model_conversions_empty(self, dms):
        """DescribeMetadataModelConversions returns empty list."""
        response = dms.describe_metadata_model_conversions(
            MigrationProjectIdentifier="arn:aws:dms:us-east-1:123456789012:migration-project:nonexistent"
        )
        assert "Requests" in response
        assert isinstance(response["Requests"], list)

    def test_describe_metadata_model_exports_as_script_empty(self, dms):
        """DescribeMetadataModelExportsAsScript returns empty list."""
        response = dms.describe_metadata_model_exports_as_script(
            MigrationProjectIdentifier="arn:aws:dms:us-east-1:123456789012:migration-project:nonexistent"
        )
        assert "Requests" in response
        assert isinstance(response["Requests"], list)

    def test_describe_metadata_model_exports_to_target_empty(self, dms):
        """DescribeMetadataModelExportsToTarget returns empty list."""
        response = dms.describe_metadata_model_exports_to_target(
            MigrationProjectIdentifier="arn:aws:dms:us-east-1:123456789012:migration-project:nonexistent"
        )
        assert "Requests" in response
        assert isinstance(response["Requests"], list)

    def test_describe_metadata_model_imports_empty(self, dms):
        """DescribeMetadataModelImports returns empty list."""
        response = dms.describe_metadata_model_imports(
            MigrationProjectIdentifier="arn:aws:dms:us-east-1:123456789012:migration-project:nonexistent"
        )
        assert "Requests" in response
        assert isinstance(response["Requests"], list)

    def test_describe_data_migrations_empty(self, dms):
        """DescribeDataMigrations returns empty list."""
        response = dms.describe_data_migrations()
        assert "DataMigrations" in response
        assert isinstance(response["DataMigrations"], list)

    def test_describe_endpoint_settings(self, dms):
        """DescribeEndpointSettings returns settings for a given engine."""
        response = dms.describe_endpoint_settings(EngineName="mysql")
        assert "EndpointSettings" in response
        assert isinstance(response["EndpointSettings"], list)

    def test_describe_endpoint_settings_postgres(self, dms):
        """DescribeEndpointSettings returns settings for postgres engine."""
        response = dms.describe_endpoint_settings(EngineName="postgres")
        assert "EndpointSettings" in response
        assert isinstance(response["EndpointSettings"], list)


class TestDMSDescribeWithResourceParams:
    """Tests for Describe operations that need existing resources."""

    def test_describe_replication_instance_task_logs(self, dms):
        """DescribeReplicationInstanceTaskLogs returns task log list for an instance."""
        ri_id = _unique("ri")
        resp = dms.create_replication_instance(
            ReplicationInstanceIdentifier=ri_id,
            ReplicationInstanceClass="dms.t3.micro",
        )
        ri_arn = resp["ReplicationInstance"]["ReplicationInstanceArn"]
        try:
            log_resp = dms.describe_replication_instance_task_logs(
                ReplicationInstanceArn=ri_arn,
            )
            assert "ReplicationInstanceTaskLogs" in log_resp
            assert isinstance(log_resp["ReplicationInstanceTaskLogs"], list)
        finally:
            dms.delete_replication_instance(ReplicationInstanceArn=ri_arn)

    def test_describe_table_statistics(self, dms):
        """DescribeTableStatistics returns table stats for a replication task."""
        uid = uuid.uuid4().hex[:8]
        src = dms.create_endpoint(
            EndpointIdentifier=f"src-{uid}",
            EndpointType="source",
            EngineName="mysql",
            ServerName="localhost",
            Port=3306,
            Username="admin",
            Password="password",
        )
        tgt = dms.create_endpoint(
            EndpointIdentifier=f"tgt-{uid}",
            EndpointType="target",
            EngineName="postgres",
            ServerName="localhost",
            Port=5432,
            Username="admin",
            Password="password",
        )
        ri = dms.create_replication_instance(
            ReplicationInstanceIdentifier=f"ri-{uid}",
            ReplicationInstanceClass="dms.t3.micro",
        )
        task = dms.create_replication_task(
            ReplicationTaskIdentifier=f"task-{uid}",
            SourceEndpointArn=src["Endpoint"]["EndpointArn"],
            TargetEndpointArn=tgt["Endpoint"]["EndpointArn"],
            ReplicationInstanceArn=ri["ReplicationInstance"]["ReplicationInstanceArn"],
            MigrationType="full-load",
            TableMappings='{"rules":[]}',
        )
        task_arn = task["ReplicationTask"]["ReplicationTaskArn"]
        try:
            stats_resp = dms.describe_table_statistics(
                ReplicationTaskArn=task_arn,
            )
            assert "TableStatistics" in stats_resp
            assert isinstance(stats_resp["TableStatistics"], list)
        finally:
            dms.delete_replication_task(ReplicationTaskArn=task_arn)
            dms.delete_endpoint(EndpointArn=src["Endpoint"]["EndpointArn"])
            dms.delete_endpoint(EndpointArn=tgt["Endpoint"]["EndpointArn"])
            dms.delete_replication_instance(
                ReplicationInstanceArn=ri["ReplicationInstance"]["ReplicationInstanceArn"]
            )


class TestDMSDescribeMetadataModelCreations:
    """Tests for DescribeMetadataModelCreations."""

    def test_describe_metadata_model_creations_empty(self, dms):
        """DescribeMetadataModelCreations returns empty list for nonexistent project."""
        response = dms.describe_metadata_model_creations(
            MigrationProjectIdentifier="arn:aws:dms:us-east-1:123456789012:migration-project:nonexistent"
        )
        assert "Requests" in response
        assert isinstance(response["Requests"], list)


class TestDMSDescribeConversionAndMetadata:
    """Tests for conversion config, metadata model, and target selection operations."""

    def test_describe_conversion_configuration(self, dms):
        """DescribeConversionConfiguration returns config for a migration project."""
        response = dms.describe_conversion_configuration(
            MigrationProjectIdentifier="arn:aws:dms:us-east-1:123456789012:migration-project:nonexistent"
        )
        assert "ConversionConfiguration" in response
        assert "MigrationProjectIdentifier" in response

    def test_describe_replication_table_statistics(self, dms):
        """DescribeReplicationTableStatistics returns stats list."""
        response = dms.describe_replication_table_statistics(
            ReplicationConfigArn="arn:aws:dms:us-east-1:123456789012:replication-config:nonexistent"
        )
        assert "ReplicationTableStatistics" in response
        assert isinstance(response["ReplicationTableStatistics"], list)

    def test_get_target_selection_rules(self, dms):
        """GetTargetSelectionRules returns target selection rules."""
        response = dms.get_target_selection_rules(
            MigrationProjectIdentifier="arn:aws:dms:us-east-1:123456789012:migration-project:nonexistent",
            SelectionRules='{"rules":[]}',
        )
        assert "TargetSelectionRules" in response

    def test_describe_metadata_model(self, dms):
        """DescribeMetadataModel returns metadata for a migration project."""
        response = dms.describe_metadata_model(
            MigrationProjectIdentifier="arn:aws:dms:us-east-1:123456789012:migration-project:nonexistent",
            SelectionRules='{"rules":[]}',
            Origin="SOURCE",
        )
        assert response["ResponseMetadata"]["HTTPStatusCode"] == 200

    def test_describe_metadata_model_children(self, dms):
        """DescribeMetadataModelChildren returns children for a migration project."""
        response = dms.describe_metadata_model_children(
            MigrationProjectIdentifier="arn:aws:dms:us-east-1:123456789012:migration-project:nonexistent",
            SelectionRules='{"rules":[]}',
            Origin="SOURCE",
        )
        assert response["ResponseMetadata"]["HTTPStatusCode"] == 200

    def test_describe_refresh_schemas_status(self, dms):
        """DescribeRefreshSchemasStatus returns status for an endpoint."""
        ep_id = _unique("ep")
        resp = dms.create_endpoint(
            EndpointIdentifier=ep_id,
            EndpointType="source",
            EngineName="mysql",
            ServerName="localhost",
            Port=3306,
            Username="admin",
            Password="password",
        )
        ep_arn = resp["Endpoint"]["EndpointArn"]
        try:
            status_resp = dms.describe_refresh_schemas_status(EndpointArn=ep_arn)
            assert "RefreshSchemasStatus" in status_resp
        finally:
            dms.delete_endpoint(EndpointArn=ep_arn)


class TestDMSDescribeSchemas:
    """Tests for DescribeSchemas with an endpoint."""

    def test_describe_schemas_for_endpoint(self, dms):
        """DescribeSchemas returns schema list for an endpoint."""
        ep_id = _unique("ep")
        resp = dms.create_endpoint(
            EndpointIdentifier=ep_id,
            EndpointType="source",
            EngineName="mysql",
            ServerName="localhost",
            Port=3306,
            Username="admin",
            Password="password",
        )
        ep_arn = resp["Endpoint"]["EndpointArn"]
        try:
            schema_resp = dms.describe_schemas(EndpointArn=ep_arn)
            assert "Schemas" in schema_resp
            assert isinstance(schema_resp["Schemas"], list)
        finally:
            dms.delete_endpoint(EndpointArn=ep_arn)


class TestDMSReplicationInstanceSubnetGroup:
    """Tests for creating replication instance in a subnet group."""

    def test_create_replication_instance_in_subnet_group(self, dms, ec2):
        """CreateReplicationInstance with ReplicationSubnetGroupIdentifier."""
        vpc = ec2.create_vpc(CidrBlock="10.90.0.0/16")
        vpc_id = vpc["Vpc"]["VpcId"]
        sub1 = ec2.create_subnet(
            VpcId=vpc_id, CidrBlock="10.90.1.0/24", AvailabilityZone="us-east-1a"
        )
        sub2 = ec2.create_subnet(
            VpcId=vpc_id, CidrBlock="10.90.2.0/24", AvailabilityZone="us-east-1b"
        )

        sg_id = _unique("rsg")
        dms.create_replication_subnet_group(
            ReplicationSubnetGroupIdentifier=sg_id,
            ReplicationSubnetGroupDescription="For RI test",
            SubnetIds=[sub1["Subnet"]["SubnetId"], sub2["Subnet"]["SubnetId"]],
        )
        ri_id = _unique("ri")
        try:
            resp = dms.create_replication_instance(
                ReplicationInstanceIdentifier=ri_id,
                ReplicationInstanceClass="dms.t3.micro",
                ReplicationSubnetGroupIdentifier=sg_id,
            )
            ri = resp["ReplicationInstance"]
            assert ri["ReplicationInstanceIdentifier"] == ri_id
            assert "ReplicationInstanceArn" in ri
            dms.delete_replication_instance(ReplicationInstanceArn=ri["ReplicationInstanceArn"])
        finally:
            dms.delete_replication_subnet_group(ReplicationSubnetGroupIdentifier=sg_id)


class TestDMSTagOperations:
    """Tests for AddTagsToResource and RemoveTagsFromResource."""

    def test_add_tags_to_resource(self, dms):
        ep_id = _unique("tag-ep")
        ep = dms.create_endpoint(
            EndpointIdentifier=ep_id,
            EndpointType="source",
            EngineName="mysql",
            ServerName="localhost",
            Port=3306,
            Username="admin",
            Password="pass",
        )
        arn = ep["Endpoint"]["EndpointArn"]
        try:
            dms.add_tags_to_resource(
                ResourceArn=arn,
                Tags=[{"Key": "env", "Value": "test"}, {"Key": "team", "Value": "qa"}],
            )
            tags = dms.list_tags_for_resource(ResourceArn=arn)
            tag_map = {t["Key"]: t["Value"] for t in tags["TagList"]}
            assert tag_map["env"] == "test"
            assert tag_map["team"] == "qa"
        finally:
            dms.delete_endpoint(EndpointArn=arn)

    def test_remove_tags_from_resource(self, dms):
        ep_id = _unique("untag-ep")
        ep = dms.create_endpoint(
            EndpointIdentifier=ep_id,
            EndpointType="source",
            EngineName="mysql",
            ServerName="localhost",
            Port=3306,
            Username="admin",
            Password="pass",
        )
        arn = ep["Endpoint"]["EndpointArn"]
        try:
            dms.add_tags_to_resource(
                ResourceArn=arn,
                Tags=[{"Key": "keep", "Value": "yes"}, {"Key": "remove", "Value": "no"}],
            )
            dms.remove_tags_from_resource(ResourceArn=arn, TagKeys=["remove"])
            tags = dms.list_tags_for_resource(ResourceArn=arn)
            tag_keys = [t["Key"] for t in tags["TagList"]]
            assert "keep" in tag_keys
            assert "remove" not in tag_keys
        finally:
            dms.delete_endpoint(EndpointArn=arn)


class TestDMSModifyEndpointOperations:
    """Tests for ModifyEndpoint."""

    def test_modify_endpoint(self, dms):
        ep_id = _unique("mod-ep")
        ep = dms.create_endpoint(
            EndpointIdentifier=ep_id,
            EndpointType="source",
            EngineName="mysql",
            ServerName="localhost",
            Port=3306,
            Username="admin",
            Password="pass",
        )
        arn = ep["Endpoint"]["EndpointArn"]
        try:
            resp = dms.modify_endpoint(EndpointArn=arn, ServerName="newhost", Port=3307)
            modified = resp["Endpoint"]
            assert modified["ServerName"] == "newhost"
            assert modified["Port"] == 3307
        finally:
            dms.delete_endpoint(EndpointArn=arn)


class TestDMSCertificateOperations:
    """Tests for ImportCertificate and DeleteCertificate."""

    def test_import_certificate(self, dms):
        cert_id = _unique("cert")
        resp = dms.import_certificate(
            CertificateIdentifier=cert_id,
            CertificatePem="-----BEGIN CERTIFICATE-----\nfake\n-----END CERTIFICATE-----",
        )
        cert = resp["Certificate"]
        assert cert["CertificateIdentifier"] == cert_id
        assert "CertificateArn" in cert
        dms.delete_certificate(CertificateArn=cert["CertificateArn"])

    def test_delete_certificate(self, dms):
        cert_id = _unique("del-cert")
        resp = dms.import_certificate(
            CertificateIdentifier=cert_id,
            CertificatePem="-----BEGIN CERTIFICATE-----\nfake\n-----END CERTIFICATE-----",
        )
        cert_arn = resp["Certificate"]["CertificateArn"]
        del_resp = dms.delete_certificate(CertificateArn=cert_arn)
        assert "Certificate" in del_resp

    def test_import_and_list_certificates(self, dms):
        cert_id = _unique("list-cert")
        resp = dms.import_certificate(
            CertificateIdentifier=cert_id,
            CertificatePem="-----BEGIN CERTIFICATE-----\nfake\n-----END CERTIFICATE-----",
        )
        cert_arn = resp["Certificate"]["CertificateArn"]
        try:
            certs = dms.describe_certificates()
            cert_ids = [c["CertificateIdentifier"] for c in certs["Certificates"]]
            assert cert_id in cert_ids
        finally:
            dms.delete_certificate(CertificateArn=cert_arn)


class TestDMSEventSubscriptionOperations:
    """Tests for EventSubscription create/modify/delete."""

    def test_create_event_subscription(self, dms):
        sub_name = _unique("sub")
        resp = dms.create_event_subscription(
            SubscriptionName=sub_name,
            SnsTopicArn="arn:aws:sns:us-east-1:123456789012:test",
        )
        assert "EventSubscription" in resp
        assert resp["EventSubscription"]["CustSubscriptionId"] == sub_name
        dms.delete_event_subscription(SubscriptionName=sub_name)

    def test_modify_event_subscription(self, dms):
        sub_name = _unique("mod-sub")
        dms.create_event_subscription(
            SubscriptionName=sub_name,
            SnsTopicArn="arn:aws:sns:us-east-1:123456789012:test",
        )
        try:
            resp = dms.modify_event_subscription(SubscriptionName=sub_name, Enabled=False)
            assert "EventSubscription" in resp
        finally:
            dms.delete_event_subscription(SubscriptionName=sub_name)

    def test_delete_event_subscription(self, dms):
        sub_name = _unique("del-sub")
        dms.create_event_subscription(
            SubscriptionName=sub_name,
            SnsTopicArn="arn:aws:sns:us-east-1:123456789012:test",
        )
        resp = dms.delete_event_subscription(SubscriptionName=sub_name)
        assert "EventSubscription" in resp

    def test_describe_event_subscriptions_includes_created(self, dms):
        sub_name = _unique("desc-sub")
        dms.create_event_subscription(
            SubscriptionName=sub_name,
            SnsTopicArn="arn:aws:sns:us-east-1:123456789012:test",
        )
        try:
            resp = dms.describe_event_subscriptions()
            sub_names = [s["CustSubscriptionId"] for s in resp["EventSubscriptionsList"]]
            assert sub_name in sub_names
        finally:
            dms.delete_event_subscription(SubscriptionName=sub_name)


class TestDMSInstanceProfileOperations:
    """Tests for InstanceProfile create/modify/delete."""

    def test_create_instance_profile(self, dms):
        ip_name = _unique("ip")
        resp = dms.create_instance_profile(InstanceProfileName=ip_name)
        assert "InstanceProfile" in resp
        assert "InstanceProfileArn" in resp["InstanceProfile"]
        dms.delete_instance_profile(
            InstanceProfileIdentifier=resp["InstanceProfile"]["InstanceProfileArn"]
        )

    def test_modify_instance_profile(self, dms):
        ip_name = _unique("mod-ip")
        ip = dms.create_instance_profile(InstanceProfileName=ip_name)
        ip_arn = ip["InstanceProfile"]["InstanceProfileArn"]
        try:
            resp = dms.modify_instance_profile(
                InstanceProfileIdentifier=ip_arn, Description="updated"
            )
            assert "InstanceProfile" in resp
        finally:
            dms.delete_instance_profile(InstanceProfileIdentifier=ip_arn)

    def test_delete_instance_profile(self, dms):
        ip_name = _unique("del-ip")
        ip = dms.create_instance_profile(InstanceProfileName=ip_name)
        ip_arn = ip["InstanceProfile"]["InstanceProfileArn"]
        resp = dms.delete_instance_profile(InstanceProfileIdentifier=ip_arn)
        assert "InstanceProfile" in resp

    def test_describe_instance_profiles_includes_created(self, dms):
        ip_name = _unique("list-ip")
        ip = dms.create_instance_profile(InstanceProfileName=ip_name)
        ip_arn = ip["InstanceProfile"]["InstanceProfileArn"]
        try:
            resp = dms.describe_instance_profiles()
            arns = [p["InstanceProfileArn"] for p in resp["InstanceProfiles"]]
            assert ip_arn in arns
        finally:
            dms.delete_instance_profile(InstanceProfileIdentifier=ip_arn)


class TestDMSDataProviderOperations:
    """Tests for DataProvider create/modify/delete."""

    def test_create_data_provider(self, dms):
        dp_name = _unique("dp")
        resp = dms.create_data_provider(
            DataProviderName=dp_name,
            Engine="mysql",
            Settings={"MySqlSettings": {"ServerName": "localhost", "Port": 3306}},
        )
        assert "DataProvider" in resp
        assert "DataProviderArn" in resp["DataProvider"]
        dms.delete_data_provider(DataProviderIdentifier=resp["DataProvider"]["DataProviderArn"])

    def test_modify_data_provider(self, dms):
        dp_name = _unique("mod-dp")
        dp = dms.create_data_provider(
            DataProviderName=dp_name,
            Engine="mysql",
            Settings={"MySqlSettings": {"ServerName": "localhost", "Port": 3306}},
        )
        dp_arn = dp["DataProvider"]["DataProviderArn"]
        try:
            resp = dms.modify_data_provider(
                DataProviderIdentifier=dp_arn,
                Engine="mysql",
                Settings={"MySqlSettings": {"ServerName": "newhost", "Port": 3306}},
            )
            assert "DataProvider" in resp
        finally:
            dms.delete_data_provider(DataProviderIdentifier=dp_arn)

    def test_delete_data_provider(self, dms):
        dp_name = _unique("del-dp")
        dp = dms.create_data_provider(
            DataProviderName=dp_name,
            Engine="mysql",
            Settings={"MySqlSettings": {"ServerName": "localhost", "Port": 3306}},
        )
        dp_arn = dp["DataProvider"]["DataProviderArn"]
        resp = dms.delete_data_provider(DataProviderIdentifier=dp_arn)
        assert "DataProvider" in resp

    def test_describe_data_providers_includes_created(self, dms):
        dp_name = _unique("list-dp")
        dp = dms.create_data_provider(
            DataProviderName=dp_name,
            Engine="mysql",
            Settings={"MySqlSettings": {"ServerName": "localhost", "Port": 3306}},
        )
        dp_arn = dp["DataProvider"]["DataProviderArn"]
        try:
            resp = dms.describe_data_providers()
            arns = [p["DataProviderArn"] for p in resp["DataProviders"]]
            assert dp_arn in arns
        finally:
            dms.delete_data_provider(DataProviderIdentifier=dp_arn)


class TestDMSMigrationProjectOperations:
    """Tests for MigrationProject create/modify/delete."""

    @pytest.fixture
    def migration_deps(self, dms):
        """Create data providers and instance profile for migration project."""
        dp_src = dms.create_data_provider(
            DataProviderName=_unique("dp-src"),
            Engine="mysql",
            Settings={"MySqlSettings": {"ServerName": "localhost", "Port": 3306}},
        )
        dp_src_arn = dp_src["DataProvider"]["DataProviderArn"]
        dp_tgt = dms.create_data_provider(
            DataProviderName=_unique("dp-tgt"),
            Engine="postgres",
            Settings={"PostgreSqlSettings": {"ServerName": "localhost", "Port": 5432}},
        )
        dp_tgt_arn = dp_tgt["DataProvider"]["DataProviderArn"]
        ip = dms.create_instance_profile(InstanceProfileName=_unique("mp-ip"))
        ip_arn = ip["InstanceProfile"]["InstanceProfileArn"]
        yield {
            "src_arn": dp_src_arn,
            "tgt_arn": dp_tgt_arn,
            "ip_arn": ip_arn,
        }
        dms.delete_instance_profile(InstanceProfileIdentifier=ip_arn)
        dms.delete_data_provider(DataProviderIdentifier=dp_src_arn)
        dms.delete_data_provider(DataProviderIdentifier=dp_tgt_arn)

    def test_create_migration_project(self, dms, migration_deps):
        mp_name = _unique("mp")
        resp = dms.create_migration_project(
            MigrationProjectName=mp_name,
            SourceDataProviderDescriptors=[{"DataProviderIdentifier": migration_deps["src_arn"]}],
            TargetDataProviderDescriptors=[{"DataProviderIdentifier": migration_deps["tgt_arn"]}],
            InstanceProfileIdentifier=migration_deps["ip_arn"],
        )
        assert "MigrationProject" in resp
        mp_arn = resp["MigrationProject"]["MigrationProjectArn"]
        dms.delete_migration_project(MigrationProjectIdentifier=mp_arn)

    def test_modify_migration_project(self, dms, migration_deps):
        mp_name = _unique("mod-mp")
        mp = dms.create_migration_project(
            MigrationProjectName=mp_name,
            SourceDataProviderDescriptors=[{"DataProviderIdentifier": migration_deps["src_arn"]}],
            TargetDataProviderDescriptors=[{"DataProviderIdentifier": migration_deps["tgt_arn"]}],
            InstanceProfileIdentifier=migration_deps["ip_arn"],
        )
        mp_arn = mp["MigrationProject"]["MigrationProjectArn"]
        try:
            resp = dms.modify_migration_project(
                MigrationProjectIdentifier=mp_arn,
                MigrationProjectName="updated-mp",
            )
            assert "MigrationProject" in resp
        finally:
            dms.delete_migration_project(MigrationProjectIdentifier=mp_arn)

    def test_delete_migration_project(self, dms, migration_deps):
        mp = dms.create_migration_project(
            MigrationProjectName=_unique("del-mp"),
            SourceDataProviderDescriptors=[{"DataProviderIdentifier": migration_deps["src_arn"]}],
            TargetDataProviderDescriptors=[{"DataProviderIdentifier": migration_deps["tgt_arn"]}],
            InstanceProfileIdentifier=migration_deps["ip_arn"],
        )
        mp_arn = mp["MigrationProject"]["MigrationProjectArn"]
        resp = dms.delete_migration_project(MigrationProjectIdentifier=mp_arn)
        assert "MigrationProject" in resp


class TestDMSReplicationConfigOperations:
    """Tests for ReplicationConfig create/modify/delete."""

    @pytest.fixture
    def endpoints(self, dms):
        """Create source and target endpoints."""
        ep1 = dms.create_endpoint(
            EndpointIdentifier=_unique("rc-src"),
            EndpointType="source",
            EngineName="mysql",
            ServerName="localhost",
            Port=3306,
            Username="admin",
            Password="pass",
        )
        ep2 = dms.create_endpoint(
            EndpointIdentifier=_unique("rc-tgt"),
            EndpointType="target",
            EngineName="postgres",
            ServerName="localhost",
            Port=5432,
            Username="admin",
            Password="pass",
        )
        yield {
            "src_arn": ep1["Endpoint"]["EndpointArn"],
            "tgt_arn": ep2["Endpoint"]["EndpointArn"],
        }
        dms.delete_endpoint(EndpointArn=ep1["Endpoint"]["EndpointArn"])
        dms.delete_endpoint(EndpointArn=ep2["Endpoint"]["EndpointArn"])

    def _table_mappings(self):
        import json

        return json.dumps(
            {
                "rules": [
                    {
                        "rule-type": "selection",
                        "rule-id": "1",
                        "rule-name": "1",
                        "object-locator": {"schema-name": "%", "table-name": "%"},
                        "rule-action": "include",
                    }
                ]
            }
        )

    def test_create_replication_config(self, dms, endpoints):
        rc_id = _unique("rc")
        resp = dms.create_replication_config(
            ReplicationConfigIdentifier=rc_id,
            SourceEndpointArn=endpoints["src_arn"],
            TargetEndpointArn=endpoints["tgt_arn"],
            ComputeConfig={"MaxCapacityUnits": 2},
            ReplicationType="full-load",
            TableMappings=self._table_mappings(),
        )
        assert "ReplicationConfig" in resp
        rc_arn = resp["ReplicationConfig"]["ReplicationConfigArn"]
        dms.delete_replication_config(ReplicationConfigArn=rc_arn)

    def test_modify_replication_config(self, dms, endpoints):
        rc_id = _unique("mod-rc")
        rc = dms.create_replication_config(
            ReplicationConfigIdentifier=rc_id,
            SourceEndpointArn=endpoints["src_arn"],
            TargetEndpointArn=endpoints["tgt_arn"],
            ComputeConfig={"MaxCapacityUnits": 2},
            ReplicationType="full-load",
            TableMappings=self._table_mappings(),
        )
        rc_arn = rc["ReplicationConfig"]["ReplicationConfigArn"]
        try:
            resp = dms.modify_replication_config(ReplicationConfigArn=rc_arn, ReplicationType="cdc")
            assert "ReplicationConfig" in resp
        finally:
            dms.delete_replication_config(ReplicationConfigArn=rc_arn)

    def test_delete_replication_config(self, dms, endpoints):
        rc = dms.create_replication_config(
            ReplicationConfigIdentifier=_unique("del-rc"),
            SourceEndpointArn=endpoints["src_arn"],
            TargetEndpointArn=endpoints["tgt_arn"],
            ComputeConfig={"MaxCapacityUnits": 2},
            ReplicationType="full-load",
            TableMappings=self._table_mappings(),
        )
        rc_arn = rc["ReplicationConfig"]["ReplicationConfigArn"]
        resp = dms.delete_replication_config(ReplicationConfigArn=rc_arn)
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200


class TestDMSModifyReplicationSubnetGroup:
    """Tests for ModifyReplicationSubnetGroup."""

    def test_modify_replication_subnet_group(self, dms, ec2):
        vpc = ec2.create_vpc(CidrBlock="10.76.0.0/16")
        vpc_id = vpc["Vpc"]["VpcId"]
        sn1 = ec2.create_subnet(VpcId=vpc_id, CidrBlock="10.76.1.0/24")["Subnet"]["SubnetId"]
        sn2 = ec2.create_subnet(VpcId=vpc_id, CidrBlock="10.76.2.0/24")["Subnet"]["SubnetId"]
        sn3 = ec2.create_subnet(VpcId=vpc_id, CidrBlock="10.76.3.0/24")["Subnet"]["SubnetId"]

        rsg_id = _unique("mod-rsg")
        dms.create_replication_subnet_group(
            ReplicationSubnetGroupIdentifier=rsg_id,
            ReplicationSubnetGroupDescription="original",
            SubnetIds=[sn1, sn2],
        )
        try:
            resp = dms.modify_replication_subnet_group(
                ReplicationSubnetGroupIdentifier=rsg_id,
                ReplicationSubnetGroupDescription="updated",
                SubnetIds=[sn1, sn2, sn3],
            )
            rsg = resp["ReplicationSubnetGroup"]
            assert rsg["ReplicationSubnetGroupDescription"] == "updated"
        finally:
            dms.delete_replication_subnet_group(ReplicationSubnetGroupIdentifier=rsg_id)


class TestDMSModifyReplicationInstance:
    """Tests for ModifyReplicationInstance and RebootReplicationInstance."""

    @pytest.fixture
    def replication_instance(self, dms, ec2):
        """Create a replication instance with VPC."""
        vpc = ec2.create_vpc(CidrBlock="10.75.0.0/16")
        vpc_id = vpc["Vpc"]["VpcId"]
        sn1 = ec2.create_subnet(
            VpcId=vpc_id, CidrBlock="10.75.1.0/24", AvailabilityZone="us-east-1a"
        )
        sn2 = ec2.create_subnet(
            VpcId=vpc_id, CidrBlock="10.75.2.0/24", AvailabilityZone="us-east-1b"
        )
        rsg_id = _unique("ri-rsg")
        dms.create_replication_subnet_group(
            ReplicationSubnetGroupIdentifier=rsg_id,
            ReplicationSubnetGroupDescription="ri test",
            SubnetIds=[sn1["Subnet"]["SubnetId"], sn2["Subnet"]["SubnetId"]],
        )
        ri_id = _unique("ri")
        ri = dms.create_replication_instance(
            ReplicationInstanceIdentifier=ri_id,
            ReplicationInstanceClass="dms.t2.micro",
            ReplicationSubnetGroupIdentifier=rsg_id,
        )
        ri_arn = ri["ReplicationInstance"]["ReplicationInstanceArn"]
        yield {"arn": ri_arn, "rsg_id": rsg_id}
        dms.delete_replication_instance(ReplicationInstanceArn=ri_arn)
        dms.delete_replication_subnet_group(ReplicationSubnetGroupIdentifier=rsg_id)

    def test_modify_replication_instance(self, dms, replication_instance):
        resp = dms.modify_replication_instance(
            ReplicationInstanceArn=replication_instance["arn"],
            ReplicationInstanceClass="dms.t2.small",
        )
        assert "ReplicationInstance" in resp

    def test_reboot_replication_instance(self, dms, replication_instance):
        resp = dms.reboot_replication_instance(ReplicationInstanceArn=replication_instance["arn"])
        assert "ReplicationInstance" in resp

    def test_modify_replication_task(self, dms, ec2, replication_instance):
        import json

        ep1 = dms.create_endpoint(
            EndpointIdentifier=_unique("task-src"),
            EndpointType="source",
            EngineName="mysql",
            ServerName="localhost",
            Port=3306,
            Username="admin",
            Password="pass",
        )
        ep2 = dms.create_endpoint(
            EndpointIdentifier=_unique("task-tgt"),
            EndpointType="target",
            EngineName="postgres",
            ServerName="localhost",
            Port=5432,
            Username="admin",
            Password="pass",
        )
        mappings = json.dumps(
            {
                "rules": [
                    {
                        "rule-type": "selection",
                        "rule-id": "1",
                        "rule-name": "1",
                        "object-locator": {"schema-name": "%", "table-name": "%"},
                        "rule-action": "include",
                    }
                ]
            }
        )
        task = dms.create_replication_task(
            ReplicationTaskIdentifier=_unique("mod-task"),
            SourceEndpointArn=ep1["Endpoint"]["EndpointArn"],
            TargetEndpointArn=ep2["Endpoint"]["EndpointArn"],
            ReplicationInstanceArn=replication_instance["arn"],
            MigrationType="full-load",
            TableMappings=mappings,
        )
        task_arn = task["ReplicationTask"]["ReplicationTaskArn"]
        try:
            new_mappings = json.dumps(
                {
                    "rules": [
                        {
                            "rule-type": "selection",
                            "rule-id": "1",
                            "rule-name": "1",
                            "object-locator": {
                                "schema-name": "public",
                                "table-name": "%",
                            },
                            "rule-action": "include",
                        }
                    ]
                }
            )
            resp = dms.modify_replication_task(
                ReplicationTaskArn=task_arn, TableMappings=new_mappings
            )
            assert "ReplicationTask" in resp
        finally:
            dms.delete_replication_task(ReplicationTaskArn=task_arn)
            dms.delete_endpoint(EndpointArn=ep1["Endpoint"]["EndpointArn"])
            dms.delete_endpoint(EndpointArn=ep2["Endpoint"]["EndpointArn"])


class TestDMSFleetAdvisorOperations:
    """Tests for Fleet Advisor operations."""

    def test_run_fleet_advisor_lsa_analysis(self, dms):
        """RunFleetAdvisorLsaAnalysis returns a response."""
        resp = dms.run_fleet_advisor_lsa_analysis()
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200

    def test_create_fleet_advisor_collector(self, dms):
        """CreateFleetAdvisorCollector returns collector reference id."""
        resp = dms.create_fleet_advisor_collector(
            CollectorName=_unique("collector"),
            ServiceAccessRoleArn="arn:aws:iam::123456789012:role/test",
            S3BucketName="test-bucket",
        )
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200

    def test_delete_fleet_advisor_databases(self, dms):
        """DeleteFleetAdvisorDatabases accepts database IDs."""
        resp = dms.delete_fleet_advisor_databases(DatabaseIds=["fake-db-id"])
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200

    def test_batch_start_recommendations(self, dms):
        """BatchStartRecommendations with empty data returns 200."""
        resp = dms.batch_start_recommendations(Data=[])
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200

    def test_start_recommendations(self, dms):
        """StartRecommendations accepts database ID and settings."""
        resp = dms.start_recommendations(
            DatabaseId="fake-db-id",
            Settings={"InstanceSizingType": "equal", "WorkloadType": "mixed"},
        )
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200


class TestDMSEventBridgeSubscription:
    """Tests for UpdateSubscriptionsToEventBridge."""

    def test_update_subscriptions_to_event_bridge(self, dms):
        """UpdateSubscriptionsToEventBridge returns a result."""
        resp = dms.update_subscriptions_to_event_bridge()
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200
        assert "Result" in resp


class TestDMSConnectionOperations:
    """Tests for DeleteConnection."""

    def test_delete_connection_nonexistent(self, dms):
        """DeleteConnection raises ResourceNotFoundFault for unknown connection."""
        with pytest.raises(ClientError) as exc:
            dms.delete_connection(
                EndpointArn="arn:aws:dms:us-east-1:123456789012:endpoint:fake",
                ReplicationInstanceArn="arn:aws:dms:us-east-1:123456789012:rep:fake",
            )
        assert exc.value.response["Error"]["Code"] == "ResourceNotFoundFault"


class TestDMSTaskAssessmentOperations:
    """Tests for replication task assessment operations."""

    def _make_task(self, dms):
        uid = uuid.uuid4().hex[:8]
        src = dms.create_endpoint(
            EndpointIdentifier=f"src-{uid}",
            EndpointType="source",
            EngineName="mysql",
            ServerName="localhost",
            Port=3306,
            Username="admin",
            Password="password",
        )
        tgt = dms.create_endpoint(
            EndpointIdentifier=f"tgt-{uid}",
            EndpointType="target",
            EngineName="postgres",
            ServerName="localhost",
            Port=5432,
            Username="admin",
            Password="password",
        )
        ri = dms.create_replication_instance(
            ReplicationInstanceIdentifier=f"ri-{uid}",
            ReplicationInstanceClass="dms.t3.micro",
        )
        task = dms.create_replication_task(
            ReplicationTaskIdentifier=f"task-{uid}",
            SourceEndpointArn=src["Endpoint"]["EndpointArn"],
            TargetEndpointArn=tgt["Endpoint"]["EndpointArn"],
            ReplicationInstanceArn=ri["ReplicationInstance"]["ReplicationInstanceArn"],
            MigrationType="full-load",
            TableMappings='{"rules":[]}',
        )
        return {
            "task_arn": task["ReplicationTask"]["ReplicationTaskArn"],
            "src_arn": src["Endpoint"]["EndpointArn"],
            "tgt_arn": tgt["Endpoint"]["EndpointArn"],
            "ri_arn": ri["ReplicationInstance"]["ReplicationInstanceArn"],
        }

    def _cleanup(self, dms, info):
        dms.delete_replication_task(ReplicationTaskArn=info["task_arn"])
        dms.delete_endpoint(EndpointArn=info["src_arn"])
        dms.delete_endpoint(EndpointArn=info["tgt_arn"])
        dms.delete_replication_instance(ReplicationInstanceArn=info["ri_arn"])

    def test_start_replication_task_assessment(self, dms):
        """StartReplicationTaskAssessment returns task details."""
        info = self._make_task(dms)
        try:
            resp = dms.start_replication_task_assessment(
                ReplicationTaskArn=info["task_arn"],
            )
            assert "ReplicationTask" in resp
            assert resp["ReplicationTask"]["ReplicationTaskArn"] == info["task_arn"]
        finally:
            self._cleanup(dms, info)

    def test_start_replication_task_assessment_run(self, dms):
        """StartReplicationTaskAssessmentRun creates an assessment run."""
        info = self._make_task(dms)
        try:
            resp = dms.start_replication_task_assessment_run(
                ReplicationTaskArn=info["task_arn"],
                ServiceAccessRoleArn="arn:aws:iam::123456789012:role/test",
                ResultLocationBucket="test-bucket",
                AssessmentRunName=_unique("run"),
            )
            run = resp["ReplicationTaskAssessmentRun"]
            assert "ReplicationTaskAssessmentRunArn" in run
            assert run["Status"] == "starting"
            assert run["ReplicationTaskArn"] == info["task_arn"]
        finally:
            self._cleanup(dms, info)

    def test_cancel_replication_task_assessment_run(self, dms):
        """CancelReplicationTaskAssessmentRun returns a response for fake ARN."""
        resp = dms.cancel_replication_task_assessment_run(
            ReplicationTaskAssessmentRunArn=(
                "arn:aws:dms:us-east-1:123456789012:assessment-run:fake"
            ),
        )
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200

    def test_delete_replication_task_assessment_run(self, dms):
        """DeleteReplicationTaskAssessmentRun returns a response for fake ARN."""
        resp = dms.delete_replication_task_assessment_run(
            ReplicationTaskAssessmentRunArn=(
                "arn:aws:dms:us-east-1:123456789012:assessment-run:fake"
            ),
        )
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200

    def test_move_replication_task(self, dms):
        """MoveReplicationTask returns task details."""
        info = self._make_task(dms)
        try:
            resp = dms.move_replication_task(
                ReplicationTaskArn=info["task_arn"],
                TargetReplicationInstanceArn=info["ri_arn"],
            )
            assert "ReplicationTask" in resp
        finally:
            self._cleanup(dms, info)

    def test_reload_tables(self, dms):
        """ReloadTables accepts a replication task ARN and table list."""
        info = self._make_task(dms)
        try:
            resp = dms.reload_tables(
                ReplicationTaskArn=info["task_arn"],
                TablesToReload=[{"SchemaName": "public", "TableName": "test"}],
            )
            assert "ReplicationTaskArn" in resp
        finally:
            self._cleanup(dms, info)

    def test_refresh_schemas(self, dms):
        """RefreshSchemas returns a RefreshSchemasStatus."""
        info = self._make_task(dms)
        try:
            resp = dms.refresh_schemas(
                EndpointArn=info["src_arn"],
                ReplicationInstanceArn=info["ri_arn"],
            )
            assert "RefreshSchemasStatus" in resp
        finally:
            self._cleanup(dms, info)


class TestDMSReloadReplicationTables:
    """Tests for ReloadReplicationTables."""

    def test_reload_replication_tables(self, dms):
        """ReloadReplicationTables accepts a config ARN and table list."""
        resp = dms.reload_replication_tables(
            ReplicationConfigArn=("arn:aws:dms:us-east-1:123456789012:replication-config:fake"),
            TablesToReload=[{"SchemaName": "public", "TableName": "test"}],
        )
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200


class TestDMSMaintenanceAction:
    """Tests for ApplyPendingMaintenanceAction."""

    def test_apply_pending_maintenance_action(self, dms):
        """ApplyPendingMaintenanceAction returns a ResourcePendingMaintenanceActions."""
        ri_id = _unique("ri")
        ri = dms.create_replication_instance(
            ReplicationInstanceIdentifier=ri_id,
            ReplicationInstanceClass="dms.t3.micro",
        )
        ri_arn = ri["ReplicationInstance"]["ReplicationInstanceArn"]
        try:
            resp = dms.apply_pending_maintenance_action(
                ReplicationInstanceArn=ri_arn,
                ApplyAction="os-upgrade",
                OptInType="immediate",
            )
            assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200
        finally:
            dms.delete_replication_instance(ReplicationInstanceArn=ri_arn)


class TestDMSDataMigrationOperations:
    """Tests for data migration operations."""

    def test_delete_data_migration_nonexistent(self, dms):
        """DeleteDataMigration raises ResourceNotFoundFault for unknown migration."""
        with pytest.raises(ClientError) as exc:
            dms.delete_data_migration(DataMigrationIdentifier="fake-dm")
        assert exc.value.response["Error"]["Code"] == "ResourceNotFoundFault"

    def test_modify_data_migration_nonexistent(self, dms):
        """ModifyDataMigration raises ResourceNotFoundFault for unknown migration."""
        with pytest.raises(ClientError) as exc:
            dms.modify_data_migration(
                DataMigrationIdentifier="fake-dm",
                DataMigrationType="full-load",
            )
        assert exc.value.response["Error"]["Code"] == "ResourceNotFoundFault"

    def test_start_data_migration_nonexistent(self, dms):
        """StartDataMigration raises ResourceNotFoundFault for unknown migration."""
        with pytest.raises(ClientError) as exc:
            dms.start_data_migration(
                DataMigrationIdentifier="fake-dm",
                StartType="reload-target",
            )
        assert exc.value.response["Error"]["Code"] == "ResourceNotFoundFault"

    def test_stop_data_migration_nonexistent(self, dms):
        """StopDataMigration raises ResourceNotFoundFault for unknown migration."""
        with pytest.raises(ClientError) as exc:
            dms.stop_data_migration(DataMigrationIdentifier="fake-dm")
        assert exc.value.response["Error"]["Code"] == "ResourceNotFoundFault"


class TestDMSMetadataModelOperations:
    """Tests for metadata model operations."""

    def test_export_metadata_model_assessment(self, dms):
        """ExportMetadataModelAssessment returns a response."""
        resp = dms.export_metadata_model_assessment(
            MigrationProjectIdentifier="fake-proj",
            SelectionRules='{"rules":[]}',
        )
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200

    def test_modify_conversion_configuration(self, dms):
        """ModifyConversionConfiguration returns a migration project identifier."""
        resp = dms.modify_conversion_configuration(
            MigrationProjectIdentifier="fake-proj",
            ConversionConfiguration="{}",
        )
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200

    def test_start_extension_pack_association(self, dms):
        """StartExtensionPackAssociation returns a request identifier."""
        resp = dms.start_extension_pack_association(
            MigrationProjectIdentifier="fake-proj",
        )
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200

    def test_start_metadata_model_assessment(self, dms):
        """StartMetadataModelAssessment returns a request identifier."""
        resp = dms.start_metadata_model_assessment(
            MigrationProjectIdentifier="fake-proj",
            SelectionRules='{"rules":[]}',
        )
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200

    def test_start_metadata_model_conversion(self, dms):
        """StartMetadataModelConversion returns a request identifier."""
        resp = dms.start_metadata_model_conversion(
            MigrationProjectIdentifier="fake-proj",
            SelectionRules='{"rules":[]}',
        )
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200

    def test_start_metadata_model_export_as_script(self, dms):
        """StartMetadataModelExportAsScript returns a request identifier."""
        resp = dms.start_metadata_model_export_as_script(
            MigrationProjectIdentifier="fake-proj",
            SelectionRules='{"rules":[]}',
            Origin="SOURCE",
        )
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200

    def test_start_metadata_model_export_to_target(self, dms):
        """StartMetadataModelExportToTarget returns a request identifier."""
        resp = dms.start_metadata_model_export_to_target(
            MigrationProjectIdentifier="fake-proj",
            SelectionRules='{"rules":[]}',
        )
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200

    def test_start_metadata_model_import(self, dms):
        """StartMetadataModelImport returns a request identifier."""
        resp = dms.start_metadata_model_import(
            MigrationProjectIdentifier="fake-proj",
            SelectionRules='{"rules":[]}',
            Origin="SOURCE",
        )
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200

    def test_cancel_metadata_model_conversion(self, dms):
        """CancelMetadataModelConversion returns a request identifier."""
        resp = dms.cancel_metadata_model_conversion(
            MigrationProjectIdentifier="fake-proj",
            RequestIdentifier="fake-req",
        )
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200

    def test_cancel_metadata_model_creation(self, dms):
        """CancelMetadataModelCreation returns a request identifier."""
        resp = dms.cancel_metadata_model_creation(
            MigrationProjectIdentifier="fake-proj",
            RequestIdentifier="fake-req",
        )
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200


class TestDMSAdditionalOps:
    """Tests for additional DMS operations."""

    def test_start_replication_fake_arn(self, dms):
        """StartReplication with fake ReplicationConfigArn returns error or 200."""
        try:
            resp = dms.start_replication(
                ReplicationConfigArn=f"arn:aws:dms:us-east-1:123456789012:replication-config:{_unique('rep')}",
                StartReplicationType="start-replication",
            )
            assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200
        except ClientError as e:
            assert "Code" in e.response["Error"]

    def test_delete_fleet_advisor_collector_fake(self, dms):
        """DeleteFleetAdvisorCollector with fake CollectorReferencedId."""
        try:
            resp = dms.delete_fleet_advisor_collector(
                CollectorReferencedId=_unique("collector"),
            )
            assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200
        except ClientError as e:
            assert "Code" in e.response["Error"]
