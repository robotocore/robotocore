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
