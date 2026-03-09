"""Redshift compatibility tests."""

import uuid

import pytest
from botocore.exceptions import ClientError

from tests.compatibility.conftest import make_client


def _uid():
    return uuid.uuid4().hex[:8]


@pytest.fixture
def redshift():
    return make_client("redshift")


class TestRedshiftOperations:
    def test_create_cluster(self, redshift):
        response = redshift.create_cluster(
            ClusterIdentifier="test-cluster",
            NodeType="dc2.large",
            MasterUsername="admin",
            MasterUserPassword="Password1!",
            NumberOfNodes=1,
            ClusterType="single-node",
        )
        cluster = response["Cluster"]
        assert cluster["ClusterIdentifier"] == "test-cluster"
        assert cluster["NodeType"] == "dc2.large"
        assert cluster["MasterUsername"] == "admin"

        # Cleanup
        redshift.delete_cluster(
            ClusterIdentifier="test-cluster",
            SkipFinalClusterSnapshot=True,
        )

    def test_describe_clusters(self, redshift):
        redshift.create_cluster(
            ClusterIdentifier="describe-cluster",
            NodeType="dc2.large",
            MasterUsername="admin",
            MasterUserPassword="Password1!",
            NumberOfNodes=1,
            ClusterType="single-node",
        )
        response = redshift.describe_clusters(ClusterIdentifier="describe-cluster")
        assert len(response["Clusters"]) == 1
        assert response["Clusters"][0]["ClusterIdentifier"] == "describe-cluster"

        # Cleanup
        redshift.delete_cluster(
            ClusterIdentifier="describe-cluster",
            SkipFinalClusterSnapshot=True,
        )

    def test_delete_cluster(self, redshift):
        redshift.create_cluster(
            ClusterIdentifier="delete-cluster",
            NodeType="dc2.large",
            MasterUsername="admin",
            MasterUserPassword="Password1!",
            NumberOfNodes=1,
            ClusterType="single-node",
        )
        response = redshift.delete_cluster(
            ClusterIdentifier="delete-cluster",
            SkipFinalClusterSnapshot=True,
        )
        assert response["Cluster"]["ClusterIdentifier"] == "delete-cluster"

    def test_create_cluster_snapshot(self, redshift):
        redshift.create_cluster(
            ClusterIdentifier="snapshot-cluster",
            NodeType="dc2.large",
            MasterUsername="admin",
            MasterUserPassword="Password1!",
            NumberOfNodes=1,
            ClusterType="single-node",
        )
        response = redshift.create_cluster_snapshot(
            SnapshotIdentifier="test-snapshot",
            ClusterIdentifier="snapshot-cluster",
        )
        assert response["Snapshot"]["SnapshotIdentifier"] == "test-snapshot"
        assert response["Snapshot"]["ClusterIdentifier"] == "snapshot-cluster"

        # Cleanup
        redshift.delete_cluster_snapshot(SnapshotIdentifier="test-snapshot")
        redshift.delete_cluster(
            ClusterIdentifier="snapshot-cluster",
            SkipFinalClusterSnapshot=True,
        )

    def test_create_cluster_parameter_group(self, redshift):
        response = redshift.create_cluster_parameter_group(
            ParameterGroupName="test-param-group",
            ParameterGroupFamily="redshift-1.0",
            Description="Test parameter group",
        )
        assert response["ClusterParameterGroup"]["ParameterGroupName"] == "test-param-group"

        # Verify it shows up in listing
        desc_response = redshift.describe_cluster_parameter_groups(
            ParameterGroupName="test-param-group"
        )
        assert len(desc_response["ParameterGroups"]) == 1

        # Cleanup
        redshift.delete_cluster_parameter_group(ParameterGroupName="test-param-group")

    def test_describe_clusters_empty(self, redshift):
        response = redshift.describe_clusters()
        assert "Clusters" in response

    def test_create_cluster_subnet_group(self, redshift):
        ec2 = make_client("ec2")
        vpc = ec2.create_vpc(CidrBlock="10.200.0.0/16")
        vpc_id = vpc["Vpc"]["VpcId"]
        subnet = ec2.create_subnet(VpcId=vpc_id, CidrBlock="10.200.1.0/24")
        subnet_id = subnet["Subnet"]["SubnetId"]
        name = f"test-sg-{_uid()}"
        response = redshift.create_cluster_subnet_group(
            ClusterSubnetGroupName=name,
            Description="Test subnet group",
            SubnetIds=[subnet_id],
        )
        assert response["ClusterSubnetGroup"]["ClusterSubnetGroupName"] == name
        redshift.delete_cluster_subnet_group(ClusterSubnetGroupName=name)

    def test_create_cluster_with_tags(self, redshift):
        cid = f"tagged-{_uid()}"
        response = redshift.create_cluster(
            ClusterIdentifier=cid,
            NodeType="dc2.large",
            MasterUsername="admin",
            MasterUserPassword="Password1!",
            NumberOfNodes=1,
            ClusterType="single-node",
            Tags=[{"Key": "env", "Value": "test"}],
        )
        tags = {t["Key"]: t["Value"] for t in response["Cluster"].get("Tags", [])}
        assert tags.get("env") == "test"
        redshift.delete_cluster(ClusterIdentifier=cid, SkipFinalClusterSnapshot=True)

    def test_describe_cluster_not_found(self, redshift):
        with pytest.raises(ClientError) as exc:
            redshift.describe_clusters(ClusterIdentifier="nonexistent-cluster-xyz")
        assert "ClusterNotFound" in exc.value.response["Error"]["Code"]

    def test_describe_cluster_snapshots(self, redshift):
        cid = f"snap-{_uid()}"
        redshift.create_cluster(
            ClusterIdentifier=cid,
            NodeType="dc2.large",
            MasterUsername="admin",
            MasterUserPassword="Password1!",
            NumberOfNodes=1,
            ClusterType="single-node",
        )
        snap_name = f"snap-{_uid()}"
        redshift.create_cluster_snapshot(SnapshotIdentifier=snap_name, ClusterIdentifier=cid)
        response = redshift.describe_cluster_snapshots(SnapshotIdentifier=snap_name)
        assert len(response["Snapshots"]) == 1
        assert response["Snapshots"][0]["SnapshotIdentifier"] == snap_name
        redshift.delete_cluster_snapshot(SnapshotIdentifier=snap_name)
        redshift.delete_cluster(ClusterIdentifier=cid, SkipFinalClusterSnapshot=True)

    def test_modify_cluster(self, redshift):
        cid = f"mod-{_uid()}"
        redshift.create_cluster(
            ClusterIdentifier=cid,
            NodeType="dc2.large",
            MasterUsername="admin",
            MasterUserPassword="Password1!",
            NumberOfNodes=1,
            ClusterType="single-node",
        )
        try:
            resp = redshift.modify_cluster(
                ClusterIdentifier=cid,
                AllowVersionUpgrade=False,
            )
            assert resp["Cluster"]["ClusterIdentifier"] == cid
        finally:
            redshift.delete_cluster(ClusterIdentifier=cid, SkipFinalClusterSnapshot=True)

    def test_describe_cluster_parameter_groups_all(self, redshift):
        resp = redshift.describe_cluster_parameter_groups()
        assert "ParameterGroups" in resp

    def test_describe_cluster_security_groups(self, redshift):
        resp = redshift.describe_cluster_security_groups()
        assert "ClusterSecurityGroups" in resp

    def test_list_cluster_tags(self, redshift):
        cid = f"tags-{_uid()}"
        create = redshift.create_cluster(
            ClusterIdentifier=cid,
            NodeType="dc2.large",
            MasterUsername="admin",
            MasterUserPassword="Password1!",
            NumberOfNodes=1,
            ClusterType="single-node",
            Tags=[{"Key": "env", "Value": "test"}],
        )
        _arn = (
            create["Cluster"]["ClusterNamespaceArn"]
            if "ClusterNamespaceArn" in create["Cluster"]
            else None
        )
        try:
            desc = redshift.describe_clusters(ClusterIdentifier=cid)
            tags = {t["Key"]: t["Value"] for t in desc["Clusters"][0].get("Tags", [])}
            assert tags.get("env") == "test"
        finally:
            redshift.delete_cluster(ClusterIdentifier=cid, SkipFinalClusterSnapshot=True)

    def test_create_delete_cluster_parameter_group(self, redshift):
        name = f"pg-{_uid()}"
        redshift.create_cluster_parameter_group(
            ParameterGroupName=name,
            ParameterGroupFamily="redshift-1.0",
            Description="temp group",
        )
        redshift.delete_cluster_parameter_group(ParameterGroupName=name)
        resp = redshift.describe_cluster_parameter_groups()
        names = [g["ParameterGroupName"] for g in resp["ParameterGroups"]]
        assert name not in names

    def test_describe_cluster_parameters(self, redshift):
        name = f"cpg-{_uid()}"
        redshift.create_cluster_parameter_group(
            ParameterGroupName=name,
            ParameterGroupFamily="redshift-1.0",
            Description="Params test",
        )
        try:
            resp = redshift.describe_cluster_parameters(ParameterGroupName=name)
            assert "Parameters" in resp
        finally:
            redshift.delete_cluster_parameter_group(ParameterGroupName=name)

    def test_describe_default_cluster_parameters(self, redshift):
        resp = redshift.describe_default_cluster_parameters(ParameterGroupFamily="redshift-1.0")
        assert "DefaultClusterParameters" in resp
        assert resp["DefaultClusterParameters"]["ParameterGroupFamily"] == "redshift-1.0"

    def test_describe_cluster_subnet_groups_all(self, redshift):
        resp = redshift.describe_cluster_subnet_groups()
        assert "ClusterSubnetGroups" in resp

    def test_create_and_describe_snapshot_copy_grant(self, redshift):
        name = f"scg-{_uid()}"
        resp = redshift.create_snapshot_copy_grant(SnapshotCopyGrantName=name)
        assert resp["SnapshotCopyGrant"]["SnapshotCopyGrantName"] == name
        try:
            desc = redshift.describe_snapshot_copy_grants(SnapshotCopyGrantName=name)
            assert len(desc["SnapshotCopyGrants"]) == 1
            assert desc["SnapshotCopyGrants"][0]["SnapshotCopyGrantName"] == name
        finally:
            redshift.delete_snapshot_copy_grant(SnapshotCopyGrantName=name)

    def test_describe_snapshot_copy_grants_empty(self, redshift):
        resp = redshift.describe_snapshot_copy_grants()
        assert "SnapshotCopyGrants" in resp

    def test_create_tags_and_describe_tags(self, redshift):
        name = f"tagpg-{_uid()}"
        redshift.create_cluster_parameter_group(
            ParameterGroupName=name,
            ParameterGroupFamily="redshift-1.0",
            Description="Tag test",
        )
        arn = f"arn:aws:redshift:us-east-1:123456789012:parametergroup:{name}"
        try:
            redshift.create_tags(
                ResourceName=arn,
                Tags=[{"Key": "team", "Value": "platform"}],
            )
            resp = redshift.describe_tags(ResourceName=arn)
            assert "TaggedResources" in resp
            tag_map = {t["Tag"]["Key"]: t["Tag"]["Value"] for t in resp["TaggedResources"]}
            assert tag_map.get("team") == "platform"
        finally:
            redshift.delete_cluster_parameter_group(ParameterGroupName=name)

    def test_delete_tags(self, redshift):
        name = f"deltag-{_uid()}"
        redshift.create_cluster_parameter_group(
            ParameterGroupName=name,
            ParameterGroupFamily="redshift-1.0",
            Description="Delete tag test",
        )
        arn = f"arn:aws:redshift:us-east-1:123456789012:parametergroup:{name}"
        try:
            redshift.create_tags(
                ResourceName=arn,
                Tags=[{"Key": "remove", "Value": "me"}],
            )
            redshift.delete_tags(ResourceName=arn, TagKeys=["remove"])
            resp = redshift.describe_tags(ResourceName=arn)
            keys = [t["Tag"]["Key"] for t in resp["TaggedResources"]]
            assert "remove" not in keys
        finally:
            redshift.delete_cluster_parameter_group(ParameterGroupName=name)

    def test_describe_tags_all(self, redshift):
        resp = redshift.describe_tags()
        assert "TaggedResources" in resp

    def test_describe_logging_status(self, redshift):
        cid = f"logst-{_uid()}"
        redshift.create_cluster(
            ClusterIdentifier=cid,
            NodeType="dc2.large",
            MasterUsername="admin",
            MasterUserPassword="Password1!",
            NumberOfNodes=1,
            ClusterType="single-node",
        )
        try:
            resp = redshift.describe_logging_status(ClusterIdentifier=cid)
            assert "LoggingEnabled" in resp
        finally:
            redshift.delete_cluster(ClusterIdentifier=cid, SkipFinalClusterSnapshot=True)

    def test_delete_cluster_subnet_group(self, redshift):
        ec2 = make_client("ec2")
        vpc = ec2.create_vpc(CidrBlock="10.201.0.0/16")
        vpc_id = vpc["Vpc"]["VpcId"]
        subnet = ec2.create_subnet(VpcId=vpc_id, CidrBlock="10.201.1.0/24")
        subnet_id = subnet["Subnet"]["SubnetId"]
        name = f"del-sg-{_uid()}"
        redshift.create_cluster_subnet_group(
            ClusterSubnetGroupName=name,
            Description="Delete subnet group test",
            SubnetIds=[subnet_id],
        )
        redshift.delete_cluster_subnet_group(ClusterSubnetGroupName=name)
        resp = redshift.describe_cluster_subnet_groups()
        names = [g["ClusterSubnetGroupName"] for g in resp["ClusterSubnetGroups"]]
        assert name not in names

    def test_delete_cluster_parameter_group_not_found(self, redshift):
        with pytest.raises(ClientError) as exc:
            redshift.delete_cluster_parameter_group(ParameterGroupName="nonexistent-pg-xyz")
        assert "ClusterParameterGroupNotFound" in exc.value.response["Error"]["Code"]


class TestRedshiftGapStubs:
    """Tests for newly-stubbed Redshift describe operations that return empty results."""

    def test_describe_snapshot_schedules(self, redshift):
        resp = redshift.describe_snapshot_schedules()
        assert "SnapshotSchedules" in resp
        assert isinstance(resp["SnapshotSchedules"], list)

    def test_describe_event_subscriptions(self, redshift):
        resp = redshift.describe_event_subscriptions()
        assert "EventSubscriptionsList" in resp
        assert isinstance(resp["EventSubscriptionsList"], list)

    def test_describe_data_shares(self, redshift):
        resp = redshift.describe_data_shares()
        assert "DataShares" in resp
        assert isinstance(resp["DataShares"], list)

    def test_describe_data_shares_for_consumer(self, redshift):
        resp = redshift.describe_data_shares_for_consumer()
        assert "DataShares" in resp
        assert isinstance(resp["DataShares"], list)

    def test_describe_data_shares_for_producer(self, redshift):
        resp = redshift.describe_data_shares_for_producer()
        assert "DataShares" in resp
        assert isinstance(resp["DataShares"], list)

    def test_describe_endpoint_access(self, redshift):
        resp = redshift.describe_endpoint_access()
        assert "EndpointAccessList" in resp
        assert isinstance(resp["EndpointAccessList"], list)

    def test_describe_endpoint_authorization(self, redshift):
        resp = redshift.describe_endpoint_authorization()
        assert "EndpointAuthorizationList" in resp
        assert isinstance(resp["EndpointAuthorizationList"], list)

    def test_describe_usage_limits(self, redshift):
        resp = redshift.describe_usage_limits()
        assert "UsageLimits" in resp
        assert isinstance(resp["UsageLimits"], list)

    def test_describe_hsm_client_certificates(self, redshift):
        resp = redshift.describe_hsm_client_certificates()
        assert "HsmClientCertificates" in resp
        assert isinstance(resp["HsmClientCertificates"], list)

    def test_describe_hsm_configurations(self, redshift):
        resp = redshift.describe_hsm_configurations()
        assert "HsmConfigurations" in resp
        assert isinstance(resp["HsmConfigurations"], list)

    def test_describe_cluster_db_revisions(self, redshift):
        resp = redshift.describe_cluster_db_revisions()
        assert "ClusterDbRevisions" in resp
        assert isinstance(resp["ClusterDbRevisions"], list)

    def test_describe_cluster_tracks(self, redshift):
        resp = redshift.describe_cluster_tracks()
        assert "MaintenanceTracks" in resp
        assert isinstance(resp["MaintenanceTracks"], list)

    def test_describe_events(self, redshift):
        resp = redshift.describe_events()
        assert "Events" in resp
        assert isinstance(resp["Events"], list)

    def test_describe_reserved_nodes(self, redshift):
        resp = redshift.describe_reserved_nodes()
        assert "ReservedNodes" in resp
        assert isinstance(resp["ReservedNodes"], list)

    def test_describe_reserved_node_offerings(self, redshift):
        resp = redshift.describe_reserved_node_offerings()
        assert "ReservedNodeOfferings" in resp
        assert isinstance(resp["ReservedNodeOfferings"], list)

    def test_describe_reserved_node_exchange_status(self, redshift):
        resp = redshift.describe_reserved_node_exchange_status()
        assert "ReservedNodeExchangeStatusDetails" in resp
        assert isinstance(resp["ReservedNodeExchangeStatusDetails"], list)

    def test_describe_table_restore_status(self, redshift):
        resp = redshift.describe_table_restore_status()
        assert "TableRestoreStatusDetails" in resp
        assert isinstance(resp["TableRestoreStatusDetails"], list)

    def test_describe_custom_domain_associations(self, redshift):
        resp = redshift.describe_custom_domain_associations()
        assert "Associations" in resp
        assert isinstance(resp["Associations"], list)

    def test_describe_inbound_integrations(self, redshift):
        resp = redshift.describe_inbound_integrations()
        assert "InboundIntegrations" in resp
        assert isinstance(resp["InboundIntegrations"], list)

    def test_describe_snapshot_copy_grants_empty(self, redshift):
        resp = redshift.describe_snapshot_copy_grants()
        assert "SnapshotCopyGrants" in resp
        assert isinstance(resp["SnapshotCopyGrants"], list)

    def test_describe_cluster_versions(self, redshift):
        resp = redshift.describe_cluster_versions()
        assert "ClusterVersions" in resp
        assert isinstance(resp["ClusterVersions"], list)

    def test_describe_orderable_cluster_options(self, redshift):
        resp = redshift.describe_orderable_cluster_options()
        assert "OrderableClusterOptions" in resp
        assert isinstance(resp["OrderableClusterOptions"], list)

    def test_describe_storage(self, redshift):
        resp = redshift.describe_storage()
        assert "TotalBackupSizeInMegaBytes" in resp
        assert "TotalProvisionedStorageInMegaBytes" in resp
        assert isinstance(resp["TotalBackupSizeInMegaBytes"], float)
        assert isinstance(resp["TotalProvisionedStorageInMegaBytes"], float)

    def test_describe_authentication_profiles(self, redshift):
        resp = redshift.describe_authentication_profiles()
        assert "AuthenticationProfiles" in resp
        assert isinstance(resp["AuthenticationProfiles"], list)

    def test_describe_redshift_idc_applications(self, redshift):
        resp = redshift.describe_redshift_idc_applications()
        assert "RedshiftIdcApplications" in resp
        assert isinstance(resp["RedshiftIdcApplications"], list)


class TestRedshiftAutoCoverage:
    """Auto-generated coverage tests for redshift."""

    @pytest.fixture
    def client(self):
        return make_client("redshift")

    def test_create_snapshot_schedule(self, client):
        """CreateSnapshotSchedule returns a response."""
        resp = client.create_snapshot_schedule()
        assert "ScheduleDefinitions" in resp

    def test_describe_account_attributes(self, client):
        """DescribeAccountAttributes returns a response."""
        resp = client.describe_account_attributes()
        assert "AccountAttributes" in resp

    def test_describe_event_categories(self, client):
        """DescribeEventCategories returns a response."""
        resp = client.describe_event_categories()
        assert "EventCategoriesMapList" in resp

    def test_describe_integrations(self, client):
        """DescribeIntegrations returns a response."""
        resp = client.describe_integrations()
        assert "Integrations" in resp

    def test_describe_scheduled_actions(self, client):
        """DescribeScheduledActions returns a response."""
        resp = client.describe_scheduled_actions()
        assert "ScheduledActions" in resp

    def test_get_cluster_credentials_with_iam(self, client):
        """GetClusterCredentialsWithIAM returns a response."""
        resp = client.get_cluster_credentials_with_iam()
        assert "DbUser" in resp

    def test_list_recommendations(self, client):
        """ListRecommendations returns a response."""
        resp = client.list_recommendations()
        assert "Recommendations" in resp

    def test_revoke_endpoint_access(self, client):
        """RevokeEndpointAccess returns a response."""
        resp = client.revoke_endpoint_access()
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200

    def test_create_cluster_security_group(self, client):
        """CreateClusterSecurityGroup creates and returns the group."""
        name = f"csg-{_uid()}"
        try:
            resp = client.create_cluster_security_group(
                ClusterSecurityGroupName=name,
                Description="Test security group",
            )
            assert resp["ClusterSecurityGroup"]["ClusterSecurityGroupName"] == name
            assert resp["ClusterSecurityGroup"]["Description"] == "Test security group"
        finally:
            client.delete_cluster_security_group(ClusterSecurityGroupName=name)

    def test_delete_cluster_security_group(self, client):
        """DeleteClusterSecurityGroup removes the group."""
        name = f"csg-del-{_uid()}"
        client.create_cluster_security_group(
            ClusterSecurityGroupName=name,
            Description="To be deleted",
        )
        client.delete_cluster_security_group(ClusterSecurityGroupName=name)
        resp = client.describe_cluster_security_groups()
        names = [g["ClusterSecurityGroupName"] for g in resp["ClusterSecurityGroups"]]
        assert name not in names


class TestRedshiftClusterLifecycle:
    """Tests for cluster lifecycle operations: pause, resume, credentials, snapshots."""

    def test_get_cluster_credentials(self, redshift):
        cid = f"cred-{_uid()}"
        redshift.create_cluster(
            ClusterIdentifier=cid,
            NodeType="dc2.large",
            MasterUsername="admin",
            MasterUserPassword="Password1!",
            NumberOfNodes=1,
            ClusterType="single-node",
        )
        try:
            resp = redshift.get_cluster_credentials(
                DbUser="admin",
                ClusterIdentifier=cid,
            )
            assert "DbUser" in resp
            assert "DbPassword" in resp
            assert "Expiration" in resp
        finally:
            redshift.delete_cluster(ClusterIdentifier=cid, SkipFinalClusterSnapshot=True)

    def test_pause_cluster(self, redshift):
        cid = f"pause-{_uid()}"
        redshift.create_cluster(
            ClusterIdentifier=cid,
            NodeType="dc2.large",
            MasterUsername="admin",
            MasterUserPassword="Password1!",
            NumberOfNodes=1,
            ClusterType="single-node",
        )
        try:
            resp = redshift.pause_cluster(ClusterIdentifier=cid)
            assert resp["Cluster"]["ClusterIdentifier"] == cid
        finally:
            redshift.delete_cluster(ClusterIdentifier=cid, SkipFinalClusterSnapshot=True)

    def test_resume_cluster(self, redshift):
        cid = f"resume-{_uid()}"
        redshift.create_cluster(
            ClusterIdentifier=cid,
            NodeType="dc2.large",
            MasterUsername="admin",
            MasterUserPassword="Password1!",
            NumberOfNodes=1,
            ClusterType="single-node",
        )
        try:
            redshift.pause_cluster(ClusterIdentifier=cid)
            resp = redshift.resume_cluster(ClusterIdentifier=cid)
            assert resp["Cluster"]["ClusterIdentifier"] == cid
        finally:
            redshift.delete_cluster(ClusterIdentifier=cid, SkipFinalClusterSnapshot=True)

    def test_enable_and_disable_snapshot_copy(self, redshift):
        cid = f"snapcp-{_uid()}"
        redshift.create_cluster(
            ClusterIdentifier=cid,
            NodeType="dc2.large",
            MasterUsername="admin",
            MasterUserPassword="Password1!",
            NumberOfNodes=1,
            ClusterType="single-node",
        )
        try:
            resp = redshift.enable_snapshot_copy(
                ClusterIdentifier=cid,
                DestinationRegion="us-west-2",
            )
            assert resp["Cluster"]["ClusterIdentifier"] == cid

            resp2 = redshift.disable_snapshot_copy(ClusterIdentifier=cid)
            assert resp2["Cluster"]["ClusterIdentifier"] == cid
        finally:
            redshift.delete_cluster(ClusterIdentifier=cid, SkipFinalClusterSnapshot=True)

    def test_restore_from_cluster_snapshot(self, redshift):
        src = f"rsrc-{_uid()}"
        snap = f"rsnap-{_uid()}"
        restored = f"rest-{_uid()}"
        redshift.create_cluster(
            ClusterIdentifier=src,
            NodeType="dc2.large",
            MasterUsername="admin",
            MasterUserPassword="Password1!",
            NumberOfNodes=1,
            ClusterType="single-node",
        )
        try:
            redshift.create_cluster_snapshot(
                SnapshotIdentifier=snap,
                ClusterIdentifier=src,
            )
            resp = redshift.restore_from_cluster_snapshot(
                ClusterIdentifier=restored,
                SnapshotIdentifier=snap,
            )
            assert resp["Cluster"]["ClusterIdentifier"] == restored
            assert resp["Cluster"]["NodeType"] == "dc2.large"
            redshift.delete_cluster(ClusterIdentifier=restored, SkipFinalClusterSnapshot=True)
        finally:
            redshift.delete_cluster_snapshot(SnapshotIdentifier=snap)
            redshift.delete_cluster(ClusterIdentifier=src, SkipFinalClusterSnapshot=True)

    def test_authorize_cluster_security_group_ingress(self, redshift):
        name = f"auth-sg-{_uid()}"
        redshift.create_cluster_security_group(
            ClusterSecurityGroupName=name,
            Description="Auth ingress test",
        )
        try:
            resp = redshift.authorize_cluster_security_group_ingress(
                ClusterSecurityGroupName=name,
                CIDRIP="10.0.0.0/24",
            )
            sg = resp["ClusterSecurityGroup"]
            assert sg["ClusterSecurityGroupName"] == name
            cidrs = [r.get("CIDRIP") for r in sg.get("IPRanges", [])]
            assert "10.0.0.0/24" in cidrs
        finally:
            redshift.delete_cluster_security_group(ClusterSecurityGroupName=name)

    def test_create_snapshot_schedule_with_definition(self, redshift):
        sid = f"sched-{_uid()}"
        resp = redshift.create_snapshot_schedule(
            ScheduleIdentifier=sid,
            ScheduleDefinitions=["rate(12 hours)"],
        )
        assert resp["ScheduleIdentifier"] == sid
        assert "rate(12 hours)" in resp["ScheduleDefinitions"]

        # Verify it shows up in list
        desc = redshift.describe_snapshot_schedules(ScheduleIdentifier=sid)
        assert len(desc["SnapshotSchedules"]) == 1
        assert desc["SnapshotSchedules"][0]["ScheduleIdentifier"] == sid

    def test_enable_and_disable_logging(self, redshift):
        cid = f"log-{_uid()}"
        redshift.create_cluster(
            ClusterIdentifier=cid,
            NodeType="dc2.large",
            MasterUsername="admin",
            MasterUserPassword="Password1!",
            NumberOfNodes=1,
            ClusterType="single-node",
        )
        try:
            resp = redshift.enable_logging(
                ClusterIdentifier=cid,
                LogDestinationType="cloudwatch",
                LogExports=["connectionlog", "userlog"],
            )
            assert resp["LoggingEnabled"] is True

            resp2 = redshift.disable_logging(ClusterIdentifier=cid)
            assert resp2["LoggingEnabled"] is False
        finally:
            redshift.delete_cluster(ClusterIdentifier=cid, SkipFinalClusterSnapshot=True)

    def test_create_tags_on_cluster(self, redshift):
        cid = f"ctag-{_uid()}"
        redshift.create_cluster(
            ClusterIdentifier=cid,
            NodeType="dc2.large",
            MasterUsername="admin",
            MasterUserPassword="Password1!",
            NumberOfNodes=1,
            ClusterType="single-node",
        )
        arn = f"arn:aws:redshift:us-east-1:123456789012:cluster:{cid}"
        try:
            redshift.create_tags(
                ResourceName=arn,
                Tags=[{"Key": "k1", "Value": "v1"}, {"Key": "k2", "Value": "v2"}],
            )
            resp = redshift.describe_tags(ResourceName=arn)
            tag_map = {t["Tag"]["Key"]: t["Tag"]["Value"] for t in resp["TaggedResources"]}
            assert tag_map["k1"] == "v1"
            assert tag_map["k2"] == "v2"
        finally:
            redshift.delete_cluster(ClusterIdentifier=cid, SkipFinalClusterSnapshot=True)

    def test_delete_tags_on_cluster(self, redshift):
        cid = f"dtag-{_uid()}"
        redshift.create_cluster(
            ClusterIdentifier=cid,
            NodeType="dc2.large",
            MasterUsername="admin",
            MasterUserPassword="Password1!",
            NumberOfNodes=1,
            ClusterType="single-node",
        )
        arn = f"arn:aws:redshift:us-east-1:123456789012:cluster:{cid}"
        try:
            redshift.create_tags(
                ResourceName=arn,
                Tags=[{"Key": "del1", "Value": "a"}, {"Key": "keep1", "Value": "b"}],
            )
            redshift.delete_tags(ResourceName=arn, TagKeys=["del1"])
            resp = redshift.describe_tags(ResourceName=arn)
            keys = [t["Tag"]["Key"] for t in resp["TaggedResources"]]
            assert "del1" not in keys
            assert "keep1" in keys
        finally:
            redshift.delete_cluster(ClusterIdentifier=cid, SkipFinalClusterSnapshot=True)

    def test_describe_events_with_source_type(self, redshift):
        resp = redshift.describe_events(SourceType="cluster")
        assert "Events" in resp
        assert isinstance(resp["Events"], list)

    def test_describe_cluster_snapshots_all(self, redshift):
        resp = redshift.describe_cluster_snapshots()
        assert "Snapshots" in resp
        assert isinstance(resp["Snapshots"], list)

    def test_describe_cluster_security_groups_by_name(self, redshift):
        name = f"csg-fn-{_uid()}"
        redshift.create_cluster_security_group(
            ClusterSecurityGroupName=name,
            Description="Filter test",
        )
        try:
            resp = redshift.describe_cluster_security_groups(
                ClusterSecurityGroupName=name,
            )
            assert len(resp["ClusterSecurityGroups"]) == 1
            assert resp["ClusterSecurityGroups"][0]["ClusterSecurityGroupName"] == name
        finally:
            redshift.delete_cluster_security_group(ClusterSecurityGroupName=name)

    def test_describe_cluster_subnet_groups_by_name(self, redshift):
        ec2 = make_client("ec2")
        vpc = ec2.create_vpc(CidrBlock="10.220.0.0/16")
        vpc_id = vpc["Vpc"]["VpcId"]
        subnet = ec2.create_subnet(VpcId=vpc_id, CidrBlock="10.220.1.0/24")
        subnet_id = subnet["Subnet"]["SubnetId"]
        name = f"sng-fn-{_uid()}"
        redshift.create_cluster_subnet_group(
            ClusterSubnetGroupName=name,
            Description="Filter test",
            SubnetIds=[subnet_id],
        )
        try:
            resp = redshift.describe_cluster_subnet_groups(
                ClusterSubnetGroupName=name,
            )
            assert len(resp["ClusterSubnetGroups"]) == 1
            assert resp["ClusterSubnetGroups"][0]["ClusterSubnetGroupName"] == name
        finally:
            redshift.delete_cluster_subnet_group(ClusterSubnetGroupName=name)

    def test_describe_cluster_parameters_by_group(self, redshift):
        name = f"cpg-fn-{_uid()}"
        redshift.create_cluster_parameter_group(
            ParameterGroupName=name,
            ParameterGroupFamily="redshift-1.0",
            Description="Filter test",
        )
        try:
            resp = redshift.describe_cluster_parameters(ParameterGroupName=name)
            assert "Parameters" in resp
            assert isinstance(resp["Parameters"], list)
        finally:
            redshift.delete_cluster_parameter_group(ParameterGroupName=name)

    def test_describe_snapshot_schedules_by_id(self, redshift):
        sid = f"ss-fn-{_uid()}"
        redshift.create_snapshot_schedule(
            ScheduleIdentifier=sid,
            ScheduleDefinitions=["rate(12 hours)"],
        )
        try:
            resp = redshift.describe_snapshot_schedules(ScheduleIdentifier=sid)
            assert len(resp["SnapshotSchedules"]) == 1
            assert resp["SnapshotSchedules"][0]["ScheduleIdentifier"] == sid
        finally:
            pass  # no delete_snapshot_schedule available

    def test_describe_snapshot_copy_grants_by_name(self, redshift):
        name = f"scg-fn-{_uid()}"
        redshift.create_snapshot_copy_grant(SnapshotCopyGrantName=name)
        try:
            resp = redshift.describe_snapshot_copy_grants(SnapshotCopyGrantName=name)
            assert len(resp["SnapshotCopyGrants"]) == 1
            assert resp["SnapshotCopyGrants"][0]["SnapshotCopyGrantName"] == name
        finally:
            redshift.delete_snapshot_copy_grant(SnapshotCopyGrantName=name)

    def test_describe_tags_by_resource_type(self, redshift):
        """DescribeTags with ResourceType filter requires resources to exist."""
        name = f"tagrt-{_uid()}"
        redshift.create_cluster_parameter_group(
            ParameterGroupName=name,
            ParameterGroupFamily="redshift-1.0",
            Description="Tag resource type test",
        )
        arn = f"arn:aws:redshift:us-east-1:123456789012:parametergroup:{name}"
        try:
            redshift.create_tags(
                ResourceName=arn,
                Tags=[{"Key": "rtype", "Value": "test"}],
            )
            resp = redshift.describe_tags(ResourceType="parametergroup")
            assert "TaggedResources" in resp
            assert any(t["Tag"]["Key"] == "rtype" for t in resp["TaggedResources"])
        finally:
            redshift.delete_cluster_parameter_group(ParameterGroupName=name)


class TestRedshiftExpandedCoverage:
    """Expanded CRUD tests for Redshift operations."""

    def test_delete_cluster_snapshot(self, redshift):
        """DeleteClusterSnapshot removes a snapshot."""
        cid = f"dsnap-{_uid()}"
        snap = f"dsnap-{_uid()}"
        redshift.create_cluster(
            ClusterIdentifier=cid,
            NodeType="dc2.large",
            MasterUsername="admin",
            MasterUserPassword="Password1!",
            NumberOfNodes=1,
            ClusterType="single-node",
        )
        try:
            redshift.create_cluster_snapshot(SnapshotIdentifier=snap, ClusterIdentifier=cid)
            resp = redshift.delete_cluster_snapshot(SnapshotIdentifier=snap)
            assert resp["Snapshot"]["SnapshotIdentifier"] == snap
        finally:
            redshift.delete_cluster(ClusterIdentifier=cid, SkipFinalClusterSnapshot=True)

    def test_describe_cluster_snapshots_by_cluster(self, redshift):
        """DescribeClusterSnapshots filtered by ClusterIdentifier."""
        cid = f"sncl-{_uid()}"
        snap = f"sncl-{_uid()}"
        redshift.create_cluster(
            ClusterIdentifier=cid,
            NodeType="dc2.large",
            MasterUsername="admin",
            MasterUserPassword="Password1!",
            NumberOfNodes=1,
            ClusterType="single-node",
        )
        try:
            redshift.create_cluster_snapshot(SnapshotIdentifier=snap, ClusterIdentifier=cid)
            resp = redshift.describe_cluster_snapshots(ClusterIdentifier=cid)
            assert len(resp["Snapshots"]) >= 1
            assert any(s["SnapshotIdentifier"] == snap for s in resp["Snapshots"])
            redshift.delete_cluster_snapshot(SnapshotIdentifier=snap)
        finally:
            redshift.delete_cluster(ClusterIdentifier=cid, SkipFinalClusterSnapshot=True)

    def test_describe_cluster_snapshots_by_type_manual(self, redshift):
        """DescribeClusterSnapshots filtered by SnapshotType manual."""
        resp = redshift.describe_cluster_snapshots(SnapshotType="manual")
        assert "Snapshots" in resp
        assert isinstance(resp["Snapshots"], list)

    def test_describe_cluster_snapshot_not_found(self, redshift):
        """DescribeClusterSnapshots raises error for nonexistent snapshot."""
        with pytest.raises(ClientError) as exc:
            redshift.describe_cluster_snapshots(SnapshotIdentifier=f"nonexistent-snap-{_uid()}")
        assert "ClusterSnapshotNotFound" in exc.value.response["Error"]["Code"]

    def test_delete_cluster_snapshot_not_found(self, redshift):
        """DeleteClusterSnapshot raises error for nonexistent snapshot."""
        with pytest.raises(ClientError) as exc:
            redshift.delete_cluster_snapshot(SnapshotIdentifier=f"nonexistent-snap-{_uid()}")
        assert "ClusterSnapshotNotFound" in exc.value.response["Error"]["Code"]

    def test_delete_snapshot_copy_grant(self, redshift):
        """DeleteSnapshotCopyGrant removes the grant."""
        name = f"scg-d-{_uid()}"
        redshift.create_snapshot_copy_grant(SnapshotCopyGrantName=name)
        redshift.delete_snapshot_copy_grant(SnapshotCopyGrantName=name)
        resp = redshift.describe_snapshot_copy_grants()
        names = [g["SnapshotCopyGrantName"] for g in resp["SnapshotCopyGrants"]]
        assert name not in names

    def test_delete_snapshot_copy_grant_not_found(self, redshift):
        """DeleteSnapshotCopyGrant raises error for nonexistent grant."""
        with pytest.raises(ClientError) as exc:
            redshift.delete_snapshot_copy_grant(SnapshotCopyGrantName=f"nonexistent-scg-{_uid()}")
        assert "SnapshotCopyGrantNotFoundFault" in exc.value.response["Error"]["Code"]

    def test_delete_cluster_security_group_not_found(self, redshift):
        """DeleteClusterSecurityGroup raises error for nonexistent group."""
        with pytest.raises(ClientError) as exc:
            redshift.delete_cluster_security_group(
                ClusterSecurityGroupName=f"nonexistent-csg-{_uid()}"
            )
        assert "ClusterSecurityGroupNotFound" in exc.value.response["Error"]["Code"]

    def test_describe_cluster_security_groups_not_found(self, redshift):
        """DescribeClusterSecurityGroups raises error for nonexistent group."""
        with pytest.raises(ClientError) as exc:
            redshift.describe_cluster_security_groups(
                ClusterSecurityGroupName=f"nonexistent-csg-{_uid()}"
            )
        assert "ClusterSecurityGroupNotFound" in exc.value.response["Error"]["Code"]

    def test_enable_logging_s3_and_describe_and_disable(self, redshift):
        """EnableLogging with S3, DescribeLoggingStatus, DisableLogging cycle."""
        cid = f"logs3-{_uid()}"
        bucket = f"rs-log-{_uid()}"
        s3 = make_client("s3")
        s3.create_bucket(Bucket=bucket)
        redshift.create_cluster(
            ClusterIdentifier=cid,
            NodeType="dc2.large",
            MasterUsername="admin",
            MasterUserPassword="Password1!",
            NumberOfNodes=1,
            ClusterType="single-node",
        )
        try:
            resp = redshift.enable_logging(
                ClusterIdentifier=cid,
                BucketName=bucket,
                S3KeyPrefix="logs/",
            )
            assert resp["LoggingEnabled"] is True
            assert resp["BucketName"] == bucket

            status = redshift.describe_logging_status(ClusterIdentifier=cid)
            assert status["LoggingEnabled"] is True
            assert status["BucketName"] == bucket

            dis = redshift.disable_logging(ClusterIdentifier=cid)
            assert dis["LoggingEnabled"] is False
        finally:
            redshift.delete_cluster(ClusterIdentifier=cid, SkipFinalClusterSnapshot=True)

    def test_describe_events_with_source_type_parameter_group(self, redshift):
        """DescribeEvents filtered by SourceType cluster-parameter-group."""
        resp = redshift.describe_events(SourceType="cluster-parameter-group")
        assert "Events" in resp
        assert isinstance(resp["Events"], list)

    def test_modify_cluster_resize(self, redshift):
        """ModifyCluster to resize cluster from single to multi-node."""
        cid = f"modrs-{_uid()}"
        redshift.create_cluster(
            ClusterIdentifier=cid,
            NodeType="dc2.large",
            MasterUsername="admin",
            MasterUserPassword="Password1!",
            NumberOfNodes=1,
            ClusterType="single-node",
        )
        try:
            resp = redshift.modify_cluster(
                ClusterIdentifier=cid,
                NumberOfNodes=2,
                ClusterType="multi-node",
                NodeType="dc2.large",
            )
            assert resp["Cluster"]["ClusterIdentifier"] == cid
            assert resp["Cluster"]["NumberOfNodes"] == 2
        finally:
            redshift.delete_cluster(ClusterIdentifier=cid, SkipFinalClusterSnapshot=True)

    def test_modify_cluster_password(self, redshift):
        """ModifyCluster to change master password."""
        cid = f"modpw-{_uid()}"
        redshift.create_cluster(
            ClusterIdentifier=cid,
            NodeType="dc2.large",
            MasterUsername="admin",
            MasterUserPassword="Password1!",
            NumberOfNodes=1,
            ClusterType="single-node",
        )
        try:
            resp = redshift.modify_cluster(
                ClusterIdentifier=cid,
                MasterUserPassword="NewPassword2!",
            )
            assert resp["Cluster"]["ClusterIdentifier"] == cid
        finally:
            redshift.delete_cluster(ClusterIdentifier=cid, SkipFinalClusterSnapshot=True)

    def test_create_cluster_encrypted(self, redshift):
        """CreateCluster with Encrypted=True."""
        cid = f"encr-{_uid()}"
        try:
            resp = redshift.create_cluster(
                ClusterIdentifier=cid,
                NodeType="dc2.large",
                MasterUsername="admin",
                MasterUserPassword="Password1!",
                NumberOfNodes=1,
                ClusterType="single-node",
                Encrypted=True,
            )
            assert resp["Cluster"]["ClusterIdentifier"] == cid
            assert resp["Cluster"]["Encrypted"] is True
        finally:
            redshift.delete_cluster(ClusterIdentifier=cid, SkipFinalClusterSnapshot=True)

    def test_modify_snapshot_copy_retention_period(self, redshift):
        """ModifySnapshotCopyRetentionPeriod succeeds after EnableSnapshotCopy."""
        cid = f"mscr-{_uid()}"
        redshift.create_cluster(
            ClusterIdentifier=cid,
            NodeType="dc2.large",
            MasterUsername="admin",
            MasterUserPassword="Password1!",
            NumberOfNodes=1,
            ClusterType="single-node",
        )
        try:
            redshift.enable_snapshot_copy(
                ClusterIdentifier=cid,
                DestinationRegion="us-west-2",
            )
            resp = redshift.modify_snapshot_copy_retention_period(
                ClusterIdentifier=cid, RetentionPeriod=14
            )
            assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200
            redshift.disable_snapshot_copy(ClusterIdentifier=cid)
        finally:
            redshift.delete_cluster(ClusterIdentifier=cid, SkipFinalClusterSnapshot=True)

    def test_create_tags_on_subnet_group(self, redshift):
        """CreateTags on a ClusterSubnetGroup resource."""
        ec2 = make_client("ec2")
        vpc = ec2.create_vpc(CidrBlock="10.240.0.0/16")
        vpc_id = vpc["Vpc"]["VpcId"]
        subnet = ec2.create_subnet(VpcId=vpc_id, CidrBlock="10.240.1.0/24")
        subnet_id = subnet["Subnet"]["SubnetId"]
        name = f"sngtag-{_uid()}"
        redshift.create_cluster_subnet_group(
            ClusterSubnetGroupName=name,
            Description="Tags test",
            SubnetIds=[subnet_id],
        )
        arn = f"arn:aws:redshift:us-east-1:123456789012:subnetgroup:{name}"
        try:
            redshift.create_tags(
                ResourceName=arn,
                Tags=[{"Key": "env", "Value": "staging"}],
            )
            resp = redshift.describe_tags(ResourceName=arn)
            assert "TaggedResources" in resp
            tag_map = {t["Tag"]["Key"]: t["Tag"]["Value"] for t in resp["TaggedResources"]}
            assert tag_map.get("env") == "staging"
        finally:
            redshift.delete_cluster_subnet_group(ClusterSubnetGroupName=name)

    def test_delete_tags_from_security_group(self, redshift):
        """DeleteTags removes tags from a ClusterSecurityGroup."""
        name = f"sgdt-{_uid()}"
        redshift.create_cluster_security_group(
            ClusterSecurityGroupName=name,
            Description="Delete tags test",
        )
        arn = f"arn:aws:redshift:us-east-1:123456789012:securitygroup:{name}"
        try:
            redshift.create_tags(
                ResourceName=arn,
                Tags=[{"Key": "alpha", "Value": "1"}, {"Key": "beta", "Value": "2"}],
            )
            redshift.delete_tags(ResourceName=arn, TagKeys=["alpha"])
            resp = redshift.describe_tags(ResourceName=arn)
            keys = [t["Tag"]["Key"] for t in resp["TaggedResources"]]
            assert "alpha" not in keys
            assert "beta" in keys
        finally:
            redshift.delete_cluster_security_group(ClusterSecurityGroupName=name)

    def test_describe_cluster_snapshots_after_delete(self, redshift):
        """After deleting a snapshot, it no longer appears in describe."""
        cid = f"sdel-{_uid()}"
        snap = f"sdel-{_uid()}"
        redshift.create_cluster(
            ClusterIdentifier=cid,
            NodeType="dc2.large",
            MasterUsername="admin",
            MasterUserPassword="Password1!",
            NumberOfNodes=1,
            ClusterType="single-node",
        )
        try:
            redshift.create_cluster_snapshot(SnapshotIdentifier=snap, ClusterIdentifier=cid)
            redshift.delete_cluster_snapshot(SnapshotIdentifier=snap)
            with pytest.raises(ClientError) as exc:
                redshift.describe_cluster_snapshots(SnapshotIdentifier=snap)
            assert "ClusterSnapshotNotFound" in exc.value.response["Error"]["Code"]
        finally:
            redshift.delete_cluster(ClusterIdentifier=cid, SkipFinalClusterSnapshot=True)

    def test_snapshot_copy_grant_duplicate_error(self, redshift):
        """Creating a duplicate SnapshotCopyGrant raises an error."""
        name = f"scgdup-{_uid()}"
        redshift.create_snapshot_copy_grant(SnapshotCopyGrantName=name)
        try:
            with pytest.raises(ClientError) as exc:
                redshift.create_snapshot_copy_grant(SnapshotCopyGrantName=name)
            assert "already exists" in str(exc.value).lower() or "SnapshotCopyGrant" in str(
                exc.value
            )
        finally:
            redshift.delete_snapshot_copy_grant(SnapshotCopyGrantName=name)

    def test_describe_cluster_security_groups_all(self, redshift):
        """DescribeClusterSecurityGroups returns list with at least one group after creation."""
        name = f"csgall-{_uid()}"
        redshift.create_cluster_security_group(
            ClusterSecurityGroupName=name,
            Description="List test",
        )
        try:
            resp = redshift.describe_cluster_security_groups()
            assert "ClusterSecurityGroups" in resp
            names = [g["ClusterSecurityGroupName"] for g in resp["ClusterSecurityGroups"]]
            assert name in names
        finally:
            redshift.delete_cluster_security_group(ClusterSecurityGroupName=name)

    def test_create_cluster_duplicate_error(self, redshift):
        """Creating a duplicate Cluster raises an error."""
        cid = f"dupclst-{_uid()}"
        redshift.create_cluster(
            ClusterIdentifier=cid,
            NodeType="dc2.large",
            MasterUsername="admin",
            MasterUserPassword="Password1!",
            NumberOfNodes=1,
            ClusterType="single-node",
        )
        try:
            with pytest.raises(ClientError) as exc:
                redshift.create_cluster(
                    ClusterIdentifier=cid,
                    NodeType="dc2.large",
                    MasterUsername="admin",
                    MasterUserPassword="Password1!",
                    NumberOfNodes=1,
                    ClusterType="single-node",
                )
            assert "ClusterAlreadyExists" in exc.value.response["Error"]["Code"]
        finally:
            redshift.delete_cluster(ClusterIdentifier=cid, SkipFinalClusterSnapshot=True)

    def test_describe_snapshot_schedules_created(self, redshift):
        """DescribeSnapshotSchedules returns a created schedule."""
        sid = f"ssched-{_uid()}"
        redshift.create_snapshot_schedule(
            ScheduleIdentifier=sid,
            ScheduleDefinitions=["rate(12 hours)"],
        )
        resp = redshift.describe_snapshot_schedules(ScheduleIdentifier=sid)
        assert len(resp["SnapshotSchedules"]) == 1
        assert resp["SnapshotSchedules"][0]["ScheduleIdentifier"] == sid
        assert "rate(12 hours)" in resp["SnapshotSchedules"][0]["ScheduleDefinitions"]

    def test_create_snapshot_schedule_with_tags(self, redshift):
        """CreateSnapshotSchedule with tags."""
        sid = f"sstag-{_uid()}"
        resp = redshift.create_snapshot_schedule(
            ScheduleIdentifier=sid,
            ScheduleDefinitions=["rate(24 hours)"],
            Tags=[{"Key": "team", "Value": "data"}],
        )
        assert resp["ScheduleIdentifier"] == sid
        tags = {t["Key"]: t["Value"] for t in resp.get("Tags", [])}
        assert tags.get("team") == "data"

    def test_describe_cluster_parameter_group_not_found(self, redshift):
        """DescribeClusterParameterGroups raises error for nonexistent group."""
        with pytest.raises(ClientError) as exc:
            redshift.describe_cluster_parameter_groups(
                ParameterGroupName=f"nonexistent-pg-{_uid()}"
            )
        assert "ClusterParameterGroupNotFound" in exc.value.response["Error"]["Code"]

    def test_describe_cluster_subnet_group_not_found(self, redshift):
        """DescribeClusterSubnetGroups raises error for nonexistent group."""
        with pytest.raises(ClientError) as exc:
            redshift.describe_cluster_subnet_groups(
                ClusterSubnetGroupName=f"nonexistent-sng-{_uid()}"
            )
        assert "ClusterSubnetGroupNotFound" in exc.value.response["Error"]["Code"]

    def test_create_multiple_snapshots_same_cluster(self, redshift):
        """Multiple snapshots on same cluster, all visible via describe."""
        cid = f"msn-{_uid()}"
        snap1 = f"msn1-{_uid()}"
        snap2 = f"msn2-{_uid()}"
        redshift.create_cluster(
            ClusterIdentifier=cid,
            NodeType="dc2.large",
            MasterUsername="admin",
            MasterUserPassword="Password1!",
            NumberOfNodes=1,
            ClusterType="single-node",
        )
        try:
            redshift.create_cluster_snapshot(SnapshotIdentifier=snap1, ClusterIdentifier=cid)
            redshift.create_cluster_snapshot(SnapshotIdentifier=snap2, ClusterIdentifier=cid)
            resp = redshift.describe_cluster_snapshots(ClusterIdentifier=cid)
            snap_ids = [s["SnapshotIdentifier"] for s in resp["Snapshots"]]
            assert snap1 in snap_ids
            assert snap2 in snap_ids
            redshift.delete_cluster_snapshot(SnapshotIdentifier=snap1)
            redshift.delete_cluster_snapshot(SnapshotIdentifier=snap2)
        finally:
            redshift.delete_cluster(ClusterIdentifier=cid, SkipFinalClusterSnapshot=True)

    def test_create_and_delete_multiple_snapshot_copy_grants(self, redshift):
        """Create multiple SnapshotCopyGrants and verify all appear in describe."""
        name1 = f"scgm1-{_uid()}"
        name2 = f"scgm2-{_uid()}"
        redshift.create_snapshot_copy_grant(SnapshotCopyGrantName=name1)
        redshift.create_snapshot_copy_grant(SnapshotCopyGrantName=name2)
        try:
            resp = redshift.describe_snapshot_copy_grants()
            names = [g["SnapshotCopyGrantName"] for g in resp["SnapshotCopyGrants"]]
            assert name1 in names
            assert name2 in names
        finally:
            redshift.delete_snapshot_copy_grant(SnapshotCopyGrantName=name1)
            redshift.delete_snapshot_copy_grant(SnapshotCopyGrantName=name2)

    def test_create_cluster_security_group_with_tags(self, redshift):
        """CreateClusterSecurityGroup with Tags."""
        name = f"csgtg-{_uid()}"
        resp = redshift.create_cluster_security_group(
            ClusterSecurityGroupName=name,
            Description="Tags test",
            Tags=[{"Key": "dept", "Value": "eng"}],
        )
        assert resp["ClusterSecurityGroup"]["ClusterSecurityGroupName"] == name
        tags = {t["Key"]: t["Value"] for t in resp["ClusterSecurityGroup"].get("Tags", [])}
        assert tags.get("dept") == "eng"
        redshift.delete_cluster_security_group(ClusterSecurityGroupName=name)


class TestRedshiftAdditionalCoverage:
    """Additional tests for deeper Redshift coverage."""

    @pytest.fixture
    def redshift(self):
        return make_client("redshift")

    def test_get_cluster_credentials_with_iam_on_cluster(self, redshift):
        """GetClusterCredentialsWithIAM with a real cluster returns credentials."""
        cid = f"iamcred-{_uid()}"
        redshift.create_cluster(
            ClusterIdentifier=cid,
            NodeType="dc2.large",
            MasterUsername="admin",
            MasterUserPassword="Password1!",
            NumberOfNodes=1,
            ClusterType="single-node",
        )
        try:
            resp = redshift.get_cluster_credentials_with_iam(
                ClusterIdentifier=cid,
            )
            assert "DbUser" in resp
            assert "Expiration" in resp
        finally:
            redshift.delete_cluster(ClusterIdentifier=cid, SkipFinalClusterSnapshot=True)

    def test_describe_cluster_snapshots_after_create(self, redshift):
        """DescribeClusterSnapshots returns snapshots filtered by snapshot identifier."""
        cid = f"snapdsc-{_uid()}"
        snap_id = f"snap-{_uid()}"
        redshift.create_cluster(
            ClusterIdentifier=cid,
            NodeType="dc2.large",
            MasterUsername="admin",
            MasterUserPassword="Password1!",
            NumberOfNodes=1,
            ClusterType="single-node",
        )
        try:
            redshift.create_cluster_snapshot(
                SnapshotIdentifier=snap_id,
                ClusterIdentifier=cid,
            )
            resp = redshift.describe_cluster_snapshots(
                SnapshotIdentifier=snap_id,
            )
            assert len(resp["Snapshots"]) == 1
            assert resp["Snapshots"][0]["SnapshotIdentifier"] == snap_id
            assert resp["Snapshots"][0]["ClusterIdentifier"] == cid
            redshift.delete_cluster_snapshot(SnapshotIdentifier=snap_id)
        finally:
            redshift.delete_cluster(ClusterIdentifier=cid, SkipFinalClusterSnapshot=True)

    def test_describe_cluster_parameter_groups_by_name(self, redshift):
        """DescribeClusterParameterGroups filtered by name."""
        name = f"pg-{_uid()}"
        redshift.create_cluster_parameter_group(
            ParameterGroupName=name,
            ParameterGroupFamily="redshift-1.0",
            Description="test param group",
        )
        try:
            resp = redshift.describe_cluster_parameter_groups(
                ParameterGroupName=name,
            )
            groups = resp["ParameterGroups"]
            assert len(groups) == 1
            assert groups[0]["ParameterGroupName"] == name
            assert groups[0]["ParameterGroupFamily"] == "redshift-1.0"
        finally:
            redshift.delete_cluster_parameter_group(ParameterGroupName=name)

    def test_describe_events_by_source_identifier(self, redshift):
        """DescribeEvents filtered by source type cluster."""
        resp = redshift.describe_events(SourceType="cluster")
        assert "Events" in resp
        assert isinstance(resp["Events"], list)

    def test_describe_orderable_cluster_options_with_node_type(self, redshift):
        """DescribeOrderableClusterOptions filtered by node type."""
        resp = redshift.describe_orderable_cluster_options(NodeType="dc2.large")
        assert "OrderableClusterOptions" in resp
        assert isinstance(resp["OrderableClusterOptions"], list)

    def test_describe_cluster_versions_with_family(self, redshift):
        """DescribeClusterVersions filtered by parameter group family."""
        resp = redshift.describe_cluster_versions(
            ClusterParameterGroupFamily="redshift-1.0",
        )
        assert "ClusterVersions" in resp
        assert isinstance(resp["ClusterVersions"], list)

    def test_describe_default_cluster_parameters_family(self, redshift):
        """DescribeDefaultClusterParameters for redshift-1.0."""
        resp = redshift.describe_default_cluster_parameters(
            ParameterGroupFamily="redshift-1.0",
        )
        params = resp["DefaultClusterParameters"]
        assert "ParameterGroupFamily" in params
        assert params["ParameterGroupFamily"] == "redshift-1.0"
        assert "Parameters" in params

    def test_describe_logging_status_on_cluster(self, redshift):
        """DescribeLoggingStatus on a specific cluster."""
        cid = f"logstat-{_uid()}"
        redshift.create_cluster(
            ClusterIdentifier=cid,
            NodeType="dc2.large",
            MasterUsername="admin",
            MasterUserPassword="Password1!",
            NumberOfNodes=1,
            ClusterType="single-node",
        )
        try:
            resp = redshift.describe_logging_status(ClusterIdentifier=cid)
            assert "LoggingEnabled" in resp
        finally:
            redshift.delete_cluster(ClusterIdentifier=cid, SkipFinalClusterSnapshot=True)

    def test_create_and_describe_snapshot_schedule(self, redshift):
        """Create and describe a snapshot schedule."""
        sched_id = f"sched-{_uid()}"
        resp = redshift.create_snapshot_schedule(
            ScheduleIdentifier=sched_id,
            ScheduleDefinitions=["rate(12 hours)"],
        )
        assert resp["ScheduleIdentifier"] == sched_id
        assert "rate(12 hours)" in resp["ScheduleDefinitions"]

        # Describe
        desc = redshift.describe_snapshot_schedules(
            ScheduleIdentifier=sched_id,
        )
        assert len(desc["SnapshotSchedules"]) == 1
        assert desc["SnapshotSchedules"][0]["ScheduleIdentifier"] == sched_id

    def test_describe_account_attributes_has_keys(self, redshift):
        """DescribeAccountAttributes returns AccountAttributes list with expected structure."""
        resp = redshift.describe_account_attributes()
        assert "AccountAttributes" in resp
        assert isinstance(resp["AccountAttributes"], list)

    def test_describe_storage_returns_totals(self, redshift):
        """DescribeStorage returns backup and provisioned storage totals."""
        resp = redshift.describe_storage()
        assert "TotalBackupSizeInMegaBytes" in resp
        assert "TotalProvisionedStorageInMegaBytes" in resp

    def test_describe_tags_on_tagged_cluster(self, redshift):
        """DescribeTags on a cluster with tags returns the tags."""
        cid = f"tagged-{_uid()}"
        redshift.create_cluster(
            ClusterIdentifier=cid,
            NodeType="dc2.large",
            MasterUsername="admin",
            MasterUserPassword="Password1!",
            NumberOfNodes=1,
            ClusterType="single-node",
            Tags=[{"Key": "env", "Value": "test"}],
        )
        try:
            resp = redshift.describe_tags(ResourceType="cluster")
            assert "TaggedResources" in resp
            assert isinstance(resp["TaggedResources"], list)
            # At least our tagged cluster should appear
            assert len(resp["TaggedResources"]) >= 1
        finally:
            redshift.delete_cluster(ClusterIdentifier=cid, SkipFinalClusterSnapshot=True)

    def test_describe_event_categories_for_cluster(self, redshift):
        """DescribeEventCategories filtered by source type."""
        resp = redshift.describe_event_categories(SourceType="cluster")
        assert "EventCategoriesMapList" in resp
        assert isinstance(resp["EventCategoriesMapList"], list)

    def test_describe_reserved_node_offerings_structure(self, redshift):
        """DescribeReservedNodeOfferings returns list with expected structure."""
        resp = redshift.describe_reserved_node_offerings()
        assert "ReservedNodeOfferings" in resp
        assert isinstance(resp["ReservedNodeOfferings"], list)

    def test_describe_cluster_subnet_groups_create_describe_delete(self, redshift):
        """Full CRUD for cluster subnet groups."""
        import boto3
        from botocore.config import Config

        ec2 = boto3.client(
            "ec2",
            endpoint_url="http://localhost:4566",
            region_name="us-east-1",
            aws_access_key_id="testing",
            aws_secret_access_key="testing",
            config=Config(),
        )
        # Create VPC and subnets
        vpc = ec2.create_vpc(CidrBlock="10.0.0.0/16")
        vpc_id = vpc["Vpc"]["VpcId"]
        subnet = ec2.create_subnet(VpcId=vpc_id, CidrBlock="10.0.1.0/24")
        subnet_id = subnet["Subnet"]["SubnetId"]

        name = f"sng-{_uid()}"
        try:
            resp = redshift.create_cluster_subnet_group(
                ClusterSubnetGroupName=name,
                Description="test subnet group",
                SubnetIds=[subnet_id],
            )
            assert resp["ClusterSubnetGroup"]["ClusterSubnetGroupName"] == name

            desc = redshift.describe_cluster_subnet_groups(
                ClusterSubnetGroupName=name,
            )
            assert len(desc["ClusterSubnetGroups"]) == 1
            assert desc["ClusterSubnetGroups"][0]["ClusterSubnetGroupName"] == name

            redshift.delete_cluster_subnet_group(ClusterSubnetGroupName=name)

            # After delete, should get error
            with pytest.raises(ClientError) as exc_info:
                redshift.describe_cluster_subnet_groups(
                    ClusterSubnetGroupName=name,
                )
            assert "ClusterSubnetGroupNotFoundFault" in str(exc_info.value)
        finally:
            ec2.delete_subnet(SubnetId=subnet_id)
            ec2.delete_vpc(VpcId=vpc_id)


class TestRedshiftNewCoverage:
    """New tests for previously untested Redshift operations."""

    @pytest.fixture
    def redshift(self):
        return make_client("redshift")

    def test_modify_cluster_not_found(self, redshift):
        """ModifyCluster raises error for nonexistent cluster."""
        with pytest.raises(ClientError) as exc:
            redshift.modify_cluster(
                ClusterIdentifier=f"nonexistent-{_uid()}",
                AllowVersionUpgrade=False,
            )
        assert "ClusterNotFound" in exc.value.response["Error"]["Code"]

    def test_pause_cluster_not_found(self, redshift):
        """PauseCluster raises error for nonexistent cluster."""
        with pytest.raises(ClientError) as exc:
            redshift.pause_cluster(ClusterIdentifier=f"nonexistent-{_uid()}")
        assert "ClusterNotFound" in exc.value.response["Error"]["Code"]

    def test_resume_cluster_not_found(self, redshift):
        """ResumeCluster raises error for nonexistent cluster."""
        with pytest.raises(ClientError) as exc:
            redshift.resume_cluster(ClusterIdentifier=f"nonexistent-{_uid()}")
        assert "ClusterNotFound" in exc.value.response["Error"]["Code"]

    def test_create_cluster_with_parameter_group(self, redshift):
        """CreateCluster with a custom parameter group."""
        pg = f"clpg-{_uid()}"
        cid = f"clpg-{_uid()}"
        redshift.create_cluster_parameter_group(
            ParameterGroupName=pg,
            ParameterGroupFamily="redshift-1.0",
            Description="For cluster test",
        )
        try:
            resp = redshift.create_cluster(
                ClusterIdentifier=cid,
                NodeType="dc2.large",
                MasterUsername="admin",
                MasterUserPassword="Password1!",
                NumberOfNodes=1,
                ClusterType="single-node",
                ClusterParameterGroupName=pg,
            )
            assert resp["Cluster"]["ClusterIdentifier"] == cid
            pg_status = resp["Cluster"].get("ClusterParameterGroups", [])
            pg_names = [p["ParameterGroupName"] for p in pg_status]
            assert pg in pg_names
        finally:
            redshift.delete_cluster(ClusterIdentifier=cid, SkipFinalClusterSnapshot=True)
            redshift.delete_cluster_parameter_group(ParameterGroupName=pg)

    def test_describe_cluster_db_revisions_on_cluster(self, redshift):
        """DescribeClusterDbRevisions with ClusterIdentifier filter."""
        cid = f"dbrev-{_uid()}"
        redshift.create_cluster(
            ClusterIdentifier=cid,
            NodeType="dc2.large",
            MasterUsername="admin",
            MasterUserPassword="Password1!",
            NumberOfNodes=1,
            ClusterType="single-node",
        )
        try:
            resp = redshift.describe_cluster_db_revisions(ClusterIdentifier=cid)
            assert "ClusterDbRevisions" in resp
            assert isinstance(resp["ClusterDbRevisions"], list)
        finally:
            redshift.delete_cluster(ClusterIdentifier=cid, SkipFinalClusterSnapshot=True)

    def test_create_cluster_with_subnet_group(self, redshift):
        """CreateCluster with a custom subnet group."""
        ec2 = make_client("ec2")
        vpc = ec2.create_vpc(CidrBlock="10.251.0.0/16")
        vpc_id = vpc["Vpc"]["VpcId"]
        subnet = ec2.create_subnet(VpcId=vpc_id, CidrBlock="10.251.1.0/24")
        subnet_id = subnet["Subnet"]["SubnetId"]
        sng = f"clsng-{_uid()}"
        cid = f"clsng-{_uid()}"
        redshift.create_cluster_subnet_group(
            ClusterSubnetGroupName=sng,
            Description="For cluster test",
            SubnetIds=[subnet_id],
        )
        try:
            resp = redshift.create_cluster(
                ClusterIdentifier=cid,
                NodeType="dc2.large",
                MasterUsername="admin",
                MasterUserPassword="Password1!",
                NumberOfNodes=1,
                ClusterType="single-node",
                ClusterSubnetGroupName=sng,
            )
            assert resp["Cluster"]["ClusterIdentifier"] == cid
            assert resp["Cluster"]["ClusterSubnetGroupName"] == sng
        finally:
            redshift.delete_cluster(ClusterIdentifier=cid, SkipFinalClusterSnapshot=True)
            redshift.delete_cluster_subnet_group(ClusterSubnetGroupName=sng)

    def test_get_cluster_credentials_auto_create(self, redshift):
        """GetClusterCredentials with AutoCreate creates a new user."""
        cid = f"autocr-{_uid()}"
        redshift.create_cluster(
            ClusterIdentifier=cid,
            NodeType="dc2.large",
            MasterUsername="admin",
            MasterUserPassword="Password1!",
            NumberOfNodes=1,
            ClusterType="single-node",
        )
        try:
            resp = redshift.get_cluster_credentials(
                DbUser="newuser",
                ClusterIdentifier=cid,
                AutoCreate=True,
            )
            assert "DbUser" in resp
            assert "DbPassword" in resp
        finally:
            redshift.delete_cluster(ClusterIdentifier=cid, SkipFinalClusterSnapshot=True)

    def test_describe_cluster_tracks_structure(self, redshift):
        """DescribeClusterTracks returns maintenance tracks with structure."""
        resp = redshift.describe_cluster_tracks()
        assert "MaintenanceTracks" in resp
        assert isinstance(resp["MaintenanceTracks"], list)

    def test_describe_authentication_profiles_structure(self, redshift):
        """DescribeAuthenticationProfiles returns empty list by default."""
        resp = redshift.describe_authentication_profiles()
        assert "AuthenticationProfiles" in resp
        assert isinstance(resp["AuthenticationProfiles"], list)

    def test_get_cluster_credentials_with_db_groups(self, redshift):
        """GetClusterCredentials with DbGroups parameter."""
        cid = f"dbgrp-{_uid()}"
        redshift.create_cluster(
            ClusterIdentifier=cid,
            NodeType="dc2.large",
            MasterUsername="admin",
            MasterUserPassword="Password1!",
            NumberOfNodes=1,
            ClusterType="single-node",
        )
        try:
            resp = redshift.get_cluster_credentials(
                DbUser="admin",
                ClusterIdentifier=cid,
                DbGroups=["analysts", "readonly"],
            )
            assert "DbUser" in resp
            assert "DbPassword" in resp
            assert "Expiration" in resp
        finally:
            redshift.delete_cluster(ClusterIdentifier=cid, SkipFinalClusterSnapshot=True)

    def test_create_cluster_with_security_group(self, redshift):
        """CreateCluster with a ClusterSecurityGroup."""
        sg = f"clsg-{_uid()}"
        cid = f"clsg-{_uid()}"
        redshift.create_cluster_security_group(
            ClusterSecurityGroupName=sg,
            Description="For cluster test",
        )
        try:
            resp = redshift.create_cluster(
                ClusterIdentifier=cid,
                NodeType="dc2.large",
                MasterUsername="admin",
                MasterUserPassword="Password1!",
                NumberOfNodes=1,
                ClusterType="single-node",
                ClusterSecurityGroups=[sg],
            )
            assert resp["Cluster"]["ClusterIdentifier"] == cid
        finally:
            redshift.delete_cluster(ClusterIdentifier=cid, SkipFinalClusterSnapshot=True)
            redshift.delete_cluster_security_group(ClusterSecurityGroupName=sg)

    def test_modify_cluster_allow_version_upgrade(self, redshift):
        """ModifyCluster to change AllowVersionUpgrade."""
        cid = f"modvu-{_uid()}"
        redshift.create_cluster(
            ClusterIdentifier=cid,
            NodeType="dc2.large",
            MasterUsername="admin",
            MasterUserPassword="Password1!",
            NumberOfNodes=1,
            ClusterType="single-node",
        )
        try:
            resp = redshift.modify_cluster(
                ClusterIdentifier=cid,
                AllowVersionUpgrade=True,
            )
            assert resp["Cluster"]["ClusterIdentifier"] == cid
            assert resp["Cluster"]["AllowVersionUpgrade"] is True
        finally:
            redshift.delete_cluster(ClusterIdentifier=cid, SkipFinalClusterSnapshot=True)

    def test_describe_events_with_duration(self, redshift):
        """DescribeEvents with Duration parameter."""
        resp = redshift.describe_events(Duration=60)
        assert "Events" in resp
        assert isinstance(resp["Events"], list)

    def test_describe_event_subscriptions_empty(self, redshift):
        """DescribeEventSubscriptions returns empty list by default."""
        resp = redshift.describe_event_subscriptions()
        assert "EventSubscriptionsList" in resp
        assert isinstance(resp["EventSubscriptionsList"], list)

    def test_create_snapshot_with_tags(self, redshift):
        """CreateClusterSnapshot with tags attached."""
        cid = f"sntag-{_uid()}"
        snap = f"sntag-{_uid()}"
        redshift.create_cluster(
            ClusterIdentifier=cid,
            NodeType="dc2.large",
            MasterUsername="admin",
            MasterUserPassword="Password1!",
            NumberOfNodes=1,
            ClusterType="single-node",
        )
        try:
            resp = redshift.create_cluster_snapshot(
                SnapshotIdentifier=snap,
                ClusterIdentifier=cid,
                Tags=[{"Key": "backup", "Value": "daily"}],
            )
            assert resp["Snapshot"]["SnapshotIdentifier"] == snap
            tags = {t["Key"]: t["Value"] for t in resp["Snapshot"].get("Tags", [])}
            assert tags.get("backup") == "daily"
            redshift.delete_cluster_snapshot(SnapshotIdentifier=snap)
        finally:
            redshift.delete_cluster(ClusterIdentifier=cid, SkipFinalClusterSnapshot=True)

    def test_describe_cluster_parameter_groups_default(self, redshift):
        """DescribeClusterParameterGroups includes the default group."""
        resp = redshift.describe_cluster_parameter_groups()
        names = [g["ParameterGroupName"] for g in resp["ParameterGroups"]]
        assert "default.redshift-1.0" in names

    def test_create_cluster_snapshot_duplicate_error(self, redshift):
        """Creating a duplicate snapshot raises an error."""
        cid = f"dupsnp-{_uid()}"
        snap = f"dupsnp-{_uid()}"
        redshift.create_cluster(
            ClusterIdentifier=cid,
            NodeType="dc2.large",
            MasterUsername="admin",
            MasterUserPassword="Password1!",
            NumberOfNodes=1,
            ClusterType="single-node",
        )
        try:
            redshift.create_cluster_snapshot(SnapshotIdentifier=snap, ClusterIdentifier=cid)
            with pytest.raises(ClientError) as exc:
                redshift.create_cluster_snapshot(SnapshotIdentifier=snap, ClusterIdentifier=cid)
            assert "AlreadyExists" in exc.value.response["Error"]["Code"]
            redshift.delete_cluster_snapshot(SnapshotIdentifier=snap)
        finally:
            redshift.delete_cluster(ClusterIdentifier=cid, SkipFinalClusterSnapshot=True)

    def test_describe_hsm_client_certificates_empty(self, redshift):
        """DescribeHsmClientCertificates returns list."""
        resp = redshift.describe_hsm_client_certificates()
        assert "HsmClientCertificates" in resp
        assert isinstance(resp["HsmClientCertificates"], list)

    def test_describe_hsm_configurations_empty(self, redshift):
        """DescribeHsmConfigurations returns list."""
        resp = redshift.describe_hsm_configurations()
        assert "HsmConfigurations" in resp
        assert isinstance(resp["HsmConfigurations"], list)


class TestRedshiftGetReservedNodeExchangeOfferings:
    """Tests for GetReservedNodeExchangeOfferings."""

    def test_get_reserved_node_exchange_offerings(self, redshift):
        """GetReservedNodeExchangeOfferings with fake node ID returns empty list."""
        resp = redshift.get_reserved_node_exchange_offerings(ReservedNodeId="fake-node-id-12345")
        assert "ReservedNodeOfferings" in resp
        assert isinstance(resp["ReservedNodeOfferings"], list)


class TestRedshiftScheduledActions:
    """Tests for ScheduledAction CRUD operations."""

    def test_create_scheduled_action(self, redshift):
        """CreateScheduledAction returns the action details."""
        name = f"sa-{_uid()}"
        try:
            resp = redshift.create_scheduled_action(
                ScheduledActionName=name,
                TargetAction={
                    "ResizeCluster": {
                        "ClusterIdentifier": "fake-cluster",
                        "NumberOfNodes": 2,
                    }
                },
                Schedule="rate(1 hour)",
                IamRole="arn:aws:iam::123456789012:role/redshift-role",
            )
            assert resp["ScheduledActionName"] == name
            assert resp["Schedule"] == "rate(1 hour)"
            assert "State" in resp
        finally:
            redshift.delete_scheduled_action(ScheduledActionName=name)

    def test_describe_scheduled_actions_contains_created(self, redshift):
        """DescribeScheduledActions includes a freshly created action."""
        name = f"sa-{_uid()}"
        try:
            redshift.create_scheduled_action(
                ScheduledActionName=name,
                TargetAction={
                    "ResizeCluster": {
                        "ClusterIdentifier": "fake-cluster",
                        "NumberOfNodes": 2,
                    }
                },
                Schedule="rate(1 hour)",
                IamRole="arn:aws:iam::123456789012:role/redshift-role",
            )
            resp = redshift.describe_scheduled_actions()
            names = [a["ScheduledActionName"] for a in resp["ScheduledActions"]]
            assert name in names
        finally:
            redshift.delete_scheduled_action(ScheduledActionName=name)

    def test_modify_scheduled_action(self, redshift):
        """ModifyScheduledAction updates the schedule."""
        name = f"sa-{_uid()}"
        try:
            redshift.create_scheduled_action(
                ScheduledActionName=name,
                TargetAction={
                    "ResizeCluster": {
                        "ClusterIdentifier": "fake-cluster",
                        "NumberOfNodes": 2,
                    }
                },
                Schedule="rate(1 hour)",
                IamRole="arn:aws:iam::123456789012:role/redshift-role",
            )
            resp = redshift.modify_scheduled_action(
                ScheduledActionName=name,
                Schedule="rate(2 hours)",
            )
            assert resp["ScheduledActionName"] == name
            assert resp["Schedule"] == "rate(2 hours)"
        finally:
            redshift.delete_scheduled_action(ScheduledActionName=name)

    def test_delete_scheduled_action(self, redshift):
        """DeleteScheduledAction removes the action from describe."""
        name = f"sa-{_uid()}"
        redshift.create_scheduled_action(
            ScheduledActionName=name,
            TargetAction={
                "ResizeCluster": {
                    "ClusterIdentifier": "fake-cluster",
                    "NumberOfNodes": 2,
                }
            },
            Schedule="rate(1 hour)",
            IamRole="arn:aws:iam::123456789012:role/redshift-role",
        )
        redshift.delete_scheduled_action(ScheduledActionName=name)
        resp = redshift.describe_scheduled_actions()
        names = [a["ScheduledActionName"] for a in resp["ScheduledActions"]]
        assert name not in names

    def test_describe_scheduled_actions_by_name(self, redshift):
        """DescribeScheduledActions filtered by name returns the specific action."""
        name = f"sa-{_uid()}"
        try:
            redshift.create_scheduled_action(
                ScheduledActionName=name,
                TargetAction={
                    "ResizeCluster": {
                        "ClusterIdentifier": "fake-cluster",
                        "NumberOfNodes": 2,
                    }
                },
                Schedule="rate(1 hour)",
                IamRole="arn:aws:iam::123456789012:role/redshift-role",
            )
            resp = redshift.describe_scheduled_actions(ScheduledActionName=name)
            assert len(resp["ScheduledActions"]) == 1
            assert resp["ScheduledActions"][0]["ScheduledActionName"] == name
        finally:
            redshift.delete_scheduled_action(ScheduledActionName=name)

    def test_create_scheduled_action_with_description(self, redshift):
        """CreateScheduledAction with description stores it."""
        name = f"sa-{_uid()}"
        try:
            resp = redshift.create_scheduled_action(
                ScheduledActionName=name,
                TargetAction={
                    "ResizeCluster": {
                        "ClusterIdentifier": "fake-cluster",
                        "NumberOfNodes": 2,
                    }
                },
                Schedule="rate(1 hour)",
                IamRole="arn:aws:iam::123456789012:role/redshift-role",
                ScheduledActionDescription="My scheduled resize",
            )
            assert resp["ScheduledActionDescription"] == "My scheduled resize"
        finally:
            redshift.delete_scheduled_action(ScheduledActionName=name)


class TestRedshiftUsageLimits:
    """Tests for UsageLimit CRUD operations."""

    @pytest.fixture
    def cluster(self, redshift):
        """Create a cluster for usage limit tests."""
        cid = f"ul-{_uid()}"
        redshift.create_cluster(
            ClusterIdentifier=cid,
            NodeType="dc2.large",
            MasterUsername="admin",
            MasterUserPassword="Password1!",
            NumberOfNodes=1,
            ClusterType="single-node",
        )
        yield cid
        redshift.delete_cluster(ClusterIdentifier=cid, SkipFinalClusterSnapshot=True)

    def test_create_usage_limit(self, redshift, cluster):
        """CreateUsageLimit returns usage limit details."""
        resp = redshift.create_usage_limit(
            ClusterIdentifier=cluster,
            FeatureType="spectrum",
            LimitType="data-scanned",
            Amount=100,
        )
        assert resp["ClusterIdentifier"] == cluster
        assert resp["FeatureType"] == "spectrum"
        assert resp["LimitType"] == "data-scanned"
        assert resp["Amount"] == 100
        assert "UsageLimitId" in resp
        # cleanup
        redshift.delete_usage_limit(UsageLimitId=resp["UsageLimitId"])

    def test_describe_usage_limits_for_cluster(self, redshift, cluster):
        """DescribeUsageLimits filtered by cluster returns the limit."""
        resp = redshift.create_usage_limit(
            ClusterIdentifier=cluster,
            FeatureType="spectrum",
            LimitType="data-scanned",
            Amount=50,
        )
        ul_id = resp["UsageLimitId"]
        try:
            desc = redshift.describe_usage_limits(ClusterIdentifier=cluster)
            ids = [u["UsageLimitId"] for u in desc["UsageLimits"]]
            assert ul_id in ids
        finally:
            redshift.delete_usage_limit(UsageLimitId=ul_id)

    def test_modify_usage_limit(self, redshift, cluster):
        """ModifyUsageLimit updates the amount."""
        resp = redshift.create_usage_limit(
            ClusterIdentifier=cluster,
            FeatureType="spectrum",
            LimitType="data-scanned",
            Amount=100,
        )
        ul_id = resp["UsageLimitId"]
        try:
            mod = redshift.modify_usage_limit(UsageLimitId=ul_id, Amount=200)
            assert mod["UsageLimitId"] == ul_id
            assert mod["Amount"] == 200
        finally:
            redshift.delete_usage_limit(UsageLimitId=ul_id)

    def test_modify_usage_limit_breach_action(self, redshift, cluster):
        """ModifyUsageLimit can change the breach action."""
        resp = redshift.create_usage_limit(
            ClusterIdentifier=cluster,
            FeatureType="spectrum",
            LimitType="data-scanned",
            Amount=100,
        )
        ul_id = resp["UsageLimitId"]
        try:
            mod = redshift.modify_usage_limit(UsageLimitId=ul_id, BreachAction="disable")
            assert mod["BreachAction"] == "disable"
        finally:
            redshift.delete_usage_limit(UsageLimitId=ul_id)

    def test_delete_usage_limit(self, redshift, cluster):
        """DeleteUsageLimit removes the limit from describe."""
        resp = redshift.create_usage_limit(
            ClusterIdentifier=cluster,
            FeatureType="spectrum",
            LimitType="data-scanned",
            Amount=100,
        )
        ul_id = resp["UsageLimitId"]
        redshift.delete_usage_limit(UsageLimitId=ul_id)
        desc = redshift.describe_usage_limits(ClusterIdentifier=cluster)
        ids = [u["UsageLimitId"] for u in desc["UsageLimits"]]
        assert ul_id not in ids

    def test_create_usage_limit_concurrency_scaling(self, redshift, cluster):
        """CreateUsageLimit for concurrency-scaling feature."""
        resp = redshift.create_usage_limit(
            ClusterIdentifier=cluster,
            FeatureType="concurrency-scaling",
            LimitType="time",
            Amount=60,
        )
        assert resp["FeatureType"] == "concurrency-scaling"
        assert resp["LimitType"] == "time"
        assert resp["Amount"] == 60
        redshift.delete_usage_limit(UsageLimitId=resp["UsageLimitId"])


class TestRedshiftAuthenticationProfiles:
    """Tests for authentication profile CRUD operations."""

    def test_create_authentication_profile(self, redshift):
        """CreateAuthenticationProfile returns name and content."""
        name = f"auth-{_uid()}"
        try:
            resp = redshift.create_authentication_profile(
                AuthenticationProfileName=name,
                AuthenticationProfileContent='{"AllowDBUserOverride": "1"}',
            )
            assert resp["AuthenticationProfileName"] == name
            assert "AuthenticationProfileContent" in resp
        finally:
            redshift.delete_authentication_profile(AuthenticationProfileName=name)

    def test_describe_authentication_profile_by_name(self, redshift):
        """DescribeAuthenticationProfiles filtered by name returns the profile."""
        name = f"auth-{_uid()}"
        try:
            redshift.create_authentication_profile(
                AuthenticationProfileName=name,
                AuthenticationProfileContent='{"key": "value"}',
            )
            resp = redshift.describe_authentication_profiles(
                AuthenticationProfileName=name,
            )
            profiles = resp["AuthenticationProfiles"]
            assert len(profiles) >= 1
            found = [p for p in profiles if p["AuthenticationProfileName"] == name]
            assert len(found) == 1
            assert "AuthenticationProfileContent" in found[0]
        finally:
            redshift.delete_authentication_profile(AuthenticationProfileName=name)

    def test_modify_authentication_profile(self, redshift):
        """ModifyAuthenticationProfile updates content."""
        name = f"auth-{_uid()}"
        try:
            redshift.create_authentication_profile(
                AuthenticationProfileName=name,
                AuthenticationProfileContent='{"old": "content"}',
            )
            resp = redshift.modify_authentication_profile(
                AuthenticationProfileName=name,
                AuthenticationProfileContent='{"new": "content"}',
            )
            assert resp["AuthenticationProfileName"] == name
            assert "AuthenticationProfileContent" in resp
        finally:
            redshift.delete_authentication_profile(AuthenticationProfileName=name)

    def test_delete_authentication_profile(self, redshift):
        """DeleteAuthenticationProfile removes the profile."""
        name = f"auth-{_uid()}"
        redshift.create_authentication_profile(
            AuthenticationProfileName=name,
            AuthenticationProfileContent='{"tmp": "data"}',
        )
        resp = redshift.delete_authentication_profile(AuthenticationProfileName=name)
        assert resp["AuthenticationProfileName"] == name


class TestRedshiftSnapshotAdvanced:
    """Tests for snapshot copy, authorize, and batch operations."""

    @pytest.fixture
    def cluster(self, redshift):
        cid = f"snap-adv-{_uid()}"
        redshift.create_cluster(
            ClusterIdentifier=cid,
            NodeType="dc2.large",
            MasterUsername="admin",
            MasterUserPassword="Password1!",
            NumberOfNodes=1,
            ClusterType="single-node",
        )
        yield cid
        redshift.delete_cluster(ClusterIdentifier=cid, SkipFinalClusterSnapshot=True)

    def test_copy_cluster_snapshot(self, redshift, cluster):
        """CopyClusterSnapshot creates a copy of a snapshot."""
        snap_id = f"snap-{_uid()}"
        copy_id = f"copy-{_uid()}"
        redshift.create_cluster_snapshot(SnapshotIdentifier=snap_id, ClusterIdentifier=cluster)
        try:
            resp = redshift.copy_cluster_snapshot(
                SourceSnapshotIdentifier=snap_id,
                TargetSnapshotIdentifier=copy_id,
            )
            assert "Snapshot" in resp
            assert resp["Snapshot"]["SnapshotIdentifier"] == copy_id
            assert resp["Snapshot"]["ClusterIdentifier"] == cluster
        finally:
            try:
                redshift.delete_cluster_snapshot(SnapshotIdentifier=copy_id)
            except Exception:
                pass
            redshift.delete_cluster_snapshot(SnapshotIdentifier=snap_id)

    def test_authorize_and_revoke_snapshot_access(self, redshift, cluster):
        """AuthorizeSnapshotAccess grants access, RevokeSnapshotAccess removes it."""
        snap_id = f"snap-{_uid()}"
        redshift.create_cluster_snapshot(SnapshotIdentifier=snap_id, ClusterIdentifier=cluster)
        try:
            auth_resp = redshift.authorize_snapshot_access(
                SnapshotIdentifier=snap_id,
                AccountWithRestoreAccess="999999999999",
            )
            assert "Snapshot" in auth_resp
            assert "AccountsWithRestoreAccess" in auth_resp["Snapshot"]

            revoke_resp = redshift.revoke_snapshot_access(
                SnapshotIdentifier=snap_id,
                AccountWithRestoreAccess="999999999999",
            )
            assert "Snapshot" in revoke_resp
        finally:
            redshift.delete_cluster_snapshot(SnapshotIdentifier=snap_id)

    def test_batch_modify_cluster_snapshots(self, redshift, cluster):
        """BatchModifyClusterSnapshots modifies retention on snapshots."""
        snap_id = f"snap-{_uid()}"
        redshift.create_cluster_snapshot(SnapshotIdentifier=snap_id, ClusterIdentifier=cluster)
        try:
            resp = redshift.batch_modify_cluster_snapshots(
                SnapshotIdentifierList=[snap_id],
                ManualSnapshotRetentionPeriod=7,
            )
            assert "Resources" in resp
        finally:
            redshift.delete_cluster_snapshot(SnapshotIdentifier=snap_id)

    def test_batch_delete_cluster_snapshots(self, redshift, cluster):
        """BatchDeleteClusterSnapshots deletes multiple snapshots."""
        snap_id = f"snap-{_uid()}"
        redshift.create_cluster_snapshot(SnapshotIdentifier=snap_id, ClusterIdentifier=cluster)
        resp = redshift.batch_delete_cluster_snapshots(
            Identifiers=[{"SnapshotIdentifier": snap_id}],
        )
        assert "Resources" in resp


class TestRedshiftClusterIamAndEndpoint:
    """Tests for ModifyClusterIamRoles and AuthorizeEndpointAccess."""

    @pytest.fixture
    def cluster(self, redshift):
        cid = f"iam-ep-{_uid()}"
        redshift.create_cluster(
            ClusterIdentifier=cid,
            NodeType="dc2.large",
            MasterUsername="admin",
            MasterUserPassword="Password1!",
            NumberOfNodes=1,
            ClusterType="single-node",
        )
        yield cid
        redshift.delete_cluster(ClusterIdentifier=cid, SkipFinalClusterSnapshot=True)

    def test_modify_cluster_iam_roles_add(self, redshift, cluster):
        """ModifyClusterIamRoles adds IAM roles to a cluster."""
        role_arn = "arn:aws:iam::123456789012:role/RedshiftRole"
        resp = redshift.modify_cluster_iam_roles(
            ClusterIdentifier=cluster,
            AddIamRoles=[role_arn],
        )
        assert "Cluster" in resp
        assert resp["Cluster"]["ClusterIdentifier"] == cluster

    def test_modify_cluster_iam_roles_remove(self, redshift, cluster):
        """ModifyClusterIamRoles can add then remove a role."""
        role_arn = "arn:aws:iam::123456789012:role/TestRole"
        redshift.modify_cluster_iam_roles(
            ClusterIdentifier=cluster,
            AddIamRoles=[role_arn],
        )
        resp = redshift.modify_cluster_iam_roles(
            ClusterIdentifier=cluster,
            RemoveIamRoles=[role_arn],
        )
        assert "Cluster" in resp

    def test_authorize_endpoint_access(self, redshift, cluster):
        """AuthorizeEndpointAccess grants endpoint access to an account."""
        resp = redshift.authorize_endpoint_access(
            ClusterIdentifier=cluster,
            Account="999999999999",
        )
        assert "Grantee" in resp
        assert "ClusterIdentifier" in resp
        assert resp["ClusterIdentifier"] == cluster
        assert "Status" in resp


class TestRedshiftAdditionalOps:
    """Tests for additional Redshift operations."""

    def test_describe_node_configuration_options(self, redshift):
        """DescribeNodeConfigurationOptions returns a list."""
        resp = redshift.describe_node_configuration_options(ActionType="recommend-node-config")
        assert "NodeConfigurationOptionList" in resp
        assert isinstance(resp["NodeConfigurationOptionList"], list)

    def test_describe_partners(self, redshift):
        """DescribePartners returns partner info list."""
        resp = redshift.describe_partners(
            AccountId="123456789012",
            ClusterIdentifier="nonexistent-cluster",
        )
        assert "PartnerIntegrationInfoList" in resp
        assert isinstance(resp["PartnerIntegrationInfoList"], list)

    def test_describe_resize_cluster_not_found(self, redshift):
        """DescribeResize with nonexistent cluster raises ClusterNotFound."""
        with pytest.raises(ClientError) as exc:
            redshift.describe_resize(ClusterIdentifier="nonexistent-cluster-xyz")
        assert "ClusterNotFound" in exc.value.response["Error"]["Code"]

    def test_get_reserved_node_exchange_configuration_options(self, redshift):
        """GetReservedNodeExchangeConfigurationOptions returns a list."""
        resp = redshift.get_reserved_node_exchange_configuration_options(
            ActionType="restore-cluster"
        )
        assert "ReservedNodeConfigurationOptionList" in resp
        assert isinstance(resp["ReservedNodeConfigurationOptionList"], list)
