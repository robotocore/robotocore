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
