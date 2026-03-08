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
