"""DAX compatibility tests."""

import uuid

import pytest

from tests.compatibility.conftest import make_client


@pytest.fixture
def dax():
    return make_client("dax")


def _unique(prefix):
    return f"{prefix}-{uuid.uuid4().hex[:8]}"


class TestDAXClusterOperations:
    @pytest.fixture
    def cluster(self, dax):
        name = _unique("dax")
        resp = dax.create_cluster(
            ClusterName=name,
            NodeType="dax.r4.large",
            ReplicationFactor=1,
            IamRoleArn="arn:aws:iam::123456789012:role/DAXRole",
        )
        arn = resp["Cluster"]["ClusterArn"]
        yield {"name": name, "arn": arn}
        try:
            dax.delete_cluster(ClusterName=name)
        except Exception:
            pass

    def test_create_cluster(self, dax):
        name = _unique("dax")
        resp = dax.create_cluster(
            ClusterName=name,
            NodeType="dax.r4.large",
            ReplicationFactor=1,
            IamRoleArn="arn:aws:iam::123456789012:role/DAXRole",
        )
        cluster = resp["Cluster"]
        assert cluster["ClusterName"] == name
        assert cluster["Status"] == "creating"
        assert "ClusterArn" in cluster
        try:
            dax.delete_cluster(ClusterName=name)
        except Exception:
            pass  # cluster may have advanced past "deleting" before delete returns

    def test_describe_clusters_filtered(self, dax, cluster):
        resp = dax.describe_clusters(ClusterNames=[cluster["name"]])
        clusters = resp["Clusters"]
        assert len(clusters) == 1
        assert clusters[0]["ClusterName"] == cluster["name"]

    def test_delete_cluster(self, dax):
        name = _unique("dax")
        dax.create_cluster(
            ClusterName=name,
            NodeType="dax.r4.large",
            ReplicationFactor=1,
            IamRoleArn="arn:aws:iam::123456789012:role/DAXRole",
        )
        resp = dax.delete_cluster(ClusterName=name)
        assert resp["Cluster"]["ClusterName"] == name

    def test_decrease_replication_factor(self, dax):
        name = _unique("dax")
        dax.create_cluster(
            ClusterName=name,
            NodeType="dax.r4.large",
            ReplicationFactor=3,
            IamRoleArn="arn:aws:iam::123456789012:role/DAXRole",
        )
        try:
            resp = dax.decrease_replication_factor(
                ClusterName=name,
                NewReplicationFactor=1,
            )
            cluster = resp["Cluster"]
            assert cluster["ClusterName"] == name
            assert "TotalNodes" in cluster
        finally:
            try:
                dax.delete_cluster(ClusterName=name)
            except Exception:
                pass

    def test_list_tags(self, dax, cluster):
        resp = dax.list_tags(ResourceName=cluster["arn"])
        assert "Tags" in resp
        assert isinstance(resp["Tags"], list)

    def test_tag_resource(self, dax, cluster):
        resp = dax.tag_resource(
            ResourceName=cluster["arn"],
            Tags=[{"Key": "env", "Value": "test"}],
        )
        assert "Tags" in resp
        assert any(t["Key"] == "env" and t["Value"] == "test" for t in resp["Tags"])

    def test_untag_resource(self, dax, cluster):
        dax.tag_resource(
            ResourceName=cluster["arn"],
            Tags=[{"Key": "removeme", "Value": "yes"}],
        )
        resp = dax.untag_resource(
            ResourceName=cluster["arn"],
            TagKeys=["removeme"],
        )
        assert "Tags" in resp
        assert all(t["Key"] != "removeme" for t in resp["Tags"])

    def test_update_cluster(self, dax, cluster):
        resp = dax.update_cluster(
            ClusterName=cluster["name"],
            Description="updated-description",
        )
        assert resp["Cluster"]["ClusterName"] == cluster["name"]

    def test_increase_replication_factor(self, dax):
        name = _unique("dax")
        dax.create_cluster(
            ClusterName=name,
            NodeType="dax.r4.large",
            ReplicationFactor=1,
            IamRoleArn="arn:aws:iam::123456789012:role/DAXRole",
        )
        try:
            resp = dax.increase_replication_factor(
                ClusterName=name,
                NewReplicationFactor=3,
            )
            assert resp["Cluster"]["ClusterName"] == name
            assert "TotalNodes" in resp["Cluster"]
        finally:
            try:
                dax.delete_cluster(ClusterName=name)
            except Exception:
                pass

    def test_describe_clusters_by_name(self, dax, cluster):
        resp = dax.describe_clusters(ClusterNames=[cluster["name"]])
        assert "Clusters" in resp
        assert len(resp["Clusters"]) >= 1

    def test_describe_events(self, dax):
        resp = dax.describe_events()
        assert "Events" in resp
        assert isinstance(resp["Events"], list)

    def test_describe_default_parameters(self, dax):
        resp = dax.describe_default_parameters()
        assert "Parameters" in resp
        assert isinstance(resp["Parameters"], list)

    def test_reboot_node_not_found(self, dax, cluster):
        with pytest.raises(dax.exceptions.NodeNotFoundFault):
            dax.reboot_node(ClusterName=cluster["name"], NodeId="fake-node-id")


class TestDAXParameterGroupOperations:
    @pytest.fixture
    def param_group(self, dax):
        name = _unique("pg")
        resp = dax.create_parameter_group(
            ParameterGroupName=name,
            Description="test param group",
        )
        yield {"name": name, "resp": resp}
        try:
            dax.delete_parameter_group(ParameterGroupName=name)
        except Exception:
            pass

    def test_create_parameter_group(self, dax):
        name = _unique("pg")
        resp = dax.create_parameter_group(
            ParameterGroupName=name,
            Description="test create",
        )
        assert resp["ParameterGroup"]["ParameterGroupName"] == name
        dax.delete_parameter_group(ParameterGroupName=name)

    def test_describe_parameter_groups(self, dax, param_group):
        resp = dax.describe_parameter_groups()
        assert "ParameterGroups" in resp
        assert len(resp["ParameterGroups"]) >= 1

    def test_describe_parameter_groups_filtered(self, dax, param_group):
        resp = dax.describe_parameter_groups(
            ParameterGroupNames=[param_group["name"]],
        )
        assert len(resp["ParameterGroups"]) == 1
        assert resp["ParameterGroups"][0]["ParameterGroupName"] == param_group["name"]

    def test_describe_parameters(self, dax, param_group):
        resp = dax.describe_parameters(
            ParameterGroupName=param_group["name"],
        )
        assert "Parameters" in resp
        assert isinstance(resp["Parameters"], list)

    def test_update_parameter_group(self, dax, param_group):
        resp = dax.update_parameter_group(
            ParameterGroupName=param_group["name"],
            ParameterNameValues=[
                {"ParameterName": "query-ttl-millis", "ParameterValue": "100000"},
            ],
        )
        assert resp["ParameterGroup"]["ParameterGroupName"] == param_group["name"]

    def test_delete_parameter_group(self, dax):
        name = _unique("pg")
        dax.create_parameter_group(ParameterGroupName=name)
        resp = dax.delete_parameter_group(ParameterGroupName=name)
        assert "DeletionMessage" in resp
        assert name in resp["DeletionMessage"]

    def test_delete_parameter_group_not_found(self, dax):
        with pytest.raises(dax.exceptions.ParameterGroupNotFoundFault):
            dax.delete_parameter_group(ParameterGroupName="nonexistent-pg-xyz")


class TestDAXSubnetGroupOperations:
    @pytest.fixture
    def subnet_group(self, dax):
        name = _unique("sg")
        resp = dax.create_subnet_group(
            SubnetGroupName=name,
            SubnetIds=["subnet-00000000"],
            Description="test subnet group",
        )
        yield {"name": name, "resp": resp}
        try:
            dax.delete_subnet_group(SubnetGroupName=name)
        except Exception:
            pass

    def test_create_subnet_group(self, dax):
        name = _unique("sg")
        resp = dax.create_subnet_group(
            SubnetGroupName=name,
            SubnetIds=["subnet-00000000"],
        )
        assert resp["SubnetGroup"]["SubnetGroupName"] == name
        dax.delete_subnet_group(SubnetGroupName=name)

    def test_describe_subnet_groups(self, dax, subnet_group):
        resp = dax.describe_subnet_groups()
        assert "SubnetGroups" in resp
        assert len(resp["SubnetGroups"]) >= 1

    def test_describe_subnet_groups_filtered(self, dax, subnet_group):
        resp = dax.describe_subnet_groups(
            SubnetGroupNames=[subnet_group["name"]],
        )
        assert len(resp["SubnetGroups"]) == 1
        assert resp["SubnetGroups"][0]["SubnetGroupName"] == subnet_group["name"]

    def test_update_subnet_group(self, dax, subnet_group):
        resp = dax.update_subnet_group(
            SubnetGroupName=subnet_group["name"],
            Description="updated description",
        )
        assert resp["SubnetGroup"]["SubnetGroupName"] == subnet_group["name"]

    def test_delete_subnet_group(self, dax):
        name = _unique("sg")
        dax.create_subnet_group(
            SubnetGroupName=name,
            SubnetIds=["subnet-00000000"],
        )
        resp = dax.delete_subnet_group(SubnetGroupName=name)
        assert "DeletionMessage" in resp
        assert name in resp["DeletionMessage"]

    def test_delete_subnet_group_not_found(self, dax):
        with pytest.raises(dax.exceptions.SubnetGroupNotFoundFault):
            dax.delete_subnet_group(SubnetGroupName="nonexistent-sg-xyz")
