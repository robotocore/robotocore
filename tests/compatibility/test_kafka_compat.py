"""MSK (Managed Streaming for Kafka) compatibility tests."""

import uuid

import pytest

from tests.compatibility.conftest import make_client


@pytest.fixture
def kafka():
    return make_client("kafka")


def _unique(prefix):
    return f"{prefix}-{uuid.uuid4().hex[:8]}"


class TestMSKClusterOperations:
    """Tests for MSK cluster create and list operations."""

    def test_create_cluster(self, kafka):
        name = _unique("cluster")
        resp = kafka.create_cluster(
            ClusterName=name,
            KafkaVersion="2.8.1",
            NumberOfBrokerNodes=3,
            BrokerNodeGroupInfo={
                "InstanceType": "kafka.m5.large",
                "ClientSubnets": ["subnet-1", "subnet-2", "subnet-3"],
            },
        )
        assert "ClusterArn" in resp
        assert resp["State"] == "CREATING"
        assert resp["ClusterName"] == name

    def test_create_cluster_v2(self, kafka):
        name = _unique("cluster-v2")
        resp = kafka.create_cluster_v2(
            ClusterName=name,
            Provisioned={
                "BrokerNodeGroupInfo": {
                    "InstanceType": "kafka.m5.large",
                    "ClientSubnets": ["subnet-1", "subnet-2", "subnet-3"],
                },
                "KafkaVersion": "2.8.1",
                "NumberOfBrokerNodes": 3,
            },
        )
        assert "ClusterArn" in resp
        assert resp["State"] == "CREATING"
        assert resp["ClusterName"] == name
        assert resp["ClusterType"] == "PROVISIONED"

    def test_list_clusters_returns_created(self, kafka):
        name = _unique("cluster")
        kafka.create_cluster(
            ClusterName=name,
            KafkaVersion="2.8.1",
            NumberOfBrokerNodes=3,
            BrokerNodeGroupInfo={
                "InstanceType": "kafka.m5.large",
                "ClientSubnets": ["subnet-1", "subnet-2", "subnet-3"],
            },
        )
        resp = kafka.list_clusters()
        names = [c["ClusterName"] for c in resp["ClusterInfoList"]]
        assert name in names

    def test_list_clusters_v2_returns_created(self, kafka):
        name = _unique("cluster-v2")
        kafka.create_cluster_v2(
            ClusterName=name,
            Provisioned={
                "BrokerNodeGroupInfo": {
                    "InstanceType": "kafka.m5.large",
                    "ClientSubnets": ["subnet-1", "subnet-2", "subnet-3"],
                },
                "KafkaVersion": "2.8.1",
                "NumberOfBrokerNodes": 3,
            },
        )
        resp = kafka.list_clusters_v2()
        names = [c["ClusterName"] for c in resp["ClusterInfoList"]]
        assert name in names

    def test_list_clusters_v2_has_provisioned_info(self, kafka):
        name = _unique("cluster-v2")
        kafka.create_cluster_v2(
            ClusterName=name,
            Provisioned={
                "BrokerNodeGroupInfo": {
                    "InstanceType": "kafka.m5.large",
                    "ClientSubnets": ["subnet-1", "subnet-2", "subnet-3"],
                },
                "KafkaVersion": "2.8.1",
                "NumberOfBrokerNodes": 3,
            },
        )
        resp = kafka.list_clusters_v2()
        cluster = next(c for c in resp["ClusterInfoList"] if c["ClusterName"] == name)
        assert cluster["ClusterType"] == "PROVISIONED"
        assert "Provisioned" in cluster
        prov = cluster["Provisioned"]
        assert prov["BrokerNodeGroupInfo"]["InstanceType"] == "kafka.m5.large"
        assert prov["NumberOfBrokerNodes"] == 3


class TestMSKDescribeCluster:
    """Tests for MSK describe cluster operations."""

    @pytest.fixture
    def cluster_with_arn(self, kafka):
        name = _unique("desc-cluster")
        resp = kafka.create_cluster(
            ClusterName=name,
            KafkaVersion="2.8.1",
            NumberOfBrokerNodes=3,
            BrokerNodeGroupInfo={
                "InstanceType": "kafka.m5.large",
                "ClientSubnets": ["subnet-1", "subnet-2", "subnet-3"],
            },
        )
        return name, resp["ClusterArn"]

    def test_describe_cluster_returns_cluster_info(self, kafka, cluster_with_arn):
        name, arn = cluster_with_arn
        resp = kafka.describe_cluster(ClusterArn=arn)
        info = resp["ClusterInfo"]
        assert info["ClusterName"] == name
        assert info["ClusterArn"] == arn
        assert info["State"] == "CREATING"

    def test_describe_cluster_has_broker_info(self, kafka, cluster_with_arn):
        _name, arn = cluster_with_arn
        resp = kafka.describe_cluster(ClusterArn=arn)
        info = resp["ClusterInfo"]
        assert info["BrokerNodeGroupInfo"]["InstanceType"] == "kafka.m5.large"
        assert info["NumberOfBrokerNodes"] == 3

    def test_describe_cluster_has_kafka_version(self, kafka, cluster_with_arn):
        _name, arn = cluster_with_arn
        resp = kafka.describe_cluster(ClusterArn=arn)
        info = resp["ClusterInfo"]
        assert info["CurrentBrokerSoftwareInfo"]["KafkaVersion"] == "2.8.1"

    def test_describe_cluster_has_zookeeper_string(self, kafka, cluster_with_arn):
        _name, arn = cluster_with_arn
        resp = kafka.describe_cluster(ClusterArn=arn)
        info = resp["ClusterInfo"]
        assert "ZookeeperConnectString" in info
        assert isinstance(info["ZookeeperConnectString"], str)

    def test_describe_cluster_has_current_version(self, kafka, cluster_with_arn):
        _name, arn = cluster_with_arn
        resp = kafka.describe_cluster(ClusterArn=arn)
        info = resp["ClusterInfo"]
        assert "CurrentVersion" in info

    def test_describe_cluster_v2_returns_cluster_info(self, kafka, cluster_with_arn):
        name, arn = cluster_with_arn
        resp = kafka.describe_cluster_v2(ClusterArn=arn)
        info = resp["ClusterInfo"]
        assert info["ClusterName"] == name
        assert info["ClusterArn"] == arn
        assert info["ClusterType"] == "PROVISIONED"
        assert info["State"] == "CREATING"

    def test_describe_cluster_v2_has_provisioned_section(self, kafka, cluster_with_arn):
        _name, arn = cluster_with_arn
        resp = kafka.describe_cluster_v2(ClusterArn=arn)
        info = resp["ClusterInfo"]
        assert "Provisioned" in info
        prov = info["Provisioned"]
        assert prov["BrokerNodeGroupInfo"]["InstanceType"] == "kafka.m5.large"
        assert prov["NumberOfBrokerNodes"] == 3
        assert prov["CurrentBrokerSoftwareInfo"]["KafkaVersion"] == "2.8.1"


class TestMSKDeleteCluster:
    """Tests for MSK delete cluster operations."""

    def test_delete_cluster_returns_arn(self, kafka):
        name = _unique("del-cluster")
        create_resp = kafka.create_cluster(
            ClusterName=name,
            KafkaVersion="2.8.1",
            NumberOfBrokerNodes=3,
            BrokerNodeGroupInfo={
                "InstanceType": "kafka.m5.large",
                "ClientSubnets": ["subnet-1", "subnet-2", "subnet-3"],
            },
        )
        arn = create_resp["ClusterArn"]
        del_resp = kafka.delete_cluster(ClusterArn=arn)
        assert del_resp["ClusterArn"] == arn
        assert "State" in del_resp

    def test_delete_cluster_removes_from_list(self, kafka):
        name = _unique("del-cluster")
        create_resp = kafka.create_cluster(
            ClusterName=name,
            KafkaVersion="2.8.1",
            NumberOfBrokerNodes=3,
            BrokerNodeGroupInfo={
                "InstanceType": "kafka.m5.large",
                "ClientSubnets": ["subnet-1", "subnet-2", "subnet-3"],
            },
        )
        arn = create_resp["ClusterArn"]
        kafka.delete_cluster(ClusterArn=arn)
        resp = kafka.list_clusters()
        arns = [c["ClusterArn"] for c in resp["ClusterInfoList"]]
        assert arn not in arns


class TestMSKServerlessCluster:
    """Tests for MSK serverless cluster operations via create_cluster_v2."""

    def test_create_serverless_cluster(self, kafka):
        name = _unique("serverless")
        resp = kafka.create_cluster_v2(
            ClusterName=name,
            Serverless={
                "VpcConfigs": [{"SubnetIds": ["subnet-1", "subnet-2"]}],
                "ClientAuthentication": {"Sasl": {"Iam": {"Enabled": True}}},
            },
        )
        assert "ClusterArn" in resp
        assert resp["State"] == "CREATING"
        assert resp["ClusterName"] == name
        assert resp["ClusterType"] == "SERVERLESS"

    def test_serverless_cluster_in_list_v2(self, kafka):
        name = _unique("serverless")
        kafka.create_cluster_v2(
            ClusterName=name,
            Serverless={
                "VpcConfigs": [{"SubnetIds": ["subnet-1", "subnet-2"]}],
                "ClientAuthentication": {"Sasl": {"Iam": {"Enabled": True}}},
            },
        )
        resp = kafka.list_clusters_v2()
        cluster = next((c for c in resp["ClusterInfoList"] if c["ClusterName"] == name), None)
        assert cluster is not None
        assert cluster["ClusterType"] == "SERVERLESS"

    def test_describe_serverless_cluster_v2(self, kafka):
        name = _unique("serverless")
        create_resp = kafka.create_cluster_v2(
            ClusterName=name,
            Serverless={
                "VpcConfigs": [{"SubnetIds": ["subnet-1", "subnet-2"]}],
                "ClientAuthentication": {"Sasl": {"Iam": {"Enabled": True}}},
            },
        )
        arn = create_resp["ClusterArn"]
        resp = kafka.describe_cluster_v2(ClusterArn=arn)
        info = resp["ClusterInfo"]
        assert info["ClusterName"] == name
        assert info["ClusterType"] == "SERVERLESS"
        assert "Serverless" in info

    def test_create_cluster_with_tags(self, kafka):
        name = _unique("tagged")
        resp = kafka.create_cluster(
            ClusterName=name,
            KafkaVersion="2.8.1",
            NumberOfBrokerNodes=3,
            BrokerNodeGroupInfo={
                "InstanceType": "kafka.m5.large",
                "ClientSubnets": ["subnet-1", "subnet-2", "subnet-3"],
            },
            Tags={"env": "staging", "project": "kafka-test"},
        )
        arn = resp["ClusterArn"]
        tags_resp = kafka.list_tags_for_resource(ResourceArn=arn)
        assert tags_resp["Tags"]["env"] == "staging"
        assert tags_resp["Tags"]["project"] == "kafka-test"

    def test_create_cluster_v2_with_tags(self, kafka):
        name = _unique("tagged-v2")
        resp = kafka.create_cluster_v2(
            ClusterName=name,
            Provisioned={
                "BrokerNodeGroupInfo": {
                    "InstanceType": "kafka.m5.large",
                    "ClientSubnets": ["subnet-1", "subnet-2", "subnet-3"],
                },
                "KafkaVersion": "2.8.1",
                "NumberOfBrokerNodes": 3,
            },
            Tags={"env": "prod"},
        )
        arn = resp["ClusterArn"]
        tags_resp = kafka.list_tags_for_resource(ResourceArn=arn)
        assert tags_resp["Tags"]["env"] == "prod"


class TestMSKListOperations:
    """Tests for MSK list operations."""

    def test_list_clusters_returns_list(self, kafka):
        resp = kafka.list_clusters()
        assert "ClusterInfoList" in resp
        assert isinstance(resp["ClusterInfoList"], list)

    def test_list_clusters_v2_returns_list(self, kafka):
        resp = kafka.list_clusters_v2()
        assert "ClusterInfoList" in resp
        assert isinstance(resp["ClusterInfoList"], list)


class TestMSKTags:
    """Tests for MSK tag operations on clusters."""

    @pytest.fixture
    def cluster_arn(self, kafka):
        name = _unique("tag-cluster")
        resp = kafka.create_cluster(
            ClusterName=name,
            KafkaVersion="2.8.1",
            NumberOfBrokerNodes=3,
            BrokerNodeGroupInfo={
                "InstanceType": "kafka.m5.large",
                "ClientSubnets": ["subnet-1", "subnet-2", "subnet-3"],
            },
        )
        return resp["ClusterArn"]

    def test_tag_resource(self, kafka, cluster_arn):
        kafka.tag_resource(ResourceArn=cluster_arn, Tags={"env": "test", "team": "platform"})
        resp = kafka.list_tags_for_resource(ResourceArn=cluster_arn)
        assert resp["Tags"]["env"] == "test"
        assert resp["Tags"]["team"] == "platform"

    def test_list_tags_empty(self, kafka, cluster_arn):
        resp = kafka.list_tags_for_resource(ResourceArn=cluster_arn)
        assert isinstance(resp["Tags"], dict)

    def test_untag_resource(self, kafka, cluster_arn):
        kafka.tag_resource(ResourceArn=cluster_arn, Tags={"a": "1", "b": "2", "c": "3"})
        kafka.untag_resource(ResourceArn=cluster_arn, TagKeys=["b"])
        resp = kafka.list_tags_for_resource(ResourceArn=cluster_arn)
        assert "a" in resp["Tags"]
        assert "b" not in resp["Tags"]
        assert "c" in resp["Tags"]

    def test_untag_multiple_keys(self, kafka, cluster_arn):
        kafka.tag_resource(ResourceArn=cluster_arn, Tags={"x": "1", "y": "2", "z": "3"})
        kafka.untag_resource(ResourceArn=cluster_arn, TagKeys=["x", "z"])
        resp = kafka.list_tags_for_resource(ResourceArn=cluster_arn)
        assert resp["Tags"] == {"y": "2"}

    def test_tag_overwrite(self, kafka, cluster_arn):
        kafka.tag_resource(ResourceArn=cluster_arn, Tags={"key": "old"})
        kafka.tag_resource(ResourceArn=cluster_arn, Tags={"key": "new"})
        resp = kafka.list_tags_for_resource(ResourceArn=cluster_arn)
        assert resp["Tags"]["key"] == "new"
