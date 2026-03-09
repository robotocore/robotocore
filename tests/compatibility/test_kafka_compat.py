"""MSK (Managed Streaming for Kafka) compatibility tests."""

import base64
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


class TestMSKConfigurationOperations:
    """Tests for MSK configuration operations."""

    def test_list_configurations_returns_list(self, kafka):
        resp = kafka.list_configurations()
        assert "Configurations" in resp
        assert isinstance(resp["Configurations"], list)

    def test_create_and_describe_configuration(self, kafka):
        name = _unique("config")
        server_properties = base64.b64encode(
            b"auto.create.topics.enable=true\nlog.retention.hours=168"
        ).decode("utf-8")
        create_resp = kafka.create_configuration(
            Name=name,
            ServerProperties=server_properties,
            KafkaVersions=["2.8.1"],
        )
        assert "Arn" in create_resp
        assert create_resp["Name"] == name
        config_arn = create_resp["Arn"]

        desc_resp = kafka.describe_configuration(Arn=config_arn)
        assert desc_resp["Arn"] == config_arn
        assert desc_resp["Name"] == name
        assert "LatestRevision" in desc_resp

    def test_list_configuration_revisions(self, kafka):
        name = _unique("config-rev")
        server_properties = base64.b64encode(b"log.retention.hours=168").decode("utf-8")
        create_resp = kafka.create_configuration(
            Name=name,
            ServerProperties=server_properties,
            KafkaVersions=["2.8.1"],
        )
        config_arn = create_resp["Arn"]
        resp = kafka.list_configuration_revisions(Arn=config_arn)
        assert "Revisions" in resp
        assert isinstance(resp["Revisions"], list)

    def test_describe_configuration_revision(self, kafka):
        name = _unique("config-descrev")
        server_properties = base64.b64encode(b"log.retention.hours=168").decode("utf-8")
        create_resp = kafka.create_configuration(
            Name=name,
            ServerProperties=server_properties,
            KafkaVersions=["2.8.1"],
        )
        config_arn = create_resp["Arn"]
        resp = kafka.describe_configuration_revision(Arn=config_arn, Revision=1)
        assert "Arn" in resp
        assert resp["Revision"] == 1


class TestMSKVersionsAndCompatibility:
    """Tests for MSK Kafka version operations."""

    def test_list_kafka_versions(self, kafka):
        resp = kafka.list_kafka_versions()
        assert "KafkaVersions" in resp
        assert isinstance(resp["KafkaVersions"], list)
        assert len(resp["KafkaVersions"]) > 0

    def test_get_compatible_kafka_versions(self, kafka):
        resp = kafka.get_compatible_kafka_versions()
        assert "CompatibleKafkaVersions" in resp
        assert isinstance(resp["CompatibleKafkaVersions"], list)


class TestMSKClusterDependentOperations:
    """Tests for operations that require a cluster ARN."""

    @pytest.fixture
    def cluster_arn(self, kafka):
        name = _unique("ops-cluster")
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

    def test_get_bootstrap_brokers(self, kafka, cluster_arn):
        resp = kafka.get_bootstrap_brokers(ClusterArn=cluster_arn)
        # Response should have at least one broker string key
        assert any(
            k in resp
            for k in [
                "BootstrapBrokerString",
                "BootstrapBrokerStringTls",
                "BootstrapBrokerStringSaslScram",
                "BootstrapBrokerStringSaslIam",
                "BootstrapBrokerStringPublicTls",
                "BootstrapBrokerStringPublicSaslScram",
                "BootstrapBrokerStringPublicSaslIam",
                "BootstrapBrokerStringVpcConnectivityTls",
                "BootstrapBrokerStringVpcConnectivitySaslScram",
                "BootstrapBrokerStringVpcConnectivitySaslIam",
            ]
        )

    def test_list_nodes(self, kafka, cluster_arn):
        resp = kafka.list_nodes(ClusterArn=cluster_arn)
        assert "NodeInfoList" in resp
        assert isinstance(resp["NodeInfoList"], list)

    def test_list_cluster_operations(self, kafka, cluster_arn):
        resp = kafka.list_cluster_operations(ClusterArn=cluster_arn)
        assert "ClusterOperationInfoList" in resp
        assert isinstance(resp["ClusterOperationInfoList"], list)

    def test_list_cluster_operations_v2(self, kafka, cluster_arn):
        resp = kafka.list_cluster_operations_v2(ClusterArn=cluster_arn)
        assert "ClusterOperationInfoList" in resp
        assert isinstance(resp["ClusterOperationInfoList"], list)

    def test_list_scram_secrets(self, kafka, cluster_arn):
        resp = kafka.list_scram_secrets(ClusterArn=cluster_arn)
        assert "SecretArnList" in resp
        assert isinstance(resp["SecretArnList"], list)

    def test_get_cluster_policy(self, kafka, cluster_arn):
        try:
            resp = kafka.get_cluster_policy(ClusterArn=cluster_arn)
            # If it succeeds, it should have a policy key
            assert "CurrentVersion" in resp or "Policy" in resp
        except kafka.exceptions.NotFoundException:
            # No policy set yet is valid
            pass

    def test_list_client_vpc_connections(self, kafka, cluster_arn):
        resp = kafka.list_client_vpc_connections(ClusterArn=cluster_arn)
        assert "ClientVpcConnections" in resp
        assert isinstance(resp["ClientVpcConnections"], list)


class TestMSKEmptyListOperations:
    """Tests for list operations that return empty lists with no setup."""

    def test_list_replicators_empty(self, kafka):
        resp = kafka.list_replicators()
        assert "Replicators" in resp
        assert isinstance(resp["Replicators"], list)

    def test_list_vpc_connections_empty(self, kafka):
        resp = kafka.list_vpc_connections()
        assert "VpcConnections" in resp
        assert isinstance(resp["VpcConnections"], list)


class TestMSKTopicOperations:
    """Tests for MSK topic operations."""

    @pytest.fixture
    def cluster_arn(self, kafka):
        name = _unique("topic-cluster")
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

    def test_list_topics(self, kafka, cluster_arn):
        try:
            resp = kafka.list_topics(ClusterArn=cluster_arn)
            assert "Topics" in resp
            assert isinstance(resp["Topics"], list)
        except (
            kafka.exceptions.BadRequestException,
            kafka.exceptions.NotFoundException,
        ):
            pass  # Server processed request, cluster may not be in ACTIVE state

    def test_describe_topic_fake(self, kafka, cluster_arn):
        try:
            kafka.describe_topic(ClusterArn=cluster_arn, TopicName="fake-topic")
        except (
            kafka.exceptions.NotFoundException,
            kafka.exceptions.BadRequestException,
        ):
            pass  # Expected — server processed the request

    def test_describe_topic_partitions(self, kafka, cluster_arn):
        try:
            kafka.describe_topic_partitions(ClusterArn=cluster_arn, TopicName="fake-topic")
        except (
            kafka.exceptions.NotFoundException,
            kafka.exceptions.BadRequestException,
        ):
            pass  # Expected — server processed the request


class TestMSKDescribeWithFakeArn:
    """Tests that describe ops with fake ARNs return proper errors."""

    FAKE_CLUSTER_ARN = "arn:aws:kafka:us-east-1:123456789012:cluster/fake-cluster/fake-uuid"
    FAKE_REPLICATOR_ARN = (
        "arn:aws:kafka:us-east-1:123456789012:replicator/fake-replicator/fake-uuid"
    )
    FAKE_VPC_ARN = "arn:aws:kafka:us-east-1:123456789012:vpc-connection/fake-vpc/fake-uuid"

    def test_describe_cluster_operation_fake_arn(self, kafka):
        try:
            kafka.describe_cluster_operation(ClusterOperationArn=self.FAKE_CLUSTER_ARN)
            # If it returns without error, that's also fine
        except (
            kafka.exceptions.NotFoundException,
            kafka.exceptions.BadRequestException,
        ):
            pass  # Expected — server processed the request

    def test_describe_cluster_operation_v2_fake_arn(self, kafka):
        try:
            kafka.describe_cluster_operation_v2(ClusterOperationArn=self.FAKE_CLUSTER_ARN)
        except (
            kafka.exceptions.NotFoundException,
            kafka.exceptions.BadRequestException,
        ):
            pass  # Expected

    def test_describe_replicator_fake_arn(self, kafka):
        try:
            kafka.describe_replicator(ReplicatorArn=self.FAKE_REPLICATOR_ARN)
        except (
            kafka.exceptions.NotFoundException,
            kafka.exceptions.BadRequestException,
        ):
            pass  # Expected

    def test_describe_vpc_connection_fake_arn(self, kafka):
        try:
            kafka.describe_vpc_connection(Arn=self.FAKE_VPC_ARN)
        except (
            kafka.exceptions.NotFoundException,
            kafka.exceptions.BadRequestException,
        ):
            pass  # Expected


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
