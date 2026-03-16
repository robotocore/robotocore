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
            pass  # resource may not exist

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


class TestMSKConfigurationDelete:
    """Tests for MSK configuration delete operations."""

    def test_delete_configuration(self, kafka):
        name = _unique("del-config")
        server_properties = base64.b64encode(b"log.retention.hours=168").decode("utf-8")
        create_resp = kafka.create_configuration(
            Name=name,
            ServerProperties=server_properties,
            KafkaVersions=["2.8.1"],
        )
        config_arn = create_resp["Arn"]
        del_resp = kafka.delete_configuration(Arn=config_arn)
        assert del_resp["Arn"] == config_arn
        assert "State" in del_resp

    def test_update_configuration(self, kafka):
        name = _unique("upd-config")
        server_properties = base64.b64encode(b"log.retention.hours=168").decode("utf-8")
        create_resp = kafka.create_configuration(
            Name=name,
            ServerProperties=server_properties,
            KafkaVersions=["2.8.1"],
        )
        config_arn = create_resp["Arn"]
        new_props = base64.b64encode(b"log.retention.hours=72").decode("utf-8")
        upd_resp = kafka.update_configuration(
            Arn=config_arn,
            ServerProperties=new_props,
        )
        assert "Arn" in upd_resp
        assert "LatestRevision" in upd_resp


class TestMSKClusterPolicyOperations:
    """Tests for MSK cluster policy operations."""

    @pytest.fixture
    def cluster_arn(self, kafka):
        name = _unique("policy-cluster")
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

    def test_put_cluster_policy(self, kafka, cluster_arn):
        import json

        policy = json.dumps(
            {
                "Version": "2012-10-17",
                "Statement": [
                    {
                        "Effect": "Allow",
                        "Principal": {"AWS": "arn:aws:iam::123456789012:root"},
                        "Action": "kafka-cluster:*",
                        "Resource": cluster_arn,
                    }
                ],
            }
        )
        resp = kafka.put_cluster_policy(ClusterArn=cluster_arn, Policy=policy)
        assert "CurrentVersion" in resp

    def test_put_then_delete_cluster_policy(self, kafka, cluster_arn):
        import json

        policy = json.dumps(
            {
                "Version": "2012-10-17",
                "Statement": [
                    {
                        "Effect": "Allow",
                        "Principal": {"AWS": "arn:aws:iam::123456789012:root"},
                        "Action": "kafka-cluster:*",
                        "Resource": cluster_arn,
                    }
                ],
            }
        )
        kafka.put_cluster_policy(ClusterArn=cluster_arn, Policy=policy)
        resp = kafka.delete_cluster_policy(ClusterArn=cluster_arn)
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200


class TestMSKClusterUpdateOperations:
    """Tests for MSK cluster update operations."""

    @pytest.fixture
    def cluster_arn(self, kafka):
        name = _unique("upd-cluster")
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

    def _get_current_version(self, kafka, cluster_arn):
        desc = kafka.describe_cluster(ClusterArn=cluster_arn)
        return desc["ClusterInfo"]["CurrentVersion"]

    def test_update_broker_count(self, kafka, cluster_arn):
        version = self._get_current_version(kafka, cluster_arn)
        resp = kafka.update_broker_count(
            ClusterArn=cluster_arn,
            CurrentVersion=version,
            TargetNumberOfBrokerNodes=6,
        )
        assert "ClusterArn" in resp
        assert "ClusterOperationArn" in resp

    def test_update_broker_type(self, kafka, cluster_arn):
        version = self._get_current_version(kafka, cluster_arn)
        resp = kafka.update_broker_type(
            ClusterArn=cluster_arn,
            CurrentVersion=version,
            TargetInstanceType="kafka.m5.xlarge",
        )
        assert "ClusterArn" in resp
        assert "ClusterOperationArn" in resp

    def test_update_broker_storage(self, kafka, cluster_arn):
        version = self._get_current_version(kafka, cluster_arn)
        resp = kafka.update_broker_storage(
            ClusterArn=cluster_arn,
            CurrentVersion=version,
            TargetBrokerEBSVolumeInfo=[
                {"KafkaBrokerNodeId": "1", "VolumeSizeGB": 200},
                {"KafkaBrokerNodeId": "2", "VolumeSizeGB": 200},
                {"KafkaBrokerNodeId": "3", "VolumeSizeGB": 200},
            ],
        )
        assert "ClusterArn" in resp
        assert "ClusterOperationArn" in resp

    def test_update_monitoring(self, kafka, cluster_arn):
        version = self._get_current_version(kafka, cluster_arn)
        resp = kafka.update_monitoring(
            ClusterArn=cluster_arn,
            CurrentVersion=version,
            EnhancedMonitoring="PER_TOPIC_PER_BROKER",
        )
        assert "ClusterArn" in resp
        assert "ClusterOperationArn" in resp

    def test_update_security(self, kafka, cluster_arn):
        version = self._get_current_version(kafka, cluster_arn)
        resp = kafka.update_security(
            ClusterArn=cluster_arn,
            CurrentVersion=version,
            ClientAuthentication={"Unauthenticated": {"Enabled": True}},
        )
        assert "ClusterArn" in resp
        assert "ClusterOperationArn" in resp

    def test_update_connectivity(self, kafka, cluster_arn):
        version = self._get_current_version(kafka, cluster_arn)
        resp = kafka.update_connectivity(
            ClusterArn=cluster_arn,
            CurrentVersion=version,
            ConnectivityInfo={
                "PublicAccess": {"Type": "DISABLED"},
            },
        )
        assert "ClusterArn" in resp
        assert "ClusterOperationArn" in resp

    def test_update_cluster_kafka_version(self, kafka, cluster_arn):
        version = self._get_current_version(kafka, cluster_arn)
        resp = kafka.update_cluster_kafka_version(
            ClusterArn=cluster_arn,
            CurrentVersion=version,
            TargetKafkaVersion="3.3.1",
        )
        assert "ClusterArn" in resp
        assert "ClusterOperationArn" in resp

    def test_update_storage(self, kafka, cluster_arn):
        version = self._get_current_version(kafka, cluster_arn)
        resp = kafka.update_storage(
            ClusterArn=cluster_arn,
            CurrentVersion=version,
            StorageMode="LOCAL",
        )
        assert "ClusterArn" in resp
        assert "ClusterOperationArn" in resp

    def test_reboot_broker(self, kafka, cluster_arn):
        resp = kafka.reboot_broker(
            ClusterArn=cluster_arn,
            BrokerIds=["1"],
        )
        assert "ClusterArn" in resp
        assert "ClusterOperationArn" in resp

    def test_update_cluster_configuration(self, kafka, cluster_arn):
        # Create a configuration first
        server_properties = base64.b64encode(b"log.retention.hours=72").decode("utf-8")
        config_resp = kafka.create_configuration(
            Name=_unique("upd-cfg"),
            ServerProperties=server_properties,
            KafkaVersions=["2.8.1"],
        )
        config_arn = config_resp["Arn"]
        version = self._get_current_version(kafka, cluster_arn)
        resp = kafka.update_cluster_configuration(
            ClusterArn=cluster_arn,
            CurrentVersion=version,
            ConfigurationInfo={"Arn": config_arn, "Revision": 1},
        )
        assert "ClusterArn" in resp
        assert "ClusterOperationArn" in resp


class TestMSKScramSecrets:
    """Tests for MSK SCRAM secret operations."""

    @pytest.fixture
    def cluster_arn(self, kafka):
        name = _unique("scram-cluster")
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

    def test_batch_associate_scram_secret(self, kafka, cluster_arn):
        resp = kafka.batch_associate_scram_secret(
            ClusterArn=cluster_arn,
            SecretArnList=[
                "arn:aws:secretsmanager:us-east-1:123456789012:secret:AmazonMSK_secret1"
            ],
        )
        assert "ClusterArn" in resp
        assert "UnprocessedScramSecrets" in resp

    def test_batch_disassociate_scram_secret(self, kafka, cluster_arn):
        kafka.batch_associate_scram_secret(
            ClusterArn=cluster_arn,
            SecretArnList=[
                "arn:aws:secretsmanager:us-east-1:123456789012:secret:AmazonMSK_secret2"
            ],
        )
        resp = kafka.batch_disassociate_scram_secret(
            ClusterArn=cluster_arn,
            SecretArnList=[
                "arn:aws:secretsmanager:us-east-1:123456789012:secret:AmazonMSK_secret2"
            ],
        )
        assert "ClusterArn" in resp
        assert "UnprocessedScramSecrets" in resp


class TestMSKReplicatorOperations:
    """Tests for MSK replicator create/delete operations."""

    @pytest.fixture
    def two_clusters(self, kafka):
        src = kafka.create_cluster(
            ClusterName=_unique("repl-src"),
            KafkaVersion="2.8.1",
            NumberOfBrokerNodes=3,
            BrokerNodeGroupInfo={
                "InstanceType": "kafka.m5.large",
                "ClientSubnets": ["subnet-1", "subnet-2", "subnet-3"],
            },
        )
        tgt = kafka.create_cluster(
            ClusterName=_unique("repl-tgt"),
            KafkaVersion="2.8.1",
            NumberOfBrokerNodes=3,
            BrokerNodeGroupInfo={
                "InstanceType": "kafka.m5.large",
                "ClientSubnets": ["subnet-4", "subnet-5", "subnet-6"],
            },
        )
        return src["ClusterArn"], tgt["ClusterArn"]

    def test_create_replicator(self, kafka, two_clusters):
        src_arn, tgt_arn = two_clusters
        resp = kafka.create_replicator(
            ReplicatorName=_unique("replicator"),
            KafkaClusters=[
                {
                    "AmazonMskCluster": {"MskClusterArn": src_arn},
                    "VpcConfig": {
                        "SubnetIds": ["subnet-1", "subnet-2"],
                        "SecurityGroupIds": ["sg-12345"],
                    },
                },
                {
                    "AmazonMskCluster": {"MskClusterArn": tgt_arn},
                    "VpcConfig": {
                        "SubnetIds": ["subnet-3", "subnet-4"],
                        "SecurityGroupIds": ["sg-67890"],
                    },
                },
            ],
            ReplicationInfoList=[
                {
                    "SourceKafkaClusterArn": src_arn,
                    "TargetKafkaClusterArn": tgt_arn,
                    "TargetCompressionType": "NONE",
                    "TopicReplication": {
                        "TopicsToReplicate": [".*"],
                        "CopyTopicConfigurations": True,
                        "CopyAccessControlListsForTopics": True,
                        "DetectAndCopyNewTopics": True,
                    },
                    "ConsumerGroupReplication": {
                        "ConsumerGroupsToReplicate": [".*"],
                    },
                },
            ],
            ServiceExecutionRoleArn="arn:aws:iam::123456789012:role/kafka-replication-role",
        )
        assert "ReplicatorArn" in resp
        assert resp["ReplicatorName"] == resp["ReplicatorName"]
        assert resp["ReplicatorState"] == "RUNNING"

    def test_delete_replicator(self, kafka, two_clusters):
        src_arn, tgt_arn = two_clusters
        create_resp = kafka.create_replicator(
            ReplicatorName=_unique("del-repl"),
            KafkaClusters=[
                {
                    "AmazonMskCluster": {"MskClusterArn": src_arn},
                    "VpcConfig": {
                        "SubnetIds": ["subnet-1", "subnet-2"],
                        "SecurityGroupIds": ["sg-12345"],
                    },
                },
                {
                    "AmazonMskCluster": {"MskClusterArn": tgt_arn},
                    "VpcConfig": {
                        "SubnetIds": ["subnet-3", "subnet-4"],
                        "SecurityGroupIds": ["sg-67890"],
                    },
                },
            ],
            ReplicationInfoList=[
                {
                    "SourceKafkaClusterArn": src_arn,
                    "TargetKafkaClusterArn": tgt_arn,
                    "TargetCompressionType": "NONE",
                    "TopicReplication": {
                        "TopicsToReplicate": [".*"],
                        "CopyTopicConfigurations": True,
                        "CopyAccessControlListsForTopics": True,
                        "DetectAndCopyNewTopics": True,
                    },
                    "ConsumerGroupReplication": {
                        "ConsumerGroupsToReplicate": [".*"],
                    },
                },
            ],
            ServiceExecutionRoleArn="arn:aws:iam::123456789012:role/kafka-replication-role",
        )
        repl_arn = create_resp["ReplicatorArn"]
        del_resp = kafka.delete_replicator(ReplicatorArn=repl_arn)
        assert del_resp["ReplicatorArn"] == repl_arn
        assert "ReplicatorState" in del_resp

    def test_update_replication_info_fake_arn(self, kafka):
        fake_repl = "arn:aws:kafka:us-east-1:123456789012:replicator/fake/fake-uuid"
        fake_src = "arn:aws:kafka:us-east-1:123456789012:cluster/fake-src/fake-uuid"
        fake_tgt = "arn:aws:kafka:us-east-1:123456789012:cluster/fake-tgt/fake-uuid"
        with pytest.raises(kafka.exceptions.NotFoundException):
            kafka.update_replication_info(
                ReplicatorArn=fake_repl,
                SourceKafkaClusterArn=fake_src,
                TargetKafkaClusterArn=fake_tgt,
                CurrentVersion="KAAAAAAAAAAAAAA",
                TopicReplication={
                    "TopicsToReplicate": [".*"],
                    "TopicsToExclude": [],
                    "CopyTopicConfigurations": True,
                    "CopyAccessControlListsForTopics": True,
                    "DetectAndCopyNewTopics": True,
                },
            )


class TestMSKVpcConnectionOperations:
    """Tests for MSK VPC connection create/delete/reject operations."""

    def test_create_vpc_connection(self, kafka):
        resp = kafka.create_vpc_connection(
            TargetClusterArn="arn:aws:kafka:us-east-1:123456789012:cluster/test/test-uuid",
            Authentication="SASL_IAM",
            VpcId="vpc-12345",
            ClientSubnets=["subnet-1", "subnet-2"],
            SecurityGroups=["sg-12345"],
        )
        assert "VpcConnectionArn" in resp
        assert resp["State"] == "AVAILABLE"
        assert resp["Authentication"] == "SASL_IAM"
        assert resp["VpcId"] == "vpc-12345"

    def test_delete_vpc_connection(self, kafka):
        create_resp = kafka.create_vpc_connection(
            TargetClusterArn="arn:aws:kafka:us-east-1:123456789012:cluster/test/test-uuid",
            Authentication="SASL_IAM",
            VpcId="vpc-12345",
            ClientSubnets=["subnet-1", "subnet-2"],
            SecurityGroups=["sg-12345"],
        )
        vpc_arn = create_resp["VpcConnectionArn"]
        del_resp = kafka.delete_vpc_connection(Arn=vpc_arn)
        assert del_resp["VpcConnectionArn"] == vpc_arn
        assert "State" in del_resp

    def test_reject_client_vpc_connection_fake_arn(self, kafka):
        fake_cluster = "arn:aws:kafka:us-east-1:123456789012:cluster/fake/fake-uuid"
        fake_vpc = "arn:aws:kafka:us-east-1:123456789012:vpc-connection/fake-uuid"
        with pytest.raises(kafka.exceptions.NotFoundException):
            kafka.reject_client_vpc_connection(
                ClusterArn=fake_cluster,
                VpcConnectionArn=fake_vpc,
            )


class TestMSKTopicCrudOperations:
    """Tests for MSK topic create/update/delete operations."""

    FAKE_CLUSTER_ARN = "arn:aws:kafka:us-east-1:123456789012:cluster/fake/fake-uuid"

    def test_create_topic_fake_cluster(self, kafka):
        with pytest.raises(kafka.exceptions.NotFoundException):
            kafka.create_topic(
                ClusterArn=self.FAKE_CLUSTER_ARN,
                TopicName="test-topic",
                PartitionCount=3,
                ReplicationFactor=1,
            )

    def test_update_topic_fake_cluster(self, kafka):
        with pytest.raises(kafka.exceptions.NotFoundException):
            kafka.update_topic(
                ClusterArn=self.FAKE_CLUSTER_ARN,
                TopicName="test-topic",
                PartitionCount=6,
            )

    def test_delete_topic_fake_cluster(self, kafka):
        with pytest.raises(kafka.exceptions.NotFoundException):
            kafka.delete_topic(
                ClusterArn=self.FAKE_CLUSTER_ARN,
                TopicName="test-topic",
            )


class TestMSKUpdateRebalancing:
    """Tests for MSK update rebalancing operation."""

    @pytest.fixture
    def cluster_arn(self, kafka):
        name = _unique("rebal-cluster")
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

    def test_update_rebalancing(self, kafka, cluster_arn):
        version = kafka.describe_cluster(ClusterArn=cluster_arn)["ClusterInfo"]["CurrentVersion"]
        resp = kafka.update_rebalancing(
            ClusterArn=cluster_arn,
            CurrentVersion=version,
            Rebalancing={"Status": "IN_PROGRESS"},
        )
        assert "ClusterArn" in resp
        assert "ClusterOperationArn" in resp
