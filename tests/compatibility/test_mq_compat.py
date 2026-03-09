"""Amazon MQ compatibility tests."""

import uuid

import pytest

from tests.compatibility.conftest import make_client


@pytest.fixture
def mq():
    return make_client("mq")


def _unique(prefix):
    return f"{prefix}-{uuid.uuid4().hex[:8]}"


@pytest.fixture
def broker(mq):
    resp = mq.create_broker(
        BrokerName=_unique("broker"),
        EngineType="ACTIVEMQ",
        EngineVersion="5.17.6",
        HostInstanceType="mq.t3.micro",
        DeploymentMode="SINGLE_INSTANCE",
        PubliclyAccessible=False,
        Users=[{"Username": "admin", "Password": "Admin12345!"}],
    )
    broker_id = resp["BrokerId"]
    broker_arn = resp["BrokerArn"]
    yield {"BrokerId": broker_id, "BrokerArn": broker_arn}
    mq.delete_broker(BrokerId=broker_id)


@pytest.fixture
def configuration(mq):
    resp = mq.create_configuration(
        Name=_unique("config"),
        EngineType="ACTIVEMQ",
        EngineVersion="5.17.6",
    )
    return {"Id": resp["Id"], "Arn": resp["Arn"], "Name": resp["Name"]}


class TestMQBrokerOperations:
    def test_create_broker(self, mq):
        name = _unique("broker")
        resp = mq.create_broker(
            BrokerName=name,
            EngineType="ACTIVEMQ",
            EngineVersion="5.17.6",
            HostInstanceType="mq.t3.micro",
            DeploymentMode="SINGLE_INSTANCE",
            PubliclyAccessible=False,
            Users=[{"Username": "admin", "Password": "Admin12345!"}],
        )
        assert "BrokerId" in resp
        assert "BrokerArn" in resp
        mq.delete_broker(BrokerId=resp["BrokerId"])

    def test_describe_broker(self, mq, broker):
        resp = mq.describe_broker(BrokerId=broker["BrokerId"])
        assert resp["BrokerId"] == broker["BrokerId"]
        assert resp["BrokerArn"] == broker["BrokerArn"]
        assert resp["EngineType"] == "ACTIVEMQ"
        assert resp["EngineVersion"] == "5.17.6"
        assert resp["HostInstanceType"] == "mq.t3.micro"
        assert resp["DeploymentMode"] == "SINGLE_INSTANCE"
        assert resp["PubliclyAccessible"] is False

    def test_list_brokers(self, mq, broker):
        resp = mq.list_brokers()
        broker_ids = [b["BrokerId"] for b in resp["BrokerSummaries"]]
        assert broker["BrokerId"] in broker_ids

    def test_create_tags(self, mq, broker):
        mq.create_tags(
            ResourceArn=broker["BrokerArn"],
            Tags={"env": "test", "team": "dev"},
        )
        resp = mq.list_tags(ResourceArn=broker["BrokerArn"])
        assert resp["Tags"]["env"] == "test"
        assert resp["Tags"]["team"] == "dev"

    def test_delete_tags(self, mq, broker):
        mq.create_tags(
            ResourceArn=broker["BrokerArn"],
            Tags={"env": "test", "team": "dev"},
        )
        mq.delete_tags(ResourceArn=broker["BrokerArn"], TagKeys=["team"])
        resp = mq.list_tags(ResourceArn=broker["BrokerArn"])
        assert "env" in resp["Tags"]
        assert "team" not in resp["Tags"]

    def test_update_broker(self, mq, broker):
        resp = mq.update_broker(
            BrokerId=broker["BrokerId"],
            AutoMinorVersionUpgrade=True,
        )
        assert resp["BrokerId"] == broker["BrokerId"]

    def test_delete_broker(self, mq):
        resp = mq.create_broker(
            BrokerName=_unique("broker"),
            EngineType="ACTIVEMQ",
            EngineVersion="5.17.6",
            HostInstanceType="mq.t3.micro",
            DeploymentMode="SINGLE_INSTANCE",
            PubliclyAccessible=False,
            Users=[{"Username": "admin", "Password": "Admin12345!"}],
        )
        broker_id = resp["BrokerId"]
        del_resp = mq.delete_broker(BrokerId=broker_id)
        assert del_resp["BrokerId"] == broker_id
        # Verify it's gone from the list
        brokers = mq.list_brokers()
        broker_ids = [b["BrokerId"] for b in brokers["BrokerSummaries"]]
        assert broker_id not in broker_ids


class TestMQConfigurationOperations:
    def test_create_configuration(self, mq):
        name = _unique("config")
        resp = mq.create_configuration(
            Name=name,
            EngineType="ACTIVEMQ",
            EngineVersion="5.17.6",
        )
        assert "Id" in resp
        assert "Arn" in resp
        assert resp["Name"] == name

    def test_describe_configuration(self, mq, configuration):
        resp = mq.describe_configuration(ConfigurationId=configuration["Id"])
        assert resp["Id"] == configuration["Id"]
        assert resp["Arn"] == configuration["Arn"]
        assert resp["Name"] == configuration["Name"]
        assert resp["EngineType"] == "ACTIVEMQ"
        assert resp["EngineVersion"] == "5.17.6"

    def test_list_configurations(self, mq, configuration):
        resp = mq.list_configurations()
        config_ids = [c["Id"] for c in resp["Configurations"]]
        assert configuration["Id"] in config_ids


class TestMQUserOperations:
    def test_create_user(self, mq, broker):
        resp = mq.create_user(
            BrokerId=broker["BrokerId"],
            Username="testuser",
            Password="TestPass123!",
        )
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200

    def test_describe_user(self, mq, broker):
        mq.create_user(
            BrokerId=broker["BrokerId"],
            Username="descuser",
            Password="TestPass123!",
        )
        resp = mq.describe_user(
            BrokerId=broker["BrokerId"],
            Username="descuser",
        )
        assert resp["Username"] == "descuser"
        assert resp["BrokerId"] == broker["BrokerId"]

    def test_update_user(self, mq, broker):
        mq.create_user(
            BrokerId=broker["BrokerId"],
            Username="upduser",
            Password="TestPass123!",
        )
        resp = mq.update_user(
            BrokerId=broker["BrokerId"],
            Username="upduser",
            Password="NewPass456!",
            Groups=["admins"],
        )
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200
        # Verify the update took effect
        desc = mq.describe_user(
            BrokerId=broker["BrokerId"],
            Username="upduser",
        )
        assert "admins" in desc.get("Groups", [])

    def test_delete_user(self, mq, broker):
        mq.create_user(
            BrokerId=broker["BrokerId"],
            Username="deluser",
            Password="TestPass123!",
        )
        resp = mq.delete_user(
            BrokerId=broker["BrokerId"],
            Username="deluser",
        )
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200

    def test_list_users(self, mq, broker):
        resp = mq.list_users(BrokerId=broker["BrokerId"])
        assert "Users" in resp
        # The broker was created with an "admin" user
        usernames = [u["Username"] for u in resp["Users"]]
        assert "admin" in usernames


class TestMQErrors:
    """Tests for MQ error handling."""

    def test_describe_broker_nonexistent(self, mq):
        """DescribeBroker for nonexistent broker raises NotFoundException."""
        from botocore.exceptions import ClientError

        with pytest.raises(ClientError) as exc:
            mq.describe_broker(BrokerId="nonexistent-broker-id")
        assert exc.value.response["Error"]["Code"] == "NotFoundException"

    def test_describe_configuration_nonexistent(self, mq):
        """DescribeConfiguration for nonexistent config raises NotFoundException."""
        from botocore.exceptions import ClientError

        with pytest.raises(ClientError) as exc:
            mq.describe_configuration(ConfigurationId="nonexistent-config-id")
        assert exc.value.response["Error"]["Code"] == "NotFoundException"

    def test_describe_user_nonexistent_broker(self, mq):
        """DescribeUser on nonexistent broker raises NotFoundException."""
        from botocore.exceptions import ClientError

        with pytest.raises(ClientError) as exc:
            mq.describe_user(BrokerId="nonexistent-broker-id", Username="admin")
        assert exc.value.response["Error"]["Code"] == "NotFoundException"

    def test_list_users_nonexistent_broker(self, mq):
        """ListUsers on nonexistent broker raises NotFoundException."""
        from botocore.exceptions import ClientError

        with pytest.raises(ClientError) as exc:
            mq.list_users(BrokerId="nonexistent-broker-id")
        assert exc.value.response["Error"]["Code"] == "NotFoundException"
