"""DynamoDB Streams compatibility tests."""

import uuid

import pytest

from tests.compatibility.conftest import make_client


@pytest.fixture
def dynamodb():
    return make_client("dynamodb")


@pytest.fixture
def dynamodbstreams():
    return make_client("dynamodbstreams")


def _uid():
    return uuid.uuid4().hex[:8]


class TestDynamoDBStreamsOperations:
    def test_list_streams(self, dynamodb, dynamodbstreams):
        table_name = f"stream-table-{_uid()}"
        dynamodb.create_table(
            TableName=table_name,
            AttributeDefinitions=[{"AttributeName": "pk", "AttributeType": "S"}],
            KeySchema=[{"AttributeName": "pk", "KeyType": "HASH"}],
            BillingMode="PAY_PER_REQUEST",
            StreamSpecification={
                "StreamEnabled": True,
                "StreamViewType": "NEW_AND_OLD_IMAGES",
            },
        )
        response = dynamodbstreams.list_streams(TableName=table_name)
        assert "Streams" in response
        assert len(response["Streams"]) >= 1
        assert response["Streams"][0]["TableName"] == table_name

        dynamodb.delete_table(TableName=table_name)

    def test_describe_stream(self, dynamodb, dynamodbstreams):
        table_name = f"desc-stream-{_uid()}"
        dynamodb.create_table(
            TableName=table_name,
            AttributeDefinitions=[{"AttributeName": "pk", "AttributeType": "S"}],
            KeySchema=[{"AttributeName": "pk", "KeyType": "HASH"}],
            BillingMode="PAY_PER_REQUEST",
            StreamSpecification={
                "StreamEnabled": True,
                "StreamViewType": "NEW_AND_OLD_IMAGES",
            },
        )
        streams = dynamodbstreams.list_streams(TableName=table_name)
        stream_arn = streams["Streams"][0]["StreamArn"]

        response = dynamodbstreams.describe_stream(StreamArn=stream_arn)
        assert response["StreamDescription"]["TableName"] == table_name
        assert response["StreamDescription"]["StreamViewType"] == "NEW_AND_OLD_IMAGES"

        dynamodb.delete_table(TableName=table_name)

    def test_get_shard_iterator(self, dynamodb, dynamodbstreams):
        table_name = f"shard-table-{_uid()}"
        dynamodb.create_table(
            TableName=table_name,
            AttributeDefinitions=[{"AttributeName": "pk", "AttributeType": "S"}],
            KeySchema=[{"AttributeName": "pk", "KeyType": "HASH"}],
            BillingMode="PAY_PER_REQUEST",
            StreamSpecification={
                "StreamEnabled": True,
                "StreamViewType": "NEW_AND_OLD_IMAGES",
            },
        )
        streams = dynamodbstreams.list_streams(TableName=table_name)
        stream_arn = streams["Streams"][0]["StreamArn"]

        desc = dynamodbstreams.describe_stream(StreamArn=stream_arn)
        shards = desc["StreamDescription"]["Shards"]
        if shards:
            shard_id = shards[0]["ShardId"]
            response = dynamodbstreams.get_shard_iterator(
                StreamArn=stream_arn,
                ShardId=shard_id,
                ShardIteratorType="TRIM_HORIZON",
            )
            assert "ShardIterator" in response

        dynamodb.delete_table(TableName=table_name)
