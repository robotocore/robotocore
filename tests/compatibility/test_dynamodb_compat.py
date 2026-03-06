"""DynamoDB compatibility tests."""

import pytest
from tests.compatibility.conftest import make_client


@pytest.fixture
def dynamodb():
    return make_client("dynamodb")


@pytest.fixture
def table(dynamodb):
    table_name = "test-compat-table"
    dynamodb.create_table(
        TableName=table_name,
        KeySchema=[{"AttributeName": "pk", "KeyType": "HASH"}],
        AttributeDefinitions=[{"AttributeName": "pk", "AttributeType": "S"}],
        BillingMode="PAY_PER_REQUEST",
    )
    yield table_name
    dynamodb.delete_table(TableName=table_name)


class TestDynamoDBOperations:
    def test_create_table(self, dynamodb):
        response = dynamodb.create_table(
            TableName="create-test-table",
            KeySchema=[{"AttributeName": "id", "KeyType": "HASH"}],
            AttributeDefinitions=[{"AttributeName": "id", "AttributeType": "S"}],
            BillingMode="PAY_PER_REQUEST",
        )
        assert response["TableDescription"]["TableName"] == "create-test-table"
        dynamodb.delete_table(TableName="create-test-table")

    def test_list_tables(self, dynamodb, table):
        response = dynamodb.list_tables()
        assert table in response["TableNames"]

    def test_put_and_get_item(self, dynamodb, table):
        dynamodb.put_item(TableName=table, Item={"pk": {"S": "key1"}, "data": {"S": "value1"}})
        response = dynamodb.get_item(TableName=table, Key={"pk": {"S": "key1"}})
        assert response["Item"]["data"]["S"] == "value1"

    def test_delete_item(self, dynamodb, table):
        dynamodb.put_item(TableName=table, Item={"pk": {"S": "del-key"}})
        dynamodb.delete_item(TableName=table, Key={"pk": {"S": "del-key"}})
        response = dynamodb.get_item(TableName=table, Key={"pk": {"S": "del-key"}})
        assert "Item" not in response

    def test_query(self, dynamodb, table):
        dynamodb.put_item(TableName=table, Item={"pk": {"S": "q-key"}, "val": {"N": "42"}})
        response = dynamodb.query(
            TableName=table,
            KeyConditionExpression="pk = :pk",
            ExpressionAttributeValues={":pk": {"S": "q-key"}},
        )
        assert response["Count"] == 1
        assert response["Items"][0]["val"]["N"] == "42"

    def test_scan(self, dynamodb, table):
        dynamodb.put_item(TableName=table, Item={"pk": {"S": "s1"}})
        dynamodb.put_item(TableName=table, Item={"pk": {"S": "s2"}})
        response = dynamodb.scan(TableName=table)
        assert response["Count"] >= 2

    def test_update_item(self, dynamodb, table):
        dynamodb.put_item(TableName=table, Item={"pk": {"S": "upd"}, "cnt": {"N": "0"}})
        dynamodb.update_item(
            TableName=table,
            Key={"pk": {"S": "upd"}},
            UpdateExpression="SET cnt = cnt + :inc",
            ExpressionAttributeValues={":inc": {"N": "1"}},
        )
        response = dynamodb.get_item(TableName=table, Key={"pk": {"S": "upd"}})
        assert response["Item"]["cnt"]["N"] == "1"
