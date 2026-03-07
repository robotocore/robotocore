"""DynamoDB compatibility tests."""

import uuid

import pytest
from botocore.exceptions import ClientError

from tests.compatibility.conftest import make_client


@pytest.fixture
def dynamodb():
    return make_client("dynamodb")


@pytest.fixture
def table(dynamodb):
    table_name = f"test-compat-table-{uuid.uuid4().hex[:8]}"
    dynamodb.create_table(
        TableName=table_name,
        KeySchema=[{"AttributeName": "pk", "KeyType": "HASH"}],
        AttributeDefinitions=[{"AttributeName": "pk", "AttributeType": "S"}],
        BillingMode="PAY_PER_REQUEST",
    )
    yield table_name
    dynamodb.delete_table(TableName=table_name)


@pytest.fixture
def composite_table(dynamodb):
    """Table with partition key + sort key for query tests."""
    table_name = f"test-composite-{uuid.uuid4().hex[:8]}"
    dynamodb.create_table(
        TableName=table_name,
        KeySchema=[
            {"AttributeName": "pk", "KeyType": "HASH"},
            {"AttributeName": "sk", "KeyType": "RANGE"},
        ],
        AttributeDefinitions=[
            {"AttributeName": "pk", "AttributeType": "S"},
            {"AttributeName": "sk", "AttributeType": "S"},
        ],
        BillingMode="PAY_PER_REQUEST",
    )
    yield table_name
    dynamodb.delete_table(TableName=table_name)


@pytest.fixture
def gsi_table(dynamodb):
    """Table with a Global Secondary Index."""
    table_name = f"test-gsi-{uuid.uuid4().hex[:8]}"
    dynamodb.create_table(
        TableName=table_name,
        KeySchema=[{"AttributeName": "pk", "KeyType": "HASH"}],
        AttributeDefinitions=[
            {"AttributeName": "pk", "AttributeType": "S"},
            {"AttributeName": "gsi_pk", "AttributeType": "S"},
        ],
        BillingMode="PAY_PER_REQUEST",
        GlobalSecondaryIndexes=[
            {
                "IndexName": "gsi-index",
                "KeySchema": [{"AttributeName": "gsi_pk", "KeyType": "HASH"}],
                "Projection": {"ProjectionType": "ALL"},
            }
        ],
    )
    yield table_name
    dynamodb.delete_table(TableName=table_name)


class TestDynamoDBOperations:
    def test_create_table(self, dynamodb):
        tname = f"create-test-{uuid.uuid4().hex[:8]}"
        response = dynamodb.create_table(
            TableName=tname,
            KeySchema=[{"AttributeName": "id", "KeyType": "HASH"}],
            AttributeDefinitions=[{"AttributeName": "id", "AttributeType": "S"}],
            BillingMode="PAY_PER_REQUEST",
        )
        assert response["TableDescription"]["TableName"] == tname
        dynamodb.delete_table(TableName=tname)

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


class TestBatchOperations:
    def test_batch_write_item(self, dynamodb, table):
        """batch_write_item puts multiple items in one call."""
        items = [
            {"PutRequest": {"Item": {"pk": {"S": f"batch-{i}"}, "val": {"N": str(i)}}}}
            for i in range(5)
        ]
        response = dynamodb.batch_write_item(RequestItems={table: items})
        assert response["UnprocessedItems"] == {} or table not in response["UnprocessedItems"]

        # Verify all items exist
        for i in range(5):
            resp = dynamodb.get_item(TableName=table, Key={"pk": {"S": f"batch-{i}"}})
            assert resp["Item"]["val"]["N"] == str(i)

    def test_batch_get_item(self, dynamodb, table):
        """batch_get_item retrieves multiple items in one call."""
        for i in range(3):
            dynamodb.put_item(
                TableName=table, Item={"pk": {"S": f"bg-{i}"}, "data": {"S": f"val-{i}"}}
            )

        response = dynamodb.batch_get_item(
            RequestItems={table: {"Keys": [{"pk": {"S": f"bg-{i}"}} for i in range(3)]}}
        )
        items = response["Responses"][table]
        assert len(items) == 3
        pks = sorted(item["pk"]["S"] for item in items)
        assert pks == ["bg-0", "bg-1", "bg-2"]


class TestQueryWithSortKey:
    def test_query_partition_and_sort_key(self, dynamodb, composite_table):
        """Query with KeyConditionExpression on partition + sort key."""
        for i in range(5):
            dynamodb.put_item(
                TableName=composite_table,
                Item={
                    "pk": {"S": "user-1"},
                    "sk": {"S": f"order-{i:03d}"},
                    "amount": {"N": str(i * 10)},
                },
            )
        # Also insert a different partition key to ensure isolation
        dynamodb.put_item(
            TableName=composite_table,
            Item={"pk": {"S": "user-2"}, "sk": {"S": "order-000"}, "amount": {"N": "99"}},
        )

        response = dynamodb.query(
            TableName=composite_table,
            KeyConditionExpression="pk = :pk AND sk BETWEEN :lo AND :hi",
            ExpressionAttributeValues={
                ":pk": {"S": "user-1"},
                ":lo": {"S": "order-001"},
                ":hi": {"S": "order-003"},
            },
        )
        assert response["Count"] == 3
        sks = [item["sk"]["S"] for item in response["Items"]]
        assert sks == ["order-001", "order-002", "order-003"]

    def test_query_sort_key_begins_with(self, dynamodb, composite_table):
        """Query with begins_with on sort key."""
        dynamodb.put_item(
            TableName=composite_table,
            Item={"pk": {"S": "doc"}, "sk": {"S": "v1#section-a"}},
        )
        dynamodb.put_item(
            TableName=composite_table,
            Item={"pk": {"S": "doc"}, "sk": {"S": "v1#section-b"}},
        )
        dynamodb.put_item(
            TableName=composite_table,
            Item={"pk": {"S": "doc"}, "sk": {"S": "v2#section-a"}},
        )

        response = dynamodb.query(
            TableName=composite_table,
            KeyConditionExpression="pk = :pk AND begins_with(sk, :prefix)",
            ExpressionAttributeValues={
                ":pk": {"S": "doc"},
                ":prefix": {"S": "v1#"},
            },
        )
        assert response["Count"] == 2


class TestScanWithFilter:
    def test_scan_filter_expression(self, dynamodb, table):
        """Scan with FilterExpression returns only matching items."""
        for i in range(10):
            dynamodb.put_item(
                TableName=table,
                Item={"pk": {"S": f"sf-{i}"}, "score": {"N": str(i * 10)}},
            )

        response = dynamodb.scan(
            TableName=table,
            FilterExpression="score >= :min",
            ExpressionAttributeValues={":min": {"N": "50"}},
        )
        # Items with score 50, 60, 70, 80, 90
        assert response["Count"] == 5
        for item in response["Items"]:
            assert int(item["score"]["N"]) >= 50


class TestUpdateExpressions:
    def test_update_set_multiple_attributes(self, dynamodb, table):
        """update_item with SET for multiple attributes."""
        dynamodb.put_item(
            TableName=table,
            Item={"pk": {"S": "ue-set"}, "name": {"S": "old"}, "age": {"N": "20"}},
        )
        dynamodb.update_item(
            TableName=table,
            Key={"pk": {"S": "ue-set"}},
            UpdateExpression="SET #n = :name, age = :age",
            ExpressionAttributeNames={"#n": "name"},
            ExpressionAttributeValues={":name": {"S": "new"}, ":age": {"N": "30"}},
        )
        response = dynamodb.get_item(TableName=table, Key={"pk": {"S": "ue-set"}})
        assert response["Item"]["name"]["S"] == "new"
        assert response["Item"]["age"]["N"] == "30"

    def test_update_remove_attribute(self, dynamodb, table):
        """update_item with REMOVE deletes an attribute."""
        dynamodb.put_item(
            TableName=table,
            Item={"pk": {"S": "ue-rm"}, "keep": {"S": "yes"}, "extra": {"S": "bye"}},
        )
        dynamodb.update_item(
            TableName=table,
            Key={"pk": {"S": "ue-rm"}},
            UpdateExpression="REMOVE extra",
        )
        response = dynamodb.get_item(TableName=table, Key={"pk": {"S": "ue-rm"}})
        assert "extra" not in response["Item"]
        assert response["Item"]["keep"]["S"] == "yes"

    def test_update_set_and_remove_combined(self, dynamodb, table):
        """update_item with SET and REMOVE in a single expression."""
        dynamodb.put_item(
            TableName=table,
            Item={"pk": {"S": "ue-combo"}, "a": {"S": "1"}, "b": {"S": "2"}},
        )
        dynamodb.update_item(
            TableName=table,
            Key={"pk": {"S": "ue-combo"}},
            UpdateExpression="SET c = :c REMOVE b",
            ExpressionAttributeValues={":c": {"S": "3"}},
        )
        response = dynamodb.get_item(TableName=table, Key={"pk": {"S": "ue-combo"}})
        assert response["Item"]["a"]["S"] == "1"
        assert "b" not in response["Item"]
        assert response["Item"]["c"]["S"] == "3"


class TestConditionalWrites:
    def test_put_item_condition_succeeds(self, dynamodb, table):
        """put_item with ConditionExpression succeeds when condition is met."""
        dynamodb.put_item(
            TableName=table,
            Item={"pk": {"S": "cond-ok"}, "phase": {"S": "draft"}},
        )
        # Overwrite only if phase is draft
        dynamodb.put_item(
            TableName=table,
            Item={"pk": {"S": "cond-ok"}, "phase": {"S": "published"}},
            ConditionExpression="phase = :expected",
            ExpressionAttributeValues={":expected": {"S": "draft"}},
        )
        response = dynamodb.get_item(TableName=table, Key={"pk": {"S": "cond-ok"}})
        assert response["Item"]["phase"]["S"] == "published"

    def test_put_item_condition_fails(self, dynamodb, table):
        """put_item with ConditionExpression raises ConditionalCheckFailedException."""
        dynamodb.put_item(
            TableName=table,
            Item={"pk": {"S": "cond-fail"}, "phase": {"S": "published"}},
        )
        with pytest.raises(ClientError) as exc_info:
            dynamodb.put_item(
                TableName=table,
                Item={"pk": {"S": "cond-fail"}, "phase": {"S": "archived"}},
                ConditionExpression="phase = :expected",
                ExpressionAttributeValues={":expected": {"S": "draft"}},
            )
        assert exc_info.value.response["Error"]["Code"] == "ConditionalCheckFailedException"

    def test_put_item_condition_attribute_not_exists(self, dynamodb, table):
        """put_item only if the item does not already exist."""
        dynamodb.put_item(
            TableName=table,
            Item={"pk": {"S": "cond-new"}},
            ConditionExpression="attribute_not_exists(pk)",
        )
        with pytest.raises(ClientError) as exc_info:
            dynamodb.put_item(
                TableName=table,
                Item={"pk": {"S": "cond-new"}},
                ConditionExpression="attribute_not_exists(pk)",
            )
        assert exc_info.value.response["Error"]["Code"] == "ConditionalCheckFailedException"


class TestGlobalSecondaryIndex:
    def test_gsi_table_has_index(self, dynamodb, gsi_table):
        """Describe table shows the GSI."""
        response = dynamodb.describe_table(TableName=gsi_table)
        gsis = response["Table"]["GlobalSecondaryIndexes"]
        assert len(gsis) == 1
        assert gsis[0]["IndexName"] == "gsi-index"

    def test_query_on_gsi(self, dynamodb, gsi_table):
        """Query items using a Global Secondary Index."""
        dynamodb.put_item(
            TableName=gsi_table,
            Item={"pk": {"S": "a"}, "gsi_pk": {"S": "group-1"}, "val": {"S": "first"}},
        )
        dynamodb.put_item(
            TableName=gsi_table,
            Item={"pk": {"S": "b"}, "gsi_pk": {"S": "group-1"}, "val": {"S": "second"}},
        )
        dynamodb.put_item(
            TableName=gsi_table,
            Item={"pk": {"S": "c"}, "gsi_pk": {"S": "group-2"}, "val": {"S": "other"}},
        )

        response = dynamodb.query(
            TableName=gsi_table,
            IndexName="gsi-index",
            KeyConditionExpression="gsi_pk = :gpk",
            ExpressionAttributeValues={":gpk": {"S": "group-1"}},
        )
        assert response["Count"] == 2
        vals = sorted(item["val"]["S"] for item in response["Items"])
        assert vals == ["first", "second"]


class TestTTL:
    def test_describe_ttl_default(self, dynamodb, table):
        """TTL is disabled by default."""
        response = dynamodb.describe_time_to_live(TableName=table)
        assert response["TimeToLiveDescription"]["TimeToLiveStatus"] in (
            "DISABLED",
            "DISABLING",
        )

    def test_update_and_describe_ttl(self, dynamodb, table):
        """Enable TTL and verify with describe."""
        dynamodb.update_time_to_live(
            TableName=table,
            TimeToLiveSpecification={
                "Enabled": True,
                "AttributeName": "expires_at",
            },
        )
        response = dynamodb.describe_time_to_live(TableName=table)
        ttl = response["TimeToLiveDescription"]
        assert ttl["TimeToLiveStatus"] in ("ENABLED", "ENABLING")
        assert ttl["AttributeName"] == "expires_at"


class TestTransactions:
    def test_transact_write_items_put_and_delete(self, dynamodb, table):
        """transact_write_items with Put and Delete operations."""
        # Pre-insert an item to delete
        dynamodb.put_item(
            TableName=table,
            Item={"pk": {"S": "txn-delete-me"}, "data": {"S": "old"}},
        )

        dynamodb.transact_write_items(
            TransactItems=[
                {
                    "Put": {
                        "TableName": table,
                        "Item": {"pk": {"S": "txn-new-1"}, "data": {"S": "created"}},
                    }
                },
                {
                    "Put": {
                        "TableName": table,
                        "Item": {"pk": {"S": "txn-new-2"}, "data": {"S": "also-created"}},
                    }
                },
                {
                    "Delete": {
                        "TableName": table,
                        "Key": {"pk": {"S": "txn-delete-me"}},
                    }
                },
            ]
        )

        # Verify puts
        r1 = dynamodb.get_item(TableName=table, Key={"pk": {"S": "txn-new-1"}})
        assert r1["Item"]["data"]["S"] == "created"
        r2 = dynamodb.get_item(TableName=table, Key={"pk": {"S": "txn-new-2"}})
        assert r2["Item"]["data"]["S"] == "also-created"
        # Verify delete
        r3 = dynamodb.get_item(TableName=table, Key={"pk": {"S": "txn-delete-me"}})
        assert "Item" not in r3

    def test_transact_write_items_condition_failure(self, dynamodb, table):
        """transact_write_items fails entirely when a condition check fails."""
        dynamodb.put_item(
            TableName=table,
            Item={"pk": {"S": "txn-cond"}, "phase": {"S": "locked"}},
        )

        with pytest.raises(ClientError) as exc_info:
            dynamodb.transact_write_items(
                TransactItems=[
                    {
                        "Put": {
                            "TableName": table,
                            "Item": {"pk": {"S": "txn-should-not-exist"}},
                        }
                    },
                    {
                        "ConditionCheck": {
                            "TableName": table,
                            "Key": {"pk": {"S": "txn-cond"}},
                            "ConditionExpression": "phase = :expected",
                            "ExpressionAttributeValues": {
                                ":expected": {"S": "unlocked"},
                            },
                        }
                    },
                ]
            )
        err_code = exc_info.value.response["Error"]["Code"]
        assert err_code in ("TransactionCanceledException", "ValidationException")

        # The put should not have been applied due to transaction failure
        r = dynamodb.get_item(TableName=table, Key={"pk": {"S": "txn-should-not-exist"}})
        assert "Item" not in r


class TestBatchWriteAndDelete:
    def test_batch_write_put_and_delete(self, dynamodb, table):
        """batch_write_item with both Put and Delete requests in one call."""
        # Pre-insert items to delete
        dynamodb.put_item(TableName=table, Item={"pk": {"S": "bwd-del-1"}, "val": {"S": "old1"}})
        dynamodb.put_item(TableName=table, Item={"pk": {"S": "bwd-del-2"}, "val": {"S": "old2"}})

        response = dynamodb.batch_write_item(
            RequestItems={
                table: [
                    {"PutRequest": {"Item": {"pk": {"S": "bwd-put-1"}, "val": {"S": "new1"}}}},
                    {"PutRequest": {"Item": {"pk": {"S": "bwd-put-2"}, "val": {"S": "new2"}}}},
                    {"DeleteRequest": {"Key": {"pk": {"S": "bwd-del-1"}}}},
                    {"DeleteRequest": {"Key": {"pk": {"S": "bwd-del-2"}}}},
                ]
            }
        )
        assert response["UnprocessedItems"] == {} or table not in response["UnprocessedItems"]

        # Verify puts
        r1 = dynamodb.get_item(TableName=table, Key={"pk": {"S": "bwd-put-1"}})
        assert r1["Item"]["val"]["S"] == "new1"
        r2 = dynamodb.get_item(TableName=table, Key={"pk": {"S": "bwd-put-2"}})
        assert r2["Item"]["val"]["S"] == "new2"

        # Verify deletes
        r3 = dynamodb.get_item(TableName=table, Key={"pk": {"S": "bwd-del-1"}})
        assert "Item" not in r3
        r4 = dynamodb.get_item(TableName=table, Key={"pk": {"S": "bwd-del-2"}})
        assert "Item" not in r4


class TestBatchGetAdvanced:
    def test_batch_get_with_projection(self, dynamodb, table):
        """batch_get_item with ProjectionExpression returns only requested attributes."""
        dynamodb.put_item(
            TableName=table,
            Item={"pk": {"S": "bgp-1"}, "name": {"S": "Alice"}, "age": {"N": "30"}},
        )
        dynamodb.put_item(
            TableName=table,
            Item={"pk": {"S": "bgp-2"}, "name": {"S": "Bob"}, "age": {"N": "25"}},
        )

        response = dynamodb.batch_get_item(
            RequestItems={
                table: {
                    "Keys": [{"pk": {"S": "bgp-1"}}, {"pk": {"S": "bgp-2"}}],
                    "ProjectionExpression": "pk, #n",
                    "ExpressionAttributeNames": {"#n": "name"},
                }
            }
        )
        items = response["Responses"][table]
        assert len(items) == 2
        for item in items:
            assert "name" in item
            assert "age" not in item


class TestTransactGetItems:
    def test_transact_get_items(self, dynamodb, table):
        """transact_get_items retrieves multiple items atomically."""
        dynamodb.put_item(
            TableName=table, Item={"pk": {"S": "tg-1"}, "data": {"S": "val-1"}}
        )
        dynamodb.put_item(
            TableName=table, Item={"pk": {"S": "tg-2"}, "data": {"S": "val-2"}}
        )
        dynamodb.put_item(
            TableName=table, Item={"pk": {"S": "tg-3"}, "data": {"S": "val-3"}}
        )

        response = dynamodb.transact_get_items(
            TransactItems=[
                {"Get": {"TableName": table, "Key": {"pk": {"S": "tg-1"}}}},
                {"Get": {"TableName": table, "Key": {"pk": {"S": "tg-2"}}}},
                {"Get": {"TableName": table, "Key": {"pk": {"S": "tg-3"}}}},
            ]
        )
        items = [r["Item"] for r in response["Responses"]]
        assert len(items) == 3
        data_vals = sorted(item["data"]["S"] for item in items)
        assert data_vals == ["val-1", "val-2", "val-3"]


class TestTransactWriteUpdate:
    def test_transact_write_with_update(self, dynamodb, table):
        """transact_write_items with Update operation."""
        dynamodb.put_item(
            TableName=table,
            Item={"pk": {"S": "twu-1"}, "counter": {"N": "10"}},
        )

        dynamodb.transact_write_items(
            TransactItems=[
                {
                    "Update": {
                        "TableName": table,
                        "Key": {"pk": {"S": "twu-1"}},
                        "UpdateExpression": "SET #c = #c + :inc",
                        "ExpressionAttributeNames": {"#c": "counter"},
                        "ExpressionAttributeValues": {":inc": {"N": "5"}},
                    }
                }
            ]
        )
        r = dynamodb.get_item(TableName=table, Key={"pk": {"S": "twu-1"}})
        assert r["Item"]["counter"]["N"] == "15"


class TestUpdateWithCondition:
    def test_update_with_condition_expression_succeeds(self, dynamodb, table):
        """update_item with ConditionExpression that passes."""
        dynamodb.put_item(
            TableName=table,
            Item={"pk": {"S": "uc-ok"}, "status": {"S": "active"}, "count": {"N": "0"}},
        )

        dynamodb.update_item(
            TableName=table,
            Key={"pk": {"S": "uc-ok"}},
            UpdateExpression="SET #c = :val",
            ConditionExpression="#s = :expected",
            ExpressionAttributeNames={"#c": "count", "#s": "status"},
            ExpressionAttributeValues={":val": {"N": "1"}, ":expected": {"S": "active"}},
        )
        r = dynamodb.get_item(TableName=table, Key={"pk": {"S": "uc-ok"}})
        assert r["Item"]["count"]["N"] == "1"

    def test_update_with_condition_expression_fails(self, dynamodb, table):
        """update_item with ConditionExpression that fails raises error."""
        dynamodb.put_item(
            TableName=table,
            Item={"pk": {"S": "uc-fail"}, "status": {"S": "locked"}},
        )

        with pytest.raises(ClientError) as exc_info:
            dynamodb.update_item(
                TableName=table,
                Key={"pk": {"S": "uc-fail"}},
                UpdateExpression="SET #s = :new",
                ConditionExpression="#s = :expected",
                ExpressionAttributeNames={"#s": "status"},
                ExpressionAttributeValues={":new": {"S": "open"}, ":expected": {"S": "active"}},
            )
        assert exc_info.value.response["Error"]["Code"] == "ConditionalCheckFailedException"


class TestQueryAdvanced:
    def test_query_begins_with(self, dynamodb, composite_table):
        """Query with begins_with on sort key returns matching items."""
        for suffix in ["alpha", "alpha-2", "beta"]:
            dynamodb.put_item(
                TableName=composite_table,
                Item={"pk": {"S": "qbw"}, "sk": {"S": suffix}, "val": {"S": suffix}},
            )

        response = dynamodb.query(
            TableName=composite_table,
            KeyConditionExpression="pk = :pk AND begins_with(sk, :prefix)",
            ExpressionAttributeValues={
                ":pk": {"S": "qbw"},
                ":prefix": {"S": "alpha"},
            },
        )
        assert response["Count"] == 2
        sks = sorted(item["sk"]["S"] for item in response["Items"])
        assert sks == ["alpha", "alpha-2"]

    def test_query_between(self, dynamodb, composite_table):
        """Query with BETWEEN on sort key returns items in range."""
        for i in range(10):
            dynamodb.put_item(
                TableName=composite_table,
                Item={"pk": {"S": "qbt"}, "sk": {"S": f"item-{i:03d}"}},
            )

        response = dynamodb.query(
            TableName=composite_table,
            KeyConditionExpression="pk = :pk AND sk BETWEEN :lo AND :hi",
            ExpressionAttributeValues={
                ":pk": {"S": "qbt"},
                ":lo": {"S": "item-003"},
                ":hi": {"S": "item-006"},
            },
        )
        assert response["Count"] == 4
        sks = [item["sk"]["S"] for item in response["Items"]]
        assert sks == ["item-003", "item-004", "item-005", "item-006"]

    def test_query_scan_index_forward_false(self, dynamodb, composite_table):
        """Query with ScanIndexForward=False returns items in reverse order."""
        for i in range(5):
            dynamodb.put_item(
                TableName=composite_table,
                Item={"pk": {"S": "qrev"}, "sk": {"S": f"s-{i:03d}"}},
            )

        response = dynamodb.query(
            TableName=composite_table,
            KeyConditionExpression="pk = :pk",
            ExpressionAttributeValues={":pk": {"S": "qrev"}},
            ScanIndexForward=False,
        )
        sks = [item["sk"]["S"] for item in response["Items"]]
        assert sks == ["s-004", "s-003", "s-002", "s-001", "s-000"]

    def test_query_with_limit(self, dynamodb, composite_table):
        """Query with Limit returns at most that many items."""
        for i in range(10):
            dynamodb.put_item(
                TableName=composite_table,
                Item={"pk": {"S": "qlim"}, "sk": {"S": f"r-{i:03d}"}},
            )

        response = dynamodb.query(
            TableName=composite_table,
            KeyConditionExpression="pk = :pk",
            ExpressionAttributeValues={":pk": {"S": "qlim"}},
            Limit=3,
        )
        assert len(response["Items"]) == 3
        assert "LastEvaluatedKey" in response


class TestScanAdvanced:
    def test_scan_with_projection(self, dynamodb, table):
        """Scan with ProjectionExpression returns only selected attributes."""
        dynamodb.put_item(
            TableName=table,
            Item={"pk": {"S": "sp-1"}, "name": {"S": "Alice"}, "age": {"N": "30"}},
        )
        response = dynamodb.scan(
            TableName=table,
            FilterExpression="pk = :pk",
            ProjectionExpression="pk, #n",
            ExpressionAttributeNames={"#n": "name"},
            ExpressionAttributeValues={":pk": {"S": "sp-1"}},
        )
        assert response["Count"] == 1
        item = response["Items"][0]
        assert "name" in item
        assert "age" not in item


class TestDescribeTable:
    def test_describe_table_basic(self, dynamodb, table):
        """describe_table returns table metadata."""
        response = dynamodb.describe_table(TableName=table)
        desc = response["Table"]
        assert desc["TableName"] == table
        assert desc["TableStatus"] == "ACTIVE"
        assert len(desc["KeySchema"]) == 1
        assert desc["KeySchema"][0]["AttributeName"] == "pk"
        assert desc["KeySchema"][0]["KeyType"] == "HASH"

    def test_describe_table_with_gsi(self, dynamodb, gsi_table):
        """describe_table shows GSI information."""
        response = dynamodb.describe_table(TableName=gsi_table)
        gsis = response["Table"]["GlobalSecondaryIndexes"]
        assert len(gsis) == 1
        assert gsis[0]["IndexName"] == "gsi-index"
        assert gsis[0]["Projection"]["ProjectionType"] == "ALL"
        gsi_keys = {ks["AttributeName"]: ks["KeyType"] for ks in gsis[0]["KeySchema"]}
        assert gsi_keys["gsi_pk"] == "HASH"


class TestLocalSecondaryIndex:
    def test_query_on_lsi(self, dynamodb):
        """Create a table with LSI and query on it."""
        table_name = f"test-lsi-{uuid.uuid4().hex[:8]}"
        dynamodb.create_table(
            TableName=table_name,
            KeySchema=[
                {"AttributeName": "pk", "KeyType": "HASH"},
                {"AttributeName": "sk", "KeyType": "RANGE"},
            ],
            AttributeDefinitions=[
                {"AttributeName": "pk", "AttributeType": "S"},
                {"AttributeName": "sk", "AttributeType": "S"},
                {"AttributeName": "lsi_sk", "AttributeType": "S"},
            ],
            BillingMode="PAY_PER_REQUEST",
            LocalSecondaryIndexes=[
                {
                    "IndexName": "lsi-index",
                    "KeySchema": [
                        {"AttributeName": "pk", "KeyType": "HASH"},
                        {"AttributeName": "lsi_sk", "KeyType": "RANGE"},
                    ],
                    "Projection": {"ProjectionType": "ALL"},
                }
            ],
        )
        try:
            dynamodb.put_item(
                TableName=table_name,
                Item={
                    "pk": {"S": "u1"},
                    "sk": {"S": "a"},
                    "lsi_sk": {"S": "z-first"},
                    "data": {"S": "hello"},
                },
            )
            dynamodb.put_item(
                TableName=table_name,
                Item={
                    "pk": {"S": "u1"},
                    "sk": {"S": "b"},
                    "lsi_sk": {"S": "a-second"},
                    "data": {"S": "world"},
                },
            )

            response = dynamodb.query(
                TableName=table_name,
                IndexName="lsi-index",
                KeyConditionExpression="pk = :pk",
                ExpressionAttributeValues={":pk": {"S": "u1"}},
            )
            assert response["Count"] == 2
            # LSI should sort by lsi_sk, so "a-second" comes first
            assert response["Items"][0]["lsi_sk"]["S"] == "a-second"
            assert response["Items"][1]["lsi_sk"]["S"] == "z-first"
        finally:
            dynamodb.delete_table(TableName=table_name)


class TestUpdateTable:
    def test_update_table_add_gsi(self, dynamodb):
        """Update a table to add a GSI."""
        table_name = f"test-updgsi-{uuid.uuid4().hex[:8]}"
        dynamodb.create_table(
            TableName=table_name,
            KeySchema=[{"AttributeName": "pk", "KeyType": "HASH"}],
            AttributeDefinitions=[
                {"AttributeName": "pk", "AttributeType": "S"},
            ],
            BillingMode="PAY_PER_REQUEST",
        )
        try:
            dynamodb.update_table(
                TableName=table_name,
                AttributeDefinitions=[
                    {"AttributeName": "pk", "AttributeType": "S"},
                    {"AttributeName": "email", "AttributeType": "S"},
                ],
                GlobalSecondaryIndexUpdates=[
                    {
                        "Create": {
                            "IndexName": "email-index",
                            "KeySchema": [{"AttributeName": "email", "KeyType": "HASH"}],
                            "Projection": {"ProjectionType": "ALL"},
                        }
                    }
                ],
            )
            desc = dynamodb.describe_table(TableName=table_name)
            gsi_names = [g["IndexName"] for g in desc["Table"].get("GlobalSecondaryIndexes", [])]
            assert "email-index" in gsi_names
        finally:
            dynamodb.delete_table(TableName=table_name)


class TestTableTags:
    def test_tag_untag_table(self, dynamodb, table):
        """Tag and untag a DynamoDB table."""
        desc = dynamodb.describe_table(TableName=table)
        table_arn = desc["Table"]["TableArn"]

        dynamodb.tag_resource(
            ResourceArn=table_arn,
            Tags=[
                {"Key": "env", "Value": "test"},
                {"Key": "team", "Value": "platform"},
            ],
        )

        response = dynamodb.list_tags_of_resource(ResourceArn=table_arn)
        tags = {t["Key"]: t["Value"] for t in response["Tags"]}
        assert tags["env"] == "test"
        assert tags["team"] == "platform"

        dynamodb.untag_resource(ResourceArn=table_arn, TagKeys=["team"])
        response = dynamodb.list_tags_of_resource(ResourceArn=table_arn)
        tag_keys = [t["Key"] for t in response["Tags"]]
        assert "team" not in tag_keys
        assert "env" in tag_keys
