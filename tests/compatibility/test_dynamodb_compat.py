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

    def test_batch_write_and_batch_get_item(self, dynamodb):
        """batch_write_item to put 3 items, batch_get_item to retrieve them."""
        tname = f"test-bwbg-{uuid.uuid4().hex[:8]}"
        dynamodb.create_table(
            TableName=tname,
            KeySchema=[{"AttributeName": "id", "KeyType": "HASH"}],
            AttributeDefinitions=[{"AttributeName": "id", "AttributeType": "S"}],
            BillingMode="PAY_PER_REQUEST",
        )
        try:
            dynamodb.batch_write_item(
                RequestItems={
                    tname: [
                        {"PutRequest": {"Item": {"id": {"S": "bw-1"}, "v": {"S": "a"}}}},
                        {"PutRequest": {"Item": {"id": {"S": "bw-2"}, "v": {"S": "b"}}}},
                        {"PutRequest": {"Item": {"id": {"S": "bw-3"}, "v": {"S": "c"}}}},
                    ]
                }
            )
            response = dynamodb.batch_get_item(
                RequestItems={
                    tname: {
                        "Keys": [
                            {"id": {"S": "bw-1"}},
                            {"id": {"S": "bw-2"}},
                            {"id": {"S": "bw-3"}},
                        ]
                    }
                }
            )
            items = response["Responses"][tname]
            assert len(items) == 3
            ids = sorted(item["id"]["S"] for item in items)
            assert ids == ["bw-1", "bw-2", "bw-3"]
        finally:
            dynamodb.delete_table(TableName=tname)

    def test_describe_time_to_live(self, dynamodb):
        """describe_time_to_live returns TimeToLiveDescription."""
        tname = f"test-dttl-{uuid.uuid4().hex[:8]}"
        dynamodb.create_table(
            TableName=tname,
            KeySchema=[{"AttributeName": "id", "KeyType": "HASH"}],
            AttributeDefinitions=[{"AttributeName": "id", "AttributeType": "S"}],
            BillingMode="PAY_PER_REQUEST",
        )
        try:
            response = dynamodb.describe_time_to_live(TableName=tname)
            assert "TimeToLiveDescription" in response
        finally:
            dynamodb.delete_table(TableName=tname)

    def test_update_time_to_live(self, dynamodb):
        """Enable TTL and verify via describe."""
        tname = f"test-uttl-{uuid.uuid4().hex[:8]}"
        dynamodb.create_table(
            TableName=tname,
            KeySchema=[{"AttributeName": "id", "KeyType": "HASH"}],
            AttributeDefinitions=[{"AttributeName": "id", "AttributeType": "S"}],
            BillingMode="PAY_PER_REQUEST",
        )
        try:
            dynamodb.update_time_to_live(
                TableName=tname,
                TimeToLiveSpecification={
                    "Enabled": True,
                    "AttributeName": "ttl",
                },
            )
            response = dynamodb.describe_time_to_live(TableName=tname)
            ttl = response["TimeToLiveDescription"]
            assert ttl["TimeToLiveStatus"] in ("ENABLED", "ENABLING")
            assert ttl["AttributeName"] == "ttl"
        finally:
            dynamodb.delete_table(TableName=tname)

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

    def test_transact_write_items_two_puts(self, dynamodb):
        """transact_write_items with 2 Put actions, verify both items exist."""
        tname = f"test-txn-puts-{uuid.uuid4().hex[:8]}"
        dynamodb.create_table(
            TableName=tname,
            KeySchema=[{"AttributeName": "id", "KeyType": "HASH"}],
            AttributeDefinitions=[{"AttributeName": "id", "AttributeType": "S"}],
            BillingMode="PAY_PER_REQUEST",
        )
        try:
            dynamodb.transact_write_items(
                TransactItems=[
                    {
                        "Put": {
                            "TableName": tname,
                            "Item": {"id": {"S": "txn-a"}, "val": {"S": "alpha"}},
                        }
                    },
                    {
                        "Put": {
                            "TableName": tname,
                            "Item": {"id": {"S": "txn-b"}, "val": {"S": "beta"}},
                        }
                    },
                ]
            )
            r1 = dynamodb.get_item(TableName=tname, Key={"id": {"S": "txn-a"}})
            assert r1["Item"]["val"]["S"] == "alpha"
            r2 = dynamodb.get_item(TableName=tname, Key={"id": {"S": "txn-b"}})
            assert r2["Item"]["val"]["S"] == "beta"
        finally:
            dynamodb.delete_table(TableName=tname)

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


# ---------------------------------------------------------------------------
# Additional test fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
def lsi_table(dynamodb):
    """Table with a Local Secondary Index."""
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
    yield table_name
    dynamodb.delete_table(TableName=table_name)


@pytest.fixture
def gsi_composite_table(dynamodb):
    """Table with a GSI that has both hash and range key."""
    table_name = f"test-gsic-{uuid.uuid4().hex[:8]}"
    dynamodb.create_table(
        TableName=table_name,
        KeySchema=[{"AttributeName": "pk", "KeyType": "HASH"}],
        AttributeDefinitions=[
            {"AttributeName": "pk", "AttributeType": "S"},
            {"AttributeName": "gsi_pk", "AttributeType": "S"},
            {"AttributeName": "gsi_sk", "AttributeType": "S"},
        ],
        BillingMode="PAY_PER_REQUEST",
        GlobalSecondaryIndexes=[
            {
                "IndexName": "gsi-composite",
                "KeySchema": [
                    {"AttributeName": "gsi_pk", "KeyType": "HASH"},
                    {"AttributeName": "gsi_sk", "KeyType": "RANGE"},
                ],
                "Projection": {"ProjectionType": "ALL"},
            }
        ],
    )
    yield table_name
    dynamodb.delete_table(TableName=table_name)


@pytest.fixture
def gsi_keys_only_table(dynamodb):
    """Table with a GSI that uses KEYS_ONLY projection."""
    table_name = f"test-gsiko-{uuid.uuid4().hex[:8]}"
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
                "IndexName": "gsi-keys-only",
                "KeySchema": [{"AttributeName": "gsi_pk", "KeyType": "HASH"}],
                "Projection": {"ProjectionType": "KEYS_ONLY"},
            }
        ],
    )
    yield table_name
    dynamodb.delete_table(TableName=table_name)


# ---------------------------------------------------------------------------
# GSI operations
# ---------------------------------------------------------------------------


class TestGSIAdvanced:
    def test_gsi_query_with_sort_key(self, dynamodb, gsi_composite_table):
        """Query GSI that has both hash and range key."""
        for i in range(5):
            dynamodb.put_item(
                TableName=gsi_composite_table,
                Item={
                    "pk": {"S": f"item-{i}"},
                    "gsi_pk": {"S": "dept-eng"},
                    "gsi_sk": {"S": f"emp-{i:03d}"},
                    "name": {"S": f"person-{i}"},
                },
            )
        response = dynamodb.query(
            TableName=gsi_composite_table,
            IndexName="gsi-composite",
            KeyConditionExpression="gsi_pk = :gpk AND gsi_sk BETWEEN :lo AND :hi",
            ExpressionAttributeValues={
                ":gpk": {"S": "dept-eng"},
                ":lo": {"S": "emp-001"},
                ":hi": {"S": "emp-003"},
            },
        )
        assert response["Count"] == 3
        sks = [item["gsi_sk"]["S"] for item in response["Items"]]
        assert sks == ["emp-001", "emp-002", "emp-003"]

    def test_gsi_keys_only_projection(self, dynamodb, gsi_keys_only_table):
        """KEYS_ONLY GSI returns only key attributes."""
        dynamodb.put_item(
            TableName=gsi_keys_only_table,
            Item={
                "pk": {"S": "ko-1"},
                "gsi_pk": {"S": "grp-a"},
                "extra": {"S": "should-not-appear"},
            },
        )
        response = dynamodb.query(
            TableName=gsi_keys_only_table,
            IndexName="gsi-keys-only",
            KeyConditionExpression="gsi_pk = :gpk",
            ExpressionAttributeValues={":gpk": {"S": "grp-a"}},
        )
        assert response["Count"] == 1
        item = response["Items"][0]
        assert "pk" in item
        assert "gsi_pk" in item
        assert "extra" not in item

    def test_gsi_project_specific_attributes(self, dynamodb):
        """GSI with INCLUDE projection returns only specified attributes."""
        table_name = f"test-gsi-incl-{uuid.uuid4().hex[:8]}"
        try:
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
                        "IndexName": "gsi-include",
                        "KeySchema": [{"AttributeName": "gsi_pk", "KeyType": "HASH"}],
                        "Projection": {
                            "ProjectionType": "INCLUDE",
                            "NonKeyAttributes": ["included_attr"],
                        },
                    }
                ],
            )
            dynamodb.put_item(
                TableName=table_name,
                Item={
                    "pk": {"S": "inc-1"},
                    "gsi_pk": {"S": "grp-b"},
                    "included_attr": {"S": "yes"},
                    "excluded_attr": {"S": "no"},
                },
            )
            response = dynamodb.query(
                TableName=table_name,
                IndexName="gsi-include",
                KeyConditionExpression="gsi_pk = :gpk",
                ExpressionAttributeValues={":gpk": {"S": "grp-b"}},
            )
            assert response["Count"] == 1
            item = response["Items"][0]
            assert item["included_attr"]["S"] == "yes"
            assert "excluded_attr" not in item
        finally:
            dynamodb.delete_table(TableName=table_name)

    def test_gsi_scan(self, dynamodb, gsi_table):
        """Scan a GSI returns all items indexed."""
        for i in range(3):
            dynamodb.put_item(
                TableName=gsi_table,
                Item={
                    "pk": {"S": f"gs-{i}"},
                    "gsi_pk": {"S": f"category-{i}"},
                },
            )
        response = dynamodb.scan(TableName=gsi_table, IndexName="gsi-index")
        assert response["Count"] >= 3


# ---------------------------------------------------------------------------
# LSI operations
# ---------------------------------------------------------------------------


class TestLocalSecondaryIndex:
    def test_lsi_table_has_index(self, dynamodb, lsi_table):
        """Describe table shows the LSI."""
        response = dynamodb.describe_table(TableName=lsi_table)
        lsis = response["Table"]["LocalSecondaryIndexes"]
        assert len(lsis) == 1
        assert lsis[0]["IndexName"] == "lsi-index"

    def test_query_on_lsi(self, dynamodb, lsi_table):
        """Query using LSI alternate sort key."""
        dynamodb.put_item(
            TableName=lsi_table,
            Item={
                "pk": {"S": "user-1"},
                "sk": {"S": "order-001"},
                "lsi_sk": {"S": "2024-01-15"},
                "amount": {"N": "100"},
            },
        )
        dynamodb.put_item(
            TableName=lsi_table,
            Item={
                "pk": {"S": "user-1"},
                "sk": {"S": "order-002"},
                "lsi_sk": {"S": "2024-01-10"},
                "amount": {"N": "200"},
            },
        )
        dynamodb.put_item(
            TableName=lsi_table,
            Item={
                "pk": {"S": "user-1"},
                "sk": {"S": "order-003"},
                "lsi_sk": {"S": "2024-01-20"},
                "amount": {"N": "50"},
            },
        )

        response = dynamodb.query(
            TableName=lsi_table,
            IndexName="lsi-index",
            KeyConditionExpression="pk = :pk AND lsi_sk BETWEEN :lo AND :hi",
            ExpressionAttributeValues={
                ":pk": {"S": "user-1"},
                ":lo": {"S": "2024-01-10"},
                ":hi": {"S": "2024-01-15"},
            },
        )
        assert response["Count"] == 2
        dates = [item["lsi_sk"]["S"] for item in response["Items"]]
        assert dates == ["2024-01-10", "2024-01-15"]

    def test_lsi_query_scan_forward_false(self, dynamodb, lsi_table):
        """Query LSI in reverse order."""
        for i in range(3):
            dynamodb.put_item(
                TableName=lsi_table,
                Item={
                    "pk": {"S": "rev"},
                    "sk": {"S": f"s-{i}"},
                    "lsi_sk": {"S": f"lsi-{i:03d}"},
                },
            )
        response = dynamodb.query(
            TableName=lsi_table,
            IndexName="lsi-index",
            KeyConditionExpression="pk = :pk",
            ExpressionAttributeValues={":pk": {"S": "rev"}},
            ScanIndexForward=False,
        )
        lsi_sks = [item["lsi_sk"]["S"] for item in response["Items"]]
        assert lsi_sks == ["lsi-002", "lsi-001", "lsi-000"]


# ---------------------------------------------------------------------------
# Batch operations (advanced)
# ---------------------------------------------------------------------------


class TestBatchOperationsAdvanced:
    def test_batch_write_put_and_delete(self, dynamodb, table):
        """BatchWriteItem with both PutRequest and DeleteRequest."""
        # Pre-insert items to delete
        for i in range(3):
            dynamodb.put_item(
                TableName=table,
                Item={"pk": {"S": f"bwd-del-{i}"}, "val": {"S": "old"}},
            )

        dynamodb.batch_write_item(
            RequestItems={
                table: [
                    # Delete 3 existing items
                    {"DeleteRequest": {"Key": {"pk": {"S": "bwd-del-0"}}}},
                    {"DeleteRequest": {"Key": {"pk": {"S": "bwd-del-1"}}}},
                    {"DeleteRequest": {"Key": {"pk": {"S": "bwd-del-2"}}}},
                    # Put 2 new items
                    {"PutRequest": {"Item": {"pk": {"S": "bwd-new-0"}, "val": {"S": "fresh"}}}},
                    {"PutRequest": {"Item": {"pk": {"S": "bwd-new-1"}, "val": {"S": "fresh"}}}},
                ]
            }
        )

        # Verify deletes
        for i in range(3):
            r = dynamodb.get_item(TableName=table, Key={"pk": {"S": f"bwd-del-{i}"}})
            assert "Item" not in r

        # Verify puts
        for i in range(2):
            r = dynamodb.get_item(TableName=table, Key={"pk": {"S": f"bwd-new-{i}"}})
            assert r["Item"]["val"]["S"] == "fresh"

    def test_batch_get_item_with_projection(self, dynamodb, table):
        """BatchGetItem with ProjectionExpression returns only requested attrs."""
        for i in range(3):
            dynamodb.put_item(
                TableName=table,
                Item={
                    "pk": {"S": f"bgp-{i}"},
                    "name": {"S": f"name-{i}"},
                    "secret": {"S": "hidden"},
                },
            )

        response = dynamodb.batch_get_item(
            RequestItems={
                table: {
                    "Keys": [{"pk": {"S": f"bgp-{i}"}} for i in range(3)],
                    "ProjectionExpression": "pk, #n",
                    "ExpressionAttributeNames": {"#n": "name"},
                }
            }
        )
        items = response["Responses"][table]
        assert len(items) == 3
        for item in items:
            assert "pk" in item
            assert "name" in item
            assert "secret" not in item

    def test_batch_write_multiple_tables(self, dynamodb):
        """BatchWriteItem across two tables."""
        t1 = f"test-bwm1-{uuid.uuid4().hex[:8]}"
        t2 = f"test-bwm2-{uuid.uuid4().hex[:8]}"
        try:
            for t in [t1, t2]:
                dynamodb.create_table(
                    TableName=t,
                    KeySchema=[{"AttributeName": "pk", "KeyType": "HASH"}],
                    AttributeDefinitions=[{"AttributeName": "pk", "AttributeType": "S"}],
                    BillingMode="PAY_PER_REQUEST",
                )

            dynamodb.batch_write_item(
                RequestItems={
                    t1: [{"PutRequest": {"Item": {"pk": {"S": "t1-a"}, "src": {"S": "table1"}}}}],
                    t2: [{"PutRequest": {"Item": {"pk": {"S": "t2-a"}, "src": {"S": "table2"}}}}],
                }
            )

            r1 = dynamodb.get_item(TableName=t1, Key={"pk": {"S": "t1-a"}})
            assert r1["Item"]["src"]["S"] == "table1"
            r2 = dynamodb.get_item(TableName=t2, Key={"pk": {"S": "t2-a"}})
            assert r2["Item"]["src"]["S"] == "table2"
        finally:
            dynamodb.delete_table(TableName=t1)
            dynamodb.delete_table(TableName=t2)


# ---------------------------------------------------------------------------
# Conditional expressions (advanced)
# ---------------------------------------------------------------------------


class TestConditionalWritesAdvanced:
    def test_update_item_with_condition_succeeds(self, dynamodb, table):
        """UpdateItem with ConditionExpression succeeds when met."""
        dynamodb.put_item(
            TableName=table,
            Item={"pk": {"S": "uc-ok"}, "status": {"S": "pending"}, "count": {"N": "0"}},
        )
        dynamodb.update_item(
            TableName=table,
            Key={"pk": {"S": "uc-ok"}},
            UpdateExpression="SET #s = :new_status",
            ConditionExpression="#s = :expected",
            ExpressionAttributeNames={"#s": "status"},
            ExpressionAttributeValues={
                ":new_status": {"S": "active"},
                ":expected": {"S": "pending"},
            },
        )
        r = dynamodb.get_item(TableName=table, Key={"pk": {"S": "uc-ok"}})
        assert r["Item"]["status"]["S"] == "active"

    def test_update_item_with_condition_fails(self, dynamodb, table):
        """UpdateItem with failing condition raises error."""
        dynamodb.put_item(
            TableName=table,
            Item={"pk": {"S": "uc-fail"}, "status": {"S": "active"}},
        )
        with pytest.raises(ClientError) as exc_info:
            dynamodb.update_item(
                TableName=table,
                Key={"pk": {"S": "uc-fail"}},
                UpdateExpression="SET #s = :val",
                ConditionExpression="#s = :expected",
                ExpressionAttributeNames={"#s": "status"},
                ExpressionAttributeValues={
                    ":val": {"S": "closed"},
                    ":expected": {"S": "pending"},
                },
            )
        assert exc_info.value.response["Error"]["Code"] == "ConditionalCheckFailedException"

    def test_delete_item_with_condition_succeeds(self, dynamodb, table):
        """DeleteItem with ConditionExpression succeeds when met."""
        dynamodb.put_item(
            TableName=table,
            Item={"pk": {"S": "dc-ok"}, "deletable": {"BOOL": True}},
        )
        dynamodb.delete_item(
            TableName=table,
            Key={"pk": {"S": "dc-ok"}},
            ConditionExpression="deletable = :val",
            ExpressionAttributeValues={":val": {"BOOL": True}},
        )
        r = dynamodb.get_item(TableName=table, Key={"pk": {"S": "dc-ok"}})
        assert "Item" not in r

    def test_delete_item_with_condition_fails(self, dynamodb, table):
        """DeleteItem with failing condition raises error."""
        dynamodb.put_item(
            TableName=table,
            Item={"pk": {"S": "dc-fail"}, "deletable": {"BOOL": False}},
        )
        with pytest.raises(ClientError) as exc_info:
            dynamodb.delete_item(
                TableName=table,
                Key={"pk": {"S": "dc-fail"}},
                ConditionExpression="deletable = :val",
                ExpressionAttributeValues={":val": {"BOOL": True}},
            )
        assert exc_info.value.response["Error"]["Code"] == "ConditionalCheckFailedException"

    def test_condition_attribute_exists(self, dynamodb, table):
        """ConditionExpression with attribute_exists."""
        dynamodb.put_item(
            TableName=table,
            Item={"pk": {"S": "ae-test"}, "marker": {"S": "present"}},
        )
        # Should succeed because 'marker' exists
        dynamodb.update_item(
            TableName=table,
            Key={"pk": {"S": "ae-test"}},
            UpdateExpression="SET marker = :val",
            ConditionExpression="attribute_exists(marker)",
            ExpressionAttributeValues={":val": {"S": "updated"}},
        )
        r = dynamodb.get_item(TableName=table, Key={"pk": {"S": "ae-test"}})
        assert r["Item"]["marker"]["S"] == "updated"


# ---------------------------------------------------------------------------
# Filter expressions
# ---------------------------------------------------------------------------


class TestFilterExpressions:
    def test_query_with_filter_expression(self, dynamodb, composite_table):
        """Query with FilterExpression filters results after key evaluation."""
        for i in range(5):
            dynamodb.put_item(
                TableName=composite_table,
                Item={
                    "pk": {"S": "qf-user"},
                    "sk": {"S": f"item-{i:03d}"},
                    "active": {"BOOL": i % 2 == 0},
                },
            )
        response = dynamodb.query(
            TableName=composite_table,
            KeyConditionExpression="pk = :pk",
            FilterExpression="active = :val",
            ExpressionAttributeValues={
                ":pk": {"S": "qf-user"},
                ":val": {"BOOL": True},
            },
        )
        # Items 0, 2, 4 are active
        assert response["Count"] == 3

    def test_scan_filter_with_contains(self, dynamodb, table):
        """Scan with FilterExpression using contains function."""
        dynamodb.put_item(
            TableName=table,
            Item={"pk": {"S": "fc-1"}, "tags": {"SS": ["python", "aws"]}},
        )
        dynamodb.put_item(
            TableName=table,
            Item={"pk": {"S": "fc-2"}, "tags": {"SS": ["java", "aws"]}},
        )
        dynamodb.put_item(
            TableName=table,
            Item={"pk": {"S": "fc-3"}, "tags": {"SS": ["rust", "linux"]}},
        )
        response = dynamodb.scan(
            TableName=table,
            FilterExpression="contains(tags, :tag)",
            ExpressionAttributeValues={":tag": {"S": "aws"}},
        )
        assert response["Count"] == 2
        pks = sorted(item["pk"]["S"] for item in response["Items"])
        assert pks == ["fc-1", "fc-2"]

    def test_scan_filter_with_size(self, dynamodb, table):
        """Scan with FilterExpression using size function."""
        dynamodb.put_item(
            TableName=table,
            Item={"pk": {"S": "sz-1"}, "data": {"S": "ab"}},
        )
        dynamodb.put_item(
            TableName=table,
            Item={"pk": {"S": "sz-2"}, "data": {"S": "abcdef"}},
        )
        response = dynamodb.scan(
            TableName=table,
            FilterExpression="size(#d) > :len",
            ExpressionAttributeNames={"#d": "data"},
            ExpressionAttributeValues={":len": {"N": "3"}},
        )
        assert response["Count"] == 1
        assert response["Items"][0]["pk"]["S"] == "sz-2"

    def test_scan_filter_not_equals(self, dynamodb, table):
        """Scan with FilterExpression using <> operator."""
        for i in range(4):
            dynamodb.put_item(
                TableName=table,
                Item={
                    "pk": {"S": f"ne-{i}"},
                    "color": {"S": "red" if i < 2 else "blue"},
                },
            )
        response = dynamodb.scan(
            TableName=table,
            FilterExpression="color <> :val",
            ExpressionAttributeValues={":val": {"S": "red"}},
        )
        assert response["Count"] == 2


# ---------------------------------------------------------------------------
# Update expressions (advanced)
# ---------------------------------------------------------------------------


class TestUpdateExpressionsAdvanced:
    def test_set_nested_attribute(self, dynamodb, table):
        """SET a nested map attribute."""
        dynamodb.put_item(
            TableName=table,
            Item={
                "pk": {"S": "nest-1"},
                "profile": {"M": {"name": {"S": "alice"}, "age": {"N": "30"}}},
            },
        )
        dynamodb.update_item(
            TableName=table,
            Key={"pk": {"S": "nest-1"}},
            UpdateExpression="SET profile.age = :age",
            ExpressionAttributeValues={":age": {"N": "31"}},
        )
        r = dynamodb.get_item(TableName=table, Key={"pk": {"S": "nest-1"}})
        assert r["Item"]["profile"]["M"]["age"]["N"] == "31"
        assert r["Item"]["profile"]["M"]["name"]["S"] == "alice"

    def test_add_to_number(self, dynamodb, table):
        """ADD increments a number attribute."""
        dynamodb.put_item(
            TableName=table,
            Item={"pk": {"S": "add-num"}, "cnt": {"N": "10"}},
        )
        dynamodb.update_item(
            TableName=table,
            Key={"pk": {"S": "add-num"}},
            UpdateExpression="ADD cnt :inc",
            ExpressionAttributeValues={":inc": {"N": "5"}},
        )
        r = dynamodb.get_item(TableName=table, Key={"pk": {"S": "add-num"}})
        assert r["Item"]["cnt"]["N"] == "15"

    def test_add_to_set(self, dynamodb, table):
        """ADD elements to a string set."""
        dynamodb.put_item(
            TableName=table,
            Item={"pk": {"S": "add-set"}, "colors": {"SS": ["red", "blue"]}},
        )
        dynamodb.update_item(
            TableName=table,
            Key={"pk": {"S": "add-set"}},
            UpdateExpression="ADD colors :vals",
            ExpressionAttributeValues={":vals": {"SS": ["green", "blue"]}},
        )
        r = dynamodb.get_item(TableName=table, Key={"pk": {"S": "add-set"}})
        colors = set(r["Item"]["colors"]["SS"])
        assert colors == {"red", "blue", "green"}

    def test_delete_from_set(self, dynamodb, table):
        """DELETE elements from a string set."""
        dynamodb.put_item(
            TableName=table,
            Item={"pk": {"S": "del-set"}, "tags": {"SS": ["a", "b", "c", "d"]}},
        )
        dynamodb.update_item(
            TableName=table,
            Key={"pk": {"S": "del-set"}},
            UpdateExpression="DELETE tags :vals",
            ExpressionAttributeValues={":vals": {"SS": ["b", "d"]}},
        )
        r = dynamodb.get_item(TableName=table, Key={"pk": {"S": "del-set"}})
        tags = set(r["Item"]["tags"]["SS"])
        assert tags == {"a", "c"}

    def test_add_creates_nonexistent_number(self, dynamodb, table):
        """ADD on a nonexistent attribute creates it."""
        dynamodb.put_item(
            TableName=table,
            Item={"pk": {"S": "add-new"}},
        )
        dynamodb.update_item(
            TableName=table,
            Key={"pk": {"S": "add-new"}},
            UpdateExpression="ADD new_cnt :val",
            ExpressionAttributeValues={":val": {"N": "42"}},
        )
        r = dynamodb.get_item(TableName=table, Key={"pk": {"S": "add-new"}})
        assert r["Item"]["new_cnt"]["N"] == "42"

    def test_set_if_not_exists(self, dynamodb, table):
        """SET with if_not_exists to provide a default value."""
        dynamodb.put_item(
            TableName=table,
            Item={"pk": {"S": "ine-1"}, "existing": {"S": "yes"}},
        )
        dynamodb.update_item(
            TableName=table,
            Key={"pk": {"S": "ine-1"}},
            UpdateExpression=(
                "SET existing = if_not_exists(existing, :def1),"
                " new_attr = if_not_exists(new_attr, :def2)"
            ),
            ExpressionAttributeValues={
                ":def1": {"S": "default"},
                ":def2": {"S": "created"},
            },
        )
        r = dynamodb.get_item(TableName=table, Key={"pk": {"S": "ine-1"}})
        assert r["Item"]["existing"]["S"] == "yes"  # Not overwritten
        assert r["Item"]["new_attr"]["S"] == "created"  # Created with default


# ---------------------------------------------------------------------------
# Projection expressions
# ---------------------------------------------------------------------------


class TestProjectionExpressions:
    def test_get_item_with_projection(self, dynamodb, table):
        """GetItem with ProjectionExpression returns only requested attrs."""
        dynamodb.put_item(
            TableName=table,
            Item={
                "pk": {"S": "proj-1"},
                "name": {"S": "alice"},
                "age": {"N": "30"},
                "secret": {"S": "hidden"},
            },
        )
        response = dynamodb.get_item(
            TableName=table,
            Key={"pk": {"S": "proj-1"}},
            ProjectionExpression="pk, #n",
            ExpressionAttributeNames={"#n": "name"},
        )
        assert "pk" in response["Item"]
        assert "name" in response["Item"]
        assert "age" not in response["Item"]
        assert "secret" not in response["Item"]

    def test_get_item_nested_projection(self, dynamodb, table):
        """GetItem projection on nested map attributes."""
        dynamodb.put_item(
            TableName=table,
            Item={
                "pk": {"S": "proj-nest"},
                "profile": {
                    "M": {
                        "name": {"S": "bob"},
                        "address": {
                            "M": {
                                "city": {"S": "NYC"},
                                "zip": {"S": "10001"},
                            }
                        },
                    }
                },
                "other": {"S": "excluded"},
            },
        )
        response = dynamodb.get_item(
            TableName=table,
            Key={"pk": {"S": "proj-nest"}},
            ProjectionExpression="profile.address.city",
        )
        item = response["Item"]
        assert "other" not in item
        assert item["profile"]["M"]["address"]["M"]["city"]["S"] == "NYC"

    def test_query_with_projection(self, dynamodb, composite_table):
        """Query with ProjectionExpression."""
        dynamodb.put_item(
            TableName=composite_table,
            Item={
                "pk": {"S": "qp-user"},
                "sk": {"S": "item-1"},
                "visible": {"S": "yes"},
                "hidden": {"S": "no"},
            },
        )
        response = dynamodb.query(
            TableName=composite_table,
            KeyConditionExpression="pk = :pk",
            ProjectionExpression="pk, sk, visible",
            ExpressionAttributeValues={":pk": {"S": "qp-user"}},
        )
        assert response["Count"] == 1
        item = response["Items"][0]
        assert "visible" in item
        assert "hidden" not in item

    def test_scan_with_projection(self, dynamodb, table):
        """Scan with ProjectionExpression limits returned attributes."""
        dynamodb.put_item(
            TableName=table,
            Item={
                "pk": {"S": "sp-1"},
                "keep": {"S": "yes"},
                "drop": {"S": "no"},
            },
        )
        response = dynamodb.scan(
            TableName=table,
            FilterExpression="pk = :pk",
            ProjectionExpression="pk, keep",
            ExpressionAttributeValues={":pk": {"S": "sp-1"}},
        )
        assert response["Count"] == 1
        item = response["Items"][0]
        assert "keep" in item
        assert "drop" not in item


# ---------------------------------------------------------------------------
# Transactions (advanced)
# ---------------------------------------------------------------------------


class TestTransactionsAdvanced:
    def test_transact_write_with_update(self, dynamodb, table):
        """TransactWriteItems with Update operation."""
        dynamodb.put_item(
            TableName=table,
            Item={"pk": {"S": "twu-1"}, "balance": {"N": "100"}},
        )
        dynamodb.put_item(
            TableName=table,
            Item={"pk": {"S": "twu-2"}, "balance": {"N": "50"}},
        )

        # Transfer 30 from twu-1 to twu-2
        dynamodb.transact_write_items(
            TransactItems=[
                {
                    "Update": {
                        "TableName": table,
                        "Key": {"pk": {"S": "twu-1"}},
                        "UpdateExpression": "SET balance = balance - :amt",
                        "ExpressionAttributeValues": {":amt": {"N": "30"}},
                    }
                },
                {
                    "Update": {
                        "TableName": table,
                        "Key": {"pk": {"S": "twu-2"}},
                        "UpdateExpression": "SET balance = balance + :amt",
                        "ExpressionAttributeValues": {":amt": {"N": "30"}},
                    }
                },
            ]
        )

        r1 = dynamodb.get_item(TableName=table, Key={"pk": {"S": "twu-1"}})
        assert r1["Item"]["balance"]["N"] == "70"
        r2 = dynamodb.get_item(TableName=table, Key={"pk": {"S": "twu-2"}})
        assert r2["Item"]["balance"]["N"] == "80"

    def test_transact_get_items(self, dynamodb, table):
        """transact_get_items retrieves multiple items transactionally."""
        for i in range(3):
            dynamodb.put_item(
                TableName=table,
                Item={"pk": {"S": f"tg-{i}"}, "data": {"S": f"value-{i}"}},
            )

        response = dynamodb.transact_get_items(
            TransactItems=[
                {"Get": {"TableName": table, "Key": {"pk": {"S": f"tg-{i}"}}}} for i in range(3)
            ]
        )
        items = [r["Item"] for r in response["Responses"]]
        assert len(items) == 3
        vals = sorted(item["data"]["S"] for item in items)
        assert vals == ["value-0", "value-1", "value-2"]

    def test_transact_get_items_with_projection(self, dynamodb, table):
        """TransactGetItems with ProjectionExpression returns requested attrs."""
        dynamodb.put_item(
            TableName=table,
            Item={"pk": {"S": "tgp-1"}, "visible": {"S": "alice"}, "extra": {"S": "data"}},
        )
        response = dynamodb.transact_get_items(
            TransactItems=[
                {
                    "Get": {
                        "TableName": table,
                        "Key": {"pk": {"S": "tgp-1"}},
                        "ProjectionExpression": "pk, visible",
                    }
                }
            ]
        )
        item = response["Responses"][0]["Item"]
        assert "pk" in item
        assert "visible" in item

    def test_transact_write_put_update_delete(self, dynamodb, table):
        """TransactWriteItems with Put, Update, and Delete in one transaction."""
        dynamodb.put_item(
            TableName=table,
            Item={"pk": {"S": "tpud-upd"}, "count": {"N": "1"}},
        )
        dynamodb.put_item(
            TableName=table,
            Item={"pk": {"S": "tpud-del"}, "val": {"S": "bye"}},
        )

        dynamodb.transact_write_items(
            TransactItems=[
                {
                    "Put": {
                        "TableName": table,
                        "Item": {"pk": {"S": "tpud-new"}, "val": {"S": "hello"}},
                    }
                },
                {
                    "Update": {
                        "TableName": table,
                        "Key": {"pk": {"S": "tpud-upd"}},
                        "UpdateExpression": "SET #c = #c + :inc",
                        "ExpressionAttributeNames": {"#c": "count"},
                        "ExpressionAttributeValues": {":inc": {"N": "9"}},
                    }
                },
                {
                    "Delete": {
                        "TableName": table,
                        "Key": {"pk": {"S": "tpud-del"}},
                    }
                },
            ]
        )

        r_new = dynamodb.get_item(TableName=table, Key={"pk": {"S": "tpud-new"}})
        assert r_new["Item"]["val"]["S"] == "hello"
        r_upd = dynamodb.get_item(TableName=table, Key={"pk": {"S": "tpud-upd"}})
        assert r_upd["Item"]["count"]["N"] == "10"
        r_del = dynamodb.get_item(TableName=table, Key={"pk": {"S": "tpud-del"}})
        assert "Item" not in r_del


# ---------------------------------------------------------------------------
# TTL (advanced)
# ---------------------------------------------------------------------------


class TestTTLAdvanced:
    def test_disable_ttl(self, dynamodb, table):
        """Enable then disable TTL."""
        dynamodb.update_time_to_live(
            TableName=table,
            TimeToLiveSpecification={"Enabled": True, "AttributeName": "ttl_field"},
        )
        dynamodb.update_time_to_live(
            TableName=table,
            TimeToLiveSpecification={"Enabled": False, "AttributeName": "ttl_field"},
        )
        response = dynamodb.describe_time_to_live(TableName=table)
        assert response["TimeToLiveDescription"]["TimeToLiveStatus"] in (
            "DISABLED",
            "DISABLING",
        )


# ---------------------------------------------------------------------------
# Table tags
# ---------------------------------------------------------------------------


class TestTableTags:
    def test_tag_and_list_tags(self, dynamodb, table):
        """TagResource and ListTagsOfResource."""
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

    def test_untag_resource(self, dynamodb, table):
        """UntagResource removes specific tags."""
        desc = dynamodb.describe_table(TableName=table)
        table_arn = desc["Table"]["TableArn"]

        dynamodb.tag_resource(
            ResourceArn=table_arn,
            Tags=[
                {"Key": "keep", "Value": "yes"},
                {"Key": "remove", "Value": "bye"},
            ],
        )
        dynamodb.untag_resource(ResourceArn=table_arn, TagKeys=["remove"])

        response = dynamodb.list_tags_of_resource(ResourceArn=table_arn)
        tag_keys = [t["Key"] for t in response["Tags"]]
        assert "keep" in tag_keys
        assert "remove" not in tag_keys

    def test_tag_overwrite(self, dynamodb, table):
        """Tagging with an existing key overwrites the value."""
        desc = dynamodb.describe_table(TableName=table)
        table_arn = desc["Table"]["TableArn"]

        dynamodb.tag_resource(
            ResourceArn=table_arn,
            Tags=[{"Key": "version", "Value": "1"}],
        )
        dynamodb.tag_resource(
            ResourceArn=table_arn,
            Tags=[{"Key": "version", "Value": "2"}],
        )
        response = dynamodb.list_tags_of_resource(ResourceArn=table_arn)
        tags = {t["Key"]: t["Value"] for t in response["Tags"]}
        assert tags["version"] == "2"


# ---------------------------------------------------------------------------
# ExpressionAttributeNames for reserved words
# ---------------------------------------------------------------------------


class TestExpressionAttributeNames:
    def test_reserved_word_in_update(self, dynamodb, table):
        """Use ExpressionAttributeNames for reserved word 'status'."""
        dynamodb.put_item(
            TableName=table,
            Item={"pk": {"S": "ean-1"}, "status": {"S": "open"}},
        )
        dynamodb.update_item(
            TableName=table,
            Key={"pk": {"S": "ean-1"}},
            UpdateExpression="SET #s = :val",
            ExpressionAttributeNames={"#s": "status"},
            ExpressionAttributeValues={":val": {"S": "closed"}},
        )
        r = dynamodb.get_item(TableName=table, Key={"pk": {"S": "ean-1"}})
        assert r["Item"]["status"]["S"] == "closed"

    def test_reserved_word_in_condition(self, dynamodb, table):
        """Use ExpressionAttributeNames for reserved word in condition."""
        dynamodb.put_item(
            TableName=table,
            Item={"pk": {"S": "ean-2"}, "comment": {"S": "hello"}},
        )
        dynamodb.update_item(
            TableName=table,
            Key={"pk": {"S": "ean-2"}},
            UpdateExpression="SET #c = :val",
            ConditionExpression="#c = :expected",
            ExpressionAttributeNames={"#c": "comment"},
            ExpressionAttributeValues={
                ":val": {"S": "updated"},
                ":expected": {"S": "hello"},
            },
        )
        r = dynamodb.get_item(TableName=table, Key={"pk": {"S": "ean-2"}})
        assert r["Item"]["comment"]["S"] == "updated"

    def test_reserved_word_in_filter(self, dynamodb, table):
        """Use ExpressionAttributeNames for reserved word in FilterExpression."""
        dynamodb.put_item(
            TableName=table,
            Item={"pk": {"S": "ean-3"}, "timestamp": {"N": "100"}},
        )
        dynamodb.put_item(
            TableName=table,
            Item={"pk": {"S": "ean-4"}, "timestamp": {"N": "200"}},
        )
        response = dynamodb.scan(
            TableName=table,
            FilterExpression="#ts > :val",
            ExpressionAttributeNames={"#ts": "timestamp"},
            ExpressionAttributeValues={":val": {"N": "150"}},
        )
        assert response["Count"] >= 1
        for item in response["Items"]:
            assert int(item["timestamp"]["N"]) > 150

    def test_multiple_reserved_words(self, dynamodb, table):
        """Use multiple ExpressionAttributeNames in one operation."""
        dynamodb.put_item(
            TableName=table,
            Item={
                "pk": {"S": "ean-5"},
                "name": {"S": "test"},
                "size": {"N": "10"},
                "comment": {"S": "initial"},
            },
        )
        dynamodb.update_item(
            TableName=table,
            Key={"pk": {"S": "ean-5"}},
            UpdateExpression="SET #n = :name, #s = :size, #c = :comment",
            ExpressionAttributeNames={
                "#n": "name",
                "#s": "size",
                "#c": "comment",
            },
            ExpressionAttributeValues={
                ":name": {"S": "updated"},
                ":size": {"N": "20"},
                ":comment": {"S": "changed"},
            },
        )
        r = dynamodb.get_item(TableName=table, Key={"pk": {"S": "ean-5"}})
        assert r["Item"]["name"]["S"] == "updated"
        assert r["Item"]["size"]["N"] == "20"
        assert r["Item"]["comment"]["S"] == "changed"


# ---------------------------------------------------------------------------
# Describe table details
# ---------------------------------------------------------------------------


class TestDescribeTable:
    def test_describe_table_key_schema(self, dynamodb, table):
        """DescribeTable returns correct key schema."""
        response = dynamodb.describe_table(TableName=table)
        td = response["Table"]
        assert td["TableName"] == table
        assert td["KeySchema"] == [{"AttributeName": "pk", "KeyType": "HASH"}]

    def test_describe_table_billing_mode(self, dynamodb, table):
        """DescribeTable shows PAY_PER_REQUEST billing mode."""
        response = dynamodb.describe_table(TableName=table)
        td = response["Table"]
        assert td.get("BillingModeSummary", {}).get("BillingMode") == "PAY_PER_REQUEST"

    def test_describe_table_item_count(self, dynamodb, table):
        """DescribeTable has ItemCount field."""
        response = dynamodb.describe_table(TableName=table)
        td = response["Table"]
        assert "ItemCount" in td
        assert isinstance(td["ItemCount"], int)

    def test_describe_nonexistent_table(self, dynamodb):
        """Describing a nonexistent table raises ResourceNotFoundException."""
        with pytest.raises(ClientError) as exc_info:
            dynamodb.describe_table(TableName=f"nonexistent-{uuid.uuid4().hex[:8]}")
        assert exc_info.value.response["Error"]["Code"] == "ResourceNotFoundException"


# ---------------------------------------------------------------------------
# Query advanced patterns
# ---------------------------------------------------------------------------


class TestQueryAdvanced:
    def test_query_with_limit(self, dynamodb, composite_table):
        """Query with Limit returns at most that many items."""
        for i in range(10):
            dynamodb.put_item(
                TableName=composite_table,
                Item={
                    "pk": {"S": "lim-user"},
                    "sk": {"S": f"item-{i:03d}"},
                },
            )
        response = dynamodb.query(
            TableName=composite_table,
            KeyConditionExpression="pk = :pk",
            ExpressionAttributeValues={":pk": {"S": "lim-user"}},
            Limit=3,
        )
        assert response["Count"] == 3
        assert len(response["Items"]) == 3

    def test_query_scan_forward_false(self, dynamodb, composite_table):
        """Query with ScanIndexForward=False returns items in reverse order."""
        for i in range(5):
            dynamodb.put_item(
                TableName=composite_table,
                Item={
                    "pk": {"S": "rev-user"},
                    "sk": {"S": f"sk-{i:03d}"},
                },
            )
        response = dynamodb.query(
            TableName=composite_table,
            KeyConditionExpression="pk = :pk",
            ExpressionAttributeValues={":pk": {"S": "rev-user"}},
            ScanIndexForward=False,
        )
        sks = [item["sk"]["S"] for item in response["Items"]]
        assert sks == ["sk-004", "sk-003", "sk-002", "sk-001", "sk-000"]

    def test_query_count_select(self, dynamodb, composite_table):
        """Query with Select=COUNT returns count without items."""
        for i in range(5):
            dynamodb.put_item(
                TableName=composite_table,
                Item={"pk": {"S": "cnt-user"}, "sk": {"S": f"i-{i}"}},
            )
        response = dynamodb.query(
            TableName=composite_table,
            KeyConditionExpression="pk = :pk",
            ExpressionAttributeValues={":pk": {"S": "cnt-user"}},
            Select="COUNT",
        )
        assert response["Count"] == 5
        assert len(response.get("Items", [])) == 0


# ---------------------------------------------------------------------------
# Scan advanced patterns
# ---------------------------------------------------------------------------


class TestScanAdvanced:
    def test_scan_with_limit(self, dynamodb, table):
        """Scan with Limit returns at most that many items."""
        for i in range(10):
            dynamodb.put_item(
                TableName=table,
                Item={"pk": {"S": f"sl-{i}"}},
            )
        response = dynamodb.scan(TableName=table, Limit=5)
        assert len(response["Items"]) <= 5

    def test_scan_with_exclusive_start_key(self, dynamodb, table):
        """Scan with ExclusiveStartKey for pagination."""
        for i in range(5):
            dynamodb.put_item(
                TableName=table,
                Item={"pk": {"S": f"pag-{i}"}},
            )
        # First page
        r1 = dynamodb.scan(TableName=table, Limit=2)
        assert len(r1["Items"]) <= 2
        if "LastEvaluatedKey" in r1:
            # Second page
            r2 = dynamodb.scan(
                TableName=table,
                Limit=2,
                ExclusiveStartKey=r1["LastEvaluatedKey"],
            )
            assert len(r2["Items"]) >= 1


# ---------------------------------------------------------------------------
# Error handling
# ---------------------------------------------------------------------------


class TestErrorHandling:
    def test_get_item_missing_key(self, dynamodb, table):
        """GetItem for nonexistent key returns no Item."""
        r = dynamodb.get_item(TableName=table, Key={"pk": {"S": "does-not-exist"}})
        assert "Item" not in r

    def test_delete_nonexistent_table(self, dynamodb):
        """Deleting a nonexistent table raises ResourceNotFoundException."""
        with pytest.raises(ClientError) as exc_info:
            dynamodb.delete_table(TableName=f"gone-{uuid.uuid4().hex[:8]}")
        assert exc_info.value.response["Error"]["Code"] == "ResourceNotFoundException"

    def test_create_duplicate_table(self, dynamodb, table):
        """Creating a table that already exists raises ResourceInUseException."""
        with pytest.raises(ClientError) as exc_info:
            dynamodb.create_table(
                TableName=table,
                KeySchema=[{"AttributeName": "pk", "KeyType": "HASH"}],
                AttributeDefinitions=[{"AttributeName": "pk", "AttributeType": "S"}],
                BillingMode="PAY_PER_REQUEST",
            )
        assert exc_info.value.response["Error"]["Code"] == "ResourceInUseException"


class TestDescribeEndpointsAndLimits:
    def test_describe_endpoints(self, dynamodb):
        """DescribeEndpoints returns a list of endpoints."""
        response = dynamodb.describe_endpoints()
        assert "Endpoints" in response
        assert isinstance(response["Endpoints"], list)
        assert len(response["Endpoints"]) >= 1
        # Each endpoint has Address and CachePeriodInMinutes
        ep = response["Endpoints"][0]
        assert "Address" in ep
        assert "CachePeriodInMinutes" in ep

    def test_describe_limits(self, dynamodb):
        """DescribeLimits returns account-level DynamoDB limits."""
        response = dynamodb.describe_limits()
        assert "AccountMaxReadCapacityUnits" in response
        assert "AccountMaxWriteCapacityUnits" in response
        assert "TableMaxReadCapacityUnits" in response
        assert "TableMaxWriteCapacityUnits" in response


class TestDynamoDBExtendedOperations:
    """Extended DynamoDB operations: backups, global tables, PartiQL, tags, etc."""

    def test_describe_continuous_backups(self, dynamodb, table):
        """DescribeContinuousBackups returns point-in-time recovery status."""
        response = dynamodb.describe_continuous_backups(TableName=table)
        cb = response["ContinuousBackupsDescription"]
        assert "ContinuousBackupsStatus" in cb
        assert cb["ContinuousBackupsStatus"] in ("ENABLED", "DISABLED")

    def test_create_and_delete_backup(self, dynamodb, table):
        """CreateBackup, DescribeBackup, ListBackups, DeleteBackup lifecycle."""
        backup_name = f"backup-{uuid.uuid4().hex[:8]}"
        try:
            create_resp = dynamodb.create_backup(TableName=table, BackupName=backup_name)
            backup_arn = create_resp["BackupDetails"]["BackupArn"]
            assert create_resp["BackupDetails"]["BackupName"] == backup_name
            assert create_resp["BackupDetails"]["BackupStatus"] in (
                "CREATING",
                "AVAILABLE",
            )

            # DescribeBackup
            desc_resp = dynamodb.describe_backup(BackupArn=backup_arn)
            assert desc_resp["BackupDescription"]["BackupDetails"]["BackupArn"] == backup_arn

            # ListBackups
            list_resp = dynamodb.list_backups(TableName=table)
            arns = [b["BackupArn"] for b in list_resp["BackupSummaries"]]
            assert backup_arn in arns

            # DeleteBackup
            del_resp = dynamodb.delete_backup(BackupArn=backup_arn)
            assert del_resp["BackupDescription"]["BackupDetails"]["BackupStatus"] in (
                "DELETED",
                "DELETING",
                "AVAILABLE",
            )
        except ClientError:
            raise

    def test_create_and_describe_global_table(self, dynamodb):
        """CreateGlobalTable, DescribeGlobalTable, ListGlobalTables."""
        table_name = f"global-tbl-{uuid.uuid4().hex[:8]}"
        try:
            # Must create table first
            dynamodb.create_table(
                TableName=table_name,
                KeySchema=[{"AttributeName": "pk", "KeyType": "HASH"}],
                AttributeDefinitions=[{"AttributeName": "pk", "AttributeType": "S"}],
                BillingMode="PAY_PER_REQUEST",
                StreamSpecification={
                    "StreamEnabled": True,
                    "StreamViewType": "NEW_AND_OLD_IMAGES",
                },
            )

            create_resp = dynamodb.create_global_table(
                GlobalTableName=table_name,
                ReplicationGroup=[{"RegionName": "us-east-1"}],
            )
            assert "GlobalTableDescription" in create_resp
            assert create_resp["GlobalTableDescription"]["GlobalTableName"] == table_name

            desc_resp = dynamodb.describe_global_table(GlobalTableName=table_name)
            assert desc_resp["GlobalTableDescription"]["GlobalTableName"] == table_name

            list_resp = dynamodb.list_global_tables()
            names = [gt["GlobalTableName"] for gt in list_resp["GlobalTables"]]
            assert table_name in names
        finally:
            dynamodb.delete_table(TableName=table_name)

    def test_execute_statement_partiql_select(self, dynamodb, table):
        """ExecuteStatement with PartiQL SELECT."""
        dynamodb.put_item(
            TableName=table,
            Item={"pk": {"S": "partiql-1"}, "val": {"S": "hello"}},
        )
        dynamodb.put_item(
            TableName=table,
            Item={"pk": {"S": "partiql-2"}, "val": {"S": "world"}},
        )
        response = dynamodb.execute_statement(
            Statement=f"SELECT * FROM \"{table}\" WHERE pk = 'partiql-1'"
        )
        assert "Items" in response
        assert len(response["Items"]) == 1
        assert response["Items"][0]["pk"]["S"] == "partiql-1"

    def test_batch_execute_statement(self, dynamodb, table):
        """BatchExecuteStatement with multiple PartiQL statements."""
        dynamodb.put_item(
            TableName=table,
            Item={"pk": {"S": "bexec-1"}, "val": {"S": "a"}},
        )
        dynamodb.put_item(
            TableName=table,
            Item={"pk": {"S": "bexec-2"}, "val": {"S": "b"}},
        )
        response = dynamodb.batch_execute_statement(
            Statements=[
                {"Statement": f"SELECT * FROM \"{table}\" WHERE pk = 'bexec-1'"},
                {"Statement": f"SELECT * FROM \"{table}\" WHERE pk = 'bexec-2'"},
            ]
        )
        assert "Responses" in response
        assert len(response["Responses"]) == 2

    def test_scan_with_filter_and_limit(self, dynamodb, table):
        """Scan with FilterExpression and Limit parameter."""
        for i in range(10):
            dynamodb.put_item(
                TableName=table,
                Item={"pk": {"S": f"scanlim-{i}"}, "category": {"S": "A" if i < 7 else "B"}},
            )
        response = dynamodb.scan(
            TableName=table,
            FilterExpression="category = :cat",
            ExpressionAttributeValues={":cat": {"S": "A"}},
            Limit=3,
        )
        # Limit applies before filter, so we may get fewer than 3 matching items
        assert response["ScannedCount"] <= 3
        for item in response["Items"]:
            assert item["category"]["S"] == "A"

    def test_update_item_add_to_number(self, dynamodb, table):
        """UpdateItem with ADD on a numeric attribute."""
        dynamodb.put_item(
            TableName=table,
            Item={"pk": {"S": "add-num"}, "cnt": {"N": "10"}},
        )
        dynamodb.update_item(
            TableName=table,
            Key={"pk": {"S": "add-num"}},
            UpdateExpression="ADD cnt :inc",
            ExpressionAttributeValues={":inc": {"N": "5"}},
        )
        response = dynamodb.get_item(TableName=table, Key={"pk": {"S": "add-num"}})
        assert response["Item"]["cnt"]["N"] == "15"

    def test_update_item_add_to_set(self, dynamodb, table):
        """UpdateItem with ADD on a string set attribute."""
        dynamodb.put_item(
            TableName=table,
            Item={"pk": {"S": "add-set"}, "tags": {"SS": ["alpha", "beta"]}},
        )
        dynamodb.update_item(
            TableName=table,
            Key={"pk": {"S": "add-set"}},
            UpdateExpression="ADD tags :newtags",
            ExpressionAttributeValues={":newtags": {"SS": ["gamma"]}},
        )
        response = dynamodb.get_item(TableName=table, Key={"pk": {"S": "add-set"}})
        assert set(response["Item"]["tags"]["SS"]) == {"alpha", "beta", "gamma"}

    def test_update_item_delete_from_set(self, dynamodb, table):
        """UpdateItem with DELETE on a string set attribute."""
        dynamodb.put_item(
            TableName=table,
            Item={"pk": {"S": "del-set"}, "tags": {"SS": ["x", "y", "z"]}},
        )
        dynamodb.update_item(
            TableName=table,
            Key={"pk": {"S": "del-set"}},
            UpdateExpression="DELETE tags :rm",
            ExpressionAttributeValues={":rm": {"SS": ["y"]}},
        )
        response = dynamodb.get_item(TableName=table, Key={"pk": {"S": "del-set"}})
        assert set(response["Item"]["tags"]["SS"]) == {"x", "z"}

    def test_delete_item_with_condition_expression(self, dynamodb, table):
        """DeleteItem with ConditionExpression succeeds when condition met."""
        dynamodb.put_item(
            TableName=table,
            Item={"pk": {"S": "condel"}, "status": {"S": "inactive"}},
        )
        dynamodb.delete_item(
            TableName=table,
            Key={"pk": {"S": "condel"}},
            ConditionExpression="#s = :expected",
            ExpressionAttributeNames={"#s": "status"},
            ExpressionAttributeValues={":expected": {"S": "inactive"}},
        )
        response = dynamodb.get_item(TableName=table, Key={"pk": {"S": "condel"}})
        assert "Item" not in response

    def test_delete_item_condition_fails(self, dynamodb, table):
        """DeleteItem with ConditionExpression fails when condition not met."""
        dynamodb.put_item(
            TableName=table,
            Item={"pk": {"S": "condel-fail"}, "status": {"S": "active"}},
        )
        with pytest.raises(ClientError) as exc_info:
            dynamodb.delete_item(
                TableName=table,
                Key={"pk": {"S": "condel-fail"}},
                ConditionExpression="#s = :expected",
                ExpressionAttributeNames={"#s": "status"},
                ExpressionAttributeValues={":expected": {"S": "inactive"}},
            )
        assert exc_info.value.response["Error"]["Code"] == "ConditionalCheckFailedException"

    def test_describe_table_replica_auto_scaling(self, dynamodb, table):
        """DescribeTableReplicaAutoScaling returns auto scaling info."""
        response = dynamodb.describe_table_replica_auto_scaling(TableName=table)
        assert "TableAutoScalingDescription" in response

    def test_tag_resource(self, dynamodb, table):
        """TagResource, UntagResource, ListTagsOfResource on a table."""
        # Get the table ARN
        desc = dynamodb.describe_table(TableName=table)
        table_arn = desc["Table"]["TableArn"]

        # Tag the table
        dynamodb.tag_resource(
            ResourceArn=table_arn,
            Tags=[
                {"Key": "env", "Value": "test"},
                {"Key": "project", "Value": "robotocore"},
            ],
        )

        # List tags
        tag_resp = dynamodb.list_tags_of_resource(ResourceArn=table_arn)
        tag_map = {t["Key"]: t["Value"] for t in tag_resp["Tags"]}
        assert tag_map["env"] == "test"
        assert tag_map["project"] == "robotocore"

        # Untag
        dynamodb.untag_resource(ResourceArn=table_arn, TagKeys=["env"])
        tag_resp2 = dynamodb.list_tags_of_resource(ResourceArn=table_arn)
        tag_keys = [t["Key"] for t in tag_resp2["Tags"]]
        assert "env" not in tag_keys
        assert "project" in tag_keys


class TestDynamoDBMoreOperations:
    """Additional DynamoDB operations for higher coverage."""

    @pytest.fixture
    def dynamodb(self):
        from tests.compatibility.conftest import make_client

        return make_client("dynamodb")

    @pytest.fixture
    def table(self, dynamodb):
        name = f"more-ops-{uuid.uuid4().hex[:8]}"
        dynamodb.create_table(
            TableName=name,
            KeySchema=[{"AttributeName": "pk", "KeyType": "HASH"}],
            AttributeDefinitions=[{"AttributeName": "pk", "AttributeType": "S"}],
            BillingMode="PAY_PER_REQUEST",
        )
        yield name
        dynamodb.delete_table(TableName=name)

    def test_list_tables(self, dynamodb, table):
        resp = dynamodb.list_tables()
        assert table in resp["TableNames"]

    def test_list_tables_with_limit(self, dynamodb, table):
        resp = dynamodb.list_tables(Limit=100)
        assert "TableNames" in resp
        assert table in resp["TableNames"]

    def test_update_table_add_gsi(self, dynamodb):
        """UpdateTable to add a Global Secondary Index."""
        name = f"gsi-table-{uuid.uuid4().hex[:8]}"
        dynamodb.create_table(
            TableName=name,
            KeySchema=[{"AttributeName": "pk", "KeyType": "HASH"}],
            AttributeDefinitions=[
                {"AttributeName": "pk", "AttributeType": "S"},
            ],
            BillingMode="PAY_PER_REQUEST",
        )
        try:
            dynamodb.update_table(
                TableName=name,
                AttributeDefinitions=[
                    {"AttributeName": "gsi_key", "AttributeType": "S"},
                ],
                GlobalSecondaryIndexUpdates=[
                    {
                        "Create": {
                            "IndexName": "gsi-index",
                            "KeySchema": [{"AttributeName": "gsi_key", "KeyType": "HASH"}],
                            "Projection": {"ProjectionType": "ALL"},
                        }
                    }
                ],
            )
            desc = dynamodb.describe_table(TableName=name)
            gsi_names = [g["IndexName"] for g in desc["Table"].get("GlobalSecondaryIndexes", [])]
            assert "gsi-index" in gsi_names
        finally:
            dynamodb.delete_table(TableName=name)

    def test_transact_write_items(self, dynamodb, table):
        """TransactWriteItems with Put and ConditionCheck."""
        dynamodb.transact_write_items(
            TransactItems=[
                {
                    "Put": {
                        "TableName": table,
                        "Item": {"pk": {"S": "txn-1"}, "val": {"S": "a"}},
                    }
                },
                {
                    "Put": {
                        "TableName": table,
                        "Item": {"pk": {"S": "txn-2"}, "val": {"S": "b"}},
                    }
                },
            ]
        )
        r1 = dynamodb.get_item(TableName=table, Key={"pk": {"S": "txn-1"}})
        r2 = dynamodb.get_item(TableName=table, Key={"pk": {"S": "txn-2"}})
        assert r1["Item"]["val"]["S"] == "a"
        assert r2["Item"]["val"]["S"] == "b"

    def test_transact_get_items(self, dynamodb, table):
        """TransactGetItems reads multiple items atomically."""
        dynamodb.put_item(TableName=table, Item={"pk": {"S": "tg-1"}, "v": {"N": "10"}})
        dynamodb.put_item(TableName=table, Item={"pk": {"S": "tg-2"}, "v": {"N": "20"}})
        resp = dynamodb.transact_get_items(
            TransactItems=[
                {"Get": {"TableName": table, "Key": {"pk": {"S": "tg-1"}}}},
                {"Get": {"TableName": table, "Key": {"pk": {"S": "tg-2"}}}},
            ]
        )
        assert len(resp["Responses"]) == 2
        vals = [int(r["Item"]["v"]["N"]) for r in resp["Responses"]]
        assert set(vals) == {10, 20}

    def test_update_item_return_values(self, dynamodb, table):
        """UpdateItem with ReturnValues=ALL_NEW."""
        dynamodb.put_item(TableName=table, Item={"pk": {"S": "rv-1"}, "count": {"N": "5"}})
        resp = dynamodb.update_item(
            TableName=table,
            Key={"pk": {"S": "rv-1"}},
            UpdateExpression="SET #c = #c + :inc",
            ExpressionAttributeNames={"#c": "count"},
            ExpressionAttributeValues={":inc": {"N": "3"}},
            ReturnValues="ALL_NEW",
        )
        assert resp["Attributes"]["count"]["N"] == "8"

    def test_update_item_return_values_all_old(self, dynamodb, table):
        """UpdateItem with ReturnValues=ALL_OLD."""
        dynamodb.put_item(TableName=table, Item={"pk": {"S": "rv-old"}, "v": {"S": "before"}})
        resp = dynamodb.update_item(
            TableName=table,
            Key={"pk": {"S": "rv-old"}},
            UpdateExpression="SET v = :new",
            ExpressionAttributeValues={":new": {"S": "after"}},
            ReturnValues="ALL_OLD",
        )
        assert resp["Attributes"]["v"]["S"] == "before"

    def test_put_item_return_values(self, dynamodb, table):
        """PutItem with ReturnValues=ALL_OLD for overwrite."""
        dynamodb.put_item(TableName=table, Item={"pk": {"S": "pv-1"}, "x": {"S": "old"}})
        resp = dynamodb.put_item(
            TableName=table,
            Item={"pk": {"S": "pv-1"}, "x": {"S": "new"}},
            ReturnValues="ALL_OLD",
        )
        assert resp["Attributes"]["x"]["S"] == "old"

    def test_query_with_begins_with(self, dynamodb):
        """Query with begins_with on sort key."""
        name = f"begins-{uuid.uuid4().hex[:8]}"
        dynamodb.create_table(
            TableName=name,
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
        try:
            for suffix in ["order#001", "order#002", "user#001"]:
                dynamodb.put_item(TableName=name, Item={"pk": {"S": "main"}, "sk": {"S": suffix}})
            resp = dynamodb.query(
                TableName=name,
                KeyConditionExpression="pk = :pk AND begins_with(sk, :prefix)",
                ExpressionAttributeValues={
                    ":pk": {"S": "main"},
                    ":prefix": {"S": "order#"},
                },
            )
            assert resp["Count"] == 2
            for item in resp["Items"]:
                assert item["sk"]["S"].startswith("order#")
        finally:
            dynamodb.delete_table(TableName=name)

    def test_query_between(self, dynamodb):
        """Query with BETWEEN on sort key."""
        name = f"between-{uuid.uuid4().hex[:8]}"
        dynamodb.create_table(
            TableName=name,
            KeySchema=[
                {"AttributeName": "pk", "KeyType": "HASH"},
                {"AttributeName": "sk", "KeyType": "RANGE"},
            ],
            AttributeDefinitions=[
                {"AttributeName": "pk", "AttributeType": "S"},
                {"AttributeName": "sk", "AttributeType": "N"},
            ],
            BillingMode="PAY_PER_REQUEST",
        )
        try:
            for i in range(1, 11):
                dynamodb.put_item(TableName=name, Item={"pk": {"S": "data"}, "sk": {"N": str(i)}})
            resp = dynamodb.query(
                TableName=name,
                KeyConditionExpression="pk = :pk AND sk BETWEEN :lo AND :hi",
                ExpressionAttributeValues={
                    ":pk": {"S": "data"},
                    ":lo": {"N": "3"},
                    ":hi": {"N": "7"},
                },
            )
            assert resp["Count"] == 5
        finally:
            dynamodb.delete_table(TableName=name)

    def test_execute_statement_insert(self, dynamodb, table):
        """ExecuteStatement with PartiQL INSERT."""
        dynamodb.execute_statement(
            Statement=f"INSERT INTO \"{table}\" VALUE {{'pk': 'partiql-ins', 'v': 'inserted'}}"
        )
        r = dynamodb.get_item(TableName=table, Key={"pk": {"S": "partiql-ins"}})
        assert r["Item"]["v"]["S"] == "inserted"

    def test_execute_statement_update(self, dynamodb, table):
        """ExecuteStatement with PartiQL UPDATE."""
        dynamodb.put_item(TableName=table, Item={"pk": {"S": "partiql-upd"}, "v": {"S": "old"}})
        dynamodb.execute_statement(
            Statement=f"UPDATE \"{table}\" SET v='new' WHERE pk='partiql-upd'"
        )
        r = dynamodb.get_item(TableName=table, Key={"pk": {"S": "partiql-upd"}})
        assert r["Item"]["v"]["S"] == "new"

    def test_execute_statement_delete(self, dynamodb, table):
        """ExecuteStatement with PartiQL DELETE."""
        dynamodb.put_item(TableName=table, Item={"pk": {"S": "partiql-del"}, "v": {"S": "bye"}})
        dynamodb.execute_statement(Statement=f"DELETE FROM \"{table}\" WHERE pk='partiql-del'")
        r = dynamodb.get_item(TableName=table, Key={"pk": {"S": "partiql-del"}})
        assert "Item" not in r

    def test_scan_with_projection_expression(self, dynamodb, table):
        """Scan with ProjectionExpression to return only specific attributes."""
        dynamodb.put_item(
            TableName=table,
            Item={"pk": {"S": "proj-1"}, "a": {"S": "x"}, "b": {"S": "y"}, "c": {"S": "z"}},
        )
        resp = dynamodb.scan(
            TableName=table,
            FilterExpression="pk = :pk",
            ExpressionAttributeValues={":pk": {"S": "proj-1"}},
            ProjectionExpression="pk, a",
        )
        assert len(resp["Items"]) >= 1
        item = resp["Items"][0]
        assert "pk" in item
        assert "a" in item
        assert "b" not in item

    def test_update_item_remove(self, dynamodb, table):
        """UpdateItem with REMOVE to delete an attribute."""
        dynamodb.put_item(
            TableName=table,
            Item={"pk": {"S": "rm-1"}, "keep": {"S": "yes"}, "drop": {"S": "no"}},
        )
        dynamodb.update_item(
            TableName=table,
            Key={"pk": {"S": "rm-1"}},
            UpdateExpression="REMOVE #d",
            ExpressionAttributeNames={"#d": "drop"},
        )
        r = dynamodb.get_item(TableName=table, Key={"pk": {"S": "rm-1"}})
        assert "keep" in r["Item"]
        assert "drop" not in r["Item"]


class TestDynamoDBAdvanced:
    @pytest.fixture
    def dynamodb(self):
        return make_client("dynamodb")

    @pytest.fixture
    def table(self, dynamodb):
        import uuid

        name = f"adv-table-{uuid.uuid4().hex[:8]}"
        dynamodb.create_table(
            TableName=name,
            AttributeDefinitions=[{"AttributeName": "pk", "AttributeType": "S"}],
            KeySchema=[{"AttributeName": "pk", "KeyType": "HASH"}],
            BillingMode="PAY_PER_REQUEST",
        )
        yield name
        dynamodb.delete_table(TableName=name)

    def test_batch_write_item(self, dynamodb, table):
        dynamodb.batch_write_item(
            RequestItems={
                table: [
                    {"PutRequest": {"Item": {"pk": {"S": f"bw-{i}"}, "v": {"N": str(i)}}}}
                    for i in range(5)
                ]
            }
        )
        resp = dynamodb.scan(TableName=table)
        assert resp["Count"] >= 5

    def test_batch_get_item(self, dynamodb, table):
        for i in range(3):
            dynamodb.put_item(TableName=table, Item={"pk": {"S": f"bg-{i}"}, "v": {"S": "val"}})
        resp = dynamodb.batch_get_item(
            RequestItems={table: {"Keys": [{"pk": {"S": f"bg-{i}"}} for i in range(3)]}}
        )
        assert len(resp["Responses"][table]) == 3

    def test_conditional_put_item(self, dynamodb, table):
        dynamodb.put_item(TableName=table, Item={"pk": {"S": "cond-1"}, "v": {"S": "exists"}})
        with pytest.raises(ClientError) as exc:
            dynamodb.put_item(
                TableName=table,
                Item={"pk": {"S": "cond-1"}, "v": {"S": "new"}},
                ConditionExpression="attribute_not_exists(pk)",
            )
        assert "ConditionalCheckFailed" in exc.value.response["Error"]["Code"]

    def test_update_item_add_number(self, dynamodb, table):
        dynamodb.put_item(TableName=table, Item={"pk": {"S": "add-1"}, "count": {"N": "10"}})
        dynamodb.update_item(
            TableName=table,
            Key={"pk": {"S": "add-1"}},
            UpdateExpression="ADD #c :inc",
            ExpressionAttributeNames={"#c": "count"},
            ExpressionAttributeValues={":inc": {"N": "5"}},
        )
        r = dynamodb.get_item(TableName=table, Key={"pk": {"S": "add-1"}})
        assert r["Item"]["count"]["N"] == "15"

    def test_update_item_set_if_not_exists(self, dynamodb, table):
        dynamodb.put_item(TableName=table, Item={"pk": {"S": "ifne-1"}})
        dynamodb.update_item(
            TableName=table,
            Key={"pk": {"S": "ifne-1"}},
            UpdateExpression="SET v = if_not_exists(v, :default)",
            ExpressionAttributeValues={":default": {"S": "default_val"}},
        )
        r = dynamodb.get_item(TableName=table, Key={"pk": {"S": "ifne-1"}})
        assert r["Item"]["v"]["S"] == "default_val"

    def test_scan_filter_expression(self, dynamodb, table):
        for i in range(5):
            dynamodb.put_item(
                TableName=table,
                Item={"pk": {"S": f"sf-{i}"}, "num": {"N": str(i * 10)}},
            )
        resp = dynamodb.scan(
            TableName=table,
            FilterExpression="num > :threshold",
            ExpressionAttributeValues={":threshold": {"N": "20"}},
        )
        assert all(int(item["num"]["N"]) > 20 for item in resp["Items"])

    def test_scan_with_limit(self, dynamodb, table):
        for i in range(10):
            dynamodb.put_item(TableName=table, Item={"pk": {"S": f"lim-{i}"}})
        resp = dynamodb.scan(TableName=table, Limit=3)
        assert resp["Count"] <= 3

    def test_scan_pagination(self, dynamodb, table):
        for i in range(10):
            dynamodb.put_item(TableName=table, Item={"pk": {"S": f"pg-{i}"}})
        resp1 = dynamodb.scan(TableName=table, Limit=5)
        assert "LastEvaluatedKey" in resp1
        resp2 = dynamodb.scan(TableName=table, ExclusiveStartKey=resp1["LastEvaluatedKey"])
        total = resp1["Count"] + resp2["Count"]
        assert total >= 10

    def test_delete_item_return_values(self, dynamodb, table):
        dynamodb.put_item(TableName=table, Item={"pk": {"S": "del-rv"}, "v": {"S": "gone"}})
        resp = dynamodb.delete_item(
            TableName=table,
            Key={"pk": {"S": "del-rv"}},
            ReturnValues="ALL_OLD",
        )
        assert resp["Attributes"]["pk"]["S"] == "del-rv"
        assert resp["Attributes"]["v"]["S"] == "gone"

    def test_update_item_list_append(self, dynamodb, table):
        dynamodb.put_item(
            TableName=table,
            Item={"pk": {"S": "la-1"}, "vals": {"L": [{"S": "a"}]}},
        )
        dynamodb.update_item(
            TableName=table,
            Key={"pk": {"S": "la-1"}},
            UpdateExpression="SET vals = list_append(vals, :new)",
            ExpressionAttributeValues={":new": {"L": [{"S": "b"}, {"S": "c"}]}},
        )
        r = dynamodb.get_item(TableName=table, Key={"pk": {"S": "la-1"}})
        items = [v["S"] for v in r["Item"]["vals"]["L"]]
        assert items == ["a", "b", "c"]

    def test_describe_table_fields(self, dynamodb, table):
        resp = dynamodb.describe_table(TableName=table)
        t = resp["Table"]
        assert t["TableName"] == table
        assert "TableArn" in t
        assert "TableStatus" in t
        assert t["TableStatus"] == "ACTIVE"
        assert "KeySchema" in t
        assert "AttributeDefinitions" in t

    def test_describe_table_item_count(self, dynamodb, table):
        for i in range(3):
            dynamodb.put_item(TableName=table, Item={"pk": {"S": f"cnt-{i}"}})
        resp = dynamodb.describe_table(TableName=table)
        assert resp["Table"]["ItemCount"] >= 0  # May be approximate

    def test_put_item_with_map_type(self, dynamodb, table):
        dynamodb.put_item(
            TableName=table,
            Item={
                "pk": {"S": "map-1"},
                "nested": {"M": {"key1": {"S": "val1"}, "key2": {"N": "42"}}},
            },
        )
        r = dynamodb.get_item(TableName=table, Key={"pk": {"S": "map-1"}})
        assert r["Item"]["nested"]["M"]["key1"]["S"] == "val1"
        assert r["Item"]["nested"]["M"]["key2"]["N"] == "42"

    def test_put_item_with_boolean_and_null(self, dynamodb, table):
        dynamodb.put_item(
            TableName=table,
            Item={
                "pk": {"S": "types-1"},
                "flag": {"BOOL": True},
                "empty": {"NULL": True},
            },
        )
        r = dynamodb.get_item(TableName=table, Key={"pk": {"S": "types-1"}})
        assert r["Item"]["flag"]["BOOL"] is True
        assert r["Item"]["empty"]["NULL"] is True

    def test_put_item_with_binary(self, dynamodb, table):
        data = b"\x00\x01\x02\x03"
        dynamodb.put_item(
            TableName=table,
            Item={"pk": {"S": "bin-1"}, "data": {"B": data}},
        )
        r = dynamodb.get_item(TableName=table, Key={"pk": {"S": "bin-1"}})
        assert r["Item"]["data"]["B"] == data

    def test_put_item_with_string_set(self, dynamodb, table):
        dynamodb.put_item(
            TableName=table,
            Item={"pk": {"S": "ss-1"}, "tags": {"SS": ["a", "b", "c"]}},
        )
        r = dynamodb.get_item(TableName=table, Key={"pk": {"S": "ss-1"}})
        assert set(r["Item"]["tags"]["SS"]) == {"a", "b", "c"}

    def test_put_item_with_number_set(self, dynamodb, table):
        dynamodb.put_item(
            TableName=table,
            Item={"pk": {"S": "ns-1"}, "nums": {"NS": ["1", "2", "3"]}},
        )
        r = dynamodb.get_item(TableName=table, Key={"pk": {"S": "ns-1"}})
        assert set(r["Item"]["nums"]["NS"]) == {"1", "2", "3"}


class TestBackupRestore:
    """Tests for DynamoDB backup and restore operations."""

    def test_restore_table_from_backup(self, dynamodb):
        """CreateBackup then RestoreTableFromBackup creates a new table."""
        src = f"backup-src-{uuid.uuid4().hex[:8]}"
        target = f"backup-tgt-{uuid.uuid4().hex[:8]}"
        tables_to_delete = [src]
        try:
            dynamodb.create_table(
                TableName=src,
                KeySchema=[{"AttributeName": "pk", "KeyType": "HASH"}],
                AttributeDefinitions=[{"AttributeName": "pk", "AttributeType": "S"}],
                BillingMode="PAY_PER_REQUEST",
            )
            dynamodb.put_item(TableName=src, Item={"pk": {"S": "item-1"}, "v": {"S": "hello"}})

            backup_resp = dynamodb.create_backup(TableName=src, BackupName="restore-test")
            backup_arn = backup_resp["BackupDetails"]["BackupArn"]

            restore_resp = dynamodb.restore_table_from_backup(
                TargetTableName=target,
                BackupArn=backup_arn,
            )
            tables_to_delete.append(target)
            td = restore_resp["TableDescription"]
            assert td["TableName"] == target
            assert td["TableStatus"] in ("CREATING", "ACTIVE")

            dynamodb.delete_backup(BackupArn=backup_arn)
        finally:
            for t in tables_to_delete:
                try:
                    dynamodb.delete_table(TableName=t)
                except ClientError:
                    pass

    def test_restore_table_to_point_in_time(self, dynamodb):
        """RestoreTableToPointInTime creates a restored copy of a table."""
        src = f"pitr-src-{uuid.uuid4().hex[:8]}"
        target = f"pitr-tgt-{uuid.uuid4().hex[:8]}"
        tables_to_delete = [src]
        try:
            dynamodb.create_table(
                TableName=src,
                KeySchema=[{"AttributeName": "pk", "KeyType": "HASH"}],
                AttributeDefinitions=[{"AttributeName": "pk", "AttributeType": "S"}],
                BillingMode="PAY_PER_REQUEST",
            )

            restore_resp = dynamodb.restore_table_to_point_in_time(
                SourceTableName=src,
                TargetTableName=target,
                UseLatestRestorableTime=True,
            )
            tables_to_delete.append(target)
            td = restore_resp["TableDescription"]
            assert td["TableName"] == target
            assert td["TableStatus"] in ("CREATING", "ACTIVE")
        finally:
            for t in tables_to_delete:
                try:
                    dynamodb.delete_table(TableName=t)
                except ClientError:
                    pass

    def test_create_table_replica(self, dynamodb):
        """UpdateTable with ReplicaUpdates Create adds a replica."""
        tname = f"replica-src-{uuid.uuid4().hex[:8]}"
        try:
            dynamodb.create_table(
                TableName=tname,
                KeySchema=[{"AttributeName": "pk", "KeyType": "HASH"}],
                AttributeDefinitions=[{"AttributeName": "pk", "AttributeType": "S"}],
                BillingMode="PAY_PER_REQUEST",
                StreamSpecification={
                    "StreamEnabled": True,
                    "StreamViewType": "NEW_AND_OLD_IMAGES",
                },
            )
            resp = dynamodb.update_table(
                TableName=tname,
                ReplicaUpdates=[{"Create": {"RegionName": "eu-west-1"}}],
            )
            td = resp["TableDescription"]
            assert td["TableName"] == tname
            # Replicas should include eu-west-1
            if "Replicas" in td:
                regions = [r["RegionName"] for r in td["Replicas"]]
                assert "eu-west-1" in regions
        finally:
            try:
                dynamodb.delete_table(TableName=tname)
            except ClientError:
                pass


class TestDynamoDBGapStubs:
    """Tests for gap operations: describe_endpoints, list_exports."""

    def test_describe_endpoints(self, dynamodb):
        resp = dynamodb.describe_endpoints()
        assert "Endpoints" in resp
        assert len(resp["Endpoints"]) > 0
        endpoint = resp["Endpoints"][0]
        assert "Address" in endpoint
        assert "CachePeriodInMinutes" in endpoint

    def test_list_exports(self, dynamodb):
        resp = dynamodb.list_exports()
        assert "ExportSummaries" in resp
