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


class TestScanWithFilterAdvanced:
    def test_scan_filter_contains(self, dynamodb, table):
        """Scan with FilterExpression using contains function."""
        dynamodb.put_item(
            TableName=table,
            Item={"pk": {"S": "sc-1"}, "tags": {"S": "python,aws,cloud"}},
        )
        dynamodb.put_item(
            TableName=table,
            Item={"pk": {"S": "sc-2"}, "tags": {"S": "java,azure"}},
        )
        dynamodb.put_item(
            TableName=table,
            Item={"pk": {"S": "sc-3"}, "tags": {"S": "python,gcp"}},
        )

        response = dynamodb.scan(
            TableName=table,
            FilterExpression="contains(tags, :lang)",
            ExpressionAttributeValues={":lang": {"S": "python"}},
        )
        assert response["Count"] == 2
        pks = sorted(item["pk"]["S"] for item in response["Items"])
        assert pks == ["sc-1", "sc-3"]

    def test_scan_with_projection_expression(self, dynamodb, table):
        """Scan with ProjectionExpression returns only requested attributes."""
        dynamodb.put_item(
            TableName=table,
            Item={
                "pk": {"S": "proj-1"},
                "name": {"S": "Alice"},
                "age": {"N": "30"},
                "email": {"S": "alice@example.com"},
            },
        )

        response = dynamodb.scan(
            TableName=table,
            FilterExpression="pk = :pk",
            ExpressionAttributeValues={":pk": {"S": "proj-1"}},
            ProjectionExpression="pk, #n",
            ExpressionAttributeNames={"#n": "name"},
        )
        assert response["Count"] == 1
        item = response["Items"][0]
        assert "pk" in item
        assert "name" in item
        assert "age" not in item
        assert "email" not in item


class TestQueryWithProjection:
    def test_query_with_projection_expression(self, dynamodb, composite_table):
        """Query with ProjectionExpression returns only specified attributes."""
        dynamodb.put_item(
            TableName=composite_table,
            Item={
                "pk": {"S": "proj-user"},
                "sk": {"S": "profile"},
                "name": {"S": "Bob"},
                "age": {"N": "25"},
                "email": {"S": "bob@example.com"},
            },
        )

        response = dynamodb.query(
            TableName=composite_table,
            KeyConditionExpression="pk = :pk",
            ExpressionAttributeValues={":pk": {"S": "proj-user"}},
            ProjectionExpression="pk, sk, #n",
            ExpressionAttributeNames={"#n": "name"},
        )
        assert response["Count"] == 1
        item = response["Items"][0]
        assert item["pk"]["S"] == "proj-user"
        assert item["name"]["S"] == "Bob"
        assert "age" not in item
        assert "email" not in item

    def test_query_scan_index_forward_false(self, dynamodb, composite_table):
        """Query with ScanIndexForward=False returns items in descending order."""
        for i in range(5):
            dynamodb.put_item(
                TableName=composite_table,
                Item={
                    "pk": {"S": "order-user"},
                    "sk": {"S": f"item-{i:03d}"},
                    "val": {"N": str(i)},
                },
            )

        response = dynamodb.query(
            TableName=composite_table,
            KeyConditionExpression="pk = :pk",
            ExpressionAttributeValues={":pk": {"S": "order-user"}},
            ScanIndexForward=False,
        )
        assert response["Count"] == 5
        sks = [item["sk"]["S"] for item in response["Items"]]
        assert sks == ["item-004", "item-003", "item-002", "item-001", "item-000"]


class TestDescribeTable:
    def test_describe_table_key_schema(self, dynamodb, table):
        """describe_table returns correct key schema for a hash-only table."""
        response = dynamodb.describe_table(TableName=table)
        td = response["Table"]
        assert td["TableName"] == table
        assert td["KeySchema"] == [{"AttributeName": "pk", "KeyType": "HASH"}]
        assert {"AttributeName": "pk", "AttributeType": "S"} in td["AttributeDefinitions"]
        assert "TableArn" in td
        assert td["TableStatus"] in ("ACTIVE", "CREATING")

    def test_describe_table_composite_key(self, dynamodb, composite_table):
        """describe_table returns correct key schema for a composite key table."""
        response = dynamodb.describe_table(TableName=composite_table)
        td = response["Table"]
        key_schema = td["KeySchema"]
        assert len(key_schema) == 2
        hash_key = [k for k in key_schema if k["KeyType"] == "HASH"]
        range_key = [k for k in key_schema if k["KeyType"] == "RANGE"]
        assert len(hash_key) == 1
        assert hash_key[0]["AttributeName"] == "pk"
        assert len(range_key) == 1
        assert range_key[0]["AttributeName"] == "sk"

    def test_describe_table_billing_mode(self, dynamodb, table):
        """describe_table includes billing mode summary."""
        response = dynamodb.describe_table(TableName=table)
        td = response["Table"]
        # PAY_PER_REQUEST tables have BillingModeSummary
        if "BillingModeSummary" in td:
            assert td["BillingModeSummary"]["BillingMode"] == "PAY_PER_REQUEST"


class TestTags:
    def test_tag_and_list_tags(self, dynamodb, table):
        """tag_resource and list_tags_of_resource work together."""
        # Get table ARN
        desc = dynamodb.describe_table(TableName=table)
        table_arn = desc["Table"]["TableArn"]

        dynamodb.tag_resource(
            ResourceArn=table_arn,
            Tags=[
                {"Key": "env", "Value": "test"},
                {"Key": "team", "Value": "backend"},
            ],
        )

        response = dynamodb.list_tags_of_resource(ResourceArn=table_arn)
        tags = {t["Key"]: t["Value"] for t in response["Tags"]}
        assert tags["env"] == "test"
        assert tags["team"] == "backend"

    def test_untag_resource(self, dynamodb, table):
        """untag_resource removes specified tags."""
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


class TestBatchOperationsAdvanced:
    def test_batch_write_delete(self, dynamodb, table):
        """batch_write_item can delete multiple items."""
        for i in range(3):
            dynamodb.put_item(
                TableName=table, Item={"pk": {"S": f"bd-{i}"}, "data": {"S": f"val-{i}"}}
            )

        dynamodb.batch_write_item(
            RequestItems={
                table: [
                    {"DeleteRequest": {"Key": {"pk": {"S": f"bd-{i}"}}}} for i in range(3)
                ]
            }
        )

        for i in range(3):
            resp = dynamodb.get_item(TableName=table, Key={"pk": {"S": f"bd-{i}"}})
            assert "Item" not in resp

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
            assert "pk" in item
            assert "name" in item
            assert "age" not in item
