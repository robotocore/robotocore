"""DynamoDB item-op validation and expression-semantics fidelity (moto 5.2.5).

Covers semantic-audit findings 1, 4, 5, 6, 7, 8, 13: key type/missing errors,
empty key/set/NULL-GSI write validation, unresolved expression-attribute
names, ADD/DELETE operand typing, set equality semantics, and GetItem's
invalid-projection behavior. Every 400 text mirrors moto exactly.
"""

import os
import uuid

import boto3
import pytest
from botocore.exceptions import ClientError

ENDPOINT_URL = os.environ.get("ENDPOINT_URL", "http://localhost:4566")


@pytest.fixture
def ddb():
    return boto3.client(
        "dynamodb",
        endpoint_url=ENDPOINT_URL,
        region_name="us-east-1",
        aws_access_key_id="testing",
        aws_secret_access_key="testing",
    )


def err_code(exc):
    return exc.value.response["Error"]["Code"]


def err_msg(exc):
    return exc.value.response["Error"]["Message"]


@pytest.fixture
def table(ddb):
    name = f"tx5-{uuid.uuid4().hex[:10]}"
    ddb.create_table(
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
    yield name
    try:
        items = ddb.scan(TableName=name).get("Items", [])
        for item in items:
            ddb.delete_item(
                TableName=name,
                Key={k: item[k] for k in ("pk", "sk") if k in item},
            )
        ddb.delete_table(TableName=name)
    except Exception:
        pass  # best-effort cleanup


@pytest.fixture
def gsi_table(ddb):
    name = f"gsi5-{uuid.uuid4().hex[:8]}"
    ddb.create_table(
        TableName=name,
        KeySchema=[{"AttributeName": "pk", "KeyType": "HASH"}],
        AttributeDefinitions=[
            {"AttributeName": "pk", "AttributeType": "S"},
            {"AttributeName": "gsik", "AttributeType": "S"},
        ],
        GlobalSecondaryIndexes=[
            {
                "IndexName": "by-gsik",
                "KeySchema": [{"AttributeName": "gsik", "KeyType": "HASH"}],
                "Projection": {"ProjectionType": "ALL"},
            }
        ],
        BillingMode="PAY_PER_REQUEST",
    )
    yield name
    try:
        items = ddb.scan(TableName=name).get("Items", [])
        for item in items:
            ddb.delete_item(TableName=name, Key={"pk": item["pk"]})
        ddb.delete_table(TableName=name)
    except Exception:
        pass  # best-effort cleanup


class TestItemKeyValidation:
    """moto models/table.py:627-654 + exceptions.py:299-303."""

    def test_missing_hash_key_text(self, ddb, table):
        with pytest.raises(ClientError) as exc:
            ddb.put_item(TableName=table, Item={"sk": {"N": "1"}})
        assert err_msg(exc) == (
            "One or more parameter values were invalid: Missing the key pk in the item"
        )

    def test_missing_range_key_text(self, ddb, table):
        with pytest.raises(ClientError) as exc:
            ddb.put_item(TableName=table, Item={"pk": {"S": "k"}})
        assert err_msg(exc).endswith("Missing the key sk in the item")

    def test_key_type_mismatch_text(self, ddb, table):
        # {"S":"5"} where the schema declares an N range key
        with pytest.raises(ClientError) as exc:
            ddb.put_item(TableName=table, Item={"pk": {"S": "k"}, "sk": {"S": "5"}})
        assert err_msg(exc) == (
            "One or more parameter values were invalid: Type mismatch for key sk "
            "expected: N actual: S"
        )

    def test_empty_string_key_rejected(self, ddb, table):
        with pytest.raises(ClientError) as exc:
            ddb.put_item(TableName=table, Item={"pk": {"S": ""}, "sk": {"N": "1"}})
        assert err_msg(exc) == (
            "One or more parameter values were invalid: An AttributeValue may not "
            "contain an empty string. Key: pk"
        )

    def test_empty_gsi_key_rejected_with_index_name(self, ddb, gsi_table):
        with pytest.raises(ClientError) as exc:
            ddb.put_item(
                TableName=gsi_table,
                Item={"pk": {"S": "a"}, "gsik": {"S": ""}},
            )
        assert err_msg(exc) == (
            "One or more parameter values are not valid. A value specified for a "
            "secondary index key is not supported. The AttributeValue for a key "
            "attribute cannot contain an empty string value. IndexName: by-gsik, "
            "IndexKey: gsik"
        )

    def test_empty_set_rejected(self, ddb, table):
        for tag, label in (("SS", "string"), ("NS", "number")):
            with pytest.raises(ClientError) as exc:
                ddb.put_item(
                    TableName=table,
                    Item={"pk": {"S": "k"}, "sk": {"N": "1"}, "tags": {tag: []}},
                )
            assert err_msg(exc) == (
                "1 validation error detected: One or more parameter values were "
                f"invalid: An {label} set  may not be empty"
            )

    def test_null_typed_gsi_key_rejected(self, ddb, gsi_table):
        with pytest.raises(ClientError) as exc:
            ddb.put_item(
                TableName=gsi_table,
                Item={"pk": {"S": "a"}, "gsik": {"NULL": True}},
            )
        assert err_msg(exc) == (
            "One or more parameter values were invalid: Type mismatch for Index "
            "Key gsik Expected: S Actual: NULL IndexName: by-gsik"
        )


class TestUpdateExpressionSemantics:
    """moto parsing/executors.py:177-272 + validators.py:108-112."""

    def seed(self, ddb, table):
        ddb.put_item(
            TableName=table,
            Item={"pk": {"S": "a"}, "sk": {"N": "1"}, "name": {"S": "x"}},
        )

    def test_add_number_onto_string_attr_rejected(self, ddb, table):
        self.seed(ddb, table)
        with pytest.raises(ClientError) as exc:
            ddb.update_item(
                TableName=table,
                Key={"pk": {"S": "a"}, "sk": {"N": "1"}},
                UpdateExpression="ADD name :inc",
                ExpressionAttributeValues={":inc": {"N": "5"}},
            )
        assert err_msg(exc) == ("An operand in the update expression has an incorrect data type")
        assert err_code(exc) == "ValidationException"

    def test_add_set_onto_string_attr_rejected(self, ddb, table):
        self.seed(ddb, table)
        with pytest.raises(ClientError) as exc:
            ddb.update_item(
                TableName=table,
                Key={"pk": {"S": "a"}, "sk": {"N": "1"}},
                UpdateExpression="ADD name :s",
                ExpressionAttributeValues={":s": {"SS": ["a"]}},
            )
        assert err_msg(exc).endswith("incorrect data type")

    def test_add_list_operand_rejected(self, ddb, table):
        self.seed(ddb, table)
        with pytest.raises(ClientError) as exc:
            ddb.update_item(
                TableName=table,
                Key={"pk": {"S": "a"}, "sk": {"N": "1"}},
                UpdateExpression="ADD name :l",
                ExpressionAttributeValues={":l": {"L": [{"S": "a"}]}},
            )
        assert err_msg(exc).endswith("incorrect data type")

    def test_delete_last_set_element_removes_attribute(self, ddb, table):
        self.seed(ddb, table)
        ddb.update_item(
            TableName=table,
            Key={"pk": {"S": "a"}, "sk": {"N": "1"}},
            UpdateExpression="SET sset = :v",
            ExpressionAttributeValues={":v": {"SS": ["only", "one"]}},
        )
        ddb.update_item(
            TableName=table,
            Key={"pk": {"S": "a"}, "sk": {"N": "1"}},
            UpdateExpression="DELETE sset :v",
            ExpressionAttributeValues={":v": {"SS": ["one", "only"]}},
        )
        item = ddb.get_item(TableName=table, Key={"pk": {"S": "a"}, "sk": {"N": "1"}})
        assert "sset" not in item["Item"]

    def test_add_empty_set_operand_rejected(self, ddb, table):
        self.seed(ddb, table)
        ddb.update_item(
            TableName=table,
            Key={"pk": {"S": "a"}, "sk": {"N": "1"}},
            UpdateExpression="SET sset = :v",
            ExpressionAttributeValues={":v": {"SS": ["x"]}},
        )
        with pytest.raises(ClientError) as exc:
            ddb.update_item(
                TableName=table,
                Key={"pk": {"S": "a"}, "sk": {"N": "1"}},
                UpdateExpression="ADD sset :empty",
                ExpressionAttributeValues={":empty": {"SS": []}},
            )
        assert "may not be empty" in err_msg(exc)

    def test_set_equality_is_order_and_format_insensitive(self, ddb, table):
        self.seed(ddb, table)
        ddb.update_item(
            TableName=table,
            Key={"pk": {"S": "a"}, "sk": {"N": "1"}},
            UpdateExpression="SET nset = :v",
            ExpressionAttributeValues={":v": {"NS": ["1"]}},
        )
        # numeric-equal element in a filtered set comparison
        resp = ddb.scan(
            TableName=table,
            FilterExpression="nset = :v",
            ExpressionAttributeValues={":v": {"NS": ["1.0"]}},
        )
        assert resp["ScannedCount"] == 1
        # and ADD dedups numerically-equal members instead of duplicating
        ddb.update_item(
            TableName=table,
            Key={"pk": {"S": "a"}, "sk": {"N": "1"}},
            UpdateExpression="ADD nset :one",
            ExpressionAttributeValues={":one": {"NS": ["1.0", "2"]}},
        )
        item = ddb.get_item(TableName=table, Key={"pk": {"S": "a"}, "sk": {"N": "1"}})["Item"]
        assert sorted(item["nset"]["NS"]) == ["1", "2"]

    def test_unresolved_name_in_update_rejected(self, ddb, table):
        self.seed(ddb, table)
        with pytest.raises(ClientError) as exc:
            ddb.update_item(
                TableName=table,
                Key={"pk": {"S": "a"}, "sk": {"N": "1"}},
                UpdateExpression="SET #nope = :v",
                ExpressionAttributeValues={":v": {"S": "x"}},
            )
        assert err_code(exc) == "ValidationException"
        assert err_msg(exc) == (
            "An expression attribute name used in the document path is not "
            "defined; attribute name: #nope"
        )
        item = ddb.get_item(TableName=table, Key={"pk": {"S": "a"}, "sk": {"N": "1"}})
        assert "#nope" not in item.get("Item", {})

    def test_unresolved_name_in_condition_rejected(self, ddb, table):
        self.seed(ddb, table)
        with pytest.raises(ClientError) as exc:
            ddb.update_item(
                TableName=table,
                Key={"pk": {"S": "a"}, "sk": {"N": "1"}},
                UpdateExpression="SET name = :v",
                ExpressionAttributeValues={":v": {"S": "y"}},
                ConditionExpression="#nope = :v",
            )
        assert err_code(exc) == "ValidationException"


class TestGetItemProjection:
    """moto responses.py:245-281 — GetItem rejects bad projections."""

    def test_duplicate_paths_rejected(self, ddb, table):
        ddb.put_item(
            TableName=table,
            Item={"pk": {"S": "a"}, "sk": {"N": "1"}, "name": {"S": "x"}},
        )
        with pytest.raises(ClientError) as exc:
            ddb.get_item(
                TableName=table,
                Key={"pk": {"S": "a"}, "sk": {"N": "1"}},
                ProjectionExpression="name, #n",
                ExpressionAttributeNames={"#n": "name"},
            )
        assert err_msg(exc) == (
            "1 validation error detected: Invalid ProjectionExpression: Two document "
            "paths overlap with each other; must remove or rewrite one of these "
            "paths; path one: [name], path two: [name]"
        )
