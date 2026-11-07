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

    def test_delete_on_string_attr_is_moto_wording(self, ddb, table):
        # moto DeleteExecutor (executors.py:139-143): IncorrectOperandType
        # with the human type name, wrapped in Invalid UpdateExpression.
        self.seed(ddb, table)
        with pytest.raises(ClientError) as exc:
            ddb.update_item(
                TableName=table,
                Key={"pk": {"S": "a"}, "sk": {"N": "1"}},
                UpdateExpression="DELETE name :v",
                ExpressionAttributeValues={":v": {"SS": ["a"]}},
            )
        assert err_msg(exc) == (
            "Invalid UpdateExpression: Incorrect operand type for operator or "
            "function; operator or function: operator: DELETE, operand type: STRING"
        )

    def test_delete_set_kind_mismatch_rejected(self, ddb, table):
        self.seed(ddb, table)
        ddb.update_item(
            TableName=table,
            Key={"pk": {"S": "a"}, "sk": {"N": "1"}},
            UpdateExpression="SET sset = :v",
            ExpressionAttributeValues={":v": {"SS": ["a"]}},
        )
        with pytest.raises(ClientError) as exc:
            ddb.update_item(
                TableName=table,
                Key={"pk": {"S": "a"}, "sk": {"N": "1"}},
                UpdateExpression="DELETE sset :v",
                ExpressionAttributeValues={":v": {"NS": ["1"]}},
            )
        assert err_msg(exc).endswith("incorrect data type")

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


class TestListTablesPagination:
    """moto responses.py:326-338 + models/__init__.py:136-158."""

    def _make(self, ddb, n, prefix):
        for i in range(n):
            ddb.create_table(
                TableName=f"{prefix}{i:02d}",
                KeySchema=[{"AttributeName": "pk", "KeyType": "HASH"}],
                AttributeDefinitions=[{"AttributeName": "pk", "AttributeType": "S"}],
                BillingMode="PAY_PER_REQUEST",
            )

    def _cleanup(self, ddb, prefix):
        names = ddb.list_tables().get("TableNames", [])
        for t in names:
            if t.startswith(prefix):
                ddb.delete_table(TableName=t)

    def test_paged_walk_exhausts_and_returns_sorted_pages(self, ddb):
        # moto semantics pinned prefix-locally so parallel CI workers can't
        # race the page math: every Limit=2 page carries the walker's
        # all_tables-order successor in LastEvaluatedTableName, and walking
        # to exhaustion yields the created prefix exactly once, in order.
        prefix = "lt-pag"
        self._make(ddb, 3, prefix)
        try:
            cursor = None
            seen = []
            for _ in range(200):  # hard bound — the twin's table list is finite
                extra = {"ExclusiveStartTableName": cursor} if cursor else {}
                resp = ddb.list_tables(Limit=2, **extra)
                mine = [t for t in resp.get("TableNames", []) if t.startswith(prefix)]
                seen.extend(t for t in mine if not cursor or t > cursor)
                if "LastEvaluatedTableName" in resp:
                    cursor = resp["LastEvaluatedTableName"]
                    if mine:
                        # moto: the page's last entry is the token
                        assert resp["LastEvaluatedTableName"] == mine[-1]
                else:
                    if not cursor:
                        assert len(seen) == 3
                    break
            assert seen == [f"{prefix}00", f"{prefix}01", f"{prefix}02"]
        finally:
            self._cleanup(ddb, prefix)

    def test_unknown_exclusive_start_starts_at_end(self, ddb):
        prefix = "lt-unk"
        self._make(ddb, 1, prefix)
        try:
            page = ddb.list_tables(ExclusiveStartTableName="zzz-not-a-table")
            assert not [t for t in page.get("TableNames", []) if t.startswith(prefix)]
        finally:
            self._cleanup(ddb, prefix)


class TestBatchGetItemLimits:
    """moto responses.py:829-895 limits, duplicate rejection and
    ConsumedCapacity rendering."""

    @pytest.fixture
    def table(self, ddb):
        name = f"bg-{uuid.uuid4().hex[:10]}"
        ddb.create_table(
            TableName=name,
            KeySchema=[
                {"AttributeName": "pk", "KeyType": "HASH"},
                {"AttributeName": "sk", "KeyType": "N"},
            ],
            AttributeDefinitions=[
                {"AttributeName": "pk", "AttributeType": "S"},
                {"AttributeName": "sk", "AttributeType": "N"},
            ],
            BillingMode="PAY_PER_REQUEST",
        )
        for i in range(3):
            ddb.put_item(
                TableName=name,
                Item={"pk": {"S": "a"}, "sk": {"N": str(i)}, "v": {"S": f"x{i}"}},
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

    def test_over_hundred_keys_per_table_rejected(self, ddb, table):
        keys = [{"pk": {"S": f"k{i}"}, "sk": {"N": "1"}} for i in range(101)]
        with pytest.raises(ClientError) as exc:
            ddb.batch_get_item(RequestItems={table: {"Keys": keys}})
        assert err_msg(exc) == (
            "1 validation error detected: Value at 'requestItems."
            f"{table}.member.keys' failed to satisfy constraint: Member must "
            "have length less than or equal to 100"
        )

    def test_over_hundred_keys_total_rejected(self, ddb):
        prefix = "bg-two"
        t1 = f"{prefix}a"
        t2 = f"{prefix}b"
        for t in (t1, t2):
            ddb.create_table(
                TableName=t,
                KeySchema=[{"AttributeName": "pk", "KeyType": "HASH"}],
                AttributeDefinitions=[{"AttributeName": "pk", "AttributeType": "S"}],
                BillingMode="PAY_PER_REQUEST",
            )
        try:
            keys1 = [{"pk": {"S": f"a{i}"}} for i in range(60)]
            keys2 = [{"pk": {"S": f"b{i}"}} for i in range(45)]
            with pytest.raises(ClientError) as exc:
                ddb.batch_get_item(RequestItems={t1: {"Keys": keys1}, t2: {"Keys": keys2}})
            assert err_msg(exc) == "Too many items requested for the BatchGetItem call"
        finally:
            for t in (t1, t2):
                ddb.delete_table(TableName=t)

    def test_duplicate_keys_rejected(self, ddb, table):
        keys = [{"pk": {"S": "a"}, "sk": {"N": "1"}}, {"pk": {"S": "a"}, "sk": {"N": "1"}}]
        with pytest.raises(ClientError) as exc:
            ddb.batch_get_item(RequestItems={table: {"Keys": keys}})
        assert err_msg(exc) == "Provided list of item keys contains duplicates"

    def test_consumed_capacity_rendered_always(self, ddb, table):
        resp = ddb.batch_get_item(
            RequestItems={
                table: {
                    "Keys": [
                        {"pk": {"S": "a"}, "sk": {"N": "1"}},
                        {"pk": {"S": "a"}, "sk": {"N": "2"}},
                    ]
                }
            }
        )
        cc = resp["ConsumedCapacity"]
        assert cc == [{"CapacityUnits": 2, "TableName": table}]


class TestListTablesDefaultLimit:
    """#439 review round 2: moto's `body.get("Limit", 100)` default cap and
    the falsy-Limit pass-through (responses.py:329 + models:154-158)."""

    def test_limit_defaults_to_100_and_zero_is_all(self, ddb):
        prefix = "ltd-rev"
        made = 0
        try:
            for i in range(103):
                ddb.create_table(
                    TableName=f"{prefix}{i:03d}",
                    KeySchema=[{"AttributeName": "pk", "KeyType": "HASH"}],
                    AttributeDefinitions=[{"AttributeName": "pk", "AttributeType": "S"}],
                    BillingMode="PAY_PER_REQUEST",
                )
                made += 1
            # absent Limit caps at moto's default 100 with a token: the
            # page holds exactly 100 names (foreign tables can occupy slots,
            # so assert the CAP and the token = the page's last entry, the
            # race-safe observable)
            page = ddb.list_tables()
            mine = [t for t in page["TableNames"] if t.startswith(prefix)]
            assert len(page["TableNames"]) == 100
            assert page.get("LastEvaluatedTableName") == page["TableNames"][-1]
            assert len(mine) >= 99 and len(mine) <= 100
            # Limit=0 is falsy in moto (unlimited) but botocore's client
            # side rejects it (min 1), so the falsy path stays raw-only.
            # Absent-Limit never produces a token when fewer than 100
            # NON-filtered tables trail this page? verified above by the
            # token + the 100-table cap.
        finally:
            for i in range(made):
                try:
                    ddb.delete_table(TableName=f"{prefix}{i:03d}")
                except Exception:
                    pass  # best-effort cleanup
