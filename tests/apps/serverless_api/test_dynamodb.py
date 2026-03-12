"""
DynamoDB table and item operations for the serverless API.

Tests CRUD, queries, scans, batch operations, and GSI queries
through the ServerlessApp abstraction.
"""

import uuid

import pytest

from .models import TableSchema


class TestDynamoDBOperations:
    """DynamoDB storage layer tests."""

    @pytest.fixture
    def users_table(self, serverless_app, unique_name):
        """Create a users table via the app."""
        schema = TableSchema(
            table_name=f"users-{unique_name}",
            key_schema=[{"AttributeName": "user_id", "KeyType": "HASH"}],
            attributes=[{"AttributeName": "user_id", "AttributeType": "S"}],
        )
        table_name = serverless_app.create_table(schema)
        yield serverless_app, table_name

    @pytest.fixture
    def users_table_with_gsi(self, serverless_app, unique_name):
        """Create a users table with email GSI."""
        schema = TableSchema(
            table_name=f"users-gsi-{unique_name}",
            key_schema=[{"AttributeName": "user_id", "KeyType": "HASH"}],
            attributes=[
                {"AttributeName": "user_id", "AttributeType": "S"},
                {"AttributeName": "email", "AttributeType": "S"},
            ],
            gsis=[
                {
                    "IndexName": "email-index",
                    "KeySchema": [
                        {"AttributeName": "email", "KeyType": "HASH"},
                    ],
                    "Projection": {"ProjectionType": "ALL"},
                }
            ],
        )
        table_name = serverless_app.create_table(schema)
        yield serverless_app, table_name

    def test_put_and_get_item(self, users_table):
        """Put an item and retrieve it by key."""
        app, table = users_table
        user_id = str(uuid.uuid4())

        app.put_item(
            table,
            {
                "user_id": {"S": user_id},
                "email": {"S": "alice@example.com"},
                "name": {"S": "Alice Johnson"},
            },
        )

        item = app.get_item(table, {"user_id": {"S": user_id}})
        assert item is not None
        assert item["email"]["S"] == "alice@example.com"
        assert item["name"]["S"] == "Alice Johnson"

    def test_get_nonexistent_item(self, users_table):
        """Getting a nonexistent item returns None."""
        app, table = users_table
        item = app.get_item(table, {"user_id": {"S": "does-not-exist"}})
        assert item is None

    def test_query_with_key_condition(self, users_table):
        """Query by partition key returns matching items."""
        app, table = users_table
        user_id = str(uuid.uuid4())

        app.put_item(
            table,
            {
                "user_id": {"S": user_id},
                "email": {"S": "bob@example.com"},
            },
        )

        items = app.query_table(
            table,
            "user_id = :uid",
            {":uid": {"S": user_id}},
        )
        assert len(items) == 1
        assert items[0]["email"]["S"] == "bob@example.com"

    def test_scan_with_filter(self, users_table):
        """Scan with a filter expression narrows results."""
        app, table = users_table
        uid1 = str(uuid.uuid4())
        uid2 = str(uuid.uuid4())

        app.put_item(
            table,
            {
                "user_id": {"S": uid1},
                "plan": {"S": "free"},
            },
        )
        app.put_item(
            table,
            {
                "user_id": {"S": uid2},
                "plan": {"S": "pro"},
            },
        )

        items = app.scan_table(
            table,
            filter_expression="#p = :p",
            expression_values={":p": {"S": "pro"}},
            expression_names={"#p": "plan"},
        )
        assert len(items) == 1
        assert items[0]["user_id"]["S"] == uid2

    def test_scan_no_filter(self, users_table):
        """Scan without filter returns all items."""
        app, table = users_table
        uid1 = str(uuid.uuid4())
        uid2 = str(uuid.uuid4())

        app.put_item(table, {"user_id": {"S": uid1}, "name": {"S": "A"}})
        app.put_item(table, {"user_id": {"S": uid2}, "name": {"S": "B"}})

        items = app.scan_table(table)
        assert len(items) >= 2

    def test_update_item_with_expressions(self, users_table):
        """Update an item's attributes using expression syntax."""
        app, table = users_table
        user_id = str(uuid.uuid4())

        app.put_item(
            table,
            {
                "user_id": {"S": user_id},
                "plan": {"S": "free"},
                "name": {"S": "Charlie"},
            },
        )

        app.update_item(
            table,
            key={"user_id": {"S": user_id}},
            update_expression="SET #p = :plan, #n = :name",
            expression_names={"#p": "plan", "#n": "name"},
            expression_values={":plan": {"S": "enterprise"}, ":name": {"S": "Charles"}},
        )

        item = app.get_item(table, {"user_id": {"S": user_id}})
        assert item["plan"]["S"] == "enterprise"
        assert item["name"]["S"] == "Charles"

    def test_delete_item(self, users_table):
        """Delete an item and verify it is gone."""
        app, table = users_table
        user_id = str(uuid.uuid4())

        app.put_item(table, {"user_id": {"S": user_id}, "name": {"S": "Gone"}})
        item = app.get_item(table, {"user_id": {"S": user_id}})
        assert item is not None

        app.delete_item(table, {"user_id": {"S": user_id}})
        item = app.get_item(table, {"user_id": {"S": user_id}})
        assert item is None

    def test_gsi_query(self, users_table_with_gsi):
        """Query a Global Secondary Index."""
        app, table = users_table_with_gsi
        user_id = str(uuid.uuid4())

        app.put_item(
            table,
            {
                "user_id": {"S": user_id},
                "email": {"S": "gsi-test@example.com"},
                "name": {"S": "GSI User"},
            },
        )

        items = app.query_table(
            table,
            "email = :email",
            {":email": {"S": "gsi-test@example.com"}},
            index_name="email-index",
        )
        assert len(items) == 1
        assert items[0]["user_id"]["S"] == user_id
        assert items[0]["name"]["S"] == "GSI User"

    def test_batch_write_and_get(self, users_table):
        """Batch write multiple items then batch get them."""
        app, table = users_table
        uids = [str(uuid.uuid4()) for _ in range(5)]

        items = [{"user_id": {"S": uid}, "name": {"S": f"User-{i}"}} for i, uid in enumerate(uids)]
        app.batch_write(table, items)

        keys = [{"user_id": {"S": uid}} for uid in uids]
        results = app.batch_get(table, keys)
        assert len(results) == 5

        result_ids = {r["user_id"]["S"] for r in results}
        assert result_ids == set(uids)
