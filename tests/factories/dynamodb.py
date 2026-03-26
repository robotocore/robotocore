"""DynamoDB test data factories with automatic cleanup.

Provides context managers for creating DynamoDB tables that are
automatically cleaned up after the test.

Usage:
    from tests.factories.dynamodb import table, table_with_items

    def test_put_get(dynamodb):
        with table(dynamodb) as table_name:
            dynamodb.put_item(
                TableName=table_name,
                Item={"pk": {"S": "id1"}, "data": {"S": "hello"}}
            )
            response = dynamodb.get_item(TableName=table_name, Key={"pk": {"S": "id1"}})
            assert response["Item"]["data"]["S"] == "hello"

    def test_query(dynamodb):
        with table_with_items(dynamodb, items=[...]) as table_name:
            response = dynamodb.scan(TableName=table_name)
            assert len(response["Items"]) > 0
"""

from collections.abc import Generator
from contextlib import contextmanager
from typing import Any

from botocore.exceptions import ClientError, WaiterError

from . import unique_name

__all__ = ["table", "table_with_items"]

# Default key schema (single partition key named "pk")
DEFAULT_KEY_SCHEMA = [{"AttributeName": "pk", "KeyType": "HASH"}]

# Default attribute definitions
DEFAULT_ATTRIBUTE_DEFINITIONS = [{"AttributeName": "pk", "AttributeType": "S"}]


@contextmanager
def table(
    client: Any,
    name: str | None = None,
    key_schema: list[dict] | None = None,
    attribute_definitions: list[dict] | None = None,
    billing_mode: str = "PAY_PER_REQUEST",
    **kwargs: Any,
) -> Generator[str, None, None]:
    """Create a DynamoDB table with automatic cleanup.

    Args:
        client: boto3 DynamoDB client
        name: Optional table name (auto-generated if not provided)
        key_schema: Key schema (default: single partition key "pk")
        attribute_definitions: Attribute definitions (default: "pk" as String)
        billing_mode: Billing mode (default: PAY_PER_REQUEST)
        **kwargs: Additional arguments to create_table

    Yields:
        Table name

    Example:
        with table(dynamodb) as table_name:
            dynamodb.put_item(
                TableName=table_name,
                Item={"pk": {"S": "id1"}, "data": {"S": "hello"}}
            )

        # Custom key schema
        with table(
            dynamodb,
            key_schema=[
                {"AttributeName": "pk", "KeyType": "HASH"},
                {"AttributeName": "sk", "KeyType": "RANGE"}
            ],
            attribute_definitions=[
                {"AttributeName": "pk", "AttributeType": "S"},
                {"AttributeName": "sk", "AttributeType": "S"}
            ]
        ) as table_name:
            ...
    """
    table_name = name or unique_name("test-table")
    keys = key_schema or DEFAULT_KEY_SCHEMA
    attrs = attribute_definitions or DEFAULT_ATTRIBUTE_DEFINITIONS

    client.create_table(
        TableName=table_name,
        KeySchema=keys,
        AttributeDefinitions=attrs,
        BillingMode=billing_mode,
        **kwargs,
    )

    # Wait for table to be active
    try:
        waiter = client.get_waiter("table_exists")
        waiter.wait(TableName=table_name, WaiterConfig={"Delay": 1, "MaxAttempts": 30})
    except (ClientError, WaiterError):
        pass  # Waiter may not be available, continue anyway

    try:
        yield table_name
    finally:
        try:
            client.delete_table(TableName=table_name)
        except ClientError:
            pass  # Best effort cleanup


@contextmanager
def table_with_items(
    client: Any,
    items: list[dict[str, dict]] | None = None,
    count: int | None = None,
    name: str | None = None,
    **kwargs: Any,
) -> Generator[str, None, None]:
    """Create a DynamoDB table pre-populated with items.

    Args:
        client: boto3 DynamoDB client
        items: List of items (DynamoDB format with type descriptors)
        count: Number of items to create (if items not provided)
        name: Optional table name (auto-generated if not provided)
        **kwargs: Additional arguments passed to table()

    Yields:
        Table name

    Example:
        items = [
            {"pk": {"S": "id1"}, "data": {"S": "hello"}},
            {"pk": {"S": "id2"}, "data": {"S": "world"}},
        ]
        with table_with_items(dynamodb, items=items) as table_name:
            response = dynamodb.scan(TableName=table_name)
            assert len(response["Items"]) == 2

        # Auto-generate items
        with table_with_items(dynamodb, count=10) as table_name:
            response = dynamodb.scan(TableName=table_name)
            assert len(response["Items"]) == 10
    """
    if items is None:
        items = [
            {"pk": {"S": f"item-{i}"}, "data": {"S": f"test-data-{i}"}} for i in range(count or 5)
        ]

    with table(client, name=name, **kwargs) as table_name:
        for item in items:
            client.put_item(TableName=table_name, Item=item)
        yield table_name
