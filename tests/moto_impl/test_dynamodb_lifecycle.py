"""Resource lifecycle tests for dynamodb (auto-generated)."""

import pytest
from botocore.exceptions import ClientError


@pytest.fixture
def client():
    import boto3

    return boto3.client(
        "dynamodb",
        endpoint_url="http://localhost:4566",
        region_name="us-east-1",
        aws_access_key_id="testing",
        aws_secret_access_key="testing",
    )


def test_backup_lifecycle(client):
    """Test Backup CRUD lifecycle."""
    # CREATE
    create_resp = client.create_backup(
        TableName="test-name-1",
        BackupName="test-name-1",
    )
    assert isinstance(create_resp.get("BackupDetails", {}), dict)

    # DESCRIBE
    desc_resp = client.describe_backup(
        BackupArn="arn:aws:iam::123456789012:role/test-role",
    )
    assert isinstance(desc_resp.get("BackupDescription", {}), dict)

    # DELETE
    client.delete_backup(
        BackupArn="arn:aws:iam::123456789012:role/test-role",
    )

    # DESCRIBE after DELETE should fail
    with pytest.raises(ClientError) as exc:
        client.describe_backup(
            BackupArn="arn:aws:iam::123456789012:role/test-role",
        )
    assert exc.value.response["Error"]["Code"] in (
        "ResourceNotFoundException",
        "ResourcePolicyNotFoundException",
        "NotFoundException",
        "EntityNotFoundException",
        "InvalidRequestException",
        "NoSuchEntity",
    )


def test_backup_not_found(client):
    """Test that describing a non-existent Backup raises error."""
    with pytest.raises(ClientError) as exc:
        client.describe_backup(
            BackupArn="fake-id",
        )
    assert exc.value.response["Error"]["Code"] in (
        "ResourceNotFoundException",
        "ResourcePolicyNotFoundException",
        "NotFoundException",
        "EntityNotFoundException",
        "InvalidRequestException",
        "NoSuchEntity",
    )


def test_global_table_lifecycle(client):
    """Test GlobalTable CRUD lifecycle."""
    # CREATE
    create_resp = client.create_global_table(
        GlobalTableName="test-name-1",
        ReplicationGroup=[{}],
    )
    assert isinstance(create_resp.get("GlobalTableDescription", {}), dict)

    # DESCRIBE
    desc_resp = client.describe_global_table(
        GlobalTableName="test-name-1",
    )
    assert isinstance(desc_resp.get("GlobalTableDescription", {}), dict)


def test_global_table_not_found(client):
    """Test that describing a non-existent GlobalTable raises error."""
    with pytest.raises(ClientError) as exc:
        client.describe_global_table(
            GlobalTableName="fake-id",
        )
    assert exc.value.response["Error"]["Code"] in (
        "ResourceNotFoundException",
        "ResourcePolicyNotFoundException",
        "NotFoundException",
        "EntityNotFoundException",
        "InvalidRequestException",
        "NoSuchEntity",
    )


def test_item_lifecycle(client):
    """Test Item CRUD lifecycle."""
    # CREATE
    create_resp = client.put_item(
        TableName="test-name-1",
        Item={},
    )
    assert isinstance(create_resp.get("Attributes", {}), dict)
    assert isinstance(create_resp.get("ConsumedCapacity", {}), dict)
    assert isinstance(create_resp.get("ItemCollectionMetrics", {}), dict)

    # DESCRIBE
    desc_resp = client.get_item(
        TableName="test-name-1",
        Key={},
    )
    assert isinstance(desc_resp.get("Item", {}), dict)
    assert isinstance(desc_resp.get("ConsumedCapacity", {}), dict)

    # DELETE
    client.delete_item(
        TableName="test-name-1",
        Key={},
    )

    # DESCRIBE after DELETE should fail
    with pytest.raises(ClientError) as exc:
        client.get_item(
            TableName="test-name-1",
            Key={},
        )
    assert exc.value.response["Error"]["Code"] in (
        "ResourceNotFoundException",
        "ResourcePolicyNotFoundException",
        "NotFoundException",
        "EntityNotFoundException",
        "InvalidRequestException",
        "NoSuchEntity",
    )


def test_item_not_found(client):
    """Test that describing a non-existent Item raises error."""
    with pytest.raises(ClientError) as exc:
        client.get_item(
            TableName="fake-id",
            Key="fake-id",
        )
    assert exc.value.response["Error"]["Code"] in (
        "ResourceNotFoundException",
        "ResourcePolicyNotFoundException",
        "NotFoundException",
        "EntityNotFoundException",
        "InvalidRequestException",
        "NoSuchEntity",
    )


def test_resource_policy_lifecycle(client):
    """Test ResourcePolicy CRUD lifecycle."""
    # CREATE
    client.put_resource_policy(
        ResourceArn="arn:aws:iam::123456789012:role/test-role",
        Policy="test-string",
    )

    # DESCRIBE
    client.get_resource_policy(
        ResourceArn="arn:aws:iam::123456789012:role/test-role",
    )

    # DELETE
    client.delete_resource_policy(
        ResourceArn="arn:aws:iam::123456789012:role/test-role",
    )

    # DESCRIBE after DELETE should fail
    with pytest.raises(ClientError) as exc:
        client.get_resource_policy(
            ResourceArn="arn:aws:iam::123456789012:role/test-role",
        )
    assert exc.value.response["Error"]["Code"] in (
        "ResourceNotFoundException",
        "ResourcePolicyNotFoundException",
        "NotFoundException",
        "EntityNotFoundException",
        "InvalidRequestException",
        "NoSuchEntity",
    )


def test_resource_policy_not_found(client):
    """Test that describing a non-existent ResourcePolicy raises error."""
    with pytest.raises(ClientError) as exc:
        client.get_resource_policy(
            ResourceArn="fake-id",
        )
    assert exc.value.response["Error"]["Code"] in (
        "ResourceNotFoundException",
        "ResourcePolicyNotFoundException",
        "NotFoundException",
        "EntityNotFoundException",
        "InvalidRequestException",
        "NoSuchEntity",
    )


def test_table_lifecycle(client):
    """Test Table CRUD lifecycle."""
    # CREATE
    create_resp = client.create_table(
        TableName="test-name-1",
    )
    assert isinstance(create_resp.get("TableDescription", {}), dict)

    # DESCRIBE
    desc_resp = client.describe_table(
        TableName="test-name-1",
    )
    assert isinstance(desc_resp.get("Table", {}), dict)

    # DELETE
    client.delete_table(
        TableName="test-name-1",
    )

    # DESCRIBE after DELETE should fail
    with pytest.raises(ClientError) as exc:
        client.describe_table(
            TableName="test-name-1",
        )
    assert exc.value.response["Error"]["Code"] in (
        "ResourceNotFoundException",
        "ResourcePolicyNotFoundException",
        "NotFoundException",
        "EntityNotFoundException",
        "InvalidRequestException",
        "NoSuchEntity",
    )


def test_table_not_found(client):
    """Test that describing a non-existent Table raises error."""
    with pytest.raises(ClientError) as exc:
        client.describe_table(
            TableName="fake-id",
        )
    assert exc.value.response["Error"]["Code"] in (
        "ResourceNotFoundException",
        "ResourcePolicyNotFoundException",
        "NotFoundException",
        "EntityNotFoundException",
        "InvalidRequestException",
        "NoSuchEntity",
    )
