"""Resource lifecycle tests for iot (auto-generated)."""

import pytest
from botocore.exceptions import ClientError


@pytest.fixture
def client():
    import boto3

    return boto3.client(
        "iot",
        endpoint_url="http://localhost:4566",
        region_name="us-east-1",
        aws_access_key_id="testing",
        aws_secret_access_key="testing",
    )


def test_command_lifecycle(client):
    """Test Command CRUD lifecycle."""
    # CREATE
    create_resp = client.create_command(
        commandId="test-id-1",
    )
    assert isinstance(create_resp.get("commandId"), str)
    assert len(create_resp.get("commandId", "")) > 0
    assert isinstance(create_resp.get("commandArn"), str)
    assert create_resp["commandArn"].startswith("arn:aws:")

    # DESCRIBE
    desc_resp = client.get_command(
        commandId="test-id-1",
    )
    assert isinstance(desc_resp.get("commandId"), str)
    assert len(desc_resp.get("commandId", "")) > 0
    assert isinstance(desc_resp.get("commandArn"), str)
    assert desc_resp["commandArn"].startswith("arn:aws:")
    assert isinstance(desc_resp.get("namespace"), str)
    assert len(desc_resp.get("namespace", "")) > 0
    assert isinstance(desc_resp.get("displayName"), str)
    assert len(desc_resp.get("displayName", "")) > 0
    assert isinstance(desc_resp.get("mandatoryParameters", []), list)
    assert isinstance(desc_resp.get("payload", {}), dict)
    assert isinstance(desc_resp.get("preprocessor", {}), dict)
    assert isinstance(desc_resp.get("roleArn"), str)
    assert desc_resp["roleArn"].startswith("arn:aws:")

    # DELETE
    client.delete_command(
        commandId="test-id-1",
    )

    # DESCRIBE after DELETE should fail
    with pytest.raises(ClientError) as exc:
        client.get_command(
            commandId="test-id-1",
        )
    assert exc.value.response["Error"]["Code"] in (
        "ResourceNotFoundException",
        "NotFoundException",
        "EntityNotFoundException",
        "InvalidRequestException",
        "NoSuchEntity",
    )


def test_command_not_found(client):
    """Test that describing a non-existent Command raises an error."""
    with pytest.raises(ClientError) as exc:
        client.get_command(
            commandId="fake-id",
        )
    assert exc.value.response["Error"]["Code"] in (
        "ResourceNotFoundException",
        "NotFoundException",
        "EntityNotFoundException",
        "InvalidRequestException",
        "NoSuchEntity",
    )
