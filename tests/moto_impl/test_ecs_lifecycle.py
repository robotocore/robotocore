"""Resource lifecycle tests for ecs (auto-generated)."""

import pytest
from botocore.exceptions import ClientError


@pytest.fixture
def client():
    import boto3

    return boto3.client(
        "ecs",
        endpoint_url="http://localhost:4566",
        region_name="us-east-1",
        aws_access_key_id="testing",
        aws_secret_access_key="testing",
    )


def test_express_gateway_service_lifecycle(client):
    """Test ExpressGatewayService CRUD lifecycle."""
    # CREATE
    create_resp = client.create_express_gateway_service(
        executionRoleArn="arn:aws:iam::123456789012:role/test-role",
        infrastructureRoleArn="arn:aws:iam::123456789012:role/test-role",
        primaryContainer={"image": "test-string"},
    )
    assert isinstance(create_resp.get("service", {}), dict)

    # DESCRIBE
    desc_resp = client.describe_express_gateway_service(
        serviceArn="arn:aws:iam::123456789012:role/test-role",
    )
    assert isinstance(desc_resp.get("service", {}), dict)

    # DELETE
    client.delete_express_gateway_service(
        serviceArn="arn:aws:iam::123456789012:role/test-role",
    )

    # DESCRIBE after DELETE should fail
    with pytest.raises(ClientError) as exc:
        client.describe_express_gateway_service(
            serviceArn="arn:aws:iam::123456789012:role/test-role",
        )
    assert exc.value.response["Error"]["Code"] in (
        "ResourceNotFoundException",
        "ResourcePolicyNotFoundException",
        "NotFoundException",
        "EntityNotFoundException",
        "InvalidRequestException",
        "NoSuchEntity",
    )


def test_express_gateway_service_not_found(client):
    """Test that describing a non-existent ExpressGatewayService raises error."""
    with pytest.raises(ClientError) as exc:
        client.describe_express_gateway_service(
            serviceArn="fake-id",
        )
    assert exc.value.response["Error"]["Code"] in (
        "ResourceNotFoundException",
        "ResourcePolicyNotFoundException",
        "NotFoundException",
        "EntityNotFoundException",
        "InvalidRequestException",
        "NoSuchEntity",
    )


def test_task_definition_lifecycle(client):
    """Test TaskDefinition CRUD lifecycle."""
    # CREATE
    create_resp = client.register_task_definition(
        family="test-string",
        containerDefinitions=[{}],
    )
    assert isinstance(create_resp.get("taskDefinition", {}), dict)
    assert isinstance(create_resp.get("tags", []), list)

    task_definition = create_resp["taskDefinition"]

    # DESCRIBE
    desc_resp = client.describe_task_definition(
        taskDefinition=task_definition,
    )
    assert isinstance(desc_resp.get("taskDefinition", {}), dict)
    assert isinstance(desc_resp.get("tags", []), list)

    # DELETE
    client.deregister_task_definition(
        taskDefinition=task_definition,
    )

    # DESCRIBE after DELETE should fail
    with pytest.raises(ClientError) as exc:
        client.describe_task_definition(
            taskDefinition=task_definition,
        )
    assert exc.value.response["Error"]["Code"] in (
        "ResourceNotFoundException",
        "ResourcePolicyNotFoundException",
        "NotFoundException",
        "EntityNotFoundException",
        "InvalidRequestException",
        "NoSuchEntity",
    )


def test_task_definition_not_found(client):
    """Test that describing a non-existent TaskDefinition raises error."""
    with pytest.raises(ClientError) as exc:
        client.describe_task_definition(
            taskDefinition="fake-id",
        )
    assert exc.value.response["Error"]["Code"] in (
        "ResourceNotFoundException",
        "ResourcePolicyNotFoundException",
        "NotFoundException",
        "EntityNotFoundException",
        "InvalidRequestException",
        "NoSuchEntity",
    )
