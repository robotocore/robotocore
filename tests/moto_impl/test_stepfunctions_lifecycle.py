"""Resource lifecycle tests for stepfunctions (auto-generated)."""

import pytest
from botocore.exceptions import ClientError


@pytest.fixture
def client():
    import boto3

    return boto3.client(
        "stepfunctions",
        endpoint_url="http://localhost:4566",
        region_name="us-east-1",
        aws_access_key_id="testing",
        aws_secret_access_key="testing",
    )


def test_activity_lifecycle(client):
    """Test Activity CRUD lifecycle."""
    # CREATE
    create_resp = client.create_activity(
        name="test-name-1",
    )
    assert isinstance(create_resp.get("activityArn"), str)
    assert create_resp["activityArn"].startswith("arn:aws:")
    assert create_resp.get("creationDate") is not None

    activity_arn = create_resp["activityArn"]

    # DESCRIBE
    desc_resp = client.describe_activity(
        activityArn=activity_arn,
    )
    assert isinstance(desc_resp.get("activityArn"), str)
    assert desc_resp["activityArn"].startswith("arn:aws:")
    assert isinstance(desc_resp.get("encryptionConfiguration", {}), dict)

    # DELETE
    client.delete_activity(
        activityArn=activity_arn,
    )

    # DESCRIBE after DELETE should fail
    with pytest.raises(ClientError) as exc:
        client.describe_activity(
            activityArn=activity_arn,
        )
    assert exc.value.response["Error"]["Code"] in (
        "ResourceNotFoundException",
        "ResourcePolicyNotFoundException",
        "NotFoundException",
        "EntityNotFoundException",
        "InvalidRequestException",
        "NoSuchEntity",
    )


def test_activity_not_found(client):
    """Test that describing a non-existent Activity raises error."""
    with pytest.raises(ClientError) as exc:
        client.describe_activity(
            activityArn="fake-id",
        )
    assert exc.value.response["Error"]["Code"] in (
        "ResourceNotFoundException",
        "ResourcePolicyNotFoundException",
        "NotFoundException",
        "EntityNotFoundException",
        "InvalidRequestException",
        "NoSuchEntity",
    )


def test_execution_lifecycle(client):
    """Test Execution CRUD lifecycle."""
    # CREATE
    create_resp = client.start_execution(
        stateMachineArn="arn:aws:iam::123456789012:role/test-role",
    )
    assert isinstance(create_resp.get("executionArn"), str)
    assert create_resp["executionArn"].startswith("arn:aws:")
    assert create_resp.get("startDate") is not None

    execution_arn = create_resp["executionArn"]

    # DESCRIBE
    desc_resp = client.describe_execution(
        executionArn=execution_arn,
    )
    assert isinstance(desc_resp.get("executionArn"), str)
    assert desc_resp["executionArn"].startswith("arn:aws:")
    assert isinstance(desc_resp.get("inputDetails", {}), dict)
    assert isinstance(desc_resp.get("outputDetails", {}), dict)

    # DELETE
    client.stop_execution(
        executionArn=execution_arn,
    )

    # DESCRIBE after DELETE should fail
    with pytest.raises(ClientError) as exc:
        client.describe_execution(
            executionArn=execution_arn,
        )
    assert exc.value.response["Error"]["Code"] in (
        "ResourceNotFoundException",
        "ResourcePolicyNotFoundException",
        "NotFoundException",
        "EntityNotFoundException",
        "InvalidRequestException",
        "NoSuchEntity",
    )


def test_execution_not_found(client):
    """Test that describing a non-existent Execution raises error."""
    with pytest.raises(ClientError) as exc:
        client.describe_execution(
            executionArn="fake-id",
        )
    assert exc.value.response["Error"]["Code"] in (
        "ResourceNotFoundException",
        "ResourcePolicyNotFoundException",
        "NotFoundException",
        "EntityNotFoundException",
        "InvalidRequestException",
        "NoSuchEntity",
    )


def test_state_machine_lifecycle(client):
    """Test StateMachine CRUD lifecycle."""
    # CREATE
    create_resp = client.create_state_machine(
        name="test-name-1",
        definition="test-string",
        roleArn="arn:aws:iam::123456789012:role/test-role",
    )
    assert isinstance(create_resp.get("stateMachineArn"), str)
    assert create_resp["stateMachineArn"].startswith("arn:aws:")
    assert create_resp.get("creationDate") is not None

    state_machine_arn = create_resp["stateMachineArn"]

    # DESCRIBE
    desc_resp = client.describe_state_machine(
        stateMachineArn=state_machine_arn,
    )
    assert isinstance(desc_resp.get("stateMachineArn"), str)
    assert desc_resp["stateMachineArn"].startswith("arn:aws:")
    assert isinstance(desc_resp.get("loggingConfiguration", {}), dict)
    assert isinstance(desc_resp.get("tracingConfiguration", {}), dict)
    assert isinstance(desc_resp.get("encryptionConfiguration", {}), dict)
    assert isinstance(desc_resp.get("variableReferences", {}), dict)

    # DELETE
    client.delete_state_machine(
        stateMachineArn=state_machine_arn,
    )

    # DESCRIBE after DELETE should fail
    with pytest.raises(ClientError) as exc:
        client.describe_state_machine(
            stateMachineArn=state_machine_arn,
        )
    assert exc.value.response["Error"]["Code"] in (
        "ResourceNotFoundException",
        "ResourcePolicyNotFoundException",
        "NotFoundException",
        "EntityNotFoundException",
        "InvalidRequestException",
        "NoSuchEntity",
    )


def test_state_machine_not_found(client):
    """Test that describing a non-existent StateMachine raises error."""
    with pytest.raises(ClientError) as exc:
        client.describe_state_machine(
            stateMachineArn="fake-id",
        )
    assert exc.value.response["Error"]["Code"] in (
        "ResourceNotFoundException",
        "ResourcePolicyNotFoundException",
        "NotFoundException",
        "EntityNotFoundException",
        "InvalidRequestException",
        "NoSuchEntity",
    )


def test_state_machine_alias_lifecycle(client):
    """Test StateMachineAlias CRUD lifecycle."""
    # CREATE
    create_resp = client.create_state_machine_alias(
        name="test-name-1",
        routingConfiguration=[
            {"stateMachineVersionArn": "arn:aws:iam::123456789012:role/test-role", "weight": 1}
        ],
    )
    assert isinstance(create_resp.get("stateMachineAliasArn"), str)
    assert create_resp["stateMachineAliasArn"].startswith("arn:aws:")
    assert create_resp.get("creationDate") is not None

    state_machine_alias_arn = create_resp["stateMachineAliasArn"]

    # DESCRIBE
    desc_resp = client.describe_state_machine_alias(
        stateMachineAliasArn=state_machine_alias_arn,
    )
    assert isinstance(desc_resp.get("stateMachineAliasArn"), str)
    assert desc_resp["stateMachineAliasArn"].startswith("arn:aws:")
    assert isinstance(desc_resp.get("routingConfiguration", []), list)

    # DELETE
    client.delete_state_machine_alias(
        stateMachineAliasArn=state_machine_alias_arn,
    )

    # DESCRIBE after DELETE should fail
    with pytest.raises(ClientError) as exc:
        client.describe_state_machine_alias(
            stateMachineAliasArn=state_machine_alias_arn,
        )
    assert exc.value.response["Error"]["Code"] in (
        "ResourceNotFoundException",
        "ResourcePolicyNotFoundException",
        "NotFoundException",
        "EntityNotFoundException",
        "InvalidRequestException",
        "NoSuchEntity",
    )


def test_state_machine_alias_not_found(client):
    """Test that describing a non-existent StateMachineAlias raises error."""
    with pytest.raises(ClientError) as exc:
        client.describe_state_machine_alias(
            stateMachineAliasArn="fake-id",
        )
    assert exc.value.response["Error"]["Code"] in (
        "ResourceNotFoundException",
        "ResourcePolicyNotFoundException",
        "NotFoundException",
        "EntityNotFoundException",
        "InvalidRequestException",
        "NoSuchEntity",
    )
