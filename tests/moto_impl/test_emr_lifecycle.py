"""Resource lifecycle tests for emr (auto-generated)."""

import pytest
from botocore.exceptions import ClientError


@pytest.fixture
def client():
    import boto3

    return boto3.client(
        "emr",
        endpoint_url="http://localhost:4566",
        region_name="us-east-1",
        aws_access_key_id="testing",
        aws_secret_access_key="testing",
    )


def test_auto_termination_policy_lifecycle(client):
    """Test AutoTerminationPolicy CRUD lifecycle."""
    # CREATE
    client.put_auto_termination_policy(
        ClusterId="test-id-1",
    )

    # DESCRIBE
    desc_resp = client.get_auto_termination_policy(
        ClusterId="test-id-1",
    )
    assert isinstance(desc_resp.get("AutoTerminationPolicy", {}), dict)

    # DELETE
    client.remove_auto_termination_policy(
        ClusterId="test-id-1",
    )

    # DESCRIBE after DELETE should fail
    with pytest.raises(ClientError) as exc:
        client.get_auto_termination_policy(
            ClusterId="test-id-1",
        )
    assert exc.value.response["Error"]["Code"] in (
        "ResourceNotFoundException",
        "ResourcePolicyNotFoundException",
        "NotFoundException",
        "EntityNotFoundException",
        "InvalidRequestException",
        "NoSuchEntity",
    )


def test_auto_termination_policy_not_found(client):
    """Test that describing a non-existent AutoTerminationPolicy raises error."""
    with pytest.raises(ClientError) as exc:
        client.get_auto_termination_policy(
            ClusterId="fake-id",
        )
    assert exc.value.response["Error"]["Code"] in (
        "ResourceNotFoundException",
        "ResourcePolicyNotFoundException",
        "NotFoundException",
        "EntityNotFoundException",
        "InvalidRequestException",
        "NoSuchEntity",
    )


def test_block_public_access_configuration_lifecycle(client):
    """Test BlockPublicAccessConfiguration CRUD lifecycle."""
    # CREATE
    client.put_block_public_access_configuration(
        BlockPublicAccessConfiguration={"BlockPublicSecurityGroupRules": True},
    )

    # DESCRIBE
    desc_resp = client.get_block_public_access_configuration()
    assert isinstance(desc_resp.get("BlockPublicAccessConfiguration", {}), dict)
    assert isinstance(desc_resp.get("BlockPublicAccessConfigurationMetadata", {}), dict)


def test_block_public_access_configuration_not_found(client):
    """Test that describing a non-existent BlockPublicAccessConfiguration raises error."""
    with pytest.raises(ClientError) as exc:
        client.get_block_public_access_configuration()
    assert exc.value.response["Error"]["Code"] in (
        "ResourceNotFoundException",
        "ResourcePolicyNotFoundException",
        "NotFoundException",
        "EntityNotFoundException",
        "InvalidRequestException",
        "NoSuchEntity",
    )


def test_managed_scaling_policy_lifecycle(client):
    """Test ManagedScalingPolicy CRUD lifecycle."""
    # CREATE
    client.put_managed_scaling_policy(
        ClusterId="test-id-1",
        ManagedScalingPolicy={},
    )

    # DESCRIBE
    desc_resp = client.get_managed_scaling_policy(
        ClusterId="test-id-1",
    )
    assert isinstance(desc_resp.get("ManagedScalingPolicy", {}), dict)

    # DELETE
    client.remove_managed_scaling_policy(
        ClusterId="test-id-1",
    )

    # DESCRIBE after DELETE should fail
    with pytest.raises(ClientError) as exc:
        client.get_managed_scaling_policy(
            ClusterId="test-id-1",
        )
    assert exc.value.response["Error"]["Code"] in (
        "ResourceNotFoundException",
        "ResourcePolicyNotFoundException",
        "NotFoundException",
        "EntityNotFoundException",
        "InvalidRequestException",
        "NoSuchEntity",
    )


def test_managed_scaling_policy_not_found(client):
    """Test that describing a non-existent ManagedScalingPolicy raises error."""
    with pytest.raises(ClientError) as exc:
        client.get_managed_scaling_policy(
            ClusterId="fake-id",
        )
    assert exc.value.response["Error"]["Code"] in (
        "ResourceNotFoundException",
        "ResourcePolicyNotFoundException",
        "NotFoundException",
        "EntityNotFoundException",
        "InvalidRequestException",
        "NoSuchEntity",
    )


def test_notebook_execution_lifecycle(client):
    """Test NotebookExecution CRUD lifecycle."""
    # CREATE
    create_resp = client.start_notebook_execution(
        ExecutionEngine={"Id": "test-id-1"},
        ServiceRole="test-string",
    )
    assert isinstance(create_resp.get("NotebookExecutionId"), str)
    assert len(create_resp.get("NotebookExecutionId", "")) > 0

    notebook_execution_id = create_resp["NotebookExecutionId"]

    # DESCRIBE
    desc_resp = client.describe_notebook_execution(
        NotebookExecutionId=notebook_execution_id,
    )
    assert isinstance(desc_resp.get("NotebookExecution", {}), dict)

    # DELETE
    client.stop_notebook_execution(
        NotebookExecutionId=notebook_execution_id,
    )

    # DESCRIBE after DELETE should fail
    with pytest.raises(ClientError) as exc:
        client.describe_notebook_execution(
            NotebookExecutionId=notebook_execution_id,
        )
    assert exc.value.response["Error"]["Code"] in (
        "ResourceNotFoundException",
        "ResourcePolicyNotFoundException",
        "NotFoundException",
        "EntityNotFoundException",
        "InvalidRequestException",
        "NoSuchEntity",
    )


def test_notebook_execution_not_found(client):
    """Test that describing a non-existent NotebookExecution raises error."""
    with pytest.raises(ClientError) as exc:
        client.describe_notebook_execution(
            NotebookExecutionId="fake-id",
        )
    assert exc.value.response["Error"]["Code"] in (
        "ResourceNotFoundException",
        "ResourcePolicyNotFoundException",
        "NotFoundException",
        "EntityNotFoundException",
        "InvalidRequestException",
        "NoSuchEntity",
    )


def test_persistent_app_ui_lifecycle(client):
    """Test PersistentAppUI CRUD lifecycle."""
    # CREATE
    create_resp = client.create_persistent_app_ui(
        TargetResourceArn="arn:aws:iam::123456789012:role/test-role",
    )
    assert isinstance(create_resp.get("PersistentAppUIId"), str)
    assert len(create_resp.get("PersistentAppUIId", "")) > 0

    persistent_app_ui_id = create_resp["PersistentAppUIId"]

    # DESCRIBE
    desc_resp = client.describe_persistent_app_ui(
        PersistentAppUIId=persistent_app_ui_id,
    )
    assert isinstance(desc_resp.get("PersistentAppUI", {}), dict)


def test_persistent_app_ui_not_found(client):
    """Test that describing a non-existent PersistentAppUI raises error."""
    with pytest.raises(ClientError) as exc:
        client.describe_persistent_app_ui(
            PersistentAppUIId="fake-id",
        )
    assert exc.value.response["Error"]["Code"] in (
        "ResourceNotFoundException",
        "ResourcePolicyNotFoundException",
        "NotFoundException",
        "EntityNotFoundException",
        "InvalidRequestException",
        "NoSuchEntity",
    )


def test_security_configuration_lifecycle(client):
    """Test SecurityConfiguration CRUD lifecycle."""
    # CREATE
    create_resp = client.create_security_configuration(
        Name="test-name-1",
        SecurityConfiguration="test-string",
    )
    assert isinstance(create_resp.get("Name"), str)
    assert len(create_resp.get("Name", "")) > 0
    assert create_resp.get("CreationDateTime") is not None

    # DESCRIBE
    desc_resp = client.describe_security_configuration(
        Name="test-name-1",
    )
    assert isinstance(desc_resp.get("Name"), str)
    assert len(desc_resp.get("Name", "")) > 0

    # DELETE
    client.delete_security_configuration(
        Name="test-name-1",
    )

    # DESCRIBE after DELETE should fail
    with pytest.raises(ClientError) as exc:
        client.describe_security_configuration(
            Name="test-name-1",
        )
    assert exc.value.response["Error"]["Code"] in (
        "ResourceNotFoundException",
        "ResourcePolicyNotFoundException",
        "NotFoundException",
        "EntityNotFoundException",
        "InvalidRequestException",
        "NoSuchEntity",
    )


def test_security_configuration_not_found(client):
    """Test that describing a non-existent SecurityConfiguration raises error."""
    with pytest.raises(ClientError) as exc:
        client.describe_security_configuration(
            Name="fake-id",
        )
    assert exc.value.response["Error"]["Code"] in (
        "ResourceNotFoundException",
        "ResourcePolicyNotFoundException",
        "NotFoundException",
        "EntityNotFoundException",
        "InvalidRequestException",
        "NoSuchEntity",
    )


def test_studio_lifecycle(client):
    """Test Studio CRUD lifecycle."""
    # CREATE
    create_resp = client.create_studio(
        Name="test-name-1",
        AuthMode="SSO",
        VpcId="test-id-1",
        SubnetIds=["test-string"],
        ServiceRole="test-string",
        WorkspaceSecurityGroupId="test-id-1",
        EngineSecurityGroupId="test-id-1",
        DefaultS3Location="test-string",
    )
    assert isinstance(create_resp.get("StudioId"), str)
    assert len(create_resp.get("StudioId", "")) > 0

    studio_id = create_resp["StudioId"]

    # DESCRIBE
    desc_resp = client.describe_studio(
        StudioId=studio_id,
    )
    assert isinstance(desc_resp.get("Studio", {}), dict)

    # DELETE
    client.delete_studio(
        StudioId=studio_id,
    )

    # DESCRIBE after DELETE should fail
    with pytest.raises(ClientError) as exc:
        client.describe_studio(
            StudioId=studio_id,
        )
    assert exc.value.response["Error"]["Code"] in (
        "ResourceNotFoundException",
        "ResourcePolicyNotFoundException",
        "NotFoundException",
        "EntityNotFoundException",
        "InvalidRequestException",
        "NoSuchEntity",
    )


def test_studio_not_found(client):
    """Test that describing a non-existent Studio raises error."""
    with pytest.raises(ClientError) as exc:
        client.describe_studio(
            StudioId="fake-id",
        )
    assert exc.value.response["Error"]["Code"] in (
        "ResourceNotFoundException",
        "ResourcePolicyNotFoundException",
        "NotFoundException",
        "EntityNotFoundException",
        "InvalidRequestException",
        "NoSuchEntity",
    )


def test_studio_session_mapping_lifecycle(client):
    """Test StudioSessionMapping CRUD lifecycle."""
    # CREATE
    client.create_studio_session_mapping(
        StudioId="test-id-1",
        IdentityType="USER",
        SessionPolicyArn="arn:aws:iam::123456789012:role/test-role",
    )

    # DESCRIBE
    desc_resp = client.get_studio_session_mapping(
        StudioId="test-id-1",
        IdentityType="USER",
    )
    assert isinstance(desc_resp.get("SessionMapping", {}), dict)

    # DELETE
    client.delete_studio_session_mapping(
        StudioId="test-id-1",
        IdentityType="USER",
    )

    # DESCRIBE after DELETE should fail
    with pytest.raises(ClientError) as exc:
        client.get_studio_session_mapping(
            StudioId="test-id-1",
            IdentityType="USER",
        )
    assert exc.value.response["Error"]["Code"] in (
        "ResourceNotFoundException",
        "ResourcePolicyNotFoundException",
        "NotFoundException",
        "EntityNotFoundException",
        "InvalidRequestException",
        "NoSuchEntity",
    )


def test_studio_session_mapping_not_found(client):
    """Test that describing a non-existent StudioSessionMapping raises error."""
    with pytest.raises(ClientError) as exc:
        client.get_studio_session_mapping(
            StudioId="fake-id",
            IdentityType="fake-id",
        )
    assert exc.value.response["Error"]["Code"] in (
        "ResourceNotFoundException",
        "ResourcePolicyNotFoundException",
        "NotFoundException",
        "EntityNotFoundException",
        "InvalidRequestException",
        "NoSuchEntity",
    )
