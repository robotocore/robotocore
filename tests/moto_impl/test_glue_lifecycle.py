"""Resource lifecycle tests for glue (auto-generated)."""

import pytest
from botocore.exceptions import ClientError


@pytest.fixture
def client():
    import boto3

    return boto3.client(
        "glue",
        endpoint_url="http://localhost:4566",
        region_name="us-east-1",
        aws_access_key_id="testing",
        aws_secret_access_key="testing",
    )


def test_connection_type_lifecycle(client):
    """Test ConnectionType CRUD lifecycle."""
    # CREATE
    create_resp = client.register_connection_type(
        ConnectionType="test-string",
        IntegrationType="REST",
        ConnectionProperties={},
        ConnectorAuthenticationConfiguration={},
        RestConfiguration={},
    )
    assert isinstance(create_resp.get("ConnectionTypeArn"), str)
    assert create_resp["ConnectionTypeArn"].startswith("arn:aws:")

    # DESCRIBE
    desc_resp = client.describe_connection_type(
        ConnectionType="test-string",
    )
    assert isinstance(desc_resp.get("ConnectionType"), str)
    assert isinstance(desc_resp.get("Description"), str)
    assert isinstance(desc_resp.get("Capabilities", {}), dict)
    assert isinstance(desc_resp.get("ConnectionProperties", {}), dict)
    assert isinstance(desc_resp.get("ConnectionOptions", {}), dict)
    assert isinstance(desc_resp.get("AuthenticationConfiguration", {}), dict)
    assert isinstance(desc_resp.get("ComputeEnvironmentConfigurations", {}), dict)
    assert isinstance(desc_resp.get("PhysicalConnectionRequirements", {}), dict)
    assert isinstance(desc_resp.get("AthenaConnectionProperties", {}), dict)
    assert isinstance(desc_resp.get("PythonConnectionProperties", {}), dict)
    assert isinstance(desc_resp.get("SparkConnectionProperties", {}), dict)
    assert isinstance(desc_resp.get("RestConfiguration", {}), dict)

    # DELETE
    client.delete_connection_type(
        ConnectionType="test-string",
    )

    # DESCRIBE after DELETE should fail
    with pytest.raises(ClientError) as exc:
        client.describe_connection_type(
            ConnectionType="test-string",
        )
    assert exc.value.response["Error"]["Code"] in (
        "ResourceNotFoundException",
        "NotFoundException",
        "EntityNotFoundException",
        "InvalidRequestException",
        "NoSuchEntity",
    )


def test_connection_type_not_found(client):
    """Test that describing a non-existent ConnectionType raises an error."""
    with pytest.raises(ClientError) as exc:
        client.describe_connection_type(
            ConnectionType="fake-id",
        )
    assert exc.value.response["Error"]["Code"] in (
        "ResourceNotFoundException",
        "NotFoundException",
        "EntityNotFoundException",
        "InvalidRequestException",
        "NoSuchEntity",
    )


def test_glue_identity_center_configuration_lifecycle(client):
    """Test GlueIdentityCenterConfiguration CRUD lifecycle."""
    # CREATE
    create_resp = client.create_glue_identity_center_configuration(
        InstanceArn="arn:aws:iam::123456789012:role/test-role",
    )
    assert isinstance(create_resp.get("ApplicationArn"), str)
    assert create_resp["ApplicationArn"].startswith("arn:aws:")

    # DESCRIBE
    desc_resp = client.get_glue_identity_center_configuration()
    assert isinstance(desc_resp.get("ApplicationArn"), str)
    assert desc_resp["ApplicationArn"].startswith("arn:aws:")
    assert isinstance(desc_resp.get("InstanceArn"), str)
    assert desc_resp["InstanceArn"].startswith("arn:aws:")
    assert isinstance(desc_resp.get("Scopes", []), list)
    assert isinstance(desc_resp.get("UserBackgroundSessionsEnabled"), bool)

    # DELETE
    client.delete_glue_identity_center_configuration()

    # DESCRIBE after DELETE should fail
    with pytest.raises(ClientError) as exc:
        client.get_glue_identity_center_configuration()
    assert exc.value.response["Error"]["Code"] in (
        "ResourceNotFoundException",
        "NotFoundException",
        "EntityNotFoundException",
        "InvalidRequestException",
        "NoSuchEntity",
    )


def test_glue_identity_center_configuration_not_found(client):
    """Test that describing a non-existent GlueIdentityCenterConfiguration raises an error."""
    with pytest.raises(ClientError) as exc:
        client.get_glue_identity_center_configuration()
    assert exc.value.response["Error"]["Code"] in (
        "ResourceNotFoundException",
        "NotFoundException",
        "EntityNotFoundException",
        "InvalidRequestException",
        "NoSuchEntity",
    )


def test_materialized_view_refresh_task_run_lifecycle(client):
    """Test MaterializedViewRefreshTaskRun CRUD lifecycle."""
    # CREATE
    create_resp = client.start_materialized_view_refresh_task_run(
        CatalogId="test-id-1",
        DatabaseName="test-name-1",
        TableName="test-name-1",
    )
    assert isinstance(create_resp.get("MaterializedViewRefreshTaskRunId"), str)
    assert len(create_resp.get("MaterializedViewRefreshTaskRunId", "")) > 0

    # DESCRIBE
    desc_resp = client.get_materialized_view_refresh_task_run(
        CatalogId="test-id-1",
        MaterializedViewRefreshTaskRunId="test-id-1",
    )
    assert isinstance(desc_resp.get("MaterializedViewRefreshTaskRun", {}), dict)

    # DELETE
    client.stop_materialized_view_refresh_task_run(
        CatalogId="test-id-1",
        DatabaseName="test-name-1",
        TableName="test-name-1",
    )

    # DESCRIBE after DELETE should fail
    with pytest.raises(ClientError) as exc:
        client.get_materialized_view_refresh_task_run(
            CatalogId="test-id-1",
            MaterializedViewRefreshTaskRunId="test-id-1",
        )
    assert exc.value.response["Error"]["Code"] in (
        "ResourceNotFoundException",
        "NotFoundException",
        "EntityNotFoundException",
        "InvalidRequestException",
        "NoSuchEntity",
    )


def test_materialized_view_refresh_task_run_not_found(client):
    """Test that describing a non-existent MaterializedViewRefreshTaskRun raises an error."""
    with pytest.raises(ClientError) as exc:
        client.get_materialized_view_refresh_task_run(
            CatalogId="fake-id",
            MaterializedViewRefreshTaskRunId="fake-id",
        )
    assert exc.value.response["Error"]["Code"] in (
        "ResourceNotFoundException",
        "NotFoundException",
        "EntityNotFoundException",
        "InvalidRequestException",
        "NoSuchEntity",
    )
