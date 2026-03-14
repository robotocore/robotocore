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
    )


def test_table_optimizer_lifecycle(client):
    """Test TableOptimizer CRUD lifecycle."""
    # CREATE
    client.create_table_optimizer(
        CatalogId="test-id-1",
        DatabaseName="test-database-1",
        TableName="test-table-1",
        Type="DEFAULT",
        TableOptimizerConfiguration={},
    )

    # DESCRIBE
    desc_resp = client.get_table_optimizer(
        CatalogId="test-id-1",
        DatabaseName="test-database-1",
        TableName="test-table-1",
        Type="DEFAULT",
    )
    assert isinstance(desc_resp.get("CatalogId"), str)
    assert len(desc_resp["CatalogId"]) > 0
    assert isinstance(desc_resp.get("DatabaseName"), str)
    assert len(desc_resp.get("DatabaseName", "")) > 0
    assert isinstance(desc_resp.get("TableName"), str)

    # DELETE
    client.delete_table_optimizer(
        CatalogId="test-id-1",
        DatabaseName="test-database-1",
        TableName="test-table-1",
        Type="DEFAULT",
    )

    # DESCRIBE after DELETE should fail
    with pytest.raises(ClientError) as exc:
        client.get_table_optimizer(
            CatalogId="test-id-1",
            DatabaseName="test-database-1",
            TableName="test-table-1",
            Type="DEFAULT",
        )
    assert exc.value.response["Error"]["Code"] in (
        "ResourceNotFoundException",
        "NotFoundException",
        "EntityNotFoundException",
        "InvalidRequestException",
    )


def test_table_optimizer_not_found(client):
    """Test that describing a non-existent TableOptimizer raises an error."""
    with pytest.raises(ClientError) as exc:
        client.get_table_optimizer(
            CatalogId="fake-id",
            DatabaseName="fake-id",
            TableName="fake-id",
            Type="fake-id",
        )
    assert exc.value.response["Error"]["Code"] in (
        "ResourceNotFoundException",
        "NotFoundException",
        "EntityNotFoundException",
        "InvalidRequestException",
    )


def test_connection_type_lifecycle(client):
    """Test ConnectionType CRUD lifecycle."""
    # CREATE
    create_resp = client.register_connection_type(
        ConnectionType="DEFAULT",
        IntegrationType="DEFAULT",
        ConnectionProperties={},
        ConnectorAuthenticationConfiguration={"AuthenticationTypes": []},
        RestConfiguration={},
    )
    assert isinstance(create_resp.get("ConnectionTypeArn"), str)
    assert create_resp["ConnectionTypeArn"].startswith("arn:aws:")

    # DESCRIBE
    desc_resp = client.describe_connection_type(ConnectionType="DEFAULT")
    assert isinstance(desc_resp.get("ConnectionType"), str)
    assert isinstance(desc_resp.get("Description"), str)
    assert isinstance(desc_resp.get("Capabilities", {}), dict)
    assert isinstance(desc_resp.get("AuthenticationConfiguration", {}), dict)
    assert isinstance(desc_resp.get("RestConfiguration", {}), dict)

    # DELETE
    client.delete_connection_type(ConnectionType="DEFAULT")

    # DESCRIBE after DELETE should fail
    with pytest.raises(ClientError) as exc:
        client.describe_connection_type(ConnectionType="DEFAULT")
    assert exc.value.response["Error"]["Code"] in (
        "ResourceNotFoundException",
        "NotFoundException",
        "EntityNotFoundException",
        "InvalidRequestException",
    )


def test_connection_type_not_found(client):
    """Test that describing a non-existent ConnectionType raises an error."""
    with pytest.raises(ClientError) as exc:
        client.describe_connection_type(ConnectionType="fake-id")
    assert exc.value.response["Error"]["Code"] in (
        "ResourceNotFoundException",
        "NotFoundException",
        "EntityNotFoundException",
        "InvalidRequestException",
    )


def test_materialized_view_refresh_task_run_lifecycle(client):
    """Test MaterializedViewRefreshTaskRun CRUD lifecycle."""
    # CREATE
    create_resp = client.start_materialized_view_refresh_task_run(
        CatalogId="test-id-1",
        DatabaseName="test-database-1",
        TableName="test-table-1",
    )
    assert isinstance(create_resp.get("MaterializedViewRefreshTaskRunId"), str)
    assert len(create_resp["MaterializedViewRefreshTaskRunId"]) > 0

    # DESCRIBE
    desc_resp = client.get_materialized_view_refresh_task_run(
        CatalogId="test-id-1",
        MaterializedViewRefreshTaskRunId="test-id-1",
    )
    assert isinstance(desc_resp.get("MaterializedViewRefreshTaskRun", {}), dict)

    # DELETE
    client.stop_materialized_view_refresh_task_run(
        CatalogId="test-id-1",
        DatabaseName="test-database-1",
        TableName="test-table-1",
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
    )
