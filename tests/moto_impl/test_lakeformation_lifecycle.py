"""Resource lifecycle tests for lakeformation (auto-generated)."""

import pytest
from botocore.exceptions import ClientError


@pytest.fixture
def client():
    import boto3

    return boto3.client(
        "lakeformation",
        endpoint_url="http://localhost:4566",
        region_name="us-east-1",
        aws_access_key_id="testing",
        aws_secret_access_key="testing",
    )


def test_data_cells_filter_lifecycle(client):
    """Test DataCellsFilter CRUD lifecycle."""
    # CREATE
    client.create_data_cells_filter(
        TableData={
            "TableCatalogId": "test-id-1",
            "DatabaseName": "test-name-1",
            "TableName": "test-name-1",
            "Name": "test-name-1",
        },
    )

    # DESCRIBE
    desc_resp = client.get_data_cells_filter(
        TableCatalogId="test-id-1",
        DatabaseName="test-name-1",
        TableName="test-name-1",
        Name="test-name-1",
    )
    assert isinstance(desc_resp.get("DataCellsFilter", {}), dict)

    # DELETE
    client.delete_data_cells_filter()

    # DESCRIBE after DELETE should fail
    with pytest.raises(ClientError) as exc:
        client.get_data_cells_filter(
            TableCatalogId="test-id-1",
            DatabaseName="test-name-1",
            TableName="test-name-1",
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


def test_data_cells_filter_not_found(client):
    """Test that describing a non-existent DataCellsFilter raises error."""
    with pytest.raises(ClientError) as exc:
        client.get_data_cells_filter(
            TableCatalogId="fake-id",
            DatabaseName="fake-id",
            TableName="fake-id",
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


def test_data_lake_settings_lifecycle(client):
    """Test DataLakeSettings CRUD lifecycle."""
    # CREATE
    client.put_data_lake_settings(
        DataLakeSettings={},
    )

    # DESCRIBE
    desc_resp = client.get_data_lake_settings()
    assert isinstance(desc_resp.get("DataLakeSettings", {}), dict)


def test_data_lake_settings_not_found(client):
    """Test that describing a non-existent DataLakeSettings raises error."""
    with pytest.raises(ClientError) as exc:
        client.get_data_lake_settings()
    assert exc.value.response["Error"]["Code"] in (
        "ResourceNotFoundException",
        "ResourcePolicyNotFoundException",
        "NotFoundException",
        "EntityNotFoundException",
        "InvalidRequestException",
        "NoSuchEntity",
    )


def test_lf_tag_lifecycle(client):
    """Test LFTag CRUD lifecycle."""
    # CREATE
    client.create_lf_tag(
        TagKey="test-string",
        TagValues=["test-string"],
    )

    # DESCRIBE
    desc_resp = client.get_lf_tag(
        TagKey="test-string",
    )
    assert isinstance(desc_resp.get("TagKey"), str)
    assert len(desc_resp.get("TagKey", "")) > 0
    assert isinstance(desc_resp.get("TagValues", []), list)

    # DELETE
    client.delete_lf_tag(
        TagKey="test-string",
    )

    # DESCRIBE after DELETE should fail
    with pytest.raises(ClientError) as exc:
        client.get_lf_tag(
            TagKey="test-string",
        )
    assert exc.value.response["Error"]["Code"] in (
        "ResourceNotFoundException",
        "ResourcePolicyNotFoundException",
        "NotFoundException",
        "EntityNotFoundException",
        "InvalidRequestException",
        "NoSuchEntity",
    )


def test_lf_tag_not_found(client):
    """Test that describing a non-existent LFTag raises error."""
    with pytest.raises(ClientError) as exc:
        client.get_lf_tag(
            TagKey="fake-id",
        )
    assert exc.value.response["Error"]["Code"] in (
        "ResourceNotFoundException",
        "ResourcePolicyNotFoundException",
        "NotFoundException",
        "EntityNotFoundException",
        "InvalidRequestException",
        "NoSuchEntity",
    )


def test_lf_tag_expression_lifecycle(client):
    """Test LFTagExpression CRUD lifecycle."""
    # CREATE
    client.create_lf_tag_expression(
        Name="test-name-1",
        Expression=[{"TagKey": "test-string", "TagValues": ["test-string"]}],
    )

    # DESCRIBE
    desc_resp = client.get_lf_tag_expression(
        Name="test-name-1",
    )
    assert isinstance(desc_resp.get("Name"), str)
    assert len(desc_resp.get("Name", "")) > 0
    assert isinstance(desc_resp.get("Expression", []), list)

    # DELETE
    client.delete_lf_tag_expression(
        Name="test-name-1",
    )

    # DESCRIBE after DELETE should fail
    with pytest.raises(ClientError) as exc:
        client.get_lf_tag_expression(
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


def test_lf_tag_expression_not_found(client):
    """Test that describing a non-existent LFTagExpression raises error."""
    with pytest.raises(ClientError) as exc:
        client.get_lf_tag_expression(
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


def test_lake_formation_identity_center_configuration_lifecycle(client):
    """Test LakeFormationIdentityCenterConfiguration CRUD lifecycle."""
    # CREATE
    client.create_lake_formation_identity_center_configuration()

    # DESCRIBE
    desc_resp = client.describe_lake_formation_identity_center_configuration()
    assert isinstance(desc_resp.get("ExternalFiltering", {}), dict)
    assert isinstance(desc_resp.get("ShareRecipients", []), list)
    assert isinstance(desc_resp.get("ServiceIntegrations", []), list)

    # DELETE
    client.delete_lake_formation_identity_center_configuration()

    # DESCRIBE after DELETE should fail
    with pytest.raises(ClientError) as exc:
        client.describe_lake_formation_identity_center_configuration()
    assert exc.value.response["Error"]["Code"] in (
        "ResourceNotFoundException",
        "ResourcePolicyNotFoundException",
        "NotFoundException",
        "EntityNotFoundException",
        "InvalidRequestException",
        "NoSuchEntity",
    )


def test_lake_formation_identity_center_configuration_not_found(client):
    """Test that describing a non-existent LakeFormationIdentityCenterConfigurat... raises error."""
    with pytest.raises(ClientError) as exc:
        client.describe_lake_formation_identity_center_configuration()
    assert exc.value.response["Error"]["Code"] in (
        "ResourceNotFoundException",
        "ResourcePolicyNotFoundException",
        "NotFoundException",
        "EntityNotFoundException",
        "InvalidRequestException",
        "NoSuchEntity",
    )


def test_transaction_lifecycle(client):
    """Test Transaction CRUD lifecycle."""
    # CREATE
    create_resp = client.start_transaction()
    assert isinstance(create_resp.get("TransactionId"), str)
    assert len(create_resp.get("TransactionId", "")) > 0

    transaction_id = create_resp["TransactionId"]

    # DESCRIBE
    desc_resp = client.describe_transaction(
        TransactionId=transaction_id,
    )
    assert isinstance(desc_resp.get("TransactionDescription", {}), dict)


def test_transaction_not_found(client):
    """Test that describing a non-existent Transaction raises error."""
    with pytest.raises(ClientError) as exc:
        client.describe_transaction(
            TransactionId="fake-id",
        )
    assert exc.value.response["Error"]["Code"] in (
        "ResourceNotFoundException",
        "ResourcePolicyNotFoundException",
        "NotFoundException",
        "EntityNotFoundException",
        "InvalidRequestException",
        "NoSuchEntity",
    )
