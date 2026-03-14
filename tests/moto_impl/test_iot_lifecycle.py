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


def test_certificate_provider_lifecycle(client):
    """Test CertificateProvider CRUD lifecycle."""
    # CREATE
    create_resp = client.create_certificate_provider(
        certificateProviderName="test-name-1",
        lambdaFunctionArn="arn:aws:iam::123456789012:role/test-role",
        accountDefaultForOperations=[],
    )
    assert isinstance(create_resp.get("certificateProviderName"), str)
    assert len(create_resp.get("certificateProviderName", "")) > 0
    assert isinstance(create_resp.get("certificateProviderArn"), str)
    assert create_resp["certificateProviderArn"].startswith("arn:aws:")

    # DESCRIBE
    desc_resp = client.describe_certificate_provider(certificateProviderName="test-name-1")
    assert isinstance(desc_resp.get("certificateProviderName"), str)
    assert len(desc_resp.get("certificateProviderName", "")) > 0
    assert isinstance(desc_resp.get("certificateProviderArn"), str)
    assert desc_resp["certificateProviderArn"].startswith("arn:aws:")
    assert isinstance(desc_resp.get("lambdaFunctionArn"), str)

    # DELETE
    client.delete_certificate_provider(certificateProviderName="test-name-1")

    # DESCRIBE after DELETE should fail
    with pytest.raises(ClientError) as exc:
        client.describe_certificate_provider(certificateProviderName="test-name-1")
    assert exc.value.response["Error"]["Code"] in (
        "ResourceNotFoundException",
        "NotFoundException",
        "EntityNotFoundException",
        "InvalidRequestException",
    )


def test_certificate_provider_not_found(client):
    """Test that describing a non-existent CertificateProvider raises an error."""
    with pytest.raises(ClientError) as exc:
        client.describe_certificate_provider(certificateProviderName="fake-id")
    assert exc.value.response["Error"]["Code"] in (
        "ResourceNotFoundException",
        "NotFoundException",
        "EntityNotFoundException",
        "InvalidRequestException",
    )


def test_command_lifecycle(client):
    """Test Command CRUD lifecycle."""
    # CREATE
    create_resp = client.create_command(commandId="test-id-1")
    assert isinstance(create_resp.get("commandId"), str)
    assert len(create_resp["commandId"]) > 0
    assert isinstance(create_resp.get("commandArn"), str)
    assert create_resp["commandArn"].startswith("arn:aws:")

    # DESCRIBE
    desc_resp = client.get_command(commandId="test-id-1")
    assert isinstance(desc_resp.get("commandId"), str)
    assert len(desc_resp["commandId"]) > 0
    assert isinstance(desc_resp.get("commandArn"), str)
    assert desc_resp["commandArn"].startswith("arn:aws:")
    assert isinstance(desc_resp.get("namespace"), str)

    # DELETE
    client.delete_command(commandId="test-id-1")

    # DESCRIBE after DELETE should fail
    with pytest.raises(ClientError) as exc:
        client.get_command(commandId="test-id-1")
    assert exc.value.response["Error"]["Code"] in (
        "ResourceNotFoundException",
        "NotFoundException",
        "EntityNotFoundException",
        "InvalidRequestException",
    )


def test_command_not_found(client):
    """Test that describing a non-existent Command raises an error."""
    with pytest.raises(ClientError) as exc:
        client.get_command(commandId="fake-id")
    assert exc.value.response["Error"]["Code"] in (
        "ResourceNotFoundException",
        "NotFoundException",
        "EntityNotFoundException",
        "InvalidRequestException",
    )
