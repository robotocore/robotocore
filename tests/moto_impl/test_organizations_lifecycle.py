"""Resource lifecycle tests for organizations (auto-generated)."""

import pytest
from botocore.exceptions import ClientError


@pytest.fixture
def client():
    import boto3

    return boto3.client(
        "organizations",
        endpoint_url="http://localhost:4566",
        region_name="us-east-1",
        aws_access_key_id="testing",
        aws_secret_access_key="testing",
    )


@pytest.fixture(autouse=True)
def org(client):
    resp = client.create_organization(FeatureSet="ALL")
    yield resp["Organization"]
    try:
        client.delete_organization()
    except Exception:
        pass


def test_organization_lifecycle(client):
    """Test Organization CRUD lifecycle."""
    # CREATE
    create_resp = client.create_organization()
    assert isinstance(create_resp.get("Organization", {}), dict)

    # DESCRIBE
    desc_resp = client.describe_organization()
    assert isinstance(desc_resp.get("Organization", {}), dict)

    # DELETE
    client.delete_organization()

    # DESCRIBE after DELETE should fail
    with pytest.raises(ClientError) as exc:
        client.describe_organization()
    assert exc.value.response["Error"]["Code"] in (
        "ResourceNotFoundException",
        "ResourcePolicyNotFoundException",
        "NotFoundException",
        "EntityNotFoundException",
        "InvalidRequestException",
        "NoSuchEntity",
    )


def test_organization_not_found(client):
    """Test that describing a non-existent Organization raises error."""
    with pytest.raises(ClientError) as exc:
        client.describe_organization()
    assert exc.value.response["Error"]["Code"] in (
        "ResourceNotFoundException",
        "ResourcePolicyNotFoundException",
        "NotFoundException",
        "EntityNotFoundException",
        "InvalidRequestException",
        "NoSuchEntity",
    )


def test_organizational_unit_lifecycle(client):
    """Test OrganizationalUnit CRUD lifecycle."""
    # CREATE
    create_resp = client.create_organizational_unit(
        ParentId="test-id-1",
        Name="test-name-1",
    )
    assert isinstance(create_resp.get("OrganizationalUnit", {}), dict)

    # DESCRIBE
    desc_resp = client.describe_organizational_unit(
        OrganizationalUnitId="test-id-1",
    )
    assert isinstance(desc_resp.get("OrganizationalUnit", {}), dict)

    # DELETE
    client.delete_organizational_unit(
        OrganizationalUnitId="test-id-1",
    )

    # DESCRIBE after DELETE should fail
    with pytest.raises(ClientError) as exc:
        client.describe_organizational_unit(
            OrganizationalUnitId="test-id-1",
        )
    assert exc.value.response["Error"]["Code"] in (
        "ResourceNotFoundException",
        "ResourcePolicyNotFoundException",
        "NotFoundException",
        "EntityNotFoundException",
        "InvalidRequestException",
        "NoSuchEntity",
    )


def test_organizational_unit_not_found(client):
    """Test that describing a non-existent OrganizationalUnit raises error."""
    with pytest.raises(ClientError) as exc:
        client.describe_organizational_unit(
            OrganizationalUnitId="fake-id",
        )
    assert exc.value.response["Error"]["Code"] in (
        "ResourceNotFoundException",
        "ResourcePolicyNotFoundException",
        "NotFoundException",
        "EntityNotFoundException",
        "InvalidRequestException",
        "NoSuchEntity",
    )


def test_policy_lifecycle(client):
    """Test Policy CRUD lifecycle."""
    # CREATE
    create_resp = client.create_policy(
        Content="test-string",
        Description="test-string",
        Name="test-name-1",
        Type="SERVICE_CONTROL_POLICY",
    )
    assert isinstance(create_resp.get("Policy", {}), dict)

    # DESCRIBE
    desc_resp = client.describe_policy(
        PolicyId="test-id-1",
    )
    assert isinstance(desc_resp.get("Policy", {}), dict)

    # DELETE
    client.delete_policy(
        PolicyId="test-id-1",
    )

    # DESCRIBE after DELETE should fail
    with pytest.raises(ClientError) as exc:
        client.describe_policy(
            PolicyId="test-id-1",
        )
    assert exc.value.response["Error"]["Code"] in (
        "ResourceNotFoundException",
        "ResourcePolicyNotFoundException",
        "NotFoundException",
        "EntityNotFoundException",
        "InvalidRequestException",
        "NoSuchEntity",
    )


def test_policy_not_found(client):
    """Test that describing a non-existent Policy raises error."""
    with pytest.raises(ClientError) as exc:
        client.describe_policy(
            PolicyId="fake-id",
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
    create_resp = client.put_resource_policy(
        Content="test-string",
    )
    assert isinstance(create_resp.get("ResourcePolicy", {}), dict)

    # DESCRIBE
    desc_resp = client.describe_resource_policy()
    assert isinstance(desc_resp.get("ResourcePolicy", {}), dict)

    # DELETE
    client.delete_resource_policy()

    # DESCRIBE after DELETE should fail
    with pytest.raises(ClientError) as exc:
        client.describe_resource_policy()
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
        client.describe_resource_policy()
    assert exc.value.response["Error"]["Code"] in (
        "ResourceNotFoundException",
        "ResourcePolicyNotFoundException",
        "NotFoundException",
        "EntityNotFoundException",
        "InvalidRequestException",
        "NoSuchEntity",
    )
