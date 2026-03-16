"""Resource lifecycle tests for cognito-idp (auto-generated)."""

import pytest
from botocore.exceptions import ClientError


@pytest.fixture
def client():
    import boto3

    return boto3.client(
        "cognito-idp",
        endpoint_url="http://localhost:4566",
        region_name="us-east-1",
        aws_access_key_id="testing",
        aws_secret_access_key="testing",
    )


@pytest.fixture
def user_pool_id(client):
    resp = client.create_user_pool(PoolName="test-pool-1")
    pool_id = resp["UserPool"]["Id"]
    yield pool_id
    try:
        client.delete_user_pool(UserPoolId=pool_id)
    except Exception:
        pass  # best-effort cleanup


def test_group_lifecycle(client, user_pool_id):
    """Test Group CRUD lifecycle."""
    # CREATE
    create_resp = client.create_group(
        GroupName="test-name-1",
        UserPoolId=user_pool_id,
    )
    assert isinstance(create_resp.get("Group", {}), dict)

    # DESCRIBE
    desc_resp = client.get_group(
        GroupName="test-name-1",
        UserPoolId=user_pool_id,
    )
    assert isinstance(desc_resp.get("Group", {}), dict)

    # DELETE
    client.delete_group(
        GroupName="test-name-1",
        UserPoolId=user_pool_id,
    )

    # DESCRIBE after DELETE should fail
    with pytest.raises(ClientError) as exc:
        client.get_group(
            GroupName="test-name-1",
            UserPoolId=user_pool_id,
        )
    assert exc.value.response["Error"]["Code"] in (
        "ResourceNotFoundException",
        "ResourcePolicyNotFoundException",
        "NotFoundException",
        "EntityNotFoundException",
        "InvalidRequestException",
        "NoSuchEntity",
    )


def test_group_not_found(client, user_pool_id):
    """Test that describing a non-existent Group raises error."""
    with pytest.raises(ClientError) as exc:
        client.get_group(
            GroupName="fake-id",
            UserPoolId=user_pool_id,
        )
    assert exc.value.response["Error"]["Code"] in (
        "ResourceNotFoundException",
        "ResourcePolicyNotFoundException",
        "NotFoundException",
        "EntityNotFoundException",
        "InvalidRequestException",
        "NoSuchEntity",
    )


def test_identity_provider_lifecycle(client, user_pool_id):
    """Test IdentityProvider CRUD lifecycle."""
    # CREATE
    create_resp = client.create_identity_provider(
        UserPoolId=user_pool_id,
        ProviderName="test-name-1",
        ProviderType="SAML",
        ProviderDetails={},
    )
    assert isinstance(create_resp.get("IdentityProvider", {}), dict)

    # DESCRIBE
    desc_resp = client.describe_identity_provider(
        UserPoolId=user_pool_id,
        ProviderName="test-name-1",
    )
    assert isinstance(desc_resp.get("IdentityProvider", {}), dict)

    # DELETE
    client.delete_identity_provider(
        UserPoolId=user_pool_id,
        ProviderName="test-name-1",
    )

    # DESCRIBE after DELETE should fail
    with pytest.raises(ClientError) as exc:
        client.describe_identity_provider(
            UserPoolId=user_pool_id,
            ProviderName="test-name-1",
        )
    assert exc.value.response["Error"]["Code"] in (
        "ResourceNotFoundException",
        "ResourcePolicyNotFoundException",
        "NotFoundException",
        "EntityNotFoundException",
        "InvalidRequestException",
        "NoSuchEntity",
    )


def test_identity_provider_not_found(client, user_pool_id):
    """Test that describing a non-existent IdentityProvider raises error."""
    with pytest.raises(ClientError) as exc:
        client.describe_identity_provider(
            UserPoolId=user_pool_id,
            ProviderName="fake-id",
        )
    assert exc.value.response["Error"]["Code"] in (
        "ResourceNotFoundException",
        "ResourcePolicyNotFoundException",
        "NotFoundException",
        "EntityNotFoundException",
        "InvalidRequestException",
        "NoSuchEntity",
    )


def test_managed_login_branding_lifecycle(client, user_pool_id):
    """Test ManagedLoginBranding CRUD lifecycle."""
    # CREATE
    create_resp = client.create_managed_login_branding(
        UserPoolId=user_pool_id,
        ClientId="test-id-1",
    )
    assert isinstance(create_resp.get("ManagedLoginBranding", {}), dict)

    managed_login_branding_id = create_resp["ManagedLoginBranding"]["ManagedLoginBrandingId"]

    # DESCRIBE
    desc_resp = client.describe_managed_login_branding(
        UserPoolId=user_pool_id,
        ManagedLoginBrandingId=managed_login_branding_id,
    )
    assert isinstance(desc_resp.get("ManagedLoginBranding", {}), dict)

    # DELETE
    client.delete_managed_login_branding(
        ManagedLoginBrandingId=managed_login_branding_id,
        UserPoolId=user_pool_id,
    )

    # DESCRIBE after DELETE should fail
    with pytest.raises(ClientError) as exc:
        client.describe_managed_login_branding(
            UserPoolId=user_pool_id,
            ManagedLoginBrandingId=managed_login_branding_id,
        )
    assert exc.value.response["Error"]["Code"] in (
        "ResourceNotFoundException",
        "ResourcePolicyNotFoundException",
        "NotFoundException",
        "EntityNotFoundException",
        "InvalidRequestException",
        "NoSuchEntity",
    )


def test_managed_login_branding_not_found(client, user_pool_id):
    """Test that describing a non-existent ManagedLoginBranding raises error."""
    with pytest.raises(ClientError) as exc:
        client.describe_managed_login_branding(
            UserPoolId=user_pool_id,
            ManagedLoginBrandingId="fake-id",
        )
    assert exc.value.response["Error"]["Code"] in (
        "ResourceNotFoundException",
        "ResourcePolicyNotFoundException",
        "NotFoundException",
        "EntityNotFoundException",
        "InvalidRequestException",
        "NoSuchEntity",
    )


def test_resource_server_lifecycle(client, user_pool_id):
    """Test ResourceServer CRUD lifecycle."""
    # CREATE
    create_resp = client.create_resource_server(
        UserPoolId=user_pool_id,
        Identifier="test-name-1",
        Name="test-name-1",
    )
    assert isinstance(create_resp.get("ResourceServer", {}), dict)

    # DESCRIBE
    desc_resp = client.describe_resource_server(
        UserPoolId=user_pool_id,
        Identifier="test-name-1",
    )
    assert isinstance(desc_resp.get("ResourceServer", {}), dict)

    # DELETE
    client.delete_resource_server(
        UserPoolId=user_pool_id,
        Identifier="test-id-1",
    )

    # DESCRIBE after DELETE should fail
    with pytest.raises(ClientError) as exc:
        client.describe_resource_server(
            UserPoolId=user_pool_id,
            Identifier="test-name-1",
        )
    assert exc.value.response["Error"]["Code"] in (
        "ResourceNotFoundException",
        "ResourcePolicyNotFoundException",
        "NotFoundException",
        "EntityNotFoundException",
        "InvalidRequestException",
        "NoSuchEntity",
    )


def test_resource_server_not_found(client, user_pool_id):
    """Test that describing a non-existent ResourceServer raises error."""
    with pytest.raises(ClientError) as exc:
        client.describe_resource_server(
            UserPoolId=user_pool_id,
            Identifier="fake-id",
        )
    assert exc.value.response["Error"]["Code"] in (
        "ResourceNotFoundException",
        "ResourcePolicyNotFoundException",
        "NotFoundException",
        "EntityNotFoundException",
        "InvalidRequestException",
        "NoSuchEntity",
    )


def test_terms_lifecycle(client, user_pool_id):
    """Test Terms CRUD lifecycle."""
    # CREATE
    create_resp = client.create_terms(
        UserPoolId=user_pool_id,
        ClientId="test-id-1",
        TermsName="test-name-1",
        TermsSource="LINK",
        Enforcement="NONE",
    )
    assert isinstance(create_resp.get("Terms", {}), dict)

    terms_id = create_resp["Terms"]["TermsId"]

    # DESCRIBE
    desc_resp = client.describe_terms(
        TermsId=terms_id,
        UserPoolId=user_pool_id,
    )
    assert isinstance(desc_resp.get("Terms", {}), dict)

    # DELETE
    client.delete_terms(
        TermsId=terms_id,
        UserPoolId=user_pool_id,
    )

    # DESCRIBE after DELETE should fail
    with pytest.raises(ClientError) as exc:
        client.describe_terms(
            TermsId=terms_id,
            UserPoolId=user_pool_id,
        )
    assert exc.value.response["Error"]["Code"] in (
        "ResourceNotFoundException",
        "ResourcePolicyNotFoundException",
        "NotFoundException",
        "EntityNotFoundException",
        "InvalidRequestException",
        "NoSuchEntity",
    )


def test_terms_not_found(client, user_pool_id):
    """Test that describing a non-existent Terms raises error."""
    with pytest.raises(ClientError) as exc:
        client.describe_terms(
            TermsId="fake-id",
            UserPoolId=user_pool_id,
        )
    assert exc.value.response["Error"]["Code"] in (
        "ResourceNotFoundException",
        "ResourcePolicyNotFoundException",
        "NotFoundException",
        "EntityNotFoundException",
        "InvalidRequestException",
        "NoSuchEntity",
    )


def test_user_import_job_lifecycle(client, user_pool_id):
    """Test UserImportJob CRUD lifecycle."""
    # CREATE
    create_resp = client.create_user_import_job(
        JobName="test-name-1",
        UserPoolId=user_pool_id,
        CloudWatchLogsRoleArn="arn:aws:iam::123456789012:role/test-role",
    )
    assert isinstance(create_resp.get("UserImportJob", {}), dict)

    job_id = create_resp["UserImportJob"]["JobId"]

    # DESCRIBE
    desc_resp = client.describe_user_import_job(
        UserPoolId=user_pool_id,
        JobId=job_id,
    )
    assert isinstance(desc_resp.get("UserImportJob", {}), dict)

    # DELETE
    client.stop_user_import_job(
        UserPoolId=user_pool_id,
        JobId=job_id,
    )

    # DESCRIBE after DELETE should fail
    with pytest.raises(ClientError) as exc:
        client.describe_user_import_job(
            UserPoolId=user_pool_id,
            JobId=job_id,
        )
    assert exc.value.response["Error"]["Code"] in (
        "ResourceNotFoundException",
        "ResourcePolicyNotFoundException",
        "NotFoundException",
        "EntityNotFoundException",
        "InvalidRequestException",
        "NoSuchEntity",
    )


def test_user_import_job_not_found(client, user_pool_id):
    """Test that describing a non-existent UserImportJob raises error."""
    with pytest.raises(ClientError) as exc:
        client.describe_user_import_job(
            UserPoolId=user_pool_id,
            JobId="fake-id",
        )
    assert exc.value.response["Error"]["Code"] in (
        "ResourceNotFoundException",
        "ResourcePolicyNotFoundException",
        "NotFoundException",
        "EntityNotFoundException",
        "InvalidRequestException",
        "NoSuchEntity",
    )


def test_user_pool_lifecycle(client, user_pool_id):
    """Test UserPool CRUD lifecycle."""
    # CREATE
    create_resp = client.create_user_pool(
        PoolName="test-name-1",
    )
    assert isinstance(create_resp.get("UserPool", {}), dict)

    # DESCRIBE
    desc_resp = client.describe_user_pool(
        UserPoolId=user_pool_id,
    )
    assert isinstance(desc_resp.get("UserPool", {}), dict)

    # DELETE
    client.delete_user_pool(
        UserPoolId=user_pool_id,
    )

    # DESCRIBE after DELETE should fail
    with pytest.raises(ClientError) as exc:
        client.describe_user_pool(
            UserPoolId=user_pool_id,
        )
    assert exc.value.response["Error"]["Code"] in (
        "ResourceNotFoundException",
        "ResourcePolicyNotFoundException",
        "NotFoundException",
        "EntityNotFoundException",
        "InvalidRequestException",
        "NoSuchEntity",
    )


def test_user_pool_not_found(client, user_pool_id):
    """Test that describing a non-existent UserPool raises error."""
    with pytest.raises(ClientError) as exc:
        client.describe_user_pool(
            UserPoolId=user_pool_id,
        )
    assert exc.value.response["Error"]["Code"] in (
        "ResourceNotFoundException",
        "ResourcePolicyNotFoundException",
        "NotFoundException",
        "EntityNotFoundException",
        "InvalidRequestException",
        "NoSuchEntity",
    )


def test_user_pool_client_lifecycle(client, user_pool_id):
    """Test UserPoolClient CRUD lifecycle."""
    # CREATE
    create_resp = client.create_user_pool_client(
        UserPoolId=user_pool_id,
        ClientName="test-name-1",
    )
    assert isinstance(create_resp.get("UserPoolClient", {}), dict)

    client_id = create_resp["UserPoolClient"]["ClientId"]

    # DESCRIBE
    desc_resp = client.describe_user_pool_client(
        UserPoolId=user_pool_id,
        ClientId=client_id,
    )
    assert isinstance(desc_resp.get("UserPoolClient", {}), dict)

    # DELETE
    client.delete_user_pool_client(
        UserPoolId=user_pool_id,
        ClientId=client_id,
    )

    # DESCRIBE after DELETE should fail
    with pytest.raises(ClientError) as exc:
        client.describe_user_pool_client(
            UserPoolId=user_pool_id,
            ClientId=client_id,
        )
    assert exc.value.response["Error"]["Code"] in (
        "ResourceNotFoundException",
        "ResourcePolicyNotFoundException",
        "NotFoundException",
        "EntityNotFoundException",
        "InvalidRequestException",
        "NoSuchEntity",
    )


def test_user_pool_client_not_found(client, user_pool_id):
    """Test that describing a non-existent UserPoolClient raises error."""
    with pytest.raises(ClientError) as exc:
        client.describe_user_pool_client(
            UserPoolId=user_pool_id,
            ClientId="fake-id",
        )
    assert exc.value.response["Error"]["Code"] in (
        "ResourceNotFoundException",
        "ResourcePolicyNotFoundException",
        "NotFoundException",
        "EntityNotFoundException",
        "InvalidRequestException",
        "NoSuchEntity",
    )


def test_user_pool_domain_lifecycle(client, user_pool_id):
    """Test UserPoolDomain CRUD lifecycle."""
    # CREATE
    client.create_user_pool_domain(
        Domain="test-string",
        UserPoolId=user_pool_id,
    )

    # DESCRIBE
    desc_resp = client.describe_user_pool_domain(
        Domain="test-string",
    )
    assert isinstance(desc_resp.get("DomainDescription", {}), dict)

    # DELETE
    client.delete_user_pool_domain(
        Domain="test-string",
        UserPoolId=user_pool_id,
    )

    # DESCRIBE after DELETE should fail
    with pytest.raises(ClientError) as exc:
        client.describe_user_pool_domain(
            Domain="test-string",
        )
    assert exc.value.response["Error"]["Code"] in (
        "ResourceNotFoundException",
        "ResourcePolicyNotFoundException",
        "NotFoundException",
        "EntityNotFoundException",
        "InvalidRequestException",
        "NoSuchEntity",
    )


def test_user_pool_domain_not_found(client, user_pool_id):
    """Test that describing a non-existent UserPoolDomain raises error."""
    with pytest.raises(ClientError) as exc:
        client.describe_user_pool_domain(
            Domain="fake-id",
        )
    assert exc.value.response["Error"]["Code"] in (
        "ResourceNotFoundException",
        "ResourcePolicyNotFoundException",
        "NotFoundException",
        "EntityNotFoundException",
        "InvalidRequestException",
        "NoSuchEntity",
    )
