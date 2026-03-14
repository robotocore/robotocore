"""Resource lifecycle tests for opensearch (auto-generated)."""

import pytest
from botocore.exceptions import ClientError


@pytest.fixture
def client():
    import boto3

    return boto3.client(
        "opensearch",
        endpoint_url="http://localhost:4566",
        region_name="us-east-1",
        aws_access_key_id="testing",
        aws_secret_access_key="testing",
    )


@pytest.fixture
def domain_name(client):
    name = "test-domain-1"
    try:
        client.create_domain(DomainName=name)
    except Exception:
        pass
    yield name


def test_application_lifecycle(client, domain_name):
    """Test Application CRUD lifecycle."""
    # CREATE
    create_resp = client.create_application(
        name="test-name-1",
    )
    assert isinstance(create_resp.get("id"), str)
    assert len(create_resp.get("id", "")) > 0
    assert isinstance(create_resp.get("dataSources", []), list)
    assert isinstance(create_resp.get("iamIdentityCenterOptions", {}), dict)
    assert isinstance(create_resp.get("appConfigs", []), list)
    assert isinstance(create_resp.get("tagList", []), list)
    assert create_resp.get("createdAt") is not None

    id = create_resp["id"]

    # DESCRIBE
    desc_resp = client.get_application(
        id=id,
    )
    assert isinstance(desc_resp.get("id"), str)
    assert len(desc_resp.get("id", "")) > 0
    assert isinstance(desc_resp.get("iamIdentityCenterOptions", {}), dict)
    assert isinstance(desc_resp.get("dataSources", []), list)
    assert isinstance(desc_resp.get("appConfigs", []), list)

    # DELETE
    client.delete_application(
        id=id,
    )

    # DESCRIBE after DELETE should fail
    with pytest.raises(ClientError) as exc:
        client.get_application(
            id=id,
        )
    assert exc.value.response["Error"]["Code"] in (
        "ResourceNotFoundException",
        "ResourcePolicyNotFoundException",
        "NotFoundException",
        "EntityNotFoundException",
        "InvalidRequestException",
        "NoSuchEntity",
    )


def test_application_not_found(client, domain_name):
    """Test that describing a non-existent Application raises error."""
    with pytest.raises(ClientError) as exc:
        client.get_application(
            id="fake-id",
        )
    assert exc.value.response["Error"]["Code"] in (
        "ResourceNotFoundException",
        "ResourcePolicyNotFoundException",
        "NotFoundException",
        "EntityNotFoundException",
        "InvalidRequestException",
        "NoSuchEntity",
    )


def test_data_source_lifecycle(client, domain_name):
    """Test DataSource CRUD lifecycle."""
    # CREATE
    client.add_data_source(
        DomainName=domain_name,
        Name="test-name-1",
        DataSourceType={"S3GlueDataCatalog": {}},
    )

    # DESCRIBE
    desc_resp = client.get_data_source(
        DomainName=domain_name,
        Name="test-name-1",
    )
    assert isinstance(desc_resp.get("DataSourceType", {}), dict)
    assert isinstance(desc_resp.get("Name"), str)
    assert len(desc_resp.get("Name", "")) > 0

    # DELETE
    client.delete_data_source(
        DomainName=domain_name,
        Name="test-name-1",
    )

    # DESCRIBE after DELETE should fail
    with pytest.raises(ClientError) as exc:
        client.get_data_source(
            DomainName=domain_name,
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


def test_data_source_not_found(client, domain_name):
    """Test that describing a non-existent DataSource raises error."""
    with pytest.raises(ClientError) as exc:
        client.get_data_source(
            DomainName=domain_name,
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


def test_default_application_setting_lifecycle(client, domain_name):
    """Test DefaultApplicationSetting CRUD lifecycle."""
    # CREATE
    client.put_default_application_setting(
        applicationArn="arn:aws:iam::123456789012:role/test-role",
        setAsDefault=True,
    )

    # DESCRIBE
    client.get_default_application_setting()


def test_default_application_setting_not_found(client, domain_name):
    """Test that describing a non-existent DefaultApplicationSetting raises error."""
    with pytest.raises(ClientError) as exc:
        client.get_default_application_setting()
    assert exc.value.response["Error"]["Code"] in (
        "ResourceNotFoundException",
        "ResourcePolicyNotFoundException",
        "NotFoundException",
        "EntityNotFoundException",
        "InvalidRequestException",
        "NoSuchEntity",
    )


def test_direct_query_data_source_lifecycle(client, domain_name):
    """Test DirectQueryDataSource CRUD lifecycle."""
    # CREATE
    client.add_direct_query_data_source(
        DataSourceName="test-name-1",
        DataSourceType={"CloudWatchLog": {"RoleArn": "arn:aws:iam::123456789012:role/test-role"}},
        OpenSearchArns=["test-string"],
    )

    # DESCRIBE
    desc_resp = client.get_direct_query_data_source(
        DataSourceName="test-name-1",
    )
    assert isinstance(desc_resp.get("DataSourceName"), str)
    assert len(desc_resp.get("DataSourceName", "")) > 0
    assert isinstance(desc_resp.get("DataSourceType", {}), dict)
    assert isinstance(desc_resp.get("OpenSearchArns", []), list)

    # DELETE
    client.delete_direct_query_data_source(
        DataSourceName="test-name-1",
    )

    # DESCRIBE after DELETE should fail
    with pytest.raises(ClientError) as exc:
        client.get_direct_query_data_source(
            DataSourceName="test-name-1",
        )
    assert exc.value.response["Error"]["Code"] in (
        "ResourceNotFoundException",
        "ResourcePolicyNotFoundException",
        "NotFoundException",
        "EntityNotFoundException",
        "InvalidRequestException",
        "NoSuchEntity",
    )


def test_direct_query_data_source_not_found(client, domain_name):
    """Test that describing a non-existent DirectQueryDataSource raises error."""
    with pytest.raises(ClientError) as exc:
        client.get_direct_query_data_source(
            DataSourceName="fake-id",
        )
    assert exc.value.response["Error"]["Code"] in (
        "ResourceNotFoundException",
        "ResourcePolicyNotFoundException",
        "NotFoundException",
        "EntityNotFoundException",
        "InvalidRequestException",
        "NoSuchEntity",
    )


def test_domain_lifecycle(client, domain_name):
    """Test Domain CRUD lifecycle."""
    # CREATE
    create_resp = client.create_domain(
        DomainName=domain_name,
    )
    assert isinstance(create_resp.get("DomainStatus", {}), dict)

    # DESCRIBE
    desc_resp = client.describe_domain(
        DomainName=domain_name,
    )
    assert isinstance(desc_resp.get("DomainStatus", {}), dict)

    # DELETE
    client.delete_domain(
        DomainName=domain_name,
    )

    # DESCRIBE after DELETE should fail
    with pytest.raises(ClientError) as exc:
        client.describe_domain(
            DomainName=domain_name,
        )
    assert exc.value.response["Error"]["Code"] in (
        "ResourceNotFoundException",
        "ResourcePolicyNotFoundException",
        "NotFoundException",
        "EntityNotFoundException",
        "InvalidRequestException",
        "NoSuchEntity",
    )


def test_domain_not_found(client, domain_name):
    """Test that describing a non-existent Domain raises error."""
    with pytest.raises(ClientError) as exc:
        client.describe_domain(
            DomainName=domain_name,
        )
    assert exc.value.response["Error"]["Code"] in (
        "ResourceNotFoundException",
        "ResourcePolicyNotFoundException",
        "NotFoundException",
        "EntityNotFoundException",
        "InvalidRequestException",
        "NoSuchEntity",
    )


def test_index_lifecycle(client, domain_name):
    """Test Index CRUD lifecycle."""
    # CREATE
    create_resp = client.create_index(
        DomainName=domain_name,
        IndexName="test-name-1",
        IndexSchema={},
    )
    assert isinstance(create_resp.get("Status"), str)

    # DESCRIBE
    desc_resp = client.get_index(
        DomainName=domain_name,
        IndexName="test-name-1",
    )
    assert isinstance(desc_resp.get("IndexSchema", {}), dict)

    # DELETE
    client.delete_index(
        DomainName=domain_name,
        IndexName="test-name-1",
    )

    # DESCRIBE after DELETE should fail
    with pytest.raises(ClientError) as exc:
        client.get_index(
            DomainName=domain_name,
            IndexName="test-name-1",
        )
    assert exc.value.response["Error"]["Code"] in (
        "ResourceNotFoundException",
        "ResourcePolicyNotFoundException",
        "NotFoundException",
        "EntityNotFoundException",
        "InvalidRequestException",
        "NoSuchEntity",
    )


def test_index_not_found(client, domain_name):
    """Test that describing a non-existent Index raises error."""
    with pytest.raises(ClientError) as exc:
        client.get_index(
            DomainName=domain_name,
            IndexName="fake-id",
        )
    assert exc.value.response["Error"]["Code"] in (
        "ResourceNotFoundException",
        "ResourcePolicyNotFoundException",
        "NotFoundException",
        "EntityNotFoundException",
        "InvalidRequestException",
        "NoSuchEntity",
    )
