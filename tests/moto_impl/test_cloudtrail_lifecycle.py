"""Resource lifecycle tests for cloudtrail (auto-generated)."""

import pytest
from botocore.exceptions import ClientError


@pytest.fixture
def client():
    import boto3

    return boto3.client(
        "cloudtrail",
        endpoint_url="http://localhost:4566",
        region_name="us-east-1",
        aws_access_key_id="testing",
        aws_secret_access_key="testing",
    )


def test_channel_lifecycle(client):
    """Test Channel CRUD lifecycle."""
    # CREATE
    create_resp = client.create_channel(
        Name="test-name-1",
        Source="test-string",
        Destinations=[{"Type": "EVENT_DATA_STORE", "Location": "test-string"}],
    )
    assert isinstance(create_resp.get("Destinations", []), list)
    assert isinstance(create_resp.get("Tags", []), list)

    # DESCRIBE
    desc_resp = client.get_channel(
        Channel="test-string",
    )
    assert isinstance(desc_resp.get("SourceConfig", {}), dict)
    assert isinstance(desc_resp.get("Destinations", []), list)
    assert isinstance(desc_resp.get("IngestionStatus", {}), dict)

    # DELETE
    client.delete_channel(
        Channel="test-string",
    )

    # DESCRIBE after DELETE should fail
    with pytest.raises(ClientError) as exc:
        client.get_channel(
            Channel="test-string",
        )
    assert exc.value.response["Error"]["Code"] in (
        "ResourceNotFoundException",
        "ResourcePolicyNotFoundException",
        "NotFoundException",
        "EntityNotFoundException",
        "InvalidRequestException",
        "NoSuchEntity",
    )


def test_channel_not_found(client):
    """Test that describing a non-existent Channel raises error."""
    with pytest.raises(ClientError) as exc:
        client.get_channel(
            Channel="fake-id",
        )
    assert exc.value.response["Error"]["Code"] in (
        "ResourceNotFoundException",
        "ResourcePolicyNotFoundException",
        "NotFoundException",
        "EntityNotFoundException",
        "InvalidRequestException",
        "NoSuchEntity",
    )


def test_dashboard_lifecycle(client):
    """Test Dashboard CRUD lifecycle."""
    # CREATE
    create_resp = client.create_dashboard(
        Name="test-name-1",
    )
    assert isinstance(create_resp.get("Widgets", []), list)
    assert isinstance(create_resp.get("TagsList", []), list)
    assert isinstance(create_resp.get("RefreshSchedule", {}), dict)

    # DESCRIBE
    desc_resp = client.get_dashboard(
        DashboardId="test-name-1",
    )
    assert isinstance(desc_resp.get("Widgets", []), list)
    assert isinstance(desc_resp.get("RefreshSchedule", {}), dict)

    # DELETE
    client.delete_dashboard(
        DashboardId="test-name-1",
    )

    # DESCRIBE after DELETE should fail
    with pytest.raises(ClientError) as exc:
        client.get_dashboard(
            DashboardId="test-name-1",
        )
    assert exc.value.response["Error"]["Code"] in (
        "ResourceNotFoundException",
        "ResourcePolicyNotFoundException",
        "NotFoundException",
        "EntityNotFoundException",
        "InvalidRequestException",
        "NoSuchEntity",
    )


def test_dashboard_not_found(client):
    """Test that describing a non-existent Dashboard raises error."""
    with pytest.raises(ClientError) as exc:
        client.get_dashboard(
            DashboardId="fake-id",
        )
    assert exc.value.response["Error"]["Code"] in (
        "ResourceNotFoundException",
        "ResourcePolicyNotFoundException",
        "NotFoundException",
        "EntityNotFoundException",
        "InvalidRequestException",
        "NoSuchEntity",
    )


def test_event_configuration_lifecycle(client):
    """Test EventConfiguration CRUD lifecycle."""
    # CREATE
    create_resp = client.put_event_configuration()
    assert isinstance(create_resp.get("ContextKeySelectors", []), list)
    assert isinstance(create_resp.get("AggregationConfigurations", []), list)

    # DESCRIBE
    desc_resp = client.get_event_configuration()
    assert isinstance(desc_resp.get("ContextKeySelectors", []), list)
    assert isinstance(desc_resp.get("AggregationConfigurations", []), list)


def test_event_configuration_not_found(client):
    """Test that describing a non-existent EventConfiguration raises error."""
    with pytest.raises(ClientError) as exc:
        client.get_event_configuration()
    assert exc.value.response["Error"]["Code"] in (
        "ResourceNotFoundException",
        "ResourcePolicyNotFoundException",
        "NotFoundException",
        "EntityNotFoundException",
        "InvalidRequestException",
        "NoSuchEntity",
    )


def test_event_data_store_lifecycle(client):
    """Test EventDataStore CRUD lifecycle."""
    # CREATE
    create_resp = client.create_event_data_store(
        Name="test-name-1",
    )
    assert isinstance(create_resp.get("AdvancedEventSelectors", []), list)
    assert isinstance(create_resp.get("TagsList", []), list)
    assert create_resp.get("CreatedTimestamp") is not None
    assert create_resp.get("UpdatedTimestamp") is not None

    # DESCRIBE
    desc_resp = client.get_event_data_store(
        EventDataStore="test-string",
    )
    assert isinstance(desc_resp.get("AdvancedEventSelectors", []), list)
    assert isinstance(desc_resp.get("PartitionKeys", []), list)

    # DELETE
    client.delete_event_data_store(
        EventDataStore="test-string",
    )

    # DESCRIBE after DELETE should fail
    with pytest.raises(ClientError) as exc:
        client.get_event_data_store(
            EventDataStore="test-string",
        )
    assert exc.value.response["Error"]["Code"] in (
        "ResourceNotFoundException",
        "ResourcePolicyNotFoundException",
        "NotFoundException",
        "EntityNotFoundException",
        "InvalidRequestException",
        "NoSuchEntity",
    )


def test_event_data_store_not_found(client):
    """Test that describing a non-existent EventDataStore raises error."""
    with pytest.raises(ClientError) as exc:
        client.get_event_data_store(
            EventDataStore="fake-id",
        )
    assert exc.value.response["Error"]["Code"] in (
        "ResourceNotFoundException",
        "ResourcePolicyNotFoundException",
        "NotFoundException",
        "EntityNotFoundException",
        "InvalidRequestException",
        "NoSuchEntity",
    )


def test_event_selectors_lifecycle(client):
    """Test EventSelectors CRUD lifecycle."""
    # CREATE
    create_resp = client.put_event_selectors(
        TrailName="test-name-1",
    )
    assert isinstance(create_resp.get("EventSelectors", []), list)
    assert isinstance(create_resp.get("AdvancedEventSelectors", []), list)

    # DESCRIBE
    desc_resp = client.get_event_selectors(
        TrailName="test-name-1",
    )
    assert isinstance(desc_resp.get("EventSelectors", []), list)
    assert isinstance(desc_resp.get("AdvancedEventSelectors", []), list)


def test_event_selectors_not_found(client):
    """Test that describing a non-existent EventSelectors raises error."""
    with pytest.raises(ClientError) as exc:
        client.get_event_selectors(
            TrailName="fake-id",
        )
    assert exc.value.response["Error"]["Code"] in (
        "ResourceNotFoundException",
        "ResourcePolicyNotFoundException",
        "NotFoundException",
        "EntityNotFoundException",
        "InvalidRequestException",
        "NoSuchEntity",
    )


def test_import_lifecycle(client):
    """Test Import CRUD lifecycle."""
    # CREATE
    create_resp = client.start_import()
    assert isinstance(create_resp.get("ImportId"), str)
    assert len(create_resp.get("ImportId", "")) > 0
    assert isinstance(create_resp.get("Destinations", []), list)
    assert isinstance(create_resp.get("ImportSource", {}), dict)
    assert create_resp.get("StartEventTime") is not None
    assert create_resp.get("EndEventTime") is not None
    assert create_resp.get("CreatedTimestamp") is not None
    assert create_resp.get("UpdatedTimestamp") is not None

    import_id = create_resp["ImportId"]

    # DESCRIBE
    desc_resp = client.get_import(
        ImportId=import_id,
    )
    assert isinstance(desc_resp.get("ImportId"), str)
    assert len(desc_resp.get("ImportId", "")) > 0
    assert isinstance(desc_resp.get("Destinations", []), list)
    assert isinstance(desc_resp.get("ImportSource", {}), dict)
    assert isinstance(desc_resp.get("ImportStatistics", {}), dict)

    # DELETE
    client.stop_import(
        ImportId=import_id,
    )

    # DESCRIBE after DELETE should fail
    with pytest.raises(ClientError) as exc:
        client.get_import(
            ImportId=import_id,
        )
    assert exc.value.response["Error"]["Code"] in (
        "ResourceNotFoundException",
        "ResourcePolicyNotFoundException",
        "NotFoundException",
        "EntityNotFoundException",
        "InvalidRequestException",
        "NoSuchEntity",
    )


def test_import_not_found(client):
    """Test that describing a non-existent Import raises error."""
    with pytest.raises(ClientError) as exc:
        client.get_import(
            ImportId="fake-id",
        )
    assert exc.value.response["Error"]["Code"] in (
        "ResourceNotFoundException",
        "ResourcePolicyNotFoundException",
        "NotFoundException",
        "EntityNotFoundException",
        "InvalidRequestException",
        "NoSuchEntity",
    )


def test_insight_selectors_lifecycle(client):
    """Test InsightSelectors CRUD lifecycle."""
    # CREATE
    create_resp = client.put_insight_selectors(
        InsightSelectors=[{}],
    )
    assert isinstance(create_resp.get("InsightSelectors", []), list)

    # DESCRIBE
    desc_resp = client.get_insight_selectors()
    assert isinstance(desc_resp.get("InsightSelectors", []), list)


def test_insight_selectors_not_found(client):
    """Test that describing a non-existent InsightSelectors raises error."""
    with pytest.raises(ClientError) as exc:
        client.get_insight_selectors()
    assert exc.value.response["Error"]["Code"] in (
        "ResourceNotFoundException",
        "ResourcePolicyNotFoundException",
        "NotFoundException",
        "EntityNotFoundException",
        "InvalidRequestException",
        "NoSuchEntity",
    )


def test_query_lifecycle(client):
    """Test Query CRUD lifecycle."""
    # CREATE
    client.start_query()

    # DESCRIBE
    desc_resp = client.describe_query()
    assert isinstance(desc_resp.get("QueryStatistics", {}), dict)


def test_query_not_found(client):
    """Test that describing a non-existent Query raises error."""
    with pytest.raises(ClientError) as exc:
        client.describe_query()
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
        ResourceArn="arn:aws:iam::123456789012:role/test-role",
        ResourcePolicy="test-string",
    )
    assert isinstance(create_resp.get("ResourceArn"), str)
    assert create_resp["ResourceArn"].startswith("arn:aws:")

    # DESCRIBE
    desc_resp = client.get_resource_policy(
        ResourceArn="arn:aws:iam::123456789012:role/test-role",
    )
    assert isinstance(desc_resp.get("ResourceArn"), str)
    assert desc_resp["ResourceArn"].startswith("arn:aws:")

    # DELETE
    client.delete_resource_policy(
        ResourceArn="arn:aws:iam::123456789012:role/test-role",
    )

    # DESCRIBE after DELETE should fail
    with pytest.raises(ClientError) as exc:
        client.get_resource_policy(
            ResourceArn="arn:aws:iam::123456789012:role/test-role",
        )
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
        client.get_resource_policy(
            ResourceArn="fake-id",
        )
    assert exc.value.response["Error"]["Code"] in (
        "ResourceNotFoundException",
        "ResourcePolicyNotFoundException",
        "NotFoundException",
        "EntityNotFoundException",
        "InvalidRequestException",
        "NoSuchEntity",
    )


def test_trail_lifecycle(client):
    """Test Trail CRUD lifecycle."""
    # CREATE
    create_resp = client.create_trail(
        Name="test-name-1",
        S3BucketName="test-name-1",
    )
    assert isinstance(create_resp.get("Name"), str)
    assert len(create_resp.get("Name", "")) > 0

    # DESCRIBE
    desc_resp = client.get_trail(
        Name="test-name-1",
    )
    assert isinstance(desc_resp.get("Trail", {}), dict)

    # DELETE
    client.delete_trail(
        Name="test-name-1",
    )

    # DESCRIBE after DELETE should fail
    with pytest.raises(ClientError) as exc:
        client.get_trail(
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


def test_trail_not_found(client):
    """Test that describing a non-existent Trail raises error."""
    with pytest.raises(ClientError) as exc:
        client.get_trail(
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
