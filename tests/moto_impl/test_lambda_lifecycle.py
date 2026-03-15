"""Resource lifecycle tests for lambda (auto-generated)."""

import pytest
from botocore.exceptions import ClientError


@pytest.fixture
def client():
    import boto3

    return boto3.client(
        "lambda",
        endpoint_url="http://localhost:4566",
        region_name="us-east-1",
        aws_access_key_id="testing",
        aws_secret_access_key="testing",
    )


def test_alias_lifecycle(client):
    """Test Alias CRUD lifecycle."""
    # CREATE
    create_resp = client.create_alias(
        FunctionName="test-name-1",
        Name="test-name-1",
        FunctionVersion="test-string",
    )
    assert isinstance(create_resp.get("Name"), str)
    assert len(create_resp.get("Name", "")) > 0
    assert isinstance(create_resp.get("RoutingConfig", {}), dict)

    # DESCRIBE
    desc_resp = client.get_alias(
        FunctionName="test-name-1",
        Name="test-name-1",
    )
    assert isinstance(desc_resp.get("Name"), str)
    assert len(desc_resp.get("Name", "")) > 0
    assert isinstance(desc_resp.get("RoutingConfig", {}), dict)

    # DELETE
    client.delete_alias(
        FunctionName="test-name-1",
        Name="test-name-1",
    )

    # DESCRIBE after DELETE should fail
    with pytest.raises(ClientError) as exc:
        client.get_alias(
            FunctionName="test-name-1",
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


def test_alias_not_found(client):
    """Test that describing a non-existent Alias raises error."""
    with pytest.raises(ClientError) as exc:
        client.get_alias(
            FunctionName="fake-id",
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


def test_capacity_provider_lifecycle(client):
    """Test CapacityProvider CRUD lifecycle."""
    # CREATE
    create_resp = client.create_capacity_provider(
        CapacityProviderName="test-name-1",
        VpcConfig={"SubnetIds": ["test-string"], "SecurityGroupIds": ["test-string"]},
        PermissionsConfig={
            "CapacityProviderOperatorRoleArn": "arn:aws:iam::123456789012:role/test-role"
        },
    )
    assert isinstance(create_resp.get("CapacityProvider", {}), dict)

    # DESCRIBE
    desc_resp = client.get_capacity_provider(
        CapacityProviderName="test-name-1",
    )
    assert isinstance(desc_resp.get("CapacityProvider", {}), dict)

    # DELETE
    client.delete_capacity_provider(
        CapacityProviderName="test-name-1",
    )

    # DESCRIBE after DELETE should fail
    with pytest.raises(ClientError) as exc:
        client.get_capacity_provider(
            CapacityProviderName="test-name-1",
        )
    assert exc.value.response["Error"]["Code"] in (
        "ResourceNotFoundException",
        "ResourcePolicyNotFoundException",
        "NotFoundException",
        "EntityNotFoundException",
        "InvalidRequestException",
        "NoSuchEntity",
    )


def test_capacity_provider_not_found(client):
    """Test that describing a non-existent CapacityProvider raises error."""
    with pytest.raises(ClientError) as exc:
        client.get_capacity_provider(
            CapacityProviderName="fake-id",
        )
    assert exc.value.response["Error"]["Code"] in (
        "ResourceNotFoundException",
        "ResourcePolicyNotFoundException",
        "NotFoundException",
        "EntityNotFoundException",
        "InvalidRequestException",
        "NoSuchEntity",
    )


def test_code_signing_config_lifecycle(client):
    """Test CodeSigningConfig CRUD lifecycle."""
    # CREATE
    create_resp = client.create_code_signing_config(
        AllowedPublishers={"SigningProfileVersionArns": ["test-string"]},
    )
    assert isinstance(create_resp.get("CodeSigningConfig", {}), dict)

    code_signing_config_arn = create_resp["CodeSigningConfig"]["CodeSigningConfigArn"]

    # DESCRIBE
    desc_resp = client.get_code_signing_config(
        CodeSigningConfigArn=code_signing_config_arn,
    )
    assert isinstance(desc_resp.get("CodeSigningConfig", {}), dict)

    # DELETE
    client.delete_code_signing_config(
        CodeSigningConfigArn=code_signing_config_arn,
    )

    # DESCRIBE after DELETE should fail
    with pytest.raises(ClientError) as exc:
        client.get_code_signing_config(
            CodeSigningConfigArn=code_signing_config_arn,
        )
    assert exc.value.response["Error"]["Code"] in (
        "ResourceNotFoundException",
        "ResourcePolicyNotFoundException",
        "NotFoundException",
        "EntityNotFoundException",
        "InvalidRequestException",
        "NoSuchEntity",
    )


def test_code_signing_config_not_found(client):
    """Test that describing a non-existent CodeSigningConfig raises error."""
    with pytest.raises(ClientError) as exc:
        client.get_code_signing_config(
            CodeSigningConfigArn="fake-id",
        )
    assert exc.value.response["Error"]["Code"] in (
        "ResourceNotFoundException",
        "ResourcePolicyNotFoundException",
        "NotFoundException",
        "EntityNotFoundException",
        "InvalidRequestException",
        "NoSuchEntity",
    )


def test_event_source_mapping_lifecycle(client):
    """Test EventSourceMapping CRUD lifecycle."""
    # CREATE
    create_resp = client.create_event_source_mapping(
        FunctionName="test-name-1",
    )
    assert isinstance(create_resp.get("UUID"), str)
    assert len(create_resp.get("UUID", "")) > 0
    assert create_resp.get("StartingPositionTimestamp") is not None
    assert isinstance(create_resp.get("FilterCriteria", {}), dict)
    assert create_resp.get("LastModified") is not None
    assert isinstance(create_resp.get("DestinationConfig", {}), dict)
    assert isinstance(create_resp.get("Topics", []), list)
    assert isinstance(create_resp.get("Queues", []), list)
    assert isinstance(create_resp.get("SourceAccessConfigurations", []), list)
    assert isinstance(create_resp.get("SelfManagedEventSource", {}), dict)
    assert isinstance(create_resp.get("FunctionResponseTypes", []), list)
    assert isinstance(create_resp.get("AmazonManagedKafkaEventSourceConfig", {}), dict)
    assert isinstance(create_resp.get("SelfManagedKafkaEventSourceConfig", {}), dict)
    assert isinstance(create_resp.get("ScalingConfig", {}), dict)
    assert isinstance(create_resp.get("DocumentDBEventSourceConfig", {}), dict)
    assert isinstance(create_resp.get("FilterCriteriaError", {}), dict)
    assert isinstance(create_resp.get("MetricsConfig", {}), dict)
    assert isinstance(create_resp.get("LoggingConfig", {}), dict)
    assert isinstance(create_resp.get("ProvisionedPollerConfig", {}), dict)

    uuid = create_resp["UUID"]

    # DESCRIBE
    desc_resp = client.get_event_source_mapping(
        UUID=uuid,
    )
    assert isinstance(desc_resp.get("UUID"), str)
    assert len(desc_resp.get("UUID", "")) > 0
    assert isinstance(desc_resp.get("FilterCriteria", {}), dict)
    assert isinstance(desc_resp.get("DestinationConfig", {}), dict)
    assert isinstance(desc_resp.get("Topics", []), list)
    assert isinstance(desc_resp.get("Queues", []), list)
    assert isinstance(desc_resp.get("SourceAccessConfigurations", []), list)
    assert isinstance(desc_resp.get("SelfManagedEventSource", {}), dict)
    assert isinstance(desc_resp.get("FunctionResponseTypes", []), list)
    assert isinstance(desc_resp.get("AmazonManagedKafkaEventSourceConfig", {}), dict)
    assert isinstance(desc_resp.get("SelfManagedKafkaEventSourceConfig", {}), dict)
    assert isinstance(desc_resp.get("ScalingConfig", {}), dict)
    assert isinstance(desc_resp.get("DocumentDBEventSourceConfig", {}), dict)
    assert isinstance(desc_resp.get("FilterCriteriaError", {}), dict)
    assert isinstance(desc_resp.get("MetricsConfig", {}), dict)
    assert isinstance(desc_resp.get("LoggingConfig", {}), dict)
    assert isinstance(desc_resp.get("ProvisionedPollerConfig", {}), dict)

    # DELETE
    client.delete_event_source_mapping(
        UUID=uuid,
    )

    # DESCRIBE after DELETE should fail
    with pytest.raises(ClientError) as exc:
        client.get_event_source_mapping(
            UUID=uuid,
        )
    assert exc.value.response["Error"]["Code"] in (
        "ResourceNotFoundException",
        "ResourcePolicyNotFoundException",
        "NotFoundException",
        "EntityNotFoundException",
        "InvalidRequestException",
        "NoSuchEntity",
    )


def test_event_source_mapping_not_found(client):
    """Test that describing a non-existent EventSourceMapping raises error."""
    with pytest.raises(ClientError) as exc:
        client.get_event_source_mapping(
            UUID="fake-id",
        )
    assert exc.value.response["Error"]["Code"] in (
        "ResourceNotFoundException",
        "ResourcePolicyNotFoundException",
        "NotFoundException",
        "EntityNotFoundException",
        "InvalidRequestException",
        "NoSuchEntity",
    )


def test_function_lifecycle(client):
    """Test Function CRUD lifecycle."""
    # CREATE
    create_resp = client.create_function(
        FunctionName="test-name-1",
        Role="test-string",
        Code={},
    )
    assert isinstance(create_resp.get("FunctionName"), str)
    assert len(create_resp.get("FunctionName", "")) > 0
    assert isinstance(create_resp.get("VpcConfig", {}), dict)
    assert isinstance(create_resp.get("DeadLetterConfig", {}), dict)
    assert isinstance(create_resp.get("Environment", {}), dict)
    assert isinstance(create_resp.get("TracingConfig", {}), dict)
    assert isinstance(create_resp.get("Layers", []), list)
    assert isinstance(create_resp.get("FileSystemConfigs", []), list)
    assert isinstance(create_resp.get("ImageConfigResponse", {}), dict)
    assert isinstance(create_resp.get("Architectures", []), list)
    assert isinstance(create_resp.get("EphemeralStorage", {}), dict)
    assert isinstance(create_resp.get("SnapStart", {}), dict)
    assert isinstance(create_resp.get("RuntimeVersionConfig", {}), dict)
    assert isinstance(create_resp.get("LoggingConfig", {}), dict)
    assert isinstance(create_resp.get("CapacityProviderConfig", {}), dict)
    assert isinstance(create_resp.get("DurableConfig", {}), dict)
    assert isinstance(create_resp.get("TenancyConfig", {}), dict)

    # DESCRIBE
    desc_resp = client.get_function(
        FunctionName="test-name-1",
    )
    assert isinstance(desc_resp.get("Configuration", {}), dict)
    assert isinstance(desc_resp.get("Code", {}), dict)
    assert isinstance(desc_resp.get("Tags", {}), dict)
    assert isinstance(desc_resp.get("TagsError", {}), dict)
    assert isinstance(desc_resp.get("Concurrency", {}), dict)

    # DELETE
    client.delete_function(
        FunctionName="test-name-1",
    )

    # DESCRIBE after DELETE should fail
    with pytest.raises(ClientError) as exc:
        client.get_function(
            FunctionName="test-name-1",
        )
    assert exc.value.response["Error"]["Code"] in (
        "ResourceNotFoundException",
        "ResourcePolicyNotFoundException",
        "NotFoundException",
        "EntityNotFoundException",
        "InvalidRequestException",
        "NoSuchEntity",
    )


def test_function_not_found(client):
    """Test that describing a non-existent Function raises error."""
    with pytest.raises(ClientError) as exc:
        client.get_function(
            FunctionName="fake-id",
        )
    assert exc.value.response["Error"]["Code"] in (
        "ResourceNotFoundException",
        "ResourcePolicyNotFoundException",
        "NotFoundException",
        "EntityNotFoundException",
        "InvalidRequestException",
        "NoSuchEntity",
    )


def test_function_code_signing_config_lifecycle(client):
    """Test FunctionCodeSigningConfig CRUD lifecycle."""
    # CREATE
    create_resp = client.put_function_code_signing_config(
        CodeSigningConfigArn="arn:aws:iam::123456789012:role/test-role",
        FunctionName="test-name-1",
    )
    assert isinstance(create_resp.get("CodeSigningConfigArn"), str)
    assert isinstance(create_resp.get("FunctionName"), str)
    assert len(create_resp.get("FunctionName", "")) > 0

    # DESCRIBE
    desc_resp = client.get_function_code_signing_config(
        FunctionName="test-name-1",
    )
    assert isinstance(desc_resp.get("FunctionName"), str)
    assert len(desc_resp.get("FunctionName", "")) > 0

    # DELETE
    client.delete_function_code_signing_config(
        FunctionName="test-name-1",
    )

    # DESCRIBE after DELETE should fail
    with pytest.raises(ClientError) as exc:
        client.get_function_code_signing_config(
            FunctionName="test-name-1",
        )
    assert exc.value.response["Error"]["Code"] in (
        "ResourceNotFoundException",
        "ResourcePolicyNotFoundException",
        "NotFoundException",
        "EntityNotFoundException",
        "InvalidRequestException",
        "NoSuchEntity",
    )


def test_function_code_signing_config_not_found(client):
    """Test that describing a non-existent FunctionCodeSigningConfig raises error."""
    with pytest.raises(ClientError) as exc:
        client.get_function_code_signing_config(
            FunctionName="fake-id",
        )
    assert exc.value.response["Error"]["Code"] in (
        "ResourceNotFoundException",
        "ResourcePolicyNotFoundException",
        "NotFoundException",
        "EntityNotFoundException",
        "InvalidRequestException",
        "NoSuchEntity",
    )


def test_function_concurrency_lifecycle(client):
    """Test FunctionConcurrency CRUD lifecycle."""
    # CREATE
    client.put_function_concurrency(
        FunctionName="test-name-1",
        ReservedConcurrentExecutions=1,
    )

    # DESCRIBE
    client.get_function_concurrency(
        FunctionName="test-name-1",
    )

    # DELETE
    client.delete_function_concurrency(
        FunctionName="test-name-1",
    )

    # DESCRIBE after DELETE should fail
    with pytest.raises(ClientError) as exc:
        client.get_function_concurrency(
            FunctionName="test-name-1",
        )
    assert exc.value.response["Error"]["Code"] in (
        "ResourceNotFoundException",
        "ResourcePolicyNotFoundException",
        "NotFoundException",
        "EntityNotFoundException",
        "InvalidRequestException",
        "NoSuchEntity",
    )


def test_function_concurrency_not_found(client):
    """Test that describing a non-existent FunctionConcurrency raises error."""
    with pytest.raises(ClientError) as exc:
        client.get_function_concurrency(
            FunctionName="fake-id",
        )
    assert exc.value.response["Error"]["Code"] in (
        "ResourceNotFoundException",
        "ResourcePolicyNotFoundException",
        "NotFoundException",
        "EntityNotFoundException",
        "InvalidRequestException",
        "NoSuchEntity",
    )


def test_function_event_invoke_config_lifecycle(client):
    """Test FunctionEventInvokeConfig CRUD lifecycle."""
    # CREATE
    create_resp = client.put_function_event_invoke_config(
        FunctionName="test-name-1",
    )
    assert create_resp.get("LastModified") is not None
    assert isinstance(create_resp.get("DestinationConfig", {}), dict)

    # DESCRIBE
    desc_resp = client.get_function_event_invoke_config(
        FunctionName="test-name-1",
    )
    assert isinstance(desc_resp.get("DestinationConfig", {}), dict)

    # DELETE
    client.delete_function_event_invoke_config(
        FunctionName="test-name-1",
    )

    # DESCRIBE after DELETE should fail
    with pytest.raises(ClientError) as exc:
        client.get_function_event_invoke_config(
            FunctionName="test-name-1",
        )
    assert exc.value.response["Error"]["Code"] in (
        "ResourceNotFoundException",
        "ResourcePolicyNotFoundException",
        "NotFoundException",
        "EntityNotFoundException",
        "InvalidRequestException",
        "NoSuchEntity",
    )


def test_function_event_invoke_config_not_found(client):
    """Test that describing a non-existent FunctionEventInvokeConfig raises error."""
    with pytest.raises(ClientError) as exc:
        client.get_function_event_invoke_config(
            FunctionName="fake-id",
        )
    assert exc.value.response["Error"]["Code"] in (
        "ResourceNotFoundException",
        "ResourcePolicyNotFoundException",
        "NotFoundException",
        "EntityNotFoundException",
        "InvalidRequestException",
        "NoSuchEntity",
    )


def test_function_recursion_config_lifecycle(client):
    """Test FunctionRecursionConfig CRUD lifecycle."""
    # CREATE
    client.put_function_recursion_config(
        FunctionName="test-name-1",
        RecursiveLoop="Allow",
    )

    # DESCRIBE
    client.get_function_recursion_config(
        FunctionName="test-name-1",
    )


def test_function_recursion_config_not_found(client):
    """Test that describing a non-existent FunctionRecursionConfig raises error."""
    with pytest.raises(ClientError) as exc:
        client.get_function_recursion_config(
            FunctionName="fake-id",
        )
    assert exc.value.response["Error"]["Code"] in (
        "ResourceNotFoundException",
        "ResourcePolicyNotFoundException",
        "NotFoundException",
        "EntityNotFoundException",
        "InvalidRequestException",
        "NoSuchEntity",
    )


def test_function_scaling_config_lifecycle(client):
    """Test FunctionScalingConfig CRUD lifecycle."""
    # CREATE
    client.put_function_scaling_config(
        FunctionName="test-name-1",
        Qualifier="test-string",
    )

    # DESCRIBE
    desc_resp = client.get_function_scaling_config(
        FunctionName="test-name-1",
        Qualifier="test-string",
    )
    assert isinstance(desc_resp.get("AppliedFunctionScalingConfig", {}), dict)
    assert isinstance(desc_resp.get("RequestedFunctionScalingConfig", {}), dict)


def test_function_scaling_config_not_found(client):
    """Test that describing a non-existent FunctionScalingConfig raises error."""
    with pytest.raises(ClientError) as exc:
        client.get_function_scaling_config(
            FunctionName="fake-id",
            Qualifier="fake-id",
        )
    assert exc.value.response["Error"]["Code"] in (
        "ResourceNotFoundException",
        "ResourcePolicyNotFoundException",
        "NotFoundException",
        "EntityNotFoundException",
        "InvalidRequestException",
        "NoSuchEntity",
    )


def test_function_url_config_lifecycle(client):
    """Test FunctionUrlConfig CRUD lifecycle."""
    # CREATE
    create_resp = client.create_function_url_config(
        FunctionName="test-name-1",
        AuthType="NONE",
    )
    assert isinstance(create_resp.get("FunctionUrl"), str)
    assert isinstance(create_resp.get("FunctionArn"), str)
    assert isinstance(create_resp.get("AuthType"), str)
    assert isinstance(create_resp.get("Cors", {}), dict)
    assert isinstance(create_resp.get("CreationTime"), str)

    # DESCRIBE
    desc_resp = client.get_function_url_config(
        FunctionName="test-name-1",
    )
    assert isinstance(desc_resp.get("Cors", {}), dict)

    # DELETE
    client.delete_function_url_config(
        FunctionName="test-name-1",
    )

    # DESCRIBE after DELETE should fail
    with pytest.raises(ClientError) as exc:
        client.get_function_url_config(
            FunctionName="test-name-1",
        )
    assert exc.value.response["Error"]["Code"] in (
        "ResourceNotFoundException",
        "ResourcePolicyNotFoundException",
        "NotFoundException",
        "EntityNotFoundException",
        "InvalidRequestException",
        "NoSuchEntity",
    )


def test_function_url_config_not_found(client):
    """Test that describing a non-existent FunctionUrlConfig raises error."""
    with pytest.raises(ClientError) as exc:
        client.get_function_url_config(
            FunctionName="fake-id",
        )
    assert exc.value.response["Error"]["Code"] in (
        "ResourceNotFoundException",
        "ResourcePolicyNotFoundException",
        "NotFoundException",
        "EntityNotFoundException",
        "InvalidRequestException",
        "NoSuchEntity",
    )


def test_provisioned_concurrency_config_lifecycle(client):
    """Test ProvisionedConcurrencyConfig CRUD lifecycle."""
    # CREATE
    client.put_provisioned_concurrency_config(
        FunctionName="test-name-1",
        Qualifier="test-string",
        ProvisionedConcurrentExecutions=1,
    )

    # DESCRIBE
    client.get_provisioned_concurrency_config(
        FunctionName="test-name-1",
        Qualifier="test-string",
    )

    # DELETE
    client.delete_provisioned_concurrency_config(
        FunctionName="test-name-1",
        Qualifier="test-string",
    )

    # DESCRIBE after DELETE should fail
    with pytest.raises(ClientError) as exc:
        client.get_provisioned_concurrency_config(
            FunctionName="test-name-1",
            Qualifier="test-string",
        )
    assert exc.value.response["Error"]["Code"] in (
        "ResourceNotFoundException",
        "ResourcePolicyNotFoundException",
        "NotFoundException",
        "EntityNotFoundException",
        "InvalidRequestException",
        "NoSuchEntity",
    )


def test_provisioned_concurrency_config_not_found(client):
    """Test that describing a non-existent ProvisionedConcurrencyConfig raises error."""
    with pytest.raises(ClientError) as exc:
        client.get_provisioned_concurrency_config(
            FunctionName="fake-id",
            Qualifier="fake-id",
        )
    assert exc.value.response["Error"]["Code"] in (
        "ResourceNotFoundException",
        "ResourcePolicyNotFoundException",
        "NotFoundException",
        "EntityNotFoundException",
        "InvalidRequestException",
        "NoSuchEntity",
    )


def test_runtime_management_config_lifecycle(client):
    """Test RuntimeManagementConfig CRUD lifecycle."""
    # CREATE
    create_resp = client.put_runtime_management_config(
        FunctionName="test-name-1",
        UpdateRuntimeOn="Auto",
    )
    assert isinstance(create_resp.get("UpdateRuntimeOn"), str)
    assert isinstance(create_resp.get("FunctionArn"), str)

    # DESCRIBE
    client.get_runtime_management_config(
        FunctionName="test-name-1",
    )


def test_runtime_management_config_not_found(client):
    """Test that describing a non-existent RuntimeManagementConfig raises error."""
    with pytest.raises(ClientError) as exc:
        client.get_runtime_management_config(
            FunctionName="fake-id",
        )
    assert exc.value.response["Error"]["Code"] in (
        "ResourceNotFoundException",
        "ResourcePolicyNotFoundException",
        "NotFoundException",
        "EntityNotFoundException",
        "InvalidRequestException",
        "NoSuchEntity",
    )
