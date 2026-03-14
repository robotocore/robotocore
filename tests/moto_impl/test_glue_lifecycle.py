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


def test_blueprint_lifecycle(client):
    """Test Blueprint CRUD lifecycle."""
    # CREATE
    create_resp = client.create_blueprint(
        Name="test-name-1",
        BlueprintLocation="test-string",
    )
    assert isinstance(create_resp.get("Name"), str)
    assert len(create_resp.get("Name", "")) > 0

    # DESCRIBE
    desc_resp = client.get_blueprint(
        Name="test-name-1",
    )
    assert isinstance(desc_resp.get("Blueprint", {}), dict)

    # DELETE
    client.delete_blueprint(
        Name="test-name-1",
    )

    # DESCRIBE after DELETE should fail
    with pytest.raises(ClientError) as exc:
        client.get_blueprint(
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


def test_blueprint_not_found(client):
    """Test that describing a non-existent Blueprint raises error."""
    with pytest.raises(ClientError) as exc:
        client.get_blueprint(
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


def test_blueprint_run_lifecycle(client):
    """Test BlueprintRun CRUD lifecycle."""
    # CREATE
    create_resp = client.start_blueprint_run(
        BlueprintName="test-name-1",
        RoleArn="arn:aws:iam::123456789012:role/test-role",
    )
    assert isinstance(create_resp.get("RunId"), str)
    assert len(create_resp.get("RunId", "")) > 0

    run_id = create_resp["RunId"]

    # DESCRIBE
    desc_resp = client.get_blueprint_run(
        BlueprintName="test-name-1",
        RunId=run_id,
    )
    assert isinstance(desc_resp.get("BlueprintRun", {}), dict)


def test_blueprint_run_not_found(client):
    """Test that describing a non-existent BlueprintRun raises error."""
    with pytest.raises(ClientError) as exc:
        client.get_blueprint_run(
            BlueprintName="fake-id",
            RunId="fake-id",
        )
    assert exc.value.response["Error"]["Code"] in (
        "ResourceNotFoundException",
        "ResourcePolicyNotFoundException",
        "NotFoundException",
        "EntityNotFoundException",
        "InvalidRequestException",
        "NoSuchEntity",
    )


def test_catalog_lifecycle(client):
    """Test Catalog CRUD lifecycle."""
    # CREATE
    client.create_catalog(
        Name="test-name-1",
        CatalogInput={},
    )

    # DESCRIBE
    desc_resp = client.get_catalog(
        CatalogId="test-id-1",
    )
    assert isinstance(desc_resp.get("Catalog", {}), dict)

    # DELETE
    client.delete_catalog(
        CatalogId="test-id-1",
    )

    # DESCRIBE after DELETE should fail
    with pytest.raises(ClientError) as exc:
        client.get_catalog(
            CatalogId="test-id-1",
        )
    assert exc.value.response["Error"]["Code"] in (
        "ResourceNotFoundException",
        "ResourcePolicyNotFoundException",
        "NotFoundException",
        "EntityNotFoundException",
        "InvalidRequestException",
        "NoSuchEntity",
    )


def test_catalog_not_found(client):
    """Test that describing a non-existent Catalog raises error."""
    with pytest.raises(ClientError) as exc:
        client.get_catalog(
            CatalogId="fake-id",
        )
    assert exc.value.response["Error"]["Code"] in (
        "ResourceNotFoundException",
        "ResourcePolicyNotFoundException",
        "NotFoundException",
        "EntityNotFoundException",
        "InvalidRequestException",
        "NoSuchEntity",
    )


def test_classifier_lifecycle(client):
    """Test Classifier CRUD lifecycle."""
    # CREATE
    client.create_classifier()

    # DESCRIBE
    desc_resp = client.get_classifier(
        Name="test-name-1",
    )
    assert isinstance(desc_resp.get("Classifier", {}), dict)

    # DELETE
    client.delete_classifier(
        Name="test-name-1",
    )

    # DESCRIBE after DELETE should fail
    with pytest.raises(ClientError) as exc:
        client.get_classifier(
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


def test_classifier_not_found(client):
    """Test that describing a non-existent Classifier raises error."""
    with pytest.raises(ClientError) as exc:
        client.get_classifier(
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


def test_column_statistics_task_run_lifecycle(client):
    """Test ColumnStatisticsTaskRun CRUD lifecycle."""
    # CREATE
    create_resp = client.start_column_statistics_task_run(
        DatabaseName="test-name-1",
        TableName="test-name-1",
        Role="test-string",
    )
    assert isinstance(create_resp.get("ColumnStatisticsTaskRunId"), str)
    assert len(create_resp.get("ColumnStatisticsTaskRunId", "")) > 0

    column_statistics_task_run_id = create_resp["ColumnStatisticsTaskRunId"]

    # DESCRIBE
    desc_resp = client.get_column_statistics_task_run(
        ColumnStatisticsTaskRunId=column_statistics_task_run_id,
    )
    assert isinstance(desc_resp.get("ColumnStatisticsTaskRun", {}), dict)

    # DELETE
    client.stop_column_statistics_task_run(
        DatabaseName="test-name-1",
        TableName="test-name-1",
    )

    # DESCRIBE after DELETE should fail
    with pytest.raises(ClientError) as exc:
        client.get_column_statistics_task_run(
            ColumnStatisticsTaskRunId=column_statistics_task_run_id,
        )
    assert exc.value.response["Error"]["Code"] in (
        "ResourceNotFoundException",
        "ResourcePolicyNotFoundException",
        "NotFoundException",
        "EntityNotFoundException",
        "InvalidRequestException",
        "NoSuchEntity",
    )


def test_column_statistics_task_run_not_found(client):
    """Test that describing a non-existent ColumnStatisticsTaskRun raises error."""
    with pytest.raises(ClientError) as exc:
        client.get_column_statistics_task_run(
            ColumnStatisticsTaskRunId="fake-id",
        )
    assert exc.value.response["Error"]["Code"] in (
        "ResourceNotFoundException",
        "ResourcePolicyNotFoundException",
        "NotFoundException",
        "EntityNotFoundException",
        "InvalidRequestException",
        "NoSuchEntity",
    )


def test_column_statistics_task_settings_lifecycle(client):
    """Test ColumnStatisticsTaskSettings CRUD lifecycle."""
    # CREATE
    client.create_column_statistics_task_settings(
        DatabaseName="test-name-1",
        TableName="test-name-1",
        Role="test-string",
    )

    # DESCRIBE
    desc_resp = client.get_column_statistics_task_settings(
        DatabaseName="test-name-1",
        TableName="test-name-1",
    )
    assert isinstance(desc_resp.get("ColumnStatisticsTaskSettings", {}), dict)

    # DELETE
    client.delete_column_statistics_task_settings(
        DatabaseName="test-name-1",
        TableName="test-name-1",
    )

    # DESCRIBE after DELETE should fail
    with pytest.raises(ClientError) as exc:
        client.get_column_statistics_task_settings(
            DatabaseName="test-name-1",
            TableName="test-name-1",
        )
    assert exc.value.response["Error"]["Code"] in (
        "ResourceNotFoundException",
        "ResourcePolicyNotFoundException",
        "NotFoundException",
        "EntityNotFoundException",
        "InvalidRequestException",
        "NoSuchEntity",
    )


def test_column_statistics_task_settings_not_found(client):
    """Test that describing a non-existent ColumnStatisticsTaskSettings raises error."""
    with pytest.raises(ClientError) as exc:
        client.get_column_statistics_task_settings(
            DatabaseName="fake-id",
            TableName="fake-id",
        )
    assert exc.value.response["Error"]["Code"] in (
        "ResourceNotFoundException",
        "ResourcePolicyNotFoundException",
        "NotFoundException",
        "EntityNotFoundException",
        "InvalidRequestException",
        "NoSuchEntity",
    )


def test_connection_lifecycle(client):
    """Test Connection CRUD lifecycle."""
    # CREATE
    client.create_connection(
        ConnectionInput={
            "Name": "test-name-1",
            "ConnectionType": "JDBC",
            "ConnectionProperties": {},
        },
    )

    # DESCRIBE
    desc_resp = client.get_connection(
        Name="test-name-1",
    )
    assert isinstance(desc_resp.get("Connection", {}), dict)

    # DELETE
    client.delete_connection(
        ConnectionName="test-name-1",
    )

    # DESCRIBE after DELETE should fail
    with pytest.raises(ClientError) as exc:
        client.get_connection(
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


def test_connection_not_found(client):
    """Test that describing a non-existent Connection raises error."""
    with pytest.raises(ClientError) as exc:
        client.get_connection(
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


def test_connection_type_lifecycle(client):
    """Test ConnectionType CRUD lifecycle."""
    # CREATE
    client.register_connection_type(
        ConnectionType="test-string",
        IntegrationType="REST",
        ConnectionProperties={},
        ConnectorAuthenticationConfiguration={"AuthenticationTypes": ["BASIC"]},
        RestConfiguration={},
    )

    # DESCRIBE
    desc_resp = client.describe_connection_type(
        ConnectionType="test-string",
    )
    assert isinstance(desc_resp.get("ConnectionType"), str)
    assert len(desc_resp.get("ConnectionType", "")) > 0
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
        "ResourcePolicyNotFoundException",
        "NotFoundException",
        "EntityNotFoundException",
        "InvalidRequestException",
        "NoSuchEntity",
    )


def test_connection_type_not_found(client):
    """Test that describing a non-existent ConnectionType raises error."""
    with pytest.raises(ClientError) as exc:
        client.describe_connection_type(
            ConnectionType="fake-id",
        )
    assert exc.value.response["Error"]["Code"] in (
        "ResourceNotFoundException",
        "ResourcePolicyNotFoundException",
        "NotFoundException",
        "EntityNotFoundException",
        "InvalidRequestException",
        "NoSuchEntity",
    )


def test_crawler_lifecycle(client):
    """Test Crawler CRUD lifecycle."""
    # CREATE
    client.create_crawler(
        Name="test-name-1",
        Role="test-string",
        Targets={},
    )

    # DESCRIBE
    desc_resp = client.get_crawler(
        Name="test-name-1",
    )
    assert isinstance(desc_resp.get("Crawler", {}), dict)

    # DELETE
    client.delete_crawler(
        Name="test-name-1",
    )

    # DESCRIBE after DELETE should fail
    with pytest.raises(ClientError) as exc:
        client.get_crawler(
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


def test_crawler_not_found(client):
    """Test that describing a non-existent Crawler raises error."""
    with pytest.raises(ClientError) as exc:
        client.get_crawler(
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


def test_custom_entity_type_lifecycle(client):
    """Test CustomEntityType CRUD lifecycle."""
    # CREATE
    create_resp = client.create_custom_entity_type(
        Name="test-name-1",
        RegexString="test-string",
    )
    assert isinstance(create_resp.get("Name"), str)
    assert len(create_resp.get("Name", "")) > 0

    # DESCRIBE
    desc_resp = client.get_custom_entity_type(
        Name="test-name-1",
    )
    assert isinstance(desc_resp.get("Name"), str)
    assert len(desc_resp.get("Name", "")) > 0
    assert isinstance(desc_resp.get("ContextWords", []), list)

    # DELETE
    client.delete_custom_entity_type(
        Name="test-name-1",
    )

    # DESCRIBE after DELETE should fail
    with pytest.raises(ClientError) as exc:
        client.get_custom_entity_type(
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


def test_custom_entity_type_not_found(client):
    """Test that describing a non-existent CustomEntityType raises error."""
    with pytest.raises(ClientError) as exc:
        client.get_custom_entity_type(
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


def test_data_catalog_encryption_settings_lifecycle(client):
    """Test DataCatalogEncryptionSettings CRUD lifecycle."""
    # CREATE
    client.put_data_catalog_encryption_settings(
        DataCatalogEncryptionSettings={},
    )

    # DESCRIBE
    desc_resp = client.get_data_catalog_encryption_settings()
    assert isinstance(desc_resp.get("DataCatalogEncryptionSettings", {}), dict)


def test_data_catalog_encryption_settings_not_found(client):
    """Test that describing a non-existent DataCatalogEncryptionSettings raises error."""
    with pytest.raises(ClientError) as exc:
        client.get_data_catalog_encryption_settings()
    assert exc.value.response["Error"]["Code"] in (
        "ResourceNotFoundException",
        "ResourcePolicyNotFoundException",
        "NotFoundException",
        "EntityNotFoundException",
        "InvalidRequestException",
        "NoSuchEntity",
    )


def test_data_quality_rule_recommendation_run_lifecycle(client):
    """Test DataQualityRuleRecommendationRun CRUD lifecycle."""
    # CREATE
    create_resp = client.start_data_quality_rule_recommendation_run(
        DataSource={},
        Role="test-string",
    )
    assert isinstance(create_resp.get("RunId"), str)
    assert len(create_resp.get("RunId", "")) > 0

    run_id = create_resp["RunId"]

    # DESCRIBE
    desc_resp = client.get_data_quality_rule_recommendation_run(
        RunId=run_id,
    )
    assert isinstance(desc_resp.get("RunId"), str)
    assert len(desc_resp.get("RunId", "")) > 0
    assert isinstance(desc_resp.get("DataSource", {}), dict)


def test_data_quality_rule_recommendation_run_not_found(client):
    """Test that describing a non-existent DataQualityRuleRecommendationRun raises error."""
    with pytest.raises(ClientError) as exc:
        client.get_data_quality_rule_recommendation_run(
            RunId="fake-id",
        )
    assert exc.value.response["Error"]["Code"] in (
        "ResourceNotFoundException",
        "ResourcePolicyNotFoundException",
        "NotFoundException",
        "EntityNotFoundException",
        "InvalidRequestException",
        "NoSuchEntity",
    )


def test_data_quality_ruleset_lifecycle(client):
    """Test DataQualityRuleset CRUD lifecycle."""
    # CREATE
    create_resp = client.create_data_quality_ruleset(
        Name="test-name-1",
        Ruleset="test-string",
    )
    assert isinstance(create_resp.get("Name"), str)
    assert len(create_resp.get("Name", "")) > 0

    # DESCRIBE
    desc_resp = client.get_data_quality_ruleset(
        Name="test-name-1",
    )
    assert isinstance(desc_resp.get("Name"), str)
    assert len(desc_resp.get("Name", "")) > 0
    assert isinstance(desc_resp.get("TargetTable", {}), dict)

    # DELETE
    client.delete_data_quality_ruleset(
        Name="test-name-1",
    )

    # DESCRIBE after DELETE should fail
    with pytest.raises(ClientError) as exc:
        client.get_data_quality_ruleset(
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


def test_data_quality_ruleset_not_found(client):
    """Test that describing a non-existent DataQualityRuleset raises error."""
    with pytest.raises(ClientError) as exc:
        client.get_data_quality_ruleset(
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


def test_data_quality_ruleset_evaluation_run_lifecycle(client):
    """Test DataQualityRulesetEvaluationRun CRUD lifecycle."""
    # CREATE
    create_resp = client.start_data_quality_ruleset_evaluation_run(
        DataSource={},
        Role="test-string",
        RulesetNames=["test-string"],
    )
    assert isinstance(create_resp.get("RunId"), str)
    assert len(create_resp.get("RunId", "")) > 0

    run_id = create_resp["RunId"]

    # DESCRIBE
    desc_resp = client.get_data_quality_ruleset_evaluation_run(
        RunId=run_id,
    )
    assert isinstance(desc_resp.get("RunId"), str)
    assert len(desc_resp.get("RunId", "")) > 0
    assert isinstance(desc_resp.get("DataSource", {}), dict)
    assert isinstance(desc_resp.get("AdditionalRunOptions", {}), dict)
    assert isinstance(desc_resp.get("RulesetNames", []), list)
    assert isinstance(desc_resp.get("ResultIds", []), list)
    assert isinstance(desc_resp.get("AdditionalDataSources", {}), dict)


def test_data_quality_ruleset_evaluation_run_not_found(client):
    """Test that describing a non-existent DataQualityRulesetEvaluationRun raises error."""
    with pytest.raises(ClientError) as exc:
        client.get_data_quality_ruleset_evaluation_run(
            RunId="fake-id",
        )
    assert exc.value.response["Error"]["Code"] in (
        "ResourceNotFoundException",
        "ResourcePolicyNotFoundException",
        "NotFoundException",
        "EntityNotFoundException",
        "InvalidRequestException",
        "NoSuchEntity",
    )


def test_database_lifecycle(client):
    """Test Database CRUD lifecycle."""
    # CREATE
    client.create_database(
        DatabaseInput={"Name": "test-name-1"},
    )

    # DESCRIBE
    desc_resp = client.get_database(
        Name="test-name-1",
    )
    assert isinstance(desc_resp.get("Database", {}), dict)

    # DELETE
    client.delete_database(
        Name="test-name-1",
    )

    # DESCRIBE after DELETE should fail
    with pytest.raises(ClientError) as exc:
        client.get_database(
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


def test_database_not_found(client):
    """Test that describing a non-existent Database raises error."""
    with pytest.raises(ClientError) as exc:
        client.get_database(
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


def test_dev_endpoint_lifecycle(client):
    """Test DevEndpoint CRUD lifecycle."""
    # CREATE
    create_resp = client.create_dev_endpoint(
        EndpointName="test-name-1",
        RoleArn="arn:aws:iam::123456789012:role/test-role",
    )
    assert isinstance(create_resp.get("EndpointName"), str)
    assert len(create_resp.get("EndpointName", "")) > 0
    assert isinstance(create_resp.get("SecurityGroupIds", []), list)
    assert create_resp.get("CreatedTimestamp") is not None
    assert isinstance(create_resp.get("Arguments", {}), dict)

    # DESCRIBE
    desc_resp = client.get_dev_endpoint(
        EndpointName="test-name-1",
    )
    assert isinstance(desc_resp.get("DevEndpoint", {}), dict)

    # DELETE
    client.delete_dev_endpoint(
        EndpointName="test-name-1",
    )

    # DESCRIBE after DELETE should fail
    with pytest.raises(ClientError) as exc:
        client.get_dev_endpoint(
            EndpointName="test-name-1",
        )
    assert exc.value.response["Error"]["Code"] in (
        "ResourceNotFoundException",
        "ResourcePolicyNotFoundException",
        "NotFoundException",
        "EntityNotFoundException",
        "InvalidRequestException",
        "NoSuchEntity",
    )


def test_dev_endpoint_not_found(client):
    """Test that describing a non-existent DevEndpoint raises error."""
    with pytest.raises(ClientError) as exc:
        client.get_dev_endpoint(
            EndpointName="fake-id",
        )
    assert exc.value.response["Error"]["Code"] in (
        "ResourceNotFoundException",
        "ResourcePolicyNotFoundException",
        "NotFoundException",
        "EntityNotFoundException",
        "InvalidRequestException",
        "NoSuchEntity",
    )


def test_glue_identity_center_configuration_lifecycle(client):
    """Test GlueIdentityCenterConfiguration CRUD lifecycle."""
    # CREATE
    client.create_glue_identity_center_configuration(
        InstanceArn="arn:aws:iam::123456789012:role/test-role",
    )

    # DESCRIBE
    desc_resp = client.get_glue_identity_center_configuration()
    assert isinstance(desc_resp.get("Scopes", []), list)

    # DELETE
    client.delete_glue_identity_center_configuration()

    # DESCRIBE after DELETE should fail
    with pytest.raises(ClientError) as exc:
        client.get_glue_identity_center_configuration()
    assert exc.value.response["Error"]["Code"] in (
        "ResourceNotFoundException",
        "ResourcePolicyNotFoundException",
        "NotFoundException",
        "EntityNotFoundException",
        "InvalidRequestException",
        "NoSuchEntity",
    )


def test_glue_identity_center_configuration_not_found(client):
    """Test that describing a non-existent GlueIdentityCenterConfiguration raises error."""
    with pytest.raises(ClientError) as exc:
        client.get_glue_identity_center_configuration()
    assert exc.value.response["Error"]["Code"] in (
        "ResourceNotFoundException",
        "ResourcePolicyNotFoundException",
        "NotFoundException",
        "EntityNotFoundException",
        "InvalidRequestException",
        "NoSuchEntity",
    )


def test_integration_resource_property_lifecycle(client):
    """Test IntegrationResourceProperty CRUD lifecycle."""
    # CREATE
    create_resp = client.create_integration_resource_property(
        ResourceArn="arn:aws:iam::123456789012:role/test-role",
    )
    assert isinstance(create_resp.get("ResourceArn"), str)
    assert create_resp["ResourceArn"].startswith("arn:aws:")
    assert isinstance(create_resp.get("SourceProcessingProperties", {}), dict)
    assert isinstance(create_resp.get("TargetProcessingProperties", {}), dict)

    # DESCRIBE
    desc_resp = client.get_integration_resource_property(
        ResourceArn="arn:aws:iam::123456789012:role/test-role",
    )
    assert isinstance(desc_resp.get("ResourceArn"), str)
    assert desc_resp["ResourceArn"].startswith("arn:aws:")
    assert isinstance(desc_resp.get("SourceProcessingProperties", {}), dict)
    assert isinstance(desc_resp.get("TargetProcessingProperties", {}), dict)

    # DELETE
    client.delete_integration_resource_property(
        ResourceArn="arn:aws:iam::123456789012:role/test-role",
    )

    # DESCRIBE after DELETE should fail
    with pytest.raises(ClientError) as exc:
        client.get_integration_resource_property(
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


def test_integration_resource_property_not_found(client):
    """Test that describing a non-existent IntegrationResourceProperty raises error."""
    with pytest.raises(ClientError) as exc:
        client.get_integration_resource_property(
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


def test_integration_table_properties_lifecycle(client):
    """Test IntegrationTableProperties CRUD lifecycle."""
    # CREATE
    client.create_integration_table_properties(
        ResourceArn="arn:aws:iam::123456789012:role/test-role",
        TableName="test-name-1",
    )

    # DESCRIBE
    desc_resp = client.get_integration_table_properties(
        ResourceArn="arn:aws:iam::123456789012:role/test-role",
        TableName="test-name-1",
    )
    assert isinstance(desc_resp.get("ResourceArn"), str)
    assert desc_resp["ResourceArn"].startswith("arn:aws:")
    assert isinstance(desc_resp.get("TableName"), str)
    assert len(desc_resp.get("TableName", "")) > 0
    assert isinstance(desc_resp.get("SourceTableConfig", {}), dict)
    assert isinstance(desc_resp.get("TargetTableConfig", {}), dict)

    # DELETE
    client.delete_integration_table_properties(
        ResourceArn="arn:aws:iam::123456789012:role/test-role",
        TableName="test-name-1",
    )

    # DESCRIBE after DELETE should fail
    with pytest.raises(ClientError) as exc:
        client.get_integration_table_properties(
            ResourceArn="arn:aws:iam::123456789012:role/test-role",
            TableName="test-name-1",
        )
    assert exc.value.response["Error"]["Code"] in (
        "ResourceNotFoundException",
        "ResourcePolicyNotFoundException",
        "NotFoundException",
        "EntityNotFoundException",
        "InvalidRequestException",
        "NoSuchEntity",
    )


def test_integration_table_properties_not_found(client):
    """Test that describing a non-existent IntegrationTableProperties raises error."""
    with pytest.raises(ClientError) as exc:
        client.get_integration_table_properties(
            ResourceArn="fake-id",
            TableName="fake-id",
        )
    assert exc.value.response["Error"]["Code"] in (
        "ResourceNotFoundException",
        "ResourcePolicyNotFoundException",
        "NotFoundException",
        "EntityNotFoundException",
        "InvalidRequestException",
        "NoSuchEntity",
    )


def test_job_lifecycle(client):
    """Test Job CRUD lifecycle."""
    # CREATE
    client.create_job(
        Name="test-name-1",
        Role="test-string",
        Command={},
    )

    # DESCRIBE
    desc_resp = client.get_job(
        JobName="test-name-1",
    )
    assert isinstance(desc_resp.get("Job", {}), dict)

    # DELETE
    client.delete_job(
        JobName="test-name-1",
    )

    # DESCRIBE after DELETE should fail
    with pytest.raises(ClientError) as exc:
        client.get_job(
            JobName="test-name-1",
        )
    assert exc.value.response["Error"]["Code"] in (
        "ResourceNotFoundException",
        "ResourcePolicyNotFoundException",
        "NotFoundException",
        "EntityNotFoundException",
        "InvalidRequestException",
        "NoSuchEntity",
    )


def test_job_not_found(client):
    """Test that describing a non-existent Job raises error."""
    with pytest.raises(ClientError) as exc:
        client.get_job(
            JobName="fake-id",
        )
    assert exc.value.response["Error"]["Code"] in (
        "ResourceNotFoundException",
        "ResourcePolicyNotFoundException",
        "NotFoundException",
        "EntityNotFoundException",
        "InvalidRequestException",
        "NoSuchEntity",
    )


def test_job_run_lifecycle(client):
    """Test JobRun CRUD lifecycle."""
    # CREATE
    client.start_job_run(
        JobName="test-name-1",
    )

    # DESCRIBE
    desc_resp = client.get_job_run(
        JobName="test-name-1",
        RunId="test-id-1",
    )
    assert isinstance(desc_resp.get("JobRun", {}), dict)


def test_job_run_not_found(client):
    """Test that describing a non-existent JobRun raises error."""
    with pytest.raises(ClientError) as exc:
        client.get_job_run(
            JobName="fake-id",
            RunId="fake-id",
        )
    assert exc.value.response["Error"]["Code"] in (
        "ResourceNotFoundException",
        "ResourcePolicyNotFoundException",
        "NotFoundException",
        "EntityNotFoundException",
        "InvalidRequestException",
        "NoSuchEntity",
    )


def test_ml_transform_lifecycle(client):
    """Test MLTransform CRUD lifecycle."""
    # CREATE
    create_resp = client.create_ml_transform(
        Name="test-name-1",
        InputRecordTables=[{"DatabaseName": "test-name-1", "TableName": "test-name-1"}],
        Parameters={"TransformType": "FIND_MATCHES"},
        Role="test-string",
    )
    assert isinstance(create_resp.get("TransformId"), str)
    assert len(create_resp.get("TransformId", "")) > 0

    transform_id = create_resp["TransformId"]

    # DESCRIBE
    desc_resp = client.get_ml_transform(
        TransformId=transform_id,
    )
    assert isinstance(desc_resp.get("TransformId"), str)
    assert len(desc_resp.get("TransformId", "")) > 0
    assert isinstance(desc_resp.get("InputRecordTables", []), list)
    assert isinstance(desc_resp.get("Parameters", {}), dict)
    assert isinstance(desc_resp.get("EvaluationMetrics", {}), dict)
    assert isinstance(desc_resp.get("Schema", []), list)
    assert isinstance(desc_resp.get("TransformEncryption", {}), dict)

    # DELETE
    client.delete_ml_transform(
        TransformId=transform_id,
    )

    # DESCRIBE after DELETE should fail
    with pytest.raises(ClientError) as exc:
        client.get_ml_transform(
            TransformId=transform_id,
        )
    assert exc.value.response["Error"]["Code"] in (
        "ResourceNotFoundException",
        "ResourcePolicyNotFoundException",
        "NotFoundException",
        "EntityNotFoundException",
        "InvalidRequestException",
        "NoSuchEntity",
    )


def test_ml_transform_not_found(client):
    """Test that describing a non-existent MLTransform raises error."""
    with pytest.raises(ClientError) as exc:
        client.get_ml_transform(
            TransformId="fake-id",
        )
    assert exc.value.response["Error"]["Code"] in (
        "ResourceNotFoundException",
        "ResourcePolicyNotFoundException",
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

    materialized_view_refresh_task_run_id = create_resp["MaterializedViewRefreshTaskRunId"]

    # DESCRIBE
    desc_resp = client.get_materialized_view_refresh_task_run(
        CatalogId="test-id-1",
        MaterializedViewRefreshTaskRunId=materialized_view_refresh_task_run_id,
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
            MaterializedViewRefreshTaskRunId=materialized_view_refresh_task_run_id,
        )
    assert exc.value.response["Error"]["Code"] in (
        "ResourceNotFoundException",
        "ResourcePolicyNotFoundException",
        "NotFoundException",
        "EntityNotFoundException",
        "InvalidRequestException",
        "NoSuchEntity",
    )


def test_materialized_view_refresh_task_run_not_found(client):
    """Test that describing a non-existent MaterializedViewRefreshTaskRun raises error."""
    with pytest.raises(ClientError) as exc:
        client.get_materialized_view_refresh_task_run(
            CatalogId="fake-id",
            MaterializedViewRefreshTaskRunId="fake-id",
        )
    assert exc.value.response["Error"]["Code"] in (
        "ResourceNotFoundException",
        "ResourcePolicyNotFoundException",
        "NotFoundException",
        "EntityNotFoundException",
        "InvalidRequestException",
        "NoSuchEntity",
    )


def test_partition_lifecycle(client):
    """Test Partition CRUD lifecycle."""
    # CREATE
    client.create_partition(
        DatabaseName="test-name-1",
        TableName="test-name-1",
        PartitionInput={},
    )

    # DESCRIBE
    desc_resp = client.get_partition(
        DatabaseName="test-name-1",
        TableName="test-name-1",
        PartitionValues=["test-string"],
    )
    assert isinstance(desc_resp.get("Partition", {}), dict)

    # DELETE
    client.delete_partition(
        DatabaseName="test-name-1",
        TableName="test-name-1",
        PartitionValues=["test-string"],
    )

    # DESCRIBE after DELETE should fail
    with pytest.raises(ClientError) as exc:
        client.get_partition(
            DatabaseName="test-name-1",
            TableName="test-name-1",
            PartitionValues=["test-string"],
        )
    assert exc.value.response["Error"]["Code"] in (
        "ResourceNotFoundException",
        "ResourcePolicyNotFoundException",
        "NotFoundException",
        "EntityNotFoundException",
        "InvalidRequestException",
        "NoSuchEntity",
    )


def test_partition_not_found(client):
    """Test that describing a non-existent Partition raises error."""
    with pytest.raises(ClientError) as exc:
        client.get_partition(
            DatabaseName="fake-id",
            TableName="fake-id",
            PartitionValues="fake-id",
        )
    assert exc.value.response["Error"]["Code"] in (
        "ResourceNotFoundException",
        "ResourcePolicyNotFoundException",
        "NotFoundException",
        "EntityNotFoundException",
        "InvalidRequestException",
        "NoSuchEntity",
    )


def test_registry_lifecycle(client):
    """Test Registry CRUD lifecycle."""
    # CREATE
    create_resp = client.create_registry(
        RegistryName="test-name-1",
    )
    assert isinstance(create_resp.get("Tags", {}), dict)

    # DESCRIBE
    client.get_registry(
        RegistryId={},
    )

    # DELETE
    client.delete_registry(
        RegistryId={},
    )

    # DESCRIBE after DELETE should fail
    with pytest.raises(ClientError) as exc:
        client.get_registry(
            RegistryId={},
        )
    assert exc.value.response["Error"]["Code"] in (
        "ResourceNotFoundException",
        "ResourcePolicyNotFoundException",
        "NotFoundException",
        "EntityNotFoundException",
        "InvalidRequestException",
        "NoSuchEntity",
    )


def test_registry_not_found(client):
    """Test that describing a non-existent Registry raises error."""
    with pytest.raises(ClientError) as exc:
        client.get_registry(
            RegistryId="fake-id",
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
    client.put_resource_policy(
        PolicyInJson="test-string",
    )

    # DESCRIBE
    client.get_resource_policy()

    # DELETE
    client.delete_resource_policy()

    # DESCRIBE after DELETE should fail
    with pytest.raises(ClientError) as exc:
        client.get_resource_policy()
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
        client.get_resource_policy()
    assert exc.value.response["Error"]["Code"] in (
        "ResourceNotFoundException",
        "ResourcePolicyNotFoundException",
        "NotFoundException",
        "EntityNotFoundException",
        "InvalidRequestException",
        "NoSuchEntity",
    )


def test_schema_lifecycle(client):
    """Test Schema CRUD lifecycle."""
    # CREATE
    create_resp = client.create_schema(
        SchemaName="test-name-1",
        DataFormat="AVRO",
    )
    assert isinstance(create_resp.get("Tags", {}), dict)

    # DESCRIBE
    client.get_schema(
        SchemaId={},
    )

    # DELETE
    client.delete_schema(
        SchemaId={},
    )

    # DESCRIBE after DELETE should fail
    with pytest.raises(ClientError) as exc:
        client.get_schema(
            SchemaId={},
        )
    assert exc.value.response["Error"]["Code"] in (
        "ResourceNotFoundException",
        "ResourcePolicyNotFoundException",
        "NotFoundException",
        "EntityNotFoundException",
        "InvalidRequestException",
        "NoSuchEntity",
    )


def test_schema_not_found(client):
    """Test that describing a non-existent Schema raises error."""
    with pytest.raises(ClientError) as exc:
        client.get_schema(
            SchemaId="fake-id",
        )
    assert exc.value.response["Error"]["Code"] in (
        "ResourceNotFoundException",
        "ResourcePolicyNotFoundException",
        "NotFoundException",
        "EntityNotFoundException",
        "InvalidRequestException",
        "NoSuchEntity",
    )


def test_schema_version_lifecycle(client):
    """Test SchemaVersion CRUD lifecycle."""
    # CREATE
    client.register_schema_version(
        SchemaId={},
        SchemaDefinition="test-string",
    )

    # DESCRIBE
    client.get_schema_version()


def test_schema_version_not_found(client):
    """Test that describing a non-existent SchemaVersion raises error."""
    with pytest.raises(ClientError) as exc:
        client.get_schema_version()
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
        EncryptionConfiguration={},
    )
    assert isinstance(create_resp.get("Name"), str)
    assert len(create_resp.get("Name", "")) > 0
    assert create_resp.get("CreatedTimestamp") is not None

    # DESCRIBE
    desc_resp = client.get_security_configuration(
        Name="test-name-1",
    )
    assert isinstance(desc_resp.get("SecurityConfiguration", {}), dict)

    # DELETE
    client.delete_security_configuration(
        Name="test-name-1",
    )

    # DESCRIBE after DELETE should fail
    with pytest.raises(ClientError) as exc:
        client.get_security_configuration(
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
        client.get_security_configuration(
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


def test_session_lifecycle(client):
    """Test Session CRUD lifecycle."""
    # CREATE
    create_resp = client.create_session(
        Id="test-id-1",
        Role="test-string",
        Command={},
    )
    assert isinstance(create_resp.get("Session", {}), dict)

    # DESCRIBE
    desc_resp = client.get_session(
        Id="test-id-1",
    )
    assert isinstance(desc_resp.get("Session", {}), dict)

    # DELETE
    client.delete_session(
        Id="test-id-1",
    )

    # DESCRIBE after DELETE should fail
    with pytest.raises(ClientError) as exc:
        client.get_session(
            Id="test-id-1",
        )
    assert exc.value.response["Error"]["Code"] in (
        "ResourceNotFoundException",
        "ResourcePolicyNotFoundException",
        "NotFoundException",
        "EntityNotFoundException",
        "InvalidRequestException",
        "NoSuchEntity",
    )


def test_session_not_found(client):
    """Test that describing a non-existent Session raises error."""
    with pytest.raises(ClientError) as exc:
        client.get_session(
            Id="fake-id",
        )
    assert exc.value.response["Error"]["Code"] in (
        "ResourceNotFoundException",
        "ResourcePolicyNotFoundException",
        "NotFoundException",
        "EntityNotFoundException",
        "InvalidRequestException",
        "NoSuchEntity",
    )


def test_statement_lifecycle(client):
    """Test Statement CRUD lifecycle."""
    # CREATE
    create_resp = client.run_statement(
        SessionId="test-id-1",
        Code="test-string",
    )
    assert isinstance(create_resp.get("Id"), int)

    id = create_resp["Id"]

    # DESCRIBE
    desc_resp = client.get_statement(
        SessionId="test-id-1",
        Id=id,
    )
    assert isinstance(desc_resp.get("Statement", {}), dict)


def test_statement_not_found(client):
    """Test that describing a non-existent Statement raises error."""
    with pytest.raises(ClientError) as exc:
        client.get_statement(
            SessionId="fake-id",
            Id=99999,
        )
    assert exc.value.response["Error"]["Code"] in (
        "ResourceNotFoundException",
        "ResourcePolicyNotFoundException",
        "NotFoundException",
        "EntityNotFoundException",
        "InvalidRequestException",
        "NoSuchEntity",
    )


def test_table_lifecycle(client):
    """Test Table CRUD lifecycle."""
    # CREATE
    client.create_table(
        DatabaseName="test-name-1",
    )

    # DESCRIBE
    desc_resp = client.get_table(
        DatabaseName="test-name-1",
        Name="test-name-1",
    )
    assert isinstance(desc_resp.get("Table", {}), dict)

    # DELETE
    client.delete_table(
        DatabaseName="test-name-1",
        Name="test-name-1",
    )

    # DESCRIBE after DELETE should fail
    with pytest.raises(ClientError) as exc:
        client.get_table(
            DatabaseName="test-name-1",
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


def test_table_not_found(client):
    """Test that describing a non-existent Table raises error."""
    with pytest.raises(ClientError) as exc:
        client.get_table(
            DatabaseName="fake-id",
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


def test_table_optimizer_lifecycle(client):
    """Test TableOptimizer CRUD lifecycle."""
    # CREATE
    client.create_table_optimizer(
        CatalogId="test-id-1",
        DatabaseName="test-name-1",
        TableName="test-name-1",
        Type="compaction",
        TableOptimizerConfiguration={},
    )

    # DESCRIBE
    desc_resp = client.get_table_optimizer(
        CatalogId="test-id-1",
        DatabaseName="test-name-1",
        TableName="test-name-1",
        Type="compaction",
    )
    assert isinstance(desc_resp.get("CatalogId"), str)
    assert len(desc_resp.get("CatalogId", "")) > 0
    assert isinstance(desc_resp.get("DatabaseName"), str)
    assert len(desc_resp.get("DatabaseName", "")) > 0
    assert isinstance(desc_resp.get("TableName"), str)
    assert len(desc_resp.get("TableName", "")) > 0
    assert isinstance(desc_resp.get("TableOptimizer", {}), dict)

    # DELETE
    client.delete_table_optimizer(
        CatalogId="test-id-1",
        DatabaseName="test-name-1",
        TableName="test-name-1",
        Type="compaction",
    )

    # DESCRIBE after DELETE should fail
    with pytest.raises(ClientError) as exc:
        client.get_table_optimizer(
            CatalogId="test-id-1",
            DatabaseName="test-name-1",
            TableName="test-name-1",
            Type="compaction",
        )
    assert exc.value.response["Error"]["Code"] in (
        "ResourceNotFoundException",
        "ResourcePolicyNotFoundException",
        "NotFoundException",
        "EntityNotFoundException",
        "InvalidRequestException",
        "NoSuchEntity",
    )


def test_table_optimizer_not_found(client):
    """Test that describing a non-existent TableOptimizer raises error."""
    with pytest.raises(ClientError) as exc:
        client.get_table_optimizer(
            CatalogId="fake-id",
            DatabaseName="fake-id",
            TableName="fake-id",
            Type="fake-id",
        )
    assert exc.value.response["Error"]["Code"] in (
        "ResourceNotFoundException",
        "ResourcePolicyNotFoundException",
        "NotFoundException",
        "EntityNotFoundException",
        "InvalidRequestException",
        "NoSuchEntity",
    )


def test_trigger_lifecycle(client):
    """Test Trigger CRUD lifecycle."""
    # CREATE
    create_resp = client.create_trigger(
        Name="test-name-1",
        Type="SCHEDULED",
        Actions=[{}],
    )
    assert isinstance(create_resp.get("Name"), str)
    assert len(create_resp.get("Name", "")) > 0

    # DESCRIBE
    desc_resp = client.get_trigger(
        Name="test-name-1",
    )
    assert isinstance(desc_resp.get("Trigger", {}), dict)

    # DELETE
    client.delete_trigger(
        Name="test-name-1",
    )

    # DESCRIBE after DELETE should fail
    with pytest.raises(ClientError) as exc:
        client.get_trigger(
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


def test_trigger_not_found(client):
    """Test that describing a non-existent Trigger raises error."""
    with pytest.raises(ClientError) as exc:
        client.get_trigger(
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


def test_usage_profile_lifecycle(client):
    """Test UsageProfile CRUD lifecycle."""
    # CREATE
    create_resp = client.create_usage_profile(
        Name="test-name-1",
        Configuration={},
    )
    assert isinstance(create_resp.get("Name"), str)
    assert len(create_resp.get("Name", "")) > 0

    # DESCRIBE
    desc_resp = client.get_usage_profile(
        Name="test-name-1",
    )
    assert isinstance(desc_resp.get("Name"), str)
    assert len(desc_resp.get("Name", "")) > 0
    assert isinstance(desc_resp.get("Configuration", {}), dict)

    # DELETE
    client.delete_usage_profile(
        Name="test-name-1",
    )

    # DESCRIBE after DELETE should fail
    with pytest.raises(ClientError) as exc:
        client.get_usage_profile(
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


def test_usage_profile_not_found(client):
    """Test that describing a non-existent UsageProfile raises error."""
    with pytest.raises(ClientError) as exc:
        client.get_usage_profile(
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


def test_user_defined_function_lifecycle(client):
    """Test UserDefinedFunction CRUD lifecycle."""
    # CREATE
    client.create_user_defined_function(
        DatabaseName="test-name-1",
        FunctionInput={},
    )

    # DESCRIBE
    desc_resp = client.get_user_defined_function(
        DatabaseName="test-name-1",
        FunctionName="test-name-1",
    )
    assert isinstance(desc_resp.get("UserDefinedFunction", {}), dict)

    # DELETE
    client.delete_user_defined_function(
        DatabaseName="test-name-1",
        FunctionName="test-name-1",
    )

    # DESCRIBE after DELETE should fail
    with pytest.raises(ClientError) as exc:
        client.get_user_defined_function(
            DatabaseName="test-name-1",
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


def test_user_defined_function_not_found(client):
    """Test that describing a non-existent UserDefinedFunction raises error."""
    with pytest.raises(ClientError) as exc:
        client.get_user_defined_function(
            DatabaseName="fake-id",
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


def test_workflow_lifecycle(client):
    """Test Workflow CRUD lifecycle."""
    # CREATE
    create_resp = client.create_workflow(
        Name="test-name-1",
    )
    assert isinstance(create_resp.get("Name"), str)
    assert len(create_resp.get("Name", "")) > 0

    # DESCRIBE
    desc_resp = client.get_workflow(
        Name="test-name-1",
    )
    assert isinstance(desc_resp.get("Workflow", {}), dict)

    # DELETE
    client.delete_workflow(
        Name="test-name-1",
    )

    # DESCRIBE after DELETE should fail
    with pytest.raises(ClientError) as exc:
        client.get_workflow(
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


def test_workflow_not_found(client):
    """Test that describing a non-existent Workflow raises error."""
    with pytest.raises(ClientError) as exc:
        client.get_workflow(
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


def test_workflow_run_lifecycle(client):
    """Test WorkflowRun CRUD lifecycle."""
    # CREATE
    create_resp = client.start_workflow_run(
        Name="test-name-1",
    )
    assert isinstance(create_resp.get("RunId"), str)
    assert len(create_resp.get("RunId", "")) > 0

    run_id = create_resp["RunId"]

    # DESCRIBE
    desc_resp = client.get_workflow_run(
        Name="test-name-1",
        RunId=run_id,
    )
    assert isinstance(desc_resp.get("Run", {}), dict)

    # DELETE
    client.stop_workflow_run(
        Name="test-name-1",
        RunId=run_id,
    )

    # DESCRIBE after DELETE should fail
    with pytest.raises(ClientError) as exc:
        client.get_workflow_run(
            Name="test-name-1",
            RunId=run_id,
        )
    assert exc.value.response["Error"]["Code"] in (
        "ResourceNotFoundException",
        "ResourcePolicyNotFoundException",
        "NotFoundException",
        "EntityNotFoundException",
        "InvalidRequestException",
        "NoSuchEntity",
    )


def test_workflow_run_not_found(client):
    """Test that describing a non-existent WorkflowRun raises error."""
    with pytest.raises(ClientError) as exc:
        client.get_workflow_run(
            Name="fake-id",
            RunId="fake-id",
        )
    assert exc.value.response["Error"]["Code"] in (
        "ResourceNotFoundException",
        "ResourcePolicyNotFoundException",
        "NotFoundException",
        "EntityNotFoundException",
        "InvalidRequestException",
        "NoSuchEntity",
    )


def test_workflow_run_properties_lifecycle(client):
    """Test WorkflowRunProperties CRUD lifecycle."""
    # CREATE
    client.put_workflow_run_properties(
        Name="test-name-1",
        RunId="test-id-1",
        RunProperties={},
    )

    # DESCRIBE
    desc_resp = client.get_workflow_run_properties(
        Name="test-name-1",
        RunId="test-id-1",
    )
    assert isinstance(desc_resp.get("RunProperties", {}), dict)


def test_workflow_run_properties_not_found(client):
    """Test that describing a non-existent WorkflowRunProperties raises error."""
    with pytest.raises(ClientError) as exc:
        client.get_workflow_run_properties(
            Name="fake-id",
            RunId="fake-id",
        )
    assert exc.value.response["Error"]["Code"] in (
        "ResourceNotFoundException",
        "ResourcePolicyNotFoundException",
        "NotFoundException",
        "EntityNotFoundException",
        "InvalidRequestException",
        "NoSuchEntity",
    )
