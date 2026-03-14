"""Resource lifecycle tests for connect (auto-generated)."""

import pytest
from botocore.exceptions import ClientError


@pytest.fixture
def client():
    import boto3

    return boto3.client(
        "connect",
        endpoint_url="http://localhost:4566",
        region_name="us-east-1",
        aws_access_key_id="testing",
        aws_secret_access_key="testing",
    )


@pytest.fixture
def instance_id(client):
    resp = client.create_instance(
        IdentityManagementType="CONNECT_MANAGED",
        InboundCallsEnabled=True,
        OutboundCallsEnabled=True,
    )
    iid = resp["Id"]
    yield iid
    try:
        client.delete_instance(InstanceId=iid)
    except Exception:
        pass


def test_agent_status_lifecycle(client, instance_id):
    """Test AgentStatus CRUD lifecycle."""
    # CREATE
    create_resp = client.create_agent_status(
        InstanceId=instance_id,
        Name="test-name-1",
        State="ENABLED",
    )
    assert isinstance(create_resp.get("AgentStatusId"), str)
    assert len(create_resp.get("AgentStatusId", "")) > 0

    agent_status_id = create_resp["AgentStatusId"]

    # DESCRIBE
    desc_resp = client.describe_agent_status(
        InstanceId=instance_id,
        AgentStatusId=agent_status_id,
    )
    assert isinstance(desc_resp.get("AgentStatus", {}), dict)


def test_agent_status_not_found(client, instance_id):
    """Test that describing a non-existent AgentStatus raises error."""
    with pytest.raises(ClientError) as exc:
        client.describe_agent_status(
            InstanceId=instance_id,
            AgentStatusId="fake-id",
        )
    assert exc.value.response["Error"]["Code"] in (
        "ResourceNotFoundException",
        "NotFoundException",
        "EntityNotFoundException",
        "InvalidRequestException",
        "NoSuchEntity",
    )


def test_contact_lifecycle(client, instance_id):
    """Test Contact CRUD lifecycle."""
    # CREATE
    create_resp = client.create_contact(
        InstanceId=instance_id,
        Channel="VOICE",
        InitiationMethod="INBOUND",
    )
    assert isinstance(create_resp.get("ContactId"), str)
    assert len(create_resp.get("ContactId", "")) > 0

    contact_id = create_resp["ContactId"]

    # DESCRIBE
    desc_resp = client.describe_contact(
        InstanceId=instance_id,
        ContactId=contact_id,
    )
    assert isinstance(desc_resp.get("Contact", {}), dict)

    # DELETE
    client.stop_contact(
        ContactId=contact_id,
        InstanceId=instance_id,
    )

    # DESCRIBE after DELETE should fail
    with pytest.raises(ClientError) as exc:
        client.describe_contact(
            InstanceId=instance_id,
            ContactId=contact_id,
        )
    assert exc.value.response["Error"]["Code"] in (
        "ResourceNotFoundException",
        "NotFoundException",
        "EntityNotFoundException",
        "InvalidRequestException",
        "NoSuchEntity",
    )


def test_contact_not_found(client, instance_id):
    """Test that describing a non-existent Contact raises error."""
    with pytest.raises(ClientError) as exc:
        client.describe_contact(
            InstanceId=instance_id,
            ContactId="fake-id",
        )
    assert exc.value.response["Error"]["Code"] in (
        "ResourceNotFoundException",
        "NotFoundException",
        "EntityNotFoundException",
        "InvalidRequestException",
        "NoSuchEntity",
    )


def test_contact_evaluation_lifecycle(client, instance_id):
    """Test ContactEvaluation CRUD lifecycle."""
    # CREATE
    create_resp = client.start_contact_evaluation(
        InstanceId=instance_id,
        ContactId="test-id-1",
        EvaluationFormId="test-id-1",
    )
    assert isinstance(create_resp.get("EvaluationId"), str)
    assert len(create_resp.get("EvaluationId", "")) > 0
    assert isinstance(create_resp.get("EvaluationArn"), str)

    evaluation_id = create_resp["EvaluationId"]

    # DESCRIBE
    desc_resp = client.describe_contact_evaluation(
        InstanceId=instance_id,
        EvaluationId=evaluation_id,
    )
    assert isinstance(desc_resp.get("Evaluation", {}), dict)
    assert isinstance(desc_resp.get("EvaluationForm", {}), dict)

    # DELETE
    client.delete_contact_evaluation(
        InstanceId=instance_id,
        EvaluationId=evaluation_id,
    )

    # DESCRIBE after DELETE should fail
    with pytest.raises(ClientError) as exc:
        client.describe_contact_evaluation(
            InstanceId=instance_id,
            EvaluationId=evaluation_id,
        )
    assert exc.value.response["Error"]["Code"] in (
        "ResourceNotFoundException",
        "NotFoundException",
        "EntityNotFoundException",
        "InvalidRequestException",
        "NoSuchEntity",
    )


def test_contact_evaluation_not_found(client, instance_id):
    """Test that describing a non-existent ContactEvaluation raises error."""
    with pytest.raises(ClientError) as exc:
        client.describe_contact_evaluation(
            InstanceId=instance_id,
            EvaluationId="fake-id",
        )
    assert exc.value.response["Error"]["Code"] in (
        "ResourceNotFoundException",
        "NotFoundException",
        "EntityNotFoundException",
        "InvalidRequestException",
        "NoSuchEntity",
    )


def test_contact_flow_lifecycle(client, instance_id):
    """Test ContactFlow CRUD lifecycle."""
    # CREATE
    create_resp = client.create_contact_flow(
        InstanceId=instance_id,
        Name="test-name-1",
        Type="CONTACT_FLOW",
        Content="test-string",
    )
    assert isinstance(create_resp.get("ContactFlowId"), str)
    assert len(create_resp.get("ContactFlowId", "")) > 0

    contact_flow_id = create_resp["ContactFlowId"]

    # DESCRIBE
    desc_resp = client.describe_contact_flow(
        InstanceId=instance_id,
        ContactFlowId=contact_flow_id,
    )
    assert isinstance(desc_resp.get("ContactFlow", {}), dict)

    # DELETE
    client.delete_contact_flow(
        InstanceId=instance_id,
        ContactFlowId=contact_flow_id,
    )

    # DESCRIBE after DELETE should fail
    with pytest.raises(ClientError) as exc:
        client.describe_contact_flow(
            InstanceId=instance_id,
            ContactFlowId=contact_flow_id,
        )
    assert exc.value.response["Error"]["Code"] in (
        "ResourceNotFoundException",
        "NotFoundException",
        "EntityNotFoundException",
        "InvalidRequestException",
        "NoSuchEntity",
    )


def test_contact_flow_not_found(client, instance_id):
    """Test that describing a non-existent ContactFlow raises error."""
    with pytest.raises(ClientError) as exc:
        client.describe_contact_flow(
            InstanceId=instance_id,
            ContactFlowId="fake-id",
        )
    assert exc.value.response["Error"]["Code"] in (
        "ResourceNotFoundException",
        "NotFoundException",
        "EntityNotFoundException",
        "InvalidRequestException",
        "NoSuchEntity",
    )


def test_contact_flow_module_lifecycle(client, instance_id):
    """Test ContactFlowModule CRUD lifecycle."""
    # CREATE
    client.create_contact_flow_module(
        InstanceId=instance_id,
        Name="test-name-1",
        Content="test-string",
    )

    # DESCRIBE
    desc_resp = client.describe_contact_flow_module(
        InstanceId=instance_id,
        ContactFlowModuleId="test-id-1",
    )
    assert isinstance(desc_resp.get("ContactFlowModule", {}), dict)

    # DELETE
    client.delete_contact_flow_module(
        InstanceId=instance_id,
        ContactFlowModuleId="test-id-1",
    )

    # DESCRIBE after DELETE should fail
    with pytest.raises(ClientError) as exc:
        client.describe_contact_flow_module(
            InstanceId=instance_id,
            ContactFlowModuleId="test-id-1",
        )
    assert exc.value.response["Error"]["Code"] in (
        "ResourceNotFoundException",
        "NotFoundException",
        "EntityNotFoundException",
        "InvalidRequestException",
        "NoSuchEntity",
    )


def test_contact_flow_module_not_found(client, instance_id):
    """Test that describing a non-existent ContactFlowModule raises error."""
    with pytest.raises(ClientError) as exc:
        client.describe_contact_flow_module(
            InstanceId=instance_id,
            ContactFlowModuleId="fake-id",
        )
    assert exc.value.response["Error"]["Code"] in (
        "ResourceNotFoundException",
        "NotFoundException",
        "EntityNotFoundException",
        "InvalidRequestException",
        "NoSuchEntity",
    )


def test_contact_flow_module_alias_lifecycle(client, instance_id):
    """Test ContactFlowModuleAlias CRUD lifecycle."""
    # CREATE
    client.create_contact_flow_module_alias(
        InstanceId=instance_id,
        ContactFlowModuleId="test-id-1",
        ContactFlowModuleVersion=1,
        AliasName="test-name-1",
    )

    # DESCRIBE
    desc_resp = client.describe_contact_flow_module_alias(
        InstanceId=instance_id,
        ContactFlowModuleId="test-id-1",
        AliasId="test-id-1",
    )
    assert isinstance(desc_resp.get("ContactFlowModuleAlias", {}), dict)

    # DELETE
    client.delete_contact_flow_module_alias(
        InstanceId=instance_id,
        ContactFlowModuleId="test-id-1",
        AliasId="test-id-1",
    )

    # DESCRIBE after DELETE should fail
    with pytest.raises(ClientError) as exc:
        client.describe_contact_flow_module_alias(
            InstanceId=instance_id,
            ContactFlowModuleId="test-id-1",
            AliasId="test-id-1",
        )
    assert exc.value.response["Error"]["Code"] in (
        "ResourceNotFoundException",
        "NotFoundException",
        "EntityNotFoundException",
        "InvalidRequestException",
        "NoSuchEntity",
    )


def test_contact_flow_module_alias_not_found(client, instance_id):
    """Test that describing a non-existent ContactFlowModuleAlias raises error."""
    with pytest.raises(ClientError) as exc:
        client.describe_contact_flow_module_alias(
            InstanceId=instance_id,
            ContactFlowModuleId="fake-id",
            AliasId="fake-id",
        )
    assert exc.value.response["Error"]["Code"] in (
        "ResourceNotFoundException",
        "NotFoundException",
        "EntityNotFoundException",
        "InvalidRequestException",
        "NoSuchEntity",
    )


def test_data_table_lifecycle(client, instance_id):
    """Test DataTable CRUD lifecycle."""
    # CREATE
    create_resp = client.create_data_table(
        InstanceId=instance_id,
        Name="test-name-1",
        TimeZone="test-string",
        ValueLockLevel="NONE",
        Status="PUBLISHED",
    )
    assert isinstance(create_resp.get("Id"), str)
    assert isinstance(create_resp.get("Arn"), str)
    assert isinstance(create_resp.get("LockVersion", {}), dict)

    # DESCRIBE
    desc_resp = client.describe_data_table(
        InstanceId=instance_id,
        DataTableId="test-id-1",
    )
    assert isinstance(desc_resp.get("DataTable", {}), dict)

    # DELETE
    client.delete_data_table(
        InstanceId=instance_id,
        DataTableId="test-id-1",
    )

    # DESCRIBE after DELETE should fail
    with pytest.raises(ClientError) as exc:
        client.describe_data_table(
            InstanceId=instance_id,
            DataTableId="test-id-1",
        )
    assert exc.value.response["Error"]["Code"] in (
        "ResourceNotFoundException",
        "NotFoundException",
        "EntityNotFoundException",
        "InvalidRequestException",
        "NoSuchEntity",
    )


def test_data_table_not_found(client, instance_id):
    """Test that describing a non-existent DataTable raises error."""
    with pytest.raises(ClientError) as exc:
        client.describe_data_table(
            InstanceId=instance_id,
            DataTableId="fake-id",
        )
    assert exc.value.response["Error"]["Code"] in (
        "ResourceNotFoundException",
        "NotFoundException",
        "EntityNotFoundException",
        "InvalidRequestException",
        "NoSuchEntity",
    )


def test_data_table_attribute_lifecycle(client, instance_id):
    """Test DataTableAttribute CRUD lifecycle."""
    # CREATE
    create_resp = client.create_data_table_attribute(
        InstanceId=instance_id,
        DataTableId="test-id-1",
        Name="test-name-1",
        ValueType="TEXT",
    )
    assert isinstance(create_resp.get("Name"), str)
    assert isinstance(create_resp.get("LockVersion", {}), dict)

    # DESCRIBE
    desc_resp = client.describe_data_table_attribute(
        InstanceId=instance_id,
        DataTableId="test-id-1",
        AttributeName="test-name-1",
    )
    assert isinstance(desc_resp.get("Attribute", {}), dict)

    # DELETE
    client.delete_data_table_attribute(
        InstanceId=instance_id,
        DataTableId="test-id-1",
        AttributeName="test-name-1",
    )

    # DESCRIBE after DELETE should fail
    with pytest.raises(ClientError) as exc:
        client.describe_data_table_attribute(
            InstanceId=instance_id,
            DataTableId="test-id-1",
            AttributeName="test-name-1",
        )
    assert exc.value.response["Error"]["Code"] in (
        "ResourceNotFoundException",
        "NotFoundException",
        "EntityNotFoundException",
        "InvalidRequestException",
        "NoSuchEntity",
    )


def test_data_table_attribute_not_found(client, instance_id):
    """Test that describing a non-existent DataTableAttribute raises error."""
    with pytest.raises(ClientError) as exc:
        client.describe_data_table_attribute(
            InstanceId=instance_id,
            DataTableId="fake-id",
            AttributeName="fake-id",
        )
    assert exc.value.response["Error"]["Code"] in (
        "ResourceNotFoundException",
        "NotFoundException",
        "EntityNotFoundException",
        "InvalidRequestException",
        "NoSuchEntity",
    )


def test_email_address_lifecycle(client, instance_id):
    """Test EmailAddress CRUD lifecycle."""
    # CREATE
    create_resp = client.create_email_address(
        InstanceId=instance_id,
        EmailAddress="test@example.com",
    )
    assert isinstance(create_resp.get("EmailAddressId"), str)
    assert len(create_resp.get("EmailAddressId", "")) > 0

    email_address_id = create_resp["EmailAddressId"]

    # DESCRIBE
    desc_resp = client.describe_email_address(
        InstanceId=instance_id,
        EmailAddressId=email_address_id,
    )
    assert isinstance(desc_resp.get("EmailAddressId"), str)
    assert len(desc_resp.get("EmailAddressId", "")) > 0
    assert isinstance(desc_resp.get("AliasConfigurations", []), list)
    assert isinstance(desc_resp.get("Tags", {}), dict)

    # DELETE
    client.delete_email_address(
        InstanceId=instance_id,
        EmailAddressId=email_address_id,
    )

    # DESCRIBE after DELETE should fail
    with pytest.raises(ClientError) as exc:
        client.describe_email_address(
            InstanceId=instance_id,
            EmailAddressId=email_address_id,
        )
    assert exc.value.response["Error"]["Code"] in (
        "ResourceNotFoundException",
        "NotFoundException",
        "EntityNotFoundException",
        "InvalidRequestException",
        "NoSuchEntity",
    )


def test_email_address_not_found(client, instance_id):
    """Test that describing a non-existent EmailAddress raises error."""
    with pytest.raises(ClientError) as exc:
        client.describe_email_address(
            InstanceId=instance_id,
            EmailAddressId="fake-id",
        )
    assert exc.value.response["Error"]["Code"] in (
        "ResourceNotFoundException",
        "NotFoundException",
        "EntityNotFoundException",
        "InvalidRequestException",
        "NoSuchEntity",
    )


def test_evaluation_form_lifecycle(client, instance_id):
    """Test EvaluationForm CRUD lifecycle."""
    # CREATE
    create_resp = client.create_evaluation_form(
        InstanceId=instance_id,
        Title="test-string",
        Items=[{}],
    )
    assert isinstance(create_resp.get("EvaluationFormId"), str)
    assert len(create_resp.get("EvaluationFormId", "")) > 0
    assert isinstance(create_resp.get("EvaluationFormArn"), str)

    evaluation_form_id = create_resp["EvaluationFormId"]

    # DESCRIBE
    desc_resp = client.describe_evaluation_form(
        InstanceId=instance_id,
        EvaluationFormId=evaluation_form_id,
    )
    assert isinstance(desc_resp.get("EvaluationForm", {}), dict)

    # DELETE
    client.delete_evaluation_form(
        InstanceId=instance_id,
        EvaluationFormId=evaluation_form_id,
    )

    # DESCRIBE after DELETE should fail
    with pytest.raises(ClientError) as exc:
        client.describe_evaluation_form(
            InstanceId=instance_id,
            EvaluationFormId=evaluation_form_id,
        )
    assert exc.value.response["Error"]["Code"] in (
        "ResourceNotFoundException",
        "NotFoundException",
        "EntityNotFoundException",
        "InvalidRequestException",
        "NoSuchEntity",
    )


def test_evaluation_form_not_found(client, instance_id):
    """Test that describing a non-existent EvaluationForm raises error."""
    with pytest.raises(ClientError) as exc:
        client.describe_evaluation_form(
            InstanceId=instance_id,
            EvaluationFormId="fake-id",
        )
    assert exc.value.response["Error"]["Code"] in (
        "ResourceNotFoundException",
        "NotFoundException",
        "EntityNotFoundException",
        "InvalidRequestException",
        "NoSuchEntity",
    )


def test_hours_of_operation_lifecycle(client, instance_id):
    """Test HoursOfOperation CRUD lifecycle."""
    # CREATE
    create_resp = client.create_hours_of_operation(
        InstanceId=instance_id,
        Name="test-name-1",
        TimeZone="test-string",
        Config=[
            {
                "Day": "SUNDAY",
                "StartTime": {"Hours": 1, "Minutes": 1},
                "EndTime": {"Hours": 1, "Minutes": 1},
            }
        ],
    )
    assert isinstance(create_resp.get("HoursOfOperationId"), str)
    assert len(create_resp.get("HoursOfOperationId", "")) > 0

    hours_of_operation_id = create_resp["HoursOfOperationId"]

    # DESCRIBE
    desc_resp = client.describe_hours_of_operation(
        InstanceId=instance_id,
        HoursOfOperationId=hours_of_operation_id,
    )
    assert isinstance(desc_resp.get("HoursOfOperation", {}), dict)

    # DELETE
    client.delete_hours_of_operation(
        InstanceId=instance_id,
        HoursOfOperationId=hours_of_operation_id,
    )

    # DESCRIBE after DELETE should fail
    with pytest.raises(ClientError) as exc:
        client.describe_hours_of_operation(
            InstanceId=instance_id,
            HoursOfOperationId=hours_of_operation_id,
        )
    assert exc.value.response["Error"]["Code"] in (
        "ResourceNotFoundException",
        "NotFoundException",
        "EntityNotFoundException",
        "InvalidRequestException",
        "NoSuchEntity",
    )


def test_hours_of_operation_not_found(client, instance_id):
    """Test that describing a non-existent HoursOfOperation raises error."""
    with pytest.raises(ClientError) as exc:
        client.describe_hours_of_operation(
            InstanceId=instance_id,
            HoursOfOperationId="fake-id",
        )
    assert exc.value.response["Error"]["Code"] in (
        "ResourceNotFoundException",
        "NotFoundException",
        "EntityNotFoundException",
        "InvalidRequestException",
        "NoSuchEntity",
    )


def test_hours_of_operation_override_lifecycle(client, instance_id):
    """Test HoursOfOperationOverride CRUD lifecycle."""
    # CREATE
    create_resp = client.create_hours_of_operation_override(
        InstanceId=instance_id,
        HoursOfOperationId="test-id-1",
        Name="test-name-1",
        Config=[{}],
        EffectiveFrom="test-string",
        EffectiveTill="test-string",
    )
    assert isinstance(create_resp.get("HoursOfOperationOverrideId"), str)
    assert len(create_resp.get("HoursOfOperationOverrideId", "")) > 0

    hours_of_operation_override_id = create_resp["HoursOfOperationOverrideId"]

    # DESCRIBE
    desc_resp = client.describe_hours_of_operation_override(
        InstanceId=instance_id,
        HoursOfOperationId="test-id-1",
        HoursOfOperationOverrideId=hours_of_operation_override_id,
    )
    assert isinstance(desc_resp.get("HoursOfOperationOverride", {}), dict)

    # DELETE
    client.delete_hours_of_operation_override(
        InstanceId=instance_id,
        HoursOfOperationId="test-id-1",
        HoursOfOperationOverrideId=hours_of_operation_override_id,
    )

    # DESCRIBE after DELETE should fail
    with pytest.raises(ClientError) as exc:
        client.describe_hours_of_operation_override(
            InstanceId=instance_id,
            HoursOfOperationId="test-id-1",
            HoursOfOperationOverrideId=hours_of_operation_override_id,
        )
    assert exc.value.response["Error"]["Code"] in (
        "ResourceNotFoundException",
        "NotFoundException",
        "EntityNotFoundException",
        "InvalidRequestException",
        "NoSuchEntity",
    )


def test_hours_of_operation_override_not_found(client, instance_id):
    """Test that describing a non-existent HoursOfOperationOverride raises error."""
    with pytest.raises(ClientError) as exc:
        client.describe_hours_of_operation_override(
            InstanceId=instance_id,
            HoursOfOperationId="fake-id",
            HoursOfOperationOverrideId="fake-id",
        )
    assert exc.value.response["Error"]["Code"] in (
        "ResourceNotFoundException",
        "NotFoundException",
        "EntityNotFoundException",
        "InvalidRequestException",
        "NoSuchEntity",
    )


def test_instance_lifecycle(client, instance_id):
    """Test Instance CRUD lifecycle."""
    # CREATE
    client.create_instance(
        IdentityManagementType="SAML",
        InboundCallsEnabled=True,
        OutboundCallsEnabled=True,
    )

    # DESCRIBE
    desc_resp = client.describe_instance(
        InstanceId=instance_id,
    )
    assert isinstance(desc_resp.get("Instance", {}), dict)
    assert isinstance(desc_resp.get("ReplicationConfiguration", {}), dict)

    # DELETE
    client.delete_instance(
        InstanceId=instance_id,
    )

    # DESCRIBE after DELETE should fail
    with pytest.raises(ClientError) as exc:
        client.describe_instance(
            InstanceId=instance_id,
        )
    assert exc.value.response["Error"]["Code"] in (
        "ResourceNotFoundException",
        "NotFoundException",
        "EntityNotFoundException",
        "InvalidRequestException",
        "NoSuchEntity",
    )


def test_instance_not_found(client, instance_id):
    """Test that describing a non-existent Instance raises error."""
    with pytest.raises(ClientError) as exc:
        client.describe_instance(
            InstanceId=instance_id,
        )
    assert exc.value.response["Error"]["Code"] in (
        "ResourceNotFoundException",
        "NotFoundException",
        "EntityNotFoundException",
        "InvalidRequestException",
        "NoSuchEntity",
    )


def test_notification_lifecycle(client, instance_id):
    """Test Notification CRUD lifecycle."""
    # CREATE
    create_resp = client.create_notification(
        InstanceId=instance_id,
        Recipients=["test-string"],
        Content={},
    )
    assert isinstance(create_resp.get("NotificationId"), str)
    assert len(create_resp.get("NotificationId", "")) > 0
    assert isinstance(create_resp.get("NotificationArn"), str)

    notification_id = create_resp["NotificationId"]

    # DESCRIBE
    desc_resp = client.describe_notification(
        InstanceId=instance_id,
        NotificationId=notification_id,
    )
    assert isinstance(desc_resp.get("Notification", {}), dict)

    # DELETE
    client.delete_notification(
        InstanceId=instance_id,
        NotificationId=notification_id,
    )

    # DESCRIBE after DELETE should fail
    with pytest.raises(ClientError) as exc:
        client.describe_notification(
            InstanceId=instance_id,
            NotificationId=notification_id,
        )
    assert exc.value.response["Error"]["Code"] in (
        "ResourceNotFoundException",
        "NotFoundException",
        "EntityNotFoundException",
        "InvalidRequestException",
        "NoSuchEntity",
    )


def test_notification_not_found(client, instance_id):
    """Test that describing a non-existent Notification raises error."""
    with pytest.raises(ClientError) as exc:
        client.describe_notification(
            InstanceId=instance_id,
            NotificationId="fake-id",
        )
    assert exc.value.response["Error"]["Code"] in (
        "ResourceNotFoundException",
        "NotFoundException",
        "EntityNotFoundException",
        "InvalidRequestException",
        "NoSuchEntity",
    )


def test_predefined_attribute_lifecycle(client, instance_id):
    """Test PredefinedAttribute CRUD lifecycle."""
    # CREATE
    client.create_predefined_attribute(
        InstanceId=instance_id,
        Name="test-name-1",
    )

    # DESCRIBE
    desc_resp = client.describe_predefined_attribute(
        InstanceId=instance_id,
        Name="test-name-1",
    )
    assert isinstance(desc_resp.get("PredefinedAttribute", {}), dict)

    # DELETE
    client.delete_predefined_attribute(
        InstanceId=instance_id,
        Name="test-name-1",
    )

    # DESCRIBE after DELETE should fail
    with pytest.raises(ClientError) as exc:
        client.describe_predefined_attribute(
            InstanceId=instance_id,
            Name="test-name-1",
        )
    assert exc.value.response["Error"]["Code"] in (
        "ResourceNotFoundException",
        "NotFoundException",
        "EntityNotFoundException",
        "InvalidRequestException",
        "NoSuchEntity",
    )


def test_predefined_attribute_not_found(client, instance_id):
    """Test that describing a non-existent PredefinedAttribute raises error."""
    with pytest.raises(ClientError) as exc:
        client.describe_predefined_attribute(
            InstanceId=instance_id,
            Name="fake-id",
        )
    assert exc.value.response["Error"]["Code"] in (
        "ResourceNotFoundException",
        "NotFoundException",
        "EntityNotFoundException",
        "InvalidRequestException",
        "NoSuchEntity",
    )


def test_prompt_lifecycle(client, instance_id):
    """Test Prompt CRUD lifecycle."""
    # CREATE
    create_resp = client.create_prompt(
        InstanceId=instance_id,
        Name="test-name-1",
        S3Uri="test-string",
    )
    assert isinstance(create_resp.get("PromptId"), str)
    assert len(create_resp.get("PromptId", "")) > 0

    prompt_id = create_resp["PromptId"]

    # DESCRIBE
    desc_resp = client.describe_prompt(
        InstanceId=instance_id,
        PromptId=prompt_id,
    )
    assert isinstance(desc_resp.get("Prompt", {}), dict)

    # DELETE
    client.delete_prompt(
        InstanceId=instance_id,
        PromptId=prompt_id,
    )

    # DESCRIBE after DELETE should fail
    with pytest.raises(ClientError) as exc:
        client.describe_prompt(
            InstanceId=instance_id,
            PromptId=prompt_id,
        )
    assert exc.value.response["Error"]["Code"] in (
        "ResourceNotFoundException",
        "NotFoundException",
        "EntityNotFoundException",
        "InvalidRequestException",
        "NoSuchEntity",
    )


def test_prompt_not_found(client, instance_id):
    """Test that describing a non-existent Prompt raises error."""
    with pytest.raises(ClientError) as exc:
        client.describe_prompt(
            InstanceId=instance_id,
            PromptId="fake-id",
        )
    assert exc.value.response["Error"]["Code"] in (
        "ResourceNotFoundException",
        "NotFoundException",
        "EntityNotFoundException",
        "InvalidRequestException",
        "NoSuchEntity",
    )


def test_queue_lifecycle(client, instance_id):
    """Test Queue CRUD lifecycle."""
    # CREATE
    create_resp = client.create_queue(
        InstanceId=instance_id,
        Name="test-name-1",
        HoursOfOperationId="test-id-1",
    )
    assert isinstance(create_resp.get("QueueId"), str)
    assert len(create_resp.get("QueueId", "")) > 0

    queue_id = create_resp["QueueId"]

    # DESCRIBE
    desc_resp = client.describe_queue(
        InstanceId=instance_id,
        QueueId=queue_id,
    )
    assert isinstance(desc_resp.get("Queue", {}), dict)

    # DELETE
    client.delete_queue(
        InstanceId=instance_id,
        QueueId=queue_id,
    )

    # DESCRIBE after DELETE should fail
    with pytest.raises(ClientError) as exc:
        client.describe_queue(
            InstanceId=instance_id,
            QueueId=queue_id,
        )
    assert exc.value.response["Error"]["Code"] in (
        "ResourceNotFoundException",
        "NotFoundException",
        "EntityNotFoundException",
        "InvalidRequestException",
        "NoSuchEntity",
    )


def test_queue_not_found(client, instance_id):
    """Test that describing a non-existent Queue raises error."""
    with pytest.raises(ClientError) as exc:
        client.describe_queue(
            InstanceId=instance_id,
            QueueId="fake-id",
        )
    assert exc.value.response["Error"]["Code"] in (
        "ResourceNotFoundException",
        "NotFoundException",
        "EntityNotFoundException",
        "InvalidRequestException",
        "NoSuchEntity",
    )


def test_quick_connect_lifecycle(client, instance_id):
    """Test QuickConnect CRUD lifecycle."""
    # CREATE
    create_resp = client.create_quick_connect(
        InstanceId=instance_id,
        Name="test-name-1",
        QuickConnectConfig={"QuickConnectType": "USER"},
    )
    assert isinstance(create_resp.get("QuickConnectId"), str)
    assert len(create_resp.get("QuickConnectId", "")) > 0

    quick_connect_id = create_resp["QuickConnectId"]

    # DESCRIBE
    desc_resp = client.describe_quick_connect(
        InstanceId=instance_id,
        QuickConnectId=quick_connect_id,
    )
    assert isinstance(desc_resp.get("QuickConnect", {}), dict)

    # DELETE
    client.delete_quick_connect(
        InstanceId=instance_id,
        QuickConnectId=quick_connect_id,
    )

    # DESCRIBE after DELETE should fail
    with pytest.raises(ClientError) as exc:
        client.describe_quick_connect(
            InstanceId=instance_id,
            QuickConnectId=quick_connect_id,
        )
    assert exc.value.response["Error"]["Code"] in (
        "ResourceNotFoundException",
        "NotFoundException",
        "EntityNotFoundException",
        "InvalidRequestException",
        "NoSuchEntity",
    )


def test_quick_connect_not_found(client, instance_id):
    """Test that describing a non-existent QuickConnect raises error."""
    with pytest.raises(ClientError) as exc:
        client.describe_quick_connect(
            InstanceId=instance_id,
            QuickConnectId="fake-id",
        )
    assert exc.value.response["Error"]["Code"] in (
        "ResourceNotFoundException",
        "NotFoundException",
        "EntityNotFoundException",
        "InvalidRequestException",
        "NoSuchEntity",
    )


def test_routing_profile_lifecycle(client, instance_id):
    """Test RoutingProfile CRUD lifecycle."""
    # CREATE
    create_resp = client.create_routing_profile(
        InstanceId=instance_id,
        Name="test-name-1",
        Description="test-string",
        DefaultOutboundQueueId="test-id-1",
        MediaConcurrencies=[{"Channel": "VOICE", "Concurrency": 1}],
    )
    assert isinstance(create_resp.get("RoutingProfileId"), str)
    assert len(create_resp.get("RoutingProfileId", "")) > 0

    routing_profile_id = create_resp["RoutingProfileId"]

    # DESCRIBE
    desc_resp = client.describe_routing_profile(
        InstanceId=instance_id,
        RoutingProfileId=routing_profile_id,
    )
    assert isinstance(desc_resp.get("RoutingProfile", {}), dict)

    # DELETE
    client.delete_routing_profile(
        InstanceId=instance_id,
        RoutingProfileId=routing_profile_id,
    )

    # DESCRIBE after DELETE should fail
    with pytest.raises(ClientError) as exc:
        client.describe_routing_profile(
            InstanceId=instance_id,
            RoutingProfileId=routing_profile_id,
        )
    assert exc.value.response["Error"]["Code"] in (
        "ResourceNotFoundException",
        "NotFoundException",
        "EntityNotFoundException",
        "InvalidRequestException",
        "NoSuchEntity",
    )


def test_routing_profile_not_found(client, instance_id):
    """Test that describing a non-existent RoutingProfile raises error."""
    with pytest.raises(ClientError) as exc:
        client.describe_routing_profile(
            InstanceId=instance_id,
            RoutingProfileId="fake-id",
        )
    assert exc.value.response["Error"]["Code"] in (
        "ResourceNotFoundException",
        "NotFoundException",
        "EntityNotFoundException",
        "InvalidRequestException",
        "NoSuchEntity",
    )


def test_rule_lifecycle(client, instance_id):
    """Test Rule CRUD lifecycle."""
    # CREATE
    create_resp = client.create_rule(
        InstanceId=instance_id,
        Name="test-name-1",
        TriggerEventSource={"EventSourceName": "OnPostCallAnalysisAvailable"},
        Function="test-string",
        Actions=[{"ActionType": "CREATE_TASK"}],
        PublishStatus="DRAFT",
    )
    assert isinstance(create_resp.get("RuleArn"), str)
    assert isinstance(create_resp.get("RuleId"), str)
    assert len(create_resp.get("RuleId", "")) > 0

    rule_id = create_resp["RuleId"]

    # DESCRIBE
    desc_resp = client.describe_rule(
        InstanceId=instance_id,
        RuleId=rule_id,
    )
    assert isinstance(desc_resp.get("Rule", {}), dict)

    # DELETE
    client.delete_rule(
        InstanceId=instance_id,
        RuleId=rule_id,
    )

    # DESCRIBE after DELETE should fail
    with pytest.raises(ClientError) as exc:
        client.describe_rule(
            InstanceId=instance_id,
            RuleId=rule_id,
        )
    assert exc.value.response["Error"]["Code"] in (
        "ResourceNotFoundException",
        "NotFoundException",
        "EntityNotFoundException",
        "InvalidRequestException",
        "NoSuchEntity",
    )


def test_rule_not_found(client, instance_id):
    """Test that describing a non-existent Rule raises error."""
    with pytest.raises(ClientError) as exc:
        client.describe_rule(
            InstanceId=instance_id,
            RuleId="fake-id",
        )
    assert exc.value.response["Error"]["Code"] in (
        "ResourceNotFoundException",
        "NotFoundException",
        "EntityNotFoundException",
        "InvalidRequestException",
        "NoSuchEntity",
    )


def test_security_profile_lifecycle(client, instance_id):
    """Test SecurityProfile CRUD lifecycle."""
    # CREATE
    create_resp = client.create_security_profile(
        SecurityProfileName="test-name-1",
        InstanceId=instance_id,
    )
    assert isinstance(create_resp.get("SecurityProfileId"), str)
    assert len(create_resp.get("SecurityProfileId", "")) > 0

    security_profile_id = create_resp["SecurityProfileId"]

    # DESCRIBE
    desc_resp = client.describe_security_profile(
        SecurityProfileId=security_profile_id,
        InstanceId=instance_id,
    )
    assert isinstance(desc_resp.get("SecurityProfile", {}), dict)

    # DELETE
    client.delete_security_profile(
        InstanceId=instance_id,
        SecurityProfileId=security_profile_id,
    )

    # DESCRIBE after DELETE should fail
    with pytest.raises(ClientError) as exc:
        client.describe_security_profile(
            SecurityProfileId=security_profile_id,
            InstanceId=instance_id,
        )
    assert exc.value.response["Error"]["Code"] in (
        "ResourceNotFoundException",
        "NotFoundException",
        "EntityNotFoundException",
        "InvalidRequestException",
        "NoSuchEntity",
    )


def test_security_profile_not_found(client, instance_id):
    """Test that describing a non-existent SecurityProfile raises error."""
    with pytest.raises(ClientError) as exc:
        client.describe_security_profile(
            SecurityProfileId="fake-id",
            InstanceId=instance_id,
        )
    assert exc.value.response["Error"]["Code"] in (
        "ResourceNotFoundException",
        "NotFoundException",
        "EntityNotFoundException",
        "InvalidRequestException",
        "NoSuchEntity",
    )


def test_task_template_lifecycle(client, instance_id):
    """Test TaskTemplate CRUD lifecycle."""
    # CREATE
    create_resp = client.create_task_template(
        InstanceId=instance_id,
        Name="test-name-1",
        Fields=[{"Id": {}}],
    )
    assert isinstance(create_resp.get("Id"), str)
    assert isinstance(create_resp.get("Arn"), str)

    # DESCRIBE
    desc_resp = client.get_task_template(
        InstanceId=instance_id,
        TaskTemplateId="test-id-1",
    )
    assert isinstance(desc_resp.get("InstanceId"), str)
    assert len(desc_resp.get("InstanceId", "")) > 0
    assert isinstance(desc_resp.get("Constraints", {}), dict)
    assert isinstance(desc_resp.get("Defaults", {}), dict)
    assert isinstance(desc_resp.get("Fields", []), list)
    assert isinstance(desc_resp.get("Tags", {}), dict)

    # DELETE
    client.delete_task_template(
        InstanceId=instance_id,
        TaskTemplateId="test-id-1",
    )

    # DESCRIBE after DELETE should fail
    with pytest.raises(ClientError) as exc:
        client.get_task_template(
            InstanceId=instance_id,
            TaskTemplateId="test-id-1",
        )
    assert exc.value.response["Error"]["Code"] in (
        "ResourceNotFoundException",
        "NotFoundException",
        "EntityNotFoundException",
        "InvalidRequestException",
        "NoSuchEntity",
    )


def test_task_template_not_found(client, instance_id):
    """Test that describing a non-existent TaskTemplate raises error."""
    with pytest.raises(ClientError) as exc:
        client.get_task_template(
            InstanceId=instance_id,
            TaskTemplateId="fake-id",
        )
    assert exc.value.response["Error"]["Code"] in (
        "ResourceNotFoundException",
        "NotFoundException",
        "EntityNotFoundException",
        "InvalidRequestException",
        "NoSuchEntity",
    )


def test_test_case_lifecycle(client, instance_id):
    """Test TestCase CRUD lifecycle."""
    # CREATE
    create_resp = client.create_test_case(
        InstanceId=instance_id,
        Name="test-name-1",
        Content="test-string",
    )
    assert isinstance(create_resp.get("TestCaseId"), str)
    assert len(create_resp.get("TestCaseId", "")) > 0

    test_case_id = create_resp["TestCaseId"]

    # DESCRIBE
    desc_resp = client.describe_test_case(
        InstanceId=instance_id,
        TestCaseId=test_case_id,
    )
    assert isinstance(desc_resp.get("TestCase", {}), dict)

    # DELETE
    client.delete_test_case(
        InstanceId=instance_id,
        TestCaseId=test_case_id,
    )

    # DESCRIBE after DELETE should fail
    with pytest.raises(ClientError) as exc:
        client.describe_test_case(
            InstanceId=instance_id,
            TestCaseId=test_case_id,
        )
    assert exc.value.response["Error"]["Code"] in (
        "ResourceNotFoundException",
        "NotFoundException",
        "EntityNotFoundException",
        "InvalidRequestException",
        "NoSuchEntity",
    )


def test_test_case_not_found(client, instance_id):
    """Test that describing a non-existent TestCase raises error."""
    with pytest.raises(ClientError) as exc:
        client.describe_test_case(
            InstanceId=instance_id,
            TestCaseId="fake-id",
        )
    assert exc.value.response["Error"]["Code"] in (
        "ResourceNotFoundException",
        "NotFoundException",
        "EntityNotFoundException",
        "InvalidRequestException",
        "NoSuchEntity",
    )


def test_traffic_distribution_group_lifecycle(client, instance_id):
    """Test TrafficDistributionGroup CRUD lifecycle."""
    # CREATE
    client.create_traffic_distribution_group(
        Name="test-name-1",
        InstanceId=instance_id,
    )

    # DESCRIBE
    desc_resp = client.describe_traffic_distribution_group(
        TrafficDistributionGroupId="test-id-1",
    )
    assert isinstance(desc_resp.get("TrafficDistributionGroup", {}), dict)

    # DELETE
    client.delete_traffic_distribution_group(
        TrafficDistributionGroupId="test-id-1",
    )

    # DESCRIBE after DELETE should fail
    with pytest.raises(ClientError) as exc:
        client.describe_traffic_distribution_group(
            TrafficDistributionGroupId="test-id-1",
        )
    assert exc.value.response["Error"]["Code"] in (
        "ResourceNotFoundException",
        "NotFoundException",
        "EntityNotFoundException",
        "InvalidRequestException",
        "NoSuchEntity",
    )


def test_traffic_distribution_group_not_found(client, instance_id):
    """Test that describing a non-existent TrafficDistributionGroup raises error."""
    with pytest.raises(ClientError) as exc:
        client.describe_traffic_distribution_group(
            TrafficDistributionGroupId="fake-id",
        )
    assert exc.value.response["Error"]["Code"] in (
        "ResourceNotFoundException",
        "NotFoundException",
        "EntityNotFoundException",
        "InvalidRequestException",
        "NoSuchEntity",
    )


def test_user_lifecycle(client, instance_id):
    """Test User CRUD lifecycle."""
    # CREATE
    create_resp = client.create_user(
        Username="test-name-1",
        SecurityProfileIds=["test-string"],
        RoutingProfileId="test-id-1",
        InstanceId=instance_id,
    )
    assert isinstance(create_resp.get("UserId"), str)
    assert len(create_resp.get("UserId", "")) > 0

    user_id = create_resp["UserId"]

    # DESCRIBE
    desc_resp = client.describe_user(
        UserId=user_id,
        InstanceId=instance_id,
    )
    assert isinstance(desc_resp.get("User", {}), dict)

    # DELETE
    client.delete_user(
        InstanceId=instance_id,
        UserId=user_id,
    )

    # DESCRIBE after DELETE should fail
    with pytest.raises(ClientError) as exc:
        client.describe_user(
            UserId=user_id,
            InstanceId=instance_id,
        )
    assert exc.value.response["Error"]["Code"] in (
        "ResourceNotFoundException",
        "NotFoundException",
        "EntityNotFoundException",
        "InvalidRequestException",
        "NoSuchEntity",
    )


def test_user_not_found(client, instance_id):
    """Test that describing a non-existent User raises error."""
    with pytest.raises(ClientError) as exc:
        client.describe_user(
            UserId="fake-id",
            InstanceId=instance_id,
        )
    assert exc.value.response["Error"]["Code"] in (
        "ResourceNotFoundException",
        "NotFoundException",
        "EntityNotFoundException",
        "InvalidRequestException",
        "NoSuchEntity",
    )


def test_user_hierarchy_group_lifecycle(client, instance_id):
    """Test UserHierarchyGroup CRUD lifecycle."""
    # CREATE
    create_resp = client.create_user_hierarchy_group(
        Name="test-name-1",
        InstanceId=instance_id,
    )
    assert isinstance(create_resp.get("HierarchyGroupId"), str)
    assert len(create_resp.get("HierarchyGroupId", "")) > 0

    hierarchy_group_id = create_resp["HierarchyGroupId"]

    # DESCRIBE
    desc_resp = client.describe_user_hierarchy_group(
        HierarchyGroupId=hierarchy_group_id,
        InstanceId=instance_id,
    )
    assert isinstance(desc_resp.get("HierarchyGroup", {}), dict)

    # DELETE
    client.delete_user_hierarchy_group(
        HierarchyGroupId=hierarchy_group_id,
        InstanceId=instance_id,
    )

    # DESCRIBE after DELETE should fail
    with pytest.raises(ClientError) as exc:
        client.describe_user_hierarchy_group(
            HierarchyGroupId=hierarchy_group_id,
            InstanceId=instance_id,
        )
    assert exc.value.response["Error"]["Code"] in (
        "ResourceNotFoundException",
        "NotFoundException",
        "EntityNotFoundException",
        "InvalidRequestException",
        "NoSuchEntity",
    )


def test_user_hierarchy_group_not_found(client, instance_id):
    """Test that describing a non-existent UserHierarchyGroup raises error."""
    with pytest.raises(ClientError) as exc:
        client.describe_user_hierarchy_group(
            HierarchyGroupId="fake-id",
            InstanceId=instance_id,
        )
    assert exc.value.response["Error"]["Code"] in (
        "ResourceNotFoundException",
        "NotFoundException",
        "EntityNotFoundException",
        "InvalidRequestException",
        "NoSuchEntity",
    )


def test_view_lifecycle(client, instance_id):
    """Test View CRUD lifecycle."""
    # CREATE
    create_resp = client.create_view(
        InstanceId=instance_id,
        Status="PUBLISHED",
        Content={},
        Name="test-name-1",
    )
    assert isinstance(create_resp.get("View", {}), dict)

    # DESCRIBE
    desc_resp = client.describe_view(
        InstanceId=instance_id,
        ViewId="test-id-1",
    )
    assert isinstance(desc_resp.get("View", {}), dict)

    # DELETE
    client.delete_view(
        InstanceId=instance_id,
        ViewId="test-id-1",
    )

    # DESCRIBE after DELETE should fail
    with pytest.raises(ClientError) as exc:
        client.describe_view(
            InstanceId=instance_id,
            ViewId="test-id-1",
        )
    assert exc.value.response["Error"]["Code"] in (
        "ResourceNotFoundException",
        "NotFoundException",
        "EntityNotFoundException",
        "InvalidRequestException",
        "NoSuchEntity",
    )


def test_view_not_found(client, instance_id):
    """Test that describing a non-existent View raises error."""
    with pytest.raises(ClientError) as exc:
        client.describe_view(
            InstanceId=instance_id,
            ViewId="fake-id",
        )
    assert exc.value.response["Error"]["Code"] in (
        "ResourceNotFoundException",
        "NotFoundException",
        "EntityNotFoundException",
        "InvalidRequestException",
        "NoSuchEntity",
    )


def test_vocabulary_lifecycle(client, instance_id):
    """Test Vocabulary CRUD lifecycle."""
    # CREATE
    create_resp = client.create_vocabulary(
        InstanceId=instance_id,
        VocabularyName="test-name-1",
        LanguageCode="ar-AE",
        Content="test-string",
    )
    assert isinstance(create_resp.get("VocabularyArn"), str)
    assert isinstance(create_resp.get("VocabularyId"), str)
    assert len(create_resp.get("VocabularyId", "")) > 0
    assert isinstance(create_resp.get("State"), str)

    vocabulary_id = create_resp["VocabularyId"]

    # DESCRIBE
    desc_resp = client.describe_vocabulary(
        InstanceId=instance_id,
        VocabularyId=vocabulary_id,
    )
    assert isinstance(desc_resp.get("Vocabulary", {}), dict)

    # DELETE
    client.delete_vocabulary(
        InstanceId=instance_id,
        VocabularyId=vocabulary_id,
    )

    # DESCRIBE after DELETE should fail
    with pytest.raises(ClientError) as exc:
        client.describe_vocabulary(
            InstanceId=instance_id,
            VocabularyId=vocabulary_id,
        )
    assert exc.value.response["Error"]["Code"] in (
        "ResourceNotFoundException",
        "NotFoundException",
        "EntityNotFoundException",
        "InvalidRequestException",
        "NoSuchEntity",
    )


def test_vocabulary_not_found(client, instance_id):
    """Test that describing a non-existent Vocabulary raises error."""
    with pytest.raises(ClientError) as exc:
        client.describe_vocabulary(
            InstanceId=instance_id,
            VocabularyId="fake-id",
        )
    assert exc.value.response["Error"]["Code"] in (
        "ResourceNotFoundException",
        "NotFoundException",
        "EntityNotFoundException",
        "InvalidRequestException",
        "NoSuchEntity",
    )


def test_workspace_lifecycle(client, instance_id):
    """Test Workspace CRUD lifecycle."""
    # CREATE
    create_resp = client.create_workspace(
        InstanceId=instance_id,
        Name="test-name-1",
    )
    assert isinstance(create_resp.get("WorkspaceId"), str)
    assert len(create_resp.get("WorkspaceId", "")) > 0
    assert isinstance(create_resp.get("WorkspaceArn"), str)

    workspace_id = create_resp["WorkspaceId"]

    # DESCRIBE
    desc_resp = client.describe_workspace(
        InstanceId=instance_id,
        WorkspaceId=workspace_id,
    )
    assert isinstance(desc_resp.get("Workspace", {}), dict)

    # DELETE
    client.delete_workspace(
        InstanceId=instance_id,
        WorkspaceId=workspace_id,
    )

    # DESCRIBE after DELETE should fail
    with pytest.raises(ClientError) as exc:
        client.describe_workspace(
            InstanceId=instance_id,
            WorkspaceId=workspace_id,
        )
    assert exc.value.response["Error"]["Code"] in (
        "ResourceNotFoundException",
        "NotFoundException",
        "EntityNotFoundException",
        "InvalidRequestException",
        "NoSuchEntity",
    )


def test_workspace_not_found(client, instance_id):
    """Test that describing a non-existent Workspace raises error."""
    with pytest.raises(ClientError) as exc:
        client.describe_workspace(
            InstanceId=instance_id,
            WorkspaceId="fake-id",
        )
    assert exc.value.response["Error"]["Code"] in (
        "ResourceNotFoundException",
        "NotFoundException",
        "EntityNotFoundException",
        "InvalidRequestException",
        "NoSuchEntity",
    )
