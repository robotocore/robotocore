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
    assert create_resp["EvaluationArn"].startswith("arn:aws:")

    # DESCRIBE
    desc_resp = client.describe_contact_evaluation(
        InstanceId=instance_id,
        EvaluationId="test-id-1",
    )
    assert isinstance(desc_resp.get("Evaluation", {}), dict)
    assert isinstance(desc_resp.get("EvaluationForm", {}), dict)

    # DELETE
    client.delete_contact_evaluation(
        InstanceId=instance_id,
        EvaluationId="test-id-1",
    )

    # DESCRIBE after DELETE should fail
    with pytest.raises(ClientError) as exc:
        client.describe_contact_evaluation(
            InstanceId=instance_id,
            EvaluationId="test-id-1",
        )
    assert exc.value.response["Error"]["Code"] in (
        "ResourceNotFoundException",
        "NotFoundException",
        "EntityNotFoundException",
        "InvalidRequestException",
        "NoSuchEntity",
    )


def test_contact_evaluation_not_found(client, instance_id):
    """Test that describing a non-existent ContactEvaluation raises an error."""
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


def test_contact_flow_module_alias_lifecycle(client, instance_id):
    """Test ContactFlowModuleAlias CRUD lifecycle."""
    # CREATE
    create_resp = client.create_contact_flow_module_alias(
        InstanceId=instance_id,
        ContactFlowModuleId="test-id-1",
        ContactFlowModuleVersion=1,
        AliasName="test-name-1",
    )
    assert isinstance(create_resp.get("ContactFlowModuleArn"), str)
    assert create_resp["ContactFlowModuleArn"].startswith("arn:aws:")
    assert isinstance(create_resp.get("Id"), str)
    assert len(create_resp.get("Id", "")) > 0

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
    """Test that describing a non-existent ContactFlowModuleAlias raises an error."""
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
    assert len(create_resp.get("Id", "")) > 0
    assert isinstance(create_resp.get("Arn"), str)
    assert create_resp["Arn"].startswith("arn:aws:")
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
    """Test that describing a non-existent DataTable raises an error."""
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
    assert len(create_resp.get("Name", "")) > 0
    assert isinstance(create_resp.get("AttributeId"), str)
    assert len(create_resp.get("AttributeId", "")) > 0
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
    """Test that describing a non-existent DataTableAttribute raises an error."""
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
    assert isinstance(create_resp.get("EmailAddressArn"), str)
    assert create_resp["EmailAddressArn"].startswith("arn:aws:")

    # DESCRIBE
    desc_resp = client.describe_email_address(
        InstanceId=instance_id,
        EmailAddressId="test-id-1",
    )
    assert isinstance(desc_resp.get("EmailAddressId"), str)
    assert len(desc_resp.get("EmailAddressId", "")) > 0
    assert isinstance(desc_resp.get("EmailAddressArn"), str)
    assert desc_resp["EmailAddressArn"].startswith("arn:aws:")
    assert isinstance(desc_resp.get("DisplayName"), str)
    assert len(desc_resp.get("DisplayName", "")) > 0
    assert isinstance(desc_resp.get("AliasConfigurations", []), list)
    assert isinstance(desc_resp.get("Tags", {}), dict)

    # DELETE
    client.delete_email_address(
        InstanceId=instance_id,
        EmailAddressId="test-id-1",
    )

    # DESCRIBE after DELETE should fail
    with pytest.raises(ClientError) as exc:
        client.describe_email_address(
            InstanceId=instance_id,
            EmailAddressId="test-id-1",
        )
    assert exc.value.response["Error"]["Code"] in (
        "ResourceNotFoundException",
        "NotFoundException",
        "EntityNotFoundException",
        "InvalidRequestException",
        "NoSuchEntity",
    )


def test_email_address_not_found(client, instance_id):
    """Test that describing a non-existent EmailAddress raises an error."""
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


def test_notification_lifecycle(client, instance_id):
    """Test Notification CRUD lifecycle."""
    # CREATE
    create_resp = client.create_notification(
        InstanceId=instance_id,
        Recipients=[],
        Content={},
    )
    assert isinstance(create_resp.get("NotificationId"), str)
    assert len(create_resp.get("NotificationId", "")) > 0
    assert isinstance(create_resp.get("NotificationArn"), str)
    assert create_resp["NotificationArn"].startswith("arn:aws:")

    # DESCRIBE
    desc_resp = client.describe_notification(
        InstanceId=instance_id,
        NotificationId="test-id-1",
    )
    assert isinstance(desc_resp.get("Notification", {}), dict)

    # DELETE
    client.delete_notification(
        InstanceId=instance_id,
        NotificationId="test-id-1",
    )

    # DESCRIBE after DELETE should fail
    with pytest.raises(ClientError) as exc:
        client.describe_notification(
            InstanceId=instance_id,
            NotificationId="test-id-1",
        )
    assert exc.value.response["Error"]["Code"] in (
        "ResourceNotFoundException",
        "NotFoundException",
        "EntityNotFoundException",
        "InvalidRequestException",
        "NoSuchEntity",
    )


def test_notification_not_found(client, instance_id):
    """Test that describing a non-existent Notification raises an error."""
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
    assert isinstance(create_resp.get("TestCaseArn"), str)
    assert create_resp["TestCaseArn"].startswith("arn:aws:")

    # DESCRIBE
    desc_resp = client.describe_test_case(
        InstanceId=instance_id,
        TestCaseId="test-id-1",
    )
    assert isinstance(desc_resp.get("TestCase", {}), dict)

    # DELETE
    client.delete_test_case(
        InstanceId=instance_id,
        TestCaseId="test-id-1",
    )

    # DESCRIBE after DELETE should fail
    with pytest.raises(ClientError) as exc:
        client.describe_test_case(
            InstanceId=instance_id,
            TestCaseId="test-id-1",
        )
    assert exc.value.response["Error"]["Code"] in (
        "ResourceNotFoundException",
        "NotFoundException",
        "EntityNotFoundException",
        "InvalidRequestException",
        "NoSuchEntity",
    )


def test_test_case_not_found(client, instance_id):
    """Test that describing a non-existent TestCase raises an error."""
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
    assert create_resp["WorkspaceArn"].startswith("arn:aws:")

    # DESCRIBE
    desc_resp = client.describe_workspace(
        InstanceId=instance_id,
        WorkspaceId="test-id-1",
    )
    assert isinstance(desc_resp.get("Workspace", {}), dict)

    # DELETE
    client.delete_workspace(
        InstanceId=instance_id,
        WorkspaceId="test-id-1",
    )

    # DESCRIBE after DELETE should fail
    with pytest.raises(ClientError) as exc:
        client.describe_workspace(
            InstanceId=instance_id,
            WorkspaceId="test-id-1",
        )
    assert exc.value.response["Error"]["Code"] in (
        "ResourceNotFoundException",
        "NotFoundException",
        "EntityNotFoundException",
        "InvalidRequestException",
        "NoSuchEntity",
    )


def test_workspace_not_found(client, instance_id):
    """Test that describing a non-existent Workspace raises an error."""
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
