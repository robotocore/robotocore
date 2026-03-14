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


def test_workspace_lifecycle(client):
    """Test Workspace CRUD lifecycle."""
    # CREATE
    create_resp = client.create_workspace(
        InstanceId="test-instance-1",
        Name="test-name-1",
    )
    assert isinstance(create_resp.get("WorkspaceId"), str)
    assert len(create_resp["WorkspaceId"]) > 0
    assert isinstance(create_resp.get("WorkspaceArn"), str)
    assert create_resp["WorkspaceArn"].startswith("arn:aws:")

    # DESCRIBE
    desc_resp = client.describe_workspace(
        InstanceId="test-instance-1",
        WorkspaceId="test-id-1",
    )
    assert isinstance(desc_resp.get("Workspace", {}), dict)

    # DELETE
    client.delete_workspace(
        InstanceId="test-instance-1",
        WorkspaceId="test-id-1",
    )

    # DESCRIBE after DELETE should fail
    with pytest.raises(ClientError) as exc:
        client.describe_workspace(
            InstanceId="test-instance-1",
            WorkspaceId="test-id-1",
        )
    assert exc.value.response["Error"]["Code"] in (
        "ResourceNotFoundException",
        "NotFoundException",
        "EntityNotFoundException",
        "InvalidRequestException",
    )


def test_workspace_not_found(client):
    """Test that describing a non-existent Workspace raises an error."""
    with pytest.raises(ClientError) as exc:
        client.describe_workspace(InstanceId="fake-id", WorkspaceId="fake-id")
    assert exc.value.response["Error"]["Code"] in (
        "ResourceNotFoundException",
        "NotFoundException",
        "EntityNotFoundException",
        "InvalidRequestException",
    )


def test_contact_flow_module_alias_lifecycle(client):
    """Test ContactFlowModuleAlias CRUD lifecycle."""
    # CREATE
    create_resp = client.create_contact_flow_module_alias(
        InstanceId="test-instance-1",
        ContactFlowModuleId="test-id-1",
        ContactFlowModuleVersion=1,
        AliasName="test-name-1",
    )
    assert isinstance(create_resp.get("ContactFlowModuleArn"), str)
    assert create_resp["ContactFlowModuleArn"].startswith("arn:aws:")
    assert isinstance(create_resp.get("Id"), str)
    assert len(create_resp["Id"]) > 0

    # DESCRIBE
    desc_resp = client.describe_contact_flow_module_alias(
        InstanceId="test-instance-1",
        ContactFlowModuleId="test-id-1",
        AliasId="test-id-1",
    )
    assert isinstance(desc_resp.get("ContactFlowModuleAlias", {}), dict)

    # DELETE
    client.delete_contact_flow_module_alias(
        InstanceId="test-instance-1",
        ContactFlowModuleId="test-id-1",
        AliasId="test-id-1",
    )

    # DESCRIBE after DELETE should fail
    with pytest.raises(ClientError) as exc:
        client.describe_contact_flow_module_alias(
            InstanceId="test-instance-1",
            ContactFlowModuleId="test-id-1",
            AliasId="test-id-1",
        )
    assert exc.value.response["Error"]["Code"] in (
        "ResourceNotFoundException",
        "NotFoundException",
        "EntityNotFoundException",
        "InvalidRequestException",
    )


def test_contact_flow_module_alias_not_found(client):
    """Test that describing a non-existent ContactFlowModuleAlias raises an error."""
    with pytest.raises(ClientError) as exc:
        client.describe_contact_flow_module_alias(
            InstanceId="fake-id",
            ContactFlowModuleId="fake-id",
            AliasId="fake-id",
        )
    assert exc.value.response["Error"]["Code"] in (
        "ResourceNotFoundException",
        "NotFoundException",
        "EntityNotFoundException",
        "InvalidRequestException",
    )


def test_data_table_attribute_lifecycle(client):
    """Test DataTableAttribute CRUD lifecycle."""
    # CREATE
    create_resp = client.create_data_table_attribute(
        InstanceId="test-instance-1",
        DataTableId="test-id-1",
        Name="test-name-1",
        ValueType="DEFAULT",
    )
    assert isinstance(create_resp.get("Name"), str)
    assert len(create_resp.get("Name", "")) > 0
    assert isinstance(create_resp.get("AttributeId"), str)
    assert len(create_resp["AttributeId"]) > 0
    assert isinstance(create_resp.get("LockVersion", {}), dict)

    # DESCRIBE
    desc_resp = client.describe_data_table_attribute(
        InstanceId="test-instance-1",
        DataTableId="test-id-1",
        AttributeName="test-name-1",
    )
    assert isinstance(desc_resp.get("Attribute", {}), dict)

    # DELETE
    client.delete_data_table_attribute(
        InstanceId="test-instance-1",
        DataTableId="test-id-1",
        AttributeName="test-name-1",
    )

    # DESCRIBE after DELETE should fail
    with pytest.raises(ClientError) as exc:
        client.describe_data_table_attribute(
            InstanceId="test-instance-1",
            DataTableId="test-id-1",
            AttributeName="test-name-1",
        )
    assert exc.value.response["Error"]["Code"] in (
        "ResourceNotFoundException",
        "NotFoundException",
        "EntityNotFoundException",
        "InvalidRequestException",
    )


def test_data_table_attribute_not_found(client):
    """Test that describing a non-existent DataTableAttribute raises an error."""
    with pytest.raises(ClientError) as exc:
        client.describe_data_table_attribute(
            InstanceId="fake-id",
            DataTableId="fake-id",
            AttributeName="fake-id",
        )
    assert exc.value.response["Error"]["Code"] in (
        "ResourceNotFoundException",
        "NotFoundException",
        "EntityNotFoundException",
        "InvalidRequestException",
    )


def test_test_case_lifecycle(client):
    """Test TestCase CRUD lifecycle."""
    # CREATE
    create_resp = client.create_test_case(
        InstanceId="test-instance-1",
        Name="test-name-1",
        Content="test-string",
    )
    assert isinstance(create_resp.get("TestCaseId"), str)
    assert len(create_resp["TestCaseId"]) > 0
    assert isinstance(create_resp.get("TestCaseArn"), str)
    assert create_resp["TestCaseArn"].startswith("arn:aws:")

    # DESCRIBE
    desc_resp = client.describe_test_case(
        InstanceId="test-instance-1",
        TestCaseId="test-id-1",
    )
    assert isinstance(desc_resp.get("TestCase", {}), dict)

    # DELETE
    client.delete_test_case(
        InstanceId="test-instance-1",
        TestCaseId="test-id-1",
    )

    # DESCRIBE after DELETE should fail
    with pytest.raises(ClientError) as exc:
        client.describe_test_case(
            InstanceId="test-instance-1",
            TestCaseId="test-id-1",
        )
    assert exc.value.response["Error"]["Code"] in (
        "ResourceNotFoundException",
        "NotFoundException",
        "EntityNotFoundException",
        "InvalidRequestException",
    )


def test_test_case_not_found(client):
    """Test that describing a non-existent TestCase raises an error."""
    with pytest.raises(ClientError) as exc:
        client.describe_test_case(InstanceId="fake-id", TestCaseId="fake-id")
    assert exc.value.response["Error"]["Code"] in (
        "ResourceNotFoundException",
        "NotFoundException",
        "EntityNotFoundException",
        "InvalidRequestException",
    )


def test_contact_evaluation_lifecycle(client):
    """Test ContactEvaluation CRUD lifecycle."""
    # CREATE
    create_resp = client.start_contact_evaluation(
        InstanceId="test-instance-1",
        ContactId="test-id-1",
        EvaluationFormId="test-id-1",
    )
    assert isinstance(create_resp.get("EvaluationId"), str)
    assert len(create_resp["EvaluationId"]) > 0
    assert isinstance(create_resp.get("EvaluationArn"), str)
    assert create_resp["EvaluationArn"].startswith("arn:aws:")

    # DESCRIBE
    desc_resp = client.describe_contact_evaluation(
        InstanceId="test-instance-1",
        EvaluationId="test-id-1",
    )
    assert isinstance(desc_resp.get("Evaluation", {}), dict)
    assert isinstance(desc_resp.get("EvaluationForm", {}), dict)

    # DELETE
    client.delete_contact_evaluation(
        InstanceId="test-instance-1",
        EvaluationId="test-id-1",
    )

    # DESCRIBE after DELETE should fail
    with pytest.raises(ClientError) as exc:
        client.describe_contact_evaluation(
            InstanceId="test-instance-1",
            EvaluationId="test-id-1",
        )
    assert exc.value.response["Error"]["Code"] in (
        "ResourceNotFoundException",
        "NotFoundException",
        "EntityNotFoundException",
        "InvalidRequestException",
    )


def test_contact_evaluation_not_found(client):
    """Test that describing a non-existent ContactEvaluation raises an error."""
    with pytest.raises(ClientError) as exc:
        client.describe_contact_evaluation(InstanceId="fake-id", EvaluationId="fake-id")
    assert exc.value.response["Error"]["Code"] in (
        "ResourceNotFoundException",
        "NotFoundException",
        "EntityNotFoundException",
        "InvalidRequestException",
    )


def test_data_table_lifecycle(client):
    """Test DataTable CRUD lifecycle."""
    # CREATE
    create_resp = client.create_data_table(
        InstanceId="test-instance-1",
        Name="test-name-1",
        TimeZone="test-string",
        ValueLockLevel="test-string",
        Status="test-string",
    )
    assert isinstance(create_resp.get("Id"), str)
    assert len(create_resp["Id"]) > 0
    assert isinstance(create_resp.get("Arn"), str)
    assert create_resp["Arn"].startswith("arn:aws:")
    assert isinstance(create_resp.get("LockVersion", {}), dict)

    # DESCRIBE
    desc_resp = client.describe_data_table(
        InstanceId="test-instance-1",
        DataTableId="test-id-1",
    )
    assert isinstance(desc_resp.get("DataTable", {}), dict)

    # DELETE
    client.delete_data_table(
        InstanceId="test-instance-1",
        DataTableId="test-id-1",
    )

    # DESCRIBE after DELETE should fail
    with pytest.raises(ClientError) as exc:
        client.describe_data_table(
            InstanceId="test-instance-1",
            DataTableId="test-id-1",
        )
    assert exc.value.response["Error"]["Code"] in (
        "ResourceNotFoundException",
        "NotFoundException",
        "EntityNotFoundException",
        "InvalidRequestException",
    )


def test_data_table_not_found(client):
    """Test that describing a non-existent DataTable raises an error."""
    with pytest.raises(ClientError) as exc:
        client.describe_data_table(InstanceId="fake-id", DataTableId="fake-id")
    assert exc.value.response["Error"]["Code"] in (
        "ResourceNotFoundException",
        "NotFoundException",
        "EntityNotFoundException",
        "InvalidRequestException",
    )


def test_email_address_lifecycle(client):
    """Test EmailAddress CRUD lifecycle."""
    # CREATE
    create_resp = client.create_email_address(
        InstanceId="test-instance-1",
        EmailAddress="test-string",
    )
    assert isinstance(create_resp.get("EmailAddressId"), str)
    assert len(create_resp["EmailAddressId"]) > 0
    assert isinstance(create_resp.get("EmailAddressArn"), str)
    assert create_resp["EmailAddressArn"].startswith("arn:aws:")

    # DESCRIBE
    desc_resp = client.describe_email_address(
        InstanceId="test-instance-1",
        EmailAddressId="test-id-1",
    )
    assert isinstance(desc_resp.get("EmailAddressId"), str)
    assert len(desc_resp["EmailAddressId"]) > 0
    assert isinstance(desc_resp.get("EmailAddressArn"), str)
    assert desc_resp["EmailAddressArn"].startswith("arn:aws:")
    assert isinstance(desc_resp.get("EmailAddress"), str)

    # DELETE
    client.delete_email_address(
        InstanceId="test-instance-1",
        EmailAddressId="test-id-1",
    )

    # DESCRIBE after DELETE should fail
    with pytest.raises(ClientError) as exc:
        client.describe_email_address(
            InstanceId="test-instance-1",
            EmailAddressId="test-id-1",
        )
    assert exc.value.response["Error"]["Code"] in (
        "ResourceNotFoundException",
        "NotFoundException",
        "EntityNotFoundException",
        "InvalidRequestException",
    )


def test_email_address_not_found(client):
    """Test that describing a non-existent EmailAddress raises an error."""
    with pytest.raises(ClientError) as exc:
        client.describe_email_address(InstanceId="fake-id", EmailAddressId="fake-id")
    assert exc.value.response["Error"]["Code"] in (
        "ResourceNotFoundException",
        "NotFoundException",
        "EntityNotFoundException",
        "InvalidRequestException",
    )


def test_notification_lifecycle(client):
    """Test Notification CRUD lifecycle."""
    # CREATE
    create_resp = client.create_notification(
        InstanceId="test-instance-1",
        Recipients=[],
        Content={},
    )
    assert isinstance(create_resp.get("NotificationId"), str)
    assert len(create_resp["NotificationId"]) > 0
    assert isinstance(create_resp.get("NotificationArn"), str)
    assert create_resp["NotificationArn"].startswith("arn:aws:")

    # DESCRIBE
    desc_resp = client.describe_notification(
        InstanceId="test-instance-1",
        NotificationId="test-id-1",
    )
    assert isinstance(desc_resp.get("Notification", {}), dict)

    # DELETE
    client.delete_notification(
        InstanceId="test-instance-1",
        NotificationId="test-id-1",
    )

    # DESCRIBE after DELETE should fail
    with pytest.raises(ClientError) as exc:
        client.describe_notification(
            InstanceId="test-instance-1",
            NotificationId="test-id-1",
        )
    assert exc.value.response["Error"]["Code"] in (
        "ResourceNotFoundException",
        "NotFoundException",
        "EntityNotFoundException",
        "InvalidRequestException",
    )


def test_notification_not_found(client):
    """Test that describing a non-existent Notification raises an error."""
    with pytest.raises(ClientError) as exc:
        client.describe_notification(InstanceId="fake-id", NotificationId="fake-id")
    assert exc.value.response["Error"]["Code"] in (
        "ResourceNotFoundException",
        "NotFoundException",
        "EntityNotFoundException",
        "InvalidRequestException",
    )
