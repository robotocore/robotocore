"""Resource lifecycle tests for backup (auto-generated)."""

import logging

import pytest
from botocore.exceptions import ClientError


@pytest.fixture
def client():
    import boto3

    return boto3.client(
        "backup",
        endpoint_url="http://localhost:4566",
        region_name="us-east-1",
        aws_access_key_id="testing",
        aws_secret_access_key="testing",
    )


@pytest.fixture
def vault_name(client):
    name = "test-vault-1"
    client.create_backup_vault(BackupVaultName=name)
    yield name
    try:
        client.delete_backup_vault(BackupVaultName=name)
    except ClientError as e:
        logging.debug("pre-cleanup skipped: %s", e)


def test_backup_job_lifecycle(client, vault_name):
    """Test BackupJob CRUD lifecycle."""
    # CREATE
    create_resp = client.start_backup_job(
        BackupVaultName=vault_name,
        ResourceArn="arn:aws:iam::123456789012:role/test-role",
        IamRoleArn="arn:aws:iam::123456789012:role/test-role",
    )
    assert isinstance(create_resp.get("BackupJobId"), str)
    assert len(create_resp.get("BackupJobId", "")) > 0
    assert create_resp.get("CreationDate") is not None

    backup_job_id = create_resp["BackupJobId"]

    # DESCRIBE
    desc_resp = client.describe_backup_job(
        BackupJobId=backup_job_id,
    )
    assert isinstance(desc_resp.get("BackupJobId"), str)
    assert len(desc_resp.get("BackupJobId", "")) > 0
    assert isinstance(desc_resp.get("RecoveryPointLifecycle", {}), dict)
    assert isinstance(desc_resp.get("CreatedBy", {}), dict)
    assert isinstance(desc_resp.get("BackupOptions", {}), dict)
    assert isinstance(desc_resp.get("ChildJobsInState", {}), dict)

    # DELETE
    client.stop_backup_job(
        BackupJobId=backup_job_id,
    )

    # DESCRIBE after DELETE should fail
    with pytest.raises(ClientError) as exc:
        client.describe_backup_job(
            BackupJobId=backup_job_id,
        )
    assert exc.value.response["Error"]["Code"] in (
        "ResourceNotFoundException",
        "ResourcePolicyNotFoundException",
        "NotFoundException",
        "EntityNotFoundException",
        "InvalidRequestException",
        "NoSuchEntity",
    )


def test_backup_job_not_found(client, vault_name):
    """Test that describing a non-existent BackupJob raises error."""
    with pytest.raises(ClientError) as exc:
        client.describe_backup_job(
            BackupJobId="fake-id",
        )
    assert exc.value.response["Error"]["Code"] in (
        "ResourceNotFoundException",
        "ResourcePolicyNotFoundException",
        "NotFoundException",
        "EntityNotFoundException",
        "InvalidRequestException",
        "NoSuchEntity",
    )


def test_backup_plan_lifecycle(client, vault_name):
    """Test BackupPlan CRUD lifecycle."""
    # CREATE
    create_resp = client.create_backup_plan(
        BackupPlan={
            "BackupPlanName": "test-name-1",
            "Rules": [{"RuleName": "test-name-1", "TargetBackupVaultName": "test-name-1"}],
        },
    )
    assert isinstance(create_resp.get("BackupPlanId"), str)
    assert len(create_resp.get("BackupPlanId", "")) > 0
    assert create_resp.get("CreationDate") is not None
    assert isinstance(create_resp.get("AdvancedBackupSettings", []), list)

    backup_plan_id = create_resp["BackupPlanId"]

    # DESCRIBE
    desc_resp = client.get_backup_plan(
        BackupPlanId=backup_plan_id,
    )
    assert isinstance(desc_resp.get("BackupPlan", {}), dict)
    assert isinstance(desc_resp.get("BackupPlanId"), str)
    assert len(desc_resp.get("BackupPlanId", "")) > 0
    assert isinstance(desc_resp.get("AdvancedBackupSettings", []), list)
    assert isinstance(desc_resp.get("ScheduledRunsPreview", []), list)

    # DELETE
    client.delete_backup_plan(
        BackupPlanId=backup_plan_id,
    )

    # DESCRIBE after DELETE should fail
    with pytest.raises(ClientError) as exc:
        client.get_backup_plan(
            BackupPlanId=backup_plan_id,
        )
    assert exc.value.response["Error"]["Code"] in (
        "ResourceNotFoundException",
        "ResourcePolicyNotFoundException",
        "NotFoundException",
        "EntityNotFoundException",
        "InvalidRequestException",
        "NoSuchEntity",
    )


def test_backup_plan_not_found(client, vault_name):
    """Test that describing a non-existent BackupPlan raises error."""
    with pytest.raises(ClientError) as exc:
        client.get_backup_plan(
            BackupPlanId="fake-id",
        )
    assert exc.value.response["Error"]["Code"] in (
        "ResourceNotFoundException",
        "ResourcePolicyNotFoundException",
        "NotFoundException",
        "EntityNotFoundException",
        "InvalidRequestException",
        "NoSuchEntity",
    )


def test_backup_selection_lifecycle(client, vault_name):
    """Test BackupSelection CRUD lifecycle."""
    # CREATE
    create_resp = client.create_backup_selection(
        BackupPlanId="test-id-1",
        BackupSelection={
            "SelectionName": "test-name-1",
            "IamRoleArn": "arn:aws:iam::123456789012:role/test-role",
        },
    )
    assert isinstance(create_resp.get("SelectionId"), str)
    assert len(create_resp.get("SelectionId", "")) > 0
    assert isinstance(create_resp.get("BackupPlanId"), str)
    assert len(create_resp.get("BackupPlanId", "")) > 0
    assert create_resp.get("CreationDate") is not None

    selection_id = create_resp["SelectionId"]

    # DESCRIBE
    desc_resp = client.get_backup_selection(
        BackupPlanId="test-id-1",
        SelectionId=selection_id,
    )
    assert isinstance(desc_resp.get("BackupSelection", {}), dict)
    assert isinstance(desc_resp.get("SelectionId"), str)
    assert len(desc_resp.get("SelectionId", "")) > 0
    assert isinstance(desc_resp.get("BackupPlanId"), str)
    assert len(desc_resp.get("BackupPlanId", "")) > 0

    # DELETE
    client.delete_backup_selection(
        BackupPlanId="test-id-1",
        SelectionId=selection_id,
    )

    # DESCRIBE after DELETE should fail
    with pytest.raises(ClientError) as exc:
        client.get_backup_selection(
            BackupPlanId="test-id-1",
            SelectionId=selection_id,
        )
    assert exc.value.response["Error"]["Code"] in (
        "ResourceNotFoundException",
        "ResourcePolicyNotFoundException",
        "NotFoundException",
        "EntityNotFoundException",
        "InvalidRequestException",
        "NoSuchEntity",
    )


def test_backup_selection_not_found(client, vault_name):
    """Test that describing a non-existent BackupSelection raises error."""
    with pytest.raises(ClientError) as exc:
        client.get_backup_selection(
            BackupPlanId="fake-id",
            SelectionId="fake-id",
        )
    assert exc.value.response["Error"]["Code"] in (
        "ResourceNotFoundException",
        "ResourcePolicyNotFoundException",
        "NotFoundException",
        "EntityNotFoundException",
        "InvalidRequestException",
        "NoSuchEntity",
    )


def test_backup_vault_lifecycle(client, vault_name):
    """Test BackupVault CRUD lifecycle."""
    # CREATE
    create_resp = client.create_backup_vault(
        BackupVaultName=vault_name,
    )
    assert isinstance(create_resp.get("BackupVaultName"), str)
    assert len(create_resp.get("BackupVaultName", "")) > 0
    assert create_resp.get("CreationDate") is not None

    # DESCRIBE
    desc_resp = client.describe_backup_vault(
        BackupVaultName=vault_name,
    )
    assert isinstance(desc_resp.get("BackupVaultName"), str)
    assert len(desc_resp.get("BackupVaultName", "")) > 0
    assert isinstance(desc_resp.get("LatestMpaApprovalTeamUpdate", {}), dict)

    # DELETE
    client.delete_backup_vault(
        BackupVaultName=vault_name,
    )

    # DESCRIBE after DELETE should fail
    with pytest.raises(ClientError) as exc:
        client.describe_backup_vault(
            BackupVaultName=vault_name,
        )
    assert exc.value.response["Error"]["Code"] in (
        "ResourceNotFoundException",
        "ResourcePolicyNotFoundException",
        "NotFoundException",
        "EntityNotFoundException",
        "InvalidRequestException",
        "NoSuchEntity",
    )


def test_backup_vault_not_found(client, vault_name):
    """Test that describing a non-existent BackupVault raises error."""
    with pytest.raises(ClientError) as exc:
        client.describe_backup_vault(
            BackupVaultName=vault_name,
        )
    assert exc.value.response["Error"]["Code"] in (
        "ResourceNotFoundException",
        "ResourcePolicyNotFoundException",
        "NotFoundException",
        "EntityNotFoundException",
        "InvalidRequestException",
        "NoSuchEntity",
    )


def test_backup_vault_access_policy_lifecycle(client, vault_name):
    """Test BackupVaultAccessPolicy CRUD lifecycle."""
    # CREATE
    client.put_backup_vault_access_policy(
        BackupVaultName=vault_name,
    )

    # DESCRIBE
    desc_resp = client.get_backup_vault_access_policy(
        BackupVaultName=vault_name,
    )
    assert isinstance(desc_resp.get("BackupVaultName"), str)
    assert len(desc_resp.get("BackupVaultName", "")) > 0

    # DELETE
    client.delete_backup_vault_access_policy(
        BackupVaultName=vault_name,
    )

    # DESCRIBE after DELETE should fail
    with pytest.raises(ClientError) as exc:
        client.get_backup_vault_access_policy(
            BackupVaultName=vault_name,
        )
    assert exc.value.response["Error"]["Code"] in (
        "ResourceNotFoundException",
        "ResourcePolicyNotFoundException",
        "NotFoundException",
        "EntityNotFoundException",
        "InvalidRequestException",
        "NoSuchEntity",
    )


def test_backup_vault_access_policy_not_found(client, vault_name):
    """Test that describing a non-existent BackupVaultAccessPolicy raises error."""
    with pytest.raises(ClientError) as exc:
        client.get_backup_vault_access_policy(
            BackupVaultName=vault_name,
        )
    assert exc.value.response["Error"]["Code"] in (
        "ResourceNotFoundException",
        "ResourcePolicyNotFoundException",
        "NotFoundException",
        "EntityNotFoundException",
        "InvalidRequestException",
        "NoSuchEntity",
    )


def test_backup_vault_notifications_lifecycle(client, vault_name):
    """Test BackupVaultNotifications CRUD lifecycle."""
    # CREATE
    client.put_backup_vault_notifications(
        BackupVaultName=vault_name,
        SNSTopicArn="arn:aws:iam::123456789012:role/test-role",
        BackupVaultEvents=["BACKUP_JOB_STARTED"],
    )

    # DESCRIBE
    desc_resp = client.get_backup_vault_notifications(
        BackupVaultName=vault_name,
    )
    assert isinstance(desc_resp.get("BackupVaultName"), str)
    assert len(desc_resp.get("BackupVaultName", "")) > 0
    assert isinstance(desc_resp.get("BackupVaultEvents", []), list)

    # DELETE
    client.delete_backup_vault_notifications(
        BackupVaultName=vault_name,
    )

    # DESCRIBE after DELETE should fail
    with pytest.raises(ClientError) as exc:
        client.get_backup_vault_notifications(
            BackupVaultName=vault_name,
        )
    assert exc.value.response["Error"]["Code"] in (
        "ResourceNotFoundException",
        "ResourcePolicyNotFoundException",
        "NotFoundException",
        "EntityNotFoundException",
        "InvalidRequestException",
        "NoSuchEntity",
    )


def test_backup_vault_notifications_not_found(client, vault_name):
    """Test that describing a non-existent BackupVaultNotifications raises error."""
    with pytest.raises(ClientError) as exc:
        client.get_backup_vault_notifications(
            BackupVaultName=vault_name,
        )
    assert exc.value.response["Error"]["Code"] in (
        "ResourceNotFoundException",
        "ResourcePolicyNotFoundException",
        "NotFoundException",
        "EntityNotFoundException",
        "InvalidRequestException",
        "NoSuchEntity",
    )


def test_copy_job_lifecycle(client, vault_name):
    """Test CopyJob CRUD lifecycle."""
    # CREATE
    create_resp = client.start_copy_job(
        RecoveryPointArn="arn:aws:iam::123456789012:role/test-role",
        SourceBackupVaultName="test-name-1",
        DestinationBackupVaultArn="arn:aws:iam::123456789012:role/test-role",
        IamRoleArn="arn:aws:iam::123456789012:role/test-role",
    )
    assert isinstance(create_resp.get("CopyJobId"), str)
    assert len(create_resp.get("CopyJobId", "")) > 0
    assert create_resp.get("CreationDate") is not None

    copy_job_id = create_resp["CopyJobId"]

    # DESCRIBE
    desc_resp = client.describe_copy_job(
        CopyJobId=copy_job_id,
    )
    assert isinstance(desc_resp.get("CopyJob", {}), dict)


def test_copy_job_not_found(client, vault_name):
    """Test that describing a non-existent CopyJob raises error."""
    with pytest.raises(ClientError) as exc:
        client.describe_copy_job(
            CopyJobId="fake-id",
        )
    assert exc.value.response["Error"]["Code"] in (
        "ResourceNotFoundException",
        "ResourcePolicyNotFoundException",
        "NotFoundException",
        "EntityNotFoundException",
        "InvalidRequestException",
        "NoSuchEntity",
    )


def test_framework_lifecycle(client, vault_name):
    """Test Framework CRUD lifecycle."""
    # CREATE
    create_resp = client.create_framework(
        FrameworkName="test-name-1",
        FrameworkControls=[{"ControlName": "test-name-1"}],
    )
    assert isinstance(create_resp.get("FrameworkName"), str)
    assert len(create_resp.get("FrameworkName", "")) > 0

    # DESCRIBE
    desc_resp = client.describe_framework(
        FrameworkName="test-name-1",
    )
    assert isinstance(desc_resp.get("FrameworkName"), str)
    assert len(desc_resp.get("FrameworkName", "")) > 0
    assert isinstance(desc_resp.get("FrameworkControls", []), list)

    # DELETE
    client.delete_framework(
        FrameworkName="test-name-1",
    )

    # DESCRIBE after DELETE should fail
    with pytest.raises(ClientError) as exc:
        client.describe_framework(
            FrameworkName="test-name-1",
        )
    assert exc.value.response["Error"]["Code"] in (
        "ResourceNotFoundException",
        "ResourcePolicyNotFoundException",
        "NotFoundException",
        "EntityNotFoundException",
        "InvalidRequestException",
        "NoSuchEntity",
    )


def test_framework_not_found(client, vault_name):
    """Test that describing a non-existent Framework raises error."""
    with pytest.raises(ClientError) as exc:
        client.describe_framework(
            FrameworkName="fake-id",
        )
    assert exc.value.response["Error"]["Code"] in (
        "ResourceNotFoundException",
        "ResourcePolicyNotFoundException",
        "NotFoundException",
        "EntityNotFoundException",
        "InvalidRequestException",
        "NoSuchEntity",
    )


def test_legal_hold_lifecycle(client, vault_name):
    """Test LegalHold CRUD lifecycle."""
    # CREATE
    create_resp = client.create_legal_hold(
        Title="test-string",
        Description="test-string",
    )
    assert isinstance(create_resp.get("LegalHoldId"), str)
    assert len(create_resp.get("LegalHoldId", "")) > 0
    assert create_resp.get("CreationDate") is not None
    assert isinstance(create_resp.get("RecoveryPointSelection", {}), dict)

    legal_hold_id = create_resp["LegalHoldId"]

    # DESCRIBE
    desc_resp = client.get_legal_hold(
        LegalHoldId=legal_hold_id,
    )
    assert isinstance(desc_resp.get("LegalHoldId"), str)
    assert len(desc_resp.get("LegalHoldId", "")) > 0
    assert isinstance(desc_resp.get("RecoveryPointSelection", {}), dict)


def test_legal_hold_not_found(client, vault_name):
    """Test that describing a non-existent LegalHold raises error."""
    with pytest.raises(ClientError) as exc:
        client.get_legal_hold(
            LegalHoldId="fake-id",
        )
    assert exc.value.response["Error"]["Code"] in (
        "ResourceNotFoundException",
        "ResourcePolicyNotFoundException",
        "NotFoundException",
        "EntityNotFoundException",
        "InvalidRequestException",
        "NoSuchEntity",
    )


def test_report_job_lifecycle(client, vault_name):
    """Test ReportJob CRUD lifecycle."""
    # CREATE
    create_resp = client.start_report_job(
        ReportPlanName="test-name-1",
    )
    assert isinstance(create_resp.get("ReportJobId"), str)
    assert len(create_resp.get("ReportJobId", "")) > 0

    report_job_id = create_resp["ReportJobId"]

    # DESCRIBE
    desc_resp = client.describe_report_job(
        ReportJobId=report_job_id,
    )
    assert isinstance(desc_resp.get("ReportJob", {}), dict)


def test_report_job_not_found(client, vault_name):
    """Test that describing a non-existent ReportJob raises error."""
    with pytest.raises(ClientError) as exc:
        client.describe_report_job(
            ReportJobId="fake-id",
        )
    assert exc.value.response["Error"]["Code"] in (
        "ResourceNotFoundException",
        "ResourcePolicyNotFoundException",
        "NotFoundException",
        "EntityNotFoundException",
        "InvalidRequestException",
        "NoSuchEntity",
    )


def test_report_plan_lifecycle(client, vault_name):
    """Test ReportPlan CRUD lifecycle."""
    # CREATE
    create_resp = client.create_report_plan(
        ReportPlanName="test-name-1",
        ReportDeliveryChannel={"S3BucketName": "test-name-1"},
        ReportSetting={"ReportTemplate": "test-string"},
    )
    assert isinstance(create_resp.get("ReportPlanName"), str)
    assert len(create_resp.get("ReportPlanName", "")) > 0
    assert create_resp.get("CreationTime") is not None

    # DESCRIBE
    desc_resp = client.describe_report_plan(
        ReportPlanName="test-name-1",
    )
    assert isinstance(desc_resp.get("ReportPlan", {}), dict)

    # DELETE
    client.delete_report_plan(
        ReportPlanName="test-name-1",
    )

    # DESCRIBE after DELETE should fail
    with pytest.raises(ClientError) as exc:
        client.describe_report_plan(
            ReportPlanName="test-name-1",
        )
    assert exc.value.response["Error"]["Code"] in (
        "ResourceNotFoundException",
        "ResourcePolicyNotFoundException",
        "NotFoundException",
        "EntityNotFoundException",
        "InvalidRequestException",
        "NoSuchEntity",
    )


def test_report_plan_not_found(client, vault_name):
    """Test that describing a non-existent ReportPlan raises error."""
    with pytest.raises(ClientError) as exc:
        client.describe_report_plan(
            ReportPlanName="fake-id",
        )
    assert exc.value.response["Error"]["Code"] in (
        "ResourceNotFoundException",
        "ResourcePolicyNotFoundException",
        "NotFoundException",
        "EntityNotFoundException",
        "InvalidRequestException",
        "NoSuchEntity",
    )


def test_restore_job_lifecycle(client, vault_name):
    """Test RestoreJob CRUD lifecycle."""
    # CREATE
    create_resp = client.start_restore_job(
        RecoveryPointArn="arn:aws:iam::123456789012:role/test-role",
        Metadata={},
    )
    assert isinstance(create_resp.get("RestoreJobId"), str)
    assert len(create_resp.get("RestoreJobId", "")) > 0

    restore_job_id = create_resp["RestoreJobId"]

    # DESCRIBE
    desc_resp = client.describe_restore_job(
        RestoreJobId=restore_job_id,
    )
    assert isinstance(desc_resp.get("RestoreJobId"), str)
    assert len(desc_resp.get("RestoreJobId", "")) > 0
    assert isinstance(desc_resp.get("CreatedBy", {}), dict)


def test_restore_job_not_found(client, vault_name):
    """Test that describing a non-existent RestoreJob raises error."""
    with pytest.raises(ClientError) as exc:
        client.describe_restore_job(
            RestoreJobId="fake-id",
        )
    assert exc.value.response["Error"]["Code"] in (
        "ResourceNotFoundException",
        "ResourcePolicyNotFoundException",
        "NotFoundException",
        "EntityNotFoundException",
        "InvalidRequestException",
        "NoSuchEntity",
    )


def test_restore_testing_plan_lifecycle(client, vault_name):
    """Test RestoreTestingPlan CRUD lifecycle."""
    # CREATE
    create_resp = client.create_restore_testing_plan(
        RestoreTestingPlan={
            "RecoveryPointSelection": {},
            "RestoreTestingPlanName": "test-name-1",
            "ScheduleExpression": "test-string",
        },
    )
    assert create_resp.get("CreationTime") is not None
    assert isinstance(create_resp.get("RestoreTestingPlanArn"), str)
    assert isinstance(create_resp.get("RestoreTestingPlanName"), str)
    assert len(create_resp.get("RestoreTestingPlanName", "")) > 0

    restore_testing_plan_name = create_resp["RestoreTestingPlanName"]

    # DESCRIBE
    desc_resp = client.get_restore_testing_plan(
        RestoreTestingPlanName=restore_testing_plan_name,
    )
    assert isinstance(desc_resp.get("RestoreTestingPlan", {}), dict)

    # DELETE
    client.delete_restore_testing_plan(
        RestoreTestingPlanName=restore_testing_plan_name,
    )

    # DESCRIBE after DELETE should fail
    with pytest.raises(ClientError) as exc:
        client.get_restore_testing_plan(
            RestoreTestingPlanName=restore_testing_plan_name,
        )
    assert exc.value.response["Error"]["Code"] in (
        "ResourceNotFoundException",
        "ResourcePolicyNotFoundException",
        "NotFoundException",
        "EntityNotFoundException",
        "InvalidRequestException",
        "NoSuchEntity",
    )


def test_restore_testing_plan_not_found(client, vault_name):
    """Test that describing a non-existent RestoreTestingPlan raises error."""
    with pytest.raises(ClientError) as exc:
        client.get_restore_testing_plan(
            RestoreTestingPlanName="fake-id",
        )
    assert exc.value.response["Error"]["Code"] in (
        "ResourceNotFoundException",
        "ResourcePolicyNotFoundException",
        "NotFoundException",
        "EntityNotFoundException",
        "InvalidRequestException",
        "NoSuchEntity",
    )


def test_restore_testing_selection_lifecycle(client, vault_name):
    """Test RestoreTestingSelection CRUD lifecycle."""
    # CREATE
    create_resp = client.create_restore_testing_selection(
        RestoreTestingPlanName="test-name-1",
        RestoreTestingSelection={
            "IamRoleArn": "arn:aws:iam::123456789012:role/test-role",
            "ProtectedResourceType": "test-string",
            "RestoreTestingSelectionName": "test-name-1",
        },
    )
    assert create_resp.get("CreationTime") is not None
    assert isinstance(create_resp.get("RestoreTestingPlanArn"), str)
    assert isinstance(create_resp.get("RestoreTestingPlanName"), str)
    assert len(create_resp.get("RestoreTestingPlanName", "")) > 0
    assert isinstance(create_resp.get("RestoreTestingSelectionName"), str)
    assert len(create_resp.get("RestoreTestingSelectionName", "")) > 0

    restore_testing_selection_name = create_resp["RestoreTestingSelectionName"]

    # DESCRIBE
    desc_resp = client.get_restore_testing_selection(
        RestoreTestingPlanName="test-name-1",
        RestoreTestingSelectionName=restore_testing_selection_name,
    )
    assert isinstance(desc_resp.get("RestoreTestingSelection", {}), dict)

    # DELETE
    client.delete_restore_testing_selection(
        RestoreTestingPlanName="test-name-1",
        RestoreTestingSelectionName=restore_testing_selection_name,
    )

    # DESCRIBE after DELETE should fail
    with pytest.raises(ClientError) as exc:
        client.get_restore_testing_selection(
            RestoreTestingPlanName="test-name-1",
            RestoreTestingSelectionName=restore_testing_selection_name,
        )
    assert exc.value.response["Error"]["Code"] in (
        "ResourceNotFoundException",
        "ResourcePolicyNotFoundException",
        "NotFoundException",
        "EntityNotFoundException",
        "InvalidRequestException",
        "NoSuchEntity",
    )


def test_restore_testing_selection_not_found(client, vault_name):
    """Test that describing a non-existent RestoreTestingSelection raises error."""
    with pytest.raises(ClientError) as exc:
        client.get_restore_testing_selection(
            RestoreTestingPlanName="fake-id",
            RestoreTestingSelectionName="fake-id",
        )
    assert exc.value.response["Error"]["Code"] in (
        "ResourceNotFoundException",
        "ResourcePolicyNotFoundException",
        "NotFoundException",
        "EntityNotFoundException",
        "InvalidRequestException",
        "NoSuchEntity",
    )


def test_scan_job_lifecycle(client, vault_name):
    """Test ScanJob CRUD lifecycle."""
    # CREATE
    create_resp = client.start_scan_job(
        BackupVaultName=vault_name,
        IamRoleArn="arn:aws:iam::123456789012:role/test-role",
        MalwareScanner="GUARDDUTY",
        RecoveryPointArn="arn:aws:iam::123456789012:role/test-role",
        ScanMode="FULL_SCAN",
        ScannerRoleArn="arn:aws:iam::123456789012:role/test-role",
    )
    assert create_resp.get("CreationDate") is not None
    assert isinstance(create_resp.get("ScanJobId"), str)
    assert len(create_resp.get("ScanJobId", "")) > 0

    scan_job_id = create_resp["ScanJobId"]

    # DESCRIBE
    desc_resp = client.describe_scan_job(
        ScanJobId=scan_job_id,
    )
    assert isinstance(desc_resp.get("CreatedBy", {}), dict)
    assert isinstance(desc_resp.get("ScanJobId"), str)
    assert len(desc_resp.get("ScanJobId", "")) > 0
    assert isinstance(desc_resp.get("ScanResult", {}), dict)


def test_scan_job_not_found(client, vault_name):
    """Test that describing a non-existent ScanJob raises error."""
    with pytest.raises(ClientError) as exc:
        client.describe_scan_job(
            ScanJobId="fake-id",
        )
    assert exc.value.response["Error"]["Code"] in (
        "ResourceNotFoundException",
        "ResourcePolicyNotFoundException",
        "NotFoundException",
        "EntityNotFoundException",
        "InvalidRequestException",
        "NoSuchEntity",
    )


def test_tiering_configuration_lifecycle(client, vault_name):
    """Test TieringConfiguration CRUD lifecycle."""
    # CREATE
    create_resp = client.create_tiering_configuration(
        TieringConfiguration={
            "TieringConfigurationName": "test-name-1",
            "BackupVaultName": "test-name-1",
            "ResourceSelection": [
                {
                    "Resources": ["test-string"],
                    "TieringDownSettingsInDays": 60,
                    "ResourceType": "test-string",
                }
            ],
        },
    )
    assert isinstance(create_resp.get("TieringConfigurationName"), str)
    assert len(create_resp.get("TieringConfigurationName", "")) > 0
    assert create_resp.get("CreationTime") is not None

    tiering_configuration_name = create_resp["TieringConfigurationName"]

    # DESCRIBE
    desc_resp = client.get_tiering_configuration(
        TieringConfigurationName=tiering_configuration_name,
    )
    assert isinstance(desc_resp.get("TieringConfiguration", {}), dict)

    # DELETE
    client.delete_tiering_configuration(
        TieringConfigurationName=tiering_configuration_name,
    )

    # DESCRIBE after DELETE should fail
    with pytest.raises(ClientError) as exc:
        client.get_tiering_configuration(
            TieringConfigurationName=tiering_configuration_name,
        )
    assert exc.value.response["Error"]["Code"] in (
        "ResourceNotFoundException",
        "ResourcePolicyNotFoundException",
        "NotFoundException",
        "EntityNotFoundException",
        "InvalidRequestException",
        "NoSuchEntity",
    )


def test_tiering_configuration_not_found(client, vault_name):
    """Test that describing a non-existent TieringConfiguration raises error."""
    with pytest.raises(ClientError) as exc:
        client.get_tiering_configuration(
            TieringConfigurationName="fake-id",
        )
    assert exc.value.response["Error"]["Code"] in (
        "ResourceNotFoundException",
        "ResourcePolicyNotFoundException",
        "NotFoundException",
        "EntityNotFoundException",
        "InvalidRequestException",
        "NoSuchEntity",
    )
