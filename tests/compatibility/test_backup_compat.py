"""AWS Backup compatibility tests."""

import uuid

import pytest
from botocore.exceptions import ClientError

from tests.compatibility.conftest import make_client


@pytest.fixture
def backup():
    return make_client("backup")


def _unique(prefix):
    return f"{prefix}-{uuid.uuid4().hex[:8]}"


class TestBackupVaultOperations:
    def test_create_and_describe_vault(self, backup):
        vault_name = _unique("vault")
        backup.create_backup_vault(BackupVaultName=vault_name)

        resp = backup.describe_backup_vault(BackupVaultName=vault_name)
        assert resp["BackupVaultName"] == vault_name
        assert "BackupVaultArn" in resp

        backup.delete_backup_vault(BackupVaultName=vault_name)

    def test_list_backup_vaults(self, backup):
        vault_name = _unique("vault")
        backup.create_backup_vault(BackupVaultName=vault_name)

        resp = backup.list_backup_vaults()
        vault_names = [v["BackupVaultName"] for v in resp["BackupVaultList"]]
        assert vault_name in vault_names

        backup.delete_backup_vault(BackupVaultName=vault_name)

    def test_describe_nonexistent_vault(self, backup):
        with pytest.raises(ClientError) as exc:
            backup.describe_backup_vault(BackupVaultName=_unique("no-such"))
        assert exc.value.response["Error"]["Code"] == "ResourceNotFoundException"

    def test_tag_and_untag_vault(self, backup):
        vault_name = _unique("vault")
        backup.create_backup_vault(BackupVaultName=vault_name)

        desc = backup.describe_backup_vault(BackupVaultName=vault_name)
        vault_arn = desc["BackupVaultArn"]

        backup.tag_resource(ResourceArn=vault_arn, Tags={"env": "test", "team": "dev"})
        tags = backup.list_tags(ResourceArn=vault_arn)["Tags"]
        assert tags["env"] == "test"
        assert tags["team"] == "dev"

        backup.untag_resource(ResourceArn=vault_arn, TagKeyList=["team"])
        tags = backup.list_tags(ResourceArn=vault_arn)["Tags"]
        assert tags.get("env") == "test"
        assert "team" not in tags

        backup.delete_backup_vault(BackupVaultName=vault_name)

    def test_delete_vault(self, backup):
        vault_name = _unique("vault")
        backup.create_backup_vault(BackupVaultName=vault_name)
        backup.delete_backup_vault(BackupVaultName=vault_name)

        with pytest.raises(ClientError) as exc:
            backup.describe_backup_vault(BackupVaultName=vault_name)
        assert exc.value.response["Error"]["Code"] == "ResourceNotFoundException"


class TestBackupPlanOperations:
    def test_create_and_get_plan(self, backup):
        vault_name = _unique("vault")
        backup.create_backup_vault(BackupVaultName=vault_name)

        plan_name = _unique("plan")
        resp = backup.create_backup_plan(
            BackupPlan={
                "BackupPlanName": plan_name,
                "Rules": [
                    {
                        "RuleName": "daily",
                        "TargetBackupVaultName": vault_name,
                        "ScheduleExpression": "cron(0 12 * * ? *)",
                    }
                ],
            }
        )
        plan_id = resp["BackupPlanId"]
        assert plan_id

        got = backup.get_backup_plan(BackupPlanId=plan_id)
        assert got["BackupPlan"]["BackupPlanName"] == plan_name
        assert len(got["BackupPlan"]["Rules"]) == 1
        assert got["BackupPlan"]["Rules"][0]["RuleName"] == "daily"

        backup.delete_backup_plan(BackupPlanId=plan_id)
        backup.delete_backup_vault(BackupVaultName=vault_name)

    def test_list_backup_plans(self, backup):
        vault_name = _unique("vault")
        backup.create_backup_vault(BackupVaultName=vault_name)

        plan_name = _unique("plan")
        resp = backup.create_backup_plan(
            BackupPlan={
                "BackupPlanName": plan_name,
                "Rules": [
                    {
                        "RuleName": "weekly",
                        "TargetBackupVaultName": vault_name,
                        "ScheduleExpression": "cron(0 12 ? * SUN *)",
                    }
                ],
            }
        )
        plan_id = resp["BackupPlanId"]

        plans = backup.list_backup_plans()
        plan_names = [p["BackupPlanName"] for p in plans["BackupPlansList"]]
        assert plan_name in plan_names

        backup.delete_backup_plan(BackupPlanId=plan_id)
        backup.delete_backup_vault(BackupVaultName=vault_name)

    def test_get_nonexistent_plan(self, backup):
        with pytest.raises(ClientError) as exc:
            backup.get_backup_plan(BackupPlanId="00000000-0000-0000-0000-000000000000")
        assert exc.value.response["Error"]["Code"] == "ResourceNotFoundException"

    def test_delete_plan(self, backup):
        vault_name = _unique("vault")
        backup.create_backup_vault(BackupVaultName=vault_name)

        resp = backup.create_backup_plan(
            BackupPlan={
                "BackupPlanName": _unique("plan"),
                "Rules": [
                    {
                        "RuleName": "daily",
                        "TargetBackupVaultName": vault_name,
                        "ScheduleExpression": "cron(0 12 * * ? *)",
                    }
                ],
            }
        )
        plan_id = resp["BackupPlanId"]

        backup.delete_backup_plan(BackupPlanId=plan_id)

        # Deleted plans still retrievable (with DeletionDate) but absent from list
        plans = backup.list_backup_plans()
        plan_ids = [p["BackupPlanId"] for p in plans.get("BackupPlansList", [])]
        assert plan_id not in plan_ids

        got = backup.get_backup_plan(BackupPlanId=plan_id)
        assert got.get("DeletionDate") is not None

        backup.delete_backup_vault(BackupVaultName=vault_name)


class TestBackupAutoCoverage:
    """Auto-generated coverage tests for backup."""

    @pytest.fixture
    def client(self):
        return make_client("backup")

    def test_list_report_plans(self, client):
        """ListReportPlans returns a response."""
        resp = client.list_report_plans()
        assert "ReportPlans" in resp
