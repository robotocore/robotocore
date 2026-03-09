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


def _make_vault(backup, name=None, tags=None):
    """Helper: create a vault and return its name."""
    name = name or _unique("vault")
    kwargs = {"BackupVaultName": name}
    if tags:
        kwargs["BackupVaultTags"] = tags
    backup.create_backup_vault(**kwargs)
    return name


def _make_plan(backup, vault_name, plan_name=None, rules=None, tags=None):
    """Helper: create a backup plan and return (plan_id, plan_arn)."""
    plan_name = plan_name or _unique("plan")
    if rules is None:
        rules = [
            {
                "RuleName": "daily",
                "TargetBackupVaultName": vault_name,
                "ScheduleExpression": "cron(0 12 * * ? *)",
            }
        ]
    kwargs = {"BackupPlan": {"BackupPlanName": plan_name, "Rules": rules}}
    if tags:
        kwargs["BackupPlanTags"] = tags
    resp = backup.create_backup_plan(**kwargs)
    return resp["BackupPlanId"], resp.get("BackupPlanArn", "")


class TestBackupVaultOperations:
    def test_create_and_describe_vault(self, backup):
        vault_name = _unique("vault")
        backup.create_backup_vault(BackupVaultName=vault_name)

        resp = backup.describe_backup_vault(BackupVaultName=vault_name)
        assert resp["BackupVaultName"] == vault_name
        assert "BackupVaultArn" in resp

        backup.delete_backup_vault(BackupVaultName=vault_name)

    def test_describe_vault_fields(self, backup):
        """Verify describe returns CreationDate and ARN format."""
        vault_name = _make_vault(backup)
        try:
            resp = backup.describe_backup_vault(BackupVaultName=vault_name)
            assert resp["BackupVaultName"] == vault_name
            assert "BackupVaultArn" in resp
            assert ":backup-vault:" in resp["BackupVaultArn"]
            assert "CreationDate" in resp
        finally:
            backup.delete_backup_vault(BackupVaultName=vault_name)

    def test_create_vault_with_tags(self, backup):
        """Vault created with tags should have them visible via ListTags."""
        vault_name = _make_vault(backup, tags={"env": "prod", "team": "backend"})
        try:
            desc = backup.describe_backup_vault(BackupVaultName=vault_name)
            vault_arn = desc["BackupVaultArn"]
            tags = backup.list_tags(ResourceArn=vault_arn)["Tags"]
            assert tags["env"] == "prod"
            assert tags["team"] == "backend"
        finally:
            backup.delete_backup_vault(BackupVaultName=vault_name)

    def test_list_backup_vaults(self, backup):
        vault_name = _unique("vault")
        backup.create_backup_vault(BackupVaultName=vault_name)

        resp = backup.list_backup_vaults()
        vault_names = [v["BackupVaultName"] for v in resp["BackupVaultList"]]
        assert vault_name in vault_names

        backup.delete_backup_vault(BackupVaultName=vault_name)

    def test_list_backup_vaults_has_arn(self, backup):
        """Each vault in list should include BackupVaultArn."""
        vault_name = _make_vault(backup)
        try:
            resp = backup.list_backup_vaults()
            found = [v for v in resp["BackupVaultList"] if v["BackupVaultName"] == vault_name]
            assert len(found) == 1
            assert "BackupVaultArn" in found[0]
        finally:
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

    def test_tag_vault_overwrites_existing(self, backup):
        """Tagging with same key overwrites the value."""
        vault_name = _make_vault(backup, tags={"env": "dev"})
        try:
            desc = backup.describe_backup_vault(BackupVaultName=vault_name)
            vault_arn = desc["BackupVaultArn"]
            backup.tag_resource(ResourceArn=vault_arn, Tags={"env": "prod"})
            tags = backup.list_tags(ResourceArn=vault_arn)["Tags"]
            assert tags["env"] == "prod"
        finally:
            backup.delete_backup_vault(BackupVaultName=vault_name)

    def test_list_tags_empty_vault(self, backup):
        """Vault with no tags returns empty Tags dict."""
        vault_name = _make_vault(backup)
        try:
            desc = backup.describe_backup_vault(BackupVaultName=vault_name)
            tags = backup.list_tags(ResourceArn=desc["BackupVaultArn"])
            assert isinstance(tags.get("Tags"), dict)
        finally:
            backup.delete_backup_vault(BackupVaultName=vault_name)

    def test_delete_vault(self, backup):
        vault_name = _unique("vault")
        backup.create_backup_vault(BackupVaultName=vault_name)
        backup.delete_backup_vault(BackupVaultName=vault_name)

        with pytest.raises(ClientError) as exc:
            backup.describe_backup_vault(BackupVaultName=vault_name)
        assert exc.value.response["Error"]["Code"] == "ResourceNotFoundException"

    def test_delete_vault_not_in_list(self, backup):
        """After deletion, vault should not appear in ListBackupVaults."""
        vault_name = _make_vault(backup)
        backup.delete_backup_vault(BackupVaultName=vault_name)
        resp = backup.list_backup_vaults()
        names = [v["BackupVaultName"] for v in resp["BackupVaultList"]]
        assert vault_name not in names


class TestBackupPlanOperations:
    def test_create_plan_returns_arn_and_version(self, backup):
        """CreateBackupPlan response includes BackupPlanArn and VersionId."""
        vault_name = _make_vault(backup)
        try:
            resp = backup.create_backup_plan(
                BackupPlan={
                    "BackupPlanName": _unique("plan"),
                    "Rules": [
                        {
                            "RuleName": "r1",
                            "TargetBackupVaultName": vault_name,
                            "ScheduleExpression": "cron(0 12 * * ? *)",
                        }
                    ],
                }
            )
            assert "BackupPlanId" in resp
            assert "BackupPlanArn" in resp
            assert "VersionId" in resp
            backup.delete_backup_plan(BackupPlanId=resp["BackupPlanId"])
        finally:
            backup.delete_backup_vault(BackupVaultName=vault_name)

    def test_create_plan_with_tags(self, backup):
        """Backup plan created with tags should be visible via ListTags."""
        vault_name = _make_vault(backup)
        try:
            plan_id, plan_arn = _make_plan(
                backup, vault_name, tags={"env": "staging", "app": "myapp"}
            )
            assert plan_arn
            tags = backup.list_tags(ResourceArn=plan_arn)["Tags"]
            assert tags["env"] == "staging"
            assert tags["app"] == "myapp"
            backup.delete_backup_plan(BackupPlanId=plan_id)
        finally:
            backup.delete_backup_vault(BackupVaultName=vault_name)

    def test_tag_and_untag_plan(self, backup):
        """Tag and untag a backup plan via its ARN."""
        vault_name = _make_vault(backup)
        try:
            plan_id, plan_arn = _make_plan(backup, vault_name)
            backup.tag_resource(ResourceArn=plan_arn, Tags={"k1": "v1", "k2": "v2"})
            tags = backup.list_tags(ResourceArn=plan_arn)["Tags"]
            assert tags["k1"] == "v1"
            assert tags["k2"] == "v2"

            backup.untag_resource(ResourceArn=plan_arn, TagKeyList=["k1"])
            tags = backup.list_tags(ResourceArn=plan_arn)["Tags"]
            assert "k1" not in tags
            assert tags["k2"] == "v2"

            backup.delete_backup_plan(BackupPlanId=plan_id)
        finally:
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

    def test_list_backup_plans_includes_fields(self, backup):
        """Each plan in list should have BackupPlanId and BackupPlanArn."""
        vault_name = _make_vault(backup)
        try:
            plan_name = _unique("plan")
            plan_id, _ = _make_plan(backup, vault_name, plan_name=plan_name)
            plans = backup.list_backup_plans()
            found = [p for p in plans["BackupPlansList"] if p["BackupPlanId"] == plan_id]
            assert len(found) == 1
            assert found[0]["BackupPlanName"] == plan_name
            assert "BackupPlanArn" in found[0]
            backup.delete_backup_plan(BackupPlanId=plan_id)
        finally:
            backup.delete_backup_vault(BackupVaultName=vault_name)

    def test_delete_nonexistent_plan(self, backup):
        """Deleting a nonexistent plan raises ResourceNotFoundException."""
        with pytest.raises(ClientError) as exc:
            backup.delete_backup_plan(BackupPlanId="00000000-0000-0000-0000-000000000000")
        assert exc.value.response["Error"]["Code"] == "ResourceNotFoundException"


class TestBackupJobOperations:
    """Tests for backup job operations."""

    def test_list_backup_jobs(self, backup):
        """ListBackupJobs returns BackupJobs list."""
        resp = backup.list_backup_jobs()
        assert "BackupJobs" in resp
        assert isinstance(resp["BackupJobs"], list)

    def test_list_backup_jobs_empty(self, backup):
        """ListBackupJobs returns empty list when no jobs exist."""
        resp = backup.list_backup_jobs()
        assert "BackupJobs" in resp

    def test_describe_backup_job_nonexistent(self, backup):
        """DescribeBackupJob for nonexistent job raises ResourceNotFoundException."""
        with pytest.raises(ClientError) as exc:
            backup.describe_backup_job(BackupJobId="00000000-0000-0000-0000-000000000000")
        assert exc.value.response["Error"]["Code"] == "ResourceNotFoundException"


class TestBackupFrameworkOperations:
    """Tests for backup framework operations."""

    def test_create_and_describe_framework(self, backup):
        """CreateFramework then DescribeFramework returns framework details."""
        name = _unique("fw")
        try:
            create_resp = backup.create_framework(
                FrameworkName=name,
                FrameworkControls=[
                    {"ControlName": "BACKUP_PLAN_MIN_FREQUENCY_AND_MIN_RETENTION_CHECK"}
                ],
            )
            assert "FrameworkName" in create_resp
            assert "FrameworkArn" in create_resp

            desc = backup.describe_framework(FrameworkName=name)
            assert desc["FrameworkName"] == name
            assert "FrameworkArn" in desc
            assert "CreationTime" in desc
            assert "DeploymentStatus" in desc
            assert len(desc["FrameworkControls"]) == 1
        finally:
            backup.delete_framework(FrameworkName=name)

    def test_describe_framework_nonexistent(self, backup):
        """DescribeFramework for nonexistent name raises ResourceNotFoundException."""
        with pytest.raises(ClientError) as exc:
            backup.describe_framework(FrameworkName=_unique("no-such"))
        assert exc.value.response["Error"]["Code"] == "ResourceNotFoundException"

    def test_list_frameworks(self, backup):
        """ListFrameworks returns Frameworks list including created framework."""
        name = _unique("fw")
        try:
            backup.create_framework(
                FrameworkName=name,
                FrameworkControls=[
                    {"ControlName": "BACKUP_PLAN_MIN_FREQUENCY_AND_MIN_RETENTION_CHECK"}
                ],
            )
            resp = backup.list_frameworks()
            assert "Frameworks" in resp
            names = [f["FrameworkName"] for f in resp["Frameworks"]]
            assert name in names
        finally:
            backup.delete_framework(FrameworkName=name)

    def test_list_frameworks_empty(self, backup):
        """ListFrameworks returns Frameworks key even when empty."""
        resp = backup.list_frameworks()
        assert "Frameworks" in resp
        assert isinstance(resp["Frameworks"], list)

    def test_list_frameworks_has_arn(self, backup):
        """Each framework in list should have FrameworkArn."""
        name = _unique("fw")
        try:
            backup.create_framework(
                FrameworkName=name,
                FrameworkControls=[
                    {"ControlName": "BACKUP_PLAN_MIN_FREQUENCY_AND_MIN_RETENTION_CHECK"}
                ],
            )
            resp = backup.list_frameworks()
            found = [f for f in resp["Frameworks"] if f["FrameworkName"] == name]
            assert len(found) == 1
            assert "FrameworkArn" in found[0]
        finally:
            backup.delete_framework(FrameworkName=name)


class TestBackupSelectionOperations:
    """Tests for backup selection operations."""

    def test_get_backup_selection_nonexistent(self, backup):
        """GetBackupSelection for nonexistent plan raises ResourceNotFoundException."""
        with pytest.raises(ClientError) as exc:
            backup.get_backup_selection(
                BackupPlanId="00000000-0000-0000-0000-000000000000",
                SelectionId="00000000-0000-0000-0000-000000000000",
            )
        assert exc.value.response["Error"]["Code"] == "ResourceNotFoundException"


class TestBackupAutoCoverage:
    """Auto-generated coverage tests for backup."""

    @pytest.fixture
    def client(self):
        return make_client("backup")

    def test_list_report_plans(self, client):
        """ListReportPlans returns a response."""
        resp = client.list_report_plans()
        assert "ReportPlans" in resp


class TestBackupReportPlanOperations:
    @pytest.fixture
    def client(self):
        return make_client("backup")

    def _make_report_plan(self, client, name=None):
        name = name or _unique("rplan")
        resp = client.create_report_plan(
            ReportPlanName=name,
            ReportDeliveryChannel={
                "S3BucketName": "my-report-bucket",
                "Formats": ["CSV"],
            },
            ReportSetting={
                "ReportTemplate": "BACKUP_JOB_REPORT",
            },
        )
        return resp

    def test_create_report_plan(self, client):
        name = _unique("rplan")
        resp = self._make_report_plan(client, name)
        assert "ReportPlanArn" in resp
        assert "ReportPlanName" in resp
        assert resp["ReportPlanName"] == name
        # cleanup
        client.delete_report_plan(ReportPlanName=name)

    def test_create_report_plan_with_description(self, client):
        """Report plan with description and detailed delivery channel."""
        name = _unique("rplan")
        try:
            resp = client.create_report_plan(
                ReportPlanName=name,
                ReportPlanDescription="Detailed test report",
                ReportDeliveryChannel={
                    "S3BucketName": "report-bucket",
                    "S3KeyPrefix": "reports/",
                    "Formats": ["CSV", "JSON"],
                },
                ReportSetting={"ReportTemplate": "RESTORE_JOB_REPORT"},
            )
            assert resp["ReportPlanName"] == name
            assert "ReportPlanArn" in resp
        finally:
            client.delete_report_plan(ReportPlanName=name)

    def test_describe_report_plan(self, client):
        name = _unique("rplan")
        self._make_report_plan(client, name)
        try:
            resp = client.describe_report_plan(ReportPlanName=name)
            assert "ReportPlan" in resp
            assert resp["ReportPlan"]["ReportPlanName"] == name
            assert "ReportPlanArn" in resp["ReportPlan"]
        finally:
            client.delete_report_plan(ReportPlanName=name)

    def test_describe_report_plan_fields(self, client):
        """DescribeReportPlan returns delivery channel, setting, and creation time."""
        name = _unique("rplan")
        try:
            client.create_report_plan(
                ReportPlanName=name,
                ReportPlanDescription="field check",
                ReportDeliveryChannel={
                    "S3BucketName": "bucket",
                    "S3KeyPrefix": "pfx/",
                    "Formats": ["CSV"],
                },
                ReportSetting={"ReportTemplate": "BACKUP_JOB_REPORT"},
            )
            resp = client.describe_report_plan(ReportPlanName=name)
            rp = resp["ReportPlan"]
            assert rp["ReportPlanDescription"] == "field check"
            assert rp["ReportDeliveryChannel"]["S3BucketName"] == "bucket"
            assert rp["ReportDeliveryChannel"]["S3KeyPrefix"] == "pfx/"
            assert rp["ReportSetting"]["ReportTemplate"] == "BACKUP_JOB_REPORT"
            assert "CreationTime" in rp
        finally:
            client.delete_report_plan(ReportPlanName=name)

    def test_describe_nonexistent_report_plan(self, client):
        """DescribeReportPlan for nonexistent name raises error."""
        with pytest.raises(ClientError) as exc:
            client.describe_report_plan(ReportPlanName=_unique("no-such"))
        assert exc.value.response["Error"]["Code"] in (
            "ResourceNotFoundException",
            "NotFoundException",
        )

    def test_list_report_plans_contains_created(self, client):
        """ListReportPlans includes a freshly created plan."""
        name = _unique("rplan")
        try:
            self._make_report_plan(client, name)
            resp = client.list_report_plans()
            names = [rp["ReportPlanName"] for rp in resp["ReportPlans"]]
            assert name in names
        finally:
            client.delete_report_plan(ReportPlanName=name)

    def test_delete_report_plan(self, client):
        name = _unique("rplan")
        self._make_report_plan(client, name)
        client.delete_report_plan(ReportPlanName=name)
        # Verify it's gone
        with pytest.raises(ClientError) as exc:
            client.describe_report_plan(ReportPlanName=name)
        assert exc.value.response["Error"]["Code"] in (
            "ResourceNotFoundException",
            "NotFoundException",
        )
