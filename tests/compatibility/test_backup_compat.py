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

    def test_update_backup_plan(self, backup):
        """UpdateBackupPlan changes the plan name and schedule."""
        vault_name = _make_vault(backup)
        try:
            plan_id, _ = _make_plan(backup, vault_name, plan_name=_unique("plan"))
            update_resp = backup.update_backup_plan(
                BackupPlanId=plan_id,
                BackupPlan={
                    "BackupPlanName": "updated-plan",
                    "Rules": [
                        {
                            "RuleName": "r1",
                            "TargetBackupVaultName": vault_name,
                            "ScheduleExpression": "cron(0 6 * * ? *)",
                        }
                    ],
                },
            )
            assert update_resp["BackupPlanId"] == plan_id
            assert "VersionId" in update_resp
            assert "BackupPlanArn" in update_resp

            # Verify updated name appears in list
            plans = backup.list_backup_plans()
            found = [p for p in plans["BackupPlansList"] if p["BackupPlanId"] == plan_id]
            assert len(found) == 1
            assert found[0]["BackupPlanName"] == "updated-plan"
            backup.delete_backup_plan(BackupPlanId=plan_id)
        finally:
            backup.delete_backup_vault(BackupVaultName=vault_name)

    def test_update_backup_plan_returns_new_version(self, backup):
        """Each update should produce a new VersionId."""
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
            plan_id = resp["BackupPlanId"]
            v1 = resp["VersionId"]

            update_resp = backup.update_backup_plan(
                BackupPlanId=plan_id,
                BackupPlan={
                    "BackupPlanName": "v2-plan",
                    "Rules": [
                        {
                            "RuleName": "r1",
                            "TargetBackupVaultName": vault_name,
                            "ScheduleExpression": "cron(0 6 * * ? *)",
                        }
                    ],
                },
            )
            v2 = update_resp["VersionId"]
            assert v1 != v2
            backup.delete_backup_plan(BackupPlanId=plan_id)
        finally:
            backup.delete_backup_vault(BackupVaultName=vault_name)


class TestBackupVaultLockOperations:
    """Tests for vault lock configuration operations."""

    def test_put_and_describe_vault_lock(self, backup):
        """PutBackupVaultLockConfiguration sets lock, visible in DescribeBackupVault."""
        vault_name = _make_vault(backup)
        try:
            backup.put_backup_vault_lock_configuration(
                BackupVaultName=vault_name,
                MinRetentionDays=7,
                MaxRetentionDays=365,
            )
            desc = backup.describe_backup_vault(BackupVaultName=vault_name)
            assert desc["Locked"] is True
            assert desc["MinRetentionDays"] == 7
            assert desc["MaxRetentionDays"] == 365
        finally:
            backup.delete_backup_vault_lock_configuration(BackupVaultName=vault_name)
            backup.delete_backup_vault(BackupVaultName=vault_name)

    def test_delete_vault_lock_configuration(self, backup):
        """DeleteBackupVaultLockConfiguration removes the lock."""
        vault_name = _make_vault(backup)
        try:
            backup.put_backup_vault_lock_configuration(
                BackupVaultName=vault_name,
                MinRetentionDays=1,
                MaxRetentionDays=90,
            )
            backup.delete_backup_vault_lock_configuration(BackupVaultName=vault_name)
            desc = backup.describe_backup_vault(BackupVaultName=vault_name)
            assert desc.get("Locked") is False
        finally:
            backup.delete_backup_vault(BackupVaultName=vault_name)

    def test_vault_lock_min_only(self, backup):
        """PutBackupVaultLockConfiguration with only MinRetentionDays."""
        vault_name = _make_vault(backup)
        try:
            backup.put_backup_vault_lock_configuration(
                BackupVaultName=vault_name,
                MinRetentionDays=30,
            )
            desc = backup.describe_backup_vault(BackupVaultName=vault_name)
            assert desc["Locked"] is True
            assert desc["MinRetentionDays"] == 30
        finally:
            backup.delete_backup_vault_lock_configuration(BackupVaultName=vault_name)
            backup.delete_backup_vault(BackupVaultName=vault_name)


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

    def test_start_backup_job(self, backup):
        """StartBackupJob creates a job and returns BackupJobId."""
        vault_name = _make_vault(backup)
        try:
            resp = backup.start_backup_job(
                BackupVaultName=vault_name,
                ResourceArn="arn:aws:dynamodb:us-east-1:123456789012:table/my-table",
                IamRoleArn="arn:aws:iam::123456789012:role/backup-role",
            )
            assert "BackupJobId" in resp
            assert "CreationDate" in resp
            assert "RecoveryPointArn" in resp
        finally:
            backup.delete_backup_vault(BackupVaultName=vault_name)

    def test_start_and_describe_backup_job(self, backup):
        """StartBackupJob then DescribeBackupJob returns job details."""
        vault_name = _make_vault(backup)
        try:
            start_resp = backup.start_backup_job(
                BackupVaultName=vault_name,
                ResourceArn="arn:aws:dynamodb:us-east-1:123456789012:table/t1",
                IamRoleArn="arn:aws:iam::123456789012:role/backup-role",
            )
            job_id = start_resp["BackupJobId"]

            desc = backup.describe_backup_job(BackupJobId=job_id)
            assert desc["BackupJobId"] == job_id
            assert desc["BackupVaultName"] == vault_name
            assert "State" in desc
        finally:
            backup.delete_backup_vault(BackupVaultName=vault_name)

    def test_start_backup_job_appears_in_list(self, backup):
        """A started backup job should appear in ListBackupJobs."""
        vault_name = _make_vault(backup)
        try:
            start_resp = backup.start_backup_job(
                BackupVaultName=vault_name,
                ResourceArn="arn:aws:dynamodb:us-east-1:123456789012:table/t2",
                IamRoleArn="arn:aws:iam::123456789012:role/backup-role",
            )
            job_id = start_resp["BackupJobId"]

            jobs = backup.list_backup_jobs()
            job_ids = [j["BackupJobId"] for j in jobs["BackupJobs"]]
            assert job_id in job_ids
        finally:
            backup.delete_backup_vault(BackupVaultName=vault_name)

    def test_stop_backup_job_completed(self, backup):
        """StopBackupJob on a completed job raises InvalidRequestException."""
        vault_name = _make_vault(backup)
        try:
            start_resp = backup.start_backup_job(
                BackupVaultName=vault_name,
                ResourceArn="arn:aws:dynamodb:us-east-1:123456789012:table/t3",
                IamRoleArn="arn:aws:iam::123456789012:role/backup-role",
            )
            job_id = start_resp["BackupJobId"]
            with pytest.raises(ClientError) as exc:
                backup.stop_backup_job(BackupJobId=job_id)
            assert "InvalidRequestException" in exc.value.response["Error"]["Code"]
        finally:
            backup.delete_backup_vault(BackupVaultName=vault_name)


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

    def test_update_framework(self, backup):
        """UpdateFramework adds controls and reflects in DescribeFramework."""
        name = _unique("fw")
        try:
            backup.create_framework(
                FrameworkName=name,
                FrameworkControls=[
                    {"ControlName": "BACKUP_PLAN_MIN_FREQUENCY_AND_MIN_RETENTION_CHECK"}
                ],
            )
            update_resp = backup.update_framework(
                FrameworkName=name,
                FrameworkControls=[
                    {"ControlName": "BACKUP_PLAN_MIN_FREQUENCY_AND_MIN_RETENTION_CHECK"},
                    {"ControlName": "BACKUP_RESOURCES_PROTECTED_BY_BACKUP_PLAN"},
                ],
            )
            assert update_resp["FrameworkName"] == name
            assert "FrameworkArn" in update_resp

            desc = backup.describe_framework(FrameworkName=name)
            assert len(desc["FrameworkControls"]) == 2
            control_names = [c["ControlName"] for c in desc["FrameworkControls"]]
            assert "BACKUP_RESOURCES_PROTECTED_BY_BACKUP_PLAN" in control_names
        finally:
            backup.delete_framework(FrameworkName=name)

    def test_delete_framework_removes_from_list(self, backup):
        """After DeleteFramework, the framework should not appear in ListFrameworks."""
        name = _unique("fw")
        backup.create_framework(
            FrameworkName=name,
            FrameworkControls=[
                {"ControlName": "BACKUP_PLAN_MIN_FREQUENCY_AND_MIN_RETENTION_CHECK"}
            ],
        )
        backup.delete_framework(FrameworkName=name)
        resp = backup.list_frameworks()
        names = [f["FrameworkName"] for f in resp["Frameworks"]]
        assert name not in names


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

    def test_delete_backup_selection_nonexistent(self, backup):
        """DeleteBackupSelection for nonexistent selection returns 200 (idempotent)."""
        resp = backup.delete_backup_selection(
            BackupPlanId="00000000-0000-0000-0000-000000000000",
            SelectionId="00000000-0000-0000-0000-000000000000",
        )
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200


class TestBackupJobAdvanced:
    """Additional tests for backup job operations."""

    def test_list_backup_jobs_by_vault_name(self, backup):
        """ListBackupJobs filtered by vault name returns matching jobs."""
        vault_name = _make_vault(backup)
        try:
            backup.start_backup_job(
                BackupVaultName=vault_name,
                ResourceArn="arn:aws:dynamodb:us-east-1:123456789012:table/tbl1",
                IamRoleArn="arn:aws:iam::123456789012:role/backup-role",
            )
            jobs = backup.list_backup_jobs(ByBackupVaultName=vault_name)
            assert "BackupJobs" in jobs
            assert len(jobs["BackupJobs"]) >= 1
            for job in jobs["BackupJobs"]:
                assert job["BackupVaultName"] == vault_name
        finally:
            backup.delete_backup_vault(BackupVaultName=vault_name)

    def test_start_backup_job_with_lifecycle(self, backup):
        """StartBackupJob with Lifecycle returns BackupJobId."""
        vault_name = _make_vault(backup)
        try:
            resp = backup.start_backup_job(
                BackupVaultName=vault_name,
                ResourceArn="arn:aws:dynamodb:us-east-1:123456789012:table/lc-tbl",
                IamRoleArn="arn:aws:iam::123456789012:role/backup-role",
                Lifecycle={"DeleteAfterDays": 30},
            )
            assert "BackupJobId" in resp
            assert "RecoveryPointArn" in resp
            assert "CreationDate" in resp
        finally:
            backup.delete_backup_vault(BackupVaultName=vault_name)

    def test_describe_backup_job_has_resource_arn(self, backup):
        """DescribeBackupJob returns ResourceArn and IamRoleArn."""
        vault_name = _make_vault(backup)
        resource_arn = "arn:aws:dynamodb:us-east-1:123456789012:table/desc-tbl"
        try:
            start_resp = backup.start_backup_job(
                BackupVaultName=vault_name,
                ResourceArn=resource_arn,
                IamRoleArn="arn:aws:iam::123456789012:role/backup-role",
            )
            desc = backup.describe_backup_job(BackupJobId=start_resp["BackupJobId"])
            assert desc["ResourceArn"] == resource_arn
            assert "IamRoleArn" in desc
            assert "CreationDate" in desc
        finally:
            backup.delete_backup_vault(BackupVaultName=vault_name)


class TestBackupFrameworkAdvanced:
    """Additional framework tests."""

    def test_create_framework_with_description(self, backup):
        """CreateFramework with description and tags."""
        name = _unique("fw")
        try:
            resp = backup.create_framework(
                FrameworkName=name,
                FrameworkDescription="A test framework",
                FrameworkControls=[
                    {"ControlName": "BACKUP_PLAN_MIN_FREQUENCY_AND_MIN_RETENTION_CHECK"}
                ],
                FrameworkTags={"env": "test"},
            )
            assert resp["FrameworkName"] == name
            assert "FrameworkArn" in resp

            desc = backup.describe_framework(FrameworkName=name)
            assert desc["FrameworkDescription"] == "A test framework"
        finally:
            backup.delete_framework(FrameworkName=name)

    def test_framework_tags_via_create(self, backup):
        """Tags passed at creation are visible via ListTags."""
        name = _unique("fw")
        try:
            backup.create_framework(
                FrameworkName=name,
                FrameworkControls=[
                    {"ControlName": "BACKUP_PLAN_MIN_FREQUENCY_AND_MIN_RETENTION_CHECK"}
                ],
                FrameworkTags={"project": "robotocore", "tier": "free"},
            )
            desc = backup.describe_framework(FrameworkName=name)
            tags = backup.list_tags(ResourceArn=desc["FrameworkArn"])["Tags"]
            assert tags["project"] == "robotocore"
            assert tags["tier"] == "free"
        finally:
            backup.delete_framework(FrameworkName=name)

    def test_framework_multiple_controls(self, backup):
        """CreateFramework with multiple controls."""
        name = _unique("fw")
        try:
            backup.create_framework(
                FrameworkName=name,
                FrameworkControls=[
                    {"ControlName": "BACKUP_PLAN_MIN_FREQUENCY_AND_MIN_RETENTION_CHECK"},
                    {"ControlName": "BACKUP_RESOURCES_PROTECTED_BY_BACKUP_PLAN"},
                ],
            )
            desc = backup.describe_framework(FrameworkName=name)
            assert len(desc["FrameworkControls"]) == 2
            control_names = {c["ControlName"] for c in desc["FrameworkControls"]}
            assert "BACKUP_PLAN_MIN_FREQUENCY_AND_MIN_RETENTION_CHECK" in control_names
            assert "BACKUP_RESOURCES_PROTECTED_BY_BACKUP_PLAN" in control_names
        finally:
            backup.delete_framework(FrameworkName=name)


class TestBackupPlanAdvanced:
    """Additional backup plan tests."""

    def test_create_plan_multiple_rules(self, backup):
        """BackupPlan with multiple rules."""
        vault_name = _make_vault(backup)
        try:
            resp = backup.create_backup_plan(
                BackupPlan={
                    "BackupPlanName": _unique("plan"),
                    "Rules": [
                        {
                            "RuleName": "daily",
                            "TargetBackupVaultName": vault_name,
                            "ScheduleExpression": "cron(0 12 * * ? *)",
                        },
                        {
                            "RuleName": "weekly",
                            "TargetBackupVaultName": vault_name,
                            "ScheduleExpression": "cron(0 12 ? * SUN *)",
                        },
                    ],
                }
            )
            assert "BackupPlanId" in resp
            backup.delete_backup_plan(BackupPlanId=resp["BackupPlanId"])
        finally:
            backup.delete_backup_vault(BackupVaultName=vault_name)

    def test_update_plan_changes_name(self, backup):
        """UpdateBackupPlan changes the plan name visible in list."""
        vault_name = _make_vault(backup)
        try:
            plan_id, _ = _make_plan(backup, vault_name)
            new_name = _unique("updated")
            backup.update_backup_plan(
                BackupPlanId=plan_id,
                BackupPlan={
                    "BackupPlanName": new_name,
                    "Rules": [
                        {
                            "RuleName": "r1",
                            "TargetBackupVaultName": vault_name,
                            "ScheduleExpression": "cron(0 6 * * ? *)",
                        }
                    ],
                },
            )
            plans = backup.list_backup_plans()
            found = [p for p in plans["BackupPlansList"] if p["BackupPlanId"] == plan_id]
            assert len(found) == 1
            assert found[0]["BackupPlanName"] == new_name
            backup.delete_backup_plan(BackupPlanId=plan_id)
        finally:
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


class TestBackupDescribeOperations:
    """Tests for various describe/get/list operations."""

    @pytest.fixture
    def client(self):
        return make_client("backup")

    def test_describe_global_settings(self, client):
        """DescribeGlobalSettings returns GlobalSettings dict."""
        resp = client.describe_global_settings()
        assert "GlobalSettings" in resp
        assert isinstance(resp["GlobalSettings"], dict)

    def test_describe_region_settings(self, client):
        """DescribeRegionSettings returns ResourceTypeOptInPreference."""
        resp = client.describe_region_settings()
        assert "ResourceTypeOptInPreference" in resp
        assert isinstance(resp["ResourceTypeOptInPreference"], dict)

    def test_get_supported_resource_types(self, client):
        """GetSupportedResourceTypes returns ResourceTypes list."""
        resp = client.get_supported_resource_types()
        assert "ResourceTypes" in resp
        assert isinstance(resp["ResourceTypes"], list)

    def test_list_backup_plan_templates(self, client):
        """ListBackupPlanTemplates returns BackupPlanTemplatesList."""
        resp = client.list_backup_plan_templates()
        assert "BackupPlanTemplatesList" in resp
        assert isinstance(resp["BackupPlanTemplatesList"], list)

    def test_list_copy_jobs_empty(self, client):
        """ListCopyJobs returns CopyJobs list."""
        resp = client.list_copy_jobs()
        assert "CopyJobs" in resp
        assert isinstance(resp["CopyJobs"], list)

    def test_list_protected_resources_empty(self, client):
        """ListProtectedResources returns Results list."""
        resp = client.list_protected_resources()
        assert "Results" in resp
        assert isinstance(resp["Results"], list)

    def test_list_restore_testing_plans_empty(self, client):
        """ListRestoreTestingPlans returns RestoreTestingPlans list."""
        resp = client.list_restore_testing_plans()
        assert "RestoreTestingPlans" in resp
        assert isinstance(resp["RestoreTestingPlans"], list)

    def test_describe_copy_job_nonexistent(self, client):
        """DescribeCopyJob for nonexistent job raises ResourceNotFoundException."""
        with pytest.raises(ClientError) as exc:
            client.describe_copy_job(CopyJobId="00000000-0000-0000-0000-000000000000")
        assert exc.value.response["Error"]["Code"] == "ResourceNotFoundException"

    def test_describe_protected_resource_nonexistent(self, client):
        """DescribeProtectedResource for nonexistent resource raises error."""
        with pytest.raises(ClientError) as exc:
            client.describe_protected_resource(
                ResourceArn="arn:aws:dynamodb:us-east-1:123456789012:table/fake"
            )
        assert exc.value.response["Error"]["Code"] == "ResourceNotFoundException"

    def test_describe_recovery_point_nonexistent(self, client):
        """DescribeRecoveryPoint for nonexistent vault raises ResourceNotFoundException."""
        with pytest.raises(ClientError) as exc:
            client.describe_recovery_point(
                BackupVaultName="fake-vault",
                RecoveryPointArn="arn:aws:backup:us-east-1:123456789012:recovery-point:fake",
            )
        assert exc.value.response["Error"]["Code"] == "ResourceNotFoundException"

    def test_get_backup_vault_access_policy_nonexistent(self, client):
        """GetBackupVaultAccessPolicy for nonexistent vault raises error."""
        with pytest.raises(ClientError) as exc:
            client.get_backup_vault_access_policy(BackupVaultName="fake-vault")
        assert exc.value.response["Error"]["Code"] == "ResourceNotFoundException"

    def test_get_backup_vault_notifications_nonexistent(self, client):
        """GetBackupVaultNotifications for nonexistent vault raises error."""
        with pytest.raises(ClientError) as exc:
            client.get_backup_vault_notifications(BackupVaultName="fake-vault")
        assert exc.value.response["Error"]["Code"] == "ResourceNotFoundException"

    def test_get_restore_testing_plan_nonexistent(self, client):
        """GetRestoreTestingPlan for nonexistent plan raises error."""
        with pytest.raises(ClientError) as exc:
            client.get_restore_testing_plan(RestoreTestingPlanName="fake-plan")
        assert exc.value.response["Error"]["Code"] == "ResourceNotFoundException"

    def test_get_restore_testing_selection_nonexistent(self, client):
        """GetRestoreTestingSelection for nonexistent plan raises error."""
        with pytest.raises(ClientError) as exc:
            client.get_restore_testing_selection(
                RestoreTestingPlanName="fake-plan",
                RestoreTestingSelectionName="fake-sel",
            )
        assert exc.value.response["Error"]["Code"] == "ResourceNotFoundException"

    def test_list_restore_testing_selections(self, client):
        """ListRestoreTestingSelections returns selections for a plan."""
        plan_name = _unique("plan")
        client.create_restore_testing_plan(
            RestoreTestingPlan={
                "RestoreTestingPlanName": plan_name,
                "ScheduleExpression": "cron(0 12 * * ? *)",
                "RecoveryPointSelection": {
                    "Algorithm": "LATEST_WITHIN_WINDOW",
                    "IncludeVaults": ["*"],
                    "RecoveryPointTypes": ["CONTINUOUS"],
                    "SelectionWindowDays": 7,
                },
            }
        )
        try:
            resp = client.list_restore_testing_selections(
                RestoreTestingPlanName=plan_name,
            )
            assert "RestoreTestingSelections" in resp
            assert isinstance(resp["RestoreTestingSelections"], list)
        finally:
            client.delete_restore_testing_plan(RestoreTestingPlanName=plan_name)


class TestBackupVaultPolicyAndNotifications:
    """Tests for vault access policy and notification operations."""

    @pytest.fixture
    def client(self):
        return make_client("backup")

    def test_put_and_delete_backup_vault_access_policy(self, client):
        """PutBackupVaultAccessPolicy sets a policy, DeleteBackupVaultAccessPolicy removes it."""
        vault_name = _make_vault(client)
        try:
            import json

            policy = json.dumps(
                {
                    "Version": "2012-10-17",
                    "Statement": [
                        {
                            "Effect": "Deny",
                            "Principal": "*",
                            "Action": "backup:DeleteRecoveryPoint",
                            "Resource": "*",
                        }
                    ],
                }
            )
            resp = client.put_backup_vault_access_policy(BackupVaultName=vault_name, Policy=policy)
            assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200

            # Verify the policy is set
            get_resp = client.get_backup_vault_access_policy(BackupVaultName=vault_name)
            assert "Policy" in get_resp

            # Delete the policy
            del_resp = client.delete_backup_vault_access_policy(BackupVaultName=vault_name)
            assert del_resp["ResponseMetadata"]["HTTPStatusCode"] == 200

            # Verify it's gone
            with pytest.raises(ClientError) as exc:
                client.get_backup_vault_access_policy(BackupVaultName=vault_name)
            assert exc.value.response["Error"]["Code"] == "ResourceNotFoundException"
        finally:
            client.delete_backup_vault(BackupVaultName=vault_name)

    def test_put_and_delete_backup_vault_notifications(self, client):
        """PutBackupVaultNotifications sets notifications, delete removes."""
        vault_name = _make_vault(client)
        try:
            resp = client.put_backup_vault_notifications(
                BackupVaultName=vault_name,
                SNSTopicArn="arn:aws:sns:us-east-1:123456789012:backup-notifications",
                BackupVaultEvents=["BACKUP_JOB_COMPLETED", "RESTORE_JOB_COMPLETED"],
            )
            assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200

            # Verify notifications are set
            get_resp = client.get_backup_vault_notifications(BackupVaultName=vault_name)
            expected_arn = "arn:aws:sns:us-east-1:123456789012:backup-notifications"
            assert get_resp["SNSTopicArn"] == expected_arn
            assert "BACKUP_JOB_COMPLETED" in get_resp["BackupVaultEvents"]

            # Delete notifications
            del_resp = client.delete_backup_vault_notifications(BackupVaultName=vault_name)
            assert del_resp["ResponseMetadata"]["HTTPStatusCode"] == 200

            # Verify they're gone
            with pytest.raises(ClientError) as exc:
                client.get_backup_vault_notifications(BackupVaultName=vault_name)
            assert exc.value.response["Error"]["Code"] == "ResourceNotFoundException"
        finally:
            client.delete_backup_vault(BackupVaultName=vault_name)

    def test_delete_backup_vault_access_policy_nonexistent(self, client):
        """DeleteBackupVaultAccessPolicy on nonexistent vault raises error."""
        with pytest.raises(ClientError) as exc:
            client.delete_backup_vault_access_policy(BackupVaultName="fake-vault-nonexistent")
        assert exc.value.response["Error"]["Code"] == "ResourceNotFoundException"

    def test_delete_backup_vault_notifications_nonexistent(self, client):
        """DeleteBackupVaultNotifications on nonexistent vault raises error."""
        with pytest.raises(ClientError) as exc:
            client.delete_backup_vault_notifications(BackupVaultName="fake-vault-nonexistent")
        assert exc.value.response["Error"]["Code"] == "ResourceNotFoundException"


class TestBackupRestoreTestingSelectionCRUD:
    """Tests for restore testing selection create/delete operations."""

    @pytest.fixture
    def client(self):
        return make_client("backup")

    def _make_plan(self, client, plan_name=None):
        plan_name = plan_name or _unique("plan")
        client.create_restore_testing_plan(
            RestoreTestingPlan={
                "RestoreTestingPlanName": plan_name,
                "ScheduleExpression": "cron(0 12 * * ? *)",
                "RecoveryPointSelection": {
                    "Algorithm": "LATEST_WITHIN_WINDOW",
                    "IncludeVaults": ["*"],
                    "RecoveryPointTypes": ["CONTINUOUS"],
                    "SelectionWindowDays": 7,
                },
            }
        )
        return plan_name

    def test_create_and_delete_restore_testing_selection(self, client):
        """CreateRestoreTestingSelection creates, delete removes."""
        plan_name = self._make_plan(client)
        sel_name = _unique("sel")
        try:
            resp = client.create_restore_testing_selection(
                RestoreTestingPlanName=plan_name,
                RestoreTestingSelection={
                    "RestoreTestingSelectionName": sel_name,
                    "ProtectedResourceType": "DynamoDB",
                    "IamRoleArn": "arn:aws:iam::123456789012:role/backup-role",
                },
            )
            assert "RestoreTestingSelectionName" in resp
            assert resp["RestoreTestingSelectionName"] == sel_name

            # Delete it
            del_resp = client.delete_restore_testing_selection(
                RestoreTestingPlanName=plan_name,
                RestoreTestingSelectionName=sel_name,
            )
            assert del_resp["ResponseMetadata"]["HTTPStatusCode"] in (200, 204)
        finally:
            client.delete_restore_testing_plan(RestoreTestingPlanName=plan_name)

    def test_delete_restore_testing_selection_nonexistent(self, client):
        """DeleteRestoreTestingSelection for nonexistent plan succeeds silently."""
        resp = client.delete_restore_testing_selection(
            RestoreTestingPlanName="fake-plan",
            RestoreTestingSelectionName="fake-sel",
        )
        assert resp["ResponseMetadata"]["HTTPStatusCode"] in (200, 204)


class TestBackupSettingsOperations:
    """Tests for UpdateGlobalSettings and UpdateRegionSettings."""

    @pytest.fixture
    def client(self):
        return make_client("backup")

    def test_update_global_settings(self, client):
        """UpdateGlobalSettings sets global settings."""
        resp = client.update_global_settings(GlobalSettings={"isCrossAccountBackupEnabled": "true"})
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200

        # Verify
        get_resp = client.describe_global_settings()
        assert "GlobalSettings" in get_resp

    def test_update_region_settings(self, client):
        """UpdateRegionSettings modifies region opt-in settings."""
        resp = client.update_region_settings(ResourceTypeOptInPreference={"DynamoDB": True})
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200

        # Verify
        get_resp = client.describe_region_settings()
        assert "ResourceTypeOptInPreference" in get_resp


class TestBackupCopyJobOperations:
    """Tests for StartCopyJob."""

    @pytest.fixture
    def client(self):
        return make_client("backup")

    def test_start_copy_job_returns_copy_job_id(self, client):
        """StartCopyJob returns a CopyJobId."""
        vault_name = _make_vault(client)
        try:
            resp = client.start_copy_job(
                RecoveryPointArn="arn:aws:backup:us-east-1:123456789012:recovery-point:fake-rp",
                SourceBackupVaultName=vault_name,
                DestinationBackupVaultArn=(
                    "arn:aws:backup:us-east-1:123456789012:backup-vault:" + vault_name
                ),
                IamRoleArn="arn:aws:iam::123456789012:role/backup-role",
            )
            assert "CopyJobId" in resp
            assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200
        finally:
            client.delete_backup_vault(BackupVaultName=vault_name)


class TestBackupLegalHoldOperations:
    """Tests for legal hold CRUD operations."""

    def test_create_and_list_legal_holds(self, backup):
        title = _unique("hold")
        resp = backup.create_legal_hold(
            Title=title,
            Description="test legal hold",
        )
        assert "LegalHoldId" in resp
        assert resp["Title"] == title
        assert resp["Status"] in ("CREATING", "ACTIVE")
        hold_id = resp["LegalHoldId"]

        listed = backup.list_legal_holds()
        assert "LegalHolds" in listed
        hold_ids = [h["LegalHoldId"] for h in listed["LegalHolds"]]
        assert hold_id in hold_ids

        # Cancel the hold
        cancel_resp = backup.cancel_legal_hold(LegalHoldId=hold_id, CancelDescription="done")
        assert cancel_resp["ResponseMetadata"]["HTTPStatusCode"] in (200, 201)

    def test_list_legal_holds_empty(self, backup):
        resp = backup.list_legal_holds()
        assert "LegalHolds" in resp
        assert isinstance(resp["LegalHolds"], list)


class TestBackupRestoreTestingPlanOperations:
    """Tests for restore testing plan operations."""

    def test_create_and_get_restore_testing_plan(self, backup):
        name = _unique("rtp")
        resp = backup.create_restore_testing_plan(
            RestoreTestingPlan={
                "RestoreTestingPlanName": name,
                "ScheduleExpression": "cron(0 12 * * ? *)",
                "RecoveryPointSelection": {
                    "Algorithm": "LATEST_WITHIN_WINDOW",
                    "RecoveryPointTypes": ["CONTINUOUS"],
                    "IncludeVaults": ["*"],
                    "SelectionWindowDays": 7,
                },
                "StartWindowHours": 1,
            },
        )
        assert "RestoreTestingPlanName" in resp
        assert resp["RestoreTestingPlanName"] == name

        get_resp = backup.get_restore_testing_plan(RestoreTestingPlanName=name)
        plan = get_resp["RestoreTestingPlan"]
        assert plan["RestoreTestingPlanName"] == name
        assert "RestoreTestingPlanArn" in plan

        backup.delete_restore_testing_plan(RestoreTestingPlanName=name)

    def test_list_restore_testing_plans(self, backup):
        name = _unique("rtp")
        backup.create_restore_testing_plan(
            RestoreTestingPlan={
                "RestoreTestingPlanName": name,
                "ScheduleExpression": "cron(0 12 * * ? *)",
                "RecoveryPointSelection": {
                    "Algorithm": "LATEST_WITHIN_WINDOW",
                    "RecoveryPointTypes": ["CONTINUOUS"],
                    "IncludeVaults": ["*"],
                    "SelectionWindowDays": 7,
                },
                "StartWindowHours": 1,
            },
        )
        try:
            resp = backup.list_restore_testing_plans()
            assert "RestoreTestingPlans" in resp
            plan_names = [p["RestoreTestingPlanName"] for p in resp["RestoreTestingPlans"]]
            assert name in plan_names
        finally:
            backup.delete_restore_testing_plan(RestoreTestingPlanName=name)

    def test_delete_restore_testing_plan(self, backup):
        name = _unique("rtp")
        backup.create_restore_testing_plan(
            RestoreTestingPlan={
                "RestoreTestingPlanName": name,
                "ScheduleExpression": "cron(0 12 * * ? *)",
                "RecoveryPointSelection": {
                    "Algorithm": "LATEST_WITHIN_WINDOW",
                    "RecoveryPointTypes": ["CONTINUOUS"],
                    "IncludeVaults": ["*"],
                    "SelectionWindowDays": 7,
                },
                "StartWindowHours": 1,
            },
        )
        resp = backup.delete_restore_testing_plan(RestoreTestingPlanName=name)
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 204


class TestBackupAdditionalListOperations:
    """Tests for additional list operations."""

    def test_list_restore_jobs(self, backup):
        resp = backup.list_restore_jobs()
        assert "RestoreJobs" in resp
        assert isinstance(resp["RestoreJobs"], list)

    def test_list_copy_jobs(self, backup):
        resp = backup.list_copy_jobs()
        assert "CopyJobs" in resp
        assert isinstance(resp["CopyJobs"], list)


class TestBackupPlanCRUD:
    """Dedicated tests for CreateBackupPlan, GetBackupPlan, DeleteBackupPlan, UpdateBackupPlan."""

    def test_create_backup_plan_returns_id_and_arn(self, backup):
        """CreateBackupPlan returns BackupPlanId, BackupPlanArn, VersionId."""
        vault_name = _make_vault(backup)
        try:
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
            assert "BackupPlanId" in resp
            assert "BackupPlanArn" in resp
            assert "VersionId" in resp
            assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200
            backup.delete_backup_plan(BackupPlanId=resp["BackupPlanId"])
        finally:
            backup.delete_backup_vault(BackupVaultName=vault_name)

    def test_delete_backup_plan_removes_from_list(self, backup):
        """DeleteBackupPlan removes the plan from ListBackupPlans."""
        vault_name = _make_vault(backup)
        try:
            plan_id, _ = _make_plan(backup, vault_name)
            backup.delete_backup_plan(BackupPlanId=plan_id)
            plans = backup.list_backup_plans()
            plan_ids = [p["BackupPlanId"] for p in plans["BackupPlansList"]]
            assert plan_id not in plan_ids
        finally:
            backup.delete_backup_vault(BackupVaultName=vault_name)

    def test_update_backup_plan_changes_version(self, backup):
        """UpdateBackupPlan produces a new VersionId and updates the plan."""
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
            plan_id = resp["BackupPlanId"]
            v1 = resp["VersionId"]

            update_resp = backup.update_backup_plan(
                BackupPlanId=plan_id,
                BackupPlan={
                    "BackupPlanName": "updated-name",
                    "Rules": [
                        {
                            "RuleName": "r1",
                            "TargetBackupVaultName": vault_name,
                            "ScheduleExpression": "cron(0 6 * * ? *)",
                        }
                    ],
                },
            )
            assert update_resp["BackupPlanId"] == plan_id
            assert "VersionId" in update_resp
            assert update_resp["VersionId"] != v1
            backup.delete_backup_plan(BackupPlanId=plan_id)
        finally:
            backup.delete_backup_vault(BackupVaultName=vault_name)


class TestBackupPlanFromJSON:
    """Tests for GetBackupPlanFromJSON."""

    def test_get_backup_plan_from_json(self, backup):
        """GetBackupPlanFromJSON parses a JSON string into a backup plan."""
        import json

        plan_json = json.dumps(
            {
                "BackupPlanName": "from-json-plan",
                "Rules": [
                    {
                        "RuleName": "r1",
                        "TargetBackupVaultName": "Default",
                        "ScheduleExpression": "cron(0 12 * * ? *)",
                    }
                ],
            }
        )
        resp = backup.get_backup_plan_from_json(BackupPlanTemplateJson=plan_json)
        assert "BackupPlan" in resp
        plan = resp["BackupPlan"]
        assert plan["BackupPlanName"] == "from-json-plan"
        assert len(plan["Rules"]) >= 1
        assert plan["Rules"][0]["RuleName"] == "r1"

    def test_get_backup_plan_from_json_multiple_rules(self, backup):
        """GetBackupPlanFromJSON with multiple rules."""
        import json

        plan_json = json.dumps(
            {
                "BackupPlanName": "multi-rule",
                "Rules": [
                    {
                        "RuleName": "daily",
                        "TargetBackupVaultName": "Default",
                        "ScheduleExpression": "cron(0 12 * * ? *)",
                    },
                    {
                        "RuleName": "weekly",
                        "TargetBackupVaultName": "Default",
                        "ScheduleExpression": "cron(0 12 ? * SUN *)",
                    },
                ],
            }
        )
        resp = backup.get_backup_plan_from_json(BackupPlanTemplateJson=plan_json)
        assert "BackupPlan" in resp
        assert len(resp["BackupPlan"]["Rules"]) == 2


class TestBackupReportJobOperations:
    """Tests for DescribeReportJob."""

    def test_describe_report_job_nonexistent(self, backup):
        """DescribeReportJob for nonexistent job raises ResourceNotFoundException."""
        with pytest.raises(ClientError) as exc:
            backup.describe_report_job(ReportJobId="00000000-0000-0000-0000-000000000000")
        assert exc.value.response["Error"]["Code"] == "ResourceNotFoundException"


class TestBackupLegalHoldExtended:
    """Extended tests for CreateLegalHold, CancelLegalHold fields."""

    def test_create_legal_hold_returns_fields(self, backup):
        """CreateLegalHold returns LegalHoldId, Title, Status, LegalHoldArn."""
        title = _unique("hold")
        resp = backup.create_legal_hold(
            Title=title,
            Description="test hold for field check",
        )
        assert "LegalHoldId" in resp
        assert "LegalHoldArn" in resp
        assert resp["Title"] == title
        assert resp["Status"] in ("CREATING", "ACTIVE")
        assert "CreationDate" in resp

        # cleanup
        backup.cancel_legal_hold(LegalHoldId=resp["LegalHoldId"], CancelDescription="cleanup")

    def test_cancel_legal_hold_status_code(self, backup):
        """CancelLegalHold returns 200 or 201."""
        title = _unique("hold")
        resp = backup.create_legal_hold(
            Title=title,
            Description="to be cancelled",
        )
        hold_id = resp["LegalHoldId"]

        cancel_resp = backup.cancel_legal_hold(LegalHoldId=hold_id, CancelDescription="test cancel")
        assert cancel_resp["ResponseMetadata"]["HTTPStatusCode"] in (200, 201)

    def test_cancel_legal_hold_removes_from_active_list(self, backup):
        """After CancelLegalHold, the hold should not appear as active."""
        title = _unique("hold")
        resp = backup.create_legal_hold(
            Title=title,
            Description="cancel-test",
        )
        hold_id = resp["LegalHoldId"]

        backup.cancel_legal_hold(LegalHoldId=hold_id, CancelDescription="done")

        # List holds - cancelled holds may still appear but with CANCELED status
        listed = backup.list_legal_holds()
        assert "LegalHolds" in listed
        active_ids = [h["LegalHoldId"] for h in listed["LegalHolds"] if h.get("Status") == "ACTIVE"]
        assert hold_id not in active_ids

    def test_create_legal_hold_with_recovery_point_selection(self, backup):
        """CreateLegalHold with RecoveryPointSelection."""
        title = _unique("hold")
        resp = backup.create_legal_hold(
            Title=title,
            Description="hold with selection",
            RecoveryPointSelection={
                "ResourceIdentifiers": ["arn:aws:dynamodb:us-east-1:123456789012:table/my-table"],
                "VaultNames": ["Default"],
            },
        )
        assert "LegalHoldId" in resp
        assert resp["Title"] == title

        # cleanup
        backup.cancel_legal_hold(LegalHoldId=resp["LegalHoldId"], CancelDescription="cleanup")

    def test_list_legal_holds_response_key(self, backup):
        resp = backup.list_legal_holds()
        assert "LegalHolds" in resp
        assert isinstance(resp["LegalHolds"], list)

    def test_get_backup_plan_from_template(self, backup):
        # List templates first to find a valid ID
        templates = backup.list_backup_plan_templates()
        assert "BackupPlanTemplatesList" in templates
        if templates["BackupPlanTemplatesList"]:
            template_id = templates["BackupPlanTemplatesList"][0]["BackupPlanTemplateId"]
            resp = backup.get_backup_plan_from_template(
                BackupPlanTemplateId=template_id,
            )
            assert "BackupPlanDocument" in resp
            assert "BackupPlanName" in resp["BackupPlanDocument"]


class TestBackupErrorPathAdditional:
    """Additional error path tests for backup operations."""

    def test_describe_restore_job_nonexistent(self, backup):
        with pytest.raises(ClientError) as exc:
            backup.describe_restore_job(RestoreJobId="00000000-0000-0000-0000-000000000000")
        assert exc.value.response["Error"]["Code"] == "ResourceNotFoundException"

    def test_describe_copy_job_nonexistent(self, backup):
        with pytest.raises(ClientError) as exc:
            backup.describe_copy_job(CopyJobId="00000000-0000-0000-0000-000000000000")
        assert exc.value.response["Error"]["Code"] == "ResourceNotFoundException"

    def test_delete_recovery_point_nonexistent(self, backup):
        vault_name = _make_vault(backup)
        try:
            with pytest.raises(ClientError) as exc:
                backup.delete_recovery_point(
                    BackupVaultName=vault_name,
                    RecoveryPointArn="arn:aws:backup:us-east-1:123456789012:recovery-point:fake",
                )
            assert exc.value.response["Error"]["Code"] == "ResourceNotFoundException"
        finally:
            backup.delete_backup_vault(BackupVaultName=vault_name)

    def test_get_recovery_point_restore_metadata_nonexistent(self, backup):
        vault_name = _make_vault(backup)
        try:
            with pytest.raises(ClientError) as exc:
                backup.get_recovery_point_restore_metadata(
                    BackupVaultName=vault_name,
                    RecoveryPointArn="arn:aws:backup:us-east-1:123456789012:recovery-point:fake",
                )
            assert exc.value.response["Error"]["Code"] == "ResourceNotFoundException"
        finally:
            backup.delete_backup_vault(BackupVaultName=vault_name)

    def test_describe_recovery_point_nonexistent_vault(self, backup):
        with pytest.raises(ClientError) as exc:
            backup.describe_recovery_point(
                BackupVaultName="nonexistent-vault",
                RecoveryPointArn="arn:aws:backup:us-east-1:123456789012:recovery-point:fake",
            )
        assert exc.value.response["Error"]["Code"] == "ResourceNotFoundException"

    def test_get_restore_job_metadata_nonexistent(self, backup):
        with pytest.raises(ClientError) as exc:
            backup.get_restore_job_metadata(RestoreJobId="00000000-0000-0000-0000-000000000000")
        assert exc.value.response["Error"]["Code"] == "ResourceNotFoundException"

    def test_list_recovery_points_by_resource_nonexistent(self, backup):
        resp = backup.list_recovery_points_by_resource(
            ResourceArn="arn:aws:dynamodb:us-east-1:123456789012:table/nonexistent-table"
        )
        assert "RecoveryPoints" in resp
        assert isinstance(resp["RecoveryPoints"], list)

    def test_list_restore_jobs_by_protected_resource(self, backup):
        resp = backup.list_restore_jobs_by_protected_resource(
            ResourceArn="arn:aws:dynamodb:us-east-1:123456789012:table/nonexistent-table"
        )
        assert "RestoreJobs" in resp
        assert isinstance(resp["RestoreJobs"], list)

    def test_start_restore_job(self, backup):
        """StartRestoreJob creates a restore job and returns a RestoreJobId."""
        resp = backup.start_restore_job(
            RecoveryPointArn="arn:aws:backup:us-east-1:123456789012:recovery-point:test-rp",
            Metadata={"key": "value"},
            IamRoleArn="arn:aws:iam::123456789012:role/test-role",
        )
        assert "RestoreJobId" in resp
        assert isinstance(resp["RestoreJobId"], str)
        assert len(resp["RestoreJobId"]) > 0

    def test_put_restore_validation_result_nonexistent(self, backup):
        """PutRestoreValidationResult for nonexistent job raises ResourceNotFoundException."""
        with pytest.raises(ClientError) as exc:
            backup.put_restore_validation_result(
                RestoreJobId="00000000-0000-0000-0000-000000000000",
                ValidationStatus="SUCCESSFUL",
            )
        assert exc.value.response["Error"]["Code"] == "ResourceNotFoundException"

    def test_update_recovery_point_lifecycle_nonexistent(self, backup):
        """UpdateRecoveryPointLifecycle for nonexistent vault raises ResourceNotFoundException."""
        with pytest.raises(ClientError) as exc:
            backup.update_recovery_point_lifecycle(
                BackupVaultName="nonexistent-vault-xyz",
                RecoveryPointArn="arn:aws:backup:us-east-1:123456789012:recovery-point:fake",
                Lifecycle={"DeleteAfterDays": 30},
            )
        assert exc.value.response["Error"]["Code"] == "ResourceNotFoundException"


class TestBackupGlobalAndRegionSettings:
    """Tests for global settings, region settings, and supported resource types."""

    def test_describe_global_settings(self, backup):
        """DescribeGlobalSettings returns GlobalSettings map."""
        resp = backup.describe_global_settings()
        assert "GlobalSettings" in resp
        assert isinstance(resp["GlobalSettings"], dict)
        assert "isCrossAccountBackupEnabled" in resp["GlobalSettings"]

    def test_describe_region_settings(self, backup):
        """DescribeRegionSettings returns ResourceTypeOptInPreference map."""
        resp = backup.describe_region_settings()
        assert "ResourceTypeOptInPreference" in resp
        prefs = resp["ResourceTypeOptInPreference"]
        assert isinstance(prefs, dict)
        # At least some common resource types should be present
        assert "EBS" in prefs
        assert "S3" in prefs

    def test_get_supported_resource_types(self, backup):
        """GetSupportedResourceTypes returns a non-empty list of resource types."""
        resp = backup.get_supported_resource_types()
        assert "ResourceTypes" in resp
        types = resp["ResourceTypes"]
        assert isinstance(types, list)
        assert len(types) > 0
        # Common resource types should be present
        assert "EBS" in types
        assert "S3" in types


class TestBackupProtectedResources:
    """Tests for protected resource operations."""

    def test_list_protected_resources_empty(self, backup):
        """ListProtectedResources returns Results list (may be empty)."""
        resp = backup.list_protected_resources()
        assert "Results" in resp
        assert isinstance(resp["Results"], list)

    def test_describe_protected_resource_nonexistent(self, backup):
        """DescribeProtectedResource for nonexistent ARN raises ResourceNotFoundException."""
        with pytest.raises(ClientError) as exc:
            backup.describe_protected_resource(
                ResourceArn="arn:aws:ec2:us-east-1:123456789012:instance/i-nonexistent"
            )
        assert exc.value.response["Error"]["Code"] == "ResourceNotFoundException"


class TestBackupVaultAccessAndNotifications:
    """Tests for vault access policy and notification operations."""

    def test_get_backup_vault_access_policy_no_policy(self, backup):
        """GetBackupVaultAccessPolicy on vault without policy raises ResourceNotFoundException."""
        vault_name = _unique("vault")
        backup.create_backup_vault(BackupVaultName=vault_name)
        try:
            with pytest.raises(ClientError) as exc:
                backup.get_backup_vault_access_policy(BackupVaultName=vault_name)
            assert exc.value.response["Error"]["Code"] == "ResourceNotFoundException"
        finally:
            backup.delete_backup_vault(BackupVaultName=vault_name)

    def test_get_backup_vault_access_policy_nonexistent_vault(self, backup):
        """GetBackupVaultAccessPolicy on nonexistent vault raises ResourceNotFoundException."""
        with pytest.raises(ClientError) as exc:
            backup.get_backup_vault_access_policy(BackupVaultName="nonexistent-vault-xyz")
        assert exc.value.response["Error"]["Code"] == "ResourceNotFoundException"

    def test_get_backup_vault_notifications_no_notifications(self, backup):
        """GetBackupVaultNotifications on vault without config raises ResourceNotFoundException."""
        vault_name = _unique("vault")
        backup.create_backup_vault(BackupVaultName=vault_name)
        try:
            with pytest.raises(ClientError) as exc:
                backup.get_backup_vault_notifications(BackupVaultName=vault_name)
            assert exc.value.response["Error"]["Code"] == "ResourceNotFoundException"
        finally:
            backup.delete_backup_vault(BackupVaultName=vault_name)

    def test_get_backup_vault_notifications_nonexistent_vault(self, backup):
        """GetBackupVaultNotifications on nonexistent vault raises ResourceNotFoundException."""
        with pytest.raises(ClientError) as exc:
            backup.get_backup_vault_notifications(BackupVaultName="nonexistent-vault-xyz")
        assert exc.value.response["Error"]["Code"] == "ResourceNotFoundException"


class TestBackupReportPlanOpsExtended:
    """Extended tests for report plan operations."""

    def test_list_report_plans_empty(self, backup):
        """ListReportPlans returns ReportPlans list."""
        resp = backup.list_report_plans()
        assert "ReportPlans" in resp
        assert isinstance(resp["ReportPlans"], list)

    def test_describe_report_plan_nonexistent(self, backup):
        """DescribeReportPlan for nonexistent plan raises ResourceNotFoundException."""
        with pytest.raises(ClientError) as exc:
            backup.describe_report_plan(ReportPlanName="nonexistent-report-plan")
        assert exc.value.response["Error"]["Code"] == "ResourceNotFoundException"


class TestBackupRestoreTestingSelectionOperations:
    """Tests for restore testing selection operations."""

    def test_get_restore_testing_selection_nonexistent(self, backup):
        """GetRestoreTestingSelection for nonexistent selection raises error."""
        vault_name = _unique("vault")
        plan_name = _unique("rtp")
        backup.create_backup_vault(BackupVaultName=vault_name)
        try:
            backup.create_restore_testing_plan(
                RestoreTestingPlan={
                    "RestoreTestingPlanName": plan_name,
                    "ScheduleExpression": "cron(0 12 * * ? *)",
                    "RecoveryPointSelection": {
                        "Algorithm": "LATEST_WITHIN_WINDOW",
                        "RecoveryPointTypes": ["CONTINUOUS"],
                        "IncludeVaults": [
                            f"arn:aws:backup:us-east-1:123456789012:backup-vault:{vault_name}"
                        ],
                    },
                }
            )
            with pytest.raises(ClientError) as exc:
                backup.get_restore_testing_selection(
                    RestoreTestingPlanName=plan_name,
                    RestoreTestingSelectionName="nonexistent-selection",
                )
            assert exc.value.response["Error"]["Code"] == "ResourceNotFoundException"
            backup.delete_restore_testing_plan(RestoreTestingPlanName=plan_name)
        finally:
            backup.delete_backup_vault(BackupVaultName=vault_name)


class TestBackupLogicallyAirGappedVault:
    """Tests for logically air-gapped backup vault."""

    def test_create_logically_air_gapped_vault(self, backup):
        """CreateLogicallyAirGappedBackupVault returns vault name and ARN."""
        name = _unique("airgap")
        resp = backup.create_logically_air_gapped_backup_vault(
            BackupVaultName=name,
            MinRetentionDays=7,
            MaxRetentionDays=365,
        )
        assert resp["BackupVaultName"] == name
        assert "BackupVaultArn" in resp
        assert "VaultState" in resp
        backup.delete_backup_vault(BackupVaultName=name)


class TestBackupLegalHoldCRUD:
    """Tests for legal hold CRUD operations."""

    def test_create_legal_hold(self, backup):
        """CreateLegalHold returns hold ID and ARN."""
        resp = backup.create_legal_hold(
            Title="test-hold",
            Description="test legal hold",
            RecoveryPointSelection={
                "ResourceIdentifiers": ["*"],
                "VaultNames": ["*"],
            },
        )
        assert "LegalHoldId" in resp
        assert "LegalHoldArn" in resp
        assert resp["Title"] == "test-hold"
        backup.cancel_legal_hold(
            LegalHoldId=resp["LegalHoldId"],
            CancelDescription="cleanup",
            RetainRecordInDays=1,
        )

    def test_cancel_legal_hold(self, backup):
        """CancelLegalHold cancels an active hold."""
        resp = backup.create_legal_hold(
            Title="cancel-test",
            Description="will cancel",
            RecoveryPointSelection={
                "ResourceIdentifiers": ["*"],
                "VaultNames": ["*"],
            },
        )
        hold_id = resp["LegalHoldId"]
        cancel_resp = backup.cancel_legal_hold(
            LegalHoldId=hold_id,
            CancelDescription="done testing",
            RetainRecordInDays=1,
        )
        assert cancel_resp["ResponseMetadata"]["HTTPStatusCode"] == 200

    def test_list_legal_holds_includes_created(self, backup):
        """ListLegalHolds includes a newly created hold."""
        resp = backup.create_legal_hold(
            Title="list-test",
            Description="for listing",
            RecoveryPointSelection={
                "ResourceIdentifiers": ["*"],
                "VaultNames": ["*"],
            },
        )
        hold_id = resp["LegalHoldId"]
        list_resp = backup.list_legal_holds()
        assert "LegalHolds" in list_resp
        hold_ids = [h["LegalHoldId"] for h in list_resp["LegalHolds"]]
        assert hold_id in hold_ids
        backup.cancel_legal_hold(
            LegalHoldId=hold_id,
            CancelDescription="cleanup",
            RetainRecordInDays=1,
        )


class TestBackupUpdateOperations:
    """Tests for update operations on restore testing plans and selections."""

    def _make_restore_testing_plan(self, backup):
        """Helper to create a vault and restore testing plan."""
        vault_name = _unique("vault")
        plan_name = _unique("rtp")
        backup.create_backup_vault(BackupVaultName=vault_name)
        backup.create_restore_testing_plan(
            RestoreTestingPlan={
                "RestoreTestingPlanName": plan_name,
                "ScheduleExpression": "cron(0 12 * * ? *)",
                "RecoveryPointSelection": {
                    "Algorithm": "LATEST_WITHIN_WINDOW",
                    "RecoveryPointTypes": ["CONTINUOUS"],
                    "IncludeVaults": [
                        f"arn:aws:backup:us-east-1:123456789012:backup-vault:{vault_name}"
                    ],
                },
            }
        )
        return vault_name, plan_name

    def test_update_restore_testing_plan(self, backup):
        """UpdateRestoreTestingPlan updates the plan."""
        vault_name, plan_name = self._make_restore_testing_plan(backup)
        try:
            resp = backup.update_restore_testing_plan(
                RestoreTestingPlan={
                    "RecoveryPointSelection": {
                        "Algorithm": "LATEST_WITHIN_WINDOW",
                        "RecoveryPointTypes": ["CONTINUOUS"],
                        "IncludeVaults": [
                            f"arn:aws:backup:us-east-1:123456789012:backup-vault:{vault_name}"
                        ],
                    },
                },
                RestoreTestingPlanName=plan_name,
            )
            assert resp["RestoreTestingPlanName"] == plan_name
            assert "RestoreTestingPlanArn" in resp
            assert "UpdateTime" in resp
        finally:
            backup.delete_restore_testing_plan(RestoreTestingPlanName=plan_name)
            backup.delete_backup_vault(BackupVaultName=vault_name)

    def test_update_restore_testing_selection(self, backup):
        """UpdateRestoreTestingSelection updates the selection."""
        vault_name, plan_name = self._make_restore_testing_plan(backup)
        sel_name = _unique("sel")
        try:
            backup.create_restore_testing_selection(
                RestoreTestingPlanName=plan_name,
                RestoreTestingSelection={
                    "RestoreTestingSelectionName": sel_name,
                    "ProtectedResourceType": "DynamoDB",
                    "IamRoleArn": "arn:aws:iam::123456789012:role/test",
                },
            )
            resp = backup.update_restore_testing_selection(
                RestoreTestingPlanName=plan_name,
                RestoreTestingSelectionName=sel_name,
                RestoreTestingSelection={
                    "IamRoleArn": "arn:aws:iam::123456789012:role/updated",
                },
            )
            assert resp["RestoreTestingSelectionName"] == sel_name
            assert "UpdateTime" in resp
            backup.delete_restore_testing_selection(
                RestoreTestingPlanName=plan_name,
                RestoreTestingSelectionName=sel_name,
            )
        finally:
            backup.delete_restore_testing_plan(RestoreTestingPlanName=plan_name)
            backup.delete_backup_vault(BackupVaultName=vault_name)

    def test_update_report_plan(self, backup):
        """UpdateReportPlan updates the plan description."""
        name = _unique("rp")
        backup.create_report_plan(
            ReportPlanName=name,
            ReportDeliveryChannel={"S3BucketName": "my-bucket", "Formats": ["CSV"]},
            ReportSetting={"ReportTemplate": "RESTORE_JOB_REPORT"},
        )
        try:
            resp = backup.update_report_plan(
                ReportPlanName=name,
                ReportPlanDescription="updated description",
            )
            assert resp["ReportPlanName"] == name
            assert "ReportPlanArn" in resp
        finally:
            backup.delete_report_plan(ReportPlanName=name)


class TestBackupPlanJsonParsing:
    """Tests for backup plan JSON parsing operations."""

    @pytest.fixture
    def backup(self):
        return make_client("backup")

    def test_get_backup_plan_from_json(self, backup):
        """GetBackupPlanFromJSON parses a JSON string into a plan."""
        import json

        plan_json = json.dumps(
            {
                "BackupPlanName": "json-plan",
                "Rules": [
                    {
                        "RuleName": "daily",
                        "TargetBackupVaultName": "Default",
                        "ScheduleExpression": "cron(0 12 * * ? *)",
                    }
                ],
            }
        )
        resp = backup.get_backup_plan_from_json(BackupPlanTemplateJson=plan_json)
        assert "BackupPlan" in resp
        assert resp["BackupPlan"]["BackupPlanName"] == "json-plan"
        assert len(resp["BackupPlan"]["Rules"]) == 1


class TestBackupAdditionalOperations:
    """Tests for additional backup operations."""

    @pytest.fixture
    def backup(self):
        return make_client("backup")

    def test_list_backup_job_summaries(self, backup):
        """ListBackupJobSummaries returns a list of summaries."""
        resp = backup.list_backup_job_summaries()
        assert "BackupJobSummaries" in resp

    def test_list_copy_job_summaries(self, backup):
        """ListCopyJobSummaries returns a list of summaries."""
        resp = backup.list_copy_job_summaries()
        assert "CopyJobSummaries" in resp

    def test_list_restore_job_summaries(self, backup):
        """ListRestoreJobSummaries returns a list of summaries."""
        resp = backup.list_restore_job_summaries()
        assert "RestoreJobSummaries" in resp

    def test_list_report_jobs(self, backup):
        """ListReportJobs returns a list of report jobs."""
        resp = backup.list_report_jobs()
        assert "ReportJobs" in resp

    def test_list_recovery_points_by_legal_hold(self, backup):
        """ListRecoveryPointsByLegalHold returns recovery points for a legal hold."""
        # Create a legal hold to get a valid ID
        name = _unique("hold")
        resp = backup.create_legal_hold(
            Title=name,
            Description="test hold",
        )
        hold_id = resp["LegalHoldId"]
        try:
            resp = backup.list_recovery_points_by_legal_hold(LegalHoldId=hold_id)
            assert "RecoveryPoints" in resp
        finally:
            backup.cancel_legal_hold(LegalHoldId=hold_id, CancelDescription="cleanup")

    def test_describe_report_job_not_found(self, backup):
        """DescribeReportJob with a fake ID returns an error."""
        with pytest.raises(ClientError) as exc_info:
            backup.describe_report_job(ReportJobId="fake-report-job-id-12345")
        err = exc_info.value.response["Error"]
        assert err["Code"] in ("ResourceNotFoundException", "NotFoundException")

    def test_disassociate_recovery_point_not_found(self, backup):
        """DisassociateRecoveryPoint with fake IDs returns an error."""
        vault_name = _make_vault(backup)
        try:
            with pytest.raises(ClientError) as exc_info:
                backup.disassociate_recovery_point(
                    BackupVaultName=vault_name,
                    RecoveryPointArn="arn:aws:backup:us-east-1:123456789012:recovery-point:fake-rp-id",
                )
            err = exc_info.value.response["Error"]
            assert err["Code"] in (
                "ResourceNotFoundException",
                "NotFoundException",
                "InvalidParameterValueException",
            )
        finally:
            backup.delete_backup_vault(BackupVaultName=vault_name)

    def test_disassociate_recovery_point_from_parent_not_found(self, backup):
        """DisassociateRecoveryPointFromParent with fake IDs returns an error."""
        vault_name = _make_vault(backup)
        try:
            with pytest.raises(ClientError) as exc_info:
                backup.disassociate_recovery_point_from_parent(
                    BackupVaultName=vault_name,
                    RecoveryPointArn="arn:aws:backup:us-east-1:123456789012:recovery-point:fake-rp-id",
                )
            err = exc_info.value.response["Error"]
            assert err["Code"] in (
                "ResourceNotFoundException",
                "NotFoundException",
                "InvalidParameterValueException",
            )
        finally:
            backup.delete_backup_vault(BackupVaultName=vault_name)


class TestBackupScanJobOperations:
    def test_list_scan_jobs_returns_key(self, backup):
        """ListScanJobs returns a ScanJobs list key."""
        resp = backup.list_scan_jobs()
        assert "ScanJobs" in resp

    def test_describe_scan_job_not_found(self, backup):
        """DescribeScanJob with fake ID returns ResourceNotFoundException."""
        with pytest.raises(ClientError) as exc_info:
            backup.describe_scan_job(ScanJobId="fake-scan-job-id-00000000")
        err = exc_info.value.response["Error"]
        assert err["Code"] in ("ResourceNotFoundException", "NotFoundException")


class TestBackupTieringConfigurationOperations:
    def test_list_tiering_configurations_returns_key(self, backup):
        """ListTieringConfigurations returns a TieringConfigurations list key."""
        resp = backup.list_tiering_configurations()
        assert "TieringConfigurations" in resp

    def test_get_tiering_configuration_not_found(self, backup):
        """GetTieringConfiguration with fake name returns ResourceNotFoundException."""
        with pytest.raises(ClientError) as exc_info:
            backup.get_tiering_configuration(TieringConfigurationName="nonexistent-tiering-cfg")
        err = exc_info.value.response["Error"]
        assert err["Code"] in ("ResourceNotFoundException", "NotFoundException")

    def test_create_tiering_configuration_returns_arn(self, backup):
        """CreateTieringConfiguration returns ARN and name."""
        vault_name = _unique("vault")
        tc_name = _unique("tc")
        backup.create_backup_vault(BackupVaultName=vault_name)
        try:
            resp = backup.create_tiering_configuration(
                TieringConfiguration={
                    "TieringConfigurationName": tc_name,
                    "BackupVaultName": vault_name,
                    "ResourceSelection": [],
                }
            )
            assert resp["TieringConfigurationName"] == tc_name
            assert "TieringConfigurationArn" in resp
            assert "CreationTime" in resp
        finally:
            backup.delete_tiering_configuration(TieringConfigurationName=tc_name)
            backup.delete_backup_vault(BackupVaultName=vault_name)

    def test_create_tiering_configuration_appears_in_list(self, backup):
        """A created TieringConfiguration appears in ListTieringConfigurations."""
        vault_name = _unique("vault")
        tc_name = _unique("tc")
        backup.create_backup_vault(BackupVaultName=vault_name)
        try:
            backup.create_tiering_configuration(
                TieringConfiguration={
                    "TieringConfigurationName": tc_name,
                    "BackupVaultName": vault_name,
                    "ResourceSelection": [],
                }
            )
            resp = backup.list_tiering_configurations()
            assert "TieringConfigurations" in resp
            names = [tc["TieringConfigurationName"] for tc in resp["TieringConfigurations"]]
            assert tc_name in names
        finally:
            backup.delete_tiering_configuration(TieringConfigurationName=tc_name)
            backup.delete_backup_vault(BackupVaultName=vault_name)

    def test_get_tiering_configuration_after_create(self, backup):
        """GetTieringConfiguration retrieves a created configuration."""
        vault_name = _unique("vault")
        tc_name = _unique("tc")
        backup.create_backup_vault(BackupVaultName=vault_name)
        try:
            backup.create_tiering_configuration(
                TieringConfiguration={
                    "TieringConfigurationName": tc_name,
                    "BackupVaultName": vault_name,
                    "ResourceSelection": [],
                }
            )
            resp = backup.get_tiering_configuration(TieringConfigurationName=tc_name)
            assert "TieringConfiguration" in resp
            tc = resp["TieringConfiguration"]
            assert tc["TieringConfigurationName"] == tc_name
            assert "TieringConfigurationArn" in tc
        finally:
            backup.delete_tiering_configuration(TieringConfigurationName=tc_name)
            backup.delete_backup_vault(BackupVaultName=vault_name)

    def test_update_tiering_configuration_returns_name(self, backup):
        """UpdateTieringConfiguration returns updated configuration name and timestamp."""
        vault_name = _unique("vault")
        tc_name = _unique("tc")
        backup.create_backup_vault(BackupVaultName=vault_name)
        try:
            backup.create_tiering_configuration(
                TieringConfiguration={
                    "TieringConfigurationName": tc_name,
                    "BackupVaultName": vault_name,
                    "ResourceSelection": [],
                }
            )
            resp = backup.update_tiering_configuration(
                TieringConfigurationName=tc_name,
                TieringConfiguration={
                    "BackupVaultName": vault_name,
                    "ResourceSelection": [],
                },
            )
            assert resp["TieringConfigurationName"] == tc_name
            assert "LastUpdatedTime" in resp
        finally:
            backup.delete_tiering_configuration(TieringConfigurationName=tc_name)
            backup.delete_backup_vault(BackupVaultName=vault_name)

    def test_delete_tiering_configuration_removes_from_list(self, backup):
        """DeleteTieringConfiguration removes the configuration from list."""
        vault_name = _unique("vault")
        tc_name = _unique("tc")
        backup.create_backup_vault(BackupVaultName=vault_name)
        try:
            backup.create_tiering_configuration(
                TieringConfiguration={
                    "TieringConfigurationName": tc_name,
                    "BackupVaultName": vault_name,
                    "ResourceSelection": [],
                }
            )
            backup.delete_tiering_configuration(TieringConfigurationName=tc_name)
            resp = backup.list_tiering_configurations()
            names = [tc["TieringConfigurationName"] for tc in resp["TieringConfigurations"]]
            assert tc_name not in names
        finally:
            backup.delete_backup_vault(BackupVaultName=vault_name)
