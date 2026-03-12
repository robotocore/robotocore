"""
Tests for tenant provisioning and deprovisioning lifecycle.
"""

import pytest


class TestProvisionTenant:
    """Verify that provisioning creates all expected resources."""

    def test_provision_creates_dynamodb_metadata(self, platform, tenant_a):
        """Provisioning inserts a TENANT# metadata entity in DynamoDB."""
        entity = platform.get_entity("tenant-a", "TENANT#tenant-a")
        assert entity is not None
        assert entity.entity_type == "TENANT"
        data = entity.data
        assert data["name"] == "Acme Analytics"
        assert data["plan"] == "starter"
        assert data["status"] == "active"

    def test_provision_creates_ssm_config(self, platform, tenant_a):
        """Provisioning writes plan-derived SSM parameters."""
        config = platform.get_tenant_config("tenant-a")
        assert config.max_users == 10
        assert "billing" in config.features
        assert "reports" in config.features

    def test_provision_creates_s3_prefix(self, platform, tenant_a):
        """Provisioning uploads a welcome file, establishing the S3 prefix."""
        files = platform.list_files("tenant-a")
        assert any("welcome.txt" in f for f in files)

    def test_provision_creates_secret(self, platform, tenant_a):
        """Provisioning creates DB credentials in Secrets Manager."""
        creds = platform.get_tenant_credentials("tenant-a")
        assert creds["host"] == "tenant-a-db.internal.example.com"
        assert creds["port"] == 5432
        assert creds["username"] == "tenant-a_app"

    def test_provision_queues_onboarding_tasks(self, platform, tenant_a):
        """Provisioning sends onboarding tasks to SQS."""
        tasks = platform.process_onboarding_tasks(max_tasks=10)
        task_types = {t.task_type for t in tasks}
        assert "create_db" in task_types
        assert "seed_data" in task_types
        assert "configure_dns" in task_types
        assert "send_welcome" in task_types

    def test_provision_records_audit(self, platform, tenant_a):
        """Provisioning is recorded in the audit log."""
        log = platform.get_audit_log()
        provision_entries = [e for e in log if e["action"] == "provision_tenant"]
        assert len(provision_entries) >= 1
        assert provision_entries[0]["tenant_id"] == "tenant-a"

    def test_provision_sets_tenant_active(self, platform, tenant_a):
        """New tenant starts in 'active' status."""
        t = platform.get_tenant("tenant-a")
        assert t.status == "active"

    def test_provision_with_different_plans(self, platform, tenant_a, tenant_b):
        """Two tenants on different plans get different config."""
        config_a = platform.get_tenant_config("tenant-a")
        config_b = platform.get_tenant_config("tenant-b")
        assert config_a.max_users == 10  # starter
        assert config_b.max_users == 500  # enterprise
        assert len(config_b.features) > len(config_a.features)


class TestDeprovisionTenant:
    """Verify that deprovisioning cleans up all resources."""

    def test_deprovision_removes_dynamodb_entities(self, platform, tenant_a):
        """After deprovisioning, querying DynamoDB returns nothing."""
        # Add an entity first
        from .models import TenantEntity

        platform.put_entity(
            TenantEntity(
                tenant_id="tenant-a",
                entity_key="USER#1",
                entity_type="USER",
                data={"name": "Alice"},
            )
        )
        platform.deprovision_tenant("tenant-a")

        # Direct DynamoDB query should return 0 items
        resp = platform._ddb.query(
            TableName=platform._table,
            KeyConditionExpression="tenant_id = :tid",
            ExpressionAttributeValues={":tid": {"S": "tenant-a"}},
        )
        assert resp["Count"] == 0

    def test_deprovision_removes_s3_files(self, platform, tenant_a):
        """After deprovisioning, S3 prefix is empty."""
        platform.upload_file("tenant-a", "data/report.csv", b"col1,col2")
        platform.deprovision_tenant("tenant-a")

        resp = platform._s3.list_objects_v2(Bucket=platform._bucket, Prefix="tenant-a/")
        assert resp.get("KeyCount", 0) == 0

    def test_deprovision_removes_ssm_config(self, platform, tenant_a):
        """After deprovisioning, SSM parameters are gone."""
        platform.deprovision_tenant("tenant-a")

        resp = platform._ssm.get_parameters_by_path(
            Path=f"{platform._ssm_prefix}/tenant-a/", Recursive=True
        )
        assert len(resp.get("Parameters", [])) == 0

    def test_deprovision_removes_secret(self, platform, tenant_a):
        """After deprovisioning, the Secrets Manager secret is deleted."""
        platform.deprovision_tenant("tenant-a")

        secret_name = f"{platform._secret_prefix}/tenant-a/db-credentials"
        with pytest.raises(Exception):
            platform._sm.get_secret_value(SecretId=secret_name)

    def test_deprovision_marks_tenant_deprovisioned(self, platform, tenant_a):
        """Tenant status is updated to deprovisioned."""
        platform.deprovision_tenant("tenant-a")
        t = platform._tenants.get("tenant-a")
        assert t is not None
        assert t.status == "deprovisioned"
