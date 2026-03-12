"""
Tests for the SQS-based onboarding task queue.
"""

from .models import TenantEntity


class TestOnboardingQueue:
    """Queue and process onboarding tasks."""

    def test_provisioning_queues_all_task_types(self, platform, tenant_a):
        """Provisioning a tenant enqueues all required onboarding tasks."""
        tasks = platform.process_onboarding_tasks(max_tasks=10)
        task_types = {t.task_type for t in tasks}
        expected = {"create_db", "seed_data", "configure_dns", "send_welcome"}
        assert expected.issubset(task_types)

    def test_tasks_are_marked_completed(self, platform, tenant_a):
        """Processed tasks have status='completed'."""
        tasks = platform.process_onboarding_tasks(max_tasks=10)
        for task in tasks:
            assert task.status == "completed"

    def test_queue_custom_task(self, platform, tenant_a):
        """Queue and process an ad-hoc onboarding task."""
        platform.queue_onboarding_task("tenant-a", "enable_sso")
        tasks = platform.process_onboarding_tasks(max_tasks=10)
        task_types = {t.task_type for t in tasks}
        assert "enable_sso" in task_types

    def test_tasks_consumed_once(self, platform, tenant_a):
        """After processing, the queue is empty (tasks are deleted)."""
        # Drain whatever provisioning put in
        platform.process_onboarding_tasks(max_tasks=20)

        # Queue should now be empty
        tasks = platform.process_onboarding_tasks(max_tasks=10)
        assert len(tasks) == 0

    def test_multiple_tenants_share_queue(self, platform, tenant_a, tenant_b):
        """Tasks from both tenants go through the same queue."""
        # Drain provisioning tasks first
        platform.process_onboarding_tasks(max_tasks=20)

        platform.queue_onboarding_task("tenant-a", "migrate_data")
        platform.queue_onboarding_task("tenant-b", "migrate_data")

        tasks = platform.process_onboarding_tasks(max_tasks=10)
        tenant_ids = {t.tenant_id for t in tasks}
        assert "tenant-a" in tenant_ids
        assert "tenant-b" in tenant_ids

    def test_task_ordering_fifo_approximation(self, platform, tenant_a):
        """Tasks are received roughly in the order they were sent."""
        # Drain provisioning tasks
        platform.process_onboarding_tasks(max_tasks=20)

        for i in range(3):
            platform.queue_onboarding_task("tenant-a", f"step_{i}")

        tasks = platform.process_onboarding_tasks(max_tasks=10)
        step_tasks = [t for t in tasks if t.task_type.startswith("step_")]
        # SQS standard queue doesn't guarantee strict FIFO, but we should
        # get all 3 tasks.
        assert len(step_tasks) == 3
        assert {t.task_type for t in step_tasks} == {"step_0", "step_1", "step_2"}


class TestOnboardingEndToEnd:
    """Full onboarding flow using the platform API."""

    def test_provision_process_then_use(self, platform):
        """Provision tenant, process onboarding, then use the platform."""
        # Provision
        platform.provision_tenant(
            tenant_id="tenant-new",
            name="NewCo",
            plan="pro",
            admin_email="admin@newco.example.com",
        )

        # Process onboarding
        tasks = platform.process_onboarding_tasks(max_tasks=10)
        assert len(tasks) >= 4

        # Use the platform: write data
        platform.put_entity(
            TenantEntity(
                tenant_id="tenant-new",
                entity_key="USER#founder",
                entity_type="USER",
                data={"name": "Founder", "role": "admin"},
            )
        )

        # Verify
        entity = platform.get_entity("tenant-new", "USER#founder")
        assert entity is not None
        assert entity.data["role"] == "admin"

        # Config reflects pro plan
        config = platform.get_tenant_config("tenant-new")
        assert config.max_users == 50
        assert "api_access" in config.features

        # Clean up
        platform.deprovision_tenant("tenant-new")
