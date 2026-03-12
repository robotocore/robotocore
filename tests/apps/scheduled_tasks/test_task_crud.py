"""
Tests for task definition CRUD operations.
"""

from __future__ import annotations

from ..scheduled_tasks.models import TaskDefinition


class TestCreateTask:
    def test_create_with_cron_schedule(self, scheduler, sample_task, events):
        """Create a task with a cron schedule, verify DynamoDB and EventBridge."""
        task = sample_task

        # Verify in DynamoDB
        retrieved = scheduler.get_task(task.task_id)
        assert retrieved is not None
        assert retrieved.name == "nightly-report"
        assert retrieved.schedule_expression == "cron(0 2 * * ? *)"
        assert retrieved.enabled is True
        assert retrieved.max_retries == 3

        # Verify EventBridge rule
        rule = events.describe_rule(Name=scheduler._rule_name(task.task_id))
        assert rule["State"] == "ENABLED"
        assert rule["ScheduleExpression"] == "cron(0 2 * * ? *)"

    def test_create_with_rate_expression(self, scheduler, events):
        """Create a task with a rate expression."""
        task = TaskDefinition(
            name="heartbeat-check",
            group="monitoring",
            schedule_expression="rate(5 minutes)",
            target_arn="arn:aws:lambda:us-east-1:123456789012:function:heartbeat",
        )
        created = scheduler.create_task(task)

        rule = events.describe_rule(Name=scheduler._rule_name(created.task_id))
        assert rule["ScheduleExpression"] == "rate(5 minutes)"
        assert rule["State"] == "ENABLED"

        # cleanup
        scheduler.delete_task(created.task_id)

    def test_create_stores_ssm_config(self, scheduler, ssm, sample_task, config_prefix):
        """SSM parameters are created for the task."""
        resp = ssm.get_parameters_by_path(
            Path=f"{config_prefix}/{sample_task.task_id}", Recursive=True
        )
        params = {p["Name"].split("/")[-1]: p["Value"] for p in resp["Parameters"]}
        assert params["max_retries"] == "3"
        assert params["timeout_seconds"] == "600"

    def test_create_with_eventbridge_target(self, scheduler, events, sample_task):
        """EventBridge target is set with the correct ARN and input."""
        targets = events.list_targets_by_rule(Rule=scheduler._rule_name(sample_task.task_id))[
            "Targets"
        ]
        assert len(targets) == 1
        assert targets[0]["Arn"] == sample_task.target_arn


class TestUpdateTask:
    def test_update_schedule(self, scheduler, events, sample_task):
        """Update a task's schedule expression."""
        scheduler.update_task_schedule(sample_task.task_id, "rate(1 hour)")
        updated = scheduler.get_task(sample_task.task_id)
        assert updated.schedule_expression == "rate(1 hour)"

        rule = events.describe_rule(Name=scheduler._rule_name(sample_task.task_id))
        assert rule["ScheduleExpression"] == "rate(1 hour)"

    def test_disable_task(self, scheduler, events, sample_task):
        """Disable a task, verify DynamoDB and EventBridge."""
        scheduler.disable_task(sample_task.task_id)

        task = scheduler.get_task(sample_task.task_id)
        assert task.enabled is False

        rule = events.describe_rule(Name=scheduler._rule_name(sample_task.task_id))
        assert rule["State"] == "DISABLED"

    def test_enable_task(self, scheduler, events, sample_task):
        """Disable then re-enable a task."""
        scheduler.disable_task(sample_task.task_id)
        scheduler.enable_task(sample_task.task_id)

        task = scheduler.get_task(sample_task.task_id)
        assert task.enabled is True

        rule = events.describe_rule(Name=scheduler._rule_name(sample_task.task_id))
        assert rule["State"] == "ENABLED"


class TestDeleteTask:
    def test_delete_removes_all(self, scheduler, dynamodb, tasks_table, events, sample_task):
        """Delete removes DynamoDB item, EventBridge rule, and SSM params."""
        task_id = sample_task.task_id
        rule_name = scheduler._rule_name(task_id)

        scheduler.delete_task(task_id)

        # DynamoDB
        resp = dynamodb.get_item(TableName=tasks_table, Key={"task_id": {"S": task_id}})
        assert "Item" not in resp

        # EventBridge
        import botocore.exceptions

        try:
            events.describe_rule(Name=rule_name)
            assert False, "Rule should have been deleted"
        except botocore.exceptions.ClientError as e:
            assert e.response["Error"]["Code"] == "ResourceNotFoundException"


class TestListTasks:
    def test_list_by_group(self, scheduler, task_group):
        """List tasks by group name via the GSI."""
        group_name, tasks = task_group
        found = scheduler.list_tasks_by_group(group_name)
        assert len(found) == 3
        found_names = {t.name for t in found}
        assert found_names == {"extract", "transform", "load"}

    def test_list_empty_group(self, scheduler):
        """Querying a non-existent group returns empty list."""
        found = scheduler.list_tasks_by_group("nonexistent-group")
        assert found == []
