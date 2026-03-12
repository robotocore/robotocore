"""
Tests for execution lifecycle and history queries.
"""

from __future__ import annotations

from ..scheduled_tasks.models import ExecutionStatus, TaskDefinition


class TestExecutionLifecycle:
    def test_start_execution_running(self, scheduler, sample_task):
        """Starting an execution records RUNNING status."""
        exe = scheduler.start_execution(sample_task.task_id)
        assert exe.status == ExecutionStatus.RUNNING
        assert exe.started_at != ""
        assert exe.attempt == 1

        # Verify in DynamoDB
        fetched = scheduler.get_execution(sample_task.task_id, exe.execution_id)
        assert fetched is not None
        assert fetched.status == ExecutionStatus.RUNNING

    def test_complete_execution_success(self, scheduler, sample_task):
        """Mark execution SUCCESS with output."""
        exe = scheduler.start_execution(sample_task.task_id)
        result = scheduler.complete_execution(
            sample_task.task_id,
            exe.execution_id,
            output={"rows": 100, "status": "ok"},
        )
        assert result.status == ExecutionStatus.SUCCESS
        assert result.finished_at != ""
        assert result.output_key != ""

    def test_fail_execution(self, scheduler, sample_task):
        """Mark execution FAILED with error message."""
        exe = scheduler.start_execution(sample_task.task_id)
        result = scheduler.fail_execution(
            sample_task.task_id,
            exe.execution_id,
            "ConnectionError: timeout",
        )
        assert result.status == ExecutionStatus.FAILED
        assert result.error_message == "ConnectionError: timeout"

    def test_timeout_execution(self, scheduler, sample_task):
        """Mark execution as TIMED_OUT."""
        exe = scheduler.start_execution(sample_task.task_id)
        result = scheduler.timeout_execution(sample_task.task_id, exe.execution_id)
        assert result.status == ExecutionStatus.TIMED_OUT
        assert result.finished_at != ""


class TestExecutionHistory:
    def test_list_executions_for_task(self, scheduler, sample_task):
        """Multiple executions for a single task are all returned."""
        exec_ids = []
        for _ in range(4):
            exe = scheduler.start_execution(sample_task.task_id)
            scheduler.complete_execution(sample_task.task_id, exe.execution_id)
            exec_ids.append(exe.execution_id)

        history = scheduler.list_executions(sample_task.task_id)
        assert len(history) == 4
        returned_ids = {e.execution_id for e in history}
        assert returned_ids == set(exec_ids)

    def test_query_by_status(self, scheduler):
        """Query executions filtered by status via GSI."""
        # Create two tasks, run them to different outcomes
        t1 = scheduler.create_task(TaskDefinition(name="status-test-a", group="test"))
        t2 = scheduler.create_task(TaskDefinition(name="status-test-b", group="test"))

        e1 = scheduler.start_execution(t1.task_id)
        scheduler.complete_execution(t1.task_id, e1.execution_id)

        e2 = scheduler.start_execution(t2.task_id)
        scheduler.fail_execution(t2.task_id, e2.execution_id, "boom")

        successes = scheduler.list_executions_by_status(ExecutionStatus.SUCCESS)
        success_ids = {e.execution_id for e in successes}
        assert e1.execution_id in success_ids

        failures = scheduler.list_executions_by_status(ExecutionStatus.FAILED)
        fail_ids = {e.execution_id for e in failures}
        assert e2.execution_id in fail_ids

        # cleanup
        scheduler.delete_task(t1.task_id)
        scheduler.delete_task(t2.task_id)

    def test_query_by_date_range(self, scheduler, sample_task):
        """Filter executions by date range."""
        # Create a few executions
        for _ in range(3):
            exe = scheduler.start_execution(sample_task.task_id)
            scheduler.complete_execution(sample_task.task_id, exe.execution_id)

        all_execs = scheduler.list_executions(sample_task.task_id)
        assert len(all_execs) >= 3

        # Use a wide date range that includes all
        results = scheduler.list_executions_by_date_range(
            sample_task.task_id,
            "2020-01-01T00:00:00",
            "2030-01-01T00:00:00",
        )
        assert len(results) >= 3
