"""
Tests for automatic retry logic.
"""

from __future__ import annotations

from ..scheduled_tasks.models import ExecutionStatus, TaskDefinition


class TestRetry:
    def test_retry_creates_new_execution(self, scheduler, sample_task):
        """A retry produces a new execution with incremented attempt."""
        exe = scheduler.start_execution(sample_task.task_id)
        scheduler.fail_execution(sample_task.task_id, exe.execution_id, "transient")

        retried = scheduler.retry_execution(sample_task.task_id, exe.execution_id)
        assert retried is not None
        assert retried.execution_id != exe.execution_id
        assert retried.attempt == 2
        assert retried.status == ExecutionStatus.RUNNING

    def test_each_retry_tracked_separately(self, scheduler, sample_task):
        """Each retry is a separate DynamoDB record."""
        ids = []
        exe = scheduler.start_execution(sample_task.task_id)
        scheduler.fail_execution(sample_task.task_id, exe.execution_id, "err")
        ids.append(exe.execution_id)

        for _ in range(2):
            retried = scheduler.retry_execution(sample_task.task_id, ids[-1])
            assert retried is not None
            scheduler.fail_execution(sample_task.task_id, retried.execution_id, "err again")
            ids.append(retried.execution_id)

        all_execs = scheduler.list_executions(sample_task.task_id)
        assert len(all_execs) == 3
        assert len(set(ids)) == 3

    def test_exhausted_retries_returns_none(self, scheduler):
        """After max_retries attempts, retry returns None."""
        task = scheduler.create_task(TaskDefinition(name="no-retries", group="test", max_retries=2))
        # Attempt 1
        e1 = scheduler.start_execution(task.task_id)
        scheduler.fail_execution(task.task_id, e1.execution_id, "fail 1")

        # Attempt 2 (retry)
        e2 = scheduler.retry_execution(task.task_id, e1.execution_id)
        assert e2 is not None
        assert e2.attempt == 2
        scheduler.fail_execution(task.task_id, e2.execution_id, "fail 2")

        # Attempt 3: should be exhausted (max_retries=2)
        e3 = scheduler.retry_execution(task.task_id, e2.execution_id)
        assert e3 is None

        scheduler.delete_task(task.task_id)

    def test_retry_with_backoff_tracking(self, scheduler, sample_task):
        """Retries are trackable via attempt numbers for backoff."""
        exe = scheduler.start_execution(sample_task.task_id)
        scheduler.fail_execution(sample_task.task_id, exe.execution_id, "fail")

        r1 = scheduler.retry_execution(sample_task.task_id, exe.execution_id)
        assert r1 is not None
        assert r1.attempt == 2

        scheduler.fail_execution(sample_task.task_id, r1.execution_id, "fail again")
        r2 = scheduler.retry_execution(sample_task.task_id, r1.execution_id)
        assert r2 is not None
        assert r2.attempt == 3

        # Verify all attempts are in the history
        history = scheduler.list_executions(sample_task.task_id)
        attempts = sorted(e.attempt for e in history)
        assert attempts == [1, 2, 3]
