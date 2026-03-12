"""
Tests for task dependency (DAG) scheduling.
"""

from __future__ import annotations

from ..scheduled_tasks.models import (
    DependencyCondition,
    ExecutionStatus,
    TaskDefinition,
    TaskDependency,
)


class TestDependencies:
    def test_task_b_depends_on_a(self, scheduler):
        """B depends on A; A succeeds -> B can execute."""
        a = scheduler.create_task(TaskDefinition(name="dep-a", group="dep"))
        b = scheduler.create_task(TaskDefinition(name="dep-b", group="dep"))
        scheduler.add_dependency(
            TaskDependency(
                task_id=b.task_id,
                depends_on=a.task_id,
                condition=DependencyCondition.SUCCESS,
            )
        )

        # Before A runs, B cannot execute
        assert scheduler.can_execute(b.task_id) is False

        # Run A to success
        ea = scheduler.start_execution(a.task_id)
        scheduler.complete_execution(a.task_id, ea.execution_id)

        # Now B can execute
        assert scheduler.can_execute(b.task_id) is True

        scheduler.delete_task(a.task_id)
        scheduler.delete_task(b.task_id)

    def test_a_fails_b_blocked(self, scheduler):
        """A fails -> B with SUCCESS condition is blocked."""
        a = scheduler.create_task(TaskDefinition(name="fail-a", group="dep"))
        b = scheduler.create_task(TaskDefinition(name="blocked-b", group="dep"))
        scheduler.add_dependency(
            TaskDependency(
                task_id=b.task_id,
                depends_on=a.task_id,
                condition=DependencyCondition.SUCCESS,
            )
        )

        ea = scheduler.start_execution(a.task_id)
        scheduler.fail_execution(a.task_id, ea.execution_id, "crash")

        assert scheduler.can_execute(b.task_id) is False

        # execute_with_dependencies records SKIPPED
        result = scheduler.execute_with_dependencies(b.task_id)
        assert result is None

        execs = scheduler.list_executions(b.task_id)
        assert len(execs) == 1
        assert execs[0].status == ExecutionStatus.SKIPPED

        scheduler.delete_task(a.task_id)
        scheduler.delete_task(b.task_id)

    def test_chain_of_three(self, scheduler):
        """A -> B -> C: C can run only after B succeeds, B after A."""
        a = scheduler.create_task(TaskDefinition(name="chain-a", group="chain"))
        b = scheduler.create_task(TaskDefinition(name="chain-b", group="chain"))
        c = scheduler.create_task(TaskDefinition(name="chain-c", group="chain"))

        scheduler.add_dependency(TaskDependency(task_id=b.task_id, depends_on=a.task_id))
        scheduler.add_dependency(TaskDependency(task_id=c.task_id, depends_on=b.task_id))

        # Nothing can run yet except A
        assert scheduler.can_execute(a.task_id) is True
        assert scheduler.can_execute(b.task_id) is False
        assert scheduler.can_execute(c.task_id) is False

        # Complete A
        ea = scheduler.start_execution(a.task_id)
        scheduler.complete_execution(a.task_id, ea.execution_id)
        assert scheduler.can_execute(b.task_id) is True
        assert scheduler.can_execute(c.task_id) is False

        # Complete B
        eb = scheduler.start_execution(b.task_id)
        scheduler.complete_execution(b.task_id, eb.execution_id)
        assert scheduler.can_execute(c.task_id) is True

        # Complete C
        ec = scheduler.execute_with_dependencies(c.task_id)
        assert ec is not None
        assert ec.status == ExecutionStatus.RUNNING

        scheduler.delete_task(a.task_id)
        scheduler.delete_task(b.task_id)
        scheduler.delete_task(c.task_id)

    def test_completed_condition_accepts_failure(self, scheduler):
        """COMPLETED condition passes even if upstream failed."""
        a = scheduler.create_task(TaskDefinition(name="any-a", group="dep"))
        b = scheduler.create_task(TaskDefinition(name="any-b", group="dep"))
        scheduler.add_dependency(
            TaskDependency(
                task_id=b.task_id,
                depends_on=a.task_id,
                condition=DependencyCondition.COMPLETED,
            )
        )

        ea = scheduler.start_execution(a.task_id)
        scheduler.fail_execution(a.task_id, ea.execution_id, "error")

        # B can run because COMPLETED accepts any terminal state
        assert scheduler.can_execute(b.task_id) is True

        scheduler.delete_task(a.task_id)
        scheduler.delete_task(b.task_id)
