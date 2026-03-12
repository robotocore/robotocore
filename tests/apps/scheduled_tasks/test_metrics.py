"""
Tests for task execution metrics and CloudWatch publishing.
"""

from __future__ import annotations


class TestMetrics:
    def test_compute_execution_stats(self, scheduler, sample_task):
        """Compute metrics from execution history."""
        # 2 successes, 1 failure
        for _ in range(2):
            exe = scheduler.start_execution(sample_task.task_id)
            scheduler.complete_execution(sample_task.task_id, exe.execution_id, output={"ok": True})

        exe_f = scheduler.start_execution(sample_task.task_id)
        scheduler.fail_execution(sample_task.task_id, exe_f.execution_id, "fail")

        metrics = scheduler.compute_metrics(sample_task.task_id)
        assert metrics.total_executions == 3
        assert metrics.successes == 2
        assert metrics.failures == 1
        assert metrics.task_id == sample_task.task_id

    def test_publish_cloudwatch_metrics(
        self,
        scheduler,
        sample_task,
        cloudwatch,
        metrics_namespace,
    ):
        """Publish metrics and verify they appear in CloudWatch."""
        exe = scheduler.start_execution(sample_task.task_id)
        scheduler.complete_execution(sample_task.task_id, exe.execution_id)

        scheduler.publish_task_metrics(sample_task.task_id)

        resp = cloudwatch.list_metrics(Namespace=metrics_namespace)
        metric_names = {m["MetricName"] for m in resp["Metrics"]}
        # Should include at least TotalExecutions and Successes
        assert "TotalExecutions" in metric_names
        assert "Successes" in metric_names

    def test_success_rate_calculation(self, scheduler, sample_task):
        """Success rate computed correctly."""
        for _ in range(3):
            exe = scheduler.start_execution(sample_task.task_id)
            scheduler.complete_execution(sample_task.task_id, exe.execution_id)

        exe_f = scheduler.start_execution(sample_task.task_id)
        scheduler.fail_execution(sample_task.task_id, exe_f.execution_id, "fail")

        metrics = scheduler.compute_metrics(sample_task.task_id)
        rate = metrics.successes / (metrics.successes + metrics.failures)
        assert rate == 0.75

    def test_average_duration(self, scheduler, sample_task):
        """Average duration is computed from completed executions."""
        for _ in range(2):
            exe = scheduler.start_execution(sample_task.task_id)
            scheduler.complete_execution(sample_task.task_id, exe.execution_id)

        metrics = scheduler.compute_metrics(sample_task.task_id)
        # Duration is ~0 because start and complete are near-instant in tests
        assert metrics.avg_duration_seconds >= 0

    def test_group_aggregate_metrics(self, scheduler, task_group, cloudwatch, metrics_namespace):
        """Publish aggregate metrics for a task group."""
        group_name, tasks = task_group

        # Run one execution per task
        for task in tasks:
            exe = scheduler.start_execution(task.task_id)
            scheduler.complete_execution(task.task_id, exe.execution_id)

        result = scheduler.publish_group_metrics(group_name)
        assert result["group"] == group_name
        assert result["total_executions"] == 3
        assert result["successes"] == 3
        assert result["failures"] == 0

        resp = cloudwatch.list_metrics(Namespace=metrics_namespace)
        metric_names = {m["MetricName"] for m in resp["Metrics"]}
        assert "GroupTotalExecutions" in metric_names

    def test_last_run_tracked(self, scheduler, sample_task):
        """Metrics include last_run timestamp."""
        exe = scheduler.start_execution(sample_task.task_id)
        scheduler.complete_execution(sample_task.task_id, exe.execution_id)

        metrics = scheduler.compute_metrics(sample_task.task_id)
        assert metrics.last_run != ""
