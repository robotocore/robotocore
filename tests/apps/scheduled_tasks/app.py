"""
TaskScheduler -- a distributed cron-like task execution system built on AWS.

Architecture:
  EventBridge  -- schedule rules (cron / rate expressions)
  DynamoDB     -- task definitions table + execution history table
  SSM          -- per-task configuration (timeouts, retries, payloads)
  S3           -- execution output / artifacts
  SNS          -- execution alerts (failure, success, timeout)
  CloudWatch   -- execution metrics and detailed logs

No robotocore or moto imports.  Only boto3, stdlib, and the local models.
"""

from __future__ import annotations

import json
import time
import uuid
from datetime import UTC, datetime
from typing import Any

from .models import (
    AlertType,
    DependencyCondition,
    ExecutionAlert,
    ExecutionStatus,
    TaskDefinition,
    TaskDependency,
    TaskExecution,
    TaskGroup,
    TaskMetrics,
)


class TaskScheduler:
    """Manages the full lifecycle of scheduled tasks using AWS services."""

    # ------------------------------------------------------------------ init
    def __init__(
        self,
        dynamodb,
        events,
        ssm,
        s3,
        sns,
        sqs,
        cloudwatch,
        logs,
        tasks_table: str,
        executions_table: str,
        config_prefix: str,
        output_bucket: str,
        alert_topic_arn: str,
        log_group: str,
        metrics_namespace: str,
    ) -> None:
        self.dynamodb = dynamodb
        self.events = events
        self.ssm = ssm
        self.s3 = s3
        self.sns = sns
        self.sqs = sqs
        self.cloudwatch = cloudwatch
        self.logs = logs

        self.tasks_table = tasks_table
        self.executions_table = executions_table
        self.config_prefix = config_prefix
        self.output_bucket = output_bucket
        self.alert_topic_arn = alert_topic_arn
        self.log_group = log_group
        self.metrics_namespace = metrics_namespace

    # ======================================================================
    # Task CRUD
    # ======================================================================

    def create_task(self, task: TaskDefinition) -> TaskDefinition:
        """Persist a task definition in DynamoDB and create an EventBridge rule."""
        item: dict[str, Any] = {
            "task_id": {"S": task.task_id},
            "name": {"S": task.name},
            "group_name": {"S": task.group},
            "schedule_expression": {"S": task.schedule_expression},
            "target_arn": {"S": task.target_arn},
            "input_payload": {"S": json.dumps(task.input_payload)},
            "enabled": {"BOOL": task.enabled},
            "max_retries": {"N": str(task.max_retries)},
            "timeout_seconds": {"N": str(task.timeout_seconds)},
            "max_concurrent": {"N": str(task.max_concurrent)},
            "description": {"S": task.description},
            "created_at": {"S": task.created_at},
        }
        self.dynamodb.put_item(TableName=self.tasks_table, Item=item)

        # EventBridge rule
        state = "ENABLED" if task.enabled else "DISABLED"
        self.events.put_rule(
            Name=self._rule_name(task.task_id),
            ScheduleExpression=task.schedule_expression,
            State=state,
            Description=f"Schedule for task {task.task_id}",
        )
        if task.target_arn:
            self.events.put_targets(
                Rule=self._rule_name(task.task_id),
                Targets=[
                    {
                        "Id": f"target-{task.task_id}",
                        "Arn": task.target_arn,
                        "Input": json.dumps(
                            {"task_id": task.task_id, "payload": task.input_payload}
                        ),
                    }
                ],
            )

        # SSM config
        self._store_task_config(task)
        return task

    def get_task(self, task_id: str) -> TaskDefinition | None:
        """Retrieve a task definition from DynamoDB."""
        resp = self.dynamodb.get_item(
            TableName=self.tasks_table,
            Key={"task_id": {"S": task_id}},
        )
        item = resp.get("Item")
        if not item:
            return None
        return self._item_to_task(item)

    def update_task_schedule(self, task_id: str, schedule_expression: str) -> TaskDefinition | None:
        """Change a task's schedule expression in DynamoDB and EventBridge."""
        self.dynamodb.update_item(
            TableName=self.tasks_table,
            Key={"task_id": {"S": task_id}},
            UpdateExpression="SET schedule_expression = :se",
            ExpressionAttributeValues={":se": {"S": schedule_expression}},
        )
        self.events.put_rule(
            Name=self._rule_name(task_id),
            ScheduleExpression=schedule_expression,
        )
        return self.get_task(task_id)

    def disable_task(self, task_id: str) -> None:
        """Disable a task: mark it in DynamoDB and disable the EventBridge rule."""
        self.dynamodb.update_item(
            TableName=self.tasks_table,
            Key={"task_id": {"S": task_id}},
            UpdateExpression="SET enabled = :e",
            ExpressionAttributeValues={":e": {"BOOL": False}},
        )
        self.events.disable_rule(Name=self._rule_name(task_id))

    def enable_task(self, task_id: str) -> None:
        """Re-enable a previously disabled task."""
        self.dynamodb.update_item(
            TableName=self.tasks_table,
            Key={"task_id": {"S": task_id}},
            UpdateExpression="SET enabled = :e",
            ExpressionAttributeValues={":e": {"BOOL": True}},
        )
        self.events.enable_rule(Name=self._rule_name(task_id))

    def delete_task(self, task_id: str) -> None:
        """Remove a task, its EventBridge rule, its SSM config, and its outputs."""
        # Remove EventBridge targets then rule
        rule_name = self._rule_name(task_id)
        try:
            targets = self.events.list_targets_by_rule(Rule=rule_name).get("Targets", [])
            if targets:
                self.events.remove_targets(
                    Rule=rule_name,
                    Ids=[t["Id"] for t in targets],
                )
            self.events.delete_rule(Name=rule_name)
        except Exception:
            pass  # rule may not exist

        # Remove SSM params
        self._delete_task_config(task_id)

        # Remove DynamoDB item
        self.dynamodb.delete_item(
            TableName=self.tasks_table,
            Key={"task_id": {"S": task_id}},
        )

    def list_tasks_by_group(self, group_name: str) -> list[TaskDefinition]:
        """Query the by-group GSI to list all tasks in a group."""
        resp = self.dynamodb.query(
            TableName=self.tasks_table,
            IndexName="by-group",
            KeyConditionExpression="group_name = :g",
            ExpressionAttributeValues={":g": {"S": group_name}},
        )
        # Filter out non-task items (groups, dependencies) that share the GSI
        items = [
            item
            for item in resp.get("Items", [])
            if not item["task_id"]["S"].startswith(("group#", "dep#"))
        ]
        return [self._item_to_task(item) for item in items]

    # ======================================================================
    # Execution lifecycle
    # ======================================================================

    def start_execution(self, task_id: str, attempt: int = 1) -> TaskExecution:
        """Record the start of a task execution."""
        now = datetime.now(UTC).isoformat()
        execution = TaskExecution(
            execution_id=f"exec-{uuid.uuid4().hex[:8]}",
            task_id=task_id,
            status=ExecutionStatus.RUNNING,
            started_at=now,
            attempt=attempt,
        )
        self.dynamodb.put_item(
            TableName=self.executions_table,
            Item={
                "task_id": {"S": task_id},
                "execution_id": {"S": execution.execution_id},
                "status": {"S": execution.status.value},
                "started_at": {"S": now},
                "attempt": {"N": str(attempt)},
            },
        )
        self._log_event(task_id, execution.execution_id, f"Execution started (attempt {attempt})")
        return execution

    def complete_execution(
        self, task_id: str, execution_id: str, output: dict[str, Any] | None = None
    ) -> TaskExecution:
        """Mark an execution as SUCCESS, store output in S3."""
        now = datetime.now(UTC).isoformat()
        output_key = ""
        if output is not None:
            output_key = self._store_output(task_id, execution_id, output)

        # Calculate duration
        exec_item = self._get_execution_item(task_id, execution_id)
        started = exec_item.get("started_at", {}).get("S", now)
        try:
            t0 = datetime.fromisoformat(started)
            t1 = datetime.fromisoformat(now)
            duration = (t1 - t0).total_seconds()
        except Exception:
            duration = 0.0

        self.dynamodb.update_item(
            TableName=self.executions_table,
            Key={
                "task_id": {"S": task_id},
                "execution_id": {"S": execution_id},
            },
            UpdateExpression=(
                "SET #st = :s, finished_at = :f, output_key = :o, duration_seconds = :d"
            ),
            ExpressionAttributeNames={"#st": "status"},
            ExpressionAttributeValues={
                ":s": {"S": ExecutionStatus.SUCCESS.value},
                ":f": {"S": now},
                ":o": {"S": output_key},
                ":d": {"N": str(duration)},
            },
        )
        self._log_event(task_id, execution_id, "Execution completed successfully")
        self._publish_metric(task_id, "ExecutionSuccess", 1)
        self._publish_metric(task_id, "ExecutionDuration", duration, "Seconds")

        return TaskExecution(
            execution_id=execution_id,
            task_id=task_id,
            status=ExecutionStatus.SUCCESS,
            started_at=started,
            finished_at=now,
            output_key=output_key,
            duration_seconds=duration,
        )

    def fail_execution(self, task_id: str, execution_id: str, error_message: str) -> TaskExecution:
        """Mark an execution as FAILED, send alert, possibly retry."""
        now = datetime.now(UTC).isoformat()
        exec_item = self._get_execution_item(task_id, execution_id)
        attempt = int(exec_item.get("attempt", {}).get("N", "1"))

        self.dynamodb.update_item(
            TableName=self.executions_table,
            Key={
                "task_id": {"S": task_id},
                "execution_id": {"S": execution_id},
            },
            UpdateExpression="SET #st = :s, finished_at = :f, error_message = :e",
            ExpressionAttributeNames={"#st": "status"},
            ExpressionAttributeValues={
                ":s": {"S": ExecutionStatus.FAILED.value},
                ":f": {"S": now},
                ":e": {"S": error_message},
            },
        )
        self._log_event(task_id, execution_id, f"Execution FAILED: {error_message}")
        self._publish_metric(task_id, "ExecutionFailure", 1)

        # Send failure alert
        self._send_alert(
            ExecutionAlert(
                execution_id=execution_id,
                task_id=task_id,
                alert_type=AlertType.FAILURE,
                message=error_message,
            )
        )

        return TaskExecution(
            execution_id=execution_id,
            task_id=task_id,
            status=ExecutionStatus.FAILED,
            finished_at=now,
            attempt=attempt,
            error_message=error_message,
        )

    def timeout_execution(self, task_id: str, execution_id: str) -> TaskExecution:
        """Mark an execution as TIMED_OUT and alert."""
        now = datetime.now(UTC).isoformat()
        self.dynamodb.update_item(
            TableName=self.executions_table,
            Key={
                "task_id": {"S": task_id},
                "execution_id": {"S": execution_id},
            },
            UpdateExpression="SET #st = :s, finished_at = :f",
            ExpressionAttributeNames={"#st": "status"},
            ExpressionAttributeValues={
                ":s": {"S": ExecutionStatus.TIMED_OUT.value},
                ":f": {"S": now},
            },
        )
        self._log_event(task_id, execution_id, "Execution TIMED OUT")
        self._publish_metric(task_id, "ExecutionTimeout", 1)
        self._send_alert(
            ExecutionAlert(
                execution_id=execution_id,
                task_id=task_id,
                alert_type=AlertType.TIMEOUT,
                message=f"Task {task_id} execution {execution_id} timed out",
            )
        )
        return TaskExecution(
            execution_id=execution_id,
            task_id=task_id,
            status=ExecutionStatus.TIMED_OUT,
            finished_at=now,
        )

    def retry_execution(self, task_id: str, failed_execution_id: str) -> TaskExecution | None:
        """Retry a failed execution if retries remain.

        Returns the new execution on retry, or None if max retries exhausted.
        """
        task = self.get_task(task_id)
        if task is None:
            return None
        exec_item = self._get_execution_item(task_id, failed_execution_id)
        attempt = int(exec_item.get("attempt", {}).get("N", "1"))
        if attempt >= task.max_retries:
            # Exhausted
            self._send_alert(
                ExecutionAlert(
                    execution_id=failed_execution_id,
                    task_id=task_id,
                    alert_type=AlertType.RETRY_EXHAUSTED,
                    message=(f"Task {task_id} exhausted {task.max_retries} retries"),
                )
            )
            self._log_event(
                task_id,
                failed_execution_id,
                f"Retries exhausted ({attempt}/{task.max_retries})",
            )
            return None
        new_exec = self.start_execution(task_id, attempt=attempt + 1)
        self._log_event(
            task_id,
            new_exec.execution_id,
            f"Retry attempt {attempt + 1}/{task.max_retries}",
        )
        return new_exec

    # ======================================================================
    # Execution queries
    # ======================================================================

    def get_execution(self, task_id: str, execution_id: str) -> TaskExecution | None:
        """Fetch a single execution record."""
        item = self._get_execution_item(task_id, execution_id)
        if not item:
            return None
        return self._item_to_execution(item)

    def list_executions(self, task_id: str) -> list[TaskExecution]:
        """List all executions for a task, newest first."""
        resp = self.dynamodb.query(
            TableName=self.executions_table,
            KeyConditionExpression="task_id = :tid",
            ExpressionAttributeValues={":tid": {"S": task_id}},
            ScanIndexForward=False,
        )
        return [self._item_to_execution(i) for i in resp.get("Items", [])]

    def list_executions_by_status(self, status: ExecutionStatus) -> list[TaskExecution]:
        """Query by-status GSI."""
        resp = self.dynamodb.query(
            TableName=self.executions_table,
            IndexName="by-status",
            KeyConditionExpression="#st = :s",
            ExpressionAttributeNames={"#st": "status"},
            ExpressionAttributeValues={":s": {"S": status.value}},
        )
        return [self._item_to_execution(i) for i in resp.get("Items", [])]

    def list_executions_by_date_range(
        self, task_id: str, start_date: str, end_date: str
    ) -> list[TaskExecution]:
        """Query executions for a task within a date range using a filter."""
        resp = self.dynamodb.query(
            TableName=self.executions_table,
            KeyConditionExpression="task_id = :tid",
            FilterExpression="started_at BETWEEN :s AND :e",
            ExpressionAttributeValues={
                ":tid": {"S": task_id},
                ":s": {"S": start_date},
                ":e": {"S": end_date},
            },
        )
        return [self._item_to_execution(i) for i in resp.get("Items", [])]

    # ======================================================================
    # Output / artifacts
    # ======================================================================

    def store_output(self, task_id: str, execution_id: str, data: dict[str, Any]) -> str:
        """Store execution output in S3 and return the key."""
        return self._store_output(task_id, execution_id, data)

    def get_output(self, task_id: str, execution_id: str) -> dict[str, Any] | None:
        """Retrieve execution output from S3."""
        key = f"output/{task_id}/{execution_id}/result.json"
        try:
            resp = self.s3.get_object(Bucket=self.output_bucket, Key=key)
            return json.loads(resp["Body"].read().decode())
        except Exception:
            return None

    def list_outputs(self, task_id: str) -> list[str]:
        """List all output keys for a task."""
        prefix = f"output/{task_id}/"
        resp = self.s3.list_objects_v2(Bucket=self.output_bucket, Prefix=prefix)
        return [obj["Key"] for obj in resp.get("Contents", [])]

    def store_large_output(
        self, task_id: str, execution_id: str, data: bytes, filename: str
    ) -> str:
        """Store a large binary artifact in S3."""
        key = f"output/{task_id}/{execution_id}/{filename}"
        self.s3.put_object(Bucket=self.output_bucket, Key=key, Body=data)
        return key

    # ======================================================================
    # Alerts
    # ======================================================================

    def send_success_alert(self, task_id: str, execution_id: str, details: str = "") -> None:
        """Explicitly send a success notification."""
        self._send_alert(
            ExecutionAlert(
                execution_id=execution_id,
                task_id=task_id,
                alert_type=AlertType.SUCCESS,
                message=details or f"Task {task_id} completed successfully",
            )
        )

    # ======================================================================
    # Dependencies (DAG)
    # ======================================================================

    def add_dependency(self, dependency: TaskDependency) -> None:
        """Record that *task_id* depends on *depends_on*."""
        self.dynamodb.put_item(
            TableName=self.tasks_table,
            Item={
                "task_id": {"S": f"dep#{dependency.task_id}"},
                "depends_on": {"S": dependency.depends_on},
                "condition": {"S": dependency.condition.value},
            },
        )

    def get_dependencies(self, task_id: str) -> list[TaskDependency]:
        """Return all dependencies for a task."""
        resp = self.dynamodb.query(
            TableName=self.tasks_table,
            KeyConditionExpression="task_id = :tid",
            ExpressionAttributeValues={":tid": {"S": f"dep#{task_id}"}},
        )
        deps: list[TaskDependency] = []
        for item in resp.get("Items", []):
            deps.append(
                TaskDependency(
                    task_id=task_id,
                    depends_on=item["depends_on"]["S"],
                    condition=DependencyCondition(item["condition"]["S"]),
                )
            )
        return deps

    def can_execute(self, task_id: str) -> bool:
        """Check whether all upstream dependencies are satisfied.

        A dependency with condition SUCCESS requires the upstream task's most
        recent execution to be SUCCESS.  COMPLETED just needs any terminal state.
        """
        deps = self.get_dependencies(task_id)
        if not deps:
            return True
        for dep in deps:
            execs = self.list_executions(dep.depends_on)
            if not execs:
                return False
            latest = execs[0]
            if dep.condition == DependencyCondition.SUCCESS:
                if latest.status != ExecutionStatus.SUCCESS:
                    return False
            elif dep.condition == DependencyCondition.COMPLETED:
                terminal = {
                    ExecutionStatus.SUCCESS,
                    ExecutionStatus.FAILED,
                    ExecutionStatus.TIMED_OUT,
                }
                if latest.status not in terminal:
                    return False
        return True

    def execute_with_dependencies(self, task_id: str) -> TaskExecution | None:
        """Start an execution only if all dependencies are met.

        Returns None (and records SKIPPED) when deps are not satisfied.
        """
        if not self.can_execute(task_id):
            # Record a skipped execution
            now = datetime.now(UTC).isoformat()
            exec_id = f"exec-{uuid.uuid4().hex[:8]}"
            self.dynamodb.put_item(
                TableName=self.executions_table,
                Item={
                    "task_id": {"S": task_id},
                    "execution_id": {"S": exec_id},
                    "status": {"S": ExecutionStatus.SKIPPED.value},
                    "started_at": {"S": now},
                    "finished_at": {"S": now},
                    "attempt": {"N": "0"},
                },
            )
            self._log_event(task_id, exec_id, "Execution SKIPPED: dependencies not met")
            return None
        return self.start_execution(task_id)

    # ======================================================================
    # Task groups
    # ======================================================================

    def create_group(self, group: TaskGroup) -> TaskGroup:
        """Persist a task group definition."""
        self.dynamodb.put_item(
            TableName=self.tasks_table,
            Item={
                "task_id": {"S": f"group#{group.group_name}"},
                "group_name": {"S": group.group_name},
                "tasks": {"SS": group.tasks if group.tasks else ["__empty__"]},
                "description": {"S": group.description},
            },
        )
        return group

    def get_group(self, group_name: str) -> TaskGroup | None:
        """Retrieve a task group."""
        resp = self.dynamodb.get_item(
            TableName=self.tasks_table,
            Key={"task_id": {"S": f"group#{group_name}"}},
        )
        item = resp.get("Item")
        if not item:
            return None
        tasks = list(item.get("tasks", {}).get("SS", []))
        tasks = [t for t in tasks if t != "__empty__"]
        return TaskGroup(
            group_name=item["group_name"]["S"],
            tasks=tasks,
            description=item.get("description", {}).get("S", ""),
        )

    # ======================================================================
    # Metrics
    # ======================================================================

    def compute_metrics(self, task_id: str) -> TaskMetrics:
        """Compute execution statistics from the executions table."""
        execs = self.list_executions(task_id)
        total = len(execs)
        successes = sum(1 for e in execs if e.status == ExecutionStatus.SUCCESS)
        failures = sum(1 for e in execs if e.status == ExecutionStatus.FAILED)
        timeouts = sum(1 for e in execs if e.status == ExecutionStatus.TIMED_OUT)
        durations = [e.duration_seconds for e in execs if e.duration_seconds > 0]
        avg_dur = sum(durations) / len(durations) if durations else 0.0
        last_run = execs[0].started_at if execs else ""
        return TaskMetrics(
            task_id=task_id,
            total_executions=total,
            successes=successes,
            failures=failures,
            timeouts=timeouts,
            avg_duration_seconds=avg_dur,
            last_run=last_run,
        )

    def publish_task_metrics(self, task_id: str) -> TaskMetrics:
        """Compute and publish metrics to CloudWatch."""
        metrics = self.compute_metrics(task_id)
        metric_data = [
            {
                "MetricName": "TotalExecutions",
                "Dimensions": [{"Name": "TaskId", "Value": task_id}],
                "Value": float(metrics.total_executions),
                "Unit": "Count",
            },
            {
                "MetricName": "Successes",
                "Dimensions": [{"Name": "TaskId", "Value": task_id}],
                "Value": float(metrics.successes),
                "Unit": "Count",
            },
            {
                "MetricName": "Failures",
                "Dimensions": [{"Name": "TaskId", "Value": task_id}],
                "Value": float(metrics.failures),
                "Unit": "Count",
            },
            {
                "MetricName": "Timeouts",
                "Dimensions": [{"Name": "TaskId", "Value": task_id}],
                "Value": float(metrics.timeouts),
                "Unit": "Count",
            },
            {
                "MetricName": "AvgDuration",
                "Dimensions": [{"Name": "TaskId", "Value": task_id}],
                "Value": metrics.avg_duration_seconds,
                "Unit": "Seconds",
            },
        ]
        if metrics.successes + metrics.failures > 0:
            rate = metrics.successes / (metrics.successes + metrics.failures)
            metric_data.append(
                {
                    "MetricName": "SuccessRate",
                    "Dimensions": [{"Name": "TaskId", "Value": task_id}],
                    "Value": rate * 100,
                    "Unit": "Percent",
                }
            )
        self.cloudwatch.put_metric_data(
            Namespace=self.metrics_namespace,
            MetricData=metric_data,
        )
        return metrics

    def publish_group_metrics(self, group_name: str) -> dict[str, Any]:
        """Aggregate metrics across all tasks in a group and publish."""
        tasks = self.list_tasks_by_group(group_name)
        total_execs = 0
        total_success = 0
        total_fail = 0
        total_timeout = 0
        for task in tasks:
            m = self.compute_metrics(task.task_id)
            total_execs += m.total_executions
            total_success += m.successes
            total_fail += m.failures
            total_timeout += m.timeouts

        self.cloudwatch.put_metric_data(
            Namespace=self.metrics_namespace,
            MetricData=[
                {
                    "MetricName": "GroupTotalExecutions",
                    "Dimensions": [{"Name": "Group", "Value": group_name}],
                    "Value": float(total_execs),
                    "Unit": "Count",
                },
                {
                    "MetricName": "GroupSuccesses",
                    "Dimensions": [{"Name": "Group", "Value": group_name}],
                    "Value": float(total_success),
                    "Unit": "Count",
                },
                {
                    "MetricName": "GroupFailures",
                    "Dimensions": [{"Name": "Group", "Value": group_name}],
                    "Value": float(total_fail),
                    "Unit": "Count",
                },
            ],
        )
        return {
            "group": group_name,
            "total_executions": total_execs,
            "successes": total_success,
            "failures": total_fail,
            "timeouts": total_timeout,
        }

    # ======================================================================
    # Concurrent execution control
    # ======================================================================

    def count_running_executions(self, task_id: str) -> int:
        """Count RUNNING executions for a task."""
        execs = self.list_executions(task_id)
        return sum(1 for e in execs if e.status == ExecutionStatus.RUNNING)

    def start_execution_if_allowed(self, task_id: str) -> TaskExecution | None:
        """Start only if under max_concurrent limit."""
        task = self.get_task(task_id)
        if task is None:
            return None
        running = self.count_running_executions(task_id)
        if running >= task.max_concurrent:
            self._log_event(
                task_id,
                "N/A",
                f"Rejected: {running}/{task.max_concurrent} already running",
            )
            return None
        return self.start_execution(task_id)

    # ======================================================================
    # Task templates
    # ======================================================================

    def create_task_from_template(
        self,
        template: TaskDefinition,
        overrides: dict[str, Any] | None = None,
    ) -> TaskDefinition:
        """Create a new task from a template with optional overrides."""
        import copy

        task = copy.deepcopy(template)
        task.task_id = f"task-{uuid.uuid4().hex[:8]}"
        task.created_at = datetime.now(UTC).isoformat()
        if overrides:
            for key, value in overrides.items():
                if hasattr(task, key):
                    setattr(task, key, value)
        return self.create_task(task)

    # ======================================================================
    # Private helpers
    # ======================================================================

    def _rule_name(self, task_id: str) -> str:
        return f"sched-{task_id}"

    def _store_task_config(self, task: TaskDefinition) -> None:
        prefix = f"{self.config_prefix}/{task.task_id}"
        params = {
            f"{prefix}/max_retries": str(task.max_retries),
            f"{prefix}/timeout_seconds": str(task.timeout_seconds),
            f"{prefix}/max_concurrent": str(task.max_concurrent),
        }
        if task.input_payload:
            params[f"{prefix}/input_payload"] = json.dumps(task.input_payload)
        for name, value in params.items():
            self.ssm.put_parameter(Name=name, Value=value, Type="String", Overwrite=True)

    def _delete_task_config(self, task_id: str) -> None:
        prefix = f"{self.config_prefix}/{task_id}"
        try:
            resp = self.ssm.get_parameters_by_path(Path=prefix, Recursive=True)
            for p in resp.get("Parameters", []):
                self.ssm.delete_parameter(Name=p["Name"])
        except Exception:
            pass  # best-effort cleanup

    def _store_output(self, task_id: str, execution_id: str, data: dict[str, Any]) -> str:
        key = f"output/{task_id}/{execution_id}/result.json"
        self.s3.put_object(
            Bucket=self.output_bucket,
            Key=key,
            Body=json.dumps(data).encode(),
            ContentType="application/json",
        )
        return key

    def _get_execution_item(self, task_id: str, execution_id: str) -> dict[str, Any]:
        resp = self.dynamodb.get_item(
            TableName=self.executions_table,
            Key={
                "task_id": {"S": task_id},
                "execution_id": {"S": execution_id},
            },
        )
        return resp.get("Item", {})

    def _item_to_task(self, item: dict[str, Any]) -> TaskDefinition:
        return TaskDefinition(
            task_id=item["task_id"]["S"],
            name=item.get("name", {}).get("S", ""),
            group=item.get("group_name", {}).get("S", "default"),
            schedule_expression=item.get("schedule_expression", {}).get("S", ""),
            target_arn=item.get("target_arn", {}).get("S", ""),
            input_payload=json.loads(item.get("input_payload", {}).get("S", "{}")),
            enabled=item.get("enabled", {}).get("BOOL", True),
            max_retries=int(item.get("max_retries", {}).get("N", "3")),
            timeout_seconds=int(item.get("timeout_seconds", {}).get("N", "300")),
            max_concurrent=int(item.get("max_concurrent", {}).get("N", "1")),
            description=item.get("description", {}).get("S", ""),
            created_at=item.get("created_at", {}).get("S", ""),
        )

    def _item_to_execution(self, item: dict[str, Any]) -> TaskExecution:
        return TaskExecution(
            execution_id=item["execution_id"]["S"],
            task_id=item["task_id"]["S"],
            status=ExecutionStatus(item.get("status", {}).get("S", "PENDING")),
            started_at=item.get("started_at", {}).get("S", ""),
            finished_at=item.get("finished_at", {}).get("S", ""),
            attempt=int(item.get("attempt", {}).get("N", "1")),
            output_key=item.get("output_key", {}).get("S", ""),
            error_message=item.get("error_message", {}).get("S", ""),
            duration_seconds=float(item.get("duration_seconds", {}).get("N", "0")),
        )

    def _log_event(self, task_id: str, execution_id: str, message: str) -> None:
        ts = int(time.time() * 1000)
        stream_name = f"{task_id}/{execution_id}"
        # Ensure log stream exists
        try:
            self.logs.create_log_stream(logGroupName=self.log_group, logStreamName=stream_name)
        except Exception:
            pass  # already exists
        self.logs.put_log_events(
            logGroupName=self.log_group,
            logStreamName=stream_name,
            logEvents=[{"timestamp": ts, "message": message}],
        )

    def _publish_metric(
        self,
        task_id: str,
        metric_name: str,
        value: float,
        unit: str = "Count",
    ) -> None:
        self.cloudwatch.put_metric_data(
            Namespace=self.metrics_namespace,
            MetricData=[
                {
                    "MetricName": metric_name,
                    "Dimensions": [{"Name": "TaskId", "Value": task_id}],
                    "Value": value,
                    "Unit": unit,
                }
            ],
        )

    def _send_alert(self, alert: ExecutionAlert) -> None:
        self.sns.publish(
            TopicArn=self.alert_topic_arn,
            Subject=f"Task Alert: {alert.alert_type.value}",
            Message=json.dumps(
                {
                    "execution_id": alert.execution_id,
                    "task_id": alert.task_id,
                    "alert_type": alert.alert_type.value,
                    "message": alert.message,
                    "sent_at": alert.sent_at,
                }
            ),
        )
