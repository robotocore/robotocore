"""
Tests for execution alerts via SNS -> SQS.
"""

from __future__ import annotations

import json
import time

from ..scheduled_tasks.models import TaskDefinition


class TestAlerts:
    def _poll_messages(self, sqs, queue_url, timeout=10):
        """Poll the SQS queue until at least one message arrives."""
        messages = []
        deadline = time.time() + timeout
        while not messages and time.time() < deadline:
            resp = sqs.receive_message(
                QueueUrl=queue_url, MaxNumberOfMessages=10, WaitTimeSeconds=1
            )
            messages.extend(resp.get("Messages", []))
        return messages

    def _extract_alert(self, message):
        """Unwrap SNS envelope from SQS message body."""
        body = json.loads(message["Body"])
        if "Message" in body:
            return json.loads(body["Message"])
        return body

    def test_failure_triggers_alert(self, scheduler, sample_task, sqs, alert_queue):
        """A failed execution sends a FAILURE alert via SNS."""
        queue_url, _ = alert_queue
        exe = scheduler.start_execution(sample_task.task_id)
        scheduler.fail_execution(sample_task.task_id, exe.execution_id, "OOMKilled")

        messages = self._poll_messages(sqs, queue_url)
        assert len(messages) >= 1
        alert = self._extract_alert(messages[0])
        assert alert["alert_type"] == "FAILURE"
        assert alert["task_id"] == sample_task.task_id
        assert "OOMKilled" in alert["message"]

    def test_success_alert(self, scheduler, sample_task, sqs, alert_queue):
        """Explicit success alert contains task details."""
        queue_url, _ = alert_queue
        exe = scheduler.start_execution(sample_task.task_id)
        scheduler.complete_execution(sample_task.task_id, exe.execution_id)
        scheduler.send_success_alert(
            sample_task.task_id, exe.execution_id, "All 500 rows processed"
        )

        messages = self._poll_messages(sqs, queue_url)
        assert len(messages) >= 1
        alert = self._extract_alert(messages[0])
        assert alert["alert_type"] == "SUCCESS"
        assert alert["task_id"] == sample_task.task_id

    def test_timeout_alert(self, scheduler, sample_task, sqs, alert_queue):
        """A timed-out execution sends a TIMEOUT alert."""
        queue_url, _ = alert_queue
        exe = scheduler.start_execution(sample_task.task_id)
        scheduler.timeout_execution(sample_task.task_id, exe.execution_id)

        messages = self._poll_messages(sqs, queue_url)
        assert len(messages) >= 1
        alert = self._extract_alert(messages[0])
        assert alert["alert_type"] == "TIMEOUT"
        assert exe.execution_id in alert["execution_id"]

    def test_alert_contains_details(self, scheduler, sample_task, sqs, alert_queue):
        """Alert JSON payload includes execution_id, task_id, sent_at."""
        queue_url, _ = alert_queue
        exe = scheduler.start_execution(sample_task.task_id)
        scheduler.fail_execution(sample_task.task_id, exe.execution_id, "disk full")

        messages = self._poll_messages(sqs, queue_url)
        assert len(messages) >= 1
        alert = self._extract_alert(messages[0])
        assert "execution_id" in alert
        assert "task_id" in alert
        assert "sent_at" in alert
        assert "message" in alert

    def test_alert_received_via_sqs(self, scheduler, sqs, alert_queue):
        """Create a fresh task, fail it, and confirm SQS delivery."""
        queue_url, _ = alert_queue
        task = scheduler.create_task(TaskDefinition(name="sqs-alert-test", group="test"))
        exe = scheduler.start_execution(task.task_id)
        scheduler.fail_execution(task.task_id, exe.execution_id, "test error")

        messages = self._poll_messages(sqs, queue_url)
        assert len(messages) >= 1
        alert = self._extract_alert(messages[0])
        assert alert["task_id"] == task.task_id

        scheduler.delete_task(task.task_id)
