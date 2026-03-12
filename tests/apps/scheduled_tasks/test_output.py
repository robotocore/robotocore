"""
Tests for execution output / artifact storage in S3.
"""

from __future__ import annotations

import json


class TestOutputStorage:
    def test_store_and_retrieve_output(self, scheduler, sample_task):
        """Store JSON output and retrieve it by execution ID."""
        exe = scheduler.start_execution(sample_task.task_id)
        data = {"rows_processed": 500, "errors": 0, "status": "ok"}
        key = scheduler.store_output(sample_task.task_id, exe.execution_id, data)
        assert key != ""

        retrieved = scheduler.get_output(sample_task.task_id, exe.execution_id)
        assert retrieved is not None
        assert retrieved["rows_processed"] == 500
        assert retrieved["status"] == "ok"

    def test_list_outputs_for_task(self, scheduler, sample_task):
        """List all output keys for a task across executions."""
        for i in range(3):
            exe = scheduler.start_execution(sample_task.task_id)
            scheduler.store_output(
                sample_task.task_id,
                exe.execution_id,
                {"batch": i},
            )

        keys = scheduler.list_outputs(sample_task.task_id)
        assert len(keys) == 3
        assert all(sample_task.task_id in k for k in keys)

    def test_large_output_binary(self, scheduler, sample_task, s3, output_bucket):
        """Store a large binary artifact."""
        exe = scheduler.start_execution(sample_task.task_id)
        payload = b"X" * 10_000
        key = scheduler.store_large_output(
            sample_task.task_id, exe.execution_id, payload, "big-report.bin"
        )
        assert "big-report.bin" in key

        resp = s3.get_object(Bucket=output_bucket, Key=key)
        body = resp["Body"].read()
        assert len(body) == 10_000

    def test_complete_stores_output(self, scheduler, sample_task, s3, output_bucket):
        """complete_execution with output stores it in S3 automatically."""
        exe = scheduler.start_execution(sample_task.task_id)
        result = scheduler.complete_execution(
            sample_task.task_id,
            exe.execution_id,
            output={"final": True},
        )
        assert result.output_key != ""

        resp = s3.get_object(Bucket=output_bucket, Key=result.output_key)
        data = json.loads(resp["Body"].read().decode())
        assert data["final"] is True
