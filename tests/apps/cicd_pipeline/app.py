"""
CI/CD Pipeline Application

A CI/CD build pipeline system similar to a simplified AWS CodePipeline.
Uses S3 for artifact storage, DynamoDB for build history, SSM for config,
SNS+SQS for notifications, CloudWatch Logs for build logs, and Step Functions
for orchestration.

Only uses boto3 -- no internal robotocore imports.
"""

from __future__ import annotations

import json
import time
import uuid
from datetime import UTC, datetime
from typing import Any

from .models import (
    Artifact,
    Build,
    BuildLog,
    BuildNotification,
    PipelineConfig,
    PipelineMetrics,
)


class CICDPipeline:
    """Manages a CI/CD build pipeline backed by AWS services."""

    # Build status constants
    QUEUED = "QUEUED"
    BUILDING = "BUILDING"
    TESTING = "TESTING"
    DEPLOYING = "DEPLOYING"
    SUCCESS = "SUCCESS"
    FAILED = "FAILED"
    CANCELLED = "CANCELLED"

    VALID_STATUSES = {QUEUED, BUILDING, TESTING, DEPLOYING, SUCCESS, FAILED, CANCELLED}
    TERMINAL_STATUSES = {SUCCESS, FAILED, CANCELLED}

    def __init__(
        self,
        s3_client: Any,
        dynamodb_client: Any,
        ssm_client: Any,
        sns_client: Any,
        sqs_client: Any,
        logs_client: Any,
        stepfunctions_client: Any,
        iam_client: Any,
        artifact_bucket: str,
        builds_table: str,
        config_prefix: str,
        log_group_prefix: str,
    ) -> None:
        self.s3 = s3_client
        self.dynamodb = dynamodb_client
        self.ssm = ssm_client
        self.sns = sns_client
        self.sqs = sqs_client
        self.logs = logs_client
        self.stepfunctions = stepfunctions_client
        self.iam = iam_client
        self.artifact_bucket = artifact_bucket
        self.builds_table = builds_table
        self.config_prefix = config_prefix
        self.log_group_prefix = log_group_prefix

    # -----------------------------------------------------------------------
    # Configuration management (SSM Parameter Store)
    # -----------------------------------------------------------------------

    def store_config(self, repo: str, config: PipelineConfig) -> None:
        """Store pipeline configuration in SSM Parameter Store."""
        prefix = f"{self.config_prefix}/{repo}"
        params = config.to_ssm_params(prefix)
        for name, value in params.items():
            self.ssm.put_parameter(Name=name, Value=value, Type="String", Overwrite=True)

    def get_config(self, repo: str) -> PipelineConfig:
        """Retrieve pipeline configuration from SSM Parameter Store."""
        prefix = f"{self.config_prefix}/{repo}"
        resp = self.ssm.get_parameters_by_path(Path=prefix, Recursive=True)
        params = {p["Name"]: p["Value"] for p in resp["Parameters"]}
        return PipelineConfig.from_ssm_params(params, prefix)

    def update_config_field(self, repo: str, field: str, value: str) -> None:
        """Update a single configuration field."""
        param_name = f"{self.config_prefix}/{repo}/{field}"
        self.ssm.put_parameter(Name=param_name, Value=value, Type="String", Overwrite=True)

    def delete_config(self, repo: str) -> None:
        """Delete all configuration for a repo."""
        prefix = f"{self.config_prefix}/{repo}"
        resp = self.ssm.get_parameters_by_path(Path=prefix, Recursive=True)
        for p in resp["Parameters"]:
            self.ssm.delete_parameter(Name=p["Name"])

    # -----------------------------------------------------------------------
    # Build lifecycle
    # -----------------------------------------------------------------------

    def queue_build(
        self,
        repo: str,
        branch: str,
        commit_sha: str,
        build_number: int = 0,
    ) -> Build:
        """Queue a new build. Returns the Build object with QUEUED status."""
        build_id = f"build-{uuid.uuid4().hex[:12]}"
        now = datetime.now(UTC).strftime("%Y-%m-%dT%H:%M:%SZ")
        build = Build(
            build_id=build_id,
            repo=repo,
            branch=branch,
            commit_sha=commit_sha,
            status=self.QUEUED,
            started_at=now,
            build_number=build_number,
        )
        self.dynamodb.put_item(
            TableName=self.builds_table,
            Item=build.to_dynamodb_item(),
        )
        # Ensure log group and stream exist
        self._ensure_log_stream(repo, build_id)
        self._write_log(repo, build_id, "INFO", f"Build {build_id} queued for {repo}@{branch}")
        return build

    def transition_build(self, build_id: str, new_status: str) -> Build:
        """Transition a build to a new status. Returns updated Build."""
        if new_status not in self.VALID_STATUSES:
            raise ValueError(f"Invalid status: {new_status}")

        update_expr = "SET #s = :s"
        expr_names: dict[str, str] = {"#s": "status"}
        expr_values: dict[str, dict[str, str]] = {":s": {"S": new_status}}

        if new_status in self.TERMINAL_STATUSES:
            now = datetime.now(UTC).strftime("%Y-%m-%dT%H:%M:%SZ")
            update_expr += ", finished_at = :f"
            expr_values[":f"] = {"S": now}

        self.dynamodb.update_item(
            TableName=self.builds_table,
            Key={"build_id": {"S": build_id}},
            UpdateExpression=update_expr,
            ExpressionAttributeNames=expr_names,
            ExpressionAttributeValues=expr_values,
        )
        return self.get_build(build_id)

    def fail_build(self, build_id: str, error_message: str) -> Build:
        """Mark a build as FAILED with an error message."""
        now = datetime.now(UTC).strftime("%Y-%m-%dT%H:%M:%SZ")
        self.dynamodb.update_item(
            TableName=self.builds_table,
            Key={"build_id": {"S": build_id}},
            UpdateExpression="SET #s = :s, finished_at = :f, error_message = :e",
            ExpressionAttributeNames={"#s": "status"},
            ExpressionAttributeValues={
                ":s": {"S": self.FAILED},
                ":f": {"S": now},
                ":e": {"S": error_message},
            },
        )
        build = self.get_build(build_id)
        self._write_log(build.repo, build_id, "ERROR", f"Build failed: {error_message}")
        return build

    def cancel_build(self, build_id: str) -> Build:
        """Cancel a build. Only non-terminal builds can be cancelled."""
        build = self.get_build(build_id)
        if build.status in self.TERMINAL_STATUSES:
            raise ValueError(f"Cannot cancel build in {build.status} state")
        return self.transition_build(build_id, self.CANCELLED)

    def retry_build(self, build_id: str) -> Build:
        """Retry a failed build with the same parameters."""
        original = self.get_build(build_id)
        if original.status != self.FAILED:
            raise ValueError("Can only retry FAILED builds")
        return self.queue_build(
            repo=original.repo,
            branch=original.branch,
            commit_sha=original.commit_sha,
            build_number=original.build_number + 1,
        )

    def get_build(self, build_id: str) -> Build:
        """Get a build by ID."""
        resp = self.dynamodb.get_item(
            TableName=self.builds_table,
            Key={"build_id": {"S": build_id}},
        )
        if "Item" not in resp:
            raise KeyError(f"Build {build_id} not found")
        return Build.from_dynamodb_item(resp["Item"])

    def list_builds_by_repo(self, repo: str, limit: int = 50) -> list[Build]:
        """List builds for a repo, ordered by start time."""
        resp = self.dynamodb.query(
            TableName=self.builds_table,
            IndexName="by-repo",
            KeyConditionExpression="repo_name = :r",
            ExpressionAttributeValues={":r": {"S": repo}},
            ScanIndexForward=False,
            Limit=limit,
        )
        return [Build.from_dynamodb_item(item) for item in resp["Items"]]

    def list_builds_by_status(self, status: str, limit: int = 50) -> list[Build]:
        """List builds with a given status."""
        resp = self.dynamodb.query(
            TableName=self.builds_table,
            IndexName="by-status",
            KeyConditionExpression="#s = :s",
            ExpressionAttributeNames={"#s": "status"},
            ExpressionAttributeValues={":s": {"S": status}},
            ScanIndexForward=False,
            Limit=limit,
        )
        return [Build.from_dynamodb_item(item) for item in resp["Items"]]

    # -----------------------------------------------------------------------
    # Artifact management (S3)
    # -----------------------------------------------------------------------

    def upload_artifact(
        self,
        build_id: str,
        artifact_name: str,
        content: bytes,
        commit_sha: str,
        branch: str,
        build_number: int = 0,
        environment: str = "staging",
    ) -> Artifact:
        """Upload a build artifact to S3 with metadata tags."""
        key = f"artifacts/{build_id}/{artifact_name}"
        metadata = {
            "commit-sha": commit_sha,
            "branch": branch,
            "build-number": str(build_number),
            "build-id": build_id,
            "environment": environment,
        }
        self.s3.put_object(
            Bucket=self.artifact_bucket,
            Key=key,
            Body=content,
            Metadata=metadata,
        )
        # Update build record with artifact key
        self.dynamodb.update_item(
            TableName=self.builds_table,
            Key={"build_id": {"S": build_id}},
            UpdateExpression="SET artifact_key = :k",
            ExpressionAttributeValues={":k": {"S": key}},
        )
        return Artifact(
            key=key,
            bucket=self.artifact_bucket,
            size=len(content),
            sha=commit_sha,
            tags=metadata,
            uploaded_at=datetime.now(UTC).strftime("%Y-%m-%dT%H:%M:%SZ"),
        )

    def get_artifact_metadata(self, key: str) -> dict[str, str]:
        """Get metadata for an artifact."""
        resp = self.s3.head_object(Bucket=self.artifact_bucket, Key=key)
        return resp["Metadata"]

    def download_artifact(self, key: str) -> bytes:
        """Download an artifact's content."""
        resp = self.s3.get_object(Bucket=self.artifact_bucket, Key=key)
        return resp["Body"].read()

    def list_artifacts(self, build_id: str | None = None, prefix: str = "artifacts/") -> list[str]:
        """List artifact keys, optionally filtered by build_id."""
        if build_id:
            prefix = f"artifacts/{build_id}/"
        resp = self.s3.list_objects_v2(Bucket=self.artifact_bucket, Prefix=prefix)
        return [obj["Key"] for obj in resp.get("Contents", [])]

    def delete_artifacts(self, build_id: str) -> int:
        """Delete all artifacts for a build. Returns count of deleted objects."""
        keys = self.list_artifacts(build_id=build_id)
        if not keys:
            return 0
        self.s3.delete_objects(
            Bucket=self.artifact_bucket,
            Delete={"Objects": [{"Key": k} for k in keys]},
        )
        return len(keys)

    def promote_artifact(
        self,
        source_key: str,
        target_environment: str,
    ) -> str:
        """Promote an artifact from one environment to another by copying with new metadata."""
        # Get existing metadata
        existing_meta = self.get_artifact_metadata(source_key)
        new_meta = dict(existing_meta)
        new_meta["environment"] = target_environment

        # Build the new key: artifacts/{build_id}/production/{artifact_name}
        parts = source_key.split("/")
        # parts: ["artifacts", build_id, artifact_name]
        build_id = parts[1]
        artifact_name = parts[-1]
        target_key = f"artifacts/{build_id}/{target_environment}/{artifact_name}"

        self.s3.copy_object(
            Bucket=self.artifact_bucket,
            Key=target_key,
            CopySource={"Bucket": self.artifact_bucket, "Key": source_key},
            Metadata=new_meta,
            MetadataDirective="REPLACE",
        )
        return target_key

    def calculate_storage_usage(self, repo: str | None = None) -> dict[str, Any]:
        """Calculate storage usage for artifacts."""
        resp = self.s3.list_objects_v2(Bucket=self.artifact_bucket, Prefix="artifacts/")
        contents = resp.get("Contents", [])
        total_size = sum(obj["Size"] for obj in contents)
        total_count = len(contents)
        return {
            "total_objects": total_count,
            "total_size_bytes": total_size,
        }

    # -----------------------------------------------------------------------
    # Build logging (CloudWatch Logs)
    # -----------------------------------------------------------------------

    def _log_group_name(self, repo: str) -> str:
        return f"{self.log_group_prefix}/{repo}"

    def _log_stream_name(self, build_id: str) -> str:
        return build_id

    def _ensure_log_stream(self, repo: str, build_id: str) -> None:
        """Create log group and stream if they don't exist."""
        group_name = self._log_group_name(repo)
        try:
            self.logs.create_log_group(logGroupName=group_name)
        except self.logs.exceptions.ResourceAlreadyExistsException:
            pass  # resource may not exist
        try:
            self.logs.create_log_stream(
                logGroupName=group_name,
                logStreamName=self._log_stream_name(build_id),
            )
        except self.logs.exceptions.ResourceAlreadyExistsException:
            pass  # resource may not exist

    def _write_log(self, repo: str, build_id: str, level: str, message: str) -> None:
        """Write a single log event."""
        self.logs.put_log_events(
            logGroupName=self._log_group_name(repo),
            logStreamName=self._log_stream_name(build_id),
            logEvents=[
                {
                    "timestamp": int(time.time() * 1000),
                    "message": f"[{level}] {message}",
                }
            ],
        )

    def write_build_logs(self, repo: str, build_id: str, logs: list[BuildLog]) -> None:
        """Write multiple log events for a build."""
        self._ensure_log_stream(repo, build_id)
        events = [
            {
                "timestamp": log.timestamp,
                "message": f"[{log.level}] {log.message}",
            }
            for log in logs
        ]
        if events:
            self.logs.put_log_events(
                logGroupName=self._log_group_name(repo),
                logStreamName=self._log_stream_name(build_id),
                logEvents=events,
            )

    def get_build_logs(self, repo: str, build_id: str) -> list[dict[str, Any]]:
        """Retrieve all logs for a build."""
        resp = self.logs.get_log_events(
            logGroupName=self._log_group_name(repo),
            logStreamName=self._log_stream_name(build_id),
            startFromHead=True,
        )
        return resp["events"]

    def filter_build_logs(
        self,
        repo: str,
        filter_pattern: str = "",
        start_time: int | None = None,
        end_time: int | None = None,
    ) -> list[dict[str, Any]]:
        """Filter logs across all builds for a repo."""
        kwargs: dict[str, Any] = {
            "logGroupName": self._log_group_name(repo),
        }
        if filter_pattern:
            kwargs["filterPattern"] = filter_pattern
        if start_time is not None:
            kwargs["startTime"] = start_time
        if end_time is not None:
            kwargs["endTime"] = end_time
        resp = self.logs.filter_log_events(**kwargs)
        return resp["events"]

    # -----------------------------------------------------------------------
    # Notifications (SNS -> SQS)
    # -----------------------------------------------------------------------

    def create_notification_topic(self, name: str) -> str:
        """Create an SNS topic for build notifications. Returns topic ARN."""
        resp = self.sns.create_topic(Name=name)
        return resp["TopicArn"]

    def subscribe_queue(self, topic_arn: str, queue_url: str) -> str:
        """Subscribe an SQS queue to the notification topic. Returns subscription ARN."""
        queue_arn = self.sqs.get_queue_attributes(QueueUrl=queue_url, AttributeNames=["QueueArn"])[
            "Attributes"
        ]["QueueArn"]
        resp = self.sns.subscribe(
            TopicArn=topic_arn,
            Protocol="sqs",
            Endpoint=queue_arn,
        )
        return resp["SubscriptionArn"]

    def notify_build_event(
        self,
        topic_arn: str,
        build: Build,
        event_type: str = "status_change",
    ) -> str:
        """Publish a build notification. Returns the message ID."""
        notification = BuildNotification(
            build_id=build.build_id,
            repo=build.repo,
            status=build.status,
            message=f"Build {build.build_id} is now {build.status}",
            timestamp=datetime.now(UTC).strftime("%Y-%m-%dT%H:%M:%SZ"),
        )
        resp = self.sns.publish(
            TopicArn=topic_arn,
            Message=json.dumps(
                {
                    "build_id": notification.build_id,
                    "repo": notification.repo,
                    "status": notification.status,
                    "message": notification.message,
                    "timestamp": notification.timestamp,
                    "event_type": event_type,
                }
            ),
            Subject=f"Build {build.status}: {build.repo}",
        )
        return resp["MessageId"]

    def receive_notifications(
        self,
        queue_url: str,
        max_messages: int = 10,
        wait_seconds: int = 5,
    ) -> list[dict[str, Any]]:
        """Receive notifications from an SQS queue. Returns parsed notification payloads."""
        messages = []
        deadline = time.time() + wait_seconds
        while not messages and time.time() < deadline:
            resp = self.sqs.receive_message(
                QueueUrl=queue_url,
                MaxNumberOfMessages=max_messages,
                WaitTimeSeconds=1,
            )
            for msg in resp.get("Messages", []):
                body = json.loads(msg["Body"])
                # SNS wraps the message in an envelope
                if "Message" in body:
                    payload = json.loads(body["Message"])
                else:
                    payload = body
                messages.append(payload)
        return messages

    # -----------------------------------------------------------------------
    # Pipeline orchestration (Step Functions)
    # -----------------------------------------------------------------------

    def create_pipeline_role(self, role_name: str) -> str:
        """Create an IAM role for the pipeline state machine. Returns role ARN."""
        assume_policy = json.dumps(
            {
                "Version": "2012-10-17",
                "Statement": [
                    {
                        "Effect": "Allow",
                        "Principal": {"Service": "states.amazonaws.com"},
                        "Action": "sts:AssumeRole",
                    }
                ],
            }
        )
        resp = self.iam.create_role(
            RoleName=role_name,
            AssumeRolePolicyDocument=assume_policy,
            Path="/",
        )
        return resp["Role"]["Arn"]

    def create_pipeline_state_machine(
        self,
        name: str,
        role_arn: str,
        with_deploy: bool = True,
    ) -> str:
        """Create a Step Functions state machine for the pipeline. Returns state machine ARN."""
        states: dict[str, Any] = {
            "Checkout": {
                "Type": "Pass",
                "Result": {"phase": "checkout", "status": "complete"},
                "Next": "Build",
            },
            "Build": {
                "Type": "Pass",
                "Result": {"phase": "build", "status": "complete"},
                "Next": "Test",
            },
            "Test": {
                "Type": "Pass",
                "Result": {"phase": "test", "status": "complete"},
            },
        }

        if with_deploy:
            states["Test"]["Next"] = "Deploy"
            states["Test"].pop("End", None)
            states["Deploy"] = {
                "Type": "Pass",
                "Result": {"phase": "deploy", "status": "complete"},
                "End": True,
            }
        else:
            states["Test"]["End"] = True

        definition = json.dumps(
            {
                "Comment": f"CI/CD Pipeline: {name}",
                "StartAt": "Checkout",
                "States": states,
            }
        )

        resp = self.stepfunctions.create_state_machine(
            name=name,
            definition=definition,
            roleArn=role_arn,
        )
        return resp["stateMachineArn"]

    def execute_pipeline(
        self,
        state_machine_arn: str,
        build_id: str,
        repo: str,
        branch: str,
        commit_sha: str,
    ) -> str:
        """Start a pipeline execution. Returns execution ARN."""
        resp = self.stepfunctions.start_execution(
            stateMachineArn=state_machine_arn,
            input=json.dumps(
                {
                    "build_id": build_id,
                    "repo": repo,
                    "branch": branch,
                    "commit_sha": commit_sha,
                }
            ),
        )
        return resp["executionArn"]

    def get_execution_status(self, execution_arn: str) -> str:
        """Get the status of a pipeline execution."""
        resp = self.stepfunctions.describe_execution(executionArn=execution_arn)
        return resp["status"]

    def describe_state_machine(self, state_machine_arn: str) -> dict[str, Any]:
        """Describe a state machine."""
        resp = self.stepfunctions.describe_state_machine(stateMachineArn=state_machine_arn)
        return {
            "name": resp["name"],
            "arn": resp["stateMachineArn"],
            "definition": json.loads(resp["definition"]),
            "status": resp.get("status", "ACTIVE"),
        }

    # -----------------------------------------------------------------------
    # Pipeline metrics
    # -----------------------------------------------------------------------

    def get_pipeline_metrics(self, repo: str) -> PipelineMetrics:
        """Calculate pipeline metrics for a repo."""
        builds = self.list_builds_by_repo(repo, limit=1000)
        if not builds:
            return PipelineMetrics(
                total_builds=0,
                success_count=0,
                failure_count=0,
                avg_duration_seconds=0.0,
            )

        success_count = sum(1 for b in builds if b.status == self.SUCCESS)
        failure_count = sum(1 for b in builds if b.status == self.FAILED)

        # Calculate average duration for completed builds
        durations = []
        for b in builds:
            if b.finished_at and b.started_at:
                try:
                    start = datetime.strptime(b.started_at, "%Y-%m-%dT%H:%M:%SZ")
                    end = datetime.strptime(b.finished_at, "%Y-%m-%dT%H:%M:%SZ")
                    durations.append((end - start).total_seconds())
                except ValueError:
                    pass  # conversion may fail; not critical

        avg_duration = sum(durations) / len(durations) if durations else 0.0

        return PipelineMetrics(
            total_builds=len(builds),
            success_count=success_count,
            failure_count=failure_count,
            avg_duration_seconds=avg_duration,
        )

    # -----------------------------------------------------------------------
    # Build comparison
    # -----------------------------------------------------------------------

    def compare_builds(self, build_id_a: str, build_id_b: str) -> dict[str, Any]:
        """Compare two builds' metadata."""
        build_a = self.get_build(build_id_a)
        build_b = self.get_build(build_id_b)
        differences: dict[str, dict[str, Any]] = {}
        for field_name in ("repo", "branch", "commit_sha", "status", "build_number"):
            val_a = getattr(build_a, field_name)
            val_b = getattr(build_b, field_name)
            if val_a != val_b:
                differences[field_name] = {"a": val_a, "b": val_b}
        return {
            "build_a": build_id_a,
            "build_b": build_id_b,
            "differences": differences,
            "same_repo": build_a.repo == build_b.repo,
            "same_branch": build_a.branch == build_b.branch,
        }

    # -----------------------------------------------------------------------
    # Cleanup helpers
    # -----------------------------------------------------------------------

    def cleanup_log_group(self, repo: str) -> None:
        """Delete the log group for a repo (best-effort)."""
        try:
            self.logs.delete_log_group(logGroupName=self._log_group_name(repo))
        except Exception:
            pass  # best-effort cleanup

    def cleanup_all_artifacts(self) -> None:
        """Delete all artifacts in the bucket."""
        resp = self.s3.list_objects_v2(Bucket=self.artifact_bucket, Prefix="artifacts/")
        contents = resp.get("Contents", [])
        if contents:
            self.s3.delete_objects(
                Bucket=self.artifact_bucket,
                Delete={"Objects": [{"Key": obj["Key"]} for obj in contents]},
            )
