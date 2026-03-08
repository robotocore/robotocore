"""
CI/CD Build Pipeline Application Tests

Simulates a CI/CD build system where:
- Build artifacts are stored in S3 (versioned, with lifecycle)
- Build history is tracked in DynamoDB (with GSIs for querying by repo and status)
- Pipeline configuration lives in SSM Parameter Store
- Build notifications go through SNS -> SQS
- Build logs stream to CloudWatch Logs
- Pipeline orchestration uses Step Functions
"""

import json
import os
import time
import uuid

import pytest

ENDPOINT_URL = os.environ.get("AWS_ENDPOINT_URL", "http://localhost:4566")


# ---------------------------------------------------------------------------
# Client fixtures (for services not in conftest)
# ---------------------------------------------------------------------------


@pytest.fixture
def logs(boto_session):
    return boto_session.client("logs", endpoint_url=ENDPOINT_URL)


@pytest.fixture
def ssm(boto_session):
    return boto_session.client("ssm", endpoint_url=ENDPOINT_URL)


# ---------------------------------------------------------------------------
# Resource fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
def artifact_bucket(s3, unique_name):
    bucket_name = f"artifacts-{unique_name}"
    s3.create_bucket(Bucket=bucket_name)
    yield bucket_name
    # Cleanup: delete all objects then the bucket
    objs = s3.list_objects_v2(Bucket=bucket_name).get("Contents", [])
    if objs:
        s3.delete_objects(
            Bucket=bucket_name,
            Delete={"Objects": [{"Key": o["Key"]} for o in objs]},
        )
    s3.delete_bucket(Bucket=bucket_name)


@pytest.fixture
def builds_table(dynamodb, unique_name):
    table_name = f"builds-{unique_name}"
    dynamodb.create_table(
        TableName=table_name,
        KeySchema=[{"AttributeName": "build_id", "KeyType": "HASH"}],
        AttributeDefinitions=[
            {"AttributeName": "build_id", "AttributeType": "S"},
            {"AttributeName": "repo_name", "AttributeType": "S"},
            {"AttributeName": "status", "AttributeType": "S"},
            {"AttributeName": "started_at", "AttributeType": "S"},
        ],
        GlobalSecondaryIndexes=[
            {
                "IndexName": "by-repo",
                "KeySchema": [
                    {"AttributeName": "repo_name", "KeyType": "HASH"},
                    {"AttributeName": "started_at", "KeyType": "RANGE"},
                ],
                "Projection": {"ProjectionType": "ALL"},
            },
            {
                "IndexName": "by-status",
                "KeySchema": [
                    {"AttributeName": "status", "KeyType": "HASH"},
                    {"AttributeName": "started_at", "KeyType": "RANGE"},
                ],
                "Projection": {"ProjectionType": "ALL"},
            },
        ],
        BillingMode="PAY_PER_REQUEST",
    )
    yield table_name
    dynamodb.delete_table(TableName=table_name)


@pytest.fixture
def pipeline_config(ssm, unique_name):
    prefix = f"/cicd/{unique_name}"
    params = {
        f"{prefix}/default_branch": "main",
        f"{prefix}/timeout_minutes": "30",
        f"{prefix}/parallel_jobs": "4",
    }
    for name, value in params.items():
        ssm.put_parameter(Name=name, Value=value, Type="String")
    yield prefix, params
    for name in params:
        ssm.delete_parameter(Name=name)


@pytest.fixture
def build_notifications(sns, unique_name):
    topic_name = f"build-notify-{unique_name}"
    resp = sns.create_topic(Name=topic_name)
    topic_arn = resp["TopicArn"]
    yield topic_arn
    sns.delete_topic(TopicArn=topic_arn)


@pytest.fixture
def build_logs(logs, unique_name):
    group_name = f"/cicd/{unique_name}/builds"
    stream_name = "build-output"
    logs.create_log_group(logGroupName=group_name)
    logs.create_log_stream(logGroupName=group_name, logStreamName=stream_name)
    yield group_name, stream_name
    logs.delete_log_group(logGroupName=group_name)


@pytest.fixture
def pipeline_role(iam, unique_name):
    role_name = f"pipeline-role-{unique_name}"
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
    resp = iam.create_role(
        RoleName=role_name,
        AssumeRolePolicyDocument=assume_policy,
        Path="/",
    )
    role_arn = resp["Role"]["Arn"]
    yield role_arn, role_name
    iam.delete_role(RoleName=role_name)


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


class TestArtifactManagement:
    """S3-based build artifact storage."""

    def test_upload_build_artifact(self, s3, artifact_bucket):
        build_id = f"build-{uuid.uuid4().hex[:8]}"
        key = f"artifacts/{build_id}/app.zip"
        s3.put_object(Bucket=artifact_bucket, Key=key, Body=b"fake zip content")

        resp = s3.get_object(Bucket=artifact_bucket, Key=key)
        body = resp["Body"].read()
        assert body == b"fake zip content"

    def test_artifact_metadata(self, s3, artifact_bucket):
        build_id = f"build-{uuid.uuid4().hex[:8]}"
        key = f"artifacts/{build_id}/app.zip"
        metadata = {
            "commitsha": "abc123def456",
            "branch": "feature/login",
            "buildnumber": "42",
        }
        s3.put_object(
            Bucket=artifact_bucket,
            Key=key,
            Body=b"zip bytes",
            Metadata=metadata,
        )

        resp = s3.head_object(Bucket=artifact_bucket, Key=key)
        assert resp["Metadata"]["commitsha"] == "abc123def456"
        assert resp["Metadata"]["branch"] == "feature/login"
        assert resp["Metadata"]["buildnumber"] == "42"

    def test_list_artifacts_by_build(self, s3, artifact_bucket):
        build_id = f"build-{uuid.uuid4().hex[:8]}"
        for name in ["app.zip", "tests-report.xml", "coverage.json"]:
            s3.put_object(
                Bucket=artifact_bucket,
                Key=f"artifacts/{build_id}/{name}",
                Body=b"data",
            )

        resp = s3.list_objects_v2(Bucket=artifact_bucket, Prefix=f"artifacts/{build_id}/")
        assert resp["KeyCount"] == 3
        keys = sorted(o["Key"].split("/")[-1] for o in resp["Contents"])
        assert keys == ["app.zip", "coverage.json", "tests-report.xml"]

    def test_artifact_overwrite(self, s3, artifact_bucket):
        build_id = f"build-{uuid.uuid4().hex[:8]}"
        key = f"artifacts/{build_id}/app.zip"
        s3.put_object(Bucket=artifact_bucket, Key=key, Body=b"version-1")
        s3.put_object(Bucket=artifact_bucket, Key=key, Body=b"version-2")

        resp = s3.get_object(Bucket=artifact_bucket, Key=key)
        assert resp["Body"].read() == b"version-2"


class TestBuildHistory:
    """DynamoDB-based build history tracking."""

    def test_record_build(self, dynamodb, builds_table):
        build_id = f"build-{uuid.uuid4().hex[:8]}"
        item = {
            "build_id": {"S": build_id},
            "repo_name": {"S": "myorg/myapp"},
            "status": {"S": "running"},
            "started_at": {"S": "2026-03-08T10:00:00Z"},
            "commit_sha": {"S": "deadbeef"},
        }
        dynamodb.put_item(TableName=builds_table, Item=item)

        resp = dynamodb.get_item(TableName=builds_table, Key={"build_id": {"S": build_id}})
        assert resp["Item"]["repo_name"]["S"] == "myorg/myapp"
        assert resp["Item"]["status"]["S"] == "running"
        assert resp["Item"]["commit_sha"]["S"] == "deadbeef"

    def test_query_builds_by_repo(self, dynamodb, builds_table):
        # Insert 4 builds for repo-A, 2 for repo-B
        for i in range(4):
            dynamodb.put_item(
                TableName=builds_table,
                Item={
                    "build_id": {"S": f"build-a-{i}"},
                    "repo_name": {"S": "org/repo-A"},
                    "status": {"S": "success"},
                    "started_at": {"S": f"2026-03-08T10:0{i}:00Z"},
                },
            )
        for i in range(2):
            dynamodb.put_item(
                TableName=builds_table,
                Item={
                    "build_id": {"S": f"build-b-{i}"},
                    "repo_name": {"S": "org/repo-B"},
                    "status": {"S": "success"},
                    "started_at": {"S": f"2026-03-08T11:0{i}:00Z"},
                },
            )

        resp = dynamodb.query(
            TableName=builds_table,
            IndexName="by-repo",
            KeyConditionExpression="repo_name = :r",
            ExpressionAttributeValues={":r": {"S": "org/repo-A"}},
        )
        assert resp["Count"] == 4

    def test_query_builds_by_status(self, dynamodb, builds_table):
        statuses = ["running", "running", "success", "success", "success", "failed"]
        for i, status in enumerate(statuses):
            dynamodb.put_item(
                TableName=builds_table,
                Item={
                    "build_id": {"S": f"build-status-{i}"},
                    "repo_name": {"S": "org/myrepo"},
                    "status": {"S": status},
                    "started_at": {"S": f"2026-03-08T12:{i:02d}:00Z"},
                },
            )

        resp = dynamodb.query(
            TableName=builds_table,
            IndexName="by-status",
            KeyConditionExpression="#s = :s",
            ExpressionAttributeNames={"#s": "status"},
            ExpressionAttributeValues={":s": {"S": "success"}},
        )
        assert resp["Count"] == 3

    def test_update_build_status(self, dynamodb, builds_table):
        build_id = f"build-{uuid.uuid4().hex[:8]}"
        dynamodb.put_item(
            TableName=builds_table,
            Item={
                "build_id": {"S": build_id},
                "repo_name": {"S": "org/app"},
                "status": {"S": "running"},
                "started_at": {"S": "2026-03-08T14:00:00Z"},
            },
        )

        dynamodb.update_item(
            TableName=builds_table,
            Key={"build_id": {"S": build_id}},
            UpdateExpression="SET #s = :s, finished_at = :f, duration_seconds = :d",
            ExpressionAttributeNames={"#s": "status"},
            ExpressionAttributeValues={
                ":s": {"S": "success"},
                ":f": {"S": "2026-03-08T14:05:00Z"},
                ":d": {"N": "300"},
            },
        )

        resp = dynamodb.get_item(TableName=builds_table, Key={"build_id": {"S": build_id}})
        assert resp["Item"]["status"]["S"] == "success"
        assert resp["Item"]["finished_at"]["S"] == "2026-03-08T14:05:00Z"
        assert resp["Item"]["duration_seconds"]["N"] == "300"

    def test_batch_record_builds(self, dynamodb, builds_table):
        items = []
        for i in range(12):
            items.append(
                {
                    "PutRequest": {
                        "Item": {
                            "build_id": {"S": f"batch-build-{i}"},
                            "repo_name": {"S": "org/batch-repo"},
                            "status": {"S": "success"},
                            "started_at": {"S": f"2026-03-08T15:{i:02d}:00Z"},
                        }
                    }
                }
            )
        # BatchWriteItem supports max 25 items; 12 is fine in one call
        dynamodb.batch_write_item(RequestItems={builds_table: items})

        resp = dynamodb.scan(TableName=builds_table, Select="COUNT")
        assert resp["Count"] == 12


class TestBuildLogs:
    """CloudWatch Logs-based build log streaming."""

    def test_stream_build_output(self, logs, build_logs):
        group_name, stream_name = build_logs
        now = int(time.time() * 1000)
        events = [
            {"timestamp": now + i * 1000, "message": f"[step {i}] Building..."} for i in range(10)
        ]
        logs.put_log_events(
            logGroupName=group_name,
            logStreamName=stream_name,
            logEvents=events,
        )

        resp = logs.get_log_events(
            logGroupName=group_name,
            logStreamName=stream_name,
            startFromHead=True,
        )
        returned = resp["events"]
        assert len(returned) == 10
        assert returned[0]["message"] == "[step 0] Building..."
        assert returned[9]["message"] == "[step 9] Building..."

    def test_filter_error_logs(self, logs, build_logs):
        group_name, stream_name = build_logs
        now = int(time.time() * 1000)
        messages = [
            "INFO: Compiling source",
            "INFO: Running tests",
            "ERROR: Test suite failed",
            "WARN: Deprecated API usage",
            "ERROR: Build aborted",
            "INFO: Cleaning up",
        ]
        events = [{"timestamp": now + i * 1000, "message": msg} for i, msg in enumerate(messages)]
        logs.put_log_events(
            logGroupName=group_name,
            logStreamName=stream_name,
            logEvents=events,
        )

        resp = logs.filter_log_events(
            logGroupName=group_name,
            filterPattern="ERROR",
        )
        error_messages = [e["message"] for e in resp["events"]]
        assert len(error_messages) == 2
        assert all("ERROR" in m for m in error_messages)

    def test_multi_step_logs(self, logs, unique_name):
        group_name = f"/cicd/{unique_name}/multi-step"
        logs.create_log_group(logGroupName=group_name)
        try:
            streams = ["build", "test", "deploy"]
            for stream in streams:
                logs.create_log_stream(logGroupName=group_name, logStreamName=stream)
                now = int(time.time() * 1000)
                logs.put_log_events(
                    logGroupName=group_name,
                    logStreamName=stream,
                    logEvents=[
                        {
                            "timestamp": now,
                            "message": f"Running {stream} phase",
                        }
                    ],
                )

            # Verify isolation: each stream has only its own events
            for stream in streams:
                resp = logs.get_log_events(
                    logGroupName=group_name,
                    logStreamName=stream,
                    startFromHead=True,
                )
                msgs = [e["message"] for e in resp["events"]]
                assert len(msgs) == 1
                assert msgs[0] == f"Running {stream} phase"
        finally:
            logs.delete_log_group(logGroupName=group_name)


class TestPipelineConfig:
    """SSM Parameter Store config and SNS notifications."""

    def test_read_pipeline_config(self, ssm, pipeline_config):
        prefix, expected_params = pipeline_config
        resp = ssm.get_parameters_by_path(Path=prefix, Recursive=True)
        params = {p["Name"]: p["Value"] for p in resp["Parameters"]}
        assert params[f"{prefix}/default_branch"] == "main"
        assert params[f"{prefix}/timeout_minutes"] == "30"
        assert params[f"{prefix}/parallel_jobs"] == "4"

    def test_update_config(self, ssm, pipeline_config):
        prefix, _ = pipeline_config
        param_name = f"{prefix}/timeout_minutes"
        ssm.put_parameter(Name=param_name, Value="60", Type="String", Overwrite=True)

        resp = ssm.get_parameter(Name=param_name)
        assert resp["Parameter"]["Value"] == "60"

    def test_build_notification(self, sns, sqs, build_notifications, unique_name):
        topic_arn = build_notifications

        # Create an SQS queue to subscribe
        queue_resp = sqs.create_queue(QueueName=f"notify-sub-{unique_name}")
        queue_url = queue_resp["QueueUrl"]
        queue_arn = sqs.get_queue_attributes(QueueUrl=queue_url, AttributeNames=["QueueArn"])[
            "Attributes"
        ]["QueueArn"]

        try:
            sns.subscribe(TopicArn=topic_arn, Protocol="sqs", Endpoint=queue_arn)

            notification = json.dumps(
                {
                    "build_id": "build-notify-001",
                    "status": "success",
                    "repo": "org/myapp",
                }
            )
            sns.publish(TopicArn=topic_arn, Message=notification, Subject="Build Success")

            # Poll for the message
            messages = []
            deadline = time.time() + 10
            while not messages and time.time() < deadline:
                resp = sqs.receive_message(
                    QueueUrl=queue_url, MaxNumberOfMessages=10, WaitTimeSeconds=1
                )
                messages.extend(resp.get("Messages", []))

            assert len(messages) >= 1
            body = json.loads(messages[0]["Body"])
            # SNS wraps in envelope
            inner = json.loads(body["Message"]) if "Message" in body else body
            assert inner["build_id"] == "build-notify-001"
            assert inner["status"] == "success"
        finally:
            sqs.delete_queue(QueueUrl=queue_url)


class TestPipelineOrchestration:
    """Step Functions-based pipeline orchestration."""

    def test_create_state_machine(self, stepfunctions, pipeline_role, unique_name):
        role_arn, _ = pipeline_role
        sm_name = f"pipeline-{unique_name}"
        definition = json.dumps(
            {
                "Comment": "CI/CD Pipeline",
                "StartAt": "Build",
                "States": {"Build": {"Type": "Pass", "Result": "built", "End": True}},
            }
        )

        create_resp = stepfunctions.create_state_machine(
            name=sm_name,
            definition=definition,
            roleArn=role_arn,
        )
        sm_arn = create_resp["stateMachineArn"]
        try:
            assert "stateMachineArn" in create_resp

            desc = stepfunctions.describe_state_machine(stateMachineArn=sm_arn)
            assert desc["name"] == sm_name
            parsed_def = json.loads(desc["definition"])
            assert parsed_def["StartAt"] == "Build"
        finally:
            stepfunctions.delete_state_machine(stateMachineArn=sm_arn)

    def test_execute_state_machine(self, stepfunctions, pipeline_role, unique_name):
        role_arn, _ = pipeline_role
        sm_name = f"exec-pipeline-{unique_name}"
        definition = json.dumps(
            {
                "Comment": "Simple pass-through",
                "StartAt": "Done",
                "States": {
                    "Done": {
                        "Type": "Pass",
                        "Result": {"status": "success"},
                        "End": True,
                    }
                },
            }
        )

        create_resp = stepfunctions.create_state_machine(
            name=sm_name, definition=definition, roleArn=role_arn
        )
        sm_arn = create_resp["stateMachineArn"]
        try:
            exec_resp = stepfunctions.start_execution(
                stateMachineArn=sm_arn,
                input=json.dumps({"build_id": "exec-001"}),
            )
            exec_arn = exec_resp["executionArn"]
            assert "executionArn" in exec_resp

            desc = stepfunctions.describe_execution(executionArn=exec_arn)
            assert desc["status"] in ("RUNNING", "SUCCEEDED")
        finally:
            stepfunctions.delete_state_machine(stateMachineArn=sm_arn)

    def test_full_build_pipeline(
        self,
        s3,
        dynamodb,
        ssm,
        sns,
        sqs,
        logs,
        artifact_bucket,
        builds_table,
        pipeline_config,
        build_notifications,
        build_logs,
        unique_name,
    ):
        """End-to-end: config -> record build -> upload artifact -> logs -> update -> notify."""
        build_id = f"e2e-{uuid.uuid4().hex[:8]}"
        prefix, _ = pipeline_config
        topic_arn = build_notifications
        group_name, stream_name = build_logs

        # 1. Read config from SSM
        config_resp = ssm.get_parameters_by_path(Path=prefix, Recursive=True)
        config = {p["Name"].split("/")[-1]: p["Value"] for p in config_resp["Parameters"]}
        assert config["default_branch"] == "main"

        # 2. Record build start in DynamoDB
        dynamodb.put_item(
            TableName=builds_table,
            Item={
                "build_id": {"S": build_id},
                "repo_name": {"S": "org/fullpipeline"},
                "status": {"S": "running"},
                "started_at": {"S": "2026-03-08T20:00:00Z"},
                "branch": {"S": config["default_branch"]},
            },
        )

        # 3. Upload artifact to S3
        artifact_key = f"artifacts/{build_id}/app.zip"
        s3.put_object(
            Bucket=artifact_bucket,
            Key=artifact_key,
            Body=b"full pipeline artifact",
            Metadata={"buildid": build_id},
        )

        # 4. Write build logs to CloudWatch
        now = int(time.time() * 1000)
        logs.put_log_events(
            logGroupName=group_name,
            logStreamName=stream_name,
            logEvents=[
                {"timestamp": now, "message": f"[{build_id}] Build started"},
                {"timestamp": now + 1000, "message": f"[{build_id}] Tests passed"},
                {"timestamp": now + 2000, "message": f"[{build_id}] Build complete"},
            ],
        )

        # 5. Update build status to success
        dynamodb.update_item(
            TableName=builds_table,
            Key={"build_id": {"S": build_id}},
            UpdateExpression="SET #s = :s, finished_at = :f",
            ExpressionAttributeNames={"#s": "status"},
            ExpressionAttributeValues={
                ":s": {"S": "success"},
                ":f": {"S": "2026-03-08T20:05:00Z"},
            },
        )

        # 6. Publish notification via SNS
        notify_queue_resp = sqs.create_queue(QueueName=f"e2e-notify-{unique_name}")
        notify_url = notify_queue_resp["QueueUrl"]
        notify_arn = sqs.get_queue_attributes(QueueUrl=notify_url, AttributeNames=["QueueArn"])[
            "Attributes"
        ]["QueueArn"]

        try:
            sns.subscribe(TopicArn=topic_arn, Protocol="sqs", Endpoint=notify_arn)
            sns.publish(
                TopicArn=topic_arn,
                Message=json.dumps({"build_id": build_id, "status": "success"}),
                Subject="Build Complete",
            )

            # 7. Receive notification from subscriber queue
            messages = []
            deadline = time.time() + 10
            while not messages and time.time() < deadline:
                resp = sqs.receive_message(
                    QueueUrl=notify_url, MaxNumberOfMessages=10, WaitTimeSeconds=1
                )
                messages.extend(resp.get("Messages", []))

            assert len(messages) >= 1
            body = json.loads(messages[0]["Body"])
            inner = json.loads(body["Message"]) if "Message" in body else body
            assert inner["build_id"] == build_id
            assert inner["status"] == "success"

            # 8. Verify final state in DynamoDB
            final = dynamodb.get_item(TableName=builds_table, Key={"build_id": {"S": build_id}})
            assert final["Item"]["status"]["S"] == "success"
            assert final["Item"]["finished_at"]["S"] == "2026-03-08T20:05:00Z"

            # Verify artifact is in S3
            art_resp = s3.head_object(Bucket=artifact_bucket, Key=artifact_key)
            assert art_resp["Metadata"]["buildid"] == build_id

            # Verify logs were written
            log_resp = logs.get_log_events(
                logGroupName=group_name,
                logStreamName=stream_name,
                startFromHead=True,
            )
            log_messages = [e["message"] for e in log_resp["events"]]
            assert any(build_id in m for m in log_messages)
        finally:
            sqs.delete_queue(QueueUrl=notify_url)
