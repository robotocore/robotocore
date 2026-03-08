"""
Scheduled Task / Job Execution System Tests

Simulates a cron-like task scheduler where:
- EventBridge rules define schedules and event-driven triggers
- Task definitions and execution history in DynamoDB
- Task configuration in SSM Parameter Store
- Task output/artifacts stored in S3
- Execution alerts via SNS
- Execution logs in CloudWatch Logs
- Execution metrics in CloudWatch
"""

import json
import time

import pytest

# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
def tasks_table(dynamodb, unique_name):
    table_name = f"scheduler-tasks-{unique_name}"
    dynamodb.create_table(
        TableName=table_name,
        KeySchema=[
            {"AttributeName": "task_id", "KeyType": "HASH"},
            {"AttributeName": "execution_id", "KeyType": "RANGE"},
        ],
        AttributeDefinitions=[
            {"AttributeName": "task_id", "AttributeType": "S"},
            {"AttributeName": "execution_id", "AttributeType": "S"},
            {"AttributeName": "status", "AttributeType": "S"},
            {"AttributeName": "scheduled_at", "AttributeType": "S"},
            {"AttributeName": "schedule_name", "AttributeType": "S"},
            {"AttributeName": "next_run", "AttributeType": "S"},
        ],
        GlobalSecondaryIndexes=[
            {
                "IndexName": "by-status",
                "KeySchema": [
                    {"AttributeName": "status", "KeyType": "HASH"},
                    {"AttributeName": "scheduled_at", "KeyType": "RANGE"},
                ],
                "Projection": {"ProjectionType": "ALL"},
            },
            {
                "IndexName": "by-schedule",
                "KeySchema": [
                    {"AttributeName": "schedule_name", "KeyType": "HASH"},
                    {"AttributeName": "next_run", "KeyType": "RANGE"},
                ],
                "Projection": {"ProjectionType": "ALL"},
            },
        ],
        BillingMode="PAY_PER_REQUEST",
    )
    yield table_name
    dynamodb.delete_table(TableName=table_name)


@pytest.fixture
def task_config(ssm, unique_name):
    prefix = f"/scheduler/{unique_name}"
    params = {
        f"{prefix}/max_retries": "3",
        f"{prefix}/timeout_seconds": "300",
        f"{prefix}/concurrency_limit": "10",
    }
    for name, value in params.items():
        ssm.put_parameter(Name=name, Value=value, Type="String", Overwrite=True)
    yield prefix, params
    for name in params:
        ssm.delete_parameter(Name=name)


@pytest.fixture
def output_bucket(s3, unique_name):
    bucket_name = f"task-output-{unique_name}"
    s3.create_bucket(Bucket=bucket_name)
    yield bucket_name
    # Cleanup: delete all objects then bucket
    resp = s3.list_objects_v2(Bucket=bucket_name)
    for obj in resp.get("Contents", []):
        s3.delete_object(Bucket=bucket_name, Key=obj["Key"])
    s3.delete_bucket(Bucket=bucket_name)


@pytest.fixture
def alert_topic(sns, unique_name):
    topic_name = f"task-alerts-{unique_name}"
    resp = sns.create_topic(Name=topic_name)
    topic_arn = resp["TopicArn"]
    yield topic_arn
    sns.delete_topic(TopicArn=topic_arn)


@pytest.fixture
def alert_queue(sqs, sns, alert_topic, unique_name):
    queue_name = f"task-alert-recv-{unique_name}"
    resp = sqs.create_queue(QueueName=queue_name)
    queue_url = resp["QueueUrl"]
    queue_arn = sqs.get_queue_attributes(QueueUrl=queue_url, AttributeNames=["QueueArn"])[
        "Attributes"
    ]["QueueArn"]
    sns.subscribe(TopicArn=alert_topic, Protocol="sqs", Endpoint=queue_arn)
    yield queue_url, queue_arn
    sqs.delete_queue(QueueUrl=queue_url)


@pytest.fixture
def execution_log(logs, unique_name):
    group_name = f"/scheduler/{unique_name}/executions"
    stream_name = "task-runner"
    logs.create_log_group(logGroupName=group_name)
    logs.create_log_stream(logGroupName=group_name, logStreamName=stream_name)
    yield group_name, stream_name
    logs.delete_log_stream(logGroupName=group_name, logStreamName=stream_name)
    logs.delete_log_group(logGroupName=group_name)


@pytest.fixture
def task_metrics_ns(unique_name):
    return f"Scheduler/{unique_name}"


# ---------------------------------------------------------------------------
# TestTaskDefinitions
# ---------------------------------------------------------------------------


class TestTaskDefinitions:
    def test_create_task_definition(self, dynamodb, tasks_table):
        """Create a task definition and verify it can be read back."""
        dynamodb.put_item(
            TableName=tasks_table,
            Item={
                "task_id": {"S": "task-def"},
                "execution_id": {"S": "DEFINITION"},
                "schedule_name": {"S": "daily-cleanup"},
                "cron_expression": {"S": "cron(0 2 * * ? *)"},
                "handler": {"S": "cleanup_handler"},
                "enabled": {"BOOL": True},
                "next_run": {"S": "2026-03-09T02:00:00Z"},
            },
        )

        resp = dynamodb.get_item(
            TableName=tasks_table,
            Key={"task_id": {"S": "task-def"}, "execution_id": {"S": "DEFINITION"}},
        )
        item = resp["Item"]
        assert item["schedule_name"]["S"] == "daily-cleanup"
        assert item["cron_expression"]["S"] == "cron(0 2 * * ? *)"
        assert item["handler"]["S"] == "cleanup_handler"
        assert item["enabled"]["BOOL"] is True

    def test_update_task_schedule(self, dynamodb, tasks_table):
        """Update a task definition's schedule and verify the change."""
        dynamodb.put_item(
            TableName=tasks_table,
            Item={
                "task_id": {"S": "task-update"},
                "execution_id": {"S": "DEFINITION"},
                "schedule_name": {"S": "hourly-sync"},
                "cron_expression": {"S": "cron(0 * * * ? *)"},
                "handler": {"S": "sync_handler"},
                "enabled": {"BOOL": True},
                "next_run": {"S": "2026-03-08T13:00:00Z"},
            },
        )

        dynamodb.update_item(
            TableName=tasks_table,
            Key={"task_id": {"S": "task-update"}, "execution_id": {"S": "DEFINITION"}},
            UpdateExpression="SET cron_expression = :cron, next_run = :nr",
            ExpressionAttributeValues={
                ":cron": {"S": "cron(0 */2 * * ? *)"},
                ":nr": {"S": "2026-03-08T14:00:00Z"},
            },
        )

        resp = dynamodb.get_item(
            TableName=tasks_table,
            Key={"task_id": {"S": "task-update"}, "execution_id": {"S": "DEFINITION"}},
        )
        item = resp["Item"]
        assert item["cron_expression"]["S"] == "cron(0 */2 * * ? *)"
        assert item["next_run"]["S"] == "2026-03-08T14:00:00Z"

    def test_query_tasks_by_schedule(self, dynamodb, tasks_table):
        """Query GSI by-schedule to find all tasks for a given schedule name."""
        # 3 tasks for "daily-cleanup"
        for i in range(3):
            dynamodb.put_item(
                TableName=tasks_table,
                Item={
                    "task_id": {"S": f"task-dc-{i}"},
                    "execution_id": {"S": "DEFINITION"},
                    "schedule_name": {"S": "daily-cleanup"},
                    "next_run": {"S": f"2026-03-09T0{i}:00:00Z"},
                },
            )
        # 2 tasks for "hourly-sync"
        for i in range(2):
            dynamodb.put_item(
                TableName=tasks_table,
                Item={
                    "task_id": {"S": f"task-hs-{i}"},
                    "execution_id": {"S": "DEFINITION"},
                    "schedule_name": {"S": "hourly-sync"},
                    "next_run": {"S": f"2026-03-08T1{i}:00:00Z"},
                },
            )

        resp = dynamodb.query(
            TableName=tasks_table,
            IndexName="by-schedule",
            KeyConditionExpression="schedule_name = :sn",
            ExpressionAttributeValues={":sn": {"S": "daily-cleanup"}},
        )
        assert resp["Count"] == 3

    def test_read_scheduler_config(self, ssm, task_config):
        """Read scheduler configuration from SSM Parameter Store."""
        prefix, expected_params = task_config
        resp = ssm.get_parameters_by_path(Path=prefix, Recursive=True)
        params = {p["Name"]: p["Value"] for p in resp["Parameters"]}
        assert params[f"{prefix}/max_retries"] == "3"
        assert params[f"{prefix}/timeout_seconds"] == "300"
        assert params[f"{prefix}/concurrency_limit"] == "10"


# ---------------------------------------------------------------------------
# TestTaskExecution
# ---------------------------------------------------------------------------


class TestTaskExecution:
    def test_record_execution_start(self, dynamodb, tasks_table):
        """Record the start of a task execution."""
        dynamodb.put_item(
            TableName=tasks_table,
            Item={
                "task_id": {"S": "task-run-001"},
                "execution_id": {"S": "exec-001"},
                "status": {"S": "running"},
                "scheduled_at": {"S": "2026-03-08T10:00:00Z"},
                "started_at": {"S": "2026-03-08T10:00:02Z"},
            },
        )

        resp = dynamodb.get_item(
            TableName=tasks_table,
            Key={"task_id": {"S": "task-run-001"}, "execution_id": {"S": "exec-001"}},
        )
        item = resp["Item"]
        assert item["status"]["S"] == "running"
        assert item["started_at"]["S"] == "2026-03-08T10:00:02Z"

    def test_record_execution_complete(self, dynamodb, tasks_table):
        """Mark an execution as completed with duration and exit code."""
        dynamodb.put_item(
            TableName=tasks_table,
            Item={
                "task_id": {"S": "task-done-001"},
                "execution_id": {"S": "exec-002"},
                "status": {"S": "running"},
                "scheduled_at": {"S": "2026-03-08T11:00:00Z"},
                "started_at": {"S": "2026-03-08T11:00:01Z"},
            },
        )

        dynamodb.update_item(
            TableName=tasks_table,
            Key={"task_id": {"S": "task-done-001"}, "execution_id": {"S": "exec-002"}},
            UpdateExpression=(
                "SET #st = :status, finished_at = :fin, duration_ms = :dur, exit_code = :ec"
            ),
            ExpressionAttributeNames={"#st": "status"},
            ExpressionAttributeValues={
                ":status": {"S": "completed"},
                ":fin": {"S": "2026-03-08T11:05:23Z"},
                ":dur": {"N": "322000"},
                ":ec": {"N": "0"},
            },
        )

        resp = dynamodb.get_item(
            TableName=tasks_table,
            Key={"task_id": {"S": "task-done-001"}, "execution_id": {"S": "exec-002"}},
        )
        item = resp["Item"]
        assert item["status"]["S"] == "completed"
        assert item["finished_at"]["S"] == "2026-03-08T11:05:23Z"
        assert item["duration_ms"]["N"] == "322000"
        assert item["exit_code"]["N"] == "0"

    def test_record_execution_failure(self, dynamodb, tasks_table):
        """Mark an execution as failed with error details."""
        dynamodb.put_item(
            TableName=tasks_table,
            Item={
                "task_id": {"S": "task-fail-001"},
                "execution_id": {"S": "exec-003"},
                "status": {"S": "running"},
                "scheduled_at": {"S": "2026-03-08T12:00:00Z"},
                "started_at": {"S": "2026-03-08T12:00:01Z"},
            },
        )

        dynamodb.update_item(
            TableName=tasks_table,
            Key={"task_id": {"S": "task-fail-001"}, "execution_id": {"S": "exec-003"}},
            UpdateExpression=("SET #st = :status, error_message = :err, retry_count = :rc"),
            ExpressionAttributeNames={"#st": "status"},
            ExpressionAttributeValues={
                ":status": {"S": "failed"},
                ":err": {"S": "ConnectionError: database unreachable"},
                ":rc": {"N": "2"},
            },
        )

        resp = dynamodb.get_item(
            TableName=tasks_table,
            Key={"task_id": {"S": "task-fail-001"}, "execution_id": {"S": "exec-003"}},
        )
        item = resp["Item"]
        assert item["status"]["S"] == "failed"
        assert item["error_message"]["S"] == "ConnectionError: database unreachable"
        assert item["retry_count"]["N"] == "2"

    def test_query_running_executions(self, dynamodb, tasks_table):
        """Query GSI by-status to find all running executions."""
        # 2 running
        for i in range(2):
            dynamodb.put_item(
                TableName=tasks_table,
                Item={
                    "task_id": {"S": f"task-r-{i}"},
                    "execution_id": {"S": f"exec-r-{i}"},
                    "status": {"S": "running"},
                    "scheduled_at": {"S": f"2026-03-08T0{i}:00:00Z"},
                },
            )
        # 3 completed
        for i in range(3):
            dynamodb.put_item(
                TableName=tasks_table,
                Item={
                    "task_id": {"S": f"task-c-{i}"},
                    "execution_id": {"S": f"exec-c-{i}"},
                    "status": {"S": "completed"},
                    "scheduled_at": {"S": f"2026-03-08T1{i}:00:00Z"},
                },
            )
        # 1 failed
        dynamodb.put_item(
            TableName=tasks_table,
            Item={
                "task_id": {"S": "task-f-0"},
                "execution_id": {"S": "exec-f-0"},
                "status": {"S": "failed"},
                "scheduled_at": {"S": "2026-03-08T20:00:00Z"},
            },
        )

        resp = dynamodb.query(
            TableName=tasks_table,
            IndexName="by-status",
            KeyConditionExpression="#st = :status",
            ExpressionAttributeNames={"#st": "status"},
            ExpressionAttributeValues={":status": {"S": "running"}},
        )
        assert resp["Count"] == 2

    def test_execution_history(self, dynamodb, tasks_table):
        """Query all executions for a single task and verify distinct IDs."""
        task_id = "task-history-001"
        for i in range(5):
            dynamodb.put_item(
                TableName=tasks_table,
                Item={
                    "task_id": {"S": task_id},
                    "execution_id": {"S": f"exec-h-{i:03d}"},
                    "status": {"S": "completed"},
                    "scheduled_at": {"S": f"2026-03-0{i + 1}T00:00:00Z"},
                },
            )

        resp = dynamodb.query(
            TableName=tasks_table,
            KeyConditionExpression="task_id = :tid",
            ExpressionAttributeValues={":tid": {"S": task_id}},
        )
        assert resp["Count"] == 5
        exec_ids = {item["execution_id"]["S"] for item in resp["Items"]}
        assert len(exec_ids) == 5


# ---------------------------------------------------------------------------
# TestTaskOutput
# ---------------------------------------------------------------------------


class TestTaskOutput:
    def test_store_task_output(self, s3, output_bucket):
        """Store task output as JSON in S3 and read it back."""
        key = "output/task-out-001/exec-out-001/result.json"
        body = json.dumps({"rows_processed": 1500, "errors": 0, "status": "success"})
        s3.put_object(Bucket=output_bucket, Key=key, Body=body.encode())

        resp = s3.get_object(Bucket=output_bucket, Key=key)
        data = json.loads(resp["Body"].read().decode())
        assert data["rows_processed"] == 1500
        assert data["errors"] == 0
        assert data["status"] == "success"

    def test_list_execution_outputs(self, s3, output_bucket):
        """List all output files for a single execution."""
        prefix = "output/task-list-001/exec-list-001/"
        for name in ["result.json", "metrics.csv", "debug.log"]:
            s3.put_object(
                Bucket=output_bucket,
                Key=f"{prefix}{name}",
                Body=f"content-of-{name}".encode(),
            )

        resp = s3.list_objects_v2(Bucket=output_bucket, Prefix=prefix)
        assert resp["KeyCount"] == 3

    def test_stream_execution_logs(self, logs, execution_log):
        """Write execution log lines and verify order."""
        group, stream = execution_log
        messages = [
            "Starting task cleanup_handler",
            "Step 1: connecting to database",
            "Step 2: querying stale records",
            "Step 3: deleting 42 records",
            "Step 4: compacting tables",
            "Step 5: updating statistics",
            "Step 6: sending report",
            "Task completed successfully",
        ]
        base_ts = int(time.time() * 1000)
        events = [
            {"timestamp": base_ts + i * 1000, "message": msg} for i, msg in enumerate(messages)
        ]

        logs.put_log_events(logGroupName=group, logStreamName=stream, logEvents=events)

        resp = logs.get_log_events(logGroupName=group, logStreamName=stream, startFromHead=True)
        returned = [e["message"] for e in resp["events"]]
        assert len(returned) == 8
        assert returned[0] == "Starting task cleanup_handler"
        assert returned[-1] == "Task completed successfully"

    def test_filter_error_logs(self, logs, execution_log):
        """Filter log events to find only ERROR lines."""
        group, stream = execution_log
        base_ts = int(time.time() * 1000)
        events = [
            {"timestamp": base_ts, "message": "INFO: Task started"},
            {"timestamp": base_ts + 1000, "message": "INFO: Processing batch 1"},
            {"timestamp": base_ts + 2000, "message": "ERROR: Connection timeout on batch 2"},
            {"timestamp": base_ts + 3000, "message": "INFO: Retrying batch 2"},
            {"timestamp": base_ts + 4000, "message": "ERROR: Retry failed for batch 2"},
            {"timestamp": base_ts + 5000, "message": "INFO: Task aborted"},
        ]
        logs.put_log_events(logGroupName=group, logStreamName=stream, logEvents=events)

        resp = logs.filter_log_events(logGroupName=group, filterPattern="ERROR")
        error_msgs = [e["message"] for e in resp["events"]]
        assert len(error_msgs) == 2
        assert all("ERROR" in m for m in error_msgs)


# ---------------------------------------------------------------------------
# TestTaskAlerts
# ---------------------------------------------------------------------------


class TestTaskAlerts:
    def test_failure_alert(self, sns, sqs, alert_topic, alert_queue):
        """Publish a failure alert via SNS and receive it from the subscribed queue."""
        queue_url, _ = alert_queue
        alert_body = json.dumps(
            {
                "task_id": "task-alert-001",
                "execution_id": "exec-alert-001",
                "error_message": "OutOfMemoryError: heap space exhausted",
            }
        )
        sns.publish(
            TopicArn=alert_topic,
            Subject="Task Execution Failed",
            Message=alert_body,
        )

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
        # SNS wraps the message — unwrap to get original
        if "Message" in body:
            inner = json.loads(body["Message"])
        else:
            inner = body
        assert inner["task_id"] == "task-alert-001"
        assert "OutOfMemoryError" in inner["error_message"]

    def test_execution_metrics(self, cloudwatch, task_metrics_ns):
        """Publish execution metrics and retrieve statistics."""
        cloudwatch.put_metric_data(
            Namespace=task_metrics_ns,
            MetricData=[
                {
                    "MetricName": "TaskExecutions",
                    "Value": 50,
                    "Unit": "Count",
                },
                {
                    "MetricName": "TaskFailures",
                    "Value": 3,
                    "Unit": "Count",
                },
                {
                    "MetricName": "AvgDurationMs",
                    "Value": 1200,
                    "Unit": "Milliseconds",
                },
            ],
        )

        resp = cloudwatch.list_metrics(Namespace=task_metrics_ns)
        metric_names = {m["MetricName"] for m in resp["Metrics"]}
        assert "TaskExecutions" in metric_names
        assert "TaskFailures" in metric_names
        assert "AvgDurationMs" in metric_names

    def test_failure_rate_alarm(self, cloudwatch, task_metrics_ns, unique_name):
        """Create a CloudWatch alarm for task failure rate."""
        alarm_name = f"task-failures-high-{unique_name}"
        cloudwatch.put_metric_alarm(
            AlarmName=alarm_name,
            Namespace=task_metrics_ns,
            MetricName="TaskFailures",
            Statistic="Sum",
            Period=300,
            EvaluationPeriods=1,
            Threshold=10,
            ComparisonOperator="GreaterThanThreshold",
            ActionsEnabled=False,
        )

        cloudwatch.set_alarm_state(
            AlarmName=alarm_name,
            StateValue="ALARM",
            StateReason="Testing alarm state transition",
        )

        resp = cloudwatch.describe_alarms(AlarmNames=[alarm_name])
        alarms = resp["MetricAlarms"]
        assert len(alarms) == 1
        assert alarms[0]["AlarmName"] == alarm_name
        assert alarms[0]["StateValue"] == "ALARM"

        cloudwatch.delete_alarms(AlarmNames=[alarm_name])


# ---------------------------------------------------------------------------
# TestSchedulerEndToEnd
# ---------------------------------------------------------------------------


class TestSchedulerEndToEnd:
    def test_eventbridge_schedule_rule(self, events, sqs, unique_name):
        """Create an EventBridge schedule rule with an SQS target."""
        rule_name = f"periodic-task-{unique_name}"
        queue_name = f"schedule-target-{unique_name}"

        # Create the target queue
        q_resp = sqs.create_queue(QueueName=queue_name)
        queue_url = q_resp["QueueUrl"]
        queue_arn = sqs.get_queue_attributes(QueueUrl=queue_url, AttributeNames=["QueueArn"])[
            "Attributes"
        ]["QueueArn"]

        try:
            events.put_rule(
                Name=rule_name,
                ScheduleExpression="rate(5 minutes)",
                State="ENABLED",
            )
            events.put_targets(
                Rule=rule_name,
                Targets=[{"Id": "schedule-queue", "Arn": queue_arn}],
            )

            targets_resp = events.list_targets_by_rule(Rule=rule_name)
            targets = targets_resp["Targets"]
            assert len(targets) == 1
            assert targets[0]["Id"] == "schedule-queue"
            assert targets[0]["Arn"] == queue_arn

            rule_resp = events.describe_rule(Name=rule_name)
            assert rule_resp["State"] == "ENABLED"
            assert rule_resp["ScheduleExpression"] == "rate(5 minutes)"
        finally:
            events.remove_targets(Rule=rule_name, Ids=["schedule-queue"])
            events.delete_rule(Name=rule_name)
            sqs.delete_queue(QueueUrl=queue_url)

    def test_full_task_execution_cycle(
        self,
        dynamodb,
        tasks_table,
        ssm,
        task_config,
        s3,
        output_bucket,
        logs,
        execution_log,
        sns,
        alert_topic,
        sqs,
        alert_queue,
        cloudwatch,
        task_metrics_ns,
    ):
        """End-to-end: config -> define -> execute -> log -> store -> alert -> metrics."""
        task_id = "task-e2e-001"
        execution_id = "exec-e2e-001"
        prefix, _ = task_config
        queue_url, _ = alert_queue
        log_group, log_stream = execution_log

        # 1. Read config from SSM
        config_resp = ssm.get_parameters_by_path(Path=prefix, Recursive=True)
        config = {p["Name"].split("/")[-1]: p["Value"] for p in config_resp["Parameters"]}
        assert config["max_retries"] == "3"

        # 2. Create task definition in DynamoDB
        dynamodb.put_item(
            TableName=tasks_table,
            Item={
                "task_id": {"S": task_id},
                "execution_id": {"S": "DEFINITION"},
                "schedule_name": {"S": "e2e-schedule"},
                "handler": {"S": "e2e_handler"},
                "enabled": {"BOOL": True},
                "next_run": {"S": "2026-03-09T00:00:00Z"},
            },
        )

        # 3. Record execution start
        dynamodb.put_item(
            TableName=tasks_table,
            Item={
                "task_id": {"S": task_id},
                "execution_id": {"S": execution_id},
                "status": {"S": "running"},
                "scheduled_at": {"S": "2026-03-08T23:00:00Z"},
                "started_at": {"S": "2026-03-08T23:00:01Z"},
            },
        )

        # 4. Write execution logs to CloudWatch
        base_ts = int(time.time() * 1000)
        logs.put_log_events(
            logGroupName=log_group,
            logStreamName=log_stream,
            logEvents=[
                {"timestamp": base_ts, "message": "INFO: e2e_handler starting"},
                {"timestamp": base_ts + 1000, "message": "INFO: processing records"},
                {"timestamp": base_ts + 2000, "message": "INFO: e2e_handler completed"},
            ],
        )

        # 5. Store output to S3
        output_key = f"output/{task_id}/{execution_id}/result.json"
        result_data = {"records_processed": 250, "status": "ok"}
        s3.put_object(
            Bucket=output_bucket,
            Key=output_key,
            Body=json.dumps(result_data).encode(),
        )

        # 6. Update execution to completed
        dynamodb.update_item(
            TableName=tasks_table,
            Key={"task_id": {"S": task_id}, "execution_id": {"S": execution_id}},
            UpdateExpression=(
                "SET #st = :status, finished_at = :fin, duration_ms = :dur, exit_code = :ec"
            ),
            ExpressionAttributeNames={"#st": "status"},
            ExpressionAttributeValues={
                ":status": {"S": "completed"},
                ":fin": {"S": "2026-03-08T23:04:12Z"},
                ":dur": {"N": "251000"},
                ":ec": {"N": "0"},
            },
        )

        # 7. Publish success alert via SNS
        sns.publish(
            TopicArn=alert_topic,
            Subject="Task Completed",
            Message=json.dumps(
                {
                    "task_id": task_id,
                    "execution_id": execution_id,
                    "status": "completed",
                    "duration_ms": 251000,
                }
            ),
        )

        # 8. Receive alert from queue
        messages = []
        deadline = time.time() + 10
        while not messages and time.time() < deadline:
            resp = sqs.receive_message(
                QueueUrl=queue_url, MaxNumberOfMessages=10, WaitTimeSeconds=1
            )
            messages.extend(resp.get("Messages", []))
        assert len(messages) >= 1

        # 9. Publish metrics to CloudWatch
        cloudwatch.put_metric_data(
            Namespace=task_metrics_ns,
            MetricData=[
                {"MetricName": "TaskExecutions", "Value": 1, "Unit": "Count"},
                {"MetricName": "TaskDurationMs", "Value": 251000, "Unit": "Milliseconds"},
            ],
        )

        # 10. Verify final execution state in DynamoDB
        exec_resp = dynamodb.get_item(
            TableName=tasks_table,
            Key={"task_id": {"S": task_id}, "execution_id": {"S": execution_id}},
        )
        assert exec_resp["Item"]["status"]["S"] == "completed"
        assert exec_resp["Item"]["exit_code"]["N"] == "0"

        # 11. Verify S3 has output
        s3_resp = s3.get_object(Bucket=output_bucket, Key=output_key)
        stored = json.loads(s3_resp["Body"].read().decode())
        assert stored["records_processed"] == 250

        # 12. Verify logs have entries
        log_resp = logs.get_log_events(
            logGroupName=log_group, logStreamName=log_stream, startFromHead=True
        )
        assert len(log_resp["events"]) >= 3
        log_messages = [e["message"] for e in log_resp["events"]]
        assert any("e2e_handler starting" in m for m in log_messages)
