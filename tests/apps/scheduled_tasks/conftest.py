"""
Fixtures for scheduled-tasks integration tests.
"""

from __future__ import annotations

import pytest

from .app import TaskScheduler
from .models import TaskDefinition, TaskGroup


@pytest.fixture
def tasks_table(dynamodb, unique_name):
    """DynamoDB table for task definitions, groups, and dependencies."""
    table_name = f"sched-tasks-{unique_name}"
    dynamodb.create_table(
        TableName=table_name,
        KeySchema=[{"AttributeName": "task_id", "KeyType": "HASH"}],
        AttributeDefinitions=[
            {"AttributeName": "task_id", "AttributeType": "S"},
            {"AttributeName": "group_name", "AttributeType": "S"},
        ],
        GlobalSecondaryIndexes=[
            {
                "IndexName": "by-group",
                "KeySchema": [
                    {"AttributeName": "group_name", "KeyType": "HASH"},
                ],
                "Projection": {"ProjectionType": "ALL"},
            },
        ],
        BillingMode="PAY_PER_REQUEST",
    )
    yield table_name
    dynamodb.delete_table(TableName=table_name)


@pytest.fixture
def executions_table(dynamodb, unique_name):
    """DynamoDB table for execution history."""
    table_name = f"sched-exec-{unique_name}"
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
            {"AttributeName": "started_at", "AttributeType": "S"},
        ],
        GlobalSecondaryIndexes=[
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
def config_prefix(unique_name):
    return f"/scheduler/{unique_name}"


@pytest.fixture
def output_bucket(s3, unique_name):
    bucket_name = f"sched-output-{unique_name}"
    s3.create_bucket(Bucket=bucket_name)
    yield bucket_name
    resp = s3.list_objects_v2(Bucket=bucket_name)
    for obj in resp.get("Contents", []):
        s3.delete_object(Bucket=bucket_name, Key=obj["Key"])
    s3.delete_bucket(Bucket=bucket_name)


@pytest.fixture
def alert_topic(sns, unique_name):
    topic_name = f"sched-alerts-{unique_name}"
    resp = sns.create_topic(Name=topic_name)
    arn = resp["TopicArn"]
    yield arn
    sns.delete_topic(TopicArn=arn)


@pytest.fixture
def alert_queue(sqs, sns, alert_topic, unique_name):
    """SQS queue subscribed to the alert SNS topic -- used to verify alerts."""
    queue_name = f"sched-alert-recv-{unique_name}"
    resp = sqs.create_queue(QueueName=queue_name)
    queue_url = resp["QueueUrl"]
    queue_arn = sqs.get_queue_attributes(QueueUrl=queue_url, AttributeNames=["QueueArn"])[
        "Attributes"
    ]["QueueArn"]
    sns.subscribe(TopicArn=alert_topic, Protocol="sqs", Endpoint=queue_arn)
    yield queue_url, queue_arn
    sqs.delete_queue(QueueUrl=queue_url)


@pytest.fixture
def log_group(logs, unique_name):
    group_name = f"/scheduler/{unique_name}/executions"
    logs.create_log_group(logGroupName=group_name)
    yield group_name
    # cleanup: delete all streams then the group
    streams = logs.describe_log_streams(logGroupName=group_name).get("logStreams", [])
    for s in streams:
        logs.delete_log_stream(logGroupName=group_name, logStreamName=s["logStreamName"])
    logs.delete_log_group(logGroupName=group_name)


@pytest.fixture
def metrics_namespace(unique_name):
    return f"Scheduler/{unique_name}"


@pytest.fixture
def scheduler(
    dynamodb,
    events,
    ssm,
    s3,
    sqs,
    sns,
    cloudwatch,
    logs,
    tasks_table,
    executions_table,
    config_prefix,
    output_bucket,
    alert_topic,
    log_group,
    metrics_namespace,
):
    """Fully-wired TaskScheduler instance."""
    return TaskScheduler(
        dynamodb=dynamodb,
        events=events,
        ssm=ssm,
        s3=s3,
        sns=sns,
        sqs=sqs,
        cloudwatch=cloudwatch,
        logs=logs,
        tasks_table=tasks_table,
        executions_table=executions_table,
        config_prefix=config_prefix,
        output_bucket=output_bucket,
        alert_topic_arn=alert_topic,
        log_group=log_group,
        metrics_namespace=metrics_namespace,
    )


@pytest.fixture
def sample_task(scheduler):
    """A pre-created task with a cron schedule."""
    task = TaskDefinition(
        name="nightly-report",
        group="reports",
        schedule_expression="cron(0 2 * * ? *)",
        target_arn="arn:aws:lambda:us-east-1:123456789012:function:nightly-report",
        input_payload={"report_type": "daily", "format": "csv"},
        max_retries=3,
        timeout_seconds=600,
        max_concurrent=1,
    )
    return scheduler.create_task(task)


@pytest.fixture
def task_group(scheduler):
    """A group of 3 related tasks."""
    group_name = "nightly-pipeline"
    tasks = []
    for i, (name, expr) in enumerate(
        [
            ("extract", "cron(0 1 * * ? *)"),
            ("transform", "cron(0 2 * * ? *)"),
            ("load", "cron(0 3 * * ? *)"),
        ]
    ):
        t = TaskDefinition(
            name=name,
            group=group_name,
            schedule_expression=expr,
            target_arn=f"arn:aws:lambda:us-east-1:123456789012:function:{name}",
            max_retries=2,
        )
        created = scheduler.create_task(t)
        tasks.append(created)

    scheduler.create_group(
        TaskGroup(
            group_name=group_name,
            tasks=[t.task_id for t in tasks],
            description="Nightly ETL pipeline",
        )
    )
    return group_name, tasks
