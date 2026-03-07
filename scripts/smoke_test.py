#!/usr/bin/env python3
"""Smoke test: verify robotocore boots and core operations work.

Usage:
    uv run python scripts/smoke_test.py [endpoint_url]

Runs basic operations against each native provider to verify the server
is functional. Returns exit code 0 if all pass, 1 if any fail.
"""

import json
import sys
import time
import uuid
import zipfile
from io import BytesIO

import boto3
from botocore.config import Config


ENDPOINT_URL = sys.argv[1] if len(sys.argv) > 1 else "http://localhost:4566"
REGION = "us-east-1"


def client(service_name: str):
    config_kwargs = {}
    if service_name == "s3":
        config_kwargs["s3"] = {"addressing_style": "path"}
    return boto3.client(
        service_name,
        endpoint_url=ENDPOINT_URL,
        region_name=REGION,
        aws_access_key_id="testing",
        aws_secret_access_key="testing",
        config=Config(**config_kwargs),
    )


def uid(prefix: str = "smoke") -> str:
    return f"{prefix}-{uuid.uuid4().hex[:8]}"


passed = 0
failed = 0
errors: list[str] = []


def check(name: str, fn):
    global passed, failed
    try:
        fn()
        print(f"  PASS  {name}")
        passed += 1
    except Exception as e:
        print(f"  FAIL  {name}: {e}")
        failed += 1
        errors.append(f"{name}: {e}")


# ---- Health check ----
def test_health():
    import urllib.request

    resp = urllib.request.urlopen(f"{ENDPOINT_URL}/_robotocore/health")
    data = json.loads(resp.read())
    assert data["status"] in ("ok", "healthy", "running"), f"Unexpected status: {data}"


# ---- S3 ----
def test_s3():
    s3 = client("s3")
    bucket = uid("bucket")
    s3.create_bucket(Bucket=bucket)
    s3.put_object(Bucket=bucket, Key="hello.txt", Body=b"world")
    obj = s3.get_object(Bucket=bucket, Key="hello.txt")
    assert obj["Body"].read() == b"world"
    s3.delete_object(Bucket=bucket, Key="hello.txt")
    s3.delete_bucket(Bucket=bucket)


# ---- SQS ----
def test_sqs():
    sqs = client("sqs")
    queue_name = uid("queue")
    url = sqs.create_queue(QueueName=queue_name)["QueueUrl"]
    sqs.send_message(QueueUrl=url, MessageBody="hello")
    msgs = sqs.receive_message(QueueUrl=url, MaxNumberOfMessages=1)
    assert msgs["Messages"][0]["Body"] == "hello"
    sqs.delete_queue(QueueUrl=url)


# ---- SNS ----
def test_sns():
    sns = client("sns")
    topic = sns.create_topic(Name=uid("topic"))
    arn = topic["TopicArn"]
    attrs = sns.get_topic_attributes(TopicArn=arn)
    assert "Attributes" in attrs
    sns.delete_topic(TopicArn=arn)


# ---- DynamoDB ----
def test_dynamodb():
    ddb = client("dynamodb")
    table_name = uid("table")
    ddb.create_table(
        TableName=table_name,
        KeySchema=[{"AttributeName": "id", "KeyType": "HASH"}],
        AttributeDefinitions=[{"AttributeName": "id", "AttributeType": "S"}],
        BillingMode="PAY_PER_REQUEST",
    )
    ddb.put_item(TableName=table_name, Item={"id": {"S": "1"}, "val": {"S": "test"}})
    item = ddb.get_item(TableName=table_name, Key={"id": {"S": "1"}})
    assert item["Item"]["val"]["S"] == "test"
    ddb.delete_table(TableName=table_name)


# ---- Lambda ----
def test_lambda():
    iam = client("iam")
    role_name = uid("lambda-role")
    iam.create_role(
        RoleName=role_name,
        AssumeRolePolicyDocument=json.dumps({
            "Version": "2012-10-17",
            "Statement": [{"Effect": "Allow", "Principal": {"Service": "lambda.amazonaws.com"}, "Action": "sts:AssumeRole"}],
        }),
    )
    role_arn = iam.get_role(RoleName=role_name)["Role"]["Arn"]
    lam = client("lambda")
    fn_name = uid("fn")
    code = b'def handler(event, context):\n    return {"ok": True}\n'
    buf = BytesIO()
    with zipfile.ZipFile(buf, "w") as zf:
        zf.writestr("handler.py", code)
    buf.seek(0)
    lam.create_function(
        FunctionName=fn_name,
        Runtime="python3.12",
        Role=role_arn,
        Handler="handler.handler",
        Code={"ZipFile": buf.read()},
    )
    resp = lam.invoke(FunctionName=fn_name)
    payload = json.loads(resp["Payload"].read())
    assert payload["ok"] is True
    lam.delete_function(FunctionName=fn_name)
    iam.delete_role(RoleName=role_name)


# ---- IAM ----
def test_iam():
    iam = client("iam")
    role_name = uid("role")
    iam.create_role(
        RoleName=role_name,
        AssumeRolePolicyDocument=json.dumps({
            "Version": "2012-10-17",
            "Statement": [{"Effect": "Allow", "Principal": {"Service": "lambda.amazonaws.com"}, "Action": "sts:AssumeRole"}],
        }),
    )
    role = iam.get_role(RoleName=role_name)
    assert role["Role"]["RoleName"] == role_name
    iam.delete_role(RoleName=role_name)


# ---- KMS ----
def test_kms():
    kms = client("kms")
    key = kms.create_key(Description="smoke-test")
    key_id = key["KeyMetadata"]["KeyId"]
    assert key_id
    kms.schedule_key_deletion(KeyId=key_id, PendingWindowInDays=7)


# ---- EventBridge ----
def test_events():
    eb = client("events")
    bus_name = uid("bus")
    eb.create_event_bus(Name=bus_name)
    buses = eb.list_event_buses()
    names = [b["Name"] for b in buses["EventBuses"]]
    assert bus_name in names
    eb.delete_event_bus(Name=bus_name)


# ---- Step Functions ----
def test_stepfunctions():
    sfn = client("stepfunctions")
    name = uid("sm")
    definition = json.dumps({
        "StartAt": "Pass",
        "States": {"Pass": {"Type": "Pass", "Result": "ok", "End": True}},
    })
    sm = sfn.create_state_machine(
        name=name,
        definition=definition,
        roleArn="arn:aws:iam::000000000000:role/test",
    )
    arn = sm["stateMachineArn"]
    execution = sfn.start_execution(stateMachineArn=arn, input="{}")
    exec_arn = execution["executionArn"]
    # Give it a moment to execute
    time.sleep(0.5)
    desc = sfn.describe_execution(executionArn=exec_arn)
    assert desc["status"] in ("SUCCEEDED", "RUNNING")
    sfn.delete_state_machine(stateMachineArn=arn)


# ---- Kinesis ----
def test_kinesis():
    kin = client("kinesis")
    stream_name = uid("stream")
    kin.create_stream(StreamName=stream_name, ShardCount=1)
    desc = kin.describe_stream(StreamName=stream_name)
    assert desc["StreamDescription"]["StreamName"] == stream_name
    kin.delete_stream(StreamName=stream_name)


# ---- CloudWatch ----
def test_cloudwatch():
    cw = client("cloudwatch")
    cw.put_metric_data(
        Namespace="Smoke/Test",
        MetricData=[{"MetricName": "TestMetric", "Value": 42.0, "Unit": "Count"}],
    )
    # Just verify the API call works
    metrics = cw.list_metrics(Namespace="Smoke/Test")
    assert "Metrics" in metrics


# ---- CloudWatch Logs ----
def test_logs():
    logs = client("logs")
    group_name = uid("/smoke/test")
    logs.create_log_group(logGroupName=group_name)
    groups = logs.describe_log_groups(logGroupNamePrefix=group_name)
    assert any(g["logGroupName"] == group_name for g in groups["logGroups"])
    logs.delete_log_group(logGroupName=group_name)


# ---- STS ----
def test_sts():
    sts = client("sts")
    identity = sts.get_caller_identity()
    assert "Account" in identity


# ---- Secrets Manager ----
def test_secretsmanager():
    sm = client("secretsmanager")
    name = uid("secret")
    sm.create_secret(Name=name, SecretString="hunter2")
    val = sm.get_secret_value(SecretId=name)
    assert val["SecretString"] == "hunter2"
    sm.delete_secret(SecretId=name, ForceDeleteWithoutRecovery=True)


# ---- SSM ----
def test_ssm():
    ssm = client("ssm")
    param_name = f"/smoke/{uid('param')}"
    ssm.put_parameter(Name=param_name, Value="test-value", Type="String")
    val = ssm.get_parameter(Name=param_name)
    assert val["Parameter"]["Value"] == "test-value"
    ssm.delete_parameter(Name=param_name)


# ---- Cognito ----
def test_cognito():
    cog = client("cognito-idp")
    pool_name = uid("pool")
    pool = cog.create_user_pool(PoolName=pool_name)["UserPool"]
    pool_id = pool["Id"]
    assert pool["Name"] == pool_name
    cog.delete_user_pool(UserPoolId=pool_id)


# ---- ECS ----
def test_ecs():
    ecs = client("ecs")
    cluster_name = uid("cluster")
    cluster = ecs.create_cluster(clusterName=cluster_name)
    assert cluster["cluster"]["clusterName"] == cluster_name
    ecs.delete_cluster(cluster=cluster_name)


# ---- Scheduler ----
def test_scheduler():
    sched = client("scheduler")
    group_name = uid("group")
    sched.create_schedule_group(Name=group_name)
    groups = sched.list_schedule_groups()
    names = [g["Name"] for g in groups["ScheduleGroups"]]
    assert group_name in names
    sched.delete_schedule_group(Name=group_name)


# ---- Firehose ----
def test_firehose():
    fh = client("firehose")
    # Firehose needs an S3 bucket
    s3 = client("s3")
    bucket = uid("fh-bucket")
    s3.create_bucket(Bucket=bucket)
    stream_name = uid("delivery")
    fh.create_delivery_stream(
        DeliveryStreamName=stream_name,
        S3DestinationConfiguration={
            "BucketARN": f"arn:aws:s3:::{bucket}",
            "RoleARN": "arn:aws:iam::000000000000:role/firehose",
        },
    )
    desc = fh.describe_delivery_stream(DeliveryStreamName=stream_name)
    assert desc["DeliveryStreamDescription"]["DeliveryStreamName"] == stream_name
    fh.delete_delivery_stream(DeliveryStreamName=stream_name)
    s3.delete_bucket(Bucket=bucket)


def main():
    print(f"\nRobotocore Smoke Test — {ENDPOINT_URL}\n")
    print("=" * 50)

    tests = [
        ("Health Check", test_health),
        ("S3", test_s3),
        ("SQS", test_sqs),
        ("SNS", test_sns),
        ("DynamoDB", test_dynamodb),
        ("Lambda", test_lambda),
        ("IAM", test_iam),
        ("KMS", test_kms),
        ("EventBridge", test_events),
        ("Step Functions", test_stepfunctions),
        ("Kinesis", test_kinesis),
        ("CloudWatch", test_cloudwatch),
        ("CloudWatch Logs", test_logs),
        ("STS", test_sts),
        ("Secrets Manager", test_secretsmanager),
        ("SSM", test_ssm),
        ("Cognito", test_cognito),
        ("ECS", test_ecs),
        ("Scheduler", test_scheduler),
        ("Firehose", test_firehose),
    ]

    for name, fn in tests:
        check(name, fn)

    print("=" * 50)
    print(f"\nResults: {passed} passed, {failed} failed out of {passed + failed}")

    if errors:
        print("\nFailures:")
        for e in errors:
            print(f"  - {e}")

    return 0 if failed == 0 else 1


if __name__ == "__main__":
    sys.exit(main())
