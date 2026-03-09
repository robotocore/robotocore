"""Reusable assertion helpers for validating AWS resources exist."""

from __future__ import annotations


def assert_s3_bucket_exists(client, bucket_name: str) -> dict:
    """Assert an S3 bucket exists and return its metadata."""
    resp = client.head_bucket(Bucket=bucket_name)
    assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200, f"S3 bucket {bucket_name!r} not found"
    return resp


def assert_dynamodb_table_exists(client, table_name: str, expected_status: str = "ACTIVE") -> dict:
    """Assert a DynamoDB table exists with the expected status."""
    resp = client.describe_table(TableName=table_name)
    table = resp["Table"]
    assert table["TableStatus"] == expected_status, (
        f"DynamoDB table {table_name!r} status is {table['TableStatus']!r}, "
        f"expected {expected_status!r}"
    )
    return table


def assert_lambda_function_exists(client, function_name: str) -> dict:
    """Assert a Lambda function exists and return its configuration."""
    resp = client.get_function(FunctionName=function_name)
    config = resp["Configuration"]
    assert config["FunctionName"] == function_name, (
        f"Lambda function name mismatch: {config['FunctionName']!r} != {function_name!r}"
    )
    return config


def assert_sqs_queue_exists(client, queue_name: str) -> str:
    """Assert an SQS queue exists and return its URL."""
    resp = client.get_queue_url(QueueName=queue_name)
    queue_url = resp["QueueUrl"]
    assert queue_url, f"SQS queue {queue_name!r} not found"
    return queue_url


def assert_sns_topic_exists(client, topic_arn: str) -> dict:
    """Assert an SNS topic exists and return its attributes."""
    resp = client.get_topic_attributes(TopicArn=topic_arn)
    attrs = resp["Attributes"]
    assert attrs.get("TopicArn") == topic_arn, (
        f"SNS topic ARN mismatch: {attrs.get('TopicArn')!r} != {topic_arn!r}"
    )
    return attrs


def assert_iam_role_exists(client, role_name: str) -> dict:
    """Assert an IAM role exists and return its details."""
    resp = client.get_role(RoleName=role_name)
    role = resp["Role"]
    assert role["RoleName"] == role_name, (
        f"IAM role name mismatch: {role['RoleName']!r} != {role_name!r}"
    )
    return role


def assert_api_gateway_exists(client, api_id: str) -> dict:
    """Assert an API Gateway REST API exists."""
    resp = client.get_rest_api(restApiId=api_id)
    assert resp["id"] == api_id, f"API Gateway id mismatch: {resp['id']!r} != {api_id!r}"
    return resp


def assert_vpc_exists(client, vpc_id: str) -> dict:
    """Assert a VPC exists and return its details."""
    resp = client.describe_vpcs(VpcIds=[vpc_id])
    vpcs = resp["Vpcs"]
    assert len(vpcs) == 1, f"VPC {vpc_id!r} not found"
    return vpcs[0]


def assert_kinesis_stream_exists(client, stream_name: str, expected_status: str = "ACTIVE") -> dict:
    """Assert a Kinesis stream exists with the expected status."""
    resp = client.describe_stream(StreamName=stream_name)
    desc = resp["StreamDescription"]
    assert desc["StreamStatus"] == expected_status, (
        f"Kinesis stream {stream_name!r} status is {desc['StreamStatus']!r}, "
        f"expected {expected_status!r}"
    )
    return desc


def assert_cognito_user_pool_exists(client, pool_id: str) -> dict:
    """Assert a Cognito user pool exists."""
    resp = client.describe_user_pool(UserPoolId=pool_id)
    pool = resp["UserPool"]
    assert pool["Id"] == pool_id, f"Cognito user pool id mismatch: {pool['Id']!r} != {pool_id!r}"
    return pool


def assert_cloudwatch_alarm_exists(client, alarm_name: str) -> dict:
    """Assert a CloudWatch alarm exists."""
    resp = client.describe_alarms(AlarmNames=[alarm_name])
    alarms = resp["MetricAlarms"]
    assert len(alarms) == 1, f"CloudWatch alarm {alarm_name!r} not found"
    return alarms[0]


def assert_log_group_exists(client, log_group_name: str) -> dict:
    """Assert a CloudWatch log group exists."""
    resp = client.describe_log_groups(logGroupNamePrefix=log_group_name)
    groups = [g for g in resp["logGroups"] if g["logGroupName"] == log_group_name]
    assert len(groups) == 1, f"Log group {log_group_name!r} not found"
    return groups[0]


def assert_route53_hosted_zone_exists(client, zone_id: str) -> dict:
    """Assert a Route53 hosted zone exists."""
    resp = client.get_hosted_zone(Id=zone_id)
    zone = resp["HostedZone"]
    assert zone["Id"].endswith(zone_id) or zone["Id"] == zone_id, (
        f"Route53 zone id mismatch: {zone['Id']!r} does not match {zone_id!r}"
    )
    return zone
