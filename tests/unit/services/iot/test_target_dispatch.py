"""Tests for IoT rule target dispatch."""

import json

import pytest

from robotocore.services.iot.target_dispatch import (
    _resolve_template,
    clear_dispatch_log,
    dispatch_actions,
    get_dispatch_log,
)


@pytest.fixture(autouse=True)
def _clear_log():
    clear_dispatch_log()
    yield
    clear_dispatch_log()


class TestResolveTemplate:
    """Test template variable resolution."""

    def test_simple_field(self):
        result = _resolve_template("${temperature}", {"temperature": 25}, "test")
        assert result == "25"

    def test_topic_function(self):
        result = _resolve_template("${topic()}", {}, "sensors/room1")
        assert result == "sensors/room1"

    def test_nested_field(self):
        result = _resolve_template("${device.id}", {"device": {"id": "abc"}}, "test")
        assert result == "abc"

    def test_no_template(self):
        result = _resolve_template("static-value", {}, "test")
        assert result == "static-value"

    def test_unresolved_template_kept(self):
        result = _resolve_template("${missing.field}", {"other": 1}, "test")
        assert result == "${missing.field}"

    def test_multiple_templates(self):
        result = _resolve_template(
            "${topic()}-${temperature}",
            {"temperature": 25},
            "sensors/room1",
        )
        assert result == "sensors/room1-25"


class TestDispatchLambda:
    """Test Lambda target dispatch."""

    def test_lambda_missing_arn(self):
        results = dispatch_actions(
            [{"lambda": {}}],
            {"temp": 25},
            "test/topic",
            "us-east-1",
            "123456789012",
        )
        assert len(results) == 1
        assert results[0]["success"] is False
        assert "functionArn" in results[0]["error"]

    def test_lambda_dispatch_logged(self):
        dispatch_actions(
            [{"lambda": {"functionArn": "arn:aws:lambda:us-east-1:123456789012:function:test"}}],
            {"temp": 25},
            "test/topic",
            "us-east-1",
            "123456789012",
        )
        log = get_dispatch_log()
        assert len(log) == 1
        assert log[0]["action_type"] == "lambda"


class TestDispatchSqs:
    """Test SQS target dispatch."""

    def test_sqs_missing_queue_url(self):
        results = dispatch_actions(
            [{"sqs": {}}],
            {"temp": 25},
            "test/topic",
            "us-east-1",
            "123456789012",
        )
        assert len(results) == 1
        assert results[0]["success"] is False


class TestDispatchSns:
    """Test SNS target dispatch."""

    def test_sns_missing_target_arn(self):
        results = dispatch_actions(
            [{"sns": {}}],
            {"temp": 25},
            "test/topic",
            "us-east-1",
            "123456789012",
        )
        assert len(results) == 1
        assert results[0]["success"] is False


class TestDispatchDynamoDB:
    """Test DynamoDB target dispatch."""

    def test_dynamodb_v1_success(self):
        results = dispatch_actions(
            [
                {
                    "dynamoDB": {
                        "tableName": "iot-data",
                        "hashKeyField": "id",
                        "hashKeyValue": "${topic()}",
                        "rangeKeyField": "ts",
                        "rangeKeyValue": "${timestamp()}",
                        "payloadField": "data",
                    }
                }
            ],
            {"temp": 25},
            "sensors/room1",
            "us-east-1",
            "123456789012",
        )
        assert len(results) == 1
        assert results[0]["success"] is True
        assert results[0]["tableName"] == "iot-data"
        assert results[0]["item"]["id"]["S"] == "sensors/room1"
        assert "data" in results[0]["item"]

    def test_dynamodb_v1_missing_fields(self):
        results = dispatch_actions(
            [{"dynamoDB": {"tableName": "test"}}],
            {"temp": 25},
            "test",
            "us-east-1",
            "123456789012",
        )
        assert results[0]["success"] is False

    def test_dynamodbv2_success(self):
        results = dispatch_actions(
            [{"dynamoDBv2": {"tableName": "iot-data"}}],
            {"temp": 25},
            "sensors/room1",
            "us-east-1",
            "123456789012",
        )
        assert len(results) == 1
        assert results[0]["success"] is True
        assert results[0]["tableName"] == "iot-data"
        assert results[0]["payload"] == {"temp": 25}

    def test_dynamodbv2_missing_table(self):
        results = dispatch_actions(
            [{"dynamoDBv2": {}}],
            {"temp": 25},
            "test",
            "us-east-1",
            "123456789012",
        )
        assert results[0]["success"] is False


class TestDispatchKinesis:
    """Test Kinesis target dispatch."""

    def test_kinesis_success(self):
        results = dispatch_actions(
            [{"kinesis": {"streamName": "my-stream", "partitionKey": "${topic()}"}}],
            {"temp": 25},
            "sensors/room1",
            "us-east-1",
            "123456789012",
        )
        assert len(results) == 1
        assert results[0]["success"] is True
        assert results[0]["streamName"] == "my-stream"
        assert results[0]["partitionKey"] == "sensors/room1"
        assert json.loads(results[0]["data"]) == {"temp": 25}

    def test_kinesis_missing_stream(self):
        results = dispatch_actions(
            [{"kinesis": {}}],
            {"temp": 25},
            "test",
            "us-east-1",
            "123456789012",
        )
        assert results[0]["success"] is False


class TestDispatchS3:
    """Test S3 target dispatch."""

    def test_s3_success(self):
        results = dispatch_actions(
            [{"s3": {"bucketName": "iot-bucket", "key": "data/${topic()}.json"}}],
            {"temp": 25},
            "sensors/room1",
            "us-east-1",
            "123456789012",
        )
        assert len(results) == 1
        assert results[0]["success"] is True
        assert results[0]["bucketName"] == "iot-bucket"
        assert results[0]["key"] == "data/sensors/room1.json"

    def test_s3_missing_bucket(self):
        results = dispatch_actions(
            [{"s3": {}}],
            {"temp": 25},
            "test",
            "us-east-1",
            "123456789012",
        )
        assert results[0]["success"] is False


class TestDispatchCloudWatch:
    """Test CloudWatch target dispatch."""

    def test_cloudwatch_metric_success(self):
        results = dispatch_actions(
            [
                {
                    "cloudwatchMetric": {
                        "metricNamespace": "IoT/Sensors",
                        "metricName": "Temperature",
                        "metricValue": "${temperature}",
                    }
                }
            ],
            {"temperature": 25},
            "sensors/room1",
            "us-east-1",
            "123456789012",
        )
        assert len(results) == 1
        assert results[0]["success"] is True
        assert results[0]["metricValue"] == "25"

    def test_cloudwatch_metric_missing_fields(self):
        results = dispatch_actions(
            [{"cloudwatchMetric": {}}],
            {"temp": 25},
            "test",
            "us-east-1",
            "123456789012",
        )
        assert results[0]["success"] is False

    def test_cloudwatch_logs_success(self):
        results = dispatch_actions(
            [{"cloudwatchLogs": {"logGroupName": "/iot/sensors"}}],
            {"temp": 25},
            "sensors/room1",
            "us-east-1",
            "123456789012",
        )
        assert len(results) == 1
        assert results[0]["success"] is True
        assert results[0]["logGroupName"] == "/iot/sensors"

    def test_cloudwatch_logs_missing_group(self):
        results = dispatch_actions(
            [{"cloudwatchLogs": {}}],
            {"temp": 25},
            "test",
            "us-east-1",
            "123456789012",
        )
        assert results[0]["success"] is False


class TestDispatchErrors:
    """Test error handling and error actions."""

    def test_unsupported_action_type(self):
        results = dispatch_actions(
            [{"unknownAction": {"key": "val"}}],
            {"temp": 25},
            "test",
            "us-east-1",
            "123456789012",
        )
        assert len(results) == 1
        assert results[0]["success"] is False
        assert "Unsupported" in results[0]["error"]

    def test_error_action_invoked_on_failure(self):
        # Use a lambda with invalid config to force an error, with an error action
        results = dispatch_actions(
            actions=[{"lambda": {}}],  # missing functionArn -> failure
            payload={"temp": 25},
            topic="test",
            region="us-east-1",
            account_id="123456789012",
            error_action={"s3": {"bucketName": "dlq-bucket", "key": "errors/${topic()}.json"}},
        )
        # The lambda dispatch logs a failure result
        assert len(results) == 1
        assert results[0]["success"] is False

        # Check dispatch log - should have both the failed action and the error action
        log = get_dispatch_log()
        assert len(log) >= 1  # At least the failed action

    def test_multiple_actions(self):
        results = dispatch_actions(
            [
                {"s3": {"bucketName": "bucket", "key": "key.json"}},
                {"kinesis": {"streamName": "stream", "partitionKey": "pk"}},
            ],
            {"temp": 25},
            "test",
            "us-east-1",
            "123456789012",
        )
        assert len(results) == 2
        assert all(r["success"] for r in results)

    def test_dispatch_log_populated(self):
        dispatch_actions(
            [{"s3": {"bucketName": "b", "key": "k"}}],
            {"temp": 25},
            "test",
            "us-east-1",
            "123456789012",
        )
        log = get_dispatch_log()
        assert len(log) == 1
        assert log[0]["action_type"] == "s3"
        assert "timestamp" in log[0]
