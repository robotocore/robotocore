"""Unit tests for EventBridge Pipes pipeline execution (source, enrichment, target)."""

from unittest.mock import MagicMock, patch

import pytest

from robotocore.services.pipes.provider import (
    _deliver_to_eventbridge,
    _deliver_to_kinesis,
    _deliver_to_lambda,
    _deliver_to_sns,
    _deliver_to_sqs,
    _deliver_to_stepfunctions,
    _get_poll_interval,
    _lambda_enrichment,
    _poll_source,
    _run_enrichment,
    reset_pipes_state,
)

REGION = "us-east-1"
ACCOUNT = "123456789012"


@pytest.fixture(autouse=True)
def clean_state():
    reset_pipes_state()
    yield
    reset_pipes_state()


class TestSQSSourcePolling:
    @patch("robotocore.services.pipes.provider.get_backend")
    def test_sqs_source_receives_messages(self, mock_get_backend):
        """Test that SQS source polling receives messages and returns records."""
        mock_msg = MagicMock()
        mock_msg.id = "msg-1"
        mock_msg.receipt_handle = "rh-1"
        mock_msg.body = '{"key": "value"}'
        mock_msg.system_attributes = {}
        mock_msg.message_attributes = {}
        mock_msg.body_md5 = "abc123"

        mock_queue = MagicMock()
        mock_queue.visibility_timeout = 30

        mock_backend = MagicMock()
        mock_backend.get_queue.return_value = mock_queue
        mock_backend.receive_message.return_value = [mock_msg]
        mock_get_backend.return_value = {ACCOUNT: {REGION: mock_backend}}

        pipe = {
            "Source": "arn:aws:sqs:us-east-1:123456789012:test-queue",
            "SourceParameters": {"SqsQueueParameters": {"BatchSize": 5}},
        }

        records = _poll_source(pipe, REGION, ACCOUNT)

        assert len(records) == 1
        assert records[0]["messageId"] == "msg-1"
        assert records[0]["body"] == '{"key": "value"}'
        assert records[0]["eventSource"] == "aws:sqs"
        assert records[0]["eventSourceARN"] == pipe["Source"]
        mock_backend.delete_message.assert_called_once_with("test-queue", "rh-1")


class TestKinesisSourcePolling:
    @patch("robotocore.services.pipes.provider.get_backend")
    def test_kinesis_source_receives_records(self, mock_get_backend):
        """Test that Kinesis source polling reads records from shards."""
        mock_record = MagicMock()
        mock_record.partition_key = "pk-1"
        mock_record.sequence_number = "seq-1"
        mock_record.data = "dGVzdA=="
        mock_record.created_at = 1234567890.0

        mock_stream = MagicMock()
        mock_stream.shards = {"shard-0": MagicMock()}

        mock_backend = MagicMock()
        mock_backend.describe_stream.return_value = mock_stream
        mock_backend.get_shard_iterator.return_value = "iter-1"
        mock_backend.get_records.return_value = ([mock_record], 0, "iter-2")
        mock_get_backend.return_value = {ACCOUNT: {REGION: mock_backend}}

        pipe = {
            "Source": "arn:aws:kinesis:us-east-1:123456789012:stream/test-stream",
            "SourceParameters": {
                "KinesisStreamParameters": {"BatchSize": 100, "StartingPosition": "LATEST"}
            },
        }

        records = _poll_source(pipe, REGION, ACCOUNT)

        assert len(records) == 1
        assert records[0]["partitionKey"] == "pk-1"
        assert records[0]["eventSource"] == "aws:kinesis"


class TestDynamoDBStreamSourcePolling:
    @patch("robotocore.services.pipes.provider.get_backend")
    def test_dynamodb_stream_receives_records(self, mock_get_backend):
        """Test that DynamoDB Streams source polling reads stream records."""
        mock_backend = MagicMock()
        mock_backend.describe_stream.return_value = {
            "StreamDescription": {
                "Shards": [{"ShardId": "shard-0"}],
            }
        }
        mock_backend.get_shard_iterator.return_value = "iter-1"
        mock_backend.get_records.return_value = {
            "Records": [
                {
                    "eventID": "ev-1",
                    "eventName": "INSERT",
                    "dynamodb": {"Keys": {"id": {"S": "1"}}},
                }
            ]
        }
        mock_get_backend.return_value = {ACCOUNT: {REGION: mock_backend}}

        pipe = {
            "Source": "arn:aws:dynamodb:us-east-1:123456789012:table/test/stream/2024-01-01",
            "SourceParameters": {"DynamoDBStreamParameters": {"BatchSize": 50}},
        }

        records = _poll_source(pipe, REGION, ACCOUNT)

        assert len(records) == 1
        assert records[0]["eventSource"] == "aws:dynamodb"
        assert records[0]["eventName"] == "INSERT"


class TestLambdaEnrichment:
    @patch("robotocore.services.pipes.provider.invoke_lambda_sync")
    def test_lambda_enrichment_transforms_records(self, mock_invoke):
        """Test that Lambda enrichment passes records and uses response."""
        enriched = [{"key": "enriched-value"}]
        mock_invoke.return_value = (enriched, None, "")

        result = _lambda_enrichment(
            "arn:aws:lambda:us-east-1:123456789012:function:enrich",
            [{"key": "original"}],
            REGION,
            ACCOUNT,
        )

        assert result == enriched
        mock_invoke.assert_called_once()

    @patch("robotocore.services.pipes.provider.invoke_lambda_sync")
    def test_lambda_enrichment_error_returns_original(self, mock_invoke):
        """Test that enrichment errors return original records."""
        mock_invoke.return_value = (None, "LambdaError", "error logs")

        original = [{"key": "original"}]
        result = _lambda_enrichment(
            "arn:aws:lambda:us-east-1:123456789012:function:bad",
            original,
            REGION,
            ACCOUNT,
        )

        assert result == original


class TestNoEnrichment:
    def test_no_enrichment_passes_records_through(self):
        """Test that records pass through when no enrichment is configured."""
        records = [{"key": "value"}]
        pipe = {"Enrichment": "", "EnrichmentParameters": {}}
        result = _run_enrichment(pipe, records, REGION, ACCOUNT)
        assert result is records

    def test_empty_enrichment_passes_through(self):
        pipe = {"EnrichmentParameters": {}}
        records = [{"a": 1}]
        result = _run_enrichment(pipe, records, REGION, ACCOUNT)
        assert result is records


class TestSQSTarget:
    @patch("robotocore.services.pipes.provider.get_backend")
    def test_sqs_target_sends_messages(self, mock_get_backend):
        """Test that SQS target sends each record as a message."""
        mock_backend = MagicMock()
        mock_get_backend.return_value = {ACCOUNT: {REGION: mock_backend}}

        records = [{"body": "msg1"}, {"body": "msg2"}]
        _deliver_to_sqs(
            "arn:aws:sqs:us-east-1:123456789012:target-queue",
            records,
            {},
            REGION,
            ACCOUNT,
        )

        assert mock_backend.send_message.call_count == 2


class TestSNSTarget:
    @patch("robotocore.services.pipes.provider.get_backend")
    def test_sns_target_publishes_messages(self, mock_get_backend):
        """Test that SNS target publishes each record."""
        mock_backend = MagicMock()
        mock_get_backend.return_value = {ACCOUNT: {REGION: mock_backend}}

        records = [{"data": "event1"}]
        _deliver_to_sns(
            "arn:aws:sns:us-east-1:123456789012:my-topic",
            records,
            {},
            REGION,
            ACCOUNT,
        )

        mock_backend.publish.assert_called_once()
        call_kwargs = mock_backend.publish.call_args
        assert "my-topic" in str(call_kwargs)


class TestLambdaTarget:
    @patch("robotocore.services.pipes.provider.invoke_lambda_async")
    def test_lambda_target_invokes_function(self, mock_invoke):
        """Test that Lambda target invokes the function with records."""
        records = [{"data": "event"}]
        _deliver_to_lambda(
            "arn:aws:lambda:us-east-1:123456789012:function:target-fn",
            records,
            {},
            REGION,
            ACCOUNT,
        )

        mock_invoke.assert_called_once()
        call_kwargs = mock_invoke.call_args
        assert call_kwargs[1]["payload"] == {"Records": records}


class TestEventBridgeTarget:
    @patch("robotocore.services.pipes.provider.get_backend")
    def test_eventbridge_target_puts_events(self, mock_get_backend):
        """Test that EventBridge target puts events on the bus."""
        mock_backend = MagicMock()
        mock_get_backend.return_value = {ACCOUNT: {REGION: mock_backend}}

        records = [{"detail": "data"}]
        _deliver_to_eventbridge(
            "arn:aws:events:us-east-1:123456789012:event-bus/default",
            records,
            {},
            REGION,
            ACCOUNT,
        )

        mock_backend.put_events.assert_called_once()


class TestStepFunctionsTarget:
    @patch("robotocore.services.pipes.provider.get_backend")
    def test_sfn_target_starts_execution(self, mock_get_backend):
        """Test that Step Functions target starts an execution."""
        mock_backend = MagicMock()
        mock_get_backend.return_value = {ACCOUNT: {REGION: mock_backend}}

        records = [{"input": "data"}]
        _deliver_to_stepfunctions(
            "arn:aws:states:us-east-1:123456789012:stateMachine:my-sm",
            records,
            {},
            REGION,
            ACCOUNT,
        )

        mock_backend.start_execution.assert_called_once()


class TestKinesisTarget:
    @patch("robotocore.services.pipes.provider.get_backend")
    def test_kinesis_target_puts_records(self, mock_get_backend):
        """Test that Kinesis target puts records to the stream."""
        mock_backend = MagicMock()
        mock_get_backend.return_value = {ACCOUNT: {REGION: mock_backend}}

        records = [{"data": "record1"}, {"data": "record2"}]
        _deliver_to_kinesis(
            "arn:aws:kinesis:us-east-1:123456789012:stream/target-stream",
            records,
            {},
            REGION,
            ACCOUNT,
        )

        assert mock_backend.put_record.call_count == 2


class TestErrorHandling:
    @patch("robotocore.services.pipes.provider.get_backend")
    def test_target_failure_doesnt_crash(self, mock_get_backend):
        """Test that target delivery errors are caught and logged, not raised."""
        mock_backend = MagicMock()
        mock_backend.send_message.side_effect = Exception("SQS error")
        mock_get_backend.return_value = {ACCOUNT: {REGION: mock_backend}}

        # Should not raise
        from robotocore.services.pipes.provider import _deliver_to_target

        pipe = {
            "Target": "arn:aws:sqs:us-east-1:123456789012:broken-queue",
            "TargetParameters": {},
        }
        _deliver_to_target(pipe, [{"msg": "test"}], REGION, ACCOUNT)


class TestStoppedPipeNoPoll:
    def test_stopped_pipe_does_not_poll(self):
        """A STOPPED pipe should not have an active polling thread."""
        from robotocore.services.pipes.provider import (
            _create_pipe,
            _pipe_key,
            _polling_threads,
        )

        _create_pipe("stopped-nopoll", _make_pipe_params(DesiredState="STOPPED"), REGION, ACCOUNT)
        key = _pipe_key(ACCOUNT, REGION, "stopped-nopoll")
        assert key not in _polling_threads


class TestPollInterval:
    def test_default_interval(self):
        pipe = {"SourceParameters": {}}
        assert _get_poll_interval(pipe) == 1.0

    def test_sqs_window(self):
        pipe = {"SourceParameters": {"SqsQueueParameters": {"MaximumBatchingWindowInSeconds": 5}}}
        assert _get_poll_interval(pipe) == 5.0

    def test_kinesis_window(self):
        pipe = {
            "SourceParameters": {"KinesisStreamParameters": {"MaximumBatchingWindowInSeconds": 3}}
        }
        assert _get_poll_interval(pipe) == 3.0

    def test_minimum_interval(self):
        pipe = {"SourceParameters": {"SqsQueueParameters": {"MaximumBatchingWindowInSeconds": 0}}}
        assert _get_poll_interval(pipe) == 0.1


def _make_pipe_params(**overrides):
    defaults = {
        "Source": "arn:aws:sqs:us-east-1:123456789012:my-source-queue",
        "Target": "arn:aws:sqs:us-east-1:123456789012:my-target-queue",
        "RoleArn": "arn:aws:iam::123456789012:role/pipe-role",
        "DesiredState": "STOPPED",
    }
    defaults.update(overrides)
    return defaults
