"""Semantic integration tests for EventBridge Pipes end-to-end pipeline flows."""

from unittest.mock import MagicMock, patch

import pytest

from robotocore.services.pipes.provider import (
    _create_pipe,
    _delete_pipe,
    _deliver_to_target,
    _describe_pipe,
    _pipe_key,
    _poll_source,
    _polling_threads,
    _run_enrichment,
    _start_pipe,
    _stop_pipe,
    reset_pipes_state,
)

REGION = "us-east-1"
ACCOUNT = "123456789012"


@pytest.fixture(autouse=True)
def clean_state():
    reset_pipes_state()
    yield
    reset_pipes_state()


def _make_pipe_params(**overrides):
    defaults = {
        "Source": "arn:aws:sqs:us-east-1:123456789012:source-queue",
        "Target": "arn:aws:sqs:us-east-1:123456789012:target-queue",
        "RoleArn": "arn:aws:iam::123456789012:role/pipe-role",
        "DesiredState": "STOPPED",
    }
    defaults.update(overrides)
    return defaults


class TestSQSToLambdaEnrichmentToSQSPipeline:
    """Test end-to-end: SQS source -> Lambda enrichment -> SQS target."""

    @patch("robotocore.services.pipes.provider.invoke_lambda_sync")
    @patch("robotocore.services.pipes.provider.get_backend")
    def test_full_pipeline(self, mock_get_backend, mock_invoke):
        # Set up SQS source mock
        mock_msg = MagicMock()
        mock_msg.id = "msg-1"
        mock_msg.receipt_handle = "rh-1"
        mock_msg.body = '{"input": "raw"}'
        mock_msg.system_attributes = {}
        mock_msg.message_attributes = {}
        mock_msg.body_md5 = "md5"

        mock_queue = MagicMock()
        mock_queue.visibility_timeout = 30

        mock_sqs_backend = MagicMock()
        mock_sqs_backend.get_queue.return_value = mock_queue
        mock_sqs_backend.receive_message.return_value = [mock_msg]

        def side_effect(svc):
            if svc == "sqs":
                return {ACCOUNT: {REGION: mock_sqs_backend}}
            return {ACCOUNT: {REGION: MagicMock()}}

        mock_get_backend.side_effect = side_effect

        # Lambda enrichment returns transformed data
        mock_invoke.return_value = ([{"input": "enriched"}], None, "")

        pipe = {
            "Source": "arn:aws:sqs:us-east-1:123456789012:source-queue",
            "SourceParameters": {"SqsQueueParameters": {"BatchSize": 10}},
            "Enrichment": "arn:aws:lambda:us-east-1:123456789012:function:enricher",
            "EnrichmentParameters": {},
            "Target": "arn:aws:sqs:us-east-1:123456789012:target-queue",
            "TargetParameters": {},
        }

        # 1. Poll source
        records = _poll_source(pipe, REGION, ACCOUNT)
        assert len(records) == 1
        assert records[0]["eventSource"] == "aws:sqs"

        # 2. Enrich
        enriched = _run_enrichment(pipe, records, REGION, ACCOUNT)
        assert len(enriched) == 1
        assert enriched[0]["input"] == "enriched"

        # 3. Deliver to target
        _deliver_to_target(
            {"Target": pipe["Target"], "TargetParameters": {}},
            enriched,
            REGION,
            ACCOUNT,
        )
        mock_sqs_backend.send_message.assert_called_once()


class TestDynamoDBStreamToSNSPipeline:
    """Test end-to-end: DynamoDB Streams source -> SNS target."""

    @patch("robotocore.services.pipes.provider.get_backend")
    def test_dynamodb_to_sns(self, mock_get_backend):
        mock_ddb_backend = MagicMock()
        mock_ddb_backend.describe_stream.return_value = {
            "StreamDescription": {
                "Shards": [{"ShardId": "shard-0"}],
            }
        }
        mock_ddb_backend.get_shard_iterator.return_value = "iter-1"
        mock_ddb_backend.get_records.return_value = {
            "Records": [
                {
                    "eventID": "ev-1",
                    "eventName": "INSERT",
                    "dynamodb": {"Keys": {"pk": {"S": "1"}}},
                }
            ]
        }

        mock_sns_backend = MagicMock()

        def side_effect(svc):
            if svc == "dynamodbstreams":
                return {ACCOUNT: {REGION: mock_ddb_backend}}
            elif svc == "sns":
                return {ACCOUNT: {REGION: mock_sns_backend}}
            return {ACCOUNT: {REGION: MagicMock()}}

        mock_get_backend.side_effect = side_effect

        pipe = {
            "Source": "arn:aws:dynamodb:us-east-1:123456789012:table/tbl/stream/2024",
            "SourceParameters": {"DynamoDBStreamParameters": {"BatchSize": 50}},
            "Enrichment": "",
            "EnrichmentParameters": {},
            "Target": "arn:aws:sns:us-east-1:123456789012:target-topic",
            "TargetParameters": {},
        }

        records = _poll_source(pipe, REGION, ACCOUNT)
        assert len(records) == 1
        assert records[0]["eventSource"] == "aws:dynamodb"

        enriched = _run_enrichment(pipe, records, REGION, ACCOUNT)
        assert enriched is records  # No enrichment

        _deliver_to_target(
            {"Target": pipe["Target"], "TargetParameters": {}},
            enriched,
            REGION,
            ACCOUNT,
        )
        mock_sns_backend.publish.assert_called_once()


class TestKinesisToLambdaPipeline:
    """Test end-to-end: Kinesis source -> Lambda target."""

    @patch("robotocore.services.pipes.provider.invoke_lambda_async")
    @patch("robotocore.services.pipes.provider.get_backend")
    def test_kinesis_to_lambda(self, mock_get_backend, mock_invoke):
        mock_record = MagicMock()
        mock_record.partition_key = "pk-1"
        mock_record.sequence_number = "seq-1"
        mock_record.data = "dGVzdA=="
        mock_record.created_at = 1234567890.0

        mock_stream = MagicMock()
        mock_stream.shards = {"shard-0": MagicMock()}

        mock_kinesis_backend = MagicMock()
        mock_kinesis_backend.describe_stream.return_value = mock_stream
        mock_kinesis_backend.get_shard_iterator.return_value = "iter-1"
        mock_kinesis_backend.get_records.return_value = ([mock_record], 0, "iter-2")

        mock_get_backend.return_value = {ACCOUNT: {REGION: mock_kinesis_backend}}

        pipe = {
            "Source": "arn:aws:kinesis:us-east-1:123456789012:stream/src-stream",
            "SourceParameters": {
                "KinesisStreamParameters": {"BatchSize": 100, "StartingPosition": "LATEST"}
            },
            "Enrichment": "",
            "EnrichmentParameters": {},
            "Target": "arn:aws:lambda:us-east-1:123456789012:function:target-fn",
            "TargetParameters": {},
        }

        records = _poll_source(pipe, REGION, ACCOUNT)
        assert len(records) == 1
        assert records[0]["eventSource"] == "aws:kinesis"

        enriched = _run_enrichment(pipe, records, REGION, ACCOUNT)
        assert enriched is records

        _deliver_to_target(
            {"Target": pipe["Target"], "TargetParameters": {}},
            enriched,
            REGION,
            ACCOUNT,
        )
        mock_invoke.assert_called_once()


class TestPipeLifecyclePolling:
    """Test pipe lifecycle: create -> start -> verify polling -> stop -> delete."""

    def test_lifecycle_polling_state(self):
        # Create stopped
        _create_pipe(
            "lifecycle-poll",
            _make_pipe_params(DesiredState="STOPPED"),
            REGION,
            ACCOUNT,
        )
        key = _pipe_key(ACCOUNT, REGION, "lifecycle-poll")
        assert key not in _polling_threads

        # Start
        _start_pipe("lifecycle-poll", REGION, ACCOUNT)
        assert key in _polling_threads

        # Verify the pipe state is RUNNING
        pipe = _describe_pipe("lifecycle-poll", REGION, ACCOUNT)
        assert pipe["CurrentState"] == "RUNNING"

        # Stop
        _stop_pipe("lifecycle-poll", REGION, ACCOUNT)
        assert key not in _polling_threads

        pipe = _describe_pipe("lifecycle-poll", REGION, ACCOUNT)
        assert pipe["CurrentState"] == "STOPPED"

        # Delete
        _delete_pipe("lifecycle-poll", REGION, ACCOUNT)
        from robotocore.services.pipes.provider import PipesError

        with pytest.raises(PipesError):
            _describe_pipe("lifecycle-poll", REGION, ACCOUNT)
