"""Tests for cross-service delivery with correct account_id parsing from ARNs.

These verify that SQS/SNS delivery functions parse both region AND account_id
from target ARNs, rather than defaulting to the caller's account.
"""

import uuid
from unittest.mock import MagicMock, patch

import robotocore.services.dynamodbstreams.hooks as hooks_module
from robotocore.services.dynamodbstreams.hooks import get_store


class TestS3SqsDeliveryParsesAccountFromArn:
    def test_delivers_to_correct_account_store(self):
        """S3 notification to SQS should use the account from the queue ARN."""
        from robotocore.services.s3.notifications import _deliver_to_sqs

        mock_queue = MagicMock()
        mock_store = MagicMock()
        mock_store.get_queue.return_value = mock_queue

        with patch(
            "robotocore.services.s3.notifications.get_sqs_store",
            return_value=mock_store,
        ) as mock_get_store:
            _deliver_to_sqs(
                "arn:aws:sqs:us-west-2:999999999999:my-queue",
                '{"Records": []}',
                "us-east-1",  # caller's region — should be ignored
            )

        # Should parse region and account from the ARN, not use caller's region
        mock_get_store.assert_called_once_with("us-west-2", "999999999999")
        mock_store.get_queue.assert_called_once_with("my-queue")
        mock_queue.put.assert_called_once()


class TestS3SnsDeliveryParsesAccountFromArn:
    def test_delivers_to_correct_account_store(self):
        """S3 notification to SNS should use the account from the topic ARN."""
        from robotocore.services.s3.notifications import _deliver_to_sns

        mock_topic = MagicMock()
        mock_topic.subscriptions = []
        mock_store = MagicMock()
        mock_store.get_topic.return_value = mock_topic

        with patch(
            "robotocore.services.s3.notifications.get_sns_store",
            return_value=mock_store,
        ) as mock_get_store:
            _deliver_to_sns(
                "arn:aws:sns:eu-west-1:888888888888:my-topic",
                '{"Records": []}',
                "us-east-1",  # caller's region
            )

        mock_get_store.assert_called_once_with("eu-west-1", "888888888888")
        mock_store.get_topic.assert_called_once_with("arn:aws:sns:eu-west-1:888888888888:my-topic")


class TestEventBridgeSqsTargetParsesAccountFromArn:
    def test_delivers_to_correct_account_store(self):
        """EventBridge SQS target should parse account from the queue ARN."""
        from robotocore.services.events.provider import _invoke_sqs_target

        mock_queue = MagicMock()
        mock_store = MagicMock()
        mock_store.get_queue.return_value = mock_queue

        with patch(
            "robotocore.services.sqs.provider._get_store",
            return_value=mock_store,
        ) as mock_get_store:
            _invoke_sqs_target(
                "arn:aws:sqs:ap-southeast-1:777777777777:target-queue",
                '{"detail": "test"}',
                "us-east-1",  # caller's region
                "123456789012",  # caller's account
            )

        mock_get_store.assert_called_once_with("ap-southeast-1", "777777777777")


class TestEventBridgeSnsTargetParsesRegionFromArn:
    def test_delivers_to_correct_region_and_account(self):
        """EventBridge SNS target should parse region and account from the topic ARN."""
        from robotocore.services.events.provider import _invoke_sns_target

        mock_topic = MagicMock()
        mock_topic.subscriptions = []
        mock_store = MagicMock()
        mock_store.get_topic.return_value = mock_topic

        with patch(
            "robotocore.services.sns.provider._get_store",
            return_value=mock_store,
        ) as mock_get_store:
            _invoke_sns_target(
                "arn:aws:sns:eu-central-1:666666666666:cross-account-topic",
                '{"detail": "test"}',
                "us-east-1",  # caller's region — should be ignored
                "123456789012",  # caller's account — should be ignored
            )

        mock_get_store.assert_called_once_with("eu-central-1", "666666666666")


class TestSnsSqsDeliveryParsesAccountFromArn:
    def test_delivers_to_correct_account_store(self):
        """SNS -> SQS delivery should parse account from the SQS queue ARN."""
        from robotocore.services.sns.provider import _deliver_to_sqs

        mock_queue = MagicMock()
        mock_store = MagicMock()
        mock_store.get_queue.return_value = mock_queue

        mock_sub = MagicMock()
        mock_sub.endpoint = "arn:aws:sqs:us-west-2:555555555555:subscriber-queue"
        mock_sub.raw_message_delivery = True

        with patch(
            "robotocore.services.sns.provider.get_sqs_store",
            return_value=mock_store,
        ) as mock_get_store:
            _deliver_to_sqs(
                mock_sub,
                "test message",
                None,
                {},
                str(uuid.uuid4()),
                "arn:aws:sns:us-east-1:123456789012:my-topic",
                "us-east-1",
            )

        mock_get_store.assert_called_once_with("us-west-2", "555555555555")
        mock_store.get_queue.assert_called_once_with("subscriber-queue")


class TestDynamoDBStreamsStoreKeyedByAccount:
    def setup_method(self):
        hooks_module._stores.clear()

    def test_different_accounts_get_different_stores(self):
        """Two accounts in the same region should have isolated stream stores."""
        store_a = get_store("us-east-1", "111111111111")
        store_b = get_store("us-east-1", "222222222222")
        assert store_a is not store_b

    def test_same_account_and_region_returns_same_store(self):
        store_a = get_store("us-east-1", "111111111111")
        store_b = get_store("us-east-1", "111111111111")
        assert store_a is store_b
