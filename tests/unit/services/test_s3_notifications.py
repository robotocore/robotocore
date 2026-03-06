"""Tests for robotocore.services.s3.notifications."""

import json
from unittest.mock import MagicMock, patch

from robotocore.services.s3.notifications import (
    NotificationConfig,
    _bucket_notifications,
    _build_event_record,
    _deliver_to_sns,
    _deliver_to_sqs,
    _event_matches,
    fire_event,
    get_notification_config,
    set_notification_config,
)


class TestNotificationConfig:
    def test_defaults(self):
        c = NotificationConfig()
        assert c.queue_configs == []
        assert c.topic_configs == []
        assert c.lambda_configs == []


class TestSetGetNotificationConfig:
    def setup_method(self):
        _bucket_notifications.clear()

    def test_set_and_get(self):
        cfg = NotificationConfig(queue_configs=[{"QueueArn": "arn:sqs:q"}])
        set_notification_config("my-bucket", cfg)
        result = get_notification_config("my-bucket")
        assert result is cfg
        assert len(result.queue_configs) == 1

    def test_get_missing_returns_empty(self):
        result = get_notification_config("no-such-bucket")
        assert result.queue_configs == []
        assert result.topic_configs == []

    def test_overwrite(self):
        set_notification_config("b", NotificationConfig(queue_configs=[{"a": 1}]))
        set_notification_config("b", NotificationConfig(queue_configs=[{"a": 2}]))
        result = get_notification_config("b")
        assert result.queue_configs == [{"a": 2}]


class TestEventMatches:
    def test_exact_event_match(self):
        assert _event_matches("s3:ObjectCreated:Put", ["s3:ObjectCreated:Put"], "key", None) is True

    def test_wildcard_all(self):
        assert _event_matches("s3:ObjectCreated:Put", ["s3:*"], "key", None) is True

    def test_wildcard_prefix(self):
        assert _event_matches("s3:ObjectCreated:Put", ["s3:ObjectCreated:*"], "key", None) is True

    def test_no_match(self):
        assert (
            _event_matches(
                "s3:ObjectRemoved:Delete",
                ["s3:ObjectCreated:Put"],
                "key",
                None,
            )
            is False
        )

    def test_empty_events_list(self):
        assert _event_matches("s3:ObjectCreated:Put", [], "key", None) is False

    def test_filter_prefix(self):
        filter_rules = {
            "Key": {"FilterRules": [{"Name": "prefix", "Value": "images/"}]},
        }
        assert (
            _event_matches(
                "s3:ObjectCreated:Put",
                ["s3:*"],
                "images/photo.jpg",
                filter_rules,
            )
            is True
        )
        assert (
            _event_matches(
                "s3:ObjectCreated:Put",
                ["s3:*"],
                "docs/file.txt",
                filter_rules,
            )
            is False
        )

    def test_filter_suffix(self):
        filter_rules = {"Key": {"FilterRules": [{"Name": "suffix", "Value": ".jpg"}]}}
        assert _event_matches("s3:ObjectCreated:Put", ["s3:*"], "photo.jpg", filter_rules) is True
        assert _event_matches("s3:ObjectCreated:Put", ["s3:*"], "photo.png", filter_rules) is False

    def test_filter_prefix_and_suffix(self):
        filter_rules = {
            "Key": {
                "FilterRules": [
                    {"Name": "prefix", "Value": "logs/"},
                    {"Name": "suffix", "Value": ".gz"},
                ]
            }
        }
        assert (
            _event_matches(
                "s3:ObjectCreated:Put",
                ["s3:*"],
                "logs/app.gz",
                filter_rules,
            )
            is True
        )
        assert (
            _event_matches(
                "s3:ObjectCreated:Put",
                ["s3:*"],
                "logs/app.txt",
                filter_rules,
            )
            is False
        )
        assert (
            _event_matches(
                "s3:ObjectCreated:Put",
                ["s3:*"],
                "other/app.gz",
                filter_rules,
            )
            is False
        )

    def test_filter_no_key(self):
        # Filter exists but has no Key entry
        assert _event_matches("s3:ObjectCreated:Put", ["s3:*"], "key", {}) is True

    def test_event_matches_but_filter_fails(self):
        filter_rules = {"Key": {"FilterRules": [{"Name": "prefix", "Value": "x/"}]}}
        assert _event_matches("s3:ObjectCreated:Put", ["s3:*"], "y/file", filter_rules) is False


class TestBuildEventRecord:
    def test_record_structure(self):
        rec = _build_event_record(
            "s3:ObjectCreated:Put",
            "my-bucket",
            "my-key",
            "us-east-1",
            "123456789012",
            1024,
            "abc123",
        )
        assert rec["eventVersion"] == "2.1"
        assert rec["eventSource"] == "aws:s3"
        assert rec["awsRegion"] == "us-east-1"
        assert rec["eventName"] == "Put"
        assert rec["s3"]["bucket"]["name"] == "my-bucket"
        assert rec["s3"]["bucket"]["arn"] == "arn:aws:s3:::my-bucket"
        assert rec["s3"]["object"]["key"] == "my-key"
        assert rec["s3"]["object"]["size"] == 1024
        assert rec["s3"]["object"]["eTag"] == "abc123"
        assert rec["userIdentity"]["principalId"] == "123456789012"

    def test_event_name_strips_prefix(self):
        rec = _build_event_record("s3:ObjectRemoved:Delete", "b", "k", "r", "a", 0, "")
        assert rec["eventName"] == "Delete"


class TestFireEvent:
    def setup_method(self):
        _bucket_notifications.clear()

    def test_no_config_no_delivery(self):
        # Should not raise, just return silently
        fire_event("s3:ObjectCreated:Put", "bucket", "key")

    @patch("robotocore.services.s3.notifications._deliver_to_sqs")
    def test_delivers_to_sqs(self, mock_deliver):
        cfg = NotificationConfig(
            queue_configs=[
                {"QueueArn": "arn:aws:sqs:us-east-1:123:q", "Events": ["s3:ObjectCreated:*"]},
            ]
        )
        set_notification_config("bucket", cfg)
        fire_event("s3:ObjectCreated:Put", "bucket", "key", "us-east-1", "123")
        mock_deliver.assert_called_once()
        args = mock_deliver.call_args
        assert args[0][0] == "arn:aws:sqs:us-east-1:123:q"
        body = json.loads(args[0][1])
        assert "Records" in body

    @patch("robotocore.services.s3.notifications._deliver_to_sns")
    def test_delivers_to_sns(self, mock_deliver):
        cfg = NotificationConfig(
            topic_configs=[
                {"TopicArn": "arn:aws:sns:us-east-1:123:t", "Events": ["s3:*"]},
            ]
        )
        set_notification_config("bucket", cfg)
        fire_event("s3:ObjectCreated:Put", "bucket", "key", "us-east-1", "123")
        mock_deliver.assert_called_once()

    @patch("robotocore.services.s3.notifications._deliver_to_sqs")
    def test_filter_prevents_delivery(self, mock_deliver):
        cfg = NotificationConfig(
            queue_configs=[
                {
                    "QueueArn": "arn:aws:sqs:us-east-1:123:q",
                    "Events": ["s3:ObjectCreated:*"],
                    "Filter": {"Key": {"FilterRules": [{"Name": "suffix", "Value": ".jpg"}]}},
                },
            ]
        )
        set_notification_config("bucket", cfg)
        fire_event("s3:ObjectCreated:Put", "bucket", "file.txt", "us-east-1", "123")
        mock_deliver.assert_not_called()

    @patch("robotocore.services.s3.notifications._deliver_to_sqs")
    def test_event_type_mismatch_prevents_delivery(self, mock_deliver):
        cfg = NotificationConfig(
            queue_configs=[
                {"QueueArn": "arn:aws:sqs:us-east-1:123:q", "Events": ["s3:ObjectRemoved:*"]},
            ]
        )
        set_notification_config("bucket", cfg)
        fire_event("s3:ObjectCreated:Put", "bucket", "key", "us-east-1", "123")
        mock_deliver.assert_not_called()


class TestDeliverToSqs:
    @patch("robotocore.services.s3.notifications.get_sqs_store")
    def test_deliver_to_existing_queue(self, mock_get_store):
        mock_queue = MagicMock()
        mock_store = MagicMock()
        mock_store.get_queue.return_value = mock_queue
        mock_get_store.return_value = mock_store

        _deliver_to_sqs("arn:aws:sqs:us-east-1:123:myqueue", '{"msg": "hi"}', "us-east-1")

        mock_store.get_queue.assert_called_with("myqueue")
        mock_queue.put.assert_called_once()

    @patch("robotocore.services.s3.notifications.get_sqs_store")
    def test_deliver_to_missing_queue(self, mock_get_store):
        mock_store = MagicMock()
        mock_store.get_queue.return_value = None
        mock_get_store.return_value = mock_store

        # Should not raise
        _deliver_to_sqs("arn:aws:sqs:us-east-1:123:gone", '{"msg": "hi"}', "us-east-1")


class TestDeliverToSns:
    @patch("robotocore.services.s3.notifications.get_sns_store")
    def test_deliver_to_missing_topic(self, mock_get_store):
        mock_store = MagicMock()
        mock_store.get_topic.return_value = None
        mock_get_store.return_value = mock_store

        # Should not raise
        _deliver_to_sns("arn:aws:sns:us-east-1:123:gone", '{"msg": "hi"}', "us-east-1")

    @patch("robotocore.services.sns.provider._deliver_to_subscriber")
    @patch("robotocore.services.s3.notifications.get_sns_store")
    def test_deliver_to_existing_topic_with_subscribers(self, mock_get_store, mock_deliver_sub):
        confirmed_sub = MagicMock()
        confirmed_sub.confirmed = True
        unconfirmed_sub = MagicMock()
        unconfirmed_sub.confirmed = False

        mock_topic = MagicMock()
        mock_topic.subscriptions = [confirmed_sub, unconfirmed_sub]
        mock_store = MagicMock()
        mock_store.get_topic.return_value = mock_topic
        mock_get_store.return_value = mock_store

        _deliver_to_sns("arn:aws:sns:us-east-1:123:my-topic", '{"msg": "hi"}', "us-east-1")

        # Only confirmed subscriber should get delivery
        assert mock_deliver_sub.call_count == 1
        call_args = mock_deliver_sub.call_args[0]
        assert call_args[0] is confirmed_sub


class TestFireEventMultipleTargets:
    """Test fire_event with multiple and mixed notification targets."""

    def setup_method(self):
        _bucket_notifications.clear()

    @patch("robotocore.services.s3.notifications._deliver_to_sns")
    @patch("robotocore.services.s3.notifications._deliver_to_sqs")
    def test_delivers_to_both_sqs_and_sns(self, mock_sqs, mock_sns):
        cfg = NotificationConfig(
            queue_configs=[
                {
                    "QueueArn": "arn:aws:sqs:us-east-1:123:q",
                    "Events": ["s3:ObjectCreated:*"],
                },
            ],
            topic_configs=[
                {
                    "TopicArn": "arn:aws:sns:us-east-1:123:t",
                    "Events": ["s3:ObjectCreated:*"],
                },
            ],
        )
        set_notification_config("bucket", cfg)
        fire_event("s3:ObjectCreated:Put", "bucket", "key", "us-east-1", "123")
        mock_sqs.assert_called_once()
        mock_sns.assert_called_once()

    @patch("robotocore.services.s3.notifications._deliver_to_sqs")
    def test_delivers_to_multiple_queues(self, mock_sqs):
        cfg = NotificationConfig(
            queue_configs=[
                {
                    "QueueArn": "arn:aws:sqs:us-east-1:123:q1",
                    "Events": ["s3:ObjectCreated:*"],
                },
                {
                    "QueueArn": "arn:aws:sqs:us-east-1:123:q2",
                    "Events": ["s3:ObjectCreated:*"],
                },
            ],
        )
        set_notification_config("bucket", cfg)
        fire_event("s3:ObjectCreated:Put", "bucket", "key", "us-east-1", "123")
        assert mock_sqs.call_count == 2
        arns = {call[0][0] for call in mock_sqs.call_args_list}
        assert arns == {
            "arn:aws:sqs:us-east-1:123:q1",
            "arn:aws:sqs:us-east-1:123:q2",
        }

    @patch("robotocore.services.s3.notifications._deliver_to_sqs")
    def test_only_matching_configs_fire(self, mock_sqs):
        """One config matches, the other does not."""
        cfg = NotificationConfig(
            queue_configs=[
                {
                    "QueueArn": "arn:aws:sqs:us-east-1:123:q1",
                    "Events": ["s3:ObjectCreated:*"],
                },
                {
                    "QueueArn": "arn:aws:sqs:us-east-1:123:q2",
                    "Events": ["s3:ObjectRemoved:*"],
                },
            ],
        )
        set_notification_config("bucket", cfg)
        fire_event("s3:ObjectCreated:Put", "bucket", "key", "us-east-1", "123")
        assert mock_sqs.call_count == 1
        assert mock_sqs.call_args[0][0] == "arn:aws:sqs:us-east-1:123:q1"
