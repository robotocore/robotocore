"""Advanced tests for S3 EventBridge notifications and replication engine."""

import json
import logging
from unittest.mock import MagicMock, patch

from robotocore.services.s3.notifications import (
    NotificationConfig,
    _bucket_notifications,
    _build_event_record,
    _deliver_to_eventbridge,
    fire_event,
    get_notification_config,
    set_notification_config,
)
from robotocore.services.s3.replication import (
    _replicate_object,
    maybe_replicate,
)


class TestEventBridgeFiresForCopyObject:
    """Events fire for CopyObject operations via EventBridge."""

    def setup_method(self):
        _bucket_notifications.clear()

    def test_copy_object_fires_eventbridge(self):
        config = NotificationConfig(eventbridge_enabled=True)
        set_notification_config("src-bucket", config)
        with patch("robotocore.services.s3.notifications._deliver_to_eventbridge") as mock_eb:
            fire_event(
                "s3:ObjectCreated:Copy",
                "src-bucket",
                "copied-key.txt",
                "us-east-1",
                "123456789012",
                2048,
                "etag-copy",
            )
        mock_eb.assert_called_once_with(
            "s3:ObjectCreated:Copy",
            "src-bucket",
            "copied-key.txt",
            "us-east-1",
            "123456789012",
            2048,
            "etag-copy",
        )

    def test_copy_object_eventbridge_detail_type(self):
        """CopyObject should produce detail-type 'Object Created' with reason 'CopyObject'."""
        with patch("robotocore.services.events.provider.publish_event_to_bus") as mock_pub:
            import robotocore.services.events.provider as _ev_mod

            original = _ev_mod.publish_event_to_bus
            _ev_mod.publish_event_to_bus = mock_pub
            try:
                _deliver_to_eventbridge(
                    "s3:ObjectCreated:Copy",
                    "my-bucket",
                    "dest-key.txt",
                    "us-west-2",
                    "111222333444",
                    512,
                    "etag-abc",
                )
            finally:
                _ev_mod.publish_event_to_bus = original
        event = mock_pub.call_args[0][0]
        assert event["detail-type"] == "Object Created"
        assert event["detail"]["reason"] == "CopyObject"
        assert event["detail"]["object"]["key"] == "dest-key.txt"
        assert event["detail"]["object"]["size"] == 512
        assert event["detail"]["object"]["etag"] == "etag-abc"


class TestEventBridgeFiresForDeleteWithVersioning:
    """Events fire for DeleteObject and DeleteMarkerCreated."""

    def setup_method(self):
        _bucket_notifications.clear()

    def test_delete_object_fires_eventbridge(self):
        config = NotificationConfig(eventbridge_enabled=True)
        set_notification_config("versioned-bucket", config)
        with patch("robotocore.services.s3.notifications._deliver_to_eventbridge") as mock_eb:
            fire_event(
                "s3:ObjectRemoved:Delete",
                "versioned-bucket",
                "deleted-key",
                "us-east-1",
                "123456789012",
            )
        mock_eb.assert_called_once()
        assert mock_eb.call_args[0][0] == "s3:ObjectRemoved:Delete"

    def test_delete_marker_created_fires_eventbridge(self):
        config = NotificationConfig(eventbridge_enabled=True)
        set_notification_config("versioned-bucket", config)
        with patch("robotocore.services.s3.notifications._deliver_to_eventbridge") as mock_eb:
            fire_event(
                "s3:ObjectRemoved:DeleteMarkerCreated",
                "versioned-bucket",
                "deleted-key",
            )
        mock_eb.assert_called_once()
        assert mock_eb.call_args[0][0] == "s3:ObjectRemoved:DeleteMarkerCreated"

    def test_delete_eventbridge_detail_type_is_object_deleted(self):
        with patch("robotocore.services.events.provider.publish_event_to_bus") as mock_pub:
            import robotocore.services.events.provider as _ev_mod

            original = _ev_mod.publish_event_to_bus
            _ev_mod.publish_event_to_bus = mock_pub
            try:
                _deliver_to_eventbridge(
                    "s3:ObjectRemoved:Delete",
                    "b",
                    "k",
                    "us-east-1",
                    "123456789012",
                    0,
                    "",
                )
            finally:
                _ev_mod.publish_event_to_bus = original
        event = mock_pub.call_args[0][0]
        assert event["detail-type"] == "Object Deleted"
        assert event["detail"]["reason"] == "DeleteObject"

    def test_delete_marker_created_detail_type(self):
        with patch("robotocore.services.events.provider.publish_event_to_bus") as mock_pub:
            import robotocore.services.events.provider as _ev_mod

            original = _ev_mod.publish_event_to_bus
            _ev_mod.publish_event_to_bus = mock_pub
            try:
                _deliver_to_eventbridge(
                    "s3:ObjectRemoved:DeleteMarkerCreated",
                    "b",
                    "k",
                    "us-east-1",
                    "123456789012",
                    0,
                    "",
                )
            finally:
                _ev_mod.publish_event_to_bus = original
        event = mock_pub.call_args[0][0]
        assert event["detail-type"] == "Object Deleted"


class TestEventBridgePersistsAcrossNotificationConfigs:
    """EventBridge config persists across PutBucketNotificationConfiguration calls."""

    def setup_method(self):
        _bucket_notifications.clear()

    def test_eventbridge_survives_overwrite_when_re_enabled(self):
        cfg1 = NotificationConfig(eventbridge_enabled=True, queue_configs=[{"q": 1}])
        set_notification_config("bucket", cfg1)
        assert get_notification_config("bucket").eventbridge_enabled is True

        # Simulate PutBucketNotificationConfiguration with eventbridge still enabled
        cfg2 = NotificationConfig(
            eventbridge_enabled=True,
            queue_configs=[{"q": 2}],
        )
        set_notification_config("bucket", cfg2)
        result = get_notification_config("bucket")
        assert result.eventbridge_enabled is True
        assert result.queue_configs == [{"q": 2}]

    def test_eventbridge_disabled_after_overwrite(self):
        cfg1 = NotificationConfig(eventbridge_enabled=True)
        set_notification_config("bucket", cfg1)
        cfg2 = NotificationConfig(eventbridge_enabled=False)
        set_notification_config("bucket", cfg2)
        assert get_notification_config("bucket").eventbridge_enabled is False

    def test_multiple_buckets_independent_config(self):
        set_notification_config("a", NotificationConfig(eventbridge_enabled=True))
        set_notification_config("b", NotificationConfig(eventbridge_enabled=False))
        assert get_notification_config("a").eventbridge_enabled is True
        assert get_notification_config("b").eventbridge_enabled is False


class TestMultipleEventTypesInSingleNotification:
    """A single notification config can match multiple event types."""

    def setup_method(self):
        _bucket_notifications.clear()

    @patch("robotocore.services.s3.notifications._deliver_to_sqs")
    def test_single_queue_config_with_wildcard_receives_multiple_events(self, mock_sqs):
        cfg = NotificationConfig(
            queue_configs=[
                {
                    "QueueArn": "arn:aws:sqs:us-east-1:123:all-events",
                    "Events": ["s3:ObjectCreated:*", "s3:ObjectRemoved:*"],
                }
            ]
        )
        set_notification_config("bucket", cfg)

        fire_event("s3:ObjectCreated:Put", "bucket", "k1", "us-east-1", "123")
        fire_event("s3:ObjectRemoved:Delete", "bucket", "k2", "us-east-1", "123")

        assert mock_sqs.call_count == 2
        body1 = json.loads(mock_sqs.call_args_list[0][0][1])
        body2 = json.loads(mock_sqs.call_args_list[1][0][1])
        assert body1["Records"][0]["eventName"] == "ObjectCreated:Put"
        assert body2["Records"][0]["eventName"] == "ObjectRemoved:Delete"

    @patch("robotocore.services.s3.notifications._deliver_to_sqs")
    @patch("robotocore.services.s3.notifications._deliver_to_eventbridge")
    def test_sqs_and_eventbridge_both_fire(self, mock_eb, mock_sqs):
        cfg = NotificationConfig(
            queue_configs=[
                {
                    "QueueArn": "arn:aws:sqs:us-east-1:123:q",
                    "Events": ["s3:ObjectCreated:*"],
                }
            ],
            eventbridge_enabled=True,
        )
        set_notification_config("bucket", cfg)
        fire_event("s3:ObjectCreated:Put", "bucket", "key", "us-east-1", "123")
        mock_sqs.assert_called_once()
        mock_eb.assert_called_once()


class TestEventRecordMetadata:
    """Events contain correct bucket/key/size metadata."""

    def test_record_has_correct_bucket_arn(self):
        rec = _build_event_record(
            "s3:ObjectCreated:Put", "test-bkt", "obj.txt", "eu-west-1", "999", 100, "e1"
        )
        assert rec["s3"]["bucket"]["arn"] == "arn:aws:s3:::test-bkt"
        assert rec["s3"]["bucket"]["name"] == "test-bkt"
        assert rec["s3"]["bucket"]["ownerIdentity"]["principalId"] == "999"

    def test_record_has_correct_object_metadata(self):
        rec = _build_event_record(
            "s3:ObjectCreated:Put", "b", "path/to/obj.bin", "us-east-1", "123", 65536, "deadbeef"
        )
        assert rec["s3"]["object"]["key"] == "path/to/obj.bin"
        assert rec["s3"]["object"]["size"] == 65536
        assert rec["s3"]["object"]["eTag"] == "deadbeef"

    def test_record_has_zero_size_for_empty_object(self):
        rec = _build_event_record("s3:ObjectCreated:Put", "b", "empty", "r", "a", 0, "")
        assert rec["s3"]["object"]["size"] == 0
        assert rec["s3"]["object"]["eTag"] == ""

    def test_record_sequencer_is_hex_string(self):
        rec = _build_event_record("s3:ObjectCreated:Put", "b", "k", "r", "a", 0, "")
        seq = rec["s3"]["object"]["sequencer"]
        assert len(seq) == 16
        int(seq, 16)  # Must parse as hex

    def test_event_time_format(self):
        rec = _build_event_record("s3:ObjectCreated:Put", "b", "k", "r", "a", 0, "")
        assert rec["eventTime"].endswith("Z")
        assert "T" in rec["eventTime"]

    def test_eventbridge_event_has_correct_metadata(self):
        with patch("robotocore.services.events.provider.publish_event_to_bus") as mock_pub:
            import robotocore.services.events.provider as _ev_mod

            original = _ev_mod.publish_event_to_bus
            _ev_mod.publish_event_to_bus = mock_pub
            try:
                _deliver_to_eventbridge(
                    "s3:ObjectCreated:Put",
                    "meta-bucket",
                    "meta-key.txt",
                    "ap-southeast-1",
                    "555666777888",
                    4096,
                    "etag-meta",
                )
            finally:
                _ev_mod.publish_event_to_bus = original
        event = mock_pub.call_args[0][0]
        assert event["source"] == "aws.s3"
        assert event["account"] == "555666777888"
        assert event["region"] == "ap-southeast-1"
        assert "arn:aws:s3:::meta-bucket" in event["resources"]
        assert event["detail"]["bucket"]["name"] == "meta-bucket"
        assert event["detail"]["object"]["key"] == "meta-key.txt"
        assert event["detail"]["object"]["size"] == 4096
        assert event["detail"]["object"]["etag"] == "etag-meta"
        assert event["detail"]["requester"] == "555666777888"


class TestReplicationEngineOnPut:
    """Replication engine: replicate on put."""

    def test_replicate_object_copies_and_fires_event(self):
        mock_backend = MagicMock()
        src_obj = MagicMock()
        mock_backend.get_object.return_value = src_obj
        with patch("robotocore.services.s3.replication.get_backend") as mock_gb:
            mock_gb.return_value.__getitem__.return_value.__getitem__.return_value = mock_backend
            with patch("robotocore.services.s3.replication.fire_event") as mock_fire:
                _replicate_object(
                    "src-bucket", "data/file.txt", "dest-bucket", "us-east-1", "123456789012", "r1"
                )
        mock_backend.get_object.assert_called_once_with("src-bucket", "data/file.txt")
        mock_backend.copy_object.assert_called_once_with(src_obj, "dest-bucket", "data/file.txt")
        mock_fire.assert_called_once_with(
            "s3:Replication:OperationReplicatedAfterThreshold",
            "src-bucket",
            "data/file.txt",
            "us-east-1",
            "123456789012",
        )


class TestReplicationMissingDestBucket:
    """Replication handles missing destination bucket gracefully."""

    def test_copy_to_nonexistent_dest_logs_error(self, caplog):
        mock_backend = MagicMock()
        mock_backend.get_object.return_value = MagicMock()
        mock_backend.copy_object.side_effect = Exception("NoSuchBucket")
        with patch("robotocore.services.s3.replication.get_backend") as mock_gb:
            mock_gb.return_value.__getitem__.return_value.__getitem__.return_value = mock_backend
            with caplog.at_level(logging.ERROR, logger="robotocore.services.s3.replication"):
                _replicate_object(
                    "src", "key.txt", "nonexistent-dest", "us-east-1", "123456789012", "r1"
                )
        assert any("nonexistent-dest" in rec.message for rec in caplog.records)

    def test_missing_dest_does_not_fire_event(self):
        mock_backend = MagicMock()
        mock_backend.get_object.return_value = MagicMock()
        mock_backend.copy_object.side_effect = Exception("NoSuchBucket")
        with patch("robotocore.services.s3.replication.get_backend") as mock_gb:
            mock_gb.return_value.__getitem__.return_value.__getitem__.return_value = mock_backend
            with patch("robotocore.services.s3.replication.fire_event") as mock_fire:
                _replicate_object("src", "key.txt", "bad-dest", "us-east-1", "123456789012", "r1")
        mock_fire.assert_not_called()


class TestVersionedReplication:
    """Replication with versioned objects."""

    def test_replicate_preserves_key_path(self):
        """Replication uses the same key in the destination bucket."""
        mock_backend = MagicMock()
        src_obj = MagicMock()
        mock_backend.get_object.return_value = src_obj
        with patch("robotocore.services.s3.replication.get_backend") as mock_gb:
            mock_gb.return_value.__getitem__.return_value.__getitem__.return_value = mock_backend
            with patch("robotocore.services.s3.replication.fire_event"):
                _replicate_object(
                    "src", "deep/nested/key.txt", "dest", "us-east-1", "123456789012", "r1"
                )
        mock_backend.copy_object.assert_called_once_with(src_obj, "dest", "deep/nested/key.txt")

    def test_maybe_replicate_with_filter_prefix(self):
        """Replication rules using Filter.Prefix correctly match keys."""
        mock_backend = MagicMock()
        mock_backend.get_bucket_replication.return_value = {
            "Rule": [
                {
                    "Status": "Enabled",
                    "Filter": {"Prefix": "versioned/"},
                    "Destination": {"Bucket": "arn:aws:s3:::dest"},
                    "ID": "v1",
                }
            ]
        }
        with patch("robotocore.services.s3.replication.get_backend") as mock_gb:
            mock_gb.return_value.__getitem__.return_value.__getitem__.return_value = mock_backend
            with patch("robotocore.services.s3.replication._executor") as mock_ex:
                maybe_replicate("src", "versioned/obj.txt", "us-east-1", "123456789012")
        mock_ex.submit.assert_called_once()

    def test_maybe_replicate_filter_prefix_no_match(self):
        mock_backend = MagicMock()
        mock_backend.get_bucket_replication.return_value = {
            "Rule": [
                {
                    "Status": "Enabled",
                    "Filter": {"Prefix": "versioned/"},
                    "Destination": {"Bucket": "arn:aws:s3:::dest"},
                    "ID": "v1",
                }
            ]
        }
        with patch("robotocore.services.s3.replication.get_backend") as mock_gb:
            mock_gb.return_value.__getitem__.return_value.__getitem__.return_value = mock_backend
            with patch("robotocore.services.s3.replication._executor") as mock_ex:
                maybe_replicate("src", "other/obj.txt", "us-east-1", "123456789012")
        mock_ex.submit.assert_not_called()

    def test_replicate_fires_replication_event_not_created(self):
        """Replication fires s3:Replication:* event, not s3:ObjectCreated:*."""
        mock_backend = MagicMock()
        mock_backend.get_object.return_value = MagicMock()
        with patch("robotocore.services.s3.replication.get_backend") as mock_gb:
            mock_gb.return_value.__getitem__.return_value.__getitem__.return_value = mock_backend
            with patch("robotocore.services.s3.replication.fire_event") as mock_fire:
                _replicate_object("s", "k", "d", "us-east-1", "123456789012", "r1")
        event_name = mock_fire.call_args[0][0]
        assert event_name.startswith("s3:Replication:")
        assert "ObjectCreated" not in event_name
