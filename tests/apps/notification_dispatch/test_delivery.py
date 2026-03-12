"""
Tests for delivery tracking: sending notifications, recording delivery
records in DynamoDB, status tracking, retries, and bulk sends.
"""

from .models import Channel, DeliveryStatus


class TestSendAndTrack:
    def test_send_notification_creates_delivery_record(
        self, notifier, email_channel, sample_template
    ):
        """Sending a notification creates a delivery record in DynamoDB."""
        notif = notifier.send_notification(
            user_id="user-001",
            channel=Channel.EMAIL,
            template_id="welcome-email",
            variables={"name": "Alice", "company": "Acme"},
        )
        assert notif is not None
        assert notif.notification_id.startswith("NOTIF-")
        assert notif.status == DeliveryStatus.SENT

        record = notifier.get_delivery_record(notif.notification_id, Channel.EMAIL.value)
        assert record is not None
        assert record.user_id == "user-001"
        assert record.status == DeliveryStatus.SENT

    def test_delivery_status_tracking(self, notifier, email_channel, sample_template):
        """Delivery status can be updated from SENT to DELIVERED."""
        notif = notifier.send_notification(
            user_id="user-002",
            channel=Channel.EMAIL,
            template_id="welcome-email",
            variables={"name": "Bob", "company": "Corp"},
        )
        assert notif is not None

        notifier.update_delivery_status(
            notif.notification_id, Channel.EMAIL.value, DeliveryStatus.DELIVERED
        )
        record = notifier.get_delivery_record(notif.notification_id, Channel.EMAIL.value)
        assert record is not None
        assert record.status == DeliveryStatus.DELIVERED

    def test_failed_delivery_recorded(self, notifier, email_channel, sample_template):
        """Failed delivery is recorded with error message."""
        notif = notifier.send_notification(
            user_id="user-003",
            channel=Channel.EMAIL,
            template_id="welcome-email",
            variables={"name": "Carol", "company": "Inc"},
        )
        assert notif is not None

        notifier.update_delivery_status(
            notif.notification_id,
            Channel.EMAIL.value,
            DeliveryStatus.FAILED,
            error_message="Mailbox full",
        )
        record = notifier.get_delivery_record(notif.notification_id, Channel.EMAIL.value)
        assert record is not None
        assert record.status == DeliveryStatus.FAILED
        assert record.error_message == "Mailbox full"


class TestRetry:
    def test_retry_failed_delivery(self, notifier, email_channel, sample_template):
        """Retrying a failed delivery increments attempt and re-publishes."""
        notif = notifier.send_notification(
            user_id="user-004",
            channel=Channel.EMAIL,
            template_id="welcome-email",
            variables={"name": "Dana", "company": "LLC"},
        )
        assert notif is not None

        # Mark as failed
        notifier.update_delivery_status(
            notif.notification_id, Channel.EMAIL.value, DeliveryStatus.FAILED
        )

        # Retry
        retried = notifier.retry_failed_delivery(notif.notification_id, Channel.EMAIL.value)
        assert retried is not None
        assert retried.attempt == 2
        assert retried.status == DeliveryStatus.SENT

    def test_retry_nonexistent_returns_none(self, notifier, email_channel):
        """Retrying a nonexistent delivery returns None."""
        result = notifier.retry_failed_delivery("NOTIF-nonexistent", "email")
        assert result is None


class TestBulkSend:
    def test_bulk_send(self, notifier, email_channel, sample_template):
        """Bulk send delivers to multiple users."""
        result = notifier.bulk_send(
            user_ids=["user-b1", "user-b2", "user-b3"],
            channel=Channel.EMAIL,
            template_id="welcome-email",
            variables={"name": "User", "company": "Bulk Corp"},
        )
        assert result.total == 3
        assert result.sent == 3
        assert result.failed == 0
        assert len(result.notification_ids) == 3

        # Verify each has a delivery record
        for nid in result.notification_ids:
            record = notifier.get_delivery_record(nid, Channel.EMAIL.value)
            assert record is not None
            assert record.status == DeliveryStatus.SENT

    def test_bulk_send_with_opted_out_users(self, notifier, email_channel, sample_template):
        """Bulk send skips users who opted out of the channel."""
        from .models import UserPreferences

        notifier.set_user_preferences(
            UserPreferences(
                user_id="user-opted-out",
                channels={"email": False},
            )
        )

        result = notifier.bulk_send(
            user_ids=["user-active", "user-opted-out"],
            channel=Channel.EMAIL,
            template_id="welcome-email",
            variables={"name": "Test", "company": "Co"},
        )
        assert result.total == 2
        assert result.sent == 1
        assert result.failed == 1
