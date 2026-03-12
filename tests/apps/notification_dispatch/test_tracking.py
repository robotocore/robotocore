"""
Tests for delivery history queries, stats aggregation, and CloudWatch metrics.
"""

from .models import Channel, DeliveryStatus


class TestDeliveryHistory:
    def test_query_by_user(self, notifier, email_channel, sample_template):
        """Query delivery records for a specific user."""
        for i in range(3):
            notifier.send_notification(
                user_id="track-user-A",
                channel=Channel.EMAIL,
                template_id="welcome-email",
                variables={"name": f"User{i}", "company": "Corp"},
            )
        # Send one for a different user
        notifier.send_notification(
            user_id="track-user-B",
            channel=Channel.EMAIL,
            template_id="welcome-email",
            variables={"name": "Other", "company": "Corp"},
        )

        records = notifier.query_deliveries_by_user("track-user-A")
        assert len(records) >= 3
        assert all(r.user_id == "track-user-A" for r in records)

    def test_query_by_channel(
        self,
        notifier,
        email_channel,
        sms_channel,
        sample_template,
        sms_template,
    ):
        """Query delivery records by channel."""
        notifier.send_notification(
            user_id="chan-user",
            channel=Channel.EMAIL,
            template_id="welcome-email",
            variables={"name": "ChanUser", "company": "Corp"},
        )
        notifier.send_notification(
            user_id="chan-user",
            channel=Channel.SMS,
            template_id="verify-sms",
            variables={"code": "1234", "minutes": "10"},
        )

        email_records = notifier.query_deliveries_by_channel("email")
        sms_records = notifier.query_deliveries_by_channel("sms")
        assert len(email_records) >= 1
        assert len(sms_records) >= 1
        assert all(r.channel == "email" for r in email_records)
        assert all(r.channel == "sms" for r in sms_records)

    def test_query_by_date_range(self, notifier, email_channel, sample_template):
        """Query delivery records within a date range."""
        notif = notifier.send_notification(
            user_id="date-user",
            channel=Channel.EMAIL,
            template_id="welcome-email",
            variables={"name": "DateUser", "company": "Corp"},
        )
        assert notif is not None

        # Query with a wide range that includes now
        records = notifier.query_deliveries_by_date_range(
            "date-user", "2020-01-01T00:00:00Z", "2030-12-31T23:59:59Z"
        )
        assert len(records) >= 1
        assert records[0].user_id == "date-user"


class TestDeliveryStats:
    def test_delivery_stats_calculation(self, notifier, email_channel, sample_template):
        """Stats calculated from delivery records."""
        # Send 3, deliver 2, fail 1
        nids = []
        for i in range(3):
            notif = notifier.send_notification(
                user_id=f"stat-user-{i}",
                channel=Channel.EMAIL,
                template_id="welcome-email",
                variables={"name": f"Stat{i}", "company": "Corp"},
            )
            assert notif is not None
            nids.append(notif.notification_id)

        notifier.update_delivery_status(nids[0], "email", DeliveryStatus.DELIVERED)
        notifier.update_delivery_status(nids[1], "email", DeliveryStatus.DELIVERED)
        notifier.update_delivery_status(nids[2], "email", DeliveryStatus.FAILED)

        stats = notifier.get_delivery_stats("email")
        assert stats.channel == "email"
        assert stats.total_sent >= 3
        assert stats.delivered >= 2
        assert stats.failed >= 1


class TestCloudWatchMetrics:
    def test_metrics_published_on_send(self, notifier, email_channel, sample_template):
        """Sending a notification publishes a CloudWatch metric."""
        notif = notifier.send_notification(
            user_id="metric-user",
            channel=Channel.EMAIL,
            template_id="welcome-email",
            variables={"name": "MetricUser", "company": "Corp"},
        )
        assert notif is not None

        datapoints = notifier.get_cloudwatch_metric("NotificationSent", "email")
        assert len(datapoints) >= 1
        assert datapoints[0]["Sum"] >= 1.0

    def test_publish_delivery_metrics(self, notifier, email_channel, sample_template):
        """publish_delivery_metrics calculates and publishes aggregated stats."""
        for i in range(2):
            notif = notifier.send_notification(
                user_id=f"agg-user-{i}",
                channel=Channel.EMAIL,
                template_id="welcome-email",
                variables={"name": f"Agg{i}", "company": "Corp"},
            )
            assert notif is not None

        stats = notifier.publish_delivery_metrics("email")
        assert stats.total_sent >= 2
