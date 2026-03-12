"""
Tests for scheduled notifications: create, list, cancel, and bulk schedule.
"""

from .models import Channel, Priority


class TestScheduleNotification:
    def test_schedule_notification(self, notifier, email_channel, sample_template):
        """Schedule a notification for future delivery."""
        sched = notifier.schedule_notification(
            user_id="sched-user-001",
            channel=Channel.EMAIL,
            template_id="welcome-email",
            variables={"name": "Scheduler", "company": "Corp"},
            scheduled_for="2026-04-01T09:00:00Z",
        )
        assert sched.schedule_id.startswith("SCHED-")
        assert sched.user_id == "sched-user-001"
        assert sched.channel == Channel.EMAIL
        assert sched.scheduled_for == "2026-04-01T09:00:00Z"
        assert sched.cancelled is False

    def test_list_scheduled_notifications(self, notifier, email_channel, sample_template):
        """List scheduled notifications for a user."""
        for i in range(3):
            notifier.schedule_notification(
                user_id="sched-list-user",
                channel=Channel.EMAIL,
                template_id="welcome-email",
                variables={"name": f"Sched{i}", "company": "Corp"},
                scheduled_for=f"2026-04-{i + 1:02d}T09:00:00Z",
            )

        scheduled = notifier.list_scheduled_notifications("sched-list-user")
        assert len(scheduled) == 3
        assert all(s.user_id == "sched-list-user" for s in scheduled)

    def test_cancel_scheduled_notification(self, notifier, email_channel, sample_template):
        """Cancel a scheduled notification."""
        sched = notifier.schedule_notification(
            user_id="sched-cancel-user",
            channel=Channel.EMAIL,
            template_id="welcome-email",
            variables={"name": "Cancel", "company": "Corp"},
            scheduled_for="2026-04-15T09:00:00Z",
        )

        result = notifier.cancel_scheduled_notification(sched.schedule_id)
        assert result is True

        # Cancelled notifications should not appear in list
        scheduled = notifier.list_scheduled_notifications("sched-cancel-user")
        assert len(scheduled) == 0

    def test_cancel_nonexistent_returns_false(self, notifier):
        """Cancelling a nonexistent schedule returns False."""
        result = notifier.cancel_scheduled_notification("SCHED-nonexistent")
        assert result is False


class TestBulkSchedule:
    def test_bulk_schedule(self, notifier, email_channel, sample_template):
        """Schedule the same notification for multiple users."""
        results = notifier.bulk_schedule(
            user_ids=["bulk-sched-1", "bulk-sched-2", "bulk-sched-3"],
            channel=Channel.EMAIL,
            template_id="welcome-email",
            variables={"name": "BulkUser", "company": "Corp"},
            scheduled_for="2026-05-01T10:00:00Z",
        )
        assert len(results) == 3
        assert all(s.scheduled_for == "2026-05-01T10:00:00Z" for s in results)
        user_ids = {s.user_id for s in results}
        assert user_ids == {"bulk-sched-1", "bulk-sched-2", "bulk-sched-3"}

    def test_bulk_schedule_with_priority(self, notifier, email_channel, sample_template):
        """Bulk schedule with HIGH priority."""
        results = notifier.bulk_schedule(
            user_ids=["pri-sched-1", "pri-sched-2"],
            channel=Channel.EMAIL,
            template_id="welcome-email",
            variables={"name": "Priority", "company": "Corp"},
            scheduled_for="2026-05-01T10:00:00Z",
            priority=Priority.HIGH,
        )
        assert len(results) == 2
        assert all(s.priority == Priority.HIGH for s in results)
