"""
Tests for user preference management: channel opt-in/opt-out,
CRITICAL priority bypass, and quiet hours.
"""

from .models import Channel, DeliveryStatus, Priority, UserPreferences


class TestUserPreferences:
    def test_set_and_get_preferences(self, notifier):
        """Set user preferences and retrieve them."""
        prefs = UserPreferences(
            user_id="pref-user-001",
            channels={"email": True, "sms": False, "push": True},
        )
        notifier.set_user_preferences(prefs)

        loaded = notifier.get_user_preferences("pref-user-001")
        assert loaded.user_id == "pref-user-001"
        assert loaded.channels["email"] is True
        assert loaded.channels["sms"] is False
        assert loaded.channels["push"] is True

    def test_default_preferences_all_enabled(self, notifier):
        """Unset preferences default to all channels enabled."""
        prefs = notifier.get_user_preferences("unknown-user")
        assert prefs.is_channel_enabled(Channel.EMAIL) is True
        assert prefs.is_channel_enabled(Channel.SMS) is True

    def test_unsubscribe_channel(self, notifier):
        """Unsubscribing a channel sets it to False."""
        notifier.set_user_preferences(
            UserPreferences(user_id="unsub-user", channels={"email": True, "sms": True})
        )
        notifier.unsubscribe_channel("unsub-user", Channel.SMS)
        prefs = notifier.get_user_preferences("unsub-user")
        assert prefs.channels["sms"] is False
        assert prefs.channels["email"] is True

    def test_subscribe_channel(self, notifier):
        """Re-subscribing a channel sets it back to True."""
        notifier.set_user_preferences(
            UserPreferences(user_id="resub-user", channels={"email": False})
        )
        notifier.subscribe_channel("resub-user", Channel.EMAIL)
        prefs = notifier.get_user_preferences("resub-user")
        assert prefs.channels["email"] is True


class TestPreferenceEnforcement:
    def test_opted_out_channel_blocks_notification(self, notifier, email_channel, sample_template):
        """Notification to an opted-out channel returns None."""
        notifier.set_user_preferences(
            UserPreferences(user_id="block-user", channels={"email": False})
        )
        result = notifier.send_notification(
            user_id="block-user",
            channel=Channel.EMAIL,
            template_id="welcome-email",
            variables={"name": "Blocked", "company": "Corp"},
        )
        assert result is None

    def test_opted_in_channel_sends(self, notifier, email_channel, sample_template):
        """Notification to an opted-in channel succeeds."""
        notifier.set_user_preferences(
            UserPreferences(user_id="optin-user", channels={"email": True})
        )
        result = notifier.send_notification(
            user_id="optin-user",
            channel=Channel.EMAIL,
            template_id="welcome-email",
            variables={"name": "OptIn", "company": "Corp"},
        )
        assert result is not None
        assert result.status == DeliveryStatus.SENT

    def test_critical_bypasses_opt_out(self, notifier, email_channel, sample_template):
        """CRITICAL priority sends even if user opted out of the channel."""
        notifier.set_user_preferences(
            UserPreferences(user_id="critical-user", channels={"email": False})
        )
        result = notifier.send_notification(
            user_id="critical-user",
            channel=Channel.EMAIL,
            template_id="welcome-email",
            variables={"name": "Critical", "company": "Corp"},
            priority=Priority.CRITICAL,
        )
        assert result is not None
        assert result.priority == Priority.CRITICAL
        assert result.status == DeliveryStatus.SENT


class TestQuietHours:
    def test_quiet_hours_blocks_notification(self, notifier, email_channel, sample_template):
        """Notifications during quiet hours are suppressed."""
        notifier.set_user_preferences(
            UserPreferences(
                user_id="quiet-user",
                channels={"email": True},
                quiet_hours_start="22:00",
                quiet_hours_end="08:00",
            )
        )
        # Simulate sending at 23:00 (within quiet hours)
        result = notifier.send_notification_quiet_hours_aware(
            user_id="quiet-user",
            channel=Channel.EMAIL,
            template_id="welcome-email",
            variables={"name": "Quiet", "company": "Corp"},
            current_hour=23,
            current_minute=0,
        )
        assert result is None

    def test_outside_quiet_hours_sends(self, notifier, email_channel, sample_template):
        """Notifications outside quiet hours are sent."""
        notifier.set_user_preferences(
            UserPreferences(
                user_id="noisy-user",
                channels={"email": True},
                quiet_hours_start="22:00",
                quiet_hours_end="08:00",
            )
        )
        # Simulate sending at 12:00 (outside quiet hours)
        result = notifier.send_notification_quiet_hours_aware(
            user_id="noisy-user",
            channel=Channel.EMAIL,
            template_id="welcome-email",
            variables={"name": "Noisy", "company": "Corp"},
            current_hour=12,
            current_minute=0,
        )
        assert result is not None
        assert result.status == DeliveryStatus.SENT

    def test_critical_bypasses_quiet_hours(self, notifier, email_channel, sample_template):
        """CRITICAL priority sends even during quiet hours."""
        notifier.set_user_preferences(
            UserPreferences(
                user_id="crit-quiet-user",
                channels={"email": True},
                quiet_hours_start="22:00",
                quiet_hours_end="08:00",
            )
        )
        result = notifier.send_notification_quiet_hours_aware(
            user_id="crit-quiet-user",
            channel=Channel.EMAIL,
            template_id="welcome-email",
            variables={"name": "CritQuiet", "company": "Corp"},
            priority=Priority.CRITICAL,
            current_hour=23,
            current_minute=30,
        )
        assert result is not None
        assert result.priority == Priority.CRITICAL
