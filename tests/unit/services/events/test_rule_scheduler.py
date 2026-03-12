"""Tests for EventBridge Rule Scheduler."""

import time
from unittest.mock import patch

from robotocore.services.events.models import EventRule, EventsStore, EventTarget
from robotocore.services.events.rule_scheduler import EventBridgeRuleScheduler
from robotocore.services.synthetics.scheduler import parse_cron_minutes, parse_rate_seconds

# -- Parser tests (imported from synthetics, verify they work for our expressions) --


class TestParseRateSeconds:
    def test_rate_minutes(self):
        assert parse_rate_seconds("rate(5 minutes)") == 300

    def test_rate_single_minute(self):
        assert parse_rate_seconds("rate(1 minute)") == 60

    def test_rate_hours(self):
        assert parse_rate_seconds("rate(1 hour)") == 3600

    def test_rate_days(self):
        assert parse_rate_seconds("rate(1 day)") == 86400

    def test_rate_zero(self):
        assert parse_rate_seconds("rate(0 minutes)") is None

    def test_invalid(self):
        assert parse_rate_seconds("cron(0 * * * ? *)") is None


class TestParseCronMinutes:
    def test_every_5_minutes(self):
        assert parse_cron_minutes("cron(0/5 * * * ? *)") == 300

    def test_every_10_minutes(self):
        assert parse_cron_minutes("cron(0/10 * * * ? *)") == 600

    def test_star_step(self):
        assert parse_cron_minutes("cron(*/15 * * * ? *)") == 900

    def test_hourly(self):
        assert parse_cron_minutes("cron(0 * * * ? *)") == 3600

    def test_invalid(self):
        assert parse_cron_minutes("rate(5 minutes)") is None


# -- Scheduler logic tests --


class TestEventBridgeRuleScheduler:
    def _make_store_with_scheduled_rule(
        self,
        rule_name="test-rule",
        schedule_expr="rate(1 minute)",
        state="ENABLED",
        bus_name="default",
        target_arn="arn:aws:sqs:us-east-1:123456789012:my-queue",
    ):
        """Create a store with a scheduled rule and a target."""
        store = EventsStore()
        store.ensure_default_bus("us-east-1", "123456789012")
        bus = store.buses[bus_name]

        rule = EventRule(
            name=rule_name,
            event_bus_name=bus_name,
            region="us-east-1",
            account_id="123456789012",
            state=state,
            schedule_expression=schedule_expr,
        )
        target = EventTarget(
            target_id="target-1",
            arn=target_arn,
        )
        rule.targets["target-1"] = target
        bus.rules[rule_name] = rule

        return store

    @patch("robotocore.services.events.provider._dispatch_to_targets")
    def test_fires_scheduled_rule(self, mock_dispatch):
        """A rule with schedule_expression and targets should fire."""
        store = self._make_store_with_scheduled_rule()

        scheduler = EventBridgeRuleScheduler()

        # Patch _stores to include our test store
        with patch(
            "robotocore.services.events.provider._stores",
            {"123456789012:us-east-1": store},
        ):
            scheduler._check_all_rules()

        assert mock_dispatch.called
        call_args = mock_dispatch.call_args
        rule_arg = call_args[0][0]
        event_arg = call_args[0][1]
        assert rule_arg.name == "test-rule"
        assert event_arg["source"] == "aws.events"
        assert event_arg["detail-type"] == "Scheduled Event"
        assert event_arg["detail"] == {}
        assert event_arg["account"] == "123456789012"
        assert event_arg["region"] == "us-east-1"

    @patch("robotocore.services.events.provider._dispatch_to_targets")
    def test_disabled_rule_not_fired(self, mock_dispatch):
        """DISABLED rules should not fire."""
        store = self._make_store_with_scheduled_rule(state="DISABLED")

        scheduler = EventBridgeRuleScheduler()
        with patch(
            "robotocore.services.events.provider._stores",
            {"123456789012:us-east-1": store},
        ):
            scheduler._check_all_rules()

        assert not mock_dispatch.called

    @patch("robotocore.services.events.provider._dispatch_to_targets")
    def test_no_targets_not_fired(self, mock_dispatch):
        """Rules with no targets should not fire."""
        store = self._make_store_with_scheduled_rule()
        # Remove targets
        bus = store.buses["default"]
        bus.rules["test-rule"].targets.clear()

        scheduler = EventBridgeRuleScheduler()
        with patch(
            "robotocore.services.events.provider._stores",
            {"123456789012:us-east-1": store},
        ):
            scheduler._check_all_rules()

        assert not mock_dispatch.called

    @patch("robotocore.services.events.provider._dispatch_to_targets")
    def test_rate_based_interval_tracking(self, mock_dispatch):
        """Rule should not fire again until interval has elapsed."""
        store = self._make_store_with_scheduled_rule(schedule_expr="rate(1 minute)")

        scheduler = EventBridgeRuleScheduler()
        stores_dict = {"123456789012:us-east-1": store}

        with patch("robotocore.services.events.provider._stores", stores_dict):
            # First check — should fire
            scheduler._check_all_rules()
            assert mock_dispatch.call_count == 1

            # Second check immediately — should NOT fire (interval not elapsed)
            scheduler._check_all_rules()
            assert mock_dispatch.call_count == 1

    @patch("robotocore.services.events.provider._dispatch_to_targets")
    def test_fires_after_interval_elapsed(self, mock_dispatch):
        """Rule should fire again after the interval has elapsed."""
        store = self._make_store_with_scheduled_rule(schedule_expr="rate(1 minute)")

        scheduler = EventBridgeRuleScheduler()
        stores_dict = {"123456789012:us-east-1": store}

        with patch("robotocore.services.events.provider._stores", stores_dict):
            scheduler._check_all_rules()
            assert mock_dispatch.call_count == 1

            # Manually set last-fired to >60s ago
            key = ("123456789012:us-east-1", "default", "test-rule")
            scheduler._last_fired[key] = time.monotonic() - 61

            scheduler._check_all_rules()
            assert mock_dispatch.call_count == 2

    @patch("robotocore.services.events.provider._dispatch_to_targets")
    def test_cron_schedule_fires(self, mock_dispatch):
        """A cron-based schedule rule should also fire."""
        store = self._make_store_with_scheduled_rule(schedule_expr="cron(0/5 * * * ? *)")

        scheduler = EventBridgeRuleScheduler()
        with patch(
            "robotocore.services.events.provider._stores",
            {"123456789012:us-east-1": store},
        ):
            scheduler._check_all_rules()

        assert mock_dispatch.called

    def test_start_stop(self):
        """Scheduler can be started and stopped."""
        scheduler = EventBridgeRuleScheduler()
        assert not scheduler.is_running()
        scheduler.start()
        assert scheduler.is_running()
        scheduler.stop()
        # Give thread time to observe the flag
        time.sleep(0.1)
        assert not scheduler.is_running()

    @patch("robotocore.services.events.provider._dispatch_to_targets")
    def test_event_contains_rule_arn(self, mock_dispatch):
        """The synthetic event should contain the rule ARN in resources."""
        store = self._make_store_with_scheduled_rule()

        scheduler = EventBridgeRuleScheduler()
        with patch(
            "robotocore.services.events.provider._stores",
            {"123456789012:us-east-1": store},
        ):
            scheduler._check_all_rules()

        event = mock_dispatch.call_args[0][1]
        assert len(event["resources"]) == 1
        assert "arn:aws:events:us-east-1:123456789012:rule/test-rule" in event["resources"]
