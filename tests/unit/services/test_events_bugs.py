"""Failing tests for EventBridge provider bugs.

Each test documents a specific bug and is expected to FAIL against the current code.
Do NOT fix the bugs — only document them with failing tests.
"""

import json

from robotocore.services.events.models import (
    EventRule,
    EventsStore,
    _match_value_list,
)
from robotocore.services.events.provider import (
    _apply_input_transformer,
    _put_rule,
    _resolve_jsonpath,
    _start_replay,
)

# ─── Bug 1: Schedule-based rules should NOT match PutEvents events ───────────
# A rule with only a schedule_expression (no event_pattern) should never match
# events delivered via PutEvents. Schedule rules fire on a cron/rate schedule,
# not on event content. The current code returns True from matches_event() when
# schedule_expression is set and event_pattern is None.


class TestScheduleRuleShouldNotMatchEvents:
    def test_schedule_rule_does_not_match_any_event(self):
        rule = EventRule(
            name="every-5-min",
            event_bus_name="default",
            region="us-east-1",
            account_id="123456789012",
            state="ENABLED",
            schedule_expression="rate(5 minutes)",
            event_pattern=None,
        )
        event = {
            "source": "aws.ec2",
            "detail-type": "EC2 Instance State-change Notification",
            "detail": {"state": "running"},
        }
        # Schedule rules should NOT match events — they are time-triggered only
        assert rule.matches_event(event) is False


# ─── Bug 2: anything-but with prefix not supported ──────────────────────────
# AWS EventBridge supports {"anything-but": {"prefix": "foo"}} which excludes
# events where the value starts with "foo". The current code only handles
# list and scalar anything-but, not the dict form with prefix/suffix.


class TestAnythingButWithPrefix:
    def test_anything_but_prefix_excludes_matching(self):
        """anything-but with prefix should exclude values starting with the prefix."""
        pattern_values = [{"anything-but": {"prefix": "prod"}}]
        # "production" starts with "prod" — should NOT match
        assert _match_value_list(pattern_values, "production") is False

    def test_anything_but_prefix_includes_non_matching(self):
        """anything-but with prefix should include values NOT starting with the prefix."""
        pattern_values = [{"anything-but": {"prefix": "prod"}}]
        # "staging" does not start with "prod" — should match
        assert _match_value_list(pattern_values, "staging") is True

    def test_anything_but_suffix_excludes_matching(self):
        """anything-but with suffix should exclude values ending with the suffix."""
        pattern_values = [{"anything-but": {"suffix": ".txt"}}]
        # "readme.txt" ends with ".txt" — should NOT match
        assert _match_value_list(pattern_values, "readme.txt") is False

    def test_anything_but_suffix_includes_non_matching(self):
        """anything-but with suffix should include values NOT ending with the suffix."""
        pattern_values = [{"anything-but": {"suffix": ".txt"}}]
        # "readme.md" does not end with ".txt" — should match
        assert _match_value_list(pattern_values, "readme.md") is True


# ─── Bug 3: InputTransformer doesn't quote strings in JSON templates ─────────
# Per AWS docs: when InputTemplate is valid JSON and a resolved value is a
# string, it must be quoted. E.g., template '{"instance": <instance>}' with
# instance="i-123" should produce '{"instance": "i-123"}', not
# '{"instance": i-123}' (which is invalid JSON).


class TestInputTransformerStringQuoting:
    def test_string_value_is_quoted_in_json_template(self):
        """Resolved string values should be JSON-quoted when template is JSON."""
        transformer = {
            "InputPathsMap": {"instance": "$.detail.instance-id"},
            "InputTemplate": '{"instance": <instance>}',
        }
        event = {
            "detail": {"instance-id": "i-1234567890abcdef0"},
        }
        result = _apply_input_transformer(transformer, event)
        # The result should be valid JSON with the string value quoted
        parsed = json.loads(result)
        assert parsed["instance"] == "i-1234567890abcdef0"

    def test_numeric_value_is_not_quoted_in_json_template(self):
        """Resolved numeric values should NOT be quoted in JSON templates."""
        transformer = {
            "InputPathsMap": {"count": "$.detail.count"},
            "InputTemplate": '{"count": <count>}',
        }
        event = {
            "detail": {"count": 42},
        }
        result = _apply_input_transformer(transformer, event)
        parsed = json.loads(result)
        assert parsed["count"] == 42


# ─── Bug 4: _resolve_jsonpath returns "" for missing paths, should be "null" ─
# When a JSONPath references a field that doesn't exist in the event, AWS
# substitutes the literal string "null" (without quotes). The current code
# returns an empty string "".


class TestResolveJsonPathMissingField:
    def test_missing_field_returns_null_string(self):
        """Missing JSONPath fields should resolve to 'null', not empty string."""
        event = {"detail": {"name": "test"}}
        result = _resolve_jsonpath("$.detail.nonexistent", event)
        assert result == "null"

    def test_missing_nested_field_returns_null_string(self):
        """Missing nested JSONPath fields should resolve to 'null'."""
        event = {"detail": {}}
        result = _resolve_jsonpath("$.detail.level1.level2", event)
        assert result == "null"


# ─── Bug 5: PutRule silently falls back to default bus ───────────────────────
# When EventBusName references a non-existent bus, PutRule should raise
# ResourceNotFoundException. Instead, it silently places the rule on the
# default bus.


class TestPutRuleNonExistentBus:
    def test_put_rule_on_nonexistent_bus_raises_error(self):
        """PutRule on a non-existent event bus should raise ResourceNotFoundException."""
        store = EventsStore()
        store.ensure_default_bus("us-east-1", "123456789012")
        # "custom-bus" doesn't exist
        try:
            _put_rule(
                store,
                {
                    "Name": "my-rule",
                    "EventBusName": "custom-bus-that-does-not-exist",
                    "EventPattern": '{"source": ["aws.ec2"]}',
                },
                "us-east-1",
                "123456789012",
            )
        except Exception:
            return  # Good — an error was raised

        # If we get here, the rule was silently put on the wrong bus.
        # Verify it ended up on default (proving the bug):
        rule = store.get_rule("my-rule", "default")
        # The rule should NOT be on the default bus — it should have failed
        assert rule is None, (
            "PutRule silently placed rule on default bus instead of raising "
            "ResourceNotFoundException for non-existent bus"
        )


# ─── Bug 6: Replay doesn't filter events by time range ──────────────────────
# StartReplay accepts EventStartTime and EventEndTime but the implementation
# replays ALL archived events regardless of their timestamps. AWS only replays
# events within the specified time window.


class TestReplayTimeFiltering:
    def test_replay_only_includes_events_in_time_range(self):
        """Replay should only replay events within the start/end time window."""
        store = EventsStore()
        store.ensure_default_bus("us-east-1", "123456789012")

        bus = store.get_bus("default")
        bus_arn = bus.arn

        # Create archive
        archive = store.create_archive(
            "test-archive",
            bus_arn,
            "us-east-1",
            "123456789012",
        )

        # Add events with different timestamps
        old_event = {
            "source": "test",
            "detail-type": "Test",
            "detail": {"seq": "old"},
            "time": "2020-01-01T00:00:00Z",
        }
        recent_event = {
            "source": "test",
            "detail-type": "Test",
            "detail": {"seq": "recent"},
            "time": "2025-06-15T00:00:00Z",
        }

        archive.events.append(old_event)
        archive.events.append(recent_event)
        archive.event_count = 2

        # Create a rule that matches everything, with a target we can track
        store.put_rule(
            "catch-all",
            "default",
            "us-east-1",
            "123456789012",
            event_pattern={"source": ["test"]},
        )

        # Replay only events from 2025 onward
        # EventStartTime = 2025-01-01, EventEndTime = 2026-01-01
        start_time = 1735689600  # 2025-01-01
        end_time = 1767225600  # 2026-01-01

        _start_replay(
            store,
            {
                "ReplayName": "test-replay",
                "EventSourceArn": archive.arn,
                "Destination": {"Arn": bus_arn},
                "EventStartTime": start_time,
                "EventEndTime": end_time,
            },
            "us-east-1",
            "123456789012",
        )

        replay = store.get_replay("test-replay")
        # Should have replayed only 1 event (the recent one), not both
        assert replay.events_replayed == 1, (
            f"Expected 1 event replayed (only recent), got {replay.events_replayed}. "
            "Replay does not filter by time range."
        )


# ─── Bug 7: anything-but doesn't work with list event values ────────────────
# When an event field is a list (e.g., resources: ["arn:...", "arn:..."]),
# anything-but should check each element in the list. Currently, the code
# compares the entire list object against the excluded values.


class TestAnythingButWithListEventValue:
    def test_anything_but_with_list_event_value(self):
        """anything-but should check individual elements when event value is a list."""
        # Event value is a list; none of the elements are in the excluded set
        pattern_values = [{"anything-but": ["blocked"]}]
        event_value = ["allowed1", "allowed2"]
        # Should match because no element in the list equals "blocked"
        assert _match_value_list(pattern_values, event_value) is True

    def test_anything_but_excludes_list_containing_blocked_value(self):
        """anything-but should reject list event values containing an excluded element."""
        pattern_values = [{"anything-but": ["blocked"]}]
        event_value = ["allowed", "blocked"]
        # Should NOT match because "blocked" is in the event value list
        assert _match_value_list(pattern_values, event_value) is False


# ─── Bug 8: Event pattern matching with prefix on list event values ──────────
# When the event value is a list and pattern uses prefix matching, the prefix
# should be checked against each element in the list. Currently, prefix only
# checks isinstance(event_value, str), which fails for list values.


class TestPrefixMatchOnListEventValue:
    def test_prefix_matches_element_in_list(self):
        """Prefix matching should work when event value is a list of strings."""
        pattern_values = [{"prefix": "prod"}]
        event_value = ["staging-app", "production-app"]
        # "production-app" starts with "prod", so this should match
        assert _match_value_list(pattern_values, event_value) is True

    def test_prefix_no_match_in_list(self):
        """Prefix matching should fail when no list element matches."""
        pattern_values = [{"prefix": "prod"}]
        event_value = ["staging-app", "dev-app"]
        assert _match_value_list(pattern_values, event_value) is False


# ─── Bug 9: suffix match on list event values ───────────────────────────────


class TestSuffixMatchOnListEventValue:
    def test_suffix_matches_element_in_list(self):
        """Suffix matching should work when event value is a list of strings."""
        pattern_values = [{"suffix": ".json"}]
        event_value = ["data.csv", "config.json"]
        assert _match_value_list(pattern_values, event_value) is True

    def test_suffix_no_match_in_list(self):
        """Suffix matching should fail when no list element matches."""
        pattern_values = [{"suffix": ".json"}]
        event_value = ["data.csv", "config.yaml"]
        assert _match_value_list(pattern_values, event_value) is False


# ─── Bug 10: Archive event stores mutable reference ─────────────────────────
# archive_event() stores the same dict reference. If the event is later
# modified, the archived copy changes too. Should store a deep copy.


class TestArchiveStoresDeepCopy:
    def test_archived_event_is_independent_copy(self):
        """Archived events should be independent copies, not references."""
        store = EventsStore()
        store.ensure_default_bus("us-east-1", "123456789012")
        bus = store.get_bus("default")

        archive = store.create_archive(
            "test-archive",
            bus.arn,
            "us-east-1",
            "123456789012",
        )

        event = {
            "source": "test",
            "detail-type": "Test",
            "detail": {"key": "original"},
        }

        store.archive_event(event, "default")

        # Mutate the original event after archiving
        event["detail"]["key"] = "mutated"

        # The archived copy should still have the original value
        assert archive.events[0]["detail"]["key"] == "original", (
            "Archive stores a reference to the event dict, not a deep copy. "
            "Mutating the original event corrupts the archive."
        )


# ─── Bug 12: EventRule.arn doesn't include event bus name for custom buses ───
# For rules on custom event buses, the ARN should be:
#   arn:aws:events:{region}:{account}:rule/{bus-name}/{rule-name}
# But the current code always generates:
#   arn:aws:events:{region}:{account}:rule/{rule-name}


class TestRuleArnIncludesBusName:
    def test_rule_on_custom_bus_includes_bus_in_arn(self):
        """Rules on custom buses should have the bus name in the ARN."""
        rule = EventRule(
            name="my-rule",
            event_bus_name="custom-bus",
            region="us-east-1",
            account_id="123456789012",
        )
        expected = "arn:aws:events:us-east-1:123456789012:rule/custom-bus/my-rule"
        assert rule.arn == expected, (
            f"Rule ARN on custom bus should be '{expected}' but got '{rule.arn}'. "
            "Custom bus rules need the bus name in the ARN path."
        )

    def test_rule_on_default_bus_has_simple_arn(self):
        """Rules on default bus should have simple ARN without bus name."""
        rule = EventRule(
            name="my-rule",
            event_bus_name="default",
            region="us-east-1",
            account_id="123456789012",
        )
        expected = "arn:aws:events:us-east-1:123456789012:rule/my-rule"
        assert rule.arn == expected
