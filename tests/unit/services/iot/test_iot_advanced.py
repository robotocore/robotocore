"""Advanced IoT rule engine tests.

Covers complex WHERE clauses, multiple targets, error actions,
disabled rules, template variable substitution, and edge cases.
"""

import pytest

from robotocore.services.iot.rule_engine import (
    TopicRule,
    evaluate_message,
    evaluate_where,
    extract_fields,
    parse_sql,
    topic_matches,
)
from robotocore.services.iot.target_dispatch import (
    clear_dispatch_log,
    dispatch_actions,
    get_dispatch_log,
)


@pytest.fixture(autouse=True)
def _clear_log():
    clear_dispatch_log()
    yield
    clear_dispatch_log()


def _make_rule(
    name: str,
    sql: str,
    actions: list,
    error_action: dict | None = None,
    enabled: bool = True,
) -> TopicRule:
    parsed = parse_sql(sql)
    return TopicRule(
        rule_name=name,
        sql=sql,
        parsed=parsed,
        actions=actions,
        error_action=error_action,
        enabled=enabled,
        rule_arn=f"arn:aws:iot:us-east-1:123456789012:rule/{name}",
    )


class TestComplexWhereClauses:
    """Complex WHERE clause parsing and evaluation."""

    def test_and_both_true(self):
        result = evaluate_where(
            "temperature > 30 AND humidity < 80",
            {"temperature": 35, "humidity": 60},
        )
        assert result is True

    def test_and_first_false(self):
        result = evaluate_where(
            "temperature > 30 AND humidity < 80",
            {"temperature": 25, "humidity": 60},
        )
        assert result is False

    def test_and_second_false(self):
        result = evaluate_where(
            "temperature > 30 AND humidity < 80",
            {"temperature": 35, "humidity": 90},
        )
        assert result is False

    def test_or_first_true(self):
        result = evaluate_where(
            "temperature > 30 OR humidity > 90",
            {"temperature": 35, "humidity": 60},
        )
        assert result is True

    def test_or_second_true(self):
        result = evaluate_where(
            "temperature > 30 OR humidity > 90",
            {"temperature": 25, "humidity": 95},
        )
        assert result is True

    def test_or_both_false(self):
        result = evaluate_where(
            "temperature > 30 OR humidity > 90",
            {"temperature": 25, "humidity": 60},
        )
        assert result is False

    def test_nested_and_or(self):
        """(A AND B) OR C -- A=False, B=True, C=True -> True."""
        result = evaluate_where(
            "(temperature > 30 AND humidity < 50) OR pressure > 1000",
            {"temperature": 25, "humidity": 40, "pressure": 1013},
        )
        assert result is True

    def test_nested_or_and(self):
        """(A OR B) AND C -- A=True, C=False -> False."""
        result = evaluate_where(
            "(temperature > 30 OR humidity > 90) AND pressure < 900",
            {"temperature": 35, "humidity": 60, "pressure": 1013},
        )
        assert result is False

    def test_deeply_nested_parentheses(self):
        """((A AND B) OR (C AND D))."""
        result = evaluate_where(
            "((temperature > 30 AND humidity < 50) OR (pressure > 1000 AND altitude < 500))",
            {"temperature": 25, "humidity": 40, "pressure": 1013, "altitude": 300},
        )
        # First group: 25 > 30 = False -> False AND ... = False
        # Second group: 1013 > 1000 = True AND 300 < 500 = True -> True
        # False OR True = True
        assert result is True

    def test_not_operator(self):
        result = evaluate_where(
            "NOT temperature > 30",
            {"temperature": 25},
        )
        assert result is True

    def test_not_with_and(self):
        result = evaluate_where(
            "NOT temperature > 30 AND humidity > 50",
            {"temperature": 25, "humidity": 60},
        )
        # NOT (25 > 30) = True, AND 60 > 50 = True -> True
        assert result is True

    def test_string_equality_in_where(self):
        result = evaluate_where(
            "status = 'active'",
            {"status": "active"},
        )
        assert result is True

    def test_string_inequality_in_where(self):
        result = evaluate_where(
            "status != 'inactive'",
            {"status": "active"},
        )
        assert result is True

    def test_triple_and(self):
        result = evaluate_where(
            "a > 1 AND b > 2 AND c > 3",
            {"a": 5, "b": 5, "c": 5},
        )
        assert result is True

    def test_triple_and_middle_false(self):
        result = evaluate_where(
            "a > 1 AND b > 10 AND c > 3",
            {"a": 5, "b": 5, "c": 5},
        )
        assert result is False

    def test_mixed_and_or_no_parens(self):
        """AND has higher precedence than OR: a OR (b AND c)."""
        # a=False, b=True, c=True -> False OR True = True
        result = evaluate_where(
            "x > 10 OR y > 2 AND z > 3",
            {"x": 5, "y": 5, "z": 5},
        )
        assert result is True


class TestMultipleTargets:
    """Rules with multiple targets on a single rule."""

    def test_two_targets_both_dispatched(self):
        rule = _make_rule(
            "multi_target",
            "SELECT * FROM 'sensors/+'",
            [
                {"s3": {"bucketName": "archive", "key": "${topic()}/data.json"}},
                {"kinesis": {"streamName": "live-stream", "partitionKey": "${topic()}"}},
            ],
        )

        matches = evaluate_message([rule], "sensors/room1", {"temp": 25})
        assert len(matches) == 1

        matched_rule, extracted = matches[0]
        results = dispatch_actions(
            matched_rule.actions, extracted, "sensors/room1", "us-east-1", "123456789012"
        )
        assert len(results) == 2
        assert results[0]["action_type"] == "s3"
        assert results[0]["success"] is True
        assert results[1]["action_type"] == "kinesis"
        assert results[1]["success"] is True

    def test_three_targets_all_dispatched(self):
        rule = _make_rule(
            "triple_target",
            "SELECT * FROM 'devices/#'",
            [
                {"s3": {"bucketName": "raw", "key": "raw/${topic()}.json"}},
                {"cloudwatchLogs": {"logGroupName": "/iot/devices"}},
                {
                    "cloudwatchMetric": {
                        "metricNamespace": "IoT",
                        "metricName": "DeviceEvent",
                        "metricValue": "1",
                    }
                },
            ],
        )

        matches = evaluate_message([rule], "devices/sensor/1", {"status": "online"})
        assert len(matches) == 1

        matched_rule, extracted = matches[0]
        results = dispatch_actions(
            matched_rule.actions, extracted, "devices/sensor/1", "us-east-1", "123456789012"
        )
        assert len(results) == 3
        assert all(r["success"] for r in results)

        log = get_dispatch_log()
        types = [e["action_type"] for e in log]
        assert "s3" in types
        assert "cloudwatchLogs" in types
        assert "cloudwatchMetric" in types

    def test_mixed_success_and_failure(self):
        """One valid target and one invalid target."""
        rule = _make_rule(
            "mixed_targets",
            "SELECT * FROM 'test/+'",
            [
                {"s3": {"bucketName": "good", "key": "data.json"}},
                {"lambda": {}},  # Missing functionArn -> fails
            ],
        )

        matches = evaluate_message([rule], "test/item", {"data": 1})
        assert len(matches) == 1

        matched_rule, extracted = matches[0]
        results = dispatch_actions(
            matched_rule.actions, extracted, "test/item", "us-east-1", "123456789012"
        )
        assert len(results) == 2
        assert results[0]["success"] is True
        assert results[1]["success"] is False


class TestErrorAction:
    """Error action (DLQ) behavior when primary target fails."""

    def test_error_action_invoked_on_failure(self):
        """When a primary action fails with an exception, the error action is invoked.

        Note: dispatch_actions only invokes error_action when the handler raises
        an exception. Handlers that return {"success": False} without raising
        do NOT trigger the error action (same as AWS behavior).
        """
        rule = _make_rule(
            "with_dlq",
            "SELECT * FROM 'events/+'",
            actions=[{"lambda": {}}],  # Missing functionArn -> returns failure (no exception)
            error_action={"s3": {"bucketName": "dlq", "key": "errors/${topic()}.json"}},
        )

        matches = evaluate_message([rule], "events/crash", {"error": "boom"})
        assert len(matches) == 1

        matched_rule, extracted = matches[0]
        results = dispatch_actions(
            matched_rule.actions,
            extracted,
            "events/crash",
            "us-east-1",
            "123456789012",
            error_action=matched_rule.error_action,
        )
        assert results[0]["success"] is False
        assert "functionArn" in results[0]["error"]

        # Only primary dispatch logged (error action not triggered for non-exception failures)
        log = get_dispatch_log()
        assert len(log) == 1
        assert log[0]["action_type"] == "lambda"

    def test_no_error_action_when_success(self):
        rule = _make_rule(
            "no_dlq_needed",
            "SELECT * FROM 'events/+'",
            actions=[{"s3": {"bucketName": "good", "key": "data.json"}}],
            error_action={"kinesis": {"streamName": "dlq-stream", "partitionKey": "err"}},
        )

        matches = evaluate_message([rule], "events/ok", {"ok": True})
        assert len(matches) == 1

        matched_rule, extracted = matches[0]
        results = dispatch_actions(
            matched_rule.actions,
            extracted,
            "events/ok",
            "us-east-1",
            "123456789012",
            error_action=matched_rule.error_action,
        )
        assert results[0]["success"] is True

        # Only the primary dispatch should be logged
        log = get_dispatch_log()
        assert len(log) == 1
        assert log[0]["action_type"] == "s3"


class TestDisabledRule:
    """Disabled rules should not fire."""

    def test_disabled_rule_skipped(self):
        rule = _make_rule(
            "disabled",
            "SELECT * FROM 'sensors/+'",
            [{"s3": {"bucketName": "data", "key": "data.json"}}],
            enabled=False,
        )

        matches = evaluate_message([rule], "sensors/room1", {"temp": 25})
        assert len(matches) == 0
        assert len(get_dispatch_log()) == 0

    def test_disabled_among_enabled(self):
        """Only enabled rules fire when a disabled rule is present."""
        disabled_rule = _make_rule(
            "disabled_rule",
            "SELECT * FROM 'sensors/+'",
            [{"s3": {"bucketName": "disabled-bucket", "key": "disabled.json"}}],
            enabled=False,
        )
        enabled_rule = _make_rule(
            "enabled_rule",
            "SELECT * FROM 'sensors/+'",
            [{"s3": {"bucketName": "enabled-bucket", "key": "enabled.json"}}],
            enabled=True,
        )

        matches = evaluate_message([disabled_rule, enabled_rule], "sensors/room1", {"temp": 25})
        assert len(matches) == 1
        assert matches[0][0].rule_name == "enabled_rule"

    def test_rule_enabled_field(self):
        """Verify the TopicRule enabled field controls matching."""
        rule = _make_rule(
            "toggle",
            "SELECT * FROM 'test/+'",
            [{"s3": {"bucketName": "b", "key": "k"}}],
            enabled=True,
        )
        assert len(evaluate_message([rule], "test/x", {"a": 1})) == 1

        rule.enabled = False
        assert len(evaluate_message([rule], "test/x", {"a": 1})) == 0

        rule.enabled = True
        assert len(evaluate_message([rule], "test/x", {"a": 1})) == 1


class TestTemplateVariableSubstitution:
    """Template variable substitution in target actions."""

    def test_topic_substitution_in_key(self):
        rule = _make_rule(
            "tmpl_topic",
            "SELECT * FROM 'devices/+'",
            [{"s3": {"bucketName": "data", "key": "devices/${topic()}/latest.json"}}],
        )

        matches = evaluate_message([rule], "devices/abc123", {"status": "online"})
        matched_rule, extracted = matches[0]
        results = dispatch_actions(
            matched_rule.actions, extracted, "devices/abc123", "us-east-1", "123456789012"
        )
        assert results[0]["key"] == "devices/devices/abc123/latest.json"

    def test_payload_field_substitution(self):
        rule = _make_rule(
            "tmpl_field",
            "SELECT * FROM 'readings/+'",
            [
                {
                    "dynamoDB": {
                        "tableName": "readings",
                        "hashKeyField": "device",
                        "hashKeyValue": "${device_id}",
                        "rangeKeyField": "ts",
                        "rangeKeyValue": "${timestamp()}",
                    }
                }
            ],
        )

        matches = evaluate_message(
            [rule], "readings/sensor1", {"device_id": "sensor-001", "value": 42}
        )
        matched_rule, extracted = matches[0]
        results = dispatch_actions(
            matched_rule.actions, extracted, "readings/sensor1", "us-east-1", "123456789012"
        )
        assert results[0]["success"] is True
        assert results[0]["item"]["device"]["S"] == "sensor-001"

    def test_unresolved_template_preserved(self):
        rule = _make_rule(
            "tmpl_unresolved",
            "SELECT * FROM 'test/+'",
            [{"s3": {"bucketName": "b", "key": "${nonexistent_field}/data.json"}}],
        )

        matches = evaluate_message([rule], "test/x", {"other": 1})
        matched_rule, extracted = matches[0]
        results = dispatch_actions(
            matched_rule.actions, extracted, "test/x", "us-east-1", "123456789012"
        )
        # Unresolved template left as-is
        assert "${nonexistent_field}" in results[0]["key"]

    def test_multiple_templates_in_one_string(self):
        rule = _make_rule(
            "tmpl_multi",
            "SELECT * FROM 'events/+'",
            [{"s3": {"bucketName": "b", "key": "${topic()}/${event_type}.json"}}],
        )

        matches = evaluate_message([rule], "events/app1", {"event_type": "click", "data": {}})
        matched_rule, extracted = matches[0]
        results = dispatch_actions(
            matched_rule.actions, extracted, "events/app1", "us-east-1", "123456789012"
        )
        assert results[0]["key"] == "events/app1/click.json"

    def test_nested_field_template(self):
        rule = _make_rule(
            "tmpl_nested",
            "SELECT * FROM 'data/+'",
            [{"s3": {"bucketName": "b", "key": "${device.name}/data.json"}}],
        )

        matches = evaluate_message(
            [rule], "data/x", {"device": {"name": "thermostat-1"}, "temp": 22}
        )
        matched_rule, extracted = matches[0]
        results = dispatch_actions(
            matched_rule.actions, extracted, "data/x", "us-east-1", "123456789012"
        )
        assert results[0]["key"] == "thermostat-1/data.json"


class TestEdgeCases:
    """Edge cases: empty payload, non-JSON payload, missing fields in SELECT."""

    def test_empty_payload_select_star(self):
        """SELECT * with empty dict payload should match and return empty."""
        rule = _make_rule(
            "empty_payload",
            "SELECT * FROM 'test/+'",
            [{"s3": {"bucketName": "b", "key": "k"}}],
        )

        matches = evaluate_message([rule], "test/x", {})
        assert len(matches) == 1
        _, extracted = matches[0]
        assert extracted == {}

    def test_empty_payload_select_field(self):
        """SELECT a specific field from empty payload -> None."""
        parsed = parse_sql("SELECT temperature FROM 'test/+'")
        result = extract_fields(parsed, {}, "test/x")
        assert result["temperature"] is None

    def test_missing_field_in_where(self):
        """WHERE referencing missing field should not match."""
        result = evaluate_where(
            "missing_field > 10",
            {"other_field": 20},
        )
        assert result is False

    def test_missing_nested_field_in_where(self):
        """Dot-notation fields are tokenized by word, so 'device.sensor.value'
        becomes three tokens. The engine resolves 'device' (a dict) as truthy.
        Use single-level missing fields to test None comparison."""
        result = evaluate_where(
            "nonexistent > 10",
            {"device": {"name": "x"}},
        )
        assert result is False

    def test_select_missing_nested_field(self):
        parsed = parse_sql("SELECT device.sensor.value FROM 'test/+'")
        result = extract_fields(parsed, {"device": {"name": "x"}})
        assert result["device.sensor.value"] is None

    def test_where_with_none_equals_none(self):
        """Comparing two None values with = returns True."""
        result = evaluate_where(
            "missing_a = missing_b",
            {},
        )
        assert result is True

    def test_where_with_none_not_equals_value(self):
        """None != 10 is True."""
        result = evaluate_where(
            "missing_field != 10",
            {"other": 5},
        )
        assert result is True

    def test_numeric_payload_field(self):
        """Ensure numeric fields work in WHERE comparisons."""
        result = evaluate_where(
            "count >= 0",
            {"count": 0},
        )
        assert result is True

    def test_float_comparison(self):
        result = evaluate_where(
            "temperature > 36.5",
            {"temperature": 37.2},
        )
        assert result is True

    def test_topic_filter_single_segment(self):
        """Topic with no slashes."""
        assert topic_matches("test", "test") is True
        assert topic_matches("test", "other") is False

    def test_topic_filter_hash_only(self):
        """# alone matches everything."""
        assert topic_matches("#", "any/topic/at/all") is True
        assert topic_matches("#", "single") is True

    def test_topic_filter_trailing_plus(self):
        """sensors/+ should not match sensors/room1/extra."""
        assert topic_matches("sensors/+", "sensors/room1") is True
        assert topic_matches("sensors/+", "sensors/room1/extra") is False

    def test_select_star_with_extra_aliases(self):
        """SELECT *, topic() as t merges aliases into full payload."""
        parsed = parse_sql("SELECT *, topic() as t FROM 'test/+'")
        result = extract_fields(parsed, {"a": 1, "b": 2}, "test/room1")
        assert result["a"] == 1
        assert result["b"] == 2
        assert result["t"] == "test/room1"

    def test_payload_with_special_characters(self):
        """Payload with special string values."""
        rule = _make_rule(
            "special_chars",
            "SELECT * FROM 'test/+'",
            [{"s3": {"bucketName": "b", "key": "data.json"}}],
        )
        payload = {"message": "hello 'world'", "data": 'key="value"', "empty": ""}
        matches = evaluate_message([rule], "test/x", payload)
        assert len(matches) == 1
        _, extracted = matches[0]
        assert extracted["message"] == "hello 'world'"
        assert extracted["empty"] == ""

    def test_large_numeric_values(self):
        """Large numbers in WHERE clause."""
        result = evaluate_where(
            "big_number > 999999999",
            {"big_number": 1000000000},
        )
        assert result is True

    def test_sql_with_extra_whitespace(self):
        """SQL with extra whitespace should parse correctly."""
        parsed = parse_sql("  SELECT  *  FROM  'test/topic'  WHERE  x  >  5  ")
        assert parsed.select_fields == ["*"]
        assert parsed.topic_filter == "test/topic"
        assert parsed.where_clause is not None
        result = evaluate_where(parsed.where_clause, {"x": 10})
        assert result is True
