"""Tests for IoT SQL rule engine -- parsing, topic matching, WHERE evaluation, field extraction."""

import time

import pytest

from robotocore.services.iot.rule_engine import (
    TopicRule,
    evaluate_message,
    evaluate_where,
    extract_fields,
    parse_sql,
    topic_matches,
)


class TestParseSql:
    """Test IoT SQL parsing."""

    def test_select_star_simple_topic(self):
        parsed = parse_sql("SELECT * FROM 'topic/test'")
        assert parsed.select_fields == ["*"]
        assert parsed.topic_filter == "topic/test"
        assert parsed.where_clause is None

    def test_select_fields(self):
        parsed = parse_sql("SELECT temperature, humidity FROM 'sensors/room1'")
        assert "temperature" in parsed.select_fields
        assert "humidity" in parsed.select_fields
        assert parsed.topic_filter == "sensors/room1"

    def test_select_with_where(self):
        parsed = parse_sql("SELECT * FROM 'sensors/+' WHERE temperature > 30")
        assert parsed.select_fields == ["*"]
        assert parsed.topic_filter == "sensors/+"
        assert parsed.where_clause == "temperature > 30"

    def test_select_with_alias(self):
        parsed = parse_sql("SELECT *, topic() as t FROM 'devices/#'")
        assert "t" in parsed.aliases
        assert parsed.aliases["t"] == "topic()"
        assert parsed.topic_filter == "devices/#"

    def test_select_with_multiple_aliases(self):
        parsed = parse_sql("SELECT temperature, topic() as t, timestamp() as ts FROM 'sensors/+'")
        assert "t" in parsed.aliases
        assert "ts" in parsed.aliases
        assert parsed.topic_filter == "sensors/+"

    def test_case_insensitive(self):
        parsed = parse_sql("select * from 'test/topic' where x > 1")
        assert parsed.select_fields == ["*"]
        assert parsed.topic_filter == "test/topic"
        assert parsed.where_clause == "x > 1"

    def test_invalid_sql_raises(self):
        with pytest.raises(ValueError, match="Invalid IoT SQL"):
            parse_sql("NOT VALID SQL")

    def test_wildcard_topic_filter(self):
        parsed = parse_sql("SELECT * FROM 'devices/+/status'")
        assert parsed.topic_filter == "devices/+/status"

    def test_multi_level_wildcard(self):
        parsed = parse_sql("SELECT * FROM 'devices/#'")
        assert parsed.topic_filter == "devices/#"


class TestTopicMatches:
    """Test MQTT topic filter matching."""

    def test_exact_match(self):
        assert topic_matches("sensors/room1/temp", "sensors/room1/temp") is True

    def test_exact_no_match(self):
        assert topic_matches("sensors/room1/temp", "sensors/room2/temp") is False

    def test_single_level_wildcard(self):
        assert topic_matches("sensors/+/temp", "sensors/room1/temp") is True
        assert topic_matches("sensors/+/temp", "sensors/room2/temp") is True

    def test_single_level_wildcard_no_match(self):
        assert topic_matches("sensors/+/temp", "sensors/room1/humidity") is False

    def test_multi_level_wildcard(self):
        assert topic_matches("sensors/#", "sensors/room1/temp") is True
        assert topic_matches("sensors/#", "sensors/room1") is True
        assert topic_matches("sensors/#", "sensors") is True

    def test_multi_level_wildcard_no_match(self):
        assert topic_matches("sensors/#", "devices/room1") is False

    def test_wildcard_at_start(self):
        assert topic_matches("+/temp", "room1/temp") is True
        assert topic_matches("+/temp", "room2/temp") is True

    def test_all_wildcard(self):
        assert topic_matches("#", "anything/at/all") is True
        assert topic_matches("#", "single") is True

    def test_multiple_single_wildcards(self):
        assert topic_matches("+/+/temp", "building/room1/temp") is True
        assert topic_matches("+/+/temp", "building/room1/humidity") is False

    def test_different_depth_no_match(self):
        assert topic_matches("a/b", "a/b/c") is False
        assert topic_matches("a/b/c", "a/b") is False


class TestEvaluateWhere:
    """Test WHERE clause evaluation."""

    def test_no_where_clause(self):
        assert evaluate_where(None, {"temp": 25}) is True
        assert evaluate_where("", {"temp": 25}) is True

    def test_greater_than(self):
        assert evaluate_where("temperature > 30", {"temperature": 35}) is True
        assert evaluate_where("temperature > 30", {"temperature": 25}) is False

    def test_less_than(self):
        assert evaluate_where("temperature < 30", {"temperature": 25}) is True

    def test_equals(self):
        assert evaluate_where("status = 'active'", {"status": "active"}) is True
        assert evaluate_where("status = 'active'", {"status": "inactive"}) is False

    def test_not_equals(self):
        assert evaluate_where("status != 'error'", {"status": "ok"}) is True
        assert evaluate_where("status != 'error'", {"status": "error"}) is False

    def test_greater_than_or_equal(self):
        assert evaluate_where("temp >= 30", {"temp": 30}) is True
        assert evaluate_where("temp >= 30", {"temp": 29}) is False

    def test_less_than_or_equal(self):
        assert evaluate_where("temp <= 30", {"temp": 30}) is True
        assert evaluate_where("temp <= 30", {"temp": 31}) is False

    def test_and_operator(self):
        payload = {"temp": 35, "humidity": 80}
        assert evaluate_where("temp > 30 AND humidity > 70", payload) is True
        assert evaluate_where("temp > 30 AND humidity > 90", payload) is False

    def test_or_operator(self):
        payload = {"temp": 35, "humidity": 50}
        assert evaluate_where("temp > 40 OR humidity < 60", payload) is True
        assert evaluate_where("temp > 40 OR humidity > 60", payload) is False

    def test_not_operator(self):
        assert evaluate_where("NOT status = 'error'", {"status": "ok"}) is True
        assert evaluate_where("NOT status = 'ok'", {"status": "ok"}) is False

    def test_parentheses(self):
        payload = {"a": 1, "b": 2, "c": 3}
        assert evaluate_where("(a = 1 OR b = 5) AND c = 3", payload) is True
        assert evaluate_where("(a = 5 OR b = 5) AND c = 3", payload) is False

    def test_nested_field(self):
        payload = {"device": {"temp": 35}}
        assert evaluate_where("device.temp > 30", payload) is True

    def test_missing_field_returns_false(self):
        assert evaluate_where("missing > 30", {"temp": 35}) is False

    def test_topic_function_in_where(self):
        result = evaluate_where("topic() = 'sensors/room1'", {}, topic="sensors/room1")
        assert result is True

    def test_numeric_string_comparison(self):
        assert evaluate_where("count > 5", {"count": 10}) is True


class TestExtractFields:
    """Test SELECT field extraction."""

    def test_select_star(self):
        parsed = parse_sql("SELECT * FROM 'test'")
        payload = {"temp": 25, "humidity": 60}
        result = extract_fields(parsed, payload)
        assert result == payload

    def test_select_specific_fields(self):
        parsed = parse_sql("SELECT temperature, humidity FROM 'test'")
        payload = {"temperature": 25, "humidity": 60, "pressure": 1013}
        result = extract_fields(parsed, payload)
        assert result == {"temperature": 25, "humidity": 60}

    def test_select_star_with_function_alias(self):
        parsed = parse_sql("SELECT *, topic() as t FROM 'test'")
        payload = {"temp": 25}
        result = extract_fields(parsed, payload, topic="sensors/room1")
        assert result["temp"] == 25
        assert result["t"] == "sensors/room1"

    def test_select_timestamp_alias(self):
        parsed = parse_sql("SELECT timestamp() as ts FROM 'test'")
        before = int(time.time() * 1000)
        result = extract_fields(parsed, {})
        after = int(time.time() * 1000)
        assert before <= result["ts"] <= after

    def test_select_nested_field(self):
        parsed = parse_sql("SELECT device.temp FROM 'test'")
        payload = {"device": {"temp": 25, "id": "abc"}}
        result = extract_fields(parsed, payload)
        assert result["device.temp"] == 25

    def test_select_missing_field_returns_none(self):
        parsed = parse_sql("SELECT missing FROM 'test'")
        result = extract_fields(parsed, {"temp": 25})
        assert result["missing"] is None

    def test_select_clientid_alias(self):
        parsed = parse_sql("SELECT clientid() as cid FROM 'test'")
        result = extract_fields(parsed, {}, client_id="device-123")
        assert result["cid"] == "device-123"


class TestEvaluateMessage:
    """Test full rule evaluation against messages."""

    def _make_rule(self, sql: str, actions: list | None = None, enabled: bool = True) -> TopicRule:
        parsed = parse_sql(sql)
        return TopicRule(
            rule_name="test_rule",
            sql=sql,
            parsed=parsed,
            actions=actions or [],
            enabled=enabled,
        )

    def test_simple_match(self):
        rule = self._make_rule("SELECT * FROM 'sensors/room1'")
        matches = evaluate_message([rule], "sensors/room1", {"temp": 25})
        assert len(matches) == 1
        assert matches[0][1] == {"temp": 25}

    def test_no_match_different_topic(self):
        rule = self._make_rule("SELECT * FROM 'sensors/room1'")
        matches = evaluate_message([rule], "sensors/room2", {"temp": 25})
        assert len(matches) == 0

    def test_wildcard_match(self):
        rule = self._make_rule("SELECT * FROM 'sensors/+'")
        matches = evaluate_message([rule], "sensors/room1", {"temp": 25})
        assert len(matches) == 1

    def test_where_filters(self):
        rule = self._make_rule("SELECT * FROM 'sensors/+' WHERE temperature > 30")
        matches_hot = evaluate_message([rule], "sensors/room1", {"temperature": 35})
        assert len(matches_hot) == 1
        matches_cold = evaluate_message([rule], "sensors/room1", {"temperature": 25})
        assert len(matches_cold) == 0

    def test_disabled_rule_skipped(self):
        rule = self._make_rule("SELECT * FROM 'sensors/+'", enabled=False)
        matches = evaluate_message([rule], "sensors/room1", {"temp": 25})
        assert len(matches) == 0

    def test_multiple_rules(self):
        rule1 = self._make_rule("SELECT * FROM 'sensors/+'")
        rule2 = self._make_rule("SELECT temperature FROM 'sensors/+' WHERE temperature > 30")
        payload = {"temperature": 35, "humidity": 60}
        matches = evaluate_message([rule1, rule2], "sensors/room1", payload)
        assert len(matches) == 2
        # First match returns full payload, second returns only temperature
        assert matches[0][1] == payload
        assert matches[1][1] == {"temperature": 35}

    def test_field_extraction_on_match(self):
        rule = self._make_rule("SELECT temperature, topic() as src FROM 'sensors/+'")
        matches = evaluate_message([rule], "sensors/room1", {"temperature": 25, "humidity": 60})
        assert len(matches) == 1
        extracted = matches[0][1]
        assert extracted["temperature"] == 25
        assert extracted["src"] == "sensors/room1"
        assert "humidity" not in extracted
