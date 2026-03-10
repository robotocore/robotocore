"""End-to-end tests: publish message -> rule matches -> target invoked."""

import json

import pytest

from robotocore.services.iot.rule_engine import TopicRule, evaluate_message, parse_sql
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


class TestEndToEnd:
    """Simulate the full publish -> rule eval -> dispatch flow."""

    def test_publish_matches_rule_dispatches_to_s3(self):
        """Publish to sensors/room1, rule matches sensors/+, dispatches to S3."""
        rule = _make_rule(
            "store_sensor_data",
            "SELECT * FROM 'sensors/+'",
            [{"s3": {"bucketName": "iot-data", "key": "data/${topic()}.json"}}],
        )

        topic = "sensors/room1"
        payload = {"temperature": 25, "humidity": 60}

        # Evaluate rules
        matches = evaluate_message([rule], topic, payload)
        assert len(matches) == 1

        # Dispatch
        matched_rule, extracted = matches[0]
        results = dispatch_actions(
            matched_rule.actions, extracted, topic, "us-east-1", "123456789012"
        )
        assert len(results) == 1
        assert results[0]["success"] is True
        assert results[0]["key"] == "data/sensors/room1.json"
        assert json.loads(results[0]["body"]) == payload

    def test_publish_with_where_filter(self):
        """Only dispatch when temperature exceeds threshold."""
        rule = _make_rule(
            "high_temp_alert",
            "SELECT temperature, topic() as source FROM 'sensors/+' WHERE temperature > 30",
            [
                {
                    "cloudwatchMetric": {
                        "metricNamespace": "IoT/Alerts",
                        "metricName": "HighTemp",
                        "metricValue": "${temperature}",
                    }
                }
            ],
        )

        # Normal temperature - no match
        matches = evaluate_message([rule], "sensors/room1", {"temperature": 25})
        assert len(matches) == 0

        # High temperature - match
        matches = evaluate_message([rule], "sensors/room1", {"temperature": 35})
        assert len(matches) == 1

        matched_rule, extracted = matches[0]
        assert extracted["temperature"] == 35
        assert extracted["source"] == "sensors/room1"

        results = dispatch_actions(
            matched_rule.actions, extracted, "sensors/room1", "us-east-1", "123456789012"
        )
        assert results[0]["success"] is True
        assert results[0]["metricValue"] == "35"

    def test_multiple_rules_multiple_targets(self):
        """A message matches two rules, each dispatching to different targets."""
        rule_archive = _make_rule(
            "archive_all",
            "SELECT * FROM 'sensors/#'",
            [{"s3": {"bucketName": "archive", "key": "${topic()}/latest.json"}}],
        )
        rule_alert = _make_rule(
            "high_temp",
            "SELECT temperature FROM 'sensors/+' WHERE temperature > 30",
            [{"kinesis": {"streamName": "alerts", "partitionKey": "${topic()}"}}],
        )

        topic = "sensors/room1"
        payload = {"temperature": 35, "humidity": 60}

        matches = evaluate_message([rule_archive, rule_alert], topic, payload)
        assert len(matches) == 2

        # Dispatch all
        all_results = []
        for matched_rule, extracted in matches:
            results = dispatch_actions(
                matched_rule.actions, extracted, topic, "us-east-1", "123456789012"
            )
            all_results.extend(results)

        assert len(all_results) == 2
        assert all(r["success"] for r in all_results)

        # Verify dispatch log
        log = get_dispatch_log()
        action_types = [entry["action_type"] for entry in log]
        assert "s3" in action_types
        assert "kinesis" in action_types

    def test_disabled_rule_not_dispatched(self):
        """Disabled rules should not trigger dispatch."""
        rule = _make_rule(
            "disabled_rule",
            "SELECT * FROM 'sensors/+'",
            [{"s3": {"bucketName": "bucket", "key": "key"}}],
            enabled=False,
        )

        matches = evaluate_message([rule], "sensors/room1", {"temp": 25})
        assert len(matches) == 0
        assert len(get_dispatch_log()) == 0

    def test_dynamodb_v1_with_template_vars(self):
        """DynamoDB v1 action with template variable resolution."""
        rule = _make_rule(
            "store_to_ddb",
            "SELECT * FROM 'devices/+'",
            [
                {
                    "dynamoDB": {
                        "tableName": "device-readings",
                        "hashKeyField": "device_id",
                        "hashKeyValue": "${topic()}",
                        "rangeKeyField": "timestamp",
                        "rangeKeyValue": "${timestamp()}",
                        "payloadField": "data",
                    }
                }
            ],
        )

        topic = "devices/sensor-001"
        payload = {"reading": 42}

        matches = evaluate_message([rule], topic, payload)
        assert len(matches) == 1

        matched_rule, extracted = matches[0]
        results = dispatch_actions(
            matched_rule.actions, extracted, topic, "us-east-1", "123456789012"
        )
        assert results[0]["success"] is True
        assert results[0]["item"]["device_id"]["S"] == "devices/sensor-001"
        assert "data" in results[0]["item"]
        assert json.loads(results[0]["item"]["data"]["S"]) == payload

    def test_rule_with_field_selection_and_where(self):
        """Complex rule: specific fields, WHERE, and multiple targets."""
        rule = _make_rule(
            "complex_rule",
            "SELECT device.name, temperature FROM 'building/+/sensors' "
            "WHERE temperature > 28 AND device.type = 'thermostat'",
            [
                {"s3": {"bucketName": "hot-zones", "key": "${device.name}.json"}},
                {
                    "cloudwatchLogs": {
                        "logGroupName": "/iot/high-temp",
                    }
                },
            ],
        )

        topic = "building/floor1/sensors"
        payload = {
            "device": {"name": "thermostat-1", "type": "thermostat"},
            "temperature": 32,
            "humidity": 45,
        }

        matches = evaluate_message([rule], topic, payload)
        assert len(matches) == 1

        matched_rule, extracted = matches[0]
        # Only selected fields
        assert "device.name" in extracted
        assert "temperature" in extracted
        assert "humidity" not in extracted

        results = dispatch_actions(
            matched_rule.actions, extracted, topic, "us-east-1", "123456789012"
        )
        assert len(results) == 2
        assert all(r["success"] for r in results)

    def test_no_match_no_dispatch(self):
        """Publish to unrelated topic triggers nothing."""
        rule = _make_rule(
            "sensors_only",
            "SELECT * FROM 'sensors/+'",
            [{"s3": {"bucketName": "bucket", "key": "key"}}],
        )

        matches = evaluate_message([rule], "devices/status", {"online": True})
        assert len(matches) == 0
        assert len(get_dispatch_log()) == 0

    def test_error_action_on_dispatch_failure(self):
        """When a target returns failure, verify the failure is recorded."""
        rule = _make_rule(
            "with_error_action",
            "SELECT * FROM 'sensors/+'",
            actions=[{"lambda": {}}],  # Missing functionArn -> returns failure
            error_action={"s3": {"bucketName": "dlq", "key": "errors.json"}},
        )

        matches = evaluate_message([rule], "sensors/room1", {"temp": 25})
        assert len(matches) == 1

        matched_rule, extracted = matches[0]
        results = dispatch_actions(
            matched_rule.actions,
            extracted,
            "sensors/room1",
            "us-east-1",
            "123456789012",
            error_action=matched_rule.error_action,
        )
        # The primary action failed (missing functionArn)
        assert results[0]["success"] is False
        assert "functionArn" in results[0]["error"]

        # Dispatch log records the failed action
        log = get_dispatch_log()
        assert len(log) >= 1
        assert log[0]["action_type"] == "lambda"
