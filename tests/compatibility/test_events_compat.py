"""EventBridge compatibility tests."""

import json

import pytest
from tests.compatibility.conftest import make_client


@pytest.fixture
def events():
    return make_client("events")


class TestEventBridgeOperations:
    def test_put_rule(self, events):
        response = events.put_rule(
            Name="test-rule",
            ScheduleExpression="rate(5 minutes)",
            State="ENABLED",
        )
        assert "RuleArn" in response
        events.delete_rule(Name="test-rule")

    def test_list_rules(self, events):
        events.put_rule(Name="list-rule", ScheduleExpression="rate(1 hour)")
        response = events.list_rules()
        names = [r["Name"] for r in response["Rules"]]
        assert "list-rule" in names
        events.delete_rule(Name="list-rule")

    def test_describe_rule(self, events):
        events.put_rule(Name="desc-rule", ScheduleExpression="rate(1 day)")
        response = events.describe_rule(Name="desc-rule")
        assert response["Name"] == "desc-rule"
        events.delete_rule(Name="desc-rule")

    def test_put_events(self, events):
        response = events.put_events(
            Entries=[
                {
                    "Source": "test.source",
                    "DetailType": "TestEvent",
                    "Detail": json.dumps({"key": "value"}),
                }
            ]
        )
        assert response["FailedEntryCount"] == 0

    def test_put_and_list_targets(self, events):
        events.put_rule(Name="target-rule", ScheduleExpression="rate(1 hour)")
        events.put_targets(
            Rule="target-rule",
            Targets=[{"Id": "target-1", "Arn": "arn:aws:sqs:us-east-1:123456789012:test-queue"}],
        )
        response = events.list_targets_by_rule(Rule="target-rule")
        assert len(response["Targets"]) == 1
        events.remove_targets(Rule="target-rule", Ids=["target-1"])
        events.delete_rule(Name="target-rule")
