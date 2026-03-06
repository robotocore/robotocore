"""EventBridge compatibility tests — including cross-service target delivery."""

import json
import time
import uuid

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

    def test_event_pattern_rule(self, events):
        """Test creating a rule with an event pattern."""
        suffix = uuid.uuid4().hex[:8]
        rule_name = f"pattern-rule-{suffix}"
        pattern = {"source": ["myapp.orders"], "detail-type": ["OrderPlaced"]}
        response = events.put_rule(
            Name=rule_name,
            EventPattern=json.dumps(pattern),
        )
        assert "RuleArn" in response
        desc = events.describe_rule(Name=rule_name)
        assert json.loads(desc["EventPattern"]) == pattern
        events.delete_rule(Name=rule_name)

    def test_enable_disable_rule(self, events):
        suffix = uuid.uuid4().hex[:8]
        rule_name = f"toggle-rule-{suffix}"
        events.put_rule(Name=rule_name, ScheduleExpression="rate(1 hour)")
        events.disable_rule(Name=rule_name)
        desc = events.describe_rule(Name=rule_name)
        assert desc["State"] == "DISABLED"
        events.enable_rule(Name=rule_name)
        desc = events.describe_rule(Name=rule_name)
        assert desc["State"] == "ENABLED"
        events.delete_rule(Name=rule_name)

    def test_create_and_delete_event_bus(self, events):
        suffix = uuid.uuid4().hex[:8]
        bus_name = f"custom-bus-{suffix}"
        response = events.create_event_bus(Name=bus_name)
        assert "EventBusArn" in response
        buses = events.list_event_buses()
        names = [b["Name"] for b in buses["EventBuses"]]
        assert bus_name in names
        events.delete_event_bus(Name=bus_name)


class TestEventBridgeSQSTarget:
    """Test EventBridge → SQS cross-service delivery."""

    def test_event_delivered_to_sqs(self, events):
        """When PutEvents matches a rule with SQS target, message appears in queue."""
        sqs = make_client("sqs")
        suffix = uuid.uuid4().hex[:8]
        queue_name = f"eb-target-{suffix}"
        rule_name = f"sqs-rule-{suffix}"

        # Create target queue
        q_resp = sqs.create_queue(QueueName=queue_name)
        queue_url = q_resp["QueueUrl"]
        q_attrs = sqs.get_queue_attributes(QueueUrl=queue_url, AttributeNames=["QueueArn"])
        queue_arn = q_attrs["Attributes"]["QueueArn"]

        # Create rule matching our source
        events.put_rule(
            Name=rule_name,
            EventPattern=json.dumps({"source": ["test.delivery"]}),
        )
        events.put_targets(
            Rule=rule_name,
            Targets=[{"Id": "sqs-target", "Arn": queue_arn}],
        )

        # Put event
        events.put_events(Entries=[{
            "Source": "test.delivery",
            "DetailType": "TestDelivery",
            "Detail": json.dumps({"message": "hello-from-eventbridge"}),
        }])

        # Check SQS for the message
        time.sleep(1)
        msgs = sqs.receive_message(QueueUrl=queue_url, WaitTimeSeconds=2)
        received = msgs.get("Messages", [])
        assert len(received) >= 1, "Expected at least one message from EventBridge"
        body = json.loads(received[0]["Body"])
        assert body["source"] == "test.delivery"
        assert body["detail"]["message"] == "hello-from-eventbridge"

        # Clean up
        events.remove_targets(Rule=rule_name, Ids=["sqs-target"])
        events.delete_rule(Name=rule_name)
        sqs.delete_queue(QueueUrl=queue_url)

    def test_non_matching_event_not_delivered(self, events):
        """Events that don't match the rule pattern should not be delivered."""
        sqs = make_client("sqs")
        suffix = uuid.uuid4().hex[:8]
        queue_name = f"eb-no-match-{suffix}"
        rule_name = f"no-match-rule-{suffix}"

        q_resp = sqs.create_queue(QueueName=queue_name)
        queue_url = q_resp["QueueUrl"]
        q_attrs = sqs.get_queue_attributes(QueueUrl=queue_url, AttributeNames=["QueueArn"])
        queue_arn = q_attrs["Attributes"]["QueueArn"]

        events.put_rule(
            Name=rule_name,
            EventPattern=json.dumps({"source": ["specific.source"]}),
        )
        events.put_targets(
            Rule=rule_name,
            Targets=[{"Id": "sqs-target", "Arn": queue_arn}],
        )

        # Put event with DIFFERENT source
        events.put_events(Entries=[{
            "Source": "other.source",
            "DetailType": "TestNoMatch",
            "Detail": json.dumps({"key": "value"}),
        }])

        time.sleep(1)
        msgs = sqs.receive_message(QueueUrl=queue_url, WaitTimeSeconds=1)
        received = msgs.get("Messages", [])
        assert len(received) == 0, "Non-matching event should not be delivered"

        events.remove_targets(Rule=rule_name, Ids=["sqs-target"])
        events.delete_rule(Name=rule_name)
        sqs.delete_queue(QueueUrl=queue_url)
