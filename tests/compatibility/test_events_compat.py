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

    def test_put_events_multiple_entries(self, events):
        """Put multiple events in a single call."""
        response = events.put_events(
            Entries=[
                {
                    "Source": "test.multi",
                    "DetailType": "Event1",
                    "Detail": json.dumps({"index": 1}),
                },
                {
                    "Source": "test.multi",
                    "DetailType": "Event2",
                    "Detail": json.dumps({"index": 2}),
                },
                {
                    "Source": "test.multi",
                    "DetailType": "Event3",
                    "Detail": json.dumps({"index": 3}),
                },
            ]
        )
        assert response["FailedEntryCount"] == 0
        assert len(response["Entries"]) == 3

    def test_describe_event_bus_default(self, events):
        """Describe the default event bus."""
        response = events.describe_event_bus(Name="default")
        assert response["Name"] == "default"
        assert "Arn" in response

    def test_describe_custom_event_bus(self, events):
        """Create and describe a custom event bus."""
        suffix = uuid.uuid4().hex[:8]
        bus_name = f"describe-bus-{suffix}"
        events.create_event_bus(Name=bus_name)
        response = events.describe_event_bus(Name=bus_name)
        assert response["Name"] == bus_name
        assert "Arn" in response
        events.delete_event_bus(Name=bus_name)

    def test_put_rule_with_tags(self, events):
        """Create a rule with tags inline."""
        suffix = uuid.uuid4().hex[:8]
        rule_name = f"tagged-rule-{suffix}"
        resp = events.put_rule(
            Name=rule_name,
            ScheduleExpression="rate(1 hour)",
            Tags=[
                {"Key": "env", "Value": "test"},
                {"Key": "team", "Value": "platform"},
            ],
        )
        assert "RuleArn" in resp
        desc = events.describe_rule(Name=rule_name)
        assert desc["Name"] == rule_name
        assert desc["ScheduleExpression"] == "rate(1 hour)"
        events.delete_rule(Name=rule_name)

    def test_list_targets_by_rule_multiple(self, events):
        """Put multiple targets on a rule and list them."""
        suffix = uuid.uuid4().hex[:8]
        rule_name = f"multi-target-{suffix}"
        events.put_rule(Name=rule_name, ScheduleExpression="rate(1 hour)")
        events.put_targets(
            Rule=rule_name,
            Targets=[
                {"Id": "t1", "Arn": "arn:aws:sqs:us-east-1:123456789012:queue-a"},
                {"Id": "t2", "Arn": "arn:aws:sqs:us-east-1:123456789012:queue-b"},
            ],
        )
        response = events.list_targets_by_rule(Rule=rule_name)
        ids = [t["Id"] for t in response["Targets"]]
        assert "t1" in ids
        assert "t2" in ids
        assert len(response["Targets"]) == 2
        events.remove_targets(Rule=rule_name, Ids=["t1", "t2"])
        events.delete_rule(Name=rule_name)


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
        events.put_events(
            Entries=[
                {
                    "Source": "test.delivery",
                    "DetailType": "TestDelivery",
                    "Detail": json.dumps({"message": "hello-from-eventbridge"}),
                }
            ]
        )

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
        events.put_events(
            Entries=[
                {
                    "Source": "other.source",
                    "DetailType": "TestNoMatch",
                    "Detail": json.dumps({"key": "value"}),
                }
            ]
        )

        time.sleep(1)
        msgs = sqs.receive_message(QueueUrl=queue_url, WaitTimeSeconds=1)
        received = msgs.get("Messages", [])
        assert len(received) == 0, "Non-matching event should not be delivered"

        events.remove_targets(Rule=rule_name, Ids=["sqs-target"])
        events.delete_rule(Name=rule_name)
        sqs.delete_queue(QueueUrl=queue_url)


class TestEventBridgeExtended:
    """Extended EventBridge tests for rules, buses, targets, and archives."""

    def test_list_rules_with_name_prefix(self, events):
        """ListRules with NamePrefix filters correctly."""
        suffix = uuid.uuid4().hex[:8]
        names = [f"pfx-{suffix}-alpha", f"pfx-{suffix}-beta", f"other-{suffix}"]
        for n in names:
            events.put_rule(Name=n, ScheduleExpression="rate(1 hour)")

        resp = events.list_rules(NamePrefix=f"pfx-{suffix}")
        matched = [r["Name"] for r in resp["Rules"]]
        assert f"pfx-{suffix}-alpha" in matched
        assert f"pfx-{suffix}-beta" in matched
        assert f"other-{suffix}" not in matched

        for n in names:
            events.delete_rule(Name=n)

    def test_describe_rule_details(self, events):
        """DescribeRule returns all expected fields."""
        suffix = uuid.uuid4().hex[:8]
        rule_name = f"detail-rule-{suffix}"
        events.put_rule(
            Name=rule_name,
            ScheduleExpression="rate(10 minutes)",
            State="ENABLED",
            Description="A test rule for describe",
        )
        desc = events.describe_rule(Name=rule_name)
        assert desc["Name"] == rule_name
        assert desc["State"] == "ENABLED"
        assert desc["Description"] == "A test rule for describe"
        assert desc["ScheduleExpression"] == "rate(10 minutes)"
        assert "Arn" in desc
        events.delete_rule(Name=rule_name)

    def test_enable_disable_rule_roundtrip(self, events):
        """Enable and disable a rule, verify state each time."""
        suffix = uuid.uuid4().hex[:8]
        rule_name = f"toggle2-{suffix}"
        events.put_rule(Name=rule_name, ScheduleExpression="rate(1 hour)", State="ENABLED")

        # Disable
        events.disable_rule(Name=rule_name)
        assert events.describe_rule(Name=rule_name)["State"] == "DISABLED"

        # Re-enable
        events.enable_rule(Name=rule_name)
        assert events.describe_rule(Name=rule_name)["State"] == "ENABLED"

        # Disable again
        events.disable_rule(Name=rule_name)
        assert events.describe_rule(Name=rule_name)["State"] == "DISABLED"

        events.delete_rule(Name=rule_name)

    def test_tag_untag_resource(self, events):
        """TagResource and UntagResource calls succeed (stub)."""
        suffix = uuid.uuid4().hex[:8]
        rule_name = f"tag-rule-{suffix}"
        resp = events.put_rule(Name=rule_name, ScheduleExpression="rate(1 hour)")
        rule_arn = resp["RuleArn"]

        # Tag
        events.tag_resource(
            ResourceARN=rule_arn,
            Tags=[{"Key": "env", "Value": "test"}, {"Key": "team", "Value": "core"}],
        )

        # List tags
        tag_resp = events.list_tags_for_resource(ResourceARN=rule_arn)
        assert "Tags" in tag_resp

        # Untag
        events.untag_resource(ResourceARN=rule_arn, TagKeys=["env"])

        events.delete_rule(Name=rule_name)

    def test_put_targets_with_input_transformer(self, events):
        """PutTargets with InputTransformer stores and returns transformer config."""
        suffix = uuid.uuid4().hex[:8]
        rule_name = f"xform-rule-{suffix}"
        events.put_rule(Name=rule_name, ScheduleExpression="rate(1 hour)")

        events.put_targets(
            Rule=rule_name,
            Targets=[
                {
                    "Id": "xform-t1",
                    "Arn": "arn:aws:sqs:us-east-1:123456789012:q1",
                    "InputTransformer": {
                        "InputPathsMap": {"source": "$.source", "detail": "$.detail"},
                        "InputTemplate": '"Source=<source>, Detail=<detail>"',
                    },
                }
            ],
        )

        targets = events.list_targets_by_rule(Rule=rule_name)["Targets"]
        assert len(targets) == 1
        t = targets[0]
        assert "InputTransformer" in t
        assert t["InputTransformer"]["InputPathsMap"]["source"] == "$.source"

        events.remove_targets(Rule=rule_name, Ids=["xform-t1"])
        events.delete_rule(Name=rule_name)

    def test_remove_targets(self, events):
        """RemoveTargets removes specific targets, leaves others."""
        suffix = uuid.uuid4().hex[:8]
        rule_name = f"rm-target-{suffix}"
        events.put_rule(Name=rule_name, ScheduleExpression="rate(1 hour)")
        events.put_targets(
            Rule=rule_name,
            Targets=[
                {"Id": "keep", "Arn": "arn:aws:sqs:us-east-1:123456789012:keep-q"},
                {"Id": "remove", "Arn": "arn:aws:sqs:us-east-1:123456789012:rm-q"},
            ],
        )
        assert len(events.list_targets_by_rule(Rule=rule_name)["Targets"]) == 2

        events.remove_targets(Rule=rule_name, Ids=["remove"])
        remaining = events.list_targets_by_rule(Rule=rule_name)["Targets"]
        assert len(remaining) == 1
        assert remaining[0]["Id"] == "keep"

        events.remove_targets(Rule=rule_name, Ids=["keep"])
        events.delete_rule(Name=rule_name)

    def test_create_describe_delete_event_bus(self, events):
        """Full lifecycle of a custom event bus."""
        suffix = uuid.uuid4().hex[:8]
        bus_name = f"lifecycle-bus-{suffix}"

        create_resp = events.create_event_bus(Name=bus_name)
        assert "EventBusArn" in create_resp

        desc = events.describe_event_bus(Name=bus_name)
        assert desc["Name"] == bus_name
        assert "Arn" in desc

        events.delete_event_bus(Name=bus_name)

        # Verify it's gone from list
        buses = events.list_event_buses()
        names = [b["Name"] for b in buses["EventBuses"]]
        assert bus_name not in names

    def test_put_events_custom_detail(self, events):
        """PutEvents with complex detail payload."""
        resp = events.put_events(
            Entries=[
                {
                    "Source": "myapp.orders",
                    "DetailType": "OrderPlaced",
                    "Detail": json.dumps({
                        "orderId": "12345",
                        "items": [{"sku": "A1", "qty": 2}],
                        "total": 49.99,
                    }),
                }
            ]
        )
        assert resp["FailedEntryCount"] == 0
        assert len(resp["Entries"]) == 1
        assert "EventId" in resp["Entries"][0]

    def test_create_describe_delete_archive(self, events):
        """Full archive lifecycle: create, describe, delete."""
        suffix = uuid.uuid4().hex[:8]
        archive_name = f"test-archive-{suffix}"
        bus_arn = events.describe_event_bus(Name="default")["Arn"]

        create_resp = events.create_archive(
            ArchiveName=archive_name,
            EventSourceArn=bus_arn,
            Description="Test archive",
            RetentionDays=7,
        )
        assert "ArchiveArn" in create_resp
        assert create_resp["State"] == "ENABLED"

        desc = events.describe_archive(ArchiveName=archive_name)
        assert desc["ArchiveName"] == archive_name
        assert desc["Description"] == "Test archive"
        assert desc["RetentionDays"] == 7
        assert desc["EventSourceArn"] == bus_arn

        events.delete_archive(ArchiveName=archive_name)

    def test_list_event_buses(self, events):
        """ListEventBuses returns default plus custom buses."""
        suffix = uuid.uuid4().hex[:8]
        bus_name = f"list-bus-{suffix}"
        events.create_event_bus(Name=bus_name)

        resp = events.list_event_buses()
        names = [b["Name"] for b in resp["EventBuses"]]
        assert "default" in names
        assert bus_name in names

        events.delete_event_bus(Name=bus_name)

    def test_rule_with_rate_expression(self, events):
        """Rule with rate schedule expression."""
        suffix = uuid.uuid4().hex[:8]
        rule_name = f"rate-rule-{suffix}"
        events.put_rule(Name=rule_name, ScheduleExpression="rate(5 minutes)")
        desc = events.describe_rule(Name=rule_name)
        assert desc["ScheduleExpression"] == "rate(5 minutes)"
        events.delete_rule(Name=rule_name)

    def test_rule_with_cron_expression(self, events):
        """Rule with cron schedule expression."""
        suffix = uuid.uuid4().hex[:8]
        rule_name = f"cron-rule-{suffix}"
        events.put_rule(Name=rule_name, ScheduleExpression="cron(0 12 * * ? *)")
        desc = events.describe_rule(Name=rule_name)
        assert desc["ScheduleExpression"] == "cron(0 12 * * ? *)"
        events.delete_rule(Name=rule_name)

    def test_multiple_targets_per_rule(self, events):
        """Add 3 targets to a single rule and verify all are listed."""
        suffix = uuid.uuid4().hex[:8]
        rule_name = f"multi3-{suffix}"
        events.put_rule(Name=rule_name, ScheduleExpression="rate(1 hour)")
        events.put_targets(
            Rule=rule_name,
            Targets=[
                {"Id": "t-a", "Arn": "arn:aws:sqs:us-east-1:123456789012:queue-a"},
                {"Id": "t-b", "Arn": "arn:aws:sqs:us-east-1:123456789012:queue-b"},
                {"Id": "t-c", "Arn": "arn:aws:sqs:us-east-1:123456789012:queue-c"},
            ],
        )
        targets = events.list_targets_by_rule(Rule=rule_name)["Targets"]
        ids = {t["Id"] for t in targets}
        assert ids == {"t-a", "t-b", "t-c"}

        events.remove_targets(Rule=rule_name, Ids=["t-a", "t-b", "t-c"])
        events.delete_rule(Name=rule_name)

    def test_put_events_with_resources(self, events):
        """PutEvents with Resources field."""
        resp = events.put_events(
            Entries=[
                {
                    "Source": "aws.ec2",
                    "DetailType": "EC2 Instance State-change Notification",
                    "Detail": json.dumps({"instance-id": "i-1234567890abcdef0", "state": "running"}),
                    "Resources": ["arn:aws:ec2:us-east-1:123456789012:instance/i-1234567890abcdef0"],
                }
            ]
        )
        assert resp["FailedEntryCount"] == 0

    def test_describe_rule_not_found(self, events):
        """DescribeRule for nonexistent rule raises error."""
        import botocore.exceptions

        with pytest.raises(botocore.exceptions.ClientError) as exc_info:
            events.describe_rule(Name="nonexistent-rule-xyz")
        assert "ResourceNotFoundException" in str(exc_info.value)

    def test_put_rule_overwrites_existing(self, events):
        """PutRule on same name updates the rule."""
        suffix = uuid.uuid4().hex[:8]
        rule_name = f"overwrite-{suffix}"
        events.put_rule(Name=rule_name, ScheduleExpression="rate(1 hour)")
        desc1 = events.describe_rule(Name=rule_name)
        assert desc1["ScheduleExpression"] == "rate(1 hour)"

        events.put_rule(Name=rule_name, ScheduleExpression="rate(5 minutes)")
        desc2 = events.describe_rule(Name=rule_name)
        assert desc2["ScheduleExpression"] == "rate(5 minutes)"

        events.delete_rule(Name=rule_name)

    def test_archive_with_event_pattern(self, events):
        """Create archive with event pattern filter."""
        suffix = uuid.uuid4().hex[:8]
        archive_name = f"filtered-archive-{suffix}"
        bus_arn = events.describe_event_bus(Name="default")["Arn"]

        events.create_archive(
            ArchiveName=archive_name,
            EventSourceArn=bus_arn,
            EventPattern=json.dumps({"source": ["myapp"]}),
        )
        desc = events.describe_archive(ArchiveName=archive_name)
        assert desc["ArchiveName"] == archive_name
        pattern = json.loads(desc["EventPattern"])
        assert pattern["source"] == ["myapp"]

        events.delete_archive(ArchiveName=archive_name)

    def test_put_targets_with_static_input(self, events):
        """PutTargets with static Input string."""
        suffix = uuid.uuid4().hex[:8]
        rule_name = f"static-input-{suffix}"
        events.put_rule(Name=rule_name, ScheduleExpression="rate(1 hour)")
        events.put_targets(
            Rule=rule_name,
            Targets=[
                {
                    "Id": "static-t",
                    "Arn": "arn:aws:sqs:us-east-1:123456789012:q1",
                    "Input": json.dumps({"fixed": "payload"}),
                }
            ],
        )
        targets = events.list_targets_by_rule(Rule=rule_name)["Targets"]
        assert len(targets) == 1
        assert json.loads(targets[0]["Input"]) == {"fixed": "payload"}

        events.remove_targets(Rule=rule_name, Ids=["static-t"])
        events.delete_rule(Name=rule_name)

    def test_create_rule_disabled(self, events):
        """Create a rule in DISABLED state."""
        suffix = uuid.uuid4().hex[:8]
        rule_name = f"disabled-{suffix}"
        events.put_rule(Name=rule_name, ScheduleExpression="rate(1 hour)", State="DISABLED")
        desc = events.describe_rule(Name=rule_name)
        assert desc["State"] == "DISABLED"
        events.delete_rule(Name=rule_name)
