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


    def test_describe_event_bus(self, events):
        """Call describe_event_bus for 'default', assert Name='default' and Arn present."""
        response = events.describe_event_bus(Name="default")
        assert response["Name"] == "default"
        assert "Arn" in response

    def test_list_event_buses(self, events):
        """Create event bus, list_event_buses, assert name in list. Cleanup."""
        suffix = uuid.uuid4().hex[:8]
        bus_name = f"listable-bus-{suffix}"
        events.create_event_bus(Name=bus_name)
        response = events.list_event_buses()
        names = [b["Name"] for b in response["EventBuses"]]
        assert bus_name in names
        events.delete_event_bus(Name=bus_name)

    def test_enable_disable_rule_with_pattern(self, events):
        """Create rule with event pattern, disable, verify DISABLED, enable, verify ENABLED."""
        suffix = uuid.uuid4().hex[:8]
        rule_name = f"pattern-toggle-{suffix}"
        pattern = {"source": ["test.toggle"]}
        events.put_rule(Name=rule_name, EventPattern=json.dumps(pattern))
        events.disable_rule(Name=rule_name)
        desc = events.describe_rule(Name=rule_name)
        assert desc["State"] == "DISABLED"
        events.enable_rule(Name=rule_name)
        desc = events.describe_rule(Name=rule_name)
        assert desc["State"] == "ENABLED"
        events.delete_rule(Name=rule_name)

    def test_create_and_delete_archive(self, events):
        """Create archive from default event bus, describe, delete."""
        suffix = uuid.uuid4().hex[:8]
        archive_name = f"test-archive-{suffix}"
        # Get default event bus ARN
        bus = events.describe_event_bus(Name="default")
        bus_arn = bus["Arn"]
        create_resp = events.create_archive(
            ArchiveName=archive_name,
            EventSourceArn=bus_arn,
            EventPattern=json.dumps({"source": ["test.archive"]}),
            RetentionDays=1,
        )
        assert "ArchiveArn" in create_resp
        desc_resp = events.describe_archive(ArchiveName=archive_name)
        assert desc_resp["ArchiveName"] == archive_name
        events.delete_archive(ArchiveName=archive_name)


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


class TestEventBridgeRulePatterns:
    def test_rule_with_source_pattern(self, events):
        suffix = uuid.uuid4().hex[:8]
        rule_name = f"src-pattern-{suffix}"
        pattern = {"source": ["myapp.service"]}
        try:
            resp = events.put_rule(
                Name=rule_name,
                EventPattern=json.dumps(pattern),
            )
            assert "RuleArn" in resp
            desc = events.describe_rule(Name=rule_name)
            assert json.loads(desc["EventPattern"]) == pattern
        finally:
            events.delete_rule(Name=rule_name)

    def test_rule_with_detail_type_pattern(self, events):
        suffix = uuid.uuid4().hex[:8]
        rule_name = f"dt-pattern-{suffix}"
        pattern = {"detail-type": ["UserSignUp", "UserLogin"]}
        try:
            resp = events.put_rule(
                Name=rule_name,
                EventPattern=json.dumps(pattern),
            )
            assert "RuleArn" in resp
            desc = events.describe_rule(Name=rule_name)
            returned = json.loads(desc["EventPattern"])
            assert set(returned["detail-type"]) == {"UserSignUp", "UserLogin"}
        finally:
            events.delete_rule(Name=rule_name)

    def test_rule_with_detail_field_pattern(self, events):
        suffix = uuid.uuid4().hex[:8]
        rule_name = f"detail-pattern-{suffix}"
        pattern = {
            "source": ["myapp.orders"],
            "detail-type": ["OrderPlaced"],
            "detail": {"status": ["confirmed"]},
        }
        try:
            resp = events.put_rule(
                Name=rule_name,
                EventPattern=json.dumps(pattern),
            )
            assert "RuleArn" in resp
            desc = events.describe_rule(Name=rule_name)
            returned = json.loads(desc["EventPattern"])
            assert returned["detail"]["status"] == ["confirmed"]
        finally:
            events.delete_rule(Name=rule_name)

    def test_rule_with_rate_schedule(self, events):
        suffix = uuid.uuid4().hex[:8]
        rule_name = f"rate-sched-{suffix}"
        try:
            resp = events.put_rule(
                Name=rule_name,
                ScheduleExpression="rate(10 minutes)",
            )
            assert "RuleArn" in resp
            desc = events.describe_rule(Name=rule_name)
            assert desc["ScheduleExpression"] == "rate(10 minutes)"
        finally:
            events.delete_rule(Name=rule_name)

    def test_rule_with_cron_schedule(self, events):
        suffix = uuid.uuid4().hex[:8]
        rule_name = f"cron-sched-{suffix}"
        try:
            resp = events.put_rule(
                Name=rule_name,
                ScheduleExpression="cron(0 12 * * ? *)",
            )
            assert "RuleArn" in resp
            desc = events.describe_rule(Name=rule_name)
            assert desc["ScheduleExpression"] == "cron(0 12 * * ? *)"
        finally:
            events.delete_rule(Name=rule_name)


class TestEventBridgeEventBus:
    def test_list_event_buses_includes_default(self, events):
        buses = events.list_event_buses()
        names = [b["Name"] for b in buses["EventBuses"]]
        assert "default" in names

    def test_create_describe_delete_event_bus(self, events):
        suffix = uuid.uuid4().hex[:8]
        bus_name = f"lifecycle-bus-{suffix}"
        try:
            create_resp = events.create_event_bus(Name=bus_name)
            assert "EventBusArn" in create_resp

            desc = events.describe_event_bus(Name=bus_name)
            assert desc["Name"] == bus_name
            assert "Arn" in desc
        finally:
            events.delete_event_bus(Name=bus_name)

        # Verify it's gone
        buses = events.list_event_buses()
        names = [b["Name"] for b in buses["EventBuses"]]
        assert bus_name not in names

    def test_put_events_to_custom_bus(self, events):
        suffix = uuid.uuid4().hex[:8]
        bus_name = f"custom-ev-{suffix}"
        try:
            events.create_event_bus(Name=bus_name)
            resp = events.put_events(
                Entries=[
                    {
                        "Source": "custom.source",
                        "DetailType": "CustomEvent",
                        "Detail": json.dumps({"data": "test"}),
                        "EventBusName": bus_name,
                    }
                ]
            )
            assert resp["FailedEntryCount"] == 0
        finally:
            events.delete_event_bus(Name=bus_name)


class TestEventBridgeTags:
    def test_tag_and_list_tags_on_rule(self, events):
        suffix = uuid.uuid4().hex[:8]
        rule_name = f"tag-rule-{suffix}"
        try:
            resp = events.put_rule(
                Name=rule_name,
                ScheduleExpression="rate(1 hour)",
            )
            rule_arn = resp["RuleArn"]
            events.tag_resource(
                ResourceARN=rule_arn,
                Tags=[
                    {"Key": "env", "Value": "staging"},
                    {"Key": "team", "Value": "backend"},
                ],
            )
            tags_resp = events.list_tags_for_resource(ResourceARN=rule_arn)
            tag_map = {t["Key"]: t["Value"] for t in tags_resp["Tags"]}
            assert tag_map["env"] == "staging"
            assert tag_map["team"] == "backend"
        finally:
            events.delete_rule(Name=rule_name)

    def test_untag_resource_on_rule(self, events):
        suffix = uuid.uuid4().hex[:8]
        rule_name = f"untag-rule-{suffix}"
        try:
            resp = events.put_rule(
                Name=rule_name,
                ScheduleExpression="rate(1 hour)",
            )
            rule_arn = resp["RuleArn"]
            events.tag_resource(
                ResourceARN=rule_arn,
                Tags=[
                    {"Key": "remove-me", "Value": "yes"},
                    {"Key": "keep-me", "Value": "yes"},
                ],
            )
            events.untag_resource(
                ResourceARN=rule_arn, TagKeys=["remove-me"]
            )
            tags_resp = events.list_tags_for_resource(ResourceARN=rule_arn)
            keys = [t["Key"] for t in tags_resp["Tags"]]
            assert "remove-me" not in keys
            assert "keep-me" in keys
        finally:
            events.delete_rule(Name=rule_name)


class TestEventBridgeListRules:
    def test_list_rules_with_name_prefix(self, events):
        suffix = uuid.uuid4().hex[:8]
        prefix = f"pfx-{suffix}"
        rule1 = f"{prefix}-rule-a"
        rule2 = f"{prefix}-rule-b"
        other = f"other-{suffix}-rule"
        try:
            events.put_rule(Name=rule1, ScheduleExpression="rate(1 hour)")
            events.put_rule(Name=rule2, ScheduleExpression="rate(2 hours)")
            events.put_rule(Name=other, ScheduleExpression="rate(3 hours)")

            resp = events.list_rules(NamePrefix=prefix)
            names = [r["Name"] for r in resp["Rules"]]
            assert rule1 in names
            assert rule2 in names
            assert other not in names
        finally:
            events.delete_rule(Name=rule1)
            events.delete_rule(Name=rule2)
            events.delete_rule(Name=other)

    def test_describe_rule_details(self, events):
        suffix = uuid.uuid4().hex[:8]
        rule_name = f"desc-detail-{suffix}"
        try:
            events.put_rule(
                Name=rule_name,
                ScheduleExpression="rate(5 minutes)",
                State="ENABLED",
                Description="A test rule",
            )
            desc = events.describe_rule(Name=rule_name)
            assert desc["Name"] == rule_name
            assert desc["State"] == "ENABLED"
            assert desc["Description"] == "A test rule"
            assert desc["ScheduleExpression"] == "rate(5 minutes)"
            assert "Arn" in desc
        finally:
            events.delete_rule(Name=rule_name)


class TestEventBridgeTargets:
    def test_put_targets_multiple_and_remove_subset(self, events):
        suffix = uuid.uuid4().hex[:8]
        rule_name = f"targets-rm-{suffix}"
        try:
            events.put_rule(Name=rule_name, ScheduleExpression="rate(1 hour)")
            events.put_targets(
                Rule=rule_name,
                Targets=[
                    {"Id": "t1", "Arn": "arn:aws:sqs:us-east-1:123456789012:q1"},
                    {"Id": "t2", "Arn": "arn:aws:sqs:us-east-1:123456789012:q2"},
                    {"Id": "t3", "Arn": "arn:aws:sqs:us-east-1:123456789012:q3"},
                ],
            )
            targets = events.list_targets_by_rule(Rule=rule_name)["Targets"]
            assert len(targets) == 3

            events.remove_targets(Rule=rule_name, Ids=["t1", "t3"])
            targets = events.list_targets_by_rule(Rule=rule_name)["Targets"]
            ids = [t["Id"] for t in targets]
            assert ids == ["t2"]
        finally:
            events.remove_targets(Rule=rule_name, Ids=["t2"])
            events.delete_rule(Name=rule_name)

    def test_put_targets_returns_failed_count(self, events):
        suffix = uuid.uuid4().hex[:8]
        rule_name = f"targets-resp-{suffix}"
        try:
            events.put_rule(Name=rule_name, ScheduleExpression="rate(1 hour)")
            resp = events.put_targets(
                Rule=rule_name,
                Targets=[
                    {"Id": "t1", "Arn": "arn:aws:sqs:us-east-1:123456789012:q1"},
                ],
            )
            assert resp["FailedEntryCount"] == 0
            assert "FailedEntries" in resp
        finally:
            events.remove_targets(Rule=rule_name, Ids=["t1"])
            events.delete_rule(Name=rule_name)

    def test_list_targets_empty(self, events):
        suffix = uuid.uuid4().hex[:8]
        rule_name = f"targets-empty-{suffix}"
        try:
            events.put_rule(Name=rule_name, ScheduleExpression="rate(1 hour)")
            resp = events.list_targets_by_rule(Rule=rule_name)
            assert resp["Targets"] == []
        finally:
            events.delete_rule(Name=rule_name)


class TestEventBridgeEventBuses:
    def test_put_list_remove_targets(self, events):
        """PutTargets, ListTargetsByRule, RemoveTargets full lifecycle."""
        suffix = uuid.uuid4().hex[:8]
        rule_name = f"tgt-mgmt-{suffix}"
        events.put_rule(Name=rule_name, ScheduleExpression="rate(1 hour)")
        try:
            events.put_targets(
                Rule=rule_name,
                Targets=[
                    {"Id": "tgt-a", "Arn": "arn:aws:sqs:us-east-1:123456789012:q-a"},
                    {"Id": "tgt-b", "Arn": "arn:aws:sqs:us-east-1:123456789012:q-b"},
                ],
            )
            resp = events.list_targets_by_rule(Rule=rule_name)
            ids = sorted([t["Id"] for t in resp["Targets"]])
            assert ids == ["tgt-a", "tgt-b"]

            events.remove_targets(Rule=rule_name, Ids=["tgt-a"])
            resp = events.list_targets_by_rule(Rule=rule_name)
            ids = [t["Id"] for t in resp["Targets"]]
            assert "tgt-a" not in ids
            assert "tgt-b" in ids

            events.remove_targets(Rule=rule_name, Ids=["tgt-b"])
        finally:
            events.delete_rule(Name=rule_name)


class TestEventBridgeRuleState:
    def test_create_and_describe_archive(self, events):
        suffix = uuid.uuid4().hex[:8]
        archive_name = f"test-archive-{suffix}"
        try:
            # Get the default event bus ARN
            bus = events.describe_event_bus(Name="default")
            bus_arn = bus["Arn"]

            create_resp = events.create_archive(
                ArchiveName=archive_name,
                EventSourceArn=bus_arn,
                Description="Test archive",
                RetentionDays=7,
            )
            assert "ArchiveArn" in create_resp

            desc = events.describe_archive(ArchiveName=archive_name)
            assert desc["ArchiveName"] == archive_name
            assert desc["EventSourceArn"] == bus_arn
            assert desc["RetentionDays"] == 7
        finally:
            events.delete_archive(ArchiveName=archive_name)

    def test_list_archives(self, events):
        suffix = uuid.uuid4().hex[:8]
        archive_name = f"list-archive-{suffix}"
        try:
            bus = events.describe_event_bus(Name="default")
            bus_arn = bus["Arn"]
            events.create_archive(
                ArchiveName=archive_name,
                EventSourceArn=bus_arn,
            )
            resp = events.list_archives()
            names = [a["ArchiveName"] for a in resp["Archives"]]
            assert archive_name in names
        finally:
            events.delete_archive(ArchiveName=archive_name)

    def test_delete_archive(self, events):
        suffix = uuid.uuid4().hex[:8]
        archive_name = f"del-archive-{suffix}"
        bus = events.describe_event_bus(Name="default")
        bus_arn = bus["Arn"]
        events.create_archive(
            ArchiveName=archive_name,
            EventSourceArn=bus_arn,
        )
        events.delete_archive(ArchiveName=archive_name)
        resp = events.list_archives()
        names = [a["ArchiveName"] for a in resp["Archives"]]
        assert archive_name not in names
    def test_create_describe_list_delete_archive(self, events):
        """Archive CRUD lifecycle."""
        suffix = uuid.uuid4().hex[:8]
        archive_name = f"test-archive-{suffix}"
        # Get the default bus ARN
        bus = events.describe_event_bus(Name="default")
        bus_arn = bus["Arn"]
        try:
            create_resp = events.create_archive(
                ArchiveName=archive_name,
                EventSourceArn=bus_arn,
                EventPattern=json.dumps({"source": ["test.archive"]}),
                RetentionDays=7,
            )
            assert create_resp["ArchiveArn"] is not None

            desc_resp = events.describe_archive(ArchiveName=archive_name)
            assert desc_resp["ArchiveName"] == archive_name

            list_resp = events.list_archives()
            archive_names = [a["ArchiveName"] for a in list_resp["Archives"]]
            assert archive_name in archive_names

            events.delete_archive(ArchiveName=archive_name)
        except Exception:
            # Clean up if partially created
            try:
                events.delete_archive(ArchiveName=archive_name)
            except Exception:
                pass
            raise


class TestEventBridgeDescribeRuleFields:
    def test_describe_rule_all_fields(self, events):
        """DescribeRule returns all expected fields."""
        suffix = uuid.uuid4().hex[:8]
        rule_name = f"describe-fields-{suffix}"
        events.put_rule(
            Name=rule_name,
            ScheduleExpression="rate(5 minutes)",
            State="ENABLED",
            Description="A test rule",
        )
        try:
            resp = events.describe_rule(Name=rule_name)
            assert resp["Name"] == rule_name
            assert "Arn" in resp
            assert resp["State"] == "ENABLED"
            assert resp["ScheduleExpression"] == "rate(5 minutes)"
            assert resp["Description"] == "A test rule"
            assert "EventBusName" in resp
        finally:
            events.delete_rule(Name=rule_name)


class TestEventBridgeTargets:
    def test_list_targets_by_rule(self, events):
        """ListTargetsByRule returns targets for a rule."""
        suffix = uuid.uuid4().hex[:8]
        rule_name = f"lt-rule-{suffix}"
        events.put_rule(Name=rule_name, ScheduleExpression="rate(1 hour)")
        try:
            events.put_targets(
                Rule=rule_name,
                Targets=[
                    {"Id": "t1", "Arn": "arn:aws:sqs:us-east-1:123456789012:q1"},
                ],
            )
            resp = events.list_targets_by_rule(Rule=rule_name)
            assert len(resp["Targets"]) == 1
            assert resp["Targets"][0]["Id"] == "t1"
        finally:
            events.remove_targets(Rule=rule_name, Ids=["t1"])
            events.delete_rule(Name=rule_name)

    def test_put_targets_multiple(self, events):
        """PutTargets with multiple targets on one rule."""
        suffix = uuid.uuid4().hex[:8]
        rule_name = f"multi-tgt-{suffix}"
        events.put_rule(Name=rule_name, ScheduleExpression="rate(1 hour)")
        try:
            resp = events.put_targets(
                Rule=rule_name,
                Targets=[
                    {"Id": "tA", "Arn": "arn:aws:sqs:us-east-1:123456789012:qA"},
                    {"Id": "tB", "Arn": "arn:aws:sqs:us-east-1:123456789012:qB"},
                    {"Id": "tC", "Arn": "arn:aws:lambda:us-east-1:123456789012:function:fn"},
                ],
            )
            assert resp["FailedEntryCount"] == 0
            targets = events.list_targets_by_rule(Rule=rule_name)["Targets"]
            ids = [t["Id"] for t in targets]
            assert "tA" in ids
            assert "tB" in ids
            assert "tC" in ids
        finally:
            events.remove_targets(Rule=rule_name, Ids=["tA", "tB", "tC"])
            events.delete_rule(Name=rule_name)

    def test_remove_targets(self, events):
        """RemoveTargets removes specific targets from a rule."""
        suffix = uuid.uuid4().hex[:8]
        rule_name = f"rm-tgt-{suffix}"
        events.put_rule(Name=rule_name, ScheduleExpression="rate(1 hour)")
        try:
            events.put_targets(
                Rule=rule_name,
                Targets=[
                    {"Id": "keep", "Arn": "arn:aws:sqs:us-east-1:123456789012:keep"},
                    {"Id": "remove", "Arn": "arn:aws:sqs:us-east-1:123456789012:remove"},
                ],
            )
            events.remove_targets(Rule=rule_name, Ids=["remove"])
            targets = events.list_targets_by_rule(Rule=rule_name)["Targets"]
            ids = [t["Id"] for t in targets]
            assert "keep" in ids
            assert "remove" not in ids
        finally:
            events.remove_targets(Rule=rule_name, Ids=["keep"])
            events.delete_rule(Name=rule_name)


class TestEventBridgeListRulesFilter:
    def test_create_describe_delete_archive(self, events):
        """Test CreateArchive, DescribeArchive, DeleteArchive lifecycle."""
        suffix = uuid.uuid4().hex[:8]
        archive_name = f"test-archive-{suffix}"
        # Get the default event bus ARN
        bus = events.describe_event_bus(Name="default")
        bus_arn = bus["Arn"]
        try:
            create_resp = events.create_archive(
                ArchiveName=archive_name,
                EventSourceArn=bus_arn,
                Description="Test archive",
                RetentionDays=7,
            )
            assert "ArchiveArn" in create_resp

            desc_resp = events.describe_archive(ArchiveName=archive_name)
            assert desc_resp["ArchiveName"] == archive_name
            assert desc_resp["RetentionDays"] == 7
            assert "State" in desc_resp
        finally:
            events.delete_archive(ArchiveName=archive_name)


class TestEventBridgePartnerEvents:
    @pytest.mark.xfail(reason="PutPartnerEventsSource may not be supported")
    def test_put_partner_events_source(self, events):
        """PutPartnerEventsSource is not commonly supported in emulators."""
        events.put_partner_events(
            Entries=[
                {
                    "Source": "aws.partner/example.com/test",
                    "DetailType": "PartnerEvent",
                    "Detail": json.dumps({"key": "value"}),
                }
            ]
        )


class TestEventBridgeRuleTagging:
    def test_tag_untag_list_tags_on_rule(self, events):
        """TagResource, UntagResource, ListTagsForResource on rules."""
        suffix = uuid.uuid4().hex[:8]
        rule_name = f"tag-rule-{suffix}"
        resp = events.put_rule(Name=rule_name, ScheduleExpression="rate(1 hour)")
        rule_arn = resp["RuleArn"]
        try:
            events.tag_resource(
                ResourceARN=rule_arn,
                Tags=[
                    {"Key": "env", "Value": "staging"},
                    {"Key": "team", "Value": "infra"},
                ],
            )
            tags_resp = events.list_tags_for_resource(ResourceARN=rule_arn)
            tags = {t["Key"]: t["Value"] for t in tags_resp["Tags"]}
            assert tags["env"] == "staging"
            assert tags["team"] == "infra"

            events.untag_resource(ResourceARN=rule_arn, TagKeys=["team"])
            tags_resp = events.list_tags_for_resource(ResourceARN=rule_arn)
            keys = [t["Key"] for t in tags_resp["Tags"]]
            assert "env" in keys
            assert "team" not in keys
        finally:
            events.delete_rule(Name=rule_name)


class TestEventBridgeConnections:
    @pytest.mark.xfail(reason="CreateConnection may not be supported")
    def test_create_describe_connection(self, events):
        """Test CreateConnection and DescribeConnection."""
        suffix = uuid.uuid4().hex[:8]
        conn_name = f"test-conn-{suffix}"
        try:
            resp = events.create_connection(
                Name=conn_name,
                AuthorizationType="API_KEY",
                AuthParameters={
                    "ApiKeyAuthParameters": {
                        "ApiKeyName": "x-api-key",
                        "ApiKeyValue": "secret123",
                    }
                },
            )
            assert "ConnectionArn" in resp
            desc = events.describe_connection(Name=conn_name)
            assert desc["Name"] == conn_name
            assert desc["AuthorizationType"] == "API_KEY"
        finally:
            try:
                events.delete_connection(Name=conn_name)
            except Exception:
                pass


class TestEventBridgeApiDestinations:
    @pytest.mark.xfail(reason="CreateApiDestination may not be supported")
    def test_create_describe_api_destination(self, events):
        """Test CreateApiDestination and DescribeApiDestination."""
        suffix = uuid.uuid4().hex[:8]
        conn_name = f"api-dest-conn-{suffix}"
        dest_name = f"test-api-dest-{suffix}"
        try:
            conn_resp = events.create_connection(
                Name=conn_name,
                AuthorizationType="API_KEY",
                AuthParameters={
                    "ApiKeyAuthParameters": {
                        "ApiKeyName": "x-api-key",
                        "ApiKeyValue": "secret",
                    }
                },
            )
            conn_arn = conn_resp["ConnectionArn"]
            resp = events.create_api_destination(
                Name=dest_name,
                ConnectionArn=conn_arn,
                InvocationEndpoint="https://example.com/api",
                HttpMethod="POST",
                InvocationRateLimitPerSecond=10,
            )
            assert "ApiDestinationArn" in resp
            desc = events.describe_api_destination(Name=dest_name)
            assert desc["Name"] == dest_name
            assert desc["HttpMethod"] == "POST"
        finally:
            try:
                events.delete_api_destination(Name=dest_name)
            except Exception:
                pass
            try:
                events.delete_connection(Name=conn_name)
            except Exception:
                pass
