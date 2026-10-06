"""EventBridge read-back fidelity: rules and buses return what was set."""

import os
import uuid

import boto3

ENDPOINT_URL = os.environ.get("ENDPOINT_URL", "http://localhost:4566")


def _client(service: str, account: str = "123456789012", **kw):
    return boto3.client(
        service,
        endpoint_url=ENDPOINT_URL,
        region_name=kw.pop("region_name", "us-east-1"),
        aws_access_key_id=kw.pop("aws_access_key_id", account),
        aws_secret_access_key="test",
        **kw,
    )


class TestEventBridgeReadBack:
    def test_rule_role_arn_and_tags_survive(self):
        events = _client("events")
        bus = f"tf-compat-{uuid.uuid4().hex[:8]}"
        bus_arn = events.create_event_bus(
            Name=bus, Description="prod bus", Tags=[{"Key": "team", "Value": "eb"}]
        )["EventBusArn"]
        described_bus = events.describe_event_bus(Name=bus)
        assert described_bus["Description"] == "prod bus"
        assert events.list_tags_for_resource(ResourceARN=bus_arn)["Tags"] == [
            {"Key": "team", "Value": "eb"}
        ]
        role = "arn:aws:iam::123456789012:role/eventbus-role"
        rule_arn = events.put_rule(
            Name="r",
            EventBusName=bus,
            EventPattern='{"source": ["x"]}',
            RoleArn=role,
            Tags=[{"Key": "rule", "Value": "logs"}],
        )["RuleArn"]
        assert events.describe_rule(Name="r", EventBusName=bus)["RoleArn"] == role
        assert events.list_tags_for_resource(ResourceARN=rule_arn)["Tags"] == [
            {"Key": "rule", "Value": "logs"}
        ]

    def test_put_rule_update_keeps_targets(self):
        events = _client("events")
        name = f"tf-compat-{uuid.uuid4().hex[:8]}"
        events.put_rule(Name=name, EventPattern='{"source": ["x"]}')
        events.put_targets(
            Rule=name, Targets=[{"Id": "t1", "Arn": "arn:aws:sqs:us-east-1:123456789012:q"}]
        )
        events.put_rule(Name=name, EventPattern='{"source": ["y"]}', Description="updated")
        targets = events.list_targets_by_rule(Rule=name)["Targets"]
        assert [t["Id"] for t in targets] == ["t1"]
        events.remove_targets(Rule=name, Ids=["t1"])
        events.delete_rule(Name=name)

    def test_update_event_bus(self):
        events = _client("events")
        bus = f"tf-compat-{uuid.uuid4().hex[:8]}"
        events.create_event_bus(Name=bus)
        events.update_event_bus(Name=bus, Description="now described")
        assert events.describe_event_bus(Name=bus)["Description"] == "now described"
