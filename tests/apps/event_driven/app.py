"""
EventRouter — Event-driven microservices architecture using AWS primitives.

Simulates a production event-driven system where multiple microservices
communicate through EventBridge (routing), SNS (fan-out), and SQS (consumption).
A DynamoDB table serves as a schema registry for event validation.

Architecture:
    Order Service ──┐
                    ├──► EventBridge Bus ──► Rules ──► SNS Topics ──► SQS Queues
    Inventory Svc ──┘         │                              │
                              ▼                              ▼
                        DLQ (failures)              Fan-out consumers

Services used: EventBridge, SNS, SQS, DynamoDB
"""

from __future__ import annotations

import json
import time
import uuid
from typing import Any

from .models import (
    Event,
    EventRule,
    EventSchema,
    EventStats,
    EventTarget,
    FanOutConfig,
)


class EventRouter:
    """Manages event-driven infrastructure: buses, rules, fan-out, and schemas.

    This class orchestrates the full lifecycle of an event-driven architecture:
    - Creating and managing custom EventBridge event buses
    - Defining rules with complex pattern matching
    - Wiring SNS topics for fan-out to multiple SQS consumers
    - Publishing structured events with validation
    - Tracking event statistics
    - Managing an event schema registry in DynamoDB
    """

    def __init__(
        self,
        events_client: Any,
        sns_client: Any,
        sqs_client: Any,
        dynamodb_client: Any,
        account_id: str = "123456789012",
        region: str = "us-east-1",
    ) -> None:
        self.events = events_client
        self.sns = sns_client
        self.sqs = sqs_client
        self.dynamodb = dynamodb_client
        self.account_id = account_id
        self.region = region

        # Track resources for cleanup
        self._buses: list[str] = []
        self._rules: list[EventRule] = []
        self._topics: list[str] = []
        self._queues: list[str] = []
        self._subscriptions: list[str] = []
        self._fan_outs: list[FanOutConfig] = []
        self._stats: dict[str, EventStats] = {}
        self._event_archive: list[dict[str, Any]] = []
        self._schema_table: str | None = None

    # ─── Event Bus Management ─────────────────────────────────────────

    def create_bus(self, name: str) -> str:
        """Create a custom EventBridge event bus.

        Returns the bus ARN.
        """
        resp = self.events.create_event_bus(Name=name)
        self._buses.append(name)
        return resp["EventBusArn"]

    def delete_bus(self, name: str) -> None:
        """Delete an event bus, cleaning up all its rules and targets first."""
        rules = self.events.list_rules(EventBusName=name).get("Rules", [])
        for rule in rules:
            targets = self.events.list_targets_by_rule(Rule=rule["Name"], EventBusName=name).get(
                "Targets", []
            )
            if targets:
                self.events.remove_targets(
                    Rule=rule["Name"],
                    EventBusName=name,
                    Ids=[t["Id"] for t in targets],
                )
            self.events.delete_rule(Name=rule["Name"], EventBusName=name)
        self.events.delete_event_bus(Name=name)
        if name in self._buses:
            self._buses.remove(name)

    def describe_bus(self, name: str) -> dict[str, Any]:
        """Describe an event bus."""
        return self.events.describe_event_bus(Name=name)

    def set_bus_policy(self, bus_name: str, policy: dict[str, Any]) -> None:
        """Set a resource policy on an event bus for cross-account sharing."""
        self.events.put_permission(
            EventBusName=bus_name,
            Action="events:PutEvents",
            Principal=policy.get("principal", "*"),
            StatementId=policy.get("statement_id", f"allow-{uuid.uuid4().hex[:8]}"),
        )

    # ─── Rule Management ──────────────────────────────────────────────

    def create_rule(self, rule: EventRule) -> str:
        """Create an EventBridge rule with pattern matching.

        Returns the rule ARN.
        """
        resp = self.events.put_rule(
            Name=rule.name,
            EventBusName=rule.bus_name,
            EventPattern=rule.pattern_json,
            State=rule.state,
            Description=rule.description,
        )
        rule_arn = resp["RuleArn"]

        if rule.targets:
            self.events.put_targets(
                Rule=rule.name,
                EventBusName=rule.bus_name,
                Targets=[t.to_dict() for t in rule.targets],
            )

        self._rules.append(rule)
        return rule_arn

    def add_targets(self, rule_name: str, bus_name: str, targets: list[EventTarget]) -> None:
        """Add targets to an existing rule."""
        self.events.put_targets(
            Rule=rule_name,
            EventBusName=bus_name,
            Targets=[t.to_dict() for t in targets],
        )
        for r in self._rules:
            if r.name == rule_name and r.bus_name == bus_name:
                r.targets.extend(targets)
                break

    def remove_targets(self, rule_name: str, bus_name: str, target_ids: list[str]) -> None:
        """Remove targets from a rule."""
        self.events.remove_targets(
            Rule=rule_name,
            EventBusName=bus_name,
            Ids=target_ids,
        )

    def disable_rule(self, rule_name: str, bus_name: str) -> None:
        """Disable a rule so it stops matching events."""
        self.events.disable_rule(Name=rule_name, EventBusName=bus_name)

    def enable_rule(self, rule_name: str, bus_name: str) -> None:
        """Enable a previously disabled rule."""
        self.events.enable_rule(Name=rule_name, EventBusName=bus_name)

    def list_rules(self, bus_name: str) -> list[dict[str, Any]]:
        """List all rules on an event bus."""
        return self.events.list_rules(EventBusName=bus_name).get("Rules", [])

    def describe_rule(self, rule_name: str, bus_name: str) -> dict[str, Any]:
        """Describe a specific rule."""
        return self.events.describe_rule(Name=rule_name, EventBusName=bus_name)

    def list_targets(self, rule_name: str, bus_name: str) -> list[dict[str, Any]]:
        """List targets for a rule."""
        return self.events.list_targets_by_rule(Rule=rule_name, EventBusName=bus_name).get(
            "Targets", []
        )

    # ─── Event Publishing ─────────────────────────────────────────────

    def publish_event(self, bus_name: str, event: Event) -> dict[str, Any]:
        """Publish a single event to an event bus.

        Records the event in the archive and updates statistics.
        Returns the PutEvents response.
        """
        entry = event.to_entry(bus_name)
        resp = self.events.put_events(Entries=[entry])

        # Archive the event for replay
        self._event_archive.append(
            {
                "bus": bus_name,
                "source": event.source,
                "detail_type": event.detail_type,
                "detail": event.detail,
                "time": event.time,
                "entry_id": str(uuid.uuid4()),
            }
        )

        # Update stats
        stats_key = f"{event.source}#{event.detail_type}"
        if stats_key not in self._stats:
            self._stats[stats_key] = EventStats(source=event.source, detail_type=event.detail_type)
        self._stats[stats_key].record()

        return resp

    def publish_events_batch(self, bus_name: str, events: list[Event]) -> dict[str, Any]:
        """Publish multiple events in a single PutEvents call.

        EventBridge supports up to 10 entries per call. This method
        batches larger lists into multiple calls.
        """
        all_responses: list[dict[str, Any]] = []
        batch_size = 10

        for i in range(0, len(events), batch_size):
            batch = events[i : i + batch_size]
            entries = [e.to_entry(bus_name) for e in batch]
            resp = self.events.put_events(Entries=entries)
            all_responses.append(resp)

            # Archive and track stats
            for event in batch:
                self._event_archive.append(
                    {
                        "bus": bus_name,
                        "source": event.source,
                        "detail_type": event.detail_type,
                        "detail": event.detail,
                        "time": event.time,
                        "entry_id": str(uuid.uuid4()),
                    }
                )
                stats_key = f"{event.source}#{event.detail_type}"
                if stats_key not in self._stats:
                    self._stats[stats_key] = EventStats(
                        source=event.source, detail_type=event.detail_type
                    )
                self._stats[stats_key].record()

        return {
            "FailedEntryCount": sum(r.get("FailedEntryCount", 0) for r in all_responses),
            "Entries": [entry for r in all_responses for entry in r.get("Entries", [])],
        }

    # ─── Event Replay ─────────────────────────────────────────────────

    def get_archived_events(
        self,
        bus_name: str | None = None,
        source: str | None = None,
        detail_type: str | None = None,
    ) -> list[dict[str, Any]]:
        """Retrieve archived events with optional filtering."""
        results = self._event_archive
        if bus_name:
            results = [e for e in results if e["bus"] == bus_name]
        if source:
            results = [e for e in results if e["source"] == source]
        if detail_type:
            results = [e for e in results if e["detail_type"] == detail_type]
        return results

    def replay_events(
        self,
        bus_name: str,
        source: str | None = None,
        detail_type: str | None = None,
    ) -> int:
        """Replay archived events back onto the bus.

        Returns the number of events replayed.
        """
        archived = self.get_archived_events(
            bus_name=bus_name, source=source, detail_type=detail_type
        )
        if not archived:
            return 0

        events_to_replay = [
            Event(
                source=e["source"],
                detail_type=e["detail_type"],
                detail=e["detail"],
            )
            for e in archived
        ]
        self.publish_events_batch(bus_name, events_to_replay)
        return len(events_to_replay)

    # ─── SNS Fan-Out ──────────────────────────────────────────────────

    def create_fan_out(self, topic_name: str, queue_names: list[str]) -> FanOutConfig:
        """Create an SNS topic with SQS subscriber queues wired together.

        Creates the topic, creates the queues, subscribes each queue to the
        topic, and returns a FanOutConfig with all ARNs.
        """
        topic_resp = self.sns.create_topic(Name=topic_name)
        topic_arn = topic_resp["TopicArn"]
        self._topics.append(topic_arn)

        queue_arns: list[str] = []
        for qname in queue_names:
            q_resp = self.sqs.create_queue(QueueName=qname)
            q_url = q_resp["QueueUrl"]
            self._queues.append(q_url)
            q_arn = self.sqs.get_queue_attributes(QueueUrl=q_url, AttributeNames=["QueueArn"])[
                "Attributes"
            ]["QueueArn"]
            queue_arns.append(q_arn)

            sub_resp = self.sns.subscribe(TopicArn=topic_arn, Protocol="sqs", Endpoint=q_arn)
            self._subscriptions.append(sub_resp["SubscriptionArn"])

        config = FanOutConfig(topic_arn=topic_arn, subscriptions=queue_arns)
        self._fan_outs.append(config)
        return config

    def create_filtered_subscription(
        self,
        topic_arn: str,
        queue_name: str,
        filter_policy: dict[str, Any],
        raw_delivery: bool = False,
    ) -> tuple[str, str, str]:
        """Create an SQS queue subscribed to an SNS topic with a filter policy.

        Returns (queue_url, queue_arn, subscription_arn).
        """
        q_resp = self.sqs.create_queue(QueueName=queue_name)
        q_url = q_resp["QueueUrl"]
        self._queues.append(q_url)
        q_arn = self.sqs.get_queue_attributes(QueueUrl=q_url, AttributeNames=["QueueArn"])[
            "Attributes"
        ]["QueueArn"]

        sub_resp = self.sns.subscribe(TopicArn=topic_arn, Protocol="sqs", Endpoint=q_arn)
        sub_arn = sub_resp["SubscriptionArn"]
        self._subscriptions.append(sub_arn)

        # Set filter policy
        self.sns.set_subscription_attributes(
            SubscriptionArn=sub_arn,
            AttributeName="FilterPolicy",
            AttributeValue=json.dumps(filter_policy),
        )

        if raw_delivery:
            self.sns.set_subscription_attributes(
                SubscriptionArn=sub_arn,
                AttributeName="RawMessageDelivery",
                AttributeValue="true",
            )

        return q_url, q_arn, sub_arn

    def publish_to_topic(
        self,
        topic_arn: str,
        message: str | dict[str, Any],
        subject: str | None = None,
        attributes: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        """Publish a message to an SNS topic.

        If message is a dict, it is JSON-serialized.
        Optionally includes message attributes for filter policies.
        """
        if isinstance(message, dict):
            message = json.dumps(message)

        kwargs: dict[str, Any] = {"TopicArn": topic_arn, "Message": message}
        if subject:
            kwargs["Subject"] = subject
        if attributes:
            kwargs["MessageAttributes"] = {
                k: {"DataType": "String", "StringValue": str(v)} for k, v in attributes.items()
            }

        return self.sns.publish(**kwargs)

    def get_queue_url_by_name(self, queue_name: str) -> str:
        """Look up a queue URL by name."""
        return self.sqs.get_queue_url(QueueName=queue_name)["QueueUrl"]

    def get_queue_arn(self, queue_url: str) -> str:
        """Get the ARN for a queue given its URL."""
        return self.sqs.get_queue_attributes(QueueUrl=queue_url, AttributeNames=["QueueArn"])[
            "Attributes"
        ]["QueueArn"]

    # ─── SQS Consumer Helpers ─────────────────────────────────────────

    def receive_messages(
        self,
        queue_url: str,
        expected: int = 1,
        timeout: int = 10,
        delete: bool = False,
    ) -> list[dict[str, Any]]:
        """Poll an SQS queue until expected messages arrive or timeout.

        Optionally deletes messages after receiving them.
        """
        messages: list[dict[str, Any]] = []
        deadline = time.time() + timeout
        while len(messages) < expected and time.time() < deadline:
            resp = self.sqs.receive_message(
                QueueUrl=queue_url,
                MaxNumberOfMessages=10,
                WaitTimeSeconds=1,
                MessageAttributeNames=["All"],
            )
            batch = resp.get("Messages", [])
            messages.extend(batch)
            if delete:
                for msg in batch:
                    self.sqs.delete_message(
                        QueueUrl=queue_url,
                        ReceiptHandle=msg["ReceiptHandle"],
                    )
        return messages

    def drain_queue(self, queue_url: str, timeout: int = 5) -> list[dict[str, Any]]:
        """Receive all available messages from a queue."""
        return self.receive_messages(queue_url, expected=999, timeout=timeout, delete=True)

    def parse_event_from_sqs(self, message: dict[str, Any]) -> dict[str, Any]:
        """Parse an EventBridge event envelope from an SQS message body."""
        body = json.loads(message["Body"])
        return body

    def parse_sns_from_sqs(self, message: dict[str, Any]) -> dict[str, Any]:
        """Parse an SNS notification envelope from an SQS message body."""
        body = json.loads(message["Body"])
        if "Message" in body:
            try:
                body["ParsedMessage"] = json.loads(body["Message"])
            except (json.JSONDecodeError, TypeError):
                body["ParsedMessage"] = body["Message"]
        return body

    # ─── Schema Registry (DynamoDB) ───────────────────────────────────

    def init_schema_table(self, table_name: str) -> str:
        """Create the DynamoDB table for the event schema registry.

        Returns the table name.
        """
        self.dynamodb.create_table(
            TableName=table_name,
            KeySchema=[
                {"AttributeName": "pk", "KeyType": "HASH"},
                {"AttributeName": "sk", "KeyType": "RANGE"},
            ],
            AttributeDefinitions=[
                {"AttributeName": "pk", "AttributeType": "S"},
                {"AttributeName": "sk", "AttributeType": "S"},
            ],
            BillingMode="PAY_PER_REQUEST",
        )
        self._schema_table = table_name
        return table_name

    def register_schema(self, schema: EventSchema) -> EventSchema:
        """Register an event schema in the registry.

        If a schema with the same name and source exists, auto-increments
        the version number.
        """
        if not self._schema_table:
            raise RuntimeError("Schema table not initialized. Call init_schema_table first.")

        # Check for existing versions
        existing = self.dynamodb.query(
            TableName=self._schema_table,
            KeyConditionExpression="pk = :pk",
            ExpressionAttributeValues={":pk": {"S": f"SCHEMA#{schema.source}#{schema.name}"}},
            ScanIndexForward=False,
            Limit=1,
        )
        if existing.get("Items"):
            latest_version = int(existing["Items"][0]["version"]["N"])
            schema.version = latest_version + 1

        self.dynamodb.put_item(
            TableName=self._schema_table,
            Item=schema.to_dynamodb_item(),
        )
        return schema

    def get_schema(self, name: str, source: str, version: int | None = None) -> EventSchema | None:
        """Retrieve a schema by name and source, optionally at a specific version.

        If version is None, returns the latest version.
        """
        if not self._schema_table:
            raise RuntimeError("Schema table not initialized.")

        pk = f"SCHEMA#{source}#{name}"
        if version is not None:
            resp = self.dynamodb.get_item(
                TableName=self._schema_table,
                Key={"pk": {"S": pk}, "sk": {"S": f"v{version}"}},
            )
            item = resp.get("Item")
            return EventSchema.from_dynamodb_item(item) if item else None
        else:
            resp = self.dynamodb.query(
                TableName=self._schema_table,
                KeyConditionExpression="pk = :pk",
                ExpressionAttributeValues={":pk": {"S": pk}},
                ScanIndexForward=False,
                Limit=1,
            )
            items = resp.get("Items", [])
            return EventSchema.from_dynamodb_item(items[0]) if items else None

    def list_schemas(self, source: str | None = None) -> list[EventSchema]:
        """List all schemas, optionally filtered by source."""
        if not self._schema_table:
            raise RuntimeError("Schema table not initialized.")

        if source:
            resp = self.dynamodb.scan(
                TableName=self._schema_table,
                FilterExpression="#src = :src",
                ExpressionAttributeNames={"#src": "source"},
                ExpressionAttributeValues={":src": {"S": source}},
            )
        else:
            resp = self.dynamodb.scan(TableName=self._schema_table)

        return [EventSchema.from_dynamodb_item(item) for item in resp.get("Items", [])]

    def validate_event(self, event: Event) -> tuple[bool, str]:
        """Validate an event's detail against its registered schema.

        Returns (is_valid, error_message). If no schema is registered,
        returns (True, "") — validation is advisory, not mandatory.
        """
        schema = self.get_schema(name=event.detail_type, source=event.source)
        if schema is None:
            return True, ""

        # Basic structural validation against the JSON schema
        json_schema = schema.json_schema
        required_fields = json_schema.get("required", [])
        properties = json_schema.get("properties", {})

        for field_name in required_fields:
            if field_name not in event.detail:
                return False, f"Missing required field: {field_name}"

        for field_name, value in event.detail.items():
            if field_name in properties:
                expected_type = properties[field_name].get("type")
                if expected_type == "string" and not isinstance(value, str):
                    return False, f"Field '{field_name}' must be a string"
                elif expected_type == "number" and not isinstance(value, (int, float)):
                    return False, f"Field '{field_name}' must be a number"
                elif expected_type == "integer" and not isinstance(value, int):
                    return False, f"Field '{field_name}' must be an integer"
                elif expected_type == "boolean" and not isinstance(value, bool):
                    return False, f"Field '{field_name}' must be a boolean"
                elif expected_type == "array" and not isinstance(value, list):
                    return False, f"Field '{field_name}' must be an array"
                elif expected_type == "object" and not isinstance(value, dict):
                    return False, f"Field '{field_name}' must be an object"

        return True, ""

    # ─── Event Statistics ─────────────────────────────────────────────

    def get_stats(self, source: str | None = None) -> list[EventStats]:
        """Get event statistics, optionally filtered by source."""
        stats = list(self._stats.values())
        if source:
            stats = [s for s in stats if s.source == source]
        return stats

    def get_stats_by_detail_type(self, detail_type: str) -> list[EventStats]:
        """Get event statistics filtered by detail type."""
        return [s for s in self._stats.values() if s.detail_type == detail_type]

    def reset_stats(self) -> None:
        """Reset all event statistics."""
        self._stats.clear()

    # ─── Dead Letter Queue ────────────────────────────────────────────

    def create_dlq(self, name: str) -> tuple[str, str]:
        """Create a dead-letter queue.

        Returns (queue_url, queue_arn).
        """
        resp = self.sqs.create_queue(QueueName=name)
        q_url = resp["QueueUrl"]
        self._queues.append(q_url)
        q_arn = self.get_queue_arn(q_url)
        return q_url, q_arn

    # ─── SQS Queue Creation Helper ────────────────────────────────────

    def create_queue(self, name: str) -> tuple[str, str]:
        """Create an SQS queue.

        Returns (queue_url, queue_arn).
        """
        resp = self.sqs.create_queue(QueueName=name)
        q_url = resp["QueueUrl"]
        self._queues.append(q_url)
        q_arn = self.get_queue_arn(q_url)
        return q_url, q_arn

    # ─── Cleanup ──────────────────────────────────────────────────────

    def cleanup(self) -> None:
        """Clean up all managed resources in the correct dependency order."""
        # Remove rule targets and rules first
        for rule in self._rules:
            try:
                targets = self.events.list_targets_by_rule(
                    Rule=rule.name, EventBusName=rule.bus_name
                ).get("Targets", [])
                if targets:
                    self.events.remove_targets(
                        Rule=rule.name,
                        EventBusName=rule.bus_name,
                        Ids=[t["Id"] for t in targets],
                    )
                self.events.delete_rule(Name=rule.name, EventBusName=rule.bus_name)
            except Exception:
                pass  # best-effort cleanup

        # Delete buses
        for bus_name in self._buses:
            try:
                self.events.delete_event_bus(Name=bus_name)
            except Exception:
                pass  # best-effort cleanup

        # Unsubscribe SNS subscriptions
        for sub_arn in self._subscriptions:
            try:
                if sub_arn != "PendingConfirmation":
                    self.sns.unsubscribe(SubscriptionArn=sub_arn)
            except Exception:
                pass  # best-effort cleanup

        # Delete SNS topics
        for topic_arn in self._topics:
            try:
                self.sns.delete_topic(TopicArn=topic_arn)
            except Exception:
                pass  # best-effort cleanup

        # Delete SQS queues
        for q_url in self._queues:
            try:
                self.sqs.delete_queue(QueueUrl=q_url)
            except Exception:
                pass  # best-effort cleanup

        # Delete schema table
        if self._schema_table:
            try:
                self.dynamodb.delete_table(TableName=self._schema_table)
            except Exception:
                pass  # best-effort cleanup

        self._buses.clear()
        self._rules.clear()
        self._topics.clear()
        self._queues.clear()
        self._subscriptions.clear()
        self._fan_outs.clear()
        self._stats.clear()
        self._event_archive.clear()
        self._schema_table = None
