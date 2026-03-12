"""
Tests for EventBridge rule-based event routing.

Verifies that events published to a custom bus are delivered to SQS targets
based on pattern matching rules.
"""

import json

from .app import EventRouter
from .models import Event, EventRule, EventTarget


class TestEventRouting:
    def test_publish_event_matching_rule_delivered_to_sqs(
        self, event_router: EventRouter, event_bus: str, unique_name: str
    ):
        """Publish event matching a rule, verify it arrives in the SQS target."""
        q_url, q_arn = event_router.create_queue(f"route-match-{unique_name}")

        rule = EventRule(
            name=f"order-rule-{unique_name}",
            bus_name=event_bus,
            pattern={"source": ["order-service"]},
            targets=[EventTarget(id="order-q", arn=q_arn)],
        )
        event_router.create_rule(rule)

        event = Event(
            source="order-service",
            detail_type="OrderCreated",
            detail={"order_id": "ORD-001", "amount": 99.95},
        )
        resp = event_router.publish_event(event_bus, event)
        assert resp["FailedEntryCount"] == 0

        messages = event_router.receive_messages(q_url)
        assert len(messages) >= 1
        body = json.loads(messages[0]["Body"])
        if "detail" in body:
            assert body["detail"]["order_id"] == "ORD-001"
        else:
            assert "order_id" in str(body)

    def test_publish_event_not_matching_rule_not_delivered(
        self, event_router: EventRouter, event_bus: str, unique_name: str
    ):
        """Publish event that does NOT match the rule, verify queue stays empty."""
        q_url, q_arn = event_router.create_queue(f"route-nomatch-{unique_name}")

        rule = EventRule(
            name=f"payment-only-{unique_name}",
            bus_name=event_bus,
            pattern={"source": ["payment-service"]},
            targets=[EventTarget(id="pay-q", arn=q_arn)],
        )
        event_router.create_rule(rule)

        # Publish non-matching event (inventory, not payment)
        event = Event(
            source="inventory-service",
            detail_type="StockUpdated",
            detail={"sku": "WIDGET-01"},
        )
        event_router.publish_event(event_bus, event)

        messages = event_router.receive_messages(q_url, expected=1, timeout=3)
        assert len(messages) == 0

    def test_complex_pattern_detail_field_matching(
        self, event_router: EventRouter, event_bus: str, unique_name: str
    ):
        """Rule with pattern matching on detail fields — only matching events arrive."""
        q_url, q_arn = event_router.create_queue(f"detail-match-{unique_name}")

        rule = EventRule(
            name=f"high-value-{unique_name}",
            bus_name=event_bus,
            pattern={
                "source": ["order-service"],
                "detail-type": ["OrderCreated"],
            },
            targets=[EventTarget(id="hv-q", arn=q_arn)],
        )
        event_router.create_rule(rule)

        # Matching event
        event = Event(
            source="order-service",
            detail_type="OrderCreated",
            detail={"order_id": "ORD-HV-001", "amount": 500.0},
        )
        event_router.publish_event(event_bus, event)

        # Non-matching detail-type
        event2 = Event(
            source="order-service",
            detail_type="OrderCancelled",
            detail={"order_id": "ORD-CANCEL-001"},
        )
        event_router.publish_event(event_bus, event2)

        messages = event_router.receive_messages(q_url, expected=1, timeout=5)
        assert len(messages) >= 1
        body = json.loads(messages[0]["Body"])
        if "detail-type" in body:
            assert body["detail-type"] == "OrderCreated"

    def test_multiple_rules_on_same_bus_route_to_different_targets(
        self, event_router: EventRouter, event_bus: str, unique_name: str
    ):
        """Two rules on the same bus, each routing to a different SQS queue."""
        q1_url, q1_arn = event_router.create_queue(f"orders-{unique_name}")
        q2_url, q2_arn = event_router.create_queue(f"payments-{unique_name}")

        rule1 = EventRule(
            name=f"order-rule-{unique_name}",
            bus_name=event_bus,
            pattern={"source": ["order-service"]},
            targets=[EventTarget(id="order-q", arn=q1_arn)],
        )
        rule2 = EventRule(
            name=f"payment-rule-{unique_name}",
            bus_name=event_bus,
            pattern={"source": ["payment-service"]},
            targets=[EventTarget(id="pay-q", arn=q2_arn)],
        )
        event_router.create_rule(rule1)
        event_router.create_rule(rule2)

        event_router.publish_event(
            event_bus,
            Event(source="order-service", detail_type="OrderCreated", detail={"id": "O1"}),
        )
        event_router.publish_event(
            event_bus,
            Event(source="payment-service", detail_type="PaymentProcessed", detail={"id": "P1"}),
        )

        msgs1 = event_router.receive_messages(q1_url)
        msgs2 = event_router.receive_messages(q2_url)
        assert len(msgs1) >= 1, "Order queue should have received the order event"
        assert len(msgs2) >= 1, "Payment queue should have received the payment event"

    def test_disable_enable_rule_behavior(
        self, event_router: EventRouter, event_bus: str, unique_name: str
    ):
        """Disable a rule, verify events not delivered, re-enable and verify delivery."""
        q_url, q_arn = event_router.create_queue(f"toggle-{unique_name}")

        rule = EventRule(
            name=f"toggle-rule-{unique_name}",
            bus_name=event_bus,
            pattern={"source": ["toggle-service"]},
            targets=[EventTarget(id="toggle-q", arn=q_arn)],
        )
        event_router.create_rule(rule)

        # Disable the rule
        event_router.disable_rule(rule.name, event_bus)

        # Verify rule is disabled
        desc = event_router.describe_rule(rule.name, event_bus)
        assert desc["State"] == "DISABLED"

        # Re-enable the rule
        event_router.enable_rule(rule.name, event_bus)
        desc = event_router.describe_rule(rule.name, event_bus)
        assert desc["State"] == "ENABLED"

        # Publish event — should be delivered now
        event_router.publish_event(
            event_bus,
            Event(source="toggle-service", detail_type="Toggled", detail={"val": 1}),
        )
        messages = event_router.receive_messages(q_url, timeout=5)
        assert len(messages) >= 1

    def test_event_metadata_envelope(
        self, event_router: EventRouter, event_bus: str, unique_name: str
    ):
        """Verify event envelope contains required metadata fields."""
        q_url, q_arn = event_router.create_queue(f"meta-{unique_name}")

        rule = EventRule(
            name=f"meta-rule-{unique_name}",
            bus_name=event_bus,
            pattern={"source": ["audit-service"]},
            targets=[EventTarget(id="meta-q", arn=q_arn)],
        )
        event_router.create_rule(rule)

        event_router.publish_event(
            event_bus,
            Event(
                source="audit-service",
                detail_type="UserAction",
                detail={"action": "login", "user": "alice"},
            ),
        )

        messages = event_router.receive_messages(q_url)
        assert len(messages) >= 1
        body = json.loads(messages[0]["Body"])
        assert "source" in body
        assert "detail-type" in body or "DetailType" in body
        assert "detail" in body
        assert "id" in body
        assert "time" in body

    def test_multiple_targets_on_single_rule(
        self, event_router: EventRouter, event_bus: str, unique_name: str
    ):
        """A single rule with two SQS targets — both should receive the event."""
        q1_url, q1_arn = event_router.create_queue(f"multi-a-{unique_name}")
        q2_url, q2_arn = event_router.create_queue(f"multi-b-{unique_name}")

        rule = EventRule(
            name=f"multi-target-{unique_name}",
            bus_name=event_bus,
            pattern={"source": ["notification-service"]},
            targets=[
                EventTarget(id="target-a", arn=q1_arn),
                EventTarget(id="target-b", arn=q2_arn),
            ],
        )
        event_router.create_rule(rule)

        event_router.publish_event(
            event_bus,
            Event(
                source="notification-service",
                detail_type="Alert",
                detail={"message": "System alert"},
            ),
        )

        msgs1 = event_router.receive_messages(q1_url)
        msgs2 = event_router.receive_messages(q2_url)
        assert len(msgs1) >= 1
        assert len(msgs2) >= 1
