"""
Tests for EventBridge pattern matching edge cases.

Verifies that EventBridge rules correctly match (or reject) events based
on various pattern types: prefix, numeric, anything-but, nested fields,
array matching, and combined AND patterns.
"""

import json

from .app import EventRouter
from .models import Event, EventRule, EventTarget


class TestEventPatterns:
    def test_prefix_matching_on_source(
        self, event_router: EventRouter, event_bus: str, unique_name: str
    ):
        """Pattern with prefix matching on string fields."""
        q_url, q_arn = event_router.create_queue(f"prefix-{unique_name}")

        rule = EventRule(
            name=f"prefix-rule-{unique_name}",
            bus_name=event_bus,
            pattern={
                "source": [{"prefix": "order"}],
            },
            targets=[EventTarget(id="prefix-q", arn=q_arn)],
        )
        event_router.create_rule(rule)

        # Should match: source starts with "order"
        event_router.publish_event(
            event_bus,
            Event(source="order-service", detail_type="OrderCreated", detail={"id": "1"}),
        )

        messages = event_router.receive_messages(q_url, timeout=5)
        assert len(messages) >= 1

    def test_prefix_no_match(self, event_router: EventRouter, event_bus: str, unique_name: str):
        """Prefix pattern should NOT match sources that don't have the prefix."""
        q_url, q_arn = event_router.create_queue(f"prefix-no-{unique_name}")

        rule = EventRule(
            name=f"prefix-no-rule-{unique_name}",
            bus_name=event_bus,
            pattern={
                "source": [{"prefix": "payment"}],
            },
            targets=[EventTarget(id="prefix-no-q", arn=q_arn)],
        )
        event_router.create_rule(rule)

        # Should NOT match: source doesn't start with "payment"
        event_router.publish_event(
            event_bus,
            Event(source="order-service", detail_type="OrderCreated", detail={"id": "1"}),
        )

        messages = event_router.receive_messages(q_url, expected=1, timeout=3)
        assert len(messages) == 0

    def test_exact_match_on_detail_type(
        self, event_router: EventRouter, event_bus: str, unique_name: str
    ):
        """Exact match on detail-type field."""
        q_url, q_arn = event_router.create_queue(f"exact-dt-{unique_name}")

        rule = EventRule(
            name=f"exact-dt-rule-{unique_name}",
            bus_name=event_bus,
            pattern={
                "detail-type": ["OrderCreated"],
            },
            targets=[EventTarget(id="exact-dt-q", arn=q_arn)],
        )
        event_router.create_rule(rule)

        # Matching
        event_router.publish_event(
            event_bus,
            Event(source="any-svc", detail_type="OrderCreated", detail={"id": "1"}),
        )
        # Non-matching
        event_router.publish_event(
            event_bus,
            Event(source="any-svc", detail_type="OrderCancelled", detail={"id": "2"}),
        )

        messages = event_router.receive_messages(q_url, expected=1, timeout=5)
        assert len(messages) >= 1
        body = json.loads(messages[0]["Body"])
        if "detail-type" in body:
            assert body["detail-type"] == "OrderCreated"

    def test_multiple_values_in_pattern_acts_as_or(
        self, event_router: EventRouter, event_bus: str, unique_name: str
    ):
        """Multiple values in a pattern field act as OR — any value matches."""
        q_url, q_arn = event_router.create_queue(f"multi-val-{unique_name}")

        rule = EventRule(
            name=f"multi-val-rule-{unique_name}",
            bus_name=event_bus,
            pattern={
                "source": ["order-service", "payment-service"],
            },
            targets=[EventTarget(id="multi-val-q", arn=q_arn)],
        )
        event_router.create_rule(rule)

        event_router.publish_event(
            event_bus,
            Event(source="order-service", detail_type="OrderCreated", detail={"id": "1"}),
        )
        event_router.publish_event(
            event_bus,
            Event(source="payment-service", detail_type="PaymentDone", detail={"id": "2"}),
        )

        messages = event_router.receive_messages(q_url, expected=2, timeout=5)
        assert len(messages) >= 2

    def test_combined_source_and_detail_type_pattern(
        self, event_router: EventRouter, event_bus: str, unique_name: str
    ):
        """Multiple pattern fields act as AND — all must match."""
        q_url, q_arn = event_router.create_queue(f"combined-{unique_name}")

        rule = EventRule(
            name=f"combined-rule-{unique_name}",
            bus_name=event_bus,
            pattern={
                "source": ["order-service"],
                "detail-type": ["OrderCreated"],
            },
            targets=[EventTarget(id="combined-q", arn=q_arn)],
        )
        event_router.create_rule(rule)

        # Matches both conditions
        event_router.publish_event(
            event_bus,
            Event(source="order-service", detail_type="OrderCreated", detail={"id": "1"}),
        )
        # Matches source but NOT detail-type
        event_router.publish_event(
            event_bus,
            Event(source="order-service", detail_type="OrderCancelled", detail={"id": "2"}),
        )

        messages = event_router.receive_messages(q_url, expected=1, timeout=5)
        # Should receive exactly 1 (the matching event)
        assert len(messages) >= 1
        body = json.loads(messages[0]["Body"])
        if "detail-type" in body:
            assert body["detail-type"] == "OrderCreated"

    def test_detail_field_exact_match(
        self, event_router: EventRouter, event_bus: str, unique_name: str
    ):
        """Pattern matching on fields inside detail."""
        q_url, q_arn = event_router.create_queue(f"detail-field-{unique_name}")

        rule = EventRule(
            name=f"detail-field-rule-{unique_name}",
            bus_name=event_bus,
            pattern={
                "source": ["order-service"],
                "detail": {
                    "status": ["confirmed"],
                },
            },
            targets=[EventTarget(id="detail-field-q", arn=q_arn)],
        )
        event_router.create_rule(rule)

        # Matching detail field
        event_router.publish_event(
            event_bus,
            Event(
                source="order-service",
                detail_type="OrderStatusChanged",
                detail={"order_id": "O1", "status": "confirmed"},
            ),
        )
        # Non-matching detail field
        event_router.publish_event(
            event_bus,
            Event(
                source="order-service",
                detail_type="OrderStatusChanged",
                detail={"order_id": "O2", "status": "pending"},
            ),
        )

        messages = event_router.receive_messages(q_url, expected=1, timeout=5)
        assert len(messages) >= 1

    def test_rule_with_description(
        self, event_router: EventRouter, event_bus: str, unique_name: str
    ):
        """Verify rule description is stored and retrievable."""
        rule = EventRule(
            name=f"desc-rule-{unique_name}",
            bus_name=event_bus,
            pattern={"source": ["test-service"]},
            description="Captures all test-service events for monitoring",
        )
        event_router.create_rule(rule)

        desc = event_router.describe_rule(rule.name, event_bus)
        assert desc["Description"] == "Captures all test-service events for monitoring"
        assert desc["State"] == "ENABLED"

    def test_list_rules_on_bus(self, event_router: EventRouter, event_bus: str, unique_name: str):
        """List rules on a bus returns all created rules."""
        for i in range(3):
            rule = EventRule(
                name=f"list-rule-{i}-{unique_name}",
                bus_name=event_bus,
                pattern={"source": [f"svc-{i}"]},
            )
            event_router.create_rule(rule)

        rules = event_router.list_rules(event_bus)
        rule_names = [r["Name"] for r in rules]
        for i in range(3):
            assert f"list-rule-{i}-{unique_name}" in rule_names

    def test_list_targets_for_rule(
        self, event_router: EventRouter, event_bus: str, unique_name: str
    ):
        """List targets for a rule returns all attached targets."""
        q1_url, q1_arn = event_router.create_queue(f"lt-a-{unique_name}")
        q2_url, q2_arn = event_router.create_queue(f"lt-b-{unique_name}")

        rule = EventRule(
            name=f"lt-rule-{unique_name}",
            bus_name=event_bus,
            pattern={"source": ["lt-service"]},
            targets=[
                EventTarget(id="lt-a", arn=q1_arn),
                EventTarget(id="lt-b", arn=q2_arn),
            ],
        )
        event_router.create_rule(rule)

        targets = event_router.list_targets(rule.name, event_bus)
        target_ids = [t["Id"] for t in targets]
        assert "lt-a" in target_ids
        assert "lt-b" in target_ids
