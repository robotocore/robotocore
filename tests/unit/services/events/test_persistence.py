"""Unit tests for EventBridge native state persistence."""

import json
import logging
from unittest.mock import patch

import pytest

from robotocore.services.events.models import EventsStore
from robotocore.services.events.provider import (
    _create_api_destination,
    _create_connection,
    _create_endpoint,
    _describe_api_destination,
    _describe_connection,
    _endpoints,
    _get_store,
    _reset_for_tests,
    export_state,
    load_state,
    register_state_handler,
)
from robotocore.services.events.rule_scheduler import EventBridgeRuleScheduler
from robotocore.state.manager import StateManager


@pytest.fixture(autouse=True)
def _clear_events_state(reset_state_manager_singleton_fixture):
    """Reset EventBridge globals and state-manager singleton around each test."""
    reset_state_manager_singleton_fixture()
    _reset_for_tests()
    yield
    _reset_for_tests()
    reset_state_manager_singleton_fixture()


class TestEventBridgeRoundTrip:
    def test_round_trip_preserves_buses_rules_targets_archives_replays_and_tags(self):
        store = _get_store("us-east-1", "111111111111")
        bus = store.create_event_bus("orders", "us-east-1", "111111111111")
        rule = store.put_rule(
            "route-orders",
            "orders",
            "us-east-1",
            "111111111111",
            event_pattern={"source": ["app.orders"]},
            description="route order events",
        )
        store.put_targets(
            "route-orders",
            "orders",
            [
                {
                    "Id": "orders-target",
                    "Arn": "arn:aws:sqs:us-east-1:111111111111:orders-queue",
                    "InputTransformer": {
                        "InputPathsMap": {"orderId": "$.detail.orderId"},
                        "InputTemplate": '{"orderId": <orderId>}',
                    },
                    "DeadLetterConfig": {"Arn": "arn:aws:sqs:us-east-1:111111111111:orders-dlq"},
                }
            ],
        )
        store.tag_resource(bus.arn, [{"Key": "env", "Value": "test"}])
        store.tag_resource(rule.arn, [{"Key": "team", "Value": "platform"}])

        archive = store.create_archive(
            "orders-archive",
            bus.arn,
            "us-east-1",
            "111111111111",
            description="archive order events",
            event_pattern={"detail-type": ["OrderCreated"]},
        )
        event = {
            "version": "0",
            "id": "evt-1",
            "source": "app.orders",
            "account": "111111111111",
            "time": "2026-04-21T00:00:00Z",
            "region": "us-east-1",
            "resources": [],
            "detail-type": "OrderCreated",
            "detail": {"orderId": "o-1"},
        }
        store.archive_event(event, "orders")
        replay = store.create_replay(
            "orders-replay",
            archive.arn,
            "us-east-1",
            "111111111111",
            destination_arn=bus.arn,
            start_time=0,
            end_time=9999999999,
        )
        replay.state = "COMPLETED"
        replay.events_replayed = 1

        snapshot = export_state()
        assert snapshot["schema_version"] == 1
        assert (
            snapshot["stores"]["111111111111"]["us-east-1"]["buses"]["orders"]["name"] == "orders"
        )

        load_state(snapshot)

        restored_store = _get_store("us-east-1", "111111111111")
        restored_bus = restored_store.get_bus("orders")
        assert restored_bus is not None
        assert restored_store.get_bus("default") is not None

        restored_rule = restored_store.get_rule("route-orders", "orders")
        assert restored_rule is not None
        assert restored_rule.description == "route order events"
        assert restored_rule.event_pattern == {"source": ["app.orders"]}

        restored_target = restored_rule.targets["orders-target"]
        assert restored_target.input_transformer == {
            "InputPathsMap": {"orderId": "$.detail.orderId"},
            "InputTemplate": '{"orderId": <orderId>}',
        }
        assert restored_target.dead_letter_config == {
            "Arn": "arn:aws:sqs:us-east-1:111111111111:orders-dlq"
        }

        restored_archive = restored_store.get_archive("orders-archive")
        assert restored_archive is not None
        assert restored_archive.description == "archive order events"
        assert restored_archive.events == [event]
        assert restored_archive.event_count == 1
        assert restored_archive.size_bytes == len(json.dumps(event).encode())

        restored_replay = restored_store.get_replay("orders-replay")
        assert restored_replay is not None
        assert restored_replay.archive_arn == archive.arn
        assert restored_replay.events_replayed == 1

        assert restored_store.list_tags_for_resource(bus.arn) == [{"Key": "env", "Value": "test"}]
        assert restored_store.list_tags_for_resource(rule.arn) == [
            {"Key": "team", "Value": "platform"}
        ]

    def test_round_trip_preserves_multi_account_and_region_isolation(self):
        east = _get_store("us-east-1", "111111111111")
        west = _get_store("eu-west-1", "111111111111")
        other = _get_store("us-east-1", "222222222222")

        east.create_event_bus("shared-name", "us-east-1", "111111111111")
        west.create_event_bus("shared-name", "eu-west-1", "111111111111")
        other.create_event_bus("shared-name", "us-east-1", "222222222222")

        load_state(export_state())

        restored_east = _get_store("us-east-1", "111111111111")
        restored_west = _get_store("eu-west-1", "111111111111")
        restored_other = _get_store("us-east-1", "222222222222")

        assert restored_east.get_bus("shared-name") is not None
        assert restored_east.get_bus("shared-name").arn == (
            "arn:aws:events:us-east-1:111111111111:event-bus/shared-name"
        )
        assert restored_west.get_bus("shared-name") is not None
        assert restored_west.get_bus("shared-name").arn == (
            "arn:aws:events:eu-west-1:111111111111:event-bus/shared-name"
        )
        assert restored_other.get_bus("shared-name") is not None
        assert restored_other.get_bus("shared-name").arn == (
            "arn:aws:events:us-east-1:222222222222:event-bus/shared-name"
        )

    def test_scheduled_rule_still_fires_after_restore(self):
        store = _get_store("us-east-1", "111111111111")
        store.put_rule(
            "scheduled-rule",
            "default",
            "us-east-1",
            "111111111111",
            schedule_expression="rate(1 minute)",
        )
        store.put_targets(
            "scheduled-rule",
            "default",
            [{"Id": "scheduled-target", "Arn": "arn:aws:sqs:us-east-1:111111111111:q"}],
        )

        load_state(export_state())

        scheduler = EventBridgeRuleScheduler()
        with patch("robotocore.services.events.provider._dispatch_to_targets") as mock_dispatch:
            # Scheduler cache is intentionally not persisted; after restore it starts fresh.
            scheduler._check_all_rules()

        assert mock_dispatch.call_count == 1
        rule, event, region, account_id, restored_store = mock_dispatch.call_args[0]
        assert rule.name == "scheduled-rule"
        assert event["detail-type"] == "Scheduled Event"
        assert region == "us-east-1"
        assert account_id == "111111111111"
        assert restored_store.get_rule("scheduled-rule") is not None

    @pytest.mark.parametrize("snapshot", [None, {}])
    def test_load_state_with_empty_snapshot_clears_existing_state(self, snapshot):
        store = _get_store("us-east-1", "111111111111")
        store.create_event_bus("temporary", "us-east-1", "111111111111")

        load_state(snapshot)

        restored_store = _get_store("us-east-1", "111111111111")
        assert restored_store.get_bus("temporary") is None
        assert restored_store.get_bus("default") is not None

    def test_load_state_replaces_existing_state(self):
        original = _get_store("us-east-1", "111111111111")
        original.create_event_bus("bus-a", "us-east-1", "111111111111")
        snapshot = export_state()

        mutated = _get_store("us-east-1", "111111111111")
        mutated.create_event_bus("bus-b", "us-east-1", "111111111111")
        _get_store("eu-west-1", "222222222222").create_event_bus(
            "other-bus",
            "eu-west-1",
            "222222222222",
        )

        load_state(snapshot)

        restored = _get_store("us-east-1", "111111111111")
        assert restored.get_bus("bus-a") is not None
        assert restored.get_bus("bus-b") is None
        assert _get_store("eu-west-1", "222222222222").get_bus("other-bus") is None

    def test_load_state_ignores_unknown_top_level_keys(self):
        store = _get_store("us-east-1", "111111111111")
        store.create_event_bus("known-bus", "us-east-1", "111111111111")
        snapshot = export_state()
        snapshot["unknown_future_key"] = {"version": 2}

        load_state(snapshot)

        restored = _get_store("us-east-1", "111111111111")
        assert restored.get_bus("known-bus") is not None

    def test_load_state_warns_on_future_schema_version(self, caplog):
        store = _get_store("us-east-1", "111111111111")
        store.create_event_bus("versioned-bus", "us-east-1", "111111111111")
        snapshot = export_state()
        snapshot["schema_version"] = 2

        with caplog.at_level(logging.WARNING):
            load_state(snapshot)

        assert "events snapshot schema_version=2; expected 1" in caplog.text
        restored = _get_store("us-east-1", "111111111111")
        assert restored.get_bus("versioned-bus") is not None

    def test_from_snapshot_raises_on_default_bus_identity_mismatch(self):
        snapshot = {
            "buses": {
                "default": {
                    "name": "default",
                    "region": "eu-west-1",
                    "account_id": "111111111111",
                    "rules": {},
                }
            },
            "archives": {},
            "replays": {},
            "tags": {},
        }

        with pytest.raises(
            ValueError,
            match=(
                "default bus identity 111111111111/eu-west-1 does not match store key "
                "111111111111/us-east-1"
            ),
        ):
            EventsStore.from_snapshot(snapshot, region="us-east-1", account_id="111111111111")


class TestDiskRoundTripViaStateManager:
    def test_global_connections_destinations_and_endpoints_survive_disk_round_trip(self, tmp_path):
        store = _get_store("ap-southeast-2", "333333333333")

        connection = _create_connection(
            store,
            {
                "Name": "erp-connection",
                "AuthorizationType": "API_KEY",
                "AuthParameters": {
                    "ApiKeyAuthParameters": {
                        "ApiKeyName": "x-api-key",
                        "ApiKeyValue": "secret",
                    }
                },
            },
            "ap-southeast-2",
            "333333333333",
        )
        api_destination = _create_api_destination(
            store,
            {
                "Name": "erp-destination",
                "ConnectionArn": connection["ConnectionArn"],
                "InvocationEndpoint": "https://example.test/orders",
                "HttpMethod": "POST",
                "InvocationRateLimitPerSecond": 42,
            },
            "ap-southeast-2",
            "333333333333",
        )
        endpoint = _create_endpoint(
            store,
            {
                "Name": "replica-endpoint",
                "RoutingConfig": {"FailoverConfig": {"Primary": {"HealthCheck": "ok"}}},
                "ReplicationConfig": {"State": "ENABLED"},
                "EventBuses": [
                    {
                        "EventBusArn": (
                            "arn:aws:events:ap-southeast-2:333333333333:event-bus/default"
                        )
                    }
                ],
                "RoleArn": "arn:aws:iam::333333333333:role/events-role",
                "Description": "replica endpoint",
            },
            "ap-southeast-2",
            "333333333333",
        )

        manager = StateManager(state_dir=str(tmp_path))
        register_state_handler(manager)
        manager.save(name="events-cache", services=["events"])

        _reset_for_tests()
        manager.load(name="events-cache", services=["events"])

        snapshot = export_state()
        assert snapshot["schema_version"] == 1
        assert "connections" in snapshot
        assert "api_destinations" in snapshot
        assert "endpoints" in snapshot

        restored_connection = _describe_connection(
            _get_store("ap-southeast-2", "333333333333"),
            {"Name": "erp-connection"},
            "ap-southeast-2",
            "333333333333",
        )
        restored_destination = _describe_api_destination(
            _get_store("ap-southeast-2", "333333333333"),
            {"Name": "erp-destination"},
            "ap-southeast-2",
            "333333333333",
        )

        assert restored_connection["ConnectionArn"] == connection["ConnectionArn"]
        assert restored_connection["AuthorizationType"] == "API_KEY"
        assert restored_destination["ApiDestinationArn"] == api_destination["ApiDestinationArn"]
        assert restored_destination["InvocationRateLimitPerSecond"] == 42
        assert _endpoints["replica-endpoint"]["EndpointArn"] == endpoint["Arn"]

    def test_future_snapshot_fields_on_dataclasses_are_ignored(self):
        store = _get_store("us-east-1", "111111111111")
        bus = store.create_event_bus("future-bus", "us-east-1", "111111111111")
        store.put_rule(
            "future-rule",
            "future-bus",
            "us-east-1",
            "111111111111",
            event_pattern={"source": ["future.app"]},
        )
        store.put_targets(
            "future-rule",
            "future-bus",
            [{"Id": "future-target", "Arn": "arn:aws:sqs:us-east-1:111111111111:future-q"}],
        )
        archive = store.create_archive(
            "future-archive",
            bus.arn,
            "us-east-1",
            "111111111111",
        )
        store.create_replay(
            "future-replay",
            archive.arn,
            "us-east-1",
            "111111111111",
            destination_arn=bus.arn,
            start_time=0,
            end_time=1,
        )

        snapshot = export_state()
        bus_snapshot = snapshot["stores"]["111111111111"]["us-east-1"]["buses"]["future-bus"]
        rule_snapshot = bus_snapshot["rules"]["future-rule"]
        target_snapshot = rule_snapshot["targets"]["future-target"]
        archive_snapshot = snapshot["stores"]["111111111111"]["us-east-1"]["archives"][
            "future-archive"
        ]
        replay_snapshot = snapshot["stores"]["111111111111"]["us-east-1"]["replays"][
            "future-replay"
        ]

        bus_snapshot["future_bus_field"] = True
        rule_snapshot["future_rule_field"] = True
        target_snapshot["future_target_field"] = True
        archive_snapshot["future_archive_field"] = True
        replay_snapshot["future_replay_field"] = True

        load_state(snapshot)

        restored = _get_store("us-east-1", "111111111111")
        assert restored.get_bus("future-bus") is not None
        assert restored.get_rule("future-rule", "future-bus") is not None
        assert restored.get_archive("future-archive") is not None
        assert restored.get_replay("future-replay") is not None
