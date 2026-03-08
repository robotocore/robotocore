"""Unit tests for expanded EventBridge features: 17 target types, InputTransformer,
DLQ, and archive/replay.
"""

import json
import time
import uuid
from unittest.mock import MagicMock, patch

import pytest

from robotocore.services.events.models import (
    EventArchive,
    EventReplay,
    EventRule,
    EventsStore,
    EventTarget,
)
from robotocore.services.events.provider import (
    _apply_input_transformer,
    _dispatch_to_targets,
    _get_store,
    _invoke_target,
    _resolve_jsonpath,
    _send_to_dlq,
    _stores,
    clear_invocation_log,
    get_invocation_log,
    handle_events_request,
)

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_request(operation: str, body: dict):
    """Build a fake Starlette Request."""
    from starlette.requests import Request

    target = f"AWSEvents.{operation}"
    scope = {
        "type": "http",
        "method": "POST",
        "path": "/",
        "query_string": b"",
        "headers": [(b"x-amz-target", target.encode())],
    }
    body_bytes = json.dumps(body).encode()

    async def receive():
        return {"type": "http.request", "body": body_bytes}

    return Request(scope, receive)


REGION = "us-east-1"
ACCOUNT = "123456789012"

SAMPLE_EVENT = {
    "version": "0",
    "id": str(uuid.uuid4()),
    "source": "myapp.orders",
    "detail-type": "OrderPlaced",
    "detail": {"order_id": "12345", "amount": 99.50},
    "account": ACCOUNT,
    "region": REGION,
    "time": "2026-01-01T00:00:00Z",
    "resources": [],
}


@pytest.fixture(autouse=True)
def _clear_state():
    """Reset global stores between tests."""
    _stores.clear()
    clear_invocation_log()
    yield
    _stores.clear()
    clear_invocation_log()


# ---------------------------------------------------------------------------
# InputTransformer
# ---------------------------------------------------------------------------


class TestInputTransformer:
    def test_basic_template_replacement(self):
        transformer = {
            "InputPathsMap": {"orderId": "$.detail.order_id"},
            "InputTemplate": '{"id": "<orderId>"}',
        }
        result = _apply_input_transformer(transformer, SAMPLE_EVENT)
        assert result == '{"id": "12345"}'

    def test_multiple_placeholders(self):
        transformer = {
            "InputPathsMap": {
                "orderId": "$.detail.order_id",
                "src": "$.source",
            },
            "InputTemplate": "Order <orderId> from <src>",
        }
        result = _apply_input_transformer(transformer, SAMPLE_EVENT)
        assert result == "Order 12345 from myapp.orders"

    def test_empty_paths_map(self):
        transformer = {
            "InputPathsMap": {},
            "InputTemplate": "static text",
        }
        result = _apply_input_transformer(transformer, SAMPLE_EVENT)
        assert result == "static text"

    def test_whole_event_path(self):
        transformer = {
            "InputPathsMap": {"all": "$"},
            "InputTemplate": "<all>",
        }
        result = _apply_input_transformer(transformer, SAMPLE_EVENT)
        parsed = json.loads(result)
        assert parsed["source"] == "myapp.orders"

    def test_nested_path(self):
        transformer = {
            "InputPathsMap": {"amt": "$.detail.amount"},
            "InputTemplate": "Amount: <amt>",
        }
        result = _apply_input_transformer(transformer, SAMPLE_EVENT)
        assert result == "Amount: 99.5"

    def test_missing_path_returns_null(self):
        transformer = {
            "InputPathsMap": {"x": "$.nonexistent.path"},
            "InputTemplate": "val=<x>",
        }
        result = _apply_input_transformer(transformer, SAMPLE_EVENT)
        assert result == "val=null"

    def test_input_transformer_used_by_invoke_target(self):
        """When target has input_transformer, _invoke_target uses it."""
        transformer = {
            "InputPathsMap": {"src": "$.source"},
            "InputTemplate": '{"source": <src>}',
        }
        target = EventTarget(
            target_id="t1",
            arn="arn:aws:lambda:us-east-1:123:function:f",
            input_transformer=transformer,
        )
        with patch("robotocore.services.events.provider._invoke_lambda_target") as mock:
            _invoke_target(target, SAMPLE_EVENT, REGION, ACCOUNT)
            payload = mock.call_args[0][1]
            parsed = json.loads(payload)
            assert parsed["source"] == "myapp.orders"

    def test_input_transformer_takes_precedence_over_input(self):
        """InputTransformer should take precedence over target.input."""
        transformer = {
            "InputPathsMap": {},
            "InputTemplate": "transformed",
        }
        target = EventTarget(
            target_id="t1",
            arn="arn:aws:lambda:us-east-1:123:function:f",
            input="should not be used",
            input_transformer=transformer,
        )
        with patch("robotocore.services.events.provider._invoke_lambda_target") as mock:
            _invoke_target(target, SAMPLE_EVENT, REGION, ACCOUNT)
            assert mock.call_args[0][1] == "transformed"


class TestResolveJsonpath:
    def test_root_path(self):
        result = _resolve_jsonpath("$", {"a": 1})
        assert json.loads(result) == {"a": 1}

    def test_simple_field(self):
        assert _resolve_jsonpath("$.source", SAMPLE_EVENT) == "myapp.orders"

    def test_nested_field(self):
        assert _resolve_jsonpath("$.detail.order_id", SAMPLE_EVENT) == "12345"

    def test_numeric_value(self):
        result = _resolve_jsonpath("$.detail.amount", SAMPLE_EVENT)
        assert result == "99.5"

    def test_missing_field(self):
        assert _resolve_jsonpath("$.nope", SAMPLE_EVENT) == "null"

    def test_invalid_path(self):
        result = _resolve_jsonpath("invalid", {"a": 1})
        parsed = json.loads(result)
        assert parsed == {"a": 1}


# ---------------------------------------------------------------------------
# New target types
# ---------------------------------------------------------------------------


class TestKinesisTarget:
    def test_routes_kinesis_arn(self):
        target = EventTarget(
            target_id="t1",
            arn="arn:aws:kinesis:us-east-1:123:stream/my-stream",
        )
        with patch("robotocore.services.events.provider._invoke_kinesis_target") as mock:
            _invoke_target(target, SAMPLE_EVENT, REGION, ACCOUNT)
            mock.assert_called_once()

    def test_kinesis_put_record(self):
        from robotocore.services.kinesis.models import (
            _get_store,
        )

        kin_store = _get_store(REGION)
        kin_store.create_stream("my-stream", 1, REGION, ACCOUNT)

        target = EventTarget(
            target_id="t1",
            arn="arn:aws:kinesis:us-east-1:123:stream/my-stream",
        )
        _invoke_target(target, SAMPLE_EVENT, REGION, ACCOUNT)

        log = get_invocation_log()
        assert any(e["target_type"] == "kinesis" for e in log)

    def test_kinesis_stream_not_found(self):
        """Should log error but not raise for missing streams."""
        target = EventTarget(
            target_id="t1",
            arn="arn:aws:kinesis:us-east-1:123:stream/nonexistent",
        )
        _invoke_target(target, SAMPLE_EVENT, REGION, ACCOUNT)
        log = get_invocation_log()
        assert any(
            e["target_type"] == "kinesis" and e.get("result", {}).get("error") == "stream_not_found"
            for e in log
        )


class TestFirehoseTarget:
    def test_routes_firehose_arn(self):
        target = EventTarget(
            target_id="t1",
            arn="arn:aws:firehose:us-east-1:123:deliverystream/my-stream",
        )
        with patch("robotocore.services.events.provider._invoke_firehose_target") as mock:
            _invoke_target(target, SAMPLE_EVENT, REGION, ACCOUNT)
            mock.assert_called_once()

    def test_firehose_put_record(self):
        from robotocore.services.firehose import provider as fh

        with fh._lock:
            fh._delivery_streams["my-fh"] = {
                "DeliveryStreamName": "my-fh",
            }
            fh._stream_buffers.setdefault("my-fh", [])

        try:
            target = EventTarget(
                target_id="t1",
                arn="arn:aws:firehose:us-east-1:123:deliverystream/my-fh",
            )
            _invoke_target(target, SAMPLE_EVENT, REGION, ACCOUNT)
            log = get_invocation_log()
            assert any(e["target_type"] == "firehose" for e in log)
        finally:
            with fh._lock:
                fh._delivery_streams.pop("my-fh", None)
                fh._stream_buffers.pop("my-fh", None)

    def test_firehose_stream_not_found(self):
        target = EventTarget(
            target_id="t1",
            arn="arn:aws:firehose:us-east-1:123:deliverystream/nope",
        )
        _invoke_target(target, SAMPLE_EVENT, REGION, ACCOUNT)
        log = get_invocation_log()
        assert any(
            e["target_type"] == "firehose"
            and e.get("result", {}).get("error") == "stream_not_found"
            for e in log
        )


class TestStepFunctionsTarget:
    def test_routes_states_arn(self):
        target = EventTarget(
            target_id="t1",
            arn="arn:aws:states:us-east-1:123:stateMachine:my-sm",
        )
        with patch("robotocore.services.events.provider._invoke_stepfunctions_target") as mock:
            _invoke_target(target, SAMPLE_EVENT, REGION, ACCOUNT)
            mock.assert_called_once()

    def test_stepfunctions_not_found(self):
        target = EventTarget(
            target_id="t1",
            arn="arn:aws:states:us-east-1:123:stateMachine:nonexistent",
        )
        _invoke_target(target, SAMPLE_EVENT, REGION, ACCOUNT)
        log = get_invocation_log()
        assert any(
            e["target_type"] == "stepfunctions"
            and e.get("result", {}).get("error") == "state_machine_not_found"
            for e in log
        )


class TestLogsTarget:
    def test_routes_logs_arn(self):
        target = EventTarget(
            target_id="t1",
            arn="arn:aws:logs:us-east-1:123:log-group:/aws/events/test:*",
        )
        with patch("robotocore.services.events.provider._invoke_logs_target") as mock:
            _invoke_target(target, SAMPLE_EVENT, REGION, ACCOUNT)
            mock.assert_called_once()

    def test_logs_put_event(self):
        """Should put a log event to CloudWatch Logs via moto backend."""
        target = EventTarget(
            target_id="t1",
            arn="arn:aws:logs:us-east-1:123456789012:log-group:/aws/events/test:*",
        )
        with patch("moto.backends.get_backend") as mock_backend:
            mock_logs = MagicMock()
            mock_backend.return_value = {ACCOUNT: {REGION: mock_logs}}
            from robotocore.services.events.provider import (
                _invoke_logs_target,
            )

            _invoke_logs_target(target.arn, json.dumps(SAMPLE_EVENT), REGION, ACCOUNT)
            mock_logs.put_log_events.assert_called_once()


class TestEcsTarget:
    def test_routes_ecs_arn(self):
        target = EventTarget(
            target_id="t1",
            arn="arn:aws:ecs:us-east-1:123:cluster/my-cluster",
        )
        with patch("robotocore.services.events.provider._invoke_ecs_target") as mock:
            _invoke_target(target, SAMPLE_EVENT, REGION, ACCOUNT)
            mock.assert_called_once()

    def test_ecs_logs_invocation(self):
        target = EventTarget(
            target_id="t1",
            arn="arn:aws:ecs:us-east-1:123:cluster/my-cluster",
        )
        _invoke_target(target, SAMPLE_EVENT, REGION, ACCOUNT)
        log = get_invocation_log()
        assert any(e["target_type"] == "ecs" for e in log)


class TestEventBridgeBusTarget:
    def test_routes_events_arn(self):
        target = EventTarget(
            target_id="t1",
            arn="arn:aws:events:us-east-1:123:event-bus/custom",
        )
        with patch("robotocore.services.events.provider._invoke_eventbridge_target") as mock:
            _invoke_target(target, SAMPLE_EVENT, REGION, ACCOUNT)
            mock.assert_called_once()

    def test_forward_to_another_bus(self):
        store = _get_store(REGION, ACCOUNT)
        store.create_event_bus("custom-bus", REGION, ACCOUNT)
        store.put_rule(
            "custom-rule",
            "custom-bus",
            REGION,
            ACCOUNT,
            event_pattern={"source": ["myapp.orders"]},
        )
        store.put_targets(
            "custom-rule",
            "custom-bus",
            [
                {
                    "Id": "final-target",
                    "Arn": "arn:aws:lambda:us-east-1:123:function:f",
                }
            ],
        )

        target = EventTarget(
            target_id="bus-fwd",
            arn="arn:aws:events:us-east-1:123:event-bus/custom-bus",
        )
        with patch("robotocore.services.events.provider._invoke_lambda_target") as mock:
            _invoke_target(target, SAMPLE_EVENT, REGION, ACCOUNT)
            mock.assert_called_once()

    def test_forward_bus_not_found(self):
        target = EventTarget(
            target_id="bus-fwd",
            arn="arn:aws:events:us-east-1:123:event-bus/nonexistent",
        )
        _invoke_target(target, SAMPLE_EVENT, REGION, ACCOUNT)
        log = get_invocation_log()
        assert any(
            e["target_type"] == "events" and e.get("result", {}).get("error") == "bus_not_found"
            for e in log
        )


class TestApiGatewayTarget:
    def test_routes_execute_api_arn(self):
        target = EventTarget(
            target_id="t1",
            arn="arn:aws:execute-api:us-east-1:123:api/stage/GET/path",
        )
        with patch("robotocore.services.events.provider._invoke_apigateway_target") as mock:
            _invoke_target(target, SAMPLE_EVENT, REGION, ACCOUNT)
            mock.assert_called_once()

    def test_apigateway_logs_invocation(self):
        target = EventTarget(
            target_id="t1",
            arn="arn:aws:execute-api:us-east-1:123:api/stage/GET/path",
        )
        _invoke_target(target, SAMPLE_EVENT, REGION, ACCOUNT)
        log = get_invocation_log()
        assert any(e["target_type"] == "apigateway" for e in log)


class TestSimulatedTargets:
    @pytest.mark.parametrize(
        "service,arn_fragment",
        [
            ("codebuild", ":codebuild:"),
            ("codepipeline", ":codepipeline:"),
            ("batch", ":batch:"),
            ("ssm", ":ssm:"),
            ("redshift", ":redshift:"),
            ("sagemaker", ":sagemaker:"),
            ("inspector", ":inspector:"),
        ],
    )
    def test_simulated_target_routing(self, service, arn_fragment):
        arn = f"arn:aws{arn_fragment}us-east-1:123:resource/name"
        target = EventTarget(target_id="t1", arn=arn)
        _invoke_target(target, SAMPLE_EVENT, REGION, ACCOUNT)
        log = get_invocation_log()
        assert any(e["target_type"] == service for e in log)

    def test_unsupported_target_logs(self):
        target = EventTarget(
            target_id="t1",
            arn="arn:aws:unknownservice:us-east-1:123:resource",
        )
        _invoke_target(target, SAMPLE_EVENT, REGION, ACCOUNT)
        log = get_invocation_log()
        assert any(e["target_type"] == "unsupported" for e in log)


# ---------------------------------------------------------------------------
# Dead Letter Queue
# ---------------------------------------------------------------------------


class TestDeadLetterQueue:
    def test_dlq_on_target_failure(self):
        """When a target fails and DLQ is configured, the event goes to DLQ."""
        from robotocore.services.sqs.models import StandardQueue
        from robotocore.services.sqs.provider import (
            _get_store as get_sqs_store,
        )

        sqs_store = get_sqs_store(REGION)
        dlq = StandardQueue(name="my-dlq", region=REGION, account_id=ACCOUNT)
        sqs_store.queues["my-dlq"] = dlq

        rule = EventRule(
            name="test-rule",
            event_bus_name="default",
            region=REGION,
            account_id=ACCOUNT,
            event_pattern={"source": ["myapp.orders"]},
        )
        rule.targets["t1"] = EventTarget(
            target_id="t1",
            arn="arn:aws:lambda:us-east-1:123:function:broken",
            dead_letter_config={"Arn": f"arn:aws:sqs:{REGION}:{ACCOUNT}:my-dlq"},
        )

        with patch(
            "robotocore.services.events.provider._invoke_target",
            side_effect=Exception("boom"),
        ):
            _dispatch_to_targets(rule, SAMPLE_EVENT, REGION, ACCOUNT)

        # Check DLQ got the message via invocation log
        log = get_invocation_log()
        dlq_entries = [e for e in log if e["target_type"] == "dlq"]
        assert len(dlq_entries) == 1
        body = json.loads(dlq_entries[0]["payload"])
        assert body["rule"] == "test-rule"
        assert body["target"] == "t1"
        assert "boom" in body["error"]

        # Also verify the queue received the message
        msgs = dlq.receive(max_messages=1, wait_time_seconds=0)
        assert len(msgs) == 1
        msg_body = json.loads(msgs[0][0].body)
        assert msg_body["rule"] == "test-rule"

        # Clean up
        sqs_store.queues.pop("my-dlq", None)

    def test_dlq_from_rule_level_config(self):
        """Rule-level DLQ config is used when target has no DLQ."""
        from robotocore.services.sqs.models import StandardQueue
        from robotocore.services.sqs.provider import (
            _get_store as get_sqs_store,
        )

        sqs_store = get_sqs_store(REGION)
        dlq = StandardQueue(name="rule-dlq", region=REGION, account_id=ACCOUNT)
        sqs_store.queues["rule-dlq"] = dlq

        rule = EventRule(
            name="rule-with-dlq",
            event_bus_name="default",
            region=REGION,
            account_id=ACCOUNT,
            dead_letter_config={"Arn": f"arn:aws:sqs:{REGION}:{ACCOUNT}:rule-dlq"},
        )
        rule.targets["t1"] = EventTarget(
            target_id="t1",
            arn="arn:aws:lambda:us-east-1:123:function:broken",
        )

        with patch(
            "robotocore.services.events.provider._invoke_target",
            side_effect=Exception("fail"),
        ):
            _dispatch_to_targets(rule, SAMPLE_EVENT, REGION, ACCOUNT)

        log = get_invocation_log()
        dlq_entries = [e for e in log if e["target_type"] == "dlq"]
        assert len(dlq_entries) == 1

        sqs_store.queues.pop("rule-dlq", None)

    def test_no_dlq_when_not_configured(self):
        """Without DLQ config, failure is just logged."""
        rule = EventRule(
            name="rule-no-dlq",
            event_bus_name="default",
            region=REGION,
            account_id=ACCOUNT,
        )
        rule.targets["t1"] = EventTarget(
            target_id="t1",
            arn="arn:aws:lambda:us-east-1:123:function:broken",
        )

        with patch(
            "robotocore.services.events.provider._invoke_target",
            side_effect=Exception("fail"),
        ):
            # Should not raise
            _dispatch_to_targets(rule, SAMPLE_EVENT, REGION, ACCOUNT)

        log = get_invocation_log()
        assert not any(e["target_type"] == "dlq" for e in log)

    def test_dlq_missing_queue_doesnt_raise(self):
        """If DLQ queue doesn't exist, _send_to_dlq should not raise."""
        rule = EventRule(
            name="r",
            event_bus_name="default",
            region=REGION,
            account_id=ACCOUNT,
        )
        target = EventTarget(
            target_id="t1",
            arn="arn:aws:lambda:us-east-1:123:function:f",
        )
        dlq_config = {"Arn": f"arn:aws:sqs:{REGION}:{ACCOUNT}:missing-dlq"}
        # Should not raise
        _send_to_dlq(
            dlq_config,
            SAMPLE_EVENT,
            target,
            rule,
            Exception("err"),
            REGION,
            ACCOUNT,
        )

    def test_dlq_non_sqs_arn_ignored(self):
        """DLQ config with non-SQS ARN should be silently ignored."""
        rule = EventRule(
            name="r",
            event_bus_name="default",
            region=REGION,
            account_id=ACCOUNT,
        )
        target = EventTarget(target_id="t1", arn="arn:aws:lambda:us-east-1:123:function:f")
        dlq_config = {"Arn": "arn:aws:sns:us-east-1:123:not-sqs"}
        _send_to_dlq(
            dlq_config,
            SAMPLE_EVENT,
            target,
            rule,
            Exception("err"),
            REGION,
            ACCOUNT,
        )
        log = get_invocation_log()
        assert not any(e["target_type"] == "dlq" for e in log)


# ---------------------------------------------------------------------------
# Archive and Replay models
# ---------------------------------------------------------------------------


class TestArchiveModel:
    def test_archive_arn_format(self):
        a = EventArchive(
            name="my-archive",
            source_arn="arn:aws:events:us-east-1:123:event-bus/default",
            region=REGION,
            account_id=ACCOUNT,
        )
        assert a.arn == f"arn:aws:events:{REGION}:{ACCOUNT}:archive/my-archive"

    def test_archive_defaults(self):
        a = EventArchive(
            name="a",
            source_arn="arn",
            region=REGION,
            account_id=ACCOUNT,
        )
        assert a.state == "ENABLED"
        assert a.retention_days == 0
        assert a.events == []
        assert a.event_count == 0
        assert a.size_bytes == 0


class TestReplayModel:
    def test_replay_arn_format(self):
        r = EventReplay(
            name="my-replay",
            archive_arn="arn:aws:events:us-east-1:123:archive/a",
            region=REGION,
            account_id=ACCOUNT,
            destination_arn="arn:aws:events:us-east-1:123:event-bus/default",
            start_time=0,
            end_time=100,
        )
        assert r.arn == f"arn:aws:events:{REGION}:{ACCOUNT}:replay/my-replay"

    def test_replay_defaults(self):
        r = EventReplay(
            name="r",
            archive_arn="arn",
            region=REGION,
            account_id=ACCOUNT,
            destination_arn="arn",
            start_time=0,
            end_time=100,
        )
        assert r.state == "COMPLETED"
        assert r.events_replayed == 0


# ---------------------------------------------------------------------------
# Archive store operations
# ---------------------------------------------------------------------------


class TestArchiveStore:
    def make_store(self):
        return EventsStore()

    def test_create_and_get_archive(self):
        store = self.make_store()
        archive = store.create_archive(
            "my-archive",
            "arn:aws:events:us-east-1:123:event-bus/default",
            REGION,
            ACCOUNT,
        )
        assert archive.name == "my-archive"
        assert store.get_archive("my-archive") is archive

    def test_delete_archive(self):
        store = self.make_store()
        store.create_archive("a", "arn", REGION, ACCOUNT)
        assert store.delete_archive("a") is True
        assert store.get_archive("a") is None

    def test_delete_nonexistent_archive(self):
        store = self.make_store()
        assert store.delete_archive("nope") is False

    def test_list_archives(self):
        store = self.make_store()
        store.create_archive("a1", "arn", REGION, ACCOUNT)
        store.create_archive("a2", "arn", REGION, ACCOUNT)
        assert len(store.list_archives()) == 2

    def test_list_archives_with_prefix(self):
        store = self.make_store()
        store.create_archive("test-a", "arn", REGION, ACCOUNT)
        store.create_archive("test-b", "arn", REGION, ACCOUNT)
        store.create_archive("other", "arn", REGION, ACCOUNT)
        result = store.list_archives(prefix="test")
        assert len(result) == 2

    def test_archive_event_matching(self):
        store = self.make_store()
        store.ensure_default_bus(REGION, ACCOUNT)
        bus = store.get_bus("default")
        store.create_archive(
            "order-archive",
            bus.arn,
            REGION,
            ACCOUNT,
            event_pattern={"source": ["myapp.orders"]},
        )
        store.archive_event(SAMPLE_EVENT, "default")

        archive = store.get_archive("order-archive")
        assert archive.event_count == 1
        assert len(archive.events) == 1
        assert archive.size_bytes > 0

    def test_archive_event_no_match(self):
        store = self.make_store()
        store.ensure_default_bus(REGION, ACCOUNT)
        bus = store.get_bus("default")
        store.create_archive(
            "other-archive",
            bus.arn,
            REGION,
            ACCOUNT,
            event_pattern={"source": ["other.source"]},
        )
        store.archive_event(SAMPLE_EVENT, "default")

        archive = store.get_archive("other-archive")
        assert archive.event_count == 0

    def test_archive_event_no_pattern_matches_all(self):
        store = self.make_store()
        store.ensure_default_bus(REGION, ACCOUNT)
        bus = store.get_bus("default")
        store.create_archive(
            "all-archive",
            bus.arn,
            REGION,
            ACCOUNT,
        )
        store.archive_event(SAMPLE_EVENT, "default")
        assert store.get_archive("all-archive").event_count == 1


# ---------------------------------------------------------------------------
# Archive/Replay API operations
# ---------------------------------------------------------------------------


class TestArchiveApi:
    @pytest.mark.asyncio
    async def test_create_archive(self):
        # First create a bus to have a valid source ARN
        req = _make_request("CreateEventBus", {"Name": "src-bus"})
        await handle_events_request(req, REGION, ACCOUNT)

        req = _make_request(
            "CreateArchive",
            {
                "ArchiveName": "my-archive",
                "EventSourceArn": f"arn:aws:events:{REGION}:{ACCOUNT}:event-bus/src-bus",
                "Description": "test",
                "RetentionDays": 30,
            },
        )
        resp = await handle_events_request(req, REGION, ACCOUNT)
        data = json.loads(resp.body)
        assert resp.status_code == 200
        assert "ArchiveArn" in data
        assert data["State"] == "ENABLED"

    @pytest.mark.asyncio
    async def test_create_archive_already_exists(self):
        req = _make_request(
            "CreateArchive",
            {
                "ArchiveName": "dup",
                "EventSourceArn": "arn:aws:events:us-east-1:123:event-bus/default",
            },
        )
        await handle_events_request(req, REGION, ACCOUNT)
        resp = await handle_events_request(req, REGION, ACCOUNT)
        assert resp.status_code == 400
        data = json.loads(resp.body)
        assert data["__type"] == "ResourceAlreadyExistsException"

    @pytest.mark.asyncio
    async def test_describe_archive(self):
        req1 = _make_request(
            "CreateArchive",
            {
                "ArchiveName": "desc-archive",
                "EventSourceArn": "arn:aws:events:us-east-1:123:event-bus/default",
                "Description": "my description",
            },
        )
        await handle_events_request(req1, REGION, ACCOUNT)

        req2 = _make_request(
            "DescribeArchive",
            {"ArchiveName": "desc-archive"},
        )
        resp = await handle_events_request(req2, REGION, ACCOUNT)
        data = json.loads(resp.body)
        assert data["ArchiveName"] == "desc-archive"
        assert data["Description"] == "my description"

    @pytest.mark.asyncio
    async def test_describe_nonexistent_archive(self):
        req = _make_request("DescribeArchive", {"ArchiveName": "nope"})
        resp = await handle_events_request(req, REGION, ACCOUNT)
        assert resp.status_code == 400
        assert json.loads(resp.body)["__type"] == "ResourceNotFoundException"

    @pytest.mark.asyncio
    async def test_list_archives(self):
        for name in ("a1", "a2"):
            req = _make_request(
                "CreateArchive",
                {
                    "ArchiveName": name,
                    "EventSourceArn": "arn",
                },
            )
            await handle_events_request(req, REGION, ACCOUNT)

        req = _make_request("ListArchives", {})
        resp = await handle_events_request(req, REGION, ACCOUNT)
        data = json.loads(resp.body)
        names = [a["ArchiveName"] for a in data["Archives"]]
        assert "a1" in names
        assert "a2" in names

    @pytest.mark.asyncio
    async def test_delete_archive(self):
        req1 = _make_request(
            "CreateArchive",
            {"ArchiveName": "to-delete", "EventSourceArn": "arn"},
        )
        await handle_events_request(req1, REGION, ACCOUNT)

        req2 = _make_request("DeleteArchive", {"ArchiveName": "to-delete"})
        resp = await handle_events_request(req2, REGION, ACCOUNT)
        assert resp.status_code == 200

        req3 = _make_request("DescribeArchive", {"ArchiveName": "to-delete"})
        resp3 = await handle_events_request(req3, REGION, ACCOUNT)
        assert resp3.status_code == 400

    @pytest.mark.asyncio
    async def test_delete_nonexistent_archive(self):
        req = _make_request("DeleteArchive", {"ArchiveName": "nope"})
        resp = await handle_events_request(req, REGION, ACCOUNT)
        assert resp.status_code == 400


class TestReplayApi:
    @pytest.mark.asyncio
    async def test_start_replay(self):
        # Create archive
        req1 = _make_request(
            "CreateArchive",
            {
                "ArchiveName": "replay-archive",
                "EventSourceArn": f"arn:aws:events:{REGION}:{ACCOUNT}:event-bus/default",
            },
        )
        await handle_events_request(req1, REGION, ACCOUNT)

        # Put events to archive them
        store = _get_store(REGION, ACCOUNT)
        store.archive_event(SAMPLE_EVENT, "default")

        req2 = _make_request(
            "StartReplay",
            {
                "ReplayName": "my-replay",
                "EventSourceArn": f"arn:aws:events:{REGION}:{ACCOUNT}:archive/replay-archive",
                "Destination": {"Arn": f"arn:aws:events:{REGION}:{ACCOUNT}:event-bus/default"},
                "EventStartTime": 0,
                "EventEndTime": time.time() + 1000,
            },
        )
        resp = await handle_events_request(req2, REGION, ACCOUNT)
        data = json.loads(resp.body)
        assert resp.status_code == 200
        assert data["State"] == "COMPLETED"
        assert "ReplayArn" in data

    @pytest.mark.asyncio
    async def test_start_replay_archive_not_found(self):
        req = _make_request(
            "StartReplay",
            {
                "ReplayName": "bad-replay",
                "EventSourceArn": "arn:aws:events:us-east-1:123:archive/nope",
                "Destination": {"Arn": "arn"},
            },
        )
        resp = await handle_events_request(req, REGION, ACCOUNT)
        assert resp.status_code == 400

    @pytest.mark.asyncio
    async def test_describe_replay(self):
        # Create archive and replay
        req1 = _make_request(
            "CreateArchive",
            {
                "ArchiveName": "desc-replay-archive",
                "EventSourceArn": f"arn:aws:events:{REGION}:{ACCOUNT}:event-bus/default",
            },
        )
        await handle_events_request(req1, REGION, ACCOUNT)

        req2 = _make_request(
            "StartReplay",
            {
                "ReplayName": "desc-replay",
                "EventSourceArn": f"arn:aws:events:{REGION}:{ACCOUNT}:archive/desc-replay-archive",
                "Destination": {"Arn": f"arn:aws:events:{REGION}:{ACCOUNT}:event-bus/default"},
                "EventStartTime": 0,
                "EventEndTime": time.time(),
            },
        )
        await handle_events_request(req2, REGION, ACCOUNT)

        req3 = _make_request("DescribeReplay", {"ReplayName": "desc-replay"})
        resp = await handle_events_request(req3, REGION, ACCOUNT)
        data = json.loads(resp.body)
        assert data["ReplayName"] == "desc-replay"
        assert data["State"] == "COMPLETED"

    @pytest.mark.asyncio
    async def test_describe_nonexistent_replay(self):
        req = _make_request("DescribeReplay", {"ReplayName": "nope"})
        resp = await handle_events_request(req, REGION, ACCOUNT)
        assert resp.status_code == 400


# ---------------------------------------------------------------------------
# PutEvents archives events
# ---------------------------------------------------------------------------


class TestPutEventsArchiving:
    @pytest.mark.asyncio
    async def test_put_events_archives_matching(self):
        store = _get_store(REGION, ACCOUNT)
        bus = store.get_bus("default")
        store.create_archive(
            "auto-archive",
            bus.arn,
            REGION,
            ACCOUNT,
        )

        req = _make_request(
            "PutEvents",
            {
                "Entries": [
                    {
                        "Source": "myapp",
                        "DetailType": "Test",
                        "Detail": '{"k": "v"}',
                    }
                ]
            },
        )
        await handle_events_request(req, REGION, ACCOUNT)

        archive = store.get_archive("auto-archive")
        assert archive.event_count == 1


# ---------------------------------------------------------------------------
# Target dispatch with InputPath
# ---------------------------------------------------------------------------


class TestInputPath:
    def test_input_path_extraction(self):
        target = EventTarget(
            target_id="t1",
            arn="arn:aws:lambda:us-east-1:123:function:f",
            input_path="$.detail",
        )
        with patch("robotocore.services.events.provider._invoke_lambda_target") as mock:
            _invoke_target(target, SAMPLE_EVENT, REGION, ACCOUNT)
            payload = mock.call_args[0][1]
            parsed = json.loads(payload)
            assert parsed["order_id"] == "12345"


# ---------------------------------------------------------------------------
# ListTargetsByRule includes new fields
# ---------------------------------------------------------------------------


class TestListTargetsNewFields:
    @pytest.mark.asyncio
    async def test_list_targets_includes_input_transformer(self):
        req1 = _make_request("PutRule", {"Name": "r1"})
        await handle_events_request(req1, REGION, ACCOUNT)

        req2 = _make_request(
            "PutTargets",
            {
                "Rule": "r1",
                "Targets": [
                    {
                        "Id": "t1",
                        "Arn": "arn:aws:lambda:us-east-1:123:function:f",
                        "InputTransformer": {
                            "InputPathsMap": {"s": "$.source"},
                            "InputTemplate": "<s>",
                        },
                    }
                ],
            },
        )
        await handle_events_request(req2, REGION, ACCOUNT)

        req3 = _make_request("ListTargetsByRule", {"Rule": "r1"})
        resp = await handle_events_request(req3, REGION, ACCOUNT)
        targets = json.loads(resp.body)["Targets"]
        assert "InputTransformer" in targets[0]
        assert targets[0]["InputTransformer"]["InputTemplate"] == "<s>"

    @pytest.mark.asyncio
    async def test_list_targets_includes_dead_letter_config(self):
        req1 = _make_request("PutRule", {"Name": "r2"})
        await handle_events_request(req1, REGION, ACCOUNT)

        req2 = _make_request(
            "PutTargets",
            {
                "Rule": "r2",
                "Targets": [
                    {
                        "Id": "t1",
                        "Arn": "arn:aws:sqs:us-east-1:123:q",
                        "DeadLetterConfig": {"Arn": "arn:aws:sqs:us-east-1:123:dlq"},
                    }
                ],
            },
        )
        await handle_events_request(req2, REGION, ACCOUNT)

        req3 = _make_request("ListTargetsByRule", {"Rule": "r2"})
        resp = await handle_events_request(req3, REGION, ACCOUNT)
        targets = json.loads(resp.body)["Targets"]
        assert "DeadLetterConfig" in targets[0]
