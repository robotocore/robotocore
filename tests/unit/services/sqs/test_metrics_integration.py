"""Semantic integration tests for SQS CloudWatch metrics.

Tests the end-to-end flow: SQS operations -> counter tracking -> publish to
CloudWatch backend -> verify metrics are queryable. Uses Moto's in-process
CloudWatch backend (no HTTP server needed).
"""

import hashlib
from unittest.mock import patch

import pytest

from moto import mock_aws
from robotocore.services.sqs.metrics import (
    SqsMetricsPublisher,
    _counter_lock,
    _counters,
    increment_empty_receives,
    increment_received,
    increment_sent,
    publish_metrics,
)
from robotocore.services.sqs.models import SqsMessage
from robotocore.services.sqs.provider import _get_store, _store_lock, _stores

ACCOUNT_ID = "123456789012"
REGION = "us-east-1"


@pytest.fixture(autouse=True)
def _reset_state():
    """Reset SQS stores and counters between tests."""
    with _counter_lock:
        _counters.clear()
    with _store_lock:
        _stores.clear()
    yield
    with _counter_lock:
        _counters.clear()
    with _store_lock:
        _stores.clear()


def _md5(s: str) -> str:
    return hashlib.md5(s.encode()).hexdigest()


@mock_aws
class TestEndToEndSendAndPublish:
    """Send messages -> publish metrics -> query CloudWatch."""

    def test_visible_count_published(self):
        store = _get_store(REGION, ACCOUNT_ID)
        queue = store.create_queue("visible-test", REGION, ACCOUNT_ID)
        queue.put(SqsMessage(message_id="m1", body="a", md5_of_body=_md5("a")))
        queue.put(SqsMessage(message_id="m2", body="b", md5_of_body=_md5("b")))

        publish_metrics(ACCOUNT_ID, REGION)

        from moto.backends import get_backend  # noqa: I001

        cw = get_backend("cloudwatch")[ACCOUNT_ID][REGION]
        found = _find_metric(cw, "ApproximateNumberOfMessagesVisible", "visible-test")
        assert found is not None
        assert float(found.value) == 2.0

    def test_sent_and_received_counters(self):
        store = _get_store(REGION, ACCOUNT_ID)
        store.create_queue("counter-test", REGION, ACCOUNT_ID)

        increment_sent("counter-test", 100)
        increment_sent("counter-test", 200)
        increment_received("counter-test", 2)

        publish_metrics(ACCOUNT_ID, REGION)

        from moto.backends import get_backend  # noqa: I001

        cw = get_backend("cloudwatch")[ACCOUNT_ID][REGION]

        sent = _find_metric(cw, "NumberOfMessagesSent", "counter-test")
        assert sent is not None
        assert float(sent.value) == 2.0

        received = _find_metric(cw, "NumberOfMessagesReceived", "counter-test")
        assert received is not None
        assert float(received.value) == 2.0

    def test_empty_receive_counter(self):
        store = _get_store(REGION, ACCOUNT_ID)
        store.create_queue("empty-recv-test", REGION, ACCOUNT_ID)

        increment_empty_receives("empty-recv-test")
        increment_empty_receives("empty-recv-test")
        increment_empty_receives("empty-recv-test")

        publish_metrics(ACCOUNT_ID, REGION)

        from moto.backends import get_backend  # noqa: I001

        cw = get_backend("cloudwatch")[ACCOUNT_ID][REGION]
        metric = _find_metric(cw, "NumberOfEmptyReceives", "empty-recv-test")
        assert metric is not None
        assert float(metric.value) == 3.0


@mock_aws
class TestMetricsNamespaceAndDimension:
    def test_correct_namespace(self):
        store = _get_store(REGION, ACCOUNT_ID)
        store.create_queue("ns-test", REGION, ACCOUNT_ID)
        increment_sent("ns-test", 10)

        publish_metrics(ACCOUNT_ID, REGION)

        from moto.backends import get_backend  # noqa: I001

        cw = get_backend("cloudwatch")[ACCOUNT_ID][REGION]
        # All SQS metrics should be in AWS/SQS namespace
        for datum in cw.metric_data:
            if any(d.name == "QueueName" and d.value == "ns-test" for d in datum.dimensions):
                assert datum.namespace == "AWS/SQS"

    def test_dimension_is_queue_name(self):
        store = _get_store(REGION, ACCOUNT_ID)
        store.create_queue("dim-test", REGION, ACCOUNT_ID)
        increment_sent("dim-test", 10)

        publish_metrics(ACCOUNT_ID, REGION)

        from moto.backends import get_backend  # noqa: I001

        cw = get_backend("cloudwatch")[ACCOUNT_ID][REGION]
        found_queue_dim = False
        for datum in cw.metric_data:
            for d in datum.dimensions:
                if d.name == "QueueName" and d.value == "dim-test":
                    found_queue_dim = True
                    break
        assert found_queue_dim


@mock_aws
class TestBackgroundThread:
    def test_starts_and_stops_cleanly(self):
        pub = SqsMetricsPublisher(interval=1)
        pub.start()
        assert pub.is_running
        pub.stop()
        assert not pub.is_running

    def test_does_not_start_when_disabled(self):
        with patch.dict("os.environ", {"SQS_CLOUDWATCH_METRICS": "false"}):
            pub = SqsMetricsPublisher(interval=1)
            pub.start()
            assert not pub.is_running


def _find_metric(cw_backend, metric_name: str, queue_name: str):
    """Find a metric datum in the CW backend matching name and queue dimension."""
    for datum in cw_backend.metric_data:
        if datum.name != metric_name:
            continue
        if datum.namespace != "AWS/SQS":
            continue
        for d in datum.dimensions:
            if d.name == "QueueName" and d.value == queue_name:
                return datum
    return None
