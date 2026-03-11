"""Semantic integration tests for SQS endpoint strategies.

These tests verify that the SQS provider returns URLs matching the configured strategy.
They exercise the provider code directly (no server needed).
"""

from robotocore.services.sqs.endpoint_strategy import SqsEndpointStrategy, sqs_queue_url
from robotocore.services.sqs.models import SqsStore


class TestCreateQueueReturnsStrategyUrl:
    """CreateQueue should return URLs matching the configured strategy."""

    def test_standard_strategy(self, monkeypatch):
        monkeypatch.setenv("SQS_ENDPOINT_STRATEGY", "standard")
        store = SqsStore()
        queue = store.create_queue("my-queue", "us-east-1", "123456789012")
        expected = sqs_queue_url(
            "my-queue", "us-east-1", "123456789012", SqsEndpointStrategy.STANDARD
        )
        assert queue.url == expected

    def test_domain_strategy(self, monkeypatch):
        monkeypatch.setenv("SQS_ENDPOINT_STRATEGY", "domain")
        store = SqsStore()
        queue = store.create_queue("my-queue", "us-east-1", "123456789012")
        expected = sqs_queue_url(
            "my-queue", "us-east-1", "123456789012", SqsEndpointStrategy.DOMAIN
        )
        assert queue.url == expected

    def test_path_strategy(self, monkeypatch):
        monkeypatch.setenv("SQS_ENDPOINT_STRATEGY", "path")
        store = SqsStore()
        queue = store.create_queue("my-queue", "us-east-1", "123456789012")
        expected = sqs_queue_url("my-queue", "us-east-1", "123456789012", SqsEndpointStrategy.PATH)
        assert queue.url == expected

    def test_dynamic_strategy(self, monkeypatch):
        monkeypatch.setenv("SQS_ENDPOINT_STRATEGY", "dynamic")
        store = SqsStore()
        queue = store.create_queue("my-queue", "us-east-1", "123456789012")
        expected = sqs_queue_url(
            "my-queue", "us-east-1", "123456789012", SqsEndpointStrategy.DYNAMIC
        )
        assert queue.url == expected


class TestGetQueueUrlReturnsStrategyUrl:
    """GetQueueUrl returns URL matching configured strategy."""

    def test_get_queue_url_standard(self, monkeypatch):
        monkeypatch.setenv("SQS_ENDPOINT_STRATEGY", "standard")
        store = SqsStore()
        store.create_queue("test-q", "us-east-1", "123456789012")
        queue = store.get_queue("test-q")
        assert queue is not None
        assert "sqs.us-east-1.localhost.localstack.cloud" in queue.url

    def test_get_queue_url_path(self, monkeypatch):
        monkeypatch.setenv("SQS_ENDPOINT_STRATEGY", "path")
        store = SqsStore()
        store.create_queue("test-q", "us-east-1", "123456789012")
        queue = store.get_queue("test-q")
        assert queue is not None
        assert "/queue/us-east-1/" in queue.url


class TestListQueuesReturnsStrategyUrls:
    """ListQueues returns URLs matching configured strategy."""

    def test_list_queues_standard(self, monkeypatch):
        monkeypatch.setenv("SQS_ENDPOINT_STRATEGY", "standard")
        store = SqsStore()
        store.create_queue("q1", "us-east-1", "123456789012")
        store.create_queue("q2", "us-east-1", "123456789012")
        queues = store.list_queues()
        assert len(queues) == 2
        for q in queues:
            assert "sqs.us-east-1.localhost.localstack.cloud" in q.url

    def test_list_queues_path(self, monkeypatch):
        monkeypatch.setenv("SQS_ENDPOINT_STRATEGY", "path")
        store = SqsStore()
        store.create_queue("q1", "us-east-1", "123456789012")
        queues = store.list_queues()
        assert len(queues) == 1
        assert "/queue/us-east-1/" in queues[0].url


class TestQueueUrlResolution:
    """Test that queue URL-based lookups work with all strategy URLs."""

    def test_get_queue_by_url_path_style(self, monkeypatch):
        monkeypatch.setenv("SQS_ENDPOINT_STRATEGY", "path")
        store = SqsStore()
        store.create_queue("resolve-q", "us-east-1", "123456789012")
        queue = store.get_queue("resolve-q")
        assert queue is not None
        found = store.get_queue_by_url(queue.url)
        assert found is not None
        assert found.name == "resolve-q"

    def test_get_queue_by_url_standard_style(self, monkeypatch):
        monkeypatch.setenv("SQS_ENDPOINT_STRATEGY", "standard")
        store = SqsStore()
        store.create_queue("resolve-q", "us-east-1", "123456789012")
        queue = store.get_queue("resolve-q")
        assert queue is not None
        found = store.get_queue_by_url(queue.url)
        assert found is not None
        assert found.name == "resolve-q"
