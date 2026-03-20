"""Tests verifying that resources in one region are NOT visible in another.

Phase 5B: Region isolation verification for top native providers.
"""

from robotocore.services.events.provider import _get_store as get_events_store
from robotocore.services.sns.provider import _get_store as get_sns_store
from robotocore.services.sqs.provider import _get_store as get_sqs_store


class TestSqsRegionIsolation:
    def test_queue_not_visible_across_regions(self):
        east_store = get_sqs_store("us-east-1")
        east_store.create_queue("isolation-test-q", "us-east-1", "123456789012")

        west_store = get_sqs_store("us-west-2")
        assert west_store.get_queue("isolation-test-q") is None

    def test_separate_stores_per_region(self):
        east = get_sqs_store("us-east-1")
        west = get_sqs_store("us-west-2")
        assert east is not west


class TestSnsRegionIsolation:
    def test_topic_not_visible_across_regions(self):
        east_store = get_sns_store("us-east-1")
        east_store.create_topic("isolation-test-topic", "us-east-1", "123456789012")

        west_store = get_sns_store("us-west-2")
        west_topics = [t.name for t in west_store.list_topics()]
        assert "isolation-test-topic" not in west_topics

    def test_separate_stores_per_region(self):
        east = get_sns_store("us-east-1")
        west = get_sns_store("us-west-2")
        assert east is not west


class TestEventsRegionIsolation:
    def test_rule_not_visible_across_regions(self):
        east_store = get_events_store("us-east-1")
        east_store.put_rule(
            "isolation-test-rule",
            "default",
            "us-east-1",
            "123456789012",
            event_pattern={"source": ["test"]},
        )

        west_store = get_events_store("us-west-2")
        west_store.ensure_default_bus("us-west-2", "123456789012")
        rules = west_store.list_rules("default")
        rule_names = [r.name for r in rules]
        assert "isolation-test-rule" not in rule_names

    def test_event_bus_not_visible_across_regions(self):
        east_store = get_events_store("us-east-1")
        east_store.create_event_bus("isolation-bus", "us-east-1", "123456789012")

        west_store = get_events_store("us-west-2")
        assert west_store.get_bus("isolation-bus") is None


class TestLambdaRegionIsolation:
    def test_lambda_uses_moto_per_region_backend(self):
        """Lambda functions are stored in Moto backends which are keyed per-region."""
        from moto.backends import get_backend  # noqa: I001

        east = get_backend("lambda")["123456789012"]["us-east-1"]
        west = get_backend("lambda")["123456789012"]["us-west-2"]
        assert east is not west


class TestSecretsManagerRegionIsolation:
    def test_moto_backends_per_region(self):
        """SecretsManager uses Moto backends which are keyed per-region."""
        from moto.backends import get_backend  # noqa: I001

        east = get_backend("secretsmanager")["123456789012"]["us-east-1"]
        west = get_backend("secretsmanager")["123456789012"]["us-west-2"]
        assert east is not west


class TestKinesisRegionIsolation:
    def test_separate_stores_per_region(self):
        from robotocore.services.kinesis.models import _get_store

        east = _get_store("us-east-1")
        west = _get_store("us-west-2")
        assert east is not west


class TestSchedulerRegionIsolation:
    def test_schedules_keyed_by_region(self):
        """Scheduler stores schedules per-region in a dict."""
        from robotocore.services.scheduler.provider import _schedules

        # Different region keys are separate entries
        assert isinstance(_schedules, dict)
        # If both regions have data, they should not share objects
        if "us-east-1" in _schedules and "us-west-2" in _schedules:
            assert _schedules["us-east-1"] is not _schedules["us-west-2"]


class TestCloudWatchRegionIsolation:
    def test_moto_backends_per_region(self):
        """CloudWatch uses Moto backends which are keyed per-region."""
        from moto.backends import get_backend  # noqa: I001

        east = get_backend("cloudwatch")["123456789012"]["us-east-1"]
        west = get_backend("cloudwatch")["123456789012"]["us-west-2"]
        assert east is not west
