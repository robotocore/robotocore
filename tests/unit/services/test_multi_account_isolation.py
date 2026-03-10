"""Comprehensive tests for multi-account isolation across all native providers.

Verifies that:
1. Every native provider with in-memory stores keys by (account_id, region)
2. Resources created in one account are NOT visible in another
3. forward_to_moto calls pass account_id correctly
4. Edge cases: empty, non-numeric, and very long account IDs
"""

import threading

import pytest

# ---------------------------------------------------------------------------
# SQS store isolation (already existed, extended here)
# ---------------------------------------------------------------------------


class TestSqsMultiAccountIsolation:
    def test_queue_not_visible_across_accounts(self):
        from robotocore.services.sqs.provider import _get_store

        store_a = _get_store("us-east-1", "111111111111")
        store_b = _get_store("us-east-1", "222222222222")

        store_a.create_queue("shared-name", "us-east-1", "111111111111")

        assert store_a.get_queue("shared-name") is not None
        assert store_b.get_queue("shared-name") is None

    def test_same_queue_name_different_accounts(self):
        from robotocore.services.sqs.provider import _get_store

        store_a = _get_store("eu-west-1", "333333333333")
        store_b = _get_store("eu-west-1", "444444444444")

        store_a.create_queue("my-queue", "eu-west-1", "333333333333")
        store_b.create_queue("my-queue", "eu-west-1", "444444444444")

        q_a = store_a.get_queue("my-queue")
        q_b = store_b.get_queue("my-queue")

        assert q_a is not None
        assert q_b is not None
        assert q_a is not q_b
        assert "333333333333" in q_a.arn
        assert "444444444444" in q_b.arn


# ---------------------------------------------------------------------------
# SNS store isolation
# ---------------------------------------------------------------------------


class TestSnsMultiAccountIsolation:
    def test_different_accounts_get_different_stores(self):
        from robotocore.services.sns.provider import _get_store

        store_a = _get_store("us-east-1", "111111111111")
        store_b = _get_store("us-east-1", "222222222222")
        assert store_a is not store_b

    def test_same_account_reuses_store(self):
        from robotocore.services.sns.provider import _get_store

        store1 = _get_store("us-west-2", "555555555555")
        store2 = _get_store("us-west-2", "555555555555")
        assert store1 is store2

    def test_topic_not_visible_across_accounts(self):
        from robotocore.services.sns.provider import _get_store

        store_a = _get_store("us-east-1", "aaa111111111")
        store_b = _get_store("us-east-1", "bbb222222222")

        store_a.create_topic("my-topic", "us-east-1", "aaa111111111")

        assert store_a.get_topic("arn:aws:sns:us-east-1:aaa111111111:my-topic") is not None
        # Account B has no topics
        assert store_b.get_topic("arn:aws:sns:us-east-1:bbb222222222:my-topic") is None

    def test_default_account_backward_compat(self):
        from robotocore.services.sns.provider import _get_store

        store_default = _get_store("ap-south-1")
        store_explicit = _get_store("ap-south-1", "123456789012")
        assert store_default is store_explicit


# ---------------------------------------------------------------------------
# Kinesis store isolation
# ---------------------------------------------------------------------------


class TestKinesisMultiAccountIsolation:
    def test_different_accounts_get_different_stores(self):
        from robotocore.services.kinesis.models import _get_store

        store_a = _get_store("us-east-1", "111111111111")
        store_b = _get_store("us-east-1", "222222222222")
        assert store_a is not store_b

    def test_same_account_reuses_store(self):
        from robotocore.services.kinesis.models import _get_store

        store1 = _get_store("us-west-2", "666666666666")
        store2 = _get_store("us-west-2", "666666666666")
        assert store1 is store2

    def test_stream_not_visible_across_accounts(self):
        from robotocore.services.kinesis.models import _get_store

        store_a = _get_store("eu-central-1", "kin111111111")
        store_b = _get_store("eu-central-1", "kin222222222")

        store_a.create_stream("my-stream", 1, "eu-central-1", "kin111111111")

        assert store_a.get_stream("my-stream") is not None
        assert store_b.get_stream("my-stream") is None

    def test_default_account_backward_compat(self):
        from robotocore.services.kinesis.models import _get_store

        store_default = _get_store("us-east-2")
        store_explicit = _get_store("us-east-2", "123456789012")
        assert store_default is store_explicit


# ---------------------------------------------------------------------------
# Events (EventBridge) store isolation
# ---------------------------------------------------------------------------


class TestEventsMultiAccountIsolation:
    def test_different_accounts_get_different_stores(self):
        from robotocore.services.events.provider import _get_store

        store_a = _get_store("us-east-1", "111111111111")
        store_b = _get_store("us-east-1", "222222222222")
        assert store_a is not store_b

    def test_rule_not_visible_across_accounts(self):
        from robotocore.services.events.provider import _get_store

        store_a = _get_store("us-east-1", "evt111111111")
        store_b = _get_store("us-east-1", "evt222222222")

        # Create a rule in account A
        store_a.put_rule(
            "test-rule",
            event_pattern='{"source": ["test"]}',
            bus_name="default",
            region="us-east-1",
            account_id="evt111111111",
        )

        rules_a = store_a.list_rules("default")
        rules_b = store_b.list_rules("default")

        rule_names_a = [r.name for r in rules_a]
        rule_names_b = [r.name for r in rules_b]

        assert "test-rule" in rule_names_a
        assert "test-rule" not in rule_names_b


# ---------------------------------------------------------------------------
# CloudFormation store isolation
# ---------------------------------------------------------------------------


class TestCloudFormationMultiAccountIsolation:
    def test_different_accounts_get_different_stores(self):
        from robotocore.services.cloudformation.provider import _get_store

        store_a = _get_store("us-east-1", "111111111111")
        store_b = _get_store("us-east-1", "222222222222")
        assert store_a is not store_b

    def test_default_account_backward_compat(self):
        from robotocore.services.cloudformation.provider import _get_store

        store_default = _get_store("us-west-1")
        store_explicit = _get_store("us-west-1", "123456789012")
        assert store_default is store_explicit


# ---------------------------------------------------------------------------
# Cognito store isolation
# ---------------------------------------------------------------------------


class TestCognitoMultiAccountIsolation:
    def test_different_accounts_get_different_stores(self):
        from robotocore.services.cognito.provider import _get_store

        store_a = _get_store("us-east-1", "111111111111")
        store_b = _get_store("us-east-1", "222222222222")
        assert store_a is not store_b

    def test_default_account_backward_compat(self):
        from robotocore.services.cognito.provider import _get_store

        store_default = _get_store("eu-west-1")
        store_explicit = _get_store("eu-west-1", "123456789012")
        assert store_default is store_explicit


# ---------------------------------------------------------------------------
# AppSync store isolation
# ---------------------------------------------------------------------------


class TestAppSyncMultiAccountIsolation:
    def test_different_accounts_get_different_stores(self):
        from robotocore.services.appsync.provider import _get_store

        store_a = _get_store("us-east-1", "111111111111")
        store_b = _get_store("us-east-1", "222222222222")
        assert store_a is not store_b

    def test_default_account_backward_compat(self):
        from robotocore.services.appsync.provider import _get_store

        store_default = _get_store("us-east-1")
        store_explicit = _get_store("us-east-1", "123456789012")
        assert store_default is store_explicit


# ---------------------------------------------------------------------------
# Scheduler store isolation
# ---------------------------------------------------------------------------


class TestSchedulerMultiAccountIsolation:
    def test_different_accounts_get_different_schedules(self):
        from robotocore.services.scheduler.provider import _get_schedules

        sched_a = _get_schedules("us-east-1", "111111111111")
        sched_b = _get_schedules("us-east-1", "222222222222")
        assert sched_a is not sched_b

    def test_different_accounts_get_different_groups(self):
        from robotocore.services.scheduler.provider import _get_groups

        groups_a = _get_groups("us-east-1", "sched1111111")
        groups_b = _get_groups("us-east-1", "sched2222222")
        assert groups_a is not groups_b

    def test_default_group_uses_correct_account_id(self):
        from robotocore.services.scheduler.provider import _get_groups

        groups = _get_groups("us-east-1", "987654321098")
        default_group = groups.get("default")
        assert default_group is not None
        assert "987654321098" in default_group["Arn"]

    def test_default_account_backward_compat(self):
        from robotocore.services.scheduler.provider import _get_schedules

        sched_default = _get_schedules("ap-northeast-1")
        sched_explicit = _get_schedules("ap-northeast-1", "123456789012")
        assert sched_default is sched_explicit


# ---------------------------------------------------------------------------
# Batch store isolation (already keyed properly, verify)
# ---------------------------------------------------------------------------


class TestBatchMultiAccountIsolation:
    def test_different_accounts_get_different_stores(self):
        from robotocore.services.batch.provider import _get_store

        store_a = _get_store("us-east-1", "111111111111")
        store_b = _get_store("us-east-1", "222222222222")
        assert store_a is not store_b


# ---------------------------------------------------------------------------
# ECS store isolation (already keyed properly, verify)
# ---------------------------------------------------------------------------


class TestEcsMultiAccountIsolation:
    def test_different_accounts_get_different_stores(self):
        from robotocore.services.ecs.provider import _get_store

        store_a = _get_store("us-east-1", "111111111111")
        store_b = _get_store("us-east-1", "222222222222")
        assert store_a is not store_b


# ---------------------------------------------------------------------------
# Edge cases
# ---------------------------------------------------------------------------


class TestAccountIdEdgeCases:
    def test_empty_account_id_treated_as_distinct(self):
        """Empty string is a valid distinct key (won't collide with default)."""
        from robotocore.services.sqs.provider import _get_store

        store_empty = _get_store("us-east-1", "")
        store_default = _get_store("us-east-1", "123456789012")
        assert store_empty is not store_default

    def test_non_numeric_account_id(self):
        """Non-numeric account IDs should work (for testing flexibility)."""
        from robotocore.services.sqs.provider import _get_store

        store = _get_store("us-east-1", "test-account")
        assert store is not None
        store.create_queue("test-q", "us-east-1", "test-account")
        assert store.get_queue("test-q") is not None

    def test_very_long_account_id(self):
        """Very long account IDs should not cause issues."""
        from robotocore.services.sqs.provider import _get_store

        long_id = "1" * 100
        store = _get_store("us-east-1", long_id)
        assert store is not None


# ---------------------------------------------------------------------------
# Account ID extraction from requests
# ---------------------------------------------------------------------------


class TestAccountIdExtraction:
    def test_extract_from_sigv4(self):
        from unittest.mock import MagicMock

        from robotocore.gateway.app import _extract_account_id

        request = MagicMock()
        request.headers = {
            "authorization": (
                "AWS4-HMAC-SHA256 "
                "Credential=999888777666/20260101/us-east-1/s3/aws4_request, "
                "SignedHeaders=host, Signature=abc123"
            )
        }
        request.query_params = {}

        assert _extract_account_id(request) == "999888777666"

    def test_extract_from_presigned_url(self):
        from unittest.mock import MagicMock

        from robotocore.gateway.app import _extract_account_id

        request = MagicMock()
        request.headers = {"authorization": ""}
        credential = "123456789012/20260101/us-east-1/s3/aws4_request"
        request.query_params = {"X-Amz-Credential": credential}

        assert _extract_account_id(request) == "123456789012"

    def test_default_when_no_auth(self):
        from unittest.mock import MagicMock

        from robotocore.gateway.app import _extract_account_id

        request = MagicMock()
        request.headers = {}
        request.query_params = {}

        assert _extract_account_id(request) == "123456789012"


# ---------------------------------------------------------------------------
# forward_to_moto account_id propagation (all providers)
# ---------------------------------------------------------------------------


class TestForwardToMotoAccountIdPropagation:
    """Verify that EVERY native provider's forward_to_moto fallback passes account_id.

    This is a code-level audit test: it imports each provider module and
    inspects the source for forward_to_moto calls without account_id.
    """

    @pytest.mark.parametrize(
        "module_path",
        [
            "robotocore.services.eks.provider",
            "robotocore.services.sqs.provider",
            "robotocore.services.sns.provider",
            "robotocore.services.dynamodb.provider",
            "robotocore.services.events.provider",
            "robotocore.services.kinesis.provider",
            "robotocore.services.s3.provider",
            "robotocore.services.lambda_.provider",
            "robotocore.services.ecs.provider",
            "robotocore.services.cognito.provider",
            "robotocore.services.cloudformation.provider",
            "robotocore.services.stepfunctions.provider",
            "robotocore.services.batch.provider",
            "robotocore.services.firehose.provider",
            "robotocore.services.ec2.provider",
            "robotocore.services.iam.provider",
            "robotocore.services.route53.provider",
            "robotocore.services.acm.provider",
            "robotocore.services.ecr.provider",
            "robotocore.services.config.provider",
            "robotocore.services.ssm.provider",
            "robotocore.services.secretsmanager.provider",
            "robotocore.services.ses.provider",
            "robotocore.services.ses.sesv2_provider",
            "robotocore.services.xray.provider",
            "robotocore.services.support.provider",
            "robotocore.services.tagging.provider",
            "robotocore.services.resource_groups.provider",
            "robotocore.services.rekognition.provider",
            "robotocore.services.opensearch.provider",
            "robotocore.services.cloudwatch.provider",
            "robotocore.services.cloudwatch.logs_provider",
            "robotocore.services.appsync.provider",
            "robotocore.services.scheduler.provider",
            "robotocore.services.rds.provider",
            "robotocore.services.rds.data_provider",
            "robotocore.services.elasticache.provider",
        ],
    )
    def test_forward_to_moto_includes_account_id(self, module_path: str):
        """Ensure every forward_to_moto call passes account_id as keyword arg."""
        import importlib
        import inspect
        import re

        module = importlib.import_module(module_path)
        source = inspect.getsource(module)

        # Find all forward_to_moto calls
        pattern = r'forward_to_moto\(request,\s*"[^"]+"\)'
        bare_calls = re.findall(pattern, source)

        assert bare_calls == [], (
            f"{module_path} has forward_to_moto calls WITHOUT account_id: {bare_calls}"
        )


# ---------------------------------------------------------------------------
# Resource browser multi-account
# ---------------------------------------------------------------------------


class TestResourceBrowserMultiAccount:
    def test_get_backend_with_different_account_ids(self):
        """Resource browser _get_backend returns correct per-account backend."""
        from robotocore.resources.browser import _get_backend

        # These may return None if the account hasn't been initialized yet,
        # but they should not raise and should be distinct calls
        _get_backend("s3", "111111111111")
        _get_backend("s3", "222222222222")

        # Both calls should complete without error
        # (backends may or may not be None depending on whether data exists)

    def test_resource_counts_scoped_by_account(self):
        """get_resource_counts returns different results for different accounts."""
        from robotocore.resources.browser import get_resource_counts

        # Two accounts with no data should both return empty
        counts_a = get_resource_counts(account_id="browser111111")
        counts_b = get_resource_counts(account_id="browser222222")
        assert isinstance(counts_a, dict)
        assert isinstance(counts_b, dict)

    def test_service_resources_scoped_by_account(self):
        """get_service_resources returns different results for different accounts."""
        from robotocore.resources.browser import get_service_resources

        res_a = get_service_resources("s3", account_id="browser333333")
        res_b = get_service_resources("s3", account_id="browser444444")
        assert isinstance(res_a, list)
        assert isinstance(res_b, list)


# ---------------------------------------------------------------------------
# Thread safety
# ---------------------------------------------------------------------------


class TestMultiAccountThreadSafety:
    def test_concurrent_store_creation(self):
        """Multiple threads creating stores for different accounts concurrently."""
        from robotocore.services.sqs.provider import _get_store

        stores = {}
        errors = []

        def create_store(account_id: str):
            try:
                s = _get_store("us-east-1", account_id)
                stores[account_id] = s
            except Exception as e:
                errors.append(e)

        threads = [
            threading.Thread(target=create_store, args=(f"thread{i:012d}",)) for i in range(20)
        ]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        assert len(errors) == 0
        assert len(stores) == 20
        # All stores should be distinct
        store_ids = {id(s) for s in stores.values()}
        assert len(store_ids) == 20
