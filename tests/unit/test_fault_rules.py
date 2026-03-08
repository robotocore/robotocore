"""Unit tests for the chaos fault injection rule engine."""

import threading
from unittest.mock import patch

from robotocore.chaos.fault_rules import FaultRule, FaultRuleStore


class TestFaultRule:
    def test_default_fields(self):
        rule = FaultRule(service="s3")
        assert rule.service == "s3"
        assert rule.operation is None
        assert rule.region is None
        assert rule.enabled is True
        assert rule.probability == 1.0
        assert rule.match_count == 0
        assert len(rule.rule_id) == 12

    def test_custom_rule_id(self):
        rule = FaultRule(rule_id="my-rule", service="s3")
        assert rule.rule_id == "my-rule"

    def test_throttling_defaults_to_429(self):
        rule = FaultRule(error_code="ThrottlingException")
        assert rule.status_code == 429

    def test_non_throttling_defaults_to_500(self):
        rule = FaultRule(error_code="InternalError")
        assert rule.status_code == 500

    def test_custom_status_code(self):
        rule = FaultRule(error_code="ThrottlingException", status_code=503)
        assert rule.status_code == 503

    def test_probability_clamped_to_0_1(self):
        assert FaultRule(probability=-0.5).probability == 0.0
        assert FaultRule(probability=2.0).probability == 1.0
        assert FaultRule(probability=0.5).probability == 0.5

    def test_matches_all_when_no_filters(self):
        rule = FaultRule()
        assert rule.matches("s3", "PutObject", "us-east-1") is True
        assert rule.match_count == 1

    def test_matches_service_filter(self):
        rule = FaultRule(service="s3")
        assert rule.matches("s3", "PutObject", "us-east-1") is True
        assert rule.matches("sqs", "SendMessage", "us-east-1") is False

    def test_matches_region_filter(self):
        rule = FaultRule(region="eu-west-1")
        assert rule.matches("s3", "PutObject", "eu-west-1") is True
        assert rule.matches("s3", "PutObject", "us-east-1") is False

    def test_matches_operation_regex(self):
        rule = FaultRule(operation="Put.*")
        assert rule.matches("s3", "PutObject", "us-east-1") is True
        assert rule.matches("s3", "GetObject", "us-east-1") is False

    def test_matches_operation_regex_no_op(self):
        rule = FaultRule(operation="Put.*")
        assert rule.matches("s3", None, "us-east-1") is False

    def test_disabled_rule_never_matches(self):
        rule = FaultRule(service="s3", enabled=False)
        assert rule.matches("s3", "PutObject", "us-east-1") is False

    def test_probability_zero_never_matches(self):
        rule = FaultRule(probability=0.0)
        # Probability 0 means random.random() > 0.0 is always true → no match
        for _ in range(20):
            assert rule.matches("s3", "PutObject", "us-east-1") is False

    @patch("robotocore.chaos.fault_rules.random.random", return_value=0.3)
    def test_probability_respected(self, mock_random):
        rule = FaultRule(probability=0.5)
        assert rule.matches("s3", "PutObject", "us-east-1") is True

    @patch("robotocore.chaos.fault_rules.random.random", return_value=0.8)
    def test_probability_rejected(self, mock_random):
        rule = FaultRule(probability=0.5)
        assert rule.matches("s3", "PutObject", "us-east-1") is False

    def test_to_dict_roundtrip(self):
        rule = FaultRule(
            rule_id="test-123",
            service="lambda",
            operation="Invoke",
            region="us-west-2",
            error_code="TooManyRequestsException",
            status_code=429,
            latency_ms=100,
            probability=0.5,
        )
        d = rule.to_dict()
        assert d["rule_id"] == "test-123"
        assert d["service"] == "lambda"
        assert d["operation"] == "Invoke"
        assert d["latency_ms"] == 100
        assert d["probability"] == 0.5

    def test_from_dict(self):
        data = {
            "service": "dynamodb",
            "error_code": "ProvisionedThroughputExceededException",
            "probability": 0.75,
        }
        rule = FaultRule.from_dict(data)
        assert rule.service == "dynamodb"
        assert rule.error_code == "ProvisionedThroughputExceededException"
        assert rule.probability == 0.75

    def test_match_count_increments(self):
        rule = FaultRule()
        rule.matches("s3", "PutObject", "us-east-1")
        rule.matches("s3", "GetObject", "us-east-1")
        assert rule.match_count == 2

    def test_error_message_default(self):
        rule = FaultRule(rule_id="abc")
        assert "abc" in rule.error_message

    def test_error_message_custom(self):
        rule = FaultRule(error_message="boom")
        assert rule.error_message == "boom"

    # --- from_dict/to_dict roundtrip fidelity (Bug 2) ---

    def test_from_dict_preserves_created_at(self):
        rule = FaultRule(service="s3", error_code="InternalError")
        d = rule.to_dict()
        restored = FaultRule.from_dict(d)
        assert restored.created_at == rule.created_at

    def test_from_dict_preserves_match_count(self):
        rule = FaultRule(service="s3")
        rule.match_count = 17
        d = rule.to_dict()
        restored = FaultRule.from_dict(d)
        assert restored.match_count == 17

    def test_from_dict_without_created_at_gets_new_timestamp(self):
        data = {"service": "s3", "error_code": "InternalError"}
        rule = FaultRule.from_dict(data)
        assert rule.created_at > 0

    def test_from_dict_without_match_count_defaults_to_zero(self):
        data = {"service": "s3", "error_code": "InternalError"}
        rule = FaultRule.from_dict(data)
        assert rule.match_count == 0

    def test_full_roundtrip_all_fields(self):
        """Every field in to_dict() survives from_dict() → to_dict()."""
        rule = FaultRule(
            rule_id="roundtrip-1",
            service="lambda",
            operation="Invoke",
            region="ap-southeast-1",
            error_code="TooManyRequestsException",
            error_message="custom msg",
            status_code=429,
            latency_ms=250,
            probability=0.7,
            enabled=False,
        )
        rule.match_count = 5
        d1 = rule.to_dict()
        restored = FaultRule.from_dict(d1)
        d2 = restored.to_dict()
        assert d1 == d2

    def test_from_dict_empty_dict(self):
        """from_dict with empty dict should not crash."""
        rule = FaultRule.from_dict({})
        assert rule.service is None
        assert rule.error_code is None
        assert rule.latency_ms == 0
        assert rule.probability == 1.0
        assert rule.enabled is True

    def test_to_dict_includes_all_expected_keys(self):
        rule = FaultRule(service="s3")
        d = rule.to_dict()
        expected_keys = {
            "rule_id",
            "service",
            "operation",
            "region",
            "error_code",
            "error_message",
            "status_code",
            "latency_ms",
            "probability",
            "enabled",
            "created_at",
            "match_count",
        }
        assert set(d.keys()) == expected_keys

    def test_double_roundtrip(self):
        """Serialize → deserialize → serialize → deserialize preserves all data."""
        rule = FaultRule(
            rule_id="double-rt",
            service="s3",
            operation="Put.*",
            region="us-east-1",
            error_code="InternalError",
            error_message="test",
            status_code=500,
            latency_ms=100,
            probability=0.5,
            enabled=False,
        )
        rule.match_count = 10
        d1 = rule.to_dict()
        r2 = FaultRule.from_dict(d1)
        d2 = r2.to_dict()
        r3 = FaultRule.from_dict(d2)
        d3 = r3.to_dict()
        assert d1 == d2 == d3

    def test_from_dict_extra_unknown_keys_ignored(self):
        """Extra keys in the dict should not crash from_dict."""
        data = {
            "service": "s3",
            "error_code": "InternalError",
            "unknown_field": "whatever",
            "another_extra": 42,
        }
        rule = FaultRule.from_dict(data)
        assert rule.service == "s3"
        assert rule.error_code == "InternalError"

    def test_from_dict_restores_operation_regex(self):
        """After roundtrip, the restored rule still matches operations via regex."""
        rule = FaultRule(operation="Describe.*")
        assert rule.matches("ec2", "DescribeInstances", "us-east-1") is True
        assert rule.matches("ec2", "RunInstances", "us-east-1") is False

        d = rule.to_dict()
        restored = FaultRule.from_dict(d)
        # Restored rule must recompile the regex and match correctly
        assert restored.matches("ec2", "DescribeInstances", "us-east-1") is True
        assert restored.matches("ec2", "RunInstances", "us-east-1") is False

    def test_from_dict_none_operation_no_regex(self):
        """Roundtrip with no operation keeps _op_pattern as None."""
        rule = FaultRule(service="s3")
        d = rule.to_dict()
        restored = FaultRule.from_dict(d)
        assert restored._op_pattern is None
        assert restored.matches("s3", "PutObject", "us-east-1") is True
        assert restored.matches("s3", None, "us-east-1") is True

    def test_from_dict_preserves_error_message_not_regenerated(self):
        """If error_message was custom, roundtrip preserves it exactly."""
        rule = FaultRule(error_code="InternalError", error_message="custom msg")
        d = rule.to_dict()
        restored = FaultRule.from_dict(d)
        assert restored.error_message == "custom msg"

    def test_from_dict_preserves_rule_id(self):
        """Rule ID survives roundtrip, not regenerated."""
        rule = FaultRule(rule_id="keep-me")
        d = rule.to_dict()
        restored = FaultRule.from_dict(d)
        assert restored.rule_id == "keep-me"

    def test_from_dict_status_code_preserved_not_recomputed(self):
        """from_dict uses stored status_code, doesn't recompute from error_code."""
        # ThrottlingException normally defaults to 429, but we override to 503
        rule = FaultRule(error_code="ThrottlingException", status_code=503)
        d = rule.to_dict()
        restored = FaultRule.from_dict(d)
        assert restored.status_code == 503  # Not recomputed to 429

    def test_match_count_does_not_increment_on_disabled(self):
        rule = FaultRule(enabled=False)
        rule.matches("s3", "PutObject", "us-east-1")
        assert rule.match_count == 0

    def test_match_count_does_not_increment_on_probability_miss(self):
        with patch("robotocore.chaos.fault_rules.random.random", return_value=0.99):
            rule = FaultRule(probability=0.1)
            rule.matches("s3", "PutObject", "us-east-1")
            assert rule.match_count == 0

    def test_match_count_does_not_increment_on_service_miss(self):
        rule = FaultRule(service="dynamodb")
        rule.matches("s3", "PutObject", "us-east-1")
        assert rule.match_count == 0


class TestFaultRuleStore:
    def test_add_and_list(self):
        store = FaultRuleStore()
        rule = FaultRule(service="s3", error_code="InternalError")
        rule_id = store.add(rule)
        assert rule_id == rule.rule_id
        rules = store.list_rules()
        assert len(rules) == 1
        assert rules[0]["service"] == "s3"

    def test_remove_existing(self):
        store = FaultRuleStore()
        rule = FaultRule(rule_id="r1", service="s3")
        store.add(rule)
        assert store.remove("r1") is True
        assert len(store.list_rules()) == 0

    def test_remove_nonexistent(self):
        store = FaultRuleStore()
        assert store.remove("nonexistent") is False

    def test_clear(self):
        store = FaultRuleStore()
        store.add(FaultRule(service="s3"))
        store.add(FaultRule(service="sqs"))
        count = store.clear()
        assert count == 2
        assert len(store.list_rules()) == 0

    def test_find_matching(self):
        store = FaultRuleStore()
        store.add(FaultRule(service="s3", error_code="InternalError"))
        store.add(FaultRule(service="sqs", error_code="QueueDoesNotExist"))

        match = store.find_matching("s3", "PutObject", "us-east-1")
        assert match is not None
        assert match.error_code == "InternalError"

    def test_find_matching_none(self):
        store = FaultRuleStore()
        store.add(FaultRule(service="s3", error_code="InternalError"))
        assert store.find_matching("dynamodb", "PutItem", "us-east-1") is None

    def test_find_matching_returns_first(self):
        store = FaultRuleStore()
        store.add(FaultRule(service="s3", error_code="First"))
        store.add(FaultRule(service="s3", error_code="Second"))
        match = store.find_matching("s3", "PutObject", "us-east-1")
        assert match.error_code == "First"

    def test_empty_store_find(self):
        store = FaultRuleStore()
        assert store.find_matching("s3", "PutObject", "us-east-1") is None

    def test_clear_empty(self):
        store = FaultRuleStore()
        assert store.clear() == 0

    def test_concurrent_add_and_find(self):
        """Concurrent adds and finds don't crash or lose data."""
        store = FaultRuleStore()
        errors = []

        def adder():
            try:
                for i in range(50):
                    store.add(FaultRule(rule_id=f"thread-{i}", service="s3"))
            except Exception as e:
                errors.append(e)

        def finder():
            try:
                for _ in range(50):
                    store.find_matching("s3", "PutObject", "us-east-1")
            except Exception as e:
                errors.append(e)

        threads = [threading.Thread(target=adder), threading.Thread(target=finder)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()
        assert errors == []
        assert len(store.list_rules()) == 50

    def test_concurrent_match_count_increments(self):
        """match_count incremented by find_matching from multiple threads."""
        store = FaultRuleStore()
        rule = FaultRule(service="s3", error_code="InternalError")
        store.add(rule)

        def match_n_times():
            for _ in range(100):
                store.find_matching("s3", "PutObject", "us-east-1")

        threads = [threading.Thread(target=match_n_times) for _ in range(4)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()
        # 4 threads × 100 matches = 400
        assert rule.match_count == 400

    def test_concurrent_add_remove(self):
        """Concurrent adds and removes don't crash."""
        store = FaultRuleStore()
        errors = []

        def adder():
            try:
                for i in range(50):
                    store.add(FaultRule(rule_id=f"add-rm-{i}", service="s3"))
            except Exception as e:
                errors.append(e)

        def remover():
            try:
                for i in range(50):
                    store.remove(f"add-rm-{i}")
            except Exception as e:
                errors.append(e)

        threads = [threading.Thread(target=adder), threading.Thread(target=remover)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()
        assert errors == []

    def test_concurrent_list_during_mutations(self):
        """list_rules doesn't crash while rules are being added/removed."""
        store = FaultRuleStore()
        errors = []

        def mutator():
            try:
                for i in range(100):
                    rid = store.add(FaultRule(rule_id=f"mut-{i}", service="s3"))
                    store.remove(rid)
            except Exception as e:
                errors.append(e)

        def lister():
            try:
                for _ in range(100):
                    store.list_rules()
            except Exception as e:
                errors.append(e)

        threads = [threading.Thread(target=mutator), threading.Thread(target=lister)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()
        assert errors == []

    def test_match_count_race_on_direct_matches_call(self):
        """Calling matches() directly (bypassing store lock) has a potential race.

        This test documents that match_count += 1 in matches() is not atomic.
        Through the store (find_matching), it's safe because the store lock is held.
        Direct calls from multiple threads could lose increments.
        """
        rule = FaultRule(service="s3")
        barrier = threading.Barrier(4)

        def match_n_times():
            barrier.wait()
            for _ in range(1000):
                rule.matches("s3", "PutObject", "us-east-1")

        threads = [threading.Thread(target=match_n_times) for _ in range(4)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()
        # 4 threads × 1000 = 4000, but due to race condition, may be less.
        # With CPython's GIL this usually passes, but it's not guaranteed.
        # We assert <= 4000 to document the behavior, and > 0 to confirm it ran.
        assert 0 < rule.match_count <= 4000

    def test_list_rules_returns_snapshot(self):
        """list_rules returns dicts, not live rule references."""
        store = FaultRuleStore()
        store.add(FaultRule(rule_id="r1", service="s3"))
        rules = store.list_rules()
        rules[0]["service"] = "hacked"
        # Original should be unchanged
        assert store.list_rules()[0]["service"] == "s3"

    def test_find_matching_returns_rule_object_not_copy(self):
        """find_matching returns the actual FaultRule, not a dict or copy."""
        store = FaultRuleStore()
        original = FaultRule(rule_id="r1", service="s3")
        store.add(original)
        found = store.find_matching("s3", "PutObject", "us-east-1")
        assert found is original

    def test_add_returns_rule_id(self):
        store = FaultRuleStore()
        rule = FaultRule(rule_id="explicit-id", service="s3")
        returned_id = store.add(rule)
        assert returned_id == "explicit-id"

    def test_remove_only_removes_matching_id(self):
        store = FaultRuleStore()
        store.add(FaultRule(rule_id="keep", service="s3"))
        store.add(FaultRule(rule_id="delete-me", service="sqs"))
        store.add(FaultRule(rule_id="also-keep", service="dynamodb"))
        store.remove("delete-me")
        rules = store.list_rules()
        assert len(rules) == 2
        ids = {r["rule_id"] for r in rules}
        assert ids == {"keep", "also-keep"}


class TestSingletonStore:
    def test_get_fault_store_returns_same_instance(self):
        from robotocore.chaos.fault_rules import get_fault_store

        store1 = get_fault_store()
        store2 = get_fault_store()
        assert store1 is store2

    def test_singleton_cleanup_between_tests(self):
        """Demonstrate that the global singleton persists across test methods.

        Tests using the global store MUST clear it in setup/teardown.
        """
        from robotocore.chaos.fault_rules import get_fault_store

        store = get_fault_store()
        before = len(store.list_rules())
        store.add(FaultRule(rule_id="leak-test", service="s3"))
        assert len(store.list_rules()) == before + 1
        # Clean up to not pollute other tests
        store.remove("leak-test")
