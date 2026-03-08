"""Unit tests for the chaos fault injection rule engine."""

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
        import threading

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
        import threading

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

    def test_list_rules_returns_snapshot(self):
        """list_rules returns dicts, not live rule references."""
        store = FaultRuleStore()
        store.add(FaultRule(rule_id="r1", service="s3"))
        rules = store.list_rules()
        rules[0]["service"] = "hacked"
        # Original should be unchanged
        assert store.list_rules()[0]["service"] == "s3"


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
