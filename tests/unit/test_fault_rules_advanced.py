"""Advanced tests for fault rules: regex matching, probability, thread safety,
serialization roundtrip, store operations."""

import threading

import pytest

from robotocore.chaos.fault_rules import FaultRule, FaultRuleStore, get_fault_store


class TestFaultRuleRegexMatching:
    def test_regex_operation_pattern(self):
        rule = FaultRule(service="s3", operation="Put.*", error_code="InternalError")
        assert rule.matches("s3", "PutObject", "us-east-1") is True
        assert rule.matches("s3", "PutBucketPolicy", "us-east-1") is True
        assert rule.matches("s3", "GetObject", "us-east-1") is False

    def test_regex_partial_match(self):
        """Regex uses search(), so partial matches work."""
        rule = FaultRule(service="s3", operation="Item", error_code="Err")
        assert rule.matches("s3", "PutItem", "us-east-1") is True
        assert rule.matches("s3", "GetItem", "us-east-1") is True
        assert rule.matches("s3", "CreateTable", "us-east-1") is False

    def test_invalid_regex_raises(self):
        with pytest.raises(ValueError, match="Invalid regex"):
            FaultRule(operation="[invalid")

    def test_no_operation_pattern_matches_any_operation(self):
        rule = FaultRule(service="s3", error_code="Err")
        assert rule.matches("s3", "PutObject", "us-east-1") is True
        assert rule.matches("s3", "GetObject", "us-east-1") is True
        assert rule.matches("s3", None, "us-east-1") is True

    def test_operation_pattern_with_no_operation_in_request(self):
        """If rule has operation pattern but request has no operation, no match."""
        rule = FaultRule(service="s3", operation="Put.*", error_code="Err")
        assert rule.matches("s3", None, "us-east-1") is False


class TestFaultRuleProbability:
    def test_probability_zero_never_matches(self):
        rule = FaultRule(service="s3", probability=0.0, error_code="Err")
        matches = sum(1 for _ in range(100) if rule.matches("s3", "Put", "us-east-1"))
        assert matches == 0

    def test_probability_one_always_matches(self):
        rule = FaultRule(service="s3", probability=1.0, error_code="Err")
        matches = sum(1 for _ in range(100) if rule.matches("s3", "Put", "us-east-1"))
        assert matches == 100

    def test_probability_clamped_above_one(self):
        rule = FaultRule(probability=1.5)
        assert rule.probability == 1.0

    def test_probability_clamped_below_zero(self):
        rule = FaultRule(probability=-0.5)
        assert rule.probability == 0.0


class TestFaultRuleDisabled:
    def test_disabled_rule_never_matches(self):
        rule = FaultRule(service="s3", error_code="Err", enabled=False)
        assert rule.matches("s3", "Put", "us-east-1") is False

    def test_enable_disable_toggle(self):
        rule = FaultRule(service="s3", error_code="Err", enabled=True)
        assert rule.matches("s3", "Put", "us-east-1") is True
        rule.enabled = False
        assert rule.matches("s3", "Put", "us-east-1") is False
        rule.enabled = True
        assert rule.matches("s3", "Put", "us-east-1") is True


class TestFaultRuleMatchCount:
    def test_match_count_increments(self):
        rule = FaultRule(service="s3", error_code="Err")
        assert rule.match_count == 0
        rule.matches("s3", "Put", "us-east-1")
        assert rule.match_count == 1
        rule.matches("s3", "Get", "us-east-1")
        assert rule.match_count == 2

    def test_non_match_does_not_increment(self):
        rule = FaultRule(service="s3", error_code="Err")
        rule.matches("dynamodb", "Put", "us-east-1")
        assert rule.match_count == 0


class TestFaultRuleSerializationRoundtrip:
    def test_to_dict_and_from_dict(self):
        original = FaultRule(
            rule_id="test-123",
            service="s3",
            operation="Put.*",
            region="us-west-2",
            error_code="ThrottlingException",
            error_message="slow down",
            status_code=429,
            latency_ms=200,
            probability=0.5,
            enabled=True,
        )
        d = original.to_dict()
        restored = FaultRule.from_dict(d)

        assert restored.rule_id == "test-123"
        assert restored.service == "s3"
        assert restored.operation == "Put.*"
        assert restored.region == "us-west-2"
        assert restored.error_code == "ThrottlingException"
        assert restored.error_message == "slow down"
        assert restored.status_code == 429
        assert restored.latency_ms == 200
        assert restored.probability == 0.5
        assert restored.enabled is True

    def test_from_dict_preserves_timestamps(self):
        d = {
            "rule_id": "abc",
            "service": "s3",
            "created_at": 1000.0,
            "match_count": 42,
        }
        rule = FaultRule.from_dict(d)
        assert rule.created_at == 1000.0
        assert rule.match_count == 42

    def test_from_dict_defaults(self):
        rule = FaultRule.from_dict({})
        assert rule.latency_ms == 0
        assert rule.probability == 1.0
        assert rule.enabled is True


class TestFaultRuleDefaultStatusCode:
    def test_throttling_gets_429(self):
        rule = FaultRule(error_code="ThrottlingException")
        assert rule.status_code == 429

    def test_other_error_gets_500(self):
        rule = FaultRule(error_code="InternalError")
        assert rule.status_code == 500

    def test_no_error_code_gets_500(self):
        rule = FaultRule()
        assert rule.status_code == 500

    def test_explicit_status_code_overrides(self):
        rule = FaultRule(error_code="ThrottlingException", status_code=503)
        assert rule.status_code == 503


class TestFaultRuleStoreThreadSafety:
    def test_concurrent_add_and_find(self):
        store = FaultRuleStore()
        errors = []

        def add_rules():
            try:
                for i in range(50):
                    store.add(
                        FaultRule(
                            service=f"svc-{i}",
                            error_code="Err",
                        )
                    )
            except Exception as e:
                errors.append(e)

        def find_rules():
            try:
                for _ in range(50):
                    store.find_matching("svc-0", "Op", "us-east-1")
            except Exception as e:
                errors.append(e)

        t1 = threading.Thread(target=add_rules)
        t2 = threading.Thread(target=find_rules)
        t1.start()
        t2.start()
        t1.join()
        t2.join()
        assert not errors

    def test_concurrent_add_and_remove(self):
        store = FaultRuleStore()
        errors = []
        ids = []

        def add_rules():
            try:
                for i in range(50):
                    rid = store.add(
                        FaultRule(
                            rule_id=f"rule-{i}",
                            service="s3",
                            error_code="Err",
                        )
                    )
                    ids.append(rid)
            except Exception as e:
                errors.append(e)

        def remove_rules():
            try:
                for i in range(50):
                    store.remove(f"rule-{i}")
            except Exception as e:
                errors.append(e)

        t1 = threading.Thread(target=add_rules)
        t2 = threading.Thread(target=remove_rules)
        t1.start()
        t2.start()
        t1.join()
        t2.join()
        assert not errors


class TestFaultRuleStoreOperations:
    def test_clear_returns_count(self):
        store = FaultRuleStore()
        store.add(FaultRule(service="s3", error_code="Err"))
        store.add(FaultRule(service="sqs", error_code="Err"))
        count = store.clear()
        assert count == 2
        assert store.list_rules() == []

    def test_clear_empty_store(self):
        store = FaultRuleStore()
        assert store.clear() == 0

    def test_remove_nonexistent(self):
        store = FaultRuleStore()
        assert store.remove("nonexistent") is False

    def test_find_matching_returns_first(self):
        """When multiple rules match, the first one wins."""
        store = FaultRuleStore()
        store.add(FaultRule(rule_id="first", service="s3", error_code="Err1"))
        store.add(FaultRule(rule_id="second", service="s3", error_code="Err2"))
        rule = store.find_matching("s3", "Put", "us-east-1")
        assert rule is not None
        assert rule.rule_id == "first"

    def test_find_matching_no_match(self):
        store = FaultRuleStore()
        store.add(FaultRule(service="sqs", error_code="Err"))
        result = store.find_matching("s3", "Put", "us-east-1")
        assert result is None


class TestFaultRuleStoreGlobalSingleton:
    def test_get_fault_store_returns_same_instance(self):
        s1 = get_fault_store()
        s2 = get_fault_store()
        assert s1 is s2
