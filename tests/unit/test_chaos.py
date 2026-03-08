"""Unit tests for chaos engineering module."""

from robotocore.chaos.fault_rules import FaultRule, FaultRuleStore


class TestFaultRule:
    def test_matches_all(self):
        rule = FaultRule(error_code="ThrottlingException")
        assert rule.matches("s3", "PutObject", "us-east-1")
        assert rule.matches("dynamodb", "GetItem", "eu-west-1")

    def test_matches_service_filter(self):
        rule = FaultRule(service="s3", error_code="InternalError")
        assert rule.matches("s3", "PutObject", "us-east-1")
        assert not rule.matches("dynamodb", "GetItem", "us-east-1")

    def test_matches_operation_regex(self):
        rule = FaultRule(service="s3", operation="Put.*", error_code="InternalError")
        assert rule.matches("s3", "PutObject", "us-east-1")
        assert not rule.matches("s3", "GetObject", "us-east-1")

    def test_matches_region_filter(self):
        rule = FaultRule(region="eu-west-1", error_code="InternalError")
        assert rule.matches("s3", "PutObject", "eu-west-1")
        assert not rule.matches("s3", "PutObject", "us-east-1")

    def test_disabled_rule_never_matches(self):
        rule = FaultRule(error_code="InternalError", enabled=False)
        assert not rule.matches("s3", "PutObject", "us-east-1")

    def test_probability_zero_never_matches(self):
        rule = FaultRule(error_code="InternalError", probability=0.0)
        # With probability 0, should never match
        matches = sum(rule.matches("s3", "PutObject", "us-east-1") for _ in range(100))
        assert matches == 0

    def test_to_dict_roundtrip(self):
        rule = FaultRule(
            service="dynamodb",
            operation="GetItem",
            error_code="ThrottlingException",
            latency_ms=500,
        )
        d = rule.to_dict()
        assert d["service"] == "dynamodb"
        assert d["error_code"] == "ThrottlingException"
        assert d["latency_ms"] == 500

        restored = FaultRule.from_dict(d)
        assert restored.service == "dynamodb"
        assert restored.error_code == "ThrottlingException"
        assert restored.latency_ms == 500

    def test_match_count_increments(self):
        rule = FaultRule(error_code="InternalError")
        assert rule.match_count == 0
        rule.matches("s3", "PutObject", "us-east-1")
        assert rule.match_count == 1

    def test_from_dict_preserves_created_at_and_match_count(self):
        rule = FaultRule(service="s3", error_code="InternalError")
        rule.match_count = 42
        original_created = rule.created_at
        d = rule.to_dict()
        restored = FaultRule.from_dict(d)
        assert restored.created_at == original_created
        assert restored.match_count == 42


class TestFaultRuleStore:
    def test_add_and_list(self):
        store = FaultRuleStore()
        rule = FaultRule(service="s3", error_code="InternalError")
        store.add(rule)
        rules = store.list_rules()
        assert len(rules) == 1
        assert rules[0]["service"] == "s3"

    def test_remove(self):
        store = FaultRuleStore()
        rule = FaultRule(service="s3", error_code="InternalError")
        rule_id = store.add(rule)
        assert store.remove(rule_id)
        assert len(store.list_rules()) == 0

    def test_remove_nonexistent(self):
        store = FaultRuleStore()
        assert not store.remove("nonexistent")

    def test_clear(self):
        store = FaultRuleStore()
        store.add(FaultRule(error_code="E1"))
        store.add(FaultRule(error_code="E2"))
        count = store.clear()
        assert count == 2
        assert len(store.list_rules()) == 0

    def test_find_matching(self):
        store = FaultRuleStore()
        store.add(FaultRule(service="s3", error_code="S3Error"))
        store.add(FaultRule(service="dynamodb", error_code="DDBError"))

        rule = store.find_matching("s3", "PutObject", "us-east-1")
        assert rule is not None
        assert rule.error_code == "S3Error"

        rule = store.find_matching("dynamodb", "GetItem", "us-east-1")
        assert rule is not None
        assert rule.error_code == "DDBError"

    def test_find_matching_no_match(self):
        store = FaultRuleStore()
        store.add(FaultRule(service="s3", error_code="S3Error"))
        assert store.find_matching("dynamodb", "GetItem", "us-east-1") is None
