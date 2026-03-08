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
