"""Tests for robotocore.services.events.models."""

from robotocore.services.events.models import (
    EventBus,
    EventRule,
    EventsStore,
    EventTarget,
    _match_numeric,
    _match_pattern,
    _match_value_list,
)

# ---------------------------------------------------------------------------
# EventTarget / EventRule / EventBus dataclass basics
# ---------------------------------------------------------------------------


class TestEventTarget:
    def test_basic_fields(self):
        t = EventTarget(target_id="t1", arn="arn:aws:sqs:us-east-1:123:q1")
        assert t.target_id == "t1"
        assert t.arn == "arn:aws:sqs:us-east-1:123:q1"
        assert t.role_arn == ""
        assert t.input is None
        assert t.input_path is None
        assert t.input_transformer is None

    def test_optional_fields(self):
        t = EventTarget(
            target_id="t2",
            arn="arn:aws:lambda:us-east-1:123:function:f",
            role_arn="arn:aws:iam::123:role/r",
            input='{"key": "val"}',
            input_path="$.detail",
            input_transformer={"InputPathsMap": {}, "InputTemplate": "<x>"},
        )
        assert t.role_arn.endswith("role/r")
        assert t.input == '{"key": "val"}'
        assert t.input_path == "$.detail"
        assert t.input_transformer["InputTemplate"] == "<x>"


class TestEventRule:
    def test_arn_format(self):
        r = EventRule(
            name="my-rule", event_bus_name="default", region="us-west-2", account_id="111222333444"
        )
        assert r.arn == "arn:aws:events:us-west-2:111222333444:rule/my-rule"

    def test_matches_event_disabled(self):
        r = EventRule(
            name="r",
            event_bus_name="default",
            region="us-east-1",
            account_id="123",
            state="DISABLED",
            event_pattern={"source": ["my.app"]},
        )
        assert r.matches_event({"source": "my.app"}) is False

    def test_matches_event_schedule_only(self):
        r = EventRule(
            name="r",
            event_bus_name="default",
            region="us-east-1",
            account_id="123",
            schedule_expression="rate(5 minutes)",
        )
        # No event_pattern, but has schedule -> True
        assert r.matches_event({}) is True

    def test_matches_event_no_pattern_no_schedule(self):
        r = EventRule(name="r", event_bus_name="default", region="us-east-1", account_id="123")
        # No pattern, no schedule -> False
        assert r.matches_event({}) is False

    def test_matches_event_with_pattern(self):
        r = EventRule(
            name="r",
            event_bus_name="default",
            region="us-east-1",
            account_id="123",
            event_pattern={"source": ["my.app"]},
        )
        assert r.matches_event({"source": "my.app"}) is True
        assert r.matches_event({"source": "other"}) is False

    def test_default_state_is_enabled(self):
        r = EventRule(name="r", event_bus_name="default", region="us-east-1", account_id="123")
        assert r.state == "ENABLED"


class TestEventBus:
    def test_arn_format(self):
        b = EventBus(name="custom", region="eu-west-1", account_id="999")
        assert b.arn == "arn:aws:events:eu-west-1:999:event-bus/custom"

    def test_rules_initially_empty(self):
        b = EventBus(name="x", region="r", account_id="a")
        assert b.rules == {}


# ---------------------------------------------------------------------------
# EventsStore
# ---------------------------------------------------------------------------


class TestEventsStore:
    def make_store(self) -> EventsStore:
        return EventsStore()

    # -- Bus operations --
    def test_default_bus_exists(self):
        store = self.make_store()
        assert store.get_bus("default") is not None

    def test_ensure_default_bus_updates_region(self):
        store = self.make_store()
        store.ensure_default_bus("ap-south-1", "999")
        bus = store.get_bus("default")
        assert bus.region == "ap-south-1"
        assert bus.account_id == "999"

    def test_create_and_get_bus(self):
        store = self.make_store()
        bus = store.create_event_bus("mybus", "us-east-1", "123")
        assert bus.name == "mybus"
        assert store.get_bus("mybus") is bus

    def test_delete_bus(self):
        store = self.make_store()
        store.create_event_bus("temp", "us-east-1", "123")
        assert store.delete_bus("temp") is True
        assert store.get_bus("temp") is None

    def test_cannot_delete_default_bus(self):
        store = self.make_store()
        assert store.delete_bus("default") is False
        assert store.get_bus("default") is not None

    def test_delete_nonexistent_bus(self):
        store = self.make_store()
        assert store.delete_bus("nope") is False

    def test_list_buses(self):
        store = self.make_store()
        store.create_event_bus("a", "us-east-1", "123")
        store.create_event_bus("b", "us-east-1", "123")
        names = {b.name for b in store.list_buses()}
        assert names == {"default", "a", "b"}

    # -- Rule operations --
    def test_put_and_get_rule(self):
        store = self.make_store()
        rule = store.put_rule(
            "r1", "default", "us-east-1", "123", event_pattern={"source": ["app"]}
        )
        assert rule.name == "r1"
        assert store.get_rule("r1") is rule

    def test_put_rule_falls_back_to_default_bus(self):
        store = self.make_store()
        # Put rule on a bus that doesn't exist -> falls back to default bus
        rule = store.put_rule("r1", "nonexistent", "us-east-1", "123")
        # Rule is stored in the default bus
        assert store.get_rule("r1", "default") is rule

    def test_delete_rule(self):
        store = self.make_store()
        store.put_rule("r1", "default", "us-east-1", "123")
        assert store.delete_rule("r1") is True
        assert store.get_rule("r1") is None

    def test_delete_nonexistent_rule(self):
        store = self.make_store()
        assert store.delete_rule("nope") is False

    def test_delete_rule_wrong_bus(self):
        store = self.make_store()
        store.put_rule("r1", "default", "us-east-1", "123")
        store.create_event_bus("other", "us-east-1", "123")
        assert store.delete_rule("r1", "other") is False

    def test_list_rules_empty(self):
        store = self.make_store()
        assert store.list_rules() == []

    def test_list_rules_with_prefix(self):
        store = self.make_store()
        store.put_rule("abc-1", "default", "us-east-1", "123")
        store.put_rule("abc-2", "default", "us-east-1", "123")
        store.put_rule("xyz-1", "default", "us-east-1", "123")
        result = store.list_rules(prefix="abc")
        assert len(result) == 2
        assert all(r.name.startswith("abc") for r in result)

    def test_list_rules_nonexistent_bus(self):
        store = self.make_store()
        assert store.list_rules("nope") == []

    def test_get_rule_nonexistent_bus(self):
        store = self.make_store()
        assert store.get_rule("r1", "nope") is None

    # -- Target operations --
    def test_put_targets(self):
        store = self.make_store()
        store.put_rule("r1", "default", "us-east-1", "123")
        failed = store.put_targets(
            "r1",
            "default",
            [
                {"Id": "t1", "Arn": "arn:aws:sqs:us-east-1:123:q"},
                {
                    "Id": "t2",
                    "Arn": "arn:aws:lambda:us-east-1:123:function:f",
                    "RoleArn": "arn:aws:iam::123:role/r",
                },
            ],
        )
        assert failed == []
        targets = store.list_targets("r1")
        assert len(targets) == 2

    def test_put_targets_nonexistent_rule(self):
        store = self.make_store()
        failed = store.put_targets("nope", "default", [{"Id": "t1", "Arn": "arn:a"}])
        assert len(failed) == 1
        assert failed[0]["ErrorCode"] == "ResourceNotFoundException"

    def test_put_targets_nonexistent_bus(self):
        store = self.make_store()
        failed = store.put_targets("r1", "nope", [{"Id": "t1", "Arn": "a"}])
        assert len(failed) == 1
        assert failed[0]["ErrorCode"] == "ResourceNotFoundException"

    def test_remove_targets(self):
        store = self.make_store()
        store.put_rule("r1", "default", "us-east-1", "123")
        store.put_targets("r1", "default", [{"Id": "t1", "Arn": "a"}, {"Id": "t2", "Arn": "b"}])
        failed = store.remove_targets("r1", "default", ["t1"])
        assert failed == []
        assert len(store.list_targets("r1")) == 1

    def test_remove_targets_missing_id(self):
        store = self.make_store()
        store.put_rule("r1", "default", "us-east-1", "123")
        failed = store.remove_targets("r1", "default", ["nonexistent"])
        assert len(failed) == 1
        assert failed[0]["ErrorCode"] == "ResourceNotFoundException"

    def test_remove_targets_nonexistent_bus(self):
        store = self.make_store()
        assert store.remove_targets("r1", "nope", ["t1"]) == []

    def test_remove_targets_nonexistent_rule(self):
        store = self.make_store()
        assert store.remove_targets("nope", "default", ["t1"]) == []

    def test_list_targets_empty(self):
        store = self.make_store()
        store.put_rule("r1", "default", "us-east-1", "123")
        assert store.list_targets("r1") == []

    def test_list_targets_nonexistent_rule(self):
        store = self.make_store()
        assert store.list_targets("nope") == []

    def test_list_targets_nonexistent_bus(self):
        store = self.make_store()
        assert store.list_targets("r1", "nope") == []


# ---------------------------------------------------------------------------
# Pattern matching (_match_pattern, _match_value_list, _match_numeric)
# ---------------------------------------------------------------------------


class TestMatchPattern:
    def test_exact_string_match(self):
        assert _match_pattern({"source": ["my.app"]}, {"source": "my.app"}) is True

    def test_exact_string_no_match(self):
        assert _match_pattern({"source": ["my.app"]}, {"source": "other"}) is False

    def test_nested_object(self):
        pattern = {"detail": {"type": ["order"]}}
        assert _match_pattern(pattern, {"detail": {"type": "order"}}) is True
        assert _match_pattern(pattern, {"detail": {"type": "payment"}}) is False

    def test_nested_non_dict_event_value(self):
        pattern = {"detail": {"type": ["order"]}}
        assert _match_pattern(pattern, {"detail": "string"}) is False

    def test_missing_key_fails(self):
        assert _match_pattern({"source": ["a"]}, {}) is False

    def test_empty_pattern_matches_all(self):
        assert _match_pattern({}, {"anything": "here"}) is True

    def test_direct_value_match(self):
        # Non-list, non-dict pattern value -> direct comparison
        assert _match_pattern({"x": 42}, {"x": 42}) is True
        assert _match_pattern({"x": 42}, {"x": 99}) is False


class TestMatchValueList:
    def test_simple_values(self):
        assert _match_value_list(["a", "b"], "a") is True
        assert _match_value_list(["a", "b"], "c") is False

    def test_event_value_is_list(self):
        assert _match_value_list(["a"], ["a", "b"]) is True
        assert _match_value_list(["c"], ["a", "b"]) is False

    def test_prefix_match(self):
        assert _match_value_list([{"prefix": "foo"}], "foobar") is True
        assert _match_value_list([{"prefix": "foo"}], "barfoo") is False

    def test_suffix_match(self):
        assert _match_value_list([{"suffix": ".jpg"}], "photo.jpg") is True
        assert _match_value_list([{"suffix": ".jpg"}], "photo.png") is False

    def test_anything_but_list(self):
        assert _match_value_list([{"anything-but": ["x", "y"]}], "z") is True
        assert _match_value_list([{"anything-but": ["x", "y"]}], "x") is False

    def test_anything_but_scalar(self):
        assert _match_value_list([{"anything-but": "x"}], "y") is True
        assert _match_value_list([{"anything-but": "x"}], "x") is False

    def test_exists_true(self):
        assert _match_value_list([{"exists": True}], "some_val") is True

    def test_exists_true_when_none(self):
        # exists: True, but value is None -> False
        assert _match_value_list([{"exists": True}], None) is False

    def test_exists_false_when_none(self):
        assert _match_value_list([{"exists": False}], None) is True

    def test_exists_false_when_present(self):
        # exists: False but value is present -> False
        assert _match_value_list([{"exists": False}], "val") is False

    def test_none_value_no_exists_matcher(self):
        assert _match_value_list(["a", "b"], None) is False

    def test_prefix_non_string(self):
        assert _match_value_list([{"prefix": "foo"}], 123) is False

    def test_suffix_non_string(self):
        assert _match_value_list([{"suffix": "bar"}], 123) is False


class TestMatchNumeric:
    def test_greater_than(self):
        assert _match_numeric([">", 10], 15) is True
        assert _match_numeric([">", 10], 5) is False

    def test_greater_equal(self):
        assert _match_numeric([">=", 10], 10) is True
        assert _match_numeric([">=", 10], 9) is False

    def test_less_than(self):
        assert _match_numeric(["<", 10], 5) is True
        assert _match_numeric(["<", 10], 15) is False

    def test_less_equal(self):
        assert _match_numeric(["<=", 10], 10) is True
        assert _match_numeric(["<=", 10], 11) is False

    def test_equal(self):
        assert _match_numeric(["=", 42], 42) is True
        assert _match_numeric(["=", 42], 43) is False

    def test_range(self):
        # Combined: >= 0 AND < 100
        assert _match_numeric([">=", 0, "<", 100], 50) is True
        assert _match_numeric([">=", 0, "<", 100], 100) is False
        assert _match_numeric([">=", 0, "<", 100], -1) is False

    def test_non_numeric_value(self):
        assert _match_numeric([">", 10], "string") is False

    def test_incomplete_ops(self):
        # Odd number of elements -> last op has no operand
        assert _match_numeric([">"], 5) is False

    def test_numeric_in_value_list(self):
        assert _match_value_list([{"numeric": [">", 0, "<=", 100]}], 50) is True
        assert _match_value_list([{"numeric": [">", 0, "<=", 100]}], 0) is False
