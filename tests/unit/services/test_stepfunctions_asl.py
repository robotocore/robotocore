"""Unit tests for Step Functions ASL interpreter."""

import pytest

from robotocore.services.stepfunctions.asl import (
    ASLExecutionError,
    ASLExecutor,
    _apply_path,
    _apply_result_path,
    _evaluate_choice_rule,
    _resolve_parameters,
    _resolve_path,
)

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_executor(states: dict, start_at: str = "Start") -> ASLExecutor:
    return ASLExecutor({"StartAt": start_at, "States": states})


# ---------------------------------------------------------------------------
# Pass state
# ---------------------------------------------------------------------------


class TestPassState:
    def test_pass_returns_input(self):
        executor = _make_executor(
            {
                "Start": {"Type": "Pass", "End": True},
            }
        )
        assert executor.execute({"key": "value"}) == {"key": "value"}

    def test_pass_with_result(self):
        executor = _make_executor(
            {
                "Start": {"Type": "Pass", "Result": {"fixed": 42}, "End": True},
            }
        )
        assert executor.execute({}) == {"fixed": 42}

    def test_pass_with_result_path(self):
        executor = _make_executor(
            {
                "Start": {
                    "Type": "Pass",
                    "Result": "hello",
                    "ResultPath": "$.greeting",
                    "End": True,
                },
            }
        )
        result = executor.execute({"existing": 1})
        assert result == {"existing": 1, "greeting": "hello"}

    def test_pass_chain(self):
        executor = _make_executor(
            {
                "First": {"Type": "Pass", "Result": {"step": 1}, "Next": "Second"},
                "Second": {"Type": "Pass", "Result": {"step": 2}, "End": True},
            },
            start_at="First",
        )
        assert executor.execute({}) == {"step": 2}


# ---------------------------------------------------------------------------
# Task state (with mocked Lambda)
# ---------------------------------------------------------------------------


class TestTaskState:
    def test_unknown_resource_returns_input(self):
        executor = _make_executor(
            {
                "Start": {
                    "Type": "Task",
                    "Resource": "arn:aws:states:::unknown:action",
                    "End": True,
                },
            }
        )
        result = executor.execute({"data": 1})
        assert result == {"data": 1}

    def test_function_arn_resource_with_missing_lambda(self):
        """When Lambda not found, raises ASLExecutionError."""
        executor = _make_executor(
            {
                "Start": {
                    "Type": "Task",
                    "Resource": "arn:aws:lambda:us-east-1:123456789012:function:noSuchFn",
                    "End": True,
                },
            }
        )
        with pytest.raises(ASLExecutionError, match="Lambda.ServiceException"):
            executor.execute({"data": 1})


# ---------------------------------------------------------------------------
# Choice state
# ---------------------------------------------------------------------------


class TestChoiceState:
    def test_string_equals_match(self):
        executor = _make_executor(
            {
                "Start": {
                    "Type": "Choice",
                    "Choices": [
                        {"Variable": "$.status", "StringEquals": "active", "Next": "Active"},
                    ],
                    "Default": "Inactive",
                },
                "Active": {"Type": "Pass", "Result": "active", "End": True},
                "Inactive": {"Type": "Pass", "Result": "inactive", "End": True},
            }
        )
        assert executor.execute({"status": "active"}) == "active"

    def test_string_equals_no_match_uses_default(self):
        executor = _make_executor(
            {
                "Start": {
                    "Type": "Choice",
                    "Choices": [
                        {"Variable": "$.status", "StringEquals": "active", "Next": "Active"},
                    ],
                    "Default": "Inactive",
                },
                "Active": {"Type": "Pass", "Result": "active", "End": True},
                "Inactive": {"Type": "Pass", "Result": "inactive", "End": True},
            }
        )
        assert executor.execute({"status": "disabled"}) == "inactive"

    def test_no_match_no_default_raises(self):
        executor = _make_executor(
            {
                "Start": {
                    "Type": "Choice",
                    "Choices": [
                        {"Variable": "$.status", "StringEquals": "active", "Next": "Active"},
                    ],
                },
                "Active": {"Type": "Pass", "Result": "active", "End": True},
            }
        )
        with pytest.raises(ASLExecutionError, match="NoChoiceMatched"):
            executor.execute({"status": "disabled"})

    def test_numeric_greater_than(self):
        executor = _make_executor(
            {
                "Start": {
                    "Type": "Choice",
                    "Choices": [
                        {"Variable": "$.val", "NumericGreaterThan": 10, "Next": "Big"},
                    ],
                    "Default": "Small",
                },
                "Big": {"Type": "Pass", "Result": "big", "End": True},
                "Small": {"Type": "Pass", "Result": "small", "End": True},
            }
        )
        assert executor.execute({"val": 20}) == "big"
        assert executor.execute({"val": 5}) == "small"

    def test_boolean_equals(self):
        executor = _make_executor(
            {
                "Start": {
                    "Type": "Choice",
                    "Choices": [
                        {"Variable": "$.flag", "BooleanEquals": True, "Next": "Yes"},
                    ],
                    "Default": "No",
                },
                "Yes": {"Type": "Pass", "Result": "yes", "End": True},
                "No": {"Type": "Pass", "Result": "no", "End": True},
            }
        )
        assert executor.execute({"flag": True}) == "yes"
        assert executor.execute({"flag": False}) == "no"


# ---------------------------------------------------------------------------
# Wait state
# ---------------------------------------------------------------------------


class TestWaitState:
    def test_wait_seconds(self):
        executor = _make_executor(
            {
                "Start": {"Type": "Wait", "Seconds": 0, "Next": "Done"},
                "Done": {"Type": "Pass", "End": True},
            }
        )
        result = executor.execute({"x": 1})
        assert result == {"x": 1}

    def test_wait_timestamp(self):
        executor = _make_executor(
            {
                "Start": {"Type": "Wait", "Timestamp": "2099-01-01T00:00:00Z", "Next": "Done"},
                "Done": {"Type": "Pass", "End": True},
            }
        )
        result = executor.execute({"y": 2})
        assert result == {"y": 2}


# ---------------------------------------------------------------------------
# Succeed state
# ---------------------------------------------------------------------------


class TestSucceedState:
    def test_succeed(self):
        executor = _make_executor(
            {
                "Start": {"Type": "Succeed"},
            }
        )
        result = executor.execute({"data": "ok"})
        assert result == {"data": "ok"}


# ---------------------------------------------------------------------------
# Fail state
# ---------------------------------------------------------------------------


class TestFailState:
    def test_fail_raises(self):
        executor = _make_executor(
            {
                "Start": {"Type": "Fail", "Error": "CustomError", "Cause": "something went wrong"},
            }
        )
        with pytest.raises(ASLExecutionError) as exc_info:
            executor.execute({})
        assert exc_info.value.error == "CustomError"
        assert exc_info.value.cause == "something went wrong"


# ---------------------------------------------------------------------------
# Parallel state
# ---------------------------------------------------------------------------


class TestParallelState:
    def test_parallel_branches(self):
        executor = _make_executor(
            {
                "Start": {
                    "Type": "Parallel",
                    "Branches": [
                        {
                            "StartAt": "B1",
                            "States": {"B1": {"Type": "Pass", "Result": "branch1", "End": True}},
                        },
                        {
                            "StartAt": "B2",
                            "States": {"B2": {"Type": "Pass", "Result": "branch2", "End": True}},
                        },
                    ],
                    "End": True,
                },
            }
        )
        result = executor.execute({})
        assert result == ["branch1", "branch2"]

    def test_parallel_passes_input_to_branches(self):
        executor = _make_executor(
            {
                "Start": {
                    "Type": "Parallel",
                    "Branches": [
                        {"StartAt": "B1", "States": {"B1": {"Type": "Pass", "End": True}}},
                    ],
                    "End": True,
                },
            }
        )
        result = executor.execute({"key": "val"})
        assert result == [{"key": "val"}]


# ---------------------------------------------------------------------------
# Map state
# ---------------------------------------------------------------------------


class TestMapState:
    def test_map_over_list(self):
        executor = _make_executor(
            {
                "Start": {
                    "Type": "Map",
                    "ItemsPath": "$.items",
                    "Iterator": {
                        "StartAt": "Process",
                        "States": {"Process": {"Type": "Pass", "End": True}},
                    },
                    "End": True,
                },
            }
        )
        result = executor.execute({"items": [{"a": 1}, {"a": 2}]})
        assert result == [{"a": 1}, {"a": 2}]

    def test_map_with_item_processor(self):
        executor = _make_executor(
            {
                "Start": {
                    "Type": "Map",
                    "ItemsPath": "$.items",
                    "ItemProcessor": {
                        "StartAt": "Process",
                        "States": {"Process": {"Type": "Pass", "Result": "processed", "End": True}},
                    },
                    "End": True,
                },
            }
        )
        result = executor.execute({"items": ["x", "y"]})
        assert result == ["processed", "processed"]

    def test_map_non_list_items_wrapped(self):
        executor = _make_executor(
            {
                "Start": {
                    "Type": "Map",
                    "ItemsPath": "$.item",
                    "Iterator": {
                        "StartAt": "Process",
                        "States": {"Process": {"Type": "Pass", "End": True}},
                    },
                    "End": True,
                },
            }
        )
        result = executor.execute({"item": "single"})
        assert result == ["single"]


# ---------------------------------------------------------------------------
# Error handling (Catch)
# ---------------------------------------------------------------------------


class TestErrorHandling:
    """Test _handle_error and Catch blocks directly, since the execute() loop
    checks End/Succeed before allowing Catch to redirect flow."""

    def test_handle_error_catches_specific_error(self):
        executor = _make_executor({"Start": {"Type": "Pass", "End": True}})
        state_def = {
            "Catch": [{"ErrorEquals": ["ValueError"], "Next": "Fallback"}],
        }
        next_state, output = executor._handle_error(state_def, {"orig": 1}, "ValueError", "boom")
        assert next_state == "Fallback"
        assert output["Error"] == "ValueError"
        assert output["Cause"] == "boom"

    def test_handle_error_catches_states_all(self):
        executor = _make_executor({"Start": {"Type": "Pass", "End": True}})
        state_def = {
            "Catch": [{"ErrorEquals": ["States.ALL"], "Next": "Recover"}],
        }
        next_state, output = executor._handle_error(state_def, {}, "RuntimeError", "fail")
        assert next_state == "Recover"
        assert output["Error"] == "RuntimeError"

    def test_handle_error_with_result_path(self):
        executor = _make_executor({"Start": {"Type": "Pass", "End": True}})
        state_def = {
            "Catch": [{"ErrorEquals": ["States.ALL"], "ResultPath": "$.error", "Next": "Done"}],
        }
        next_state, output = executor._handle_error(
            state_def,
            {"input": "data"},
            "TypeError",
            "bad",
        )
        assert next_state == "Done"
        assert output["input"] == "data"
        assert output["error"]["Error"] == "TypeError"

    def test_handle_error_no_match(self):
        executor = _make_executor({"Start": {"Type": "Pass", "End": True}})
        state_def = {
            "Catch": [{"ErrorEquals": ["SomeOtherError"], "Next": "Fallback"}],
        }
        next_state, output = executor._handle_error(state_def, {"x": 1}, "ValueError", "nope")
        assert next_state is None
        assert output == {"x": 1}

    def test_handle_error_no_catch_blocks(self):
        executor = _make_executor({"Start": {"Type": "Pass", "End": True}})
        next_state, output = executor._handle_error({}, {"x": 1}, "Error", "cause")
        assert next_state is None


# ---------------------------------------------------------------------------
# State not found
# ---------------------------------------------------------------------------


class TestExecutionErrors:
    def test_missing_state(self):
        executor = _make_executor({"Start": {"Type": "Pass", "Next": "Missing"}})
        with pytest.raises(ASLExecutionError, match="not found"):
            executor.execute({})

    def test_unknown_state_type(self):
        executor = _make_executor(
            {
                "Start": {"Type": "Bogus", "End": True},
            }
        )
        with pytest.raises(ASLExecutionError, match="Unknown state type"):
            executor.execute({})

    def test_max_steps_exceeded(self):
        executor = _make_executor(
            {
                "Start": {"Type": "Pass", "Next": "Start"},
            }
        )
        executor.max_steps = 5
        with pytest.raises(ASLExecutionError, match="Maximum execution steps"):
            executor.execute({})


# ---------------------------------------------------------------------------
# InputPath / OutputPath / ResultPath / Parameters
# ---------------------------------------------------------------------------


class TestPathProcessing:
    def test_input_path_filters(self):
        executor = _make_executor(
            {
                "Start": {"Type": "Pass", "InputPath": "$.nested", "End": True},
            }
        )
        result = executor.execute({"nested": {"inner": 1}, "other": 2})
        assert result == {"inner": 1}

    def test_output_path_filters(self):
        executor = _make_executor(
            {
                "Start": {
                    "Type": "Pass",
                    "Result": {"a": 1, "b": 2},
                    "OutputPath": "$.a",
                    "End": True,
                },
            }
        )
        result = executor.execute({})
        assert result == 1

    def test_result_path_null_discards_result(self):
        executor = _make_executor(
            {
                "Start": {"Type": "Pass", "Result": "ignored", "ResultPath": None, "End": True},
            }
        )
        result = executor.execute({"original": True})
        assert result == {"original": True}

    def test_parameters_static(self):
        executor = _make_executor(
            {
                "Start": {
                    "Type": "Pass",
                    "Parameters": {"fixed": "value"},
                    "End": True,
                },
            }
        )
        result = executor.execute({"anything": 1})
        assert result == {"fixed": "value"}

    def test_parameters_dynamic_reference(self):
        executor = _make_executor(
            {
                "Start": {
                    "Type": "Pass",
                    "Parameters": {"name.$": "$.user.name"},
                    "End": True,
                },
            }
        )
        result = executor.execute({"user": {"name": "Alice"}})
        assert result == {"name": "Alice"}

    def test_result_selector(self):
        """ResultSelector filters the result before ResultPath."""
        executor = _make_executor(
            {
                "Start": {
                    "Type": "Pass",
                    "Result": {"big": "data", "small": "value"},
                    "ResultSelector": {"picked.$": "$.small"},
                    "End": True,
                },
            }
        )
        result = executor.execute({})
        assert result == {"picked": "value"}


# ---------------------------------------------------------------------------
# JSONPath utilities
# ---------------------------------------------------------------------------


class TestApplyPath:
    def test_dollar_returns_whole(self):
        assert _apply_path({"a": 1}, "$") == {"a": 1}

    def test_none_returns_empty_dict(self):
        assert _apply_path({"a": 1}, None) == {}

    def test_nested_path(self):
        assert _apply_path({"a": {"b": 42}}, "$.a.b") == 42


class TestResolvePath:
    def test_dollar(self):
        assert _resolve_path({"x": 1}, "$") == {"x": 1}

    def test_nested(self):
        assert _resolve_path({"a": {"b": {"c": 3}}}, "$.a.b.c") == 3

    def test_array_index(self):
        assert _resolve_path({"items": [10, 20, 30]}, "$.items[1]") == 20

    def test_missing_key(self):
        assert _resolve_path({"a": 1}, "$.b") is None

    def test_empty_path(self):
        assert _resolve_path({"a": 1}, "") == {"a": 1}

    def test_non_dollar_prefix(self):
        assert _resolve_path({"a": 1}, "nope") == {"a": 1}


class TestApplyResultPath:
    def test_dollar_replaces(self):
        assert _apply_result_path({"old": 1}, {"new": 2}, "$") == {"new": 2}

    def test_none_discards_result(self):
        assert _apply_result_path({"old": 1}, {"new": 2}, None) == {"old": 1}

    def test_nested_path(self):
        result = _apply_result_path({"existing": 1}, "value", "$.output.data")
        assert result == {"existing": 1, "output": {"data": "value"}}

    def test_non_dollar_prefix(self):
        result = _apply_result_path({"x": 1}, "y", "notdollar")
        assert result == "y"


class TestResolveParameters:
    def test_static_values(self):
        result = _resolve_parameters({"key": "value"}, {})
        assert result == {"key": "value"}

    def test_dynamic_reference(self):
        result = _resolve_parameters({"name.$": "$.user"}, {"user": "Alice"})
        assert result == {"name": "Alice"}

    def test_nested_dict(self):
        result = _resolve_parameters({"outer": {"inner": 42}}, {})
        assert result == {"outer": {"inner": 42}}

    def test_list_values(self):
        result = _resolve_parameters({"items": [{"a": 1}]}, {})
        assert result == {"items": [{"a": 1}]}


# ---------------------------------------------------------------------------
# Choice rule evaluator
# ---------------------------------------------------------------------------


class TestEvaluateChoiceRule:
    def test_string_equals(self):
        assert (
            _evaluate_choice_rule(
                {"Variable": "$.s", "StringEquals": "yes"},
                {"s": "yes"},
            )
            is True
        )
        assert (
            _evaluate_choice_rule(
                {"Variable": "$.s", "StringEquals": "yes"},
                {"s": "no"},
            )
            is False
        )

    def test_string_greater_than(self):
        assert (
            _evaluate_choice_rule(
                {"Variable": "$.s", "StringGreaterThan": "a"},
                {"s": "b"},
            )
            is True
        )
        assert (
            _evaluate_choice_rule(
                {"Variable": "$.s", "StringGreaterThan": "b"},
                {"s": "a"},
            )
            is False
        )

    def test_string_less_than(self):
        assert _evaluate_choice_rule({"Variable": "$.s", "StringLessThan": "b"}, {"s": "a"}) is True

    def test_string_matches(self):
        assert (
            _evaluate_choice_rule(
                {"Variable": "$.s", "StringMatches": "hello*"},
                {"s": "hello world"},
            )
            is True
        )
        assert (
            _evaluate_choice_rule(
                {"Variable": "$.s", "StringMatches": "hello*"},
                {"s": "goodbye"},
            )
            is False
        )

    def test_string_equals_path(self):
        data = {"a": "same", "b": "same"}
        assert _evaluate_choice_rule({"Variable": "$.a", "StringEqualsPath": "$.b"}, data) is True

    def test_numeric_equals(self):
        assert _evaluate_choice_rule({"Variable": "$.n", "NumericEquals": 5}, {"n": 5}) is True
        assert _evaluate_choice_rule({"Variable": "$.n", "NumericEquals": 5}, {"n": 6}) is False

    def test_numeric_greater_than_equals(self):
        assert (
            _evaluate_choice_rule(
                {"Variable": "$.n", "NumericGreaterThanEquals": 5},
                {"n": 5},
            )
            is True
        )
        assert (
            _evaluate_choice_rule(
                {"Variable": "$.n", "NumericGreaterThanEquals": 5},
                {"n": 4},
            )
            is False
        )

    def test_numeric_less_than(self):
        assert _evaluate_choice_rule({"Variable": "$.n", "NumericLessThan": 10}, {"n": 5}) is True

    def test_numeric_less_than_equals(self):
        assert (
            _evaluate_choice_rule(
                {"Variable": "$.n", "NumericLessThanEquals": 10},
                {"n": 10},
            )
            is True
        )
        assert (
            _evaluate_choice_rule(
                {"Variable": "$.n", "NumericLessThanEquals": 10},
                {"n": 11},
            )
            is False
        )

    def test_boolean_equals(self):
        assert (
            _evaluate_choice_rule(
                {"Variable": "$.b", "BooleanEquals": True},
                {"b": True},
            )
            is True
        )
        assert (
            _evaluate_choice_rule(
                {"Variable": "$.b", "BooleanEquals": True},
                {"b": False},
            )
            is False
        )

    def test_is_present(self):
        assert _evaluate_choice_rule({"Variable": "$.x", "IsPresent": True}, {"x": 1}) is True
        assert _evaluate_choice_rule({"Variable": "$.x", "IsPresent": True}, {"y": 1}) is False

    def test_is_null(self):
        assert _evaluate_choice_rule({"Variable": "$.x", "IsNull": True}, {"y": 1}) is True
        assert _evaluate_choice_rule({"Variable": "$.x", "IsNull": True}, {"x": 1}) is False

    def test_is_string(self):
        assert _evaluate_choice_rule({"Variable": "$.x", "IsString": True}, {"x": "hi"}) is True
        assert _evaluate_choice_rule({"Variable": "$.x", "IsString": True}, {"x": 42}) is False

    def test_is_numeric(self):
        assert _evaluate_choice_rule({"Variable": "$.x", "IsNumeric": True}, {"x": 3.14}) is True
        assert _evaluate_choice_rule({"Variable": "$.x", "IsNumeric": True}, {"x": "hi"}) is False

    def test_is_boolean(self):
        assert _evaluate_choice_rule({"Variable": "$.x", "IsBoolean": True}, {"x": True}) is True
        assert _evaluate_choice_rule({"Variable": "$.x", "IsBoolean": True}, {"x": 1}) is False

    def test_and_operator(self):
        rule = {
            "And": [
                {"Variable": "$.a", "NumericGreaterThan": 0},
                {"Variable": "$.b", "NumericGreaterThan": 0},
            ],
            "Next": "Both",
        }
        assert _evaluate_choice_rule(rule, {"a": 1, "b": 1}) is True
        assert _evaluate_choice_rule(rule, {"a": 1, "b": -1}) is False

    def test_or_operator(self):
        rule = {
            "Or": [
                {"Variable": "$.a", "NumericGreaterThan": 0},
                {"Variable": "$.b", "NumericGreaterThan": 0},
            ],
            "Next": "Either",
        }
        assert _evaluate_choice_rule(rule, {"a": -1, "b": 1}) is True
        assert _evaluate_choice_rule(rule, {"a": -1, "b": -1}) is False

    def test_not_operator(self):
        rule = {
            "Not": {"Variable": "$.x", "StringEquals": "bad"},
            "Next": "Good",
        }
        assert _evaluate_choice_rule(rule, {"x": "good"}) is True
        assert _evaluate_choice_rule(rule, {"x": "bad"}) is False

    def test_no_matching_operator_returns_false(self):
        assert _evaluate_choice_rule({"Variable": "$.x"}, {"x": 1}) is False
