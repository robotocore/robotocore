"""Tests for Step Functions advanced features (Milestone 6).

Tests cover:
- All intrinsic functions (States.*)
- Basic JSONata evaluation
- Map state with MaxConcurrency
- Execution history events
- Service integrations (DynamoDB via mock)
- Express workflow sync execution
- Callback pattern with task tokens
- Data flow operators (InputPath, Parameters, ResultSelector, ResultPath, OutputPath)
"""

import hashlib
import json
import threading
import time
import uuid

import pytest

from robotocore.services.stepfunctions.asl import (
    ASLExecutionError,
    ASLExecutor,
    _task_tokens,
    _token_lock,
    send_task_failure,
    send_task_heartbeat,
    send_task_success,
)
from robotocore.services.stepfunctions.history import (
    ExecutionHistory,
    _detail_key,
    _resource_type,
    _state_entered_type,
    _state_exited_type,
)
from robotocore.services.stepfunctions.intrinsics import (
    IntrinsicError,
    evaluate_intrinsic,
)
from robotocore.services.stepfunctions.jsonata import (
    evaluate_jsonata,
)
from robotocore.services.stepfunctions.provider import (
    SfnError,
    _create_state_machine,
    _execution_histories,
    _executions,
    _get_execution_history,
    _list_executions,
    _start_execution,
    _start_sync_execution,
    _state_machines,
    _stop_execution,
)


def _clear_state():
    _state_machines.clear()
    _executions.clear()
    _execution_histories.clear()


def _make_executor(states: dict, start_at: str = "Start", execution_arn: str = "") -> ASLExecutor:
    return ASLExecutor({"StartAt": start_at, "States": states}, execution_arn=execution_arn)


_SIMPLE_DEFINITION = json.dumps(
    {"StartAt": "Pass", "States": {"Pass": {"Type": "Pass", "End": True}}}
)


# ===========================================================================
# Intrinsic Functions
# ===========================================================================


class TestIntrinsicFormat:
    def test_format_no_args(self):
        assert evaluate_intrinsic("States.Format('hello')") == "hello"

    def test_format_single_arg(self):
        result = evaluate_intrinsic("States.Format('hello {}', 'world')")
        assert result == "hello world"

    def test_format_multiple_args(self):
        result = evaluate_intrinsic("States.Format('{} + {} = {}', 1, 2, 3)")
        assert result == "1 + 2 = 3"

    def test_format_with_jsonpath(self):
        result = evaluate_intrinsic("States.Format('Hello, {}!', $.name)", {"name": "Alice"})
        assert result == "Hello, Alice!"


class TestIntrinsicStringToJson:
    def test_parse_object(self):
        result = evaluate_intrinsic('States.StringToJson(\'{"key": "val"}\')')
        assert result == {"key": "val"}

    def test_parse_array(self):
        result = evaluate_intrinsic("States.StringToJson('[1, 2, 3]')")
        assert result == [1, 2, 3]

    def test_parse_string(self):
        result = evaluate_intrinsic("States.StringToJson('\"hello\"')")
        assert result == "hello"


class TestIntrinsicJsonToString:
    def test_serialize_object(self):
        result = evaluate_intrinsic("States.JsonToString($.data)", {"data": {"a": 1}})
        assert result == '{"a":1}'

    def test_serialize_array(self):
        result = evaluate_intrinsic("States.JsonToString($.arr)", {"arr": [1, 2]})
        assert result == "[1,2]"


class TestIntrinsicArray:
    def test_empty_array(self):
        assert evaluate_intrinsic("States.Array()") == []

    def test_array_with_values(self):
        result = evaluate_intrinsic("States.Array(1, 2, 3)")
        assert result == [1, 2, 3]

    def test_array_with_strings(self):
        result = evaluate_intrinsic("States.Array('a', 'b')")
        assert result == ["a", "b"]

    def test_array_with_path(self):
        result = evaluate_intrinsic("States.Array($.x, $.y)", {"x": 10, "y": 20})
        assert result == [10, 20]


class TestIntrinsicArrayPartition:
    def test_even_partition(self):
        result = evaluate_intrinsic("States.ArrayPartition($.arr, 2)", {"arr": [1, 2, 3, 4]})
        assert result == [[1, 2], [3, 4]]

    def test_uneven_partition(self):
        result = evaluate_intrinsic("States.ArrayPartition($.arr, 2)", {"arr": [1, 2, 3]})
        assert result == [[1, 2], [3]]

    def test_partition_error_not_array(self):
        with pytest.raises(IntrinsicError):
            evaluate_intrinsic("States.ArrayPartition('notarray', 2)")

    def test_partition_error_zero_size(self):
        with pytest.raises(IntrinsicError):
            evaluate_intrinsic("States.ArrayPartition($.arr, 0)", {"arr": [1]})


class TestIntrinsicArrayContains:
    def test_contains_true(self):
        result = evaluate_intrinsic("States.ArrayContains($.arr, 3)", {"arr": [1, 2, 3]})
        assert result is True

    def test_contains_false(self):
        result = evaluate_intrinsic("States.ArrayContains($.arr, 5)", {"arr": [1, 2, 3]})
        assert result is False


class TestIntrinsicArrayRange:
    def test_ascending_range(self):
        result = evaluate_intrinsic("States.ArrayRange(1, 5, 2)")
        assert result == [1, 3, 5]

    def test_single_step(self):
        result = evaluate_intrinsic("States.ArrayRange(0, 3, 1)")
        assert result == [0, 1, 2, 3]

    def test_zero_step_error(self):
        with pytest.raises(IntrinsicError):
            evaluate_intrinsic("States.ArrayRange(0, 5, 0)")


class TestIntrinsicArrayGetItem:
    def test_get_first(self):
        result = evaluate_intrinsic("States.ArrayGetItem($.arr, 0)", {"arr": ["a", "b", "c"]})
        assert result == "a"

    def test_get_last(self):
        result = evaluate_intrinsic("States.ArrayGetItem($.arr, 2)", {"arr": ["a", "b", "c"]})
        assert result == "c"

    def test_out_of_bounds(self):
        with pytest.raises(IntrinsicError):
            evaluate_intrinsic("States.ArrayGetItem($.arr, 10)", {"arr": [1]})


class TestIntrinsicArrayLength:
    def test_length(self):
        result = evaluate_intrinsic("States.ArrayLength($.arr)", {"arr": [1, 2, 3]})
        assert result == 3

    def test_empty_length(self):
        result = evaluate_intrinsic("States.ArrayLength($.arr)", {"arr": []})
        assert result == 0


class TestIntrinsicArrayUnique:
    def test_unique(self):
        result = evaluate_intrinsic("States.ArrayUnique($.arr)", {"arr": [1, 2, 2, 3, 1]})
        assert result == [1, 2, 3]

    def test_already_unique(self):
        result = evaluate_intrinsic("States.ArrayUnique($.arr)", {"arr": [1, 2, 3]})
        assert result == [1, 2, 3]


class TestIntrinsicBase64:
    def test_encode(self):
        result = evaluate_intrinsic("States.Base64Encode('hello')")
        assert result == "aGVsbG8="

    def test_decode(self):
        result = evaluate_intrinsic("States.Base64Decode('aGVsbG8=')")
        assert result == "hello"

    def test_roundtrip(self):
        encoded = evaluate_intrinsic("States.Base64Encode('test data')")
        decoded = evaluate_intrinsic(f"States.Base64Decode('{encoded}')")
        assert decoded == "test data"


class TestIntrinsicHash:
    def test_md5(self):
        result = evaluate_intrinsic("States.Hash('hello', 'MD5')")
        assert result == hashlib.md5(b"hello").hexdigest()

    def test_sha256(self):
        result = evaluate_intrinsic("States.Hash('hello', 'SHA-256')")
        assert result == hashlib.sha256(b"hello").hexdigest()

    def test_sha1(self):
        result = evaluate_intrinsic("States.Hash('hello', 'SHA-1')")
        assert result == hashlib.sha1(b"hello").hexdigest()

    def test_sha384(self):
        result = evaluate_intrinsic("States.Hash('hello', 'SHA-384')")
        assert result == hashlib.sha384(b"hello").hexdigest()

    def test_sha512(self):
        result = evaluate_intrinsic("States.Hash('hello', 'SHA-512')")
        assert result == hashlib.sha512(b"hello").hexdigest()

    def test_unsupported_algorithm(self):
        with pytest.raises(IntrinsicError):
            evaluate_intrinsic("States.Hash('hello', 'BOGUS')")


class TestIntrinsicJsonMerge:
    def test_shallow_merge(self):
        result = evaluate_intrinsic(
            "States.JsonMerge($.a, $.b, false)",
            {"a": {"x": 1}, "b": {"y": 2}},
        )
        assert result == {"x": 1, "y": 2}

    def test_shallow_overwrite(self):
        result = evaluate_intrinsic(
            "States.JsonMerge($.a, $.b, false)",
            {"a": {"x": 1}, "b": {"x": 2}},
        )
        assert result == {"x": 2}

    def test_deep_merge(self):
        result = evaluate_intrinsic(
            "States.JsonMerge($.a, $.b, true)",
            {
                "a": {"nested": {"x": 1, "y": 2}},
                "b": {"nested": {"y": 3, "z": 4}},
            },
        )
        assert result == {"nested": {"x": 1, "y": 3, "z": 4}}


class TestIntrinsicMath:
    def test_math_random_in_range(self):
        for _ in range(20):
            result = evaluate_intrinsic("States.MathRandom(1, 10)")
            assert 1 <= result <= 10

    def test_math_add_integers(self):
        result = evaluate_intrinsic("States.MathAdd(3, 4)")
        assert result == 7

    def test_math_add_negative(self):
        result = evaluate_intrinsic("States.MathAdd(10, -3)")
        assert result == 7

    def test_math_add_with_path(self):
        result = evaluate_intrinsic("States.MathAdd($.a, $.b)", {"a": 5, "b": 10})
        assert result == 15


class TestIntrinsicStringSplit:
    def test_split_comma(self):
        result = evaluate_intrinsic("States.StringSplit('a,b,c', ',')")
        assert result == ["a", "b", "c"]

    def test_split_space(self):
        result = evaluate_intrinsic("States.StringSplit('hello world', ' ')")
        assert result == ["hello", "world"]


class TestIntrinsicUUID:
    def test_uuid_format(self):
        result = evaluate_intrinsic("States.UUID()")
        # Should be a valid UUID v4
        parsed = uuid.UUID(result)
        assert str(parsed) == result

    def test_uuid_unique(self):
        r1 = evaluate_intrinsic("States.UUID()")
        r2 = evaluate_intrinsic("States.UUID()")
        assert r1 != r2


class TestIntrinsicUnknown:
    def test_unknown_function(self):
        with pytest.raises(IntrinsicError):
            evaluate_intrinsic("States.Bogus()")


# ===========================================================================
# JSONata Evaluation
# ===========================================================================


class TestJSONataBasic:
    def test_field_access(self):
        assert evaluate_jsonata("name", {"name": "Alice"}) == "Alice"

    def test_nested_field(self):
        assert evaluate_jsonata("user.name", {"user": {"name": "Bob"}}) == "Bob"

    def test_string_literal(self):
        assert evaluate_jsonata('"hello"') == "hello"

    def test_number_literal(self):
        assert evaluate_jsonata("42") == 42

    def test_float_literal(self):
        assert evaluate_jsonata("3.14") == 3.14

    def test_boolean_true(self):
        assert evaluate_jsonata("true") is True

    def test_boolean_false(self):
        assert evaluate_jsonata("false") is False

    def test_null(self):
        assert evaluate_jsonata("null") is None

    def test_empty_expression(self):
        assert evaluate_jsonata("") is None


class TestJSONataConcatenation:
    def test_string_concat(self):
        result = evaluate_jsonata('name & " " & "Smith"', {"name": "John"})
        assert result == "John Smith"


class TestJSONataConditional:
    def test_ternary_true(self):
        result = evaluate_jsonata("age >= 18 ? 'adult' : 'minor'", {"age": 21})
        assert result == "adult"

    def test_ternary_false(self):
        result = evaluate_jsonata("age >= 18 ? 'adult' : 'minor'", {"age": 10})
        assert result == "minor"


class TestJSONataArithmetic:
    def test_addition(self):
        assert evaluate_jsonata("a + b", {"a": 3, "b": 4}) == 7

    def test_subtraction(self):
        assert evaluate_jsonata("a - b", {"a": 10, "b": 3}) == 7

    def test_multiplication(self):
        assert evaluate_jsonata("a * b", {"a": 3, "b": 4}) == 12

    def test_division(self):
        assert evaluate_jsonata("a / b", {"a": 10, "b": 2}) == 5.0


class TestJSONataFunctions:
    def test_length(self):
        assert evaluate_jsonata("$length('hello')") == 5

    def test_uppercase(self):
        assert evaluate_jsonata("$uppercase('hello')") == "HELLO"

    def test_lowercase(self):
        assert evaluate_jsonata("$lowercase('HELLO')") == "hello"

    def test_contains(self):
        assert evaluate_jsonata("$contains('hello world', 'world')") is True
        assert evaluate_jsonata("$contains('hello world', 'xyz')") is False

    def test_type(self):
        assert evaluate_jsonata("$type(42)") == "number"
        assert evaluate_jsonata("$type('hello')") == "string"
        assert evaluate_jsonata("$type(true)") == "boolean"

    def test_exists(self):
        assert evaluate_jsonata("$exists(name)", {"name": "Alice"}) is True

    def test_not(self):
        assert evaluate_jsonata("$not(false)") is True
        assert evaluate_jsonata("$not(true)") is False


class TestJSONataComparison:
    def test_equals(self):
        assert evaluate_jsonata("x = 5", {"x": 5}) is True
        assert evaluate_jsonata("x = 5", {"x": 3}) is False

    def test_not_equals(self):
        assert evaluate_jsonata("x != 5", {"x": 3}) is True

    def test_greater_than(self):
        assert evaluate_jsonata("x > 5", {"x": 10}) is True

    def test_less_than(self):
        assert evaluate_jsonata("x < 5", {"x": 3}) is True


# ===========================================================================
# Map State
# ===========================================================================


class TestMapStateAdvanced:
    def test_map_with_max_concurrency(self):
        executor = _make_executor(
            {
                "Start": {
                    "Type": "Map",
                    "ItemsPath": "$.items",
                    "MaxConcurrency": 2,
                    "Iterator": {
                        "StartAt": "Process",
                        "States": {"Process": {"Type": "Pass", "End": True}},
                    },
                    "End": True,
                },
            }
        )
        result = executor.execute({"items": [1, 2, 3, 4]})
        assert result == [1, 2, 3, 4]

    def test_map_with_result_path(self):
        executor = _make_executor(
            {
                "Start": {
                    "Type": "Map",
                    "ItemsPath": "$.items",
                    "Iterator": {
                        "StartAt": "Process",
                        "States": {
                            "Process": {
                                "Type": "Pass",
                                "Result": "done",
                                "End": True,
                            }
                        },
                    },
                    "ResultPath": "$.results",
                    "End": True,
                },
            }
        )
        result = executor.execute({"items": [1, 2]})
        assert result == {"items": [1, 2], "results": ["done", "done"]}

    def test_map_with_item_processor(self):
        executor = _make_executor(
            {
                "Start": {
                    "Type": "Map",
                    "ItemsPath": "$.items",
                    "ItemProcessor": {
                        "StartAt": "Double",
                        "States": {
                            "Double": {
                                "Type": "Pass",
                                "Result": "processed",
                                "End": True,
                            }
                        },
                    },
                    "End": True,
                },
            }
        )
        result = executor.execute({"items": ["a", "b"]})
        assert result == ["processed", "processed"]

    def test_map_non_list_wrapped(self):
        executor = _make_executor(
            {
                "Start": {
                    "Type": "Map",
                    "ItemsPath": "$.item",
                    "Iterator": {
                        "StartAt": "P",
                        "States": {"P": {"Type": "Pass", "End": True}},
                    },
                    "End": True,
                },
            }
        )
        result = executor.execute({"item": "single"})
        assert result == ["single"]

    def test_map_with_result_selector(self):
        executor = _make_executor(
            {
                "Start": {
                    "Type": "Map",
                    "ItemsPath": "$.items",
                    "Iterator": {
                        "StartAt": "P",
                        "States": {
                            "P": {
                                "Type": "Pass",
                                "Result": {"data": "x", "extra": "y"},
                                "End": True,
                            }
                        },
                    },
                    "ResultSelector": {"mapped.$": "$"},
                    "End": True,
                },
            }
        )
        result = executor.execute({"items": [1]})
        assert "mapped" in result
        assert isinstance(result["mapped"], list)


# ===========================================================================
# Execution History
# ===========================================================================


class TestExecutionHistory:
    def test_execution_started(self):
        history = ExecutionHistory("arn:exec:1")
        eid = history.execution_started('{"key": "val"}', "arn:role:test")
        assert eid == 1
        assert len(history.events) == 1
        event = history.events[0]
        assert event["type"] == "ExecutionStarted"
        assert event["id"] == 1
        details = event["executionStartedEventDetails"]
        assert details["input"] == '{"key": "val"}'

    def test_execution_succeeded(self):
        history = ExecutionHistory("arn:exec:1")
        history.execution_started("{}")
        eid = history.execution_succeeded('{"result": "ok"}', prev_id=1)
        assert eid == 2
        assert history.events[1]["type"] == "ExecutionSucceeded"

    def test_execution_failed(self):
        history = ExecutionHistory("arn:exec:1")
        history.execution_started("{}")
        history.execution_failed("Error", "cause", prev_id=1)
        assert history.events[1]["type"] == "ExecutionFailed"
        details = history.events[1]["executionFailedEventDetails"]
        assert details["error"] == "Error"
        assert details["cause"] == "cause"

    def test_state_entered_and_exited(self):
        history = ExecutionHistory("arn:exec:1")
        eid1 = history.state_entered("MyPass", "Pass", '{"x": 1}')
        history.state_exited("MyPass", "Pass", '{"x": 1}', prev_id=eid1)
        assert history.events[0]["type"] == "PassStateEntered"
        assert history.events[1]["type"] == "PassStateExited"

    def test_task_lifecycle(self):
        history = ExecutionHistory("arn:exec:1")
        eid1 = history.task_scheduled("arn:aws:lambda:us-east-1:123:function:f1", "{}")
        eid2 = history.task_started("arn:aws:lambda:us-east-1:123:function:f1", prev_id=eid1)
        history.task_succeeded(
            "arn:aws:lambda:us-east-1:123:function:f1", '{"ok": true}', prev_id=eid2
        )
        assert history.events[0]["type"] == "TaskScheduled"
        assert history.events[1]["type"] == "TaskStarted"
        assert history.events[2]["type"] == "TaskSucceeded"

    def test_task_failed(self):
        history = ExecutionHistory("arn:exec:1")
        history.task_failed("arn:aws:lambda:us-east-1:123:function:f1", "Error", "bad")
        assert history.events[0]["type"] == "TaskFailed"

    def test_get_events_reverse(self):
        history = ExecutionHistory("arn:exec:1")
        history.execution_started("{}")
        history.execution_succeeded("{}")
        events = history.get_events(reverse_order=True)
        assert events[0]["type"] == "ExecutionSucceeded"
        assert events[1]["type"] == "ExecutionStarted"

    def test_execution_aborted(self):
        history = ExecutionHistory("arn:exec:1")
        history.execution_started("{}")
        history.execution_aborted(prev_id=1)
        assert history.events[1]["type"] == "ExecutionAborted"

    def test_execution_timed_out(self):
        history = ExecutionHistory("arn:exec:1")
        history.execution_started("{}")
        history.execution_timed_out(prev_id=1)
        assert history.events[1]["type"] == "ExecutionTimedOut"

    def test_choice_state_events(self):
        history = ExecutionHistory("arn:exec:1")
        history.state_entered("MyChoice", "Choice", "{}")
        history.state_exited("MyChoice", "Choice", "{}")
        assert history.events[0]["type"] == "ChoiceStateEntered"
        assert history.events[1]["type"] == "ChoiceStateExited"

    def test_parallel_state_events(self):
        history = ExecutionHistory("arn:exec:1")
        history.state_entered("MyParallel", "Parallel", "{}")
        history.state_exited("MyParallel", "Parallel", "[]")
        assert history.events[0]["type"] == "ParallelStateEntered"
        assert history.events[1]["type"] == "ParallelStateExited"

    def test_map_state_events(self):
        history = ExecutionHistory("arn:exec:1")
        history.state_entered("MyMap", "Map", "{}")
        history.state_exited("MyMap", "Map", "[]")
        assert history.events[0]["type"] == "MapStateEntered"
        assert history.events[1]["type"] == "MapStateExited"


class TestHistoryHelpers:
    def test_detail_key(self):
        assert _detail_key("ExecutionStarted") == "executionStartedEventDetails"
        assert _detail_key("TaskSucceeded") == "taskSucceededEventDetails"

    def test_state_entered_type(self):
        assert _state_entered_type("Task") == "TaskStateEntered"
        assert _state_entered_type("Choice") == "ChoiceStateEntered"
        assert _state_entered_type("Map") == "MapStateEntered"

    def test_state_exited_type(self):
        assert _state_exited_type("Task") == "TaskStateExited"
        assert _state_exited_type("Parallel") == "ParallelStateExited"

    def test_resource_type(self):
        assert _resource_type("arn:aws:lambda:us-east-1:123:function:f") == "lambda"
        assert _resource_type("arn:aws:states:::sqs:sendMessage") == "sqs"
        assert _resource_type("arn:aws:states:::sns:publish") == "sns"
        assert _resource_type("arn:aws:states:::dynamodb:putItem") == "dynamodb"


class TestExecutionHistoryInExecution:
    """Test that execution history is properly recorded during ASL execution."""

    def test_pass_state_records_history(self):
        executor = _make_executor(
            {"Start": {"Type": "Pass", "End": True}},
            execution_arn="arn:exec:test1",
        )
        executor.execute({"x": 1})
        history = executor.history
        assert history is not None
        events = history.get_events()
        # Should have: ExecutionStarted, PassStateEntered, PassStateExited, ExecutionSucceeded
        types = [e["type"] for e in events]
        assert "ExecutionStarted" in types
        assert "PassStateEntered" in types
        assert "PassStateExited" in types
        assert "ExecutionSucceeded" in types

    def test_failed_execution_records_history(self):
        executor = _make_executor(
            {"Start": {"Type": "Fail", "Error": "MyError", "Cause": "reason"}},
            execution_arn="arn:exec:fail1",
        )
        with pytest.raises(ASLExecutionError):
            executor.execute({})
        events = executor.history.get_events()
        types = [e["type"] for e in events]
        assert "ExecutionStarted" in types
        assert "FailStateEntered" in types
        assert "ExecutionFailed" in types

    def test_multi_state_records_all(self):
        executor = _make_executor(
            {
                "First": {"Type": "Pass", "Next": "Second"},
                "Second": {"Type": "Pass", "End": True},
            },
            start_at="First",
            execution_arn="arn:exec:multi1",
        )
        executor.execute({})
        events = executor.history.get_events()
        types = [e["type"] for e in events]
        # Two states entered and exited
        assert types.count("PassStateEntered") == 2
        assert types.count("PassStateExited") == 2


# ===========================================================================
# Express Workflows
# ===========================================================================


class TestExpressWorkflows:
    def setup_method(self):
        _clear_state()

    def teardown_method(self):
        _clear_state()

    def test_create_express_state_machine(self):
        result = _create_state_machine(
            {
                "name": "express1",
                "definition": _SIMPLE_DEFINITION,
                "roleArn": "r",
                "type": "EXPRESS",
            },
            "us-east-1",
            "123",
        )
        assert "stateMachineArn" in result

    def test_start_sync_execution(self):
        _create_state_machine(
            {
                "name": "express1",
                "definition": _SIMPLE_DEFINITION,
                "roleArn": "r",
                "type": "EXPRESS",
            },
            "us-east-1",
            "123",
        )
        arn = "arn:aws:states:us-east-1:123:stateMachine:express1"
        result = _start_sync_execution(
            {"stateMachineArn": arn, "input": "{}"},
            "us-east-1",
            "123",
        )
        assert result["status"] == "SUCCEEDED"
        assert "output" in result
        assert "startDate" in result
        assert "stopDate" in result

    def test_start_sync_execution_fails_for_standard(self):
        _create_state_machine(
            {"name": "std1", "definition": _SIMPLE_DEFINITION, "roleArn": "r", "type": "STANDARD"},
            "us-east-1",
            "123",
        )
        arn = "arn:aws:states:us-east-1:123:stateMachine:std1"
        with pytest.raises(SfnError) as exc_info:
            _start_sync_execution(
                {"stateMachineArn": arn, "input": "{}"},
                "us-east-1",
                "123",
            )
        assert exc_info.value.code == "InvalidArn"

    def test_express_not_in_list_executions(self):
        _create_state_machine(
            {
                "name": "express1",
                "definition": _SIMPLE_DEFINITION,
                "roleArn": "r",
                "type": "EXPRESS",
            },
            "us-east-1",
            "123",
        )
        arn = "arn:aws:states:us-east-1:123:stateMachine:express1"
        _start_sync_execution(
            {"stateMachineArn": arn, "input": "{}"},
            "us-east-1",
            "123",
        )
        result = _list_executions({"stateMachineArn": arn}, "us-east-1", "123")
        assert len(result["executions"]) == 0

    def test_start_sync_execution_with_failure(self):
        fail_def = json.dumps(
            {
                "StartAt": "Fail",
                "States": {"Fail": {"Type": "Fail", "Error": "TestError", "Cause": "testing"}},
            }
        )
        _create_state_machine(
            {"name": "express_fail", "definition": fail_def, "roleArn": "r", "type": "EXPRESS"},
            "us-east-1",
            "123",
        )
        arn = "arn:aws:states:us-east-1:123:stateMachine:express_fail"
        result = _start_sync_execution(
            {"stateMachineArn": arn, "input": "{}"},
            "us-east-1",
            "123",
        )
        assert result["status"] == "FAILED"
        assert result["error"] == "TestError"
        assert result["cause"] == "testing"


# ===========================================================================
# Callback Pattern
# ===========================================================================


class TestCallbackPattern:
    def test_send_task_success(self):
        """Test the raw send_task_success function."""
        token = str(uuid.uuid4())
        event = threading.Event()
        with _token_lock:
            _task_tokens[token] = {
                "event": event,
                "result": None,
                "status": None,
                "error": None,
                "cause": None,
                "last_heartbeat": time.time(),
            }

        result = send_task_success(token, {"output": "done"})
        assert result is True
        assert _task_tokens[token]["status"] == "SUCCESS"
        assert event.is_set()

        # Clean up
        with _token_lock:
            _task_tokens.pop(token, None)

    def test_send_task_failure(self):
        token = str(uuid.uuid4())
        event = threading.Event()
        with _token_lock:
            _task_tokens[token] = {
                "event": event,
                "result": None,
                "status": None,
                "error": None,
                "cause": None,
                "last_heartbeat": time.time(),
            }

        result = send_task_failure(token, "MyError", "something broke")
        assert result is True
        assert _task_tokens[token]["status"] == "FAILED"
        assert _task_tokens[token]["error"] == "MyError"

        with _token_lock:
            _task_tokens.pop(token, None)

    def test_send_task_heartbeat(self):
        token = str(uuid.uuid4())
        event = threading.Event()
        old_time = time.time() - 100
        with _token_lock:
            _task_tokens[token] = {
                "event": event,
                "result": None,
                "status": None,
                "error": None,
                "cause": None,
                "last_heartbeat": old_time,
            }

        result = send_task_heartbeat(token)
        assert result is True
        assert _task_tokens[token]["last_heartbeat"] > old_time

        with _token_lock:
            _task_tokens.pop(token, None)

    def test_send_task_success_unknown_token(self):
        result = send_task_success("nonexistent-token", {})
        assert result is False

    def test_send_task_failure_unknown_token(self):
        result = send_task_failure("nonexistent-token", "Error", "cause")
        assert result is False

    def test_send_task_heartbeat_unknown_token(self):
        result = send_task_heartbeat("nonexistent-token")
        assert result is False


# ===========================================================================
# Provider Callback Operations
# ===========================================================================


class TestProviderCallbackOps:
    def setup_method(self):
        _clear_state()

    def teardown_method(self):
        _clear_state()
        with _token_lock:
            _task_tokens.clear()

    def test_send_task_success_op_unknown(self):
        from robotocore.services.stepfunctions.provider import _send_task_success_op

        with pytest.raises(SfnError) as exc_info:
            _send_task_success_op(
                {"taskToken": "bogus", "output": "{}"},
                "us-east-1",
                "123",
            )
        assert exc_info.value.code == "TaskDoesNotExist"

    def test_send_task_failure_op_unknown(self):
        from robotocore.services.stepfunctions.provider import _send_task_failure_op

        with pytest.raises(SfnError) as exc_info:
            _send_task_failure_op(
                {"taskToken": "bogus", "error": "E", "cause": "C"},
                "us-east-1",
                "123",
            )
        assert exc_info.value.code == "TaskDoesNotExist"

    def test_send_task_heartbeat_op_unknown(self):
        from robotocore.services.stepfunctions.provider import _send_task_heartbeat_op

        with pytest.raises(SfnError) as exc_info:
            _send_task_heartbeat_op(
                {"taskToken": "bogus"},
                "us-east-1",
                "123",
            )
        assert exc_info.value.code == "TaskDoesNotExist"


# ===========================================================================
# Service Integrations — DynamoDB dispatch
# ===========================================================================


class TestDynamoDBIntegration:
    """Test DynamoDB integration routing in ASL.

    Will fail at backend level but verifies routing.
    """

    def test_dynamodb_putitem_resource_parsing(self):
        executor = _make_executor(
            {
                "Start": {
                    "Type": "Task",
                    "Resource": "arn:aws:states:::dynamodb:putItem",
                    "Parameters": {
                        "TableName": "MyTable",
                        "Item": {"id": {"S": "123"}},
                    },
                    "Catch": [{"ErrorEquals": ["States.ALL"], "Next": "Fallback"}],
                    "Next": "Done",
                },
                "Fallback": {"Type": "Pass", "Result": {"caught": True}, "End": True},
                "Done": {"Type": "Pass", "End": True},
            }
        )
        # DynamoDB backend won't be available in unit test, so it should catch the error
        result = executor.execute({})
        assert result.get("caught") is True

    def test_dynamodb_getitem_resource_parsing(self):
        executor = _make_executor(
            {
                "Start": {
                    "Type": "Task",
                    "Resource": "arn:aws:states:::dynamodb:getItem",
                    "Parameters": {
                        "TableName": "MyTable",
                        "Key": {"id": {"S": "123"}},
                    },
                    "Catch": [{"ErrorEquals": ["States.ALL"], "Next": "Fallback"}],
                    "Next": "Done",
                },
                "Fallback": {"Type": "Pass", "Result": {"caught": True}, "End": True},
                "Done": {"Type": "Pass", "End": True},
            }
        )
        result = executor.execute({})
        assert result.get("caught") is True

    def test_dynamodb_deleteitem_resource_parsing(self):
        executor = _make_executor(
            {
                "Start": {
                    "Type": "Task",
                    "Resource": "arn:aws:states:::dynamodb:deleteItem",
                    "Parameters": {
                        "TableName": "MyTable",
                        "Key": {"id": {"S": "123"}},
                    },
                    "Catch": [{"ErrorEquals": ["States.ALL"], "Next": "Fallback"}],
                    "Next": "Done",
                },
                "Fallback": {"Type": "Pass", "Result": {"caught": True}, "End": True},
                "Done": {"Type": "Pass", "End": True},
            }
        )
        result = executor.execute({})
        assert result.get("caught") is True

    def test_dynamodb_query_resource_parsing(self):
        executor = _make_executor(
            {
                "Start": {
                    "Type": "Task",
                    "Resource": "arn:aws:states:::dynamodb:query",
                    "Parameters": {
                        "TableName": "MyTable",
                        "KeyConditionExpression": "id = :id",
                    },
                    "Catch": [{"ErrorEquals": ["States.ALL"], "Next": "Fallback"}],
                    "Next": "Done",
                },
                "Fallback": {"Type": "Pass", "Result": {"caught": True}, "End": True},
                "Done": {"Type": "Pass", "End": True},
            }
        )
        result = executor.execute({})
        assert result.get("caught") is True

    def test_dynamodb_updateitem_resource_parsing(self):
        executor = _make_executor(
            {
                "Start": {
                    "Type": "Task",
                    "Resource": "arn:aws:states:::dynamodb:updateItem",
                    "Parameters": {
                        "TableName": "MyTable",
                        "Key": {"id": {"S": "123"}},
                        "UpdateExpression": "SET val = :v",
                    },
                    "Catch": [{"ErrorEquals": ["States.ALL"], "Next": "Fallback"}],
                    "Next": "Done",
                },
                "Fallback": {"Type": "Pass", "Result": {"caught": True}, "End": True},
                "Done": {"Type": "Pass", "End": True},
            }
        )
        result = executor.execute({})
        assert result.get("caught") is True


class TestSQSIntegration:
    """Test SQS integration resource routing."""

    def test_sqs_sendmessage_routing(self):
        executor = _make_executor(
            {
                "Start": {
                    "Type": "Task",
                    "Resource": "arn:aws:states:::sqs:sendMessage",
                    "Parameters": {
                        "QueueUrl": "http://localhost/queue/test",
                        "MessageBody": "hello",
                    },
                    "Catch": [{"ErrorEquals": ["States.ALL"], "Next": "Fallback"}],
                    "Next": "Done",
                },
                "Fallback": {"Type": "Pass", "Result": {"caught": True}, "End": True},
                "Done": {"Type": "Pass", "End": True},
            }
        )
        result = executor.execute({})
        # SQS store not available in unit test, should catch error
        assert result.get("caught") is True


class TestSNSIntegration:
    """Test SNS integration resource routing."""

    def test_sns_publish_routing(self):
        executor = _make_executor(
            {
                "Start": {
                    "Type": "Task",
                    "Resource": "arn:aws:states:::sns:publish",
                    "Parameters": {
                        "TopicArn": "arn:aws:sns:us-east-1:123:TestTopic",
                        "Message": "hello",
                    },
                    "Catch": [{"ErrorEquals": ["States.ALL"], "Next": "Fallback"}],
                    "Next": "Done",
                },
                "Fallback": {"Type": "Pass", "Result": {"caught": True}, "End": True},
                "Done": {"Type": "Pass", "End": True},
            }
        )
        result = executor.execute({})
        assert result.get("caught") is True


class TestLambdaInvokeIntegration:
    """Test lambda:invoke SDK integration pattern."""

    def test_lambda_invoke_sdk_pattern(self):
        executor = _make_executor(
            {
                "Start": {
                    "Type": "Task",
                    "Resource": "arn:aws:states:::lambda:invoke",
                    "Parameters": {
                        "FunctionName": "arn:aws:lambda:us-east-1:123:function:myFn",
                        "Payload": {"key": "val"},
                    },
                    "Catch": [{"ErrorEquals": ["States.ALL"], "Next": "Fallback"}],
                    "Next": "Done",
                },
                "Fallback": {"Type": "Pass", "Result": {"caught": True}, "End": True},
                "Done": {"Type": "Pass", "End": True},
            }
        )
        result = executor.execute({})
        assert result.get("caught") is True


# ===========================================================================
# Data Flow Operators
# ===========================================================================


class TestDataFlowOperators:
    """Comprehensive tests for InputPath, Parameters, ResultSelector, ResultPath, OutputPath."""

    def test_input_path_filters_before_task(self):
        executor = _make_executor(
            {
                "Start": {
                    "Type": "Pass",
                    "InputPath": "$.data",
                    "End": True,
                },
            }
        )
        result = executor.execute({"data": {"x": 1}, "metadata": "ignore"})
        assert result == {"x": 1}

    def test_input_path_null_gives_empty(self):
        executor = _make_executor(
            {
                "Start": {
                    "Type": "Pass",
                    "InputPath": None,
                    "End": True,
                },
            }
        )
        result = executor.execute({"anything": True})
        assert result == {}

    def test_parameters_with_static_and_dynamic(self):
        executor = _make_executor(
            {
                "Start": {
                    "Type": "Pass",
                    "Parameters": {
                        "static": "value",
                        "dynamic.$": "$.name",
                    },
                    "End": True,
                },
            }
        )
        result = executor.execute({"name": "Alice"})
        assert result == {"static": "value", "dynamic": "Alice"}

    def test_parameters_with_intrinsic(self):
        executor = _make_executor(
            {
                "Start": {
                    "Type": "Pass",
                    "Parameters": {
                        "id.$": "States.UUID()",
                    },
                    "End": True,
                },
            }
        )
        result = executor.execute({})
        assert "id" in result
        # Should be a valid UUID
        uuid.UUID(result["id"])

    def test_result_selector_filters_result(self):
        executor = _make_executor(
            {
                "Start": {
                    "Type": "Pass",
                    "Result": {"full": "data", "extra": "stuff"},
                    "ResultSelector": {"needed.$": "$.full"},
                    "End": True,
                },
            }
        )
        result = executor.execute({})
        assert result == {"needed": "data"}

    def test_result_path_merges(self):
        executor = _make_executor(
            {
                "Start": {
                    "Type": "Pass",
                    "Result": "computed",
                    "ResultPath": "$.output",
                    "End": True,
                },
            }
        )
        result = executor.execute({"input": "original"})
        assert result == {"input": "original", "output": "computed"}

    def test_result_path_null_discards(self):
        executor = _make_executor(
            {
                "Start": {
                    "Type": "Pass",
                    "Result": "discarded",
                    "ResultPath": None,
                    "End": True,
                },
            }
        )
        result = executor.execute({"keep": True})
        assert result == {"keep": True}

    def test_output_path_filters_final(self):
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

    def test_all_data_flow_operators_together(self):
        """Test InputPath -> Parameters -> Result -> ResultSelector -> ResultPath -> OutputPath."""
        executor = _make_executor(
            {
                "Start": {
                    "Type": "Pass",
                    "InputPath": "$.request",
                    "Parameters": {"extracted.$": "$.value"},
                    "Result": {"computed": 42, "metadata": "extra"},
                    "ResultSelector": {"answer.$": "$.computed"},
                    "ResultPath": "$.result",
                    "OutputPath": "$.result",
                    "End": True,
                },
            }
        )
        result = executor.execute({"request": {"value": "test"}, "other": "data"})
        assert result == {"answer": 42}

    def test_nested_parameters(self):
        executor = _make_executor(
            {
                "Start": {
                    "Type": "Pass",
                    "Parameters": {
                        "outer": {
                            "inner.$": "$.val",
                        }
                    },
                    "End": True,
                },
            }
        )
        result = executor.execute({"val": 99})
        assert result == {"outer": {"inner": 99}}

    def test_parameters_list_with_dicts(self):
        executor = _make_executor(
            {
                "Start": {
                    "Type": "Pass",
                    "Parameters": {
                        "items": [{"static": True}],
                    },
                    "End": True,
                },
            }
        )
        result = executor.execute({})
        assert result == {"items": [{"static": True}]}


# ===========================================================================
# GetExecutionHistory via Provider
# ===========================================================================


class TestGetExecutionHistoryProvider:
    def setup_method(self):
        _clear_state()

    def teardown_method(self):
        _clear_state()

    def test_get_execution_history_populated(self):
        _create_state_machine(
            {"name": "sm1", "definition": _SIMPLE_DEFINITION, "roleArn": "r"},
            "us-east-1",
            "123",
        )
        sm_arn = "arn:aws:states:us-east-1:123:stateMachine:sm1"
        start = _start_execution(
            {"stateMachineArn": sm_arn, "name": "exec1", "input": "{}"},
            "us-east-1",
            "123",
        )
        result = _get_execution_history(
            {"executionArn": start["executionArn"]},
            "us-east-1",
            "123",
        )
        assert len(result["events"]) > 0
        types = [e["type"] for e in result["events"]]
        assert "ExecutionStarted" in types
        assert "ExecutionSucceeded" in types

    def test_get_execution_history_reverse(self):
        _create_state_machine(
            {"name": "sm1", "definition": _SIMPLE_DEFINITION, "roleArn": "r"},
            "us-east-1",
            "123",
        )
        sm_arn = "arn:aws:states:us-east-1:123:stateMachine:sm1"
        start = _start_execution(
            {"stateMachineArn": sm_arn, "name": "exec1", "input": "{}"},
            "us-east-1",
            "123",
        )
        result = _get_execution_history(
            {"executionArn": start["executionArn"], "reverseOrder": True},
            "us-east-1",
            "123",
        )
        events = result["events"]
        assert events[0]["type"] == "ExecutionSucceeded"
        assert events[-1]["type"] == "ExecutionStarted"

    def test_get_execution_history_empty(self):
        result = _get_execution_history(
            {"executionArn": "arn:nonexistent"},
            "us-east-1",
            "123",
        )
        assert result == {"events": []}

    def test_stop_execution_records_abort(self):
        _create_state_machine(
            {"name": "sm1", "definition": _SIMPLE_DEFINITION, "roleArn": "r"},
            "us-east-1",
            "123",
        )
        sm_arn = "arn:aws:states:us-east-1:123:stateMachine:sm1"
        start = _start_execution(
            {"stateMachineArn": sm_arn, "name": "exec1", "input": "{}"},
            "us-east-1",
            "123",
        )
        _stop_execution({"executionArn": start["executionArn"]}, "us-east-1", "123")
        result = _get_execution_history(
            {"executionArn": start["executionArn"]},
            "us-east-1",
            "123",
        )
        types = [e["type"] for e in result["events"]]
        assert "ExecutionAborted" in types


# ===========================================================================
# DynamoDB Type Helpers
# ===========================================================================


class TestDynamoDBTypeHelpers:
    def test_is_ddb_typed(self):
        from robotocore.services.stepfunctions.asl import _is_ddb_typed

        assert _is_ddb_typed({"key": {"S": "val"}}) is True
        assert _is_ddb_typed({"key": "val"}) is False
        assert _is_ddb_typed({}) is False

    def test_to_dynamodb_item(self):
        from robotocore.services.stepfunctions.asl import _to_dynamodb_item

        result = _to_dynamodb_item({"name": "Alice", "age": 30, "active": True})
        assert result["name"] == {"S": "Alice"}
        assert result["age"] == {"N": "30"}
        assert result["active"] == {"BOOL": True}

    def test_to_dynamodb_item_preserves_typed(self):
        from robotocore.services.stepfunctions.asl import _to_dynamodb_item

        result = _to_dynamodb_item({"id": {"S": "123"}, "plain": "val"})
        assert result["id"] == {"S": "123"}
        assert result["plain"] == {"S": "val"}

    def test_from_dynamodb_item(self):
        from robotocore.services.stepfunctions.asl import _from_dynamodb_item

        result = _from_dynamodb_item(
            {"name": {"S": "Alice"}, "age": {"N": "30"}, "active": {"BOOL": True}}
        )
        assert result == {"name": "Alice", "age": 30, "active": True}

    def test_to_ddb_value_null(self):
        from robotocore.services.stepfunctions.asl import _to_ddb_value

        assert _to_ddb_value(None) == {"NULL": True}

    def test_to_ddb_value_list(self):
        from robotocore.services.stepfunctions.asl import _to_ddb_value

        result = _to_ddb_value([1, "two"])
        assert result == {"L": [{"N": "1"}, {"S": "two"}]}

    def test_to_ddb_value_nested_dict(self):
        from robotocore.services.stepfunctions.asl import _to_ddb_value

        result = _to_ddb_value({"nested": "val"})
        assert result == {"M": {"nested": {"S": "val"}}}

    def test_from_ddb_value_list(self):
        from robotocore.services.stepfunctions.asl import _from_ddb_value

        result = _from_ddb_value({"L": [{"S": "a"}, {"N": "1"}]})
        assert result == ["a", 1]

    def test_from_ddb_value_map(self):
        from robotocore.services.stepfunctions.asl import _from_ddb_value

        result = _from_ddb_value({"M": {"key": {"S": "val"}}})
        assert result == {"key": "val"}

    def test_from_ddb_value_null(self):
        from robotocore.services.stepfunctions.asl import _from_ddb_value

        assert _from_ddb_value({"NULL": True}) is None

    def test_from_ddb_value_string_set(self):
        from robotocore.services.stepfunctions.asl import _from_ddb_value

        result = _from_ddb_value({"SS": ["a", "b"]})
        assert result == ["a", "b"]


# ===========================================================================
# Additional edge cases
# ===========================================================================


class TestIntrinsicNestedCalls:
    def test_nested_intrinsic(self):
        result = evaluate_intrinsic("States.ArrayLength(States.Array(1, 2, 3))")
        assert result == 3

    def test_format_with_json_to_string(self):
        result = evaluate_intrinsic(
            "States.Format('data: {}', States.JsonToString($.obj))",
            {"obj": {"a": 1}},
        )
        assert "data:" in result
        assert '"a"' in result


class TestIntrinsicEdgeCases:
    def test_array_unique_with_objects(self):
        result = evaluate_intrinsic(
            "States.ArrayUnique($.arr)",
            {"arr": [{"a": 1}, {"a": 1}, {"b": 2}]},
        )
        assert len(result) == 2

    def test_math_add_floats(self):
        # When one operand is float
        result = evaluate_intrinsic("States.MathAdd(1.5, 2.5)")
        assert result == 4.0

    def test_string_split_empty_delimiter(self):
        result = evaluate_intrinsic("States.StringSplit('abc', '')")
        assert result == ["a", "b", "c"]
