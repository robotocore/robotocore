"""Semantic tests for StepFunctions mock execution.

Tests that the ASL executor correctly uses mock config to intercept Task states,
returning pre-defined results or throwing pre-defined errors.
"""

import pytest

from robotocore.services.stepfunctions.asl import ASLExecutionError, ASLExecutor

REGION = "us-east-1"
ACCOUNT_ID = "123456789012"


def _make_executor(definition: dict, mock_test_case: dict | None = None) -> ASLExecutor:
    """Create an ASLExecutor with optional mock test case."""
    executor = ASLExecutor(definition, REGION, ACCOUNT_ID, execution_arn="arn:test")
    if mock_test_case is not None:
        executor.mock_test_case = mock_test_case
    return executor


class TestMockTaskReturn:
    """Test that Task states return mock values when mock config is present."""

    def test_simple_pass_task_succeed_with_mock(self):
        """Pass -> Task (mocked) -> Succeed: Task returns mock value."""
        definition = {
            "StartAt": "PassState",
            "States": {
                "PassState": {
                    "Type": "Pass",
                    "Result": {"input": "data"},
                    "Next": "TaskState",
                },
                "TaskState": {
                    "Type": "Task",
                    "Resource": "arn:aws:lambda:us-east-1:123456789012:function:MyFunc",
                    "End": True,
                },
            },
        }
        mock_test_case = {
            "TaskState": {"Return": {"result": "mocked_success"}},
        }
        executor = _make_executor(definition, mock_test_case)
        output = executor.execute({})
        assert output == {"result": "mocked_success"}

    def test_task_throws_mock_error(self):
        """Task with Throw mock -> execution fails with the specified error."""
        definition = {
            "StartAt": "TaskState",
            "States": {
                "TaskState": {
                    "Type": "Task",
                    "Resource": "arn:aws:lambda:us-east-1:123456789012:function:MyFunc",
                    "End": True,
                },
            },
        }
        mock_test_case = {
            "TaskState": {"Throw": {"Error": "CustomError", "Cause": "Something failed"}},
        }
        executor = _make_executor(definition, mock_test_case)
        with pytest.raises(ASLExecutionError) as exc_info:
            executor.execute({})
        assert exc_info.value.error == "CustomError"
        assert exc_info.value.cause == "Something failed"

    def test_execution_name_hash_selects_test_case(self):
        """Execution name with '#HappyPath' suffix selects correct test case."""
        from robotocore.services.stepfunctions.mock_config import extract_test_case_from_name

        clean, tc = extract_test_case_from_name("my-execution#HappyPath")
        assert tc == "HappyPath"
        assert clean == "my-execution"

    def test_no_matching_test_case_executes_normally(self):
        """With no mock test case, Pass state executes normally."""
        definition = {
            "StartAt": "PassState",
            "States": {
                "PassState": {
                    "Type": "Pass",
                    "Result": {"normal": "execution"},
                    "End": True,
                },
            },
        }
        executor = _make_executor(definition, mock_test_case=None)
        output = executor.execute({})
        assert output == {"normal": "execution"}

    def test_no_mock_config_executes_normally(self):
        """With no mock config at all, Pass state executes normally."""
        definition = {
            "StartAt": "PassState",
            "States": {
                "PassState": {
                    "Type": "Pass",
                    "Result": {"normal": True},
                    "End": True,
                },
            },
        }
        executor = ASLExecutor(definition, REGION, ACCOUNT_ID)
        output = executor.execute({})
        assert output == {"normal": True}


class TestMockParallelState:
    """Test that Parallel states apply mocks to nested Task states."""

    def test_parallel_branches_use_mocks(self):
        """Parallel state with Task states in branches should use mock config."""
        definition = {
            "StartAt": "ParallelState",
            "States": {
                "ParallelState": {
                    "Type": "Parallel",
                    "Branches": [
                        {
                            "StartAt": "BranchTask1",
                            "States": {
                                "BranchTask1": {
                                    "Type": "Task",
                                    "Resource": "arn:aws:lambda:us-east-1:123456789012:function:F1",
                                    "End": True,
                                },
                            },
                        },
                        {
                            "StartAt": "BranchTask2",
                            "States": {
                                "BranchTask2": {
                                    "Type": "Task",
                                    "Resource": "arn:aws:lambda:us-east-1:123456789012:function:F2",
                                    "End": True,
                                },
                            },
                        },
                    ],
                    "End": True,
                },
            },
        }
        mock_test_case = {
            "BranchTask1": {"Return": {"branch": 1}},
            "BranchTask2": {"Return": {"branch": 2}},
        }
        executor = _make_executor(definition, mock_test_case)
        output = executor.execute({})
        assert output == [{"branch": 1}, {"branch": 2}]


class TestMockExecutionHistory:
    """Test that mock execution produces correct history events."""

    def test_mock_execution_has_history_events(self):
        """Mock execution should record history events for traversed states."""
        definition = {
            "StartAt": "TaskState",
            "States": {
                "TaskState": {
                    "Type": "Task",
                    "Resource": "arn:aws:lambda:us-east-1:123456789012:function:MyFunc",
                    "End": True,
                },
            },
        }
        mock_test_case = {
            "TaskState": {"Return": {"result": "mocked"}},
        }
        executor = _make_executor(definition, mock_test_case)
        executor.execute({})
        assert executor.history is not None
        events = executor.history.get_events()
        event_types = [e["type"] for e in events]
        assert "ExecutionStarted" in event_types
        assert "TaskStateEntered" in event_types
        assert "ExecutionSucceeded" in event_types


class TestMockExecutionOutput:
    """Test execution output matches mock values."""

    def test_output_matches_final_state_return(self):
        """Execution output should match the Return value of the final state."""
        definition = {
            "StartAt": "First",
            "States": {
                "First": {
                    "Type": "Task",
                    "Resource": "arn:aws:lambda:us-east-1:123456789012:function:F1",
                    "Next": "Second",
                },
                "Second": {
                    "Type": "Task",
                    "Resource": "arn:aws:lambda:us-east-1:123456789012:function:F2",
                    "End": True,
                },
            },
        }
        mock_test_case = {
            "First": {"Return": {"step": 1}},
            "Second": {"Return": {"final": "output"}},
        }
        executor = _make_executor(definition, mock_test_case)
        output = executor.execute({})
        assert output == {"final": "output"}

    def test_failed_output_matches_throw(self):
        """Failed execution should have error/cause matching Throw config."""
        definition = {
            "StartAt": "TaskState",
            "States": {
                "TaskState": {
                    "Type": "Task",
                    "Resource": "arn:aws:lambda:us-east-1:123456789012:function:F",
                    "End": True,
                },
            },
        }
        mock_test_case = {
            "TaskState": {"Throw": {"Error": "TestError", "Cause": "Test cause message"}},
        }
        executor = _make_executor(definition, mock_test_case)
        with pytest.raises(ASLExecutionError) as exc_info:
            executor.execute({})
        assert exc_info.value.error == "TestError"
        assert exc_info.value.cause == "Test cause message"


class TestMultipleStateMachines:
    """Test handling configs with multiple state machines."""

    def test_different_machines_different_mocks(self):
        """Each state machine can have independent mock configs."""
        from robotocore.services.stepfunctions.mock_config import get_test_case

        config = {
            "StateMachines": {
                "MachineA": {
                    "TestCases": {
                        "Case1": {"S1": {"Return": {"from": "A"}}},
                    }
                },
                "MachineB": {
                    "TestCases": {
                        "Case1": {"S1": {"Return": {"from": "B"}}},
                    }
                },
            }
        }
        tc_a = get_test_case(config, "MachineA", "Case1")
        tc_b = get_test_case(config, "MachineB", "Case1")
        assert tc_a is not None
        assert tc_b is not None
        assert tc_a["S1"]["Return"]["from"] == "A"
        assert tc_b["S1"]["Return"]["from"] == "B"

    def test_state_not_mocked_in_task_executes_normally(self):
        """A state not in the mock test case should try to execute normally.

        For non-Task states like Pass, this works fine. For Task states
        with no real backend, it would normally fail, but Pass is safe.
        """
        definition = {
            "StartAt": "PassState",
            "States": {
                "PassState": {
                    "Type": "Pass",
                    "Result": {"pass": "through"},
                    "End": True,
                },
            },
        }
        mock_test_case = {
            "SomeOtherState": {"Return": {"not": "relevant"}},
        }
        executor = _make_executor(definition, mock_test_case)
        output = executor.execute({})
        # Pass state is not a Task — mock doesn't apply; it runs normally
        assert output == {"pass": "through"}
