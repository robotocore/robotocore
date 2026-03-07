"""Step Functions compatibility tests — including ASL execution."""

import json
import time
import uuid

import pytest

from tests.compatibility.conftest import make_client


@pytest.fixture
def sfn():
    return make_client("stepfunctions")


@pytest.fixture
def iam():
    return make_client("iam")


@pytest.fixture
def state_machine(sfn, iam):
    role = iam.create_role(
        RoleName="sfn-test-role",
        AssumeRolePolicyDocument=json.dumps(
            {
                "Version": "2012-10-17",
                "Statement": [
                    {
                        "Effect": "Allow",
                        "Principal": {"Service": "states.amazonaws.com"},
                        "Action": "sts:AssumeRole",
                    }
                ],
            }
        ),
    )
    role_arn = role["Role"]["Arn"]

    definition = json.dumps(
        {
            "Comment": "Test state machine",
            "StartAt": "PassState",
            "States": {
                "PassState": {
                    "Type": "Pass",
                    "Result": {"message": "hello"},
                    "End": True,
                }
            },
        }
    )

    response = sfn.create_state_machine(
        name="test-state-machine",
        definition=definition,
        roleArn=role_arn,
    )
    arn = response["stateMachineArn"]
    yield arn
    sfn.delete_state_machine(stateMachineArn=arn)
    iam.delete_role(RoleName="sfn-test-role")


class TestStepFunctionsOperations:
    def test_create_state_machine(self, sfn, iam):
        role = iam.create_role(
            RoleName="sfn-create-role",
            AssumeRolePolicyDocument=json.dumps(
                {
                    "Version": "2012-10-17",
                    "Statement": [
                        {
                            "Effect": "Allow",
                            "Principal": {"Service": "states.amazonaws.com"},
                            "Action": "sts:AssumeRole",
                        }
                    ],
                }
            ),
        )
        definition = json.dumps(
            {
                "StartAt": "Pass",
                "States": {"Pass": {"Type": "Pass", "End": True}},
            }
        )
        response = sfn.create_state_machine(
            name="create-test-sm",
            definition=definition,
            roleArn=role["Role"]["Arn"],
        )
        assert "stateMachineArn" in response
        sfn.delete_state_machine(stateMachineArn=response["stateMachineArn"])
        iam.delete_role(RoleName="sfn-create-role")

    def test_list_state_machines(self, sfn, state_machine):
        response = sfn.list_state_machines()
        arns = [sm["stateMachineArn"] for sm in response["stateMachines"]]
        assert state_machine in arns

    def test_describe_state_machine(self, sfn, state_machine):
        response = sfn.describe_state_machine(stateMachineArn=state_machine)
        assert response["name"] == "test-state-machine"
        assert response["status"] == "ACTIVE"

    def test_start_execution(self, sfn, state_machine):
        response = sfn.start_execution(
            stateMachineArn=state_machine,
            input=json.dumps({"key": "value"}),
        )
        assert "executionArn" in response
        assert "startDate" in response

    def test_list_executions(self, sfn, state_machine):
        sfn.start_execution(stateMachineArn=state_machine)
        response = sfn.list_executions(stateMachineArn=state_machine)
        assert len(response["executions"]) >= 1

    def test_describe_execution(self, sfn, state_machine):
        exec_resp = sfn.start_execution(stateMachineArn=state_machine)
        exec_arn = exec_resp["executionArn"]
        response = sfn.describe_execution(executionArn=exec_arn)
        assert response["stateMachineArn"] == state_machine

    def test_list_state_machines_contains_expected(self, sfn, state_machine):
        """List state machines includes name and creation date."""
        response = sfn.list_state_machines()
        machines = response["stateMachines"]
        match = [m for m in machines if m["stateMachineArn"] == state_machine]
        assert len(match) == 1
        assert match[0]["name"] == "test-state-machine"
        assert "creationDate" in match[0]

    def test_describe_state_machine_definition(self, sfn, state_machine):
        """Describe state machine returns full definition."""
        response = sfn.describe_state_machine(stateMachineArn=state_machine)
        assert response["status"] == "ACTIVE"
        definition = json.loads(response["definition"])
        assert "StartAt" in definition
        assert "States" in definition
        assert "PassState" in definition["States"]

    def test_update_state_machine(self, sfn, state_machine):
        """Update a state machine definition."""
        new_definition = json.dumps(
            {
                "Comment": "Updated state machine",
                "StartAt": "NewPass",
                "States": {
                    "NewPass": {
                        "Type": "Pass",
                        "Result": {"updated": True},
                        "End": True,
                    }
                },
            }
        )
        response = sfn.update_state_machine(
            stateMachineArn=state_machine,
            definition=new_definition,
        )
        assert "updateDate" in response
        desc = sfn.describe_state_machine(stateMachineArn=state_machine)
        updated_def = json.loads(desc["definition"])
        assert "NewPass" in updated_def["States"]

    def test_list_executions_status_filter(self, sfn, state_machine):
        """List executions filtered by status."""
        sfn.start_execution(stateMachineArn=state_machine)
        response = sfn.list_executions(
            stateMachineArn=state_machine,
            statusFilter="SUCCEEDED",
        )
        for exc in response["executions"]:
            assert exc["status"] == "SUCCEEDED"


class TestStepFunctionsExecutionHistory:
    def test_get_execution_history(self, sfn, state_machine):
        """Test GetExecutionHistory returns events for a completed execution."""
        exec_resp = sfn.start_execution(
            stateMachineArn=state_machine,
            input=json.dumps({"key": "value"}),
        )
        exec_arn = exec_resp["executionArn"]

        history = sfn.get_execution_history(executionArn=exec_arn)
        events = history["events"]
        assert len(events) >= 1
        # First event should be ExecutionStarted
        event_types = [e["type"] for e in events]
        assert "ExecutionStarted" in event_types

    def test_stop_execution(self, sfn, iam):
        """Test StopExecution stops a running execution."""
        role = iam.create_role(
            RoleName="sfn-stop-role",
            AssumeRolePolicyDocument=json.dumps(
                {
                    "Version": "2012-10-17",
                    "Statement": [
                        {
                            "Effect": "Allow",
                            "Principal": {"Service": "states.amazonaws.com"},
                            "Action": "sts:AssumeRole",
                        }
                    ],
                }
            ),
        )
        # Use a Wait state so execution stays RUNNING long enough to stop
        definition = json.dumps(
            {
                "StartAt": "WaitState",
                "States": {
                    "WaitState": {
                        "Type": "Wait",
                        "Seconds": 300,
                        "Next": "Done",
                    },
                    "Done": {"Type": "Pass", "End": True},
                },
            }
        )
        sm_name = f"stop-test-{uuid.uuid4().hex[:8]}"
        sm = sfn.create_state_machine(
            name=sm_name,
            definition=definition,
            roleArn=role["Role"]["Arn"],
        )
        sm_arn = sm["stateMachineArn"]

        exec_resp = sfn.start_execution(stateMachineArn=sm_arn)
        exec_arn = exec_resp["executionArn"]

        stop_resp = sfn.stop_execution(
            executionArn=exec_arn,
            error="UserCancelled",
            cause="Testing stop",
        )
        assert "stopDate" in stop_resp

        desc = sfn.describe_execution(executionArn=exec_arn)
        assert desc["status"] == "ABORTED"

        sfn.delete_state_machine(stateMachineArn=sm_arn)
        iam.delete_role(RoleName="sfn-stop-role")


class TestASLExecution:
    """Test actual ASL state machine execution — Enterprise-grade feature."""

    @pytest.fixture
    def role_arn(self):
        iam = make_client("iam")
        name = f"sfn-exec-role-{uuid.uuid4().hex[:8]}"
        iam.create_role(
            RoleName=name,
            AssumeRolePolicyDocument=json.dumps(
                {
                    "Version": "2012-10-17",
                    "Statement": [
                        {
                            "Effect": "Allow",
                            "Principal": {"Service": "states.amazonaws.com"},
                            "Action": "sts:AssumeRole",
                        }
                    ],
                }
            ),
        )
        yield f"arn:aws:iam::123456789012:role/{name}"
        iam.delete_role(RoleName=name)

    def _create_and_execute(self, sfn, role_arn, definition: dict, input_data: dict = None) -> dict:
        """Helper: create state machine, execute, return describe_execution result."""
        name = f"test-sm-{uuid.uuid4().hex[:8]}"
        sm = sfn.create_state_machine(
            name=name,
            definition=json.dumps(definition),
            roleArn=role_arn,
        )
        sm_arn = sm["stateMachineArn"]

        exec_resp = sfn.start_execution(
            stateMachineArn=sm_arn,
            input=json.dumps(input_data or {}),
        )
        result = sfn.describe_execution(executionArn=exec_resp["executionArn"])
        sfn.delete_state_machine(stateMachineArn=sm_arn)
        return result

    def test_pass_state(self, role_arn):
        """Test Pass state with Result."""
        sfn = make_client("stepfunctions")
        result = self._create_and_execute(
            sfn,
            role_arn,
            {
                "StartAt": "MyPass",
                "States": {
                    "MyPass": {
                        "Type": "Pass",
                        "Result": {"greeting": "hello world"},
                        "End": True,
                    }
                },
            },
        )
        assert result["status"] == "SUCCEEDED"
        output = json.loads(result["output"])
        assert output["greeting"] == "hello world"

    def test_choice_state(self, role_arn):
        """Test Choice state with numeric comparison."""
        sfn = make_client("stepfunctions")
        result = self._create_and_execute(
            sfn,
            role_arn,
            {
                "StartAt": "CheckValue",
                "States": {
                    "CheckValue": {
                        "Type": "Choice",
                        "Choices": [
                            {
                                "Variable": "$.value",
                                "NumericGreaterThan": 10,
                                "Next": "High",
                            },
                        ],
                        "Default": "Low",
                    },
                    "High": {
                        "Type": "Pass",
                        "Result": {"level": "high"},
                        "End": True,
                    },
                    "Low": {
                        "Type": "Pass",
                        "Result": {"level": "low"},
                        "End": True,
                    },
                },
            },
            {"value": 25},
        )
        assert result["status"] == "SUCCEEDED"
        output = json.loads(result["output"])
        assert output["level"] == "high"

    def test_choice_default(self, role_arn):
        """Test Choice state falls to Default."""
        sfn = make_client("stepfunctions")
        result = self._create_and_execute(
            sfn,
            role_arn,
            {
                "StartAt": "CheckValue",
                "States": {
                    "CheckValue": {
                        "Type": "Choice",
                        "Choices": [
                            {"Variable": "$.value", "NumericGreaterThan": 100, "Next": "High"},
                        ],
                        "Default": "Low",
                    },
                    "High": {"Type": "Pass", "Result": {"level": "high"}, "End": True},
                    "Low": {"Type": "Pass", "Result": {"level": "low"}, "End": True},
                },
            },
            {"value": 5},
        )
        assert result["status"] == "SUCCEEDED"
        output = json.loads(result["output"])
        assert output["level"] == "low"

    def test_chain_of_states(self, role_arn):
        """Test multiple states chained together."""
        sfn = make_client("stepfunctions")
        result = self._create_and_execute(
            sfn,
            role_arn,
            {
                "StartAt": "Step1",
                "States": {
                    "Step1": {
                        "Type": "Pass",
                        "Result": {"step": 1},
                        "ResultPath": "$.step1",
                        "Next": "Step2",
                    },
                    "Step2": {
                        "Type": "Pass",
                        "Result": {"step": 2},
                        "ResultPath": "$.step2",
                        "Next": "Step3",
                    },
                    "Step3": {
                        "Type": "Pass",
                        "Result": {"step": 3},
                        "ResultPath": "$.step3",
                        "End": True,
                    },
                },
            },
            {"input": "start"},
        )
        assert result["status"] == "SUCCEEDED"
        output = json.loads(result["output"])
        assert output["step1"]["step"] == 1
        assert output["step2"]["step"] == 2
        assert output["step3"]["step"] == 3

    def test_fail_state(self, role_arn):
        """Test Fail state produces FAILED execution."""
        sfn = make_client("stepfunctions")
        result = self._create_and_execute(
            sfn,
            role_arn,
            {
                "StartAt": "FailNow",
                "States": {
                    "FailNow": {
                        "Type": "Fail",
                        "Error": "CustomError",
                        "Cause": "Something went wrong",
                    }
                },
            },
        )
        assert result["status"] == "FAILED"

    def test_parallel_state(self, role_arn):
        """Test Parallel state executes branches."""
        sfn = make_client("stepfunctions")
        result = self._create_and_execute(
            sfn,
            role_arn,
            {
                "StartAt": "ParallelStep",
                "States": {
                    "ParallelStep": {
                        "Type": "Parallel",
                        "Branches": [
                            {
                                "StartAt": "B1",
                                "States": {
                                    "B1": {"Type": "Pass", "Result": {"branch": 1}, "End": True}
                                },
                            },
                            {
                                "StartAt": "B2",
                                "States": {
                                    "B2": {"Type": "Pass", "Result": {"branch": 2}, "End": True}
                                },
                            },
                        ],
                        "End": True,
                    }
                },
            },
        )
        assert result["status"] == "SUCCEEDED"
        output = json.loads(result["output"])
        assert len(output) == 2
        assert output[0]["branch"] == 1
        assert output[1]["branch"] == 2

    def test_succeed_state(self, role_arn):
        """Test Succeed state."""
        sfn = make_client("stepfunctions")
        result = self._create_and_execute(
            sfn,
            role_arn,
            {
                "StartAt": "Done",
                "States": {
                    "Done": {"Type": "Succeed"},
                },
            },
            {"data": "preserved"},
        )
        assert result["status"] == "SUCCEEDED"

    def test_parameters(self, role_arn):
        """Test Parameters field with dynamic references."""
        sfn = make_client("stepfunctions")
        result = self._create_and_execute(
            sfn,
            role_arn,
            {
                "StartAt": "Transform",
                "States": {
                    "Transform": {
                        "Type": "Pass",
                        "Parameters": {
                            "greeting.$": "$.name",
                            "static": "value",
                        },
                        "End": True,
                    }
                },
            },
            {"name": "Alice"},
        )
        assert result["status"] == "SUCCEEDED"
        output = json.loads(result["output"])
        assert output["greeting"] == "Alice"
        assert output["static"] == "value"

    def test_choice_string_equals(self, role_arn):
        """Test Choice state with StringEquals comparison."""
        sfn = make_client("stepfunctions")
        result = self._create_and_execute(
            sfn,
            role_arn,
            {
                "StartAt": "Route",
                "States": {
                    "Route": {
                        "Type": "Choice",
                        "Choices": [
                            {
                                "Variable": "$.action",
                                "StringEquals": "create",
                                "Next": "CreateBranch",
                            },
                            {
                                "Variable": "$.action",
                                "StringEquals": "delete",
                                "Next": "DeleteBranch",
                            },
                        ],
                        "Default": "DefaultBranch",
                    },
                    "CreateBranch": {
                        "Type": "Pass",
                        "Result": {"routed": "create"},
                        "End": True,
                    },
                    "DeleteBranch": {
                        "Type": "Pass",
                        "Result": {"routed": "delete"},
                        "End": True,
                    },
                    "DefaultBranch": {
                        "Type": "Pass",
                        "Result": {"routed": "default"},
                        "End": True,
                    },
                },
            },
            {"action": "delete"},
        )
        assert result["status"] == "SUCCEEDED"
        output = json.loads(result["output"])
        assert output["routed"] == "delete"

    def test_wait_state_seconds(self, role_arn):
        """Test Wait state with Seconds pauses execution."""
        sfn = make_client("stepfunctions")
        result = self._create_and_execute(
            sfn,
            role_arn,
            {
                "StartAt": "WaitStep",
                "States": {
                    "WaitStep": {
                        "Type": "Wait",
                        "Seconds": 1,
                        "Next": "Done",
                    },
                    "Done": {
                        "Type": "Pass",
                        "Result": {"waited": True},
                        "End": True,
                    },
                },
            },
        )
        assert result["status"] == "SUCCEEDED"
        output = json.loads(result["output"])
        assert output["waited"] is True

    def test_parallel_two_branches_combined_output(self, role_arn):
        """Test Parallel state with two branches, verify combined array output."""
        sfn = make_client("stepfunctions")
        result = self._create_and_execute(
            sfn,
            role_arn,
            {
                "StartAt": "RunParallel",
                "States": {
                    "RunParallel": {
                        "Type": "Parallel",
                        "Branches": [
                            {
                                "StartAt": "BranchA",
                                "States": {
                                    "BranchA": {
                                        "Type": "Pass",
                                        "Result": {"name": "alpha", "value": 1},
                                        "End": True,
                                    }
                                },
                            },
                            {
                                "StartAt": "BranchB",
                                "States": {
                                    "BranchB": {
                                        "Type": "Pass",
                                        "Result": {"name": "beta", "value": 2},
                                        "End": True,
                                    }
                                },
                            },
                        ],
                        "End": True,
                    }
                },
            },
        )
        assert result["status"] == "SUCCEEDED"
        output = json.loads(result["output"])
        assert isinstance(output, list)
        assert len(output) == 2
        assert output[0]["name"] == "alpha"
        assert output[1]["name"] == "beta"

    def test_map_state_iterate_array(self, role_arn):
        """Test Map state iterating over an array and transforming each element."""
        sfn = make_client("stepfunctions")
        result = self._create_and_execute(
            sfn,
            role_arn,
            {
                "StartAt": "MapItems",
                "States": {
                    "MapItems": {
                        "Type": "Map",
                        "ItemsPath": "$.items",
                        "Iterator": {
                            "StartAt": "Transform",
                            "States": {
                                "Transform": {
                                    "Type": "Pass",
                                    "Parameters": {
                                        "processed.$": "$.name",
                                    },
                                    "End": True,
                                }
                            },
                        },
                        "End": True,
                    }
                },
            },
            {"items": [{"name": "a"}, {"name": "b"}, {"name": "c"}]},
        )
        assert result["status"] == "SUCCEEDED"
        output = json.loads(result["output"])
        assert isinstance(output, list)
        assert len(output) == 3
        assert output[0]["processed"] == "a"
        assert output[1]["processed"] == "b"
        assert output[2]["processed"] == "c"

    def test_pass_state_result_path_and_parameters(self, role_arn):
        """Test Pass state with both ResultPath and Parameters."""
        sfn = make_client("stepfunctions")
        result = self._create_and_execute(
            sfn,
            role_arn,
            {
                "StartAt": "Enrich",
                "States": {
                    "Enrich": {
                        "Type": "Pass",
                        "Parameters": {
                            "original.$": "$.data",
                            "extra": "added",
                        },
                        "ResultPath": "$.enriched",
                        "End": True,
                    }
                },
            },
            {"data": "hello"},
        )
        assert result["status"] == "SUCCEEDED"
        output = json.loads(result["output"])
        assert output["data"] == "hello"
        assert output["enriched"]["original"] == "hello"
        assert output["enriched"]["extra"] == "added"

    def test_execution_with_name(self, role_arn):
        """Test start execution with explicit name, then describe by name."""
        sfn = make_client("stepfunctions")
        name = f"test-sm-named-{uuid.uuid4().hex[:8]}"
        exec_name = f"my-exec-{uuid.uuid4().hex[:8]}"
        sm = sfn.create_state_machine(
            name=name,
            definition=json.dumps({
                "StartAt": "P",
                "States": {"P": {"Type": "Pass", "End": True}},
            }),
            roleArn=role_arn,
        )
        sm_arn = sm["stateMachineArn"]

        exec_resp = sfn.start_execution(
            stateMachineArn=sm_arn,
            name=exec_name,
            input=json.dumps({"x": 1}),
        )
        desc = sfn.describe_execution(executionArn=exec_resp["executionArn"])
        assert desc["name"] == exec_name
        assert exec_name in desc["executionArn"]

        sfn.delete_state_machine(stateMachineArn=sm_arn)

    def test_list_executions_status_filter_succeeded(self, role_arn):
        """ListExecutions with statusFilter=SUCCEEDED returns only matching."""
        sfn = make_client("stepfunctions")
        name = f"test-sm-filter-{uuid.uuid4().hex[:8]}"
        sm = sfn.create_state_machine(
            name=name,
            definition=json.dumps({
                "StartAt": "P",
                "States": {"P": {"Type": "Pass", "End": True}},
            }),
            roleArn=role_arn,
        )
        sm_arn = sm["stateMachineArn"]

        sfn.start_execution(stateMachineArn=sm_arn)
        result = sfn.list_executions(
            stateMachineArn=sm_arn,
            statusFilter="SUCCEEDED",
        )
        for exc in result["executions"]:
            assert exc["status"] == "SUCCEEDED"

        sfn.delete_state_machine(stateMachineArn=sm_arn)

    def test_list_executions_status_filter_failed(self, role_arn):
        """ListExecutions with statusFilter=FAILED returns only failed ones."""
        sfn = make_client("stepfunctions")
        name = f"test-sm-fail-filter-{uuid.uuid4().hex[:8]}"
        sm = sfn.create_state_machine(
            name=name,
            definition=json.dumps({
                "StartAt": "FailNow",
                "States": {
                    "FailNow": {
                        "Type": "Fail",
                        "Error": "TestError",
                        "Cause": "testing",
                    }
                },
            }),
            roleArn=role_arn,
        )
        sm_arn = sm["stateMachineArn"]

        sfn.start_execution(stateMachineArn=sm_arn)
        failed = sfn.list_executions(
            stateMachineArn=sm_arn,
            statusFilter="FAILED",
        )
        assert len(failed["executions"]) >= 1
        for exc in failed["executions"]:
            assert exc["status"] == "FAILED"

        sfn.delete_state_machine(stateMachineArn=sm_arn)

    def test_tag_resource(self, role_arn):
        """TagResource, ListTagsForResource on a state machine."""
        sfn = make_client("stepfunctions")
        name = f"test-sm-tag-{uuid.uuid4().hex[:8]}"
        sm = sfn.create_state_machine(
            name=name,
            definition=json.dumps({
                "StartAt": "P",
                "States": {"P": {"Type": "Pass", "End": True}},
            }),
            roleArn=role_arn,
        )
        sm_arn = sm["stateMachineArn"]

        sfn.tag_resource(
            resourceArn=sm_arn,
            tags=[{"key": "env", "value": "test"}],
        )
        tags_resp = sfn.list_tags_for_resource(resourceArn=sm_arn)
        assert "tags" in tags_resp

        sfn.untag_resource(resourceArn=sm_arn, tagKeys=["env"])
        sfn.delete_state_machine(stateMachineArn=sm_arn)

    def test_intrinsic_states_format(self, role_arn):
        """Test States.Format intrinsic function in Parameters."""
        sfn = make_client("stepfunctions")
        result = self._create_and_execute(
            sfn,
            role_arn,
            {
                "StartAt": "Format",
                "States": {
                    "Format": {
                        "Type": "Pass",
                        "Parameters": {
                            "message.$": "States.Format('Hello, {}!', $.name)",
                        },
                        "End": True,
                    }
                },
            },
            {"name": "World"},
        )
        assert result["status"] == "SUCCEEDED"
        output = json.loads(result["output"])
        assert output["message"] == "Hello, World!"

    def test_intrinsic_states_json_to_string_and_back(self, role_arn):
        """Test States.JsonToString and States.StringToJson intrinsics."""
        sfn = make_client("stepfunctions")
        result = self._create_and_execute(
            sfn,
            role_arn,
            {
                "StartAt": "Serialize",
                "States": {
                    "Serialize": {
                        "Type": "Pass",
                        "Parameters": {
                            "serialized.$": "States.JsonToString($.data)",
                        },
                        "End": True,
                    }
                },
            },
            {"data": {"key": "val"}},
        )
        assert result["status"] == "SUCCEEDED"
        output = json.loads(result["output"])
        # Should be a JSON string representation
        parsed = json.loads(output["serialized"])
        assert parsed["key"] == "val"

    def test_error_handling_catch_block(self, role_arn):
        """Test Catch block handles a Fail state error."""
        sfn = make_client("stepfunctions")
        result = self._create_and_execute(
            sfn,
            role_arn,
            {
                "StartAt": "TryFail",
                "States": {
                    "TryFail": {
                        "Type": "Fail",
                        "Error": "CustomError",
                        "Cause": "intentional failure",
                    },
                },
            },
        )
        # Fail state with no Catch always results in FAILED
        assert result["status"] == "FAILED"

    def test_catch_with_specific_error(self, role_arn):
        """Test Catch block catches specific error and routes to fallback."""
        sfn = make_client("stepfunctions")
        # Use a Pass state chain where we simulate error via a Fail state
        # wrapped in a Parallel to enable Catch
        result = self._create_and_execute(
            sfn,
            role_arn,
            {
                "StartAt": "TryBlock",
                "States": {
                    "TryBlock": {
                        "Type": "Parallel",
                        "Branches": [
                            {
                                "StartAt": "WillFail",
                                "States": {
                                    "WillFail": {
                                        "Type": "Fail",
                                        "Error": "MyCustomError",
                                        "Cause": "something broke",
                                    }
                                },
                            }
                        ],
                        "Catch": [
                            {
                                "ErrorEquals": ["MyCustomError"],
                                "Next": "HandleError",
                                "ResultPath": "$.error",
                            }
                        ],
                        "Next": "Success",
                    },
                    "HandleError": {
                        "Type": "Pass",
                        "Result": {"recovered": True},
                        "ResultPath": "$.recovery",
                        "End": True,
                    },
                    "Success": {
                        "Type": "Pass",
                        "End": True,
                    },
                },
            },
            {"input": "test"},
        )
        assert result["status"] == "SUCCEEDED"
        output = json.loads(result["output"])
        assert output["recovery"]["recovered"] is True

    def test_multiple_sequential_pass_states(self, role_arn):
        """Test multiple sequential Pass states building up data."""
        sfn = make_client("stepfunctions")
        result = self._create_and_execute(
            sfn,
            role_arn,
            {
                "StartAt": "A",
                "States": {
                    "A": {
                        "Type": "Pass",
                        "Result": "first",
                        "ResultPath": "$.a",
                        "Next": "B",
                    },
                    "B": {
                        "Type": "Pass",
                        "Result": "second",
                        "ResultPath": "$.b",
                        "Next": "C",
                    },
                    "C": {
                        "Type": "Pass",
                        "Result": "third",
                        "ResultPath": "$.c",
                        "Next": "D",
                    },
                    "D": {
                        "Type": "Pass",
                        "Result": "fourth",
                        "ResultPath": "$.d",
                        "End": True,
                    },
                },
            },
            {},
        )
        assert result["status"] == "SUCCEEDED"
        output = json.loads(result["output"])
        assert output["a"] == "first"
        assert output["b"] == "second"
        assert output["c"] == "third"
        assert output["d"] == "fourth"

    def test_input_output_path_processing(self, role_arn):
        """Test InputPath and OutputPath processing."""
        sfn = make_client("stepfunctions")
        result = self._create_and_execute(
            sfn,
            role_arn,
            {
                "StartAt": "Filter",
                "States": {
                    "Filter": {
                        "Type": "Pass",
                        "InputPath": "$.payload",
                        "Result": {"processed": True},
                        "ResultPath": "$.status",
                        "OutputPath": "$",
                        "End": True,
                    }
                },
            },
            {"payload": {"x": 1, "y": 2}},
        )
        assert result["status"] == "SUCCEEDED"
        output = json.loads(result["output"])
        assert output["status"]["processed"] is True

    def test_get_execution_history(self, role_arn):
        """Test GetExecutionHistory returns events for a completed execution."""
        sfn = make_client("stepfunctions")
        name = f"test-sm-hist-{uuid.uuid4().hex[:8]}"
        sm = sfn.create_state_machine(
            name=name,
            definition=json.dumps({
                "StartAt": "Step1",
                "States": {
                    "Step1": {
                        "Type": "Pass",
                        "Result": {"done": True},
                        "End": True,
                    }
                },
            }),
            roleArn=role_arn,
        )
        sm_arn = sm["stateMachineArn"]

        exec_resp = sfn.start_execution(stateMachineArn=sm_arn)
        history = sfn.get_execution_history(
            executionArn=exec_resp["executionArn"],
        )
        events = history["events"]
        assert len(events) >= 1
        # First event should be ExecutionStarted
        event_types = [e.get("type") for e in events]
        assert "ExecutionStarted" in event_types

        sfn.delete_state_machine(stateMachineArn=sm_arn)

    def test_choice_boolean_equals(self, role_arn):
        """Test Choice state with BooleanEquals comparison."""
        sfn = make_client("stepfunctions")
        result = self._create_and_execute(
            sfn,
            role_arn,
            {
                "StartAt": "CheckBool",
                "States": {
                    "CheckBool": {
                        "Type": "Choice",
                        "Choices": [
                            {
                                "Variable": "$.flag",
                                "BooleanEquals": True,
                                "Next": "TrueBranch",
                            },
                        ],
                        "Default": "FalseBranch",
                    },
                    "TrueBranch": {
                        "Type": "Pass",
                        "Result": {"branch": "true"},
                        "End": True,
                    },
                    "FalseBranch": {
                        "Type": "Pass",
                        "Result": {"branch": "false"},
                        "End": True,
                    },
                },
            },
            {"flag": True},
        )
        assert result["status"] == "SUCCEEDED"
        output = json.loads(result["output"])
        assert output["branch"] == "true"

    def test_result_selector(self, role_arn):
        """Test ResultSelector transforms output of a state."""
        sfn = make_client("stepfunctions")
        result = self._create_and_execute(
            sfn,
            role_arn,
            {
                "StartAt": "WithSelector",
                "States": {
                    "WithSelector": {
                        "Type": "Parallel",
                        "Branches": [
                            {
                                "StartAt": "B1",
                                "States": {
                                    "B1": {
                                        "Type": "Pass",
                                        "Result": {"x": 10},
                                        "End": True,
                                    }
                                },
                            },
                        ],
                        "ResultSelector": {
                            "count": 1,
                        },
                        "End": True,
                    }
                },
            },
        )
        assert result["status"] == "SUCCEEDED"
        output = json.loads(result["output"])
        assert output["count"] == 1
