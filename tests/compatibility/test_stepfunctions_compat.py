"""Step Functions compatibility tests — including ASL execution."""

import json
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

    def test_wait_seconds(self, role_arn):
        """Test Wait state with Seconds (should complete immediately in emulator)."""
        sfn = make_client("stepfunctions")
        result = self._create_and_execute(
            sfn,
            role_arn,
            {
                "StartAt": "WaitStep",
                "States": {
                    "WaitStep": {
                        "Type": "Wait",
                        "Seconds": 0,
                        "Next": "Done",
                    },
                    "Done": {"Type": "Pass", "End": True},
                },
            },
            {"data": "preserved"},
        )
        assert result["status"] == "SUCCEEDED"

    def test_map_state_inline(self, role_arn):
        """Test Map state with inline iteration."""
        sfn = make_client("stepfunctions")
        result = self._create_and_execute(
            sfn,
            role_arn,
            {
                "StartAt": "MapStep",
                "States": {
                    "MapStep": {
                        "Type": "Map",
                        "ItemsPath": "$.items",
                        "Iterator": {
                            "StartAt": "AddField",
                            "States": {
                                "AddField": {
                                    "Type": "Pass",
                                    "Result": "processed",
                                    "ResultPath": "$.status",
                                    "End": True,
                                }
                            },
                        },
                        "End": True,
                    }
                },
            },
            {"items": [{"id": 1}, {"id": 2}, {"id": 3}]},
        )
        assert result["status"] == "SUCCEEDED"
        output = json.loads(result["output"])
        assert len(output) == 3
        for item in output:
            assert item["status"] == "processed"

    def test_input_output_path(self, role_arn):
        """Test InputPath and OutputPath filtering."""
        sfn = make_client("stepfunctions")
        result = self._create_and_execute(
            sfn,
            role_arn,
            {
                "StartAt": "Filter",
                "States": {
                    "Filter": {
                        "Type": "Pass",
                        "InputPath": "$.data",
                        "OutputPath": "$.value",
                        "End": True,
                    }
                },
            },
            {"data": {"value": 42, "extra": "ignored"}},
        )
        assert result["status"] == "SUCCEEDED"
        output = json.loads(result["output"])
        assert output == 42


class TestStepFunctionsTags:
    def test_tag_and_list_tags(self):
        """Tag a state machine and list tags."""
        sfn = make_client("stepfunctions")
        iam = make_client("iam")

        name = f"tag-sm-{uuid.uuid4().hex[:8]}"
        role = iam.create_role(
            RoleName=name,
            AssumeRolePolicyDocument=json.dumps({
                "Version": "2012-10-17",
                "Statement": [{
                    "Effect": "Allow",
                    "Principal": {"Service": "states.amazonaws.com"},
                    "Action": "sts:AssumeRole",
                }],
            }),
        )
        sm = sfn.create_state_machine(
            name=name,
            definition=json.dumps({
                "StartAt": "Pass", "States": {"Pass": {"Type": "Pass", "End": True}}
            }),
            roleArn=role["Role"]["Arn"],
        )
        arn = sm["stateMachineArn"]

        try:
            sfn.tag_resource(
                resourceArn=arn,
                tags=[
                    {"key": "env", "value": "test"},
                    {"key": "team", "value": "platform"},
                ],
            )
            response = sfn.list_tags_for_resource(resourceArn=arn)
            tag_map = {t["key"]: t["value"] for t in response["tags"]}
            assert tag_map["env"] == "test"
            assert tag_map["team"] == "platform"

            sfn.untag_resource(resourceArn=arn, tagKeys=["env"])
            response = sfn.list_tags_for_resource(resourceArn=arn)
            tag_map = {t["key"]: t["value"] for t in response["tags"]}
            assert "env" not in tag_map
            assert tag_map["team"] == "platform"
        finally:
            sfn.delete_state_machine(stateMachineArn=arn)
            iam.delete_role(RoleName=name)


class TestStepFunctionsExecutionHistory:
    def test_get_execution_history(self):
        """Get execution history for a completed execution."""
        sfn = make_client("stepfunctions")
        iam = make_client("iam")

        name = f"hist-sm-{uuid.uuid4().hex[:8]}"
        role = iam.create_role(
            RoleName=name,
            AssumeRolePolicyDocument=json.dumps({
                "Version": "2012-10-17",
                "Statement": [{
                    "Effect": "Allow",
                    "Principal": {"Service": "states.amazonaws.com"},
                    "Action": "sts:AssumeRole",
                }],
            }),
        )
        sm = sfn.create_state_machine(
            name=name,
            definition=json.dumps({
                "StartAt": "P", "States": {"P": {"Type": "Pass", "End": True}}
            }),
            roleArn=role["Role"]["Arn"],
        )
        arn = sm["stateMachineArn"]

        try:
            exec_resp = sfn.start_execution(stateMachineArn=arn)
            history = sfn.get_execution_history(
                executionArn=exec_resp["executionArn"]
            )
            assert "events" in history
            assert len(history["events"]) >= 1
            event_types = [e["type"] for e in history["events"]]
            assert "ExecutionStarted" in event_types
        finally:
            sfn.delete_state_machine(stateMachineArn=arn)
            iam.delete_role(RoleName=name)
