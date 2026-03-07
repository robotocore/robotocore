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


def _unique(prefix: str) -> str:
    return f"{prefix}-{uuid.uuid4().hex[:8]}"


SFN_TRUST_POLICY = json.dumps(
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
)


class TestStepFunctionsExtended:
    """Extended Step Functions CRUD tests."""

    @pytest.fixture
    def role_arn(self):
        iam = make_client("iam")
        name = _unique("sfn-ext-role")
        iam.create_role(
            RoleName=name,
            AssumeRolePolicyDocument=SFN_TRUST_POLICY,
        )
        yield f"arn:aws:iam::123456789012:role/{name}"
        iam.delete_role(RoleName=name)

    def test_create_state_machine_and_tag(self, sfn, role_arn):
        """Create state machine, tag it, and verify tags."""
        name = _unique("tag-sm")
        definition = json.dumps(
            {"StartAt": "P", "States": {"P": {"Type": "Pass", "End": True}}}
        )
        resp = sfn.create_state_machine(
            name=name,
            definition=definition,
            roleArn=role_arn,
        )
        sm_arn = resp["stateMachineArn"]
        try:
            sfn.tag_resource(
                resourceArn=sm_arn,
                tags=[
                    {"key": "env", "value": "test"},
                    {"key": "team", "value": "platform"},
                ],
            )
            tag_resp = sfn.list_tags_for_resource(resourceArn=sm_arn)
            tag_keys = {t["key"] for t in tag_resp["tags"]}
            assert "env" in tag_keys
            assert "team" in tag_keys
        finally:
            sfn.delete_state_machine(stateMachineArn=sm_arn)

    def test_tag_untag_state_machine(self, sfn, role_arn):
        """Tag and untag a state machine after creation."""
        name = _unique("tg-sm")
        definition = json.dumps(
            {"StartAt": "P", "States": {"P": {"Type": "Pass", "End": True}}}
        )
        resp = sfn.create_state_machine(
            name=name, definition=definition, roleArn=role_arn
        )
        sm_arn = resp["stateMachineArn"]
        try:
            sfn.tag_resource(
                resourceArn=sm_arn,
                tags=[
                    {"key": "project", "value": "robotocore"},
                    {"key": "version", "value": "1"},
                ],
            )
            tag_resp = sfn.list_tags_for_resource(resourceArn=sm_arn)
            tag_keys = {t["key"] for t in tag_resp["tags"]}
            assert "project" in tag_keys
            assert "version" in tag_keys

            sfn.untag_resource(resourceArn=sm_arn, tagKeys=["version"])
            tag_resp = sfn.list_tags_for_resource(resourceArn=sm_arn)
            tag_keys = {t["key"] for t in tag_resp["tags"]}
            assert "project" in tag_keys
            assert "version" not in tag_keys
        finally:
            sfn.delete_state_machine(stateMachineArn=sm_arn)

    def test_describe_execution_fields(self, sfn, role_arn):
        """Verify describe_execution returns expected fields."""
        name = _unique("desc-sm")
        definition = json.dumps(
            {"StartAt": "P", "States": {"P": {"Type": "Pass", "Result": {"ok": True}, "End": True}}}
        )
        resp = sfn.create_state_machine(
            name=name, definition=definition, roleArn=role_arn
        )
        sm_arn = resp["stateMachineArn"]
        try:
            exec_resp = sfn.start_execution(
                stateMachineArn=sm_arn, input=json.dumps({"x": 1})
            )
            desc = sfn.describe_execution(executionArn=exec_resp["executionArn"])
            assert "executionArn" in desc
            assert "stateMachineArn" in desc
            assert desc["stateMachineArn"] == sm_arn
            assert "status" in desc
            assert "startDate" in desc
            assert "input" in desc
        finally:
            sfn.delete_state_machine(stateMachineArn=sm_arn)

    def test_get_execution_history(self, sfn, role_arn):
        """Get execution history for a completed execution."""
        name = _unique("hist-sm")
        definition = json.dumps(
            {"StartAt": "P", "States": {"P": {"Type": "Pass", "Result": {"done": True}, "End": True}}}
        )
        resp = sfn.create_state_machine(
            name=name, definition=definition, roleArn=role_arn
        )
        sm_arn = resp["stateMachineArn"]
        try:
            exec_resp = sfn.start_execution(stateMachineArn=sm_arn)
            hist = sfn.get_execution_history(executionArn=exec_resp["executionArn"])
            assert "events" in hist
            assert len(hist["events"]) >= 1
            # First event should be ExecutionStarted
            event_types = [e["type"] for e in hist["events"]]
            assert "ExecutionStarted" in event_types
        finally:
            sfn.delete_state_machine(stateMachineArn=sm_arn)

    def test_stop_execution(self, sfn, role_arn):
        """Stop a running execution (use a Wait state to keep it running)."""
        name = _unique("stop-sm")
        definition = json.dumps(
            {
                "StartAt": "WaitLong",
                "States": {
                    "WaitLong": {
                        "Type": "Wait",
                        "Seconds": 300,
                        "Next": "Done",
                    },
                    "Done": {"Type": "Pass", "End": True},
                },
            }
        )
        resp = sfn.create_state_machine(
            name=name, definition=definition, roleArn=role_arn
        )
        sm_arn = resp["stateMachineArn"]
        try:
            exec_resp = sfn.start_execution(stateMachineArn=sm_arn)
            exec_arn = exec_resp["executionArn"]
            sfn.stop_execution(executionArn=exec_arn)
            desc = sfn.describe_execution(executionArn=exec_arn)
            assert desc["status"] == "ABORTED"
        finally:
            sfn.delete_state_machine(stateMachineArn=sm_arn)

    def test_update_state_machine_definition(self, sfn, role_arn):
        """Update state machine definition and verify."""
        name = _unique("upd-sm")
        definition = json.dumps(
            {"StartAt": "A", "States": {"A": {"Type": "Pass", "End": True}}}
        )
        resp = sfn.create_state_machine(
            name=name, definition=definition, roleArn=role_arn
        )
        sm_arn = resp["stateMachineArn"]
        try:
            new_def = json.dumps(
                {"StartAt": "B", "States": {"B": {"Type": "Pass", "Result": {"v": 2}, "End": True}}}
            )
            upd = sfn.update_state_machine(stateMachineArn=sm_arn, definition=new_def)
            assert "updateDate" in upd

            desc = sfn.describe_state_machine(stateMachineArn=sm_arn)
            parsed = json.loads(desc["definition"])
            assert "B" in parsed["States"]
        finally:
            sfn.delete_state_machine(stateMachineArn=sm_arn)

    def test_list_executions_status_filter(self, sfn, role_arn):
        """List executions filtered by SUCCEEDED status."""
        name = _unique("lef-sm")
        definition = json.dumps(
            {"StartAt": "P", "States": {"P": {"Type": "Pass", "End": True}}}
        )
        resp = sfn.create_state_machine(
            name=name, definition=definition, roleArn=role_arn
        )
        sm_arn = resp["stateMachineArn"]
        try:
            sfn.start_execution(stateMachineArn=sm_arn)
            sfn.start_execution(stateMachineArn=sm_arn)
            list_resp = sfn.list_executions(
                stateMachineArn=sm_arn, statusFilter="SUCCEEDED"
            )
            for exc in list_resp["executions"]:
                assert exc["status"] == "SUCCEEDED"
        finally:
            sfn.delete_state_machine(stateMachineArn=sm_arn)

    def test_start_execution_with_name(self, sfn, role_arn):
        """Start execution with a custom name."""
        sm_name = _unique("named-sm")
        definition = json.dumps(
            {"StartAt": "P", "States": {"P": {"Type": "Pass", "End": True}}}
        )
        resp = sfn.create_state_machine(
            name=sm_name, definition=definition, roleArn=role_arn
        )
        sm_arn = resp["stateMachineArn"]
        try:
            exec_name = _unique("my-exec")
            exec_resp = sfn.start_execution(
                stateMachineArn=sm_arn, name=exec_name
            )
            desc = sfn.describe_execution(executionArn=exec_resp["executionArn"])
            assert desc["name"] == exec_name
        finally:
            sfn.delete_state_machine(stateMachineArn=sm_arn)

    def test_list_state_machines_multiple(self, sfn, role_arn):
        """List state machines includes all created machines."""
        sm_arns = []
        names = []
        definition = json.dumps(
            {"StartAt": "P", "States": {"P": {"Type": "Pass", "End": True}}}
        )
        for _ in range(3):
            name = _unique("multi-sm")
            names.append(name)
            resp = sfn.create_state_machine(
                name=name, definition=definition, roleArn=role_arn
            )
            sm_arns.append(resp["stateMachineArn"])
        try:
            resp = sfn.list_state_machines()
            listed_names = {m["name"] for m in resp["stateMachines"]}
            for n in names:
                assert n in listed_names
        finally:
            for arn in sm_arns:
                sfn.delete_state_machine(stateMachineArn=arn)


class TestASLExecutionExtended:
    """Additional ASL execution tests."""

    @pytest.fixture
    def role_arn(self):
        iam = make_client("iam")
        name = _unique("sfn-asl-role")
        iam.create_role(
            RoleName=name,
            AssumeRolePolicyDocument=SFN_TRUST_POLICY,
        )
        yield f"arn:aws:iam::123456789012:role/{name}"
        iam.delete_role(RoleName=name)

    def _create_and_execute(self, sfn, role_arn, definition, input_data=None):
        name = _unique("asl-sm")
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

    def test_wait_state_seconds(self, role_arn):
        """Wait state with short Seconds value."""
        sfn = make_client("stepfunctions")
        result = self._create_and_execute(
            sfn,
            role_arn,
            {
                "StartAt": "ShortWait",
                "States": {
                    "ShortWait": {
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

    def test_choice_string_equals(self, role_arn):
        """Choice state with StringEquals comparison."""
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
                                "Variable": "$.color",
                                "StringEquals": "red",
                                "Next": "Red",
                            },
                            {
                                "Variable": "$.color",
                                "StringEquals": "blue",
                                "Next": "Blue",
                            },
                        ],
                        "Default": "Other",
                    },
                    "Red": {"Type": "Pass", "Result": {"picked": "red"}, "End": True},
                    "Blue": {"Type": "Pass", "Result": {"picked": "blue"}, "End": True},
                    "Other": {"Type": "Pass", "Result": {"picked": "other"}, "End": True},
                },
            },
            {"color": "blue"},
        )
        assert result["status"] == "SUCCEEDED"
        output = json.loads(result["output"])
        assert output["picked"] == "blue"

    def test_parallel_branches(self, role_arn):
        """Parallel state with three branches."""
        sfn = make_client("stepfunctions")
        result = self._create_and_execute(
            sfn,
            role_arn,
            {
                "StartAt": "Fan",
                "States": {
                    "Fan": {
                        "Type": "Parallel",
                        "Branches": [
                            {
                                "StartAt": "A",
                                "States": {"A": {"Type": "Pass", "Result": {"b": "a"}, "End": True}},
                            },
                            {
                                "StartAt": "B",
                                "States": {"B": {"Type": "Pass", "Result": {"b": "b"}, "End": True}},
                            },
                            {
                                "StartAt": "C",
                                "States": {"C": {"Type": "Pass", "Result": {"b": "c"}, "End": True}},
                            },
                        ],
                        "End": True,
                    }
                },
            },
        )
        assert result["status"] == "SUCCEEDED"
        output = json.loads(result["output"])
        assert len(output) == 3
        branches = [o["b"] for o in output]
        assert "a" in branches
        assert "b" in branches
        assert "c" in branches

    def test_map_state(self, role_arn):
        """Map state iterates over an array."""
        sfn = make_client("stepfunctions")
        result = self._create_and_execute(
            sfn,
            role_arn,
            {
                "StartAt": "MapIt",
                "States": {
                    "MapIt": {
                        "Type": "Map",
                        "ItemsPath": "$.items",
                        "Iterator": {
                            "StartAt": "Process",
                            "States": {
                                "Process": {
                                    "Type": "Pass",
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

    def test_result_path_null(self, role_arn):
        """ResultPath null discards the result, keeping original input."""
        sfn = make_client("stepfunctions")
        result = self._create_and_execute(
            sfn,
            role_arn,
            {
                "StartAt": "Discard",
                "States": {
                    "Discard": {
                        "Type": "Pass",
                        "Result": {"throwaway": True},
                        "ResultPath": None,
                        "End": True,
                    }
                },
            },
            {"keep": "me"},
        )
        assert result["status"] == "SUCCEEDED"
        output = json.loads(result["output"])
        assert output["keep"] == "me"
        assert "throwaway" not in output

    def test_input_path_output_path(self, role_arn):
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
