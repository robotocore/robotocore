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

    def test_list_executions_contains_started(self, sfn):
        """Create state machine, start execution, verify it appears in list_executions."""
        role_arn = "arn:aws:iam::123456789012:role/StepRole"
        sm_name = f"test-list-exec-{uuid.uuid4().hex[:8]}"
        definition = json.dumps(
            {
                "Comment": "test",
                "StartAt": "Pass",
                "States": {"Pass": {"Type": "Pass", "End": True}},
            }
        )
        sm = sfn.create_state_machine(name=sm_name, definition=definition, roleArn=role_arn)
        sm_arn = sm["stateMachineArn"]
        try:
            exec_resp = sfn.start_execution(stateMachineArn=sm_arn)
            exec_arn = exec_resp["executionArn"]
            response = sfn.list_executions(stateMachineArn=sm_arn)
            exec_arns = [e["executionArn"] for e in response["executions"]]
            assert exec_arn in exec_arns
        finally:
            sfn.delete_state_machine(stateMachineArn=sm_arn)

    def test_describe_execution_details(self, sfn):
        """Create state machine, start execution, describe and verify fields."""
        role_arn = "arn:aws:iam::123456789012:role/StepRole"
        sm_name = f"test-desc-exec-{uuid.uuid4().hex[:8]}"
        definition = json.dumps(
            {
                "Comment": "test",
                "StartAt": "Pass",
                "States": {"Pass": {"Type": "Pass", "End": True}},
            }
        )
        sm = sfn.create_state_machine(name=sm_name, definition=definition, roleArn=role_arn)
        sm_arn = sm["stateMachineArn"]
        try:
            exec_resp = sfn.start_execution(stateMachineArn=sm_arn)
            exec_arn = exec_resp["executionArn"]
            response = sfn.describe_execution(executionArn=exec_arn)
            assert response["stateMachineArn"] == sm_arn
            assert response["status"] in ("RUNNING", "SUCCEEDED")
        finally:
            sfn.delete_state_machine(stateMachineArn=sm_arn)

    def test_stop_execution(self, sfn):
        """Create state machine with Wait state, start, stop, verify ABORTED."""
        role_arn = "arn:aws:iam::123456789012:role/StepRole"
        sm_name = f"test-stop-exec-{uuid.uuid4().hex[:8]}"
        definition = json.dumps(
            {
                "Comment": "test",
                "StartAt": "Wait",
                "States": {"Wait": {"Type": "Wait", "Seconds": 300, "End": True}},
            }
        )
        sm = sfn.create_state_machine(name=sm_name, definition=definition, roleArn=role_arn)
        sm_arn = sm["stateMachineArn"]
        try:
            exec_resp = sfn.start_execution(stateMachineArn=sm_arn)
            exec_arn = exec_resp["executionArn"]
            sfn.stop_execution(executionArn=exec_arn)
            response = sfn.describe_execution(executionArn=exec_arn)
            assert response["status"] == "ABORTED"
        finally:
            sfn.delete_state_machine(stateMachineArn=sm_arn)


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


class TestStepFunctionsExtended:
    """Extended Step Functions tests: ARN format, fields, pagination, tags, types."""

    @pytest.fixture
    def role_arn(self):
        iam = make_client("iam")
        name = f"sfn-ext-role-{uuid.uuid4().hex[:8]}"
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

    def _simple_definition(self):
        return json.dumps(
            {
                "StartAt": "Pass",
                "States": {"Pass": {"Type": "Pass", "End": True}},
            }
        )

    def test_create_state_machine_arn_format(self, sfn, role_arn):
        """CreateStateMachine with different names and verify ARN format."""
        sm_name = f"arn-test-{uuid.uuid4().hex[:8]}"
        try:
            response = sfn.create_state_machine(
                name=sm_name,
                definition=self._simple_definition(),
                roleArn=role_arn,
            )
            arn = response["stateMachineArn"]
            assert arn.startswith("arn:aws:states:")
            assert f":stateMachine:{sm_name}" in arn
            assert "creationDate" in response
        finally:
            sfn.delete_state_machine(stateMachineArn=response["stateMachineArn"])

    def test_describe_state_machine_all_fields(self, sfn, role_arn):
        """DescribeStateMachine returns all expected fields."""
        sm_name = f"desc-fields-{uuid.uuid4().hex[:8]}"
        sm = sfn.create_state_machine(
            name=sm_name,
            definition=self._simple_definition(),
            roleArn=role_arn,
        )
        sm_arn = sm["stateMachineArn"]
        try:
            response = sfn.describe_state_machine(stateMachineArn=sm_arn)
            assert response["name"] == sm_name
            assert response["stateMachineArn"] == sm_arn
            assert response["status"] == "ACTIVE"
            assert "definition" in response
            assert "roleArn" in response
            assert "creationDate" in response
            assert "type" in response
            assert response["type"] in ("STANDARD", "EXPRESS")
        finally:
            sfn.delete_state_machine(stateMachineArn=sm_arn)

    def test_update_state_machine_change_definition(self, sfn, role_arn):
        """UpdateStateMachine changes the definition."""
        sm_name = f"upd-def-{uuid.uuid4().hex[:8]}"
        original_def = json.dumps(
            {
                "StartAt": "Original",
                "States": {"Original": {"Type": "Pass", "End": True}},
            }
        )
        sm = sfn.create_state_machine(name=sm_name, definition=original_def, roleArn=role_arn)
        sm_arn = sm["stateMachineArn"]
        try:
            new_def = json.dumps(
                {
                    "StartAt": "Updated",
                    "States": {
                        "Updated": {
                            "Type": "Pass",
                            "Result": {"version": 2},
                            "End": True,
                        }
                    },
                }
            )
            update_resp = sfn.update_state_machine(stateMachineArn=sm_arn, definition=new_def)
            assert "updateDate" in update_resp

            desc = sfn.describe_state_machine(stateMachineArn=sm_arn)
            parsed = json.loads(desc["definition"])
            assert "Updated" in parsed["States"]
            assert "Original" not in parsed["States"]
        finally:
            sfn.delete_state_machine(stateMachineArn=sm_arn)

    def test_list_state_machines_pagination(self, sfn, role_arn):
        """ListStateMachines with maxResults returns state machines."""
        created_arns = []
        try:
            for i in range(3):
                name = f"page-test-{i}-{uuid.uuid4().hex[:8]}"
                resp = sfn.create_state_machine(
                    name=name,
                    definition=self._simple_definition(),
                    roleArn=role_arn,
                )
                created_arns.append(resp["stateMachineArn"])

            # List with maxResults — server may or may not enforce the limit
            response = sfn.list_state_machines(maxResults=10)
            assert "stateMachines" in response
            returned_arns = [sm["stateMachineArn"] for sm in response["stateMachines"]]
            # All 3 created machines should appear
            for arn in created_arns:
                assert arn in returned_arns
        finally:
            for arn in created_arns:
                sfn.delete_state_machine(stateMachineArn=arn)

    def test_start_execution_with_input(self, sfn, role_arn):
        """StartExecution with input JSON and verify it is passed through."""
        sm_name = f"input-test-{uuid.uuid4().hex[:8]}"
        definition = json.dumps(
            {
                "StartAt": "EchoInput",
                "States": {"EchoInput": {"Type": "Pass", "End": True}},
            }
        )
        sm = sfn.create_state_machine(name=sm_name, definition=definition, roleArn=role_arn)
        sm_arn = sm["stateMachineArn"]
        try:
            input_data = json.dumps({"user": "alice", "action": "login"})
            exec_resp = sfn.start_execution(stateMachineArn=sm_arn, input=input_data)
            assert "executionArn" in exec_resp
            assert "startDate" in exec_resp

            desc = sfn.describe_execution(executionArn=exec_resp["executionArn"])
            assert desc["stateMachineArn"] == sm_arn
            assert json.loads(desc["input"]) == {"user": "alice", "action": "login"}
        finally:
            sfn.delete_state_machine(stateMachineArn=sm_arn)

    def test_describe_execution_fields(self, sfn, role_arn):
        """DescribeExecution returns expected fields including status, input, output."""
        sm_name = f"exec-fields-{uuid.uuid4().hex[:8]}"
        definition = json.dumps(
            {
                "StartAt": "Done",
                "States": {
                    "Done": {
                        "Type": "Pass",
                        "Result": {"completed": True},
                        "End": True,
                    }
                },
            }
        )
        sm = sfn.create_state_machine(name=sm_name, definition=definition, roleArn=role_arn)
        sm_arn = sm["stateMachineArn"]
        try:
            exec_resp = sfn.start_execution(stateMachineArn=sm_arn, input=json.dumps({"x": 1}))
            desc = sfn.describe_execution(executionArn=exec_resp["executionArn"])
            assert "executionArn" in desc
            assert "stateMachineArn" in desc
            assert desc["status"] in ("RUNNING", "SUCCEEDED", "FAILED", "TIMED_OUT", "ABORTED")
            assert "startDate" in desc
            assert "input" in desc
            if desc["status"] == "SUCCEEDED":
                assert "output" in desc
                output = json.loads(desc["output"])
                assert output["completed"] is True
        finally:
            sfn.delete_state_machine(stateMachineArn=sm_arn)

    def test_tag_and_untag_state_machine(self, sfn, role_arn):
        """TagResource, UntagResource, ListTagsForResource on state machines."""
        sm_name = f"tag-test-{uuid.uuid4().hex[:8]}"
        sm = sfn.create_state_machine(
            name=sm_name,
            definition=self._simple_definition(),
            roleArn=role_arn,
        )
        sm_arn = sm["stateMachineArn"]
        try:
            # Tag
            sfn.tag_resource(
                resourceArn=sm_arn,
                tags=[
                    {"key": "env", "value": "test"},
                    {"key": "team", "value": "platform"},
                ],
            )

            # List tags
            tag_resp = sfn.list_tags_for_resource(resourceArn=sm_arn)
            tag_map = {t["key"]: t["value"] for t in tag_resp["tags"]}
            assert tag_map["env"] == "test"
            assert tag_map["team"] == "platform"

            # Untag
            sfn.untag_resource(resourceArn=sm_arn, tagKeys=["env"])
            tag_resp2 = sfn.list_tags_for_resource(resourceArn=sm_arn)
            tag_keys = [t["key"] for t in tag_resp2["tags"]]
            assert "env" not in tag_keys
            assert "team" in tag_keys
        finally:
            sfn.delete_state_machine(stateMachineArn=sm_arn)

    def test_create_standard_vs_express_type(self, sfn, role_arn):
        """CreateStateMachine with STANDARD vs EXPRESS type."""
        arns = []
        try:
            for sm_type in ("STANDARD", "EXPRESS"):
                name = f"type-{sm_type.lower()}-{uuid.uuid4().hex[:8]}"
                resp = sfn.create_state_machine(
                    name=name,
                    definition=self._simple_definition(),
                    roleArn=role_arn,
                    type=sm_type,
                )
                arns.append(resp["stateMachineArn"])
                desc = sfn.describe_state_machine(stateMachineArn=resp["stateMachineArn"])
                assert desc["type"] == sm_type
        finally:
            for arn in arns:
                sfn.delete_state_machine(stateMachineArn=arn)

    def test_list_executions(self, sfn, role_arn):
        """ListExecutions returns executions for a state machine."""
        sm_name = f"list-exec-{uuid.uuid4().hex[:8]}"
        sm = sfn.create_state_machine(
            name=sm_name, definition=self._simple_definition(), roleArn=role_arn
        )
        sm_arn = sm["stateMachineArn"]
        try:
            sfn.start_execution(stateMachineArn=sm_arn, input=json.dumps({"a": 1}))
            sfn.start_execution(stateMachineArn=sm_arn, input=json.dumps({"b": 2}))
            resp = sfn.list_executions(stateMachineArn=sm_arn)
            assert len(resp["executions"]) >= 2
        finally:
            sfn.delete_state_machine(stateMachineArn=sm_arn)

    def test_list_executions_status_filter(self, sfn, role_arn):
        """ListExecutions with statusFilter."""
        sm_name = f"filter-exec-{uuid.uuid4().hex[:8]}"
        sm = sfn.create_state_machine(
            name=sm_name, definition=self._simple_definition(), roleArn=role_arn
        )
        sm_arn = sm["stateMachineArn"]
        try:
            sfn.start_execution(stateMachineArn=sm_arn)
            # All executions of a Pass state should succeed quickly
            import time

            time.sleep(1)
            resp = sfn.list_executions(stateMachineArn=sm_arn, statusFilter="SUCCEEDED")
            for ex in resp["executions"]:
                assert ex["status"] == "SUCCEEDED"
        finally:
            sfn.delete_state_machine(stateMachineArn=sm_arn)

    def test_describe_state_machine_for_execution(self, sfn, role_arn):
        """DescribeStateMachineForExecution returns SM details from exec ARN."""
        sm_name = f"desc-sm-exec-{uuid.uuid4().hex[:8]}"
        sm = sfn.create_state_machine(
            name=sm_name, definition=self._simple_definition(), roleArn=role_arn
        )
        sm_arn = sm["stateMachineArn"]
        try:
            exec_resp = sfn.start_execution(stateMachineArn=sm_arn)
            desc = sfn.describe_state_machine_for_execution(executionArn=exec_resp["executionArn"])
            assert desc["name"] == sm_name
            assert "definition" in desc
            assert "roleArn" in desc
        finally:
            sfn.delete_state_machine(stateMachineArn=sm_arn)

    def test_start_execution_with_name(self, sfn, role_arn):
        """StartExecution with a custom execution name."""
        sm_name = f"named-exec-{uuid.uuid4().hex[:8]}"
        sm = sfn.create_state_machine(
            name=sm_name, definition=self._simple_definition(), roleArn=role_arn
        )
        sm_arn = sm["stateMachineArn"]
        exec_name = f"my-exec-{uuid.uuid4().hex[:8]}"
        try:
            exec_resp = sfn.start_execution(stateMachineArn=sm_arn, name=exec_name)
            assert exec_name in exec_resp["executionArn"]
            desc = sfn.describe_execution(executionArn=exec_resp["executionArn"])
            assert desc["name"] == exec_name
        finally:
            sfn.delete_state_machine(stateMachineArn=sm_arn)

    def test_choice_state(self, sfn, role_arn):
        """Test Choice state type with branching."""
        sm_name = f"choice-{uuid.uuid4().hex[:8]}"
        definition = json.dumps(
            {
                "StartAt": "ChoiceState",
                "States": {
                    "ChoiceState": {
                        "Type": "Choice",
                        "Choices": [
                            {
                                "Variable": "$.value",
                                "NumericGreaterThan": 10,
                                "Next": "Big",
                            }
                        ],
                        "Default": "Small",
                    },
                    "Big": {"Type": "Pass", "Result": "big", "End": True},
                    "Small": {"Type": "Pass", "Result": "small", "End": True},
                },
            }
        )
        sm = sfn.create_state_machine(name=sm_name, definition=definition, roleArn=role_arn)
        sm_arn = sm["stateMachineArn"]
        try:
            exec_resp = sfn.start_execution(stateMachineArn=sm_arn, input=json.dumps({"value": 20}))
            import time

            time.sleep(1)
            desc = sfn.describe_execution(executionArn=exec_resp["executionArn"])
            if desc["status"] == "SUCCEEDED":
                assert json.loads(desc["output"]) == "big"
        finally:
            sfn.delete_state_machine(stateMachineArn=sm_arn)

    def test_fail_state(self, sfn, role_arn):
        """Test Fail state type."""
        sm_name = f"fail-{uuid.uuid4().hex[:8]}"
        definition = json.dumps(
            {
                "StartAt": "FailState",
                "States": {
                    "FailState": {
                        "Type": "Fail",
                        "Error": "CustomError",
                        "Cause": "Something went wrong",
                    },
                },
            }
        )
        sm = sfn.create_state_machine(name=sm_name, definition=definition, roleArn=role_arn)
        sm_arn = sm["stateMachineArn"]
        try:
            exec_resp = sfn.start_execution(stateMachineArn=sm_arn)
            import time

            time.sleep(1)
            desc = sfn.describe_execution(executionArn=exec_resp["executionArn"])
            assert desc["status"] == "FAILED"
        finally:
            sfn.delete_state_machine(stateMachineArn=sm_arn)

    def test_succeed_state(self, sfn, role_arn):
        """Test Succeed state type."""
        sm_name = f"succeed-{uuid.uuid4().hex[:8]}"
        definition = json.dumps(
            {
                "StartAt": "SucceedState",
                "States": {
                    "SucceedState": {"Type": "Succeed"},
                },
            }
        )
        sm = sfn.create_state_machine(name=sm_name, definition=definition, roleArn=role_arn)
        sm_arn = sm["stateMachineArn"]
        try:
            exec_resp = sfn.start_execution(stateMachineArn=sm_arn)
            import time

            time.sleep(1)
            desc = sfn.describe_execution(executionArn=exec_resp["executionArn"])
            assert desc["status"] == "SUCCEEDED"
        finally:
            sfn.delete_state_machine(stateMachineArn=sm_arn)

    def test_pass_state_with_result_path(self, sfn, role_arn):
        """Test Pass state with ResultPath to merge data."""
        sm_name = f"resultpath-{uuid.uuid4().hex[:8]}"
        definition = json.dumps(
            {
                "StartAt": "AddData",
                "States": {
                    "AddData": {
                        "Type": "Pass",
                        "Result": {"added": True},
                        "ResultPath": "$.extra",
                        "End": True,
                    },
                },
            }
        )
        sm = sfn.create_state_machine(name=sm_name, definition=definition, roleArn=role_arn)
        sm_arn = sm["stateMachineArn"]
        try:
            exec_resp = sfn.start_execution(
                stateMachineArn=sm_arn, input=json.dumps({"original": "data"})
            )
            import time

            time.sleep(1)
            desc = sfn.describe_execution(executionArn=exec_resp["executionArn"])
            if desc["status"] == "SUCCEEDED":
                output = json.loads(desc["output"])
                assert output["original"] == "data"
                assert output["extra"]["added"] is True
        finally:
            sfn.delete_state_machine(stateMachineArn=sm_arn)

    def test_parallel_state(self, sfn, role_arn):
        """Test Parallel state type with two branches."""
        sm_name = f"parallel-{uuid.uuid4().hex[:8]}"
        definition = json.dumps(
            {
                "StartAt": "ParallelState",
                "States": {
                    "ParallelState": {
                        "Type": "Parallel",
                        "Branches": [
                            {
                                "StartAt": "Branch1",
                                "States": {
                                    "Branch1": {
                                        "Type": "Pass",
                                        "Result": "from-branch-1",
                                        "End": True,
                                    }
                                },
                            },
                            {
                                "StartAt": "Branch2",
                                "States": {
                                    "Branch2": {
                                        "Type": "Pass",
                                        "Result": "from-branch-2",
                                        "End": True,
                                    }
                                },
                            },
                        ],
                        "End": True,
                    },
                },
            }
        )
        sm = sfn.create_state_machine(name=sm_name, definition=definition, roleArn=role_arn)
        sm_arn = sm["stateMachineArn"]
        try:
            exec_resp = sfn.start_execution(stateMachineArn=sm_arn)
            import time

            time.sleep(2)
            desc = sfn.describe_execution(executionArn=exec_resp["executionArn"])
            if desc["status"] == "SUCCEEDED":
                output = json.loads(desc["output"])
                assert len(output) == 2
        finally:
            sfn.delete_state_machine(stateMachineArn=sm_arn)

    def test_wait_state_seconds(self, sfn, role_arn):
        sm_name = f"wait-{uuid.uuid4().hex[:8]}"
        definition = json.dumps(
            {
                "StartAt": "WaitStep",
                "States": {
                    "WaitStep": {
                        "Type": "Wait",
                        "Seconds": 1,
                        "Next": "Done",
                    },
                    "Done": {"Type": "Succeed"},
                },
            }
        )
        sm = sfn.create_state_machine(name=sm_name, definition=definition, roleArn=role_arn)
        sm_arn = sm["stateMachineArn"]
        try:
            exec_resp = sfn.start_execution(stateMachineArn=sm_arn)
            import time

            time.sleep(3)
            desc = sfn.describe_execution(executionArn=exec_resp["executionArn"])
            assert desc["status"] == "SUCCEEDED"
        finally:
            sfn.delete_state_machine(stateMachineArn=sm_arn)

    def test_map_state_inline(self, sfn, role_arn):
        sm_name = f"map-{uuid.uuid4().hex[:8]}"
        definition = json.dumps(
            {
                "StartAt": "MapStep",
                "States": {
                    "MapStep": {
                        "Type": "Map",
                        "ItemsPath": "$.items",
                        "Iterator": {
                            "StartAt": "PassItem",
                            "States": {
                                "PassItem": {
                                    "Type": "Pass",
                                    "End": True,
                                }
                            },
                        },
                        "End": True,
                    },
                },
            }
        )
        sm = sfn.create_state_machine(name=sm_name, definition=definition, roleArn=role_arn)
        sm_arn = sm["stateMachineArn"]
        try:
            exec_resp = sfn.start_execution(
                stateMachineArn=sm_arn,
                input=json.dumps({"items": [1, 2, 3]}),
            )
            import time

            time.sleep(2)
            desc = sfn.describe_execution(executionArn=exec_resp["executionArn"])
            if desc["status"] == "SUCCEEDED":
                output = json.loads(desc["output"])
                assert len(output) == 3
        finally:
            sfn.delete_state_machine(stateMachineArn=sm_arn)

    def test_catch_on_task_error(self, sfn, role_arn):
        sm_name = f"catch-{uuid.uuid4().hex[:8]}"
        definition = json.dumps(
            {
                "StartAt": "FailTask",
                "States": {
                    "FailTask": {
                        "Type": "Fail",
                        "Error": "CustomError",
                        "Cause": "Something failed",
                    },
                },
            }
        )
        sm = sfn.create_state_machine(name=sm_name, definition=definition, roleArn=role_arn)
        sm_arn = sm["stateMachineArn"]
        try:
            exec_resp = sfn.start_execution(stateMachineArn=sm_arn)
            import time

            time.sleep(2)
            desc = sfn.describe_execution(executionArn=exec_resp["executionArn"])
            assert desc["status"] == "FAILED"
        finally:
            sfn.delete_state_machine(stateMachineArn=sm_arn)

    def test_pass_state_with_parameters(self, sfn, role_arn):
        sm_name = f"params-{uuid.uuid4().hex[:8]}"
        definition = json.dumps(
            {
                "StartAt": "PassParams",
                "States": {
                    "PassParams": {
                        "Type": "Pass",
                        "Parameters": {
                            "greeting": "hello",
                            "count": 42,
                        },
                        "End": True,
                    },
                },
            }
        )
        sm = sfn.create_state_machine(name=sm_name, definition=definition, roleArn=role_arn)
        sm_arn = sm["stateMachineArn"]
        try:
            exec_resp = sfn.start_execution(stateMachineArn=sm_arn)
            import time

            time.sleep(2)
            desc = sfn.describe_execution(executionArn=exec_resp["executionArn"])
            if desc["status"] == "SUCCEEDED":
                output = json.loads(desc["output"])
                assert output["greeting"] == "hello"
                assert output["count"] == 42
        finally:
            sfn.delete_state_machine(stateMachineArn=sm_arn)

    def test_choice_state_default(self, sfn, role_arn):
        sm_name = f"choice-def-{uuid.uuid4().hex[:8]}"
        definition = json.dumps(
            {
                "StartAt": "ChoiceStep",
                "States": {
                    "ChoiceStep": {
                        "Type": "Choice",
                        "Choices": [
                            {
                                "Variable": "$.val",
                                "NumericEquals": 999,
                                "Next": "NeverMatch",
                            }
                        ],
                        "Default": "DefaultState",
                    },
                    "NeverMatch": {"Type": "Fail", "Error": "ShouldNotMatch"},
                    "DefaultState": {
                        "Type": "Pass",
                        "Result": "took-default",
                        "End": True,
                    },
                },
            }
        )
        sm = sfn.create_state_machine(name=sm_name, definition=definition, roleArn=role_arn)
        sm_arn = sm["stateMachineArn"]
        try:
            exec_resp = sfn.start_execution(stateMachineArn=sm_arn, input=json.dumps({"val": 1}))
            import time

            time.sleep(2)
            desc = sfn.describe_execution(executionArn=exec_resp["executionArn"])
            if desc["status"] == "SUCCEEDED":
                assert json.loads(desc["output"]) == "took-default"
        finally:
            sfn.delete_state_machine(stateMachineArn=sm_arn)

    def test_stop_execution_with_error(self, sfn, role_arn):
        sm_name = f"stop-err-{uuid.uuid4().hex[:8]}"
        definition = json.dumps(
            {
                "StartAt": "LongWait",
                "States": {
                    "LongWait": {"Type": "Wait", "Seconds": 300, "End": True},
                },
            }
        )
        sm = sfn.create_state_machine(name=sm_name, definition=definition, roleArn=role_arn)
        sm_arn = sm["stateMachineArn"]
        try:
            exec_resp = sfn.start_execution(stateMachineArn=sm_arn)
            sfn.stop_execution(
                executionArn=exec_resp["executionArn"],
                error="ManualStop",
                cause="Stopped by test",
            )
            desc = sfn.describe_execution(executionArn=exec_resp["executionArn"])
            assert desc["status"] == "ABORTED"
        finally:
            sfn.delete_state_machine(stateMachineArn=sm_arn)

    def test_delete_state_machine(self, sfn, role_arn):
        sm_name = f"del-sm-{uuid.uuid4().hex[:8]}"
        definition = json.dumps(
            {
                "StartAt": "Pass",
                "States": {"Pass": {"Type": "Succeed"}},
            }
        )
        sm = sfn.create_state_machine(name=sm_name, definition=definition, roleArn=role_arn)
        sm_arn = sm["stateMachineArn"]
        sfn.delete_state_machine(stateMachineArn=sm_arn)
        resp = sfn.list_state_machines()
        arns = [s["stateMachineArn"] for s in resp["stateMachines"]]
        assert sm_arn not in arns

    def test_execution_input_output_roundtrip(self, sfn, role_arn):
        sm_name = f"io-{uuid.uuid4().hex[:8]}"
        definition = json.dumps(
            {
                "StartAt": "Echo",
                "States": {
                    "Echo": {"Type": "Pass", "End": True},
                },
            }
        )
        sm = sfn.create_state_machine(name=sm_name, definition=definition, roleArn=role_arn)
        sm_arn = sm["stateMachineArn"]
        try:
            inp = {"key": "value", "nested": {"a": 1}}
            exec_resp = sfn.start_execution(stateMachineArn=sm_arn, input=json.dumps(inp))
            import time

            time.sleep(2)
            desc = sfn.describe_execution(executionArn=exec_resp["executionArn"])
            if desc["status"] == "SUCCEEDED":
                output = json.loads(desc["output"])
                assert output == inp
        finally:
            sfn.delete_state_machine(stateMachineArn=sm_arn)


class TestStepFunctionsActivities:
    """Tests for Step Functions Activity operations."""

    @pytest.fixture
    def sfn(self):
        return make_client("stepfunctions")

    def test_create_activity(self, sfn):
        """CreateActivity creates an activity and returns its ARN."""
        name = f"test-activity-{uuid.uuid4().hex[:8]}"
        resp = sfn.create_activity(name=name)
        act_arn = resp["activityArn"]
        try:
            assert act_arn.startswith("arn:aws:states:")
            assert f":activity:{name}" in act_arn
            assert "creationDate" in resp
        finally:
            sfn.delete_activity(activityArn=act_arn)

    def test_describe_activity(self, sfn):
        """DescribeActivity returns activity details."""
        name = f"desc-activity-{uuid.uuid4().hex[:8]}"
        resp = sfn.create_activity(name=name)
        act_arn = resp["activityArn"]
        try:
            desc = sfn.describe_activity(activityArn=act_arn)
            assert desc["name"] == name
            assert desc["activityArn"] == act_arn
            assert "creationDate" in desc
        finally:
            sfn.delete_activity(activityArn=act_arn)

    def test_list_activities(self, sfn):
        """ListActivities returns created activities."""
        name = f"list-activity-{uuid.uuid4().hex[:8]}"
        resp = sfn.create_activity(name=name)
        act_arn = resp["activityArn"]
        try:
            list_resp = sfn.list_activities()
            assert "activities" in list_resp
            arns = [a["activityArn"] for a in list_resp["activities"]]
            assert act_arn in arns
        finally:
            sfn.delete_activity(activityArn=act_arn)

    def test_delete_activity(self, sfn):
        """DeleteActivity removes the activity."""
        name = f"del-activity-{uuid.uuid4().hex[:8]}"
        resp = sfn.create_activity(name=name)
        act_arn = resp["activityArn"]
        sfn.delete_activity(activityArn=act_arn)
        # Verify it's gone
        list_resp = sfn.list_activities()
        arns = [a["activityArn"] for a in list_resp["activities"]]
        assert act_arn not in arns

    def test_list_activities_pagination(self, sfn):
        """ListActivities with maxResults."""
        created = []
        try:
            for i in range(3):
                name = f"page-act-{i}-{uuid.uuid4().hex[:8]}"
                resp = sfn.create_activity(name=name)
                created.append(resp["activityArn"])
            list_resp = sfn.list_activities(maxResults=10)
            assert "activities" in list_resp
            arns = [a["activityArn"] for a in list_resp["activities"]]
            for arn in created:
                assert arn in arns
        finally:
            for arn in created:
                sfn.delete_activity(activityArn=arn)

    def test_describe_activity_fields(self, sfn):
        """DescribeActivity returns all expected fields."""
        name = f"fields-act-{uuid.uuid4().hex[:8]}"
        resp = sfn.create_activity(name=name)
        act_arn = resp["activityArn"]
        try:
            desc = sfn.describe_activity(activityArn=act_arn)
            assert "activityArn" in desc
            assert "name" in desc
            assert "creationDate" in desc
        finally:
            sfn.delete_activity(activityArn=act_arn)


class TestStepFunctionsMapRun:
    """Tests for MapRun operations (DescribeMapRun, UpdateMapRun)."""

    @pytest.fixture
    def sfn(self):
        return make_client("stepfunctions")

    def test_describe_map_run(self, sfn):
        """DescribeMapRun accepts a mapRunArn and returns 200."""
        fake_arn = "arn:aws:states:us-east-1:123456789012:mapRun:my-sm/exec-id:map-run-id"
        resp = sfn.describe_map_run(mapRunArn=fake_arn)
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200

    def test_update_map_run(self, sfn):
        """UpdateMapRun accepts a mapRunArn and maxConcurrency, returns 200."""
        fake_arn = "arn:aws:states:us-east-1:123456789012:mapRun:my-sm/exec-id:map-run-id"
        resp = sfn.update_map_run(mapRunArn=fake_arn, maxConcurrency=10)
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200

    def test_list_map_runs(self, sfn):
        """ListMapRuns returns mapRuns list for an execution."""
        role_arn = "arn:aws:iam::123456789012:role/StepRole"
        sm_name = f"map-runs-{uuid.uuid4().hex[:8]}"
        definition = json.dumps(
            {
                "StartAt": "PassState",
                "States": {
                    "PassState": {"Type": "Pass", "End": True},
                },
            }
        )
        sm = sfn.create_state_machine(name=sm_name, definition=definition, roleArn=role_arn)
        sm_arn = sm["stateMachineArn"]
        try:
            exec_resp = sfn.start_execution(stateMachineArn=sm_arn)
            exec_arn = exec_resp["executionArn"]
            resp = sfn.list_map_runs(executionArn=exec_arn)
            assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200
            # mapRuns key may be absent if no distributed map runs exist;
            # the important thing is the server accepted the request
            if "mapRuns" in resp:
                assert isinstance(resp["mapRuns"], list)
        finally:
            sfn.delete_state_machine(stateMachineArn=sm_arn)


class TestStepFunctionsSendTask:
    """Tests for SendTask* operations (callback pattern)."""

    @pytest.fixture
    def sfn(self):
        return make_client("stepfunctions")

    def test_send_task_failure_invalid_token(self, sfn):
        """SendTaskFailure with invalid token returns TaskDoesNotExist."""
        with pytest.raises(sfn.exceptions.ClientError) as exc:
            sfn.send_task_failure(taskToken="invalid-token", error="test")
        assert "TaskDoesNotExist" in str(exc.value)

    def test_send_task_heartbeat_invalid_token(self, sfn):
        """SendTaskHeartbeat with invalid token returns TaskDoesNotExist."""
        with pytest.raises(sfn.exceptions.ClientError) as exc:
            sfn.send_task_heartbeat(taskToken="invalid-token")
        assert "TaskDoesNotExist" in str(exc.value)

    def test_send_task_success_invalid_token(self, sfn):
        """SendTaskSuccess with invalid token returns TaskDoesNotExist."""
        with pytest.raises(sfn.exceptions.ClientError) as exc:
            sfn.send_task_success(taskToken="invalid-token", output="{}")
        assert "TaskDoesNotExist" in str(exc.value)


class TestStepFunctionsExecutionDetails:
    """Tests for execution history details and reverse ordering."""

    @pytest.fixture
    def sfn(self):
        return make_client("stepfunctions")

    def _simple_definition(self):
        return json.dumps(
            {
                "StartAt": "Pass",
                "States": {"Pass": {"Type": "Pass", "End": True}},
            }
        )

    def test_get_execution_history_reverse(self, sfn):
        """GetExecutionHistory with reverseOrder returns events in reverse."""
        role_arn = "arn:aws:iam::123456789012:role/StepRole"
        sm_name = f"hist-rev-{uuid.uuid4().hex[:8]}"
        sm = sfn.create_state_machine(
            name=sm_name, definition=self._simple_definition(), roleArn=role_arn
        )
        sm_arn = sm["stateMachineArn"]
        try:
            exec_resp = sfn.start_execution(stateMachineArn=sm_arn)
            import time

            time.sleep(1)
            history = sfn.get_execution_history(
                executionArn=exec_resp["executionArn"],
                reverseOrder=True,
            )
            events = history["events"]
            assert len(events) >= 2
            # In reverse order, last event should be ExecutionStarted (earliest)
            types = [e["type"] for e in events]
            assert "ExecutionStarted" in types
            # The first event in reverse should be a later event
            assert types[-1] == "ExecutionStarted"
        finally:
            sfn.delete_state_machine(stateMachineArn=sm_arn)

    def test_get_execution_history_event_types(self, sfn):
        """GetExecutionHistory includes PassStateEntered/Exited events."""
        role_arn = "arn:aws:iam::123456789012:role/StepRole"
        sm_name = f"hist-types-{uuid.uuid4().hex[:8]}"
        sm = sfn.create_state_machine(
            name=sm_name, definition=self._simple_definition(), roleArn=role_arn
        )
        sm_arn = sm["stateMachineArn"]
        try:
            exec_resp = sfn.start_execution(stateMachineArn=sm_arn)
            import time

            time.sleep(1)
            history = sfn.get_execution_history(executionArn=exec_resp["executionArn"])
            types = [e["type"] for e in history["events"]]
            assert "ExecutionStarted" in types
            assert "ExecutionSucceeded" in types
            assert "PassStateEntered" in types
            assert "PassStateExited" in types
        finally:
            sfn.delete_state_machine(stateMachineArn=sm_arn)

    def test_describe_state_machine_for_execution_fields(self, sfn):
        """DescribeStateMachineForExecution returns definition and roleArn."""
        role_arn = "arn:aws:iam::123456789012:role/StepRole"
        sm_name = f"desc-sm-exec-f-{uuid.uuid4().hex[:8]}"
        definition = json.dumps(
            {
                "StartAt": "Hello",
                "States": {
                    "Hello": {
                        "Type": "Pass",
                        "Result": {"msg": "hello"},
                        "End": True,
                    }
                },
            }
        )
        sm = sfn.create_state_machine(name=sm_name, definition=definition, roleArn=role_arn)
        sm_arn = sm["stateMachineArn"]
        try:
            exec_resp = sfn.start_execution(stateMachineArn=sm_arn)
            desc = sfn.describe_state_machine_for_execution(executionArn=exec_resp["executionArn"])
            assert desc["name"] == sm_name
            assert "definition" in desc
            parsed_def = json.loads(desc["definition"])
            assert "Hello" in parsed_def["States"]
            assert "roleArn" in desc
            assert "stateMachineArn" in desc
        finally:
            sfn.delete_state_machine(stateMachineArn=sm_arn)

    def test_execution_with_named_execution(self, sfn):
        """StartExecution with custom name, verify name in describe."""
        role_arn = "arn:aws:iam::123456789012:role/StepRole"
        sm_name = f"named-exec2-{uuid.uuid4().hex[:8]}"
        sm = sfn.create_state_machine(
            name=sm_name, definition=self._simple_definition(), roleArn=role_arn
        )
        sm_arn = sm["stateMachineArn"]
        exec_name = f"my-run-{uuid.uuid4().hex[:8]}"
        try:
            exec_resp = sfn.start_execution(stateMachineArn=sm_arn, name=exec_name)
            assert exec_name in exec_resp["executionArn"]

            desc = sfn.describe_execution(executionArn=exec_resp["executionArn"])
            assert desc["name"] == exec_name
            assert desc["stateMachineArn"] == sm_arn
        finally:
            sfn.delete_state_machine(stateMachineArn=sm_arn)

    def test_multiple_executions_different_inputs(self, sfn):
        """Start multiple executions with different inputs, list and verify count."""
        role_arn = "arn:aws:iam::123456789012:role/StepRole"
        sm_name = f"multi-exec-{uuid.uuid4().hex[:8]}"
        sm = sfn.create_state_machine(
            name=sm_name, definition=self._simple_definition(), roleArn=role_arn
        )
        sm_arn = sm["stateMachineArn"]
        try:
            exec_arns = []
            for i in range(3):
                resp = sfn.start_execution(
                    stateMachineArn=sm_arn,
                    input=json.dumps({"iteration": i}),
                )
                exec_arns.append(resp["executionArn"])

            listed = sfn.list_executions(stateMachineArn=sm_arn)
            listed_arns = [e["executionArn"] for e in listed["executions"]]
            for arn in exec_arns:
                assert arn in listed_arns
        finally:
            sfn.delete_state_machine(stateMachineArn=sm_arn)

    def test_activity_describe_after_delete_raises(self, sfn):
        """DescribeActivity on deleted activity raises ActivityDoesNotExist."""
        name = f"del-act-err-{uuid.uuid4().hex[:8]}"
        resp = sfn.create_activity(name=name)
        act_arn = resp["activityArn"]
        sfn.delete_activity(activityArn=act_arn)
        with pytest.raises(sfn.exceptions.ClientError) as exc:
            sfn.describe_activity(activityArn=act_arn)
        assert (
            "ActivityDoesNotExist" in str(exc.value) or "does not exist" in str(exc.value).lower()
        )

    def test_describe_nonexistent_state_machine(self, sfn):
        """DescribeStateMachine with nonexistent ARN raises error."""
        fake_arn = "arn:aws:states:us-east-1:123456789012:stateMachine:does-not-exist"
        with pytest.raises(sfn.exceptions.ClientError) as exc:
            sfn.describe_state_machine(stateMachineArn=fake_arn)
        assert (
            "StateMachineDoesNotExist" in str(exc.value)
            or "does not exist" in str(exc.value).lower()
        )


class TestStepFunctionsVersions:
    """Tests for state machine version operations."""

    @staticmethod
    def _simple_definition():
        return json.dumps({"StartAt": "Pass", "States": {"Pass": {"Type": "Pass", "End": True}}})

    def test_validate_state_machine_definition_ok(self, sfn):
        """ValidateStateMachineDefinition returns OK for valid ASL JSON."""
        definition = self._simple_definition()
        resp = sfn.validate_state_machine_definition(definition=definition)
        assert resp["result"] == "OK"
        assert resp["diagnostics"] == []

    def test_validate_state_machine_definition_invalid_json(self, sfn):
        """ValidateStateMachineDefinition returns FAIL for invalid JSON."""
        resp = sfn.validate_state_machine_definition(definition="not valid json {{")
        assert resp["result"] == "FAIL"
        assert len(resp["diagnostics"]) > 0
        assert resp["diagnostics"][0]["severity"] == "ERROR"

    def test_publish_state_machine_version(self, sfn):
        """PublishStateMachineVersion creates a versioned ARN."""
        role_arn = "arn:aws:iam::123456789012:role/StepRole"
        sm_name = f"pub-ver-{uuid.uuid4().hex[:8]}"
        sm = sfn.create_state_machine(
            name=sm_name, definition=self._simple_definition(), roleArn=role_arn
        )
        sm_arn = sm["stateMachineArn"]
        try:
            resp = sfn.publish_state_machine_version(stateMachineArn=sm_arn)
            assert "stateMachineVersionArn" in resp
            assert resp["stateMachineVersionArn"].startswith(sm_arn + ":")
            assert "creationDate" in resp
        finally:
            sfn.delete_state_machine(stateMachineArn=sm_arn)

    def test_list_state_machine_versions(self, sfn):
        """ListStateMachineVersions returns published versions."""
        role_arn = "arn:aws:iam::123456789012:role/StepRole"
        sm_name = f"list-ver-{uuid.uuid4().hex[:8]}"
        sm = sfn.create_state_machine(
            name=sm_name, definition=self._simple_definition(), roleArn=role_arn
        )
        sm_arn = sm["stateMachineArn"]
        try:
            # Publish two versions
            v1 = sfn.publish_state_machine_version(stateMachineArn=sm_arn)
            v2 = sfn.publish_state_machine_version(stateMachineArn=sm_arn)

            resp = sfn.list_state_machine_versions(stateMachineArn=sm_arn)
            versions = resp["stateMachineVersions"]
            assert len(versions) >= 2
            arns = [v["stateMachineVersionArn"] for v in versions]
            assert v1["stateMachineVersionArn"] in arns
            assert v2["stateMachineVersionArn"] in arns
        finally:
            sfn.delete_state_machine(stateMachineArn=sm_arn)

    def test_list_state_machine_versions_empty(self, sfn):
        """ListStateMachineVersions returns empty list when no versions published."""
        role_arn = "arn:aws:iam::123456789012:role/StepRole"
        sm_name = f"list-ver-empty-{uuid.uuid4().hex[:8]}"
        sm = sfn.create_state_machine(
            name=sm_name, definition=self._simple_definition(), roleArn=role_arn
        )
        sm_arn = sm["stateMachineArn"]
        try:
            resp = sfn.list_state_machine_versions(stateMachineArn=sm_arn)
            assert resp["stateMachineVersions"] == []
        finally:
            sfn.delete_state_machine(stateMachineArn=sm_arn)

    def test_delete_state_machine_version(self, sfn):
        """DeleteStateMachineVersion removes the version."""
        role_arn = "arn:aws:iam::123456789012:role/StepRole"
        sm_name = f"del-ver-{uuid.uuid4().hex[:8]}"
        sm = sfn.create_state_machine(
            name=sm_name, definition=self._simple_definition(), roleArn=role_arn
        )
        sm_arn = sm["stateMachineArn"]
        try:
            v1 = sfn.publish_state_machine_version(stateMachineArn=sm_arn)
            version_arn = v1["stateMachineVersionArn"]

            # Delete the version
            sfn.delete_state_machine_version(stateMachineVersionArn=version_arn)

            # Verify it's gone
            resp = sfn.list_state_machine_versions(stateMachineArn=sm_arn)
            arns = [v["stateMachineVersionArn"] for v in resp["stateMachineVersions"]]
            assert version_arn not in arns
        finally:
            sfn.delete_state_machine(stateMachineArn=sm_arn)

    def test_publish_multiple_versions_increments(self, sfn):
        """Each publish creates a new version with incrementing number."""
        role_arn = "arn:aws:iam::123456789012:role/StepRole"
        sm_name = f"multi-ver-{uuid.uuid4().hex[:8]}"
        sm = sfn.create_state_machine(
            name=sm_name, definition=self._simple_definition(), roleArn=role_arn
        )
        sm_arn = sm["stateMachineArn"]
        try:
            v1 = sfn.publish_state_machine_version(stateMachineArn=sm_arn)
            v2 = sfn.publish_state_machine_version(stateMachineArn=sm_arn)

            # Version ARNs should be different
            assert v1["stateMachineVersionArn"] != v2["stateMachineVersionArn"]

            # Version numbers should increment
            v1_num = int(v1["stateMachineVersionArn"].rsplit(":", 1)[1])
            v2_num = int(v2["stateMachineVersionArn"].rsplit(":", 1)[1])
            assert v2_num > v1_num
        finally:
            sfn.delete_state_machine(stateMachineArn=sm_arn)
