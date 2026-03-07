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

    def test_list_executions_contains_started(self, sfn):
        """Create state machine, start execution, verify it appears in list_executions."""
        role_arn = "arn:aws:iam::123456789012:role/StepRole"
        sm_name = f"test-list-exec-{uuid.uuid4().hex[:8]}"
        definition = json.dumps(
            {"Comment": "test", "StartAt": "Pass", "States": {"Pass": {"Type": "Pass", "End": True}}}
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
            {"Comment": "test", "StartAt": "Pass", "States": {"Pass": {"Type": "Pass", "End": True}}}
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
            {"Comment": "test", "StartAt": "Wait", "States": {"Wait": {"Type": "Wait", "Seconds": 300, "End": True}}}
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
        sm = sfn.create_state_machine(
            name=sm_name, definition=original_def, roleArn=role_arn
        )
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
            update_resp = sfn.update_state_machine(
                stateMachineArn=sm_arn, definition=new_def
            )
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
        sm = sfn.create_state_machine(
            name=sm_name, definition=definition, roleArn=role_arn
        )
        sm_arn = sm["stateMachineArn"]
        try:
            input_data = json.dumps({"user": "alice", "action": "login"})
            exec_resp = sfn.start_execution(
                stateMachineArn=sm_arn, input=input_data
            )
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
        sm = sfn.create_state_machine(
            name=sm_name, definition=definition, roleArn=role_arn
        )
        sm_arn = sm["stateMachineArn"]
        try:
            exec_resp = sfn.start_execution(
                stateMachineArn=sm_arn, input=json.dumps({"x": 1})
            )
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
                desc = sfn.describe_state_machine(
                    stateMachineArn=resp["stateMachineArn"]
                )
                assert desc["type"] == sm_type
        finally:
            for arn in arns:
                sfn.delete_state_machine(stateMachineArn=arn)
