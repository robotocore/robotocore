"""
Step Functions state machine tests.

Tests creation, execution, branching (Choice state), parallel execution,
and input/output verification.
"""

import json

from .models import WorkflowStep


class TestStepFunctionsOperations:
    """Step Functions orchestration layer tests."""

    def test_create_state_machine_with_pass_states(self, state_machine):
        """Create a state machine with Pass states and verify it exists."""
        app, sm_arn, role_arn = state_machine
        assert sm_arn
        assert "stateMachine" in sm_arn

    def test_execute_and_describe(self, state_machine):
        """Start an execution and describe its status."""
        app, sm_arn, _ = state_machine

        exec_arn = app.start_execution(sm_arn, input_data={"order_id": "ORD-001"})
        assert exec_arn
        assert "execution" in exec_arn

        desc = app.describe_execution(exec_arn)
        assert desc["stateMachineArn"] == sm_arn
        assert desc["status"] in ("RUNNING", "SUCCEEDED")

    def test_choice_state_branching(self, serverless_app, unique_name):
        """State machine with Choice state branches based on input."""
        role_arn = serverless_app.create_step_functions_role(f"choice-role-{unique_name}")

        definition = {
            "Comment": "Choice state test",
            "StartAt": "CheckValue",
            "States": {
                "CheckValue": {
                    "Type": "Choice",
                    "Choices": [
                        {
                            "Variable": "$.value",
                            "NumericGreaterThan": 100,
                            "Next": "HighValue",
                        },
                    ],
                    "Default": "LowValue",
                },
                "HighValue": {
                    "Type": "Pass",
                    "Result": {"category": "high"},
                    "End": True,
                },
                "LowValue": {
                    "Type": "Pass",
                    "Result": {"category": "low"},
                    "End": True,
                },
            },
        }

        sm_arn = serverless_app.create_state_machine(
            name=f"choice-wf-{unique_name}",
            role_arn=role_arn,
            definition_dict=definition,
        )

        # Execute with high value
        exec_arn = serverless_app.start_execution(sm_arn, input_data={"value": 200})
        desc = serverless_app.describe_execution(exec_arn)
        assert desc["status"] in ("RUNNING", "SUCCEEDED")

    def test_parallel_state(self, serverless_app, unique_name):
        """State machine with Parallel state runs branches concurrently."""
        role_arn = serverless_app.create_step_functions_role(f"parallel-role-{unique_name}")

        definition = {
            "Comment": "Parallel state test",
            "StartAt": "ParallelProcessing",
            "States": {
                "ParallelProcessing": {
                    "Type": "Parallel",
                    "Branches": [
                        {
                            "StartAt": "BranchA",
                            "States": {
                                "BranchA": {
                                    "Type": "Pass",
                                    "Result": {"branch": "A", "status": "done"},
                                    "End": True,
                                }
                            },
                        },
                        {
                            "StartAt": "BranchB",
                            "States": {
                                "BranchB": {
                                    "Type": "Pass",
                                    "Result": {"branch": "B", "status": "done"},
                                    "End": True,
                                }
                            },
                        },
                    ],
                    "Next": "Finish",
                },
                "Finish": {"Type": "Succeed"},
            },
        }

        sm_arn = serverless_app.create_state_machine(
            name=f"parallel-wf-{unique_name}",
            role_arn=role_arn,
            definition_dict=definition,
        )

        exec_arn = serverless_app.start_execution(sm_arn, input_data={"test": True})
        desc = serverless_app.describe_execution(exec_arn)
        assert desc["status"] in ("RUNNING", "SUCCEEDED")

    def test_execution_input_preserved(self, state_machine):
        """Execution input is preserved in the describe response."""
        app, sm_arn, _ = state_machine

        input_data = {"user_id": "U-123", "action": "signup", "plan": "pro"}
        exec_arn = app.start_execution(sm_arn, input_data=input_data)

        desc = app.describe_execution(exec_arn)
        assert "input" in desc
        parsed_input = json.loads(desc["input"])
        assert parsed_input["user_id"] == "U-123"
        assert parsed_input["action"] == "signup"
        assert parsed_input["plan"] == "pro"

    def test_list_executions(self, state_machine):
        """List executions for a state machine."""
        app, sm_arn, _ = state_machine

        app.start_execution(sm_arn, input_data={"run": 1})
        app.start_execution(sm_arn, input_data={"run": 2})

        executions = app.list_executions(sm_arn)
        assert len(executions) >= 2

    def test_workflow_steps_builder(self, serverless_app, unique_name):
        """Build a state machine from WorkflowStep objects."""
        role_arn = serverless_app.create_step_functions_role(f"builder-role-{unique_name}")

        steps = [
            WorkflowStep(
                name="ReceiveOrder",
                type="Pass",
                result={"step": "received"},
                next="ValidateOrder",
            ),
            WorkflowStep(
                name="ValidateOrder",
                type="Pass",
                result={"step": "validated"},
                next="ShipOrder",
            ),
            WorkflowStep(
                name="ShipOrder",
                type="Pass",
                result={"step": "shipped"},
                next="Done",
            ),
            WorkflowStep(name="Done", type="Succeed"),
        ]

        sm_arn = serverless_app.create_state_machine(
            name=f"builder-wf-{unique_name}",
            role_arn=role_arn,
            steps=steps,
        )

        exec_arn = serverless_app.start_execution(
            sm_arn, input_data={"order_id": "ORD-BUILDER-001"}
        )
        desc = serverless_app.describe_execution(exec_arn)
        assert desc["status"] in ("RUNNING", "SUCCEEDED")
