"""Tests for pipeline orchestration via Step Functions."""

import json


class TestPipelineOrchestration:
    """Step Functions-based pipeline orchestration."""

    def test_create_pipeline_state_machine(self, pipeline, unique_name):
        role_arn = pipeline.create_pipeline_role(f"orch-role-{unique_name}")
        try:
            sm_arn = pipeline.create_pipeline_state_machine(
                name=f"pipeline-{unique_name}",
                role_arn=role_arn,
                with_deploy=True,
            )
            try:
                info = pipeline.describe_state_machine(sm_arn)
                assert info["name"] == f"pipeline-{unique_name}"
                definition = info["definition"]
                assert definition["StartAt"] == "Checkout"
                assert "Build" in definition["States"]
                assert "Test" in definition["States"]
                assert "Deploy" in definition["States"]
            finally:
                pipeline.stepfunctions.delete_state_machine(stateMachineArn=sm_arn)
        finally:
            pipeline.iam.delete_role(RoleName=f"orch-role-{unique_name}")

    def test_execute_pipeline(self, pipeline, unique_name):
        role_arn = pipeline.create_pipeline_role(f"exec-role-{unique_name}")
        try:
            sm_arn = pipeline.create_pipeline_state_machine(
                name=f"exec-pipeline-{unique_name}",
                role_arn=role_arn,
            )
            try:
                exec_arn = pipeline.execute_pipeline(
                    state_machine_arn=sm_arn,
                    build_id="exec-001",
                    repo="org/exec-app",
                    branch="main",
                    commit_sha="execabc",
                )
                status = pipeline.get_execution_status(exec_arn)
                assert status in ("RUNNING", "SUCCEEDED")
            finally:
                pipeline.stepfunctions.delete_state_machine(stateMachineArn=sm_arn)
        finally:
            pipeline.iam.delete_role(RoleName=f"exec-role-{unique_name}")

    def test_pipeline_without_deploy_stage(self, pipeline, unique_name):
        role_arn = pipeline.create_pipeline_role(f"nodep-role-{unique_name}")
        try:
            sm_arn = pipeline.create_pipeline_state_machine(
                name=f"nodep-pipeline-{unique_name}",
                role_arn=role_arn,
                with_deploy=False,
            )
            try:
                info = pipeline.describe_state_machine(sm_arn)
                definition = info["definition"]
                assert "Deploy" not in definition["States"]
                assert definition["States"]["Test"].get("End") is True
            finally:
                pipeline.stepfunctions.delete_state_machine(stateMachineArn=sm_arn)
        finally:
            pipeline.iam.delete_role(RoleName=f"nodep-role-{unique_name}")

    def test_pipeline_execution_with_input(self, pipeline, unique_name):
        role_arn = pipeline.create_pipeline_role(f"input-role-{unique_name}")
        try:
            sm_arn = pipeline.create_pipeline_state_machine(
                name=f"input-pipeline-{unique_name}",
                role_arn=role_arn,
            )
            try:
                exec_arn = pipeline.execute_pipeline(
                    state_machine_arn=sm_arn,
                    build_id="input-001",
                    repo="org/input-app",
                    branch="feature/test",
                    commit_sha="inputsha",
                )
                # Verify execution was created with correct input
                desc = pipeline.stepfunctions.describe_execution(executionArn=exec_arn)
                input_data = json.loads(desc["input"])
                assert input_data["build_id"] == "input-001"
                assert input_data["repo"] == "org/input-app"
                assert input_data["branch"] == "feature/test"
            finally:
                pipeline.stepfunctions.delete_state_machine(stateMachineArn=sm_arn)
        finally:
            pipeline.iam.delete_role(RoleName=f"input-role-{unique_name}")
