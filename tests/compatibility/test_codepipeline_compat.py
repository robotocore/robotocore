"""CodePipeline compatibility tests."""

import json
import uuid

import pytest
from botocore.exceptions import ClientError

from tests.compatibility.conftest import make_client


@pytest.fixture
def codepipeline():
    return make_client("codepipeline")


@pytest.fixture
def iam():
    return make_client("iam")


TRUST_POLICY = json.dumps(
    {
        "Version": "2012-10-17",
        "Statement": [
            {
                "Effect": "Allow",
                "Principal": {"Service": "codepipeline.amazonaws.com"},
                "Action": "sts:AssumeRole",
            }
        ],
    }
)


@pytest.fixture
def pipeline_role_arn(iam):
    role_name = _unique("cp-role")
    resp = iam.create_role(
        RoleName=role_name,
        AssumeRolePolicyDocument=TRUST_POLICY,
        Path="/",
    )
    arn = resp["Role"]["Arn"]
    yield arn
    iam.delete_role(RoleName=role_name)


def _unique(prefix):
    return f"{prefix}-{uuid.uuid4().hex[:8]}"


def _pipeline_def(name, role_arn):
    return {
        "name": name,
        "roleArn": role_arn,
        "artifactStore": {
            "type": "S3",
            "location": "test-artifact-bucket",
        },
        "stages": [
            {
                "name": "Source",
                "actions": [
                    {
                        "name": "SourceAction",
                        "actionTypeId": {
                            "category": "Source",
                            "owner": "AWS",
                            "provider": "S3",
                            "version": "1",
                        },
                        "outputArtifacts": [{"name": "SourceOutput"}],
                        "configuration": {
                            "S3Bucket": "source-bucket",
                            "S3ObjectKey": "source.zip",
                        },
                    }
                ],
            },
            {
                "name": "Deploy",
                "actions": [
                    {
                        "name": "DeployAction",
                        "actionTypeId": {
                            "category": "Deploy",
                            "owner": "AWS",
                            "provider": "S3",
                            "version": "1",
                        },
                        "inputArtifacts": [{"name": "SourceOutput"}],
                        "configuration": {
                            "BucketName": "deploy-bucket",
                            "Extract": "false",
                        },
                    }
                ],
            },
        ],
    }


@pytest.fixture
def pipeline(codepipeline, pipeline_role_arn):
    name = _unique("test-pipe")
    resp = codepipeline.create_pipeline(pipeline=_pipeline_def(name, pipeline_role_arn))
    yield resp["pipeline"]
    try:
        codepipeline.delete_pipeline(name=name)
    except ClientError:
        pass  # best-effort cleanup


class TestCodePipelineOperations:
    def test_create_pipeline(self, codepipeline, pipeline_role_arn):
        name = _unique("test-pipe")
        resp = codepipeline.create_pipeline(pipeline=_pipeline_def(name, pipeline_role_arn))
        assert resp["pipeline"]["name"] == name
        assert len(resp["pipeline"]["stages"]) == 2
        codepipeline.delete_pipeline(name=name)

    def test_get_pipeline(self, codepipeline, pipeline):
        resp = codepipeline.get_pipeline(name=pipeline["name"])
        assert resp["pipeline"]["name"] == pipeline["name"]
        assert "metadata" in resp

    def test_list_pipelines(self, codepipeline, pipeline):
        resp = codepipeline.list_pipelines()
        names = [p["name"] for p in resp["pipelines"]]
        assert pipeline["name"] in names

    def test_list_tags_for_resource(self, codepipeline, pipeline):
        get_resp = codepipeline.get_pipeline(name=pipeline["name"])
        arn = get_resp["metadata"]["pipelineArn"]
        resp = codepipeline.list_tags_for_resource(resourceArn=arn)
        assert "tags" in resp

    def test_tag_and_untag_resource(self, codepipeline, pipeline):
        get_resp = codepipeline.get_pipeline(name=pipeline["name"])
        arn = get_resp["metadata"]["pipelineArn"]

        # Tag
        codepipeline.tag_resource(
            resourceArn=arn,
            tags=[{"key": "env", "value": "test"}],
        )
        resp = codepipeline.list_tags_for_resource(resourceArn=arn)
        tag_map = {t["key"]: t["value"] for t in resp["tags"]}
        assert tag_map["env"] == "test"

        # Untag
        codepipeline.untag_resource(resourceArn=arn, tagKeys=["env"])
        resp = codepipeline.list_tags_for_resource(resourceArn=arn)
        tag_keys = [t["key"] for t in resp["tags"]]
        assert "env" not in tag_keys

    def test_update_pipeline(self, codepipeline, pipeline):
        updated = pipeline.copy()
        updated["stages"][1]["actions"][0]["configuration"]["Extract"] = "true"
        resp = codepipeline.update_pipeline(pipeline=updated)
        config = resp["pipeline"]["stages"][1]["actions"][0]["configuration"]
        assert config["Extract"] == "true"

    def test_delete_pipeline(self, codepipeline, pipeline_role_arn):
        name = _unique("test-pipe")
        codepipeline.create_pipeline(pipeline=_pipeline_def(name, pipeline_role_arn))
        codepipeline.delete_pipeline(name=name)
        # Verify it's gone
        resp = codepipeline.list_pipelines()
        names = [p["name"] for p in resp["pipelines"]]
        assert name not in names

    def test_get_nonexistent_pipeline(self, codepipeline):
        with pytest.raises(ClientError) as exc_info:
            codepipeline.get_pipeline(name="does-not-exist")
        assert exc_info.value.response["Error"]["Code"] == "PipelineNotFoundException"

    def test_get_pipeline_state(self, codepipeline, pipeline):
        resp = codepipeline.get_pipeline_state(name=pipeline["name"])
        assert resp["pipelineName"] == pipeline["name"]
        assert "stageStates" in resp

    def test_start_pipeline_execution(self, codepipeline, pipeline):
        resp = codepipeline.start_pipeline_execution(name=pipeline["name"])
        assert "pipelineExecutionId" in resp

    def test_list_pipeline_executions(self, codepipeline, pipeline):
        # Start an execution first
        codepipeline.start_pipeline_execution(name=pipeline["name"])
        resp = codepipeline.list_pipeline_executions(pipelineName=pipeline["name"])
        assert "pipelineExecutionSummaries" in resp

    def test_get_pipeline_execution(self, codepipeline, pipeline):
        start_resp = codepipeline.start_pipeline_execution(name=pipeline["name"])
        exec_id = start_resp["pipelineExecutionId"]
        resp = codepipeline.get_pipeline_execution(
            pipelineName=pipeline["name"],
            pipelineExecutionId=exec_id,
        )
        assert resp["pipelineExecution"]["pipelineExecutionId"] == exec_id

    def test_stop_pipeline_execution(self, codepipeline, pipeline):
        start_resp = codepipeline.start_pipeline_execution(name=pipeline["name"])
        exec_id = start_resp["pipelineExecutionId"]
        resp = codepipeline.stop_pipeline_execution(
            pipelineName=pipeline["name"],
            pipelineExecutionId=exec_id,
            abandon=True,
            reason="test stop",
        )
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200

    def test_list_action_executions(self, codepipeline, pipeline):
        codepipeline.start_pipeline_execution(name=pipeline["name"])
        resp = codepipeline.list_action_executions(pipelineName=pipeline["name"])
        assert "actionExecutionDetails" in resp

    def test_disable_stage_transition(self, codepipeline, pipeline):
        resp = codepipeline.disable_stage_transition(
            pipelineName=pipeline["name"],
            stageName="Deploy",
            transitionType="Inbound",
            reason="testing disable",
        )
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200

    def test_enable_stage_transition(self, codepipeline, pipeline):
        # Disable first, then enable
        codepipeline.disable_stage_transition(
            pipelineName=pipeline["name"],
            stageName="Deploy",
            transitionType="Inbound",
            reason="testing",
        )
        resp = codepipeline.enable_stage_transition(
            pipelineName=pipeline["name"],
            stageName="Deploy",
            transitionType="Inbound",
        )
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200


class TestCodePipelineCustomActions:
    def test_create_custom_action_type(self, codepipeline):
        name = _unique("custom-action")
        resp = codepipeline.create_custom_action_type(
            category="Test",
            provider=name,
            version="1",
            inputArtifactDetails={"minimumCount": 0, "maximumCount": 1},
            outputArtifactDetails={"minimumCount": 0, "maximumCount": 1},
        )
        assert resp["actionType"]["id"]["provider"] == name
        # Cleanup
        codepipeline.delete_custom_action_type(category="Test", provider=name, version="1")

    def test_list_action_types(self, codepipeline):
        resp = codepipeline.list_action_types(actionOwnerFilter="AWS")
        assert "actionTypes" in resp

    def test_delete_custom_action_type(self, codepipeline):
        name = _unique("custom-del")
        codepipeline.create_custom_action_type(
            category="Test",
            provider=name,
            version="1",
            inputArtifactDetails={"minimumCount": 0, "maximumCount": 1},
            outputArtifactDetails={"minimumCount": 0, "maximumCount": 1},
        )
        resp = codepipeline.delete_custom_action_type(category="Test", provider=name, version="1")
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200

    def test_get_action_type(self, codepipeline):
        name = _unique("custom-get")
        codepipeline.create_custom_action_type(
            category="Test",
            provider=name,
            version="1",
            inputArtifactDetails={"minimumCount": 0, "maximumCount": 1},
            outputArtifactDetails={"minimumCount": 0, "maximumCount": 1},
        )
        resp = codepipeline.get_action_type(
            category="Test", owner="Custom", provider=name, version="1"
        )
        assert resp["actionType"]["id"]["provider"] == name
        codepipeline.delete_custom_action_type(category="Test", provider=name, version="1")


class TestCodePipelineWebhooks:
    def test_put_webhook(self, codepipeline, pipeline):
        webhook_name = _unique("test-hook")
        resp = codepipeline.put_webhook(
            webhook={
                "name": webhook_name,
                "targetPipeline": pipeline["name"],
                "targetAction": "SourceAction",
                "filters": [{"jsonPath": "$.ref", "matchEquals": "refs/heads/main"}],
                "authentication": "UNAUTHENTICATED",
                "authenticationConfiguration": {},
            }
        )
        assert resp["webhook"]["definition"]["name"] == webhook_name
        codepipeline.delete_webhook(name=webhook_name)

    def test_list_webhooks(self, codepipeline, pipeline):
        webhook_name = _unique("test-hook")
        codepipeline.put_webhook(
            webhook={
                "name": webhook_name,
                "targetPipeline": pipeline["name"],
                "targetAction": "SourceAction",
                "filters": [{"jsonPath": "$.ref", "matchEquals": "refs/heads/main"}],
                "authentication": "UNAUTHENTICATED",
                "authenticationConfiguration": {},
            }
        )
        resp = codepipeline.list_webhooks()
        assert "webhooks" in resp
        names = [w["definition"]["name"] for w in resp["webhooks"]]
        assert webhook_name in names
        codepipeline.delete_webhook(name=webhook_name)

    def test_delete_webhook(self, codepipeline, pipeline):
        webhook_name = _unique("test-hook")
        codepipeline.put_webhook(
            webhook={
                "name": webhook_name,
                "targetPipeline": pipeline["name"],
                "targetAction": "SourceAction",
                "filters": [{"jsonPath": "$.ref", "matchEquals": "refs/heads/main"}],
                "authentication": "UNAUTHENTICATED",
                "authenticationConfiguration": {},
            }
        )
        resp = codepipeline.delete_webhook(name=webhook_name)
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200

    def test_delete_nonexistent_webhook(self, codepipeline):
        with pytest.raises(ClientError) as exc_info:
            codepipeline.delete_webhook(name="nonexistent-webhook")
        assert exc_info.value.response["Error"]["Code"] == "WebhookNotFoundException"


class TestCodePipelineJobs:
    def test_poll_for_jobs(self, codepipeline):
        resp = codepipeline.poll_for_jobs(
            actionTypeId={
                "category": "Test",
                "owner": "Custom",
                "provider": "MyProvider",
                "version": "1",
            }
        )
        assert "jobs" in resp

    def test_get_job_details_nonexistent(self, codepipeline):
        with pytest.raises(ClientError) as exc_info:
            codepipeline.get_job_details(jobId="00000000-0000-0000-0000-000000000000")
        assert exc_info.value.response["Error"]["Code"] == "JobNotFoundException"

    def test_acknowledge_job_nonexistent(self, codepipeline):
        with pytest.raises(ClientError) as exc_info:
            codepipeline.acknowledge_job(
                jobId="00000000-0000-0000-0000-000000000000",
                nonce="test-nonce",
            )
        assert exc_info.value.response["Error"]["Code"] == "JobNotFoundException"

    def test_put_job_success_result_nonexistent(self, codepipeline):
        with pytest.raises(ClientError) as exc_info:
            codepipeline.put_job_success_result(
                jobId="00000000-0000-0000-0000-000000000000",
            )
        assert exc_info.value.response["Error"]["Code"] == "JobNotFoundException"

    def test_put_job_failure_result_nonexistent(self, codepipeline):
        with pytest.raises(ClientError) as exc_info:
            codepipeline.put_job_failure_result(
                jobId="00000000-0000-0000-0000-000000000000",
                failureDetails={
                    "type": "JobFailed",
                    "message": "test failure",
                },
            )
        assert exc_info.value.response["Error"]["Code"] == "JobNotFoundException"

    def test_poll_for_third_party_jobs(self, codepipeline):
        resp = codepipeline.poll_for_third_party_jobs(
            actionTypeId={
                "category": "Test",
                "owner": "ThirdParty",
                "provider": "MyProvider",
                "version": "1",
            }
        )
        assert "jobs" in resp

    def test_acknowledge_third_party_job_nonexistent(self, codepipeline):
        with pytest.raises(ClientError) as exc_info:
            codepipeline.acknowledge_third_party_job(
                jobId="00000000-0000-0000-0000-000000000000",
                nonce="test-nonce",
                clientToken="test-token",
            )
        assert exc_info.value.response["Error"]["Code"] == "JobNotFoundException"

    def test_get_third_party_job_details_nonexistent(self, codepipeline):
        with pytest.raises(ClientError) as exc_info:
            codepipeline.get_third_party_job_details(
                jobId="00000000-0000-0000-0000-000000000000",
                clientToken="test-token",
            )
        assert exc_info.value.response["Error"]["Code"] == "JobNotFoundException"

    def test_put_third_party_job_success_result_nonexistent(self, codepipeline):
        with pytest.raises(ClientError) as exc_info:
            codepipeline.put_third_party_job_success_result(
                jobId="00000000-0000-0000-0000-000000000000",
                clientToken="test-token",
            )
        assert exc_info.value.response["Error"]["Code"] == "JobNotFoundException"

    def test_put_third_party_job_failure_result_nonexistent(self, codepipeline):
        with pytest.raises(ClientError) as exc_info:
            codepipeline.put_third_party_job_failure_result(
                jobId="00000000-0000-0000-0000-000000000000",
                clientToken="test-token",
                failureDetails={
                    "type": "JobFailed",
                    "message": "test failure",
                },
            )
        assert exc_info.value.response["Error"]["Code"] == "JobNotFoundException"


class TestCodePipelineApproval:
    def test_put_approval_result_nonexistent_pipeline(self, codepipeline):
        with pytest.raises(ClientError) as exc_info:
            codepipeline.put_approval_result(
                pipelineName="nonexistent-pipeline",
                stageName="Approve",
                actionName="ManualApproval",
                result={"summary": "Approved", "status": "Approved"},
                token="test-token",
            )
        assert exc_info.value.response["Error"]["Code"] == "PipelineNotFoundException"


class TestCodePipelineRetryRollback:
    def test_retry_stage_execution_nonexistent_pipeline(self, codepipeline):
        with pytest.raises(ClientError) as exc_info:
            codepipeline.retry_stage_execution(
                pipelineName="nonexistent-pipeline",
                stageName="Deploy",
                pipelineExecutionId="00000000-0000-0000-0000-000000000000",
                retryMode="FAILED_ACTIONS",
            )
        assert exc_info.value.response["Error"]["Code"] == "PipelineNotFoundException"

    def test_rollback_stage_nonexistent_pipeline(self, codepipeline):
        with pytest.raises(ClientError) as exc_info:
            codepipeline.rollback_stage(
                pipelineName="nonexistent-pipeline",
                stageName="Deploy",
                targetPipelineExecutionId="00000000-0000-0000-0000-000000000000",
            )
        assert exc_info.value.response["Error"]["Code"] == "PipelineNotFoundException"


class TestCodePipelineMiscOps:
    def test_deregister_webhook_with_third_party_nonexistent(self, codepipeline):
        with pytest.raises(ClientError) as exc_info:
            codepipeline.deregister_webhook_with_third_party(webhookName="nonexistent-webhook")
        assert exc_info.value.response["Error"]["Code"] == "WebhookNotFoundException"

    def test_register_webhook_with_third_party_nonexistent(self, codepipeline):
        with pytest.raises(ClientError) as exc_info:
            codepipeline.register_webhook_with_third_party(webhookName="nonexistent-webhook")
        assert exc_info.value.response["Error"]["Code"] == "WebhookNotFoundException"

    def test_list_rule_types(self, codepipeline):
        resp = codepipeline.list_rule_types()
        assert "ruleTypes" in resp

    def test_list_rule_executions_nonexistent(self, codepipeline):
        with pytest.raises(ClientError) as exc_info:
            codepipeline.list_rule_executions(pipelineName="nonexistent-pipeline")
        assert exc_info.value.response["Error"]["Code"] == "PipelineNotFoundException"

    def test_override_stage_condition_nonexistent(self, codepipeline):
        with pytest.raises(ClientError) as exc_info:
            codepipeline.override_stage_condition(
                pipelineName="nonexistent-pipeline",
                stageName="Deploy",
                pipelineExecutionId="00000000-0000-0000-0000-000000000000",
                conditionType="BEFORE_ENTRY",
            )
        assert exc_info.value.response["Error"]["Code"] == "PipelineNotFoundException"
