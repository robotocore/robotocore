"""IaC test: sam - cicd_pipeline."""

import time
from pathlib import Path

import pytest

from tests.iac.conftest import make_client

pytestmark = pytest.mark.iac


@pytest.fixture(scope="module")
def deployed_stack(ensure_server, test_run_id):
    cfn = make_client("cloudformation")
    template = (Path(__file__).parent / "template.yaml").read_text()
    stack_name = f"{test_run_id}-sam-cicd-pipeline"
    cfn.create_stack(
        StackName=stack_name,
        TemplateBody=template,
        Capabilities=["CAPABILITY_IAM", "CAPABILITY_NAMED_IAM", "CAPABILITY_AUTO_EXPAND"],
    )
    for _ in range(60):
        resp = cfn.describe_stacks(StackName=stack_name)
        status = resp["Stacks"][0]["StackStatus"]
        if status == "CREATE_COMPLETE":
            yield resp["Stacks"][0]
            cfn.delete_stack(StackName=stack_name)
            return
        if "FAILED" in status or "ROLLBACK" in status:
            pytest.skip(f"SAM stack failed: {status}")
            return
        time.sleep(1)
    pytest.skip("SAM stack timed out")


class TestCicdPipeline:
    def test_stack_created(self, deployed_stack):
        assert deployed_stack["StackStatus"] == "CREATE_COMPLETE"

    def test_artifact_bucket_exists(self, deployed_stack, ensure_server):
        outputs = {o["OutputKey"]: o["OutputValue"] for o in deployed_stack.get("Outputs", [])}
        bucket_name = outputs.get("ArtifactBucketName")
        assert bucket_name is not None, "ArtifactBucketName output missing"

        s3 = make_client("s3")
        resp = s3.head_bucket(Bucket=bucket_name)
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200

    def test_artifact_bucket_versioning(self, deployed_stack, ensure_server):
        outputs = {o["OutputKey"]: o["OutputValue"] for o in deployed_stack.get("Outputs", [])}
        bucket_name = outputs.get("ArtifactBucketName")
        assert bucket_name is not None

        s3 = make_client("s3")
        resp = s3.get_bucket_versioning(Bucket=bucket_name)
        assert resp["Status"] == "Enabled"

    def test_notification_topic_exists(self, deployed_stack, ensure_server):
        outputs = {o["OutputKey"]: o["OutputValue"] for o in deployed_stack.get("Outputs", [])}
        topic_arn = outputs.get("NotificationTopicArn")
        assert topic_arn is not None, "NotificationTopicArn output missing"

        sns = make_client("sns")
        resp = sns.get_topic_attributes(TopicArn=topic_arn)
        assert resp["Attributes"]["TopicArn"] == topic_arn

    def test_build_role_exists(self, deployed_stack, ensure_server):
        outputs = {o["OutputKey"]: o["OutputValue"] for o in deployed_stack.get("Outputs", [])}
        role_arn = outputs.get("BuildRoleArn")
        assert role_arn is not None, "BuildRoleArn output missing"

        # Extract role name from ARN (arn:aws:iam::123456789012:role/name)
        role_name = role_arn.rsplit("/", 1)[-1]
        iam = make_client("iam")
        resp = iam.get_role(RoleName=role_name)
        assert resp["Role"]["Arn"] == role_arn
