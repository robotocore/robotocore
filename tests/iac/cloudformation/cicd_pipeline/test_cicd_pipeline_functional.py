"""Functional test: deploy CI/CD pipeline and exercise S3 artifacts and SNS notifications."""

from pathlib import Path

import pytest

from tests.iac.conftest import ACCOUNT_ID, REGION, make_client
from tests.iac.helpers.functional_validator import (
    put_and_get_s3_object,
    subscribe_sns_to_sqs_and_publish,
)

pytestmark = pytest.mark.iac

TEMPLATE = (Path(__file__).parent / "template.yaml").read_text()


def _get_outputs(stack: dict) -> dict[str, str]:
    return {o["OutputKey"]: o["OutputValue"] for o in stack.get("Outputs", [])}


class TestCicdPipelineFunctional:
    """Deploy CI/CD pipeline stack and exercise artifact storage and notifications."""

    def test_upload_and_download_artifact(self, deploy_stack):
        """Upload a build artifact to S3 and verify the roundtrip."""
        stack = deploy_stack("cicd-func-s3", TEMPLATE)
        assert stack["StackStatus"] == "CREATE_COMPLETE"

        outputs = _get_outputs(stack)
        bucket_name = outputs["ArtifactsBucketName"]
        s3 = make_client("s3")

        artifact_content = b"PK\x03\x04fake-zip-content-for-build-artifact"
        resp = put_and_get_s3_object(
            s3, bucket_name, "builds/build-42/artifact.zip", artifact_content
        )
        assert resp["ContentLength"] == len(artifact_content)

    def test_upload_multiple_artifacts(self, deploy_stack):
        """Upload multiple artifacts and list them."""
        stack = deploy_stack("cicd-func-list", TEMPLATE)
        assert stack["StackStatus"] == "CREATE_COMPLETE"

        outputs = _get_outputs(stack)
        bucket_name = outputs["ArtifactsBucketName"]
        s3 = make_client("s3")

        put_and_get_s3_object(s3, bucket_name, "builds/build-1/app.jar", b"jar-content-1")
        put_and_get_s3_object(s3, bucket_name, "builds/build-2/app.jar", b"jar-content-2")

        listing = s3.list_objects_v2(Bucket=bucket_name, Prefix="builds/")
        keys = [obj["Key"] for obj in listing.get("Contents", [])]
        assert "builds/build-1/app.jar" in keys
        assert "builds/build-2/app.jar" in keys

    def test_build_notification_via_sns(self, deploy_stack, test_run_id):
        """Subscribe SQS to build notifications topic and publish a message."""
        stack = deploy_stack("cicd-func-sns", TEMPLATE)
        assert stack["StackStatus"] == "CREATE_COMPLETE"

        outputs = _get_outputs(stack)
        topic_arn = outputs["BuildNotificationsTopicArn"]

        sqs = make_client("sqs")
        sns = make_client("sns")
        queue_name = f"{test_run_id}-cicd-notify"
        q = sqs.create_queue(QueueName=queue_name)
        queue_url = q["QueueUrl"]
        queue_arn = f"arn:aws:sqs:{REGION}:{ACCOUNT_ID}:{queue_name}"

        msg = subscribe_sns_to_sqs_and_publish(
            sns, sqs, topic_arn, queue_arn, queue_url, "Build #42 completed successfully"
        )
        assert "Build #42 completed successfully" in msg["Body"]
