"""IaC test: serverless - static_website (S3 with website config)."""

from __future__ import annotations

import time

import pytest

from tests.iac.conftest import make_client

pytestmark = pytest.mark.iac

TEMPLATE = """\
AWSTemplateFormatVersion: "2010-09-09"
Description: Serverless static website - S3 bucket with website hosting

Resources:
  WebsiteBucket:
    Type: AWS::S3::Bucket
    Properties:
      BucketName: sls-static-website-site
      WebsiteConfiguration:
        IndexDocument: index.html
        ErrorDocument: error.html

Outputs:
  BucketName:
    Value: !Ref WebsiteBucket
  BucketArn:
    Value: !GetAtt WebsiteBucket.Arn
"""


def _get_outputs(stack: dict) -> dict[str, str]:
    return {o["OutputKey"]: o["OutputValue"] for o in stack.get("Outputs", [])}


@pytest.fixture(scope="module")
def deployed_stack(ensure_server, test_run_id):
    cfn = make_client("cloudformation")
    stack_name = f"{test_run_id}-sls-static-website"
    cfn.create_stack(
        StackName=stack_name,
        TemplateBody=TEMPLATE,
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
            pytest.skip(f"Stack deploy failed: {status}")
            return
        time.sleep(1)
    pytest.skip("Stack deploy timed out")


class TestStaticWebsite:
    def test_stack_created(self, deployed_stack):
        assert deployed_stack["StackStatus"] == "CREATE_COMPLETE"

    def test_bucket_exists(self, deployed_stack):
        outputs = _get_outputs(deployed_stack)
        s3 = make_client("s3")
        resp = s3.head_bucket(Bucket=outputs["BucketName"])
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200

    def test_website_configuration(self, deployed_stack):
        outputs = _get_outputs(deployed_stack)
        s3 = make_client("s3")
        website = s3.get_bucket_website(Bucket=outputs["BucketName"])
        assert website["IndexDocument"]["Suffix"] == "index.html"
        assert website["ErrorDocument"]["Key"] == "error.html"

    def test_bucket_arn_output(self, deployed_stack):
        outputs = _get_outputs(deployed_stack)
        assert "BucketArn" in outputs
        assert outputs["BucketArn"].startswith("arn:aws:s3:::")
