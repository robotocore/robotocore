"""IaC test: serverless - static_website (S3 with website config)."""

from __future__ import annotations

import pytest

from tests.iac.conftest import make_client
from tests.iac.helpers.stack_deployer import delete_stack, deploy_and_yield, get_stack_outputs

pytestmark = pytest.mark.iac

TEMPLATE = """\
AWSTemplateFormatVersion: "2010-09-09"
Description: Serverless static website - S3 bucket with website hosting

Resources:
  WebsiteBucket:
    Type: AWS::S3::Bucket
    Properties:
      BucketName: !Sub "${AWS::StackName}-site"
      WebsiteConfiguration:
        IndexDocument: index.html
        ErrorDocument: error.html

Outputs:
  BucketName:
    Value: !Ref WebsiteBucket
  BucketArn:
    Value: !GetAtt WebsiteBucket.Arn
"""


@pytest.fixture(scope="module")
def deployed_stack(ensure_server, test_run_id):
    stack_name = f"{test_run_id}-sls-static-website"
    stack = deploy_and_yield(stack_name, TEMPLATE)
    yield stack
    delete_stack(stack_name)


class TestStaticWebsite:
    def test_stack_created(self, deployed_stack):
        assert deployed_stack["StackStatus"] == "CREATE_COMPLETE"

    def test_bucket_exists(self, deployed_stack):
        outputs = get_stack_outputs(deployed_stack)
        s3 = make_client("s3")
        resp = s3.head_bucket(Bucket=outputs["BucketName"])
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200

    def test_website_configuration(self, deployed_stack):
        outputs = get_stack_outputs(deployed_stack)
        s3 = make_client("s3")
        website = s3.get_bucket_website(Bucket=outputs["BucketName"])
        assert website["IndexDocument"]["Suffix"] == "index.html"
        assert website["ErrorDocument"]["Key"] == "error.html"

    def test_bucket_arn_output(self, deployed_stack):
        outputs = get_stack_outputs(deployed_stack)
        assert "BucketArn" in outputs
        assert outputs["BucketArn"].startswith("arn:aws:s3:::")
