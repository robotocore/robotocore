"""Tests for CloudFormation nested stacks (AWS::CloudFormation::Stack)."""

from __future__ import annotations

import pytest
from botocore.exceptions import ClientError

from tests.iac.conftest import make_client
from tests.iac.helpers.tool_runner import CloudFormationRunner

pytestmark = pytest.mark.iac

CHILD_TEMPLATE = """\
AWSTemplateFormatVersion: "2010-09-09"
Parameters:
  BucketPrefix:
    Type: String
Resources:
  ChildBucket:
    Type: AWS::S3::Bucket
    Properties:
      BucketName: !Sub "${BucketPrefix}-child-bucket"
Outputs:
  ChildBucketName:
    Value: !Ref ChildBucket
"""


def _parent_template(template_url: str) -> str:
    return f"""\
AWSTemplateFormatVersion: "2010-09-09"
Resources:
  NestedStack:
    Type: AWS::CloudFormation::Stack
    Properties:
      TemplateURL: {template_url}
      Parameters:
        BucketPrefix: !Ref "AWS::StackName"
Outputs:
  NestedStackId:
    Value: !Ref NestedStack
  ChildBucketName:
    Value: !GetAtt NestedStack.Outputs.ChildBucketName
"""


@pytest.fixture(scope="module")
def cfn(ensure_server):
    client = make_client("cloudformation")
    return CloudFormationRunner(client)


class TestNestedStacks:
    """Verify nested stack creation via AWS::CloudFormation::Stack."""

    def test_nested_stack_creates_child_resources(self, cfn, test_run_id):
        """Upload child template to S3, create parent with nested stack, verify child bucket."""
        s3 = make_client("s3")
        template_bucket = f"{test_run_id}-nested-templates"
        parent_stack = f"{test_run_id}-nested-parent"

        try:
            # Upload child template to S3
            s3.create_bucket(Bucket=template_bucket)
            s3.put_object(
                Bucket=template_bucket,
                Key="child.yaml",
                Body=CHILD_TEMPLATE,
            )
            template_url = f"http://localhost:4566/{template_bucket}/child.yaml"

            # Deploy parent stack that references the child template
            stack = cfn.deploy_stack(parent_stack, _parent_template(template_url))
            assert stack["StackStatus"] == "CREATE_COMPLETE"

            # Verify parent outputs include nested stack info
            outputs = cfn.get_stack_outputs(parent_stack)
            assert "NestedStackId" in outputs
            assert "ChildBucketName" in outputs
            assert f"{parent_stack}-child-bucket" in outputs["ChildBucketName"]

            # Verify the child bucket actually exists
            buckets = [b["Name"] for b in s3.list_buckets()["Buckets"]]
            assert outputs["ChildBucketName"] in buckets
        finally:
            try:
                cfn.delete_stack(parent_stack)
            except Exception:
                pass
            try:
                s3.delete_object(Bucket=template_bucket, Key="child.yaml")
                s3.delete_bucket(Bucket=template_bucket)
            except Exception:
                pass

    def test_nested_stack_shows_in_list(self, cfn, test_run_id):
        """The nested child stack should appear in list_stacks."""
        s3 = make_client("s3")
        template_bucket = f"{test_run_id}-nested-list-tpl"
        parent_stack = f"{test_run_id}-nested-list"

        try:
            s3.create_bucket(Bucket=template_bucket)
            s3.put_object(
                Bucket=template_bucket,
                Key="child.yaml",
                Body=CHILD_TEMPLATE,
            )
            template_url = f"http://localhost:4566/{template_bucket}/child.yaml"

            cfn.deploy_stack(parent_stack, _parent_template(template_url))

            # List stacks and look for the nested child
            client = make_client("cloudformation")
            resp = client.list_stacks(StackStatusFilter=["CREATE_COMPLETE"])
            stack_names = [s["StackName"] for s in resp["StackSummaries"]]
            # Parent should be listed
            assert parent_stack in stack_names

            # Describe stack resources to find the nested stack
            resources = client.list_stack_resources(StackName=parent_stack)
            resource_types = [r["ResourceType"] for r in resources["StackResourceSummaries"]]
            assert "AWS::CloudFormation::Stack" in resource_types
        finally:
            try:
                cfn.delete_stack(parent_stack)
            except Exception:
                pass
            try:
                s3.delete_object(Bucket=template_bucket, Key="child.yaml")
                s3.delete_bucket(Bucket=template_bucket)
            except Exception:
                pass

    def test_delete_parent_deletes_nested(self, cfn, test_run_id):
        """Deleting the parent stack should also delete the nested child stack."""
        s3 = make_client("s3")
        template_bucket = f"{test_run_id}-nested-del-tpl"
        parent_stack = f"{test_run_id}-nested-del"

        try:
            s3.create_bucket(Bucket=template_bucket)
            s3.put_object(
                Bucket=template_bucket,
                Key="child.yaml",
                Body=CHILD_TEMPLATE,
            )
            template_url = f"http://localhost:4566/{template_bucket}/child.yaml"

            cfn.deploy_stack(parent_stack, _parent_template(template_url))
            outputs = cfn.get_stack_outputs(parent_stack)
            nested_id = outputs["NestedStackId"]

            # Delete parent
            cfn.delete_stack(parent_stack)

            # Verify nested stack is also gone
            client = make_client("cloudformation")
            try:
                resp = client.describe_stacks(StackName=nested_id)
                # If it exists, it should be in DELETE_COMPLETE state
                if resp["Stacks"]:
                    assert resp["Stacks"][0]["StackStatus"] == "DELETE_COMPLETE"
            except ClientError as exc:
                assert "does not exist" in str(exc)
        finally:
            try:
                s3.delete_object(Bucket=template_bucket, Key="child.yaml")
                s3.delete_bucket(Bucket=template_bucket)
            except Exception:
                pass
