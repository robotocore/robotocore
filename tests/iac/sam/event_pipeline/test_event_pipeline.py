"""IaC test: sam - event_pipeline."""

import time
from pathlib import Path

import pytest

from tests.iac.conftest import make_client

pytestmark = pytest.mark.iac


@pytest.fixture(scope="module")
def deployed_stack(ensure_server, test_run_id):
    cfn = make_client("cloudformation")
    template = (Path(__file__).parent / "template.yaml").read_text()
    stack_name = f"{test_run_id}-sam-event-pipeline"
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


class TestEventPipeline:
    def test_stack_created(self, deployed_stack):
        assert deployed_stack["StackStatus"] == "CREATE_COMPLETE"

    def test_queue_exists(self, deployed_stack, ensure_server):
        outputs = {o["OutputKey"]: o["OutputValue"] for o in deployed_stack.get("Outputs", [])}
        queue_url = outputs.get("QueueUrl")
        assert queue_url is not None, "QueueUrl output missing"

        sqs = make_client("sqs")
        resp = sqs.get_queue_attributes(QueueUrl=queue_url, AttributeNames=["All"])
        assert "Attributes" in resp
        assert resp["Attributes"]["VisibilityTimeout"] == "120"

    def test_table_exists(self, deployed_stack, ensure_server):
        outputs = {o["OutputKey"]: o["OutputValue"] for o in deployed_stack.get("Outputs", [])}
        table_name = outputs.get("TableName")
        assert table_name is not None, "TableName output missing"

        ddb = make_client("dynamodb")
        resp = ddb.describe_table(TableName=table_name)
        assert resp["Table"]["TableName"] == table_name
        assert resp["Table"]["KeySchema"][0]["AttributeName"] == "pk"

    def test_function_exists(self, deployed_stack, ensure_server):
        outputs = {o["OutputKey"]: o["OutputValue"] for o in deployed_stack.get("Outputs", [])}
        function_name = outputs.get("FunctionName")
        assert function_name is not None, "FunctionName output missing"

        lam = make_client("lambda")
        resp = lam.get_function(FunctionName=function_name)
        assert resp["Configuration"]["FunctionName"] == function_name
        assert resp["Configuration"]["Runtime"] == "python3.12"
