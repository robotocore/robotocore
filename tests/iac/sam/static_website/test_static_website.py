"""IaC test: sam - static_website."""

from pathlib import Path

import pytest

from tests.iac.conftest import make_client
from tests.iac.helpers.stack_deployer import delete_stack, deploy_and_yield, get_stack_outputs

pytestmark = pytest.mark.iac


@pytest.fixture(scope="module")
def deployed_stack(ensure_server, test_run_id):
    template = (Path(__file__).parent / "template.yaml").read_text()
    stack_name = f"{test_run_id}-sam-static-website"
    stack = deploy_and_yield(stack_name, template)
    yield stack
    delete_stack(stack_name)


class TestStaticWebsite:
    def test_stack_created(self, deployed_stack):
        assert deployed_stack["StackStatus"] == "CREATE_COMPLETE"

    def test_bucket_exists(self, deployed_stack, ensure_server):
        outputs = get_stack_outputs(deployed_stack)
        bucket_name = outputs.get("BucketName")
        assert bucket_name is not None, "BucketName output missing"

        s3 = make_client("s3")
        resp = s3.head_bucket(Bucket=bucket_name)
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200

    def test_website_config(self, deployed_stack, ensure_server):
        outputs = get_stack_outputs(deployed_stack)
        bucket_name = outputs.get("BucketName")
        assert bucket_name is not None

        s3 = make_client("s3")
        resp = s3.get_bucket_website(Bucket=bucket_name)
        assert resp["IndexDocument"]["Suffix"] == "index.html"
        assert resp["ErrorDocument"]["Key"] == "error.html"
