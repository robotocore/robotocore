"""Functional test: deploy static website and exercise S3 data roundtrips."""

from pathlib import Path

import pytest

from tests.iac.conftest import make_client
from tests.iac.helpers.functional_validator import put_and_get_s3_object

pytestmark = pytest.mark.iac

TEMPLATE = (Path(__file__).parent / "template.yaml").read_text()


def _get_outputs(stack: dict) -> dict[str, str]:
    return {o["OutputKey"]: o["OutputValue"] for o in stack.get("Outputs", [])}


class TestStaticWebsiteFunctional:
    """Deploy static website stack and verify S3 content roundtrips."""

    def test_upload_index_html(self, deploy_stack, test_run_id):
        """Upload index.html to the website bucket and verify roundtrip."""
        stack = deploy_stack("static-func", TEMPLATE)
        assert stack["StackStatus"] == "CREATE_COMPLETE"

        outputs = _get_outputs(stack)
        bucket_name = outputs["BucketName"]
        s3 = make_client("s3")

        index_body = "<html><body><h1>Welcome</h1></body></html>"
        resp = put_and_get_s3_object(s3, bucket_name, "index.html", index_body)
        assert resp["ContentType"] is not None
        assert resp["ContentLength"] == len(index_body.encode("utf-8"))

    def test_upload_error_html(self, deploy_stack, test_run_id):
        """Upload error.html to the website bucket and verify roundtrip."""
        stack = deploy_stack("static-func-err", TEMPLATE)
        assert stack["StackStatus"] == "CREATE_COMPLETE"

        outputs = _get_outputs(stack)
        bucket_name = outputs["BucketName"]
        s3 = make_client("s3")

        error_body = "<html><body><h1>404 Not Found</h1></body></html>"
        resp = put_and_get_s3_object(s3, bucket_name, "error.html", error_body)
        assert resp["ContentLength"] == len(error_body.encode("utf-8"))

    def test_upload_multiple_assets(self, deploy_stack, test_run_id):
        """Upload both index and error pages, then list objects."""
        stack = deploy_stack("static-func-multi", TEMPLATE)
        assert stack["StackStatus"] == "CREATE_COMPLETE"

        outputs = _get_outputs(stack)
        bucket_name = outputs["BucketName"]
        s3 = make_client("s3")

        put_and_get_s3_object(s3, bucket_name, "index.html", "<h1>Home</h1>")
        put_and_get_s3_object(s3, bucket_name, "error.html", "<h1>Error</h1>")

        listing = s3.list_objects_v2(Bucket=bucket_name)
        keys = [obj["Key"] for obj in listing.get("Contents", [])]
        assert "index.html" in keys
        assert "error.html" in keys
        assert listing["KeyCount"] >= 2
