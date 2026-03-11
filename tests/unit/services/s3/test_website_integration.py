"""Semantic integration tests for S3 website hosting and virtual-hosted routing.

These tests use Moto's mock_aws to set up real S3 state and verify
the website serving and virtual-hosted routing logic end-to-end.
"""

import asyncio

from moto import mock_aws
from starlette.requests import Request

from robotocore.gateway.s3_routing import rewrite_vhost_to_path
from robotocore.services.s3.website import handle_website_request


def _make_request(path: str = "/", host: str = "localhost") -> Request:
    """Create a minimal Starlette Request for testing."""
    scope = {
        "type": "http",
        "method": "GET",
        "path": path,
        "query_string": b"",
        "headers": [(b"host", host.encode("latin-1"))],
        "root_path": "",
    }

    async def receive():
        return {"type": "http.request", "body": b""}

    return Request(scope, receive)


def _run(coro):
    """Run an async function synchronously."""
    return asyncio.get_event_loop().run_until_complete(coro)


@mock_aws
class TestWebsiteEndToEnd:
    """End-to-end: create bucket with website config -> upload -> serve."""

    def _setup_bucket_with_website(self, bucket_name="test-site"):
        """Helper to create a bucket with website configuration and upload files."""
        import boto3

        s3 = boto3.client("s3", region_name="us-east-1")

        # Create bucket
        s3.create_bucket(Bucket=bucket_name)

        # Configure website
        s3.put_bucket_website(
            Bucket=bucket_name,
            WebsiteConfiguration={
                "IndexDocument": {"Suffix": "index.html"},
                "ErrorDocument": {"Key": "error.html"},
            },
        )

        # Upload index.html
        s3.put_object(
            Bucket=bucket_name,
            Key="index.html",
            Body=b"<html><body>Hello World</body></html>",
            ContentType="text/html",
        )

        # Upload error document
        s3.put_object(
            Bucket=bucket_name,
            Key="error.html",
            Body=b"<html><body>Not Found</body></html>",
            ContentType="text/html",
        )

        # Upload a subdirectory index
        s3.put_object(
            Bucket=bucket_name,
            Key="subdir/index.html",
            Body=b"<html><body>Subdirectory</body></html>",
            ContentType="text/html",
        )

        return s3

    def test_serve_index_document(self):
        """Request / returns index.html content."""
        self._setup_bucket_with_website()

        request = _make_request("/", "test-site.s3-website.localhost.localstack.cloud")
        response = _run(handle_website_request(request, "test-site"))

        assert response.status_code == 200
        assert b"Hello World" in response.body

    def test_serve_subdir_index(self):
        """Request /subdir/ returns subdir/index.html content."""
        self._setup_bucket_with_website()

        request = _make_request("/subdir/", "test-site.s3-website.localhost.localstack.cloud")
        response = _run(handle_website_request(request, "test-site"))

        assert response.status_code == 200
        assert b"Subdirectory" in response.body

    def test_missing_file_returns_error_document(self):
        """Request for missing file returns error.html with 404 status."""
        self._setup_bucket_with_website()

        request = _make_request(
            "/nonexistent.html", "test-site.s3-website.localhost.localstack.cloud"
        )
        response = _run(handle_website_request(request, "test-site"))

        assert response.status_code == 404
        assert b"Not Found" in response.body

    def test_missing_file_no_error_document(self):
        """Request for missing file without error doc returns XML error."""
        import boto3

        s3 = boto3.client("s3", region_name="us-east-1")
        s3.create_bucket(Bucket="no-error-doc")
        s3.put_bucket_website(
            Bucket="no-error-doc",
            WebsiteConfiguration={
                "IndexDocument": {"Suffix": "index.html"},
            },
        )

        request = _make_request(
            "/missing.html", "no-error-doc.s3-website.localhost.localstack.cloud"
        )
        response = _run(handle_website_request(request, "no-error-doc"))

        assert response.status_code == 404
        assert b"NoSuchKey" in response.body

    def test_no_website_config(self):
        """Request to bucket without website config returns error."""
        import boto3

        s3 = boto3.client("s3", region_name="us-east-1")
        s3.create_bucket(Bucket="no-website")

        request = _make_request("/", "no-website.s3-website.localhost.localstack.cloud")
        response = _run(handle_website_request(request, "no-website"))

        assert response.status_code == 404
        assert b"NoSuchWebsiteConfiguration" in response.body

    def test_nonexistent_bucket(self):
        """Request to nonexistent bucket returns NoSuchBucket."""
        request = _make_request("/", "ghost-bucket.s3-website.localhost.localstack.cloud")
        response = _run(handle_website_request(request, "ghost-bucket"))

        assert response.status_code == 404
        assert b"NoSuchBucket" in response.body


@mock_aws
class TestWebsiteRedirectRules:
    """End-to-end tests for website redirect rules."""

    def test_prefix_redirect(self):
        """Redirect rule based on key prefix."""
        import boto3

        s3 = boto3.client("s3", region_name="us-east-1")
        s3.create_bucket(Bucket="redirect-site")
        s3.put_bucket_website(
            Bucket="redirect-site",
            WebsiteConfiguration={
                "IndexDocument": {"Suffix": "index.html"},
                "RoutingRules": [
                    {
                        "Condition": {"KeyPrefixEquals": "old/"},
                        "Redirect": {"ReplaceKeyPrefixWith": "new/"},
                    }
                ],
            },
        )

        request = _make_request(
            "/old/page.html", "redirect-site.s3-website.localhost.localstack.cloud"
        )
        response = _run(handle_website_request(request, "redirect-site"))

        assert response.status_code == 301
        assert "/new/page.html" in response.headers.get("location", "")

    def test_error_redirect(self):
        """Redirect rule on 404 error."""
        import boto3

        s3 = boto3.client("s3", region_name="us-east-1")
        s3.create_bucket(Bucket="error-redirect-site")
        s3.put_bucket_website(
            Bucket="error-redirect-site",
            WebsiteConfiguration={
                "IndexDocument": {"Suffix": "index.html"},
                "RoutingRules": [
                    {
                        "Condition": {"HttpErrorCodeReturnedEquals": "404"},
                        "Redirect": {"ReplaceKeyWith": "custom-404.html"},
                    }
                ],
            },
        )
        s3.put_object(
            Bucket="error-redirect-site",
            Key="custom-404.html",
            Body=b"Custom 404",
            ContentType="text/html",
        )

        request = _make_request(
            "/missing", "error-redirect-site.s3-website.localhost.localstack.cloud"
        )
        response = _run(handle_website_request(request, "error-redirect-site"))

        assert response.status_code == 301
        assert "/custom-404.html" in response.headers.get("location", "")


@mock_aws
class TestVirtualHostedRouting:
    """End-to-end: virtual-hosted GET -> returns correct object."""

    def test_vhost_rewrite_then_get(self):
        """Virtual-hosted GET rewrites path correctly for S3."""
        import boto3

        s3 = boto3.client("s3", region_name="us-east-1")
        s3.create_bucket(Bucket="vhost-test")
        s3.put_object(Bucket="vhost-test", Key="hello.txt", Body=b"world")

        scope = {
            "type": "http",
            "method": "GET",
            "path": "/hello.txt",
            "query_string": b"",
            "headers": [(b"host", b"vhost-test.s3.localhost.robotocore.cloud")],
        }

        result = rewrite_vhost_to_path(scope)
        assert result is not None
        assert result["path"] == "/vhost-test/hello.txt"

    def test_vhost_rewrite_put(self):
        """Virtual-hosted PUT rewrites path correctly for S3."""
        scope = {
            "type": "http",
            "method": "PUT",
            "path": "/new-object.txt",
            "query_string": b"",
            "headers": [(b"host", b"upload-bucket.s3.localhost.robotocore.cloud")],
        }

        result = rewrite_vhost_to_path(scope)
        assert result is not None
        assert result["path"] == "/upload-bucket/new-object.txt"

    def test_vhost_list_bucket(self):
        """Virtual-hosted GET / on bucket rewrites to /bucket for listing."""
        scope = {
            "type": "http",
            "method": "GET",
            "path": "/",
            "query_string": b"list-type=2&prefix=data/",
            "headers": [(b"host", b"mybucket.s3.localhost.robotocore.cloud")],
        }

        result = rewrite_vhost_to_path(scope)
        assert result is not None
        assert result["path"] == "/mybucket"
        assert result["query_string"] == b"list-type=2&prefix=data/"
