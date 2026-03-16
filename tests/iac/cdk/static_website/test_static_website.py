"""IaC test: cdk - static_website.

Validates S3 static website bucket with public-read policy.
Resources are created via boto3 (mirroring the CDK program).
"""

from __future__ import annotations

import json

import pytest

from tests.iac.helpers.functional_validator import put_and_get_s3_object
from tests.iac.helpers.resource_validator import assert_s3_bucket_exists

pytestmark = pytest.mark.iac


@pytest.fixture(scope="module")
def website_resources(s3_client):
    """Create S3 website bucket with policy via boto3."""
    bucket_name = "cdk-static-website"
    s3_client.create_bucket(Bucket=bucket_name)

    # Website configuration
    s3_client.put_bucket_website(
        Bucket=bucket_name,
        WebsiteConfiguration={
            "IndexDocument": {"Suffix": "index.html"},
            "ErrorDocument": {"Key": "error.html"},
        },
    )

    # Public read policy
    policy = json.dumps(
        {
            "Version": "2012-10-17",
            "Statement": [
                {
                    "Sid": "PublicReadGetObject",
                    "Effect": "Allow",
                    "Principal": "*",
                    "Action": "s3:GetObject",
                    "Resource": f"arn:aws:s3:::{bucket_name}/*",
                }
            ],
        }
    )
    s3_client.put_bucket_policy(Bucket=bucket_name, Policy=policy)

    yield {
        "bucket_name": bucket_name,
    }

    # Cleanup
    try:
        objs = s3_client.list_objects_v2(Bucket=bucket_name)
        for obj in objs.get("Contents", []):
            s3_client.delete_object(Bucket=bucket_name, Key=obj["Key"])
    except Exception:
        pass  # best-effort cleanup
    s3_client.delete_bucket_policy(Bucket=bucket_name)
    s3_client.delete_bucket(Bucket=bucket_name)


class TestStaticWebsite:
    """Validate CDK-provisioned S3 static website resources."""

    def test_bucket_exists(self, website_resources, s3_client):
        """Verify the S3 bucket exists."""
        assert_s3_bucket_exists(s3_client, website_resources["bucket_name"])

    def test_website_configuration(self, website_resources, s3_client):
        """Verify index and error documents are configured correctly."""
        bucket_name = website_resources["bucket_name"]
        resp = s3_client.get_bucket_website(Bucket=bucket_name)
        assert resp["IndexDocument"]["Suffix"] == "index.html"
        assert resp["ErrorDocument"]["Key"] == "error.html"

    def test_bucket_policy(self, website_resources, s3_client):
        """Bucket policy allows public read access."""
        bucket_name = website_resources["bucket_name"]
        resp = s3_client.get_bucket_policy(Bucket=bucket_name)
        policy = json.loads(resp["Policy"])

        statements = policy.get("Statement", [])
        assert len(statements) >= 1, "Expected at least one policy statement"

        public_stmt = statements[0]
        assert public_stmt["Effect"] == "Allow"
        assert public_stmt["Principal"] == "*"
        assert "s3:GetObject" in public_stmt["Action"]

    def test_s3_object_roundtrip(self, website_resources, s3_client):
        """Upload and download an object from the website bucket."""
        bucket_name = website_resources["bucket_name"]
        put_and_get_s3_object(
            s3_client, bucket_name, "index.html", "<html><body>Hello</body></html>"
        )
