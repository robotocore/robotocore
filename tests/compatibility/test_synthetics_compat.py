"""Compatibility tests for AWS CloudWatch Synthetics (Canaries) service."""

import uuid

import pytest

from tests.compatibility.conftest import make_client


@pytest.fixture
def synthetics():
    return make_client("synthetics")


def _unique(prefix):
    return f"{prefix}-{uuid.uuid4().hex[:8]}"


class TestSyntheticsCanaryOperations:
    """Tests for canary CRUD operations."""

    def test_create_canary(self, synthetics):
        """Create a canary and verify returned fields."""
        name = _unique("canary")
        resp = synthetics.create_canary(
            Name=name,
            Code={"S3Bucket": "test-bucket", "S3Key": "canary.zip", "Handler": "index.handler"},
            ArtifactS3Location="s3://test-bucket/artifacts",
            ExecutionRoleArn="arn:aws:iam::123456789012:role/canary-role",
            Schedule={"Expression": "rate(5 minutes)"},
            RuntimeVersion="syn-python-selenium-3.0",
        )
        canary = resp["Canary"]
        assert canary["Name"] == name
        assert canary["Status"]["State"] == "READY"
        assert canary["RuntimeVersion"] == "syn-python-selenium-3.0"
        assert canary["ExecutionRoleArn"] == "arn:aws:iam::123456789012:role/canary-role"
        assert canary["Schedule"]["Expression"] == "rate(5 minutes)"
        assert canary["Code"]["Handler"] == "index.handler"
        assert canary["ArtifactS3Location"] == "s3://test-bucket/artifacts"
        assert "Id" in canary
        assert "Timeline" in canary

    def test_create_canary_with_tags(self, synthetics):
        """Create a canary with tags and verify they are returned."""
        name = _unique("canary")
        resp = synthetics.create_canary(
            Name=name,
            Code={"S3Bucket": "test-bucket", "S3Key": "canary.zip", "Handler": "index.handler"},
            ArtifactS3Location="s3://test-bucket/artifacts",
            ExecutionRoleArn="arn:aws:iam::123456789012:role/canary-role",
            Schedule={"Expression": "rate(5 minutes)"},
            RuntimeVersion="syn-python-selenium-3.0",
            Tags={"env": "test", "project": "demo"},
        )
        canary = resp["Canary"]
        assert canary["Tags"]["env"] == "test"
        assert canary["Tags"]["project"] == "demo"

    def test_get_canary(self, synthetics):
        """Create a canary then retrieve it by name."""
        name = _unique("canary")
        synthetics.create_canary(
            Name=name,
            Code={"S3Bucket": "test-bucket", "S3Key": "canary.zip", "Handler": "index.handler"},
            ArtifactS3Location="s3://test-bucket/artifacts",
            ExecutionRoleArn="arn:aws:iam::123456789012:role/canary-role",
            Schedule={"Expression": "rate(5 minutes)"},
            RuntimeVersion="syn-python-selenium-3.0",
        )
        resp = synthetics.get_canary(Name=name)
        canary = resp["Canary"]
        assert canary["Name"] == name
        assert canary["RuntimeVersion"] == "syn-python-selenium-3.0"
        assert canary["ArtifactS3Location"] == "s3://test-bucket/artifacts"

    def test_get_canary_has_defaults(self, synthetics):
        """Verify default values are populated on created canaries."""
        name = _unique("canary")
        synthetics.create_canary(
            Name=name,
            Code={"S3Bucket": "test-bucket", "S3Key": "canary.zip", "Handler": "index.handler"},
            ArtifactS3Location="s3://test-bucket/artifacts",
            ExecutionRoleArn="arn:aws:iam::123456789012:role/canary-role",
            Schedule={"Expression": "rate(5 minutes)"},
            RuntimeVersion="syn-python-selenium-3.0",
        )
        canary = synthetics.get_canary(Name=name)["Canary"]
        assert canary["RunConfig"]["TimeoutInSeconds"] == 60
        assert canary["SuccessRetentionPeriodInDays"] == 31
        assert canary["FailureRetentionPeriodInDays"] == 31

    def test_describe_canaries_includes_created(self, synthetics):
        """Created canary appears in describe_canaries listing."""
        name = _unique("canary")
        synthetics.create_canary(
            Name=name,
            Code={"S3Bucket": "test-bucket", "S3Key": "canary.zip", "Handler": "index.handler"},
            ArtifactS3Location="s3://test-bucket/artifacts",
            ExecutionRoleArn="arn:aws:iam::123456789012:role/canary-role",
            Schedule={"Expression": "rate(5 minutes)"},
            RuntimeVersion="syn-python-selenium-3.0",
        )
        resp = synthetics.describe_canaries()
        names = [c["Name"] for c in resp["Canaries"]]
        assert name in names


class TestSyntheticsListOperations:
    """Tests for list/describe operations."""

    def test_describe_canaries_returns_list(self, synthetics):
        """describe_canaries returns a Canaries list."""
        resp = synthetics.describe_canaries()
        assert "Canaries" in resp
        assert isinstance(resp["Canaries"], list)

    def test_list_tags_for_resource(self, synthetics):
        """list_tags_for_resource returns a Tags dict for a canary ARN."""
        name = _unique("canary")
        synthetics.create_canary(
            Name=name,
            Code={"S3Bucket": "test-bucket", "S3Key": "canary.zip", "Handler": "index.handler"},
            ArtifactS3Location="s3://test-bucket/artifacts",
            ExecutionRoleArn="arn:aws:iam::123456789012:role/canary-role",
            Schedule={"Expression": "rate(5 minutes)"},
            RuntimeVersion="syn-python-selenium-3.0",
        )
        arn = f"arn:aws:synthetics:us-east-1:123456789012:canary:{name}"
        resp = synthetics.list_tags_for_resource(ResourceArn=arn)
        assert "Tags" in resp
        assert isinstance(resp["Tags"], dict)
