"""ResourceGroupsTaggingAPI compatibility tests."""

import uuid

import pytest

from tests.compatibility.conftest import make_client


def _uid():
    return uuid.uuid4().hex[:8]


@pytest.fixture
def tagging():
    return make_client("resourcegroupstaggingapi")


@pytest.fixture
def s3():
    return make_client("s3")


class TestResourceGroupsTaggingAPIOperations:
    def test_get_resources_empty(self, tagging):
        resp = tagging.get_resources()
        assert "ResourceTagMappingList" in resp

    def test_get_tag_keys_empty(self, tagging):
        resp = tagging.get_tag_keys()
        assert "TagKeys" in resp

    def test_get_tag_values(self, tagging):
        resp = tagging.get_tag_values(Key="env")
        assert "TagValues" in resp

    def test_tag_resources(self, tagging, s3):
        bucket = f"tagres-{_uid()}"
        s3.create_bucket(Bucket=bucket)
        arn = f"arn:aws:s3:::{bucket}"
        try:
            resp = tagging.tag_resources(
                ResourceARNList=[arn],
                Tags={"team": "platform"},
            )
            assert "FailedResourcesMap" in resp
        finally:
            try:
                s3.delete_bucket(Bucket=bucket)
            except Exception:
                pass  # best-effort cleanup

    def test_get_compliance_summary(self, tagging):
        resp = tagging.get_compliance_summary()
        assert "SummaryList" in resp

    def test_describe_report_creation(self, tagging):
        resp = tagging.describe_report_creation()
        assert "Status" in resp

    def test_list_required_tags(self, tagging):
        resp = tagging.list_required_tags()
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200

    def test_untag_resources(self, tagging, s3):
        bucket = f"untagres-{_uid()}"
        s3.create_bucket(Bucket=bucket)
        arn = f"arn:aws:s3:::{bucket}"
        try:
            tagging.tag_resources(
                ResourceARNList=[arn],
                Tags={"removeme": "val"},
            )
            resp = tagging.untag_resources(
                ResourceARNList=[arn],
                TagKeys=["removeme"],
            )
            assert "FailedResourcesMap" in resp
        finally:
            try:
                s3.delete_bucket(Bucket=bucket)
            except Exception:
                pass  # best-effort cleanup
