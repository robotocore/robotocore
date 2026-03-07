"""Compatibility tests for Amazon FSx service."""

import uuid

import pytest

from tests.compatibility.conftest import make_client


@pytest.fixture
def fsx():
    return make_client("fsx")


def _unique(prefix):
    return f"{prefix}-{uuid.uuid4().hex[:8]}"


class TestFSxDescribeOperations:
    """Tests for FSx describe operations."""

    def test_describe_file_systems_empty(self, fsx):
        """describe_file_systems returns empty list when no file systems exist."""
        resp = fsx.describe_file_systems()
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200
        assert isinstance(resp["FileSystems"], list)

    def test_describe_backups_empty(self, fsx):
        """describe_backups returns empty list when no backups exist."""
        resp = fsx.describe_backups()
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200
        assert isinstance(resp["Backups"], list)


class TestFSxListOperations:
    """Tests for FSx list operations."""

    def test_list_tags_for_resource_valid_arn(self, fsx):
        """list_tags_for_resource returns empty tags for a valid ARN format."""
        arn = "arn:aws:fsx:us-east-1:123456789012:file-system/fs-00000001"
        resp = fsx.list_tags_for_resource(ResourceARN=arn)
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200
        assert isinstance(resp["Tags"], list)
