"""Compatibility tests for Amazon FSx service."""

import uuid

import pytest
from botocore.exceptions import ClientError

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


class TestFSxBackupOperations:
    """Tests for FSx backup operations."""

    def test_create_and_delete_backup(self, fsx):
        """create_backup creates a backup, delete_backup removes it."""
        # Create a file system to back up
        fs_resp = fsx.create_file_system(
            FileSystemType="LUSTRE",
            StorageCapacity=1200,
            SubnetIds=["subnet-00000001"],
            LustreConfiguration={"DeploymentType": "SCRATCH_1"},
        )
        fs_id = fs_resp["FileSystem"]["FileSystemId"]
        try:
            # Create backup
            backup_resp = fsx.create_backup(FileSystemId=fs_id)
            assert "Backup" in backup_resp
            backup_id = backup_resp["Backup"]["BackupId"]
            assert backup_id.startswith("backup-")

            # Delete backup
            del_resp = fsx.delete_backup(BackupId=backup_id)
            assert del_resp["ResponseMetadata"]["HTTPStatusCode"] == 200
        finally:
            fsx.delete_file_system(FileSystemId=fs_id)

    def test_delete_backup_nonexistent(self, fsx):
        """delete_backup raises error for nonexistent backup."""
        with pytest.raises(ClientError) as exc:
            fsx.delete_backup(BackupId="backup-does-not-exist")
        assert exc.value.response["Error"]["Code"] == "ResourceNotFoundException"


class TestFSxListOperations:
    """Tests for FSx list operations."""

    def test_list_tags_for_resource_valid_arn(self, fsx):
        """list_tags_for_resource returns empty tags for a valid ARN format."""
        arn = "arn:aws:fsx:us-east-1:123456789012:file-system/fs-00000001"
        resp = fsx.list_tags_for_resource(ResourceARN=arn)
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200
        assert isinstance(resp["Tags"], list)
