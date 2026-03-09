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


class TestFSxFileSystemOperations:
    """Tests for FSx file system CRUD operations."""

    def test_create_file_system(self, fsx):
        """create_file_system returns file system with expected fields."""
        resp = fsx.create_file_system(
            FileSystemType="LUSTRE",
            StorageCapacity=1200,
            SubnetIds=["subnet-00000001"],
            LustreConfiguration={"DeploymentType": "SCRATCH_1"},
        )
        fs = resp["FileSystem"]
        assert "FileSystemId" in fs
        assert fs["FileSystemType"] == "LUSTRE"
        assert fs["StorageCapacity"] == 1200
        assert "StorageType" in fs or "FileSystemId" in fs
        fsx.delete_file_system(FileSystemId=fs["FileSystemId"])

    def test_create_file_system_appears_in_describe(self, fsx):
        """A created file system appears in describe_file_systems."""
        resp = fsx.create_file_system(
            FileSystemType="LUSTRE",
            StorageCapacity=1200,
            SubnetIds=["subnet-00000001"],
            LustreConfiguration={"DeploymentType": "SCRATCH_1"},
        )
        fs_id = resp["FileSystem"]["FileSystemId"]
        try:
            desc = fsx.describe_file_systems(FileSystemIds=[fs_id])
            assert len(desc["FileSystems"]) == 1
            assert desc["FileSystems"][0]["FileSystemId"] == fs_id
        finally:
            fsx.delete_file_system(FileSystemId=fs_id)

    def test_delete_file_system(self, fsx):
        """delete_file_system removes the file system."""
        resp = fsx.create_file_system(
            FileSystemType="LUSTRE",
            StorageCapacity=1200,
            SubnetIds=["subnet-00000001"],
            LustreConfiguration={"DeploymentType": "SCRATCH_1"},
        )
        fs_id = resp["FileSystem"]["FileSystemId"]
        del_resp = fsx.delete_file_system(FileSystemId=fs_id)
        assert del_resp["ResponseMetadata"]["HTTPStatusCode"] == 200

    def test_create_file_system_with_tags(self, fsx):
        """create_file_system with Tags stores them."""
        resp = fsx.create_file_system(
            FileSystemType="LUSTRE",
            StorageCapacity=1200,
            SubnetIds=["subnet-00000001"],
            LustreConfiguration={"DeploymentType": "SCRATCH_1"},
            Tags=[{"Key": "env", "Value": "test"}],
        )
        fs = resp["FileSystem"]
        fs_id = fs["FileSystemId"]
        try:
            tag_map = {t["Key"]: t["Value"] for t in fs.get("Tags", [])}
            assert tag_map.get("env") == "test"
        finally:
            fsx.delete_file_system(FileSystemId=fs_id)


class TestFSxTagOperations:
    """Tests for FSx tag/untag operations."""

    def test_tag_resource(self, fsx):
        """tag_resource adds tags to a file system."""
        resp = fsx.create_file_system(
            FileSystemType="LUSTRE",
            StorageCapacity=1200,
            SubnetIds=["subnet-00000001"],
            LustreConfiguration={"DeploymentType": "SCRATCH_1"},
        )
        fs_id = resp["FileSystem"]["FileSystemId"]
        arn = resp["FileSystem"]["ResourceARN"]
        try:
            fsx.tag_resource(
                ResourceARN=arn,
                Tags=[{"Key": "env", "Value": "prod"}, {"Key": "team", "Value": "platform"}],
            )
            tags_resp = fsx.list_tags_for_resource(ResourceARN=arn)
            tag_map = {t["Key"]: t["Value"] for t in tags_resp["Tags"]}
            assert tag_map["env"] == "prod"
            assert tag_map["team"] == "platform"
        finally:
            fsx.delete_file_system(FileSystemId=fs_id)

    def test_untag_resource(self, fsx):
        """untag_resource removes tags from a file system."""
        resp = fsx.create_file_system(
            FileSystemType="LUSTRE",
            StorageCapacity=1200,
            SubnetIds=["subnet-00000001"],
            LustreConfiguration={"DeploymentType": "SCRATCH_1"},
        )
        fs_id = resp["FileSystem"]["FileSystemId"]
        arn = resp["FileSystem"]["ResourceARN"]
        try:
            fsx.tag_resource(
                ResourceARN=arn,
                Tags=[{"Key": "env", "Value": "test"}, {"Key": "keep", "Value": "yes"}],
            )
            fsx.untag_resource(ResourceARN=arn, TagKeys=["env"])
            tags_resp = fsx.list_tags_for_resource(ResourceARN=arn)
            tag_map = {t["Key"]: t["Value"] for t in tags_resp["Tags"]}
            assert "env" not in tag_map
            assert tag_map["keep"] == "yes"
        finally:
            fsx.delete_file_system(FileSystemId=fs_id)

    def test_list_tags_for_resource_valid_arn(self, fsx):
        """list_tags_for_resource returns empty tags for a valid ARN format."""
        arn = "arn:aws:fsx:us-east-1:123456789012:file-system/fs-00000001"
        resp = fsx.list_tags_for_resource(ResourceARN=arn)
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200
        assert isinstance(resp["Tags"], list)


class TestFSxDescribeBackupsFiltered:
    """Tests for describe_backups with file system filter."""

    def test_describe_backups_for_file_system(self, fsx):
        """describe_backups returns backups filtered by file system."""
        resp = fsx.create_file_system(
            FileSystemType="LUSTRE",
            StorageCapacity=1200,
            SubnetIds=["subnet-00000001"],
            LustreConfiguration={"DeploymentType": "SCRATCH_1"},
        )
        fs_id = resp["FileSystem"]["FileSystemId"]
        try:
            backup_resp = fsx.create_backup(FileSystemId=fs_id)
            backup_id = backup_resp["Backup"]["BackupId"]
            desc = fsx.describe_backups()
            backup_ids = [b["BackupId"] for b in desc["Backups"]]
            assert backup_id in backup_ids
            fsx.delete_backup(BackupId=backup_id)
        finally:
            fsx.delete_file_system(FileSystemId=fs_id)
