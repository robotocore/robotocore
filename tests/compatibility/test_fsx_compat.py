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


class TestFSxDescribeEmpty:
    """Tests for FSx describe operations that return empty lists."""

    def test_describe_data_repository_associations_empty(self, fsx):
        """DescribeDataRepositoryAssociations returns empty Associations list."""
        resp = fsx.describe_data_repository_associations()
        assert "Associations" in resp
        assert isinstance(resp["Associations"], list)

    def test_describe_data_repository_tasks_empty(self, fsx):
        """DescribeDataRepositoryTasks returns empty DataRepositoryTasks list."""
        resp = fsx.describe_data_repository_tasks()
        assert "DataRepositoryTasks" in resp
        assert isinstance(resp["DataRepositoryTasks"], list)

    def test_describe_file_caches_empty(self, fsx):
        """DescribeFileCaches returns empty FileCaches list."""
        resp = fsx.describe_file_caches()
        assert "FileCaches" in resp
        assert isinstance(resp["FileCaches"], list)

    def test_describe_snapshots_empty(self, fsx):
        """DescribeSnapshots returns empty Snapshots list."""
        resp = fsx.describe_snapshots()
        assert "Snapshots" in resp
        assert isinstance(resp["Snapshots"], list)

    def test_describe_storage_virtual_machines_empty(self, fsx):
        """DescribeStorageVirtualMachines returns empty list."""
        resp = fsx.describe_storage_virtual_machines()
        assert "StorageVirtualMachines" in resp
        assert isinstance(resp["StorageVirtualMachines"], list)

    def test_describe_volumes_empty(self, fsx):
        """DescribeVolumes returns empty Volumes list."""
        resp = fsx.describe_volumes()
        assert "Volumes" in resp
        assert isinstance(resp["Volumes"], list)

    def test_describe_shared_vpc_configuration(self, fsx):
        """DescribeSharedVpcConfiguration returns configuration."""
        resp = fsx.describe_shared_vpc_configuration()
        assert "EnableFsxRouteTableUpdatesFromParticipantAccounts" in resp

    def test_describe_s3_access_point_attachments_empty(self, fsx):
        """DescribeS3AccessPointAttachments returns empty list."""
        resp = fsx.describe_s3_access_point_attachments()
        assert "S3AccessPointAttachments" in resp
        assert isinstance(resp["S3AccessPointAttachments"], list)


class TestFSxFileSystemAliases:
    """Tests for FSx file system alias operations."""

    def test_describe_file_system_aliases_for_fs(self, fsx):
        """DescribeFileSystemAliases returns Aliases list for a real file system."""
        resp = fsx.create_file_system(
            FileSystemType="LUSTRE",
            StorageCapacity=1200,
            SubnetIds=["subnet-00000001"],
            LustreConfiguration={"DeploymentType": "SCRATCH_1"},
        )
        fs_id = resp["FileSystem"]["FileSystemId"]
        try:
            alias_resp = fsx.describe_file_system_aliases(FileSystemId=fs_id)
            assert "Aliases" in alias_resp
            assert isinstance(alias_resp["Aliases"], list)
        finally:
            fsx.delete_file_system(FileSystemId=fs_id)

    def test_describe_file_system_aliases_nonexistent(self, fsx):
        """DescribeFileSystemAliases for nonexistent fs raises FileSystemNotFound."""
        with pytest.raises(ClientError) as exc:
            fsx.describe_file_system_aliases(FileSystemId="fs-does-not-exist")
        assert exc.value.response["Error"]["Code"] == "FileSystemNotFound"

    def test_associate_file_system_aliases(self, fsx):
        """AssociateFileSystemAliases returns Aliases list."""
        resp = fsx.create_file_system(
            FileSystemType="LUSTRE",
            StorageCapacity=1200,
            SubnetIds=["subnet-00000001"],
            LustreConfiguration={"DeploymentType": "SCRATCH_1"},
        )
        fs_id = resp["FileSystem"]["FileSystemId"]
        try:
            alias_resp = fsx.associate_file_system_aliases(
                FileSystemId=fs_id,
                Aliases=["accounting.example.com"],
            )
            assert "Aliases" in alias_resp
            assert isinstance(alias_resp["Aliases"], list)
        finally:
            fsx.delete_file_system(FileSystemId=fs_id)

    def test_disassociate_file_system_aliases(self, fsx):
        """DisassociateFileSystemAliases returns Aliases list."""
        resp = fsx.create_file_system(
            FileSystemType="LUSTRE",
            StorageCapacity=1200,
            SubnetIds=["subnet-00000001"],
            LustreConfiguration={"DeploymentType": "SCRATCH_1"},
        )
        fs_id = resp["FileSystem"]["FileSystemId"]
        try:
            disassoc_resp = fsx.disassociate_file_system_aliases(
                FileSystemId=fs_id,
                Aliases=["accounting.example.com"],
            )
            assert "Aliases" in disassoc_resp
            assert isinstance(disassoc_resp["Aliases"], list)
        finally:
            fsx.delete_file_system(FileSystemId=fs_id)


class TestFSxUpdateOps:
    """Tests for FSx update operations."""

    def test_update_file_system(self, fsx):
        """UpdateFileSystem modifies a file system."""
        resp = fsx.create_file_system(
            FileSystemType="LUSTRE",
            StorageCapacity=1200,
            SubnetIds=["subnet-00000001"],
            LustreConfiguration={"DeploymentType": "SCRATCH_1"},
        )
        fs_id = resp["FileSystem"]["FileSystemId"]
        try:
            upd_resp = fsx.update_file_system(
                FileSystemId=fs_id,
                LustreConfiguration={"WeeklyMaintenanceStartTime": "1:00:00"},
            )
            assert "FileSystem" in upd_resp
            assert upd_resp["FileSystem"]["FileSystemId"] == fs_id
        finally:
            fsx.delete_file_system(FileSystemId=fs_id)

    def test_update_file_system_nonexistent(self, fsx):
        """UpdateFileSystem for nonexistent fs raises error."""
        with pytest.raises(ClientError) as exc:
            fsx.update_file_system(
                FileSystemId="fs-does-not-exist",
                LustreConfiguration={"WeeklyMaintenanceStartTime": "1:00:00"},
            )
        err = exc.value.response["Error"]["Code"]
        assert err in ("ResourceNotFoundException", "BadRequest", "FileSystemNotFound")


class TestFSxSnapshotOps:
    """Tests for FSx snapshot operations."""

    def test_delete_snapshot_nonexistent(self, fsx):
        """DeleteSnapshot for nonexistent snapshot raises error."""
        with pytest.raises(ClientError) as exc:
            fsx.delete_snapshot(SnapshotId="fsvolsnap-does-not-exist")
        assert exc.value.response["Error"]["Code"] in (
            "ResourceNotFoundException",
            "SnapshotNotFound",
        )


class TestFSxFileSystemFromBackup:
    """Tests for creating file system from backup."""

    def test_create_file_system_from_backup_nonexistent(self, fsx):
        """CreateFileSystemFromBackup with nonexistent backup raises error."""
        with pytest.raises(ClientError) as exc:
            fsx.create_file_system_from_backup(
                BackupId="backup-does-not-exist",
                SubnetIds=["subnet-00000001"],
            )
        err = exc.value.response["Error"]["Code"]
        assert err in ("ResourceNotFoundException", "BackupNotFound", "BadRequest")


class TestFSxVolumeOps:
    """Tests for FSx volume operations."""

    def test_create_volume_from_backup_nonexistent(self, fsx):
        """CreateVolumeFromBackup with nonexistent backup raises error."""
        with pytest.raises(ClientError) as exc:
            fsx.create_volume_from_backup(
                BackupId="backup-does-not-exist",
                Name="test-vol",
                OntapConfiguration={
                    "SizeInMegabytes": 1024,
                    "StorageVirtualMachineId": "svm-0123456789abcdef0",
                },
            )
        err = exc.value.response["Error"]["Code"]
        assert err in ("ResourceNotFoundException", "BackupNotFound", "BadRequest")

    def test_create_volume_returns_volume(self, fsx):
        """CreateVolume with ONTAP config returns a Volume."""
        resp = fsx.create_volume(
            VolumeType="ONTAP",
            Name="test-vol",
            OntapConfiguration={
                "SizeInMegabytes": 1024,
                "StorageVirtualMachineId": "svm-0123456789abcdef0",
                "JunctionPath": "/vol1",
            },
        )
        assert "Volume" in resp
        assert resp["Volume"]["Name"] == "test-vol"

    def test_delete_volume_nonexistent(self, fsx):
        """DeleteVolume for nonexistent volume raises error."""
        with pytest.raises(ClientError) as exc:
            fsx.delete_volume(
                VolumeId="fsvol-0123456789abcdef0",
                OntapConfiguration={"SkipFinalBackup": True},
            )
        err = exc.value.response["Error"]["Code"]
        assert err in ("ResourceNotFoundException", "VolumeNotFound", "BadRequest")


class TestFSxDataRepositoryAssociationOps:
    """Tests for FSx data repository association operations."""

    def test_create_data_repository_association_nonexistent_fs(self, fsx):
        """CreateDataRepositoryAssociation with nonexistent fs raises error."""
        with pytest.raises(ClientError) as exc:
            fsx.create_data_repository_association(
                FileSystemId="fs-does-not-exist",
                FileSystemPath="/data",
                DataRepositoryPath="s3://my-bucket/prefix",
            )
        err = exc.value.response["Error"]["Code"]
        assert err in ("ResourceNotFoundException", "FileSystemNotFound", "BadRequest")

    def test_delete_data_repository_association_nonexistent(self, fsx):
        """DeleteDataRepositoryAssociation with nonexistent ID raises error."""
        with pytest.raises(ClientError) as exc:
            fsx.delete_data_repository_association(
                AssociationId="dra-does-not-exist",
                DeleteDataInFileSystem=False,
            )
        err = exc.value.response["Error"]["Code"]
        assert err in ("ResourceNotFoundException", "BadRequest")

    def test_update_data_repository_association_nonexistent(self, fsx):
        """UpdateDataRepositoryAssociation with nonexistent ID raises error."""
        with pytest.raises(ClientError) as exc:
            fsx.update_data_repository_association(
                AssociationId="dra-does-not-exist",
            )
        err = exc.value.response["Error"]["Code"]
        assert err in ("ResourceNotFoundException", "BadRequest")


class TestFSxStorageVirtualMachineOps:
    """Tests for FSx storage virtual machine operations."""

    def test_create_storage_virtual_machine_nonexistent_fs(self, fsx):
        """CreateStorageVirtualMachine with nonexistent fs raises error."""
        with pytest.raises(ClientError) as exc:
            fsx.create_storage_virtual_machine(
                FileSystemId="fs-does-not-exist",
                Name="test-svm",
            )
        err = exc.value.response["Error"]["Code"]
        assert err in ("ResourceNotFoundException", "FileSystemNotFound", "BadRequest")

    def test_delete_storage_virtual_machine_nonexistent(self, fsx):
        """DeleteStorageVirtualMachine with nonexistent ID raises error."""
        with pytest.raises(ClientError) as exc:
            fsx.delete_storage_virtual_machine(
                StorageVirtualMachineId="svm-0123456789abcdef0",
            )
        err = exc.value.response["Error"]["Code"]
        assert err in (
            "ResourceNotFoundException",
            "StorageVirtualMachineNotFound",
            "BadRequest",
        )


class TestFSxSnapshotUpdateOps:
    """Tests for FSx snapshot update operations."""

    def test_update_snapshot_nonexistent(self, fsx):
        """UpdateSnapshot for nonexistent snapshot raises error."""
        with pytest.raises(ClientError) as exc:
            fsx.update_snapshot(
                SnapshotId="fsvolsnap-does-not-exist",
                Name="updated-name",
            )
        err = exc.value.response["Error"]["Code"]
        assert err in ("ResourceNotFoundException", "SnapshotNotFound", "BadRequest")

    def test_create_snapshot_nonexistent_volume(self, fsx):
        """CreateSnapshot with nonexistent volume raises error."""
        with pytest.raises(ClientError) as exc:
            fsx.create_snapshot(
                Name="test-snap",
                VolumeId="fsvol-0123456789abcdef0",
            )
        err = exc.value.response["Error"]["Code"]
        assert err in ("ResourceNotFoundException", "VolumeNotFound", "BadRequest")
