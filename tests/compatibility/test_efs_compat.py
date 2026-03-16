"""EFS (Elastic File System) compatibility tests."""

import json
import uuid

import pytest
from botocore.exceptions import ClientError

from tests.compatibility.conftest import make_client


@pytest.fixture
def efs():
    return make_client("efs")


def _unique(prefix):
    return f"{prefix}-{uuid.uuid4().hex[:8]}"


def _create_fs(efs, **kwargs):
    """Create a file system and return its ID. Caller is responsible for cleanup."""
    token = uuid.uuid4().hex[:8]
    r = efs.create_file_system(CreationToken=token, **kwargs)
    return r["FileSystemId"]


class TestEFSFileSystemOperations:
    def test_create_file_system(self, efs):
        fs_id = _create_fs(efs)
        assert fs_id.startswith("fs-")
        efs.delete_file_system(FileSystemId=fs_id)

    def test_create_file_system_with_tags(self, efs):
        token = uuid.uuid4().hex[:8]
        r = efs.create_file_system(
            CreationToken=token,
            Tags=[{"Key": "Name", "Value": "tagged-fs"}],
        )
        fs_id = r["FileSystemId"]
        assert r["Name"] == "tagged-fs"
        tags = [t for t in r["Tags"] if t["Key"] == "Name"]
        assert tags[0]["Value"] == "tagged-fs"
        efs.delete_file_system(FileSystemId=fs_id)

    def test_create_file_system_fields(self, efs):
        token = uuid.uuid4().hex[:8]
        r = efs.create_file_system(CreationToken=token)
        fs_id = r["FileSystemId"]
        assert "FileSystemArn" in r
        assert "CreationTime" in r
        assert "LifeCycleState" in r
        assert "SizeInBytes" in r
        assert "PerformanceMode" in r
        assert "ThroughputMode" in r
        assert r["CreationToken"] == token
        efs.delete_file_system(FileSystemId=fs_id)

    def test_create_file_system_encrypted(self, efs):
        token = uuid.uuid4().hex[:8]
        r = efs.create_file_system(CreationToken=token, Encrypted=True)
        fs_id = r["FileSystemId"]
        assert r["Encrypted"] is True
        efs.delete_file_system(FileSystemId=fs_id)

    def test_describe_file_systems(self, efs):
        fs_id = _create_fs(efs)
        r = efs.describe_file_systems(FileSystemId=fs_id)
        assert len(r["FileSystems"]) == 1
        assert r["FileSystems"][0]["FileSystemId"] == fs_id
        efs.delete_file_system(FileSystemId=fs_id)

    def test_describe_file_systems_list(self, efs):
        fs1 = _create_fs(efs)
        fs2 = _create_fs(efs)
        r = efs.describe_file_systems()
        ids = [fs["FileSystemId"] for fs in r["FileSystems"]]
        assert fs1 in ids
        assert fs2 in ids
        efs.delete_file_system(FileSystemId=fs1)
        efs.delete_file_system(FileSystemId=fs2)

    def test_delete_file_system(self, efs):
        fs_id = _create_fs(efs)
        efs.delete_file_system(FileSystemId=fs_id)
        # Verify it's gone
        r = efs.describe_file_systems()
        ids = [fs["FileSystemId"] for fs in r["FileSystems"]]
        assert fs_id not in ids

    def test_delete_nonexistent_file_system(self, efs):
        with pytest.raises(ClientError) as exc:
            efs.delete_file_system(FileSystemId="fs-00000000")
        assert exc.value.response["Error"]["Code"] == "FileSystemNotFound"

    def test_describe_nonexistent_file_system(self, efs):
        with pytest.raises(ClientError) as exc:
            efs.describe_file_systems(FileSystemId="fs-00000000")
        assert exc.value.response["Error"]["Code"] == "FileSystemNotFound"

    def test_tag_resource(self, efs):
        fs_id = _create_fs(efs)
        efs.tag_resource(
            ResourceId=fs_id,
            Tags=[{"Key": "Env", "Value": "prod"}, {"Key": "Team", "Value": "infra"}],
        )
        r = efs.list_tags_for_resource(ResourceId=fs_id)
        tag_map = {t["Key"]: t["Value"] for t in r["Tags"]}
        assert tag_map["Env"] == "prod"
        assert tag_map["Team"] == "infra"
        efs.delete_file_system(FileSystemId=fs_id)

    def test_untag_resource(self, efs):
        fs_id = _create_fs(
            efs, Tags=[{"Key": "Keep", "Value": "yes"}, {"Key": "Remove", "Value": "yes"}]
        )
        efs.untag_resource(ResourceId=fs_id, TagKeys=["Remove"])
        r = efs.list_tags_for_resource(ResourceId=fs_id)
        keys = [t["Key"] for t in r["Tags"]]
        assert "Keep" in keys
        assert "Remove" not in keys
        efs.delete_file_system(FileSystemId=fs_id)

    def test_list_tags_for_resource(self, efs):
        fs_id = _create_fs(efs, Tags=[{"Key": "Name", "Value": "tag-test"}])
        r = efs.list_tags_for_resource(ResourceId=fs_id)
        tag_map = {t["Key"]: t["Value"] for t in r["Tags"]}
        assert tag_map["Name"] == "tag-test"
        efs.delete_file_system(FileSystemId=fs_id)

    def test_put_file_system_policy(self, efs):
        fs_id = _create_fs(efs)
        fs_arn = f"arn:aws:elasticfilesystem:us-east-1:123456789012:file-system/{fs_id}"
        policy = json.dumps(
            {
                "Version": "2012-10-17",
                "Statement": [
                    {
                        "Effect": "Allow",
                        "Principal": {"AWS": "*"},
                        "Action": ["elasticfilesystem:ClientMount"],
                        "Resource": fs_arn,
                    }
                ],
            }
        )
        r = efs.put_file_system_policy(FileSystemId=fs_id, Policy=policy)
        assert r["FileSystemId"] == fs_id
        assert "Policy" in r
        efs.delete_file_system(FileSystemId=fs_id)

    def test_describe_file_system_policy(self, efs):
        fs_id = _create_fs(efs)
        fs_arn = f"arn:aws:elasticfilesystem:us-east-1:123456789012:file-system/{fs_id}"
        policy = json.dumps(
            {
                "Version": "2012-10-17",
                "Statement": [
                    {
                        "Effect": "Allow",
                        "Principal": {"AWS": "*"},
                        "Action": ["elasticfilesystem:ClientMount"],
                        "Resource": fs_arn,
                    }
                ],
            }
        )
        efs.put_file_system_policy(FileSystemId=fs_id, Policy=policy)
        r = efs.describe_file_system_policy(FileSystemId=fs_id)
        assert r["FileSystemId"] == fs_id
        returned_policy = json.loads(r["Policy"])
        assert returned_policy["Version"] == "2012-10-17"
        assert len(returned_policy["Statement"]) == 1
        efs.delete_file_system(FileSystemId=fs_id)

    def test_describe_file_system_policy_not_found(self, efs):
        fs_id = _create_fs(efs)
        with pytest.raises(ClientError) as exc:
            efs.describe_file_system_policy(FileSystemId=fs_id)
        assert exc.value.response["Error"]["Code"] == "PolicyNotFound"
        efs.delete_file_system(FileSystemId=fs_id)


class TestEFSAccessPointOperations:
    def test_create_access_point(self, efs):
        fs_id = _create_fs(efs)
        token = uuid.uuid4().hex[:8]
        r = efs.create_access_point(
            ClientToken=token,
            FileSystemId=fs_id,
            Tags=[{"Key": "Name", "Value": "test-ap"}],
        )
        ap_id = r["AccessPointId"]
        assert ap_id.startswith("fsap-")
        assert r["FileSystemId"] == fs_id
        assert "AccessPointArn" in r
        efs.delete_access_point(AccessPointId=ap_id)
        efs.delete_file_system(FileSystemId=fs_id)

    def test_create_access_point_with_posix_user(self, efs):
        fs_id = _create_fs(efs)
        token = uuid.uuid4().hex[:8]
        r = efs.create_access_point(
            ClientToken=token,
            FileSystemId=fs_id,
            PosixUser={"Uid": 1000, "Gid": 1000},
        )
        ap_id = r["AccessPointId"]
        assert r["PosixUser"]["Uid"] == 1000
        assert r["PosixUser"]["Gid"] == 1000
        efs.delete_access_point(AccessPointId=ap_id)
        efs.delete_file_system(FileSystemId=fs_id)

    def test_create_access_point_with_root_directory(self, efs):
        fs_id = _create_fs(efs)
        token = uuid.uuid4().hex[:8]
        r = efs.create_access_point(
            ClientToken=token,
            FileSystemId=fs_id,
            RootDirectory={
                "Path": "/data",
                "CreationInfo": {
                    "OwnerUid": 1000,
                    "OwnerGid": 1000,
                    "Permissions": "755",
                },
            },
        )
        ap_id = r["AccessPointId"]
        assert r["RootDirectory"]["Path"] == "/data"
        efs.delete_access_point(AccessPointId=ap_id)
        efs.delete_file_system(FileSystemId=fs_id)

    def test_describe_access_points(self, efs):
        fs_id = _create_fs(efs)
        token = uuid.uuid4().hex[:8]
        r = efs.create_access_point(ClientToken=token, FileSystemId=fs_id)
        ap_id = r["AccessPointId"]
        r = efs.describe_access_points(FileSystemId=fs_id)
        ap_ids = [ap["AccessPointId"] for ap in r["AccessPoints"]]
        assert ap_id in ap_ids
        efs.delete_access_point(AccessPointId=ap_id)
        efs.delete_file_system(FileSystemId=fs_id)

    def test_describe_access_points_by_id(self, efs):
        fs_id = _create_fs(efs)
        token = uuid.uuid4().hex[:8]
        r = efs.create_access_point(ClientToken=token, FileSystemId=fs_id)
        ap_id = r["AccessPointId"]
        r = efs.describe_access_points(AccessPointId=ap_id)
        assert len(r["AccessPoints"]) == 1
        assert r["AccessPoints"][0]["AccessPointId"] == ap_id
        efs.delete_access_point(AccessPointId=ap_id)
        efs.delete_file_system(FileSystemId=fs_id)

    def test_delete_access_point(self, efs):
        fs_id = _create_fs(efs)
        token = uuid.uuid4().hex[:8]
        r = efs.create_access_point(ClientToken=token, FileSystemId=fs_id)
        ap_id = r["AccessPointId"]
        efs.delete_access_point(AccessPointId=ap_id)
        r = efs.describe_access_points(FileSystemId=fs_id)
        ap_ids = [ap["AccessPointId"] for ap in r["AccessPoints"]]
        assert ap_id not in ap_ids
        efs.delete_file_system(FileSystemId=fs_id)

    def test_delete_nonexistent_access_point(self, efs):
        """Deleting a nonexistent access point succeeds silently (idempotent)."""
        efs.delete_access_point(AccessPointId="fsap-00000000")


class TestEFSLifecycleConfiguration:
    def test_put_lifecycle_configuration(self, efs):
        fs_id = _create_fs(efs)
        r = efs.put_lifecycle_configuration(
            FileSystemId=fs_id,
            LifecyclePolicies=[{"TransitionToIA": "AFTER_30_DAYS"}],
        )
        assert len(r["LifecyclePolicies"]) == 1
        assert r["LifecyclePolicies"][0]["TransitionToIA"] == "AFTER_30_DAYS"
        efs.delete_file_system(FileSystemId=fs_id)

    def test_describe_lifecycle_configuration(self, efs):
        fs_id = _create_fs(efs)
        efs.put_lifecycle_configuration(
            FileSystemId=fs_id,
            LifecyclePolicies=[{"TransitionToIA": "AFTER_60_DAYS"}],
        )
        r = efs.describe_lifecycle_configuration(FileSystemId=fs_id)
        assert len(r["LifecyclePolicies"]) >= 1
        policies = {k: v for p in r["LifecyclePolicies"] for k, v in p.items()}
        assert "TransitionToIA" in policies
        assert policies["TransitionToIA"] == "AFTER_60_DAYS"
        efs.delete_file_system(FileSystemId=fs_id)

    def test_update_lifecycle_configuration(self, efs):
        fs_id = _create_fs(efs)
        efs.put_lifecycle_configuration(
            FileSystemId=fs_id,
            LifecyclePolicies=[{"TransitionToIA": "AFTER_30_DAYS"}],
        )
        # Update to a different policy
        r = efs.put_lifecycle_configuration(
            FileSystemId=fs_id,
            LifecyclePolicies=[{"TransitionToIA": "AFTER_90_DAYS"}],
        )
        assert r["LifecyclePolicies"][0]["TransitionToIA"] == "AFTER_90_DAYS"
        efs.delete_file_system(FileSystemId=fs_id)

    def test_clear_lifecycle_configuration(self, efs):
        fs_id = _create_fs(efs)
        efs.put_lifecycle_configuration(
            FileSystemId=fs_id,
            LifecyclePolicies=[{"TransitionToIA": "AFTER_30_DAYS"}],
        )
        # Clear by passing empty list
        r = efs.put_lifecycle_configuration(
            FileSystemId=fs_id,
            LifecyclePolicies=[],
        )
        assert r["LifecyclePolicies"] == []
        efs.delete_file_system(FileSystemId=fs_id)


class TestEFSMountTargetOperations:
    @pytest.fixture(autouse=True)
    def setup_vpc(self, efs):
        """Create a VPC and subnet for mount target tests."""
        ec2 = make_client("ec2")
        vpc = ec2.create_vpc(CidrBlock="10.0.0.0/16")
        self.vpc_id = vpc["Vpc"]["VpcId"]
        subnet = ec2.create_subnet(VpcId=self.vpc_id, CidrBlock="10.0.1.0/24")
        self.subnet_id = subnet["Subnet"]["SubnetId"]
        self.ec2 = ec2
        yield
        try:
            ec2.delete_subnet(SubnetId=self.subnet_id)
        except Exception:
            pass  # best-effort cleanup
        try:
            ec2.delete_vpc(VpcId=self.vpc_id)
        except Exception:
            pass  # best-effort cleanup

    def test_create_mount_target(self, efs):
        fs_id = _create_fs(efs)
        r = efs.create_mount_target(FileSystemId=fs_id, SubnetId=self.subnet_id)
        mt_id = r["MountTargetId"]
        assert mt_id.startswith("fsmt-")
        assert r["FileSystemId"] == fs_id
        assert r["SubnetId"] == self.subnet_id
        assert "IpAddress" in r
        assert "LifeCycleState" in r
        efs.delete_mount_target(MountTargetId=mt_id)
        efs.delete_file_system(FileSystemId=fs_id)

    def test_delete_mount_target(self, efs):
        fs_id = _create_fs(efs)
        r = efs.create_mount_target(FileSystemId=fs_id, SubnetId=self.subnet_id)
        mt_id = r["MountTargetId"]
        efs.delete_mount_target(MountTargetId=mt_id)
        r = efs.describe_mount_targets(FileSystemId=fs_id)
        mt_ids = [mt["MountTargetId"] for mt in r["MountTargets"]]
        assert mt_id not in mt_ids
        efs.delete_file_system(FileSystemId=fs_id)

    def test_describe_mount_targets(self, efs):
        fs_id = _create_fs(efs)
        r = efs.create_mount_target(FileSystemId=fs_id, SubnetId=self.subnet_id)
        mt_id = r["MountTargetId"]
        result = efs.describe_mount_targets(FileSystemId=fs_id)
        assert "MountTargets" in result
        assert len(result["MountTargets"]) == 1
        assert result["MountTargets"][0]["MountTargetId"] == mt_id
        efs.delete_mount_target(MountTargetId=mt_id)
        efs.delete_file_system(FileSystemId=fs_id)

    def test_describe_mount_targets_empty(self, efs):
        fs_id = _create_fs(efs)
        r = efs.describe_mount_targets(FileSystemId=fs_id)
        assert "MountTargets" in r
        assert len(r["MountTargets"]) == 0
        efs.delete_file_system(FileSystemId=fs_id)


class TestEFSMountTargetSecurityGroupOperations:
    @pytest.fixture(autouse=True)
    def setup_vpc(self, efs):
        """Create a VPC and subnet for mount target security group tests."""
        ec2 = make_client("ec2")
        vpc = ec2.create_vpc(CidrBlock="10.50.0.0/16")
        self.vpc_id = vpc["Vpc"]["VpcId"]
        subnet = ec2.create_subnet(VpcId=self.vpc_id, CidrBlock="10.50.1.0/24")
        self.subnet_id = subnet["Subnet"]["SubnetId"]
        self.ec2 = ec2
        yield
        try:
            ec2.delete_subnet(SubnetId=self.subnet_id)
        except Exception:
            pass  # best-effort cleanup
        try:
            ec2.delete_vpc(VpcId=self.vpc_id)
        except Exception:
            pass  # best-effort cleanup

    def test_modify_and_describe_mount_target_security_groups(self, efs):
        """Modify then describe mount target security groups."""
        fs_id = _create_fs(efs)
        mt = efs.create_mount_target(FileSystemId=fs_id, SubnetId=self.subnet_id)
        mt_id = mt["MountTargetId"]
        sg = self.ec2.create_security_group(
            GroupName=_unique("sg"),
            Description="efs sg test",
            VpcId=self.vpc_id,
        )
        sg_id = sg["GroupId"]
        efs.modify_mount_target_security_groups(MountTargetId=mt_id, SecurityGroups=[sg_id])
        r = efs.describe_mount_target_security_groups(MountTargetId=mt_id)
        assert "SecurityGroups" in r
        assert sg_id in r["SecurityGroups"]
        efs.delete_mount_target(MountTargetId=mt_id)
        efs.delete_file_system(FileSystemId=fs_id)
        self.ec2.delete_security_group(GroupId=sg_id)

    def test_create_mount_target_with_security_group(self, efs):
        """CreateMountTarget with SecurityGroups sets them on the mount target."""
        fs_id = _create_fs(efs)
        sg = self.ec2.create_security_group(
            GroupName=_unique("sg"),
            Description="efs create sg test",
            VpcId=self.vpc_id,
        )
        sg_id = sg["GroupId"]
        mt = efs.create_mount_target(
            FileSystemId=fs_id,
            SubnetId=self.subnet_id,
            SecurityGroups=[sg_id],
        )
        mt_id = mt["MountTargetId"]
        r = efs.describe_mount_target_security_groups(MountTargetId=mt_id)
        assert "SecurityGroups" in r
        assert sg_id in r["SecurityGroups"]
        efs.delete_mount_target(MountTargetId=mt_id)
        efs.delete_file_system(FileSystemId=fs_id)
        self.ec2.delete_security_group(GroupId=sg_id)

    def test_modify_mount_target_security_groups_multiple(self, efs):
        """ModifyMountTargetSecurityGroups can set multiple security groups."""
        fs_id = _create_fs(efs)
        mt = efs.create_mount_target(FileSystemId=fs_id, SubnetId=self.subnet_id)
        mt_id = mt["MountTargetId"]
        sg1 = self.ec2.create_security_group(
            GroupName=_unique("sg1"),
            Description="efs sg test 1",
            VpcId=self.vpc_id,
        )
        sg2 = self.ec2.create_security_group(
            GroupName=_unique("sg2"),
            Description="efs sg test 2",
            VpcId=self.vpc_id,
        )
        sg1_id = sg1["GroupId"]
        sg2_id = sg2["GroupId"]
        efs.modify_mount_target_security_groups(
            MountTargetId=mt_id, SecurityGroups=[sg1_id, sg2_id]
        )
        r = efs.describe_mount_target_security_groups(MountTargetId=mt_id)
        assert "SecurityGroups" in r
        assert sg1_id in r["SecurityGroups"]
        assert sg2_id in r["SecurityGroups"]
        efs.delete_mount_target(MountTargetId=mt_id)
        efs.delete_file_system(FileSystemId=fs_id)
        self.ec2.delete_security_group(GroupId=sg1_id)
        self.ec2.delete_security_group(GroupId=sg2_id)


class TestEFSBackupPolicy:
    def test_describe_backup_policy_not_found(self, efs):
        """DescribeBackupPolicy on a FS with no backup policy raises PolicyNotFound."""
        fs_id = _create_fs(efs)
        with pytest.raises(ClientError) as exc:
            efs.describe_backup_policy(FileSystemId=fs_id)
        assert exc.value.response["Error"]["Code"] == "PolicyNotFound"
        efs.delete_file_system(FileSystemId=fs_id)


class TestEFSAccountPreferences:
    def test_describe_account_preferences(self, efs):
        resp = efs.describe_account_preferences()
        assert "ResourceIdPreference" in resp


class TestEFSNewOperations:
    """Tests for newly implemented EFS operations."""

    def test_delete_file_system_policy(self, efs):
        fs_id = _create_fs(efs)
        efs.put_file_system_policy(
            FileSystemId=fs_id,
            Policy=json.dumps(
                {
                    "Version": "2012-10-17",
                    "Statement": [
                        {
                            "Effect": "Allow",
                            "Principal": {"AWS": "*"},
                            "Action": "elasticfilesystem:ClientMount",
                            "Resource": "*",
                        }
                    ],
                }
            ),
        )
        efs.delete_file_system_policy(FileSystemId=fs_id)
        with pytest.raises(ClientError) as exc:
            efs.describe_file_system_policy(FileSystemId=fs_id)
        assert exc.value.response["Error"]["Code"] == "PolicyNotFound"
        efs.delete_file_system(FileSystemId=fs_id)

    def test_put_backup_policy(self, efs):
        fs_id = _create_fs(efs)
        resp = efs.put_backup_policy(FileSystemId=fs_id, BackupPolicy={"Status": "ENABLED"})
        assert resp["BackupPolicy"]["Status"] == "ENABLED"
        desc = efs.describe_backup_policy(FileSystemId=fs_id)
        assert desc["BackupPolicy"]["Status"] == "ENABLED"
        efs.delete_file_system(FileSystemId=fs_id)

    def test_put_account_preferences(self, efs):
        resp = efs.put_account_preferences(ResourceIdType="SHORT_ID")
        assert "ResourceIdPreference" in resp
        assert resp["ResourceIdPreference"]["ResourceIdType"] == "SHORT_ID"

    def test_update_file_system(self, efs):
        fs_id = _create_fs(efs)
        resp = efs.update_file_system(FileSystemId=fs_id, ThroughputMode="bursting")
        assert resp["FileSystemId"] == fs_id
        assert resp["ThroughputMode"] == "bursting"
        efs.delete_file_system(FileSystemId=fs_id)


class TestEFSReplicationConfigurations:
    def test_describe_replication_configurations(self, efs):
        resp = efs.describe_replication_configurations()
        assert "Replications" in resp


class TestEFSDescribeTags:
    def test_describe_tags(self, efs):
        fs_id = _create_fs(efs, Tags=[{"Key": "Env", "Value": "test"}])
        resp = efs.describe_tags(FileSystemId=fs_id)
        assert "Tags" in resp
        tag_map = {t["Key"]: t["Value"] for t in resp["Tags"]}
        assert tag_map["Env"] == "test"
        efs.delete_file_system(FileSystemId=fs_id)
