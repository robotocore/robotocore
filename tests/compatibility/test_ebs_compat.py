"""EBS Direct API compatibility tests."""

import base64
import hashlib

import pytest

from tests.compatibility.conftest import make_client


@pytest.fixture
def ebs():
    return make_client("ebs")


class TestEBSOperations:
    def test_start_snapshot(self, ebs):
        resp = ebs.start_snapshot(VolumeSize=1)
        assert "SnapshotId" in resp
        assert "Status" in resp


class TestEBSMissingGapOps:
    """Tests for previously-missing EBS Direct API operations."""

    def test_complete_snapshot(self, ebs):
        """CompleteSnapshot finalizes a started snapshot."""
        snap = ebs.start_snapshot(VolumeSize=1)
        snap_id = snap["SnapshotId"]
        resp = ebs.complete_snapshot(SnapshotId=snap_id, ChangedBlocksCount=0)
        assert resp["Status"] == "completed"

    def test_list_snapshot_blocks_empty(self, ebs):
        """ListSnapshotBlocks returns empty list for new snapshot."""
        snap = ebs.start_snapshot(VolumeSize=1)
        snap_id = snap["SnapshotId"]
        resp = ebs.list_snapshot_blocks(SnapshotId=snap_id)
        assert "Blocks" in resp
        assert "VolumeSize" in resp
        assert resp["VolumeSize"] == 1

    def test_list_changed_blocks(self, ebs):
        """ListChangedBlocks compares two snapshots."""
        snap1 = ebs.start_snapshot(VolumeSize=1)
        snap2 = ebs.start_snapshot(VolumeSize=1)
        resp = ebs.list_changed_blocks(
            FirstSnapshotId=snap1["SnapshotId"],
            SecondSnapshotId=snap2["SnapshotId"],
        )
        assert "ChangedBlocks" in resp
        assert isinstance(resp["ChangedBlocks"], list)

    def test_put_and_get_snapshot_block(self, ebs):
        """PutSnapshotBlock stores a block; GetSnapshotBlock retrieves it."""
        snap = ebs.start_snapshot(VolumeSize=1)
        snap_id = snap["SnapshotId"]
        data = b"A" * 524288  # 512 KiB

        checksum = base64.b64encode(hashlib.sha256(data).digest()).decode()
        ebs.put_snapshot_block(
            SnapshotId=snap_id,
            BlockIndex=0,
            BlockData=data,
            DataLength=len(data),
            Checksum=checksum,
            ChecksumAlgorithm="SHA256",
        )
        resp = ebs.get_snapshot_block(SnapshotId=snap_id, BlockIndex=0, BlockToken=checksum)
        assert resp["DataLength"] == len(data)
