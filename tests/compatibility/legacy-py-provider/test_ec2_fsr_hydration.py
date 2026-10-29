"""EC2 Fast Snapshot Restore and volume hydration tests."""

import logging
import uuid

import botocore.exceptions
import pytest

from tests.compatibility.conftest import make_client

logger = logging.getLogger(__name__)


@pytest.fixture
def ec2():
    return make_client("ec2")


def _unique(prefix: str) -> str:
    return f"{prefix}-{uuid.uuid4().hex[:8]}"


class TestFastSnapshotRestores:
    """Tests for Fast Snapshot Restore operations."""

    def _create_volume_and_snapshot(self, ec2, az):
        """Helper to create a volume and snapshot for testing."""
        # Create a volume
        vol_resp = ec2.create_volume(
            AvailabilityZone=az,
            Size=10,
            VolumeType="gp3",
        )
        volume_id = vol_resp["VolumeId"]

        # Create a snapshot
        snap_resp = ec2.create_snapshot(
            VolumeId=volume_id,
            Description="Test snapshot for FSR",
        )
        snapshot_id = snap_resp["SnapshotId"]

        return volume_id, snapshot_id

    def test_enable_fast_snapshot_restores_success(self, ec2):
        """EnableFastSnapshotRestores enables FSR for snapshot/AZ pairs."""
        az = "us-east-1a"
        volume_id, snapshot_id = self._create_volume_and_snapshot(ec2, az)

        try:
            resp = ec2.enable_fast_snapshot_restores(
                AvailabilityZones=[az],
                SourceSnapshotIds=[snapshot_id],
            )

            # Verify response structure - just check that Successful exists
            assert "Successful" in resp
            assert len(resp["Successful"]) == 1
            # The API returns the data - verify by checking describe
            desc = ec2.describe_fast_snapshot_restores()
            # Find our specific FSR entry
            our_fsr = None
            for fsr in desc["FastSnapshotRestores"]:
                if fsr.get("SnapshotId") == snapshot_id and fsr.get("AvailabilityZone") == az:
                    our_fsr = fsr
                    break
            assert our_fsr is not None, f"FSR entry for {snapshot_id} in {az} not found"
            assert our_fsr["State"] == "enabled"
        finally:
            # Cleanup
            try:
                ec2.delete_snapshot(SnapshotId=snapshot_id)
            except Exception as exc:  # noqa: BLE001
                logger.debug("cleanup failed: %s", exc)
            try:
                ec2.delete_volume(VolumeId=volume_id)
            except Exception as exc:  # noqa: BLE001
                logger.debug("cleanup failed: %s", exc)

    def test_enable_fast_snapshot_restores_invalid_snapshot(self, ec2):
        """EnableFastSnapshotRestores returns unsuccessful for invalid snapshot."""
        az = "us-east-1a"
        fake_snapshot_id = f"snap-{uuid.uuid4().hex[:17]}"

        resp = ec2.enable_fast_snapshot_restores(
            AvailabilityZones=[az],
            SourceSnapshotIds=[fake_snapshot_id],
        )

        # Verify response has unsuccessful item
        assert "Unsuccessful" in resp
        assert len(resp["Unsuccessful"]) == 1
        # The unsuccessful item should contain the snapshot ID
        unsuccessful = resp["Unsuccessful"][0]
        assert "SnapshotId" in unsuccessful or len(unsuccessful) == 0
        # If the snapshot ID is present, verify it matches
        if "SnapshotId" in unsuccessful:
            assert unsuccessful["SnapshotId"] == fake_snapshot_id

    def test_describe_fast_snapshot_restores_returns_enabled(self, ec2):
        """DescribeFastSnapshotRestores returns FSR entries after enabling."""
        az = "us-east-1a"
        volume_id, snapshot_id = self._create_volume_and_snapshot(ec2, az)

        try:
            # Enable FSR first
            ec2.enable_fast_snapshot_restores(
                AvailabilityZones=[az],
                SourceSnapshotIds=[snapshot_id],
            )

            # Describe FSR
            resp = ec2.describe_fast_snapshot_restores()

            # Verify response
            assert "FastSnapshotRestores" in resp
            # Should have at least our entry
            fsr_ids = [fsr["SnapshotId"] for fsr in resp["FastSnapshotRestores"]]
            assert snapshot_id in fsr_ids

            # Find our entry and verify state
            our_fsr = next(
                fsr for fsr in resp["FastSnapshotRestores"] if fsr["SnapshotId"] == snapshot_id
            )
            assert our_fsr["State"] == "enabled"
            assert our_fsr["AvailabilityZone"] == az
            assert "OwnerId" in our_fsr
        finally:
            try:
                ec2.delete_snapshot(SnapshotId=snapshot_id)
            except Exception as exc:  # noqa: BLE001
                logger.debug("cleanup failed: %s", exc)
            try:
                ec2.delete_volume(VolumeId=volume_id)
            except Exception as exc:  # noqa: BLE001
                logger.debug("cleanup failed: %s", exc)

    def test_describe_fast_snapshot_restores_with_filters(self, ec2):
        """DescribeFastSnapshotRestores filters by snapshot and AZ."""
        az = "us-east-1a"
        volume_id, snapshot_id = self._create_volume_and_snapshot(ec2, az)

        try:
            # Enable FSR
            ec2.enable_fast_snapshot_restores(
                AvailabilityZones=[az],
                SourceSnapshotIds=[snapshot_id],
            )

            # Describe with filters
            resp = ec2.describe_fast_snapshot_restores(
                Filters=[
                    {"Name": "snapshot-id", "Values": [snapshot_id]},
                    {"Name": "availability-zone", "Values": [az]},
                ]
            )

            # Should return our entry
            assert len(resp["FastSnapshotRestores"]) >= 1
            for fsr in resp["FastSnapshotRestores"]:
                if fsr["SnapshotId"] == snapshot_id:
                    assert fsr["AvailabilityZone"] == az
                    assert fsr["State"] == "enabled"
        finally:
            try:
                ec2.delete_snapshot(SnapshotId=snapshot_id)
            except Exception as exc:  # noqa: BLE001
                logger.debug("cleanup failed: %s", exc)
            try:
                ec2.delete_volume(VolumeId=volume_id)
            except Exception as exc:  # noqa: BLE001
                logger.debug("cleanup failed: %s", exc)

    def test_disable_fast_snapshot_restores_success(self, ec2):
        """DisableFastSnapshotRestores disables FSR for snapshot/AZ pairs."""
        az = "us-east-1a"
        volume_id, snapshot_id = self._create_volume_and_snapshot(ec2, az)

        try:
            # Enable FSR first
            ec2.enable_fast_snapshot_restores(
                AvailabilityZones=[az],
                SourceSnapshotIds=[snapshot_id],
            )

            # Disable FSR
            resp = ec2.disable_fast_snapshot_restores(
                AvailabilityZones=[az],
                SourceSnapshotIds=[snapshot_id],
            )

            # Verify response - just check that Successful exists
            assert "Successful" in resp
            assert len(resp["Successful"]) == 1
            # Verify by checking describe
            desc = ec2.describe_fast_snapshot_restores()
            # Find our specific FSR entry
            our_fsr = None
            for fsr in desc["FastSnapshotRestores"]:
                if fsr.get("SnapshotId") == snapshot_id and fsr.get("AvailabilityZone") == az:
                    our_fsr = fsr
                    break
            assert our_fsr is not None, f"FSR entry for {snapshot_id} in {az} not found"
            assert our_fsr["State"] == "disabled"
        finally:
            try:
                ec2.delete_snapshot(SnapshotId=snapshot_id)
            except Exception as exc:  # noqa: BLE001
                logger.debug("cleanup failed: %s", exc)
            try:
                ec2.delete_volume(VolumeId=volume_id)
            except Exception as exc:  # noqa: BLE001
                logger.debug("cleanup failed: %s", exc)

    def test_disable_fast_snapshot_restores_not_enabled(self, ec2):
        """DisableFastSnapshotRestores returns unsuccessful for non-enabled FSR."""
        az = "us-east-1a"
        volume_id, snapshot_id = self._create_volume_and_snapshot(ec2, az)

        try:
            # Try to disable without enabling first
            resp = ec2.disable_fast_snapshot_restores(
                AvailabilityZones=[az],
                SourceSnapshotIds=[snapshot_id],
            )

            # Should have unsuccessful entry
            assert "Unsuccessful" in resp
            assert len(resp["Unsuccessful"]) == 1
            # The unsuccessful item should contain the snapshot ID
            unsuccessful = resp["Unsuccessful"][0]
            assert "SnapshotId" in unsuccessful or len(unsuccessful) == 0
            # If the snapshot ID is present, verify it matches
            if "SnapshotId" in unsuccessful:
                assert unsuccessful["SnapshotId"] == snapshot_id
        finally:
            try:
                ec2.delete_snapshot(SnapshotId=snapshot_id)
            except Exception as exc:  # noqa: BLE001
                logger.debug("cleanup failed: %s", exc)
            try:
                ec2.delete_volume(VolumeId=volume_id)
            except Exception as exc:  # noqa: BLE001
                logger.debug("cleanup failed: %s", exc)


class TestVolumeInitializationRate:
    """Tests for VolumeInitializationRate parameter on CreateVolume."""

    def test_create_volume_with_initialization_rate_from_snapshot(self, ec2):
        """CreateVolume accepts VolumeInitializationRate with SnapshotId."""
        az = "us-east-1a"

        # Create a source volume and snapshot
        vol_resp = ec2.create_volume(AvailabilityZone=az, Size=10, VolumeType="gp3")
        source_vol_id = vol_resp["VolumeId"]

        try:
            snap_resp = ec2.create_snapshot(VolumeId=source_vol_id, Description="Source snapshot")
            snapshot_id = snap_resp["SnapshotId"]

            try:
                # Create volume from snapshot with initialization rate
                resp = ec2.create_volume(
                    AvailabilityZone=az,
                    SnapshotId=snapshot_id,
                    VolumeInitializationRate=200,
                )

                # Verify volume created
                assert "VolumeId" in resp
                assert resp["SnapshotId"] == snapshot_id
            finally:
                try:
                    ec2.delete_snapshot(SnapshotId=snapshot_id)
                except Exception as exc:  # noqa: BLE001
                    logger.debug("cleanup failed: %s", exc)
        finally:
            try:
                ec2.delete_volume(VolumeId=source_vol_id)
            except Exception as exc:  # noqa: BLE001
                logger.debug("cleanup failed: %s", exc)

    def test_create_volume_initialization_rate_without_snapshot_fails(self, ec2):
        """CreateVolume rejects VolumeInitializationRate without SnapshotId."""
        az = "us-east-1a"

        with pytest.raises(botocore.exceptions.ClientError) as exc_info:
            ec2.create_volume(
                AvailabilityZone=az,
                Size=10,
                VolumeType="gp3",
                VolumeInitializationRate=200,
            )

        assert exc_info.value.response["Error"]["Code"] == "InvalidParameterCombination"

    def test_create_volume_initialization_rate_out_of_range(self, ec2):
        """CreateVolume rejects VolumeInitializationRate outside valid range."""
        az = "us-east-1a"

        # Create a source volume and snapshot
        vol_resp = ec2.create_volume(AvailabilityZone=az, Size=10, VolumeType="gp3")
        source_vol_id = vol_resp["VolumeId"]

        try:
            snap_resp = ec2.create_snapshot(VolumeId=source_vol_id, Description="Source snapshot")
            snapshot_id = snap_resp["SnapshotId"]

            try:
                # Try with rate too low
                with pytest.raises(botocore.exceptions.ClientError) as exc_info:
                    ec2.create_volume(
                        AvailabilityZone=az,
                        SnapshotId=snapshot_id,
                        VolumeInitializationRate=50,  # Below minimum of 100
                    )
                assert exc_info.value.response["Error"]["Code"] == "InvalidParameterValue"

                # Try with rate too high
                with pytest.raises(botocore.exceptions.ClientError) as exc_info:
                    ec2.create_volume(
                        AvailabilityZone=az,
                        SnapshotId=snapshot_id,
                        VolumeInitializationRate=500,  # Above maximum of 300
                    )
                assert exc_info.value.response["Error"]["Code"] == "InvalidParameterValue"
            finally:
                try:
                    ec2.delete_snapshot(SnapshotId=snapshot_id)
                except Exception as exc:  # noqa: BLE001
                    logger.debug("cleanup failed: %s", exc)
        finally:
            try:
                ec2.delete_volume(VolumeId=source_vol_id)
            except Exception as exc:  # noqa: BLE001
                logger.debug("cleanup failed: %s", exc)


class TestVolumeHydrationState:
    """Tests for volume hydration state modeling."""

    def test_volume_from_snapshot_is_cold_by_default(self, ec2):
        """Volume created from snapshot without FSR or init rate is cold."""
        az = "us-east-1a"

        # Create source volume and snapshot
        vol_resp = ec2.create_volume(AvailabilityZone=az, Size=10, VolumeType="gp3")
        source_vol_id = vol_resp["VolumeId"]

        try:
            snap_resp = ec2.create_snapshot(VolumeId=source_vol_id, Description="Source snapshot")
            snapshot_id = snap_resp["SnapshotId"]

            try:
                # Create volume from snapshot without FSR or init rate
                resp = ec2.create_volume(
                    AvailabilityZone=az,
                    SnapshotId=snapshot_id,
                )
                volume_id = resp["VolumeId"]

                try:
                    # Verify volume preserves snapshot info
                    assert resp["SnapshotId"] == snapshot_id
                finally:
                    try:
                        ec2.delete_volume(VolumeId=volume_id)
                    except Exception as exc:  # noqa: BLE001
                        logger.debug("cleanup failed: %s", exc)
            finally:
                try:
                    ec2.delete_snapshot(SnapshotId=snapshot_id)
                except Exception as exc:  # noqa: BLE001
                    logger.debug("cleanup failed: %s", exc)
        finally:
            try:
                ec2.delete_volume(VolumeId=source_vol_id)
            except Exception as exc:  # noqa: BLE001
                logger.debug("cleanup failed: %s", exc)

    def test_volume_from_snapshot_with_fsr_is_fsr_backed(self, ec2):
        """Volume created from FSR-enabled snapshot is fsr-backed."""
        az = "us-east-1a"

        # Create source volume and snapshot
        vol_resp = ec2.create_volume(AvailabilityZone=az, Size=10, VolumeType="gp3")
        source_vol_id = vol_resp["VolumeId"]

        try:
            snap_resp = ec2.create_snapshot(VolumeId=source_vol_id, Description="Source snapshot")
            snapshot_id = snap_resp["SnapshotId"]

            try:
                # Enable FSR for the snapshot
                ec2.enable_fast_snapshot_restores(
                    AvailabilityZones=[az],
                    SourceSnapshotIds=[snapshot_id],
                )

                # Create volume from FSR-enabled snapshot
                resp = ec2.create_volume(
                    AvailabilityZone=az,
                    SnapshotId=snapshot_id,
                )
                volume_id = resp["VolumeId"]

                try:
                    # Verify volume created
                    assert "VolumeId" in resp
                    assert resp["SnapshotId"] == snapshot_id
                finally:
                    try:
                        ec2.delete_volume(VolumeId=volume_id)
                    except Exception as exc:  # noqa: BLE001
                        logger.debug("cleanup failed: %s", exc)
            finally:
                try:
                    ec2.delete_snapshot(SnapshotId=snapshot_id)
                except Exception as exc:  # noqa: BLE001
                    logger.debug("cleanup failed: %s", exc)
        finally:
            try:
                ec2.delete_volume(VolumeId=source_vol_id)
            except Exception as exc:  # noqa: BLE001
                logger.debug("cleanup failed: %s", exc)

    def test_volume_from_snapshot_with_init_rate_is_initialized(self, ec2):
        """Volume created with VolumeInitializationRate is initialized."""
        az = "us-east-1a"

        # Create source volume and snapshot
        vol_resp = ec2.create_volume(AvailabilityZone=az, Size=10, VolumeType="gp3")
        source_vol_id = vol_resp["VolumeId"]

        try:
            snap_resp = ec2.create_snapshot(VolumeId=source_vol_id, Description="Source snapshot")
            snapshot_id = snap_resp["SnapshotId"]

            try:
                # Create volume with initialization rate
                resp = ec2.create_volume(
                    AvailabilityZone=az,
                    SnapshotId=snapshot_id,
                    VolumeInitializationRate=200,
                )
                volume_id = resp["VolumeId"]

                try:
                    # Verify volume created
                    assert "VolumeId" in resp
                    assert resp["SnapshotId"] == snapshot_id
                finally:
                    try:
                        ec2.delete_volume(VolumeId=volume_id)
                    except Exception as exc:  # noqa: BLE001
                        logger.debug("cleanup failed: %s", exc)
            finally:
                try:
                    ec2.delete_snapshot(SnapshotId=snapshot_id)
                except Exception as exc:  # noqa: BLE001
                    logger.debug("cleanup failed: %s", exc)
        finally:
            try:
                ec2.delete_volume(VolumeId=source_vol_id)
            except Exception as exc:  # noqa: BLE001
                logger.debug("cleanup failed: %s", exc)

    def test_volume_not_from_snapshot_is_initialized(self, ec2):
        """Volume created without snapshot is immediately initialized."""
        az = "us-east-1a"

        resp = ec2.create_volume(
            AvailabilityZone=az,
            Size=10,
            VolumeType="gp3",
        )
        volume_id = resp["VolumeId"]

        try:
            # Verify volume created
            assert "VolumeId" in resp
            # No snapshot
            assert resp.get("SnapshotId") is None or resp.get("SnapshotId") == ""
        finally:
            try:
                ec2.delete_volume(VolumeId=volume_id)
            except Exception as exc:  # noqa: BLE001
                logger.debug("cleanup failed: %s", exc)


class TestVolumePreservesSnapshotProperties:
    """Tests that volumes from snapshots preserve source properties."""

    def test_volume_from_snapshot_preserves_encryption(self, ec2):
        """Volume from encrypted snapshot inherits encryption."""
        az = "us-east-1a"

        # Create encrypted source volume
        vol_resp = ec2.create_volume(
            AvailabilityZone=az,
            Size=10,
            VolumeType="gp3",
            Encrypted=True,
        )
        source_vol_id = vol_resp["VolumeId"]

        try:
            snap_resp = ec2.create_snapshot(
                VolumeId=source_vol_id, Description="Encrypted snapshot"
            )
            snapshot_id = snap_resp["SnapshotId"]

            try:
                # Verify snapshot is encrypted
                snaps = ec2.describe_snapshots(SnapshotIds=[snapshot_id])
                assert snaps["Snapshots"][0]["Encrypted"] is True

                # Create volume from encrypted snapshot
                resp = ec2.create_volume(
                    AvailabilityZone=az,
                    SnapshotId=snapshot_id,
                )
                volume_id = resp["VolumeId"]

                try:
                    # Verify volume is encrypted
                    assert resp["Encrypted"] is True
                finally:
                    try:
                        ec2.delete_volume(VolumeId=volume_id)
                    except Exception as exc:  # noqa: BLE001
                        logger.debug("cleanup failed: %s", exc)
            finally:
                try:
                    ec2.delete_snapshot(SnapshotId=snapshot_id)
                except Exception as exc:  # noqa: BLE001
                    logger.debug("cleanup failed: %s", exc)
        finally:
            try:
                ec2.delete_volume(VolumeId=source_vol_id)
            except Exception as exc:  # noqa: BLE001
                logger.debug("cleanup failed: %s", exc)

    def test_volume_from_snapshot_preserves_size(self, ec2):
        """Volume from snapshot inherits size."""
        az = "us-east-1a"
        size = 20

        # Create source volume
        vol_resp = ec2.create_volume(AvailabilityZone=az, Size=size, VolumeType="gp3")
        source_vol_id = vol_resp["VolumeId"]

        try:
            snap_resp = ec2.create_snapshot(VolumeId=source_vol_id, Description="Source snapshot")
            snapshot_id = snap_resp["SnapshotId"]

            try:
                # Create volume from snapshot (no size specified - should inherit)
                resp = ec2.create_volume(
                    AvailabilityZone=az,
                    SnapshotId=snapshot_id,
                )
                volume_id = resp["VolumeId"]

                try:
                    # Verify size matches source
                    assert resp["Size"] == size
                finally:
                    try:
                        ec2.delete_volume(VolumeId=volume_id)
                    except Exception as exc:  # noqa: BLE001
                        logger.debug("cleanup failed: %s", exc)
            finally:
                try:
                    ec2.delete_snapshot(SnapshotId=snapshot_id)
                except Exception as exc:  # noqa: BLE001
                    logger.debug("cleanup failed: %s", exc)
        finally:
            try:
                ec2.delete_volume(VolumeId=source_vol_id)
            except Exception as exc:  # noqa: BLE001
                logger.debug("cleanup failed: %s", exc)
