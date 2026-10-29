"""EBS Direct API compatibility tests."""

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
