"""GuardDuty compatibility tests."""

import uuid

import pytest

from tests.compatibility.conftest import make_client


@pytest.fixture
def guardduty():
    return make_client("guardduty")


def _unique(prefix):
    return f"{prefix}-{uuid.uuid4().hex[:8]}"


class TestGuardDutyDetectorOperations:
    def test_create_and_get_detector(self, guardduty):
        resp = guardduty.create_detector(Enable=True)
        detector_id = resp["DetectorId"]
        assert detector_id

        detail = guardduty.get_detector(DetectorId=detector_id)
        assert detail["Status"] == "ENABLED"

        guardduty.delete_detector(DetectorId=detector_id)

    def test_list_detectors(self, guardduty):
        resp = guardduty.create_detector(Enable=True)
        detector_id = resp["DetectorId"]

        listed = guardduty.list_detectors()
        assert detector_id in listed["DetectorIds"]

        guardduty.delete_detector(DetectorId=detector_id)

    def test_delete_detector(self, guardduty):
        resp = guardduty.create_detector(Enable=True)
        detector_id = resp["DetectorId"]

        guardduty.delete_detector(DetectorId=detector_id)

        listed = guardduty.list_detectors()
        assert detector_id not in listed["DetectorIds"]


class TestGuardDutyFilterOperations:
    def test_create_and_get_filter(self, guardduty):
        resp = guardduty.create_detector(Enable=True)
        detector_id = resp["DetectorId"]
        filter_name = _unique("filter")

        guardduty.create_filter(
            DetectorId=detector_id,
            Name=filter_name,
            FindingCriteria={"Criterion": {"severity": {"Gte": 4}}},
        )

        detail = guardduty.get_filter(DetectorId=detector_id, FilterName=filter_name)
        assert detail["Name"] == filter_name
        assert "FindingCriteria" in detail

        guardduty.delete_filter(DetectorId=detector_id, FilterName=filter_name)
        guardduty.delete_detector(DetectorId=detector_id)


class TestGuardDutyListOperations:
    def test_list_organization_admin_accounts(self, guardduty):
        resp = guardduty.list_organization_admin_accounts()
        assert "AdminAccounts" in resp
