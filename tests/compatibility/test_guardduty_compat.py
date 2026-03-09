"""GuardDuty compatibility tests."""

import uuid

import pytest
from botocore.exceptions import ClientError

from tests.compatibility.conftest import make_client


@pytest.fixture
def guardduty():
    return make_client("guardduty")


@pytest.fixture
def detector(guardduty):
    """Create a detector and clean up after test."""
    resp = guardduty.create_detector(Enable=True)
    detector_id = resp["DetectorId"]
    yield detector_id
    try:
        guardduty.delete_detector(DetectorId=detector_id)
    except ClientError:
        pass


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

    def test_update_detector(self, guardduty):
        resp = guardduty.create_detector(Enable=True)
        detector_id = resp["DetectorId"]
        try:
            guardduty.update_detector(DetectorId=detector_id, Enable=False)
            detail = guardduty.get_detector(DetectorId=detector_id)
            assert detail["Status"] == "DISABLED"
        finally:
            guardduty.delete_detector(DetectorId=detector_id)

    def test_get_detector_returns_created_at(self, guardduty, detector):
        detail = guardduty.get_detector(DetectorId=detector)
        assert "CreatedAt" in detail
        assert detail["CreatedAt"]

    def test_get_detector_returns_updated_at(self, guardduty, detector):
        detail = guardduty.get_detector(DetectorId=detector)
        assert "UpdatedAt" in detail
        assert detail["UpdatedAt"]

    def test_get_detector_returns_service_role(self, guardduty, detector):
        detail = guardduty.get_detector(DetectorId=detector)
        assert "ServiceRole" in detail

    def test_get_detector_returns_data_sources(self, guardduty, detector):
        detail = guardduty.get_detector(DetectorId=detector)
        assert "DataSources" in detail

    def test_get_detector_returns_tags(self, guardduty, detector):
        detail = guardduty.get_detector(DetectorId=detector)
        assert "Tags" in detail

    def test_get_detector_returns_features(self, guardduty, detector):
        detail = guardduty.get_detector(DetectorId=detector)
        assert "Features" in detail

    def test_create_detector_with_finding_publishing_frequency(self, guardduty):
        resp = guardduty.create_detector(Enable=True, FindingPublishingFrequency="ONE_HOUR")
        detector_id = resp["DetectorId"]
        try:
            detail = guardduty.get_detector(DetectorId=detector_id)
            assert detail["FindingPublishingFrequency"] == "ONE_HOUR"
        finally:
            guardduty.delete_detector(DetectorId=detector_id)

    def test_update_detector_finding_publishing_frequency(self, guardduty, detector):
        guardduty.update_detector(
            DetectorId=detector,
            Enable=True,
            FindingPublishingFrequency="SIX_HOURS",
        )
        detail = guardduty.get_detector(DetectorId=detector)
        assert detail["FindingPublishingFrequency"] == "SIX_HOURS"

    def test_create_detector_with_data_sources(self, guardduty):
        resp = guardduty.create_detector(Enable=True, DataSources={"S3Logs": {"Enable": True}})
        detector_id = resp["DetectorId"]
        try:
            detail = guardduty.get_detector(DetectorId=detector_id)
            assert detail["DataSources"]["S3Logs"]["Status"] == "ENABLED"
        finally:
            guardduty.delete_detector(DetectorId=detector_id)

    def test_get_nonexistent_detector_raises_error(self, guardduty):
        with pytest.raises(ClientError) as exc_info:
            guardduty.get_detector(DetectorId="aaaabbbbccccddddeeeeffffgggghhh0")
        assert exc_info.value.response["Error"]["Code"] == "BadRequestException"

    def test_list_detectors_multiple(self, guardduty):
        resp1 = guardduty.create_detector(Enable=True)
        resp2 = guardduty.create_detector(Enable=True)
        try:
            listed = guardduty.list_detectors()
            assert resp1["DetectorId"] in listed["DetectorIds"]
            assert resp2["DetectorId"] in listed["DetectorIds"]
        finally:
            guardduty.delete_detector(DetectorId=resp1["DetectorId"])
            guardduty.delete_detector(DetectorId=resp2["DetectorId"])


class TestGuardDutyAdministratorAccountOperations:
    def test_get_administrator_account(self, guardduty):
        resp = guardduty.create_detector(Enable=True)
        detector_id = resp["DetectorId"]
        try:
            result = guardduty.get_administrator_account(DetectorId=detector_id)
            # Response returns 200 with metadata (no administrator set)
            assert result["ResponseMetadata"]["HTTPStatusCode"] == 200
        finally:
            guardduty.delete_detector(DetectorId=detector_id)

    def test_get_administrator_account_no_admin_set(self, guardduty, detector):
        result = guardduty.get_administrator_account(DetectorId=detector)
        # When no administrator is configured, the Administrator key is absent
        assert result["ResponseMetadata"]["HTTPStatusCode"] == 200


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

    def test_update_filter(self, guardduty):
        resp = guardduty.create_detector(Enable=True)
        detector_id = resp["DetectorId"]
        filter_name = _unique("filter")
        guardduty.create_filter(
            DetectorId=detector_id,
            Name=filter_name,
            FindingCriteria={"Criterion": {"severity": {"Gte": 4}}},
        )
        try:
            guardduty.update_filter(
                DetectorId=detector_id,
                FilterName=filter_name,
                FindingCriteria={"Criterion": {"severity": {"Gte": 7}}},
            )
            detail = guardduty.get_filter(DetectorId=detector_id, FilterName=filter_name)
            assert detail["FindingCriteria"]["Criterion"]["severity"]["Gte"] == 7
        finally:
            guardduty.delete_filter(DetectorId=detector_id, FilterName=filter_name)
            guardduty.delete_detector(DetectorId=detector_id)

    def test_create_filter_with_description(self, guardduty, detector):
        filter_name = _unique("filter")
        guardduty.create_filter(
            DetectorId=detector,
            Name=filter_name,
            Description="A test filter",
            FindingCriteria={"Criterion": {"severity": {"Gte": 4}}},
        )
        try:
            detail = guardduty.get_filter(DetectorId=detector, FilterName=filter_name)
            assert detail["Description"] == "A test filter"
        finally:
            guardduty.delete_filter(DetectorId=detector, FilterName=filter_name)

    def test_create_filter_with_action(self, guardduty, detector):
        filter_name = _unique("filter")
        guardduty.create_filter(
            DetectorId=detector,
            Name=filter_name,
            Action="ARCHIVE",
            FindingCriteria={"Criterion": {"severity": {"Gte": 4}}},
        )
        try:
            detail = guardduty.get_filter(DetectorId=detector, FilterName=filter_name)
            assert detail["Action"] == "ARCHIVE"
        finally:
            guardduty.delete_filter(DetectorId=detector, FilterName=filter_name)

    def test_create_filter_with_rank(self, guardduty, detector):
        filter_name = _unique("filter")
        guardduty.create_filter(
            DetectorId=detector,
            Name=filter_name,
            Rank=5,
            FindingCriteria={"Criterion": {"severity": {"Gte": 4}}},
        )
        try:
            detail = guardduty.get_filter(DetectorId=detector, FilterName=filter_name)
            assert detail["Rank"] == 5
        finally:
            guardduty.delete_filter(DetectorId=detector, FilterName=filter_name)

    def test_delete_filter(self, guardduty, detector):
        filter_name = _unique("filter")
        guardduty.create_filter(
            DetectorId=detector,
            Name=filter_name,
            FindingCriteria={"Criterion": {"severity": {"Gte": 4}}},
        )
        guardduty.delete_filter(DetectorId=detector, FilterName=filter_name)
        with pytest.raises(ClientError) as exc_info:
            guardduty.get_filter(DetectorId=detector, FilterName=filter_name)
        assert exc_info.value.response["Error"]["Code"] == "BadRequestException"

    def test_get_nonexistent_filter_raises_error(self, guardduty, detector):
        with pytest.raises(ClientError) as exc_info:
            guardduty.get_filter(DetectorId=detector, FilterName="nonexistent")
        assert exc_info.value.response["Error"]["Code"] == "BadRequestException"

    def test_update_nonexistent_filter_raises_error(self, guardduty, detector):
        with pytest.raises(ClientError) as exc_info:
            guardduty.update_filter(
                DetectorId=detector,
                FilterName="nonexistent",
                FindingCriteria={"Criterion": {"severity": {"Gte": 4}}},
            )
        assert exc_info.value.response["Error"]["Code"] == "BadRequestException"

    def test_create_filter_with_all_options(self, guardduty, detector):
        filter_name = _unique("filter")
        guardduty.create_filter(
            DetectorId=detector,
            Name=filter_name,
            Description="Full options filter",
            Action="NOOP",
            Rank=3,
            FindingCriteria={"Criterion": {"severity": {"Gte": 2}}},
        )
        try:
            detail = guardduty.get_filter(DetectorId=detector, FilterName=filter_name)
            assert detail["Name"] == filter_name
            assert detail["Description"] == "Full options filter"
            assert detail["Action"] == "NOOP"
            assert detail["Rank"] == 3
            assert detail["FindingCriteria"]["Criterion"]["severity"]["Gte"] == 2
        finally:
            guardduty.delete_filter(DetectorId=detector, FilterName=filter_name)


class TestGuardDutyOrganizationAdminAccountOperations:
    def test_list_organization_admin_accounts(self, guardduty):
        resp = guardduty.list_organization_admin_accounts()
        assert "AdminAccounts" in resp

    def test_enable_organization_admin_account(self, guardduty):
        resp = guardduty.enable_organization_admin_account(AdminAccountId="111122223333")
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200
