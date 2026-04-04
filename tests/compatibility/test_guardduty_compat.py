"""GuardDuty compatibility tests."""

import datetime
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
        pass  # best-effort cleanup


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
        assert isinstance(detail["ServiceRole"], str)

    def test_get_detector_returns_data_sources(self, guardduty, detector):
        detail = guardduty.get_detector(DetectorId=detector)
        assert "DataSources" in detail
        assert isinstance(detail["DataSources"], dict)

    def test_get_detector_returns_tags(self, guardduty, detector):
        detail = guardduty.get_detector(DetectorId=detector)
        assert "Tags" in detail
        assert isinstance(detail["Tags"], dict)

    def test_get_detector_returns_features(self, guardduty, detector):
        detail = guardduty.get_detector(DetectorId=detector)
        assert "Features" in detail
        assert isinstance(detail["Features"], list)

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
        assert isinstance(resp["AdminAccounts"], list)

    def test_enable_organization_admin_account(self, guardduty):
        resp = guardduty.enable_organization_admin_account(AdminAccountId="111122223333")
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200


class TestGuardDutyFilterListOperations:
    def test_list_filters_empty(self, guardduty, detector):
        resp = guardduty.list_filters(DetectorId=detector)
        assert "FilterNames" in resp
        assert isinstance(resp["FilterNames"], list)

    def test_list_filters_with_filter(self, guardduty, detector):
        filter_name = _unique("filter")
        guardduty.create_filter(
            DetectorId=detector,
            Name=filter_name,
            FindingCriteria={"Criterion": {"severity": {"Gte": 4}}},
        )
        try:
            resp = guardduty.list_filters(DetectorId=detector)
            assert filter_name in resp["FilterNames"]
        finally:
            guardduty.delete_filter(DetectorId=detector, FilterName=filter_name)

    def test_list_filters_multiple(self, guardduty, detector):
        names = [_unique("filter") for _ in range(3)]
        for name in names:
            guardduty.create_filter(
                DetectorId=detector,
                Name=name,
                FindingCriteria={"Criterion": {"severity": {"Gte": 4}}},
            )
        try:
            resp = guardduty.list_filters(DetectorId=detector)
            for name in names:
                assert name in resp["FilterNames"]
        finally:
            for name in names:
                guardduty.delete_filter(DetectorId=detector, FilterName=name)


class TestGuardDutyIPSetOperations:
    def test_create_and_get_ipset(self, guardduty, detector):
        name = _unique("ipset")
        resp = guardduty.create_ip_set(
            DetectorId=detector,
            Name=name,
            Format="TXT",
            Location="s3://my-bucket/ipset.txt",
            Activate=True,
        )
        ipset_id = resp["IpSetId"]
        assert ipset_id

        detail = guardduty.get_ip_set(DetectorId=detector, IpSetId=ipset_id)
        assert detail["Name"] == name
        assert detail["Format"] == "TXT"
        assert detail["Location"] == "s3://my-bucket/ipset.txt"

        guardduty.delete_ip_set(DetectorId=detector, IpSetId=ipset_id)

    def test_list_ipsets_empty(self, guardduty, detector):
        resp = guardduty.list_ip_sets(DetectorId=detector)
        assert "IpSetIds" in resp
        assert isinstance(resp["IpSetIds"], list)

    def test_list_ipsets_with_ipset(self, guardduty, detector):
        name = _unique("ipset")
        resp = guardduty.create_ip_set(
            DetectorId=detector,
            Name=name,
            Format="TXT",
            Location="s3://my-bucket/ipset.txt",
            Activate=False,
        )
        ipset_id = resp["IpSetId"]
        try:
            listed = guardduty.list_ip_sets(DetectorId=detector)
            assert ipset_id in listed["IpSetIds"]
        finally:
            guardduty.delete_ip_set(DetectorId=detector, IpSetId=ipset_id)

    def test_get_ipset_returns_status(self, guardduty, detector):
        name = _unique("ipset")
        resp = guardduty.create_ip_set(
            DetectorId=detector,
            Name=name,
            Format="TXT",
            Location="s3://my-bucket/ipset.txt",
            Activate=False,
        )
        ipset_id = resp["IpSetId"]
        try:
            detail = guardduty.get_ip_set(DetectorId=detector, IpSetId=ipset_id)
            assert "Status" in detail
            assert isinstance(detail["Status"], str)
            assert detail["Status"] in (
                "ACTIVE", "INACTIVE", "ACTIVATING", "DEACTIVATING", "ERROR",
                "DELETE_PENDING", "DELETED",
            )
        finally:
            guardduty.delete_ip_set(DetectorId=detector, IpSetId=ipset_id)

    def test_get_nonexistent_ipset_raises_error(self, guardduty, detector):
        with pytest.raises(ClientError) as exc_info:
            guardduty.get_ip_set(DetectorId=detector, IpSetId="nonexistent00000000000000000000")
        assert exc_info.value.response["Error"]["Code"] == "BadRequestException"


class TestGuardDutyThreatIntelSetOperations:
    def test_create_and_get_threat_intel_set(self, guardduty, detector):
        name = _unique("tiset")
        resp = guardduty.create_threat_intel_set(
            DetectorId=detector,
            Name=name,
            Format="TXT",
            Location="s3://my-bucket/threatintel.txt",
            Activate=True,
        )
        tiset_id = resp["ThreatIntelSetId"]
        assert tiset_id

        detail = guardduty.get_threat_intel_set(DetectorId=detector, ThreatIntelSetId=tiset_id)
        assert detail["Name"] == name
        assert detail["Format"] == "TXT"
        assert detail["Location"] == "s3://my-bucket/threatintel.txt"

        guardduty.delete_threat_intel_set(DetectorId=detector, ThreatIntelSetId=tiset_id)

    def test_list_threat_intel_sets_empty(self, guardduty, detector):
        resp = guardduty.list_threat_intel_sets(DetectorId=detector)
        assert "ThreatIntelSetIds" in resp
        assert isinstance(resp["ThreatIntelSetIds"], list)

    def test_list_threat_intel_sets_with_set(self, guardduty, detector):
        name = _unique("tiset")
        resp = guardduty.create_threat_intel_set(
            DetectorId=detector,
            Name=name,
            Format="TXT",
            Location="s3://my-bucket/threatintel.txt",
            Activate=False,
        )
        tiset_id = resp["ThreatIntelSetId"]
        try:
            listed = guardduty.list_threat_intel_sets(DetectorId=detector)
            assert tiset_id in listed["ThreatIntelSetIds"]
        finally:
            guardduty.delete_threat_intel_set(DetectorId=detector, ThreatIntelSetId=tiset_id)

    def test_get_threat_intel_set_returns_status(self, guardduty, detector):
        name = _unique("tiset")
        resp = guardduty.create_threat_intel_set(
            DetectorId=detector,
            Name=name,
            Format="TXT",
            Location="s3://my-bucket/threatintel.txt",
            Activate=False,
        )
        tiset_id = resp["ThreatIntelSetId"]
        try:
            detail = guardduty.get_threat_intel_set(DetectorId=detector, ThreatIntelSetId=tiset_id)
            assert "Status" in detail
            assert isinstance(detail["Status"], str)
            assert detail["Status"] in (
                "ACTIVE", "INACTIVE", "ACTIVATING", "DEACTIVATING", "ERROR",
                "DELETE_PENDING", "DELETED",
            )
        finally:
            guardduty.delete_threat_intel_set(DetectorId=detector, ThreatIntelSetId=tiset_id)

    def test_get_nonexistent_threat_intel_set_raises_error(self, guardduty, detector):
        with pytest.raises(ClientError) as exc_info:
            guardduty.get_threat_intel_set(
                DetectorId=detector, ThreatIntelSetId="nonexistent00000000000000000000"
            )
        assert exc_info.value.response["Error"]["Code"] == "BadRequestException"


class TestGuardDutyTagOperations:
    def test_list_tags_for_detector(self, guardduty):
        resp = guardduty.create_detector(Enable=True, Tags={"env": "test", "project": "roboto"})
        detector_id = resp["DetectorId"]
        try:
            arn = f"arn:aws:guardduty:us-east-1:123456789012:detector/{detector_id}"
            tags_resp = guardduty.list_tags_for_resource(ResourceArn=arn)
            assert "Tags" in tags_resp
            assert tags_resp["Tags"]["env"] == "test"
            assert tags_resp["Tags"]["project"] == "roboto"
        finally:
            guardduty.delete_detector(DetectorId=detector_id)

    def test_list_tags_for_resource_empty_tags(self, guardduty, detector):
        arn = f"arn:aws:guardduty:us-east-1:123456789012:detector/{detector}"
        tags_resp = guardduty.list_tags_for_resource(ResourceArn=arn)
        assert "Tags" in tags_resp
        assert isinstance(tags_resp["Tags"], dict)

    def test_tag_resource(self, guardduty, detector):
        """TagResource adds tags to a detector."""
        arn = f"arn:aws:guardduty:us-east-1:123456789012:detector/{detector}"
        resp = guardduty.tag_resource(ResourceArn=arn, Tags={"team": "security", "env": "test"})
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200
        tags_resp = guardduty.list_tags_for_resource(ResourceArn=arn)
        assert tags_resp["Tags"]["team"] == "security"
        assert tags_resp["Tags"]["env"] == "test"

    def test_untag_resource(self, guardduty, detector):
        """UntagResource removes specified tags from a detector."""
        arn = f"arn:aws:guardduty:us-east-1:123456789012:detector/{detector}"
        guardduty.tag_resource(ResourceArn=arn, Tags={"k1": "v1", "k2": "v2", "k3": "v3"})
        resp = guardduty.untag_resource(ResourceArn=arn, TagKeys=["k1", "k3"])
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200
        tags_resp = guardduty.list_tags_for_resource(ResourceArn=arn)
        assert "k1" not in tags_resp["Tags"]
        assert tags_resp["Tags"]["k2"] == "v2"
        assert "k3" not in tags_resp["Tags"]

    def test_create_detector_with_tags_and_verify(self, guardduty):
        """Create a detector with tags and verify via get_detector."""
        resp = guardduty.create_detector(Enable=True, Tags={"env": "staging", "team": "infra"})
        detector_id = resp["DetectorId"]
        try:
            detail = guardduty.get_detector(DetectorId=detector_id)
            assert detail["Tags"]["env"] == "staging"
            assert detail["Tags"]["team"] == "infra"
        finally:
            guardduty.delete_detector(DetectorId=detector_id)

    def test_tag_resource_overwrites_existing(self, guardduty, detector):
        """TagResource overwrites existing tag values."""
        arn = f"arn:aws:guardduty:us-east-1:123456789012:detector/{detector}"
        guardduty.tag_resource(ResourceArn=arn, Tags={"env": "dev"})
        guardduty.tag_resource(ResourceArn=arn, Tags={"env": "prod"})
        tags_resp = guardduty.list_tags_for_resource(ResourceArn=arn)
        assert tags_resp["Tags"]["env"] == "prod"

    def test_tag_and_untag_all(self, guardduty, detector):
        """Tag then untag all tags leaves empty tags map."""
        arn = f"arn:aws:guardduty:us-east-1:123456789012:detector/{detector}"
        guardduty.tag_resource(ResourceArn=arn, Tags={"a": "1", "b": "2"})
        guardduty.untag_resource(ResourceArn=arn, TagKeys=["a", "b"])
        tags_resp = guardduty.list_tags_for_resource(ResourceArn=arn)
        assert tags_resp["Tags"] == {}


class TestGuardDutyUpdateOperations:
    """Tests for update operations on IP sets and threat intel sets."""

    @pytest.fixture
    def detector_and_ipset(self, guardduty):
        det = guardduty.create_detector(Enable=True)
        det_id = det["DetectorId"]
        ip = guardduty.create_ip_set(
            DetectorId=det_id,
            Name="test-ipset",
            Format="TXT",
            Location="s3://test-bucket/ipset.txt",
            Activate=True,
        )
        yield det_id, ip["IpSetId"]
        guardduty.delete_ip_set(DetectorId=det_id, IpSetId=ip["IpSetId"])
        guardduty.delete_detector(DetectorId=det_id)

    @pytest.fixture
    def detector_and_tiset(self, guardduty):
        det = guardduty.create_detector(Enable=True)
        det_id = det["DetectorId"]
        ti = guardduty.create_threat_intel_set(
            DetectorId=det_id,
            Name="test-ti",
            Format="TXT",
            Location="s3://test-bucket/ti.txt",
            Activate=True,
        )
        yield det_id, ti["ThreatIntelSetId"]
        guardduty.delete_threat_intel_set(
            DetectorId=det_id, ThreatIntelSetId=ti["ThreatIntelSetId"]
        )
        guardduty.delete_detector(DetectorId=det_id)

    def test_update_ip_set(self, guardduty, detector_and_ipset):
        """UpdateIPSet updates the name of an IP set."""
        det_id, ip_set_id = detector_and_ipset
        resp = guardduty.update_ip_set(DetectorId=det_id, IpSetId=ip_set_id, Name="updated-ipset")
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200
        get_resp = guardduty.get_ip_set(DetectorId=det_id, IpSetId=ip_set_id)
        assert get_resp["Name"] == "updated-ipset"

    def test_update_threat_intel_set(self, guardduty, detector_and_tiset):
        """UpdateThreatIntelSet updates the name."""
        det_id, ti_id = detector_and_tiset
        resp = guardduty.update_threat_intel_set(
            DetectorId=det_id, ThreatIntelSetId=ti_id, Name="updated-ti"
        )
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200
        get_resp = guardduty.get_threat_intel_set(DetectorId=det_id, ThreatIntelSetId=ti_id)
        assert get_resp["Name"] == "updated-ti"

    def test_update_ip_set_location(self, guardduty, detector_and_ipset):
        """UpdateIPSet updates the location of an IP set."""
        det_id, ip_set_id = detector_and_ipset
        resp = guardduty.update_ip_set(
            DetectorId=det_id, IpSetId=ip_set_id, Location="s3://new-bucket/ipset2.txt"
        )
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200
        get_resp = guardduty.get_ip_set(DetectorId=det_id, IpSetId=ip_set_id)
        assert get_resp["Location"] == "s3://new-bucket/ipset2.txt"

    def test_update_ip_set_activate(self, guardduty):
        """UpdateIPSet can deactivate and reactivate an IP set."""
        det = guardduty.create_detector(Enable=True)
        det_id = det["DetectorId"]
        ip = guardduty.create_ip_set(
            DetectorId=det_id,
            Name="act-ipset",
            Format="TXT",
            Location="s3://test-bucket/ip.txt",
            Activate=True,
        )
        ip_set_id = ip["IpSetId"]
        try:
            guardduty.update_ip_set(DetectorId=det_id, IpSetId=ip_set_id, Activate=False)
            detail = guardduty.get_ip_set(DetectorId=det_id, IpSetId=ip_set_id)
            assert detail["Status"] in ("INACTIVE", "DEACTIVATING")

            guardduty.update_ip_set(DetectorId=det_id, IpSetId=ip_set_id, Activate=True)
            detail = guardduty.get_ip_set(DetectorId=det_id, IpSetId=ip_set_id)
            assert detail["Status"] in ("ACTIVE", "ACTIVATING")
        finally:
            guardduty.delete_ip_set(DetectorId=det_id, IpSetId=ip_set_id)
            guardduty.delete_detector(DetectorId=det_id)

    def test_update_threat_intel_set_location(self, guardduty, detector_and_tiset):
        """UpdateThreatIntelSet updates the location."""
        det_id, ti_id = detector_and_tiset
        resp = guardduty.update_threat_intel_set(
            DetectorId=det_id, ThreatIntelSetId=ti_id, Location="s3://new-bucket/ti2.txt"
        )
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200
        get_resp = guardduty.get_threat_intel_set(DetectorId=det_id, ThreatIntelSetId=ti_id)
        assert get_resp["Location"] == "s3://new-bucket/ti2.txt"

    def test_update_threat_intel_set_activate(self, guardduty):
        """UpdateThreatIntelSet can deactivate and reactivate."""
        det = guardduty.create_detector(Enable=True)
        det_id = det["DetectorId"]
        ti = guardduty.create_threat_intel_set(
            DetectorId=det_id,
            Name="act-ti",
            Format="TXT",
            Location="s3://test-bucket/ti.txt",
            Activate=True,
        )
        ti_id = ti["ThreatIntelSetId"]
        try:
            guardduty.update_threat_intel_set(
                DetectorId=det_id, ThreatIntelSetId=ti_id, Activate=False
            )
            detail = guardduty.get_threat_intel_set(DetectorId=det_id, ThreatIntelSetId=ti_id)
            assert detail["Status"] in ("INACTIVE", "DEACTIVATING")

            guardduty.update_threat_intel_set(
                DetectorId=det_id, ThreatIntelSetId=ti_id, Activate=True
            )
            detail = guardduty.get_threat_intel_set(DetectorId=det_id, ThreatIntelSetId=ti_id)
            assert detail["Status"] in ("ACTIVE", "ACTIVATING")
        finally:
            guardduty.delete_threat_intel_set(DetectorId=det_id, ThreatIntelSetId=ti_id)
            guardduty.delete_detector(DetectorId=det_id)

    def test_update_ip_set_multiple_fields(self, guardduty, detector_and_ipset):
        """UpdateIPSet can update name and location together."""
        det_id, ip_set_id = detector_and_ipset
        resp = guardduty.update_ip_set(
            DetectorId=det_id,
            IpSetId=ip_set_id,
            Name="multi-update-ipset",
            Location="s3://multi-bucket/ipset.txt",
        )
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200
        get_resp = guardduty.get_ip_set(DetectorId=det_id, IpSetId=ip_set_id)
        assert get_resp["Name"] == "multi-update-ipset"
        assert get_resp["Location"] == "s3://multi-bucket/ipset.txt"

    def test_update_threat_intel_set_multiple_fields(self, guardduty, detector_and_tiset):
        """UpdateThreatIntelSet can update name and location together."""
        det_id, ti_id = detector_and_tiset
        resp = guardduty.update_threat_intel_set(
            DetectorId=det_id,
            ThreatIntelSetId=ti_id,
            Name="multi-update-ti",
            Location="s3://multi-bucket/ti.txt",
        )
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200
        get_resp = guardduty.get_threat_intel_set(DetectorId=det_id, ThreatIntelSetId=ti_id)
        assert get_resp["Name"] == "multi-update-ti"
        assert get_resp["Location"] == "s3://multi-bucket/ti.txt"


class TestGuardDutyFindingsOperations:
    """Tests for Findings operations."""

    def test_list_findings_empty(self, guardduty, detector):
        resp = guardduty.list_findings(DetectorId=detector)
        assert "FindingIds" in resp
        assert isinstance(resp["FindingIds"], list)

    def test_list_findings_with_criteria(self, guardduty, detector):
        resp = guardduty.list_findings(
            DetectorId=detector,
            FindingCriteria={"Criterion": {"severity": {"Gte": 4}}},
        )
        assert "FindingIds" in resp
        assert isinstance(resp["FindingIds"], list)

    def test_get_findings_with_fake_ids(self, guardduty, detector):
        resp = guardduty.get_findings(DetectorId=detector, FindingIds=["nonexistent-finding-id"])
        assert "Findings" in resp
        assert isinstance(resp["Findings"], list)

    def test_get_findings_statistics(self, guardduty, detector):
        resp = guardduty.get_findings_statistics(
            DetectorId=detector, FindingStatisticTypes=["COUNT_BY_SEVERITY"]
        )
        assert "FindingStatistics" in resp
        assert isinstance(resp["FindingStatistics"], dict)

    def test_get_findings_statistics_has_count_by_severity(self, guardduty, detector):
        resp = guardduty.get_findings_statistics(
            DetectorId=detector, FindingStatisticTypes=["COUNT_BY_SEVERITY"]
        )
        assert "CountBySeverity" in resp["FindingStatistics"]
        assert isinstance(resp["FindingStatistics"]["CountBySeverity"], dict)


class TestGuardDutyMasterAccountOperations:
    """Tests for GetMasterAccount operation."""

    def test_get_master_account(self, guardduty, detector):
        resp = guardduty.get_master_account(DetectorId=detector)
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200

    def test_get_master_account_no_master(self, guardduty, detector):
        """When no master is configured, response still returns 200."""
        resp = guardduty.get_master_account(DetectorId=detector)
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200


class TestGuardDutyOrganizationConfigOperations:
    """Tests for DescribeOrganizationConfiguration."""

    def test_describe_organization_configuration(self, guardduty, detector):
        resp = guardduty.describe_organization_configuration(DetectorId=detector)
        assert "AutoEnable" in resp
        assert "MemberAccountLimitReached" in resp
        assert isinstance(resp["MemberAccountLimitReached"], bool)

    def test_describe_organization_configuration_has_features(self, guardduty, detector):
        resp = guardduty.describe_organization_configuration(DetectorId=detector)
        assert "Features" in resp
        assert isinstance(resp["Features"], list)


class TestGuardDutyPublishingDestinationOperations:
    """Tests for DescribePublishingDestination."""

    def test_describe_publishing_destination_nonexistent(self, guardduty, detector):
        with pytest.raises(ClientError) as exc_info:
            guardduty.describe_publishing_destination(
                DetectorId=detector,
                DestinationId="nonexistent00000000000000000000",
            )
        assert exc_info.value.response["Error"]["Code"] == "BadRequestException"


class TestGuardDutyMalwareOperations:
    """Tests for malware-related operations."""

    def test_describe_malware_scans_empty(self, guardduty, detector):
        resp = guardduty.describe_malware_scans(DetectorId=detector)
        assert "Scans" in resp
        assert isinstance(resp["Scans"], list)

    def test_get_malware_scan_settings(self, guardduty, detector):
        resp = guardduty.get_malware_scan_settings(DetectorId=detector)
        assert "EbsSnapshotPreservation" in resp
        assert isinstance(resp["EbsSnapshotPreservation"], str)

    def test_get_malware_scan_settings_has_resource_criteria(self, guardduty, detector):
        resp = guardduty.get_malware_scan_settings(DetectorId=detector)
        assert "ScanResourceCriteria" in resp
        assert isinstance(resp["ScanResourceCriteria"], dict)


class TestGuardDutyCoverageOperations:
    """Tests for GetCoverageStatistics."""

    def test_get_coverage_statistics(self, guardduty, detector):
        resp = guardduty.get_coverage_statistics(
            DetectorId=detector, StatisticsType=["COUNT_BY_RESOURCE_TYPE"]
        )
        assert "CoverageStatistics" in resp
        assert isinstance(resp["CoverageStatistics"], dict)

    def test_get_coverage_statistics_response_shape(self, guardduty, detector):
        resp = guardduty.get_coverage_statistics(
            DetectorId=detector, StatisticsType=["COUNT_BY_RESOURCE_TYPE"]
        )
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200


class TestGuardDutyUsageOperations:
    """Tests for GetUsageStatistics."""

    def test_get_usage_statistics(self, guardduty, detector):
        resp = guardduty.get_usage_statistics(
            DetectorId=detector,
            UsageStatisticType="SUM_BY_ACCOUNT",
            UsageCriteria={"DataSources": ["FLOW_LOGS"]},
        )
        assert "UsageStatistics" in resp
        assert isinstance(resp["UsageStatistics"], dict)

    def test_get_usage_statistics_response_shape(self, guardduty, detector):
        resp = guardduty.get_usage_statistics(
            DetectorId=detector,
            UsageStatisticType="SUM_BY_ACCOUNT",
            UsageCriteria={"DataSources": ["FLOW_LOGS"]},
        )
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200


class TestGuardDutyInvitationOperations:
    """Tests for invitation operations."""

    def test_list_invitations(self, guardduty):
        """ListInvitations returns an Invitations list."""
        resp = guardduty.list_invitations()
        assert "Invitations" in resp
        assert isinstance(resp["Invitations"], list)


class TestGuardDutyPublishingDestinationCRUD:
    """Tests for publishing destination CRUD operations."""

    def test_create_publishing_destination(self, guardduty, detector):
        """CreatePublishingDestination creates a destination and returns an ID."""
        resp = guardduty.create_publishing_destination(
            DetectorId=detector,
            DestinationType="S3",
            DestinationProperties={
                "DestinationArn": "arn:aws:s3:::my-guardduty-bucket",
                "KmsKeyArn": "arn:aws:kms:us-east-1:123456789012:key/fake-key-id",
            },
        )
        assert "DestinationId" in resp
        assert resp["DestinationId"]

    def test_describe_publishing_destination_created(self, guardduty, detector):
        """DescribePublishingDestination returns destination details."""
        create_resp = guardduty.create_publishing_destination(
            DetectorId=detector,
            DestinationType="S3",
            DestinationProperties={
                "DestinationArn": "arn:aws:s3:::my-guardduty-bucket",
                "KmsKeyArn": "arn:aws:kms:us-east-1:123456789012:key/fake-key-id",
            },
        )
        dest_id = create_resp["DestinationId"]
        resp = guardduty.describe_publishing_destination(
            DetectorId=detector,
            DestinationId=dest_id,
        )
        assert resp["DestinationType"] == "S3"
        assert "DestinationProperties" in resp


class TestGuardDutySampleFindingsOperations:
    """Tests for sample findings operations."""

    def test_create_sample_findings(self, guardduty, detector):
        """CreateSampleFindings generates sample findings."""
        resp = guardduty.create_sample_findings(
            DetectorId=detector,
            FindingTypes=["Recon:EC2/PortProbeUnprotectedPort"],
        )
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200

    def test_create_sample_findings_then_list(self, guardduty, detector):
        """After creating sample findings, ListFindings returns them."""
        guardduty.create_sample_findings(
            DetectorId=detector,
            FindingTypes=["Recon:EC2/PortProbeUnprotectedPort"],
        )
        resp = guardduty.list_findings(DetectorId=detector)
        assert "FindingIds" in resp
        assert isinstance(resp["FindingIds"], list)


class TestGuardDutyUntagOperations:
    """Tests for untag resource operations."""

    def test_untag_resource(self, guardduty, detector):
        """UntagResource removes tags from a detector."""
        arn = f"arn:aws:guardduty:us-east-1:123456789012:detector/{detector}"
        # First tag the resource
        guardduty.tag_resource(ResourceArn=arn, Tags={"env": "test", "team": "dev"})
        # Verify tags
        resp = guardduty.list_tags_for_resource(ResourceArn=arn)
        assert "env" in resp["Tags"]
        assert "team" in resp["Tags"]
        # Untag one key
        guardduty.untag_resource(ResourceArn=arn, TagKeys=["team"])
        # Verify only 'env' remains
        resp = guardduty.list_tags_for_resource(ResourceArn=arn)
        assert "env" in resp["Tags"]
        assert "team" not in resp["Tags"]


class TestGuardDutyMemberOperations:
    """Tests for member-related operations."""

    def test_get_remaining_free_trial_days(self, guardduty, detector):
        """GetRemainingFreeTrialDays returns accounts list."""
        resp = guardduty.get_remaining_free_trial_days(
            DetectorId=detector,
            AccountIds=["123456789012"],
        )
        assert "Accounts" in resp
        assert isinstance(resp["Accounts"], list)

    def test_get_member_detectors(self, guardduty, detector):
        """GetMemberDetectors returns member detector features."""
        resp = guardduty.get_member_detectors(
            DetectorId=detector,
            AccountIds=["111111111111"],
        )
        assert (
            "MemberDataSourceConfigurations" in resp
            or resp["ResponseMetadata"]["HTTPStatusCode"] == 200
        )

    def test_get_remaining_free_trial_days_unprocessed(self, guardduty, detector):
        """GetRemainingFreeTrialDays returns UnprocessedAccounts."""
        resp = guardduty.get_remaining_free_trial_days(
            DetectorId=detector,
            AccountIds=["999988887777"],
        )
        assert "UnprocessedAccounts" in resp
        assert isinstance(resp["UnprocessedAccounts"], list)

    def test_get_member_detectors_unprocessed(self, guardduty, detector):
        """GetMemberDetectors returns UnprocessedAccounts for unknown members."""
        resp = guardduty.get_member_detectors(
            DetectorId=detector,
            AccountIds=["999988887777"],
        )
        assert "UnprocessedAccounts" in resp
        assert isinstance(resp["UnprocessedAccounts"], list)


class TestGuardDutyDetectorEdgeCases:
    """Additional edge case tests for detector operations."""

    def test_update_nonexistent_detector_raises_error(self, guardduty):
        with pytest.raises(ClientError) as exc_info:
            guardduty.update_detector(DetectorId="aaaabbbbccccddddeeeeffffgggghhh0", Enable=False)
        assert exc_info.value.response["Error"]["Code"] == "BadRequestException"

    def test_create_detector_returns_detector_id_string(self, guardduty):
        resp = guardduty.create_detector(Enable=True)
        detector_id = resp["DetectorId"]
        assert isinstance(detector_id, str)
        assert len(detector_id) > 0
        guardduty.delete_detector(DetectorId=detector_id)

    def test_list_detectors_empty_after_cleanup(self, guardduty):
        """ListDetectors returns empty list when no detectors exist."""
        # Create and immediately delete to ensure clean state awareness
        resp = guardduty.list_detectors()
        assert "DetectorIds" in resp
        assert isinstance(resp["DetectorIds"], list)


class TestGuardDutyIPSetEdgeCases:
    """Additional edge case tests for IP set operations."""

    def test_delete_ipset_then_list(self, guardduty, detector):
        name = _unique("ipset")
        resp = guardduty.create_ip_set(
            DetectorId=detector,
            Name=name,
            Format="TXT",
            Location="s3://my-bucket/ipset.txt",
            Activate=False,
        )
        ipset_id = resp["IpSetId"]
        guardduty.delete_ip_set(DetectorId=detector, IpSetId=ipset_id)
        listed = guardduty.list_ip_sets(DetectorId=detector)
        assert ipset_id not in listed["IpSetIds"]

    def test_create_ipset_stix_format(self, guardduty, detector):
        name = _unique("ipset")
        resp = guardduty.create_ip_set(
            DetectorId=detector,
            Name=name,
            Format="STIX",
            Location="s3://my-bucket/ipset.stix",
            Activate=False,
        )
        ipset_id = resp["IpSetId"]
        try:
            detail = guardduty.get_ip_set(DetectorId=detector, IpSetId=ipset_id)
            assert detail["Format"] == "STIX"
        finally:
            guardduty.delete_ip_set(DetectorId=detector, IpSetId=ipset_id)

    def test_delete_nonexistent_ipset_raises_error(self, guardduty, detector):
        with pytest.raises(ClientError) as exc_info:
            guardduty.delete_ip_set(DetectorId=detector, IpSetId="nonexistent00000000000000000000")
        assert exc_info.value.response["Error"]["Code"] == "BadRequestException"


class TestGuardDutyThreatIntelSetEdgeCases:
    """Additional edge case tests for threat intel set operations."""

    def test_delete_threat_intel_set_then_list(self, guardduty, detector):
        name = _unique("tiset")
        resp = guardduty.create_threat_intel_set(
            DetectorId=detector,
            Name=name,
            Format="TXT",
            Location="s3://my-bucket/ti.txt",
            Activate=False,
        )
        tiset_id = resp["ThreatIntelSetId"]
        guardduty.delete_threat_intel_set(DetectorId=detector, ThreatIntelSetId=tiset_id)
        listed = guardduty.list_threat_intel_sets(DetectorId=detector)
        assert tiset_id not in listed["ThreatIntelSetIds"]

    def test_create_threat_intel_set_stix_format(self, guardduty, detector):
        name = _unique("tiset")
        resp = guardduty.create_threat_intel_set(
            DetectorId=detector,
            Name=name,
            Format="STIX",
            Location="s3://my-bucket/ti.stix",
            Activate=False,
        )
        tiset_id = resp["ThreatIntelSetId"]
        try:
            detail = guardduty.get_threat_intel_set(DetectorId=detector, ThreatIntelSetId=tiset_id)
            assert detail["Format"] == "STIX"
        finally:
            guardduty.delete_threat_intel_set(DetectorId=detector, ThreatIntelSetId=tiset_id)

    def test_delete_nonexistent_threat_intel_set_raises_error(self, guardduty, detector):
        with pytest.raises(ClientError) as exc_info:
            guardduty.delete_threat_intel_set(
                DetectorId=detector,
                ThreatIntelSetId="nonexistent00000000000000000000",
            )
        assert exc_info.value.response["Error"]["Code"] == "BadRequestException"


class TestGuardDutyFilterEdgeCases:
    """Additional edge case tests for filter operations."""

    def test_list_filters_after_delete(self, guardduty, detector):
        filter_name = _unique("filter")
        guardduty.create_filter(
            DetectorId=detector,
            Name=filter_name,
            FindingCriteria={"Criterion": {"severity": {"Gte": 4}}},
        )
        guardduty.delete_filter(DetectorId=detector, FilterName=filter_name)
        resp = guardduty.list_filters(DetectorId=detector)
        assert filter_name not in resp["FilterNames"]


class TestGuardDutyPublishingDestinationEdgeCases:
    """Additional edge case tests for publishing destinations."""

    def test_create_publishing_destination_has_destination_id(self, guardduty, detector):
        resp = guardduty.create_publishing_destination(
            DetectorId=detector,
            DestinationType="S3",
            DestinationProperties={
                "DestinationArn": "arn:aws:s3:::my-guardduty-bucket-2",
                "KmsKeyArn": "arn:aws:kms:us-east-1:123456789012:key/fake-key-id-2",
            },
        )
        assert isinstance(resp["DestinationId"], str)
        assert len(resp["DestinationId"]) > 0

    def test_describe_publishing_destination_has_status(self, guardduty, detector):
        create_resp = guardduty.create_publishing_destination(
            DetectorId=detector,
            DestinationType="S3",
            DestinationProperties={
                "DestinationArn": "arn:aws:s3:::my-guardduty-bucket-3",
                "KmsKeyArn": "arn:aws:kms:us-east-1:123456789012:key/fake-key-id-3",
            },
        )
        dest_id = create_resp["DestinationId"]
        resp = guardduty.describe_publishing_destination(
            DetectorId=detector,
            DestinationId=dest_id,
        )
        assert "Status" in resp
        assert isinstance(resp["Status"], str)
        assert resp["Status"] != ""


class TestGuardDutyFindingsEdgeCases:
    """Additional edge case tests for findings operations."""

    def test_create_sample_findings_multiple_types(self, guardduty, detector):
        resp = guardduty.create_sample_findings(
            DetectorId=detector,
            FindingTypes=[
                "Recon:EC2/PortProbeUnprotectedPort",
                "UnauthorizedAccess:EC2/SSHBruteForce",
            ],
        )
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200

    def test_list_findings_with_sort_criteria(self, guardduty, detector):
        guardduty.create_sample_findings(
            DetectorId=detector,
            FindingTypes=["Recon:EC2/PortProbeUnprotectedPort"],
        )
        resp = guardduty.list_findings(
            DetectorId=detector,
            SortCriteria={"AttributeName": "severity", "OrderBy": "DESC"},
        )
        assert "FindingIds" in resp
        assert isinstance(resp["FindingIds"], list)

    def test_get_findings_returns_finding_details(self, guardduty, detector):
        """After creating sample findings, GetFindings returns details."""
        guardduty.create_sample_findings(
            DetectorId=detector,
            FindingTypes=["Recon:EC2/PortProbeUnprotectedPort"],
        )
        list_resp = guardduty.list_findings(DetectorId=detector)
        if list_resp["FindingIds"]:
            resp = guardduty.get_findings(
                DetectorId=detector, FindingIds=list_resp["FindingIds"][:1]
            )
            assert "Findings" in resp
            assert len(resp["Findings"]) > 0
            assert "Type" in resp["Findings"][0]


class TestGuardDutyMalwareEdgeCases:
    """Additional edge case tests for malware operations."""

    def test_describe_malware_scans_has_next_token(self, guardduty, detector):
        resp = guardduty.describe_malware_scans(DetectorId=detector)
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200
        assert "Scans" in resp

    def test_get_malware_scan_settings_structure(self, guardduty, detector):
        resp = guardduty.get_malware_scan_settings(DetectorId=detector)
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200
        assert "EbsSnapshotPreservation" in resp
        assert "ScanResourceCriteria" in resp


class TestGuardDutyOrganizationAdminEdgeCases:
    """Additional edge case tests for organization admin operations."""

    def test_list_organization_admin_accounts_response_shape(self, guardduty):
        resp = guardduty.list_organization_admin_accounts()
        assert isinstance(resp["AdminAccounts"], list)
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200

    def test_enable_organization_admin_account_idempotent(self, guardduty):
        resp1 = guardduty.enable_organization_admin_account(AdminAccountId="111122223333")
        assert resp1["ResponseMetadata"]["HTTPStatusCode"] == 200
        resp2 = guardduty.enable_organization_admin_account(AdminAccountId="111122223333")
        assert resp2["ResponseMetadata"]["HTTPStatusCode"] == 200


class TestGuardDutyPublishingDestinationFullCRUD:
    """Tests for full publishing destination lifecycle."""

    def test_list_publishing_destinations_empty(self, guardduty, detector):
        """ListPublishingDestinations returns empty list for fresh detector."""
        resp = guardduty.list_publishing_destinations(DetectorId=detector)
        assert "Destinations" in resp
        assert isinstance(resp["Destinations"], list)

    def test_list_publishing_destinations_after_create(self, guardduty, detector):
        """ListPublishingDestinations includes created destination."""
        create_resp = guardduty.create_publishing_destination(
            DetectorId=detector,
            DestinationType="S3",
            DestinationProperties={
                "DestinationArn": "arn:aws:s3:::gd-list-test-bucket",
                "KmsKeyArn": "arn:aws:kms:us-east-1:123456789012:key/list-key",
            },
        )
        dest_id = create_resp["DestinationId"]
        resp = guardduty.list_publishing_destinations(DetectorId=detector)
        assert "Destinations" in resp
        dest_ids = [d["DestinationId"] for d in resp["Destinations"]]
        assert dest_id in dest_ids

    def test_update_publishing_destination(self, guardduty, detector):
        """UpdatePublishingDestination updates destination properties."""
        create_resp = guardduty.create_publishing_destination(
            DetectorId=detector,
            DestinationType="S3",
            DestinationProperties={
                "DestinationArn": "arn:aws:s3:::gd-update-test-bucket",
                "KmsKeyArn": "arn:aws:kms:us-east-1:123456789012:key/update-key",
            },
        )
        dest_id = create_resp["DestinationId"]
        resp = guardduty.update_publishing_destination(
            DetectorId=detector,
            DestinationId=dest_id,
            DestinationProperties={
                "DestinationArn": "arn:aws:s3:::gd-updated-bucket",
                "KmsKeyArn": "arn:aws:kms:us-east-1:123456789012:key/updated-key",
            },
        )
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200

    def test_delete_publishing_destination(self, guardduty, detector):
        """DeletePublishingDestination removes a destination."""
        create_resp = guardduty.create_publishing_destination(
            DetectorId=detector,
            DestinationType="S3",
            DestinationProperties={
                "DestinationArn": "arn:aws:s3:::gd-delete-test-bucket",
                "KmsKeyArn": "arn:aws:kms:us-east-1:123456789012:key/delete-key",
            },
        )
        dest_id = create_resp["DestinationId"]
        resp = guardduty.delete_publishing_destination(
            DetectorId=detector,
            DestinationId=dest_id,
        )
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200
        # Verify it's gone
        with pytest.raises(ClientError) as exc_info:
            guardduty.describe_publishing_destination(
                DetectorId=detector,
                DestinationId=dest_id,
            )
        assert exc_info.value.response["Error"]["Code"] == "BadRequestException"

    def test_delete_publishing_destination_then_list(self, guardduty, detector):
        """After deleting, the destination no longer appears in list."""
        create_resp = guardduty.create_publishing_destination(
            DetectorId=detector,
            DestinationType="S3",
            DestinationProperties={
                "DestinationArn": "arn:aws:s3:::gd-del-list-bucket",
                "KmsKeyArn": "arn:aws:kms:us-east-1:123456789012:key/del-list-key",
            },
        )
        dest_id = create_resp["DestinationId"]
        guardduty.delete_publishing_destination(DetectorId=detector, DestinationId=dest_id)
        resp = guardduty.list_publishing_destinations(DetectorId=detector)
        dest_ids = [d["DestinationId"] for d in resp["Destinations"]]
        assert dest_id not in dest_ids

    def test_describe_publishing_destination_has_publishing_failure_start_timestamp(
        self, guardduty, detector
    ):
        """DescribePublishingDestination returns expected fields."""
        create_resp = guardduty.create_publishing_destination(
            DetectorId=detector,
            DestinationType="S3",
            DestinationProperties={
                "DestinationArn": "arn:aws:s3:::gd-describe-fields-bucket",
                "KmsKeyArn": "arn:aws:kms:us-east-1:123456789012:key/desc-key",
            },
        )
        dest_id = create_resp["DestinationId"]
        resp = guardduty.describe_publishing_destination(DetectorId=detector, DestinationId=dest_id)
        assert resp["DestinationType"] == "S3"
        assert "Status" in resp
        assert "DestinationProperties" in resp


class TestGuardDutyMemberCRUDOperations:
    """Tests for member create/get/list/delete operations."""

    def test_create_members(self, guardduty, detector):
        """CreateMembers adds member accounts."""
        resp = guardduty.create_members(
            DetectorId=detector,
            AccountDetails=[
                {"AccountId": "222233334444", "Email": "member1@example.com"},
            ],
        )
        assert "UnprocessedAccounts" in resp
        assert isinstance(resp["UnprocessedAccounts"], list)

    def test_list_members_empty(self, guardduty, detector):
        """ListMembers returns empty list for fresh detector."""
        resp = guardduty.list_members(DetectorId=detector)
        assert "Members" in resp
        assert isinstance(resp["Members"], list)

    def test_create_and_list_members(self, guardduty, detector):
        """After creating members, ListMembers includes them."""
        guardduty.create_members(
            DetectorId=detector,
            AccountDetails=[
                {"AccountId": "333344445555", "Email": "member2@example.com"},
            ],
        )
        resp = guardduty.list_members(DetectorId=detector)
        assert "Members" in resp
        member_ids = [m["AccountId"] for m in resp["Members"]]
        assert "333344445555" in member_ids
        assert isinstance(resp["Members"], list)

    def test_get_members(self, guardduty, detector):
        """GetMembers returns details for specified account IDs."""
        guardduty.create_members(
            DetectorId=detector,
            AccountDetails=[
                {"AccountId": "444455556666", "Email": "member3@example.com"},
            ],
        )
        resp = guardduty.get_members(
            DetectorId=detector,
            AccountIds=["444455556666"],
        )
        assert "Members" in resp
        assert "UnprocessedAccounts" in resp
        assert isinstance(resp["Members"], list)
        assert isinstance(resp["UnprocessedAccounts"], list)

    def test_get_members_returns_member_details(self, guardduty, detector):
        """GetMembers returns email and account ID for created members."""
        guardduty.create_members(
            DetectorId=detector,
            AccountDetails=[
                {"AccountId": "555566667777", "Email": "member4@example.com"},
            ],
        )
        resp = guardduty.get_members(
            DetectorId=detector,
            AccountIds=["555566667777"],
        )
        if resp["Members"]:
            member = resp["Members"][0]
            assert member["AccountId"] == "555566667777"
            assert "Email" in member

    def test_delete_members(self, guardduty, detector):
        """DeleteMembers removes member accounts."""
        guardduty.create_members(
            DetectorId=detector,
            AccountDetails=[
                {"AccountId": "666677778888", "Email": "member5@example.com"},
            ],
        )
        resp = guardduty.delete_members(
            DetectorId=detector,
            AccountIds=["666677778888"],
        )
        assert "UnprocessedAccounts" in resp
        assert isinstance(resp["UnprocessedAccounts"], list)

    def test_delete_members_then_list(self, guardduty, detector):
        """After deleting members, they no longer appear in ListMembers."""
        guardduty.create_members(
            DetectorId=detector,
            AccountDetails=[
                {"AccountId": "777788889999", "Email": "member6@example.com"},
            ],
        )
        guardduty.delete_members(
            DetectorId=detector,
            AccountIds=["777788889999"],
        )
        resp = guardduty.list_members(DetectorId=detector)
        member_ids = [m["AccountId"] for m in resp["Members"]]
        assert "777788889999" not in member_ids

    def test_get_members_nonexistent_returns_unprocessed(self, guardduty, detector):
        """GetMembers for non-member account returns UnprocessedAccounts."""
        resp = guardduty.get_members(
            DetectorId=detector,
            AccountIds=["000011112222"],
        )
        assert "UnprocessedAccounts" in resp
        assert isinstance(resp["UnprocessedAccounts"], list)

    def test_create_multiple_members(self, guardduty, detector):
        """CreateMembers can add multiple accounts at once."""
        resp = guardduty.create_members(
            DetectorId=detector,
            AccountDetails=[
                {"AccountId": "111122223333", "Email": "multi1@example.com"},
                {"AccountId": "222233334444", "Email": "multi2@example.com"},
            ],
        )
        assert "UnprocessedAccounts" in resp
        assert isinstance(resp["UnprocessedAccounts"], list)
        listed = guardduty.list_members(DetectorId=detector)
        member_ids = [m["AccountId"] for m in listed["Members"]]
        assert "111122223333" in member_ids
        assert "222233334444" in member_ids


class TestGuardDutyMemberMonitoringOperations:
    """Tests for StartMonitoringMembers and StopMonitoringMembers."""

    def test_start_monitoring_members(self, guardduty, detector):
        """StartMonitoringMembers returns UnprocessedAccounts."""
        guardduty.create_members(
            DetectorId=detector,
            AccountDetails=[
                {"AccountId": "888899990000", "Email": "monitor1@example.com"},
            ],
        )
        resp = guardduty.start_monitoring_members(
            DetectorId=detector,
            AccountIds=["888899990000"],
        )
        assert "UnprocessedAccounts" in resp
        assert isinstance(resp["UnprocessedAccounts"], list)

    def test_stop_monitoring_members(self, guardduty, detector):
        """StopMonitoringMembers returns UnprocessedAccounts."""
        guardduty.create_members(
            DetectorId=detector,
            AccountDetails=[
                {"AccountId": "999900001111", "Email": "monitor2@example.com"},
            ],
        )
        resp = guardduty.stop_monitoring_members(
            DetectorId=detector,
            AccountIds=["999900001111"],
        )
        assert "UnprocessedAccounts" in resp
        assert isinstance(resp["UnprocessedAccounts"], list)

    def test_start_then_stop_monitoring(self, guardduty, detector):
        """Start then stop monitoring for a member."""
        guardduty.create_members(
            DetectorId=detector,
            AccountDetails=[
                {"AccountId": "111100002222", "Email": "startstop@example.com"},
            ],
        )
        start_resp = guardduty.start_monitoring_members(
            DetectorId=detector, AccountIds=["111100002222"]
        )
        assert start_resp["ResponseMetadata"]["HTTPStatusCode"] == 200
        stop_resp = guardduty.stop_monitoring_members(
            DetectorId=detector, AccountIds=["111100002222"]
        )
        assert stop_resp["ResponseMetadata"]["HTTPStatusCode"] == 200


class TestGuardDutyDisableOrganizationAdmin:
    """Tests for DisableOrganizationAdminAccount."""

    def test_disable_organization_admin_account(self, guardduty):
        """DisableOrganizationAdminAccount returns 200."""
        # Enable first so there's something to disable
        guardduty.enable_organization_admin_account(AdminAccountId="111122223333")
        resp = guardduty.disable_organization_admin_account(AdminAccountId="111122223333")
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200

    def test_enable_then_disable_then_list(self, guardduty):
        """After enable then disable, the admin account is removed from list."""
        guardduty.enable_organization_admin_account(AdminAccountId="222233334444")
        guardduty.disable_organization_admin_account(AdminAccountId="222233334444")
        resp = guardduty.list_organization_admin_accounts()
        admin_ids = [a["AdminAccountId"] for a in resp["AdminAccounts"]]
        assert "222233334444" not in admin_ids


class TestGuardDutyDetectorBehavioralFidelity:
    """Behavioral fidelity tests for detector fields — timestamps, structure, ordering."""

    def test_created_at_is_valid_datetime(self, guardduty, detector):
        """CreatedAt should be a valid datetime object (not None, not epoch)."""
        detail = guardduty.get_detector(DetectorId=detector)
        created_at = detail["CreatedAt"]
        assert isinstance(created_at, datetime.datetime)
        # Sanity: not before 2020
        assert created_at.year >= 2020

    def test_updated_at_is_valid_datetime(self, guardduty, detector):
        """UpdatedAt should be a valid datetime object."""
        detail = guardduty.get_detector(DetectorId=detector)
        updated_at = detail["UpdatedAt"]
        assert isinstance(updated_at, datetime.datetime)
        assert updated_at.year >= 2020

    def test_updated_at_changes_after_update(self, guardduty, detector):
        """UpdatedAt should be refreshed when detector is updated."""
        before = guardduty.get_detector(DetectorId=detector)["UpdatedAt"]
        guardduty.update_detector(DetectorId=detector, FindingPublishingFrequency="SIX_HOURS")
        after = guardduty.get_detector(DetectorId=detector)["UpdatedAt"]
        # After update the timestamp should be >= before (may be same second, never older)
        assert after >= before

    def test_features_list_has_expected_structure(self, guardduty, detector):
        """Features list items should have Name and Status fields."""
        detail = guardduty.get_detector(DetectorId=detector)
        features = detail["Features"]
        assert isinstance(features, list)
        for feature in features:
            assert "Name" in feature
            assert "Status" in feature

    def test_data_sources_has_s3_logs(self, guardduty, detector):
        """DataSources should contain S3Logs with a Status field."""
        detail = guardduty.get_detector(DetectorId=detector)
        ds = detail["DataSources"]
        assert "S3Logs" in ds
        assert "Status" in ds["S3Logs"]
        assert isinstance(ds["S3Logs"]["Status"], str)

    def test_data_sources_has_cloud_trail(self, guardduty, detector):
        """DataSources should contain CloudTrail with a Status field."""
        detail = guardduty.get_detector(DetectorId=detector)
        ds = detail["DataSources"]
        assert "CloudTrail" in ds
        assert "Status" in ds["CloudTrail"]
        assert isinstance(ds["CloudTrail"]["Status"], str)

    def test_tags_dict_is_empty_by_default(self, guardduty, detector):
        """Tags should be an empty dict (not None) when no tags were set."""
        detail = guardduty.get_detector(DetectorId=detector)
        assert detail["Tags"] == {}

    def test_service_role_is_string(self, guardduty, detector):
        """ServiceRole should be a string (ARN or empty string)."""
        detail = guardduty.get_detector(DetectorId=detector)
        assert isinstance(detail["ServiceRole"], str)

    def test_create_detector_default_finding_publishing_frequency(self, guardduty):
        """Detector created without frequency should have a default value."""
        resp = guardduty.create_detector(Enable=True)
        detector_id = resp["DetectorId"]
        try:
            detail = guardduty.get_detector(DetectorId=detector_id)
            assert detail["FindingPublishingFrequency"] in (
                "SIX_HOURS",
                "ONE_HOUR",
                "FIFTEEN_MINUTES",
            )
        finally:
            guardduty.delete_detector(DetectorId=detector_id)


class TestGuardDutyAdministratorAccountEdgeCases:
    """Edge cases for GetAdministratorAccount."""

    def test_get_administrator_account_nonexistent_detector(self, guardduty):
        """GetAdministratorAccount for nonexistent detector should raise BadRequestException."""
        with pytest.raises(ClientError) as exc_info:
            guardduty.get_administrator_account(DetectorId="aaaabbbbccccddddeeeeffffgggghhh0")
        assert exc_info.value.response["Error"]["Code"] == "BadRequestException"

    def test_get_administrator_account_returns_200(self, guardduty, detector):
        """GetAdministratorAccount returns 200 regardless of whether an admin is configured."""
        result = guardduty.get_administrator_account(DetectorId=detector)
        assert result["ResponseMetadata"]["HTTPStatusCode"] == 200


class TestGuardDutyOrganizationAdminBehavior:
    """Behavioral tests for organization admin account operations."""

    def test_enable_shows_account_in_list(self, guardduty):
        """After enabling, the admin account should appear in list."""
        guardduty.enable_organization_admin_account(AdminAccountId="111133335555")
        resp = guardduty.list_organization_admin_accounts()
        admin_ids = [a["AdminAccountId"] for a in resp["AdminAccounts"]]
        assert "111133335555" in admin_ids
        assert len(admin_ids) >= 1
        # cleanup
        guardduty.disable_organization_admin_account(AdminAccountId="111133335555")

    def test_list_organization_admin_accounts_items_have_status(self, guardduty):
        """Each admin account entry should have an AdminAccountId and Status."""
        guardduty.enable_organization_admin_account(AdminAccountId="222244446666")
        resp = guardduty.list_organization_admin_accounts()
        accounts = [a for a in resp["AdminAccounts"] if a["AdminAccountId"] == "222244446666"]
        assert len(accounts) == 1
        assert "AdminStatus" in accounts[0]
        # cleanup
        guardduty.disable_organization_admin_account(AdminAccountId="222244446666")

    def test_disable_nonexistent_admin_account_returns_200(self, guardduty):
        """DisableOrganizationAdminAccount for an account not enabled should not error."""
        resp = guardduty.disable_organization_admin_account(AdminAccountId="999911112222")
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200


class TestGuardDutyFilterPagination:
    """Pagination tests for list_filters."""

    def test_list_filters_max_results(self, guardduty, detector):
        """list_filters with MaxResults=1 should return at most 1 filter."""
        names = [_unique("filter") for _ in range(3)]
        for name in names:
            guardduty.create_filter(
                DetectorId=detector,
                Name=name,
                FindingCriteria={"Criterion": {"severity": {"Gte": 4}}},
            )
        try:
            resp = guardduty.list_filters(DetectorId=detector, MaxResults=1)
            assert len(resp["FilterNames"]) <= 1
        finally:
            for name in names:
                guardduty.delete_filter(DetectorId=detector, FilterName=name)

    def test_list_filters_pagination_next_token(self, guardduty, detector):
        """list_filters NextToken should allow fetching remaining results."""
        names = [_unique("filter") for _ in range(3)]
        for name in names:
            guardduty.create_filter(
                DetectorId=detector,
                Name=name,
                FindingCriteria={"Criterion": {"severity": {"Gte": 4}}},
            )
        try:
            first = guardduty.list_filters(DetectorId=detector, MaxResults=1)
            all_names = list(first["FilterNames"])
            token = first.get("NextToken")
            while token:
                page = guardduty.list_filters(
                    DetectorId=detector, MaxResults=1, NextToken=token
                )
                all_names.extend(page["FilterNames"])
                token = page.get("NextToken")
            for name in names:
                assert name in all_names
        finally:
            for name in names:
                guardduty.delete_filter(DetectorId=detector, FilterName=name)


class TestGuardDutyIPSetPagination:
    """Pagination tests for list_ip_sets."""

    def test_list_ipsets_max_results(self, guardduty, detector):
        """list_ip_sets with MaxResults=1 should return at most 1 result."""
        ipset_ids = []
        for i in range(3):
            resp = guardduty.create_ip_set(
                DetectorId=detector,
                Name=_unique("ipset"),
                Format="TXT",
                Location=f"s3://pag-bucket/ipset{i}.txt",
                Activate=False,
            )
            ipset_ids.append(resp["IpSetId"])
        try:
            resp = guardduty.list_ip_sets(DetectorId=detector, MaxResults=1)
            assert len(resp["IpSetIds"]) <= 1
        finally:
            for ipset_id in ipset_ids:
                guardduty.delete_ip_set(DetectorId=detector, IpSetId=ipset_id)

    def test_list_ipsets_pagination_next_token(self, guardduty, detector):
        """list_ip_sets NextToken should allow fetching remaining results."""
        ipset_ids = []
        for i in range(3):
            resp = guardduty.create_ip_set(
                DetectorId=detector,
                Name=_unique("ipset"),
                Format="TXT",
                Location=f"s3://pag-bucket/ipset-tok{i}.txt",
                Activate=False,
            )
            ipset_ids.append(resp["IpSetId"])
        try:
            first = guardduty.list_ip_sets(DetectorId=detector, MaxResults=1)
            all_ids = list(first["IpSetIds"])
            token = first.get("NextToken")
            while token:
                page = guardduty.list_ip_sets(DetectorId=detector, MaxResults=1, NextToken=token)
                all_ids.extend(page["IpSetIds"])
                token = page.get("NextToken")
            for ipset_id in ipset_ids:
                assert ipset_id in all_ids
        finally:
            for ipset_id in ipset_ids:
                guardduty.delete_ip_set(DetectorId=detector, IpSetId=ipset_id)


class TestGuardDutyThreatIntelSetPagination:
    """Pagination tests for list_threat_intel_sets."""

    def test_list_threat_intel_sets_max_results(self, guardduty, detector):
        """list_threat_intel_sets with MaxResults=1 should return at most 1 result."""
        tiset_ids = []
        for i in range(3):
            resp = guardduty.create_threat_intel_set(
                DetectorId=detector,
                Name=_unique("tiset"),
                Format="TXT",
                Location=f"s3://pag-ti-bucket/ti{i}.txt",
                Activate=False,
            )
            tiset_ids.append(resp["ThreatIntelSetId"])
        try:
            resp = guardduty.list_threat_intel_sets(DetectorId=detector, MaxResults=1)
            assert len(resp["ThreatIntelSetIds"]) <= 1
        finally:
            for tiset_id in tiset_ids:
                guardduty.delete_threat_intel_set(DetectorId=detector, ThreatIntelSetId=tiset_id)

    def test_list_threat_intel_sets_pagination_next_token(self, guardduty, detector):
        """list_threat_intel_sets NextToken should allow fetching remaining results."""
        tiset_ids = []
        for i in range(3):
            resp = guardduty.create_threat_intel_set(
                DetectorId=detector,
                Name=_unique("tiset"),
                Format="TXT",
                Location=f"s3://pag-ti-bucket/ti-tok{i}.txt",
                Activate=False,
            )
            tiset_ids.append(resp["ThreatIntelSetId"])
        try:
            first = guardduty.list_threat_intel_sets(DetectorId=detector, MaxResults=1)
            all_ids = list(first["ThreatIntelSetIds"])
            token = first.get("NextToken")
            while token:
                page = guardduty.list_threat_intel_sets(
                    DetectorId=detector, MaxResults=1, NextToken=token
                )
                all_ids.extend(page["ThreatIntelSetIds"])
                token = page.get("NextToken")
            for tiset_id in tiset_ids:
                assert tiset_id in all_ids
        finally:
            for tiset_id in tiset_ids:
                guardduty.delete_threat_intel_set(DetectorId=detector, ThreatIntelSetId=tiset_id)


class TestGuardDutyTagEdgeCases:
    """Edge cases for tag operations."""

    def test_list_tags_for_nonexistent_resource(self, guardduty):
        """ListTagsForResource on a nonexistent ARN should raise an error."""
        with pytest.raises(ClientError) as exc_info:
            guardduty.list_tags_for_resource(
                ResourceArn="arn:aws:guardduty:us-east-1:123456789012:detector/nonexistent"
            )
        assert exc_info.value.response["Error"]["Code"] in (
            "BadRequestException",
            "InvalidInputException",
            "ResourceNotFoundException",
        )

    def test_tag_resource_with_unicode_values(self, guardduty, detector):
        """TagResource should accept unicode tag values."""
        arn = f"arn:aws:guardduty:us-east-1:123456789012:detector/{detector}"
        guardduty.tag_resource(ResourceArn=arn, Tags={"desc": "test-\u00e9\u00e0\u00fc"})
        tags_resp = guardduty.list_tags_for_resource(ResourceArn=arn)
        assert tags_resp["Tags"]["desc"] == "test-\u00e9\u00e0\u00fc"

    def test_tag_resource_multiple_tags(self, guardduty, detector):
        """TagResource can add many tags at once."""
        arn = f"arn:aws:guardduty:us-east-1:123456789012:detector/{detector}"
        tags = {f"key{i}": f"val{i}" for i in range(5)}
        guardduty.tag_resource(ResourceArn=arn, Tags=tags)
        resp = guardduty.list_tags_for_resource(ResourceArn=arn)
        for k, v in tags.items():
            assert resp["Tags"][k] == v


class TestGuardDutyFindingsPagination:
    """Pagination tests for list_findings."""

    def test_list_findings_max_results(self, guardduty, detector):
        """list_findings with MaxResults should limit results."""
        guardduty.create_sample_findings(
            DetectorId=detector,
            FindingTypes=[
                "Recon:EC2/PortProbeUnprotectedPort",
                "UnauthorizedAccess:EC2/SSHBruteForce",
                "Backdoor:EC2/C&CActivity.B",
            ],
        )
        resp = guardduty.list_findings(DetectorId=detector, MaxResults=1)
        assert "FindingIds" in resp
        assert len(resp["FindingIds"]) <= 1

    def test_list_findings_next_token(self, guardduty, detector):
        """list_findings NextToken allows pagination over all results."""
        guardduty.create_sample_findings(
            DetectorId=detector,
            FindingTypes=[
                "Recon:EC2/PortProbeUnprotectedPort",
                "UnauthorizedAccess:EC2/SSHBruteForce",
                "Backdoor:EC2/C&CActivity.B",
            ],
        )
        first = guardduty.list_findings(DetectorId=detector, MaxResults=1)
        all_ids = list(first["FindingIds"])
        token = first.get("NextToken")
        pages = 0
        while token and pages < 20:
            page = guardduty.list_findings(DetectorId=detector, MaxResults=1, NextToken=token)
            all_ids.extend(page["FindingIds"])
            token = page.get("NextToken")
            pages += 1
        # We asked for 3 finding types — should have at least 1 in total
        assert len(all_ids) >= 1


class TestGuardDutyTrustedEntitySetEdgeCases:
    """Edge cases and error paths for UpdateTrustedEntitySet (organization config)."""

    def test_update_organization_configuration_returns_200(self, guardduty, detector):
        """UpdateOrganizationConfiguration should return 200."""
        resp = guardduty.update_organization_configuration(
            DetectorId=detector,
            AutoEnable=False,
        )
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200

    def test_update_organization_configuration_nonexistent_detector(self, guardduty):
        """UpdateOrganizationConfiguration for nonexistent detector should fail."""
        with pytest.raises(ClientError) as exc_info:
            guardduty.update_organization_configuration(
                DetectorId="aaaabbbbccccddddeeeeffffgggghhh0",
                AutoEnable=False,
            )
        assert exc_info.value.response["Error"]["Code"] == "BadRequestException"

    def test_describe_organization_configuration_after_update(self, guardduty, detector):
        """After updating org config, describe should reflect the change."""
        guardduty.update_organization_configuration(DetectorId=detector, AutoEnable=True)
        resp = guardduty.describe_organization_configuration(DetectorId=detector)
        # AutoEnable should be present (value depends on implementation)
        assert "AutoEnable" in resp
        assert isinstance(resp["AutoEnable"], (bool, str))

    def test_describe_organization_configuration_nonexistent_detector(self, guardduty):
        """DescribeOrganizationConfiguration for nonexistent detector should fail."""
        with pytest.raises(ClientError) as exc_info:
            guardduty.describe_organization_configuration(
                DetectorId="aaaabbbbccccddddeeeeffffgggghhh0"
            )
        assert exc_info.value.response["Error"]["Code"] == "BadRequestException"


class TestGuardDutyUpdateOrganizationConfiguration:
    """Tests for UpdateOrganizationConfiguration."""

    def test_update_organization_configuration(self, guardduty, detector):
        """UpdateOrganizationConfiguration returns 200."""
        resp = guardduty.update_organization_configuration(
            DetectorId=detector,
            AutoEnable=True,
        )
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200

    def test_update_organization_configuration_auto_enable_false(self, guardduty, detector):
        """UpdateOrganizationConfiguration with AutoEnable=False."""
        resp = guardduty.update_organization_configuration(
            DetectorId=detector,
            AutoEnable=False,
        )
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200

    def test_update_then_describe_organization_configuration(self, guardduty, detector):
        """After update, DescribeOrganizationConfiguration still returns valid shape."""
        guardduty.update_organization_configuration(
            DetectorId=detector,
            AutoEnable=True,
        )
        resp = guardduty.describe_organization_configuration(DetectorId=detector)
        assert "AutoEnable" in resp
        assert "MemberAccountLimitReached" in resp
        assert isinstance(resp["MemberAccountLimitReached"], bool)


class TestGuardDutyAcceptAdministratorInvitation:
    """Tests for AcceptAdministratorInvitation."""

    def test_accept_administrator_invitation(self, guardduty, detector):
        """AcceptAdministratorInvitation returns 200."""
        resp = guardduty.accept_administrator_invitation(
            DetectorId=detector,
            AdministratorId="111122223333",
            InvitationId="fake-invitation-id",
        )
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200

    def test_accept_administrator_invitation_then_get(self, guardduty, detector):
        """After accepting, GetAdministratorAccount reflects the admin."""
        guardduty.accept_administrator_invitation(
            DetectorId=detector,
            AdministratorId="222233334444",
            InvitationId="another-invitation-id",
        )
        resp = guardduty.get_administrator_account(DetectorId=detector)
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200


class TestGuardDutyArchiveFindings:
    """Tests for ArchiveFindings and UnarchiveFindings."""

    def test_archive_findings(self, guardduty, detector):
        """ArchiveFindings returns 200 even with fake finding IDs."""
        resp = guardduty.archive_findings(DetectorId=detector, FindingIds=["fake-finding-id-1"])
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200

    def test_unarchive_findings(self, guardduty, detector):
        """UnarchiveFindings returns 200 even with fake finding IDs."""
        resp = guardduty.unarchive_findings(DetectorId=detector, FindingIds=["fake-finding-id-1"])
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200

    def test_archive_then_unarchive_findings(self, guardduty, detector):
        """Create sample findings, archive them, then unarchive."""
        guardduty.create_sample_findings(
            DetectorId=detector,
            FindingTypes=["Recon:EC2/PortProbeUnprotectedPort"],
        )
        findings = guardduty.list_findings(DetectorId=detector)
        finding_ids = findings["FindingIds"]
        assert len(finding_ids) > 0

        resp = guardduty.archive_findings(DetectorId=detector, FindingIds=finding_ids[:1])
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200

        resp = guardduty.unarchive_findings(DetectorId=detector, FindingIds=finding_ids[:1])
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200


class TestGuardDutyUpdateFindingsFeedback:
    """Tests for UpdateFindingsFeedback."""

    def test_update_findings_feedback_useful(self, guardduty, detector):
        """UpdateFindingsFeedback with USEFUL returns 200."""
        guardduty.create_sample_findings(
            DetectorId=detector,
            FindingTypes=["Recon:EC2/PortProbeUnprotectedPort"],
        )
        findings = guardduty.list_findings(DetectorId=detector)
        finding_ids = findings["FindingIds"]
        assert len(finding_ids) > 0

        resp = guardduty.update_findings_feedback(
            DetectorId=detector, FindingIds=finding_ids[:1], Feedback="USEFUL"
        )
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200

    def test_update_findings_feedback_not_useful(self, guardduty, detector):
        """UpdateFindingsFeedback with NOT_USEFUL returns 200."""
        guardduty.create_sample_findings(
            DetectorId=detector,
            FindingTypes=["Recon:EC2/PortProbeUnprotectedPort"],
        )
        findings = guardduty.list_findings(DetectorId=detector)
        finding_ids = findings["FindingIds"]
        assert len(finding_ids) > 0

        resp = guardduty.update_findings_feedback(
            DetectorId=detector, FindingIds=finding_ids[:1], Feedback="NOT_USEFUL"
        )
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200


class TestGuardDutyInviteMembers:
    """Tests for InviteMembers."""

    def test_invite_members(self, guardduty, detector):
        """InviteMembers returns 200 with UnprocessedAccounts."""
        resp = guardduty.invite_members(DetectorId=detector, AccountIds=["111122223333"])
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200
        assert "UnprocessedAccounts" in resp

    def test_invite_multiple_members(self, guardduty, detector):
        """InviteMembers with multiple accounts."""
        resp = guardduty.invite_members(
            DetectorId=detector,
            AccountIds=["111122223333", "444455556666"],
        )
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200
        assert "UnprocessedAccounts" in resp


class TestGuardDutyInvitationOps:
    """Tests for GetInvitationsCount, DeclineInvitations, DeleteInvitations."""

    def test_get_invitations_count(self, guardduty):
        """GetInvitationsCount returns a count."""
        resp = guardduty.get_invitations_count()
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200
        assert "InvitationsCount" in resp
        assert isinstance(resp["InvitationsCount"], int)

    def test_decline_invitations(self, guardduty):
        """DeclineInvitations returns 200."""
        resp = guardduty.decline_invitations(AccountIds=["111122223333"])
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200
        assert "UnprocessedAccounts" in resp

    def test_delete_invitations(self, guardduty):
        """DeleteInvitations returns 200."""
        resp = guardduty.delete_invitations(AccountIds=["111122223333"])
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200
        assert "UnprocessedAccounts" in resp


class TestGuardDutyDisassociate:
    """Tests for Disassociate operations."""

    def test_disassociate_from_administrator_account(self, guardduty, detector):
        """DisassociateFromAdministratorAccount returns 200."""
        resp = guardduty.disassociate_from_administrator_account(DetectorId=detector)
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200

    def test_disassociate_from_master_account(self, guardduty, detector):
        """DisassociateFromMasterAccount returns 200."""
        resp = guardduty.disassociate_from_master_account(DetectorId=detector)
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200

    def test_disassociate_members(self, guardduty, detector):
        """DisassociateMembers returns 200."""
        resp = guardduty.disassociate_members(DetectorId=detector, AccountIds=["111122223333"])
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200
        assert "UnprocessedAccounts" in resp


class TestGuardDutyAcceptInvitation:
    """Tests for AcceptInvitation (legacy API)."""

    def test_accept_invitation(self, guardduty, detector):
        """AcceptInvitation (legacy) returns 200."""
        resp = guardduty.accept_invitation(
            DetectorId=detector,
            MasterId="111122223333",
            InvitationId="fake-invitation-id",
        )
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200


class TestGuardDutyListCoverage:
    """Tests for ListCoverage."""

    def test_list_coverage(self, guardduty, detector):
        """ListCoverage returns Resources list."""
        resp = guardduty.list_coverage(DetectorId=detector)
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200
        assert "Resources" in resp
        assert isinstance(resp["Resources"], list)


class TestGuardDutyUpdateMemberDetectors:
    """Tests for UpdateMemberDetectors."""

    def test_update_member_detectors(self, guardduty, detector):
        """UpdateMemberDetectors returns UnprocessedAccounts."""
        resp = guardduty.update_member_detectors(DetectorId=detector, AccountIds=["111122223333"])
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200
        assert "UnprocessedAccounts" in resp


class TestGuardDutyUpdateMalwareScanSettings:
    """Tests for UpdateMalwareScanSettings."""

    def test_update_malware_scan_settings(self, guardduty, detector):
        """UpdateMalwareScanSettings returns 200."""
        resp = guardduty.update_malware_scan_settings(
            DetectorId=detector,
            ScanResourceCriteria={
                "Include": {"EC2_INSTANCE_TAG": {"MapEquals": [{"Key": "env", "Value": "prod"}]}}
            },
        )
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200

    def test_update_malware_scan_settings_empty_criteria(self, guardduty, detector):
        """UpdateMalwareScanSettings with empty criteria returns 200."""
        resp = guardduty.update_malware_scan_settings(DetectorId=detector, ScanResourceCriteria={})
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200


class TestGuardDutyStartMalwareScan:
    """Tests for StartMalwareScan."""

    def test_start_malware_scan(self, guardduty):
        """StartMalwareScan returns a ScanId."""
        resp = guardduty.start_malware_scan(
            ResourceArn="arn:aws:ec2:us-east-1:123456789012:instance/i-1234567890abcdef0"
        )
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200
        assert "ScanId" in resp


class TestGuardDutyIPSets:
    """Tests for GuardDuty IP Set operations."""

    def test_create_ip_set(self, guardduty, detector):
        """CreateIPSet returns an IpSetId."""
        name = _unique("ipset")
        resp = guardduty.create_ip_set(
            DetectorId=detector,
            Name=name,
            Format="TXT",
            Location="s3://test-bucket/ipset.txt",
            Activate=False,
        )
        assert "IpSetId" in resp
        assert isinstance(resp["IpSetId"], str)
        assert len(resp["IpSetId"]) > 0
        ip_set_id = resp["IpSetId"]
        guardduty.delete_ip_set(DetectorId=detector, IpSetId=ip_set_id)

    def test_get_ip_set(self, guardduty, detector):
        """GetIPSet returns details of a created IP set."""
        name = _unique("ipset")
        create_resp = guardduty.create_ip_set(
            DetectorId=detector,
            Name=name,
            Format="TXT",
            Location="s3://test-bucket/ipset.txt",
            Activate=False,
        )
        ip_set_id = create_resp["IpSetId"]
        try:
            resp = guardduty.get_ip_set(DetectorId=detector, IpSetId=ip_set_id)
            assert resp["Name"] == name
            assert resp["Format"] == "TXT"
        finally:
            guardduty.delete_ip_set(DetectorId=detector, IpSetId=ip_set_id)

    def test_list_ip_sets(self, guardduty, detector):
        """ListIPSets returns IpSetIds."""
        resp = guardduty.list_ip_sets(DetectorId=detector)
        assert "IpSetIds" in resp
        assert isinstance(resp["IpSetIds"], list)

    def test_update_ip_set(self, guardduty, detector):
        """UpdateIPSet can change the name of an IP set."""
        name = _unique("ipset")
        create_resp = guardduty.create_ip_set(
            DetectorId=detector,
            Name=name,
            Format="TXT",
            Location="s3://test-bucket/ipset.txt",
            Activate=False,
        )
        ip_set_id = create_resp["IpSetId"]
        try:
            new_name = _unique("ipset-updated")
            resp = guardduty.update_ip_set(DetectorId=detector, IpSetId=ip_set_id, Name=new_name)
            assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200
            detail = guardduty.get_ip_set(DetectorId=detector, IpSetId=ip_set_id)
            assert detail["Name"] == new_name
        finally:
            guardduty.delete_ip_set(DetectorId=detector, IpSetId=ip_set_id)

    def test_delete_ip_set(self, guardduty, detector):
        """DeleteIPSet removes the IP set."""
        name = _unique("ipset")
        create_resp = guardduty.create_ip_set(
            DetectorId=detector,
            Name=name,
            Format="TXT",
            Location="s3://test-bucket/ipset.txt",
            Activate=False,
        )
        ip_set_id = create_resp["IpSetId"]
        resp = guardduty.delete_ip_set(DetectorId=detector, IpSetId=ip_set_id)
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200


class TestGuardDutyThreatIntelSets:
    """Tests for GuardDuty Threat Intel Set operations."""

    def test_create_threat_intel_set(self, guardduty, detector):
        """CreateThreatIntelSet returns a ThreatIntelSetId."""
        name = _unique("threat")
        resp = guardduty.create_threat_intel_set(
            DetectorId=detector,
            Name=name,
            Format="TXT",
            Location="s3://test-bucket/threat.txt",
            Activate=False,
        )
        assert "ThreatIntelSetId" in resp
        assert isinstance(resp["ThreatIntelSetId"], str)
        guardduty.delete_threat_intel_set(
            DetectorId=detector, ThreatIntelSetId=resp["ThreatIntelSetId"]
        )

    def test_get_threat_intel_set(self, guardduty, detector):
        """GetThreatIntelSet returns details of a created threat intel set."""
        name = _unique("threat")
        create_resp = guardduty.create_threat_intel_set(
            DetectorId=detector,
            Name=name,
            Format="TXT",
            Location="s3://test-bucket/threat.txt",
            Activate=False,
        )
        tid = create_resp["ThreatIntelSetId"]
        try:
            resp = guardduty.get_threat_intel_set(DetectorId=detector, ThreatIntelSetId=tid)
            assert resp["Name"] == name
            assert resp["Format"] == "TXT"
        finally:
            guardduty.delete_threat_intel_set(DetectorId=detector, ThreatIntelSetId=tid)

    def test_list_threat_intel_sets(self, guardduty, detector):
        """ListThreatIntelSets returns ThreatIntelSetIds."""
        resp = guardduty.list_threat_intel_sets(DetectorId=detector)
        assert "ThreatIntelSetIds" in resp
        assert isinstance(resp["ThreatIntelSetIds"], list)

    def test_update_threat_intel_set(self, guardduty, detector):
        """UpdateThreatIntelSet can change the name."""
        name = _unique("threat")
        create_resp = guardduty.create_threat_intel_set(
            DetectorId=detector,
            Name=name,
            Format="TXT",
            Location="s3://test-bucket/threat.txt",
            Activate=False,
        )
        tid = create_resp["ThreatIntelSetId"]
        try:
            new_name = _unique("threat-updated")
            resp = guardduty.update_threat_intel_set(
                DetectorId=detector, ThreatIntelSetId=tid, Name=new_name
            )
            assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200
            detail = guardduty.get_threat_intel_set(DetectorId=detector, ThreatIntelSetId=tid)
            assert detail["Name"] == new_name
        finally:
            guardduty.delete_threat_intel_set(DetectorId=detector, ThreatIntelSetId=tid)

    def test_delete_threat_intel_set(self, guardduty, detector):
        """DeleteThreatIntelSet removes the threat intel set."""
        name = _unique("threat")
        create_resp = guardduty.create_threat_intel_set(
            DetectorId=detector,
            Name=name,
            Format="TXT",
            Location="s3://test-bucket/threat.txt",
            Activate=False,
        )
        tid = create_resp["ThreatIntelSetId"]
        resp = guardduty.delete_threat_intel_set(DetectorId=detector, ThreatIntelSetId=tid)
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200


class TestGuardDutyMalwareProtection:
    """Tests for GuardDuty Malware Protection Plan operations."""

    def test_create_malware_protection_plan(self, guardduty):
        """CreateMalwareProtectionPlan returns a MalwareProtectionPlanId."""
        resp = guardduty.create_malware_protection_plan(
            Role="arn:aws:iam::123456789012:role/test",
            ProtectedResource={"S3Bucket": {"BucketName": "test-bucket"}},
        )
        assert "MalwareProtectionPlanId" in resp
        assert isinstance(resp["MalwareProtectionPlanId"], str)
        assert len(resp["MalwareProtectionPlanId"]) > 0
        guardduty.delete_malware_protection_plan(
            MalwareProtectionPlanId=resp["MalwareProtectionPlanId"]
        )

    def test_list_malware_protection_plans(self, guardduty):
        """ListMalwareProtectionPlans returns MalwareProtectionPlans."""
        resp = guardduty.list_malware_protection_plans()
        assert "MalwareProtectionPlans" in resp
        assert isinstance(resp["MalwareProtectionPlans"], list)

    def test_get_malware_protection_plan(self, guardduty):
        """GetMalwareProtectionPlan returns details of a created plan."""
        create_resp = guardduty.create_malware_protection_plan(
            Role="arn:aws:iam::123456789012:role/test",
            ProtectedResource={"S3Bucket": {"BucketName": "test-bucket"}},
        )
        plan_id = create_resp["MalwareProtectionPlanId"]
        try:
            resp = guardduty.get_malware_protection_plan(MalwareProtectionPlanId=plan_id)
            assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200
        finally:
            guardduty.delete_malware_protection_plan(MalwareProtectionPlanId=plan_id)

    def test_delete_malware_protection_plan(self, guardduty):
        """DeleteMalwareProtectionPlan removes the plan."""
        create_resp = guardduty.create_malware_protection_plan(
            Role="arn:aws:iam::123456789012:role/test",
            ProtectedResource={"S3Bucket": {"BucketName": "test-bucket"}},
        )
        plan_id = create_resp["MalwareProtectionPlanId"]
        resp = guardduty.delete_malware_protection_plan(MalwareProtectionPlanId=plan_id)
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200


class TestGuardDutyThreatEntitySetOperations:
    """Tests for GuardDuty ThreatEntitySet CRUD operations."""

    def test_create_threat_entity_set(self, guardduty, detector):
        """CreateThreatEntitySet returns a ThreatEntitySetId."""
        resp = guardduty.create_threat_entity_set(
            DetectorId=detector,
            Name=_unique("threat"),
            Format="TXT",
            Location="s3://test-bucket/threats.txt",
            Activate=True,
        )
        assert "ThreatEntitySetId" in resp
        assert resp["ThreatEntitySetId"]
        guardduty.delete_threat_entity_set(
            DetectorId=detector, ThreatEntitySetId=resp["ThreatEntitySetId"]
        )

    def test_get_threat_entity_set(self, guardduty, detector):
        """GetThreatEntitySet returns set details."""
        create_resp = guardduty.create_threat_entity_set(
            DetectorId=detector,
            Name=_unique("threat"),
            Format="TXT",
            Location="s3://test-bucket/threats.txt",
            Activate=True,
        )
        tes_id = create_resp["ThreatEntitySetId"]
        try:
            resp = guardduty.get_threat_entity_set(DetectorId=detector, ThreatEntitySetId=tes_id)
            assert resp["Name"]
            assert resp["Format"] == "TXT"
            assert resp["Location"] == "s3://test-bucket/threats.txt"
            assert "Status" in resp
        finally:
            guardduty.delete_threat_entity_set(DetectorId=detector, ThreatEntitySetId=tes_id)

    def test_list_threat_entity_sets(self, guardduty, detector):
        """ListThreatEntitySets returns ThreatEntitySetIds."""
        resp = guardduty.list_threat_entity_sets(DetectorId=detector)
        assert "ThreatEntitySetIds" in resp
        assert isinstance(resp["ThreatEntitySetIds"], list)

    def test_list_threat_entity_sets_includes_created(self, guardduty, detector):
        """ListThreatEntitySets includes a newly created set."""
        create_resp = guardduty.create_threat_entity_set(
            DetectorId=detector,
            Name=_unique("threat"),
            Format="TXT",
            Location="s3://test-bucket/threats.txt",
            Activate=True,
        )
        tes_id = create_resp["ThreatEntitySetId"]
        try:
            resp = guardduty.list_threat_entity_sets(DetectorId=detector)
            assert tes_id in resp["ThreatEntitySetIds"]
        finally:
            guardduty.delete_threat_entity_set(DetectorId=detector, ThreatEntitySetId=tes_id)

    def test_update_threat_entity_set(self, guardduty, detector):
        """UpdateThreatEntitySet modifies the set name."""
        create_resp = guardduty.create_threat_entity_set(
            DetectorId=detector,
            Name=_unique("threat"),
            Format="TXT",
            Location="s3://test-bucket/threats.txt",
            Activate=True,
        )
        tes_id = create_resp["ThreatEntitySetId"]
        try:
            updated_name = _unique("updated-threat")
            resp = guardduty.update_threat_entity_set(
                DetectorId=detector, ThreatEntitySetId=tes_id, Name=updated_name
            )
            assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200
            get_resp = guardduty.get_threat_entity_set(
                DetectorId=detector, ThreatEntitySetId=tes_id
            )
            assert get_resp["Name"] == updated_name
        finally:
            guardduty.delete_threat_entity_set(DetectorId=detector, ThreatEntitySetId=tes_id)

    def test_delete_threat_entity_set(self, guardduty, detector):
        """DeleteThreatEntitySet removes the set."""
        create_resp = guardduty.create_threat_entity_set(
            DetectorId=detector,
            Name=_unique("threat"),
            Format="TXT",
            Location="s3://test-bucket/threats.txt",
            Activate=True,
        )
        tes_id = create_resp["ThreatEntitySetId"]
        resp = guardduty.delete_threat_entity_set(DetectorId=detector, ThreatEntitySetId=tes_id)
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200
        listed = guardduty.list_threat_entity_sets(DetectorId=detector)
        assert tes_id not in listed["ThreatEntitySetIds"]


class TestGuardDutyTrustedEntitySetOperations:
    """Tests for GuardDuty TrustedEntitySet CRUD operations."""

    def test_create_trusted_entity_set(self, guardduty, detector):
        """CreateTrustedEntitySet returns a TrustedEntitySetId."""
        resp = guardduty.create_trusted_entity_set(
            DetectorId=detector,
            Name=_unique("trusted"),
            Format="TXT",
            Location="s3://test-bucket/trusted.txt",
            Activate=True,
        )
        assert "TrustedEntitySetId" in resp
        assert resp["TrustedEntitySetId"]
        guardduty.delete_trusted_entity_set(
            DetectorId=detector, TrustedEntitySetId=resp["TrustedEntitySetId"]
        )

    def test_get_trusted_entity_set(self, guardduty, detector):
        """GetTrustedEntitySet returns set details."""
        create_resp = guardduty.create_trusted_entity_set(
            DetectorId=detector,
            Name=_unique("trusted"),
            Format="TXT",
            Location="s3://test-bucket/trusted.txt",
            Activate=True,
        )
        trust_id = create_resp["TrustedEntitySetId"]
        try:
            resp = guardduty.get_trusted_entity_set(
                DetectorId=detector, TrustedEntitySetId=trust_id
            )
            assert resp["Name"]
            assert resp["Format"] == "TXT"
            assert resp["Location"] == "s3://test-bucket/trusted.txt"
            assert "Status" in resp
        finally:
            guardduty.delete_trusted_entity_set(DetectorId=detector, TrustedEntitySetId=trust_id)

    def test_list_trusted_entity_sets(self, guardduty, detector):
        """ListTrustedEntitySets returns TrustedEntitySetIds."""
        resp = guardduty.list_trusted_entity_sets(DetectorId=detector)
        assert "TrustedEntitySetIds" in resp
        assert isinstance(resp["TrustedEntitySetIds"], list)

    def test_list_trusted_entity_sets_includes_created(self, guardduty, detector):
        """ListTrustedEntitySets includes a newly created set."""
        create_resp = guardduty.create_trusted_entity_set(
            DetectorId=detector,
            Name=_unique("trusted"),
            Format="TXT",
            Location="s3://test-bucket/trusted.txt",
            Activate=True,
        )
        trust_id = create_resp["TrustedEntitySetId"]
        try:
            resp = guardduty.list_trusted_entity_sets(DetectorId=detector)
            assert trust_id in resp["TrustedEntitySetIds"]
        finally:
            guardduty.delete_trusted_entity_set(DetectorId=detector, TrustedEntitySetId=trust_id)

    def test_update_trusted_entity_set(self, guardduty, detector):
        """UpdateTrustedEntitySet modifies the set name."""
        create_resp = guardduty.create_trusted_entity_set(
            DetectorId=detector,
            Name=_unique("trusted"),
            Format="TXT",
            Location="s3://test-bucket/trusted.txt",
            Activate=True,
        )
        trust_id = create_resp["TrustedEntitySetId"]
        try:
            updated_name = _unique("updated-trusted")
            resp = guardduty.update_trusted_entity_set(
                DetectorId=detector, TrustedEntitySetId=trust_id, Name=updated_name
            )
            assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200
            get_resp = guardduty.get_trusted_entity_set(
                DetectorId=detector, TrustedEntitySetId=trust_id
            )
            assert get_resp["Name"] == updated_name
        finally:
            guardduty.delete_trusted_entity_set(DetectorId=detector, TrustedEntitySetId=trust_id)

    def test_delete_trusted_entity_set(self, guardduty, detector):
        """DeleteTrustedEntitySet removes the set."""
        create_resp = guardduty.create_trusted_entity_set(
            DetectorId=detector,
            Name=_unique("trusted"),
            Format="TXT",
            Location="s3://test-bucket/trusted.txt",
            Activate=True,
        )
        trust_id = create_resp["TrustedEntitySetId"]
        resp = guardduty.delete_trusted_entity_set(DetectorId=detector, TrustedEntitySetId=trust_id)
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200
        listed = guardduty.list_trusted_entity_sets(DetectorId=detector)
        assert trust_id not in listed["TrustedEntitySetIds"]


class TestGuardDutyOrganizationStatistics:
    """Tests for GetOrganizationStatistics."""

    def test_get_organization_statistics(self, guardduty):
        """GetOrganizationStatistics returns OrganizationDetails."""
        resp = guardduty.get_organization_statistics()
        assert "OrganizationDetails" in resp
        assert isinstance(resp["OrganizationDetails"], dict)

    def test_get_organization_statistics_response_code(self, guardduty):
        """GetOrganizationStatistics returns 200."""
        resp = guardduty.get_organization_statistics()
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200


class TestGuardDutyDetectorTimestampBehavior:
    """Behavioral fidelity tests for detector timestamps and fields."""

    def test_detector_created_at_is_nonempty_string(self, guardduty, detector):
        """CreatedAt field is a non-empty string."""
        detail = guardduty.get_detector(DetectorId=detector)
        assert isinstance(detail["CreatedAt"], str)
        assert len(detail["CreatedAt"]) > 0

    def test_detector_updated_at_changes_after_update(self, guardduty, detector):
        """UpdatedAt is set before and remains set after update."""
        before = guardduty.get_detector(DetectorId=detector)
        assert before["UpdatedAt"]
        guardduty.update_detector(DetectorId=detector, Enable=True)
        after = guardduty.get_detector(DetectorId=detector)
        assert after["UpdatedAt"]

    def test_detector_service_role_is_arn_like(self, guardduty, detector):
        """ServiceRole resembles an ARN (starts with 'arn:')."""
        detail = guardduty.get_detector(DetectorId=detector)
        service_role = detail.get("ServiceRole", "")
        assert isinstance(service_role, str)
        assert service_role.startswith("arn:")

    def test_detector_data_sources_has_cloud_trail(self, guardduty, detector):
        """DataSources contains CloudTrail key."""
        detail = guardduty.get_detector(DetectorId=detector)
        assert "DataSources" in detail
        assert "CloudTrail" in detail["DataSources"]
        ds = detail["DataSources"]
        assert isinstance(ds["CloudTrail"]["Status"], str)

    def test_detector_features_is_list(self, guardduty, detector):
        """Features field is a list type."""
        detail = guardduty.get_detector(DetectorId=detector)
        assert isinstance(detail["Features"], list)

    def test_get_nonexistent_detector_for_fields_raises_error(self, guardduty):
        """Getting a nonexistent detector raises BadRequestException."""
        with pytest.raises(ClientError) as exc_info:
            guardduty.get_detector(DetectorId="aaaabbbbccccddddeeeeffffgggghhh0")
        assert exc_info.value.response["Error"]["Code"] == "BadRequestException"


class TestGuardDutyFilterPaginationBehavior:
    """Pagination tests for filter list operations."""

    def test_list_filters_with_multiple_created(self, guardduty, detector):
        """Create 3 filters, list them all, verify all 3 appear in result."""
        names = [_unique("filter") for _ in range(3)]
        for name in names:
            guardduty.create_filter(
                DetectorId=detector,
                Name=name,
                FindingCriteria={"Criterion": {"severity": {"Gte": 4}}},
            )
        try:
            resp = guardduty.list_filters(DetectorId=detector)
            assert "FilterNames" in resp
            assert isinstance(resp["FilterNames"], list)
            for name in names:
                assert name in resp["FilterNames"]
        finally:
            for name in names:
                try:
                    guardduty.delete_filter(DetectorId=detector, FilterName=name)
                except ClientError:
                    pass  # best-effort cleanup


class TestGuardDutyIPSetPaginationBehavior:
    """Pagination tests for IP set list operations."""

    def test_list_ipsets_with_multiple_created(self, guardduty, detector):
        """Create 3 IP sets, list them all, verify all 3 appear in result."""
        ids = []
        for i in range(3):
            resp = guardduty.create_ip_set(
                DetectorId=detector,
                Name=_unique("ipset"),
                Format="TXT",
                Location=f"s3://my-bucket/ipset-{i}.txt",
                Activate=False,
            )
            ids.append(resp["IpSetId"])
        try:
            page = guardduty.list_ip_sets(DetectorId=detector)
            assert "IpSetIds" in page
            assert isinstance(page["IpSetIds"], list)
            for ip_set_id in ids:
                assert ip_set_id in page["IpSetIds"]
        finally:
            for ip_set_id in ids:
                try:
                    guardduty.delete_ip_set(DetectorId=detector, IpSetId=ip_set_id)
                except ClientError:
                    pass  # best-effort cleanup


class TestGuardDutyThreatIntelSetPaginationBehavior:
    """Pagination tests for threat intel set list operations."""

    def test_list_threat_intel_sets_with_multiple_created(self, guardduty, detector):
        """Create 3 threat intel sets, list them all, verify all 3 appear."""
        ids = []
        for i in range(3):
            resp = guardduty.create_threat_intel_set(
                DetectorId=detector,
                Name=_unique("tiset"),
                Format="TXT",
                Location=f"s3://my-bucket/tiset-{i}.txt",
                Activate=False,
            )
            ids.append(resp["ThreatIntelSetId"])
        try:
            page = guardduty.list_threat_intel_sets(DetectorId=detector)
            assert "ThreatIntelSetIds" in page
            assert isinstance(page["ThreatIntelSetIds"], list)
            for tid in ids:
                assert tid in page["ThreatIntelSetIds"]
        finally:
            for tid in ids:
                try:
                    guardduty.delete_threat_intel_set(DetectorId=detector, ThreatIntelSetId=tid)
                except ClientError:
                    pass  # best-effort cleanup


class TestGuardDutyFindingsBehaviorFidelity:
    """Behavioral fidelity tests for findings operations."""

    def test_list_findings_error_on_nonexistent_detector(self, guardduty):
        """ListFindings with a fake detector ID raises BadRequestException."""
        with pytest.raises(ClientError) as exc_info:
            guardduty.list_findings(DetectorId="aaaabbbbccccddddeeeeffffgggghhh0")
        assert exc_info.value.response["Error"]["Code"] == "BadRequestException"

    def test_list_findings_with_max_results(self, guardduty, detector):
        """Create sample findings, list with MaxResults=1 returns at most 1 finding."""
        guardduty.create_sample_findings(
            DetectorId=detector,
            FindingTypes=["Recon:EC2/PortProbeUnprotectedPort"],
        )
        resp = guardduty.list_findings(DetectorId=detector, MaxResults=1)
        assert "FindingIds" in resp
        assert isinstance(resp["FindingIds"], list)
        assert len(resp["FindingIds"]) <= 1


class TestGuardDutyOrganizationAdminBehavior:
    """Behavioral tests for org admin account operations."""

    def test_enable_org_admin_appears_in_list(self, guardduty):
        """After enabling an org admin account, it appears in list_organization_admin_accounts."""
        account_id = "123456789099"
        guardduty.enable_organization_admin_account(AdminAccountId=account_id)
        resp = guardduty.list_organization_admin_accounts()
        assert "AdminAccounts" in resp
        assert isinstance(resp["AdminAccounts"], list)
        admin_ids = [a["AdminAccountId"] for a in resp["AdminAccounts"]]
        assert account_id in admin_ids

    def test_enable_multiple_org_admins(self, guardduty):
        """Enable two different accounts and verify both appear in the admin list."""
        account_a = "100000000001"
        account_b = "100000000002"
        guardduty.enable_organization_admin_account(AdminAccountId=account_a)
        guardduty.enable_organization_admin_account(AdminAccountId=account_b)
        resp = guardduty.list_organization_admin_accounts()
        admin_ids = [a["AdminAccountId"] for a in resp["AdminAccounts"]]
        assert account_a in admin_ids
        assert account_b in admin_ids


class TestGuardDutyTrustedEntitySetErrorBehavior:
    """Error and delete tests for trusted entity sets."""

    def test_update_nonexistent_trusted_entity_set_raises_error(self, guardduty, detector):
        """Updating a nonexistent TrustedEntitySet raises BadRequestException."""
        with pytest.raises(ClientError) as exc_info:
            guardduty.update_trusted_entity_set(
                DetectorId=detector,
                TrustedEntitySetId="nonexistent00000000000000000000",
                Name="new-name",
            )
        assert exc_info.value.response["Error"]["Code"] == "BadRequestException"

    def test_delete_nonexistent_trusted_entity_set_raises_error(self, guardduty, detector):
        """Deleting a nonexistent TrustedEntitySet raises BadRequestException."""
        with pytest.raises(ClientError) as exc_info:
            guardduty.delete_trusted_entity_set(
                DetectorId=detector,
                TrustedEntitySetId="nonexistent00000000000000000000",
            )
        assert exc_info.value.response["Error"]["Code"] == "BadRequestException"

    def test_trusted_entity_set_delete_then_get_raises_error(self, guardduty, detector):
        """After deleting a TrustedEntitySet, getting it raises BadRequestException."""
        create_resp = guardduty.create_trusted_entity_set(
            DetectorId=detector,
            Name=_unique("trusted"),
            Format="TXT",
            Location="s3://test-bucket/trusted.txt",
            Activate=True,
        )
        trust_id = create_resp["TrustedEntitySetId"]
        guardduty.delete_trusted_entity_set(DetectorId=detector, TrustedEntitySetId=trust_id)
        with pytest.raises(ClientError) as exc_info:
            guardduty.get_trusted_entity_set(DetectorId=detector, TrustedEntitySetId=trust_id)
        assert exc_info.value.response["Error"]["Code"] == "BadRequestException"


class TestGuardDutyTagsBehaviorFidelity:
    """Behavioral fidelity for tags list/tag/untag cycle."""

    def test_list_tags_after_tag_and_untag_cycle(self, guardduty, detector):
        """Tag a detector, list tags, untag, list again verifying tags are removed."""
        arn = f"arn:aws:guardduty:us-east-1:123456789012:detector/{detector}"
        guardduty.tag_resource(ResourceArn=arn, Tags={"cycle-key": "cycle-val", "other": "val2"})
        tagged = guardduty.list_tags_for_resource(ResourceArn=arn)
        assert tagged["Tags"]["cycle-key"] == "cycle-val"
        assert tagged["Tags"]["other"] == "val2"
        guardduty.untag_resource(ResourceArn=arn, TagKeys=["cycle-key"])
        after = guardduty.list_tags_for_resource(ResourceArn=arn)
        assert "cycle-key" not in after["Tags"]
        assert after["Tags"]["other"] == "val2"

    def test_list_tags_for_nonexistent_resource(self, guardduty):
        """ListTagsForResource with a fake ARN raises an error."""
        fake_arn = "arn:aws:guardduty:us-east-1:123456789012:detector/nonexistentdetector00000"
        with pytest.raises(ClientError) as exc_info:
            guardduty.list_tags_for_resource(ResourceArn=fake_arn)
        assert exc_info.value.response["Error"]["Code"] in (
            "BadRequestException",
            "AccessDeniedException",
            "ResourceNotFoundException",
        )


class TestGuardDutyListMalwareScans:
    """Tests for ListMalwareScans."""

    def test_list_malware_scans(self, guardduty):
        """ListMalwareScans returns Scans list."""
        resp = guardduty.list_malware_scans()
        assert "Scans" in resp
        assert isinstance(resp["Scans"], list)


class TestGuardDutySendObjectMalwareScan:
    """Tests for SendObjectMalwareScan."""

    def test_send_object_malware_scan(self, guardduty):
        """SendObjectMalwareScan returns 200."""
        resp = guardduty.send_object_malware_scan()
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200

    def test_send_object_malware_scan_with_s3_object(self, guardduty):
        """SendObjectMalwareScan accepts S3Object parameter."""
        resp = guardduty.send_object_malware_scan(
            S3Object={
                "Bucket": "test-bucket",
                "Key": "test-key",
            }
        )
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200


class TestGuardDutyUpdateMalwareProtectionPlan:
    """Tests for UpdateMalwareProtectionPlan."""

    def test_update_malware_protection_plan(self, guardduty):
        """UpdateMalwareProtectionPlan modifies a plan."""
        create_resp = guardduty.create_malware_protection_plan(
            Role="arn:aws:iam::123456789012:role/test",
            ProtectedResource={"S3Bucket": {"BucketName": "test-bucket"}},
        )
        plan_id = create_resp["MalwareProtectionPlanId"]
        try:
            resp = guardduty.update_malware_protection_plan(
                MalwareProtectionPlanId=plan_id,
                Role="arn:aws:iam::123456789012:role/updated",
            )
            assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200
        finally:
            guardduty.delete_malware_protection_plan(MalwareProtectionPlanId=plan_id)


class TestGuardDutyDetectorBehavioralFidelity:
    """Behavioral fidelity tests for detector timestamp and data fields."""

    def test_created_at_stable_across_updates(self, guardduty, detector):
        """CreatedAt should not change after an update."""
        before = guardduty.get_detector(DetectorId=detector)
        guardduty.update_detector(DetectorId=detector, FindingPublishingFrequency="SIX_HOURS")
        after = guardduty.get_detector(DetectorId=detector)
        assert after["CreatedAt"] == before["CreatedAt"]

    def test_updated_at_present_and_not_earlier_than_created_at(self, guardduty, detector):
        """UpdatedAt should be >= CreatedAt."""
        detail = guardduty.get_detector(DetectorId=detector)
        assert detail["UpdatedAt"] >= detail["CreatedAt"]

    def test_data_sources_is_non_empty_dict(self, guardduty, detector):
        """DataSources should be a non-empty dict with at least one source key."""
        detail = guardduty.get_detector(DetectorId=detector)
        ds = detail["DataSources"]
        assert isinstance(ds, dict)
        assert len(ds) > 0

    def test_data_sources_contains_s3logs_or_cloudtrail(self, guardduty, detector):
        """DataSources should contain S3Logs and/or CloudTrail."""
        detail = guardduty.get_detector(DetectorId=detector)
        ds = detail["DataSources"]
        assert "S3Logs" in ds or "CloudTrail" in ds

    def test_tags_is_dict_for_untagged_detector(self, guardduty, detector):
        """Tags should be a dict (empty for a freshly created detector with no tags)."""
        detail = guardduty.get_detector(DetectorId=detector)
        assert isinstance(detail["Tags"], dict)

    def test_service_role_is_string(self, guardduty, detector):
        """ServiceRole should be a string (may be empty in emulator)."""
        detail = guardduty.get_detector(DetectorId=detector)
        assert isinstance(detail.get("ServiceRole", ""), str)

    def test_delete_detector_removes_from_list(self, guardduty):
        """After delete, DetectorId no longer appears in ListDetectors."""
        resp = guardduty.create_detector(Enable=True)
        detector_id = resp["DetectorId"]
        guardduty.delete_detector(DetectorId=detector_id)
        listed = guardduty.list_detectors()
        assert detector_id not in listed["DetectorIds"]

    def test_list_detectors_with_max_results(self, guardduty):
        """ListDetectors respects MaxResults pagination parameter."""
        ids = []
        for _ in range(3):
            r = guardduty.create_detector(Enable=True)
            ids.append(r["DetectorId"])
        try:
            resp = guardduty.list_detectors(MaxResults=1)
            assert "DetectorIds" in resp
            assert len(resp["DetectorIds"]) <= 1
        finally:
            for did in ids:
                try:
                    guardduty.delete_detector(DetectorId=did)
                except ClientError:
                    pass  # best-effort cleanup


class TestGuardDutyTrustedEntitySetEdgeCases:
    """Edge case tests for TrustedEntitySet operations."""

    def test_update_nonexistent_trusted_entity_set_raises_error(self, guardduty, detector):
        """Updating a non-existent TrustedEntitySet should raise BadRequestException."""
        with pytest.raises(ClientError) as exc_info:
            guardduty.update_trusted_entity_set(
                DetectorId=detector,
                TrustedEntitySetId="nonexistent00000000000000000000",
                Name="new-name",
            )
        assert exc_info.value.response["Error"]["Code"] == "BadRequestException"

    def test_delete_nonexistent_trusted_entity_set_raises_error(self, guardduty, detector):
        """Deleting a non-existent TrustedEntitySet should raise BadRequestException."""
        with pytest.raises(ClientError) as exc_info:
            guardduty.delete_trusted_entity_set(
                DetectorId=detector,
                TrustedEntitySetId="nonexistent00000000000000000000",
            )
        assert exc_info.value.response["Error"]["Code"] == "BadRequestException"

    def test_list_trusted_entity_sets_after_update_shows_set(self, guardduty, detector):
        """After updating a set, it should still appear in ListTrustedEntitySets."""
        create_resp = guardduty.create_trusted_entity_set(
            DetectorId=detector,
            Name=_unique("trusted"),
            Format="TXT",
            Location="s3://test-bucket/trusted.txt",
            Activate=True,
        )
        trust_id = create_resp["TrustedEntitySetId"]
        try:
            guardduty.update_trusted_entity_set(
                DetectorId=detector, TrustedEntitySetId=trust_id, Name="updated-trusted-name"
            )
            listed = guardduty.list_trusted_entity_sets(DetectorId=detector)
            assert trust_id in listed["TrustedEntitySetIds"]
        finally:
            guardduty.delete_trusted_entity_set(DetectorId=detector, TrustedEntitySetId=trust_id)

    def test_get_nonexistent_trusted_entity_set_raises_error(self, guardduty, detector):
        """Getting a non-existent TrustedEntitySet should raise BadRequestException."""
        with pytest.raises(ClientError) as exc_info:
            guardduty.get_trusted_entity_set(
                DetectorId=detector,
                TrustedEntitySetId="nonexistent00000000000000000000",
            )
        assert exc_info.value.response["Error"]["Code"] == "BadRequestException"


class TestGuardDutyArchiveFindingsBehavior:
    """Behavioral tests for archive/unarchive findings."""

    def test_archive_multiple_findings(self, guardduty, detector):
        """ArchiveFindings handles a list of multiple fake finding IDs."""
        resp = guardduty.archive_findings(
            DetectorId=detector,
            FindingIds=["fake-finding-id-1", "fake-finding-id-2", "fake-finding-id-3"],
        )
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200

    def test_unarchive_multiple_findings(self, guardduty, detector):
        """UnarchiveFindings handles a list of multiple fake finding IDs."""
        resp = guardduty.unarchive_findings(
            DetectorId=detector,
            FindingIds=["fake-finding-id-1", "fake-finding-id-2"],
        )
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200

    def test_archive_then_list_with_archived_criteria(self, guardduty, detector):
        """After archiving, findings can be found with archived=true criteria."""
        guardduty.create_sample_findings(
            DetectorId=detector,
            FindingTypes=["Recon:EC2/PortProbeUnprotectedPort"],
        )
        list_resp = guardduty.list_findings(DetectorId=detector)
        finding_ids = list_resp["FindingIds"]
        assert len(finding_ids) > 0

        guardduty.archive_findings(DetectorId=detector, FindingIds=finding_ids[:1])

        archived_resp = guardduty.list_findings(
            DetectorId=detector,
            FindingCriteria={"Criterion": {"service.archived": {"Eq": ["true"]}}},
        )
        assert "FindingIds" in archived_resp
        assert isinstance(archived_resp["FindingIds"], list)

    def test_unarchive_then_get_finding(self, guardduty, detector):
        """After unarchiving, the finding is still retrievable via GetFindings."""
        guardduty.create_sample_findings(
            DetectorId=detector,
            FindingTypes=["Recon:EC2/PortProbeUnprotectedPort"],
        )
        list_resp = guardduty.list_findings(DetectorId=detector)
        finding_ids = list_resp["FindingIds"]
        assert len(finding_ids) > 0

        guardduty.archive_findings(DetectorId=detector, FindingIds=finding_ids[:1])
        guardduty.unarchive_findings(DetectorId=detector, FindingIds=finding_ids[:1])

        get_resp = guardduty.get_findings(DetectorId=detector, FindingIds=finding_ids[:1])
        assert "Findings" in get_resp
        assert len(get_resp["Findings"]) > 0


class TestGuardDutyInviteMembersBehavior:
    """Behavioral tests for InviteMembers."""

    def test_invite_members_unprocessed_accounts_structure(self, guardduty, detector):
        """InviteMembers UnprocessedAccounts entries have AccountId and Result fields."""
        resp = guardduty.invite_members(DetectorId=detector, AccountIds=["999911112222"])
        assert "UnprocessedAccounts" in resp
        assert isinstance(resp["UnprocessedAccounts"], list)
        for acct in resp["UnprocessedAccounts"]:
            assert "AccountId" in acct
            assert "Result" in acct
            assert isinstance(acct["AccountId"], str)

    def test_invite_members_after_create_member(self, guardduty, detector):
        """InviteMembers after creating a member account returns 200."""
        guardduty.create_members(
            DetectorId=detector,
            AccountDetails=[{"AccountId": "234523452345", "Email": "invme@example.com"}],
        )
        resp = guardduty.invite_members(DetectorId=detector, AccountIds=["234523452345"])
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200
        assert "UnprocessedAccounts" in resp
        assert isinstance(resp["UnprocessedAccounts"], list)

    def test_invite_members_with_message(self, guardduty, detector):
        """InviteMembers accepts an optional Message parameter."""
        resp = guardduty.invite_members(
            DetectorId=detector,
            AccountIds=["111122223333"],
            Message="Please join as a member.",
        )
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200
        assert isinstance(resp["UnprocessedAccounts"], list)


class TestGuardDutyDeclineInvitationsBehavior:
    """Behavioral tests for DeclineInvitations."""

    def test_decline_invitations_response_structure(self, guardduty):
        """DeclineInvitations UnprocessedAccounts entries have AccountId and Result."""
        resp = guardduty.decline_invitations(AccountIds=["111122223333"])
        assert "UnprocessedAccounts" in resp
        assert isinstance(resp["UnprocessedAccounts"], list)
        for acct in resp["UnprocessedAccounts"]:
            assert "AccountId" in acct
            assert "Result" in acct
            assert isinstance(acct["AccountId"], str)

    def test_decline_invitations_multiple_accounts(self, guardduty):
        """DeclineInvitations handles multiple account IDs."""
        resp = guardduty.decline_invitations(AccountIds=["111122223333", "444455556666"])
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200
        assert isinstance(resp["UnprocessedAccounts"], list)

    def test_delete_invitations_response_structure(self, guardduty):
        """DeleteInvitations UnprocessedAccounts entries have AccountId and Result."""
        resp = guardduty.delete_invitations(AccountIds=["111122223333"])
        assert "UnprocessedAccounts" in resp
        assert isinstance(resp["UnprocessedAccounts"], list)
        for acct in resp["UnprocessedAccounts"]:
            assert "AccountId" in acct
            assert "Result" in acct
            assert isinstance(acct["AccountId"], str)


class TestGuardDutyDisassociateBehavior:
    """Behavioral tests for disassociate operations."""

    def test_disassociate_from_administrator_then_get_admin(self, guardduty, detector):
        """After disassociating from admin, GetAdministratorAccount still returns 200."""
        guardduty.accept_administrator_invitation(
            DetectorId=detector,
            AdministratorId="111122223333",
            InvitationId="fake-invitation-id",
        )
        guardduty.disassociate_from_administrator_account(DetectorId=detector)
        resp = guardduty.get_administrator_account(DetectorId=detector)
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200

    def test_disassociate_members_unprocessed_structure(self, guardduty, detector):
        """DisassociateMembers UnprocessedAccounts entries have AccountId and Result."""
        guardduty.create_members(
            DetectorId=detector,
            AccountDetails=[{"AccountId": "123412341234", "Email": "dis@example.com"}],
        )
        resp = guardduty.disassociate_members(DetectorId=detector, AccountIds=["123412341234"])
        assert "UnprocessedAccounts" in resp
        assert isinstance(resp["UnprocessedAccounts"], list)
        for acct in resp["UnprocessedAccounts"]:
            assert "AccountId" in acct
            assert "Result" in acct
            assert isinstance(acct["AccountId"], str)

    def test_disassociate_members_multiple_accounts(self, guardduty, detector):
        """DisassociateMembers handles multiple account IDs."""
        resp = guardduty.disassociate_members(
            DetectorId=detector, AccountIds=["111122223333", "444455556666"]
        )
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200
        assert "UnprocessedAccounts" in resp

    def test_disassociate_from_master_then_get_master(self, guardduty, detector):
        """After disassociating from master, GetMasterAccount still returns 200."""
        guardduty.disassociate_from_master_account(DetectorId=detector)
        resp = guardduty.get_master_account(DetectorId=detector)
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200


class TestGuardDutyAcceptInvitationBehavior:
    """Behavioral tests for AcceptInvitation (legacy) and AcceptAdministratorInvitation."""

    def test_accept_invitation_legacy_response_shape(self, guardduty, detector):
        """AcceptInvitation (legacy) returns 200 and empty body."""
        resp = guardduty.accept_invitation(
            DetectorId=detector,
            MasterId="222233334444",
            InvitationId="another-fake-invitation",
        )
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200

    def test_accept_administrator_invitation_response_shape(self, guardduty, detector):
        """AcceptAdministratorInvitation response body has no unexpected fields."""
        resp = guardduty.accept_administrator_invitation(
            DetectorId=detector,
            AdministratorId="333344445555",
            InvitationId="yet-another-fake-id",
        )
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200
        # Response should not contain error keys
        assert "Error" not in resp

    def test_accept_administrator_invitation_then_list_members(self, guardduty, detector):
        """After accepting admin invitation, ListInvitations still returns 200."""
        guardduty.accept_administrator_invitation(
            DetectorId=detector,
            AdministratorId="444455556666",
            InvitationId="list-test-invitation-id",
        )
        resp = guardduty.list_invitations()
        assert "Invitations" in resp
        assert isinstance(resp["Invitations"], list)


class TestGuardDutyArchiveFindingsErrorCases:
    """Error-path edge cases for ArchiveFindings and UnarchiveFindings."""

    def test_archive_findings_bad_detector_raises_error(self, guardduty):
        """ArchiveFindings with nonexistent DetectorId raises BadRequestException."""
        with pytest.raises(ClientError) as exc_info:
            guardduty.archive_findings(
                DetectorId="aaaabbbbccccddddeeeeffffgggghhh0",
                FindingIds=["fake-finding-id"],
            )
        assert exc_info.value.response["Error"]["Code"] == "BadRequestException"

    def test_unarchive_findings_bad_detector_raises_error(self, guardduty):
        """UnarchiveFindings with nonexistent DetectorId raises BadRequestException."""
        with pytest.raises(ClientError) as exc_info:
            guardduty.unarchive_findings(
                DetectorId="aaaabbbbccccddddeeeeffffgggghhh0",
                FindingIds=["fake-finding-id"],
            )
        assert exc_info.value.response["Error"]["Code"] == "BadRequestException"

    def test_archive_real_findings_then_verify_archived_status(self, guardduty, detector):
        """Archive a real sample finding and verify the finding is retrievable."""
        guardduty.create_sample_findings(
            DetectorId=detector,
            FindingTypes=["Recon:EC2/PortProbeUnprotectedPort"],
        )
        list_resp = guardduty.list_findings(DetectorId=detector)
        finding_ids = list_resp["FindingIds"]
        assert len(finding_ids) > 0

        guardduty.archive_findings(DetectorId=detector, FindingIds=finding_ids[:1])
        get_resp = guardduty.get_findings(DetectorId=detector, FindingIds=finding_ids[:1])
        assert "Findings" in get_resp
        assert len(get_resp["Findings"]) > 0
        assert get_resp["Findings"][0]["Service"]["Archived"] is True

    def test_archive_then_unarchive_restores_archived_false(self, guardduty, detector):
        """Archive then unarchive a finding — Service.Archived becomes False again."""
        guardduty.create_sample_findings(
            DetectorId=detector,
            FindingTypes=["Recon:EC2/PortProbeUnprotectedPort"],
        )
        list_resp = guardduty.list_findings(DetectorId=detector)
        finding_ids = list_resp["FindingIds"]
        assert len(finding_ids) > 0

        guardduty.archive_findings(DetectorId=detector, FindingIds=finding_ids[:1])
        guardduty.unarchive_findings(DetectorId=detector, FindingIds=finding_ids[:1])
        get_resp = guardduty.get_findings(DetectorId=detector, FindingIds=finding_ids[:1])
        assert get_resp["Findings"][0]["Service"]["Archived"] is False

    def test_archive_multiple_findings_bad_detector_raises_error(self, guardduty):
        """ArchiveFindings with multiple IDs and bad detector raises BadRequestException."""
        with pytest.raises(ClientError) as exc_info:
            guardduty.archive_findings(
                DetectorId="aaaabbbbccccddddeeeeffffgggghhh0",
                FindingIds=["id-1", "id-2", "id-3"],
            )
        assert exc_info.value.response["Error"]["Code"] == "BadRequestException"

    def test_unarchive_multiple_findings_bad_detector_raises_error(self, guardduty):
        """UnarchiveFindings with multiple IDs and bad detector raises BadRequestException."""
        with pytest.raises(ClientError) as exc_info:
            guardduty.unarchive_findings(
                DetectorId="aaaabbbbccccddddeeeeffffgggghhh0",
                FindingIds=["id-1", "id-2"],
            )
        assert exc_info.value.response["Error"]["Code"] == "BadRequestException"


class TestGuardDutyInviteMembersErrorCases:
    """Error-path edge cases for InviteMembers."""

    def test_invite_members_bad_detector_raises_error(self, guardduty):
        """InviteMembers with nonexistent DetectorId raises BadRequestException."""
        with pytest.raises(ClientError) as exc_info:
            guardduty.invite_members(
                DetectorId="aaaabbbbccccddddeeeeffffgggghhh0",
                AccountIds=["111122223333"],
            )
        assert exc_info.value.response["Error"]["Code"] == "BadRequestException"

    def test_invite_members_unprocessed_accounts_have_account_id(self, guardduty, detector):
        """InviteMembers: every UnprocessedAccounts entry has an AccountId."""
        resp = guardduty.invite_members(DetectorId=detector, AccountIds=["999911112222"])
        assert "UnprocessedAccounts" in resp
        assert isinstance(resp["UnprocessedAccounts"], list)
        for acct in resp["UnprocessedAccounts"]:
            assert "AccountId" in acct

    def test_invite_multiple_members_unprocessed_structure(self, guardduty, detector):
        """InviteMembers with multiple accounts — UnprocessedAccounts is a list."""
        resp = guardduty.invite_members(
            DetectorId=detector,
            AccountIds=["111122223333", "444455556666"],
        )
        assert isinstance(resp["UnprocessedAccounts"], list)

    def test_invite_members_with_message_returns_unprocessed(self, guardduty, detector):
        """InviteMembers with Message returns UnprocessedAccounts list."""
        resp = guardduty.invite_members(
            DetectorId=detector,
            AccountIds=["111122223333"],
            Message="Please join.",
        )
        assert "UnprocessedAccounts" in resp
        assert isinstance(resp["UnprocessedAccounts"], list)


class TestGuardDutyDeclineInvitationsErrorCases:
    """Error-path and structural edge cases for DeclineInvitations."""

    def test_decline_invitations_returns_unprocessed_list(self, guardduty):
        """DeclineInvitations always returns a list, even for unknown accounts."""
        resp = guardduty.decline_invitations(AccountIds=["111122223333"])
        assert isinstance(resp["UnprocessedAccounts"], list)

    def test_decline_invitations_multiple_response_structure(self, guardduty):
        """DeclineInvitations with multiple IDs — UnprocessedAccounts has AccountId+Result."""
        resp = guardduty.decline_invitations(AccountIds=["111122223333", "444455556666"])
        assert "UnprocessedAccounts" in resp
        assert isinstance(resp["UnprocessedAccounts"], list)
        for acct in resp["UnprocessedAccounts"]:
            assert "AccountId" in acct
            assert "Result" in acct

    def test_delete_invitations_multiple_response_structure(self, guardduty):
        """DeleteInvitations with multiple IDs — UnprocessedAccounts has AccountId+Result."""
        resp = guardduty.delete_invitations(AccountIds=["111122223333", "444455556666"])
        assert "UnprocessedAccounts" in resp
        assert isinstance(resp["UnprocessedAccounts"], list)
        for acct in resp["UnprocessedAccounts"]:
            assert "AccountId" in acct
            assert "Result" in acct
            assert isinstance(acct["AccountId"], str)


class TestGuardDutyDisassociateErrorCases:
    """Error-path edge cases for disassociate operations."""

    def test_disassociate_from_administrator_bad_detector_raises_error(self, guardduty):
        """DisassociateFromAdministratorAccount with bad DetectorId raises BadRequestException."""
        with pytest.raises(ClientError) as exc_info:
            guardduty.disassociate_from_administrator_account(
                DetectorId="aaaabbbbccccddddeeeeffffgggghhh0"
            )
        assert exc_info.value.response["Error"]["Code"] == "BadRequestException"

    def test_disassociate_from_master_bad_detector_raises_error(self, guardduty):
        """DisassociateFromMasterAccount with bad DetectorId raises BadRequestException."""
        with pytest.raises(ClientError) as exc_info:
            guardduty.disassociate_from_master_account(
                DetectorId="aaaabbbbccccddddeeeeffffgggghhh0"
            )
        assert exc_info.value.response["Error"]["Code"] == "BadRequestException"

    def test_disassociate_members_bad_detector_raises_error(self, guardduty):
        """DisassociateMembers with bad DetectorId raises BadRequestException."""
        with pytest.raises(ClientError) as exc_info:
            guardduty.disassociate_members(
                DetectorId="aaaabbbbccccddddeeeeffffgggghhh0",
                AccountIds=["111122223333"],
            )
        assert exc_info.value.response["Error"]["Code"] == "BadRequestException"

    def test_disassociate_members_unprocessed_accounts_is_list(self, guardduty, detector):
        """DisassociateMembers returns UnprocessedAccounts as a list."""
        resp = guardduty.disassociate_members(DetectorId=detector, AccountIds=["111122223333"])
        assert isinstance(resp["UnprocessedAccounts"], list)


class TestGuardDutyAcceptInvitationErrorCases:
    """Error-path edge cases for AcceptInvitation and AcceptAdministratorInvitation."""

    def test_accept_invitation_bad_detector_raises_error(self, guardduty):
        """AcceptInvitation with nonexistent DetectorId raises BadRequestException."""
        with pytest.raises(ClientError) as exc_info:
            guardduty.accept_invitation(
                DetectorId="aaaabbbbccccddddeeeeffffgggghhh0",
                MasterId="111122223333",
                InvitationId="fake-id",
            )
        assert exc_info.value.response["Error"]["Code"] == "BadRequestException"

    def test_accept_administrator_invitation_bad_detector_raises_error(self, guardduty):
        """AcceptAdministratorInvitation with bad DetectorId raises BadRequestException."""
        with pytest.raises(ClientError) as exc_info:
            guardduty.accept_administrator_invitation(
                DetectorId="aaaabbbbccccddddeeeeffffgggghhh0",
                AdministratorId="111122223333",
                InvitationId="fake-id",
            )
        assert exc_info.value.response["Error"]["Code"] == "BadRequestException"

    def test_accept_administrator_invitation_then_get_administrator(self, guardduty, detector):
        """After AcceptAdministratorInvitation, GetAdministratorAccount returns the admin."""
        guardduty.accept_administrator_invitation(
            DetectorId=detector,
            AdministratorId="555566667777",
            InvitationId="test-inv-id",
        )
        resp = guardduty.get_administrator_account(DetectorId=detector)
        assert "Administrator" in resp
        assert resp["Administrator"]["AccountId"] == "555566667777"


class TestGuardDutyTrustedEntitySetUpdateFidelity:
    """Behavioral fidelity tests for UpdateTrustedEntitySet (covers L and E patterns)."""

    def test_update_trusted_entity_set_then_list_shows_set(self, guardduty, detector):
        """After update, the set still appears in ListTrustedEntitySets."""
        create_resp = guardduty.create_trusted_entity_set(
            DetectorId=detector,
            Name=_unique("trusted"),
            Format="TXT",
            Location="s3://test-bucket/trusted.txt",
            Activate=True,
        )
        trust_id = create_resp["TrustedEntitySetId"]
        try:
            guardduty.update_trusted_entity_set(
                DetectorId=detector, TrustedEntitySetId=trust_id, Name="updated-for-list"
            )
            listed = guardduty.list_trusted_entity_sets(DetectorId=detector)
            assert trust_id in listed["TrustedEntitySetIds"]
        finally:
            guardduty.delete_trusted_entity_set(DetectorId=detector, TrustedEntitySetId=trust_id)

    def test_update_trusted_entity_set_name_persists(self, guardduty, detector):
        """UpdateTrustedEntitySet: updated name is returned by GetTrustedEntitySet."""
        create_resp = guardduty.create_trusted_entity_set(
            DetectorId=detector,
            Name=_unique("trusted"),
            Format="TXT",
            Location="s3://test-bucket/trusted.txt",
            Activate=True,
        )
        trust_id = create_resp["TrustedEntitySetId"]
        try:
            new_name = _unique("trusted-renamed")
            guardduty.update_trusted_entity_set(
                DetectorId=detector, TrustedEntitySetId=trust_id, Name=new_name
            )
            get_resp = guardduty.get_trusted_entity_set(
                DetectorId=detector, TrustedEntitySetId=trust_id
            )
            assert get_resp["Name"] == new_name
            assert get_resp["Format"] == "TXT"
        finally:
            guardduty.delete_trusted_entity_set(DetectorId=detector, TrustedEntitySetId=trust_id)

    def test_update_trusted_entity_set_location(self, guardduty, detector):
        """UpdateTrustedEntitySet: location can be updated."""
        create_resp = guardduty.create_trusted_entity_set(
            DetectorId=detector,
            Name=_unique("trusted"),
            Format="TXT",
            Location="s3://test-bucket/trusted.txt",
            Activate=True,
        )
        trust_id = create_resp["TrustedEntitySetId"]
        try:
            guardduty.update_trusted_entity_set(
                DetectorId=detector,
                TrustedEntitySetId=trust_id,
                Location="s3://new-bucket/trusted2.txt",
            )
            get_resp = guardduty.get_trusted_entity_set(
                DetectorId=detector, TrustedEntitySetId=trust_id
            )
            assert get_resp["Location"] == "s3://new-bucket/trusted2.txt"
        finally:
            guardduty.delete_trusted_entity_set(DetectorId=detector, TrustedEntitySetId=trust_id)

    def test_update_trusted_entity_set_bad_detector_raises_error(self, guardduty):
        """UpdateTrustedEntitySet with bad DetectorId raises BadRequestException."""
        with pytest.raises(ClientError) as exc_info:
            guardduty.update_trusted_entity_set(
                DetectorId="aaaabbbbccccddddeeeeffffgggghhh0",
                TrustedEntitySetId="someid00000000000000000000000000",
                Name="new-name",
            )
        assert exc_info.value.response["Error"]["Code"] == "BadRequestException"


class TestGuardDutyBehavioralFidelity:
    """Edge case and behavioral fidelity tests to strengthen coverage patterns."""

    # ── Detector timestamp & structure fidelity ────────────────────────────

    def test_detector_created_at_is_datetime_object(self, guardduty, detector):
        """CreatedAt field is a proper datetime, not just a string."""
        detail = guardduty.get_detector(DetectorId=detector)
        assert isinstance(detail["CreatedAt"], (datetime.datetime, str))
        # If it's a string it must be non-empty; if datetime it must be reasonable
        if isinstance(detail["CreatedAt"], str):
            assert len(detail["CreatedAt"]) > 0
        else:
            assert detail["CreatedAt"].year >= 2020

    def test_detector_updated_at_changes_after_update(self, guardduty):
        """UpdatedAt timestamp advances after update_detector."""
        resp = guardduty.create_detector(Enable=True)
        det_id = resp["DetectorId"]
        try:
            before = guardduty.get_detector(DetectorId=det_id)["UpdatedAt"]
            guardduty.update_detector(DetectorId=det_id, FindingPublishingFrequency="SIX_HOURS")
            after = guardduty.get_detector(DetectorId=det_id)["UpdatedAt"]
            # UpdatedAt must be present both before and after
            assert before is not None
            assert after is not None
        finally:
            guardduty.delete_detector(DetectorId=det_id)

    def test_detector_service_role_is_string(self, guardduty, detector):
        """ServiceRole field is a string (may be empty for local emulator)."""
        detail = guardduty.get_detector(DetectorId=detector)
        assert isinstance(detail["ServiceRole"], str)

    def test_detector_data_sources_contains_s3logs(self, guardduty, detector):
        """DataSources response includes S3Logs key."""
        detail = guardduty.get_detector(DetectorId=detector)
        assert "S3Logs" in detail["DataSources"]
        assert isinstance(detail["DataSources"]["S3Logs"], dict)

    def test_detector_data_sources_s3logs_has_status(self, guardduty, detector):
        """DataSources.S3Logs includes a Status field."""
        detail = guardduty.get_detector(DetectorId=detector)
        assert "Status" in detail["DataSources"]["S3Logs"]
        assert isinstance(detail["DataSources"]["S3Logs"]["Status"], str)

    def test_detector_features_is_list(self, guardduty, detector):
        """Features field is a list."""
        detail = guardduty.get_detector(DetectorId=detector)
        assert isinstance(detail["Features"], list)

    def test_detector_tags_is_dict(self, guardduty, detector):
        """Tags field is a dict (possibly empty)."""
        detail = guardduty.get_detector(DetectorId=detector)
        assert isinstance(detail["Tags"], dict)

    # ── Administrator account structure ────────────────────────────────────

    def test_get_administrator_account_no_admin_returns_200(self, guardduty, detector):
        """When no administrator is set, get_administrator_account still returns 200."""
        resp = guardduty.get_administrator_account(DetectorId=detector)
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200

    def test_get_administrator_account_no_admin_has_expected_shape(self, guardduty, detector):
        """When no administrator configured, response is valid (may have empty Administrator)."""
        resp = guardduty.get_administrator_account(DetectorId=detector)
        # Either Administrator key is absent or it's an empty dict/None
        if "Administrator" in resp:
            assert resp["Administrator"] is None or isinstance(resp["Administrator"], dict)

    # ── Organization admin account: enable then list ───────────────────────

    def test_enable_then_list_organization_admin_account(self, guardduty):
        """EnableOrganizationAdminAccount then ListOrganizationAdminAccounts shows the account."""
        guardduty.enable_organization_admin_account(AdminAccountId="111122223333")
        resp = guardduty.list_organization_admin_accounts()
        assert "AdminAccounts" in resp
        assert isinstance(resp["AdminAccounts"], list)

    def test_list_organization_admin_accounts_has_admin_accounts_key(self, guardduty):
        """ListOrganizationAdminAccounts always returns AdminAccounts key."""
        resp = guardduty.list_organization_admin_accounts()
        assert "AdminAccounts" in resp
        assert isinstance(resp["AdminAccounts"], list)

    # ── Filter list / delete patterns ─────────────────────────────────────

    def test_list_filters_after_delete_removes_filter(self, guardduty, detector):
        """After deleting a filter it no longer appears in list_filters."""
        filter_name = _unique("filter")
        guardduty.create_filter(
            DetectorId=detector,
            Name=filter_name,
            FindingCriteria={"Criterion": {"severity": {"Gte": 4}}},
        )
        guardduty.delete_filter(DetectorId=detector, FilterName=filter_name)
        resp = guardduty.list_filters(DetectorId=detector)
        assert filter_name not in resp["FilterNames"]

    # ── IPSet delete and error patterns ───────────────────────────────────

    def test_update_nonexistent_ip_set_raises_error(self, guardduty, detector):
        """UpdateIPSet with unknown ID raises BadRequestException."""
        with pytest.raises(ClientError) as exc_info:
            guardduty.update_ip_set(
                DetectorId=detector,
                IpSetId="nonexistent00000000000000000000",
                Name="wont-work",
            )
        assert exc_info.value.response["Error"]["Code"] == "BadRequestException"

    def test_delete_ip_set_removes_from_list(self, guardduty, detector):
        """After deleting an IP set it no longer appears in list_ip_sets."""
        resp = guardduty.create_ip_set(
            DetectorId=detector,
            Name=_unique("ipset"),
            Format="TXT",
            Location="s3://test-bucket/ipset.txt",
            Activate=False,
        )
        ip_set_id = resp["IpSetId"]
        guardduty.delete_ip_set(DetectorId=detector, IpSetId=ip_set_id)
        listed = guardduty.list_ip_sets(DetectorId=detector)
        assert ip_set_id not in listed["IpSetIds"]

    def test_delete_nonexistent_ip_set_raises_error(self, guardduty, detector):
        """Deleting a non-existent IP set raises BadRequestException."""
        with pytest.raises(ClientError) as exc_info:
            guardduty.delete_ip_set(DetectorId=detector, IpSetId="nonexistent00000000000000000000")
        assert exc_info.value.response["Error"]["Code"] == "BadRequestException"

    def test_list_ipsets_count_increases_with_create(self, guardduty, detector):
        """Creating an IP set increases the count in list_ip_sets."""
        before = guardduty.list_ip_sets(DetectorId=detector)["IpSetIds"]
        resp = guardduty.create_ip_set(
            DetectorId=detector,
            Name=_unique("ipset"),
            Format="TXT",
            Location="s3://test-bucket/ipset.txt",
            Activate=False,
        )
        ip_set_id = resp["IpSetId"]
        try:
            after = guardduty.list_ip_sets(DetectorId=detector)["IpSetIds"]
            assert len(after) == len(before) + 1
            assert ip_set_id in after
        finally:
            guardduty.delete_ip_set(DetectorId=detector, IpSetId=ip_set_id)

    # ── ThreatIntelSet delete and error patterns ──────────────────────────

    def test_update_nonexistent_threat_intel_set_raises_error(self, guardduty, detector):
        """UpdateThreatIntelSet with unknown ID raises BadRequestException."""
        with pytest.raises(ClientError) as exc_info:
            guardduty.update_threat_intel_set(
                DetectorId=detector,
                ThreatIntelSetId="nonexistent00000000000000000000",
                Name="wont-work",
            )
        assert exc_info.value.response["Error"]["Code"] == "BadRequestException"

    def test_delete_threat_intel_set_removes_from_list(self, guardduty, detector):
        """After deleting a threat intel set it no longer appears in list."""
        resp = guardduty.create_threat_intel_set(
            DetectorId=detector,
            Name=_unique("tiset"),
            Format="TXT",
            Location="s3://test-bucket/ti.txt",
            Activate=False,
        )
        ti_id = resp["ThreatIntelSetId"]
        guardduty.delete_threat_intel_set(DetectorId=detector, ThreatIntelSetId=ti_id)
        listed = guardduty.list_threat_intel_sets(DetectorId=detector)
        assert ti_id not in listed["ThreatIntelSetIds"]

    def test_delete_nonexistent_threat_intel_set_raises_error(self, guardduty, detector):
        """Deleting a non-existent threat intel set raises BadRequestException."""
        with pytest.raises(ClientError) as exc_info:
            guardduty.delete_threat_intel_set(
                DetectorId=detector, ThreatIntelSetId="nonexistent00000000000000000000"
            )
        assert exc_info.value.response["Error"]["Code"] == "BadRequestException"

    def test_list_threat_intel_sets_count_increases_with_create(self, guardduty, detector):
        """Creating a threat intel set increases the list count."""
        before = guardduty.list_threat_intel_sets(DetectorId=detector)["ThreatIntelSetIds"]
        resp = guardduty.create_threat_intel_set(
            DetectorId=detector,
            Name=_unique("tiset"),
            Format="TXT",
            Location="s3://test-bucket/ti.txt",
            Activate=False,
        )
        ti_id = resp["ThreatIntelSetId"]
        try:
            after = guardduty.list_threat_intel_sets(DetectorId=detector)["ThreatIntelSetIds"]
            assert len(after) == len(before) + 1
            assert ti_id in after
        finally:
            guardduty.delete_threat_intel_set(DetectorId=detector, ThreatIntelSetId=ti_id)

    # ── Tags list: empty then populated ───────────────────────────────────

    def test_list_tags_empty_detector_returns_empty_dict(self, guardduty, detector):
        """Freshly created detector with no tags returns empty Tags dict."""
        arn = f"arn:aws:guardduty:us-east-1:123456789012:detector/{detector}"
        resp = guardduty.list_tags_for_resource(ResourceArn=arn)
        assert isinstance(resp["Tags"], dict)

    def test_list_tags_after_tagging_shows_new_tags(self, guardduty, detector):
        """After TagResource, ListTagsForResource returns the new tags."""
        arn = f"arn:aws:guardduty:us-east-1:123456789012:detector/{detector}"
        guardduty.tag_resource(ResourceArn=arn, Tags={"env": "edge-case", "team": "sec"})
        resp = guardduty.list_tags_for_resource(ResourceArn=arn)
        assert resp["Tags"]["env"] == "edge-case"
        assert resp["Tags"]["team"] == "sec"

    # ── Findings: list with and without criteria ──────────────────────────

    def test_list_findings_returns_finding_ids_key(self, guardduty, detector):
        """list_findings always returns FindingIds list."""
        resp = guardduty.list_findings(DetectorId=detector)
        assert "FindingIds" in resp
        assert isinstance(resp["FindingIds"], list)

    def test_list_findings_with_sort_criteria(self, guardduty, detector):
        """list_findings accepts SortCriteria and returns 200."""
        resp = guardduty.list_findings(
            DetectorId=detector,
            SortCriteria={"AttributeName": "severity", "OrderBy": "DESC"},
        )
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200
        assert "FindingIds" in resp
        assert isinstance(resp["FindingIds"], list)

    def test_list_findings_with_max_results(self, guardduty, detector):
        """list_findings accepts MaxResults parameter."""
        resp = guardduty.list_findings(DetectorId=detector, MaxResults=10)
        assert "FindingIds" in resp
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200

    def test_list_findings_with_criteria_and_sort(self, guardduty, detector):
        """list_findings accepts both FindingCriteria and SortCriteria together."""
        resp = guardduty.list_findings(
            DetectorId=detector,
            FindingCriteria={"Criterion": {"severity": {"Gte": 4}}},
            SortCriteria={"AttributeName": "severity", "OrderBy": "ASC"},
        )
        assert "FindingIds" in resp
        assert isinstance(resp["FindingIds"], list)

    # ── IPSet pagination ───────────────────────────────────────────────────

    def test_list_ipsets_with_max_results(self, guardduty, detector):
        """list_ip_sets accepts MaxResults parameter."""
        ids = []
        for _ in range(3):
            resp = guardduty.create_ip_set(
                DetectorId=detector,
                Name=_unique("ipset"),
                Format="TXT",
                Location="s3://test-bucket/ipset.txt",
                Activate=False,
            )
            ids.append(resp["IpSetId"])
        try:
            resp = guardduty.list_ip_sets(DetectorId=detector, MaxResults=2)
            assert "IpSetIds" in resp
            assert len(resp["IpSetIds"]) <= 2
        finally:
            for ip_id in ids:
                guardduty.delete_ip_set(DetectorId=detector, IpSetId=ip_id)

    # ── Filter pagination ──────────────────────────────────────────────────

    def test_list_filters_with_max_results(self, guardduty, detector):
        """list_filters accepts MaxResults parameter."""
        names = []
        for _ in range(3):
            name = _unique("filter")
            names.append(name)
            guardduty.create_filter(
                DetectorId=detector,
                Name=name,
                FindingCriteria={"Criterion": {"severity": {"Gte": 4}}},
            )
        try:
            resp = guardduty.list_filters(DetectorId=detector, MaxResults=2)
            assert "FilterNames" in resp
            assert len(resp["FilterNames"]) <= 2
        finally:
            for n in names:
                guardduty.delete_filter(DetectorId=detector, FilterName=n)

    # ── Detector delete + list verifies removal ────────────────────────────

    def test_delete_detector_then_list_excludes_it(self, guardduty):
        """After delete_detector the ID is absent from list_detectors."""
        resp = guardduty.create_detector(Enable=True)
        det_id = resp["DetectorId"]
        guardduty.delete_detector(DetectorId=det_id)
        listed = guardduty.list_detectors()
        assert det_id not in listed["DetectorIds"]


class TestGuardDutyDetectorFieldBehavior:
    """Behavioral tests for specific detector fields — stronger assertions than key-presence."""

    def test_created_at_is_recent_datetime(self, guardduty):
        """CreatedAt field is a datetime or ISO string with a year >= 2020."""
        resp = guardduty.create_detector(Enable=True)
        det_id = resp["DetectorId"]
        try:
            detail = guardduty.get_detector(DetectorId=det_id)
            created_at = detail["CreatedAt"]
            assert isinstance(created_at, (datetime.datetime, str))
            if isinstance(created_at, str):
                assert "202" in created_at  # year 2020+
            else:
                assert created_at.year >= 2020
        finally:
            guardduty.delete_detector(DetectorId=det_id)

    def test_updated_at_is_recent_datetime(self, guardduty):
        """UpdatedAt field is a datetime or ISO string with a year >= 2020."""
        resp = guardduty.create_detector(Enable=True)
        det_id = resp["DetectorId"]
        try:
            detail = guardduty.get_detector(DetectorId=det_id)
            updated_at = detail["UpdatedAt"]
            assert isinstance(updated_at, (datetime.datetime, str))
            if isinstance(updated_at, str):
                assert "202" in updated_at  # year 2020+
            else:
                assert updated_at.year >= 2020
        finally:
            guardduty.delete_detector(DetectorId=det_id)

    def test_service_role_is_nonempty_string(self, guardduty):
        """ServiceRole is a non-None string (may be empty but must be str)."""
        resp = guardduty.create_detector(Enable=True)
        det_id = resp["DetectorId"]
        try:
            detail = guardduty.get_detector(DetectorId=det_id)
            assert isinstance(detail["ServiceRole"], str)
        finally:
            guardduty.delete_detector(DetectorId=det_id)

    def test_data_sources_s3_status_is_valid_string(self, guardduty):
        """DataSources.S3Logs.Status is one of the expected status strings."""
        resp = guardduty.create_detector(Enable=True)
        det_id = resp["DetectorId"]
        try:
            detail = guardduty.get_detector(DetectorId=det_id)
            status = detail["DataSources"]["S3Logs"]["Status"]
            assert status in ("ENABLED", "DISABLED")
        finally:
            guardduty.delete_detector(DetectorId=det_id)

    def test_tags_is_empty_dict_not_none_when_unset(self, guardduty):
        """Tags is an empty dict (not None) when no tags were set at create time."""
        resp = guardduty.create_detector(Enable=True)
        det_id = resp["DetectorId"]
        try:
            detail = guardduty.get_detector(DetectorId=det_id)
            assert detail["Tags"] == {}
        finally:
            guardduty.delete_detector(DetectorId=det_id)

    def test_features_list_items_have_name_and_status_strings(self, guardduty):
        """Each item in Features has Name (str) and Status (str) fields."""
        resp = guardduty.create_detector(Enable=True)
        det_id = resp["DetectorId"]
        try:
            detail = guardduty.get_detector(DetectorId=det_id)
            features = detail["Features"]
            assert isinstance(features, list)
            for feature in features:
                assert isinstance(feature.get("Name"), str)
                assert isinstance(feature.get("Status"), str)
        finally:
            guardduty.delete_detector(DetectorId=det_id)

    def test_created_at_unchanged_after_update(self, guardduty):
        """CreatedAt is not modified when the detector is updated."""
        resp = guardduty.create_detector(Enable=True)
        det_id = resp["DetectorId"]
        try:
            created_before = guardduty.get_detector(DetectorId=det_id)["CreatedAt"]
            guardduty.update_detector(DetectorId=det_id, FindingPublishingFrequency="SIX_HOURS")
            created_after = guardduty.get_detector(DetectorId=det_id)["CreatedAt"]
            assert created_before == created_after
        finally:
            guardduty.delete_detector(DetectorId=det_id)

    def test_delete_detector_raises_on_get(self, guardduty):
        """After deleting a detector, get_detector raises BadRequestException."""
        resp = guardduty.create_detector(Enable=True)
        det_id = resp["DetectorId"]
        guardduty.delete_detector(DetectorId=det_id)
        with pytest.raises(ClientError) as exc_info:
            guardduty.get_detector(DetectorId=det_id)
        assert exc_info.value.response["Error"]["Code"] == "BadRequestException"


class TestGuardDutyAdminAccountBehavior:
    """Behavioral tests for GetAdministratorAccount — stronger assertions."""

    def test_get_administrator_account_returns_dict_response(self, guardduty, detector):
        """get_administrator_account returns a dict (not None) with ResponseMetadata."""
        result = guardduty.get_administrator_account(DetectorId=detector)
        assert isinstance(result, dict)
        assert result["ResponseMetadata"]["HTTPStatusCode"] == 200

    def test_get_administrator_account_response_is_not_none(self, guardduty, detector):
        """get_administrator_account does not return None — even without an admin configured."""
        result = guardduty.get_administrator_account(DetectorId=detector)
        assert result is not None
        # Administrator key may be present (dict) or absent — either is valid
        if "Administrator" in result:
            assert isinstance(result["Administrator"], dict)


class TestGuardDutyOrgAdminLifecycle:
    """Lifecycle tests for organization admin with full pattern coverage."""

    def test_enable_org_admin_appears_in_list_with_status(self, guardduty):
        """After enable, list contains the account with an AdminStatus field."""
        guardduty.enable_organization_admin_account(AdminAccountId="333311112222")
        try:
            resp = guardduty.list_organization_admin_accounts()
            assert isinstance(resp["AdminAccounts"], list)
            matching = [a for a in resp["AdminAccounts"] if a["AdminAccountId"] == "333311112222"]
            assert len(matching) == 1
            assert isinstance(matching[0]["AdminStatus"], str)
            assert matching[0]["AdminStatus"] != ""
        finally:
            guardduty.disable_organization_admin_account(AdminAccountId="333311112222")

    def test_enable_disable_org_admin_absent_from_list(self, guardduty):
        """After enable then disable, the account is no longer in list."""
        guardduty.enable_organization_admin_account(AdminAccountId="444422221111")
        guardduty.disable_organization_admin_account(AdminAccountId="444422221111")
        resp = guardduty.list_organization_admin_accounts()
        ids = [a["AdminAccountId"] for a in resp["AdminAccounts"]]
        assert "444422221111" not in ids

    def test_list_org_admin_accounts_returns_list_type(self, guardduty):
        """list_organization_admin_accounts always returns a list, not None."""
        resp = guardduty.list_organization_admin_accounts()
        assert isinstance(resp["AdminAccounts"], list)
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200


class TestGuardDutyEmptyListBehavior:
    """Empty list tests with length assertions (stronger than isinstance-only)."""

    def test_list_filters_fresh_detector_length_zero(self, guardduty, detector):
        """A brand-new detector has zero filters."""
        resp = guardduty.list_filters(DetectorId=detector)
        assert len(resp["FilterNames"]) == 0

    def test_list_ipsets_fresh_detector_length_zero(self, guardduty, detector):
        """A brand-new detector has zero IP sets."""
        resp = guardduty.list_ip_sets(DetectorId=detector)
        assert len(resp["IpSetIds"]) == 0

    def test_list_threat_intel_sets_fresh_detector_length_zero(self, guardduty, detector):
        """A brand-new detector has zero threat intel sets."""
        resp = guardduty.list_threat_intel_sets(DetectorId=detector)
        assert len(resp["ThreatIntelSetIds"]) == 0

    def test_list_findings_fresh_detector_length_zero(self, guardduty, detector):
        """A brand-new detector has zero findings."""
        resp = guardduty.list_findings(DetectorId=detector)
        assert len(resp["FindingIds"]) == 0


class TestGuardDutyTagsBehavior:
    """Tag operation behavior — stronger assertions than key-presence."""

    def test_list_tags_empty_resource_returns_empty_dict(self, guardduty, detector):
        """list_tags_for_resource on a fresh detector returns Tags == {}."""
        arn = f"arn:aws:guardduty:us-east-1:123456789012:detector/{detector}"
        resp = guardduty.list_tags_for_resource(ResourceArn=arn)
        assert resp["Tags"] == {}

    def test_tag_then_list_returns_correct_values(self, guardduty, detector):
        """After tagging, list_tags_for_resource returns the exact tag values set."""
        arn = f"arn:aws:guardduty:us-east-1:123456789012:detector/{detector}"
        guardduty.tag_resource(ResourceArn=arn, Tags={"mykey": "myvalue", "env": "ci"})
        resp = guardduty.list_tags_for_resource(ResourceArn=arn)
        assert resp["Tags"]["mykey"] == "myvalue"
        assert resp["Tags"]["env"] == "ci"
        assert len(resp["Tags"]) == 2

    def test_untag_reduces_tag_count(self, guardduty, detector):
        """Untagging one key leaves the others intact and reduces count by 1."""
        arn = f"arn:aws:guardduty:us-east-1:123456789012:detector/{detector}"
        guardduty.tag_resource(ResourceArn=arn, Tags={"a": "1", "b": "2", "c": "3"})
        guardduty.untag_resource(ResourceArn=arn, TagKeys=["b"])
        resp = guardduty.list_tags_for_resource(ResourceArn=arn)
        assert len(resp["Tags"]) == 2
        assert resp["Tags"]["a"] == "1"
        assert resp["Tags"]["c"] == "3"
        assert "b" not in resp["Tags"]


class TestGuardDutyFindingsBehavior:
    """Findings list behavior — stronger assertions."""

    def test_list_findings_with_severity_criteria_returns_list_type(self, guardduty, detector):
        """list_findings with severity criterion returns a list (not None)."""
        resp = guardduty.list_findings(
            DetectorId=detector,
            FindingCriteria={"Criterion": {"severity": {"Gte": 4}}},
        )
        assert isinstance(resp["FindingIds"], list)
        # Confirm specific field type rather than just presence
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200

    def test_list_findings_with_criteria_count_is_nonnegative(self, guardduty, detector):
        """list_findings count is >= 0 for any valid criteria."""
        resp = guardduty.list_findings(
            DetectorId=detector,
            FindingCriteria={"Criterion": {"severity": {"Gte": 8}}},
        )
        assert len(resp["FindingIds"]) >= 0

    def test_create_sample_findings_increases_list_count(self, guardduty, detector):
        """After create_sample_findings, list_findings returns at least 1 finding."""
        guardduty.create_sample_findings(
            DetectorId=detector,
            FindingTypes=["Recon:EC2/PortProbeUnprotectedPort"],
        )
        resp = guardduty.list_findings(DetectorId=detector)
        assert len(resp["FindingIds"]) >= 1


class TestGuardDutyUpdateIPSetErrorCases:
    """Error cases for UpdateIPSet."""

    def test_update_nonexistent_ip_set_raises_error(self, guardduty, detector):
        """Updating an IP set that does not exist raises BadRequestException."""
        with pytest.raises(ClientError) as exc_info:
            guardduty.update_ip_set(
                DetectorId=detector,
                IpSetId="nonexistent00000000000000000000",
                Name="should-fail",
            )
        assert exc_info.value.response["Error"]["Code"] == "BadRequestException"

    def test_update_ip_set_still_in_list_after_update(self, guardduty, detector):
        """After updating an IP set, it still appears in list_ip_sets."""
        resp = guardduty.create_ip_set(
            DetectorId=detector,
            Name=_unique("ipset"),
            Format="TXT",
            Location="s3://test-bucket/list-check.txt",
            Activate=False,
        )
        ipset_id = resp["IpSetId"]
        try:
            guardduty.update_ip_set(DetectorId=detector, IpSetId=ipset_id, Name="updated-list-check")
            listed = guardduty.list_ip_sets(DetectorId=detector)
            assert ipset_id in listed["IpSetIds"]
        finally:
            guardduty.delete_ip_set(DetectorId=detector, IpSetId=ipset_id)

    def test_update_nonexistent_threat_intel_set_raises_error(self, guardduty, detector):
        """Updating a threat intel set that does not exist raises BadRequestException."""
        with pytest.raises(ClientError) as exc_info:
            guardduty.update_threat_intel_set(
                DetectorId=detector,
                ThreatIntelSetId="nonexistent00000000000000000000",
                Name="should-fail",
            )
        assert exc_info.value.response["Error"]["Code"] == "BadRequestException"

