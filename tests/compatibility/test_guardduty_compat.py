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

    def test_get_findings_statistics_has_count_by_severity(self, guardduty, detector):
        resp = guardduty.get_findings_statistics(
            DetectorId=detector, FindingStatisticTypes=["COUNT_BY_SEVERITY"]
        )
        assert "CountBySeverity" in resp["FindingStatistics"]


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

    def test_get_malware_scan_settings_has_resource_criteria(self, guardduty, detector):
        resp = guardduty.get_malware_scan_settings(DetectorId=detector)
        assert "ScanResourceCriteria" in resp


class TestGuardDutyCoverageOperations:
    """Tests for GetCoverageStatistics."""

    def test_get_coverage_statistics(self, guardduty, detector):
        resp = guardduty.get_coverage_statistics(
            DetectorId=detector, StatisticsType=["COUNT_BY_RESOURCE_TYPE"]
        )
        assert "CoverageStatistics" in resp

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

    def test_get_member_detectors_unprocessed(self, guardduty, detector):
        """GetMemberDetectors returns UnprocessedAccounts for unknown members."""
        resp = guardduty.get_member_detectors(
            DetectorId=detector,
            AccountIds=["999988887777"],
        )
        assert "UnprocessedAccounts" in resp


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
