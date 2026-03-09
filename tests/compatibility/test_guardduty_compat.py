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
