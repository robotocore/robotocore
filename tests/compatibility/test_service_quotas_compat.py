"""Service Quotas compatibility tests.

The wave-12 servicequotas port serves the whole 26-op botocore surface
(the 2026-10-25 fleet probe records working for every operation), so the
tests trimmed with the moto-parity ledger — change requests, templates,
auto-management, tagging, ``ListServices``/``ListServiceQuotas``,
``GetAWSDefaultServiceQuota`` — are restored. moto still serves only
``ListAWSDefaultServiceQuotas`` / ``GetServiceQuota``, so these restored
tests pin the twin's modeled surface beyond moto's; every body below is
live-verified against the twin.
"""

import uuid

import pytest

from tests.compatibility.conftest import make_client


@pytest.fixture
def quotas():
    return make_client("service-quotas")


class TestListOperations:
    """Tests for list operations that need no setup."""

    def test_list_services(self, quotas):
        resp = quotas.list_services()
        assert "Services" in resp
        services = resp["Services"]
        assert len(services) > 0
        service_codes = [s["ServiceCode"] for s in services]
        assert "vpc" in service_codes

    def test_list_aws_default_service_quotas(self, quotas):
        resp = quotas.list_aws_default_service_quotas(ServiceCode="vpc")
        assert "Quotas" in resp
        assert len(resp["Quotas"]) > 0
        quota = resp["Quotas"][0]
        assert "QuotaCode" in quota
        assert "QuotaName" in quota

    def test_list_service_quotas(self, quotas):
        resp = quotas.list_service_quotas(ServiceCode="vpc")
        assert "Quotas" in resp
        assert len(resp["Quotas"]) > 0

    def test_list_requested_service_quota_change_history_empty(self, quotas):
        resp = quotas.list_requested_service_quota_change_history()
        assert "RequestedQuotas" in resp
        assert isinstance(resp["RequestedQuotas"], list)

    def test_list_service_quota_increase_requests_in_template_empty(self, quotas):
        resp = quotas.list_service_quota_increase_requests_in_template()
        assert "ServiceQuotaIncreaseRequestInTemplateList" in resp
        assert isinstance(resp["ServiceQuotaIncreaseRequestInTemplateList"], list)


class TestGetDefaultQuotas:
    """Tests for getting default quota values."""

    def test_get_aws_default_service_quota(self, quotas):
        # First get a quota code from the list
        list_resp = quotas.list_aws_default_service_quotas(ServiceCode="vpc")
        quota_code = list_resp["Quotas"][0]["QuotaCode"]
        resp = quotas.get_aws_default_service_quota(ServiceCode="vpc", QuotaCode=quota_code)
        assert "Quota" in resp
        assert resp["Quota"]["QuotaCode"] == quota_code

    def test_get_service_quota(self, quotas):
        list_resp = quotas.list_aws_default_service_quotas(ServiceCode="vpc")
        quota_code = list_resp["Quotas"][0]["QuotaCode"]
        resp = quotas.get_service_quota(ServiceCode="vpc", QuotaCode=quota_code)
        assert "Quota" in resp
        assert resp["Quota"]["QuotaCode"] == quota_code

    def test_get_aws_default_service_quota_not_found(self, quotas):
        with pytest.raises(quotas.exceptions.NoSuchResourceException):
            quotas.get_aws_default_service_quota(ServiceCode="unknown-svc", QuotaCode="L-UNKNOWN")


class TestQuotaChangeRequests:
    """Tests for quota increase request lifecycle."""

    @staticmethod
    def _open_change_request(quotas, value):
        """Id of the open increase request for vpc/L-7E9ECCDB.

        The twin models one open request per (service, quota); a second
        create answers the modeled ResourceAlreadyExistsException, so the
        helper reuses the persisted request instead (works on a fresh or
        shared twin alike).
        """
        try:
            resp = quotas.request_service_quota_increase(
                ServiceCode="vpc", QuotaCode="L-7E9ECCDB", DesiredValue=value
            )
            return resp["RequestedQuota"]["Id"]
        except quotas.exceptions.ResourceAlreadyExistsException:
            hist = quotas.list_requested_service_quota_change_history_by_quota(
                ServiceCode="vpc", QuotaCode="L-7E9ECCDB"
            )
            return hist["RequestedQuotas"][0]["Id"]

    def test_request_service_quota_increase(self, quotas):
        """Create or reuse the open increase request, then pin its shape."""
        try:
            req = quotas.request_service_quota_increase(
                ServiceCode="vpc", QuotaCode="L-7E9ECCDB", DesiredValue=100.0
            )["RequestedQuota"]
        except quotas.exceptions.ResourceAlreadyExistsException:
            # one open request per (service, quota) persists on this twin
            req = quotas.list_requested_service_quota_change_history_by_quota(
                ServiceCode="vpc", QuotaCode="L-7E9ECCDB"
            )["RequestedQuotas"][0]
        assert req["ServiceCode"] == "vpc"
        assert req["Status"] == "PENDING"

    def test_request_service_quota_increase_second_open_is_modeled(self, quotas):
        """A second open request for the same quota raises the modeled guard."""
        self._open_change_request(quotas, 100.0)
        with pytest.raises(quotas.exceptions.ResourceAlreadyExistsException):
            quotas.request_service_quota_increase(
                ServiceCode="vpc", QuotaCode="L-7E9ECCDB", DesiredValue=150.0
            )

    def test_get_requested_service_quota_change(self, quotas):
        request_id = self._open_change_request(quotas, 200.0)
        resp = quotas.get_requested_service_quota_change(RequestId=request_id)
        assert "RequestedQuota" in resp
        assert resp["RequestedQuota"]["Id"] == request_id

    def test_get_requested_service_quota_change_not_found(self, quotas):
        with pytest.raises(quotas.exceptions.NoSuchResourceException):
            quotas.get_requested_service_quota_change(RequestId="nonexistent-id")

    def test_list_requested_service_quota_change_history_with_filter(self, quotas):
        self._open_change_request(quotas, 150.0)
        resp = quotas.list_requested_service_quota_change_history(ServiceCode="vpc")
        assert "RequestedQuotas" in resp
        assert any(r["ServiceCode"] == "vpc" for r in resp["RequestedQuotas"])

    def test_list_requested_quota_change_history_by_quota(self, quotas):
        self._open_change_request(quotas, 150.0)
        resp = quotas.list_requested_service_quota_change_history_by_quota(
            ServiceCode="vpc", QuotaCode="L-7E9ECCDB"
        )
        assert "RequestedQuotas" in resp
        for req in resp["RequestedQuotas"]:
            assert req["ServiceCode"] == "vpc"
            assert req["QuotaCode"] == "L-7E9ECCDB"


class TestTemplateOperations:
    """Tests for service quota template operations."""

    def test_associate_and_get_template_status(self, quotas):
        quotas.associate_service_quota_template()
        resp = quotas.get_association_for_service_quota_template()
        assert resp["ServiceQuotaTemplateAssociationStatus"] == "ASSOCIATED"

    def test_disassociate_template(self, quotas):
        quotas.associate_service_quota_template()
        quotas.disassociate_service_quota_template()
        resp = quotas.get_association_for_service_quota_template()
        assert resp["ServiceQuotaTemplateAssociationStatus"] == "DISASSOCIATED"

    def test_put_and_list_and_delete_template_request(self, quotas):
        quotas.put_service_quota_increase_request_into_template(
            QuotaCode="L-7E9ECCDB",
            ServiceCode="vpc",
            AwsRegion="us-east-1",
            DesiredValue=100.0,
        )
        list_resp = quotas.list_service_quota_increase_requests_in_template()
        items = list_resp["ServiceQuotaIncreaseRequestInTemplateList"]
        assert any(i["ServiceCode"] == "vpc" and i["QuotaCode"] == "L-7E9ECCDB" for i in items)
        # Get the template request
        get_resp = quotas.get_service_quota_increase_request_from_template(
            ServiceCode="vpc", QuotaCode="L-7E9ECCDB", AwsRegion="us-east-1"
        )
        assert get_resp["ServiceQuotaIncreaseRequestInTemplate"]["ServiceCode"] == "vpc"
        # Delete it
        quotas.delete_service_quota_increase_request_from_template(
            ServiceCode="vpc", QuotaCode="L-7E9ECCDB", AwsRegion="us-east-1"
        )
        list_resp2 = quotas.list_service_quota_increase_requests_in_template()
        items2 = list_resp2["ServiceQuotaIncreaseRequestInTemplateList"]
        assert not any(
            i["ServiceCode"] == "vpc"
            and i["QuotaCode"] == "L-7E9ECCDB"
            and i["AwsRegion"] == "us-east-1"
            for i in items2
        )


class TestAutoManagement:
    """Tests for auto management operations."""

    def test_stop_auto_management(self, quotas):
        """StopAutoManagement should succeed without error."""
        resp = quotas.stop_auto_management()
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200

    def test_start_auto_management(self, quotas):
        """StartAutoManagement should succeed."""
        resp = quotas.start_auto_management(OptInLevel="ACCOUNT", OptInType="REQUEST_INCREASE")
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200

    def test_get_auto_management_configuration(self, quotas):
        """GetAutoManagementConfiguration should return 200."""
        resp = quotas.get_auto_management_configuration()
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200

    def test_update_auto_management(self, quotas):
        """UpdateAutoManagement should succeed."""
        resp = quotas.update_auto_management(OptInType="REQUEST_INCREASE")
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200

    def test_start_quota_utilization_report(self, quotas):
        """StartQuotaUtilizationReport should succeed."""
        resp = quotas.start_quota_utilization_report()
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200

    def test_get_quota_utilization_report(self, quotas):
        """GetQuotaUtilizationReport resolves the report start creates."""
        started = quotas.start_quota_utilization_report()
        report_id = started["ReportId"]
        resp = quotas.get_quota_utilization_report(ReportId=report_id)
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200
        assert resp["ReportId"] == report_id
        assert "Status" in resp

    def test_get_quota_utilization_report_not_found(self, quotas):
        """GetQuotaUtilizationReport raises the modeled NoSuchResource for a
        nonexistent report id."""
        with pytest.raises(quotas.exceptions.NoSuchResourceException):
            quotas.get_quota_utilization_report(ReportId="nonexistent-report-id")

    def test_create_support_case(self, quotas):
        """CreateSupportCase succeeds with a fresh request id."""
        resp = quotas.create_support_case(
            RequestId=f"test-request-{uuid.uuid4().hex[:8]}"
        )
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200


class TestTaggingOperations:
    """Tests for TagResource, UntagResource, ListTagsForResource."""

    def test_tag_untag_list_tags(self, quotas):
        arn = "arn:aws:servicequotas:us-east-1:123456789012:vpc/L-7E9ECCDB"
        quotas.tag_resource(
            ResourceARN=arn,
            Tags=[{"Key": "env", "Value": "test"}, {"Key": "team", "Value": "platform"}],
        )
        resp = quotas.list_tags_for_resource(ResourceARN=arn)
        assert "Tags" in resp
        tag_dict = {t["Key"]: t["Value"] for t in resp["Tags"]}
        assert tag_dict["env"] == "test"
        assert tag_dict["team"] == "platform"
        quotas.untag_resource(ResourceARN=arn, TagKeys=["team"])
        resp2 = quotas.list_tags_for_resource(ResourceARN=arn)
        keys2 = [t["Key"] for t in resp2["Tags"]]
        assert "team" not in keys2
        assert "env" in keys2
