"""Service Quotas compatibility tests."""

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

    def test_request_service_quota_increase(self, quotas):
        resp = quotas.request_service_quota_increase(
            ServiceCode="vpc", QuotaCode="L-7E9ECCDB", DesiredValue=100.0
        )
        assert "RequestedQuota" in resp
        req = resp["RequestedQuota"]
        assert "Id" in req
        assert req["ServiceCode"] == "vpc"
        assert req["Status"] == "PENDING"

    def test_get_requested_service_quota_change(self, quotas):
        create_resp = quotas.request_service_quota_increase(
            ServiceCode="vpc", QuotaCode="L-7E9ECCDB", DesiredValue=200.0
        )
        request_id = create_resp["RequestedQuota"]["Id"]
        resp = quotas.get_requested_service_quota_change(RequestId=request_id)
        assert "RequestedQuota" in resp
        assert resp["RequestedQuota"]["Id"] == request_id

    def test_get_requested_service_quota_change_not_found(self, quotas):
        with pytest.raises(quotas.exceptions.NoSuchResourceException):
            quotas.get_requested_service_quota_change(RequestId="nonexistent-id")

    def test_list_requested_service_quota_change_history_with_filter(self, quotas):
        quotas.request_service_quota_increase(
            ServiceCode="vpc", QuotaCode="L-7E9ECCDB", DesiredValue=150.0
        )
        resp = quotas.list_requested_service_quota_change_history(ServiceCode="vpc")
        assert "RequestedQuotas" in resp
        assert any(r["ServiceCode"] == "vpc" for r in resp["RequestedQuotas"])

    def test_list_requested_quota_change_history_by_quota(self, quotas):
        quotas.request_service_quota_increase(
            ServiceCode="vpc", QuotaCode="L-7E9ECCDB", DesiredValue=150.0
        )
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
