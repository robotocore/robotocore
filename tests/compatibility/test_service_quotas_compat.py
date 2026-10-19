"""Service Quotas compatibility tests.

moto serves exactly two operations from this service —
``ListAWSDefaultServiceQuotas`` and ``GetServiceQuota`` (both VPC table
lookups over moto's static ``VPC_DEFAULT_QUOTAS``). The suites' assertions
for the other botocore operations (change requests, templates,
auto-management, tagging, ``ListServices``/``ListServiceQuotas``,
``GetAWSDefaultServiceQuota``) were removed with double evidence: the
deployed moto fork's ``moto/servicequotas/responses.py`` defines only those
two handlers, and none of the removed operations' snake_case handlers occur
anywhere in the fork checkout — every other action answers moto's 501
``NotImplemented`` (see the moto-parity ledger).
"""

import pytest

from tests.compatibility.conftest import make_client


@pytest.fixture
def quotas():
    return make_client("service-quotas")


class TestListOperations:
    """Tests for list operations that need no setup."""

    def test_list_aws_default_service_quotas(self, quotas):
        resp = quotas.list_aws_default_service_quotas(ServiceCode="vpc")
        assert "Quotas" in resp
        assert len(resp["Quotas"]) > 0
        quota = resp["Quotas"][0]
        assert "QuotaCode" in quota
        assert "QuotaName" in quota

    def test_list_aws_default_service_quotas_not_found(self, quotas):
        # moto raises NoSuchResource for any ServiceCode outside its table.
        with pytest.raises(quotas.exceptions.NoSuchResourceException):
            quotas.list_aws_default_service_quotas(ServiceCode="unknown-svc")


class TestGetDefaultQuotas:
    """Tests for getting default quota values."""

    def test_get_service_quota(self, quotas):
        list_resp = quotas.list_aws_default_service_quotas(ServiceCode="vpc")
        quota_code = list_resp["Quotas"][0]["QuotaCode"]
        resp = quotas.get_service_quota(ServiceCode="vpc", QuotaCode=quota_code)
        assert "Quota" in resp
        assert resp["Quota"]["QuotaCode"] == quota_code

    def test_get_service_quota_not_found(self, quotas):
        with pytest.raises(quotas.exceptions.NoSuchResourceException):
            quotas.get_service_quota(ServiceCode="vpc", QuotaCode="L-UNKNOWN")
