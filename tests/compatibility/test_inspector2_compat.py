"""Inspector2 compatibility tests."""

import uuid

import pytest

from tests.compatibility.conftest import make_client


@pytest.fixture
def inspector2():
    return make_client("inspector2")


def _unique(prefix):
    return f"{prefix}-{uuid.uuid4().hex[:8]}"


class TestInspector2FindingsOperations:
    def test_list_findings(self, inspector2):
        resp = inspector2.list_findings()
        assert "findings" in resp
        assert isinstance(resp["findings"], list)

    def test_batch_get_account_status(self, inspector2):
        resp = inspector2.batch_get_account_status(accountIds=["123456789012"])
        assert "accounts" in resp
        assert "failedAccounts" in resp
        assert isinstance(resp["accounts"], list)


class TestInspector2FilterOperations:
    def test_list_filters(self, inspector2):
        resp = inspector2.list_filters()
        assert "filters" in resp
        assert isinstance(resp["filters"], list)

    def test_create_filter(self, inspector2):
        name = _unique("filter")
        resp = inspector2.create_filter(
            action="NONE",
            filterCriteria={},
            name=name,
        )
        assert "arn" in resp
        assert resp["arn"]


class TestInspector2OrganizationOperations:
    def test_list_members(self, inspector2):
        resp = inspector2.list_members()
        assert "members" in resp
        assert isinstance(resp["members"], list)

    def test_describe_organization_configuration(self, inspector2):
        resp = inspector2.describe_organization_configuration()
        assert "autoEnable" in resp
        auto_enable = resp["autoEnable"]
        assert "ec2" in auto_enable
        assert "ecr" in auto_enable
        assert "lambda" in auto_enable
