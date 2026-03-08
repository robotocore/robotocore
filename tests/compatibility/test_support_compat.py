"""AWS Support compatibility tests."""

import pytest

from tests.compatibility.conftest import make_client


@pytest.fixture
def support():
    return make_client("support")


class TestSupportOperations:
    def test_describe_severity_levels(self, support):
        resp = support.describe_severity_levels()
        assert "severityLevels" in resp
        assert len(resp["severityLevels"]) > 0
        codes = [s["code"] for s in resp["severityLevels"]]
        assert "low" in codes

    def test_describe_trusted_advisor_checks(self, support):
        resp = support.describe_trusted_advisor_checks(language="en")
        assert "checks" in resp
        assert len(resp["checks"]) > 0
        # Each check has required fields
        check = resp["checks"][0]
        assert "id" in check
        assert "name" in check

    def test_describe_cases_empty(self, support):
        resp = support.describe_cases()
        assert "cases" in resp

    def test_create_case(self, support):
        resp = support.create_case(
            subject="Test case from compat tests",
            communicationBody="Automated compatibility test",
            serviceCode="general-info",
            categoryCode="using-aws",
            severityCode="low",
        )
        assert "caseId" in resp
        assert resp["caseId"]

    def test_create_and_describe_case(self, support):
        create_resp = support.create_case(
            subject="Describe case test",
            communicationBody="Testing describe",
            serviceCode="general-info",
            categoryCode="using-aws",
            severityCode="low",
        )
        case_id = create_resp["caseId"]
        desc_resp = support.describe_cases(caseIdList=[case_id])
        assert "cases" in desc_resp
        if desc_resp["cases"]:
            assert desc_resp["cases"][0]["caseId"] == case_id

    def test_describe_services(self, support):
        resp = support.describe_services()
        assert "services" in resp
        assert len(resp["services"]) > 0
        svc = resp["services"][0]
        assert "code" in svc
        assert "name" in svc


class TestSupportGapStubs:
    """Tests for gap operations: describe_trusted_advisor_checks."""

    @pytest.fixture
    def support(self):
        return make_client("support")

    def test_describe_trusted_advisor_checks(self, support):
        resp = support.describe_trusted_advisor_checks(language="en")
        assert "checks" in resp
        assert len(resp["checks"]) > 0
        check = resp["checks"][0]
        assert "id" in check
        assert "name" in check
