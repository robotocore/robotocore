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


class TestSupportAutoCoverage:
    """Auto-generated coverage tests for support."""

    @pytest.fixture
    def client(self):
        return make_client("support")

    def test_resolve_case(self, client):
        """ResolveCase returns a response."""
        resp = client.resolve_case()
        assert "initialCaseStatus" in resp

    def test_add_communication_to_case(self, client):
        """AddCommunicationToCase adds a message to an existing case."""
        create_resp = client.create_case(
            subject="Communication test",
            communicationBody="Initial message",
            serviceCode="general-info",
            categoryCode="using-aws",
            severityCode="low",
        )
        case_id = create_resp["caseId"]
        resp = client.add_communication_to_case(
            caseId=case_id,
            communicationBody="Follow-up message",
        )
        assert resp["result"] is True

    def test_describe_communications(self, client):
        """DescribeCommunications returns communications for a case."""
        create_resp = client.create_case(
            subject="Describe comms test",
            communicationBody="Hello from compat test",
            serviceCode="general-info",
            categoryCode="using-aws",
            severityCode="low",
        )
        case_id = create_resp["caseId"]
        resp = client.describe_communications(caseId=case_id)
        assert "communications" in resp
        assert len(resp["communications"]) > 0

    def test_refresh_trusted_advisor_check(self, client):
        """RefreshTrustedAdvisorCheck refreshes a check."""
        checks_resp = client.describe_trusted_advisor_checks(language="en")
        check_id = checks_resp["checks"][0]["id"]
        resp = client.refresh_trusted_advisor_check(checkId=check_id)
        assert "status" in resp

    def test_describe_trusted_advisor_check_result(self, client):
        """DescribeTrustedAdvisorCheckResult returns check result."""
        checks_resp = client.describe_trusted_advisor_checks(language="en")
        check_id = checks_resp["checks"][0]["id"]
        resp = client.describe_trusted_advisor_check_result(checkId=check_id)
        assert "result" in resp

    def test_describe_trusted_advisor_check_summaries(self, client):
        """DescribeTrustedAdvisorCheckSummaries returns summaries."""
        checks_resp = client.describe_trusted_advisor_checks(language="en")
        check_id = checks_resp["checks"][0]["id"]
        resp = client.describe_trusted_advisor_check_summaries(checkIds=[check_id])
        assert "summaries" in resp
