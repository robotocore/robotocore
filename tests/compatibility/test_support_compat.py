"""Support API compatibility tests."""

import pytest

from tests.compatibility.conftest import make_client


@pytest.fixture
def support():
    return make_client("support")


class TestSupportOperations:
    def test_describe_trusted_advisor_checks(self, support):
        response = support.describe_trusted_advisor_checks(language="en")
        assert "checks" in response
        assert len(response["checks"]) > 0
        check = response["checks"][0]
        assert "id" in check
        assert "name" in check

    def test_refresh_trusted_advisor_check(self, support):
        checks = support.describe_trusted_advisor_checks(language="en")
        check_id = checks["checks"][0]["id"]
        response = support.refresh_trusted_advisor_check(checkId=check_id)
        assert "status" in response

    def test_create_case(self, support):
        response = support.create_case(
            subject="Test Case",
            communicationBody="This is a test case for compatibility testing.",
            serviceCode="amazon-dynamodb",
            categoryCode="other",
            severityCode="low",
            language="en",
        )
        assert "caseId" in response

    def test_describe_cases(self, support):
        create_response = support.create_case(
            subject="Describe Test Case",
            communicationBody="Testing describe cases.",
            serviceCode="amazon-dynamodb",
            categoryCode="other",
            severityCode="low",
            language="en",
        )
        case_id = create_response["caseId"]
        response = support.describe_cases(caseIdList=[case_id])
        assert len(response["cases"]) >= 1
        assert response["cases"][0]["caseId"] == case_id

    def test_resolve_case(self, support):
        create_response = support.create_case(
            subject="Resolve Test Case",
            communicationBody="Testing resolve case.",
            serviceCode="amazon-dynamodb",
            categoryCode="other",
            severityCode="low",
            language="en",
        )
        case_id = create_response["caseId"]
        response = support.resolve_case(caseId=case_id)
        assert "initialCaseStatus" in response
        assert "finalCaseStatus" in response
