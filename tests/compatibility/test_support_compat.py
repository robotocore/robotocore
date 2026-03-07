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


class TestSupportExtended:
    @pytest.fixture
    def support(self):
        return make_client("support")

    @pytest.mark.xfail(reason="describe_services not implemented")
    def test_describe_services(self, support):
        resp = support.describe_services(language="en")
        assert "services" in resp
        assert len(resp["services"]) > 0
        svc = resp["services"][0]
        assert "code" in svc
        assert "name" in svc

    @pytest.mark.xfail(reason="describe_severity_levels not implemented")
    def test_describe_severity_levels(self, support):
        resp = support.describe_severity_levels(language="en")
        assert "severityLevels" in resp
        assert len(resp["severityLevels"]) > 0
        level = resp["severityLevels"][0]
        assert "code" in level
        assert "name" in level

    def test_trusted_advisor_check_has_category(self, support):
        resp = support.describe_trusted_advisor_checks(language="en")
        check = resp["checks"][0]
        assert "category" in check

    def test_trusted_advisor_check_has_description(self, support):
        resp = support.describe_trusted_advisor_checks(language="en")
        check = resp["checks"][0]
        assert "description" in check

    @pytest.mark.xfail(reason="describe_trusted_advisor_check_result not implemented")
    def test_describe_trusted_advisor_check_result(self, support):
        checks = support.describe_trusted_advisor_checks(language="en")
        check_id = checks["checks"][0]["id"]
        resp = support.describe_trusted_advisor_check_result(checkId=check_id, language="en")
        assert "result" in resp

    @pytest.mark.xfail(reason="describe_trusted_advisor_check_summaries not implemented")
    def test_describe_trusted_advisor_check_summaries(self, support):
        checks = support.describe_trusted_advisor_checks(language="en")
        check_id = checks["checks"][0]["id"]
        resp = support.describe_trusted_advisor_check_summaries(checkIds=[check_id])
        assert "summaries" in resp

    def test_create_case_with_attachment_set(self, support):
        resp = support.create_case(
            subject="Attachment Test",
            communicationBody="Testing case with more details.",
            serviceCode="amazon-dynamodb",
            categoryCode="other",
            severityCode="low",
            language="en",
            issueType="technical",
        )
        assert "caseId" in resp

    def test_describe_cases_include_resolved(self, support):
        create_resp = support.create_case(
            subject="Include Resolved Test",
            communicationBody="Testing include resolved.",
            serviceCode="amazon-dynamodb",
            categoryCode="other",
            severityCode="low",
            language="en",
        )
        case_id = create_resp["caseId"]
        support.resolve_case(caseId=case_id)
        resp = support.describe_cases(caseIdList=[case_id], includeResolvedCases=True)
        assert len(resp["cases"]) >= 1

    @pytest.mark.xfail(reason="add_communication_to_case not implemented")
    def test_add_communication_to_case(self, support):
        create_resp = support.create_case(
            subject="Comm Test",
            communicationBody="Initial message.",
            serviceCode="amazon-dynamodb",
            categoryCode="other",
            severityCode="low",
            language="en",
        )
        case_id = create_resp["caseId"]
        resp = support.add_communication_to_case(
            caseId=case_id,
            communicationBody="Follow-up message.",
        )
        assert resp["result"] is True

    @pytest.mark.xfail(reason="describe_communications not implemented")
    def test_describe_communications(self, support):
        create_resp = support.create_case(
            subject="Desc Comm Test",
            communicationBody="Initial message.",
            serviceCode="amazon-dynamodb",
            categoryCode="other",
            severityCode="low",
            language="en",
        )
        case_id = create_resp["caseId"]
        resp = support.describe_communications(caseId=case_id)
        assert "communications" in resp
        assert len(resp["communications"]) >= 1

