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


class TestSupportNewOperations:
    """Tests for newly implemented support operations."""

    @pytest.fixture
    def client(self):
        return make_client("support")

    def test_describe_supported_languages(self, client):
        """DescribeSupportedLanguages returns a list of supported languages."""
        resp = client.describe_supported_languages(
            issueType="customer-service",
            serviceCode="general-info",
            categoryCode="using-aws",
        )
        assert "supportedLanguages" in resp
        langs = resp["supportedLanguages"]
        assert len(langs) > 0
        codes = [lang["code"] for lang in langs]
        assert "en" in codes

    def test_describe_create_case_options(self, client):
        """DescribeCreateCaseOptions returns language availability."""
        resp = client.describe_create_case_options(
            issueType="customer-service",
            serviceCode="general-info",
            categoryCode="using-aws",
            language="en",
        )
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200
        assert "languageAvailability" in resp

    def test_add_attachments_to_set(self, client):
        """AddAttachmentsToSet creates an attachment set and returns set ID."""
        resp = client.add_attachments_to_set(
            attachments=[{"fileName": "test.txt", "data": b"hello world"}]
        )
        assert "attachmentSetId" in resp
        assert len(resp["attachmentSetId"]) > 0
        assert "expiryTime" in resp

    def test_add_attachments_to_existing_set(self, client):
        """AddAttachmentsToSet can add to an existing set."""
        resp1 = client.add_attachments_to_set(
            attachments=[{"fileName": "file1.txt", "data": b"first file"}]
        )
        set_id = resp1["attachmentSetId"]
        resp2 = client.add_attachments_to_set(
            attachmentSetId=set_id,
            attachments=[{"fileName": "file2.txt", "data": b"second file"}],
        )
        assert resp2["attachmentSetId"] == set_id

    def test_describe_trusted_advisor_check_refresh_statuses(self, client):
        """DescribeTrustedAdvisorCheckRefreshStatuses returns statuses for check IDs."""
        checks_resp = client.describe_trusted_advisor_checks(language="en")
        check_ids = [c["id"] for c in checks_resp["checks"][:2]]
        resp = client.describe_trusted_advisor_check_refresh_statuses(checkIds=check_ids)
        assert "statuses" in resp
        assert len(resp["statuses"]) == len(check_ids)
        for status in resp["statuses"]:
            assert "checkId" in status
            assert "status" in status
            assert status["checkId"] in check_ids

    def test_describe_attachment_not_found(self, client):
        """DescribeAttachment with a nonexistent attachment ID raises AttachmentIdNotFound."""
        with pytest.raises(client.exceptions.AttachmentIdNotFound):
            client.describe_attachment(attachmentId="nonexistent-attachment-id-12345")

    def test_describe_attachment_for_created_set(self, client):
        """DescribeAttachment on an attachment set ID returns error (set ID != attachment ID)."""
        resp = client.add_attachments_to_set(
            attachments=[{"fileName": "test.txt", "data": b"hello world"}]
        )
        set_id = resp["attachmentSetId"]
        # Attachment set ID is not the same as individual attachment ID
        with pytest.raises(client.exceptions.AttachmentIdNotFound):
            client.describe_attachment(attachmentId=set_id)
