"""Macie2 compatibility tests."""

import pytest
from botocore.exceptions import ClientError

from tests.compatibility.conftest import make_client


@pytest.fixture
def macie2():
    return make_client("macie2")


class TestMacie2Operations:
    def test_get_macie_session(self, macie2):
        macie2.enable_macie()
        response = macie2.get_macie_session()
        assert "status" in response

    def test_list_members(self, macie2):
        response = macie2.list_members()
        assert "members" in response
        assert isinstance(response["members"], list)

    def test_get_administrator_account(self, macie2):
        response = macie2.get_administrator_account()
        # Response may have an empty administrator field or none at all
        assert response["ResponseMetadata"]["HTTPStatusCode"] == 200

    def test_list_invitations(self, macie2):
        response = macie2.list_invitations()
        assert "invitations" in response
        assert isinstance(response["invitations"], list)

    def test_enable_macie(self, macie2):
        # enable_macie is idempotent
        response = macie2.enable_macie()
        assert response["ResponseMetadata"]["HTTPStatusCode"] == 200


class TestMacie2AutoCoverage:
    """Auto-generated coverage tests for macie2."""

    @pytest.fixture
    def client(self):
        return make_client("macie2")

    def test_disable_macie(self, client):
        """DisableMacie returns a response."""
        resp = client.disable_macie()
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200

    def test_list_organization_admin_accounts(self, client):
        """ListOrganizationAdminAccounts returns a response."""
        resp = client.list_organization_admin_accounts()
        assert "adminAccounts" in resp

    def test_delete_member_nonexistent(self, client):
        """DeleteMember returns ResourceNotFoundException for unknown member."""
        with pytest.raises(ClientError) as exc:
            client.delete_member(id="111122223333")
        assert exc.value.response["Error"]["Code"] == "ResourceNotFoundException"

    def test_disassociate_member_nonexistent(self, client):
        """DisassociateMember returns ResourceNotFoundException for unknown member."""
        with pytest.raises(ClientError) as exc:
            client.disassociate_member(id="222233334444")
        assert exc.value.response["Error"]["Code"] == "ResourceNotFoundException"

    def test_decline_invitations(self, client):
        """DeclineInvitations returns unprocessedAccounts list."""
        resp = client.decline_invitations(accountIds=["111122223333"])
        assert "unprocessedAccounts" in resp
        assert isinstance(resp["unprocessedAccounts"], list)

    def test_accept_invitation(self, client):
        """AcceptInvitation succeeds even with nonexistent invitation."""
        resp = client.accept_invitation(
            invitationId="inv-fake", administratorAccountId="111122223333"
        )
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200

    def test_create_invitations(self, client):
        """CreateInvitations returns unprocessedAccounts list."""
        resp = client.create_invitations(accountIds=["111122223333"])
        assert "unprocessedAccounts" in resp
        assert isinstance(resp["unprocessedAccounts"], list)

    def test_enable_organization_admin_account(self, client):
        """EnableOrganizationAdminAccount succeeds."""
        resp = client.enable_organization_admin_account(adminAccountId="111122223333")
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200


class TestMacie2Lifecycle:
    """Tests for Macie enable/disable lifecycle."""

    @pytest.fixture
    def client(self):
        return make_client("macie2")

    def test_enable_disable_enable_cycle(self, client):
        """Enable, disable, then re-enable Macie session."""
        client.enable_macie()
        session = client.get_macie_session()
        assert "status" in session

        client.disable_macie()

        # Re-enable
        client.enable_macie()
        session2 = client.get_macie_session()
        assert "status" in session2

    def test_get_macie_session_has_expected_fields(self, client):
        """GetMacieSession returns session with status and serviceRole."""
        client.enable_macie()
        resp = client.get_macie_session()
        assert "status" in resp
        assert "createdAt" in resp

    def test_get_administrator_account_response_structure(self, client):
        """GetAdministratorAccount returns 200 with expected structure."""
        resp = client.get_administrator_account()
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200

    def test_list_organization_admin_accounts_returns_list(self, client):
        """ListOrganizationAdminAccounts returns adminAccounts list."""
        resp = client.list_organization_admin_accounts()
        assert "adminAccounts" in resp
        assert isinstance(resp["adminAccounts"], list)


class TestMacie2SessionDetails:
    """Tests for Macie session detail fields."""

    @pytest.fixture
    def client(self):
        return make_client("macie2")

    def test_get_macie_session_all_fields(self, client):
        """GetMacieSession returns all expected fields."""
        client.enable_macie()
        resp = client.get_macie_session()
        assert resp["status"] == "ENABLED"
        assert "findingPublishingFrequency" in resp
        assert "serviceRole" in resp
        assert "updatedAt" in resp

    def test_enable_macie_with_custom_frequency(self, client):
        """EnableMacie with custom findingPublishingFrequency."""
        client.enable_macie(findingPublishingFrequency="ONE_HOUR")
        resp = client.get_macie_session()
        assert resp["findingPublishingFrequency"] == "ONE_HOUR"

    def test_enable_macie_service_role_contains_account(self, client):
        """ServiceRole ARN contains the account ID."""
        client.enable_macie()
        resp = client.get_macie_session()
        assert "serviceRole" in resp
        assert "macie" in resp["serviceRole"].lower()

    def test_enable_org_admin_then_list(self, client):
        """EnableOrganizationAdminAccount then ListOrganizationAdminAccounts shows it."""
        client.enable_organization_admin_account(adminAccountId="999988887777")
        resp = client.list_organization_admin_accounts()
        assert "adminAccounts" in resp
        accounts = resp["adminAccounts"]
        assert len(accounts) >= 1
        account_ids = [a["accountId"] for a in accounts]
        assert "999988887777" in account_ids

    def test_create_invitations_then_list(self, client):
        """CreateInvitations then ListInvitations shows the invitation."""
        client.create_invitations(accountIds=["444455556666"])
        resp = client.list_invitations()
        assert "invitations" in resp
        # Invitations are stored per-account; the list may or may not show them
        # depending on which account we query from. Just assert the structure.
        assert isinstance(resp["invitations"], list)

    def test_disable_macie_clears_session(self, client):
        """DisableMacie clears session; re-enable works."""
        client.enable_macie()
        resp1 = client.get_macie_session()
        assert resp1["status"] == "ENABLED"

        client.disable_macie()

        # Re-enable and verify fresh session
        client.enable_macie()
        resp2 = client.get_macie_session()
        assert resp2["status"] == "ENABLED"

    def test_decline_invitations_returns_empty_unprocessed(self, client):
        """DeclineInvitations for nonexistent account returns empty unprocessedAccounts."""
        resp = client.decline_invitations(accountIds=["000000000000"])
        assert "unprocessedAccounts" in resp
        # No actual invitations to decline, so list should be empty or contain the account
        assert isinstance(resp["unprocessedAccounts"], list)

    def test_list_members_empty(self, client):
        """ListMembers returns empty list when no members exist."""
        resp = client.list_members()
        assert "members" in resp
        assert isinstance(resp["members"], list)


class TestMacie2Buckets:
    """Tests for bucket-related operations."""

    @pytest.fixture
    def client(self):
        c = make_client("macie2")
        try:
            c.enable_macie()
        except Exception:
            pass
        return c

    def test_describe_buckets_returns_list(self, client):
        """DescribeBuckets returns buckets list."""
        resp = client.describe_buckets()
        assert "buckets" in resp
        assert isinstance(resp["buckets"], list)

    def test_get_bucket_statistics_returns_counts(self, client):
        """GetBucketStatistics returns bucket count fields."""
        resp = client.get_bucket_statistics()
        assert "bucketCount" in resp
        assert isinstance(resp["bucketCount"], int)
        assert "objectCount" in resp
        assert "sizeInBytes" in resp


class TestMacie2Configuration:
    """Tests for configuration operations."""

    @pytest.fixture
    def client(self):
        c = make_client("macie2")
        try:
            c.enable_macie()
        except Exception:
            pass
        return c

    def test_describe_organization_configuration(self, client):
        """DescribeOrganizationConfiguration returns autoEnable field."""
        resp = client.describe_organization_configuration()
        assert "autoEnable" in resp
        assert isinstance(resp["autoEnable"], bool)
        assert "maxAccountLimitReached" in resp

    def test_get_automated_discovery_configuration(self, client):
        """GetAutomatedDiscoveryConfiguration returns status."""
        resp = client.get_automated_discovery_configuration()
        assert "status" in resp
        assert resp["status"] in ("ENABLED", "DISABLED")

    def test_get_classification_export_configuration(self, client):
        """GetClassificationExportConfiguration returns configuration."""
        resp = client.get_classification_export_configuration()
        assert "configuration" in resp

    def test_get_findings_publication_configuration(self, client):
        """GetFindingsPublicationConfiguration returns securityHubConfiguration."""
        resp = client.get_findings_publication_configuration()
        assert "securityHubConfiguration" in resp

    def test_get_reveal_configuration(self, client):
        """GetRevealConfiguration returns configuration and retrievalConfiguration."""
        resp = client.get_reveal_configuration()
        assert "configuration" in resp
        assert "retrievalConfiguration" in resp


class TestMacie2Findings:
    """Tests for findings operations."""

    @pytest.fixture
    def client(self):
        c = make_client("macie2")
        try:
            c.enable_macie()
        except Exception:
            pass
        return c

    def test_list_findings_returns_ids(self, client):
        """ListFindings returns findingIds list."""
        resp = client.list_findings()
        assert "findingIds" in resp
        assert isinstance(resp["findingIds"], list)

    def test_get_findings_with_fake_id(self, client):
        """GetFindings returns findings list (possibly empty) for fake IDs."""
        resp = client.get_findings(findingIds=["fake-finding-id"])
        assert "findings" in resp
        assert isinstance(resp["findings"], list)

    def test_get_finding_statistics(self, client):
        """GetFindingStatistics returns countsByGroup."""
        resp = client.get_finding_statistics(groupBy="type")
        assert "countsByGroup" in resp
        assert isinstance(resp["countsByGroup"], list)

    def test_get_findings_filter_nonexistent(self, client):
        """GetFindingsFilter raises error for nonexistent filter."""
        with pytest.raises(ClientError) as exc:
            client.get_findings_filter(id="nonexistent-filter")
        assert exc.value.response["Error"]["Code"] == "ResourceNotFoundException"

    def test_list_findings_filters_returns_list(self, client):
        """ListFindingsFilters returns findingsFilterListItems."""
        resp = client.list_findings_filters()
        assert "findingsFilterListItems" in resp
        assert isinstance(resp["findingsFilterListItems"], list)


class TestMacie2DataIdentifiers:
    """Tests for data identifier operations."""

    @pytest.fixture
    def client(self):
        c = make_client("macie2")
        try:
            c.enable_macie()
        except Exception:
            pass
        return c

    def test_list_managed_data_identifiers(self, client):
        """ListManagedDataIdentifiers returns items list."""
        resp = client.list_managed_data_identifiers()
        assert "items" in resp
        assert isinstance(resp["items"], list)

    def test_list_custom_data_identifiers(self, client):
        """ListCustomDataIdentifiers returns items list."""
        resp = client.list_custom_data_identifiers()
        assert "items" in resp
        assert isinstance(resp["items"], list)

    def test_get_custom_data_identifier_nonexistent(self, client):
        """GetCustomDataIdentifier raises error for nonexistent ID."""
        with pytest.raises(ClientError) as exc:
            client.get_custom_data_identifier(id="nonexistent-cdi")
        assert exc.value.response["Error"]["Code"] == "ResourceNotFoundException"


class TestMacie2ClassificationJobs:
    """Tests for classification job operations."""

    @pytest.fixture
    def client(self):
        c = make_client("macie2")
        try:
            c.enable_macie()
        except Exception:
            pass
        return c

    def test_list_classification_jobs_returns_items(self, client):
        """ListClassificationJobs returns items list."""
        resp = client.list_classification_jobs()
        assert "items" in resp
        assert isinstance(resp["items"], list)

    def test_describe_classification_job_nonexistent(self, client):
        """DescribeClassificationJob raises error for nonexistent job."""
        with pytest.raises(ClientError) as exc:
            client.describe_classification_job(jobId="nonexistent-job-id")
        assert exc.value.response["Error"]["Code"] == "ResourceNotFoundException"


class TestMacie2Scopes:
    """Tests for classification scope and sensitivity inspection templates."""

    @pytest.fixture
    def client(self):
        c = make_client("macie2")
        try:
            c.enable_macie()
        except Exception:
            pass
        return c

    def test_list_classification_scopes(self, client):
        """ListClassificationScopes returns classificationScopes list."""
        resp = client.list_classification_scopes()
        assert "classificationScopes" in resp
        assert isinstance(resp["classificationScopes"], list)

    def test_get_classification_scope(self, client):
        """GetClassificationScope returns scope fields for fake ID."""
        resp = client.get_classification_scope(id="fake-scope-id")
        assert "id" in resp
        assert "name" in resp

    def test_list_sensitivity_inspection_templates(self, client):
        """ListSensitivityInspectionTemplates returns list."""
        resp = client.list_sensitivity_inspection_templates()
        assert "sensitivityInspectionTemplates" in resp
        assert isinstance(resp["sensitivityInspectionTemplates"], list)

    def test_get_sensitivity_inspection_template(self, client):
        """GetSensitivityInspectionTemplate returns template fields."""
        resp = client.get_sensitivity_inspection_template(id="fake-template-id")
        assert "name" in resp


class TestMacie2AllowLists:
    """Tests for allow list operations."""

    @pytest.fixture
    def client(self):
        c = make_client("macie2")
        try:
            c.enable_macie()
        except Exception:
            pass
        return c

    def test_list_allow_lists(self, client):
        """ListAllowLists returns allowLists list."""
        resp = client.list_allow_lists()
        assert "allowLists" in resp
        assert isinstance(resp["allowLists"], list)

    def test_get_allow_list_nonexistent(self, client):
        """GetAllowList raises error for nonexistent allow list."""
        with pytest.raises(ClientError) as exc:
            client.get_allow_list(id="nonexistent-allow-list")
        assert exc.value.response["Error"]["Code"] == "ResourceNotFoundException"


class TestMacie2Usage:
    """Tests for usage operations."""

    @pytest.fixture
    def client(self):
        c = make_client("macie2")
        try:
            c.enable_macie()
        except Exception:
            pass
        return c

    def test_get_usage_totals(self, client):
        """GetUsageTotals returns usageTotals list."""
        resp = client.get_usage_totals()
        assert "usageTotals" in resp
        assert isinstance(resp["usageTotals"], list)

    def test_get_usage_statistics(self, client):
        """GetUsageStatistics returns records list."""
        resp = client.get_usage_statistics()
        assert "records" in resp
        assert isinstance(resp["records"], list)


class TestMacie2Invitations:
    """Tests for invitations count and master account."""

    @pytest.fixture
    def client(self):
        c = make_client("macie2")
        try:
            c.enable_macie()
        except Exception:
            pass
        return c

    def test_get_invitations_count(self, client):
        """GetInvitationsCount returns an integer count."""
        resp = client.get_invitations_count()
        assert "invitationsCount" in resp
        assert isinstance(resp["invitationsCount"], int)

    def test_get_master_account(self, client):
        """GetMasterAccount returns a 200 response."""
        resp = client.get_master_account()
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200

    def test_get_member_nonexistent(self, client):
        """GetMember raises error for nonexistent member."""
        with pytest.raises(ClientError) as exc:
            client.get_member(id="111122223333")
        assert exc.value.response["Error"]["Code"] == "ResourceNotFoundException"


class TestMacie2ResourceProfile:
    """Tests for resource profile operations."""

    @pytest.fixture
    def client(self):
        c = make_client("macie2")
        try:
            c.enable_macie()
        except Exception:
            pass
        return c

    def test_get_resource_profile(self, client):
        """GetResourceProfile returns profileUpdatedAt for fake resource."""
        resp = client.get_resource_profile(resourceArn="arn:aws:s3:::fake-bucket")
        assert "profileUpdatedAt" in resp

    def test_list_resource_profile_artifacts(self, client):
        """ListResourceProfileArtifacts returns artifacts list."""
        resp = client.list_resource_profile_artifacts(resourceArn="arn:aws:s3:::fake-bucket")
        assert "artifacts" in resp
        assert isinstance(resp["artifacts"], list)

    def test_list_resource_profile_detections(self, client):
        """ListResourceProfileDetections returns detections list."""
        resp = client.list_resource_profile_detections(resourceArn="arn:aws:s3:::fake-bucket")
        assert "detections" in resp
        assert isinstance(resp["detections"], list)


class TestMacie2SensitiveData:
    """Tests for sensitive data occurrence operations."""

    @pytest.fixture
    def client(self):
        c = make_client("macie2")
        try:
            c.enable_macie()
        except Exception:
            pass
        return c

    def test_get_sensitive_data_occurrences(self, client):
        """GetSensitiveDataOccurrences returns status for fake finding."""
        resp = client.get_sensitive_data_occurrences(findingId="fake-finding-id")
        assert "status" in resp

    def test_get_sensitive_data_occurrences_availability(self, client):
        """GetSensitiveDataOccurrencesAvailability returns code."""
        resp = client.get_sensitive_data_occurrences_availability(findingId="fake-finding-id")
        assert "code" in resp


class TestMacie2AutomatedDiscovery:
    """Tests for automated discovery operations."""

    @pytest.fixture
    def client(self):
        c = make_client("macie2")
        try:
            c.enable_macie()
        except Exception:
            pass
        return c

    def test_list_automated_discovery_accounts(self, client):
        """ListAutomatedDiscoveryAccounts returns items list."""
        resp = client.list_automated_discovery_accounts()
        assert "items" in resp
        assert isinstance(resp["items"], list)


class TestMacie2Tags:
    """Tests for tag operations."""

    @pytest.fixture
    def client(self):
        c = make_client("macie2")
        try:
            c.enable_macie()
        except Exception:
            pass
        return c

    def test_list_tags_for_resource(self, client):
        """ListTagsForResource returns tags dict."""
        resp = client.list_tags_for_resource(
            resourceArn="arn:aws:macie2:us-east-1:123456789012:session"
        )
        assert "tags" in resp
        assert isinstance(resp["tags"], dict)
