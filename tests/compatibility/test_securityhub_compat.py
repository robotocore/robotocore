"""Security Hub compatibility tests."""

import uuid

import pytest
from botocore.exceptions import ClientError

from tests.compatibility.conftest import make_client


@pytest.fixture
def securityhub():
    return make_client("securityhub")


class TestSecurityHubOperations:
    def test_enable_describe_list_disable(self, securityhub):
        # Enable Security Hub
        enable_resp = securityhub.enable_security_hub(
            EnableDefaultStandards=False,
        )
        assert enable_resp["ResponseMetadata"]["HTTPStatusCode"] == 200

        try:
            # Describe hub
            describe_resp = securityhub.describe_hub()
            assert describe_resp["ResponseMetadata"]["HTTPStatusCode"] == 200
            assert "HubArn" in describe_resp

            # List members
            members_resp = securityhub.list_members()
            assert members_resp["ResponseMetadata"]["HTTPStatusCode"] == 200
            assert "Members" in members_resp
        finally:
            # Disable Security Hub (may fail if parallel state reset occurred)
            try:
                securityhub.disable_security_hub()
            except Exception:
                pass


class TestSecurityhubAutoCoverage:
    """Auto-generated coverage tests for securityhub."""

    @pytest.fixture
    def client(self):
        return make_client("securityhub")

    def test_get_administrator_account(self, client):
        """GetAdministratorAccount returns a response."""
        client.get_administrator_account()

    def test_get_findings(self, client):
        """GetFindings returns a response."""
        resp = client.get_findings()
        assert "Findings" in resp

    def test_get_master_account(self, client):
        """GetMasterAccount returns a response."""
        client.get_master_account()

    def test_describe_organization_configuration(self, client):
        """DescribeOrganizationConfiguration requires org admin access."""
        with pytest.raises(ClientError) as exc:
            client.describe_organization_configuration()
        assert exc.value.response["Error"]["Code"] == "AccessDeniedException"

    def test_update_organization_configuration(self, client):
        """UpdateOrganizationConfiguration without hub returns error."""
        with pytest.raises(ClientError) as exc:
            client.update_organization_configuration(AutoEnable=True)
        assert exc.value.response["Error"]["Code"] in (
            "ResourceNotFoundException",
            "AccessDeniedException",
        )

    def test_get_members(self, client):
        """GetMembers returns UnprocessedAccounts for unknown accounts."""
        resp = client.get_members(AccountIds=["999999999999"])
        assert "Members" in resp
        assert "UnprocessedAccounts" in resp


class TestSecurityHubFindings:
    """Tests for SecurityHub findings operations."""

    @pytest.fixture
    def client(self):
        return make_client("securityhub")

    def _make_finding(self, finding_id=None, severity="HIGH", title="Test Finding"):
        finding_id = finding_id or f"finding-{uuid.uuid4().hex[:8]}"
        return {
            "SchemaVersion": "2018-10-08",
            "Id": finding_id,
            "ProductArn": (
                "arn:aws:securityhub:us-east-1:123456789012:product/123456789012/default"
            ),
            "GeneratorId": "test-generator",
            "AwsAccountId": "123456789012",
            "Types": ["Software and Configuration Checks"],
            "CreatedAt": "2024-01-01T00:00:00Z",
            "UpdatedAt": "2024-01-01T00:00:00Z",
            "Severity": {"Label": severity},
            "Title": title,
            "Description": "Test finding for compat tests",
            "Resources": [
                {
                    "Type": "AwsEc2Instance",
                    "Id": f"i-{uuid.uuid4().hex[:8]}",
                    "Region": "us-east-1",
                }
            ],
        }

    def test_batch_import_findings(self, client):
        """BatchImportFindings imports findings successfully."""
        finding = self._make_finding()
        resp = client.batch_import_findings(Findings=[finding])
        assert resp["SuccessCount"] == 1
        assert resp["FailedCount"] == 0
        assert resp["FailedFindings"] == []

    def test_batch_import_multiple_findings(self, client):
        """BatchImportFindings handles multiple findings at once."""
        findings = [
            self._make_finding(severity="HIGH", title="Finding A"),
            self._make_finding(severity="CRITICAL", title="Finding B"),
            self._make_finding(severity="LOW", title="Finding C"),
        ]
        resp = client.batch_import_findings(Findings=findings)
        assert resp["SuccessCount"] == 3
        assert resp["FailedCount"] == 0

    def test_get_findings_with_severity_filter(self, client):
        """GetFindings can filter by severity label."""
        # Import a finding with a unique severity
        finding = self._make_finding(severity="CRITICAL", title="Critical Test")
        client.batch_import_findings(Findings=[finding])

        resp = client.get_findings(
            Filters={"SeverityLabel": [{"Value": "CRITICAL", "Comparison": "EQUALS"}]}
        )
        assert "Findings" in resp
        # We imported at least one CRITICAL finding
        titles = [f["Title"] for f in resp["Findings"]]
        assert "Critical Test" in titles

    def test_get_findings_returns_imported(self, client):
        """GetFindings returns previously imported findings."""
        unique_id = f"finding-{uuid.uuid4().hex[:8]}"
        finding = self._make_finding(finding_id=unique_id, title="Imported Finding")
        client.batch_import_findings(Findings=[finding])

        resp = client.get_findings()
        assert "Findings" in resp
        found_ids = [f["Id"] for f in resp["Findings"]]
        assert unique_id in found_ids


class TestSecurityHubMembers:
    """Tests for SecurityHub member operations."""

    @pytest.fixture
    def hub_client(self):
        client = make_client("securityhub")
        try:
            client.enable_security_hub(EnableDefaultStandards=False)
        except ClientError:
            pass  # Already enabled
        yield client
        try:
            client.disable_security_hub()
        except Exception:
            pass

    @staticmethod
    def _random_account_id():
        """Generate a random 12-digit AWS account ID."""
        return str(uuid.uuid4().int)[:12]

    def test_create_members(self, hub_client):
        """CreateMembers registers member accounts."""
        acct_id = self._random_account_id()
        resp = hub_client.create_members(
            AccountDetails=[
                {"AccountId": acct_id, "Email": "member1@example.com"},
            ]
        )
        assert "UnprocessedAccounts" in resp
        assert isinstance(resp["UnprocessedAccounts"], list)

    def test_create_and_list_members(self, hub_client):
        """CreateMembers then ListMembers shows the created member."""
        acct_id = self._random_account_id()
        hub_client.create_members(
            AccountDetails=[
                {"AccountId": acct_id, "Email": "member2@example.com"},
            ]
        )
        resp = hub_client.list_members()
        assert "Members" in resp
        account_ids = [m["AccountId"] for m in resp["Members"]]
        assert acct_id in account_ids

    def test_create_members_multiple(self, hub_client):
        """CreateMembers handles multiple accounts."""
        acct_a = self._random_account_id()
        acct_b = self._random_account_id()
        resp = hub_client.create_members(
            AccountDetails=[
                {"AccountId": acct_a, "Email": "a@example.com"},
                {"AccountId": acct_b, "Email": "b@example.com"},
            ]
        )
        assert isinstance(resp["UnprocessedAccounts"], list)
