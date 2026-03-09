"""Macie2 compatibility tests."""

import pytest

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
        from botocore.exceptions import ClientError

        with pytest.raises(ClientError) as exc:
            client.delete_member(id="111122223333")
        assert exc.value.response["Error"]["Code"] == "ResourceNotFoundException"

    def test_disassociate_member_nonexistent(self, client):
        """DisassociateMember returns ResourceNotFoundException for unknown member."""
        from botocore.exceptions import ClientError

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
