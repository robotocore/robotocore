"""Account compatibility tests."""

import pytest

from tests.compatibility.conftest import make_client


@pytest.fixture
def account():
    return make_client("account")


class TestAccountAlternateContact:
    """Tests for Account alternate contact operations."""

    def test_put_alternate_contact(self, account):
        """PutAlternateContact stores a billing contact."""
        resp = account.put_alternate_contact(
            AlternateContactType="BILLING",
            Name="Billing Test",
            EmailAddress="billing@example.com",
            PhoneNumber="555-0100",
            Title="Billing Contact",
        )
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200
        # Verify contact was stored
        get_resp = account.get_alternate_contact(AlternateContactType="BILLING")
        assert get_resp["AlternateContact"]["EmailAddress"] == "billing@example.com"

    def test_get_alternate_contact(self, account):
        """GetAlternateContact retrieves a previously stored contact."""
        account.put_alternate_contact(
            AlternateContactType="BILLING",
            Name="Get Test",
            EmailAddress="get@example.com",
            PhoneNumber="555-0200",
            Title="Get Title",
        )
        resp = account.get_alternate_contact(AlternateContactType="BILLING")
        contact = resp["AlternateContact"]
        assert contact["AlternateContactType"] == "BILLING"
        assert contact["Name"] == "Get Test"
        assert contact["EmailAddress"] == "get@example.com"
        assert contact["PhoneNumber"] == "555-0200"
        assert contact["Title"] == "Get Title"

    def test_delete_alternate_contact(self, account):
        """DeleteAlternateContact removes a stored contact."""
        account.put_alternate_contact(
            AlternateContactType="BILLING",
            Name="Delete Test",
            EmailAddress="delete@example.com",
            PhoneNumber="555-0300",
            Title="Delete Title",
        )
        resp = account.delete_alternate_contact(AlternateContactType="BILLING")
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200
        # Verify contact is gone (should raise an exception)
        from botocore.exceptions import ClientError

        try:
            account.get_alternate_contact(AlternateContactType="BILLING")
        except ClientError as exc:
            assert exc.response["Error"]["Code"] in ("ResourceNotFoundException", "NoSuchEntity")

    def test_put_operations_contact(self, account):
        """PutAlternateContact works with OPERATIONS type."""
        resp = account.put_alternate_contact(
            AlternateContactType="OPERATIONS",
            Name="Ops Contact",
            EmailAddress="ops@example.com",
            PhoneNumber="555-0400",
            Title="Operations Manager",
        )
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200
        get_resp = account.get_alternate_contact(AlternateContactType="OPERATIONS")
        assert get_resp["AlternateContact"]["Name"] == "Ops Contact"

    def test_put_security_contact(self, account):
        """PutAlternateContact works with SECURITY type."""
        resp = account.put_alternate_contact(
            AlternateContactType="SECURITY",
            Name="Security Contact",
            EmailAddress="security@example.com",
            PhoneNumber="555-0500",
            Title="CISO",
        )
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200
        get_resp = account.get_alternate_contact(AlternateContactType="SECURITY")
        assert get_resp["AlternateContact"]["Name"] == "Security Contact"

    def test_put_overwrites_existing_contact(self, account):
        """PutAlternateContact overwrites an existing contact."""
        account.put_alternate_contact(
            AlternateContactType="BILLING",
            Name="Original",
            EmailAddress="original@example.com",
            PhoneNumber="555-0600",
            Title="V1",
        )
        account.put_alternate_contact(
            AlternateContactType="BILLING",
            Name="Updated",
            EmailAddress="updated@example.com",
            PhoneNumber="555-0700",
            Title="V2",
        )
        resp = account.get_alternate_contact(AlternateContactType="BILLING")
        assert resp["AlternateContact"]["Name"] == "Updated"
        assert resp["AlternateContact"]["EmailAddress"] == "updated@example.com"


class TestAccountNewOps:
    """Tests for newly implemented Account operations."""

    def test_get_account_information(self, account):
        """GetAccountInformation returns account ID and name."""
        resp = account.get_account_information()
        assert "AccountId" in resp
        assert "AccountName" in resp

    def test_get_contact_information(self, account):
        """GetContactInformation returns ContactInformation dict."""
        resp = account.get_contact_information()
        assert "ContactInformation" in resp

    def test_put_and_get_contact_information(self, account):
        """PutContactInformation stores contact info, GetContactInformation retrieves it."""
        account.put_contact_information(
            ContactInformation={
                "FullName": "Test User",
                "AddressLine1": "123 Main St",
                "City": "Testville",
                "StateOrRegion": "CA",
                "PostalCode": "12345",
                "CountryCode": "US",
                "PhoneNumber": "+15555555555",
            }
        )
        resp = account.get_contact_information()
        ci = resp["ContactInformation"]
        assert ci["FullName"] == "Test User"
        assert ci["City"] == "Testville"

    def test_get_primary_email(self, account):
        """GetPrimaryEmail returns an email address."""
        resp = account.get_primary_email(AccountId="123456789012")
        assert "PrimaryEmail" in resp

    def test_get_region_opt_status(self, account):
        """GetRegionOptStatus returns region name and opt status."""
        resp = account.get_region_opt_status(RegionName="us-east-1")
        assert resp["RegionName"] == "us-east-1"
        assert "RegionOptStatus" in resp
        assert resp["RegionOptStatus"] in ("ENABLED", "DISABLED", "ENABLING", "DISABLING")

    def test_list_regions(self, account):
        """ListRegions returns a list of regions."""
        resp = account.list_regions()
        assert "Regions" in resp
        assert isinstance(resp["Regions"], list)
        assert len(resp["Regions"]) > 0

    def test_list_regions_filtered(self, account):
        """ListRegions with RegionOptStatusContains filters results."""
        resp = account.list_regions(RegionOptStatusContains=["ENABLED"])
        for region in resp["Regions"]:
            assert region["RegionOptStatus"] == "ENABLED"

    def test_get_gov_cloud_account_information(self, account):
        """GetGovCloudAccountInformation returns a response."""
        resp = account.get_gov_cloud_account_information()
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200
        assert isinstance(resp["AccountState"], str)

    def test_enable_region(self, account):
        """EnableRegion enables a region."""
        resp = account.enable_region(RegionName="ap-southeast-1")
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200
        # Verify region is now enabled
        regions = account.list_regions(RegionOptStatusContains=["ENABLED", "ENABLING"])
        region_names = [r["RegionName"] for r in regions["Regions"]]
        assert "ap-southeast-1" in region_names

    def test_disable_region(self, account):
        """DisableRegion disables a region."""
        resp = account.disable_region(RegionName="ap-southeast-2")
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200
        assert resp["ResponseMetadata"]["RequestId"] is not None


class TestAccountMissingGapOps:
    """Tests for previously-missing Account operations."""

    def test_put_account_name(self, account):
        """PutAccountName updates the account name."""
        resp = account.put_account_name(AccountName="My Test Account")
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200
        assert resp["ResponseMetadata"]["RequestId"] is not None

    def test_start_primary_email_update(self, account):
        """StartPrimaryEmailUpdate initiates an email update."""
        resp = account.start_primary_email_update(
            AccountId="123456789012", PrimaryEmail="new@example.com"
        )
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200
        assert isinstance(resp["Status"], str)

    def test_accept_primary_email_update(self, account):
        """AcceptPrimaryEmailUpdate completes the email update flow."""
        resp = account.accept_primary_email_update(
            AccountId="123456789012", Otp="123456", PrimaryEmail="new@example.com"
        )
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200
        assert isinstance(resp["Status"], str)
