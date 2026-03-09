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
