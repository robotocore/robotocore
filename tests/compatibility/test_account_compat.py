"""Account compatibility tests."""

import pytest

from tests.compatibility.conftest import make_client


@pytest.fixture
def account():
    return make_client("account")


class TestAccountOperations:
    def test_put_get_delete_alternate_contact(self, account):
        # Put alternate contact
        put_resp = account.put_alternate_contact(
            AlternateContactType="BILLING",
            Name="Test",
            EmailAddress="test@example.com",
            PhoneNumber="555-0100",
            Title="Billing Contact",
        )
        assert put_resp["ResponseMetadata"]["HTTPStatusCode"] == 200

        # Get alternate contact
        get_resp = account.get_alternate_contact(
            AlternateContactType="BILLING",
        )
        assert get_resp["ResponseMetadata"]["HTTPStatusCode"] == 200
        contact = get_resp["AlternateContact"]
        assert contact["AlternateContactType"] == "BILLING"
        assert contact["Name"] == "Test"
        assert contact["EmailAddress"] == "test@example.com"

        # Delete alternate contact
        delete_resp = account.delete_alternate_contact(
            AlternateContactType="BILLING",
        )
        assert delete_resp["ResponseMetadata"]["HTTPStatusCode"] == 200
