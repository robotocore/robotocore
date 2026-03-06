"""SES compatibility tests."""

import pytest
from tests.compatibility.conftest import make_client


@pytest.fixture
def ses():
    return make_client("ses")


class TestSESOperations:
    def test_verify_email_identity(self, ses):
        response = ses.verify_email_identity(EmailAddress="test@example.com")
        assert response["ResponseMetadata"]["HTTPStatusCode"] == 200

    def test_list_identities(self, ses):
        ses.verify_email_identity(EmailAddress="list@example.com")
        response = ses.list_identities()
        assert "list@example.com" in response["Identities"]

    def test_send_email(self, ses):
        ses.verify_email_identity(EmailAddress="sender@example.com")
        response = ses.send_email(
            Source="sender@example.com",
            Destination={"ToAddresses": ["recipient@example.com"]},
            Message={
                "Subject": {"Data": "Test Subject"},
                "Body": {"Text": {"Data": "Test body"}},
            },
        )
        assert "MessageId" in response

    def test_verify_domain_identity(self, ses):
        response = ses.verify_domain_identity(Domain="example.org")
        assert "VerificationToken" in response
        assert response["ResponseMetadata"]["HTTPStatusCode"] == 200
        # Confirm the domain appears in identities
        identities = ses.list_identities(IdentityType="Domain")
        assert "example.org" in identities["Identities"]

    def test_get_send_statistics(self, ses):
        response = ses.get_send_statistics()
        assert "SendDataPoints" in response
        assert isinstance(response["SendDataPoints"], list)
        assert response["ResponseMetadata"]["HTTPStatusCode"] == 200

    def test_get_identity_verification_attributes(self, ses):
        ses.verify_email_identity(EmailAddress="verify-attrs@example.com")
        response = ses.get_identity_verification_attributes(
            Identities=["verify-attrs@example.com"]
        )
        attrs = response["VerificationAttributes"]
        assert "verify-attrs@example.com" in attrs
        assert attrs["verify-attrs@example.com"]["VerificationStatus"] == "Success"
