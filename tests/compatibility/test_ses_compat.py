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
        response = ses.get_identity_verification_attributes(Identities=["verify-attrs@example.com"])
        attrs = response["VerificationAttributes"]
        assert "verify-attrs@example.com" in attrs
        assert attrs["verify-attrs@example.com"]["VerificationStatus"] == "Success"

    def test_send_raw_email(self, ses):
        """Send a raw MIME email."""
        ses.verify_email_identity(EmailAddress="raw-sender@example.com")
        raw_message = (
            "From: raw-sender@example.com\r\n"
            "To: recipient@example.com\r\n"
            "Subject: Raw Test\r\n"
            "Content-Type: text/plain\r\n"
            "\r\n"
            "This is a raw email body.\r\n"
        )
        response = ses.send_raw_email(
            Source="raw-sender@example.com",
            RawMessage={"Data": raw_message},
        )
        assert "MessageId" in response
        assert response["ResponseMetadata"]["HTTPStatusCode"] == 200

    def test_list_verified_identities_email_type(self, ses):
        """List identities filtered by email type."""
        ses.verify_email_identity(EmailAddress="filter-email@example.com")
        response = ses.list_identities(IdentityType="EmailAddress")
        assert "filter-email@example.com" in response["Identities"]

    def test_get_send_quota(self, ses):
        """Get send quota returns expected fields."""
        response = ses.get_send_quota()
        assert "Max24HourSend" in response
        assert "SentLast24Hours" in response
        assert "MaxSendRate" in response
        assert response["ResponseMetadata"]["HTTPStatusCode"] == 200

    def test_set_identity_feedback_forwarding(self, ses):
        """Enable/disable feedback forwarding for an identity."""
        ses.verify_email_identity(EmailAddress="feedback@example.com")
        response = ses.set_identity_feedback_forwarding_enabled(
            Identity="feedback@example.com",
            ForwardingEnabled=False,
        )
        assert response["ResponseMetadata"]["HTTPStatusCode"] == 200

    def test_delete_identity(self, ses):
        """Verify and then delete an identity."""
        ses.verify_email_identity(EmailAddress="delete-me@example.com")
        identities = ses.list_identities()
        assert "delete-me@example.com" in identities["Identities"]
        ses.delete_identity(Identity="delete-me@example.com")
        identities = ses.list_identities()
        assert "delete-me@example.com" not in identities["Identities"]
