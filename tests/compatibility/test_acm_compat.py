"""ACM compatibility tests."""

import pytest
from botocore.exceptions import ClientError

from tests.compatibility.conftest import make_client


@pytest.fixture
def acm():
    return make_client("acm")


class TestACMOperations:
    def test_request_certificate(self, acm):
        response = acm.request_certificate(DomainName="example.com")
        assert "CertificateArn" in response

    def test_list_certificates(self, acm):
        acm.request_certificate(DomainName="list-test.example.com")
        response = acm.list_certificates()
        assert len(response["CertificateSummaryList"]) >= 1

    def test_describe_certificate(self, acm):
        arn = acm.request_certificate(DomainName="describe.example.com")["CertificateArn"]
        response = acm.describe_certificate(CertificateArn=arn)
        assert response["Certificate"]["DomainName"] == "describe.example.com"

    def test_describe_certificate_fields(self, acm):
        arn = acm.request_certificate(DomainName="fields.example.com")["CertificateArn"]
        response = acm.describe_certificate(CertificateArn=arn)
        cert = response["Certificate"]
        assert cert["CertificateArn"] == arn
        assert "Status" in cert
        assert "Type" in cert
        assert cert["DomainName"] == "fields.example.com"

    def test_list_tags_for_certificate(self, acm):
        arn = acm.request_certificate(DomainName="tags.example.com")["CertificateArn"]
        acm.add_tags_to_certificate(
            CertificateArn=arn,
            Tags=[
                {"Key": "Environment", "Value": "test"},
                {"Key": "Project", "Value": "robotocore"},
            ],
        )
        response = acm.list_tags_for_certificate(CertificateArn=arn)
        tags = {t["Key"]: t["Value"] for t in response["Tags"]}
        assert tags["Environment"] == "test"
        assert tags["Project"] == "robotocore"

    def test_remove_tags_from_certificate(self, acm):
        arn = acm.request_certificate(DomainName="rmtags.example.com")["CertificateArn"]
        acm.add_tags_to_certificate(
            CertificateArn=arn,
            Tags=[{"Key": "ToRemove", "Value": "yes"}],
        )
        acm.remove_tags_from_certificate(
            CertificateArn=arn,
            Tags=[{"Key": "ToRemove", "Value": "yes"}],
        )
        response = acm.list_tags_for_certificate(CertificateArn=arn)
        keys = [t["Key"] for t in response.get("Tags", [])]
        assert "ToRemove" not in keys

    def test_request_certificate_with_sans(self, acm):
        """Request a certificate with SubjectAlternativeNames."""
        response = acm.request_certificate(
            DomainName="san.example.com",
            SubjectAlternativeNames=[
                "san.example.com",
                "www.san.example.com",
                "api.san.example.com",
            ],
        )
        arn = response["CertificateArn"]
        cert = acm.describe_certificate(CertificateArn=arn)["Certificate"]
        sans = cert.get("SubjectAlternativeNames", [])
        assert "san.example.com" in sans
        assert "www.san.example.com" in sans
        assert "api.san.example.com" in sans

    def test_describe_certificate_all_fields(self, acm):
        """Verify DescribeCertificate returns expected fields."""
        arn = acm.request_certificate(DomainName="allfields.example.com")["CertificateArn"]
        cert = acm.describe_certificate(CertificateArn=arn)["Certificate"]
        assert cert["CertificateArn"] == arn
        assert cert["DomainName"] == "allfields.example.com"
        assert "Status" in cert
        assert "Type" in cert
        assert "SubjectAlternativeNames" in cert
        assert "CreatedAt" in cert

    def test_delete_certificate(self, acm):
        """Request a certificate, then delete it."""
        arn = acm.request_certificate(DomainName="delete.example.com")["CertificateArn"]
        acm.delete_certificate(CertificateArn=arn)
        # Verify it's gone
        response = acm.list_certificates()
        arns = [c["CertificateArn"] for c in response["CertificateSummaryList"]]
        assert arn not in arns

    def test_request_and_delete_certificate(self, acm):
        """Request multiple certificates, delete one, verify the other remains."""
        arn1 = acm.request_certificate(DomainName="keep.example.com")["CertificateArn"]
        arn2 = acm.request_certificate(DomainName="remove.example.com")["CertificateArn"]
        acm.delete_certificate(CertificateArn=arn2)

        response = acm.list_certificates()
        arns = [c["CertificateArn"] for c in response["CertificateSummaryList"]]
        assert arn1 in arns
        assert arn2 not in arns

    def test_add_multiple_tags(self, acm):
        """Add multiple tags and verify all are returned."""
        arn = acm.request_certificate(DomainName="multitag.example.com")["CertificateArn"]
        acm.add_tags_to_certificate(
            CertificateArn=arn,
            Tags=[
                {"Key": "team", "Value": "platform"},
                {"Key": "cost-center", "Value": "12345"},
                {"Key": "managed-by", "Value": "robotocore"},
            ],
        )
        response = acm.list_tags_for_certificate(CertificateArn=arn)
        tags = {t["Key"]: t["Value"] for t in response["Tags"]}
        assert tags["team"] == "platform"
        assert tags["cost-center"] == "12345"
        assert tags["managed-by"] == "robotocore"

    def test_delete_nonexistent_certificate(self, acm):
        """Deleting a non-existent certificate should raise an error."""
        fake_arn = (
            "arn:aws:acm:us-east-1:123456789012:certificate/"
            "00000000-0000-0000-0000-000000000000"
        )
        with pytest.raises(ClientError) as exc_info:
            acm.delete_certificate(CertificateArn=fake_arn)
        assert exc_info.value.response["Error"]["Code"] == "ResourceNotFoundException"

    def test_describe_nonexistent_certificate(self, acm):
        """Describing a non-existent certificate should raise an error."""
        fake_arn = (
            "arn:aws:acm:us-east-1:123456789012:certificate/"
            "11111111-1111-1111-1111-111111111111"
        )
        with pytest.raises(ClientError) as exc_info:
            acm.describe_certificate(CertificateArn=fake_arn)
        assert exc_info.value.response["Error"]["Code"] == "ResourceNotFoundException"

    def test_get_certificate_pending_validation(self, acm):
        """GetCertificate on a pending certificate returns a response or raises."""
        arn = acm.request_certificate(DomainName="pending.example.com")["CertificateArn"]
        try:
            response = acm.get_certificate(CertificateArn=arn)
            # If it doesn't raise, it should have CertificateChain or Certificate
            assert "Certificate" in response or "CertificateChain" in response
        except ClientError as e:
            # Some implementations raise for pending certs
            error_code = e.response["Error"]["Code"]
            assert error_code in ("RequestInProgressException", "ResourceNotFoundException")

    def test_list_certificates_includes_domain(self, acm):
        """ListCertificates should show the domain name in the summary."""
        arn = acm.request_certificate(DomainName="listsummary.example.com")["CertificateArn"]
        response = acm.list_certificates()
        matching = [
            c for c in response["CertificateSummaryList"]
            if c["CertificateArn"] == arn
        ]
        assert len(matching) == 1
        assert matching[0]["DomainName"] == "listsummary.example.com"

    def test_export_certificate_invalid_arn(self, acm):
        """ExportCertificate with bad ARN should raise an error."""
        fake_arn = (
            "arn:aws:acm:us-east-1:123456789012:certificate/"
            "22222222-2222-2222-2222-222222222222"
        )
        with pytest.raises(ClientError) as exc_info:
            acm.export_certificate(
                CertificateArn=fake_arn,
                Passphrase=b"test-passphrase",
            )
        assert exc_info.value.response["Error"]["Code"] == "ResourceNotFoundException"

    def test_request_certificate_idempotency_token(self, acm):
        """Request with IdempotencyToken should succeed."""
        response = acm.request_certificate(
            DomainName="idempotent.example.com",
            IdempotencyToken="unique-token-123",
        )
        assert "CertificateArn" in response

    def test_certificate_status_is_pending(self, acm):
        """Newly requested certificate should have PENDING_VALIDATION status."""
        arn = acm.request_certificate(DomainName="status.example.com")["CertificateArn"]
        cert = acm.describe_certificate(CertificateArn=arn)["Certificate"]
        assert cert["Status"] == "PENDING_VALIDATION"
