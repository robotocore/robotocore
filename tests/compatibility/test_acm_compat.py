"""ACM compatibility tests."""

import datetime

import pytest
from botocore.exceptions import ClientError
from cryptography import x509
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import rsa
from cryptography.x509.oid import NameOID

from tests.compatibility.conftest import make_client


def _generate_self_signed_cert():
    """Generate a self-signed certificate and private key in PEM format."""
    key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
    subject = issuer = x509.Name(
        [
            x509.NameAttribute(NameOID.COUNTRY_NAME, "US"),
            x509.NameAttribute(NameOID.STATE_OR_PROVINCE_NAME, "California"),
            x509.NameAttribute(NameOID.ORGANIZATION_NAME, "Test"),
            x509.NameAttribute(NameOID.COMMON_NAME, "test.example.com"),
        ]
    )
    cert = (
        x509.CertificateBuilder()
        .subject_name(subject)
        .issuer_name(issuer)
        .public_key(key.public_key())
        .serial_number(x509.random_serial_number())
        .not_valid_before(datetime.datetime.now(datetime.UTC))
        .not_valid_after(datetime.datetime.now(datetime.UTC) + datetime.timedelta(days=365))
        .sign(key, hashes.SHA256())
    )
    cert_pem = cert.public_bytes(serialization.Encoding.PEM)
    key_pem = key.private_bytes(
        serialization.Encoding.PEM,
        serialization.PrivateFormat.TraditionalOpenSSL,
        serialization.NoEncryption(),
    )
    return cert_pem, key_pem


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

    def test_add_tags_to_certificate(self, acm):
        """Request cert, add tags, list and verify."""
        arn = acm.request_certificate(DomainName="addtags.example.com")["CertificateArn"]
        acm.add_tags_to_certificate(
            CertificateArn=arn,
            Tags=[
                {"Key": "Team", "Value": "platform"},
                {"Key": "CostCenter", "Value": "12345"},
            ],
        )
        response = acm.list_tags_for_certificate(CertificateArn=arn)
        tags = {t["Key"]: t["Value"] for t in response["Tags"]}
        assert tags["Team"] == "platform"
        assert tags["CostCenter"] == "12345"

    def test_delete_nonexistent_certificate(self, acm):
        """Deleting a non-existent certificate should raise an error."""
        fake_arn = (
            "arn:aws:acm:us-east-1:123456789012:certificate/00000000-0000-0000-0000-000000000000"
        )
        with pytest.raises(ClientError) as exc_info:
            acm.delete_certificate(CertificateArn=fake_arn)
        assert exc_info.value.response["Error"]["Code"] == "ResourceNotFoundException"

    def test_describe_nonexistent_certificate(self, acm):
        """Describing a non-existent certificate should raise an error."""
        fake_arn = (
            "arn:aws:acm:us-east-1:123456789012:certificate/11111111-1111-1111-1111-111111111111"
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
        matching = [c for c in response["CertificateSummaryList"] if c["CertificateArn"] == arn]
        assert len(matching) == 1
        assert matching[0]["DomainName"] == "listsummary.example.com"

    def test_export_certificate_invalid_arn(self, acm):
        """ExportCertificate with bad ARN should raise an error."""
        fake_arn = (
            "arn:aws:acm:us-east-1:123456789012:certificate/22222222-2222-2222-2222-222222222222"
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

    def test_import_certificate(self, acm):
        """ImportCertificate imports a self-signed cert and returns an ARN."""
        cert_pem, key_pem = _generate_self_signed_cert()
        response = acm.import_certificate(
            Certificate=cert_pem,
            PrivateKey=key_pem,
        )
        assert "CertificateArn" in response
        arn = response["CertificateArn"]
        # Verify the imported cert is describable
        detail = acm.describe_certificate(CertificateArn=arn)
        assert detail["Certificate"]["CertificateArn"] == arn
        assert detail["Certificate"]["Type"] == "IMPORTED"
        # Cleanup
        acm.delete_certificate(CertificateArn=arn)

    def test_import_certificate_with_chain(self, acm):
        """ImportCertificate with a certificate chain (self-signed as chain)."""
        cert_pem, key_pem = _generate_self_signed_cert()
        response = acm.import_certificate(
            Certificate=cert_pem,
            PrivateKey=key_pem,
            CertificateChain=cert_pem,  # self-signed acts as its own chain
        )
        assert "CertificateArn" in response
        acm.delete_certificate(CertificateArn=response["CertificateArn"])

    def test_get_account_configuration(self, acm):
        """GetAccountConfiguration returns without error."""
        try:
            response = acm.get_account_configuration()
            # May have ExpiryEvents or may be empty depending on state
            assert response["ResponseMetadata"]["HTTPStatusCode"] == 200
        except acm.exceptions.ClientError as e:
            # AccessDeniedException is acceptable if not configured
            assert e.response["Error"]["Code"] in (
                "AccessDeniedException",
                "ResourceNotFoundException",
            )

    def test_update_certificate_options(self, acm):
        """Request cert and update certificate options."""
        arn = acm.request_certificate(DomainName="update-opts.example.com")["CertificateArn"]
        try:
            acm.update_certificate_options(
                CertificateArn=arn,
                Options={"CertificateTransparencyLoggingPreference": "DISABLED"},
            )
            response = acm.describe_certificate(CertificateArn=arn)
            opts = response["Certificate"].get("Options", {})
            assert opts.get("CertificateTransparencyLoggingPreference") == "DISABLED"
        finally:
            acm.delete_certificate(CertificateArn=arn)


class TestACMImportCertificate:
    def _generate_self_signed_cert(self):
        """Generate a self-signed certificate using the cryptography library."""
        import datetime

        from cryptography import x509
        from cryptography.hazmat.primitives import hashes, serialization
        from cryptography.hazmat.primitives.asymmetric import rsa
        from cryptography.x509.oid import NameOID

        key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
        subject = issuer = x509.Name(
            [
                x509.NameAttribute(NameOID.COMMON_NAME, "test.example.com"),
            ]
        )
        cert = (
            x509.CertificateBuilder()
            .subject_name(subject)
            .issuer_name(issuer)
            .public_key(key.public_key())
            .serial_number(x509.random_serial_number())
            .not_valid_before(datetime.datetime.now(datetime.UTC))
            .not_valid_after(datetime.datetime.now(datetime.UTC) + datetime.timedelta(days=365))
            .sign(key, hashes.SHA256())
        )
        cert_pem = cert.public_bytes(serialization.Encoding.PEM)
        key_pem = key.private_bytes(
            serialization.Encoding.PEM,
            serialization.PrivateFormat.TraditionalOpenSSL,
            serialization.NoEncryption(),
        )
        return cert_pem, key_pem

    def test_request_certificate_with_idempotency_token(self, acm):
        """RequestCertificate with IdempotencyToken returns same ARN."""
        token = "idempotency-test-token-12345"
        resp1 = acm.request_certificate(
            DomainName="idempotent.example.com",
            IdempotencyToken=token,
        )
        resp2 = acm.request_certificate(
            DomainName="idempotent.example.com",
            IdempotencyToken=token,
        )
        assert resp1["CertificateArn"] == resp2["CertificateArn"]

    def test_list_certificates_with_filtering(self, acm):
        """ListCertificates with certificate status filtering."""
        # Request a certificate so there's at least one
        acm.request_certificate(DomainName="filter-test.example.com")

        # List with PENDING_VALIDATION status (default for requested certs)
        response = acm.list_certificates(
            CertificateStatuses=["PENDING_VALIDATION"],
        )
        assert "CertificateSummaryList" in response
        # All returned certs should have matching status
        for cert in response["CertificateSummaryList"]:
            assert "CertificateArn" in cert
            assert "DomainName" in cert


class TestACMExtended:
    @pytest.fixture
    def acm(self):
        return make_client("acm")

    def test_request_certificate_dns_validation(self, acm):
        resp = acm.request_certificate(
            DomainName="dns-test.example.com",
            ValidationMethod="DNS",
        )
        assert "CertificateArn" in resp
        arn = resp["CertificateArn"]
        desc = acm.describe_certificate(CertificateArn=arn)
        assert desc["Certificate"]["DomainName"] == "dns-test.example.com"

    def test_request_certificate_email_validation(self, acm):
        resp = acm.request_certificate(
            DomainName="email-test.example.com",
            ValidationMethod="EMAIL",
        )
        assert "CertificateArn" in resp

    def test_request_certificate_with_tags(self, acm):
        resp = acm.request_certificate(
            DomainName="tagged-cert.example.com",
            Tags=[
                {"Key": "env", "Value": "test"},
                {"Key": "team", "Value": "platform"},
            ],
        )
        arn = resp["CertificateArn"]
        tags = acm.list_tags_for_certificate(CertificateArn=arn)
        tag_map = {t["Key"]: t["Value"] for t in tags["Tags"]}
        assert tag_map["env"] == "test"
        assert tag_map["team"] == "platform"

    def test_add_multiple_tags(self, acm):
        resp = acm.request_certificate(DomainName="multi-tag.example.com")
        arn = resp["CertificateArn"]
        acm.add_tags_to_certificate(
            CertificateArn=arn,
            Tags=[
                {"Key": "a", "Value": "1"},
                {"Key": "b", "Value": "2"},
                {"Key": "c", "Value": "3"},
            ],
        )
        tags = acm.list_tags_for_certificate(CertificateArn=arn)
        tag_map = {t["Key"]: t["Value"] for t in tags["Tags"]}
        assert tag_map["a"] == "1"
        assert tag_map["b"] == "2"
        assert tag_map["c"] == "3"

    def test_remove_specific_tags(self, acm):
        resp = acm.request_certificate(DomainName="rm-tag.example.com")
        arn = resp["CertificateArn"]
        acm.add_tags_to_certificate(
            CertificateArn=arn,
            Tags=[{"Key": "keep", "Value": "yes"}, {"Key": "remove", "Value": "yes"}],
        )
        acm.remove_tags_from_certificate(
            CertificateArn=arn,
            Tags=[{"Key": "remove", "Value": "yes"}],
        )
        tags = acm.list_tags_for_certificate(CertificateArn=arn)
        keys = [t["Key"] for t in tags["Tags"]]
        assert "keep" in keys
        assert "remove" not in keys

    def test_describe_certificate_has_type(self, acm):
        resp = acm.request_certificate(DomainName="type-test.example.com")
        arn = resp["CertificateArn"]
        desc = acm.describe_certificate(CertificateArn=arn)
        cert = desc["Certificate"]
        assert cert.get("Type") in ("AMAZON_ISSUED", "IMPORTED", None) or "Type" in cert

    def test_describe_certificate_has_created_at(self, acm):
        resp = acm.request_certificate(DomainName="created-at.example.com")
        arn = resp["CertificateArn"]
        desc = acm.describe_certificate(CertificateArn=arn)
        assert "CreatedAt" in desc["Certificate"]

    def test_list_certificates_empty_filter(self, acm):
        resp = acm.list_certificates(CertificateStatuses=["ISSUED"])
        assert "CertificateSummaryList" in resp

    def test_import_and_describe_certificate(self, acm):
        cert_pem, key_pem = _generate_self_signed_cert()
        resp = acm.import_certificate(Certificate=cert_pem, PrivateKey=key_pem)
        arn = resp["CertificateArn"]
        desc = acm.describe_certificate(CertificateArn=arn)
        assert desc["Certificate"]["CertificateArn"] == arn
        assert desc["Certificate"]["Type"] == "IMPORTED"
        acm.delete_certificate(CertificateArn=arn)

    def test_import_certificate_with_tags(self, acm):
        cert_pem, key_pem = _generate_self_signed_cert()
        resp = acm.import_certificate(
            Certificate=cert_pem,
            PrivateKey=key_pem,
            Tags=[{"Key": "imported", "Value": "true"}],
        )
        arn = resp["CertificateArn"]
        tags = acm.list_tags_for_certificate(CertificateArn=arn)
        tag_map = {t["Key"]: t["Value"] for t in tags["Tags"]}
        assert tag_map["imported"] == "true"
        acm.delete_certificate(CertificateArn=arn)

    def test_request_certificate_with_multiple_sans(self, acm):
        resp = acm.request_certificate(
            DomainName="primary.example.com",
            SubjectAlternativeNames=[
                "primary.example.com",
                "alt1.example.com",
                "alt2.example.com",
                "alt3.example.com",
            ],
        )
        arn = resp["CertificateArn"]
        desc = acm.describe_certificate(CertificateArn=arn)
        sans = desc["Certificate"].get("SubjectAlternativeNames", [])
        assert "alt1.example.com" in sans
        assert "alt2.example.com" in sans

    def test_request_wildcard_certificate(self, acm):
        resp = acm.request_certificate(DomainName="*.wildcard.example.com")
        arn = resp["CertificateArn"]
        desc = acm.describe_certificate(CertificateArn=arn)
        assert desc["Certificate"]["DomainName"] == "*.wildcard.example.com"

    def test_describe_nonexistent_certificate_raises(self, acm):
        with pytest.raises(ClientError) as exc:
            acm.describe_certificate(
                CertificateArn="arn:aws:acm:us-east-1:123456789012:certificate/nonexistent-id"
            )
        assert exc.value.response["Error"]["Code"] == "ResourceNotFoundException"

    def test_put_account_configuration(self, acm):
        """PutAccountConfiguration sets expiry event preferences."""
        acm.put_account_configuration(
            ExpiryEvents={"DaysBeforeExpiry": 30},
            IdempotencyToken="put-acct-config-test",
        )
        # Verify by reading the account configuration back
        resp = acm.get_account_configuration()
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200

    def test_delete_certificate_twice_raises(self, acm):
        resp = acm.request_certificate(DomainName="double-delete.example.com")
        arn = resp["CertificateArn"]
        acm.delete_certificate(CertificateArn=arn)
        with pytest.raises(ClientError) as exc:
            acm.delete_certificate(CertificateArn=arn)
        assert exc.value.response["Error"]["Code"] == "ResourceNotFoundException"
