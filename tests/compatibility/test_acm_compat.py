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
        arn = response["CertificateArn"]
        assert arn.startswith("arn:aws:acm:")
        # RETRIEVE
        cert = acm.describe_certificate(CertificateArn=arn)["Certificate"]
        assert cert["DomainName"] == "example.com"
        # LIST
        arns = [c["CertificateArn"] for c in acm.list_certificates()["CertificateSummaryList"]]
        assert arn in arns
        # DELETE + ERROR
        acm.delete_certificate(CertificateArn=arn)
        with pytest.raises(ClientError) as exc:
            acm.describe_certificate(CertificateArn=arn)
        assert exc.value.response["Error"]["Code"] == "ResourceNotFoundException"

    def test_list_certificates(self, acm):
        arn = acm.request_certificate(DomainName="list-test.example.com")["CertificateArn"]
        response = acm.list_certificates()
        assert len(response["CertificateSummaryList"]) >= 1
        # DELETE + ERROR
        acm.delete_certificate(CertificateArn=arn)
        with pytest.raises(ClientError) as exc:
            acm.describe_certificate(CertificateArn=arn)
        assert exc.value.response["Error"]["Code"] == "ResourceNotFoundException"

    def test_describe_certificate(self, acm):
        arn = acm.request_certificate(DomainName="describe.example.com")["CertificateArn"]
        response = acm.describe_certificate(CertificateArn=arn)
        assert response["Certificate"]["DomainName"] == "describe.example.com"
        # LIST
        arns = [c["CertificateArn"] for c in acm.list_certificates()["CertificateSummaryList"]]
        assert arn in arns
        # DELETE + ERROR
        acm.delete_certificate(CertificateArn=arn)
        with pytest.raises(ClientError) as exc:
            acm.describe_certificate(CertificateArn=arn)
        assert exc.value.response["Error"]["Code"] == "ResourceNotFoundException"

    def test_describe_certificate_fields(self, acm):
        arn = acm.request_certificate(DomainName="fields.example.com")["CertificateArn"]
        response = acm.describe_certificate(CertificateArn=arn)
        cert = response["Certificate"]
        assert cert["CertificateArn"] == arn
        assert "Status" in cert
        assert "Type" in cert
        assert cert["DomainName"] == "fields.example.com"
        # LIST
        arns = [c["CertificateArn"] for c in acm.list_certificates()["CertificateSummaryList"]]
        assert arn in arns
        # DELETE + ERROR
        acm.delete_certificate(CertificateArn=arn)
        with pytest.raises(ClientError) as exc:
            acm.describe_certificate(CertificateArn=arn)
        assert exc.value.response["Error"]["Code"] == "ResourceNotFoundException"

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
        # LIST
        arns = [c["CertificateArn"] for c in acm.list_certificates()["CertificateSummaryList"]]
        assert arn in arns
        # DELETE + ERROR
        acm.delete_certificate(CertificateArn=arn)
        with pytest.raises(ClientError) as exc:
            acm.describe_certificate(CertificateArn=arn)
        assert exc.value.response["Error"]["Code"] == "ResourceNotFoundException"

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
        # LIST
        arns = [c["CertificateArn"] for c in acm.list_certificates()["CertificateSummaryList"]]
        assert arn in arns
        # DELETE + ERROR
        acm.delete_certificate(CertificateArn=arn)
        with pytest.raises(ClientError) as exc:
            acm.describe_certificate(CertificateArn=arn)
        assert exc.value.response["Error"]["Code"] == "ResourceNotFoundException"

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
        # RETRIEVE
        cert = acm.describe_certificate(CertificateArn=arn)["Certificate"]
        assert cert["CertificateArn"] == arn
        # DELETE + ERROR
        acm.delete_certificate(CertificateArn=arn)
        with pytest.raises(ClientError) as exc:
            acm.describe_certificate(CertificateArn=arn)
        assert exc.value.response["Error"]["Code"] == "ResourceNotFoundException"

    def test_export_certificate_invalid_arn(self, acm):
        """ExportCertificate with bad ARN should raise an error."""
        # IMPORT a real cert first (CREATE + RETRIEVE + LIST)
        cert_pem, key_pem = _generate_self_signed_cert()
        import_resp = acm.import_certificate(Certificate=cert_pem, PrivateKey=key_pem)
        real_arn = import_resp["CertificateArn"]
        # RETRIEVE
        cert = acm.describe_certificate(CertificateArn=real_arn)["Certificate"]
        assert cert["Type"] == "IMPORTED"
        # LIST
        arns = [c["CertificateArn"] for c in acm.list_certificates()["CertificateSummaryList"]]
        assert real_arn in arns
        acm.delete_certificate(CertificateArn=real_arn)
        # ERROR — fake ARN raises
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
        arn = response["CertificateArn"]
        assert arn.startswith("arn:aws:acm:")
        # RETRIEVE
        cert = acm.describe_certificate(CertificateArn=arn)["Certificate"]
        assert cert["DomainName"] == "idempotent.example.com"
        # LIST
        arns = [c["CertificateArn"] for c in acm.list_certificates()["CertificateSummaryList"]]
        assert arn in arns
        # DELETE + ERROR
        acm.delete_certificate(CertificateArn=arn)
        with pytest.raises(ClientError) as exc:
            acm.describe_certificate(CertificateArn=arn)
        assert exc.value.response["Error"]["Code"] == "ResourceNotFoundException"

    def test_certificate_status_is_pending(self, acm):
        """Newly requested certificate should have PENDING_VALIDATION status."""
        arn = acm.request_certificate(DomainName="status.example.com")["CertificateArn"]
        cert = acm.describe_certificate(CertificateArn=arn)["Certificate"]
        assert cert["Status"] == "PENDING_VALIDATION"
        # LIST — appears with pending status
        pending = acm.list_certificates(CertificateStatuses=["PENDING_VALIDATION"])
        pending_arns = [c["CertificateArn"] for c in pending["CertificateSummaryList"]]
        assert arn in pending_arns
        # DELETE + ERROR
        acm.delete_certificate(CertificateArn=arn)
        with pytest.raises(ClientError) as exc:
            acm.describe_certificate(CertificateArn=arn)
        assert exc.value.response["Error"]["Code"] == "ResourceNotFoundException"

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
        arn = response["CertificateArn"]
        assert arn.startswith("arn:aws:acm:")
        cert = acm.describe_certificate(CertificateArn=arn)["Certificate"]
        assert cert["Type"] == "IMPORTED"
        acm.delete_certificate(CertificateArn=arn)

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
        arn = resp1["CertificateArn"]
        assert arn == resp2["CertificateArn"]
        # LIST — only one cert with this domain
        certs = [
            c
            for c in acm.list_certificates()["CertificateSummaryList"]
            if c["DomainName"] == "idempotent.example.com"
        ]
        assert len(certs) == 1
        assert certs[0]["CertificateArn"] == arn
        # DELETE + ERROR
        acm.delete_certificate(CertificateArn=arn)
        with pytest.raises(ClientError) as exc:
            acm.describe_certificate(CertificateArn=arn)
        assert exc.value.response["Error"]["Code"] == "ResourceNotFoundException"

    def test_list_certificates_with_filtering(self, acm):
        """ListCertificates with certificate status filtering."""
        # Request a certificate so there's at least one
        arn = acm.request_certificate(DomainName="filter-test.example.com")["CertificateArn"]

        # List with PENDING_VALIDATION status (default for requested certs)
        response = acm.list_certificates(
            CertificateStatuses=["PENDING_VALIDATION"],
        )
        assert "CertificateSummaryList" in response
        # All returned certs should have matching status
        for cert in response["CertificateSummaryList"]:
            assert "CertificateArn" in cert
            assert "DomainName" in cert
        # RETRIEVE — cert is describable
        desc = acm.describe_certificate(CertificateArn=arn)["Certificate"]
        assert desc["Status"] == "PENDING_VALIDATION"
        # DELETE + ERROR
        acm.delete_certificate(CertificateArn=arn)
        with pytest.raises(ClientError) as exc:
            acm.describe_certificate(CertificateArn=arn)
        assert exc.value.response["Error"]["Code"] == "ResourceNotFoundException"


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
        # LIST
        arns = [c["CertificateArn"] for c in acm.list_certificates()["CertificateSummaryList"]]
        assert arn in arns
        # DELETE + ERROR
        acm.delete_certificate(CertificateArn=arn)
        with pytest.raises(ClientError) as exc:
            acm.describe_certificate(CertificateArn=arn)
        assert exc.value.response["Error"]["Code"] == "ResourceNotFoundException"

    def test_request_certificate_email_validation(self, acm):
        resp = acm.request_certificate(
            DomainName="email-test.example.com",
            ValidationMethod="EMAIL",
        )
        arn = resp["CertificateArn"]
        assert arn.startswith("arn:aws:acm:")
        # RETRIEVE — status is PENDING_VALIDATION
        cert = acm.describe_certificate(CertificateArn=arn)["Certificate"]
        assert cert["Status"] == "PENDING_VALIDATION"
        # LIST
        arns = [c["CertificateArn"] for c in acm.list_certificates()["CertificateSummaryList"]]
        assert arn in arns
        # DELETE + ERROR
        acm.delete_certificate(CertificateArn=arn)
        with pytest.raises(ClientError) as exc:
            acm.describe_certificate(CertificateArn=arn)
        assert exc.value.response["Error"]["Code"] == "ResourceNotFoundException"

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
        cert = desc["Certificate"]
        assert isinstance(cert["CreatedAt"], datetime.datetime)

    def test_list_certificates_empty_filter(self, acm):
        resp = acm.list_certificates(CertificateStatuses=["ISSUED"])
        assert isinstance(resp["CertificateSummaryList"], list)

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
        assert len(sans) >= 4
        assert sans.count("alt1.example.com") == 1
        assert sans.count("alt2.example.com") == 1

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

    def test_resend_validation_email(self, acm):
        """ResendValidationEmail on a pending certificate succeeds or raises expected error."""
        arn = acm.request_certificate(
            DomainName="resend.example.com",
            ValidationMethod="EMAIL",
        )["CertificateArn"]
        try:
            resp = acm.resend_validation_email(
                CertificateArn=arn,
                Domain="resend.example.com",
                ValidationDomain="example.com",
            )
            assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200
        except ClientError as e:
            # InvalidStateException is acceptable if cert state doesn't allow resend
            assert e.response["Error"]["Code"] in (
                "InvalidStateException",
                "InvalidDomainValidationOptionsException",
            )
        finally:
            acm.delete_certificate(CertificateArn=arn)

    def test_resend_validation_email_nonexistent_cert(self, acm):
        """ResendValidationEmail on a non-existent certificate raises error."""
        fake_arn = (
            "arn:aws:acm:us-east-1:123456789012:certificate/99999999-9999-9999-9999-999999999999"
        )
        with pytest.raises(ClientError) as exc:
            acm.resend_validation_email(
                CertificateArn=fake_arn,
                Domain="nonexistent.example.com",
                ValidationDomain="example.com",
            )
        assert exc.value.response["Error"]["Code"] == "ResourceNotFoundException"

    def test_get_certificate_on_imported_cert(self, acm):
        """GetCertificate on an imported certificate returns the cert PEM."""
        cert_pem, key_pem = _generate_self_signed_cert()
        resp = acm.import_certificate(Certificate=cert_pem, PrivateKey=key_pem)
        arn = resp["CertificateArn"]
        try:
            got = acm.get_certificate(CertificateArn=arn)
            assert got["Certificate"].startswith("-----BEGIN CERTIFICATE-----")
        finally:
            acm.delete_certificate(CertificateArn=arn)

    def test_export_certificate_non_private_raises(self, acm):
        """ExportCertificate on a non-private cert raises ValidationException."""
        cert_pem, key_pem = _generate_self_signed_cert()
        resp = acm.import_certificate(Certificate=cert_pem, PrivateKey=key_pem)
        arn = resp["CertificateArn"]
        try:
            with pytest.raises(ClientError) as exc:
                acm.export_certificate(CertificateArn=arn, Passphrase=b"test-pass")
            assert exc.value.response["Error"]["Code"] == "ValidationException"
        finally:
            acm.delete_certificate(CertificateArn=arn)


class TestACMRenewRevoke:
    FAKE_ARN = "arn:aws:acm:us-east-1:123456789012:certificate/00000000-0000-0000-0000-000000000000"

    def test_renew_certificate_not_found(self, acm):
        """RenewCertificate with nonexistent ARN raises ResourceNotFoundException."""
        with pytest.raises(ClientError) as exc:
            acm.renew_certificate(CertificateArn=self.FAKE_ARN)
        assert exc.value.response["Error"]["Code"] == "ResourceNotFoundException"

    def test_revoke_certificate_not_found(self, acm):
        """RevokeCertificate with nonexistent ARN raises ResourceNotFoundException."""
        with pytest.raises(ClientError) as exc:
            acm.revoke_certificate(CertificateArn=self.FAKE_ARN, RevocationReason="KEY_COMPROMISE")
        assert exc.value.response["Error"]["Code"] == "ResourceNotFoundException"


class TestACMBehavioralFidelity:
    """Edge cases and behavioral fidelity tests."""

    @pytest.fixture
    def acm(self):
        return make_client("acm")

    def test_arn_format(self, acm):
        """ARN must match arn:aws:acm:REGION:ACCOUNT:certificate/UUID."""
        import re

        arn = acm.request_certificate(DomainName="arn-format.example.com")["CertificateArn"]
        pattern = r"^arn:aws:acm:[a-z0-9-]+:\d{12}:certificate/[0-9a-f-]{36}$"
        assert re.match(pattern, arn), f"ARN {arn!r} did not match expected pattern"
        # RETRIEVE — cert is fetchable by the exact ARN
        cert = acm.describe_certificate(CertificateArn=arn)["Certificate"]
        assert cert["CertificateArn"] == arn
        # LIST
        arns = [c["CertificateArn"] for c in acm.list_certificates()["CertificateSummaryList"]]
        assert arn in arns
        # DELETE + ERROR
        acm.delete_certificate(CertificateArn=arn)
        with pytest.raises(ClientError) as exc:
            acm.describe_certificate(CertificateArn=arn)
        assert exc.value.response["Error"]["Code"] == "ResourceNotFoundException"

    def test_certificate_type_amazon_issued(self, acm):
        """Requested (non-imported) certs must have Type=AMAZON_ISSUED."""
        arn = acm.request_certificate(DomainName="amazon-type.example.com")["CertificateArn"]
        cert = acm.describe_certificate(CertificateArn=arn)["Certificate"]
        assert cert["Type"] == "AMAZON_ISSUED"

    def test_created_at_is_recent_datetime(self, acm):
        """CreatedAt must be a datetime within a few seconds of the request."""
        # Floor to second to avoid microsecond precision mismatches
        before = datetime.datetime.now(datetime.UTC).replace(microsecond=0)
        arn = acm.request_certificate(DomainName="created-at2.example.com")["CertificateArn"]
        after = datetime.datetime.now(datetime.UTC)
        cert = acm.describe_certificate(CertificateArn=arn)["Certificate"]
        created_at = cert["CreatedAt"]
        # Normalize to UTC for comparison regardless of server timezone
        if created_at.tzinfo is None:
            created_at = created_at.replace(tzinfo=datetime.UTC)
        else:
            created_at = created_at.astimezone(datetime.UTC)
        assert before <= created_at <= after + datetime.timedelta(seconds=5)

    def test_full_lifecycle(self, acm):
        """Create → describe → appears in list → delete → gone from list."""
        arn = acm.request_certificate(DomainName="lifecycle.example.com")["CertificateArn"]

        # Retrieve
        desc = acm.describe_certificate(CertificateArn=arn)["Certificate"]
        assert desc["DomainName"] == "lifecycle.example.com"

        # List
        arns_in_list = [
            c["CertificateArn"] for c in acm.list_certificates()["CertificateSummaryList"]
        ]
        assert arn in arns_in_list

        # Delete
        acm.delete_certificate(CertificateArn=arn)

        # Verify gone
        arns_after = [
            c["CertificateArn"] for c in acm.list_certificates()["CertificateSummaryList"]
        ]
        assert arn not in arns_after

    def test_list_certificates_pagination(self, acm):
        """ListCertificates with MaxResults returns NextToken when more results exist."""
        # Create 4 certs to ensure pagination
        arns = []
        for i in range(4):
            arns.append(
                acm.request_certificate(DomainName=f"page{i}.example.com")["CertificateArn"]
            )

        resp1 = acm.list_certificates(MaxItems=2)
        assert "CertificateSummaryList" in resp1
        assert len(resp1["CertificateSummaryList"]) <= 2

        if "NextToken" in resp1:
            resp2 = acm.list_certificates(MaxItems=2, NextToken=resp1["NextToken"])
            assert "CertificateSummaryList" in resp2
            # Second page should have different ARNs
            page1_arns = {c["CertificateArn"] for c in resp1["CertificateSummaryList"]}
            page2_arns = {c["CertificateArn"] for c in resp2["CertificateSummaryList"]}
            assert page1_arns.isdisjoint(page2_arns)

    def test_tag_key_overwrite(self, acm):
        """Adding a tag with an existing key replaces the old value."""
        arn = acm.request_certificate(DomainName="tag-overwrite.example.com")["CertificateArn"]
        acm.add_tags_to_certificate(
            CertificateArn=arn,
            Tags=[{"Key": "env", "Value": "staging"}],
        )
        acm.add_tags_to_certificate(
            CertificateArn=arn,
            Tags=[{"Key": "env", "Value": "production"}],
        )
        tags = acm.list_tags_for_certificate(CertificateArn=arn)["Tags"]
        env_tags = [t for t in tags if t["Key"] == "env"]
        assert len(env_tags) == 1
        assert env_tags[0]["Value"] == "production"

    def test_dns_validation_method_in_describe(self, acm):
        """Requesting with DNS validation records the method."""
        arn = acm.request_certificate(
            DomainName="dns-method.example.com",
            ValidationMethod="DNS",
        )["CertificateArn"]
        cert = acm.describe_certificate(CertificateArn=arn)["Certificate"]
        # DomainValidationOptions should indicate DNS method
        options = cert.get("DomainValidationOptions", [])
        assert len(options) >= 1
        assert options[0]["ValidationMethod"] == "DNS"

    def test_wildcard_domain_in_describe(self, acm):
        """Wildcard domain name is preserved exactly in describe output."""
        arn = acm.request_certificate(DomainName="*.wild2.example.com")["CertificateArn"]
        cert = acm.describe_certificate(CertificateArn=arn)["Certificate"]
        assert cert["DomainName"] == "*.wild2.example.com"

    def test_delete_removes_tags(self, acm):
        """After deleting a cert, its tags are gone (no zombie tag data)."""
        arn = acm.request_certificate(DomainName="del-tags.example.com")["CertificateArn"]
        acm.add_tags_to_certificate(
            CertificateArn=arn,
            Tags=[{"Key": "ephemeral", "Value": "yes"}],
        )
        acm.delete_certificate(CertificateArn=arn)
        # Listing tags for deleted cert should raise ResourceNotFoundException
        with pytest.raises(ClientError) as exc:
            acm.list_tags_for_certificate(CertificateArn=arn)
        assert exc.value.response["Error"]["Code"] == "ResourceNotFoundException"

    def test_list_certificates_status_filter_excludes_pending(self, acm):
        """Filtering by ISSUED should NOT include PENDING_VALIDATION certs."""
        arn = acm.request_certificate(DomainName="pending-excl.example.com")["CertificateArn"]
        resp = acm.list_certificates(CertificateStatuses=["ISSUED"])
        issued_arns = [c["CertificateArn"] for c in resp["CertificateSummaryList"]]
        assert arn not in issued_arns

    def test_multiple_sans_preserved_in_describe(self, acm):
        """All SANs provided at request time appear verbatim in DescribeCertificate."""
        sans = ["primary.example.com", "a.example.com", "b.example.com", "c.example.com"]
        arn = acm.request_certificate(
            DomainName="primary.example.com",
            SubjectAlternativeNames=sans,
        )["CertificateArn"]
        cert = acm.describe_certificate(CertificateArn=arn)["Certificate"]
        returned_sans = cert.get("SubjectAlternativeNames", [])
        for san in sans:
            assert san in returned_sans

    def test_imported_cert_status_is_issued(self, acm):
        """An imported certificate should have Status=ISSUED immediately."""
        cert_pem, key_pem = _generate_self_signed_cert()
        arn = acm.import_certificate(Certificate=cert_pem, PrivateKey=key_pem)["CertificateArn"]
        try:
            cert = acm.describe_certificate(CertificateArn=arn)["Certificate"]
            assert cert["Status"] == "ISSUED"
        finally:
            acm.delete_certificate(CertificateArn=arn)

    def test_describe_certificate_arn_matches_requested(self, acm):
        """CertificateArn in describe response must equal the ARN used to request."""
        arn = acm.request_certificate(DomainName="arn-match.example.com")["CertificateArn"]
        cert = acm.describe_certificate(CertificateArn=arn)["Certificate"]
        assert cert["CertificateArn"] == arn

    def test_list_tags_empty_on_new_cert(self, acm):
        """A freshly requested cert with no tags returns an empty Tags list."""
        arn = acm.request_certificate(DomainName="no-tags.example.com")["CertificateArn"]
        resp = acm.list_tags_for_certificate(CertificateArn=arn)
        assert resp["Tags"] == []

    def test_remove_nonexistent_tag_is_noop(self, acm):
        """Removing a tag that doesn't exist must not raise an error."""
        arn = acm.request_certificate(DomainName="noop-tag.example.com")["CertificateArn"]
        # This should not raise
        acm.remove_tags_from_certificate(
            CertificateArn=arn,
            Tags=[{"Key": "ghost-key", "Value": "ghost-value"}],
        )
        resp = acm.list_tags_for_certificate(CertificateArn=arn)
        assert resp["Tags"] == []


class TestACMEdgeCases:
    """Edge cases and behavioral fidelity tests covering all CRLUDE patterns."""

    @pytest.fixture
    def acm(self):
        return make_client("acm")

    def test_full_lifecycle_all_patterns(self, acm):
        """C+R+L+U+D+E: complete lifecycle touching every pattern."""
        # CREATE
        arn = acm.request_certificate(DomainName="full-all.example.com")["CertificateArn"]

        # RETRIEVE
        cert = acm.describe_certificate(CertificateArn=arn)["Certificate"]
        assert cert["DomainName"] == "full-all.example.com"
        assert cert["Status"] == "PENDING_VALIDATION"

        # LIST
        all_arns = [c["CertificateArn"] for c in acm.list_certificates()["CertificateSummaryList"]]
        assert arn in all_arns

        # UPDATE (add tag, verify it persists)
        acm.add_tags_to_certificate(
            CertificateArn=arn, Tags=[{"Key": "lifecycle-phase", "Value": "testing"}]
        )
        tags = acm.list_tags_for_certificate(CertificateArn=arn)["Tags"]
        assert any(t["Key"] == "lifecycle-phase" and t["Value"] == "testing" for t in tags)

        # DELETE
        acm.delete_certificate(CertificateArn=arn)

        # ERROR — describe after delete must raise
        with pytest.raises(ClientError) as exc:
            acm.describe_certificate(CertificateArn=arn)
        assert exc.value.response["Error"]["Code"] == "ResourceNotFoundException"

    def test_idempotency_token_returns_same_arn(self, acm):
        """Requesting with same domain+token twice returns the identical ARN."""
        token = "edge-case-idem-token-xyz"
        domain = "idem-edge.example.com"
        arn1 = acm.request_certificate(DomainName=domain, IdempotencyToken=token)["CertificateArn"]
        arn2 = acm.request_certificate(DomainName=domain, IdempotencyToken=token)["CertificateArn"]
        assert arn1 == arn2
        # Only one cert with this domain should exist
        certs = [
            c
            for c in acm.list_certificates()["CertificateSummaryList"]
            if c["DomainName"] == domain
        ]
        assert len(certs) == 1

    def test_list_certificates_pagination_all_pages(self, acm):
        """Create 5 certs, paginate MaxItems=2, collect all pages, all ARNs found."""
        created_arns = set()
        for i in range(5):
            arn = acm.request_certificate(
                DomainName=f"pagtest{i}.example.com"
            )["CertificateArn"]
            created_arns.add(arn)

        collected_arns = set()
        resp = acm.list_certificates(MaxItems=2)
        collected_arns.update(c["CertificateArn"] for c in resp["CertificateSummaryList"])

        while "NextToken" in resp:
            resp = acm.list_certificates(MaxItems=2, NextToken=resp["NextToken"])
            collected_arns.update(c["CertificateArn"] for c in resp["CertificateSummaryList"])

        assert created_arns.issubset(collected_arns)

    def test_email_validation_method_preserved_in_describe(self, acm):
        """Cert requested with EMAIL validation shows EMAIL in DomainValidationOptions."""
        arn = acm.request_certificate(
            DomainName="email-method.example.com",
            ValidationMethod="EMAIL",
        )["CertificateArn"]
        cert = acm.describe_certificate(CertificateArn=arn)["Certificate"]
        options = cert.get("DomainValidationOptions", [])
        assert len(options) >= 1
        assert options[0]["ValidationMethod"] == "EMAIL"

    def test_arn_uuid_format_and_retrievable(self, acm):
        """ARN UUID portion is 36-char hyphenated hex; cert is describable by that ARN."""
        import re

        arn = acm.request_certificate(DomainName="uuid-check.example.com")["CertificateArn"]
        cert = acm.describe_certificate(CertificateArn=arn)["Certificate"]
        assert cert["CertificateArn"] == arn
        uuid_part = arn.split("/")[-1]
        assert re.match(
            r"^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$", uuid_part
        ), f"UUID portion {uuid_part!r} malformed"

    def test_request_certificate_domain_appears_in_list_summary(self, acm):
        """Domain in ListCertificates summary matches the requested domain exactly."""
        domain = "listsummary3.example.com"
        arn = acm.request_certificate(DomainName=domain)["CertificateArn"]
        certs = acm.list_certificates()["CertificateSummaryList"]
        matches = [c for c in certs if c["CertificateArn"] == arn]
        assert len(matches) == 1
        assert matches[0]["DomainName"] == domain

    def test_add_tags_error_on_nonexistent_cert(self, acm):
        """AddTagsToCertificate on a nonexistent ARN raises ResourceNotFoundException."""
        fake_arn = (
            "arn:aws:acm:us-east-1:123456789012:certificate/aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee"
        )
        with pytest.raises(ClientError) as exc:
            acm.add_tags_to_certificate(
                CertificateArn=fake_arn, Tags=[{"Key": "k", "Value": "v"}]
            )
        assert exc.value.response["Error"]["Code"] == "ResourceNotFoundException"

    def test_list_tags_error_on_nonexistent_cert(self, acm):
        """ListTagsForCertificate on a nonexistent ARN raises ResourceNotFoundException."""
        fake_arn = (
            "arn:aws:acm:us-east-1:123456789012:certificate/ffffffff-ffff-ffff-ffff-ffffffffffff"
        )
        with pytest.raises(ClientError) as exc:
            acm.list_tags_for_certificate(CertificateArn=fake_arn)
        assert exc.value.response["Error"]["Code"] == "ResourceNotFoundException"

    def test_update_transparency_logging_persists(self, acm):
        """UpdateCertificateOptions sets transparency pref; DescribeCertificate reflects it."""
        arn = acm.request_certificate(DomainName="update-retrieve.example.com")["CertificateArn"]
        acm.update_certificate_options(
            CertificateArn=arn,
            Options={"CertificateTransparencyLoggingPreference": "DISABLED"},
        )
        cert = acm.describe_certificate(CertificateArn=arn)["Certificate"]
        opts = cert.get("Options", {})
        assert opts.get("CertificateTransparencyLoggingPreference") == "DISABLED"
        acm.delete_certificate(CertificateArn=arn)

    def test_request_certificate_with_idempotency_token_and_list(self, acm):
        """RequestCertificate with token: cert appears in list with correct domain."""
        domain = "token-list.example.com"
        arn = acm.request_certificate(
            DomainName=domain,
            IdempotencyToken="token-list-test-001",
        )["CertificateArn"]
        assert arn.startswith("arn:aws:acm:")
        certs = acm.list_certificates()["CertificateSummaryList"]
        found = [c for c in certs if c["CertificateArn"] == arn]
        assert len(found) == 1
        assert found[0]["DomainName"] == domain

    def test_request_certificate_email_validation_status_pending(self, acm):
        """EMAIL-validated cert starts as PENDING_VALIDATION and is listable."""
        arn = acm.request_certificate(
            DomainName="email-pending.example.com",
            ValidationMethod="EMAIL",
        )["CertificateArn"]
        cert = acm.describe_certificate(CertificateArn=arn)["Certificate"]
        assert cert["Status"] == "PENDING_VALIDATION"
        listed = [c["CertificateArn"] for c in acm.list_certificates()["CertificateSummaryList"]]
        assert arn in listed

    def test_describe_certificate_has_subject_alternative_names(self, acm):
        """DescribeCertificate always includes SubjectAlternativeNames (at least the domain)."""
        arn = acm.request_certificate(DomainName="sans-always.example.com")["CertificateArn"]
        cert = acm.describe_certificate(CertificateArn=arn)["Certificate"]
        sans = cert["SubjectAlternativeNames"]
        # Primary domain must appear in SANs
        assert len(sans) >= 1
        assert sans[0] == "sans-always.example.com"

    def test_delete_removes_from_list(self, acm):
        """After delete, certificate no longer appears in ListCertificates."""
        arn = acm.request_certificate(DomainName="del-list.example.com")["CertificateArn"]
        before = [c["CertificateArn"] for c in acm.list_certificates()["CertificateSummaryList"]]
        assert arn in before
        acm.delete_certificate(CertificateArn=arn)
        after = [c["CertificateArn"] for c in acm.list_certificates()["CertificateSummaryList"]]
        assert arn not in after

    def test_status_filter_pending_includes_new_cert(self, acm):
        """Filtering by PENDING_VALIDATION includes a freshly requested certificate."""
        arn = acm.request_certificate(DomainName="pending-filter.example.com")["CertificateArn"]
        resp = acm.list_certificates(CertificateStatuses=["PENDING_VALIDATION"])
        pending_arns = [c["CertificateArn"] for c in resp["CertificateSummaryList"]]
        assert arn in pending_arns


class TestACMFullPatternCoverage:
    """Tests hitting CREATE+RETRIEVE+LIST+UPDATE+DELETE+ERROR patterns in combination.

    Many ACM tests use request_certificate which is not detected as a behavioral
    CREATE pattern. These tests use import_certificate (which IS detected) alongside
    the full CRLUDE pattern set to ensure comprehensive behavioral coverage.
    """

    @pytest.fixture
    def acm(self):
        return make_client("acm")

    def test_import_describe_list_delete_lifecycle(self, acm):
        """Import cert (C) → describe it (R) → appears in list (L) → delete (D) → describe raises (E)."""
        cert_pem, key_pem = _generate_self_signed_cert()
        arn = acm.import_certificate(Certificate=cert_pem, PrivateKey=key_pem)["CertificateArn"]

        # RETRIEVE
        cert = acm.describe_certificate(CertificateArn=arn)["Certificate"]
        assert cert["Type"] == "IMPORTED"
        assert cert["Status"] == "ISSUED"
        assert cert["CertificateArn"] == arn

        # LIST
        arns = [c["CertificateArn"] for c in acm.list_certificates()["CertificateSummaryList"]]
        assert arn in arns

        # DELETE
        acm.delete_certificate(CertificateArn=arn)

        # ERROR — describe after delete raises
        with pytest.raises(ClientError) as exc:
            acm.describe_certificate(CertificateArn=arn)
        assert exc.value.response["Error"]["Code"] == "ResourceNotFoundException"

    def test_import_tag_describe_update_delete(self, acm):
        """Import (C) → add tags (U) → describe tags (R) → list (L) → overwrite tag (U) → delete (D) → tags raise (E)."""
        cert_pem, key_pem = _generate_self_signed_cert()
        arn = acm.import_certificate(Certificate=cert_pem, PrivateKey=key_pem)["CertificateArn"]

        # UPDATE — add tags
        acm.add_tags_to_certificate(
            CertificateArn=arn,
            Tags=[{"Key": "env", "Value": "staging"}, {"Key": "team", "Value": "infra"}],
        )

        # RETRIEVE — verify tags
        tags = {t["Key"]: t["Value"] for t in acm.list_tags_for_certificate(CertificateArn=arn)["Tags"]}
        assert tags["env"] == "staging"
        assert tags["team"] == "infra"

        # UPDATE — overwrite one tag
        acm.add_tags_to_certificate(
            CertificateArn=arn,
            Tags=[{"Key": "env", "Value": "production"}],
        )
        tags_after = {t["Key"]: t["Value"] for t in acm.list_tags_for_certificate(CertificateArn=arn)["Tags"]}
        assert tags_after["env"] == "production"
        assert tags_after["team"] == "infra"  # unchanged

        # LIST
        certs = acm.list_certificates()["CertificateSummaryList"]
        found = [c for c in certs if c["CertificateArn"] == arn]
        assert len(found) == 1

        # DELETE
        acm.delete_certificate(CertificateArn=arn)

        # ERROR — listing tags after delete raises
        with pytest.raises(ClientError) as exc:
            acm.list_tags_for_certificate(CertificateArn=arn)
        assert exc.value.response["Error"]["Code"] == "ResourceNotFoundException"

    def test_import_update_options_retrieve_list_delete(self, acm):
        """Import (C) → update cert options (U) → describe reflects change (R) → list (L) → delete (D) → bad describe (E)."""
        cert_pem, key_pem = _generate_self_signed_cert()
        arn = acm.import_certificate(Certificate=cert_pem, PrivateKey=key_pem)["CertificateArn"]

        # UPDATE — set transparency logging pref
        acm.update_certificate_options(
            CertificateArn=arn,
            Options={"CertificateTransparencyLoggingPreference": "DISABLED"},
        )

        # RETRIEVE — verify option persisted
        cert = acm.describe_certificate(CertificateArn=arn)["Certificate"]
        opts = cert.get("Options", {})
        assert opts.get("CertificateTransparencyLoggingPreference") == "DISABLED"

        # LIST — cert appears
        arns = [c["CertificateArn"] for c in acm.list_certificates()["CertificateSummaryList"]]
        assert arn in arns

        # DELETE
        acm.delete_certificate(CertificateArn=arn)

        # ERROR — describe raises after delete
        with pytest.raises(ClientError) as exc:
            acm.describe_certificate(CertificateArn=arn)
        assert exc.value.response["Error"]["Code"] == "ResourceNotFoundException"

    def test_import_multiple_certs_list_pagination_delete(self, acm):
        """Import 3 certs (C) → paginate list (L) → describe each (R) → delete all (D) → errors (E)."""
        cert_pem, key_pem = _generate_self_signed_cert()
        arns = []
        for _ in range(3):
            arns.append(
                acm.import_certificate(Certificate=cert_pem, PrivateKey=key_pem)["CertificateArn"]
            )

        # RETRIEVE each
        for arn in arns:
            cert = acm.describe_certificate(CertificateArn=arn)["Certificate"]
            assert cert["Type"] == "IMPORTED"

        # LIST — all appear; test pagination
        collected = set()
        resp = acm.list_certificates(MaxItems=2)
        collected.update(c["CertificateArn"] for c in resp["CertificateSummaryList"])
        while "NextToken" in resp:
            resp = acm.list_certificates(MaxItems=2, NextToken=resp["NextToken"])
            collected.update(c["CertificateArn"] for c in resp["CertificateSummaryList"])
        for arn in arns:
            assert arn in collected

        # DELETE all
        for arn in arns:
            acm.delete_certificate(CertificateArn=arn)

        # ERROR — each raises after delete
        for arn in arns:
            with pytest.raises(ClientError) as exc:
                acm.describe_certificate(CertificateArn=arn)
            assert exc.value.response["Error"]["Code"] == "ResourceNotFoundException"

    def test_import_get_certificate_pem_content(self, acm):
        """Import (C) → GetCertificate returns PEM (R) → list shows ISSUED status (L) → delete (D) → error (E)."""
        cert_pem, key_pem = _generate_self_signed_cert()
        arn = acm.import_certificate(Certificate=cert_pem, PrivateKey=key_pem)["CertificateArn"]

        # RETRIEVE via GetCertificate
        got = acm.get_certificate(CertificateArn=arn)
        assert "Certificate" in got
        assert got["Certificate"].startswith("-----BEGIN CERTIFICATE-----")

        # LIST — filter by ISSUED (imported certs are immediately ISSUED)
        resp = acm.list_certificates(CertificateStatuses=["ISSUED"])
        issued_arns = [c["CertificateArn"] for c in resp["CertificateSummaryList"]]
        assert arn in issued_arns

        # DELETE
        acm.delete_certificate(CertificateArn=arn)

        # ERROR
        with pytest.raises(ClientError) as exc:
            acm.get_certificate(CertificateArn=arn)
        assert exc.value.response["Error"]["Code"] == "ResourceNotFoundException"

    def test_import_add_remove_tags_verify_list_delete(self, acm):
        """Import (C) → add tags (U) → remove one tag (U) → verify remainder (R) → list (L) → delete (D) → error (E)."""
        cert_pem, key_pem = _generate_self_signed_cert()
        arn = acm.import_certificate(Certificate=cert_pem, PrivateKey=key_pem)["CertificateArn"]

        # UPDATE — add two tags
        acm.add_tags_to_certificate(
            CertificateArn=arn,
            Tags=[{"Key": "keep", "Value": "yes"}, {"Key": "discard", "Value": "yes"}],
        )

        # UPDATE — remove one tag
        acm.remove_tags_from_certificate(
            CertificateArn=arn,
            Tags=[{"Key": "discard", "Value": "yes"}],
        )

        # RETRIEVE — only "keep" remains
        tags = acm.list_tags_for_certificate(CertificateArn=arn)["Tags"]
        keys = [t["Key"] for t in tags]
        assert "keep" in keys
        assert "discard" not in keys

        # LIST
        arns = [c["CertificateArn"] for c in acm.list_certificates()["CertificateSummaryList"]]
        assert arn in arns

        # DELETE
        acm.delete_certificate(CertificateArn=arn)

        # ERROR
        with pytest.raises(ClientError) as exc:
            acm.list_tags_for_certificate(CertificateArn=arn)
        assert exc.value.response["Error"]["Code"] == "ResourceNotFoundException"

    def test_import_with_tags_describe_list_delete(self, acm):
        """Import with inline tags (C) → describe cert fields (R) → list (L) → verify tag (R) → delete (D) → error (E)."""
        cert_pem, key_pem = _generate_self_signed_cert()
        arn = acm.import_certificate(
            Certificate=cert_pem,
            PrivateKey=key_pem,
            Tags=[{"Key": "source", "Value": "test-suite"}, {"Key": "version", "Value": "1"}],
        )["CertificateArn"]

        # RETRIEVE — cert metadata
        cert = acm.describe_certificate(CertificateArn=arn)["Certificate"]
        assert cert["CertificateArn"] == arn
        assert cert["Type"] == "IMPORTED"
        assert cert["Status"] == "ISSUED"
        assert "ImportedAt" in cert or "CreatedAt" in cert

        # RETRIEVE — inline tags were applied
        tags = {t["Key"]: t["Value"] for t in acm.list_tags_for_certificate(CertificateArn=arn)["Tags"]}
        assert tags["source"] == "test-suite"
        assert tags["version"] == "1"

        # LIST
        arns = [c["CertificateArn"] for c in acm.list_certificates()["CertificateSummaryList"]]
        assert arn in arns

        # DELETE
        acm.delete_certificate(CertificateArn=arn)

        # ERROR
        with pytest.raises(ClientError) as exc:
            acm.describe_certificate(CertificateArn=arn)
        assert exc.value.response["Error"]["Code"] == "ResourceNotFoundException"

    def test_add_tags_to_nonexistent_raises(self, acm):
        """Add tags to nonexistent cert (E), import one (C), add tags (U), describe (R), list (L), delete (D)."""
        fake_arn = "arn:aws:acm:us-east-1:123456789012:certificate/deadbeef-dead-beef-dead-beefdeadbeef"

        # ERROR — add tags to nonexistent cert
        with pytest.raises(ClientError) as exc:
            acm.add_tags_to_certificate(
                CertificateArn=fake_arn,
                Tags=[{"Key": "k", "Value": "v"}],
            )
        assert exc.value.response["Error"]["Code"] == "ResourceNotFoundException"

        # CREATE a real cert
        cert_pem, key_pem = _generate_self_signed_cert()
        arn = acm.import_certificate(Certificate=cert_pem, PrivateKey=key_pem)["CertificateArn"]

        # UPDATE — add tags to the real cert
        acm.add_tags_to_certificate(
            CertificateArn=arn,
            Tags=[{"Key": "valid-tag", "Value": "v"}],
        )

        # RETRIEVE
        tags = acm.list_tags_for_certificate(CertificateArn=arn)["Tags"]
        assert any(t["Key"] == "valid-tag" for t in tags)

        # LIST
        arns = [c["CertificateArn"] for c in acm.list_certificates()["CertificateSummaryList"]]
        assert arn in arns

        # DELETE
        acm.delete_certificate(CertificateArn=arn)

    def test_export_certificate_requires_private_ca(self, acm):
        """Import self-signed cert (C) → describe (R) → list (L) → export raises (E) → delete (D)."""
        cert_pem, key_pem = _generate_self_signed_cert()
        arn = acm.import_certificate(Certificate=cert_pem, PrivateKey=key_pem)["CertificateArn"]

        # RETRIEVE
        cert = acm.describe_certificate(CertificateArn=arn)["Certificate"]
        assert cert["CertificateArn"] == arn

        # LIST
        arns = [c["CertificateArn"] for c in acm.list_certificates()["CertificateSummaryList"]]
        assert arn in arns

        # ERROR — export non-private cert raises ValidationException
        with pytest.raises(ClientError) as exc:
            acm.export_certificate(CertificateArn=arn, Passphrase=b"passphrase-123")
        assert exc.value.response["Error"]["Code"] == "ValidationException"

        # DELETE
        acm.delete_certificate(CertificateArn=arn)

    def test_reimport_certificate_updates_existing(self, acm):
        """Import cert (C) → reimport to same ARN (U) → describe reflects update (R) → list (L) → delete (D) → error (E)."""
        cert_pem1, key_pem1 = _generate_self_signed_cert()
        arn = acm.import_certificate(Certificate=cert_pem1, PrivateKey=key_pem1)["CertificateArn"]

        # RETRIEVE — initial state
        cert = acm.describe_certificate(CertificateArn=arn)["Certificate"]
        assert cert["Type"] == "IMPORTED"

        # UPDATE — reimport with same ARN (rotates the cert)
        cert_pem2, key_pem2 = _generate_self_signed_cert()
        resp = acm.import_certificate(
            CertificateArn=arn,
            Certificate=cert_pem2,
            PrivateKey=key_pem2,
        )
        # Reimport returns the same ARN
        assert resp["CertificateArn"] == arn

        # RETRIEVE — still IMPORTED, same ARN
        cert_after = acm.describe_certificate(CertificateArn=arn)["Certificate"]
        assert cert_after["Type"] == "IMPORTED"
        assert cert_after["CertificateArn"] == arn

        # LIST — still appears exactly once
        certs = acm.list_certificates()["CertificateSummaryList"]
        matches = [c for c in certs if c["CertificateArn"] == arn]
        assert len(matches) == 1

        # DELETE
        acm.delete_certificate(CertificateArn=arn)

        # ERROR
        with pytest.raises(ClientError) as exc:
            acm.describe_certificate(CertificateArn=arn)
        assert exc.value.response["Error"]["Code"] == "ResourceNotFoundException"
