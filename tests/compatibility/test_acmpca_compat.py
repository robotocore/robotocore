"""ACM Private CA compatibility tests."""

import uuid

import pytest
from botocore.exceptions import ClientError

from tests.compatibility.conftest import make_client


@pytest.fixture
def acmpca():
    return make_client("acm-pca")


@pytest.fixture
def certificate_authority(acmpca):
    """Create a certificate authority and delete it after the test."""
    unique = uuid.uuid4().hex[:8]
    resp = acmpca.create_certificate_authority(
        CertificateAuthorityConfiguration={
            "KeyAlgorithm": "RSA_2048",
            "SigningAlgorithm": "SHA256WITHRSA",
            "Subject": {
                "CommonName": f"test-ca-{unique}.example.com",
                "Organization": "RobotocoreTest",
                "Country": "US",
            },
        },
        CertificateAuthorityType="ROOT",
    )
    arn = resp["CertificateAuthorityArn"]
    yield arn
    try:
        acmpca.update_certificate_authority(CertificateAuthorityArn=arn, Status="DISABLED")
    except Exception:
        pass  # best-effort cleanup
    try:
        acmpca.delete_certificate_authority(
            CertificateAuthorityArn=arn, PermanentDeletionTimeInDays=7
        )
    except Exception:
        pass  # best-effort cleanup


class TestACMPCAOperations:
    def test_list_certificate_authorities_empty(self, acmpca):
        """ListCertificateAuthorities returns a list."""
        resp = acmpca.list_certificate_authorities()
        assert "CertificateAuthorities" in resp
        assert isinstance(resp["CertificateAuthorities"], list)

    def test_create_certificate_authority(self, acmpca):
        """CreateCertificateAuthority returns an ARN."""
        unique = uuid.uuid4().hex[:8]
        resp = acmpca.create_certificate_authority(
            CertificateAuthorityConfiguration={
                "KeyAlgorithm": "RSA_2048",
                "SigningAlgorithm": "SHA256WITHRSA",
                "Subject": {
                    "CommonName": f"create-test-{unique}.example.com",
                    "Organization": "RobotocoreTest",
                    "Country": "US",
                },
            },
            CertificateAuthorityType="ROOT",
        )
        arn = resp["CertificateAuthorityArn"]
        assert arn.startswith("arn:aws:acm-pca:")
        # Cleanup
        try:
            acmpca.update_certificate_authority(CertificateAuthorityArn=arn, Status="DISABLED")
        except Exception:
            pass  # best-effort cleanup
        acmpca.delete_certificate_authority(
            CertificateAuthorityArn=arn, PermanentDeletionTimeInDays=7
        )

    def test_describe_certificate_authority(self, acmpca, certificate_authority):
        """DescribeCertificateAuthority returns the CA details."""
        resp = acmpca.describe_certificate_authority(CertificateAuthorityArn=certificate_authority)
        ca = resp["CertificateAuthority"]
        assert ca["Arn"] == certificate_authority
        assert "Status" in ca
        assert "Type" in ca
        assert ca["Type"] == "ROOT"
        assert "CertificateAuthorityConfiguration" in ca

    def test_describe_certificate_authority_config_fields(self, acmpca, certificate_authority):
        """DescribeCertificateAuthority returns configuration details."""
        resp = acmpca.describe_certificate_authority(CertificateAuthorityArn=certificate_authority)
        ca = resp["CertificateAuthority"]
        config = ca["CertificateAuthorityConfiguration"]
        assert config["KeyAlgorithm"] == "RSA_2048"
        assert config["SigningAlgorithm"] == "SHA256WITHRSA"
        assert "Subject" in config

    def test_list_certificate_authorities_includes_created(self, acmpca, certificate_authority):
        """ListCertificateAuthorities includes the created CA."""
        resp = acmpca.list_certificate_authorities()
        arns = [ca["Arn"] for ca in resp["CertificateAuthorities"]]
        assert certificate_authority in arns

    def test_tag_certificate_authority(self, acmpca, certificate_authority):
        """TagCertificateAuthority adds tags to the CA."""
        acmpca.tag_certificate_authority(
            CertificateAuthorityArn=certificate_authority,
            Tags=[
                {"Key": "Environment", "Value": "test"},
                {"Key": "Project", "Value": "robotocore"},
            ],
        )
        resp = acmpca.list_tags(CertificateAuthorityArn=certificate_authority)
        tag_map = {t["Key"]: t["Value"] for t in resp["Tags"]}
        assert tag_map["Environment"] == "test"
        assert tag_map["Project"] == "robotocore"

    def test_list_tags_empty(self, acmpca, certificate_authority):
        """ListTags on a new CA returns an empty or absent Tags list."""
        resp = acmpca.list_tags(CertificateAuthorityArn=certificate_authority)
        tags = resp.get("Tags", [])
        assert isinstance(tags, list)

    def test_untag_certificate_authority(self, acmpca, certificate_authority):
        """UntagCertificateAuthority removes tags from the CA."""
        acmpca.tag_certificate_authority(
            CertificateAuthorityArn=certificate_authority,
            Tags=[
                {"Key": "keep", "Value": "yes"},
                {"Key": "remove", "Value": "yes"},
            ],
        )
        acmpca.untag_certificate_authority(
            CertificateAuthorityArn=certificate_authority,
            Tags=[{"Key": "remove", "Value": "yes"}],
        )
        resp = acmpca.list_tags(CertificateAuthorityArn=certificate_authority)
        keys = [t["Key"] for t in resp.get("Tags", [])]
        assert "keep" in keys
        assert "remove" not in keys

    def test_update_certificate_authority_disable(self, acmpca, certificate_authority):
        """UpdateCertificateAuthority can disable a CA."""
        acmpca.update_certificate_authority(
            CertificateAuthorityArn=certificate_authority, Status="DISABLED"
        )
        resp = acmpca.describe_certificate_authority(CertificateAuthorityArn=certificate_authority)
        assert resp["CertificateAuthority"]["Status"] == "DISABLED"

    def test_delete_certificate_authority(self, acmpca):
        """DeleteCertificateAuthority removes the CA."""
        unique = uuid.uuid4().hex[:8]
        resp = acmpca.create_certificate_authority(
            CertificateAuthorityConfiguration={
                "KeyAlgorithm": "RSA_2048",
                "SigningAlgorithm": "SHA256WITHRSA",
                "Subject": {
                    "CommonName": f"delete-test-{unique}.example.com",
                    "Organization": "RobotocoreTest",
                    "Country": "US",
                },
            },
            CertificateAuthorityType="ROOT",
        )
        arn = resp["CertificateAuthorityArn"]
        # Disable before deleting
        try:
            acmpca.update_certificate_authority(CertificateAuthorityArn=arn, Status="DISABLED")
        except Exception:
            pass  # best-effort cleanup
        acmpca.delete_certificate_authority(
            CertificateAuthorityArn=arn, PermanentDeletionTimeInDays=7
        )
        # After deletion, the CA should be in DELETED status or absent from list
        resp = acmpca.describe_certificate_authority(CertificateAuthorityArn=arn)
        assert resp["CertificateAuthority"]["Status"] == "DELETED"

    def test_tag_multiple_then_list(self, acmpca, certificate_authority):
        """Tag a CA with multiple tags and verify all are returned."""
        tags = [
            {"Key": "team", "Value": "platform"},
            {"Key": "cost-center", "Value": "12345"},
            {"Key": "owner", "Value": "test-user"},
        ]
        acmpca.tag_certificate_authority(CertificateAuthorityArn=certificate_authority, Tags=tags)
        resp = acmpca.list_tags(CertificateAuthorityArn=certificate_authority)
        tag_map = {t["Key"]: t["Value"] for t in resp["Tags"]}
        assert tag_map["team"] == "platform"
        assert tag_map["cost-center"] == "12345"
        assert tag_map["owner"] == "test-user"

    def test_delete_policy_nonexistent(self, acmpca):
        """DeletePolicy on a non-existent CA raises ResourceNotFoundException."""
        fake_arn = (
            "arn:aws:acm-pca:us-east-1:123456789012:certificate-authority"
            "/00000000-0000-0000-0000-000000000000"
        )
        with pytest.raises(ClientError) as exc_info:
            acmpca.delete_policy(ResourceArn=fake_arn)
        assert exc_info.value.response["Error"]["Code"] == "ResourceNotFoundException"

    def test_get_certificate_nonexistent(self, acmpca):
        """GetCertificate on a non-existent CA raises ResourceNotFoundException."""
        fake_ca_arn = (
            "arn:aws:acm-pca:us-east-1:123456789012:certificate-authority"
            "/00000000-0000-0000-0000-000000000000"
        )
        fake_cert_arn = (
            "arn:aws:acm-pca:us-east-1:123456789012:certificate-authority"
            "/00000000-0000-0000-0000-000000000000/certificate/aaaabbbbcccc"
        )
        with pytest.raises(ClientError) as exc_info:
            acmpca.get_certificate(
                CertificateAuthorityArn=fake_ca_arn,
                CertificateArn=fake_cert_arn,
            )
        assert exc_info.value.response["Error"]["Code"] == "ResourceNotFoundException"

    def test_get_certificate_authority_certificate_nonexistent(self, acmpca):
        """GetCertificateAuthorityCertificate on non-existent CA raises ResourceNotFound."""
        fake_arn = (
            "arn:aws:acm-pca:us-east-1:123456789012:certificate-authority"
            "/00000000-0000-0000-0000-000000000000"
        )
        with pytest.raises(ClientError) as exc_info:
            acmpca.get_certificate_authority_certificate(CertificateAuthorityArn=fake_arn)
        assert exc_info.value.response["Error"]["Code"] == "ResourceNotFoundException"

    def test_get_certificate_authority_csr(self, acmpca, certificate_authority):
        """GetCertificateAuthorityCsr returns a CSR for a created CA."""
        resp = acmpca.get_certificate_authority_csr(CertificateAuthorityArn=certificate_authority)
        assert "Csr" in resp
        assert "BEGIN CERTIFICATE REQUEST" in resp["Csr"]

    def test_get_policy_nonexistent(self, acmpca):
        """GetPolicy on a non-existent CA raises ResourceNotFoundException."""
        fake_arn = (
            "arn:aws:acm-pca:us-east-1:123456789012:certificate-authority"
            "/00000000-0000-0000-0000-000000000000"
        )
        with pytest.raises(ClientError) as exc_info:
            acmpca.get_policy(ResourceArn=fake_arn)
        assert exc_info.value.response["Error"]["Code"] == "ResourceNotFoundException"

    def test_put_policy_nonexistent_ca(self, acmpca):
        """PutPolicy on a non-existent CA raises ResourceNotFoundException."""
        import json

        fake_arn = (
            "arn:aws:acm-pca:us-east-1:123456789012:certificate-authority"
            "/00000000-0000-0000-0000-000000000000"
        )
        policy = json.dumps(
            {
                "Version": "2012-10-17",
                "Statement": [
                    {
                        "Effect": "Allow",
                        "Principal": {"AWS": "arn:aws:iam::123456789012:root"},
                        "Action": ["acm-pca:DescribeCertificateAuthority"],
                        "Resource": fake_arn,
                    }
                ],
            }
        )
        with pytest.raises(ClientError) as exc_info:
            acmpca.put_policy(ResourceArn=fake_arn, Policy=policy)
        assert exc_info.value.response["Error"]["Code"] == "ResourceNotFoundException"

    def test_issue_certificate_nonexistent_ca(self, acmpca):
        """IssueCertificate on a non-existent CA raises ResourceNotFoundException."""
        fake_arn = (
            "arn:aws:acm-pca:us-east-1:123456789012:certificate-authority"
            "/00000000-0000-0000-0000-000000000000"
        )
        with pytest.raises(ClientError) as exc_info:
            acmpca.issue_certificate(
                CertificateAuthorityArn=fake_arn,
                Csr=b"fake-csr",
                SigningAlgorithm="SHA256WITHRSA",
                Validity={"Type": "DAYS", "Value": 365},
            )
        assert exc_info.value.response["Error"]["Code"] == "ResourceNotFoundException"

    def test_revoke_certificate_nonexistent_ca(self, acmpca):
        """RevokeCertificate on a non-existent CA raises ResourceNotFoundException."""
        fake_ca_arn = (
            "arn:aws:acm-pca:us-east-1:123456789012:certificate-authority"
            "/00000000-0000-0000-0000-000000000000"
        )
        with pytest.raises(ClientError) as exc_info:
            acmpca.revoke_certificate(
                CertificateAuthorityArn=fake_ca_arn,
                CertificateSerial="01",
                RevocationReason="KEY_COMPROMISE",
            )
        assert exc_info.value.response["Error"]["Code"] == "ResourceNotFoundException"

    def test_import_certificate_authority_certificate_nonexistent(self, acmpca):
        """ImportCertificateAuthorityCertificate on non-existent CA raises ResourceNotFound."""
        fake_arn = (
            "arn:aws:acm-pca:us-east-1:123456789012:certificate-authority"
            "/00000000-0000-0000-0000-000000000000"
        )
        with pytest.raises(ClientError) as exc_info:
            acmpca.import_certificate_authority_certificate(
                CertificateAuthorityArn=fake_arn,
                Certificate=b"fake-cert-pem",
            )
        assert exc_info.value.response["Error"]["Code"] == "ResourceNotFoundException"

    def test_create_and_describe_subordinate_ca(self, acmpca):
        """Create a SUBORDINATE CA and verify its type."""
        unique = uuid.uuid4().hex[:8]
        resp = acmpca.create_certificate_authority(
            CertificateAuthorityConfiguration={
                "KeyAlgorithm": "RSA_2048",
                "SigningAlgorithm": "SHA256WITHRSA",
                "Subject": {
                    "CommonName": f"sub-ca-{unique}.example.com",
                    "Organization": "RobotocoreTest",
                    "Country": "US",
                },
            },
            CertificateAuthorityType="SUBORDINATE",
        )
        arn = resp["CertificateAuthorityArn"]
        desc = acmpca.describe_certificate_authority(CertificateAuthorityArn=arn)
        assert desc["CertificateAuthority"]["Type"] == "SUBORDINATE"
        # Cleanup
        try:
            acmpca.update_certificate_authority(CertificateAuthorityArn=arn, Status="DISABLED")
        except Exception:
            pass  # best-effort cleanup
        acmpca.delete_certificate_authority(
            CertificateAuthorityArn=arn, PermanentDeletionTimeInDays=7
        )

    def test_list_permissions_empty(self, acmpca, certificate_authority):
        """ListPermissions on a new CA returns an empty list."""
        resp = acmpca.list_permissions(CertificateAuthorityArn=certificate_authority)
        assert "Permissions" in resp
        assert isinstance(resp["Permissions"], list)
        assert len(resp["Permissions"]) == 0

    def test_create_permission(self, acmpca, certificate_authority):
        """CreatePermission grants a principal access to the CA."""
        acmpca.create_permission(
            CertificateAuthorityArn=certificate_authority,
            Principal="acm.amazonaws.com",
            SourceAccount="123456789012",
            Actions=["IssueCertificate", "GetCertificate", "ListPermissions"],
        )
        resp = acmpca.list_permissions(CertificateAuthorityArn=certificate_authority)
        assert len(resp["Permissions"]) == 1
        perm = resp["Permissions"][0]
        assert perm["Principal"] == "acm.amazonaws.com"
        assert "IssueCertificate" in perm["Actions"]

    def test_delete_permission(self, acmpca, certificate_authority):
        """DeletePermission removes the permission for a principal."""
        acmpca.create_permission(
            CertificateAuthorityArn=certificate_authority,
            Principal="acm.amazonaws.com",
            SourceAccount="123456789012",
            Actions=["IssueCertificate"],
        )
        acmpca.delete_permission(
            CertificateAuthorityArn=certificate_authority,
            Principal="acm.amazonaws.com",
        )
        resp = acmpca.list_permissions(CertificateAuthorityArn=certificate_authority)
        assert len(resp["Permissions"]) == 0

    def test_list_permissions_after_multiple_creates(self, acmpca, certificate_authority):
        """ListPermissions returns all created permissions."""
        acmpca.create_permission(
            CertificateAuthorityArn=certificate_authority,
            Principal="acm.amazonaws.com",
            SourceAccount="123456789012",
            Actions=["IssueCertificate", "GetCertificate"],
        )
        resp = acmpca.list_permissions(CertificateAuthorityArn=certificate_authority)
        principals = [p["Principal"] for p in resp["Permissions"]]
        assert "acm.amazonaws.com" in principals

    def test_restore_certificate_authority(self, acmpca):
        """RestoreCertificateAuthority restores a deleted CA to DISABLED status."""
        unique = uuid.uuid4().hex[:8]
        resp = acmpca.create_certificate_authority(
            CertificateAuthorityConfiguration={
                "KeyAlgorithm": "RSA_2048",
                "SigningAlgorithm": "SHA256WITHRSA",
                "Subject": {
                    "CommonName": f"restore-test-{unique}.example.com",
                    "Organization": "RobotocoreTest",
                    "Country": "US",
                },
            },
            CertificateAuthorityType="ROOT",
        )
        arn = resp["CertificateAuthorityArn"]
        # Disable and delete
        try:
            acmpca.update_certificate_authority(CertificateAuthorityArn=arn, Status="DISABLED")
        except Exception:
            pass  # best-effort
        acmpca.delete_certificate_authority(
            CertificateAuthorityArn=arn, PermanentDeletionTimeInDays=7
        )
        desc = acmpca.describe_certificate_authority(CertificateAuthorityArn=arn)
        assert desc["CertificateAuthority"]["Status"] == "DELETED"

        # Restore
        acmpca.restore_certificate_authority(CertificateAuthorityArn=arn)
        desc = acmpca.describe_certificate_authority(CertificateAuthorityArn=arn)
        assert desc["CertificateAuthority"]["Status"] == "DISABLED"
        # Cleanup
        acmpca.delete_certificate_authority(
            CertificateAuthorityArn=arn, PermanentDeletionTimeInDays=7
        )

    def test_restore_active_ca_fails(self, acmpca, certificate_authority):
        """RestoreCertificateAuthority on a non-DELETED CA raises InvalidStateException."""
        with pytest.raises(Exception) as exc_info:
            acmpca.restore_certificate_authority(CertificateAuthorityArn=certificate_authority)
        assert exc_info.value.response["Error"]["Code"] == "InvalidStateException"

    def test_create_audit_report_non_active_ca_fails(self, acmpca, certificate_authority):
        """CreateCertificateAuthorityAuditReport requires ACTIVE CA."""
        with pytest.raises(Exception) as exc_info:
            acmpca.create_certificate_authority_audit_report(
                CertificateAuthorityArn=certificate_authority,
                S3BucketName="test-bucket",
                AuditReportResponseFormat="JSON",
            )
        assert exc_info.value.response["Error"]["Code"] == "InvalidStateException"

    def test_list_permissions_nonexistent_ca(self, acmpca):
        """ListPermissions on a non-existent CA raises ResourceNotFoundException."""
        fake_arn = (
            "arn:aws:acm-pca:us-east-1:123456789012:certificate-authority"
            "/00000000-0000-0000-0000-000000000000"
        )
        with pytest.raises(Exception) as exc_info:
            acmpca.list_permissions(CertificateAuthorityArn=fake_arn)
        assert exc_info.value.response["Error"]["Code"] == "ResourceNotFoundException"

    def test_describe_certificate_authority_audit_report_nonexistent_ca(self, acmpca):
        """DescribeCertificateAuthorityAuditReport raises ResourceNotFoundException."""
        fake_arn = (
            "arn:aws:acm-pca:us-east-1:123456789012:certificate-authority"
            "/00000000-0000-0000-0000-000000000000"
        )
        with pytest.raises(Exception) as exc_info:
            acmpca.describe_certificate_authority_audit_report(
                CertificateAuthorityArn=fake_arn,
                AuditReportId="12345678-1234-1234-1234-123456789012",
            )
        assert exc_info.value.response["Error"]["Code"] == "ResourceNotFoundException"
