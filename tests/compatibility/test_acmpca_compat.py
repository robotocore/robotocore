"""ACM Private CA compatibility tests."""

import uuid

import pytest

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
        acmpca.update_certificate_authority(
            CertificateAuthorityArn=arn, Status="DISABLED"
        )
    except Exception:
        pass
    try:
        acmpca.delete_certificate_authority(
            CertificateAuthorityArn=arn, PermanentDeletionTimeInDays=7
        )
    except Exception:
        pass


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
            acmpca.update_certificate_authority(
                CertificateAuthorityArn=arn, Status="DISABLED"
            )
        except Exception:
            pass
        acmpca.delete_certificate_authority(
            CertificateAuthorityArn=arn, PermanentDeletionTimeInDays=7
        )

    def test_describe_certificate_authority(self, acmpca, certificate_authority):
        """DescribeCertificateAuthority returns the CA details."""
        resp = acmpca.describe_certificate_authority(
            CertificateAuthorityArn=certificate_authority
        )
        ca = resp["CertificateAuthority"]
        assert ca["Arn"] == certificate_authority
        assert "Status" in ca
        assert "Type" in ca
        assert ca["Type"] == "ROOT"
        assert "CertificateAuthorityConfiguration" in ca

    def test_describe_certificate_authority_config_fields(self, acmpca, certificate_authority):
        """DescribeCertificateAuthority returns configuration details."""
        resp = acmpca.describe_certificate_authority(
            CertificateAuthorityArn=certificate_authority
        )
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
        resp = acmpca.describe_certificate_authority(
            CertificateAuthorityArn=certificate_authority
        )
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
            acmpca.update_certificate_authority(
                CertificateAuthorityArn=arn, Status="DISABLED"
            )
        except Exception:
            pass
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
        acmpca.tag_certificate_authority(
            CertificateAuthorityArn=certificate_authority, Tags=tags
        )
        resp = acmpca.list_tags(CertificateAuthorityArn=certificate_authority)
        tag_map = {t["Key"]: t["Value"] for t in resp["Tags"]}
        assert tag_map["team"] == "platform"
        assert tag_map["cost-center"] == "12345"
        assert tag_map["owner"] == "test-user"

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
            acmpca.update_certificate_authority(
                CertificateAuthorityArn=arn, Status="DISABLED"
            )
        except Exception:
            pass
        acmpca.delete_certificate_authority(
            CertificateAuthorityArn=arn, PermanentDeletionTimeInDays=7
        )
