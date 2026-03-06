"""ACM compatibility tests."""

import pytest

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
