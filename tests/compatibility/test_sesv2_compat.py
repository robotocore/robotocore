"""SESv2 compatibility tests."""

import uuid

import pytest

from tests.compatibility.conftest import make_client


@pytest.fixture
def sesv2():
    return make_client("sesv2")


def _uid(prefix="test"):
    return f"{prefix}-{uuid.uuid4().hex[:8]}"


class TestSESv2ListOperations:
    def test_list_email_identities(self, sesv2):
        response = sesv2.list_email_identities()
        assert "EmailIdentities" in response
        assert isinstance(response["EmailIdentities"], list)

    def test_list_contact_lists(self, sesv2):
        response = sesv2.list_contact_lists()
        assert "ContactLists" in response
        assert isinstance(response["ContactLists"], list)

    def test_list_dedicated_ip_pools(self, sesv2):
        response = sesv2.list_dedicated_ip_pools()
        assert "DedicatedIpPools" in response
        assert isinstance(response["DedicatedIpPools"], list)

    def test_list_email_templates(self, sesv2):
        response = sesv2.list_email_templates()
        assert "TemplatesMetadata" in response
        assert isinstance(response["TemplatesMetadata"], list)


class TestSESv2EmailIdentityCRUD:
    def test_create_and_get_email_identity(self, sesv2):
        email = f"{_uid('id')}@example.com"
        resp = sesv2.create_email_identity(EmailIdentity=email)
        assert "IdentityType" in resp
        try:
            got = sesv2.get_email_identity(EmailIdentity=email)
            assert got["IdentityType"] == "EMAIL_ADDRESS"
        finally:
            sesv2.delete_email_identity(EmailIdentity=email)

    def test_list_email_identities_after_create(self, sesv2):
        email = f"{_uid('id')}@example.com"
        sesv2.create_email_identity(EmailIdentity=email)
        try:
            resp = sesv2.list_email_identities()
            identities = [i["IdentityName"] for i in resp["EmailIdentities"]]
            assert email in identities
        finally:
            sesv2.delete_email_identity(EmailIdentity=email)

    def test_delete_email_identity(self, sesv2):
        email = f"{_uid('id')}@example.com"
        sesv2.create_email_identity(EmailIdentity=email)
        sesv2.delete_email_identity(EmailIdentity=email)
        resp = sesv2.list_email_identities()
        identities = [i["IdentityName"] for i in resp["EmailIdentities"]]
        assert email not in identities

    def test_create_domain_identity(self, sesv2):
        domain = f"{_uid('dom')}.example.com"
        resp = sesv2.create_email_identity(EmailIdentity=domain)
        assert "DkimAttributes" in resp
        try:
            got = sesv2.get_email_identity(EmailIdentity=domain)
            assert got["IdentityType"] == "DOMAIN"
        finally:
            sesv2.delete_email_identity(EmailIdentity=domain)


class TestSESv2ContactListCRUD:
    def test_create_and_delete_contact_list(self, sesv2):
        name = _uid("cl")
        sesv2.create_contact_list(ContactListName=name)
        resp = sesv2.list_contact_lists()
        names = [cl["ContactListName"] for cl in resp["ContactLists"]]
        assert name in names
        sesv2.delete_contact_list(ContactListName=name)

    def test_delete_contact_list(self, sesv2):
        name = _uid("cl")
        sesv2.create_contact_list(ContactListName=name)
        sesv2.delete_contact_list(ContactListName=name)
        resp = sesv2.list_contact_lists()
        names = [cl["ContactListName"] for cl in resp["ContactLists"]]
        assert name not in names


class TestSESv2EmailTemplateCRUD:
    def test_create_and_get_email_template(self, sesv2):
        name = _uid("tmpl")
        sesv2.create_email_template(
            TemplateName=name,
            TemplateContent={
                "Subject": "Test Subject",
                "Text": "Hello {{name}}",
                "Html": "<h1>Hello {{name}}</h1>",
            },
        )
        try:
            got = sesv2.get_email_template(TemplateName=name)
            assert got["TemplateName"] == name
            assert got["TemplateContent"]["Subject"] == "Test Subject"
        finally:
            sesv2.delete_email_template(TemplateName=name)

    def test_list_email_templates_after_create(self, sesv2):
        name = _uid("tmpl")
        sesv2.create_email_template(
            TemplateName=name,
            TemplateContent={
                "Subject": "Test",
                "Text": "Body",
                "Html": "<p>Body</p>",
            },
        )
        try:
            resp = sesv2.list_email_templates()
            names = [t["TemplateName"] for t in resp["TemplatesMetadata"]]
            assert name in names
        finally:
            sesv2.delete_email_template(TemplateName=name)

    def test_delete_email_template(self, sesv2):
        name = _uid("tmpl")
        sesv2.create_email_template(
            TemplateName=name,
            TemplateContent={
                "Subject": "Test",
                "Text": "Body",
                "Html": "<p>Body</p>",
            },
        )
        sesv2.delete_email_template(TemplateName=name)
        resp = sesv2.list_email_templates()
        names = [t["TemplateName"] for t in resp["TemplatesMetadata"]]
        assert name not in names


class TestSesv2AutoCoverage:
    """Auto-generated coverage tests for sesv2."""

    @pytest.fixture
    def client(self):
        return make_client("sesv2")

    def test_list_configuration_sets(self, client):
        """ListConfigurationSets returns a response."""
        resp = client.list_configuration_sets()
        assert "ConfigurationSets" in resp
