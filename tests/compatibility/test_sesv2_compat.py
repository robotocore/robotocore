"""SESv2 compatibility tests."""

import uuid

import pytest
from botocore.exceptions import ClientError

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

    def test_create_configuration_set(self, client):
        """CreateConfigurationSet creates and is visible in list."""
        name = _uid("cfgset")
        client.create_configuration_set(ConfigurationSetName=name)
        try:
            resp = client.list_configuration_sets()
            assert name in resp["ConfigurationSets"]
        finally:
            client.delete_configuration_set(ConfigurationSetName=name)

    def test_get_configuration_set(self, client):
        """GetConfigurationSet returns details."""
        name = _uid("cfgset")
        client.create_configuration_set(ConfigurationSetName=name)
        try:
            resp = client.get_configuration_set(ConfigurationSetName=name)
            assert resp["ConfigurationSetName"] == name
        finally:
            client.delete_configuration_set(ConfigurationSetName=name)

    def test_delete_configuration_set(self, client):
        """DeleteConfigurationSet removes the set."""
        name = _uid("cfgset")
        client.create_configuration_set(ConfigurationSetName=name)
        client.delete_configuration_set(ConfigurationSetName=name)
        resp = client.list_configuration_sets()
        assert name not in resp["ConfigurationSets"]

    def test_create_and_get_contact(self, client):
        """CreateContact + GetContact on a contact list."""
        cl_name = _uid("cl")
        email = f"{_uid('ct')}@example.com"
        client.create_contact_list(ContactListName=cl_name)
        try:
            client.create_contact(ContactListName=cl_name, EmailAddress=email)
            resp = client.get_contact(ContactListName=cl_name, EmailAddress=email)
            assert resp["EmailAddress"] == email
            client.delete_contact(ContactListName=cl_name, EmailAddress=email)
        finally:
            client.delete_contact_list(ContactListName=cl_name)

    def test_delete_contact(self, client):
        """DeleteContact removes a contact."""
        cl_name = _uid("cl")
        email = f"{_uid('ct')}@example.com"
        client.create_contact_list(ContactListName=cl_name)
        try:
            client.create_contact(ContactListName=cl_name, EmailAddress=email)
            client.delete_contact(ContactListName=cl_name, EmailAddress=email)
            resp = client.list_contacts(ContactListName=cl_name)
            emails = [c["EmailAddress"] for c in resp["Contacts"]]
            assert email not in emails
        finally:
            client.delete_contact_list(ContactListName=cl_name)

    def test_list_contacts(self, client):
        """ListContacts returns contacts in a list."""
        cl_name = _uid("cl")
        email = f"{_uid('ct')}@example.com"
        client.create_contact_list(ContactListName=cl_name)
        try:
            client.create_contact(ContactListName=cl_name, EmailAddress=email)
            resp = client.list_contacts(ContactListName=cl_name)
            assert "Contacts" in resp
            emails = [c["EmailAddress"] for c in resp["Contacts"]]
            assert email in emails
        finally:
            client.delete_contact_list(ContactListName=cl_name)

    def test_get_contact_list(self, client):
        """GetContactList returns details."""
        cl_name = _uid("cl")
        client.create_contact_list(ContactListName=cl_name)
        try:
            resp = client.get_contact_list(ContactListName=cl_name)
            assert resp["ContactListName"] == cl_name
        finally:
            client.delete_contact_list(ContactListName=cl_name)

    def test_create_and_delete_dedicated_ip_pool(self, client):
        """CreateDedicatedIpPool + DeleteDedicatedIpPool."""
        name = _uid("pool")
        client.create_dedicated_ip_pool(PoolName=name)
        try:
            resp = client.list_dedicated_ip_pools()
            assert name in resp["DedicatedIpPools"]
        finally:
            client.delete_dedicated_ip_pool(PoolName=name)

    def test_get_dedicated_ip_pool(self, client):
        """GetDedicatedIpPool returns pool details."""
        name = _uid("pool")
        client.create_dedicated_ip_pool(PoolName=name)
        try:
            resp = client.get_dedicated_ip_pool(PoolName=name)
            assert resp["DedicatedIpPool"]["PoolName"] == name
        finally:
            client.delete_dedicated_ip_pool(PoolName=name)

    def test_create_email_identity_policy(self, client):
        """CreateEmailIdentityPolicy on a domain identity."""
        domain = f"{_uid('dom')}.example.com"
        client.create_email_identity(EmailIdentity=domain)
        policy_name = _uid("pol")
        policy_doc = (
            '{"Version":"2012-10-17","Statement":'
            '[{"Effect":"Allow","Principal":"*",'
            '"Action":"ses:SendEmail","Resource":"*"}]}'
        )
        try:
            client.create_email_identity_policy(
                EmailIdentity=domain,
                PolicyName=policy_name,
                Policy=policy_doc,
            )
            resp = client.get_email_identity_policies(EmailIdentity=domain)
            assert policy_name in resp["Policies"]
        finally:
            client.delete_email_identity(EmailIdentity=domain)

    def test_delete_email_identity_policy(self, client):
        """DeleteEmailIdentityPolicy removes a policy."""
        domain = f"{_uid('dom')}.example.com"
        client.create_email_identity(EmailIdentity=domain)
        policy_name = _uid("pol")
        policy_doc = (
            '{"Version":"2012-10-17","Statement":'
            '[{"Effect":"Allow","Principal":"*",'
            '"Action":"ses:SendEmail","Resource":"*"}]}'
        )
        try:
            client.create_email_identity_policy(
                EmailIdentity=domain,
                PolicyName=policy_name,
                Policy=policy_doc,
            )
            client.delete_email_identity_policy(EmailIdentity=domain, PolicyName=policy_name)
            resp = client.get_email_identity_policies(EmailIdentity=domain)
            assert policy_name not in resp["Policies"]
        finally:
            client.delete_email_identity(EmailIdentity=domain)

    def test_update_email_identity_policy(self, client):
        """UpdateEmailIdentityPolicy changes the policy."""
        domain = f"{_uid('dom')}.example.com"
        client.create_email_identity(EmailIdentity=domain)
        policy_name = _uid("pol")
        policy_v1 = (
            '{"Version":"2012-10-17","Statement":'
            '[{"Effect":"Allow","Principal":"*",'
            '"Action":"ses:SendEmail","Resource":"*"}]}'
        )
        policy_v2 = (
            '{"Version":"2012-10-17","Statement":'
            '[{"Effect":"Deny","Principal":"*",'
            '"Action":"ses:SendEmail","Resource":"*"}]}'
        )
        try:
            client.create_email_identity_policy(
                EmailIdentity=domain,
                PolicyName=policy_name,
                Policy=policy_v1,
            )
            client.update_email_identity_policy(
                EmailIdentity=domain,
                PolicyName=policy_name,
                Policy=policy_v2,
            )
            resp = client.get_email_identity_policies(EmailIdentity=domain)
            assert policy_name in resp["Policies"]
        finally:
            client.delete_email_identity(EmailIdentity=domain)

    def test_get_email_identity_policies(self, client):
        """GetEmailIdentityPolicies returns policies dict."""
        domain = f"{_uid('dom')}.example.com"
        client.create_email_identity(EmailIdentity=domain)
        try:
            resp = client.get_email_identity_policies(EmailIdentity=domain)
            assert "Policies" in resp
            assert isinstance(resp["Policies"], dict)
        finally:
            client.delete_email_identity(EmailIdentity=domain)

    def test_update_email_template(self, client):
        """UpdateEmailTemplate changes template content."""
        name = _uid("tmpl")
        client.create_email_template(
            TemplateName=name,
            TemplateContent={
                "Subject": "Original",
                "Text": "Original body",
                "Html": "<p>Original</p>",
            },
        )
        try:
            client.update_email_template(
                TemplateName=name,
                TemplateContent={
                    "Subject": "Updated",
                    "Text": "Updated body",
                    "Html": "<p>Updated</p>",
                },
            )
            resp = client.get_email_template(TemplateName=name)
            assert resp["TemplateContent"]["Subject"] == "Updated"
        finally:
            client.delete_email_template(TemplateName=name)

    def test_tag_resource(self, client):
        """TagResource adds tags to a configuration set."""
        name = _uid("cfgset")
        client.create_configuration_set(ConfigurationSetName=name)
        arn = f"arn:aws:ses:us-east-1:123456789012:configuration-set/{name}"
        try:
            client.tag_resource(
                ResourceArn=arn,
                Tags=[{"Key": "env", "Value": "test"}],
            )
            resp = client.list_tags_for_resource(ResourceArn=arn)
            assert "Tags" in resp
            tag_keys = [t["Key"] for t in resp["Tags"]]
            assert "env" in tag_keys
        finally:
            client.delete_configuration_set(ConfigurationSetName=name)

    def test_list_tags_for_resource(self, client):
        """ListTagsForResource returns tags."""
        name = _uid("cfgset")
        client.create_configuration_set(ConfigurationSetName=name)
        arn = f"arn:aws:ses:us-east-1:123456789012:configuration-set/{name}"
        try:
            resp = client.list_tags_for_resource(ResourceArn=arn)
            assert "Tags" in resp
            assert isinstance(resp["Tags"], list)
        finally:
            client.delete_configuration_set(ConfigurationSetName=name)

    def test_untag_resource(self, client):
        """UntagResource removes tags from a configuration set."""
        name = _uid("cfgset")
        client.create_configuration_set(ConfigurationSetName=name)
        arn = f"arn:aws:ses:us-east-1:123456789012:configuration-set/{name}"
        try:
            client.tag_resource(
                ResourceArn=arn,
                Tags=[{"Key": "remove-me", "Value": "yes"}, {"Key": "keep-me", "Value": "yes"}],
            )
            client.untag_resource(ResourceArn=arn, TagKeys=["remove-me"])
            resp = client.list_tags_for_resource(ResourceArn=arn)
            tag_keys = [t["Key"] for t in resp["Tags"]]
            assert "remove-me" not in tag_keys
            assert "keep-me" in tag_keys
        finally:
            client.delete_configuration_set(ConfigurationSetName=name)

    def test_send_email(self, client):
        """SendEmail sends a simple email and returns a MessageId."""
        email = f"{_uid('send')}@example.com"
        client.create_email_identity(EmailIdentity=email)
        try:
            resp = client.send_email(
                FromEmailAddress=email,
                Destination={"ToAddresses": ["recipient@example.com"]},
                Content={
                    "Simple": {
                        "Subject": {"Data": "Test Subject"},
                        "Body": {"Text": {"Data": "Test body content"}},
                    }
                },
            )
            assert "MessageId" in resp
            assert len(resp["MessageId"]) > 0
        finally:
            client.delete_email_identity(EmailIdentity=email)

    def test_send_email_html_body(self, client):
        """SendEmail with HTML body returns a MessageId."""
        email = f"{_uid('html')}@example.com"
        client.create_email_identity(EmailIdentity=email)
        try:
            resp = client.send_email(
                FromEmailAddress=email,
                Destination={"ToAddresses": ["to@example.com"]},
                Content={
                    "Simple": {
                        "Subject": {"Data": "HTML Test"},
                        "Body": {"Html": {"Data": "<h1>Hello</h1>"}},
                    }
                },
            )
            assert "MessageId" in resp
            assert len(resp["MessageId"]) > 0
        finally:
            client.delete_email_identity(EmailIdentity=email)

    def test_send_email_multiple_recipients(self, client):
        """SendEmail with CC and BCC."""
        email = f"{_uid('multi')}@example.com"
        client.create_email_identity(EmailIdentity=email)
        try:
            resp = client.send_email(
                FromEmailAddress=email,
                Destination={
                    "ToAddresses": ["to1@example.com", "to2@example.com"],
                    "CcAddresses": ["cc@example.com"],
                    "BccAddresses": ["bcc@example.com"],
                },
                Content={
                    "Simple": {
                        "Subject": {"Data": "Multi recipient test"},
                        "Body": {"Text": {"Data": "Hello all"}},
                    }
                },
            )
            assert "MessageId" in resp
        finally:
            client.delete_email_identity(EmailIdentity=email)

    def test_get_configuration_set_details(self, client):
        """GetConfigurationSet returns detailed attributes."""
        name = _uid("cfgdet")
        client.create_configuration_set(ConfigurationSetName=name)
        try:
            resp = client.get_configuration_set(ConfigurationSetName=name)
            assert resp["ConfigurationSetName"] == name
            assert "SendingOptions" in resp
        finally:
            client.delete_configuration_set(ConfigurationSetName=name)

    def test_get_email_identity_dkim_attributes(self, client):
        """GetEmailIdentity returns DkimAttributes for a domain."""
        domain = f"{_uid('dkim')}.example.com"
        client.create_email_identity(EmailIdentity=domain)
        try:
            resp = client.get_email_identity(EmailIdentity=domain)
            assert "DkimAttributes" in resp
            assert "SigningEnabled" in resp["DkimAttributes"]
        finally:
            client.delete_email_identity(EmailIdentity=domain)

    def test_tag_email_identity_and_list_tags(self, client):
        """TagResource on an identity, then ListTagsForResource to verify."""
        domain = f"{_uid('dtag')}.example.com"
        client.create_email_identity(EmailIdentity=domain)
        arn = f"arn:aws:ses:us-east-1:123456789012:identity/{domain}"
        try:
            client.tag_resource(
                ResourceArn=arn,
                Tags=[{"Key": "project", "Value": "robotocore"}],
            )
            resp = client.list_tags_for_resource(ResourceArn=arn)
            tag_keys = [t["Key"] for t in resp["Tags"]]
            assert "project" in tag_keys
        finally:
            client.delete_email_identity(EmailIdentity=domain)

    def test_get_email_identity_not_found(self, client):
        """GetEmailIdentity for nonexistent identity returns NotFoundException."""
        with pytest.raises(ClientError) as exc:
            client.get_email_identity(EmailIdentity="nonexistent-xyz.example.com")
        assert exc.value.response["Error"]["Code"] == "NotFoundException"

    def test_get_email_template_not_found(self, client):
        """GetEmailTemplate for nonexistent template returns NotFoundException."""
        with pytest.raises(ClientError) as exc:
            client.get_email_template(TemplateName="nonexistent-xyz-tmpl")
        assert exc.value.response["Error"]["Code"] == "NotFoundException"

    def test_get_contact_list_not_found(self, client):
        """GetContactList for nonexistent list returns NotFoundException."""
        with pytest.raises(ClientError) as exc:
            client.get_contact_list(ContactListName="nonexistent-xyz-cl")
        assert exc.value.response["Error"]["Code"] == "NotFoundException"

    def test_get_dedicated_ip_pool_not_found(self, client):
        """GetDedicatedIpPool for nonexistent pool returns NotFoundException."""
        with pytest.raises(ClientError) as exc:
            client.get_dedicated_ip_pool(PoolName="nonexistent-xyz-pool")
        assert exc.value.response["Error"]["Code"] == "NotFoundException"

    def test_get_configuration_set_not_found(self, client):
        """GetConfigurationSet for nonexistent set returns NotFoundException."""
        with pytest.raises(ClientError) as exc:
            client.get_configuration_set(ConfigurationSetName="nonexistent-xyz-cs")
        assert exc.value.response["Error"]["Code"] == "NotFoundException"

    def test_create_contact_with_topics(self, client):
        """CreateContact with topic preferences and verify via GetContact."""
        cl_name = _uid("cltop")
        email = f"{_uid('ct')}@example.com"
        client.create_contact_list(
            ContactListName=cl_name,
            Topics=[
                {
                    "TopicName": "updates",
                    "DisplayName": "Updates",
                    "DefaultSubscriptionStatus": "OPT_IN",
                }
            ],
        )
        try:
            client.create_contact(
                ContactListName=cl_name,
                EmailAddress=email,
                TopicPreferences=[{"TopicName": "updates", "SubscriptionStatus": "OPT_OUT"}],
            )
            resp = client.get_contact(ContactListName=cl_name, EmailAddress=email)
            assert resp["EmailAddress"] == email
            prefs = resp.get("TopicPreferences", [])
            assert any(p["TopicName"] == "updates" for p in prefs)
        finally:
            client.delete_contact_list(ContactListName=cl_name)

    def test_list_configuration_sets_after_multiple(self, client):
        """ListConfigurationSets returns all created sets."""
        names = [_uid("mcs") for _ in range(3)]
        for n in names:
            client.create_configuration_set(ConfigurationSetName=n)
        try:
            resp = client.list_configuration_sets()
            for n in names:
                assert n in resp["ConfigurationSets"]
        finally:
            for n in names:
                client.delete_configuration_set(ConfigurationSetName=n)
