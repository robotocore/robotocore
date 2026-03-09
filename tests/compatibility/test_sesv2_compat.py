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

    def test_create_configuration_set_with_tags(self, client):
        """CreateConfigurationSet with tags, verify via GetConfigurationSet."""
        name = _uid("cstag")
        client.create_configuration_set(
            ConfigurationSetName=name,
            Tags=[{"Key": "env", "Value": "staging"}, {"Key": "team", "Value": "platform"}],
        )
        try:
            resp = client.get_configuration_set(ConfigurationSetName=name)
            assert resp["ConfigurationSetName"] == name
            tag_map = {t["Key"]: t["Value"] for t in resp.get("Tags", [])}
            assert tag_map.get("env") == "staging"
            assert tag_map.get("team") == "platform"
        finally:
            client.delete_configuration_set(ConfigurationSetName=name)

    def test_configuration_set_has_options(self, client):
        """GetConfigurationSet returns SendingOptions and other option keys."""
        name = _uid("csopt")
        client.create_configuration_set(ConfigurationSetName=name)
        try:
            resp = client.get_configuration_set(ConfigurationSetName=name)
            assert "SendingOptions" in resp
            assert "DeliveryOptions" in resp
            assert "ReputationOptions" in resp
            assert "TrackingOptions" in resp
        finally:
            client.delete_configuration_set(ConfigurationSetName=name)

    def test_duplicate_configuration_set_raises(self, client):
        """Creating duplicate configuration set raises ConfigurationSetAlreadyExistsException."""
        name = _uid("csdup")
        client.create_configuration_set(ConfigurationSetName=name)
        try:
            with pytest.raises(ClientError) as exc:
                client.create_configuration_set(ConfigurationSetName=name)
            assert "AlreadyExists" in exc.value.response["Error"]["Code"]
        finally:
            client.delete_configuration_set(ConfigurationSetName=name)

    def test_contact_list_with_description_and_topics(self, client):
        """CreateContactList with description and topics, verify via GetContactList."""
        cl_name = _uid("cldesc")
        client.create_contact_list(
            ContactListName=cl_name,
            Description="Integration test list",
            Topics=[
                {
                    "TopicName": "news",
                    "DisplayName": "Newsletter",
                    "DefaultSubscriptionStatus": "OPT_OUT",
                }
            ],
        )
        try:
            resp = client.get_contact_list(ContactListName=cl_name)
            assert resp["ContactListName"] == cl_name
            assert resp["Description"] == "Integration test list"
            assert len(resp["Topics"]) == 1
            assert resp["Topics"][0]["TopicName"] == "news"
            assert resp["Topics"][0]["DisplayName"] == "Newsletter"
            assert resp["Topics"][0]["DefaultSubscriptionStatus"] == "OPT_OUT"
        finally:
            client.delete_contact_list(ContactListName=cl_name)

    def test_delete_nonexistent_contact_list_raises(self, client):
        """DeleteContactList for nonexistent list raises NotFoundException."""
        with pytest.raises(ClientError) as exc:
            client.delete_contact_list(ContactListName="nonexistent-xyz-dcl")
        assert exc.value.response["Error"]["Code"] == "NotFoundException"

    def test_get_contact_not_found(self, client):
        """GetContact for nonexistent contact raises NotFoundException."""
        cl_name = _uid("clgcnf")
        client.create_contact_list(ContactListName=cl_name)
        try:
            with pytest.raises(ClientError) as exc:
                client.get_contact(ContactListName=cl_name, EmailAddress="no-such@example.com")
            assert exc.value.response["Error"]["Code"] == "NotFoundException"
        finally:
            client.delete_contact_list(ContactListName=cl_name)

    def test_delete_nonexistent_contact_raises(self, client):
        """DeleteContact for nonexistent contact raises NotFoundException."""
        cl_name = _uid("cldcnf")
        client.create_contact_list(ContactListName=cl_name)
        try:
            with pytest.raises(ClientError) as exc:
                client.delete_contact(ContactListName=cl_name, EmailAddress="no-such@example.com")
            assert exc.value.response["Error"]["Code"] == "NotFoundException"
        finally:
            client.delete_contact_list(ContactListName=cl_name)

    def test_create_email_identity_with_tags(self, client):
        """CreateEmailIdentity with tags, verify via GetEmailIdentity."""
        domain = f"{_uid('tagid')}.example.com"
        client.create_email_identity(
            EmailIdentity=domain,
            Tags=[{"Key": "project", "Value": "robotocore"}],
        )
        try:
            resp = client.get_email_identity(EmailIdentity=domain)
            tag_map = {t["Key"]: t["Value"] for t in resp.get("Tags", [])}
            assert tag_map.get("project") == "robotocore"
        finally:
            client.delete_email_identity(EmailIdentity=domain)

    def test_duplicate_email_template_raises(self, client):
        """Creating duplicate email template raises AlreadyExistsException."""
        name = _uid("tmpldup")
        client.create_email_template(
            TemplateName=name,
            TemplateContent={"Subject": "S", "Text": "T", "Html": "<p>H</p>"},
        )
        try:
            with pytest.raises(ClientError) as exc:
                client.create_email_template(
                    TemplateName=name,
                    TemplateContent={"Subject": "S2", "Text": "T2", "Html": "<p>H2</p>"},
                )
            assert exc.value.response["Error"]["Code"] == "AlreadyExistsException"
        finally:
            client.delete_email_template(TemplateName=name)

    def test_delete_nonexistent_template_succeeds(self, client):
        """DeleteEmailTemplate for nonexistent template succeeds (idempotent)."""
        resp = client.delete_email_template(TemplateName="nonexistent-xyz-tmpl-del")
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200

    def test_send_raw_email(self, client):
        """SendEmail with Raw content returns a MessageId."""
        email = f"{_uid('raw')}@example.com"
        client.create_email_identity(EmailIdentity=email)
        try:
            raw_msg = (
                f"From: {email}\r\n"
                "To: recipient@example.com\r\n"
                "Subject: Raw test\r\n"
                "\r\n"
                "This is a raw email body."
            )
            resp = client.send_email(
                FromEmailAddress=email,
                Destination={"ToAddresses": ["recipient@example.com"]},
                Content={"Raw": {"Data": raw_msg.encode("utf-8")}},
            )
            assert "MessageId" in resp
            assert len(resp["MessageId"]) > 0
        finally:
            client.delete_email_identity(EmailIdentity=email)

    def test_send_email_with_reply_to(self, client):
        """SendEmail with ReplyToAddresses returns a MessageId."""
        email = f"{_uid('rto')}@example.com"
        client.create_email_identity(EmailIdentity=email)
        try:
            resp = client.send_email(
                FromEmailAddress=email,
                Destination={"ToAddresses": ["to@example.com"]},
                ReplyToAddresses=["noreply@example.com"],
                Content={
                    "Simple": {
                        "Subject": {"Data": "Reply-to test"},
                        "Body": {"Text": {"Data": "body"}},
                    }
                },
            )
            assert "MessageId" in resp
            assert len(resp["MessageId"]) > 0
        finally:
            client.delete_email_identity(EmailIdentity=email)

    def test_send_email_with_configuration_set(self, client):
        """SendEmail with ConfigurationSetName succeeds."""
        email = f"{_uid('csend')}@example.com"
        cs_name = _uid("cssend")
        client.create_email_identity(EmailIdentity=email)
        client.create_configuration_set(ConfigurationSetName=cs_name)
        try:
            resp = client.send_email(
                FromEmailAddress=email,
                Destination={"ToAddresses": ["to@example.com"]},
                Content={
                    "Simple": {
                        "Subject": {"Data": "CS send test"},
                        "Body": {"Text": {"Data": "body"}},
                    }
                },
                ConfigurationSetName=cs_name,
            )
            assert "MessageId" in resp
        finally:
            client.delete_configuration_set(ConfigurationSetName=cs_name)
            client.delete_email_identity(EmailIdentity=email)

    def test_dedicated_ip_pool_with_tags(self, client):
        """CreateDedicatedIpPool with tags, verify via GetDedicatedIpPool."""
        name = _uid("pooltag")
        client.create_dedicated_ip_pool(PoolName=name, Tags=[{"Key": "env", "Value": "test"}])
        try:
            resp = client.get_dedicated_ip_pool(PoolName=name)
            assert resp["DedicatedIpPool"]["PoolName"] == name
        finally:
            client.delete_dedicated_ip_pool(PoolName=name)

    def test_multiple_contacts_in_list(self, client):
        """Create multiple contacts in a list and verify via ListContacts."""
        cl_name = _uid("clmulti")
        emails = [f"{_uid('mc')}@example.com" for _ in range(3)]
        client.create_contact_list(ContactListName=cl_name)
        try:
            for em in emails:
                client.create_contact(ContactListName=cl_name, EmailAddress=em)
            resp = client.list_contacts(ContactListName=cl_name)
            found = [ct["EmailAddress"] for ct in resp["Contacts"]]
            for em in emails:
                assert em in found
        finally:
            client.delete_contact_list(ContactListName=cl_name)

    def test_multiple_email_identities(self, client):
        """Create multiple email identities and list them all."""
        emails = [f"{_uid('mid')}@example.com" for _ in range(3)]
        for em in emails:
            client.create_email_identity(EmailIdentity=em)
        try:
            resp = client.list_email_identities()
            found = [i["IdentityName"] for i in resp["EmailIdentities"]]
            for em in emails:
                assert em in found
        finally:
            for em in emails:
                client.delete_email_identity(EmailIdentity=em)

    def test_multiple_email_templates(self, client):
        """Create multiple email templates and list them all."""
        names = [_uid("mtmpl") for _ in range(3)]
        for nm in names:
            client.create_email_template(
                TemplateName=nm,
                TemplateContent={"Subject": "S", "Text": "T", "Html": "<p>H</p>"},
            )
        try:
            resp = client.list_email_templates()
            found = [t["TemplateName"] for t in resp["TemplatesMetadata"]]
            for nm in names:
                assert nm in found
        finally:
            for nm in names:
                client.delete_email_template(TemplateName=nm)

    def test_update_email_template_text_and_html(self, client):
        """UpdateEmailTemplate changes both text and HTML content."""
        name = _uid("utmpl")
        client.create_email_template(
            TemplateName=name,
            TemplateContent={
                "Subject": "Original",
                "Text": "Original text",
                "Html": "<p>Original</p>",
            },
        )
        try:
            client.update_email_template(
                TemplateName=name,
                TemplateContent={
                    "Subject": "Updated Subject",
                    "Text": "Updated text",
                    "Html": "<p>Updated</p>",
                },
            )
            resp = client.get_email_template(TemplateName=name)
            assert resp["TemplateContent"]["Subject"] == "Updated Subject"
            assert resp["TemplateContent"]["Text"] == "Updated text"
            assert resp["TemplateContent"]["Html"] == "<p>Updated</p>"
        finally:
            client.delete_email_template(TemplateName=name)

    def test_tag_and_untag_email_identity(self, client):
        """Tag an identity, verify, untag, verify removal."""
        domain = f"{_uid('tuid')}.example.com"
        client.create_email_identity(EmailIdentity=domain)
        arn = f"arn:aws:ses:us-east-1:123456789012:identity/{domain}"
        try:
            client.tag_resource(
                ResourceArn=arn,
                Tags=[{"Key": "a", "Value": "1"}, {"Key": "b", "Value": "2"}],
            )
            resp = client.list_tags_for_resource(ResourceArn=arn)
            tag_map = {t["Key"]: t["Value"] for t in resp["Tags"]}
            assert tag_map["a"] == "1"
            assert tag_map["b"] == "2"
            client.untag_resource(ResourceArn=arn, TagKeys=["a"])
            resp = client.list_tags_for_resource(ResourceArn=arn)
            tag_keys = [t["Key"] for t in resp["Tags"]]
            assert "a" not in tag_keys
            assert "b" in tag_keys
        finally:
            client.delete_email_identity(EmailIdentity=domain)

    def test_multiple_dedicated_ip_pools(self, client):
        """Create multiple dedicated IP pools and list them."""
        names = [_uid("mpol") for _ in range(3)]
        for nm in names:
            client.create_dedicated_ip_pool(PoolName=nm)
        try:
            resp = client.list_dedicated_ip_pools()
            for nm in names:
                assert nm in resp["DedicatedIpPools"]
        finally:
            for nm in names:
                client.delete_dedicated_ip_pool(PoolName=nm)

    def test_delete_dedicated_ip_pool(self, client):
        """DeleteDedicatedIpPool removes the pool from the list."""
        name = _uid("dpool")
        client.create_dedicated_ip_pool(PoolName=name)
        client.delete_dedicated_ip_pool(PoolName=name)
        resp = client.list_dedicated_ip_pools()
        assert name not in resp["DedicatedIpPools"]

    def test_contact_list_multiple_topics(self, client):
        """ContactList with multiple topics preserves all topics."""
        cl_name = _uid("clmt")
        client.create_contact_list(
            ContactListName=cl_name,
            Topics=[
                {
                    "TopicName": "marketing",
                    "DisplayName": "Marketing",
                    "DefaultSubscriptionStatus": "OPT_IN",
                },
                {
                    "TopicName": "transactional",
                    "DisplayName": "Transactional",
                    "DefaultSubscriptionStatus": "OPT_OUT",
                },
            ],
        )
        try:
            resp = client.get_contact_list(ContactListName=cl_name)
            topic_names = [t["TopicName"] for t in resp["Topics"]]
            assert "marketing" in topic_names
            assert "transactional" in topic_names
        finally:
            client.delete_contact_list(ContactListName=cl_name)

    def test_create_email_identity_returns_dkim(self, client):
        """CreateEmailIdentity for domain returns DkimAttributes."""
        domain = f"{_uid('dkret')}.example.com"
        resp = client.create_email_identity(EmailIdentity=domain)
        try:
            assert "DkimAttributes" in resp
        finally:
            client.delete_email_identity(EmailIdentity=domain)

    def test_get_account(self, client):
        """GetAccount returns account-level SES details."""
        resp = client.get_account()
        assert "SendQuota" in resp
        assert "SendingEnabled" in resp
        assert "EnforcementStatus" in resp
        assert "DedicatedIpAutoWarmupEnabled" in resp

    def test_get_configuration_set_event_destinations(self, client):
        """GetConfigurationSetEventDestinations returns event destinations list."""
        name = _uid("csevt")
        client.create_configuration_set(ConfigurationSetName=name)
        try:
            resp = client.get_configuration_set_event_destinations(ConfigurationSetName=name)
            assert "EventDestinations" in resp
            assert isinstance(resp["EventDestinations"], list)
        finally:
            client.delete_configuration_set(ConfigurationSetName=name)

    def test_list_custom_verification_email_templates(self, client):
        """ListCustomVerificationEmailTemplates returns templates list."""
        resp = client.list_custom_verification_email_templates()
        assert "CustomVerificationEmailTemplates" in resp
        assert isinstance(resp["CustomVerificationEmailTemplates"], list)

    def test_create_and_get_custom_verification_email_template(self, client):
        """CreateCustomVerificationEmailTemplate + GetCustomVerificationEmailTemplate."""
        tmpl_name = _uid("cvtmpl")
        client.create_custom_verification_email_template(
            TemplateName=tmpl_name,
            FromEmailAddress="test@example.com",
            TemplateSubject="Please verify",
            TemplateContent="<html>Click to verify</html>",
            SuccessRedirectionURL="https://example.com/success",
            FailureRedirectionURL="https://example.com/failure",
        )
        try:
            resp = client.get_custom_verification_email_template(TemplateName=tmpl_name)
            assert resp["TemplateName"] == tmpl_name
            assert resp["FromEmailAddress"] == "test@example.com"
            assert resp["TemplateSubject"] == "Please verify"
            assert resp["SuccessRedirectionURL"] == "https://example.com/success"
            assert resp["FailureRedirectionURL"] == "https://example.com/failure"
        finally:
            client.delete_custom_verification_email_template(TemplateName=tmpl_name)

    def test_list_custom_verification_email_templates_after_create(self, client):
        """ListCustomVerificationEmailTemplates includes created template."""
        tmpl_name = _uid("cvlist")
        client.create_custom_verification_email_template(
            TemplateName=tmpl_name,
            FromEmailAddress="list@example.com",
            TemplateSubject="Verify",
            TemplateContent="<html>Verify</html>",
            SuccessRedirectionURL="https://example.com/ok",
            FailureRedirectionURL="https://example.com/fail",
        )
        try:
            resp = client.list_custom_verification_email_templates()
            names = [t["TemplateName"] for t in resp["CustomVerificationEmailTemplates"]]
            assert tmpl_name in names
        finally:
            client.delete_custom_verification_email_template(TemplateName=tmpl_name)

    def test_list_email_identities_has_identity_type(self, client):
        """ListEmailIdentities entries have IdentityType and IdentityName."""
        email = f"{_uid('lit')}@example.com"
        client.create_email_identity(EmailIdentity=email)
        try:
            resp = client.list_email_identities()
            matching = [i for i in resp["EmailIdentities"] if i["IdentityName"] == email]
            assert len(matching) == 1
            assert matching[0]["IdentityType"] == "EMAIL_ADDRESS"
        finally:
            client.delete_email_identity(EmailIdentity=email)

    def test_put_account_sending_attributes(self, client):
        """PutAccountSendingAttributes toggles sending enabled."""
        resp = client.put_account_sending_attributes(SendingEnabled=True)
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200

    def test_put_account_suppression_attributes(self, client):
        """PutAccountSuppressionAttributes sets suppressed reasons."""
        resp = client.put_account_suppression_attributes(SuppressedReasons=["BOUNCE"])
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200

    def test_put_account_details(self, client):
        """PutAccountDetails sets mail type and website URL."""
        resp = client.put_account_details(
            MailType="MARKETING",
            WebsiteURL="https://example.com",
            UseCaseDescription="Testing robotocore",
        )
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200

    def test_put_configuration_set_sending_options(self, client):
        """PutConfigurationSetSendingOptions toggles sending on a config set."""
        cs_name = _uid("cssend")
        client.create_configuration_set(ConfigurationSetName=cs_name)
        try:
            resp = client.put_configuration_set_sending_options(
                ConfigurationSetName=cs_name, SendingEnabled=True
            )
            assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200
        finally:
            client.delete_configuration_set(ConfigurationSetName=cs_name)

    def test_put_configuration_set_reputation_options(self, client):
        """PutConfigurationSetReputationOptions enables reputation metrics."""
        cs_name = _uid("csrep")
        client.create_configuration_set(ConfigurationSetName=cs_name)
        try:
            resp = client.put_configuration_set_reputation_options(
                ConfigurationSetName=cs_name, ReputationMetricsEnabled=True
            )
            assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200
        finally:
            client.delete_configuration_set(ConfigurationSetName=cs_name)

    def test_create_and_delete_event_destination(self, client):
        """CreateConfigurationSetEventDestination + Delete round-trip."""
        cs_name = _uid("csed")
        client.create_configuration_set(ConfigurationSetName=cs_name)
        try:
            client.create_configuration_set_event_destination(
                ConfigurationSetName=cs_name,
                EventDestinationName="dest1",
                EventDestination={
                    "Enabled": True,
                    "MatchingEventTypes": ["SEND"],
                    "SnsDestination": {"TopicArn": "arn:aws:sns:us-east-1:123456789012:test-topic"},
                },
            )
            resp = client.get_configuration_set_event_destinations(ConfigurationSetName=cs_name)
            assert len(resp["EventDestinations"]) == 1
            assert resp["EventDestinations"][0]["Name"] == "dest1"

            client.delete_configuration_set_event_destination(
                ConfigurationSetName=cs_name, EventDestinationName="dest1"
            )
            resp2 = client.get_configuration_set_event_destinations(ConfigurationSetName=cs_name)
            assert len(resp2["EventDestinations"]) == 0
        finally:
            client.delete_configuration_set(ConfigurationSetName=cs_name)

    def test_update_configuration_set_event_destination(self, client):
        """UpdateConfigurationSetEventDestination changes event types."""
        cs_name = _uid("csud")
        client.create_configuration_set(ConfigurationSetName=cs_name)
        try:
            client.create_configuration_set_event_destination(
                ConfigurationSetName=cs_name,
                EventDestinationName="upd-dest",
                EventDestination={
                    "Enabled": True,
                    "MatchingEventTypes": ["SEND"],
                    "SnsDestination": {"TopicArn": "arn:aws:sns:us-east-1:123456789012:test-topic"},
                },
            )
            client.update_configuration_set_event_destination(
                ConfigurationSetName=cs_name,
                EventDestinationName="upd-dest",
                EventDestination={
                    "Enabled": False,
                    "MatchingEventTypes": ["SEND", "DELIVERY"],
                    "SnsDestination": {"TopicArn": "arn:aws:sns:us-east-1:123456789012:test-topic"},
                },
            )
            resp = client.get_configuration_set_event_destinations(ConfigurationSetName=cs_name)
            dest = resp["EventDestinations"][0]
            assert dest["Enabled"] is False
            assert "DELIVERY" in dest["MatchingEventTypes"]
        finally:
            client.delete_configuration_set(ConfigurationSetName=cs_name)
