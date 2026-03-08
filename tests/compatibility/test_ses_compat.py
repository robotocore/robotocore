"""SES compatibility tests."""

import pytest

from tests.compatibility.conftest import make_client


@pytest.fixture
def ses():
    return make_client("ses")


@pytest.fixture
def sesv2():
    return make_client("sesv2")


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

    def test_verify_domain_identity_in_list(self, ses):
        """Verify a domain and check it appears in domain identity list."""
        ses.verify_domain_identity(Domain="compat-domain.com")
        identities = ses.list_identities(IdentityType="Domain")
        assert "compat-domain.com" in identities["Identities"]

    def test_get_send_quota_fields(self, ses):
        """Verify send quota returns numeric fields."""
        quota = ses.get_send_quota()
        assert isinstance(quota["Max24HourSend"], float)
        assert isinstance(quota["MaxSendRate"], float)
        assert isinstance(quota["SentLast24Hours"], float)

    def test_get_send_statistics_structure(self, ses):
        """Verify send statistics returns a list of data points."""
        stats = ses.get_send_statistics()
        assert "SendDataPoints" in stats
        # Each data point should have expected keys if any exist
        for dp in stats["SendDataPoints"]:
            assert "Timestamp" in dp

    def test_create_receipt_rule_set(self, ses):
        """Create a receipt rule set."""
        response = ses.create_receipt_rule_set(RuleSetName="compat-ruleset")
        assert response["ResponseMetadata"]["HTTPStatusCode"] == 200
        # Clean up
        ses.delete_receipt_rule_set(RuleSetName="compat-ruleset")

    def test_create_receipt_rule(self, ses):
        """Create a receipt rule within a rule set."""
        ses.create_receipt_rule_set(RuleSetName="rule-test-set")
        try:
            response = ses.create_receipt_rule(
                RuleSetName="rule-test-set",
                Rule={
                    "Name": "test-rule",
                    "Enabled": True,
                    "Recipients": ["test@example.com"],
                    "Actions": [
                        {
                            "S3Action": {
                                "BucketName": "my-bucket",
                            }
                        }
                    ],
                },
            )
            assert response["ResponseMetadata"]["HTTPStatusCode"] == 200
        finally:
            ses.delete_receipt_rule(RuleSetName="rule-test-set", RuleName="test-rule")
            ses.delete_receipt_rule_set(RuleSetName="rule-test-set")

    def test_set_identity_notification_topic(self, ses):
        """Set notification topic for an identity."""
        ses.verify_email_identity(EmailAddress="notif@example.com")
        response = ses.set_identity_notification_topic(
            Identity="notif@example.com",
            NotificationType="Bounce",
        )
        assert response["ResponseMetadata"]["HTTPStatusCode"] == 200

    def test_send_raw_email_with_attachment(self, ses):
        """Send a raw MIME email with an attachment-like structure."""
        ses.verify_email_identity(EmailAddress="attach-sender@example.com")
        import base64

        attachment_data = base64.b64encode(b"Hello attachment content").decode()
        raw_message = (
            "From: attach-sender@example.com\r\n"
            "To: recipient@example.com\r\n"
            "Subject: Attachment Test\r\n"
            "MIME-Version: 1.0\r\n"
            'Content-Type: multipart/mixed; boundary="boundary"\r\n'
            "\r\n"
            "--boundary\r\n"
            "Content-Type: text/plain\r\n"
            "\r\n"
            "This email has an attachment.\r\n"
            "--boundary\r\n"
            "Content-Type: application/octet-stream\r\n"
            "Content-Transfer-Encoding: base64\r\n"
            'Content-Disposition: attachment; filename="test.txt"\r\n'
            "\r\n"
            f"{attachment_data}\r\n"
            "--boundary--\r\n"
        )
        response = ses.send_raw_email(
            Source="attach-sender@example.com",
            RawMessage={"Data": raw_message},
        )
        assert "MessageId" in response

    def test_send_email_html_body(self, ses):
        """Send an email with HTML body."""
        ses.verify_email_identity(EmailAddress="html-sender@example.com")
        response = ses.send_email(
            Source="html-sender@example.com",
            Destination={"ToAddresses": ["html-recipient@example.com"]},
            Message={
                "Subject": {"Data": "HTML Test"},
                "Body": {"Html": {"Data": "<h1>Hello</h1><p>World</p>"}},
            },
        )
        assert "MessageId" in response
        assert response["ResponseMetadata"]["HTTPStatusCode"] == 200

    def test_send_email_with_cc_and_bcc(self, ses):
        """Send an email with CC and BCC recipients."""
        ses.verify_email_identity(EmailAddress="cc-sender@example.com")
        response = ses.send_email(
            Source="cc-sender@example.com",
            Destination={
                "ToAddresses": ["to@example.com"],
                "CcAddresses": ["cc@example.com"],
                "BccAddresses": ["bcc@example.com"],
            },
            Message={
                "Subject": {"Data": "CC BCC Test"},
                "Body": {"Text": {"Data": "Testing CC and BCC"}},
            },
        )
        assert "MessageId" in response

    def test_get_identity_dkim_attributes(self, ses):
        """Get DKIM attributes for a verified identity."""
        ses.verify_email_identity(EmailAddress="dkim@example.com")
        response = ses.get_identity_dkim_attributes(Identities=["dkim@example.com"])
        assert "DkimAttributes" in response
        assert "dkim@example.com" in response["DkimAttributes"]

    def test_list_identities_pagination(self, ses):
        """Verify list identities supports MaxItems."""
        for i in range(5):
            ses.verify_email_identity(EmailAddress=f"page-{i}@example.com")
        response = ses.list_identities(MaxItems=2)
        assert len(response["Identities"]) <= 2


class TestSESv2Operations:
    def test_create_email_identity(self, sesv2):
        """Create an email identity via SES v2."""
        response = sesv2.create_email_identity(EmailIdentity="v2test@example.com")
        assert response["ResponseMetadata"]["HTTPStatusCode"] == 200
        sesv2.delete_email_identity(EmailIdentity="v2test@example.com")

    def test_get_email_identity(self, sesv2):
        """Create and get an email identity via SES v2."""
        sesv2.create_email_identity(EmailIdentity="v2get@example.com")
        try:
            got = sesv2.get_email_identity(EmailIdentity="v2get@example.com")
            assert got["ResponseMetadata"]["HTTPStatusCode"] == 200
            assert "IdentityType" in got
        finally:
            sesv2.delete_email_identity(EmailIdentity="v2get@example.com")

    def test_list_email_identities(self, sesv2):
        """List email identities via SES v2."""
        sesv2.create_email_identity(EmailIdentity="v2list@example.com")
        try:
            response = sesv2.list_email_identities()
            identity_names = [i["IdentityName"] for i in response["EmailIdentities"]]
            assert "v2list@example.com" in identity_names
        finally:
            sesv2.delete_email_identity(EmailIdentity="v2list@example.com")

    def test_delete_email_identity(self, sesv2):
        """Create and delete an email identity via SES v2."""
        sesv2.create_email_identity(EmailIdentity="v2del@example.com")
        sesv2.delete_email_identity(EmailIdentity="v2del@example.com")
        response = sesv2.list_email_identities()
        identity_names = [i["IdentityName"] for i in response["EmailIdentities"]]
        assert "v2del@example.com" not in identity_names

    def test_create_configuration_set(self, sesv2):
        """Create a configuration set via SES v2."""
        response = sesv2.create_configuration_set(ConfigurationSetName="compat-config-set")
        assert response["ResponseMetadata"]["HTTPStatusCode"] == 200
        sesv2.delete_configuration_set(ConfigurationSetName="compat-config-set")

    def test_list_configuration_sets(self, sesv2):
        """Create configuration sets and list them."""
        sesv2.create_configuration_set(ConfigurationSetName="list-config-1")
        sesv2.create_configuration_set(ConfigurationSetName="list-config-2")
        try:
            response = sesv2.list_configuration_sets()
            names = [cs for cs in response["ConfigurationSets"]]
            assert "list-config-1" in names
            assert "list-config-2" in names
        finally:
            sesv2.delete_configuration_set(ConfigurationSetName="list-config-1")
            sesv2.delete_configuration_set(ConfigurationSetName="list-config-2")

    def test_delete_configuration_set(self, sesv2):
        """Create and delete a configuration set."""
        sesv2.create_configuration_set(ConfigurationSetName="del-config")
        sesv2.delete_configuration_set(ConfigurationSetName="del-config")
        response = sesv2.list_configuration_sets()
        names = [cs for cs in response["ConfigurationSets"]]
        assert "del-config" not in names

    def test_create_email_template(self, sesv2):
        """Create an email template via SES v2."""
        response = sesv2.create_email_template(
            TemplateName="compat-template",
            TemplateContent={
                "Subject": "Hello {{name}}",
                "Text": "Hi {{name}}, welcome!",
                "Html": "<h1>Hi {{name}}</h1>",
            },
        )
        assert response["ResponseMetadata"]["HTTPStatusCode"] == 200
        sesv2.delete_email_template(TemplateName="compat-template")

    def test_get_email_template(self, sesv2):
        """Create and get an email template."""
        sesv2.create_email_template(
            TemplateName="get-template",
            TemplateContent={
                "Subject": "Test",
                "Text": "Body",
            },
        )
        try:
            got = sesv2.get_email_template(TemplateName="get-template")
            assert got["TemplateName"] == "get-template"
            assert got["TemplateContent"]["Subject"] == "Test"
        finally:
            sesv2.delete_email_template(TemplateName="get-template")

    def test_list_email_templates(self, sesv2):
        """Create templates and list them."""
        sesv2.create_email_template(
            TemplateName="list-tmpl-1",
            TemplateContent={"Subject": "S1", "Text": "B1"},
        )
        sesv2.create_email_template(
            TemplateName="list-tmpl-2",
            TemplateContent={"Subject": "S2", "Text": "B2"},
        )
        try:
            response = sesv2.list_email_templates()
            names = [t["TemplateName"] for t in response["TemplatesMetadata"]]
            assert "list-tmpl-1" in names
            assert "list-tmpl-2" in names
        finally:
            sesv2.delete_email_template(TemplateName="list-tmpl-1")
            sesv2.delete_email_template(TemplateName="list-tmpl-2")

    def test_update_email_template(self, sesv2):
        """Create and update an email template."""
        sesv2.create_email_template(
            TemplateName="upd-template",
            TemplateContent={"Subject": "Original", "Text": "Original body"},
        )
        try:
            sesv2.update_email_template(
                TemplateName="upd-template",
                TemplateContent={"Subject": "Updated", "Text": "Updated body"},
            )
            got = sesv2.get_email_template(TemplateName="upd-template")
            assert got["TemplateContent"]["Subject"] == "Updated"
        finally:
            sesv2.delete_email_template(TemplateName="upd-template")

    def test_create_contact_list(self, sesv2):
        """Create a contact list via SES v2."""
        response = sesv2.create_contact_list(ContactListName="compat-contacts")
        assert response["ResponseMetadata"]["HTTPStatusCode"] == 200
        sesv2.delete_contact_list(ContactListName="compat-contacts")

    def test_get_contact_list(self, sesv2):
        """Create and get a contact list."""
        sesv2.create_contact_list(ContactListName="get-contacts")
        try:
            got = sesv2.get_contact_list(ContactListName="get-contacts")
            assert got["ContactListName"] == "get-contacts"
        finally:
            sesv2.delete_contact_list(ContactListName="get-contacts")

    def test_list_contact_lists(self, sesv2):
        """Create contact lists and list them."""
        sesv2.create_contact_list(ContactListName="cl-list-1")
        sesv2.create_contact_list(ContactListName="cl-list-2")
        try:
            response = sesv2.list_contact_lists()
            names = [cl["ContactListName"] for cl in response["ContactLists"]]
            assert "cl-list-1" in names
            assert "cl-list-2" in names
        finally:
            sesv2.delete_contact_list(ContactListName="cl-list-1")
            sesv2.delete_contact_list(ContactListName="cl-list-2")

    def test_delete_contact_list(self, sesv2):
        """Create and delete a contact list."""
        sesv2.create_contact_list(ContactListName="del-contacts")
        sesv2.delete_contact_list(ContactListName="del-contacts")
        response = sesv2.list_contact_lists()
        names = [cl["ContactListName"] for cl in response["ContactLists"]]
        assert "del-contacts" not in names

    def test_create_describe_delete_configuration_set(self, ses):
        """CreateConfigurationSet / DescribeConfigurationSet / DeleteConfigurationSet."""
        cs_name = "test-config-set"
        try:
            ses.create_configuration_set(ConfigurationSet={"Name": cs_name})
            described = ses.describe_configuration_set(ConfigurationSetName=cs_name)
            assert described["ConfigurationSet"]["Name"] == cs_name
        finally:
            ses.delete_configuration_set(ConfigurationSetName=cs_name)

    def test_create_describe_delete_receipt_rule(self, ses):
        """CreateReceiptRule / DescribeReceiptRule / DeleteReceiptRule."""
        rule_set_name = "test-rule-set"
        rule_name = "test-rule"
        ses.create_receipt_rule_set(RuleSetName=rule_set_name)
        try:
            ses.create_receipt_rule(
                RuleSetName=rule_set_name,
                Rule={
                    "Name": rule_name,
                    "Enabled": True,
                    "Recipients": ["test@example.com"],
                    "Actions": [],
                },
            )
            described = ses.describe_receipt_rule(RuleSetName=rule_set_name, RuleName=rule_name)
            assert described["Rule"]["Name"] == rule_name

            ses.delete_receipt_rule(RuleSetName=rule_set_name, RuleName=rule_name)
        finally:
            ses.delete_receipt_rule_set(RuleSetName=rule_set_name)

    def test_create_get_delete_template(self, ses):
        """CreateTemplate / GetTemplate / DeleteTemplate."""
        template_name = "test-template"
        ses.create_template(
            Template={
                "TemplateName": template_name,
                "SubjectPart": "Hello {{name}}",
                "TextPart": "Dear {{name}}, welcome!",
                "HtmlPart": "<h1>Hello {{name}}</h1>",
            }
        )
        try:
            got = ses.get_template(TemplateName=template_name)
            assert got["Template"]["TemplateName"] == template_name
            assert got["Template"]["SubjectPart"] == "Hello {{name}}"
        finally:
            ses.delete_template(TemplateName=template_name)

    def test_list_templates(self, ses):
        """ListTemplates returns created templates."""
        template_name = "list-template"
        ses.create_template(
            Template={
                "TemplateName": template_name,
                "SubjectPart": "Subject",
                "TextPart": "Body",
            }
        )
        try:
            response = ses.list_templates()
            names = [t["Name"] for t in response.get("TemplatesMetadata", [])]
            assert template_name in names
        finally:
            ses.delete_template(TemplateName=template_name)

    def test_send_templated_email(self, ses):
        """SendTemplatedEmail using a template."""
        ses.verify_email_identity(EmailAddress="tmpl-sender@example.com")
        template_name = "send-tmpl"
        ses.create_template(
            Template={
                "TemplateName": template_name,
                "SubjectPart": "Hi {{name}}",
                "TextPart": "Hello {{name}}",
            }
        )
        try:
            response = ses.send_templated_email(
                Source="tmpl-sender@example.com",
                Destination={"ToAddresses": ["recipient@example.com"]},
                Template=template_name,
                TemplateData='{"name": "World"}',
            )
            assert "MessageId" in response
        finally:
            ses.delete_template(TemplateName=template_name)


class TestSESExtendedOperations:
    """Extended SES operations for higher coverage."""

    @pytest.fixture
    def ses(self):
        from tests.compatibility.conftest import make_client

        return make_client("ses")

    def test_get_send_quota(self, ses):
        resp = ses.get_send_quota()
        assert "Max24HourSend" in resp
        assert "MaxSendRate" in resp
        assert "SentLast24Hours" in resp

    def test_get_send_statistics(self, ses):
        resp = ses.get_send_statistics()
        assert "SendDataPoints" in resp

    def test_get_account_sending_enabled(self, ses):
        resp = ses.get_account_sending_enabled()
        assert "Enabled" in resp

    def test_set_identity_notification_topic(self, ses):
        ses.verify_email_identity(EmailAddress="notif@example.com")
        ses.set_identity_notification_topic(
            Identity="notif@example.com",
            NotificationType="Bounce",
        )

    def test_get_identity_notification_attributes(self, ses):
        ses.verify_email_identity(EmailAddress="notif-attrs@example.com")
        resp = ses.get_identity_notification_attributes(Identities=["notif-attrs@example.com"])
        assert "NotificationAttributes" in resp

    def test_set_identity_dkim_enabled(self, ses):
        ses.verify_email_identity(EmailAddress="dkim@example.com")
        ses.set_identity_dkim_enabled(Identity="dkim@example.com", DkimEnabled=True)

    def test_get_identity_dkim_attributes(self, ses):
        ses.verify_email_identity(EmailAddress="dkim-attrs@example.com")
        resp = ses.get_identity_dkim_attributes(Identities=["dkim-attrs@example.com"])
        assert "DkimAttributes" in resp

    def test_set_identity_feedback_forwarding_enabled(self, ses):
        ses.verify_email_identity(EmailAddress="feedback@example.com")
        ses.set_identity_feedback_forwarding_enabled(
            Identity="feedback@example.com", ForwardingEnabled=True
        )

    def test_get_identity_mail_from_domain_attributes(self, ses):
        ses.verify_email_identity(EmailAddress="mailfrom@example.com")
        resp = ses.get_identity_mail_from_domain_attributes(Identities=["mailfrom@example.com"])
        assert "MailFromDomainAttributes" in resp

    def test_send_raw_email(self, ses):
        ses.verify_email_identity(EmailAddress="raw-sender@example.com")
        raw_msg = (
            "From: raw-sender@example.com\r\n"
            "To: recipient@example.com\r\n"
            "Subject: Raw Test\r\n"
            "\r\n"
            "Raw email body\r\n"
        )
        resp = ses.send_raw_email(
            RawMessage={"Data": raw_msg},
        )
        assert "MessageId" in resp

    def test_create_receipt_rule_set(self, ses):
        import uuid

        name = f"rule-set-{uuid.uuid4().hex[:8]}"
        ses.create_receipt_rule_set(RuleSetName=name)
        resp = ses.list_receipt_rule_sets()
        names = [r["Name"] for r in resp["RuleSets"]]
        assert name in names
        ses.delete_receipt_rule_set(RuleSetName=name)

    def test_update_template(self, ses):
        import uuid

        tname = f"upd-tmpl-{uuid.uuid4().hex[:8]}"
        ses.create_template(
            Template={
                "TemplateName": tname,
                "SubjectPart": "V1",
                "TextPart": "Version 1",
            }
        )
        try:
            ses.update_template(
                Template={
                    "TemplateName": tname,
                    "SubjectPart": "V2",
                    "TextPart": "Version 2",
                }
            )
            resp = ses.get_template(TemplateName=tname)
            assert resp["Template"]["SubjectPart"] == "V2"
        finally:
            ses.delete_template(TemplateName=tname)

    def test_send_email_with_cc_bcc(self, ses):
        ses.verify_email_identity(EmailAddress="cc-sender@example.com")
        resp = ses.send_email(
            Source="cc-sender@example.com",
            Destination={
                "ToAddresses": ["to@example.com"],
                "CcAddresses": ["cc@example.com"],
                "BccAddresses": ["bcc@example.com"],
            },
            Message={
                "Subject": {"Data": "CC/BCC Test"},
                "Body": {"Text": {"Data": "Testing CC and BCC"}},
            },
        )
        assert "MessageId" in resp

    def test_verify_domain_identity(self, ses):
        resp = ses.verify_domain_identity(Domain="example.com")
        assert "VerificationToken" in resp

    def test_list_verified_email_addresses(self, ses):
        ses.verify_email_identity(EmailAddress="listed@example.com")
        resp = ses.list_verified_email_addresses()
        assert "VerifiedEmailAddresses" in resp

    def test_verify_domain_dkim(self, ses):
        resp = ses.verify_domain_dkim(Domain="dkim-domain.com")
        assert "DkimTokens" in resp
        assert isinstance(resp["DkimTokens"], list)

    def test_set_identity_mail_from_domain(self, ses):
        ses.verify_email_identity(EmailAddress="mailfrom-set@example.com")
        resp = ses.set_identity_mail_from_domain(
            Identity="mailfrom-set@example.com",
            MailFromDomain="bounce.example.com",
        )
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200

    def test_describe_receipt_rule_set(self, ses):
        import uuid

        name = f"desc-rs-{uuid.uuid4().hex[:8]}"
        ses.create_receipt_rule_set(RuleSetName=name)
        try:
            resp = ses.describe_receipt_rule_set(RuleSetName=name)
            assert resp["Metadata"]["Name"] == name
            assert "Rules" in resp
        finally:
            ses.delete_receipt_rule_set(RuleSetName=name)

    def test_set_and_describe_active_receipt_rule_set(self, ses):
        import uuid

        name = f"active-rs-{uuid.uuid4().hex[:8]}"
        ses.create_receipt_rule_set(RuleSetName=name)
        try:
            ses.set_active_receipt_rule_set(RuleSetName=name)
            resp = ses.describe_active_receipt_rule_set()
            assert resp["Metadata"]["Name"] == name
        finally:
            # Deactivate by calling with no name
            ses.set_active_receipt_rule_set()
            ses.delete_receipt_rule_set(RuleSetName=name)

    def test_list_configuration_sets_v1(self, ses):
        import uuid

        name = f"cs-list-{uuid.uuid4().hex[:8]}"
        ses.create_configuration_set(ConfigurationSet={"Name": name})
        try:
            resp = ses.list_configuration_sets()
            names = [cs["Name"] for cs in resp.get("ConfigurationSets", [])]
            assert name in names
        finally:
            ses.delete_configuration_set(ConfigurationSetName=name)

    def test_list_receipt_rule_sets(self, ses):
        import uuid

        name = f"list-rs-{uuid.uuid4().hex[:8]}"
        ses.create_receipt_rule_set(RuleSetName=name)
        try:
            resp = ses.list_receipt_rule_sets()
            names = [r["Name"] for r in resp["RuleSets"]]
            assert name in names
        finally:
            ses.delete_receipt_rule_set(RuleSetName=name)

    def test_describe_receipt_rule(self, ses):
        import uuid

        rs_name = f"desc-rule-rs-{uuid.uuid4().hex[:8]}"
        rule_name = f"desc-rule-{uuid.uuid4().hex[:8]}"
        ses.create_receipt_rule_set(RuleSetName=rs_name)
        try:
            ses.create_receipt_rule(
                RuleSetName=rs_name,
                Rule={
                    "Name": rule_name,
                    "Enabled": True,
                    "Recipients": ["test@example.com"],
                    "Actions": [],
                },
            )
            resp = ses.describe_receipt_rule(RuleSetName=rs_name, RuleName=rule_name)
            assert resp["Rule"]["Name"] == rule_name
        finally:
            ses.delete_receipt_rule(RuleSetName=rs_name, RuleName=rule_name)
            ses.delete_receipt_rule_set(RuleSetName=rs_name)


class TestSESGapStubs:
    """Tests for gap operations: custom verification templates, receipt filters, account sending."""

    @pytest.fixture
    def ses(self):
        return make_client("ses")

    def test_list_custom_verification_email_templates(self, ses):
        resp = ses.list_custom_verification_email_templates()
        assert "CustomVerificationEmailTemplates" in resp

    def test_list_receipt_filters(self, ses):
        resp = ses.list_receipt_filters()
        assert "Filters" in resp

    def test_get_account_sending_enabled(self, ses):
        resp = ses.get_account_sending_enabled()
        assert "Enabled" in resp


class TestSesAutoCoverage:
    """Auto-generated coverage tests for ses."""

    @pytest.fixture
    def client(self):
        return make_client("ses")

    def test_update_account_sending_enabled(self, client):
        """UpdateAccountSendingEnabled returns a response."""
        client.update_account_sending_enabled()

    def test_send_bulk_templated_email(self, client):
        """SendBulkTemplatedEmail sends to multiple destinations."""
        client.verify_email_identity(EmailAddress="bulk-sender@example.com")
        template_name = "bulk-tmpl"
        client.create_template(
            Template={
                "TemplateName": template_name,
                "SubjectPart": "Hi {{name}}",
                "TextPart": "Hello {{name}}",
            }
        )
        try:
            resp = client.send_bulk_templated_email(
                Source="bulk-sender@example.com",
                Template=template_name,
                DefaultTemplateData='{"name": "default"}',
                Destinations=[
                    {
                        "Destination": {"ToAddresses": ["a@example.com"]},
                        "ReplacementTemplateData": '{"name": "Alice"}',
                    },
                    {
                        "Destination": {"ToAddresses": ["b@example.com"]},
                        "ReplacementTemplateData": '{"name": "Bob"}',
                    },
                ],
            )
            assert "Status" in resp
            assert len(resp["Status"]) == 2
            assert "MessageId" in resp["Status"][0]
        finally:
            client.delete_template(TemplateName=template_name)

    def test_clone_receipt_rule_set(self, client):
        """CloneReceiptRuleSet clones an existing rule set."""
        import uuid

        orig = f"orig-rs-{uuid.uuid4().hex[:8]}"
        clone = f"clone-rs-{uuid.uuid4().hex[:8]}"
        client.create_receipt_rule_set(RuleSetName=orig)
        try:
            resp = client.clone_receipt_rule_set(
                RuleSetName=clone,
                OriginalRuleSetName=orig,
            )
            assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200
            # Verify clone exists
            rule_sets = client.list_receipt_rule_sets()
            names = [r["Name"] for r in rule_sets["RuleSets"]]
            assert clone in names
        finally:
            client.delete_receipt_rule_set(RuleSetName=clone)
            client.delete_receipt_rule_set(RuleSetName=orig)

    def test_create_configuration_set_event_destination(self, client):
        """CreateConfigurationSetEventDestination adds an event destination."""
        import uuid

        uid = uuid.uuid4().hex[:8]
        cs_name = f"evdst-cs-{uid}"
        client.create_configuration_set(ConfigurationSet={"Name": cs_name})
        try:
            resp = client.create_configuration_set_event_destination(
                ConfigurationSetName=cs_name,
                EventDestination={
                    "Name": f"test-dest-{uid}",
                    "Enabled": True,
                    "MatchingEventTypes": ["send", "bounce"],
                    "SNSDestination": {"TopicARN": "arn:aws:sns:us-east-1:123456789012:test-topic"},
                },
            )
            assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200
        finally:
            client.delete_configuration_set(ConfigurationSetName=cs_name)

    def test_update_configuration_set_reputation_metrics_enabled(self, client):
        """UpdateConfigurationSetReputationMetricsEnabled toggles metrics."""
        import uuid

        cs_name = f"rep-cs-{uuid.uuid4().hex[:8]}"
        client.create_configuration_set(ConfigurationSet={"Name": cs_name})
        try:
            resp = client.update_configuration_set_reputation_metrics_enabled(
                ConfigurationSetName=cs_name,
                Enabled=True,
            )
            assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200
        finally:
            client.delete_configuration_set(ConfigurationSetName=cs_name)

    def test_verify_email_address(self, client):
        """VerifyEmailAddress (legacy) sends verification."""
        resp = client.verify_email_address(EmailAddress="legacy-verify@example.com")
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200
        # Should appear in verified list
        listed = client.list_verified_email_addresses()
        assert "legacy-verify@example.com" in listed["VerifiedEmailAddresses"]

    def test_update_receipt_rule(self, client):
        """UpdateReceiptRule modifies an existing rule."""
        import uuid

        rs_name = f"upd-rule-rs-{uuid.uuid4().hex[:8]}"
        rule_name = f"upd-rule-{uuid.uuid4().hex[:8]}"
        client.create_receipt_rule_set(RuleSetName=rs_name)
        try:
            client.create_receipt_rule(
                RuleSetName=rs_name,
                Rule={
                    "Name": rule_name,
                    "Enabled": True,
                    "Recipients": ["test@example.com"],
                    "Actions": [],
                },
            )
            resp = client.update_receipt_rule(
                RuleSetName=rs_name,
                Rule={
                    "Name": rule_name,
                    "Enabled": False,
                    "Recipients": ["updated@example.com"],
                    "Actions": [],
                },
            )
            assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200
            # Verify the update
            described = client.describe_receipt_rule(RuleSetName=rs_name, RuleName=rule_name)
            assert described["Rule"]["Enabled"] is False
        finally:
            client.delete_receipt_rule(RuleSetName=rs_name, RuleName=rule_name)
            client.delete_receipt_rule_set(RuleSetName=rs_name)

    def test_test_render_template(self, client):
        """TestRenderTemplate renders a template with data."""
        template_name = "render-test-tmpl"
        client.create_template(
            Template={
                "TemplateName": template_name,
                "SubjectPart": "Hi {{name}}",
                "TextPart": "Hello {{name}}, welcome!",
                "HtmlPart": "<h1>Hello {{name}}</h1>",
            }
        )
        try:
            resp = client.test_render_template(
                TemplateName=template_name,
                TemplateData='{"name": "World"}',
            )
            assert "RenderedTemplate" in resp
        finally:
            client.delete_template(TemplateName=template_name)
