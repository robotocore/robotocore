"""SES compatibility tests."""

import uuid

import pytest

from tests.compatibility.conftest import make_client


@pytest.fixture
def ses():
    return make_client("ses")


class TestSESIdentities:
    """Tests for email and domain identity management."""

    def test_verify_email_identity(self, ses):
        response = ses.verify_email_identity(EmailAddress="test@example.com")
        assert response["ResponseMetadata"]["HTTPStatusCode"] == 200

    def test_list_identities(self, ses):
        ses.verify_email_identity(EmailAddress="list@example.com")
        response = ses.list_identities()
        assert "list@example.com" in response["Identities"]

    def test_verify_domain_identity(self, ses):
        response = ses.verify_domain_identity(Domain="example.org")
        assert "VerificationToken" in response
        assert response["ResponseMetadata"]["HTTPStatusCode"] == 200
        identities = ses.list_identities(IdentityType="Domain")
        assert "example.org" in identities["Identities"]

    def test_list_verified_identities_email_type(self, ses):
        """List identities filtered by email type."""
        ses.verify_email_identity(EmailAddress="filter-email@example.com")
        response = ses.list_identities(IdentityType="EmailAddress")
        assert "filter-email@example.com" in response["Identities"]

    def test_delete_identity(self, ses):
        """Verify and then delete an identity."""
        ses.verify_email_identity(EmailAddress="delete-me@example.com")
        identities = ses.list_identities()
        assert "delete-me@example.com" in identities["Identities"]
        ses.delete_identity(Identity="delete-me@example.com")
        identities = ses.list_identities()
        assert "delete-me@example.com" not in identities["Identities"]

    def test_delete_identity_idempotent(self, ses):
        """Deleting a non-existent identity should succeed without error."""
        response = ses.delete_identity(Identity="nonexistent@example.com")
        assert response["ResponseMetadata"]["HTTPStatusCode"] == 200

    def test_get_identity_verification_attributes(self, ses):
        ses.verify_email_identity(EmailAddress="verify-attrs@example.com")
        response = ses.get_identity_verification_attributes(
            Identities=["verify-attrs@example.com"]
        )
        attrs = response["VerificationAttributes"]
        assert "verify-attrs@example.com" in attrs
        assert attrs["verify-attrs@example.com"]["VerificationStatus"] == "Success"

    def test_get_identity_verification_attributes_multiple(self, ses):
        """Query verification attributes for multiple identities at once."""
        ses.verify_email_identity(EmailAddress="multi-a@example.com")
        ses.verify_email_identity(EmailAddress="multi-b@example.com")
        response = ses.get_identity_verification_attributes(
            Identities=["multi-a@example.com", "multi-b@example.com"]
        )
        attrs = response["VerificationAttributes"]
        assert "multi-a@example.com" in attrs
        assert "multi-b@example.com" in attrs

    def test_set_identity_feedback_forwarding(self, ses):
        """Enable/disable feedback forwarding for an identity."""
        ses.verify_email_identity(EmailAddress="feedback@example.com")
        response = ses.set_identity_feedback_forwarding_enabled(
            Identity="feedback@example.com",
            ForwardingEnabled=False,
        )
        assert response["ResponseMetadata"]["HTTPStatusCode"] == 200

    def test_list_identities_with_max_items(self, ses):
        """List identities respects MaxItems parameter."""
        for i in range(3):
            ses.verify_email_identity(EmailAddress=f"page-{i}@example.com")
        response = ses.list_identities(MaxItems=2)
        assert len(response["Identities"]) <= 2

    def test_verify_domain_dkim(self, ses):
        """Verify DKIM for a domain returns DKIM tokens."""
        response = ses.verify_domain_dkim(Domain="dkim-test.example.org")
        assert "DkimTokens" in response
        assert isinstance(response["DkimTokens"], list)
        assert len(response["DkimTokens"]) > 0


class TestSESSendEmail:
    """Tests for sending emails."""

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

    def test_send_email_html_body(self, ses):
        """Send an email with an HTML body."""
        ses.verify_email_identity(EmailAddress="html-sender@example.com")
        response = ses.send_email(
            Source="html-sender@example.com",
            Destination={"ToAddresses": ["recipient@example.com"]},
            Message={
                "Subject": {"Data": "HTML Test"},
                "Body": {"Html": {"Data": "<h1>Hello</h1><p>HTML body</p>"}},
            },
        )
        assert "MessageId" in response
        assert response["ResponseMetadata"]["HTTPStatusCode"] == 200

    def test_send_email_multiple_recipients(self, ses):
        """Send an email to multiple To, CC, and BCC addresses."""
        ses.verify_email_identity(EmailAddress="multi-rcpt@example.com")
        response = ses.send_email(
            Source="multi-rcpt@example.com",
            Destination={
                "ToAddresses": ["to1@example.com", "to2@example.com"],
                "CcAddresses": ["cc@example.com"],
                "BccAddresses": ["bcc@example.com"],
            },
            Message={
                "Subject": {"Data": "Multi-recipient"},
                "Body": {"Text": {"Data": "Body"}},
            },
        )
        assert "MessageId" in response

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

    def test_send_raw_email_with_destinations(self, ses):
        """Send a raw email specifying explicit destinations."""
        ses.verify_email_identity(EmailAddress="raw2@example.com")
        raw_message = (
            "From: raw2@example.com\r\n"
            "Subject: Raw with destinations\r\n"
            "Content-Type: text/plain\r\n"
            "\r\n"
            "Body content.\r\n"
        )
        response = ses.send_raw_email(
            Source="raw2@example.com",
            Destinations=["dest1@example.com", "dest2@example.com"],
            RawMessage={"Data": raw_message},
        )
        assert "MessageId" in response


class TestSESSendQuotaAndStats:
    """Tests for send quota and statistics."""

    def test_get_send_quota(self, ses):
        """Get send quota returns expected fields."""
        response = ses.get_send_quota()
        assert "Max24HourSend" in response
        assert "SentLast24Hours" in response
        assert "MaxSendRate" in response
        assert response["ResponseMetadata"]["HTTPStatusCode"] == 200

    def test_get_send_statistics(self, ses):
        response = ses.get_send_statistics()
        assert "SendDataPoints" in response
        assert isinstance(response["SendDataPoints"], list)
        assert response["ResponseMetadata"]["HTTPStatusCode"] == 200

    def test_get_account_sending_enabled(self, ses):
        """Check that account sending status can be retrieved."""
        response = ses.get_account_sending_enabled()
        assert "Enabled" in response
        assert response["ResponseMetadata"]["HTTPStatusCode"] == 200


class TestSESReceiptRules:
    """Tests for receipt rule sets and receipt rules."""

    def test_create_receipt_rule_set(self, ses):
        rule_set_name = f"test-ruleset-{uuid.uuid4().hex[:8]}"
        response = ses.create_receipt_rule_set(RuleSetName=rule_set_name)
        assert response["ResponseMetadata"]["HTTPStatusCode"] == 200
        # Cleanup
        ses.delete_receipt_rule_set(RuleSetName=rule_set_name)

    def test_describe_receipt_rule_set(self, ses):
        rule_set_name = f"test-describe-rs-{uuid.uuid4().hex[:8]}"
        ses.create_receipt_rule_set(RuleSetName=rule_set_name)
        response = ses.describe_receipt_rule_set(RuleSetName=rule_set_name)
        assert "Metadata" in response
        assert response["Metadata"]["Name"] == rule_set_name
        assert "Rules" in response
        assert isinstance(response["Rules"], list)
        # Cleanup
        ses.delete_receipt_rule_set(RuleSetName=rule_set_name)

    def test_delete_receipt_rule_set(self, ses):
        rule_set_name = f"test-delete-rs-{uuid.uuid4().hex[:8]}"
        ses.create_receipt_rule_set(RuleSetName=rule_set_name)
        response = ses.delete_receipt_rule_set(RuleSetName=rule_set_name)
        assert response["ResponseMetadata"]["HTTPStatusCode"] == 200
        # Verify it no longer exists
        with pytest.raises(ses.exceptions.RuleSetDoesNotExistException):
            ses.describe_receipt_rule_set(RuleSetName=rule_set_name)

    def test_create_receipt_rule(self, ses):
        rule_set_name = f"test-rule-rs-{uuid.uuid4().hex[:8]}"
        ses.create_receipt_rule_set(RuleSetName=rule_set_name)
        response = ses.create_receipt_rule(
            RuleSetName=rule_set_name,
            Rule={
                "Name": "my-rule",
                "Enabled": True,
                "Recipients": ["inbox@example.com"],
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
        # Verify rule exists in rule set
        desc = ses.describe_receipt_rule_set(RuleSetName=rule_set_name)
        rule_names = [r["Name"] for r in desc["Rules"]]
        assert "my-rule" in rule_names
        # Cleanup
        ses.delete_receipt_rule(RuleSetName=rule_set_name, RuleName="my-rule")
        ses.delete_receipt_rule_set(RuleSetName=rule_set_name)

    def test_delete_receipt_rule(self, ses):
        rule_set_name = f"test-delrule-rs-{uuid.uuid4().hex[:8]}"
        ses.create_receipt_rule_set(RuleSetName=rule_set_name)
        ses.create_receipt_rule(
            RuleSetName=rule_set_name,
            Rule={
                "Name": "rule-to-delete",
                "Enabled": True,
                "Recipients": [],
                "Actions": [],
            },
        )
        response = ses.delete_receipt_rule(
            RuleSetName=rule_set_name, RuleName="rule-to-delete"
        )
        assert response["ResponseMetadata"]["HTTPStatusCode"] == 200
        desc = ses.describe_receipt_rule_set(RuleSetName=rule_set_name)
        rule_names = [r["Name"] for r in desc["Rules"]]
        assert "rule-to-delete" not in rule_names
        # Cleanup
        ses.delete_receipt_rule_set(RuleSetName=rule_set_name)

    def test_list_receipt_rule_sets(self, ses):
        rule_set_name = f"test-list-rs-{uuid.uuid4().hex[:8]}"
        ses.create_receipt_rule_set(RuleSetName=rule_set_name)
        response = ses.list_receipt_rule_sets()
        assert "RuleSets" in response
        names = [rs["Name"] for rs in response["RuleSets"]]
        assert rule_set_name in names
        # Cleanup
        ses.delete_receipt_rule_set(RuleSetName=rule_set_name)


class TestSESConfigurationSets:
    """Tests for configuration set management."""

    def test_create_configuration_set(self, ses):
        cs_name = f"test-cs-{uuid.uuid4().hex[:8]}"
        response = ses.create_configuration_set(
            ConfigurationSet={"Name": cs_name}
        )
        assert response["ResponseMetadata"]["HTTPStatusCode"] == 200
        # Cleanup
        ses.delete_configuration_set(ConfigurationSetName=cs_name)

    def test_describe_configuration_set(self, ses):
        cs_name = f"test-desc-cs-{uuid.uuid4().hex[:8]}"
        ses.create_configuration_set(ConfigurationSet={"Name": cs_name})
        response = ses.describe_configuration_set(
            ConfigurationSetName=cs_name,
            ConfigurationSetAttributeNames=["eventDestinations"],
        )
        assert response["ConfigurationSet"]["Name"] == cs_name
        assert "EventDestinations" in response
        # Cleanup
        ses.delete_configuration_set(ConfigurationSetName=cs_name)

    def test_delete_configuration_set(self, ses):
        cs_name = f"test-del-cs-{uuid.uuid4().hex[:8]}"
        ses.create_configuration_set(ConfigurationSet={"Name": cs_name})
        response = ses.delete_configuration_set(ConfigurationSetName=cs_name)
        assert response["ResponseMetadata"]["HTTPStatusCode"] == 200
        # Verify deletion
        with pytest.raises(ses.exceptions.ConfigurationSetDoesNotExistException):
            ses.describe_configuration_set(
                ConfigurationSetName=cs_name,
                ConfigurationSetAttributeNames=["eventDestinations"],
            )

    def test_list_configuration_sets(self, ses):
        cs_name = f"test-listcs-{uuid.uuid4().hex[:8]}"
        ses.create_configuration_set(ConfigurationSet={"Name": cs_name})
        response = ses.list_configuration_sets()
        assert "ConfigurationSets" in response
        names = [cs["Name"] for cs in response["ConfigurationSets"]]
        assert cs_name in names
        # Cleanup
        ses.delete_configuration_set(ConfigurationSetName=cs_name)


class TestSESTemplates:
    """Tests for email template management."""

    def test_create_template(self, ses):
        tpl_name = f"test-tpl-{uuid.uuid4().hex[:8]}"
        response = ses.create_template(
            Template={
                "TemplateName": tpl_name,
                "SubjectPart": "Hello {{name}}",
                "TextPart": "Dear {{name}}, welcome!",
                "HtmlPart": "<h1>Hello {{name}}</h1>",
            }
        )
        assert response["ResponseMetadata"]["HTTPStatusCode"] == 200
        # Cleanup
        ses.delete_template(TemplateName=tpl_name)

    def test_get_template(self, ses):
        tpl_name = f"test-get-tpl-{uuid.uuid4().hex[:8]}"
        ses.create_template(
            Template={
                "TemplateName": tpl_name,
                "SubjectPart": "Subject {{var}}",
                "TextPart": "Text {{var}}",
                "HtmlPart": "<p>{{var}}</p>",
            }
        )
        response = ses.get_template(TemplateName=tpl_name)
        template = response["Template"]
        assert template["TemplateName"] == tpl_name
        assert template["SubjectPart"] == "Subject {{var}}"
        assert template["TextPart"] == "Text {{var}}"
        assert template["HtmlPart"] == "<p>{{var}}</p>"
        # Cleanup
        ses.delete_template(TemplateName=tpl_name)

    def test_list_templates(self, ses):
        tpl_name = f"test-list-tpl-{uuid.uuid4().hex[:8]}"
        ses.create_template(
            Template={
                "TemplateName": tpl_name,
                "SubjectPart": "S",
                "TextPart": "T",
            }
        )
        response = ses.list_templates()
        assert "TemplatesMetadata" in response
        names = [t["Name"] for t in response["TemplatesMetadata"]]
        assert tpl_name in names
        # Cleanup
        ses.delete_template(TemplateName=tpl_name)

    def test_delete_template(self, ses):
        tpl_name = f"test-del-tpl-{uuid.uuid4().hex[:8]}"
        ses.create_template(
            Template={
                "TemplateName": tpl_name,
                "SubjectPart": "S",
                "TextPart": "T",
            }
        )
        response = ses.delete_template(TemplateName=tpl_name)
        assert response["ResponseMetadata"]["HTTPStatusCode"] == 200
        # Verify deletion
        with pytest.raises(ses.exceptions.TemplateDoesNotExistException):
            ses.get_template(TemplateName=tpl_name)

    def test_update_template(self, ses):
        tpl_name = f"test-upd-tpl-{uuid.uuid4().hex[:8]}"
        ses.create_template(
            Template={
                "TemplateName": tpl_name,
                "SubjectPart": "Original",
                "TextPart": "Original text",
            }
        )
        response = ses.update_template(
            Template={
                "TemplateName": tpl_name,
                "SubjectPart": "Updated",
                "TextPart": "Updated text",
                "HtmlPart": "<p>Updated</p>",
            }
        )
        assert response["ResponseMetadata"]["HTTPStatusCode"] == 200
        # Verify update
        got = ses.get_template(TemplateName=tpl_name)
        assert got["Template"]["SubjectPart"] == "Updated"
        assert got["Template"]["TextPart"] == "Updated text"
        # Cleanup
        ses.delete_template(TemplateName=tpl_name)
