"""Unit tests for SES API email capture (issue #222).

Verifies that emails sent via the SES v1 and v2 APIs are captured
into the EmailStore and appear at /_robotocore/ses/messages.
"""

import base64
from unittest.mock import patch

from robotocore.services.ses.email_store import EmailStore, StoredEmail

# ---------------------------------------------------------------------------
# StoredEmail.source field
# ---------------------------------------------------------------------------


class TestStoredEmailSource:
    def test_default_source_is_smtp(self):
        msg = StoredEmail(sender="a@b.com", recipients=[], subject="s", body="b", raw="")
        assert msg.source == "smtp"

    def test_source_api_stored_and_returned(self):
        msg = StoredEmail(
            sender="a@b.com", recipients=[], subject="s", body="b", raw="", source="api"
        )
        d = msg.to_dict()
        assert d["source"] == "api"

    def test_source_smtp_returned_in_dict(self):
        msg = StoredEmail(sender="a@b.com", recipients=[], subject="s", body="b", raw="")
        d = msg.to_dict()
        assert d["source"] == "smtp"


# ---------------------------------------------------------------------------
# EmailStore.add_message with source
# ---------------------------------------------------------------------------


class TestEmailStoreSource:
    def test_add_api_message(self):
        store = EmailStore()
        store.add_message(
            sender="from@x.com",
            recipients=["to@y.com"],
            subject="API email",
            body="hello",
            raw="",
            source="api",
        )
        msgs = store.get_messages()
        assert len(msgs) == 1
        assert msgs[0]["source"] == "api"
        assert msgs[0]["subject"] == "API email"

    def test_add_smtp_message_default_source(self):
        store = EmailStore()
        store.add_message(
            sender="from@x.com",
            recipients=["to@y.com"],
            subject="SMTP email",
            body="hello",
            raw="raw data",
        )
        msgs = store.get_messages()
        assert msgs[0]["source"] == "smtp"

    def test_mixed_sources_both_appear(self):
        store = EmailStore()
        store.add_message("a@b.com", ["c@d.com"], "SMTP", "body", "raw")
        store.add_message("e@f.com", ["g@h.com"], "API", "body", "", source="api")
        msgs = store.get_messages()
        assert len(msgs) == 2
        sources = {m["source"] for m in msgs}
        assert sources == {"smtp", "api"}


# ---------------------------------------------------------------------------
# SES v1 provider capture helpers
# ---------------------------------------------------------------------------


class TestSesV1Capture:
    """Test the capture helper functions in the SES v1 provider."""

    def setup_method(self):
        self.store = EmailStore()

    def test_capture_send_email(self):
        from robotocore.services.ses.provider import _capture_send_email

        params = {
            "Source": "sender@example.com",
            "Destination.ToAddresses.member.1": "to1@example.com",
            "Destination.ToAddresses.member.2": "to2@example.com",
            "Destination.CcAddresses.member.1": "cc@example.com",
            "Message.Subject.Data": "Test subject",
            "Message.Body.Text.Data": "Test body",
        }
        _capture_send_email(params, self.store)

        msgs = self.store.get_messages()
        assert len(msgs) == 1
        m = msgs[0]
        assert m["sender"] == "sender@example.com"
        assert "to1@example.com" in m["recipients"]
        assert "to2@example.com" in m["recipients"]
        assert "cc@example.com" in m["recipients"]
        assert m["subject"] == "Test subject"
        assert m["body"] == "Test body"
        assert m["source"] == "api"

    def test_capture_send_email_html_fallback(self):
        from robotocore.services.ses.provider import _capture_send_email

        params = {
            "Source": "s@x.com",
            "Destination.ToAddresses.member.1": "t@x.com",
            "Message.Subject.Data": "HTML email",
            "Message.Body.Html.Data": "<p>HTML content</p>",
        }
        _capture_send_email(params, self.store)
        msgs = self.store.get_messages()
        assert msgs[0]["body"] == "<p>HTML content</p>"

    def test_capture_send_raw_email(self):
        from robotocore.services.ses.provider import _capture_send_raw_email

        raw = "From: from@x.com\r\nTo: to@x.com\r\nSubject: Raw subject\r\n\r\nRaw body"
        encoded = base64.b64encode(raw.encode()).decode()

        params = {
            "Source": "from@x.com",
            "Destinations.member.1": "to@x.com",
            "RawMessage.Data": encoded,
        }
        _capture_send_raw_email(params, self.store)

        msgs = self.store.get_messages()
        assert len(msgs) == 1
        m = msgs[0]
        assert m["sender"] == "from@x.com"
        assert "to@x.com" in m["recipients"]
        assert m["subject"] == "Raw subject"
        assert "Raw body" in m["body"]
        assert m["source"] == "api"

    def test_capture_send_raw_email_parses_headers_when_no_source(self):
        """When Source param is absent, extract sender/recipients from email headers."""
        from robotocore.services.ses.provider import _capture_send_raw_email

        raw = "From: auto@x.com\r\nTo: dest@y.com\r\nSubject: Auto\r\n\r\nBody"
        encoded = base64.b64encode(raw.encode()).decode()

        params = {"RawMessage.Data": encoded}
        _capture_send_raw_email(params, self.store)

        msgs = self.store.get_messages()
        assert msgs[0]["sender"] == "auto@x.com"
        assert "dest@y.com" in msgs[0]["recipients"]

    def test_capture_send_raw_email_strips_display_names(self):
        """Display names in From/To headers should not bleed into sender/recipients."""
        from robotocore.services.ses.provider import _capture_send_raw_email

        raw = (
            'From: "Jane Doe" <jane@x.com>\r\n'
            'To: "Bob Smith" <bob@y.com>, carol@z.com\r\n'
            "Subject: Display names\r\n\r\n"
            "Hello"
        )
        encoded = base64.b64encode(raw.encode()).decode()
        params = {"RawMessage.Data": encoded}
        _capture_send_raw_email(params, self.store)

        msgs = self.store.get_messages()
        assert msgs[0]["sender"] == "jane@x.com"
        assert "bob@y.com" in msgs[0]["recipients"]
        assert "carol@z.com" in msgs[0]["recipients"]
        # Display names must not appear in recipient list
        assert not any("Bob Smith" in r or "carol" == r for r in msgs[0]["recipients"])

    def test_capture_send_raw_email_multipart_body(self):
        """Multipart MIME: extract text/plain part, ignore html part."""
        from robotocore.services.ses.provider import _capture_send_raw_email

        raw = (
            "From: s@x.com\r\nTo: r@y.com\r\nSubject: Multipart\r\n"
            'MIME-Version: 1.0\r\nContent-Type: multipart/alternative; boundary="bound"\r\n\r\n'
            "--bound\r\nContent-Type: text/plain\r\n\r\nPlain text part\r\n"
            "--bound\r\nContent-Type: text/html\r\n\r\n<p>HTML part</p>\r\n"
            "--bound--"
        )
        encoded = base64.b64encode(raw.encode()).decode()
        params = {"Source": "s@x.com", "RawMessage.Data": encoded}
        _capture_send_raw_email(params, self.store)

        msgs = self.store.get_messages()
        assert "Plain text part" in msgs[0]["body"]
        assert "<p>" not in msgs[0]["body"]

    def test_capture_bulk_templated_email(self):
        """SendBulkTemplatedEmail: collect recipients from all Destinations entries."""
        from robotocore.services.ses.provider import _capture_send_templated_email

        params = {
            "Source": "bulk@x.com",
            "Template": "BulkTemplate",
            "Destinations.member.1.Destination.ToAddresses.member.1": "a@y.com",
            "Destinations.member.1.Destination.ToAddresses.member.2": "b@y.com",
            "Destinations.member.2.Destination.ToAddresses.member.1": "c@y.com",
        }
        _capture_send_templated_email(params, self.store)

        msgs = self.store.get_messages()
        assert len(msgs) == 1
        m = msgs[0]
        assert m["sender"] == "bulk@x.com"
        assert m["subject"] == "[template: BulkTemplate]"
        assert "a@y.com" in m["recipients"]
        assert "b@y.com" in m["recipients"]
        assert "c@y.com" in m["recipients"]

    def test_capture_templated_email(self):
        from robotocore.services.ses.provider import _capture_send_templated_email

        params = {
            "Source": "noreply@x.com",
            "Destination.ToAddresses.member.1": "user@y.com",
            "Template": "MyTemplate",
        }
        _capture_send_templated_email(params, self.store)

        msgs = self.store.get_messages()
        assert msgs[0]["sender"] == "noreply@x.com"
        assert msgs[0]["subject"] == "[template: MyTemplate]"
        assert msgs[0]["source"] == "api"

    def test_collect_indexed_params(self):
        from robotocore.services.ses.provider import _collect_indexed_params

        params = {
            "Destination.ToAddresses.member.1": "a@b.com",
            "Destination.ToAddresses.member.2": "c@d.com",
            "Destination.ToAddresses.member.3": "e@f.com",
        }
        result = _collect_indexed_params(params, "Destination.ToAddresses")
        assert result == ["a@b.com", "c@d.com", "e@f.com"]

    def test_collect_indexed_params_empty(self):
        from robotocore.services.ses.provider import _collect_indexed_params

        result = _collect_indexed_params({}, "SomeParam")
        assert result == []


# ---------------------------------------------------------------------------
# SES v2 provider capture helpers
# ---------------------------------------------------------------------------


class TestSesV2Capture:
    """Test the capture helper function in the SES v2 provider."""

    def setup_method(self):
        self.store = EmailStore()

    def test_capture_simple_email(self):
        from robotocore.services.ses.sesv2_provider import _capture_sesv2_send_email

        body = {
            "FromEmailAddress": "sender@x.com",
            "Destination": {
                "ToAddresses": ["to@y.com"],
                "CcAddresses": ["cc@y.com"],
            },
            "Content": {
                "Simple": {
                    "Subject": {"Data": "V2 subject"},
                    "Body": {
                        "Text": {"Data": "V2 text body"},
                        "Html": {"Data": "<p>V2 html</p>"},
                    },
                }
            },
        }
        with patch(
            "robotocore.services.ses.sesv2_provider.get_email_store", return_value=self.store
        ):
            _capture_sesv2_send_email(body)

        msgs = self.store.get_messages()
        assert len(msgs) == 1
        m = msgs[0]
        assert m["sender"] == "sender@x.com"
        assert "to@y.com" in m["recipients"]
        assert "cc@y.com" in m["recipients"]
        assert m["subject"] == "V2 subject"
        assert m["body"] == "V2 text body"  # text preferred over html
        assert m["source"] == "api"

    def test_capture_template_email(self):
        from robotocore.services.ses.sesv2_provider import _capture_sesv2_send_email

        body = {
            "FromEmailAddress": "noreply@x.com",
            "Destination": {"ToAddresses": ["user@y.com"]},
            "Content": {
                "Template": {
                    "TemplateName": "WelcomeTemplate",
                    "TemplateData": '{"name":"Alice"}',
                }
            },
        }
        with patch(
            "robotocore.services.ses.sesv2_provider.get_email_store", return_value=self.store
        ):
            _capture_sesv2_send_email(body)

        msgs = self.store.get_messages()
        assert msgs[0]["subject"] == "[template: WelcomeTemplate]"

    def test_capture_missing_fields_does_not_raise(self):
        from robotocore.services.ses.sesv2_provider import _capture_sesv2_send_email

        with patch(
            "robotocore.services.ses.sesv2_provider.get_email_store", return_value=self.store
        ):
            _capture_sesv2_send_email({})  # empty body, should not raise

        msgs = self.store.get_messages()
        assert len(msgs) == 1
        assert msgs[0]["sender"] == ""
