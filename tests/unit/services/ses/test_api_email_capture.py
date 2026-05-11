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

    def _patch_store(self):
        return patch("robotocore.services.ses.provider.get_email_store", return_value=self.store)

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
