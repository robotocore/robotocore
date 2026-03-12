"""
Tests for template management: upload, retrieve, render, update, and list
notification templates stored in S3.
"""

import pytest

from .models import Channel, Template


class TestTemplateUpload:
    def test_upload_template(self, notifier, sample_template):
        """Uploading a template stores it in S3 and it can be retrieved."""
        loaded = notifier.get_template(Channel.EMAIL, "welcome-email")
        assert loaded.template_id == "welcome-email"
        assert loaded.name == "Welcome Email"
        assert loaded.channel == Channel.EMAIL
        assert "{{name}}" in loaded.body

    def test_upload_sms_template(self, notifier, sms_template):
        """SMS template stored and retrievable."""
        loaded = notifier.get_template(Channel.SMS, "verify-sms")
        assert loaded.template_id == "verify-sms"
        assert loaded.channel == Channel.SMS
        assert "{{code}}" in loaded.body


class TestTemplateRendering:
    def test_render_template(self, notifier, sample_template):
        """Rendering with all variables produces expected output."""
        subject, body = notifier.render_template(
            Channel.EMAIL, "welcome-email", {"name": "Alice", "company": "Acme"}
        )
        assert subject == "Welcome, Alice!"
        assert "Alice" in body
        assert "Acme" in body
        assert "{{name}}" not in body
        assert "{{company}}" not in body

    def test_render_missing_variable_raises(self, notifier, sample_template):
        """Rendering with a missing variable raises ValueError."""
        with pytest.raises(ValueError, match="Missing template variables"):
            notifier.render_template(Channel.EMAIL, "welcome-email", {"name": "Bob"})

    def test_render_sms_template(self, notifier, sms_template):
        """SMS template renders with variables."""
        subject, body = notifier.render_template(
            Channel.SMS, "verify-sms", {"code": "123456", "minutes": "5"}
        )
        assert "123456" in body
        assert "5 minutes" in body


class TestTemplateUpdate:
    def test_update_template(self, notifier, sample_template):
        """Updating a template overwrites in S3."""
        updated = Template(
            template_id="welcome-email",
            name="Welcome Email v2",
            channel=Channel.EMAIL,
            subject="Hey {{name}}!",
            body="Hey {{name}}, welcome aboard at {{company}}!",
            variables=["name", "company"],
        )
        notifier.update_template(updated)

        loaded = notifier.get_template(Channel.EMAIL, "welcome-email")
        assert loaded.name == "Welcome Email v2"
        assert loaded.subject == "Hey {{name}}!"


class TestTemplateList:
    def test_list_templates(self, notifier, sample_template, sms_template):
        """Listing templates returns all uploaded templates."""
        all_keys = notifier.list_templates()
        assert len(all_keys) >= 2

    def test_list_templates_by_channel(self, notifier, sample_template, sms_template):
        """Listing templates with channel filter returns only matching."""
        email_keys = notifier.list_templates(channel=Channel.EMAIL)
        sms_keys = notifier.list_templates(channel=Channel.SMS)
        assert all("email" in k for k in email_keys)
        assert all("sms" in k for k in sms_keys)
        assert len(email_keys) >= 1
        assert len(sms_keys) >= 1
