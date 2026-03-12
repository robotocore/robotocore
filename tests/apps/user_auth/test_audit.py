"""
Tests for audit logging via CloudWatch Logs.

Covers: login event, logout event, password change event,
failed login event, query audit logs by user.
"""

import pytest

from .app import InvalidCredentialsError


class TestAudit:
    def test_login_event_logged(self, auth, registered_user):
        """A successful login writes an audit event to CloudWatch Logs."""
        auth.login(email="testuser@example.com", password=registered_user["password"])
        events = auth.get_audit_events(user_id=registered_user["user"].user_id)
        # Should have at least registration + login events
        event_types = [e["event_type"] for e in events]
        assert "login" in event_types

    def test_logout_event_logged(self, auth, registered_user):
        """Revoking a session with user_id logs a logout audit event."""
        user_id = registered_user["user"].user_id
        session = auth.login(email="testuser@example.com", password=registered_user["password"])
        auth.revoke_session(session.session_id, user_id=user_id)

        events = auth.get_audit_events(user_id=user_id)
        event_types = [e["event_type"] for e in events]
        assert "logout" in event_types

    def test_password_change_logged(self, auth, registered_user):
        """Changing password writes an audit event."""
        user_id = registered_user["user"].user_id
        auth.change_password(user_id, registered_user["password"], "ChangedP@ss1!")

        events = auth.get_audit_events(user_id=user_id)
        event_types = [e["event_type"] for e in events]
        assert "password_changed" in event_types

    def test_failed_login_logged(self, auth, registered_user):
        """A failed login attempt writes an audit event."""
        user_id = registered_user["user"].user_id
        with pytest.raises(InvalidCredentialsError):
            auth.login(email="testuser@example.com", password="Wrong!Pass123")

        events = auth.get_audit_events(user_id=user_id)
        event_types = [e["event_type"] for e in events]
        assert "login_failed" in event_types

    def test_query_audit_by_user(self, auth):
        """Audit events can be filtered by user_id."""
        u1 = auth.register_user(email="audit1@example.com", password="Aud!tPass1#")
        u2 = auth.register_user(email="audit2@example.com", password="Aud!tPass2#")

        events_u1 = auth.get_audit_events(user_id=u1.user_id)
        events_u2 = auth.get_audit_events(user_id=u2.user_id)

        # Each user should only see their own registration event
        assert all(e["user_id"] == u1.user_id for e in events_u1)
        assert all(e["user_id"] == u2.user_id for e in events_u2)

    def test_registration_event_logged(self, auth, registered_user):
        """User registration writes an audit event."""
        events = auth.get_audit_events(user_id=registered_user["user"].user_id)
        event_types = [e["event_type"] for e in events]
        assert "registration" in event_types
