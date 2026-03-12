"""
Tests for session management.

Covers: session creation with TTL, validation, expiration, revocation,
listing active sessions, multiple concurrent sessions.
"""

import time

import pytest

from .app import SessionExpiredError


class TestSessions:
    def test_create_session_stored_with_ttl(self, auth, registered_user):
        """A login creates a session that has an expires_at TTL field."""
        session = auth.login(email="testuser@example.com", password=registered_user["password"])
        # Validate the session is in DynamoDB
        validated = auth.validate_session(session.session_id)
        assert validated.user_id == registered_user["user"].user_id
        assert validated.expires_at > int(time.time())

    def test_validate_active_session(self, auth, registered_user):
        """An active (non-expired) session validates successfully."""
        session = auth.login(email="testuser@example.com", password=registered_user["password"])
        result = auth.validate_session(session.session_id)
        assert result.session_id == session.session_id
        assert result.user_id == session.user_id

    def test_expired_session_rejected(self, auth, dynamodb, sessions_table, registered_user):
        """A session with expires_at in the past is rejected."""
        # Create a session that's already expired
        session_id = "expired-sess-001"
        dynamodb.put_item(
            TableName=sessions_table,
            Item={
                "session_id": {"S": session_id},
                "user_id": {"S": registered_user["user"].user_id},
                "created_at": {"S": "2020-01-01T00:00:00Z"},
                "expires_at": {"N": "1"},  # epoch 1 = long expired
            },
        )
        with pytest.raises(SessionExpiredError):
            auth.validate_session(session_id)

    def test_revoke_session(self, auth, registered_user):
        """After revoking a session, validating it raises SessionExpiredError."""
        session = auth.login(email="testuser@example.com", password=registered_user["password"])
        auth.revoke_session(session.session_id)
        with pytest.raises(SessionExpiredError):
            auth.validate_session(session.session_id)

    def test_list_active_sessions(self, auth, registered_user):
        """Multiple logins create multiple sessions, all listed."""
        for _ in range(3):
            auth.login(email="testuser@example.com", password=registered_user["password"])

        sessions = auth.list_user_sessions(registered_user["user"].user_id)
        assert len(sessions) >= 3

    def test_multiple_concurrent_sessions(self, auth, registered_user):
        """Each login creates a distinct session_id."""
        s1 = auth.login(email="testuser@example.com", password=registered_user["password"])
        s2 = auth.login(email="testuser@example.com", password=registered_user["password"])
        assert s1.session_id != s2.session_id

        # Both are valid
        auth.validate_session(s1.session_id)
        auth.validate_session(s2.session_id)

    def test_nonexistent_session_rejected(self, auth):
        """Validating a session_id that doesn't exist raises SessionExpiredError."""
        with pytest.raises(SessionExpiredError):
            auth.validate_session("does-not-exist")
