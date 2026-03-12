"""
Tests for user login flows.

Covers: successful login, wrong password, login metrics, rate limiting,
account lockout, non-existent email.
"""

import pytest

from .app import AccountLockedError, InvalidCredentialsError


class TestLogin:
    def test_successful_login_creates_session(self, auth, registered_user):
        """A correct email/password creates a session in DynamoDB."""
        session = auth.login(
            email="testuser@example.com",
            password=registered_user["password"],
            ip_address="192.168.1.1",
        )
        assert session.session_id.startswith("sess-")
        assert session.user_id == registered_user["user"].user_id
        assert session.ip_address == "192.168.1.1"
        assert session.expires_at > 0

    def test_wrong_password_fails(self, auth, registered_user):
        """An incorrect password raises InvalidCredentialsError."""
        with pytest.raises(InvalidCredentialsError):
            auth.login(email="testuser@example.com", password="WrongP@ss999!")

    def test_login_metrics_published(self, auth, registered_user):
        """After a successful login, CloudWatch shows LoginSuccess >= 1."""
        auth.login(email="testuser@example.com", password=registered_user["password"])
        metrics = auth.get_login_metrics()
        assert metrics["LoginSuccess"] >= 1.0

    def test_failed_login_metrics_published(self, auth, registered_user):
        """After a failed login, CloudWatch shows LoginFailures >= 1."""
        with pytest.raises(InvalidCredentialsError):
            auth.login(email="testuser@example.com", password="Bad!Password1")
        metrics = auth.get_login_metrics()
        assert metrics["LoginFailures"] >= 1.0

    def test_rate_limiting_locks_account(self, auth, registered_user):
        """After max_failed_attempts, the account is locked."""
        # Config has max_failed_attempts=5
        for _ in range(5):
            with pytest.raises(InvalidCredentialsError):
                auth.login(email="testuser@example.com", password="Wrong!Pass123")

        # The 6th attempt should trigger lockout
        with pytest.raises(AccountLockedError):
            auth.login(email="testuser@example.com", password="Wrong!Pass123")

    def test_lockout_cleared_after_unlock(self, auth, registered_user):
        """An admin can unlock a locked account."""
        user_id = registered_user["user"].user_id

        # Lock the account
        for _ in range(5):
            with pytest.raises(InvalidCredentialsError):
                auth.login(email="testuser@example.com", password="Wrong!Pass123")
        with pytest.raises(AccountLockedError):
            auth.login(email="testuser@example.com", password="Wrong!Pass123")

        # Unlock
        auth.unlock_account(user_id)

        # Should be able to log in now
        session = auth.login(email="testuser@example.com", password=registered_user["password"])
        assert session.session_id.startswith("sess-")

    def test_nonexistent_email_fails(self, auth):
        """Login with an email that has no account raises InvalidCredentialsError."""
        with pytest.raises(InvalidCredentialsError):
            auth.login(email="ghost@example.com", password="Any!Pass123!")
