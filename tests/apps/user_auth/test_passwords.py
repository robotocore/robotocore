"""
Tests for password management.

Covers: password policy enforcement, password change (requires old password),
password reset flow (generate token, verify, set new), token expiry, token reuse.
"""

import pytest

from .app import (
    InvalidCredentialsError,
    PasswordPolicyError,
    TokenExpiredError,
)


class TestPasswords:
    def test_password_policy_min_length(self, auth):
        """Password shorter than min_password_length is rejected at registration."""
        with pytest.raises(PasswordPolicyError, match="at least"):
            auth.register_user(email="short@example.com", password="Ab!1")

    def test_password_policy_special_chars(self, auth):
        """Password without special characters is rejected."""
        with pytest.raises(PasswordPolicyError, match="special character"):
            auth.register_user(email="nospecial@example.com", password="NoSpecialChars123")

    def test_change_password_success(self, auth, registered_user):
        """Change password with correct old password succeeds."""
        user_id = registered_user["user"].user_id
        auth.change_password(user_id, "SecureP@ss123!", "NewS3cure!Pass#")

        # Old password no longer works
        with pytest.raises(InvalidCredentialsError):
            auth.login(email="testuser@example.com", password="SecureP@ss123!")

        # New password works
        session = auth.login(email="testuser@example.com", password="NewS3cure!Pass#")
        assert session.session_id.startswith("sess-")

    def test_change_password_wrong_old(self, auth, registered_user):
        """Change password with wrong old password fails."""
        with pytest.raises(InvalidCredentialsError, match="incorrect"):
            auth.change_password(
                registered_user["user"].user_id,
                "Wrong!OldPass1",
                "NewS3cure!Pass#",
            )

    def test_change_password_revokes_sessions(self, auth, registered_user):
        """After changing password, all existing sessions are revoked."""
        user_id = registered_user["user"].user_id
        session = auth.login(email="testuser@example.com", password=registered_user["password"])

        auth.change_password(user_id, registered_user["password"], "Brand!New1Pass#")

        # Old session should be gone
        from .app import SessionExpiredError

        with pytest.raises(SessionExpiredError):
            auth.validate_session(session.session_id)

    def test_reset_token_flow(self, auth, registered_user):
        """Generate reset token -> verify -> reset password -> login with new."""
        token = auth.generate_reset_token("testuser@example.com")
        assert token.user_id == registered_user["user"].user_id
        assert token.used is False

        # Verify token is valid
        verified = auth.verify_reset_token(token.token)
        assert verified.token == token.token

        # Reset password
        auth.reset_password(token.token, "R3set!NewPass#9")

        # Login with new password
        session = auth.login(email="testuser@example.com", password="R3set!NewPass#9")
        assert session.session_id.startswith("sess-")

    def test_reset_token_expired(self, auth, registered_user, dynamodb, reset_tokens_table):
        """An expired reset token is rejected."""
        token = auth.generate_reset_token("testuser@example.com")

        # Manually set expires_at to the past
        dynamodb.update_item(
            TableName=reset_tokens_table,
            Key={"token": {"S": token.token}},
            UpdateExpression="SET expires_at = :exp",
            ExpressionAttributeValues={":exp": {"N": "1"}},
        )

        with pytest.raises(TokenExpiredError, match="expired"):
            auth.verify_reset_token(token.token)

    def test_used_token_cannot_be_reused(self, auth, registered_user):
        """A reset token that's been used cannot be used again."""
        token = auth.generate_reset_token("testuser@example.com")
        auth.reset_password(token.token, "First!Reset1#")

        with pytest.raises(TokenExpiredError, match="already been used"):
            auth.verify_reset_token(token.token)

    def test_reset_password_policy_enforced(self, auth, registered_user):
        """Password policy is enforced during password reset too."""
        token = auth.generate_reset_token("testuser@example.com")
        with pytest.raises(PasswordPolicyError):
            auth.reset_password(token.token, "weak")
