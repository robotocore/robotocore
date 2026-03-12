"""
Tests for user registration flows.

Covers: profile creation in DynamoDB, email uniqueness via GSI,
password hashing, optional fields, default role, email validation.
"""

import pytest

from .app import DuplicateEmailError, InvalidEmailError, PasswordPolicyError


class TestRegistration:
    def test_register_user_creates_profile(self, auth):
        """Register a user and verify the profile is stored in DynamoDB."""
        user = auth.register_user(
            email="alice@example.com",
            password="Str0ng!Pass#99",
            name="Alice Johnson",
        )
        assert user.email == "alice@example.com"
        assert user.name == "Alice Johnson"
        assert user.status == "active"
        assert user.user_id.startswith("user-")

        # Verify retrievable
        fetched = auth.get_user(user.user_id)
        assert fetched.email == "alice@example.com"
        assert fetched.name == "Alice Johnson"

    def test_duplicate_email_rejected(self, auth):
        """Registering with an already-used email raises DuplicateEmailError."""
        auth.register_user(email="dup@example.com", password="Str0ng!Pass#99")
        with pytest.raises(DuplicateEmailError):
            auth.register_user(email="dup@example.com", password="An0ther!Pass#77")

    def test_password_not_stored_plaintext(self, auth):
        """The stored password_hash is NOT the plaintext password."""
        password = "MyS3cret!Pass"
        user = auth.register_user(email="hash@example.com", password=password)
        fetched = auth.get_user(user.user_id)
        assert fetched.password_hash != password
        assert len(fetched.password_hash) == 64  # SHA-256 hex digest
        assert fetched.salt != ""

    def test_registration_with_all_optional_fields(self, auth):
        """Register with name, bio, and role all provided."""
        user = auth.register_user(
            email="full@example.com",
            password="Full!Profile#1",
            name="Full User",
            bio="I have a bio",
            role="admin",
        )
        assert user.name == "Full User"
        assert user.bio == "I have a bio"
        assert user.role == "admin"

    def test_default_role_is_user(self, auth):
        """Without specifying role, a new user gets role='user'."""
        user = auth.register_user(email="default@example.com", password="Def@ultPass1!")
        assert user.role == "user"

    def test_invalid_email_rejected(self, auth):
        """Malformed emails raise InvalidEmailError."""
        with pytest.raises(InvalidEmailError):
            auth.register_user(email="not-an-email", password="Str0ng!Pass#99")

    def test_invalid_email_no_domain(self, auth):
        """Email without domain part is rejected."""
        with pytest.raises(InvalidEmailError):
            auth.register_user(email="user@", password="Str0ng!Pass#99")

    def test_weak_password_rejected(self, auth):
        """Password shorter than min_password_length is rejected."""
        with pytest.raises(PasswordPolicyError, match="at least"):
            auth.register_user(email="weak@example.com", password="Short1!")
