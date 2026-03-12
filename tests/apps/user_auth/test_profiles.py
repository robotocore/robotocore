"""
Tests for user profile CRUD operations.

Covers: get profile, update name/bio, update email (uniqueness enforced),
soft-delete, search by email (GSI), search by role, avatar upload/retrieval.
"""

import pytest
from botocore.exceptions import ClientError

from .app import DuplicateEmailError, UserNotFoundError


class TestProfiles:
    def test_get_user_profile(self, auth, registered_user):
        """Retrieve a user profile by user_id."""
        user = auth.get_user(registered_user["user"].user_id)
        assert user.email == "testuser@example.com"
        assert user.name == "Test User"
        assert user.bio == "A test user for auth tests"

    def test_update_name_and_bio(self, auth, registered_user):
        """Update name and bio, verify changes persisted."""
        user_id = registered_user["user"].user_id
        updated = auth.update_profile(user_id, name="New Name", bio="New bio text")
        assert updated.name == "New Name"
        assert updated.bio == "New bio text"
        assert updated.email == "testuser@example.com"  # unchanged

    def test_update_email_uniqueness_enforced(self, auth, registered_user):
        """Changing email to one already in use raises DuplicateEmailError."""
        auth.register_user(email="taken@example.com", password="Str0ng!Pass#99")
        with pytest.raises(DuplicateEmailError):
            auth.update_profile(registered_user["user"].user_id, email="taken@example.com")

    def test_update_email_to_own_email(self, auth, registered_user):
        """Updating email to the same value succeeds (no duplicate error)."""
        user_id = registered_user["user"].user_id
        updated = auth.update_profile(user_id, email="testuser@example.com")
        assert updated.email == "testuser@example.com"

    def test_soft_delete_user(self, auth, registered_user):
        """Soft-deleting sets status to 'deleted'."""
        user_id = registered_user["user"].user_id
        auth.soft_delete_user(user_id)
        user = auth.get_user(user_id)
        assert user.status == "deleted"

    def test_search_by_email_gsi(self, auth, registered_user):
        """Search by email returns the correct user via GSI."""
        results = auth.search_users_by_email("testuser@example.com")
        assert len(results) == 1
        assert results[0].user_id == registered_user["user"].user_id

    def test_search_by_role(self, auth):
        """Search by role returns users with that role."""
        auth.register_user(email="admin1@example.com", password="Admin!Pass1#", role="admin")
        auth.register_user(email="admin2@example.com", password="Admin!Pass2#", role="admin")
        auth.register_user(email="regular@example.com", password="Reg!Pass123#")

        admins = auth.search_users_by_role("admin")
        assert len(admins) == 2
        assert all(u.role == "admin" for u in admins)

    def test_avatar_upload_and_retrieval(self, auth, registered_user):
        """Upload an avatar and download it back."""
        user_id = registered_user["user"].user_id
        image_data = b"\x89PNG\r\n\x1a\nfake-avatar-bytes-here"
        key = auth.upload_avatar(user_id, image_data)
        assert "avatars/" in key

        downloaded = auth.get_avatar(user_id)
        assert downloaded == image_data

    def test_avatar_presigned_url(self, auth, registered_user):
        """Generate a presigned URL for an avatar."""
        user_id = registered_user["user"].user_id
        auth.upload_avatar(user_id, b"img-data")
        url = auth.get_avatar_presigned_url(user_id)
        assert isinstance(url, str)
        assert "avatars" in url

    def test_avatar_delete(self, auth, registered_user, s3, avatar_bucket):
        """Delete an avatar, verify it's gone from S3."""
        user_id = registered_user["user"].user_id
        auth.upload_avatar(user_id, b"to-delete")
        auth.delete_avatar(user_id)
        with pytest.raises(ClientError) as exc:
            s3.get_object(Bucket=avatar_bucket, Key=f"avatars/{user_id}/profile.jpg")
        assert exc.value.response["Error"]["Code"] == "NoSuchKey"

    def test_get_nonexistent_user(self, auth):
        """Getting a non-existent user raises UserNotFoundError."""
        with pytest.raises(UserNotFoundError):
            auth.get_user("nonexistent-user-id")
