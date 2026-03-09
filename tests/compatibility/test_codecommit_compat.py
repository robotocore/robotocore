"""Compatibility tests for AWS CodeCommit service."""

import uuid

import botocore.exceptions
import pytest

from tests.compatibility.conftest import make_client


@pytest.fixture
def codecommit():
    return make_client("codecommit")


def _unique(prefix):
    return f"{prefix}-{uuid.uuid4().hex[:8]}"


class TestCodeCommitRepositoryOperations:
    """Tests for CodeCommit repository CRUD operations."""

    def test_create_repository(self, codecommit):
        name = _unique("repo")
        resp = codecommit.create_repository(
            repositoryName=name,
            repositoryDescription="Test repo",
        )
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200
        meta = resp["repositoryMetadata"]
        assert meta["repositoryName"] == name
        assert "repositoryId" in meta
        assert "Arn" in meta
        codecommit.delete_repository(repositoryName=name)

    def test_create_repository_response_fields(self, codecommit):
        """Verify all expected fields in create_repository response."""
        name = _unique("repo")
        try:
            resp = codecommit.create_repository(
                repositoryName=name,
                repositoryDescription="detailed field check",
            )
            meta = resp["repositoryMetadata"]
            assert meta["repositoryName"] == name
            assert meta["repositoryDescription"] == "detailed field check"
            assert meta["accountId"] == "123456789012"
            assert name in meta["cloneUrlHttp"]
            assert name in meta["cloneUrlSsh"]
            assert "codecommit" in meta["Arn"]
            assert "repositoryId" in meta
            assert "creationDate" in meta
            assert "lastModifiedDate" in meta
        finally:
            codecommit.delete_repository(repositoryName=name)

    def test_create_repository_duplicate_name(self, codecommit):
        """Creating a repo with a duplicate name raises RepositoryNameExistsException."""
        name = _unique("repo")
        try:
            codecommit.create_repository(repositoryName=name)
            with pytest.raises(botocore.exceptions.ClientError) as exc_info:
                codecommit.create_repository(repositoryName=name)
            assert "RepositoryNameExistsException" in str(exc_info.value)
        finally:
            codecommit.delete_repository(repositoryName=name)

    def test_get_repository(self, codecommit):
        name = _unique("repo")
        codecommit.create_repository(repositoryName=name)
        resp = codecommit.get_repository(repositoryName=name)
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200
        assert resp["repositoryMetadata"]["repositoryName"] == name
        codecommit.delete_repository(repositoryName=name)

    def test_get_repository_nonexistent(self, codecommit):
        """Getting a nonexistent repo raises RepositoryDoesNotExistException."""
        with pytest.raises(botocore.exceptions.ClientError) as exc_info:
            codecommit.get_repository(repositoryName=_unique("nonexistent"))
        assert "RepositoryDoesNotExistException" in str(exc_info.value)

    def test_get_repository_clone_urls(self, codecommit):
        """Verify clone URLs contain correct region and repo name."""
        name = _unique("repo")
        try:
            codecommit.create_repository(repositoryName=name)
            resp = codecommit.get_repository(repositoryName=name)
            meta = resp["repositoryMetadata"]
            assert "us-east-1" in meta["cloneUrlHttp"]
            assert name in meta["cloneUrlHttp"]
            assert "us-east-1" in meta["cloneUrlSsh"]
            assert name in meta["cloneUrlSsh"]
        finally:
            codecommit.delete_repository(repositoryName=name)

    def test_get_repository_arn_format(self, codecommit):
        """Verify ARN follows expected format."""
        name = _unique("repo")
        try:
            codecommit.create_repository(repositoryName=name)
            resp = codecommit.get_repository(repositoryName=name)
            arn = resp["repositoryMetadata"]["Arn"]
            assert arn.startswith("arn:aws:codecommit:us-east-1:123456789012:")
            assert arn.endswith(name)
        finally:
            codecommit.delete_repository(repositoryName=name)

    def test_delete_repository(self, codecommit):
        name = _unique("repo")
        codecommit.create_repository(repositoryName=name)
        resp = codecommit.delete_repository(repositoryName=name)
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200

    def test_delete_repository_then_get(self, codecommit):
        """After deleting a repo, getting it should raise an error."""
        name = _unique("repo")
        codecommit.create_repository(repositoryName=name)
        codecommit.delete_repository(repositoryName=name)
        with pytest.raises(botocore.exceptions.ClientError) as exc_info:
            codecommit.get_repository(repositoryName=name)
        assert "RepositoryDoesNotExistException" in str(exc_info.value)

    def test_create_repository_no_description(self, codecommit):
        """Creating a repo without a description should work."""
        name = _unique("repo")
        try:
            resp = codecommit.create_repository(repositoryName=name)
            assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200
            assert resp["repositoryMetadata"]["repositoryName"] == name
        finally:
            codecommit.delete_repository(repositoryName=name)

    def test_create_multiple_repositories(self, codecommit):
        """Create multiple repos and verify each has a unique repositoryId."""
        name1 = _unique("repo")
        name2 = _unique("repo")
        try:
            r1 = codecommit.create_repository(repositoryName=name1)
            r2 = codecommit.create_repository(repositoryName=name2)
            id1 = r1["repositoryMetadata"]["repositoryId"]
            id2 = r2["repositoryMetadata"]["repositoryId"]
            assert id1 != id2
            assert r1["repositoryMetadata"]["repositoryName"] == name1
            assert r2["repositoryMetadata"]["repositoryName"] == name2
        finally:
            codecommit.delete_repository(repositoryName=name1)
            codecommit.delete_repository(repositoryName=name2)
