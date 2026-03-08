"""Compatibility tests for AWS CodeCommit service."""

import uuid

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

    def test_get_repository(self, codecommit):
        name = _unique("repo")
        codecommit.create_repository(repositoryName=name)
        resp = codecommit.get_repository(repositoryName=name)
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200
        assert resp["repositoryMetadata"]["repositoryName"] == name
        codecommit.delete_repository(repositoryName=name)

    def test_delete_repository(self, codecommit):
        name = _unique("repo")
        codecommit.create_repository(repositoryName=name)
        resp = codecommit.delete_repository(repositoryName=name)
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200
