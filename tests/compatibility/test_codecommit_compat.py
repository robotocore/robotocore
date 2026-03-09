"""Compatibility tests for AWS CodeCommit service."""

import uuid

import botocore.exceptions
import pytest

from tests.compatibility.conftest import make_client

APPROVAL_RULE_CONTENT = (
    '{"Version": "2018-11-08", "Statements": [{"Type": "Approvers", "NumberOfApprovalsNeeded": 1}]}'
)
APPROVAL_RULE_CONTENT_2 = (
    '{"Version": "2018-11-08", "Statements": [{"Type": "Approvers", "NumberOfApprovalsNeeded": 2}]}'
)


@pytest.fixture
def codecommit():
    return make_client("codecommit")


def _unique(prefix):
    return f"{prefix}-{uuid.uuid4().hex[:8]}"


def _create_repo_with_commit(cc, name=None):
    """Helper: create a repo with an initial commit on 'main'. Returns (name, commit_id)."""
    if name is None:
        name = _unique("repo")
    cc.create_repository(repositoryName=name)
    resp = cc.create_commit(
        repositoryName=name,
        branchName="main",
        putFiles=[{"filePath": "README.md", "fileContent": b"Hello World"}],
        authorName="test",
        email="test@test.com",
        commitMessage="Initial commit",
    )
    return name, resp["commitId"]


def _create_repo_with_feature_branch(cc, name=None):
    """Create repo with main + feature branch."""
    name, main_commit = _create_repo_with_commit(cc, name)
    cc.create_branch(repositoryName=name, branchName="feature", commitId=main_commit)
    resp = cc.create_commit(
        repositoryName=name,
        branchName="feature",
        putFiles=[{"filePath": "feature.txt", "fileContent": b"Feature work"}],
        authorName="test",
        email="test@test.com",
        commitMessage="Feature commit",
        parentCommitId=main_commit,
    )
    return name, main_commit, resp["commitId"]


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

    def test_list_repositories(self, codecommit):
        """ListRepositories returns created repos."""
        name = _unique("repo")
        try:
            codecommit.create_repository(repositoryName=name)
            resp = codecommit.list_repositories()
            assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200
            assert "repositories" in resp
            names = [r["repositoryName"] for r in resp["repositories"]]
            assert name in names
        finally:
            codecommit.delete_repository(repositoryName=name)

    def test_batch_get_repositories(self, codecommit):
        """BatchGetRepositories returns metadata for requested repos."""
        name1 = _unique("repo")
        name2 = _unique("repo")
        try:
            codecommit.create_repository(repositoryName=name1)
            codecommit.create_repository(repositoryName=name2)
            resp = codecommit.batch_get_repositories(repositoryNames=[name1, name2])
            assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200
            assert len(resp["repositories"]) == 2
            returned_names = {r["repositoryName"] for r in resp["repositories"]}
            assert name1 in returned_names
            assert name2 in returned_names
        finally:
            codecommit.delete_repository(repositoryName=name1)
            codecommit.delete_repository(repositoryName=name2)

    def test_batch_get_repositories_not_found(self, codecommit):
        """BatchGetRepositories returns repositoriesNotFound for missing repos."""
        resp = codecommit.batch_get_repositories(repositoryNames=[_unique("nonexistent")])
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200
        assert len(resp["repositoriesNotFound"]) == 1

    def test_update_repository_description(self, codecommit):
        """UpdateRepositoryDescription changes the description."""
        name = _unique("repo")
        try:
            codecommit.create_repository(repositoryName=name)
            codecommit.update_repository_description(
                repositoryName=name, repositoryDescription="new desc"
            )
            resp = codecommit.get_repository(repositoryName=name)
            assert resp["repositoryMetadata"]["repositoryDescription"] == "new desc"
        finally:
            codecommit.delete_repository(repositoryName=name)

    def test_update_repository_name(self, codecommit):
        """UpdateRepositoryName renames the repository."""
        old_name = _unique("repo")
        new_name = _unique("repo-renamed")
        try:
            codecommit.create_repository(repositoryName=old_name)
            codecommit.update_repository_name(oldName=old_name, newName=new_name)
            resp = codecommit.get_repository(repositoryName=new_name)
            assert resp["repositoryMetadata"]["repositoryName"] == new_name
            with pytest.raises(botocore.exceptions.ClientError):
                codecommit.get_repository(repositoryName=old_name)
        finally:
            codecommit.delete_repository(repositoryName=new_name)

    def test_update_repository_encryption_key(self, codecommit):
        """UpdateRepositoryEncryptionKey completes without error."""
        name = _unique("repo")
        try:
            codecommit.create_repository(repositoryName=name)
            resp = codecommit.update_repository_encryption_key(
                repositoryName=name,
                kmsKeyId="arn:aws:kms:us-east-1:123456789012:key/12345678-1234-1234-1234-123456789012",
            )
            assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200
        finally:
            codecommit.delete_repository(repositoryName=name)
