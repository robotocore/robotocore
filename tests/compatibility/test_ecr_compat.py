"""ECR (Elastic Container Registry) compatibility tests."""

import json
import uuid

import pytest

from tests.compatibility.conftest import make_client


@pytest.fixture
def ecr():
    return make_client("ecr")


def _unique(prefix: str) -> str:
    return f"{prefix}-{uuid.uuid4().hex[:8]}"


class TestECRRepositoryOperations:
    def test_create_repository(self, ecr):
        repo_name = _unique("test-repo")
        response = ecr.create_repository(repositoryName=repo_name)
        repo = response["repository"]
        assert repo["repositoryName"] == repo_name
        assert "repositoryArn" in repo
        assert "repositoryUri" in repo
        ecr.delete_repository(repositoryName=repo_name)

    def test_describe_repositories(self, ecr):
        repo_name = _unique("describe-repo")
        ecr.create_repository(repositoryName=repo_name)
        try:
            response = ecr.describe_repositories()
            names = [r["repositoryName"] for r in response["repositories"]]
            assert repo_name in names
        finally:
            ecr.delete_repository(repositoryName=repo_name)

    def test_describe_repositories_by_name(self, ecr):
        repo_name = _unique("byname-repo")
        ecr.create_repository(repositoryName=repo_name)
        try:
            response = ecr.describe_repositories(repositoryNames=[repo_name])
            assert len(response["repositories"]) == 1
            assert response["repositories"][0]["repositoryName"] == repo_name
        finally:
            ecr.delete_repository(repositoryName=repo_name)

    def test_put_lifecycle_policy(self, ecr):
        repo_name = _unique("lifecycle-repo")
        ecr.create_repository(repositoryName=repo_name)
        try:
            policy = {
                "rules": [
                    {
                        "rulePriority": 1,
                        "description": "Expire old images",
                        "selection": {
                            "tagStatus": "untagged",
                            "countType": "sinceImagePushed",
                            "countUnit": "days",
                            "countNumber": 14,
                        },
                        "action": {"type": "expire"},
                    }
                ]
            }
            response = ecr.put_lifecycle_policy(
                repositoryName=repo_name,
                lifecyclePolicyText=json.dumps(policy),
            )
            assert response["repositoryName"] == repo_name
            assert "lifecyclePolicyText" in response
        finally:
            ecr.delete_repository(repositoryName=repo_name)

    def test_get_lifecycle_policy(self, ecr):
        repo_name = _unique("getlc-repo")
        ecr.create_repository(repositoryName=repo_name)
        policy = {
            "rules": [
                {
                    "rulePriority": 1,
                    "description": "Keep last 10",
                    "selection": {
                        "tagStatus": "any",
                        "countType": "imageCountMoreThan",
                        "countNumber": 10,
                    },
                    "action": {"type": "expire"},
                }
            ]
        }
        try:
            ecr.put_lifecycle_policy(
                repositoryName=repo_name,
                lifecyclePolicyText=json.dumps(policy),
            )
            response = ecr.get_lifecycle_policy(repositoryName=repo_name)
            assert response["repositoryName"] == repo_name
            returned_policy = json.loads(response["lifecyclePolicyText"])
            assert len(returned_policy["rules"]) == 1
        finally:
            ecr.delete_repository(repositoryName=repo_name)

    def test_delete_repository(self, ecr):
        repo_name = _unique("del-repo")
        ecr.create_repository(repositoryName=repo_name)
        ecr.delete_repository(repositoryName=repo_name)
        response = ecr.describe_repositories()
        names = [r["repositoryName"] for r in response["repositories"]]
        assert repo_name not in names

    def test_set_repository_policy(self, ecr):
        repo_name = _unique("policy-repo")
        ecr.create_repository(repositoryName=repo_name)
        try:
            policy = json.dumps(
                {
                    "Version": "2012-10-17",
                    "Statement": [
                        {
                            "Sid": "AllowPull",
                            "Effect": "Allow",
                            "Principal": {"AWS": "arn:aws:iam::123456789012:root"},
                            "Action": [
                                "ecr:GetDownloadUrlForLayer",
                                "ecr:BatchGetImage",
                            ],
                        }
                    ],
                }
            )
            response = ecr.set_repository_policy(
                repositoryName=repo_name,
                policyText=policy,
            )
            assert response["repositoryName"] == repo_name
        finally:
            ecr.delete_repository(repositoryName=repo_name)
