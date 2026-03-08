"""ECR (Elastic Container Registry) compatibility tests."""

import json
import uuid

import pytest
from botocore.exceptions import ClientError

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


class TestECRRepositoryPolicy:
    def test_get_repository_policy(self, ecr):
        repo_name = _unique("getpol-repo")
        ecr.create_repository(repositoryName=repo_name)
        try:
            policy = json.dumps(
                {
                    "Version": "2012-10-17",
                    "Statement": [
                        {
                            "Sid": "ReadOnly",
                            "Effect": "Allow",
                            "Principal": {"AWS": "arn:aws:iam::123456789012:root"},
                            "Action": ["ecr:GetDownloadUrlForLayer", "ecr:BatchGetImage"],
                        }
                    ],
                }
            )
            ecr.set_repository_policy(repositoryName=repo_name, policyText=policy)
            response = ecr.get_repository_policy(repositoryName=repo_name)
            assert response["repositoryName"] == repo_name
            returned = json.loads(response["policyText"])
            assert returned["Version"] == "2012-10-17"
        finally:
            ecr.delete_repository(repositoryName=repo_name)

    def test_delete_repository_policy(self, ecr):
        repo_name = _unique("delpol-repo")
        ecr.create_repository(repositoryName=repo_name)
        try:
            policy = json.dumps(
                {
                    "Version": "2012-10-17",
                    "Statement": [
                        {
                            "Sid": "Test",
                            "Effect": "Allow",
                            "Principal": "*",
                            "Action": ["ecr:GetDownloadUrlForLayer"],
                        }
                    ],
                }
            )
            ecr.set_repository_policy(repositoryName=repo_name, policyText=policy)
            ecr.delete_repository_policy(repositoryName=repo_name)
            with pytest.raises(ClientError) as exc_info:
                ecr.get_repository_policy(repositoryName=repo_name)
            assert "RepositoryPolicyNotFoundException" in str(exc_info.value)
        finally:
            ecr.delete_repository(repositoryName=repo_name)


class TestECRDescribeImages:
    def test_describe_images_empty_repo(self, ecr):
        repo_name = _unique("empty-repo")
        ecr.create_repository(repositoryName=repo_name)
        try:
            response = ecr.describe_images(repositoryName=repo_name)
            assert response["imageDetails"] == []
        finally:
            ecr.delete_repository(repositoryName=repo_name)


class TestECRLifecyclePolicyErrors:
    def test_get_lifecycle_policy_not_found(self, ecr):
        repo_name = _unique("nolc-repo")
        ecr.create_repository(repositoryName=repo_name)
        try:
            with pytest.raises(ClientError) as exc_info:
                ecr.get_lifecycle_policy(repositoryName=repo_name)
            assert "LifecyclePolicyNotFoundException" in str(exc_info.value)
        finally:
            ecr.delete_repository(repositoryName=repo_name)


class TestECRImageScanning:
    def test_put_image_scanning_configuration(self, ecr):
        repo_name = _unique("scan-repo")
        ecr.create_repository(repositoryName=repo_name)
        try:
            response = ecr.put_image_scanning_configuration(
                repositoryName=repo_name,
                imageScanningConfiguration={"scanOnPush": True},
            )
            assert response["repositoryName"] == repo_name
            assert response["imageScanningConfiguration"]["scanOnPush"] is True
            # Verify via describe
            described = ecr.describe_repositories(repositoryNames=[repo_name])
            assert described["repositories"][0]["imageScanningConfiguration"]["scanOnPush"] is True
        finally:
            ecr.delete_repository(repositoryName=repo_name)


class TestECREncryption:
    def test_create_repository_with_encryption(self, ecr):
        repo_name = _unique("enc-repo")
        response = ecr.create_repository(
            repositoryName=repo_name,
            encryptionConfiguration={"encryptionType": "AES256"},
        )
        try:
            repo = response["repository"]
            assert repo["repositoryName"] == repo_name
            assert repo["encryptionConfiguration"]["encryptionType"] == "AES256"
        finally:
            ecr.delete_repository(repositoryName=repo_name)


class TestECRImageTagMutability:
    def test_put_image_tag_mutability(self, ecr):
        repo_name = _unique("mut-repo")
        ecr.create_repository(repositoryName=repo_name)
        try:
            response = ecr.put_image_tag_mutability(
                repositoryName=repo_name,
                imageTagMutability="IMMUTABLE",
            )
            assert response["repositoryName"] == repo_name
            assert response["imageTagMutability"] == "IMMUTABLE"
            described = ecr.describe_repositories(repositoryNames=[repo_name])
            assert described["repositories"][0]["imageTagMutability"] == "IMMUTABLE"
        finally:
            ecr.delete_repository(repositoryName=repo_name)


class TestECRDeleteLifecyclePolicy:
    def test_delete_lifecycle_policy(self, ecr):
        repo_name = _unique("dellc-repo")
        ecr.create_repository(repositoryName=repo_name)
        try:
            policy = {
                "rules": [
                    {
                        "rulePriority": 1,
                        "description": "Remove old",
                        "selection": {
                            "tagStatus": "untagged",
                            "countType": "sinceImagePushed",
                            "countUnit": "days",
                            "countNumber": 7,
                        },
                        "action": {"type": "expire"},
                    }
                ]
            }
            ecr.put_lifecycle_policy(
                repositoryName=repo_name,
                lifecyclePolicyText=json.dumps(policy),
            )
            response = ecr.delete_lifecycle_policy(repositoryName=repo_name)
            assert response["repositoryName"] == repo_name
            with pytest.raises(ClientError) as exc_info:
                ecr.get_lifecycle_policy(repositoryName=repo_name)
            assert "LifecyclePolicyNotFoundException" in str(exc_info.value)
        finally:
            ecr.delete_repository(repositoryName=repo_name)


class TestECRExtendedOperations:
    def test_create_repository_with_tags(self, ecr):
        repo_name = _unique("tagged-repo")
        response = ecr.create_repository(
            repositoryName=repo_name,
            tags=[
                {"Key": "env", "Value": "test"},
                {"Key": "team", "Value": "platform"},
            ],
        )
        try:
            assert response["repository"]["repositoryName"] == repo_name
        finally:
            ecr.delete_repository(repositoryName=repo_name)

    def test_list_tags_for_resource(self, ecr):
        repo_name = _unique("listtag-repo")
        resp = ecr.create_repository(repositoryName=repo_name)
        arn = resp["repository"]["repositoryArn"]
        try:
            ecr.tag_resource(
                resourceArn=arn,
                tags=[{"Key": "color", "Value": "blue"}],
            )
            tags_resp = ecr.list_tags_for_resource(resourceArn=arn)
            tag_map = {t["Key"]: t["Value"] for t in tags_resp["tags"]}
            assert tag_map["color"] == "blue"
        finally:
            ecr.delete_repository(repositoryName=repo_name)

    def test_untag_resource(self, ecr):
        repo_name = _unique("untag-repo")
        resp = ecr.create_repository(repositoryName=repo_name)
        arn = resp["repository"]["repositoryArn"]
        try:
            ecr.tag_resource(
                resourceArn=arn,
                tags=[{"Key": "temp", "Value": "yes"}, {"Key": "keep", "Value": "yes"}],
            )
            ecr.untag_resource(resourceArn=arn, tagKeys=["temp"])
            tags_resp = ecr.list_tags_for_resource(resourceArn=arn)
            keys = [t["Key"] for t in tags_resp["tags"]]
            assert "temp" not in keys
            assert "keep" in keys
        finally:
            ecr.delete_repository(repositoryName=repo_name)

    def test_create_repository_image_tag_immutable(self, ecr):
        repo_name = _unique("immut-repo")
        resp = ecr.create_repository(
            repositoryName=repo_name,
            imageTagMutability="IMMUTABLE",
        )
        try:
            assert resp["repository"]["imageTagMutability"] == "IMMUTABLE"
        finally:
            ecr.delete_repository(repositoryName=repo_name)

    def test_create_repository_scan_on_push(self, ecr):
        repo_name = _unique("scanpush-repo")
        resp = ecr.create_repository(
            repositoryName=repo_name,
            imageScanningConfiguration={"scanOnPush": True},
        )
        try:
            assert resp["repository"]["imageScanningConfiguration"]["scanOnPush"] is True
        finally:
            ecr.delete_repository(repositoryName=repo_name)

    def test_describe_repositories_pagination(self, ecr):
        repos = []
        for i in range(3):
            name = _unique(f"page-repo-{i}")
            ecr.create_repository(repositoryName=name)
            repos.append(name)
        try:
            resp = ecr.describe_repositories(maxResults=2)
            assert len(resp["repositories"]) <= 2
        finally:
            for name in repos:
                ecr.delete_repository(repositoryName=name)

    def test_get_authorization_token(self, ecr):
        resp = ecr.get_authorization_token()
        assert "authorizationData" in resp
        assert len(resp["authorizationData"]) >= 1
        auth = resp["authorizationData"][0]
        assert "authorizationToken" in auth
        assert "proxyEndpoint" in auth

    def test_describe_registry(self, ecr):
        resp = ecr.describe_registry()
        assert "registryId" in resp

    def test_batch_check_layer_availability(self, ecr):
        repo_name = _unique("layer-repo")
        ecr.create_repository(repositoryName=repo_name)
        try:
            resp = ecr.batch_check_layer_availability(
                repositoryName=repo_name,
                layerDigests=[
                    "sha256:0000000000000000000000000000000000000000000000000000000000000000"
                ],
            )
            assert "layers" in resp or "failures" in resp
        finally:
            ecr.delete_repository(repositoryName=repo_name)

    def test_batch_get_image_nonexistent(self, ecr):
        repo_name = _unique("batchget-repo")
        ecr.create_repository(repositoryName=repo_name)
        try:
            resp = ecr.batch_get_image(
                repositoryName=repo_name,
                imageIds=[{"imageTag": "nonexistent"}],
            )
            assert "failures" in resp
        finally:
            ecr.delete_repository(repositoryName=repo_name)


class TestEcrAutoCoverage:
    """Auto-generated coverage tests for ecr."""

    @pytest.fixture
    def client(self):
        return make_client("ecr")

    def test_get_registry_scanning_configuration(self, client):
        """GetRegistryScanningConfiguration returns a response."""
        resp = client.get_registry_scanning_configuration()
        assert "registryId" in resp
