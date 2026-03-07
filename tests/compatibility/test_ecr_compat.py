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

    def test_get_repository_policy(self, ecr):
        """Set and then retrieve a repository policy."""
        repo_name = _unique("getpol-repo")
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
                            "Action": ["ecr:GetDownloadUrlForLayer"],
                        }
                    ],
                }
            )
            ecr.set_repository_policy(repositoryName=repo_name, policyText=policy)
            response = ecr.get_repository_policy(repositoryName=repo_name)
            assert response["repositoryName"] == repo_name
            returned = json.loads(response["policyText"])
            assert returned["Version"] == "2012-10-17"
            assert len(returned["Statement"]) == 1
        finally:
            ecr.delete_repository(repositoryName=repo_name)

    def test_delete_repository_policy(self, ecr):
        """Set and then delete a repository policy."""
        repo_name = _unique("delpol-repo")
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
                            "Action": ["ecr:GetDownloadUrlForLayer"],
                        }
                    ],
                }
            )
            ecr.set_repository_policy(repositoryName=repo_name, policyText=policy)
            response = ecr.delete_repository_policy(repositoryName=repo_name)
            assert response["repositoryName"] == repo_name
            # Verify it's gone
            with pytest.raises(ecr.exceptions.RepositoryPolicyNotFoundException):
                ecr.get_repository_policy(repositoryName=repo_name)
        finally:
            ecr.delete_repository(repositoryName=repo_name)

    def test_delete_lifecycle_policy(self, ecr):
        """Put and then delete a lifecycle policy."""
        repo_name = _unique("dellc-repo")
        ecr.create_repository(repositoryName=repo_name)
        try:
            policy = {
                "rules": [
                    {
                        "rulePriority": 1,
                        "description": "Expire old",
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
            # Verify it's gone
            with pytest.raises(ecr.exceptions.LifecyclePolicyNotFoundException):
                ecr.get_lifecycle_policy(repositoryName=repo_name)
        finally:
            ecr.delete_repository(repositoryName=repo_name)

    def test_create_repository_with_encryption(self, ecr):
        """Create a repository with AES256 encryption configuration."""
        repo_name = _unique("enc-repo")
        response = ecr.create_repository(
            repositoryName=repo_name,
            encryptionConfiguration={"encryptionType": "AES256"},
        )
        repo = response["repository"]
        assert repo["repositoryName"] == repo_name
        assert repo["encryptionConfiguration"]["encryptionType"] == "AES256"
        ecr.delete_repository(repositoryName=repo_name)

    def test_create_repository_with_image_tag_mutability(self, ecr):
        """Create a repository with IMMUTABLE image tag mutability."""
        repo_name = _unique("immut-repo")
        response = ecr.create_repository(
            repositoryName=repo_name,
            imageTagMutability="IMMUTABLE",
        )
        repo = response["repository"]
        assert repo["imageTagMutability"] == "IMMUTABLE"
        ecr.delete_repository(repositoryName=repo_name)

    def test_create_repository_with_image_scanning(self, ecr):
        """Create a repository with scan-on-push enabled."""
        repo_name = _unique("scan-repo")
        response = ecr.create_repository(
            repositoryName=repo_name,
            imageScanningConfiguration={"scanOnPush": True},
        )
        repo = response["repository"]
        assert repo["imageScanningConfiguration"]["scanOnPush"] is True
        ecr.delete_repository(repositoryName=repo_name)

    def test_describe_repositories_nonexistent(self, ecr):
        """Describing a nonexistent repository raises RepositoryNotFoundException."""
        with pytest.raises(ecr.exceptions.RepositoryNotFoundException):
            ecr.describe_repositories(repositoryNames=["nonexistent-repo-xyz"])

    def test_delete_repository_nonexistent(self, ecr):
        """Deleting a nonexistent repository raises RepositoryNotFoundException."""
        with pytest.raises(ecr.exceptions.RepositoryNotFoundException):
            ecr.delete_repository(repositoryName="nonexistent-repo-xyz")


class TestECRImageOperations:
    def test_put_image_and_list_images(self, ecr):
        """Put a fake image manifest and list images in the repository."""
        repo_name = _unique("img-repo")
        ecr.create_repository(repositoryName=repo_name)
        try:
            manifest = json.dumps(
                {
                    "schemaVersion": 2,
                    "mediaType": "application/vnd.docker.distribution.manifest.v2+json",
                    "config": {
                        "mediaType": "application/vnd.docker.container.image.v1+json",
                        "size": 7023,
                        "digest": "sha256:" + "a" * 64,
                    },
                    "layers": [],
                }
            )
            ecr.put_image(
                repositoryName=repo_name,
                imageManifest=manifest,
                imageTag="latest",
            )
            response = ecr.list_images(repositoryName=repo_name)
            image_ids = response["imageIds"]
            assert len(image_ids) >= 1
            tags = [img.get("imageTag") for img in image_ids]
            assert "latest" in tags
        finally:
            ecr.delete_repository(repositoryName=repo_name, force=True)

    def test_batch_get_image(self, ecr):
        """Put an image and retrieve it via batch_get_image."""
        repo_name = _unique("batch-repo")
        ecr.create_repository(repositoryName=repo_name)
        try:
            manifest = json.dumps(
                {
                    "schemaVersion": 2,
                    "mediaType": "application/vnd.docker.distribution.manifest.v2+json",
                    "config": {
                        "mediaType": "application/vnd.docker.container.image.v1+json",
                        "size": 7023,
                        "digest": "sha256:" + "b" * 64,
                    },
                    "layers": [],
                }
            )
            ecr.put_image(
                repositoryName=repo_name,
                imageManifest=manifest,
                imageTag="v1",
            )
            response = ecr.batch_get_image(
                repositoryName=repo_name,
                imageIds=[{"imageTag": "v1"}],
            )
            assert len(response["images"]) == 1
            assert response["images"][0]["imageId"]["imageTag"] == "v1"
            assert "imageManifest" in response["images"][0]
        finally:
            ecr.delete_repository(repositoryName=repo_name, force=True)

    def test_describe_images(self, ecr):
        """Put an image and describe it."""
        repo_name = _unique("descimg-repo")
        ecr.create_repository(repositoryName=repo_name)
        try:
            manifest = json.dumps(
                {
                    "schemaVersion": 2,
                    "mediaType": "application/vnd.docker.distribution.manifest.v2+json",
                    "config": {
                        "mediaType": "application/vnd.docker.container.image.v1+json",
                        "size": 7023,
                        "digest": "sha256:" + "c" * 64,
                    },
                    "layers": [],
                }
            )
            ecr.put_image(
                repositoryName=repo_name,
                imageManifest=manifest,
                imageTag="v2",
            )
            response = ecr.describe_images(repositoryName=repo_name)
            assert len(response["imageDetails"]) >= 1
            tags = []
            for detail in response["imageDetails"]:
                tags.extend(detail.get("imageTags", []))
            assert "v2" in tags
        finally:
            ecr.delete_repository(repositoryName=repo_name, force=True)

    def test_list_images_empty_repository(self, ecr):
        """Listing images in an empty repository returns an empty list."""
        repo_name = _unique("empty-repo")
        ecr.create_repository(repositoryName=repo_name)
        try:
            response = ecr.list_images(repositoryName=repo_name)
            assert response["imageIds"] == []
        finally:
            ecr.delete_repository(repositoryName=repo_name)


class TestECRTagOperations:
    def test_tag_resource(self, ecr):
        """Tag an ECR repository and verify with list_tags_for_resource."""
        repo_name = _unique("tag-repo")
        response = ecr.create_repository(repositoryName=repo_name)
        arn = response["repository"]["repositoryArn"]
        try:
            ecr.tag_resource(
                resourceArn=arn,
                tags=[
                    {"Key": "Environment", "Value": "test"},
                    {"Key": "Project", "Value": "robotocore"},
                ],
            )
            tag_response = ecr.list_tags_for_resource(resourceArn=arn)
            tag_map = {t["Key"]: t["Value"] for t in tag_response["tags"]}
            assert tag_map["Environment"] == "test"
            assert tag_map["Project"] == "robotocore"
        finally:
            ecr.delete_repository(repositoryName=repo_name)

    def test_untag_resource(self, ecr):
        """Tag a repository, then remove a tag."""
        repo_name = _unique("untag-repo")
        response = ecr.create_repository(repositoryName=repo_name)
        arn = response["repository"]["repositoryArn"]
        try:
            ecr.tag_resource(
                resourceArn=arn,
                tags=[
                    {"Key": "Env", "Value": "dev"},
                    {"Key": "Team", "Value": "platform"},
                ],
            )
            ecr.untag_resource(resourceArn=arn, tagKeys=["Team"])
            tag_response = ecr.list_tags_for_resource(resourceArn=arn)
            tag_keys = [t["Key"] for t in tag_response["tags"]]
            assert "Env" in tag_keys
            assert "Team" not in tag_keys
        finally:
            ecr.delete_repository(repositoryName=repo_name)

    def test_create_repository_with_tags(self, ecr):
        """Create a repository with tags and verify via list_tags_for_resource."""
        repo_name = _unique("tagrepo")
        response = ecr.create_repository(
            repositoryName=repo_name,
            tags=[{"Key": "CreatedBy", "Value": "compat-test"}],
        )
        arn = response["repository"]["repositoryArn"]
        try:
            tag_response = ecr.list_tags_for_resource(resourceArn=arn)
            tag_map = {t["Key"]: t["Value"] for t in tag_response["tags"]}
            assert tag_map["CreatedBy"] == "compat-test"
        finally:
            ecr.delete_repository(repositoryName=repo_name)
