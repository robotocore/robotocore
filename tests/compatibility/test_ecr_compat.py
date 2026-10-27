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


class TestECRImageOperations:
    """Tests for ECR image push/list/describe/batch operations."""

    @pytest.fixture
    def repo(self, ecr):
        name = _unique("img-repo")
        ecr.create_repository(repositoryName=name)
        yield name
        ecr.delete_repository(repositoryName=name, force=True)

    def _make_manifest(self, tag_seed: str) -> str:
        import hashlib

        digest = hashlib.sha256(tag_seed.encode()).hexdigest()
        return json.dumps(
            {
                "schemaVersion": 2,
                "mediaType": "application/vnd.docker.distribution.manifest.v2+json",
                "config": {
                    "mediaType": "application/vnd.docker.container.image.v1+json",
                    "size": 7023,
                    "digest": f"sha256:{digest}",
                },
                "layers": [],
            }
        )

    def test_put_image(self, ecr, repo):
        manifest = self._make_manifest("put-test")
        resp = ecr.put_image(repositoryName=repo, imageManifest=manifest, imageTag="v1")
        assert "image" in resp
        assert resp["image"]["repositoryName"] == repo
        assert resp["image"]["imageId"]["imageTag"] == "v1"

    def test_list_images(self, ecr, repo):
        manifest = self._make_manifest("list-test")
        ecr.put_image(repositoryName=repo, imageManifest=manifest, imageTag="latest")
        resp = ecr.list_images(repositoryName=repo)
        assert "imageIds" in resp
        tags = [img.get("imageTag") for img in resp["imageIds"]]
        assert "latest" in tags

    def test_describe_images_after_put(self, ecr, repo):
        manifest = self._make_manifest("desc-test")
        ecr.put_image(repositoryName=repo, imageManifest=manifest, imageTag="desc1")
        resp = ecr.describe_images(repositoryName=repo)
        assert len(resp["imageDetails"]) >= 1
        tags = []
        for detail in resp["imageDetails"]:
            tags.extend(detail.get("imageTags", []))
        assert "desc1" in tags

    def test_batch_get_image(self, ecr, repo):
        manifest = self._make_manifest("batch-get-test")
        ecr.put_image(repositoryName=repo, imageManifest=manifest, imageTag="bg1")
        resp = ecr.batch_get_image(repositoryName=repo, imageIds=[{"imageTag": "bg1"}])
        assert len(resp["images"]) == 1
        assert resp["images"][0]["imageId"]["imageTag"] == "bg1"

    def test_batch_delete_image(self, ecr, repo):
        manifest = self._make_manifest("batch-del-test")
        ecr.put_image(repositoryName=repo, imageManifest=manifest, imageTag="bd1")
        resp = ecr.batch_delete_image(repositoryName=repo, imageIds=[{"imageTag": "bd1"}])
        assert len(resp["imageIds"]) == 1
        assert resp["imageIds"][0]["imageTag"] == "bd1"
        # Verify image is gone
        listed = ecr.list_images(repositoryName=repo)
        tags = [img.get("imageTag") for img in listed["imageIds"]]
        assert "bd1" not in tags

    def test_put_multiple_images(self, ecr, repo):
        for tag in ["alpha", "beta"]:
            manifest = self._make_manifest(f"multi-{tag}")
            ecr.put_image(repositoryName=repo, imageManifest=manifest, imageTag=tag)
        resp = ecr.list_images(repositoryName=repo)
        tags = [img.get("imageTag") for img in resp["imageIds"]]
        assert "alpha" in tags
        assert "beta" in tags


class TestECRImageScanning2:
    """Tests for ECR image scan operations."""

    @pytest.fixture
    def repo_with_image(self, ecr):
        import hashlib

        name = _unique("scan-repo")
        ecr.create_repository(repositoryName=name)
        digest = hashlib.sha256(b"scan-image-config").hexdigest()
        manifest = json.dumps(
            {
                "schemaVersion": 2,
                "mediaType": "application/vnd.docker.distribution.manifest.v2+json",
                "config": {
                    "mediaType": "application/vnd.docker.container.image.v1+json",
                    "size": 7023,
                    "digest": f"sha256:{digest}",
                },
                "layers": [],
            }
        )
        ecr.put_image(repositoryName=name, imageManifest=manifest, imageTag="scanme")
        yield name
        ecr.delete_repository(repositoryName=name, force=True)

    def test_start_image_scan(self, ecr, repo_with_image):
        resp = ecr.start_image_scan(
            repositoryName=repo_with_image,
            imageId={"imageTag": "scanme"},
        )
        assert resp["repositoryName"] == repo_with_image
        assert resp["imageScanStatus"]["status"] in ("IN_PROGRESS", "COMPLETE")

    def test_describe_image_scan_findings(self, ecr, repo_with_image):
        ecr.start_image_scan(
            repositoryName=repo_with_image,
            imageId={"imageTag": "scanme"},
        )
        resp = ecr.describe_image_scan_findings(
            repositoryName=repo_with_image,
            imageId={"imageTag": "scanme"},
        )
        assert resp["repositoryName"] == repo_with_image
        assert "imageScanStatus" in resp


class TestECRRegistryPolicy:
    """Tests for ECR registry-level policy operations."""

    def test_put_registry_policy(self, ecr):
        policy = json.dumps(
            {
                "Version": "2012-10-17",
                "Statement": [
                    {
                        "Sid": "ReplicationAccess",
                        "Effect": "Allow",
                        "Principal": {"AWS": "arn:aws:iam::123456789012:root"},
                        "Action": ["ecr:ReplicateImage"],
                        "Resource": "*",
                    }
                ],
            }
        )
        resp = ecr.put_registry_policy(policyText=policy)
        assert "policyText" in resp
        assert resp["registryId"] is not None

    def test_get_registry_policy(self, ecr):
        policy = json.dumps(
            {
                "Version": "2012-10-17",
                "Statement": [
                    {
                        "Sid": "GetTest",
                        "Effect": "Allow",
                        "Principal": {"AWS": "arn:aws:iam::123456789012:root"},
                        "Action": ["ecr:ReplicateImage"],
                        "Resource": "*",
                    }
                ],
            }
        )
        ecr.put_registry_policy(policyText=policy)
        resp = ecr.get_registry_policy()
        assert "policyText" in resp
        returned = json.loads(resp["policyText"])
        assert returned["Version"] == "2012-10-17"

    def test_delete_registry_policy(self, ecr):
        policy = json.dumps(
            {
                "Version": "2012-10-17",
                "Statement": [
                    {
                        "Sid": "DeleteTest",
                        "Effect": "Allow",
                        "Principal": {"AWS": "arn:aws:iam::123456789012:root"},
                        "Action": ["ecr:ReplicateImage"],
                        "Resource": "*",
                    }
                ],
            }
        )
        ecr.put_registry_policy(policyText=policy)
        resp = ecr.delete_registry_policy()
        assert "registryId" in resp
        # Verify it's gone
        with pytest.raises(ClientError) as exc_info:
            ecr.get_registry_policy()
        assert "RegistryPolicyNotFoundException" in str(exc_info.value)


class TestECRRegistryScanningConfig:
    """Tests for ECR registry scanning configuration."""

    def test_put_registry_scanning_configuration(self, ecr):
        resp = ecr.put_registry_scanning_configuration(
            scanType="BASIC",
            rules=[],
        )
        assert "registryScanningConfiguration" in resp
        config = resp["registryScanningConfiguration"]
        assert config["scanType"] == "BASIC"


class TestEcrAutoCoverage:
    """Auto-generated coverage tests for ecr."""

    @pytest.fixture
    def client(self):
        return make_client("ecr")

    def test_get_registry_scanning_configuration(self, client):
        """GetRegistryScanningConfiguration returns a response."""
        resp = client.get_registry_scanning_configuration()
        assert "registryId" in resp


class TestECRReplicationConfig:
    """Tests for ECR replication configuration."""

    def test_put_replication_configuration(self, ecr):
        resp = ecr.put_replication_configuration(
            replicationConfiguration={
                "rules": [
                    {
                        "destinations": [
                            {
                                "region": "us-west-2",
                                "registryId": "123456789012",
                            }
                        ],
                    }
                ]
            }
        )
        assert "replicationConfiguration" in resp
        rules = resp["replicationConfiguration"]["rules"]
        assert len(rules) == 1
        assert rules[0]["destinations"][0]["region"] == "us-west-2"


class TestECRBatchScanningConfig:
    """Tests for batch get repository scanning configuration."""

    @pytest.fixture
    def repo(self, ecr):
        name = _unique("bscan-repo")
        ecr.create_repository(repositoryName=name)
        yield name
        ecr.delete_repository(repositoryName=name, force=True)

    def test_batch_get_repository_scanning_configuration(self, ecr, repo):
        resp = ecr.batch_get_repository_scanning_configuration(
            repositoryNames=[repo],
        )
        assert "scanningConfigurations" in resp


class TestECRPullThroughCacheRules:
    """Tests for ECR pull-through cache rule operations."""

    def test_create_and_describe_pull_through_cache_rule(self, ecr):
        """CreatePullThroughCacheRule + DescribePullThroughCacheRules roundtrip."""
        prefix = _unique("ptcr")
        resp = ecr.create_pull_through_cache_rule(
            ecrRepositoryPrefix=prefix,
            upstreamRegistryUrl="public.ecr.aws",
        )
        assert "ecrRepositoryPrefix" in resp
        assert resp["ecrRepositoryPrefix"] == prefix
        try:
            desc = ecr.describe_pull_through_cache_rules()
            assert "pullThroughCacheRules" in desc
            prefixes = [r["ecrRepositoryPrefix"] for r in desc["pullThroughCacheRules"]]
            assert prefix in prefixes
        finally:
            ecr.delete_pull_through_cache_rule(ecrRepositoryPrefix=prefix)

    def test_delete_pull_through_cache_rule(self, ecr):
        """DeletePullThroughCacheRule removes the rule."""
        prefix = _unique("ptcr-del")
        ecr.create_pull_through_cache_rule(
            ecrRepositoryPrefix=prefix,
            upstreamRegistryUrl="public.ecr.aws",
        )
        resp = ecr.delete_pull_through_cache_rule(ecrRepositoryPrefix=prefix)
        assert "ecrRepositoryPrefix" in resp
        # Verify it's gone
        desc = ecr.describe_pull_through_cache_rules()
        prefixes = [r["ecrRepositoryPrefix"] for r in desc["pullThroughCacheRules"]]
        assert prefix not in prefixes

    def test_describe_pull_through_cache_rules_empty(self, ecr):
        """DescribePullThroughCacheRules returns empty list when none exist."""
        resp = ecr.describe_pull_through_cache_rules()
        assert "pullThroughCacheRules" in resp
        assert isinstance(resp["pullThroughCacheRules"], list)


class TestECRAccountSettings:
    """Tests for ECR account setting operations."""

    def test_put_and_get_account_setting(self, ecr):
        """PutAccountSetting + GetAccountSetting roundtrip."""
        put_resp = ecr.put_account_setting(
            name="BASIC_SCAN_TYPE_VERSION",
            value="AWS_NATIVE",
        )
        assert "name" in put_resp
        assert put_resp["name"] == "BASIC_SCAN_TYPE_VERSION"
        assert put_resp["value"] == "AWS_NATIVE"

        get_resp = ecr.get_account_setting(name="BASIC_SCAN_TYPE_VERSION")
        assert "name" in get_resp
        assert get_resp["value"] == "AWS_NATIVE"


class TestECRErrorPaths:
    """Tests that verify proper error responses for nonexistent resources."""

    def test_describe_repositories_nonexistent(self, ecr):
        with pytest.raises(ClientError) as exc_info:
            ecr.describe_repositories(repositoryNames=["nonexistent-repo-xyz-999"])
        assert "RepositoryNotFoundException" in str(exc_info.value)

    def test_delete_repository_nonexistent(self, ecr):
        with pytest.raises(ClientError) as exc_info:
            ecr.delete_repository(repositoryName="nonexistent-repo-xyz-999")
        assert "RepositoryNotFoundException" in str(exc_info.value)

    def test_list_images_nonexistent_repo(self, ecr):
        with pytest.raises(ClientError) as exc_info:
            ecr.list_images(repositoryName="nonexistent-repo-xyz-999")
        assert "RepositoryNotFoundException" in str(exc_info.value)

    def test_describe_images_nonexistent_repo(self, ecr):
        with pytest.raises(ClientError) as exc_info:
            ecr.describe_images(repositoryName="nonexistent-repo-xyz-999")
        assert "RepositoryNotFoundException" in str(exc_info.value)

    def test_get_repository_policy_nonexistent(self, ecr):
        with pytest.raises(ClientError) as exc_info:
            ecr.get_repository_policy(repositoryName="nonexistent-repo-xyz-999")
        assert "RepositoryNotFoundException" in str(exc_info.value)

    def test_delete_repository_policy_nonexistent(self, ecr):
        with pytest.raises(ClientError) as exc_info:
            ecr.delete_repository_policy(repositoryName="nonexistent-repo-xyz-999")
        assert "RepositoryNotFoundException" in str(exc_info.value)

    def test_set_repository_policy_nonexistent(self, ecr):
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
        with pytest.raises(ClientError) as exc_info:
            ecr.set_repository_policy(repositoryName="nonexistent-repo-xyz-999", policyText=policy)
        assert "RepositoryNotFoundException" in str(exc_info.value)

    def test_get_lifecycle_policy_nonexistent_repo(self, ecr):
        with pytest.raises(ClientError) as exc_info:
            ecr.get_lifecycle_policy(repositoryName="nonexistent-repo-xyz-999")
        assert "RepositoryNotFoundException" in str(exc_info.value)

    def test_put_image_scanning_config_nonexistent(self, ecr):
        with pytest.raises(ClientError) as exc_info:
            ecr.put_image_scanning_configuration(
                repositoryName="nonexistent-repo-xyz-999",
                imageScanningConfiguration={"scanOnPush": True},
            )
        assert "RepositoryNotFoundException" in str(exc_info.value)

    def test_start_image_scan_nonexistent(self, ecr):
        with pytest.raises(ClientError) as exc_info:
            ecr.start_image_scan(
                repositoryName="nonexistent-repo-xyz-999",
                imageId={"imageTag": "latest"},
            )
        assert "RepositoryNotFoundException" in str(exc_info.value)

    def test_describe_image_scan_findings_nonexistent(self, ecr):
        with pytest.raises(ClientError) as exc_info:
            ecr.describe_image_scan_findings(
                repositoryName="nonexistent-repo-xyz-999",
                imageId={"imageTag": "latest"},
            )
        assert "RepositoryNotFoundException" in str(exc_info.value)

    def test_delete_lifecycle_policy_nonexistent_repo(self, ecr):
        with pytest.raises(ClientError) as exc_info:
            ecr.delete_lifecycle_policy(repositoryName="nonexistent-repo-xyz-999")
        error_str = str(exc_info.value)
        assert (
            "RepositoryNotFoundException" in error_str
            or "LifecyclePolicyNotFoundException" in error_str
        )

    def test_put_image_tag_mutability_nonexistent(self, ecr):
        with pytest.raises(ClientError) as exc_info:
            ecr.put_image_tag_mutability(
                repositoryName="nonexistent-repo-xyz-999",
                imageTagMutability="IMMUTABLE",
            )
        assert "RepositoryNotFoundException" in str(exc_info.value)

    def test_create_repository_already_exists(self, ecr):
        repo_name = _unique("duprepo")
        resp = ecr.create_repository(repositoryName=repo_name)
        assert resp["repository"]["repositoryName"] == repo_name
        try:
            with pytest.raises(ClientError) as exc_info:
                ecr.create_repository(repositoryName=repo_name)
            assert "RepositoryAlreadyExistsException" in str(exc_info.value)
        finally:
            ecr.delete_repository(repositoryName=repo_name)


class TestECRRepositoryCreationTemplates:
    """Tests for ECR repository creation template CRUD operations."""

    def test_create_repository_creation_template(self, ecr):
        """CreateRepositoryCreationTemplate creates a template."""
        prefix = _unique("tmpl")
        resp = ecr.create_repository_creation_template(
            prefix=prefix,
            appliedFor=["REPLICATION"],
        )
        assert "repositoryCreationTemplate" in resp
        assert resp["repositoryCreationTemplate"]["prefix"] == prefix
        assert "REPLICATION" in resp["repositoryCreationTemplate"]["appliedFor"]
        # cleanup
        ecr.delete_repository_creation_template(prefix=prefix)

    def test_describe_repository_creation_templates(self, ecr):
        """DescribeRepositoryCreationTemplates lists created templates."""
        prefix = _unique("desc-tmpl")
        ecr.create_repository_creation_template(
            prefix=prefix,
            appliedFor=["REPLICATION"],
        )
        try:
            resp = ecr.describe_repository_creation_templates()
            assert "repositoryCreationTemplates" in resp
            prefixes = [t["prefix"] for t in resp["repositoryCreationTemplates"]]
            assert prefix in prefixes
        finally:
            ecr.delete_repository_creation_template(prefix=prefix)

    def test_update_repository_creation_template(self, ecr):
        """UpdateRepositoryCreationTemplate modifies appliedFor."""
        prefix = _unique("upd-tmpl")
        ecr.create_repository_creation_template(
            prefix=prefix,
            appliedFor=["REPLICATION"],
        )
        try:
            resp = ecr.update_repository_creation_template(
                prefix=prefix,
                appliedFor=["REPLICATION", "PULL_THROUGH_CACHE"],
            )
            assert "repositoryCreationTemplate" in resp
            applied = resp["repositoryCreationTemplate"]["appliedFor"]
            assert "REPLICATION" in applied
            assert "PULL_THROUGH_CACHE" in applied
        finally:
            ecr.delete_repository_creation_template(prefix=prefix)

    def test_delete_repository_creation_template(self, ecr):
        """DeleteRepositoryCreationTemplate removes the template."""
        prefix = _unique("del-tmpl")
        ecr.create_repository_creation_template(
            prefix=prefix,
            appliedFor=["REPLICATION"],
        )
        resp = ecr.delete_repository_creation_template(prefix=prefix)
        assert "repositoryCreationTemplate" in resp
        assert resp["repositoryCreationTemplate"]["prefix"] == prefix
        # Verify it's gone
        desc = ecr.describe_repository_creation_templates()
        prefixes = [t["prefix"] for t in desc["repositoryCreationTemplates"]]
        assert prefix not in prefixes


class TestECRValidatePullThroughCacheRule:
    """Tests for ValidatePullThroughCacheRule operation."""

    def test_validate_pull_through_cache_rule(self, ecr):
        """ValidatePullThroughCacheRule validates an existing rule."""
        prefix = _unique("val-ptcr")
        ecr.create_pull_through_cache_rule(
            ecrRepositoryPrefix=prefix,
            upstreamRegistryUrl="public.ecr.aws",
        )
        try:
            resp = ecr.validate_pull_through_cache_rule(ecrRepositoryPrefix=prefix)
            assert resp["ecrRepositoryPrefix"] == prefix
            assert resp["isValid"] is True
        finally:
            ecr.delete_pull_through_cache_rule(ecrRepositoryPrefix=prefix)


class TestECRDescribeImageReplicationStatus:
    """Tests for DescribeImageReplicationStatus operation."""

    def test_describe_image_replication_status(self, ecr):
        """DescribeImageReplicationStatus returns status for an image."""
        import hashlib

        repo_name = _unique("replst")
        ecr.create_repository(repositoryName=repo_name)
        digest = hashlib.sha256(b"repl-status-config").hexdigest()
        manifest = json.dumps(
            {
                "schemaVersion": 2,
                "mediaType": "application/vnd.docker.distribution.manifest.v2+json",
                "config": {
                    "mediaType": "application/vnd.docker.container.image.v1+json",
                    "size": 7023,
                    "digest": f"sha256:{digest}",
                },
                "layers": [],
            }
        )
        put_resp = ecr.put_image(
            repositoryName=repo_name, imageManifest=manifest, imageTag="repltest"
        )
        image_digest = put_resp["image"]["imageId"]["imageDigest"]
        try:
            resp = ecr.describe_image_replication_status(
                repositoryName=repo_name,
                imageId={"imageDigest": image_digest},
            )
            assert resp["repositoryName"] == repo_name
            assert "imageId" in resp
        finally:
            ecr.delete_repository(repositoryName=repo_name, force=True)


class TestECRUpdatePullThroughCacheRule:
    """Tests for UpdatePullThroughCacheRule operation."""

    def test_update_pull_through_cache_rule(self, ecr):
        """UpdatePullThroughCacheRule modifies an existing rule's credential ARN."""
        prefix = _unique("upd-ptcr")
        ecr.create_pull_through_cache_rule(
            ecrRepositoryPrefix=prefix,
            upstreamRegistryUrl="public.ecr.aws",
        )
        try:
            resp = ecr.update_pull_through_cache_rule(
                ecrRepositoryPrefix=prefix,
                credentialArn="arn:aws:secretsmanager:us-east-1:123456789012:secret:ecr-creds",
            )
            assert resp["ecrRepositoryPrefix"] == prefix
        finally:
            ecr.delete_pull_through_cache_rule(ecrRepositoryPrefix=prefix)


class TestECRLifecyclePolicyPreview:
    """Tests for StartLifecyclePolicyPreview and GetLifecyclePolicyPreview."""

    def test_start_lifecycle_policy_preview(self, ecr):
        """StartLifecyclePolicyPreview starts a preview."""
        repo_name = _unique("slpp")
        ecr.create_repository(repositoryName=repo_name)
        try:
            policy = json.dumps(
                {
                    "rules": [
                        {
                            "rulePriority": 1,
                            "description": "Remove old untagged",
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
            )
            ecr.put_lifecycle_policy(
                repositoryName=repo_name,
                lifecyclePolicyText=policy,
            )
            resp = ecr.start_lifecycle_policy_preview(repositoryName=repo_name)
            assert resp["repositoryName"] == repo_name
            assert resp["status"] in ("IN_PROGRESS", "COMPLETE")
        finally:
            ecr.delete_repository(repositoryName=repo_name, force=True)

    def test_get_lifecycle_policy_preview(self, ecr):
        """GetLifecyclePolicyPreview returns preview results."""
        repo_name = _unique("glpp")
        ecr.create_repository(repositoryName=repo_name)
        try:
            policy = json.dumps(
                {
                    "rules": [
                        {
                            "rulePriority": 1,
                            "description": "Remove old untagged",
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
            )
            ecr.put_lifecycle_policy(
                repositoryName=repo_name,
                lifecyclePolicyText=policy,
            )
            ecr.start_lifecycle_policy_preview(repositoryName=repo_name)
            resp = ecr.get_lifecycle_policy_preview(repositoryName=repo_name)
            assert resp["repositoryName"] == repo_name
            assert resp["status"] in ("IN_PROGRESS", "COMPLETE")
            assert "previewResults" in resp
        finally:
            ecr.delete_repository(repositoryName=repo_name, force=True)


class TestECRSigningConfiguration:
    """Tests for ECR signing configuration operations."""

    def test_get_signing_configuration(self, ecr):
        """GetSigningConfiguration returns registry signing config."""
        resp = ecr.get_signing_configuration()
        assert "registryId" in resp
        assert "signingConfiguration" in resp
        assert "rules" in resp["signingConfiguration"]
        assert isinstance(resp["signingConfiguration"]["rules"], list)
