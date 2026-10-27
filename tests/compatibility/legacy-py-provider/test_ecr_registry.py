"""ECR OCI Registry v2 data plane compatibility tests.

Tests the Docker/ECR-compatible registry endpoints for pushing and pulling images.
"""

import base64
import hashlib
import json
import os
import uuid

import httpx
import pytest

from tests.compatibility.conftest import make_client

ENDPOINT_URL = os.environ.get("ENDPOINT_URL", "http://localhost:4566")
REGISTRY_URL = f"{ENDPOINT_URL}/v2"


@pytest.fixture
def ecr():
    return make_client("ecr")


def _unique(prefix: str) -> str:
    return f"{prefix}-{uuid.uuid4().hex[:8]}"


def _get_auth_token(ecr_client, account_id: str = "123456789012") -> str:
    """Get ECR authorization token for registry access."""
    response = ecr_client.get_authorization_token()
    token = response["authorizationData"][0]["authorizationToken"]
    return token


def _compute_digest(content: bytes) -> str:
    """Compute sha256 digest for content."""
    return "sha256:" + hashlib.sha256(content).hexdigest()


class TestRegistryV2Ping:
    """Test registry availability endpoint."""

    def test_v2_ping(self, ecr):
        """GET /v2/ should return 200 with API version header."""
        token = _get_auth_token(ecr)
        headers = {"Authorization": f"Basic {token}"}

        response = httpx.get(
            f"{REGISTRY_URL}/",
            headers=headers,
            timeout=10.0,
        )

        assert response.status_code == 200
        assert response.headers.get("Docker-Distribution-API-Version") == "registry/2.0"


class TestRegistryManifestOperations:
    """Test manifest push/pull/delete operations."""

    def test_push_and_get_manifest_by_tag(self, ecr):
        """Push a manifest by tag and retrieve it."""
        repo_name = _unique("test-repo")
        ecr.create_repository(repositoryName=repo_name)

        try:
            token = _get_auth_token(ecr)
            auth_b64 = base64.b64encode(f"AWS:{token}".encode()).decode()
            headers = {
                "Authorization": f"Basic {auth_b64}",
                "Content-Type": "application/vnd.docker.distribution.manifest.v2+json",
            }

            # Create a minimal manifest
            manifest = {
                "schemaVersion": 2,
                "mediaType": "application/vnd.docker.distribution.manifest.v2+json",
                "config": {
                    "mediaType": "application/vnd.docker.container.image.v1+json",
                    "size": 7023,
                    "digest": "sha256:" + "a" * 64,
                },
                "layers": [
                    {
                        "mediaType": "application/vnd.docker.image.rootfs.diff.tar.gzip",
                        "size": 32654,
                        "digest": "sha256:" + "b" * 64,
                    }
                ],
            }
            manifest_bytes = json.dumps(manifest).encode()
            tag = "latest"

            # Push manifest
            put_response = httpx.put(
                f"{REGISTRY_URL}/{repo_name}/manifests/{tag}",
                content=manifest_bytes,
                headers=headers,
                timeout=10.0,
            )
            assert put_response.status_code == 201, f"Push failed: {put_response.text}"
            assert "Docker-Content-Digest" in put_response.headers

            # Get manifest by tag
            get_response = httpx.get(
                f"{REGISTRY_URL}/{repo_name}/manifests/{tag}",
                headers={"Authorization": f"Basic {auth_b64}"},
                timeout=10.0,
            )
            assert get_response.status_code == 200
            assert get_response.headers.get("Content-Type") == manifest["mediaType"]
            retrieved_manifest = json.loads(get_response.content)
            assert retrieved_manifest["schemaVersion"] == 2

            # Get manifest by digest
            digest = put_response.headers["Docker-Content-Digest"]
            get_by_digest = httpx.get(
                f"{REGISTRY_URL}/{repo_name}/manifests/{digest}",
                headers={"Authorization": f"Basic {auth_b64}"},
                timeout=10.0,
            )
            assert get_by_digest.status_code == 200

        finally:
            ecr.delete_repository(repositoryName=repo_name, force=True)

    def test_manifest_not_found(self, ecr):
        """Requesting a non-existent manifest returns 404."""
        repo_name = _unique("test-repo")
        ecr.create_repository(repositoryName=repo_name)

        try:
            token = _get_auth_token(ecr)
            auth_b64 = base64.b64encode(f"AWS:{token}".encode()).decode()

            response = httpx.get(
                f"{REGISTRY_URL}/{repo_name}/manifests/nonexistent",
                headers={"Authorization": f"Basic {auth_b64}"},
                timeout=10.0,
            )
            assert response.status_code == 404
            error_data = json.loads(response.content)
            assert error_data["errors"][0]["code"] == "MANIFEST_UNKNOWN"

        finally:
            ecr.delete_repository(repositoryName=repo_name, force=True)

    def test_delete_manifest(self, ecr):
        """Delete a manifest by digest."""
        repo_name = _unique("test-repo")
        ecr.create_repository(repositoryName=repo_name)

        try:
            token = _get_auth_token(ecr)
            auth_b64 = base64.b64encode(f"AWS:{token}".encode()).decode()
            headers = {
                "Authorization": f"Basic {auth_b64}",
                "Content-Type": "application/vnd.docker.distribution.manifest.v2+json",
            }

            # Push a manifest
            manifest = {
                "schemaVersion": 2,
                "mediaType": "application/vnd.docker.distribution.manifest.v2+json",
                "config": {
                    "mediaType": "application/vnd.docker.container.image.v1+json",
                    "size": 7023,
                    "digest": "sha256:" + "a" * 64,
                },
                "layers": [],
            }
            manifest_bytes = json.dumps(manifest).encode()

            put_response = httpx.put(
                f"{REGISTRY_URL}/{repo_name}/manifests/test-tag",
                content=manifest_bytes,
                headers=headers,
                timeout=10.0,
            )
            assert put_response.status_code == 201

            # Delete by digest
            digest = put_response.headers["Docker-Content-Digest"]
            delete_response = httpx.delete(
                f"{REGISTRY_URL}/{repo_name}/manifests/{digest}",
                headers={"Authorization": f"Basic {auth_b64}"},
                timeout=10.0,
            )
            assert delete_response.status_code == 202

            # Verify it's gone
            get_response = httpx.get(
                f"{REGISTRY_URL}/{repo_name}/manifests/{digest}",
                headers={"Authorization": f"Basic {auth_b64}"},
                timeout=10.0,
            )
            assert get_response.status_code == 404

        finally:
            ecr.delete_repository(repositoryName=repo_name, force=True)


class TestRegistryTagImmutability:
    """Test image tag immutability enforcement."""

    def test_immutable_tag_rejection(self, ecr):
        """Pushing to an existing tag in IMMUTABLE repo should fail."""
        repo_name = _unique("immutable-repo")
        ecr.create_repository(
            repositoryName=repo_name,
            imageTagMutability="IMMUTABLE",
        )

        try:
            token = _get_auth_token(ecr)
            auth_b64 = base64.b64encode(f"AWS:{token}".encode()).decode()
            headers = {
                "Authorization": f"Basic {auth_b64}",
                "Content-Type": "application/vnd.docker.distribution.manifest.v2+json",
            }

            # Push first manifest
            manifest1 = {
                "schemaVersion": 2,
                "mediaType": "application/vnd.docker.distribution.manifest.v2+json",
                "config": {
                    "mediaType": "application/vnd.docker.container.image.v1+json",
                    "size": 7023,
                    "digest": "sha256:" + "a" * 64,
                },
                "layers": [],
            }

            put1 = httpx.put(
                f"{REGISTRY_URL}/{repo_name}/manifests/v1.0",
                content=json.dumps(manifest1).encode(),
                headers=headers,
                timeout=10.0,
            )
            assert put1.status_code == 201

            # Try to push different manifest to same tag (should fail)
            manifest2 = {
                "schemaVersion": 2,
                "mediaType": "application/vnd.docker.distribution.manifest.v2+json",
                "config": {
                    "mediaType": "application/vnd.docker.container.image.v1+json",
                    "size": 7023,
                    "digest": "sha256:" + "c" * 64,  # Different digest
                },
                "layers": [],
            }

            put2 = httpx.put(
                f"{REGISTRY_URL}/{repo_name}/manifests/v1.0",
                content=json.dumps(manifest2).encode(),
                headers=headers,
                timeout=10.0,
            )
            # Should get an error for immutable tag
            assert put2.status_code in [400, 409]

        finally:
            ecr.delete_repository(repositoryName=repo_name, force=True)

    def test_mutable_tag_allows_overwrite(self, ecr):
        """Pushing to an existing tag in MUTABLE repo should succeed."""
        repo_name = _unique("mutable-repo")
        ecr.create_repository(
            repositoryName=repo_name,
            imageTagMutability="MUTABLE",
        )

        try:
            token = _get_auth_token(ecr)
            auth_b64 = base64.b64encode(f"AWS:{token}".encode()).decode()
            headers = {
                "Authorization": f"Basic {auth_b64}",
                "Content-Type": "application/vnd.docker.distribution.manifest.v2+json",
            }

            # Push first manifest
            manifest1 = {
                "schemaVersion": 2,
                "mediaType": "application/vnd.docker.distribution.manifest.v2+json",
                "config": {
                    "mediaType": "application/vnd.docker.container.image.v1+json",
                    "size": 7023,
                    "digest": "sha256:" + "a" * 64,
                },
                "layers": [],
            }

            put1 = httpx.put(
                f"{REGISTRY_URL}/{repo_name}/manifests/latest",
                content=json.dumps(manifest1).encode(),
                headers=headers,
                timeout=10.0,
            )
            assert put1.status_code == 201

            # Push different manifest to same tag (should succeed in MUTABLE repo)
            manifest2 = {
                "schemaVersion": 2,
                "mediaType": "application/vnd.docker.distribution.manifest.v2+json",
                "config": {
                    "mediaType": "application/vnd.docker.container.image.v1+json",
                    "size": 7023,
                    "digest": "sha256:" + "d" * 64,
                },
                "layers": [],
            }

            put2 = httpx.put(
                f"{REGISTRY_URL}/{repo_name}/manifests/latest",
                content=json.dumps(manifest2).encode(),
                headers=headers,
                timeout=10.0,
            )
            assert put2.status_code == 201

        finally:
            ecr.delete_repository(repositoryName=repo_name, force=True)


class TestRegistryBlobOperations:
    """Test blob (layer) operations."""

    def test_head_blob_exists(self, ecr):
        """HEAD request for existing blob should return 200."""
        repo_name = _unique("test-repo")
        ecr.create_repository(repositoryName=repo_name)

        try:
            token = _get_auth_token(ecr)
            auth_b64 = base64.b64encode(f"AWS:{token}".encode()).decode()

            # Create a layer digest
            layer_content = b"test layer content"
            layer_digest = _compute_digest(layer_content)

            # Push a manifest that references this layer
            manifest = {
                "schemaVersion": 2,
                "mediaType": "application/vnd.docker.distribution.manifest.v2+json",
                "config": {
                    "mediaType": "application/vnd.docker.container.image.v1+json",
                    "size": 7023,
                    "digest": "sha256:" + "a" * 64,
                },
                "layers": [
                    {
                        "mediaType": "application/vnd.docker.image.rootfs.diff.tar.gzip",
                        "size": len(layer_content),
                        "digest": layer_digest,
                    }
                ],
            }

            headers = {
                "Authorization": f"Basic {auth_b64}",
                "Content-Type": "application/vnd.docker.distribution.manifest.v2+json",
            }

            httpx.put(
                f"{REGISTRY_URL}/{repo_name}/manifests/test",
                content=json.dumps(manifest).encode(),
                headers=headers,
                timeout=10.0,
            )

            # HEAD the blob
            head_response = httpx.head(
                f"{REGISTRY_URL}/{repo_name}/blobs/{layer_digest}",
                headers={"Authorization": f"Basic {auth_b64}"},
                timeout=10.0,
            )
            assert head_response.status_code == 200
            assert head_response.headers.get("Docker-Content-Digest") == layer_digest

        finally:
            ecr.delete_repository(repositoryName=repo_name, force=True)

    def test_head_blob_not_found(self, ecr):
        """HEAD request for non-existent blob should return 404."""
        repo_name = _unique("test-repo")
        ecr.create_repository(repositoryName=repo_name)

        try:
            token = _get_auth_token(ecr)
            auth_b64 = base64.b64encode(f"AWS:{token}".encode()).decode()

            head_response = httpx.head(
                f"{REGISTRY_URL}/{repo_name}/blobs/sha256:{'0' * 64}",
                headers={"Authorization": f"Basic {auth_b64}"},
                timeout=10.0,
            )
            assert head_response.status_code == 404

        finally:
            ecr.delete_repository(repositoryName=repo_name, force=True)


class TestRegistryUploadOperations:
    """Test blob upload session flow."""

    def test_start_upload(self, ecr):
        """POST /v2/{name}/blobs/uploads/ should start an upload session."""
        repo_name = _unique("test-repo")
        ecr.create_repository(repositoryName=repo_name)

        try:
            token = _get_auth_token(ecr)
            auth_b64 = base64.b64encode(f"AWS:{token}".encode()).decode()

            response = httpx.post(
                f"{REGISTRY_URL}/{repo_name}/blobs/uploads/",
                headers={"Authorization": f"Basic {auth_b64}"},
                timeout=10.0,
            )
            assert response.status_code == 202
            assert "Location" in response.headers
            assert "Docker-Upload-UUID" in response.headers

        finally:
            ecr.delete_repository(repositoryName=repo_name, force=True)

    def test_upload_chunk_and_complete(self, ecr):
        """Test full upload flow: start, chunk, complete."""
        repo_name = _unique("test-repo")
        ecr.create_repository(repositoryName=repo_name)

        try:
            token = _get_auth_token(ecr)
            auth_b64 = base64.b64encode(f"AWS:{token}".encode()).decode()

            # Start upload
            start_response = httpx.post(
                f"{REGISTRY_URL}/{repo_name}/blobs/uploads/",
                headers={"Authorization": f"Basic {auth_b64}"},
                timeout=10.0,
            )
            assert start_response.status_code == 202

            upload_url = start_response.headers["Location"]
            # Verify Docker-Upload-UUID header is present
            assert "Docker-Upload-UUID" in start_response.headers

            # Upload content
            content = b"test blob content"
            digest = _compute_digest(content)

            # Complete upload with digest
            complete_response = httpx.put(
                f"{ENDPOINT_URL}{upload_url}?digest={digest}",
                content=content,
                headers={"Authorization": f"Basic {auth_b64}"},
                timeout=10.0,
            )
            assert complete_response.status_code == 201
            assert complete_response.headers.get("Docker-Content-Digest") == digest

        finally:
            ecr.delete_repository(repositoryName=repo_name, force=True)

    def test_cancel_upload(self, ecr):
        """DELETE upload session should cancel it."""
        repo_name = _unique("test-repo")
        ecr.create_repository(repositoryName=repo_name)

        try:
            token = _get_auth_token(ecr)
            auth_b64 = base64.b64encode(f"AWS:{token}".encode()).decode()

            # Start upload
            start_response = httpx.post(
                f"{REGISTRY_URL}/{repo_name}/blobs/uploads/",
                headers={"Authorization": f"Basic {auth_b64}"},
                timeout=10.0,
            )
            assert start_response.status_code == 202

            upload_url = start_response.headers["Location"]

            # Cancel upload
            cancel_response = httpx.delete(
                f"{ENDPOINT_URL}{upload_url}",
                headers={"Authorization": f"Basic {auth_b64}"},
                timeout=10.0,
            )
            assert cancel_response.status_code == 204

        finally:
            ecr.delete_repository(repositoryName=repo_name, force=True)

    def test_upload_and_get_blob(self, ecr):
        """Upload a blob and then GET it back, verifying bytes match."""
        repo_name = _unique("test-repo")
        ecr.create_repository(repositoryName=repo_name)

        try:
            token = _get_auth_token(ecr)
            auth_b64 = base64.b64encode(f"AWS:{token}".encode()).decode()

            # Start upload
            start_response = httpx.post(
                f"{REGISTRY_URL}/{repo_name}/blobs/uploads/",
                headers={"Authorization": f"Basic {auth_b64}"},
                timeout=10.0,
            )
            assert start_response.status_code == 202

            upload_url = start_response.headers["Location"]

            # Upload content
            content = b"test blob content for round-trip verification"
            digest = _compute_digest(content)

            # Complete upload with digest
            complete_response = httpx.put(
                f"{ENDPOINT_URL}{upload_url}?digest={digest}",
                content=content,
                headers={"Authorization": f"Basic {auth_b64}"},
                timeout=10.0,
            )
            assert complete_response.status_code == 201
            assert complete_response.headers.get("Docker-Content-Digest") == digest

            # Now GET the blob back
            get_response = httpx.get(
                f"{REGISTRY_URL}/{repo_name}/blobs/{digest}",
                headers={"Authorization": f"Basic {auth_b64}"},
                timeout=10.0,
            )
            assert get_response.status_code == 200
            assert get_response.content == content, (
                "Retrieved blob content should match uploaded content"
            )
            assert get_response.headers.get("Docker-Content-Digest") == digest
            assert get_response.headers.get("Content-Length") == str(len(content))

            # Also test HEAD request
            head_response = httpx.head(
                f"{REGISTRY_URL}/{repo_name}/blobs/{digest}",
                headers={"Authorization": f"Basic {auth_b64}"},
                timeout=10.0,
            )
            assert head_response.status_code == 200
            assert head_response.headers.get("Docker-Content-Digest") == digest
            assert head_response.headers.get("Content-Length") == str(len(content))

        finally:
            ecr.delete_repository(repositoryName=repo_name, force=True)


class TestRegistryTagList:
    """Test tag listing endpoint."""

    def test_list_tags(self, ecr):
        """GET /v2/{name}/tags/list should return all tags."""
        repo_name = _unique("test-repo")
        ecr.create_repository(repositoryName=repo_name)

        try:
            token = _get_auth_token(ecr)
            auth_b64 = base64.b64encode(f"AWS:{token}".encode()).decode()
            headers = {
                "Authorization": f"Basic {auth_b64}",
                "Content-Type": "application/vnd.docker.distribution.manifest.v2+json",
            }

            # Push manifests with different tags
            manifest = {
                "schemaVersion": 2,
                "mediaType": "application/vnd.docker.distribution.manifest.v2+json",
                "config": {
                    "mediaType": "application/vnd.docker.container.image.v1+json",
                    "size": 7023,
                    "digest": "sha256:" + "a" * 64,
                },
                "layers": [],
            }

            tags = ["v1.0", "v2.0", "latest"]
            for tag in tags:
                httpx.put(
                    f"{REGISTRY_URL}/{repo_name}/manifests/{tag}",
                    content=json.dumps(manifest).encode(),
                    headers=headers,
                    timeout=10.0,
                )

            # List tags
            list_response = httpx.get(
                f"{REGISTRY_URL}/{repo_name}/tags/list",
                headers={"Authorization": f"Basic {auth_b64}"},
                timeout=10.0,
            )
            assert list_response.status_code == 200

            data = json.loads(list_response.content)
            assert data["name"] == repo_name
            assert set(data["tags"]) == set(tags)

        finally:
            ecr.delete_repository(repositoryName=repo_name, force=True)


class TestRegistryIntegrationWithECRControlPlane:
    """Test that registry operations integrate with ECR control plane."""

    def test_push_via_registry_visible_in_batch_get_image(self, ecr):
        """Image pushed via /v2/ should be visible via BatchGetImage."""
        repo_name = _unique("test-repo")
        ecr.create_repository(repositoryName=repo_name)

        try:
            token = _get_auth_token(ecr)
            auth_b64 = base64.b64encode(f"AWS:{token}".encode()).decode()
            headers = {
                "Authorization": f"Basic {auth_b64}",
                "Content-Type": "application/vnd.docker.distribution.manifest.v2+json",
            }

            # Push via registry
            manifest = {
                "schemaVersion": 2,
                "mediaType": "application/vnd.docker.distribution.manifest.v2+json",
                "config": {
                    "mediaType": "application/vnd.docker.container.image.v1+json",
                    "size": 7023,
                    "digest": "sha256:" + "a" * 64,
                },
                "layers": [],
            }
            tag = "registry-pushed"

            put_response = httpx.put(
                f"{REGISTRY_URL}/{repo_name}/manifests/{tag}",
                content=json.dumps(manifest).encode(),
                headers=headers,
                timeout=10.0,
            )
            assert put_response.status_code == 201
            digest = put_response.headers["Docker-Content-Digest"]

            # Verify via BatchGetImage
            batch_response = ecr.batch_get_image(
                repositoryName=repo_name,
                imageIds=[{"imageTag": tag}],
            )
            assert len(batch_response["images"]) == 1
            assert batch_response["images"][0]["imageId"]["imageTag"] == tag
            assert batch_response["images"][0]["imageId"]["imageDigest"] == digest

        finally:
            ecr.delete_repository(repositoryName=repo_name, force=True)

    def test_put_image_visible_via_registry(self, ecr):
        """Image pushed via PutImage should be visible via /v2/."""
        repo_name = _unique("test-repo")
        ecr.create_repository(repositoryName=repo_name)

        try:
            # Push via PutImage API
            manifest = {
                "schemaVersion": 2,
                "mediaType": "application/vnd.docker.distribution.manifest.v2+json",
                "config": {
                    "mediaType": "application/vnd.docker.container.image.v1+json",
                    "size": 7023,
                    "digest": "sha256:" + "a" * 64,
                },
                "layers": [],
            }
            tag = "api-pushed"

            ecr.put_image(
                repositoryName=repo_name,
                imageManifest=json.dumps(manifest),
                imageTag=tag,
            )

            # Verify via registry
            token = _get_auth_token(ecr)
            auth_b64 = base64.b64encode(f"AWS:{token}".encode()).decode()

            get_response = httpx.get(
                f"{REGISTRY_URL}/{repo_name}/manifests/{tag}",
                headers={"Authorization": f"Basic {auth_b64}"},
                timeout=10.0,
            )
            assert get_response.status_code == 200
            retrieved = json.loads(get_response.content)
            assert retrieved["schemaVersion"] == 2

        finally:
            ecr.delete_repository(repositoryName=repo_name, force=True)


class TestRegistryAuthentication:
    """Test authentication requirements."""

    def test_unauthenticated_request_rejected(self, ecr):
        """Requests without valid auth should be rejected."""
        repo_name = _unique("test-repo")
        ecr.create_repository(repositoryName=repo_name)

        try:
            # Request without auth
            response = httpx.get(
                f"{REGISTRY_URL}/{repo_name}/manifests/latest",
                timeout=10.0,
            )
            assert response.status_code == 401
            assert "errors" in json.loads(response.content)

        finally:
            ecr.delete_repository(repositoryName=repo_name, force=True)

    def test_bearer_token_auth(self, ecr):
        """Bearer token authentication should work."""
        repo_name = _unique("test-repo")
        ecr.create_repository(repositoryName=repo_name)

        try:
            token = _get_auth_token(ecr)

            # Use Bearer token
            response = httpx.get(
                f"{REGISTRY_URL}/",
                headers={"Authorization": f"Bearer {token}"},
                timeout=10.0,
            )
            assert response.status_code == 200

        finally:
            ecr.delete_repository(repositoryName=repo_name, force=True)
