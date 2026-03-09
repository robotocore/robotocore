"""Compatibility tests for AWS Signer service."""

import uuid

import pytest

from tests.compatibility.conftest import make_client


@pytest.fixture
def signer():
    return make_client("signer")


def _unique(prefix):
    return f"{prefix}-{uuid.uuid4().hex[:8]}"


class TestSignerProfileOperations:
    """Tests for signing profile CRUD operations."""

    def test_put_signing_profile_returns_arn(self, signer):
        name = _unique("profile")
        resp = signer.put_signing_profile(
            profileName=name,
            platformId="AWSLambda-SHA384-ECDSA",
        )
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200
        assert "arn" in resp
        assert name in resp["arn"]
        assert "profileVersion" in resp
        assert "profileVersionArn" in resp
        signer.cancel_signing_profile(profileName=name)

    def test_get_signing_profile(self, signer):
        name = _unique("profile")
        put_resp = signer.put_signing_profile(
            profileName=name,
            platformId="AWSLambda-SHA384-ECDSA",
        )
        resp = signer.get_signing_profile(profileName=name)
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200
        assert resp["profileName"] == name
        assert resp["platformId"] == "AWSLambda-SHA384-ECDSA"
        assert resp["status"] == "Active"
        assert resp["arn"] == put_resp["arn"]
        assert resp["profileVersion"] == put_resp["profileVersion"]
        signer.cancel_signing_profile(profileName=name)

    def test_cancel_signing_profile(self, signer):
        name = _unique("profile")
        signer.put_signing_profile(
            profileName=name,
            platformId="AWSLambda-SHA384-ECDSA",
        )
        resp = signer.cancel_signing_profile(profileName=name)
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200

        # Verify status is Canceled after cancellation
        get_resp = signer.get_signing_profile(profileName=name)
        assert get_resp["status"] == "Canceled"

    def test_put_signing_profile_with_tags(self, signer):
        name = _unique("profile")
        signer.put_signing_profile(
            profileName=name,
            platformId="AWSLambda-SHA384-ECDSA",
            tags={"env": "test", "team": "dev"},
        )
        resp = signer.get_signing_profile(profileName=name)
        assert resp.get("tags") == {"env": "test", "team": "dev"}
        signer.cancel_signing_profile(profileName=name)

    def test_put_signing_profile_version_is_unique(self, signer):
        name1 = _unique("profile")
        name2 = _unique("profile")
        r1 = signer.put_signing_profile(profileName=name1, platformId="AWSLambda-SHA384-ECDSA")
        r2 = signer.put_signing_profile(profileName=name2, platformId="AWSLambda-SHA384-ECDSA")
        assert r1["profileVersion"] != r2["profileVersion"]
        signer.cancel_signing_profile(profileName=name1)
        signer.cancel_signing_profile(profileName=name2)


class TestSignerPlatformOperations:
    """Tests for signing platform list operations."""

    def test_list_signing_platforms(self, signer):
        resp = signer.list_signing_platforms()
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200
        assert "platforms" in resp
        assert len(resp["platforms"]) > 0

    def test_list_signing_platforms_contains_lambda(self, signer):
        resp = signer.list_signing_platforms()
        platform_ids = [p["platformId"] for p in resp["platforms"]]
        assert "AWSLambda-SHA384-ECDSA" in platform_ids

    def test_list_signing_platforms_have_display_names(self, signer):
        resp = signer.list_signing_platforms()
        for platform in resp["platforms"]:
            assert "platformId" in platform
            assert "displayName" in platform

    def test_get_signing_platform(self, signer):
        resp = signer.get_signing_platform(platformId="AWSLambda-SHA384-ECDSA")
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200
        assert resp["platformId"] == "AWSLambda-SHA384-ECDSA"
        assert "displayName" in resp

    def test_get_signing_platform_not_found(self, signer):
        with pytest.raises(signer.exceptions.ResourceNotFoundException):
            signer.get_signing_platform(platformId="NonExistentPlatform-12345")


class TestSignerListOperations:
    """Tests for listing operations."""

    def test_list_signing_profiles(self, signer):
        name = _unique("profile")
        signer.put_signing_profile(profileName=name, platformId="AWSLambda-SHA384-ECDSA")
        try:
            resp = signer.list_signing_profiles()
            assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200
            assert "profiles" in resp
            profile_names = [p["profileName"] for p in resp["profiles"]]
            assert name in profile_names
        finally:
            signer.cancel_signing_profile(profileName=name)

    def test_list_signing_jobs(self, signer):
        resp = signer.list_signing_jobs()
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200
        assert "jobs" in resp
        assert isinstance(resp["jobs"], list)


class TestSignerJobOperations:
    """Tests for signing job operations."""

    def test_describe_signing_job_not_found(self, signer):
        with pytest.raises(signer.exceptions.ResourceNotFoundException):
            signer.describe_signing_job(jobId="00000000-0000-0000-0000-000000000000")

    def test_start_signing_job(self, signer):
        name = _unique("profile")
        signer.put_signing_profile(profileName=name, platformId="AWSLambda-SHA384-ECDSA")
        try:
            resp = signer.start_signing_job(
                source={
                    "s3": {
                        "bucketName": "test-bucket",
                        "key": "test-input.zip",
                        "version": "v1",
                    }
                },
                destination={
                    "s3": {
                        "bucketName": "test-bucket",
                        "prefix": "signed/",
                    }
                },
                profileName=name,
            )
            assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200
            assert "jobId" in resp
            assert "jobOwner" in resp
        finally:
            signer.cancel_signing_profile(profileName=name)

    def test_sign_payload(self, signer):
        name = _unique("profile")
        put_resp = signer.put_signing_profile(profileName=name, platformId="AWSLambda-SHA384-ECDSA")
        try:
            resp = signer.sign_payload(
                profileName=name,
                profileOwner=put_resp["arn"].split(":")[4],
                payload=b"hello world",
                payloadFormat="MessageDigest",
            )
            assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200
            assert "signature" in resp or "jobId" in resp or "metadata" in resp
        finally:
            signer.cancel_signing_profile(profileName=name)


class TestSignerPermissionOperations:
    """Tests for signing profile permission operations."""

    def test_list_profile_permissions_not_found(self, signer):
        with pytest.raises(signer.exceptions.ResourceNotFoundException):
            signer.list_profile_permissions(profileName="nonexistent-profile-xyz")

    def test_add_profile_permission_not_found(self, signer):
        with pytest.raises(signer.exceptions.ResourceNotFoundException):
            signer.add_profile_permission(
                profileName="nonexistent-profile-xyz",
                action="signer:StartSigningJob",
                principal="123456789012",
                statementId="stmt1",
            )

    def test_add_and_list_profile_permissions(self, signer):
        name = _unique("profile")
        signer.put_signing_profile(profileName=name, platformId="AWSLambda-SHA384-ECDSA")
        try:
            add_resp = signer.add_profile_permission(
                profileName=name,
                action="signer:StartSigningJob",
                principal="123456789012",
                statementId="stmt1",
            )
            assert add_resp["ResponseMetadata"]["HTTPStatusCode"] == 200
            assert "revisionId" in add_resp

            list_resp = signer.list_profile_permissions(profileName=name)
            assert list_resp["ResponseMetadata"]["HTTPStatusCode"] == 200
            assert "permissions" in list_resp
            assert len(list_resp["permissions"]) >= 1
        finally:
            signer.cancel_signing_profile(profileName=name)

    def test_remove_profile_permission(self, signer):
        name = _unique("profile")
        signer.put_signing_profile(profileName=name, platformId="AWSLambda-SHA384-ECDSA")
        try:
            add_resp = signer.add_profile_permission(
                profileName=name,
                action="signer:StartSigningJob",
                principal="123456789012",
                statementId="stmt1",
            )
            revision_id = add_resp["revisionId"]
            remove_resp = signer.remove_profile_permission(
                profileName=name,
                statementId="stmt1",
                revisionId=revision_id,
            )
            assert remove_resp["ResponseMetadata"]["HTTPStatusCode"] == 200
            assert "revisionId" in remove_resp
        finally:
            signer.cancel_signing_profile(profileName=name)


class TestSignerRevokeOperations:
    """Tests for revocation operations."""

    def test_revoke_signature_not_found(self, signer):
        with pytest.raises(signer.exceptions.ResourceNotFoundException):
            signer.revoke_signature(
                jobId="00000000-0000-0000-0000-000000000000",
                reason="test revocation",
            )

    def test_revoke_signing_profile(self, signer):
        name = _unique("profile")
        put_resp = signer.put_signing_profile(profileName=name, platformId="AWSLambda-SHA384-ECDSA")
        resp = signer.revoke_signing_profile(
            profileName=name,
            profileVersion=put_resp["profileVersion"],
            reason="test revocation",
            effectiveTime="2025-01-01T00:00:00Z",
        )
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200
