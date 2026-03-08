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
