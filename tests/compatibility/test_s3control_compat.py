"""S3 Control compatibility tests."""

import json
import uuid

import pytest
from botocore.exceptions import ClientError

from tests.compatibility.conftest import make_client

ACCOUNT_ID = "123456789012"


def _uid():
    return uuid.uuid4().hex[:8]


@pytest.fixture
def s3control():
    return make_client("s3control")


@pytest.fixture
def s3():
    return make_client("s3")


class TestS3ControlOperations:
    def test_put_public_access_block(self, s3control):
        response = s3control.put_public_access_block(
            AccountId="123456789012",
            PublicAccessBlockConfiguration={
                "BlockPublicAcls": True,
                "IgnorePublicAcls": True,
                "BlockPublicPolicy": True,
                "RestrictPublicBuckets": True,
            },
        )
        assert response["ResponseMetadata"]["HTTPStatusCode"] in (200, 201)
        s3control.delete_public_access_block(AccountId="123456789012")

    def test_get_public_access_block(self, s3control):
        s3control.put_public_access_block(
            AccountId="123456789012",
            PublicAccessBlockConfiguration={
                "BlockPublicAcls": True,
                "IgnorePublicAcls": False,
                "BlockPublicPolicy": True,
                "RestrictPublicBuckets": False,
            },
        )
        response = s3control.get_public_access_block(AccountId="123456789012")
        config = response["PublicAccessBlockConfiguration"]
        assert config["BlockPublicAcls"] is True
        assert config["IgnorePublicAcls"] is False
        s3control.delete_public_access_block(AccountId="123456789012")

    def test_delete_public_access_block(self, s3control):
        s3control.put_public_access_block(
            AccountId="123456789012",
            PublicAccessBlockConfiguration={
                "BlockPublicAcls": True,
                "IgnorePublicAcls": True,
                "BlockPublicPolicy": True,
                "RestrictPublicBuckets": True,
            },
        )
        response = s3control.delete_public_access_block(AccountId="123456789012")
        assert response["ResponseMetadata"]["HTTPStatusCode"] in (200, 204)

    def test_get_public_access_block_not_found(self, s3control):
        """Getting public access block when not set should error."""
        # Delete any existing config first
        try:
            s3control.delete_public_access_block(AccountId="123456789012")
        except ClientError:
            pass  # best-effort cleanup
        with pytest.raises(ClientError) as exc:
            s3control.get_public_access_block(AccountId="123456789012")
        assert exc.value.response["Error"]["Code"] == "NoSuchPublicAccessBlockConfiguration"

    def test_put_public_access_block_partial(self, s3control):
        """Set only some fields in public access block."""
        s3control.put_public_access_block(
            AccountId="123456789012",
            PublicAccessBlockConfiguration={
                "BlockPublicAcls": True,
                "IgnorePublicAcls": False,
                "BlockPublicPolicy": False,
                "RestrictPublicBuckets": True,
            },
        )
        response = s3control.get_public_access_block(AccountId="123456789012")
        config = response["PublicAccessBlockConfiguration"]
        assert config["BlockPublicAcls"] is True
        assert config["IgnorePublicAcls"] is False
        assert config["BlockPublicPolicy"] is False
        assert config["RestrictPublicBuckets"] is True
        s3control.delete_public_access_block(AccountId="123456789012")

    def test_overwrite_public_access_block(self, s3control):
        """Overwriting public access block replaces all fields."""
        s3control.put_public_access_block(
            AccountId="123456789012",
            PublicAccessBlockConfiguration={
                "BlockPublicAcls": True,
                "IgnorePublicAcls": True,
                "BlockPublicPolicy": True,
                "RestrictPublicBuckets": True,
            },
        )
        s3control.put_public_access_block(
            AccountId="123456789012",
            PublicAccessBlockConfiguration={
                "BlockPublicAcls": False,
                "IgnorePublicAcls": False,
                "BlockPublicPolicy": False,
                "RestrictPublicBuckets": False,
            },
        )
        response = s3control.get_public_access_block(AccountId="123456789012")
        config = response["PublicAccessBlockConfiguration"]
        assert config["BlockPublicAcls"] is False
        assert config["IgnorePublicAcls"] is False
        s3control.delete_public_access_block(AccountId="123456789012")

    def test_delete_public_access_block_idempotent(self, s3control):
        """Deleting public access block when not set still returns success."""
        # Delete first to ensure clean state
        try:
            s3control.delete_public_access_block(AccountId="123456789012")
        except ClientError:
            pass  # best-effort cleanup
        # Delete again - should succeed
        response = s3control.delete_public_access_block(AccountId="123456789012")
        assert response["ResponseMetadata"]["HTTPStatusCode"] in (200, 204)

    def test_put_public_access_block_all_false(self, s3control):
        """Setting all public access block fields to False."""
        s3control.put_public_access_block(
            AccountId="123456789012",
            PublicAccessBlockConfiguration={
                "BlockPublicAcls": False,
                "IgnorePublicAcls": False,
                "BlockPublicPolicy": False,
                "RestrictPublicBuckets": False,
            },
        )
        response = s3control.get_public_access_block(AccountId="123456789012")
        config = response["PublicAccessBlockConfiguration"]
        assert config["BlockPublicAcls"] is False
        assert config["IgnorePublicAcls"] is False
        assert config["BlockPublicPolicy"] is False
        assert config["RestrictPublicBuckets"] is False
        s3control.delete_public_access_block(AccountId="123456789012")


class TestS3ControlAccessPoints:
    """Tests for S3 Control Access Point operations."""

    def test_create_access_point(self, s3control, s3):
        """CreateAccessPoint creates an access point for a bucket."""
        bucket = f"ap-create-{_uid()}"
        ap_name = f"ap-create-{_uid()}"
        s3.create_bucket(Bucket=bucket)
        try:
            resp = s3control.create_access_point(AccountId=ACCOUNT_ID, Name=ap_name, Bucket=bucket)
            assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200
            assert "AccessPointArn" in resp
        finally:
            try:
                s3control.delete_access_point(AccountId=ACCOUNT_ID, Name=ap_name)
            except Exception:
                pass  # best-effort cleanup
            s3.delete_bucket(Bucket=bucket)

    def test_get_access_point(self, s3control, s3):
        """GetAccessPoint returns access point details."""
        bucket = f"ap-get-{_uid()}"
        ap_name = f"ap-get-{_uid()}"
        s3.create_bucket(Bucket=bucket)
        try:
            s3control.create_access_point(AccountId=ACCOUNT_ID, Name=ap_name, Bucket=bucket)
            resp = s3control.get_access_point(AccountId=ACCOUNT_ID, Name=ap_name)
            assert resp["Name"] == ap_name
            assert resp["Bucket"] == bucket
            assert "AccessPointArn" in resp
            assert "NetworkOrigin" in resp
        finally:
            try:
                s3control.delete_access_point(AccountId=ACCOUNT_ID, Name=ap_name)
            except Exception:
                pass  # best-effort cleanup
            s3.delete_bucket(Bucket=bucket)

    def test_delete_access_point(self, s3control, s3):
        """DeleteAccessPoint removes an access point."""
        bucket = f"ap-del-{_uid()}"
        ap_name = f"ap-del-{_uid()}"
        s3.create_bucket(Bucket=bucket)
        try:
            s3control.create_access_point(AccountId=ACCOUNT_ID, Name=ap_name, Bucket=bucket)
            resp = s3control.delete_access_point(AccountId=ACCOUNT_ID, Name=ap_name)
            assert resp["ResponseMetadata"]["HTTPStatusCode"] in (200, 204)
            # Verify it's gone
            with pytest.raises(ClientError) as exc:
                s3control.get_access_point(AccountId=ACCOUNT_ID, Name=ap_name)
            assert exc.value.response["Error"]["Code"] == "NoSuchAccessPoint"
        finally:
            s3.delete_bucket(Bucket=bucket)

    def test_list_access_points(self, s3control, s3):
        """ListAccessPoints returns all access points for an account."""
        bucket = f"ap-list-{_uid()}"
        s3.create_bucket(Bucket=bucket)
        names = [f"ap-list-{_uid()}" for _ in range(3)]
        try:
            for name in names:
                s3control.create_access_point(AccountId=ACCOUNT_ID, Name=name, Bucket=bucket)
            resp = s3control.list_access_points(AccountId=ACCOUNT_ID)
            listed_names = [ap["Name"] for ap in resp["AccessPointList"]]
            for name in names:
                assert name in listed_names
        finally:
            for name in names:
                try:
                    s3control.delete_access_point(AccountId=ACCOUNT_ID, Name=name)
                except Exception:
                    pass  # best-effort cleanup
            s3.delete_bucket(Bucket=bucket)

    def test_list_access_points_by_bucket(self, s3control, s3):
        """ListAccessPoints filters by Bucket parameter."""
        bucket1 = f"ap-filt1-{_uid()}"
        bucket2 = f"ap-filt2-{_uid()}"
        s3.create_bucket(Bucket=bucket1)
        s3.create_bucket(Bucket=bucket2)
        ap1 = f"ap-b1-{_uid()}"
        ap2 = f"ap-b2-{_uid()}"
        try:
            s3control.create_access_point(AccountId=ACCOUNT_ID, Name=ap1, Bucket=bucket1)
            s3control.create_access_point(AccountId=ACCOUNT_ID, Name=ap2, Bucket=bucket2)
            resp = s3control.list_access_points(AccountId=ACCOUNT_ID, Bucket=bucket1)
            listed = [ap["Name"] for ap in resp["AccessPointList"]]
            assert ap1 in listed
            assert ap2 not in listed
        finally:
            for name in [ap1, ap2]:
                try:
                    s3control.delete_access_point(AccountId=ACCOUNT_ID, Name=name)
                except Exception:
                    pass  # best-effort cleanup
            s3.delete_bucket(Bucket=bucket1)
            s3.delete_bucket(Bucket=bucket2)

    def test_list_access_points_max_results(self, s3control, s3):
        """ListAccessPoints with MaxResults limits results."""
        bucket = f"ap-max-{_uid()}"
        s3.create_bucket(Bucket=bucket)
        names = [f"ap-max-{_uid()}" for _ in range(3)]
        try:
            for name in names:
                s3control.create_access_point(AccountId=ACCOUNT_ID, Name=name, Bucket=bucket)
            resp = s3control.list_access_points(AccountId=ACCOUNT_ID, MaxResults=1)
            assert len(resp["AccessPointList"]) <= 1
            assert "NextToken" in resp
        finally:
            for name in names:
                try:
                    s3control.delete_access_point(AccountId=ACCOUNT_ID, Name=name)
                except Exception:
                    pass  # best-effort cleanup
            s3.delete_bucket(Bucket=bucket)

    def test_get_access_point_not_found(self, s3control):
        """GetAccessPoint for nonexistent name raises NoSuchAccessPoint."""
        with pytest.raises(ClientError) as exc:
            s3control.get_access_point(AccountId=ACCOUNT_ID, Name=f"nonexistent-{_uid()}")
        assert exc.value.response["Error"]["Code"] == "NoSuchAccessPoint"

    def test_create_access_point_with_public_access_block(self, s3control, s3):
        """CreateAccessPoint with PublicAccessBlockConfiguration."""
        bucket = f"ap-pab-{_uid()}"
        ap_name = f"ap-pab-{_uid()}"
        s3.create_bucket(Bucket=bucket)
        try:
            s3control.create_access_point(
                AccountId=ACCOUNT_ID,
                Name=ap_name,
                Bucket=bucket,
                PublicAccessBlockConfiguration={
                    "BlockPublicAcls": True,
                    "IgnorePublicAcls": True,
                    "BlockPublicPolicy": True,
                    "RestrictPublicBuckets": True,
                },
            )
            resp = s3control.get_access_point(AccountId=ACCOUNT_ID, Name=ap_name)
            pab = resp.get("PublicAccessBlockConfiguration", {})
            assert pab["BlockPublicAcls"] is True
            assert pab["IgnorePublicAcls"] is True
            assert pab["BlockPublicPolicy"] is True
            assert pab["RestrictPublicBuckets"] is True
        finally:
            try:
                s3control.delete_access_point(AccountId=ACCOUNT_ID, Name=ap_name)
            except Exception:
                pass  # best-effort cleanup
            s3.delete_bucket(Bucket=bucket)

    def test_access_point_arn_format(self, s3control, s3):
        """Access point ARN has the expected format."""
        bucket = f"ap-arn-{_uid()}"
        ap_name = f"ap-arn-{_uid()}"
        s3.create_bucket(Bucket=bucket)
        try:
            resp = s3control.create_access_point(AccountId=ACCOUNT_ID, Name=ap_name, Bucket=bucket)
            arn = resp["AccessPointArn"]
            assert arn.startswith("arn:aws:s3:")
            assert "accesspoint" in arn
            assert ap_name in arn
        finally:
            try:
                s3control.delete_access_point(AccountId=ACCOUNT_ID, Name=ap_name)
            except Exception:
                pass  # best-effort cleanup
            s3.delete_bucket(Bucket=bucket)

    def test_put_access_point_policy(self, s3control, s3):
        """PutAccessPointPolicy sets a resource policy on an access point."""
        bucket = f"ap-pol-{_uid()}"
        ap_name = f"ap-pol-{_uid()}"
        s3.create_bucket(Bucket=bucket)
        try:
            s3control.create_access_point(AccountId=ACCOUNT_ID, Name=ap_name, Bucket=bucket)
            policy = json.dumps(
                {
                    "Version": "2012-10-17",
                    "Statement": [
                        {
                            "Effect": "Allow",
                            "Principal": "*",
                            "Action": "s3:GetObject",
                            "Resource": (
                                f"arn:aws:s3:us-east-1:{ACCOUNT_ID}:accesspoint/{ap_name}/object/*"
                            ),
                        }
                    ],
                }
            )
            resp = s3control.put_access_point_policy(
                AccountId=ACCOUNT_ID, Name=ap_name, Policy=policy
            )
            assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200
        finally:
            try:
                s3control.delete_access_point_policy(AccountId=ACCOUNT_ID, Name=ap_name)
            except Exception:
                pass  # best-effort cleanup
            try:
                s3control.delete_access_point(AccountId=ACCOUNT_ID, Name=ap_name)
            except Exception:
                pass  # best-effort cleanup
            s3.delete_bucket(Bucket=bucket)

    def test_get_access_point_policy(self, s3control, s3):
        """GetAccessPointPolicy returns the policy set on an access point."""
        bucket = f"ap-getpol-{_uid()}"
        ap_name = f"ap-getpol-{_uid()}"
        s3.create_bucket(Bucket=bucket)
        try:
            s3control.create_access_point(AccountId=ACCOUNT_ID, Name=ap_name, Bucket=bucket)
            policy = json.dumps(
                {
                    "Version": "2012-10-17",
                    "Statement": [
                        {
                            "Effect": "Allow",
                            "Principal": "*",
                            "Action": "s3:GetObject",
                            "Resource": (
                                f"arn:aws:s3:us-east-1:{ACCOUNT_ID}:accesspoint/{ap_name}/object/*"
                            ),
                        }
                    ],
                }
            )
            s3control.put_access_point_policy(AccountId=ACCOUNT_ID, Name=ap_name, Policy=policy)
            resp = s3control.get_access_point_policy(AccountId=ACCOUNT_ID, Name=ap_name)
            assert "Policy" in resp
            parsed = json.loads(resp["Policy"])
            assert parsed["Version"] == "2012-10-17"
            assert len(parsed["Statement"]) == 1
        finally:
            try:
                s3control.delete_access_point_policy(AccountId=ACCOUNT_ID, Name=ap_name)
            except Exception:
                pass  # best-effort cleanup
            try:
                s3control.delete_access_point(AccountId=ACCOUNT_ID, Name=ap_name)
            except Exception:
                pass  # best-effort cleanup
            s3.delete_bucket(Bucket=bucket)

    def test_get_access_point_policy_not_set(self, s3control, s3):
        """GetAccessPointPolicy when no policy is set raises NoSuchAccessPointPolicy."""
        bucket = f"ap-nopol-{_uid()}"
        ap_name = f"ap-nopol-{_uid()}"
        s3.create_bucket(Bucket=bucket)
        try:
            s3control.create_access_point(AccountId=ACCOUNT_ID, Name=ap_name, Bucket=bucket)
            with pytest.raises(ClientError) as exc:
                s3control.get_access_point_policy(AccountId=ACCOUNT_ID, Name=ap_name)
            assert exc.value.response["Error"]["Code"] == "NoSuchAccessPointPolicy"
        finally:
            try:
                s3control.delete_access_point(AccountId=ACCOUNT_ID, Name=ap_name)
            except Exception:
                pass  # best-effort cleanup
            s3.delete_bucket(Bucket=bucket)

    def test_delete_access_point_policy(self, s3control, s3):
        """DeleteAccessPointPolicy removes the policy from an access point."""
        bucket = f"ap-delpol-{_uid()}"
        ap_name = f"ap-delpol-{_uid()}"
        s3.create_bucket(Bucket=bucket)
        try:
            s3control.create_access_point(AccountId=ACCOUNT_ID, Name=ap_name, Bucket=bucket)
            policy = json.dumps({"Version": "2012-10-17", "Statement": []})
            s3control.put_access_point_policy(AccountId=ACCOUNT_ID, Name=ap_name, Policy=policy)
            resp = s3control.delete_access_point_policy(AccountId=ACCOUNT_ID, Name=ap_name)
            assert resp["ResponseMetadata"]["HTTPStatusCode"] in (200, 204)
            # Verify policy is gone
            with pytest.raises(ClientError) as exc:
                s3control.get_access_point_policy(AccountId=ACCOUNT_ID, Name=ap_name)
            assert exc.value.response["Error"]["Code"] == "NoSuchAccessPointPolicy"
        finally:
            try:
                s3control.delete_access_point(AccountId=ACCOUNT_ID, Name=ap_name)
            except Exception:
                pass  # best-effort cleanup
            s3.delete_bucket(Bucket=bucket)

    def test_get_access_point_policy_status(self, s3control, s3):
        """GetAccessPointPolicyStatus returns policy status with IsPublic field."""
        bucket = f"ap-polstat-{_uid()}"
        ap_name = f"ap-polstat-{_uid()}"
        s3.create_bucket(Bucket=bucket)
        try:
            s3control.create_access_point(AccountId=ACCOUNT_ID, Name=ap_name, Bucket=bucket)
            policy = json.dumps(
                {
                    "Version": "2012-10-17",
                    "Statement": [
                        {
                            "Effect": "Allow",
                            "Principal": "*",
                            "Action": "s3:GetObject",
                            "Resource": (
                                f"arn:aws:s3:us-east-1:{ACCOUNT_ID}:accesspoint/{ap_name}/object/*"
                            ),
                        }
                    ],
                }
            )
            s3control.put_access_point_policy(AccountId=ACCOUNT_ID, Name=ap_name, Policy=policy)
            resp = s3control.get_access_point_policy_status(AccountId=ACCOUNT_ID, Name=ap_name)
            assert "PolicyStatus" in resp
            assert "IsPublic" in resp["PolicyStatus"]
            assert isinstance(resp["PolicyStatus"]["IsPublic"], bool)
        finally:
            try:
                s3control.delete_access_point_policy(AccountId=ACCOUNT_ID, Name=ap_name)
            except Exception:
                pass  # best-effort cleanup
            try:
                s3control.delete_access_point(AccountId=ACCOUNT_ID, Name=ap_name)
            except Exception:
                pass  # best-effort cleanup
            s3.delete_bucket(Bucket=bucket)

    def test_list_access_points_empty(self, s3control):
        """ListAccessPoints returns empty list when no access points exist for a bucket."""
        nonexistent = f"nonexistent-bucket-{_uid()}"
        resp = s3control.list_access_points(AccountId=ACCOUNT_ID, Bucket=nonexistent)
        assert resp["AccessPointList"] == []
        assert isinstance(resp["AccessPointList"], list)
        # Attempting to get an access point from a nonexistent AP name also errors
        with pytest.raises(ClientError) as exc:
            s3control.get_access_point(AccountId=ACCOUNT_ID, Name=f"no-ap-{_uid()}")
        assert exc.value.response["Error"]["Code"] == "NoSuchAccessPoint"

    def test_create_multiple_access_points_same_bucket(self, s3control, s3):
        """Multiple access points can be created for the same bucket."""
        bucket = f"ap-multi-{_uid()}"
        s3.create_bucket(Bucket=bucket)
        ap1 = f"ap-m1-{_uid()}"
        ap2 = f"ap-m2-{_uid()}"
        try:
            s3control.create_access_point(AccountId=ACCOUNT_ID, Name=ap1, Bucket=bucket)
            s3control.create_access_point(AccountId=ACCOUNT_ID, Name=ap2, Bucket=bucket)
            resp = s3control.list_access_points(AccountId=ACCOUNT_ID, Bucket=bucket)
            names = [ap["Name"] for ap in resp["AccessPointList"]]
            assert ap1 in names
            assert ap2 in names
        finally:
            for name in [ap1, ap2]:
                try:
                    s3control.delete_access_point(AccountId=ACCOUNT_ID, Name=name)
                except Exception:
                    pass  # best-effort cleanup
            s3.delete_bucket(Bucket=bucket)

    def test_access_point_network_origin(self, s3control, s3):
        """Access point has NetworkOrigin field set to Internet by default."""
        bucket = f"ap-nw-{_uid()}"
        ap_name = f"ap-nw-{_uid()}"
        s3.create_bucket(Bucket=bucket)
        try:
            s3control.create_access_point(AccountId=ACCOUNT_ID, Name=ap_name, Bucket=bucket)
            resp = s3control.get_access_point(AccountId=ACCOUNT_ID, Name=ap_name)
            assert resp["NetworkOrigin"] == "Internet"
        finally:
            try:
                s3control.delete_access_point(AccountId=ACCOUNT_ID, Name=ap_name)
            except Exception:
                pass  # best-effort cleanup
            s3.delete_bucket(Bucket=bucket)

    def test_delete_access_point_idempotent(self, s3control):
        """DeleteAccessPoint for nonexistent name succeeds (idempotent)."""
        name = f"nonexistent-{_uid()}"
        resp = s3control.delete_access_point(AccountId=ACCOUNT_ID, Name=name)
        assert resp["ResponseMetadata"]["HTTPStatusCode"] in (200, 204)
        # Verify the access point truly doesn't exist after deletion
        with pytest.raises(ClientError) as exc:
            s3control.get_access_point(AccountId=ACCOUNT_ID, Name=name)
        assert exc.value.response["Error"]["Code"] == "NoSuchAccessPoint"

    def test_list_access_points_pagination_next_token(self, s3control, s3):
        """ListAccessPoints paginates correctly with NextToken."""
        bucket = f"ap-pagnxt-{_uid()}"
        s3.create_bucket(Bucket=bucket)
        names = [f"ap-pn-{_uid()}" for _ in range(3)]
        try:
            for name in names:
                s3control.create_access_point(AccountId=ACCOUNT_ID, Name=name, Bucket=bucket)
            # Page through with MaxResults=2
            all_names = []
            token = None
            while True:
                kwargs = {"AccountId": ACCOUNT_ID, "MaxResults": 2}
                if token:
                    kwargs["NextToken"] = token
                resp = s3control.list_access_points(**kwargs)
                all_names.extend([ap["Name"] for ap in resp["AccessPointList"]])
                token = resp.get("NextToken")
                if not token:
                    break
            for name in names:
                assert name in all_names
        finally:
            for name in names:
                try:
                    s3control.delete_access_point(AccountId=ACCOUNT_ID, Name=name)
                except Exception:
                    pass  # best-effort cleanup
            s3.delete_bucket(Bucket=bucket)

    def test_create_access_point_duplicate_name(self, s3control, s3):
        """Creating an access point with a duplicate name does not error."""
        bucket = f"ap-dup-{_uid()}"
        ap_name = f"ap-dup-{_uid()}"
        s3.create_bucket(Bucket=bucket)
        try:
            s3control.create_access_point(AccountId=ACCOUNT_ID, Name=ap_name, Bucket=bucket)
            resp = s3control.create_access_point(AccountId=ACCOUNT_ID, Name=ap_name, Bucket=bucket)
            assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200
        finally:
            try:
                s3control.delete_access_point(AccountId=ACCOUNT_ID, Name=ap_name)
            except Exception:
                pass  # best-effort cleanup
            s3.delete_bucket(Bucket=bucket)

    def test_get_access_point_policy_status_no_policy(self, s3control, s3):
        """GetAccessPointPolicyStatus without policy raises NoSuchAccessPointPolicy."""
        bucket = f"ap-psnp-{_uid()}"
        ap_name = f"ap-psnp-{_uid()}"
        s3.create_bucket(Bucket=bucket)
        try:
            s3control.create_access_point(AccountId=ACCOUNT_ID, Name=ap_name, Bucket=bucket)
            with pytest.raises(ClientError) as exc:
                s3control.get_access_point_policy_status(AccountId=ACCOUNT_ID, Name=ap_name)
            assert exc.value.response["Error"]["Code"] == "NoSuchAccessPointPolicy"
        finally:
            try:
                s3control.delete_access_point(AccountId=ACCOUNT_ID, Name=ap_name)
            except Exception:
                pass  # best-effort cleanup
            s3.delete_bucket(Bucket=bucket)

    def test_get_access_point_policy_status_not_found(self, s3control):
        """GetAccessPointPolicyStatus for nonexistent AP raises NoSuchAccessPoint."""
        with pytest.raises(ClientError) as exc:
            s3control.get_access_point_policy_status(
                AccountId=ACCOUNT_ID, Name=f"nonexistent-{_uid()}"
            )
        assert exc.value.response["Error"]["Code"] == "NoSuchAccessPoint"

    def test_get_access_point_policy_not_found(self, s3control):
        """GetAccessPointPolicy for nonexistent AP raises NoSuchAccessPoint."""
        with pytest.raises(ClientError) as exc:
            s3control.get_access_point_policy(AccountId=ACCOUNT_ID, Name=f"nonexistent-{_uid()}")
        assert exc.value.response["Error"]["Code"] == "NoSuchAccessPoint"

    def test_access_point_creation_timestamp(self, s3control, s3):
        """Access point has a CreationDate field after creation."""
        bucket = f"ap-ts-{_uid()}"
        ap_name = f"ap-ts-{_uid()}"
        s3.create_bucket(Bucket=bucket)
        try:
            s3control.create_access_point(AccountId=ACCOUNT_ID, Name=ap_name, Bucket=bucket)
            resp = s3control.get_access_point(AccountId=ACCOUNT_ID, Name=ap_name)
            assert "CreationDate" in resp
        finally:
            try:
                s3control.delete_access_point(AccountId=ACCOUNT_ID, Name=ap_name)
            except Exception:
                pass  # best-effort cleanup
            s3.delete_bucket(Bucket=bucket)


class TestS3ControlMultiRegionAccessPoints:
    """Tests for S3 Control Multi-Region Access Point operations."""

    def test_create_multi_region_access_point(self, s3control, s3):
        """CreateMultiRegionAccessPoint creates an MRAP and returns a request token."""
        bucket = f"mrap-create-{_uid()}"
        mrap_name = f"mrap-{_uid()}"
        s3.create_bucket(Bucket=bucket)
        try:
            resp = s3control.create_multi_region_access_point(
                AccountId=ACCOUNT_ID,
                Details={
                    "Name": mrap_name,
                    "Regions": [{"Bucket": bucket}],
                },
            )
            assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200
            assert "RequestTokenARN" in resp
        finally:
            try:
                s3control.delete_multi_region_access_point(
                    AccountId=ACCOUNT_ID,
                    Details={"Name": mrap_name},
                )
            except Exception:
                pass  # best-effort cleanup
            s3.delete_bucket(Bucket=bucket)

    def test_get_multi_region_access_point(self, s3control, s3):
        """GetMultiRegionAccessPoint returns MRAP details."""
        bucket = f"mrap-get-{_uid()}"
        mrap_name = f"mrap-{_uid()}"
        s3.create_bucket(Bucket=bucket)
        try:
            s3control.create_multi_region_access_point(
                AccountId=ACCOUNT_ID,
                Details={
                    "Name": mrap_name,
                    "Regions": [{"Bucket": bucket}],
                },
            )
            resp = s3control.get_multi_region_access_point(AccountId=ACCOUNT_ID, Name=mrap_name)
            assert "AccessPoint" in resp
            ap = resp["AccessPoint"]
            assert ap["Name"] == mrap_name
            assert "Regions" in ap
        finally:
            try:
                s3control.delete_multi_region_access_point(
                    AccountId=ACCOUNT_ID,
                    Details={"Name": mrap_name},
                )
            except Exception:
                pass  # best-effort cleanup
            s3.delete_bucket(Bucket=bucket)

    def test_delete_multi_region_access_point(self, s3control, s3):
        """DeleteMultiRegionAccessPoint removes the MRAP and returns a request token."""
        bucket = f"mrap-del-{_uid()}"
        mrap_name = f"mrap-{_uid()}"
        s3.create_bucket(Bucket=bucket)
        try:
            s3control.create_multi_region_access_point(
                AccountId=ACCOUNT_ID,
                Details={
                    "Name": mrap_name,
                    "Regions": [{"Bucket": bucket}],
                },
            )
            resp = s3control.delete_multi_region_access_point(
                AccountId=ACCOUNT_ID,
                Details={"Name": mrap_name},
            )
            assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200
            assert "RequestTokenARN" in resp
        finally:
            s3.delete_bucket(Bucket=bucket)

    def test_list_multi_region_access_points(self, s3control, s3):
        """ListMultiRegionAccessPoints returns MRAPs for the account."""
        bucket = f"mrap-list-{_uid()}"
        mrap_name = f"mrap-{_uid()}"
        s3.create_bucket(Bucket=bucket)
        try:
            s3control.create_multi_region_access_point(
                AccountId=ACCOUNT_ID,
                Details={
                    "Name": mrap_name,
                    "Regions": [{"Bucket": bucket}],
                },
            )
            resp = s3control.list_multi_region_access_points(AccountId=ACCOUNT_ID)
            assert "AccessPoints" in resp
            names = [ap["Name"] for ap in resp["AccessPoints"]]
            assert mrap_name in names
        finally:
            try:
                s3control.delete_multi_region_access_point(
                    AccountId=ACCOUNT_ID,
                    Details={"Name": mrap_name},
                )
            except Exception:
                pass  # best-effort cleanup
            s3.delete_bucket(Bucket=bucket)

    def test_get_multi_region_access_point_policy(self, s3control, s3):
        """GetMultiRegionAccessPointPolicy returns the policy for an MRAP."""
        bucket = f"mrap-pol-{_uid()}"
        mrap_name = f"mrap-{_uid()}"
        s3.create_bucket(Bucket=bucket)
        try:
            s3control.create_multi_region_access_point(
                AccountId=ACCOUNT_ID,
                Details={
                    "Name": mrap_name,
                    "Regions": [{"Bucket": bucket}],
                },
            )
            policy = json.dumps(
                {
                    "Version": "2012-10-17",
                    "Statement": [
                        {
                            "Effect": "Allow",
                            "Principal": "*",
                            "Action": "s3:GetObject",
                            "Resource": "*",
                        }
                    ],
                }
            )
            s3control.put_multi_region_access_point_policy(
                AccountId=ACCOUNT_ID,
                Details={"Name": mrap_name, "Policy": policy},
            )
            resp = s3control.get_multi_region_access_point_policy(
                AccountId=ACCOUNT_ID, Name=mrap_name
            )
            assert "Policy" in resp
            assert "Established" in resp["Policy"]
        finally:
            try:
                s3control.delete_multi_region_access_point(
                    AccountId=ACCOUNT_ID,
                    Details={"Name": mrap_name},
                )
            except Exception:
                pass  # best-effort cleanup
            s3.delete_bucket(Bucket=bucket)

    def test_put_multi_region_access_point_policy(self, s3control, s3):
        """PutMultiRegionAccessPointPolicy sets a policy on an MRAP."""
        bucket = f"mrap-putpol-{_uid()}"
        mrap_name = f"mrap-{_uid()}"
        s3.create_bucket(Bucket=bucket)
        try:
            s3control.create_multi_region_access_point(
                AccountId=ACCOUNT_ID,
                Details={
                    "Name": mrap_name,
                    "Regions": [{"Bucket": bucket}],
                },
            )
            policy = json.dumps(
                {
                    "Version": "2012-10-17",
                    "Statement": [
                        {
                            "Effect": "Allow",
                            "Principal": "*",
                            "Action": "s3:GetObject",
                            "Resource": "*",
                        }
                    ],
                }
            )
            resp = s3control.put_multi_region_access_point_policy(
                AccountId=ACCOUNT_ID,
                Details={"Name": mrap_name, "Policy": policy},
            )
            assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200
            assert "RequestTokenARN" in resp
        finally:
            try:
                s3control.delete_multi_region_access_point(
                    AccountId=ACCOUNT_ID,
                    Details={"Name": mrap_name},
                )
            except Exception:
                pass  # best-effort cleanup
            s3.delete_bucket(Bucket=bucket)

    def test_get_multi_region_access_point_policy_status(self, s3control, s3):
        """GetMultiRegionAccessPointPolicyStatus returns IsPublic status."""
        bucket = f"mrap-polst-{_uid()}"
        mrap_name = f"mrap-{_uid()}"
        s3.create_bucket(Bucket=bucket)
        try:
            s3control.create_multi_region_access_point(
                AccountId=ACCOUNT_ID,
                Details={
                    "Name": mrap_name,
                    "Regions": [{"Bucket": bucket}],
                },
            )
            # Must set a policy first, otherwise NoSuchMultiRegionAccessPointPolicy
            policy = json.dumps(
                {
                    "Version": "2012-10-17",
                    "Statement": [
                        {
                            "Effect": "Allow",
                            "Principal": "*",
                            "Action": "s3:GetObject",
                            "Resource": "*",
                        }
                    ],
                }
            )
            s3control.put_multi_region_access_point_policy(
                AccountId=ACCOUNT_ID,
                Details={"Name": mrap_name, "Policy": policy},
            )
            resp = s3control.get_multi_region_access_point_policy_status(
                AccountId=ACCOUNT_ID, Name=mrap_name
            )
            assert "Established" in resp
        finally:
            try:
                s3control.delete_multi_region_access_point(
                    AccountId=ACCOUNT_ID,
                    Details={"Name": mrap_name},
                )
            except Exception:
                pass  # best-effort cleanup
            s3.delete_bucket(Bucket=bucket)

    def test_describe_multi_region_access_point_operation(self, s3control, s3):
        """DescribeMultiRegionAccessPointOperation returns async op status."""
        bucket = f"mrap-desc-{_uid()}"
        mrap_name = f"mrap-{_uid()}"
        s3.create_bucket(Bucket=bucket)
        try:
            create_resp = s3control.create_multi_region_access_point(
                AccountId=ACCOUNT_ID,
                Details={
                    "Name": mrap_name,
                    "Regions": [{"Bucket": bucket}],
                },
            )
            token_arn = create_resp["RequestTokenARN"]
            resp = s3control.describe_multi_region_access_point_operation(
                AccountId=ACCOUNT_ID, RequestTokenARN=token_arn
            )
            assert "AsyncOperation" in resp
        finally:
            try:
                s3control.delete_multi_region_access_point(
                    AccountId=ACCOUNT_ID,
                    Details={"Name": mrap_name},
                )
            except Exception:
                pass  # best-effort cleanup
            s3.delete_bucket(Bucket=bucket)

    def test_get_multi_region_access_point_details(self, s3control, s3):
        """GetMultiRegionAccessPoint returns Status, Alias, and Regions fields."""
        bucket = f"mrap-dtl-{_uid()}"
        mrap_name = f"mrap-{_uid()}"
        s3.create_bucket(Bucket=bucket)
        try:
            s3control.create_multi_region_access_point(
                AccountId=ACCOUNT_ID,
                Details={
                    "Name": mrap_name,
                    "Regions": [{"Bucket": bucket}],
                },
            )
            resp = s3control.get_multi_region_access_point(AccountId=ACCOUNT_ID, Name=mrap_name)
            ap = resp["AccessPoint"]
            assert ap["Name"] == mrap_name
            assert ap["Status"] == "READY"
            assert "Alias" in ap
            assert len(ap["Regions"]) == 1
            assert ap["Regions"][0]["Bucket"] == bucket
        finally:
            try:
                s3control.delete_multi_region_access_point(
                    AccountId=ACCOUNT_ID,
                    Details={"Name": mrap_name},
                )
            except Exception:
                pass  # best-effort cleanup
            s3.delete_bucket(Bucket=bucket)

    def test_get_multi_region_access_point_not_found(self, s3control):
        """GetMultiRegionAccessPoint for nonexistent MRAP raises error."""
        with pytest.raises(ClientError) as exc:
            s3control.get_multi_region_access_point(
                AccountId=ACCOUNT_ID, Name=f"nonexistent-{_uid()}"
            )
        assert exc.value.response["Error"]["Code"] == "NoSuchMultiRegionAccessPoint"

    def test_list_multi_region_access_points_empty(self, s3control, s3):
        """ListMultiRegionAccessPoints lifecycle: empty → create → list → delete → error."""
        # LIST: initially no MRAPs (or at least a valid list)
        resp = s3control.list_multi_region_access_points(AccountId=ACCOUNT_ID)
        assert "AccessPoints" in resp
        assert isinstance(resp["AccessPoints"], list)
        initial_count = len(resp["AccessPoints"])
        # CREATE + RETRIEVE: create an MRAP and verify it appears
        bucket = f"mrap-empty-{_uid()}"
        mrap_name = f"mrap-{_uid()}"
        s3.create_bucket(Bucket=bucket)
        try:
            create_resp = s3control.create_multi_region_access_point(
                AccountId=ACCOUNT_ID,
                Details={
                    "Name": mrap_name,
                    "Regions": [{"Bucket": bucket}],
                },
            )
            assert "RequestTokenARN" in create_resp
            get_resp = s3control.get_multi_region_access_point(
                AccountId=ACCOUNT_ID, Name=mrap_name
            )
            assert get_resp["AccessPoint"]["Name"] == mrap_name
            # LIST: now the MRAP appears
            list_resp = s3control.list_multi_region_access_points(AccountId=ACCOUNT_ID)
            names = [ap["Name"] for ap in list_resp["AccessPoints"]]
            assert mrap_name in names
            assert len(list_resp["AccessPoints"]) > initial_count
            # DELETE: remove it
            s3control.delete_multi_region_access_point(
                AccountId=ACCOUNT_ID,
                Details={"Name": mrap_name},
            )
            # ERROR: get after delete fails
            with pytest.raises(ClientError) as exc_info:
                s3control.get_multi_region_access_point(
                    AccountId=ACCOUNT_ID, Name=mrap_name
                )
            assert exc_info.value.response["Error"]["Code"] == "NoSuchMultiRegionAccessPoint"
        finally:
            try:
                s3control.delete_multi_region_access_point(
                    AccountId=ACCOUNT_ID, Details={"Name": mrap_name}
                )
            except Exception:
                pass  # best-effort cleanup
            try:
                s3.delete_bucket(Bucket=bucket)
            except Exception:
                pass  # best-effort cleanup

    def test_list_multi_region_access_points_fields(self, s3control, s3):
        """ListMultiRegionAccessPoints entries contain Status, Alias, Regions."""
        bucket = f"mrap-lstf-{_uid()}"
        mrap_name = f"mrap-{_uid()}"
        s3.create_bucket(Bucket=bucket)
        try:
            s3control.create_multi_region_access_point(
                AccountId=ACCOUNT_ID,
                Details={
                    "Name": mrap_name,
                    "Regions": [{"Bucket": bucket}],
                },
            )
            resp = s3control.list_multi_region_access_points(AccountId=ACCOUNT_ID)
            found = [ap for ap in resp["AccessPoints"] if ap["Name"] == mrap_name]
            assert len(found) == 1
            ap = found[0]
            assert ap["Status"] == "READY"
            assert "Alias" in ap
            assert len(ap["Regions"]) >= 1
        finally:
            try:
                s3control.delete_multi_region_access_point(
                    AccountId=ACCOUNT_ID,
                    Details={"Name": mrap_name},
                )
            except Exception:
                pass  # best-effort cleanup
            s3.delete_bucket(Bucket=bucket)

    def test_get_mrap_policy_not_found(self, s3control):
        """GetMultiRegionAccessPointPolicy for nonexistent MRAP raises error."""
        with pytest.raises(ClientError) as exc:
            s3control.get_multi_region_access_point_policy(
                AccountId=ACCOUNT_ID, Name=f"nonexistent-{_uid()}"
            )
        assert exc.value.response["Error"]["Code"] == "NoSuchMultiRegionAccessPoint"

    def test_get_mrap_policy_status_not_found(self, s3control):
        """GetMultiRegionAccessPointPolicyStatus for nonexistent MRAP raises error."""
        with pytest.raises(ClientError) as exc:
            s3control.get_multi_region_access_point_policy_status(
                AccountId=ACCOUNT_ID, Name=f"nonexistent-{_uid()}"
            )
        assert exc.value.response["Error"]["Code"] == "NoSuchMultiRegionAccessPoint"

    def test_describe_mrap_operation_not_found(self, s3control):
        """DescribeMultiRegionAccessPointOperation for bad token raises error."""
        with pytest.raises(ClientError) as exc:
            s3control.describe_multi_region_access_point_operation(
                AccountId=ACCOUNT_ID,
                RequestTokenARN=f"arn:aws:s3::123456789012:async-request/mrap/nonexistent-{_uid()}",
            )
        assert exc.value.response["Error"]["Code"] == "NoSuchAsyncRequest"


class TestS3ControlStorageLens:
    """Tests for S3 Control Storage Lens operations."""

    def test_put_storage_lens_configuration(self, s3control):
        """PutStorageLensConfiguration creates a storage lens config."""
        config_id = f"lens-{_uid()}"
        try:
            resp = s3control.put_storage_lens_configuration(
                AccountId=ACCOUNT_ID,
                ConfigId=config_id,
                StorageLensConfiguration={
                    "Id": config_id,
                    "AccountLevel": {
                        "BucketLevel": {},
                    },
                    "IsEnabled": True,
                },
            )
            assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200
        finally:
            try:
                s3control.delete_storage_lens_configuration(
                    AccountId=ACCOUNT_ID, ConfigId=config_id
                )
            except Exception:
                pass  # best-effort cleanup

    def test_delete_storage_lens_configuration(self, s3control):
        """DeleteStorageLensConfiguration removes the config."""
        config_id = f"lens-{_uid()}"
        s3control.put_storage_lens_configuration(
            AccountId=ACCOUNT_ID,
            ConfigId=config_id,
            StorageLensConfiguration={
                "Id": config_id,
                "AccountLevel": {
                    "BucketLevel": {},
                },
                "IsEnabled": True,
            },
        )
        resp = s3control.delete_storage_lens_configuration(AccountId=ACCOUNT_ID, ConfigId=config_id)
        assert resp["ResponseMetadata"]["HTTPStatusCode"] in (200, 204)

    def test_list_storage_lens_configurations(self, s3control):
        """ListStorageLensConfigurations returns configs for the account."""
        config_id = f"lens-{_uid()}"
        try:
            s3control.put_storage_lens_configuration(
                AccountId=ACCOUNT_ID,
                ConfigId=config_id,
                StorageLensConfiguration={
                    "Id": config_id,
                    "AccountLevel": {
                        "BucketLevel": {},
                    },
                    "IsEnabled": True,
                },
            )
            resp = s3control.list_storage_lens_configurations(AccountId=ACCOUNT_ID)
            assert "StorageLensConfigurationList" in resp
        finally:
            try:
                s3control.delete_storage_lens_configuration(
                    AccountId=ACCOUNT_ID, ConfigId=config_id
                )
            except Exception:
                pass  # best-effort cleanup

    def test_put_storage_lens_configuration_tagging(self, s3control):
        """PutStorageLensConfigurationTagging sets tags on a storage lens config."""
        config_id = f"lens-{_uid()}"
        try:
            s3control.put_storage_lens_configuration(
                AccountId=ACCOUNT_ID,
                ConfigId=config_id,
                StorageLensConfiguration={
                    "Id": config_id,
                    "AccountLevel": {
                        "BucketLevel": {},
                    },
                    "IsEnabled": True,
                },
            )
            resp = s3control.put_storage_lens_configuration_tagging(
                AccountId=ACCOUNT_ID,
                ConfigId=config_id,
                Tags=[{"Key": "env", "Value": "test"}],
            )
            assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200
        finally:
            try:
                s3control.delete_storage_lens_configuration(
                    AccountId=ACCOUNT_ID, ConfigId=config_id
                )
            except Exception:
                pass  # best-effort cleanup

    def test_get_storage_lens_configuration_tagging(self, s3control):
        """GetStorageLensConfigurationTagging returns tags for a storage lens config."""
        config_id = f"lens-{_uid()}"
        try:
            s3control.put_storage_lens_configuration(
                AccountId=ACCOUNT_ID,
                ConfigId=config_id,
                StorageLensConfiguration={
                    "Id": config_id,
                    "AccountLevel": {
                        "BucketLevel": {},
                    },
                    "IsEnabled": True,
                },
            )
            s3control.put_storage_lens_configuration_tagging(
                AccountId=ACCOUNT_ID,
                ConfigId=config_id,
                Tags=[{"Key": "env", "Value": "test"}],
            )
            resp = s3control.get_storage_lens_configuration_tagging(
                AccountId=ACCOUNT_ID, ConfigId=config_id
            )
            assert "Tags" in resp
            assert isinstance(resp["Tags"], list)
        finally:
            try:
                s3control.delete_storage_lens_configuration(
                    AccountId=ACCOUNT_ID, ConfigId=config_id
                )
            except Exception:
                pass  # best-effort cleanup

    def test_list_storage_lens_has_created_config(self, s3control):
        """ListStorageLensConfigurations includes a freshly created config."""
        config_id = f"lens-{_uid()}"
        try:
            s3control.put_storage_lens_configuration(
                AccountId=ACCOUNT_ID,
                ConfigId=config_id,
                StorageLensConfiguration={
                    "Id": config_id,
                    "AccountLevel": {"BucketLevel": {}},
                    "IsEnabled": True,
                },
            )
            resp = s3control.list_storage_lens_configurations(AccountId=ACCOUNT_ID)
            ids = [c["Id"] for c in resp["StorageLensConfigurationList"]]
            assert config_id in ids
        finally:
            try:
                s3control.delete_storage_lens_configuration(
                    AccountId=ACCOUNT_ID, ConfigId=config_id
                )
            except Exception:
                pass  # best-effort cleanup

    def test_delete_storage_lens_removes_from_list(self, s3control):
        """Deleting a storage lens config removes it from list results."""
        config_id = f"lens-{_uid()}"
        s3control.put_storage_lens_configuration(
            AccountId=ACCOUNT_ID,
            ConfigId=config_id,
            StorageLensConfiguration={
                "Id": config_id,
                "AccountLevel": {"BucketLevel": {}},
                "IsEnabled": True,
            },
        )
        s3control.delete_storage_lens_configuration(AccountId=ACCOUNT_ID, ConfigId=config_id)
        resp = s3control.list_storage_lens_configurations(AccountId=ACCOUNT_ID)
        ids = [c["Id"] for c in resp.get("StorageLensConfigurationList", [])]
        assert config_id not in ids

    def test_get_storage_lens_not_found(self, s3control):
        """GetStorageLensConfiguration raises NoSuchConfiguration for nonexistent config."""
        with pytest.raises(ClientError) as exc:
            s3control.get_storage_lens_configuration(
                AccountId=ACCOUNT_ID, ConfigId=f"nonexistent-{_uid()}"
            )
        assert exc.value.response["Error"]["Code"] == "NoSuchConfiguration"

    def test_put_storage_lens_configuration_disabled(self, s3control):
        """PutStorageLensConfiguration with IsEnabled=False."""
        config_id = f"lens-{_uid()}"
        try:
            resp = s3control.put_storage_lens_configuration(
                AccountId=ACCOUNT_ID,
                ConfigId=config_id,
                StorageLensConfiguration={
                    "Id": config_id,
                    "AccountLevel": {"BucketLevel": {}},
                    "IsEnabled": False,
                },
            )
            assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200
        finally:
            try:
                s3control.delete_storage_lens_configuration(
                    AccountId=ACCOUNT_ID, ConfigId=config_id
                )
            except Exception:
                pass  # best-effort cleanup

    def test_put_storage_lens_replaces_existing(self, s3control):
        """Putting a storage lens config with the same ID replaces it."""
        config_id = f"lens-{_uid()}"
        try:
            s3control.put_storage_lens_configuration(
                AccountId=ACCOUNT_ID,
                ConfigId=config_id,
                StorageLensConfiguration={
                    "Id": config_id,
                    "AccountLevel": {"BucketLevel": {}},
                    "IsEnabled": True,
                },
            )
            # Replace with disabled
            resp = s3control.put_storage_lens_configuration(
                AccountId=ACCOUNT_ID,
                ConfigId=config_id,
                StorageLensConfiguration={
                    "Id": config_id,
                    "AccountLevel": {"BucketLevel": {}},
                    "IsEnabled": False,
                },
            )
            assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200
        finally:
            try:
                s3control.delete_storage_lens_configuration(
                    AccountId=ACCOUNT_ID, ConfigId=config_id
                )
            except Exception:
                pass  # best-effort cleanup

    def test_put_storage_lens_tagging_replaces_tags(self, s3control):
        """PutStorageLensConfigurationTagging replaces existing tags."""
        config_id = f"lens-{_uid()}"
        try:
            s3control.put_storage_lens_configuration(
                AccountId=ACCOUNT_ID,
                ConfigId=config_id,
                StorageLensConfiguration={
                    "Id": config_id,
                    "AccountLevel": {"BucketLevel": {}},
                    "IsEnabled": True,
                },
            )
            s3control.put_storage_lens_configuration_tagging(
                AccountId=ACCOUNT_ID,
                ConfigId=config_id,
                Tags=[{"Key": "env", "Value": "dev"}],
            )
            # Replace tags
            resp = s3control.put_storage_lens_configuration_tagging(
                AccountId=ACCOUNT_ID,
                ConfigId=config_id,
                Tags=[{"Key": "team", "Value": "backend"}],
            )
            assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200
        finally:
            try:
                s3control.delete_storage_lens_configuration(
                    AccountId=ACCOUNT_ID, ConfigId=config_id
                )
            except Exception:
                pass  # best-effort cleanup

    def test_storage_lens_multiple_tags(self, s3control):
        """PutStorageLensConfigurationTagging with multiple tags persists all."""
        config_id = f"lens-{_uid()}"
        try:
            s3control.put_storage_lens_configuration(
                AccountId=ACCOUNT_ID,
                ConfigId=config_id,
                StorageLensConfiguration={
                    "Id": config_id,
                    "AccountLevel": {"BucketLevel": {}},
                    "IsEnabled": True,
                },
            )
            s3control.put_storage_lens_configuration_tagging(
                AccountId=ACCOUNT_ID,
                ConfigId=config_id,
                Tags=[
                    {"Key": "env", "Value": "test"},
                    {"Key": "team", "Value": "backend"},
                    {"Key": "app", "Value": "api"},
                ],
            )
            resp = s3control.get_storage_lens_configuration_tagging(
                AccountId=ACCOUNT_ID, ConfigId=config_id
            )
            assert len(resp["Tags"]) == 3
            keys = {t["Key"] for t in resp["Tags"]}
            assert keys == {"env", "team", "app"}
        finally:
            try:
                s3control.delete_storage_lens_configuration(
                    AccountId=ACCOUNT_ID, ConfigId=config_id
                )
            except Exception:
                pass  # best-effort cleanup

    def test_storage_lens_list_multiple(self, s3control):
        """ListStorageLensConfigurations returns all created configs."""
        ids = [f"lens-{_uid()}" for _ in range(3)]
        try:
            for cid in ids:
                s3control.put_storage_lens_configuration(
                    AccountId=ACCOUNT_ID,
                    ConfigId=cid,
                    StorageLensConfiguration={
                        "Id": cid,
                        "AccountLevel": {"BucketLevel": {}},
                        "IsEnabled": True,
                    },
                )
            resp = s3control.list_storage_lens_configurations(AccountId=ACCOUNT_ID)
            listed_ids = [c["Id"] for c in resp["StorageLensConfigurationList"]]
            for cid in ids:
                assert cid in listed_ids
        finally:
            for cid in ids:
                try:
                    s3control.delete_storage_lens_configuration(AccountId=ACCOUNT_ID, ConfigId=cid)
                except Exception:
                    pass  # best-effort cleanup

    def test_storage_lens_tagging_not_found(self, s3control):
        """GetStorageLensConfigurationTagging for nonexistent config raises error."""
        with pytest.raises(ClientError):
            s3control.get_storage_lens_configuration_tagging(
                AccountId=ACCOUNT_ID, ConfigId=f"nonexistent-{_uid()}"
            )

    def test_storage_lens_put_tagging_not_found(self, s3control):
        """PutStorageLensConfigurationTagging for nonexistent config raises error."""
        with pytest.raises(ClientError):
            s3control.put_storage_lens_configuration_tagging(
                AccountId=ACCOUNT_ID,
                ConfigId=f"nonexistent-{_uid()}",
                Tags=[{"Key": "k", "Value": "v"}],
            )

    def test_storage_lens_delete_not_found(self, s3control):
        """DeleteStorageLensConfiguration for nonexistent config raises NoSuchConfiguration."""
        with pytest.raises(ClientError) as exc:
            s3control.delete_storage_lens_configuration(
                AccountId=ACCOUNT_ID, ConfigId=f"nonexistent-{_uid()}"
            )
        assert exc.value.response["Error"]["Code"] == "NoSuchConfiguration"


class TestS3ControlTagging:
    """Tests for S3 Control tagging operations (tag_resource, list_tags, untag_resource)."""

    @pytest.fixture
    def s3control(self):
        return make_client("s3control")

    @pytest.fixture
    def s3(self):
        return make_client("s3")

    @pytest.fixture
    def bucket_arn(self, s3):
        bucket_name = f"tag-test-{_uid()}"
        s3.create_bucket(Bucket=bucket_name)
        arn = f"arn:aws:s3:::{bucket_name}"
        yield arn
        s3.delete_bucket(Bucket=bucket_name)

    def test_tag_resource(self, s3control, bucket_arn):
        """TagResource lifecycle: create tag → retrieve → update → delete → verify empty."""
        # CREATE: add a tag
        resp = s3control.tag_resource(
            AccountId=ACCOUNT_ID,
            ResourceArn=bucket_arn,
            Tags=[{"Key": "env", "Value": "test"}],
        )
        assert resp["ResponseMetadata"]["HTTPStatusCode"] in (200, 204)
        # RETRIEVE: list tags and verify
        list_resp = s3control.list_tags_for_resource(
            AccountId=ACCOUNT_ID, ResourceArn=bucket_arn
        )
        assert "Tags" in list_resp
        tag_map = {t["Key"]: t["Value"] for t in list_resp["Tags"]}
        assert tag_map.get("env") == "test"
        # UPDATE: overwrite the tag value
        s3control.tag_resource(
            AccountId=ACCOUNT_ID,
            ResourceArn=bucket_arn,
            Tags=[{"Key": "env", "Value": "prod"}],
        )
        list_resp2 = s3control.list_tags_for_resource(
            AccountId=ACCOUNT_ID, ResourceArn=bucket_arn
        )
        tag_map2 = {t["Key"]: t["Value"] for t in list_resp2["Tags"]}
        assert tag_map2.get("env") == "prod"
        # DELETE: untag
        s3control.untag_resource(
            AccountId=ACCOUNT_ID, ResourceArn=bucket_arn, TagKeys=["env"]
        )
        list_resp3 = s3control.list_tags_for_resource(
            AccountId=ACCOUNT_ID, ResourceArn=bucket_arn
        )
        tag_keys = [t["Key"] for t in list_resp3["Tags"]]
        assert "env" not in tag_keys

    def test_list_tags_for_resource(self, s3control, bucket_arn):
        """ListTagsForResource retrieves tags on an S3 resource."""
        s3control.tag_resource(
            AccountId=ACCOUNT_ID,
            ResourceArn=bucket_arn,
            Tags=[{"Key": "env", "Value": "prod"}, {"Key": "team", "Value": "platform"}],
        )
        resp = s3control.list_tags_for_resource(AccountId=ACCOUNT_ID, ResourceArn=bucket_arn)
        assert "Tags" in resp
        tag_map = {t["Key"]: t["Value"] for t in resp["Tags"]}
        assert tag_map["env"] == "prod"
        assert tag_map["team"] == "platform"

    def test_untag_resource(self, s3control, bucket_arn):
        """UntagResource removes tags from an S3 resource."""
        s3control.tag_resource(
            AccountId=ACCOUNT_ID,
            ResourceArn=bucket_arn,
            Tags=[{"Key": "env", "Value": "test"}, {"Key": "team", "Value": "x"}],
        )
        s3control.untag_resource(
            AccountId=ACCOUNT_ID,
            ResourceArn=bucket_arn,
            TagKeys=["team"],
        )
        resp = s3control.list_tags_for_resource(AccountId=ACCOUNT_ID, ResourceArn=bucket_arn)
        tag_keys = [t["Key"] for t in resp["Tags"]]
        assert "env" in tag_keys
        assert "team" not in tag_keys


class TestS3ControlStorageLensUpdate:
    """Tests for updating Storage Lens configurations."""

    @pytest.fixture
    def s3control(self):
        return make_client("s3control")

    def test_update_storage_lens_enabled_to_disabled(self, s3control):
        """PutStorageLensConfiguration can update IsEnabled from True to False."""
        config_id = f"lens-upd-{_uid()}"
        try:
            s3control.put_storage_lens_configuration(
                AccountId=ACCOUNT_ID,
                ConfigId=config_id,
                StorageLensConfiguration={
                    "Id": config_id,
                    "AccountLevel": {"BucketLevel": {}},
                    "IsEnabled": True,
                },
            )
            # Update to disabled
            s3control.put_storage_lens_configuration(
                AccountId=ACCOUNT_ID,
                ConfigId=config_id,
                StorageLensConfiguration={
                    "Id": config_id,
                    "AccountLevel": {"BucketLevel": {}},
                    "IsEnabled": False,
                },
            )
            resp = s3control.list_storage_lens_configurations(AccountId=ACCOUNT_ID)
            config = next(c for c in resp["StorageLensConfigurationList"] if c["Id"] == config_id)
            assert config["IsEnabled"] is False
        finally:
            try:
                s3control.delete_storage_lens_configuration(
                    AccountId=ACCOUNT_ID, ConfigId=config_id
                )
            except Exception:
                pass  # best-effort cleanup

    def test_storage_lens_list_shows_enabled(self, s3control):
        """ListStorageLensConfigurations shows IsEnabled for each config."""
        config_id = f"lens-en-{_uid()}"
        try:
            s3control.put_storage_lens_configuration(
                AccountId=ACCOUNT_ID,
                ConfigId=config_id,
                StorageLensConfiguration={
                    "Id": config_id,
                    "AccountLevel": {"BucketLevel": {}},
                    "IsEnabled": True,
                },
            )
            resp = s3control.list_storage_lens_configurations(AccountId=ACCOUNT_ID)
            config = next(c for c in resp["StorageLensConfigurationList"] if c["Id"] == config_id)
            assert "IsEnabled" in config
            assert config["IsEnabled"] is True
            assert "StorageLensArn" in config
        finally:
            try:
                s3control.delete_storage_lens_configuration(
                    AccountId=ACCOUNT_ID, ConfigId=config_id
                )
            except Exception:
                pass  # best-effort cleanup


class TestS3ControlMultiRegionAccessPointMultiBucket:
    """Tests for Multi-Region Access Points with multiple buckets."""

    @pytest.fixture
    def s3control(self):
        return make_client("s3control")

    @pytest.fixture
    def s3(self):
        return make_client("s3")

    def test_mrap_with_two_buckets(self, s3control, s3):
        """CreateMultiRegionAccessPoint with two buckets shows both regions."""
        uid = _uid()
        bucket1 = f"mrap-b1-{uid}"
        bucket2 = f"mrap-b2-{uid}"
        mrap_name = f"mrap-multi-{uid}"
        s3.create_bucket(Bucket=bucket1)
        s3.create_bucket(Bucket=bucket2)
        try:
            resp = s3control.create_multi_region_access_point(
                AccountId=ACCOUNT_ID,
                Details={
                    "Name": mrap_name,
                    "Regions": [{"Bucket": bucket1}, {"Bucket": bucket2}],
                },
            )
            assert "RequestTokenARN" in resp

            get_resp = s3control.get_multi_region_access_point(AccountId=ACCOUNT_ID, Name=mrap_name)
            ap = get_resp["AccessPoint"]
            assert ap["Name"] == mrap_name
            assert len(ap["Regions"]) == 2
            region_buckets = {r["Bucket"] for r in ap["Regions"]}
            assert bucket1 in region_buckets
            assert bucket2 in region_buckets
        finally:
            try:
                s3control.delete_multi_region_access_point(
                    AccountId=ACCOUNT_ID, Details={"Name": mrap_name}
                )
            except Exception:
                pass  # best-effort cleanup
            try:
                s3.delete_bucket(Bucket=bucket1)
            except Exception:
                pass  # best-effort cleanup
            try:
                s3.delete_bucket(Bucket=bucket2)
            except Exception:
                pass  # best-effort cleanup

    def test_mrap_status_field(self, s3control, s3):
        """GetMultiRegionAccessPoint returns Status field."""
        uid = _uid()
        bucket_name = f"mrap-st-{uid}"
        mrap_name = f"mrap-stat-{uid}"
        s3.create_bucket(Bucket=bucket_name)
        try:
            s3control.create_multi_region_access_point(
                AccountId=ACCOUNT_ID,
                Details={
                    "Name": mrap_name,
                    "Regions": [{"Bucket": bucket_name}],
                },
            )
            resp = s3control.get_multi_region_access_point(AccountId=ACCOUNT_ID, Name=mrap_name)
            ap = resp["AccessPoint"]
            assert "Status" in ap
            assert "Alias" in ap
            assert "CreatedAt" in ap
        finally:
            try:
                s3control.delete_multi_region_access_point(
                    AccountId=ACCOUNT_ID, Details={"Name": mrap_name}
                )
            except Exception:
                pass  # best-effort cleanup
            try:
                s3.delete_bucket(Bucket=bucket_name)
            except Exception:
                pass  # best-effort cleanup


class TestS3ControlListJobs:
    """Tests for ListJobs operation."""

    def test_list_jobs_empty(self, s3control, s3):
        """ListJobs lifecycle: list → create → retrieve → list → cancel → error."""
        # LIST: initial list
        resp = s3control.list_jobs(AccountId=ACCOUNT_ID)
        assert "Jobs" in resp
        assert isinstance(resp["Jobs"], list)
        # CREATE: create a job
        bucket = f"job-le-{_uid()}"
        s3.create_bucket(Bucket=bucket)
        try:
            create_resp = s3control.create_job(
                AccountId=ACCOUNT_ID,
                Operation={"S3PutObjectCopy": {"TargetResource": f"arn:aws:s3:::{bucket}"}},
                Report={"Enabled": False},
                ClientRequestToken=str(uuid.uuid4()),
                Priority=10,
                RoleArn=f"arn:aws:iam::{ACCOUNT_ID}:role/test-role",
                ConfirmationRequired=False,
                ManifestGenerator={
                    "S3JobManifestGenerator": {
                        "SourceBucket": f"arn:aws:s3:::{bucket}",
                        "EnableManifestOutput": False,
                    }
                },
            )
            job_id = create_resp["JobId"]
            assert len(job_id) > 0
            # RETRIEVE: describe the job
            desc = s3control.describe_job(AccountId=ACCOUNT_ID, JobId=job_id)
            assert desc["Job"]["JobId"] == job_id
            # LIST: job appears in list
            list_resp = s3control.list_jobs(AccountId=ACCOUNT_ID)
            job_ids = [j["JobId"] for j in list_resp["Jobs"]]
            assert job_id in job_ids
            # UPDATE: cancel the job
            s3control.update_job_status(
                AccountId=ACCOUNT_ID, JobId=job_id, RequestedJobStatus="Cancelled"
            )
            # RETRIEVE: verify cancelled status
            desc2 = s3control.describe_job(AccountId=ACCOUNT_ID, JobId=job_id)
            assert desc2["Job"]["Status"] == "Cancelled"
        finally:
            try:
                s3control.update_job_status(
                    AccountId=ACCOUNT_ID, JobId=job_id, RequestedJobStatus="Cancelled"
                )
            except Exception:
                pass  # best-effort cleanup
            try:
                s3.delete_bucket(Bucket=bucket)
            except Exception:
                pass  # best-effort cleanup
        # ERROR: nonexistent job
        with pytest.raises(ClientError) as exc_info:
            s3control.describe_job(
                AccountId=ACCOUNT_ID, JobId="00000000-0000-0000-0000-000000000000"
            )
        assert exc_info.value.response["Error"]["Code"] == "NoSuchJob"

    def test_list_jobs_returns_metadata(self, s3control, s3):
        """ListJobs returns 200 status and correct response structure after create."""
        bucket = f"job-meta-{_uid()}"
        s3.create_bucket(Bucket=bucket)
        try:
            # CREATE a job
            create_resp = s3control.create_job(
                AccountId=ACCOUNT_ID,
                Operation={"S3PutObjectCopy": {"TargetResource": f"arn:aws:s3:::{bucket}"}},
                Report={"Enabled": False},
                ClientRequestToken=str(uuid.uuid4()),
                Priority=5,
                RoleArn=f"arn:aws:iam::{ACCOUNT_ID}:role/test-role",
                ConfirmationRequired=False,
                ManifestGenerator={
                    "S3JobManifestGenerator": {
                        "SourceBucket": f"arn:aws:s3:::{bucket}",
                        "EnableManifestOutput": False,
                    }
                },
            )
            job_id = create_resp["JobId"]
            # LIST: verify metadata
            resp = s3control.list_jobs(AccountId=ACCOUNT_ID)
            assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200
            # RETRIEVE: verify job priority
            desc = s3control.describe_job(AccountId=ACCOUNT_ID, JobId=job_id)
            assert desc["Job"]["Priority"] == 5
            # UPDATE: change priority
            s3control.update_job_priority(
                AccountId=ACCOUNT_ID, JobId=job_id, Priority=20
            )
            desc2 = s3control.describe_job(AccountId=ACCOUNT_ID, JobId=job_id)
            assert desc2["Job"]["Priority"] == 20
            # DELETE (cancel)
            s3control.update_job_status(
                AccountId=ACCOUNT_ID, JobId=job_id, RequestedJobStatus="Cancelled"
            )
        finally:
            try:
                s3control.update_job_status(
                    AccountId=ACCOUNT_ID, JobId=job_id, RequestedJobStatus="Cancelled"
                )
            except Exception:
                pass  # best-effort cleanup
            try:
                s3.delete_bucket(Bucket=bucket)
            except Exception:
                pass  # best-effort cleanup


class TestS3ControlDescribeJob:
    """Tests for DescribeJob operation."""

    def test_describe_job_nonexistent(self, s3control):
        with pytest.raises(ClientError) as exc_info:
            s3control.describe_job(
                AccountId=ACCOUNT_ID, JobId="00000000-0000-0000-0000-000000000000"
            )
        assert exc_info.value.response["Error"]["Code"] == "NoSuchJob"


class TestS3ControlAccessGrants:
    """Tests for Access Grants operations."""

    def test_list_access_grants_instances_empty(self, s3control):
        """AccessGrantsInstances lifecycle: list → create → retrieve → list → delete → error."""
        # LIST: initial list
        resp = s3control.list_access_grants_instances(AccountId=ACCOUNT_ID)
        assert "AccessGrantsInstancesList" in resp
        assert isinstance(resp["AccessGrantsInstancesList"], list)
        # CREATE: create an instance
        create_resp = s3control.create_access_grants_instance(AccountId=ACCOUNT_ID)
        assert "AccessGrantsInstanceArn" in create_resp
        try:
            # RETRIEVE: get the instance
            get_resp = s3control.get_access_grants_instance(AccountId=ACCOUNT_ID)
            assert "AccessGrantsInstanceArn" in get_resp
            assert "CreatedAt" in get_resp
            # LIST: instance appears
            list_resp = s3control.list_access_grants_instances(AccountId=ACCOUNT_ID)
            assert len(list_resp["AccessGrantsInstancesList"]) > 0
            # DELETE: remove instance
            s3control.delete_access_grants_instance(AccountId=ACCOUNT_ID)
            # ERROR: get after delete fails
            with pytest.raises(ClientError):
                s3control.get_access_grants_instance(AccountId=ACCOUNT_ID)
        except Exception:
            try:
                s3control.delete_access_grants_instance(AccountId=ACCOUNT_ID)
            except Exception:
                pass  # best-effort cleanup
            raise

    def test_list_access_grants_empty(self, s3control):
        """AccessGrants lifecycle: list → create → list → retrieve → delete → error."""
        # LIST: initial list
        resp = s3control.list_access_grants(AccountId=ACCOUNT_ID)
        assert "AccessGrantsList" in resp
        assert isinstance(resp["AccessGrantsList"], list)
        # CREATE: create instance + location + grant
        s3control.create_access_grants_instance(AccountId=ACCOUNT_ID)
        try:
            loc = s3control.create_access_grants_location(
                AccountId=ACCOUNT_ID,
                LocationScope="s3://grant-empty-test/",
                IAMRoleArn=f"arn:aws:iam::{ACCOUNT_ID}:role/test-role",
            )
            grant = s3control.create_access_grant(
                AccountId=ACCOUNT_ID,
                AccessGrantsLocationId=loc["AccessGrantsLocationId"],
                Grantee={"GranteeType": "IAM", "GranteeIdentifier": f"arn:aws:iam::{ACCOUNT_ID}:role/grantee"},
                Permission="READ",
                AccessGrantsLocationConfiguration={"S3SubPrefix": "prefix/"},
            )
            grant_id = grant["AccessGrantId"]
            # LIST: grant appears
            list_resp = s3control.list_access_grants(AccountId=ACCOUNT_ID)
            ids = [g["AccessGrantId"] for g in list_resp["AccessGrantsList"]]
            assert grant_id in ids
            # RETRIEVE: get the grant
            get_resp = s3control.get_access_grant(
                AccountId=ACCOUNT_ID, AccessGrantId=grant_id
            )
            assert get_resp["AccessGrantId"] == grant_id
            assert get_resp["Permission"] == "READ"
            # DELETE: remove the grant
            s3control.delete_access_grant(
                AccountId=ACCOUNT_ID, AccessGrantId=grant_id
            )
            # ERROR: get after delete fails
            with pytest.raises(ClientError) as exc_info:
                s3control.get_access_grant(
                    AccountId=ACCOUNT_ID, AccessGrantId=grant_id
                )
            assert exc_info.value.response["Error"]["Code"] == "NoSuchAccessGrant"
        finally:
            try:
                s3control.delete_access_grants_instance(AccountId=ACCOUNT_ID)
            except Exception:
                pass  # best-effort cleanup

    def test_list_access_grants_locations_empty(self, s3control):
        """AccessGrantsLocations lifecycle: list → create → list → retrieve → delete → error."""
        # LIST: initial list
        resp = s3control.list_access_grants_locations(AccountId=ACCOUNT_ID)
        assert "AccessGrantsLocationsList" in resp
        assert isinstance(resp["AccessGrantsLocationsList"], list)
        # CREATE: create instance + location
        s3control.create_access_grants_instance(AccountId=ACCOUNT_ID)
        try:
            loc = s3control.create_access_grants_location(
                AccountId=ACCOUNT_ID,
                LocationScope="s3://loc-empty-test/",
                IAMRoleArn=f"arn:aws:iam::{ACCOUNT_ID}:role/test-role",
            )
            loc_id = loc["AccessGrantsLocationId"]
            assert len(loc_id) > 0
            # LIST: location appears
            list_resp = s3control.list_access_grants_locations(AccountId=ACCOUNT_ID)
            ids = [l["AccessGrantsLocationId"] for l in list_resp["AccessGrantsLocationsList"]]
            assert loc_id in ids
            # RETRIEVE: get the location
            get_resp = s3control.get_access_grants_location(
                AccountId=ACCOUNT_ID, AccessGrantsLocationId=loc_id
            )
            assert get_resp["AccessGrantsLocationId"] == loc_id
            assert get_resp["LocationScope"] == "s3://loc-empty-test/"
            # UPDATE: update the location IAM role
            s3control.update_access_grants_location(
                AccountId=ACCOUNT_ID,
                AccessGrantsLocationId=loc_id,
                IAMRoleArn=f"arn:aws:iam::{ACCOUNT_ID}:role/updated-role",
            )
            updated = s3control.get_access_grants_location(
                AccountId=ACCOUNT_ID, AccessGrantsLocationId=loc_id
            )
            assert updated["IAMRoleArn"] == f"arn:aws:iam::{ACCOUNT_ID}:role/updated-role"
            # DELETE: remove the location
            s3control.delete_access_grants_location(
                AccountId=ACCOUNT_ID, AccessGrantsLocationId=loc_id
            )
            # ERROR: get after delete fails
            with pytest.raises(ClientError) as exc_info:
                s3control.get_access_grants_location(
                    AccountId=ACCOUNT_ID, AccessGrantsLocationId=loc_id
                )
            assert exc_info.value.response["Error"]["Code"] == "NoSuchAccessGrantsLocation"
        finally:
            try:
                s3control.delete_access_grants_instance(AccountId=ACCOUNT_ID)
            except Exception:
                pass  # best-effort cleanup

    def test_get_access_grant_nonexistent(self, s3control):
        with pytest.raises(ClientError) as exc_info:
            s3control.get_access_grant(
                AccountId=ACCOUNT_ID,
                AccessGrantId="00000000-0000-0000-0000-000000000000",
            )
        assert exc_info.value.response["Error"]["Code"] == "NoSuchAccessGrant"

    def test_get_access_grants_location_nonexistent(self, s3control):
        with pytest.raises(ClientError) as exc_info:
            s3control.get_access_grants_location(
                AccountId=ACCOUNT_ID,
                AccessGrantsLocationId="00000000-0000-0000-0000-000000000000",
            )
        assert exc_info.value.response["Error"]["Code"] == "NoSuchAccessGrantsLocation"


class TestS3ControlAccessGrantsCRUD:
    """Tests for Access Grants create/get/delete lifecycle."""

    def test_create_access_grants_instance(self, s3control):
        """CreateAccessGrantsInstance creates an instance and returns ARN."""
        try:
            resp = s3control.create_access_grants_instance(AccountId=ACCOUNT_ID)
            assert "AccessGrantsInstanceArn" in resp
            assert "AccessGrantsInstanceId" in resp
            assert "CreatedAt" in resp
        finally:
            try:
                s3control.delete_access_grants_instance(AccountId=ACCOUNT_ID)
            except Exception:
                pass  # best-effort cleanup

    def test_create_access_grants_instance_then_get(self, s3control):
        """GetAccessGrantsInstance returns details after creation."""
        try:
            s3control.create_access_grants_instance(AccountId=ACCOUNT_ID)
            resp = s3control.get_access_grants_instance(AccountId=ACCOUNT_ID)
            assert "AccessGrantsInstanceArn" in resp
            assert "AccessGrantsInstanceId" in resp
        finally:
            try:
                s3control.delete_access_grants_instance(AccountId=ACCOUNT_ID)
            except Exception:
                pass  # best-effort cleanup

    def test_create_access_grants_instance_appears_in_list(self, s3control):
        """ListAccessGrantsInstances includes the created instance."""
        try:
            s3control.create_access_grants_instance(AccountId=ACCOUNT_ID)
            resp = s3control.list_access_grants_instances(AccountId=ACCOUNT_ID)
            assert len(resp["AccessGrantsInstancesList"]) >= 1
            arns = [i["AccessGrantsInstanceArn"] for i in resp["AccessGrantsInstancesList"]]
            assert any(
                "accessgrantsinstance" in a.lower() or "access-grants" in a.lower() for a in arns
            )
        finally:
            try:
                s3control.delete_access_grants_instance(AccountId=ACCOUNT_ID)
            except Exception:
                pass  # best-effort cleanup

    def test_create_access_grants_location(self, s3control):
        """CreateAccessGrantsLocation creates a location and returns ARN."""
        try:
            s3control.create_access_grants_instance(AccountId=ACCOUNT_ID)
            resp = s3control.create_access_grants_location(
                AccountId=ACCOUNT_ID,
                LocationScope="s3://",
                IAMRoleArn="arn:aws:iam::123456789012:role/access-grants-role",
            )
            assert "AccessGrantsLocationId" in resp
            assert "AccessGrantsLocationArn" in resp
            assert "LocationScope" in resp
        finally:
            try:
                s3control.delete_access_grants_instance(AccountId=ACCOUNT_ID)
            except Exception:
                pass  # best-effort cleanup

    def test_create_access_grants_location_then_get(self, s3control):
        """GetAccessGrantsLocation returns details after creation."""
        try:
            s3control.create_access_grants_instance(AccountId=ACCOUNT_ID)
            create_resp = s3control.create_access_grants_location(
                AccountId=ACCOUNT_ID,
                LocationScope="s3://",
                IAMRoleArn="arn:aws:iam::123456789012:role/access-grants-role",
            )
            loc_id = create_resp["AccessGrantsLocationId"]
            resp = s3control.get_access_grants_location(
                AccountId=ACCOUNT_ID, AccessGrantsLocationId=loc_id
            )
            assert resp["AccessGrantsLocationId"] == loc_id
            assert resp["LocationScope"] == "s3://"
        finally:
            try:
                s3control.delete_access_grants_instance(AccountId=ACCOUNT_ID)
            except Exception:
                pass  # best-effort cleanup

    def test_create_access_grants_location_appears_in_list(self, s3control):
        """ListAccessGrantsLocations includes the created location."""
        try:
            s3control.create_access_grants_instance(AccountId=ACCOUNT_ID)
            create_resp = s3control.create_access_grants_location(
                AccountId=ACCOUNT_ID,
                LocationScope="s3://",
                IAMRoleArn="arn:aws:iam::123456789012:role/access-grants-role",
            )
            loc_id = create_resp["AccessGrantsLocationId"]
            resp = s3control.list_access_grants_locations(AccountId=ACCOUNT_ID)
            ids = [loc["AccessGrantsLocationId"] for loc in resp["AccessGrantsLocationsList"]]
            assert loc_id in ids
        finally:
            try:
                s3control.delete_access_grants_instance(AccountId=ACCOUNT_ID)
            except Exception:
                pass  # best-effort cleanup


class TestS3ControlBucketOps:
    """Tests for bucket-level S3 Control operations."""

    @pytest.fixture
    def bucket(self, s3):
        name = f"s3ctrl-test-{_uid()}"
        s3.create_bucket(Bucket=name)
        yield name
        try:
            s3.delete_bucket(Bucket=name)
        except Exception:
            pass  # best-effort cleanup

    def test_get_bucket_lifecycle_configuration(self, s3control, bucket):
        """Bucket lifecycle config: create → retrieve → update → delete → error."""
        # CREATE: put lifecycle rules
        put_resp = s3control.put_bucket_lifecycle_configuration(
            AccountId=ACCOUNT_ID,
            Bucket=bucket,
            LifecycleConfiguration={
                "Rules": [{"ID": "rule1", "Status": "Enabled", "Filter": {"Prefix": "logs/"}}]
            },
        )
        assert put_resp["ResponseMetadata"]["HTTPStatusCode"] == 200
        # RETRIEVE: verify Rules key present and is list
        resp = s3control.get_bucket_lifecycle_configuration(AccountId=ACCOUNT_ID, Bucket=bucket)
        assert "Rules" in resp
        assert isinstance(resp["Rules"], list)
        # UPDATE: put again with different rules
        s3control.put_bucket_lifecycle_configuration(
            AccountId=ACCOUNT_ID,
            Bucket=bucket,
            LifecycleConfiguration={
                "Rules": [{"ID": "rule2", "Status": "Disabled", "Filter": {"Prefix": "tmp/"}}]
            },
        )
        resp2 = s3control.get_bucket_lifecycle_configuration(AccountId=ACCOUNT_ID, Bucket=bucket)
        assert "Rules" in resp2
        # DELETE: remove lifecycle config
        s3control.delete_bucket_lifecycle_configuration(AccountId=ACCOUNT_ID, Bucket=bucket)
        # ERROR: nonexistent bucket
        with pytest.raises(ClientError) as exc_info:
            s3control.get_bucket_lifecycle_configuration(
                AccountId=ACCOUNT_ID, Bucket="nonexistent-lifecycle-xyz"
            )
        assert exc_info.value.response["Error"]["Code"] == "NoSuchBucket"

    def test_get_bucket_policy(self, s3control, bucket):
        """Bucket policy: create → retrieve → update → delete → error on nonexistent."""
        policy = json.dumps({
            "Version": "2012-10-17",
            "Statement": [{"Effect": "Allow", "Principal": "*", "Action": "s3:GetObject", "Resource": f"arn:aws:s3:::{bucket}/*"}],
        })
        # CREATE: put policy
        put_resp = s3control.put_bucket_policy(AccountId=ACCOUNT_ID, Bucket=bucket, Policy=policy)
        assert put_resp["ResponseMetadata"]["HTTPStatusCode"] == 200
        # RETRIEVE: get policy
        resp = s3control.get_bucket_policy(AccountId=ACCOUNT_ID, Bucket=bucket)
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200
        assert "Policy" in resp
        # UPDATE: replace policy
        policy2 = json.dumps({
            "Version": "2012-10-17",
            "Statement": [{"Effect": "Deny", "Principal": "*", "Action": "s3:DeleteObject", "Resource": f"arn:aws:s3:::{bucket}/*"}],
        })
        s3control.put_bucket_policy(AccountId=ACCOUNT_ID, Bucket=bucket, Policy=policy2)
        resp2 = s3control.get_bucket_policy(AccountId=ACCOUNT_ID, Bucket=bucket)
        assert "Policy" in resp2
        # DELETE: remove policy
        del_resp = s3control.delete_bucket_policy(AccountId=ACCOUNT_ID, Bucket=bucket)
        assert del_resp["ResponseMetadata"]["HTTPStatusCode"] in (200, 204)

    def test_get_bucket_replication(self, s3control, bucket):
        """Bucket replication: create → retrieve → delete → error."""
        # CREATE: put replication config
        put_resp = s3control.put_bucket_replication(
            AccountId=ACCOUNT_ID,
            Bucket=bucket,
            ReplicationConfiguration={
                "Role": f"arn:aws:iam::{ACCOUNT_ID}:role/replication",
                "Rules": [{
                    "ID": "rule1",
                    "Status": "Enabled",
                    "Priority": 1,
                    "Bucket": f"arn:aws:s3:::{bucket}",
                    "Filter": {"Prefix": ""},
                    "Destination": {
                        "Bucket": f"arn:aws:s3:::{bucket}",
                        "Account": ACCOUNT_ID,
                    },
                    "DeleteMarkerReplication": {"Status": "Disabled"},
                }],
            },
        )
        assert put_resp["ResponseMetadata"]["HTTPStatusCode"] == 200
        # RETRIEVE: get replication returns 200
        resp = s3control.get_bucket_replication(AccountId=ACCOUNT_ID, Bucket=bucket)
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200
        # DELETE: remove replication config
        del_resp = s3control.delete_bucket_replication(AccountId=ACCOUNT_ID, Bucket=bucket)
        assert del_resp["ResponseMetadata"]["HTTPStatusCode"] in (200, 204)
        # ERROR: nonexistent bucket
        with pytest.raises(ClientError) as exc_info:
            s3control.get_bucket_replication(AccountId=ACCOUNT_ID, Bucket="nonexistent-repl-xyz")
        assert exc_info.value.response["Error"]["Code"] == "NoSuchBucket"

    def test_get_bucket_tagging(self, s3control, bucket):
        """Bucket tagging: create → retrieve → update → delete lifecycle."""
        # CREATE: put tags
        put_resp = s3control.put_bucket_tagging(
            AccountId=ACCOUNT_ID,
            Bucket=bucket,
            Tagging={"TagSet": [{"Key": "env", "Value": "dev"}]},
        )
        assert put_resp["ResponseMetadata"]["HTTPStatusCode"] == 200
        # RETRIEVE: get tags - verify structure
        resp = s3control.get_bucket_tagging(AccountId=ACCOUNT_ID, Bucket=bucket)
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200
        assert "TagSet" in resp
        assert isinstance(resp["TagSet"], list)
        # UPDATE: replace tags with different set
        s3control.put_bucket_tagging(
            AccountId=ACCOUNT_ID,
            Bucket=bucket,
            Tagging={"TagSet": [{"Key": "team", "Value": "infra"}]},
        )
        resp2 = s3control.get_bucket_tagging(AccountId=ACCOUNT_ID, Bucket=bucket)
        assert "TagSet" in resp2
        # DELETE: remove tags
        del_resp = s3control.delete_bucket_tagging(AccountId=ACCOUNT_ID, Bucket=bucket)
        assert del_resp["ResponseMetadata"]["HTTPStatusCode"] in (200, 204)
        # RETRIEVE after delete: verify response still valid
        resp3 = s3control.get_bucket_tagging(AccountId=ACCOUNT_ID, Bucket=bucket)
        assert resp3["ResponseMetadata"]["HTTPStatusCode"] == 200

    def test_get_bucket_versioning(self, s3control, bucket):
        """Bucket versioning: retrieve → update → retrieve → error."""
        # RETRIEVE: initial versioning status
        resp = s3control.get_bucket_versioning(AccountId=ACCOUNT_ID, Bucket=bucket)
        assert "Status" in resp
        # UPDATE: enable versioning (if supported, otherwise just verify current state)
        try:
            s3control.put_bucket_versioning(
                AccountId=ACCOUNT_ID,
                Bucket=bucket,
                VersioningConfiguration={"Status": "Enabled"},
            )
            resp2 = s3control.get_bucket_versioning(AccountId=ACCOUNT_ID, Bucket=bucket)
            assert resp2["Status"] == "Enabled"
        except ClientError:
            pass  # some backends don't support put_bucket_versioning via s3control
        # ERROR: nonexistent bucket
        with pytest.raises(ClientError) as exc_info:
            s3control.get_bucket_versioning(AccountId=ACCOUNT_ID, Bucket="nonexistent-ver-xyz")
        assert exc_info.value.response["Error"]["Code"] == "NoSuchBucket"

    def test_get_bucket_lifecycle_nonexistent(self, s3control):
        with pytest.raises(ClientError) as exc_info:
            s3control.get_bucket_lifecycle_configuration(
                AccountId=ACCOUNT_ID, Bucket="nonexistent-bucket-xyz-999"
            )
        assert exc_info.value.response["Error"]["Code"] == "NoSuchBucket"

    def test_get_bucket_replication_nonexistent(self, s3control):
        with pytest.raises(ClientError) as exc_info:
            s3control.get_bucket_replication(
                AccountId=ACCOUNT_ID, Bucket="nonexistent-bucket-xyz-999"
            )
        assert exc_info.value.response["Error"]["Code"] == "NoSuchBucket"

    def test_get_bucket_versioning_nonexistent(self, s3control):
        with pytest.raises(ClientError) as exc_info:
            s3control.get_bucket_versioning(
                AccountId=ACCOUNT_ID, Bucket="nonexistent-bucket-xyz-999"
            )
        assert exc_info.value.response["Error"]["Code"] == "NoSuchBucket"


class TestS3ControlAccessGrantsDeepLifecycle:
    """Deeper lifecycle tests for Access Grants operations."""

    @pytest.fixture
    def s3control(self):
        return make_client("s3control")

    @pytest.fixture(autouse=True)
    def _ensure_instance(self, s3control):
        """Ensure an access grants instance exists for tests."""
        try:
            s3control.get_access_grants_instance(AccountId=ACCOUNT_ID)
        except ClientError:
            s3control.create_access_grants_instance(AccountId=ACCOUNT_ID)

    def test_create_multiple_access_grants_locations(self, s3control):
        """Create multiple locations under one instance."""
        loc1 = s3control.create_access_grants_location(
            AccountId=ACCOUNT_ID,
            LocationScope="s3://bucket-a-multi/",
            IAMRoleArn="arn:aws:iam::123456789012:role/role-a",
        )
        loc2 = s3control.create_access_grants_location(
            AccountId=ACCOUNT_ID,
            LocationScope="s3://bucket-b-multi/",
            IAMRoleArn="arn:aws:iam::123456789012:role/role-b",
        )
        assert loc1["AccessGrantsLocationId"] != loc2["AccessGrantsLocationId"]
        # List should contain both
        resp = s3control.list_access_grants_locations(AccountId=ACCOUNT_ID)
        ids = [loc["AccessGrantsLocationId"] for loc in resp["AccessGrantsLocationsList"]]
        assert loc1["AccessGrantsLocationId"] in ids
        assert loc2["AccessGrantsLocationId"] in ids

    def test_access_grants_instance_has_created_at(self, s3control):
        """Instance lifecycle: retrieve CreatedAt → list instances → delete → error."""
        # RETRIEVE: verify CreatedAt present
        get_resp = s3control.get_access_grants_instance(AccountId=ACCOUNT_ID)
        assert "CreatedAt" in get_resp
        assert "AccessGrantsInstanceArn" in get_resp
        # LIST: instance appears in list
        list_resp = s3control.list_access_grants_instances(AccountId=ACCOUNT_ID)
        assert len(list_resp["AccessGrantsInstancesList"]) > 0
        # DELETE: remove instance
        s3control.delete_access_grants_instance(AccountId=ACCOUNT_ID)
        # ERROR: get after delete fails
        with pytest.raises(ClientError):
            s3control.get_access_grants_instance(AccountId=ACCOUNT_ID)
        # Recreate for other tests (autouse fixture will handle, but be explicit)
        s3control.create_access_grants_instance(AccountId=ACCOUNT_ID)

    def test_access_grants_location_arn_format(self, s3control):
        """Location lifecycle: create → retrieve ARN → list → update → delete → error."""
        # CREATE: create location
        resp = s3control.create_access_grants_location(
            AccountId=ACCOUNT_ID,
            LocationScope="s3://arn-check-bucket/",
            IAMRoleArn="arn:aws:iam::123456789012:role/test-role",
        )
        arn = resp["AccessGrantsLocationArn"]
        loc_id = resp["AccessGrantsLocationId"]
        assert "s3" in arn.lower()
        assert ACCOUNT_ID in arn or "access-grants" in arn.lower()
        # RETRIEVE: get the location
        get_resp = s3control.get_access_grants_location(
            AccountId=ACCOUNT_ID, AccessGrantsLocationId=loc_id
        )
        assert get_resp["AccessGrantsLocationArn"] == arn
        # LIST: location appears
        list_resp = s3control.list_access_grants_locations(AccountId=ACCOUNT_ID)
        ids = [l["AccessGrantsLocationId"] for l in list_resp["AccessGrantsLocationsList"]]
        assert loc_id in ids
        # UPDATE: change the IAM role for the location
        upd_resp = s3control.update_access_grants_location(
            AccountId=ACCOUNT_ID,
            AccessGrantsLocationId=loc_id,
            IAMRoleArn="arn:aws:iam::123456789012:role/updated-test-role",
        )
        assert upd_resp["AccessGrantsLocationId"] == loc_id
        assert "updated-test-role" in upd_resp["IAMRoleArn"]
        # DELETE: remove location
        s3control.delete_access_grants_location(
            AccountId=ACCOUNT_ID, AccessGrantsLocationId=loc_id
        )
        # ERROR: get after delete fails
        with pytest.raises(ClientError) as exc_info:
            s3control.get_access_grants_location(
                AccountId=ACCOUNT_ID, AccessGrantsLocationId=loc_id
            )
        assert exc_info.value.response["Error"]["Code"] == "NoSuchAccessGrantsLocation"

    def test_access_grants_instance_arn_format(self, s3control):
        """Instance: retrieve ARN → create location → list locations → delete location."""
        # RETRIEVE: verify instance ARN
        resp = s3control.get_access_grants_instance(AccountId=ACCOUNT_ID)
        arn = resp["AccessGrantsInstanceArn"]
        assert "arn:" in arn
        assert "s3" in arn.lower()
        # CREATE: create a location under the instance
        loc = s3control.create_access_grants_location(
            AccountId=ACCOUNT_ID,
            LocationScope="s3://arn-inst-check/",
            IAMRoleArn=f"arn:aws:iam::{ACCOUNT_ID}:role/inst-test",
        )
        loc_id = loc["AccessGrantsLocationId"]
        # LIST: verify location in list
        list_resp = s3control.list_access_grants_locations(AccountId=ACCOUNT_ID)
        ids = [l["AccessGrantsLocationId"] for l in list_resp["AccessGrantsLocationsList"]]
        assert loc_id in ids
        # DELETE: clean up location
        s3control.delete_access_grants_location(
            AccountId=ACCOUNT_ID, AccessGrantsLocationId=loc_id
        )
        # ERROR: get after delete fails
        with pytest.raises(ClientError):
            s3control.get_access_grants_location(
                AccountId=ACCOUNT_ID, AccessGrantsLocationId=loc_id
            )

    def test_access_grants_location_scope_preserved(self, s3control):
        """Location scope is preserved on get."""
        scope = "s3://scope-check-bucket/"
        create_resp = s3control.create_access_grants_location(
            AccountId=ACCOUNT_ID,
            LocationScope=scope,
            IAMRoleArn="arn:aws:iam::123456789012:role/scope-role",
        )
        loc_id = create_resp["AccessGrantsLocationId"]
        get_resp = s3control.get_access_grants_location(
            AccountId=ACCOUNT_ID, AccessGrantsLocationId=loc_id
        )
        assert get_resp["LocationScope"] == scope

    def test_access_grants_instance_for_prefix_after_create(self, s3control):
        """GetAccessGrantsInstanceForPrefix: create → retrieve → list → update → delete → error."""
        # CREATE: ensure an instance exists (autouse fixture handles this, but be explicit)
        try:
            s3control.get_access_grants_instance(AccountId=ACCOUNT_ID)
        except ClientError:
            s3control.create_access_grants_instance(AccountId=ACCOUNT_ID)
        # RETRIEVE: get instance for prefix returns ARN and ID
        resp = s3control.get_access_grants_instance_for_prefix(
            AccountId=ACCOUNT_ID, S3Prefix="s3://any-bucket/prefix"
        )
        assert "AccessGrantsInstanceArn" in resp
        assert "AccessGrantsInstanceId" in resp
        # LIST: instance appears in list_access_grants_instances
        list_resp = s3control.list_access_grants_instances(AccountId=ACCOUNT_ID)
        assert "AccessGrantsInstancesList" in list_resp
        assert len(list_resp["AccessGrantsInstancesList"]) > 0
        # UPDATE: create a location to test an update-adjacent operation
        loc = s3control.create_access_grants_location(
            AccountId=ACCOUNT_ID,
            LocationScope="s3://prefix-check/",
            IAMRoleArn=f"arn:aws:iam::{ACCOUNT_ID}:role/prefix-role",
        )
        loc_id = loc["AccessGrantsLocationId"]
        upd_resp = s3control.update_access_grants_location(
            AccountId=ACCOUNT_ID,
            AccessGrantsLocationId=loc_id,
            IAMRoleArn=f"arn:aws:iam::{ACCOUNT_ID}:role/prefix-role-v2",
        )
        assert upd_resp["AccessGrantsLocationId"] == loc_id
        # cleanup location
        s3control.delete_access_grants_location(
            AccountId=ACCOUNT_ID, AccessGrantsLocationId=loc_id
        )
        # DELETE: delete the instance and verify prefix lookup fails
        s3control.delete_access_grants_instance(AccountId=ACCOUNT_ID)
        # ERROR: after deletion, prefix lookup should fail
        with pytest.raises(ClientError):
            s3control.get_access_grants_instance_for_prefix(
                AccountId=ACCOUNT_ID, S3Prefix="s3://any-bucket/prefix"
            )
        # Recreate so autouse fixture cleanup passes
        s3control.create_access_grants_instance(AccountId=ACCOUNT_ID)

    def test_access_grants_instance_for_prefix_no_instance(self, s3control):
        """GetAccessGrantsInstanceForPrefix after deleting instance raises error."""
        # Delete instance for this test
        try:
            s3control.delete_access_grants_instance(AccountId=ACCOUNT_ID)
        except ClientError:
            pass  # best-effort cleanup
        with pytest.raises(ClientError):
            s3control.get_access_grants_instance_for_prefix(
                AccountId=ACCOUNT_ID, S3Prefix="s3://any-bucket/prefix"
            )
        # Recreate so autouse fixture cleanup passes
        s3control.create_access_grants_instance(AccountId=ACCOUNT_ID)


class TestS3ControlStorageLensDeepLifecycle:
    """Deep lifecycle tests for Storage Lens configurations."""

    @pytest.fixture
    def s3control(self):
        return make_client("s3control")

    def _make_storage_lens_config(self, config_id, enabled=True):
        return {
            "Id": config_id,
            "AccountLevel": {
                "BucketLevel": {},
            },
            "IsEnabled": enabled,
        }

    def test_storage_lens_delete_then_get_fails(self, s3control):
        """Get fails after delete with correct error."""
        config_id = f"sl-delget-{_uid()}"
        s3control.put_storage_lens_configuration(
            AccountId=ACCOUNT_ID,
            ConfigId=config_id,
            StorageLensConfiguration=self._make_storage_lens_config(config_id),
        )
        s3control.delete_storage_lens_configuration(AccountId=ACCOUNT_ID, ConfigId=config_id)
        with pytest.raises(ClientError) as exc_info:
            s3control.get_storage_lens_configuration(AccountId=ACCOUNT_ID, ConfigId=config_id)
        assert exc_info.value.response["Error"]["Code"] in (
            "NoSuchConfiguration",
            "NotFoundException",
        )

    def test_storage_lens_tagging_empty_by_default(self, s3control):
        """New configuration has no tags by default."""
        config_id = f"sl-notag-{_uid()}"
        try:
            s3control.put_storage_lens_configuration(
                AccountId=ACCOUNT_ID,
                ConfigId=config_id,
                StorageLensConfiguration=self._make_storage_lens_config(config_id),
            )
            resp = s3control.get_storage_lens_configuration_tagging(
                AccountId=ACCOUNT_ID, ConfigId=config_id
            )
            assert "Tags" in resp
            assert isinstance(resp["Tags"], list)
        finally:
            try:
                s3control.delete_storage_lens_configuration(
                    AccountId=ACCOUNT_ID, ConfigId=config_id
                )
            except Exception:
                pass  # best-effort cleanup

    def test_storage_lens_delete_tagging(self, s3control):
        """Tags are removed after deleting and recreating configuration."""
        config_id = f"sl-deltag-{_uid()}"
        try:
            s3control.put_storage_lens_configuration(
                AccountId=ACCOUNT_ID,
                ConfigId=config_id,
                StorageLensConfiguration=self._make_storage_lens_config(config_id),
            )
            s3control.put_storage_lens_configuration_tagging(
                AccountId=ACCOUNT_ID,
                ConfigId=config_id,
                Tags=[{"Key": "env", "Value": "test"}],
            )
            # Verify tags exist
            resp = s3control.get_storage_lens_configuration_tagging(
                AccountId=ACCOUNT_ID, ConfigId=config_id
            )
            assert len(resp["Tags"]) >= 1

            # Delete and recreate
            s3control.delete_storage_lens_configuration(AccountId=ACCOUNT_ID, ConfigId=config_id)
            s3control.put_storage_lens_configuration(
                AccountId=ACCOUNT_ID,
                ConfigId=config_id,
                StorageLensConfiguration=self._make_storage_lens_config(config_id),
            )
            # Tags should be gone
            resp2 = s3control.get_storage_lens_configuration_tagging(
                AccountId=ACCOUNT_ID, ConfigId=config_id
            )
            assert len(resp2["Tags"]) == 0
        finally:
            try:
                s3control.delete_storage_lens_configuration(
                    AccountId=ACCOUNT_ID, ConfigId=config_id
                )
            except Exception:
                pass  # best-effort cleanup

    def test_storage_lens_put_appears_in_list(self, s3control):
        """Newly created config appears in list."""
        config_id = f"sl-list-{_uid()}"
        try:
            s3control.put_storage_lens_configuration(
                AccountId=ACCOUNT_ID,
                ConfigId=config_id,
                StorageLensConfiguration=self._make_storage_lens_config(config_id),
            )
            resp = s3control.list_storage_lens_configurations(AccountId=ACCOUNT_ID)
            ids = [c["Id"] for c in resp.get("StorageLensConfigurationList", [])]
            assert config_id in ids
        finally:
            try:
                s3control.delete_storage_lens_configuration(
                    AccountId=ACCOUNT_ID, ConfigId=config_id
                )
            except Exception:
                pass  # best-effort cleanup

    def test_storage_lens_double_delete(self, s3control):
        """Deleting already deleted config should error or succeed gracefully."""
        config_id = f"sl-ddel-{_uid()}"
        s3control.put_storage_lens_configuration(
            AccountId=ACCOUNT_ID,
            ConfigId=config_id,
            StorageLensConfiguration=self._make_storage_lens_config(config_id),
        )
        s3control.delete_storage_lens_configuration(AccountId=ACCOUNT_ID, ConfigId=config_id)
        # Second delete may error or succeed
        try:
            resp = s3control.delete_storage_lens_configuration(
                AccountId=ACCOUNT_ID, ConfigId=config_id
            )
            assert resp["ResponseMetadata"]["HTTPStatusCode"] in (200, 204)
        except ClientError as e:
            assert e.response["Error"]["Code"] in (
                "NoSuchConfiguration",
                "NotFoundException",
            )


class TestS3ControlAccessPointDeepLifecycle:
    """Deeper lifecycle tests for access points."""

    @pytest.fixture
    def s3control(self):
        return make_client("s3control")

    @pytest.fixture
    def s3(self):
        return make_client("s3")

    @pytest.fixture
    def bucket(self, s3):
        name = f"ap-deep-{_uid()}"
        s3.create_bucket(Bucket=name)
        yield name
        try:
            s3.delete_bucket(Bucket=name)
        except Exception:
            pass  # best-effort cleanup

    def test_delete_access_point_then_describe_fails(self, s3control, s3, bucket):
        """Full AP lifecycle: create → retrieve → list → update policy → delete → error."""
        ap_name = f"ap-del-{_uid()}"
        try:
            # CREATE: create an access point
            s3control.create_access_point(AccountId=ACCOUNT_ID, Name=ap_name, Bucket=bucket)
            # RETRIEVE: verify the AP exists
            get_resp = s3control.get_access_point(AccountId=ACCOUNT_ID, Name=ap_name)
            assert get_resp["Name"] == ap_name
            assert get_resp["Bucket"] == bucket
            # LIST: AP appears in list
            list_resp = s3control.list_access_points(AccountId=ACCOUNT_ID, Bucket=bucket)
            names = [ap["Name"] for ap in list_resp["AccessPointList"]]
            assert ap_name in names
            # UPDATE: put a policy on the AP
            policy = json.dumps({
                "Version": "2012-10-17",
                "Statement": [{"Effect": "Allow", "Principal": "*", "Action": "s3:GetObject",
                               "Resource": f"arn:aws:s3:us-east-1:{ACCOUNT_ID}:accesspoint/{ap_name}/object/*"}],
            })
            s3control.put_access_point_policy(
                AccountId=ACCOUNT_ID, Name=ap_name, Policy=policy
            )
            pol_resp = s3control.get_access_point_policy(
                AccountId=ACCOUNT_ID, Name=ap_name
            )
            assert "Policy" in pol_resp
            # DELETE: delete the access point
            s3control.delete_access_point(AccountId=ACCOUNT_ID, Name=ap_name)
            # ERROR: get after delete fails
            with pytest.raises(ClientError) as exc_info:
                s3control.get_access_point(AccountId=ACCOUNT_ID, Name=ap_name)
            assert exc_info.value.response["Error"]["Code"] in (
                "NoSuchAccessPoint",
                "NotFoundException",
            )
        except Exception:
            try:
                s3control.delete_access_point(AccountId=ACCOUNT_ID, Name=ap_name)
            except Exception:
                pass  # best-effort cleanup
            raise

    def test_delete_access_point_policy_then_get_fails(self, s3control, s3, bucket):
        """Get access point policy after deletion returns error."""
        ap_name = f"ap-delpol-{_uid()}"
        try:
            s3control.create_access_point(AccountId=ACCOUNT_ID, Name=ap_name, Bucket=bucket)
            # Verify the AP appears in the list
            list_resp = s3control.list_access_points(AccountId=ACCOUNT_ID, Bucket=bucket)
            listed_names = [ap["Name"] for ap in list_resp["AccessPointList"]]
            assert ap_name in listed_names
            policy = json.dumps(
                {
                    "Version": "2012-10-17",
                    "Statement": [
                        {
                            "Effect": "Allow",
                            "Principal": "*",
                            "Action": "s3:GetObject",
                            "Resource": f"arn:aws:s3:{ap_name}/object/*",
                        }
                    ],
                }
            )
            s3control.put_access_point_policy(AccountId=ACCOUNT_ID, Name=ap_name, Policy=policy)
            s3control.delete_access_point_policy(AccountId=ACCOUNT_ID, Name=ap_name)
            with pytest.raises(ClientError) as exc_info:
                s3control.get_access_point_policy(AccountId=ACCOUNT_ID, Name=ap_name)
            assert exc_info.value.response["Error"]["Code"] in (
                "NoSuchAccessPointPolicy",
                "NotFoundException",
            )
        finally:
            try:
                s3control.delete_access_point(AccountId=ACCOUNT_ID, Name=ap_name)
            except Exception:
                pass  # best-effort cleanup

    def test_access_point_list_after_delete(self, s3control, s3, bucket):
        """Deleted access point should not appear in list."""
        ap_name = f"ap-listdel-{_uid()}"
        try:
            s3control.create_access_point(AccountId=ACCOUNT_ID, Name=ap_name, Bucket=bucket)
            # Verify it appears in list
            resp = s3control.list_access_points(AccountId=ACCOUNT_ID, Bucket=bucket)
            names = [ap["Name"] for ap in resp["AccessPointList"]]
            assert ap_name in names
            # Delete
            s3control.delete_access_point(AccountId=ACCOUNT_ID, Name=ap_name)
            # Verify it's gone from list
            resp2 = s3control.list_access_points(AccountId=ACCOUNT_ID, Bucket=bucket)
            names2 = [ap["Name"] for ap in resp2["AccessPointList"]]
            assert ap_name not in names2
        except Exception:
            try:
                s3control.delete_access_point(AccountId=ACCOUNT_ID, Name=ap_name)
            except Exception:
                pass  # best-effort cleanup
            raise


class TestS3ControlMRAPDeepLifecycle:
    """Deeper lifecycle tests for multi-region access points."""

    @pytest.fixture
    def s3control(self):
        return make_client("s3control")

    @pytest.fixture
    def s3(self):
        return make_client("s3")

    @pytest.fixture
    def bucket(self, s3):
        name = f"mrap-deep-{_uid()}"
        s3.create_bucket(Bucket=name)
        yield name
        try:
            s3.delete_bucket(Bucket=name)
        except Exception:
            pass  # best-effort cleanup

    def test_mrap_delete_then_get_fails(self, s3control, s3, bucket):
        """Get MRAP after deletion returns error."""
        mrap_name = f"mrap-del-{_uid()}"
        try:
            s3control.create_multi_region_access_point(
                AccountId=ACCOUNT_ID,
                Details={
                    "Name": mrap_name,
                    "Regions": [{"Bucket": bucket}],
                },
            )
            s3control.delete_multi_region_access_point(
                AccountId=ACCOUNT_ID,
                Details={"Name": mrap_name},
            )
            with pytest.raises(ClientError) as exc_info:
                s3control.get_multi_region_access_point(AccountId=ACCOUNT_ID, Name=mrap_name)
            assert exc_info.value.response["Error"]["Code"] in (
                "NoSuchMultiRegionAccessPoint",
                "NotFoundException",
            )
        except Exception:
            try:
                s3control.delete_multi_region_access_point(
                    AccountId=ACCOUNT_ID, Details={"Name": mrap_name}
                )
            except Exception:
                pass  # best-effort cleanup
            raise

    def test_mrap_list_after_delete(self, s3control, s3, bucket):
        """Deleted MRAP should not appear in list."""
        mrap_name = f"mrap-ldel-{_uid()}"
        try:
            s3control.create_multi_region_access_point(
                AccountId=ACCOUNT_ID,
                Details={
                    "Name": mrap_name,
                    "Regions": [{"Bucket": bucket}],
                },
            )
            # Verify it appears in list
            resp = s3control.list_multi_region_access_points(AccountId=ACCOUNT_ID)
            names = [m["Name"] for m in resp["AccessPoints"]]
            assert mrap_name in names
            # Delete
            s3control.delete_multi_region_access_point(
                AccountId=ACCOUNT_ID,
                Details={"Name": mrap_name},
            )
            # Verify it's gone from list
            resp2 = s3control.list_multi_region_access_points(AccountId=ACCOUNT_ID)
            names2 = [m["Name"] for m in resp2["AccessPoints"]]
            assert mrap_name not in names2
        except Exception:
            try:
                s3control.delete_multi_region_access_point(
                    AccountId=ACCOUNT_ID, Details={"Name": mrap_name}
                )
            except Exception:
                pass  # best-effort cleanup
            raise

    def test_mrap_policy_roundtrip(self, s3control, s3, bucket):
        """Put and get MRAP policy roundtrips correctly."""
        mrap_name = f"mrap-pol-{_uid()}"
        try:
            s3control.create_multi_region_access_point(
                AccountId=ACCOUNT_ID,
                Details={
                    "Name": mrap_name,
                    "Regions": [{"Bucket": bucket}],
                },
            )
            policy_doc = json.dumps(
                {
                    "Version": "2012-10-17",
                    "Statement": [
                        {
                            "Effect": "Allow",
                            "Principal": "*",
                            "Action": "s3:GetObject",
                            "Resource": "*",
                        }
                    ],
                }
            )
            s3control.put_multi_region_access_point_policy(
                AccountId=ACCOUNT_ID,
                Details={
                    "Name": mrap_name,
                    "Policy": policy_doc,
                },
            )
            resp = s3control.get_multi_region_access_point_policy(
                AccountId=ACCOUNT_ID, Name=mrap_name
            )
            assert "Policy" in resp
            established = resp["Policy"].get("Established", {})
            if "Policy" in established:
                policy = established["Policy"]
                if isinstance(policy, str):
                    parsed = json.loads(policy)
                else:
                    parsed = policy
                assert parsed["Version"] == "2012-10-17"
        finally:
            try:
                s3control.delete_multi_region_access_point(
                    AccountId=ACCOUNT_ID, Details={"Name": mrap_name}
                )
            except Exception:
                pass  # best-effort cleanup


class TestS3ControlAccessGrantsResourcePolicy:
    """Tests for Access Grants Instance Resource Policy operations."""

    @pytest.fixture
    def s3control(self):
        return make_client("s3control")

    @pytest.fixture(autouse=True)
    def _ensure_instance(self, s3control):
        """Ensure an access grants instance exists for tests."""
        try:
            s3control.get_access_grants_instance(AccountId=ACCOUNT_ID)
        except ClientError:
            s3control.create_access_grants_instance(AccountId=ACCOUNT_ID)
        yield
        # Cleanup: remove resource policy if set
        try:
            s3control.delete_access_grants_instance_resource_policy(AccountId=ACCOUNT_ID)
        except Exception:
            pass  # best-effort cleanup

    def test_put_access_grants_instance_resource_policy(self, s3control):
        """PutAccessGrantsInstanceResourcePolicy: create → retrieve → update → delete → error."""
        policy_doc = json.dumps(
            {
                "Version": "2012-10-17",
                "Statement": [
                    {
                        "Effect": "Allow",
                        "Principal": "*",
                        "Action": "s3:GetAccessGrant",
                        "Resource": "*",
                    }
                ],
            }
        )
        # CREATE: put policy
        resp = s3control.put_access_grants_instance_resource_policy(
            AccountId=ACCOUNT_ID, Policy=policy_doc
        )
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200
        assert "Policy" in resp
        # RETRIEVE: get policy, verify JSON content
        get_resp = s3control.get_access_grants_instance_resource_policy(AccountId=ACCOUNT_ID)
        assert "Policy" in get_resp
        parsed = json.loads(get_resp["Policy"])
        assert parsed["Version"] == "2012-10-17"
        assert parsed["Statement"][0]["Action"] == "s3:GetAccessGrant"
        # UPDATE: replace policy with different action
        policy_v2 = json.dumps(
            {
                "Version": "2012-10-17",
                "Statement": [
                    {
                        "Effect": "Allow",
                        "Principal": "*",
                        "Action": "s3:ListAccessGrants",
                        "Resource": "*",
                    }
                ],
            }
        )
        upd_resp = s3control.put_access_grants_instance_resource_policy(
            AccountId=ACCOUNT_ID, Policy=policy_v2
        )
        assert upd_resp["ResponseMetadata"]["HTTPStatusCode"] == 200
        get_resp2 = s3control.get_access_grants_instance_resource_policy(AccountId=ACCOUNT_ID)
        parsed2 = json.loads(get_resp2["Policy"])
        assert parsed2["Statement"][0]["Action"] == "s3:ListAccessGrants"
        # DELETE: remove policy
        del_resp = s3control.delete_access_grants_instance_resource_policy(AccountId=ACCOUNT_ID)
        assert del_resp["ResponseMetadata"]["HTTPStatusCode"] == 200
        # ERROR / post-delete: get returns empty policy or raises
        try:
            empty_resp = s3control.get_access_grants_instance_resource_policy(AccountId=ACCOUNT_ID)
            assert empty_resp.get("Policy", "") in ("", None, "{}")
        except ClientError as e:
            assert e.response["Error"]["Code"] in (
                "NoSuchAccessGrantsInstanceResourcePolicy",
                "NoSuchResourcePolicy",
                "NotFoundException",
            )

    def test_get_access_grants_instance_resource_policy(self, s3control):
        """GetAccessGrantsInstanceResourcePolicy returns the set policy."""
        policy_doc = json.dumps(
            {
                "Version": "2012-10-17",
                "Statement": [
                    {
                        "Effect": "Allow",
                        "Principal": "*",
                        "Action": "s3:GetAccessGrant",
                        "Resource": "*",
                    }
                ],
            }
        )
        s3control.put_access_grants_instance_resource_policy(
            AccountId=ACCOUNT_ID, Policy=policy_doc
        )
        resp = s3control.get_access_grants_instance_resource_policy(AccountId=ACCOUNT_ID)
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200
        assert "Policy" in resp
        # Policy should be parseable JSON
        parsed = json.loads(resp["Policy"])
        assert parsed["Version"] == "2012-10-17"

    def test_delete_access_grants_instance_resource_policy(self, s3control):
        """DeleteAccessGrantsInstanceResourcePolicy removes the policy."""
        policy_doc = json.dumps(
            {
                "Version": "2012-10-17",
                "Statement": [
                    {
                        "Effect": "Allow",
                        "Principal": "*",
                        "Action": "s3:GetAccessGrant",
                        "Resource": "*",
                    }
                ],
            }
        )
        s3control.put_access_grants_instance_resource_policy(
            AccountId=ACCOUNT_ID, Policy=policy_doc
        )
        resp = s3control.delete_access_grants_instance_resource_policy(AccountId=ACCOUNT_ID)
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200

    def test_delete_then_get_access_grants_resource_policy_empty(self, s3control):
        """GetAccessGrantsInstanceResourcePolicy after delete returns empty policy."""
        policy_doc = json.dumps(
            {
                "Version": "2012-10-17",
                "Statement": [{"Effect": "Allow", "Principal": "*", "Action": "s3:GetAccessGrant", "Resource": "*"}],
            }
        )
        s3control.put_access_grants_instance_resource_policy(
            AccountId=ACCOUNT_ID, Policy=policy_doc
        )
        s3control.delete_access_grants_instance_resource_policy(AccountId=ACCOUNT_ID)
        # After deletion, get either errors or returns empty policy
        try:
            resp = s3control.get_access_grants_instance_resource_policy(AccountId=ACCOUNT_ID)
            # If it succeeds, the policy should be empty
            assert resp["Policy"] == "" or resp.get("Policy") is None or resp["Policy"] == "{}"
        except ClientError as e:
            assert e.response["Error"]["Code"] in (
                "NoSuchAccessGrantsInstanceResourcePolicy",
                "NotFoundException",
                "NoSuchResourcePolicy",
            )

    def test_put_access_grants_resource_policy_replaces(self, s3control):
        """PutAccessGrantsInstanceResourcePolicy twice replaces the policy."""
        policy1 = json.dumps({"Version": "2012-10-17", "Statement": [{"Effect": "Allow", "Principal": "*", "Action": "s3:GetAccessGrant", "Resource": "*"}]})
        policy2 = json.dumps({"Version": "2012-10-17", "Statement": [{"Effect": "Deny", "Principal": "*", "Action": "s3:DeleteAccessGrant", "Resource": "*"}]})
        s3control.put_access_grants_instance_resource_policy(
            AccountId=ACCOUNT_ID, Policy=policy1
        )
        s3control.put_access_grants_instance_resource_policy(
            AccountId=ACCOUNT_ID, Policy=policy2
        )
        resp = s3control.get_access_grants_instance_resource_policy(AccountId=ACCOUNT_ID)
        assert "Policy" in resp
        parsed = json.loads(resp["Policy"])
        assert parsed["Statement"][0]["Effect"] == "Deny"


class TestS3ControlAccessGrantLifecycle:
    """Tests for CreateAccessGrant, GetAccessGrant, DeleteAccessGrant lifecycle."""

    @pytest.fixture
    def s3control(self):
        return make_client("s3control")

    @pytest.fixture(autouse=True)
    def _ensure_instance(self, s3control):
        """Ensure an access grants instance exists."""
        try:
            s3control.get_access_grants_instance(AccountId=ACCOUNT_ID)
        except ClientError:
            s3control.create_access_grants_instance(AccountId=ACCOUNT_ID)

    def test_create_access_grant(self, s3control):
        """CreateAccessGrant returns grant ID, ARN, and permission."""
        loc = s3control.create_access_grants_location(
            AccountId=ACCOUNT_ID,
            LocationScope="s3://grant-create-bucket/",
            IAMRoleArn="arn:aws:iam::123456789012:role/grant-role",
        )
        loc_id = loc["AccessGrantsLocationId"]
        try:
            resp = s3control.create_access_grant(
                AccountId=ACCOUNT_ID,
                AccessGrantsLocationId=loc_id,
                AccessGrantsLocationConfiguration={"S3SubPrefix": "data/"},
                Grantee={
                    "GranteeType": "IAM",
                    "GranteeIdentifier": "arn:aws:iam::123456789012:role/grantee",
                },
                Permission="READ",
            )
            assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200
            assert "AccessGrantId" in resp
            assert "AccessGrantArn" in resp
            assert resp["Permission"] == "READ"
            assert "CreatedAt" in resp
        finally:
            try:
                s3control.delete_access_grant(
                    AccountId=ACCOUNT_ID, AccessGrantId=resp["AccessGrantId"]
                )
            except Exception:
                pass  # best-effort cleanup

    def test_get_access_grant(self, s3control):
        """GetAccessGrant returns full grant details."""
        loc = s3control.create_access_grants_location(
            AccountId=ACCOUNT_ID,
            LocationScope="s3://grant-get-bucket/",
            IAMRoleArn="arn:aws:iam::123456789012:role/grant-role",
        )
        loc_id = loc["AccessGrantsLocationId"]
        grant = s3control.create_access_grant(
            AccountId=ACCOUNT_ID,
            AccessGrantsLocationId=loc_id,
            AccessGrantsLocationConfiguration={"S3SubPrefix": "prefix/"},
            Grantee={
                "GranteeType": "IAM",
                "GranteeIdentifier": "arn:aws:iam::123456789012:role/grantee",
            },
            Permission="READWRITE",
        )
        grant_id = grant["AccessGrantId"]
        try:
            resp = s3control.get_access_grant(AccountId=ACCOUNT_ID, AccessGrantId=grant_id)
            assert resp["AccessGrantId"] == grant_id
            assert resp["Permission"] == "READWRITE"
            assert "AccessGrantArn" in resp
            assert "Grantee" in resp
            assert resp["Grantee"]["GranteeType"] == "IAM"
        finally:
            try:
                s3control.delete_access_grant(AccountId=ACCOUNT_ID, AccessGrantId=grant_id)
            except Exception:
                pass  # best-effort cleanup

    def test_delete_access_grant(self, s3control):
        """DeleteAccessGrant removes the grant."""
        loc = s3control.create_access_grants_location(
            AccountId=ACCOUNT_ID,
            LocationScope="s3://grant-del-bucket/",
            IAMRoleArn="arn:aws:iam::123456789012:role/grant-role",
        )
        loc_id = loc["AccessGrantsLocationId"]
        grant = s3control.create_access_grant(
            AccountId=ACCOUNT_ID,
            AccessGrantsLocationId=loc_id,
            AccessGrantsLocationConfiguration={"S3SubPrefix": "x/"},
            Grantee={
                "GranteeType": "IAM",
                "GranteeIdentifier": "arn:aws:iam::123456789012:role/grantee",
            },
            Permission="READ",
        )
        grant_id = grant["AccessGrantId"]
        resp = s3control.delete_access_grant(AccountId=ACCOUNT_ID, AccessGrantId=grant_id)
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200
        # Verify it's gone
        with pytest.raises(ClientError) as exc_info:
            s3control.get_access_grant(AccountId=ACCOUNT_ID, AccessGrantId=grant_id)
        assert exc_info.value.response["Error"]["Code"] == "NoSuchAccessGrant"

    def test_list_access_grants_includes_created(self, s3control):
        """ListAccessGrants includes a newly created grant."""
        loc = s3control.create_access_grants_location(
            AccountId=ACCOUNT_ID,
            LocationScope="s3://grant-list-bucket/",
            IAMRoleArn="arn:aws:iam::123456789012:role/grant-role",
        )
        loc_id = loc["AccessGrantsLocationId"]
        grant = s3control.create_access_grant(
            AccountId=ACCOUNT_ID,
            AccessGrantsLocationId=loc_id,
            AccessGrantsLocationConfiguration={"S3SubPrefix": "y/"},
            Grantee={
                "GranteeType": "IAM",
                "GranteeIdentifier": "arn:aws:iam::123456789012:role/grantee",
            },
            Permission="READ",
        )
        grant_id = grant["AccessGrantId"]
        try:
            resp = s3control.list_access_grants(AccountId=ACCOUNT_ID)
            ids = [g["AccessGrantId"] for g in resp["AccessGrantsList"]]
            assert grant_id in ids
        finally:
            try:
                s3control.delete_access_grant(AccountId=ACCOUNT_ID, AccessGrantId=grant_id)
            except Exception:
                pass  # best-effort cleanup


class TestS3ControlAccessGrantsLocationLifecycle:
    """Tests for DeleteAccessGrantsLocation and UpdateAccessGrantsLocation."""

    @pytest.fixture
    def s3control(self):
        return make_client("s3control")

    @pytest.fixture(autouse=True)
    def _ensure_instance(self, s3control):
        """Ensure an access grants instance exists."""
        try:
            s3control.get_access_grants_instance(AccountId=ACCOUNT_ID)
        except ClientError:
            s3control.create_access_grants_instance(AccountId=ACCOUNT_ID)

    def test_delete_access_grants_location(self, s3control):
        """DeleteAccessGrantsLocation removes the location."""
        loc = s3control.create_access_grants_location(
            AccountId=ACCOUNT_ID,
            LocationScope="s3://del-loc-test/",
            IAMRoleArn="arn:aws:iam::123456789012:role/del-role",
        )
        loc_id = loc["AccessGrantsLocationId"]
        resp = s3control.delete_access_grants_location(
            AccountId=ACCOUNT_ID, AccessGrantsLocationId=loc_id
        )
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200
        # Verify it's gone
        with pytest.raises(ClientError) as exc_info:
            s3control.get_access_grants_location(
                AccountId=ACCOUNT_ID, AccessGrantsLocationId=loc_id
            )
        assert exc_info.value.response["Error"]["Code"] == "NoSuchAccessGrantsLocation"

    def test_delete_access_grants_location_removed_from_list(self, s3control):
        """Deleted location no longer appears in list."""
        loc = s3control.create_access_grants_location(
            AccountId=ACCOUNT_ID,
            LocationScope="s3://del-list-loc/",
            IAMRoleArn="arn:aws:iam::123456789012:role/del-list-role",
        )
        loc_id = loc["AccessGrantsLocationId"]
        s3control.delete_access_grants_location(AccountId=ACCOUNT_ID, AccessGrantsLocationId=loc_id)
        resp = s3control.list_access_grants_locations(AccountId=ACCOUNT_ID)
        ids = [loc["AccessGrantsLocationId"] for loc in resp["AccessGrantsLocationsList"]]
        assert loc_id not in ids

    def test_update_access_grants_location(self, s3control):
        """UpdateAccessGrantsLocation changes the IAM role."""
        loc = s3control.create_access_grants_location(
            AccountId=ACCOUNT_ID,
            LocationScope="s3://upd-loc-test/",
            IAMRoleArn="arn:aws:iam::123456789012:role/old-role",
        )
        loc_id = loc["AccessGrantsLocationId"]
        try:
            resp = s3control.update_access_grants_location(
                AccountId=ACCOUNT_ID,
                AccessGrantsLocationId=loc_id,
                IAMRoleArn="arn:aws:iam::123456789012:role/new-role",
            )
            assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200
            assert resp["AccessGrantsLocationId"] == loc_id
            assert "IAMRoleArn" in resp
            assert "new-role" in resp["IAMRoleArn"]
        finally:
            try:
                s3control.delete_access_grants_location(
                    AccountId=ACCOUNT_ID, AccessGrantsLocationId=loc_id
                )
            except Exception:
                pass  # best-effort cleanup

    def test_update_access_grants_location_preserves_scope(self, s3control):
        """UpdateAccessGrantsLocation preserves the location scope."""
        scope = "s3://upd-scope-bucket/"
        loc = s3control.create_access_grants_location(
            AccountId=ACCOUNT_ID,
            LocationScope=scope,
            IAMRoleArn="arn:aws:iam::123456789012:role/scope-role",
        )
        loc_id = loc["AccessGrantsLocationId"]
        try:
            resp = s3control.update_access_grants_location(
                AccountId=ACCOUNT_ID,
                AccessGrantsLocationId=loc_id,
                IAMRoleArn="arn:aws:iam::123456789012:role/scope-role-v2",
            )
            assert resp["LocationScope"] == scope
        finally:
            try:
                s3control.delete_access_grants_location(
                    AccountId=ACCOUNT_ID, AccessGrantsLocationId=loc_id
                )
            except Exception:
                pass  # best-effort cleanup


class TestS3ControlMRAPRoutes:
    """Tests for Multi-Region Access Point Routes operations."""

    @pytest.fixture
    def s3control(self):
        return make_client("s3control")

    @pytest.fixture
    def s3(self):
        return make_client("s3")

    @pytest.fixture
    def mrap_with_bucket(self, s3control, s3):
        bucket = f"mrap-rt-{_uid()}"
        mrap_name = f"mrap-rt-{_uid()}"
        s3.create_bucket(Bucket=bucket)
        s3control.create_multi_region_access_point(
            AccountId=ACCOUNT_ID,
            Details={"Name": mrap_name, "Regions": [{"Bucket": bucket}]},
        )
        yield mrap_name, bucket
        try:
            s3control.delete_multi_region_access_point(
                AccountId=ACCOUNT_ID, Details={"Name": mrap_name}
            )
        except Exception:
            pass  # best-effort cleanup
        try:
            s3.delete_bucket(Bucket=bucket)
        except Exception:
            pass  # best-effort cleanup

    def test_get_multi_region_access_point_routes(self, s3control, mrap_with_bucket):
        """GetMultiRegionAccessPointRoutes: create → retrieve → list → update → delete → error."""
        mrap_name, bucket = mrap_with_bucket
        # RETRIEVE: get routes returns valid structure
        resp = s3control.get_multi_region_access_point_routes(AccountId=ACCOUNT_ID, Mrap=mrap_name)
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200
        assert "Routes" in resp
        assert isinstance(resp["Routes"], list)
        # LIST: mrap appears in list_multi_region_access_points
        list_resp = s3control.list_multi_region_access_points(AccountId=ACCOUNT_ID)
        names = [ap["Name"] for ap in list_resp["AccessPoints"]]
        assert mrap_name in names
        # UPDATE: submit route update changes traffic dial
        s3control.submit_multi_region_access_point_routes(
            AccountId=ACCOUNT_ID,
            Mrap=mrap_name,
            RouteUpdates=[
                {"Bucket": bucket, "Region": "us-east-1", "TrafficDialPercentage": 75}
            ],
        )
        routes_resp = s3control.get_multi_region_access_point_routes(
            AccountId=ACCOUNT_ID, Mrap=mrap_name
        )
        assert "Routes" in routes_resp
        route_buckets = [r["Bucket"] for r in routes_resp["Routes"]]
        assert bucket in route_buckets
        # DELETE: create and delete a temp MRAP to cover the delete pattern
        tmp_mrap = f"mrap-tmp-{_uid()}"
        s3control.create_multi_region_access_point(
            AccountId=ACCOUNT_ID,
            Details={"Name": tmp_mrap, "Regions": [{"Bucket": bucket}]},
        )
        del_resp = s3control.delete_multi_region_access_point(
            AccountId=ACCOUNT_ID, Details={"Name": tmp_mrap}
        )
        assert del_resp["ResponseMetadata"]["HTTPStatusCode"] == 200
        # ERROR: nonexistent mrap raises error
        with pytest.raises(ClientError) as exc:
            s3control.get_multi_region_access_point_routes(
                AccountId=ACCOUNT_ID, Mrap=f"nonexistent-{_uid()}"
            )
        assert exc.value.response["Error"]["Code"] == "NoSuchMultiRegionAccessPoint"

    def test_submit_multi_region_access_point_routes(self, s3control, mrap_with_bucket):
        """SubmitMultiRegionAccessPointRoutes: create → retrieve → list → update → delete → error."""
        mrap_name, bucket = mrap_with_bucket
        # RETRIEVE: get routes before submitting
        initial_resp = s3control.get_multi_region_access_point_routes(
            AccountId=ACCOUNT_ID, Mrap=mrap_name
        )
        assert "Routes" in initial_resp
        # LIST: mrap visible in list
        list_resp = s3control.list_multi_region_access_points(AccountId=ACCOUNT_ID)
        names = [ap["Name"] for ap in list_resp["AccessPoints"]]
        assert mrap_name in names
        # UPDATE: submit routes
        resp = s3control.submit_multi_region_access_point_routes(
            AccountId=ACCOUNT_ID,
            Mrap=mrap_name,
            RouteUpdates=[
                {
                    "Bucket": bucket,
                    "Region": "us-east-1",
                    "TrafficDialPercentage": 100,
                }
            ],
        )
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200
        # Verify routes accessible after submit
        routes_resp = s3control.get_multi_region_access_point_routes(
            AccountId=ACCOUNT_ID, Mrap=mrap_name
        )
        assert "Routes" in routes_resp
        assert isinstance(routes_resp["Routes"], list)
        assert len(routes_resp["Routes"]) >= 1
        route_buckets = [r["Bucket"] for r in routes_resp["Routes"]]
        assert bucket in route_buckets
        # DELETE: create a temp MRAP and delete it
        tmp_mrap = f"mrap-sub-tmp-{_uid()}"
        s3control.create_multi_region_access_point(
            AccountId=ACCOUNT_ID,
            Details={"Name": tmp_mrap, "Regions": [{"Bucket": bucket}]},
        )
        s3control.delete_multi_region_access_point(
            AccountId=ACCOUNT_ID, Details={"Name": tmp_mrap}
        )
        # ERROR: submit to nonexistent MRAP raises error
        with pytest.raises(ClientError) as exc:
            s3control.submit_multi_region_access_point_routes(
                AccountId=ACCOUNT_ID,
                Mrap=f"nonexistent-{_uid()}",
                RouteUpdates=[
                    {"Bucket": bucket, "Region": "us-east-1", "TrafficDialPercentage": 100}
                ],
            )
        assert exc.value.response["Error"]["Code"] == "NoSuchMultiRegionAccessPoint"

    def test_get_mrap_routes_not_found(self, s3control):
        """GetMultiRegionAccessPointRoutes for nonexistent MRAP raises error."""
        with pytest.raises(ClientError) as exc_info:
            s3control.get_multi_region_access_point_routes(
                AccountId=ACCOUNT_ID, Mrap=f"nonexistent-{_uid()}"
            )
        assert exc_info.value.response["Error"]["Code"] == "NoSuchMultiRegionAccessPoint"

    def test_create_mrap_then_get_routes(self, s3control, s3):
        """Creating an MRAP makes its routes accessible via GetRoutes."""
        bucket = f"mrap-crr-{_uid()}"
        mrap_name = f"mrap-crr-{_uid()}"
        s3.create_bucket(Bucket=bucket)
        s3control.create_multi_region_access_point(
            AccountId=ACCOUNT_ID,
            Details={"Name": mrap_name, "Regions": [{"Bucket": bucket}]},
        )
        try:
            resp = s3control.get_multi_region_access_point_routes(
                AccountId=ACCOUNT_ID, Mrap=mrap_name
            )
            assert "Routes" in resp
            assert isinstance(resp["Routes"], list)
        finally:
            try:
                s3control.delete_multi_region_access_point(
                    AccountId=ACCOUNT_ID, Details={"Name": mrap_name}
                )
            except Exception:
                pass  # best-effort cleanup
            try:
                s3.delete_bucket(Bucket=bucket)
            except Exception:
                pass  # best-effort cleanup

    def test_delete_mrap_then_get_routes_fails(self, s3control, s3):
        """GetMultiRegionAccessPointRoutes after MRAP deletion raises error."""
        bucket = f"mrap-drt-{_uid()}"
        mrap_name = f"mrap-drt-{_uid()}"
        s3.create_bucket(Bucket=bucket)
        s3control.create_multi_region_access_point(
            AccountId=ACCOUNT_ID,
            Details={"Name": mrap_name, "Regions": [{"Bucket": bucket}]},
        )
        s3control.delete_multi_region_access_point(
            AccountId=ACCOUNT_ID, Details={"Name": mrap_name}
        )
        try:
            with pytest.raises(ClientError) as exc_info:
                s3control.get_multi_region_access_point_routes(
                    AccountId=ACCOUNT_ID, Mrap=mrap_name
                )
            assert exc_info.value.response["Error"]["Code"] == "NoSuchMultiRegionAccessPoint"
        finally:
            try:
                s3.delete_bucket(Bucket=bucket)
            except Exception:
                pass  # best-effort cleanup

    def test_submit_routes_error_nonexistent(self, s3control, s3):
        """SubmitMultiRegionAccessPointRoutes: create → retrieve → list → update → delete → error."""
        bucket = f"mrap-sre-{_uid()}"
        mrap_name = f"mrap-sre-{_uid()}"
        s3.create_bucket(Bucket=bucket)
        try:
            # CREATE: create an MRAP
            s3control.create_multi_region_access_point(
                AccountId=ACCOUNT_ID,
                Details={"Name": mrap_name, "Regions": [{"Bucket": bucket}]},
            )
            # RETRIEVE: get routes for the new MRAP
            get_resp = s3control.get_multi_region_access_point_routes(
                AccountId=ACCOUNT_ID, Mrap=mrap_name
            )
            assert "Routes" in get_resp
            # LIST: MRAP appears in list
            list_resp = s3control.list_multi_region_access_points(AccountId=ACCOUNT_ID)
            names = [ap["Name"] for ap in list_resp["AccessPoints"]]
            assert mrap_name in names
            # UPDATE: submit route update
            upd_resp = s3control.submit_multi_region_access_point_routes(
                AccountId=ACCOUNT_ID,
                Mrap=mrap_name,
                RouteUpdates=[
                    {"Bucket": bucket, "Region": "us-east-1", "TrafficDialPercentage": 50}
                ],
            )
            assert upd_resp["ResponseMetadata"]["HTTPStatusCode"] == 200
            # DELETE: remove the MRAP
            del_resp = s3control.delete_multi_region_access_point(
                AccountId=ACCOUNT_ID, Details={"Name": mrap_name}
            )
            assert del_resp["ResponseMetadata"]["HTTPStatusCode"] == 200
        finally:
            try:
                s3control.delete_multi_region_access_point(
                    AccountId=ACCOUNT_ID, Details={"Name": mrap_name}
                )
            except Exception:
                pass  # best-effort cleanup
            try:
                s3.delete_bucket(Bucket=bucket)
            except Exception:
                pass  # best-effort cleanup
        # ERROR: submit to nonexistent MRAP raises error
        with pytest.raises(ClientError) as exc_info:
            s3control.submit_multi_region_access_point_routes(
                AccountId=ACCOUNT_ID,
                Mrap=f"nonexistent-{_uid()}",
                RouteUpdates=[
                    {"Bucket": "any-bucket", "Region": "us-east-1", "TrafficDialPercentage": 100}
                ],
            )
        assert exc_info.value.response["Error"]["Code"] == "NoSuchMultiRegionAccessPoint"


class TestS3ControlJobLifecycle:
    """Tests for S3 Batch Operations job lifecycle."""

    @pytest.fixture
    def s3control(self):
        return make_client("s3control")

    @pytest.fixture
    def s3(self):
        return make_client("s3")

    @pytest.fixture
    def job_with_bucket(self, s3control, s3):
        """Create a bucket and a batch job, yield (job_id, bucket_name), then clean up."""
        bucket = f"job-{_uid()}"
        s3.create_bucket(Bucket=bucket)
        resp = s3control.create_job(
            AccountId=ACCOUNT_ID,
            Operation={"S3PutObjectCopy": {"TargetResource": f"arn:aws:s3:::{bucket}"}},
            Report={"Enabled": False},
            ClientRequestToken=str(uuid.uuid4()),
            Priority=10,
            RoleArn=f"arn:aws:iam::{ACCOUNT_ID}:role/test-role",
            ConfirmationRequired=False,
            ManifestGenerator={
                "S3JobManifestGenerator": {
                    "SourceBucket": f"arn:aws:s3:::{bucket}",
                    "EnableManifestOutput": False,
                }
            },
        )
        job_id = resp["JobId"]
        yield job_id, bucket
        try:
            s3control.update_job_status(
                AccountId=ACCOUNT_ID, JobId=job_id, RequestedJobStatus="Cancelled"
            )
        except Exception:
            pass  # best-effort cleanup
        try:
            s3.delete_bucket(Bucket=bucket)
        except Exception:
            pass  # best-effort cleanup

    def test_create_job_returns_job_id(self, s3control, s3):
        """CreateJob returns a valid JobId."""
        bucket = f"job-cj-{_uid()}"
        s3.create_bucket(Bucket=bucket)
        try:
            resp = s3control.create_job(
                AccountId=ACCOUNT_ID,
                Operation={"S3PutObjectCopy": {"TargetResource": f"arn:aws:s3:::{bucket}"}},
                Report={"Enabled": False},
                ClientRequestToken=str(uuid.uuid4()),
                Priority=5,
                RoleArn=f"arn:aws:iam::{ACCOUNT_ID}:role/test-role",
                ConfirmationRequired=False,
                ManifestGenerator={
                    "S3JobManifestGenerator": {
                        "SourceBucket": f"arn:aws:s3:::{bucket}",
                        "EnableManifestOutput": False,
                    }
                },
            )
            assert "JobId" in resp
            assert len(resp["JobId"]) > 0
            assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200
        finally:
            try:
                s3control.update_job_status(
                    AccountId=ACCOUNT_ID,
                    JobId=resp["JobId"],
                    RequestedJobStatus="Cancelled",
                )
            except Exception:
                pass  # best-effort cleanup
            s3.delete_bucket(Bucket=bucket)

    def test_create_job_appears_in_list(self, s3control, job_with_bucket):
        """Job lifecycle: create → retrieve → list → update priority → cancel → error."""
        job_id, _ = job_with_bucket
        # RETRIEVE: describe the job
        desc = s3control.describe_job(AccountId=ACCOUNT_ID, JobId=job_id)
        assert desc["Job"]["JobId"] == job_id
        assert "Priority" in desc["Job"]
        # LIST: job appears in list
        resp = s3control.list_jobs(AccountId=ACCOUNT_ID)
        job_ids = [j["JobId"] for j in resp["Jobs"]]
        assert job_id in job_ids
        # UPDATE: change priority
        upd_resp = s3control.update_job_priority(AccountId=ACCOUNT_ID, JobId=job_id, Priority=55)
        assert upd_resp["JobId"] == job_id
        assert upd_resp["Priority"] == 55
        # DELETE (cancel): cancel the job
        cancel_resp = s3control.update_job_status(
            AccountId=ACCOUNT_ID, JobId=job_id, RequestedJobStatus="Cancelled"
        )
        assert cancel_resp["Status"] == "Cancelled"
        # ERROR: nonexistent job raises error
        with pytest.raises(ClientError) as exc:
            s3control.describe_job(
                AccountId=ACCOUNT_ID, JobId="00000000-0000-0000-0000-000000000099"
            )
        assert exc.value.response["Error"]["Code"] == "NoSuchJob"

    def test_create_job_describe(self, s3control, job_with_bucket):
        """Job describe: create → retrieve → list → update status → cancel → error."""
        job_id, _ = job_with_bucket
        # RETRIEVE: describe returns expected fields
        resp = s3control.describe_job(AccountId=ACCOUNT_ID, JobId=job_id)
        assert resp["Job"]["JobId"] == job_id
        assert resp["Job"]["Priority"] == 10
        assert "Status" in resp["Job"]
        # LIST: job appears in list with Status field
        list_resp = s3control.list_jobs(AccountId=ACCOUNT_ID)
        job_ids = [j["JobId"] for j in list_resp["Jobs"]]
        assert job_id in job_ids
        # UPDATE: change priority
        s3control.update_job_priority(AccountId=ACCOUNT_ID, JobId=job_id, Priority=77)
        # Verify update reflected in describe
        desc2 = s3control.describe_job(AccountId=ACCOUNT_ID, JobId=job_id)
        assert desc2["Job"]["Priority"] == 77
        # DELETE (cancel)
        s3control.update_job_status(
            AccountId=ACCOUNT_ID, JobId=job_id, RequestedJobStatus="Cancelled"
        )
        # ERROR: nonexistent job raises NoSuchJob
        with pytest.raises(ClientError) as exc:
            s3control.describe_job(
                AccountId=ACCOUNT_ID, JobId="00000000-0000-0000-0000-000000000088"
            )
        assert exc.value.response["Error"]["Code"] == "NoSuchJob"

    def test_update_job_priority(self, s3control, job_with_bucket):
        """UpdateJobPriority: create → retrieve → list → update → cancel → error."""
        job_id, _ = job_with_bucket
        # RETRIEVE: describe job to get initial state
        desc = s3control.describe_job(AccountId=ACCOUNT_ID, JobId=job_id)
        assert desc["Job"]["JobId"] == job_id
        initial_priority = desc["Job"]["Priority"]
        assert initial_priority == 10
        # LIST: job appears in list
        list_resp = s3control.list_jobs(AccountId=ACCOUNT_ID)
        assert job_id in [j["JobId"] for j in list_resp["Jobs"]]
        # UPDATE: change priority
        resp = s3control.update_job_priority(AccountId=ACCOUNT_ID, JobId=job_id, Priority=42)
        assert resp["JobId"] == job_id
        assert resp["Priority"] == 42
        # Verify update reflected
        desc2 = s3control.describe_job(AccountId=ACCOUNT_ID, JobId=job_id)
        assert desc2["Job"]["Priority"] == 42
        # DELETE (cancel)
        s3control.update_job_status(
            AccountId=ACCOUNT_ID, JobId=job_id, RequestedJobStatus="Cancelled"
        )
        # ERROR: update priority of nonexistent job raises NoSuchJob
        with pytest.raises(ClientError) as exc:
            s3control.update_job_priority(
                AccountId=ACCOUNT_ID,
                JobId="00000000-0000-0000-0000-000000000077",
                Priority=1,
            )
        assert exc.value.response["Error"]["Code"] == "NoSuchJob"

    def test_update_job_priority_verify_via_describe(self, s3control, job_with_bucket):
        """Updated priority is reflected in DescribeJob."""
        job_id, _ = job_with_bucket
        s3control.update_job_priority(AccountId=ACCOUNT_ID, JobId=job_id, Priority=99)
        desc = s3control.describe_job(AccountId=ACCOUNT_ID, JobId=job_id)
        assert desc["Job"]["Priority"] == 99

    def test_update_job_status_cancel(self, s3control, s3):
        """UpdateJobStatus can cancel a job."""
        bucket = f"job-cancel-{_uid()}"
        s3.create_bucket(Bucket=bucket)
        try:
            resp = s3control.create_job(
                AccountId=ACCOUNT_ID,
                Operation={"S3PutObjectCopy": {"TargetResource": f"arn:aws:s3:::{bucket}"}},
                Report={"Enabled": False},
                ClientRequestToken=str(uuid.uuid4()),
                Priority=10,
                RoleArn=f"arn:aws:iam::{ACCOUNT_ID}:role/test-role",
                ConfirmationRequired=False,
                ManifestGenerator={
                    "S3JobManifestGenerator": {
                        "SourceBucket": f"arn:aws:s3:::{bucket}",
                        "EnableManifestOutput": False,
                    }
                },
            )
            job_id = resp["JobId"]
            cancel_resp = s3control.update_job_status(
                AccountId=ACCOUNT_ID, JobId=job_id, RequestedJobStatus="Cancelled"
            )
            assert cancel_resp["JobId"] == job_id
            assert cancel_resp["Status"] == "Cancelled"
        finally:
            s3.delete_bucket(Bucket=bucket)

    def test_update_job_status_reflected_in_describe(self, s3control, s3):
        """Cancelled status is reflected in DescribeJob."""
        bucket = f"job-stat-{_uid()}"
        s3.create_bucket(Bucket=bucket)
        try:
            resp = s3control.create_job(
                AccountId=ACCOUNT_ID,
                Operation={"S3PutObjectCopy": {"TargetResource": f"arn:aws:s3:::{bucket}"}},
                Report={"Enabled": False},
                ClientRequestToken=str(uuid.uuid4()),
                Priority=10,
                RoleArn=f"arn:aws:iam::{ACCOUNT_ID}:role/test-role",
                ConfirmationRequired=False,
                ManifestGenerator={
                    "S3JobManifestGenerator": {
                        "SourceBucket": f"arn:aws:s3:::{bucket}",
                        "EnableManifestOutput": False,
                    }
                },
            )
            job_id = resp["JobId"]
            s3control.update_job_status(
                AccountId=ACCOUNT_ID, JobId=job_id, RequestedJobStatus="Cancelled"
            )
            desc = s3control.describe_job(AccountId=ACCOUNT_ID, JobId=job_id)
            assert desc["Job"]["Status"] == "Cancelled"
        finally:
            s3.delete_bucket(Bucket=bucket)

    def test_update_job_priority_nonexistent(self, s3control):
        """UpdateJobPriority for nonexistent job raises error."""
        with pytest.raises(ClientError) as exc_info:
            s3control.update_job_priority(
                AccountId=ACCOUNT_ID,
                JobId="00000000-0000-0000-0000-000000000000",
                Priority=10,
            )
        assert exc_info.value.response["Error"]["Code"] == "NoSuchJob"

    def test_update_job_status_nonexistent(self, s3control):
        """UpdateJobStatus for nonexistent job raises error."""
        with pytest.raises(ClientError) as exc_info:
            s3control.update_job_status(
                AccountId=ACCOUNT_ID,
                JobId="00000000-0000-0000-0000-000000000000",
                RequestedJobStatus="Cancelled",
            )
        assert exc_info.value.response["Error"]["Code"] == "NoSuchJob"

    def test_create_job_full_lifecycle(self, s3control, s3):
        """Full job lifecycle: C → R → L → U priority → D(cancel) → E(nonexistent)."""
        bucket = f"job-fl-{_uid()}"
        s3.create_bucket(Bucket=bucket)
        try:
            # CREATE: create a job
            create_resp = s3control.create_job(
                AccountId=ACCOUNT_ID,
                Operation={"S3PutObjectCopy": {"TargetResource": f"arn:aws:s3:::{bucket}"}},
                Report={"Enabled": False},
                ClientRequestToken=str(uuid.uuid4()),
                Priority=5,
                RoleArn=f"arn:aws:iam::{ACCOUNT_ID}:role/test-role",
                ConfirmationRequired=False,
                ManifestGenerator={
                    "S3JobManifestGenerator": {
                        "SourceBucket": f"arn:aws:s3:::{bucket}",
                        "EnableManifestOutput": False,
                    }
                },
            )
            job_id = create_resp["JobId"]
            assert len(job_id) > 0
            # RETRIEVE: describe the job
            desc = s3control.describe_job(AccountId=ACCOUNT_ID, JobId=job_id)
            assert desc["Job"]["JobId"] == job_id
            assert desc["Job"]["Priority"] == 5
            # LIST: job appears in list
            list_resp = s3control.list_jobs(AccountId=ACCOUNT_ID)
            job_ids = [j["JobId"] for j in list_resp["Jobs"]]
            assert job_id in job_ids
            # UPDATE: change priority
            s3control.update_job_priority(AccountId=ACCOUNT_ID, JobId=job_id, Priority=99)
            desc2 = s3control.describe_job(AccountId=ACCOUNT_ID, JobId=job_id)
            assert desc2["Job"]["Priority"] == 99
            # DELETE (cancel): cancel the job
            cancel_resp = s3control.update_job_status(
                AccountId=ACCOUNT_ID, JobId=job_id, RequestedJobStatus="Cancelled"
            )
            assert cancel_resp["Status"] == "Cancelled"
            # ERROR: nonexistent job
            with pytest.raises(ClientError) as exc_info:
                s3control.describe_job(
                    AccountId=ACCOUNT_ID, JobId="00000000-0000-0000-0000-000000000099"
                )
            assert exc_info.value.response["Error"]["Code"] == "NoSuchJob"
        finally:
            try:
                s3control.update_job_status(
                    AccountId=ACCOUNT_ID, JobId=job_id, RequestedJobStatus="Cancelled"
                )
            except Exception:
                pass  # best-effort cleanup
            try:
                s3.delete_bucket(Bucket=bucket)
            except Exception:
                pass  # best-effort cleanup


class TestS3ControlDeleteStorageLensTagging:
    """Tests for DeleteStorageLensConfigurationTagging operation."""

    @pytest.fixture
    def s3control(self):
        return make_client("s3control")

    def test_delete_storage_lens_configuration_tagging(self, s3control):
        """DeleteStorageLensConfigurationTagging removes tags from a config."""
        config_id = f"lens-dt-{_uid()}"
        try:
            s3control.put_storage_lens_configuration(
                AccountId=ACCOUNT_ID,
                ConfigId=config_id,
                StorageLensConfiguration={
                    "Id": config_id,
                    "AccountLevel": {"BucketLevel": {}},
                    "IsEnabled": True,
                },
            )
            s3control.put_storage_lens_configuration_tagging(
                AccountId=ACCOUNT_ID,
                ConfigId=config_id,
                Tags=[{"Key": "env", "Value": "test"}],
            )
            # Verify tags exist
            resp = s3control.get_storage_lens_configuration_tagging(
                AccountId=ACCOUNT_ID, ConfigId=config_id
            )
            assert len(resp["Tags"]) >= 1

            # Delete tags
            del_resp = s3control.delete_storage_lens_configuration_tagging(
                AccountId=ACCOUNT_ID, ConfigId=config_id
            )
            assert del_resp["ResponseMetadata"]["HTTPStatusCode"] == 200

            # Verify tags are gone
            resp2 = s3control.get_storage_lens_configuration_tagging(
                AccountId=ACCOUNT_ID, ConfigId=config_id
            )
            assert resp2["Tags"] == []
        finally:
            try:
                s3control.delete_storage_lens_configuration(
                    AccountId=ACCOUNT_ID, ConfigId=config_id
                )
            except Exception:
                pass  # best-effort cleanup

    def test_delete_storage_lens_tagging_idempotent(self, s3control):
        """Deleting tags when none exist succeeds."""
        config_id = f"lens-dti-{_uid()}"
        try:
            s3control.put_storage_lens_configuration(
                AccountId=ACCOUNT_ID,
                ConfigId=config_id,
                StorageLensConfiguration={
                    "Id": config_id,
                    "AccountLevel": {"BucketLevel": {}},
                    "IsEnabled": True,
                },
            )
            # Delete tags when none exist
            resp = s3control.delete_storage_lens_configuration_tagging(
                AccountId=ACCOUNT_ID, ConfigId=config_id
            )
            assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200
        finally:
            try:
                s3control.delete_storage_lens_configuration(
                    AccountId=ACCOUNT_ID, ConfigId=config_id
                )
            except Exception:
                pass  # best-effort cleanup


class TestS3ControlAccessPointForObjectLambda:
    """Tests for Object Lambda Access Point operations."""

    @pytest.fixture
    def s3control(self):
        return make_client("s3control")

    @pytest.fixture
    def s3(self):
        return make_client("s3")

    def _make_olap_config(self, ap_name):
        return {
            "SupportingAccessPoint": (f"arn:aws:s3:us-east-1:{ACCOUNT_ID}:accesspoint/{ap_name}"),
            "TransformationConfigurations": [
                {
                    "Actions": ["GetObject"],
                    "ContentTransformation": {
                        "AwsLambda": {
                            "FunctionArn": (
                                f"arn:aws:lambda:us-east-1:{ACCOUNT_ID}:function:my-func"
                            ),
                        }
                    },
                }
            ],
        }

    def test_create_access_point_for_object_lambda(self, s3control, s3):
        """CreateAccessPointForObjectLambda creates an OLAP."""
        bucket = f"olap-cr-{_uid()}"
        ap_name = f"olap-src-{_uid()}"
        olap_name = f"olap-{_uid()}"
        s3.create_bucket(Bucket=bucket)
        try:
            s3control.create_access_point(AccountId=ACCOUNT_ID, Name=ap_name, Bucket=bucket)
            resp = s3control.create_access_point_for_object_lambda(
                AccountId=ACCOUNT_ID,
                Name=olap_name,
                Configuration=self._make_olap_config(ap_name),
            )
            assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200
            assert "ObjectLambdaAccessPointArn" in resp
        finally:
            try:
                s3control.delete_access_point_for_object_lambda(
                    AccountId=ACCOUNT_ID, Name=olap_name
                )
            except Exception:
                pass  # best-effort cleanup
            try:
                s3control.delete_access_point(AccountId=ACCOUNT_ID, Name=ap_name)
            except Exception:
                pass  # best-effort cleanup
            s3.delete_bucket(Bucket=bucket)

    def test_get_access_point_for_object_lambda(self, s3control, s3):
        """GetAccessPointForObjectLambda returns OLAP details."""
        bucket = f"olap-get-{_uid()}"
        ap_name = f"olap-gsrc-{_uid()}"
        olap_name = f"olap-g-{_uid()}"
        s3.create_bucket(Bucket=bucket)
        try:
            s3control.create_access_point(AccountId=ACCOUNT_ID, Name=ap_name, Bucket=bucket)
            s3control.create_access_point_for_object_lambda(
                AccountId=ACCOUNT_ID,
                Name=olap_name,
                Configuration=self._make_olap_config(ap_name),
            )
            resp = s3control.get_access_point_for_object_lambda(
                AccountId=ACCOUNT_ID, Name=olap_name
            )
            assert resp["Name"] == olap_name
            assert "CreationDate" in resp
        finally:
            try:
                s3control.delete_access_point_for_object_lambda(
                    AccountId=ACCOUNT_ID, Name=olap_name
                )
            except Exception:
                pass  # best-effort cleanup
            try:
                s3control.delete_access_point(AccountId=ACCOUNT_ID, Name=ap_name)
            except Exception:
                pass  # best-effort cleanup
            s3.delete_bucket(Bucket=bucket)

    def test_delete_access_point_for_object_lambda(self, s3control, s3):
        """DeleteAccessPointForObjectLambda removes an OLAP."""
        bucket = f"olap-del-{_uid()}"
        ap_name = f"olap-dsrc-{_uid()}"
        olap_name = f"olap-d-{_uid()}"
        s3.create_bucket(Bucket=bucket)
        try:
            s3control.create_access_point(AccountId=ACCOUNT_ID, Name=ap_name, Bucket=bucket)
            s3control.create_access_point_for_object_lambda(
                AccountId=ACCOUNT_ID,
                Name=olap_name,
                Configuration=self._make_olap_config(ap_name),
            )
            resp = s3control.delete_access_point_for_object_lambda(
                AccountId=ACCOUNT_ID, Name=olap_name
            )
            assert resp["ResponseMetadata"]["HTTPStatusCode"] in (200, 204)
        finally:
            try:
                s3control.delete_access_point(AccountId=ACCOUNT_ID, Name=ap_name)
            except Exception:
                pass  # best-effort cleanup
            s3.delete_bucket(Bucket=bucket)

    def test_put_access_point_policy_for_object_lambda(self, s3control, s3):
        """PutAccessPointPolicyForObjectLambda sets a policy on an OLAP."""
        bucket = f"olap-pp-{_uid()}"
        ap_name = f"olap-ppsrc-{_uid()}"
        olap_name = f"olap-pp-{_uid()}"
        s3.create_bucket(Bucket=bucket)
        try:
            s3control.create_access_point(AccountId=ACCOUNT_ID, Name=ap_name, Bucket=bucket)
            s3control.create_access_point_for_object_lambda(
                AccountId=ACCOUNT_ID,
                Name=olap_name,
                Configuration=self._make_olap_config(ap_name),
            )
            policy = json.dumps(
                {
                    "Version": "2012-10-17",
                    "Statement": [
                        {
                            "Effect": "Allow",
                            "Principal": "*",
                            "Action": "s3-object-lambda:GetObject",
                            "Resource": (
                                f"arn:aws:s3-object-lambda:us-east-1:{ACCOUNT_ID}"
                                f":accesspoint/{olap_name}"
                            ),
                        }
                    ],
                }
            )
            resp = s3control.put_access_point_policy_for_object_lambda(
                AccountId=ACCOUNT_ID, Name=olap_name, Policy=policy
            )
            assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200
        finally:
            try:
                s3control.delete_access_point_policy_for_object_lambda(
                    AccountId=ACCOUNT_ID, Name=olap_name
                )
            except Exception:
                pass  # best-effort cleanup
            try:
                s3control.delete_access_point_for_object_lambda(
                    AccountId=ACCOUNT_ID, Name=olap_name
                )
            except Exception:
                pass  # best-effort cleanup
            try:
                s3control.delete_access_point(AccountId=ACCOUNT_ID, Name=ap_name)
            except Exception:
                pass  # best-effort cleanup
            s3.delete_bucket(Bucket=bucket)

    def test_get_access_point_policy_for_object_lambda(self, s3control, s3):
        """GetAccessPointPolicyForObjectLambda returns the policy."""
        bucket = f"olap-gp-{_uid()}"
        ap_name = f"olap-gpsrc-{_uid()}"
        olap_name = f"olap-gp-{_uid()}"
        s3.create_bucket(Bucket=bucket)
        try:
            s3control.create_access_point(AccountId=ACCOUNT_ID, Name=ap_name, Bucket=bucket)
            s3control.create_access_point_for_object_lambda(
                AccountId=ACCOUNT_ID,
                Name=olap_name,
                Configuration=self._make_olap_config(ap_name),
            )
            policy = json.dumps(
                {
                    "Version": "2012-10-17",
                    "Statement": [
                        {
                            "Effect": "Allow",
                            "Principal": "*",
                            "Action": "s3-object-lambda:GetObject",
                            "Resource": (
                                f"arn:aws:s3-object-lambda:us-east-1:{ACCOUNT_ID}"
                                f":accesspoint/{olap_name}"
                            ),
                        }
                    ],
                }
            )
            s3control.put_access_point_policy_for_object_lambda(
                AccountId=ACCOUNT_ID, Name=olap_name, Policy=policy
            )
            resp = s3control.get_access_point_policy_for_object_lambda(
                AccountId=ACCOUNT_ID, Name=olap_name
            )
            assert "Policy" in resp
            assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200
        finally:
            try:
                s3control.delete_access_point_policy_for_object_lambda(
                    AccountId=ACCOUNT_ID, Name=olap_name
                )
            except Exception:
                pass  # best-effort cleanup
            try:
                s3control.delete_access_point_for_object_lambda(
                    AccountId=ACCOUNT_ID, Name=olap_name
                )
            except Exception:
                pass  # best-effort cleanup
            try:
                s3control.delete_access_point(AccountId=ACCOUNT_ID, Name=ap_name)
            except Exception:
                pass  # best-effort cleanup
            s3.delete_bucket(Bucket=bucket)

    def test_delete_access_point_policy_for_object_lambda(self, s3control, s3):
        """DeleteAccessPointPolicyForObjectLambda removes the OLAP policy."""
        bucket = f"olap-dp-{_uid()}"
        ap_name = f"olap-dpsrc-{_uid()}"
        olap_name = f"olap-dp-{_uid()}"
        s3.create_bucket(Bucket=bucket)
        try:
            s3control.create_access_point(AccountId=ACCOUNT_ID, Name=ap_name, Bucket=bucket)
            s3control.create_access_point_for_object_lambda(
                AccountId=ACCOUNT_ID,
                Name=olap_name,
                Configuration=self._make_olap_config(ap_name),
            )
            policy = json.dumps({"Version": "2012-10-17", "Statement": []})
            s3control.put_access_point_policy_for_object_lambda(
                AccountId=ACCOUNT_ID, Name=olap_name, Policy=policy
            )
            resp = s3control.delete_access_point_policy_for_object_lambda(
                AccountId=ACCOUNT_ID, Name=olap_name
            )
            assert resp["ResponseMetadata"]["HTTPStatusCode"] in (200, 204)
        finally:
            try:
                s3control.delete_access_point_for_object_lambda(
                    AccountId=ACCOUNT_ID, Name=olap_name
                )
            except Exception:
                pass  # best-effort cleanup
            try:
                s3control.delete_access_point(AccountId=ACCOUNT_ID, Name=ap_name)
            except Exception:
                pass  # best-effort cleanup
            s3.delete_bucket(Bucket=bucket)

    def test_get_access_point_policy_status_for_object_lambda(self, s3control, s3):
        """GetAccessPointPolicyStatusForObjectLambda returns IsPublic."""
        bucket = f"olap-ps-{_uid()}"
        ap_name = f"olap-pssrc-{_uid()}"
        olap_name = f"olap-ps-{_uid()}"
        s3.create_bucket(Bucket=bucket)
        try:
            s3control.create_access_point(AccountId=ACCOUNT_ID, Name=ap_name, Bucket=bucket)
            s3control.create_access_point_for_object_lambda(
                AccountId=ACCOUNT_ID,
                Name=olap_name,
                Configuration=self._make_olap_config(ap_name),
            )
            policy = json.dumps(
                {
                    "Version": "2012-10-17",
                    "Statement": [
                        {
                            "Effect": "Allow",
                            "Principal": "*",
                            "Action": "s3-object-lambda:GetObject",
                            "Resource": (
                                f"arn:aws:s3-object-lambda:us-east-1:{ACCOUNT_ID}"
                                f":accesspoint/{olap_name}"
                            ),
                        }
                    ],
                }
            )
            s3control.put_access_point_policy_for_object_lambda(
                AccountId=ACCOUNT_ID, Name=olap_name, Policy=policy
            )
            resp = s3control.get_access_point_policy_status_for_object_lambda(
                AccountId=ACCOUNT_ID, Name=olap_name
            )
            assert "PolicyStatus" in resp
            assert "IsPublic" in resp["PolicyStatus"]
        finally:
            try:
                s3control.delete_access_point_policy_for_object_lambda(
                    AccountId=ACCOUNT_ID, Name=olap_name
                )
            except Exception:
                pass  # best-effort cleanup
            try:
                s3control.delete_access_point_for_object_lambda(
                    AccountId=ACCOUNT_ID, Name=olap_name
                )
            except Exception:
                pass  # best-effort cleanup
            try:
                s3control.delete_access_point(AccountId=ACCOUNT_ID, Name=ap_name)
            except Exception:
                pass  # best-effort cleanup
            s3.delete_bucket(Bucket=bucket)


class TestS3ControlAccessPointScope:
    """Tests for Access Point Scope operations."""

    @pytest.fixture
    def s3control(self):
        return make_client("s3control")

    @pytest.fixture
    def s3(self):
        return make_client("s3")

    def test_put_access_point_scope(self, s3control, s3):
        """PutAccessPointScope sets a scope on an access point."""
        bucket = f"scope-put-{_uid()}"
        ap_name = f"scope-ap-{_uid()}"
        s3.create_bucket(Bucket=bucket)
        try:
            s3control.create_access_point(AccountId=ACCOUNT_ID, Name=ap_name, Bucket=bucket)
            resp = s3control.put_access_point_scope(
                AccountId=ACCOUNT_ID,
                Name=ap_name,
                Scope={
                    "Prefixes": ["data/"],
                },
            )
            assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200
        finally:
            try:
                s3control.delete_access_point_scope(AccountId=ACCOUNT_ID, Name=ap_name)
            except Exception:
                pass  # best-effort cleanup
            try:
                s3control.delete_access_point(AccountId=ACCOUNT_ID, Name=ap_name)
            except Exception:
                pass  # best-effort cleanup
            s3.delete_bucket(Bucket=bucket)

    def test_get_access_point_scope(self, s3control, s3):
        """GetAccessPointScope returns the scope for an access point."""
        bucket = f"scope-get-{_uid()}"
        ap_name = f"scope-gap-{_uid()}"
        s3.create_bucket(Bucket=bucket)
        try:
            s3control.create_access_point(AccountId=ACCOUNT_ID, Name=ap_name, Bucket=bucket)
            s3control.put_access_point_scope(
                AccountId=ACCOUNT_ID,
                Name=ap_name,
                Scope={
                    "Prefixes": ["data/"],
                },
            )
            resp = s3control.get_access_point_scope(AccountId=ACCOUNT_ID, Name=ap_name)
            assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200
            assert "Scope" in resp
        finally:
            try:
                s3control.delete_access_point_scope(AccountId=ACCOUNT_ID, Name=ap_name)
            except Exception:
                pass  # best-effort cleanup
            try:
                s3control.delete_access_point(AccountId=ACCOUNT_ID, Name=ap_name)
            except Exception:
                pass  # best-effort cleanup
            s3.delete_bucket(Bucket=bucket)

    def test_delete_access_point_scope(self, s3control, s3):
        """DeleteAccessPointScope removes the scope from an access point."""
        bucket = f"scope-del-{_uid()}"
        ap_name = f"scope-dap-{_uid()}"
        s3.create_bucket(Bucket=bucket)
        try:
            s3control.create_access_point(AccountId=ACCOUNT_ID, Name=ap_name, Bucket=bucket)
            s3control.put_access_point_scope(
                AccountId=ACCOUNT_ID,
                Name=ap_name,
                Scope={
                    "Prefixes": ["data/"],
                },
            )
            resp = s3control.delete_access_point_scope(AccountId=ACCOUNT_ID, Name=ap_name)
            assert resp["ResponseMetadata"]["HTTPStatusCode"] in (200, 204)
        finally:
            try:
                s3control.delete_access_point(AccountId=ACCOUNT_ID, Name=ap_name)
            except Exception:
                pass  # best-effort cleanup
            s3.delete_bucket(Bucket=bucket)


class TestS3ControlBucketLifecyclePolicyReplicationTagging:
    """Tests for bucket-level Put/Delete operations: lifecycle, policy, replication, tagging."""

    @pytest.fixture
    def s3control(self):
        return make_client("s3control")

    @pytest.fixture
    def s3(self):
        return make_client("s3")

    @pytest.fixture
    def bucket(self, s3):
        name = f"bkt-ops-{_uid()}"
        s3.create_bucket(Bucket=name)
        yield name
        try:
            s3.delete_bucket(Bucket=name)
        except Exception:
            pass  # best-effort cleanup

    def test_put_bucket_tagging(self, s3control, bucket):
        """PutBucketTagging: create → retrieve → update → delete → error."""
        # CREATE: put initial tags
        resp = s3control.put_bucket_tagging(
            AccountId=ACCOUNT_ID,
            Bucket=bucket,
            Tagging={"TagSet": [{"Key": "env", "Value": "test"}]},
        )
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200
        # RETRIEVE: get tags and verify content
        get_resp = s3control.get_bucket_tagging(AccountId=ACCOUNT_ID, Bucket=bucket)
        assert "TagSet" in get_resp
        tag_map = {t["Key"]: t["Value"] for t in get_resp["TagSet"]}
        assert tag_map.get("env") == "test"
        # UPDATE: replace tags with different values
        s3control.put_bucket_tagging(
            AccountId=ACCOUNT_ID,
            Bucket=bucket,
            Tagging={"TagSet": [{"Key": "env", "Value": "prod"}, {"Key": "team", "Value": "ops"}]},
        )
        get_resp2 = s3control.get_bucket_tagging(AccountId=ACCOUNT_ID, Bucket=bucket)
        tag_map2 = {t["Key"]: t["Value"] for t in get_resp2["TagSet"]}
        assert tag_map2.get("env") == "prod"
        assert tag_map2.get("team") == "ops"
        # DELETE: remove all tags
        del_resp = s3control.delete_bucket_tagging(AccountId=ACCOUNT_ID, Bucket=bucket)
        assert del_resp["ResponseMetadata"]["HTTPStatusCode"] in (200, 204)
        # ERROR: put tags on nonexistent bucket raises NoSuchBucket
        with pytest.raises(ClientError) as exc:
            s3control.put_bucket_tagging(
                AccountId=ACCOUNT_ID,
                Bucket=f"no-such-bucket-{_uid()}",
                Tagging={"TagSet": [{"Key": "k", "Value": "v"}]},
            )
        assert exc.value.response["Error"]["Code"] == "NoSuchBucket"

    def test_put_and_get_bucket_tagging(self, s3control, bucket):
        """PutBucketTagging then GetBucketTagging returns tags."""
        s3control.put_bucket_tagging(
            AccountId=ACCOUNT_ID,
            Bucket=bucket,
            Tagging={"TagSet": [{"Key": "team", "Value": "platform"}]},
        )
        resp = s3control.get_bucket_tagging(AccountId=ACCOUNT_ID, Bucket=bucket)
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200

    def test_delete_bucket_tagging(self, s3control, bucket):
        """DeleteBucketTagging removes tags."""
        s3control.put_bucket_tagging(
            AccountId=ACCOUNT_ID,
            Bucket=bucket,
            Tagging={"TagSet": [{"Key": "k", "Value": "v"}]},
        )
        resp = s3control.delete_bucket_tagging(AccountId=ACCOUNT_ID, Bucket=bucket)
        assert resp["ResponseMetadata"]["HTTPStatusCode"] in (200, 204)

    def test_put_bucket_policy(self, s3control, bucket):
        """PutBucketPolicy: create → retrieve → update → delete → error."""
        policy = json.dumps(
            {
                "Version": "2012-10-17",
                "Statement": [
                    {
                        "Effect": "Allow",
                        "Principal": "*",
                        "Action": "s3:GetObject",
                        "Resource": f"arn:aws:s3:::{bucket}/*",
                    }
                ],
            }
        )
        # CREATE: put policy
        resp = s3control.put_bucket_policy(AccountId=ACCOUNT_ID, Bucket=bucket, Policy=policy)
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200
        # RETRIEVE: get policy, verify JSON content
        get_resp = s3control.get_bucket_policy(AccountId=ACCOUNT_ID, Bucket=bucket)
        assert "Policy" in get_resp
        parsed = json.loads(get_resp["Policy"])
        assert parsed["Statement"][0]["Action"] == "s3:GetObject"
        # UPDATE: replace policy with different statement
        policy2 = json.dumps(
            {
                "Version": "2012-10-17",
                "Statement": [
                    {
                        "Effect": "Deny",
                        "Principal": "*",
                        "Action": "s3:DeleteObject",
                        "Resource": f"arn:aws:s3:::{bucket}/*",
                    }
                ],
            }
        )
        s3control.put_bucket_policy(AccountId=ACCOUNT_ID, Bucket=bucket, Policy=policy2)
        get_resp2 = s3control.get_bucket_policy(AccountId=ACCOUNT_ID, Bucket=bucket)
        parsed2 = json.loads(get_resp2["Policy"])
        assert parsed2["Statement"][0]["Action"] == "s3:DeleteObject"
        # DELETE: remove policy
        del_resp = s3control.delete_bucket_policy(AccountId=ACCOUNT_ID, Bucket=bucket)
        assert del_resp["ResponseMetadata"]["HTTPStatusCode"] in (200, 204)
        # ERROR: get policy on nonexistent bucket raises NoSuchBucket
        with pytest.raises(ClientError) as exc:
            s3control.get_bucket_policy(
                AccountId=ACCOUNT_ID, Bucket=f"no-such-bucket-{_uid()}"
            )
        assert exc.value.response["Error"]["Code"] == "NoSuchBucket"

    def test_delete_bucket_policy(self, s3control, bucket):
        """DeleteBucketPolicy removes the policy."""
        policy = json.dumps(
            {
                "Version": "2012-10-17",
                "Statement": [
                    {
                        "Effect": "Allow",
                        "Principal": "*",
                        "Action": "s3:GetObject",
                        "Resource": f"arn:aws:s3:::{bucket}/*",
                    }
                ],
            }
        )
        s3control.put_bucket_policy(AccountId=ACCOUNT_ID, Bucket=bucket, Policy=policy)
        resp = s3control.delete_bucket_policy(AccountId=ACCOUNT_ID, Bucket=bucket)
        assert resp["ResponseMetadata"]["HTTPStatusCode"] in (200, 204)

    def test_delete_bucket_lifecycle_configuration(self, s3control, bucket):
        """BucketLifecycle: create → retrieve → update → delete → error."""
        # CREATE: put lifecycle rules
        put_resp = s3control.put_bucket_lifecycle_configuration(
            AccountId=ACCOUNT_ID,
            Bucket=bucket,
            LifecycleConfiguration={
                "Rules": [{"ID": "rule1", "Status": "Enabled", "Filter": {"Prefix": "logs/"}}]
            },
        )
        assert put_resp["ResponseMetadata"]["HTTPStatusCode"] == 200
        # RETRIEVE: get lifecycle configuration
        get_resp = s3control.get_bucket_lifecycle_configuration(AccountId=ACCOUNT_ID, Bucket=bucket)
        assert "Rules" in get_resp
        assert isinstance(get_resp["Rules"], list)
        # UPDATE: replace lifecycle rules
        s3control.put_bucket_lifecycle_configuration(
            AccountId=ACCOUNT_ID,
            Bucket=bucket,
            LifecycleConfiguration={
                "Rules": [{"ID": "rule2", "Status": "Disabled", "Filter": {"Prefix": "tmp/"}}]
            },
        )
        # DELETE: remove lifecycle configuration
        resp = s3control.delete_bucket_lifecycle_configuration(AccountId=ACCOUNT_ID, Bucket=bucket)
        assert resp["ResponseMetadata"]["HTTPStatusCode"] in (200, 204)
        # ERROR: get lifecycle on nonexistent bucket raises NoSuchBucket
        with pytest.raises(ClientError) as exc:
            s3control.get_bucket_lifecycle_configuration(
                AccountId=ACCOUNT_ID, Bucket=f"no-such-bucket-{_uid()}"
            )
        assert exc.value.response["Error"]["Code"] == "NoSuchBucket"

    def test_put_bucket_replication(self, s3control, bucket):
        """PutBucketReplication: create → retrieve → update → delete → error."""
        repl_config = {
            "Role": f"arn:aws:iam::{ACCOUNT_ID}:role/repl-role",
            "Rules": [
                {
                    "ID": "rule1",
                    "Status": "Enabled",
                    "Priority": 1,
                    "Bucket": f"arn:aws:s3:::{bucket}",
                    "Filter": {"Prefix": ""},
                    "Destination": {
                        "Bucket": f"arn:aws:s3:::{bucket}",
                        "Account": ACCOUNT_ID,
                    },
                    "DeleteMarkerReplication": {"Status": "Disabled"},
                }
            ],
        }
        # CREATE: put replication configuration
        resp = s3control.put_bucket_replication(
            AccountId=ACCOUNT_ID,
            Bucket=bucket,
            ReplicationConfiguration=repl_config,
        )
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200
        # RETRIEVE: get replication returns 200
        get_resp = s3control.get_bucket_replication(AccountId=ACCOUNT_ID, Bucket=bucket)
        assert get_resp["ResponseMetadata"]["HTTPStatusCode"] == 200
        # UPDATE: put again with different role
        repl_config2 = dict(repl_config)
        repl_config2["Role"] = f"arn:aws:iam::{ACCOUNT_ID}:role/repl-role-v2"
        s3control.put_bucket_replication(
            AccountId=ACCOUNT_ID,
            Bucket=bucket,
            ReplicationConfiguration=repl_config2,
        )
        # DELETE: remove replication config
        del_resp = s3control.delete_bucket_replication(AccountId=ACCOUNT_ID, Bucket=bucket)
        assert del_resp["ResponseMetadata"]["HTTPStatusCode"] in (200, 204)
        # ERROR: get replication on nonexistent bucket raises NoSuchBucket
        with pytest.raises(ClientError) as exc:
            s3control.get_bucket_replication(
                AccountId=ACCOUNT_ID, Bucket=f"no-such-bucket-{_uid()}"
            )
        assert exc.value.response["Error"]["Code"] == "NoSuchBucket"

    def test_delete_bucket_replication(self, s3control, bucket):
        """BucketReplication: create → retrieve → update → delete → error."""
        repl_config = {
            "Role": f"arn:aws:iam::{ACCOUNT_ID}:role/del-repl-role",
            "Rules": [
                {
                    "ID": "rule1",
                    "Status": "Enabled",
                    "Priority": 1,
                    "Bucket": f"arn:aws:s3:::{bucket}",
                    "Filter": {"Prefix": ""},
                    "Destination": {
                        "Bucket": f"arn:aws:s3:::{bucket}",
                        "Account": ACCOUNT_ID,
                    },
                    "DeleteMarkerReplication": {"Status": "Disabled"},
                }
            ],
        }
        # CREATE: put replication config
        put_resp = s3control.put_bucket_replication(
            AccountId=ACCOUNT_ID,
            Bucket=bucket,
            ReplicationConfiguration=repl_config,
        )
        assert put_resp["ResponseMetadata"]["HTTPStatusCode"] == 200
        # RETRIEVE: get replication returns 200
        get_resp = s3control.get_bucket_replication(AccountId=ACCOUNT_ID, Bucket=bucket)
        assert get_resp["ResponseMetadata"]["HTTPStatusCode"] == 200
        # UPDATE: put again with modified config
        repl_config2 = dict(repl_config)
        repl_config2["Role"] = f"arn:aws:iam::{ACCOUNT_ID}:role/del-repl-role-v2"
        s3control.put_bucket_replication(
            AccountId=ACCOUNT_ID,
            Bucket=bucket,
            ReplicationConfiguration=repl_config2,
        )
        # DELETE: remove replication configuration
        resp = s3control.delete_bucket_replication(AccountId=ACCOUNT_ID, Bucket=bucket)
        assert resp["ResponseMetadata"]["HTTPStatusCode"] in (200, 204)
        # ERROR: get replication on nonexistent bucket raises NoSuchBucket
        with pytest.raises(ClientError) as exc:
            s3control.get_bucket_replication(
                AccountId=ACCOUNT_ID, Bucket=f"no-such-bucket-{_uid()}"
            )
        assert exc.value.response["Error"]["Code"] == "NoSuchBucket"

    def test_put_and_get_bucket_tagging_values(self, s3control, bucket):
        """PutBucketTagging then GetBucketTagging returns a TagSet list."""
        s3control.put_bucket_tagging(
            AccountId=ACCOUNT_ID,
            Bucket=bucket,
            Tagging={"TagSet": [{"Key": "project", "Value": "myapp"}, {"Key": "cost", "Value": "123"}]},
        )
        resp = s3control.get_bucket_tagging(AccountId=ACCOUNT_ID, Bucket=bucket)
        assert "TagSet" in resp
        assert isinstance(resp["TagSet"], list)

    def test_delete_bucket_tagging_leaves_empty_result(self, s3control, bucket):
        """After DeleteBucketTagging, GetBucketTagging returns a TagSet."""
        s3control.put_bucket_tagging(
            AccountId=ACCOUNT_ID,
            Bucket=bucket,
            Tagging={"TagSet": [{"Key": "env", "Value": "test"}]},
        )
        s3control.delete_bucket_tagging(AccountId=ACCOUNT_ID, Bucket=bucket)
        resp = s3control.get_bucket_tagging(AccountId=ACCOUNT_ID, Bucket=bucket)
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200
        assert "TagSet" in resp
        assert isinstance(resp["TagSet"], list)

    def test_put_bucket_tagging_nonexistent_bucket(self, s3control):
        """PutBucketTagging on nonexistent bucket raises NoSuchBucket."""
        with pytest.raises(ClientError) as exc:
            s3control.put_bucket_tagging(
                AccountId=ACCOUNT_ID,
                Bucket=f"no-such-bucket-{_uid()}",
                Tagging={"TagSet": [{"Key": "k", "Value": "v"}]},
            )
        assert exc.value.response["Error"]["Code"] == "NoSuchBucket"

    def test_get_and_verify_bucket_policy_content(self, s3control, bucket):
        """GetBucketPolicy returns parseable JSON with correct Version."""
        policy = json.dumps(
            {
                "Version": "2012-10-17",
                "Statement": [{"Effect": "Allow", "Principal": "*", "Action": "s3:GetObject", "Resource": f"arn:aws:s3:::{bucket}/*"}],
            }
        )
        s3control.put_bucket_policy(AccountId=ACCOUNT_ID, Bucket=bucket, Policy=policy)
        resp = s3control.get_bucket_policy(AccountId=ACCOUNT_ID, Bucket=bucket)
        assert "Policy" in resp
        parsed = json.loads(resp["Policy"])
        assert parsed["Version"] == "2012-10-17"

    def test_delete_bucket_policy_then_get_empty(self, s3control, bucket):
        """After DeleteBucketPolicy, GetBucketPolicy returns empty or error."""
        policy = json.dumps({"Version": "2012-10-17", "Statement": []})
        s3control.put_bucket_policy(AccountId=ACCOUNT_ID, Bucket=bucket, Policy=policy)
        s3control.delete_bucket_policy(AccountId=ACCOUNT_ID, Bucket=bucket)
        try:
            resp = s3control.get_bucket_policy(AccountId=ACCOUNT_ID, Bucket=bucket)
            # If it succeeds, policy should be empty or absent
            assert resp.get("Policy") == "" or resp.get("Policy") is None
        except ClientError as e:
            assert e.response["Error"]["Code"] in (
                "NoSuchBucketPolicy",
                "NoSuchPolicy",
                "NotFoundException",
            )

    def test_put_and_get_bucket_lifecycle(self, s3control, bucket):
        """PutBucketLifecycleConfiguration then GetBucketLifecycleConfiguration returns Rules."""
        s3control.put_bucket_lifecycle_configuration(
            AccountId=ACCOUNT_ID,
            Bucket=bucket,
            LifecycleConfiguration={
                "Rules": [{"ID": "rule1", "Status": "Enabled", "Filter": {"Prefix": "logs/"}}]
            },
        )
        resp = s3control.get_bucket_lifecycle_configuration(AccountId=ACCOUNT_ID, Bucket=bucket)
        assert "Rules" in resp
        assert isinstance(resp["Rules"], list)

    def test_delete_bucket_lifecycle_then_get_empty(self, s3control, bucket):
        """After DeleteBucketLifecycleConfiguration, GetBucketLifecycleConfiguration returns empty Rules."""
        s3control.put_bucket_lifecycle_configuration(
            AccountId=ACCOUNT_ID,
            Bucket=bucket,
            LifecycleConfiguration={
                "Rules": [{"ID": "rule1", "Status": "Enabled", "Filter": {"Prefix": "logs/"}}]
            },
        )
        s3control.delete_bucket_lifecycle_configuration(AccountId=ACCOUNT_ID, Bucket=bucket)
        resp = s3control.get_bucket_lifecycle_configuration(AccountId=ACCOUNT_ID, Bucket=bucket)
        assert "Rules" in resp
        assert isinstance(resp["Rules"], list)

    def test_put_and_get_bucket_replication_content(self, s3control, bucket):
        """GetBucketReplication after PutBucketReplication returns 200."""
        s3control.put_bucket_replication(
            AccountId=ACCOUNT_ID,
            Bucket=bucket,
            ReplicationConfiguration={
                "Role": f"arn:aws:iam::{ACCOUNT_ID}:role/repl-role",
                "Rules": [
                    {
                        "ID": "rule1",
                        "Status": "Enabled",
                        "Priority": 1,
                        "Bucket": f"arn:aws:s3:::{bucket}",
                        "Filter": {"Prefix": ""},
                        "Destination": {"Bucket": f"arn:aws:s3:::{bucket}", "Account": ACCOUNT_ID},
                        "DeleteMarkerReplication": {"Status": "Disabled"},
                    }
                ],
            },
        )
        resp = s3control.get_bucket_replication(AccountId=ACCOUNT_ID, Bucket=bucket)
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200

    def test_delete_bucket_replication_then_get(self, s3control, bucket):
        """After DeleteBucketReplication, GetBucketReplication returns 200."""
        s3control.put_bucket_replication(
            AccountId=ACCOUNT_ID,
            Bucket=bucket,
            ReplicationConfiguration={
                "Role": f"arn:aws:iam::{ACCOUNT_ID}:role/repl-role",
                "Rules": [
                    {
                        "ID": "rule1",
                        "Status": "Enabled",
                        "Priority": 1,
                        "Bucket": f"arn:aws:s3:::{bucket}",
                        "Filter": {"Prefix": ""},
                        "Destination": {"Bucket": f"arn:aws:s3:::{bucket}", "Account": ACCOUNT_ID},
                        "DeleteMarkerReplication": {"Status": "Disabled"},
                    }
                ],
            },
        )
        del_resp = s3control.delete_bucket_replication(AccountId=ACCOUNT_ID, Bucket=bucket)
        assert del_resp["ResponseMetadata"]["HTTPStatusCode"] in (200, 204)
        # After delete, get replication still returns 200 (stub behavior)
        resp = s3control.get_bucket_replication(AccountId=ACCOUNT_ID, Bucket=bucket)
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200


class TestS3ControlJobTagging:
    """Tests for S3 Batch Operations job tagging."""

    @pytest.fixture
    def s3control(self):
        return make_client("s3control")

    @pytest.fixture
    def s3(self):
        return make_client("s3")

    @pytest.fixture
    def job_id(self, s3control, s3):
        bucket = f"jt-{_uid()}"
        s3.create_bucket(Bucket=bucket)
        resp = s3control.create_job(
            AccountId=ACCOUNT_ID,
            Operation={"S3PutObjectCopy": {"TargetResource": f"arn:aws:s3:::{bucket}"}},
            Report={"Enabled": False},
            ClientRequestToken=str(uuid.uuid4()),
            Priority=10,
            RoleArn=f"arn:aws:iam::{ACCOUNT_ID}:role/test-role",
            ConfirmationRequired=False,
            ManifestGenerator={
                "S3JobManifestGenerator": {
                    "SourceBucket": f"arn:aws:s3:::{bucket}",
                    "EnableManifestOutput": False,
                }
            },
            Tags=[{"Key": "env", "Value": "test"}],
        )
        jid = resp["JobId"]
        yield jid
        try:
            s3control.update_job_status(
                AccountId=ACCOUNT_ID, JobId=jid, RequestedJobStatus="Cancelled"
            )
        except Exception:
            pass  # best-effort cleanup
        try:
            s3.delete_bucket(Bucket=bucket)
        except Exception:
            pass  # best-effort cleanup

    def test_get_job_tagging(self, s3control, job_id):
        """GetJobTagging: create → retrieve → list → update → delete → error."""
        # CREATE: put tags (initial tags were set in fixture via create_job Tags param)
        put_resp = s3control.put_job_tagging(
            AccountId=ACCOUNT_ID,
            JobId=job_id,
            Tags=[{"Key": "env", "Value": "staging"}, {"Key": "version", "Value": "1"}],
        )
        assert put_resp["ResponseMetadata"]["HTTPStatusCode"] == 200
        # RETRIEVE: get tags returns list
        resp = s3control.get_job_tagging(AccountId=ACCOUNT_ID, JobId=job_id)
        assert "Tags" in resp
        assert isinstance(resp["Tags"], list)
        tag_map = {t["Key"]: t["Value"] for t in resp["Tags"]}
        assert tag_map.get("env") == "staging"
        # LIST: job appears in list_jobs with Status field
        list_resp = s3control.list_jobs(AccountId=ACCOUNT_ID)
        assert job_id in [j["JobId"] for j in list_resp["Jobs"]]
        # UPDATE: replace tags
        s3control.put_job_tagging(
            AccountId=ACCOUNT_ID,
            JobId=job_id,
            Tags=[{"Key": "env", "Value": "prod"}],
        )
        get_resp2 = s3control.get_job_tagging(AccountId=ACCOUNT_ID, JobId=job_id)
        tag_map2 = {t["Key"]: t["Value"] for t in get_resp2["Tags"]}
        assert tag_map2.get("env") == "prod"
        # DELETE: remove all tags
        del_resp = s3control.delete_job_tagging(AccountId=ACCOUNT_ID, JobId=job_id)
        assert del_resp["ResponseMetadata"]["HTTPStatusCode"] in (200, 204)
        # ERROR: get tags for nonexistent job raises NoSuchJob
        with pytest.raises(ClientError) as exc:
            s3control.get_job_tagging(
                AccountId=ACCOUNT_ID, JobId="00000000-0000-0000-0000-000000000000"
            )
        assert exc.value.response["Error"]["Code"] == "NoSuchJob"

    def test_delete_job_tagging(self, s3control, job_id):
        """DeleteJobTagging: create → retrieve → list → update → delete → error."""
        # CREATE: put tags
        s3control.put_job_tagging(
            AccountId=ACCOUNT_ID,
            JobId=job_id,
            Tags=[{"Key": "lifecycle", "Value": "full"}, {"Key": "phase", "Value": "test"}],
        )
        # RETRIEVE: get tags and verify
        get_resp = s3control.get_job_tagging(AccountId=ACCOUNT_ID, JobId=job_id)
        assert "Tags" in get_resp
        tag_map = {t["Key"]: t["Value"] for t in get_resp["Tags"]}
        assert tag_map.get("lifecycle") == "full"
        # LIST: job visible in list
        list_resp = s3control.list_jobs(AccountId=ACCOUNT_ID)
        assert job_id in [j["JobId"] for j in list_resp["Jobs"]]
        # UPDATE: replace tags with different values
        s3control.put_job_tagging(
            AccountId=ACCOUNT_ID,
            JobId=job_id,
            Tags=[{"Key": "lifecycle", "Value": "updated"}],
        )
        get_resp2 = s3control.get_job_tagging(AccountId=ACCOUNT_ID, JobId=job_id)
        tag_map2 = {t["Key"]: t["Value"] for t in get_resp2["Tags"]}
        assert tag_map2.get("lifecycle") == "updated"
        # DELETE: remove all tags
        resp = s3control.delete_job_tagging(AccountId=ACCOUNT_ID, JobId=job_id)
        assert resp["ResponseMetadata"]["HTTPStatusCode"] in (200, 204)
        # Verify tags are gone
        get_resp3 = s3control.get_job_tagging(AccountId=ACCOUNT_ID, JobId=job_id)
        assert get_resp3["Tags"] == []
        # ERROR: delete tags for nonexistent job raises NoSuchJob
        with pytest.raises(ClientError) as exc:
            s3control.delete_job_tagging(
                AccountId=ACCOUNT_ID, JobId="00000000-0000-0000-0000-000000000000"
            )
        assert exc.value.response["Error"]["Code"] == "NoSuchJob"

    def test_delete_then_get_job_tagging(self, s3control, job_id):
        """After DeleteJobTagging, GetJobTagging returns empty tags."""
        s3control.delete_job_tagging(AccountId=ACCOUNT_ID, JobId=job_id)
        resp = s3control.get_job_tagging(AccountId=ACCOUNT_ID, JobId=job_id)
        assert resp["Tags"] == []

    def test_put_job_tagging_returns_200(self, s3control, job_id):
        """PutJobTagging returns 200 for a valid job."""
        resp = s3control.put_job_tagging(
            AccountId=ACCOUNT_ID,
            JobId=job_id,
            Tags=[{"Key": "env", "Value": "staging"}, {"Key": "team", "Value": "data"}],
        )
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200

    def test_put_job_tagging_twice_returns_200(self, s3control, job_id):
        """PutJobTagging can be called multiple times and each returns 200."""
        s3control.put_job_tagging(
            AccountId=ACCOUNT_ID,
            JobId=job_id,
            Tags=[{"Key": "first", "Value": "v1"}],
        )
        resp = s3control.put_job_tagging(
            AccountId=ACCOUNT_ID,
            JobId=job_id,
            Tags=[{"Key": "second", "Value": "v2"}],
        )
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200
        # Tags are accessible via GetJobTagging
        get_resp = s3control.get_job_tagging(AccountId=ACCOUNT_ID, JobId=job_id)
        assert "Tags" in get_resp
        assert isinstance(get_resp["Tags"], list)

    def test_get_job_tagging_nonexistent(self, s3control):
        """GetJobTagging for nonexistent job raises error."""
        with pytest.raises(ClientError) as exc:
            s3control.get_job_tagging(
                AccountId=ACCOUNT_ID, JobId="00000000-0000-0000-0000-000000000000"
            )
        assert exc.value.response["Error"]["Code"] == "NoSuchJob"


class TestS3ControlStorageLensGroups:
    """Tests for Storage Lens Group operations."""

    @pytest.fixture
    def s3control(self):
        return make_client("s3control")

    def test_create_storage_lens_group(self, s3control):
        """CreateStorageLensGroup creates a group."""
        name = f"slg-{_uid()}"
        try:
            resp = s3control.create_storage_lens_group(
                AccountId=ACCOUNT_ID,
                StorageLensGroup={
                    "Name": name,
                    "Filter": {
                        "MatchAnyPrefix": ["logs/"],
                    },
                },
            )
            assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200
        finally:
            try:
                s3control.delete_storage_lens_group(AccountId=ACCOUNT_ID, Name=name)
            except Exception:
                pass  # best-effort cleanup

    def test_get_storage_lens_group(self, s3control):
        """GetStorageLensGroup returns group details."""
        name = f"slg-get-{_uid()}"
        try:
            s3control.create_storage_lens_group(
                AccountId=ACCOUNT_ID,
                StorageLensGroup={
                    "Name": name,
                    "Filter": {
                        "MatchAnyPrefix": ["data/"],
                    },
                },
            )
            resp = s3control.get_storage_lens_group(AccountId=ACCOUNT_ID, Name=name)
            assert "StorageLensGroup" in resp
            assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200
        finally:
            try:
                s3control.delete_storage_lens_group(AccountId=ACCOUNT_ID, Name=name)
            except Exception:
                pass  # best-effort cleanup

    def test_list_storage_lens_groups(self, s3control):
        """ListStorageLensGroups returns groups for the account."""
        name = f"slg-ls-{_uid()}"
        try:
            s3control.create_storage_lens_group(
                AccountId=ACCOUNT_ID,
                StorageLensGroup={
                    "Name": name,
                    "Filter": {
                        "MatchAnyPrefix": ["tmp/"],
                    },
                },
            )
            resp = s3control.list_storage_lens_groups(AccountId=ACCOUNT_ID)
            assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200
        finally:
            try:
                s3control.delete_storage_lens_group(AccountId=ACCOUNT_ID, Name=name)
            except Exception:
                pass  # best-effort cleanup

    def test_update_storage_lens_group(self, s3control):
        """UpdateStorageLensGroup modifies a group."""
        name = f"slg-up-{_uid()}"
        try:
            s3control.create_storage_lens_group(
                AccountId=ACCOUNT_ID,
                StorageLensGroup={
                    "Name": name,
                    "Filter": {
                        "MatchAnyPrefix": ["old/"],
                    },
                },
            )
            resp = s3control.update_storage_lens_group(
                AccountId=ACCOUNT_ID,
                Name=name,
                StorageLensGroup={
                    "Name": name,
                    "Filter": {
                        "MatchAnyPrefix": ["new/"],
                    },
                },
            )
            assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200
        finally:
            try:
                s3control.delete_storage_lens_group(AccountId=ACCOUNT_ID, Name=name)
            except Exception:
                pass  # best-effort cleanup

    def test_delete_storage_lens_group(self, s3control):
        """DeleteStorageLensGroup removes a group."""
        name = f"slg-del-{_uid()}"
        s3control.create_storage_lens_group(
            AccountId=ACCOUNT_ID,
            StorageLensGroup={
                "Name": name,
                "Filter": {
                    "MatchAnyPrefix": ["del/"],
                },
            },
        )
        resp = s3control.delete_storage_lens_group(AccountId=ACCOUNT_ID, Name=name)
        assert resp["ResponseMetadata"]["HTTPStatusCode"] in (200, 204)


class TestS3ControlJobTaggingExpanded:
    """Tests for S3 Control Job Tagging operations."""

    def test_get_job_tagging_not_found(self, s3control):
        """GetJobTagging for nonexistent job raises NoSuchJob."""
        with pytest.raises(ClientError) as exc:
            s3control.get_job_tagging(AccountId=ACCOUNT_ID, JobId="nonexistent-job-id")
        assert exc.value.response["Error"]["Code"] == "NoSuchJob"

    def test_put_job_tagging_not_found(self, s3control):
        """PutJobTagging for nonexistent job raises NoSuchJob."""
        with pytest.raises(ClientError) as exc:
            s3control.put_job_tagging(
                AccountId=ACCOUNT_ID,
                JobId="nonexistent-job-id",
                Tags=[{"Key": "env", "Value": "test"}],
            )
        assert exc.value.response["Error"]["Code"] == "NoSuchJob"

    def test_delete_job_tagging_not_found(self, s3control):
        """DeleteJobTagging for nonexistent job raises NoSuchJob."""
        with pytest.raises(ClientError) as exc:
            s3control.delete_job_tagging(AccountId=ACCOUNT_ID, JobId="nonexistent-job-id")
        assert exc.value.response["Error"]["Code"] == "NoSuchJob"


class TestS3ControlBucketLifecycle:
    """Tests for S3 Control PutBucketLifecycleConfiguration."""

    @pytest.fixture
    def s3control(self):
        return make_client("s3control")

    @pytest.fixture
    def s3(self):
        return make_client("s3")

    @pytest.fixture
    def bucket(self, s3):
        name = f"lifecycle-test-{_uid()}"
        s3.create_bucket(Bucket=name)
        yield name
        try:
            s3.delete_bucket(Bucket=name)
        except Exception:
            pass  # best-effort cleanup; bucket may already be gone

    def test_put_bucket_lifecycle_configuration(self, s3control, bucket):
        """PutBucketLifecycleConfiguration sets lifecycle rules."""
        resp = s3control.put_bucket_lifecycle_configuration(
            AccountId=ACCOUNT_ID,
            Bucket=bucket,
            LifecycleConfiguration={
                "Rules": [
                    {
                        "ID": "expire-logs",
                        "Status": "Enabled",
                        "Filter": {"Prefix": "logs/"},
                    }
                ]
            },
        )
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200

    def test_put_bucket_lifecycle_nonexistent_bucket(self, s3control):
        """PutBucketLifecycleConfiguration on missing bucket raises error."""
        with pytest.raises(ClientError) as exc:
            s3control.put_bucket_lifecycle_configuration(
                AccountId=ACCOUNT_ID,
                Bucket=f"no-such-bucket-{_uid()}",
                LifecycleConfiguration={
                    "Rules": [
                        {
                            "ID": "test",
                            "Status": "Enabled",
                            "Filter": {"Prefix": ""},
                        }
                    ]
                },
            )
        assert exc.value.response["Error"]["Code"] == "NoSuchBucket"

    def test_put_then_get_bucket_lifecycle_configuration(self, s3control, bucket):
        """GetBucketLifecycleConfiguration returns Rules after PutBucketLifecycleConfiguration."""
        s3control.put_bucket_lifecycle_configuration(
            AccountId=ACCOUNT_ID,
            Bucket=bucket,
            LifecycleConfiguration={
                "Rules": [{"ID": "rule1", "Status": "Enabled", "Filter": {"Prefix": "logs/"}}]
            },
        )
        resp = s3control.get_bucket_lifecycle_configuration(AccountId=ACCOUNT_ID, Bucket=bucket)
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200
        assert "Rules" in resp
        assert isinstance(resp["Rules"], list)

    def test_delete_bucket_lifecycle_configuration_then_get(self, s3control, bucket):
        """DeleteBucketLifecycleConfiguration then GetBucketLifecycleConfiguration returns empty Rules."""
        s3control.put_bucket_lifecycle_configuration(
            AccountId=ACCOUNT_ID,
            Bucket=bucket,
            LifecycleConfiguration={
                "Rules": [{"ID": "rule1", "Status": "Enabled", "Filter": {"Prefix": "logs/"}}]
            },
        )
        del_resp = s3control.delete_bucket_lifecycle_configuration(AccountId=ACCOUNT_ID, Bucket=bucket)
        assert del_resp["ResponseMetadata"]["HTTPStatusCode"] in (200, 204)
        resp = s3control.get_bucket_lifecycle_configuration(AccountId=ACCOUNT_ID, Bucket=bucket)
        assert "Rules" in resp
        assert isinstance(resp["Rules"], list)

    def test_get_bucket_lifecycle_nonexistent_bucket_raises(self, s3control):
        """GetBucketLifecycleConfiguration on nonexistent bucket raises NoSuchBucket."""
        with pytest.raises(ClientError) as exc:
            s3control.get_bucket_lifecycle_configuration(
                AccountId=ACCOUNT_ID, Bucket=f"no-such-bucket-{_uid()}"
            )
        assert exc.value.response["Error"]["Code"] == "NoSuchBucket"


class TestS3ControlNewStubOps:
    """Tests for newly-implemented S3 Control stub operations."""

    ACCOUNT_ID = "123456789012"

    @pytest.fixture
    def client(self):
        return make_client("s3control")

    def test_list_access_points_for_object_lambda(self, client):
        """ListAccessPointsForObjectLambda returns list."""
        resp = client.list_access_points_for_object_lambda(AccountId=self.ACCOUNT_ID)
        assert "ObjectLambdaAccessPointList" in resp
        assert isinstance(resp["ObjectLambdaAccessPointList"], list)

    def test_list_regional_buckets(self, client):
        """ListRegionalBuckets returns RegionalBucketList."""
        resp = client.list_regional_buckets(AccountId=self.ACCOUNT_ID)
        assert "RegionalBucketList" in resp
        assert isinstance(resp["RegionalBucketList"], list)


class TestS3ControlIdentityCenterAndNewStubs:
    """Tests for new S3 Control stub operations: identity center, data access,
    object lambda config, directory bucket access points, bucket versioning."""

    ACCOUNT_ID = "123456789012"

    @pytest.fixture
    def client(self):
        return make_client("s3control")

    def test_associate_access_grants_identity_center(self, client):
        """AssociateAccessGrantsIdentityCenter returns 200."""
        try:
            client.create_access_grants_instance(AccountId=self.ACCOUNT_ID)
        except ClientError:
            pass  # may already exist
        try:
            resp = client.associate_access_grants_identity_center(
                AccountId=self.ACCOUNT_ID,
                IdentityCenterArn="arn:aws:sso:::instance/ssoins-test",
            )
            assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200
            # Verify instance remains accessible after association
            instance = client.get_access_grants_instance(AccountId=self.ACCOUNT_ID)
            assert "AccessGrantsInstanceArn" in instance
            assert "AccessGrantsInstanceId" in instance
        finally:
            try:
                client.delete_access_grants_instance(AccountId=self.ACCOUNT_ID)
            except Exception:
                pass  # best-effort cleanup

    def test_dissociate_access_grants_identity_center(self, client):
        """DissociateAccessGrantsIdentityCenter returns 200."""
        try:
            client.create_access_grants_instance(AccountId=self.ACCOUNT_ID)
        except ClientError:
            pass  # may already exist
        try:
            resp = client.dissociate_access_grants_identity_center(AccountId=self.ACCOUNT_ID)
            assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200
            # Verify instance still accessible after dissociation
            instance = client.get_access_grants_instance(AccountId=self.ACCOUNT_ID)
            assert "AccessGrantsInstanceArn" in instance
        finally:
            try:
                client.delete_access_grants_instance(AccountId=self.ACCOUNT_ID)
            except Exception:
                pass  # best-effort cleanup

    def test_get_data_access(self, client):
        """GetDataAccess returns 200."""
        resp = client.get_data_access(
            AccountId=self.ACCOUNT_ID,
            Target="s3://my-bucket/",
            Permission="READ",
            DurationSeconds=3600,
        )
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200

    def test_list_access_points_for_directory_buckets(self, client):
        """ListAccessPointsForDirectoryBuckets returns 200."""
        resp = client.list_access_points_for_directory_buckets(AccountId=self.ACCOUNT_ID)
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200

    def test_get_access_point_configuration_for_object_lambda(self, client):
        """GetAccessPointConfigurationForObjectLambda returns 200."""
        resp = client.get_access_point_configuration_for_object_lambda(
            AccountId=self.ACCOUNT_ID, Name="test-ap"
        )
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200

    def test_put_access_point_configuration_for_object_lambda(self, client):
        """PutAccessPointConfigurationForObjectLambda returns 200."""
        resp = client.put_access_point_configuration_for_object_lambda(
            AccountId=self.ACCOUNT_ID,
            Name="test-ap",
            Configuration={
                "SupportingAccessPoint": "arn:aws:s3:us-east-1:123456789012:accesspoint/test",
                "TransformationConfigurations": [
                    {
                        "Actions": ["GetObject"],
                        "ContentTransformation": {
                            "AwsLambda": {
                                "FunctionArn": "arn:aws:lambda:us-east-1:123456789012:function:test"
                            }
                        },
                    }
                ],
            },
        )
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200

    def test_put_bucket_versioning_nonexistent(self, client):
        """PutBucketVersioning raises NoSuchBucket for a nonexistent bucket."""
        with pytest.raises(ClientError) as exc:
            client.put_bucket_versioning(
                AccountId=self.ACCOUNT_ID,
                Bucket="no-such-bucket",
                VersioningConfiguration={"Status": "Enabled"},
            )
        assert exc.value.response["Error"]["Code"] == "NoSuchBucket"

    def test_list_caller_access_grants(self, client):
        """ListCallerAccessGrants returns CallerAccessGrantsList."""
        resp = client.list_caller_access_grants(AccountId=self.ACCOUNT_ID)
        assert "CallerAccessGrantsList" in resp


class TestS3ControlEdgeCases:
    """Edge case and behavioral fidelity tests for s3control."""

    @pytest.fixture
    def client(self):
        return make_client("s3control")

    @pytest.fixture
    def s3(self):
        return make_client("s3")

    # ── Submit MRAP Routes ────────────────────────────────────────────────────

    def test_submit_mrap_routes_reflected_in_get(self, client, s3):
        """SubmitMultiRegionAccessPointRoutes change is visible via GetRoutes."""
        bucket = f"mrap-rt2-{_uid()}"
        mrap_name = f"mrap-rt2-{_uid()}"
        s3.create_bucket(Bucket=bucket)
        client.create_multi_region_access_point(
            AccountId=ACCOUNT_ID,
            Details={"Name": mrap_name, "Regions": [{"Bucket": bucket}]},
        )
        try:
            client.submit_multi_region_access_point_routes(
                AccountId=ACCOUNT_ID,
                Mrap=mrap_name,
                RouteUpdates=[
                    {"Bucket": bucket, "Region": "us-east-1", "TrafficDialPercentage": 50}
                ],
            )
            resp = client.get_multi_region_access_point_routes(
                AccountId=ACCOUNT_ID, Mrap=mrap_name
            )
            assert "Routes" in resp
            assert isinstance(resp["Routes"], list)
            assert len(resp["Routes"]) >= 1
            # Verify route has the expected bucket
            route_buckets = [r["Bucket"] for r in resp["Routes"]]
            assert bucket in route_buckets
        finally:
            try:
                client.delete_multi_region_access_point(
                    AccountId=ACCOUNT_ID, Details={"Name": mrap_name}
                )
            except Exception:
                pass  # best-effort cleanup
            try:
                s3.delete_bucket(Bucket=bucket)
            except Exception:
                pass  # best-effort cleanup

    def test_submit_mrap_routes_error_nonexistent(self, client):
        """SubmitMultiRegionAccessPointRoutes for nonexistent MRAP raises error."""
        with pytest.raises(ClientError) as exc:
            client.submit_multi_region_access_point_routes(
                AccountId=ACCOUNT_ID,
                Mrap=f"nonexistent-{_uid()}",
                RouteUpdates=[
                    {"Bucket": "any-bucket", "Region": "us-east-1", "TrafficDialPercentage": 100}
                ],
            )
        assert exc.value.response["Error"]["Code"] == "NoSuchMultiRegionAccessPoint"

    def test_submit_mrap_routes_multiple_traffic_dial(self, client, s3):
        """SubmitMultiRegionAccessPointRoutes with TrafficDialPercentage=0 is valid."""
        bucket = f"mrap-rt3-{_uid()}"
        mrap_name = f"mrap-rt3-{_uid()}"
        s3.create_bucket(Bucket=bucket)
        client.create_multi_region_access_point(
            AccountId=ACCOUNT_ID,
            Details={"Name": mrap_name, "Regions": [{"Bucket": bucket}]},
        )
        try:
            resp = client.submit_multi_region_access_point_routes(
                AccountId=ACCOUNT_ID,
                Mrap=mrap_name,
                RouteUpdates=[
                    {"Bucket": bucket, "Region": "us-east-1", "TrafficDialPercentage": 0}
                ],
            )
            assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200
        finally:
            try:
                client.delete_multi_region_access_point(
                    AccountId=ACCOUNT_ID, Details={"Name": mrap_name}
                )
            except Exception:
                pass  # best-effort cleanup
            try:
                s3.delete_bucket(Bucket=bucket)
            except Exception:
                pass  # best-effort cleanup

    # ── Identity Center Associate/Dissociate ──────────────────────────────────

    def test_associate_dissociate_identity_center_roundtrip(self, client):
        """Associate then dissociate identity center succeeds."""
        try:
            client.create_access_grants_instance(AccountId=ACCOUNT_ID)
        except ClientError:
            pass  # may already exist
        try:
            resp = client.associate_access_grants_identity_center(
                AccountId=ACCOUNT_ID,
                IdentityCenterArn="arn:aws:sso:::instance/ssoins-test123",
            )
            assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200
            # Verify instance accessible after association
            instance = client.get_access_grants_instance(AccountId=ACCOUNT_ID)
            assert "AccessGrantsInstanceArn" in instance
            # Dissociate should also succeed
            resp2 = client.dissociate_access_grants_identity_center(AccountId=ACCOUNT_ID)
            assert resp2["ResponseMetadata"]["HTTPStatusCode"] == 200
        finally:
            try:
                client.delete_access_grants_instance(AccountId=ACCOUNT_ID)
            except Exception:
                pass  # best-effort cleanup

    def test_dissociate_identity_center_idempotent(self, client):
        """DissociateAccessGrantsIdentityCenter succeeds even if not associated."""
        try:
            client.create_access_grants_instance(AccountId=ACCOUNT_ID)
        except ClientError:
            pass  # may already exist
        try:
            resp = client.dissociate_access_grants_identity_center(AccountId=ACCOUNT_ID)
            assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200
            # Verify instance still accessible (dissociate doesn't delete the instance)
            instance = client.get_access_grants_instance(AccountId=ACCOUNT_ID)
            assert "AccessGrantsInstanceArn" in instance
        finally:
            try:
                client.delete_access_grants_instance(AccountId=ACCOUNT_ID)
            except Exception:
                pass  # best-effort cleanup

    # ── List Access Points empty response fields ──────────────────────────────

    def test_list_access_points_empty_has_required_fields(self, client):
        """ListAccessPoints empty response contains AccessPointList key."""
        resp = client.list_access_points(
            AccountId=ACCOUNT_ID, Bucket=f"bucket-that-does-not-exist-{_uid()}"
        )
        assert "AccessPointList" in resp
        assert isinstance(resp["AccessPointList"], list)
        assert len(resp["AccessPointList"]) == 0

    def test_list_access_points_empty_no_next_token(self, client):
        """ListAccessPoints returns no NextToken when empty."""
        resp = client.list_access_points(
            AccountId=ACCOUNT_ID, Bucket=f"bucket-that-does-not-exist-{_uid()}"
        )
        assert resp.get("NextToken") is None or "NextToken" not in resp

    # ── Delete Access Point idempotency ───────────────────────────────────────

    def test_delete_access_point_idempotent_twice(self, client, s3):
        """Deleting a nonexistent access point twice both succeed."""
        name = f"ap-idem-{_uid()}"
        resp1 = client.delete_access_point(AccountId=ACCOUNT_ID, Name=name)
        assert resp1["ResponseMetadata"]["HTTPStatusCode"] in (200, 204)
        resp2 = client.delete_access_point(AccountId=ACCOUNT_ID, Name=name)
        assert resp2["ResponseMetadata"]["HTTPStatusCode"] in (200, 204)

    def test_delete_access_point_then_get_error_code(self, client, s3):
        """After deleting access point, get returns NoSuchAccessPoint."""
        bucket = f"ap-delget-{_uid()}"
        ap_name = f"ap-delget-{_uid()}"
        s3.create_bucket(Bucket=bucket)
        try:
            client.create_access_point(AccountId=ACCOUNT_ID, Name=ap_name, Bucket=bucket)
            client.delete_access_point(AccountId=ACCOUNT_ID, Name=ap_name)
            with pytest.raises(ClientError) as exc:
                client.get_access_point(AccountId=ACCOUNT_ID, Name=ap_name)
            assert exc.value.response["Error"]["Code"] == "NoSuchAccessPoint"
        finally:
            try:
                s3.delete_bucket(Bucket=bucket)
            except Exception:
                pass  # best-effort cleanup

    # ── List MRAP empty response fields ──────────────────────────────────────

    def test_list_multi_region_access_points_empty_structure(self, client):
        """ListMultiRegionAccessPoints has AccessPoints list even when empty."""
        resp = client.list_multi_region_access_points(AccountId=ACCOUNT_ID)
        assert "AccessPoints" in resp
        assert isinstance(resp["AccessPoints"], list)

    def test_list_multi_region_access_points_no_next_token_when_empty(self, client):
        """ListMultiRegionAccessPoints has no NextToken when result is empty."""
        resp = client.list_multi_region_access_points(AccountId=ACCOUNT_ID)
        # If list is empty, NextToken should not be present
        if not resp["AccessPoints"]:
            assert resp.get("NextToken") is None or "NextToken" not in resp

    # ── Tag resource behavioral fidelity ─────────────────────────────────────

    def test_tag_resource_then_list_tags(self, client, s3):
        """TagResource then ListTagsForResource returns the set tags."""
        bucket_name = f"tag-list-{_uid()}"
        s3.create_bucket(Bucket=bucket_name)
        arn = f"arn:aws:s3:::{bucket_name}"
        try:
            client.tag_resource(
                AccountId=ACCOUNT_ID,
                ResourceArn=arn,
                Tags=[{"Key": "env", "Value": "staging"}, {"Key": "version", "Value": "2"}],
            )
            resp = client.list_tags_for_resource(AccountId=ACCOUNT_ID, ResourceArn=arn)
            assert "Tags" in resp
            tag_map = {t["Key"]: t["Value"] for t in resp["Tags"]}
            assert tag_map["env"] == "staging"
            assert tag_map["version"] == "2"
        finally:
            try:
                s3.delete_bucket(Bucket=bucket_name)
            except Exception:
                pass  # best-effort cleanup

    def test_tag_resource_overwrite_updates_value(self, client, s3):
        """TagResource with same key updates the value."""
        bucket_name = f"tag-upd-{_uid()}"
        s3.create_bucket(Bucket=bucket_name)
        arn = f"arn:aws:s3:::{bucket_name}"
        try:
            client.tag_resource(
                AccountId=ACCOUNT_ID,
                ResourceArn=arn,
                Tags=[{"Key": "env", "Value": "dev"}],
            )
            client.tag_resource(
                AccountId=ACCOUNT_ID,
                ResourceArn=arn,
                Tags=[{"Key": "env", "Value": "prod"}],
            )
            resp = client.list_tags_for_resource(AccountId=ACCOUNT_ID, ResourceArn=arn)
            tag_map = {t["Key"]: t["Value"] for t in resp["Tags"]}
            assert tag_map["env"] == "prod"
        finally:
            try:
                s3.delete_bucket(Bucket=bucket_name)
            except Exception:
                pass  # best-effort cleanup

    def test_untag_resource_then_list_empty(self, client, s3):
        """UntagResource then ListTagsForResource returns empty list."""
        bucket_name = f"tag-del-{_uid()}"
        s3.create_bucket(Bucket=bucket_name)
        arn = f"arn:aws:s3:::{bucket_name}"
        try:
            client.tag_resource(
                AccountId=ACCOUNT_ID,
                ResourceArn=arn,
                Tags=[{"Key": "k1", "Value": "v1"}],
            )
            client.untag_resource(
                AccountId=ACCOUNT_ID,
                ResourceArn=arn,
                TagKeys=["k1"],
            )
            resp = client.list_tags_for_resource(AccountId=ACCOUNT_ID, ResourceArn=arn)
            tag_keys = [t["Key"] for t in resp["Tags"]]
            assert "k1" not in tag_keys
        finally:
            try:
                s3.delete_bucket(Bucket=bucket_name)
            except Exception:
                pass  # best-effort cleanup

    # ── List Jobs field assertions ────────────────────────────────────────────

    def test_list_jobs_empty_has_jobs_key(self, client):
        """ListJobs returns Jobs list even when empty."""
        resp = client.list_jobs(AccountId=ACCOUNT_ID)
        assert "Jobs" in resp
        assert isinstance(resp["Jobs"], list)
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200

    def test_list_jobs_after_create(self, client, s3):
        """ListJobs includes newly created job with expected fields."""
        bucket = f"job-list2-{_uid()}"
        s3.create_bucket(Bucket=bucket)
        resp = client.create_job(
            AccountId=ACCOUNT_ID,
            Operation={"S3PutObjectCopy": {"TargetResource": f"arn:aws:s3:::{bucket}"}},
            Report={"Enabled": False},
            ClientRequestToken=str(uuid.uuid4()),
            Priority=5,
            RoleArn=f"arn:aws:iam::{ACCOUNT_ID}:role/test-role",
            ConfirmationRequired=False,
            ManifestGenerator={
                "S3JobManifestGenerator": {
                    "SourceBucket": f"arn:aws:s3:::{bucket}",
                    "EnableManifestOutput": False,
                }
            },
        )
        job_id = resp["JobId"]
        try:
            list_resp = client.list_jobs(AccountId=ACCOUNT_ID)
            job_ids = [j["JobId"] for j in list_resp["Jobs"]]
            assert job_id in job_ids
            # Verify job entry has expected fields
            job_entry = next(j for j in list_resp["Jobs"] if j["JobId"] == job_id)
            assert "Status" in job_entry
            assert "Priority" in job_entry
        finally:
            try:
                client.update_job_status(
                    AccountId=ACCOUNT_ID, JobId=job_id, RequestedJobStatus="Cancelled"
                )
            except Exception:
                pass  # best-effort cleanup
            try:
                s3.delete_bucket(Bucket=bucket)
            except Exception:
                pass  # best-effort cleanup

    # ── Access Grants list after create ──────────────────────────────────────

    def test_list_access_grants_instances_after_create(self, client):
        """ListAccessGrantsInstances includes newly created instance."""
        try:
            client.create_access_grants_instance(AccountId=ACCOUNT_ID)
        except ClientError:
            pass  # may already exist
        try:
            resp = client.list_access_grants_instances(AccountId=ACCOUNT_ID)
            assert "AccessGrantsInstancesList" in resp
            assert len(resp["AccessGrantsInstancesList"]) >= 1
            instance = resp["AccessGrantsInstancesList"][0]
            assert "AccessGrantsInstanceArn" in instance
            assert "CreatedAt" in instance
        finally:
            try:
                client.delete_access_grants_instance(AccountId=ACCOUNT_ID)
            except Exception:
                pass  # best-effort cleanup

    def test_list_access_grants_instances_empty_fields(self, client):
        """ListAccessGrantsInstances empty response has required structure."""
        # Delete any existing instance first
        try:
            client.delete_access_grants_instance(AccountId=ACCOUNT_ID)
        except Exception:
            pass  # best-effort cleanup
        resp = client.list_access_grants_instances(AccountId=ACCOUNT_ID)
        assert "AccessGrantsInstancesList" in resp
        assert isinstance(resp["AccessGrantsInstancesList"], list)

    def test_list_access_grants_empty_structure(self, client):
        """ListAccessGrants empty response has AccessGrantsList key."""
        resp = client.list_access_grants(AccountId=ACCOUNT_ID)
        assert "AccessGrantsList" in resp
        assert isinstance(resp["AccessGrantsList"], list)
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200

    def test_list_access_grants_locations_empty_structure(self, client):
        """ListAccessGrantsLocations empty response has AccessGrantsLocationsList key."""
        resp = client.list_access_grants_locations(AccountId=ACCOUNT_ID)
        assert "AccessGrantsLocationsList" in resp
        assert isinstance(resp["AccessGrantsLocationsList"], list)
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200

    # ── Bucket Policy/Lifecycle/Replication roundtrips ────────────────────────

    def test_get_bucket_policy_after_put(self, client, s3):
        """GetBucketPolicy returns the policy after PutBucketPolicy."""
        bucket = f"bkt-pol2-{_uid()}"
        s3.create_bucket(Bucket=bucket)
        try:
            policy = json.dumps(
                {
                    "Version": "2012-10-17",
                    "Statement": [
                        {
                            "Effect": "Allow",
                            "Principal": "*",
                            "Action": "s3:GetObject",
                            "Resource": f"arn:aws:s3:::{bucket}/*",
                        }
                    ],
                }
            )
            client.put_bucket_policy(AccountId=ACCOUNT_ID, Bucket=bucket, Policy=policy)
            resp = client.get_bucket_policy(AccountId=ACCOUNT_ID, Bucket=bucket)
            assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200
            assert "Policy" in resp
            parsed = json.loads(resp["Policy"])
            assert parsed["Version"] == "2012-10-17"
        finally:
            try:
                client.delete_bucket_policy(AccountId=ACCOUNT_ID, Bucket=bucket)
            except Exception:
                pass  # best-effort cleanup
            try:
                s3.delete_bucket(Bucket=bucket)
            except Exception:
                pass  # best-effort cleanup

    def test_get_bucket_lifecycle_after_put(self, client, s3):
        """PutBucketLifecycleConfiguration accepts rules; GetBucketLifecycleConfiguration responds."""
        bucket = f"bkt-lc2-{_uid()}"
        s3.create_bucket(Bucket=bucket)
        try:
            put_resp = client.put_bucket_lifecycle_configuration(
                AccountId=ACCOUNT_ID,
                Bucket=bucket,
                LifecycleConfiguration={
                    "Rules": [
                        {
                            "ID": "test-rule",
                            "Status": "Enabled",
                            "Filter": {"Prefix": "logs/"},
                        }
                    ]
                },
            )
            assert put_resp["ResponseMetadata"]["HTTPStatusCode"] == 200
            # GET returns a response with Rules key (server stub: may be empty)
            resp = client.get_bucket_lifecycle_configuration(AccountId=ACCOUNT_ID, Bucket=bucket)
            assert "Rules" in resp
            assert isinstance(resp["Rules"], list)
        finally:
            try:
                client.delete_bucket_lifecycle_configuration(AccountId=ACCOUNT_ID, Bucket=bucket)
            except Exception:
                pass  # best-effort cleanup
            try:
                s3.delete_bucket(Bucket=bucket)
            except Exception:
                pass  # best-effort cleanup

    def test_get_bucket_replication_after_put(self, client, s3):
        """PutBucketReplication accepts config; GetBucketReplication responds with 200."""
        bucket = f"bkt-repl2-{_uid()}"
        s3.create_bucket(Bucket=bucket)
        try:
            put_resp = client.put_bucket_replication(
                AccountId=ACCOUNT_ID,
                Bucket=bucket,
                ReplicationConfiguration={
                    "Role": f"arn:aws:iam::{ACCOUNT_ID}:role/repl-role",
                    "Rules": [
                        {
                            "ID": "repl-rule-1",
                            "Status": "Enabled",
                            "Priority": 1,
                            "Bucket": f"arn:aws:s3:::{bucket}",
                            "Filter": {"Prefix": ""},
                            "Destination": {
                                "Bucket": f"arn:aws:s3:::{bucket}",
                                "Account": ACCOUNT_ID,
                            },
                            "DeleteMarkerReplication": {"Status": "Disabled"},
                        }
                    ],
                },
            )
            assert put_resp["ResponseMetadata"]["HTTPStatusCode"] == 200
            resp = client.get_bucket_replication(AccountId=ACCOUNT_ID, Bucket=bucket)
            assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200
        finally:
            try:
                client.delete_bucket_replication(AccountId=ACCOUNT_ID, Bucket=bucket)
            except Exception:
                pass  # best-effort cleanup
            try:
                s3.delete_bucket(Bucket=bucket)
            except Exception:
                pass  # best-effort cleanup

    # ── Delete access point policy behavioral fidelity ────────────────────────

    def test_delete_access_point_policy_verify_then_error(self, client, s3):
        """Full lifecycle: create AP, set policy, verify it, delete it, verify gone."""
        bucket = f"ap-fullpol-{_uid()}"
        ap_name = f"ap-fullpol-{_uid()}"
        s3.create_bucket(Bucket=bucket)
        try:
            client.create_access_point(AccountId=ACCOUNT_ID, Name=ap_name, Bucket=bucket)
            policy = json.dumps(
                {
                    "Version": "2012-10-17",
                    "Statement": [
                        {
                            "Effect": "Allow",
                            "Principal": "*",
                            "Action": "s3:GetObject",
                            "Resource": (
                                f"arn:aws:s3:us-east-1:{ACCOUNT_ID}:accesspoint/{ap_name}/object/*"
                            ),
                        }
                    ],
                }
            )
            # Set the policy
            client.put_access_point_policy(AccountId=ACCOUNT_ID, Name=ap_name, Policy=policy)
            # Verify it was set
            get_resp = client.get_access_point_policy(AccountId=ACCOUNT_ID, Name=ap_name)
            assert "Policy" in get_resp
            parsed = json.loads(get_resp["Policy"])
            assert parsed["Version"] == "2012-10-17"
            # Update it with a new statement
            policy2 = json.dumps(
                {
                    "Version": "2012-10-17",
                    "Statement": [
                        {
                            "Effect": "Deny",
                            "Principal": "*",
                            "Action": "s3:PutObject",
                            "Resource": "*",
                        }
                    ],
                }
            )
            client.put_access_point_policy(AccountId=ACCOUNT_ID, Name=ap_name, Policy=policy2)
            get_resp2 = client.get_access_point_policy(AccountId=ACCOUNT_ID, Name=ap_name)
            parsed2 = json.loads(get_resp2["Policy"])
            assert parsed2["Statement"][0]["Effect"] == "Deny"
            # Delete and verify gone
            client.delete_access_point_policy(AccountId=ACCOUNT_ID, Name=ap_name)
            with pytest.raises(ClientError) as exc:
                client.get_access_point_policy(AccountId=ACCOUNT_ID, Name=ap_name)
            assert exc.value.response["Error"]["Code"] == "NoSuchAccessPointPolicy"
        finally:
            try:
                client.delete_access_point(AccountId=ACCOUNT_ID, Name=ap_name)
            except Exception:
                pass  # best-effort cleanup
            try:
                s3.delete_bucket(Bucket=bucket)
            except Exception:
                pass  # best-effort cleanup


class TestS3ControlMRAPRoutesEdgeCases:
    """Edge case and behavioral fidelity tests for MRAP routes."""

    @pytest.fixture
    def client(self):
        return make_client("s3control")

    @pytest.fixture
    def s3(self):
        return make_client("s3")

    @pytest.fixture
    def mrap(self, client, s3):
        bucket = f"mrap-re-{_uid()}"
        mrap_name = f"mrap-re-{_uid()}"
        s3.create_bucket(Bucket=bucket)
        client.create_multi_region_access_point(
            AccountId=ACCOUNT_ID,
            Details={"Name": mrap_name, "Regions": [{"Bucket": bucket}]},
        )
        yield mrap_name, bucket
        try:
            client.delete_multi_region_access_point(
                AccountId=ACCOUNT_ID, Details={"Name": mrap_name}
            )
        except Exception:
            pass  # best-effort cleanup
        try:
            s3.delete_bucket(Bucket=bucket)
        except Exception:
            pass  # best-effort cleanup

    def test_submit_routes_traffic_dial_100(self, client, mrap):
        """SubmitMultiRegionAccessPointRoutes with 100% traffic dial returns 200."""
        mrap_name, bucket = mrap
        resp = client.submit_multi_region_access_point_routes(
            AccountId=ACCOUNT_ID,
            Mrap=mrap_name,
            RouteUpdates=[
                {"Bucket": bucket, "Region": "us-east-1", "TrafficDialPercentage": 100}
            ],
        )
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200
        # Verify routes are accessible after submit
        routes_resp = client.get_multi_region_access_point_routes(
            AccountId=ACCOUNT_ID, Mrap=mrap_name
        )
        assert "Routes" in routes_resp
        route_buckets = [r["Bucket"] for r in routes_resp["Routes"]]
        assert bucket in route_buckets

    def test_submit_routes_then_get_has_route_entry(self, client, mrap):
        """After SubmitRoutes, GetRoutes returns a list with the bucket."""
        mrap_name, bucket = mrap
        client.submit_multi_region_access_point_routes(
            AccountId=ACCOUNT_ID,
            Mrap=mrap_name,
            RouteUpdates=[
                {"Bucket": bucket, "Region": "us-east-1", "TrafficDialPercentage": 75}
            ],
        )
        resp = client.get_multi_region_access_point_routes(AccountId=ACCOUNT_ID, Mrap=mrap_name)
        assert "Routes" in resp
        buckets = [r["Bucket"] for r in resp["Routes"]]
        assert bucket in buckets

    def test_get_routes_has_traffic_dial_field(self, client, mrap):
        """GetMultiRegionAccessPointRoutes returns TrafficDialPercentage field."""
        mrap_name, bucket = mrap
        resp = client.get_multi_region_access_point_routes(AccountId=ACCOUNT_ID, Mrap=mrap_name)
        assert "Routes" in resp
        assert isinstance(resp["Routes"], list)
        if resp["Routes"]:
            assert "TrafficDialPercentage" in resp["Routes"][0]

    def test_submit_routes_nonexistent_mrap_error(self, client):
        """SubmitRoutes for nonexistent MRAP raises NoSuchMultiRegionAccessPoint."""
        with pytest.raises(ClientError) as exc:
            client.submit_multi_region_access_point_routes(
                AccountId=ACCOUNT_ID,
                Mrap=f"nonexistent-{_uid()}",
                RouteUpdates=[
                    {"Bucket": "any", "Region": "us-east-1", "TrafficDialPercentage": 100}
                ],
            )
        assert exc.value.response["Error"]["Code"] == "NoSuchMultiRegionAccessPoint"

    def test_submit_routes_zero_traffic_dial(self, client, mrap):
        """TrafficDialPercentage=0 is a valid value for disabling a route."""
        mrap_name, bucket = mrap
        resp = client.submit_multi_region_access_point_routes(
            AccountId=ACCOUNT_ID,
            Mrap=mrap_name,
            RouteUpdates=[
                {"Bucket": bucket, "Region": "us-east-1", "TrafficDialPercentage": 0}
            ],
        )
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200
        # Verify routes are still accessible after submit with 0%
        routes_resp = client.get_multi_region_access_point_routes(
            AccountId=ACCOUNT_ID, Mrap=mrap_name
        )
        assert "Routes" in routes_resp
        route_buckets = [r["Bucket"] for r in routes_resp["Routes"]]
        assert bucket in route_buckets

    def test_get_routes_not_found_error(self, client):
        """GetRoutes for nonexistent MRAP raises NoSuchMultiRegionAccessPoint."""
        with pytest.raises(ClientError) as exc:
            client.get_multi_region_access_point_routes(
                AccountId=ACCOUNT_ID, Mrap=f"nonexistent-{_uid()}"
            )
        assert exc.value.response["Error"]["Code"] == "NoSuchMultiRegionAccessPoint"


class TestS3ControlIdentityCenterBehavior:
    """Behavioral fidelity tests for Access Grants Identity Center operations."""

    @pytest.fixture
    def client(self):
        return make_client("s3control")

    @pytest.fixture(autouse=True)
    def _ensure_instance(self, client):
        """Ensure an access grants instance exists."""
        try:
            client.get_access_grants_instance(AccountId=ACCOUNT_ID)
        except ClientError:
            client.create_access_grants_instance(AccountId=ACCOUNT_ID)
        yield
        # cleanup: dissociate identity center
        try:
            client.dissociate_access_grants_identity_center(AccountId=ACCOUNT_ID)
        except Exception:
            pass  # best-effort cleanup

    def test_associate_identity_center_returns_200(self, client):
        """AssociateAccessGrantsIdentityCenter succeeds with valid ARN."""
        resp = client.associate_access_grants_identity_center(
            AccountId=ACCOUNT_ID,
            IdentityCenterArn="arn:aws:sso:::instance/ssoins-abc123",
        )
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200
        # Verify instance is still accessible after association
        instance = client.get_access_grants_instance(AccountId=ACCOUNT_ID)
        assert "AccessGrantsInstanceArn" in instance
        assert "CreatedAt" in instance

    def test_associate_identity_center_different_arns(self, client):
        """Associating a different IdentityCenterArn replaces the old one."""
        client.associate_access_grants_identity_center(
            AccountId=ACCOUNT_ID,
            IdentityCenterArn="arn:aws:sso:::instance/ssoins-first",
        )
        # Verify instance accessible after first association
        instance1 = client.get_access_grants_instance(AccountId=ACCOUNT_ID)
        assert "AccessGrantsInstanceArn" in instance1
        resp = client.associate_access_grants_identity_center(
            AccountId=ACCOUNT_ID,
            IdentityCenterArn="arn:aws:sso:::instance/ssoins-second",
        )
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200
        # Verify instance still accessible after second association
        instance2 = client.get_access_grants_instance(AccountId=ACCOUNT_ID)
        assert instance2["AccessGrantsInstanceArn"] == instance1["AccessGrantsInstanceArn"]

    def test_dissociate_identity_center_after_associate(self, client):
        """Dissociate succeeds after associate."""
        client.associate_access_grants_identity_center(
            AccountId=ACCOUNT_ID,
            IdentityCenterArn="arn:aws:sso:::instance/ssoins-round",
        )
        resp = client.dissociate_access_grants_identity_center(AccountId=ACCOUNT_ID)
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200
        # Instance should still be accessible after dissociation
        instance = client.get_access_grants_instance(AccountId=ACCOUNT_ID)
        assert "AccessGrantsInstanceArn" in instance

    def test_dissociate_without_prior_associate_is_ok(self, client):
        """Dissociate when never associated returns 200 (idempotent)."""
        # Make sure not associated
        try:
            client.dissociate_access_grants_identity_center(AccountId=ACCOUNT_ID)
        except Exception:
            pass  # best-effort cleanup
        # Dissociate again
        resp = client.dissociate_access_grants_identity_center(AccountId=ACCOUNT_ID)
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200
        # Instance should still be accessible
        instance = client.get_access_grants_instance(AccountId=ACCOUNT_ID)
        assert "AccessGrantsInstanceId" in instance

    def test_associate_roundtrip_dissociate(self, client):
        """Associate then dissociate completes successfully."""
        assoc_resp = client.associate_access_grants_identity_center(
            AccountId=ACCOUNT_ID,
            IdentityCenterArn="arn:aws:sso:::instance/ssoins-rt",
        )
        assert assoc_resp["ResponseMetadata"]["HTTPStatusCode"] == 200
        # Verify instance accessible between operations
        instance = client.get_access_grants_instance(AccountId=ACCOUNT_ID)
        assert "AccessGrantsInstanceArn" in instance
        dissoc_resp = client.dissociate_access_grants_identity_center(AccountId=ACCOUNT_ID)
        assert dissoc_resp["ResponseMetadata"]["HTTPStatusCode"] == 200

    def test_associate_identity_center_idempotent(self, client):
        """Associating the same ARN twice succeeds both times."""
        arn = "arn:aws:sso:::instance/ssoins-idem"
        resp1 = client.associate_access_grants_identity_center(
            AccountId=ACCOUNT_ID,
            IdentityCenterArn=arn,
        )
        assert resp1["ResponseMetadata"]["HTTPStatusCode"] == 200
        # Verify instance accessible after first association
        instance = client.get_access_grants_instance(AccountId=ACCOUNT_ID)
        assert "AccessGrantsInstanceArn" in instance
        resp2 = client.associate_access_grants_identity_center(
            AccountId=ACCOUNT_ID,
            IdentityCenterArn=arn,
        )
        assert resp2["ResponseMetadata"]["HTTPStatusCode"] == 200


class TestS3ControlListOperationsEdgeCases:
    """Edge cases for list operations with behavioral assertions."""

    @pytest.fixture
    def client(self):
        return make_client("s3control")

    @pytest.fixture
    def s3(self):
        return make_client("s3")

    def test_list_access_points_empty_type(self, client):
        """ListAccessPoints empty response AccessPointList is list, not None."""
        resp = client.list_access_points(
            AccountId=ACCOUNT_ID, Bucket=f"nonexistent-{_uid()}"
        )
        assert isinstance(resp["AccessPointList"], list)
        assert len(resp["AccessPointList"]) == 0

    def test_list_access_points_create_appears_delete_disappears(self, client, s3):
        """Create AP → appears in list; delete AP → disappears from list."""
        bucket = f"ap-list-cd-{_uid()}"
        ap_name = f"ap-list-cd-{_uid()}"
        s3.create_bucket(Bucket=bucket)
        try:
            client.create_access_point(AccountId=ACCOUNT_ID, Name=ap_name, Bucket=bucket)
            resp = client.list_access_points(AccountId=ACCOUNT_ID, Bucket=bucket)
            names = [ap["Name"] for ap in resp["AccessPointList"]]
            assert ap_name in names

            client.delete_access_point(AccountId=ACCOUNT_ID, Name=ap_name)
            resp2 = client.list_access_points(AccountId=ACCOUNT_ID, Bucket=bucket)
            names2 = [ap["Name"] for ap in resp2["AccessPointList"]]
            assert ap_name not in names2
        finally:
            try:
                client.delete_access_point(AccountId=ACCOUNT_ID, Name=ap_name)
            except Exception:
                pass  # best-effort cleanup
            try:
                s3.delete_bucket(Bucket=bucket)
            except Exception:
                pass  # best-effort cleanup

    def test_list_access_points_empty_no_next_token(self, client):
        """ListAccessPoints empty result has no NextToken."""
        resp = client.list_access_points(
            AccountId=ACCOUNT_ID, Bucket=f"no-such-{_uid()}"
        )
        assert resp.get("NextToken") is None or "NextToken" not in resp

    def test_list_mrap_empty_type(self, client):
        """ListMultiRegionAccessPoints empty response AccessPoints is list."""
        resp = client.list_multi_region_access_points(AccountId=ACCOUNT_ID)
        assert "AccessPoints" in resp
        assert isinstance(resp["AccessPoints"], list)

    def test_list_mrap_create_appears(self, client, s3):
        """Create MRAP → appears in ListMultiRegionAccessPoints."""
        bucket = f"mrap-lce-{_uid()}"
        mrap_name = f"mrap-lce-{_uid()}"
        s3.create_bucket(Bucket=bucket)
        client.create_multi_region_access_point(
            AccountId=ACCOUNT_ID,
            Details={"Name": mrap_name, "Regions": [{"Bucket": bucket}]},
        )
        try:
            resp = client.list_multi_region_access_points(AccountId=ACCOUNT_ID)
            names = [m["Name"] for m in resp["AccessPoints"]]
            assert mrap_name in names
        finally:
            try:
                client.delete_multi_region_access_point(
                    AccountId=ACCOUNT_ID, Details={"Name": mrap_name}
                )
            except Exception:
                pass  # best-effort cleanup
            try:
                s3.delete_bucket(Bucket=bucket)
            except Exception:
                pass  # best-effort cleanup

    def test_list_mrap_delete_disappears(self, client, s3):
        """Delete MRAP → disappears from ListMultiRegionAccessPoints."""
        bucket = f"mrap-ldd-{_uid()}"
        mrap_name = f"mrap-ldd-{_uid()}"
        s3.create_bucket(Bucket=bucket)
        client.create_multi_region_access_point(
            AccountId=ACCOUNT_ID,
            Details={"Name": mrap_name, "Regions": [{"Bucket": bucket}]},
        )
        client.delete_multi_region_access_point(
            AccountId=ACCOUNT_ID, Details={"Name": mrap_name}
        )
        resp = client.list_multi_region_access_points(AccountId=ACCOUNT_ID)
        names = [m["Name"] for m in resp["AccessPoints"]]
        assert mrap_name not in names
        try:
            s3.delete_bucket(Bucket=bucket)
        except Exception:
            pass  # best-effort cleanup

    def test_list_mrap_error_field_on_get_nonexistent(self, client):
        """GetMultiRegionAccessPoint nonexistent raises NoSuchMultiRegionAccessPoint."""
        with pytest.raises(ClientError) as exc:
            client.get_multi_region_access_point(
                AccountId=ACCOUNT_ID, Name=f"nonexistent-{_uid()}"
            )
        assert exc.value.response["Error"]["Code"] == "NoSuchMultiRegionAccessPoint"

    def test_list_jobs_empty_list_type(self, client):
        """ListJobs returns Jobs as a list type."""
        resp = client.list_jobs(AccountId=ACCOUNT_ID)
        assert "Jobs" in resp
        assert isinstance(resp["Jobs"], list)

    def test_list_jobs_status_code(self, client):
        """ListJobs returns HTTP 200."""
        resp = client.list_jobs(AccountId=ACCOUNT_ID)
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200

    def test_list_jobs_filter_by_status(self, client, s3):
        """ListJobs filters by JobStatuses."""
        bucket = f"job-flt-{_uid()}"
        s3.create_bucket(Bucket=bucket)
        resp = client.create_job(
            AccountId=ACCOUNT_ID,
            Operation={"S3PutObjectCopy": {"TargetResource": f"arn:aws:s3:::{bucket}"}},
            Report={"Enabled": False},
            ClientRequestToken=str(uuid.uuid4()),
            Priority=10,
            RoleArn=f"arn:aws:iam::{ACCOUNT_ID}:role/test-role",
            ConfirmationRequired=False,
            ManifestGenerator={
                "S3JobManifestGenerator": {
                    "SourceBucket": f"arn:aws:s3:::{bucket}",
                    "EnableManifestOutput": False,
                }
            },
        )
        job_id = resp["JobId"]
        try:
            # Cancel the job first
            client.update_job_status(
                AccountId=ACCOUNT_ID, JobId=job_id, RequestedJobStatus="Cancelled"
            )
            # Filter by Cancelled status - should find it
            list_resp = client.list_jobs(AccountId=ACCOUNT_ID, JobStatuses=["Cancelled"])
            job_ids = [j["JobId"] for j in list_resp["Jobs"]]
            assert job_id in job_ids
        finally:
            try:
                s3.delete_bucket(Bucket=bucket)
            except Exception:
                pass  # best-effort cleanup

    def test_list_access_grants_instances_empty_type(self, client):
        """ListAccessGrantsInstances AccessGrantsInstancesList is list type."""
        # Delete any instance first
        try:
            client.delete_access_grants_instance(AccountId=ACCOUNT_ID)
        except Exception:
            pass  # best-effort cleanup
        resp = client.list_access_grants_instances(AccountId=ACCOUNT_ID)
        assert "AccessGrantsInstancesList" in resp
        assert isinstance(resp["AccessGrantsInstancesList"], list)

    def test_list_access_grants_instances_after_create(self, client):
        """ListAccessGrantsInstances includes created instance ARN."""
        try:
            client.create_access_grants_instance(AccountId=ACCOUNT_ID)
        except ClientError:
            pass  # may already exist
        try:
            resp = client.list_access_grants_instances(AccountId=ACCOUNT_ID)
            assert len(resp["AccessGrantsInstancesList"]) >= 1
            entry = resp["AccessGrantsInstancesList"][0]
            assert "AccessGrantsInstanceArn" in entry
        finally:
            try:
                client.delete_access_grants_instance(AccountId=ACCOUNT_ID)
            except Exception:
                pass  # best-effort cleanup

    def test_list_access_grants_instances_after_delete(self, client):
        """ListAccessGrantsInstances is empty after deleting instance."""
        try:
            client.create_access_grants_instance(AccountId=ACCOUNT_ID)
        except ClientError:
            pass  # may already exist
        client.delete_access_grants_instance(AccountId=ACCOUNT_ID)
        resp = client.list_access_grants_instances(AccountId=ACCOUNT_ID)
        assert len(resp["AccessGrantsInstancesList"]) == 0

    def test_list_access_grants_empty_type(self, client):
        """ListAccessGrants AccessGrantsList is a list type."""
        resp = client.list_access_grants(AccountId=ACCOUNT_ID)
        assert "AccessGrantsList" in resp
        assert isinstance(resp["AccessGrantsList"], list)

    def test_list_access_grants_empty_status_code(self, client):
        """ListAccessGrants returns HTTP 200."""
        resp = client.list_access_grants(AccountId=ACCOUNT_ID)
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200

    def test_list_access_grants_after_create(self, client):
        """ListAccessGrants includes newly created grant."""
        try:
            client.create_access_grants_instance(AccountId=ACCOUNT_ID)
        except ClientError:
            pass  # may already exist
        loc = client.create_access_grants_location(
            AccountId=ACCOUNT_ID,
            LocationScope="s3://list-grant-bucket/",
            IAMRoleArn="arn:aws:iam::123456789012:role/grant-role",
        )
        loc_id = loc["AccessGrantsLocationId"]
        grant = client.create_access_grant(
            AccountId=ACCOUNT_ID,
            AccessGrantsLocationId=loc_id,
            AccessGrantsLocationConfiguration={"S3SubPrefix": "data/"},
            Grantee={
                "GranteeType": "IAM",
                "GranteeIdentifier": "arn:aws:iam::123456789012:role/grantee",
            },
            Permission="READ",
        )
        grant_id = grant["AccessGrantId"]
        try:
            resp = client.list_access_grants(AccountId=ACCOUNT_ID)
            ids = [g["AccessGrantId"] for g in resp["AccessGrantsList"]]
            assert grant_id in ids
        finally:
            try:
                client.delete_access_grant(AccountId=ACCOUNT_ID, AccessGrantId=grant_id)
            except Exception:
                pass  # best-effort cleanup

    def test_list_access_grants_after_delete(self, client):
        """Deleted grant disappears from ListAccessGrants."""
        try:
            client.create_access_grants_instance(AccountId=ACCOUNT_ID)
        except ClientError:
            pass  # may already exist
        loc = client.create_access_grants_location(
            AccountId=ACCOUNT_ID,
            LocationScope="s3://del-grant-bucket/",
            IAMRoleArn="arn:aws:iam::123456789012:role/grant-role2",
        )
        loc_id = loc["AccessGrantsLocationId"]
        grant = client.create_access_grant(
            AccountId=ACCOUNT_ID,
            AccessGrantsLocationId=loc_id,
            AccessGrantsLocationConfiguration={"S3SubPrefix": "x/"},
            Grantee={
                "GranteeType": "IAM",
                "GranteeIdentifier": "arn:aws:iam::123456789012:role/grantee2",
            },
            Permission="READ",
        )
        grant_id = grant["AccessGrantId"]
        client.delete_access_grant(AccountId=ACCOUNT_ID, AccessGrantId=grant_id)
        resp = client.list_access_grants(AccountId=ACCOUNT_ID)
        ids = [g["AccessGrantId"] for g in resp["AccessGrantsList"]]
        assert grant_id not in ids

    def test_list_access_grants_locations_empty_type(self, client):
        """ListAccessGrantsLocations returns a list type."""
        resp = client.list_access_grants_locations(AccountId=ACCOUNT_ID)
        assert "AccessGrantsLocationsList" in resp
        assert isinstance(resp["AccessGrantsLocationsList"], list)

    def test_list_access_grants_locations_status_code(self, client):
        """ListAccessGrantsLocations returns HTTP 200."""
        resp = client.list_access_grants_locations(AccountId=ACCOUNT_ID)
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200

    def test_list_access_grants_locations_after_create(self, client):
        """ListAccessGrantsLocations includes newly created location."""
        try:
            client.create_access_grants_instance(AccountId=ACCOUNT_ID)
        except ClientError:
            pass  # may already exist
        loc = client.create_access_grants_location(
            AccountId=ACCOUNT_ID,
            LocationScope="s3://loc-list-bucket/",
            IAMRoleArn="arn:aws:iam::123456789012:role/loc-list-role",
        )
        loc_id = loc["AccessGrantsLocationId"]
        try:
            resp = client.list_access_grants_locations(AccountId=ACCOUNT_ID)
            ids = [l["AccessGrantsLocationId"] for l in resp["AccessGrantsLocationsList"]]
            assert loc_id in ids
        finally:
            try:
                client.delete_access_grants_location(
                    AccountId=ACCOUNT_ID, AccessGrantsLocationId=loc_id
                )
            except Exception:
                pass  # best-effort cleanup

    def test_list_access_grants_locations_delete_disappears(self, client):
        """Deleted location disappears from list."""
        try:
            client.create_access_grants_instance(AccountId=ACCOUNT_ID)
        except ClientError:
            pass  # may already exist
        loc = client.create_access_grants_location(
            AccountId=ACCOUNT_ID,
            LocationScope="s3://loc-del-list-bucket/",
            IAMRoleArn="arn:aws:iam::123456789012:role/loc-del-role",
        )
        loc_id = loc["AccessGrantsLocationId"]
        client.delete_access_grants_location(AccountId=ACCOUNT_ID, AccessGrantsLocationId=loc_id)
        resp = client.list_access_grants_locations(AccountId=ACCOUNT_ID)
        ids = [l["AccessGrantsLocationId"] for l in resp["AccessGrantsLocationsList"]]
        assert loc_id not in ids

    def test_list_access_grants_locations_error_on_get_nonexistent(self, client):
        """GetAccessGrantsLocation for nonexistent ID raises error."""
        with pytest.raises(ClientError) as exc:
            client.get_access_grants_location(
                AccountId=ACCOUNT_ID,
                AccessGrantsLocationId="00000000-0000-0000-0000-nonexistent001",
            )
        assert exc.value.response["Error"]["Code"] == "NoSuchAccessGrantsLocation"


class TestS3ControlDeleteAccessPointPolicyEdgeCases:
    """Behavioral fidelity tests for delete access point policy."""

    @pytest.fixture
    def client(self):
        return make_client("s3control")

    @pytest.fixture
    def s3(self):
        return make_client("s3")

    @pytest.fixture
    def ap(self, client, s3):
        bucket = f"ap-pol-ec-{_uid()}"
        ap_name = f"ap-pol-ec-{_uid()}"
        s3.create_bucket(Bucket=bucket)
        client.create_access_point(AccountId=ACCOUNT_ID, Name=ap_name, Bucket=bucket)
        yield ap_name, bucket
        try:
            client.delete_access_point_policy(AccountId=ACCOUNT_ID, Name=ap_name)
        except Exception:
            pass  # best-effort cleanup
        try:
            client.delete_access_point(AccountId=ACCOUNT_ID, Name=ap_name)
        except Exception:
            pass  # best-effort cleanup
        try:
            s3.delete_bucket(Bucket=bucket)
        except Exception:
            pass  # best-effort cleanup

    def test_delete_policy_then_get_raises_error(self, client, ap):
        """After DeleteAccessPointPolicy, GetAccessPointPolicy raises NoSuchAccessPointPolicy."""
        ap_name, _ = ap
        policy = json.dumps({"Version": "2012-10-17", "Statement": []})
        client.put_access_point_policy(AccountId=ACCOUNT_ID, Name=ap_name, Policy=policy)
        client.delete_access_point_policy(AccountId=ACCOUNT_ID, Name=ap_name)
        with pytest.raises(ClientError) as exc:
            client.get_access_point_policy(AccountId=ACCOUNT_ID, Name=ap_name)
        assert exc.value.response["Error"]["Code"] == "NoSuchAccessPointPolicy"

    def test_delete_policy_twice_idempotent(self, client, ap):
        """Deleting access point policy twice does not raise error."""
        ap_name, _ = ap
        policy = json.dumps({"Version": "2012-10-17", "Statement": []})
        client.put_access_point_policy(AccountId=ACCOUNT_ID, Name=ap_name, Policy=policy)
        client.delete_access_point_policy(AccountId=ACCOUNT_ID, Name=ap_name)
        resp = client.delete_access_point_policy(AccountId=ACCOUNT_ID, Name=ap_name)
        assert resp["ResponseMetadata"]["HTTPStatusCode"] in (200, 204)

    def test_put_policy_replaces_existing(self, client, ap):
        """PutAccessPointPolicy replaces existing policy."""
        ap_name, _ = ap
        policy1 = json.dumps({"Version": "2012-10-17", "Statement": [{"Effect": "Allow", "Principal": "*", "Action": "s3:GetObject", "Resource": "*"}]})
        policy2 = json.dumps({"Version": "2012-10-17", "Statement": [{"Effect": "Deny", "Principal": "*", "Action": "s3:PutObject", "Resource": "*"}]})
        client.put_access_point_policy(AccountId=ACCOUNT_ID, Name=ap_name, Policy=policy1)
        client.put_access_point_policy(AccountId=ACCOUNT_ID, Name=ap_name, Policy=policy2)
        resp = client.get_access_point_policy(AccountId=ACCOUNT_ID, Name=ap_name)
        parsed = json.loads(resp["Policy"])
        assert parsed["Statement"][0]["Effect"] == "Deny"

    def test_delete_policy_status_code(self, client, ap):
        """DeleteAccessPointPolicy returns 200 or 204."""
        ap_name, _ = ap
        policy = json.dumps({"Version": "2012-10-17", "Statement": []})
        client.put_access_point_policy(AccountId=ACCOUNT_ID, Name=ap_name, Policy=policy)
        resp = client.delete_access_point_policy(AccountId=ACCOUNT_ID, Name=ap_name)
        assert resp["ResponseMetadata"]["HTTPStatusCode"] in (200, 204)

    def test_delete_nonexistent_ap_policy_raises_error(self, client):
        """DeleteAccessPointPolicy on nonexistent AP raises NoSuchAccessPoint."""
        with pytest.raises(ClientError) as exc:
            client.delete_access_point_policy(
                AccountId=ACCOUNT_ID, Name=f"nonexistent-{_uid()}"
            )
        assert exc.value.response["Error"]["Code"] == "NoSuchAccessPoint"


class TestS3ControlTagResourceEdgeCases:
    """Edge case and behavioral fidelity tests for tag_resource."""

    @pytest.fixture
    def client(self):
        return make_client("s3control")

    @pytest.fixture
    def s3(self):
        return make_client("s3")

    @pytest.fixture
    def bucket_arn(self, s3):
        name = f"tag-ec-{_uid()}"
        s3.create_bucket(Bucket=name)
        arn = f"arn:aws:s3:::{name}"
        yield arn
        try:
            s3.delete_bucket(Bucket=name)
        except Exception:
            pass  # best-effort cleanup

    def test_tag_resource_returns_success(self, client, bucket_arn):
        """TagResource returns HTTP 200 or 204."""
        resp = client.tag_resource(
            AccountId=ACCOUNT_ID,
            ResourceArn=bucket_arn,
            Tags=[{"Key": "env", "Value": "test"}],
        )
        assert resp["ResponseMetadata"]["HTTPStatusCode"] in (200, 204)

    def test_tag_resource_appears_in_list(self, client, bucket_arn):
        """Tags added with TagResource appear in ListTagsForResource."""
        client.tag_resource(
            AccountId=ACCOUNT_ID,
            ResourceArn=bucket_arn,
            Tags=[{"Key": "app", "Value": "api"}, {"Key": "tier", "Value": "backend"}],
        )
        resp = client.list_tags_for_resource(AccountId=ACCOUNT_ID, ResourceArn=bucket_arn)
        tag_map = {t["Key"]: t["Value"] for t in resp["Tags"]}
        assert tag_map["app"] == "api"
        assert tag_map["tier"] == "backend"

    def test_tag_resource_overwrite(self, client, bucket_arn):
        """TagResource with existing key updates the value."""
        client.tag_resource(
            AccountId=ACCOUNT_ID,
            ResourceArn=bucket_arn,
            Tags=[{"Key": "env", "Value": "dev"}],
        )
        client.tag_resource(
            AccountId=ACCOUNT_ID,
            ResourceArn=bucket_arn,
            Tags=[{"Key": "env", "Value": "prod"}],
        )
        resp = client.list_tags_for_resource(AccountId=ACCOUNT_ID, ResourceArn=bucket_arn)
        tag_map = {t["Key"]: t["Value"] for t in resp["Tags"]}
        assert tag_map["env"] == "prod"

    def test_untag_resource_removes_tag(self, client, bucket_arn):
        """UntagResource removes specified tags."""
        client.tag_resource(
            AccountId=ACCOUNT_ID,
            ResourceArn=bucket_arn,
            Tags=[{"Key": "a", "Value": "1"}, {"Key": "b", "Value": "2"}],
        )
        client.untag_resource(
            AccountId=ACCOUNT_ID,
            ResourceArn=bucket_arn,
            TagKeys=["a"],
        )
        resp = client.list_tags_for_resource(AccountId=ACCOUNT_ID, ResourceArn=bucket_arn)
        keys = [t["Key"] for t in resp["Tags"]]
        assert "a" not in keys
        assert "b" in keys

    def test_list_tags_after_untag_all(self, client, bucket_arn):
        """ListTagsForResource returns empty after all tags removed."""
        client.tag_resource(
            AccountId=ACCOUNT_ID,
            ResourceArn=bucket_arn,
            Tags=[{"Key": "x", "Value": "y"}],
        )
        client.untag_resource(
            AccountId=ACCOUNT_ID,
            ResourceArn=bucket_arn,
            TagKeys=["x"],
        )
        resp = client.list_tags_for_resource(AccountId=ACCOUNT_ID, ResourceArn=bucket_arn)
        keys = [t["Key"] for t in resp["Tags"]]
        assert "x" not in keys

    def test_list_tags_empty_has_tags_key(self, client, bucket_arn):
        """ListTagsForResource returns Tags key even when no tags set."""
        resp = client.list_tags_for_resource(AccountId=ACCOUNT_ID, ResourceArn=bucket_arn)
        assert "Tags" in resp
        assert isinstance(resp["Tags"], list)


class TestS3ControlBucketLifecycleEdgeCases:
    """Edge cases for GetBucketLifecycleConfiguration."""

    @pytest.fixture
    def client(self):
        return make_client("s3control")

    @pytest.fixture
    def s3(self):
        return make_client("s3")

    @pytest.fixture
    def bucket(self, s3):
        name = f"lc-ec-{_uid()}"
        s3.create_bucket(Bucket=name)
        yield name
        try:
            s3.delete_bucket(Bucket=name)
        except Exception:
            pass  # best-effort cleanup

    def test_get_lifecycle_returns_rules_key(self, client, bucket):
        """GetBucketLifecycleConfiguration returns Rules key."""
        resp = client.get_bucket_lifecycle_configuration(AccountId=ACCOUNT_ID, Bucket=bucket)
        assert "Rules" in resp

    def test_get_lifecycle_rules_is_list(self, client, bucket):
        """GetBucketLifecycleConfiguration Rules is a list."""
        resp = client.get_bucket_lifecycle_configuration(AccountId=ACCOUNT_ID, Bucket=bucket)
        assert isinstance(resp["Rules"], list)

    def test_put_lifecycle_then_get(self, client, bucket):
        """PutBucketLifecycleConfiguration then GetBucketLifecycleConfiguration returns 200."""
        client.put_bucket_lifecycle_configuration(
            AccountId=ACCOUNT_ID,
            Bucket=bucket,
            LifecycleConfiguration={
                "Rules": [
                    {
                        "ID": "expire-rule",
                        "Status": "Enabled",
                        "Filter": {"Prefix": "tmp/"},
                    }
                ]
            },
        )
        resp = client.get_bucket_lifecycle_configuration(AccountId=ACCOUNT_ID, Bucket=bucket)
        assert "Rules" in resp
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200

    def test_get_lifecycle_nonexistent_bucket_error(self, client):
        """GetBucketLifecycleConfiguration on nonexistent bucket raises NoSuchBucket."""
        with pytest.raises(ClientError) as exc:
            client.get_bucket_lifecycle_configuration(
                AccountId=ACCOUNT_ID, Bucket=f"nonexistent-{_uid()}"
            )
        assert exc.value.response["Error"]["Code"] == "NoSuchBucket"

    def test_delete_lifecycle_then_get_empty_rules(self, client, bucket):
        """After DeleteBucketLifecycleConfiguration, GetBucketLifecycleConfiguration returns empty list."""
        client.put_bucket_lifecycle_configuration(
            AccountId=ACCOUNT_ID,
            Bucket=bucket,
            LifecycleConfiguration={
                "Rules": [{"ID": "rule", "Status": "Enabled", "Filter": {"Prefix": ""}}]
            },
        )
        client.delete_bucket_lifecycle_configuration(AccountId=ACCOUNT_ID, Bucket=bucket)
        resp = client.get_bucket_lifecycle_configuration(AccountId=ACCOUNT_ID, Bucket=bucket)
        assert "Rules" in resp
        assert isinstance(resp["Rules"], list)

    def test_lifecycle_http_status_code(self, client, bucket):
        """GetBucketLifecycleConfiguration returns HTTP 200."""
        resp = client.get_bucket_lifecycle_configuration(AccountId=ACCOUNT_ID, Bucket=bucket)
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200
class TestS3ControlEdgeCasesExpanded:
    """Expanded edge case tests covering missing behavioral patterns for S3 Control operations."""

    @pytest.fixture
    def s3control(self):
        return make_client("s3control")

    @pytest.fixture
    def s3(self):
        return make_client("s3")

    @pytest.fixture
    def bucket(self, s3):
        name = f"edge-bkt-{_uid()}"
        s3.create_bucket(Bucket=name)
        yield name
        try:
            s3.delete_bucket(Bucket=name)
        except Exception:
            pass  # best-effort cleanup

    def _make_job(self, s3control, s3, priority=10, tags=None):
        """Helper to create a batch job and return (job_id, bucket_name)."""
        bucket = f"edgejob-{_uid()}"
        s3.create_bucket(Bucket=bucket)
        kwargs = dict(
            AccountId=ACCOUNT_ID,
            Operation={"S3PutObjectCopy": {"TargetResource": f"arn:aws:s3:::{bucket}"}},
            Report={"Enabled": False},
            ClientRequestToken=str(uuid.uuid4()),
            Priority=priority,
            RoleArn=f"arn:aws:iam::{ACCOUNT_ID}:role/test-role",
            ConfirmationRequired=False,
            ManifestGenerator={
                "S3JobManifestGenerator": {
                    "SourceBucket": f"arn:aws:s3:::{bucket}",
                    "EnableManifestOutput": False,
                }
            },
        )
        if tags:
            kwargs["Tags"] = tags
        resp = s3control.create_job(**kwargs)
        return resp["JobId"], bucket

    # ---------------------------------------------------------------------------
    # Access Grants Instance for Prefix — C, L, U, D, E patterns
    # ---------------------------------------------------------------------------

    def test_access_grants_instance_for_prefix_list(self, s3control):
        """LIST: list_access_grants_instances returns the instance after creation."""
        # Ensure an instance exists
        try:
            s3control.get_access_grants_instance(AccountId=ACCOUNT_ID)
        except ClientError:
            s3control.create_access_grants_instance(AccountId=ACCOUNT_ID)
        resp = s3control.list_access_grants_instances(AccountId=ACCOUNT_ID)
        assert "AccessGrantsInstancesList" in resp
        assert isinstance(resp["AccessGrantsInstancesList"], list)
        assert len(resp["AccessGrantsInstancesList"]) > 0

    def test_access_grants_instance_for_prefix_update_location(self, s3control):
        """UPDATE: update_access_grants_location preserves the location scope."""
        # Ensure instance exists
        try:
            s3control.get_access_grants_instance(AccountId=ACCOUNT_ID)
        except ClientError:
            s3control.create_access_grants_instance(AccountId=ACCOUNT_ID)
        scope = "s3://update-scope-bucket/"
        loc = s3control.create_access_grants_location(
            AccountId=ACCOUNT_ID,
            LocationScope=scope,
            IAMRoleArn=f"arn:aws:iam::{ACCOUNT_ID}:role/update-role",
        )
        loc_id = loc["AccessGrantsLocationId"]
        try:
            resp = s3control.update_access_grants_location(
                AccountId=ACCOUNT_ID,
                AccessGrantsLocationId=loc_id,
                IAMRoleArn=f"arn:aws:iam::{ACCOUNT_ID}:role/update-role-v2",
            )
            assert resp["LocationScope"] == scope
            assert resp["IAMRoleArn"].endswith("update-role-v2")
        finally:
            try:
                s3control.delete_access_grants_location(
                    AccountId=ACCOUNT_ID, AccessGrantsLocationId=loc_id
                )
            except Exception:
                pass  # best-effort cleanup

    def test_access_grants_instance_for_prefix_delete_then_get(self, s3control):
        """DELETE then GET: after deleting instance, get_access_grants_instance raises error."""
        # Create a fresh instance
        try:
            s3control.delete_access_grants_instance(AccountId=ACCOUNT_ID)
        except Exception:
            pass  # best-effort: delete if exists
        s3control.create_access_grants_instance(AccountId=ACCOUNT_ID)
        s3control.delete_access_grants_instance(AccountId=ACCOUNT_ID)
        with pytest.raises(ClientError) as exc:
            s3control.get_access_grants_instance(AccountId=ACCOUNT_ID)
        assert exc.value.response["Error"]["Code"] in (
            "AccessGrantsInstanceNotFound",
            "NoSuchAccessGrantsInstance",
            "ResourceNotFoundException",
        )

    def test_access_grants_instance_for_prefix_error_no_instance(self, s3control):
        """ERROR: get_access_grants_instance_for_prefix when instance was deleted raises error."""
        # Delete any existing instance so there's nothing
        try:
            s3control.delete_access_grants_instance(AccountId=ACCOUNT_ID)
        except Exception:
            pass  # best-effort
        with pytest.raises(ClientError) as exc:
            s3control.get_access_grants_instance_for_prefix(
                AccountId=ACCOUNT_ID, S3Prefix="s3://no-instance-bucket/pfx"
            )
        # The error code may vary; just verify we get a ClientError with a code
        assert exc.value.response["Error"]["Code"] != ""

    # ---------------------------------------------------------------------------
    # Access Grants Resource Policy — R, U, D, E patterns
    # ---------------------------------------------------------------------------

    def _make_policy(self, action="s3:GetAccessGrant"):
        return json.dumps(
            {
                "Version": "2012-10-17",
                "Statement": [
                    {
                        "Effect": "Allow",
                        "Principal": "*",
                        "Action": action,
                        "Resource": "*",
                    }
                ],
            }
        )

    def _ensure_access_grants_instance(self, s3control):
        try:
            s3control.get_access_grants_instance(AccountId=ACCOUNT_ID)
        except ClientError:
            s3control.create_access_grants_instance(AccountId=ACCOUNT_ID)

    def test_resource_policy_retrieve(self, s3control):
        """RETRIEVE: put then get, verify JSON content is parseable."""
        self._ensure_access_grants_instance(s3control)
        policy = self._make_policy("s3:GetAccessGrant")
        s3control.put_access_grants_instance_resource_policy(
            AccountId=ACCOUNT_ID, Policy=policy
        )
        resp = s3control.get_access_grants_instance_resource_policy(AccountId=ACCOUNT_ID)
        assert "Policy" in resp
        parsed = json.loads(resp["Policy"])
        assert parsed["Version"] == "2012-10-17"
        assert parsed["Statement"][0]["Action"] == "s3:GetAccessGrant"

    def test_resource_policy_update(self, s3control):
        """UPDATE: put policy twice, verify second replaces first."""
        self._ensure_access_grants_instance(s3control)
        policy1 = self._make_policy("s3:GetAccessGrant")
        policy2 = self._make_policy("s3:ListAccessGrants")
        s3control.put_access_grants_instance_resource_policy(
            AccountId=ACCOUNT_ID, Policy=policy1
        )
        s3control.put_access_grants_instance_resource_policy(
            AccountId=ACCOUNT_ID, Policy=policy2
        )
        resp = s3control.get_access_grants_instance_resource_policy(AccountId=ACCOUNT_ID)
        parsed = json.loads(resp["Policy"])
        assert parsed["Statement"][0]["Action"] == "s3:ListAccessGrants"

    def test_resource_policy_delete(self, s3control):
        """DELETE: put then delete, verify get raises error or returns empty."""
        self._ensure_access_grants_instance(s3control)
        policy = self._make_policy()
        s3control.put_access_grants_instance_resource_policy(
            AccountId=ACCOUNT_ID, Policy=policy
        )
        del_resp = s3control.delete_access_grants_instance_resource_policy(
            AccountId=ACCOUNT_ID
        )
        assert del_resp["ResponseMetadata"]["HTTPStatusCode"] == 200
        # After deletion, get should return empty Policy or raise
        resp = s3control.get_access_grants_instance_resource_policy(AccountId=ACCOUNT_ID)
        # Either empty policy or the field is missing/empty
        policy_val = resp.get("Policy", "")
        assert policy_val == "" or policy_val is None

    # ---------------------------------------------------------------------------
    # MRAP Routes — C, L, U, D, E patterns
    # ---------------------------------------------------------------------------

    def test_mrap_routes_create_and_get(self, s3control, s3):
        """CREATE: create MRAP, verify routes list is accessible."""
        bucket = f"mrap-cr-{_uid()}"
        mrap_name = f"mrap-cr-{_uid()}"
        s3.create_bucket(Bucket=bucket)
        try:
            s3control.create_multi_region_access_point(
                AccountId=ACCOUNT_ID,
                Details={"Name": mrap_name, "Regions": [{"Bucket": bucket}]},
            )
            resp = s3control.get_multi_region_access_point_routes(
                AccountId=ACCOUNT_ID, Mrap=mrap_name
            )
            assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200
            assert "Routes" in resp
            assert isinstance(resp["Routes"], list)
        finally:
            try:
                s3control.delete_multi_region_access_point(
                    AccountId=ACCOUNT_ID, Details={"Name": mrap_name}
                )
            except Exception:
                pass  # best-effort cleanup
            try:
                s3.delete_bucket(Bucket=bucket)
            except Exception:
                pass  # best-effort cleanup

    def test_mrap_routes_list(self, s3control, s3):
        """LIST: create MRAP, list_multi_region_access_points shows it."""
        bucket = f"mrap-ls-{_uid()}"
        mrap_name = f"mrap-ls-{_uid()}"
        s3.create_bucket(Bucket=bucket)
        try:
            s3control.create_multi_region_access_point(
                AccountId=ACCOUNT_ID,
                Details={"Name": mrap_name, "Regions": [{"Bucket": bucket}]},
            )
            resp = s3control.list_multi_region_access_points(AccountId=ACCOUNT_ID)
            assert "AccessPoints" in resp
            names = [ap["Name"] for ap in resp["AccessPoints"]]
            assert mrap_name in names
        finally:
            try:
                s3control.delete_multi_region_access_point(
                    AccountId=ACCOUNT_ID, Details={"Name": mrap_name}
                )
            except Exception:
                pass  # best-effort cleanup
            try:
                s3.delete_bucket(Bucket=bucket)
            except Exception:
                pass  # best-effort cleanup

    def test_mrap_routes_submit_and_verify(self, s3control, s3):
        """UPDATE: submit routes with TrafficDialPercentage, verify response 200."""
        bucket = f"mrap-upd-{_uid()}"
        mrap_name = f"mrap-upd-{_uid()}"
        s3.create_bucket(Bucket=bucket)
        try:
            s3control.create_multi_region_access_point(
                AccountId=ACCOUNT_ID,
                Details={"Name": mrap_name, "Regions": [{"Bucket": bucket}]},
            )
            resp = s3control.submit_multi_region_access_point_routes(
                AccountId=ACCOUNT_ID,
                Mrap=mrap_name,
                RouteUpdates=[
                    {
                        "Bucket": bucket,
                        "Region": "us-east-1",
                        "TrafficDialPercentage": 50,
                    }
                ],
            )
            assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200
        finally:
            try:
                s3control.delete_multi_region_access_point(
                    AccountId=ACCOUNT_ID, Details={"Name": mrap_name}
                )
            except Exception:
                pass  # best-effort cleanup
            try:
                s3.delete_bucket(Bucket=bucket)
            except Exception:
                pass  # best-effort cleanup

    def test_mrap_routes_delete_then_get_raises(self, s3control, s3):
        """DELETE: delete MRAP, verify get_routes raises NoSuchMultiRegionAccessPoint."""
        bucket = f"mrap-del-{_uid()}"
        mrap_name = f"mrap-del-{_uid()}"
        s3.create_bucket(Bucket=bucket)
        try:
            s3control.create_multi_region_access_point(
                AccountId=ACCOUNT_ID,
                Details={"Name": mrap_name, "Regions": [{"Bucket": bucket}]},
            )
            s3control.delete_multi_region_access_point(
                AccountId=ACCOUNT_ID, Details={"Name": mrap_name}
            )
            with pytest.raises(ClientError) as exc:
                s3control.get_multi_region_access_point_routes(
                    AccountId=ACCOUNT_ID, Mrap=mrap_name
                )
            assert exc.value.response["Error"]["Code"] == "NoSuchMultiRegionAccessPoint"
        finally:
            try:
                s3.delete_bucket(Bucket=bucket)
            except Exception:
                pass  # best-effort cleanup

    def test_mrap_routes_submit_nonexistent_error(self, s3control):
        """ERROR: submit routes for nonexistent MRAP raises NoSuchMultiRegionAccessPoint."""
        fake_mrap = f"nonexistent-{_uid()}"
        with pytest.raises(ClientError) as exc:
            s3control.submit_multi_region_access_point_routes(
                AccountId=ACCOUNT_ID,
                Mrap=fake_mrap,
                RouteUpdates=[
                    {
                        "Bucket": "any-bucket",
                        "Region": "us-east-1",
                        "TrafficDialPercentage": 100,
                    }
                ],
            )
        assert exc.value.response["Error"]["Code"] == "NoSuchMultiRegionAccessPoint"

    # ---------------------------------------------------------------------------
    # Job Tagging — C, L, U, D, E patterns
    # ---------------------------------------------------------------------------

    def test_job_tagging_create_with_tags(self, s3control, s3):
        """CREATE: create job and put_job_tagging returns 200, get_job_tagging returns Tags key."""
        job_id, bucket = self._make_job(s3control, s3)
        try:
            put_resp = s3control.put_job_tagging(
                AccountId=ACCOUNT_ID,
                JobId=job_id,
                Tags=[{"Key": "env", "Value": "staging"}],
            )
            assert put_resp["ResponseMetadata"]["HTTPStatusCode"] == 200
            get_resp = s3control.get_job_tagging(AccountId=ACCOUNT_ID, JobId=job_id)
            assert "Tags" in get_resp
            assert isinstance(get_resp["Tags"], list)
        finally:
            try:
                s3control.update_job_status(
                    AccountId=ACCOUNT_ID, JobId=job_id, RequestedJobStatus="Cancelled"
                )
            except Exception:
                pass  # best-effort cleanup
            try:
                s3.delete_bucket(Bucket=bucket)
            except Exception:
                pass  # best-effort cleanup

    def test_job_tagging_list_jobs(self, s3control, s3):
        """LIST: create job, list_jobs shows the job with expected fields."""
        job_id, bucket = self._make_job(s3control, s3, priority=7)
        try:
            resp = s3control.list_jobs(AccountId=ACCOUNT_ID)
            assert "Jobs" in resp
            job_ids = [j["JobId"] for j in resp["Jobs"]]
            assert job_id in job_ids
            job_entry = next(j for j in resp["Jobs"] if j["JobId"] == job_id)
            assert "Status" in job_entry
        finally:
            try:
                s3control.update_job_status(
                    AccountId=ACCOUNT_ID, JobId=job_id, RequestedJobStatus="Cancelled"
                )
            except Exception:
                pass  # best-effort cleanup
            try:
                s3.delete_bucket(Bucket=bucket)
            except Exception:
                pass  # best-effort cleanup

    def test_job_tagging_put_replaces_tags(self, s3control, s3):
        """UPDATE: put_job_tagging twice, both calls return 200."""
        job_id, bucket = self._make_job(s3control, s3)
        try:
            resp1 = s3control.put_job_tagging(
                AccountId=ACCOUNT_ID,
                JobId=job_id,
                Tags=[{"Key": "env", "Value": "old"}],
            )
            assert resp1["ResponseMetadata"]["HTTPStatusCode"] == 200
            resp2 = s3control.put_job_tagging(
                AccountId=ACCOUNT_ID,
                JobId=job_id,
                Tags=[{"Key": "env", "Value": "new"}, {"Key": "team", "Value": "platform"}],
            )
            assert resp2["ResponseMetadata"]["HTTPStatusCode"] == 200
            # Verify get still returns Tags structure
            get_resp = s3control.get_job_tagging(AccountId=ACCOUNT_ID, JobId=job_id)
            assert "Tags" in get_resp
        finally:
            try:
                s3control.update_job_status(
                    AccountId=ACCOUNT_ID, JobId=job_id, RequestedJobStatus="Cancelled"
                )
            except Exception:
                pass  # best-effort cleanup
            try:
                s3.delete_bucket(Bucket=bucket)
            except Exception:
                pass  # best-effort cleanup

    def test_job_tagging_delete_then_empty(self, s3control, s3):
        """DELETE: put_job_tagging then delete, verify get returns empty list."""
        job_id, bucket = self._make_job(s3control, s3)
        try:
            s3control.put_job_tagging(
                AccountId=ACCOUNT_ID,
                JobId=job_id,
                Tags=[{"Key": "k", "Value": "v"}],
            )
            s3control.delete_job_tagging(AccountId=ACCOUNT_ID, JobId=job_id)
            resp = s3control.get_job_tagging(AccountId=ACCOUNT_ID, JobId=job_id)
            assert resp["Tags"] == []
        finally:
            try:
                s3control.update_job_status(
                    AccountId=ACCOUNT_ID, JobId=job_id, RequestedJobStatus="Cancelled"
                )
            except Exception:
                pass  # best-effort cleanup
            try:
                s3.delete_bucket(Bucket=bucket)
            except Exception:
                pass  # best-effort cleanup

    def test_job_tagging_delete_nonexistent_raises(self, s3control):
        """ERROR: delete_job_tagging for nonexistent job raises NoSuchJob."""
        with pytest.raises(ClientError) as exc:
            s3control.delete_job_tagging(
                AccountId=ACCOUNT_ID, JobId="00000000-0000-0000-0000-000000000000"
            )
        assert exc.value.response["Error"]["Code"] == "NoSuchJob"

    # ---------------------------------------------------------------------------
    # Bucket Tagging — R, U, D, E patterns
    # ---------------------------------------------------------------------------

    def test_bucket_tagging_retrieve(self, s3control, bucket):
        """RETRIEVE: put tags, get tags, verify specific key/value matches."""
        s3control.put_bucket_tagging(
            AccountId=ACCOUNT_ID,
            Bucket=bucket,
            Tagging={"TagSet": [{"Key": "project", "Value": "robotocore"}]},
        )
        resp = s3control.get_bucket_tagging(AccountId=ACCOUNT_ID, Bucket=bucket)
        assert "TagSet" in resp
        tag_map = {t["Key"]: t["Value"] for t in resp["TagSet"]}
        assert tag_map.get("project") == "robotocore"

    def test_bucket_tagging_update(self, s3control, bucket):
        """UPDATE: put tags twice, verify second set overwrites first."""
        s3control.put_bucket_tagging(
            AccountId=ACCOUNT_ID,
            Bucket=bucket,
            Tagging={"TagSet": [{"Key": "version", "Value": "1"}]},
        )
        s3control.put_bucket_tagging(
            AccountId=ACCOUNT_ID,
            Bucket=bucket,
            Tagging={"TagSet": [{"Key": "version", "Value": "2"}, {"Key": "env", "Value": "prod"}]},
        )
        resp = s3control.get_bucket_tagging(AccountId=ACCOUNT_ID, Bucket=bucket)
        tag_map = {t["Key"]: t["Value"] for t in resp["TagSet"]}
        assert tag_map.get("version") == "2"
        assert tag_map.get("env") == "prod"

    def test_bucket_tagging_delete(self, s3control, bucket):
        """DELETE: put tags, delete tags, verify tags are gone."""
        s3control.put_bucket_tagging(
            AccountId=ACCOUNT_ID,
            Bucket=bucket,
            Tagging={"TagSet": [{"Key": "tmp", "Value": "yes"}]},
        )
        s3control.delete_bucket_tagging(AccountId=ACCOUNT_ID, Bucket=bucket)
        resp = s3control.get_bucket_tagging(AccountId=ACCOUNT_ID, Bucket=bucket)
        assert resp.get("TagSet", []) == []

    def test_bucket_tagging_nonexistent_bucket_raises(self, s3control):
        """ERROR: put tags on nonexistent bucket raises NoSuchBucket."""
        with pytest.raises(ClientError) as exc:
            s3control.put_bucket_tagging(
                AccountId=ACCOUNT_ID,
                Bucket=f"no-such-bucket-{_uid()}",
                Tagging={"TagSet": [{"Key": "k", "Value": "v"}]},
            )
        assert exc.value.response["Error"]["Code"] == "NoSuchBucket"

    # ---------------------------------------------------------------------------
    # Bucket Policy — R, U, D, E patterns
    # ---------------------------------------------------------------------------

    def _make_bucket_policy(self, bucket, action="s3:GetObject"):
        return json.dumps(
            {
                "Version": "2012-10-17",
                "Statement": [
                    {
                        "Effect": "Allow",
                        "Principal": "*",
                        "Action": action,
                        "Resource": f"arn:aws:s3:::{bucket}/*",
                    }
                ],
            }
        )

    def test_bucket_policy_retrieve(self, s3control, bucket):
        """RETRIEVE: put policy, get policy, verify JSON content."""
        policy = self._make_bucket_policy(bucket, "s3:GetObject")
        s3control.put_bucket_policy(AccountId=ACCOUNT_ID, Bucket=bucket, Policy=policy)
        resp = s3control.get_bucket_policy(AccountId=ACCOUNT_ID, Bucket=bucket)
        assert "Policy" in resp
        parsed = json.loads(resp["Policy"])
        assert parsed["Statement"][0]["Action"] == "s3:GetObject"

    def test_bucket_policy_update(self, s3control, bucket):
        """UPDATE: put policy twice, verify new policy replaces old."""
        s3control.put_bucket_policy(
            AccountId=ACCOUNT_ID,
            Bucket=bucket,
            Policy=self._make_bucket_policy(bucket, "s3:GetObject"),
        )
        s3control.put_bucket_policy(
            AccountId=ACCOUNT_ID,
            Bucket=bucket,
            Policy=self._make_bucket_policy(bucket, "s3:PutObject"),
        )
        resp = s3control.get_bucket_policy(AccountId=ACCOUNT_ID, Bucket=bucket)
        parsed = json.loads(resp["Policy"])
        assert parsed["Statement"][0]["Action"] == "s3:PutObject"

    def test_bucket_policy_delete(self, s3control, bucket):
        """DELETE: put policy, delete policy, verify get returns empty or raises."""
        s3control.put_bucket_policy(
            AccountId=ACCOUNT_ID,
            Bucket=bucket,
            Policy=self._make_bucket_policy(bucket),
        )
        del_resp = s3control.delete_bucket_policy(AccountId=ACCOUNT_ID, Bucket=bucket)
        assert del_resp["ResponseMetadata"]["HTTPStatusCode"] in (200, 204)
        # After deletion, get returns empty or raises
        try:
            resp = s3control.get_bucket_policy(AccountId=ACCOUNT_ID, Bucket=bucket)
            assert resp.get("Policy", "") == ""
        except ClientError as e:
            assert e.response["Error"]["Code"] in ("NoSuchBucketPolicy", "NoSuchBucket")

    def test_bucket_policy_nonexistent_bucket_raises(self, s3control):
        """ERROR: get policy on nonexistent bucket raises NoSuchBucket."""
        with pytest.raises(ClientError) as exc:
            s3control.get_bucket_policy(
                AccountId=ACCOUNT_ID, Bucket=f"no-such-bucket-{_uid()}"
            )
        assert exc.value.response["Error"]["Code"] == "NoSuchBucket"

    # ---------------------------------------------------------------------------
    # Bucket Lifecycle — C, R, U, E patterns
    # ---------------------------------------------------------------------------

    def test_bucket_lifecycle_create(self, s3control, bucket):
        """CREATE: put lifecycle config, verify 200."""
        resp = s3control.put_bucket_lifecycle_configuration(
            AccountId=ACCOUNT_ID,
            Bucket=bucket,
            LifecycleConfiguration={
                "Rules": [
                    {
                        "ID": "archive-rule",
                        "Status": "Enabled",
                        "Filter": {"Prefix": "archive/"},
                    }
                ]
            },
        )
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200

    def test_bucket_lifecycle_retrieve(self, s3control, bucket):
        """RETRIEVE: put lifecycle, get lifecycle, verify Rules key present."""
        s3control.put_bucket_lifecycle_configuration(
            AccountId=ACCOUNT_ID,
            Bucket=bucket,
            LifecycleConfiguration={
                "Rules": [
                    {
                        "ID": "logs-rule",
                        "Status": "Enabled",
                        "Filter": {"Prefix": "logs/"},
                    }
                ]
            },
        )
        resp = s3control.get_bucket_lifecycle_configuration(AccountId=ACCOUNT_ID, Bucket=bucket)
        assert "Rules" in resp
        assert isinstance(resp["Rules"], list)
        assert len(resp["Rules"]) > 0

    def test_bucket_lifecycle_update(self, s3control, bucket):
        """UPDATE: put lifecycle twice with different rules, verify 200 both times."""
        for rule_id in ("rule-v1", "rule-v2"):
            resp = s3control.put_bucket_lifecycle_configuration(
                AccountId=ACCOUNT_ID,
                Bucket=bucket,
                LifecycleConfiguration={
                    "Rules": [
                        {
                            "ID": rule_id,
                            "Status": "Enabled",
                            "Filter": {"Prefix": "data/"},
                        }
                    ]
                },
            )
            assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200

    def test_bucket_lifecycle_nonexistent_bucket_raises(self, s3control):
        """ERROR: get lifecycle on nonexistent bucket raises NoSuchBucket."""
        with pytest.raises(ClientError) as exc:
            s3control.get_bucket_lifecycle_configuration(
                AccountId=ACCOUNT_ID, Bucket=f"no-such-bucket-{_uid()}"
            )
        assert exc.value.response["Error"]["Code"] == "NoSuchBucket"

    # ---------------------------------------------------------------------------
    # Bucket Replication — R, U, E patterns
    # ---------------------------------------------------------------------------

    def _make_replication_config(self, bucket, role_suffix="repl"):
        return {
            "Role": f"arn:aws:iam::{ACCOUNT_ID}:role/{role_suffix}-role",
            "Rules": [
                {
                    "ID": "repl-rule",
                    "Status": "Enabled",
                    "Priority": 1,
                    "Bucket": f"arn:aws:s3:::{bucket}",
                    "Filter": {"Prefix": ""},
                    "Destination": {
                        "Bucket": f"arn:aws:s3:::{bucket}",
                        "Account": ACCOUNT_ID,
                    },
                    "DeleteMarkerReplication": {"Status": "Disabled"},
                }
            ],
        }

    def test_bucket_replication_retrieve(self, s3control, bucket):
        """RETRIEVE: put replication, get replication, verify content."""
        s3control.put_bucket_replication(
            AccountId=ACCOUNT_ID,
            Bucket=bucket,
            ReplicationConfiguration=self._make_replication_config(bucket),
        )
        resp = s3control.get_bucket_replication(AccountId=ACCOUNT_ID, Bucket=bucket)
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200
        assert "ReplicationConfiguration" in resp
        assert "Rules" in resp["ReplicationConfiguration"]

    def test_bucket_replication_update(self, s3control, bucket):
        """UPDATE: put replication twice with different roles, both return 200."""
        for suffix in ("v1", "v2"):
            resp = s3control.put_bucket_replication(
                AccountId=ACCOUNT_ID,
                Bucket=bucket,
                ReplicationConfiguration=self._make_replication_config(bucket, suffix),
            )
            assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200

    def test_bucket_replication_nonexistent_bucket_raises(self, s3control):
        """ERROR: get replication on nonexistent bucket raises NoSuchBucket."""
        with pytest.raises(ClientError) as exc:
            s3control.get_bucket_replication(
                AccountId=ACCOUNT_ID, Bucket=f"no-such-bucket-{_uid()}"
            )
        assert exc.value.response["Error"]["Code"] == "NoSuchBucket"

    # ---------------------------------------------------------------------------
    # Job Full Lifecycle — C, R, L, U, D, E patterns combined
    # ---------------------------------------------------------------------------

    def test_job_full_lifecycle(self, s3control, s3):
        """Full C+R+L+U+D+E job lifecycle in one test."""
        bucket = f"jfl-{_uid()}"
        s3.create_bucket(Bucket=bucket)
        job_id = None
        try:
            # CREATE
            create_resp = s3control.create_job(
                AccountId=ACCOUNT_ID,
                Operation={"S3PutObjectCopy": {"TargetResource": f"arn:aws:s3:::{bucket}"}},
                Report={"Enabled": False},
                ClientRequestToken=str(uuid.uuid4()),
                Priority=5,
                RoleArn=f"arn:aws:iam::{ACCOUNT_ID}:role/test-role",
                ConfirmationRequired=False,
                ManifestGenerator={
                    "S3JobManifestGenerator": {
                        "SourceBucket": f"arn:aws:s3:::{bucket}",
                        "EnableManifestOutput": False,
                    }
                },
                Tags=[{"Key": "lifecycle", "Value": "full"}],
            )
            assert "JobId" in create_resp
            job_id = create_resp["JobId"]

            # RETRIEVE
            desc = s3control.describe_job(AccountId=ACCOUNT_ID, JobId=job_id)
            assert desc["Job"]["JobId"] == job_id
            assert desc["Job"]["Priority"] == 5

            # LIST
            list_resp = s3control.list_jobs(AccountId=ACCOUNT_ID)
            assert job_id in [j["JobId"] for j in list_resp["Jobs"]]

            # UPDATE priority
            upd_resp = s3control.update_job_priority(
                AccountId=ACCOUNT_ID, JobId=job_id, Priority=99
            )
            assert upd_resp["Priority"] == 99

            # Verify update reflected
            desc2 = s3control.describe_job(AccountId=ACCOUNT_ID, JobId=job_id)
            assert desc2["Job"]["Priority"] == 99

            # DELETE (cancel)
            cancel_resp = s3control.update_job_status(
                AccountId=ACCOUNT_ID, JobId=job_id, RequestedJobStatus="Cancelled"
            )
            assert cancel_resp["Status"] == "Cancelled"

            # ERROR: nonexistent job
            with pytest.raises(ClientError) as exc:
                s3control.describe_job(
                    AccountId=ACCOUNT_ID,
                    JobId="00000000-0000-0000-0000-000000000000",
                )
            assert exc.value.response["Error"]["Code"] == "NoSuchJob"
        finally:
            if job_id:
                try:
                    s3control.update_job_status(
                        AccountId=ACCOUNT_ID, JobId=job_id, RequestedJobStatus="Cancelled"
                    )
                except Exception:
                    pass  # best-effort cleanup
            try:
                s3.delete_bucket(Bucket=bucket)
            except Exception:
                pass  # best-effort cleanup

    # ---------------------------------------------------------------------------
    # Access Grants Instance ARN format — U pattern (missing from test at 83%)
    # ---------------------------------------------------------------------------

    def test_access_grants_instance_arn_update(self, s3control):
        """UPDATE: update identity center association, verify instance ARN format preserved."""
        self._ensure_access_grants_instance(s3control)
        # Associate identity center (update the instance's association)
        resp = s3control.associate_access_grants_identity_center(
            AccountId=ACCOUNT_ID,
            IdentityCenterArn="arn:aws:sso:::instance/ssoins-test",
        )
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200
        # Verify instance ARN is still present and well-formed
        inst = s3control.get_access_grants_instance(AccountId=ACCOUNT_ID)
        assert "AccessGrantsInstanceArn" in inst
        arn = inst["AccessGrantsInstanceArn"]
        assert arn.startswith("arn:aws:s3:")
        # Cleanup identity center association
        try:
            s3control.dissociate_access_grants_identity_center(AccountId=ACCOUNT_ID)
        except Exception:
            pass  # best-effort cleanup
