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
        resp = s3control.list_access_points(
            AccountId=ACCOUNT_ID, Bucket=f"nonexistent-bucket-{_uid()}"
        )
        assert resp["AccessPointList"] == []

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
        resp = s3control.delete_access_point(AccountId=ACCOUNT_ID, Name=f"nonexistent-{_uid()}")
        assert resp["ResponseMetadata"]["HTTPStatusCode"] in (200, 204)

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

    def test_list_multi_region_access_points_empty(self, s3control):
        """ListMultiRegionAccessPoints returns empty list when none exist."""
        resp = s3control.list_multi_region_access_points(AccountId=ACCOUNT_ID)
        assert "AccessPoints" in resp
        assert isinstance(resp["AccessPoints"], list)

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
        """TagResource adds tags to an S3 resource."""
        resp = s3control.tag_resource(
            AccountId=ACCOUNT_ID,
            ResourceArn=bucket_arn,
            Tags=[{"Key": "env", "Value": "test"}],
        )
        assert resp["ResponseMetadata"]["HTTPStatusCode"] in (200, 204)

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

    def test_list_jobs_empty(self, s3control):
        resp = s3control.list_jobs(AccountId=ACCOUNT_ID)
        assert "Jobs" in resp
        assert isinstance(resp["Jobs"], list)

    def test_list_jobs_returns_metadata(self, s3control):
        resp = s3control.list_jobs(AccountId=ACCOUNT_ID)
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200


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
        resp = s3control.list_access_grants_instances(AccountId=ACCOUNT_ID)
        assert "AccessGrantsInstancesList" in resp
        assert isinstance(resp["AccessGrantsInstancesList"], list)

    def test_list_access_grants_empty(self, s3control):
        resp = s3control.list_access_grants(AccountId=ACCOUNT_ID)
        assert "AccessGrantsList" in resp
        assert isinstance(resp["AccessGrantsList"], list)

    def test_list_access_grants_locations_empty(self, s3control):
        resp = s3control.list_access_grants_locations(AccountId=ACCOUNT_ID)
        assert "AccessGrantsLocationsList" in resp
        assert isinstance(resp["AccessGrantsLocationsList"], list)

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
        resp = s3control.get_bucket_lifecycle_configuration(AccountId=ACCOUNT_ID, Bucket=bucket)
        assert "Rules" in resp
        assert isinstance(resp["Rules"], list)

    def test_get_bucket_policy(self, s3control, bucket):
        resp = s3control.get_bucket_policy(AccountId=ACCOUNT_ID, Bucket=bucket)
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200

    def test_get_bucket_replication(self, s3control, bucket):
        resp = s3control.get_bucket_replication(AccountId=ACCOUNT_ID, Bucket=bucket)
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200

    def test_get_bucket_tagging(self, s3control, bucket):
        resp = s3control.get_bucket_tagging(AccountId=ACCOUNT_ID, Bucket=bucket)
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200

    def test_get_bucket_versioning(self, s3control, bucket):
        resp = s3control.get_bucket_versioning(AccountId=ACCOUNT_ID, Bucket=bucket)
        assert "Status" in resp

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
        """Verify CreatedAt timestamp is present."""
        get_resp = s3control.get_access_grants_instance(AccountId=ACCOUNT_ID)
        assert "CreatedAt" in get_resp

    def test_access_grants_location_arn_format(self, s3control):
        """Verify location ARN contains expected components."""
        resp = s3control.create_access_grants_location(
            AccountId=ACCOUNT_ID,
            LocationScope="s3://arn-check-bucket/",
            IAMRoleArn="arn:aws:iam::123456789012:role/test-role",
        )
        arn = resp["AccessGrantsLocationArn"]
        assert "s3" in arn.lower()
        assert ACCOUNT_ID in arn or "access-grants" in arn.lower()

    def test_access_grants_instance_arn_format(self, s3control):
        """Verify instance ARN contains expected components."""
        resp = s3control.get_access_grants_instance(AccountId=ACCOUNT_ID)
        arn = resp["AccessGrantsInstanceArn"]
        assert "arn:" in arn
        assert "s3" in arn.lower()

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
        """GetAccessGrantsInstanceForPrefix works when instance exists."""
        resp = s3control.get_access_grants_instance_for_prefix(
            AccountId=ACCOUNT_ID, S3Prefix="s3://any-bucket/prefix"
        )
        assert "AccessGrantsInstanceArn" in resp
        assert "AccessGrantsInstanceId" in resp


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
        """Get access point after deletion returns error."""
        ap_name = f"ap-del-{_uid()}"
        try:
            s3control.create_access_point(AccountId=ACCOUNT_ID, Name=ap_name, Bucket=bucket)
            s3control.delete_access_point(AccountId=ACCOUNT_ID, Name=ap_name)
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
        """PutAccessGrantsInstanceResourcePolicy sets a policy and returns it."""
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
        resp = s3control.put_access_grants_instance_resource_policy(
            AccountId=ACCOUNT_ID, Policy=policy_doc
        )
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200
        assert "Policy" in resp

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
        """GetMultiRegionAccessPointRoutes returns routes for a MRAP."""
        mrap_name, _ = mrap_with_bucket
        resp = s3control.get_multi_region_access_point_routes(AccountId=ACCOUNT_ID, Mrap=mrap_name)
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200
        assert "Routes" in resp
        assert isinstance(resp["Routes"], list)

    def test_submit_multi_region_access_point_routes(self, s3control, mrap_with_bucket):
        """SubmitMultiRegionAccessPointRoutes updates route configuration."""
        mrap_name, bucket = mrap_with_bucket
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

    def test_get_mrap_routes_not_found(self, s3control):
        """GetMultiRegionAccessPointRoutes for nonexistent MRAP raises error."""
        with pytest.raises(ClientError) as exc_info:
            s3control.get_multi_region_access_point_routes(
                AccountId=ACCOUNT_ID, Mrap=f"nonexistent-{_uid()}"
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
        """Created job should appear in ListJobs."""
        job_id, _ = job_with_bucket
        resp = s3control.list_jobs(AccountId=ACCOUNT_ID)
        job_ids = [j["JobId"] for j in resp["Jobs"]]
        assert job_id in job_ids

    def test_create_job_describe(self, s3control, job_with_bucket):
        """Created job should be describable."""
        job_id, _ = job_with_bucket
        resp = s3control.describe_job(AccountId=ACCOUNT_ID, JobId=job_id)
        assert resp["Job"]["JobId"] == job_id
        assert resp["Job"]["Priority"] == 10

    def test_update_job_priority(self, s3control, job_with_bucket):
        """UpdateJobPriority changes the job's priority."""
        job_id, _ = job_with_bucket
        resp = s3control.update_job_priority(AccountId=ACCOUNT_ID, JobId=job_id, Priority=42)
        assert resp["JobId"] == job_id
        assert resp["Priority"] == 42

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
        """PutBucketTagging sets tags on a bucket."""
        resp = s3control.put_bucket_tagging(
            AccountId=ACCOUNT_ID,
            Bucket=bucket,
            Tagging={"TagSet": [{"Key": "env", "Value": "test"}]},
        )
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200

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
        """PutBucketPolicy sets a policy on a bucket."""
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
        resp = s3control.put_bucket_policy(AccountId=ACCOUNT_ID, Bucket=bucket, Policy=policy)
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200

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
        """DeleteBucketLifecycleConfiguration removes lifecycle rules."""
        resp = s3control.delete_bucket_lifecycle_configuration(AccountId=ACCOUNT_ID, Bucket=bucket)
        assert resp["ResponseMetadata"]["HTTPStatusCode"] in (200, 204)

    def test_put_bucket_replication(self, s3control, bucket):
        """PutBucketReplication sets replication config."""
        resp = s3control.put_bucket_replication(
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
                        "Destination": {
                            "Bucket": f"arn:aws:s3:::{bucket}",
                            "Account": ACCOUNT_ID,
                        },
                        "DeleteMarkerReplication": {"Status": "Disabled"},
                    }
                ],
            },
        )
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200

    def test_delete_bucket_replication(self, s3control, bucket):
        """DeleteBucketReplication removes replication config."""
        resp = s3control.delete_bucket_replication(AccountId=ACCOUNT_ID, Bucket=bucket)
        assert resp["ResponseMetadata"]["HTTPStatusCode"] in (200, 204)


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
        """GetJobTagging returns tags for a job."""
        resp = s3control.get_job_tagging(AccountId=ACCOUNT_ID, JobId=job_id)
        assert "Tags" in resp
        assert isinstance(resp["Tags"], list)

    def test_delete_job_tagging(self, s3control, job_id):
        """DeleteJobTagging removes tags from a job."""
        resp = s3control.delete_job_tagging(AccountId=ACCOUNT_ID, JobId=job_id)
        assert resp["ResponseMetadata"]["HTTPStatusCode"] in (200, 204)

    def test_delete_then_get_job_tagging(self, s3control, job_id):
        """After DeleteJobTagging, GetJobTagging returns empty tags."""
        s3control.delete_job_tagging(AccountId=ACCOUNT_ID, JobId=job_id)
        resp = s3control.get_job_tagging(AccountId=ACCOUNT_ID, JobId=job_id)
        assert resp["Tags"] == []


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
