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
            pass
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
                pass
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
                pass
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
                    pass
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
                    pass
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
                    pass
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
                pass
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
                pass
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
                pass
            try:
                s3control.delete_access_point(AccountId=ACCOUNT_ID, Name=ap_name)
            except Exception:
                pass
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
                pass
            try:
                s3control.delete_access_point(AccountId=ACCOUNT_ID, Name=ap_name)
            except Exception:
                pass
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
                pass
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
                pass
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
                pass
            try:
                s3control.delete_access_point(AccountId=ACCOUNT_ID, Name=ap_name)
            except Exception:
                pass
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
                    pass
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
                pass
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
                pass
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
                pass
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
                pass
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
                pass
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
                pass
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
                pass
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
                pass
            s3.delete_bucket(Bucket=bucket)


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
                pass

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
                pass

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
                pass

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
                pass
