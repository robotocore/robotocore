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

    def test_delete_public_access_block_idempotent(self, s3control):
        """Deleting public access block when not set still returns success."""
        # Delete first to ensure clean state
        try:
            s3control.delete_public_access_block(AccountId="123456789012")
        except ClientError:
            pass
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

    def test_create_access_point_with_vpc(self, s3control, s3):
        """CreateAccessPoint with VpcConfiguration sets NetworkOrigin to VPC."""
        ec2 = make_client("ec2")
        bucket = f"ap-vpc-{_uid()}"
        ap_name = f"ap-vpc-{_uid()}"
        s3.create_bucket(Bucket=bucket)
        vpc = ec2.create_vpc(CidrBlock="10.98.0.0/16")
        vpc_id = vpc["Vpc"]["VpcId"]
        try:
            s3control.create_access_point(
                AccountId=ACCOUNT_ID,
                Name=ap_name,
                Bucket=bucket,
                VpcConfiguration={"VpcId": vpc_id},
            )
            resp = s3control.get_access_point(AccountId=ACCOUNT_ID, Name=ap_name)
            assert resp["NetworkOrigin"] == "VPC"
            assert resp["VpcConfiguration"]["VpcId"] == vpc_id
        finally:
            try:
                s3control.delete_access_point(AccountId=ACCOUNT_ID, Name=ap_name)
            except Exception:
                pass
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
                    pass
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
                pass
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
                pass
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
                pass
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
                pass
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
                pass

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
                pass

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
                pass

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
                pass

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
                pass

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
                    pass

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
                pass

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
                pass


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
                pass
            try:
                s3.delete_bucket(Bucket=bucket1)
            except Exception:
                pass
            try:
                s3.delete_bucket(Bucket=bucket2)
            except Exception:
                pass

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
                pass
            try:
                s3.delete_bucket(Bucket=bucket_name)
            except Exception:
                pass


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

    def test_get_access_grants_instance(self, s3control):
        resp = s3control.get_access_grants_instance(AccountId=ACCOUNT_ID)
        assert "ResponseMetadata" in resp

    def test_get_access_grants_instance_for_prefix(self, s3control):
        resp = s3control.get_access_grants_instance_for_prefix(
            AccountId=ACCOUNT_ID, S3Prefix="s3://my-bucket/prefix"
        )
        assert "ResponseMetadata" in resp

    def test_get_access_grants_instance_resource_policy(self, s3control):
        resp = s3control.get_access_grants_instance_resource_policy(AccountId=ACCOUNT_ID)
        assert "ResponseMetadata" in resp

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
                pass

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
                pass

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
                pass

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
                pass

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
                pass

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
                pass


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
            pass

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
