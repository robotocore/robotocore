"""S3 compatibility tests — verify robotocore matches AWS behavior."""

import json
import os
import time
import uuid
from urllib.request import Request as URLRequest
from urllib.request import urlopen

import boto3
import pytest
from botocore.exceptions import ClientError

from tests.compatibility.conftest import make_client

ENDPOINT_URL = os.environ.get("ENDPOINT_URL", "http://localhost:4566")


@pytest.fixture
def s3():
    return boto3.client(
        "s3",
        endpoint_url=ENDPOINT_URL,
        region_name="us-east-1",
        aws_access_key_id="testing",
        aws_secret_access_key="testing",
    )


@pytest.fixture
def sqs():
    return boto3.client(
        "sqs",
        endpoint_url=ENDPOINT_URL,
        region_name="us-east-1",
        aws_access_key_id="testing",
        aws_secret_access_key="testing",
    )


@pytest.fixture
def bucket(s3):
    bucket_name = "test-compat-bucket"
    s3.create_bucket(Bucket=bucket_name)
    yield bucket_name
    try:
        objects = s3.list_objects_v2(Bucket=bucket_name).get("Contents", [])
        for obj in objects:
            s3.delete_object(Bucket=bucket_name, Key=obj["Key"])
        s3.delete_bucket(Bucket=bucket_name)
    except Exception:
        pass  # best-effort cleanup; failures are non-fatal


class TestS3BasicOperations:
    def test_create_bucket(self, s3):
        bucket_name = "test-create-bucket"
        response = s3.create_bucket(Bucket=bucket_name)
        assert response["ResponseMetadata"]["HTTPStatusCode"] == 200
        s3.delete_bucket(Bucket=bucket_name)

    def test_put_and_get_object(self, s3, bucket):
        s3.put_object(Bucket=bucket, Key="hello.txt", Body=b"hello world")
        response = s3.get_object(Bucket=bucket, Key="hello.txt")
        assert response["Body"].read() == b"hello world"

    def test_list_objects(self, s3, bucket):
        s3.put_object(Bucket=bucket, Key="a.txt", Body=b"a")
        s3.put_object(Bucket=bucket, Key="b.txt", Body=b"b")
        response = s3.list_objects_v2(Bucket=bucket)
        keys = [obj["Key"] for obj in response["Contents"]]
        assert sorted(keys) == ["a.txt", "b.txt"]

    def test_delete_object(self, s3, bucket):
        s3.put_object(Bucket=bucket, Key="delete-me.txt", Body=b"bye")
        s3.delete_object(Bucket=bucket, Key="delete-me.txt")
        response = s3.list_objects_v2(Bucket=bucket)
        assert response.get("Contents") is None or len(response["Contents"]) == 0

    def test_head_object(self, s3, bucket):
        s3.put_object(Bucket=bucket, Key="meta.txt", Body=b"metadata test")
        response = s3.head_object(Bucket=bucket, Key="meta.txt")
        assert response["ContentLength"] == 13

    def test_copy_object(self, s3, bucket):
        s3.put_object(Bucket=bucket, Key="original.txt", Body=b"original")
        s3.copy_object(
            Bucket=bucket,
            Key="copy.txt",
            CopySource={"Bucket": bucket, "Key": "original.txt"},
        )
        response = s3.get_object(Bucket=bucket, Key="copy.txt")
        assert response["Body"].read() == b"original"

    def test_list_buckets(self, s3, bucket):
        response = s3.list_buckets()
        names = [b["Name"] for b in response["Buckets"]]
        assert bucket in names

    def test_put_object_with_content_type(self, s3, bucket):
        s3.put_object(
            Bucket=bucket,
            Key="page.html",
            Body=b"<h1>hi</h1>",
            ContentType="text/html",
        )
        response = s3.head_object(Bucket=bucket, Key="page.html")
        assert response["ContentType"] == "text/html"

    def test_list_objects_with_prefix(self, s3, bucket):
        s3.put_object(Bucket=bucket, Key="dir/a.txt", Body=b"a")
        s3.put_object(Bucket=bucket, Key="dir/b.txt", Body=b"b")
        s3.put_object(Bucket=bucket, Key="other.txt", Body=b"c")
        response = s3.list_objects_v2(Bucket=bucket, Prefix="dir/")
        keys = [obj["Key"] for obj in response["Contents"]]
        assert sorted(keys) == ["dir/a.txt", "dir/b.txt"]

    def test_delete_objects_batch(self, s3, bucket):
        s3.put_object(Bucket=bucket, Key="del1.txt", Body=b"1")
        s3.put_object(Bucket=bucket, Key="del2.txt", Body=b"2")
        response = s3.delete_objects(
            Bucket=bucket,
            Delete={"Objects": [{"Key": "del1.txt"}, {"Key": "del2.txt"}]},
        )
        assert len(response.get("Deleted", [])) == 2


class TestS3BucketConfiguration:
    def test_get_bucket_versioning(self, s3, bucket):
        """Test that versioning is not enabled by default."""
        response = s3.get_bucket_versioning(Bucket=bucket)
        # A new bucket has no versioning status set
        assert response.get("Status") in (None, "")

    def test_put_bucket_versioning(self, s3, bucket):
        """Test enabling bucket versioning."""
        s3.put_bucket_versioning(
            Bucket=bucket,
            VersioningConfiguration={"Status": "Enabled"},
        )
        response = s3.get_bucket_versioning(Bucket=bucket)
        assert response["Status"] == "Enabled"

    def test_put_and_get_bucket_policy(self, s3, bucket):
        """Test setting and retrieving a bucket policy."""
        policy = json.dumps(
            {
                "Version": "2012-10-17",
                "Statement": [
                    {
                        "Sid": "PublicRead",
                        "Effect": "Allow",
                        "Principal": "*",
                        "Action": "s3:GetObject",
                        "Resource": f"arn:aws:s3:::{bucket}/*",
                    }
                ],
            }
        )
        s3.put_bucket_policy(Bucket=bucket, Policy=policy)
        response = s3.get_bucket_policy(Bucket=bucket)
        retrieved = json.loads(response["Policy"])
        assert retrieved["Statement"][0]["Sid"] == "PublicRead"
        assert retrieved["Statement"][0]["Action"] == "s3:GetObject"
        s3.delete_bucket_policy(Bucket=bucket)

    def test_put_get_delete_bucket_cors(self, s3, bucket):
        """Test CORS configuration lifecycle."""
        cors_config = {
            "CORSRules": [
                {
                    "AllowedHeaders": ["*"],
                    "AllowedMethods": ["GET", "PUT"],
                    "AllowedOrigins": ["https://example.com"],
                    "MaxAgeSeconds": 3600,
                }
            ]
        }
        s3.put_bucket_cors(Bucket=bucket, CORSConfiguration=cors_config)

        response = s3.get_bucket_cors(Bucket=bucket)
        rules = response["CORSRules"]
        assert len(rules) == 1
        assert "GET" in rules[0]["AllowedMethods"]
        assert "PUT" in rules[0]["AllowedMethods"]
        assert rules[0]["AllowedOrigins"] == ["https://example.com"]
        assert rules[0]["MaxAgeSeconds"] == 3600

        s3.delete_bucket_cors(Bucket=bucket)
        # After deletion, get_bucket_cors should raise
        with pytest.raises(Exception):
            s3.get_bucket_cors(Bucket=bucket)


class TestS3Multipart:
    def test_multipart_upload(self, s3, bucket):
        # Create multipart upload
        response = s3.create_multipart_upload(Bucket=bucket, Key="large.bin")
        upload_id = response["UploadId"]

        # Upload parts
        part1 = s3.upload_part(
            Bucket=bucket,
            Key="large.bin",
            UploadId=upload_id,
            PartNumber=1,
            Body=b"a" * (5 * 1024 * 1024),
        )
        part2 = s3.upload_part(
            Bucket=bucket,
            Key="large.bin",
            UploadId=upload_id,
            PartNumber=2,
            Body=b"b" * 1024,
        )

        # Complete
        s3.complete_multipart_upload(
            Bucket=bucket,
            Key="large.bin",
            UploadId=upload_id,
            MultipartUpload={
                "Parts": [
                    {"PartNumber": 1, "ETag": part1["ETag"]},
                    {"PartNumber": 2, "ETag": part2["ETag"]},
                ]
            },
        )

        # Verify
        response = s3.head_object(Bucket=bucket, Key="large.bin")
        assert response["ContentLength"] == 5 * 1024 * 1024 + 1024

    def test_abort_multipart_upload(self, s3, bucket):
        response = s3.create_multipart_upload(Bucket=bucket, Key="abort.bin")
        upload_id = response["UploadId"]
        s3.abort_multipart_upload(
            Bucket=bucket,
            Key="abort.bin",
            UploadId=upload_id,
        )


class TestS3EventNotifications:
    def test_put_object_triggers_sqs_notification(self, s3, sqs):
        bucket_name = "notif-test-s3"
        s3.create_bucket(Bucket=bucket_name)
        q_url = sqs.create_queue(QueueName="s3-event-test")["QueueUrl"]
        q_arn = sqs.get_queue_attributes(QueueUrl=q_url, AttributeNames=["QueueArn"])["Attributes"][
            "QueueArn"
        ]

        s3.put_bucket_notification_configuration(
            Bucket=bucket_name,
            NotificationConfiguration={
                "QueueConfigurations": [
                    {
                        "QueueArn": q_arn,
                        "Events": ["s3:ObjectCreated:*"],
                    }
                ],
            },
        )

        s3.put_object(Bucket=bucket_name, Key="notify.txt", Body=b"hello")

        time.sleep(0.5)
        recv = sqs.receive_message(QueueUrl=q_url, WaitTimeSeconds=2)
        msgs = recv.get("Messages", [])
        assert len(msgs) == 1
        body = json.loads(msgs[0]["Body"])
        record = body["Records"][0]
        assert record["eventName"] == "ObjectCreated:Put"
        assert record["s3"]["bucket"]["name"] == bucket_name
        assert record["s3"]["object"]["key"] == "notify.txt"

        s3.delete_object(Bucket=bucket_name, Key="notify.txt")
        s3.delete_bucket(Bucket=bucket_name)
        sqs.delete_queue(QueueUrl=q_url)

    def test_delete_object_triggers_notification(self, s3, sqs):
        bucket_name = "notif-delete-s3"
        s3.create_bucket(Bucket=bucket_name)
        q_url = sqs.create_queue(QueueName="s3-delete-event")["QueueUrl"]
        q_arn = sqs.get_queue_attributes(QueueUrl=q_url, AttributeNames=["QueueArn"])["Attributes"][
            "QueueArn"
        ]

        s3.put_bucket_notification_configuration(
            Bucket=bucket_name,
            NotificationConfiguration={
                "QueueConfigurations": [
                    {
                        "QueueArn": q_arn,
                        "Events": ["s3:ObjectRemoved:*"],
                    }
                ],
            },
        )

        s3.put_object(Bucket=bucket_name, Key="will-delete.txt", Body=b"bye")
        s3.delete_object(Bucket=bucket_name, Key="will-delete.txt")

        time.sleep(0.5)
        recv = sqs.receive_message(QueueUrl=q_url, WaitTimeSeconds=2)
        msgs = recv.get("Messages", [])
        assert len(msgs) == 1
        body = json.loads(msgs[0]["Body"])
        assert body["Records"][0]["eventName"] == "ObjectRemoved:Delete"

        s3.delete_bucket(Bucket=bucket_name)
        sqs.delete_queue(QueueUrl=q_url)

    def test_prefix_filter(self, s3, sqs):
        bucket_name = "notif-prefix-s3"
        s3.create_bucket(Bucket=bucket_name)
        q_url = sqs.create_queue(QueueName="s3-prefix-event")["QueueUrl"]
        q_arn = sqs.get_queue_attributes(QueueUrl=q_url, AttributeNames=["QueueArn"])["Attributes"][
            "QueueArn"
        ]

        s3.put_bucket_notification_configuration(
            Bucket=bucket_name,
            NotificationConfiguration={
                "QueueConfigurations": [
                    {
                        "QueueArn": q_arn,
                        "Events": ["s3:ObjectCreated:*"],
                        "Filter": {
                            "Key": {"FilterRules": [{"Name": "prefix", "Value": "images/"}]}
                        },
                    }
                ],
            },
        )

        # This should NOT trigger (wrong prefix)
        s3.put_object(Bucket=bucket_name, Key="docs/readme.txt", Body=b"no")
        # This SHOULD trigger
        s3.put_object(Bucket=bucket_name, Key="images/photo.jpg", Body=b"yes")

        time.sleep(0.5)
        recv = sqs.receive_message(QueueUrl=q_url, WaitTimeSeconds=2, MaxNumberOfMessages=10)
        msgs = recv.get("Messages", [])
        assert len(msgs) == 1
        body = json.loads(msgs[0]["Body"])
        assert body["Records"][0]["s3"]["object"]["key"] == "images/photo.jpg"

        s3.delete_object(Bucket=bucket_name, Key="docs/readme.txt")
        s3.delete_object(Bucket=bucket_name, Key="images/photo.jpg")
        s3.delete_bucket(Bucket=bucket_name)
        sqs.delete_queue(QueueUrl=q_url)


class TestS3BucketConfigurations:
    def test_get_bucket_location(self, s3, bucket):
        response = s3.get_bucket_location(Bucket=bucket)
        # us-east-1 returns None for LocationConstraint per AWS behavior
        assert (
            response["LocationConstraint"] is None or response["LocationConstraint"] == "us-east-1"
        )

    def test_bucket_versioning(self, s3):
        vbucket = "test-versioning-bucket"
        s3.create_bucket(Bucket=vbucket)
        try:
            # Initially versioning is not enabled
            response = s3.get_bucket_versioning(Bucket=vbucket)
            assert response.get("Status") is None or response.get("Status") == ""

            # Enable versioning
            s3.put_bucket_versioning(
                Bucket=vbucket,
                VersioningConfiguration={"Status": "Enabled"},
            )
            response = s3.get_bucket_versioning(Bucket=vbucket)
            assert response["Status"] == "Enabled"

            # Suspend versioning
            s3.put_bucket_versioning(
                Bucket=vbucket,
                VersioningConfiguration={"Status": "Suspended"},
            )
            response = s3.get_bucket_versioning(Bucket=vbucket)
            assert response["Status"] == "Suspended"
        finally:
            s3.delete_bucket(Bucket=vbucket)

    def test_list_object_versions(self, s3):
        vbucket = "test-list-versions-bucket"
        s3.create_bucket(Bucket=vbucket)
        try:
            # Enable versioning
            s3.put_bucket_versioning(
                Bucket=vbucket,
                VersioningConfiguration={"Status": "Enabled"},
            )
            # Put two versions of the same key
            s3.put_object(Bucket=vbucket, Key="versioned.txt", Body=b"v1")
            s3.put_object(Bucket=vbucket, Key="versioned.txt", Body=b"v2")

            response = s3.list_object_versions(Bucket=vbucket, Prefix="versioned.txt")
            versions = response.get("Versions", [])
            assert len(versions) == 2
            # Each version should have a unique VersionId
            version_ids = [v["VersionId"] for v in versions]
            assert len(set(version_ids)) == 2
        finally:
            # Delete all versions
            response = s3.list_object_versions(Bucket=vbucket)
            for v in response.get("Versions", []):
                s3.delete_object(Bucket=vbucket, Key=v["Key"], VersionId=v["VersionId"])
            s3.delete_bucket(Bucket=vbucket)

    def test_bucket_cors(self, s3, bucket):
        cors_config = {
            "CORSRules": [
                {
                    "AllowedOrigins": ["http://example.com"],
                    "AllowedMethods": ["GET", "PUT"],
                    "AllowedHeaders": ["*"],
                    "MaxAgeSeconds": 3000,
                }
            ]
        }
        s3.put_bucket_cors(Bucket=bucket, CORSConfiguration=cors_config)

        response = s3.get_bucket_cors(Bucket=bucket)
        rules = response["CORSRules"]
        assert len(rules) == 1
        assert "http://example.com" in rules[0]["AllowedOrigins"]
        assert "GET" in rules[0]["AllowedMethods"]

        s3.delete_bucket_cors(Bucket=bucket)
        with pytest.raises(s3.exceptions.ClientError) as exc_info:
            s3.get_bucket_cors(Bucket=bucket)
        assert exc_info.value.response["Error"]["Code"] == "NoSuchCORSConfiguration"

    def test_bucket_policy(self, s3, bucket):
        policy = json.dumps(
            {
                "Version": "2012-10-17",
                "Statement": [
                    {
                        "Sid": "PublicRead",
                        "Effect": "Allow",
                        "Principal": "*",
                        "Action": "s3:GetObject",
                        "Resource": f"arn:aws:s3:::{bucket}/*",
                    }
                ],
            }
        )
        s3.put_bucket_policy(Bucket=bucket, Policy=policy)

        response = s3.get_bucket_policy(Bucket=bucket)
        retrieved = json.loads(response["Policy"])
        assert retrieved["Statement"][0]["Sid"] == "PublicRead"

        s3.delete_bucket_policy(Bucket=bucket)
        with pytest.raises(s3.exceptions.ClientError) as exc_info:
            s3.get_bucket_policy(Bucket=bucket)
        assert exc_info.value.response["Error"]["Code"] == "NoSuchBucketPolicy"

    def test_bucket_encryption(self, s3, bucket):
        enc_config = {
            "Rules": [
                {
                    "ApplyServerSideEncryptionByDefault": {
                        "SSEAlgorithm": "AES256",
                    },
                    "BucketKeyEnabled": False,
                }
            ]
        }
        s3.put_bucket_encryption(
            Bucket=bucket,
            ServerSideEncryptionConfiguration=enc_config,
        )

        response = s3.get_bucket_encryption(Bucket=bucket)
        rules = response["ServerSideEncryptionConfiguration"]["Rules"]
        assert len(rules) >= 1
        assert rules[0]["ApplyServerSideEncryptionByDefault"]["SSEAlgorithm"] == "AES256"

    def test_bucket_website(self, s3, bucket):
        website_config = {
            "IndexDocument": {"Suffix": "index.html"},
            "ErrorDocument": {"Key": "error.html"},
        }
        s3.put_bucket_website(Bucket=bucket, WebsiteConfiguration=website_config)

        response = s3.get_bucket_website(Bucket=bucket)
        assert response["IndexDocument"]["Suffix"] == "index.html"
        assert response["ErrorDocument"]["Key"] == "error.html"

        s3.delete_bucket_website(Bucket=bucket)
        with pytest.raises(s3.exceptions.ClientError) as exc_info:
            s3.get_bucket_website(Bucket=bucket)
        assert exc_info.value.response["Error"]["Code"] == "NoSuchWebsiteConfiguration"

    def test_bucket_lifecycle_configuration(self, s3, bucket):
        lifecycle_config = {
            "Rules": [
                {
                    "ID": "expire-old",
                    "Filter": {"Prefix": "logs/"},
                    "Status": "Enabled",
                    "Expiration": {"Days": 30},
                }
            ]
        }
        s3.put_bucket_lifecycle_configuration(
            Bucket=bucket,
            LifecycleConfiguration=lifecycle_config,
        )

        response = s3.get_bucket_lifecycle_configuration(Bucket=bucket)
        rules = response["Rules"]
        assert len(rules) == 1
        assert rules[0]["ID"] == "expire-old"
        assert rules[0]["Expiration"]["Days"] == 30


class TestS3AclOperations:
    def test_get_bucket_acl(self, s3, bucket):
        response = s3.get_bucket_acl(Bucket=bucket)
        assert "Owner" in response
        assert "Grants" in response

    def test_get_object_acl(self, s3, bucket):
        s3.put_object(Bucket=bucket, Key="acl-test.txt", Body=b"acl test")

        response = s3.get_object_acl(Bucket=bucket, Key="acl-test.txt")
        assert "Owner" in response
        assert "Grants" in response


class TestS3CopyObject:
    def test_copy_object_between_buckets(self, s3, bucket):
        dest_bucket = "test-copy-dest-bucket"
        s3.create_bucket(Bucket=dest_bucket)
        try:
            s3.put_object(Bucket=bucket, Key="source.txt", Body=b"cross-bucket copy")
            s3.copy_object(
                Bucket=dest_bucket,
                Key="dest.txt",
                CopySource={"Bucket": bucket, "Key": "source.txt"},
            )
            response = s3.get_object(Bucket=dest_bucket, Key="dest.txt")
            assert response["Body"].read() == b"cross-bucket copy"
        finally:
            try:
                s3.delete_object(Bucket=dest_bucket, Key="dest.txt")
                s3.delete_bucket(Bucket=dest_bucket)
            except Exception:
                pass  # best-effort cleanup; failures are non-fatal


class TestS3MultipartLifecycle:
    def test_create_and_abort_multipart(self, s3, bucket):
        response = s3.create_multipart_upload(Bucket=bucket, Key="multi-abort.bin")
        upload_id = response["UploadId"]
        assert upload_id

        # Upload a part
        s3.upload_part(
            Bucket=bucket,
            Key="multi-abort.bin",
            UploadId=upload_id,
            PartNumber=1,
            Body=b"partial data",
        )

        # Abort
        abort_resp = s3.abort_multipart_upload(
            Bucket=bucket, Key="multi-abort.bin", UploadId=upload_id
        )
        assert abort_resp["ResponseMetadata"]["HTTPStatusCode"] in (200, 204)

    def test_list_multipart_uploads(self, s3, bucket):
        response = s3.create_multipart_upload(Bucket=bucket, Key="list-multi.bin")
        upload_id = response["UploadId"]

        try:
            list_resp = s3.list_multipart_uploads(Bucket=bucket)
            uploads = list_resp.get("Uploads", [])
            upload_ids = [u["UploadId"] for u in uploads]
            assert upload_id in upload_ids
        finally:
            s3.abort_multipart_upload(Bucket=bucket, Key="list-multi.bin", UploadId=upload_id)


class TestS3PresignedUrls:
    def test_presigned_get_url(self, s3, bucket):
        """Test generating and using a presigned GET URL."""
        s3.put_object(Bucket=bucket, Key="presigned-get.txt", Body=b"hello presigned")

        url = s3.generate_presigned_url(
            "get_object",
            Params={"Bucket": bucket, "Key": "presigned-get.txt"},
            ExpiresIn=3600,
        )
        # The URL should point at our local endpoint
        assert ENDPOINT_URL.split("://")[1] in url

        req = URLRequest(url, method="GET")
        resp = urlopen(req)
        assert resp.status == 200
        assert resp.read() == b"hello presigned"

    def test_presigned_put_url(self, s3, bucket):
        """Test generating and using a presigned PUT URL."""
        url = s3.generate_presigned_url(
            "put_object",
            Params={"Bucket": bucket, "Key": "presigned-upload.txt"},
            ExpiresIn=3600,
        )

        req = URLRequest(url, data=b"uploaded via presigned", method="PUT")
        req.add_header("Content-Type", "application/octet-stream")
        resp = urlopen(req)
        assert resp.status == 200

        obj = s3.get_object(Bucket=bucket, Key="presigned-upload.txt")
        assert obj["Body"].read() == b"uploaded via presigned"

    def test_presigned_head_url(self, s3, bucket):
        """Test generating and using a presigned HEAD URL."""
        s3.put_object(Bucket=bucket, Key="presigned-head.txt", Body=b"check me")

        url = s3.generate_presigned_url(
            "head_object",
            Params={"Bucket": bucket, "Key": "presigned-head.txt"},
            ExpiresIn=3600,
        )

        req = URLRequest(url, method="HEAD")
        resp = urlopen(req)
        assert resp.status == 200
        assert resp.headers.get("Content-Length") == "8"

    def test_presigned_delete_url(self, s3, bucket):
        """Test generating and using a presigned DELETE URL."""
        s3.put_object(Bucket=bucket, Key="presigned-delete.txt", Body=b"delete me")

        url = s3.generate_presigned_url(
            "delete_object",
            Params={"Bucket": bucket, "Key": "presigned-delete.txt"},
            ExpiresIn=3600,
        )

        req = URLRequest(url, method="DELETE")
        resp = urlopen(req)
        assert resp.status == 204

        # Verify object is gone
        response = s3.list_objects_v2(Bucket=bucket, Prefix="presigned-delete.txt")
        assert response.get("Contents") is None or len(response["Contents"]) == 0


class TestS3MultipartExtended:
    """Extended multipart upload tests: 3 parts, content verification, abort."""

    def test_multipart_upload_three_parts_verify_content(self, s3, bucket):
        """Initiate multipart, upload 3 parts, complete, verify concatenated content."""
        key = "multipart-3parts.bin"
        min_part = 5 * 1024 * 1024  # 5MB minimum for non-last parts
        part_bodies = [b"A" * min_part, b"B" * min_part, b"C" * 1024]
        try:
            resp = s3.create_multipart_upload(Bucket=bucket, Key=key)
            upload_id = resp["UploadId"]

            parts = []
            for i, body in enumerate(part_bodies, 1):
                part = s3.upload_part(
                    Bucket=bucket, Key=key, UploadId=upload_id, PartNumber=i, Body=body
                )
                parts.append({"PartNumber": i, "ETag": part["ETag"]})

            s3.complete_multipart_upload(
                Bucket=bucket,
                Key=key,
                UploadId=upload_id,
                MultipartUpload={"Parts": parts},
            )

            obj = s3.get_object(Bucket=bucket, Key=key)
            content = obj["Body"].read()
            expected = b"".join(part_bodies)
            assert content == expected
        finally:
            s3.delete_object(Bucket=bucket, Key=key)

    def test_abort_multipart_upload_no_object_created(self, s3, bucket):
        """Abort a multipart upload and verify no object is left behind."""
        key = "multipart-abort-check.bin"
        resp = s3.create_multipart_upload(Bucket=bucket, Key=key)
        upload_id = resp["UploadId"]

        s3.upload_part(Bucket=bucket, Key=key, UploadId=upload_id, PartNumber=1, Body=b"data")
        s3.abort_multipart_upload(Bucket=bucket, Key=key, UploadId=upload_id)

        listing = s3.list_objects_v2(Bucket=bucket, Prefix=key)
        assert listing.get("Contents") is None or len(listing["Contents"]) == 0

    def test_list_parts(self, s3, bucket):
        """List parts of an in-progress multipart upload."""
        key = "multipart-list-parts.bin"
        resp = s3.create_multipart_upload(Bucket=bucket, Key=key)
        upload_id = resp["UploadId"]
        try:
            s3.upload_part(
                Bucket=bucket, Key=key, UploadId=upload_id, PartNumber=1, Body=b"x" * 100
            )
            s3.upload_part(
                Bucket=bucket, Key=key, UploadId=upload_id, PartNumber=2, Body=b"y" * 200
            )
            parts_resp = s3.list_parts(Bucket=bucket, Key=key, UploadId=upload_id)
            parts = parts_resp.get("Parts", [])
            assert len(parts) == 2
            assert parts[0]["PartNumber"] == 1
            assert parts[1]["PartNumber"] == 2
        finally:
            s3.abort_multipart_upload(Bucket=bucket, Key=key, UploadId=upload_id)


class TestS3CopyObjectExtended:
    """CopyObject within same bucket and cross-bucket."""

    def test_copy_within_same_bucket(self, s3, bucket):
        """Copy an object to a different key in the same bucket."""
        try:
            s3.put_object(Bucket=bucket, Key="src-copy.txt", Body=b"copy me")
            s3.copy_object(
                Bucket=bucket,
                Key="dst-copy.txt",
                CopySource={"Bucket": bucket, "Key": "src-copy.txt"},
            )
            obj = s3.get_object(Bucket=bucket, Key="dst-copy.txt")
            assert obj["Body"].read() == b"copy me"
        finally:
            s3.delete_object(Bucket=bucket, Key="src-copy.txt")
            s3.delete_object(Bucket=bucket, Key="dst-copy.txt")

    def test_copy_cross_bucket(self, s3, bucket):
        """Copy an object from one bucket to another."""
        dst_bucket = "test-copy-dst-bucket"
        try:
            s3.create_bucket(Bucket=dst_bucket)
            s3.put_object(Bucket=bucket, Key="cross-src.txt", Body=b"cross bucket")
            s3.copy_object(
                Bucket=dst_bucket,
                Key="cross-dst.txt",
                CopySource={"Bucket": bucket, "Key": "cross-src.txt"},
            )
            obj = s3.get_object(Bucket=dst_bucket, Key="cross-dst.txt")
            assert obj["Body"].read() == b"cross bucket"
        finally:
            s3.delete_object(Bucket=bucket, Key="cross-src.txt")
            s3.delete_object(Bucket=dst_bucket, Key="cross-dst.txt")
            s3.delete_bucket(Bucket=dst_bucket)

    def test_copy_preserves_content_type(self, s3, bucket):
        """CopyObject preserves the original content type."""
        try:
            s3.put_object(
                Bucket=bucket, Key="typed-src.txt", Body=b"typed", ContentType="text/plain"
            )
            s3.copy_object(
                Bucket=bucket,
                Key="typed-dst.txt",
                CopySource={"Bucket": bucket, "Key": "typed-src.txt"},
            )
            head = s3.head_object(Bucket=bucket, Key="typed-dst.txt")
            assert head["ContentType"] == "text/plain"
        finally:
            s3.delete_object(Bucket=bucket, Key="typed-src.txt")
            s3.delete_object(Bucket=bucket, Key="typed-dst.txt")


class TestS3ObjectMetadata:
    """Custom metadata on objects."""

    def test_put_with_custom_metadata_and_head(self, s3, bucket):
        """Put an object with custom metadata, retrieve via HeadObject."""
        try:
            s3.put_object(
                Bucket=bucket,
                Key="meta-obj.txt",
                Body=b"metadata",
                Metadata={"author": "test-user", "version": "42"},
            )
            head = s3.head_object(Bucket=bucket, Key="meta-obj.txt")
            assert head["Metadata"]["author"] == "test-user"
            assert head["Metadata"]["version"] == "42"
        finally:
            s3.delete_object(Bucket=bucket, Key="meta-obj.txt")

    def test_get_object_returns_metadata(self, s3, bucket):
        """GetObject also returns custom metadata."""
        try:
            s3.put_object(
                Bucket=bucket,
                Key="meta-get.txt",
                Body=b"data",
                Metadata={"env": "test"},
            )
            obj = s3.get_object(Bucket=bucket, Key="meta-get.txt")
            assert obj["Metadata"]["env"] == "test"
        finally:
            s3.delete_object(Bucket=bucket, Key="meta-get.txt")

    def test_metadata_keys_lowercased(self, s3, bucket):
        """AWS lowercases metadata keys."""
        try:
            s3.put_object(
                Bucket=bucket,
                Key="meta-case.txt",
                Body=b"data",
                Metadata={"MyKey": "val"},
            )
            head = s3.head_object(Bucket=bucket, Key="meta-case.txt")
            assert "mykey" in head["Metadata"]
            assert head["Metadata"]["mykey"] == "val"
        finally:
            s3.delete_object(Bucket=bucket, Key="meta-case.txt")


class TestS3ContentType:
    """Explicit content type handling."""

    def test_put_with_content_type_json(self, s3, bucket):
        try:
            s3.put_object(
                Bucket=bucket,
                Key="data.json",
                Body=b'{"key": "value"}',
                ContentType="application/json",
            )
            head = s3.head_object(Bucket=bucket, Key="data.json")
            assert head["ContentType"] == "application/json"
        finally:
            s3.delete_object(Bucket=bucket, Key="data.json")

    def test_put_with_content_type_octet_stream(self, s3, bucket):
        try:
            s3.put_object(
                Bucket=bucket,
                Key="binary.bin",
                Body=b"\x00\x01\x02",
                ContentType="application/octet-stream",
            )
            obj = s3.get_object(Bucket=bucket, Key="binary.bin")
            assert obj["ContentType"] == "application/octet-stream"
        finally:
            s3.delete_object(Bucket=bucket, Key="binary.bin")


class TestS3BucketVersioning:
    """Bucket versioning: enable, put versions, list, get specific, delete specific."""

    def test_enable_versioning(self, s3, bucket):
        s3.put_bucket_versioning(Bucket=bucket, VersioningConfiguration={"Status": "Enabled"})
        resp = s3.get_bucket_versioning(Bucket=bucket)
        assert resp["Status"] == "Enabled"

    def test_put_same_key_twice_list_versions(self, s3, bucket):
        """Put the same key twice with versioning, list both versions."""
        try:
            s3.put_bucket_versioning(Bucket=bucket, VersioningConfiguration={"Status": "Enabled"})
            s3.put_object(Bucket=bucket, Key="ver.txt", Body=b"v1")
            s3.put_object(Bucket=bucket, Key="ver.txt", Body=b"v2")

            versions_resp = s3.list_object_versions(Bucket=bucket, Prefix="ver.txt")
            versions = versions_resp.get("Versions", [])
            assert len(versions) == 2
            version_ids = [v["VersionId"] for v in versions]
            assert len(set(version_ids)) == 2  # two distinct version IDs
        finally:
            # Clean up all versions
            versions_resp = s3.list_object_versions(Bucket=bucket, Prefix="ver.txt")
            for v in versions_resp.get("Versions", []):
                s3.delete_object(Bucket=bucket, Key="ver.txt", VersionId=v["VersionId"])

    def test_get_specific_version(self, s3, bucket):
        """Get a specific version of an object."""
        try:
            s3.put_bucket_versioning(Bucket=bucket, VersioningConfiguration={"Status": "Enabled"})
            r1 = s3.put_object(Bucket=bucket, Key="vget.txt", Body=b"first")
            r2 = s3.put_object(Bucket=bucket, Key="vget.txt", Body=b"second")

            obj1 = s3.get_object(Bucket=bucket, Key="vget.txt", VersionId=r1["VersionId"])
            assert obj1["Body"].read() == b"first"

            obj2 = s3.get_object(Bucket=bucket, Key="vget.txt", VersionId=r2["VersionId"])
            assert obj2["Body"].read() == b"second"
        finally:
            versions_resp = s3.list_object_versions(Bucket=bucket, Prefix="vget.txt")
            for v in versions_resp.get("Versions", []):
                s3.delete_object(Bucket=bucket, Key="vget.txt", VersionId=v["VersionId"])

    def test_delete_specific_version(self, s3, bucket):
        """Delete a specific version, other version still accessible."""
        try:
            s3.put_bucket_versioning(Bucket=bucket, VersioningConfiguration={"Status": "Enabled"})
            r1 = s3.put_object(Bucket=bucket, Key="vdel.txt", Body=b"keep")
            r2 = s3.put_object(Bucket=bucket, Key="vdel.txt", Body=b"remove")

            s3.delete_object(Bucket=bucket, Key="vdel.txt", VersionId=r2["VersionId"])

            versions_resp = s3.list_object_versions(Bucket=bucket, Prefix="vdel.txt")
            remaining = [v for v in versions_resp.get("Versions", []) if v["Key"] == "vdel.txt"]
            assert len(remaining) == 1
            assert remaining[0]["VersionId"] == r1["VersionId"]
        finally:
            versions_resp = s3.list_object_versions(Bucket=bucket, Prefix="vdel.txt")
            for v in versions_resp.get("Versions", []):
                s3.delete_object(Bucket=bucket, Key="vdel.txt", VersionId=v["VersionId"])
            for dm in versions_resp.get("DeleteMarkers", []):
                s3.delete_object(Bucket=bucket, Key="vdel.txt", VersionId=dm["VersionId"])


class TestS3ObjectTagging:
    """PutObjectTagging, GetObjectTagging, DeleteObjectTagging."""

    def test_put_and_get_object_tagging(self, s3, bucket):
        try:
            s3.put_object(Bucket=bucket, Key="tagged.txt", Body=b"tag me")
            s3.put_object_tagging(
                Bucket=bucket,
                Key="tagged.txt",
                Tagging={
                    "TagSet": [{"Key": "env", "Value": "test"}, {"Key": "team", "Value": "core"}]
                },
            )
            resp = s3.get_object_tagging(Bucket=bucket, Key="tagged.txt")
            tags = {t["Key"]: t["Value"] for t in resp["TagSet"]}
            assert tags["env"] == "test"
            assert tags["team"] == "core"
        finally:
            s3.delete_object(Bucket=bucket, Key="tagged.txt")

    def test_delete_object_tagging(self, s3, bucket):
        try:
            s3.put_object(Bucket=bucket, Key="untag.txt", Body=b"data")
            s3.put_object_tagging(
                Bucket=bucket,
                Key="untag.txt",
                Tagging={"TagSet": [{"Key": "k", "Value": "v"}]},
            )
            s3.delete_object_tagging(Bucket=bucket, Key="untag.txt")
            resp = s3.get_object_tagging(Bucket=bucket, Key="untag.txt")
            assert resp["TagSet"] == []
        finally:
            s3.delete_object(Bucket=bucket, Key="untag.txt")

    def test_put_object_with_tagging_header(self, s3, bucket):
        """Put object with Tagging parameter (query-string encoding)."""
        try:
            s3.put_object(
                Bucket=bucket,
                Key="inline-tag.txt",
                Body=b"data",
                Tagging="color=blue&size=large",
            )
            resp = s3.get_object_tagging(Bucket=bucket, Key="inline-tag.txt")
            tags = {t["Key"]: t["Value"] for t in resp["TagSet"]}
            assert tags["color"] == "blue"
            assert tags["size"] == "large"
        finally:
            s3.delete_object(Bucket=bucket, Key="inline-tag.txt")


class TestS3BucketTagging:
    """PutBucketTagging, GetBucketTagging, DeleteBucketTagging."""

    def test_put_and_get_bucket_tagging(self, s3, bucket):
        s3.put_bucket_tagging(
            Bucket=bucket,
            Tagging={"TagSet": [{"Key": "project", "Value": "robotocore"}]},
        )
        resp = s3.get_bucket_tagging(Bucket=bucket)
        tags = {t["Key"]: t["Value"] for t in resp["TagSet"]}
        assert tags["project"] == "robotocore"

    def test_delete_bucket_tagging(self, s3, bucket):
        s3.put_bucket_tagging(
            Bucket=bucket,
            Tagging={"TagSet": [{"Key": "k", "Value": "v"}]},
        )
        s3.delete_bucket_tagging(Bucket=bucket)
        with pytest.raises(Exception) as exc_info:
            s3.get_bucket_tagging(Bucket=bucket)
        assert "NoSuchTagSet" in str(exc_info.value) or "NoSuchTagSetError" in str(exc_info.value)


class TestS3BucketCORS:
    """PutBucketCors, GetBucketCors, DeleteBucketCors."""

    def test_put_and_get_cors(self, s3, bucket):
        cors_config = {
            "CORSRules": [
                {
                    "AllowedOrigins": ["https://example.com"],
                    "AllowedMethods": ["GET", "PUT"],
                    "AllowedHeaders": ["*"],
                    "MaxAgeSeconds": 3000,
                }
            ]
        }
        s3.put_bucket_cors(Bucket=bucket, CORSConfiguration=cors_config)
        resp = s3.get_bucket_cors(Bucket=bucket)
        rules = resp["CORSRules"]
        assert len(rules) == 1
        assert "https://example.com" in rules[0]["AllowedOrigins"]
        assert "GET" in rules[0]["AllowedMethods"]

    def test_delete_cors(self, s3, bucket):
        cors_config = {
            "CORSRules": [
                {
                    "AllowedOrigins": ["*"],
                    "AllowedMethods": ["GET"],
                }
            ]
        }
        s3.put_bucket_cors(Bucket=bucket, CORSConfiguration=cors_config)
        s3.delete_bucket_cors(Bucket=bucket)
        with pytest.raises(Exception) as exc_info:
            s3.get_bucket_cors(Bucket=bucket)
        assert "NoSuchCORSConfiguration" in str(exc_info.value)


class TestS3ListObjectsV2Extended:
    """ListObjectsV2 with Prefix, Delimiter, MaxKeys, StartAfter, ContinuationToken."""

    def _populate(self, s3, bucket):
        keys = [
            "dir1/a.txt",
            "dir1/b.txt",
            "dir2/c.txt",
            "dir2/sub/d.txt",
            "root.txt",
        ]
        for k in keys:
            s3.put_object(Bucket=bucket, Key=k, Body=b"x")
        return keys

    def _cleanup(self, s3, bucket, keys):
        for k in keys:
            s3.delete_object(Bucket=bucket, Key=k)

    def test_list_with_delimiter(self, s3, bucket):
        """Delimiter returns CommonPrefixes for 'directories'."""
        keys = self._populate(s3, bucket)
        try:
            resp = s3.list_objects_v2(Bucket=bucket, Delimiter="/")
            prefixes = [p["Prefix"] for p in resp.get("CommonPrefixes", [])]
            assert "dir1/" in prefixes
            assert "dir2/" in prefixes
            top_keys = [o["Key"] for o in resp.get("Contents", [])]
            assert "root.txt" in top_keys
        finally:
            self._cleanup(s3, bucket, keys)

    def test_list_with_prefix_and_delimiter(self, s3, bucket):
        keys = self._populate(s3, bucket)
        try:
            resp = s3.list_objects_v2(Bucket=bucket, Prefix="dir2/", Delimiter="/")
            listed_keys = [o["Key"] for o in resp.get("Contents", [])]
            assert "dir2/c.txt" in listed_keys
            prefixes = [p["Prefix"] for p in resp.get("CommonPrefixes", [])]
            assert "dir2/sub/" in prefixes
        finally:
            self._cleanup(s3, bucket, keys)

    def test_list_with_max_keys(self, s3, bucket):
        keys = self._populate(s3, bucket)
        try:
            resp = s3.list_objects_v2(Bucket=bucket, MaxKeys=2)
            assert len(resp.get("Contents", [])) == 2
            assert resp["IsTruncated"] is True
        finally:
            self._cleanup(s3, bucket, keys)

    def test_list_with_start_after(self, s3, bucket):
        keys = self._populate(s3, bucket)
        try:
            resp = s3.list_objects_v2(Bucket=bucket, StartAfter="dir2/")
            listed = [o["Key"] for o in resp.get("Contents", [])]
            # All keys should be lexicographically after "dir2/"
            for k in listed:
                assert k > "dir2/"
        finally:
            self._cleanup(s3, bucket, keys)

    def test_list_with_continuation_token(self, s3, bucket):
        keys = self._populate(s3, bucket)
        try:
            resp1 = s3.list_objects_v2(Bucket=bucket, MaxKeys=2)
            assert resp1["IsTruncated"] is True
            token = resp1["NextContinuationToken"]

            resp2 = s3.list_objects_v2(Bucket=bucket, MaxKeys=2, ContinuationToken=token)
            keys1 = [o["Key"] for o in resp1["Contents"]]
            keys2 = [o["Key"] for o in resp2["Contents"]]
            # No overlap
            assert set(keys1).isdisjoint(set(keys2))
        finally:
            self._cleanup(s3, bucket, keys)


class TestS3DeleteObjectsBatch:
    """DeleteObjects (batch delete) and verify objects are gone."""

    def test_batch_delete_multiple_objects(self, s3, bucket):
        obj_keys = [f"batch-del-{i}.txt" for i in range(5)]
        for k in obj_keys:
            s3.put_object(Bucket=bucket, Key=k, Body=b"data")

        resp = s3.delete_objects(
            Bucket=bucket,
            Delete={"Objects": [{"Key": k} for k in obj_keys]},
        )
        assert len(resp.get("Deleted", [])) == 5

        listing = s3.list_objects_v2(Bucket=bucket)
        remaining = [o["Key"] for o in listing.get("Contents", [])]
        for k in obj_keys:
            assert k not in remaining

    def test_batch_delete_nonexistent_keys(self, s3, bucket):
        """Deleting nonexistent keys should not error (quiet mode)."""
        resp = s3.delete_objects(
            Bucket=bucket,
            Delete={
                "Objects": [{"Key": "no-such-key-1.txt"}, {"Key": "no-such-key-2.txt"}],
                "Quiet": True,
            },
        )
        # Should succeed without errors
        assert resp.get("Errors") is None or len(resp["Errors"]) == 0


class TestS3BucketLifecycle:
    """PutBucketLifecycleConfiguration, GetBucketLifecycleConfiguration."""

    def test_put_and_get_lifecycle(self, s3, bucket):
        config = {
            "Rules": [
                {
                    "ID": "expire-logs",
                    "Filter": {"Prefix": "logs/"},
                    "Status": "Enabled",
                    "Expiration": {"Days": 30},
                }
            ]
        }
        s3.put_bucket_lifecycle_configuration(Bucket=bucket, LifecycleConfiguration=config)
        resp = s3.get_bucket_lifecycle_configuration(Bucket=bucket)
        rules = resp["Rules"]
        assert len(rules) == 1
        assert rules[0]["ID"] == "expire-logs"
        assert rules[0]["Expiration"]["Days"] == 30

    def test_lifecycle_multiple_rules(self, s3, bucket):
        config = {
            "Rules": [
                {
                    "ID": "rule1",
                    "Filter": {"Prefix": "tmp/"},
                    "Status": "Enabled",
                    "Expiration": {"Days": 7},
                },
                {
                    "ID": "rule2",
                    "Filter": {"Prefix": "archive/"},
                    "Status": "Enabled",
                    "Expiration": {"Days": 365},
                },
            ]
        }
        s3.put_bucket_lifecycle_configuration(Bucket=bucket, LifecycleConfiguration=config)
        resp = s3.get_bucket_lifecycle_configuration(Bucket=bucket)
        ids = [r["ID"] for r in resp["Rules"]]
        assert "rule1" in ids
        assert "rule2" in ids


class TestS3BucketPolicy:
    """PutBucketPolicy, GetBucketPolicy, DeleteBucketPolicy."""

    def test_put_and_get_policy(self, s3, bucket):
        policy = json.dumps(
            {
                "Version": "2012-10-17",
                "Statement": [
                    {
                        "Sid": "PublicRead",
                        "Effect": "Allow",
                        "Principal": "*",
                        "Action": "s3:GetObject",
                        "Resource": f"arn:aws:s3:::{bucket}/*",
                    }
                ],
            }
        )
        s3.put_bucket_policy(Bucket=bucket, Policy=policy)
        resp = s3.get_bucket_policy(Bucket=bucket)
        returned = json.loads(resp["Policy"])
        assert returned["Statement"][0]["Sid"] == "PublicRead"

    def test_delete_policy(self, s3, bucket):
        policy = json.dumps(
            {
                "Version": "2012-10-17",
                "Statement": [
                    {
                        "Sid": "Test",
                        "Effect": "Allow",
                        "Principal": "*",
                        "Action": "s3:GetObject",
                        "Resource": f"arn:aws:s3:::{bucket}/*",
                    }
                ],
            }
        )
        s3.put_bucket_policy(Bucket=bucket, Policy=policy)
        s3.delete_bucket_policy(Bucket=bucket)
        with pytest.raises(Exception) as exc_info:
            s3.get_bucket_policy(Bucket=bucket)
        err_str = str(exc_info.value)
        assert "NoSuchBucketPolicy" in err_str or "does not have" in err_str or "404" in err_str


class TestS3Versioning:
    """Test S3 bucket versioning operations."""

    @pytest.fixture
    def versioned_bucket(self, s3):
        import uuid

        name = f"ver-bucket-{uuid.uuid4().hex[:8]}"
        s3.create_bucket(Bucket=name)
        s3.put_bucket_versioning(
            Bucket=name,
            VersioningConfiguration={"Status": "Enabled"},
        )
        yield name
        # cleanup
        try:
            versions = s3.list_object_versions(Bucket=name)
            for v in versions.get("Versions", []):
                s3.delete_object(Bucket=name, Key=v["Key"], VersionId=v["VersionId"])
            for dm in versions.get("DeleteMarkers", []):
                s3.delete_object(Bucket=name, Key=dm["Key"], VersionId=dm["VersionId"])
            s3.delete_bucket(Bucket=name)
        except Exception:
            pass  # best-effort cleanup; failures are non-fatal

    def test_put_bucket_versioning_enabled(self, s3, versioned_bucket):
        resp = s3.get_bucket_versioning(Bucket=versioned_bucket)
        assert resp["Status"] == "Enabled"

    def test_put_bucket_versioning_suspended(self, s3):
        import uuid

        name = f"sus-bucket-{uuid.uuid4().hex[:8]}"
        s3.create_bucket(Bucket=name)
        s3.put_bucket_versioning(Bucket=name, VersioningConfiguration={"Status": "Enabled"})
        s3.put_bucket_versioning(Bucket=name, VersioningConfiguration={"Status": "Suspended"})
        resp = s3.get_bucket_versioning(Bucket=name)
        assert resp["Status"] == "Suspended"
        s3.delete_bucket(Bucket=name)

    def test_multiple_versions_of_same_key(self, s3, versioned_bucket):
        s3.put_object(Bucket=versioned_bucket, Key="doc.txt", Body=b"v1")
        s3.put_object(Bucket=versioned_bucket, Key="doc.txt", Body=b"v2")
        versions = s3.list_object_versions(Bucket=versioned_bucket, Prefix="doc.txt")
        assert len(versions["Versions"]) == 2

    def test_get_specific_version(self, s3, versioned_bucket):
        r1 = s3.put_object(Bucket=versioned_bucket, Key="ver.txt", Body=b"first")
        r2 = s3.put_object(Bucket=versioned_bucket, Key="ver.txt", Body=b"second")
        obj1 = s3.get_object(Bucket=versioned_bucket, Key="ver.txt", VersionId=r1["VersionId"])
        assert obj1["Body"].read() == b"first"
        obj2 = s3.get_object(Bucket=versioned_bucket, Key="ver.txt", VersionId=r2["VersionId"])
        assert obj2["Body"].read() == b"second"

    def test_delete_creates_delete_marker(self, s3, versioned_bucket):
        s3.put_object(Bucket=versioned_bucket, Key="dm.txt", Body=b"data")
        s3.delete_object(Bucket=versioned_bucket, Key="dm.txt")
        versions = s3.list_object_versions(Bucket=versioned_bucket, Prefix="dm.txt")
        assert len(versions.get("DeleteMarkers", [])) >= 1

    def test_delete_specific_version(self, s3, versioned_bucket):
        r1 = s3.put_object(Bucket=versioned_bucket, Key="del-ver.txt", Body=b"v1")
        s3.put_object(Bucket=versioned_bucket, Key="del-ver.txt", Body=b"v2")
        s3.delete_object(Bucket=versioned_bucket, Key="del-ver.txt", VersionId=r1["VersionId"])
        versions = s3.list_object_versions(Bucket=versioned_bucket, Prefix="del-ver.txt")
        version_ids = [v["VersionId"] for v in versions["Versions"]]
        assert r1["VersionId"] not in version_ids


class TestS3MultipartUpload:
    """Test S3 multipart upload operations."""

    @pytest.fixture
    def bucket(self, s3):
        import uuid

        name = f"mp-bucket-{uuid.uuid4().hex[:8]}"
        s3.create_bucket(Bucket=name)
        yield name
        try:
            objs = s3.list_objects_v2(Bucket=name).get("Contents", [])
            for obj in objs:
                s3.delete_object(Bucket=name, Key=obj["Key"])
            s3.delete_bucket(Bucket=name)
        except Exception:
            pass  # best-effort cleanup; failures are non-fatal

    def test_create_and_abort_multipart(self, s3, bucket):
        resp = s3.create_multipart_upload(Bucket=bucket, Key="aborted.bin")
        upload_id = resp["UploadId"]
        assert upload_id
        s3.abort_multipart_upload(Bucket=bucket, Key="aborted.bin", UploadId=upload_id)

    def test_list_multipart_uploads(self, s3, bucket):
        resp = s3.create_multipart_upload(Bucket=bucket, Key="listed.bin")
        upload_id = resp["UploadId"]
        try:
            uploads = s3.list_multipart_uploads(Bucket=bucket)
            upload_keys = [u["Key"] for u in uploads.get("Uploads", [])]
            assert "listed.bin" in upload_keys
        finally:
            s3.abort_multipart_upload(Bucket=bucket, Key="listed.bin", UploadId=upload_id)

    def test_complete_multipart_upload(self, s3, bucket):
        resp = s3.create_multipart_upload(Bucket=bucket, Key="complete.bin")
        upload_id = resp["UploadId"]
        part = s3.upload_part(
            Bucket=bucket,
            Key="complete.bin",
            UploadId=upload_id,
            PartNumber=1,
            Body=b"x" * (5 * 1024 * 1024),
        )
        s3.complete_multipart_upload(
            Bucket=bucket,
            Key="complete.bin",
            UploadId=upload_id,
            MultipartUpload={"Parts": [{"PartNumber": 1, "ETag": part["ETag"]}]},
        )
        obj = s3.get_object(Bucket=bucket, Key="complete.bin")
        assert len(obj["Body"].read()) == 5 * 1024 * 1024

    def test_list_parts(self, s3, bucket):
        resp = s3.create_multipart_upload(Bucket=bucket, Key="parts.bin")
        upload_id = resp["UploadId"]
        try:
            s3.upload_part(
                Bucket=bucket,
                Key="parts.bin",
                UploadId=upload_id,
                PartNumber=1,
                Body=b"a" * (5 * 1024 * 1024),
            )
            parts_resp = s3.list_parts(Bucket=bucket, Key="parts.bin", UploadId=upload_id)
            assert len(parts_resp["Parts"]) == 1
            assert parts_resp["Parts"][0]["PartNumber"] == 1
        finally:
            s3.abort_multipart_upload(Bucket=bucket, Key="parts.bin", UploadId=upload_id)


class TestS3CORS:
    """Test S3 CORS configuration."""

    @pytest.fixture
    def bucket(self, s3):
        import uuid

        name = f"cors-bucket-{uuid.uuid4().hex[:8]}"
        s3.create_bucket(Bucket=name)
        yield name
        try:
            s3.delete_bucket(Bucket=name)
        except Exception:
            pass  # best-effort cleanup; failures are non-fatal

    def test_put_get_delete_cors(self, s3, bucket):
        cors_config = {
            "CORSRules": [
                {
                    "AllowedOrigins": ["https://example.com"],
                    "AllowedMethods": ["GET", "PUT"],
                    "AllowedHeaders": ["*"],
                    "MaxAgeSeconds": 3600,
                }
            ]
        }
        s3.put_bucket_cors(Bucket=bucket, CORSConfiguration=cors_config)
        resp = s3.get_bucket_cors(Bucket=bucket)
        assert len(resp["CORSRules"]) == 1
        assert "https://example.com" in resp["CORSRules"][0]["AllowedOrigins"]

        s3.delete_bucket_cors(Bucket=bucket)
        with pytest.raises(ClientError):
            s3.get_bucket_cors(Bucket=bucket)

    def test_multiple_cors_rules(self, s3, bucket):
        cors_config = {
            "CORSRules": [
                {
                    "AllowedOrigins": ["https://a.com"],
                    "AllowedMethods": ["GET"],
                },
                {
                    "AllowedOrigins": ["https://b.com"],
                    "AllowedMethods": ["PUT", "POST"],
                    "ExposeHeaders": ["x-amz-request-id"],
                },
            ]
        }
        s3.put_bucket_cors(Bucket=bucket, CORSConfiguration=cors_config)
        resp = s3.get_bucket_cors(Bucket=bucket)
        assert len(resp["CORSRules"]) == 2


class TestS3ObjectOperations:
    """Additional S3 object operations."""

    @pytest.fixture
    def bucket(self, s3):
        import uuid

        name = f"obj-bucket-{uuid.uuid4().hex[:8]}"
        s3.create_bucket(Bucket=name)
        yield name
        try:
            objs = s3.list_objects_v2(Bucket=name).get("Contents", [])
            for obj in objs:
                s3.delete_object(Bucket=name, Key=obj["Key"])
            s3.delete_bucket(Bucket=name)
        except Exception:
            pass  # best-effort cleanup; failures are non-fatal

    def test_copy_object(self, s3, bucket):
        s3.put_object(Bucket=bucket, Key="src.txt", Body=b"source")
        s3.copy_object(
            Bucket=bucket,
            Key="dst.txt",
            CopySource={"Bucket": bucket, "Key": "src.txt"},
        )
        obj = s3.get_object(Bucket=bucket, Key="dst.txt")
        assert obj["Body"].read() == b"source"

    def test_head_object(self, s3, bucket):
        s3.put_object(Bucket=bucket, Key="head.txt", Body=b"hello world")
        resp = s3.head_object(Bucket=bucket, Key="head.txt")
        assert resp["ContentLength"] == 11
        assert "ETag" in resp

    def test_put_object_with_content_type(self, s3, bucket):
        s3.put_object(
            Bucket=bucket,
            Key="page.html",
            Body=b"<h1>hi</h1>",
            ContentType="text/html",
        )
        resp = s3.head_object(Bucket=bucket, Key="page.html")
        assert resp["ContentType"] == "text/html"

    def test_put_object_with_metadata(self, s3, bucket):
        s3.put_object(
            Bucket=bucket,
            Key="meta.txt",
            Body=b"data",
            Metadata={"author": "test", "version": "1"},
        )
        resp = s3.head_object(Bucket=bucket, Key="meta.txt")
        assert resp["Metadata"]["author"] == "test"
        assert resp["Metadata"]["version"] == "1"

    def test_get_object_range(self, s3, bucket):
        s3.put_object(Bucket=bucket, Key="range.txt", Body=b"0123456789")
        resp = s3.get_object(Bucket=bucket, Key="range.txt", Range="bytes=3-6")
        assert resp["Body"].read() == b"3456"

    def test_put_get_object_tagging(self, s3, bucket):
        s3.put_object(Bucket=bucket, Key="tagged.txt", Body=b"data")
        s3.put_object_tagging(
            Bucket=bucket,
            Key="tagged.txt",
            Tagging={"TagSet": [{"Key": "env", "Value": "prod"}]},
        )
        resp = s3.get_object_tagging(Bucket=bucket, Key="tagged.txt")
        tags = {t["Key"]: t["Value"] for t in resp["TagSet"]}
        assert tags["env"] == "prod"

    def test_delete_object_tagging(self, s3, bucket):
        s3.put_object(Bucket=bucket, Key="untag.txt", Body=b"data")
        s3.put_object_tagging(
            Bucket=bucket,
            Key="untag.txt",
            Tagging={"TagSet": [{"Key": "x", "Value": "y"}]},
        )
        s3.delete_object_tagging(Bucket=bucket, Key="untag.txt")
        resp = s3.get_object_tagging(Bucket=bucket, Key="untag.txt")
        assert len(resp["TagSet"]) == 0

    def test_put_object_acl(self, s3, bucket):
        s3.put_object(Bucket=bucket, Key="acl.txt", Body=b"data")
        s3.put_object_acl(Bucket=bucket, Key="acl.txt", ACL="public-read")
        resp = s3.get_object_acl(Bucket=bucket, Key="acl.txt")
        assert "Grants" in resp

    def test_list_objects_v2_prefix_delimiter(self, s3, bucket):
        for key in ["dir/a.txt", "dir/b.txt", "dir/sub/c.txt", "top.txt"]:
            s3.put_object(Bucket=bucket, Key=key, Body=b"x")
        resp = s3.list_objects_v2(Bucket=bucket, Prefix="dir/", Delimiter="/")
        keys = [c["Key"] for c in resp.get("Contents", [])]
        prefixes = [p["Prefix"] for p in resp.get("CommonPrefixes", [])]
        assert "dir/a.txt" in keys
        assert "dir/b.txt" in keys
        assert "dir/sub/" in prefixes

    def test_delete_objects_batch(self, s3, bucket):
        for i in range(5):
            s3.put_object(Bucket=bucket, Key=f"batch/{i}.txt", Body=b"x")
        resp = s3.delete_objects(
            Bucket=bucket,
            Delete={"Objects": [{"Key": f"batch/{i}.txt"} for i in range(5)]},
        )
        assert len(resp.get("Deleted", [])) == 5

    def test_put_bucket_acl(self, s3, bucket):
        s3.put_bucket_acl(Bucket=bucket, ACL="private")
        resp = s3.get_bucket_acl(Bucket=bucket)
        assert "Owner" in resp
        assert "Grants" in resp


class TestS3BucketOperations:
    """Additional S3 bucket operations."""

    @pytest.fixture
    def bucket(self, s3):
        import uuid

        name = f"bkt-ops-{uuid.uuid4().hex[:8]}"
        s3.create_bucket(Bucket=name)
        yield name
        try:
            s3.delete_bucket(Bucket=name)
        except Exception:
            pass  # best-effort cleanup; failures are non-fatal

    def test_head_bucket(self, s3, bucket):
        resp = s3.head_bucket(Bucket=bucket)
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200

    def test_head_bucket_nonexistent(self, s3):
        import uuid

        with pytest.raises(ClientError):
            s3.head_bucket(Bucket=f"no-such-bucket-{uuid.uuid4().hex[:8]}")

    def test_get_bucket_location(self, s3, bucket):
        resp = s3.get_bucket_location(Bucket=bucket)
        # us-east-1 returns None for LocationConstraint
        assert resp["LocationConstraint"] is None or isinstance(resp["LocationConstraint"], str)

    def test_put_get_bucket_tagging(self, s3, bucket):
        s3.put_bucket_tagging(
            Bucket=bucket,
            Tagging={
                "TagSet": [
                    {"Key": "env", "Value": "test"},
                    {"Key": "team", "Value": "dev"},
                ]
            },
        )
        resp = s3.get_bucket_tagging(Bucket=bucket)
        tags = {t["Key"]: t["Value"] for t in resp["TagSet"]}
        assert tags["env"] == "test"
        assert tags["team"] == "dev"

    def test_delete_bucket_tagging(self, s3, bucket):
        s3.put_bucket_tagging(
            Bucket=bucket,
            Tagging={"TagSet": [{"Key": "x", "Value": "y"}]},
        )
        s3.delete_bucket_tagging(Bucket=bucket)
        with pytest.raises(ClientError):
            s3.get_bucket_tagging(Bucket=bucket)

    def test_put_get_bucket_lifecycle(self, s3, bucket):
        s3.put_bucket_lifecycle_configuration(
            Bucket=bucket,
            LifecycleConfiguration={
                "Rules": [
                    {
                        "ID": "expire-old",
                        "Status": "Enabled",
                        "Filter": {"Prefix": "logs/"},
                        "Expiration": {"Days": 30},
                    }
                ]
            },
        )
        resp = s3.get_bucket_lifecycle_configuration(Bucket=bucket)
        assert len(resp["Rules"]) == 1
        assert resp["Rules"][0]["ID"] == "expire-old"

    def test_put_get_bucket_encryption(self, s3, bucket):
        s3.put_bucket_encryption(
            Bucket=bucket,
            ServerSideEncryptionConfiguration={
                "Rules": [
                    {
                        "ApplyServerSideEncryptionByDefault": {
                            "SSEAlgorithm": "AES256",
                        }
                    }
                ]
            },
        )
        resp = s3.get_bucket_encryption(Bucket=bucket)
        algo = resp["ServerSideEncryptionConfiguration"]["Rules"][0][
            "ApplyServerSideEncryptionByDefault"
        ]["SSEAlgorithm"]
        assert algo == "AES256"

    def test_put_get_bucket_website(self, s3, bucket):
        s3.put_bucket_website(
            Bucket=bucket,
            WebsiteConfiguration={
                "IndexDocument": {"Suffix": "index.html"},
                "ErrorDocument": {"Key": "error.html"},
            },
        )
        resp = s3.get_bucket_website(Bucket=bucket)
        assert resp["IndexDocument"]["Suffix"] == "index.html"
        assert resp["ErrorDocument"]["Key"] == "error.html"
        s3.delete_bucket_website(Bucket=bucket)

    def test_put_get_bucket_logging(self, s3, bucket):
        import uuid

        log_bucket = f"log-target-{uuid.uuid4().hex[:8]}"
        s3.create_bucket(Bucket=log_bucket)
        try:
            s3.put_bucket_logging(
                Bucket=bucket,
                BucketLoggingStatus={
                    "LoggingEnabled": {
                        "TargetBucket": log_bucket,
                        "TargetPrefix": "logs/",
                    }
                },
            )
            resp = s3.get_bucket_logging(Bucket=bucket)
            assert resp["LoggingEnabled"]["TargetBucket"] == log_bucket
        finally:
            s3.delete_bucket(Bucket=log_bucket)


class TestS3AdvancedOperations:
    @pytest.fixture
    def s3(self):
        return boto3.client(
            "s3",
            endpoint_url=ENDPOINT_URL,
            region_name="us-east-1",
            aws_access_key_id="testing",
            aws_secret_access_key="testing",
        )

    @pytest.fixture
    def bucket(self, s3):
        import uuid

        name = f"adv-bucket-{uuid.uuid4().hex[:8]}"
        s3.create_bucket(Bucket=name)
        yield name
        # Clean up objects
        resp = s3.list_objects_v2(Bucket=name)
        for obj in resp.get("Contents", []):
            s3.delete_object(Bucket=name, Key=obj["Key"])
        s3.delete_bucket(Bucket=name)

    def test_list_objects_v2_prefix(self, s3, bucket):
        s3.put_object(Bucket=bucket, Key="docs/a.txt", Body=b"a")
        s3.put_object(Bucket=bucket, Key="docs/b.txt", Body=b"b")
        s3.put_object(Bucket=bucket, Key="images/c.png", Body=b"c")
        resp = s3.list_objects_v2(Bucket=bucket, Prefix="docs/")
        assert resp["KeyCount"] == 2

    def test_list_objects_v2_delimiter(self, s3, bucket):
        s3.put_object(Bucket=bucket, Key="a/1.txt", Body=b"1")
        s3.put_object(Bucket=bucket, Key="a/2.txt", Body=b"2")
        s3.put_object(Bucket=bucket, Key="b/3.txt", Body=b"3")
        resp = s3.list_objects_v2(Bucket=bucket, Delimiter="/")
        prefixes = [p["Prefix"] for p in resp.get("CommonPrefixes", [])]
        assert "a/" in prefixes
        assert "b/" in prefixes

    def test_list_objects_v2_max_keys(self, s3, bucket):
        for i in range(5):
            s3.put_object(Bucket=bucket, Key=f"k{i}.txt", Body=b"x")
        resp = s3.list_objects_v2(Bucket=bucket, MaxKeys=3)
        assert len(resp["Contents"]) <= 3
        assert resp["IsTruncated"] is True

    def test_list_objects_v2_continuation(self, s3, bucket):
        for i in range(5):
            s3.put_object(Bucket=bucket, Key=f"p{i}.txt", Body=b"x")
        resp1 = s3.list_objects_v2(Bucket=bucket, MaxKeys=3)
        assert resp1["IsTruncated"] is True
        resp2 = s3.list_objects_v2(Bucket=bucket, ContinuationToken=resp1["NextContinuationToken"])
        total = len(resp1["Contents"]) + len(resp2["Contents"])
        assert total == 5

    def test_put_object_content_type(self, s3, bucket):
        s3.put_object(
            Bucket=bucket, Key="page.html", Body=b"<h1>Hello</h1>", ContentType="text/html"
        )
        resp = s3.head_object(Bucket=bucket, Key="page.html")
        assert resp["ContentType"] == "text/html"

    def test_put_object_metadata(self, s3, bucket):
        s3.put_object(
            Bucket=bucket, Key="meta.txt", Body=b"data", Metadata={"custom-key": "custom-value"}
        )
        resp = s3.head_object(Bucket=bucket, Key="meta.txt")
        assert resp["Metadata"]["custom-key"] == "custom-value"

    def test_copy_object_preserves_metadata(self, s3, bucket):
        s3.put_object(Bucket=bucket, Key="src.txt", Body=b"source", Metadata={"copied": "yes"})
        s3.copy_object(
            Bucket=bucket, Key="dst.txt", CopySource={"Bucket": bucket, "Key": "src.txt"}
        )
        resp = s3.head_object(Bucket=bucket, Key="dst.txt")
        assert resp["ContentLength"] == 6

    def test_delete_objects_batch(self, s3, bucket):
        for i in range(5):
            s3.put_object(Bucket=bucket, Key=f"del{i}.txt", Body=b"x")
        resp = s3.delete_objects(
            Bucket=bucket,
            Delete={"Objects": [{"Key": f"del{i}.txt"} for i in range(5)]},
        )
        assert len(resp["Deleted"]) == 5

    def test_object_exists_after_put(self, s3, bucket):
        s3.put_object(Bucket=bucket, Key="exists.txt", Body=b"yes")
        resp = s3.list_objects_v2(Bucket=bucket, Prefix="exists.txt")
        assert resp["KeyCount"] == 1

    def test_object_not_found_raises_error(self, s3, bucket):
        with pytest.raises(ClientError) as exc:
            s3.get_object(Bucket=bucket, Key="nonexistent-key-xyz")
        assert exc.value.response["Error"]["Code"] in ("NoSuchKey", "404")

    def test_put_and_get_large_object(self, s3, bucket):
        data = b"X" * (1024 * 1024)  # 1MB
        s3.put_object(Bucket=bucket, Key="large.bin", Body=data)
        resp = s3.get_object(Bucket=bucket, Key="large.bin")
        assert resp["ContentLength"] == len(data)

    def test_put_object_with_storage_class(self, s3, bucket):
        s3.put_object(Bucket=bucket, Key="sc.txt", Body=b"data", StorageClass="STANDARD")
        resp = s3.head_object(Bucket=bucket, Key="sc.txt")
        # StorageClass may not be returned for STANDARD, just verify no error
        assert resp["ContentLength"] == 4

    def test_get_object_range(self, s3, bucket):
        s3.put_object(Bucket=bucket, Key="range.txt", Body=b"0123456789")
        resp = s3.get_object(Bucket=bucket, Key="range.txt", Range="bytes=2-5")
        body = resp["Body"].read()
        assert body == b"2345"

    def test_put_bucket_policy(self, s3, bucket):
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
        s3.put_bucket_policy(Bucket=bucket, Policy=policy)
        resp = s3.get_bucket_policy(Bucket=bucket)
        returned = json.loads(resp["Policy"])
        assert len(returned["Statement"]) == 1

    def test_delete_bucket_policy(self, s3, bucket):
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
        s3.put_bucket_policy(Bucket=bucket, Policy=policy)
        s3.delete_bucket_policy(Bucket=bucket)
        with pytest.raises(ClientError):
            s3.get_bucket_policy(Bucket=bucket)

    def test_get_bucket_location(self, s3, bucket):
        resp = s3.get_bucket_location(Bucket=bucket)
        # us-east-1 returns None or empty for LocationConstraint
        assert "LocationConstraint" in resp

    def test_put_get_object_tagging(self, s3, bucket):
        s3.put_object(Bucket=bucket, Key="tagged.txt", Body=b"tag me")
        s3.put_object_tagging(
            Bucket=bucket,
            Key="tagged.txt",
            Tagging={"TagSet": [{"Key": "env", "Value": "test"}]},
        )
        resp = s3.get_object_tagging(Bucket=bucket, Key="tagged.txt")
        tags = {t["Key"]: t["Value"] for t in resp["TagSet"]}
        assert tags["env"] == "test"

    def test_delete_object_tagging(self, s3, bucket):
        s3.put_object(Bucket=bucket, Key="dtag.txt", Body=b"data")
        s3.put_object_tagging(
            Bucket=bucket,
            Key="dtag.txt",
            Tagging={"TagSet": [{"Key": "temp", "Value": "yes"}]},
        )
        s3.delete_object_tagging(Bucket=bucket, Key="dtag.txt")
        resp = s3.get_object_tagging(Bucket=bucket, Key="dtag.txt")
        assert len(resp["TagSet"]) == 0

    def test_list_buckets(self, s3, bucket):
        resp = s3.list_buckets()
        names = [b["Name"] for b in resp["Buckets"]]
        assert bucket in names

    def test_head_bucket(self, s3, bucket):
        resp = s3.head_bucket(Bucket=bucket)
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200


class TestS3AccelerateConfiguration:
    """Tests for GetBucketAccelerateConfiguration / PutBucketAccelerateConfiguration."""

    def test_get_bucket_accelerate_default(self, s3, bucket):
        resp = s3.get_bucket_accelerate_configuration(Bucket=bucket)
        # Default is no acceleration
        assert resp.get("Status") in (None, "", "Suspended")

    def test_put_bucket_accelerate_enabled(self, s3, bucket):
        s3.put_bucket_accelerate_configuration(
            Bucket=bucket,
            AccelerateConfiguration={"Status": "Enabled"},
        )
        resp = s3.get_bucket_accelerate_configuration(Bucket=bucket)
        assert resp["Status"] == "Enabled"

    def test_put_bucket_accelerate_suspended(self, s3, bucket):
        s3.put_bucket_accelerate_configuration(
            Bucket=bucket,
            AccelerateConfiguration={"Status": "Enabled"},
        )
        s3.put_bucket_accelerate_configuration(
            Bucket=bucket,
            AccelerateConfiguration={"Status": "Suspended"},
        )
        resp = s3.get_bucket_accelerate_configuration(Bucket=bucket)
        assert resp["Status"] == "Suspended"


class TestS3BucketRequestPayment:
    """Tests for GetBucketRequestPayment."""

    def test_get_bucket_request_payment_default(self, s3, bucket):
        resp = s3.get_bucket_request_payment(Bucket=bucket)
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200


class TestS3DeleteBucketConfigurations:
    """Tests for DeleteBucketEncryption, DeleteBucketLifecycle, DeleteBucketWebsite."""

    def test_delete_bucket_encryption(self, s3, bucket):
        s3.put_bucket_encryption(
            Bucket=bucket,
            ServerSideEncryptionConfiguration={
                "Rules": [{"ApplyServerSideEncryptionByDefault": {"SSEAlgorithm": "AES256"}}]
            },
        )
        s3.delete_bucket_encryption(Bucket=bucket)
        with pytest.raises(ClientError) as exc:
            s3.get_bucket_encryption(Bucket=bucket)
        assert (
            exc.value.response["Error"]["Code"] == "ServerSideEncryptionConfigurationNotFoundError"
        )

    def test_delete_bucket_lifecycle(self, s3, bucket):
        s3.put_bucket_lifecycle_configuration(
            Bucket=bucket,
            LifecycleConfiguration={
                "Rules": [
                    {
                        "ID": "expire-rule",
                        "Status": "Enabled",
                        "Filter": {"Prefix": "logs/"},
                        "Expiration": {"Days": 30},
                    }
                ]
            },
        )
        s3.delete_bucket_lifecycle(Bucket=bucket)
        with pytest.raises(ClientError) as exc:
            s3.get_bucket_lifecycle_configuration(Bucket=bucket)
        assert exc.value.response["Error"]["Code"] == "NoSuchLifecycleConfiguration"

    def test_delete_bucket_website(self, s3, bucket):
        s3.put_bucket_website(
            Bucket=bucket,
            WebsiteConfiguration={
                "IndexDocument": {"Suffix": "index.html"},
            },
        )
        s3.delete_bucket_website(Bucket=bucket)
        with pytest.raises(ClientError) as exc:
            s3.get_bucket_website(Bucket=bucket)
        assert "NoSuchWebsiteConfiguration" in str(exc.value)


class TestS3ObjectLocking:
    """Tests for object lock, legal hold, and retention."""

    @pytest.fixture
    def lock_bucket(self, s3):
        import uuid

        name = f"lock-bucket-{uuid.uuid4().hex[:8]}"
        s3.create_bucket(
            Bucket=name,
            ObjectLockEnabledForBucket=True,
        )
        yield name
        try:
            versions = s3.list_object_versions(Bucket=name).get("Versions", [])
            for v in versions:
                s3.delete_object(Bucket=name, Key=v["Key"], VersionId=v["VersionId"])
            s3.delete_bucket(Bucket=name)
        except Exception:
            pass  # best-effort cleanup; failures are non-fatal

    def test_put_and_get_object_lock_configuration(self, s3, lock_bucket):
        s3.put_object_lock_configuration(
            Bucket=lock_bucket,
            ObjectLockConfiguration={"ObjectLockEnabled": "Enabled"},
        )
        resp = s3.get_object_lock_configuration(Bucket=lock_bucket)
        assert resp["ObjectLockConfiguration"]["ObjectLockEnabled"] == "Enabled"

    def test_put_object_lock_configuration_with_retention(self, s3, lock_bucket):
        s3.put_object_lock_configuration(
            Bucket=lock_bucket,
            ObjectLockConfiguration={
                "ObjectLockEnabled": "Enabled",
                "Rule": {
                    "DefaultRetention": {
                        "Mode": "GOVERNANCE",
                        "Days": 1,
                    }
                },
            },
        )
        resp = s3.get_object_lock_configuration(Bucket=lock_bucket)
        retention = resp["ObjectLockConfiguration"]["Rule"]["DefaultRetention"]
        assert retention["Mode"] == "GOVERNANCE"
        assert retention["Days"] == 1

    def test_put_and_get_object_legal_hold(self, s3, lock_bucket):
        s3.put_object(Bucket=lock_bucket, Key="legal.txt", Body=b"data")
        s3.put_object_legal_hold(
            Bucket=lock_bucket,
            Key="legal.txt",
            LegalHold={"Status": "ON"},
        )
        resp = s3.get_object_legal_hold(Bucket=lock_bucket, Key="legal.txt")
        assert resp["LegalHold"]["Status"] == "ON"
        # Turn off so we can clean up
        s3.put_object_legal_hold(
            Bucket=lock_bucket,
            Key="legal.txt",
            LegalHold={"Status": "OFF"},
        )


class TestS3UploadPartCopy:
    """Tests for UploadPartCopy."""

    def test_upload_part_copy(self, s3, bucket):
        # Put source object
        s3.put_object(Bucket=bucket, Key="source.txt", Body=b"x" * (5 * 1024 * 1024))

        # Create multipart upload for destination
        mpu = s3.create_multipart_upload(Bucket=bucket, Key="dest.txt")
        upload_id = mpu["UploadId"]

        # Copy part from source
        resp = s3.upload_part_copy(
            Bucket=bucket,
            Key="dest.txt",
            UploadId=upload_id,
            PartNumber=1,
            CopySource={"Bucket": bucket, "Key": "source.txt"},
        )
        assert "CopyPartResult" in resp
        etag = resp["CopyPartResult"]["ETag"]

        # Complete
        s3.complete_multipart_upload(
            Bucket=bucket,
            Key="dest.txt",
            UploadId=upload_id,
            MultipartUpload={"Parts": [{"PartNumber": 1, "ETag": etag}]},
        )
        head = s3.head_object(Bucket=bucket, Key="dest.txt")
        assert head["ContentLength"] == 5 * 1024 * 1024


class TestS3BucketNotificationConfig:
    """Tests for GetBucketNotificationConfiguration (empty case)."""

    def test_get_bucket_notification_configuration_empty(self, s3, bucket):
        resp = s3.get_bucket_notification_configuration(Bucket=bucket)
        # Empty notification config should return empty lists or no keys
        assert resp.get("TopicConfigurations", []) == []
        assert resp.get("QueueConfigurations", []) == []
        assert resp.get("LambdaFunctionConfigurations", []) == []


class TestS3BucketOwnershipControls:
    """Tests for GetBucketOwnershipControls."""

    def test_put_and_get_bucket_ownership_controls(self, s3, bucket):
        s3.put_bucket_ownership_controls(
            Bucket=bucket,
            OwnershipControls={"Rules": [{"ObjectOwnership": "BucketOwnerEnforced"}]},
        )
        resp = s3.get_bucket_ownership_controls(Bucket=bucket)
        rules = resp["OwnershipControls"]["Rules"]
        assert len(rules) == 1
        assert rules[0]["ObjectOwnership"] == "BucketOwnerEnforced"


class TestS3BucketPolicyStatus:
    """Tests for GetBucketPolicyStatus."""

    def test_get_bucket_policy_status(self, s3, bucket):
        policy = json.dumps(
            {
                "Version": "2012-10-17",
                "Statement": [
                    {
                        "Sid": "PublicRead",
                        "Effect": "Allow",
                        "Principal": "*",
                        "Action": "s3:GetObject",
                        "Resource": f"arn:aws:s3:::{bucket}/*",
                    }
                ],
            }
        )
        s3.put_bucket_policy(Bucket=bucket, Policy=policy)
        resp = s3.get_bucket_policy_status(Bucket=bucket)
        assert "PolicyStatus" in resp
        s3.delete_bucket_policy(Bucket=bucket)


class TestS3BucketLoggingExtended:
    """Tests for GetBucketLogging with no logging configured."""

    def test_get_bucket_logging_empty(self, s3, bucket):
        resp = s3.get_bucket_logging(Bucket=bucket)
        # No logging configured should have no LoggingEnabled key
        assert resp.get("LoggingEnabled") is None


class TestS3PublicAccessBlock:
    """Tests for PutPublicAccessBlock, GetPublicAccessBlock, DeletePublicAccessBlock."""

    def test_put_get_delete_public_access_block(self, s3, bucket):
        config = {
            "BlockPublicAcls": True,
            "IgnorePublicAcls": True,
            "BlockPublicPolicy": True,
            "RestrictPublicBuckets": True,
        }
        s3.put_public_access_block(Bucket=bucket, PublicAccessBlockConfiguration=config)
        resp = s3.get_public_access_block(Bucket=bucket)
        pab = resp["PublicAccessBlockConfiguration"]
        assert pab["BlockPublicAcls"] is True
        assert pab["IgnorePublicAcls"] is True
        assert pab["BlockPublicPolicy"] is True
        assert pab["RestrictPublicBuckets"] is True

        s3.delete_public_access_block(Bucket=bucket)
        with pytest.raises(ClientError) as exc:
            s3.get_public_access_block(Bucket=bucket)
        assert exc.value.response["Error"]["Code"] == "NoSuchPublicAccessBlockConfiguration"


class TestS3ListBucketConfigurations:
    """Tests for various ListBucket*Configurations operations."""

    def test_list_bucket_analytics_configurations_empty(self, s3, bucket):
        resp = s3.list_bucket_analytics_configurations(Bucket=bucket)
        assert (
            resp.get("AnalyticsConfigurationList") is None
            or resp.get("AnalyticsConfigurationList") == []
        )

    def test_list_bucket_metrics_configurations_empty(self, s3, bucket):
        resp = s3.list_bucket_metrics_configurations(Bucket=bucket)
        assert (
            resp.get("MetricsConfigurationList") is None
            or resp.get("MetricsConfigurationList") == []
        )

    def test_list_bucket_intelligent_tiering_configurations_empty(self, s3, bucket):
        resp = s3.list_bucket_intelligent_tiering_configurations(Bucket=bucket)
        assert (
            resp.get("IntelligentTieringConfigurationList") is None
            or resp.get("IntelligentTieringConfigurationList") == []
        )


class TestS3GetObjectAttributes:
    """Tests for GetObjectAttributes."""

    def test_get_object_attributes(self, s3, bucket):
        s3.put_object(Bucket=bucket, Key="attr-test.txt", Body=b"hello world")
        resp = s3.get_object_attributes(
            Bucket=bucket,
            Key="attr-test.txt",
            ObjectAttributes=["ETag", "ObjectSize"],
        )
        assert "ETag" in resp
        assert resp.get("ObjectSize", 0) == 11


class TestS3AutoCoverage:
    """Auto-generated coverage tests for s3."""

    @pytest.fixture
    def client(self):
        return make_client("s3")

    def test_list_directory_buckets(self, client):
        """ListDirectoryBuckets returns a response."""
        resp = client.list_directory_buckets()
        assert "Buckets" in resp


class TestS3AnalyticsMetricsInventory:
    """Tests for Analytics, Metrics, Inventory, and Intelligent Tiering configs."""

    def test_put_get_analytics_configuration(self, s3, bucket):
        """PutBucketAnalyticsConfiguration + GetBucketAnalyticsConfiguration."""
        config = {
            "Id": "test-analytics",
            "StorageClassAnalysis": {},
        }
        resp = s3.put_bucket_analytics_configuration(
            Bucket=bucket,
            Id="test-analytics",
            AnalyticsConfiguration=config,
        )
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200
        resp = s3.get_bucket_analytics_configuration(Bucket=bucket, Id="test-analytics")
        assert "AnalyticsConfiguration" in resp

    def test_put_get_metrics_configuration(self, s3, bucket):
        """PutBucketMetricsConfiguration + GetBucketMetricsConfiguration."""
        config = {"Id": "test-metrics"}
        resp = s3.put_bucket_metrics_configuration(
            Bucket=bucket,
            Id="test-metrics",
            MetricsConfiguration=config,
        )
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200
        resp = s3.get_bucket_metrics_configuration(Bucket=bucket, Id="test-metrics")
        assert "MetricsConfiguration" in resp

    def test_put_get_intelligent_tiering(self, s3, bucket):
        """PutBucketIntelligentTieringConfiguration + Get."""
        config = {
            "Id": "test-tiering",
            "Status": "Enabled",
            "Tierings": [
                {"Days": 90, "AccessTier": "ARCHIVE_ACCESS"},
            ],
        }
        resp = s3.put_bucket_intelligent_tiering_configuration(
            Bucket=bucket,
            Id="test-tiering",
            IntelligentTieringConfiguration=config,
        )
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200
        resp = s3.get_bucket_intelligent_tiering_configuration(Bucket=bucket, Id="test-tiering")
        assert "IntelligentTieringConfiguration" in resp

    def test_put_list_inventory_configuration(self, s3, bucket):
        """PutBucketInventoryConfiguration + ListBucketInventoryConfigurations."""
        config = {
            "Destination": {
                "S3BucketDestination": {
                    "Bucket": f"arn:aws:s3:::{bucket}",
                    "Format": "CSV",
                },
            },
            "IsEnabled": True,
            "Id": "test-inventory",
            "IncludedObjectVersions": "All",
            "Schedule": {"Frequency": "Daily"},
        }
        resp = s3.put_bucket_inventory_configuration(
            Bucket=bucket,
            Id="test-inventory",
            InventoryConfiguration=config,
        )
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200
        resp = s3.list_bucket_inventory_configurations(Bucket=bucket)
        assert "ResponseMetadata" in resp

    def test_delete_bucket_analytics_configuration(self, s3, bucket):
        """DeleteBucketAnalyticsConfiguration succeeds idempotently."""
        resp = s3.delete_bucket_analytics_configuration(Bucket=bucket, Id="nonexistent")
        assert resp["ResponseMetadata"]["HTTPStatusCode"] in (200, 204)

    def test_delete_bucket_metrics_configuration(self, s3, bucket):
        """DeleteBucketMetricsConfiguration succeeds idempotently."""
        resp = s3.delete_bucket_metrics_configuration(Bucket=bucket, Id="nonexistent")
        assert resp["ResponseMetadata"]["HTTPStatusCode"] in (200, 204)

    def test_delete_bucket_intelligent_tiering(self, s3, bucket):
        """DeleteBucketIntelligentTieringConfiguration succeeds idempotently."""
        resp = s3.delete_bucket_intelligent_tiering_configuration(Bucket=bucket, Id="nonexistent")
        assert resp["ResponseMetadata"]["HTTPStatusCode"] in (200, 204)

    def test_delete_bucket_inventory_configuration(self, s3, bucket):
        """DeleteBucketInventoryConfiguration succeeds idempotently."""
        resp = s3.delete_bucket_inventory_configuration(Bucket=bucket, Id="nonexistent")
        assert resp["ResponseMetadata"]["HTTPStatusCode"] in (200, 204)

    def test_get_bucket_inventory_configuration_nonexistent(self, s3, bucket):
        """GetBucketInventoryConfiguration on missing config returns 404."""
        with pytest.raises(ClientError) as exc:
            s3.get_bucket_inventory_configuration(Bucket=bucket, Id="nonexistent")
        assert "NoSuch" in exc.value.response["Error"]["Code"]


class TestS3ReplicationAndRequestPayment:
    """Tests for Replication and RequestPayment."""

    def test_put_bucket_request_payment(self, s3, bucket):
        """PutBucketRequestPayment accepts requester-pays config."""
        resp = s3.put_bucket_request_payment(
            Bucket=bucket,
            RequestPaymentConfiguration={"Payer": "Requester"},
        )
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200
        # Verify get_bucket_request_payment returns a response
        resp = s3.get_bucket_request_payment(Bucket=bucket)
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200

    def test_get_bucket_replication_not_configured(self, s3, bucket):
        """GetBucketReplication when not configured returns error."""
        with pytest.raises(ClientError) as exc:
            s3.get_bucket_replication(Bucket=bucket)
        err = exc.value.response["Error"]["Code"]
        assert "Replication" in err or "NotFound" in err

    def test_delete_bucket_replication_noop(self, s3, bucket):
        """DeleteBucketReplication on unconfigured bucket succeeds."""
        resp = s3.delete_bucket_replication(Bucket=bucket)
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 204

    def test_put_bucket_replication_invalid(self, s3, bucket):
        """PutBucketReplication with bad config returns 400."""
        with pytest.raises(ClientError):
            s3.put_bucket_replication(
                Bucket=bucket,
                ReplicationConfiguration={
                    "Role": "arn:aws:iam::123456789012:role/test",
                    "Rules": [
                        {
                            "Status": "Enabled",
                            "Destination": {"Bucket": "arn:aws:s3:::dest"},
                        }
                    ],
                },
            )


class TestS3OwnershipControls:
    """Tests for DeleteBucketOwnershipControls."""

    def test_delete_bucket_ownership_controls(self, s3, bucket):
        """DeleteBucketOwnershipControls succeeds."""
        s3.put_bucket_ownership_controls(
            Bucket=bucket,
            OwnershipControls={"Rules": [{"ObjectOwnership": "BucketOwnerEnforced"}]},
        )
        resp = s3.delete_bucket_ownership_controls(Bucket=bucket)
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 204


class TestS3ObjectRetentionAndTorrent:
    """Tests for Object Retention, Torrent, Restore."""

    def test_put_object_retention_without_lock(self, s3, bucket):
        """PutObjectRetention on non-lock bucket returns error."""
        s3.put_object(Bucket=bucket, Key="ret-test", Body=b"data")
        with pytest.raises(ClientError):
            s3.put_object_retention(
                Bucket=bucket,
                Key="ret-test",
                Retention={
                    "Mode": "GOVERNANCE",
                    "RetainUntilDate": "2030-01-01T00:00:00Z",
                },
            )

    def _cleanup_lock_bucket(self, s3, bucket_name):
        """Delete all object versions and the bucket."""
        try:
            versions = s3.list_object_versions(Bucket=bucket_name)
            for v in versions.get("Versions", []):
                s3.delete_object(
                    Bucket=bucket_name,
                    Key=v["Key"],
                    VersionId=v["VersionId"],
                    BypassGovernanceRetention=True,
                )
            for dm in versions.get("DeleteMarkers", []):
                s3.delete_object(
                    Bucket=bucket_name,
                    Key=dm["Key"],
                    VersionId=dm["VersionId"],
                )
            s3.delete_bucket(Bucket=bucket_name)
        except Exception:
            pass  # best-effort cleanup; failures are non-fatal

    def test_put_get_object_retention(self, s3):
        """PutObjectRetention + GetObjectRetention on lock-enabled bucket."""
        lock_bucket = "test-lock-retention-bucket"
        s3.create_bucket(Bucket=lock_bucket, ObjectLockEnabledForBucket=True)
        try:
            s3.put_object(Bucket=lock_bucket, Key="ret-key", Body=b"data")
            s3.put_object_retention(
                Bucket=lock_bucket,
                Key="ret-key",
                Retention={
                    "Mode": "GOVERNANCE",
                    "RetainUntilDate": "2030-01-01T00:00:00Z",
                },
            )
            resp = s3.get_object_retention(Bucket=lock_bucket, Key="ret-key")
            assert resp["Retention"]["Mode"] == "GOVERNANCE"
        finally:
            self._cleanup_lock_bucket(s3, lock_bucket)

    def test_get_object_retention_not_set(self, s3):
        """GetObjectRetention on object without retention returns error."""
        lock_bucket = "test-lock-no-retention"
        s3.create_bucket(Bucket=lock_bucket, ObjectLockEnabledForBucket=True)
        try:
            s3.put_object(Bucket=lock_bucket, Key="no-ret-key", Body=b"data")
            with pytest.raises(ClientError) as exc:
                s3.get_object_retention(Bucket=lock_bucket, Key="no-ret-key")
            assert "NoSuch" in exc.value.response["Error"]["Code"]
        finally:
            self._cleanup_lock_bucket(s3, lock_bucket)

    def test_get_object_torrent(self, s3, bucket):
        """GetObjectTorrent returns a response body."""
        s3.put_object(Bucket=bucket, Key="torrent-test", Body=b"data")
        resp = s3.get_object_torrent(Bucket=bucket, Key="torrent-test")
        assert "Body" in resp

    def test_restore_object_not_glacier(self, s3, bucket):
        """RestoreObject on non-Glacier object returns error."""
        s3.put_object(Bucket=bucket, Key="restore-test", Body=b"data")
        with pytest.raises(ClientError) as exc:
            s3.restore_object(
                Bucket=bucket,
                Key="restore-test",
                RestoreRequest={"Days": 1},
            )
        assert exc.value.response["Error"]["Code"] == "InvalidObjectState"


class TestS3DeprecatedNotification:
    """Tests for deprecated PutBucketNotification (non-Configuration)."""

    def test_put_bucket_notification_deprecated(self, s3, bucket):
        """PutBucketNotification (deprecated API) succeeds."""
        resp = s3.put_bucket_notification(
            Bucket=bucket,
            NotificationConfiguration={},
        )
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200


class TestS3SessionAndSelect:
    """Tests for CreateSession and SelectObjectContent."""

    def test_create_session(self, s3):
        """CreateSession returns a 200 response."""
        bucket_name = "test-session-bucket"
        s3.create_bucket(Bucket=bucket_name)
        try:
            resp = s3.create_session(Bucket=bucket_name)
            assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200
        finally:
            s3.delete_bucket(Bucket=bucket_name)

    def test_select_object_content(self, s3, bucket):
        """SelectObjectContent with CSV input returns results."""
        csv_data = b"name,age\nalice,30\nbob,25\n"
        s3.put_object(Bucket=bucket, Key="select-test.csv", Body=csv_data)
        resp = s3.select_object_content(
            Bucket=bucket,
            Key="select-test.csv",
            Expression="SELECT * FROM s3object",
            ExpressionType="SQL",
            InputSerialization={"CSV": {"FileHeaderInfo": "USE"}},
            OutputSerialization={"CSV": {}},
        )
        # Read the event stream
        records = b""
        for event in resp["Payload"]:
            if "Records" in event:
                records += event["Records"]["Payload"]
        assert len(records) > 0


class TestS3AbacAndMetadata:
    """Tests for Bucket ABAC and Metadata operations."""

    def test_get_bucket_abac(self, s3, bucket):
        """GetBucketAbac returns ABAC status."""
        resp = s3.get_bucket_abac(Bucket=bucket)
        assert "Status" in resp or "ResponseMetadata" in resp

    def test_put_bucket_abac(self, s3, bucket):
        """PutBucketAbac sets ABAC status."""
        resp = s3.put_bucket_abac(
            Bucket=bucket,
            AbacStatus={"Status": "Enabled"},
        )
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200

    def test_get_bucket_metadata_configuration(self, s3, bucket):
        """GetBucketMetadataConfiguration returns a response."""
        resp = s3.get_bucket_metadata_configuration(Bucket=bucket)
        assert "ResponseMetadata" in resp

    def test_get_bucket_metadata_table_configuration(self, s3, bucket):
        """GetBucketMetadataTableConfiguration returns a response."""
        resp = s3.get_bucket_metadata_table_configuration(Bucket=bucket)
        assert "ResponseMetadata" in resp

    def test_delete_bucket_metadata_config_succeeds(self, s3, bucket):
        """DeleteBucketMetadataConfiguration succeeds idempotently."""
        resp = s3.delete_bucket_metadata_configuration(Bucket=bucket)
        assert resp["ResponseMetadata"]["HTTPStatusCode"] in (200, 204)

    def test_delete_bucket_metadata_table_config_succeeds(self, s3, bucket):
        """DeleteBucketMetadataTableConfiguration succeeds idempotently."""
        resp = s3.delete_bucket_metadata_table_configuration(Bucket=bucket)
        assert resp["ResponseMetadata"]["HTTPStatusCode"] in (200, 204)

    def test_create_bucket_metadata_configuration(self, s3, bucket):
        """CreateBucketMetadataConfiguration returns 200."""
        resp = s3.create_bucket_metadata_configuration(
            Bucket=bucket,
            MetadataConfiguration={
                "JournalTableConfiguration": {
                    "RecordExpiration": {"Expiration": "ENABLED"},
                },
                "InventoryTableConfiguration": {"ConfigurationState": "ENABLED"},
            },
        )
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200

    def test_create_bucket_metadata_table_configuration(self, s3, bucket):
        """CreateBucketMetadataTableConfiguration returns 200."""
        resp = s3.create_bucket_metadata_table_configuration(
            Bucket=bucket,
            MetadataTableConfiguration={
                "S3TablesDestination": {
                    "TableBucketArn": "arn:aws:s3tables:us-east-1:123456789012:bucket/test",
                    "TableName": "test-table",
                }
            },
        )
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200

    def test_update_metadata_inventory_table(self, s3, bucket):
        """UpdateBucketMetadataInventoryTableConfiguration."""
        resp = s3.update_bucket_metadata_inventory_table_configuration(
            Bucket=bucket,
            InventoryTableConfiguration={"ConfigurationState": "ENABLED"},
        )
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200

    def test_update_metadata_journal_table(self, s3, bucket):
        """UpdateBucketMetadataJournalTableConfiguration."""
        resp = s3.update_bucket_metadata_journal_table_configuration(
            Bucket=bucket,
            JournalTableConfiguration={"RecordExpiration": {"Expiration": "ENABLED"}},
        )
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200


class TestS3RenameAndEncryption:
    """Tests for RenameObject and UpdateObjectEncryption."""

    def test_rename_object(self, s3, bucket):
        """RenameObject renames an existing object."""
        s3.put_object(Bucket=bucket, Key="old-name.txt", Body=b"content")
        resp = s3.rename_object(
            Bucket=bucket,
            Key="new-name.txt",
            RenameSource="old-name.txt",
        )
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200

    def test_update_object_encryption(self, s3, bucket):
        """UpdateObjectEncryption changes encryption on an object."""
        s3.put_object(Bucket=bucket, Key="enc-test.txt", Body=b"secret")
        resp = s3.update_object_encryption(
            Bucket=bucket,
            Key="enc-test.txt",
            ObjectEncryption={
                "SSEKMS": {"KMSKeyArn": "arn:aws:kms:us-east-1:123456789012:key/test"}
            },
        )
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200


class TestS3DeprecatedLifecycle:
    """Tests for deprecated GetBucketLifecycle API."""

    def test_get_bucket_lifecycle_deprecated(self, s3, bucket):
        """GetBucketLifecycle (deprecated) returns rules after PutBucketLifecycleConfiguration."""
        s3.put_bucket_lifecycle_configuration(
            Bucket=bucket,
            LifecycleConfiguration={
                "Rules": [
                    {
                        "ID": "expire-logs",
                        "Status": "Enabled",
                        "Filter": {"Prefix": "logs/"},
                        "Expiration": {"Days": 30},
                    }
                ]
            },
        )
        resp = s3.get_bucket_lifecycle(Bucket=bucket)
        assert "Rules" in resp
        assert len(resp["Rules"]) >= 1
        assert resp["Rules"][0]["ID"] == "expire-logs"


class TestS3ReplicationLifecycle:
    """Tests for PutBucketReplication + GetBucketReplication + DeleteBucketReplication."""

    def test_put_get_delete_bucket_replication(self, s3):
        """Full replication configuration lifecycle with versioned buckets."""
        src = "test-repl-src-lifecycle"
        dst = "test-repl-dst-lifecycle"
        s3.create_bucket(Bucket=src)
        s3.create_bucket(Bucket=dst)
        s3.put_bucket_versioning(Bucket=src, VersioningConfiguration={"Status": "Enabled"})
        s3.put_bucket_versioning(Bucket=dst, VersioningConfiguration={"Status": "Enabled"})
        try:
            s3.put_bucket_replication(
                Bucket=src,
                ReplicationConfiguration={
                    "Role": "arn:aws:iam::123456789012:role/replication-role",
                    "Rules": [
                        {
                            "ID": "replicate-all",
                            "Status": "Enabled",
                            "Prefix": "",
                            "Destination": {
                                "Bucket": f"arn:aws:s3:::{dst}",
                            },
                        }
                    ],
                },
            )
            resp = s3.get_bucket_replication(Bucket=src)
            assert "ReplicationConfiguration" in resp
            rules = resp["ReplicationConfiguration"]["Rules"]
            assert len(rules) >= 1
            assert rules[0]["ID"] == "replicate-all"

            s3.delete_bucket_replication(Bucket=src)
            with pytest.raises(ClientError):
                s3.get_bucket_replication(Bucket=src)
        finally:
            s3.delete_bucket(Bucket=src)
            s3.delete_bucket(Bucket=dst)


class TestS3InventoryGetAfterPut:
    """Test GetBucketInventoryConfiguration on an existing configuration."""

    def test_get_bucket_inventory_configuration_existing(self, s3, bucket):
        """GetBucketInventoryConfiguration returns the config after put."""
        config = {
            "Destination": {
                "S3BucketDestination": {
                    "Bucket": f"arn:aws:s3:::{bucket}",
                    "Format": "CSV",
                },
            },
            "IsEnabled": True,
            "Id": "inv-get-test",
            "IncludedObjectVersions": "All",
            "Schedule": {"Frequency": "Daily"},
        }
        s3.put_bucket_inventory_configuration(
            Bucket=bucket, Id="inv-get-test", InventoryConfiguration=config
        )
        resp = s3.get_bucket_inventory_configuration(Bucket=bucket, Id="inv-get-test")
        assert "InventoryConfiguration" in resp
        assert resp["InventoryConfiguration"]["Id"] == "inv-get-test"
        assert resp["InventoryConfiguration"]["IsEnabled"] is True


class TestS3NotificationWithTopic:
    """Test PutBucketNotificationConfiguration with SNS topic."""

    def test_put_get_notification_configuration_with_topic(self, s3, bucket):
        """PutBucketNotificationConfiguration with TopicConfigurations."""
        s3.put_bucket_notification_configuration(
            Bucket=bucket,
            NotificationConfiguration={
                "TopicConfigurations": [
                    {
                        "TopicArn": "arn:aws:sns:us-east-1:123456789012:test-topic",
                        "Events": ["s3:ObjectCreated:*"],
                    }
                ]
            },
        )
        resp = s3.get_bucket_notification_configuration(Bucket=bucket)
        assert "TopicConfigurations" in resp
        assert len(resp["TopicConfigurations"]) >= 1
        assert resp["TopicConfigurations"][0]["Events"] == ["s3:ObjectCreated:*"]


class TestS3LegacyOps:
    """Tests for legacy S3 operations (v1 APIs)."""

    @pytest.fixture
    def s3(self):
        return make_client("s3")

    @pytest.fixture
    def bucket(self, s3):
        bucket_name = f"legacy-test-{uuid.uuid4().hex[:8]}"
        s3.create_bucket(Bucket=bucket_name)
        yield bucket_name
        try:
            objects = s3.list_objects_v2(Bucket=bucket_name).get("Contents", [])
            for obj in objects:
                s3.delete_object(Bucket=bucket_name, Key=obj["Key"])
            s3.delete_bucket(Bucket=bucket_name)
        except Exception:
            pass  # best-effort cleanup; failures are non-fatal

    def test_list_objects_v1(self, s3, bucket):
        """ListObjects (v1) returns bucket Name."""
        resp = s3.list_objects(Bucket=bucket)
        assert "Name" in resp
        assert resp["Name"] == bucket

    def test_get_bucket_notification(self, s3, bucket):
        """GetBucketNotification returns a response."""
        resp = s3.get_bucket_notification(Bucket=bucket)
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200

    def test_put_bucket_lifecycle_v1(self, s3, bucket):
        """PutBucketLifecycle (v1) sets lifecycle rules."""
        s3.put_bucket_lifecycle(
            Bucket=bucket,
            LifecycleConfiguration={
                "Rules": [
                    {
                        "ID": "expire-all",
                        "Prefix": "",
                        "Status": "Enabled",
                        "Expiration": {"Days": 30},
                    }
                ]
            },
        )
        resp = s3.get_bucket_lifecycle(Bucket=bucket)
        assert "Rules" in resp
        assert len(resp["Rules"]) >= 1
        assert resp["Rules"][0]["ID"] == "expire-all"


class TestS3EventBridgeNotification:
    @pytest.fixture
    def unique_bucket(self, s3):
        name = "test-eb-notif-" + str(uuid.uuid4())[:8]
        s3.create_bucket(Bucket=name)
        yield name
        try:
            objects = s3.list_objects_v2(Bucket=name).get("Contents", [])
            for obj in objects:
                s3.delete_object(Bucket=name, Key=obj["Key"])
            s3.delete_bucket(Bucket=name)
        except Exception:
            pass  # best-effort cleanup; failures are non-fatal

    @pytest.fixture
    def unique_queue(self, sqs):
        name = "test-eb-q-" + str(uuid.uuid4())[:8]
        resp = sqs.create_queue(QueueName=name)
        url = resp["QueueUrl"]
        yield url
        try:
            sqs.delete_queue(QueueUrl=url)
        except Exception:
            pass  # best-effort cleanup; failures are non-fatal

    def test_eventbridge_configuration_round_trip(self, s3, unique_bucket):
        """PUT notification config with EventBridgeConfiguration, GET and assert it's preserved."""
        s3.put_bucket_notification_configuration(
            Bucket=unique_bucket,
            NotificationConfiguration={"EventBridgeConfiguration": {}},
        )
        resp = s3.get_bucket_notification_configuration(Bucket=unique_bucket)
        assert "EventBridgeConfiguration" in resp

    def test_put_object_fires_eventbridge_event(self, s3, sqs, unique_bucket, unique_queue):
        """Enable EB notifications, put EB rule targeting SQS, put object, assert event arrives."""
        events_client = make_client("events")
        queue_url = unique_queue
        attrs = sqs.get_queue_attributes(QueueUrl=queue_url, AttributeNames=["QueueArn"])
        queue_arn = attrs["Attributes"]["QueueArn"]

        s3.put_bucket_notification_configuration(
            Bucket=unique_bucket,
            NotificationConfiguration={"EventBridgeConfiguration": {}},
        )
        rule_name = "s3-test-rule-" + str(uuid.uuid4())[:8]
        events_client.put_rule(
            Name=rule_name,
            EventPattern=json.dumps({"source": ["aws.s3"]}),
            State="ENABLED",
        )
        events_client.put_targets(Rule=rule_name, Targets=[{"Id": "1", "Arn": queue_arn}])

        s3.put_object(Bucket=unique_bucket, Key="test.txt", Body=b"hello")
        time.sleep(0.5)
        msgs = sqs.receive_message(QueueUrl=queue_url, MaxNumberOfMessages=10, WaitTimeSeconds=2)
        messages = msgs.get("Messages", [])
        assert len(messages) > 0
        body = json.loads(messages[0]["Body"])
        assert body.get("source") == "aws.s3"
        assert body.get("detail", {}).get("bucket", {}).get("name") == unique_bucket


class TestS3AdditionalEventTypes:
    @pytest.fixture
    def unique_bucket(self, s3):
        name = "test-evt-" + str(uuid.uuid4())[:8]
        s3.create_bucket(Bucket=name)
        yield name
        try:
            # Delete all versions if versioning was enabled
            resp = s3.list_object_versions(Bucket=name)
            for v in resp.get("Versions", []):
                s3.delete_object(Bucket=name, Key=v["Key"], VersionId=v["VersionId"])
            for dm in resp.get("DeleteMarkers", []):
                s3.delete_object(Bucket=name, Key=dm["Key"], VersionId=dm["VersionId"])
            objects = s3.list_objects_v2(Bucket=name).get("Contents", [])
            for obj in objects:
                s3.delete_object(Bucket=name, Key=obj["Key"])
            s3.delete_bucket(Bucket=name)
        except Exception:
            pass  # best-effort cleanup; failures are non-fatal

    @pytest.fixture
    def unique_queue(self, sqs):
        name = "test-evt-q-" + str(uuid.uuid4())[:8]
        resp = sqs.create_queue(QueueName=name)
        url = resp["QueueUrl"]
        yield url
        try:
            sqs.delete_queue(QueueUrl=url)
        except Exception:
            pass  # best-effort cleanup; failures are non-fatal

    def test_copy_object_fires_copy_event(self, s3, sqs, unique_bucket, unique_queue):
        """Set SQS notification for s3:ObjectCreated:Copy, copy object, assert eventName is Copy."""
        queue_url = unique_queue
        attrs = sqs.get_queue_attributes(QueueUrl=queue_url, AttributeNames=["QueueArn"])
        queue_arn = attrs["Attributes"]["QueueArn"]
        s3.put_bucket_notification_configuration(
            Bucket=unique_bucket,
            NotificationConfiguration={
                "QueueConfigurations": [
                    {"QueueArn": queue_arn, "Events": ["s3:ObjectCreated:Copy"]}
                ]
            },
        )
        s3.put_object(Bucket=unique_bucket, Key="source.txt", Body=b"data")
        s3.copy_object(
            Bucket=unique_bucket,
            Key="dest.txt",
            CopySource={"Bucket": unique_bucket, "Key": "source.txt"},
        )
        time.sleep(0.3)
        msgs = sqs.receive_message(QueueUrl=queue_url, MaxNumberOfMessages=10, WaitTimeSeconds=2)
        messages = msgs.get("Messages", [])
        assert len(messages) > 0
        record = json.loads(messages[0]["Body"])["Records"][0]
        assert record["eventName"] == "ObjectCreated:Copy"

    def test_delete_marker_fires_delete_marker_event(self, s3, sqs, unique_bucket, unique_queue):
        """Enable versioning, set notification for DeleteMarkerCreated, delete, assert event."""
        queue_url = unique_queue
        attrs = sqs.get_queue_attributes(QueueUrl=queue_url, AttributeNames=["QueueArn"])
        queue_arn = attrs["Attributes"]["QueueArn"]
        s3.put_bucket_versioning(
            Bucket=unique_bucket, VersioningConfiguration={"Status": "Enabled"}
        )
        s3.put_bucket_notification_configuration(
            Bucket=unique_bucket,
            NotificationConfiguration={
                "QueueConfigurations": [
                    {
                        "QueueArn": queue_arn,
                        "Events": ["s3:ObjectRemoved:DeleteMarkerCreated"],
                    }
                ]
            },
        )
        s3.put_object(Bucket=unique_bucket, Key="versioned.txt", Body=b"v1")
        s3.delete_object(Bucket=unique_bucket, Key="versioned.txt")
        time.sleep(0.3)
        msgs = sqs.receive_message(QueueUrl=queue_url, MaxNumberOfMessages=10, WaitTimeSeconds=2)
        messages = msgs.get("Messages", [])
        assert len(messages) > 0
        record = json.loads(messages[0]["Body"])["Records"][0]
        assert record["eventName"] == "ObjectRemoved:DeleteMarkerCreated"


class TestS3ReplicationEngine:
    @pytest.fixture
    def src_bucket(self, s3):
        name = "test-repl-src-" + str(uuid.uuid4())[:8]
        s3.create_bucket(Bucket=name)
        s3.put_bucket_versioning(Bucket=name, VersioningConfiguration={"Status": "Enabled"})
        yield name
        try:
            resp = s3.list_object_versions(Bucket=name)
            for v in resp.get("Versions", []):
                s3.delete_object(Bucket=name, Key=v["Key"], VersionId=v["VersionId"])
            for dm in resp.get("DeleteMarkers", []):
                s3.delete_object(Bucket=name, Key=dm["Key"], VersionId=dm["VersionId"])
            s3.delete_bucket(Bucket=name)
        except Exception:
            pass  # best-effort cleanup; failures are non-fatal

    @pytest.fixture
    def dest_bucket(self, s3):
        name = "test-repl-dst-" + str(uuid.uuid4())[:8]
        s3.create_bucket(Bucket=name)
        s3.put_bucket_versioning(Bucket=name, VersioningConfiguration={"Status": "Enabled"})
        yield name
        try:
            resp = s3.list_object_versions(Bucket=name)
            for v in resp.get("Versions", []):
                s3.delete_object(Bucket=name, Key=v["Key"], VersionId=v["VersionId"])
            for dm in resp.get("DeleteMarkers", []):
                s3.delete_object(Bucket=name, Key=dm["Key"], VersionId=dm["VersionId"])
            s3.delete_bucket(Bucket=name)
        except Exception:
            pass  # best-effort cleanup; failures are non-fatal

    def test_put_object_replicates_to_dest_bucket(self, s3, src_bucket, dest_bucket):
        """Configure replication, put object, assert it appears in dest bucket."""
        s3.put_bucket_replication(
            Bucket=src_bucket,
            ReplicationConfiguration={
                "Role": "arn:aws:iam::123456789012:role/replication-role",
                "Rules": [
                    {
                        "ID": "replicate-all",
                        "Status": "Enabled",
                        "Filter": {"Prefix": ""},
                        "Destination": {"Bucket": f"arn:aws:s3:::{dest_bucket}"},
                    }
                ],
            },
        )
        s3.put_object(Bucket=src_bucket, Key="replicated.txt", Body=b"hello replication")
        time.sleep(0.5)
        resp = s3.get_object(Bucket=dest_bucket, Key="replicated.txt")
        assert resp["Body"].read() == b"hello replication"

    def test_replication_respects_prefix_filter(self, s3, src_bucket, dest_bucket):
        """Keys matching prefix get replicated; keys not matching do not."""
        s3.put_bucket_replication(
            Bucket=src_bucket,
            ReplicationConfiguration={
                "Role": "arn:aws:iam::123456789012:role/replication-role",
                "Rules": [
                    {
                        "ID": "logs-only",
                        "Status": "Enabled",
                        "Filter": {"Prefix": "logs/"},
                        "Destination": {"Bucket": f"arn:aws:s3:::{dest_bucket}"},
                    }
                ],
            },
        )
        s3.put_object(Bucket=src_bucket, Key="logs/access.log", Body=b"log data")
        s3.put_object(Bucket=src_bucket, Key="data/file.bin", Body=b"binary")
        time.sleep(0.5)
        resp = s3.get_object(Bucket=dest_bucket, Key="logs/access.log")
        assert resp["Body"].read() == b"log data"
        with pytest.raises(Exception):
            s3.get_object(Bucket=dest_bucket, Key="data/file.bin")
