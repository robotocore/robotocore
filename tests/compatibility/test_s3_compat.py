"""S3 compatibility tests — verify robotocore matches LocalStack behavior."""

import json
import os
import time
from urllib.request import Request as URLRequest
from urllib.request import urlopen

import boto3
import pytest

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
        pass


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
        assert record["eventName"] == "Put"
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
        assert body["Records"][0]["eventName"] == "Delete"

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


class TestS3ObjectTagging:
    def test_put_get_delete_object_tagging(self, s3, bucket):
        """Test full lifecycle of object tagging."""
        s3.put_object(Bucket=bucket, Key="tagged.txt", Body=b"tagged content")

        # Put tags
        s3.put_object_tagging(
            Bucket=bucket,
            Key="tagged.txt",
            Tagging={"TagSet": [{"Key": "env", "Value": "test"}, {"Key": "team", "Value": "dev"}]},
        )

        # Get tags
        response = s3.get_object_tagging(Bucket=bucket, Key="tagged.txt")
        tags = {t["Key"]: t["Value"] for t in response["TagSet"]}
        assert tags["env"] == "test"
        assert tags["team"] == "dev"

        # Delete tags
        s3.delete_object_tagging(Bucket=bucket, Key="tagged.txt")
        response = s3.get_object_tagging(Bucket=bucket, Key="tagged.txt")
        assert len(response["TagSet"]) == 0

    def test_object_tagging_overwrite(self, s3, bucket):
        """Putting new tags replaces existing tags entirely."""
        s3.put_object(Bucket=bucket, Key="retag.txt", Body=b"data")
        s3.put_object_tagging(
            Bucket=bucket,
            Key="retag.txt",
            Tagging={"TagSet": [{"Key": "old", "Value": "1"}]},
        )
        s3.put_object_tagging(
            Bucket=bucket,
            Key="retag.txt",
            Tagging={"TagSet": [{"Key": "new", "Value": "2"}]},
        )
        response = s3.get_object_tagging(Bucket=bucket, Key="retag.txt")
        tags = {t["Key"]: t["Value"] for t in response["TagSet"]}
        assert "old" not in tags
        assert tags["new"] == "2"


class TestS3BucketTagging:
    def test_put_get_delete_bucket_tagging(self, s3, bucket):
        """Test full lifecycle of bucket tagging."""
        s3.put_bucket_tagging(
            Bucket=bucket,
            Tagging={
                "TagSet": [
                    {"Key": "project", "Value": "robotocore"},
                    {"Key": "env", "Value": "ci"},
                ]
            },
        )
        response = s3.get_bucket_tagging(Bucket=bucket)
        tags = {t["Key"]: t["Value"] for t in response["TagSet"]}
        assert tags["project"] == "robotocore"
        assert tags["env"] == "ci"

        s3.delete_bucket_tagging(Bucket=bucket)
        with pytest.raises(Exception):
            s3.get_bucket_tagging(Bucket=bucket)


class TestS3ListObjectsV2Advanced:
    def test_list_objects_v2_with_delimiter(self, s3, bucket):
        """List objects with prefix and delimiter returns common prefixes."""
        s3.put_object(Bucket=bucket, Key="photos/2024/jan.jpg", Body=b"1")
        s3.put_object(Bucket=bucket, Key="photos/2024/feb.jpg", Body=b"2")
        s3.put_object(Bucket=bucket, Key="photos/2025/mar.jpg", Body=b"3")
        s3.put_object(Bucket=bucket, Key="photos/readme.txt", Body=b"4")

        response = s3.list_objects_v2(Bucket=bucket, Prefix="photos/", Delimiter="/")
        # Should have common prefixes for the year directories
        prefixes = [p["Prefix"] for p in response.get("CommonPrefixes", [])]
        assert "photos/2024/" in prefixes
        assert "photos/2025/" in prefixes
        # readme.txt is directly under photos/ so it should be in Contents
        keys = [obj["Key"] for obj in response.get("Contents", [])]
        assert "photos/readme.txt" in keys

    def test_list_objects_v2_max_keys(self, s3, bucket):
        """List objects with MaxKeys limits results and returns continuation token."""
        for i in range(5):
            s3.put_object(Bucket=bucket, Key=f"page-{i:03d}.txt", Body=b"x")

        response = s3.list_objects_v2(Bucket=bucket, MaxKeys=2)
        assert len(response["Contents"]) == 2
        assert response["IsTruncated"] is True
        assert "NextContinuationToken" in response

        # Fetch next page
        response2 = s3.list_objects_v2(
            Bucket=bucket,
            MaxKeys=2,
            ContinuationToken=response["NextContinuationToken"],
        )
        assert len(response2["Contents"]) == 2

    def test_list_objects_v2_start_after(self, s3, bucket):
        """List objects with StartAfter skips objects."""
        for c in ["a", "b", "c", "d"]:
            s3.put_object(Bucket=bucket, Key=f"sa-{c}.txt", Body=b"x")

        response = s3.list_objects_v2(Bucket=bucket, Prefix="sa-", StartAfter="sa-b.txt")
        keys = [obj["Key"] for obj in response["Contents"]]
        assert "sa-a.txt" not in keys
        assert "sa-b.txt" not in keys
        assert "sa-c.txt" in keys
        assert "sa-d.txt" in keys


class TestS3DeleteMultipleObjects:
    def test_delete_objects_returns_deleted_keys(self, s3, bucket):
        """delete_objects returns list of deleted keys."""
        for i in range(4):
            s3.put_object(Bucket=bucket, Key=f"multi-del-{i}.txt", Body=b"data")

        response = s3.delete_objects(
            Bucket=bucket,
            Delete={
                "Objects": [{"Key": f"multi-del-{i}.txt"} for i in range(4)],
                "Quiet": False,
            },
        )
        deleted_keys = sorted(d["Key"] for d in response.get("Deleted", []))
        assert deleted_keys == [f"multi-del-{i}.txt" for i in range(4)]

    def test_delete_objects_nonexistent_key(self, s3, bucket):
        """delete_objects with a nonexistent key does not error."""
        response = s3.delete_objects(
            Bucket=bucket,
            Delete={"Objects": [{"Key": "does-not-exist.txt"}], "Quiet": False},
        )
        # S3 reports nonexistent keys as deleted (not errors)
        assert len(response.get("Errors", [])) == 0


class TestS3ObjectMetadata:
    def test_head_object_content_type(self, s3, bucket):
        """head_object returns correct content type."""
        s3.put_object(
            Bucket=bucket, Key="app.json", Body=b'{"a":1}', ContentType="application/json"
        )
        response = s3.head_object(Bucket=bucket, Key="app.json")
        assert response["ContentType"] == "application/json"
        assert response["ContentLength"] == 7

    def test_put_object_with_metadata(self, s3, bucket):
        """Custom metadata is preserved on objects."""
        s3.put_object(
            Bucket=bucket,
            Key="meta.txt",
            Body=b"hello",
            Metadata={"author": "test-user", "version": "1.0"},
        )
        response = s3.head_object(Bucket=bucket, Key="meta.txt")
        assert response["Metadata"]["author"] == "test-user"
        assert response["Metadata"]["version"] == "1.0"

    def test_copy_object_preserves_content_type(self, s3, bucket):
        """Copied objects preserve content type from source."""
        s3.put_object(
            Bucket=bucket, Key="src.html", Body=b"<h1>hi</h1>", ContentType="text/html"
        )
        s3.copy_object(
            Bucket=bucket,
            Key="dst.html",
            CopySource={"Bucket": bucket, "Key": "src.html"},
        )
        response = s3.head_object(Bucket=bucket, Key="dst.html")
        assert response["ContentType"] == "text/html"


class TestS3MultipartAdvanced:
    def test_list_multipart_uploads(self, s3, bucket):
        """List in-progress multipart uploads."""
        upload1 = s3.create_multipart_upload(Bucket=bucket, Key="mp-list-1.bin")
        upload2 = s3.create_multipart_upload(Bucket=bucket, Key="mp-list-2.bin")

        response = s3.list_multipart_uploads(Bucket=bucket)
        upload_keys = [u["Key"] for u in response.get("Uploads", [])]
        assert "mp-list-1.bin" in upload_keys
        assert "mp-list-2.bin" in upload_keys

        # Cleanup
        s3.abort_multipart_upload(
            Bucket=bucket, Key="mp-list-1.bin", UploadId=upload1["UploadId"]
        )
        s3.abort_multipart_upload(
            Bucket=bucket, Key="mp-list-2.bin", UploadId=upload2["UploadId"]
        )

    def test_list_parts(self, s3, bucket):
        """List uploaded parts of a multipart upload."""
        resp = s3.create_multipart_upload(Bucket=bucket, Key="mp-parts.bin")
        upload_id = resp["UploadId"]

        s3.upload_part(
            Bucket=bucket, Key="mp-parts.bin", UploadId=upload_id,
            PartNumber=1, Body=b"a" * (5 * 1024 * 1024),
        )
        s3.upload_part(
            Bucket=bucket, Key="mp-parts.bin", UploadId=upload_id,
            PartNumber=2, Body=b"b" * 1024,
        )

        response = s3.list_parts(Bucket=bucket, Key="mp-parts.bin", UploadId=upload_id)
        parts = response["Parts"]
        assert len(parts) == 2
        assert parts[0]["PartNumber"] == 1
        assert parts[1]["PartNumber"] == 2

        s3.abort_multipart_upload(Bucket=bucket, Key="mp-parts.bin", UploadId=upload_id)


class TestS3BucketLifecycle:
    def test_put_get_delete_lifecycle(self, s3, bucket):
        """Test bucket lifecycle configuration CRUD."""
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
        response = s3.get_bucket_lifecycle_configuration(Bucket=bucket)
        rules = response["Rules"]
        assert len(rules) == 1
        assert rules[0]["ID"] == "expire-old"
        assert rules[0]["Expiration"]["Days"] == 30

        s3.delete_bucket_lifecycle(Bucket=bucket)
        with pytest.raises(Exception):
            s3.get_bucket_lifecycle_configuration(Bucket=bucket)


class TestS3BucketCORS:
    def test_put_get_delete_cors(self, s3, bucket):
        """Test bucket CORS configuration CRUD."""
        s3.put_bucket_cors(
            Bucket=bucket,
            CORSConfiguration={
                "CORSRules": [
                    {
                        "AllowedOrigins": ["https://example.com"],
                        "AllowedMethods": ["GET", "PUT"],
                        "AllowedHeaders": ["*"],
                        "MaxAgeSeconds": 3600,
                    }
                ]
            },
        )
        response = s3.get_bucket_cors(Bucket=bucket)
        rules = response["CORSRules"]
        assert len(rules) == 1
        assert "https://example.com" in rules[0]["AllowedOrigins"]
        assert "GET" in rules[0]["AllowedMethods"]

        s3.delete_bucket_cors(Bucket=bucket)
        with pytest.raises(Exception):
            s3.get_bucket_cors(Bucket=bucket)


class TestS3BucketPolicy:
    def test_put_get_delete_bucket_policy(self, s3, bucket):
        """Test bucket policy CRUD."""
        policy = json.dumps({
            "Version": "2012-10-17",
            "Statement": [
                {
                    "Sid": "PublicReadGetObject",
                    "Effect": "Allow",
                    "Principal": "*",
                    "Action": "s3:GetObject",
                    "Resource": f"arn:aws:s3:::{bucket}/*",
                }
            ],
        })
        s3.put_bucket_policy(Bucket=bucket, Policy=policy)

        response = s3.get_bucket_policy(Bucket=bucket)
        retrieved = json.loads(response["Policy"])
        assert retrieved["Statement"][0]["Sid"] == "PublicReadGetObject"

        s3.delete_bucket_policy(Bucket=bucket)
        with pytest.raises(Exception):
            s3.get_bucket_policy(Bucket=bucket)


class TestS3ObjectACL:
    def test_get_default_object_acl(self, s3, bucket):
        """Newly created objects have a default ACL."""
        s3.put_object(Bucket=bucket, Key="acl-test.txt", Body=b"acl")
        response = s3.get_object_acl(Bucket=bucket, Key="acl-test.txt")
        assert "Owner" in response
        assert "Grants" in response
        assert len(response["Grants"]) >= 1

    def test_put_object_canned_acl(self, s3, bucket):
        """Put object with a canned ACL."""
        s3.put_object(
            Bucket=bucket, Key="public-read.txt", Body=b"public", ACL="public-read"
        )
        response = s3.get_object_acl(Bucket=bucket, Key="public-read.txt")
        # Should have at least the owner grant and a public-read grant
        assert len(response["Grants"]) >= 1
