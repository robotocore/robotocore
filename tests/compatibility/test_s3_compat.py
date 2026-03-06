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
            Bucket=bucket, Key="page.html",
            Body=b"<h1>hi</h1>", ContentType="text/html",
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
            Bucket=bucket, Key="large.bin", UploadId=upload_id,
            PartNumber=1, Body=b"a" * (5 * 1024 * 1024),
        )
        part2 = s3.upload_part(
            Bucket=bucket, Key="large.bin", UploadId=upload_id,
            PartNumber=2, Body=b"b" * 1024,
        )

        # Complete
        s3.complete_multipart_upload(
            Bucket=bucket, Key="large.bin", UploadId=upload_id,
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
            Bucket=bucket, Key="abort.bin", UploadId=upload_id,
        )


class TestS3EventNotifications:
    def test_put_object_triggers_sqs_notification(self, s3, sqs):
        bucket_name = "notif-test-s3"
        s3.create_bucket(Bucket=bucket_name)
        q_url = sqs.create_queue(QueueName="s3-event-test")["QueueUrl"]
        q_arn = sqs.get_queue_attributes(
            QueueUrl=q_url, AttributeNames=["QueueArn"]
        )["Attributes"]["QueueArn"]

        s3.put_bucket_notification_configuration(
            Bucket=bucket_name,
            NotificationConfiguration={
                "QueueConfigurations": [{
                    "QueueArn": q_arn,
                    "Events": ["s3:ObjectCreated:*"],
                }],
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
        q_arn = sqs.get_queue_attributes(
            QueueUrl=q_url, AttributeNames=["QueueArn"]
        )["Attributes"]["QueueArn"]

        s3.put_bucket_notification_configuration(
            Bucket=bucket_name,
            NotificationConfiguration={
                "QueueConfigurations": [{
                    "QueueArn": q_arn,
                    "Events": ["s3:ObjectRemoved:*"],
                }],
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
        q_arn = sqs.get_queue_attributes(
            QueueUrl=q_url, AttributeNames=["QueueArn"]
        )["Attributes"]["QueueArn"]

        s3.put_bucket_notification_configuration(
            Bucket=bucket_name,
            NotificationConfiguration={
                "QueueConfigurations": [{
                    "QueueArn": q_arn,
                    "Events": ["s3:ObjectCreated:*"],
                    "Filter": {
                        "Key": {
                            "FilterRules": [{"Name": "prefix", "Value": "images/"}]
                        }
                    },
                }],
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
