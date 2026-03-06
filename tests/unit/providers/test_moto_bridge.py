"""Tests for the Moto bridge dispatch layer."""

import pytest

from robotocore.providers.moto_bridge import (
    _extract_region,
    _get_dispatcher,
    _get_moto_routing_table,
)


class TestMotoRoutingTable:
    def test_builds_routing_table_for_sts(self):
        url_map = _get_moto_routing_table("sts")
        rules = list(url_map.iter_rules())
        assert len(rules) >= 1

    def test_builds_routing_table_for_sqs(self):
        url_map = _get_moto_routing_table("sqs")
        rules = list(url_map.iter_rules())
        assert len(rules) >= 2

    def test_builds_routing_table_for_s3(self):
        url_map = _get_moto_routing_table("s3")
        rules = list(url_map.iter_rules())
        assert len(rules) >= 4

    def test_caches_routing_table(self):
        table1 = _get_moto_routing_table("sts")
        table2 = _get_moto_routing_table("sts")
        assert table1 is table2


class TestGetDispatcher:
    def test_sts_root_path(self):
        dispatch = _get_dispatcher("sts", "/")
        assert callable(dispatch)

    def test_sqs_root_path(self):
        dispatch = _get_dispatcher("sqs", "/")
        assert callable(dispatch)

    def test_sqs_queue_path(self):
        dispatch = _get_dispatcher("sqs", "/123456789012/my-queue")
        assert callable(dispatch)

    def test_s3_bucket_path(self):
        dispatch = _get_dispatcher("s3", "/my-bucket")
        assert callable(dispatch)

    def test_s3_key_path(self):
        dispatch = _get_dispatcher("s3", "/my-bucket/my-key.txt")
        assert callable(dispatch)


class TestExtractRegion:
    def test_from_auth_header(self):
        headers = {
            "authorization": (
                "AWS4-HMAC-SHA256 "
                "Credential=AKID/20260305/eu-west-1/s3/aws4_request, "
                "SignedHeaders=host, Signature=abc"
            )
        }
        assert _extract_region(headers) == "eu-west-1"

    def test_defaults_to_us_east_1(self):
        assert _extract_region({}) == "us-east-1"

    def test_no_auth_header(self):
        assert _extract_region({"authorization": "Basic foo"}) == "us-east-1"


class TestForwardToMoto:
    """Integration tests using the Starlette test client."""

    @pytest.fixture
    def client(self):
        from starlette.testclient import TestClient

        from robotocore.gateway.app import app

        return TestClient(app)

    def _auth_header(self, service: str, region: str = "us-east-1") -> dict:
        return {
            "Authorization": (
                f"AWS4-HMAC-SHA256 "
                f"Credential=testing/20260305/{region}/{service}/aws4_request, "
                f"SignedHeaders=host, Signature=abc"
            ),
        }

    def test_sts_get_caller_identity(self, client):
        response = client.post(
            "/",
            data="Action=GetCallerIdentity&Version=2011-06-15",
            headers={
                "Content-Type": "application/x-www-form-urlencoded",
                **self._auth_header("sts"),
            },
        )
        assert response.status_code == 200
        assert "GetCallerIdentityResult" in response.text
        assert "123456789012" in response.text

    def test_s3_create_bucket(self, client):
        response = client.put(
            "/test-bridge-bucket",
            headers=self._auth_header("s3"),
        )
        assert response.status_code == 200

    def test_s3_put_and_get_object(self, client):
        # Create bucket
        client.put("/test-obj-bucket", headers=self._auth_header("s3"))

        # Put object
        response = client.put(
            "/test-obj-bucket/hello.txt",
            content=b"hello world",
            headers=self._auth_header("s3"),
        )
        assert response.status_code == 200

        # Get object
        response = client.get(
            "/test-obj-bucket/hello.txt",
            headers=self._auth_header("s3"),
        )
        assert response.status_code == 200
        assert response.content == b"hello world"

    def test_sqs_create_queue(self, client):
        response = client.post(
            "/",
            data="Action=CreateQueue&QueueName=test-bridge-queue",
            headers={
                "Content-Type": "application/x-www-form-urlencoded",
                **self._auth_header("sqs"),
            },
        )
        assert response.status_code == 200
        assert "CreateQueueResponse" in response.text
        assert "test-bridge-queue" in response.text

    def test_sqs_send_and_receive(self, client):
        # Create queue
        create_resp = client.post(
            "/",
            data="Action=CreateQueue&QueueName=test-msg-queue",
            headers={
                "Content-Type": "application/x-www-form-urlencoded",
                **self._auth_header("sqs"),
            },
        )
        assert create_resp.status_code == 200

        # Send message - SQS uses queue URL path
        response = client.post(
            "/123456789012/test-msg-queue",
            data="Action=SendMessage&MessageBody=hello+from+bridge",
            headers={
                "Content-Type": "application/x-www-form-urlencoded",
                **self._auth_header("sqs"),
            },
        )
        assert response.status_code == 200
        assert "SendMessageResponse" in response.text

        # Receive message
        response = client.post(
            "/123456789012/test-msg-queue",
            data="Action=ReceiveMessage&MaxNumberOfMessages=1",
            headers={
                "Content-Type": "application/x-www-form-urlencoded",
                **self._auth_header("sqs"),
            },
        )
        assert response.status_code == 200
        assert "hello from bridge" in response.text

    def test_unknown_service_returns_501(self, client):
        response = client.get(
            "/",
            headers={
                "Authorization": (
                    "AWS4-HMAC-SHA256 "
                    "Credential=testing/20260305/us-east-1/nonexistent/aws4_request, "
                    "SignedHeaders=host, Signature=abc"
                )
            },
        )
        assert response.status_code == 501
