"""MediaStore Data compatibility tests."""

import pytest

from tests.compatibility.conftest import make_client


@pytest.fixture
def mediastore_data():
    return make_client("mediastore-data")


class TestMediaStoreDataOperations:
    """Tests for MediaStore Data operations (object CRUD)."""

    @pytest.fixture
    def client(self):
        return make_client("mediastore-data")

    def test_put_object(self, client):
        """PutObject stores an object and returns an ETag."""
        resp = client.put_object(
            Body=b"hello world",
            Path="put-test-obj",
            ContentType="text/plain",
        )
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200
        assert "ETag" in resp
        # cleanup
        client.delete_object(Path="put-test-obj")

    def test_list_items(self, client):
        """ListItems returns a list with correct structure after adding an object."""
        client.put_object(Body=b"list-body", Path="list-items-base-obj", ContentType="text/plain")
        try:
            resp = client.list_items()
            assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200
            assert isinstance(resp["Items"], list)
            names = [item["Name"] for item in resp["Items"]]
            assert "list-items-base-obj" in names
        finally:
            client.delete_object(Path="list-items-base-obj")

    def test_list_items_reflects_put(self, client):
        """ListItems includes an object after PutObject."""
        client.put_object(Body=b"content", Path="list-test-obj", ContentType="text/plain")
        try:
            resp = client.list_items()
            names = [item["Name"] for item in resp["Items"]]
            assert len([n for n in names if n == "list-test-obj"]) == 1
        finally:
            client.delete_object(Path="list-test-obj")

    def test_get_object(self, client):
        """GetObject returns the body that was stored via PutObject."""
        client.put_object(Body=b"get-test-body", Path="get-test-obj", ContentType="text/plain")
        try:
            resp = client.get_object(Path="get-test-obj")
            assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200
            body = resp["Body"].read()
            assert body == b"get-test-body"
        finally:
            client.delete_object(Path="get-test-obj")

    def test_delete_object(self, client):
        """DeleteObject removes an object so it no longer appears in ListItems."""
        client.put_object(Body=b"to-delete", Path="delete-test-obj", ContentType="text/plain")
        resp = client.delete_object(Path="delete-test-obj")
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200
        # Verify object is gone
        items = client.list_items()["Items"]
        names = [item["Name"] for item in items]
        assert "delete-test-obj" not in names

    def test_describe_object(self, client):
        """DescribeObject returns metadata headers for an existing object."""
        client.put_object(
            Body=b"describe-body",
            Path="describe-test-obj",
            ContentType="text/plain",
        )
        try:
            resp = client.describe_object(Path="describe-test-obj")
            assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200
            assert "ETag" in resp
            assert "ContentType" in resp
            assert "ContentLength" in resp
            assert "LastModified" in resp
            assert resp["ContentLength"] == len(b"describe-body")
        finally:
            client.delete_object(Path="describe-test-obj")

    def test_describe_object_not_found(self, client):
        """DescribeObject raises ObjectNotFoundException for a nonexistent path."""
        import pytest
        from botocore.exceptions import ClientError

        with pytest.raises(ClientError) as exc_info:
            client.describe_object(Path="nonexistent-path-xyz")
        assert exc_info.value.response["Error"]["Code"] == "ObjectNotFoundException"
