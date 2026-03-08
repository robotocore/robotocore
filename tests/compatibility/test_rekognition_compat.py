"""Compatibility tests for Amazon Rekognition service."""

import uuid

import pytest

from tests.compatibility.conftest import make_client


@pytest.fixture
def rekognition():
    return make_client("rekognition")


def _unique(prefix):
    return f"{prefix}-{uuid.uuid4().hex[:8]}"


class TestRekognitionCollectionOperations:
    """Tests for collection CRUD operations."""

    def test_create_collection(self, rekognition):
        col_id = _unique("col")
        resp = rekognition.create_collection(CollectionId=col_id)
        assert resp["StatusCode"] == 200
        assert "CollectionArn" in resp
        assert col_id in resp["CollectionArn"]
        assert resp["FaceModelVersion"] == "6.0"
        # cleanup
        rekognition.delete_collection(CollectionId=col_id)

    def test_describe_collection(self, rekognition):
        col_id = _unique("col")
        rekognition.create_collection(CollectionId=col_id)
        resp = rekognition.describe_collection(CollectionId=col_id)
        assert resp["FaceCount"] == 0
        assert "CollectionARN" in resp
        assert col_id in resp["CollectionARN"]
        assert resp["FaceModelVersion"] == "6.0"
        assert "CreationTimestamp" in resp
        # cleanup
        rekognition.delete_collection(CollectionId=col_id)

    def test_list_collections_contains_created(self, rekognition):
        col_id = _unique("col")
        rekognition.create_collection(CollectionId=col_id)
        resp = rekognition.list_collections()
        assert col_id in resp["CollectionIds"]
        # cleanup
        rekognition.delete_collection(CollectionId=col_id)

    def test_delete_collection(self, rekognition):
        col_id = _unique("col")
        rekognition.create_collection(CollectionId=col_id)
        resp = rekognition.delete_collection(CollectionId=col_id)
        assert resp["StatusCode"] == 200
        # Verify it's gone
        listed = rekognition.list_collections()
        assert col_id not in listed["CollectionIds"]

    def test_delete_nonexistent_collection_raises(self, rekognition):
        from botocore.exceptions import ClientError

        with pytest.raises(ClientError) as exc_info:
            rekognition.delete_collection(CollectionId=_unique("nope"))
        assert "ResourceNotFoundException" in str(exc_info.value)

    def test_describe_nonexistent_collection_raises(self, rekognition):
        from botocore.exceptions import ClientError

        with pytest.raises(ClientError) as exc_info:
            rekognition.describe_collection(CollectionId=_unique("nope"))
        assert "ResourceNotFoundException" in str(exc_info.value)

    def test_create_duplicate_collection_raises(self, rekognition):
        from botocore.exceptions import ClientError

        col_id = _unique("col")
        rekognition.create_collection(CollectionId=col_id)
        with pytest.raises(ClientError) as exc_info:
            rekognition.create_collection(CollectionId=col_id)
        assert "ResourceAlreadyExistsException" in str(exc_info.value)
        # cleanup
        rekognition.delete_collection(CollectionId=col_id)


class TestRekognitionListOperations:
    """Tests for listing collections."""

    def test_list_collections_empty(self, rekognition):
        """List collections returns an empty list when none exist (modulo other tests)."""
        resp = rekognition.list_collections()
        assert "CollectionIds" in resp
        assert isinstance(resp["CollectionIds"], list)

    def test_list_collections_multiple(self, rekognition):
        ids = [_unique("col") for _ in range(3)]
        for cid in ids:
            rekognition.create_collection(CollectionId=cid)
        resp = rekognition.list_collections()
        for cid in ids:
            assert cid in resp["CollectionIds"]
        # cleanup
        for cid in ids:
            rekognition.delete_collection(CollectionId=cid)


class TestRekognitionTags:
    """Tests for tag_resource, list_tags_for_resource, untag_resource."""

    def _create_and_get_arn(self, rekognition):
        col_id = _unique("tag")
        rekognition.create_collection(CollectionId=col_id)
        desc = rekognition.describe_collection(CollectionId=col_id)
        return col_id, desc["CollectionARN"]

    def test_tag_resource(self, rekognition):
        col_id, arn = self._create_and_get_arn(rekognition)
        rekognition.tag_resource(ResourceArn=arn, Tags={"env": "test", "team": "backend"})
        resp = rekognition.list_tags_for_resource(ResourceArn=arn)
        assert resp["Tags"]["env"] == "test"
        assert resp["Tags"]["team"] == "backend"
        # cleanup
        rekognition.delete_collection(CollectionId=col_id)

    def test_list_tags_for_resource_empty(self, rekognition):
        col_id, arn = self._create_and_get_arn(rekognition)
        resp = rekognition.list_tags_for_resource(ResourceArn=arn)
        assert resp["Tags"] == {}
        # cleanup
        rekognition.delete_collection(CollectionId=col_id)

    def test_untag_resource(self, rekognition):
        col_id, arn = self._create_and_get_arn(rekognition)
        rekognition.tag_resource(ResourceArn=arn, Tags={"a": "1", "b": "2", "c": "3"})
        rekognition.untag_resource(ResourceArn=arn, TagKeys=["a", "c"])
        resp = rekognition.list_tags_for_resource(ResourceArn=arn)
        assert resp["Tags"] == {"b": "2"}
        # cleanup
        rekognition.delete_collection(CollectionId=col_id)

    def test_tag_resource_overwrites(self, rekognition):
        col_id, arn = self._create_and_get_arn(rekognition)
        rekognition.tag_resource(ResourceArn=arn, Tags={"key": "old"})
        rekognition.tag_resource(ResourceArn=arn, Tags={"key": "new"})
        resp = rekognition.list_tags_for_resource(ResourceArn=arn)
        assert resp["Tags"]["key"] == "new"
        # cleanup
        rekognition.delete_collection(CollectionId=col_id)


class TestRekognitionFaceSearchOperations:
    """Tests for StartFaceSearch and GetFaceSearch video analysis operations."""

    def test_start_face_search(self, rekognition):
        col_id = _unique("col")
        rekognition.create_collection(CollectionId=col_id)
        try:
            resp = rekognition.start_face_search(
                Video={"S3Object": {"Bucket": "test-bucket", "Name": "test-video.mp4"}},
                CollectionId=col_id,
            )
            assert "JobId" in resp
            assert len(resp["JobId"]) > 0
        finally:
            rekognition.delete_collection(CollectionId=col_id)

    def test_get_face_search_with_valid_job(self, rekognition):
        col_id = _unique("col")
        rekognition.create_collection(CollectionId=col_id)
        try:
            start_resp = rekognition.start_face_search(
                Video={"S3Object": {"Bucket": "test-bucket", "Name": "test.mp4"}},
                CollectionId=col_id,
            )
            job_id = start_resp["JobId"]
            resp = rekognition.get_face_search(JobId=job_id)
            assert resp["JobStatus"] in ("SUCCEEDED", "IN_PROGRESS", "FAILED")
            assert "VideoMetadata" in resp
            assert "Persons" in resp
        finally:
            rekognition.delete_collection(CollectionId=col_id)

    def test_get_face_search_with_fake_job_id(self, rekognition):
        resp = rekognition.get_face_search(JobId="fake-nonexistent-job-id")
        assert resp["JobStatus"] in ("SUCCEEDED", "IN_PROGRESS", "FAILED")
        assert "Persons" in resp
