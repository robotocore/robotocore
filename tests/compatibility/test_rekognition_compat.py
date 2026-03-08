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


class TestRekognitionTextDetection:
    """Tests for StartTextDetection and GetTextDetection video analysis."""

    def test_get_text_detection_with_fake_job_id(self, rekognition):
        """GetTextDetection with a fake job ID returns a response."""
        resp = rekognition.get_text_detection(JobId="fake-text-detection-job-id")
        assert resp["JobStatus"] in ("SUCCEEDED", "IN_PROGRESS", "FAILED")
        assert "TextDetections" in resp

    def test_start_text_detection(self, rekognition):
        """StartTextDetection returns a JobId."""
        resp = rekognition.start_text_detection(
            Video={"S3Object": {"Bucket": "test-bucket", "Name": "test-video.mp4"}}
        )
        assert "JobId" in resp
        assert len(resp["JobId"]) > 0

    def test_start_and_get_text_detection(self, rekognition):
        """StartTextDetection followed by GetTextDetection returns job results."""
        start = rekognition.start_text_detection(
            Video={"S3Object": {"Bucket": "test-bucket", "Name": "vid.mp4"}}
        )
        job_id = start["JobId"]
        get_resp = rekognition.get_text_detection(JobId=job_id)
        assert get_resp["JobStatus"] in ("SUCCEEDED", "IN_PROGRESS", "FAILED")
        assert "TextDetections" in get_resp


class TestRekognitionImageAnalysis:
    """Tests for image analysis operations: CompareFaces, DetectLabels, DetectText."""

    _TINY_JPEG = (
        b"\xff\xd8\xff\xe0\x00\x10JFIF\x00\x01\x01\x00\x00\x01\x00\x01\x00\x00"
        b"\xff\xdb\x00C\x00\x08\x06\x06\x07\x06\x05\x08\x07\x07\x07\t\t"
        b"\x08\n\x0c\x14\r\x0c\x0b\x0b\x0c\x19\x12\x13\x0f\x14\x1d\x1a"
        b"\x1f\x1e\x1d\x1a\x1c\x1c $.' \",#\x1c\x1c(7),01444\x1f'9=82<.342"
        b"\xff\xc0\x00\x0b\x08\x00\x01\x00\x01\x01\x01\x11\x00"
        b"\xff\xc4\x00\x1f\x00\x00\x01\x05\x01\x01\x01\x01\x01\x01\x00\x00"
        b"\x00\x00\x00\x00\x00\x00\x01\x02\x03\x04\x05\x06\x07\x08\t\n\x0b"
        b"\xff\xc4\x00\xb5\x10\x00\x02\x01\x03\x03\x02\x04\x03\x05\x05\x04"
        b"\x04\x00\x00\x01}\x01\x02\x03\x00\x04\x11\x05\x12!1A\x06\x13Qa"
        b'\x07"q\x142\x81\x91\xa1\x08#B\xb1\xc1\x15R\xd1\xf0$3br\x82\t\n'
        b"\x16\x17\x18\x19\x1a%&'()*456789:CDEFGHIJSTUVWXYZcdefghijstuvwxyz"
        b"\x83\x84\x85\x86\x87\x88\x89\x8a\x92\x93\x94\x95\x96\x97\x98\x99"
        b"\x9a\xa2\xa3\xa4\xa5\xa6\xa7\xa8\xa9\xaa\xb2\xb3\xb4\xb5\xb6\xb7"
        b"\xb8\xb9\xba\xc2\xc3\xc4\xc5\xc6\xc7\xc8\xc9\xca\xd2\xd3\xd4\xd5"
        b"\xd6\xd7\xd8\xd9\xda\xe1\xe2\xe3\xe4\xe5\xe6\xe7\xe8\xe9\xea\xf1"
        b"\xf2\xf3\xf4\xf5\xf6\xf7\xf8\xf9\xfa"
        b"\xff\xda\x00\x08\x01\x01\x00\x00?\x00\xfb\xd2\x8a(\x03\xff\xd9"
    )

    def test_compare_faces(self, rekognition):
        """CompareFaces returns FaceMatches and UnmatchedFaces."""
        resp = rekognition.compare_faces(
            SourceImage={"Bytes": self._TINY_JPEG},
            TargetImage={"Bytes": self._TINY_JPEG},
        )
        assert "FaceMatches" in resp
        assert isinstance(resp["FaceMatches"], list)
        assert "UnmatchedFaces" in resp

    def test_detect_labels(self, rekognition):
        """DetectLabels returns a list of labels."""
        resp = rekognition.detect_labels(Image={"Bytes": self._TINY_JPEG})
        assert "Labels" in resp
        assert isinstance(resp["Labels"], list)

    def test_detect_text(self, rekognition):
        """DetectText returns text detections."""
        resp = rekognition.detect_text(Image={"Bytes": self._TINY_JPEG})
        assert "TextDetections" in resp
        assert isinstance(resp["TextDetections"], list)

    def test_detect_custom_labels(self, rekognition):
        """DetectCustomLabels returns custom labels list."""
        resp = rekognition.detect_custom_labels(
            ProjectVersionArn=(
                "arn:aws:rekognition:us-east-1:123456789012:project/test/version/1/1234567890"
            ),
            Image={"Bytes": self._TINY_JPEG},
        )
        assert "CustomLabels" in resp
        assert isinstance(resp["CustomLabels"], list)
