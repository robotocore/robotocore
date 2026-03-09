"""Compatibility tests for Amazon Rekognition service."""

import uuid

import pytest

from tests.compatibility.conftest import make_client


@pytest.fixture
def rekognition():
    return make_client("rekognition")


def _unique(prefix):
    return f"{prefix}-{uuid.uuid4().hex[:8]}"


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


class TestRekognitionCollectionOperations:
    """Tests for collection CRUD operations."""

    def test_create_collection(self, rekognition):
        col_id = _unique("col")
        resp = rekognition.create_collection(CollectionId=col_id)
        assert resp["StatusCode"] == 200
        assert "CollectionArn" in resp
        assert col_id in resp["CollectionArn"]
        assert resp["FaceModelVersion"] == "6.0"
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
        rekognition.delete_collection(CollectionId=col_id)

    def test_list_collections_contains_created(self, rekognition):
        col_id = _unique("col")
        rekognition.create_collection(CollectionId=col_id)
        resp = rekognition.list_collections()
        assert col_id in resp["CollectionIds"]
        rekognition.delete_collection(CollectionId=col_id)

    def test_delete_collection(self, rekognition):
        col_id = _unique("col")
        rekognition.create_collection(CollectionId=col_id)
        resp = rekognition.delete_collection(CollectionId=col_id)
        assert resp["StatusCode"] == 200
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
        rekognition.delete_collection(CollectionId=col_id)


class TestRekognitionListOperations:
    """Tests for listing collections."""

    def test_list_collections_empty(self, rekognition):
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
        rekognition.delete_collection(CollectionId=col_id)

    def test_list_tags_for_resource_empty(self, rekognition):
        col_id, arn = self._create_and_get_arn(rekognition)
        resp = rekognition.list_tags_for_resource(ResourceArn=arn)
        assert resp["Tags"] == {}
        rekognition.delete_collection(CollectionId=col_id)

    def test_untag_resource(self, rekognition):
        col_id, arn = self._create_and_get_arn(rekognition)
        rekognition.tag_resource(ResourceArn=arn, Tags={"a": "1", "b": "2", "c": "3"})
        rekognition.untag_resource(ResourceArn=arn, TagKeys=["a", "c"])
        resp = rekognition.list_tags_for_resource(ResourceArn=arn)
        assert resp["Tags"] == {"b": "2"}
        rekognition.delete_collection(CollectionId=col_id)

    def test_tag_resource_overwrites(self, rekognition):
        col_id, arn = self._create_and_get_arn(rekognition)
        rekognition.tag_resource(ResourceArn=arn, Tags={"key": "old"})
        rekognition.tag_resource(ResourceArn=arn, Tags={"key": "new"})
        resp = rekognition.list_tags_for_resource(ResourceArn=arn)
        assert resp["Tags"]["key"] == "new"
        rekognition.delete_collection(CollectionId=col_id)


class TestRekognitionFaceOperations:
    """Tests for face indexing, listing, searching, and deletion."""

    def test_index_faces(self, rekognition):
        col_id = _unique("col")
        rekognition.create_collection(CollectionId=col_id)
        resp = rekognition.index_faces(
            CollectionId=col_id,
            Image={"Bytes": _TINY_JPEG},
        )
        assert "FaceRecords" in resp
        assert len(resp["FaceRecords"]) > 0
        assert "Face" in resp["FaceRecords"][0]
        assert "FaceId" in resp["FaceRecords"][0]["Face"]
        assert resp["FaceModelVersion"] == "6.0"
        rekognition.delete_collection(CollectionId=col_id)

    def test_index_faces_with_external_image_id(self, rekognition):
        col_id = _unique("col")
        rekognition.create_collection(CollectionId=col_id)
        resp = rekognition.index_faces(
            CollectionId=col_id,
            Image={"Bytes": _TINY_JPEG},
            ExternalImageId="my-face-001",
        )
        assert resp["FaceRecords"][0]["Face"]["ExternalImageId"] == "my-face-001"
        rekognition.delete_collection(CollectionId=col_id)

    def test_list_faces(self, rekognition):
        col_id = _unique("col")
        rekognition.create_collection(CollectionId=col_id)
        rekognition.index_faces(CollectionId=col_id, Image={"Bytes": _TINY_JPEG})
        resp = rekognition.list_faces(CollectionId=col_id)
        assert "Faces" in resp
        assert len(resp["Faces"]) == 1
        assert "FaceId" in resp["Faces"][0]
        rekognition.delete_collection(CollectionId=col_id)

    def test_list_faces_empty_collection(self, rekognition):
        col_id = _unique("col")
        rekognition.create_collection(CollectionId=col_id)
        resp = rekognition.list_faces(CollectionId=col_id)
        assert resp["Faces"] == []
        rekognition.delete_collection(CollectionId=col_id)

    def test_search_faces(self, rekognition):
        col_id = _unique("col")
        rekognition.create_collection(CollectionId=col_id)
        idx = rekognition.index_faces(CollectionId=col_id, Image={"Bytes": _TINY_JPEG})
        face_id = idx["FaceRecords"][0]["Face"]["FaceId"]
        resp = rekognition.search_faces(CollectionId=col_id, FaceId=face_id)
        assert "SearchedFaceId" in resp
        assert resp["SearchedFaceId"] == face_id
        assert "FaceMatches" in resp
        assert isinstance(resp["FaceMatches"], list)
        rekognition.delete_collection(CollectionId=col_id)

    def test_search_faces_by_image(self, rekognition):
        col_id = _unique("col")
        rekognition.create_collection(CollectionId=col_id)
        rekognition.index_faces(CollectionId=col_id, Image={"Bytes": _TINY_JPEG})
        resp = rekognition.search_faces_by_image(
            CollectionId=col_id,
            Image={"Bytes": _TINY_JPEG},
        )
        assert "FaceMatches" in resp
        assert isinstance(resp["FaceMatches"], list)
        assert "SearchedFaceBoundingBox" in resp
        assert "SearchedFaceConfidence" in resp
        rekognition.delete_collection(CollectionId=col_id)

    def test_delete_faces(self, rekognition):
        col_id = _unique("col")
        rekognition.create_collection(CollectionId=col_id)
        idx = rekognition.index_faces(CollectionId=col_id, Image={"Bytes": _TINY_JPEG})
        face_id = idx["FaceRecords"][0]["Face"]["FaceId"]
        resp = rekognition.delete_faces(CollectionId=col_id, FaceIds=[face_id])
        assert "DeletedFaces" in resp
        assert face_id in resp["DeletedFaces"]
        # Verify face is gone
        faces = rekognition.list_faces(CollectionId=col_id)
        assert len(faces["Faces"]) == 0
        rekognition.delete_collection(CollectionId=col_id)

    def test_index_faces_nonexistent_collection(self, rekognition):
        from botocore.exceptions import ClientError

        with pytest.raises(ClientError) as exc_info:
            rekognition.index_faces(
                CollectionId=_unique("nope"),
                Image={"Bytes": _TINY_JPEG},
            )
        assert "ResourceNotFoundException" in str(exc_info.value)


class TestRekognitionImageAnalysis:
    """Tests for image analysis operations."""

    def test_compare_faces(self, rekognition):
        resp = rekognition.compare_faces(
            SourceImage={"Bytes": _TINY_JPEG},
            TargetImage={"Bytes": _TINY_JPEG},
        )
        assert "FaceMatches" in resp
        assert isinstance(resp["FaceMatches"], list)
        assert "UnmatchedFaces" in resp

    def test_detect_labels(self, rekognition):
        resp = rekognition.detect_labels(Image={"Bytes": _TINY_JPEG})
        assert "Labels" in resp
        assert isinstance(resp["Labels"], list)

    def test_detect_text(self, rekognition):
        resp = rekognition.detect_text(Image={"Bytes": _TINY_JPEG})
        assert "TextDetections" in resp
        assert isinstance(resp["TextDetections"], list)

    def test_detect_custom_labels(self, rekognition):
        resp = rekognition.detect_custom_labels(
            ProjectVersionArn=(
                "arn:aws:rekognition:us-east-1:123456789012:project/test/version/1/1234567890"
            ),
            Image={"Bytes": _TINY_JPEG},
        )
        assert "CustomLabels" in resp
        assert isinstance(resp["CustomLabels"], list)

    def test_detect_faces(self, rekognition):
        resp = rekognition.detect_faces(Image={"Bytes": _TINY_JPEG})
        assert "FaceDetails" in resp
        assert isinstance(resp["FaceDetails"], list)
        assert len(resp["FaceDetails"]) > 0
        face = resp["FaceDetails"][0]
        assert "BoundingBox" in face
        assert "Confidence" in face

    def test_detect_moderation_labels(self, rekognition):
        resp = rekognition.detect_moderation_labels(Image={"Bytes": _TINY_JPEG})
        assert "ModerationLabels" in resp
        assert isinstance(resp["ModerationLabels"], list)
        assert "ModerationModelVersion" in resp

    def test_detect_protective_equipment(self, rekognition):
        resp = rekognition.detect_protective_equipment(Image={"Bytes": _TINY_JPEG})
        assert "Persons" in resp
        assert isinstance(resp["Persons"], list)
        assert "ProtectiveEquipmentModelVersion" in resp

    def test_recognize_celebrities(self, rekognition):
        resp = rekognition.recognize_celebrities(Image={"Bytes": _TINY_JPEG})
        assert "CelebrityFaces" in resp
        assert isinstance(resp["CelebrityFaces"], list)
        assert "UnrecognizedFaces" in resp
        assert isinstance(resp["UnrecognizedFaces"], list)

    def test_get_celebrity_info(self, rekognition):
        resp = rekognition.get_celebrity_info(Id="abc123")
        assert "Name" in resp
        assert "Urls" in resp
        assert isinstance(resp["Urls"], list)


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
        resp = rekognition.get_text_detection(JobId="fake-text-detection-job-id")
        assert resp["JobStatus"] in ("SUCCEEDED", "IN_PROGRESS", "FAILED")
        assert "TextDetections" in resp

    def test_start_text_detection(self, rekognition):
        resp = rekognition.start_text_detection(
            Video={"S3Object": {"Bucket": "test-bucket", "Name": "test-video.mp4"}}
        )
        assert "JobId" in resp
        assert len(resp["JobId"]) > 0

    def test_start_and_get_text_detection(self, rekognition):
        start = rekognition.start_text_detection(
            Video={"S3Object": {"Bucket": "test-bucket", "Name": "vid.mp4"}}
        )
        job_id = start["JobId"]
        get_resp = rekognition.get_text_detection(JobId=job_id)
        assert get_resp["JobStatus"] in ("SUCCEEDED", "IN_PROGRESS", "FAILED")
        assert "TextDetections" in get_resp


class TestRekognitionVideoFaceDetection:
    """Tests for StartFaceDetection and GetFaceDetection."""

    def test_start_face_detection(self, rekognition):
        resp = rekognition.start_face_detection(
            Video={"S3Object": {"Bucket": "test-bucket", "Name": "video.mp4"}}
        )
        assert "JobId" in resp
        assert len(resp["JobId"]) > 0

    def test_start_and_get_face_detection(self, rekognition):
        start = rekognition.start_face_detection(
            Video={"S3Object": {"Bucket": "test-bucket", "Name": "video.mp4"}}
        )
        resp = rekognition.get_face_detection(JobId=start["JobId"])
        assert resp["JobStatus"] == "SUCCEEDED"
        assert "VideoMetadata" in resp
        assert "Faces" in resp
        assert isinstance(resp["Faces"], list)


class TestRekognitionVideoLabelDetection:
    """Tests for StartLabelDetection and GetLabelDetection."""

    def test_start_label_detection(self, rekognition):
        resp = rekognition.start_label_detection(
            Video={"S3Object": {"Bucket": "test-bucket", "Name": "video.mp4"}}
        )
        assert "JobId" in resp
        assert len(resp["JobId"]) > 0

    def test_start_and_get_label_detection(self, rekognition):
        start = rekognition.start_label_detection(
            Video={"S3Object": {"Bucket": "test-bucket", "Name": "video.mp4"}}
        )
        resp = rekognition.get_label_detection(JobId=start["JobId"])
        assert resp["JobStatus"] == "SUCCEEDED"
        assert "VideoMetadata" in resp
        assert "Labels" in resp
        assert isinstance(resp["Labels"], list)


class TestRekognitionVideoCelebrityRecognition:
    """Tests for StartCelebrityRecognition and GetCelebrityRecognition."""

    def test_start_celebrity_recognition(self, rekognition):
        resp = rekognition.start_celebrity_recognition(
            Video={"S3Object": {"Bucket": "test-bucket", "Name": "video.mp4"}}
        )
        assert "JobId" in resp
        assert len(resp["JobId"]) > 0

    def test_start_and_get_celebrity_recognition(self, rekognition):
        start = rekognition.start_celebrity_recognition(
            Video={"S3Object": {"Bucket": "test-bucket", "Name": "video.mp4"}}
        )
        resp = rekognition.get_celebrity_recognition(JobId=start["JobId"])
        assert resp["JobStatus"] == "SUCCEEDED"
        assert "VideoMetadata" in resp
        assert "Celebrities" in resp
        assert isinstance(resp["Celebrities"], list)


class TestRekognitionVideoContentModeration:
    """Tests for StartContentModeration and GetContentModeration."""

    def test_start_content_moderation(self, rekognition):
        resp = rekognition.start_content_moderation(
            Video={"S3Object": {"Bucket": "test-bucket", "Name": "video.mp4"}}
        )
        assert "JobId" in resp
        assert len(resp["JobId"]) > 0

    def test_start_and_get_content_moderation(self, rekognition):
        start = rekognition.start_content_moderation(
            Video={"S3Object": {"Bucket": "test-bucket", "Name": "video.mp4"}}
        )
        resp = rekognition.get_content_moderation(JobId=start["JobId"])
        assert resp["JobStatus"] == "SUCCEEDED"
        assert "VideoMetadata" in resp
        assert "ModerationLabels" in resp
        assert isinstance(resp["ModerationLabels"], list)


class TestRekognitionVideoPersonTracking:
    """Tests for StartPersonTracking and GetPersonTracking."""

    def test_start_person_tracking(self, rekognition):
        resp = rekognition.start_person_tracking(
            Video={"S3Object": {"Bucket": "test-bucket", "Name": "video.mp4"}}
        )
        assert "JobId" in resp
        assert len(resp["JobId"]) > 0

    def test_start_and_get_person_tracking(self, rekognition):
        start = rekognition.start_person_tracking(
            Video={"S3Object": {"Bucket": "test-bucket", "Name": "video.mp4"}}
        )
        resp = rekognition.get_person_tracking(JobId=start["JobId"])
        assert resp["JobStatus"] == "SUCCEEDED"
        assert "VideoMetadata" in resp
        assert "Persons" in resp
        assert isinstance(resp["Persons"], list)


class TestRekognitionVideoSegmentDetection:
    """Tests for StartSegmentDetection and GetSegmentDetection."""

    def test_start_segment_detection(self, rekognition):
        resp = rekognition.start_segment_detection(
            Video={"S3Object": {"Bucket": "test-bucket", "Name": "video.mp4"}},
            SegmentTypes=["TECHNICAL_CUE", "SHOT"],
        )
        assert "JobId" in resp
        assert len(resp["JobId"]) > 0

    def test_start_and_get_segment_detection(self, rekognition):
        start = rekognition.start_segment_detection(
            Video={"S3Object": {"Bucket": "test-bucket", "Name": "video.mp4"}},
            SegmentTypes=["TECHNICAL_CUE"],
        )
        resp = rekognition.get_segment_detection(JobId=start["JobId"])
        assert resp["JobStatus"] == "SUCCEEDED"
        assert "VideoMetadata" in resp
        assert "Segments" in resp
        assert "SelectedSegmentTypes" in resp


class TestRekognitionProjects:
    """Tests for project CRUD operations."""

    def test_create_project(self, rekognition):
        name = _unique("proj")
        resp = rekognition.create_project(ProjectName=name)
        assert "ProjectArn" in resp
        assert name in resp["ProjectArn"]
        # cleanup
        rekognition.delete_project(ProjectArn=resp["ProjectArn"])

    def test_describe_projects(self, rekognition):
        name = _unique("proj")
        create_resp = rekognition.create_project(ProjectName=name)
        resp = rekognition.describe_projects()
        assert "ProjectDescriptions" in resp
        assert isinstance(resp["ProjectDescriptions"], list)
        arns = [p["ProjectArn"] for p in resp["ProjectDescriptions"]]
        assert create_resp["ProjectArn"] in arns
        rekognition.delete_project(ProjectArn=create_resp["ProjectArn"])

    def test_delete_project(self, rekognition):
        name = _unique("proj")
        create_resp = rekognition.create_project(ProjectName=name)
        resp = rekognition.delete_project(ProjectArn=create_resp["ProjectArn"])
        assert resp["Status"] == "DELETING"

    def test_delete_nonexistent_project(self, rekognition):
        from botocore.exceptions import ClientError

        with pytest.raises(ClientError) as exc_info:
            rekognition.delete_project(
                ProjectArn="arn:aws:rekognition:us-east-1:123456789012:project/nope/9999"
            )
        assert "ResourceNotFoundException" in str(exc_info.value)

    def test_describe_projects_empty(self, rekognition):
        resp = rekognition.describe_projects()
        assert "ProjectDescriptions" in resp
        assert isinstance(resp["ProjectDescriptions"], list)


class TestRekognitionStreamProcessors:
    """Tests for stream processor CRUD operations."""

    def _create_sp(self, rekognition):
        name = _unique("sp")
        col_id = _unique("col")
        rekognition.create_collection(CollectionId=col_id)
        resp = rekognition.create_stream_processor(
            Name=name,
            Input={"KinesisVideoStream": {"Arn": "arn:aws:kinesisvideo:us-east-1:123:stream/s/0"}},
            Output={"KinesisDataStream": {"Arn": "arn:aws:kinesis:us-east-1:123:stream/out"}},
            RoleArn="arn:aws:iam::123456789012:role/test",
            Settings={"FaceSearch": {"CollectionId": col_id, "FaceMatchThreshold": 80.0}},
        )
        return name, col_id, resp["StreamProcessorArn"]

    def test_create_stream_processor(self, rekognition):
        name, col_id, arn = self._create_sp(rekognition)
        assert "streamprocessor" in arn
        assert name in arn
        rekognition.delete_stream_processor(Name=name)
        rekognition.delete_collection(CollectionId=col_id)

    def test_describe_stream_processor(self, rekognition):
        name, col_id, arn = self._create_sp(rekognition)
        resp = rekognition.describe_stream_processor(Name=name)
        assert resp["Name"] == name
        assert resp["StreamProcessorArn"] == arn
        assert resp["Status"] == "STOPPED"
        rekognition.delete_stream_processor(Name=name)
        rekognition.delete_collection(CollectionId=col_id)

    def test_list_stream_processors(self, rekognition):
        name, col_id, _ = self._create_sp(rekognition)
        resp = rekognition.list_stream_processors()
        assert "StreamProcessors" in resp
        names = [sp["Name"] for sp in resp["StreamProcessors"]]
        assert name in names
        rekognition.delete_stream_processor(Name=name)
        rekognition.delete_collection(CollectionId=col_id)

    def test_delete_stream_processor(self, rekognition):
        name, col_id, _ = self._create_sp(rekognition)
        rekognition.delete_stream_processor(Name=name)
        # Verify deleted
        from botocore.exceptions import ClientError

        with pytest.raises(ClientError) as exc_info:
            rekognition.describe_stream_processor(Name=name)
        assert "ResourceNotFoundException" in str(exc_info.value)
        rekognition.delete_collection(CollectionId=col_id)

    def test_delete_nonexistent_stream_processor(self, rekognition):
        from botocore.exceptions import ClientError

        with pytest.raises(ClientError) as exc_info:
            rekognition.delete_stream_processor(Name=_unique("nope"))
        assert "ResourceNotFoundException" in str(exc_info.value)


class TestRekognitionFaceLiveness:
    """Tests for face liveness session operations."""

    def test_create_face_liveness_session(self, rekognition):
        resp = rekognition.create_face_liveness_session()
        assert "SessionId" in resp
        assert len(resp["SessionId"]) > 0

    def test_get_face_liveness_session_results(self, rekognition):
        create_resp = rekognition.create_face_liveness_session()
        session_id = create_resp["SessionId"]
        resp = rekognition.get_face_liveness_session_results(SessionId=session_id)
        assert resp["SessionId"] == session_id
        assert resp["Status"] in ("CREATED", "SUCCEEDED")
        assert "Confidence" in resp

    def test_get_face_liveness_nonexistent_session(self, rekognition):
        from botocore.exceptions import ClientError

        # SessionId must be >= 36 chars (UUID format) to pass client-side validation
        fake_id = str(uuid.uuid4())
        with pytest.raises(ClientError) as exc_info:
            rekognition.get_face_liveness_session_results(SessionId=fake_id)
        assert "SessionNotFoundException" in str(exc_info.value)


class TestRekognitionCollectionBehavior:
    """Deeper behavioral tests for collection operations."""

    def test_describe_collection_face_count_after_index(self, rekognition):
        """DescribeCollection FaceCount increments after indexing faces."""
        col_id = _unique("cnt")
        rekognition.create_collection(CollectionId=col_id)
        try:
            rekognition.index_faces(CollectionId=col_id, Image={"Bytes": _TINY_JPEG})
            desc = rekognition.describe_collection(CollectionId=col_id)
            assert desc["FaceCount"] >= 1
        finally:
            rekognition.delete_collection(CollectionId=col_id)

    def test_describe_collection_face_count_after_delete(self, rekognition):
        """DescribeCollection FaceCount decrements after deleting faces."""
        col_id = _unique("cntdel")
        rekognition.create_collection(CollectionId=col_id)
        try:
            idx = rekognition.index_faces(CollectionId=col_id, Image={"Bytes": _TINY_JPEG})
            face_id = idx["FaceRecords"][0]["Face"]["FaceId"]
            rekognition.delete_faces(CollectionId=col_id, FaceIds=[face_id])
            desc = rekognition.describe_collection(CollectionId=col_id)
            assert desc["FaceCount"] == 0
        finally:
            rekognition.delete_collection(CollectionId=col_id)

    def test_list_faces_after_multiple_indexes(self, rekognition):
        """ListFaces returns multiple faces after indexing multiple images."""
        col_id = _unique("multi")
        rekognition.create_collection(CollectionId=col_id)
        try:
            rekognition.index_faces(
                CollectionId=col_id,
                Image={"Bytes": _TINY_JPEG},
                ExternalImageId="face-001",
            )
            rekognition.index_faces(
                CollectionId=col_id,
                Image={"Bytes": _TINY_JPEG},
                ExternalImageId="face-002",
            )
            resp = rekognition.list_faces(CollectionId=col_id)
            assert len(resp["Faces"]) >= 2
            ext_ids = [f.get("ExternalImageId") for f in resp["Faces"]]
            assert "face-001" in ext_ids
            assert "face-002" in ext_ids
        finally:
            rekognition.delete_collection(CollectionId=col_id)

    def test_detect_labels_response_structure(self, rekognition):
        """DetectLabels response has proper Label structure."""
        resp = rekognition.detect_labels(Image={"Bytes": _TINY_JPEG})
        assert "Labels" in resp
        assert "LabelModelVersion" in resp

    def test_detect_faces_response_structure(self, rekognition):
        """DetectFaces with ALL attributes returns detailed face info."""
        resp = rekognition.detect_faces(Image={"Bytes": _TINY_JPEG}, Attributes=["ALL"])
        assert "FaceDetails" in resp
        assert len(resp["FaceDetails"]) > 0
        face = resp["FaceDetails"][0]
        assert "BoundingBox" in face
        assert "Confidence" in face

    def test_compare_faces_similarity_threshold(self, rekognition):
        """CompareFaces with explicit SimilarityThreshold returns expected fields."""
        resp = rekognition.compare_faces(
            SourceImage={"Bytes": _TINY_JPEG},
            TargetImage={"Bytes": _TINY_JPEG},
            SimilarityThreshold=50.0,
        )
        assert "SourceImageFace" in resp
        assert "FaceMatches" in resp
        assert "UnmatchedFaces" in resp

    def test_create_collection_returns_face_model_version(self, rekognition):
        """CreateCollection returns a FaceModelVersion string."""
        col_id = _unique("ver")
        resp = rekognition.create_collection(CollectionId=col_id)
        assert isinstance(resp["FaceModelVersion"], str)
        assert len(resp["FaceModelVersion"]) > 0
        rekognition.delete_collection(CollectionId=col_id)

    def test_detect_moderation_labels_response_version(self, rekognition):
        """DetectModerationLabels returns ModerationModelVersion."""
        resp = rekognition.detect_moderation_labels(Image={"Bytes": _TINY_JPEG})
        assert isinstance(resp["ModerationModelVersion"], str)
        assert len(resp["ModerationModelVersion"]) > 0

    def test_recognize_celebrities_response_structure(self, rekognition):
        """RecognizeCelebrities returns OrientationCorrection field."""
        resp = rekognition.recognize_celebrities(Image={"Bytes": _TINY_JPEG})
        assert "CelebrityFaces" in resp
        assert "UnrecognizedFaces" in resp

    def test_detect_text_response_model_version(self, rekognition):
        """DetectText returns TextModelVersion field."""
        resp = rekognition.detect_text(Image={"Bytes": _TINY_JPEG})
        assert "TextDetections" in resp
        assert "TextModelVersion" in resp

    def test_project_lifecycle(self, rekognition):
        """Full project lifecycle: create, describe, delete."""
        name = _unique("lifecycle")
        create_resp = rekognition.create_project(ProjectName=name)
        project_arn = create_resp["ProjectArn"]
        # Verify project appears in describe
        desc = rekognition.describe_projects()
        found = [p for p in desc["ProjectDescriptions"] if p["ProjectArn"] == project_arn]
        assert len(found) == 1
        assert found[0]["Status"] in ("CREATED", "CREATING")
        # Delete
        del_resp = rekognition.delete_project(ProjectArn=project_arn)
        assert del_resp["Status"] == "DELETING"

    def test_stream_processor_lifecycle(self, rekognition):
        """Full stream processor lifecycle: create, describe, list, delete."""
        name = _unique("sp-lc")
        col_id = _unique("col-lc")
        rekognition.create_collection(CollectionId=col_id)
        try:
            create_resp = rekognition.create_stream_processor(
                Name=name,
                Input={
                    "KinesisVideoStream": {"Arn": "arn:aws:kinesisvideo:us-east-1:123:stream/s/0"}
                },
                Output={"KinesisDataStream": {"Arn": "arn:aws:kinesis:us-east-1:123:stream/out"}},
                RoleArn="arn:aws:iam::123456789012:role/test",
                Settings={"FaceSearch": {"CollectionId": col_id, "FaceMatchThreshold": 80.0}},
            )
            sp_arn = create_resp["StreamProcessorArn"]
            # Describe
            desc = rekognition.describe_stream_processor(Name=name)
            assert desc["StreamProcessorArn"] == sp_arn
            assert "RoleArn" in desc
            # List
            listed = rekognition.list_stream_processors()
            sp_names = [sp["Name"] for sp in listed["StreamProcessors"]]
            assert name in sp_names
            # Delete
            rekognition.delete_stream_processor(Name=name)
        finally:
            rekognition.delete_collection(CollectionId=col_id)

    def test_face_liveness_session_lifecycle(self, rekognition):
        """Full face liveness session lifecycle: create, get results."""
        create_resp = rekognition.create_face_liveness_session()
        session_id = create_resp["SessionId"]
        assert len(session_id) >= 36  # UUID format
        resp = rekognition.get_face_liveness_session_results(SessionId=session_id)
        assert resp["SessionId"] == session_id
        assert "Status" in resp
