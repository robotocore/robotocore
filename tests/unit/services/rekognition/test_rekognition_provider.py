"""Comprehensive unit tests for the Rekognition native provider.

Covers all action functions: collection CRUD, face operations, image analysis,
video analysis (start/get pattern), projects, stream processors, face liveness,
and tagging. Tests inner functions directly without a running server.
"""

import json
from unittest.mock import AsyncMock, MagicMock

import pytest

from robotocore.services.rekognition import provider as rekog


@pytest.fixture(autouse=True)
def _clear_state():
    """Reset all module-level stores between tests."""
    rekog._collections.clear()
    rekog._tags.clear()
    rekog._faces.clear()
    rekog._video_jobs.clear()
    rekog._projects.clear()
    rekog._stream_processors.clear()
    rekog._liveness_sessions.clear()
    yield
    rekog._collections.clear()
    rekog._tags.clear()
    rekog._faces.clear()
    rekog._video_jobs.clear()
    rekog._projects.clear()
    rekog._stream_processors.clear()
    rekog._liveness_sessions.clear()


ACCOUNT = "123456789012"
REGION = "us-east-1"


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_request(action: str, body: dict | None = None) -> MagicMock:
    req = MagicMock()
    req.headers = {"x-amz-target": f"RekognitionService.{action}"}
    req.method = "POST"
    req.url = MagicMock()
    req.url.path = "/"
    req.query_params = {}
    payload = json.dumps(body or {}).encode()
    req.body = AsyncMock(return_value=payload)
    return req


def _call(fn, params=None):
    """Call an inner handler function directly."""
    return fn(params or {}, REGION, ACCOUNT)


def _create_collection(cid="test-col"):
    return _call(rekog._create_collection, {"CollectionId": cid})


def _create_project(name="test-proj"):
    return _call(rekog._create_project, {"ProjectName": name})


def _create_stream_processor(name="test-sp"):
    return _call(
        rekog._create_stream_processor,
        {
            "Name": name,
            "Input": {"KinesisVideoStream": {"Arn": "arn:aws:kinesisvideo:us-east-1:123:stream/s"}},
            "Output": {"KinesisDataStream": {"Arn": "arn:aws:kinesis:us-east-1:123:stream/o"}},
            "RoleArn": "arn:aws:iam::123:role/r",
            "Settings": {"FaceSearch": {"CollectionId": "c", "FaceMatchThreshold": 80.0}},
        },
    )


# ---------------------------------------------------------------------------
# Collection CRUD (direct function calls)
# ---------------------------------------------------------------------------


class TestCreateCollection:
    def test_returns_arn_and_model_version(self):
        result = _create_collection("my-col")
        assert result["StatusCode"] == 200
        assert "my-col" in result["CollectionArn"]
        assert result["FaceModelVersion"] == "6.0"

    def test_duplicate_returns_error_tuple(self):
        _create_collection("dup")
        result = _create_collection("dup")
        assert isinstance(result, tuple)
        assert result[0] == 400
        assert "ResourceAlreadyExistsException" in result[1]["__type"]

    def test_stores_tags_on_create(self):
        _call(
            rekog._create_collection,
            {"CollectionId": "tagged", "Tags": {"env": "prod"}},
        )
        arn = rekog._collection_arn(ACCOUNT, REGION, "tagged")
        assert rekog._tags[arn] == {"env": "prod"}

    def test_creates_empty_tags_when_none_provided(self):
        _create_collection("no-tags")
        arn = rekog._collection_arn(ACCOUNT, REGION, "no-tags")
        assert rekog._tags[arn] == {}


class TestDescribeCollection:
    def test_success(self):
        _create_collection("desc-col")
        result = _call(rekog._describe_collection, {"CollectionId": "desc-col"})
        assert result["FaceCount"] == 0
        assert result["FaceModelVersion"] == "6.0"
        assert "desc-col" in result["CollectionARN"]
        assert "CreationTimestamp" in result

    def test_not_found(self):
        result = _call(rekog._describe_collection, {"CollectionId": "nope"})
        assert isinstance(result, tuple)
        assert result[0] == 400
        assert "ResourceNotFoundException" in result[1]["__type"]


class TestListCollections:
    def test_empty(self):
        result = _call(rekog._list_collections)
        assert result["CollectionIds"] == []
        assert result["FaceModelVersions"] == []

    def test_returns_sorted_ids(self):
        for cid in ["c", "a", "b"]:
            _create_collection(cid)
        result = _call(rekog._list_collections)
        assert result["CollectionIds"] == ["a", "b", "c"]

    def test_pagination_with_max_results(self):
        for i in range(5):
            _create_collection(f"col-{i:02d}")
        result = _call(rekog._list_collections, {"MaxResults": 2})
        assert len(result["CollectionIds"]) == 2
        assert result["NextToken"] == "2"

    def test_pagination_next_token(self):
        for i in range(5):
            _create_collection(f"col-{i:02d}")
        result = _call(rekog._list_collections, {"MaxResults": 2, "NextToken": "2"})
        assert len(result["CollectionIds"]) == 2
        assert result["NextToken"] == "4"

    def test_last_page_has_no_next_token(self):
        for i in range(3):
            _create_collection(f"col-{i}")
        result = _call(rekog._list_collections, {"MaxResults": 10})
        assert "NextToken" not in result


class TestDeleteCollection:
    def test_success(self):
        _create_collection("del-me")
        result = _call(rekog._delete_collection, {"CollectionId": "del-me"})
        assert result["StatusCode"] == 200
        store = rekog._get_collections(ACCOUNT, REGION)
        assert "del-me" not in store

    def test_not_found(self):
        result = _call(rekog._delete_collection, {"CollectionId": "ghost"})
        assert isinstance(result, tuple)
        assert result[0] == 400

    def test_cleans_up_tags(self):
        _call(
            rekog._create_collection,
            {"CollectionId": "tagged-del", "Tags": {"k": "v"}},
        )
        arn = rekog._collection_arn(ACCOUNT, REGION, "tagged-del")
        assert arn in rekog._tags
        _call(rekog._delete_collection, {"CollectionId": "tagged-del"})
        assert arn not in rekog._tags

    def test_cleans_up_faces(self):
        _create_collection("face-del")
        _call(
            rekog._index_faces,
            {"CollectionId": "face-del", "Image": {"Bytes": "fake"}},
        )
        face_store = rekog._get_faces(ACCOUNT, REGION)
        assert "face-del" in face_store
        _call(rekog._delete_collection, {"CollectionId": "face-del"})
        assert "face-del" not in face_store


# ---------------------------------------------------------------------------
# Face operations
# ---------------------------------------------------------------------------


class TestIndexFaces:
    def test_returns_face_record(self):
        _create_collection("face-col")
        result = _call(
            rekog._index_faces,
            {"CollectionId": "face-col", "Image": {"Bytes": "fake"}},
        )
        assert len(result["FaceRecords"]) == 1
        face = result["FaceRecords"][0]["Face"]
        assert "FaceId" in face
        assert "ImageId" in face
        assert face["Confidence"] == 99.99
        assert result["FaceModelVersion"] == "6.0"
        assert result["UnindexedFaces"] == []

    def test_increments_face_count(self):
        _create_collection("count-col")
        _call(rekog._index_faces, {"CollectionId": "count-col", "Image": {"Bytes": "a"}})
        _call(rekog._index_faces, {"CollectionId": "count-col", "Image": {"Bytes": "b"}})
        store = rekog._get_collections(ACCOUNT, REGION)
        assert store["count-col"]["FaceCount"] == 2

    def test_stores_external_image_id(self):
        _create_collection("ext-col")
        result = _call(
            rekog._index_faces,
            {
                "CollectionId": "ext-col",
                "Image": {"Bytes": "fake"},
                "ExternalImageId": "my-photo-123",
            },
        )
        assert result["FaceRecords"][0]["Face"]["ExternalImageId"] == "my-photo-123"

    def test_not_found_collection(self):
        result = _call(
            rekog._index_faces,
            {"CollectionId": "no-col", "Image": {"Bytes": "fake"}},
        )
        assert isinstance(result, tuple)
        assert result[0] == 400

    def test_face_detail_has_landmarks_and_quality(self):
        _create_collection("detail-col")
        result = _call(
            rekog._index_faces,
            {"CollectionId": "detail-col", "Image": {"Bytes": "x"}},
        )
        detail = result["FaceRecords"][0]["FaceDetail"]
        assert len(detail["Landmarks"]) == 3
        assert detail["Quality"]["Brightness"] == 80.0
        assert detail["Quality"]["Sharpness"] == 90.0


class TestListFaces:
    def test_empty_collection(self):
        _create_collection("empty-faces")
        result = _call(rekog._list_faces, {"CollectionId": "empty-faces"})
        assert result["Faces"] == []
        assert result["FaceModelVersion"] == "6.0"

    def test_lists_indexed_faces(self):
        _create_collection("list-faces")
        _call(rekog._index_faces, {"CollectionId": "list-faces", "Image": {"Bytes": "a"}})
        _call(rekog._index_faces, {"CollectionId": "list-faces", "Image": {"Bytes": "b"}})
        result = _call(rekog._list_faces, {"CollectionId": "list-faces"})
        assert len(result["Faces"]) == 2

    def test_max_results(self):
        _create_collection("max-faces")
        for _ in range(5):
            _call(rekog._index_faces, {"CollectionId": "max-faces", "Image": {"Bytes": "x"}})
        result = _call(rekog._list_faces, {"CollectionId": "max-faces", "MaxResults": 2})
        assert len(result["Faces"]) == 2

    def test_not_found(self):
        result = _call(rekog._list_faces, {"CollectionId": "nope"})
        assert isinstance(result, tuple)
        assert result[0] == 400


class TestSearchFaces:
    def test_returns_matches_excluding_query(self):
        _create_collection("search-col")
        r1 = _call(rekog._index_faces, {"CollectionId": "search-col", "Image": {"Bytes": "a"}})
        r2 = _call(rekog._index_faces, {"CollectionId": "search-col", "Image": {"Bytes": "b"}})
        face_id_1 = r1["FaceRecords"][0]["Face"]["FaceId"]
        face_id_2 = r2["FaceRecords"][0]["Face"]["FaceId"]

        result = _call(
            rekog._search_faces,
            {"CollectionId": "search-col", "FaceId": face_id_1},
        )
        assert result["SearchedFaceId"] == face_id_1
        match_ids = [m["Face"]["FaceId"] for m in result["FaceMatches"]]
        assert face_id_2 in match_ids
        assert face_id_1 not in match_ids
        assert result["FaceModelVersion"] == "6.0"

    def test_empty_collection_no_matches(self):
        _create_collection("empty-search")
        result = _call(
            rekog._search_faces,
            {"CollectionId": "empty-search", "FaceId": "fake-id"},
        )
        assert result["FaceMatches"] == []

    def test_not_found(self):
        result = _call(
            rekog._search_faces,
            {"CollectionId": "nope", "FaceId": "x"},
        )
        assert isinstance(result, tuple)
        assert result[0] == 400


class TestSearchFacesByImage:
    def test_returns_all_faces_as_matches(self):
        _create_collection("sbi-col")
        _call(rekog._index_faces, {"CollectionId": "sbi-col", "Image": {"Bytes": "a"}})
        _call(rekog._index_faces, {"CollectionId": "sbi-col", "Image": {"Bytes": "b"}})

        result = _call(
            rekog._search_faces_by_image,
            {"CollectionId": "sbi-col", "Image": {"Bytes": "query"}},
        )
        assert len(result["FaceMatches"]) == 2
        assert result["SearchedFaceConfidence"] == 99.99
        assert "SearchedFaceBoundingBox" in result
        assert result["FaceModelVersion"] == "6.0"

    def test_empty_collection(self):
        _create_collection("sbi-empty")
        result = _call(
            rekog._search_faces_by_image,
            {"CollectionId": "sbi-empty", "Image": {"Bytes": "q"}},
        )
        assert result["FaceMatches"] == []

    def test_not_found(self):
        result = _call(
            rekog._search_faces_by_image,
            {"CollectionId": "nope", "Image": {"Bytes": "q"}},
        )
        assert isinstance(result, tuple)
        assert result[0] == 400


class TestDeleteFaces:
    def test_deletes_specified_faces(self):
        _create_collection("del-faces")
        r1 = _call(rekog._index_faces, {"CollectionId": "del-faces", "Image": {"Bytes": "a"}})
        r2 = _call(rekog._index_faces, {"CollectionId": "del-faces", "Image": {"Bytes": "b"}})
        fid1 = r1["FaceRecords"][0]["Face"]["FaceId"]
        fid2 = r2["FaceRecords"][0]["Face"]["FaceId"]

        result = _call(
            rekog._delete_faces,
            {"CollectionId": "del-faces", "FaceIds": [fid1]},
        )
        assert fid1 in result["DeletedFaces"]
        assert fid2 not in result["DeletedFaces"]

        # Face count updated
        store = rekog._get_collections(ACCOUNT, REGION)
        assert store["del-faces"]["FaceCount"] == 1

    def test_deletes_nonexistent_face_ids_silently(self):
        _create_collection("del-silent")
        result = _call(
            rekog._delete_faces,
            {"CollectionId": "del-silent", "FaceIds": ["nonexistent-id"]},
        )
        assert result["DeletedFaces"] == []

    def test_not_found_collection(self):
        result = _call(
            rekog._delete_faces,
            {"CollectionId": "nope", "FaceIds": ["x"]},
        )
        assert isinstance(result, tuple)
        assert result[0] == 400


# ---------------------------------------------------------------------------
# Image analysis (synthetic / mock responses)
# ---------------------------------------------------------------------------


class TestDetectFaces:
    def test_returns_face_details(self):
        result = _call(rekog._detect_faces, {"Image": {"Bytes": "fake"}})
        assert len(result["FaceDetails"]) == 1
        detail = result["FaceDetails"][0]
        assert detail["Confidence"] == 99.99
        assert "BoundingBox" in detail
        assert len(detail["Landmarks"]) == 5
        assert detail["Quality"]["Brightness"] == 80.0
        assert "Pose" in detail


class TestDetectModerationLabels:
    def test_returns_empty_labels(self):
        result = _call(rekog._detect_moderation_labels, {"Image": {"Bytes": "safe"}})
        assert result["ModerationLabels"] == []
        assert result["ModerationModelVersion"] == "6.0"


class TestDetectProtectiveEquipment:
    def test_returns_person_with_body_parts(self):
        result = _call(rekog._detect_protective_equipment, {"Image": {"Bytes": "ppe"}})
        assert len(result["Persons"]) == 1
        person = result["Persons"][0]
        assert person["Id"] == 0
        assert person["Confidence"] == 99.0
        assert len(person["BodyParts"]) == 1
        assert person["BodyParts"][0]["Name"] == "FACE"
        assert result["ProtectiveEquipmentModelVersion"] == "1.0"


class TestRecognizeCelebrities:
    def test_returns_empty_celebrities_with_unrecognized(self):
        result = _call(rekog._recognize_celebrities, {"Image": {"Bytes": "celeb"}})
        assert result["CelebrityFaces"] == []
        assert len(result["UnrecognizedFaces"]) == 1
        unrecognized = result["UnrecognizedFaces"][0]
        assert "BoundingBox" in unrecognized
        assert unrecognized["Confidence"] == 99.0


class TestGetCelebrityInfo:
    def test_returns_celebrity_by_id(self):
        result = _call(rekog._get_celebrity_info, {"Id": "abc123"})
        assert result["Name"] == "Celebrity-abc123"
        assert result["Urls"] == []

    def test_empty_id(self):
        result = _call(rekog._get_celebrity_info, {})
        assert result["Name"] == "Celebrity-"


# ---------------------------------------------------------------------------
# Video analysis (start/get pattern)
# ---------------------------------------------------------------------------


class TestStartGetFaceDetection:
    def test_start_returns_job_id(self):
        result = _call(rekog._start_face_detection, {"Video": {"S3Object": {}}})
        assert "JobId" in result
        assert len(result["JobId"]) == 36  # UUID

    def test_get_returns_job_results(self):
        start = _call(rekog._start_face_detection, {"Video": {"S3Object": {}}})
        result = _call(rekog._get_face_detection, {"JobId": start["JobId"]})
        assert result["JobStatus"] == "SUCCEEDED"
        assert "VideoMetadata" in result
        assert len(result["Faces"]) == 1
        assert result["Faces"][0]["Timestamp"] == 0

    def test_get_unknown_job_returns_defaults(self):
        result = _call(rekog._get_face_detection, {"JobId": "unknown"})
        assert result["JobStatus"] == "SUCCEEDED"
        assert result["Faces"] == []


class TestStartGetLabelDetection:
    def test_roundtrip(self):
        start = _call(rekog._start_label_detection, {"Video": {"S3Object": {}}})
        result = _call(rekog._get_label_detection, {"JobId": start["JobId"]})
        assert result["JobStatus"] == "SUCCEEDED"
        assert len(result["Labels"]) == 1
        assert result["Labels"][0]["Label"]["Name"] == "Person"
        assert result["LabelModelVersion"] == "3.0"

    def test_get_unknown_job(self):
        result = _call(rekog._get_label_detection, {"JobId": "missing"})
        assert result["Labels"] == []
        assert result["LabelModelVersion"] == "3.0"


class TestStartGetCelebrityRecognition:
    def test_roundtrip(self):
        start = _call(rekog._start_celebrity_recognition, {"Video": {"S3Object": {}}})
        result = _call(rekog._get_celebrity_recognition, {"JobId": start["JobId"]})
        assert result["JobStatus"] == "SUCCEEDED"
        assert result["Celebrities"] == []
        assert "VideoMetadata" in result

    def test_get_unknown_job(self):
        result = _call(rekog._get_celebrity_recognition, {"JobId": "missing"})
        assert result["Celebrities"] == []


class TestStartGetContentModeration:
    def test_roundtrip(self):
        start = _call(rekog._start_content_moderation, {"Video": {"S3Object": {}}})
        result = _call(rekog._get_content_moderation, {"JobId": start["JobId"]})
        assert result["JobStatus"] == "SUCCEEDED"
        assert result["ModerationLabels"] == []
        assert result["ModerationModelVersion"] == "6.0"


class TestStartGetPersonTracking:
    def test_roundtrip(self):
        start = _call(rekog._start_person_tracking, {"Video": {"S3Object": {}}})
        result = _call(rekog._get_person_tracking, {"JobId": start["JobId"]})
        assert result["JobStatus"] == "SUCCEEDED"
        assert len(result["Persons"]) == 1
        assert result["Persons"][0]["Person"]["Index"] == 0


class TestStartGetSegmentDetection:
    def test_roundtrip(self):
        start = _call(rekog._start_segment_detection, {"Video": {"S3Object": {}}})
        result = _call(rekog._get_segment_detection, {"JobId": start["JobId"]})
        assert result["JobStatus"] == "SUCCEEDED"
        assert result["Segments"] == []
        assert len(result["SelectedSegmentTypes"]) == 2
        assert result["SelectedSegmentTypes"][0]["Type"] == "TECHNICAL_CUE"

    def test_video_metadata_is_list(self):
        start = _call(rekog._start_segment_detection, {"Video": {"S3Object": {}}})
        result = _call(rekog._get_segment_detection, {"JobId": start["JobId"]})
        assert isinstance(result["VideoMetadata"], list)


class TestVideoMetadataHelper:
    def test_returns_expected_fields(self):
        meta = rekog._video_metadata()
        assert meta["Codec"] == "h264"
        assert meta["DurationMillis"] == 5000
        assert meta["FrameRate"] == 29.97
        assert meta["FrameHeight"] == 720
        assert meta["FrameWidth"] == 1280


# ---------------------------------------------------------------------------
# Projects
# ---------------------------------------------------------------------------


class TestCreateProject:
    def test_returns_arn(self):
        result = _create_project("my-proj")
        assert "ProjectArn" in result
        assert "my-proj" in result["ProjectArn"]

    def test_duplicate_returns_error(self):
        _create_project("dup-proj")
        result = _create_project("dup-proj")
        assert isinstance(result, tuple)
        assert result[0] == 400
        assert "ResourceInUseException" in result[1]["__type"]

    def test_stores_project_metadata(self):
        _create_project("meta-proj")
        store = rekog._get_projects(ACCOUNT, REGION)
        assert "meta-proj" in store
        assert store["meta-proj"]["Status"] == "CREATED"


class TestDescribeProjects:
    def test_empty(self):
        result = _call(rekog._describe_projects)
        assert result["ProjectDescriptions"] == []

    def test_lists_created_projects(self):
        _create_project("p1")
        _create_project("p2")
        result = _call(rekog._describe_projects)
        assert len(result["ProjectDescriptions"]) == 2
        statuses = {p["Status"] for p in result["ProjectDescriptions"]}
        assert statuses == {"CREATED"}

    def test_max_results(self):
        for i in range(5):
            _create_project(f"proj-{i}")
        result = _call(rekog._describe_projects, {"MaxResults": 2})
        assert len(result["ProjectDescriptions"]) == 2


class TestDeleteProject:
    def test_success(self):
        create_result = _create_project("del-proj")
        arn = create_result["ProjectArn"]
        result = _call(rekog._delete_project, {"ProjectArn": arn})
        assert result["Status"] == "DELETING"
        store = rekog._get_projects(ACCOUNT, REGION)
        assert "del-proj" not in store

    def test_not_found(self):
        fake_arn = "arn:aws:rekognition:us-east-1:123:project/nope/999"
        result = _call(rekog._delete_project, {"ProjectArn": fake_arn})
        assert isinstance(result, tuple)
        assert result[0] == 400

    def test_cleans_up_tags(self):
        create_result = _create_project("tag-proj")
        arn = create_result["ProjectArn"]
        rekog._tags[arn] = {"k": "v"}
        _call(rekog._delete_project, {"ProjectArn": arn})
        assert arn not in rekog._tags


# ---------------------------------------------------------------------------
# Stream Processors
# ---------------------------------------------------------------------------


class TestCreateStreamProcessor:
    def test_returns_arn(self):
        result = _create_stream_processor("my-sp")
        assert "StreamProcessorArn" in result
        assert "my-sp" in result["StreamProcessorArn"]

    def test_duplicate_returns_error(self):
        _create_stream_processor("dup-sp")
        result = _create_stream_processor("dup-sp")
        assert isinstance(result, tuple)
        assert result[0] == 400
        assert "ResourceInUseException" in result[1]["__type"]

    def test_stores_all_fields(self):
        _create_stream_processor("full-sp")
        store = rekog._get_stream_processors(ACCOUNT, REGION)
        sp = store["full-sp"]
        assert sp["Status"] == "STOPPED"
        assert "Input" in sp
        assert "Output" in sp
        assert "RoleArn" in sp
        assert "Settings" in sp


class TestDescribeStreamProcessor:
    def test_success(self):
        _create_stream_processor("desc-sp")
        result = _call(rekog._describe_stream_processor, {"Name": "desc-sp"})
        assert result["Name"] == "desc-sp"
        assert result["Status"] == "STOPPED"
        assert "StreamProcessorArn" in result
        assert "CreationTimestamp" in result

    def test_not_found(self):
        result = _call(rekog._describe_stream_processor, {"Name": "nope"})
        assert isinstance(result, tuple)
        assert result[0] == 400


class TestListStreamProcessors:
    def test_empty(self):
        result = _call(rekog._list_stream_processors)
        assert result["StreamProcessors"] == []

    def test_lists_created_processors(self):
        _create_stream_processor("sp1")
        _create_stream_processor("sp2")
        result = _call(rekog._list_stream_processors)
        assert len(result["StreamProcessors"]) == 2
        names = {sp["Name"] for sp in result["StreamProcessors"]}
        assert names == {"sp1", "sp2"}

    def test_max_results(self):
        for i in range(5):
            _create_stream_processor(f"sp-{i}")
        result = _call(rekog._list_stream_processors, {"MaxResults": 2})
        assert len(result["StreamProcessors"]) == 2


class TestDeleteStreamProcessor:
    def test_success(self):
        _create_stream_processor("del-sp")
        result = _call(rekog._delete_stream_processor, {"Name": "del-sp"})
        assert result == {}
        store = rekog._get_stream_processors(ACCOUNT, REGION)
        assert "del-sp" not in store

    def test_not_found(self):
        result = _call(rekog._delete_stream_processor, {"Name": "ghost"})
        assert isinstance(result, tuple)
        assert result[0] == 400

    def test_cleans_up_tags(self):
        _create_stream_processor("tag-sp")
        arn = rekog._stream_processor_arn(ACCOUNT, REGION, "tag-sp")
        rekog._tags[arn] = {"k": "v"}
        _call(rekog._delete_stream_processor, {"Name": "tag-sp"})
        assert arn not in rekog._tags


# ---------------------------------------------------------------------------
# Face Liveness
# ---------------------------------------------------------------------------


class TestCreateFaceLivenessSession:
    def test_returns_session_id(self):
        result = _call(rekog._create_face_liveness_session)
        assert "SessionId" in result
        assert len(result["SessionId"]) == 36  # UUID

    def test_stores_session(self):
        result = _call(rekog._create_face_liveness_session)
        sid = result["SessionId"]
        assert sid in rekog._liveness_sessions
        assert rekog._liveness_sessions[sid]["Status"] == "CREATED"
        assert rekog._liveness_sessions[sid]["Confidence"] == 99.5


class TestGetFaceLivenessSessionResults:
    def test_success(self):
        create = _call(rekog._create_face_liveness_session)
        sid = create["SessionId"]
        result = _call(rekog._get_face_liveness_session_results, {"SessionId": sid})
        assert result["SessionId"] == sid
        assert result["Status"] == "SUCCEEDED"
        assert result["Confidence"] == 99.5

    def test_not_found(self):
        result = _call(
            rekog._get_face_liveness_session_results,
            {"SessionId": "nonexistent-session"},
        )
        assert isinstance(result, tuple)
        assert result[0] == 400
        assert "SessionNotFoundException" in result[1]["__type"]


# ---------------------------------------------------------------------------
# Tagging
# ---------------------------------------------------------------------------


class TestTagResource:
    def test_tag_collection(self):
        _create_collection("tag-col")
        arn = rekog._collection_arn(ACCOUNT, REGION, "tag-col")
        result = _call(rekog._tag_resource, {"ResourceArn": arn, "Tags": {"k": "v"}})
        assert result == {}
        assert rekog._tags[arn]["k"] == "v"

    def test_tag_project(self):
        create = _create_project("tag-proj")
        arn = create["ProjectArn"]
        result = _call(rekog._tag_resource, {"ResourceArn": arn, "Tags": {"env": "dev"}})
        assert result == {}
        assert rekog._tags[arn]["env"] == "dev"

    def test_tag_stream_processor(self):
        _create_stream_processor("tag-sp")
        arn = rekog._stream_processor_arn(ACCOUNT, REGION, "tag-sp")
        result = _call(rekog._tag_resource, {"ResourceArn": arn, "Tags": {"team": "ml"}})
        assert result == {}

    def test_tag_nonexistent_resource(self):
        result = _call(
            rekog._tag_resource,
            {
                "ResourceArn": "arn:aws:rekognition:us-east-1:123:collection/nope",
                "Tags": {"k": "v"},
            },
        )
        assert isinstance(result, tuple)
        assert result[0] == 400

    def test_tag_overwrites_existing_key(self):
        _create_collection("overwrite-col")
        arn = rekog._collection_arn(ACCOUNT, REGION, "overwrite-col")
        _call(rekog._tag_resource, {"ResourceArn": arn, "Tags": {"k": "v1"}})
        _call(rekog._tag_resource, {"ResourceArn": arn, "Tags": {"k": "v2"}})
        assert rekog._tags[arn]["k"] == "v2"


class TestListTagsForResource:
    def test_returns_tags(self):
        _call(
            rekog._create_collection,
            {"CollectionId": "list-tag-col", "Tags": {"a": "1", "b": "2"}},
        )
        arn = rekog._collection_arn(ACCOUNT, REGION, "list-tag-col")
        result = _call(rekog._list_tags_for_resource, {"ResourceArn": arn})
        assert result["Tags"] == {"a": "1", "b": "2"}

    def test_nonexistent_resource(self):
        result = _call(
            rekog._list_tags_for_resource,
            {"ResourceArn": "arn:aws:rekognition:us-east-1:123:collection/nope"},
        )
        assert isinstance(result, tuple)
        assert result[0] == 400


class TestUntagResource:
    def test_removes_specified_keys(self):
        _call(
            rekog._create_collection,
            {"CollectionId": "untag-col", "Tags": {"a": "1", "b": "2", "c": "3"}},
        )
        arn = rekog._collection_arn(ACCOUNT, REGION, "untag-col")
        result = _call(rekog._untag_resource, {"ResourceArn": arn, "TagKeys": ["a", "c"]})
        assert result == {}
        assert rekog._tags[arn] == {"b": "2"}

    def test_nonexistent_key_is_silent(self):
        _create_collection("untag-silent")
        arn = rekog._collection_arn(ACCOUNT, REGION, "untag-silent")
        result = _call(rekog._untag_resource, {"ResourceArn": arn, "TagKeys": ["nonexistent"]})
        assert result == {}

    def test_nonexistent_resource(self):
        result = _call(
            rekog._untag_resource,
            {"ResourceArn": "arn:aws:rekognition:us-east-1:123:collection/nope", "TagKeys": ["k"]},
        )
        assert isinstance(result, tuple)
        assert result[0] == 400


# ---------------------------------------------------------------------------
# Resource existence helper
# ---------------------------------------------------------------------------


class TestResourceExists:
    def test_collection_arn(self):
        _create_collection("exists-col")
        arn = rekog._collection_arn(ACCOUNT, REGION, "exists-col")
        assert rekog._resource_exists(arn, REGION, ACCOUNT) is True

    def test_project_arn(self):
        create = _create_project("exists-proj")
        arn = create["ProjectArn"]
        assert rekog._resource_exists(arn, REGION, ACCOUNT) is True

    def test_stream_processor_arn(self):
        _create_stream_processor("exists-sp")
        arn = rekog._stream_processor_arn(ACCOUNT, REGION, "exists-sp")
        assert rekog._resource_exists(arn, REGION, ACCOUNT) is True

    def test_unknown_arn(self):
        fake = "arn:aws:rekognition:us-east-1:123:collection/nope"
        assert rekog._resource_exists(fake, REGION, ACCOUNT) is False


# ---------------------------------------------------------------------------
# ARN helpers
# ---------------------------------------------------------------------------


class TestArnHelpers:
    def test_collection_arn_format(self):
        arn = rekog._collection_arn("111111111111", "eu-west-1", "my-col")
        assert arn == "arn:aws:rekognition:eu-west-1:111111111111:collection/my-col"

    def test_stream_processor_arn_format(self):
        arn = rekog._stream_processor_arn("222222222222", "ap-south-1", "my-sp")
        assert arn == "arn:aws:rekognition:ap-south-1:222222222222:streamprocessor/my-sp"

    def test_project_arn_contains_name(self):
        arn = rekog._project_arn("333333333333", "us-west-2", "my-proj")
        assert "my-proj" in arn
        assert "333333333333" in arn


# ---------------------------------------------------------------------------
# Store isolation
# ---------------------------------------------------------------------------


class TestStoreIsolation:
    def test_collections_isolated_by_account(self):
        _call(rekog._create_collection, {"CollectionId": "iso-col"})
        store1 = rekog._get_collections(ACCOUNT, REGION)
        store2 = rekog._get_collections("999999999999", REGION)
        assert "iso-col" in store1
        assert "iso-col" not in store2

    def test_collections_isolated_by_region(self):
        _call(rekog._create_collection, {"CollectionId": "iso-col"})
        store1 = rekog._get_collections(ACCOUNT, REGION)
        store2 = rekog._get_collections(ACCOUNT, "eu-west-1")
        assert "iso-col" in store1
        assert "iso-col" not in store2

    def test_faces_isolated_by_account(self):
        store1 = rekog._get_faces(ACCOUNT, REGION)
        store2 = rekog._get_faces("999999999999", REGION)
        assert store1 is not store2

    def test_projects_isolated_by_region(self):
        store1 = rekog._get_projects(ACCOUNT, REGION)
        store2 = rekog._get_projects(ACCOUNT, "ap-southeast-1")
        assert store1 is not store2


# ---------------------------------------------------------------------------
# handle_rekognition_request integration (async dispatch)
# ---------------------------------------------------------------------------


class TestHandleRekognitionRequest:
    def test_dispatches_known_action(self):
        import asyncio

        req = _make_request("CreateCollection", {"CollectionId": "async-col"})
        resp = asyncio.new_event_loop().run_until_complete(
            rekog.handle_rekognition_request(req, REGION, ACCOUNT)
        )
        assert resp.status_code == 200
        body = json.loads(resp.body)
        assert "CollectionArn" in body

    def test_error_returns_status_code(self):
        import asyncio

        req = _make_request("DescribeCollection", {"CollectionId": "nope"})
        resp = asyncio.new_event_loop().run_until_complete(
            rekog.handle_rekognition_request(req, REGION, ACCOUNT)
        )
        assert resp.status_code == 400
        body = json.loads(resp.body)
        assert "ResourceNotFoundException" in body["__type"]

    def test_json_content_type(self):
        import asyncio

        req = _make_request("ListCollections", {})
        resp = asyncio.new_event_loop().run_until_complete(
            rekog.handle_rekognition_request(req, REGION, ACCOUNT)
        )
        assert resp.media_type == "application/x-amz-json-1.1"

    def test_empty_body_handled(self):
        import asyncio

        req = MagicMock()
        req.headers = {"x-amz-target": "RekognitionService.ListCollections"}
        req.body = AsyncMock(return_value=b"")
        resp = asyncio.new_event_loop().run_until_complete(
            rekog.handle_rekognition_request(req, REGION, ACCOUNT)
        )
        assert resp.status_code == 200


# ---------------------------------------------------------------------------
# Action map completeness
# ---------------------------------------------------------------------------


class TestActionMapCompleteness:
    def test_all_expected_actions_are_mapped(self):
        expected = {
            "CreateCollection",
            "DescribeCollection",
            "ListCollections",
            "DeleteCollection",
            "TagResource",
            "ListTagsForResource",
            "UntagResource",
            "IndexFaces",
            "ListFaces",
            "SearchFaces",
            "SearchFacesByImage",
            "DeleteFaces",
            "DetectFaces",
            "DetectModerationLabels",
            "DetectProtectiveEquipment",
            "RecognizeCelebrities",
            "GetCelebrityInfo",
            "StartFaceDetection",
            "GetFaceDetection",
            "StartLabelDetection",
            "GetLabelDetection",
            "StartCelebrityRecognition",
            "GetCelebrityRecognition",
            "StartContentModeration",
            "GetContentModeration",
            "StartPersonTracking",
            "GetPersonTracking",
            "StartSegmentDetection",
            "GetSegmentDetection",
            "CreateProject",
            "DescribeProjects",
            "DeleteProject",
            "CreateStreamProcessor",
            "DescribeStreamProcessor",
            "ListStreamProcessors",
            "DeleteStreamProcessor",
            "CreateFaceLivenessSession",
            "GetFaceLivenessSessionResults",
            "ListUsers",
        }
        assert set(rekog._ACTION_MAP.keys()) == expected

    def test_all_action_map_values_are_callable(self):
        for action, handler in rekog._ACTION_MAP.items():
            assert callable(handler), f"{action} handler is not callable"
