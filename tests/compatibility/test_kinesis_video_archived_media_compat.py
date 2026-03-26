"""Compatibility tests for Kinesis Video Archived Media service."""

import datetime
import uuid

import pytest

from .conftest import make_client


@pytest.fixture
def kinesisvideo():
    return make_client("kinesisvideo")


@pytest.fixture
def kvam():
    return make_client("kinesis-video-archived-media")


@pytest.fixture
def stream_name():
    return f"test-kvam-{uuid.uuid4().hex[:8]}"


@pytest.fixture
def created_stream(kinesisvideo, stream_name):
    """Create a KV stream and clean it up after the test."""
    resp = kinesisvideo.create_stream(StreamName=stream_name, DataRetentionInHours=24)
    arn = resp["StreamARN"]
    yield {"StreamName": stream_name, "StreamARN": arn}
    try:
        kinesisvideo.delete_stream(StreamARN=arn)
    except Exception:
        pass  # best-effort cleanup


class TestKinesisVideoArchivedMediaOperations:
    """Tests for Kinesis Video Archived Media operations."""

    def test_list_fragments_empty(self, kvam, created_stream):
        resp = kvam.list_fragments(StreamName=created_stream["StreamName"])
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200
        assert "Fragments" in resp
        assert isinstance(resp["Fragments"], list)

    def test_list_fragments_by_arn(self, kvam, created_stream):
        resp = kvam.list_fragments(StreamARN=created_stream["StreamARN"])
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200
        assert "Fragments" in resp

    def test_get_images_empty(self, kvam, created_stream):
        resp = kvam.get_images(
            StreamName=created_stream["StreamName"],
            ImageSelectorType="SERVER_TIMESTAMP",
            StartTimestamp=datetime.datetime(2024, 1, 1, tzinfo=datetime.UTC),
            EndTimestamp=datetime.datetime(2024, 1, 2, tzinfo=datetime.UTC),
            Format="JPEG",
        )
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200
        assert "Images" in resp
        assert isinstance(resp["Images"], list)

    def test_get_media_for_fragment_list(self, kvam, created_stream):
        resp = kvam.get_media_for_fragment_list(
            StreamName=created_stream["StreamName"],
            Fragments=["fragment-0001"],
        )
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200
        assert "ContentType" in resp
        assert "Payload" in resp

    def test_get_clip_not_found(self, kvam):
        """GetClip raises ResourceNotFoundException for nonexistent stream."""
        with pytest.raises(kvam.exceptions.ResourceNotFoundException):
            kvam.get_clip(
                StreamName="nonexistent-stream-xyz",
                ClipFragmentSelector={
                    "FragmentSelectorType": "SERVER_TIMESTAMP",
                    "TimestampRange": {
                        "StartTimestamp": datetime.datetime(2024, 1, 1, tzinfo=datetime.UTC),
                        "EndTimestamp": datetime.datetime(2024, 1, 2, tzinfo=datetime.UTC),
                    },
                },
            )

    def test_get_dash_streaming_session_url_not_found(self, kvam):
        """GetDASHStreamingSessionURL raises ResourceNotFoundException for nonexistent stream."""
        with pytest.raises(kvam.exceptions.ResourceNotFoundException):
            kvam.get_dash_streaming_session_url(
                StreamName="nonexistent-stream-xyz",
            )

    def test_get_hls_streaming_session_url_not_found(self, kvam):
        """GetHLSStreamingSessionURL raises ResourceNotFoundException for nonexistent stream."""
        with pytest.raises(kvam.exceptions.ResourceNotFoundException):
            kvam.get_hls_streaming_session_url(
                StreamName="nonexistent-stream-xyz",
            )
