"""Compatibility tests for Kinesis Video Archived Media service."""

import datetime
import uuid

import pytest
from botocore.exceptions import ClientError

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
    """The surface moto's kinesisvideoarchivedmedia backend serves."""

    def test_get_hls_streaming_session_url(self, kvam, created_stream):
        resp = kvam.get_hls_streaming_session_url(StreamName=created_stream["StreamName"])
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200
        url = resp["HLSStreamingSessionURL"]
        assert url.startswith("https://b-")
        assert ".kinesisvideo.us-east-1.amazonaws.com" in url
        assert "/hls/v1/getHLSMasterPlaylist.m3u8?SessionToken=" in url

    def test_get_hls_streaming_session_url_by_arn_matches_data_endpoint(
        self, kvam, created_stream, kinesisvideo
    ):
        endpoint = kinesisvideo.get_data_endpoint(
            StreamName=created_stream["StreamName"],
            APIName="GET_HLS_STREAMING_SESSION_URL",
        )["DataEndpoint"]
        url = kvam.get_hls_streaming_session_url(StreamARN=created_stream["StreamARN"])[
            "HLSStreamingSessionURL"
        ]
        assert url.startswith(endpoint)

    def test_get_dash_streaming_session_url(self, kvam, created_stream):
        resp = kvam.get_dash_streaming_session_url(StreamName=created_stream["StreamName"])
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200
        url = resp["DASHStreamingSessionURL"]
        assert "/dash/v1/getDASHManifest.mpd?SessionToken=" in url

    def test_get_clip(self, kvam, created_stream):
        resp = kvam.get_clip(
            StreamName=created_stream["StreamName"],
            ClipFragmentSelector={
                "FragmentSelectorType": "SERVER_TIMESTAMP",
                "TimestampRange": {
                    "StartTimestamp": datetime.datetime(2024, 1, 1, tzinfo=datetime.UTC),
                    "EndTimestamp": datetime.datetime(2024, 1, 2, tzinfo=datetime.UTC),
                },
            },
        )
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200
        assert resp["ContentType"] == "video/mp4"
        assert resp["Payload"].read() == b"sample-mp4-video"

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


class TestOutsideTheMotoSurface:
    """Botocore members moto routes but never handles: moto's own catch-all
    raises the NotImplemented template for them, so the twin answers 501.
    (The checked-in moto fork has neither a handler method nor a dedicated
    route — these pins keep the boundary honest rather than aspirational.)
    """

    @pytest.mark.parametrize(
        "op",
        ["get_images", "get_media_for_fragment_list", "list_fragments"],
    )
    def test_outside_operations_answer_motos_501(self, kvam, created_stream, op):
        kwargs = {"StreamName": created_stream["StreamName"]}
        if op == "get_images":
            kwargs.update(
                ImageSelectorType="SERVER_TIMESTAMP",
                StartTimestamp=datetime.datetime(2024, 1, 1, tzinfo=datetime.UTC),
                EndTimestamp=datetime.datetime(2024, 1, 2, tzinfo=datetime.UTC),
                Format="JPEG",
            )
        if op == "get_media_for_fragment_list":
            kwargs["Fragments"] = ["fragment-0001"]
        with pytest.raises(ClientError) as exc_info:
            getattr(kvam, op)(**kwargs)
        err = exc_info.value.response["Error"]
        assert err["Code"] == "NotImplemented"
        assert exc_info.value.response["ResponseMetadata"]["HTTPStatusCode"] == 501
        assert err["Message"].startswith(f"The {op} action has not been")
