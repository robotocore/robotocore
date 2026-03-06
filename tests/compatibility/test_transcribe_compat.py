"""Transcribe compatibility tests."""

import pytest

from tests.compatibility.conftest import make_client


@pytest.fixture
def transcribe():
    return make_client("transcribe")


class TestTranscribeOperations:
    def test_start_transcription_job(self, transcribe):
        response = transcribe.start_transcription_job(
            TranscriptionJobName="test-job",
            LanguageCode="en-US",
            Media={"MediaFileUri": "s3://my-bucket/my-audio.wav"},
        )
        job = response["TranscriptionJob"]
        assert job["TranscriptionJobName"] == "test-job"
        assert job["LanguageCode"] == "en-US"

        # Cleanup
        transcribe.delete_transcription_job(TranscriptionJobName="test-job")

    def test_get_transcription_job(self, transcribe):
        transcribe.start_transcription_job(
            TranscriptionJobName="get-job",
            LanguageCode="en-US",
            Media={"MediaFileUri": "s3://my-bucket/audio.wav"},
        )
        response = transcribe.get_transcription_job(TranscriptionJobName="get-job")
        job = response["TranscriptionJob"]
        assert job["TranscriptionJobName"] == "get-job"

        # Cleanup
        transcribe.delete_transcription_job(TranscriptionJobName="get-job")

    def test_list_transcription_jobs(self, transcribe):
        transcribe.start_transcription_job(
            TranscriptionJobName="list-job",
            LanguageCode="en-US",
            Media={"MediaFileUri": "s3://my-bucket/audio.wav"},
        )
        response = transcribe.list_transcription_jobs()
        job_names = [j["TranscriptionJobName"] for j in response["TranscriptionJobSummaries"]]
        assert "list-job" in job_names

        # Cleanup
        transcribe.delete_transcription_job(TranscriptionJobName="list-job")

    def test_delete_transcription_job(self, transcribe):
        transcribe.start_transcription_job(
            TranscriptionJobName="delete-job",
            LanguageCode="en-US",
            Media={"MediaFileUri": "s3://my-bucket/audio.wav"},
        )
        response = transcribe.delete_transcription_job(TranscriptionJobName="delete-job")
        assert response["ResponseMetadata"]["HTTPStatusCode"] == 200

    def test_create_vocabulary(self, transcribe):
        response = transcribe.create_vocabulary(
            VocabularyName="test-vocab",
            LanguageCode="en-US",
            Phrases=["hello", "world"],
        )
        assert response["VocabularyName"] == "test-vocab"
        assert response["LanguageCode"] == "en-US"

        # Verify it can be retrieved
        get_response = transcribe.get_vocabulary(VocabularyName="test-vocab")
        assert get_response["VocabularyName"] == "test-vocab"

        # Cleanup
        transcribe.delete_vocabulary(VocabularyName="test-vocab")
