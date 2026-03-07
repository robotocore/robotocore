"""Transcribe compatibility tests."""

import uuid

import pytest

from tests.compatibility.conftest import make_client


def _uid():
    return uuid.uuid4().hex[:8]


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

    def test_list_vocabularies(self, transcribe):
        name = f"list-vocab-{_uid()}"
        transcribe.create_vocabulary(
            VocabularyName=name, LanguageCode="en-US", Phrases=["test"]
        )
        response = transcribe.list_vocabularies()
        names = [v["VocabularyName"] for v in response.get("Vocabularies", [])]
        assert name in names
        transcribe.delete_vocabulary(VocabularyName=name)

    def test_delete_vocabulary(self, transcribe):
        name = f"del-vocab-{_uid()}"
        transcribe.create_vocabulary(
            VocabularyName=name, LanguageCode="en-US", Phrases=["hello"]
        )
        response = transcribe.delete_vocabulary(VocabularyName=name)
        assert response["ResponseMetadata"]["HTTPStatusCode"] == 200

    def test_start_job_with_settings(self, transcribe):
        name = f"settings-job-{_uid()}"
        response = transcribe.start_transcription_job(
            TranscriptionJobName=name,
            LanguageCode="en-US",
            Media={"MediaFileUri": "s3://my-bucket/audio.wav"},
            Settings={"ShowSpeakerLabels": True, "MaxSpeakerLabels": 5},
        )
        job = response["TranscriptionJob"]
        assert job["TranscriptionJobName"] == name
        transcribe.delete_transcription_job(TranscriptionJobName=name)

    def test_list_transcription_jobs_with_status(self, transcribe):
        name = f"status-job-{_uid()}"
        transcribe.start_transcription_job(
            TranscriptionJobName=name,
            LanguageCode="en-US",
            Media={"MediaFileUri": "s3://my-bucket/audio.wav"},
        )
        response = transcribe.list_transcription_jobs(Status="IN_PROGRESS")
        assert "TranscriptionJobSummaries" in response
        transcribe.delete_transcription_job(TranscriptionJobName=name)
