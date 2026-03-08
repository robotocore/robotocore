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
        transcribe.create_vocabulary(VocabularyName=name, LanguageCode="en-US", Phrases=["test"])
        response = transcribe.list_vocabularies()
        names = [v["VocabularyName"] for v in response.get("Vocabularies", [])]
        assert name in names
        transcribe.delete_vocabulary(VocabularyName=name)

    def test_delete_vocabulary(self, transcribe):
        name = f"del-vocab-{_uid()}"
        transcribe.create_vocabulary(VocabularyName=name, LanguageCode="en-US", Phrases=["hello"])
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


class TestTranscribeExtended:
    @pytest.fixture
    def transcribe(self):
        return make_client("transcribe")

    def test_start_job_with_output_location(self, transcribe):
        name = f"output-job-{_uid()}"
        resp = transcribe.start_transcription_job(
            TranscriptionJobName=name,
            LanguageCode="en-US",
            Media={"MediaFileUri": "s3://my-bucket/audio.wav"},
            OutputBucketName="my-output-bucket",
        )
        try:
            assert resp["TranscriptionJob"]["TranscriptionJobName"] == name
        finally:
            transcribe.delete_transcription_job(TranscriptionJobName=name)

    def test_start_job_with_media_format(self, transcribe):
        name = f"format-job-{_uid()}"
        resp = transcribe.start_transcription_job(
            TranscriptionJobName=name,
            LanguageCode="en-US",
            MediaFormat="wav",
            Media={"MediaFileUri": "s3://my-bucket/audio.wav"},
        )
        try:
            assert resp["TranscriptionJob"]["MediaFormat"] == "wav"
        finally:
            transcribe.delete_transcription_job(TranscriptionJobName=name)

    def test_start_job_with_sample_rate(self, transcribe):
        name = f"rate-job-{_uid()}"
        resp = transcribe.start_transcription_job(
            TranscriptionJobName=name,
            LanguageCode="en-US",
            MediaSampleRateHertz=16000,
            Media={"MediaFileUri": "s3://my-bucket/audio.wav"},
        )
        try:
            job = resp["TranscriptionJob"]
            assert job["TranscriptionJobName"] == name
        finally:
            transcribe.delete_transcription_job(TranscriptionJobName=name)

    def test_get_job_has_creation_time(self, transcribe):
        name = f"time-job-{_uid()}"
        transcribe.start_transcription_job(
            TranscriptionJobName=name,
            LanguageCode="en-US",
            Media={"MediaFileUri": "s3://my-bucket/audio.wav"},
        )
        try:
            resp = transcribe.get_transcription_job(TranscriptionJobName=name)
            assert "CreationTime" in resp["TranscriptionJob"]
        finally:
            transcribe.delete_transcription_job(TranscriptionJobName=name)

    def test_get_job_has_status(self, transcribe):
        name = f"stat-job-{_uid()}"
        transcribe.start_transcription_job(
            TranscriptionJobName=name,
            LanguageCode="en-US",
            Media={"MediaFileUri": "s3://my-bucket/audio.wav"},
        )
        try:
            resp = transcribe.get_transcription_job(TranscriptionJobName=name)
            assert resp["TranscriptionJob"]["TranscriptionJobStatus"] in (
                "QUEUED",
                "IN_PROGRESS",
                "COMPLETED",
                "FAILED",
            )
        finally:
            transcribe.delete_transcription_job(TranscriptionJobName=name)

    def test_create_vocabulary_with_phrases(self, transcribe):
        name = f"vocab-phrases-{_uid()}"
        resp = transcribe.create_vocabulary(
            VocabularyName=name,
            LanguageCode="en-US",
            Phrases=["hello", "world", "custom-word"],
        )
        try:
            assert resp["VocabularyName"] == name
            assert resp["LanguageCode"] == "en-US"
        finally:
            transcribe.delete_vocabulary(VocabularyName=name)

    def test_get_vocabulary(self, transcribe):
        name = f"get-vocab-{_uid()}"
        transcribe.create_vocabulary(
            VocabularyName=name,
            LanguageCode="en-US",
            Phrases=["test"],
        )
        try:
            resp = transcribe.get_vocabulary(VocabularyName=name)
            assert resp["VocabularyName"] == name
            assert resp["LanguageCode"] == "en-US"
            assert "VocabularyState" in resp
        finally:
            transcribe.delete_vocabulary(VocabularyName=name)

    def test_list_vocabularies_filtered(self, transcribe):
        name = f"filt-vocab-{_uid()}"
        transcribe.create_vocabulary(
            VocabularyName=name,
            LanguageCode="en-US",
            Phrases=["filter-test"],
        )
        try:
            resp = transcribe.list_vocabularies(NameContains=name[:10])
            names = [v["VocabularyName"] for v in resp.get("Vocabularies", [])]
            assert name in names
        finally:
            transcribe.delete_vocabulary(VocabularyName=name)

    def test_list_transcription_jobs_name_contains(self, transcribe):
        name = f"contains-job-{_uid()}"
        transcribe.start_transcription_job(
            TranscriptionJobName=name,
            LanguageCode="en-US",
            Media={"MediaFileUri": "s3://my-bucket/audio.wav"},
        )
        try:
            resp = transcribe.list_transcription_jobs(JobNameContains="contains-job")
            names = [j["TranscriptionJobName"] for j in resp["TranscriptionJobSummaries"]]
            assert name in names
        finally:
            transcribe.delete_transcription_job(TranscriptionJobName=name)

    def test_start_multiple_jobs(self, transcribe):
        names = [f"multi-{_uid()}" for _ in range(3)]
        try:
            for n in names:
                transcribe.start_transcription_job(
                    TranscriptionJobName=n,
                    LanguageCode="en-US",
                    Media={"MediaFileUri": "s3://my-bucket/audio.wav"},
                )
            resp = transcribe.list_transcription_jobs()
            found = [j["TranscriptionJobName"] for j in resp["TranscriptionJobSummaries"]]
            for n in names:
                assert n in found
        finally:
            for n in names:
                transcribe.delete_transcription_job(TranscriptionJobName=n)


class TestTranscribeMedicalJobs:
    """Tests for medical transcription job operations."""

    @pytest.fixture
    def transcribe(self):
        return make_client("transcribe")

    def test_start_medical_transcription_job(self, transcribe):
        name = f"med-start-{_uid()}"
        resp = transcribe.start_medical_transcription_job(
            MedicalTranscriptionJobName=name,
            LanguageCode="en-US",
            Media={"MediaFileUri": "s3://my-bucket/audio.wav"},
            OutputBucketName="my-output-bucket",
            Specialty="PRIMARYCARE",
            Type="CONVERSATION",
        )
        try:
            job = resp["MedicalTranscriptionJob"]
            assert job["MedicalTranscriptionJobName"] == name
            assert job["LanguageCode"] == "en-US"
        finally:
            transcribe.delete_medical_transcription_job(MedicalTranscriptionJobName=name)

    def test_get_medical_transcription_job(self, transcribe):
        name = f"med-get-{_uid()}"
        transcribe.start_medical_transcription_job(
            MedicalTranscriptionJobName=name,
            LanguageCode="en-US",
            Media={"MediaFileUri": "s3://my-bucket/audio.wav"},
            OutputBucketName="my-output-bucket",
            Specialty="PRIMARYCARE",
            Type="CONVERSATION",
        )
        try:
            resp = transcribe.get_medical_transcription_job(MedicalTranscriptionJobName=name)
            job = resp["MedicalTranscriptionJob"]
            assert job["MedicalTranscriptionJobName"] == name
            assert job["Specialty"] == "PRIMARYCARE"
        finally:
            transcribe.delete_medical_transcription_job(MedicalTranscriptionJobName=name)

    def test_list_medical_transcription_jobs(self, transcribe):
        name = f"med-list-{_uid()}"
        transcribe.start_medical_transcription_job(
            MedicalTranscriptionJobName=name,
            LanguageCode="en-US",
            Media={"MediaFileUri": "s3://my-bucket/audio.wav"},
            OutputBucketName="my-output-bucket",
            Specialty="PRIMARYCARE",
            Type="CONVERSATION",
        )
        try:
            resp = transcribe.list_medical_transcription_jobs()
            job_names = [
                j["MedicalTranscriptionJobName"]
                for j in resp.get("MedicalTranscriptionJobSummaries", [])
            ]
            assert name in job_names
        finally:
            transcribe.delete_medical_transcription_job(MedicalTranscriptionJobName=name)

    def test_delete_medical_transcription_job(self, transcribe):
        name = f"med-del-{_uid()}"
        transcribe.start_medical_transcription_job(
            MedicalTranscriptionJobName=name,
            LanguageCode="en-US",
            Media={"MediaFileUri": "s3://my-bucket/audio.wav"},
            OutputBucketName="my-output-bucket",
            Specialty="PRIMARYCARE",
            Type="CONVERSATION",
        )
        resp = transcribe.delete_medical_transcription_job(MedicalTranscriptionJobName=name)
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200


class TestTranscribeMedicalVocabularies:
    """Tests for medical vocabulary operations."""

    @pytest.fixture
    def transcribe(self):
        return make_client("transcribe")

    def test_create_medical_vocabulary(self, transcribe):
        name = f"med-vocab-{_uid()}"
        resp = transcribe.create_medical_vocabulary(
            VocabularyName=name,
            LanguageCode="en-US",
            VocabularyFileUri="s3://my-bucket/vocab.txt",
        )
        try:
            assert resp["VocabularyName"] == name
            assert resp["LanguageCode"] == "en-US"
        finally:
            transcribe.delete_medical_vocabulary(VocabularyName=name)

    def test_get_medical_vocabulary(self, transcribe):
        name = f"med-vocab-get-{_uid()}"
        transcribe.create_medical_vocabulary(
            VocabularyName=name,
            LanguageCode="en-US",
            VocabularyFileUri="s3://my-bucket/vocab.txt",
        )
        try:
            resp = transcribe.get_medical_vocabulary(VocabularyName=name)
            assert resp["VocabularyName"] == name
            assert resp["LanguageCode"] == "en-US"
            assert "VocabularyState" in resp
        finally:
            transcribe.delete_medical_vocabulary(VocabularyName=name)

    def test_list_medical_vocabularies(self, transcribe):
        name = f"med-vocab-list-{_uid()}"
        transcribe.create_medical_vocabulary(
            VocabularyName=name,
            LanguageCode="en-US",
            VocabularyFileUri="s3://my-bucket/vocab.txt",
        )
        try:
            resp = transcribe.list_medical_vocabularies()
            vocab_names = [v["VocabularyName"] for v in resp.get("Vocabularies", [])]
            assert name in vocab_names
        finally:
            transcribe.delete_medical_vocabulary(VocabularyName=name)

    def test_delete_medical_vocabulary(self, transcribe):
        name = f"med-vocab-del-{_uid()}"
        transcribe.create_medical_vocabulary(
            VocabularyName=name,
            LanguageCode="en-US",
            VocabularyFileUri="s3://my-bucket/vocab.txt",
        )
        resp = transcribe.delete_medical_vocabulary(VocabularyName=name)
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200


class TestTranscribeGapStubs:
    """Tests for gap operations: call analytics, language models, medical jobs/vocabs."""

    @pytest.fixture
    def transcribe(self):
        return make_client("transcribe")

    def test_list_call_analytics_categories(self, transcribe):
        resp = transcribe.list_call_analytics_categories()
        assert "Categories" in resp

    def test_list_call_analytics_jobs(self, transcribe):
        resp = transcribe.list_call_analytics_jobs()
        assert "CallAnalyticsJobSummaries" in resp

    def test_list_language_models(self, transcribe):
        resp = transcribe.list_language_models()
        assert "Models" in resp

    def test_list_medical_transcription_jobs(self, transcribe):
        resp = transcribe.list_medical_transcription_jobs()
        assert "MedicalTranscriptionJobSummaries" in resp

    def test_list_medical_vocabularies(self, transcribe):
        resp = transcribe.list_medical_vocabularies()
        assert "Vocabularies" in resp
