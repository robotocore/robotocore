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


class TestTranscribeErrorHandling:
    """Tests for error handling on nonexistent resources."""

    @pytest.fixture
    def transcribe(self):
        return make_client("transcribe")

    def test_get_transcription_job_nonexistent(self, transcribe):
        """GetTranscriptionJob for nonexistent job raises BadRequestException."""
        from botocore.exceptions import ClientError

        with pytest.raises(ClientError) as exc:
            transcribe.get_transcription_job(TranscriptionJobName="nonexistent-job-xyz")
        assert exc.value.response["Error"]["Code"] == "BadRequestException"

    def test_get_vocabulary_nonexistent(self, transcribe):
        """GetVocabulary for nonexistent vocabulary raises BadRequestException."""
        from botocore.exceptions import ClientError

        with pytest.raises(ClientError) as exc:
            transcribe.get_vocabulary(VocabularyName="nonexistent-vocab-xyz")
        assert exc.value.response["Error"]["Code"] == "BadRequestException"

    def test_delete_vocabulary_nonexistent(self, transcribe):
        """DeleteVocabulary for nonexistent vocabulary raises BadRequestException."""
        from botocore.exceptions import ClientError

        with pytest.raises(ClientError) as exc:
            transcribe.delete_vocabulary(VocabularyName="nonexistent-vocab-xyz")
        assert exc.value.response["Error"]["Code"] == "BadRequestException"

    def test_get_medical_transcription_job_nonexistent(self, transcribe):
        """GetMedicalTranscriptionJob for nonexistent job raises BadRequestException."""
        from botocore.exceptions import ClientError

        with pytest.raises(ClientError) as exc:
            transcribe.get_medical_transcription_job(
                MedicalTranscriptionJobName="nonexistent-job-xyz"
            )
        assert exc.value.response["Error"]["Code"] == "BadRequestException"

    def test_get_medical_vocabulary_nonexistent(self, transcribe):
        """GetMedicalVocabulary for nonexistent vocab raises BadRequestException."""
        from botocore.exceptions import ClientError

        with pytest.raises(ClientError) as exc:
            transcribe.get_medical_vocabulary(VocabularyName="nonexistent-vocab-xyz")
        assert exc.value.response["Error"]["Code"] == "BadRequestException"


class TestTranscribeAutoCoverage:
    """Auto-generated coverage tests for transcribe."""

    @pytest.fixture
    def client(self):
        return make_client("transcribe")

    def test_list_medical_scribe_jobs(self, client):
        """ListMedicalScribeJobs returns a response."""
        resp = client.list_medical_scribe_jobs()
        assert "MedicalScribeJobSummaries" in resp

    def test_list_vocabulary_filters(self, client):
        """ListVocabularyFilters returns a response."""
        resp = client.list_vocabulary_filters()
        assert "VocabularyFilters" in resp


class TestTranscribeVocabularyFilterCRUD:
    """Tests for VocabularyFilter CRUD operations."""

    @pytest.fixture
    def client(self):
        return make_client("transcribe")

    def test_create_vocabulary_filter(self, client):
        """CreateVocabularyFilter creates and returns a filter."""
        name = f"filter-{_uid()}"
        resp = client.create_vocabulary_filter(
            VocabularyFilterName=name,
            LanguageCode="en-US",
            Words=["bad", "words"],
        )
        assert resp["VocabularyFilterName"] == name
        assert resp["LanguageCode"] == "en-US"
        client.delete_vocabulary_filter(VocabularyFilterName=name)

    def test_get_vocabulary_filter(self, client):
        """GetVocabularyFilter returns filter details."""
        name = f"filter-{_uid()}"
        client.create_vocabulary_filter(
            VocabularyFilterName=name,
            LanguageCode="en-US",
            Words=["test"],
        )
        try:
            resp = client.get_vocabulary_filter(VocabularyFilterName=name)
            assert resp["VocabularyFilterName"] == name
            assert resp["LanguageCode"] == "en-US"
        finally:
            client.delete_vocabulary_filter(VocabularyFilterName=name)

    def test_list_vocabulary_filters_with_created(self, client):
        """ListVocabularyFilters includes a created filter."""
        name = f"filter-{_uid()}"
        client.create_vocabulary_filter(
            VocabularyFilterName=name,
            LanguageCode="en-US",
            Words=["test"],
        )
        try:
            resp = client.list_vocabulary_filters()
            names = [f["VocabularyFilterName"] for f in resp["VocabularyFilters"]]
            assert name in names
        finally:
            client.delete_vocabulary_filter(VocabularyFilterName=name)

    def test_update_vocabulary_filter(self, client):
        """UpdateVocabularyFilter updates the filter."""
        name = f"filter-{_uid()}"
        client.create_vocabulary_filter(
            VocabularyFilterName=name,
            LanguageCode="en-US",
            Words=["old"],
        )
        try:
            resp = client.update_vocabulary_filter(
                VocabularyFilterName=name,
                Words=["new", "words"],
            )
            assert resp["VocabularyFilterName"] == name
        finally:
            client.delete_vocabulary_filter(VocabularyFilterName=name)

    def test_delete_vocabulary_filter(self, client):
        """DeleteVocabularyFilter removes the filter."""
        name = f"filter-{_uid()}"
        client.create_vocabulary_filter(
            VocabularyFilterName=name,
            LanguageCode="en-US",
            Words=["test"],
        )
        client.delete_vocabulary_filter(VocabularyFilterName=name)
        resp = client.list_vocabulary_filters()
        names = [f["VocabularyFilterName"] for f in resp["VocabularyFilters"]]
        assert name not in names

    def test_get_vocabulary_filter_nonexistent(self, client):
        """GetVocabularyFilter for nonexistent raises BadRequestException."""
        from botocore.exceptions import ClientError

        with pytest.raises(ClientError) as exc:
            client.get_vocabulary_filter(VocabularyFilterName="no-such-filter")
        assert exc.value.response["Error"]["Code"] in (
            "BadRequestException",
            "NotFoundException",
        )


class TestTranscribeLanguageModelCRUD:
    """Tests for LanguageModel CRUD operations."""

    @pytest.fixture
    def client(self):
        return make_client("transcribe")

    def test_create_language_model(self, client):
        """CreateLanguageModel creates a model."""
        name = f"model-{_uid()}"
        resp = client.create_language_model(
            LanguageCode="en-US",
            BaseModelName="NarrowBand",
            ModelName=name,
            InputDataConfig={
                "S3Uri": "s3://bucket/data/",
                "DataAccessRoleArn": "arn:aws:iam::123456789012:role/test",
            },
        )
        assert resp["ModelName"] == name
        assert resp["LanguageCode"] == "en-US"
        client.delete_language_model(ModelName=name)

    def test_describe_language_model(self, client):
        """DescribeLanguageModel returns model details."""
        name = f"model-{_uid()}"
        client.create_language_model(
            LanguageCode="en-US",
            BaseModelName="NarrowBand",
            ModelName=name,
            InputDataConfig={
                "S3Uri": "s3://bucket/data/",
                "DataAccessRoleArn": "arn:aws:iam::123456789012:role/test",
            },
        )
        try:
            resp = client.describe_language_model(ModelName=name)
            model = resp["LanguageModel"]
            assert model["ModelName"] == name
            assert model["LanguageCode"] == "en-US"
        finally:
            client.delete_language_model(ModelName=name)

    def test_list_language_models(self, client):
        """ListLanguageModels includes created model."""
        name = f"model-{_uid()}"
        client.create_language_model(
            LanguageCode="en-US",
            BaseModelName="NarrowBand",
            ModelName=name,
            InputDataConfig={
                "S3Uri": "s3://bucket/data/",
                "DataAccessRoleArn": "arn:aws:iam::123456789012:role/test",
            },
        )
        try:
            resp = client.list_language_models()
            names = [m["ModelName"] for m in resp.get("Models", [])]
            assert name in names
        finally:
            client.delete_language_model(ModelName=name)

    def test_delete_language_model(self, client):
        """DeleteLanguageModel removes the model."""
        name = f"model-{_uid()}"
        client.create_language_model(
            LanguageCode="en-US",
            BaseModelName="NarrowBand",
            ModelName=name,
            InputDataConfig={
                "S3Uri": "s3://bucket/data/",
                "DataAccessRoleArn": "arn:aws:iam::123456789012:role/test",
            },
        )
        client.delete_language_model(ModelName=name)
        resp = client.list_language_models()
        names = [m["ModelName"] for m in resp.get("Models", [])]
        assert name not in names

    def test_describe_language_model_nonexistent(self, client):
        """DescribeLanguageModel for nonexistent raises error."""
        from botocore.exceptions import ClientError

        with pytest.raises(ClientError) as exc:
            client.describe_language_model(ModelName="no-such-model")
        assert exc.value.response["Error"]["Code"] in (
            "BadRequestException",
            "NotFoundException",
        )


class TestTranscribeDeepCoverage:
    """Deeper CRUD tests that verify cross-operation consistency."""

    @pytest.fixture
    def client(self):
        return make_client("transcribe")

    def test_start_transcription_job_fields(self, client):
        """StartTranscriptionJob returns all expected fields."""
        name = f"deep-job-{_uid()}"
        resp = client.start_transcription_job(
            TranscriptionJobName=name,
            LanguageCode="en-US",
            MediaFormat="wav",
            MediaSampleRateHertz=16000,
            Media={"MediaFileUri": "s3://my-bucket/deep-audio.wav"},
            OutputBucketName="my-output-bucket",
        )
        try:
            job = resp["TranscriptionJob"]
            assert job["TranscriptionJobName"] == name
            assert job["LanguageCode"] == "en-US"
            assert job["MediaFormat"] == "wav"
            assert "CreationTime" in job
        finally:
            client.delete_transcription_job(TranscriptionJobName=name)

    def test_get_transcription_job_media_uri(self, client):
        """GetTranscriptionJob returns the Media URI that was submitted."""
        name = f"media-uri-{_uid()}"
        client.start_transcription_job(
            TranscriptionJobName=name,
            LanguageCode="en-US",
            Media={"MediaFileUri": "s3://my-bucket/specific-file.wav"},
        )
        try:
            resp = client.get_transcription_job(TranscriptionJobName=name)
            job = resp["TranscriptionJob"]
            assert job["Media"]["MediaFileUri"] == "s3://my-bucket/specific-file.wav"
        finally:
            client.delete_transcription_job(TranscriptionJobName=name)

    def test_delete_transcription_job_removes_from_list(self, client):
        """After deletion, job no longer appears in list."""
        name = f"del-verify-{_uid()}"
        client.start_transcription_job(
            TranscriptionJobName=name,
            LanguageCode="en-US",
            Media={"MediaFileUri": "s3://my-bucket/audio.wav"},
        )
        client.delete_transcription_job(TranscriptionJobName=name)
        resp = client.list_transcription_jobs()
        names = [j["TranscriptionJobName"] for j in resp["TranscriptionJobSummaries"]]
        assert name not in names

    def test_medical_transcription_job_full_lifecycle(self, client):
        """Medical job: create, get fields, list, delete, verify gone."""
        name = f"med-life-{_uid()}"
        client.start_medical_transcription_job(
            MedicalTranscriptionJobName=name,
            LanguageCode="en-US",
            Media={"MediaFileUri": "s3://my-bucket/medical.wav"},
            OutputBucketName="my-output-bucket",
            Specialty="PRIMARYCARE",
            Type="CONVERSATION",
        )
        try:
            # Get and verify fields
            resp = client.get_medical_transcription_job(MedicalTranscriptionJobName=name)
            job = resp["MedicalTranscriptionJob"]
            assert job["MedicalTranscriptionJobName"] == name
            assert job["Specialty"] == "PRIMARYCARE"
            assert job["Type"] == "CONVERSATION"
            assert "CreationTime" in job

            # Verify in list
            list_resp = client.list_medical_transcription_jobs()
            job_names = [
                j["MedicalTranscriptionJobName"]
                for j in list_resp["MedicalTranscriptionJobSummaries"]
            ]
            assert name in job_names
        finally:
            client.delete_medical_transcription_job(MedicalTranscriptionJobName=name)

    def test_vocabulary_get_has_state(self, client):
        """GetVocabulary returns VocabularyState."""
        name = f"state-vocab-{_uid()}"
        client.create_vocabulary(
            VocabularyName=name,
            LanguageCode="en-US",
            Phrases=["alpha", "bravo"],
        )
        try:
            get_resp = client.get_vocabulary(VocabularyName=name)
            assert get_resp["VocabularyName"] == name
            assert get_resp["VocabularyState"] in ("PENDING", "READY", "FAILED")
        finally:
            client.delete_vocabulary(VocabularyName=name)

    def test_vocabulary_delete_removes_from_list(self, client):
        """Deleted vocabulary no longer appears in list."""
        name = f"delvocab-{_uid()}"
        client.create_vocabulary(
            VocabularyName=name,
            LanguageCode="en-US",
            Phrases=["test"],
        )
        client.delete_vocabulary(VocabularyName=name)
        resp = client.list_vocabularies()
        names = [v["VocabularyName"] for v in resp.get("Vocabularies", [])]
        assert name not in names

    def test_medical_vocabulary_get_has_fields(self, client):
        """GetMedicalVocabulary returns expected fields."""
        name = f"medvocab-deep-{_uid()}"
        client.create_medical_vocabulary(
            VocabularyName=name,
            LanguageCode="en-US",
            VocabularyFileUri="s3://my-bucket/med-vocab.txt",
        )
        try:
            get_resp = client.get_medical_vocabulary(VocabularyName=name)
            assert get_resp["VocabularyName"] == name
            assert get_resp["LanguageCode"] == "en-US"
            assert "VocabularyState" in get_resp
        finally:
            client.delete_medical_vocabulary(VocabularyName=name)

    def test_medical_vocabulary_delete_removes_from_list(self, client):
        """Deleted medical vocabulary no longer appears in list."""
        name = f"medvdel-{_uid()}"
        client.create_medical_vocabulary(
            VocabularyName=name,
            LanguageCode="en-US",
            VocabularyFileUri="s3://my-bucket/vocab.txt",
        )
        client.delete_medical_vocabulary(VocabularyName=name)
        resp = client.list_medical_vocabularies()
        names = [v["VocabularyName"] for v in resp.get("Vocabularies", [])]
        assert name not in names

    def test_vocabulary_filter_update_and_verify(self, client):
        """Update vocabulary filter and verify changes via get."""
        name = f"upd-filter-{_uid()}"
        client.create_vocabulary_filter(
            VocabularyFilterName=name,
            LanguageCode="en-US",
            Words=["original"],
        )
        try:
            upd = client.update_vocabulary_filter(
                VocabularyFilterName=name,
                Words=["updated", "words"],
            )
            assert upd["VocabularyFilterName"] == name
            assert upd["LanguageCode"] == "en-US"

            get_resp = client.get_vocabulary_filter(VocabularyFilterName=name)
            assert get_resp["VocabularyFilterName"] == name
        finally:
            client.delete_vocabulary_filter(VocabularyFilterName=name)

    def test_language_model_full_lifecycle(self, client):
        """Language model: create, describe, list, delete, verify gone."""
        name = f"lm-life-{_uid()}"
        client.create_language_model(
            LanguageCode="en-US",
            BaseModelName="NarrowBand",
            ModelName=name,
            InputDataConfig={
                "S3Uri": "s3://bucket/data/",
                "DataAccessRoleArn": "arn:aws:iam::123456789012:role/test",
            },
        )
        try:
            desc = client.describe_language_model(ModelName=name)
            model = desc["LanguageModel"]
            assert model["ModelName"] == name
            assert model["LanguageCode"] == "en-US"
            assert model["BaseModelName"] == "NarrowBand"
            assert "ModelStatus" in model

            list_resp = client.list_language_models()
            names = [m["ModelName"] for m in list_resp.get("Models", [])]
            assert name in names
        finally:
            client.delete_language_model(ModelName=name)

        # Verify deletion
        list_after = client.list_language_models()
        names_after = [m["ModelName"] for m in list_after.get("Models", [])]
        assert name not in names_after

    def test_delete_transcription_job_nonexistent(self, client):
        """DeleteTranscriptionJob for nonexistent job raises BadRequestException."""
        from botocore.exceptions import ClientError

        with pytest.raises(ClientError) as exc:
            client.delete_transcription_job(TranscriptionJobName="nonexistent-xyz-job")
        assert exc.value.response["Error"]["Code"] == "BadRequestException"

    def test_delete_medical_transcription_job_nonexistent(self, client):
        """DeleteMedicalTranscriptionJob for nonexistent raises BadRequestException."""
        from botocore.exceptions import ClientError

        with pytest.raises(ClientError) as exc:
            client.delete_medical_transcription_job(
                MedicalTranscriptionJobName="nonexistent-med-xyz"
            )
        assert exc.value.response["Error"]["Code"] == "BadRequestException"

    def test_delete_language_model_nonexistent(self, client):
        """DeleteLanguageModel for nonexistent raises error."""
        from botocore.exceptions import ClientError

        with pytest.raises(ClientError) as exc:
            client.delete_language_model(ModelName="nonexistent-model-xyz")
        assert exc.value.response["Error"]["Code"] in (
            "BadRequestException",
            "NotFoundException",
        )

    def test_delete_vocabulary_filter_nonexistent(self, client):
        """DeleteVocabularyFilter for nonexistent raises error."""
        from botocore.exceptions import ClientError

        with pytest.raises(ClientError) as exc:
            client.delete_vocabulary_filter(VocabularyFilterName="nonexistent-filter-xyz")
        assert exc.value.response["Error"]["Code"] in (
            "BadRequestException",
            "NotFoundException",
        )

    def test_delete_medical_vocabulary_nonexistent(self, client):
        """DeleteMedicalVocabulary for nonexistent raises BadRequestException."""
        from botocore.exceptions import ClientError

        with pytest.raises(ClientError) as exc:
            client.delete_medical_vocabulary(VocabularyName="nonexistent-medvocab-xyz")
        assert exc.value.response["Error"]["Code"] == "BadRequestException"
