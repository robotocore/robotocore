"""Compatibility tests for Amazon Polly."""

import uuid

import pytest

from tests.compatibility.conftest import make_client


@pytest.fixture
def polly_client():
    return make_client("polly")


@pytest.fixture
def s3_bucket():
    """Create an S3 bucket for Polly output, yield its name, then delete it."""
    s3 = make_client("s3")
    bucket_name = f"polly-compat-{uuid.uuid4().hex[:12]}"
    s3.create_bucket(Bucket=bucket_name)
    yield bucket_name
    try:
        # Delete all objects first
        objects = s3.list_objects_v2(Bucket=bucket_name).get("Contents", [])
        for obj in objects:
            s3.delete_object(Bucket=bucket_name, Key=obj["Key"])
        s3.delete_bucket(Bucket=bucket_name)
    except Exception as exc:
        import logging

        logging.debug("s3_bucket cleanup failed: %s", exc)


@pytest.fixture
def lexicon_name(polly_client):
    """Create a lexicon, yield its name, then delete it."""
    name = f"compat{uuid.uuid4().hex[:8]}"
    polly_client.put_lexicon(
        Name=name,
        Content=(
            '<?xml version="1.0" encoding="UTF-8"?>'
            '<lexicon version="1.0"'
            ' xmlns="http://www.w3.org/2005/01/pronunciation-lexicon"'
            ' xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"'
            ' xsi:schemaLocation="http://www.w3.org/2005/01/pronunciation-lexicon'
            ' http://www.w3.org/2005/01/pronunciation-lexicon"'
            ' alphabet="ipa" xml:lang="en-US">'
            "<lexeme><grapheme>W3C</grapheme>"
            "<alias>World Wide Web Consortium</alias></lexeme>"
            "</lexicon>"
        ),
    )
    yield name
    try:
        polly_client.delete_lexicon(Name=name)
    except Exception:
        pass  # best-effort cleanup


class TestDescribeVoices:
    def test_returns_voices_list(self, polly_client):
        resp = polly_client.describe_voices()
        voices = resp["Voices"]
        assert isinstance(voices, list)
        assert len(voices) > 0

    def test_voice_has_expected_fields(self, polly_client):
        resp = polly_client.describe_voices()
        voice = resp["Voices"][0]
        assert voice["Gender"] in ("Male", "Female")
        assert len(voice["Id"]) > 0
        assert len(voice["Name"]) > 0
        assert "-" in voice["LanguageCode"]  # e.g. "en-US"
        assert len(voice["LanguageName"]) > 0

    def test_voice_has_supported_engines_field(self, polly_client):
        resp = polly_client.describe_voices()
        voice = resp["Voices"][0]
        assert "SupportedEngines" in voice
        assert isinstance(voice["SupportedEngines"], list)
        assert len(voice["SupportedEngines"]) > 0

    def test_filter_by_language(self, polly_client):
        resp = polly_client.describe_voices(LanguageCode="en-US")
        voices = resp["Voices"]
        assert len(voices) > 0
        for v in voices:
            assert v["LanguageCode"] == "en-US"

    def test_filter_by_engine_standard(self, polly_client):
        resp = polly_client.describe_voices(Engine="standard")
        voices = resp["Voices"]
        assert len(voices) > 0
        for v in voices:
            assert "standard" in v["SupportedEngines"]

    def test_filter_by_engine_neural(self, polly_client):
        resp = polly_client.describe_voices(Engine="neural")
        voices = resp["Voices"]
        assert len(voices) > 0
        for v in voices:
            assert "neural" in v["SupportedEngines"]

    def test_voice_ids_are_unique(self, polly_client):
        resp = polly_client.describe_voices()
        voice_ids = [v["Id"] for v in resp["Voices"]]
        assert len(voice_ids) == len(set(voice_ids))

    def test_filter_returns_subset_of_all(self, polly_client):
        all_voices = polly_client.describe_voices()["Voices"]
        en_us_voices = polly_client.describe_voices(LanguageCode="en-US")["Voices"]
        assert len(en_us_voices) < len(all_voices)


class TestSynthesizeSpeech:
    def test_mp3_output(self, polly_client):
        resp = polly_client.synthesize_speech(
            OutputFormat="mp3",
            Text="Hello world",
            VoiceId="Joanna",
        )
        assert resp["ContentType"] == "audio/mpeg"
        data = resp["AudioStream"].read()
        assert len(data) > 0

    def test_different_voice(self, polly_client):
        resp = polly_client.synthesize_speech(
            OutputFormat="mp3",
            Text="Testing",
            VoiceId="Matthew",
        )
        assert resp["ContentType"] == "audio/mpeg"
        data = resp["AudioStream"].read()
        assert len(data) > 0

    def test_ogg_vorbis_output(self, polly_client):
        resp = polly_client.synthesize_speech(
            OutputFormat="ogg_vorbis",
            Text="Hello world",
            VoiceId="Joanna",
        )
        assert resp["ContentType"] == "audio/ogg"
        data = resp["AudioStream"].read()
        assert len(data) > 0

    def test_pcm_output(self, polly_client):
        resp = polly_client.synthesize_speech(
            OutputFormat="pcm",
            Text="Hello world",
            VoiceId="Joanna",
        )
        assert resp["ContentType"] == "audio/pcm"
        data = resp["AudioStream"].read()
        assert len(data) > 0

    def test_ssml_text_type(self, polly_client):
        resp = polly_client.synthesize_speech(
            OutputFormat="mp3",
            Text="<speak>Hello <break time='300ms'/> world</speak>",
            TextType="ssml",
            VoiceId="Joanna",
        )
        assert resp["ContentType"] == "audio/mpeg"
        data = resp["AudioStream"].read()
        assert len(data) > 0

    def test_response_includes_request_characters(self, polly_client):
        resp = polly_client.synthesize_speech(
            OutputFormat="mp3",
            Text="Hello world",
            VoiceId="Joanna",
        )
        assert "RequestCharacters" in resp
        assert resp["RequestCharacters"] == 11

    def test_neural_engine(self, polly_client):
        resp = polly_client.synthesize_speech(
            Engine="neural",
            OutputFormat="mp3",
            Text="Hello neural",
            VoiceId="Joanna",
        )
        assert resp["ContentType"] == "audio/mpeg"
        data = resp["AudioStream"].read()
        assert len(data) > 0


SAMPLE_LEXICON_CONTENT = (
    '<?xml version="1.0" encoding="UTF-8"?>'
    '<lexicon version="1.0"'
    ' xmlns="http://www.w3.org/2005/01/pronunciation-lexicon"'
    ' xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"'
    ' xsi:schemaLocation="http://www.w3.org/2005/01/pronunciation-lexicon'
    ' http://www.w3.org/2005/01/pronunciation-lexicon"'
    ' alphabet="ipa" xml:lang="en-US">'
    "<lexeme><grapheme>AWS</grapheme>"
    "<alias>Amazon Web Services</alias></lexeme>"
    "</lexicon>"
)


class TestLexicons:
    def test_put_and_list_lexicon(self, polly_client, lexicon_name):
        resp = polly_client.list_lexicons()
        names = [lex["Name"] for lex in resp["Lexicons"]]
        assert lexicon_name in names

    def test_get_lexicon(self, polly_client, lexicon_name):
        resp = polly_client.get_lexicon(Name=lexicon_name)
        assert resp["Lexicon"]["Name"] == lexicon_name
        assert "Content" in resp["Lexicon"]
        assert "LexiconAttributes" in resp

    def test_lexicon_attributes_fields(self, polly_client, lexicon_name):
        resp = polly_client.get_lexicon(Name=lexicon_name)
        attrs = resp["LexiconAttributes"]
        assert attrs["Alphabet"] == "ipa"
        assert attrs["LanguageCode"] == "en-US"
        assert attrs["LexemesCount"] == 1
        assert attrs["Size"] > 0
        assert attrs["LexiconArn"].startswith("arn:")

    def test_lexicon_arn_format(self, polly_client, lexicon_name):
        resp = polly_client.get_lexicon(Name=lexicon_name)
        arn = resp["LexiconAttributes"]["LexiconArn"]
        assert arn.startswith("arn:aws:polly:")
        assert lexicon_name in arn

    def test_delete_lexicon(self, polly_client):
        name = f"compat{uuid.uuid4().hex[:8]}"
        polly_client.put_lexicon(
            Name=name,
            Content=(
                '<?xml version="1.0" encoding="UTF-8"?>'
                '<lexicon version="1.0"'
                ' xmlns="http://www.w3.org/2005/01/pronunciation-lexicon"'
                ' alphabet="ipa" xml:lang="en-US">'
                "<lexeme><grapheme>test</grapheme>"
                "<alias>testing</alias></lexeme>"
                "</lexicon>"
            ),
        )
        polly_client.delete_lexicon(Name=name)
        resp = polly_client.list_lexicons()
        names = [lex["Name"] for lex in resp["Lexicons"]]
        assert name not in names

    def test_list_lexicons_empty(self, polly_client):
        resp = polly_client.list_lexicons()
        assert "Lexicons" in resp
        assert isinstance(resp["Lexicons"], list)

    def test_put_lexicon_idempotent(self, polly_client):
        """Putting the same lexicon twice succeeds (update semantics)."""
        name = f"compat{uuid.uuid4().hex[:8]}"
        try:
            polly_client.put_lexicon(Name=name, Content=SAMPLE_LEXICON_CONTENT)
            polly_client.put_lexicon(Name=name, Content=SAMPLE_LEXICON_CONTENT)
            resp = polly_client.get_lexicon(Name=name)
            assert resp["Lexicon"]["Name"] == name
        finally:
            try:
                polly_client.delete_lexicon(Name=name)
            except Exception as exc:
                import logging
                logging.debug("lexicon cleanup failed: %s", exc)

    def test_list_multiple_lexicons(self, polly_client):
        """All created lexicons appear in the list result."""
        names = []
        try:
            for _ in range(3):
                name = f"compat{uuid.uuid4().hex[:8]}"
                polly_client.put_lexicon(Name=name, Content=SAMPLE_LEXICON_CONTENT)
                names.append(name)
            resp = polly_client.list_lexicons()
            listed = [lex["Name"] for lex in resp["Lexicons"]]
            for name in names:
                assert name in listed
        finally:
            for name in names:
                try:
                    polly_client.delete_lexicon(Name=name)
                except Exception as exc:
                    import logging
                    logging.debug("lexicon cleanup failed: %s", exc)

    def test_synthesize_speech_with_lexicon(self, polly_client, lexicon_name):
        """SynthesizeSpeech accepts LexiconNames parameter."""
        resp = polly_client.synthesize_speech(
            OutputFormat="mp3",
            Text="Hello AWS",
            VoiceId="Joanna",
            LexiconNames=[lexicon_name],
        )
        assert resp["ContentType"] == "audio/mpeg"
        data = resp["AudioStream"].read()
        assert len(data) > 0


class TestSpeechSynthesisTasks:
    """Tests for async speech synthesis task management."""

    def test_start_and_get_speech_synthesis_task(self, polly_client, s3_bucket):
        """StartSpeechSynthesisTask creates a task and GetSpeechSynthesisTask retrieves it."""
        resp = polly_client.start_speech_synthesis_task(
            Engine="standard",
            OutputFormat="mp3",
            OutputS3BucketName=s3_bucket,
            Text="Hello world",
            VoiceId="Joanna",
        )
        task = resp["SynthesisTask"]
        assert "TaskId" in task
        assert task["TaskStatus"] in ("scheduled", "inProgress", "completed", "failed")
        assert "OutputUri" in task
        assert s3_bucket in task["OutputUri"]

        task_id = task["TaskId"]
        get_resp = polly_client.get_speech_synthesis_task(TaskId=task_id)
        get_task = get_resp["SynthesisTask"]
        assert get_task["TaskId"] == task_id
        assert get_task["TaskStatus"] in ("scheduled", "inProgress", "completed", "failed")
        assert get_task["VoiceId"] == "Joanna"
        assert get_task["OutputFormat"] == "mp3"

    def test_task_output_uri_format(self, polly_client, s3_bucket):
        """OutputUri should be an HTTPS S3 URI containing the bucket name."""
        resp = polly_client.start_speech_synthesis_task(
            OutputFormat="mp3",
            OutputS3BucketName=s3_bucket,
            Text="Test output URI format",
            VoiceId="Joanna",
        )
        task = resp["SynthesisTask"]
        uri = task["OutputUri"]
        assert uri.startswith("https://")
        assert s3_bucket in uri
        assert uri.endswith(".mp3")

    def test_task_has_creation_time(self, polly_client, s3_bucket):
        """Task should include CreationTime timestamp."""
        import datetime

        resp = polly_client.start_speech_synthesis_task(
            OutputFormat="mp3",
            OutputS3BucketName=s3_bucket,
            Text="Test creation time",
            VoiceId="Joanna",
        )
        task = resp["SynthesisTask"]
        creation_time = task["CreationTime"]
        assert isinstance(creation_time, datetime.datetime)

    def test_task_fields_preserved_on_get(self, polly_client, s3_bucket):
        """Fields set at creation are readable via GetSpeechSynthesisTask."""
        resp = polly_client.start_speech_synthesis_task(
            OutputFormat="mp3",
            OutputS3BucketName=s3_bucket,
            Text="Test field preservation",
            VoiceId="Matthew",
            Engine="standard",
        )
        task_id = resp["SynthesisTask"]["TaskId"]
        get_resp = polly_client.get_speech_synthesis_task(TaskId=task_id)
        task = get_resp["SynthesisTask"]
        assert task["VoiceId"] == "Matthew"
        assert task["OutputFormat"] == "mp3"
        assert task["OutputUri"] != ""

    def test_list_speech_synthesis_tasks(self, polly_client, s3_bucket):
        """ListSpeechSynthesisTasks returns tasks that were started."""
        polly_client.start_speech_synthesis_task(
            OutputFormat="mp3",
            OutputS3BucketName=s3_bucket,
            Text="First task",
            VoiceId="Joanna",
        )
        polly_client.start_speech_synthesis_task(
            OutputFormat="mp3",
            OutputS3BucketName=s3_bucket,
            Text="Second task",
            VoiceId="Matthew",
        )

        resp = polly_client.list_speech_synthesis_tasks()
        assert "SynthesisTasks" in resp
        tasks = resp["SynthesisTasks"]
        assert len(tasks) >= 2
        for task in tasks:
            assert "TaskId" in task
            assert "TaskStatus" in task

    def test_list_tasks_max_results(self, polly_client, s3_bucket):
        """MaxResults limits the number of tasks returned."""
        for i in range(3):
            polly_client.start_speech_synthesis_task(
                OutputFormat="mp3",
                OutputS3BucketName=s3_bucket,
                Text=f"Task number {i}",
                VoiceId="Joanna",
            )
        resp = polly_client.list_speech_synthesis_tasks(MaxResults=2)
        assert "SynthesisTasks" in resp
        assert len(resp["SynthesisTasks"]) <= 2

    def test_get_speech_synthesis_task_not_found(self, polly_client):
        """GetSpeechSynthesisTask raises SynthesisTaskNotFoundException for unknown TaskId."""
        from botocore.exceptions import ClientError

        with pytest.raises(ClientError) as exc:
            polly_client.get_speech_synthesis_task(TaskId="nonexistent-task-id-xyz")
        assert exc.value.response["Error"]["Code"] == "SynthesisTaskNotFoundException"


class TestPollyErrors:
    """Tests for Polly error handling."""

    def test_get_lexicon_nonexistent(self, polly_client):
        """GetLexicon for nonexistent lexicon raises LexiconNotFoundException."""
        from botocore.exceptions import ClientError

        with pytest.raises(ClientError) as exc:
            polly_client.get_lexicon(Name="nonexistent-lexicon-xyz")
        assert exc.value.response["Error"]["Code"] == "LexiconNotFoundException"

    def test_delete_lexicon_nonexistent(self, polly_client):
        """DeleteLexicon for nonexistent lexicon raises LexiconNotFoundException."""
        from botocore.exceptions import ClientError

        with pytest.raises(ClientError) as exc:
            polly_client.delete_lexicon(Name="nonexistent-lexicon-xyz")
        assert exc.value.response["Error"]["Code"] == "LexiconNotFoundException"
