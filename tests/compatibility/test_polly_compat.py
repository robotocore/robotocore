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

    def test_ssml_request_characters_excludes_tags(self, polly_client):
        """SSML tags should not count toward RequestCharacters; only text content counts."""
        resp = polly_client.synthesize_speech(
            OutputFormat="mp3",
            Text="<speak>Hello world</speak>",
            TextType="ssml",
            VoiceId="Joanna",
        )
        # "Hello world" is 11 characters; SSML tags should not be counted
        assert resp["RequestCharacters"] == 11

    def test_response_metadata_present(self, polly_client):
        """SynthesizeSpeech response includes ResponseMetadata with HTTP status 200."""
        resp = polly_client.synthesize_speech(
            OutputFormat="mp3",
            Text="Hello",
            VoiceId="Joanna",
        )
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200

    def test_request_characters_scales_with_text_length(self, polly_client):
        """RequestCharacters reflects the length of the input text."""
        text = "The quick brown fox jumps over the lazy dog"
        resp = polly_client.synthesize_speech(
            OutputFormat="mp3",
            Text=text,
            VoiceId="Joanna",
        )
        assert resp["RequestCharacters"] == len(text)


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

    def test_list_lexicons_returns_list(self, polly_client):
        resp = polly_client.list_lexicons()
        assert isinstance(resp["Lexicons"], list)
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200

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

    def test_lexicon_content_preserved_after_update(self, polly_client):
        """Updating a lexicon stores the new content and it is readable via GetLexicon."""
        name = f"compat{uuid.uuid4().hex[:8]}"
        original_content = (
            '<?xml version="1.0" encoding="UTF-8"?>'
            '<lexicon version="1.0"'
            ' xmlns="http://www.w3.org/2005/01/pronunciation-lexicon"'
            ' alphabet="ipa" xml:lang="en-US">'
            "<lexeme><grapheme>AWS</grapheme>"
            "<alias>Amazon Web Services</alias></lexeme>"
            "</lexicon>"
        )
        updated_content = (
            '<?xml version="1.0" encoding="UTF-8"?>'
            '<lexicon version="1.0"'
            ' xmlns="http://www.w3.org/2005/01/pronunciation-lexicon"'
            ' alphabet="ipa" xml:lang="en-US">'
            "<lexeme><grapheme>CPU</grapheme>"
            "<alias>Central Processing Unit</alias></lexeme>"
            "</lexicon>"
        )
        try:
            polly_client.put_lexicon(Name=name, Content=original_content)
            polly_client.put_lexicon(Name=name, Content=updated_content)
            resp = polly_client.get_lexicon(Name=name)
            content = resp["Lexicon"]["Content"]
            assert "CPU" in content
            assert "AWS" not in content  # original content should be replaced
        finally:
            try:
                polly_client.delete_lexicon(Name=name)
            except Exception as exc:
                import logging
                logging.debug("lexicon cleanup failed: %s", exc)

    def test_lexicon_last_modified_time_present(self, polly_client, lexicon_name):
        """GetLexicon response includes LastModified timestamp in LexiconAttributes."""
        import datetime

        resp = polly_client.get_lexicon(Name=lexicon_name)
        attrs = resp["LexiconAttributes"]
        assert "LastModified" in attrs
        assert isinstance(attrs["LastModified"], datetime.datetime)

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

    def test_task_with_s3_key_prefix(self, polly_client, s3_bucket):
        """OutputUri includes the S3KeyPrefix when provided."""
        resp = polly_client.start_speech_synthesis_task(
            OutputFormat="mp3",
            OutputS3BucketName=s3_bucket,
            OutputS3KeyPrefix="audio/speech",
            Text="Testing key prefix",
            VoiceId="Joanna",
        )
        task = resp["SynthesisTask"]
        uri = task["OutputUri"]
        assert "audio/speech" in uri
        assert s3_bucket in uri

    def test_task_engine_field_preserved(self, polly_client, s3_bucket):
        """Engine field set at creation is present in the task response."""
        resp = polly_client.start_speech_synthesis_task(
            Engine="standard",
            OutputFormat="mp3",
            OutputS3BucketName=s3_bucket,
            Text="Engine preservation test",
            VoiceId="Joanna",
        )
        task = resp["SynthesisTask"]
        assert "Engine" in task
        assert task["Engine"] == "standard"

    def test_list_tasks_pagination_with_next_token(self, polly_client, s3_bucket):
        """ListSpeechSynthesisTasks returns NextToken when more results are available."""
        for i in range(4):
            polly_client.start_speech_synthesis_task(
                OutputFormat="mp3",
                OutputS3BucketName=s3_bucket,
                Text=f"Pagination task {i}",
                VoiceId="Joanna",
            )
        resp = polly_client.list_speech_synthesis_tasks(MaxResults=2)
        assert "SynthesisTasks" in resp
        assert len(resp["SynthesisTasks"]) <= 2
        if "NextToken" in resp:
            resp2 = polly_client.list_speech_synthesis_tasks(
                MaxResults=2, NextToken=resp["NextToken"]
            )
            assert "SynthesisTasks" in resp2
            assert len(resp2["SynthesisTasks"]) > 0


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

    def test_synthesize_speech_invalid_voice(self, polly_client):
        """SynthesizeSpeech with an invalid VoiceId raises an error."""
        from botocore.exceptions import ClientError

        with pytest.raises(ClientError) as exc:
            polly_client.synthesize_speech(
                OutputFormat="mp3",
                Text="Hello",
                VoiceId="NotARealVoice",
            )
        assert exc.value.response["Error"]["Code"] == "InvalidParameterValue"


class TestDescribeVoicesEdgeCases:
    def test_describe_voices_with_language_and_engine_combined(self, polly_client):
        """Filtering by both LanguageCode and Engine returns voices matching both criteria."""
        resp = polly_client.describe_voices(LanguageCode="en-US", Engine="neural")
        voices = resp["Voices"]
        assert len(voices) > 0
        for v in voices:
            assert v["LanguageCode"] == "en-US"
            assert "neural" in v["SupportedEngines"]

    def test_describe_voices_en_us_contains_joanna(self, polly_client):
        """Joanna is a known en-US voice and must appear in results."""
        resp = polly_client.describe_voices(LanguageCode="en-US")
        joanna_voices = [v for v in resp["Voices"] if v["Id"] == "Joanna"]
        assert len(joanna_voices) == 1
        assert joanna_voices[0]["LanguageCode"] == "en-US"

    def test_describe_voices_neural_and_standard_overlap(self, polly_client):
        """Some voices support both standard and neural engines."""
        neural = {v["Id"] for v in polly_client.describe_voices(Engine="neural")["Voices"]}
        standard = {v["Id"] for v in polly_client.describe_voices(Engine="standard")["Voices"]}
        # Joanna supports both
        assert len(neural & standard) > 0


class TestSynthesizeSpeechEdgeCases:
    def test_synthesize_unicode_text_request_characters(self, polly_client):
        """RequestCharacters counts Unicode characters (not bytes)."""
        text = "Héllo wörld"  # 11 chars, but >11 bytes in UTF-8
        resp = polly_client.synthesize_speech(
            OutputFormat="mp3",
            Text=text,
            VoiceId="Joanna",
        )
        assert resp["RequestCharacters"] == len(text)

    def test_synthesize_speech_with_nonexistent_lexicon_raises_error(self, polly_client):
        """SynthesizeSpeech with a nonexistent LexiconName raises LexiconNotFoundException."""
        from botocore.exceptions import ClientError

        with pytest.raises(ClientError) as exc:
            polly_client.synthesize_speech(
                OutputFormat="mp3",
                Text="Hello world",
                VoiceId="Joanna",
                LexiconNames=["nonexistent-lexicon-xyz"],
            )
        assert exc.value.response["Error"]["Code"] == "LexiconNotFoundException"

    def test_synthesize_speech_ssml_marks_require_ssml_text_type(self, polly_client):
        """ssml SpeechMarkType with plain text raises SsmlMarksNotSupportedForTextTypeException."""
        from botocore.exceptions import ClientError

        with pytest.raises(ClientError) as exc:
            polly_client.synthesize_speech(
                OutputFormat="json",
                Text="Hello world",
                VoiceId="Joanna",
                SpeechMarkTypes=["ssml"],
            )
        assert exc.value.response["Error"]["Code"] == "SsmlMarksNotSupportedForTextTypeException"

    def test_synthesize_speech_json_output_ssml_speech_marks(self, polly_client):
        """OutputFormat=json with ssml SpeechMarkTypes and ssml TextType returns json-stream."""
        resp = polly_client.synthesize_speech(
            OutputFormat="json",
            Text="<speak>Hello world</speak>",
            TextType="ssml",
            VoiceId="Joanna",
            SpeechMarkTypes=["ssml"],
        )
        assert resp["ContentType"] in ("application/x-json-stream", "audio/json")


class TestLexiconEdgeCases:
    def test_lexicon_full_lifecycle(self, polly_client):
        """Create, retrieve, update, and delete a lexicon in one test."""
        name = f"compat{uuid.uuid4().hex[:8]}"
        content1 = (
            '<?xml version="1.0" encoding="UTF-8"?>'
            '<lexicon version="1.0"'
            ' xmlns="http://www.w3.org/2005/01/pronunciation-lexicon"'
            ' alphabet="ipa" xml:lang="en-US">'
            "<lexeme><grapheme>SQS</grapheme>"
            "<alias>Simple Queue Service</alias></lexeme>"
            "</lexicon>"
        )
        content2 = (
            '<?xml version="1.0" encoding="UTF-8"?>'
            '<lexicon version="1.0"'
            ' xmlns="http://www.w3.org/2005/01/pronunciation-lexicon"'
            ' alphabet="ipa" xml:lang="en-US">'
            "<lexeme><grapheme>SNS</grapheme>"
            "<alias>Simple Notification Service</alias></lexeme>"
            "</lexicon>"
        )
        try:
            # CREATE
            polly_client.put_lexicon(Name=name, Content=content1)
            # RETRIEVE
            get = polly_client.get_lexicon(Name=name)
            assert "SQS" in get["Lexicon"]["Content"]
            # UPDATE
            polly_client.put_lexicon(Name=name, Content=content2)
            get2 = polly_client.get_lexicon(Name=name)
            assert "SNS" in get2["Lexicon"]["Content"]
            assert "SQS" not in get2["Lexicon"]["Content"]
            # DELETE
            polly_client.delete_lexicon(Name=name)
            # ERROR: get after delete
            from botocore.exceptions import ClientError
            with pytest.raises(ClientError) as exc:
                polly_client.get_lexicon(Name=name)
            assert exc.value.response["Error"]["Code"] == "LexiconNotFoundException"
        except Exception:
            try:
                polly_client.delete_lexicon(Name=name)
            except Exception as cleanup_exc:
                import logging
                logging.debug("lexicon cleanup failed: %s", cleanup_exc)
            raise

    def test_lexicon_arn_contains_account_and_region(self, polly_client):
        """LexiconArn follows arn:aws:polly:{region}:{account}:lexicon/{name} format."""
        name = f"compat{uuid.uuid4().hex[:8]}"
        content = (
            '<?xml version="1.0" encoding="UTF-8"?>'
            '<lexicon version="1.0"'
            ' xmlns="http://www.w3.org/2005/01/pronunciation-lexicon"'
            ' alphabet="ipa" xml:lang="en-US">'
            "<lexeme><grapheme>EC2</grapheme>"
            "<alias>Elastic Compute Cloud</alias></lexeme>"
            "</lexicon>"
        )
        try:
            polly_client.put_lexicon(Name=name, Content=content)
            resp = polly_client.get_lexicon(Name=name)
            arn = resp["LexiconAttributes"]["LexiconArn"]
            parts = arn.split(":")
            assert parts[0] == "arn"
            assert parts[1] == "aws"
            assert parts[2] == "polly"
            assert len(parts[3]) > 0  # region
            assert len(parts[4]) > 0  # account
            assert name in arn
        finally:
            try:
                polly_client.delete_lexicon(Name=name)
            except Exception as exc:
                import logging
                logging.debug("lexicon cleanup: %s", exc)

    def test_lexicon_size_attribute_reflects_content_length(self, polly_client):
        """LexiconAttributes.Size matches the actual byte length of the content."""
        name = f"compat{uuid.uuid4().hex[:8]}"
        content = (
            '<?xml version="1.0" encoding="UTF-8"?>'
            '<lexicon version="1.0"'
            ' xmlns="http://www.w3.org/2005/01/pronunciation-lexicon"'
            ' alphabet="ipa" xml:lang="en-US">'
            "<lexeme><grapheme>IAM</grapheme>"
            "<alias>Identity and Access Management</alias></lexeme>"
            "</lexicon>"
        )
        try:
            polly_client.put_lexicon(Name=name, Content=content)
            resp = polly_client.get_lexicon(Name=name)
            size = resp["LexiconAttributes"]["Size"]
            assert size == len(content.encode("utf-8"))
        finally:
            try:
                polly_client.delete_lexicon(Name=name)
            except Exception as exc:
                import logging
                logging.debug("lexicon cleanup: %s", exc)


class TestSpeechSynthesisTaskEdgeCases:
    def test_task_full_lifecycle_create_and_retrieve(self, polly_client, s3_bucket):
        """Create a task with all optional fields and verify all are returned via Get."""
        resp = polly_client.start_speech_synthesis_task(
            Engine="neural",
            OutputFormat="mp3",
            OutputS3BucketName=s3_bucket,
            OutputS3KeyPrefix="edge-case/",
            Text="<speak>Full lifecycle test</speak>",
            TextType="ssml",
            VoiceId="Joanna",
            SampleRate="22050",
        )
        task = resp["SynthesisTask"]
        task_id = task["TaskId"]
        assert task["Engine"] == "neural"
        assert task["OutputFormat"] == "mp3"
        assert task["VoiceId"] == "Joanna"

        get_resp = polly_client.get_speech_synthesis_task(TaskId=task_id)
        get_task = get_resp["SynthesisTask"]
        assert get_task["TaskId"] == task_id
        assert get_task["Engine"] == "neural"
        assert get_task["VoiceId"] == "Joanna"
        assert get_task["OutputFormat"] == "mp3"
        assert "edge-case/" in get_task["OutputUri"]

    def test_task_unique_ids(self, polly_client, s3_bucket):
        """Each StartSpeechSynthesisTask call produces a unique TaskId."""
        ids = []
        for i in range(3):
            resp = polly_client.start_speech_synthesis_task(
                OutputFormat="mp3",
                OutputS3BucketName=s3_bucket,
                Text=f"Task {i}",
                VoiceId="Joanna",
            )
            ids.append(resp["SynthesisTask"]["TaskId"])
        assert len(ids) == len(set(ids))

    def test_list_tasks_returns_all_tasks(self, polly_client, s3_bucket):
        """All started tasks appear in ListSpeechSynthesisTasks."""
        started_ids = set()
        for i in range(3):
            resp = polly_client.start_speech_synthesis_task(
                OutputFormat="mp3",
                OutputS3BucketName=s3_bucket,
                Text=f"List test {i}",
                VoiceId="Joanna",
            )
            started_ids.add(resp["SynthesisTask"]["TaskId"])

        list_resp = polly_client.list_speech_synthesis_tasks()
        listed_ids = {t["TaskId"] for t in list_resp["SynthesisTasks"]}
        assert started_ids.issubset(listed_ids)

    def test_get_task_nonexistent_uuid_raises_error(self, polly_client):
        """GetSpeechSynthesisTask with a well-formed but nonexistent UUID raises SynthesisTaskNotFoundException."""
        from botocore.exceptions import ClientError

        fake_id = str(uuid.uuid4())
        with pytest.raises(ClientError) as exc:
            polly_client.get_speech_synthesis_task(TaskId=fake_id)
        assert exc.value.response["Error"]["Code"] == "SynthesisTaskNotFoundException"

    def test_task_ogg_vorbis_output_format(self, polly_client, s3_bucket):
        """StartSpeechSynthesisTask supports ogg_vorbis output format."""
        resp = polly_client.start_speech_synthesis_task(
            OutputFormat="ogg_vorbis",
            OutputS3BucketName=s3_bucket,
            Text="Ogg format test",
            VoiceId="Joanna",
        )
        task = resp["SynthesisTask"]
        assert task["OutputFormat"] == "ogg_vorbis"
        assert "ogg" in task["OutputUri"]

    def test_task_pcm_output_format(self, polly_client, s3_bucket):
        """StartSpeechSynthesisTask supports pcm output format."""
        resp = polly_client.start_speech_synthesis_task(
            OutputFormat="pcm",
            OutputS3BucketName=s3_bucket,
            Text="PCM format test",
            VoiceId="Joanna",
        )
        task = resp["SynthesisTask"]
        assert task["OutputFormat"] == "pcm"
        assert task["OutputUri"].endswith(".pcm")

    def test_task_creation_time_is_recent(self, polly_client, s3_bucket):
        """CreationTime on a new task is within a reasonable range of now."""
        import datetime

        before = datetime.datetime.now(datetime.timezone.utc)
        resp = polly_client.start_speech_synthesis_task(
            OutputFormat="mp3",
            OutputS3BucketName=s3_bucket,
            Text="Timestamp test",
            VoiceId="Joanna",
        )
        after = datetime.datetime.now(datetime.timezone.utc)
        creation_time = resp["SynthesisTask"]["CreationTime"]
        # Normalize to UTC for comparison
        if creation_time.tzinfo is None:
            creation_time = creation_time.replace(tzinfo=datetime.timezone.utc)
        assert before <= creation_time <= after


class TestVoiceBehavioralFidelity:
    def test_describe_voices_response_metadata_ok(self, polly_client):
        """DescribeVoices returns HTTP 200."""
        resp = polly_client.describe_voices()
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200

    def test_synthesize_speech_creates_and_validates_via_voice_list(self, polly_client):
        """Voice used in SynthesizeSpeech is always found in DescribeVoices."""
        voices = polly_client.describe_voices()["Voices"]
        en_us_voices = [v for v in voices if v["LanguageCode"] == "en-US"]
        assert len(en_us_voices) > 0
        voice_id = en_us_voices[0]["Id"]
        resp = polly_client.synthesize_speech(
            OutputFormat="mp3",
            Text="Hello",
            VoiceId=voice_id,
        )
        assert resp["ContentType"] == "audio/mpeg"
        assert len(resp["AudioStream"].read()) > 0

    def test_lexicon_list_after_create_and_delete(self, polly_client):
        """Lexicon appears in list after creation and disappears after deletion."""
        name = f"compat{uuid.uuid4().hex[:8]}"
        content = (
            '<?xml version="1.0" encoding="UTF-8"?>'
            '<lexicon version="1.0"'
            ' xmlns="http://www.w3.org/2005/01/pronunciation-lexicon"'
            ' alphabet="ipa" xml:lang="en-US">'
            "<lexeme><grapheme>DDB</grapheme>"
            "<alias>DynamoDB</alias></lexeme>"
            "</lexicon>"
        )
        polly_client.put_lexicon(Name=name, Content=content)
        after_create = [lex["Name"] for lex in polly_client.list_lexicons()["Lexicons"]]
        assert name in after_create

        polly_client.delete_lexicon(Name=name)
        after_delete = [lex["Name"] for lex in polly_client.list_lexicons()["Lexicons"]]
        assert name not in after_delete

    def test_synthesize_speech_invalid_voice_raises_error(self, polly_client):
        """SynthesizeSpeech with an invalid VoiceId raises InvalidParameterValue."""
        from botocore.exceptions import ClientError

        with pytest.raises(ClientError) as exc:
            polly_client.synthesize_speech(
                OutputFormat="mp3",
                Text="Hello",
                VoiceId="NotARealVoiceXYZ",
            )
        assert exc.value.response["Error"]["Code"] == "InvalidParameterValue"
