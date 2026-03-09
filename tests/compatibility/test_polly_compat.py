"""Compatibility tests for Amazon Polly."""

import uuid

import pytest

from tests.compatibility.conftest import make_client


@pytest.fixture
def polly_client():
    return make_client("polly")


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
        pass


class TestDescribeVoices:
    def test_returns_voices_list(self, polly_client):
        resp = polly_client.describe_voices()
        voices = resp["Voices"]
        assert isinstance(voices, list)
        assert len(voices) > 0

    def test_voice_has_expected_fields(self, polly_client):
        resp = polly_client.describe_voices()
        voice = resp["Voices"][0]
        assert "Id" in voice
        assert "Name" in voice
        assert "LanguageCode" in voice
        assert "Gender" in voice

    def test_filter_by_language(self, polly_client):
        resp = polly_client.describe_voices(LanguageCode="en-US")
        voices = resp["Voices"]
        assert len(voices) > 0
        for v in voices:
            assert v["LanguageCode"] == "en-US"


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
