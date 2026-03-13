"""Tests for Lambda event source mapping engine utilities."""

from robotocore.services.lambda_.event_source import EventSourceEngine, _extract_function_name


class TestExtractFunctionName:
    def test_unqualified_arn(self):
        """Unqualified ARN returns just the function name."""
        arn = "arn:aws:lambda:us-east-1:123456789012:function:myFunc"
        assert _extract_function_name(arn) == "myFunc"

    def test_qualified_arn_with_alias(self):
        """Qualified ARN with alias returns name:alias."""
        arn = "arn:aws:lambda:us-east-1:123456789012:function:myFunc:myAlias"
        assert _extract_function_name(arn) == "myFunc:myAlias"

    def test_qualified_arn_with_version(self):
        """Qualified ARN with version returns name:version."""
        arn = "arn:aws:lambda:us-east-1:123456789012:function:myFunc:42"
        assert _extract_function_name(arn) == "myFunc:42"

    def test_plain_function_name(self):
        """Plain function name (not an ARN) is returned as-is."""
        assert _extract_function_name("myFunc") == "myFunc"

    def test_short_arn_like_string(self):
        """ARN-like string with too few parts returns as-is."""
        assert _extract_function_name("arn:aws:lambda") == "arn:aws:lambda"


class TestEngineInitializesPositionDicts:
    def test_kinesis_positions_initialized(self):
        engine = EventSourceEngine()
        assert hasattr(engine, "_kinesis_positions")
        assert engine._kinesis_positions == {}

    def test_dynamo_stream_positions_initialized(self):
        engine = EventSourceEngine()
        assert hasattr(engine, "_dynamo_stream_positions")
        assert engine._dynamo_stream_positions == {}
