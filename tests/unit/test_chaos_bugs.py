"""Failing tests that expose bugs in the chaos engineering module.

Each test targets a specific bug. All tests are expected to FAIL against
the current implementation — they document correctness issues.
"""

import re
from unittest.mock import MagicMock, patch

from robotocore.chaos.fault_rules import FaultRule, FaultRuleStore
from robotocore.chaos.middleware import chaos_handler
from robotocore.gateway.handler_chain import RequestContext


def _make_context(service="s3", operation="PutObject", region="us-east-1"):
    request = MagicMock()
    ctx = RequestContext(
        request=request,
        service_name=service,
        operation=operation,
        region=region,
    )
    return ctx


class TestBugOperationRegexPartialMatch:
    """Bug: operation regex uses re.search() instead of re.fullmatch().

    This means operation="Get" matches "ForGetAboutIt" because search()
    finds "Get" as a substring. Users expect operation patterns to match
    the full operation name, not substrings.

    Fix: use re.fullmatch() or re.match() with anchors instead of re.search().
    """

    def test_operation_filter_should_not_match_substring(self):
        """operation="Get" should NOT match "ForGetAboutIt"."""
        rule = FaultRule(operation="Get")
        # "Get" appears as a substring — search() finds it but the intent
        # is to match only the operation literally named "Get"
        assert rule.matches("s3", "ForGetAboutIt", "us-east-1") is False

    def test_operation_filter_should_not_match_suffix(self):
        """operation="List" should NOT match "GetBucketList"."""
        rule = FaultRule(operation="List")
        assert rule.matches("s3", "GetBucketList", "us-east-1") is False

    def test_operation_filter_should_not_match_prefix_substring(self):
        """operation="Put" should NOT match "PutObject".

        If the user wants all Put* operations, they should use "Put.*".
        A bare "Put" should only match an operation literally named "Put".
        """
        rule = FaultRule(operation="Put")
        assert rule.matches("s3", "PutObject", "us-east-1") is False


class TestBugStatusCodeZeroOverridden:
    """Bug: status_code=0 is treated as falsy and overridden.

    In __init__: self.status_code = status_code or (429 if ... else 500)
    The `or` operator treats 0 as falsy, so an explicit status_code=0
    gets silently replaced with the computed default (429 or 500).

    Fix: use `status_code if status_code is not None else (429 if ... else 500)`.
    """

    def test_explicit_status_code_zero_is_preserved(self):
        """Passing status_code=0 should store 0, not recompute a default."""
        rule = FaultRule(error_code="ThrottlingException", status_code=0)
        assert rule.status_code == 0

    def test_from_dict_status_code_zero_preserved(self):
        """from_dict with status_code=0 should preserve it."""
        data = {"error_code": "ThrottlingException", "status_code": 0}
        rule = FaultRule.from_dict(data)
        assert rule.status_code == 0

    def test_roundtrip_status_code_zero(self):
        """to_dict -> from_dict should preserve status_code=0."""
        # Bypass __init__'s or-expression to set status_code=0
        rule = FaultRule.__new__(FaultRule)
        rule.rule_id = "test"
        rule.service = None
        rule.operation = None
        rule.region = None
        rule.error_code = "Test"
        rule.error_message = "test"
        rule.status_code = 0
        rule.latency_ms = 0
        rule.probability = 1.0
        rule.enabled = True
        rule.created_at = 0
        rule.match_count = 0
        rule._op_pattern = None

        d = rule.to_dict()
        assert d["status_code"] == 0  # to_dict preserves 0
        restored = FaultRule.from_dict(d)
        assert restored.status_code == 0  # from_dict should too


class TestBugInvalidRegexCrashesOnConstruction:
    """Bug: invalid regex in operation raises re.error during __init__.

    FaultRule(operation="[unclosed") crashes with an opaque re.PatternError.
    There's no validation, no user-friendly error, and no way to recover.
    User-supplied input from the POST /_robotocore/chaos/rules API flows
    directly into re.compile() without error handling.

    Fix: catch re.error in __init__ and raise ValueError with a clear message,
    or validate in from_dict/the API handler before construction.
    """

    def test_invalid_regex_should_not_crash(self):
        """Creating a rule with invalid regex should not raise re.error."""
        try:
            FaultRule(operation="[unclosed")
        except re.error:
            raise AssertionError(
                "FaultRule should handle invalid regex gracefully, "
                "but it raises re.error on construction"
            )

    def test_from_dict_invalid_regex_should_not_crash(self):
        """from_dict with invalid regex operation should not raise re.error."""
        data = {"operation": "(unclosed"}
        try:
            FaultRule.from_dict(data)
        except re.error:
            raise AssertionError("FaultRule.from_dict should handle invalid regex gracefully")


class TestBugErrorResponseAlwaysJson:
    """Bug: chaos error responses are always JSON, even for XML-protocol services.

    S3 uses rest-xml protocol. AWS SDKs expect XML error responses like:
      <Error><Code>SlowDown</Code><Message>...</Message></Error>

    SQS (query protocol) also expects XML error responses.

    Chaos always returns JSON ({"__type": "...", "message": "..."}),
    which causes AWS SDK XML parsers to fail with confusing errors,
    making the chaos injection untestable for XML-protocol services.

    Fix: check context.protocol and format errors accordingly.
    """

    def test_s3_error_should_be_xml(self):
        """S3 chaos errors should be XML, not JSON, to match AWS wire format."""
        store = FaultRuleStore()
        store.add(FaultRule(service="s3", error_code="SlowDown", status_code=503))
        with patch("robotocore.chaos.middleware.get_fault_store", return_value=store):
            ctx = _make_context(service="s3", operation="PutObject")
            ctx.protocol = "rest-xml"
            chaos_handler(ctx)
            assert ctx.response is not None
            body = ctx.response.body.decode()
            assert body.startswith("<?xml") or body.startswith("<Error"), (
                f"S3 error should be XML but got: {body}"
            )

    def test_sqs_query_error_should_be_xml(self):
        """SQS chaos errors should be XML for query protocol."""
        store = FaultRuleStore()
        store.add(FaultRule(service="sqs", error_code="ThrottlingException"))
        with patch("robotocore.chaos.middleware.get_fault_store", return_value=store):
            ctx = _make_context(service="sqs", operation="SendMessage")
            ctx.protocol = "query"
            chaos_handler(ctx)
            assert ctx.response is not None
            body = ctx.response.body.decode()
            assert "<Error" in body or body.startswith("<?xml"), (
                f"SQS error should be XML but got: {body}"
            )
