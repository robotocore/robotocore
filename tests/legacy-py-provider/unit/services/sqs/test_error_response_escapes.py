"""Unit tests for the native SQS provider's error builder."""

import json
from xml.etree import ElementTree

from robotocore.services.sqs.provider import _error


class TestErrorResponses:
    NS = "{http://queue.amazonaws.com/doc/2012-11-05/}"

    def test_xml_error_escapes_caller_text(self):
        """Queue names and unexpected-exception text can contain '<' or '&':
        the error body must stay parseable XML with the message intact."""
        resp = _error("InvalidAttributeName", "bad name <weird> & co", 400, use_json=False)
        assert resp.status_code == 400
        root = ElementTree.fromstring(resp.body.decode())
        code = root.find(f"{self.NS}Error/{self.NS}Code")
        message = root.find(f"{self.NS}Error/{self.NS}Message")
        assert code is not None and message is not None
        assert code.text == "InvalidAttributeName"
        assert message.text == "bad name <weird> & co"

    def test_json_error_carries_the_message_unchanged(self):
        resp = _error("InternalError", "boom <script>", 500, use_json=True)
        parsed = json.loads(resp.body.decode())
        assert parsed["__type"] == "InternalError"
        assert parsed["message"] == "boom <script>"
