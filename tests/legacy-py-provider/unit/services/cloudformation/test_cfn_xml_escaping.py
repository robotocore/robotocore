"""CFN responses must escape caller-provided values in their XML bodies.

Stack names, parameters and tag values can contain `<`, `&` or quotes; the
response builder rendered them verbatim, so a single `&` in a parameter made
the whole DescribeStacks document unparseable by botocore's XML parser.
"""

import xml.etree.ElementTree as ET

from robotocore.services.cloudformation.provider import _xml_response


class TestXmlEscaping:
    def test_tag_values_with_entities_round_trip(self):
        resp = _xml_response(
            "DescribeStacksResponse",
            {
                "Stacks": [
                    {
                        "StackId": "arn:s",
                        "StackName": "stack&<name>",
                        "Tags": [{"Key": "k&1", "Value": "v<2>"}],
                    }
                ]
            },
        )
        root = ET.fromstring(resp.body.decode())
        stacks = root.find("{*}DescribeStacksResult").find("{*}Stacks").findall("{*}member")
        assert len(stacks) == 1
        assert stacks[0].find("{*}StackName").text == "stack&<name>"
        tags = stacks[0].find("{*}Tags").findall("{*}member")
        assert tags[0].find("{*}Key").text == "k&1"
        assert tags[0].find("{*}Value").text == "v<2>"

    def test_error_message_escapes_user_values(self):
        from robotocore.services.cloudformation.provider import _error

        resp = _error("AlreadyExistsException", "Stack [a&b<name>] already exists", 400)
        root = ET.fromstring(resp.body.decode())
        assert root.find("{*}Error").find("{*}Message").text == "Stack [a&b<name>] already exists"
