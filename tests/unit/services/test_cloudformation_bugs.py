"""Tests for correctness bugs in the CloudFormation provider and engine."""

from robotocore.services.cloudformation.engine import (
    CfnResource,
    resolve_intrinsics,
)

REGION = "us-east-1"
ACCOUNT_ID = "123456789012"


class TestFnGetAttDottedStringMultiComponent:
    def test_getatt_dotted_string_with_multiple_dots(self):
        """Fn::GetAtt 'MyResource.Attr.SubAttr' should look up 'Attr.SubAttr'."""
        resources = {
            "MyResource": CfnResource(
                logical_id="MyResource",
                resource_type="AWS::Some::Resource",
                properties={},
                physical_id="phys-123",
                attributes={"Attr.SubAttr": "the-value"},
            ),
        }
        result = resolve_intrinsics(
            {"Fn::GetAtt": "MyResource.Attr.SubAttr"},
            resources,
            {},
            REGION,
            ACCOUNT_ID,
        )
        assert result == "the-value"
