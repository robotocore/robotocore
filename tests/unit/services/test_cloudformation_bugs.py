"""Failing tests exposing bugs in the CloudFormation provider and engine.

Each test documents a specific bug. All tests are expected to FAIL against the
current codebase.
"""

import json

from robotocore.services.cloudformation.engine import (
    CfnResource,
    CfnStack,
    CfnStore,
    evaluate_conditions,
    resolve_intrinsics,
)
from robotocore.services.cloudformation.provider import (
    _delete_stack_action,
    _update_stack,
)

REGION = "us-east-1"
ACCOUNT_ID = "123456789012"


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

_EMPTY_RESOURCES: dict[str, CfnResource] = {}
_EMPTY_PARAMS: dict = {}


def _make_store_with_stack(
    stack_name: str = "test-stack",
    template_body: str = '{"Resources":{}}',
    parameters: dict | None = None,
    exports: dict | None = None,
) -> tuple[CfnStore, CfnStack]:
    """Create a store with a single stack pre-loaded."""
    store = CfnStore()
    stack = CfnStack(
        stack_id=f"arn:aws:cloudformation:{REGION}:{ACCOUNT_ID}:stack/{stack_name}/fake-id",
        stack_name=stack_name,
        template_body=template_body,
        parameters=parameters or {},
        status="CREATE_COMPLETE",
    )
    if exports:
        stack.exports = dict(exports)
        store.exports = dict(exports)
    store.put_stack(stack)
    return store, stack


# ===========================================================================
# Bug 1: Fn::GetAtt with dotted string form loses attribute name components
#
# When Fn::GetAtt is written as a dotted string "Resource.Some.Nested.Attr",
# only the second token ("Some") is used as attr_name, discarding the rest.
# The attribute key should be "Some.Nested.Attr" (everything after the first dot).
# ===========================================================================


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
        # Bug: the code does parts[1] which gives "Attr", not "Attr.SubAttr"
        assert result == "the-value"


# ===========================================================================
# Bug 2: Fn::Select does not resolve the index when it is an intrinsic
#
# If the index argument is a Ref or other intrinsic, Fn::Select passes it
# directly to int(), which will raise TypeError/ValueError on a dict.
# ===========================================================================


class TestFnSelectIndexIntrinsic:
    def test_select_with_ref_index(self):
        """Fn::Select should resolve the index if it's a Ref."""
        params = {"MyIndex": "1"}
        result = resolve_intrinsics(
            {"Fn::Select": [{"Ref": "MyIndex"}, ["a", "b", "c"]]},
            _EMPTY_RESOURCES,
            params,
            REGION,
            ACCOUNT_ID,
        )
        # Bug: int({"Ref": "MyIndex"}) raises TypeError
        assert result == "b"


# ===========================================================================
# Bug 3: Stack delete does not clean up exports from the global store
#
# When a stack that has exports is deleted, those exports remain in
# store.exports, making them available to Fn::ImportValue even though the
# exporting stack no longer exists.
# ===========================================================================


class TestDeleteStackCleansUpExports:
    def test_exports_removed_on_stack_delete(self):
        """Deleting a stack should remove its exports from the global store."""
        template = json.dumps(
            {
                "Resources": {},
                "Outputs": {
                    "Out1": {
                        "Value": "val1",
                        "Export": {"Name": "shared-output"},
                    }
                },
            }
        )
        store, stack = _make_store_with_stack(
            template_body=template,
            exports={"shared-output": "val1"},
        )

        # Confirm export exists before delete
        assert "shared-output" in store.exports

        params = {"StackName": "test-stack"}
        _delete_stack_action(store, params, REGION, ACCOUNT_ID)

        # Bug: exports are NOT removed from store.exports
        assert "shared-output" not in store.exports


# ===========================================================================
# Bug 4: _update_stack discards previous parameter values
#
# When updating a stack, the code replaces stack.parameters with only the
# new parameters from the update request. Any parameters from the original
# stack creation that are not re-supplied are lost, even though AWS preserves
# them (UsePreviousValue behavior and default carry-over).
# ===========================================================================


class TestUpdateStackPreservesParameters:
    def test_update_preserves_unrepeated_parameters(self):
        """Parameters not re-supplied in update should keep their previous values."""
        original_template = json.dumps(
            {
                "Parameters": {
                    "Env": {"Type": "String"},
                    "Region": {"Type": "String", "Default": "us-west-2"},
                },
                "Resources": {},
            }
        )
        store, stack = _make_store_with_stack(
            template_body=original_template,
            parameters={"Env": "production", "Region": "eu-west-1"},
        )

        new_template = json.dumps(
            {
                "Parameters": {
                    "Env": {"Type": "String"},
                    "Region": {"Type": "String", "Default": "us-west-2"},
                },
                "Resources": {},
            }
        )

        # Update with only the Env parameter — Region should be preserved
        params = {
            "StackName": "test-stack",
            "TemplateBody": new_template,
            "Parameters.member.1.ParameterKey": "Env",
            "Parameters.member.1.ParameterValue": "staging",
        }
        _update_stack(store, params, REGION, ACCOUNT_ID)

        updated_stack = store.get_stack("test-stack")
        # Bug: stack.parameters is replaced entirely, losing "Region": "eu-west-1"
        # It falls back to the default "us-west-2" instead of keeping "eu-west-1"
        assert updated_stack.parameters.get("Region") == "eu-west-1"


# ===========================================================================
# Bug 5: _execute_change_set is a complete no-op
#
# ExecuteChangeSet should apply the change set to create/update the stack,
# but the handler just returns {} without doing anything. A CREATE-type
# change set leaves the stack in REVIEW_IN_PROGRESS forever.
# ===========================================================================


class TestExecuteChangeSetActuallyExecutes:
    def test_execute_create_changeset_creates_resources(self):
        """ExecuteChangeSet with CREATE type should deploy the stack."""
        from robotocore.services.cloudformation.provider import (
            _create_change_set,
            _execute_change_set,
        )

        store = CfnStore()
        template = json.dumps(
            {
                "Resources": {},
            }
        )

        # Create a change set of type CREATE
        cs_params = {
            "StackName": "cs-test-stack",
            "ChangeSetName": "my-changeset",
            "TemplateBody": template,
            "ChangeSetType": "CREATE",
        }
        _create_change_set(store, cs_params, REGION, ACCOUNT_ID)

        # Confirm stack is in REVIEW_IN_PROGRESS
        stack = store.get_stack("cs-test-stack")
        assert stack is not None
        assert stack.status == "REVIEW_IN_PROGRESS"

        # Execute the change set
        exec_params = {
            "ChangeSetName": "my-changeset",
            "StackName": "cs-test-stack",
        }
        _execute_change_set(store, exec_params, REGION, ACCOUNT_ID)

        # Bug: stack stays in REVIEW_IN_PROGRESS because _execute_change_set is a no-op
        stack = store.get_stack("cs-test-stack")
        assert stack.status == "CREATE_COMPLETE"


# ===========================================================================
# Bug 6: Fn::If defaults to the true branch when the condition is undefined
#
# If Fn::If references a condition name that doesn't exist in __conditions__,
# the code silently takes the true branch. AWS would raise a validation error.
# This masks template authoring mistakes.
# ===========================================================================


class TestFnIfUndefinedCondition:
    def test_fn_if_with_undefined_condition_should_error(self):
        """Fn::If with a condition not in __conditions__ should not silently pick true."""
        params = {"__conditions__": {"ExistingCond": True}}
        # Reference a condition that does NOT exist
        value = {"Fn::If": ["NonExistentCondition", "yes", "no"]}
        # Bug: returns "yes" (true branch) instead of raising an error
        try:
            result = resolve_intrinsics(value, {}, params, REGION, ACCOUNT_ID)
            # If no exception, the bug is that it returned "yes" silently
            assert result != "yes", (
                "Fn::If with undefined condition should not silently return the true branch"
            )
        except (KeyError, ValueError):
            # This is the expected behavior — an error for undefined conditions
            pass


# ===========================================================================
# Bug 8: evaluate_conditions passes empty resources dict, but conditions
# that reference other conditions via Fn::If fail
#
# When conditions reference other conditions (e.g., via Fn::If or Fn::Equals
# with Fn::If), the order of evaluation matters. The current implementation
# iterates conditions.items() in insertion order, but a condition may
# reference another condition that hasn't been evaluated yet.
# ===========================================================================


class TestConditionDependencyOrder:
    def test_condition_referencing_another_condition(self):
        """A condition referencing another condition via Fn::If should work."""
        template = {
            "Parameters": {},
            "Conditions": {
                # BaseCond is defined first, should evaluate first
                "BaseCond": {"Fn::Equals": ["a", "a"]},
                # DerivedCond depends on BaseCond via Fn::If
                "DerivedCond": {
                    "Fn::If": ["BaseCond", {"Fn::Equals": ["x", "x"]}, {"Fn::Equals": ["x", "y"]}]
                },
            },
            "Resources": {},
        }
        params = {}
        result = evaluate_conditions(template, {}, params, REGION, ACCOUNT_ID)

        # BaseCond should be True (a == a)
        assert result["BaseCond"] is True

        # DerivedCond depends on BaseCond being already evaluated.
        # Since BaseCond is True, DerivedCond should use the true branch: Fn::Equals("x","x") = True
        # Bug: When evaluating DerivedCond, Fn::If looks up "BaseCond" in
        # params.get("__conditions__", {}), but __conditions__ is not populated
        # until evaluate_conditions returns. So BaseCond won't be found,
        # and Fn::If defaults to the true branch (accidentally correct here).
        #
        # But if the order is reversed (DerivedCond before BaseCond), it would
        # also default to true branch, which is wrong when BaseCond is False.
        assert result["DerivedCond"] is True

        # Now test with reversed order where the bug manifests:
        template2 = {
            "Parameters": {},
            "Conditions": {
                # BaseCond is False
                "BaseCond": {"Fn::Equals": ["a", "b"]},
                # DerivedCond should use false branch since BaseCond is False
                "DerivedCond": {
                    "Fn::If": [
                        "BaseCond",
                        {"Fn::Equals": ["x", "y"]},  # true branch -> False
                        {"Fn::Equals": ["x", "x"]},  # false branch -> True
                    ]
                },
            },
            "Resources": {},
        }
        result2 = evaluate_conditions(template2, {}, {}, REGION, ACCOUNT_ID)
        assert result2["BaseCond"] is False
        # Bug: Fn::If can't find "BaseCond" in __conditions__ (not populated yet),
        # so it defaults to true branch, giving Fn::Equals("x","y") = False.
        # The correct answer is: BaseCond is False -> false branch -> Fn::Equals("x","x") = True
        assert result2["DerivedCond"] is True


# ===========================================================================
# Bug 9: Fn::Cidr computes wrong subnet prefix for common cases
#
# compute_cidr uses `new_prefix = network.max_prefixlen - cidr_bits`.
# For IPv4, max_prefixlen=32. With cidr_bits=8 and ip_block="10.0.0.0/16",
# new_prefix=24, which gives /24 subnets. This is correct for AWS's
# interpretation where cidr_bits is the number of host bits.
#
# BUT: For cidr_bits=4 with a /16, new_prefix=28, giving /28 subnets with
# 16 addresses each (14 usable). AWS docs say cidr_bits determines the
# size of the subnet. For cidr_bits=4 on a /16, AWS returns /20 subnets
# (4 bits added to prefix), not /28 subnets (4 host bits).
#
# Actually, let me verify: AWS Fn::Cidr(["10.0.0.0/16", 4, 8]) means
# "4 subnets with 2^8=256 addresses each" = /24 subnets.
# compute_cidr(ip_block="10.0.0.0/16", count=4, cidr_bits=8) gives
# new_prefix=32-8=24, which is /24. That's correct!
#
# But compute_cidr(ip_block="10.0.0.0/16", count=6, cidr_bits=4) gives
# new_prefix=32-4=28, which is /28 (16 addresses). AWS returns /28 too.
# Actually this is correct.
#
# Let me find a real bug instead...
# ===========================================================================

# (Removed — Fn::Cidr is actually correct)


# ===========================================================================
# Bug 10: _xml_response doesn't escape XML special characters in values
#
# If a stack description, parameter value, or resource ID contains XML
# special characters (< > & " '), they are inserted raw into the XML
# response, producing invalid XML.
# ===========================================================================


class TestXmlResponseEscaping:
    def test_xml_special_chars_in_values(self):
        """XML special characters in values should be escaped."""
        from robotocore.services.cloudformation.provider import _xml_response

        data = {"Description": 'Test with <special> & "chars"'}
        response = _xml_response("TestResponse", data)
        body = response.body.decode()
        # Bug: raw < > & " in XML body makes it invalid
        assert "&lt;" in body, "< should be escaped as &lt;"
        assert "&amp;" in body, "& should be escaped as &amp;"
        assert "<special>" not in body, "raw <special> should not appear in XML"
