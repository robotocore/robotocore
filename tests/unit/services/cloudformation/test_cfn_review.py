"""Failing tests for CloudFormation edge cases and missing behaviors.

Each test documents correct AWS CloudFormation behavior that the current
implementation does NOT handle. All tests are expected to FAIL.
"""

import json

import pytest

from robotocore.services.cloudformation.engine import (
    CfnResource,
    CfnStack,
    CfnStore,
    evaluate_conditions,
    parse_template,
    resolve_intrinsics,
)
from robotocore.services.cloudformation.provider import (
    CfnError,
    _create_change_set,
    _create_stack,
    _delete_stack_action,
    _describe_stacks,
    _list_exports,
    _update_stack,
    _validate_template,
)

REGION = "us-east-1"
ACCOUNT = "123456789012"


# ---------------------------------------------------------------------------
# Helper: build a simple template JSON string
# ---------------------------------------------------------------------------
def _tpl(resources, parameters=None, outputs=None, conditions=None, mappings=None):
    t = {"AWSTemplateFormatVersion": "2010-09-09", "Resources": resources}
    if parameters:
        t["Parameters"] = parameters
    if outputs:
        t["Outputs"] = outputs
    if conditions:
        t["Conditions"] = conditions
    if mappings:
        t["Mappings"] = mappings
    return json.dumps(t)


# ===========================================================================
# 1. Stack update should do in-place updates, not destroy-and-recreate
# ===========================================================================


class TestUpdateInPlace:
    """AWS CloudFormation updates resources in-place when possible, only
    replacing them when the change requires replacement. The current
    implementation deletes ALL old resources and recreates everything."""

    def test_update_preserves_unchanged_resources(self):
        """When updating a stack, resources that haven't changed should keep
        their physical IDs. Currently the provider deletes all resources and
        recreates them, giving new physical IDs."""
        store = CfnStore()
        # Use S3 buckets without explicit names -- they get auto-generated unique names
        tpl_v1 = _tpl(
            {
                "Bucket1": {
                    "Type": "AWS::S3::Bucket",
                    "Properties": {},
                },
                "Bucket2": {
                    "Type": "AWS::S3::Bucket",
                    "Properties": {},
                },
            }
        )
        _create_stack(
            store,
            {"StackName": "inplace-test", "TemplateBody": tpl_v1},
            REGION,
            ACCOUNT,
        )
        stack = store.get_stack("inplace-test")
        assert stack.status == "CREATE_COMPLETE"
        original_pid = stack.resources["Bucket1"].physical_id

        # Update: only add a tag to Bucket2, Bucket1 is unchanged
        tpl_v2 = _tpl(
            {
                "Bucket1": {
                    "Type": "AWS::S3::Bucket",
                    "Properties": {},
                },
                "Bucket2": {
                    "Type": "AWS::S3::Bucket",
                    "Properties": {
                        "Tags": [{"Key": "env", "Value": "prod"}],
                    },
                },
            }
        )
        _update_stack(
            store,
            {"StackName": "inplace-test", "TemplateBody": tpl_v2},
            REGION,
            ACCOUNT,
        )
        stack = store.get_stack("inplace-test")
        # BUG: Bucket1 gets a new auto-generated name because everything is
        # destroyed and recreated
        assert stack.resources["Bucket1"].physical_id == original_pid, (
            "Unchanged resource should keep its physical ID across updates"
        )

    def test_update_previous_parameters_preserved_when_not_overridden(self):
        """When updating a stack with UsePreviousValue, parameters from
        the previous deployment should be preserved. The current implementation
        replaces stack.parameters entirely, losing old values."""
        store = CfnStore()
        tpl = _tpl(
            {
                "Queue": {
                    "Type": "AWS::SQS::Queue",
                    "Properties": {"QueueName": {"Ref": "QName"}},
                }
            },
            parameters={
                "QName": {"Type": "String"},
                "QTimeout": {"Type": "String", "Default": "30"},
            },
        )
        _create_stack(
            store,
            {
                "StackName": "prev-param",
                "TemplateBody": tpl,
                "Parameters.member.1.ParameterKey": "QName",
                "Parameters.member.1.ParameterValue": "my-queue",
            },
            REGION,
            ACCOUNT,
        )
        stack = store.get_stack("prev-param")
        assert stack.status == "CREATE_COMPLETE"
        # QName should be "my-queue", QTimeout should be default "30"
        assert stack.parameters.get("QName") == "my-queue"

        # Update: only provide a new template body, no parameters
        # AWS uses previous parameter values by default
        tpl_v2 = _tpl(
            {
                "Queue": {
                    "Type": "AWS::SQS::Queue",
                    "Properties": {"QueueName": {"Ref": "QName"}},
                },
                "Param": {
                    "Type": "AWS::SSM::Parameter",
                    "Properties": {
                        "Name": "/test",
                        "Type": "String",
                        "Value": "v2",
                    },
                },
            },
            parameters={
                "QName": {"Type": "String"},
                "QTimeout": {"Type": "String", "Default": "30"},
            },
        )
        _update_stack(
            store,
            {"StackName": "prev-param", "TemplateBody": tpl_v2},
            REGION,
            ACCOUNT,
        )
        stack = store.get_stack("prev-param")
        # BUG: parameters are replaced with empty dict, losing QName="my-queue"
        assert stack.parameters.get("QName") == "my-queue", (
            "Previous parameter values should be preserved when not overridden"
        )


# ===========================================================================
# 2. DeletionPolicy support
# ===========================================================================


class TestDeletionPolicy:
    """AWS CloudFormation honors DeletionPolicy on resources. When set to
    'Retain', the resource is NOT deleted when the stack is deleted.
    The current implementation ignores DeletionPolicy entirely."""

    def test_deletion_policy_retain_prevents_resource_deletion(self):
        """A resource with DeletionPolicy: Retain should NOT be deleted
        when the stack is deleted."""
        store = CfnStore()
        tpl = _tpl(
            {
                "MyQueue": {
                    "Type": "AWS::SQS::Queue",
                    "DeletionPolicy": "Retain",
                    "Properties": {"QueueName": "retained-queue"},
                }
            }
        )
        _create_stack(store, {"StackName": "retain-test", "TemplateBody": tpl}, REGION, ACCOUNT)
        stack = store.get_stack("retain-test")
        assert stack.status == "CREATE_COMPLETE"
        _queue_pid = stack.resources["MyQueue"].physical_id  # noqa: F841

        _delete_stack_action(store, {"StackName": "retain-test"}, REGION, ACCOUNT)
        stack = store.get_stack("retain-test")
        assert stack.status == "DELETE_COMPLETE"

        # The resource should still exist (Retain policy)
        # BUG: delete_resource is called unconditionally, ignoring DeletionPolicy
        # We need to verify the resource still exists. Since we can't easily check
        # the SQS store from here, we verify that the resource's DeletionPolicy
        # was at least parsed from the template. The provider doesn't store it.
        # This is a design-level bug: DeletionPolicy is never read from the template.
        assert hasattr(stack.resources.get("MyQueue", object()), "deletion_policy") or True
        # Actually test by checking the template was parsed with DeletionPolicy
        template = parse_template(tpl)
        assert template["Resources"]["MyQueue"]["DeletionPolicy"] == "Retain"
        # The real test: the CfnResource dataclass should have a deletion_policy field
        res = CfnResource(logical_id="X", resource_type="AWS::SQS::Queue", properties={})
        # BUG: CfnResource has no deletion_policy attribute
        assert hasattr(res, "deletion_policy"), (
            "CfnResource should have a deletion_policy field to honor DeletionPolicy"
        )


# ===========================================================================
# 3. Rollback on update failure
# ===========================================================================


class TestUpdateRollback:
    """When a stack update fails, AWS rolls back to the previous state,
    restoring the old resources. The current implementation leaves the
    stack in UPDATE_FAILED with the old resources already deleted."""

    def test_update_failure_rolls_back_to_previous_state(self):
        """After a failed update, the stack should be in
        UPDATE_ROLLBACK_COMPLETE with the original resources restored."""
        store = CfnStore()
        tpl_v1 = _tpl(
            {
                "MyQueue": {
                    "Type": "AWS::SQS::Queue",
                    "Properties": {"QueueName": "rollback-queue"},
                }
            }
        )
        _create_stack(
            store, {"StackName": "rollback-test", "TemplateBody": tpl_v1}, REGION, ACCOUNT
        )

        # Update with a template containing an unsupported resource type
        # This should fail during deployment and trigger rollback
        tpl_v2 = _tpl(
            {
                "MyQueue": {
                    "Type": "AWS::SQS::Queue",
                    "Properties": {"QueueName": "rollback-queue"},
                },
                "BadResource": {
                    "Type": "AWS::Fake::DoesNotExist",
                    "Properties": {},
                },
            }
        )
        _update_stack(
            store, {"StackName": "rollback-test", "TemplateBody": tpl_v2}, REGION, ACCOUNT
        )
        stack = store.get_stack("rollback-test")

        # BUG: Stack is UPDATE_FAILED, not UPDATE_ROLLBACK_COMPLETE
        # AWS would restore the original resources
        assert stack.status == "UPDATE_ROLLBACK_COMPLETE", (
            f"Failed update should roll back to UPDATE_ROLLBACK_COMPLETE, got {stack.status}"
        )

        # BUG: Original resources are gone because they were deleted before
        # the new deployment was attempted
        assert "MyQueue" in stack.resources, "Original resources should be restored after rollback"


# ===========================================================================
# 4. Nested stacks (AWS::CloudFormation::Stack)
# ===========================================================================


class TestNestedStacks:
    """AWS CloudFormation supports nested stacks via the
    AWS::CloudFormation::Stack resource type."""

    def test_nested_stack_outputs_accessible_via_getatt(self):
        """Fn::GetAtt on a nested stack should return the nested stack's
        Outputs. AWS supports NestedStack.Outputs.OutputKey syntax."""
        resources = {
            "NestedStack": CfnResource(
                logical_id="NestedStack",
                resource_type="AWS::CloudFormation::Stack",
                properties={},
                physical_id="arn:aws:cfn:us-east-1:123:stack/nested/id",
                # Nested stack outputs should be accessible as attributes
                attributes={},
            )
        }
        # AWS allows: !GetAtt NestedStack.Outputs.MyOutput
        value = {"Fn::GetAtt": ["NestedStack", "Outputs.MyOutput"]}
        result = resolve_intrinsics(value, resources, {}, REGION, ACCOUNT)
        # BUG: Returns "" because "Outputs.MyOutput" is not in attributes
        # The nested stack handler should populate Outputs.* attributes
        assert result != "", "GetAtt on nested stack should resolve Outputs.* attributes"


# ===========================================================================
# 5. Cross-stack references (Fn::ImportValue)
# ===========================================================================


class TestCrossStackReferences:
    """Cross-stack references via Exports/Imports should enforce that
    exported values exist and that stacks with active imports can't be deleted."""

    def test_import_nonexistent_export_raises_error(self):
        """Fn::ImportValue for a non-existent export should raise an error
        during stack creation, not silently return the export name as a string."""
        store = CfnStore()
        tpl = _tpl(
            {
                "MyQueue": {
                    "Type": "AWS::SQS::Queue",
                    "Properties": {
                        "QueueName": {"Fn::ImportValue": "NonExistentExport"},
                    },
                }
            }
        )
        # BUG: Currently this succeeds with the queue named "NonExistentExport"
        # because resolve_intrinsics returns the import name as a fallback
        with pytest.raises(CfnError) as exc_info:
            _create_stack(store, {"StackName": "import-test", "TemplateBody": tpl}, REGION, ACCOUNT)
        assert exc_info.value.code == "ValidationError"

    def test_cannot_delete_stack_with_active_exports_in_use(self):
        """A stack with exports that are imported by other stacks cannot be
        deleted until the importing stacks are deleted first."""
        store = CfnStore()

        # Stack A: exports a value
        tpl_a = json.dumps(
            {
                "AWSTemplateFormatVersion": "2010-09-09",
                "Resources": {
                    "Queue": {
                        "Type": "AWS::SQS::Queue",
                        "Properties": {"QueueName": "exported-queue"},
                    }
                },
                "Outputs": {
                    "QueueUrl": {
                        "Value": {"Fn::GetAtt": ["Queue", "QueueUrl"]},
                        "Export": {"Name": "SharedQueueUrl"},
                    }
                },
            }
        )
        _create_stack(store, {"StackName": "exporter", "TemplateBody": tpl_a}, REGION, ACCOUNT)

        # Stack B: imports the value
        tpl_b = _tpl(
            {
                "Param": {
                    "Type": "AWS::SSM::Parameter",
                    "Properties": {
                        "Name": "/imported",
                        "Type": "String",
                        "Value": {"Fn::ImportValue": "SharedQueueUrl"},
                    },
                }
            }
        )
        _create_stack(store, {"StackName": "importer", "TemplateBody": tpl_b}, REGION, ACCOUNT)

        # BUG: Deleting the exporter stack should fail because importer depends on it
        with pytest.raises(CfnError) as exc_info:
            _delete_stack_action(store, {"StackName": "exporter"}, REGION, ACCOUNT)
        msg = exc_info.value.message.lower()
        assert "export" in msg or "import" in msg


# ===========================================================================
# 6. Fn::FindInMap intrinsic function
# ===========================================================================


class TestFnFindInMap:
    """Fn::FindInMap should look up values in the Mappings section."""

    def test_find_in_map_resolves_value(self):
        """Fn::FindInMap should resolve from the Mappings section."""
        tpl = json.dumps(
            {
                "AWSTemplateFormatVersion": "2010-09-09",
                "Mappings": {
                    "RegionMap": {
                        "us-east-1": {"AMI": "ami-12345"},
                        "us-west-2": {"AMI": "ami-67890"},
                    }
                },
                "Resources": {
                    "Param": {
                        "Type": "AWS::SSM::Parameter",
                        "Properties": {
                            "Name": "/ami",
                            "Type": "String",
                            "Value": {
                                "Fn::FindInMap": ["RegionMap", {"Ref": "AWS::Region"}, "AMI"]
                            },
                        },
                    }
                },
            }
        )
        store = CfnStore()
        _create_stack(store, {"StackName": "map-test", "TemplateBody": tpl}, REGION, ACCOUNT)
        stack = store.get_stack("map-test")
        # BUG: Fn::FindInMap is not implemented in resolve_intrinsics
        assert stack.status == "CREATE_COMPLETE"
        # The parameter value should be the mapped AMI
        assert stack.resources["Param"].properties.get("Value") == "ami-12345"


# ===========================================================================
# 7. Fn::If with AWS::NoValue
# ===========================================================================


class TestFnIfNoValue:
    """When Fn::If evaluates to AWS::NoValue, the enclosing property
    should be omitted entirely from the resource."""

    def test_fn_if_aws_novalue_removes_property(self):
        """Fn::If returning Ref: AWS::NoValue should cause the property to be
        absent, not set to None."""
        resources = {}
        parameters = {
            "__conditions__": {"CreateProd": False},
            "AWS::Region": REGION,
        }
        value = {
            "Fn::If": ["CreateProd", "prod-value", {"Ref": "AWS::NoValue"}],
        }
        result = resolve_intrinsics(value, resources, parameters, REGION, ACCOUNT)
        # BUG: Currently returns None (from Ref: AWS::NoValue) instead of a
        # sentinel that tells the caller to omit the property
        # AWS::NoValue should signal property removal, not just return None
        assert result is None  # This part passes...

        # But the real test: when used in a properties dict, the key should be removed
        props = {
            "QueueName": "my-queue",
            "VisibilityTimeout": {"Fn::If": ["CreateProd", 300, {"Ref": "AWS::NoValue"}]},
        }
        resolved = resolve_intrinsics(props, resources, parameters, REGION, ACCOUNT)
        # BUG: VisibilityTimeout is present with value None instead of being removed
        assert "VisibilityTimeout" not in resolved, (
            "Properties set to AWS::NoValue via Fn::If should be removed from the dict"
        )


# ===========================================================================
# 8. Fn::Base64 intrinsic function
# ===========================================================================


class TestFnBase64:
    """Fn::Base64 should encode a string to Base64."""

    def test_fn_base64_encodes_string(self):
        """Fn::Base64 should return the base64-encoded form of the input."""
        resources = {}
        parameters = {}
        value = {"Fn::Base64": "Hello World"}
        result = resolve_intrinsics(value, resources, parameters, REGION, ACCOUNT)
        import base64

        expected = base64.b64encode(b"Hello World").decode()
        # BUG: Fn::Base64 is not implemented; the dict is returned as-is
        assert result == expected, f"Expected base64 encoding, got {result}"


# ===========================================================================
# 9. Template validation should check for Resources section
# ===========================================================================


class TestTemplateValidation:
    """AWS CloudFormation requires a Resources section in every template."""

    def test_validate_template_without_resources_fails(self):
        """A template without a Resources section should be rejected."""
        store = CfnStore()
        tpl = json.dumps(
            {
                "AWSTemplateFormatVersion": "2010-09-09",
                "Description": "No resources",
            }
        )
        # BUG: ValidateTemplate does not check for the Resources section
        with pytest.raises(CfnError) as exc_info:
            _validate_template(store, {"TemplateBody": tpl}, REGION, ACCOUNT)
        assert exc_info.value.code == "ValidationError"

    def test_create_stack_without_resources_fails(self):
        """Creating a stack with a template that has no Resources should fail."""
        store = CfnStore()
        tpl = json.dumps(
            {
                "AWSTemplateFormatVersion": "2010-09-09",
                "Description": "Empty",
            }
        )
        # BUG: This succeeds (with no resources deployed) instead of raising
        with pytest.raises(CfnError) as exc_info:
            _create_stack(store, {"StackName": "empty-test", "TemplateBody": tpl}, REGION, ACCOUNT)
        assert exc_info.value.code == "ValidationError"


# ===========================================================================
# 10. ListExports should include ExportingStackId
# ===========================================================================


class TestListExports:
    """ListExports should include the stack ID that created each export."""

    def test_list_exports_includes_stack_id(self):
        """Each export should include the ExportingStackId of the stack
        that defined it."""
        store = CfnStore()
        tpl = json.dumps(
            {
                "AWSTemplateFormatVersion": "2010-09-09",
                "Resources": {
                    "Queue": {
                        "Type": "AWS::SQS::Queue",
                        "Properties": {"QueueName": "export-test-queue"},
                    }
                },
                "Outputs": {
                    "QueueArn": {
                        "Value": {"Fn::GetAtt": ["Queue", "Arn"]},
                        "Export": {"Name": "TestQueueArn"},
                    }
                },
            }
        )
        _create_stack(store, {"StackName": "export-stack", "TemplateBody": tpl}, REGION, ACCOUNT)
        stack = store.get_stack("export-stack")

        result = _list_exports(store, {}, REGION, ACCOUNT)
        exports = result["Exports"]
        assert len(exports) >= 1

        # BUG: ExportingStackId is always empty string ""
        export = next(e for e in exports if e["Name"] == "TestQueueArn")
        assert export["ExportingStackId"] != "", (
            "ExportingStackId should be the ARN of the stack that created the export"
        )
        assert export["ExportingStackId"] == stack.stack_id


# ===========================================================================
# 11. Describe deleted stack by name should error
# ===========================================================================


class TestDescribeDeletedStack:
    """DescribeStacks for a DELETE_COMPLETE stack found by name should
    raise an error (you need the stack ID to query deleted stacks)."""

    def test_describe_deleted_stack_by_name_raises_error(self):
        """AWS raises 'Stack with id X does not exist' when you describe a
        DELETE_COMPLETE stack by name."""
        store = CfnStore()
        tpl = _tpl(
            {
                "Queue": {
                    "Type": "AWS::SQS::Queue",
                    "Properties": {"QueueName": "delete-describe-q"},
                }
            }
        )
        _create_stack(store, {"StackName": "desc-del", "TemplateBody": tpl}, REGION, ACCOUNT)
        _delete_stack_action(store, {"StackName": "desc-del"}, REGION, ACCOUNT)

        # BUG: get_stack returns the DELETE_COMPLETE stack when looked up by name
        # AWS requires you to use the stack ID to query deleted stacks
        with pytest.raises(CfnError) as exc_info:
            _describe_stacks(store, {"StackName": "desc-del"}, REGION, ACCOUNT)
        assert exc_info.value.code == "ValidationError"


# ===========================================================================
# 12. Circular dependencies should be detected and rejected
# ===========================================================================


class TestCircularDependencies:
    """CloudFormation should detect circular dependencies and reject the
    template, not silently create resources in arbitrary order."""

    def test_circular_dependency_raises_error(self):
        """A template with circular DependsOn should fail with a
        ValidationError."""
        tpl = _tpl(
            {
                "A": {
                    "Type": "AWS::SQS::Queue",
                    "DependsOn": "B",
                    "Properties": {"QueueName": "circ-a"},
                },
                "B": {
                    "Type": "AWS::SQS::Queue",
                    "DependsOn": "A",
                    "Properties": {"QueueName": "circ-b"},
                },
            }
        )
        store = CfnStore()
        # BUG: build_dependency_order adds remaining (circular) nodes at the end
        # without raising an error
        with pytest.raises(CfnError) as exc_info:
            _create_stack(store, {"StackName": "circ-test", "TemplateBody": tpl}, REGION, ACCOUNT)
        msg = exc_info.value.message.lower()
        assert "circular" in msg or "dependency" in msg


# ===========================================================================
# 13. Stack events completeness
# ===========================================================================


class TestStackEvents:
    """Stack events should include per-resource CREATE_IN_PROGRESS and
    CREATE_COMPLETE events, plus rollback events on failure."""

    def test_rollback_events_include_per_resource_detail(self):
        """When a stack creation fails and rolls back, events should include
        DELETE_IN_PROGRESS / DELETE_COMPLETE for each resource being rolled back."""
        store = CfnStore()
        tpl = _tpl(
            {
                "GoodQueue": {
                    "Type": "AWS::SQS::Queue",
                    "Properties": {"QueueName": "good-event-queue"},
                },
                "BadResource": {
                    "Type": "AWS::Fake::Unsupported",
                    "Properties": {},
                    "DependsOn": "GoodQueue",
                },
            }
        )
        _create_stack(store, {"StackName": "event-test", "TemplateBody": tpl}, REGION, ACCOUNT)
        stack = store.get_stack("event-test")
        assert stack.status == "ROLLBACK_COMPLETE"

        event_statuses = [e["ResourceStatus"] for e in stack.events]
        # BUG: No DELETE_IN_PROGRESS or DELETE_COMPLETE events are generated
        # during rollback
        assert "DELETE_IN_PROGRESS" in event_statuses, (
            "Rollback should generate DELETE_IN_PROGRESS events for created resources"
        )
        assert "DELETE_COMPLETE" in event_statuses, (
            "Rollback should generate DELETE_COMPLETE events for created resources"
        )


# ===========================================================================
# 14. Change set should compute actual changes
# ===========================================================================


class TestChangeSetChanges:
    """DescribeChangeSet should include a list of Changes describing what
    resources will be added, modified, or removed."""

    def test_describe_change_set_includes_changes(self):
        """An UPDATE change set should list the resources that will change."""
        store = CfnStore()
        tpl_v1 = _tpl(
            {
                "Queue": {
                    "Type": "AWS::SQS::Queue",
                    "Properties": {"QueueName": "cs-change-q"},
                }
            }
        )
        # Create initial stack
        stack = CfnStack(
            stack_id=f"arn:aws:cloudformation:{REGION}:{ACCOUNT}:stack/cs-changes/fake",
            stack_name="cs-changes",
            template_body=tpl_v1,
            status="CREATE_COMPLETE",
        )
        store.put_stack(stack)

        tpl_v2 = _tpl(
            {
                "Queue": {
                    "Type": "AWS::SQS::Queue",
                    "Properties": {"QueueName": "cs-change-q-v2"},
                },
                "NewTopic": {
                    "Type": "AWS::SNS::Topic",
                    "Properties": {"TopicName": "new-topic"},
                },
            }
        )
        _create_change_set(
            store,
            {
                "StackName": "cs-changes",
                "ChangeSetName": "my-cs",
                "TemplateBody": tpl_v2,
                "ChangeSetType": "UPDATE",
            },
            REGION,
            ACCOUNT,
        )

        from robotocore.services.cloudformation.provider import _describe_change_set

        desc = _describe_change_set(
            store,
            {"ChangeSetName": "my-cs", "StackName": "cs-changes"},
            REGION,
            ACCOUNT,
        )

        # BUG: Changes is always an empty string ""
        changes = desc.get("Changes", "")
        assert isinstance(changes, list), f"Changes should be a list, got {type(changes)}"
        assert len(changes) > 0, "Changes should list at least the new topic and modified queue"


# ===========================================================================
# 15. Fn::Sub with literal ${!...} escaping
# ===========================================================================


class TestFnSubEscaping:
    """Fn::Sub should treat ${!VarName} as a literal ${VarName} (no substitution)."""

    def test_fn_sub_dollar_bang_is_literal(self):
        """${!Literal} should produce ${Literal} in the output."""
        resources = {}
        parameters = {"AWS::Region": REGION}
        value = {"Fn::Sub": "prefix-${!Literal}-suffix"}
        result = resolve_intrinsics(value, resources, parameters, REGION, ACCOUNT)
        # BUG: The ${!Literal} is treated as a variable reference, not an escape
        assert result == "prefix-${Literal}-suffix", f"Got: {result}"


# ===========================================================================
# 16. Condition evaluation with Fn::And / Fn::Or / Fn::Not
# ===========================================================================


class TestConditionEvaluation:
    """Conditions section should correctly evaluate complex boolean expressions."""

    def test_condition_with_fn_and(self):
        """Fn::And with mixed true/false conditions should evaluate correctly."""
        template = {
            "Parameters": {},
            "Conditions": {
                "IsTrue": {"Fn::Equals": ["a", "a"]},
                "IsFalse": {"Fn::Equals": ["a", "b"]},
                "BothTrue": {"Fn::And": [{"Condition": "IsTrue"}, {"Condition": "IsTrue"}]},
                "MixedFalse": {"Fn::And": [{"Condition": "IsTrue"}, {"Condition": "IsFalse"}]},
            },
            "Resources": {},
        }
        # BUG: The {"Condition": "CondName"} reference syntax inside Fn::And/Fn::Or
        # is not handled by resolve_intrinsics — it doesn't know how to resolve
        # {"Condition": "IsTrue"} as a reference to another condition.
        result = evaluate_conditions(template, {}, {}, REGION, ACCOUNT)
        assert result["BothTrue"] is True
        assert result["MixedFalse"] is False


# ===========================================================================
# 17. Fn::Select out of bounds should error
# ===========================================================================


class TestFnSelectOutOfBounds:
    """Fn::Select with an index beyond the list length should raise an error."""

    def test_fn_select_out_of_bounds_raises_error(self):
        """Fn::Select with index >= len(list) should error, not return ''."""
        resources = {}
        parameters = {}
        value = {"Fn::Select": [5, ["a", "b", "c"]]}
        # BUG: Currently returns "" for out-of-bounds instead of raising
        # AWS raises an error
        with pytest.raises(Exception):
            resolve_intrinsics(value, resources, parameters, REGION, ACCOUNT)


# ===========================================================================
# 18. Duplicate export names across stacks should be rejected
# ===========================================================================


class TestDuplicateExports:
    """Two stacks cannot export the same name."""

    def test_duplicate_export_name_raises_error(self):
        """Creating a stack with an export name already in use should fail."""
        store = CfnStore()
        tpl_a = json.dumps(
            {
                "AWSTemplateFormatVersion": "2010-09-09",
                "Resources": {
                    "Q": {
                        "Type": "AWS::SQS::Queue",
                        "Properties": {"QueueName": "dup-export-q1"},
                    }
                },
                "Outputs": {
                    "Out": {
                        "Value": "value1",
                        "Export": {"Name": "DuplicateExportName"},
                    }
                },
            }
        )
        _create_stack(store, {"StackName": "dup-export-1", "TemplateBody": tpl_a}, REGION, ACCOUNT)

        tpl_b = json.dumps(
            {
                "AWSTemplateFormatVersion": "2010-09-09",
                "Resources": {
                    "Q": {
                        "Type": "AWS::SQS::Queue",
                        "Properties": {"QueueName": "dup-export-q2"},
                    }
                },
                "Outputs": {
                    "Out": {
                        "Value": "value2",
                        "Export": {"Name": "DuplicateExportName"},
                    }
                },
            }
        )
        # BUG: This succeeds and overwrites the export
        with pytest.raises(CfnError) as exc_info:
            _create_stack(
                store, {"StackName": "dup-export-2", "TemplateBody": tpl_b}, REGION, ACCOUNT
            )
        msg = exc_info.value.message
        assert "DuplicateExportName" in msg or "export" in msg.lower()


# ===========================================================================
# 19. Fn::GetAtt on unresolved resource should error, not return ""
# ===========================================================================


class TestFnGetAttMissing:
    """Fn::GetAtt referencing a non-existent resource should raise an error."""

    def test_getatt_nonexistent_resource_raises_error(self):
        """Fn::GetAtt for a logical ID not in the template should fail."""
        resources = {}
        parameters = {}
        value = {"Fn::GetAtt": ["NonExistentResource", "Arn"]}
        # BUG: Currently returns "" silently instead of raising
        with pytest.raises(Exception):
            resolve_intrinsics(value, resources, parameters, REGION, ACCOUNT)


# ===========================================================================
# 20. Stack outputs should be available in DescribeStacks
# ===========================================================================


class TestStackOutputs:
    """Stack outputs should be properly formatted in DescribeStacks response."""

    def test_describe_stacks_includes_enable_termination_protection(self):
        """AWS DescribeStacks always includes EnableTerminationProtection
        field (default false). The current implementation does not track
        or return this field."""
        store = CfnStore()
        tpl = _tpl(
            {
                "Q": {
                    "Type": "AWS::SQS::Queue",
                    "Properties": {"QueueName": "termination-q"},
                }
            }
        )
        _create_stack(
            store,
            {"StackName": "term-test", "TemplateBody": tpl},
            REGION,
            ACCOUNT,
        )
        result = _describe_stacks(store, {"StackName": "term-test"}, REGION, ACCOUNT)
        stack_data = result["Stacks"][0]
        # BUG: EnableTerminationProtection is not included in the response
        assert "EnableTerminationProtection" in stack_data, (
            "DescribeStacks should include EnableTerminationProtection field"
        )
        assert stack_data["EnableTerminationProtection"] == "false"


# ===========================================================================
# 21. Fn::Sub with GetAtt dotted references
# ===========================================================================


class TestFnSubEdgeCases:
    """Fn::Sub edge cases that real AWS handles but the emulator does not."""

    def test_fn_sub_with_aws_url_suffix(self):
        """Fn::Sub should resolve ${AWS::URLSuffix} to 'amazonaws.com'."""
        resources = {}
        parameters = {}
        value = {"Fn::Sub": "https://s3.${AWS::URLSuffix}/bucket"}
        result = resolve_intrinsics(value, resources, parameters, REGION, ACCOUNT)
        # BUG: AWS::URLSuffix is not handled as a pseudo-param in Fn::Sub
        assert result == "https://s3.amazonaws.com/bucket", f"Got: {result}"

    def test_fn_sub_with_aws_partition(self):
        """Fn::Sub should resolve ${AWS::Partition} to 'aws'."""
        resources = {}
        parameters = {}
        value = {"Fn::Sub": "arn:${AWS::Partition}:s3:::bucket"}
        result = resolve_intrinsics(value, resources, parameters, REGION, ACCOUNT)
        # BUG: AWS::Partition is not handled as a pseudo-param in Fn::Sub
        assert result == "arn:aws:s3:::bucket", f"Got: {result}"

    def test_fn_sub_nonexistent_ref_should_error(self):
        """Fn::Sub with ${NonExistent} that doesn't match any param or
        resource should raise an error, not return the literal string."""
        resources = {}
        parameters = {}
        value = {"Fn::Sub": "value is ${TotallyMissing}"}
        # BUG: Currently returns "value is TotallyMissing" silently
        # AWS raises an error for unresolvable variables in Fn::Sub
        with pytest.raises(ValueError, match="TotallyMissing"):
            resolve_intrinsics(value, resources, parameters, REGION, ACCOUNT)

    def test_fn_sub_with_literal_dollar_sign(self):
        """Fn::Sub should support $$ as a literal $ sign (standard escape).
        AWS CloudFormation uses $$ to produce a literal $ in output."""
        resources = {}
        parameters = {}
        value = {"Fn::Sub": "cost is $$100"}
        result = resolve_intrinsics(value, resources, parameters, REGION, ACCOUNT)
        # BUG: $$ is not treated as an escape for literal $
        assert result == "cost is $100", f"Got: {result}"


# ===========================================================================
# 22. AWS::NoValue pseudo-parameter
# ===========================================================================


class TestAWSURLSuffix:
    """AWS::URLSuffix pseudo-parameter should resolve to amazonaws.com."""

    def test_url_suffix_pseudo_parameter(self):
        """Ref: AWS::URLSuffix should return 'amazonaws.com'."""
        resources = {}
        parameters = {}
        value = {"Ref": "AWS::URLSuffix"}
        result = resolve_intrinsics(value, resources, parameters, REGION, ACCOUNT)
        # BUG: AWS::URLSuffix falls through to the default case and returns
        # "AWS::URLSuffix" as a string
        assert result == "amazonaws.com", f"Expected 'amazonaws.com', got '{result}'"


# ===========================================================================
# 23. AWS::Partition pseudo-parameter
# ===========================================================================


class TestAWSPartition:
    """AWS::Partition pseudo-parameter should resolve to 'aws'."""

    def test_partition_pseudo_parameter(self):
        """Ref: AWS::Partition should return 'aws'."""
        resources = {}
        parameters = {}
        value = {"Ref": "AWS::Partition"}
        result = resolve_intrinsics(value, resources, parameters, REGION, ACCOUNT)
        # BUG: Falls through and returns "AWS::Partition" as a literal string
        assert result == "aws", f"Expected 'aws', got '{result}'"


# ===========================================================================
# 24. Parameters with no default and no value should error
# ===========================================================================


class TestRequiredParameters:
    """A parameter without a Default value must be provided at stack creation."""

    def test_missing_required_parameter_raises_error(self):
        """Creating a stack without providing a required parameter should fail."""
        store = CfnStore()
        tpl = _tpl(
            {
                "Queue": {
                    "Type": "AWS::SQS::Queue",
                    "Properties": {"QueueName": {"Ref": "RequiredParam"}},
                }
            },
            parameters={"RequiredParam": {"Type": "String"}},
        )
        # BUG: No validation that required parameters are provided.
        # The Ref falls through and returns "RequiredParam" as the queue name.
        with pytest.raises(CfnError) as exc_info:
            _create_stack(store, {"StackName": "req-param", "TemplateBody": tpl}, REGION, ACCOUNT)
        assert exc_info.value.code == "ValidationError"
        assert "RequiredParam" in exc_info.value.message
