"""Unit tests for CloudFormation ExecuteChangeSet."""

import json

import pytest

from robotocore.services.cloudformation.engine import (
    CfnChangeSet,
    CfnResource,
    CfnStack,
    CfnStore,
)
from robotocore.services.cloudformation.provider import (
    CfnError,
    _create_change_set,
    _delete_change_set,
    _describe_change_set,
    _describe_stacks,
    _execute_change_set,
)

REGION = "us-east-1"
ACCOUNT = "123456789012"

SIMPLE_SQS_TEMPLATE = json.dumps(
    {
        "AWSTemplateFormatVersion": "2010-09-09",
        "Resources": {
            "MyQueue": {
                "Type": "AWS::SQS::Queue",
                "Properties": {"QueueName": "test-queue-exec"},
            },
        },
    }
)

SIMPLE_SNS_TEMPLATE = json.dumps(
    {
        "AWSTemplateFormatVersion": "2010-09-09",
        "Resources": {
            "MyTopic": {
                "Type": "AWS::SNS::Topic",
                "Properties": {"TopicName": "test-topic-exec"},
            },
        },
    }
)

TWO_QUEUES_TEMPLATE = json.dumps(
    {
        "AWSTemplateFormatVersion": "2010-09-09",
        "Resources": {
            "QueueA": {
                "Type": "AWS::SQS::Queue",
                "Properties": {"QueueName": "queue-a"},
            },
            "QueueB": {
                "Type": "AWS::SQS::Queue",
                "Properties": {"QueueName": "queue-b"},
            },
        },
    }
)

PARAMETERIZED_TEMPLATE = json.dumps(
    {
        "AWSTemplateFormatVersion": "2010-09-09",
        "Parameters": {
            "QueueName": {"Type": "String", "Default": "default-queue"},
        },
        "Resources": {
            "MyQueue": {
                "Type": "AWS::SQS::Queue",
                "Properties": {"QueueName": {"Ref": "QueueName"}},
            },
        },
    }
)


def _make_store_with_stack(
    stack_name: str = "my-stack",
    template: str = SIMPLE_SQS_TEMPLATE,
    status: str = "CREATE_COMPLETE",
) -> CfnStore:
    """Helper: create a CfnStore with a pre-existing stack."""
    store = CfnStore()
    stack = CfnStack(
        stack_id=f"arn:aws:cloudformation:{REGION}:{ACCOUNT}:stack/{stack_name}/fake-id",
        stack_name=stack_name,
        template_body=template,
        status=status,
    )
    store.put_stack(stack)
    return store


# ===========================================================================
# CreateChangeSet tests
# ===========================================================================


class TestCreateChangeSet:
    """Tests for _create_change_set."""

    def test_create_changeset_returns_id_and_stack_id(self):
        store = CfnStore()
        result = _create_change_set(
            store,
            {
                "StackName": "cs-stack",
                "ChangeSetName": "cs-1",
                "TemplateBody": SIMPLE_SQS_TEMPLATE,
                "ChangeSetType": "CREATE",
            },
            REGION,
            ACCOUNT,
        )
        assert "Id" in result
        assert "StackId" in result
        assert "changeSet/cs-1/" in result["Id"]
        assert "stack/cs-stack/" in result["StackId"]

    def test_create_changeset_type_create_creates_stub_stack(self):
        """CREATE type should create a REVIEW_IN_PROGRESS stub stack."""
        store = CfnStore()
        _create_change_set(
            store,
            {
                "StackName": "stub-test",
                "ChangeSetName": "cs-1",
                "TemplateBody": SIMPLE_SQS_TEMPLATE,
                "ChangeSetType": "CREATE",
            },
            REGION,
            ACCOUNT,
        )
        stack = store.get_stack("stub-test")
        assert stack is not None
        assert stack.status == "REVIEW_IN_PROGRESS"
        assert stack.template_body == SIMPLE_SQS_TEMPLATE

    def test_create_changeset_type_update_uses_existing_stack(self):
        """UPDATE type should use the existing stack's ID."""
        store = _make_store_with_stack("upd-stack")
        existing_stack = store.get_stack("upd-stack")
        result = _create_change_set(
            store,
            {
                "StackName": "upd-stack",
                "ChangeSetName": "cs-upd",
                "TemplateBody": SIMPLE_SNS_TEMPLATE,
                "ChangeSetType": "UPDATE",
            },
            REGION,
            ACCOUNT,
        )
        assert result["StackId"] == existing_stack.stack_id

    def test_create_changeset_stored_in_store(self):
        store = CfnStore()
        result = _create_change_set(
            store,
            {
                "StackName": "stored-test",
                "ChangeSetName": "cs-stored",
                "TemplateBody": SIMPLE_SQS_TEMPLATE,
                "ChangeSetType": "CREATE",
            },
            REGION,
            ACCOUNT,
        )
        cs_id = result["Id"]
        assert cs_id in store.change_sets
        cs = store.change_sets[cs_id]
        assert cs.change_set_name == "cs-stored"
        assert cs.stack_name == "stored-test"
        assert cs.template_body == SIMPLE_SQS_TEMPLATE
        assert cs.status == "CREATE_COMPLETE"

    def test_create_changeset_with_parameters(self):
        store = CfnStore()
        _create_change_set(
            store,
            {
                "StackName": "param-cs",
                "ChangeSetName": "cs-p",
                "TemplateBody": PARAMETERIZED_TEMPLATE,
                "ChangeSetType": "CREATE",
                "Parameters.member.1.ParameterKey": "QueueName",
                "Parameters.member.1.ParameterValue": "my-custom-q",
            },
            REGION,
            ACCOUNT,
        )
        cs = list(store.change_sets.values())[0]
        assert cs.parameters == {"QueueName": "my-custom-q"}

    def test_create_changeset_missing_stack_name_raises(self):
        store = CfnStore()
        with pytest.raises(CfnError) as exc_info:
            _create_change_set(
                store,
                {"ChangeSetName": "cs-1", "TemplateBody": SIMPLE_SQS_TEMPLATE},
                REGION,
                ACCOUNT,
            )
        assert exc_info.value.code == "ValidationError"
        assert "StackName" in exc_info.value.message

    def test_create_changeset_missing_name_raises(self):
        store = CfnStore()
        with pytest.raises(CfnError) as exc_info:
            _create_change_set(
                store,
                {"StackName": "my-stack", "TemplateBody": SIMPLE_SQS_TEMPLATE},
                REGION,
                ACCOUNT,
            )
        assert exc_info.value.code == "ValidationError"
        assert "ChangeSetName" in exc_info.value.message

    def test_create_changeset_default_type_is_update(self):
        """Default ChangeSetType should be UPDATE when not specified."""
        store = _make_store_with_stack("default-type")
        _create_change_set(
            store,
            {
                "StackName": "default-type",
                "ChangeSetName": "cs-default",
                "TemplateBody": SIMPLE_SQS_TEMPLATE,
            },
            REGION,
            ACCOUNT,
        )
        cs = list(store.change_sets.values())[0]
        assert cs.change_set_type == "UPDATE"


# ===========================================================================
# ExecuteChangeSet CREATE type tests
# ===========================================================================


class TestExecuteChangeSetCreate:
    """Tests for ExecuteChangeSet with CREATE type."""

    def test_execute_create_changeset_deploys_resources(self):
        """Create change set + execute -> stack status is CREATE_COMPLETE with resources."""
        store = CfnStore()
        cs_result = _create_change_set(
            store,
            {
                "StackName": "exec-test-stack",
                "ChangeSetName": "my-cs",
                "TemplateBody": SIMPLE_SQS_TEMPLATE,
                "ChangeSetType": "CREATE",
            },
            REGION,
            ACCOUNT,
        )
        assert "Id" in cs_result
        assert "StackId" in cs_result

        # Verify stub stack is in REVIEW_IN_PROGRESS
        stack = store.get_stack("exec-test-stack")
        assert stack is not None
        assert stack.status == "REVIEW_IN_PROGRESS"

        # Execute the change set
        _execute_change_set(
            store,
            {"ChangeSetName": "my-cs", "StackName": "exec-test-stack"},
            REGION,
            ACCOUNT,
        )

        # Verify stack is now CREATE_COMPLETE
        stack = store.get_stack("exec-test-stack")
        assert stack.status == "CREATE_COMPLETE"
        assert "MyQueue" in stack.resources
        assert stack.resources["MyQueue"].resource_type == "AWS::SQS::Queue"
        assert stack.resources["MyQueue"].physical_id is not None

    def test_execute_create_changeset_sets_changeset_status(self):
        """After execution, change set status should be EXECUTE_COMPLETE."""
        store = CfnStore()
        _create_change_set(
            store,
            {
                "StackName": "cs-status-test",
                "ChangeSetName": "my-cs",
                "TemplateBody": SIMPLE_SQS_TEMPLATE,
                "ChangeSetType": "CREATE",
            },
            REGION,
            ACCOUNT,
        )
        _execute_change_set(
            store,
            {"ChangeSetName": "my-cs", "StackName": "cs-status-test"},
            REGION,
            ACCOUNT,
        )

        cs = None
        for c in store.change_sets.values():
            if c.change_set_name == "my-cs":
                cs = c
                break
        assert cs is not None
        assert cs.status == "EXECUTE_COMPLETE"

    def test_execute_create_changeset_generates_events(self):
        """Executing a CREATE change set should generate stack events."""
        store = CfnStore()
        _create_change_set(
            store,
            {
                "StackName": "events-test",
                "ChangeSetName": "ev-cs",
                "TemplateBody": SIMPLE_SQS_TEMPLATE,
                "ChangeSetType": "CREATE",
            },
            REGION,
            ACCOUNT,
        )
        _execute_change_set(
            store,
            {"ChangeSetName": "ev-cs", "StackName": "events-test"},
            REGION,
            ACCOUNT,
        )
        stack = store.get_stack("events-test")
        assert len(stack.events) > 0

        # Should have CREATE_IN_PROGRESS and CREATE_COMPLETE events for the stack
        statuses = [e["ResourceStatus"] for e in stack.events]
        assert "CREATE_IN_PROGRESS" in statuses
        assert "CREATE_COMPLETE" in statuses

    def test_execute_create_changeset_with_multiple_resources(self):
        """CREATE change set with two resources deploys both."""
        store = CfnStore()
        _create_change_set(
            store,
            {
                "StackName": "multi-res",
                "ChangeSetName": "multi-cs",
                "TemplateBody": TWO_QUEUES_TEMPLATE,
                "ChangeSetType": "CREATE",
            },
            REGION,
            ACCOUNT,
        )
        _execute_change_set(
            store,
            {"ChangeSetName": "multi-cs", "StackName": "multi-res"},
            REGION,
            ACCOUNT,
        )
        stack = store.get_stack("multi-res")
        assert stack.status == "CREATE_COMPLETE"
        assert "QueueA" in stack.resources
        assert "QueueB" in stack.resources
        assert stack.resources["QueueA"].physical_id is not None
        assert stack.resources["QueueB"].physical_id is not None

    def test_execute_create_changeset_updates_template_on_stack(self):
        """The stack's template_body should match the change set's template."""
        store = CfnStore()
        _create_change_set(
            store,
            {
                "StackName": "tmpl-test",
                "ChangeSetName": "tmpl-cs",
                "TemplateBody": SIMPLE_SQS_TEMPLATE,
                "ChangeSetType": "CREATE",
            },
            REGION,
            ACCOUNT,
        )
        _execute_change_set(
            store,
            {"ChangeSetName": "tmpl-cs", "StackName": "tmpl-test"},
            REGION,
            ACCOUNT,
        )
        stack = store.get_stack("tmpl-test")
        assert stack.template_body == SIMPLE_SQS_TEMPLATE

    def test_execute_create_changeset_lookup_by_id(self):
        """ExecuteChangeSet should work when ChangeSetName is a full ARN/ID."""
        store = CfnStore()
        result = _create_change_set(
            store,
            {
                "StackName": "id-lookup",
                "ChangeSetName": "cs-by-id",
                "TemplateBody": SIMPLE_SQS_TEMPLATE,
                "ChangeSetType": "CREATE",
            },
            REGION,
            ACCOUNT,
        )
        cs_id = result["Id"]

        # Execute using the full change set ID
        _execute_change_set(
            store,
            {"ChangeSetName": cs_id, "StackName": "id-lookup"},
            REGION,
            ACCOUNT,
        )
        stack = store.get_stack("id-lookup")
        assert stack.status == "CREATE_COMPLETE"


# ===========================================================================
# ExecuteChangeSet UPDATE type tests
# ===========================================================================


class TestExecuteChangeSetUpdate:
    """Tests for ExecuteChangeSet with UPDATE type."""

    def test_execute_update_changeset_replaces_resources(self):
        """UPDATE change set replaces stack resources with new template."""
        store = _make_store_with_stack("upd-test")

        _create_change_set(
            store,
            {
                "StackName": "upd-test",
                "ChangeSetName": "update-cs",
                "TemplateBody": SIMPLE_SNS_TEMPLATE,
                "ChangeSetType": "UPDATE",
            },
            REGION,
            ACCOUNT,
        )
        _execute_change_set(
            store,
            {"ChangeSetName": "update-cs", "StackName": "upd-test"},
            REGION,
            ACCOUNT,
        )

        stack = store.get_stack("upd-test")
        assert stack.status == "UPDATE_COMPLETE"
        assert "MyTopic" in stack.resources
        assert stack.resources["MyTopic"].resource_type == "AWS::SNS::Topic"

    def test_execute_update_changeset_clears_old_resources(self):
        """UPDATE should remove old resources and only have new ones."""
        store = _make_store_with_stack("clear-res")
        # Manually add a resource to the existing stack
        existing = store.get_stack("clear-res")
        existing.resources["OldQueue"] = CfnResource(
            logical_id="OldQueue",
            resource_type="AWS::SQS::Queue",
            properties={},
            physical_id="old-queue-phys-id",
            status="CREATE_COMPLETE",
        )

        _create_change_set(
            store,
            {
                "StackName": "clear-res",
                "ChangeSetName": "upd-clear",
                "TemplateBody": SIMPLE_SNS_TEMPLATE,
                "ChangeSetType": "UPDATE",
            },
            REGION,
            ACCOUNT,
        )
        _execute_change_set(
            store,
            {"ChangeSetName": "upd-clear", "StackName": "clear-res"},
            REGION,
            ACCOUNT,
        )

        stack = store.get_stack("clear-res")
        assert stack.status == "UPDATE_COMPLETE"
        # Old resource should be gone
        assert "OldQueue" not in stack.resources
        # New resource should be present
        assert "MyTopic" in stack.resources

    def test_execute_update_changeset_clears_old_outputs(self):
        """UPDATE should clear old outputs before deploying."""
        store = _make_store_with_stack("clear-out")
        existing = store.get_stack("clear-out")
        existing.outputs = {"OldOutput": {"OutputKey": "OldOutput", "OutputValue": "old-val"}}

        _create_change_set(
            store,
            {
                "StackName": "clear-out",
                "ChangeSetName": "upd-out",
                "TemplateBody": SIMPLE_SQS_TEMPLATE,
                "ChangeSetType": "UPDATE",
            },
            REGION,
            ACCOUNT,
        )
        _execute_change_set(
            store,
            {"ChangeSetName": "upd-out", "StackName": "clear-out"},
            REGION,
            ACCOUNT,
        )

        stack = store.get_stack("clear-out")
        assert stack.status == "UPDATE_COMPLETE"
        # Old outputs should be cleared (SQS template has no outputs)
        assert "OldOutput" not in stack.outputs

    def test_execute_update_changeset_generates_update_events(self):
        """UPDATE change set should produce UPDATE_IN_PROGRESS and UPDATE_COMPLETE events."""
        store = _make_store_with_stack("upd-events")

        _create_change_set(
            store,
            {
                "StackName": "upd-events",
                "ChangeSetName": "ev-upd",
                "TemplateBody": SIMPLE_SNS_TEMPLATE,
                "ChangeSetType": "UPDATE",
            },
            REGION,
            ACCOUNT,
        )
        _execute_change_set(
            store,
            {"ChangeSetName": "ev-upd", "StackName": "upd-events"},
            REGION,
            ACCOUNT,
        )

        stack = store.get_stack("upd-events")
        statuses = [e["ResourceStatus"] for e in stack.events]
        assert "UPDATE_IN_PROGRESS" in statuses
        assert "UPDATE_COMPLETE" in statuses

    def test_execute_update_changeset_updates_template_body(self):
        """After update, stack.template_body should be the new template."""
        store = _make_store_with_stack("tmpl-upd")

        _create_change_set(
            store,
            {
                "StackName": "tmpl-upd",
                "ChangeSetName": "tmpl-cs",
                "TemplateBody": SIMPLE_SNS_TEMPLATE,
                "ChangeSetType": "UPDATE",
            },
            REGION,
            ACCOUNT,
        )
        _execute_change_set(
            store,
            {"ChangeSetName": "tmpl-cs", "StackName": "tmpl-upd"},
            REGION,
            ACCOUNT,
        )

        stack = store.get_stack("tmpl-upd")
        assert stack.template_body == SIMPLE_SNS_TEMPLATE

    def test_execute_update_changeset_with_parameters(self):
        """UPDATE change set passes parameters to the updated stack."""
        store = _make_store_with_stack("upd-param", template=PARAMETERIZED_TEMPLATE)

        _create_change_set(
            store,
            {
                "StackName": "upd-param",
                "ChangeSetName": "upd-p-cs",
                "TemplateBody": PARAMETERIZED_TEMPLATE,
                "ChangeSetType": "UPDATE",
                "Parameters.member.1.ParameterKey": "QueueName",
                "Parameters.member.1.ParameterValue": "updated-queue",
            },
            REGION,
            ACCOUNT,
        )
        _execute_change_set(
            store,
            {"ChangeSetName": "upd-p-cs", "StackName": "upd-param"},
            REGION,
            ACCOUNT,
        )

        stack = store.get_stack("upd-param")
        assert stack.status == "UPDATE_COMPLETE"
        assert stack.parameters.get("QueueName") == "updated-queue"


# ===========================================================================
# Status transition tests
# ===========================================================================


class TestChangeSetStatusTransitions:
    """Tests verifying correct status transitions during change set lifecycle."""

    def test_create_changeset_initial_status_is_create_complete(self):
        """Newly created change set has status CREATE_COMPLETE."""
        store = CfnStore()
        _create_change_set(
            store,
            {
                "StackName": "status-init",
                "ChangeSetName": "st-cs",
                "TemplateBody": SIMPLE_SQS_TEMPLATE,
                "ChangeSetType": "CREATE",
            },
            REGION,
            ACCOUNT,
        )
        cs = list(store.change_sets.values())[0]
        assert cs.status == "CREATE_COMPLETE"

    def test_after_execute_changeset_status_is_execute_complete(self):
        """After execution, change set status transitions to EXECUTE_COMPLETE."""
        store = CfnStore()
        _create_change_set(
            store,
            {
                "StackName": "status-exec",
                "ChangeSetName": "st-cs",
                "TemplateBody": SIMPLE_SQS_TEMPLATE,
                "ChangeSetType": "CREATE",
            },
            REGION,
            ACCOUNT,
        )
        _execute_change_set(
            store,
            {"ChangeSetName": "st-cs", "StackName": "status-exec"},
            REGION,
            ACCOUNT,
        )
        cs = list(store.change_sets.values())[0]
        assert cs.status == "EXECUTE_COMPLETE"

    def test_create_type_stack_transitions_review_to_create_complete(self):
        """CREATE type: stack goes REVIEW_IN_PROGRESS -> CREATE_COMPLETE."""
        store = CfnStore()
        _create_change_set(
            store,
            {
                "StackName": "transition-test",
                "ChangeSetName": "t-cs",
                "TemplateBody": SIMPLE_SQS_TEMPLATE,
                "ChangeSetType": "CREATE",
            },
            REGION,
            ACCOUNT,
        )

        stack = store.get_stack("transition-test")
        assert stack.status == "REVIEW_IN_PROGRESS"

        _execute_change_set(
            store,
            {"ChangeSetName": "t-cs", "StackName": "transition-test"},
            REGION,
            ACCOUNT,
        )

        stack = store.get_stack("transition-test")
        assert stack.status == "CREATE_COMPLETE"

    def test_update_type_stack_transitions_to_update_complete(self):
        """UPDATE type: stack goes CREATE_COMPLETE -> UPDATE_COMPLETE."""
        store = _make_store_with_stack("upd-trans")

        _create_change_set(
            store,
            {
                "StackName": "upd-trans",
                "ChangeSetName": "upd-t-cs",
                "TemplateBody": SIMPLE_SNS_TEMPLATE,
                "ChangeSetType": "UPDATE",
            },
            REGION,
            ACCOUNT,
        )
        _execute_change_set(
            store,
            {"ChangeSetName": "upd-t-cs", "StackName": "upd-trans"},
            REGION,
            ACCOUNT,
        )

        stack = store.get_stack("upd-trans")
        assert stack.status == "UPDATE_COMPLETE"


# ===========================================================================
# Error cases
# ===========================================================================


class TestExecuteChangeSetErrors:
    """Tests for error cases in ExecuteChangeSet."""

    def test_execute_nonexistent_changeset_raises_error(self):
        """Executing a nonexistent change set raises ChangeSetNotFoundException."""
        store = CfnStore()
        with pytest.raises(CfnError) as exc_info:
            _execute_change_set(
                store,
                {"ChangeSetName": "does-not-exist", "StackName": "no-stack"},
                REGION,
                ACCOUNT,
            )
        assert exc_info.value.code == "ChangeSetNotFoundException"
        assert exc_info.value.status == 404

    def test_execute_already_executed_changeset_raises_error(self):
        """Executing an already-executed change set raises InvalidChangeSetStatusException."""
        store = CfnStore()
        _create_change_set(
            store,
            {
                "StackName": "double-exec",
                "ChangeSetName": "my-cs",
                "TemplateBody": SIMPLE_SQS_TEMPLATE,
                "ChangeSetType": "CREATE",
            },
            REGION,
            ACCOUNT,
        )
        _execute_change_set(
            store,
            {"ChangeSetName": "my-cs", "StackName": "double-exec"},
            REGION,
            ACCOUNT,
        )

        with pytest.raises(CfnError) as exc_info:
            _execute_change_set(
                store,
                {"ChangeSetName": "my-cs", "StackName": "double-exec"},
                REGION,
                ACCOUNT,
            )
        assert exc_info.value.code == "InvalidChangeSetStatusException"

    def test_execute_changeset_nonexistent_stack_raises_for_update(self):
        """UPDATE change set on a nonexistent stack should raise ValidationError."""
        store = CfnStore()
        # Manually create a change set pointing to a nonexistent stack
        cs = CfnChangeSet(
            change_set_id="arn:aws:cloudformation:us-east-1:123456789012:changeSet/orphan/id",
            change_set_name="orphan-cs",
            stack_name="ghost-stack",
            template_body=SIMPLE_SQS_TEMPLATE,
            change_set_type="UPDATE",
            status="CREATE_COMPLETE",
        )
        store.change_sets[cs.change_set_id] = cs

        with pytest.raises(CfnError) as exc_info:
            _execute_change_set(
                store,
                {"ChangeSetName": cs.change_set_id},
                REGION,
                ACCOUNT,
            )
        assert exc_info.value.code == "ValidationError"

    def test_execute_changeset_nonexistent_stack_raises_for_create(self):
        """CREATE change set where the stub was deleted should raise ValidationError."""
        store = CfnStore()
        # Create a change set that references a stack name, but no stub exists
        cs = CfnChangeSet(
            change_set_id="arn:aws:cloudformation:us-east-1:123456789012:changeSet/no-stub/id",
            change_set_name="no-stub-cs",
            stack_name="deleted-stub",
            template_body=SIMPLE_SQS_TEMPLATE,
            change_set_type="CREATE",
            status="CREATE_COMPLETE",
        )
        store.change_sets[cs.change_set_id] = cs

        with pytest.raises(CfnError) as exc_info:
            _execute_change_set(
                store,
                {"ChangeSetName": cs.change_set_id},
                REGION,
                ACCOUNT,
            )
        assert exc_info.value.code == "ValidationError"

    def test_execute_changeset_with_parameters(self):
        """Change set with parameters passes them through to stack deployment."""
        store = CfnStore()
        _create_change_set(
            store,
            {
                "StackName": "param-test",
                "ChangeSetName": "param-cs",
                "TemplateBody": PARAMETERIZED_TEMPLATE,
                "ChangeSetType": "CREATE",
                "Parameters.member.1.ParameterKey": "QueueName",
                "Parameters.member.1.ParameterValue": "custom-queue-name",
            },
            REGION,
            ACCOUNT,
        )

        _execute_change_set(
            store,
            {"ChangeSetName": "param-cs", "StackName": "param-test"},
            REGION,
            ACCOUNT,
        )

        stack = store.get_stack("param-test")
        assert stack.status == "CREATE_COMPLETE"
        assert stack.parameters.get("QueueName") == "custom-queue-name"


# ===========================================================================
# DescribeChangeSet tests
# ===========================================================================


class TestDescribeChangeSet:
    """Tests for _describe_change_set."""

    def test_describe_changeset_by_name(self):
        store = CfnStore()
        result = _create_change_set(
            store,
            {
                "StackName": "desc-stack",
                "ChangeSetName": "desc-cs",
                "TemplateBody": SIMPLE_SQS_TEMPLATE,
                "ChangeSetType": "CREATE",
            },
            REGION,
            ACCOUNT,
        )
        desc = _describe_change_set(
            store,
            {"ChangeSetName": "desc-cs", "StackName": "desc-stack"},
            REGION,
            ACCOUNT,
        )
        assert desc["ChangeSetName"] == "desc-cs"
        assert desc["StackName"] == "desc-stack"
        assert desc["Status"] == "CREATE_COMPLETE"
        assert desc["ChangeSetId"] == result["Id"]

    def test_describe_changeset_by_id(self):
        store = CfnStore()
        result = _create_change_set(
            store,
            {
                "StackName": "desc-id-stack",
                "ChangeSetName": "desc-id-cs",
                "TemplateBody": SIMPLE_SQS_TEMPLATE,
                "ChangeSetType": "CREATE",
            },
            REGION,
            ACCOUNT,
        )
        cs_id = result["Id"]
        desc = _describe_change_set(
            store,
            {"ChangeSetName": cs_id},
            REGION,
            ACCOUNT,
        )
        assert desc["ChangeSetId"] == cs_id
        assert desc["ChangeSetName"] == "desc-id-cs"

    def test_describe_nonexistent_changeset_raises(self):
        store = CfnStore()
        with pytest.raises(CfnError) as exc_info:
            _describe_change_set(
                store,
                {"ChangeSetName": "no-such-cs"},
                REGION,
                ACCOUNT,
            )
        assert exc_info.value.code == "ChangeSetNotFoundException"
        assert exc_info.value.status == 404

    def test_describe_changeset_after_execution_shows_execute_complete(self):
        store = CfnStore()
        _create_change_set(
            store,
            {
                "StackName": "desc-exec",
                "ChangeSetName": "desc-exec-cs",
                "TemplateBody": SIMPLE_SQS_TEMPLATE,
                "ChangeSetType": "CREATE",
            },
            REGION,
            ACCOUNT,
        )
        _execute_change_set(
            store,
            {"ChangeSetName": "desc-exec-cs", "StackName": "desc-exec"},
            REGION,
            ACCOUNT,
        )
        desc = _describe_change_set(
            store,
            {"ChangeSetName": "desc-exec-cs", "StackName": "desc-exec"},
            REGION,
            ACCOUNT,
        )
        assert desc["Status"] == "EXECUTE_COMPLETE"


# ===========================================================================
# DeleteChangeSet tests
# ===========================================================================


class TestDeleteChangeSet:
    """Tests for _delete_change_set."""

    def test_delete_changeset_removes_from_store(self):
        store = CfnStore()
        result = _create_change_set(
            store,
            {
                "StackName": "del-stack",
                "ChangeSetName": "del-cs",
                "TemplateBody": SIMPLE_SQS_TEMPLATE,
                "ChangeSetType": "CREATE",
            },
            REGION,
            ACCOUNT,
        )
        cs_id = result["Id"]
        assert cs_id in store.change_sets

        _delete_change_set(
            store,
            {"ChangeSetName": "del-cs", "StackName": "del-stack"},
            REGION,
            ACCOUNT,
        )
        # Change set should be removed
        found = any(c.change_set_name == "del-cs" for c in store.change_sets.values())
        assert not found

    def test_delete_changeset_by_id(self):
        store = CfnStore()
        result = _create_change_set(
            store,
            {
                "StackName": "del-id-stack",
                "ChangeSetName": "del-id-cs",
                "TemplateBody": SIMPLE_SQS_TEMPLATE,
                "ChangeSetType": "CREATE",
            },
            REGION,
            ACCOUNT,
        )
        cs_id = result["Id"]
        _delete_change_set(
            store,
            {"ChangeSetName": cs_id},
            REGION,
            ACCOUNT,
        )
        assert cs_id not in store.change_sets

    def test_delete_nonexistent_changeset_is_noop(self):
        """Deleting a nonexistent change set should not raise."""
        store = CfnStore()
        result = _delete_change_set(
            store,
            {"ChangeSetName": "no-such", "StackName": "no-stack"},
            REGION,
            ACCOUNT,
        )
        assert result == {}


# ===========================================================================
# Full lifecycle tests (create -> describe -> execute -> describe)
# ===========================================================================


class TestChangeSetFullLifecycle:
    """End-to-end lifecycle tests."""

    def test_full_create_lifecycle(self):
        """Create change set -> describe -> execute -> describe stack."""
        store = CfnStore()

        # Step 1: Create change set
        cs_result = _create_change_set(
            store,
            {
                "StackName": "lifecycle-stack",
                "ChangeSetName": "lifecycle-cs",
                "TemplateBody": SIMPLE_SQS_TEMPLATE,
                "ChangeSetType": "CREATE",
            },
            REGION,
            ACCOUNT,
        )
        cs_id = cs_result["Id"]

        # Step 2: Describe change set - should be CREATE_COMPLETE
        desc = _describe_change_set(
            store,
            {"ChangeSetName": cs_id},
            REGION,
            ACCOUNT,
        )
        assert desc["Status"] == "CREATE_COMPLETE"

        # Step 3: Execute change set
        _execute_change_set(
            store,
            {"ChangeSetName": cs_id},
            REGION,
            ACCOUNT,
        )

        # Step 4: Describe change set after execution
        desc = _describe_change_set(
            store,
            {"ChangeSetName": cs_id},
            REGION,
            ACCOUNT,
        )
        assert desc["Status"] == "EXECUTE_COMPLETE"

        # Step 5: Describe the stack
        stacks = _describe_stacks(store, {"StackName": "lifecycle-stack"}, REGION, ACCOUNT)
        assert len(stacks["Stacks"]) == 1
        assert stacks["Stacks"][0]["StackStatus"] == "CREATE_COMPLETE"

    def test_full_update_lifecycle(self):
        """Create stack -> create UPDATE change set -> execute -> verify update."""
        store = _make_store_with_stack("upd-lifecycle", template=SIMPLE_SQS_TEMPLATE)

        # Create UPDATE change set
        cs_result = _create_change_set(
            store,
            {
                "StackName": "upd-lifecycle",
                "ChangeSetName": "upd-lc-cs",
                "TemplateBody": SIMPLE_SNS_TEMPLATE,
                "ChangeSetType": "UPDATE",
            },
            REGION,
            ACCOUNT,
        )
        cs_id = cs_result["Id"]

        # Execute
        _execute_change_set(
            store,
            {"ChangeSetName": cs_id},
            REGION,
            ACCOUNT,
        )

        # Verify stack was updated
        stack = store.get_stack("upd-lifecycle")
        assert stack.status == "UPDATE_COMPLETE"
        assert stack.template_body == SIMPLE_SNS_TEMPLATE
        assert "MyTopic" in stack.resources

    def test_create_execute_then_update_execute(self):
        """CREATE change set -> execute -> UPDATE change set -> execute."""
        store = CfnStore()

        # CREATE
        _create_change_set(
            store,
            {
                "StackName": "two-phase",
                "ChangeSetName": "create-cs",
                "TemplateBody": SIMPLE_SQS_TEMPLATE,
                "ChangeSetType": "CREATE",
            },
            REGION,
            ACCOUNT,
        )
        _execute_change_set(
            store,
            {"ChangeSetName": "create-cs", "StackName": "two-phase"},
            REGION,
            ACCOUNT,
        )
        stack = store.get_stack("two-phase")
        assert stack.status == "CREATE_COMPLETE"
        assert "MyQueue" in stack.resources

        # UPDATE
        _create_change_set(
            store,
            {
                "StackName": "two-phase",
                "ChangeSetName": "update-cs",
                "TemplateBody": SIMPLE_SNS_TEMPLATE,
                "ChangeSetType": "UPDATE",
            },
            REGION,
            ACCOUNT,
        )
        _execute_change_set(
            store,
            {"ChangeSetName": "update-cs", "StackName": "two-phase"},
            REGION,
            ACCOUNT,
        )
        stack = store.get_stack("two-phase")
        assert stack.status == "UPDATE_COMPLETE"
        assert "MyTopic" in stack.resources
        # Old SQS queue should be gone
        assert "MyQueue" not in stack.resources

    def test_multiple_changesets_same_stack(self):
        """Multiple change sets can be created for the same stack."""
        store = _make_store_with_stack("multi-cs-stack")

        _create_change_set(
            store,
            {
                "StackName": "multi-cs-stack",
                "ChangeSetName": "cs-alpha",
                "TemplateBody": SIMPLE_SNS_TEMPLATE,
                "ChangeSetType": "UPDATE",
            },
            REGION,
            ACCOUNT,
        )
        _create_change_set(
            store,
            {
                "StackName": "multi-cs-stack",
                "ChangeSetName": "cs-beta",
                "TemplateBody": TWO_QUEUES_TEMPLATE,
                "ChangeSetType": "UPDATE",
            },
            REGION,
            ACCOUNT,
        )

        # Both should exist
        names = {c.change_set_name for c in store.change_sets.values()}
        assert "cs-alpha" in names
        assert "cs-beta" in names

        # Execute only one
        _execute_change_set(
            store,
            {"ChangeSetName": "cs-beta", "StackName": "multi-cs-stack"},
            REGION,
            ACCOUNT,
        )
        stack = store.get_stack("multi-cs-stack")
        assert stack.status == "UPDATE_COMPLETE"
        assert "QueueA" in stack.resources
        assert "QueueB" in stack.resources


# ===========================================================================
# CfnStore tests
# ===========================================================================


class TestCfnStore:
    """Tests for CfnStore change_sets storage."""

    def test_store_change_sets_initially_empty(self):
        store = CfnStore()
        assert len(store.change_sets) == 0

    def test_store_put_and_get_change_set(self):
        store = CfnStore()
        cs = CfnChangeSet(
            change_set_id="cs-id-1",
            change_set_name="test-cs",
            stack_name="test-stack",
        )
        store.change_sets[cs.change_set_id] = cs
        assert "cs-id-1" in store.change_sets
        assert store.change_sets["cs-id-1"].change_set_name == "test-cs"

    def test_store_get_stack_returns_none_for_missing(self):
        store = CfnStore()
        assert store.get_stack("nonexistent") is None

    def test_store_get_stack_prefers_non_deleted(self):
        store = CfnStore()
        # Put a deleted stack
        deleted = CfnStack(
            stack_id="arn:old",
            stack_name="my-stack",
            template_body="{}",
            status="DELETE_COMPLETE",
        )
        store.put_stack(deleted)

        # Put a live stack with same name
        live = CfnStack(
            stack_id="arn:new",
            stack_name="my-stack",
            template_body="{}",
            status="CREATE_COMPLETE",
        )
        store.put_stack(live)

        result = store.get_stack("my-stack")
        assert result.stack_id == "arn:new"
        assert result.status == "CREATE_COMPLETE"


# ===========================================================================
# CfnChangeSet dataclass tests
# ===========================================================================


class TestCfnChangeSetDataclass:
    """Tests for the CfnChangeSet dataclass defaults and fields."""

    def test_default_status(self):
        cs = CfnChangeSet(
            change_set_id="id-1",
            change_set_name="cs-1",
            stack_name="stack-1",
        )
        assert cs.status == "CREATE_COMPLETE"

    def test_default_change_set_type(self):
        cs = CfnChangeSet(
            change_set_id="id-2",
            change_set_name="cs-2",
            stack_name="stack-2",
        )
        assert cs.change_set_type == "CREATE"

    def test_default_parameters_empty(self):
        cs = CfnChangeSet(
            change_set_id="id-3",
            change_set_name="cs-3",
            stack_name="stack-3",
        )
        assert cs.parameters == {}

    def test_default_changes_empty(self):
        cs = CfnChangeSet(
            change_set_id="id-4",
            change_set_name="cs-4",
            stack_name="stack-4",
        )
        assert cs.changes == []

    def test_fields_stored_correctly(self):
        cs = CfnChangeSet(
            change_set_id="arn:cs:123",
            change_set_name="my-cs",
            stack_name="my-stack",
            stack_id="arn:stack:456",
            template_body='{"Resources":{}}',
            status="CREATE_COMPLETE",
            change_set_type="UPDATE",
            parameters={"key": "val"},
        )
        assert cs.change_set_id == "arn:cs:123"
        assert cs.change_set_name == "my-cs"
        assert cs.stack_name == "my-stack"
        assert cs.stack_id == "arn:stack:456"
        assert cs.template_body == '{"Resources":{}}'
        assert cs.change_set_type == "UPDATE"
        assert cs.parameters == {"key": "val"}
