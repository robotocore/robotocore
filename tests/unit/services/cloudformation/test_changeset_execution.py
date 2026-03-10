"""Unit tests for CloudFormation ExecuteChangeSet."""

import json

from robotocore.services.cloudformation.engine import (
    CfnStack,
    CfnStore,
)
from robotocore.services.cloudformation.provider import (
    CfnError,
    _create_change_set,
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


class TestExecuteChangeSetCreate:
    """Tests for ExecuteChangeSet with CREATE type."""

    def test_execute_create_changeset_deploys_resources(self):
        """Create change set + execute -> stack status is CREATE_COMPLETE with resources."""
        store = CfnStore()

        # Create the change set (also creates stub stack)
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

        # Verify resources were created
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

        # Find the change set and verify status
        cs = None
        for c in store.change_sets.values():
            if c.change_set_name == "my-cs":
                cs = c
                break
        assert cs is not None
        assert cs.status == "EXECUTE_COMPLETE"


class TestExecuteChangeSetUpdate:
    """Tests for ExecuteChangeSet with UPDATE type."""

    def test_execute_update_changeset_replaces_resources(self):
        """UPDATE change set replaces stack resources with new template."""
        store = CfnStore()

        # Create the initial stack directly
        stack = CfnStack(
            stack_id=f"arn:aws:cloudformation:{REGION}:{ACCOUNT}:stack/upd-test/fake-id",
            stack_name="upd-test",
            template_body=SIMPLE_SQS_TEMPLATE,
            status="CREATE_COMPLETE",
        )
        store.put_stack(stack)

        # Create an UPDATE change set with a different template
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

        # Execute the update change set
        _execute_change_set(
            store,
            {"ChangeSetName": "update-cs", "StackName": "upd-test"},
            REGION,
            ACCOUNT,
        )

        # Verify stack is UPDATE_COMPLETE
        stack = store.get_stack("upd-test")
        assert stack.status == "UPDATE_COMPLETE"

        # Verify new resources were deployed (SNS topic, not SQS queue)
        assert "MyTopic" in stack.resources
        assert stack.resources["MyTopic"].resource_type == "AWS::SNS::Topic"


class TestExecuteChangeSetErrors:
    """Tests for error cases in ExecuteChangeSet."""

    def test_execute_nonexistent_changeset_raises_error(self):
        """Executing a nonexistent change set raises ChangeSetNotFoundException."""
        store = CfnStore()
        try:
            _execute_change_set(
                store,
                {"ChangeSetName": "does-not-exist", "StackName": "no-stack"},
                REGION,
                ACCOUNT,
            )
            assert False, "Should have raised CfnError"
        except CfnError as e:
            assert e.code == "ChangeSetNotFoundException"

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

        # Execute once
        _execute_change_set(
            store,
            {"ChangeSetName": "my-cs", "StackName": "double-exec"},
            REGION,
            ACCOUNT,
        )

        # Execute again - should fail
        try:
            _execute_change_set(
                store,
                {"ChangeSetName": "my-cs", "StackName": "double-exec"},
                REGION,
                ACCOUNT,
            )
            assert False, "Should have raised CfnError"
        except CfnError as e:
            assert e.code == "InvalidChangeSetStatusException"

    def test_execute_changeset_with_parameters(self):
        """Change set with parameters passes them through to stack deployment."""
        template = json.dumps(
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
        store = CfnStore()
        _create_change_set(
            store,
            {
                "StackName": "param-test",
                "ChangeSetName": "param-cs",
                "TemplateBody": template,
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
        # The parameter should have been passed through
        assert stack.parameters.get("QueueName") == "custom-queue-name"
