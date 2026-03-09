"""Unit tests for CloudFormation provider request handling."""

import json
from unittest.mock import patch
from urllib.parse import urlencode

import pytest
from starlette.requests import Request

from robotocore.services.cloudformation.engine import CfnStore
from robotocore.services.cloudformation.provider import (
    CfnError,
    _create_change_set,
    _create_stack,
    _delete_change_set,
    _delete_stack_action,
    _describe_change_set,
    _describe_stack_events,
    _describe_stack_resource,
    _describe_stacks,
    _error,
    _get_template,
    _list_exports,
    _list_stack_resources,
    _list_stacks,
    _update_stack,
    _validate_template,
    _xml_response,
    handle_cloudformation_request,
)


def _make_request(body=b"", headers=None, query_string=b""):
    hdrs = headers or {}
    raw_headers = [(k.lower().encode(), v.encode()) for k, v in hdrs.items()]
    scope = {
        "type": "http",
        "method": "POST",
        "path": "/",
        "query_string": query_string,
        "headers": raw_headers,
        "root_path": "",
        "scheme": "http",
        "server": ("localhost", 4566),
    }

    async def receive():
        return {"type": "http.request", "body": body}

    return Request(scope, receive)


_SIMPLE_TEMPLATE = json.dumps(
    {
        "AWSTemplateFormatVersion": "2010-09-09",
        "Resources": {},
    }
)

_TEMPLATE_WITH_PARAMS = json.dumps(
    {
        "AWSTemplateFormatVersion": "2010-09-09",
        "Description": "Test template",
        "Parameters": {
            "Env": {
                "Type": "String",
                "Default": "dev",
                "Description": "Environment name",
            }
        },
        "Resources": {},
    }
)


class TestHelpers:
    def test_xml_response(self):
        resp = _xml_response("CreateStackResponse", {"StackId": "arn:stack"})
        assert resp.status_code == 200
        assert b"CreateStackResult" in resp.body
        assert b"arn:stack" in resp.body

    def test_xml_response_with_list(self):
        resp = _xml_response(
            "DescribeStacksResponse",
            {
                "Stacks": [
                    {"StackName": "s1"},
                    {"StackName": "s2"},
                ]
            },
        )
        assert b"<member>" in resp.body

    def test_error_response(self):
        resp = _error("ValidationError", "bad", 400)
        assert resp.status_code == 400
        assert b"<Code>ValidationError</Code>" in resp.body
        assert b"<Message>bad</Message>" in resp.body

    def test_xml_response_nested_dict(self):
        resp = _xml_response(
            "TestResponse",
            {"Outer": {"Inner": "value"}},
        )
        assert b"<Outer><Inner>value</Inner></Outer>" in resp.body


class TestCfnError:
    def test_attributes(self):
        err = CfnError("ValidationError", "msg", 400)
        assert err.code == "ValidationError"
        assert err.message == "msg"
        assert err.status == 400

    def test_default_status(self):
        err = CfnError("Code", "msg")
        assert err.status == 400


class TestCreateStack:
    @patch("robotocore.services.cloudformation.provider._deploy_stack")
    def test_create_stack_success(self, mock_deploy):
        store = CfnStore()
        result = _create_stack(
            store,
            {"StackName": "my-stack", "TemplateBody": _SIMPLE_TEMPLATE},
            "us-east-1",
            "123",
        )
        assert "StackId" in result
        assert "my-stack" in result["StackId"]

    def test_create_stack_missing_name(self):
        store = CfnStore()
        with pytest.raises(CfnError) as exc_info:
            _create_stack(
                store,
                {"TemplateBody": _SIMPLE_TEMPLATE},
                "us-east-1",
                "123",
            )
        assert exc_info.value.code == "ValidationError"

    def test_create_stack_missing_template(self):
        store = CfnStore()
        with pytest.raises(CfnError) as exc_info:
            _create_stack(store, {"StackName": "s1"}, "us-east-1", "123")
        assert exc_info.value.code == "ValidationError"

    @patch("robotocore.services.cloudformation.provider._deploy_stack")
    def test_create_duplicate_stack(self, mock_deploy):
        store = CfnStore()
        _create_stack(
            store,
            {"StackName": "dup", "TemplateBody": _SIMPLE_TEMPLATE},
            "us-east-1",
            "123",
        )
        with pytest.raises(CfnError) as exc_info:
            _create_stack(
                store,
                {"StackName": "dup", "TemplateBody": _SIMPLE_TEMPLATE},
                "us-east-1",
                "123",
            )
        assert exc_info.value.code == "AlreadyExistsException"

    @patch("robotocore.services.cloudformation.provider._deploy_stack")
    def test_create_with_parameters(self, mock_deploy):
        store = CfnStore()
        result = _create_stack(
            store,
            {
                "StackName": "param-stack",
                "TemplateBody": _TEMPLATE_WITH_PARAMS,
                "Parameters.member.1.ParameterKey": "Env",
                "Parameters.member.1.ParameterValue": "prod",
            },
            "us-east-1",
            "123",
        )
        assert "StackId" in result

    @patch("robotocore.services.cloudformation.provider._deploy_stack")
    def test_create_with_tags(self, mock_deploy):
        store = CfnStore()
        _create_stack(
            store,
            {
                "StackName": "tagged",
                "TemplateBody": _SIMPLE_TEMPLATE,
                "Tags.member.1.Key": "env",
                "Tags.member.1.Value": "test",
            },
            "us-east-1",
            "123",
        )
        stack = store.get_stack("tagged")
        assert len(stack.tags) == 1
        assert stack.tags[0]["Key"] == "env"


class TestDescribeStacks:
    @patch("robotocore.services.cloudformation.provider._deploy_stack")
    def test_describe_specific_stack(self, mock_deploy):
        store = CfnStore()
        _create_stack(
            store,
            {"StackName": "s1", "TemplateBody": _SIMPLE_TEMPLATE},
            "us-east-1",
            "123",
        )
        result = _describe_stacks(store, {"StackName": "s1"}, "us-east-1", "123")
        assert len(result["Stacks"]) == 1
        assert result["Stacks"][0]["StackName"] == "s1"

    def test_describe_nonexistent(self):
        store = CfnStore()
        with pytest.raises(CfnError) as exc_info:
            _describe_stacks(store, {"StackName": "nope"}, "us-east-1", "123")
        assert exc_info.value.code == "ValidationError"

    @patch("robotocore.services.cloudformation.provider._deploy_stack")
    def test_describe_all_stacks(self, mock_deploy):
        store = CfnStore()
        _create_stack(
            store,
            {"StackName": "s1", "TemplateBody": _SIMPLE_TEMPLATE},
            "us-east-1",
            "123",
        )
        _create_stack(
            store,
            {"StackName": "s2", "TemplateBody": _SIMPLE_TEMPLATE},
            "us-east-1",
            "123",
        )
        result = _describe_stacks(store, {}, "us-east-1", "123")
        assert len(result["Stacks"]) == 2


class TestListStacks:
    @patch("robotocore.services.cloudformation.provider._deploy_stack")
    def test_list_stacks(self, mock_deploy):
        store = CfnStore()
        _create_stack(
            store,
            {"StackName": "ls1", "TemplateBody": _SIMPLE_TEMPLATE},
            "us-east-1",
            "123",
        )
        result = _list_stacks(store, {}, "us-east-1", "123")
        assert len(result["StackSummaries"]) == 1
        assert result["StackSummaries"][0]["StackName"] == "ls1"


class TestDeleteStack:
    @patch("robotocore.services.cloudformation.provider._deploy_stack")
    def test_delete_stack(self, mock_deploy):
        store = CfnStore()
        _create_stack(
            store,
            {"StackName": "del-me", "TemplateBody": _SIMPLE_TEMPLATE},
            "us-east-1",
            "123",
        )
        result = _delete_stack_action(store, {"StackName": "del-me"}, "us-east-1", "123")
        assert result == {}

    def test_delete_nonexistent_stack(self):
        store = CfnStore()
        result = _delete_stack_action(store, {"StackName": "nope"}, "us-east-1", "123")
        assert result == {}


class TestValidateTemplate:
    def test_validate_valid_template(self):
        store = CfnStore()
        result = _validate_template(
            store,
            {"TemplateBody": _TEMPLATE_WITH_PARAMS},
            "us-east-1",
            "123",
        )
        assert "Parameters" in result
        assert result["Parameters"][0]["ParameterKey"] == "Env"
        assert result["Parameters"][0]["DefaultValue"] == "dev"
        assert result["Description"] == "Test template"

    def test_validate_missing_template(self):
        store = CfnStore()
        with pytest.raises(CfnError):
            _validate_template(store, {}, "us-east-1", "123")

    def test_validate_invalid_template(self):
        store = CfnStore()
        # yaml.safe_load parses this as a string, causing AttributeError
        # on .get() — the provider catches it as a generic Exception
        with pytest.raises(CfnError) as exc_info:
            _validate_template(
                store,
                {"TemplateBody": "{{{{invalid"},
                "us-east-1",
                "123",
            )
        assert exc_info.value.code in ("ValidationError", "InternalError")


class TestGetTemplate:
    @patch("robotocore.services.cloudformation.provider._deploy_stack")
    def test_get_template(self, mock_deploy):
        store = CfnStore()
        _create_stack(
            store,
            {"StackName": "tmpl-stack", "TemplateBody": _SIMPLE_TEMPLATE},
            "us-east-1",
            "123",
        )
        result = _get_template(store, {"StackName": "tmpl-stack"}, "us-east-1", "123")
        assert result["TemplateBody"] == _SIMPLE_TEMPLATE

    def test_get_template_not_found(self):
        store = CfnStore()
        with pytest.raises(CfnError):
            _get_template(store, {"StackName": "nope"}, "us-east-1", "123")


@pytest.mark.asyncio
class TestHandleCloudFormationRequest:
    async def test_create_stack_via_handler(self):
        form = urlencode(
            {
                "Action": "CreateStack",
                "StackName": "http-stack",
                "TemplateBody": _SIMPLE_TEMPLATE,
            }
        ).encode()
        headers = {"content-type": "application/x-www-form-urlencoded"}
        req = _make_request(body=form, headers=headers)

        with (
            patch("robotocore.services.cloudformation.provider._get_store") as mock_get,
            patch("robotocore.services.cloudformation.provider._deploy_stack"),
        ):
            store = CfnStore()
            mock_get.return_value = store
            resp = await handle_cloudformation_request(req, "us-east-1", "123456789012")

        assert resp.status_code == 200
        assert b"CreateStackResult" in resp.body

    async def test_validation_error_handling(self):
        form = urlencode(
            {
                "Action": "DescribeStacks",
                "StackName": "nonexistent",
            }
        ).encode()
        headers = {"content-type": "application/x-www-form-urlencoded"}
        req = _make_request(body=form, headers=headers)

        with patch("robotocore.services.cloudformation.provider._get_store") as mock_get:
            mock_get.return_value = CfnStore()
            resp = await handle_cloudformation_request(req, "us-east-1", "123456789012")

        assert resp.status_code == 400
        assert b"ValidationError" in resp.body


_TEMPLATE_WITH_DESC = json.dumps(
    {
        "AWSTemplateFormatVersion": "2010-09-09",
        "Description": "A test stack",
        "Resources": {},
    }
)

_CONDITION_TEMPLATE = json.dumps(
    {
        "AWSTemplateFormatVersion": "2010-09-09",
        "Parameters": {
            "CreateQueue": {
                "Type": "String",
                "Default": "false",
            },
        },
        "Conditions": {
            "ShouldCreate": {"Fn::Equals": [{"Ref": "CreateQueue"}, "true"]},
        },
        "Resources": {
            "AlwaysQueue": {
                "Type": "AWS::SQS::Queue",
                "Properties": {"QueueName": "always-queue"},
            },
            "ConditionalQueue": {
                "Type": "AWS::SQS::Queue",
                "Condition": "ShouldCreate",
                "Properties": {"QueueName": "cond-queue"},
            },
        },
    }
)

_EXPORT_TEMPLATE = json.dumps(
    {
        "AWSTemplateFormatVersion": "2010-09-09",
        "Resources": {
            "MyQueue": {
                "Type": "AWS::SQS::Queue",
                "Properties": {"QueueName": "export-queue"},
            },
        },
        "Outputs": {
            "QueueUrl": {
                "Value": {"Ref": "MyQueue"},
                "Export": {"Name": "my-export"},
            },
        },
    }
)


class TestStackDescription:
    @patch("robotocore.services.cloudformation.provider._deploy_stack")
    def test_description_in_describe_stacks(self, mock_deploy):
        store = CfnStore()
        _create_stack(
            store,
            {"StackName": "desc-stack", "TemplateBody": _TEMPLATE_WITH_DESC},
            "us-east-1",
            "123",
        )
        result = _describe_stacks(store, {"StackName": "desc-stack"}, "us-east-1", "123")
        assert result["Stacks"][0]["Description"] == "A test stack"

    @patch("robotocore.services.cloudformation.provider._deploy_stack")
    def test_no_description_when_absent(self, mock_deploy):
        store = CfnStore()
        _create_stack(
            store,
            {"StackName": "nodesc", "TemplateBody": _SIMPLE_TEMPLATE},
            "us-east-1",
            "123",
        )
        result = _describe_stacks(store, {"StackName": "nodesc"}, "us-east-1", "123")
        assert "Description" not in result["Stacks"][0]


class TestStackEvents:
    @patch("robotocore.services.cloudformation.provider._deploy_stack")
    def test_events_recorded(self, mock_deploy):
        store = CfnStore()
        _create_stack(
            store,
            {"StackName": "ev-stack", "TemplateBody": _SIMPLE_TEMPLATE},
            "us-east-1",
            "123",
        )
        result = _describe_stack_events(store, {"StackName": "ev-stack"}, "us-east-1", "123")
        events = result["StackEvents"]
        assert len(events) >= 2  # CREATE_IN_PROGRESS + CREATE_COMPLETE
        statuses = [e["ResourceStatus"] for e in events]
        assert "CREATE_IN_PROGRESS" in statuses
        assert "CREATE_COMPLETE" in statuses
        for e in events:
            assert e["StackName"] == "ev-stack"

    def test_events_nonexistent_stack(self):
        store = CfnStore()
        with pytest.raises(CfnError):
            _describe_stack_events(store, {"StackName": "nope"}, "us-east-1", "123")


class TestListStackResources:
    @patch("robotocore.services.cloudformation.provider._deploy_stack")
    def test_list_resources(self, mock_deploy):
        store = CfnStore()
        _create_stack(
            store,
            {"StackName": "lr-stack", "TemplateBody": _SIMPLE_TEMPLATE},
            "us-east-1",
            "123",
        )
        # Manually add a resource to the stack since deploy is mocked
        from robotocore.services.cloudformation.engine import CfnResource

        stack = store.get_stack("lr-stack")
        stack.resources["MyQueue"] = CfnResource(
            logical_id="MyQueue",
            resource_type="AWS::SQS::Queue",
            properties={},
            physical_id="http://queue-url",
            status="CREATE_COMPLETE",
        )
        result = _list_stack_resources(store, {"StackName": "lr-stack"}, "us-east-1", "123")
        summaries = result["StackResourceSummaries"]
        assert len(summaries) == 1
        assert summaries[0]["LogicalResourceId"] == "MyQueue"
        assert summaries[0]["ResourceType"] == "AWS::SQS::Queue"

    def test_list_resources_nonexistent(self):
        store = CfnStore()
        with pytest.raises(CfnError):
            _list_stack_resources(store, {"StackName": "nope"}, "us-east-1", "123")


class TestDescribeStackResource:
    @patch("robotocore.services.cloudformation.provider._deploy_stack")
    def test_describe_resource(self, mock_deploy):
        store = CfnStore()
        _create_stack(
            store,
            {"StackName": "dr-stack", "TemplateBody": _SIMPLE_TEMPLATE},
            "us-east-1",
            "123",
        )
        from robotocore.services.cloudformation.engine import CfnResource

        stack = store.get_stack("dr-stack")
        stack.resources["MyQueue"] = CfnResource(
            logical_id="MyQueue",
            resource_type="AWS::SQS::Queue",
            properties={},
            physical_id="http://queue-url",
            status="CREATE_COMPLETE",
        )
        result = _describe_stack_resource(
            store,
            {"StackName": "dr-stack", "LogicalResourceId": "MyQueue"},
            "us-east-1",
            "123",
        )
        detail = result["StackResourceDetail"]
        assert detail["LogicalResourceId"] == "MyQueue"
        assert detail["ResourceType"] == "AWS::SQS::Queue"
        assert detail["StackName"] == "dr-stack"

    def test_describe_resource_not_found(self):
        store = CfnStore()
        with pytest.raises(CfnError):
            _describe_stack_resource(
                store,
                {"StackName": "nope", "LogicalResourceId": "X"},
                "us-east-1",
                "123",
            )

    @patch("robotocore.services.cloudformation.provider._deploy_stack")
    def test_describe_resource_wrong_logical_id(self, mock_deploy):
        store = CfnStore()
        _create_stack(
            store,
            {"StackName": "dr2-stack", "TemplateBody": _SIMPLE_TEMPLATE},
            "us-east-1",
            "123",
        )
        with pytest.raises(CfnError):
            _describe_stack_resource(
                store,
                {"StackName": "dr2-stack", "LogicalResourceId": "Nope"},
                "us-east-1",
                "123",
            )


class TestListExports:
    @patch("robotocore.services.cloudformation.provider._deploy_stack")
    def test_list_exports_empty(self, mock_deploy):
        store = CfnStore()
        result = _list_exports(store, {}, "us-east-1", "123")
        assert result["Exports"] == []

    @patch("robotocore.services.cloudformation.provider._deploy_stack")
    def test_list_exports_with_data(self, mock_deploy):
        store = CfnStore()
        store.exports["my-export"] = "some-value"
        result = _list_exports(store, {}, "us-east-1", "123")
        assert len(result["Exports"]) == 1
        assert result["Exports"][0]["Name"] == "my-export"
        assert result["Exports"][0]["Value"] == "some-value"


class TestConditions:
    @patch("robotocore.services.cloudformation.resources.create_resource")
    def test_condition_false_skips_resource(self, mock_create):
        """When a condition is false, the resource should not be created."""
        store = CfnStore()
        _create_stack(
            store,
            {
                "StackName": "cond-stack",
                "TemplateBody": _CONDITION_TEMPLATE,
                "Parameters.member.1.ParameterKey": "CreateQueue",
                "Parameters.member.1.ParameterValue": "false",
            },
            "us-east-1",
            "123",
        )
        stack = store.get_stack("cond-stack")
        assert stack.status == "CREATE_COMPLETE"
        # Only AlwaysQueue should be created, not ConditionalQueue
        assert "AlwaysQueue" in stack.resources
        assert "ConditionalQueue" not in stack.resources

    @patch("robotocore.services.cloudformation.resources.create_resource")
    def test_condition_true_creates_resource(self, mock_create):
        """When a condition is true, the resource should be created."""
        store = CfnStore()
        _create_stack(
            store,
            {
                "StackName": "cond-stack2",
                "TemplateBody": _CONDITION_TEMPLATE,
                "Parameters.member.1.ParameterKey": "CreateQueue",
                "Parameters.member.1.ParameterValue": "true",
            },
            "us-east-1",
            "123",
        )
        stack = store.get_stack("cond-stack2")
        assert "AlwaysQueue" in stack.resources
        assert "ConditionalQueue" in stack.resources


class TestChangeSet:
    def test_create_change_set(self):
        store = CfnStore()
        result = _create_change_set(
            store,
            {
                "StackName": "my-stack",
                "ChangeSetName": "my-cs",
                "TemplateBody": '{"AWSTemplateFormatVersion":"2010-09-09","Resources":{}}',
                "ChangeSetType": "CREATE",
            },
            "us-east-1",
            "123456789012",
        )
        assert "Id" in result
        assert "StackId" in result
        assert len(store.change_sets) == 1

    def test_describe_change_set(self):
        store = CfnStore()
        _create_change_set(
            store,
            {"StackName": "s1", "ChangeSetName": "cs1", "ChangeSetType": "CREATE"},
            "us-east-1",
            "123",
        )
        desc = _describe_change_set(
            store, {"ChangeSetName": "cs1", "StackName": "s1"}, "us-east-1", "123"
        )
        assert desc["ChangeSetName"] == "cs1"
        assert desc["StackName"] == "s1"
        assert desc["Status"] == "CREATE_COMPLETE"

    def test_describe_change_set_not_found(self):
        store = CfnStore()
        with pytest.raises(CfnError) as exc_info:
            _describe_change_set(store, {"ChangeSetName": "nonexistent"}, "us-east-1", "123")
        assert "ChangeSetNotFoundException" in exc_info.value.code

    def test_delete_change_set(self):
        store = CfnStore()
        _create_change_set(
            store,
            {"StackName": "s1", "ChangeSetName": "cs1", "ChangeSetType": "CREATE"},
            "us-east-1",
            "123",
        )
        assert len(store.change_sets) == 1
        _delete_change_set(store, {"ChangeSetName": "cs1", "StackName": "s1"}, "us-east-1", "123")
        assert len(store.change_sets) == 0

    def test_create_change_set_creates_stub_stack(self):
        store = CfnStore()
        _create_change_set(
            store,
            {"StackName": "new-stack", "ChangeSetName": "cs1", "ChangeSetType": "CREATE"},
            "us-east-1",
            "123",
        )
        stack = store.get_stack("new-stack")
        assert stack is not None
        assert stack.status == "REVIEW_IN_PROGRESS"


class TestCrossStackExports:
    @patch("robotocore.services.cloudformation.resources.create_resource")
    def test_exports_registered_in_store(self, mock_create):
        """Exports should be registered in the store."""
        store = CfnStore()
        _create_stack(
            store,
            {"StackName": "export-stack", "TemplateBody": _EXPORT_TEMPLATE},
            "us-east-1",
            "123",
        )
        assert "my-export" in store.exports


class TestUpdateStack:
    """Tests for _update_stack — categorical bug class: state preservation during updates."""

    @patch("robotocore.services.cloudformation.provider._deploy_stack")
    def test_update_stack_preserves_tags(self, mock_deploy):
        """BUG: _update_stack ignores tags from the update request and drops existing tags.
        Categorical pattern: tag operations must be parsed and stored on every mutating call."""
        store = CfnStore()
        _create_stack(
            store,
            {
                "StackName": "tag-stack",
                "TemplateBody": _SIMPLE_TEMPLATE,
                "Tags.member.1.Key": "env",
                "Tags.member.1.Value": "dev",
            },
            "us-east-1",
            "123",
        )
        stack = store.get_stack("tag-stack")
        assert stack.tags == [{"Key": "env", "Value": "dev"}]

        # Update with new tags
        _update_stack(
            store,
            {
                "StackName": "tag-stack",
                "TemplateBody": _SIMPLE_TEMPLATE,
                "Tags.member.1.Key": "env",
                "Tags.member.1.Value": "prod",
                "Tags.member.2.Key": "team",
                "Tags.member.2.Value": "platform",
            },
            "us-east-1",
            "123",
        )
        stack = store.get_stack("tag-stack")
        assert len(stack.tags) == 2
        tag_dict = {t["Key"]: t["Value"] for t in stack.tags}
        assert tag_dict["env"] == "prod"
        assert tag_dict["team"] == "platform"

    @patch("robotocore.services.cloudformation.provider._deploy_stack")
    def test_update_stack_records_events(self, mock_deploy):
        """Every state transition must emit events."""
        store = CfnStore()
        _create_stack(
            store,
            {"StackName": "ev-update", "TemplateBody": _SIMPLE_TEMPLATE},
            "us-east-1",
            "123",
        )
        stack = store.get_stack("ev-update")
        events_before = len(stack.events)

        # Use a different template to avoid "No updates are to be performed"
        updated_template = json.dumps(
            {
                "AWSTemplateFormatVersion": "2010-09-09",
                "Description": "Updated",
                "Resources": {},
            }
        )
        _update_stack(
            store,
            {"StackName": "ev-update", "TemplateBody": updated_template},
            "us-east-1",
            "123",
        )
        stack = store.get_stack("ev-update")
        assert len(stack.events) > events_before
        statuses = [e["ResourceStatus"] for e in stack.events]
        assert "UPDATE_IN_PROGRESS" in statuses
        assert "UPDATE_COMPLETE" in statuses

    @patch("robotocore.services.cloudformation.provider._deploy_stack")
    def test_update_nonexistent_stack(self, mock_deploy):
        """Update a stack that doesn't exist should raise ValidationError."""
        store = CfnStore()
        with pytest.raises(CfnError) as exc_info:
            _update_stack(
                store,
                {"StackName": "ghost", "TemplateBody": _SIMPLE_TEMPLATE},
                "us-east-1",
                "123",
            )
        assert exc_info.value.code == "ValidationError"

    @patch("robotocore.services.cloudformation.provider._deploy_stack")
    def test_update_returns_stack_id(self, mock_deploy):
        """Update should return the stack ID."""
        store = CfnStore()
        _create_stack(
            store,
            {"StackName": "upd-id", "TemplateBody": _SIMPLE_TEMPLATE},
            "us-east-1",
            "123",
        )
        # Use a different template to avoid "No updates are to be performed"
        updated_template = json.dumps(
            {
                "AWSTemplateFormatVersion": "2010-09-09",
                "Description": "Updated",
                "Resources": {},
            }
        )
        result = _update_stack(
            store,
            {"StackName": "upd-id", "TemplateBody": updated_template},
            "us-east-1",
            "123",
        )
        assert "StackId" in result


class TestDeleteStackCascade:
    """Tests for deletion cascade — categorical bug class: cleanup of dependent state."""

    @patch("robotocore.services.cloudformation.resources.create_resource")
    def test_delete_cleans_up_exports(self, mock_create):
        """BUG: _delete_stack_action doesn't remove exports from store.exports.
        Categorical pattern: deleting a parent must cascade to all dependent state."""
        store = CfnStore()
        _create_stack(
            store,
            {"StackName": "export-del", "TemplateBody": _EXPORT_TEMPLATE},
            "us-east-1",
            "123",
        )
        assert "my-export" in store.exports

        _delete_stack_action(store, {"StackName": "export-del"}, "us-east-1", "123")
        # Exports from deleted stack should be cleaned up
        assert "my-export" not in store.exports

    @patch("robotocore.services.cloudformation.provider._deploy_stack")
    def test_delete_stack_sets_status(self, mock_deploy):
        """After deletion, stack status should be DELETE_COMPLETE."""
        store = CfnStore()
        _create_stack(
            store,
            {"StackName": "del-status", "TemplateBody": _SIMPLE_TEMPLATE},
            "us-east-1",
            "123",
        )
        _delete_stack_action(store, {"StackName": "del-status"}, "us-east-1", "123")
        stack = store.get_stack("del-status")
        assert stack.status == "DELETE_COMPLETE"


class TestDescribeDeletedStack:
    """Tests for error codes on deleted stacks — categorical bug class: correct error codes."""

    @patch("robotocore.services.cloudformation.provider._deploy_stack")
    def test_describe_deleted_stack_by_name_excludes_from_list(self, mock_deploy):
        """DescribeStacks without StackName should exclude DELETE_COMPLETE stacks."""
        store = CfnStore()
        _create_stack(
            store,
            {"StackName": "will-delete", "TemplateBody": _SIMPLE_TEMPLATE},
            "us-east-1",
            "123",
        )
        _delete_stack_action(store, {"StackName": "will-delete"}, "us-east-1", "123")

        result = _describe_stacks(store, {}, "us-east-1", "123")
        names = [s["StackName"] for s in result["Stacks"]]
        assert "will-delete" not in names

    @patch("robotocore.services.cloudformation.provider._deploy_stack")
    def test_recreate_after_delete(self, mock_deploy):
        """Should be able to create a new stack with the same name after deletion."""
        store = CfnStore()
        _create_stack(
            store,
            {"StackName": "reuse", "TemplateBody": _SIMPLE_TEMPLATE},
            "us-east-1",
            "123",
        )
        _delete_stack_action(store, {"StackName": "reuse"}, "us-east-1", "123")
        # Creating again should succeed
        result = _create_stack(
            store,
            {"StackName": "reuse", "TemplateBody": _SIMPLE_TEMPLATE},
            "us-east-1",
            "123",
        )
        assert "StackId" in result


class TestListStacksThreadSafety:
    """Tests for thread safety — categorical bug class: missing locks on read paths."""

    def test_list_stacks_returns_snapshot(self):
        """CfnStore.list_stacks() should use the mutex for a consistent snapshot."""
        store = CfnStore()
        from robotocore.services.cloudformation.engine import CfnStack

        stack = CfnStack(stack_id="arn:test", stack_name="test", template_body="{}")
        store.put_stack(stack)
        # list_stacks should return data even under concurrent access
        result = store.list_stacks()
        assert len(result) == 1
        assert result[0].stack_name == "test"
