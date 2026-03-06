"""Unit tests for CloudFormation provider request handling."""

import json
from unittest.mock import patch
from urllib.parse import urlencode

import pytest
from starlette.requests import Request

from robotocore.services.cloudformation.engine import CfnStore
from robotocore.services.cloudformation.provider import (
    CfnError,
    _create_stack,
    _delete_stack_action,
    _describe_stacks,
    _error,
    _get_template,
    _list_stacks,
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

    async def test_unknown_action(self):
        form = urlencode({"Action": "BogusAction"}).encode()
        headers = {"content-type": "application/x-www-form-urlencoded"}
        req = _make_request(body=form, headers=headers)

        with patch("robotocore.services.cloudformation.provider._get_store") as mock_get:
            mock_get.return_value = CfnStore()
            resp = await handle_cloudformation_request(req, "us-east-1", "123456789012")

        assert resp.status_code == 400
        assert b"InvalidAction" in resp.body

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
