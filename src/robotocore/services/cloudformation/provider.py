"""Native CloudFormation provider."""

import threading
import time
import uuid
from collections import OrderedDict
from collections.abc import Callable

from starlette.requests import Request
from starlette.responses import Response

from robotocore.services.cloudformation.engine import (
    CfnResource,
    CfnStack,
    CfnStore,
    build_dependency_order,
    parse_template,
    resolve_intrinsics,
)
from robotocore.services.cloudformation.resources import create_resource, delete_resource

_stores: dict[str, CfnStore] = {}
_store_lock = threading.Lock()


def _get_store(region: str = "us-east-1") -> CfnStore:
    with _store_lock:
        if region not in _stores:
            _stores[region] = CfnStore()
        return _stores[region]


def _new_id() -> str:
    return str(uuid.uuid4())


class CfnError(Exception):
    def __init__(self, code: str, message: str, status: int = 400):
        self.code = code
        self.message = message
        self.status = status


async def handle_cloudformation_request(request: Request, region: str, account_id: str) -> Response:
    """Handle a CloudFormation API request."""
    body = await request.body()
    content_type = request.headers.get("content-type", "")

    from urllib.parse import parse_qs

    if "x-www-form-urlencoded" in content_type:
        parsed = parse_qs(body.decode(), keep_blank_values=True)
    else:
        parsed = parse_qs(str(request.url.query), keep_blank_values=True)
    params = {k: v[0] if len(v) == 1 else v for k, v in parsed.items()}
    action = params.get("Action", "")

    store = _get_store(region)
    handler = _ACTION_MAP.get(action)
    if handler is None:
        return _error("InvalidAction", f"Unknown action: {action}", 400)

    try:
        result = handler(store, params, region, account_id)
        return _xml_response(action + "Response", result)
    except CfnError as e:
        return _error(e.code, e.message, e.status)
    except Exception as e:
        return _error("InternalError", str(e), 500)


def _create_stack(store: CfnStore, params: dict, region: str, account_id: str) -> dict:
    name = params.get("StackName", "")
    template_body = params.get("TemplateBody", "")

    if not name:
        raise CfnError("ValidationError", "StackName is required")
    if not template_body:
        raise CfnError("ValidationError", "TemplateBody is required")

    existing = store.get_stack(name)
    if existing and existing.status not in ("DELETE_COMPLETE",):
        raise CfnError("AlreadyExistsException", f"Stack [{name}] already exists")

    stack_id = f"arn:aws:cloudformation:{region}:{account_id}:stack/{name}/{_new_id()}"

    # Parse parameters
    cfn_params = {}
    i = 1
    while f"Parameters.member.{i}.ParameterKey" in params:
        key = params[f"Parameters.member.{i}.ParameterKey"]
        value = params.get(f"Parameters.member.{i}.ParameterValue", "")
        cfn_params[key] = value
        i += 1

    # Parse tags
    tags = []
    i = 1
    while f"Tags.member.{i}.Key" in params:
        tags.append(
            {
                "Key": params[f"Tags.member.{i}.Key"],
                "Value": params.get(f"Tags.member.{i}.Value", ""),
            }
        )
        i += 1

    stack = CfnStack(
        stack_id=stack_id,
        stack_name=name,
        template_body=template_body,
        parameters=cfn_params,
        tags=tags,
    )

    # Deploy resources
    try:
        _deploy_stack(stack, region, account_id)
        stack.status = "CREATE_COMPLETE"
    except Exception as e:
        stack.status = "CREATE_FAILED"
        stack.status_reason = str(e)

    store.put_stack(stack)
    return {"StackId": stack_id}


def _deploy_stack(stack: CfnStack, region: str, account_id: str) -> None:
    """Parse template and create all resources in dependency order."""
    template = parse_template(stack.template_body)

    # Merge template parameter defaults
    for pname, pdef in template.get("Parameters", {}).items():
        if pname not in stack.parameters and "Default" in pdef:
            stack.parameters[pname] = str(pdef["Default"])

    # Add pseudo parameters
    stack.parameters["AWS::Region"] = region
    stack.parameters["AWS::AccountId"] = account_id
    stack.parameters["AWS::StackName"] = stack.stack_name
    stack.parameters["AWS::StackId"] = stack.stack_id

    resource_defs = template.get("Resources", {})
    order = build_dependency_order(template)

    for logical_id in order:
        res_def = resource_defs[logical_id]
        res_type = res_def["Type"]
        raw_props = res_def.get("Properties", {})

        # Resolve intrinsic functions in properties
        resolved_props = resolve_intrinsics(
            raw_props, stack.resources, stack.parameters, region, account_id
        )

        resource = CfnResource(
            logical_id=logical_id,
            resource_type=res_type,
            properties=resolved_props,
        )

        create_resource(resource, region, account_id)
        stack.resources[logical_id] = resource

    # Resolve outputs
    for out_name, out_def in template.get("Outputs", {}).items():
        value = resolve_intrinsics(
            out_def.get("Value"), stack.resources, stack.parameters, region, account_id
        )
        stack.outputs[out_name] = {
            "OutputKey": out_name,
            "OutputValue": str(value),
            "Description": out_def.get("Description", ""),
        }
        if "Export" in out_def:
            export_name = resolve_intrinsics(
                out_def["Export"].get("Name"), stack.resources, stack.parameters, region, account_id
            )
            stack.outputs[out_name]["ExportName"] = str(export_name)


def _delete_stack_action(store: CfnStore, params: dict, region: str, account_id: str) -> dict:
    name = params.get("StackName", "")
    stack = store.get_stack(name)
    if not stack:
        return {}

    # Delete resources in reverse order
    for logical_id in reversed(list(stack.resources.keys())):
        resource = stack.resources[logical_id]
        try:
            delete_resource(resource, region, account_id)
        except Exception:
            pass

    stack.status = "DELETE_COMPLETE"
    store.delete_stack(stack.stack_id)
    return {}


def _describe_stacks(store: CfnStore, params: dict, region: str, account_id: str) -> dict:
    name = params.get("StackName")
    if name:
        stack = store.get_stack(name)
        if not stack:
            raise CfnError("ValidationError", f"Stack with id {name} does not exist")
        stacks = [stack]
    else:
        stacks = store.list_stacks()

    members = []
    for s in stacks:
        member = {
            "StackId": s.stack_id,
            "StackName": s.stack_name,
            "StackStatus": s.status,
            "CreationTime": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime(s.created)),
        }
        if s.status_reason:
            member["StackStatusReason"] = s.status_reason
        if s.outputs:
            member["Outputs"] = list(s.outputs.values())
        if s.tags:
            member["Tags"] = s.tags
        members.append(member)

    return {"Stacks": members}


def _list_stacks(store: CfnStore, params: dict, region: str, account_id: str) -> dict:
    stacks = store.list_stacks()
    summaries = []
    for s in stacks:
        summaries.append(
            {
                "StackId": s.stack_id,
                "StackName": s.stack_name,
                "StackStatus": s.status,
                "CreationTime": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime(s.created)),
            }
        )
    return {"StackSummaries": summaries}


def _describe_stack_resources(store: CfnStore, params: dict, region: str, account_id: str) -> dict:
    name = params.get("StackName", "")
    stack = store.get_stack(name)
    if not stack:
        raise CfnError("ValidationError", f"Stack [{name}] does not exist")

    resources = []
    for lid, res in stack.resources.items():
        resources.append(
            {
                "LogicalResourceId": lid,
                "PhysicalResourceId": res.physical_id or "",
                "ResourceType": res.resource_type,
                "ResourceStatus": res.status,
                "Timestamp": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime(stack.created)),
            }
        )
    return {"StackResources": resources}


def _update_stack(store: CfnStore, params: dict, region: str, account_id: str) -> dict:
    name = params.get("StackName", "")
    template_body = params.get("TemplateBody", "")

    if not name:
        raise CfnError("ValidationError", "StackName is required")
    if not template_body:
        raise CfnError("ValidationError", "TemplateBody is required")

    stack = store.get_stack(name)
    if not stack:
        raise CfnError("ValidationError", f"Stack [{name}] does not exist")
    if stack.status in ("DELETE_COMPLETE", "DELETE_IN_PROGRESS"):
        raise CfnError("ValidationError", f"Stack [{name}] does not exist")

    # Parse new parameters
    cfn_params = {}
    i = 1
    while f"Parameters.member.{i}.ParameterKey" in params:
        key = params[f"Parameters.member.{i}.ParameterKey"]
        value = params.get(f"Parameters.member.{i}.ParameterValue", "")
        cfn_params[key] = value
        i += 1

    stack.status = "UPDATE_IN_PROGRESS"

    try:
        # Delete old resources in reverse order
        for logical_id in reversed(list(stack.resources.keys())):
            resource = stack.resources[logical_id]
            try:
                delete_resource(resource, region, account_id)
            except Exception:
                pass

        # Clear old state
        stack.resources = OrderedDict()
        stack.outputs = {}

        # Update stack fields
        stack.template_body = template_body
        stack.parameters = cfn_params

        # Deploy new resources
        _deploy_stack(stack, region, account_id)
        stack.status = "UPDATE_COMPLETE"
    except Exception as e:
        stack.status = "UPDATE_FAILED"
        stack.status_reason = str(e)

    store.put_stack(stack)
    return {"StackId": stack.stack_id}


def _validate_template(store: CfnStore, params: dict, region: str, account_id: str) -> dict:
    template_body = params.get("TemplateBody", "")
    if not template_body:
        raise CfnError("ValidationError", "TemplateBody is required")

    try:
        template = parse_template(template_body)
    except Exception as e:
        raise CfnError("ValidationError", f"Template format error: {e}")

    result: dict = {}
    template_params = template.get("Parameters", {})
    if template_params:
        members = []
        for pname, pdef in template_params.items():
            member: dict = {
                "ParameterKey": pname,
            }
            if "Default" in pdef:
                member["DefaultValue"] = str(pdef["Default"])
            if "Type" in pdef:
                member["ParameterType"] = pdef["Type"]
            if "Description" in pdef:
                member["Description"] = pdef["Description"]
            member["NoEcho"] = str(pdef.get("NoEcho", False)).lower()
            members.append(member)
        result["Parameters"] = members

    if "Description" in template:
        result["Description"] = template["Description"]

    return result


def _get_template(store: CfnStore, params: dict, region: str, account_id: str) -> dict:
    name = params.get("StackName", "")
    stack = store.get_stack(name)
    if not stack:
        raise CfnError("ValidationError", f"Stack [{name}] does not exist")
    return {"TemplateBody": stack.template_body}


# --- XML Response ---


def _xml_response(action: str, data: dict) -> Response:
    def dict_to_xml(d) -> str:
        if isinstance(d, str):
            return d
        if isinstance(d, list):
            parts = []
            for item in d:
                if isinstance(item, dict):
                    parts.append(f"<member>{dict_to_xml(item)}</member>")
                else:
                    parts.append(f"<member>{item}</member>")
            return "".join(parts)
        if isinstance(d, dict):
            parts = []
            for k, v in d.items():
                if isinstance(v, list):
                    parts.append(f"<{k}>{dict_to_xml(v)}</{k}>")
                elif isinstance(v, dict):
                    parts.append(f"<{k}>{dict_to_xml(v)}</{k}>")
                else:
                    parts.append(f"<{k}>{v}</{k}>")
            return "".join(parts)
        return str(d)

    result_name = action.replace("Response", "Result")
    body_xml = dict_to_xml(data)
    xml = (
        f'<?xml version="1.0"?>'
        f'<{action} xmlns="http://cloudformation.amazonaws.com/doc/2010-05-15/">'
        f"<{result_name}>{body_xml}</{result_name}>"
        f"<ResponseMetadata><RequestId>{_new_id()}</RequestId></ResponseMetadata>"
        f"</{action}>"
    )
    return Response(content=xml, status_code=200, media_type="text/xml")


def _error(code: str, message: str, status: int) -> Response:
    xml = (
        f'<?xml version="1.0"?>'
        f'<ErrorResponse xmlns="http://cloudformation.amazonaws.com/doc/2010-05-15/">'
        f"<Error><Type>Sender</Type><Code>{code}</Code><Message>{message}</Message></Error>"
        f"<RequestId>{_new_id()}</RequestId>"
        f"</ErrorResponse>"
    )
    return Response(content=xml, status_code=status, media_type="text/xml")


_ACTION_MAP: dict[str, Callable] = {
    "CreateStack": _create_stack,
    "UpdateStack": _update_stack,
    "DeleteStack": _delete_stack_action,
    "DescribeStacks": _describe_stacks,
    "ListStacks": _list_stacks,
    "DescribeStackResources": _describe_stack_resources,
    "GetTemplate": _get_template,
    "ValidateTemplate": _validate_template,
}
