"""Native CloudFormation provider."""

import copy
import threading
import time
import uuid
from collections import OrderedDict
from collections.abc import Callable

from starlette.requests import Request
from starlette.responses import Response

from robotocore.services.cloudformation.engine import (
    CfnChangeSet,
    CfnResource,
    CfnStack,
    CfnStore,
    build_dependency_order,
    evaluate_conditions,
    parse_template,
    resolve_intrinsics,
)
from robotocore.services.cloudformation.resources import create_resource, delete_resource

DEFAULT_ACCOUNT_ID = "123456789012"

_stores: dict[tuple[str, str], CfnStore] = {}
_store_lock = threading.Lock()


def _get_store(region: str = "us-east-1", account_id: str = DEFAULT_ACCOUNT_ID) -> CfnStore:
    key = (account_id, region)
    with _store_lock:
        if key not in _stores:
            _stores[key] = CfnStore()
        return _stores[key]


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

    store = _get_store(region, account_id)
    handler = _ACTION_MAP.get(action)
    if handler is None:
        # Fall back to Moto for operations we don't intercept
        from robotocore.providers.moto_bridge import forward_to_moto

        return await forward_to_moto(request, "cloudformation", account_id=account_id)

    try:
        result = handler(store, params, region, account_id)
        return _xml_response(action + "Response", result)
    except CfnError as e:
        return _error(e.code, e.message, e.status)
    except Exception as e:
        return _error("InternalError", str(e), 500)


def _validate_parameters(template: dict, cfn_params: dict) -> None:
    """Validate parameters against AllowedValues from the template."""
    param_defs = template.get("Parameters", {})
    for pname, pdef in param_defs.items():
        allowed = pdef.get("AllowedValues")
        if not allowed:
            continue
        value = cfn_params.get(pname, str(pdef.get("Default", "")))
        if value and str(value) not in [str(v) for v in allowed]:
            raise CfnError(
                "ValidationError",
                f"Parameter '{pname}' must be one of AllowedValues: "
                f"{', '.join(str(v) for v in allowed)}. Got: {value}",
            )


def _expand_sam_transform(template: dict) -> dict:
    """Expand AWS::Serverless resources into standard CloudFormation resources."""
    transform = template.get("Transform")
    if transform != "AWS::Serverless-2016-10-31":
        if not (isinstance(transform, list) and "AWS::Serverless-2016-10-31" in transform):
            return template

    template = copy.deepcopy(template)
    resources = template.get("Resources", {})
    new_resources: dict[str, dict] = {}

    for logical_id, res_def in list(resources.items()):
        if res_def.get("Type") != "AWS::Serverless::Function":
            continue

        props = res_def.get("Properties", {})

        role_ref: dict | str = props.get("Role", "")
        role_id = f"{logical_id}Role"
        if not role_ref:
            new_resources[role_id] = {
                "Type": "AWS::IAM::Role",
                "Properties": {
                    "AssumeRolePolicyDocument": {
                        "Version": "2012-10-17",
                        "Statement": [
                            {
                                "Effect": "Allow",
                                "Principal": {"Service": "lambda.amazonaws.com"},
                                "Action": "sts:AssumeRole",
                            }
                        ],
                    },
                    "ManagedPolicyArns": [
                        "arn:aws:iam::aws:policy/service-role/AWSLambdaBasicExecutionRole"
                    ],
                },
            }
            role_ref = {"Fn::GetAtt": [role_id, "Arn"]}

        code: dict = {}
        if "InlineCode" in props:
            code = {"ZipFile": props["InlineCode"]}
        elif "CodeUri" in props:
            code_uri = props["CodeUri"]
            if isinstance(code_uri, str):
                code = {"S3Bucket": "sam-artifacts", "S3Key": code_uri}
            elif isinstance(code_uri, dict):
                code = {
                    "S3Bucket": code_uri.get("Bucket", "sam-artifacts"),
                    "S3Key": code_uri.get("Key", code_uri.get("Uri", "")),
                }
                if "Version" in code_uri:
                    code["S3ObjectVersion"] = code_uri["Version"]

        lambda_props: dict = {
            "Runtime": props.get("Runtime", "python3.12"),
            "Handler": props.get("Handler", "index.handler"),
            "Code": code,
            "Role": role_ref,
        }
        opt_keys = ("FunctionName", "Timeout", "MemorySize", "Environment", "Layers", "Description")
        for opt_key in opt_keys:
            if opt_key in props:
                lambda_props[opt_key] = props[opt_key]

        depends: list[str] = []
        if role_id in new_resources:
            depends.append(role_id)

        resources[logical_id] = {
            "Type": "AWS::Lambda::Function",
            "Properties": lambda_props,
        }
        if depends:
            resources[logical_id]["DependsOn"] = depends

        for event_name, event_def in props.get("Events", {}).items():
            event_type = event_def.get("Type", "")
            event_props = event_def.get("Properties", {})

            if event_type == "SQS":
                esm_id = f"{logical_id}{event_name}ESM"
                new_resources[esm_id] = {
                    "Type": "AWS::Lambda::EventSourceMapping",
                    "Properties": {
                        "FunctionName": {"Ref": logical_id},
                        "EventSourceArn": event_props.get("Queue"),
                        "BatchSize": event_props.get("BatchSize", 10),
                    },
                    "DependsOn": [logical_id],
                }
            elif event_type == "Api":
                api_id = f"{logical_id}Api"
                if api_id not in new_resources:
                    new_resources[api_id] = {
                        "Type": "AWS::ApiGateway::RestApi",
                        "Properties": {
                            "Name": {"Fn::Sub": "${AWS::StackName}-api"},
                        },
                    }

    resources.update(new_resources)
    template["Resources"] = resources
    template.pop("Transform", None)
    return template


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

    # Parse template to get description, expanding SAM transforms first
    template = parse_template(template_body)
    template = _expand_sam_transform(template)
    description = template.get("Description", "")

    # Validate parameters against AllowedValues
    _validate_parameters(template, cfn_params)

    stack = CfnStack(
        stack_id=stack_id,
        stack_name=name,
        template_body=template_body,
        parameters=cfn_params,
        tags=tags,
        description=description,
    )

    # Add initial event
    _add_event(stack, name, "AWS::CloudFormation::Stack", stack_id, "CREATE_IN_PROGRESS")

    # Deploy resources
    try:
        _deploy_stack(stack, region, account_id, store)
        stack.status = "CREATE_COMPLETE"
        _add_event(stack, name, "AWS::CloudFormation::Stack", stack_id, "CREATE_COMPLETE")
    except Exception as e:
        # Rollback: delete any resources that were created
        for logical_id in reversed(list(stack.resources.keys())):
            try:
                delete_resource(stack.resources[logical_id], region, account_id)
            except Exception:
                pass
        stack.status = "ROLLBACK_COMPLETE"
        stack.status_reason = str(e)
        _add_event(stack, name, "AWS::CloudFormation::Stack", stack_id, "ROLLBACK_COMPLETE", str(e))

    store.put_stack(stack)
    return {"StackId": stack_id}


def _add_event(
    stack: CfnStack,
    logical_id: str,
    resource_type: str,
    physical_id: str,
    status: str,
    reason: str = "",
) -> None:
    """Add a stack event."""
    event = {
        "StackId": stack.stack_id,
        "StackName": stack.stack_name,
        "EventId": _new_id(),
        "LogicalResourceId": logical_id,
        "PhysicalResourceId": physical_id,
        "ResourceType": resource_type,
        "ResourceStatus": status,
        "Timestamp": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
    }
    if reason:
        event["ResourceStatusReason"] = reason
    stack.events.append(event)


def _deploy_stack(
    stack: CfnStack, region: str, account_id: str, store: CfnStore | None = None
) -> None:
    """Parse template and create all resources in dependency order."""
    template = parse_template(stack.template_body)
    template = _expand_sam_transform(template)

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

    # Evaluate conditions
    conditions = evaluate_conditions(
        template, stack.resources, stack.parameters, region, account_id
    )
    # Store conditions so Fn::If can use them
    stack.parameters["__conditions__"] = conditions

    # Collect global exports for Fn::ImportValue resolution
    if store:
        stack.parameters["__imports__"] = dict(store.exports)

    for logical_id in order:
        res_def = resource_defs[logical_id]

        # Check if resource has a Condition that evaluates to false
        condition_name = res_def.get("Condition")
        if condition_name and not conditions.get(condition_name, True):
            # Skip this resource — condition is false
            continue

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

        _add_event(stack, logical_id, res_type, "", "CREATE_IN_PROGRESS")
        create_resource(resource, region, account_id)
        _add_event(stack, logical_id, res_type, resource.physical_id or "", "CREATE_COMPLETE")
        stack.resources[logical_id] = resource

    # Resolve outputs
    for out_name, out_def in template.get("Outputs", {}).items():
        # Check output condition
        condition_name = out_def.get("Condition")
        if condition_name and not conditions.get(condition_name, True):
            continue

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
            export_str = str(export_name)
            stack.outputs[out_name]["ExportName"] = export_str
            stack.exports[export_str] = str(value)
            # Also register in global store
            if store:
                store.exports[export_str] = str(value)


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

    # Clean up exports from the global store (categorical: deletion must cascade)
    for export_name in list(stack.exports.keys()):
        store.exports.pop(export_name, None)

    stack.status = "DELETE_COMPLETE"
    # Keep stack in store for list_stacks DELETE_COMPLETE queries
    return {}


def _describe_stacks(store: CfnStore, params: dict, region: str, account_id: str) -> dict:
    name = params.get("StackName")
    if name:
        stack = store.get_stack(name)
        if not stack:
            raise CfnError("ValidationError", f"Stack with id {name} does not exist")
        stacks = [stack]
    else:
        # Exclude DELETE_COMPLETE stacks from describe_stacks (matches AWS behavior)
        stacks = [s for s in store.list_stacks() if s.status != "DELETE_COMPLETE"]

    members = []
    for s in stacks:
        member = {
            "StackId": s.stack_id,
            "StackName": s.stack_name,
            "StackStatus": s.status,
            "CreationTime": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime(s.created)),
        }
        if s.description:
            member["Description"] = s.description
        if s.status_reason:
            member["StackStatusReason"] = s.status_reason
        if s.outputs:
            member["Outputs"] = list(s.outputs.values())
        if s.parameters:
            # Filter out pseudo-parameters and internal keys
            _pseudo = {
                "AWS::Region",
                "AWS::AccountId",
                "AWS::StackName",
                "AWS::StackId",
                "AWS::URLSuffix",
                "AWS::NoValue",
                "AWS::NotificationARNs",
                "AWS::Partition",
                "__conditions__",
                "__imports__",
            }
            member["Parameters"] = [
                {"ParameterKey": k, "ParameterValue": v}
                for k, v in s.parameters.items()
                if k not in _pseudo
            ]
        if s.tags:
            member["Tags"] = s.tags
        members.append(member)

    return {"Stacks": members}


def _list_stacks(store: CfnStore, params: dict, region: str, account_id: str) -> dict:
    stacks = store.list_stacks()
    # Parse StackStatusFilter from query protocol
    status_filter = set()
    i = 1
    while f"StackStatusFilter.member.{i}" in params:
        status_filter.add(params[f"StackStatusFilter.member.{i}"])
        i += 1
    if status_filter:
        stacks = [s for s in stacks if s.status in status_filter]
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

    # Parse tags (categorical: every mutating action must parse tags)
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

    # Detect no-changes update
    if template_body == stack.template_body and not cfn_params and not tags:
        raise CfnError(
            "ValidationError",
            "No updates are to be performed.",
        )

    stack.status = "UPDATE_IN_PROGRESS"
    _add_event(stack, name, "AWS::CloudFormation::Stack", stack.stack_id, "UPDATE_IN_PROGRESS")

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
        if tags:
            stack.tags = tags

        # Deploy new resources
        _deploy_stack(stack, region, account_id, store)
        stack.status = "UPDATE_COMPLETE"
        _add_event(stack, name, "AWS::CloudFormation::Stack", stack.stack_id, "UPDATE_COMPLETE")
    except Exception as e:
        stack.status = "UPDATE_FAILED"
        stack.status_reason = str(e)
        _add_event(
            stack, name, "AWS::CloudFormation::Stack", stack.stack_id, "UPDATE_FAILED", str(e)
        )

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

    result: dict = {"Parameters": []}
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


def _describe_stack_events(store: CfnStore, params: dict, region: str, account_id: str) -> dict:
    name = params.get("StackName", "")
    stack = store.get_stack(name)
    if not stack:
        raise CfnError("ValidationError", f"Stack [{name}] does not exist")
    # Return events in reverse chronological order (newest first)
    return {"StackEvents": list(reversed(stack.events))}


def _list_stack_resources(store: CfnStore, params: dict, region: str, account_id: str) -> dict:
    name = params.get("StackName", "")
    stack = store.get_stack(name)
    if not stack:
        raise CfnError("ValidationError", f"Stack [{name}] does not exist")

    summaries = []
    for lid, res in stack.resources.items():
        summaries.append(
            {
                "LogicalResourceId": lid,
                "PhysicalResourceId": res.physical_id or "",
                "ResourceType": res.resource_type,
                "ResourceStatus": res.status,
                "LastUpdatedTimestamp": time.strftime(
                    "%Y-%m-%dT%H:%M:%SZ", time.gmtime(stack.created)
                ),
            }
        )
    return {"StackResourceSummaries": summaries}


def _describe_stack_resource(store: CfnStore, params: dict, region: str, account_id: str) -> dict:
    name = params.get("StackName", "")
    logical_id = params.get("LogicalResourceId", "")
    stack = store.get_stack(name)
    if not stack:
        raise CfnError("ValidationError", f"Stack [{name}] does not exist")

    res = stack.resources.get(logical_id)
    if not res:
        raise CfnError(
            "ValidationError",
            f"Resource [{logical_id}] does not exist in stack [{name}]",
        )

    return {
        "StackResourceDetail": {
            "StackId": stack.stack_id,
            "StackName": stack.stack_name,
            "LogicalResourceId": logical_id,
            "PhysicalResourceId": res.physical_id or "",
            "ResourceType": res.resource_type,
            "ResourceStatus": res.status,
            "LastUpdatedTimestamp": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime(stack.created)),
        }
    }


def _create_change_set(store: CfnStore, params: dict, region: str, account_id: str) -> dict:
    stack_name = params.get("StackName", "")
    cs_name = params.get("ChangeSetName", "")
    template_body = params.get("TemplateBody", "")
    cs_type = params.get("ChangeSetType", "UPDATE")

    if not stack_name:
        raise CfnError("ValidationError", "StackName is required")
    if not cs_name:
        raise CfnError("ValidationError", "ChangeSetName is required")

    # Parse parameters
    cfn_params = {}
    i = 1
    while f"Parameters.member.{i}.ParameterKey" in params:
        key = params[f"Parameters.member.{i}.ParameterKey"]
        value = params.get(f"Parameters.member.{i}.ParameterValue", "")
        cfn_params[key] = value
        i += 1

    cs_id = f"arn:aws:cloudformation:{region}:{account_id}:changeSet/{cs_name}/{_new_id()}"

    # For CREATE type, also create a stub stack if it doesn't exist
    stack = store.get_stack(stack_name)
    stack_id = (
        stack.stack_id
        if stack
        else (f"arn:aws:cloudformation:{region}:{account_id}:stack/{stack_name}/{_new_id()}")
    )
    if not stack and cs_type == "CREATE":
        stub = CfnStack(
            stack_id=stack_id,
            stack_name=stack_name,
            template_body=template_body,
            status="REVIEW_IN_PROGRESS",
        )
        store.put_stack(stub)

    cs = CfnChangeSet(
        change_set_id=cs_id,
        change_set_name=cs_name,
        stack_name=stack_name,
        stack_id=stack_id,
        template_body=template_body,
        change_set_type=cs_type,
        parameters=cfn_params,
        status="CREATE_COMPLETE",
    )
    with store.mutex:
        store.change_sets[cs_id] = cs

    return {"Id": cs_id, "StackId": stack_id}


def _describe_change_set(store: CfnStore, params: dict, region: str, account_id: str) -> dict:
    cs_name = params.get("ChangeSetName", "")
    stack_name = params.get("StackName", "")

    cs = None
    with store.mutex:
        # Look up by ID or name
        if cs_name in store.change_sets:
            cs = store.change_sets[cs_name]
        else:
            for c in store.change_sets.values():
                if c.change_set_name == cs_name and (not stack_name or c.stack_name == stack_name):
                    cs = c
                    break

    if not cs:
        raise CfnError(
            "ChangeSetNotFoundException",
            f"ChangeSet [{cs_name}] does not exist",
            404,
        )

    return {
        "ChangeSetId": cs.change_set_id,
        "ChangeSetName": cs.change_set_name,
        "StackId": cs.stack_id or "",
        "StackName": cs.stack_name,
        "Status": cs.status,
        "StatusReason": cs.status_reason,
        "Changes": "",
    }


def _delete_change_set(store: CfnStore, params: dict, region: str, account_id: str) -> dict:
    cs_name = params.get("ChangeSetName", "")
    stack_name = params.get("StackName", "")

    with store.mutex:
        to_delete = None
        if cs_name in store.change_sets:
            to_delete = cs_name
        else:
            for cs_id, c in store.change_sets.items():
                if c.change_set_name == cs_name and (not stack_name or c.stack_name == stack_name):
                    to_delete = cs_id
                    break
        if to_delete:
            del store.change_sets[to_delete]

    return {}


def _execute_change_set(store: CfnStore, params: dict, region: str, account_id: str) -> dict:
    cs_name = params.get("ChangeSetName", "")
    stack_name = params.get("StackName", "")

    # Look up the change set
    cs = None
    with store.mutex:
        if cs_name in store.change_sets:
            cs = store.change_sets[cs_name]
        else:
            for c in store.change_sets.values():
                if c.change_set_name == cs_name and (not stack_name or c.stack_name == stack_name):
                    cs = c
                    break

    if not cs:
        raise CfnError(
            "ChangeSetNotFoundException",
            f"ChangeSet [{cs_name}] does not exist",
            404,
        )

    if cs.status == "EXECUTE_COMPLETE":
        raise CfnError(
            "InvalidChangeSetStatusException",
            f"ChangeSet [{cs.change_set_id}] is in EXECUTE_COMPLETE state and cannot be executed.",
        )

    # Look up the stack
    stack = store.get_stack(cs.stack_name)

    if cs.change_set_type == "CREATE":
        # For CREATE type, the stub stack was created in _create_change_set with
        # REVIEW_IN_PROGRESS status. Update it with template + params and deploy.
        if not stack:
            raise CfnError("ValidationError", f"Stack [{cs.stack_name}] does not exist")

        stack.template_body = cs.template_body
        stack.parameters = dict(cs.parameters)
        stack.status = "CREATE_IN_PROGRESS"
        _add_event(
            stack,
            stack.stack_name,
            "AWS::CloudFormation::Stack",
            stack.stack_id,
            "CREATE_IN_PROGRESS",
        )

        try:
            _deploy_stack(stack, region, account_id, store)
            stack.status = "CREATE_COMPLETE"
            _add_event(
                stack,
                stack.stack_name,
                "AWS::CloudFormation::Stack",
                stack.stack_id,
                "CREATE_COMPLETE",
            )
        except Exception as e:
            for logical_id in reversed(list(stack.resources.keys())):
                try:
                    delete_resource(stack.resources[logical_id], region, account_id)
                except Exception:
                    pass
            stack.status = "ROLLBACK_COMPLETE"
            stack.status_reason = str(e)
            _add_event(
                stack,
                stack.stack_name,
                "AWS::CloudFormation::Stack",
                stack.stack_id,
                "ROLLBACK_COMPLETE",
                str(e),
            )

    else:
        # UPDATE type: update existing stack with new template + params
        if not stack:
            raise CfnError("ValidationError", f"Stack [{cs.stack_name}] does not exist")

        stack.status = "UPDATE_IN_PROGRESS"
        _add_event(
            stack,
            stack.stack_name,
            "AWS::CloudFormation::Stack",
            stack.stack_id,
            "UPDATE_IN_PROGRESS",
        )

        try:
            # Delete old resources in reverse order
            for logical_id in reversed(list(stack.resources.keys())):
                try:
                    delete_resource(stack.resources[logical_id], region, account_id)
                except Exception:
                    pass

            stack.resources = OrderedDict()
            stack.outputs = {}
            stack.template_body = cs.template_body
            stack.parameters = dict(cs.parameters)

            _deploy_stack(stack, region, account_id, store)
            stack.status = "UPDATE_COMPLETE"
            _add_event(
                stack,
                stack.stack_name,
                "AWS::CloudFormation::Stack",
                stack.stack_id,
                "UPDATE_COMPLETE",
            )
        except Exception as e:
            stack.status = "UPDATE_FAILED"
            stack.status_reason = str(e)
            _add_event(
                stack,
                stack.stack_name,
                "AWS::CloudFormation::Stack",
                stack.stack_id,
                "UPDATE_FAILED",
                str(e),
            )

    store.put_stack(stack)
    cs.status = "EXECUTE_COMPLETE"
    return {}


def _list_exports(store: CfnStore, params: dict, region: str, account_id: str) -> dict:
    exports = []
    for export_name, export_value in store.exports.items():
        exports.append(
            {
                "ExportingStackId": "",
                "Name": export_name,
                "Value": str(export_value),
            }
        )
    return {"Exports": exports}


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
    "DescribeStackResource": _describe_stack_resource,
    "DescribeStackEvents": _describe_stack_events,
    "ListStackResources": _list_stack_resources,
    "ListExports": _list_exports,
    "GetTemplate": _get_template,
    "ValidateTemplate": _validate_template,
    "CreateChangeSet": _create_change_set,
    "DescribeChangeSet": _describe_change_set,
    "DeleteChangeSet": _delete_change_set,
    "ExecuteChangeSet": _execute_change_set,
}
