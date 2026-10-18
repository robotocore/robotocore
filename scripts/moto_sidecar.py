"""Moto bridge sidecar - wraps Moto backends behind a minimal HTTP server.

This is the Python component of the Rust-Moto bridge. The Rust robotocore
server handles native services directly and proxies all other AWS service
requests to this sidecar, which dispatches them to Moto backends.

Run with:
    python scripts/moto_sidecar.py --port 4567

The Rust server talks to this over HTTP on localhost.
"""

import argparse
import asyncio
import io
import json
import logging
import os
import re
import time

os.environ.setdefault("MOTO_ALLOW_NONEXISTENT_REGION", "true")

import moto.backends as moto_backends
import uvicorn
from moto.core.base_backend import BackendDict
from starlette.applications import Starlette
from starlette.requests import Request
from starlette.responses import Response
from starlette.routing import Route
from urllib.parse import parse_qsl
from werkzeug.datastructures import ImmutableMultiDict
from werkzeug.routing import Map, Rule
from werkzeug.routing.converters import BaseConverter
from werkzeug.test import EnvironBuilder
from werkzeug.wrappers import Request as WerkzeugRequest

DEFAULT_ACCOUNT_ID = "123456789012"

logger = logging.getLogger("moto_sidecar")


def _error_response(service_name: str, error_code: str, message: str, status_code: int):
    """Protocol-aware error response, mirroring the Python gateway's moto path.

    JSON-protocol services get `{"__type": code, "message": msg}`; everything
    else gets the AWS query-protocol XML error document.
    """
    import xml.sax.saxutils as _sax

    try:
        from robotocore.protocols.service_info import get_service_protocol

        protocol = get_service_protocol(service_name) or "query"
    except Exception:  # noqa: BLE001
        protocol = "query"

    if protocol in ("json", "rest-json", "smithy-rpc-v2-cbor"):
        return Response(
            content=json.dumps({"__type": error_code, "message": message}),
            status_code=status_code,
            media_type="application/x-amz-json-1.0",
        )
    safe_message = _sax.escape(message)
    body = (
        f"<ErrorResponse><Error><Code>{error_code}</Code>"
        f"<Message>{safe_message}</Message></Error></ErrorResponse>"
    )
    return Response(content=body, status_code=status_code, media_type="application/xml")


# ---------------------------------------------------------------------------
# CloudFormation parity hooks.
#
# On the Python ground-truth server CloudFormation is a native provider whose
# engine instantiates real resources in the *native* stores (SQS, SNS,
# DynamoDB, IAM, SSM - see services/cloudformation/resources.py). Moto
# instead creates moto-side resources which the Rust server's native
# services cannot see. To keep cross-service behavior on par, these hooks:
#   1. reimplement the few ops where moto's response shape or error code
#      diverges from the Python provider (ValidateTemplate, ListStackResources
#      timestamps, DescribeChangeSet not-found),
#   2. mirror moto's resulting stack resources into the Rust server's native
#      stores over the real AWS APIs (boto3 pointed back at the Rust server).
# Mirroring is best-effort: failures are logged and never break the moto
# response itself.
# ---------------------------------------------------------------------------

_MIRROR_ACTIONS_CREATE = {"CreateStack", "UpdateStack", "ExecuteChangeSet"}
_MIRROR_ACTIONS_DELETE = {"DeleteStack"}

_pre_stack_state: dict = {}


def _callback_base() -> str:
    """Base URL of the Rust (native) server, used for state mirroring."""
    url = os.environ.get("ROBOTOCORE_CALLBACK_URL")
    if url:
        return url.rstrip("/")
    return f"http://127.0.0.1:{os.environ.get('RUST_PORT', '4567')}"


_boto_clients: dict = {}


def _boto(service: str, account_id: str, region: str):
    import boto3
    import botocore.config

    key = (service, account_id, region)
    if key not in _boto_clients:
        _boto_clients[key] = boto3.client(
            service,
            endpoint_url=_callback_base(),
            region_name=region,
            aws_access_key_id=account_id or DEFAULT_ACCOUNT_ID,
            aws_secret_access_key="testing",
            config=botocore.config.Config(
                retries={"total_max_attempts": 1},
                read_timeout=20,
                connect_timeout=3,
            ),
        )
    return _boto_clients[key]


def _moto_backend(service: str, account_id: str, region: str):
    """Fetch a moto service backend for a concrete account/region."""
    d = moto_backends.get_backend(service)
    try:
        if isinstance(d, BackendDict):
            return d[account_id][region]
        return d[region]
    except (KeyError, TypeError):
        return None


def _stack_for(backend, name_or_id):
    """Find a moto CloudFormation stack by name, stack-id, or arn."""
    if backend is None:
        return None
    for stack in backend.stacks.values():
        if name_or_id in (stack.name, stack.stack_id):
            return stack
    return None


def _mirror_sqs(name_url: str, account_id, region):
    name = name_url.rstrip("/").rsplit("/", 1)[-1]
    if not name:
        return
    _boto("sqs", account_id, region).create_queue(QueueName=name)


def _mirror_sqs_delete(name_url: str, account_id, region):
    name = name_url.rstrip("/").rsplit("/", 1)[-1]
    if not name:
        return
    try:
        url = _boto("sqs", account_id, region).get_queue_url(QueueName=name)["QueueUrl"]
        _boto("sqs", account_id, region).delete_queue(QueueUrl=url)
    except Exception as e:  # noqa: BLE001
        logger.debug("mirror sqs delete %s failed: %s", name, e)


def _mirror_sns(topic_arn: str, account_id, region):
    name = topic_arn.rsplit(":", 1)[-1]
    if not name:
        return
    _boto("sns", account_id, region).create_topic(Name=name)


def _mirror_sns_delete(topic_arn: str, account_id, region):
    try:
        _boto("sns", account_id, region).delete_topic(TopicArn=topic_arn)
    except Exception as e:  # noqa: BLE001
        logger.debug("mirror sns delete %s failed: %s", topic_arn, e)


from copy import deepcopy


def _resolved_stack_resources(stack):
    """(logical_id, resource_type, resolved props, physical_resource_id) list.

    Resolves CFN intrinsics through moto's own parse_resource so Ref/Fn::Sub/
    Fn::GetAtt values in properties are concrete names/ARNs - this is what the
    mirror layer needs to recreate the same resources in the Rust server.
    """
    if stack is None:
        return []
    from moto.cloudformation.parsing import parse_resource, clean_json

    tdict = getattr(stack, "template_dict", None) or {}
    rmap = getattr(stack, "resource_map", {}) or {}
    out = []
    for lid, rj in (tdict.get("Resources") or {}).items():
        rtype = rj.get("Type", "")
        props: dict = {}
        try:
            result = parse_resource(deepcopy(rj), rmap)
            if result is None:
                # Unsupported resource type (e.g. AWS::SNS::Subscription):
                # resolve properties manually with moto's clean_json so
                # Ref/Fn::Sub/Fn::GetAtt still yield concrete values.
                raw_props = rj.get("Properties") or {}
                for k, v in raw_props.items():
                    try:
                        props[k] = clean_json(deepcopy(v), rmap)
                    except Exception:  # noqa: BLE001
                        props[k] = v
            else:
                _cls, resolved, rtype = result
                props = dict(resolved.get("Properties") or {})
        except Exception as e:  # noqa: BLE001
            logger.debug("resolve %s failed (%s), using physical id fallback", lid, e)
        inst = rmap.get(lid)
        rtype = getattr(inst, "cf_resource_type", "") or rtype
        phys = getattr(inst, "physical_resource_id", "") if inst is not None else ""
        if not isinstance(phys, str):
            phys = ""
        out.append((lid, rtype, props, phys))
    return out


def _mirror_sns_subscription(props: dict, account_id, region):
    topic_arn = props.get("TopicArn") or ""
    protocol = props.get("Protocol") or "sqs"
    endpoint = props.get("Endpoint") or ""
    if not (topic_arn and endpoint):
        return
    try:
        _boto("sns", account_id, region).subscribe(
            TopicArn=topic_arn, Protocol=protocol, Endpoint=endpoint
        )
    except Exception as e:  # noqa: BLE001
        logger.debug("mirror sns subscribe failed: %s", e)


def _mirror_dynamo_table(props: dict, phys: str, account_id, region):
    name = props.get("TableName") or phys or ""
    if not name:
        return
    ddb = _moto_backend("dynamodb", account_id, region)
    moto_table = None
    if ddb is not None:
        try:
            moto_table = ddb.get_table(name)
        except Exception:  # noqa: BLE001
            moto_table = None

    kwargs = {"TableName": name}
    if moto_table is not None:
        for src, dst in (
            ("attribute_definitions", "AttributeDefinitions"),
            ("key_schema", "KeySchema"),
            ("global_secondary_indexes", "GlobalSecondaryIndexes"),
            ("local_secondary_indexes", "LocalSecondaryIndexes"),
        ):
            val = getattr(moto_table, src, None)
            if val:
                kwargs[dst] = json.loads(json.dumps(val, default=str))
        if getattr(moto_table, "billing_mode", None):
            kwargs["BillingMode"] = moto_table.billing_mode
        else:
            pth = getattr(moto_table, "provisioned_throughput", None)
            kwargs.setdefault(
                "ProvisionedThroughput",
                {
                    "ReadCapacityUnits": pth.get("ReadCapacityUnits", 1)
                    if isinstance(pth, dict)
                    else 1,
                    "WriteCapacityUnits": pth.get("WriteCapacityUnits", 1)
                    if isinstance(pth, dict)
                    else 1,
                },
            )
    else:
        kwargs["AttributeDefinitions"] = props.get("AttributeDefinitions") or [
            {"AttributeName": "id", "AttributeType": "S"}
        ]
        kwargs["KeySchema"] = props.get("KeySchema") or [{"AttributeName": "id", "KeyType": "HASH"}]
        if props.get("BillingMode"):
            kwargs["BillingMode"] = props["BillingMode"]
        kwargs.setdefault(
            "ProvisionedThroughput", {"ReadCapacityUnits": 1, "WriteCapacityUnits": 1}
        )
    try:
        _boto("dynamodb", account_id, region).create_table(**kwargs)
    except Exception as e:  # noqa: BLE001
        logger.debug("mirror dynamo create %s failed: %s", name, e)


def _mirror_dynamo_delete(name: str, account_id, region):
    try:
        _boto("dynamodb", account_id, region).delete_table(TableName=name)
    except Exception as e:  # noqa: BLE001
        logger.debug("mirror dynamo delete %s failed: %s", name, e)


def _mirror_iam_role(props: dict, phys: str, account_id, region):
    name = props.get("RoleName") or phys or ""
    if not name:
        return
    assume = props.get("AssumeRolePolicyDocument")
    if assume is None:
        iam = _moto_backend("iam", account_id, region)
        role = getattr(iam, "roles", {}).get(name) if iam is not None else None
        assume = getattr(role, "assume_role_policy_document", None) if role else None
    try:
        _boto("iam", account_id, region).create_role(
            RoleName=name,
            AssumeRolePolicyDocument=assume if isinstance(assume, str) else json.dumps(assume or {}),
        )
    except Exception as e:  # noqa: BLE001
        logger.debug("mirror iam create %s failed: %s", name, e)


def _mirror_iam_delete(name: str, account_id, region):
    try:
        _boto("iam", account_id, region).delete_role(RoleName=name)
    except Exception as e:  # noqa: BLE001
        logger.debug("mirror iam delete %s failed: %s", name, e)


def _mirror_ssm_parameter(props: dict, phys: str, account_id, region):
    name = props.get("Name") or phys or ""
    value = props.get("Value")
    ptype = props.get("Type", "String")
    if not name:
        return
    try:
        _boto("ssm", account_id, region).put_parameter(
            Name=name, Value=str(value), Type=ptype, Overwrite=True
        )
    except Exception as e:  # noqa: BLE001
        logger.debug("mirror ssm put %s failed: %s", name, e)


def _mirror_ssm_delete(name: str, account_id, region):
    try:
        _boto("ssm", account_id, region).delete_parameter(Name=name)
    except Exception as e:  # noqa: BLE001
        logger.debug("mirror ssm delete %s failed: %s", name, e)


def _snapshot_stack_resources(backend, stack_name):
    """(logical_id, resource_type, props, physical_id) before a delete."""
    stack = _stack_for(backend, stack_name)
    if stack is None:
        return []
    out = []
    for lid, rtype, props, phys in _resolved_stack_resources(stack):
        if not props:
            # Fallback: derive name-bearing props from the physical id so
            # deletes can still target the mirrored resource.
            if rtype == "AWS::SQS::Queue" and phys:
                props = {"QueueName": phys.rstrip("/").rsplit("/", 1)[-1]}
            elif rtype == "AWS::SSM::Parameter" and phys:
                props = {"Name": phys}
        out.append((lid, rtype, props, phys))
    return out


def _mirror_create_side(backend, params, account_id, region):
    stack_name = params.get("StackName", "")
    stack = _stack_for(backend, stack_name)
    if stack is None:
        return
    for lid, rtype, props, phys in _resolved_stack_resources(stack):
        try:
            if rtype == "AWS::SQS::Queue":
                _mirror_sqs(props.get("QueueName") or phys, account_id, region)
            elif rtype == "AWS::SNS::Topic":
                if props.get("TopicName"):
                    _mirror_sns(
                        f"arn:aws:sns:{region}:{account_id}:{props['TopicName']}",
                        account_id,
                        region,
                    )
                elif phys:
                    _mirror_sns(phys, account_id, region)
            elif rtype == "AWS::SNS::Subscription":
                _mirror_sns_subscription(props, account_id, region)
            elif rtype == "AWS::DynamoDB::Table":
                _mirror_dynamo_table(props, phys, account_id, region)
            elif rtype == "AWS::IAM::Role":
                _mirror_iam_role(props, phys, account_id, region)
            elif rtype == "AWS::SSM::Parameter":
                _mirror_ssm_parameter(props, phys, account_id, region)
        except Exception as e:  # noqa: BLE001
            logger.debug("mirror create %s/%s failed: %s", stack_name, lid, e)


def _mirror_delete_side(snapshot, account_id, region):
    for _lid, rtype, props, phys in snapshot:
        if not isinstance(phys, str):
            phys = ""
        try:
            if rtype == "AWS::SQS::Queue":
                name = props.get("QueueName") or (phys.rstrip("/").rsplit("/", 1)[-1] if phys else "")
                if name:
                    _mirror_sqs_delete(name, account_id, region)
            elif rtype == "AWS::SNS::Topic":
                arn = props.get("TopicArn") or ""
                arn = arn if isinstance(arn, str) else phys
                if arn:
                    _mirror_sns_delete(arn, account_id, region)
            elif rtype == "AWS::DynamoDB::Table":
                name = props.get("TableName") or phys
                if name and isinstance(name, str):
                    _mirror_dynamo_delete(name, account_id, region)
            elif rtype == "AWS::IAM::Role":
                name = props.get("RoleName") or (phys if isinstance(phys, str) else "")
                if name:
                    _mirror_iam_delete(name, account_id, region)
            elif rtype == "AWS::SSM::Parameter":
                name = props.get("Name") or (phys if isinstance(phys, str) else "")
                if name:
                    _mirror_ssm_delete(name, account_id, region)
        except Exception as e:  # noqa: BLE001
            logger.debug("mirror delete %s failed: %s", rtype, e)


def _ec2_normalize_vpc_endpoint_types(backend):
    """Moto crashes when a VPC endpoint was created without VpcEndpointType
    ('NoneType' object has no attribute 'lower' in delete_vpc_endpoints).
    AWS defaults such endpoints to Gateway type - set that before dispatch."""
    try:
        for vpe in (getattr(backend, "vpc_end_points", None) or {}).values():
            if getattr(vpe, "vpc_endpoint_type", None) is None:
                vpe.vpc_endpoint_type = "Gateway"
    except Exception as e:  # noqa: BLE001
        logger.debug("vpc endpoint type normalization failed: %s", e)


_EC2_VPCE_ACTIONS = {
    "DeleteVpcEndpoints",
    "ModifyVpcEndpoint",
    "DescribeVpcEndpoints",
    "DescribeVpcEndpointConnections",
    "DescribeVpcEndpointConnectionNotifications",
    "DescribeVpcEndpointServiceConfigurations",
}

# ---------------------------------------------------------------------------
# EC2 placement groups.
#
# Moto has no Create/Describe/DeletePlacementGroup support while the Python
# ground truth (native EC2 provider) does; keep a small in-process store and
# answer these ops directly, mirroring the Python provider's XML shape.
# ---------------------------------------------------------------------------
_pg_store: dict = {}


def _pg_partitioner(account_id: str, region: str) -> dict:
    return _pg_store.setdefault((account_id, region), {})


_EC2_PG_ACTIONS = {"CreatePlacementGroup", "DescribePlacementGroups", "DeletePlacementGroup"}


def _ec2_placement_groups(action: str, params: dict, account_id: str, region: str) -> Response:
    import uuid

    rid = str(uuid.uuid4())
    store = _pg_partitioner(account_id, region)
    name = params.get("GroupName", "")

    if action == "CreatePlacementGroup":
        strategy = (params.get("Strategy") or "cluster").lower()
        count = params.get("PartitionCount") or params.get("SpreadLevel")
        pg = {
            "groupName": name,
            "strategy": strategy,
            "state": "available",
            "groupId": f"pg-{uuid.uuid4().hex[:16]}",
        }
        if count:
            key = "partitionCount" if strategy == "partition" else "spreadLevel"
            pg[key] = count
        if name in store:
            return _error_response(
                "ec2", "InvalidPlacementGroup.Duplicate",
                f"The placement group '{name}' already exists", 409,
            )
        store[name] = pg
        body = (
            '<?xml version="1.0" encoding="UTF-8"?>\n'
            '<CreatePlacementGroupResponse xmlns="http://ec2.amazonaws.com/doc/2016-11-15/">\n'
            f"    <requestId>{rid}</requestId>\n"
            "    <return>true</return>\n"
            "    <placementGroup>\n"
            + "".join(f"        <{k}>{v}</{k}>\n" for k, v in pg.items())
            + "    </placementGroup>\n"
            "</CreatePlacementGroupResponse>"
        )
        return Response(content=body, status_code=200, media_type="application/xml")

    if action == "DescribePlacementGroups":
        # The wire key is GroupName.1 (botocore serialization) but be lenient
        # and accept GroupNames.* spellings as well.
        wanted = [
            v
            for k, v in params.items()
            if k.startswith("GroupName") and k != "GroupName"
        ]
        groups = [g for n, g in store.items() if not wanted or n in wanted]
        items = "".join(
            "        <item>\n"
            + "".join(f"            <{k}>{v}</{k}>\n" for k, v in g.items())
            + "        </item>\n"
            for g in groups
        )
        body = (
            '<?xml version="1.0" encoding="UTF-8"?>\n'
            '<DescribePlacementGroupsResponse xmlns="http://ec2.amazonaws.com/doc/2016-11-15/">\n'
            f"    <requestId>{rid}</requestId>\n"
            "    <placementGroupSet>\n"
            + items
            + "    </placementGroupSet>\n"
            "</DescribePlacementGroupsResponse>"
        )
        return Response(content=body, status_code=200, media_type="application/xml")

    # DeletePlacementGroup
    if name in store:
        store.pop(name)
        body = (
            '<?xml version="1.0" encoding="UTF-8"?>\n'
            '<DeletePlacementGroupResponse xmlns="http://ec2.amazonaws.com/doc/2016-11-15/">\n'
            f"    <requestId>{rid}</requestId>\n"
            "    <return>true</return>\n"
            "</DeletePlacementGroupResponse>"
        )
        return Response(content=body, status_code=200, media_type="application/xml")
    return _error_response(
        "ec2", "InvalidGroup.NotFound", f"The placement group '{name}' was not found", 404,
    )



def _ec2_mirror_iam_instance_profile(params: dict, account_id: str) -> None:
    """AssociateIamInstanceProfile validates the profile against moto's IAM
    backend, but robotocore creates instance profiles in the NATIVE Rust IAM
    store, invisible to moto. Mirror the profile (with its role, when the
    name matches a moto-known role) into moto's IAM state on first use —
    the same cross-store mirroring the CFN pre-dispatch performs.
    """
    # Query-protocol params arrive flat: "IamInstanceProfile.Arn=..." /
    # "IamInstanceProfile.Name=..." (matches moto's own _get_param reads).
    arn = params.get("IamInstanceProfile.Arn")
    name = params.get("IamInstanceProfile.Name")
    if not name and isinstance(arn, str):
        # arn:aws:iam::<acct>:instance-profile/<path>/<name>
        tail = arn.split(":instance-profile/", 1)[-1]
        name = tail.rsplit("/", 1)[-1]
    if not name:
        return
    from moto.backends import get_backend as moto_get_backend

    try:
        backend_dict = moto_get_backend("iam")
        if account_id in backend_dict:
            iam_backend = backend_dict[account_id]["global"]
        else:
            iam_backend = backend_dict[DEFAULT_ACCOUNT_ID]["global"]
    except Exception as e:  # noqa: BLE001
        logger.debug("iam mirror backend unavailable: %s", e)
        return
    if name in getattr(iam_backend, "instance_profiles", {}):
        return
    path = "/"
    if isinstance(arn, str) and ":instance-profile/" in arn:
        tail = arn.split(":instance-profile/", 1)[1]
        if "/" in tail:
            path = "/" + tail.rsplit("/", 1)[0] + "/"
    iam_backend.create_instance_profile(name, path=path, role_names=[], tags=[])

def _ec2_fill_detach_params(werkzeug_request, params: dict, backend) -> None:
    """DetachVolume may come with only VolumeId (as real SDKs sometimes do).
    Moto's dispatch requires InstanceId/Device; recover them from the
    volume's current attachment like the native Python provider does."""
    if not isinstance(backend, object) or backend is None:
        return
    if params.get("Action") != "DetachVolume" or params.get("InstanceId"):
        return
    try:
        vol = backend.get_volume(params.get("VolumeId", ""))
        att = getattr(vol, "attachment", None)
        if att is None:
            return
        new_items = [
            (k, v) for k, v in (werkzeug_request._form_cache or {}).items(multi=True)
        ]
        new_items.append(("InstanceId", att.instance.id))
        if getattr(att, "device", None):
            new_items.append(("Device", att.device))
        werkzeug_request._form_cache = ImmutableMultiDict(new_items)
        params["InstanceId"] = att.instance.id
        if getattr(att, "device", None):
            params["Device"] = att.device
    except Exception as e:  # noqa: BLE001
        logger.debug("detach param fill failed: %s", e)


def _cfn_validate_template(params: dict, service: str) -> Response | None:
    """ValidateTemplate mirroring the Python provider's semantics."""
    template_body = params.get("TemplateBody") or ""
    if not template_body:
        return _error_response(service, "ValidationError", "TemplateBody is required", 400)
    try:
        from robotocore.services.cloudformation.engine import parse_template

        template = parse_template(template_body)
    except Exception as e:  # noqa: BLE001
        return _error_response(service, "ValidationError", f"Template format error: {e}", 400)
    if not template.get("Resources"):
        return _error_response(
            service, "ValidationError", "Template must contain a Resources section", 400
        )
    members = []
    for pname, pdef in (template.get("Parameters") or {}).items():
        member = f"<member><ParameterKey>{pname}</ParameterKey>"
        if "Default" in pdef:
            member += f"<DefaultValue>{pdef['Default']}</DefaultValue>"
        if "Type" in pdef:
            member += f"<ParameterType>{pdef['Type']}</ParameterType>"
        if "Description" in pdef:
            member += f"<Description>{pdef['Description']}</Description>"
        member += "<NoEcho>false</NoEcho></member>"
        members.append(member)
    body = '<ValidateTemplateResponse xmlns="http://cloudformation.amazonaws.com/doc/2010-05-15/">'
    body += "<ValidateTemplateResult>"
    if template.get("Description"):
        body += f"<Description>{template['Description']}</Description>"
    body += "<Parameters>" + "".join(members) + "</Parameters>"
    body += "</ValidateTemplateResult>"
    body += "<ResponseMetadata><RequestId>d67839b1-d0d9-4011-a5c5-123456789012</RequestId></ResponseMetadata>"
    body += "</ValidateTemplateResponse>"
    return Response(content=body, status_code=200, media_type="application/xml")


def _cfn_pre_dispatch(service, action, params, backend):
    """Pre-dispatch hook. Returns None (fall through to moto) or a Response."""
    if service != "cloudformation":
        return None
    if action == "ValidateTemplate":
        return _cfn_validate_template(params, service)
    if action == "DescribeChangeSet":
        cs_name = params.get("ChangeSetName", "")
        try:
            backend.describe_change_set(cs_name)
        except Exception:
            return _error_response(
                service,
                "ChangeSetNotFoundException",
                f"ChangeSet [{cs_name}] does not exist",
                404,
            )
    if action in _MIRROR_ACTIONS_DELETE:
        _pre_stack_state[params.get("StackName", "")] = _snapshot_stack_resources(
            backend, params.get("StackName", "")
        )
    return None


def _cfn_post_process(service, action, params, resp_body, backend, account_id, region):
    """Best-effort normalization after moto produced a response.

    `resp_body` is bytes or None. Returns the (possibly modified) body bytes.
    """
    if service != "cloudformation":
        return resp_body
    if action == "ListStackResources" and resp_body:
        body = resp_body.decode("utf-8", "replace")
        ts = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
        body = re.sub(r"<LastUpdate(d)?Timestamp>[^<]*</LastUpdate(d)?Timestamp>", "", body)
        body = re.sub(
            r"(<ResourceStatus>[^<]*</ResourceStatus>)",
            r"\1<LastUpdatedTimestamp>" + ts + "</LastUpdatedTimestamp>",
            body,
        )
        return body.encode("utf-8")
    if action in _MIRROR_ACTIONS_CREATE:
        try:
            _mirror_create_side(backend, params, account_id, region)
        except Exception as e:  # noqa: BLE001
            logger.debug("mirror create side failed: %s", e)
    elif action in _MIRROR_ACTIONS_DELETE:
        snapshot = _pre_stack_state.pop(params.get("StackName", ""), [])
        if snapshot:
            _mirror_delete_side(snapshot, account_id, region)
    return resp_body


class WerkzeugRawBodyRequest(WerkzeugRequest):
    """Werkzeug request that preserves a raw body for Moto.

    Moto reads `request.body` (boto-style) and also `request.form`
    (query-protocol params live in the urlencoded body). Parse the form from
    the raw body for urlencoded content types so parameters survive, while
    still exposing the undecoded payload via `.body` / `.data`.
    """

    def __init__(self, environ: dict, body: bytes):
        super().__init__(environ)
        self.body = body
        # Single parse for the whole request lifetime: .form and .files must
        # come from the SAME pass (a second parse would read an exhausted
        # stream and raise ClientDisconnected). The `_form_cache` attribute is
        # part of the pre-dispatch contract: mutation hooks (ec2 fill)
        # overwrite it and moto then sees the amended params.
        self._form_cache: ImmutableMultiDict | None = None
        self._parsed: WerkzeugRequest | None = None

    @property
    def data(self) -> bytes:
        return self.body

    def get_data(self, *args, **kwargs) -> bytes:
        return self.body

    @property
    def form(self):
        if self._form_cache is None:
            mimetype = (self.mimetype or "").split(";")[0].strip()
            if mimetype in ("application/x-www-form-urlencoded", "multipart/form-data"):
                if self._parsed is None:
                    self._parsed = WerkzeugRequest(self.environ)
                self._form_cache = self._parsed.form
            else:
                self._form_cache = ImmutableMultiDict(())
        return self._form_cache

    @property
    def files(self):
        if self._parsed is not None:
            return self._parsed.files
        return ImmutableMultiDict(())


class _RegexConverter(BaseConverter):
    part_isolating = False

    def __init__(self, map, *args, **kwargs):
        super().__init__(map, *args, **kwargs)
        self.regex = args[0] if args else ".*"


_routing_cache: dict[str, Map] = {}


def _get_routing_table(service: str, region: str = "us-east-1") -> Map:
    """Build and cache a Werkzeug URL Map from a Moto backend's flask_paths."""
    cache_key = f"{service}:{region}"
    if cache_key in _routing_cache:
        return _routing_cache[cache_key]

    backend_dict = moto_backends.get_backend(service)
    if isinstance(backend_dict, BackendDict):
        # BackendDict auto-creates backends on access
        try:
            backend = backend_dict[DEFAULT_ACCOUNT_ID][region]
        except (KeyError, TypeError):
            backend = backend_dict[DEFAULT_ACCOUNT_ID]["us-east-1"]
    else:
        try:
            backend = backend_dict[region]
        except (KeyError, TypeError):
            backend = backend_dict["global"]

    url_map = Map()
    url_map.converters["regex"] = _RegexConverter

    for url_path, handler in backend.flask_paths.items():
        if url_path in ("", "/"):
            url_map.add(Rule("/", endpoint=handler, strict_slashes=False))
            continue
        if url_path in ("/.*", "/.+"):
            url_map.add(Rule("/<path:__catch_all>", endpoint=handler, strict_slashes=False))
            url_map.add(Rule("/", endpoint=handler, strict_slashes=False))
            continue
        url_map.add(Rule(url_path, endpoint=handler, strict_slashes=False))

    _routing_cache[cache_key] = url_map
    return url_map


def _extract_service(request: Request) -> str:
    """Extract the AWS service name from the request."""
    # 1. X-Amz-Target header
    target = request.headers.get("x-amz-target", "")
    if target:
        prefix = target.split(".")[0]
        # Map known prefixes
        prefix_map = {
            "DynamoDB": "dynamodb",
            "DynamoDBStreams": "dynamodbstreams",
            "SecretsManager": "secretsmanager",
            "secretsmanager": "secretsmanager",
            "AmazonSSM": "ssm",
            "TrentService": "kms",
            "Kinesis_20131202": "kinesis",
            "Firehose_20150804": "firehose",
            "Logs_20140328": "logs",
            "AWSEvents": "events",
            "CloudWatchEvents": "events",
            "monitoring": "cloudwatch",
            "AmazonEC2ContainerRegistry": "ecr",
            "AmazonEC2ContainerServiceV20141113": "ecs",
            "AWSStepFunctions": "stepfunctions",
            "CertificateManager": "acm",
            "RekognitionService": "rekognition",
            "StarlingDoveService": "config",
            "OvertureService": "support",
            "AWSSupport": "support",
        }
        if prefix in prefix_map:
            return prefix_map[prefix]
        # Fallback: use the prefix as-is (lowercase)
        return prefix.lower()

    # 2. Authorization header (credential scope)
    # Format: AWS4-HMAC-SHA256 Credential=KEY/DATE/REGION/SERVICE/aws4_request
    auth = request.headers.get("authorization", "")
    if "Credential=" in auth:
        try:
            cred_part = auth.split("Credential=")[1].split(",")[0]
            # Format: KEY/DATE/REGION/SERVICE/aws4_request
            parts = cred_part.strip().split("/")
            if len(parts) >= 4:
                return parts[3]
        except (IndexError, AttributeError):
            pass

    # 3. Host header
    host = request.headers.get("host", "")
    if ".amazonaws.com" in host or ".amazonaws.com.cn" in host:
        service = host.split(".")[0]
        if service and service != "s3":
            return service

    # 4. Path-based detection
    path = request.url.path
    if path.startswith("/2015-03-31/"):
        return "lambda"
    if path.startswith("/2018-01-01/"):
        return "lambda"
    if path.startswith("/2016-11-15/"):
        return "apigatewayv2"

    # 5. Use the path component as service name (e.g., /rds, /ec2)
    # This is how the Rust proxy sends requests
    parts = path.strip("/").split("/")
    if parts and parts[0]:
        return parts[0]

    # 5. Body-based (Action= parameter for query protocol)
    try:
        body = (
            request.url.query.decode()
            if hasattr(request.url.query, "decode")
            else str(request.url.query)
        )
        if "Action=" in body:
            pass  # need auth header to map action to service
    except Exception:
        pass

    return ""


async def moto_handler(request: Request) -> Response:
    """Main handler - dispatch to Moto backend."""
    body = await request.body()

    # Get the service from the X-Robotocore-Service header,
    # the path component, or extract it from the request
    service = request.headers.get("x-robotocore-service", "")
    if not service:
        parts = request.url.path.strip("/").split("/")
        if parts and parts[0]:
            service = parts[0]
    if not service:
        service = _extract_service(request)

    # Map service names to Moto backend names
    service_map = {
        "amazonsqs": "sqs",
        "s3": "s3",
        "ec2": "ec2",
        "rds": "rds",
        "cloudformation": "cloudformation",
        "lambda": "lambda",
        "dynamodb": "dynamodb",
        "sts": "sts",
        "iam": "iam",
        "sns": "sns",
        "kms": "kms",
        "ssm": "ssm",
        "logs": "logs",
        "events": "events",
        "kinesis": "kinesis",
        "firehose": "firehose",
        "secretsmanager": "secretsmanager",
        "stepfunctions": "stepfunctions",
        "cloudwatch": "cloudwatch",
    }
    service = service_map.get(service, service)

    if not service:
        return Response(
            content=json.dumps({
                "__type": "UnknownService",
                "message": "Could not determine service",
            }),
            status_code=400,
            media_type="application/json",
        )

    # Extract region from auth header
    region = "us-east-1"
    auth = request.headers.get("authorization", "")
    if "Credential=" in auth:
        try:
            cred_part = auth.split("Credential=")[1].split(",")[0]
            parts = cred_part.strip().split("/")
            if len(parts) >= 3:
                region = parts[2]
        except (IndexError, AttributeError):
            pass

    # Extract account id from the auth header (12-digit access key = account ID)
    auth = request.headers.get("authorization", "")
    account_id_from_headers = DEFAULT_ACCOUNT_ID
    if "Credential=" in auth:
        try:
            cred_part = auth.split("Credential=")[1].split(",")[0]
            access_key = cred_part.strip().split("/")[0]
            if access_key.isdigit() and len(access_key) == 12:
                account_id_from_headers = access_key
        except (IndexError, AttributeError):
            pass

    # Build the full URL
    full_url = str(request.url)

    # Find the Moto backend and dispatcher
    try:
        routing_map = _get_routing_table(service, region)
    except Exception as e:
        return _error_response(
            service,
            "NotImplemented",
            f"Moto backend not found for {service}: {e}",
            501,
        )

    # Build the Werkzeug environ
    method = request.method
    # Prefer the raw (percent-encoded) path: Starlette's url.path decodes
    # %2F to '/', which breaks moto's "[^/]+" route converters for ARN
    # path params (bedrock policy ARNs, glacier vault names, ...). The
    # Python gateway forwards raw_path for exactly this reason.
    raw_path = getattr(request, "scope", {}).get("raw_path", b"")
    if raw_path:
        path = raw_path.decode("latin-1").split("?", 1)[0]
    else:
        path = request.url.path
    query = str(request.url.query)

    headers = dict(request.headers)
    # Inject Moto's multi-account header so the correct backend is used
    # (mirrors the Python gateway's moto path).
    headers["x-moto-account-id"] = account_id_from_headers

    environ = EnvironBuilder(
        path=f"{path}?{query}" if query else path,
        method=method,
        data=body,
        headers=headers,
    ).get_environ()

    # The gateway strips Content-Length from forwarded requests; werkzeug's
    # form parser refuses an unterminated stream (400 Bad Request). Re-bind a
    # buffered, length-declared input over the already-known body.
    environ["CONTENT_LENGTH"] = str(len(body))
    environ["wsgi.input"] = io.BytesIO(body)
    environ.pop("HTTP_TRANSFER_ENCODING", None)

    werkzeug_request = WerkzeugRawBodyRequest(environ, body)

    # Query-protocol params for pre-dispatch interception (form + query).
    params: dict = {}
    for k, v in werkzeug_request.form.items():
        params[k] = v
    for k, v in parse_qsl(query, keep_blank_values=True):
        params.setdefault(k, v)

    cfn_backend = None
    if service in ("cloudformation",):
        try:
            cfn_backend = _moto_backend(service, account_id_from_headers, region)
        except Exception:  # noqa: BLE001
            cfn_backend = None
        try:
            pre = await asyncio.to_thread(
                _cfn_pre_dispatch, service, params.get("Action", ""), params, cfn_backend
            )
        except Exception as e:  # noqa: BLE001
            logger.debug("cfn pre-dispatch failed: %s", e)
            pre = None
        if pre is not None:
            return pre
    elif service == "ec2":
        action = params.get("Action", "")
        if action in _EC2_VPCE_ACTIONS:
            try:
                _ec2_normalize_vpc_endpoint_types(
                    _moto_backend("ec2", account_id_from_headers, region)
                )
            except Exception as e:  # noqa: BLE001
                logger.debug("ec2 pre-dispatch failed: %s", e)
        if action in _EC2_PG_ACTIONS:
            return _ec2_placement_groups(
                action, params, account_id_from_headers, region
            )
        if action == "AssociateIamInstanceProfile":
            try:
                _ec2_mirror_iam_instance_profile(params, account_id_from_headers)
            except Exception as e:  # noqa: BLE001
                logger.warning("iam profile mirror failed: %s", e)
        if action == "DetachVolume":
            try:
                _ec2_fill_detach_params(
                    werkzeug_request, params,
                    _moto_backend("ec2", account_id_from_headers, region),
                )
            except Exception as e:  # noqa: BLE001
                logger.debug("ec2 detach fill failed: %s", e)

    # Route to the correct Moto handler
    try:
        binding = routing_map.bind_to_environ(environ)
        endpoint, _args = binding.match()
    except Exception:
        return _error_response(
            service,
            "NotImplemented",
            f"Service {service} is not yet implemented",
            501,
        )

    # Call the Moto dispatcher with (request, full_url, headers)
    from werkzeug.exceptions import HTTPException as WerkzeugHTTPException

    try:
        # moto handlers can block (large describes, flaky mirrors); keep the
        # event loop responsive for health checks and other services.
        result = await asyncio.to_thread(
            endpoint, werkzeug_request, full_url, dict(werkzeug_request.headers)
        )
        if not result:
            return _error_response(
                service,
                "NotImplemented",
                f"Operation not implemented for {service}",
                501,
            )
        status_code, resp_headers, resp_body = result
        if isinstance(resp_body, (str, bytes)) and len(resp_body) == 0:
            resp_body = None
        if isinstance(resp_body, str):
            resp_body = resp_body.encode("utf-8")

        if service == "cloudformation" and resp_body:
            try:
                resp_body = _cfn_post_process(
                    service,
                    params.get("Action", ""),
                    params,
                    resp_body,
                    cfn_backend,
                    account_id_from_headers,
                    region,
                )
            except Exception as e:  # noqa: BLE001
                logger.debug("cfn post-process failed: %s", e)

        headers_dict = dict(resp_headers) if resp_headers else {}
        # Moto handlers don't set Content-Type on their response headers; the
        # response dict is XML. Match the previous behavior (and the default
        # the Python gateway applies) so the bridge doesn't misread XML as
        # JSON. Explicit moto Content-Type wins.
        content_type = None
        for k in headers_dict:
            if k.lower() == "content-type":
                content_type = headers_dict[k]
                break
        if content_type is None:
            content_type = "application/xml"
        clean_headers = {
            k: v
            for k, v in headers_dict.items()
            if k.lower() not in ("content-length", "content-type")
        }
        is_head = method == "HEAD"
        # For HEAD requests the content-length is object metadata; for other
        # methods drop it and let Starlette recompute to avoid h11 framing
        # errors (same approach as the Python gateway).
        if is_head:
            clean_headers = headers_dict
            resp_body = None
            content_type = "application/xml"
        return Response(
            content=resp_body,
            status_code=status_code,
            media_type=content_type,
            headers=clean_headers,
        )
    except NotImplementedError as e:
        import traceback

        traceback.print_exc()
        return _error_response(service, "NotImplemented", str(e), 501)
    except WerkzeugHTTPException as e:
        import traceback
        traceback.print_exc()
        logger.warning("moto raised HTTPException: %r body=%r", e, e.body)
        resp = e.get_response()
        body_text = resp.get_data(as_text=True) if resp else str(e)
        return Response(
            content=body_text,
            status_code=e.code or 400,
            headers={"Content-Type": "application/json"},
        )
    except Exception as e:  # noqa: BLE001
        import traceback

        traceback.print_exc()
        return _error_response(service, "InternalError", str(e), 500)



async def health(request: Request) -> Response:
    """Health check endpoint."""
    services = sorted(moto_backends.backends.keys()) if hasattr(moto_backends, "backends") else []
    return Response(
        content=json.dumps(
            {
                "status": "ok",
                "service": "moto-sidecar",
                "moto_version": __import__("moto").__version__,
                "services": len(services),
            }
        ),
        media_type="application/json",
    )


async def echo_debug(request: Request) -> Response:
    """Debug: dump exactly what the sidecar receives."""
    body = await request.body()
    info = {
        "path": request.url.path,
        "query": str(request.url.query),
        "method": request.method,
        "service_header": request.headers.get("x-robotocore-service"),
        "content_type": request.headers.get("content-type"),
        "authorization": request.headers.get("authorization"),
        "x_amz_target": request.headers.get("x-amz-target"),
        "body_len": len(body),
        "body_preview": body[:300].decode("utf-8", "replace"),
    }
    return Response(content=json.dumps(info), media_type="application/json")


def create_app() -> Starlette:
    """Create the Starlette ASGI application."""
    all_methods = ["GET", "POST", "PUT", "DELETE", "PATCH", "HEAD", "OPTIONS"]
    return Starlette(
        routes=[
            Route("/_sidecar/health", health, methods=["GET"]),
            Route("/_sidecar/echo", echo_debug, methods=["GET", "POST"]),
            Route("/{service:path}", moto_handler, methods=all_methods),
        ],
    )


def main():
    parser = argparse.ArgumentParser(description="Moto bridge sidecar")
    parser.add_argument("--port", type=int, default=4567)
    parser.add_argument("--host", type=str, default="127.0.0.1")
    args = parser.parse_args()

    logging.basicConfig(level=logging.DEBUG if os.environ.get("SIDECAR_DEBUG") else logging.INFO)
    logger.info(f"Starting Moto sidecar on {args.host}:{args.port}")

    app = create_app()
    uvicorn.run(app, host=args.host, port=args.port, log_level="warning")


if __name__ == "__main__":
    main()
