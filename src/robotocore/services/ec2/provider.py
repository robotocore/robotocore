"""Native EC2 provider.

Intercepts operations that Moto doesn't implement or has bugs in:
- CreatePlacementGroup / DescribePlacementGroups / DeletePlacementGroup: Not implemented
- DetachVolume: Moto crashes when InstanceId is omitted
- DeleteVpcEndpoints: Moto crashes with NoneType.lower() error
- EnableFastSnapshotRestores / DisableFastSnapshotRestores / DescribeFastSnapshotRestores:
  Fast Snapshot Restore state machine modeling
- CreateVolume: VolumeInitializationRate validation and volume hydration state tracking
- RunInstances: Capacity check gate + optional guest container execution
- RequestSpotInstances: Capacity check gate
- TerminateInstances: Capacity release + optional guest container cleanup

Also provides Packer-compatible virtual instance transport for opt-in
container-backed instances with SSH/SSM connectivity.
"""

from __future__ import annotations

import base64
import logging
import os
import re
import threading
import uuid
from datetime import UTC, datetime
from typing import Any
from urllib.parse import parse_qs, urlencode
from xml.sax.saxutils import escape as xml_escape

from starlette.requests import Request
from starlette.responses import Response

from robotocore.providers.moto_bridge import forward_to_moto, forward_to_moto_with_body
from robotocore.services.ec2 import ipam as _ipam
from robotocore.services.ec2.capacity import get_capacity_store
from robotocore.services.ec2.guest.executor import (
    get_guest_executor,
    is_guest_executor_enabled,
)

logger = logging.getLogger(__name__)

# Optional packer transport - only loaded when enabled
_packer_transport_available = False
try:
    from .packer.ami_builder import get_ami_builder

    _packer_transport_available = True
except ImportError as e:
    logger.debug("Packer transport not available: %s", e)

# In-memory placement group store: {account_id: {region: {name: group}}}
_placement_groups: dict[str, dict[str, dict[str, dict]]] = {}
_placement_groups_lock = threading.Lock()

VALID_STRATEGIES = {"cluster", "spread", "partition"}

# Fast Snapshot Restore state store: {account_id: {region: {(snapshot_id, az): fsr_state}}}
_fsr_store: dict[str, dict[str, dict[tuple[str, str], dict]]] = {}
_fsr_lock = threading.Lock()

# Volume hydration state store: {account_id: {region: {volume_id: hydration_state}}}
# Hydration states: "cold" (lazy-loaded), "initialized" (fully hydrated),
# "fsr-backed" (instant-ready)
_volume_hydration: dict[str, dict[str, dict[str, dict]]] = {}
_volume_hydration_lock = threading.Lock()

# FSR state machine states
FSR_STATE_ENABLING = "enabling"
FSR_STATE_OPTIMIZING = "optimizing"
FSR_STATE_ENABLED = "enabled"
FSR_STATE_DISABLING = "disabling"
FSR_STATE_DISABLED = "disabled"

# Volume hydration states
HYDRATION_COLD = "cold"
HYDRATION_INITIALIZED = "initialized"
HYDRATION_FSR_BACKED = "fsr-backed"

# AWS-valid VolumeInitializationRate range
MIN_VOLUME_INIT_RATE = 100
MAX_VOLUME_INIT_RATE = 300

# Packer transport configuration
PACKER_TRANSPORT_ENABLED = os.environ.get("ROBOTOCORE_PACKER_TRANSPORT", "").lower() in (
    "1",
    "true",
    "yes",
    "enabled",
)

# Track instances with active transport: {instance_id: transport}
_instance_transports: dict[str, Any] = {}
_instance_transports_lock = threading.Lock()

# State handler registration flag
_default_state_handler_registered = False


def register_state_handler(manager=None) -> None:
    """Register EC2 native state save/load hooks with a state manager."""
    global _default_state_handler_registered

    is_default_manager = manager is None
    if manager is None:
        if _default_state_handler_registered:
            return
        from robotocore.state.manager import get_state_manager

        manager = get_state_manager()

    store = get_capacity_store()
    manager.register_native_handler("ec2_capacity", store.export_state, store.load_state)
    if is_default_manager:
        _default_state_handler_registered = True


def _utc_timestamp() -> str:
    """Return current UTC timestamp in AWS format."""
    return datetime.now(UTC).strftime("%Y-%m-%dT%H:%M:%S.000Z")


def _get_moto_backend(account_id: str, region: str) -> Any:
    """Get the Moto EC2 backend for the given account and region."""
    from moto.backends import get_backend

    return get_backend("ec2")[account_id][region]


def _record_audit(
    service: str,
    operation: str,
    status_code: int,
    account_id: str,
    region: str,
    error: str | None = None,
) -> None:
    """Record an event in the audit log."""
    try:
        from robotocore.audit.log import get_audit_log

        audit = get_audit_log()
        audit.record(
            service=service,
            operation=operation,
            status_code=status_code,
            account_id=account_id,
            region=region,
            error=error,
        )
    except Exception as exc:  # noqa: BLE001
        logger.debug("Could not record audit event: %s", exc)


def _check_chaos_injection(service: str, operation: str, region: str) -> dict | None:
    """Check if a chaos rule applies to this request."""
    try:
        from robotocore.chaos.fault_rules import get_fault_store

        store = get_fault_store()
        return store.find_matching(service, operation, region)
    except Exception as exc:  # noqa: BLE001
        logger.debug("Could not check chaos rules: %s", exc)
        return None


async def handle_ec2_request(request: Request, region: str, account_id: str) -> Response:
    """Handle EC2 requests, intercepting unimplemented operations."""
    # Ensure state handler is registered
    register_state_handler()

    body = await request.body()
    params = parse_qs(body.decode("utf-8")) if body else {}
    # Also check query params
    for key, val in request.query_params.items():
        if key not in params:
            params[key] = [val]

    action = _get_param(params, "Action")

    # Check for chaos injection
    chaos_rule = _check_chaos_injection("ec2", action, region)
    if chaos_rule:
        _record_audit(
            "ec2",
            f"chaos:{chaos_rule.error_code}",
            chaos_rule.status_code,
            account_id,
            region,
            chaos_rule.error_message,
        )
        return _ec2_error(
            chaos_rule.error_code or "InternalError",
            chaos_rule.error_message,
            chaos_rule.status_code,
        )

    if action == "DescribeIpamScopes":
        return _ipam.describe_ipam_scopes(params, region, account_id)

    if action == "DescribeIpamPools":
        return _ipam.describe_ipam_pools(params, region, account_id)

    if action == "CreateVpc" and _get_param(params, "Ipv4IpamPoolId"):
        return await _create_vpc_from_ipam(request, params, region, account_id)
    if action == "DescribeRegions":
        return _same_partition_regions(
            await forward_to_moto(request, "ec2", account_id=account_id), region
        )

    # Handle RunInstances with capacity check and guest execution
    if action == "RunInstances":
        return await _run_instances(request, params, region, account_id)

    # Handle TerminateInstances with guest cleanup
    if action == "TerminateInstances":
        return await _terminate_instances(request, params, region, account_id)

    # Handle RequestSpotInstances with capacity check
    if action == "RequestSpotInstances":
        return await _request_spot_instances(request, params, region, account_id)

    handler = _ACTION_MAP.get(action)
    if handler:
        try:
            response = handler(params, region, account_id)
            # Record successful audit for state-changing operations
            if action in (
                "EnableFastSnapshotRestores",
                "DisableFastSnapshotRestores",
                "CreateVolume",
            ):
                _record_audit("ec2", action, 200, account_id, region)
            return response
        except NotImplementedError as e:
            _record_audit("ec2", action, 501, account_id, region, str(e))
            xml = (
                f'<?xml version="1.0" encoding="UTF-8"?>'
                f"<Response><Errors><Error><Code>NotImplemented</Code>"
                f"<Message>{xml_escape(str(e))}</Message></Error></Errors></Response>"
            )
            return Response(content=xml, status_code=501, media_type="text/xml")
        except Exception as e:  # noqa: BLE001
            _record_audit("ec2", action, 500, account_id, region, str(e))
            xml = (
                f'<?xml version="1.0" encoding="UTF-8"?>'
                f"<Response><Errors><Error><Code>InternalError</Code>"
                f"<Message>{xml_escape(str(e))}</Message></Error></Errors></Response>"
            )
            return Response(content=xml, status_code=500, media_type="text/xml")

    return await forward_to_moto(request, "ec2", account_id=account_id)


def _get_param(params: dict, key: str) -> str:
    vals = params.get(key, [])
    return vals[0] if vals else ""


def _get_int_param(params: dict, key: str, default: int = 0) -> int:
    val = _get_param(params, key)
    if not val:
        return default
    try:
        return int(val)
    except ValueError:
        return default


def _parse_tag_specifications(params: dict, resource_type: str) -> dict[str, str]:
    """Parse TagSpecifications from request params for a given resource type.

    Returns a dict of {tag_key: tag_value} for the specified resource type.
    """
    tags: dict[str, str] = {}

    # TagSpecifications are structured as:
    # TagSpecification.1.ResourceType=volume
    # TagSpecification.1.Tag.1.Key=Name
    # TagSpecification.1.Tag.1.Value=MyVolume
    # TagSpecification.1.Tag.2.Key=Environment
    # TagSpecification.1.Tag.2.Value=Production

    # Find all TagSpecification entries
    spec_index = 1
    while True:
        resource_type_key = f"TagSpecification.{spec_index}.ResourceType"
        if resource_type_key not in params:
            break

        spec_resource_type = _get_param(params, resource_type_key)
        if spec_resource_type == resource_type:
            # Parse tags for this resource type
            tag_index = 1
            while True:
                tag_key = f"TagSpecification.{spec_index}.Tag.{tag_index}.Key"
                tag_value_key = f"TagSpecification.{spec_index}.Tag.{tag_index}.Value"

                if tag_key not in params:
                    break

                key = _get_param(params, tag_key)
                value = _get_param(params, tag_value_key)
                if key:
                    tags[key] = value
                tag_index += 1

        spec_index += 1

    return tags


def _get_param_list(params: dict, prefix: str) -> list[str]:
    """Extract a list of parameters with numeric suffixes."""
    result = []
    i = 1
    while True:
        val = _get_param(params, f"{prefix}.{i}")
        if not val:
            break
        result.append(val)
        i += 1
    return result


async def _create_vpc_from_ipam(
    request: Request, params: dict, region: str, account_id: str
) -> Response:
    """CreateVpc from an IPAM pool: allocate the CIDR, create the VPC, tag the allocation."""
    prepared = _ipam.prepare_create_vpc(params, region, account_id)
    if isinstance(prepared, Response):
        return prepared
    if prepared is None:
        return await forward_to_moto(request, "ec2", account_id=account_id)
    new_params, pool = prepared
    body = urlencode(new_params, doseq=True).encode()
    response = await forward_to_moto_with_body(request, "ec2", body, account_id=account_id)
    cidr = new_params["CidrBlock"][0]
    allocation = next(
        (a for a in pool.allocations.values() if a.cidr == cidr and not a.resource_id), None
    )
    if allocation is not None:
        if response.status_code == 200:
            match = re.search(rb"<vpcId>(vpc-[0-9a-f]+)</vpcId>", response.body)
            allocation.resource_id = match.group(1).decode() if match else ""
        else:
            pool.allocations.pop(allocation.ipam_pool_allocation_id, None)
    return response


# AWS-managed interface endpoint services missing from Moto's catalog.
_EXTRA_ENDPOINT_SERVICES = (
    "oidc-eks",
    "sqs-fips",
    "ec2-fips",
    "acm-pca-fips",
    "eks-fips",
    "sts-fips",
)


def _register_extra_endpoint_services() -> None:
    from moto.ec2.models import vpcs

    for name in _EXTRA_ENDPOINT_SERVICES:
        if name not in vpcs.AWS_ENDPOINT_SERVICES:
            vpcs.AWS_ENDPOINT_SERVICES.append(name)
    vpcs.DEFAULT_VPC_ENDPOINT_SERVICES.clear()  # rebuilt lazily per region with the additions


_register_extra_endpoint_services()

_REGION_ITEM_RE = re.compile(
    rb"<item>(?:(?!</item>).)*?<regionName>([^<]+)</regionName>.*?</item>", re.S
)


def _same_partition_regions(response: Response, region: str) -> Response:
    """DescribeRegions lists only the caller's partition (aws, aws-us-gov, aws-cn), as AWS does."""
    if response.status_code != 200:
        return response
    from moto.utilities.utils import get_partition

    mine = get_partition(region)

    def keep(m: re.Match) -> bytes:
        return m.group(0) if get_partition(m.group(1).decode()) == mine else b""

    body = _REGION_ITEM_RE.sub(keep, response.body)
    headers = {k: v for k, v in response.headers.items() if k.lower() != "content-length"}
    return Response(content=body, status_code=200, headers=headers, media_type="text/xml")


def _ec2_error(code: str, message: str, status_code: int = 400) -> Response:
    """Return a standard EC2 XML error response."""
    xml = (
        f'<?xml version="1.0" encoding="UTF-8"?>'
        f"<Response><Errors><Error><Code>{xml_escape(code)}</Code>"
        f"<Message>{xml_escape(message)}</Message></Error></Errors>"
        f"<RequestID>{uuid.uuid4()}</RequestID></Response>"
    )
    return Response(content=xml, status_code=status_code, media_type="text/xml")


def _create_placement_group(params: dict, region: str, account_id: str) -> Response:
    name = _get_param(params, "GroupName")
    strategy = _get_param(params, "Strategy") or "cluster"
    partition_count = _get_param(params, "PartitionCount")

    if not name:
        return _ec2_error(
            "MissingParameter",
            "The request must contain the parameter GroupName.",
        )

    if strategy not in VALID_STRATEGIES:
        return _ec2_error(
            "InvalidParameterValue",
            f"Value ({strategy}) for parameter strategy is invalid. "
            f"Unknown placement group strategy.",
        )

    with _placement_groups_lock:
        store = _placement_groups.setdefault(account_id, {}).setdefault(region, {})
        if name in store:
            return _ec2_error(
                "InvalidPlacementGroup.Duplicate",
                f"Placement group '{name}' already exists.",
            )

        group_id = f"pg-{uuid.uuid4().hex[:17]}"
        group = {
            "groupName": name,
            "strategy": strategy,
            "state": "available",
            "groupId": group_id,
            "partitionCount": partition_count or ("7" if strategy == "partition" else ""),
        }
        store[name] = group

    partition_count_xml = ""
    if group["partitionCount"]:
        partition_count_xml = (
            f"        <partitionCount>{group['partitionCount']}</partitionCount>\n"
        )

    xml = f"""<?xml version="1.0" encoding="UTF-8"?>
<CreatePlacementGroupResponse xmlns="http://ec2.amazonaws.com/doc/2016-11-15/">
    <requestId>{uuid.uuid4()}</requestId>
    <return>true</return>
    <placementGroup>
        <groupName>{name}</groupName>
        <state>available</state>
        <strategy>{strategy}</strategy>
        <groupId>{group_id}</groupId>
{partition_count_xml}    </placementGroup>
</CreatePlacementGroupResponse>"""
    return Response(content=xml, status_code=200, media_type="text/xml")


def _describe_placement_groups(params: dict, region: str, account_id: str) -> Response:
    with _placement_groups_lock:
        store = _placement_groups.get(account_id, {}).get(region, {})
        names = _get_param_list(params, "GroupName")

        if names:
            for n in names:
                if n not in store:
                    return _ec2_error(
                        "InvalidPlacementGroup.Unknown",
                        f"The placement group '{n}' is unknown.",
                    )
            groups = [store[n] for n in names]
        else:
            groups = list(store.values())

    items = ""
    for g in groups:
        partition_count_xml = ""
        if g.get("partitionCount"):
            partition_count_xml = (
                f"            <partitionCount>{g['partitionCount']}</partitionCount>\n"
            )
        items += f"""        <item>
            <groupName>{g["groupName"]}</groupName>
            <strategy>{g["strategy"]}</strategy>
            <state>{g["state"]}</state>
            <groupId>{g["groupId"]}</groupId>
{partition_count_xml}        </item>
"""

    xml = f"""<?xml version="1.0" encoding="UTF-8"?>
<DescribePlacementGroupsResponse xmlns="http://ec2.amazonaws.com/doc/2016-11-15/">
    <requestId>{uuid.uuid4()}</requestId>
    <placementGroupSet>
{items}    </placementGroupSet>
</DescribePlacementGroupsResponse>"""
    return Response(content=xml, status_code=200, media_type="text/xml")


def _delete_placement_group(params: dict, region: str, account_id: str) -> Response:
    name = _get_param(params, "GroupName")

    if not name:
        return _ec2_error(
            "MissingParameter",
            "The request must contain the parameter GroupName.",
        )

    with _placement_groups_lock:
        store = _placement_groups.get(account_id, {}).get(region, {})
        if name not in store:
            return _ec2_error(
                "InvalidPlacementGroup.Unknown",
                f"The placement group '{name}' is unknown.",
            )
        store.pop(name)

    xml = f"""<?xml version="1.0" encoding="UTF-8"?>
<DeletePlacementGroupResponse xmlns="http://ec2.amazonaws.com/doc/2016-11-15/">
    <requestId>{uuid.uuid4()}</requestId>
    <return>true</return>
</DeletePlacementGroupResponse>"""
    return Response(content=xml, status_code=200, media_type="text/xml")


def _detach_volume(params: dict, region: str, account_id: str) -> Response:
    """DetachVolume — handle missing InstanceId by finding it from the volume."""
    volume_id = _get_param(params, "VolumeId")
    instance_id = _get_param(params, "InstanceId")
    device = _get_param(params, "Device")

    backend = _get_moto_backend(account_id, region)
    volume = backend.get_volume(volume_id)

    if not instance_id and volume.attachment:
        instance_id = volume.attachment.instance.id
    if not device and volume.attachment:
        device = volume.attachment.device

    attachment = backend.detach_volume(volume_id, instance_id, device)

    att_vol_id = attachment.volume.id if hasattr(attachment.volume, "id") else volume_id
    att_inst_id = attachment.instance.id if hasattr(attachment.instance, "id") else instance_id

    xml = f"""<?xml version="1.0" encoding="UTF-8"?>
<DetachVolumeResponse xmlns="http://ec2.amazonaws.com/doc/2016-11-15/">
    <requestId>{uuid.uuid4()}</requestId>
    <volumeId>{att_vol_id}</volumeId>
    <instanceId>{att_inst_id}</instanceId>
    <device>{attachment.device}</device>
    <status>{attachment.status}</status>
</DetachVolumeResponse>"""
    return Response(content=xml, status_code=200, media_type="text/xml")


def _delete_vpc_endpoints(params: dict, region: str, account_id: str) -> Response:
    """DeleteVpcEndpoints — handle Moto NoneType.lower() bug."""
    endpoint_ids = _get_param_list(params, "VpcEndpointId")
    backend = _get_moto_backend(account_id, region)

    for eid in endpoint_ids:
        for ep in list(backend.vpc_end_points.values()):
            if ep.id == eid:
                ep.state = "deleted"
                break

    xml = f"""<?xml version="1.0" encoding="UTF-8"?>
<DeleteVpcEndpointsResponse xmlns="http://ec2.amazonaws.com/doc/2016-11-15/">
    <requestId>{uuid.uuid4()}</requestId>
    <unsuccessful/>
</DeleteVpcEndpointsResponse>"""
    return Response(content=xml, status_code=200, media_type="text/xml")


def _get_snapshot_from_backend(snapshot_id: str, account_id: str, region: str) -> Any:
    """Get a snapshot from the Moto backend."""
    backend = _get_moto_backend(account_id, region)
    return backend.get_snapshot(snapshot_id)


def _get_fsr_key(snapshot_id: str, az: str) -> tuple[str, str]:
    """Generate a unique key for FSR state."""
    return (snapshot_id, az)


def _get_fsr_state(account_id: str, region: str, snapshot_id: str, az: str) -> dict | None:
    """Get the FSR state for a snapshot/AZ pair."""
    with _fsr_lock:
        store = _fsr_store.get(account_id, {}).get(region, {})
        return store.get(_get_fsr_key(snapshot_id, az))


def _set_fsr_state(account_id: str, region: str, snapshot_id: str, az: str, state: dict) -> None:
    """Set the FSR state for a snapshot/AZ pair."""
    with _fsr_lock:
        store = _fsr_store.setdefault(account_id, {}).setdefault(region, {})
        store[_get_fsr_key(snapshot_id, az)] = state


def _is_fsr_enabled(account_id: str, region: str, snapshot_id: str, az: str) -> bool:
    """Check if FSR is enabled for a snapshot/AZ."""
    fsr = _get_fsr_state(account_id, region, snapshot_id, az)
    if fsr is None:
        return False
    return fsr["state"] in (FSR_STATE_ENABLED, FSR_STATE_OPTIMIZING)


def _enable_fast_snapshot_restores(params: dict, region: str, account_id: str) -> Response:
    """EnableFastSnapshotRestores — enable FSR for snapshot/AZ pairs."""
    snapshot_ids = _get_param_list(params, "SourceSnapshotId")
    availability_zones = _get_param_list(params, "AvailabilityZone")

    if not snapshot_ids:
        return _ec2_error(
            "MissingParameter",
            "The request must contain the parameter SourceSnapshotId.",
        )
    if not availability_zones:
        return _ec2_error(
            "MissingParameter",
            "The request must contain the parameter AvailabilityZone.",
        )

    successful_items = []
    unsuccessful_items = []

    for snapshot_id in snapshot_ids:
        snapshot_exists = True
        try:
            _get_snapshot_from_backend(snapshot_id, account_id, region)
        except Exception:  # noqa: BLE001
            snapshot_exists = False

        for az in availability_zones:
            if not snapshot_exists:
                unsuccessful_items.append(
                    {
                        "snapshot_id": snapshot_id,
                        "fast_snapshot_restore_state_errors": [
                            {
                                "availability_zone": az,
                                "error": {
                                    "code": "InvalidSnapshot.NotFound",
                                    "message": f"The snapshot '{snapshot_id}' does not exist.",
                                },
                            }
                        ],
                    }
                )
                continue

            existing = _get_fsr_state(account_id, region, snapshot_id, az)

            if existing and existing["state"] in (
                FSR_STATE_ENABLED,
                FSR_STATE_ENABLING,
                FSR_STATE_OPTIMIZING,
            ):
                unsuccessful_items.append(
                    {
                        "snapshot_id": snapshot_id,
                        "fast_snapshot_restore_state_errors": [
                            {
                                "availability_zone": az,
                                "error": {
                                    "code": "FastSnapshotRestoreStateError",
                                    "message": (
                                        f"Fast snapshot restore is already enabled or enabling "
                                        f"for snapshot {snapshot_id} in availability zone {az}."
                                    ),
                                },
                            }
                        ],
                    }
                )
                continue

            now = _utc_timestamp()
            fsr_state = {
                "snapshot_id": snapshot_id,
                "availability_zone": az,
                "availability_zone_id": "",
                "state": FSR_STATE_ENABLED,
                "state_transition_reason": "Client initiated",
                "owner_id": account_id,
                "owner_alias": "",
                "enabling_time": now,
                "optimizing_time": now,
                "enabled_time": now,
                "disabling_time": "",
                "disabled_time": "",
            }
            _set_fsr_state(account_id, region, snapshot_id, az, fsr_state)
            successful_items.append(
                {
                    "snapshot_id": snapshot_id,
                    "availability_zone": az,
                    "state": fsr_state["state"],
                    "state_transition_reason": fsr_state["state_transition_reason"],
                }
            )

    successful_xml = ""
    for item in successful_items:
        successful_xml += f"""        <item>
            <SnapshotId>{item["snapshot_id"]}</SnapshotId>
            <AvailabilityZone>{item["availability_zone"]}</AvailabilityZone>
            <State>{item["state"]}</State>
            <StateTransitionReason>{item["state_transition_reason"]}</StateTransitionReason>
        </item>
"""

    unsuccessful_xml = ""
    for item in unsuccessful_items:
        errors_xml = ""
        for error_item in item.get("fast_snapshot_restore_state_errors", []):
            errors_xml += f"""                <item>
                    <AvailabilityZone>{error_item["availability_zone"]}</AvailabilityZone>
                    <Error>
                        <Code>{error_item["error"]["code"]}</Code>
                        <Message>{error_item["error"]["message"]}</Message>
                    </Error>
                </item>
"""
        unsuccessful_xml += f"""        <item>
            <SnapshotId>{item["snapshot_id"]}</SnapshotId>
            <FastSnapshotRestoreStateErrors>
{errors_xml}            </FastSnapshotRestoreStateErrors>
        </item>
"""

    xml = f"""<?xml version="1.0" encoding="UTF-8"?>
<EnableFastSnapshotRestoresResponse xmlns="http://ec2.amazonaws.com/doc/2016-11-15/">
    <requestId>{uuid.uuid4()}</requestId>
    <successful>
{successful_xml}    </successful>
    <unsuccessful>
{unsuccessful_xml}    </unsuccessful>
</EnableFastSnapshotRestoresResponse>"""
    return Response(content=xml, status_code=200, media_type="text/xml")


def _disable_fast_snapshot_restores(params: dict, region: str, account_id: str) -> Response:
    """DisableFastSnapshotRestores — disable FSR for snapshot/AZ pairs."""
    snapshot_ids = _get_param_list(params, "SourceSnapshotId")
    availability_zones = _get_param_list(params, "AvailabilityZone")

    if not snapshot_ids:
        return _ec2_error(
            "MissingParameter",
            "The request must contain the parameter SourceSnapshotId.",
        )
    if not availability_zones:
        return _ec2_error(
            "MissingParameter",
            "The request must contain the parameter AvailabilityZone.",
        )

    successful_items = []
    unsuccessful_items = []

    for snapshot_id in snapshot_ids:
        for az in availability_zones:
            existing = _get_fsr_state(account_id, region, snapshot_id, az)

            if not existing:
                unsuccessful_items.append(
                    {
                        "snapshot_id": snapshot_id,
                        "fast_snapshot_restore_state_errors": [
                            {
                                "availability_zone": az,
                                "error": {
                                    "code": "FastSnapshotRestoreStateError",
                                    "message": (
                                        f"Fast snapshot restore is not enabled for snapshot "
                                        f"{snapshot_id} in availability zone {az}."
                                    ),
                                },
                            }
                        ],
                    }
                )
                continue

            if existing["state"] in (FSR_STATE_DISABLING, FSR_STATE_DISABLED):
                unsuccessful_items.append(
                    {
                        "snapshot_id": snapshot_id,
                        "fast_snapshot_restore_state_errors": [
                            {
                                "availability_zone": az,
                                "error": {
                                    "code": "FastSnapshotRestoreStateError",
                                    "message": (
                                        f"Fast snapshot restore is already disabling or disabled "
                                        f"for snapshot {snapshot_id} in availability zone {az}."
                                    ),
                                },
                            }
                        ],
                    }
                )
                continue

            now = _utc_timestamp()
            existing["state"] = FSR_STATE_DISABLED
            existing["disabled_time"] = now
            existing["state_transition_reason"] = "Client initiated"
            _set_fsr_state(account_id, region, snapshot_id, az, existing)
            successful_items.append(
                {
                    "snapshot_id": snapshot_id,
                    "availability_zone": az,
                    "state": existing["state"],
                    "state_transition_reason": existing["state_transition_reason"],
                }
            )

    successful_xml = ""
    for item in successful_items:
        successful_xml += f"""        <item>
            <SnapshotId>{item["snapshot_id"]}</SnapshotId>
            <AvailabilityZone>{item["availability_zone"]}</AvailabilityZone>
            <State>{item["state"]}</State>
            <StateTransitionReason>{item["state_transition_reason"]}</StateTransitionReason>
        </item>
"""

    unsuccessful_xml = ""
    for item in unsuccessful_items:
        errors_xml = ""
        for error_item in item.get("fast_snapshot_restore_state_errors", []):
            errors_xml += f"""                <item>
                    <AvailabilityZone>{error_item["availability_zone"]}</AvailabilityZone>
                    <Error>
                        <Code>{error_item["error"]["code"]}</Code>
                        <Message>{error_item["error"]["message"]}</Message>
                    </Error>
                </item>
"""
        unsuccessful_xml += f"""        <item>
            <SnapshotId>{item["snapshot_id"]}</SnapshotId>
            <FastSnapshotRestoreStateErrors>
{errors_xml}            </FastSnapshotRestoreStateErrors>
        </item>
"""

    xml = f"""<?xml version="1.0" encoding="UTF-8"?>
<DisableFastSnapshotRestoresResponse xmlns="http://ec2.amazonaws.com/doc/2016-11-15/">
    <requestId>{uuid.uuid4()}</requestId>
    <successful>
{successful_xml}    </successful>
    <unsuccessful>
{unsuccessful_xml}    </unsuccessful>
</DisableFastSnapshotRestoresResponse>"""
    return Response(content=xml, status_code=200, media_type="text/xml")


def _describe_fast_snapshot_restores(params: dict, region: str, account_id: str) -> Response:
    """DescribeFastSnapshotRestores — describe FSR state for snapshot/AZ pairs."""
    snapshot_ids = _get_param_list(params, "SourceSnapshotId")
    availability_zones = _get_param_list(params, "AvailabilityZone")

    with _fsr_lock:
        store = _fsr_store.get(account_id, {}).get(region, {})
        items = []
        for (snap_id, az), fsr in store.items():
            if snapshot_ids and snap_id not in snapshot_ids:
                continue
            if availability_zones and az not in availability_zones:
                continue
            items.append(fsr)

    items_xml = ""
    for fsr in items:
        az_id_xml = (
            f"<availabilityZoneId>{fsr['availability_zone_id']}</availabilityZoneId>"
            if fsr.get("availability_zone_id")
            else ""
        )
        owner_alias_xml = (
            f"<ownerAlias>{fsr['owner_alias']}</ownerAlias>" if fsr.get("owner_alias") else ""
        )
        enabling_time_xml = (
            f"<enablingTime>{fsr['enabling_time']}</enablingTime>"
            if fsr.get("enabling_time")
            else ""
        )
        optimizing_time_xml = (
            f"<optimizingTime>{fsr['optimizing_time']}</optimizingTime>"
            if fsr.get("optimizing_time")
            else ""
        )
        enabled_time_xml = (
            f"<enabledTime>{fsr['enabled_time']}</enabledTime>" if fsr.get("enabled_time") else ""
        )
        disabling_time_xml = (
            f"<disablingTime>{fsr['disabling_time']}</disablingTime>"
            if fsr.get("disabling_time")
            else ""
        )
        disabled_time_xml = (
            f"<disabledTime>{fsr['disabled_time']}</disabledTime>"
            if fsr.get("disabled_time")
            else ""
        )
        items_xml += f"""        <item>
            <snapshotId>{fsr["snapshot_id"]}</snapshotId>
            <availabilityZone>{fsr["availability_zone"]}</availabilityZone>
            {az_id_xml}
            <state>{fsr["state"]}</state>
            <stateTransitionReason>{fsr["state_transition_reason"]}</stateTransitionReason>
            <ownerId>{fsr["owner_id"]}</ownerId>
            {owner_alias_xml}
            {enabling_time_xml}
            {optimizing_time_xml}
            {enabled_time_xml}
            {disabling_time_xml}
            {disabled_time_xml}
        </item>
"""

    xml = f"""<?xml version="1.0" encoding="UTF-8"?>
<DescribeFastSnapshotRestoresResponse xmlns="http://ec2.amazonaws.com/doc/2016-11-15/">
    <requestId>{uuid.uuid4()}</requestId>
    <fastSnapshotRestoreSet>
{items_xml}    </fastSnapshotRestoreSet>
</DescribeFastSnapshotRestoresResponse>"""
    return Response(content=xml, status_code=200, media_type="text/xml")


def _create_volume(params: dict, region: str, account_id: str) -> Response:
    """CreateVolume — intercept to handle VolumeInitializationRate and hydration state."""
    snapshot_id = _get_param(params, "SnapshotId")
    volume_init_rate = _get_param(params, "VolumeInitializationRate")
    availability_zone = _get_param(params, "AvailabilityZone")

    if volume_init_rate:
        if not snapshot_id:
            return _ec2_error(
                "InvalidParameterCombination",
                "VolumeInitializationRate can only be specified when creating "
                "a volume from a snapshot.",
            )
        try:
            rate = int(volume_init_rate)
            if rate < MIN_VOLUME_INIT_RATE or rate > MAX_VOLUME_INIT_RATE:
                return _ec2_error(
                    "InvalidParameterValue",
                    f"VolumeInitializationRate must be between "
                    f"{MIN_VOLUME_INIT_RATE} and {MAX_VOLUME_INIT_RATE}.",
                )
        except ValueError:
            return _ec2_error(
                "InvalidParameterValue",
                f"VolumeInitializationRate must be an integer between "
                f"{MIN_VOLUME_INIT_RATE} and {MAX_VOLUME_INIT_RATE}.",
            )

    backend = _get_moto_backend(account_id, region)
    size = _get_param(params, "Size")
    size_int = int(size) if size else None
    encrypted = _get_param(params, "Encrypted").lower() == "true"
    kms_key_id = _get_param(params, "KmsKeyId") or None
    volume_type = _get_param(params, "VolumeType") or None
    iops = _get_param(params, "Iops")
    iops_int = int(iops) if iops else None
    throughput = _get_param(params, "Throughput")
    throughput_int = int(throughput) if throughput else None
    multi_attach_param = _get_param(params, "MultiAttachEnabled")
    multi_attach = multi_attach_param.lower() == "true" if multi_attach_param else None

    try:
        volume = backend.create_volume(
            size=size_int,
            zone_name=availability_zone or f"{region}a",
            snapshot_id=snapshot_id or None,
            encrypted=encrypted,
            kms_key_id=kms_key_id,
            volume_type=volume_type,
            iops=iops_int,
            throughput=throughput_int,
            multi_attach_enabled=multi_attach,
        )
    except Exception as exc:  # noqa: BLE001
        return _ec2_error("InvalidParameterValue", str(exc))

    # Parse and apply TagSpecifications for volumes
    volume_tags = _parse_tag_specifications(params, "volume")
    if volume_tags:
        volume.add_tags(volume_tags)

    hydration_state = HYDRATION_COLD
    if snapshot_id:
        if _is_fsr_enabled(account_id, region, snapshot_id, availability_zone or f"{region}a"):
            hydration_state = HYDRATION_FSR_BACKED
        elif volume_init_rate:
            hydration_state = HYDRATION_INITIALIZED
    else:
        hydration_state = HYDRATION_INITIALIZED

    with _volume_hydration_lock:
        store = _volume_hydration.setdefault(account_id, {}).setdefault(region, {})
        store[volume.id] = {
            "volume_id": volume.id,
            "snapshot_id": snapshot_id,
            "hydration_state": hydration_state,
            "volume_initialization_rate": int(volume_init_rate) if volume_init_rate else None,
            "created_at": _utc_timestamp(),
        }

    snapshot_xml = (
        f"<snapshotId>{volume.snapshot_id}</snapshotId>" if volume.snapshot_id else "<snapshotId/>"
    )
    encrypted_xml = "true" if volume.encrypted else "false"
    kms_key_xml = f"<kmsKeyId>{volume.kms_key_id}</kmsKeyId>" if volume.kms_key_id else ""
    iops_xml = f"<iops>{volume.iops}</iops>" if volume.iops else ""
    throughput_xml = f"<throughput>{volume.throughput}</throughput>" if volume.throughput else ""
    multi_attach_xml = (
        f"<multiAttachEnabled>{str(volume.multi_attach_enabled).lower()}</multiAttachEnabled>"
        if volume.multi_attach_enabled is not None
        else ""
    )

    # Build tagSet XML
    tags = volume.get_tags()
    if tags:
        tag_items = ""
        for tag in tags:
            tag_key = tag.get("key") or tag.get("Key", "")
            tag_value = tag.get("value") or tag.get("Value", "")
            tag_items += f"""            <item>
                <key>{tag_key}</key>
                <value>{tag_value}</value>
            </item>
"""
        tag_set_xml = f"""    <tagSet>
{tag_items}    </tagSet>
"""
    else:
        tag_set_xml = ""

    xml = f"""<?xml version="1.0" encoding="UTF-8"?>
<CreateVolumeResponse xmlns="http://ec2.amazonaws.com/doc/2016-11-15/">
    <requestId>{uuid.uuid4()}</requestId>
    <volumeId>{volume.id}</volumeId>
    <size>{volume.size}</size>
    {snapshot_xml}
    <encrypted>{encrypted_xml}</encrypted>
    {kms_key_xml}
    <availabilityZone>{volume.zone.name if volume.zone else availability_zone}</availabilityZone>
    <status>creating</status>
    <createTime>{volume.create_time}</createTime>
{tag_set_xml}    <volumeType>{volume.volume_type}</volumeType>
    {iops_xml}
    {throughput_xml}
    {multi_attach_xml}
</CreateVolumeResponse>"""
    return Response(content=xml, status_code=200, media_type="text/xml")


def get_volume_hydration_state(volume_id: str, account_id: str, region: str) -> dict | None:
    """Get the hydration state for a volume (for testing/inspection)."""
    with _volume_hydration_lock:
        store = _volume_hydration.get(account_id, {}).get(region, {})
        return store.get(volume_id)


def get_fsr_state(snapshot_id: str, az: str, account_id: str, region: str) -> dict | None:
    """Get the FSR state for a snapshot/AZ pair (for testing/inspection)."""
    return _get_fsr_state(account_id, region, snapshot_id, az)


def _create_image(params: dict, region: str, account_id: str) -> Response:
    """CreateImage — support Packer-compatible AMI creation with identity clearing.

    When ROBOTOCORE_PACKER_TRANSPORT is enabled, this creates an AMI
    from a container-backed instance with proper identity clearing.
    """
    instance_id = _get_param(params, "InstanceId")
    ami_name = _get_param(params, "Name")
    description = _get_param(params, "Description")
    no_reboot = _get_param(params, "NoReboot") == "true"

    if not instance_id:
        return _ec2_error(
            "MissingParameter",
            "The request must contain the parameter InstanceId.",
        )

    if not ami_name:
        return _ec2_error(
            "MissingParameter",
            "The request must contain the parameter Name.",
        )

    # Check if packer transport is enabled and this instance has a transport
    if PACKER_TRANSPORT_ENABLED and _packer_transport_available:
        try:
            with _instance_transports_lock:
                transport = _instance_transports.get(instance_id)

            if transport and transport.is_running():
                # Use the AMI builder to create the AMI
                builder = get_ami_builder()
                result = builder.create_ami(
                    instance_id=instance_id,
                    ami_name=ami_name,
                    description=description,
                    no_reboot=no_reboot,
                    transport=transport,
                )

                # Return the response with the new AMI ID
                xml = f"""<?xml version="1.0" encoding="UTF-8"?>
<CreateImageResponse xmlns="http://ec2.amazonaws.com/doc/2016-11-15/">
    <requestId>{uuid.uuid4()}</requestId>
    <imageId>{result.ami_id}</imageId>
</CreateImageResponse>"""
                return Response(content=xml, status_code=200, media_type="text/xml")
        except Exception as e:  # noqa: BLE001
            logger.warning("Packer transport CreateImage failed, falling back to Moto: %s", e)

    # Fall back to Moto's CreateImage
    from moto.backends import get_backend  # noqa: I001

    backend = get_backend("ec2")[account_id][region]

    # Parse tag specifications from request
    tag_specifications: list[dict[str, Any]] = []
    i = 1
    while True:
        resource_type = _get_param(params, f"TagSpecification.{i}.ResourceType")
        if not resource_type:
            break
        tags = []
        j = 1
        while True:
            key = _get_param(params, f"TagSpecification.{i}.Tag.{j}.Key")
            if not key:
                break
            value = _get_param(params, f"TagSpecification.{i}.Tag.{j}.Value")
            tags.append({"Key": key, "Value": value})
            j += 1
        if tags:
            tag_specifications.append({"ResourceType": resource_type, "Tags": tags})
        i += 1

    # Create the image using Moto's backend
    ami = backend.create_image(
        instance_id=instance_id,
        name=ami_name,
        description=description,
        tag_specifications=tag_specifications,
    )

    xml = f"""<?xml version="1.0" encoding="UTF-8"?>
<CreateImageResponse xmlns="http://ec2.amazonaws.com/doc/2016-11-15/">
    <requestId>{uuid.uuid4()}</requestId>
    <imageId>{ami.id}</imageId>
</CreateImageResponse>"""
    return Response(content=xml, status_code=200, media_type="text/xml")


async def _run_instances(request: Request, params: dict, region: str, account_id: str) -> Response:
    """RunInstances - capacity check, forward to Moto, then guest container."""
    from moto.backends import get_backend  # noqa: I001

    instance_type = _get_param(params, "InstanceType") or "m1.small"
    min_count = _get_int_param(params, "MinCount", 1)
    max_count = _get_int_param(params, "MaxCount", min_count)
    placement_az = _get_param(params, "Placement.AvailabilityZone")
    subnet_id = _get_param(params, "SubnetId")

    # Determine AZ
    az = placement_az
    if not az and subnet_id:
        try:
            backend = get_backend("ec2")[account_id][region]
            subnet = backend.get_subnet(subnet_id)
            az = subnet.availability_zone
        except Exception as exc:  # noqa: BLE001
            logger.debug("Could not get AZ from subnet: %s", exc)

    if not az:
        az = f"{region}a"

    # Check for market options (spot)
    market_type = _get_param(params, "InstanceMarketOptions.MarketType")
    is_spot = market_type == "spot"

    # Check capacity only if an explicit profile exists
    store = get_capacity_store()
    profile = store.get_profile(account_id, region, instance_type, az)
    capacity_consumed = 0

    # Always check chaos override (even without explicit profile)
    chaos_override = store.get_chaos_override()
    if chaos_override:
        error_code = chaos_override.get("error_code")
        if error_code == "InsufficientInstanceCapacity":
            return _ec2_error(
                "InsufficientInstanceCapacity",
                f"We currently do not have sufficient {instance_type} capacity "
                f"in the Availability Zone you requested ({az}). "
                f"Our system will be working on provisioning additional capacity. "
                f"You can currently get {instance_type} capacity by not specifying "
                f"an Availability Zone in your request or choosing "
                f"{region}b, {region}c.",
                status_code=500,
            )
        elif error_code == "Unsupported":
            return _ec2_error(
                "Unsupported",
                "The requested configuration is currently not supported. "
                "Please check the documentation for supported configurations.",
                status_code=400,
            )

    if profile is not None:
        # An explicit capacity profile exists - enforce it
        if not profile.enabled:
            return _ec2_error(
                "Unsupported",
                "The requested configuration is currently not supported. "
                "Please check the documentation for supported configurations.",
                status_code=400,
            )

        if is_spot:
            spot_available, _ = store.check_spot_capacity(account_id, region, instance_type, az)
            if not spot_available:
                return _ec2_error(
                    "InsufficientInstanceCapacity",
                    f"We currently do not have sufficient {instance_type} capacity "
                    f"in the Availability Zone you requested ({az}). "
                    f"Our system will be working on provisioning additional capacity. "
                    f"You can currently get {instance_type} capacity by not specifying "
                    f"an Availability Zone in your request or choosing "
                    f"{region}b, {region}c.",
                    status_code=500,
                )

        # Check on-demand capacity
        success, error_code = store.check_capacity(account_id, region, instance_type, az, max_count)
        if not success:
            if error_code == "InsufficientInstanceCapacity":
                return _ec2_error(
                    "InsufficientInstanceCapacity",
                    f"We currently do not have sufficient {instance_type} capacity "
                    f"in the Availability Zone you requested ({az}). "
                    f"Our system will be working on provisioning additional capacity. "
                    f"You can currently get {instance_type} capacity by not specifying "
                    f"an Availability Zone in your request or choosing "
                    f"{region}b, {region}c.",
                    status_code=500,
                )
            elif error_code == "Unsupported":
                return _ec2_error(
                    "Unsupported",
                    "The requested configuration is currently not supported. "
                    "Please check the documentation for supported configurations.",
                    status_code=400,
                )

        # Consume capacity
        if not store.consume_capacity(account_id, region, instance_type, az, max_count):
            return _ec2_error(
                "InsufficientInstanceCapacity",
                f"We currently do not have sufficient {instance_type} capacity "
                f"in the Availability Zone you requested ({az}). "
                f"Our system will be working on provisioning additional capacity. "
                f"You can currently get {instance_type} capacity by not specifying "
                f"an Availability Zone in your request or choosing "
                f"{region}b, {region}c.",
                status_code=500,
            )
        capacity_consumed = max_count

    # Forward to Moto for actual instance creation
    try:
        response = await forward_to_moto(request, "ec2", account_id=account_id)
    except Exception as e:  # noqa: BLE001
        logger.exception("Error in RunInstances")
        # Release capacity on failure
        if capacity_consumed > 0:
            store.release_capacity(account_id, region, instance_type, az, capacity_consumed)
        return _ec2_error(
            "InternalError",
            f"An internal error has occurred: {e}",
            status_code=500,
        )

    # If guest execution is disabled, just return the response
    if not is_guest_executor_enabled():
        return response

    # If the response is not successful, release capacity and return
    if response.status_code != 200:
        if capacity_consumed > 0:
            store.release_capacity(account_id, region, instance_type, az, capacity_consumed)
        return response

    # Parse the response to get instance IDs and launch guest containers
    try:
        import xml.etree.ElementTree as ET

        body = response.body if hasattr(response, "body") else b""
        if not body:
            return response

        root = ET.fromstring(body)

        # Find instance IDs by iterating through the XML tree
        instances = []
        for child in root:
            if "instancesSet" in child.tag:
                for item in child:
                    if "item" in item.tag:
                        for subchild in item:
                            if "instanceId" in subchild.tag:
                                instances.append(subchild)
                                break

        if not instances:
            return response

        # Extract parameters from the request
        user_data = _get_param(params, "UserData")
        instance_type_param = _get_param(params, "InstanceType") or "t2.micro"

        # Parse block device mappings
        block_device_mappings = []
        i = 1
        while True:
            device_name = _get_param(params, f"BlockDeviceMapping.{i}.DeviceName")
            if not device_name:
                break
            volume_size = _get_param(params, f"BlockDeviceMapping.{i}.Ebs.VolumeSize") or "8"
            block_device_mappings.append(
                {
                    "DeviceName": device_name,
                    "Ebs": {"VolumeSize": volume_size},
                }
            )
            i += 1

        # Parse IAM instance profile
        iam_instance_profile = None
        iam_profile_arn = _get_param(params, "IamInstanceProfile.Arn")
        iam_profile_name = _get_param(params, "IamInstanceProfile.Name")
        if iam_profile_arn or iam_profile_name:
            iam_instance_profile = {
                "Arn": iam_profile_arn,
                "Name": iam_profile_name,
            }

        # Decode base64 user-data if present
        if user_data:
            try:
                user_data = base64.b64decode(user_data).decode("utf-8")
            except Exception:
                pass  # Keep as-is if not valid base64

        # Launch guest containers for each instance
        executor = get_guest_executor()
        for inst_elem in instances:
            instance_id = inst_elem.text if hasattr(inst_elem, "text") else inst_elem
            if instance_id:
                logger.debug("Launching guest container for instance %s", instance_id)

                # Run in background thread to not block response
                def launch():
                    executor.launch_instance(
                        instance_id=instance_id,
                        account_id=account_id,
                        region=region,
                        user_data=user_data,
                        instance_type=instance_type_param,
                        block_device_mappings=block_device_mappings,
                        iam_instance_profile=iam_instance_profile,
                    )

                threading.Thread(target=launch, daemon=True).start()

    except Exception as e:
        logger.warning("Failed to launch guest container: %s", e)

    return response


async def _terminate_instances(
    request: Request, params: dict, region: str, account_id: str
) -> Response:
    """TerminateInstances - forward to Moto, release capacity, clean up guest containers."""
    from moto.backends import get_backend  # noqa: I001

    # Parse the request to get instance IDs before termination
    instance_ids = []
    i = 1
    while True:
        inst_id = _get_param(params, f"InstanceId.{i}")
        if not inst_id:
            break
        instance_ids.append(inst_id)
        i += 1

    # Look up instance details before termination (to get type/AZ for capacity release)
    backend = get_backend("ec2")[account_id][region]
    instances_to_release = []
    for inst_id in instance_ids:
        try:
            instance = backend.get_instance(inst_id)
            # Only release capacity if instance was not already terminated
            if instance.state != "terminated":
                instances_to_release.append(
                    {
                        "id": inst_id,
                        "type": instance.instance_type,
                        "az": instance._placement.zone,
                    }
                )
        except Exception as exc:  # noqa: BLE001
            logger.debug("Could not get instance %s for capacity release: %s", inst_id, exc)

    # Forward to Moto to terminate the instances
    response = await forward_to_moto(request, "ec2", account_id=account_id)

    # Release capacity for terminated instances
    store = get_capacity_store()
    for inst in instances_to_release:
        try:
            # Only release if an explicit profile exists
            profile = store.get_profile(account_id, region, inst["type"], inst["az"])
            if profile is not None:
                store.release_capacity(account_id, region, inst["type"], inst["az"], 1)
                logger.debug(
                    "Released capacity for terminated instance %s (%s/%s)",
                    inst["id"],
                    inst["type"],
                    inst["az"],
                )
        except Exception as exc:  # noqa: BLE001
            logger.debug("Failed to release capacity for instance %s: %s", inst["id"], exc)

    # If guest execution is disabled, just return the response
    if not is_guest_executor_enabled():
        return response

    # Clean up guest containers
    executor = get_guest_executor()
    for inst_id in instance_ids:
        logger.info("Terminating guest container for instance %s", inst_id)
        executor.terminate_instance(inst_id)

    return response


async def _request_spot_instances(
    request: Request, params: dict, region: str, account_id: str
) -> Response:
    """RequestSpotInstances - capacity check gate, then forward to Moto."""
    from moto.backends import get_backend  # noqa: I001

    instance_type = _get_param(params, "LaunchSpecification.InstanceType") or "m1.small"
    count = _get_int_param(params, "InstanceCount", 1)
    placement_az = _get_param(params, "LaunchSpecification.Placement.AvailabilityZone")
    subnet_id = _get_param(params, "LaunchSpecification.SubnetId")

    # Determine AZ
    az = placement_az
    if not az and subnet_id:
        try:
            backend = get_backend("ec2")[account_id][region]
            subnet = backend.get_subnet(subnet_id)
            az = subnet.availability_zone
        except Exception as exc:  # noqa: BLE001
            logger.debug("Could not get AZ from subnet: %s", exc)

    if not az:
        az = f"{region}a"

    store = get_capacity_store()
    profile = store.get_profile(account_id, region, instance_type, az)
    capacity_consumed = 0

    # Only enforce capacity if an explicit profile exists
    if profile is not None:
        if not profile.enabled:
            return _build_spot_capacity_unavailable_response(params, count)

        # Check chaos override
        chaos_override = store.get_chaos_override()
        if chaos_override:
            error_code = chaos_override.get("error_code")
            if error_code == "InsufficientInstanceCapacity":
                return _build_spot_capacity_unavailable_response(params, count)

        # Check spot capacity
        spot_available, _ = store.check_spot_capacity(account_id, region, instance_type, az)
        if not spot_available:
            return _build_spot_capacity_unavailable_response(params, count)

        # Consume capacity
        if not store.consume_capacity(account_id, region, instance_type, az, count):
            return _build_spot_capacity_unavailable_response(params, count)
        capacity_consumed = count

    # Forward to Moto for actual spot request creation
    try:
        return await forward_to_moto(request, "ec2", account_id=account_id)
    except Exception as e:  # noqa: BLE001
        logger.exception("Error in RequestSpotInstances")
        # Release capacity on failure
        if capacity_consumed > 0:
            store.release_capacity(account_id, region, instance_type, az, capacity_consumed)
        return _ec2_error(
            "InternalError",
            f"An internal error has occurred: {e}",
            status_code=500,
        )


def _build_spot_capacity_unavailable_response(params: dict, count: int) -> Response:
    """Build a spot capacity unavailable response."""
    spot_price = _get_param(params, "SpotPrice")
    image_id = _get_param(params, "LaunchSpecification.ImageId")
    instance_type = _get_param(params, "LaunchSpecification.InstanceType") or "m1.small"

    requests_xml = ""
    for _ in range(count):
        request_id = f"sir-{uuid.uuid4().hex[:17]}"
        requests_xml += f"""        <item>
            <spotInstanceRequestId>{request_id}</spotInstanceRequestId>
            <spotPrice>{spot_price or "0.05"}</spotPrice>
            <type>one-time</type>
            <state>open</state>
            <status>
                <code>capacity-not-available</code>
                <updateTime>2024-01-01T00:00:00.000Z</updateTime>
                <message>There is no Spot capacity available that matches your request.</message>
            </status>
            <instanceId/>
            <availabilityZoneGroup/>
            <launchSpecification>
                <imageId>{image_id or "ami-12345678"}</imageId>
                <instanceType>{instance_type}</instanceType>
            </launchSpecification>
            <launchGroup/>
            <createTime>2024-01-01T00:00:00.000Z</createTime>
            <productDescription>Linux/UNIX</productDescription>
        </item>
"""

    xml = f"""<?xml version="1.0" encoding="UTF-8"?>
<RequestSpotInstancesResponse xmlns="http://ec2.amazonaws.com/doc/2016-11-15/">
    <requestId>{uuid.uuid4()}</requestId>
    <spotInstanceRequestSet>
{requests_xml}    </spotInstanceRequestSet>
</RequestSpotInstancesResponse>"""
    return Response(content=xml, status_code=200, media_type="text/xml")


_ACTION_MAP = {
    "CreatePlacementGroup": _create_placement_group,
    "DescribePlacementGroups": _describe_placement_groups,
    "DeletePlacementGroup": _delete_placement_group,
    "DetachVolume": _detach_volume,
    "DeleteVpcEndpoints": _delete_vpc_endpoints,
    "EnableFastSnapshotRestores": _enable_fast_snapshot_restores,
    "DisableFastSnapshotRestores": _disable_fast_snapshot_restores,
    "DescribeFastSnapshotRestores": _describe_fast_snapshot_restores,
    "CreateVolume": _create_volume,
    "CreateImage": _create_image,
}
