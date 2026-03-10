"""Native SSM provider.

Intercepts operations where Moto has bugs:
- SendCommand with Targets using 'instanceids' filter key (Moto doesn't support)
"""

import json
import uuid
from datetime import UTC, datetime

from starlette.requests import Request
from starlette.responses import Response

from robotocore.providers.moto_bridge import forward_to_moto

# Store commands we create natively
# (account_id, region) -> {command_id -> command}
_commands: dict[str, dict[str, dict]] = {}


async def handle_ssm_request(request: Request, region: str, account_id: str) -> Response:
    """Handle SSM requests, intercepting buggy operations."""
    body = await request.body()
    target = request.headers.get("x-amz-target", "")
    action = target.split(".")[-1] if "." in target else ""

    if action == "SendCommand":
        params = json.loads(body) if body else {}
        targets = params.get("Targets", [])
        # Check if any target uses the problematic 'instanceids' key
        has_instanceids = any(t.get("Key", "").lower() == "instanceids" for t in targets)
        if has_instanceids:
            return _send_command_native(params, region, account_id)

    if action == "ListCommands":
        params = json.loads(body) if body else {}
        command_id = params.get("CommandId")
        store_key = f"{account_id}:{region}"
        if command_id and command_id in _commands.get(store_key, {}):
            cmd = _commands[store_key][command_id]
            return Response(
                content=json.dumps({"Commands": [cmd]}),
                status_code=200,
                media_type="application/x-amz-json-1.1",
            )

    if action == "ListCommandInvocations":
        params = json.loads(body) if body else {}
        command_id = params.get("CommandId")
        store_key = f"{account_id}:{region}"
        if command_id and command_id in _commands.get(store_key, {}):
            return Response(
                content=json.dumps({"CommandInvocations": []}),
                status_code=200,
                media_type="application/x-amz-json-1.1",
            )

    return await forward_to_moto(request, "ssm", account_id=account_id)


def _send_command_native(params: dict, region: str, account_id: str) -> Response:
    command_id = str(uuid.uuid4())
    now = datetime.now(UTC).isoformat()

    # Extract instance IDs from targets
    instance_ids = []
    for target in params.get("Targets", []):
        if target.get("Key", "").lower() == "instanceids":
            instance_ids.extend(target.get("Values", []))

    command = {
        "CommandId": command_id,
        "DocumentName": params.get("DocumentName", ""),
        "Comment": params.get("Comment", ""),
        "ExpiresAfter": now,
        "Parameters": params.get("Parameters", {}),
        "InstanceIds": instance_ids,
        "Targets": params.get("Targets", []),
        "RequestedDateTime": now,
        "Status": "Success",
        "StatusDetails": "Details placeholder",
        "OutputS3BucketName": "",
        "OutputS3KeyPrefix": "",
        "MaxConcurrency": "50",
        "MaxErrors": "0",
        "TargetCount": len(instance_ids),
        "CompletedCount": len(instance_ids),
        "ErrorCount": 0,
        "DeliveryTimedOutCount": 0,
    }

    store_key = f"{account_id}:{region}"
    _commands.setdefault(store_key, {})[command_id] = command

    return Response(
        content=json.dumps({"Command": command}),
        status_code=200,
        media_type="application/x-amz-json-1.1",
    )
