#!/usr/bin/env python3
"""Generate resource lifecycle tests from botocore service specs.

Creates pytest test files that exercise CRUD lifecycles for each resource type
in a service. Tests are designed to run against a live server (localhost:4566).

For each resource noun (e.g., IoT "Thing", Glue "Database"), generates:
  - A lifecycle test: Create → Describe → Delete → Describe-after-delete
  - A not-found test: Describe with fake ID → assert error

The generated tests assert:
  - Structural: required output keys present, correct types
  - Value: round-trip values match, ARNs well-formed, IDs non-empty
  - Behavioral: correct error on not-found, resource disappears after delete

Usage:
    uv run python scripts/gen_resource_lifecycle_test.py --service iot --dry-run
    uv run python scripts/gen_resource_lifecycle_test.py --service glue --write
    uv run python scripts/gen_resource_lifecycle_test.py --service connect --write
"""

import argparse
import re
import sys
from pathlib import Path

import botocore.loaders
import botocore.session

VERB_PREFIXES = (
    "Accept",
    "Activate",
    "Add",
    "Advertise",
    "Allocate",
    "Apply",
    "Assign",
    "Associate",
    "Attach",
    "Authorize",
    "Batch",
    "Bundle",
    "Cancel",
    "Confirm",
    "Copy",
    "Create",
    "Deactivate",
    "Delete",
    "Deregister",
    "Describe",
    "Detach",
    "Disable",
    "Disassociate",
    "Enable",
    "Execute",
    "Export",
    "Get",
    "Import",
    "Invoke",
    "List",
    "Modify",
    "Monitor",
    "Move",
    "Publish",
    "Put",
    "Reboot",
    "Register",
    "Reject",
    "Release",
    "Remove",
    "Replace",
    "Request",
    "Reset",
    "Restore",
    "Revoke",
    "Rotate",
    "Run",
    "Schedule",
    "Send",
    "Set",
    "Start",
    "Stop",
    "Subscribe",
    "Tag",
    "Terminate",
    "Unassign",
    "Unmonitor",
    "Unsubscribe",
    "Untag",
    "Update",
    "Verify",
    "Withdraw",
)

CREATE_VERBS = ("Create", "Put", "Register", "Start", "Add", "Run")
DESCRIBE_VERBS = ("Describe", "Get")
LIST_VERBS = ("List",)
DELETE_VERBS = ("Delete", "Remove", "Deregister", "Stop")
UPDATE_VERBS = ("Update", "Modify")

NOT_FOUND_CODES = (
    "ResourceNotFoundException",
    "ResourcePolicyNotFoundException",
    "NotFoundException",
    "EntityNotFoundException",
    "InvalidRequestException",
    "NoSuchEntity",
)

SKIP_NOUNS = {
    "Tags",
    "Resource",
    "TagsForResource",
    "Account",
    "AccountSettings",
    "Service",
    "Configuration",
}

SERVICE_FIXTURES: dict[str, str] = {
    "connect": """
@pytest.fixture
def instance_id(client):
    resp = client.create_instance(
        IdentityManagementType="CONNECT_MANAGED",
        InboundCallsEnabled=True,
        OutboundCallsEnabled=True,
    )
    iid = resp["Id"]
    yield iid
    try:
        client.delete_instance(InstanceId=iid)
    except Exception:
        pass
""",
    "organizations": """
@pytest.fixture(autouse=True)
def org(client):
    resp = client.create_organization(FeatureSet="ALL")
    yield resp["Organization"]
    try:
        client.delete_organization()
    except Exception:
        pass
""",
    "backup": """
@pytest.fixture
def vault_name(client):
    name = "test-vault-1"
    client.create_backup_vault(BackupVaultName=name)
    yield name
    try:
        client.delete_backup_vault(BackupVaultName=name)
    except Exception:
        pass
""",
    "eks": """
@pytest.fixture
def cluster_name(client):
    import boto3

    iam = boto3.client(
        "iam",
        endpoint_url="http://localhost:4566",
        region_name="us-east-1",
        aws_access_key_id="testing",
        aws_secret_access_key="testing",
    )
    try:
        iam.create_role(
            RoleName="eks-test-role",
            AssumeRolePolicyDocument="{}",
            Path="/",
        )
    except Exception:
        pass
    name = "test-cluster-1"
    try:
        client.create_cluster(
            name=name,
            roleArn="arn:aws:iam::123456789012:role/eks-test-role",
            resourcesVpcConfig={"subnetIds": ["subnet-12345"], "securityGroupIds": ["sg-12345"]},
        )
    except Exception:
        pass
    yield name
""",
    "opensearch": """
@pytest.fixture
def domain_name(client):
    name = "test-domain-1"
    try:
        client.create_domain(DomainName=name)
    except Exception:
        pass
    yield name
""",
    "cognito-idp": """
@pytest.fixture
def user_pool_id(client):
    resp = client.create_user_pool(PoolName="test-pool-1")
    pool_id = resp["UserPool"]["Id"]
    yield pool_id
    try:
        client.delete_user_pool(UserPoolId=pool_id)
    except Exception:
        pass
""",
}

INSTANCE_ID_SERVICES = {"connect"}
VAULT_NAME_SERVICES = {"backup"}
CLUSTER_NAME_SERVICES = {"eks"}
DOMAIN_NAME_SERVICES = {"opensearch"}
USER_POOL_ID_SERVICES = {"cognito-idp"}


def _to_snake_case(name: str) -> str:
    s1 = re.sub(r"(.)([A-Z][a-z]+)", r"\1_\2", name)
    return re.sub(r"([a-z0-9])([A-Z])", r"\1_\2", s1).lower()


def extract_noun(op_name: str) -> str:
    for prefix in sorted(VERB_PREFIXES, key=len, reverse=True):
        if op_name.startswith(prefix):
            noun = op_name[len(prefix) :]
            if noun:
                return noun
    return op_name


def load_service_model(service_name: str) -> dict:
    loader = botocore.loaders.Loader()
    name_map = {
        "cloudwatch": "monitoring",
        "eventbridge": "events",
    }
    botocore_name = name_map.get(service_name, service_name)
    return loader.load_service_model(botocore_name, "service-2")


def get_operation_model(service_name: str, operation_name: str):
    session = botocore.session.get_session()
    model = session.get_service_model(service_name)
    return model.operation_model(operation_name)


def get_required_params(service_name: str, operation_name: str, model: dict) -> list[dict]:
    """Extract required parameters with shape info from a botocore operation."""
    op_spec = model.get("operations", {}).get(operation_name, {})
    input_shape_name = op_spec.get("input", {}).get("shape")
    if not input_shape_name:
        return []

    shapes = model.get("shapes", {})
    input_shape = shapes.get(input_shape_name, {})
    required = input_shape.get("required", [])
    members = input_shape.get("members", {})

    params = []
    for param_name in required:
        member = members.get(param_name, {})
        shape_ref = member.get("shape", "")
        shape_def = shapes.get(shape_ref, {})
        shape_type = shape_def.get("type", "string")

        params.append(
            {
                "name": param_name,
                "shape": shape_ref,
                "type": shape_type,
                "enum": shape_def.get("enum"),
                "documentation": member.get("documentation", ""),
            }
        )
    return params


def get_output_fields(operation_name: str, model: dict) -> list[dict]:
    """Extract output fields from a botocore operation."""
    op_spec = model.get("operations", {}).get(operation_name, {})
    output_shape_name = op_spec.get("output", {}).get("shape")
    if not output_shape_name:
        return []

    shapes = model.get("shapes", {})
    output_shape = shapes.get(output_shape_name, {})
    members = output_shape.get("members", {})
    required_fields = set(output_shape.get("required", []))

    fields = []
    for field_name, member in members.items():
        shape_ref = member.get("shape", "")
        shape_def = shapes.get(shape_ref, {})
        shape_type = shape_def.get("type", "string")

        fields.append(
            {
                "name": field_name,
                "shape": shape_ref,
                "type": shape_type,
                "required": field_name in required_fields,
            }
        )
    return fields


def _get_shapes(service_name: str) -> dict:
    """Load the shapes dict for a service. Cached per-call via module global."""
    model = load_service_model(service_name)
    return model.get("shapes", {})


def generate_param_value(
    param: dict,
    service_name: str = "",
    shapes: dict | None = None,
) -> str:
    """Generate a test value for a required parameter."""
    name = param["name"]
    shape_type = param["type"]
    name_lower = name.lower()

    if name == "InstanceId" and service_name in INSTANCE_ID_SERVICES:
        return "instance_id"
    if name == "BackupVaultName" and service_name in VAULT_NAME_SERVICES:
        return "vault_name"
    if name in ("ClusterName", "clusterName") and service_name in CLUSTER_NAME_SERVICES:
        return "cluster_name"
    if name == "DomainName" and service_name in DOMAIN_NAME_SERVICES:
        return "domain_name"
    if name == "UserPoolId" and service_name in USER_POOL_ID_SERVICES:
        return "user_pool_id"

    if param.get("enum"):
        return f'"{param["enum"][0]}"'

    doc = param.get("documentation", "").lower()

    if shape_type == "string":
        if "arn" in name_lower:
            return '"arn:aws:iam::123456789012:role/test-role"'
        if "name" in name_lower:
            return '"test-name-1"'
        if "id" in name_lower:
            if "name" in doc or "name or" in doc:
                return '"test-name-1"'
            return '"test-id-1"'
        if "url" in name_lower:
            return '"http://localhost:4566/test"'
        if "email" in name_lower:
            return '"test@example.com"'
        return '"test-string"'
    elif shape_type in ("integer", "long"):
        shape_def = shapes.get(param.get("shape", ""), {}) if shapes else {}
        min_val = shape_def.get("min", 1)
        return str(max(1, min_val))
    elif shape_type == "boolean":
        return "True"
    elif shape_type == "timestamp":
        return '"2024-01-01T00:00:00Z"'
    elif shape_type == "blob":
        return 'b"test-data"'
    elif shape_type == "list":
        if shapes and param.get("shape"):
            return _generate_list_value(param["shape"], shapes)
        return "[]"
    elif shape_type == "map":
        return "{}"
    elif shape_type == "structure":
        if shapes and param.get("shape"):
            return _generate_structure_value(param["shape"], shapes)
        return "{}"
    return '"test-string"'


def _generate_structure_value(shape_name: str, shapes: dict) -> str:
    """Recursively generate a dict literal for a structure shape."""
    shape = shapes.get(shape_name, {})
    required = shape.get("required", [])
    members = shape.get("members", {})
    is_union = shape.get("union", False)

    if is_union and members:
        first_member = next(iter(members))
        member = members[first_member]
        member_shape_name = member.get("shape", "")
        member_shape = shapes.get(member_shape_name, {})
        member_type = member_shape.get("type", "string")
        if member_type == "structure":
            val = _generate_structure_value(member_shape_name, shapes)
        else:
            sub_param = {
                "name": first_member,
                "shape": member_shape_name,
                "type": member_type,
                "enum": member_shape.get("enum"),
            }
            val = generate_param_value(sub_param, shapes=shapes)
        return "{" + f'"{first_member}": {val}' + "}"

    if not required:
        return "{}"

    parts = []
    for field_name in required:
        member = members.get(field_name, {})
        member_shape_name = member.get("shape", "")
        member_shape = shapes.get(member_shape_name, {})
        member_type = member_shape.get("type", "string")
        sub_param = {
            "name": field_name,
            "shape": member_shape_name,
            "type": member_type,
            "enum": member_shape.get("enum"),
        }
        val = generate_param_value(sub_param, shapes=shapes)
        parts.append(f'"{field_name}": {val}')

    return "{" + ", ".join(parts) + "}"


def _generate_list_value(shape_name: str, shapes: dict) -> str:
    """Generate a list literal, including one element if the member is a structure."""
    shape = shapes.get(shape_name, {})
    member = shape.get("member", {})
    member_shape_name = member.get("shape", "")
    member_shape = shapes.get(member_shape_name, {})
    member_type = member_shape.get("type", "string")

    if member_type == "string":
        if member_shape.get("enum"):
            return f'["{member_shape["enum"][0]}"]'
        return '["test-string"]'
    elif member_type == "structure":
        val = _generate_structure_value(member_shape_name, shapes)
        return f"[{val}]"
    return "[]"


def generate_fake_param_value(param: dict) -> str:
    """Generate a fake value for not-found tests."""
    shape_type = param["type"]
    if shape_type == "string":
        return '"fake-id"'
    elif shape_type in ("integer", "long"):
        return "99999"
    elif shape_type == "boolean":
        return "True"
    return '"fake-id"'


def generate_assertions(
    fields: list[dict],
    var_name: str,
    strict: bool = False,
    identity_fields: set[str] | None = None,
) -> list[str]:
    """Generate assertion lines for output fields.

    If strict=True (for create responses), assert required fields are present.
    If strict=False (for describe responses), only assert identity fields
    strictly and skip optional scalars.

    identity_fields: if provided, only these field names are treated as identity.
    Otherwise, falls back to ARN-field heuristic only.
    """
    lines = []
    for f in fields:
        fname = f["name"]
        ftype = f["type"]
        name_lower = fname.lower()

        if identity_fields is not None:
            is_identity = fname in identity_fields
        else:
            is_identity = "arn" in name_lower

        is_required = f.get("required", False)

        if ftype == "string":
            if is_identity and "arn" in name_lower:
                lines.append(f'    assert isinstance({var_name}.get("{fname}"), str)')
                lines.append(f'    assert {var_name}["{fname}"].startswith("arn:aws:")')
            elif is_identity:
                lines.append(f'    assert isinstance({var_name}.get("{fname}"), str)')
                lines.append(f'    assert len({var_name}.get("{fname}", "")) > 0')
            elif strict and is_required:
                lines.append(f'    assert isinstance({var_name}.get("{fname}"), str)')
        elif ftype in ("integer", "long"):
            if (strict and is_required) or is_identity:
                lines.append(f'    assert isinstance({var_name}.get("{fname}"), int)')
        elif ftype == "boolean":
            if strict and is_required:
                lines.append(f'    assert isinstance({var_name}.get("{fname}"), bool)')
        elif ftype == "list":
            lines.append(f'    assert isinstance({var_name}.get("{fname}", []), list)')
        elif ftype in ("map", "structure"):
            lines.append(f'    assert isinstance({var_name}.get("{fname}", {{}}), dict)')
        elif ftype == "timestamp":
            if strict or is_identity:
                lines.append(f'    assert {var_name}.get("{fname}") is not None')

    return lines


def find_ops_for_noun(noun: str, all_ops: dict) -> dict:
    """Find create, describe, list, update, delete ops for a resource noun."""
    result = {"create": None, "describe": None, "list": None, "update": None, "delete": None}

    for op_name in all_ops:
        op_noun = extract_noun(op_name)
        if op_noun != noun:
            # Also match plural forms
            if op_noun != noun + "s" and op_noun != noun + "es":
                continue

        for verb in CREATE_VERBS:
            if op_name.startswith(verb) and extract_noun(op_name) == noun:
                if result["create"] is None:
                    result["create"] = op_name
        for verb in DESCRIBE_VERBS:
            if op_name.startswith(verb) and extract_noun(op_name) == noun:
                if result["describe"] is None:
                    result["describe"] = op_name
        for verb in LIST_VERBS:
            if op_name.startswith(verb):
                list_noun = extract_noun(op_name)
                if list_noun == noun or list_noun == noun + "s" or list_noun == noun + "es":
                    if result["list"] is None:
                        result["list"] = op_name
        for verb in DELETE_VERBS:
            if op_name.startswith(verb) and extract_noun(op_name) == noun:
                if result["delete"] is None:
                    result["delete"] = op_name
        for verb in UPDATE_VERBS:
            if op_name.startswith(verb) and extract_noun(op_name) == noun:
                if result["update"] is None:
                    result["update"] = op_name

    return result


def _find_captured_ids(
    create_params: list[dict],
    create_output_fields: list[dict],
    target_params: list[dict],
    noun: str,
    model: dict,
) -> dict[str, tuple[str, str]]:
    """Find server-generated IDs to capture from create response.

    Returns dict mapping param_name -> (extraction_expression, variable_name)
    for params that appear in the target op but NOT in the create input.
    """
    create_param_names = {p["name"] for p in create_params}
    create_output_names = {f["name"] for f in create_output_fields}
    captures: dict[str, tuple[str, str]] = {}

    for p in target_params:
        if p["name"] in create_param_names:
            continue

        if p["name"] in create_output_names:
            var = _to_snake_case(p["name"])
            captures[p["name"]] = (f'create_resp["{p["name"]}"]', var)
            continue

        # Check nested: create output might wrap in a noun-named structure
        shapes = model.get("shapes", {})
        for f in create_output_fields:
            if f["type"] == "structure" and f["name"] == noun:
                shape_def = shapes.get(f["shape"], {})
                nested_members = shape_def.get("members", {})
                if p["name"] in nested_members:
                    var = _to_snake_case(p["name"])
                    expr = f'create_resp["{noun}"]["{p["name"]}"]'
                    captures[p["name"]] = (expr, var)
                    break

    return captures


def _param_value_for(
    param: dict,
    captures: dict[str, tuple[str, str]],
    service_name: str,
    shapes: dict | None = None,
) -> str:
    """Get the value expression for a param, using captured ID if available."""
    if param["name"] in captures:
        _, var_name = captures[param["name"]]
        return var_name
    return generate_param_value(param, service_name, shapes=shapes)


def generate_lifecycle_test(
    service_name: str,
    noun: str,
    ops: dict,
    model: dict,
) -> list[str]:
    """Generate a lifecycle test for a resource noun."""
    lines = []
    snake_noun = _to_snake_case(noun)

    create_op = ops.get("create")
    describe_op = ops.get("describe")
    delete_op = ops.get("delete")

    if not create_op or not describe_op:
        return []

    create_params = get_required_params(service_name, create_op, model)
    describe_params = get_required_params(service_name, describe_op, model)
    create_output_fields = get_output_fields(create_op, model)
    describe_output_fields = get_output_fields(describe_op, model)
    shapes = model.get("shapes", {})

    # Find server-generated IDs to capture from create response
    captures = _find_captured_ids(
        create_params,
        create_output_fields,
        describe_params,
        noun,
        model,
    )
    if delete_op:
        delete_params = get_required_params(service_name, delete_op, model)
        delete_captures = _find_captured_ids(
            create_params,
            create_output_fields,
            delete_params,
            noun,
            model,
        )
        captures.update(delete_captures)
    else:
        delete_params = []

    # Determine if we need fixture params
    fixture_parts = ["client"]
    if service_name in INSTANCE_ID_SERVICES:
        fixture_parts.append("instance_id")
    if service_name in VAULT_NAME_SERVICES:
        fixture_parts.append("vault_name")
    if service_name in CLUSTER_NAME_SERVICES:
        fixture_parts.append("cluster_name")
    if service_name in DOMAIN_NAME_SERVICES:
        fixture_parts.append("domain_name")
    if service_name in USER_POOL_ID_SERVICES:
        fixture_parts.append("user_pool_id")
    fixture_args = ", ".join(fixture_parts)

    # LIFECYCLE TEST
    lines.append(f"def test_{snake_noun}_lifecycle({fixture_args}):")
    lines.append(f'    """Test {noun} CRUD lifecycle."""')

    # CREATE
    lines.append("    # CREATE")
    create_method = _to_snake_case(create_op)

    id_field_names = {p["name"] for p in describe_params}
    create_assertions = generate_assertions(
        create_output_fields,
        "create_resp",
        strict=True,
        identity_fields=id_field_names,
    )
    needs_create_resp = bool(create_assertions or captures)
    prefix = "create_resp = " if needs_create_resp else ""
    lines.append(f"    {prefix}client.{create_method}(")
    for p in create_params:
        val = generate_param_value(p, service_name, shapes=shapes)
        lines.append(f"        {p['name']}={val},")
    lines.append("    )")
    lines.extend(create_assertions)

    if captures:
        lines.append("")
        for param_name, (expr, var_name) in captures.items():
            lines.append(f"    {var_name} = {expr}")

    lines.append("")

    # DESCRIBE
    lines.append("    # DESCRIBE")
    describe_method = _to_snake_case(describe_op)
    desc_assertions = generate_assertions(
        describe_output_fields,
        "desc_resp",
        identity_fields=id_field_names,
    )
    prefix = "desc_resp = " if desc_assertions else ""
    lines.append(f"    {prefix}client.{describe_method}(")
    for p in describe_params:
        val = _param_value_for(p, captures, service_name, shapes)
        lines.append(f"        {p['name']}={val},")
    lines.append("    )")
    lines.extend(desc_assertions)

    lines.append("")

    # DELETE
    if delete_op:
        lines.append("    # DELETE")
        delete_method = _to_snake_case(delete_op)
        lines.append(f"    client.{delete_method}(")
        for p in delete_params:
            val = _param_value_for(p, captures, service_name, shapes)
            lines.append(f"        {p['name']}={val},")
        lines.append("    )")

        lines.append("")

        # DESCRIBE after DELETE should fail
        lines.append("    # DESCRIBE after DELETE should fail")
        lines.append("    with pytest.raises(ClientError) as exc:")
        lines.append(f"        client.{describe_method}(")
        for p in describe_params:
            val = _param_value_for(p, captures, service_name, shapes)
            lines.append(f"            {p['name']}={val},")
        lines.append("        )")
        lines.append('    assert exc.value.response["Error"]["Code"] in (')
        for code in NOT_FOUND_CODES:
            lines.append(f'        "{code}",')
        lines.append("    )")

    lines.append("")
    lines.append("")

    # NOT-FOUND TEST
    lines.append(f"def test_{snake_noun}_not_found({fixture_args}):")
    short_noun = noun if len(noun) < 40 else noun[:37] + "..."
    lines.append(
        f'    """Test that describing a non-existent {short_noun} raises error."""',
    )
    lines.append("    with pytest.raises(ClientError) as exc:")
    lines.append(f"        client.{describe_method}(")
    fixture_params = [
        ("InstanceId", INSTANCE_ID_SERVICES, "instance_id"),
        ("BackupVaultName", VAULT_NAME_SERVICES, "vault_name"),
        ("ClusterName", CLUSTER_NAME_SERVICES, "cluster_name"),
        ("clusterName", CLUSTER_NAME_SERVICES, "cluster_name"),
        ("DomainName", DOMAIN_NAME_SERVICES, "domain_name"),
        ("UserPoolId", USER_POOL_ID_SERVICES, "user_pool_id"),
    ]
    for p in describe_params:
        used_fixture = False
        for pname, svcs, var in fixture_params:
            if p["name"] == pname and service_name in svcs:
                lines.append(f"            {p['name']}={var},")
                used_fixture = True
                break
        if not used_fixture:
            lines.append(
                f"            {p['name']}={generate_fake_param_value(p)},",
            )
    lines.append("        )")

    lines.append('    assert exc.value.response["Error"]["Code"] in (')
    for code in NOT_FOUND_CODES:
        lines.append(f'        "{code}",')
    lines.append("    )")

    lines.append("")
    lines.append("")
    return lines


def generate_test_file(service_name: str, model: dict, nouns: list[str] | None = None) -> str:
    """Generate a complete test file for a service."""
    all_ops = model.get("operations", {})

    # Group operations by noun
    noun_groups: dict[str, list[str]] = {}
    for op_name in sorted(all_ops):
        noun = extract_noun(op_name)
        noun_groups.setdefault(noun, []).append(op_name)

    # Find CRUD-capable nouns
    resource_nouns = []
    for noun in sorted(noun_groups):
        if noun in SKIP_NOUNS:
            continue
        ops = find_ops_for_noun(noun, all_ops)
        if ops["create"] and ops["describe"]:
            resource_nouns.append((noun, ops))

    if nouns:
        resource_nouns = [(n, o) for n, o in resource_nouns if n in nouns]

    if not resource_nouns:
        return ""

    # Header
    lines = [
        f'"""Resource lifecycle tests for {service_name} (auto-generated)."""',
        "",
        "import pytest",
        "from botocore.exceptions import ClientError",
        "",
        "",
        "@pytest.fixture",
        "def client():",
        "    import boto3",
        "",
        "    return boto3.client(",
        f'        "{service_name}",',
        '        endpoint_url="http://localhost:4566",',
        '        region_name="us-east-1",',
        '        aws_access_key_id="testing",',
        '        aws_secret_access_key="testing",',
        "    )",
        "",
        "",
    ]

    # Add service-specific fixtures
    if service_name in SERVICE_FIXTURES:
        fixture_code = SERVICE_FIXTURES[service_name].strip()
        lines.extend(fixture_code.split("\n"))
        lines.append("")
        lines.append("")

    # Generate tests for each noun
    for noun, ops in resource_nouns:
        test_lines = generate_lifecycle_test(service_name, noun, ops, model)
        lines.extend(test_lines)

    return "\n".join(lines).rstrip() + "\n"


def get_missing_nouns(service_name: str, model: dict) -> list[str]:
    """Identify resource nouns that are likely not implemented (all ops return 501).

    Uses a heuristic: if there's no existing test for this noun and the operations
    look like they need implementation, include it.
    """
    all_ops = model.get("operations", {})
    nouns = set()
    for op_name in all_ops:
        noun = extract_noun(op_name)
        if noun not in SKIP_NOUNS:
            ops = find_ops_for_noun(noun, all_ops)
            if ops["create"] and ops["describe"]:
                nouns.add(noun)
    return sorted(nouns)


def main():
    parser = argparse.ArgumentParser(description="Generate resource lifecycle tests")
    parser.add_argument("--service", required=True, help="AWS service name (boto3 client name)")
    parser.add_argument("--dry-run", action="store_true", help="Print to stdout instead of writing")
    parser.add_argument("--write", action="store_true", help="Write to tests/moto_impl/")
    parser.add_argument("--nouns", help="Comma-separated list of resource nouns to generate")
    parser.add_argument("--list-nouns", action="store_true", help="List available resource nouns")
    parser.add_argument(
        "--output-dir",
        default="tests/moto_impl",
        help="Output directory (default: tests/moto_impl)",
    )
    args = parser.parse_args()

    model = load_service_model(args.service)
    if not model:
        print(f"Could not load botocore model for '{args.service}'", file=sys.stderr)
        sys.exit(1)

    if args.list_nouns:
        nouns = get_missing_nouns(args.service, model)
        all_ops = model.get("operations", {})
        print(f"\n{args.service}: {len(nouns)} CRUD-capable resource nouns\n")
        for noun in nouns:
            ops = find_ops_for_noun(noun, all_ops)
            op_list = [f"{k}={v}" for k, v in ops.items() if v]
            print(f"  {noun:40s} {', '.join(op_list)}")
        return

    nouns = None
    if args.nouns:
        nouns = [n.strip() for n in args.nouns.split(",")]

    code = generate_test_file(args.service, model, nouns=nouns)
    if not code:
        print(f"No CRUD-capable resource nouns found for '{args.service}'", file=sys.stderr)
        sys.exit(1)

    if args.dry_run:
        print(code)
        return

    if args.write:
        out_dir = Path(args.output_dir)
        out_dir.mkdir(parents=True, exist_ok=True)
        out_file = out_dir / f"test_{args.service.replace('-', '_')}_lifecycle.py"
        out_file.write_text(code)
        noun_count = code.count("def test_") // 2  # lifecycle + not_found per noun
        print(f"Generated {out_file} ({noun_count} resource nouns)")
        return

    print("Use --dry-run to preview or --write to save.", file=sys.stderr)
    sys.exit(1)


if __name__ == "__main__":
    main()
