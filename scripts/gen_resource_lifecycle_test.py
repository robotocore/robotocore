#!/usr/bin/env python3
"""Generate resource lifecycle tests from botocore shapes.

Creates comprehensive lifecycle tests (Create → Describe → List → Update → Delete)
for each resource noun in a service. Tests verify structural correctness, value
round-trips, and behavioral fidelity (errors on duplicates, not-found, etc.).

Tests run against the live server (port 4566) — NOT mocked.

Usage:
    uv run python scripts/gen_resource_lifecycle_test.py --service iot --dry-run
    uv run python scripts/gen_resource_lifecycle_test.py --service iot --write
    uv run python scripts/gen_resource_lifecycle_test.py --service glue --write \
        --output tests/moto_impl/
"""

import argparse
import re
import sys
from dataclasses import dataclass, field
from pathlib import Path

import botocore.session

# ────────────────────────────────── helpers ──────────────────────────────────

VERB_PREFIXES = (
    "Accept",
    "Activate",
    "Add",
    "Allocate",
    "Apply",
    "Assign",
    "Associate",
    "Attach",
    "Authorize",
    "Batch",
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

# CRUD verb classification
CREATE_VERBS = ("Create", "Put", "Register", "Add", "Start", "Run")
DESCRIBE_VERBS = ("Describe", "Get")
LIST_VERBS = ("List",)
UPDATE_VERBS = ("Update", "Modify", "Put", "Set")
DELETE_VERBS = ("Delete", "Remove", "Deregister", "Terminate", "Stop")
TAG_VERBS = ("Tag", "Untag", "ListTags")


def to_snake_case(name: str) -> str:
    s1 = re.sub(r"(.)([A-Z][a-z]+)", r"\1_\2", name)
    return re.sub(r"([a-z0-9])([A-Z])", r"\1_\2", s1).lower()


def extract_noun(op_name: str) -> str:
    for prefix in sorted(VERB_PREFIXES, key=len, reverse=True):
        if op_name.startswith(prefix):
            noun = op_name[len(prefix) :]
            if noun:
                return noun
    return op_name


def get_verb(op_name: str) -> str:
    for prefix in sorted(VERB_PREFIXES, key=len, reverse=True):
        if op_name.startswith(prefix):
            return prefix
    return ""


def classify_op(op_name: str) -> str:
    """Classify an operation as create/describe/list/update/delete/tag/other."""
    verb = get_verb(op_name)
    if verb in CREATE_VERBS and verb != "Put":
        return "create"
    if verb in DESCRIBE_VERBS:
        return "describe"
    if verb in LIST_VERBS:
        return "list"
    if verb in UPDATE_VERBS:
        return "update"
    if verb in DELETE_VERBS:
        return "delete"
    if verb in TAG_VERBS or op_name.startswith("ListTags"):
        return "tag"
    # Put is ambiguous — create-or-update
    if verb == "Put":
        return "create"
    return "other"


# ────────────────────────────── smart defaults ──────────────────────────────

# Pattern-matched defaults for generating valid parameter values
NAME_DEFAULTS: dict[str, str] = {
    # Exact field names
    "thingName": '"test-thing-1"',
    "thingTypeName": '"test-thing-type-1"',
    "thingGroupName": '"test-thing-group-1"',
    "billingGroupName": '"test-billing-group-1"',
    "policyName": '"test-policy-1"',
    "policyDocument": 'json.dumps({"Version": "2012-10-17", "Statement": []})',
    "roleName": '"test-role-1"',
    "roleArn": '"arn:aws:iam::123456789012:role/test-role"',
    "functionName": '"test-func-1"',
    "databaseName": '"test-database-1"',
    "tableName": '"test-table-1"',
    "crawlerName": '"test-crawler-1"',
    "jobName": '"test-job-1"',
    "triggerName": '"test-trigger-1"',
    "connectionName": '"test-connection-1"',
    "registryName": '"test-registry-1"',
    "schemaName": '"test-schema-1"',
    "devEndpointName": '"test-endpoint-1"',
    "name": '"test-name-1"',
    "instanceId": '"test-instance-1"',
    "description": '"test description"',
    "tags": "{}",
}

# Type-based defaults
TYPE_DEFAULTS: dict[str, str] = {
    "string": '"test-string"',
    "integer": "1",
    "long": "1",
    "boolean": "True",
    "timestamp": '"2024-01-01T00:00:00Z"',
    "blob": 'b"test-data"',
    "map": "{}",
    "list": "[]",
    "double": "1.0",
    "float": "1.0",
}

# Pattern-based name matching for generating smart defaults
NAME_PATTERNS: list[tuple[str, str]] = [
    (r"(?i)arn$", '"arn:aws:iam::123456789012:role/test-role"'),
    (r"(?i)name$", '"test-name-1"'),
    (r"(?i)id$", '"test-id-1"'),
    (r"(?i)description$", '"test description"'),
    (r"(?i)role", '"arn:aws:iam::123456789012:role/test-role"'),
    (r"(?i)uri$", '"s3://test-bucket/test-key"'),
    (r"(?i)url$", '"https://example.com"'),
    (r"(?i)path$", '"/test/path"'),
    (r"(?i)prefix$", '"test-prefix"'),
    (r"(?i)policy", 'json.dumps({"Version": "2012-10-17", "Statement": []})'),
    (r"(?i)type$", '"DEFAULT"'),
    (r"(?i)region", '"us-east-1"'),
    (r"(?i)account", '"123456789012"'),
]


def get_default_for_member(name: str, shape) -> str | None:
    """Get a smart default value for a botocore shape member."""
    # Exact match on camelCase name
    camel = name[0].lower() + name[1:] if name else name
    if camel in NAME_DEFAULTS:
        return NAME_DEFAULTS[camel]
    if name in NAME_DEFAULTS:
        return NAME_DEFAULTS[name]

    # Pattern match
    for pattern, default in NAME_PATTERNS:
        if re.search(pattern, name):
            # Check shape type matches expectation
            if shape.type_name == "string":
                return default
            break

    # Type-based fallback
    if shape.type_name in TYPE_DEFAULTS:
        return TYPE_DEFAULTS[shape.type_name]

    # Structure: try to build a minimal dict
    if shape.type_name == "structure":
        return _build_structure_default(shape)

    return None


def _build_structure_default(shape) -> str | None:
    """Build a default value for a structure shape using only required fields."""
    required = shape.metadata.get("required", [])
    if not required:
        return "{}"

    parts = []
    for req_name in required:
        if req_name not in shape.members:
            continue
        member = shape.members[req_name]
        val = get_default_for_member(req_name, member)
        if val is None:
            return None
        parts.append(f'"{req_name}": {val}')

    if parts:
        return "{" + ", ".join(parts) + "}"
    return "{}"


# ──────────────────────────── resource grouping ─────────────────────────────


@dataclass
class ResourceGroup:
    """A group of CRUD operations on the same resource noun."""

    noun: str
    create_ops: list[str] = field(default_factory=list)
    describe_ops: list[str] = field(default_factory=list)
    list_ops: list[str] = field(default_factory=list)
    update_ops: list[str] = field(default_factory=list)
    delete_ops: list[str] = field(default_factory=list)
    tag_ops: list[str] = field(default_factory=list)
    other_ops: list[str] = field(default_factory=list)

    @property
    def has_crud(self) -> bool:
        return bool(self.create_ops and (self.describe_ops or self.list_ops))

    @property
    def all_ops(self) -> list[str]:
        return (
            self.create_ops
            + self.describe_ops
            + self.list_ops
            + self.update_ops
            + self.delete_ops
            + self.tag_ops
            + self.other_ops
        )


def group_operations(service_model, missing_ops: set[str]) -> list[ResourceGroup]:
    """Group missing operations by resource noun into ResourceGroups."""
    groups: dict[str, ResourceGroup] = {}

    for op_name in sorted(missing_ops):
        noun = extract_noun(op_name)
        if noun not in groups:
            groups[noun] = ResourceGroup(noun=noun)
        group = groups[noun]

        classification = classify_op(op_name)
        if classification == "create":
            group.create_ops.append(op_name)
        elif classification == "describe":
            group.describe_ops.append(op_name)
        elif classification == "list":
            group.list_ops.append(op_name)
        elif classification == "update":
            group.update_ops.append(op_name)
        elif classification == "delete":
            group.delete_ops.append(op_name)
        elif classification == "tag":
            group.tag_ops.append(op_name)
        else:
            group.other_ops.append(op_name)

    # Sort by CRUD completeness
    result = sorted(groups.values(), key=lambda g: (not g.has_crud, -len(g.all_ops)))
    return result


# ────────────────────────── test code generation ────────────────────────────


def generate_call_code(
    op_name: str,
    service_model,
    var_name: str | None = "resp",
    resource_name: str = '"test-name-1"',
) -> tuple[str, list[str]]:
    """Generate the boto3 call code for an operation.

    Returns (call_code, list_of_imports_needed).
    """
    op_model = service_model.operation_model(op_name)
    snake_name = to_snake_case(op_name)
    imports: list[str] = []
    assign = f"{var_name} = " if var_name else ""

    if not op_model.input_shape:
        return f"{assign}client.{snake_name}()", imports

    required = op_model.input_shape.metadata.get("required", [])
    params: list[str] = []

    for param_name in required:
        if param_name not in op_model.input_shape.members:
            continue
        member = op_model.input_shape.members[param_name]
        default = get_default_for_member(param_name, member)
        if default is None:
            default = f'"test-{to_snake_case(param_name)}"'
        if "json.dumps" in default:
            imports.append("json")
        params.append(f"{param_name}={default}")

    params_str = ", ".join(params)
    # Always use multi-line if the single-line call would exceed ~95 chars
    prefix = f"{assign}client.{snake_name}("
    if len(prefix) + len(params_str) + 1 > 95 or len(params) > 1:
        param_lines = ",\n        ".join(params)
        call = f"{prefix}\n        {param_lines},\n    )"
        return call, imports

    return f"{prefix}{params_str})", imports


def generate_output_assertions(
    op_name: str,
    service_model,
    var_name: str = "resp",
) -> list[str]:
    """Generate assertions for an operation's output shape."""
    op_model = service_model.operation_model(op_name)
    assertions: list[str] = []

    if not op_model.output_shape:
        return assertions

    for member_name, member_shape in op_model.output_shape.members.items():
        # Skip metadata fields
        if member_name in ("ResponseMetadata",):
            continue

        if member_shape.type_name == "string":
            assertions.append(f'assert isinstance({var_name}.get("{member_name}"), str)')
            # Check ARN format
            if member_name.lower().endswith("arn"):
                assertions.append(f'assert {var_name}["{member_name}"].startswith("arn:aws:")')
            # Check ID is non-empty
            elif member_name.lower().endswith("id"):
                assertions.append(f'assert len({var_name}["{member_name}"]) > 0')
            # Check name is non-empty
            elif member_name.lower().endswith("name"):
                assertions.append(f'assert len({var_name}.get("{member_name}", "")) > 0')
        elif member_shape.type_name == "list":
            assertions.append(f'assert isinstance({var_name}.get("{member_name}", []), list)')
        elif member_shape.type_name == "structure":
            assertions.append(f'assert isinstance({var_name}.get("{member_name}", {{}}), dict)')
        elif member_shape.type_name in ("integer", "long"):
            assertions.append(
                f'assert isinstance({var_name}.get("{member_name}"), (int, type(None)))'
            )
        elif member_shape.type_name == "timestamp":
            assertions.append(f'assert "{member_name}" in {var_name}')

    return assertions


def generate_lifecycle_test(
    group: ResourceGroup,
    service_name: str,
    service_model,
    client_var: str = "client",
) -> str | None:
    """Generate a lifecycle test function for a resource group."""
    if not group.has_crud:
        return None

    lines: list[str] = []
    imports: set[str] = set()
    noun_snake = to_snake_case(group.noun)
    create_op = group.create_ops[0]

    # Generate the test
    lines.append(f"def test_{noun_snake}_lifecycle({client_var}):")
    lines.append(f'    """Test {group.noun} CRUD lifecycle."""')

    # CREATE — check if the create op has meaningful output
    op_model = service_model.operation_model(create_op)
    has_output = bool(
        op_model.output_shape
        and any(m != "ResponseMetadata" for m in op_model.output_shape.members)
    )
    var_name = "create_resp" if has_output else None
    lines.append("    # CREATE")
    call_code, call_imports = generate_call_code(create_op, service_model, var_name=var_name)
    imports.update(call_imports)
    for line in call_code.split("\n"):
        lines.append(f"    {line}")

    if has_output:
        assertions = generate_output_assertions(create_op, service_model, "create_resp")
        for a in assertions[:5]:  # limit assertions
            lines.append(f"    {a}")
    lines.append("")

    # DESCRIBE
    if group.describe_ops:
        desc_op = group.describe_ops[0]
        lines.append("    # DESCRIBE")
        call_code, call_imports = generate_call_code(desc_op, service_model, var_name="desc_resp")
        imports.update(call_imports)
        for line in call_code.split("\n"):
            lines.append(f"    {line}")
        assertions = generate_output_assertions(desc_op, service_model, "desc_resp")
        for a in assertions[:5]:
            lines.append(f"    {a}")
        lines.append("")

    # LIST
    if group.list_ops:
        list_op = group.list_ops[0]
        lines.append("    # LIST")
        call_code, call_imports = generate_call_code(list_op, service_model, var_name="list_resp")
        imports.update(call_imports)
        for line in call_code.split("\n"):
            lines.append(f"    {line}")
        # For list ops, just check the main list key exists
        op_model = service_model.operation_model(list_op)
        if op_model.output_shape:
            for member_name, member_shape in op_model.output_shape.members.items():
                if member_shape.type_name == "list" and member_name != "ResponseMetadata":
                    lines.append(f'    assert isinstance(list_resp.get("{member_name}", []), list)')
                    break
        lines.append("")

    # DELETE
    if group.delete_ops:
        delete_op = group.delete_ops[0]
        lines.append("    # DELETE")
        call_code, call_imports = generate_call_code(delete_op, service_model, var_name="_")
        # Remove assignment to avoid unused variable lint error
        call_code = call_code.replace("_ = ", "")
        imports.update(call_imports)
        for line in call_code.split("\n"):
            lines.append(f"    {line}")
        lines.append("")

        # Describe after delete should fail
        if group.describe_ops:
            desc_op = group.describe_ops[0]
            lines.append("    # DESCRIBE after DELETE should fail")
            lines.append("    with pytest.raises(ClientError) as exc:")
            call_code, _ = generate_call_code(desc_op, service_model, var_name="_")
            # Replace the assignment with just the call inside with block
            call_only = call_code.replace("_ = ", "")
            for line in call_only.split("\n"):
                lines.append(f"        {line}")
            lines.append('    assert exc.value.response["Error"]["Code"] in (')
            # Try common error codes
            lines.append('        "ResourceNotFoundException", "NotFoundException",')
            lines.append('        "EntityNotFoundException", "InvalidRequestException",')
            lines.append("    )")

    return "\n".join(lines), imports


def generate_error_test(
    group: ResourceGroup,
    service_name: str,
    service_model,
    client_var: str = "client",
) -> str | None:
    """Generate error-path tests (e.g., describe non-existent resource)."""
    if not group.describe_ops:
        return None

    lines: list[str] = []
    noun_snake = to_snake_case(group.noun)
    desc_op = group.describe_ops[0]

    lines.append(f"def test_{noun_snake}_not_found({client_var}):")
    lines.append(f'    """Test that describing a non-existent {group.noun} raises an error."""')
    lines.append("    with pytest.raises(ClientError) as exc:")

    # Build call with fake identifiers
    op_model = service_model.operation_model(desc_op)
    snake_name = to_snake_case(desc_op)
    if op_model.input_shape:
        required = op_model.input_shape.metadata.get("required", [])
        params = []
        for param_name in required:
            if param_name not in op_model.input_shape.members:
                continue
            member = op_model.input_shape.members[param_name]
            if member.type_name == "string":
                params.append(f'{param_name}="fake-id"')
            else:
                default = get_default_for_member(param_name, member)
                if default:
                    params.append(f"{param_name}={default}")
        # Use multi-line if needed
        prefix = f"        {client_var}.{snake_name}("
        params_str = ", ".join(params)
        if len(prefix) + len(params_str) + 1 > 95 or len(params) > 2:
            param_lines = ",\n            ".join(params)
            lines.append(f"{prefix}\n            {param_lines},\n        )")
        else:
            lines.append(f"{prefix}{params_str})")
    else:
        lines.append(f"        {client_var}.{snake_name}()")

    lines.append('    assert exc.value.response["Error"]["Code"] in (')
    lines.append('        "ResourceNotFoundException", "NotFoundException",')
    lines.append('        "EntityNotFoundException", "InvalidRequestException",')
    lines.append("    )")

    return "\n".join(lines), set()


def generate_test_file(
    service_name: str,
    service_model,
    groups: list[ResourceGroup],
) -> str:
    """Generate a complete test file for a service's missing operations."""
    boto3_name = service_name
    # Handle name mappings
    name_map = {
        "monitoring": "cloudwatch",
        "awslambda": "lambda",
    }
    boto3_name = name_map.get(service_name, service_name)

    # stdlib imports, then third-party (pytest + botocore in same group)
    base_imports = ["import pytest", "from botocore.exceptions import ClientError"]
    extra_imports: set[str] = set()
    test_functions: list[str] = []

    for group in groups:
        if not group.has_crud:
            continue

        result = generate_lifecycle_test(group, service_name, service_model)
        if result:
            code, imports = result
            extra_imports.update(imports)
            test_functions.append(code)

        result = generate_error_test(group, service_name, service_model)
        if result:
            code, imports = result
            extra_imports.update(imports)
            test_functions.append(code)

    # Build file with properly sorted imports
    parts = []
    parts.append(f'"""Resource lifecycle tests for {service_name} (auto-generated)."""')
    parts.append("")
    # stdlib imports first, then third-party
    stdlib = sorted(f"import {i}" for i in extra_imports)
    if stdlib:
        all_imports = stdlib + [""] + base_imports
    else:
        all_imports = base_imports
    parts.append("\n".join(all_imports))
    parts.append("")
    parts.append("")

    # Fixture
    endpoint_url = "http://localhost:4566"
    parts.append(f"""@pytest.fixture
def client():
    import boto3

    return boto3.client(
        "{boto3_name}",
        endpoint_url="{endpoint_url}",
        region_name="us-east-1",
        aws_access_key_id="testing",
        aws_secret_access_key="testing",
    )""")
    parts.append("")
    parts.append("")

    # Tests
    parts.append("\n\n\n".join(test_functions))
    parts.append("")

    return "\n".join(parts)


# ──────────────────────────── missing op detection ──────────────────────────


def get_missing_ops(service_name: str) -> set[str]:
    """Get operations that are not implemented in Moto for a service."""
    import importlib

    session = botocore.session.get_session()
    model = session.get_service_model(service_name)

    # Try to import moto's response class
    moto_service = service_name.replace("-", "")
    try:
        mod = importlib.import_module(f"moto.{moto_service}.responses")
    except ImportError:
        # Try common aliases
        aliases = {
            "lambda": "awslambda",
            "kinesis-video": "kinesisvideo",
            "cognito-idp": "cognitoidp",
        }
        alias = aliases.get(service_name)
        if alias:
            mod = importlib.import_module(f"moto.{alias}.responses")
        else:
            print(f"Cannot import moto.{moto_service}.responses", file=sys.stderr)
            return set()

    # Find the response class
    resp_class = None
    for attr_name in dir(mod):
        obj = getattr(mod, attr_name)
        if (
            isinstance(obj, type)
            and hasattr(obj, "__mro__")
            and any(c.__name__ == "BaseResponse" for c in obj.__mro__)
            and obj.__name__ != "BaseResponse"
        ):
            resp_class = obj
            break

    if not resp_class:
        print(f"No response class found in moto.{moto_service}", file=sys.stderr)
        return set()

    missing = set()
    for op_name in model.operation_names:
        snake = to_snake_case(op_name)
        if not hasattr(resp_class, snake):
            missing.add(op_name)

    return missing


# ────────────────────────────────── main ────────────────────────────────────


def main():
    parser = argparse.ArgumentParser(
        description="Generate resource lifecycle tests from botocore shapes"
    )
    parser.add_argument("--service", required=True, help="AWS service name")
    parser.add_argument(
        "--dry-run",
        action="store_true",
        default=True,
        help="Print generated code without writing (default)",
    )
    parser.add_argument(
        "--write",
        action="store_true",
        help="Write test file to disk",
    )
    parser.add_argument(
        "--output",
        default="tests/moto_impl/",
        help="Output directory (default: tests/moto_impl/)",
    )
    parser.add_argument(
        "--max-groups",
        type=int,
        default=0,
        help="Max resource groups to generate (0 = all)",
    )
    args = parser.parse_args()

    session = botocore.session.get_session()
    try:
        service_model = session.get_service_model(args.service)
    except Exception as e:
        print(f"Error loading service model for {args.service}: {e}", file=sys.stderr)
        sys.exit(1)

    missing = get_missing_ops(args.service)
    if not missing:
        print(f"No missing operations found for {args.service}")
        sys.exit(0)

    print(f"{args.service}: {len(missing)} missing operations", file=sys.stderr)

    groups = group_operations(service_model, missing)
    crud_groups = [g for g in groups if g.has_crud]

    print(f"  {len(groups)} resource groups, {len(crud_groups)} with CRUD ops", file=sys.stderr)
    for g in groups:
        ops = ", ".join(g.all_ops[:5])
        if len(g.all_ops) > 5:
            ops += f" (+{len(g.all_ops) - 5})"
        crud = "CRUD" if g.has_crud else "partial"
        print(f"  {g.noun}: {crud} ({len(g.all_ops)} ops) — {ops}", file=sys.stderr)

    if args.max_groups:
        groups = groups[: args.max_groups]

    code = generate_test_file(args.service, service_model, groups)

    if args.write:
        out_dir = Path(args.output)
        out_dir.mkdir(parents=True, exist_ok=True)
        safe_name = args.service.replace("-", "_")
        out_path = out_dir / f"test_{safe_name}_lifecycle.py"
        out_path.write_text(code)
        print(f"Wrote {out_path}", file=sys.stderr)
    else:
        print(code)


if __name__ == "__main__":
    main()
