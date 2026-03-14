#!/usr/bin/env python3
"""Generate Moto implementation scaffolding for missing operations.

Analyzes a service's existing models.py and responses.py to understand patterns,
then generates implementation code for missing operations that follows those patterns.

Generates:
- Model classes (FakeResource) with storage, ARN generation, to_dict()
- Backend methods (create_X, describe_X, list_X, update_X, delete_X)
- Response methods that delegate to backend

Usage:
    uv run python scripts/gen_moto_impl.py --service iot --dry-run
    uv run python scripts/gen_moto_impl.py --service iot --write
    uv run python scripts/gen_moto_impl.py --service glue --write --noun CertificateProvider
"""

import argparse
import importlib
import re
import sys
from dataclasses import dataclass, field
from pathlib import Path

import botocore.session

# Re-use helpers from the lifecycle test generator
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


# ────────────────────────────── service analysis ────────────────────────────


@dataclass
class ServiceInfo:
    """Information about a Moto service's implementation patterns."""

    service_name: str
    moto_module: str
    protocol: str
    backend_class_name: str
    response_class_name: str
    error_base_class: str
    not_found_exception: str
    already_exists_exception: str
    uses_json_body: bool  # JSON protocol reads from self.body
    uses_action_result: bool  # Returns ActionResult vs tuple


def analyze_service(service_name: str) -> ServiceInfo | None:
    """Analyze a Moto service to understand its patterns."""
    session = botocore.session.get_session()
    model = session.get_service_model(service_name)
    protocol = model.metadata.get("protocol", "unknown")

    moto_name = service_name.replace("-", "")

    # Try importing
    aliases = {
        "lambda": "awslambda",
        "kinesis-video": "kinesisvideo",
        "cognito-idp": "cognitoidp",
    }
    moto_module = aliases.get(service_name, moto_name)

    try:
        resp_mod = importlib.import_module(f"moto.{moto_module}.responses")
        model_mod = importlib.import_module(f"moto.{moto_module}.models")
    except ImportError:
        print(f"Cannot import moto.{moto_module}", file=sys.stderr)
        return None

    # Find response class
    resp_class_name = None
    for attr in dir(resp_mod):
        obj = getattr(resp_mod, attr)
        if (
            isinstance(obj, type)
            and hasattr(obj, "__mro__")
            and any(c.__name__ == "BaseResponse" for c in obj.__mro__)
            and obj.__name__ != "BaseResponse"
        ):
            resp_class_name = obj.__name__
            break

    # Find backend class
    backend_class_name = None
    for attr in dir(model_mod):
        obj = getattr(model_mod, attr)
        if (
            isinstance(obj, type)
            and hasattr(obj, "__mro__")
            and any(c.__name__ == "BaseBackend" for c in obj.__mro__)
            and obj.__name__ != "BaseBackend"
        ):
            backend_class_name = obj.__name__
            break

    # Detect patterns
    # JSON protocol (glue, stepfunctions) reads from json.loads(self.body)
    # rest-json (iot, connect) uses _get_param which handles both
    uses_json = protocol == "json"
    uses_action_result = True  # Modern moto pattern

    # Try to find exception classes
    try:
        exc_mod = importlib.import_module(f"moto.{moto_module}.exceptions")
        error_base = None
        not_found = "ResourceNotFoundException"
        already_exists = "ResourceAlreadyExistsException"

        for attr in dir(exc_mod):
            obj = getattr(exc_mod, attr)
            if isinstance(obj, type):
                if attr.endswith("ClientError") or attr.endswith("Error"):
                    if any(
                        c.__name__ in ("JsonRESTError", "RESTError")
                        for c in getattr(obj, "__mro__", [])
                    ):
                        error_base = attr
                if "NotFound" in attr or "EntityNotFound" in attr:
                    not_found = attr
                if "AlreadyExists" in attr or "Conflict" in attr:
                    already_exists = attr
    except ImportError:
        error_base = "JsonRESTError"
        not_found = "ResourceNotFoundException"
        already_exists = "ResourceAlreadyExistsException"

    return ServiceInfo(
        service_name=service_name,
        moto_module=moto_module,
        protocol=protocol,
        backend_class_name=backend_class_name or f"{moto_name.title()}Backend",
        response_class_name=resp_class_name or f"{moto_name.title()}Response",
        error_base_class=error_base or "JsonRESTError",
        not_found_exception=not_found,
        already_exists_exception=already_exists,
        uses_json_body=uses_json,
        uses_action_result=uses_action_result,
    )


# ────────────────────────── code generation ─────────────────────────────────


@dataclass
class ResourceGroupOps:
    noun: str
    create_op: str | None = None
    describe_op: str | None = None
    list_op: str | None = None
    update_op: str | None = None
    delete_op: str | None = None
    all_ops: list[str] = field(default_factory=list)


def group_missing_ops(service_name: str) -> list[ResourceGroupOps]:
    """Group missing operations by resource noun."""
    session = botocore.session.get_session()
    model = session.get_service_model(service_name)

    moto_name = service_name.replace("-", "")
    aliases = {"lambda": "awslambda", "kinesis-video": "kinesisvideo", "cognito-idp": "cognitoidp"}
    moto_module = aliases.get(service_name, moto_name)

    try:
        resp_mod = importlib.import_module(f"moto.{moto_module}.responses")
    except ImportError:
        return []

    resp_class = None
    for attr in dir(resp_mod):
        obj = getattr(resp_mod, attr)
        if (
            isinstance(obj, type)
            and hasattr(obj, "__mro__")
            and any(c.__name__ == "BaseResponse" for c in obj.__mro__)
            and obj.__name__ != "BaseResponse"
        ):
            resp_class = obj
            break
    if not resp_class:
        return []

    missing = set()
    for op_name in model.operation_names:
        snake = to_snake_case(op_name)
        if not hasattr(resp_class, snake):
            missing.add(op_name)

    # Group by noun
    groups: dict[str, ResourceGroupOps] = {}
    for op_name in sorted(missing):
        noun = extract_noun(op_name)
        if noun not in groups:
            groups[noun] = ResourceGroupOps(noun=noun)
        g = groups[noun]
        g.all_ops.append(op_name)
        verb = get_verb(op_name)
        if verb in ("Create", "Put", "Register") and not g.create_op:
            g.create_op = op_name
        elif verb in ("Describe", "Get") and not g.describe_op:
            g.describe_op = op_name
        elif verb == "List" and not g.list_op:
            g.list_op = op_name
        elif verb in ("Update", "Modify") and not g.update_op:
            g.update_op = op_name
        elif verb in ("Delete", "Remove", "Deregister") and not g.delete_op:
            g.delete_op = op_name

    # Sort CRUD-complete first
    result = sorted(
        groups.values(),
        key=lambda g: (
            not (g.create_op and g.describe_op),
            -len(g.all_ops),
        ),
    )
    return result


def gen_model_class(
    noun: str,
    group: ResourceGroupOps,
    service_model,
    service_info: ServiceInfo,
) -> str:
    """Generate a Fake{Noun} model class."""
    snake_noun = to_snake_case(noun)
    lines = []

    # Determine fields from create operation's input/output shapes
    create_fields: list[tuple[str, str]] = []  # (name, type)
    output_fields: list[tuple[str, str]] = []

    if group.create_op:
        op = service_model.operation_model(group.create_op)
        if op.input_shape:
            for name, member in op.input_shape.members.items():
                py_type = _shape_to_python_type(member)
                create_fields.append((name, py_type))
        if op.output_shape:
            for name, member in op.output_shape.members.items():
                if name == "ResponseMetadata":
                    continue
                py_type = _shape_to_python_type(member)
                output_fields.append((name, py_type))

    # Determine the primary identifier field
    id_field = None
    name_field = None
    arn_field = None
    for name, _ in create_fields:
        lower = name.lower()
        if lower.endswith("name") and not name_field:
            name_field = name
        if lower.endswith("id") and not id_field:
            id_field = name
    for name, _ in output_fields:
        lower = name.lower()
        if lower.endswith("arn"):
            arn_field = name
        if lower.endswith("id") and not id_field:
            id_field = name

    primary_key = name_field or id_field

    lines.append(f"class Fake{noun}(BaseModel):")
    lines.append("    def __init__(")
    lines.append("        self,")
    lines.append("        account_id: str,")
    lines.append("        region_name: str,")

    # Constructor params from required input fields
    if group.create_op:
        op = service_model.operation_model(group.create_op)
        required = set(op.input_shape.metadata.get("required", [])) if op.input_shape else set()
        for name, py_type in create_fields:
            if name in required:
                lines.append(f"        {to_snake_case(name)}: {py_type},")
        for name, py_type in create_fields:
            if name not in required:
                default = "None" if py_type.startswith("Optional") else '""'
                if py_type == "dict":
                    default = "None"
                    py_type = f"Optional[{py_type}]"
                elif py_type == "list":
                    default = "None"
                    py_type = f"Optional[{py_type}]"
                lines.append(f"        {to_snake_case(name)}: {py_type} = {default},")

    lines.append("    ) -> None:")
    lines.append("        self.account_id = account_id")
    lines.append("        self.region_name = region_name")

    # Store all input fields
    if group.create_op:
        op = service_model.operation_model(group.create_op)
        for name, _ in create_fields:
            snake = to_snake_case(name)
            lines.append(f"        self.{snake} = {snake}")

    # Generate derived fields (ARN, ID, timestamps)
    if arn_field:
        svc = service_info.service_name
        lines.append(
            f'        self.arn = f"arn:aws:{svc}:{{region_name}}:{{account_id}}:'
            f'{snake_noun}/{{self.{to_snake_case(primary_key or "name")}}}"'
        )
    if id_field and not any(to_snake_case(id_field) == to_snake_case(n) for n, _ in create_fields):
        lines.append(f"        self.{to_snake_case(id_field)} = str(random.uuid4())")

    lines.append("        self.created_at = utcnow()")
    lines.append("")

    # to_dict method
    lines.append("    def to_dict(self) -> dict:")
    lines.append("        result: dict = {")
    for name, _ in output_fields:
        snake = to_snake_case(name)
        if name.lower().endswith("arn"):
            lines.append(f'            "{name}": self.arn,')
        elif name.lower().endswith("id") and id_field and name == id_field:
            lines.append(f'            "{name}": self.{to_snake_case(id_field)},')
        elif any(to_snake_case(name) == to_snake_case(n) for n, _ in create_fields):
            lines.append(f'            "{name}": self.{snake},')
        elif "time" in name.lower() or "date" in name.lower() or "created" in name.lower():
            lines.append(f'            "{name}": self.created_at.isoformat(),')
        else:
            lines.append(f'            "{name}": self.{snake},')
    lines.append("        }")
    lines.append("        return result")
    lines.append("")

    return "\n".join(lines)


def gen_backend_methods(
    noun: str,
    group: ResourceGroupOps,
    service_model,
    service_info: ServiceInfo,
) -> str:
    """Generate backend methods for a resource group."""
    snake_noun = to_snake_case(noun)
    snake_plural = snake_noun + "s"
    lines = []

    # Storage dict initialization (to add to __init__)
    lines.append("    # Add to __init__:")
    lines.append(f"    # self.{snake_plural}: dict[str, Fake{noun}] = {{}}")
    lines.append("")

    # Determine primary key
    primary_key_name = None
    primary_key_snake = None
    if group.create_op:
        op = service_model.operation_model(group.create_op)
        if op.input_shape:
            required = op.input_shape.metadata.get("required", [])
            for name in required:
                if name.lower().endswith("name") or name.lower().endswith("id"):
                    primary_key_name = name
                    primary_key_snake = to_snake_case(name)
                    break
            if not primary_key_name and required:
                primary_key_name = required[0]
                primary_key_snake = to_snake_case(required[0])

    # CREATE
    if group.create_op:
        op = service_model.operation_model(group.create_op)
        snake_op = to_snake_case(group.create_op)
        params = []
        if op.input_shape:
            for name in op.input_shape.members:
                params.append(f"{to_snake_case(name)}: str")
        lines.append(f"    def {snake_op}(self, {', '.join(params)}) -> Fake{noun}:")
        if primary_key_snake:
            lines.append(f"        if {primary_key_snake} in self.{snake_plural}:")
            lines.append(
                f"            raise {service_info.already_exists_exception}"
                f'("{noun} already exists")'
            )
        lines.append(f"        resource = Fake{noun}(")
        lines.append("            account_id=self.account_id,")
        lines.append("            region_name=self.region_name,")
        if op.input_shape:
            for name in op.input_shape.members:
                lines.append(f"            {to_snake_case(name)}={to_snake_case(name)},")
        lines.append("        )")
        if primary_key_snake:
            lines.append(f"        self.{snake_plural}[{primary_key_snake}] = resource")
        lines.append("        return resource")
        lines.append("")

    # DESCRIBE / GET
    if group.describe_op:
        op = service_model.operation_model(group.describe_op)
        snake_op = to_snake_case(group.describe_op)
        params = []
        if op.input_shape:
            for name in op.input_shape.metadata.get("required", []):
                params.append(f"{to_snake_case(name)}: str")
        lines.append(f"    def {snake_op}(self, {', '.join(params)}) -> Fake{noun}:")
        if primary_key_snake:
            key_param = params[0].split(":")[0].strip() if params else primary_key_snake
            lines.append(f"        if {key_param} not in self.{snake_plural}:")
            lines.append(
                f"            raise {service_info.not_found_exception}"
                f'(f"{noun} {{{key_param}}} not found")'
            )
            lines.append(f"        return self.{snake_plural}[{key_param}]")
        else:
            lines.append("        # TODO: implement lookup")
            lines.append(f"        raise {service_info.not_found_exception}('{noun} not found')")
        lines.append("")

    # LIST
    if group.list_op:
        snake_op = to_snake_case(group.list_op)
        lines.append(f"    def {snake_op}(self) -> list[Fake{noun}]:")
        lines.append(f"        return list(self.{snake_plural}.values())")
        lines.append("")

    # UPDATE
    if group.update_op:
        op = service_model.operation_model(group.update_op)
        snake_op = to_snake_case(group.update_op)
        params = []
        if op.input_shape:
            for name in op.input_shape.members:
                params.append(f"{to_snake_case(name)}: str = None")
        lines.append(f"    def {snake_op}(self, {', '.join(params)}) -> Fake{noun}:")
        if primary_key_snake:
            lines.append(f"        if {primary_key_snake} not in self.{snake_plural}:")
            lines.append(
                f"            raise {service_info.not_found_exception}"
                f'(f"{noun} {{{primary_key_snake}}} not found")'
            )
            lines.append(f"        resource = self.{snake_plural}[{primary_key_snake}]")
        else:
            lines.append("        # TODO: implement lookup")
            lines.append("        resource = None")
        # Update fields
        if op.input_shape:
            required = set(op.input_shape.metadata.get("required", []))
            for name in op.input_shape.members:
                if name not in required:
                    snake = to_snake_case(name)
                    lines.append(f"        if {snake} is not None:")
                    lines.append(f"            resource.{snake} = {snake}")
        lines.append("        return resource")
        lines.append("")

    # DELETE
    if group.delete_op:
        op = service_model.operation_model(group.delete_op)
        snake_op = to_snake_case(group.delete_op)
        params = []
        if op.input_shape:
            for name in op.input_shape.metadata.get("required", []):
                params.append(f"{to_snake_case(name)}: str")
        lines.append(f"    def {snake_op}(self, {', '.join(params)}) -> None:")
        if primary_key_snake:
            key_param = params[0].split(":")[0].strip() if params else primary_key_snake
            lines.append(f"        if {key_param} not in self.{snake_plural}:")
            lines.append(
                f"            raise {service_info.not_found_exception}"
                f'(f"{noun} {{{key_param}}} not found")'
            )
            lines.append(f"        del self.{snake_plural}[{key_param}]")
        else:
            lines.append("        # TODO: implement deletion")
            lines.append("        pass")
        lines.append("")

    return "\n".join(lines)


def gen_response_methods(
    noun: str,
    group: ResourceGroupOps,
    service_model,
    service_info: ServiceInfo,
) -> str:
    """Generate response class methods for a resource group."""
    # Derive backend property name from existing response class pattern
    # e.g., IoTBackend -> iot_backend, GlueBackend -> glue_backend
    backend_stem = service_info.moto_module
    backend_prop = f"{backend_stem}_backend"
    lines = []

    for op_name in group.all_ops:
        op = service_model.operation_model(op_name)
        snake_op = to_snake_case(op_name)
        verb = get_verb(op_name)

        lines.append(f"    def {snake_op}(self) -> ActionResult:")

        # Extract params
        if op.input_shape:
            if service_info.uses_json_body:
                # JSON protocol: params from json body
                lines.append("        params = json.loads(self.body)")
                for name in op.input_shape.members:
                    snake = to_snake_case(name)
                    lines.append(f'        {snake} = params.get("{name}")')
            else:
                # rest-json / query: params from _get_param
                for name in op.input_shape.members:
                    snake = to_snake_case(name)
                    lines.append(f'        {snake} = self._get_param("{name}")')

        # Call backend
        backend_params = []
        if op.input_shape:
            for name in op.input_shape.members:
                snake = to_snake_case(name)
                backend_params.append(f"{snake}={snake}")

        params_str = ", ".join(backend_params)

        if verb in ("Create", "Put", "Register", "Start", "Run"):
            lines.append(f"        resource = self.{backend_prop}.{snake_op}({params_str})")
            lines.append("        return ActionResult(resource.to_dict())")
        elif verb in ("Describe", "Get"):
            lines.append(f"        resource = self.{backend_prop}.{snake_op}({params_str})")
            lines.append("        return ActionResult(resource.to_dict())")
        elif verb == "List":
            lines.append(f"        resources = self.{backend_prop}.{snake_op}({params_str})")
            # Find list key name from output shape
            list_key = None
            if op.output_shape:
                for member_name, member_shape in op.output_shape.members.items():
                    if member_shape.type_name == "list":
                        list_key = member_name
                        break
            if list_key:
                lines.append(
                    f'        return ActionResult({{"{list_key}": '
                    f"[r.to_dict() for r in resources]}})"
                )
            else:
                lines.append("        return ActionResult({})")
        elif verb in ("Update", "Modify"):
            lines.append(f"        resource = self.{backend_prop}.{snake_op}({params_str})")
            lines.append("        return ActionResult(resource.to_dict())")
        elif verb in ("Delete", "Remove", "Deregister"):
            lines.append(f"        self.{backend_prop}.{snake_op}({params_str})")
            lines.append("        return EmptyResult()")
        else:
            lines.append(f"        # TODO: implement {op_name}")
            lines.append("        return EmptyResult()")

        lines.append("")

    return "\n".join(lines)


def _shape_to_python_type(shape) -> str:
    type_map = {
        "string": "str",
        "integer": "int",
        "long": "int",
        "boolean": "bool",
        "timestamp": "str",
        "blob": "bytes",
        "map": "dict",
        "list": "list",
        "double": "float",
        "float": "float",
        "structure": "dict",
    }
    return type_map.get(shape.type_name, "Any")


# ────────────────────────────────── main ────────────────────────────────────


def main():
    parser = argparse.ArgumentParser(description="Generate Moto implementation scaffolding")
    parser.add_argument("--service", required=True, help="AWS service name")
    parser.add_argument("--dry-run", action="store_true", default=True)
    parser.add_argument(
        "--write", action="store_true", help="Write scaffolding to stdout (review before pasting)"
    )
    parser.add_argument("--noun", help="Only generate for specific resource noun")
    parser.add_argument("--output", default=None, help="Output directory for scaffolding files")
    args = parser.parse_args()

    service_info = analyze_service(args.service)
    if not service_info:
        sys.exit(1)

    session = botocore.session.get_session()
    service_model = session.get_service_model(args.service)

    groups = group_missing_ops(args.service)
    if not groups:
        print(f"No missing operations for {args.service}")
        sys.exit(0)

    if args.noun:
        groups = [g for g in groups if g.noun == args.noun]
        if not groups:
            print(f"No group found for noun '{args.noun}'", file=sys.stderr)
            sys.exit(1)

    print(f"# {args.service}: {len(groups)} resource groups with missing ops")
    print(f"# Protocol: {service_info.protocol}")
    print(f"# Backend: {service_info.backend_class_name}")
    print(f"# Response: {service_info.response_class_name}")
    print(f"# Not-found exception: {service_info.not_found_exception}")
    print(f"# Already-exists exception: {service_info.already_exists_exception}")
    print()

    # Generate model classes
    print("# " + "=" * 70)
    print("# MODEL CLASSES (add to models.py)")
    print("# " + "=" * 70)
    print()

    for group in groups:
        if not (group.create_op and group.describe_op):
            continue
        print(f"# --- {group.noun} ---")
        print(gen_model_class(group.noun, group, service_model, service_info))
        print()

    # Generate backend methods
    print()
    print("# " + "=" * 70)
    print(f"# BACKEND METHODS (add to {service_info.backend_class_name})")
    print("# " + "=" * 70)
    print()

    for group in groups:
        if not group.all_ops:
            continue
        print(f"    # --- {group.noun} ---")
        print(gen_backend_methods(group.noun, group, service_model, service_info))
        print()

    # Generate response methods
    print()
    print("# " + "=" * 70)
    print(f"# RESPONSE METHODS (add to {service_info.response_class_name})")
    print("# " + "=" * 70)
    print()

    for group in groups:
        if not group.all_ops:
            continue
        print(f"    # --- {group.noun} ---")
        print(gen_response_methods(group.noun, group, service_model, service_info))
        print()

    # If writing to files, generate separate output
    if args.write and args.output:
        out_dir = Path(args.output)
        out_dir.mkdir(parents=True, exist_ok=True)
        safe_name = args.service.replace("-", "_")

        # Write models scaffolding
        models_path = out_dir / f"{safe_name}_models_scaffold.py"
        with open(models_path, "w") as f:
            f.write(f"# Model scaffolding for {args.service}\n")
            f.write(f"# Paste relevant sections into vendor/moto/moto/{safe_name}/models.py\n\n")
            for group in groups:
                if group.create_op and group.describe_op:
                    f.write(gen_model_class(group.noun, group, service_model, service_info))
                    f.write("\n\n")
        print(f"\nWrote {models_path}", file=sys.stderr)

        # Write responses scaffolding
        resp_path = out_dir / f"{safe_name}_responses_scaffold.py"
        with open(resp_path, "w") as f:
            f.write(f"# Response scaffolding for {args.service}\n")
            f.write(f"# Paste relevant sections into vendor/moto/moto/{safe_name}/responses.py\n\n")
            for group in groups:
                if group.all_ops:
                    f.write(gen_response_methods(group.noun, group, service_model, service_info))
                    f.write("\n\n")
        print(f"Wrote {resp_path}", file=sys.stderr)


if __name__ == "__main__":
    main()
