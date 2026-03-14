#!/usr/bin/env python3
"""Generate Moto implementation scaffolding for missing service operations.

Analyzes a service's botocore model and existing Moto implementation,
then generates model classes, backend methods, response methods, and
URL entries for missing CRUD-capable resource nouns.

Follows each service's existing patterns (serialization method names,
exception hierarchy, dispatch style).

Usage:
    uv run python scripts/gen_moto_impl.py --service iot --dry-run
    uv run python scripts/gen_moto_impl.py --service glue --write
    uv run python scripts/gen_moto_impl.py --service connect \\
        --nouns "Workspace,EmailAddress" --dry-run
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

SKIP_NOUNS = {
    "Tags",
    "Resource",
    "TagsForResource",
    "Account",
    "AccountSettings",
    "Service",
    "Configuration",
}


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
    name_map = {"cloudwatch": "monitoring", "eventbridge": "events"}
    botocore_name = name_map.get(service_name, service_name)
    return loader.load_service_model(botocore_name, "service-2")


def get_protocol(model: dict) -> str:
    return model.get("metadata", {}).get("protocol", "json")


def detect_url_style(service_name: str) -> str:
    """Detect whether a service uses catchall or explicit URL patterns."""
    urls_path = Path(f"vendor/moto/moto/{service_name}/urls.py")
    if not urls_path.exists():
        # Try underscore variant
        urls_path = Path(f"vendor/moto/moto/{service_name.replace('-', '_')}/urls.py")
    if not urls_path.exists():
        return "catchall"

    content = urls_path.read_text()
    if ".*$" in content:
        return "catchall"
    return "explicit"


def detect_response_style(service_name: str) -> str:
    """Detect whether responses use ActionResult or raw JSON strings."""
    resp_path = Path(f"vendor/moto/moto/{service_name}/responses.py")
    if not resp_path.exists():
        resp_path = Path(f"vendor/moto/moto/{service_name.replace('-', '_')}/responses.py")
    if not resp_path.exists():
        return "action_result"

    content = resp_path.read_text()
    if "ActionResult" in content:
        return "action_result"
    if "json.dumps" in content:
        return "json_dumps"
    return "action_result"


def detect_serializer_method(service_name: str) -> str:
    """Detect whether models use as_dict() or to_dict()."""
    models_path = Path(f"vendor/moto/moto/{service_name}/models.py")
    if not models_path.exists():
        models_path = Path(f"vendor/moto/moto/{service_name.replace('-', '_')}/models.py")
    if not models_path.exists():
        return "to_dict"

    content = models_path.read_text()
    as_dict_count = content.count("def as_dict")
    to_dict_count = content.count("def to_dict")
    return "as_dict" if as_dict_count > to_dict_count else "to_dict"


def detect_exception_base(service_name: str) -> tuple[str, str]:
    """Detect the service's exception base class and module."""
    exc_path = Path(f"vendor/moto/moto/{service_name}/exceptions.py")
    if not exc_path.exists():
        exc_path = Path(f"vendor/moto/moto/{service_name.replace('-', '_')}/exceptions.py")
    if not exc_path.exists():
        return "JsonRESTError", "moto.core.exceptions"

    content = exc_path.read_text()

    # Look for a service-specific base class like GlueClientError, IoTClientError
    match = re.search(r"class (\w+ClientError)\(", content)
    if match:
        return match.group(1), f"moto.{service_name.replace('-', '_')}.exceptions"

    # Look for ResourceNotFoundException
    if "ResourceNotFoundException" in content:
        return "ResourceNotFoundException", f"moto.{service_name.replace('-', '_')}.exceptions"

    return "JsonRESTError", "moto.core.exceptions"


def detect_backend_class(service_name: str) -> str | None:
    """Find the backend class name (e.g., GlueBackend, IoTBackend)."""
    models_path = Path(f"vendor/moto/moto/{service_name}/models.py")
    if not models_path.exists():
        models_path = Path(f"vendor/moto/moto/{service_name.replace('-', '_')}/models.py")
    if not models_path.exists():
        return None

    content = models_path.read_text()
    match = re.search(r"class (\w+Backend)\(BaseBackend\)", content)
    if match:
        return match.group(1)
    return None


def detect_response_class(service_name: str) -> str | None:
    """Find the response class name."""
    resp_path = Path(f"vendor/moto/moto/{service_name}/responses.py")
    if not resp_path.exists():
        resp_path = Path(f"vendor/moto/moto/{service_name.replace('-', '_')}/responses.py")
    if not resp_path.exists():
        return None

    content = resp_path.read_text()
    match = re.search(r"class (\w+Response)\(BaseResponse\)", content)
    if match:
        return match.group(1)
    return None


def get_implemented_methods(service_name: str) -> set[str]:
    """Get the set of already-implemented method names in responses.py."""
    resp_path = Path(f"vendor/moto/moto/{service_name}/responses.py")
    if not resp_path.exists():
        resp_path = Path(f"vendor/moto/moto/{service_name.replace('-', '_')}/responses.py")
    if not resp_path.exists():
        return set()

    content = resp_path.read_text()
    return set(re.findall(r"def (\w+)\(self\)", content))


def find_ops_for_noun(noun: str, all_ops: dict) -> dict:
    result = {"create": None, "describe": None, "list": None, "update": None, "delete": None}

    for op_name in all_ops:
        op_noun = extract_noun(op_name)
        if op_noun != noun:
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


def get_required_params(operation_name: str, model: dict) -> list[dict]:
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
        params.append(
            {
                "name": param_name,
                "shape": shape_ref,
                "type": shape_def.get("type", "string"),
                "enum": shape_def.get("enum"),
            }
        )
    return params


def get_output_fields(operation_name: str, model: dict) -> list[dict]:
    op_spec = model.get("operations", {}).get(operation_name, {})
    output_shape_name = op_spec.get("output", {}).get("shape")
    if not output_shape_name:
        return []

    shapes = model.get("shapes", {})
    output_shape = shapes.get(output_shape_name, {})
    members = output_shape.get("members", {})

    fields = []
    for field_name, member in members.items():
        shape_ref = member.get("shape", "")
        shape_def = shapes.get(shape_ref, {})
        fields.append(
            {
                "name": field_name,
                "shape": shape_ref,
                "type": shape_def.get("type", "string"),
            }
        )
    return fields


def get_request_uri(operation_name: str, model: dict) -> str:
    op_spec = model.get("operations", {}).get(operation_name, {})
    return op_spec.get("http", {}).get("requestUri", "/")


def get_http_method(operation_name: str, model: dict) -> str:
    op_spec = model.get("operations", {}).get(operation_name, {})
    return op_spec.get("http", {}).get("method", "POST")


def python_type_for(shape_type: str) -> str:
    return {
        "string": "str",
        "integer": "int",
        "long": "int",
        "boolean": "bool",
        "timestamp": "Any",
        "blob": "bytes",
        "list": "list",
        "map": "dict",
        "structure": "dict",
    }.get(shape_type, "Any")


def default_for(shape_type: str) -> str:
    return {
        "string": '""',
        "integer": "0",
        "long": "0",
        "boolean": "False",
        "list": "[]",
        "map": "{}",
        "structure": "{}",
    }.get(shape_type, "None")


def generate_model_class(
    noun: str,
    ops: dict,
    model: dict,
    service_name: str,
    serializer_method: str,
) -> str:
    """Generate a Fake{Noun} model class."""
    class_name = f"Fake{noun}"
    create_op = ops.get("create")
    describe_op = ops.get("describe")

    create_params = get_required_params(create_op, model) if create_op else []
    describe_output = get_output_fields(describe_op, model) if describe_op else []

    # Determine which params become instance attributes
    init_params = []
    for p in create_params:
        snake = _to_snake_case(p["name"])
        init_params.append(
            {
                "name": p["name"],
                "snake": snake,
                "type": python_type_for(p["type"]),
                "shape_type": p["type"],
            }
        )

    lines = []
    lines.append(f"class {class_name}(BaseModel):")

    # __init__
    init_args = ["self"]
    for p in init_params:
        init_args.append(f"{p['snake']}: {p['type']}")
    init_args.extend(["account_id: str", "region_name: str"])

    lines.append("    def __init__(")
    for i, arg in enumerate(init_args):
        comma = "," if i < len(init_args) - 1 else ","
        lines.append(f"        {arg}{comma}")
    lines.append("    ) -> None:")

    # Assignments
    lines.append("        self.account_id = account_id")
    lines.append("        self.region_name = region_name")

    for p in init_params:
        lines.append(f"        self.{p['snake']} = {p['snake']}")

    # Generate ID and ARN
    id_field = None
    arn_field = None
    for f in describe_output:
        fl = f["name"].lower()
        if fl.endswith("id") and not id_field and f["type"] == "string":
            id_field = f["name"]
        if fl.endswith("arn") and not arn_field and f["type"] == "string":
            arn_field = f["name"]

    # Only generate a UUID for ID fields not already provided as init params
    init_param_snakes = {p["snake"] for p in init_params}
    if id_field and _to_snake_case(id_field) not in init_param_snakes:
        lines.append(f"        self.{_to_snake_case(id_field)} = str(uuid.uuid4())")
    if arn_field:
        # Find the primary name/id param for the ARN
        primary_key = "resource"
        for p in init_params:
            if "name" in p["snake"] or "id" in p["snake"]:
                primary_key = p["snake"]
                break
        lines.append(
            f'        self.arn = f"arn:aws:{service_name}:{{region_name}}:{{account_id}}:'
            f'{_to_snake_case(noun)}/{{self.{primary_key}}}"'
        )

    lines.append("        self.created_at = datetime.now(timezone.utc)")

    lines.append("")

    # to_dict / as_dict
    lines.append(f"    def {serializer_method}(self) -> dict[str, Any]:")
    lines.append("        return {")

    for f in describe_output:
        fl = f["name"].lower()
        if f["name"] == arn_field:
            lines.append(f'            "{f["name"]}": self.arn,')
        elif fl.endswith("arn") and f["type"] == "string":
            # Other ARN fields (not the primary ARN) — use empty default
            snake = _to_snake_case(f["name"])
            if snake in init_param_snakes:
                lines.append(f'            "{f["name"]}": self.{snake},')
            else:
                lines.append(f'            "{f["name"]}": "",')
        elif fl.endswith("id") and id_field and f["name"] == id_field:
            lines.append(f'            "{f["name"]}": self.{_to_snake_case(id_field)},')
        else:
            # Try to map to an init param
            snake = _to_snake_case(f["name"])
            matched = False
            for p in init_params:
                if p["snake"] == snake:
                    lines.append(f'            "{f["name"]}": self.{snake},')
                    matched = True
                    break
            if not matched:
                if f["type"] == "string":
                    lines.append(f'            "{f["name"]}": "",')
                elif f["type"] in ("integer", "long"):
                    lines.append(f'            "{f["name"]}": 0,')
                elif f["type"] == "boolean":
                    lines.append(f'            "{f["name"]}": False,')
                elif f["type"] == "list":
                    lines.append(f'            "{f["name"]}": [],')
                elif f["type"] in ("map", "structure"):
                    lines.append(f'            "{f["name"]}": {{}},')
                elif f["type"] == "timestamp":
                    lines.append(f'            "{f["name"]}": self.created_at,')
                else:
                    lines.append(f'            "{f["name"]}": None,')

    lines.append("        }")
    lines.append("")

    return "\n".join(lines)


def generate_backend_methods(
    noun: str,
    ops: dict,
    model: dict,
    service_name: str,
    serializer_method: str,
) -> str:
    """Generate backend methods for a resource noun."""
    class_name = f"Fake{noun}"
    snake_noun = _to_snake_case(noun)
    storage_name = f"{snake_noun}s"
    lines = []

    create_op = ops.get("create")
    describe_op = ops.get("describe")
    list_op = ops.get("list")
    delete_op = ops.get("delete")
    update_op = ops.get("update")

    def _method_sig(name: str, params: list[str], ret: str) -> list[str]:
        """Build a method signature, wrapping if too long."""
        one_line = f"    def {name}(self, {', '.join(params)}) -> {ret}:"
        if len(one_line) <= 100:
            return [one_line]
        result = [f"    def {name}("]
        result.append("        self,")
        for p in params:
            result.append(f"        {p},")
        result.append(f"    ) -> {ret}:")
        return result

    def _raise_not_found(noun_: str, key_: str) -> str:
        msg = f'f"{noun_} {{{key_}}} not found."'
        line = f"            raise ResourceNotFoundException({msg})"
        if len(line) <= 100:
            return line
        return (
            f"            raise ResourceNotFoundException(\n"
            f'                f"{noun_} {{{key_}}} not found."\n'
            f"            )"
        )

    if create_op:
        create_params = get_required_params(create_op, model)
        method_params = []
        for p in create_params:
            method_params.append(f"{_to_snake_case(p['name'])}: {python_type_for(p['type'])}")

        lines.extend(_method_sig(f"create_{snake_noun}", method_params, class_name))
        # Build the constructor call
        ctor_args = []
        for p in create_params:
            ctor_args.append(f"{_to_snake_case(p['name'])}={_to_snake_case(p['name'])}")
        ctor_args.extend(["account_id=self.account_id", "region_name=self.region_name"])

        # Determine the key for storage
        key_param = None
        for p in create_params:
            pl = p["name"].lower()
            if "name" in pl or "id" in pl:
                key_param = p
                break
        if key_param is None and create_params:
            key_param = create_params[0]

        key_snake = _to_snake_case(key_param["name"]) if key_param else "name"

        lines.append(f"        {snake_noun} = {class_name}(")
        for a in ctor_args:
            lines.append(f"            {a},")
        lines.append("        )")
        lines.append(f"        self.{storage_name}[{key_snake}] = {snake_noun}")
        lines.append(f"        return {snake_noun}")
        lines.append("")

    if describe_op:
        desc_params = get_required_params(describe_op, model)
        method_params = []
        for p in desc_params:
            method_params.append(f"{_to_snake_case(p['name'])}: {python_type_for(p['type'])}")

        key_param = None
        for p in desc_params:
            pl = p["name"].lower()
            if "name" in pl or "id" in pl:
                key_param = p
                break
        if key_param is None and desc_params:
            key_param = desc_params[0]

        key_snake = _to_snake_case(key_param["name"]) if key_param else "name"

        if describe_op.startswith("Get"):
            desc_method = f"get_{snake_noun}"
        else:
            desc_method = f"describe_{snake_noun}"
        lines.extend(_method_sig(desc_method, method_params, class_name))
        lines.append(f"        if {key_snake} not in self.{storage_name}:")
        lines.append(_raise_not_found(noun, key_snake))
        lines.append(f"        return self.{storage_name}[{key_snake}]")
        lines.append("")

    if list_op:
        lines.append(f"    def list_{snake_noun}s(self) -> list[{class_name}]:")
        lines.append(f"        return list(self.{storage_name}.values())")
        lines.append("")

    if delete_op:
        del_params = get_required_params(delete_op, model)
        method_params = []
        for p in del_params:
            method_params.append(f"{_to_snake_case(p['name'])}: {python_type_for(p['type'])}")

        key_param = None
        for p in del_params:
            pl = p["name"].lower()
            if "name" in pl or "id" in pl:
                key_param = p
                break
        if key_param is None and del_params:
            key_param = del_params[0]

        key_snake = _to_snake_case(key_param["name"]) if key_param else "name"

        lines.extend(_method_sig(f"delete_{snake_noun}", method_params, "None"))
        lines.append(f"        if {key_snake} not in self.{storage_name}:")
        lines.append(_raise_not_found(noun, key_snake))
        lines.append(f"        del self.{storage_name}[{key_snake}]")
        lines.append("")

    if update_op:
        upd_params = get_required_params(update_op, model)
        method_params = []
        for p in upd_params:
            method_params.append(f"{_to_snake_case(p['name'])}: {python_type_for(p['type'])}")

        key_param = None
        for p in upd_params:
            pl = p["name"].lower()
            if "name" in pl or "id" in pl:
                key_param = p
                break
        if key_param is None and upd_params:
            key_param = upd_params[0]

        key_snake = _to_snake_case(key_param["name"]) if key_param else "name"

        lines.extend(_method_sig(f"update_{snake_noun}", method_params, class_name))
        lines.append(f"        if {key_snake} not in self.{storage_name}:")
        lines.append(_raise_not_found(noun, key_snake))
        lines.append(f"        {snake_noun} = self.{storage_name}[{key_snake}]")
        for p in upd_params:
            if p != key_param:
                sn = _to_snake_case(p["name"])
                lines.append(f"        {snake_noun}.{sn} = {sn}")
        lines.append(f"        return {snake_noun}")
        lines.append("")

    return "\n".join(lines)


def generate_response_methods(
    noun: str,
    ops: dict,
    model: dict,
    service_name: str,
    response_style: str,
    serializer_method: str,
    protocol: str,
) -> str:
    """Generate response methods for a resource noun."""
    snake_noun = _to_snake_case(noun)
    backend_attr = f"self.{service_name.replace('-', '_')}_backend"
    lines = []

    create_op = ops.get("create")
    describe_op = ops.get("describe")
    list_op = ops.get("list")
    delete_op = ops.get("delete")
    update_op = ops.get("update")

    def _param_extraction(params: list[dict]) -> list[str]:
        result = []
        for p in params:
            snake = _to_snake_case(p["name"])
            if protocol == "json":
                result.append(f'        {snake} = self.parameters.get("{p["name"]}")')
            else:
                result.append(f'        {snake} = self._get_param("{p["name"]}")')
        return result

    if create_op:
        create_params = get_required_params(create_op, model)
        create_output = get_output_fields(create_op, model)

        method_name = _to_snake_case(create_op)
        if response_style == "action_result":
            lines.append(f"    def {method_name}(self) -> ActionResult:")
        else:
            lines.append(f"    def {method_name}(self) -> str:")

        lines.extend(_param_extraction(create_params))
        call_args = ", ".join(
            f"{_to_snake_case(p['name'])}={_to_snake_case(p['name'])}" for p in create_params
        )
        lines.append(f"        {snake_noun} = {backend_attr}.create_{snake_noun}({call_args})")

        if response_style == "action_result":
            if create_output:
                lines.append(f"        return ActionResult({snake_noun}.{serializer_method}())")
            else:
                lines.append("        return EmptyResult()")
        else:
            if create_output:
                lines.append(f"        return json.dumps({snake_noun}.{serializer_method}())")
            else:
                lines.append('        return "{}"')
        lines.append("")

    if describe_op:
        desc_params = get_required_params(describe_op, model)
        desc_output = get_output_fields(describe_op, model)

        method_name = _to_snake_case(describe_op)
        if describe_op.startswith("Get"):
            desc_backend_method = f"get_{snake_noun}"
        else:
            desc_backend_method = f"describe_{snake_noun}"

        if response_style == "action_result":
            lines.append(f"    def {method_name}(self) -> ActionResult:")
        else:
            lines.append(f"    def {method_name}(self) -> str:")

        lines.extend(_param_extraction(desc_params))
        call_args = ", ".join(
            f"{_to_snake_case(p['name'])}={_to_snake_case(p['name'])}" for p in desc_params
        )
        lines.append(f"        {snake_noun} = {backend_attr}.{desc_backend_method}({call_args})")

        # Check if output wraps in a key (e.g., {"Thing": {...}})
        wraps_in_key = len(desc_output) == 1 and desc_output[0]["type"] == "structure"

        ser = f"{snake_noun}.{serializer_method}()"
        if response_style == "action_result":
            if wraps_in_key:
                key = desc_output[0]["name"]
                lines.append(f'        return ActionResult({{"{key}": {ser}}})')
            else:
                lines.append(f"        return ActionResult({ser})")
        else:
            if wraps_in_key:
                key = desc_output[0]["name"]
                lines.append(f'        return json.dumps({{"{key}": {ser}}})')
            else:
                lines.append(f"        return json.dumps({ser})")
        lines.append("")

    if list_op:
        list_output = get_output_fields(list_op, model)
        method_name = _to_snake_case(list_op)

        # Find the list key in output
        list_key = None
        for f in list_output:
            if f["type"] == "list":
                list_key = f["name"]
                break
        if not list_key:
            list_key = f"{noun}s"

        if response_style == "action_result":
            lines.append(f"    def {method_name}(self) -> ActionResult:")
        else:
            lines.append(f"    def {method_name}(self) -> str:")

        lines.append(f"        items = {backend_attr}.list_{snake_noun}s()")

        ser = f"[i.{serializer_method}() for i in items]"
        if response_style == "action_result":
            lines.append(f'        return ActionResult({{"{list_key}": {ser}}})')
        else:
            lines.append(f'        return json.dumps({{"{list_key}": {ser}}})')
        lines.append("")

    if delete_op:
        del_params = get_required_params(delete_op, model)
        method_name = _to_snake_case(delete_op)

        if response_style == "action_result":
            lines.append(f"    def {method_name}(self) -> ActionResult:")
        else:
            lines.append(f"    def {method_name}(self) -> str:")

        lines.extend(_param_extraction(del_params))
        call_args = ", ".join(
            f"{_to_snake_case(p['name'])}={_to_snake_case(p['name'])}" for p in del_params
        )
        lines.append(f"        {backend_attr}.delete_{snake_noun}({call_args})")

        if response_style == "action_result":
            lines.append("        return EmptyResult()")
        else:
            lines.append('        return "{}"')
        lines.append("")

    if update_op:
        upd_params = get_required_params(update_op, model)
        method_name = _to_snake_case(update_op)

        if response_style == "action_result":
            lines.append(f"    def {method_name}(self) -> ActionResult:")
        else:
            lines.append(f"    def {method_name}(self) -> str:")

        lines.extend(_param_extraction(upd_params))
        call_args = ", ".join(
            f"{_to_snake_case(p['name'])}={_to_snake_case(p['name'])}" for p in upd_params
        )
        lines.append(f"        {snake_noun} = {backend_attr}.update_{snake_noun}({call_args})")

        if response_style == "action_result":
            lines.append(f"        return ActionResult({snake_noun}.{serializer_method}())")
        else:
            lines.append(f"        return json.dumps({snake_noun}.{serializer_method}())")
        lines.append("")

    return "\n".join(lines)


def generate_url_entries(
    noun: str,
    ops: dict,
    model: dict,
) -> list[str]:
    """Generate URL pattern entries for rest-json explicit URL services."""
    entries = []
    for role, op_name in ops.items():
        if not op_name:
            continue
        uri = get_request_uri(op_name, model)
        if uri == "/":
            continue

        # Convert {param} to (?P<param>[^/]+)
        pattern = re.sub(r"\{(\w+)\+?\}", r"(?P<\1>[^/]+)", uri)
        # Remove leading /
        pattern = pattern.lstrip("/")
        entries.append(f'    "{{0}}/{pattern}$"')

    return entries


def generate_init_storage(nouns: list[str]) -> str:
    """Generate storage dict initializations for __init__."""
    lines = []
    for noun in nouns:
        snake = _to_snake_case(noun)
        lines.append(f"        self.{snake}s: dict[str, Fake{noun}] = {{}}")
    return "\n".join(lines)


def main():
    parser = argparse.ArgumentParser(description="Generate Moto implementation scaffolding")
    parser.add_argument("--service", required=True, help="AWS service name")
    parser.add_argument("--dry-run", action="store_true", help="Print to stdout")
    parser.add_argument("--write", action="store_true", help="Append to existing files")
    parser.add_argument("--nouns", help="Comma-separated list of resource nouns")
    parser.add_argument("--list-nouns", action="store_true", help="List missing nouns")
    args = parser.parse_args()

    model = load_service_model(args.service)
    if not model:
        print(f"Could not load botocore model for '{args.service}'", file=sys.stderr)
        sys.exit(1)

    protocol = get_protocol(model)
    url_style = detect_url_style(args.service)
    response_style = detect_response_style(args.service)
    serializer_method = detect_serializer_method(args.service)
    backend_class = detect_backend_class(args.service)
    response_class = detect_response_class(args.service)
    implemented = get_implemented_methods(args.service)

    all_ops = model.get("operations", {})

    # Find CRUD-capable nouns with at least one unimplemented op
    noun_groups: dict[str, list[str]] = {}
    for op_name in sorted(all_ops):
        noun = extract_noun(op_name)
        noun_groups.setdefault(noun, []).append(op_name)

    resource_nouns = []
    for noun in sorted(noun_groups):
        if noun in SKIP_NOUNS:
            continue
        ops = find_ops_for_noun(noun, all_ops)
        if ops["create"] and ops["describe"]:
            # Check if any ops are missing from implementation
            any_missing = False
            for role, op_name in ops.items():
                if op_name:
                    method_name = _to_snake_case(op_name)
                    if method_name not in implemented:
                        any_missing = True
                        break
            if any_missing:
                resource_nouns.append((noun, ops))

    if args.nouns:
        selected = [n.strip() for n in args.nouns.split(",")]
        resource_nouns = [(n, o) for n, o in resource_nouns if n in selected]

    if args.list_nouns:
        print(f"\n{args.service}: {len(resource_nouns)} nouns with missing implementations")
        print(f"  Protocol: {protocol}, URL style: {url_style}, Response style: {response_style}")
        print(f"  Serializer: {serializer_method}, Backend: {backend_class}")
        print()
        for noun, ops in resource_nouns:
            missing = []
            for role, op_name in ops.items():
                if op_name and _to_snake_case(op_name) not in implemented:
                    missing.append(f"{role}={op_name}")
            print(f"  {noun:40s} missing: {', '.join(missing)}")
        return

    if not resource_nouns:
        print(f"No missing resource nouns for '{args.service}'", file=sys.stderr)
        sys.exit(1)

    # Generate all code sections
    sections = {
        "models": [],
        "backend_methods": [],
        "backend_init": [],
        "responses": [],
        "urls": [],
    }

    for noun, ops in resource_nouns:
        model_code = generate_model_class(noun, ops, model, args.service, serializer_method)
        sections["models"].append(model_code)

        backend_code = generate_backend_methods(noun, ops, model, args.service, serializer_method)
        sections["backend_methods"].append(backend_code)

        sn = _to_snake_case(noun)
        sections["backend_init"].append(f"        self.{sn}s: dict[str, Fake{noun}] = {{}}")

        resp_code = generate_response_methods(
            noun,
            ops,
            model,
            args.service,
            response_style,
            serializer_method,
            protocol,
        )
        sections["responses"].append(resp_code)

        if url_style == "explicit":
            url_entries = generate_url_entries(noun, ops, model)
            sections["urls"].extend(url_entries)

    # Output
    nouns_list = [n for n, _ in resource_nouns]

    output = []
    output.append("# ========================================")
    output.append(f"# Generated scaffolding for {args.service}")
    output.append(f"# Nouns: {', '.join(nouns_list)}")
    output.append(f"# Protocol: {protocol}, URL style: {url_style}")
    output.append("# ========================================")
    output.append("")

    output.append("# --- models.py: Model classes ---")
    output.append("# Add these imports to the top of models.py:")
    output.append("# import uuid")
    output.append("# from datetime import datetime, timezone")
    output.append("# from typing import Any")
    output.append("")
    for code in sections["models"]:
        output.append(code)

    output.append("")
    output.append("# --- models.py: Backend __init__ additions ---")
    output.append(f"# Add to {backend_class}.__init__():")
    for line in sections["backend_init"]:
        output.append(line)

    output.append("")
    output.append("# --- models.py: Backend methods ---")
    output.append(f"# Add to {backend_class}:")
    for code in sections["backend_methods"]:
        output.append(code)

    output.append("")
    output.append("# --- responses.py: Response methods ---")
    output.append(f"# Add to {response_class}:")
    for code in sections["responses"]:
        output.append(code)

    if sections["urls"]:
        output.append("")
        output.append("# --- urls.py: URL entries ---")
        output.append("# Add to url_paths dict (dedup against existing):")
        seen = set()
        for entry in sections["urls"]:
            if entry not in seen:
                output.append(f"    {entry}: {response_class}.dispatch,")
                seen.add(entry)

    if args.dry_run:
        print("\n".join(output))
    elif args.write:
        out_dir = Path("generated")
        out_dir.mkdir(exist_ok=True)
        out_file = out_dir / f"{args.service.replace('-', '_')}_impl.py"
        out_file.write_text("\n".join(output) + "\n")
        print(f"Wrote scaffolding to {out_file}")
        print(f"  {len(nouns_list)} resource nouns")
        print(f"  {len(sections['models'])} model classes")
        print(f"  {len(sections['responses'])} response method groups")
        if sections["urls"]:
            print(f"  {len(set(sections['urls']))} URL entries")
    else:
        print("Use --dry-run to preview or --write to save.", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()
