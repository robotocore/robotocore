"""CloudFormation template engine — parses templates and orchestrates resources."""

import json
import re
import threading
import time
from collections import OrderedDict
from dataclasses import dataclass, field

import yaml


@dataclass
class CfnResource:
    logical_id: str
    resource_type: str
    properties: dict
    physical_id: str | None = None
    attributes: dict = field(default_factory=dict)
    status: str = "CREATE_IN_PROGRESS"


@dataclass
class CfnStack:
    stack_id: str
    stack_name: str
    template_body: str
    parameters: dict = field(default_factory=dict)
    resources: OrderedDict = field(default_factory=OrderedDict)
    outputs: dict = field(default_factory=dict)
    status: str = "CREATE_IN_PROGRESS"
    status_reason: str = ""
    created: float = field(default_factory=time.time)
    tags: list = field(default_factory=list)
    description: str = ""
    events: list = field(default_factory=list)
    exports: dict = field(default_factory=dict)

    @property
    def arn(self) -> str:
        return self.stack_id


@dataclass
class CfnChangeSet:
    change_set_id: str
    change_set_name: str
    stack_name: str
    stack_id: str | None = None
    template_body: str = ""
    status: str = "CREATE_COMPLETE"
    status_reason: str = ""
    change_set_type: str = "CREATE"
    parameters: dict = field(default_factory=dict)
    changes: list = field(default_factory=list)
    created: float = field(default_factory=time.time)


class CfnStore:
    """Per-region CloudFormation store."""

    def __init__(self):
        self.stacks: dict[str, CfnStack] = {}
        self.exports: dict[str, dict] = {}  # export_name -> {Value, StackId}
        self.change_sets: dict[str, CfnChangeSet] = {}  # change_set_id -> CfnChangeSet
        self.mutex = threading.RLock()

    def get_stack(self, name_or_id: str) -> CfnStack | None:
        with self.mutex:
            if name_or_id in self.stacks:
                return self.stacks[name_or_id]
            # Search by name, preferring non-deleted stacks
            deleted = None
            for stack in self.stacks.values():
                if stack.stack_name == name_or_id:
                    if stack.status != "DELETE_COMPLETE":
                        return stack
                    deleted = stack
            return deleted

    def list_stacks(self) -> list[CfnStack]:
        with self.mutex:
            return list(self.stacks.values())

    def put_stack(self, stack: CfnStack) -> None:
        with self.mutex:
            self.stacks[stack.stack_id] = stack

    def delete_stack(self, stack_id: str) -> None:
        with self.mutex:
            self.stacks.pop(stack_id, None)


def _cfn_tag_constructor(loader, tag, node):
    """Convert CloudFormation shorthand tags (!Ref, !Sub, etc.) to dicts."""
    if tag == "!GetAtt":
        if isinstance(node.value, list):
            return {f"Fn::{tag[1:]}": node.value}
        return {f"Fn::{tag[1:]}": node.value.split(".")}
    if tag == "!Ref":
        key = "Ref"
    else:
        key = f"Fn::{tag[1:]}"
    if isinstance(node, yaml.SequenceNode):
        return {key: loader.construct_sequence(node)}
    if isinstance(node, yaml.MappingNode):
        return {key: loader.construct_mapping(node)}
    return {key: node.value}


# Register once at import time for the default YAML loader
yaml.add_multi_constructor("", _cfn_tag_constructor, Loader=yaml.SafeLoader)


def parse_template(template_str: str) -> dict:
    """Parse a CloudFormation template (JSON or YAML).

    Supports CloudFormation shorthand tags: !Ref, !Sub, !GetAtt, !Join,
    !Select, !Split, !Equals, !If, !Not, !And, !Or, !FindInMap, etc.
    """
    try:
        return json.loads(template_str)
    except (json.JSONDecodeError, ValueError):
        pass
    return yaml.safe_load(template_str)


def resolve_intrinsics(
    value, resources: dict[str, CfnResource], parameters: dict, region: str, account_id: str
):
    """Resolve CloudFormation intrinsic functions recursively."""
    if isinstance(value, str):
        return value
    if isinstance(value, list):
        return [resolve_intrinsics(v, resources, parameters, region, account_id) for v in value]
    if not isinstance(value, dict):
        return value

    if "Ref" in value:
        ref = value["Ref"]
        if ref == "AWS::Region":
            return region
        if ref == "AWS::AccountId":
            return account_id
        if ref == "AWS::StackName":
            return parameters.get("AWS::StackName", "stack")
        if ref == "AWS::StackId":
            return parameters.get("AWS::StackId", "")
        if ref == "AWS::NoValue":
            return None
        if ref in parameters:
            return parameters[ref]
        if ref in resources and resources[ref].physical_id:
            return resources[ref].physical_id
        return ref

    if "Fn::GetAtt" in value:
        args = value["Fn::GetAtt"]
        if isinstance(args, str):
            parts = args.split(".")
        else:
            parts = args
        logical_id = parts[0]
        attr_name = ".".join(parts[1:]) if len(parts) > 1 else ""
        res = resources.get(logical_id)
        if res and attr_name in res.attributes:
            return res.attributes[attr_name]
        return ""

    if "Fn::Join" in value:
        delimiter, values = value["Fn::Join"]
        resolved = [
            str(resolve_intrinsics(v, resources, parameters, region, account_id)) for v in values
        ]
        return delimiter.join(resolved)

    if "Fn::Sub" in value:
        sub_val = value["Fn::Sub"]
        if isinstance(sub_val, list):
            template_str, vars_map = sub_val
            resolved_vars = {
                k: resolve_intrinsics(v, resources, parameters, region, account_id)
                for k, v in vars_map.items()
            }
        else:
            template_str = sub_val
            resolved_vars = {}

        def replace_var(m):
            var = m.group(1)
            if var in resolved_vars:
                return str(resolved_vars[var])
            if "." in var:
                parts = var.split(".", 1)
                res = resources.get(parts[0])
                if res and parts[1] in res.attributes:
                    return str(res.attributes[parts[1]])
            ref_result = resolve_intrinsics({"Ref": var}, resources, parameters, region, account_id)
            return str(ref_result)

        return re.sub(r"\$\{([^}]+)\}", replace_var, template_str)

    if "Fn::Select" in value:
        index, options = value["Fn::Select"]
        resolved = resolve_intrinsics(options, resources, parameters, region, account_id)
        idx = int(index)
        if isinstance(resolved, list) and idx < len(resolved):
            return resolved[idx]
        return ""

    if "Fn::Split" in value:
        delimiter, source = value["Fn::Split"]
        resolved = str(resolve_intrinsics(source, resources, parameters, region, account_id))
        return resolved.split(delimiter)

    if "Fn::If" in value:
        cond_name, true_val, false_val = value["Fn::If"]
        # Check the Conditions map stored in parameters under __conditions__
        conditions = parameters.get("__conditions__", {})
        if cond_name in conditions:
            cond_val = conditions[cond_name]
            # Evaluate if it's an intrinsic
            if isinstance(cond_val, dict):
                cond_val = resolve_intrinsics(cond_val, resources, parameters, region, account_id)
            if cond_val:
                return resolve_intrinsics(true_val, resources, parameters, region, account_id)
            return resolve_intrinsics(false_val, resources, parameters, region, account_id)
        # Default: take the true branch
        return resolve_intrinsics(true_val, resources, parameters, region, account_id)

    if "Fn::Equals" in value:
        a, b = value["Fn::Equals"]
        ra = resolve_intrinsics(a, resources, parameters, region, account_id)
        rb = resolve_intrinsics(b, resources, parameters, region, account_id)
        return str(ra) == str(rb)

    if "Fn::Not" in value:
        conditions = value["Fn::Not"]
        result = resolve_intrinsics(conditions[0], resources, parameters, region, account_id)
        return not result

    if "Fn::GetAZs" in value:
        return [f"{region}a", f"{region}b", f"{region}c"]

    if "Fn::ImportValue" in value:
        import_name = resolve_intrinsics(
            value["Fn::ImportValue"], resources, parameters, region, account_id
        )
        # Check __imports__ map in parameters
        imports = parameters.get("__imports__", {})
        return imports.get(str(import_name), str(import_name))

    if "Fn::Cidr" in value:
        args = value["Fn::Cidr"]
        ip_block = str(resolve_intrinsics(args[0], resources, parameters, region, account_id))
        count = int(resolve_intrinsics(args[1], resources, parameters, region, account_id))
        cidr_bits = (
            int(resolve_intrinsics(args[2], resources, parameters, region, account_id))
            if len(args) > 2
            else 8
        )
        from robotocore.services.cloudformation.resources import compute_cidr

        return compute_cidr(ip_block, count, cidr_bits)

    if "Fn::Transform" in value:
        # Macros are not fully supported; return the input section as-is
        transform = value["Fn::Transform"]
        params = transform.get("Parameters", {})
        resolved_params = {
            k: resolve_intrinsics(v, resources, parameters, region, account_id)
            for k, v in params.items()
        }
        return {"Fn::Transform": {**transform, "Parameters": resolved_params}}

    if "Fn::And" in value:
        conditions = value["Fn::And"]
        return all(
            resolve_intrinsics(c, resources, parameters, region, account_id) for c in conditions
        )

    if "Fn::Or" in value:
        conditions = value["Fn::Or"]
        return any(
            resolve_intrinsics(c, resources, parameters, region, account_id) for c in conditions
        )

    # Recursively resolve all values in the dict
    return {
        k: resolve_intrinsics(v, resources, parameters, region, account_id)
        for k, v in value.items()
    }


def evaluate_conditions(
    template: dict, resources: dict, parameters: dict, region: str, account_id: str
) -> dict[str, bool]:
    """Evaluate all Conditions in a template, returning {name: bool}."""
    conditions = template.get("Conditions", {})
    result = {}
    for name, expr in conditions.items():
        val = resolve_intrinsics(expr, resources, parameters, region, account_id)
        result[name] = bool(val)
    return result


def build_dependency_order(template: dict) -> list[str]:
    """Topological sort of resources based on DependsOn and Ref/GetAtt usage."""
    resources = template.get("Resources", {})
    deps: dict[str, set[str]] = {lid: set() for lid in resources}

    for lid, res_def in resources.items():
        # Explicit DependsOn
        depends_on = res_def.get("DependsOn", [])
        if isinstance(depends_on, str):
            depends_on = [depends_on]
        for d in depends_on:
            if d in resources:
                deps[lid].add(d)

        # Implicit deps from Ref and GetAtt
        _find_refs(res_def.get("Properties", {}), resources.keys(), deps[lid])

    # Topological sort (Kahn's algorithm)
    in_degree = {lid: 0 for lid in resources}
    for lid, d in deps.items():
        for dep in d:
            in_degree[lid] += 1 if dep in resources else 0

    # Recount properly
    in_degree = {lid: 0 for lid in resources}
    for lid in resources:
        for dep in deps[lid]:
            pass
    # Simple: count how many deps each node has
    queue = [lid for lid in resources if not deps[lid]]
    result = []
    visited = set()
    while queue:
        node = queue.pop(0)
        if node in visited:
            continue
        visited.add(node)
        result.append(node)
        for lid in resources:
            if node in deps[lid]:
                deps[lid].discard(node)
                if not deps[lid] and lid not in visited:
                    queue.append(lid)

    # Add any remaining (circular deps)
    for lid in resources:
        if lid not in visited:
            result.append(lid)

    return result


def _find_refs(value, resource_names, refs: set):
    """Find all Ref and GetAtt references in a value."""
    if isinstance(value, dict):
        if "Ref" in value and value["Ref"] in resource_names:
            refs.add(value["Ref"])
        if "Fn::GetAtt" in value:
            args = value["Fn::GetAtt"]
            name = args[0] if isinstance(args, list) else args.split(".")[0]
            if name in resource_names:
                refs.add(name)
        if "Fn::Sub" in value:
            sub_val = value["Fn::Sub"]
            template_str = sub_val[0] if isinstance(sub_val, list) else sub_val
            for m in re.finditer(r"\$\{([^.}]+)", template_str):
                if m.group(1) in resource_names:
                    refs.add(m.group(1))
        for v in value.values():
            _find_refs(v, resource_names, refs)
    elif isinstance(value, list):
        for v in value:
            _find_refs(v, resource_names, refs)
