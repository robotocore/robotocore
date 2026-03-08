"""Velocity Template Language (VTL) evaluator for API Gateway mapping templates.

Supports a subset of VTL used by API Gateway:
- Variable references: $input.body, $input.json('$.field'), $input.path('$.field')
- Context variables: $context.requestId, $context.httpMethod, etc.
- Stage variables: $stageVariables.key
- Utility functions: $util.escapeJavaScript(), $util.urlEncode(), $util.base64Encode()
- Directives: #set, #if/#else/#end, #foreach
"""

import json
import re
import urllib.parse
from base64 import b64decode, b64encode


class VtlContext:
    """Context for VTL template evaluation."""

    def __init__(
        self,
        body: str | None = None,
        headers: dict | None = None,
        query_params: dict | None = None,
        path_params: dict | None = None,
        stage_variables: dict | None = None,
        context_vars: dict | None = None,
    ):
        self.body = body or ""
        self.headers = headers or {}
        self.query_params = query_params or {}
        self.path_params = path_params or {}
        self.stage_variables = stage_variables or {}
        self.context_vars = context_vars or {}
        self._variables: dict[str, object] = {}

    def _parse_body_json(self) -> dict | list | None:
        """Parse body as JSON, return None on failure."""
        if not self.body:
            return None
        try:
            return json.loads(self.body)
        except (json.JSONDecodeError, TypeError):
            return None

    def _json_path(self, expression: str) -> object:
        """Evaluate a simple JSON path expression like '$.field' or '$.a.b'."""
        data = self._parse_body_json()
        if data is None:
            return ""
        parts = expression.lstrip("$").split(".")
        current = data
        for part in parts:
            if not part:
                continue
            # Handle array index: field[0]
            idx_match = re.match(r"^(\w+)\[(\d+)\]$", part)
            if idx_match:
                key, idx = idx_match.group(1), int(idx_match.group(2))
                if isinstance(current, dict) and key in current:
                    current = current[key]
                    if isinstance(current, list) and idx < len(current):
                        current = current[idx]
                    else:
                        return ""
                else:
                    return ""
            elif isinstance(current, dict) and part in current:
                current = current[part]
            else:
                return ""
        return current


class VtlUtil:  # noqa: N801
    """$util functions available in VTL templates.

    Method names match AWS VTL built-in function names (camelCase).
    """

    @staticmethod
    def escapeJavaScript(s: str) -> str:  # noqa: N802
        """Escape string for JavaScript."""
        if not isinstance(s, str):
            s = str(s)
        return (
            s.replace("\\", "\\\\")
            .replace('"', '\\"')
            .replace("'", "\\'")
            .replace("\n", "\\n")
            .replace("\r", "\\r")
            .replace("\t", "\\t")
        )

    @staticmethod
    def urlEncode(s: str) -> str:  # noqa: N802
        if not isinstance(s, str):
            s = str(s)
        return urllib.parse.quote(s, safe="")

    @staticmethod
    def urlDecode(s: str) -> str:  # noqa: N802
        if not isinstance(s, str):
            s = str(s)
        return urllib.parse.unquote(s)

    @staticmethod
    def base64Encode(s: str | bytes) -> str:  # noqa: N802
        if isinstance(s, str):
            s = s.encode()
        return b64encode(s).decode()

    @staticmethod
    def base64Decode(s: str) -> str:  # noqa: N802
        return b64decode(s).decode()

    @staticmethod
    def parseJson(s: str) -> dict | list:  # noqa: N802
        return json.loads(s)

    @staticmethod
    def toJson(obj: object) -> str:  # noqa: N802
        return json.dumps(obj)


def evaluate_vtl(template: str, ctx: VtlContext) -> str:
    """Evaluate a VTL mapping template string.

    Returns the rendered string.
    """
    if not template:
        return ""

    # Process directives first (#set, #if/#else/#end, #foreach)
    result = _process_directives(template, ctx)

    # Then resolve variable references
    result = _resolve_variables(result, ctx)

    return result


def _process_directives(template: str, ctx: VtlContext) -> str:
    """Process VTL directives: #set, #if/#else/#end, #foreach."""
    lines = template.split("\n")
    output_lines: list[str] = []
    i = 0

    while i < len(lines):
        line = lines[i]
        stripped = line.strip()

        # #set($var = value)
        set_match = re.match(r"#set\s*\(\s*\$(\w+)\s*=\s*(.+?)\s*\)", stripped)
        if set_match:
            var_name = set_match.group(1)
            value_expr = set_match.group(2)
            ctx._variables[var_name] = _eval_expression(value_expr, ctx)
            i += 1
            continue

        # #if(condition)
        if_match = re.match(r"#if\s*\(\s*(.+?)\s*\)", stripped)
        if if_match:
            condition = if_match.group(1)
            # Collect blocks until #end
            if_block: list[str] = []
            else_block: list[str] = []
            in_else = False
            depth = 1
            i += 1
            while i < len(lines) and depth > 0:
                inner = lines[i].strip()
                if re.match(r"#if\s*\(", inner):
                    depth += 1
                if inner == "#end":
                    depth -= 1
                    if depth == 0:
                        i += 1
                        break
                if inner == "#else" and depth == 1:
                    in_else = True
                    i += 1
                    continue
                if in_else:
                    else_block.append(lines[i])
                else:
                    if_block.append(lines[i])
                i += 1

            if _eval_condition(condition, ctx):
                sub = "\n".join(if_block)
            else:
                sub = "\n".join(else_block)
            if sub:
                output_lines.append(_process_directives(sub, ctx))
            continue

        # #foreach($item in $list)
        foreach_match = re.match(r"#foreach\s*\(\s*\$(\w+)\s+in\s+(.+?)\s*\)", stripped)
        if foreach_match:
            var_name = foreach_match.group(1)
            list_expr = foreach_match.group(2)
            body_lines: list[str] = []
            depth = 1
            i += 1
            while i < len(lines) and depth > 0:
                inner = lines[i].strip()
                if re.match(r"#foreach\s*\(", inner):
                    depth += 1
                if inner == "#end":
                    depth -= 1
                    if depth == 0:
                        i += 1
                        break
                body_lines.append(lines[i])
                i += 1

            items = _eval_expression(list_expr, ctx)
            if isinstance(items, (list, tuple)):
                body_template = "\n".join(body_lines)
                parts = []
                for item in items:
                    ctx._variables[var_name] = item
                    parts.append(evaluate_vtl(body_template, ctx))
                output_lines.append("\n".join(parts))
            continue

        output_lines.append(line)
        i += 1

    return "\n".join(output_lines)


def _resolve_variables(template: str, ctx: VtlContext) -> str:
    """Resolve $variable references in a template string."""

    def replacer(match: re.Match) -> str:
        expr = match.group(0)
        val = _eval_expression(expr, ctx)
        if val is None:
            return ""
        if isinstance(val, (dict, list)):
            return json.dumps(val)
        return str(val)

    # Match $var, $obj.prop, $obj.method('arg'), ${var}
    pattern = (
        r"\$\{(\w+)\}"  # ${var}
        r"|\$(\w+(?:\.\w+(?:\([^)]*\))?)*)"  # $obj.prop.method('arg')
    )
    return re.sub(pattern, replacer, template)


def _eval_expression(expr: str, ctx: VtlContext) -> object:
    """Evaluate a VTL expression and return its value."""
    expr = expr.strip()

    # String literal
    if (expr.startswith('"') and expr.endswith('"')) or (
        expr.startswith("'") and expr.endswith("'")
    ):
        return expr[1:-1]

    # Numeric literal
    if re.match(r"^-?\d+(\.\d+)?$", expr):
        return float(expr) if "." in expr else int(expr)

    # Boolean
    if expr == "true":
        return True
    if expr == "false":
        return False

    # List literal [a, b, c]
    if expr.startswith("[") and expr.endswith("]"):
        inner = expr[1:-1].strip()
        if not inner:
            return []
        items = [_eval_expression(x.strip(), ctx) for x in _split_args(inner)]
        return items

    # Variable reference
    if expr.startswith("${") and expr.endswith("}"):
        var_name = expr[2:-1]
        return ctx._variables.get(var_name, "")

    if expr.startswith("$"):
        return _resolve_dollar_ref(expr[1:], ctx)

    return expr


def _resolve_dollar_ref(ref: str, ctx: VtlContext) -> object:
    """Resolve a dollar reference like 'input.body' or 'util.urlEncode("x")'."""
    # Split into parts, handling method calls
    parts = _split_dot_path(ref)
    if not parts:
        return ""

    first = parts[0]
    rest = parts[1:]

    # Check user-defined variables first
    if first in ctx._variables:
        obj = ctx._variables[first]
        for part in rest:
            obj = _access(obj, part, ctx)
        return obj

    # Built-in objects
    if first == "input":
        return _resolve_input(rest, ctx)
    if first == "context":
        return _resolve_context(rest, ctx)
    if first == "stageVariables":
        return _resolve_stage_vars(rest, ctx)
    if first == "util":
        return _resolve_util(rest, ctx)
    if first == "method":
        return _resolve_method_ref(rest, ctx)

    # Fallback: try variables
    return ctx._variables.get(first, "")


def _resolve_input(parts: list[str], ctx: VtlContext) -> object:
    """Resolve $input references."""
    if not parts:
        return ""

    first = parts[0]

    if first == "body":
        return ctx.body
    if first == "params" or first.startswith("params("):
        # $input.params('name') — first may be "params('name')" or "params"
        arg = _extract_method_arg(first) if "(" in first else None
        if arg is None and len(parts) > 1:
            arg = _extract_method_arg(parts[1]) if "(" in parts[1] else None
        if arg:
            # Check path > query > header
            return ctx.path_params.get(arg) or ctx.query_params.get(arg) or ctx.headers.get(arg, "")
        return {}

    # $input.json('$.path')
    if first.startswith("json("):
        arg = _extract_method_arg(first)
        if arg:
            result = ctx._json_path(arg)
            if isinstance(result, (dict, list)):
                return json.dumps(result)
            return result
        return ""

    # $input.path('$.path')
    if first.startswith("path("):
        arg = _extract_method_arg(first)
        if arg:
            return ctx._json_path(arg)
        return ""

    return ""


def _resolve_context(parts: list[str], ctx: VtlContext) -> object:
    """Resolve $context references."""
    if not parts:
        return ctx.context_vars
    key = parts[0]
    val = ctx.context_vars.get(key)
    if val is not None:
        # Support nested: $context.identity.sourceIp
        if isinstance(val, dict) and len(parts) > 1:
            for p in parts[1:]:
                if isinstance(val, dict):
                    val = val.get(p, "")
                else:
                    break
        return val
    return ""


def _resolve_stage_vars(parts: list[str], ctx: VtlContext) -> object:
    """Resolve $stageVariables references."""
    if not parts:
        return ctx.stage_variables
    return ctx.stage_variables.get(parts[0], "")


def _resolve_util(parts: list[str], ctx: VtlContext) -> object:
    """Resolve $util method calls."""
    if not parts:
        return ""

    method_call = parts[0]
    name, arg_str = _parse_method_call(method_call)
    if not name:
        return ""

    util = VtlUtil()
    fn = getattr(util, name, None)
    if fn is None:
        return ""

    # Evaluate arguments
    if arg_str is not None:
        arg_val = _eval_expression(arg_str, ctx)
        return fn(arg_val)
    return fn("")


def _resolve_method_ref(parts: list[str], ctx: VtlContext) -> object:
    """Resolve $method references (for $method.request.path etc.)."""
    return ""


def _access(obj: object, part: str, ctx: VtlContext) -> object:
    """Access a property or call a method on an object."""
    if isinstance(obj, dict):
        name, _ = _parse_method_call(part)
        if name and name in ("get", "containsKey", "keySet", "values", "size"):
            if name == "size":
                return len(obj)
            if name == "keySet":
                return list(obj.keys())
            if name == "values":
                return list(obj.values())
            arg = _extract_method_arg(part)
            if name == "get" and arg:
                return obj.get(arg, "")
            if name == "containsKey" and arg:
                return arg in obj
        return obj.get(part, "")

    if isinstance(obj, (list, tuple)):
        name, _ = _parse_method_call(part)
        if name == "size":
            return len(obj)
        if name == "get":
            arg = _extract_method_arg(part)
            if arg is not None:
                try:
                    return obj[int(arg)]
                except (ValueError, IndexError):
                    return ""
        try:
            return obj[int(part)]
        except (ValueError, IndexError):
            return ""

    if isinstance(obj, str):
        name, _ = _parse_method_call(part)
        if name == "length":
            return len(obj)
        if name == "trim":
            return obj.strip()
        if name == "toLowerCase":
            return obj.lower()
        if name == "toUpperCase":
            return obj.upper()

    return ""


def _extract_method_arg(s: str) -> str | None:
    """Extract the argument from a method call like "json('$.x')" or 'params("key")'."""
    match = re.search(r"\(\s*['\"](.+?)['\"]\s*\)", s)
    if match:
        return match.group(1)
    # Numeric arg
    match = re.search(r"\(\s*(\d+)\s*\)", s)
    if match:
        return match.group(1)
    return None


def _parse_method_call(s: str) -> tuple[str | None, str | None]:
    """Parse 'methodName(arg)' into (name, arg_string)."""
    match = re.match(r"(\w+)\((.*)?\)", s)
    if match:
        return match.group(1), match.group(2).strip() if match.group(2) else None
    return s if re.match(r"^\w+$", s) else None, None


def _split_dot_path(ref: str) -> list[str]:
    """Split a dot-separated path, respecting parenthesized expressions.

    'input.json("$.a.b").field' -> ['input', 'json("$.a.b")', 'field']
    """
    parts: list[str] = []
    current = ""
    depth = 0
    for ch in ref:
        if ch == "(":
            depth += 1
            current += ch
        elif ch == ")":
            depth -= 1
            current += ch
        elif ch == "." and depth == 0:
            if current:
                parts.append(current)
            current = ""
        else:
            current += ch
    if current:
        parts.append(current)
    return parts


def _split_args(s: str) -> list[str]:
    """Split comma-separated args respecting parentheses and quotes."""
    result: list[str] = []
    current = ""
    depth = 0
    in_str = False
    str_char = ""
    for ch in s:
        if in_str:
            current += ch
            if ch == str_char:
                in_str = False
            continue
        if ch in ('"', "'"):
            in_str = True
            str_char = ch
            current += ch
        elif ch == "(":
            depth += 1
            current += ch
        elif ch == ")":
            depth -= 1
            current += ch
        elif ch == "," and depth == 0:
            result.append(current.strip())
            current = ""
        else:
            current += ch
    if current.strip():
        result.append(current.strip())
    return result


def _eval_condition(condition: str, ctx: VtlContext) -> bool:
    """Evaluate a VTL condition for #if."""
    condition = condition.strip()

    # Handle negation
    if condition.startswith("!"):
        return not _eval_condition(condition[1:].strip(), ctx)

    # Handle comparison operators
    for op in (" == ", " != ", " > ", " < ", " >= ", " <= "):
        if op in condition:
            left_str, right_str = condition.split(op, 1)
            left = _eval_expression(left_str.strip(), ctx)
            right = _eval_expression(right_str.strip(), ctx)
            if op.strip() == "==":
                return left == right
            if op.strip() == "!=":
                return left != right
            try:
                left_n = float(left) if not isinstance(left, (int, float)) else left
                right_n = float(right) if not isinstance(right, (int, float)) else right
                if op.strip() == ">":
                    return left_n > right_n
                if op.strip() == "<":
                    return left_n < right_n
                if op.strip() == ">=":
                    return left_n >= right_n
                if op.strip() == "<=":
                    return left_n <= right_n
            except (ValueError, TypeError):
                return False
            break

    # Handle && and ||
    if " && " in condition:
        parts = condition.split(" && ")
        return all(_eval_condition(p.strip(), ctx) for p in parts)
    if " || " in condition:
        parts = condition.split(" || ")
        return any(_eval_condition(p.strip(), ctx) for p in parts)

    # Evaluate as truthy
    val = _eval_expression(condition, ctx)
    if val is None or val == "" or val is False or val == 0:
        return False
    return True
