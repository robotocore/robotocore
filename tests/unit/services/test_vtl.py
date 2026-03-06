"""Tests for VTL (Velocity Template Language) evaluator."""

import json

from robotocore.services.apigateway.vtl import VtlContext, VtlUtil, evaluate_vtl


class TestVtlInputBody:
    def test_input_body_reference(self):
        ctx = VtlContext(body='{"name": "test"}')
        result = evaluate_vtl("$input.body", ctx)
        assert result == '{"name": "test"}'

    def test_input_body_empty(self):
        ctx = VtlContext(body="")
        result = evaluate_vtl("$input.body", ctx)
        assert result == ""


class TestVtlInputJson:
    def test_json_simple_field(self):
        ctx = VtlContext(body='{"name": "Alice"}')
        result = evaluate_vtl("$input.json('$.name')", ctx)
        assert result == "Alice"

    def test_json_nested_field(self):
        ctx = VtlContext(body='{"user": {"name": "Bob"}}')
        result = evaluate_vtl("$input.json('$.user.name')", ctx)
        assert result == "Bob"

    def test_json_returns_object_as_json(self):
        ctx = VtlContext(body='{"user": {"name": "Bob"}}')
        result = evaluate_vtl("$input.json('$.user')", ctx)
        parsed = json.loads(result)
        assert parsed == {"name": "Bob"}

    def test_json_missing_field(self):
        ctx = VtlContext(body='{"name": "Alice"}')
        result = evaluate_vtl("$input.json('$.missing')", ctx)
        assert result == ""

    def test_json_invalid_body(self):
        ctx = VtlContext(body="not json")
        result = evaluate_vtl("$input.json('$.name')", ctx)
        assert result == ""


class TestVtlInputPath:
    def test_path_simple(self):
        ctx = VtlContext(body='{"id": 42}')
        result = evaluate_vtl("$input.path('$.id')", ctx)
        assert result == "42"

    def test_path_nested(self):
        ctx = VtlContext(body='{"a": {"b": "deep"}}')
        result = evaluate_vtl("$input.path('$.a.b')", ctx)
        assert result == "deep"


class TestVtlInputParams:
    def test_params_path(self):
        ctx = VtlContext(path_params={"id": "123"})
        result = evaluate_vtl("$input.params('id')", ctx)
        assert result == "123"

    def test_params_query(self):
        ctx = VtlContext(query_params={"page": "2"})
        result = evaluate_vtl("$input.params('page')", ctx)
        assert result == "2"

    def test_params_header(self):
        ctx = VtlContext(headers={"x-custom": "val"})
        result = evaluate_vtl("$input.params('x-custom')", ctx)
        assert result == "val"


class TestVtlContext:
    def test_context_request_id(self):
        ctx = VtlContext(context_vars={"requestId": "abc-123"})
        result = evaluate_vtl("$context.requestId", ctx)
        assert result == "abc-123"

    def test_context_http_method(self):
        ctx = VtlContext(context_vars={"httpMethod": "POST"})
        result = evaluate_vtl("$context.httpMethod", ctx)
        assert result == "POST"

    def test_context_resource_path(self):
        ctx = VtlContext(context_vars={"resourcePath": "/users/{id}"})
        result = evaluate_vtl("$context.resourcePath", ctx)
        assert result == "/users/{id}"

    def test_context_nested(self):
        ctx = VtlContext(
            context_vars={"identity": {"sourceIp": "10.0.0.1"}}
        )
        result = evaluate_vtl("$context.identity.sourceIp", ctx)
        assert result == "10.0.0.1"


class TestVtlStageVariables:
    def test_stage_variable(self):
        ctx = VtlContext(stage_variables={"env": "prod"})
        result = evaluate_vtl("$stageVariables.env", ctx)
        assert result == "prod"

    def test_stage_variable_missing(self):
        ctx = VtlContext(stage_variables={})
        result = evaluate_vtl("$stageVariables.missing", ctx)
        assert result == ""


class TestVtlSetDirective:
    def test_set_string(self):
        template = '#set($name = "hello")\n$name'
        ctx = VtlContext()
        result = evaluate_vtl(template, ctx)
        assert "hello" in result

    def test_set_number(self):
        template = "#set($x = 42)\n$x"
        ctx = VtlContext()
        result = evaluate_vtl(template, ctx)
        assert "42" in result

    def test_set_expression(self):
        template = '#set($greeting = "hi")\n$greeting world'
        ctx = VtlContext()
        result = evaluate_vtl(template, ctx)
        assert "hi world" in result


class TestVtlIfDirective:
    def test_if_true(self):
        template = '#if(true)\nyes\n#end'
        ctx = VtlContext()
        result = evaluate_vtl(template, ctx)
        assert "yes" in result

    def test_if_false(self):
        template = '#if(false)\nyes\n#end'
        ctx = VtlContext()
        result = evaluate_vtl(template, ctx)
        assert "yes" not in result

    def test_if_else(self):
        template = '#if(false)\nyes\n#else\nno\n#end'
        ctx = VtlContext()
        result = evaluate_vtl(template, ctx)
        assert "no" in result
        assert "yes" not in result

    def test_if_variable_comparison(self):
        template = '#set($x = 1)\n#if($x == 1)\nmatch\n#end'
        ctx = VtlContext()
        result = evaluate_vtl(template, ctx)
        assert "match" in result

    def test_if_not_equal(self):
        template = '#set($x = 2)\n#if($x != 1)\ndifferent\n#end'
        ctx = VtlContext()
        result = evaluate_vtl(template, ctx)
        assert "different" in result

    def test_if_negation(self):
        template = '#if(!false)\nnegated\n#end'
        ctx = VtlContext()
        result = evaluate_vtl(template, ctx)
        assert "negated" in result

    def test_if_and(self):
        template = '#if(true && true)\nboth\n#end'
        ctx = VtlContext()
        result = evaluate_vtl(template, ctx)
        assert "both" in result

    def test_if_or(self):
        template = '#if(false || true)\none\n#end'
        ctx = VtlContext()
        result = evaluate_vtl(template, ctx)
        assert "one" in result


class TestVtlForeachDirective:
    def test_foreach_list(self):
        template = '#set($items = ["a", "b", "c"])\n#foreach($item in $items)\n$item\n#end'
        ctx = VtlContext()
        result = evaluate_vtl(template, ctx)
        assert "a" in result
        assert "b" in result
        assert "c" in result

    def test_foreach_empty(self):
        template = '#set($items = [])\n#foreach($item in $items)\n$item\n#end'
        ctx = VtlContext()
        result = evaluate_vtl(template, ctx)
        assert result.strip() == ""


class TestVtlUtil:
    def test_escape_javascript(self):
        result = VtlUtil.escapeJavaScript('hello "world"')
        assert result == 'hello \\"world\\"'

    def test_escape_javascript_newlines(self):
        result = VtlUtil.escapeJavaScript("line1\nline2")
        assert result == "line1\\nline2"

    def test_url_encode(self):
        result = VtlUtil.urlEncode("hello world")
        assert result == "hello%20world"

    def test_url_decode(self):
        result = VtlUtil.urlDecode("hello%20world")
        assert result == "hello world"

    def test_base64_encode(self):
        result = VtlUtil.base64Encode("hello")
        assert result == "aGVsbG8="

    def test_base64_decode(self):
        result = VtlUtil.base64Decode("aGVsbG8=")
        assert result == "hello"

    def test_parse_json(self):
        result = VtlUtil.parseJson('{"key": "value"}')
        assert result == {"key": "value"}

    def test_to_json(self):
        result = VtlUtil.toJson({"key": "value"})
        assert json.loads(result) == {"key": "value"}


class TestVtlUtilInTemplate:
    def test_util_escape_in_template(self):
        ctx = VtlContext()
        ctx._variables["val"] = 'has "quotes"'
        result = evaluate_vtl("$util.escapeJavaScript($val)", ctx)
        assert '\\"' in result

    def test_util_url_encode_in_template(self):
        ctx = VtlContext()
        result = evaluate_vtl("$util.urlEncode('hello world')", ctx)
        assert "hello%20world" in result

    def test_util_base64_encode_in_template(self):
        ctx = VtlContext()
        result = evaluate_vtl("$util.base64Encode('hello')", ctx)
        assert "aGVsbG8=" in result


class TestVtlComplexTemplates:
    def test_request_mapping_template(self):
        body = '{"name": "Alice", "age": 30}'
        ctx = VtlContext(
            body=body,
            headers={"content-type": "application/json"},
            context_vars={"requestId": "req-1", "httpMethod": "POST"},
        )
        template = (
            '{"requestId": "$context.requestId", '
            '"name": "$input.json(\'$.name\')", '
            '"method": "$context.httpMethod"}'
        )
        result = evaluate_vtl(template, ctx)
        parsed = json.loads(result)
        assert parsed["requestId"] == "req-1"
        assert parsed["name"] == "Alice"
        assert parsed["method"] == "POST"

    def test_passthrough_template(self):
        body = '{"key": "value"}'
        ctx = VtlContext(body=body)
        # Common passthrough pattern
        result = evaluate_vtl("$input.body", ctx)
        assert result == body

    def test_empty_template(self):
        ctx = VtlContext()
        assert evaluate_vtl("", ctx) == ""

    def test_no_variables_template(self):
        ctx = VtlContext()
        result = evaluate_vtl("plain text", ctx)
        assert result == "plain text"

    def test_json_path_array_index(self):
        ctx = VtlContext(body='{"items": ["a", "b", "c"]}')
        result = evaluate_vtl("$input.path('$.items[0]')", ctx)
        assert result == "a"
