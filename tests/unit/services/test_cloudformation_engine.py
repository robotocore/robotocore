"""Unit tests for CloudFormation template engine."""

import json

from robotocore.services.cloudformation.engine import (
    CfnResource,
    CfnStack,
    CfnStore,
    _find_refs,
    build_dependency_order,
    parse_template,
    resolve_intrinsics,
)

# ---------------------------------------------------------------------------
# parse_template
# ---------------------------------------------------------------------------


class TestParseTemplate:
    def test_parse_json(self):
        tmpl = json.dumps({"AWSTemplateFormatVersion": "2010-09-09", "Resources": {}})
        result = parse_template(tmpl)
        assert result["AWSTemplateFormatVersion"] == "2010-09-09"
        assert result["Resources"] == {}

    def test_parse_yaml(self):
        tmpl = (
            "AWSTemplateFormatVersion: '2010-09-09'\n"
            "Resources:\n  Bucket:\n    Type: AWS::S3::Bucket"
        )
        result = parse_template(tmpl)
        assert result["AWSTemplateFormatVersion"] == "2010-09-09"
        assert "Bucket" in result["Resources"]

    def test_parse_json_with_nested(self):
        tmpl = json.dumps(
            {
                "Resources": {
                    "MyQueue": {
                        "Type": "AWS::SQS::Queue",
                        "Properties": {"QueueName": "test-queue"},
                    }
                }
            }
        )
        result = parse_template(tmpl)
        assert result["Resources"]["MyQueue"]["Properties"]["QueueName"] == "test-queue"

    def test_parse_yaml_over_invalid_json(self):
        tmpl = "Key: value\nList:\n  - a\n  - b"
        result = parse_template(tmpl)
        assert result["Key"] == "value"
        assert result["List"] == ["a", "b"]


# ---------------------------------------------------------------------------
# resolve_intrinsics
# ---------------------------------------------------------------------------

_RESOURCES = {
    "MyBucket": CfnResource(
        logical_id="MyBucket",
        resource_type="AWS::S3::Bucket",
        properties={},
        physical_id="my-bucket-abc123",
        attributes={
            "Arn": "arn:aws:s3:::my-bucket-abc123",
            "DomainName": "my-bucket.s3.amazonaws.com",
        },
    ),
    "MyQueue": CfnResource(
        logical_id="MyQueue",
        resource_type="AWS::SQS::Queue",
        properties={},
        physical_id="https://sqs.us-east-1.amazonaws.com/123456789012/my-queue",
        attributes={
            "Arn": "arn:aws:sqs:us-east-1:123456789012:my-queue",
            "QueueName": "my-queue",
        },
    ),
}

_PARAMS = {"EnvName": "prod", "AWS::StackName": "my-stack", "AWS::StackId": "arn:aws:cfn:stack-id"}
_REGION = "us-east-1"
_ACCOUNT = "123456789012"


class TestResolveIntrinsicsRef:
    def test_ref_parameter(self):
        result = resolve_intrinsics({"Ref": "EnvName"}, _RESOURCES, _PARAMS, _REGION, _ACCOUNT)
        assert result == "prod"

    def test_ref_aws_region(self):
        result = resolve_intrinsics({"Ref": "AWS::Region"}, _RESOURCES, _PARAMS, _REGION, _ACCOUNT)
        assert result == "us-east-1"

    def test_ref_aws_account_id(self):
        result = resolve_intrinsics(
            {"Ref": "AWS::AccountId"},
            _RESOURCES,
            _PARAMS,
            _REGION,
            _ACCOUNT,
        )
        assert result == "123456789012"

    def test_ref_aws_stack_name(self):
        result = resolve_intrinsics(
            {"Ref": "AWS::StackName"},
            _RESOURCES,
            _PARAMS,
            _REGION,
            _ACCOUNT,
        )
        assert result == "my-stack"

    def test_ref_aws_stack_id(self):
        result = resolve_intrinsics(
            {"Ref": "AWS::StackId"},
            _RESOURCES,
            _PARAMS,
            _REGION,
            _ACCOUNT,
        )
        assert result == "arn:aws:cfn:stack-id"

    def test_ref_aws_no_value(self):
        result = resolve_intrinsics(
            {"Ref": "AWS::NoValue"},
            _RESOURCES,
            _PARAMS,
            _REGION,
            _ACCOUNT,
        )
        assert result is None

    def test_ref_resource_physical_id(self):
        result = resolve_intrinsics({"Ref": "MyBucket"}, _RESOURCES, _PARAMS, _REGION, _ACCOUNT)
        assert result == "my-bucket-abc123"

    def test_ref_unknown_returns_name(self):
        result = resolve_intrinsics({"Ref": "Unknown"}, _RESOURCES, _PARAMS, _REGION, _ACCOUNT)
        assert result == "Unknown"


class TestResolveIntrinsicsGetAtt:
    def test_get_att_list_form(self):
        result = resolve_intrinsics(
            {"Fn::GetAtt": ["MyBucket", "Arn"]},
            _RESOURCES,
            _PARAMS,
            _REGION,
            _ACCOUNT,
        )
        assert result == "arn:aws:s3:::my-bucket-abc123"

    def test_get_att_string_form(self):
        result = resolve_intrinsics(
            {"Fn::GetAtt": "MyBucket.DomainName"},
            _RESOURCES,
            _PARAMS,
            _REGION,
            _ACCOUNT,
        )
        assert result == "my-bucket.s3.amazonaws.com"

    def test_get_att_unknown_resource(self):
        result = resolve_intrinsics(
            {"Fn::GetAtt": ["Nonexistent", "Arn"]},
            _RESOURCES,
            _PARAMS,
            _REGION,
            _ACCOUNT,
        )
        assert result == ""

    def test_get_att_unknown_attribute(self):
        result = resolve_intrinsics(
            {"Fn::GetAtt": ["MyBucket", "NoSuchAttr"]},
            _RESOURCES,
            _PARAMS,
            _REGION,
            _ACCOUNT,
        )
        assert result == ""


class TestResolveIntrinsicsJoin:
    def test_join_simple(self):
        val = {"Fn::Join": ["-", ["a", "b", "c"]]}
        assert resolve_intrinsics(val, _RESOURCES, _PARAMS, _REGION, _ACCOUNT) == "a-b-c"

    def test_join_empty_delimiter(self):
        val = {"Fn::Join": ["", ["hello", "world"]]}
        assert resolve_intrinsics(val, _RESOURCES, _PARAMS, _REGION, _ACCOUNT) == "helloworld"

    def test_join_with_refs(self):
        val = {"Fn::Join": ["-", [{"Ref": "EnvName"}, "queue"]]}
        assert resolve_intrinsics(val, _RESOURCES, _PARAMS, _REGION, _ACCOUNT) == "prod-queue"


class TestResolveIntrinsicsSub:
    def test_sub_simple(self):
        val = {"Fn::Sub": "arn:aws:s3:::${EnvName}-bucket"}
        result = resolve_intrinsics(val, _RESOURCES, _PARAMS, _REGION, _ACCOUNT)
        assert result == "arn:aws:s3:::prod-bucket"

    def test_sub_with_pseudo_params(self):
        val = {"Fn::Sub": "arn:aws:lambda:${AWS::Region}:${AWS::AccountId}:function:my-fn"}
        result = resolve_intrinsics(val, _RESOURCES, _PARAMS, _REGION, _ACCOUNT)
        assert result == "arn:aws:lambda:us-east-1:123456789012:function:my-fn"

    def test_sub_with_vars_map(self):
        val = {"Fn::Sub": ["${Bucket}/${Key}", {"Bucket": "my-bucket", "Key": "data.json"}]}
        result = resolve_intrinsics(val, _RESOURCES, _PARAMS, _REGION, _ACCOUNT)
        assert result == "my-bucket/data.json"

    def test_sub_with_getatt_syntax(self):
        val = {"Fn::Sub": "Bucket ARN is ${MyBucket.Arn}"}
        result = resolve_intrinsics(val, _RESOURCES, _PARAMS, _REGION, _ACCOUNT)
        assert result == "Bucket ARN is arn:aws:s3:::my-bucket-abc123"


class TestResolveIntrinsicsSelect:
    def test_select(self):
        val = {"Fn::Select": [1, ["a", "b", "c"]]}
        assert resolve_intrinsics(val, _RESOURCES, _PARAMS, _REGION, _ACCOUNT) == "b"

    def test_select_first(self):
        val = {"Fn::Select": [0, ["x", "y"]]}
        assert resolve_intrinsics(val, _RESOURCES, _PARAMS, _REGION, _ACCOUNT) == "x"

    def test_select_out_of_bounds(self):
        val = {"Fn::Select": [5, ["a"]]}
        assert resolve_intrinsics(val, _RESOURCES, _PARAMS, _REGION, _ACCOUNT) == ""


class TestResolveIntrinsicsSplit:
    def test_split(self):
        val = {"Fn::Split": [",", "a,b,c"]}
        assert resolve_intrinsics(val, _RESOURCES, _PARAMS, _REGION, _ACCOUNT) == ["a", "b", "c"]

    def test_split_with_ref(self):
        val = {"Fn::Split": ["-", {"Ref": "EnvName"}]}
        # "prod" has no dash, so result is ["prod"]
        assert resolve_intrinsics(val, _RESOURCES, _PARAMS, _REGION, _ACCOUNT) == ["prod"]


class TestResolveIntrinsicsConditions:
    def test_fn_if_returns_true_branch(self):
        val = {"Fn::If": ["IsProduction", "yes", "no"]}
        # Always takes true branch per implementation
        assert resolve_intrinsics(val, _RESOURCES, _PARAMS, _REGION, _ACCOUNT) == "yes"

    def test_fn_equals_true(self):
        val = {"Fn::Equals": ["hello", "hello"]}
        assert resolve_intrinsics(val, _RESOURCES, _PARAMS, _REGION, _ACCOUNT) is True

    def test_fn_equals_false(self):
        val = {"Fn::Equals": ["hello", "world"]}
        assert resolve_intrinsics(val, _RESOURCES, _PARAMS, _REGION, _ACCOUNT) is False

    def test_fn_equals_with_ref(self):
        val = {"Fn::Equals": [{"Ref": "EnvName"}, "prod"]}
        assert resolve_intrinsics(val, _RESOURCES, _PARAMS, _REGION, _ACCOUNT) is True

    def test_fn_not_true(self):
        val = {"Fn::Not": [True]}
        assert resolve_intrinsics(val, _RESOURCES, _PARAMS, _REGION, _ACCOUNT) is False

    def test_fn_not_false(self):
        val = {"Fn::Not": [False]}
        assert resolve_intrinsics(val, _RESOURCES, _PARAMS, _REGION, _ACCOUNT) is True


class TestResolveIntrinsicsGetAZs:
    def test_get_azs(self):
        val = {"Fn::GetAZs": ""}
        result = resolve_intrinsics(val, _RESOURCES, _PARAMS, _REGION, _ACCOUNT)
        assert result == ["us-east-1a", "us-east-1b", "us-east-1c"]


class TestResolveIntrinsicsRecursion:
    def test_string_passthrough(self):
        assert resolve_intrinsics("hello", _RESOURCES, _PARAMS, _REGION, _ACCOUNT) == "hello"

    def test_list_recursion(self):
        val = [{"Ref": "EnvName"}, "literal"]
        result = resolve_intrinsics(val, _RESOURCES, _PARAMS, _REGION, _ACCOUNT)
        assert result == ["prod", "literal"]

    def test_non_dict_passthrough(self):
        assert resolve_intrinsics(42, _RESOURCES, _PARAMS, _REGION, _ACCOUNT) == 42
        assert resolve_intrinsics(True, _RESOURCES, _PARAMS, _REGION, _ACCOUNT) is True

    def test_dict_recursion(self):
        val = {"Key1": {"Ref": "EnvName"}, "Key2": "plain"}
        result = resolve_intrinsics(val, _RESOURCES, _PARAMS, _REGION, _ACCOUNT)
        assert result == {"Key1": "prod", "Key2": "plain"}


# ---------------------------------------------------------------------------
# build_dependency_order
# ---------------------------------------------------------------------------


class TestBuildDependencyOrder:
    def test_no_deps(self):
        tmpl = {"Resources": {"A": {"Type": "AWS::S3::Bucket"}, "B": {"Type": "AWS::SQS::Queue"}}}
        order = build_dependency_order(tmpl)
        assert set(order) == {"A", "B"}
        assert len(order) == 2

    def test_explicit_depends_on_string(self):
        tmpl = {
            "Resources": {
                "A": {"Type": "AWS::S3::Bucket"},
                "B": {"Type": "AWS::SQS::Queue", "DependsOn": "A"},
            }
        }
        order = build_dependency_order(tmpl)
        assert order.index("A") < order.index("B")

    def test_explicit_depends_on_list(self):
        tmpl = {
            "Resources": {
                "A": {"Type": "AWS::S3::Bucket"},
                "B": {"Type": "AWS::SNS::Topic"},
                "C": {"Type": "AWS::SQS::Queue", "DependsOn": ["A", "B"]},
            }
        }
        order = build_dependency_order(tmpl)
        assert order.index("A") < order.index("C")
        assert order.index("B") < order.index("C")

    def test_implicit_ref_dependency(self):
        tmpl = {
            "Resources": {
                "Bucket": {"Type": "AWS::S3::Bucket"},
                "Queue": {
                    "Type": "AWS::SQS::Queue",
                    "Properties": {"Tags": [{"Value": {"Ref": "Bucket"}}]},
                },
            }
        }
        order = build_dependency_order(tmpl)
        assert order.index("Bucket") < order.index("Queue")

    def test_implicit_getatt_dependency(self):
        tmpl = {
            "Resources": {
                "Bucket": {"Type": "AWS::S3::Bucket"},
                "Queue": {
                    "Type": "AWS::SQS::Queue",
                    "Properties": {"QueueName": {"Fn::GetAtt": ["Bucket", "Arn"]}},
                },
            }
        }
        order = build_dependency_order(tmpl)
        assert order.index("Bucket") < order.index("Queue")

    def test_implicit_sub_dependency(self):
        tmpl = {
            "Resources": {
                "Bucket": {"Type": "AWS::S3::Bucket"},
                "Lambda": {
                    "Type": "AWS::Lambda::Function",
                    "Properties": {"Code": {"Fn::Sub": "s3://${Bucket}/code.zip"}},
                },
            }
        }
        order = build_dependency_order(tmpl)
        assert order.index("Bucket") < order.index("Lambda")

    def test_chain_dependency(self):
        tmpl = {
            "Resources": {
                "A": {"Type": "T"},
                "B": {"Type": "T", "DependsOn": "A"},
                "C": {"Type": "T", "DependsOn": "B"},
            }
        }
        order = build_dependency_order(tmpl)
        assert order == ["A", "B", "C"]

    def test_empty_resources(self):
        assert build_dependency_order({"Resources": {}}) == []
        assert build_dependency_order({}) == []


# ---------------------------------------------------------------------------
# _find_refs
# ---------------------------------------------------------------------------


class TestFindRefs:
    def test_finds_ref(self):
        refs = set()
        _find_refs({"Ref": "MyBucket"}, {"MyBucket", "MyQueue"}, refs)
        assert refs == {"MyBucket"}

    def test_finds_getatt(self):
        refs = set()
        _find_refs({"Fn::GetAtt": ["MyQueue", "Arn"]}, {"MyBucket", "MyQueue"}, refs)
        assert refs == {"MyQueue"}

    def test_finds_getatt_string(self):
        refs = set()
        _find_refs({"Fn::GetAtt": "MyQueue.Arn"}, {"MyBucket", "MyQueue"}, refs)
        assert refs == {"MyQueue"}

    def test_finds_sub_refs(self):
        refs = set()
        _find_refs({"Fn::Sub": "arn:${MyBucket}"}, {"MyBucket"}, refs)
        assert refs == {"MyBucket"}

    def test_nested(self):
        refs = set()
        _find_refs({"Outer": {"Inner": {"Ref": "MyBucket"}}}, {"MyBucket"}, refs)
        assert refs == {"MyBucket"}

    def test_list(self):
        refs = set()
        _find_refs([{"Ref": "MyBucket"}, {"Ref": "MyQueue"}], {"MyBucket", "MyQueue"}, refs)
        assert refs == {"MyBucket", "MyQueue"}

    def test_ignores_non_resource_ref(self):
        refs = set()
        _find_refs({"Ref": "SomeParameter"}, {"MyBucket"}, refs)
        assert refs == set()


# ---------------------------------------------------------------------------
# CfnResource & CfnStack dataclasses
# ---------------------------------------------------------------------------


class TestCfnResource:
    def test_defaults(self):
        r = CfnResource(logical_id="Res", resource_type="AWS::S3::Bucket", properties={})
        assert r.physical_id is None
        assert r.attributes == {}
        assert r.status == "CREATE_IN_PROGRESS"

    def test_custom_values(self):
        r = CfnResource(
            logical_id="Q",
            resource_type="AWS::SQS::Queue",
            properties={"QueueName": "q"},
            physical_id="phys-123",
            attributes={"Arn": "arn"},
            status="CREATE_COMPLETE",
        )
        assert r.physical_id == "phys-123"
        assert r.attributes["Arn"] == "arn"


class TestCfnStack:
    def test_arn_returns_stack_id(self):
        s = CfnStack(stack_id="arn:stack-id", stack_name="my-stack", template_body="{}")
        assert s.arn == "arn:stack-id"

    def test_defaults(self):
        s = CfnStack(stack_id="id", stack_name="name", template_body="{}")
        assert s.parameters == {}
        assert s.status == "CREATE_IN_PROGRESS"
        assert s.tags == []


# ---------------------------------------------------------------------------
# CfnStore
# ---------------------------------------------------------------------------


class TestCfnStore:
    def test_put_and_get_by_id(self):
        store = CfnStore()
        stack = CfnStack(stack_id="id-1", stack_name="stack-a", template_body="{}")
        store.put_stack(stack)
        assert store.get_stack("id-1") is stack

    def test_get_by_name(self):
        store = CfnStore()
        stack = CfnStack(stack_id="id-1", stack_name="stack-a", template_body="{}")
        store.put_stack(stack)
        assert store.get_stack("stack-a") is stack

    def test_get_nonexistent(self):
        store = CfnStore()
        assert store.get_stack("nope") is None

    def test_list_stacks(self):
        store = CfnStore()
        store.put_stack(CfnStack(stack_id="a", stack_name="sa", template_body="{}"))
        store.put_stack(CfnStack(stack_id="b", stack_name="sb", template_body="{}"))
        names = {s.stack_name for s in store.list_stacks()}
        assert names == {"sa", "sb"}

    def test_delete_stack(self):
        store = CfnStore()
        store.put_stack(CfnStack(stack_id="a", stack_name="sa", template_body="{}"))
        store.delete_stack("a")
        assert store.get_stack("a") is None

    def test_delete_nonexistent_no_error(self):
        store = CfnStore()
        store.delete_stack("nonexistent")  # Should not raise

    def test_overwrite_stack(self):
        store = CfnStore()
        store.put_stack(CfnStack(stack_id="a", stack_name="v1", template_body="{}"))
        store.put_stack(CfnStack(stack_id="a", stack_name="v2", template_body="{}"))
        assert store.get_stack("a").stack_name == "v2"
