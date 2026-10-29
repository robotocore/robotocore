"""SSM fix-wave lane probe: driver for before/after behavior checks.

Run with the venv python against the scratch twin (ENDPOINT_URL). Not a test
suites; prints JSON findings for the lane report.
"""

import json
import os
import uuid

import boto3
from botocore.exceptions import ClientError

ENDPOINT = os.environ.get("ENDPOINT_URL", "http://localhost:4805")


def client():
    return boto3.client(
        "ssm",
        endpoint_url=ENDPOINT,
        aws_access_key_id="123456789012",
        aws_secret_access_key="probe",
        region_name="us-east-1",
    )


def probe(name, fn):
    try:
        result = fn()
    except ClientError as e:
        code = e.response["Error"]["Code"]
        msg = e.response["Error"]["Message"]
        result = {"error": code, "message": msg}
    except Exception as e:  # noqa: BLE001
        result = {"crash": repr(e)}
    print(f"== {name}\n{json.dumps(result, sort_keys=True, default=str)}")
    return result


def mk(prefix):
    return f"/probe{prefix}-{uuid.uuid4().hex[:6]}"


def main():
    ssm = client()
    out = []

    # --- Fixture parameter tree ---
    base = mk("tree")
    names = {
        "top": f"{base}/top",  # depth 1 under base
        "deep": f"{base}/a/b/c",  # depth 3
        "labeled": f"{base}/lbl",
        "secure": f"{base}/pwd",
    }
    ssm.put_parameter(Name=names["top"], Value="v", Type="String")
    ssm.put_parameter(Name=names["deep"], Value="v", Type="String")
    ssm.put_parameter(Name=names["labeled"], Value="v", Type="String")
    ssm.put_parameter(Name=names["secure"], Value="v", Type="SecureString")
    ssm.label_parameter_version(Name=names["labeled"], Labels=["lbl1"])
    ssm.add_tags_to_resource(
        ResourceId=names["top"], ResourceType="Parameter", Tags=[{"Key": "k1", "Value": "v1"}]
    )

    # F: tag filter actually matching
    def tag_filter():
        r = ssm.describe_parameters(
            ParameterFilters=[{"Key": "tag:k1", "Values": ["v1"]}]
        )
        return [p["Name"] for p in r["Parameters"]]

    out.append(probe("describe tag:k1=v1 -> should list top", tag_filter))

    def tag_filter_nomatch():
        r = ssm.describe_parameters(
            ParameterFilters=[{"Key": "tag:k1", "Values": ["nope"]}]
        )
        return [p["Name"] for p in r["Parameters"]]

    out.append(probe("describe tag:k1=nope -> should be []", tag_filter_nomatch))

    def tag_filter_empty_vals():
        r = ssm.describe_parameters(ParameterFilters=[{"Key": "tag:k1", "Values": []}])
        return [p["Name"] for p in r["Parameters"]]

    out.append(
        probe("describe tag:k1=() -> moto: params having tag k1 only", tag_filter_empty_vals)
    )

    # F: Path OneLevel rooted at "/"
    def path_onelevel_root():
        r = ssm.describe_parameters(
            ParameterFilters=[{"Key": "Path", "Values": ["/"]}]
        )
        return [p["Name"] for p in r["Parameters"]][:8]

    out.append(
        probe("describe Path=/ OneLevel -> moto: only depth-1 params", path_onelevel_root)
    )

    # F: Type filter with empty Values (moto: matches nothing)
    def type_empty_vals():
        r = ssm.describe_parameters(
            ParameterFilters=[{"Key": "Type", "Values": []}]
        )
        return len(r["Parameters"])

    out.append(probe("describe Type=() -> moto: 0 results", type_empty_vals))

    # F: KeyId BeginsWith
    def keyid_begins():
        r = ssm.describe_parameters(
            ParameterFilters=[
                {"Key": "KeyId", "Option": "BeginsWith", "Values": ["alias/"]}
            ]
        )
        return len(r["Parameters"])

    out.append(probe("describe KeyId BeginsWith alias/ -> moto: secure params", keyid_begins))

    # F: Label filter via get_parameters_by_path (valid: Label allowed there)
    def label_by_path():
        r = ssm.get_parameters_by_path(
            Path=base, ParameterFilters=[{"Key": "Label", "Values": ["lbl1"]}]
        )
        return [p["Name"] for p in r["Parameters"]]

    out.append(probe("bypath Label=lbl1 -> should list labeled", label_by_path))

    # _validate_parameter_filters port: describe_parameters (by_path=False)
    def label_in_describe():
        ssm.describe_parameters(ParameterFilters=[{"Key": "Label", "Values": ["lbl1"]}])

    out.append(
        probe(
            "describe Label -> moto: InvalidFilterKey", label_in_describe
        )
    )

    def missing_values():
        ssm.describe_parameters(ParameterFilters=[{"Key": "Name"}])

    out.append(
        probe(
            "describe Name with no Values -> moto: InvalidFilterValue",
            missing_values,
        )
    )

    def dup_filter():
        ssm.describe_parameters(
            ParameterFilters=[
                {"Key": "Name", "Values": ["a"]},
                {"Key": "Name", "Values": ["b"]},
            ]
        )

    out.append(probe("describe dup Name -> moto: InvalidFilterKey", dup_filter))

    def reserved_path_vals():
        ssm.describe_parameters(
            ParameterFilters=[{"Key": "Path", "Values": ["/aws", "/ssm"]}]
        )

    out.append(
        probe("describe Path=/aws,/ssm -> moto: ValidationException", reserved_path_vals)
    )

    def bad_path_shape():
        ssm.describe_parameters(ParameterFilters=[{"Key": "Path", "Values": ["test"]}])

    out.append(
        probe("describe Path=test -> moto: ValidationException", bad_path_shape)
    )

    def bad_tier_value():
        ssm.describe_parameters(ParameterFilters=[{"Key": "Tier", "Values": ["zzz"]}])

    out.append(probe("describe Tier=zzz -> moto: InvalidFilterOption", bad_tier_value))

    def bad_option():
        ssm.describe_parameters(
            ParameterFilters=[{"Key": "Name", "Option": "Wobble", "Values": ["x"]}]
        )

    out.append(probe("describe Name Option=Wobble -> moto: InvalidFilterOption", bad_option))

    def both_filters():
        ssm.describe_parameters(
            Filters=[{"Key": "Name", "Values": ["x"]}],
            ParameterFilters=[{"Key": "Name", "Values": ["x"]}],
        )

    out.append(probe("describe Filters+ParameterFilters -> moto: ValidationException", both_filters))

    def shape_errors():
        ssm.describe_parameters(
            ParameterFilters=[
                {"Key": "Name", "Option": "over 10 chars"},
                {"Key": "key"},
            ]
        )

    out.append(probe("describe shape x2 -> moto: '2 validation errors detected:'", shape_errors))

    # _validate_parameter_filters (by_path=True): Name/Path/Tier invalid there
    def bypath_name_filter():
        ssm.get_parameters_by_path(
            Path=base, ParameterFilters=[{"Key": "Name", "Values": ["x"]}]
        )

    out.append(probe("bypath Name -> moto: InvalidFilterKey", bypath_name_filter))

    def bypath_tier_filter():
        ssm.get_parameters_by_path(
            Path=base, ParameterFilters=[{"Key": "Tier", "Values": ["Standard"]}]
        )

    out.append(probe("bypath Tier -> moto: InvalidFilterKey", bypath_tier_filter))

    # reserved-name validation
    def put_aws_reserved():
        name = f"{base}/aws/x"
        ssm.put_parameter(Name=name, Value="v", Type="String")

    out.append(
        probe(
            "put /aws-path -> moto: AccessDeniedException 'No access to reserved parameter name'",
            put_aws_reserved,
        )
    )

    def put_aws_no_leading():
        ssm.put_parameter(
            Name="aws-path-" + uuid.uuid4().hex[:4], Value="v", Type="String"
        )

    out.append(
        probe(
            "put aws-prefixed bare name -> moto: ValidationException (can't be prefixed with aws)",
            put_aws_no_leading,
        )
    )

    def put_ssm_path():
        ssm.put_parameter(
            Name="ssm-path/" + uuid.uuid4().hex[:4], Value="v", Type="String"
        )

    out.append(
        probe(
            "put ssm/path name -> moto: ValidationException (ssm path message)",
            put_ssm_path,
        )
    )

    def put_aws_leaf():
        ssm.put_parameter(Name="x/aws", Value="v", Type="String")

    out.append(
        probe(
            "put a subdir path containing 'aws' sub-path segment -> ok (not reserved)",
            put_aws_leaf,
        )
    )

    def ok_path():
        ssm.put_parameter(
            Name="awsx/" + uuid.uuid4().hex[:4] + "/aws",
            Value="v",
            Type="String",
        )

    out.append(probe("put non-reserved path containing aws-ok names", ok_path))

    # cleanup
    for n in names.values():
        try:
            ssm.delete_parameter(Name=n)
        except Exception:  # noqa: BLE001
            pass


if __name__ == "__main__":
    main()
