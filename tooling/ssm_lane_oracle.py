"""SSM fix-wave oracle: run the same probes against pinned moto (in-process).

Usage: PYTHONPATH=<moto root> <venv python> tooling/ssm_lane_oracle.py
"""

import json
import uuid

import boto3
import moto
from botocore.exceptions import ClientError


def probe(name, fn):
    try:
        result = fn()
    except ClientError as e:
        result = {"error": e.response["Error"]["Code"], "message": e.response["Error"]["Message"]}
    except Exception as e:  # noqa: BLE001
        import traceback

        result = {"crash": repr(e), "tb": traceback.format_exc()[-1500:]}
    print(f"== {name}\n{json.dumps(result, sort_keys=True, default=str)}")
    return result


def main():
    with moto.mock_aws():
        ssm = boto3.client("ssm", region_name="us-east-1")
        base = "/oracle-" + uuid.uuid4().hex[:6]
        names = {
            "top": f"{base}/top",
            "deep": f"{base}/a/b/c",
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

        def names_of(resp):
            return [p["Name"] for p in resp["Parameters"]]

        probe(
            "describe tag:k1=v1",
            lambda: names_of(
                ssm.describe_parameters(ParameterFilters=[{"Key": "tag:k1", "Values": ["v1"]}])
            ),
        )
        probe(
            "describe tag:k1=nope",
            lambda: names_of(
                ssm.describe_parameters(ParameterFilters=[{"Key": "tag:k1", "Values": ["nope"]}])
            ),
        )
        probe(
            "describe Path=/ OneLevel",
            lambda: names_of(
                ssm.describe_parameters(ParameterFilters=[{"Key": "Path", "Values": ["/"]}])[
                    "Parameters"
                ]
            )[:8],
        )
        probe(
            "describe Path=base Recursive",
            lambda: names_of(
                ssm.describe_parameters(
                    ParameterFilters=[{"Key": "Path", "Option": "Recursive", "Values": [base]}]
                )
            ),
        )
        probe(
            "describe Path=base OneLevel",
            lambda: names_of(
                ssm.describe_parameters(
                    ParameterFilters=[{"Key": "Path", "Values": [base]}]
                )
            ),
        )
        probe(
            "describe KeyId BeginsWith alias/",
            lambda: names_of(
                ssm.describe_parameters(
                    ParameterFilters=[{"Key": "KeyId", "Option": "BeginsWith", "Values": ["alias/"]}]
                )
            ),
        )
        probe(
            "describe KeyId Equals empty-values",
            lambda: names_of(
                ssm.describe_parameters(ParameterFilters=[{"Key": "KeyId", "Values": []}])
            ),
        )
        probe(
            "describe Type Equals empty-values",
            lambda: names_of(
                ssm.describe_parameters(ParameterFilters=[{"Key": "Type", "Values": []}])
            ),
        )
        probe(
            "describe Label",
            lambda: names_of(
                ssm.describe_parameters(ParameterFilters=[{"Key": "Label", "Values": ["lbl1"]}])
            ),
        )
        probe(
            "describe Name no Values",
            lambda: names_of(ssm.describe_parameters(ParameterFilters=[{"Key": "Name"}])),
        )
        probe(
            "describe dup Name",
            lambda: names_of(
                ssm.describe_parameters(
                    ParameterFilters=[
                        {"Key": "Name", "Values": ["a"]},
                        {"Key": "Name", "Values": ["b"]},
                    ]
                )
            ),
        )
        probe(
            "describe Path=/aws,/ssm",
            lambda: names_of(
                ssm.describe_parameters(
                    ParameterFilters=[{"Key": "Path", "Values": ["/aws", "/ssm"]}]
                )
            ),
        )
        probe(
            "describe Path=test",
            lambda: names_of(
                ssm.describe_parameters(ParameterFilters=[{"Key": "Path", "Values": ["test"]}])
            ),
        )
        probe(
            "describe Tier=zzz",
            lambda: names_of(
                ssm.describe_parameters(ParameterFilters=[{"Key": "Tier", "Values": ["zzz"]}])
            ),
        )
        probe(
            "describe Tier=Standard",
            lambda: names_of(
                ssm.describe_parameters(ParameterFilters=[{"Key": "Tier", "Values": ["Standard"]}])
            ),
        )
        probe(
            "describe Name Option=Wobble",
            lambda: names_of(
                ssm.describe_parameters(
                    ParameterFilters=[{"Key": "Name", "Option": "Wobble", "Values": ["x"]}]
                )
            ),
        )
        probe(
            "describe Filters+ParameterFilters",
            lambda: names_of(
                ssm.describe_parameters(
                    Filters=[{"Key": "Name", "Values": ["x"]}],
                    ParameterFilters=[{"Key": "Name", "Values": ["x"]}],
                )
            ),
        )
        probe(
            "describe shape x2",
            lambda: names_of(
                ssm.describe_parameters(
                    ParameterFilters=[
                        {"Key": "Name", "Option": "over 10 chars"},
                        {"Key": "key"},
                    ]
                )
            ),
        )
        probe(
            "describe Name Option Contains",
            lambda: names_of(
                ssm.describe_parameters(
                    ParameterFilters=[{"Key": "Name", "Option": "Contains", "Values": ["top"]}]
                )
            ),
        )
        probe(
            "bypath Name",
            lambda: names_of(
                ssm.get_parameters_by_path(
                    Path=base, ParameterFilters=[{"Key": "Name", "Values": ["x"]}]
                )["Parameters"]
            ),
        )
        probe(
            "bypath Tier",
            lambda: names_of(
                ssm.get_parameters_by_path(
                    Path=base, ParameterFilters=[{"Key": "Tier", "Values": ["Standard"]}]
                )["Parameters"]
            ),
        )
        probe(
            "bypath Path filter",
            lambda: names_of(
                ssm.get_parameters_by_path(
                    Path=base, ParameterFilters=[{"Key": "Path", "Values": ["/x"]}]
                )["Parameters"]
            ),
        )
        probe(
            "put /{base}/aws/x (reserved /aws path)",
            lambda: ssm.put_parameter(Name=f"{base}/aws/x", Value="v", Type="String"),
        )
        probe(
            "put bare aws-prefixed name",
            lambda: ssm.put_parameter(Name="aws-probe" + uuid.uuid4().hex[:4], Value="v", Type="String")[
                "Version"
            ],
        )
        probe(
            "put ssm/path name",
            lambda: ssm.put_parameter(
                Name="ssm-probe/" + uuid.uuid4().hex[:4], Value="v", Type="String"
            )["Version"],
        )
        probe(
            "put /SSM/x path (case-insensitive aws path)",
            lambda: ssm.put_parameter(
                Name="/AWS/" + uuid.uuid4().hex[:4], Value="v", Type="String"
            )["Version"],
        )
        probe(
            "put bare ssm name (no slash)",
            lambda: ssm.put_parameter(Name="ssm" + uuid.uuid4().hex[:4], Value="v", Type="String")[
                "Version"
            ],
        )
        probe(
            "put bare aws name (no slash)",
            lambda: ssm.put_parameter(Name="aws" + uuid.uuid4().hex[:4], Value="v", Type="String")[
                "Version"
            ],
        )
        probe(
            "put non-reserved path containing aws segment",
            lambda: ssm.put_parameter(
                Name="ok-" + uuid.uuid4().hex[:4] + "/aws", Value="v", Type="String"
            )["Version"],
        )


if __name__ == "__main__":
    main()
