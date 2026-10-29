#!/usr/bin/env python3
"""tflocal — Terraform CLI wrapper that auto-configures Terraform to use robotocore endpoints.

Usage:
    tflocal plan
    tflocal apply -auto-approve
    tflocal destroy

Environment variables:
    ROBOTOCORE_ENDPOINT  — Full endpoint URL (default: http://localhost:4566)
    ROBOTOCORE_HOST      — Host only (default: localhost), ignored if ROBOTOCORE_ENDPOINT set
    ROBOTOCORE_PORT      — Port only (default: 4566), ignored if ROBOTOCORE_ENDPOINT set
    TF_COMPAT_MODE       — Set to "1" for additional compatibility fixes
"""

from __future__ import annotations

import os
import shutil
import subprocess
import sys

# All AWS services supported by the Terraform AWS provider.
# Each key is the Terraform provider endpoint name, mapping to the robotocore endpoint.
TERRAFORM_AWS_SERVICES: list[str] = [
    "accessanalyzer",
    "account",
    "acm",
    "acmpca",
    "amg",
    "amp",
    "amplify",
    "apigateway",
    "apigatewayv2",
    "appautoscaling",
    "appconfig",
    "appfabric",
    "appflow",
    "appintegrations",
    "applicationinsights",
    "appmesh",
    "apprunner",
    "appstream",
    "appsync",
    "athena",
    "auditmanager",
    "autoscaling",
    "autoscalingplans",
    "backup",
    "batch",
    "bcmdataexports",
    "bedrock",
    "bedrockagent",
    "budgets",
    "ce",
    "chatbot",
    "chime",
    "chimesdkmediapipelines",
    "chimesdkvoice",
    "cleanrooms",
    "cloud9",
    "cloudcontrol",
    "cloudformation",
    "cloudfront",
    "cloudfrontkeyvaluestore",
    "cloudhsmv2",
    "cloudsearch",
    "cloudtrail",
    "cloudwatch",
    "codeartifact",
    "codebuild",
    "codecatalyst",
    "codecommit",
    "codeconnections",
    "codedeploy",
    "codeguruprofiler",
    "codegurureviewer",
    "codepipeline",
    "codestarconnections",
    "codestarnotifications",
    "cognitoidentity",
    "cognitoidp",
    "comprehend",
    "computeoptimizer",
    "configservice",
    "connect",
    "connectcases",
    "controltower",
    "costoptimizationhub",
    "cur",
    "customerprofiles",
    "dataexchange",
    "datapipeline",
    "datasync",
    "datazone",
    "dax",
    "deploy",
    "detective",
    "devicefarm",
    "devopsguru",
    "directconnect",
    "dlm",
    "dms",
    "docdb",
    "docdbelastic",
    "drs",
    "dynamodb",
    "ec2",
    "ecr",
    "ecrpublic",
    "ecs",
    "efs",
    "eks",
    "elasticache",
    "elasticbeanstalk",
    "elastictranscoder",
    "elb",
    "elbv2",
    "emr",
    "emrcontainers",
    "emrserverless",
    "es",
    "eventbridge",
    "evidently",
    "finspace",
    "firehose",
    "fis",
    "fms",
    "fsx",
    "gamelift",
    "glacier",
    "globalaccelerator",
    "glue",
    "grafana",
    "greengrass",
    "groundstation",
    "guardduty",
    "healthlake",
    "iam",
    "identitystore",
    "imagebuilder",
    "inspector",
    "inspector2",
    "internetmonitor",
    "iot",
    "iotanalytics",
    "iotevents",
    "ivs",
    "ivschat",
    "kafka",
    "kafkaconnect",
    "kendra",
    "keyspaces",
    "kinesis",
    "kinesisanalytics",
    "kinesisanalyticsv2",
    "kinesisvideo",
    "kms",
    "lakeformation",
    "lambda",
    "launchwizard",
    "lex",
    "lexv2models",
    "licensemanager",
    "lightsail",
    "location",
    "logs",
    "lookoutmetrics",
    "m2",
    "macie2",
    "mediaconnect",
    "mediaconvert",
    "medialive",
    "mediapackage",
    "mediapackagev2",
    "mediastore",
    "memorydb",
    "mq",
    "mwaa",
    "neptune",
    "neptunegraph",
    "networkfirewall",
    "networkmanager",
    "networkmonitor",
    "oam",
    "opensearch",
    "opensearchserverless",
    "opsworks",
    "organizations",
    "osis",
    "outposts",
    "paymentcryptography",
    "pcaconnectorad",
    "pcs",
    "pinpoint",
    "pinpointsmsvoicev2",
    "pipes",
    "polly",
    "pricing",
    "prometheus",
    "qbusiness",
    "qldb",
    "quicksight",
    "ram",
    "rbin",
    "rds",
    "redshift",
    "redshiftdata",
    "redshiftserverless",
    "rekognition",
    "resiliencehub",
    "resourceexplorer2",
    "resourcegroups",
    "resourcegroupstaggingapi",
    "rolesanywhere",
    "route53",
    "route53domains",
    "route53profiles",
    "route53recoverycontrolconfig",
    "route53recoveryreadiness",
    "route53resolver",
    "rum",
    "s3",
    "s3control",
    "s3outposts",
    "s3tables",
    "sagemaker",
    "scheduler",
    "schemas",
    "secretsmanager",
    "securityhub",
    "securitylake",
    "serverlessrepo",
    "servicecatalog",
    "servicecatalogappregistry",
    "servicediscovery",
    "servicequotas",
    "ses",
    "sesv2",
    "sfn",
    "shield",
    "signer",
    "simpledb",
    "sns",
    "sqs",
    "ssm",
    "ssmcontacts",
    "ssmincidents",
    "ssmquicksetup",
    "ssmsap",
    "sso",
    "ssoadmin",
    "storagegateway",
    "sts",
    "swf",
    "synthetics",
    "timestreaminfluxdb",
    "timestreamwrite",
    "transcribe",
    "transfer",
    "verifiedpermissions",
    "vpclattice",
    "waf",
    "wafregional",
    "wafv2",
    "wellarchitected",
    "worklink",
    "workspaces",
    "workspacesweb",
    "xray",
]

# Terraform subcommands that need the provider override file
COMMANDS_NEEDING_OVERRIDE: set[str] = {
    "init",
    "plan",
    "apply",
    "destroy",
    "import",
    "refresh",
    "taint",
    "untaint",
    "state",
    "output",
    "show",
    "providers",
    "console",
    "graph",
    "test",
}

OVERRIDE_FILENAME = "localstack_providers_override.tf.json"


def get_endpoint_url(env: dict[str, str] | None = None) -> str:
    """Build the robotocore endpoint URL from environment variables."""
    if env is None:
        env = os.environ
    endpoint = env.get("ROBOTOCORE_ENDPOINT")
    if endpoint:
        return endpoint.rstrip("/")
    host = env.get("ROBOTOCORE_HOST", "localhost")
    port = env.get("ROBOTOCORE_PORT", "4566")
    return f"http://{host}:{port}"


def get_aws_env(env: dict[str, str] | None = None) -> dict[str, str]:
    """Return AWS credential env vars, only setting defaults for unset vars."""
    if env is None:
        env = os.environ
    result: dict[str, str] = {}
    if "AWS_ACCESS_KEY_ID" not in env:
        result["AWS_ACCESS_KEY_ID"] = "test"
    if "AWS_SECRET_ACCESS_KEY" not in env:
        result["AWS_SECRET_ACCESS_KEY"] = "test"
    if "AWS_DEFAULT_REGION" not in env:
        result["AWS_DEFAULT_REGION"] = "us-east-1"
    return result


def build_provider_override(endpoint_url: str, compat_mode: bool = False) -> dict:
    """Build the Terraform provider override configuration as a dict.

    Returns a dict suitable for JSON serialization as a .tf.json override file.
    """
    endpoints = {svc: endpoint_url for svc in TERRAFORM_AWS_SERVICES}

    provider_config: dict = {
        "skip_credentials_validation": True,
        "skip_metadata_api_check": True,
        "skip_requesting_account_id": True,
        "access_key": "test",
        "secret_key": "test",
        "region": "us-east-1",
        "endpoints": endpoints,
    }

    if compat_mode:
        provider_config["s3_use_path_style"] = True

    return {"provider": {"aws": provider_config}}


def should_generate_override(args: list[str]) -> bool:
    """Determine if the given terraform arguments need a provider override file.

    Returns False for commands like 'fmt', 'validate', 'version', etc.
    """
    for arg in args:
        if not arg.startswith("-"):
            return arg in COMMANDS_NEEDING_OVERRIDE
    # No subcommand found — don't generate
    return False


def write_override_file(working_dir: str, endpoint_url: str, compat_mode: bool = False) -> str:
    """Write the provider override file and return its path."""
    import json

    override = build_provider_override(endpoint_url, compat_mode=compat_mode)
    path = os.path.join(working_dir, OVERRIDE_FILENAME)
    with open(path, "w") as f:
        json.dump(override, f, indent=2)
    return path


def cleanup_override_file(working_dir: str) -> None:
    """Remove the provider override file if it exists."""
    path = os.path.join(working_dir, OVERRIDE_FILENAME)
    try:
        os.unlink(path)
    except FileNotFoundError:
        pass


def find_terraform() -> str | None:
    """Find the terraform binary on PATH."""
    return shutil.which("terraform")


def run_terraform(
    args: list[str],
    env: dict[str, str] | None = None,
    working_dir: str | None = None,
) -> int:
    """Run terraform with the given args.

    Handles:
    - Setting AWS credentials if not already set
    - Generating provider override file for relevant commands
    - Cleaning up override file after terraform exits

    Returns the terraform exit code.
    """
    if env is None:
        env = dict(os.environ)
    if working_dir is None:
        working_dir = os.getcwd()

    # Find terraform
    tf_bin = find_terraform()
    if tf_bin is None:
        print(
            "Error: terraform not found on PATH.\n"
            "Install Terraform: https://developer.hashicorp.com/terraform/install",
            file=sys.stderr,
        )
        return 127

    # Build endpoint URL
    endpoint_url = get_endpoint_url(env)

    # Set AWS env vars (only defaults for unset vars)
    aws_env = get_aws_env(env)
    merged_env = {**env, **aws_env}
    merged_env["AWS_ENDPOINT_URL"] = endpoint_url

    # Determine if we need the override file
    needs_override = should_generate_override(args)
    compat_mode = env.get("TF_COMPAT_MODE") == "1"

    override_path = None
    try:
        if needs_override:
            override_path = write_override_file(working_dir, endpoint_url, compat_mode=compat_mode)

        result = subprocess.run(
            [tf_bin, *args],
            env=merged_env,
            cwd=working_dir,
        )
        return result.returncode
    finally:
        if override_path:
            cleanup_override_file(working_dir)


def main() -> None:
    """CLI entrypoint."""
    sys.exit(run_terraform(sys.argv[1:]))


if __name__ == "__main__":
    main()
