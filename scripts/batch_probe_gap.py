#!/usr/bin/env python3
"""Batch-probe all gap operations against a live robotocore server.

Reads the 34 gap services from analyze_localstack.py output, probes every
botocore operation for each service, and classifies each as:

  - working: returned 200 or a "resource not found" style error
  - needs_params: botocore param validation failed (op exists, needs real resources)
  - not_implemented: server returned "not implemented" / "unknown action"
  - 500_error: server returned 500

Usage:
    # Probe all gap services (requires running server on port 4566)
    uv run python scripts/batch_probe_gap.py --all

    # Probe specific services
    uv run python scripts/batch_probe_gap.py --services kms,ec2,logs

    # JSON output
    uv run python scripts/batch_probe_gap.py --all --json

    # Show only working ops
    uv run python scripts/batch_probe_gap.py --all --filter working

    # Summary only
    uv run python scripts/batch_probe_gap.py --all --summary
"""

import argparse
import json
import re
import sys
import time

import boto3
import botocore.config
import botocore.exceptions
import botocore.loaders
import botocore.session

DEFAULT_OP_TIMEOUT = 10  # seconds per operation (connect + read)

# ---------------------------------------------------------------------------
# Known params: enough to get past botocore validation for common operations.
# These do NOT need to reference real resources — the goal is to reach the
# server and see if it returns "not implemented" vs a real response/error.
# ---------------------------------------------------------------------------
KNOWN_PARAMS: dict[str, dict[str, dict]] = {
    "kms": {
        "ListKeys": {},
        "ListAliases": {},
        "CreateKey": {},
        "DescribeKey": {"KeyId": "alias/test"},
        "CreateAlias": {"AliasName": "alias/probe-test", "TargetKeyId": "1234"},
        "DeleteAlias": {"AliasName": "alias/probe-test"},
        "Encrypt": {"KeyId": "alias/test", "Plaintext": b"test"},
        "Decrypt": {"CiphertextBlob": b"test"},
        "GenerateDataKey": {"KeyId": "alias/test", "KeySpec": "AES_256"},
        "GenerateDataKeyWithoutPlaintext": {"KeyId": "alias/test", "KeySpec": "AES_256"},
        "GenerateRandom": {"NumberOfBytes": 16},
        "GetKeyPolicy": {"KeyId": "1234", "PolicyName": "default"},
        "GetKeyRotationStatus": {"KeyId": "1234"},
        "EnableKeyRotation": {"KeyId": "1234"},
        "DisableKeyRotation": {"KeyId": "1234"},
        "EnableKey": {"KeyId": "1234"},
        "DisableKey": {"KeyId": "1234"},
        "ScheduleKeyDeletion": {"KeyId": "1234"},
        "CancelKeyDeletion": {"KeyId": "1234"},
        "CreateGrant": {
            "KeyId": "1234",
            "GranteePrincipal": "arn:aws:iam::123456789012:root",
            "Operations": ["Encrypt"],
        },
        "ListGrants": {"KeyId": "1234"},
        "RetireGrant": {"GrantId": "1234", "KeyId": "1234"},
        "RevokeGrant": {"GrantId": "1234", "KeyId": "1234"},
        "ListRetirableGrants": {"RetiringPrincipal": "arn:aws:iam::123456789012:root"},
        "ReEncrypt": {
            "CiphertextBlob": b"test",
            "DestinationKeyId": "1234",
        },
        "TagResource": {"KeyId": "1234", "Tags": [{"TagKey": "k", "TagValue": "v"}]},
        "UntagResource": {"KeyId": "1234", "TagKeys": ["k"]},
        "ListResourceTags": {"KeyId": "1234"},
        "PutKeyPolicy": {"KeyId": "1234", "PolicyName": "default", "Policy": "{}"},
        "UpdateKeyDescription": {"KeyId": "1234", "Description": "test"},
        "Sign": {
            "KeyId": "1234",
            "Message": b"test",
            "SigningAlgorithm": "RSASSA_PSS_SHA_256",
            "MessageType": "RAW",
        },
        "Verify": {
            "KeyId": "1234",
            "Message": b"test",
            "Signature": b"test",
            "SigningAlgorithm": "RSASSA_PSS_SHA_256",
            "MessageType": "RAW",
        },
        "GetPublicKey": {"KeyId": "1234"},
        "UpdateAlias": {"AliasName": "alias/probe-test", "TargetKeyId": "1234"},
    },
    "cloudformation": {
        "ListStacks": {},
        "ListExports": {},
        "ListImports": {"ExportName": "test"},
        "ListTypes": {},
        "ListStackResources": {"StackName": "test"},
        "DescribeStacks": {},
        "DescribeStackEvents": {"StackName": "test"},
        "DescribeStackResources": {"StackName": "test"},
        "DescribeStackResource": {"StackName": "test", "LogicalResourceId": "test"},
        "GetTemplate": {"StackName": "test"},
        "GetTemplateSummary": {"TemplateBody": "{}"},
        "ListChangeSets": {"StackName": "test"},
        "ValidateTemplate": {"TemplateBody": "{}"},
        "EstimateTemplateCost": {"TemplateBody": "{}"},
        "DescribeAccountLimits": {},
        "ListStackSets": {},
        "DescribeType": {"TypeName": "AWS::S3::Bucket", "Type": "RESOURCE"},
    },
    "ec2": {
        "DescribeInstances": {},
        "DescribeVpcs": {},
        "DescribeSubnets": {},
        "DescribeSecurityGroups": {},
        "DescribeKeyPairs": {},
        "DescribeImages": {},
        "DescribeRegions": {},
        "DescribeAvailabilityZones": {},
        "DescribeAddresses": {},
        "DescribeVolumes": {},
        "DescribeSnapshots": {},
        "DescribeNetworkInterfaces": {},
        "DescribeRouteTables": {},
        "DescribeInternetGateways": {},
        "DescribeNatGateways": {},
        "DescribeVpcEndpoints": {},
        "DescribeVpcPeeringConnections": {},
        "DescribeDhcpOptions": {},
        "DescribeNetworkAcls": {},
        "DescribePlacementGroups": {},
        "DescribeFlowLogs": {},
        "DescribePrefixLists": {},
        "DescribeTags": {},
        "DescribeLaunchTemplates": {},
        "DescribeVpnGateways": {},
        "DescribeCustomerGateways": {},
        "DescribeVpnConnections": {},
        "DescribeTransitGateways": {},
        "DescribeTransitGatewayAttachments": {},
        "DescribeTransitGatewayRouteTables": {},
        "DescribeTransitGatewayVpcAttachments": {},
        "DescribeManagedPrefixLists": {},
        "DescribeSpotInstanceRequests": {},
        "DescribeReservedInstances": {},
        "DescribeCarrierGateways": {},
        "DescribeIpamPools": {},
        "DescribeIpams": {},
        "CreateVpc": {"CidrBlock": "10.0.0.0/16"},
        "CreateSubnet": {"VpcId": "vpc-12345", "CidrBlock": "10.0.1.0/24"},
        "CreateSecurityGroup": {
            "GroupName": "probe-test",
            "Description": "probe",
        },
        "CreateKeyPair": {"KeyName": "probe-test"},
        "AllocateAddress": {"Domain": "vpc"},
        "CreateVolume": {"AvailabilityZone": "us-east-1a", "Size": 1},
        "CreateTags": {"Resources": ["i-12345"], "Tags": [{"Key": "k", "Value": "v"}]},
        "CreateFlowLogs": {
            "ResourceIds": ["vpc-12345"],
            "ResourceType": "VPC",
            "TrafficType": "ALL",
            "LogGroupName": "test",
        },
    },
    "logs": {
        "DescribeLogGroups": {},
        "DescribeLogStreams": {"logGroupName": "test"},
        "CreateLogGroup": {"logGroupName": "probe-test-log-group"},
        "DeleteLogGroup": {"logGroupName": "probe-test-log-group"},
        "CreateLogStream": {"logGroupName": "test", "logStreamName": "probe-stream"},
        "PutLogEvents": {
            "logGroupName": "test",
            "logStreamName": "test",
            "logEvents": [{"timestamp": 1000000, "message": "test"}],
        },
        "GetLogEvents": {"logGroupName": "test", "logStreamName": "test"},
        "FilterLogEvents": {"logGroupName": "test"},
        "PutRetentionPolicy": {"logGroupName": "test", "retentionInDays": 7},
        "DeleteRetentionPolicy": {"logGroupName": "test"},
        "PutSubscriptionFilter": {
            "logGroupName": "test",
            "filterName": "test",
            "filterPattern": "",
            "destinationArn": "arn:aws:lambda:us-east-1:123456789012:function:test",
        },
        "DescribeSubscriptionFilters": {"logGroupName": "test"},
        "DeleteSubscriptionFilter": {"logGroupName": "test", "filterName": "test"},
        "PutMetricFilter": {
            "logGroupName": "test",
            "filterName": "test",
            "filterPattern": "",
            "metricTransformations": [
                {"metricName": "test", "metricNamespace": "test", "metricValue": "1"}
            ],
        },
        "DescribeMetricFilters": {"logGroupName": "test"},
        "DeleteMetricFilter": {"logGroupName": "test", "filterName": "test"},
        "TagResource": {
            "resourceArn": "arn:aws:logs:us-east-1:123456789012:log-group:test",
            "tags": {"k": "v"},
        },
        "UntagResource": {
            "resourceArn": "arn:aws:logs:us-east-1:123456789012:log-group:test",
            "tagKeys": ["k"],
        },
        "ListTagsForResource": {
            "resourceArn": "arn:aws:logs:us-east-1:123456789012:log-group:test",
        },
        "ListTagsLogGroup": {"logGroupName": "test"},
        "TagLogGroup": {"logGroupName": "test", "tags": {"k": "v"}},
        "UntagLogGroup": {"logGroupName": "test", "tags": ["k"]},
        "PutResourcePolicy": {"policyName": "test", "policyDocument": "{}"},
        "DescribeResourcePolicies": {},
        "DeleteResourcePolicy": {"policyName": "test"},
        "DescribeExportTasks": {},
        "PutDestination": {
            "destinationName": "test",
            "targetArn": "arn:aws:kinesis:us-east-1:123456789012:stream/test",
            "roleArn": "arn:aws:iam::123456789012:role/test",
        },
        "DescribeDestinations": {},
        "PutQueryDefinition": {
            "name": "test",
            "queryString": "fields @timestamp",
        },
        "DescribeQueryDefinitions": {},
        "StartQuery": {
            "logGroupName": "test",
            "startTime": 1000000,
            "endTime": 2000000,
            "queryString": "fields @timestamp",
        },
    },
    "ssm": {
        "DescribeParameters": {},
        "GetParameter": {"Name": "test"},
        "GetParameters": {"Names": ["test"]},
        "PutParameter": {"Name": "probe-test-param", "Value": "v", "Type": "String"},
        "DeleteParameter": {"Name": "probe-test-param"},
        "DeleteParameters": {"Names": ["probe-test-param"]},
        "GetParametersByPath": {"Path": "/"},
        "GetParameterHistory": {"Name": "test"},
        "DescribeDocument": {"Name": "test"},
        "ListDocuments": {},
        "ListCommands": {},
        "ListComplianceSummaries": {},
        "ListComplianceItems": {"ResourceIds": ["i-1234"], "ResourceTypes": ["ManagedInstance"]},
        "GetServiceSetting": {"SettingId": "/ssm/test"},
        "AddTagsToResource": {
            "ResourceType": "Parameter",
            "ResourceId": "test",
            "Tags": [{"Key": "k", "Value": "v"}],
        },
        "ListTagsForResource": {"ResourceType": "Parameter", "ResourceId": "test"},
        "RemoveTagsFromResource": {
            "ResourceType": "Parameter",
            "ResourceId": "test",
            "TagKeys": ["k"],
        },
        "LabelParameterVersion": {"Name": "test", "Labels": ["test"]},
    },
    "dynamodb": {
        "ListTables": {},
        "DescribeTable": {"TableName": "test"},
        "DescribeEndpoints": {},
        "DescribeLimits": {},
        "ListGlobalTables": {},
        "ListBackups": {},
        "DescribeContinuousBackups": {"TableName": "test"},
        "DescribeTimeToLive": {"TableName": "test"},
        "ListTagsOfResource": {"ResourceArn": "arn:aws:dynamodb:us-east-1:123456789012:table/test"},
    },
    "lambda": {
        "ListFunctions": {},
        "ListLayers": {},
        "ListEventSourceMappings": {},
        "GetAccountSettings": {},
        "ListCodeSigningConfigs": {},
    },
    "events": {
        "ListEventBuses": {},
        "ListRules": {},
        "DescribeEventBus": {},
        "ListArchives": {},
        "ListReplays": {},
        "ListConnections": {},
        "ListApiDestinations": {},
    },
    "s3": {
        "ListBuckets": {},
    },
    "sns": {
        "ListTopics": {},
        "ListSubscriptions": {},
        "ListPlatformApplications": {},
        "ListSMSSandboxPhoneNumbers": {},
    },
    "sqs": {
        "ListQueues": {},
    },
    "iam": {
        "ListRoles": {},
        "ListUsers": {},
        "ListPolicies": {},
        "ListGroups": {},
        "ListInstanceProfiles": {},
        "ListOpenIDConnectProviders": {},
        "ListSAMLProviders": {},
        "ListServerCertificates": {},
        "ListServiceSpecificCredentials": {},
        "ListSigningCertificates": {},
        "ListMFADevices": {},
        "ListAccountAliases": {},
        "GetAccountSummary": {},
        "GetAccountAuthorizationDetails": {},
    },
    "secretsmanager": {
        "ListSecrets": {},
        "GetRandomPassword": {},
    },
    "route53": {
        "ListHostedZones": {},
        "GetHostedZoneCount": {},
        "ListHealthChecks": {},
        "GetCheckerIpRanges": {},
        "ListReusableDelegationSets": {},
    },
    "route53resolver": {
        "ListResolverEndpoints": {},
        "ListResolverRules": {},
        "ListResolverRuleAssociations": {},
        "ListResolverQueryLogConfigs": {},
        "ListResolverQueryLogConfigAssociations": {},
        "ListFirewallDomainLists": {},
        "ListFirewallRuleGroups": {},
        "ListFirewallRuleGroupAssociations": {},
    },
    "opensearch": {
        "ListDomainNames": {},
        "ListVersions": {},
        "ListTags": {"ARN": "arn:aws:es:us-east-1:123456789012:domain/test"},
        "DescribeDomain": {"DomainName": "test"},
        "DescribeDomainConfig": {"DomainName": "test"},
    },
    "es": {
        "ListDomainNames": {},
        "ListElasticsearchVersions": {},
        "ListTags": {"ARN": "arn:aws:es:us-east-1:123456789012:domain/test"},
        "DescribeElasticsearchDomain": {"DomainName": "test"},
        "DescribeElasticsearchDomainConfig": {"DomainName": "test"},
    },
    "ses": {
        "ListIdentities": {},
        "GetSendQuota": {},
        "GetAccountSendingEnabled": {},
        "ListConfigurationSets": {},
        "ListReceiptRuleSets": {},
        "ListTemplates": {},
        "ListVerifiedEmailAddresses": {},
    },
    "acm": {
        "ListCertificates": {},
        "GetAccountConfiguration": {},
    },
    "cloudwatch": {
        "ListMetrics": {},
        "ListDashboards": {},
        "DescribeAlarms": {},
        "DescribeAlarmsForMetric": {"MetricName": "test", "Namespace": "test"},
        "DescribeAnomalyDetectors": {},
        "DescribeInsightRules": {},
        "ListManagedInsightRules": {
            "ResourceARN": "arn:aws:ec2:us-east-1:123456789012:instance/i-1234"
        },  # noqa: E501
    },
    "stepfunctions": {
        "ListStateMachines": {},
        "ListActivities": {},
    },
    "kinesis": {
        "ListStreams": {},
        "ListStreamConsumers": {"StreamARN": "arn:aws:kinesis:us-east-1:123456789012:stream/test"},
    },
    "firehose": {
        "ListDeliveryStreams": {},
    },
    "transcribe": {
        "ListTranscriptionJobs": {},
        "ListVocabularies": {},
        "ListLanguageModels": {},
        "ListMedicalTranscriptionJobs": {},
        "ListCallAnalyticsJobs": {},
    },
    "redshift": {
        "DescribeClusters": {},
        "DescribeClusterSubnetGroups": {},
        "DescribeClusterParameterGroups": {},
    },
    "swf": {
        "ListDomains": {"registrationStatusFilter": "REGISTERED"},
    },
    "s3control": {
        "ListAccessPoints": {"AccountId": "123456789012"},
        "GetPublicAccessBlock": {"AccountId": "123456789012"},
    },
    "config": {
        "DescribeConfigRules": {},
        "DescribeComplianceByConfigRule": {},
        "DescribeComplianceByResource": {},
        "DescribeConfigurationRecorders": {},
        "DescribeConfigurationRecorderStatus": {},
        "DescribeDeliveryChannels": {},
        "DescribeDeliveryChannelStatus": {},
    },
    "resource-groups": {
        "ListGroups": {},
    },
    "resourcegroupstaggingapi": {
        "GetResources": {},
        "GetTagKeys": {},
        "GetTagValues": {"Key": "test"},
    },
    "support": {
        "DescribeServices": {},
        "DescribeTrustedAdvisorChecks": {"language": "en"},
    },
}

# Operations to skip during probing (destructive or cause permanent side effects)
SKIP_OPERATIONS = {
    "DeleteBucket",
    "DeleteQueue",
    "DeleteTopic",
    "DeleteTable",
    "DeleteFunction",
    "DeleteStack",
    "DeleteApi",
    "DeleteRestApi",
    "PurgeQueue",
    "TerminateInstances",
    "DeleteCluster",
    "DeleteStateMachine",
    "DeleteActivity",
    "DeregisterStreamConsumer",
    "DeleteDeliveryStream",
    "DeleteDomain",
    "DeleteElasticsearchDomain",
}

# GAP_SERVICES: services identified in analyze_localstack.py --robotocore-gap
GAP_SERVICES = [
    "acm",
    "cloudformation",
    "cloudwatch",
    "config",
    "dynamodb",
    "dynamodbstreams",
    "ec2",
    "es",
    "events",
    "firehose",
    "iam",
    "kinesis",
    "kms",
    "lambda",
    "logs",
    "opensearch",
    "redshift",
    "resource-groups",
    "resourcegroupstaggingapi",
    "route53",
    "route53resolver",
    "s3",
    "s3control",
    "scheduler",
    "secretsmanager",
    "ses",
    "sns",
    "sqs",
    "ssm",
    "stepfunctions",
    "sts",
    "support",
    "swf",
    "transcribe",
]


# Registry service name → botocore/boto3 service name
BOTOCORE_NAME_MAP: dict[str, str] = {
    "acmpca": "acm-pca",
    "applicationautoscaling": "application-autoscaling",
    "bedrockagent": "bedrock-agent",
    "cognitoidentity": "cognito-identity",
    "ec2instanceconnect": "ec2-instance-connect",
    "emrcontainers": "emr-containers",
    "emrserverless": "emr-serverless",
    "iotdata": "iot-data",
    "lexv2models": "lexv2-models",
    "networkfirewall": "network-firewall",
    "rdsdata": "rds-data",
    "redshiftdata": "redshift-data",
    "servicecatalogappregistry": "servicecatalog-appregistry",
    "ssoadmin": "sso-admin",
    "timestreaminfluxdb": "timestream-influxdb",
    "timestreamquery": "timestream-query",
    "timestreamwrite": "timestream-write",
    "vpclattice": "vpc-lattice",
    "workspacesweb": "workspaces-web",
}


def _to_botocore_name(service_name: str) -> str:
    """Map registry service name to botocore/boto3 service name."""
    return BOTOCORE_NAME_MAP.get(service_name, service_name)


def _to_snake_case(name: str) -> str:
    """Convert PascalCase to snake_case."""
    s1 = re.sub("(.)([A-Z][a-z]+)", r"\1_\2", name)
    return re.sub("([a-z0-9])([A-Z])", r"\1_\2", s1).lower()


def classify_response(client, operation_name: str, error: Exception | None) -> str:
    """Classify probe result into a status category."""
    if error is None:
        return "working"

    if isinstance(error, botocore.exceptions.ParamValidationError):
        return "needs_params"

    if isinstance(error, botocore.exceptions.ClientError):
        code = error.response["Error"]["Code"]
        msg = error.response["Error"]["Message"]
        status_code = error.response["ResponseMetadata"]["HTTPStatusCode"]

        # "Not implemented" / "Unknown action" = truly missing
        if "not yet implemented" in msg.lower() or "not implemented" in msg.lower():
            return "not_implemented"
        if "unknown action" in msg.lower() or "unknown operation" in msg.lower():
            return "not_implemented"

        # 500 with NotImplementedError traceback = Moto stub
        if status_code == 500:
            if "NotImplementedError" in msg or "notimplemented" in msg.lower():
                return "not_implemented"
            return "500_error"

        # Resource-not-found style errors = operation IS implemented
        implemented_codes = {
            "ResourceNotFoundException",
            "ValidationException",
            "NotFoundException",
            "NoSuchEntity",
            "InvalidParameterValue",
            "MissingParameter",
            "InvalidParameter",
            "MalformedPolicyDocument",
            "NoSuchBucket",
            "QueueDoesNotExist",
            "ResourceNotFoundFault",
            "HostedZoneNotFound",
            "InvalidInput",
            "FunctionNotFound",
            "RepositoryNotFoundException",
            "ParameterNotFound",
            "SecretNotFoundException",
            "AccessDeniedException",
            "InvalidRequestException",
            "InvalidParameterException",
            "ResourceInUseException",
            "ResourceAlreadyExistsException",
            "ConflictException",
            "ThrottlingException",
            "LimitExceededException",
            "ServiceException",
            "InvalidKeyId",
            "InvalidArnException",
            "DisabledException",
            "KMSInvalidStateException",
            "DependencyTimeoutException",
            "OperationNotPermittedException",
            "TagException",
            "InvalidCiphertextException",
            "IncorrectKeyException",
            "KeyUnavailableException",
            "NoSuchHostedZone",
            "InvalidChangeBatch",
            "InvalidDomainName",
            "DomainNotFound",
            "EntityAlreadyExists",
            "EntityDoesNotExist",
            "DeleteConflict",
            "UnrecognizedClientException",
            "SerializationException",
            "StackNotFoundException",
            "ChangeSetNotFoundException",
            "TypeNotFoundException",
        }
        if code in implemented_codes:
            return "working"

        # 4xx errors generally mean the endpoint exists
        if 400 <= status_code < 500:
            return "working"

        return "500_error"

    # Other exceptions
    return "error"


def probe_operation(client, operation_name: str, params: dict) -> tuple[str, str]:
    """Probe a single operation. Returns (status, detail_message).

    Timeout is enforced by the client's botocore Config (connect_timeout /
    read_timeout).  ConnectTimeoutError and ReadTimeoutError are caught here
    and reported as "error" so the probe loop can continue.
    """
    try:
        method = getattr(client, _to_snake_case(operation_name))
        method(**params)
        return "working", "OK"
    except botocore.exceptions.ParamValidationError as e:
        return "needs_params", f"param validation: {str(e)[:80]}"
    except (botocore.exceptions.ConnectTimeoutError, botocore.exceptions.ReadTimeoutError) as e:
        return "error", f"timeout: {type(e).__name__}"
    except botocore.exceptions.EndpointConnectionError:
        return "error", "endpoint unreachable"
    except botocore.exceptions.ClientError as e:
        status = classify_response(client, operation_name, e)
        code = e.response["Error"]["Code"]
        msg = e.response["Error"]["Message"][:80]
        return status, f"{code}: {msg}"
    except Exception as e:
        return "error", f"{type(e).__name__}: {str(e)[:80]}"


def get_all_operations(service_name: str) -> list[str]:
    """Get ALL botocore operations for a service."""
    boto_name = _to_botocore_name(service_name)
    loader = botocore.loaders.Loader()
    try:
        api = loader.load_service_model(boto_name, "service-2")
    except Exception:
        return []
    return sorted(api.get("operations", {}).keys())


def probe_service(
    service_name: str,
    endpoint: str = "http://localhost:4566",
    verbose: bool = False,
    op_timeout: int = DEFAULT_OP_TIMEOUT,
) -> dict:
    """Probe all operations for a service. Returns classification dict.

    Each HTTP call is bounded by op_timeout seconds (connect + read).
    """
    boto_name = _to_botocore_name(service_name)
    client = boto3.client(
        boto_name,
        endpoint_url=endpoint,
        region_name="us-east-1",
        aws_access_key_id="testing",
        aws_secret_access_key="testing",
        config=botocore.config.Config(
            connect_timeout=min(op_timeout, 5),
            read_timeout=op_timeout,
            retries={"max_attempts": 1},  # no retries — this is a probe, not production
        ),
    )

    operations = get_all_operations(service_name)
    known = KNOWN_PARAMS.get(service_name, {})

    results = {}
    for op in operations:
        if op in SKIP_OPERATIONS:
            results[op] = {"status": "skipped", "detail": "destructive operation"}
            continue

        params = known.get(op, {})
        status, detail = probe_operation(client, op, params)
        results[op] = {"status": status, "detail": detail}

        if verbose:
            icon = {
                "working": "+",
                "needs_params": "~",
                "not_implemented": "-",
                "500_error": "!",
                "skipped": "S",
                "error": "?",
            }  # noqa: E501
            print(f"  {icon.get(status, '?')} {op}: {status} ({detail})")

    return results


def summarize(all_results: dict) -> dict:
    """Produce summary statistics."""
    summary = {
        "total_services": len(all_results),
        "total_ops": 0,
        "working": 0,
        "needs_params": 0,
        "not_implemented": 0,
        "500_error": 0,
        "skipped": 0,
        "error": 0,
        "per_service": {},
    }

    for service, ops in all_results.items():
        svc_summary = {
            "working": 0,
            "needs_params": 0,
            "not_implemented": 0,
            "500_error": 0,
            "skipped": 0,
            "error": 0,
        }  # noqa: E501
        for op, info in ops.items():
            status = info["status"]
            summary["total_ops"] += 1
            summary[status] = summary.get(status, 0) + 1
            svc_summary[status] = svc_summary.get(status, 0) + 1
        svc_summary["total"] = len(ops)
        svc_summary["effective_working"] = svc_summary["working"] + svc_summary["needs_params"]
        summary["per_service"][service] = svc_summary

    summary["effective_working"] = summary["working"] + summary["needs_params"]
    return summary


def main():
    parser = argparse.ArgumentParser(description="Batch-probe gap operations")
    parser.add_argument("--all", action="store_true", help="Probe all 34 gap services")
    parser.add_argument(
        "--all-registered",
        action="store_true",
        help="Probe ALL registered services (147)",
    )
    parser.add_argument("--services", help="Comma-separated service names")
    parser.add_argument("--endpoint", default="http://localhost:4566", help="Server endpoint")
    parser.add_argument("--json", action="store_true", help="Output full JSON results")
    parser.add_argument("--summary", action="store_true", help="Show summary only")
    parser.add_argument(
        "--filter",
        choices=["working", "needs_params", "not_implemented", "500_error"],
        help="Show only ops with this status",
    )
    parser.add_argument("--verbose", "-v", action="store_true", help="Show per-op progress")
    parser.add_argument(
        "--op-timeout",
        type=int,
        default=DEFAULT_OP_TIMEOUT,
        metavar="SECS",
        help=f"Per-operation timeout in seconds (default: {DEFAULT_OP_TIMEOUT})",
    )
    args = parser.parse_args()

    if args.all_registered:
        from robotocore.services.registry import get_enabled_services

        services = get_enabled_services()
    elif args.all:
        services = GAP_SERVICES
    elif args.services:
        services = [s.strip() for s in args.services.split(",")]
    else:
        parser.error("Specify --all, --all-registered, or --services")

    all_results = {}
    t0 = time.time()

    for i, service in enumerate(services, 1):
        if not args.json:
            print(
                f"[{i}/{len(services)}] Probing {service}...",
                end="" if not args.verbose else "\n",
                flush=True,
            )  # noqa: E501

        results = probe_service(
            service, args.endpoint, verbose=args.verbose, op_timeout=args.op_timeout
        )
        all_results[service] = results

        # Quick per-service summary
        counts = {}
        for op_info in results.values():
            s = op_info["status"]
            counts[s] = counts.get(s, 0) + 1

        if not args.json and not args.verbose:
            parts = []
            for s in ["working", "needs_params", "not_implemented", "500_error", "skipped"]:
                if s in counts:
                    parts.append(f"{counts[s]} {s}")
            print(f" {', '.join(parts)}")

    elapsed = time.time() - t0

    if args.json:
        output = {
            "results": all_results,
            "summary": summarize(all_results),
            "elapsed_seconds": round(elapsed, 1),
        }  # noqa: E501
        print(json.dumps(output, indent=2))
        return

    # Print summary table
    summary = summarize(all_results)
    print(f"\n{'=' * 90}")
    print(
        f"  Batch Probe Results ({len(services)} services, {summary['total_ops']} operations, {elapsed:.1f}s)"  # noqa: E501
    )
    print(f"{'=' * 90}")
    print(
        f"  {'Service':<25} {'Total':>6} {'Working':>8} {'NeedsPrm':>9} "
        f"{'NotImpl':>8} {'500Err':>7} {'Effect%':>8}"
    )
    print(f"  {'-' * 83}")

    for service in services:
        if service not in summary["per_service"]:
            continue
        s = summary["per_service"][service]
        eff_pct = (s["effective_working"] / max(s["total"] - s["skipped"], 1)) * 100
        print(
            f"  {service:<25} {s['total']:>6} {s['working']:>8} {s['needs_params']:>9} "
            f"{s['not_implemented']:>8} {s['500_error']:>7} {eff_pct:>7.0f}%"
        )

    print(f"  {'-' * 83}")
    total_non_skip = summary["total_ops"] - summary["skipped"]
    eff_pct = (summary["effective_working"] / max(total_non_skip, 1)) * 100
    print(
        f"  {'TOTAL':<25} {summary['total_ops']:>6} {summary['working']:>8} "
        f"{summary['needs_params']:>9} {summary['not_implemented']:>8} "
        f"{summary['500_error']:>7} {eff_pct:>7.0f}%"
    )
    print(f"\n  Effective working (working + needs_params): {summary['effective_working']}")
    print(
        f"  Truly missing (not_implemented + 500_error): {summary['not_implemented'] + summary['500_error']}"  # noqa: E501
    )

    # If filter requested, print matching ops
    if args.filter:
        print(f"\n  Operations with status '{args.filter}':")
        for service in services:
            if service not in all_results:
                continue
            filtered = [
                op for op, info in all_results[service].items() if info["status"] == args.filter
            ]
            if filtered:
                print(f"\n  {service} ({len(filtered)}):")
                for op in filtered:
                    detail = all_results[service][op]["detail"]
                    print(f"    {op}: {detail}")


if __name__ == "__main__":
    sys.exit(main() or 0)
