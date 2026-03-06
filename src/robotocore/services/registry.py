"""Service registry tracking which AWS services are enabled and their status."""

from dataclasses import dataclass
from enum import Enum


class ServiceStatus(Enum):
    MOTO_BACKED = "moto_backed"
    NATIVE = "native"
    EXTERNAL = "external"
    PLANNED = "planned"


@dataclass
class ServiceInfo:
    name: str
    status: ServiceStatus
    protocol: str
    description: str = ""


# All services with their implementation status
SERVICE_REGISTRY: dict[str, ServiceInfo] = {
    # Phase 1 - Core (Moto-backed)
    "s3": ServiceInfo("s3", ServiceStatus.NATIVE, "rest-xml", "S3 with event notifications"),
    "sqs": ServiceInfo("sqs", ServiceStatus.NATIVE, "json", "SQS with full message lifecycle"),
    "sns": ServiceInfo("sns", ServiceStatus.NATIVE, "query", "SNS with cross-service delivery"),
    "dynamodb": ServiceInfo("dynamodb", ServiceStatus.MOTO_BACKED, "json", "DynamoDB"),
    "iam": ServiceInfo("iam", ServiceStatus.MOTO_BACKED, "query", "Identity and Access Management"),
    "sts": ServiceInfo("sts", ServiceStatus.MOTO_BACKED, "query", "Security Token Service"),
    "cloudformation": ServiceInfo("cloudformation", ServiceStatus.NATIVE, "query", "CloudFormation engine"),
    "cloudwatch": ServiceInfo("cloudwatch", ServiceStatus.MOTO_BACKED, "query", "CloudWatch Metrics"),
    "logs": ServiceInfo("logs", ServiceStatus.MOTO_BACKED, "json", "CloudWatch Logs"),
    "kms": ServiceInfo("kms", ServiceStatus.MOTO_BACKED, "json", "Key Management Service"),
    "lambda": ServiceInfo("lambda", ServiceStatus.NATIVE, "rest-json", "Lambda with in-process Python execution"),
    "events": ServiceInfo("events", ServiceStatus.NATIVE, "json", "EventBridge with cross-service target invocation"),
    "kinesis": ServiceInfo("kinesis", ServiceStatus.MOTO_BACKED, "json", "Kinesis Streams"),
    "firehose": ServiceInfo("firehose", ServiceStatus.NATIVE, "json", "Firehose with S3 delivery"),
    "stepfunctions": ServiceInfo("stepfunctions", ServiceStatus.MOTO_BACKED, "json", "Step Functions"),
    # Phase 2 - Integration (Moto-backed)
    "apigateway": ServiceInfo("apigateway", ServiceStatus.MOTO_BACKED, "rest-json", "API Gateway"),
    "secretsmanager": ServiceInfo("secretsmanager", ServiceStatus.MOTO_BACKED, "json", "Secrets Manager"),
    "ssm": ServiceInfo("ssm", ServiceStatus.MOTO_BACKED, "json", "Systems Manager"),
    "scheduler": ServiceInfo("scheduler", ServiceStatus.MOTO_BACKED, "rest-json", "EventBridge Scheduler"),
    "s3control": ServiceInfo("s3control", ServiceStatus.MOTO_BACKED, "rest-xml", "S3 Control"),
    # Phase 3 - Remaining (Moto-backed)
    "acm": ServiceInfo("acm", ServiceStatus.MOTO_BACKED, "json", "Certificate Manager"),
    "config": ServiceInfo("config", ServiceStatus.MOTO_BACKED, "json", "Config"),
    "ec2": ServiceInfo("ec2", ServiceStatus.MOTO_BACKED, "ec2", "Elastic Compute Cloud"),
    "redshift": ServiceInfo("redshift", ServiceStatus.MOTO_BACKED, "query", "Redshift"),
    "resource-groups": ServiceInfo("resource-groups", ServiceStatus.MOTO_BACKED, "rest-json", "Resource Groups"),
    "resourcegroupstaggingapi": ServiceInfo("resourcegroupstaggingapi", ServiceStatus.MOTO_BACKED, "json", "Resource Groups Tagging API"),
    "route53": ServiceInfo("route53", ServiceStatus.MOTO_BACKED, "rest-xml", "Route 53"),
    "route53resolver": ServiceInfo("route53resolver", ServiceStatus.MOTO_BACKED, "json", "Route 53 Resolver"),
    "ses": ServiceInfo("ses", ServiceStatus.MOTO_BACKED, "query", "Simple Email Service"),
    "support": ServiceInfo("support", ServiceStatus.MOTO_BACKED, "json", "Support"),
    "swf": ServiceInfo("swf", ServiceStatus.MOTO_BACKED, "json", "Simple Workflow"),
    "transcribe": ServiceInfo("transcribe", ServiceStatus.MOTO_BACKED, "json", "Transcribe"),
}


def get_enabled_services() -> list[str]:
    """Return names of all enabled services."""
    return sorted(SERVICE_REGISTRY.keys())


def is_service_enabled(service_name: str) -> bool:
    return service_name in SERVICE_REGISTRY
