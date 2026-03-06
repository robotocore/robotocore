#!/usr/bin/env python3
"""Generate CloudFormation resource handler boilerplate from CloudFormation resource specs.

Maps CloudFormation resource types to Moto backend calls. Input: resource type
(e.g., AWS::SQS::Queue), output: create/update/delete handler functions with
property-to-API mapping.

Usage:
    uv run python scripts/gen_cfn_resource.py AWS::SQS::Queue
    uv run python scripts/gen_cfn_resource.py --list                  # List all supported types
    uv run python scripts/gen_cfn_resource.py --batch resources.txt   # Generate from file
    uv run python scripts/gen_cfn_resource.py AWS::EC2::Instance --write  # Write to resources.py
    uv run python scripts/gen_cfn_resource.py --dry-run               # Preview all changes

Resource type -> Moto backend mapping:
    AWS::SQS::Queue     -> moto.sqs.models.SQSBackend
    AWS::IAM::Role      -> moto.iam.models.IAMBackend
    etc.
"""

import argparse
import re
import sys
from pathlib import Path

# CloudFormation resource type -> (moto_service, is_global, create_method, delete_method)
RESOURCE_SPECS: dict[str, dict] = {
    # --- IAM ---
    "AWS::IAM::Role": {
        "service": "iam",
        "global": True,
        "properties": {
            "RoleName": {"required": False, "auto_name": True},
            "AssumeRolePolicyDocument": {"required": True, "json_encode": True},
            "Path": {"required": False, "default": "/"},
            "Description": {"required": False, "default": ""},
            "MaxSessionDuration": {"required": False},
            "PermissionsBoundary": {"required": False},
            "Tags": {"required": False, "default": "[]"},
            "ManagedPolicyArns": {"required": False, "post_create": True},
            "Policies": {"required": False, "post_create": True},
        },
        "create": {
            "method": "create_role",
            "args": [
                ("RoleName", "name"),
                ("AssumeRolePolicyDocument", "assume_role_policy_document"),
                ("Path", "path"),
            ],
            "kwargs": {
                "permissions_boundary": "PermissionsBoundary",
                "description": "Description",
                "tags": "Tags",
                "max_session_duration": "MaxSessionDuration",
            },
            "physical_id": "name",
            "attributes": {"Arn": "role.arn", "RoleId": "role.id"},
        },
        "delete": {"method": "delete_role", "args": ["physical_id"]},
    },
    "AWS::IAM::ManagedPolicy": {
        "service": "iam",
        "global": True,
        "properties": {
            "ManagedPolicyName": {"required": False, "auto_name": True},
            "PolicyDocument": {"required": True, "json_encode": True},
            "Path": {"required": False, "default": "/"},
            "Description": {"required": False, "default": ""},
            "Groups": {"required": False, "post_create": True},
            "Roles": {"required": False, "post_create": True},
            "Users": {"required": False, "post_create": True},
        },
        "create": {
            "method": "create_policy",
            "args": [
                ("Description", "description"),
                ("Path", "path"),
                ("PolicyDocument", "policy_document"),
                ("ManagedPolicyName", "policy_name"),
            ],
            "kwargs": {},
            "physical_id": "policy.arn",
            "attributes": {"Arn": "policy.arn"},
        },
        "delete": {"method": "delete_policy", "args": ["physical_id"]},
    },
    "AWS::IAM::InstanceProfile": {
        "service": "iam",
        "global": True,
        "properties": {
            "InstanceProfileName": {"required": False, "auto_name": True},
            "Path": {"required": False, "default": "/"},
            "Roles": {"required": False, "post_create": True},
        },
        "create": {
            "method": "create_instance_profile",
            "args": [("InstanceProfileName", "name"), ("Path", "path")],
            "kwargs": {"tags": "[]"},
            "physical_id": "name",
            "attributes": {"Arn": "profile.arn"},
        },
        "delete": {"method": "delete_instance_profile", "args": ["physical_id"]},
    },
    "AWS::IAM::User": {
        "service": "iam",
        "global": True,
        "properties": {
            "UserName": {"required": False, "auto_name": True},
            "Path": {"required": False, "default": "/"},
            "Tags": {"required": False, "default": "[]"},
            "ManagedPolicyArns": {"required": False, "post_create": True},
            "Policies": {"required": False, "post_create": True},
            "Groups": {"required": False, "post_create": True},
        },
        "create": {
            "method": "create_user",
            "args": [("UserName", "user_name"), ("Path", "path")],
            "kwargs": {"tags": "Tags"},
            "physical_id": "name",
            "attributes": {"Arn": "user.arn"},
        },
        "delete": {"method": "delete_user", "args": ["physical_id"]},
    },
    "AWS::IAM::Group": {
        "service": "iam",
        "global": True,
        "properties": {
            "GroupName": {"required": False, "auto_name": True},
            "Path": {"required": False, "default": "/"},
            "ManagedPolicyArns": {"required": False, "post_create": True},
            "Policies": {"required": False, "post_create": True},
        },
        "create": {
            "method": "create_group",
            "args": [("GroupName", "group_name"), ("Path", "path")],
            "kwargs": {},
            "physical_id": "name",
            "attributes": {"Arn": "group.arn"},
        },
        "delete": {"method": "delete_group", "args": ["physical_id"]},
    },
    "AWS::IAM::AccessKey": {
        "service": "iam",
        "global": True,
        "properties": {
            "UserName": {"required": True},
            "Status": {"required": False, "default": "Active"},
        },
        "create": {
            "method": "create_access_key",
            "args": [("UserName", "user_name")],
            "kwargs": {},
            "physical_id": "key.access_key_id",
            "attributes": {"SecretAccessKey": "key.secret_access_key"},
        },
        "delete": {
            "method": "delete_access_key",
            "args": ["_props.UserName", "physical_id"],
        },
    },
    # --- Lambda ---
    "AWS::Lambda::Function": {
        "service": "lambda",
        "global": False,
        "properties": {
            "FunctionName": {"required": False, "auto_name": True},
            "Runtime": {"required": False, "default": "python3.12"},
            "Role": {"required": True},
            "Handler": {"required": False, "default": "index.handler"},
            "Code": {"required": True},
            "Description": {"required": False, "default": ""},
            "Timeout": {"required": False, "default": 3},
            "MemorySize": {"required": False, "default": 128},
            "Environment": {"required": False},
            "Layers": {"required": False},
            "Tags": {"required": False},
        },
        "create": {"method": "create_function", "args": ["spec_dict"]},
        "delete": {"method": "delete_function", "args": ["function_name"]},
    },
    "AWS::Lambda::Version": {
        "service": "lambda",
        "global": False,
        "properties": {
            "FunctionName": {"required": True},
            "Description": {"required": False, "default": ""},
        },
        "create": {
            "method": "publish_version_with_name",
            "args": [("FunctionName", "function_name")],
            "kwargs": {"description": "Description"},
        },
        "delete": {"method": "noop"},
    },
    "AWS::Lambda::Alias": {
        "service": "lambda",
        "global": False,
        "properties": {
            "FunctionName": {"required": True},
            "FunctionVersion": {"required": True},
            "Name": {"required": True},
            "Description": {"required": False, "default": ""},
        },
        "create": {"method": "create_alias"},
        "delete": {"method": "delete_alias"},
    },
    "AWS::Lambda::EventSourceMapping": {
        "service": "lambda",
        "global": False,
        "properties": {
            "FunctionName": {"required": True},
            "EventSourceArn": {"required": True},
            "BatchSize": {"required": False, "default": 100},
            "Enabled": {"required": False, "default": True},
            "StartingPosition": {"required": False},
        },
        "create": {"method": "create_event_source_mapping"},
        "delete": {"method": "delete_event_source_mapping"},
    },
    "AWS::Lambda::Permission": {
        "service": "lambda",
        "global": False,
        "properties": {
            "Action": {"required": True},
            "FunctionName": {"required": True},
            "Principal": {"required": True},
            "SourceArn": {"required": False},
            "SourceAccount": {"required": False},
        },
        "create": {"method": "add_permission"},
        "delete": {"method": "remove_permission"},
    },
    "AWS::Lambda::LayerVersion": {
        "service": "lambda",
        "global": False,
        "properties": {
            "LayerName": {"required": True},
            "Content": {"required": True},
            "CompatibleRuntimes": {"required": False},
            "Description": {"required": False, "default": ""},
        },
        "create": {"method": "publish_layer_version"},
        "delete": {"method": "delete_layer_version"},
    },
    # --- EC2 ---
    "AWS::EC2::VPC": {
        "service": "ec2",
        "global": False,
        "properties": {
            "CidrBlock": {"required": True},
            "EnableDnsSupport": {"required": False, "default": True},
            "EnableDnsHostnames": {"required": False, "default": False},
            "Tags": {"required": False, "default": "[]"},
        },
        "create": {
            "method": "create_vpc",
            "args": [("CidrBlock", "cidr_block")],
            "kwargs": {},
            "physical_id": "vpc.id",
            "attributes": {"VpcId": "vpc.id", "CidrBlock": "vpc.cidr_block"},
        },
        "delete": {"method": "delete_vpc", "args": ["physical_id"]},
    },
    "AWS::EC2::Subnet": {
        "service": "ec2",
        "global": False,
        "properties": {
            "VpcId": {"required": True},
            "CidrBlock": {"required": True},
            "AvailabilityZone": {"required": False},
            "Tags": {"required": False, "default": "[]"},
        },
        "create": {
            "method": "create_subnet",
            "args": [("VpcId", "vpc_id"), ("CidrBlock", "cidr_block")],
            "kwargs": {"availability_zone": "AvailabilityZone"},
            "physical_id": "subnet.id",
            "attributes": {
                "SubnetId": "subnet.id",
                "AvailabilityZone": "subnet.availability_zone",
            },
        },
        "delete": {"method": "delete_subnet", "args": ["physical_id"]},
    },
    "AWS::EC2::SecurityGroup": {
        "service": "ec2",
        "global": False,
        "properties": {
            "GroupDescription": {"required": True},
            "GroupName": {"required": False, "auto_name": True},
            "VpcId": {"required": False},
            "SecurityGroupIngress": {"required": False, "post_create": True},
            "SecurityGroupEgress": {"required": False, "post_create": True},
            "Tags": {"required": False, "default": "[]"},
        },
        "create": {
            "method": "create_security_group",
            "args": [
                ("GroupName", "name"),
                ("GroupDescription", "description"),
            ],
            "kwargs": {"vpc_id": "VpcId"},
            "physical_id": "group.id",
            "attributes": {"GroupId": "group.id", "VpcId": "group.vpc_id"},
        },
        "delete": {"method": "delete_security_group", "args": ["physical_id"]},
    },
    "AWS::EC2::InternetGateway": {
        "service": "ec2",
        "global": False,
        "properties": {"Tags": {"required": False, "default": "[]"}},
        "create": {
            "method": "create_internet_gateway",
            "args": [],
            "kwargs": {},
            "physical_id": "igw.id",
            "attributes": {"InternetGatewayId": "igw.id"},
        },
        "delete": {"method": "delete_internet_gateway", "args": ["physical_id"]},
    },
    "AWS::EC2::RouteTable": {
        "service": "ec2",
        "global": False,
        "properties": {
            "VpcId": {"required": True},
            "Tags": {"required": False, "default": "[]"},
        },
        "create": {
            "method": "create_route_table",
            "args": [("VpcId", "vpc_id")],
            "kwargs": {},
            "physical_id": "rt.id",
            "attributes": {"RouteTableId": "rt.id"},
        },
        "delete": {"method": "delete_route_table", "args": ["physical_id"]},
    },
    "AWS::EC2::Route": {
        "service": "ec2",
        "global": False,
        "properties": {
            "RouteTableId": {"required": True},
            "DestinationCidrBlock": {"required": False},
            "GatewayId": {"required": False},
            "NatGatewayId": {"required": False},
            "InstanceId": {"required": False},
        },
        "create": {"method": "create_route"},
        "delete": {"method": "delete_route"},
    },
    "AWS::EC2::Instance": {
        "service": "ec2",
        "global": False,
        "properties": {
            "ImageId": {"required": True},
            "InstanceType": {"required": False, "default": "t2.micro"},
            "KeyName": {"required": False},
            "SecurityGroupIds": {"required": False},
            "SubnetId": {"required": False},
            "Tags": {"required": False, "default": "[]"},
            "UserData": {"required": False},
        },
        "create": {"method": "run_instances"},
        "delete": {"method": "terminate_instances"},
    },
    "AWS::EC2::EIP": {
        "service": "ec2",
        "global": False,
        "properties": {
            "Domain": {"required": False, "default": "vpc"},
            "Tags": {"required": False, "default": "[]"},
        },
        "create": {
            "method": "allocate_address",
            "args": [],
            "kwargs": {"domain": "Domain"},
            "physical_id": "eip.allocation_id",
            "attributes": {"AllocationId": "eip.allocation_id", "PublicIp": "eip.public_ip"},
        },
        "delete": {"method": "release_address", "args": ["physical_id"]},
    },
    "AWS::EC2::LaunchTemplate": {
        "service": "ec2",
        "global": False,
        "properties": {
            "LaunchTemplateName": {"required": False, "auto_name": True},
            "LaunchTemplateData": {"required": True},
            "Tags": {"required": False, "default": "[]"},
        },
        "create": {"method": "create_launch_template"},
        "delete": {"method": "delete_launch_template"},
    },
    "AWS::EC2::KeyPair": {
        "service": "ec2",
        "global": False,
        "properties": {
            "KeyName": {"required": True},
            "KeyType": {"required": False, "default": "rsa"},
            "Tags": {"required": False, "default": "[]"},
        },
        "create": {
            "method": "create_key_pair",
            "args": [("KeyName", "key_name")],
            "kwargs": {},
            "physical_id": "name",
            "attributes": {"KeyFingerprint": "key.key_fingerprint"},
        },
        "delete": {"method": "delete_key_pair", "args": ["physical_id"]},
    },
    # --- S3 ---
    "AWS::S3::Bucket": {
        "service": "s3",
        "global": True,
        "properties": {
            "BucketName": {"required": False, "auto_name": True},
            "VersioningConfiguration": {"required": False, "post_create": True},
            "CorsConfiguration": {"required": False, "post_create": True},
            "NotificationConfiguration": {"required": False, "post_create": True},
            "Tags": {"required": False, "default": "[]"},
        },
        "create": {
            "method": "create_bucket",
            "args": [("BucketName", "bucket_name"), ("_region", "region")],
            "kwargs": {},
            "physical_id": "name",
            "attributes": {
                "Arn": "f'arn:aws:s3:::{name}'",
                "DomainName": "f'{name}.s3.amazonaws.com'",
                "RegionalDomainName": "f'{name}.s3.{region}.amazonaws.com'",
            },
        },
        "delete": {"method": "delete_bucket", "args": ["physical_id"]},
    },
    "AWS::S3::BucketPolicy": {
        "service": "s3",
        "global": True,
        "properties": {
            "Bucket": {"required": True},
            "PolicyDocument": {"required": True, "json_encode": True},
        },
        "create": {"method": "put_bucket_policy"},
        "delete": {"method": "delete_bucket_policy"},
    },
    # --- DynamoDB ---
    "AWS::DynamoDB::Table": {
        "service": "dynamodb",
        "global": False,
        "properties": {
            "TableName": {"required": False, "auto_name": True},
            "KeySchema": {"required": True},
            "AttributeDefinitions": {"required": True},
            "BillingMode": {"required": False, "default": "PROVISIONED"},
            "ProvisionedThroughput": {"required": False, "default": "{}"},
            "GlobalSecondaryIndexes": {"required": False},
            "LocalSecondaryIndexes": {"required": False},
            "StreamSpecification": {"required": False},
            "Tags": {"required": False, "default": "[]"},
        },
        "create": {"method": "create_table"},
        "delete": {"method": "delete_table", "args": ["physical_id"]},
    },
    # --- SQS ---
    "AWS::SQS::Queue": {
        "service": "sqs",
        "global": False,
        "properties": {
            "QueueName": {"required": False, "auto_name": True},
            "VisibilityTimeout": {"required": False},
            "DelaySeconds": {"required": False},
            "FifoQueue": {"required": False},
            "RedrivePolicy": {"required": False, "json_encode": True},
            "Tags": {"required": False, "default": "{}"},
        },
        "create": {"method": "native"},
        "delete": {"method": "native"},
    },
    "AWS::SQS::QueuePolicy": {
        "service": "sqs",
        "global": False,
        "properties": {
            "Queues": {"required": True},
            "PolicyDocument": {"required": True, "json_encode": True},
        },
        "create": {"method": "native"},
        "delete": {"method": "noop"},
    },
    # --- SNS ---
    "AWS::SNS::Topic": {
        "service": "sns",
        "global": False,
        "properties": {
            "TopicName": {"required": False, "auto_name": True},
            "DisplayName": {"required": False},
            "FifoTopic": {"required": False},
            "Tags": {"required": False, "default": "[]"},
        },
        "create": {"method": "native"},
        "delete": {"method": "native"},
    },
    "AWS::SNS::Subscription": {
        "service": "sns",
        "global": False,
        "properties": {
            "TopicArn": {"required": True},
            "Protocol": {"required": True},
            "Endpoint": {"required": True},
            "FilterPolicy": {"required": False},
        },
        "create": {"method": "native"},
        "delete": {"method": "native"},
    },
    # --- API Gateway ---
    "AWS::ApiGateway::RestApi": {
        "service": "apigateway",
        "global": False,
        "properties": {
            "Name": {"required": True},
            "Description": {"required": False, "default": ""},
            "EndpointConfiguration": {"required": False},
        },
        "create": {
            "method": "create_rest_api",
            "args": [("Name", "name"), ("Description", "description")],
            "kwargs": {},
            "physical_id": "api.id",
            "attributes": {"RootResourceId": "api.get_resource_for_path('/').id"},
        },
        "delete": {"method": "delete_rest_api", "args": ["physical_id"]},
    },
    "AWS::ApiGateway::Resource": {
        "service": "apigateway",
        "global": False,
        "properties": {
            "RestApiId": {"required": True},
            "ParentId": {"required": True},
            "PathPart": {"required": True},
        },
        "create": {"method": "create_resource"},
        "delete": {"method": "delete_resource"},
    },
    "AWS::ApiGateway::Method": {
        "service": "apigateway",
        "global": False,
        "properties": {
            "RestApiId": {"required": True},
            "ResourceId": {"required": True},
            "HttpMethod": {"required": True},
            "AuthorizationType": {"required": False, "default": "NONE"},
            "Integration": {"required": False, "post_create": True},
        },
        "create": {"method": "create_method"},
        "delete": {"method": "delete_method"},
    },
    "AWS::ApiGateway::Deployment": {
        "service": "apigateway",
        "global": False,
        "properties": {
            "RestApiId": {"required": True},
            "StageName": {"required": False},
            "Description": {"required": False, "default": ""},
        },
        "create": {"method": "create_deployment"},
        "delete": {"method": "delete_deployment"},
    },
    "AWS::ApiGateway::Stage": {
        "service": "apigateway",
        "global": False,
        "properties": {
            "RestApiId": {"required": True},
            "StageName": {"required": True},
            "DeploymentId": {"required": True},
            "Description": {"required": False, "default": ""},
            "Variables": {"required": False},
        },
        "create": {"method": "create_stage"},
        "delete": {"method": "delete_stage"},
    },
    "AWS::ApiGateway::ApiKey": {
        "service": "apigateway",
        "global": False,
        "properties": {
            "Name": {"required": False, "auto_name": True},
            "Enabled": {"required": False, "default": True},
            "Description": {"required": False, "default": ""},
        },
        "create": {"method": "create_apikey"},
        "delete": {"method": "delete_apikey"},
    },
    "AWS::ApiGateway::UsagePlan": {
        "service": "apigateway",
        "global": False,
        "properties": {
            "UsagePlanName": {"required": False, "auto_name": True},
            "Description": {"required": False, "default": ""},
            "ApiStages": {"required": False},
            "Throttle": {"required": False},
            "Quota": {"required": False},
        },
        "create": {"method": "create_usage_plan"},
        "delete": {"method": "delete_usage_plan"},
    },
    # --- CloudWatch ---
    "AWS::CloudWatch::Alarm": {
        "service": "cloudwatch",
        "global": False,
        "properties": {
            "AlarmName": {"required": True},
            "MetricName": {"required": True},
            "Namespace": {"required": True},
            "Statistic": {"required": False, "default": "Average"},
            "Period": {"required": False, "default": 300},
            "EvaluationPeriods": {"required": True},
            "Threshold": {"required": True},
            "ComparisonOperator": {"required": True},
            "AlarmActions": {"required": False, "default": "[]"},
            "OKActions": {"required": False, "default": "[]"},
            "Dimensions": {"required": False, "default": "[]"},
        },
        "create": {"method": "put_metric_alarm"},
        "delete": {"method": "delete_alarms", "args": [["physical_id"]]},
    },
    "AWS::Logs::LogGroup": {
        "service": "logs",
        "global": False,
        "properties": {
            "LogGroupName": {"required": False, "auto_name": True, "prefix": "/cfn/"},
            "RetentionInDays": {"required": False},
            "Tags": {"required": False, "default": "{}"},
        },
        "create": {
            "method": "create_log_group",
            "args": [("LogGroupName", "log_group_name"), ("Tags", "tags")],
            "kwargs": {},
            "physical_id": "name",
            "attributes": {
                "Arn": "f'arn:aws:logs:{region}:{account_id}:log-group:{name}'"
            },
        },
        "delete": {"method": "delete_log_group", "args": ["physical_id"]},
    },
    "AWS::Logs::LogStream": {
        "service": "logs",
        "global": False,
        "properties": {
            "LogGroupName": {"required": True},
            "LogStreamName": {"required": False, "auto_name": True},
        },
        "create": {"method": "create_log_stream"},
        "delete": {"method": "delete_log_stream"},
    },
    # --- EventBridge ---
    "AWS::Events::Rule": {
        "service": "events",
        "global": False,
        "properties": {
            "Name": {"required": False, "auto_name": True},
            "EventPattern": {"required": False, "json_encode": True},
            "ScheduleExpression": {"required": False},
            "State": {"required": False, "default": "ENABLED"},
            "Description": {"required": False, "default": ""},
            "EventBusName": {"required": False, "default": "default"},
            "Targets": {"required": False, "post_create": True},
        },
        "create": {"method": "native"},
        "delete": {"method": "native"},
    },
    "AWS::Events::EventBus": {
        "service": "events",
        "global": False,
        "properties": {
            "Name": {"required": True},
        },
        "create": {"method": "native"},
        "delete": {"method": "native"},
    },
    # --- Step Functions ---
    "AWS::StepFunctions::StateMachine": {
        "service": "stepfunctions",
        "global": False,
        "properties": {
            "StateMachineName": {"required": False, "auto_name": True},
            "DefinitionString": {"required": True},
            "RoleArn": {"required": True},
            "StateMachineType": {"required": False, "default": "STANDARD"},
            "Tags": {"required": False, "default": "[]"},
        },
        "create": {"method": "create_state_machine"},
        "delete": {"method": "delete_state_machine"},
    },
    # --- KMS ---
    "AWS::KMS::Key": {
        "service": "kms",
        "global": False,
        "properties": {
            "Description": {"required": False, "default": ""},
            "KeyUsage": {"required": False, "default": "ENCRYPT_DECRYPT"},
            "KeySpec": {"required": False, "default": "SYMMETRIC_DEFAULT"},
            "KeyPolicy": {"required": False, "json_encode": True},
            "Tags": {"required": False, "default": "[]"},
        },
        "create": {"method": "native"},
        "delete": {"method": "native"},
    },
    "AWS::KMS::Alias": {
        "service": "kms",
        "global": False,
        "properties": {
            "AliasName": {"required": True},
            "TargetKeyId": {"required": True},
        },
        "create": {"method": "create_alias"},
        "delete": {"method": "delete_alias"},
    },
    # --- Secrets Manager ---
    "AWS::SecretsManager::Secret": {
        "service": "secretsmanager",
        "global": False,
        "properties": {
            "Name": {"required": False, "auto_name": True},
            "Description": {"required": False, "default": ""},
            "SecretString": {"required": False},
            "KmsKeyId": {"required": False},
            "Tags": {"required": False, "default": "[]"},
        },
        "create": {"method": "create_secret"},
        "delete": {"method": "delete_secret"},
    },
    # --- SSM ---
    "AWS::SSM::Parameter": {
        "service": "ssm",
        "global": False,
        "properties": {
            "Name": {"required": False, "auto_name": True, "prefix": "/cfn/"},
            "Type": {"required": True},
            "Value": {"required": True},
            "Description": {"required": False, "default": ""},
            "Tags": {"required": False, "default": "[]"},
        },
        "create": {"method": "native"},
        "delete": {"method": "native"},
    },
    "AWS::SSM::Document": {
        "service": "ssm",
        "global": False,
        "properties": {
            "Name": {"required": True},
            "Content": {"required": True, "json_encode": True},
            "DocumentType": {"required": False, "default": "Command"},
            "DocumentFormat": {"required": False, "default": "JSON"},
        },
        "create": {"method": "create_document"},
        "delete": {"method": "delete_document"},
    },
    # --- Kinesis ---
    "AWS::Kinesis::Stream": {
        "service": "kinesis",
        "global": False,
        "properties": {
            "Name": {"required": False, "auto_name": True},
            "ShardCount": {"required": False, "default": 1},
            "StreamModeDetails": {"required": False},
            "Tags": {"required": False, "default": "[]"},
        },
        "create": {"method": "create_stream"},
        "delete": {"method": "delete_stream"},
    },
    # --- ECS ---
    "AWS::ECS::Cluster": {
        "service": "ecs",
        "global": False,
        "properties": {
            "ClusterName": {"required": False, "auto_name": True},
            "Tags": {"required": False, "default": "[]"},
            "CapacityProviders": {"required": False},
        },
        "create": {
            "method": "create_cluster",
            "args": [("ClusterName", "cluster_name")],
            "kwargs": {},
            "physical_id": "cluster.arn",
            "attributes": {"Arn": "cluster.arn"},
        },
        "delete": {"method": "delete_cluster", "args": ["physical_id"]},
    },
    "AWS::ECS::TaskDefinition": {
        "service": "ecs",
        "global": False,
        "properties": {
            "Family": {"required": True},
            "ContainerDefinitions": {"required": True},
            "Cpu": {"required": False},
            "Memory": {"required": False},
            "NetworkMode": {"required": False},
            "RequiresCompatibilities": {"required": False},
            "ExecutionRoleArn": {"required": False},
            "TaskRoleArn": {"required": False},
        },
        "create": {"method": "register_task_definition"},
        "delete": {"method": "deregister_task_definition"},
    },
    "AWS::ECS::Service": {
        "service": "ecs",
        "global": False,
        "properties": {
            "ServiceName": {"required": False, "auto_name": True},
            "Cluster": {"required": False},
            "TaskDefinition": {"required": True},
            "DesiredCount": {"required": False, "default": 1},
            "LaunchType": {"required": False, "default": "EC2"},
        },
        "create": {"method": "create_service"},
        "delete": {"method": "delete_service"},
    },
    # --- ELB ---
    "AWS::ElasticLoadBalancingV2::LoadBalancer": {
        "service": "elbv2",
        "global": False,
        "properties": {
            "Name": {"required": False, "auto_name": True},
            "Type": {"required": False, "default": "application"},
            "Scheme": {"required": False, "default": "internet-facing"},
            "Subnets": {"required": False},
            "SecurityGroups": {"required": False},
            "Tags": {"required": False, "default": "[]"},
        },
        "create": {"method": "create_load_balancer"},
        "delete": {"method": "delete_load_balancer"},
    },
    "AWS::ElasticLoadBalancingV2::TargetGroup": {
        "service": "elbv2",
        "global": False,
        "properties": {
            "Name": {"required": False, "auto_name": True},
            "Protocol": {"required": False, "default": "HTTP"},
            "Port": {"required": False, "default": 80},
            "VpcId": {"required": False},
            "TargetType": {"required": False, "default": "instance"},
            "HealthCheckPath": {"required": False, "default": "/"},
        },
        "create": {"method": "create_target_group"},
        "delete": {"method": "delete_target_group"},
    },
    "AWS::ElasticLoadBalancingV2::Listener": {
        "service": "elbv2",
        "global": False,
        "properties": {
            "LoadBalancerArn": {"required": True},
            "Port": {"required": True},
            "Protocol": {"required": False, "default": "HTTP"},
            "DefaultActions": {"required": True},
        },
        "create": {"method": "create_listener"},
        "delete": {"method": "delete_listener"},
    },
    # --- CloudFront ---
    "AWS::CloudFront::Distribution": {
        "service": "cloudfront",
        "global": True,
        "properties": {
            "DistributionConfig": {"required": True},
            "Tags": {"required": False, "default": "[]"},
        },
        "create": {"method": "create_distribution"},
        "delete": {"method": "delete_distribution"},
    },
    # --- Route53 ---
    "AWS::Route53::HostedZone": {
        "service": "route53",
        "global": True,
        "properties": {
            "Name": {"required": True},
            "HostedZoneConfig": {"required": False},
        },
        "create": {"method": "create_hosted_zone"},
        "delete": {"method": "delete_hosted_zone"},
    },
    "AWS::Route53::RecordSet": {
        "service": "route53",
        "global": True,
        "properties": {
            "HostedZoneId": {"required": False},
            "HostedZoneName": {"required": False},
            "Name": {"required": True},
            "Type": {"required": True},
            "ResourceRecords": {"required": False},
            "TTL": {"required": False, "default": 300},
            "AliasTarget": {"required": False},
        },
        "create": {"method": "change_resource_record_sets"},
        "delete": {"method": "change_resource_record_sets"},
    },
    # --- ACM ---
    "AWS::CertificateManager::Certificate": {
        "service": "acm",
        "global": False,
        "properties": {
            "DomainName": {"required": True},
            "SubjectAlternativeNames": {"required": False},
            "ValidationMethod": {"required": False, "default": "DNS"},
            "Tags": {"required": False, "default": "[]"},
        },
        "create": {"method": "request_certificate"},
        "delete": {"method": "delete_certificate"},
    },
}


def _to_snake_case(name: str) -> str:
    s1 = re.sub(r"(.)([A-Z][a-z]+)", r"\1_\2", name)
    return re.sub(r"([a-z0-9])([A-Z])", r"\1_\2", s1).lower()


def generate_handler_code(resource_type: str, spec: dict) -> str:
    """Generate create and delete handler functions for a resource type."""
    parts = resource_type.split("::")
    short_name = _to_snake_case(parts[-1])
    service = spec["service"]
    is_global = spec.get("global", False)

    lines = []
    lines.append(f"# --- {resource_type} ---")
    lines.append("")

    # Create handler
    lines.append("")
    lines.append(
        f"def _create_{service}_{short_name}"
        f"(resource: CfnResource, region: str, account_id: str) -> None:"
    )

    if is_global:
        lines.append(f'    backend = _moto_global_backend("{service}", account_id)')
    else:
        lines.append(f'    backend = _moto_backend("{service}", account_id, region)')

    # Generate property extraction
    for prop_name, prop_spec in spec.get("properties", {}).items():
        if prop_spec.get("post_create"):
            continue
        snake = _to_snake_case(prop_name)
        if prop_spec.get("auto_name"):
            prefix = prop_spec.get("prefix", "")
            lines.append(
                f"    {snake} = resource.properties.get("
                f'"{prop_name}", '
                f'f"{prefix}cfn-{{resource.logical_id}}-{{uuid.uuid4().hex[:8]}}")'
            )
        elif prop_spec.get("required"):
            lines.append(
                f'    {snake} = resource.properties.get("{prop_name}", "")'
            )
        else:
            default = prop_spec.get("default", "")
            if isinstance(default, str) and default not in ("", "[]", "{}"):
                lines.append(
                    f'    {snake} = resource.properties.get("{prop_name}", "{default}")'
                )
            elif isinstance(default, bool):
                lines.append(
                    f'    {snake} = resource.properties.get("{prop_name}", {default})'
                )
            elif isinstance(default, (int, float)):
                lines.append(
                    f'    {snake} = resource.properties.get("{prop_name}", {default})'
                )
            else:
                lines.append(
                    f'    {snake} = resource.properties.get("{prop_name}", {default or "None"})'
                )

        if prop_spec.get("json_encode"):
            lines.append(f"    if isinstance({snake}, dict):")
            lines.append(f"        {snake} = json.dumps({snake})")

    # The actual create call
    lines.append(f"    # TODO: Call backend.{spec['create']['method']}(...)")
    lines.append(
        f"    resource.physical_id = "
        f'f"{resource_type.replace("::", "-").lower()}-{{uuid.uuid4().hex[:8]}}"'
    )
    lines.append('    resource.status = "CREATE_COMPLETE"')

    lines.append("")
    lines.append("")

    # Delete handler
    lines.append(
        f"def _delete_{service}_{short_name}"
        f"(resource: CfnResource, region: str, account_id: str) -> None:"
    )
    lines.append("    try:")
    if is_global:
        lines.append(f'        backend = _moto_global_backend("{service}", account_id)')
    else:
        lines.append(f'        backend = _moto_backend("{service}", account_id, region)')
    lines.append("        if resource.physical_id:")
    del_method = spec.get("delete", {}).get("method", "delete")
    lines.append(f"            pass  # TODO: backend.{del_method}(...)")
    lines.append("    except Exception:")
    lines.append("        pass")
    lines.append("")

    return "\n".join(lines)


def generate_handler_map_entries(resource_type: str, spec: dict) -> tuple[str, str]:
    """Generate the handler map entries for a resource type."""
    parts = resource_type.split("::")
    short_name = _to_snake_case(parts[-1])
    service = spec["service"]

    create_entry = f'    "{resource_type}": _create_{service}_{short_name},'
    delete_entry = f'    "{resource_type}": _delete_{service}_{short_name},'
    return create_entry, delete_entry


def list_all_types():
    """List all supported resource types."""
    print(f"\nSupported CloudFormation Resource Types ({len(RESOURCE_SPECS)}):\n")
    by_service: dict[str, list[str]] = {}
    for rt, spec in sorted(RESOURCE_SPECS.items()):
        svc = spec["service"]
        by_service.setdefault(svc, []).append(rt)

    for svc, types in sorted(by_service.items()):
        print(f"  {svc}:")
        for t in types:
            status = "native" if RESOURCE_SPECS[t]["create"]["method"] == "native" else "moto"
            print(f"    {t} ({status})")
    print(f"\n  Total: {len(RESOURCE_SPECS)} resource types")


def generate_all(write: bool = False, target_file: str | None = None):
    """Generate handler code for all resource types not yet in resources.py."""
    resources_path = Path(
        target_file
        or "src/robotocore/services/cloudformation/resources.py"
    )
    existing = resources_path.read_text() if resources_path.exists() else ""

    # Find which types are already implemented
    existing_types = set(re.findall(r'"(AWS::[^"]+)"', existing))
    new_types = {t: s for t, s in RESOURCE_SPECS.items() if t not in existing_types}

    if not new_types:
        print("All resource types are already implemented!")
        return

    print(f"\nNew resource types to generate ({len(new_types)}):")
    for t in sorted(new_types):
        print(f"  {t}")

    # Generate code for each new type
    handler_code = []
    create_entries = []
    delete_entries = []

    for rt, spec in sorted(new_types.items()):
        handler_code.append(generate_handler_code(rt, spec))
        create, delete = generate_handler_map_entries(rt, spec)
        create_entries.append(create)
        delete_entries.append(delete)

    if write:
        print(f"\nWould need to add {len(new_types)} handlers to {resources_path}")
        print("Handler code and map entries generated. Integrate manually or use --write.")
    else:
        print("\n# --- Generated Handler Code ---\n")
        print("\n\n".join(handler_code))
        print("\n# --- CREATE_HANDLERS additions ---\n")
        print("\n".join(create_entries))
        print("\n# --- DELETE_HANDLERS additions ---\n")
        print("\n".join(delete_entries))


def main():
    parser = argparse.ArgumentParser(description="Generate CloudFormation resource handlers")
    parser.add_argument("resource_type", nargs="?", help="e.g., AWS::EC2::VPC")
    parser.add_argument("--list", action="store_true", help="List all supported types")
    parser.add_argument("--dry-run", action="store_true", help="Preview all new handlers")
    parser.add_argument("--write", action="store_true", help="Write to resources.py")
    parser.add_argument("--batch", help="File with resource types (one per line)")
    parser.add_argument(
        "--file", default="src/robotocore/services/cloudformation/resources.py",
        help="Target resources.py file",
    )
    args = parser.parse_args()

    if args.list:
        list_all_types()
        return

    if args.dry_run:
        generate_all(write=False, target_file=args.file)
        return

    if args.batch:
        types = Path(args.batch).read_text().strip().splitlines()
        for t in types:
            t = t.strip()
            if not t or t.startswith("#"):
                continue
            if t not in RESOURCE_SPECS:
                print(f"WARNING: Unknown resource type: {t}", file=sys.stderr)
                continue
            print(generate_handler_code(t, RESOURCE_SPECS[t]))
        return

    if args.resource_type:
        if args.resource_type not in RESOURCE_SPECS:
            print(f"Unknown resource type: {args.resource_type}", file=sys.stderr)
            print("Use --list to see supported types", file=sys.stderr)
            sys.exit(1)
        spec = RESOURCE_SPECS[args.resource_type]
        print(generate_handler_code(args.resource_type, spec))
        create, delete = generate_handler_map_entries(args.resource_type, spec)
        print("\n# Handler map entries:")
        print("# _CREATE_HANDLERS:")
        print(create)
        print("# _DELETE_HANDLERS:")
        print(delete)
        return

    if args.write:
        generate_all(write=True, target_file=args.file)
        return

    parser.print_help()


if __name__ == "__main__":
    main()
