"""Pulumi program: REST API with API Gateway + Lambda."""

import json

import pulumi
import pulumi_aws as aws

# IAM role for Lambda
assume_role_policy = json.dumps(
    {
        "Version": "2012-10-17",
        "Statement": [
            {
                "Effect": "Allow",
                "Principal": {"Service": "lambda.amazonaws.com"},
                "Action": "sts:AssumeRole",
            }
        ],
    }
)

lambda_role = aws.iam.Role(
    "lambda-role",
    assume_role_policy=assume_role_policy,
)

aws.iam.RolePolicyAttachment(
    "lambda-basic-execution",
    role=lambda_role.name,
    policy_arn="arn:aws:iam::aws:policy/service-role/AWSLambdaBasicExecutionRole",
)

# Lambda function (inline handler)
handler_code = """
def handler(event, context):
    return {
        "statusCode": 200,
        "body": '{"message": "hello from lambda"}'
    }
"""

lambda_fn = aws.lambda_.Function(
    "api-handler",
    runtime="python3.12",
    handler="index.handler",
    role=lambda_role.arn,
    code=pulumi.AssetArchive({"index.py": pulumi.StringAsset(handler_code)}),
)

# API Gateway REST API
rest_api = aws.apigateway.RestApi(
    "rest-api",
    name="rest-api",
    description="Pulumi IaC test REST API",
)

# /hello resource
resource = aws.apigateway.Resource(
    "hello-resource",
    rest_api=rest_api.id,
    parent_id=rest_api.root_resource_id,
    path_part="hello",
)

# GET method
method = aws.apigateway.Method(
    "hello-get",
    rest_api=rest_api.id,
    resource_id=resource.id,
    http_method="GET",
    authorization="NONE",
)

# Lambda integration
integration = aws.apigateway.Integration(
    "hello-integration",
    rest_api=rest_api.id,
    resource_id=resource.id,
    http_method=method.http_method,
    type="AWS_PROXY",
    integration_http_method="POST",
    uri=lambda_fn.invoke_arn,
)

# Exports
pulumi.export("rest_api_id", rest_api.id)
pulumi.export("rest_api_name", rest_api.name)
pulumi.export("lambda_function_name", lambda_fn.name)
pulumi.export("lambda_role_name", lambda_role.name)
