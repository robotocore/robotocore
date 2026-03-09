"""CDK app: REST API with Lambda backend and API Gateway."""

from __future__ import annotations

import json

import aws_cdk as cdk
import aws_cdk.aws_apigateway as apigw
import aws_cdk.aws_iam as iam
import aws_cdk.aws_lambda as lambda_


class RestApiStack(cdk.Stack):
    """Stack that creates an API Gateway REST API backed by a Lambda function."""

    def __init__(self, scope: cdk.App, construct_id: str, **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)

        # IAM role for Lambda
        role = iam.Role(
            self,
            "LambdaRole",
            role_name=f"{construct_id}-lambda-role",
            assumed_by=iam.ServicePrincipal("lambda.amazonaws.com"),
            managed_policies=[
                iam.ManagedPolicy.from_aws_managed_policy_name(
                    "service-role/AWSLambdaBasicExecutionRole"
                ),
            ],
        )

        # Lambda function with inline code
        handler = lambda_.Function(
            self,
            "HelloFunction",
            function_name=f"{construct_id}-hello",
            runtime=lambda_.Runtime.PYTHON_3_12,
            handler="index.handler",
            role=role,
            code=lambda_.Code.from_inline(
                json.dumps(
                    {
                        "handler": (
                            "import json\n"
                            "def handler(event, context):\n"
                            '    return {"statusCode": 200,'
                            ' "body": json.dumps({"message": "hello from lambda"})}\n'
                        )
                    }
                )
            ),
        )

        # API Gateway REST API
        api = apigw.RestApi(
            self,
            "RestApi",
            rest_api_name=f"{construct_id}-api",
            description="REST API for hello function",
        )

        # /hello resource with GET method integrated with Lambda
        hello_resource = api.root.add_resource("hello")
        hello_resource.add_method(
            "GET",
            apigw.LambdaIntegration(handler, proxy=True),
            authorization_type=apigw.AuthorizationType.NONE,
        )

        # Deploy to "test" stage
        deployment = apigw.Deployment(self, "Deployment", api=api)
        apigw.Stage(self, "TestStage", deployment=deployment, stage_name="test")

        # Outputs
        cdk.CfnOutput(self, "RestApiId", value=api.rest_api_id)
        cdk.CfnOutput(self, "LambdaFunctionName", value=handler.function_name)
        cdk.CfnOutput(self, "RoleName", value=role.role_name)


app = cdk.App()
RestApiStack(
    app,
    "CdkRestApiStack",
    env=cdk.Environment(account="123456789012", region="us-east-1"),
)
app.synth()
