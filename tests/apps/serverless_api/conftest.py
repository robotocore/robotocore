"""
Fixtures for the serverless API application tests.

Provides pre-configured ServerlessApp instances, deployed API stacks,
and Step Functions state machines for use across test files.
"""

import pytest

from .app import ServerlessApp
from .handlers import HELLO_HANDLER
from .models import ApiEndpoint, LambdaConfig, TableSchema, WorkflowStep


@pytest.fixture
def serverless_app(dynamodb, lambda_client, iam, apigateway, stepfunctions):
    """A ServerlessApp instance with all AWS clients wired up."""
    app = ServerlessApp(
        dynamodb=dynamodb,
        lambda_client=lambda_client,
        iam=iam,
        apigateway=apigateway,
        stepfunctions=stepfunctions,
    )
    yield app
    app.cleanup()


@pytest.fixture
def deployed_api(serverless_app, unique_name):
    """Deploy a full CRUD API: DynamoDB table + IAM role + Lambda + API Gateway.

    Yields (serverless_app, stack) where stack is a DeployedStack.
    Cleanup happens automatically via the serverless_app fixture.
    """
    table_name = f"users-{unique_name}"
    function_name = f"crud-handler-{unique_name}"

    table_schema = TableSchema(
        table_name=table_name,
        key_schema=[{"AttributeName": "user_id", "KeyType": "HASH"}],
        attributes=[{"AttributeName": "user_id", "AttributeType": "S"}],
    )

    lambda_config = LambdaConfig(
        function_name=function_name,
        handler="index.handler",
        runtime="python3.12",
        code=HELLO_HANDLER,
        env_vars={"TABLE_NAME": table_name},
    )

    endpoints = [
        ApiEndpoint(method="GET", path="/users", handler_name=function_name),
        ApiEndpoint(method="POST", path="/users", handler_name=function_name),
    ]

    stack = serverless_app.deploy_full_stack(
        stack_name=f"crud-{unique_name}",
        table_schemas=[table_schema],
        lambda_configs=[lambda_config],
        endpoints=endpoints,
    )

    yield serverless_app, stack


@pytest.fixture
def state_machine(serverless_app, unique_name):
    """Create a Step Functions state machine with Pass states.

    Yields (serverless_app, state_machine_arn, role_arn).
    Cleanup happens via the serverless_app fixture.
    """
    role_arn = serverless_app.create_step_functions_role(f"sfn-role-{unique_name}")

    steps = [
        WorkflowStep(
            name="ValidateInput",
            type="Pass",
            result={"status": "validated"},
            next="ProcessData",
        ),
        WorkflowStep(
            name="ProcessData",
            type="Pass",
            result={"status": "processed"},
            next="Complete",
        ),
        WorkflowStep(
            name="Complete",
            type="Succeed",
        ),
    ]

    sm_arn = serverless_app.create_state_machine(
        name=f"workflow-{unique_name}",
        role_arn=role_arn,
        steps=steps,
    )

    yield serverless_app, sm_arn, role_arn
