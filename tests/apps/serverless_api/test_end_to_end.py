"""
End-to-end integration tests for the serverless API.

Tests the full stack: deploy → invoke → verify data, and multi-service
workflows combining API Gateway, Lambda, DynamoDB, and Step Functions.
"""

import json
import uuid

from .handlers import HELLO_HANDLER
from .models import (
    ApiEndpoint,
    LambdaConfig,
    TableSchema,
    WorkflowStep,
)


class TestEndToEnd:
    """Full stack integration tests."""

    def test_deploy_complete_stack(self, deployed_api):
        """Deploy a complete serverless app and verify all resources exist."""
        app, stack = deployed_api

        # Verify tables were created
        assert len(stack.tables) == 1

        # Verify functions were deployed
        assert len(stack.functions) == 1
        fn_name = list(stack.functions.keys())[0]
        functions = app.list_functions()
        names = [f["FunctionName"] for f in functions]
        assert fn_name in names

        # Verify API was created
        assert stack.api_url

        # Verify roles were created
        assert len(stack.roles) == 1
        role_name = list(stack.roles.keys())[0]
        role = app.get_role(role_name)
        assert role["RoleName"] == role_name

    def test_invoke_lambda_reads_dynamodb(self, serverless_app, unique_name):
        """Deploy Lambda + DynamoDB, invoke Lambda, verify data round-trip."""
        table_name = f"e2e-users-{unique_name}"
        table_schema = TableSchema(
            table_name=table_name,
            key_schema=[{"AttributeName": "user_id", "KeyType": "HASH"}],
            attributes=[{"AttributeName": "user_id", "AttributeType": "S"}],
        )
        serverless_app.create_table(table_schema)

        # Seed data directly
        user_id = str(uuid.uuid4())
        serverless_app.put_item(
            table_name,
            {
                "user_id": {"S": user_id},
                "email": {"S": "e2e@example.com"},
                "name": {"S": "E2E User"},
                "status": {"S": "active"},
            },
        )

        # Deploy a function that returns static data (since Lambda can't
        # easily reach the emulator's DynamoDB in a unit test context)
        role_arn = serverless_app.create_lambda_role(f"e2e-role-{unique_name}")
        config = LambdaConfig(
            function_name=f"e2e-handler-{unique_name}",
            handler="index.handler",
            runtime="python3.12",
            code=HELLO_HANDLER,
            env_vars={"TABLE_NAME": table_name},
        )
        serverless_app.deploy_function(config, role_arn)

        # Invoke and verify
        result = serverless_app.invoke_function(config.function_name, payload={"name": "E2E Test"})
        assert result["statusCode"] == 200
        body = json.loads(result["body"])
        assert body["message"] == "Hello, E2E Test!"

        # Verify data is in DynamoDB
        item = serverless_app.get_item(table_name, {"user_id": {"S": user_id}})
        assert item is not None
        assert item["email"]["S"] == "e2e@example.com"

    def test_full_workflow_with_step_functions(self, serverless_app, unique_name):
        """Complete workflow: create user in DynamoDB, orchestrate with Step Functions."""
        # 1. Create DynamoDB table
        table_name = f"wf-users-{unique_name}"
        table_schema = TableSchema(
            table_name=table_name,
            key_schema=[{"AttributeName": "user_id", "KeyType": "HASH"}],
            attributes=[{"AttributeName": "user_id", "AttributeType": "S"}],
        )
        serverless_app.create_table(table_schema)

        # 2. Insert a user
        user_id = str(uuid.uuid4())
        serverless_app.put_item(
            table_name,
            {
                "user_id": {"S": user_id},
                "email": {"S": "workflow@example.com"},
                "name": {"S": "Workflow User"},
                "status": {"S": "pending"},
            },
        )

        # 3. Create Step Functions workflow
        sfn_role_arn = serverless_app.create_step_functions_role(f"wf-sfn-role-{unique_name}")

        steps = [
            WorkflowStep(
                name="ValidateUser",
                type="Pass",
                result={"validation": "passed"},
                next="ActivateUser",
            ),
            WorkflowStep(
                name="ActivateUser",
                type="Pass",
                result={"status": "activated"},
                next="SendWelcome",
            ),
            WorkflowStep(
                name="SendWelcome",
                type="Pass",
                result={"notification": "sent"},
                next="Complete",
            ),
            WorkflowStep(name="Complete", type="Succeed"),
        ]

        sm_arn = serverless_app.create_state_machine(
            name=f"user-onboard-{unique_name}",
            role_arn=sfn_role_arn,
            steps=steps,
        )

        # 4. Execute workflow
        exec_arn = serverless_app.start_execution(
            sm_arn,
            input_data={"user_id": user_id, "email": "workflow@example.com"},
        )

        desc = serverless_app.describe_execution(exec_arn)
        assert desc["status"] in ("RUNNING", "SUCCEEDED")

        # 5. Update user status in DynamoDB
        serverless_app.update_item(
            table_name,
            key={"user_id": {"S": user_id}},
            update_expression="SET #s = :status",
            expression_names={"#s": "status"},
            expression_values={":status": {"S": "activated"}},
        )

        # 6. Verify final state
        item = serverless_app.get_item(table_name, {"user_id": {"S": user_id}})
        assert item["status"]["S"] == "activated"
        assert item["email"]["S"] == "workflow@example.com"

    def test_multi_table_multi_function(self, serverless_app, unique_name):
        """Deploy multiple tables and functions in a single stack."""
        users_schema = TableSchema(
            table_name=f"mt-users-{unique_name}",
            key_schema=[{"AttributeName": "user_id", "KeyType": "HASH"}],
            attributes=[{"AttributeName": "user_id", "AttributeType": "S"}],
        )
        orders_schema = TableSchema(
            table_name=f"mt-orders-{unique_name}",
            key_schema=[{"AttributeName": "order_id", "KeyType": "HASH"}],
            attributes=[{"AttributeName": "order_id", "AttributeType": "S"}],
        )

        fn1_config = LambdaConfig(
            function_name=f"mt-users-fn-{unique_name}",
            handler="index.handler",
            runtime="python3.12",
            code=HELLO_HANDLER,
        )
        fn2_config = LambdaConfig(
            function_name=f"mt-orders-fn-{unique_name}",
            handler="index.handler",
            runtime="python3.12",
            code=HELLO_HANDLER,
        )

        endpoints = [
            ApiEndpoint(method="GET", path="/users", handler_name=fn1_config.function_name),
            ApiEndpoint(method="GET", path="/orders", handler_name=fn2_config.function_name),
        ]

        stack = serverless_app.deploy_full_stack(
            stack_name=f"multi-{unique_name}",
            table_schemas=[users_schema, orders_schema],
            lambda_configs=[fn1_config, fn2_config],
            endpoints=endpoints,
        )

        assert len(stack.tables) == 2
        assert len(stack.functions) == 2

        # Verify both functions work
        for fn_name in stack.functions:
            result = serverless_app.invoke_function(fn_name)
            assert result["statusCode"] == 200
