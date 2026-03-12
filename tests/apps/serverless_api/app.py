"""
ServerlessApp — orchestrates a complete serverless REST API stack.

Manages the lifecycle of:
- DynamoDB tables (storage)
- IAM roles (permissions)
- Lambda functions (compute)
- API Gateway REST APIs (HTTP routing)
- Step Functions state machines (workflow orchestration)

Only depends on boto3 — no robotocore or moto imports. Works against
robotocore (localhost:4566) or real AWS.
"""

from __future__ import annotations

import json
from typing import Any

from tests.apps.conftest import make_lambda_zip

from .models import (
    ApiDeployment,
    ApiEndpoint,
    DeployedStack,
    LambdaConfig,
    TableSchema,
    WorkflowStep,
)


class ServerlessApp:
    """Full-lifecycle manager for a serverless REST API stack.

    Provides methods for every stage of building a serverless application:
    table creation, IAM role setup, Lambda deployment, API Gateway wiring,
    Step Functions orchestration, and clean teardown.
    """

    def __init__(
        self,
        dynamodb,
        lambda_client,
        iam,
        apigateway,
        stepfunctions,
        *,
        account_id: str = "123456789012",
        region: str = "us-east-1",
    ):
        self.dynamodb = dynamodb
        self.lambda_client = lambda_client
        self.iam = iam
        self.apigateway = apigateway
        self.stepfunctions = stepfunctions
        self.account_id = account_id
        self.region = region

        # Track created resources for cleanup
        self._tables: list[str] = []
        self._roles: list[str] = []
        self._policies: list[tuple[str, str]] = []  # (role_name, policy_arn)
        self._functions: list[str] = []
        self._rest_apis: list[str] = []
        self._state_machines: list[str] = []
        self._api_keys: list[str] = []
        self._usage_plans: list[str] = []

    # ------------------------------------------------------------------ #
    #  DynamoDB table management                                          #
    # ------------------------------------------------------------------ #

    def create_table(self, schema: TableSchema) -> str:
        """Create a DynamoDB table from a TableSchema definition.

        Returns the table name.
        """
        kwargs: dict[str, Any] = {
            "TableName": schema.table_name,
            "KeySchema": schema.key_schema,
            "AttributeDefinitions": schema.attributes,
            "BillingMode": "PAY_PER_REQUEST",
        }
        if schema.gsis:
            kwargs["GlobalSecondaryIndexes"] = schema.gsis
        self.dynamodb.create_table(**kwargs)
        self._tables.append(schema.table_name)
        return schema.table_name

    def put_item(self, table_name: str, item: dict) -> dict:
        """Put a raw DynamoDB item (already in DynamoDB JSON format)."""
        return self.dynamodb.put_item(TableName=table_name, Item=item)

    def get_item(self, table_name: str, key: dict) -> dict | None:
        """Get an item by key. Returns the Item dict or None."""
        resp = self.dynamodb.get_item(TableName=table_name, Key=key)
        return resp.get("Item")

    def query_table(
        self,
        table_name: str,
        key_condition: str,
        expression_values: dict,
        *,
        index_name: str | None = None,
    ) -> list[dict]:
        """Query a table or GSI. Returns the Items list."""
        kwargs: dict[str, Any] = {
            "TableName": table_name,
            "KeyConditionExpression": key_condition,
            "ExpressionAttributeValues": expression_values,
        }
        if index_name:
            kwargs["IndexName"] = index_name
        resp = self.dynamodb.query(**kwargs)
        return resp.get("Items", [])

    def scan_table(
        self,
        table_name: str,
        filter_expression: str | None = None,
        expression_values: dict | None = None,
        expression_names: dict | None = None,
    ) -> list[dict]:
        """Scan a table with optional filter. Returns the Items list."""
        kwargs: dict[str, Any] = {"TableName": table_name}
        if filter_expression:
            kwargs["FilterExpression"] = filter_expression
        if expression_values:
            kwargs["ExpressionAttributeValues"] = expression_values
        if expression_names:
            kwargs["ExpressionAttributeNames"] = expression_names
        resp = self.dynamodb.scan(**kwargs)
        return resp.get("Items", [])

    def update_item(
        self,
        table_name: str,
        key: dict,
        update_expression: str,
        expression_names: dict | None = None,
        expression_values: dict | None = None,
    ) -> dict:
        """Update an item with an UpdateExpression."""
        kwargs: dict[str, Any] = {
            "TableName": table_name,
            "Key": key,
            "UpdateExpression": update_expression,
        }
        if expression_names:
            kwargs["ExpressionAttributeNames"] = expression_names
        if expression_values:
            kwargs["ExpressionAttributeValues"] = expression_values
        return self.dynamodb.update_item(**kwargs)

    def delete_item(self, table_name: str, key: dict) -> dict:
        """Delete an item by key."""
        return self.dynamodb.delete_item(TableName=table_name, Key=key)

    def batch_write(self, table_name: str, items: list[dict]) -> dict:
        """Batch write items (PutRequest) to a table."""
        request_items = {table_name: [{"PutRequest": {"Item": item}} for item in items]}
        return self.dynamodb.batch_write_item(RequestItems=request_items)

    def batch_get(self, table_name: str, keys: list[dict]) -> list[dict]:
        """Batch get items by keys. Returns the Items list."""
        resp = self.dynamodb.batch_get_item(RequestItems={table_name: {"Keys": keys}})
        return resp.get("Responses", {}).get(table_name, [])

    # ------------------------------------------------------------------ #
    #  IAM role management                                                #
    # ------------------------------------------------------------------ #

    def create_lambda_role(self, role_name: str) -> str:
        """Create an IAM role for Lambda execution.

        Returns the role ARN.
        """
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
        resp = self.iam.create_role(
            RoleName=role_name,
            AssumeRolePolicyDocument=assume_role_policy,
            Description=f"Lambda execution role: {role_name}",
        )
        self._roles.append(role_name)
        return resp["Role"]["Arn"]

    def create_step_functions_role(self, role_name: str) -> str:
        """Create an IAM role for Step Functions execution.

        Returns the role ARN.
        """
        assume_role_policy = json.dumps(
            {
                "Version": "2012-10-17",
                "Statement": [
                    {
                        "Effect": "Allow",
                        "Principal": {"Service": "states.amazonaws.com"},
                        "Action": "sts:AssumeRole",
                    }
                ],
            }
        )
        resp = self.iam.create_role(
            RoleName=role_name,
            AssumeRolePolicyDocument=assume_role_policy,
            Description=f"Step Functions execution role: {role_name}",
        )
        self._roles.append(role_name)
        return resp["Role"]["Arn"]

    def attach_role_policy(self, role_name: str, policy_arn: str) -> None:
        """Attach a managed policy to a role."""
        self.iam.attach_role_policy(RoleName=role_name, PolicyArn=policy_arn)
        self._policies.append((role_name, policy_arn))

    def get_role(self, role_name: str) -> dict:
        """Describe an IAM role."""
        resp = self.iam.get_role(RoleName=role_name)
        return resp["Role"]

    # ------------------------------------------------------------------ #
    #  Lambda function management                                         #
    # ------------------------------------------------------------------ #

    def deploy_function(self, config: LambdaConfig, role_arn: str) -> str:
        """Deploy a Lambda function from a LambdaConfig.

        Returns the function ARN.
        """
        kwargs: dict[str, Any] = {
            "FunctionName": config.function_name,
            "Runtime": config.runtime,
            "Role": role_arn,
            "Handler": config.handler,
            "Code": {"ZipFile": make_lambda_zip(config.code)},
            "Timeout": config.timeout,
            "MemorySize": config.memory,
        }
        if config.env_vars:
            kwargs["Environment"] = {"Variables": config.env_vars}
        resp = self.lambda_client.create_function(**kwargs)
        self._functions.append(config.function_name)
        return resp["FunctionArn"]

    def invoke_function(
        self,
        function_name: str,
        payload: dict | None = None,
        *,
        invocation_type: str = "RequestResponse",
    ) -> dict:
        """Invoke a Lambda function and return the parsed response payload."""
        kwargs: dict[str, Any] = {
            "FunctionName": function_name,
            "InvocationType": invocation_type,
        }
        if payload is not None:
            kwargs["Payload"] = json.dumps(payload)
        resp = self.lambda_client.invoke(**kwargs)
        if invocation_type == "Event":
            return {"StatusCode": resp["StatusCode"]}
        raw = resp["Payload"].read()
        return json.loads(raw)

    def update_function_code(self, function_name: str, code: str) -> dict:
        """Update a Lambda function's code."""
        return self.lambda_client.update_function_code(
            FunctionName=function_name,
            ZipFile=make_lambda_zip(code),
        )

    def update_function_env(self, function_name: str, env_vars: dict[str, str]) -> dict:
        """Update a Lambda function's environment variables."""
        return self.lambda_client.update_function_configuration(
            FunctionName=function_name,
            Environment={"Variables": env_vars},
        )

    def list_functions(self) -> list[dict]:
        """List all Lambda functions."""
        resp = self.lambda_client.list_functions()
        return resp.get("Functions", [])

    def publish_version(self, function_name: str, description: str = "") -> dict:
        """Publish a new version of a Lambda function."""
        return self.lambda_client.publish_version(
            FunctionName=function_name,
            Description=description,
        )

    def create_alias(self, function_name: str, alias_name: str, version: str) -> dict:
        """Create an alias pointing to a specific function version."""
        return self.lambda_client.create_alias(
            FunctionName=function_name,
            Name=alias_name,
            FunctionVersion=version,
        )

    def delete_function(self, function_name: str) -> None:
        """Delete a Lambda function."""
        self.lambda_client.delete_function(FunctionName=function_name)
        if function_name in self._functions:
            self._functions.remove(function_name)

    # ------------------------------------------------------------------ #
    #  API Gateway management                                             #
    # ------------------------------------------------------------------ #

    def create_rest_api(self, name: str, description: str = "") -> str:
        """Create a new REST API. Returns the API ID."""
        resp = self.apigateway.create_rest_api(
            name=name, description=description or f"REST API: {name}"
        )
        api_id = resp["id"]
        self._rest_apis.append(api_id)
        return api_id

    def get_root_resource_id(self, api_id: str) -> str:
        """Get the root resource ID for a REST API."""
        resources = self.apigateway.get_resources(restApiId=api_id)
        for r in resources["items"]:
            if r["path"] == "/":
                return r["id"]
        raise ValueError(f"No root resource found for API {api_id}")

    def create_resource(self, api_id: str, parent_id: str, path_part: str) -> str:
        """Create a child resource. Returns the resource ID."""
        resp = self.apigateway.create_resource(
            restApiId=api_id, parentId=parent_id, pathPart=path_part
        )
        return resp["id"]

    def add_method(
        self,
        api_id: str,
        resource_id: str,
        http_method: str,
        authorization_type: str = "NONE",
        *,
        api_key_required: bool = False,
    ) -> dict:
        """Add an HTTP method to a resource."""
        return self.apigateway.put_method(
            restApiId=api_id,
            resourceId=resource_id,
            httpMethod=http_method,
            authorizationType=authorization_type,
            apiKeyRequired=api_key_required,
        )

    def add_lambda_integration(
        self,
        api_id: str,
        resource_id: str,
        http_method: str,
        function_arn: str,
    ) -> dict:
        """Wire a Lambda proxy integration to a method."""
        uri = (
            f"arn:aws:apigateway:{self.region}:lambda:path"
            f"/2015-03-31/functions/{function_arn}/invocations"
        )
        return self.apigateway.put_integration(
            restApiId=api_id,
            resourceId=resource_id,
            httpMethod=http_method,
            type="AWS_PROXY",
            integrationHttpMethod="POST",
            uri=uri,
        )

    def add_mock_integration(
        self,
        api_id: str,
        resource_id: str,
        http_method: str,
    ) -> dict:
        """Add a MOCK integration (useful for OPTIONS / CORS preflight)."""
        return self.apigateway.put_integration(
            restApiId=api_id,
            resourceId=resource_id,
            httpMethod=http_method,
            type="MOCK",
            requestTemplates={"application/json": '{"statusCode": 200}'},
        )

    def add_method_response(
        self,
        api_id: str,
        resource_id: str,
        http_method: str,
        status_code: str = "200",
        response_parameters: dict | None = None,
    ) -> dict:
        """Add a method response (needed for CORS headers, etc.)."""
        kwargs: dict[str, Any] = {
            "restApiId": api_id,
            "resourceId": resource_id,
            "httpMethod": http_method,
            "statusCode": status_code,
        }
        if response_parameters:
            kwargs["responseParameters"] = response_parameters
        return self.apigateway.put_method_response(**kwargs)

    def add_integration_response(
        self,
        api_id: str,
        resource_id: str,
        http_method: str,
        status_code: str = "200",
        response_parameters: dict | None = None,
    ) -> dict:
        """Add an integration response."""
        kwargs: dict[str, Any] = {
            "restApiId": api_id,
            "resourceId": resource_id,
            "httpMethod": http_method,
            "statusCode": status_code,
        }
        if response_parameters:
            kwargs["responseParameters"] = response_parameters
        return self.apigateway.put_integration_response(**kwargs)

    def configure_cors(
        self,
        api_id: str,
        resource_id: str,
        allowed_origins: str = "'*'",
        allowed_methods: str = "'GET,POST,PUT,DELETE,OPTIONS'",
        allowed_headers: str = "'Content-Type,Authorization'",
    ) -> None:
        """Configure CORS on a resource by adding OPTIONS method with headers.

        Sets up the OPTIONS method with MOCK integration and appropriate
        response headers for CORS preflight requests.
        """
        # OPTIONS method
        self.add_method(api_id, resource_id, "OPTIONS")
        self.add_mock_integration(api_id, resource_id, "OPTIONS")

        # Method response with CORS headers
        cors_params = {
            "method.response.header.Access-Control-Allow-Origin": False,
            "method.response.header.Access-Control-Allow-Methods": False,
            "method.response.header.Access-Control-Allow-Headers": False,
        }
        self.add_method_response(
            api_id, resource_id, "OPTIONS", "200", response_parameters=cors_params
        )

        # Integration response with actual header values
        integration_params = {
            "method.response.header.Access-Control-Allow-Origin": allowed_origins,
            "method.response.header.Access-Control-Allow-Methods": allowed_methods,
            "method.response.header.Access-Control-Allow-Headers": allowed_headers,
        }
        self.add_integration_response(
            api_id, resource_id, "OPTIONS", "200", response_parameters=integration_params
        )

    def deploy_api(self, api_id: str, stage_name: str = "prod") -> ApiDeployment:
        """Create a deployment and stage for the REST API."""
        resp = self.apigateway.create_deployment(restApiId=api_id, stageName=stage_name)
        return ApiDeployment(
            rest_api_id=api_id,
            stage_name=stage_name,
            deployment_id=resp["id"],
        )

    def get_api_resources(self, api_id: str) -> list[dict]:
        """List all resources on an API."""
        resp = self.apigateway.get_resources(restApiId=api_id)
        return resp.get("items", [])

    def create_api_key(self, name: str, *, enabled: bool = True) -> str:
        """Create an API key. Returns the key ID."""
        resp = self.apigateway.create_api_key(name=name, enabled=enabled)
        key_id = resp["id"]
        self._api_keys.append(key_id)
        return key_id

    def create_usage_plan(
        self,
        name: str,
        api_id: str,
        stage_name: str,
        *,
        throttle_rate: float = 100.0,
        throttle_burst: int = 200,
        quota_limit: int = 10000,
        quota_period: str = "MONTH",
    ) -> str:
        """Create a usage plan tied to an API stage. Returns the plan ID."""
        resp = self.apigateway.create_usage_plan(
            name=name,
            apiStages=[{"apiId": api_id, "stage": stage_name}],
            throttle={"rateLimit": throttle_rate, "burstLimit": throttle_burst},
            quota={"limit": quota_limit, "period": quota_period},
        )
        plan_id = resp["id"]
        self._usage_plans.append(plan_id)
        return plan_id

    def add_api_key_to_plan(self, plan_id: str, key_id: str) -> dict:
        """Associate an API key with a usage plan."""
        return self.apigateway.create_usage_plan_key(
            usagePlanId=plan_id, keyId=key_id, keyType="API_KEY"
        )

    # ------------------------------------------------------------------ #
    #  Full endpoint wiring (convenience)                                 #
    # ------------------------------------------------------------------ #

    def wire_endpoint(
        self,
        api_id: str,
        endpoint: ApiEndpoint,
        function_arns: dict[str, str],
    ) -> str:
        """Wire a single ApiEndpoint: create resource, method, integration.

        Returns the resource ID.
        """
        root_id = self.get_root_resource_id(api_id)

        # Build resource hierarchy: /users/{id} → create /users, then /{id}
        parts = [p for p in endpoint.path.strip("/").split("/") if p]
        parent_id = root_id
        for part in parts:
            # Check if resource already exists
            existing = self.get_api_resources(api_id)
            # Build expected path up to this part
            found = False
            for r in existing:
                if r.get("pathPart") == part and r.get("parentId") == parent_id:
                    parent_id = r["id"]
                    found = True
                    break
            if not found:
                parent_id = self.create_resource(api_id, parent_id, part)

        resource_id = parent_id

        # Add method
        self.add_method(
            api_id,
            resource_id,
            endpoint.method,
            endpoint.authorization_type,
        )

        # Add Lambda integration
        fn_arn = function_arns[endpoint.handler_name]
        self.add_lambda_integration(api_id, resource_id, endpoint.method, fn_arn)

        return resource_id

    # ------------------------------------------------------------------ #
    #  Step Functions management                                          #
    # ------------------------------------------------------------------ #

    def create_state_machine(
        self,
        name: str,
        role_arn: str,
        steps: list[WorkflowStep] | None = None,
        definition_dict: dict | None = None,
    ) -> str:
        """Create a Step Functions state machine.

        Provide either `steps` (list of WorkflowStep) or `definition_dict`
        (raw ASL definition). Returns the state machine ARN.
        """
        if definition_dict is not None:
            definition = json.dumps(definition_dict)
        elif steps is not None:
            definition = json.dumps(self._build_definition(steps))
        else:
            raise ValueError("Provide either steps or definition_dict")

        resp = self.stepfunctions.create_state_machine(
            name=name, definition=definition, roleArn=role_arn
        )
        sm_arn = resp["stateMachineArn"]
        self._state_machines.append(sm_arn)
        return sm_arn

    def start_execution(
        self,
        state_machine_arn: str,
        input_data: dict | None = None,
        name: str | None = None,
    ) -> str:
        """Start a state machine execution. Returns the execution ARN."""
        kwargs: dict[str, Any] = {"stateMachineArn": state_machine_arn}
        if input_data is not None:
            kwargs["input"] = json.dumps(input_data)
        if name:
            kwargs["name"] = name
        resp = self.stepfunctions.start_execution(**kwargs)
        return resp["executionArn"]

    def describe_execution(self, execution_arn: str) -> dict:
        """Describe a state machine execution."""
        return self.stepfunctions.describe_execution(executionArn=execution_arn)

    def list_executions(self, state_machine_arn: str) -> list[dict]:
        """List executions for a state machine."""
        resp = self.stepfunctions.list_executions(stateMachineArn=state_machine_arn)
        return resp.get("executions", [])

    def _build_definition(self, steps: list[WorkflowStep]) -> dict:
        """Convert a list of WorkflowStep objects to an ASL definition."""
        states = {}
        for step in steps:
            state: dict[str, Any] = {"Type": step.type}
            if step.resource:
                state["Resource"] = step.resource
            if step.result is not None:
                state["Result"] = step.result
            if step.next:
                state["Next"] = step.next
            if step.end:
                state["End"] = True
            if step.retry:
                state["Retry"] = step.retry
            if step.catch:
                state["Catch"] = step.catch
            if step.choices:
                state["Choices"] = step.choices
            if step.default:
                state["Default"] = step.default
            if step.branches:
                state["Branches"] = step.branches
            states[step.name] = state

        return {
            "Comment": "Generated by ServerlessApp",
            "StartAt": steps[0].name,
            "States": states,
        }

    # ------------------------------------------------------------------ #
    #  Full stack deployment                                              #
    # ------------------------------------------------------------------ #

    def deploy_full_stack(
        self,
        *,
        stack_name: str,
        table_schemas: list[TableSchema],
        lambda_configs: list[LambdaConfig],
        endpoints: list[ApiEndpoint],
        stage_name: str = "prod",
    ) -> DeployedStack:
        """Deploy a complete serverless stack: tables + role + functions + API.

        Returns a DeployedStack with all resource identifiers.
        """
        # 1. Create tables
        tables = []
        for schema in table_schemas:
            self.create_table(schema)
            tables.append(schema.table_name)

        # 2. Create Lambda execution role
        role_name = f"{stack_name}-lambda-role"
        role_arn = self.create_lambda_role(role_name)

        # 3. Deploy functions
        function_arns = {}
        for config in lambda_configs:
            arn = self.deploy_function(config, role_arn)
            function_arns[config.function_name] = arn

        # 4. Create REST API
        api_id = self.create_rest_api(f"{stack_name}-api")

        # 5. Wire endpoints
        for ep in endpoints:
            self.wire_endpoint(api_id, ep, function_arns)

        # 6. Deploy
        self.deploy_api(api_id, stage_name)

        api_url = f"https://{api_id}.execute-api.{self.region}.amazonaws.com/{stage_name}"

        return DeployedStack(
            api_url=api_url,
            functions=function_arns,
            tables=tables,
            state_machines={},
            roles={role_name: role_arn},
        )

    # ------------------------------------------------------------------ #
    #  Cleanup                                                            #
    # ------------------------------------------------------------------ #

    def cleanup(self) -> None:
        """Delete all resources created by this app instance.

        Tears down in reverse dependency order: state machines, APIs,
        functions, policies, roles, tables.
        """
        errors: list[str] = []

        # State machines
        for arn in self._state_machines:
            try:
                self.stepfunctions.delete_state_machine(stateMachineArn=arn)
            except Exception as e:
                errors.append(f"state machine {arn}: {e}")

        # Usage plans (before API keys)
        for plan_id in self._usage_plans:
            try:
                self.apigateway.delete_usage_plan(usagePlanId=plan_id)
            except Exception as e:
                errors.append(f"usage plan {plan_id}: {e}")

        # API keys
        for key_id in self._api_keys:
            try:
                self.apigateway.delete_api_key(apiKey=key_id)
            except Exception as e:
                errors.append(f"api key {key_id}: {e}")

        # REST APIs
        for api_id in self._rest_apis:
            try:
                self.apigateway.delete_rest_api(restApiId=api_id)
            except Exception as e:
                errors.append(f"rest api {api_id}: {e}")

        # Lambda functions
        for fn_name in self._functions:
            try:
                self.lambda_client.delete_function(FunctionName=fn_name)
            except Exception as e:
                errors.append(f"function {fn_name}: {e}")

        # Detach policies before deleting roles
        for role_name, policy_arn in self._policies:
            try:
                self.iam.detach_role_policy(RoleName=role_name, PolicyArn=policy_arn)
            except Exception as e:
                errors.append(f"detach {policy_arn} from {role_name}: {e}")

        # IAM roles
        for role_name in self._roles:
            try:
                self.iam.delete_role(RoleName=role_name)
            except Exception as e:
                errors.append(f"role {role_name}: {e}")

        # DynamoDB tables
        for table_name in self._tables:
            try:
                self.dynamodb.delete_table(TableName=table_name)
            except Exception as e:
                errors.append(f"table {table_name}: {e}")

        # Clear tracking lists
        self._state_machines.clear()
        self._usage_plans.clear()
        self._api_keys.clear()
        self._rest_apis.clear()
        self._functions.clear()
        self._policies.clear()
        self._roles.clear()
        self._tables.clear()

        if errors:
            # Log but don't raise — cleanup is best-effort
            print(f"Cleanup warnings: {errors}")
