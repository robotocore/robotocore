"""
API Gateway REST API management tests.

Tests API creation, resource hierarchy, method configuration,
Lambda integration, deployments, API keys, usage plans, and CORS.
"""

from .handlers import HELLO_HANDLER
from .models import LambdaConfig


class TestApiGatewayOperations:
    """API Gateway routing layer tests."""

    def test_create_rest_api_with_resources(self, serverless_app, unique_name):
        """Create a REST API with nested resources."""
        api_id = serverless_app.create_rest_api(f"test-api-{unique_name}", "Test API for resources")
        root_id = serverless_app.get_root_resource_id(api_id)
        assert root_id

        # Create /users
        users_id = serverless_app.create_resource(api_id, root_id, "users")
        assert users_id

        # Create /users/{id}
        user_id_resource = serverless_app.create_resource(api_id, users_id, "{id}")
        assert user_id_resource

        # Verify resource tree
        resources = serverless_app.get_api_resources(api_id)
        paths = {r["path"] for r in resources}
        assert "/" in paths
        assert "/users" in paths
        assert "/users/{id}" in paths

    def test_create_deployment_and_stage(self, serverless_app, unique_name):
        """Create an API, add a method, deploy to a stage."""
        api_id = serverless_app.create_rest_api(f"deploy-api-{unique_name}")
        root_id = serverless_app.get_root_resource_id(api_id)

        serverless_app.add_method(api_id, root_id, "GET")
        # Need an integration for deployment
        serverless_app.add_mock_integration(api_id, root_id, "GET")

        deployment = serverless_app.deploy_api(api_id, "staging")
        assert deployment.rest_api_id == api_id
        assert deployment.stage_name == "staging"
        assert deployment.deployment_id

    def test_lambda_integration(self, serverless_app, unique_name):
        """Wire a Lambda function as a proxy integration."""
        role_arn = serverless_app.create_lambda_role(f"apigw-role-{unique_name}")
        config = LambdaConfig(
            function_name=f"apigw-handler-{unique_name}",
            handler="index.handler",
            runtime="python3.12",
            code=HELLO_HANDLER,
        )
        fn_arn = serverless_app.deploy_function(config, role_arn)

        api_id = serverless_app.create_rest_api(f"lambda-api-{unique_name}")
        root_id = serverless_app.get_root_resource_id(api_id)
        users_id = serverless_app.create_resource(api_id, root_id, "users")

        serverless_app.add_method(api_id, users_id, "GET")
        resp = serverless_app.add_lambda_integration(api_id, users_id, "GET", fn_arn)
        assert resp["type"] == "AWS_PROXY"

    def test_api_key_and_usage_plan(self, serverless_app, unique_name):
        """Create an API key and usage plan, associate them."""
        api_id = serverless_app.create_rest_api(f"key-api-{unique_name}")
        root_id = serverless_app.get_root_resource_id(api_id)
        serverless_app.add_method(api_id, root_id, "GET", api_key_required=True)
        serverless_app.add_mock_integration(api_id, root_id, "GET")

        serverless_app.deploy_api(api_id, "prod")

        key_id = serverless_app.create_api_key(f"test-key-{unique_name}")
        assert key_id

        plan_id = serverless_app.create_usage_plan(
            f"test-plan-{unique_name}",
            api_id,
            "prod",
            throttle_rate=50.0,
            throttle_burst=100,
            quota_limit=5000,
        )
        assert plan_id

        assoc = serverless_app.add_api_key_to_plan(plan_id, key_id)
        assert assoc["id"] == key_id

    def test_multiple_resources_same_api(self, serverless_app, unique_name):
        """Create multiple sibling resources on the same API."""
        api_id = serverless_app.create_rest_api(f"multi-api-{unique_name}")
        root_id = serverless_app.get_root_resource_id(api_id)

        resources_created = []
        for path_part in ["users", "orders", "products", "health"]:
            rid = serverless_app.create_resource(api_id, root_id, path_part)
            resources_created.append(rid)
            serverless_app.add_method(api_id, rid, "GET")
            serverless_app.add_mock_integration(api_id, rid, "GET")

        assert len(resources_created) == 4

        resources = serverless_app.get_api_resources(api_id)
        paths = {r["path"] for r in resources}
        assert "/users" in paths
        assert "/orders" in paths
        assert "/products" in paths
        assert "/health" in paths

    def test_cors_configuration(self, serverless_app, unique_name):
        """Configure CORS on a resource with OPTIONS method."""
        api_id = serverless_app.create_rest_api(f"cors-api-{unique_name}")
        root_id = serverless_app.get_root_resource_id(api_id)
        users_id = serverless_app.create_resource(api_id, root_id, "users")

        # Add the main method
        serverless_app.add_method(api_id, users_id, "GET")
        serverless_app.add_mock_integration(api_id, users_id, "GET")

        # Configure CORS
        serverless_app.configure_cors(api_id, users_id)

        # Verify OPTIONS method exists
        resources = serverless_app.get_api_resources(api_id)
        users_resource = next(r for r in resources if r.get("path") == "/users")
        methods = users_resource.get("resourceMethods", {})
        assert "OPTIONS" in methods
        assert "GET" in methods
