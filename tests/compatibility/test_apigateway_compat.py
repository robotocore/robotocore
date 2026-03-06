"""API Gateway compatibility tests."""

import pytest

from tests.compatibility.conftest import make_client


@pytest.fixture
def apigw():
    return make_client("apigateway")


@pytest.fixture
def rest_api(apigw):
    response = apigw.create_rest_api(name="test-api", description="Test API")
    api_id = response["id"]
    yield api_id
    apigw.delete_rest_api(restApiId=api_id)


class TestAPIGatewayOperations:
    def test_create_rest_api(self, apigw):
        response = apigw.create_rest_api(name="create-test", description="Test")
        assert "id" in response
        assert response["name"] == "create-test"
        apigw.delete_rest_api(restApiId=response["id"])

    def test_get_rest_api(self, apigw, rest_api):
        response = apigw.get_rest_api(restApiId=rest_api)
        assert response["name"] == "test-api"

    def test_get_rest_apis(self, apigw, rest_api):
        response = apigw.get_rest_apis()
        ids = [api["id"] for api in response["items"]]
        assert rest_api in ids

    def test_get_resources(self, apigw, rest_api):
        response = apigw.get_resources(restApiId=rest_api)
        assert len(response["items"]) >= 1
        # Root resource exists
        root = [r for r in response["items"] if r["path"] == "/"]
        assert len(root) == 1

    def test_create_resource(self, apigw, rest_api):
        resources = apigw.get_resources(restApiId=rest_api)
        root_id = [r for r in resources["items"] if r["path"] == "/"][0]["id"]
        response = apigw.create_resource(restApiId=rest_api, parentId=root_id, pathPart="users")
        assert response["pathPart"] == "users"

    def test_put_method(self, apigw, rest_api):
        resources = apigw.get_resources(restApiId=rest_api)
        root_id = [r for r in resources["items"] if r["path"] == "/"][0]["id"]
        resource = apigw.create_resource(restApiId=rest_api, parentId=root_id, pathPart="items")
        response = apigw.put_method(
            restApiId=rest_api,
            resourceId=resource["id"],
            httpMethod="GET",
            authorizationType="NONE",
        )
        assert response["httpMethod"] == "GET"

    def test_delete_rest_api(self, apigw):
        api = apigw.create_rest_api(name="delete-me")
        apigw.delete_rest_api(restApiId=api["id"])
        apis = apigw.get_rest_apis()
        ids = [a["id"] for a in apis["items"]]
        assert api["id"] not in ids

    def test_create_stage(self, apigw, rest_api):
        """Create a deployment and then a named stage pointing to it."""
        # Need at least one method for deployment to succeed in some impls,
        # but moto allows empty deployments.
        resources = apigw.get_resources(restApiId=rest_api)
        root_id = [r for r in resources["items"] if r["path"] == "/"][0]["id"]
        apigw.put_method(
            restApiId=rest_api,
            resourceId=root_id,
            httpMethod="GET",
            authorizationType="NONE",
        )
        apigw.put_integration(
            restApiId=rest_api,
            resourceId=root_id,
            httpMethod="GET",
            type="MOCK",
            requestTemplates={"application/json": '{"statusCode": 200}'},
        )

        deployment = apigw.create_deployment(restApiId=rest_api)
        assert "id" in deployment

        stage = apigw.create_stage(
            restApiId=rest_api,
            stageName="prod",
            deploymentId=deployment["id"],
        )
        assert stage["stageName"] == "prod"
        assert stage["deploymentId"] == deployment["id"]

        # Verify we can retrieve the stage
        got = apigw.get_stage(restApiId=rest_api, stageName="prod")
        assert got["stageName"] == "prod"

    def test_get_rest_apis_multiple(self, apigw):
        """Create multiple APIs and verify they all appear in list."""
        created_ids = []
        for i in range(3):
            resp = apigw.create_rest_api(name=f"multi-api-{i}", description=f"API {i}")
            created_ids.append(resp["id"])

        try:
            response = apigw.get_rest_apis()
            listed_ids = [api["id"] for api in response["items"]]
            for api_id in created_ids:
                assert api_id in listed_ids
        finally:
            for api_id in created_ids:
                apigw.delete_rest_api(restApiId=api_id)

    def test_put_mock_integration(self, apigw, rest_api):
        """Create a MOCK integration and verify its configuration."""
        resources = apigw.get_resources(restApiId=rest_api)
        root_id = [r for r in resources["items"] if r["path"] == "/"][0]["id"]

        resource = apigw.create_resource(restApiId=rest_api, parentId=root_id, pathPart="mock")
        apigw.put_method(
            restApiId=rest_api,
            resourceId=resource["id"],
            httpMethod="GET",
            authorizationType="NONE",
        )
        apigw.put_integration(
            restApiId=rest_api,
            resourceId=resource["id"],
            httpMethod="GET",
            type="MOCK",
            requestTemplates={"application/json": '{"statusCode": 200}'},
        )

        integration = apigw.get_integration(
            restApiId=rest_api,
            resourceId=resource["id"],
            httpMethod="GET",
        )
        assert integration["type"] == "MOCK"

    def test_api_key(self, apigw):
        """Create and list API keys."""
        key = apigw.create_api_key(name="test-key", enabled=True)
        assert "id" in key
        assert key["name"] == "test-key"
        assert key["enabled"] is True

        try:
            keys = apigw.get_api_keys()
            key_ids = [k["id"] for k in keys["items"]]
            assert key["id"] in key_ids
        finally:
            apigw.delete_api_key(apiKey=key["id"])

    def test_usage_plan(self, apigw, rest_api):
        """Create a usage plan and associate it with an API key."""
        # Create a deployment + stage first (usage plans reference stages)
        resources = apigw.get_resources(restApiId=rest_api)
        root_id = [r for r in resources["items"] if r["path"] == "/"][0]["id"]
        apigw.put_method(
            restApiId=rest_api,
            resourceId=root_id,
            httpMethod="GET",
            authorizationType="NONE",
        )
        apigw.put_integration(
            restApiId=rest_api,
            resourceId=root_id,
            httpMethod="GET",
            type="MOCK",
            requestTemplates={"application/json": '{"statusCode": 200}'},
        )
        apigw.create_deployment(restApiId=rest_api, stageName="test")

        # Create usage plan linked to the API stage
        plan = apigw.create_usage_plan(
            name="test-plan",
            throttle={"burstLimit": 100, "rateLimit": 50.0},
            apiStages=[{"apiId": rest_api, "stage": "test"}],
        )
        assert "id" in plan
        assert plan["name"] == "test-plan"

        # Create API key and associate with usage plan
        key = apigw.create_api_key(name="plan-key", enabled=True)
        apigw.create_usage_plan_key(
            usagePlanId=plan["id"],
            keyId=key["id"],
            keyType="API_KEY",
        )

        # Verify usage plan key is listed
        plan_keys = apigw.get_usage_plan_keys(usagePlanId=plan["id"])
        key_ids = [k["id"] for k in plan_keys["items"]]
        assert key["id"] in key_ids

        # Clean up
        apigw.delete_usage_plan_key(usagePlanId=plan["id"], keyId=key["id"])
        apigw.delete_api_key(apiKey=key["id"])
        apigw.delete_usage_plan(usagePlanId=plan["id"])
