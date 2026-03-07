"""API Gateway compatibility tests."""

import uuid

import pytest

from tests.compatibility.conftest import make_client


def _uid():
    return uuid.uuid4().hex[:8]


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


class TestAPIGatewayModels:
    def test_create_get_delete_model(self, apigw, rest_api):
        """Create a model, retrieve it, then delete it."""
        model_name = f"Model{_uid()}"
        created = apigw.create_model(
            restApiId=rest_api,
            name=model_name,
            contentType="application/json",
            schema='{"type": "object"}',
        )
        assert created["name"] == model_name
        assert created["contentType"] == "application/json"

        got = apigw.get_model(restApiId=rest_api, modelName=model_name)
        assert got["name"] == model_name

        apigw.delete_model(restApiId=rest_api, modelName=model_name)
        # Verify deletion
        models = apigw.get_models(restApiId=rest_api)
        model_names = [m["name"] for m in models["items"]]
        assert model_name not in model_names

    def test_get_models_list(self, apigw, rest_api):
        """Create multiple models and list them."""
        names = [f"ListModel{_uid()}" for _ in range(2)]
        for name in names:
            apigw.create_model(
                restApiId=rest_api,
                name=name,
                contentType="application/json",
                schema='{"type": "string"}',
            )
        try:
            models = apigw.get_models(restApiId=rest_api)
            found_names = [m["name"] for m in models["items"]]
            for name in names:
                assert name in found_names
        finally:
            for name in names:
                apigw.delete_model(restApiId=rest_api, modelName=name)


class TestAPIGatewayDeploymentStage:
    def _setup_method_integration(self, apigw, api_id):
        """Helper: create a GET method with MOCK integration on root resource."""
        resources = apigw.get_resources(restApiId=api_id)
        root_id = [r for r in resources["items"] if r["path"] == "/"][0]["id"]
        apigw.put_method(
            restApiId=api_id,
            resourceId=root_id,
            httpMethod="GET",
            authorizationType="NONE",
        )
        apigw.put_integration(
            restApiId=api_id,
            resourceId=root_id,
            httpMethod="GET",
            type="MOCK",
            requestTemplates={"application/json": '{"statusCode": 200}'},
        )
        return root_id

    def test_create_deployment_with_stage(self, apigw):
        """Create a deployment that automatically creates a stage."""
        api = apigw.create_rest_api(name=f"deploy-api-{_uid()}")
        api_id = api["id"]
        try:
            self._setup_method_integration(apigw, api_id)
            deployment = apigw.create_deployment(
                restApiId=api_id,
                stageName="dev",
                stageDescription="Development stage",
            )
            assert "id" in deployment

            stage = apigw.get_stage(restApiId=api_id, stageName="dev")
            assert stage["stageName"] == "dev"
            assert stage["deploymentId"] == deployment["id"]
        finally:
            try:
                apigw.delete_stage(restApiId=api_id, stageName="dev")
            except Exception:
                pass
            apigw.delete_rest_api(restApiId=api_id)

    def test_update_stage(self, apigw):
        """Update a stage's description via patch operations."""
        api = apigw.create_rest_api(name=f"upd-stage-{_uid()}")
        api_id = api["id"]
        try:
            self._setup_method_integration(apigw, api_id)
            deployment = apigw.create_deployment(restApiId=api_id, stageName="staging")

            updated = apigw.update_stage(
                restApiId=api_id,
                stageName="staging",
                patchOperations=[
                    {"op": "replace", "path": "/description", "value": "Updated desc"},
                ],
            )
            assert updated["description"] == "Updated desc"
        finally:
            try:
                apigw.delete_stage(restApiId=api_id, stageName="staging")
            except Exception:
                pass
            apigw.delete_rest_api(restApiId=api_id)

    def test_delete_stage(self, apigw):
        """Delete a stage and verify it's gone."""
        api = apigw.create_rest_api(name=f"del-stage-{_uid()}")
        api_id = api["id"]
        try:
            self._setup_method_integration(apigw, api_id)
            apigw.create_deployment(restApiId=api_id, stageName="todelete")
            apigw.delete_stage(restApiId=api_id, stageName="todelete")

            stages = apigw.get_stages(restApiId=api_id)
            stage_names = [s["stageName"] for s in stages["item"]]
            assert "todelete" not in stage_names
        finally:
            apigw.delete_rest_api(restApiId=api_id)

    def test_get_stages_list(self, apigw):
        """Create multiple stages and list them."""
        api = apigw.create_rest_api(name=f"stages-list-{_uid()}")
        api_id = api["id"]
        try:
            self._setup_method_integration(apigw, api_id)
            dep1 = apigw.create_deployment(restApiId=api_id, stageName="alpha")
            dep2 = apigw.create_deployment(restApiId=api_id, stageName="beta")

            stages = apigw.get_stages(restApiId=api_id)
            stage_names = [s["stageName"] for s in stages["item"]]
            assert "alpha" in stage_names
            assert "beta" in stage_names
        finally:
            try:
                apigw.delete_stage(restApiId=api_id, stageName="alpha")
                apigw.delete_stage(restApiId=api_id, stageName="beta")
            except Exception:
                pass
            apigw.delete_rest_api(restApiId=api_id)


class TestAPIGatewayKeys:
    def test_create_get_delete_api_key(self, apigw):
        """Full lifecycle of an API key."""
        key_name = f"key-{_uid()}"
        created = apigw.create_api_key(name=key_name, enabled=True)
        key_id = created["id"]
        assert created["name"] == key_name

        try:
            got = apigw.get_api_key(apiKey=key_id)
            assert got["name"] == key_name
            assert got["enabled"] is True
        finally:
            apigw.delete_api_key(apiKey=key_id)

        # Verify deletion
        keys = apigw.get_api_keys()
        key_ids = [k["id"] for k in keys["items"]]
        assert key_id not in key_ids

    def test_api_key_with_value(self, apigw):
        """Create an API key with a specified value."""
        key_name = f"val-key-{_uid()}"
        key_value = f"myCustomKeyValue{_uid()}"
        created = apigw.create_api_key(
            name=key_name, enabled=True, value=key_value
        )
        try:
            got = apigw.get_api_key(apiKey=created["id"], includeValue=True)
            assert got["value"] == key_value
        finally:
            apigw.delete_api_key(apiKey=created["id"])


class TestAPIGatewayUsagePlans:
    def test_create_usage_plan_with_throttle_and_quota(self, apigw):
        """Create a usage plan with throttle and quota settings."""
        plan_name = f"plan-{_uid()}"
        plan = apigw.create_usage_plan(
            name=plan_name,
            throttle={"burstLimit": 200, "rateLimit": 100.0},
            quota={"limit": 10000, "period": "MONTH"},
        )
        try:
            assert plan["name"] == plan_name
            assert plan["throttle"]["burstLimit"] == 200
            assert plan["throttle"]["rateLimit"] == 100.0
            assert plan["quota"]["limit"] == 10000
            assert plan["quota"]["period"] == "MONTH"
        finally:
            apigw.delete_usage_plan(usagePlanId=plan["id"])

    def test_get_usage_plans(self, apigw):
        """List usage plans."""
        plan_name = f"list-plan-{_uid()}"
        plan = apigw.create_usage_plan(name=plan_name)
        try:
            plans = apigw.get_usage_plans()
            plan_ids = [p["id"] for p in plans["items"]]
            assert plan["id"] in plan_ids
        finally:
            apigw.delete_usage_plan(usagePlanId=plan["id"])


class TestAPIGatewayMethodResponse:
    def test_put_method_response(self, apigw, rest_api):
        """Create a method response for a given status code."""
        resources = apigw.get_resources(restApiId=rest_api)
        root_id = [r for r in resources["items"] if r["path"] == "/"][0]["id"]
        resource = apigw.create_resource(
            restApiId=rest_api, parentId=root_id, pathPart=f"resp-{_uid()}"
        )
        apigw.put_method(
            restApiId=rest_api,
            resourceId=resource["id"],
            httpMethod="GET",
            authorizationType="NONE",
        )
        resp = apigw.put_method_response(
            restApiId=rest_api,
            resourceId=resource["id"],
            httpMethod="GET",
            statusCode="200",
            responseModels={"application/json": "Empty"},
        )
        assert resp["statusCode"] == "200"

    def test_put_integration_response(self, apigw, rest_api):
        """Create an integration response."""
        resources = apigw.get_resources(restApiId=rest_api)
        root_id = [r for r in resources["items"] if r["path"] == "/"][0]["id"]
        resource = apigw.create_resource(
            restApiId=rest_api, parentId=root_id, pathPart=f"intresp-{_uid()}"
        )
        apigw.put_method(
            restApiId=rest_api,
            resourceId=resource["id"],
            httpMethod="POST",
            authorizationType="NONE",
        )
        apigw.put_integration(
            restApiId=rest_api,
            resourceId=resource["id"],
            httpMethod="POST",
            type="MOCK",
            requestTemplates={"application/json": '{"statusCode": 200}'},
        )
        apigw.put_method_response(
            restApiId=rest_api,
            resourceId=resource["id"],
            httpMethod="POST",
            statusCode="200",
        )
        resp = apigw.put_integration_response(
            restApiId=rest_api,
            resourceId=resource["id"],
            httpMethod="POST",
            statusCode="200",
            responseTemplates={"application/json": ""},
        )
        assert resp["statusCode"] == "200"
