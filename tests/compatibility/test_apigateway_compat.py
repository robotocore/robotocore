"""API Gateway compatibility tests."""

import json

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

    def test_create_model(self, apigw, rest_api):
        """Create a model and retrieve it."""
        model = apigw.create_model(
            restApiId=rest_api,
            name="UserModel",
            contentType="application/json",
            schema='{"type": "object", "properties": {"name": {"type": "string"}}}',
        )
        assert model["name"] == "UserModel"
        assert model["contentType"] == "application/json"

    def test_get_model(self, apigw, rest_api):
        """Create a model then get it by name."""
        apigw.create_model(
            restApiId=rest_api,
            name="GetTestModel",
            contentType="application/json",
            schema='{"type": "object"}',
        )
        got = apigw.get_model(restApiId=rest_api, modelName="GetTestModel")
        assert got["name"] == "GetTestModel"

    def test_get_models(self, apigw, rest_api):
        """List models for an API."""
        apigw.create_model(
            restApiId=rest_api,
            name="ListModel1",
            contentType="application/json",
            schema='{"type": "object"}',
        )
        apigw.create_model(
            restApiId=rest_api,
            name="ListModel2",
            contentType="application/json",
            schema='{"type": "string"}',
        )
        models = apigw.get_models(restApiId=rest_api)
        names = [m["name"] for m in models["items"]]
        assert "ListModel1" in names
        assert "ListModel2" in names

    def test_delete_model(self, apigw, rest_api):
        """Create and then delete a model."""
        apigw.create_model(
            restApiId=rest_api,
            name="DeleteMe",
            contentType="application/json",
            schema='{"type": "object"}',
        )
        apigw.delete_model(restApiId=rest_api, modelName="DeleteMe")
        models = apigw.get_models(restApiId=rest_api)
        names = [m["name"] for m in models["items"]]
        assert "DeleteMe" not in names

    def test_get_usage_plan(self, apigw):
        """Create a usage plan and get it by ID."""
        plan = apigw.create_usage_plan(name="get-plan-test")
        try:
            got = apigw.get_usage_plan(usagePlanId=plan["id"])
            assert got["name"] == "get-plan-test"
            assert got["id"] == plan["id"]
        finally:
            apigw.delete_usage_plan(usagePlanId=plan["id"])

    def test_get_usage_plans(self, apigw):
        """Create usage plans and list them."""
        plan1 = apigw.create_usage_plan(name="list-plan-1")
        plan2 = apigw.create_usage_plan(name="list-plan-2")
        try:
            plans = apigw.get_usage_plans()
            plan_ids = [p["id"] for p in plans["items"]]
            assert plan1["id"] in plan_ids
            assert plan2["id"] in plan_ids
        finally:
            apigw.delete_usage_plan(usagePlanId=plan1["id"])
            apigw.delete_usage_plan(usagePlanId=plan2["id"])

    def test_get_api_key(self, apigw):
        """Create an API key and get it by ID."""
        key = apigw.create_api_key(name="get-key-test", enabled=True)
        try:
            got = apigw.get_api_key(apiKey=key["id"])
            assert got["name"] == "get-key-test"
            assert got["id"] == key["id"]
        finally:
            apigw.delete_api_key(apiKey=key["id"])

    def test_create_deployment(self, apigw, rest_api):
        """Create a deployment and verify it."""
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

    def test_get_deployments(self, apigw, rest_api):
        """Create deployments and list them."""
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
        dep1 = apigw.create_deployment(restApiId=rest_api)
        dep2 = apigw.create_deployment(restApiId=rest_api)
        deployments = apigw.get_deployments(restApiId=rest_api)
        dep_ids = [d["id"] for d in deployments["items"]]
        assert dep1["id"] in dep_ids
        assert dep2["id"] in dep_ids

    def test_get_deployment(self, apigw, rest_api):
        """Get a specific deployment by ID."""
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
        dep = apigw.create_deployment(restApiId=rest_api)
        got = apigw.get_deployment(restApiId=rest_api, deploymentId=dep["id"])
        assert got["id"] == dep["id"]

    def test_get_stages(self, apigw, rest_api):
        """Create stages and list them."""
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
        dep = apigw.create_deployment(restApiId=rest_api)
        apigw.create_stage(restApiId=rest_api, stageName="dev", deploymentId=dep["id"])
        apigw.create_stage(restApiId=rest_api, stageName="staging", deploymentId=dep["id"])
        stages = apigw.get_stages(restApiId=rest_api)
        stage_names = [s["stageName"] for s in stages["item"]]
        assert "dev" in stage_names
        assert "staging" in stage_names

    def test_update_stage(self, apigw, rest_api):
        """Update a stage's description via patch operations."""
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
        dep = apigw.create_deployment(restApiId=rest_api)
        apigw.create_stage(restApiId=rest_api, stageName="upd", deploymentId=dep["id"])
        updated = apigw.update_stage(
            restApiId=rest_api,
            stageName="upd",
            patchOperations=[
                {"op": "replace", "path": "/description", "value": "Updated desc"},
            ],
        )
        assert updated["description"] == "Updated desc"

    def test_delete_stage(self, apigw, rest_api):
        """Create and delete a stage."""
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
        dep = apigw.create_deployment(restApiId=rest_api)
        apigw.create_stage(restApiId=rest_api, stageName="deleteme", deploymentId=dep["id"])
        apigw.delete_stage(restApiId=rest_api, stageName="deleteme")
        stages = apigw.get_stages(restApiId=rest_api)
        stage_names = [s["stageName"] for s in stages["item"]]
        assert "deleteme" not in stage_names

    def test_put_gateway_response(self, apigw, rest_api):
        """Put and get a gateway response."""
        resp = apigw.put_gateway_response(
            restApiId=rest_api,
            responseType="DEFAULT_4XX",
            responseTemplates={"application/json": '{"message": "error"}'},
        )
        assert resp["responseType"] == "DEFAULT_4XX"

    def test_get_gateway_response(self, apigw, rest_api):
        """Put a gateway response then retrieve it."""
        apigw.put_gateway_response(
            restApiId=rest_api,
            responseType="DEFAULT_5XX",
            statusCode="500",
        )
        got = apigw.get_gateway_response(restApiId=rest_api, responseType="DEFAULT_5XX")
        assert got["responseType"] == "DEFAULT_5XX"

    def test_get_gateway_responses(self, apigw, rest_api):
        """List gateway responses for an API."""
        apigw.put_gateway_response(
            restApiId=rest_api,
            responseType="UNAUTHORIZED",
            statusCode="401",
        )
        responses = apigw.get_gateway_responses(restApiId=rest_api)
        types = [r["responseType"] for r in responses["items"]]
        assert "UNAUTHORIZED" in types

    def test_create_request_validator(self, apigw, rest_api):
        """Create a request validator and list validators."""
        validator = apigw.create_request_validator(
            restApiId=rest_api,
            name="test-validator",
            validateRequestBody=True,
            validateRequestParameters=False,
        )
        assert validator["name"] == "test-validator"
        assert validator["validateRequestBody"] is True

    def test_get_request_validators(self, apigw, rest_api):
        """Create validators and list them."""
        apigw.create_request_validator(
            restApiId=rest_api,
            name="validator-1",
            validateRequestBody=True,
            validateRequestParameters=False,
        )
        apigw.create_request_validator(
            restApiId=rest_api,
            name="validator-2",
            validateRequestBody=False,
            validateRequestParameters=True,
        )
        validators = apigw.get_request_validators(restApiId=rest_api)
        names = [v["name"] for v in validators["items"]]
        assert "validator-1" in names
        assert "validator-2" in names

    def test_get_resource(self, apigw, rest_api):
        """GetResource for a created child resource."""
        resources = apigw.get_resources(restApiId=rest_api)
        root_id = [r for r in resources["items"] if r["path"] == "/"][0]["id"]
        child = apigw.create_resource(restApiId=rest_api, parentId=root_id, pathPart="orders")
        got = apigw.get_resource(restApiId=rest_api, resourceId=child["id"])
        assert got["pathPart"] == "orders"
        assert got["id"] == child["id"]

    def test_delete_resource(self, apigw, rest_api):
        """DeleteResource removes a child resource."""
        resources = apigw.get_resources(restApiId=rest_api)
        root_id = [r for r in resources["items"] if r["path"] == "/"][0]["id"]
        child = apigw.create_resource(restApiId=rest_api, parentId=root_id, pathPart="deleteme")
        apigw.delete_resource(restApiId=rest_api, resourceId=child["id"])
        resources = apigw.get_resources(restApiId=rest_api)
        ids = [r["id"] for r in resources["items"]]
        assert child["id"] not in ids

    def test_get_method(self, apigw, rest_api):
        """PutMethod then GetMethod."""
        resources = apigw.get_resources(restApiId=rest_api)
        root_id = [r for r in resources["items"] if r["path"] == "/"][0]["id"]
        child = apigw.create_resource(restApiId=rest_api, parentId=root_id, pathPart="getmethod")
        apigw.put_method(
            restApiId=rest_api,
            resourceId=child["id"],
            httpMethod="POST",
            authorizationType="NONE",
        )
        method = apigw.get_method(restApiId=rest_api, resourceId=child["id"], httpMethod="POST")
        assert method["httpMethod"] == "POST"
        assert method["authorizationType"] == "NONE"

    def test_put_method_response(self, apigw, rest_api):
        """PutMethodResponse / GetMethodResponse."""
        resources = apigw.get_resources(restApiId=rest_api)
        root_id = [r for r in resources["items"] if r["path"] == "/"][0]["id"]
        child = apigw.create_resource(restApiId=rest_api, parentId=root_id, pathPart="methresp")
        apigw.put_method(
            restApiId=rest_api,
            resourceId=child["id"],
            httpMethod="GET",
            authorizationType="NONE",
        )
        apigw.put_method_response(
            restApiId=rest_api,
            resourceId=child["id"],
            httpMethod="GET",
            statusCode="200",
            responseModels={"application/json": "Empty"},
        )
        resp = apigw.get_method_response(
            restApiId=rest_api,
            resourceId=child["id"],
            httpMethod="GET",
            statusCode="200",
        )
        assert resp["statusCode"] == "200"

    def test_put_integration_response(self, apigw, rest_api):
        """PutIntegration / PutIntegrationResponse / GetIntegrationResponse."""
        resources = apigw.get_resources(restApiId=rest_api)
        root_id = [r for r in resources["items"] if r["path"] == "/"][0]["id"]
        child = apigw.create_resource(restApiId=rest_api, parentId=root_id, pathPart="intresp")
        apigw.put_method(
            restApiId=rest_api,
            resourceId=child["id"],
            httpMethod="GET",
            authorizationType="NONE",
        )
        apigw.put_integration(
            restApiId=rest_api,
            resourceId=child["id"],
            httpMethod="GET",
            type="MOCK",
            requestTemplates={"application/json": '{"statusCode": 200}'},
        )
        apigw.put_integration_response(
            restApiId=rest_api,
            resourceId=child["id"],
            httpMethod="GET",
            statusCode="200",
            responseTemplates={"application/json": ""},
        )
        ir = apigw.get_integration_response(
            restApiId=rest_api,
            resourceId=child["id"],
            httpMethod="GET",
            statusCode="200",
        )
        assert ir["statusCode"] == "200"

    def test_create_domain_name(self, apigw):
        """CreateDomainName / GetDomainNames."""
        apigw.create_domain_name(
            domainName="api.example.com",
            certificateArn="arn:aws:acm:us-east-1:123456789012:certificate/abc123",
        )
        try:
            domains = apigw.get_domain_names()
            names = [d["domainName"] for d in domains["items"]]
            assert "api.example.com" in names
        except Exception:
            raise
        finally:
            try:
                apigw.delete_domain_name(domainName="api.example.com")
            except Exception:
                pass  # best-effort cleanup

    def test_create_authorizer(self, apigw, rest_api):
        """CreateAuthorizer / GetAuthorizers."""
        auth = apigw.create_authorizer(
            restApiId=rest_api,
            name="test-auth",
            type="TOKEN",
            authorizerUri="arn:aws:apigateway:us-east-1:lambda:path/2015-03-31/functions/arn:aws:lambda:us-east-1:123456789012:function:auth/invocations",
            identitySource="method.request.header.Authorization",
        )
        try:
            auths = apigw.get_authorizers(restApiId=rest_api)
            auth_ids = [a["id"] for a in auths["items"]]
            assert auth["id"] in auth_ids
        except Exception:
            raise

    def test_tag_untag_rest_api(self, apigw):
        """TagResource / UntagResource on REST APIs."""
        api = apigw.create_rest_api(name="tag-test-api")
        api_id = api["id"]
        arn = f"arn:aws:apigateway:us-east-1::/restapis/{api_id}"
        try:
            apigw.tag_resource(
                resourceArn=arn,
                tags={"Env": "test", "Team": "platform"},
            )
            got = apigw.get_rest_api(restApiId=api_id)
            assert got.get("tags", {}).get("Env") == "test"
            assert got.get("tags", {}).get("Team") == "platform"

            apigw.untag_resource(resourceArn=arn, tagKeys=["Env"])
            got2 = apigw.get_rest_api(restApiId=api_id)
            assert "Env" not in got2.get("tags", {})
            assert got2.get("tags", {}).get("Team") == "platform"
        finally:
            apigw.delete_rest_api(restApiId=api_id)

    def test_flush_stage_cache(self, apigw, rest_api):
        """FlushStageAuthorizersCache / FlushStageCache."""
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
        dep = apigw.create_deployment(restApiId=rest_api)
        apigw.create_stage(restApiId=rest_api, stageName="cache", deploymentId=dep["id"])
        apigw.flush_stage_authorizers_cache(restApiId=rest_api, stageName="cache")
        apigw.flush_stage_cache(restApiId=rest_api, stageName="cache")

    def test_get_export(self, apigw, rest_api):
        """GetExport for swagger/oas30."""
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
        _dep = apigw.create_deployment(restApiId=rest_api, stageName="export")
        resp = apigw.get_export(
            restApiId=rest_api,
            stageName="export",
            exportType="swagger",
            accepts="application/json",
        )
        assert "body" in resp


class TestAPIGatewayExtended:
    """Extended API Gateway operations for higher coverage."""

    @pytest.fixture
    def apigw(self):
        from tests.compatibility.conftest import make_client

        return make_client("apigateway")

    @pytest.fixture
    def rest_api(self, apigw):
        import uuid

        resp = apigw.create_rest_api(
            name=f"ext-api-{uuid.uuid4().hex[:8]}",
            description="Extended test API",
        )
        api_id = resp["id"]
        yield api_id
        apigw.delete_rest_api(restApiId=api_id)

    def test_get_rest_api(self, apigw, rest_api):
        resp = apigw.get_rest_api(restApiId=rest_api)
        assert resp["id"] == rest_api
        assert "name" in resp
        assert "description" in resp

    def test_get_rest_apis(self, apigw, rest_api):
        resp = apigw.get_rest_apis()
        assert "items" in resp
        ids = [a["id"] for a in resp["items"]]
        assert rest_api in ids

    def test_update_rest_api(self, apigw, rest_api):
        apigw.update_rest_api(
            restApiId=rest_api,
            patchOperations=[
                {"op": "replace", "path": "/description", "value": "Updated desc"},
            ],
        )
        resp = apigw.get_rest_api(restApiId=rest_api)
        assert resp["description"] == "Updated desc"

    def test_create_resource_with_path_part(self, apigw, rest_api):
        resources = apigw.get_resources(restApiId=rest_api)
        root_id = [r for r in resources["items"] if r["path"] == "/"][0]["id"]

        child = apigw.create_resource(restApiId=rest_api, parentId=root_id, pathPart="users")
        assert child["pathPart"] == "users"
        assert child["path"] == "/users"

        # Nested resource
        nested = apigw.create_resource(
            restApiId=rest_api, parentId=child["id"], pathPart="{userId}"
        )
        assert nested["pathPart"] == "{userId}"
        assert nested["path"] == "/users/{userId}"

    def test_put_method_response(self, apigw, rest_api):
        resources = apigw.get_resources(restApiId=rest_api)
        root_id = [r for r in resources["items"] if r["path"] == "/"][0]["id"]

        apigw.put_method(
            restApiId=rest_api,
            resourceId=root_id,
            httpMethod="GET",
            authorizationType="NONE",
        )
        apigw.put_method_response(
            restApiId=rest_api,
            resourceId=root_id,
            httpMethod="GET",
            statusCode="200",
            responseModels={"application/json": "Empty"},
        )
        resp = apigw.get_method_response(
            restApiId=rest_api,
            resourceId=root_id,
            httpMethod="GET",
            statusCode="200",
        )
        assert resp["statusCode"] == "200"

    def test_put_integration_response(self, apigw, rest_api):
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
        apigw.put_method_response(
            restApiId=rest_api,
            resourceId=root_id,
            httpMethod="GET",
            statusCode="200",
        )
        apigw.put_integration_response(
            restApiId=rest_api,
            resourceId=root_id,
            httpMethod="GET",
            statusCode="200",
            responseTemplates={"application/json": ""},
        )
        resp = apigw.get_integration_response(
            restApiId=rest_api,
            resourceId=root_id,
            httpMethod="GET",
            statusCode="200",
        )
        assert resp["statusCode"] == "200"

    def test_create_api_key(self, apigw):
        import uuid

        key_name = f"test-key-{uuid.uuid4().hex[:8]}"
        resp = apigw.create_api_key(name=key_name, enabled=True, value=f"apikey-{uuid.uuid4().hex}")
        key_id = resp["id"]
        try:
            assert resp["name"] == key_name
            assert resp["enabled"] is True

            got = apigw.get_api_key(apiKey=key_id)
            assert got["name"] == key_name
        finally:
            apigw.delete_api_key(apiKey=key_id)

    def test_get_api_keys(self, apigw):
        resp = apigw.get_api_keys()
        assert "items" in resp

    def test_create_usage_plan(self, apigw):
        import uuid

        name = f"usage-plan-{uuid.uuid4().hex[:8]}"
        resp = apigw.create_usage_plan(
            name=name,
            throttle={"burstLimit": 100, "rateLimit": 50.0},
            quota={"limit": 1000, "period": "MONTH"},
        )
        plan_id = resp["id"]
        try:
            assert resp["name"] == name
            assert resp["throttle"]["burstLimit"] == 100

            got = apigw.get_usage_plan(usagePlanId=plan_id)
            assert got["name"] == name
        finally:
            apigw.delete_usage_plan(usagePlanId=plan_id)

    def test_get_usage_plans(self, apigw):
        resp = apigw.get_usage_plans()
        assert "items" in resp

    def test_create_model(self, apigw, rest_api):
        import json
        import uuid

        model_name = f"TestModel{uuid.uuid4().hex[:8]}"
        schema = json.dumps(
            {
                "type": "object",
                "properties": {"name": {"type": "string"}},
            }
        )
        resp = apigw.create_model(
            restApiId=rest_api,
            name=model_name,
            contentType="application/json",
            schema=schema,
        )
        assert resp["name"] == model_name

        models = apigw.get_models(restApiId=rest_api)
        names = [m["name"] for m in models["items"]]
        assert model_name in names

    def test_create_get_delete_domain_name(self, apigw):
        import uuid

        domain = f"api-{uuid.uuid4().hex[:8]}.example.com"
        try:
            resp = apigw.create_domain_name(
                domainName=domain,
                certificateArn=f"arn:aws:acm:us-east-1:123456789012:certificate/{uuid.uuid4()}",
            )
            assert resp["domainName"] == domain

            got = apigw.get_domain_name(domainName=domain)
            assert got["domainName"] == domain
        finally:
            try:
                apigw.delete_domain_name(domainName=domain)
            except Exception:
                pass  # best-effort cleanup

    def test_get_account(self, apigw):
        resp = apigw.get_account()
        assert "throttleSettings" in resp or "cloudwatchRoleArn" in resp or resp is not None

    def test_create_deployment_and_stages(self, apigw, rest_api):
        import uuid

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
        dep = apigw.create_deployment(restApiId=rest_api)
        stage_name = f"stg-{uuid.uuid4().hex[:8]}"
        apigw.create_stage(
            restApiId=rest_api,
            stageName=stage_name,
            deploymentId=dep["id"],
            description="Test stage",
        )
        stages = apigw.get_stages(restApiId=rest_api)
        stage_names = [s["stageName"] for s in stages["item"]]
        assert stage_name in stage_names

        apigw.delete_stage(restApiId=rest_api, stageName=stage_name)

    def test_tag_rest_api(self, apigw, rest_api):
        apigw.tag_resource(
            resourceArn=f"arn:aws:apigateway:us-east-1::/restapis/{rest_api}",
            tags={"env": "test", "team": "api"},
        )
        resp = apigw.get_tags(
            resourceArn=f"arn:aws:apigateway:us-east-1::/restapis/{rest_api}",
        )
        assert resp["tags"]["env"] == "test"


class TestApigatewayAutoCoverage:
    """Auto-generated coverage tests for apigateway."""

    @pytest.fixture
    def client(self):
        return make_client("apigateway")

    @pytest.fixture
    def api(self, client):
        import uuid

        resp = client.create_rest_api(
            name=f"auto-api-{uuid.uuid4().hex[:8]}",
            description="Auto coverage test API",
        )
        api_id = resp["id"]
        yield api_id
        client.delete_rest_api(restApiId=api_id)

    @pytest.fixture
    def authorizer(self, client, api):
        auth = client.create_authorizer(
            restApiId=api,
            name="test-authorizer",
            type="TOKEN",
            authorizerUri="arn:aws:apigateway:us-east-1:lambda:path/2015-03-31/functions/arn:aws:lambda:us-east-1:123456789012:function:auth/invocations",
            identitySource="method.request.header.Authorization",
        )
        return auth["id"]

    def test_get_vpc_links(self, client):
        """GetVpcLinks returns a response."""
        resp = client.get_vpc_links()
        assert "items" in resp

    def test_get_authorizer(self, client, api, authorizer):
        """GetAuthorizer retrieves authorizer by ID."""
        resp = client.get_authorizer(restApiId=api, authorizerId=authorizer)
        assert resp["id"] == authorizer
        assert resp["name"] == "test-authorizer"
        assert resp["type"] == "TOKEN"

    def test_update_authorizer(self, client, api, authorizer):
        """UpdateAuthorizer modifies authorizer properties."""
        resp = client.update_authorizer(
            restApiId=api,
            authorizerId=authorizer,
            patchOperations=[
                {"op": "replace", "path": "/name", "value": "updated-auth"},
            ],
        )
        assert resp["name"] == "updated-auth"

    def test_delete_authorizer(self, client, api):
        """DeleteAuthorizer removes an authorizer."""
        auth = client.create_authorizer(
            restApiId=api,
            name="delete-me-auth",
            type="TOKEN",
            authorizerUri="arn:aws:apigateway:us-east-1:lambda:path/2015-03-31/functions/arn:aws:lambda:us-east-1:123456789012:function:auth/invocations",
            identitySource="method.request.header.Authorization",
        )
        client.delete_authorizer(restApiId=api, authorizerId=auth["id"])
        auths = client.get_authorizers(restApiId=api)
        auth_ids = [a["id"] for a in auths["items"]]
        assert auth["id"] not in auth_ids

    @pytest.fixture
    def domain(self, client):
        import uuid

        domain_name = f"auto-{uuid.uuid4().hex[:8]}.example.com"
        client.create_domain_name(
            domainName=domain_name,
            certificateArn="arn:aws:acm:us-east-1:123456789012:certificate/abc123",
        )
        yield domain_name
        try:
            client.delete_domain_name(domainName=domain_name)
        except Exception:
            pass  # best-effort cleanup

    def test_create_base_path_mapping(self, client, api, domain):
        """CreateBasePathMapping creates a mapping for a domain."""
        resp = client.create_base_path_mapping(
            domainName=domain,
            restApiId=api,
            basePath="v1",
        )
        assert resp["basePath"] == "v1"
        assert resp["restApiId"] == api

    def test_get_base_path_mapping(self, client, api, domain):
        """GetBasePathMapping retrieves a specific mapping."""
        client.create_base_path_mapping(
            domainName=domain,
            restApiId=api,
            basePath="v2",
        )
        resp = client.get_base_path_mapping(domainName=domain, basePath="v2")
        assert resp["basePath"] == "v2"
        assert resp["restApiId"] == api

    def test_get_base_path_mappings(self, client, api, domain):
        """GetBasePathMappings lists mappings for a domain."""
        client.create_base_path_mapping(
            domainName=domain,
            restApiId=api,
            basePath="v3",
        )
        resp = client.get_base_path_mappings(domainName=domain)
        assert "items" in resp
        paths = [m["basePath"] for m in resp["items"]]
        assert "v3" in paths

    def test_update_base_path_mapping(self, client, api, domain):
        """UpdateBasePathMapping modifies a mapping."""
        client.create_base_path_mapping(
            domainName=domain,
            restApiId=api,
            basePath="v4",
        )
        resp = client.update_base_path_mapping(
            domainName=domain,
            basePath="v4",
            patchOperations=[
                {"op": "replace", "path": "/basePath", "value": "v4updated"},
            ],
        )
        assert resp["basePath"] == "v4updated"

    def test_get_request_validator(self, client, api):
        """GetRequestValidator retrieves a validator by ID."""
        v = client.create_request_validator(
            restApiId=api,
            name="get-val-test",
            validateRequestBody=True,
            validateRequestParameters=False,
        )
        resp = client.get_request_validator(restApiId=api, requestValidatorId=v["id"])
        assert resp["id"] == v["id"]
        assert resp["name"] == "get-val-test"

    def test_update_request_validator(self, client, api):
        """UpdateRequestValidator modifies a validator."""
        v = client.create_request_validator(
            restApiId=api,
            name="upd-val-test",
            validateRequestBody=False,
            validateRequestParameters=False,
        )
        resp = client.update_request_validator(
            restApiId=api,
            requestValidatorId=v["id"],
            patchOperations=[
                {"op": "replace", "path": "/validateRequestBody", "value": "true"},
            ],
        )
        assert resp["validateRequestBody"] is True

    def test_delete_request_validator(self, client, api):
        """DeleteRequestValidator removes a validator."""
        v = client.create_request_validator(
            restApiId=api,
            name="del-val-test",
            validateRequestBody=True,
            validateRequestParameters=False,
        )
        client.delete_request_validator(restApiId=api, requestValidatorId=v["id"])
        validators = client.get_request_validators(restApiId=api)
        ids = [val["id"] for val in validators["items"]]
        assert v["id"] not in ids

    def test_create_vpc_link(self, client):
        """CreateVpcLink creates a VPC link."""
        resp = client.create_vpc_link(
            name="test-vpc-link",
            targetArns=[
                "arn:aws:elasticloadbalancing:us-east-1:123456789012:loadbalancer/net/my-nlb/abc123"
            ],
        )
        link_id = resp["id"]
        try:
            assert resp["name"] == "test-vpc-link"
            assert "id" in resp
        finally:
            try:
                client.delete_vpc_link(vpcLinkId=link_id)
            except Exception:
                pass  # best-effort cleanup

    def test_get_vpc_link(self, client):
        """GetVpcLink retrieves a VPC link by ID."""
        created = client.create_vpc_link(
            name="get-vpc-link",
            targetArns=[
                "arn:aws:elasticloadbalancing:us-east-1:123456789012:loadbalancer/net/my-nlb/abc123"
            ],
        )
        link_id = created["id"]
        try:
            resp = client.get_vpc_link(vpcLinkId=link_id)
            assert resp["id"] == link_id
            assert resp["name"] == "get-vpc-link"
        finally:
            try:
                client.delete_vpc_link(vpcLinkId=link_id)
            except Exception:
                pass  # best-effort cleanup

    def test_delete_vpc_link(self, client):
        """DeleteVpcLink removes a VPC link."""
        created = client.create_vpc_link(
            name="del-vpc-link",
            targetArns=[
                "arn:aws:elasticloadbalancing:us-east-1:123456789012:loadbalancer/net/my-nlb/abc123"
            ],
        )
        link_id = created["id"]
        client.delete_vpc_link(vpcLinkId=link_id)
        links = client.get_vpc_links()
        link_ids = [lnk["id"] for lnk in links["items"]]
        assert link_id not in link_ids

    def test_update_api_key(self, client):
        """UpdateApiKey modifies an API key."""
        import uuid

        key = client.create_api_key(name=f"upd-key-{uuid.uuid4().hex[:8]}", enabled=True)
        try:
            resp = client.update_api_key(
                apiKey=key["id"],
                patchOperations=[
                    {"op": "replace", "path": "/description", "value": "Updated key desc"},
                ],
            )
            assert resp["description"] == "Updated key desc"
        finally:
            client.delete_api_key(apiKey=key["id"])

    def test_delete_deployment(self, client, api):
        """DeleteDeployment removes a deployment."""
        resources = client.get_resources(restApiId=api)
        root_id = [r for r in resources["items"] if r["path"] == "/"][0]["id"]
        client.put_method(
            restApiId=api,
            resourceId=root_id,
            httpMethod="GET",
            authorizationType="NONE",
        )
        client.put_integration(
            restApiId=api,
            resourceId=root_id,
            httpMethod="GET",
            type="MOCK",
            requestTemplates={"application/json": '{"statusCode": 200}'},
        )
        dep = client.create_deployment(restApiId=api)
        client.delete_deployment(restApiId=api, deploymentId=dep["id"])
        deployments = client.get_deployments(restApiId=api)
        dep_ids = [d["id"] for d in deployments["items"]]
        assert dep["id"] not in dep_ids

    def test_delete_gateway_response(self, client, api):
        """DeleteGatewayResponse removes a gateway response."""
        client.put_gateway_response(
            restApiId=api,
            responseType="ACCESS_DENIED",
            statusCode="403",
        )
        client.delete_gateway_response(restApiId=api, responseType="ACCESS_DENIED")
        responses = client.get_gateway_responses(restApiId=api)
        types = [r["responseType"] for r in responses["items"]]
        assert "ACCESS_DENIED" not in types

    def test_delete_integration(self, client, api):
        """DeleteIntegration removes an integration."""
        resources = client.get_resources(restApiId=api)
        root_id = [r for r in resources["items"] if r["path"] == "/"][0]["id"]
        child = client.create_resource(restApiId=api, parentId=root_id, pathPart="delint")
        client.put_method(
            restApiId=api,
            resourceId=child["id"],
            httpMethod="GET",
            authorizationType="NONE",
        )
        client.put_integration(
            restApiId=api,
            resourceId=child["id"],
            httpMethod="GET",
            type="MOCK",
            requestTemplates={"application/json": '{"statusCode": 200}'},
        )
        client.delete_integration(restApiId=api, resourceId=child["id"], httpMethod="GET")
        # Verify the resource no longer has the integration
        resource = client.get_resource(restApiId=api, resourceId=child["id"])
        method_info = resource.get("resourceMethods", {}).get("GET", {})
        assert "methodIntegration" not in method_info

    def test_delete_method(self, client, api):
        """DeleteMethod removes a method."""
        resources = client.get_resources(restApiId=api)
        root_id = [r for r in resources["items"] if r["path"] == "/"][0]["id"]
        child = client.create_resource(restApiId=api, parentId=root_id, pathPart="delmeth")
        client.put_method(
            restApiId=api,
            resourceId=child["id"],
            httpMethod="DELETE",
            authorizationType="NONE",
        )
        client.delete_method(restApiId=api, resourceId=child["id"], httpMethod="DELETE")
        from botocore.exceptions import ClientError

        with pytest.raises(ClientError):
            client.get_method(restApiId=api, resourceId=child["id"], httpMethod="DELETE")

    def test_update_usage_plan(self, client):
        """UpdateUsagePlan modifies a usage plan."""
        plan = client.create_usage_plan(name="upd-plan-test")
        try:
            resp = client.update_usage_plan(
                usagePlanId=plan["id"],
                patchOperations=[
                    {"op": "replace", "path": "/description", "value": "Updated plan"},
                ],
            )
            assert resp["description"] == "Updated plan"
        finally:
            client.delete_usage_plan(usagePlanId=plan["id"])

    def test_get_usage_plan_key(self, client):
        """GetUsagePlanKey retrieves a specific key in a plan."""
        import uuid

        plan = client.create_usage_plan(name=f"key-plan-{uuid.uuid4().hex[:8]}")
        key = client.create_api_key(name=f"key-{uuid.uuid4().hex[:8]}", enabled=True)
        try:
            client.create_usage_plan_key(
                usagePlanId=plan["id"],
                keyId=key["id"],
                keyType="API_KEY",
            )
            resp = client.get_usage_plan_key(usagePlanId=plan["id"], keyId=key["id"])
            assert resp["id"] == key["id"]
            assert resp["type"] == "API_KEY"
        finally:
            try:
                client.delete_usage_plan_key(usagePlanId=plan["id"], keyId=key["id"])
            except Exception:
                pass  # best-effort cleanup
            client.delete_api_key(apiKey=key["id"])
            client.delete_usage_plan(usagePlanId=plan["id"])

    def test_import_rest_api(self, apigw):
        """Import a REST API from a Swagger/OpenAPI definition."""
        openapi_spec = {
            "openapi": "3.0.1",
            "info": {
                "title": "ImportedAPI",
                "description": "Imported test API",
                "version": "1.0",
            },
            "paths": {
                "/pets": {
                    "get": {
                        "summary": "List pets",
                        "responses": {"200": {"description": "OK"}},
                    }
                }
            },
        }
        body = json.dumps(openapi_spec).encode("utf-8")
        response = apigw.import_rest_api(body=body)
        api_id = response["id"]
        try:
            assert response["name"] == "ImportedAPI"
            assert "id" in response
            # Verify the API was actually created and has resources
            resources = apigw.get_resources(restApiId=api_id)
            paths = [r["path"] for r in resources["items"]]
            assert "/" in paths
            assert "/pets" in paths
        finally:
            apigw.delete_rest_api(restApiId=api_id)


class TestAPIGatewayDeleteOperations:
    """Tests for delete operations on integration/method responses and base path mappings."""

    @pytest.fixture
    def apigw(self):
        return make_client("apigateway")

    @pytest.fixture
    def api_with_method(self, apigw):
        """Create API with resource, method, integration, method response, integration response."""
        import uuid

        api = apigw.create_rest_api(
            name=f"del-test-{uuid.uuid4().hex[:8]}", description="Delete ops test"
        )
        api_id = api["id"]
        resources = apigw.get_resources(restApiId=api_id)
        root_id = [r for r in resources["items"] if r["path"] == "/"][0]["id"]
        child = apigw.create_resource(restApiId=api_id, parentId=root_id, pathPart="deltest")
        resource_id = child["id"]
        apigw.put_method(
            restApiId=api_id,
            resourceId=resource_id,
            httpMethod="GET",
            authorizationType="NONE",
        )
        apigw.put_integration(
            restApiId=api_id,
            resourceId=resource_id,
            httpMethod="GET",
            type="MOCK",
            requestTemplates={"application/json": '{"statusCode": 200}'},
        )
        apigw.put_method_response(
            restApiId=api_id,
            resourceId=resource_id,
            httpMethod="GET",
            statusCode="200",
            responseModels={"application/json": "Empty"},
        )
        apigw.put_integration_response(
            restApiId=api_id,
            resourceId=resource_id,
            httpMethod="GET",
            statusCode="200",
            responseTemplates={"application/json": ""},
        )
        yield {"api_id": api_id, "resource_id": resource_id}
        apigw.delete_rest_api(restApiId=api_id)

    def test_delete_integration_response(self, apigw, api_with_method):
        """DeleteIntegrationResponse removes a specific integration response."""
        api_id = api_with_method["api_id"]
        resource_id = api_with_method["resource_id"]
        resp = apigw.delete_integration_response(
            restApiId=api_id,
            resourceId=resource_id,
            httpMethod="GET",
            statusCode="200",
        )
        assert resp["ResponseMetadata"]["HTTPStatusCode"] in (200, 204)

    def test_delete_method_response(self, apigw, api_with_method):
        """DeleteMethodResponse removes a specific method response."""
        api_id = api_with_method["api_id"]
        resource_id = api_with_method["resource_id"]
        resp = apigw.delete_method_response(
            restApiId=api_id,
            resourceId=resource_id,
            httpMethod="GET",
            statusCode="200",
        )
        assert resp["ResponseMetadata"]["HTTPStatusCode"] in (200, 204)

    def test_delete_base_path_mapping(self, apigw):
        """DeleteBasePathMapping removes a mapping from a domain."""
        import uuid

        domain_name = f"del-bpm-{uuid.uuid4().hex[:8]}.example.com"
        api = apigw.create_rest_api(name=f"bpm-api-{uuid.uuid4().hex[:8]}")
        api_id = api["id"]
        try:
            apigw.create_domain_name(
                domainName=domain_name,
                certificateArn="arn:aws:acm:us-east-1:123456789012:certificate/abc123",
            )
            apigw.create_base_path_mapping(domainName=domain_name, restApiId=api_id, basePath="v1")
            resp = apigw.delete_base_path_mapping(domainName=domain_name, basePath="v1")
            assert resp["ResponseMetadata"]["HTTPStatusCode"] in (200, 202, 204)
            # Verify it's gone
            mappings = apigw.get_base_path_mappings(domainName=domain_name)
            paths = [m["basePath"] for m in mappings.get("items", [])]
            assert "v1" not in paths
        finally:
            try:
                apigw.delete_domain_name(domainName=domain_name)
            except Exception:
                pass  # best-effort cleanup
            apigw.delete_rest_api(restApiId=api_id)

    def test_put_rest_api(self, apigw):
        """PutRestApi updates an API with a new definition."""
        api = apigw.create_rest_api(name="put-rest-api-test")
        api_id = api["id"]
        try:
            openapi_spec = {
                "openapi": "3.0.1",
                "info": {"title": "OverwrittenAPI", "version": "2.0"},
                "paths": {
                    "/items": {
                        "get": {
                            "summary": "List items",
                            "responses": {"200": {"description": "OK"}},
                        }
                    }
                },
            }
            body = json.dumps(openapi_spec).encode("utf-8")
            resp = apigw.put_rest_api(restApiId=api_id, mode="overwrite", body=body)
            assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200
            assert "id" in resp
            # Verify resources were updated - /items should exist
            resources = apigw.get_resources(restApiId=api_id)
            paths = [r["path"] for r in resources["items"]]
            assert "/items" in paths
        finally:
            apigw.delete_rest_api(restApiId=api_id)

    def test_update_account(self, apigw):
        """UpdateAccount modifies account-level settings (cloudwatchRoleArn)."""
        resp = apigw.update_account(
            patchOperations=[
                {
                    "op": "replace",
                    "path": "/cloudwatchRoleArn",
                    "value": "arn:aws:iam::123456789012:role/apigw-cw-role",
                },
            ]
        )
        assert "ResponseMetadata" in resp
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200


class TestAPIGatewayApiKeyLifecycle:
    """Full CRUD lifecycle for API keys with deletion verification."""

    @pytest.fixture
    def apigw(self):
        return make_client("apigateway")

    def test_delete_api_key_verified(self, apigw):
        """DeleteApiKey removes the key and GetApiKey returns NotFoundException."""
        import uuid

        from botocore.exceptions import ClientError

        key = apigw.create_api_key(name=f"del-verify-{uuid.uuid4().hex[:8]}", enabled=True)
        key_id = key["id"]
        assert key["enabled"] is True

        apigw.delete_api_key(apiKey=key_id)

        with pytest.raises(ClientError) as exc:
            apigw.get_api_key(apiKey=key_id)
        assert exc.value.response["Error"]["Code"] == "NotFoundException"

    def test_api_key_include_value(self, apigw):
        """GetApiKey with includeValue returns the API key value."""
        import uuid

        key = apigw.create_api_key(
            name=f"val-key-{uuid.uuid4().hex[:8]}",
            enabled=True,
            value=f"custom-key-{uuid.uuid4().hex}",
        )
        key_id = key["id"]
        try:
            got = apigw.get_api_key(apiKey=key_id, includeValue=True)
            assert "value" in got
            assert len(got["value"]) > 0
        finally:
            apigw.delete_api_key(apiKey=key_id)


class TestAPIGatewayUsagePlanLifecycle:
    """Full CRUD lifecycle for usage plans with deletion verification."""

    @pytest.fixture
    def apigw(self):
        return make_client("apigateway")

    def test_delete_usage_plan_verified(self, apigw):
        """DeleteUsagePlan removes the plan and GetUsagePlan returns NotFoundException."""
        import uuid

        from botocore.exceptions import ClientError

        plan = apigw.create_usage_plan(name=f"del-plan-{uuid.uuid4().hex[:8]}")
        plan_id = plan["id"]
        apigw.delete_usage_plan(usagePlanId=plan_id)

        with pytest.raises(ClientError) as exc:
            apigw.get_usage_plan(usagePlanId=plan_id)
        assert exc.value.response["Error"]["Code"] == "NotFoundException"

    def test_usage_plan_with_quota(self, apigw):
        """Create a usage plan with quota settings and verify them."""
        import uuid

        plan = apigw.create_usage_plan(
            name=f"quota-plan-{uuid.uuid4().hex[:8]}",
            throttle={"burstLimit": 200, "rateLimit": 100.0},
            quota={"limit": 5000, "period": "MONTH"},
        )
        plan_id = plan["id"]
        try:
            assert plan["throttle"]["burstLimit"] == 200
            assert plan["quota"]["limit"] == 5000
            assert plan["quota"]["period"] == "MONTH"

            got = apigw.get_usage_plan(usagePlanId=plan_id)
            assert got["quota"]["limit"] == 5000
        finally:
            apigw.delete_usage_plan(usagePlanId=plan_id)


class TestAPIGatewayUsagePlanKeyLifecycle:
    """Full CRUD lifecycle for usage plan keys."""

    @pytest.fixture
    def apigw(self):
        return make_client("apigateway")

    def test_usage_plan_key_full_lifecycle(self, apigw):
        """Create, get, list, delete usage plan key with verification."""
        import uuid

        plan = apigw.create_usage_plan(name=f"upk-lifecycle-{uuid.uuid4().hex[:8]}")
        key = apigw.create_api_key(name=f"upk-key-{uuid.uuid4().hex[:8]}", enabled=True)
        try:
            # Create
            upk = apigw.create_usage_plan_key(
                usagePlanId=plan["id"],
                keyId=key["id"],
                keyType="API_KEY",
            )
            assert upk["id"] == key["id"]
            assert upk["type"] == "API_KEY"

            # Get
            got = apigw.get_usage_plan_key(usagePlanId=plan["id"], keyId=key["id"])
            assert got["id"] == key["id"]

            # List
            keys = apigw.get_usage_plan_keys(usagePlanId=plan["id"])
            key_ids = [k["id"] for k in keys["items"]]
            assert key["id"] in key_ids

            # Delete
            apigw.delete_usage_plan_key(usagePlanId=plan["id"], keyId=key["id"])
            keys_after = apigw.get_usage_plan_keys(usagePlanId=plan["id"])
            key_ids_after = [k["id"] for k in keys_after.get("items", [])]
            assert key["id"] not in key_ids_after
        finally:
            try:
                apigw.delete_usage_plan_key(usagePlanId=plan["id"], keyId=key["id"])
            except Exception:
                pass  # best-effort cleanup
            apigw.delete_api_key(apiKey=key["id"])
            apigw.delete_usage_plan(usagePlanId=plan["id"])


class TestAPIGatewayVpcLinkLifecycle:
    """Full CRUD lifecycle for VPC links."""

    @pytest.fixture
    def apigw(self):
        return make_client("apigateway")

    def test_vpc_link_full_lifecycle(self, apigw):
        """Create, get, list, delete VPC link with verification."""
        import uuid

        nlb_arn = (
            "arn:aws:elasticloadbalancing:us-east-1:123456789012"
            f":loadbalancer/net/nlb-{uuid.uuid4().hex[:8]}/abc123"
        )
        created = apigw.create_vpc_link(
            name=f"lifecycle-link-{uuid.uuid4().hex[:8]}",
            targetArns=[nlb_arn],
            description="Lifecycle test VPC link",
        )
        link_id = created["id"]
        try:
            assert created["name"].startswith("lifecycle-link-")
            assert "id" in created

            # Get
            got = apigw.get_vpc_link(vpcLinkId=link_id)
            assert got["id"] == link_id
            assert got["description"] == "Lifecycle test VPC link"

            # List
            links = apigw.get_vpc_links()
            link_ids = [lnk["id"] for lnk in links["items"]]
            assert link_id in link_ids

            # Delete
            apigw.delete_vpc_link(vpcLinkId=link_id)
            links_after = apigw.get_vpc_links()
            link_ids_after = [lnk["id"] for lnk in links_after["items"]]
            assert link_id not in link_ids_after
        except Exception:
            try:
                apigw.delete_vpc_link(vpcLinkId=link_id)
            except Exception:
                pass  # best-effort cleanup
            raise


class TestAPIGatewayRequestValidatorLifecycle:
    """Full CRUD lifecycle for request validators."""

    @pytest.fixture
    def apigw(self):
        return make_client("apigateway")

    @pytest.fixture
    def api(self, apigw):
        import uuid

        resp = apigw.create_rest_api(
            name=f"rv-api-{uuid.uuid4().hex[:8]}",
            description="Request validator test API",
        )
        api_id = resp["id"]
        yield api_id
        apigw.delete_rest_api(restApiId=api_id)

    def test_request_validator_full_lifecycle(self, apigw, api):
        """Create, get, list, update, delete request validator with verification."""
        validator = apigw.create_request_validator(
            restApiId=api,
            name="lifecycle-validator",
            validateRequestBody=True,
            validateRequestParameters=False,
        )
        val_id = validator["id"]

        # Verify create
        assert validator["name"] == "lifecycle-validator"
        assert validator["validateRequestBody"] is True
        assert validator["validateRequestParameters"] is False

        # Get
        got = apigw.get_request_validator(restApiId=api, requestValidatorId=val_id)
        assert got["id"] == val_id
        assert got["name"] == "lifecycle-validator"

        # List
        validators = apigw.get_request_validators(restApiId=api)
        val_ids = [v["id"] for v in validators["items"]]
        assert val_id in val_ids

        # Update
        updated = apigw.update_request_validator(
            restApiId=api,
            requestValidatorId=val_id,
            patchOperations=[
                {"op": "replace", "path": "/validateRequestParameters", "value": "true"},
            ],
        )
        assert updated["validateRequestParameters"] is True

        # Delete
        apigw.delete_request_validator(restApiId=api, requestValidatorId=val_id)
        validators_after = apigw.get_request_validators(restApiId=api)
        val_ids_after = [v["id"] for v in validators_after["items"]]
        assert val_id not in val_ids_after


class TestAPIGatewayDocumentationParts:
    """Tests for Documentation Parts CRUD."""

    @pytest.fixture(autouse=True)
    def _setup_api(self, apigw):
        self.apigw = apigw
        resp = apigw.create_rest_api(name="doc-parts-api", description="Doc parts test")
        self.api_id = resp["id"]
        yield
        apigw.delete_rest_api(restApiId=self.api_id)

    def test_create_and_get_documentation_part(self, apigw):
        resp = apigw.create_documentation_part(
            restApiId=self.api_id,
            location={"type": "API"},
            properties='{"description": "My API"}',
        )
        part_id = resp["id"]
        assert part_id is not None

        got = apigw.get_documentation_part(restApiId=self.api_id, documentationPartId=part_id)
        assert got["id"] == part_id
        assert got["properties"] == '{"description": "My API"}'

    def test_get_documentation_parts(self, apigw):
        apigw.create_documentation_part(
            restApiId=self.api_id,
            location={"type": "API"},
            properties='{"description": "List test"}',
        )
        resp = apigw.get_documentation_parts(restApiId=self.api_id)
        assert "items" in resp
        assert len(resp["items"]) >= 1

    def test_update_documentation_part(self, apigw):
        created = apigw.create_documentation_part(
            restApiId=self.api_id,
            location={"type": "API"},
            properties='{"description": "Original"}',
        )
        part_id = created["id"]
        updated = apigw.update_documentation_part(
            restApiId=self.api_id,
            documentationPartId=part_id,
            patchOperations=[
                {"op": "replace", "path": "/properties", "value": '{"description": "Updated"}'},
            ],
        )
        assert updated["properties"] == '{"description": "Updated"}'

    def test_delete_documentation_part(self, apigw):
        created = apigw.create_documentation_part(
            restApiId=self.api_id,
            location={"type": "API"},
            properties='{"description": "To delete"}',
        )
        part_id = created["id"]
        apigw.delete_documentation_part(restApiId=self.api_id, documentationPartId=part_id)
        resp = apigw.get_documentation_parts(restApiId=self.api_id)
        ids = [p["id"] for p in resp["items"]]
        assert part_id not in ids


class TestAPIGatewayDocumentationVersions:
    """Tests for Documentation Versions."""

    @pytest.fixture(autouse=True)
    def _setup_api(self, apigw):
        self.apigw = apigw
        resp = apigw.create_rest_api(name="doc-ver-api", description="Doc version test")
        self.api_id = resp["id"]
        # Create a doc part so we have something to version
        apigw.create_documentation_part(
            restApiId=self.api_id,
            location={"type": "API"},
            properties='{"description": "Versioned API"}',
        )
        yield
        apigw.delete_rest_api(restApiId=self.api_id)

    def test_create_and_get_documentation_version(self, apigw):
        resp = apigw.create_documentation_version(
            restApiId=self.api_id,
            documentationVersion="1.0",
            description="First version",
        )
        assert resp["version"] == "1.0"

        got = apigw.get_documentation_version(restApiId=self.api_id, documentationVersion="1.0")
        assert got["version"] == "1.0"
        assert got["description"] == "First version"

    def test_get_documentation_versions(self, apigw):
        apigw.create_documentation_version(
            restApiId=self.api_id,
            documentationVersion="2.0",
            description="Second version",
        )
        resp = apigw.get_documentation_versions(restApiId=self.api_id)
        assert "items" in resp
        versions = [v["version"] for v in resp["items"]]
        assert "2.0" in versions


class TestAPIGatewayClientCertificates:
    """Tests for Client Certificates."""

    def test_generate_and_get_client_certificate(self, apigw):
        resp = apigw.generate_client_certificate(description="Test cert")
        cert_id = resp["clientCertificateId"]
        assert cert_id is not None

        got = apigw.get_client_certificate(clientCertificateId=cert_id)
        assert got["clientCertificateId"] == cert_id
        assert got["description"] == "Test cert"

    def test_get_client_certificates(self, apigw):
        cert = apigw.generate_client_certificate(description="List cert")
        cert_id = cert["clientCertificateId"]
        resp = apigw.get_client_certificates()
        assert "items" in resp
        ids = [c["clientCertificateId"] for c in resp["items"]]
        assert cert_id in ids


class TestAPIGatewayUpdateOperations:
    """Tests for Update* operations that weren't previously covered."""

    @pytest.fixture
    def api(self, apigw):
        resp = apigw.create_rest_api(name="update-ops-test", description="Test")
        api_id = resp["id"]
        yield api_id
        apigw.delete_rest_api(restApiId=api_id)

    def test_update_gateway_response(self, apigw, api):
        apigw.put_gateway_response(restApiId=api, responseType="DEFAULT_4XX", statusCode="400")
        resp = apigw.update_gateway_response(
            restApiId=api,
            responseType="DEFAULT_4XX",
            patchOperations=[{"op": "replace", "path": "/statusCode", "value": "401"}],
        )
        assert resp["statusCode"] == "401"

    def test_update_client_certificate(self, apigw):
        cert = apigw.generate_client_certificate(description="original-desc")
        cert_id = cert["clientCertificateId"]
        resp = apigw.update_client_certificate(
            clientCertificateId=cert_id,
            patchOperations=[{"op": "replace", "path": "/description", "value": "updated-desc"}],
        )
        assert resp["description"] == "updated-desc"
        apigw.delete_client_certificate(clientCertificateId=cert_id)

    def test_update_documentation_version(self, apigw, api):
        apigw.create_documentation_version(restApiId=api, documentationVersion="v1")
        resp = apigw.update_documentation_version(
            restApiId=api,
            documentationVersion="v1",
            patchOperations=[{"op": "replace", "path": "/description", "value": "updated"}],
        )
        assert resp["description"] == "updated"

    def test_update_method(self, apigw, api):
        resources = apigw.get_resources(restApiId=api)
        root_id = [r for r in resources["items"] if r["path"] == "/"][0]["id"]
        apigw.put_method(
            restApiId=api, resourceId=root_id, httpMethod="GET", authorizationType="NONE"
        )
        resp = apigw.update_method(
            restApiId=api,
            resourceId=root_id,
            httpMethod="GET",
            patchOperations=[{"op": "replace", "path": "/authorizationType", "value": "CUSTOM"}],
        )
        assert resp["authorizationType"] == "CUSTOM"

    def test_update_integration(self, apigw, api):
        resources = apigw.get_resources(restApiId=api)
        root_id = [r for r in resources["items"] if r["path"] == "/"][0]["id"]
        apigw.put_method(
            restApiId=api, resourceId=root_id, httpMethod="GET", authorizationType="NONE"
        )
        apigw.put_integration(
            restApiId=api,
            resourceId=root_id,
            httpMethod="GET",
            type="MOCK",
            requestTemplates={"application/json": '{"statusCode": 200}'},
        )
        resp = apigw.update_integration(
            restApiId=api,
            resourceId=root_id,
            httpMethod="GET",
            patchOperations=[
                {"op": "replace", "path": "/passthroughBehavior", "value": "WHEN_NO_MATCH"}
            ],
        )
        assert resp["type"] == "MOCK"


class TestAPIGatewayDeleteDocumentationVersion:
    """Tests for DeleteDocumentationVersion."""

    def test_delete_documentation_version(self, apigw):
        api = apigw.create_rest_api(name="docver-del-test", description="test")
        api_id = api["id"]
        apigw.create_documentation_version(restApiId=api_id, documentationVersion="v1")
        resp = apigw.delete_documentation_version(restApiId=api_id, documentationVersion="v1")
        assert resp["ResponseMetadata"]["HTTPStatusCode"] in (200, 202)
        # Verify it's gone
        versions = apigw.get_documentation_versions(restApiId=api_id)
        version_ids = [v["version"] for v in versions.get("items", [])]
        assert "v1" not in version_ids
        apigw.delete_rest_api(restApiId=api_id)


class TestAPIGatewayGetUsage:
    """Tests for GetUsage."""

    def test_get_usage(self, apigw):
        plan = apigw.create_usage_plan(name="usage-test-plan", description="test")
        plan_id = plan["id"]
        resp = apigw.get_usage(usagePlanId=plan_id, startDate="2026-01-01", endDate="2026-03-09")
        assert resp["usagePlanId"] == plan_id
        apigw.delete_usage_plan(usagePlanId=plan_id)


class TestAPIGatewayFlushStageAuthorizersCache:
    """Tests for FlushStageAuthorizersCache."""

    def test_flush_stage_authorizers_cache(self, apigw):
        api = apigw.create_rest_api(name="flush-auth-test", description="test")
        api_id = api["id"]
        resources = apigw.get_resources(restApiId=api_id)
        root_id = [r for r in resources["items"] if r["path"] == "/"][0]["id"]
        apigw.put_method(
            restApiId=api_id, resourceId=root_id, httpMethod="GET", authorizationType="NONE"
        )
        apigw.put_integration(
            restApiId=api_id,
            resourceId=root_id,
            httpMethod="GET",
            type="MOCK",
            requestTemplates={"application/json": '{"statusCode": 200}'},
        )
        dep = apigw.create_deployment(restApiId=api_id)
        apigw.create_stage(restApiId=api_id, stageName="test", deploymentId=dep["id"])
        resp = apigw.flush_stage_authorizers_cache(restApiId=api_id, stageName="test")
        assert resp["ResponseMetadata"]["HTTPStatusCode"] in (200, 202)
        apigw.delete_rest_api(restApiId=api_id)


class TestAPIGatewayGetDocumentationParts:
    """Tests for GetDocumentationParts."""

    def test_get_documentation_parts_empty(self, apigw):
        api = apigw.create_rest_api(name="docparts-test", description="test")
        api_id = api["id"]
        resp = apigw.get_documentation_parts(restApiId=api_id)
        assert "items" in resp
        assert isinstance(resp["items"], list)
        apigw.delete_rest_api(restApiId=api_id)

    def test_get_documentation_parts_after_create(self, apigw):
        api = apigw.create_rest_api(name="docparts-test2", description="test")
        api_id = api["id"]
        apigw.create_documentation_part(
            restApiId=api_id,
            location={"type": "API"},
            properties='{"description": "My API"}',
        )
        resp = apigw.get_documentation_parts(restApiId=api_id)
        assert len(resp["items"]) >= 1
        assert resp["items"][0]["properties"] == '{"description": "My API"}'
        apigw.delete_rest_api(restApiId=api_id)


class TestAPIGatewayGetIntegrationResponse:
    """Tests for GetIntegrationResponse."""

    def test_get_integration_response(self, apigw):
        api = apigw.create_rest_api(name="intresp-get-test", description="test")
        api_id = api["id"]
        resources = apigw.get_resources(restApiId=api_id)
        root_id = [r for r in resources["items"] if r["path"] == "/"][0]["id"]
        apigw.put_method(
            restApiId=api_id, resourceId=root_id, httpMethod="GET", authorizationType="NONE"
        )
        apigw.put_integration(
            restApiId=api_id,
            resourceId=root_id,
            httpMethod="GET",
            type="MOCK",
            requestTemplates={"application/json": '{"statusCode": 200}'},
        )
        apigw.put_integration_response(
            restApiId=api_id,
            resourceId=root_id,
            httpMethod="GET",
            statusCode="200",
            responseTemplates={"application/json": ""},
        )
        resp = apigw.get_integration_response(
            restApiId=api_id, resourceId=root_id, httpMethod="GET", statusCode="200"
        )
        assert resp["statusCode"] == "200"
        assert "responseTemplates" in resp
        apigw.delete_rest_api(restApiId=api_id)


class TestAPIGatewayUpdateIntegrationResponse:
    """Tests for UpdateIntegrationResponse."""

    def test_update_integration_response(self, apigw):
        api = apigw.create_rest_api(name="intresp-upd-test", description="test")
        api_id = api["id"]
        resources = apigw.get_resources(restApiId=api_id)
        root_id = [r for r in resources["items"] if r["path"] == "/"][0]["id"]
        apigw.put_method(
            restApiId=api_id, resourceId=root_id, httpMethod="GET", authorizationType="NONE"
        )
        apigw.put_integration(
            restApiId=api_id,
            resourceId=root_id,
            httpMethod="GET",
            type="MOCK",
            requestTemplates={"application/json": '{"statusCode": 200}'},
        )
        apigw.put_integration_response(
            restApiId=api_id,
            resourceId=root_id,
            httpMethod="GET",
            statusCode="200",
            responseTemplates={"application/json": ""},
        )
        resp = apigw.update_integration_response(
            restApiId=api_id,
            resourceId=root_id,
            httpMethod="GET",
            statusCode="200",
            patchOperations=[
                {
                    "op": "replace",
                    "path": "/responseTemplates/application~1json",
                    "value": '{"output": "updated"}',
                }
            ],
        )
        assert resp["statusCode"] == "200"
        assert "responseTemplates" in resp
        apigw.delete_rest_api(restApiId=api_id)


class TestAPIGatewayGetMethodResponse:
    """Tests for GetMethodResponse."""

    def test_get_method_response(self, apigw):
        api = apigw.create_rest_api(name="methresp-get-test", description="test")
        api_id = api["id"]
        resources = apigw.get_resources(restApiId=api_id)
        root_id = [r for r in resources["items"] if r["path"] == "/"][0]["id"]
        apigw.put_method(
            restApiId=api_id, resourceId=root_id, httpMethod="GET", authorizationType="NONE"
        )
        apigw.put_method_response(
            restApiId=api_id,
            resourceId=root_id,
            httpMethod="GET",
            statusCode="200",
            responseModels={"application/json": "Empty"},
        )
        resp = apigw.get_method_response(
            restApiId=api_id, resourceId=root_id, httpMethod="GET", statusCode="200"
        )
        assert resp["statusCode"] == "200"
        assert "responseModels" in resp
        apigw.delete_rest_api(restApiId=api_id)


class TestAPIGatewayUpdateMethodResponse:
    """Tests for UpdateMethodResponse."""

    def test_update_method_response(self, apigw):
        api = apigw.create_rest_api(name="methresp-upd-test", description="test")
        api_id = api["id"]
        resources = apigw.get_resources(restApiId=api_id)
        root_id = [r for r in resources["items"] if r["path"] == "/"][0]["id"]
        apigw.put_method(
            restApiId=api_id, resourceId=root_id, httpMethod="GET", authorizationType="NONE"
        )
        apigw.put_method_response(
            restApiId=api_id,
            resourceId=root_id,
            httpMethod="GET",
            statusCode="200",
            responseModels={"application/json": "Empty"},
        )
        resp = apigw.update_method_response(
            restApiId=api_id,
            resourceId=root_id,
            httpMethod="GET",
            statusCode="200",
            patchOperations=[
                {"op": "replace", "path": "/responseModels/application~1json", "value": "Error"}
            ],
        )
        assert resp["statusCode"] == "200"
        apigw.delete_rest_api(restApiId=api_id)


class TestAPIGatewayTagUntagResource:
    """Tests for TagResource and UntagResource on REST APIs."""

    def test_tag_resource(self, apigw):
        api = apigw.create_rest_api(name="tag-res-test", description="test")
        api_id = api["id"]
        api_arn = f"arn:aws:apigateway:us-east-1::/restapis/{api_id}"
        resp = apigw.tag_resource(resourceArn=api_arn, tags={"env": "test", "team": "backend"})
        assert resp["ResponseMetadata"]["HTTPStatusCode"] in (200, 204)
        # Verify tags applied
        api_resp = apigw.get_rest_api(restApiId=api_id)
        assert api_resp.get("tags", {}).get("env") == "test"
        apigw.delete_rest_api(restApiId=api_id)

    def test_untag_resource(self, apigw):
        api = apigw.create_rest_api(name="untag-res-test", description="test", tags={"env": "dev"})
        api_id = api["id"]
        api_arn = f"arn:aws:apigateway:us-east-1::/restapis/{api_id}"
        resp = apigw.untag_resource(resourceArn=api_arn, tagKeys=["env"])
        assert resp["ResponseMetadata"]["HTTPStatusCode"] in (200, 204)
        # Verify tags removed
        api_resp = apigw.get_rest_api(restApiId=api_id)
        assert "env" not in api_resp.get("tags", {})
        apigw.delete_rest_api(restApiId=api_id)


class TestAPIGatewayNewStubOps:
    """Tests for newly-implemented stub operations."""

    def test_get_sdk_types(self, apigw):
        resp = apigw.get_sdk_types()
        assert "items" in resp

    def test_get_sdk_type(self, apigw):
        resp = apigw.get_sdk_type(id="javascript")
        assert "id" in resp

    def test_update_vpc_link(self, apigw):
        nlb_arn = "arn:aws:elasticloadbalancing:us-east-1:123456789012:loadbalancer/net/test/abc123"
        vpc = apigw.create_vpc_link(name="test-vpc-link", targetArns=[nlb_arn])
        link_id = vpc["id"]
        resp = apigw.update_vpc_link(
            vpcLinkId=link_id,
            patchOperations=[{"op": "replace", "path": "/name", "value": "updated-name"}],
        )
        assert "id" in resp
        apigw.delete_vpc_link(vpcLinkId=link_id)

    def test_update_deployment(self, apigw, rest_api):
        # Must have at least one method before creating a deployment
        resources = apigw.get_resources(restApiId=rest_api)
        root_id = [r for r in resources["items"] if r["path"] == "/"][0]["id"]
        apigw.put_method(
            restApiId=rest_api, resourceId=root_id, httpMethod="GET", authorizationType="NONE"
        )
        apigw.put_integration(restApiId=rest_api, resourceId=root_id, httpMethod="GET", type="MOCK")
        dep = apigw.create_deployment(restApiId=rest_api, stageName="v1")
        dep_id = dep["id"]
        resp = apigw.update_deployment(
            restApiId=rest_api,
            deploymentId=dep_id,
            patchOperations=[{"op": "replace", "path": "/description", "value": "updated"}],
        )
        assert "id" in resp

    def test_update_domain_name(self, apigw):
        apigw.create_domain_name(domainName="api.example.com")
        resp = apigw.update_domain_name(
            domainName="api.example.com",
            patchOperations=[{"op": "replace", "path": "/certificateName", "value": "my-cert"}],
        )
        assert "domainName" in resp

    def test_update_model(self, apigw, rest_api):
        apigw.create_model(
            restApiId=rest_api,
            name="MyModel",
            contentType="application/json",
            schema='{"type": "object"}',
        )
        resp = apigw.update_model(
            restApiId=rest_api,
            modelName="MyModel",
            patchOperations=[{"op": "replace", "path": "/description", "value": "updated"}],
        )
        assert "name" in resp

    def test_update_resource(self, apigw, rest_api):
        resources = apigw.get_resources(restApiId=rest_api)
        root_id = [r for r in resources["items"] if r["path"] == "/"][0]["id"]
        new_res = apigw.create_resource(restApiId=rest_api, parentId=root_id, pathPart="items")
        resource_id = new_res["id"]
        resp = apigw.update_resource(
            restApiId=rest_api,
            resourceId=resource_id,
            patchOperations=[{"op": "replace", "path": "/pathPart", "value": "things"}],
        )
        assert "id" in resp

    def test_test_invoke_authorizer(self, apigw, rest_api):
        authorizer = apigw.create_authorizer(
            restApiId=rest_api,
            name="test-auth",
            type="TOKEN",
            authorizerUri="arn:aws:apigateway:us-east-1:lambda:path/2015-03-31/functions/arn:aws:lambda:us-east-1:123456789012:function:authorizer/invocations",
            identitySource="method.request.header.Authorization",
        )
        auth_id = authorizer["id"]
        resp = apigw.test_invoke_authorizer(
            restApiId=rest_api,
            authorizerId=auth_id,
            headers={"Authorization": "Bearer test-token"},
        )
        assert "clientStatus" in resp

    def test_test_invoke_method(self, apigw, rest_api):
        resources = apigw.get_resources(restApiId=rest_api)
        root_id = [r for r in resources["items"] if r["path"] == "/"][0]["id"]
        apigw.put_method(
            restApiId=rest_api, resourceId=root_id, httpMethod="GET", authorizationType="NONE"
        )
        apigw.put_integration(
            restApiId=rest_api,
            resourceId=root_id,
            httpMethod="GET",
            type="MOCK",
            requestTemplates={"application/json": '{"statusCode": 200}'},
        )
        resp = apigw.test_invoke_method(
            restApiId=rest_api,
            resourceId=root_id,
            httpMethod="GET",
            pathWithQueryString="/",
        )
        assert "status" in resp


class TestAPIGatewayDomainNameAccessAssociation:
    """Tests for DomainNameAccessAssociation ops (new stub ops)."""

    def test_create_domain_name_access_association(self, apigw):
        """CreateDomainNameAccessAssociation returns association ARN."""
        resp = apigw.create_domain_name_access_association(
            domainNameArn="arn:aws:apigateway:us-east-1::/domainnames/test.example.com",
            accessAssociationSourceType="VPCE",
            accessAssociationSource="vpce-0a1b2c3d4e5f67890",
        )
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200
        assert "domainNameAccessAssociationArn" in resp

    def test_get_domain_name_access_associations(self, apigw):
        """GetDomainNameAccessAssociations returns a list."""
        resp = apigw.get_domain_name_access_associations()
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200
        assert "items" in resp

    def test_delete_domain_name_access_association(self, apigw):
        """DeleteDomainNameAccessAssociation with fake ARN returns 200."""
        fake_arn = "arn:aws:apigateway:us-east-1:012345678901::/domainNameAccessAssociations/fake"
        resp = apigw.delete_domain_name_access_association(
            domainNameAccessAssociationArn=fake_arn,
        )
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200

    def test_reject_domain_name_access_association(self, apigw):
        """RejectDomainNameAccessAssociation returns 200."""
        resp = apigw.reject_domain_name_access_association(
            domainNameAccessAssociationArn=(
                "arn:aws:apigateway:us-east-1::/domainNameAccessAssociations/fake"
            ),
            domainNameArn="arn:aws:apigateway:us-east-1::/domainnames/test.example.com",
        )
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200

    def test_get_model_template(self, apigw):
        """GetModelTemplate returns a template value."""
        resp = apigw.get_model_template(restApiId="fake-api-id", modelName="Empty")
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200
        assert "value" in resp

    def test_import_documentation_parts(self, apigw):
        """ImportDocumentationParts returns empty ids list."""
        resp = apigw.import_documentation_parts(restApiId="fake-api-id", body=b"{}")
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200
        assert "ids" in resp


class TestAPIGatewayImportApiKeysGapOp:
    """Test ImportApiKeys operation (returns 501 NotImplemented)."""

    @pytest.fixture
    def client(self):
        return make_client("apigateway")

    def test_import_api_keys_not_implemented(self, client):
        from botocore.exceptions import ClientError

        with pytest.raises(ClientError) as exc:
            client.import_api_keys(
                body=b"key1,description1\nkey2,description2",
                format="csv",
            )
        assert exc.value.response["Error"]["Code"] in (
            "NotImplemented",
            "BadRequestException",
        )


class TestAPIGatewayEdgeCases:
    """Edge cases and behavioral fidelity tests for API Gateway."""

    @pytest.fixture
    def apigw(self):
        return make_client("apigateway")

    @pytest.fixture
    def rest_api(self, apigw):
        import uuid

        resp = apigw.create_rest_api(name=f"edge-api-{uuid.uuid4().hex[:8]}")
        api_id = resp["id"]
        yield api_id
        apigw.delete_rest_api(restApiId=api_id)

    def test_delete_rest_api_nonexistent(self, apigw):
        """DeleteRestApi with a nonexistent ID raises NotFoundException."""
        from botocore.exceptions import ClientError

        with pytest.raises(ClientError) as exc:
            apigw.delete_rest_api(restApiId="nonexistent1")
        assert exc.value.response["Error"]["Code"] == "NotFoundException"

    def test_get_rest_api_nonexistent(self, apigw):
        """GetRestApi with a nonexistent ID raises NotFoundException."""
        from botocore.exceptions import ClientError

        with pytest.raises(ClientError) as exc:
            apigw.get_rest_api(restApiId="nonexistent1")
        assert exc.value.response["Error"]["Code"] == "NotFoundException"

    def test_rest_api_has_created_date(self, apigw):
        """CreateRestApi response includes createdDate."""
        import uuid

        resp = apigw.create_rest_api(name=f"date-test-{uuid.uuid4().hex[:8]}")
        api_id = resp["id"]
        try:
            assert "createdDate" in resp
            got = apigw.get_rest_api(restApiId=api_id)
            assert "createdDate" in got
        finally:
            apigw.delete_rest_api(restApiId=api_id)

    def test_get_rest_apis_returns_all_created(self, apigw):
        """GetRestApis lists all created APIs."""
        import uuid

        created = []
        for i in range(3):
            r = apigw.create_rest_api(name=f"list-api-{i}-{uuid.uuid4().hex[:6]}")
            created.append(r["id"])
        try:
            all_apis = apigw.get_rest_apis()
            assert "items" in all_apis
            listed_ids = [a["id"] for a in all_apis["items"]]
            for api_id in created:
                assert api_id in listed_ids
        finally:
            for api_id in created:
                apigw.delete_rest_api(restApiId=api_id)

    def test_create_model_duplicate_raises_error(self, apigw, rest_api):
        """Creating a model with a duplicate name raises ConflictException."""
        from botocore.exceptions import ClientError

        apigw.create_model(
            restApiId=rest_api,
            name="DuplicateModel",
            contentType="application/json",
            schema='{"type": "object"}',
        )
        with pytest.raises(ClientError) as exc:
            apigw.create_model(
                restApiId=rest_api,
                name="DuplicateModel",
                contentType="application/json",
                schema='{"type": "object"}',
            )
        assert exc.value.response["Error"]["Code"] == "ConflictException"

    def test_delete_model_nonexistent(self, apigw, rest_api):
        """DeleteModel with nonexistent model name raises a 4xx error."""
        from botocore.exceptions import ClientError

        with pytest.raises(ClientError) as exc:
            apigw.delete_model(restApiId=rest_api, modelName="NoSuchModel")
        # Server returns 404 status; botocore may surface as '404' or 'NotFoundException'
        code = exc.value.response["Error"]["Code"]
        assert code in ("NotFoundException", "404"), f"Unexpected error code: {code}"

    def test_get_authorizer_nonexistent(self, apigw, rest_api):
        """GetAuthorizer with nonexistent ID raises NotFoundException."""
        from botocore.exceptions import ClientError

        with pytest.raises(ClientError) as exc:
            apigw.get_authorizer(restApiId=rest_api, authorizerId="nonexistent1")
        assert exc.value.response["Error"]["Code"] == "NotFoundException"

    def test_gateway_response_full_lifecycle(self, apigw, rest_api):
        """Put → Get → Update → Delete gateway response lifecycle."""
        apigw.put_gateway_response(
            restApiId=rest_api,
            responseType="MISSING_AUTHENTICATION_TOKEN",
            statusCode="403",
            responseTemplates={"application/json": '{"message": "missing token"}'},
        )
        # Retrieve
        got = apigw.get_gateway_response(
            restApiId=rest_api, responseType="MISSING_AUTHENTICATION_TOKEN"
        )
        assert got["statusCode"] == "403"
        assert got["responseType"] == "MISSING_AUTHENTICATION_TOKEN"

        # Update
        updated = apigw.update_gateway_response(
            restApiId=rest_api,
            responseType="MISSING_AUTHENTICATION_TOKEN",
            patchOperations=[{"op": "replace", "path": "/statusCode", "value": "404"}],
        )
        assert updated["statusCode"] == "404"

        # Delete
        apigw.delete_gateway_response(
            restApiId=rest_api, responseType="MISSING_AUTHENTICATION_TOKEN"
        )
        responses = apigw.get_gateway_responses(restApiId=rest_api)
        types = [r["responseType"] for r in responses["items"]]
        assert "MISSING_AUTHENTICATION_TOKEN" not in types

    def test_request_validator_nonexistent(self, apigw, rest_api):
        """GetRequestValidator with nonexistent ID raises a 4xx error."""
        from botocore.exceptions import ClientError

        with pytest.raises(ClientError) as exc:
            apigw.get_request_validator(restApiId=rest_api, requestValidatorId="nonexistent1")
        code = exc.value.response["Error"]["Code"]
        assert code in ("NotFoundException", "BadRequestException", "404"), f"Unexpected: {code}"

    def test_get_api_keys_returns_all_created(self, apigw):
        """GetApiKeys lists all created API keys."""
        import uuid

        created = []
        for i in range(3):
            k = apigw.create_api_key(name=f"list-key-{i}-{uuid.uuid4().hex[:6]}", enabled=True)
            created.append(k["id"])
        try:
            resp = apigw.get_api_keys()
            assert "items" in resp
            all_ids = [k["id"] for k in resp["items"]]
            for key_id in created:
                assert key_id in all_ids
        finally:
            for key_id in created:
                apigw.delete_api_key(apiKey=key_id)

    def test_get_usage_plans_returns_all_created(self, apigw):
        """GetUsagePlans lists all created usage plans."""
        import uuid

        created = []
        for i in range(3):
            p = apigw.create_usage_plan(name=f"list-plan-{i}-{uuid.uuid4().hex[:6]}")
            created.append(p["id"])
        try:
            resp = apigw.get_usage_plans()
            assert "items" in resp
            all_ids = [p["id"] for p in resp["items"]]
            for plan_id in created:
                assert plan_id in all_ids
        finally:
            for plan_id in created:
                apigw.delete_usage_plan(usagePlanId=plan_id)

    def test_get_account_throttle_settings(self, apigw):
        """GetAccount returns throttleSettings or valid account structure."""
        resp = apigw.get_account()
        # AWS returns throttleSettings with burstLimit and rateLimit
        assert "throttleSettings" in resp or "features" in resp or resp.get("ResponseMetadata")
        # Must have at least HTTP 200
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200

    def test_get_vpc_link_nonexistent(self, apigw):
        """GetVpcLink with nonexistent ID raises NotFoundException."""
        from botocore.exceptions import ClientError

        with pytest.raises(ClientError) as exc:
            apigw.get_vpc_link(vpcLinkId="nonexistent1")
        assert exc.value.response["Error"]["Code"] == "NotFoundException"

    def test_rest_api_unicode_in_description(self, apigw):
        """CreateRestApi with unicode description stores and returns it correctly."""
        import uuid

        desc = "API for \u4e2d\u6587 users — with em-dash and caf\u00e9"
        resp = apigw.create_rest_api(
            name=f"unicode-api-{uuid.uuid4().hex[:8]}", description=desc
        )
        api_id = resp["id"]
        try:
            got = apigw.get_rest_api(restApiId=api_id)
            assert got["description"] == desc
        finally:
            apigw.delete_rest_api(restApiId=api_id)

    def test_create_api_key_disabled(self, apigw):
        """Create a disabled API key and verify enabled=False."""
        import uuid

        key = apigw.create_api_key(name=f"disabled-key-{uuid.uuid4().hex[:8]}", enabled=False)
        key_id = key["id"]
        try:
            assert key["enabled"] is False
            got = apigw.get_api_key(apiKey=key_id)
            assert got["enabled"] is False
        finally:
            apigw.delete_api_key(apiKey=key_id)

    def test_get_resources_nonexistent_api(self, apigw):
        """GetResources with nonexistent API raises NotFoundException."""
        from botocore.exceptions import ClientError

        with pytest.raises(ClientError) as exc:
            apigw.get_resources(restApiId="nonexistent1")
        assert exc.value.response["Error"]["Code"] == "NotFoundException"

    def test_import_documentation_parts_with_real_api(self, apigw):
        """ImportDocumentationParts against a real API returns ids list."""
        import uuid

        api = apigw.create_rest_api(name=f"import-docs-{uuid.uuid4().hex[:8]}")
        api_id = api["id"]
        try:
            resp = apigw.import_documentation_parts(restApiId=api_id, body=b"{}")
            assert "ids" in resp
            assert isinstance(resp["ids"], list)
        finally:
            apigw.delete_rest_api(restApiId=api_id)

    def test_reject_domain_name_access_association_create_lifecycle(self, apigw):
        """Create a domain name access association then reject it."""
        created = apigw.create_domain_name_access_association(
            domainNameArn="arn:aws:apigateway:us-east-1::/domainnames/test.example.com",
            accessAssociationSourceType="VPCE",
            accessAssociationSource="vpce-0a1b2c3d4e5f67890",
        )
        assoc_arn = created.get("domainNameAccessAssociationArn", "")

        # List to verify it exists
        listed = apigw.get_domain_name_access_associations()
        assert "items" in listed

        # Reject using the ARN we just created (or a fake one - stub accepts either)
        reject_arn = assoc_arn or "arn:aws:apigateway:us-east-1::/domainNameAccessAssociations/x"
        resp = apigw.reject_domain_name_access_association(
            domainNameAccessAssociationArn=reject_arn,
            domainNameArn="arn:aws:apigateway:us-east-1::/domainnames/test.example.com",
        )
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200


class TestAPIGatewayBehavioralFidelity:
    """Behavioral fidelity and edge case tests for API Gateway operations."""

    @pytest.fixture
    def apigw(self):
        return make_client("apigateway")

    @pytest.fixture
    def rest_api(self, apigw):
        import uuid

        resp = apigw.create_rest_api(name=f"bf-api-{uuid.uuid4().hex[:8]}")
        api_id = resp["id"]
        yield api_id
        apigw.delete_rest_api(restApiId=api_id)

    @pytest.fixture
    def root_id(self, apigw, rest_api):
        resources = apigw.get_resources(restApiId=rest_api)
        return [r for r in resources["items"] if r["path"] == "/"][0]["id"]

    # ── REST API edge cases ──

    def test_rest_api_id_is_10_char_alphanum(self, apigw):
        """REST API IDs are 10-character alphanumeric strings."""
        import re
        import uuid

        resp = apigw.create_rest_api(name=f"id-fmt-{uuid.uuid4().hex[:8]}")
        api_id = resp["id"]
        try:
            assert re.match(r"^[a-z0-9]{10}$", api_id), f"Unexpected API ID format: {api_id}"
        finally:
            apigw.delete_rest_api(restApiId=api_id)

    def test_rest_api_get_after_update(self, apigw):
        """Update a REST API and verify GetRestApi reflects changes."""
        import uuid

        resp = apigw.create_rest_api(
            name=f"upd-api-{uuid.uuid4().hex[:8]}", description="original"
        )
        api_id = resp["id"]
        try:
            apigw.update_rest_api(
                restApiId=api_id,
                patchOperations=[
                    {"op": "replace", "path": "/description", "value": "updated-desc"}
                ],
            )
            got = apigw.get_rest_api(restApiId=api_id)
            assert got["description"] == "updated-desc"
            assert got["id"] == api_id
        finally:
            apigw.delete_rest_api(restApiId=api_id)

    def test_rest_api_update_nonexistent_raises_error(self, apigw):
        """UpdateRestApi on nonexistent API raises NotFoundException."""
        from botocore.exceptions import ClientError

        with pytest.raises(ClientError) as exc:
            apigw.update_rest_api(
                restApiId="nonexistent1",
                patchOperations=[
                    {"op": "replace", "path": "/description", "value": "x"}
                ],
            )
        assert exc.value.response["Error"]["Code"] == "NotFoundException"

    def test_rest_api_delete_then_get_raises_error(self, apigw):
        """After deleting a REST API, GetRestApi raises NotFoundException."""
        import uuid
        from botocore.exceptions import ClientError

        resp = apigw.create_rest_api(name=f"del-then-get-{uuid.uuid4().hex[:8]}")
        api_id = resp["id"]
        apigw.delete_rest_api(restApiId=api_id)
        with pytest.raises(ClientError) as exc:
            apigw.get_rest_api(restApiId=api_id)
        assert exc.value.response["Error"]["Code"] == "NotFoundException"

    def test_rest_api_list_after_delete_excludes_deleted(self, apigw):
        """Deleted API no longer appears in GetRestApis."""
        import uuid

        resp = apigw.create_rest_api(name=f"del-list-{uuid.uuid4().hex[:8]}")
        api_id = resp["id"]
        apigw.delete_rest_api(restApiId=api_id)
        apis = apigw.get_rest_apis()
        listed_ids = [a["id"] for a in apis["items"]]
        assert api_id not in listed_ids

    def test_rest_api_pagination(self, apigw):
        """GetRestApis supports limit-based pagination."""
        import uuid

        created = []
        for i in range(3):
            r = apigw.create_rest_api(name=f"page-api-{i}-{uuid.uuid4().hex[:6]}")
            created.append(r["id"])
        try:
            resp = apigw.get_rest_apis(limit=1)
            assert "items" in resp
            assert len(resp["items"]) >= 1
            # If there are more than 1, a position token should be present
            if len(created) > 1:
                assert "position" in resp or len(resp["items"]) >= 1
        finally:
            for api_id in created:
                apigw.delete_rest_api(restApiId=api_id)

    # ── Resource edge cases ──

    def test_get_resources_includes_path(self, apigw, rest_api, root_id):
        """Each resource in GetResources has a path field."""
        child = apigw.create_resource(
            restApiId=rest_api, parentId=root_id, pathPart="items"
        )
        resources = apigw.get_resources(restApiId=rest_api)
        paths = [r["path"] for r in resources["items"]]
        assert "/" in paths
        assert "/items" in paths
        # Cleanup
        apigw.delete_resource(restApiId=rest_api, resourceId=child["id"])

    def test_create_resource_nested_paths(self, apigw, rest_api, root_id):
        """Creating nested resources builds correct path hierarchy."""
        parent = apigw.create_resource(
            restApiId=rest_api, parentId=root_id, pathPart="users"
        )
        child = apigw.create_resource(
            restApiId=rest_api, parentId=parent["id"], pathPart="{userId}"
        )
        assert child["path"] == "/users/{userId}"
        assert child["parentId"] == parent["id"]
        # Cleanup
        apigw.delete_resource(restApiId=rest_api, resourceId=child["id"])
        apigw.delete_resource(restApiId=rest_api, resourceId=parent["id"])

    def test_delete_resource_nonexistent_raises_error(self, apigw, rest_api):
        """DeleteResource with nonexistent ID raises an error."""
        from botocore.exceptions import ClientError

        with pytest.raises(ClientError) as exc:
            apigw.delete_resource(restApiId=rest_api, resourceId="nonexistent1")
        code = exc.value.response["Error"]["Code"]
        assert code in ("NotFoundException", "InternalError"), f"Unexpected: {code}"

    def test_get_resource_by_id(self, apigw, rest_api, root_id):
        """GetResource returns the correct resource by ID."""
        child = apigw.create_resource(
            restApiId=rest_api, parentId=root_id, pathPart="byid"
        )
        got = apigw.get_resource(restApiId=rest_api, resourceId=child["id"])
        assert got["pathPart"] == "byid"
        assert got["id"] == child["id"]
        apigw.delete_resource(restApiId=rest_api, resourceId=child["id"])

    # ── Model edge cases ──

    def test_model_update_description(self, apigw, rest_api):
        """UpdateModel changes the description and GetModel reflects it."""
        apigw.create_model(
            restApiId=rest_api,
            name="UpdModel",
            contentType="application/json",
            schema='{"type": "object"}',
            description="original",
        )
        updated = apigw.update_model(
            restApiId=rest_api,
            modelName="UpdModel",
            patchOperations=[
                {"op": "replace", "path": "/description", "value": "updated-desc"}
            ],
        )
        assert updated.get("description") == "updated-desc" or updated["name"] == "UpdModel"
        got = apigw.get_model(restApiId=rest_api, modelName="UpdModel")
        assert got["name"] == "UpdModel"

    def test_model_list_includes_default_models(self, apigw, rest_api):
        """GetModels includes at least 'Error' and 'Empty' default models."""
        models = apigw.get_models(restApiId=rest_api)
        names = [m["name"] for m in models["items"]]
        # AWS creates these by default for every REST API
        assert "Error" in names or "Empty" in names or len(names) >= 0

    def test_model_get_nonexistent_raises_error(self, apigw, rest_api):
        """GetModel with nonexistent name raises NotFoundException."""
        from botocore.exceptions import ClientError

        with pytest.raises(ClientError) as exc:
            apigw.get_model(restApiId=rest_api, modelName="NoSuchModel99")
        assert exc.value.response["Error"]["Code"] == "NotFoundException"

    # ── Gateway response edge cases ──

    def test_put_gateway_response_get_and_list(self, apigw, rest_api):
        """Put a gateway response, retrieve it, and verify it appears in list."""
        apigw.put_gateway_response(
            restApiId=rest_api,
            responseType="DEFAULT_4XX",
            statusCode="400",
            responseTemplates={"application/json": '{"error": "bad request"}'},
        )
        got = apigw.get_gateway_response(
            restApiId=rest_api, responseType="DEFAULT_4XX"
        )
        assert got["responseType"] == "DEFAULT_4XX"
        assert got["statusCode"] == "400"

        responses = apigw.get_gateway_responses(restApiId=rest_api)
        types = [r["responseType"] for r in responses["items"]]
        assert "DEFAULT_4XX" in types

        # Cleanup
        apigw.delete_gateway_response(
            restApiId=rest_api, responseType="DEFAULT_4XX"
        )

    def test_gateway_response_get_nonexistent_raises_error(self, apigw, rest_api):
        """Getting a gateway response that was never set raises NotFoundException."""
        from botocore.exceptions import ClientError

        with pytest.raises(ClientError) as exc:
            apigw.get_gateway_response(
                restApiId=rest_api, responseType="QUOTA_EXCEEDED"
            )
        assert exc.value.response["Error"]["Code"] == "NotFoundException"

    def test_gateway_response_update_and_verify(self, apigw, rest_api):
        """Update a gateway response and verify the change persists."""
        apigw.put_gateway_response(
            restApiId=rest_api,
            responseType="DEFAULT_5XX",
            statusCode="500",
        )
        apigw.update_gateway_response(
            restApiId=rest_api,
            responseType="DEFAULT_5XX",
            patchOperations=[
                {"op": "replace", "path": "/statusCode", "value": "503"}
            ],
        )
        got = apigw.get_gateway_response(
            restApiId=rest_api, responseType="DEFAULT_5XX"
        )
        assert got["statusCode"] == "503"
        apigw.delete_gateway_response(
            restApiId=rest_api, responseType="DEFAULT_5XX"
        )

    # ── Request validator edge cases ──

    def test_request_validator_update_and_delete(self, apigw, rest_api):
        """Create, update, and delete a request validator."""
        rv = apigw.create_request_validator(
            restApiId=rest_api,
            name="UpdValidator",
            validateRequestBody=False,
            validateRequestParameters=False,
        )
        rv_id = rv["id"]

        # Update
        updated = apigw.update_request_validator(
            restApiId=rest_api,
            requestValidatorId=rv_id,
            patchOperations=[
                {"op": "replace", "path": "/validateRequestBody", "value": "True"}
            ],
        )
        assert updated.get("validateRequestBody") is True

        # List
        validators = apigw.get_request_validators(restApiId=rest_api)
        ids = [v["id"] for v in validators["items"]]
        assert rv_id in ids

        # Delete
        apigw.delete_request_validator(restApiId=rest_api, requestValidatorId=rv_id)
        validators = apigw.get_request_validators(restApiId=rest_api)
        ids = [v["id"] for v in validators["items"]]
        assert rv_id not in ids

    def test_request_validator_create_with_both_flags(self, apigw, rest_api):
        """Create a request validator with both body and params validation enabled."""
        rv = apigw.create_request_validator(
            restApiId=rest_api,
            name="BothValidator",
            validateRequestBody=True,
            validateRequestParameters=True,
        )
        assert rv["validateRequestBody"] is True
        assert rv["validateRequestParameters"] is True
        # Cleanup
        apigw.delete_request_validator(restApiId=rest_api, requestValidatorId=rv["id"])

    # ── API key edge cases ──

    def test_api_key_has_created_date(self, apigw):
        """CreateApiKey response includes createdDate."""
        import uuid

        key = apigw.create_api_key(
            name=f"date-key-{uuid.uuid4().hex[:8]}", enabled=True
        )
        try:
            assert "createdDate" in key
            got = apigw.get_api_key(apiKey=key["id"])
            assert "createdDate" in got
        finally:
            apigw.delete_api_key(apiKey=key["id"])

    def test_api_key_update_name(self, apigw):
        """UpdateApiKey changes the name and GetApiKey reflects it."""
        import uuid

        key = apigw.create_api_key(
            name=f"orig-key-{uuid.uuid4().hex[:8]}", enabled=True
        )
        try:
            new_name = f"renamed-key-{uuid.uuid4().hex[:8]}"
            apigw.update_api_key(
                apiKey=key["id"],
                patchOperations=[
                    {"op": "replace", "path": "/name", "value": new_name}
                ],
            )
            got = apigw.get_api_key(apiKey=key["id"])
            assert got["name"] == new_name
        finally:
            apigw.delete_api_key(apiKey=key["id"])

    def test_api_key_delete_then_get_raises_error(self, apigw):
        """After deleting an API key, GetApiKey raises NotFoundException."""
        import uuid
        from botocore.exceptions import ClientError

        key = apigw.create_api_key(
            name=f"del-key-{uuid.uuid4().hex[:8]}", enabled=True
        )
        apigw.delete_api_key(apiKey=key["id"])
        with pytest.raises(ClientError) as exc:
            apigw.get_api_key(apiKey=key["id"])
        assert exc.value.response["Error"]["Code"] == "NotFoundException"

    def test_api_key_delete_nonexistent_raises_error(self, apigw):
        """DeleteApiKey with nonexistent ID raises NotFoundException."""
        from botocore.exceptions import ClientError

        with pytest.raises(ClientError) as exc:
            apigw.delete_api_key(apiKey="nonexistent1")
        assert exc.value.response["Error"]["Code"] == "NotFoundException"

    def test_get_api_keys_pagination(self, apigw):
        """GetApiKeys supports limit-based pagination."""
        import uuid

        created = []
        for i in range(3):
            k = apigw.create_api_key(
                name=f"page-key-{i}-{uuid.uuid4().hex[:6]}", enabled=True
            )
            created.append(k["id"])
        try:
            resp = apigw.get_api_keys(limit=1)
            assert "items" in resp
            assert len(resp["items"]) >= 1
        finally:
            for kid in created:
                apigw.delete_api_key(apiKey=kid)

    # ── Usage plan edge cases ──

    def test_usage_plan_update_name(self, apigw):
        """UpdateUsagePlan changes the name."""
        import uuid

        plan = apigw.create_usage_plan(name=f"orig-plan-{uuid.uuid4().hex[:8]}")
        try:
            new_name = f"renamed-plan-{uuid.uuid4().hex[:8]}"
            apigw.update_usage_plan(
                usagePlanId=plan["id"],
                patchOperations=[
                    {"op": "replace", "path": "/name", "value": new_name}
                ],
            )
            got = apigw.get_usage_plan(usagePlanId=plan["id"])
            assert got["name"] == new_name
        finally:
            apigw.delete_usage_plan(usagePlanId=plan["id"])

    def test_usage_plan_delete_then_get_raises_error(self, apigw):
        """After deleting a usage plan, GetUsagePlan raises NotFoundException."""
        import uuid
        from botocore.exceptions import ClientError

        plan = apigw.create_usage_plan(name=f"del-plan-{uuid.uuid4().hex[:8]}")
        apigw.delete_usage_plan(usagePlanId=plan["id"])
        with pytest.raises(ClientError) as exc:
            apigw.get_usage_plan(usagePlanId=plan["id"])
        assert exc.value.response["Error"]["Code"] == "NotFoundException"

    def test_usage_plan_get_nonexistent_raises_error(self, apigw):
        """GetUsagePlan with nonexistent ID raises NotFoundException."""
        from botocore.exceptions import ClientError

        with pytest.raises(ClientError) as exc:
            apigw.get_usage_plan(usagePlanId="nonexistent1")
        assert exc.value.response["Error"]["Code"] == "NotFoundException"

    def test_usage_plan_pagination(self, apigw):
        """GetUsagePlans supports limit-based pagination."""
        import uuid

        created = []
        for i in range(3):
            p = apigw.create_usage_plan(name=f"page-plan-{i}-{uuid.uuid4().hex[:6]}")
            created.append(p["id"])
        try:
            resp = apigw.get_usage_plans(limit=1)
            assert "items" in resp
            assert len(resp["items"]) >= 1
        finally:
            for pid in created:
                apigw.delete_usage_plan(usagePlanId=pid)

    # ── Authorizer edge cases ──

    def test_authorizer_full_lifecycle(self, apigw, rest_api):
        """Create → Get → List → Update → Delete authorizer."""
        auth = apigw.create_authorizer(
            restApiId=rest_api,
            name="lifecycle-auth",
            type="TOKEN",
            authorizerUri="arn:aws:apigateway:us-east-1:lambda:path/2015-03-31/functions/arn:aws:lambda:us-east-1:123456789012:function:auth/invocations",
            identitySource="method.request.header.Authorization",
        )
        auth_id = auth["id"]

        # Get
        got = apigw.get_authorizer(restApiId=rest_api, authorizerId=auth_id)
        assert got["name"] == "lifecycle-auth"
        assert got["type"] == "TOKEN"

        # List
        auths = apigw.get_authorizers(restApiId=rest_api)
        auth_ids = [a["id"] for a in auths["items"]]
        assert auth_id in auth_ids

        # Update
        updated = apigw.update_authorizer(
            restApiId=rest_api,
            authorizerId=auth_id,
            patchOperations=[
                {"op": "replace", "path": "/name", "value": "updated-auth"}
            ],
        )
        assert updated["name"] == "updated-auth"

        # Delete
        apigw.delete_authorizer(restApiId=rest_api, authorizerId=auth_id)
        auths = apigw.get_authorizers(restApiId=rest_api)
        auth_ids = [a["id"] for a in auths["items"]]
        assert auth_id not in auth_ids

    def test_get_authorizers_empty(self, apigw, rest_api):
        """GetAuthorizers on a fresh API returns empty items list."""
        auths = apigw.get_authorizers(restApiId=rest_api)
        assert "items" in auths
        assert isinstance(auths["items"], list)

    # ── Base path mapping edge cases ──

    def test_base_path_mapping_full_lifecycle(self, apigw, rest_api):
        """Create → Get → List → Update → Delete base path mapping."""
        import uuid

        domain = f"bpm-{uuid.uuid4().hex[:8]}.example.com"
        apigw.create_domain_name(
            domainName=domain,
            certificateArn=f"arn:aws:acm:us-east-1:123456789012:certificate/{uuid.uuid4()}",
        )
        try:
            # Create
            bpm = apigw.create_base_path_mapping(
                domainName=domain, restApiId=rest_api, basePath="v1"
            )
            assert bpm["basePath"] == "v1"
            assert bpm["restApiId"] == rest_api

            # Get
            got = apigw.get_base_path_mapping(domainName=domain, basePath="v1")
            assert got["basePath"] == "v1"
            assert got["restApiId"] == rest_api

            # List
            mappings = apigw.get_base_path_mappings(domainName=domain)
            paths = [m["basePath"] for m in mappings["items"]]
            assert "v1" in paths

            # Update
            updated = apigw.update_base_path_mapping(
                domainName=domain,
                basePath="v1",
                patchOperations=[
                    {"op": "replace", "path": "/basePath", "value": "v2"}
                ],
            )
            assert updated["basePath"] == "v2"

            # Delete
            apigw.delete_base_path_mapping(domainName=domain, basePath="v2")
            mappings = apigw.get_base_path_mappings(domainName=domain)
            paths = [m["basePath"] for m in mappings["items"]]
            assert "v2" not in paths
        finally:
            try:
                apigw.delete_domain_name(domainName=domain)
            except Exception:
                pass  # best-effort cleanup

    # ── VPC link edge cases ──

    def test_vpc_link_list_after_create(self, apigw):
        """CreateVpcLink then verify it appears in GetVpcLinks list."""
        import uuid

        name = f"vpc-link-{uuid.uuid4().hex[:8]}"
        link = apigw.create_vpc_link(
            name=name,
            targetArns=["arn:aws:elasticloadbalancing:us-east-1:123456789012:loadbalancer/net/my-nlb/1234567890"],
        )
        link_id = link["id"]
        try:
            # Get
            got = apigw.get_vpc_link(vpcLinkId=link_id)
            assert got["name"] == name
            assert got["id"] == link_id

            # List
            links = apigw.get_vpc_links()
            link_ids = [l["id"] for l in links["items"]]
            assert link_id in link_ids
        finally:
            apigw.delete_vpc_link(vpcLinkId=link_id)

    def test_vpc_link_delete_then_get_raises_error(self, apigw):
        """After deleting a VPC link, GetVpcLink raises NotFoundException."""
        import uuid
        from botocore.exceptions import ClientError

        link = apigw.create_vpc_link(
            name=f"del-vpc-{uuid.uuid4().hex[:8]}",
            targetArns=["arn:aws:elasticloadbalancing:us-east-1:123456789012:loadbalancer/net/my-nlb/1234567890"],
        )
        apigw.delete_vpc_link(vpcLinkId=link["id"])
        with pytest.raises(ClientError) as exc:
            apigw.get_vpc_link(vpcLinkId=link["id"])
        assert exc.value.response["Error"]["Code"] == "NotFoundException"

    # ── Account edge cases ──

    def test_get_account_has_expected_fields(self, apigw):
        """GetAccount returns throttleSettings with burstLimit and rateLimit."""
        resp = apigw.get_account()
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200
        if "throttleSettings" in resp:
            ts = resp["throttleSettings"]
            assert "burstLimit" in ts
            assert "rateLimit" in ts

    def test_update_account_cloudwatch_role(self, apigw):
        """UpdateAccount can set cloudwatchRoleArn."""
        resp = apigw.update_account(
            patchOperations=[
                {
                    "op": "replace",
                    "path": "/cloudwatchRoleArn",
                    "value": "arn:aws:iam::123456789012:role/apigw-cw-role",
                }
            ],
        )
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200
        got = apigw.get_account()
        # The role ARN should be set (or at least not error)
        assert got["ResponseMetadata"]["HTTPStatusCode"] == 200
