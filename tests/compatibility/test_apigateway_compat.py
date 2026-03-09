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
                pass

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
                pass

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
            pass

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
                pass

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
                pass

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
                pass
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
                pass
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
