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
        response = apigw.create_resource(
            restApiId=rest_api, parentId=root_id, pathPart="users"
        )
        assert response["pathPart"] == "users"

    def test_put_method(self, apigw, rest_api):
        resources = apigw.get_resources(restApiId=rest_api)
        root_id = [r for r in resources["items"] if r["path"] == "/"][0]["id"]
        resource = apigw.create_resource(
            restApiId=rest_api, parentId=root_id, pathPart="items"
        )
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
