"""AppSync GraphQL API compatibility tests."""

import uuid

import pytest

from tests.compatibility.conftest import make_client


@pytest.fixture
def appsync():
    return make_client("appsync")


def _unique(prefix: str) -> str:
    return f"{prefix}-{uuid.uuid4().hex[:8]}"


class TestGraphQLApiCrud:
    def test_create_graphql_api(self, appsync):
        name = _unique("test-api")
        resp = appsync.create_graphql_api(
            name=name, authenticationType="API_KEY"
        )
        api = resp["graphqlApi"]
        assert api["name"] == name
        assert "apiId" in api
        appsync.delete_graphql_api(apiId=api["apiId"])

    def test_get_graphql_api(self, appsync):
        name = _unique("get-api")
        created = appsync.create_graphql_api(
            name=name, authenticationType="API_KEY"
        )
        api_id = created["graphqlApi"]["apiId"]
        try:
            resp = appsync.get_graphql_api(apiId=api_id)
            assert resp["graphqlApi"]["name"] == name
        finally:
            appsync.delete_graphql_api(apiId=api_id)

    def test_list_graphql_apis(self, appsync):
        name = _unique("list-api")
        created = appsync.create_graphql_api(
            name=name, authenticationType="API_KEY"
        )
        api_id = created["graphqlApi"]["apiId"]
        try:
            resp = appsync.list_graphql_apis()
            names = [a["name"] for a in resp["graphqlApis"]]
            assert name in names
        finally:
            appsync.delete_graphql_api(apiId=api_id)

    def test_update_graphql_api(self, appsync):
        name = _unique("upd-api")
        created = appsync.create_graphql_api(
            name=name, authenticationType="API_KEY"
        )
        api_id = created["graphqlApi"]["apiId"]
        try:
            new_name = _unique("updated")
            resp = appsync.update_graphql_api(
                apiId=api_id, name=new_name, authenticationType="API_KEY"
            )
            assert resp["graphqlApi"]["name"] == new_name
        finally:
            appsync.delete_graphql_api(apiId=api_id)

    def test_delete_graphql_api(self, appsync):
        name = _unique("del-api")
        created = appsync.create_graphql_api(
            name=name, authenticationType="API_KEY"
        )
        api_id = created["graphqlApi"]["apiId"]
        appsync.delete_graphql_api(apiId=api_id)
        resp = appsync.list_graphql_apis()
        ids = [a["apiId"] for a in resp["graphqlApis"]]
        assert api_id not in ids


class TestApiKeys:
    @pytest.fixture
    def api(self, appsync):
        created = appsync.create_graphql_api(
            name=_unique("key-api"), authenticationType="API_KEY"
        )
        api_id = created["graphqlApi"]["apiId"]
        yield api_id
        appsync.delete_graphql_api(apiId=api_id)

    def test_create_api_key(self, appsync, api):
        resp = appsync.create_api_key(apiId=api)
        assert "apiKey" in resp
        assert "id" in resp["apiKey"]

    def test_list_api_keys(self, appsync, api):
        appsync.create_api_key(apiId=api)
        resp = appsync.list_api_keys(apiId=api)
        assert len(resp["apiKeys"]) >= 1

    def test_delete_api_key(self, appsync, api):
        key = appsync.create_api_key(apiId=api)
        key_id = key["apiKey"]["id"]
        appsync.delete_api_key(apiId=api, id=key_id)
        resp = appsync.list_api_keys(apiId=api)
        ids = [k["id"] for k in resp["apiKeys"]]
        assert key_id not in ids


class TestDataSources:
    @pytest.fixture
    def api(self, appsync):
        created = appsync.create_graphql_api(
            name=_unique("ds-api"), authenticationType="API_KEY"
        )
        api_id = created["graphqlApi"]["apiId"]
        yield api_id
        appsync.delete_graphql_api(apiId=api_id)

    def test_create_data_source(self, appsync, api):
        name = _unique("ds")
        resp = appsync.create_data_source(
            apiId=api, name=name, type="NONE"
        )
        assert resp["dataSource"]["name"] == name

    def test_list_data_sources(self, appsync, api):
        name = _unique("ds-list")
        appsync.create_data_source(apiId=api, name=name, type="NONE")
        resp = appsync.list_data_sources(apiId=api)
        names = [d["name"] for d in resp["dataSources"]]
        assert name in names

    def test_delete_data_source(self, appsync, api):
        name = _unique("ds-del")
        appsync.create_data_source(apiId=api, name=name, type="NONE")
        appsync.delete_data_source(apiId=api, name=name)
        resp = appsync.list_data_sources(apiId=api)
        names = [d["name"] for d in resp["dataSources"]]
        assert name not in names

    def test_get_data_source(self, appsync, api):
        name = _unique("ds-get")
        appsync.create_data_source(apiId=api, name=name, type="NONE")
        resp = appsync.get_data_source(apiId=api, name=name)
        assert resp["dataSource"]["name"] == name
        assert resp["dataSource"]["type"] == "NONE"
