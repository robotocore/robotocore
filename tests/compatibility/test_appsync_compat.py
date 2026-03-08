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
        resp = appsync.create_graphql_api(name=name, authenticationType="API_KEY")
        api = resp["graphqlApi"]
        assert api["name"] == name
        assert "apiId" in api
        appsync.delete_graphql_api(apiId=api["apiId"])

    def test_get_graphql_api(self, appsync):
        name = _unique("get-api")
        created = appsync.create_graphql_api(name=name, authenticationType="API_KEY")
        api_id = created["graphqlApi"]["apiId"]
        try:
            resp = appsync.get_graphql_api(apiId=api_id)
            assert resp["graphqlApi"]["name"] == name
        finally:
            appsync.delete_graphql_api(apiId=api_id)

    def test_list_graphql_apis(self, appsync):
        name = _unique("list-api")
        created = appsync.create_graphql_api(name=name, authenticationType="API_KEY")
        api_id = created["graphqlApi"]["apiId"]
        try:
            resp = appsync.list_graphql_apis()
            names = [a["name"] for a in resp["graphqlApis"]]
            assert name in names
        finally:
            appsync.delete_graphql_api(apiId=api_id)

    def test_update_graphql_api(self, appsync):
        name = _unique("upd-api")
        created = appsync.create_graphql_api(name=name, authenticationType="API_KEY")
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
        created = appsync.create_graphql_api(name=name, authenticationType="API_KEY")
        api_id = created["graphqlApi"]["apiId"]
        appsync.delete_graphql_api(apiId=api_id)
        resp = appsync.list_graphql_apis()
        ids = [a["apiId"] for a in resp["graphqlApis"]]
        assert api_id not in ids


class TestApiKeys:
    @pytest.fixture
    def api(self, appsync):
        created = appsync.create_graphql_api(name=_unique("key-api"), authenticationType="API_KEY")
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
        created = appsync.create_graphql_api(name=_unique("ds-api"), authenticationType="API_KEY")
        api_id = created["graphqlApi"]["apiId"]
        yield api_id
        appsync.delete_graphql_api(apiId=api_id)

    def test_create_data_source(self, appsync, api):
        name = _unique("ds")
        resp = appsync.create_data_source(apiId=api, name=name, type="NONE")
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

    def test_update_data_source(self, appsync, api):
        name = _unique("ds-upd")
        appsync.create_data_source(apiId=api, name=name, type="NONE")
        resp = appsync.update_data_source(
            apiId=api,
            name=name,
            type="NONE",
            description="updated desc",
        )
        assert resp["dataSource"]["name"] == name


class TestAppSyncExtended:
    @pytest.fixture
    def appsync(self):
        return make_client("appsync")

    @pytest.fixture
    def api(self, appsync):
        created = appsync.create_graphql_api(name=_unique("ext-api"), authenticationType="API_KEY")
        api_id = created["graphqlApi"]["apiId"]
        yield api_id
        appsync.delete_graphql_api(apiId=api_id)

    def test_create_api_with_cognito_auth(self, appsync):
        name = _unique("cognito-api")
        resp = appsync.create_graphql_api(
            name=name,
            authenticationType="AMAZON_COGNITO_USER_POOLS",
            userPoolConfig={
                "userPoolId": "us-east-1_fake",
                "awsRegion": "us-east-1",
                "defaultAction": "ALLOW",
            },
        )
        api_id = resp["graphqlApi"]["apiId"]
        try:
            assert resp["graphqlApi"]["authenticationType"] == "AMAZON_COGNITO_USER_POOLS"
        finally:
            appsync.delete_graphql_api(apiId=api_id)

    def test_create_api_with_iam_auth(self, appsync):
        name = _unique("iam-api")
        resp = appsync.create_graphql_api(name=name, authenticationType="AWS_IAM")
        api_id = resp["graphqlApi"]["apiId"]
        try:
            assert resp["graphqlApi"]["authenticationType"] == "AWS_IAM"
        finally:
            appsync.delete_graphql_api(apiId=api_id)

    def test_api_has_uris(self, appsync, api):
        resp = appsync.get_graphql_api(apiId=api)
        assert "uris" in resp["graphqlApi"]

    def test_api_has_arn(self, appsync, api):
        resp = appsync.get_graphql_api(apiId=api)
        assert "arn" in resp["graphqlApi"]
        assert "appsync" in resp["graphqlApi"]["arn"]

    def test_create_multiple_api_keys(self, appsync, api):
        k1 = appsync.create_api_key(apiId=api)
        k2 = appsync.create_api_key(apiId=api)
        resp = appsync.list_api_keys(apiId=api)
        assert len(resp["apiKeys"]) >= 2
        ids = {k["id"] for k in resp["apiKeys"]}
        assert k1["apiKey"]["id"] in ids
        assert k2["apiKey"]["id"] in ids

    def test_create_data_source_http(self, appsync, api):
        name = _unique("http-ds")
        resp = appsync.create_data_source(
            apiId=api,
            name=name,
            type="HTTP",
            httpConfig={"endpoint": "https://example.com"},
        )
        assert resp["dataSource"]["type"] == "HTTP"

    def test_update_graphql_api_auth_type(self, appsync, api):
        resp = appsync.update_graphql_api(
            apiId=api,
            name=_unique("upd-auth"),
            authenticationType="AWS_IAM",
        )
        assert resp["graphqlApi"]["authenticationType"] == "AWS_IAM"

    def test_start_schema_creation(self, appsync, api):
        schema = b"type Query { hello: String }"
        import base64

        resp = appsync.start_schema_creation(
            apiId=api,
            definition=base64.b64encode(schema),
        )
        assert resp["status"] in ("ACTIVE", "PROCESSING", "SUCCESS")

    def test_list_apis_returns_all(self, appsync):
        apis = []
        for i in range(3):
            r = appsync.create_graphql_api(name=_unique(f"list-{i}"), authenticationType="API_KEY")
            apis.append(r["graphqlApi"]["apiId"])
        try:
            resp = appsync.list_graphql_apis()
            ids = {a["apiId"] for a in resp["graphqlApis"]}
            for api_id in apis:
                assert api_id in ids
        finally:
            for api_id in apis:
                appsync.delete_graphql_api(apiId=api_id)

    def test_tag_graphql_api(self, appsync, api):
        resp = appsync.get_graphql_api(apiId=api)
        arn = resp["graphqlApi"]["arn"]
        appsync.tag_resource(
            resourceArn=arn,
            tags={"env": "test"},
        )
        tags_resp = appsync.list_tags_for_resource(resourceArn=arn)
        assert tags_resp["tags"]["env"] == "test"

    def test_untag_graphql_api(self, appsync, api):
        resp = appsync.get_graphql_api(apiId=api)
        arn = resp["graphqlApi"]["arn"]
        appsync.tag_resource(resourceArn=arn, tags={"temp": "yes"})
        appsync.untag_resource(resourceArn=arn, tagKeys=["temp"])
        tags_resp = appsync.list_tags_for_resource(resourceArn=arn)
        assert "temp" not in tags_resp.get("tags", {})


class TestAppsyncAutoCoverage:
    """Auto-generated coverage tests for appsync."""

    @pytest.fixture
    def client(self):
        return make_client("appsync")

    def test_list_apis(self, client):
        """ListApis returns a response."""
        client.list_apis()
