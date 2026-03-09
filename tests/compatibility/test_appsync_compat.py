"""AppSync GraphQL API compatibility tests."""

import base64
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


class TestChannelNamespace:
    """ChannelNamespace CRUD operations (Event API)."""

    @pytest.fixture
    def client(self):
        return make_client("appsync")

    @pytest.fixture
    def event_api(self, client):
        auth_mode = {"authType": "API_KEY"}
        resp = client.create_api(
            name=_unique("evt-api"),
            eventConfig={
                "authProviders": [auth_mode],
                "connectionAuthModes": [auth_mode],
                "defaultPublishAuthModes": [auth_mode],
                "defaultSubscribeAuthModes": [auth_mode],
            },
        )
        api_id = resp["api"]["apiId"]
        yield api_id
        client.delete_api(apiId=api_id)

    def test_create_channel_namespace(self, client, event_api):
        name = _unique("chan")
        resp = client.create_channel_namespace(apiId=event_api, name=name)
        assert "channelNamespace" in resp
        assert resp["channelNamespace"]["name"] == name
        client.delete_channel_namespace(apiId=event_api, name=name)

    def test_get_channel_namespace(self, client, event_api):
        name = _unique("chan-get")
        client.create_channel_namespace(apiId=event_api, name=name)
        try:
            resp = client.get_channel_namespace(apiId=event_api, name=name)
            assert resp["channelNamespace"]["name"] == name
            assert "apiId" in resp["channelNamespace"]
        finally:
            client.delete_channel_namespace(apiId=event_api, name=name)

    def test_update_channel_namespace(self, client, event_api):
        name = _unique("chan-upd")
        client.create_channel_namespace(apiId=event_api, name=name)
        try:
            resp = client.update_channel_namespace(
                apiId=event_api,
                name=name,
                subscribeAuthModes=[{"authType": "API_KEY"}],
            )
            assert "channelNamespace" in resp
            assert resp["channelNamespace"]["name"] == name
        finally:
            client.delete_channel_namespace(apiId=event_api, name=name)

    def test_delete_channel_namespace(self, client, event_api):
        name = _unique("chan-del")
        client.create_channel_namespace(apiId=event_api, name=name)
        client.delete_channel_namespace(apiId=event_api, name=name)
        # Verify it's gone by trying to get it
        with pytest.raises(Exception):
            client.get_channel_namespace(apiId=event_api, name=name)


class TestResolversAndTypes:
    """Tests for resolver and type operations."""

    @pytest.fixture
    def client(self):
        return make_client("appsync")

    @pytest.fixture
    def api_with_schema(self, client):

        created = client.create_graphql_api(
            name=_unique("resolver-api"), authenticationType="API_KEY"
        )
        api_id = created["graphqlApi"]["apiId"]
        schema = b"type Query { hello: String }"
        client.start_schema_creation(apiId=api_id, definition=base64.b64encode(schema))
        # Create a NONE data source for resolvers
        client.create_data_source(apiId=api_id, name="noneds", type="NONE")
        yield api_id
        client.delete_graphql_api(apiId=api_id)

    def test_get_resolver_not_found(self, client, api_with_schema):
        """GetResolver with a non-existent field returns NotFoundException."""
        with pytest.raises(client.exceptions.NotFoundException):
            client.get_resolver(apiId=api_with_schema, typeName="Query", fieldName="nonexistent")

    def test_create_and_get_resolver(self, client, api_with_schema):
        """Create a resolver then retrieve it with GetResolver."""
        resp = client.create_resolver(
            apiId=api_with_schema,
            typeName="Query",
            fieldName="hello",
            dataSourceName="noneds",
        )
        assert resp["resolver"]["fieldName"] == "hello"
        get_resp = client.get_resolver(apiId=api_with_schema, typeName="Query", fieldName="hello")
        assert get_resp["resolver"]["fieldName"] == "hello"
        assert get_resp["resolver"]["typeName"] == "Query"

    def test_list_resolvers_empty(self, client, api_with_schema):
        """ListResolvers on a type with no resolvers returns empty list."""
        resp = client.list_resolvers(apiId=api_with_schema, typeName="Query")
        assert "resolvers" in resp
        assert isinstance(resp["resolvers"], list)

    def test_list_resolvers_with_resolver(self, client, api_with_schema):
        """ListResolvers returns created resolvers."""
        client.create_resolver(
            apiId=api_with_schema,
            typeName="Query",
            fieldName="hello",
            dataSourceName="noneds",
        )
        resp = client.list_resolvers(apiId=api_with_schema, typeName="Query")
        fields = [r["fieldName"] for r in resp["resolvers"]]
        assert "hello" in fields

    def test_get_schema_creation_status(self, client, api_with_schema):
        """GetSchemaCreationStatus returns status for API with schema."""
        resp = client.get_schema_creation_status(apiId=api_with_schema)
        assert "status" in resp
        assert resp["status"] in ("ACTIVE", "SUCCESS", "PROCESSING", "NOT_APPLICABLE")

    def test_get_type_not_found(self, client, api_with_schema):
        """GetType with non-existent type returns NotFoundException."""
        with pytest.raises(client.exceptions.NotFoundException):
            client.get_type(apiId=api_with_schema, typeName="NonExistentType", format="SDL")

    def test_list_types(self, client, api_with_schema):
        """ListTypes returns types defined in the schema."""
        resp = client.list_types(apiId=api_with_schema, format="SDL")
        assert "types" in resp
        assert isinstance(resp["types"], list)


class TestEventApiOps:
    """Tests for Event API operations (GetApi, ListChannelNamespaces)."""

    @pytest.fixture
    def client(self):
        return make_client("appsync")

    @pytest.fixture
    def event_api(self, client):
        auth_mode = {"authType": "API_KEY"}
        resp = client.create_api(
            name=_unique("evt-ops"),
            eventConfig={
                "authProviders": [auth_mode],
                "connectionAuthModes": [auth_mode],
                "defaultPublishAuthModes": [auth_mode],
                "defaultSubscribeAuthModes": [auth_mode],
            },
        )
        api_id = resp["api"]["apiId"]
        yield api_id
        client.delete_api(apiId=api_id)

    def test_get_api(self, client, event_api):
        """GetApi retrieves an Event API by ID."""
        resp = client.get_api(apiId=event_api)
        assert "api" in resp
        assert resp["api"]["apiId"] == event_api

    def test_get_api_not_found(self, client):
        """GetApi with a fake ID returns NotFoundException."""
        with pytest.raises(client.exceptions.NotFoundException):
            client.get_api(apiId="fake-api-id-12345")

    def test_list_channel_namespaces_empty(self, client, event_api):
        """ListChannelNamespaces on API with no namespaces returns empty list."""
        resp = client.list_channel_namespaces(apiId=event_api)
        assert "channelNamespaces" in resp
        assert isinstance(resp["channelNamespaces"], list)

    def test_list_channel_namespaces_with_namespace(self, client, event_api):
        """ListChannelNamespaces returns created namespaces."""
        name = _unique("ns-list")
        client.create_channel_namespace(apiId=event_api, name=name)
        try:
            resp = client.list_channel_namespaces(apiId=event_api)
            names = [ns["name"] for ns in resp["channelNamespaces"]]
            assert name in names
        finally:
            client.delete_channel_namespace(apiId=event_api, name=name)


class TestApiCache:
    """Tests for API cache CRUD operations."""

    @pytest.fixture
    def client(self):
        return make_client("appsync")

    @pytest.fixture
    def api(self, client):
        created = client.create_graphql_api(name=_unique("cache-api"), authenticationType="API_KEY")
        api_id = created["graphqlApi"]["apiId"]
        yield api_id
        client.delete_graphql_api(apiId=api_id)

    def test_create_api_cache(self, client, api):
        resp = client.create_api_cache(
            apiId=api,
            apiCachingBehavior="FULL_REQUEST_CACHING",
            type="T2_SMALL",
            ttl=3600,
            transitEncryptionEnabled=False,
        )
        assert "apiCache" in resp
        assert resp["apiCache"]["type"] == "T2_SMALL"
        assert resp["apiCache"]["status"] == "AVAILABLE"

    def test_get_api_cache(self, client, api):
        client.create_api_cache(
            apiId=api,
            apiCachingBehavior="FULL_REQUEST_CACHING",
            type="T2_SMALL",
            ttl=3600,
            transitEncryptionEnabled=False,
        )
        resp = client.get_api_cache(apiId=api)
        assert "apiCache" in resp
        assert resp["apiCache"]["ttl"] == 3600

    def test_update_api_cache(self, client, api):
        client.create_api_cache(
            apiId=api,
            apiCachingBehavior="FULL_REQUEST_CACHING",
            type="T2_SMALL",
            ttl=3600,
            transitEncryptionEnabled=False,
        )
        resp = client.update_api_cache(
            apiId=api,
            apiCachingBehavior="PER_RESOLVER_CACHING",
            type="T2_MEDIUM",
            ttl=7200,
        )
        assert resp["apiCache"]["type"] == "T2_MEDIUM"
        assert resp["apiCache"]["ttl"] == 7200

    def test_delete_api_cache(self, client, api):
        client.create_api_cache(
            apiId=api,
            apiCachingBehavior="FULL_REQUEST_CACHING",
            type="T2_SMALL",
            ttl=3600,
            transitEncryptionEnabled=False,
        )
        client.delete_api_cache(apiId=api)
        with pytest.raises(Exception):
            client.get_api_cache(apiId=api)

    def test_flush_api_cache(self, client, api):
        client.create_api_cache(
            apiId=api,
            apiCachingBehavior="FULL_REQUEST_CACHING",
            type="T2_SMALL",
            ttl=3600,
            transitEncryptionEnabled=False,
        )
        # flush_api_cache should succeed without error
        resp = client.flush_api_cache(apiId=api)
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200


class TestUpdateApiKey:
    """Tests for update_api_key operation."""

    @pytest.fixture
    def client(self):
        return make_client("appsync")

    @pytest.fixture
    def api(self, client):
        created = client.create_graphql_api(
            name=_unique("updkey-api"), authenticationType="API_KEY"
        )
        api_id = created["graphqlApi"]["apiId"]
        yield api_id
        client.delete_graphql_api(apiId=api_id)

    def test_update_api_key_description(self, client, api):
        key = client.create_api_key(apiId=api, description="original desc")
        key_id = key["apiKey"]["id"]
        resp = client.update_api_key(apiId=api, id=key_id, description="updated desc")
        assert "apiKey" in resp
        assert resp["apiKey"]["description"] == "updated desc"


class TestIntrospectionSchema:
    """Tests for get_introspection_schema operation."""

    @pytest.fixture
    def client(self):
        return make_client("appsync")

    @pytest.fixture
    def api_with_schema(self, client):

        created = client.create_graphql_api(name=_unique("intro-api"), authenticationType="API_KEY")
        api_id = created["graphqlApi"]["apiId"]
        schema = b"type Query { hello: String }"
        client.start_schema_creation(apiId=api_id, definition=base64.b64encode(schema))
        yield api_id
        client.delete_graphql_api(apiId=api_id)

    def test_get_introspection_schema_sdl(self, client, api_with_schema):
        resp = client.get_introspection_schema(apiId=api_with_schema, format="SDL")
        assert "schema" in resp
        # The schema body is a StreamingBody
        body = resp["schema"].read()
        assert len(body) > 0


class TestAppsyncTypeOperations:
    """Tests for Type CRUD operations."""

    @pytest.fixture
    def client(self):
        return make_client("appsync")

    @pytest.fixture
    def api_with_schema(self, client):

        created = client.create_graphql_api(name=_unique("type-api"), authenticationType="API_KEY")
        api_id = created["graphqlApi"]["apiId"]
        schema = b"type Query { hello: String }"
        client.start_schema_creation(apiId=api_id, definition=base64.b64encode(schema))
        yield api_id
        client.delete_graphql_api(apiId=api_id)

    def test_create_type(self, client, api_with_schema):
        """CreateType adds a new type to the API."""
        resp = client.create_type(
            apiId=api_with_schema,
            definition="type Mutation { addItem(name: String): String }",
            format="SDL",
        )
        assert "type" in resp
        assert resp["type"]["name"] == "Mutation"

    def test_create_and_get_type(self, client, api_with_schema):
        """GetType retrieves a type that was explicitly created."""
        client.create_type(
            apiId=api_with_schema,
            definition="type Item { id: ID name: String }",
            format="SDL",
        )
        resp = client.get_type(
            apiId=api_with_schema,
            typeName="Item",
            format="SDL",
        )
        assert "type" in resp
        assert resp["type"]["name"] == "Item"

    def test_create_type_and_list(self, client, api_with_schema):
        """CreateType adds a type that appears in ListTypes."""
        client.create_type(
            apiId=api_with_schema,
            definition="type Subscription { onEvent: String }",
            format="SDL",
        )
        resp = client.list_types(apiId=api_with_schema, format="SDL")
        names = [t["name"] for t in resp["types"]]
        assert "Subscription" in names


class TestAppsyncDeleteResolver:
    """Tests for DeleteResolver operation."""

    @pytest.fixture
    def client(self):
        return make_client("appsync")

    @pytest.fixture
    def api_with_schema(self, client):

        created = client.create_graphql_api(
            name=_unique("delres-api"), authenticationType="API_KEY"
        )
        api_id = created["graphqlApi"]["apiId"]
        schema = b"type Query { hello: String }"
        client.start_schema_creation(apiId=api_id, definition=base64.b64encode(schema))
        client.create_data_source(apiId=api_id, name="noneds", type="NONE")
        yield api_id
        client.delete_graphql_api(apiId=api_id)

    def test_delete_resolver(self, client, api_with_schema):
        """DeleteResolver removes a resolver."""
        client.create_resolver(
            apiId=api_with_schema,
            typeName="Query",
            fieldName="hello",
            dataSourceName="noneds",
        )
        client.delete_resolver(
            apiId=api_with_schema,
            typeName="Query",
            fieldName="hello",
        )
        # Verify it's gone
        with pytest.raises(client.exceptions.NotFoundException):
            client.get_resolver(apiId=api_with_schema, typeName="Query", fieldName="hello")


class TestAppsyncEventApiExtended:
    """Extended tests for Event API (CreateApi/UpdateApi)."""

    @pytest.fixture
    def client(self):
        return make_client("appsync")

    def test_create_api_returns_fields(self, client):
        """CreateApi returns api with expected fields."""
        auth_mode = {"authType": "API_KEY"}
        resp = client.create_api(
            name=_unique("evt-fields"),
            eventConfig={
                "authProviders": [auth_mode],
                "connectionAuthModes": [auth_mode],
                "defaultPublishAuthModes": [auth_mode],
                "defaultSubscribeAuthModes": [auth_mode],
            },
        )
        api = resp["api"]
        assert "apiId" in api
        assert "name" in api
        assert "apiArn" in api
        client.delete_api(apiId=api["apiId"])

    def test_update_api(self, client):
        """UpdateApi changes the API name."""
        auth_mode = {"authType": "API_KEY"}
        resp = client.create_api(
            name=_unique("evt-upd"),
            eventConfig={
                "authProviders": [auth_mode],
                "connectionAuthModes": [auth_mode],
                "defaultPublishAuthModes": [auth_mode],
                "defaultSubscribeAuthModes": [auth_mode],
            },
        )
        api_id = resp["api"]["apiId"]
        try:
            new_name = _unique("evt-updated")
            upd_resp = client.update_api(
                apiId=api_id,
                name=new_name,
                eventConfig={
                    "authProviders": [auth_mode],
                    "connectionAuthModes": [auth_mode],
                    "defaultPublishAuthModes": [auth_mode],
                    "defaultSubscribeAuthModes": [auth_mode],
                },
            )
            assert upd_resp["api"]["name"] == new_name
        finally:
            client.delete_api(apiId=api_id)

    def test_list_apis_includes_created(self, client):
        """ListApis includes a newly created Event API."""
        auth_mode = {"authType": "API_KEY"}
        resp = client.create_api(
            name=_unique("evt-list"),
            eventConfig={
                "authProviders": [auth_mode],
                "connectionAuthModes": [auth_mode],
                "defaultPublishAuthModes": [auth_mode],
                "defaultSubscribeAuthModes": [auth_mode],
            },
        )
        api_id = resp["api"]["apiId"]
        try:
            list_resp = client.list_apis()
            ids = [a["apiId"] for a in list_resp["apis"]]
            assert api_id in ids
        finally:
            client.delete_api(apiId=api_id)


class TestAppsyncResolverUpdate:
    """Tests for resolver update operations."""

    @pytest.fixture
    def client(self):
        return make_client("appsync")

    @pytest.fixture
    def api_with_resolver(self, client):

        created = client.create_graphql_api(
            name=_unique("updres-api"), authenticationType="API_KEY"
        )
        api_id = created["graphqlApi"]["apiId"]
        schema = b"type Query { hello: String }"
        client.start_schema_creation(apiId=api_id, definition=base64.b64encode(schema))
        client.create_data_source(apiId=api_id, name="noneds", type="NONE")
        client.create_resolver(
            apiId=api_id,
            typeName="Query",
            fieldName="hello",
            dataSourceName="noneds",
        )
        yield api_id
        client.delete_graphql_api(apiId=api_id)

    def test_get_resolver_after_create(self, client, api_with_resolver):
        """GetResolver returns resolver details."""
        resp = client.get_resolver(apiId=api_with_resolver, typeName="Query", fieldName="hello")
        assert resp["resolver"]["fieldName"] == "hello"
        assert resp["resolver"]["dataSourceName"] == "noneds"

    def test_list_resolvers_count(self, client, api_with_resolver):
        """ListResolvers shows exact count of resolvers."""
        resp = client.list_resolvers(apiId=api_with_resolver, typeName="Query")
        assert len(resp["resolvers"]) == 1
        assert resp["resolvers"][0]["fieldName"] == "hello"


class TestAppsyncAutoCoverage:
    """Auto-generated coverage tests for appsync."""

    @pytest.fixture
    def client(self):
        return make_client("appsync")

    def test_list_apis(self, client):
        """ListApis returns a response with apis key."""
        resp = client.list_apis()
        assert "apis" in resp
