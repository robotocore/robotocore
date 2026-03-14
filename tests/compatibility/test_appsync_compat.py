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


class TestAppsyncUpdateType:
    """Tests for UpdateType operation."""

    @pytest.fixture
    def client(self):
        return make_client("appsync")

    @pytest.fixture
    def api_with_type(self, client):
        created = client.create_graphql_api(
            name=_unique("updtype-api"), authenticationType="API_KEY"
        )
        api_id = created["graphqlApi"]["apiId"]
        schema = b"type Query { hello: String }"
        client.start_schema_creation(apiId=api_id, definition=base64.b64encode(schema))
        client.create_type(
            apiId=api_id,
            definition="type Item { id: ID name: String }",
            format="SDL",
        )
        yield api_id
        client.delete_graphql_api(apiId=api_id)

    def test_update_type_definition(self, client, api_with_type):
        """UpdateType changes the type definition."""
        resp = client.update_type(
            apiId=api_with_type,
            typeName="Item",
            format="SDL",
            definition="type Item { id: ID name: String age: Int }",
        )
        assert resp["type"]["name"] == "Item"
        assert "age" in resp["type"]["definition"]

    def test_update_type_get_reflects_change(self, client, api_with_type):
        """GetType after UpdateType returns updated definition."""
        client.update_type(
            apiId=api_with_type,
            typeName="Item",
            format="SDL",
            definition="type Item { id: ID title: String }",
        )
        resp = client.get_type(apiId=api_with_type, typeName="Item", format="SDL")
        assert "title" in resp["type"]["definition"]


class TestAppsyncDeleteType:
    """Tests for DeleteType operation."""

    @pytest.fixture
    def client(self):
        return make_client("appsync")

    @pytest.fixture
    def api_with_schema(self, client):
        created = client.create_graphql_api(
            name=_unique("deltype-api"), authenticationType="API_KEY"
        )
        api_id = created["graphqlApi"]["apiId"]
        schema = b"type Query { hello: String }"
        client.start_schema_creation(apiId=api_id, definition=base64.b64encode(schema))
        yield api_id
        client.delete_graphql_api(apiId=api_id)

    def test_delete_type(self, client, api_with_schema):
        """DeleteType removes a type."""
        client.create_type(
            apiId=api_with_schema,
            definition="type Ephemeral { x: Int }",
            format="SDL",
        )
        client.delete_type(apiId=api_with_schema, typeName="Ephemeral")
        with pytest.raises(client.exceptions.NotFoundException):
            client.get_type(apiId=api_with_schema, typeName="Ephemeral", format="SDL")

    def test_delete_type_removes_from_list(self, client, api_with_schema):
        """DeleteType removes the type from ListTypes."""
        client.create_type(
            apiId=api_with_schema,
            definition="type ToDelete { y: String }",
            format="SDL",
        )
        client.delete_type(apiId=api_with_schema, typeName="ToDelete")
        resp = client.list_types(apiId=api_with_schema, format="SDL")
        names = [t["name"] for t in resp["types"]]
        assert "ToDelete" not in names


class TestAppsyncUpdateResolver:
    """Tests for UpdateResolver operation."""

    @pytest.fixture
    def client(self):
        return make_client("appsync")

    @pytest.fixture
    def api_with_resolver(self, client):
        created = client.create_graphql_api(
            name=_unique("updres2-api"), authenticationType="API_KEY"
        )
        api_id = created["graphqlApi"]["apiId"]
        schema = b"type Query { hello: String }"
        client.start_schema_creation(apiId=api_id, definition=base64.b64encode(schema))
        client.create_data_source(apiId=api_id, name="noneds", type="NONE")
        client.create_data_source(apiId=api_id, name="noneds2", type="NONE")
        client.create_resolver(
            apiId=api_id,
            typeName="Query",
            fieldName="hello",
            dataSourceName="noneds",
        )
        yield api_id
        client.delete_graphql_api(apiId=api_id)

    def test_update_resolver_data_source(self, client, api_with_resolver):
        """UpdateResolver changes the data source."""
        resp = client.update_resolver(
            apiId=api_with_resolver,
            typeName="Query",
            fieldName="hello",
            dataSourceName="noneds2",
        )
        assert resp["resolver"]["fieldName"] == "hello"
        assert resp["resolver"]["dataSourceName"] == "noneds2"

    def test_update_resolver_reflected_in_get(self, client, api_with_resolver):
        """GetResolver returns updated values after UpdateResolver."""
        client.update_resolver(
            apiId=api_with_resolver,
            typeName="Query",
            fieldName="hello",
            dataSourceName="noneds2",
        )
        resp = client.get_resolver(apiId=api_with_resolver, typeName="Query", fieldName="hello")
        assert resp["resolver"]["dataSourceName"] == "noneds2"


class TestAppsyncFunctionCrud:
    """Tests for Function CRUD operations."""

    @pytest.fixture
    def client(self):
        return make_client("appsync")

    @pytest.fixture
    def api(self, client):
        created = client.create_graphql_api(name=_unique("func-api"), authenticationType="API_KEY")
        api_id = created["graphqlApi"]["apiId"]
        schema = b"type Query { hello: String }"
        client.start_schema_creation(apiId=api_id, definition=base64.b64encode(schema))
        client.create_data_source(apiId=api_id, name="noneds", type="NONE")
        yield api_id
        client.delete_graphql_api(apiId=api_id)

    def test_create_function(self, client, api):
        """CreateFunction returns the function configuration."""
        resp = client.create_function(
            apiId=api,
            name="myFunc",
            dataSourceName="noneds",
            functionVersion="2018-05-29",
        )
        assert resp["functionConfiguration"]["name"] == "myFunc"
        assert "functionId" in resp["functionConfiguration"]

    def test_get_function(self, client, api):
        """GetFunction retrieves a created function."""
        created = client.create_function(
            apiId=api,
            name="getFunc",
            dataSourceName="noneds",
            functionVersion="2018-05-29",
        )
        func_id = created["functionConfiguration"]["functionId"]
        resp = client.get_function(apiId=api, functionId=func_id)
        assert resp["functionConfiguration"]["name"] == "getFunc"
        assert resp["functionConfiguration"]["functionId"] == func_id

    def test_update_function(self, client, api):
        """UpdateFunction modifies function name."""
        created = client.create_function(
            apiId=api,
            name="origFunc",
            dataSourceName="noneds",
            functionVersion="2018-05-29",
        )
        func_id = created["functionConfiguration"]["functionId"]
        resp = client.update_function(
            apiId=api,
            functionId=func_id,
            name="renamedFunc",
            dataSourceName="noneds",
            functionVersion="2018-05-29",
        )
        assert resp["functionConfiguration"]["name"] == "renamedFunc"

    def test_list_functions(self, client, api):
        """ListFunctions returns created functions."""
        client.create_function(
            apiId=api,
            name="listFunc1",
            dataSourceName="noneds",
            functionVersion="2018-05-29",
        )
        client.create_function(
            apiId=api,
            name="listFunc2",
            dataSourceName="noneds",
            functionVersion="2018-05-29",
        )
        resp = client.list_functions(apiId=api)
        names = [f["name"] for f in resp["functions"]]
        assert "listFunc1" in names
        assert "listFunc2" in names

    def test_delete_function(self, client, api):
        """DeleteFunction removes the function."""
        created = client.create_function(
            apiId=api,
            name="delFunc",
            dataSourceName="noneds",
            functionVersion="2018-05-29",
        )
        func_id = created["functionConfiguration"]["functionId"]
        client.delete_function(apiId=api, functionId=func_id)
        resp = client.list_functions(apiId=api)
        ids = [f["functionId"] for f in resp["functions"]]
        assert func_id not in ids


class TestAppsyncDomainNameCrud:
    """Tests for DomainName CRUD operations."""

    @pytest.fixture
    def client(self):
        return make_client("appsync")

    def test_create_domain_name(self, client):
        """CreateDomainName returns domain name config."""
        domain = _unique("dom") + ".example.com"
        resp = client.create_domain_name(
            domainName=domain,
            certificateArn="arn:aws:acm:us-east-1:123456789012:certificate/fake",
        )
        try:
            assert resp["domainNameConfig"]["domainName"] == domain
            assert "appsyncDomainName" in resp["domainNameConfig"]
        finally:
            client.delete_domain_name(domainName=domain)

    def test_get_domain_name(self, client):
        """GetDomainName retrieves a created domain."""
        domain = _unique("dom") + ".example.com"
        client.create_domain_name(
            domainName=domain,
            certificateArn="arn:aws:acm:us-east-1:123456789012:certificate/fake",
        )
        try:
            resp = client.get_domain_name(domainName=domain)
            assert resp["domainNameConfig"]["domainName"] == domain
        finally:
            client.delete_domain_name(domainName=domain)

    def test_list_domain_names(self, client):
        """ListDomainNames includes created domains."""
        domain = _unique("dom") + ".example.com"
        client.create_domain_name(
            domainName=domain,
            certificateArn="arn:aws:acm:us-east-1:123456789012:certificate/fake",
        )
        try:
            resp = client.list_domain_names()
            names = [d["domainName"] for d in resp["domainNameConfigs"]]
            assert domain in names
        finally:
            client.delete_domain_name(domainName=domain)

    def test_delete_domain_name(self, client):
        """DeleteDomainName removes the domain."""
        domain = _unique("dom") + ".example.com"
        client.create_domain_name(
            domainName=domain,
            certificateArn="arn:aws:acm:us-east-1:123456789012:certificate/fake",
        )
        client.delete_domain_name(domainName=domain)
        with pytest.raises(client.exceptions.NotFoundException):
            client.get_domain_name(domainName=domain)


class TestAppsyncDomainNameUpdate:
    """Tests for UpdateDomainName operation."""

    @pytest.fixture
    def client(self):
        return make_client("appsync")

    def test_update_domain_name_not_found(self, client):
        """UpdateDomainName with a fake domain returns NotFoundException."""
        with pytest.raises(client.exceptions.NotFoundException):
            client.update_domain_name(
                domainName="nonexistent.example.com",
                description="should fail",
            )


class TestAppsyncApiAssociation:
    """Tests for AssociateApi, GetApiAssociation, DisassociateApi."""

    @pytest.fixture
    def client(self):
        return make_client("appsync")

    def test_associate_api_not_found(self, client):
        """AssociateApi with a fake domain returns NotFoundException."""
        with pytest.raises(client.exceptions.NotFoundException):
            client.associate_api(
                domainName="nonexistent.example.com",
                apiId="fake-api-id",
            )

    def test_get_api_association_not_found(self, client):
        """GetApiAssociation with a fake domain returns NotFoundException."""
        with pytest.raises(client.exceptions.NotFoundException):
            client.get_api_association(domainName="nonexistent.example.com")

    def test_disassociate_api_not_found(self, client):
        """DisassociateApi with a fake domain returns NotFoundException."""
        with pytest.raises(client.exceptions.NotFoundException):
            client.disassociate_api(domainName="nonexistent.example.com")


class TestAppsyncAutoCoverage:
    """Auto-generated coverage tests for appsync."""

    @pytest.fixture
    def client(self):
        return make_client("appsync")

    def test_list_apis(self, client):
        """ListApis returns a response with apis key."""
        resp = client.list_apis()
        assert "apis" in resp


class TestAppsyncMergedApiOps:
    """Tests for merged/source GraphQL API association operations."""

    @pytest.fixture
    def client(self):
        return make_client("appsync")

    @pytest.fixture
    def merged_api(self, client):
        """Create a MERGED-type GraphQL API."""
        created = client.create_graphql_api(
            name=_unique("merged-api"),
            authenticationType="API_KEY",
            apiType="MERGED",
            mergedApiExecutionRoleArn="arn:aws:iam::123456789012:role/fake-role",
        )
        api_id = created["graphqlApi"]["apiId"]
        yield created["graphqlApi"]
        client.delete_graphql_api(apiId=api_id)

    @pytest.fixture
    def source_api(self, client):
        """Create a regular GraphQL API to use as source."""
        created = client.create_graphql_api(
            name=_unique("source-api"),
            authenticationType="API_KEY",
        )
        api_id = created["graphqlApi"]["apiId"]
        yield created["graphqlApi"]
        client.delete_graphql_api(apiId=api_id)

    def test_associate_merged_graphql_api(self, client, source_api, merged_api):
        """AssociateMergedGraphqlApi creates an association."""
        resp = client.associate_merged_graphql_api(
            sourceApiIdentifier=source_api["apiId"],
            mergedApiIdentifier=merged_api["apiId"],
        )
        assert "sourceApiAssociation" in resp
        assert "associationId" in resp["sourceApiAssociation"]
        assert resp["sourceApiAssociation"]["sourceApiId"] == source_api["apiId"]

    def test_associate_source_graphql_api(self, client, source_api, merged_api):
        """AssociateSourceGraphqlApi creates an association from merged side."""
        resp = client.associate_source_graphql_api(
            mergedApiIdentifier=merged_api["apiId"],
            sourceApiIdentifier=source_api["apiId"],
        )
        assert "sourceApiAssociation" in resp
        assert "associationId" in resp["sourceApiAssociation"]
        assert resp["sourceApiAssociation"]["mergedApiId"] == merged_api["apiId"]

    def test_disassociate_merged_graphql_api(self, client, source_api, merged_api):
        """DisassociateMergedGraphqlApi removes an association."""
        assoc = client.associate_merged_graphql_api(
            sourceApiIdentifier=source_api["apiId"],
            mergedApiIdentifier=merged_api["apiId"],
        )
        assoc_id = assoc["sourceApiAssociation"]["associationId"]
        resp = client.disassociate_merged_graphql_api(
            sourceApiIdentifier=source_api["apiId"],
            associationId=assoc_id,
        )
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200

    def test_disassociate_source_graphql_api(self, client, source_api, merged_api):
        """DisassociateSourceGraphqlApi removes an association."""
        assoc = client.associate_source_graphql_api(
            mergedApiIdentifier=merged_api["apiId"],
            sourceApiIdentifier=source_api["apiId"],
        )
        assoc_id = assoc["sourceApiAssociation"]["associationId"]
        resp = client.disassociate_source_graphql_api(
            mergedApiIdentifier=merged_api["apiId"],
            associationId=assoc_id,
        )
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200

    def test_get_source_api_association(self, client, source_api, merged_api):
        """GetSourceApiAssociation retrieves an existing association."""
        assoc = client.associate_merged_graphql_api(
            sourceApiIdentifier=source_api["apiId"],
            mergedApiIdentifier=merged_api["apiId"],
        )
        assoc_id = assoc["sourceApiAssociation"]["associationId"]
        resp = client.get_source_api_association(
            mergedApiIdentifier=merged_api["apiId"],
            associationId=assoc_id,
        )
        assert "sourceApiAssociation" in resp
        assert resp["sourceApiAssociation"]["associationId"] == assoc_id

    def test_list_source_api_associations(self, client, merged_api):
        """ListSourceApiAssociations returns list for API."""
        resp = client.list_source_api_associations(apiId=merged_api["apiId"])
        assert "sourceApiAssociationSummaries" in resp
        assert isinstance(resp["sourceApiAssociationSummaries"], list)

    def test_list_types_by_association(self, client, source_api, merged_api):
        """ListTypesByAssociation returns types for an association."""
        assoc = client.associate_merged_graphql_api(
            sourceApiIdentifier=source_api["apiId"],
            mergedApiIdentifier=merged_api["apiId"],
        )
        assoc_id = assoc["sourceApiAssociation"]["associationId"]
        resp = client.list_types_by_association(
            mergedApiIdentifier=merged_api["apiId"],
            associationId=assoc_id,
            format="SDL",
        )
        assert "types" in resp
        assert isinstance(resp["types"], list)

    def test_start_schema_merge(self, client, source_api, merged_api):
        """StartSchemaMerge initiates a merge."""
        assoc = client.associate_merged_graphql_api(
            sourceApiIdentifier=source_api["apiId"],
            mergedApiIdentifier=merged_api["apiId"],
        )
        assoc_id = assoc["sourceApiAssociation"]["associationId"]
        resp = client.start_schema_merge(
            mergedApiIdentifier=merged_api["apiId"],
            associationId=assoc_id,
        )
        assert "sourceApiAssociationStatus" in resp
        assert resp["sourceApiAssociationStatus"] in (
            "MERGE_IN_PROGRESS",
            "MERGE_SUCCESS",
            "MERGE_SCHEDULED",
        )

    def test_update_source_api_association(self, client, source_api, merged_api):
        """UpdateSourceApiAssociation updates an existing association's description."""
        assoc = client.associate_merged_graphql_api(
            sourceApiIdentifier=source_api["apiId"],
            mergedApiIdentifier=merged_api["apiId"],
        )
        assoc_id = assoc["sourceApiAssociation"]["associationId"]
        resp = client.update_source_api_association(
            associationId=assoc_id,
            mergedApiIdentifier=merged_api["apiId"],
            description="updated-description",
        )
        assert "sourceApiAssociation" in resp
        assert resp["sourceApiAssociation"]["associationId"] == assoc_id


class TestAppsyncEvalAndIntrospection:
    """Tests for EvaluateCode, EvaluateMappingTemplate, and data source introspection."""

    @pytest.fixture
    def client(self):
        return make_client("appsync")

    @pytest.fixture
    def api(self, client):
        created = client.create_graphql_api(name=_unique("eval-api"), authenticationType="API_KEY")
        api_id = created["graphqlApi"]["apiId"]
        yield api_id
        client.delete_graphql_api(apiId=api_id)

    def test_evaluate_mapping_template(self, client):
        """EvaluateMappingTemplate evaluates a VTL template."""
        resp = client.evaluate_mapping_template(
            template='$util.toJson({"hello": "world"})',
            context='{"arguments": {}, "source": {}}',
        )
        assert "evaluationResult" in resp

    def test_evaluate_code(self, client):
        """EvaluateCode evaluates an AppSync JS runtime code."""
        resp = client.evaluate_code(
            runtime={"name": "APPSYNC_JS", "runtimeVersion": "1.0.0"},
            code=(
                "export function request(ctx) { return {}; }"
                " export function response(ctx) { return ctx.prev.result; }"
            ),
            context='{"arguments": {}, "source": {}}',
            function="request",
        )
        assert "evaluationResult" in resp or "error" in resp

    def test_start_data_source_introspection(self, client):
        """StartDataSourceIntrospection returns an introspection ID."""
        resp = client.start_data_source_introspection(
            rdsDataApiConfig={
                "resourceArn": "arn:aws:rds:us-east-1:123456789012:cluster:fake-cluster",
                "secretArn": "arn:aws:secretsmanager:us-east-1:123456789012:secret:fake",
                "databaseName": "testdb",
            }
        )
        assert "introspectionId" in resp
        assert isinstance(resp["introspectionId"], str)
        assert len(resp["introspectionId"]) > 0

    def test_get_data_source_introspection(self, client):
        """GetDataSourceIntrospection returns status for an introspection ID."""
        # Start an introspection first to get a valid ID
        start_resp = client.start_data_source_introspection(
            rdsDataApiConfig={
                "resourceArn": "arn:aws:rds:us-east-1:123456789012:cluster:fake",
                "secretArn": "arn:aws:secretsmanager:us-east-1:123456789012:secret:fake",
                "databaseName": "testdb",
            }
        )
        introspection_id = start_resp["introspectionId"]
        resp = client.get_data_source_introspection(
            introspectionId=introspection_id,
        )
        assert "introspectionStatus" in resp
        assert resp["introspectionStatus"] in ("PROCESSING", "SUCCESS", "FAILED")

    def test_get_graphql_api_environment_variables_not_found(self, client):
        """GetGraphqlApiEnvironmentVariables with fake API returns NotFoundException."""
        with pytest.raises(client.exceptions.NotFoundException):
            client.get_graphql_api_environment_variables(apiId="fake-api-id-12345")

    def test_put_graphql_api_environment_variables_not_found(self, client):
        """PutGraphqlApiEnvironmentVariables with fake API returns NotFoundException."""
        with pytest.raises(client.exceptions.NotFoundException):
            client.put_graphql_api_environment_variables(
                apiId="fake-api-id-12345",
                environmentVariables={"MY_VAR": "my_value"},
            )

    def test_list_resolvers_by_function_not_found(self, client):
        """ListResolversByFunction with fake API returns NotFoundException."""
        with pytest.raises(client.exceptions.NotFoundException):
            client.list_resolvers_by_function(apiId="fake-api-id", functionId="fake-func-id")

    def test_start_schema_creation_returns_status(self, client, api):
        """StartSchemaCreation returns a status field."""
        schema = b"type Query { greeting: String }"
        resp = client.start_schema_creation(
            apiId=api,
            definition=base64.b64encode(schema),
        )
        assert "status" in resp
        assert resp["status"] in ("ACTIVE", "PROCESSING", "SUCCESS")
