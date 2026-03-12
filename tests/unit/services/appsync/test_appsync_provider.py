"""Unit tests for the AppSync native provider.

Tests all inner functions directly (store-level CRUD) without HTTP routing.
"""

import pytest

from robotocore.services.appsync.provider import (
    AppSyncError,
    AppSyncStore,
    _create_api_cache,
    _create_api_key,
    _create_channel_namespace,
    _create_data_source,
    _create_domain_name,
    _create_event_api,
    _create_function,
    _create_graphql_api,
    _create_resolver,
    _create_type,
    _delete_api_cache,
    _delete_api_key,
    _delete_channel_namespace,
    _delete_data_source,
    _delete_domain_name,
    _delete_event_api,
    _delete_function,
    _delete_graphql_api,
    _delete_resolver,
    _delete_type,
    _error,
    _find_resource_by_arn,
    _flush_api_cache,
    _get_api_cache,
    _get_channel_namespace,
    _get_data_source,
    _get_domain_name,
    _get_event_api,
    _get_function,
    _get_graphql_api,
    _get_introspection_schema_response,
    _get_resolver,
    _get_schema_creation_status,
    _get_store,
    _get_type,
    _json_response,
    _list_api_keys,
    _list_channel_namespaces,
    _list_data_sources,
    _list_domain_names,
    _list_event_apis,
    _list_functions,
    _list_graphql_apis,
    _list_resolvers,
    _list_tags,
    _list_types,
    _require_api,
    _require_event_api,
    _start_schema_creation,
    _tag_resource,
    _untag_resource,
    _update_api_cache,
    _update_api_key,
    _update_channel_namespace,
    _update_data_source,
    _update_event_api,
    _update_function,
    _update_graphql_api,
    _update_resolver,
    _update_type,
)

REGION = "us-east-1"
ACCOUNT = "123456789012"


@pytest.fixture()
def store():
    """Fresh AppSyncStore for each test."""
    return AppSyncStore()


# ---------------------------------------------------------------------------
# Helper: create an API in the store and return its ID
# ---------------------------------------------------------------------------


def _make_api(store: AppSyncStore, name: str = "TestAPI") -> str:
    result = _create_graphql_api(store, {"name": name}, REGION, ACCOUNT)
    return result["graphqlApi"]["apiId"]


def _make_event_api(store: AppSyncStore, name: str = "TestEventAPI") -> str:
    result = _create_event_api(store, {"name": name}, REGION, ACCOUNT)
    return result["api"]["apiId"]


# ===================================================================
# GraphQL API CRUD
# ===================================================================


class TestGraphqlApiCrud:
    def test_create_api_returns_api_fields(self, store):
        result = _create_graphql_api(store, {"name": "MyAPI"}, REGION, ACCOUNT)
        api = result["graphqlApi"]
        assert api["name"] == "MyAPI"
        assert api["authenticationType"] == "API_KEY"
        assert "apiId" in api
        assert api["arn"].startswith("arn:aws:appsync:")
        assert "GRAPHQL" in api["uris"]
        assert "REALTIME" in api["uris"]

    def test_create_api_custom_auth_type(self, store):
        result = _create_graphql_api(
            store, {"name": "AuthAPI", "authenticationType": "AWS_IAM"}, REGION, ACCOUNT
        )
        assert result["graphqlApi"]["authenticationType"] == "AWS_IAM"

    def test_create_api_with_tags(self, store):
        result = _create_graphql_api(
            store, {"name": "TaggedAPI", "tags": {"env": "test"}}, REGION, ACCOUNT
        )
        assert result["graphqlApi"]["tags"] == {"env": "test"}

    def test_create_api_name_required(self, store):
        with pytest.raises(AppSyncError) as exc:
            _create_graphql_api(store, {}, REGION, ACCOUNT)
        assert exc.value.code == "BadRequestException"
        assert "name is required" in exc.value.message

    def test_create_api_empty_name_rejected(self, store):
        with pytest.raises(AppSyncError) as exc:
            _create_graphql_api(store, {"name": ""}, REGION, ACCOUNT)
        assert exc.value.code == "BadRequestException"

    def test_get_api(self, store):
        api_id = _make_api(store)
        result = _get_graphql_api(store, api_id, REGION, ACCOUNT)
        assert result["graphqlApi"]["apiId"] == api_id

    def test_get_api_not_found(self, store):
        with pytest.raises(AppSyncError) as exc:
            _get_graphql_api(store, "nonexistent", REGION, ACCOUNT)
        assert exc.value.status == 404
        assert exc.value.code == "NotFoundException"

    def test_list_apis_empty(self, store):
        result = _list_graphql_apis(store, REGION, ACCOUNT)
        assert result["graphqlApis"] == []

    def test_list_apis_multiple(self, store):
        _make_api(store, "A")
        _make_api(store, "B")
        result = _list_graphql_apis(store, REGION, ACCOUNT)
        names = {a["name"] for a in result["graphqlApis"]}
        assert names == {"A", "B"}

    def test_update_api_name(self, store):
        api_id = _make_api(store, "Old")
        result = _update_graphql_api(store, api_id, {"name": "New"}, REGION, ACCOUNT)
        assert result["graphqlApi"]["name"] == "New"

    def test_update_api_auth_type(self, store):
        api_id = _make_api(store)
        result = _update_graphql_api(
            store, api_id, {"authenticationType": "AMAZON_COGNITO_USER_POOLS"}, REGION, ACCOUNT
        )
        assert result["graphqlApi"]["authenticationType"] == "AMAZON_COGNITO_USER_POOLS"

    def test_update_api_xray(self, store):
        api_id = _make_api(store)
        result = _update_graphql_api(store, api_id, {"xrayEnabled": True}, REGION, ACCOUNT)
        assert result["graphqlApi"]["xrayEnabled"] is True

    def test_update_api_not_found(self, store):
        with pytest.raises(AppSyncError) as exc:
            _update_graphql_api(store, "missing", {"name": "X"}, REGION, ACCOUNT)
        assert exc.value.status == 404

    def test_delete_api(self, store):
        api_id = _make_api(store)
        result = _delete_graphql_api(store, api_id, REGION, ACCOUNT)
        assert result == {}
        # Verify it's gone
        with pytest.raises(AppSyncError):
            _get_graphql_api(store, api_id, REGION, ACCOUNT)

    def test_delete_api_cleans_up_children(self, store):
        api_id = _make_api(store)
        _create_api_key(store, api_id, {}, REGION, ACCOUNT)
        _create_data_source(store, api_id, {"name": "ds1"}, REGION, ACCOUNT)
        _start_schema_creation(store, api_id, {"definition": "type Query { id: ID }"})
        _delete_graphql_api(store, api_id, REGION, ACCOUNT)
        # All child collections should be cleaned up
        assert api_id not in store.api_keys
        assert api_id not in store.data_sources
        assert api_id not in store.schemas

    def test_delete_api_not_found(self, store):
        with pytest.raises(AppSyncError) as exc:
            _delete_graphql_api(store, "missing", REGION, ACCOUNT)
        assert exc.value.status == 404


# ===================================================================
# API Keys
# ===================================================================


class TestApiKeys:
    def test_create_key(self, store):
        api_id = _make_api(store)
        result = _create_api_key(store, api_id, {}, REGION, ACCOUNT)
        key = result["apiKey"]
        assert key["id"].startswith("da2-")
        assert "expires" in key
        assert "deletes" in key

    def test_create_key_with_description(self, store):
        api_id = _make_api(store)
        result = _create_api_key(store, api_id, {"description": "test key"}, REGION, ACCOUNT)
        assert result["apiKey"]["description"] == "test key"

    def test_create_key_custom_expires(self, store):
        api_id = _make_api(store)
        result = _create_api_key(store, api_id, {"expires": 9999999999}, REGION, ACCOUNT)
        assert result["apiKey"]["expires"] == 9999999999

    def test_create_key_api_not_found(self, store):
        with pytest.raises(AppSyncError) as exc:
            _create_api_key(store, "missing", {}, REGION, ACCOUNT)
        assert exc.value.status == 404

    def test_list_keys_empty(self, store):
        api_id = _make_api(store)
        result = _list_api_keys(store, api_id)
        assert result["apiKeys"] == []

    def test_list_keys(self, store):
        api_id = _make_api(store)
        _create_api_key(store, api_id, {"description": "k1"}, REGION, ACCOUNT)
        _create_api_key(store, api_id, {"description": "k2"}, REGION, ACCOUNT)
        result = _list_api_keys(store, api_id)
        assert len(result["apiKeys"]) == 2

    def test_delete_key(self, store):
        api_id = _make_api(store)
        key_id = _create_api_key(store, api_id, {}, REGION, ACCOUNT)["apiKey"]["id"]
        result = _delete_api_key(store, api_id, key_id)
        assert result == {}
        assert len(_list_api_keys(store, api_id)["apiKeys"]) == 0

    def test_delete_key_not_found(self, store):
        api_id = _make_api(store)
        with pytest.raises(AppSyncError) as exc:
            _delete_api_key(store, api_id, "bad-key")
        assert exc.value.status == 404

    def test_update_key_description(self, store):
        api_id = _make_api(store)
        key_id = _create_api_key(store, api_id, {"description": "old"}, REGION, ACCOUNT)["apiKey"][
            "id"
        ]
        result = _update_api_key(store, api_id, key_id, {"description": "new"}, REGION, ACCOUNT)
        assert result["apiKey"]["description"] == "new"

    def test_update_key_expires(self, store):
        api_id = _make_api(store)
        key_id = _create_api_key(store, api_id, {}, REGION, ACCOUNT)["apiKey"]["id"]
        result = _update_api_key(store, api_id, key_id, {"expires": 1234567890}, REGION, ACCOUNT)
        assert result["apiKey"]["expires"] == 1234567890

    def test_update_key_not_found(self, store):
        api_id = _make_api(store)
        with pytest.raises(AppSyncError) as exc:
            _update_api_key(store, api_id, "bad-key", {"description": "x"}, REGION, ACCOUNT)
        assert exc.value.status == 404


# ===================================================================
# Schema
# ===================================================================


class TestSchema:
    def test_start_schema_creation(self, store):
        api_id = _make_api(store)
        result = _start_schema_creation(store, api_id, {"definition": "type Query { id: ID }"})
        assert result["status"] == "SUCCESS"

    def test_get_schema_status(self, store):
        api_id = _make_api(store)
        _start_schema_creation(store, api_id, {"definition": "type Query { id: ID }"})
        result = _get_schema_creation_status(store, api_id)
        assert result["status"] == "SUCCESS"

    def test_get_schema_not_found(self, store):
        api_id = _make_api(store)
        with pytest.raises(AppSyncError) as exc:
            _get_schema_creation_status(store, api_id)
        assert exc.value.status == 404

    def test_schema_api_not_found(self, store):
        with pytest.raises(AppSyncError):
            _start_schema_creation(store, "missing", {"definition": "x"})


# ===================================================================
# Resolvers
# ===================================================================


class TestResolvers:
    def test_create_resolver(self, store):
        api_id = _make_api(store)
        result = _create_resolver(store, api_id, "Query", {"fieldName": "getItem"}, REGION, ACCOUNT)
        resolver = result["resolver"]
        assert resolver["typeName"] == "Query"
        assert resolver["fieldName"] == "getItem"
        assert resolver["kind"] == "UNIT"
        assert "resolverArn" in resolver

    def test_create_resolver_field_required(self, store):
        api_id = _make_api(store)
        with pytest.raises(AppSyncError) as exc:
            _create_resolver(store, api_id, "Query", {}, REGION, ACCOUNT)
        assert exc.value.code == "BadRequestException"

    def test_create_resolver_with_templates(self, store):
        api_id = _make_api(store)
        result = _create_resolver(
            store,
            api_id,
            "Query",
            {
                "fieldName": "f",
                "requestMappingTemplate": "req",
                "responseMappingTemplate": "resp",
                "kind": "PIPELINE",
            },
            REGION,
            ACCOUNT,
        )
        r = result["resolver"]
        assert r["requestMappingTemplate"] == "req"
        assert r["responseMappingTemplate"] == "resp"
        assert r["kind"] == "PIPELINE"

    def test_get_resolver(self, store):
        api_id = _make_api(store)
        _create_resolver(store, api_id, "Query", {"fieldName": "f1"}, REGION, ACCOUNT)
        result = _get_resolver(store, api_id, "Query", "f1")
        assert result["resolver"]["fieldName"] == "f1"

    def test_get_resolver_not_found(self, store):
        api_id = _make_api(store)
        with pytest.raises(AppSyncError) as exc:
            _get_resolver(store, api_id, "Query", "missing")
        assert exc.value.status == 404

    def test_list_resolvers(self, store):
        api_id = _make_api(store)
        _create_resolver(store, api_id, "Query", {"fieldName": "a"}, REGION, ACCOUNT)
        _create_resolver(store, api_id, "Query", {"fieldName": "b"}, REGION, ACCOUNT)
        _create_resolver(store, api_id, "Mutation", {"fieldName": "c"}, REGION, ACCOUNT)
        result = _list_resolvers(store, api_id, "Query")
        assert len(result["resolvers"]) == 2

    def test_list_resolvers_filters_by_type(self, store):
        api_id = _make_api(store)
        _create_resolver(store, api_id, "Query", {"fieldName": "a"}, REGION, ACCOUNT)
        _create_resolver(store, api_id, "Mutation", {"fieldName": "b"}, REGION, ACCOUNT)
        result = _list_resolvers(store, api_id, "Mutation")
        assert len(result["resolvers"]) == 1
        assert result["resolvers"][0]["fieldName"] == "b"

    def test_update_resolver(self, store):
        api_id = _make_api(store)
        _create_resolver(store, api_id, "Query", {"fieldName": "f1"}, REGION, ACCOUNT)
        result = _update_resolver(
            store, api_id, "Query", "f1", {"dataSourceName": "myDS"}, REGION, ACCOUNT
        )
        assert result["resolver"]["dataSourceName"] == "myDS"

    def test_update_resolver_not_found(self, store):
        api_id = _make_api(store)
        with pytest.raises(AppSyncError) as exc:
            _update_resolver(store, api_id, "Query", "missing", {}, REGION, ACCOUNT)
        assert exc.value.status == 404

    def test_delete_resolver(self, store):
        api_id = _make_api(store)
        _create_resolver(store, api_id, "Query", {"fieldName": "f1"}, REGION, ACCOUNT)
        result = _delete_resolver(store, api_id, "Query", "f1")
        assert result == {}
        with pytest.raises(AppSyncError):
            _get_resolver(store, api_id, "Query", "f1")

    def test_delete_resolver_not_found(self, store):
        api_id = _make_api(store)
        with pytest.raises(AppSyncError) as exc:
            _delete_resolver(store, api_id, "Query", "missing")
        assert exc.value.status == 404


# ===================================================================
# Data Sources
# ===================================================================


class TestDataSources:
    def test_create_data_source(self, store):
        api_id = _make_api(store)
        result = _create_data_source(
            store, api_id, {"name": "myDS", "type": "NONE"}, REGION, ACCOUNT
        )
        ds = result["dataSource"]
        assert ds["name"] == "myDS"
        assert ds["type"] == "NONE"
        assert "dataSourceArn" in ds

    def test_create_data_source_name_required(self, store):
        api_id = _make_api(store)
        with pytest.raises(AppSyncError) as exc:
            _create_data_source(store, api_id, {}, REGION, ACCOUNT)
        assert exc.value.code == "BadRequestException"

    def test_create_data_source_duplicate(self, store):
        api_id = _make_api(store)
        _create_data_source(store, api_id, {"name": "dup"}, REGION, ACCOUNT)
        with pytest.raises(AppSyncError) as exc:
            _create_data_source(store, api_id, {"name": "dup"}, REGION, ACCOUNT)
        assert "already exists" in exc.value.message

    def test_create_data_source_with_configs(self, store):
        api_id = _make_api(store)
        result = _create_data_source(
            store,
            api_id,
            {
                "name": "dynamo",
                "type": "AMAZON_DYNAMODB",
                "dynamodbConfig": {"tableName": "t", "awsRegion": "us-east-1"},
                "serviceRoleArn": "arn:aws:iam::123456789012:role/r",
            },
            REGION,
            ACCOUNT,
        )
        ds = result["dataSource"]
        assert ds["dynamodbConfig"]["tableName"] == "t"
        assert ds["serviceRoleArn"] == "arn:aws:iam::123456789012:role/r"

    def test_get_data_source(self, store):
        api_id = _make_api(store)
        _create_data_source(store, api_id, {"name": "ds1"}, REGION, ACCOUNT)
        result = _get_data_source(store, api_id, "ds1")
        assert result["dataSource"]["name"] == "ds1"

    def test_get_data_source_not_found(self, store):
        api_id = _make_api(store)
        with pytest.raises(AppSyncError) as exc:
            _get_data_source(store, api_id, "missing")
        assert exc.value.status == 404

    def test_list_data_sources_empty(self, store):
        api_id = _make_api(store)
        result = _list_data_sources(store, api_id)
        assert result["dataSources"] == []

    def test_list_data_sources(self, store):
        api_id = _make_api(store)
        _create_data_source(store, api_id, {"name": "a"}, REGION, ACCOUNT)
        _create_data_source(store, api_id, {"name": "b"}, REGION, ACCOUNT)
        result = _list_data_sources(store, api_id)
        assert len(result["dataSources"]) == 2

    def test_update_data_source(self, store):
        api_id = _make_api(store)
        _create_data_source(store, api_id, {"name": "ds1", "type": "NONE"}, REGION, ACCOUNT)
        result = _update_data_source(
            store, api_id, "ds1", {"type": "HTTP", "description": "updated"}, REGION, ACCOUNT
        )
        ds = result["dataSource"]
        assert ds["type"] == "HTTP"
        assert ds["description"] == "updated"

    def test_update_data_source_not_found(self, store):
        api_id = _make_api(store)
        with pytest.raises(AppSyncError) as exc:
            _update_data_source(store, api_id, "missing", {}, REGION, ACCOUNT)
        assert exc.value.status == 404

    def test_delete_data_source(self, store):
        api_id = _make_api(store)
        _create_data_source(store, api_id, {"name": "ds1"}, REGION, ACCOUNT)
        result = _delete_data_source(store, api_id, "ds1")
        assert result == {}
        with pytest.raises(AppSyncError):
            _get_data_source(store, api_id, "ds1")

    def test_delete_data_source_not_found(self, store):
        api_id = _make_api(store)
        with pytest.raises(AppSyncError) as exc:
            _delete_data_source(store, api_id, "missing")
        assert exc.value.status == 404


# ===================================================================
# Types
# ===================================================================


class TestTypes:
    def test_create_type(self, store):
        api_id = _make_api(store)
        result = _create_type(
            store, api_id, {"definition": "type Query { id: ID }", "format": "SDL"}, REGION, ACCOUNT
        )
        t = result["type"]
        assert t["name"] == "Query"
        assert t["format"] == "SDL"
        assert "arn" in t

    def test_create_type_extracts_name_from_definition(self, store):
        api_id = _make_api(store)
        result = _create_type(
            store,
            api_id,
            {"definition": "input CreatePostInput { title: String }"},
            REGION,
            ACCOUNT,
        )
        assert result["type"]["name"] == "CreatePostInput"

    def test_create_type_enum(self, store):
        api_id = _make_api(store)
        result = _create_type(
            store, api_id, {"definition": "enum Status { ACTIVE INACTIVE }"}, REGION, ACCOUNT
        )
        assert result["type"]["name"] == "Status"

    def test_get_type(self, store):
        api_id = _make_api(store)
        _create_type(store, api_id, {"definition": "type Foo { x: Int }"}, REGION, ACCOUNT)
        result = _get_type(store, api_id, "Foo")
        assert result["type"]["name"] == "Foo"

    def test_get_type_not_found(self, store):
        api_id = _make_api(store)
        with pytest.raises(AppSyncError) as exc:
            _get_type(store, api_id, "Missing")
        assert exc.value.status == 404

    def test_list_types_empty(self, store):
        api_id = _make_api(store)
        result = _list_types(store, api_id)
        assert result["types"] == []

    def test_list_types(self, store):
        api_id = _make_api(store)
        _create_type(store, api_id, {"definition": "type A { x: Int }"}, REGION, ACCOUNT)
        _create_type(store, api_id, {"definition": "type B { y: Int }"}, REGION, ACCOUNT)
        result = _list_types(store, api_id)
        assert len(result["types"]) == 2

    def test_update_type(self, store):
        api_id = _make_api(store)
        _create_type(
            store,
            api_id,
            {"definition": "type T { x: Int }", "format": "SDL"},
            REGION,
            ACCOUNT,
        )
        result = _update_type(
            store,
            api_id,
            "T",
            {"definition": "type T { x: String }", "format": "JSON"},
            REGION,
            ACCOUNT,
        )
        assert result["type"]["definition"] == "type T { x: String }"
        assert result["type"]["format"] == "JSON"

    def test_update_type_not_found(self, store):
        api_id = _make_api(store)
        with pytest.raises(AppSyncError) as exc:
            _update_type(store, api_id, "Missing", {}, REGION, ACCOUNT)
        assert exc.value.status == 404

    def test_delete_type(self, store):
        api_id = _make_api(store)
        _create_type(store, api_id, {"definition": "type X { a: Int }"}, REGION, ACCOUNT)
        result = _delete_type(store, api_id, "X")
        assert result == {}
        with pytest.raises(AppSyncError):
            _get_type(store, api_id, "X")

    def test_delete_type_not_found(self, store):
        api_id = _make_api(store)
        with pytest.raises(AppSyncError) as exc:
            _delete_type(store, api_id, "Missing")
        assert exc.value.status == 404


# ===================================================================
# Functions
# ===================================================================


class TestFunctions:
    def test_create_function(self, store):
        api_id = _make_api(store)
        result = _create_function(
            store, api_id, {"name": "myFunc", "dataSourceName": "ds1"}, REGION, ACCOUNT
        )
        func = result["functionConfiguration"]
        assert func["name"] == "myFunc"
        assert func["dataSourceName"] == "ds1"
        assert "functionId" in func
        assert "functionArn" in func
        assert func["functionVersion"] == "2018-05-29"

    def test_create_function_name_required(self, store):
        api_id = _make_api(store)
        with pytest.raises(AppSyncError) as exc:
            _create_function(store, api_id, {}, REGION, ACCOUNT)
        assert exc.value.code == "BadRequestException"

    def test_create_function_with_code(self, store):
        api_id = _make_api(store)
        result = _create_function(
            store,
            api_id,
            {"name": "f", "code": "export function handler(ctx) {}"},
            REGION,
            ACCOUNT,
        )
        assert result["functionConfiguration"]["code"] == "export function handler(ctx) {}"

    def test_get_function(self, store):
        api_id = _make_api(store)
        func_id = _create_function(store, api_id, {"name": "f1"}, REGION, ACCOUNT)[
            "functionConfiguration"
        ]["functionId"]
        result = _get_function(store, api_id, func_id)
        assert result["functionConfiguration"]["name"] == "f1"

    def test_get_function_not_found(self, store):
        api_id = _make_api(store)
        with pytest.raises(AppSyncError) as exc:
            _get_function(store, api_id, "missing")
        assert exc.value.status == 404

    def test_list_functions_empty(self, store):
        api_id = _make_api(store)
        result = _list_functions(store, api_id)
        assert result["functions"] == []

    def test_list_functions(self, store):
        api_id = _make_api(store)
        _create_function(store, api_id, {"name": "a"}, REGION, ACCOUNT)
        _create_function(store, api_id, {"name": "b"}, REGION, ACCOUNT)
        result = _list_functions(store, api_id)
        assert len(result["functions"]) == 2

    def test_update_function(self, store):
        api_id = _make_api(store)
        func_id = _create_function(store, api_id, {"name": "f1"}, REGION, ACCOUNT)[
            "functionConfiguration"
        ]["functionId"]
        result = _update_function(
            store, api_id, func_id, {"name": "f1-updated", "description": "desc"}, REGION, ACCOUNT
        )
        func = result["functionConfiguration"]
        assert func["name"] == "f1-updated"
        assert func["description"] == "desc"

    def test_update_function_not_found(self, store):
        api_id = _make_api(store)
        with pytest.raises(AppSyncError) as exc:
            _update_function(store, api_id, "missing", {"name": "x"}, REGION, ACCOUNT)
        assert exc.value.status == 404

    def test_delete_function(self, store):
        api_id = _make_api(store)
        func_id = _create_function(store, api_id, {"name": "f1"}, REGION, ACCOUNT)[
            "functionConfiguration"
        ]["functionId"]
        result = _delete_function(store, api_id, func_id)
        assert result == {}
        with pytest.raises(AppSyncError):
            _get_function(store, api_id, func_id)

    def test_delete_function_not_found(self, store):
        api_id = _make_api(store)
        with pytest.raises(AppSyncError) as exc:
            _delete_function(store, api_id, "missing")
        assert exc.value.status == 404


# ===================================================================
# Domain Names
# ===================================================================


class TestDomainNames:
    def test_create_domain_name(self, store):
        result = _create_domain_name(
            store,
            {
                "domainName": "api.example.com",
                "certificateArn": "arn:aws:acm:us-east-1:123:cert/abc",
            },
            REGION,
            ACCOUNT,
        )
        domain = result["domainNameConfig"]
        assert domain["domainName"] == "api.example.com"
        assert domain["certificateArn"] == "arn:aws:acm:us-east-1:123:cert/abc"
        assert "appsyncDomainName" in domain
        assert domain["hostedZoneId"] == "Z2FDTNDATAQYW2"

    def test_create_domain_name_required(self, store):
        with pytest.raises(AppSyncError) as exc:
            _create_domain_name(store, {}, REGION, ACCOUNT)
        assert exc.value.code == "BadRequestException"

    def test_create_domain_name_duplicate(self, store):
        _create_domain_name(store, {"domainName": "dup.example.com"}, REGION, ACCOUNT)
        with pytest.raises(AppSyncError) as exc:
            _create_domain_name(store, {"domainName": "dup.example.com"}, REGION, ACCOUNT)
        assert "already exists" in exc.value.message

    def test_get_domain_name(self, store):
        _create_domain_name(store, {"domainName": "test.example.com"}, REGION, ACCOUNT)
        result = _get_domain_name(store, "test.example.com")
        assert result["domainNameConfig"]["domainName"] == "test.example.com"

    def test_get_domain_name_not_found(self, store):
        with pytest.raises(AppSyncError) as exc:
            _get_domain_name(store, "missing.example.com")
        assert exc.value.status == 404

    def test_list_domain_names_empty(self, store):
        result = _list_domain_names(store)
        assert result["domainNameConfigs"] == []

    def test_list_domain_names(self, store):
        _create_domain_name(store, {"domainName": "a.example.com"}, REGION, ACCOUNT)
        _create_domain_name(store, {"domainName": "b.example.com"}, REGION, ACCOUNT)
        result = _list_domain_names(store)
        assert len(result["domainNameConfigs"]) == 2

    def test_delete_domain_name(self, store):
        _create_domain_name(store, {"domainName": "del.example.com"}, REGION, ACCOUNT)
        result = _delete_domain_name(store, "del.example.com")
        assert result == {}
        with pytest.raises(AppSyncError):
            _get_domain_name(store, "del.example.com")

    def test_delete_domain_name_not_found(self, store):
        with pytest.raises(AppSyncError) as exc:
            _delete_domain_name(store, "missing.example.com")
        assert exc.value.status == 404


# ===================================================================
# Event APIs (v2)
# ===================================================================


class TestEventApis:
    def test_create_event_api(self, store):
        result = _create_event_api(store, {"name": "EventAPI"}, REGION, ACCOUNT)
        api = result["api"]
        assert api["name"] == "EventAPI"
        assert "apiId" in api
        assert "apiArn" in api
        assert "HTTP" in api["dns"]
        assert "REALTIME" in api["dns"]

    def test_create_event_api_name_required(self, store):
        with pytest.raises(AppSyncError) as exc:
            _create_event_api(store, {}, REGION, ACCOUNT)
        assert exc.value.code == "BadRequestException"

    def test_create_event_api_with_config(self, store):
        config = {"authProviders": [{"authType": "API_KEY"}]}
        result = _create_event_api(
            store, {"name": "E", "eventConfig": config, "ownerContact": "me"}, REGION, ACCOUNT
        )
        api = result["api"]
        assert api["eventConfig"] == config
        assert api["ownerContact"] == "me"

    def test_get_event_api(self, store):
        api_id = _make_event_api(store)
        result = _get_event_api(store, api_id, REGION, ACCOUNT)
        assert result["api"]["apiId"] == api_id

    def test_get_event_api_not_found(self, store):
        with pytest.raises(AppSyncError) as exc:
            _get_event_api(store, "missing", REGION, ACCOUNT)
        assert exc.value.status == 404

    def test_list_event_apis_empty(self, store):
        result = _list_event_apis(store)
        assert result["apis"] == []

    def test_list_event_apis(self, store):
        _make_event_api(store, "A")
        _make_event_api(store, "B")
        result = _list_event_apis(store)
        assert len(result["apis"]) == 2

    def test_update_event_api(self, store):
        api_id = _make_event_api(store)
        result = _update_event_api(store, api_id, {"name": "Updated"}, REGION, ACCOUNT)
        assert result["api"]["name"] == "Updated"

    def test_update_event_api_not_found(self, store):
        with pytest.raises(AppSyncError) as exc:
            _update_event_api(store, "missing", {"name": "X"}, REGION, ACCOUNT)
        assert exc.value.status == 404

    def test_delete_event_api(self, store):
        api_id = _make_event_api(store)
        result = _delete_event_api(store, api_id)
        assert result == {}
        with pytest.raises(AppSyncError):
            _get_event_api(store, api_id, REGION, ACCOUNT)

    def test_delete_event_api_cleans_up_namespaces(self, store):
        api_id = _make_event_api(store)
        _create_channel_namespace(store, api_id, {"name": "ns1"}, REGION, ACCOUNT)
        _delete_event_api(store, api_id)
        assert api_id not in store.channel_namespaces

    def test_delete_event_api_not_found(self, store):
        with pytest.raises(AppSyncError) as exc:
            _delete_event_api(store, "missing")
        assert exc.value.status == 404


# ===================================================================
# Channel Namespaces
# ===================================================================


class TestChannelNamespaces:
    def test_create_channel_namespace(self, store):
        api_id = _make_event_api(store)
        result = _create_channel_namespace(store, api_id, {"name": "chat"}, REGION, ACCOUNT)
        ns = result["channelNamespace"]
        assert ns["name"] == "chat"
        assert ns["apiId"] == api_id
        assert "channelNamespaceArn" in ns

    def test_create_channel_namespace_name_required(self, store):
        api_id = _make_event_api(store)
        with pytest.raises(AppSyncError) as exc:
            _create_channel_namespace(store, api_id, {}, REGION, ACCOUNT)
        assert exc.value.code == "BadRequestException"

    def test_create_channel_namespace_with_auth(self, store):
        api_id = _make_event_api(store)
        result = _create_channel_namespace(
            store,
            api_id,
            {
                "name": "ns",
                "subscribeAuthModes": [{"authType": "API_KEY"}],
                "publishAuthModes": [{"authType": "AWS_IAM"}],
            },
            REGION,
            ACCOUNT,
        )
        ns = result["channelNamespace"]
        assert len(ns["subscribeAuthModes"]) == 1
        assert len(ns["publishAuthModes"]) == 1

    def test_create_channel_namespace_api_not_found(self, store):
        with pytest.raises(AppSyncError) as exc:
            _create_channel_namespace(store, "missing", {"name": "ns"}, REGION, ACCOUNT)
        assert exc.value.status == 404

    def test_get_channel_namespace(self, store):
        api_id = _make_event_api(store)
        _create_channel_namespace(store, api_id, {"name": "ns1"}, REGION, ACCOUNT)
        result = _get_channel_namespace(store, api_id, "ns1", REGION, ACCOUNT)
        assert result["channelNamespace"]["name"] == "ns1"

    def test_get_channel_namespace_not_found(self, store):
        api_id = _make_event_api(store)
        with pytest.raises(AppSyncError) as exc:
            _get_channel_namespace(store, api_id, "missing", REGION, ACCOUNT)
        assert exc.value.status == 404

    def test_list_channel_namespaces_empty(self, store):
        api_id = _make_event_api(store)
        result = _list_channel_namespaces(store, api_id)
        assert result["channelNamespaces"] == []

    def test_list_channel_namespaces(self, store):
        api_id = _make_event_api(store)
        _create_channel_namespace(store, api_id, {"name": "a"}, REGION, ACCOUNT)
        _create_channel_namespace(store, api_id, {"name": "b"}, REGION, ACCOUNT)
        result = _list_channel_namespaces(store, api_id)
        assert len(result["channelNamespaces"]) == 2

    def test_update_channel_namespace(self, store):
        api_id = _make_event_api(store)
        _create_channel_namespace(store, api_id, {"name": "ns1"}, REGION, ACCOUNT)
        result = _update_channel_namespace(
            store, api_id, "ns1", {"codeHandlers": "new-code"}, REGION, ACCOUNT
        )
        assert result["channelNamespace"]["codeHandlers"] == "new-code"
        assert "lastModified" in result["channelNamespace"]

    def test_update_channel_namespace_not_found(self, store):
        api_id = _make_event_api(store)
        with pytest.raises(AppSyncError) as exc:
            _update_channel_namespace(store, api_id, "missing", {}, REGION, ACCOUNT)
        assert exc.value.status == 404

    def test_delete_channel_namespace(self, store):
        api_id = _make_event_api(store)
        _create_channel_namespace(store, api_id, {"name": "ns1"}, REGION, ACCOUNT)
        result = _delete_channel_namespace(store, api_id, "ns1")
        assert result == {}
        with pytest.raises(AppSyncError):
            _get_channel_namespace(store, api_id, "ns1", REGION, ACCOUNT)

    def test_delete_channel_namespace_not_found(self, store):
        api_id = _make_event_api(store)
        with pytest.raises(AppSyncError) as exc:
            _delete_channel_namespace(store, api_id, "missing")
        assert exc.value.status == 404


# ===================================================================
# API Cache
# ===================================================================


class TestApiCache:
    def test_create_api_cache(self, store):
        api_id = _make_api(store)
        result = _create_api_cache(store, api_id, {"ttl": 600, "type": "T2_MEDIUM"})
        cache = result["apiCache"]
        assert cache["ttl"] == 600
        assert cache["type"] == "T2_MEDIUM"
        assert cache["status"] == "AVAILABLE"

    def test_create_api_cache_defaults(self, store):
        api_id = _make_api(store)
        result = _create_api_cache(store, api_id, {})
        cache = result["apiCache"]
        assert cache["ttl"] == 3600
        assert cache["type"] == "T2_SMALL"
        assert cache["apiCachingBehavior"] == "FULL_REQUEST_CACHING"

    def test_get_api_cache(self, store):
        api_id = _make_api(store)
        _create_api_cache(store, api_id, {"ttl": 300})
        result = _get_api_cache(store, api_id)
        assert result["apiCache"]["ttl"] == 300

    def test_get_api_cache_not_found(self, store):
        api_id = _make_api(store)
        with pytest.raises(AppSyncError) as exc:
            _get_api_cache(store, api_id)
        assert exc.value.status == 404

    def test_update_api_cache(self, store):
        api_id = _make_api(store)
        _create_api_cache(store, api_id, {"ttl": 300})
        result = _update_api_cache(store, api_id, {"ttl": 900, "type": "R4_LARGE"})
        assert result["apiCache"]["ttl"] == 900
        assert result["apiCache"]["type"] == "R4_LARGE"

    def test_update_api_cache_not_found(self, store):
        api_id = _make_api(store)
        with pytest.raises(AppSyncError) as exc:
            _update_api_cache(store, api_id, {"ttl": 100})
        assert exc.value.status == 404

    def test_delete_api_cache(self, store):
        api_id = _make_api(store)
        _create_api_cache(store, api_id, {})
        _delete_api_cache(store, api_id)
        with pytest.raises(AppSyncError):
            _get_api_cache(store, api_id)

    def test_delete_api_cache_not_found(self, store):
        api_id = _make_api(store)
        with pytest.raises(AppSyncError) as exc:
            _delete_api_cache(store, api_id)
        assert exc.value.status == 404

    def test_flush_api_cache(self, store):
        api_id = _make_api(store)
        _create_api_cache(store, api_id, {})
        # Should not raise
        _flush_api_cache(store, api_id)

    def test_flush_api_cache_not_found(self, store):
        api_id = _make_api(store)
        with pytest.raises(AppSyncError) as exc:
            _flush_api_cache(store, api_id)
        assert exc.value.status == 404


# ===================================================================
# Tags
# ===================================================================


class TestTags:
    def test_list_tags_on_api(self, store):
        result = _create_graphql_api(
            store, {"name": "Tagged", "tags": {"env": "dev"}}, REGION, ACCOUNT
        )
        arn = result["graphqlApi"]["arn"]
        tags = _list_tags(store, arn)
        assert tags == {"env": "dev"}

    def test_tag_resource(self, store):
        result = _create_graphql_api(store, {"name": "T"}, REGION, ACCOUNT)
        arn = result["graphqlApi"]["arn"]
        _tag_resource(store, arn, {"key1": "val1", "key2": "val2"})
        tags = _list_tags(store, arn)
        assert tags["key1"] == "val1"
        assert tags["key2"] == "val2"

    def test_untag_resource(self, store):
        result = _create_graphql_api(
            store, {"name": "T", "tags": {"a": "1", "b": "2", "c": "3"}}, REGION, ACCOUNT
        )
        arn = result["graphqlApi"]["arn"]
        _untag_resource(store, arn, ["a", "c"])
        tags = _list_tags(store, arn)
        assert tags == {"b": "2"}

    def test_list_tags_not_found(self, store):
        with pytest.raises(AppSyncError) as exc:
            _list_tags(store, "arn:aws:appsync:us-east-1:123:apis/missing")
        assert exc.value.status == 404

    def test_tag_resource_not_found(self, store):
        with pytest.raises(AppSyncError):
            _tag_resource(store, "arn:aws:appsync:us-east-1:123:apis/missing", {"k": "v"})

    def test_untag_resource_not_found(self, store):
        with pytest.raises(AppSyncError):
            _untag_resource(store, "arn:aws:appsync:us-east-1:123:apis/missing", ["k"])

    def test_tags_on_event_api(self, store):
        result = _create_event_api(store, {"name": "E", "tags": {"env": "prod"}}, REGION, ACCOUNT)
        arn = result["api"]["apiArn"]
        tags = _list_tags(store, arn)
        assert tags == {"env": "prod"}

    def test_tags_on_channel_namespace(self, store):
        api_id = _make_event_api(store)
        result = _create_channel_namespace(
            store, api_id, {"name": "ns", "tags": {"team": "backend"}}, REGION, ACCOUNT
        )
        arn = result["channelNamespace"]["channelNamespaceArn"]
        tags = _list_tags(store, arn)
        assert tags == {"team": "backend"}


# ===================================================================
# Introspection Schema
# ===================================================================


class TestIntrospectionSchema:
    def test_get_introspection_schema_sdl(self, store):
        api_id = _make_api(store)
        _start_schema_creation(store, api_id, {"definition": "type Query { id: ID }"})
        resp = _get_introspection_schema_response(store, api_id, "SDL", True)
        assert resp.status_code == 200
        assert resp.body.decode() == "type Query { id: ID }"

    def test_get_introspection_schema_json(self, store):
        import json

        api_id = _make_api(store)
        _start_schema_creation(store, api_id, {"definition": "type Query { id: ID }"})
        resp = _get_introspection_schema_response(store, api_id, "JSON", True)
        assert resp.status_code == 200
        body = json.loads(resp.body.decode())
        assert "__schema" in body

    def test_get_introspection_schema_no_schema(self, store):
        api_id = _make_api(store)
        with pytest.raises(AppSyncError) as exc:
            _get_introspection_schema_response(store, api_id, "SDL", True)
        assert exc.value.status == 404

    def test_get_introspection_schema_api_not_found(self, store):
        with pytest.raises(AppSyncError):
            _get_introspection_schema_response(store, "missing", "SDL", True)


# ===================================================================
# Helpers and utilities
# ===================================================================


class TestHelpers:
    def test_get_store_returns_same_instance(self):
        s1 = _get_store("us-west-2", "111111111111")
        s2 = _get_store("us-west-2", "111111111111")
        assert s1 is s2

    def test_get_store_different_regions(self):
        s1 = _get_store("us-east-1", "111111111111")
        s2 = _get_store("eu-west-1", "111111111111")
        assert s1 is not s2

    def test_require_api_raises_on_missing(self, store):
        with pytest.raises(AppSyncError) as exc:
            _require_api(store, "missing")
        assert exc.value.status == 404

    def test_require_event_api_raises_on_missing(self, store):
        with pytest.raises(AppSyncError) as exc:
            _require_event_api(store, "missing")
        assert exc.value.status == 404

    def test_find_resource_by_arn_graphql_api(self, store):
        result = _create_graphql_api(store, {"name": "A"}, REGION, ACCOUNT)
        arn = result["graphqlApi"]["arn"]
        found = _find_resource_by_arn(store, arn)
        assert found is not None
        assert found["name"] == "A"

    def test_find_resource_by_arn_event_api(self, store):
        result = _create_event_api(store, {"name": "E"}, REGION, ACCOUNT)
        arn = result["api"]["apiArn"]
        found = _find_resource_by_arn(store, arn)
        assert found is not None
        assert found["name"] == "E"

    def test_find_resource_by_arn_channel_namespace(self, store):
        api_id = _make_event_api(store)
        result = _create_channel_namespace(store, api_id, {"name": "ns"}, REGION, ACCOUNT)
        arn = result["channelNamespace"]["channelNamespaceArn"]
        found = _find_resource_by_arn(store, arn)
        assert found is not None
        assert found["name"] == "ns"

    def test_find_resource_by_arn_not_found(self, store):
        assert _find_resource_by_arn(store, "arn:aws:appsync:us-east-1:123:apis/missing") is None

    def test_json_response(self):
        resp = _json_response({"key": "value"})
        assert resp.status_code == 200
        assert resp.media_type == "application/json"
        import json

        body = json.loads(resp.body.decode())
        assert body == {"key": "value"}

    def test_json_response_custom_status(self):
        resp = _json_response({"ok": True}, status=201)
        assert resp.status_code == 201

    def test_error_response(self):
        import json

        resp = _error("BadRequestException", "bad input", 400)
        assert resp.status_code == 400
        body = json.loads(resp.body.decode())
        assert body["__type"] == "BadRequestException"
        assert body["message"] == "bad input"

    def test_appsync_error_defaults(self):
        err = AppSyncError("TestError", "msg")
        assert err.code == "TestError"
        assert err.message == "msg"
        assert err.status == 400

    def test_appsync_error_custom_status(self):
        err = AppSyncError("NotFound", "gone", 404)
        assert err.status == 404
