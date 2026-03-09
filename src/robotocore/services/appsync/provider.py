"""Native AppSync GraphQL provider.

REST-JSON protocol with URL path routing (/v1/apis, /v1/apis/{apiId}, etc.).
"""

import json
import re
import threading
import time
import uuid
from urllib.parse import unquote

from starlette.requests import Request
from starlette.responses import Response

# ---------------------------------------------------------------------------
# In-memory stores (region-scoped)
# ---------------------------------------------------------------------------

_stores: dict[str, "AppSyncStore"] = {}
_lock = threading.RLock()


class AppSyncStore:
    """Per-region in-memory store for AppSync resources."""

    def __init__(self) -> None:
        self.apis: dict[str, dict] = {}  # api_id -> api
        self.api_keys: dict[str, dict[str, dict]] = {}  # api_id -> key_id -> key
        self.schemas: dict[str, dict] = {}  # api_id -> schema info
        self.resolvers: dict[str, dict[str, dict]] = {}  # api_id -> "type.field" -> resolver
        self.data_sources: dict[str, dict[str, dict]] = {}  # api_id -> name -> ds
        self.types: dict[str, dict[str, dict]] = {}  # api_id -> name -> type
        self.functions: dict[str, dict[str, dict]] = {}  # api_id -> func_id -> function
        self.domain_names: dict[str, dict] = {}  # domain_name -> domain config
        self.event_apis: dict[str, dict] = {}  # api_id -> event api
        self.channel_namespaces: dict[str, dict[str, dict]] = {}  # api_id -> name -> ns
        self.lock = threading.RLock()


def _get_store(region: str = "us-east-1") -> AppSyncStore:
    with _lock:
        if region not in _stores:
            _stores[region] = AppSyncStore()
        return _stores[region]


def _new_id() -> str:
    return str(uuid.uuid4())


# ---------------------------------------------------------------------------
# Error handling
# ---------------------------------------------------------------------------


class AppSyncError(Exception):
    def __init__(self, code: str, message: str, status: int = 400):
        self.code = code
        self.message = message
        self.status = status


# ---------------------------------------------------------------------------
# Path patterns
# ---------------------------------------------------------------------------

_APIS_LIST = re.compile(r"^/v1/apis/?$")
_API_ITEM = re.compile(r"^/v1/apis/([^/]+)/?$")
_API_KEYS_LIST = re.compile(r"^/v1/apis/([^/]+)/apikeys/?$")
_API_KEY_ITEM = re.compile(r"^/v1/apis/([^/]+)/apikeys/([^/]+)/?$")
_SCHEMA = re.compile(r"^/v1/apis/([^/]+)/schemacreation/?$")
_RESOLVERS_LIST = re.compile(r"^/v1/apis/([^/]+)/types/([^/]+)/resolvers/?$")
_RESOLVER_ITEM = re.compile(r"^/v1/apis/([^/]+)/types/([^/]+)/resolvers/([^/]+)/?$")
_DATA_SOURCES_LIST = re.compile(r"^/v1/apis/([^/]+)/datasources/?$")
_DATA_SOURCE_ITEM = re.compile(r"^/v1/apis/([^/]+)/datasources/([^/]+)/?$")
_TYPES_LIST = re.compile(r"^/v1/apis/([^/]+)/types/?$")
_TYPE_ITEM = re.compile(r"^/v1/apis/([^/]+)/types/([^/]+)/?$")
_TAGS = re.compile(r"^/v1/tags/(.+)$")
_API_CACHE = re.compile(r"^/v1/apis/([^/]+)/ApiCaches/?$")
_API_CACHE_UPDATE = re.compile(r"^/v1/apis/([^/]+)/ApiCaches/update/?$")
_FLUSH_CACHE = re.compile(r"^/v1/apis/([^/]+)/FlushCache/?$")
_INTROSPECTION_SCHEMA = re.compile(r"^/v1/apis/([^/]+)/schema/?$")
_FUNCTIONS_LIST = re.compile(r"^/v1/apis/([^/]+)/functions/?$")
_FUNCTION_ITEM = re.compile(r"^/v1/apis/([^/]+)/functions/([^/]+)/?$")
_DOMAIN_NAMES_LIST = re.compile(r"^/v1/domainnames/?$")
_DOMAIN_NAME_ITEM = re.compile(r"^/v1/domainnames/([^/]+)/?$")

# v2 Event API paths
_V2_APIS_LIST = re.compile(r"^/v2/apis/?$")
_V2_API_ITEM = re.compile(r"^/v2/apis/([^/]+)/?$")
_V2_CHANNEL_NS_LIST = re.compile(r"^/v2/apis/([^/]+)/channelNamespaces/?$")
_V2_CHANNEL_NS_ITEM = re.compile(r"^/v2/apis/([^/]+)/channelNamespaces/([^/]+)/?$")


# ---------------------------------------------------------------------------
# Main handler
# ---------------------------------------------------------------------------


async def handle_appsync_request(request: Request, region: str, account_id: str) -> Response:
    """Handle an AppSync API request."""
    path = request.url.path
    method = request.method.upper()
    body = await request.body()
    params = json.loads(body) if body else {}
    store = _get_store(region)

    try:
        # GraphQL APIs
        m = _API_ITEM.match(path)
        if m:
            api_id = m.group(1)
            if method == "GET":
                return _json_response(_get_graphql_api(store, api_id, region, account_id))
            elif method == "POST":
                return _json_response(
                    _update_graphql_api(store, api_id, params, region, account_id)
                )
            elif method == "DELETE":
                return _json_response(_delete_graphql_api(store, api_id, region, account_id))

        if _APIS_LIST.match(path):
            if method == "POST":
                return _json_response(_create_graphql_api(store, params, region, account_id))
            elif method == "GET":
                return _json_response(_list_graphql_apis(store, region, account_id))

        # API Keys
        m = _API_KEY_ITEM.match(path)
        if m:
            api_id, key_id = m.group(1), m.group(2)
            if method == "DELETE":
                return _json_response(_delete_api_key(store, api_id, key_id))
            elif method == "POST":
                return _json_response(
                    _update_api_key(store, api_id, key_id, params, region, account_id)
                )

        m = _API_KEYS_LIST.match(path)
        if m:
            api_id = m.group(1)
            if method == "POST":
                return _json_response(_create_api_key(store, api_id, params, region, account_id))
            elif method == "GET":
                return _json_response(_list_api_keys(store, api_id))

        # Schema
        m = _SCHEMA.match(path)
        if m:
            api_id = m.group(1)
            if method == "POST":
                return _json_response(_start_schema_creation(store, api_id, params))
            elif method == "GET":
                return _json_response(_get_schema_creation_status(store, api_id))

        # Resolvers
        m = _RESOLVER_ITEM.match(path)
        if m:
            api_id, type_name, field_name = m.group(1), m.group(2), m.group(3)
            if method == "GET":
                return _json_response(_get_resolver(store, api_id, type_name, field_name))
            elif method == "POST":
                return _json_response(
                    _update_resolver(
                        store, api_id, type_name, field_name, params, region, account_id
                    )
                )
            elif method == "DELETE":
                return _json_response(_delete_resolver(store, api_id, type_name, field_name))

        m = _RESOLVERS_LIST.match(path)
        if m:
            api_id, type_name = m.group(1), m.group(2)
            if method == "POST":
                return _json_response(
                    _create_resolver(store, api_id, type_name, params, region, account_id)
                )
            elif method == "GET":
                return _json_response(_list_resolvers(store, api_id, type_name))

        # Data Sources
        m = _DATA_SOURCE_ITEM.match(path)
        if m:
            api_id, ds_name = m.group(1), m.group(2)
            if method == "GET":
                return _json_response(_get_data_source(store, api_id, ds_name))
            elif method == "DELETE":
                return _json_response(_delete_data_source(store, api_id, ds_name))
            elif method in ("POST", "PUT"):
                return _json_response(
                    _update_data_source(store, api_id, ds_name, params, region, account_id)
                )

        m = _DATA_SOURCES_LIST.match(path)
        if m:
            api_id = m.group(1)
            if method == "POST":
                return _json_response(
                    _create_data_source(store, api_id, params, region, account_id)
                )
            elif method == "GET":
                return _json_response(_list_data_sources(store, api_id))

        # Types
        m = _TYPE_ITEM.match(path)
        if m:
            api_id, type_name = m.group(1), m.group(2)
            if method == "GET":
                return _json_response(_get_type(store, api_id, type_name))
            elif method == "POST":
                return _json_response(
                    _update_type(store, api_id, type_name, params, region, account_id)
                )
            elif method == "DELETE":
                return _json_response(_delete_type(store, api_id, type_name))

        m = _TYPES_LIST.match(path)
        if m:
            api_id = m.group(1)
            if method == "POST":
                return _json_response(_create_type(store, api_id, params, region, account_id))
            elif method == "GET":
                return _json_response(_list_types(store, api_id))

        # Functions
        m = _FUNCTION_ITEM.match(path)
        if m:
            api_id, func_id = m.group(1), m.group(2)
            if method == "GET":
                return _json_response(_get_function(store, api_id, func_id))
            elif method == "POST":
                return _json_response(
                    _update_function(store, api_id, func_id, params, region, account_id)
                )
            elif method == "DELETE":
                return _json_response(_delete_function(store, api_id, func_id))

        m = _FUNCTIONS_LIST.match(path)
        if m:
            api_id = m.group(1)
            if method == "POST":
                return _json_response(_create_function(store, api_id, params, region, account_id))
            elif method == "GET":
                return _json_response(_list_functions(store, api_id))

        # Domain Names
        m = _DOMAIN_NAME_ITEM.match(path)
        if m:
            domain_name = m.group(1)
            if method == "GET":
                return _json_response(_get_domain_name(store, domain_name))
            elif method == "DELETE":
                return _json_response(_delete_domain_name(store, domain_name))

        if _DOMAIN_NAMES_LIST.match(path):
            if method == "POST":
                return _json_response(_create_domain_name(store, params, region, account_id))
            elif method == "GET":
                return _json_response(_list_domain_names(store))

        # Tags
        m = _TAGS.match(path)
        if m:
            resource_arn = unquote(m.group(1))
            if method == "GET":
                return _json_response({"tags": _list_tags(store, resource_arn)})
            elif method == "POST":
                new_tags = params.get("tags", {})
                _tag_resource(store, resource_arn, new_tags)
                return _json_response({})
            elif method == "DELETE":
                tag_keys = request.query_params.getlist("tagKeys")
                _untag_resource(store, resource_arn, tag_keys)
                return _json_response({})

        # v2 Event APIs
        m = _V2_CHANNEL_NS_ITEM.match(path)
        if m:
            api_id, ns_name = m.group(1), m.group(2)
            if method == "GET":
                return _json_response(
                    _get_channel_namespace(store, api_id, ns_name, region, account_id)
                )
            elif method in ("POST", "PUT", "PATCH"):
                return _json_response(
                    _update_channel_namespace(store, api_id, ns_name, params, region, account_id)
                )
            elif method == "DELETE":
                return _json_response(_delete_channel_namespace(store, api_id, ns_name))

        m = _V2_CHANNEL_NS_LIST.match(path)
        if m:
            api_id = m.group(1)
            if method == "POST":
                return _json_response(
                    _create_channel_namespace(store, api_id, params, region, account_id)
                )
            elif method == "GET":
                return _json_response(_list_channel_namespaces(store, api_id))

        m = _V2_API_ITEM.match(path)
        if m:
            api_id = m.group(1)
            if method == "GET":
                return _json_response(_get_event_api(store, api_id, region, account_id))
            elif method in ("POST", "PUT", "PATCH"):
                return _json_response(_update_event_api(store, api_id, params, region, account_id))
            elif method == "DELETE":
                return _json_response(_delete_event_api(store, api_id))

        if _V2_APIS_LIST.match(path):
            if method == "POST":
                return _json_response(
                    _create_event_api(store, params, region, account_id), status=201
                )
            elif method == "GET":
                return _json_response(_list_event_apis(store))

        # API Cache
        m = _API_CACHE_UPDATE.match(path)
        if m:
            api_id = m.group(1)
            if method == "POST":
                return _json_response(_update_api_cache(store, api_id, params))

        m = _FLUSH_CACHE.match(path)
        if m:
            api_id = m.group(1)
            if method == "DELETE":
                _flush_api_cache(store, api_id)
                return _json_response({})

        m = _API_CACHE.match(path)
        if m:
            api_id = m.group(1)
            if method == "POST":
                return _json_response(_create_api_cache(store, api_id, params))
            elif method == "GET":
                return _json_response(_get_api_cache(store, api_id))
            elif method == "DELETE":
                _delete_api_cache(store, api_id)
                return _json_response({})

        # Introspection Schema
        m = _INTROSPECTION_SCHEMA.match(path)
        if m:
            api_id = m.group(1)
            if method == "GET":
                fmt = request.query_params.get("format", "SDL")
                include_directives = request.query_params.get("includeDirectives", "true")
                return _get_introspection_schema_response(
                    store, api_id, fmt, include_directives == "true"
                )

        # Fall through to Moto for ops not handled natively
        from robotocore.providers.moto_bridge import forward_to_moto

        return await forward_to_moto(request, "appsync")

    except AppSyncError as e:
        return _error(e.code, e.message, e.status)
    except Exception as e:
        return _error("InternalError", str(e), 500)


# ---------------------------------------------------------------------------
# GraphQL API CRUD
# ---------------------------------------------------------------------------


def _create_graphql_api(store: AppSyncStore, params: dict, region: str, account_id: str) -> dict:
    api_id = _new_id()[:26]
    name = params.get("name", "")
    if not name:
        raise AppSyncError("BadRequestException", "name is required.")

    auth_type = params.get("authenticationType", "API_KEY")
    api = {
        "apiId": api_id,
        "name": name,
        "authenticationType": auth_type,
        "arn": f"arn:aws:appsync:{region}:{account_id}:apis/{api_id}",
        "uris": {
            "GRAPHQL": f"https://{api_id}.appsync-api.{region}.amazonaws.com/graphql",
            "REALTIME": f"wss://{api_id}.appsync-realtime-api.{region}.amazonaws.com/graphql",
        },
        "tags": params.get("tags", {}),
        "xrayEnabled": params.get("xrayEnabled", False),
        "logConfig": params.get("logConfig"),
        "additionalAuthenticationProviders": params.get("additionalAuthenticationProviders", []),
    }

    with store.lock:
        store.apis[api_id] = api
        store.api_keys[api_id] = {}
        store.resolvers[api_id] = {}
        store.data_sources[api_id] = {}
        store.types[api_id] = {}
        store.functions[api_id] = {}

    return {"graphqlApi": api}


def _get_graphql_api(store: AppSyncStore, api_id: str, region: str, account_id: str) -> dict:
    with store.lock:
        api = store.apis.get(api_id)
    if not api:
        raise AppSyncError("NotFoundException", f"GraphQL API {api_id} not found.", 404)
    return {"graphqlApi": api}


def _list_graphql_apis(store: AppSyncStore, region: str, account_id: str) -> dict:
    with store.lock:
        apis = list(store.apis.values())
    return {"graphqlApis": apis}


def _update_graphql_api(
    store: AppSyncStore, api_id: str, params: dict, region: str, account_id: str
) -> dict:
    with store.lock:
        api = store.apis.get(api_id)
        if not api:
            raise AppSyncError("NotFoundException", f"GraphQL API {api_id} not found.", 404)
        if "name" in params:
            api["name"] = params["name"]
        if "authenticationType" in params:
            api["authenticationType"] = params["authenticationType"]
        if "xrayEnabled" in params:
            api["xrayEnabled"] = params["xrayEnabled"]
        if "logConfig" in params:
            api["logConfig"] = params["logConfig"]
    return {"graphqlApi": api}


def _delete_graphql_api(store: AppSyncStore, api_id: str, region: str, account_id: str) -> dict:
    with store.lock:
        if api_id not in store.apis:
            raise AppSyncError("NotFoundException", f"GraphQL API {api_id} not found.", 404)
        del store.apis[api_id]
        store.api_keys.pop(api_id, None)
        store.resolvers.pop(api_id, None)
        store.data_sources.pop(api_id, None)
        store.types.pop(api_id, None)
        store.schemas.pop(api_id, None)
    return {}


# ---------------------------------------------------------------------------
# API Keys
# ---------------------------------------------------------------------------


def _create_api_key(
    store: AppSyncStore, api_id: str, params: dict, region: str, account_id: str
) -> dict:
    _require_api(store, api_id)
    key_id = "da2-" + _new_id().replace("-", "")[:26]
    now = int(time.time())
    expires = params.get("expires", now + 7 * 86400)

    key = {
        "id": key_id,
        "description": params.get("description", ""),
        "expires": expires,
        "deletes": expires + 60 * 86400,
    }

    with store.lock:
        store.api_keys[api_id][key_id] = key
    return {"apiKey": key}


def _list_api_keys(store: AppSyncStore, api_id: str) -> dict:
    _require_api(store, api_id)
    with store.lock:
        keys = list(store.api_keys.get(api_id, {}).values())
    return {"apiKeys": keys}


def _delete_api_key(store: AppSyncStore, api_id: str, key_id: str) -> dict:
    _require_api(store, api_id)
    with store.lock:
        keys = store.api_keys.get(api_id, {})
        if key_id not in keys:
            raise AppSyncError("NotFoundException", f"API key {key_id} not found.", 404)
        del keys[key_id]
    return {}


def _update_api_key(
    store: AppSyncStore, api_id: str, key_id: str, params: dict, region: str, account_id: str
) -> dict:
    _require_api(store, api_id)
    with store.lock:
        keys = store.api_keys.get(api_id, {})
        key = keys.get(key_id)
        if not key:
            raise AppSyncError("NotFoundException", f"API key {key_id} not found.", 404)
        if "description" in params:
            key["description"] = params["description"]
        if "expires" in params:
            key["expires"] = params["expires"]
    return {"apiKey": key}


# ---------------------------------------------------------------------------
# Schema
# ---------------------------------------------------------------------------


def _start_schema_creation(store: AppSyncStore, api_id: str, params: dict) -> dict:
    _require_api(store, api_id)
    definition = params.get("definition", "")

    with store.lock:
        store.schemas[api_id] = {
            "status": "SUCCESS",
            "definition": definition,
        }
    return {"status": "SUCCESS"}


def _get_schema_creation_status(store: AppSyncStore, api_id: str) -> dict:
    _require_api(store, api_id)
    with store.lock:
        schema = store.schemas.get(api_id)
    if not schema:
        raise AppSyncError("NotFoundException", f"Schema for API {api_id} not found.", 404)
    return {"status": schema["status"]}


# ---------------------------------------------------------------------------
# Resolvers
# ---------------------------------------------------------------------------


def _create_resolver(
    store: AppSyncStore,
    api_id: str,
    type_name: str,
    params: dict,
    region: str,
    account_id: str,
) -> dict:
    _require_api(store, api_id)
    field_name = params.get("fieldName", "")
    if not field_name:
        raise AppSyncError("BadRequestException", "fieldName is required.")

    key = f"{type_name}.{field_name}"
    resolver = {
        "typeName": type_name,
        "fieldName": field_name,
        "dataSourceName": params.get("dataSourceName", ""),
        "resolverArn": (
            f"arn:aws:appsync:{region}:{account_id}"
            f":apis/{api_id}/types/{type_name}/resolvers/{field_name}"
        ),
        "requestMappingTemplate": params.get("requestMappingTemplate", ""),
        "responseMappingTemplate": params.get("responseMappingTemplate", ""),
        "kind": params.get("kind", "UNIT"),
    }

    with store.lock:
        store.resolvers[api_id][key] = resolver
    return {"resolver": resolver}


def _get_resolver(store: AppSyncStore, api_id: str, type_name: str, field_name: str) -> dict:
    _require_api(store, api_id)
    key = f"{type_name}.{field_name}"
    with store.lock:
        resolver = store.resolvers.get(api_id, {}).get(key)
    if not resolver:
        raise AppSyncError(
            "NotFoundException",
            f"Resolver {type_name}.{field_name} not found.",
            404,
        )
    return {"resolver": resolver}


def _list_resolvers(store: AppSyncStore, api_id: str, type_name: str) -> dict:
    _require_api(store, api_id)
    with store.lock:
        resolvers = [
            r for r in store.resolvers.get(api_id, {}).values() if r["typeName"] == type_name
        ]
    return {"resolvers": resolvers}


def _delete_resolver(store: AppSyncStore, api_id: str, type_name: str, field_name: str) -> dict:
    _require_api(store, api_id)
    key = f"{type_name}.{field_name}"
    with store.lock:
        resolvers = store.resolvers.get(api_id, {})
        if key not in resolvers:
            raise AppSyncError(
                "NotFoundException",
                f"Resolver {type_name}.{field_name} not found.",
                404,
            )
        del resolvers[key]
    return {}


# ---------------------------------------------------------------------------
# Data Sources
# ---------------------------------------------------------------------------


def _create_data_source(
    store: AppSyncStore, api_id: str, params: dict, region: str, account_id: str
) -> dict:
    _require_api(store, api_id)
    name = params.get("name", "")
    if not name:
        raise AppSyncError("BadRequestException", "name is required.")

    ds = {
        "dataSourceArn": (
            f"arn:aws:appsync:{region}:{account_id}:apis/{api_id}/datasources/{name}"
        ),
        "name": name,
        "type": params.get("type", "NONE"),
        "description": params.get("description", ""),
        "serviceRoleArn": params.get("serviceRoleArn", ""),
        "dynamodbConfig": params.get("dynamodbConfig"),
        "lambdaConfig": params.get("lambdaConfig"),
        "httpConfig": params.get("httpConfig"),
    }

    with store.lock:
        if name in store.data_sources.get(api_id, {}):
            raise AppSyncError(
                "BadRequestException",
                f"Data source {name} already exists.",
            )
        store.data_sources[api_id][name] = ds
    return {"dataSource": ds}


def _get_data_source(store: AppSyncStore, api_id: str, name: str) -> dict:
    _require_api(store, api_id)
    with store.lock:
        ds = store.data_sources.get(api_id, {}).get(name)
    if not ds:
        raise AppSyncError("NotFoundException", f"Data source {name} not found.", 404)
    return {"dataSource": ds}


def _list_data_sources(store: AppSyncStore, api_id: str) -> dict:
    _require_api(store, api_id)
    with store.lock:
        sources = list(store.data_sources.get(api_id, {}).values())
    return {"dataSources": sources}


def _delete_data_source(store: AppSyncStore, api_id: str, name: str) -> dict:
    _require_api(store, api_id)
    with store.lock:
        sources = store.data_sources.get(api_id, {})
        if name not in sources:
            raise AppSyncError("NotFoundException", f"Data source {name} not found.", 404)
        del sources[name]
    return {}


def _update_data_source(
    store: AppSyncStore, api_id: str, name: str, params: dict, region: str, account_id: str
) -> dict:
    _require_api(store, api_id)
    with store.lock:
        sources = store.data_sources.get(api_id, {})
        ds = sources.get(name)
        if not ds:
            raise AppSyncError("NotFoundException", f"Data source {name} not found.", 404)
        if "type" in params:
            ds["type"] = params["type"]
        if "description" in params:
            ds["description"] = params["description"]
        if "serviceRoleArn" in params:
            ds["serviceRoleArn"] = params["serviceRoleArn"]
        if "dynamodbConfig" in params:
            ds["dynamodbConfig"] = params["dynamodbConfig"]
        if "lambdaConfig" in params:
            ds["lambdaConfig"] = params["lambdaConfig"]
    return {"dataSource": ds}


# ---------------------------------------------------------------------------
# Types
# ---------------------------------------------------------------------------


def _create_type(
    store: AppSyncStore, api_id: str, params: dict, region: str, account_id: str
) -> dict:
    _require_api(store, api_id)
    definition = params.get("definition", "")
    fmt = params.get("format", "SDL")

    # Extract type name from definition (simple heuristic)
    type_name = "UnknownType"
    for token in definition.split():
        if token not in ("type", "input", "enum", "interface", "union", "scalar"):
            type_name = token.rstrip("{").strip()
            break

    t = {
        "name": type_name,
        "description": params.get("description", ""),
        "arn": (f"arn:aws:appsync:{region}:{account_id}:apis/{api_id}/types/{type_name}"),
        "definition": definition,
        "format": fmt,
    }

    with store.lock:
        store.types[api_id][type_name] = t
    return {"type": t}


def _get_type(store: AppSyncStore, api_id: str, type_name: str) -> dict:
    _require_api(store, api_id)
    with store.lock:
        t = store.types.get(api_id, {}).get(type_name)
    if not t:
        raise AppSyncError("NotFoundException", f"Type {type_name} not found.", 404)
    return {"type": t}


def _list_types(store: AppSyncStore, api_id: str) -> dict:
    _require_api(store, api_id)
    with store.lock:
        types = list(store.types.get(api_id, {}).values())
    return {"types": types}


def _update_type(
    store: AppSyncStore, api_id: str, type_name: str, params: dict, region: str, account_id: str
) -> dict:
    _require_api(store, api_id)
    with store.lock:
        t = store.types.get(api_id, {}).get(type_name)
        if not t:
            raise AppSyncError("NotFoundException", f"Type {type_name} not found.", 404)
        if "definition" in params:
            t["definition"] = params["definition"]
        if "format" in params:
            t["format"] = params["format"]
    return {"type": t}


def _delete_type(store: AppSyncStore, api_id: str, type_name: str) -> dict:
    _require_api(store, api_id)
    with store.lock:
        types = store.types.get(api_id, {})
        if type_name not in types:
            raise AppSyncError("NotFoundException", f"Type {type_name} not found.", 404)
        del types[type_name]
    return {}


# ---------------------------------------------------------------------------
# Functions
# ---------------------------------------------------------------------------


def _create_function(
    store: AppSyncStore, api_id: str, params: dict, region: str, account_id: str
) -> dict:
    _require_api(store, api_id)
    name = params.get("name", "")
    if not name:
        raise AppSyncError("BadRequestException", "name is required.")

    func_id = _new_id()[:26]
    func = {
        "functionId": func_id,
        "name": name,
        "dataSourceName": params.get("dataSourceName", ""),
        "functionArn": (f"arn:aws:appsync:{region}:{account_id}:apis/{api_id}/functions/{func_id}"),
        "requestMappingTemplate": params.get("requestMappingTemplate", ""),
        "responseMappingTemplate": params.get("responseMappingTemplate", ""),
        "functionVersion": params.get("functionVersion", "2018-05-29"),
        "description": params.get("description", ""),
        "maxBatchSize": params.get("maxBatchSize", 0),
        "code": params.get("code", ""),
        "runtime": params.get("runtime"),
    }

    with store.lock:
        store.functions.setdefault(api_id, {})[func_id] = func
    return {"functionConfiguration": func}


def _get_function(store: AppSyncStore, api_id: str, func_id: str) -> dict:
    _require_api(store, api_id)
    with store.lock:
        func = store.functions.get(api_id, {}).get(func_id)
    if not func:
        raise AppSyncError("NotFoundException", f"Function {func_id} not found.", 404)
    return {"functionConfiguration": func}


def _list_functions(store: AppSyncStore, api_id: str) -> dict:
    _require_api(store, api_id)
    with store.lock:
        functions = list(store.functions.get(api_id, {}).values())
    return {"functions": functions}


def _update_function(
    store: AppSyncStore, api_id: str, func_id: str, params: dict, region: str, account_id: str
) -> dict:
    _require_api(store, api_id)
    with store.lock:
        func = store.functions.get(api_id, {}).get(func_id)
        if not func:
            raise AppSyncError("NotFoundException", f"Function {func_id} not found.", 404)
        for key in (
            "name",
            "dataSourceName",
            "requestMappingTemplate",
            "responseMappingTemplate",
            "functionVersion",
            "description",
            "maxBatchSize",
            "code",
            "runtime",
        ):
            if key in params:
                func[key] = params[key]
    return {"functionConfiguration": func}


def _delete_function(store: AppSyncStore, api_id: str, func_id: str) -> dict:
    _require_api(store, api_id)
    with store.lock:
        functions = store.functions.get(api_id, {})
        if func_id not in functions:
            raise AppSyncError("NotFoundException", f"Function {func_id} not found.", 404)
        del functions[func_id]
    return {}


# ---------------------------------------------------------------------------
# Domain Names
# ---------------------------------------------------------------------------


def _create_domain_name(store: AppSyncStore, params: dict, region: str, account_id: str) -> dict:
    domain_name = params.get("domainName", "")
    if not domain_name:
        raise AppSyncError("BadRequestException", "domainName is required.")
    certificate_arn = params.get("certificateArn", "")

    domain = {
        "domainName": domain_name,
        "certificateArn": certificate_arn,
        "description": params.get("description", ""),
        "appsyncDomainName": f"{_new_id()[:8]}.cloudfront.net",
        "hostedZoneId": "Z2FDTNDATAQYW2",
    }

    with store.lock:
        if domain_name in store.domain_names:
            raise AppSyncError("BadRequestException", f"Domain name {domain_name} already exists.")
        store.domain_names[domain_name] = domain
    return {"domainNameConfig": domain}


def _get_domain_name(store: AppSyncStore, domain_name: str) -> dict:
    with store.lock:
        domain = store.domain_names.get(domain_name)
    if not domain:
        raise AppSyncError("NotFoundException", f"Domain name {domain_name} not found.", 404)
    return {"domainNameConfig": domain}


def _list_domain_names(store: AppSyncStore) -> dict:
    with store.lock:
        domains = list(store.domain_names.values())
    return {"domainNameConfigs": domains}


def _delete_domain_name(store: AppSyncStore, domain_name: str) -> dict:
    with store.lock:
        if domain_name not in store.domain_names:
            raise AppSyncError("NotFoundException", f"Domain name {domain_name} not found.", 404)
        del store.domain_names[domain_name]
    return {}


def _update_resolver(
    store: AppSyncStore,
    api_id: str,
    type_name: str,
    field_name: str,
    params: dict,
    region: str,
    account_id: str,
) -> dict:
    _require_api(store, api_id)
    key = f"{type_name}.{field_name}"
    with store.lock:
        resolver = store.resolvers.get(api_id, {}).get(key)
        if not resolver:
            raise AppSyncError(
                "NotFoundException",
                f"Resolver {type_name}.{field_name} not found.",
                404,
            )
        for k in (
            "dataSourceName",
            "requestMappingTemplate",
            "responseMappingTemplate",
            "kind",
        ):
            if k in params:
                resolver[k] = params[k]
    return {"resolver": resolver}


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _require_api(store: AppSyncStore, api_id: str) -> None:
    with store.lock:
        if api_id not in store.apis:
            raise AppSyncError("NotFoundException", f"GraphQL API {api_id} not found.", 404)


def _find_resource_by_arn(store: AppSyncStore, arn: str) -> dict | None:
    """Find any resource (API, event API, channel namespace) by ARN."""
    with store.lock:
        for api in store.apis.values():
            if api.get("arn") == arn:
                return api
        for api in store.event_apis.values():
            if api.get("apiArn") == arn:
                return api
        for ns_map in store.channel_namespaces.values():
            for ns in ns_map.values():
                if ns.get("channelNamespaceArn") == arn:
                    return ns
    return None


def _list_tags(store: AppSyncStore, arn: str) -> dict:
    resource = _find_resource_by_arn(store, arn)
    if resource is None:
        raise AppSyncError("NotFoundException", f"Resource {arn} not found.", 404)
    return dict(resource.get("tags", {}))


def _tag_resource(store: AppSyncStore, arn: str, new_tags: dict) -> None:
    resource = _find_resource_by_arn(store, arn)
    if resource is None:
        raise AppSyncError("NotFoundException", f"Resource {arn} not found.", 404)
    with store.lock:
        existing = resource.setdefault("tags", {})
        existing.update(new_tags)


def _untag_resource(store: AppSyncStore, arn: str, tag_keys: list[str]) -> None:
    resource = _find_resource_by_arn(store, arn)
    if resource is None:
        raise AppSyncError("NotFoundException", f"Resource {arn} not found.", 404)
    with store.lock:
        tags = resource.get("tags", {})
        for key in tag_keys:
            tags.pop(key, None)


def _json_response(data: dict, status: int = 200) -> Response:
    return Response(
        content=json.dumps(data, default=str),
        status_code=status,
        media_type="application/json",
    )


def _error(code: str, message: str, status: int) -> Response:
    body = json.dumps({"__type": code, "message": message})
    return Response(content=body, status_code=status, media_type="application/json")


# ---------------------------------------------------------------------------
# Event APIs (v2)
# ---------------------------------------------------------------------------


def _create_event_api(store: AppSyncStore, params: dict, region: str, account_id: str) -> dict:
    api_id = _new_id()[:26]
    name = params.get("name", "")
    if not name:
        raise AppSyncError("BadRequestException", "name is required.")

    event_config = params.get("eventConfig", {})
    api = {
        "apiId": api_id,
        "name": name,
        "apiArn": f"arn:aws:appsync:{region}:{account_id}:apis/{api_id}",
        "dns": {
            "HTTP": f"https://{api_id}.appsync-api.{region}.amazonaws.com/event",
            "REALTIME": f"wss://{api_id}.appsync-realtime-api.{region}.amazonaws.com/event/realtime",
        },
        "eventConfig": event_config,
        "ownerContact": params.get("ownerContact", ""),
        "tags": params.get("tags", {}),
        "createdDate": time.time(),
    }

    with store.lock:
        store.event_apis[api_id] = api
        store.channel_namespaces[api_id] = {}
    return {"api": api}


def _get_event_api(store: AppSyncStore, api_id: str, region: str, account_id: str) -> dict:
    with store.lock:
        api = store.event_apis.get(api_id)
    if not api:
        raise AppSyncError("NotFoundException", f"Event API {api_id} not found.", 404)
    return {"api": api}


def _list_event_apis(store: AppSyncStore) -> dict:
    with store.lock:
        apis = list(store.event_apis.values())
    return {"apis": apis}


def _update_event_api(
    store: AppSyncStore, api_id: str, params: dict, region: str, account_id: str
) -> dict:
    with store.lock:
        api = store.event_apis.get(api_id)
        if not api:
            raise AppSyncError("NotFoundException", f"Event API {api_id} not found.", 404)
        if "name" in params:
            api["name"] = params["name"]
        if "eventConfig" in params:
            api["eventConfig"] = params["eventConfig"]
        if "ownerContact" in params:
            api["ownerContact"] = params["ownerContact"]
    return {"api": api}


def _delete_event_api(store: AppSyncStore, api_id: str) -> dict:
    with store.lock:
        if api_id not in store.event_apis:
            raise AppSyncError("NotFoundException", f"Event API {api_id} not found.", 404)
        del store.event_apis[api_id]
        store.channel_namespaces.pop(api_id, None)
    return {}


# ---------------------------------------------------------------------------
# Channel Namespaces
# ---------------------------------------------------------------------------


def _create_channel_namespace(
    store: AppSyncStore, api_id: str, params: dict, region: str, account_id: str
) -> dict:
    _require_event_api(store, api_id)
    name = params.get("name", "")
    if not name:
        raise AppSyncError("BadRequestException", "name is required.")

    ns = {
        "apiId": api_id,
        "name": name,
        "channelNamespaceArn": (
            f"arn:aws:appsync:{region}:{account_id}:apis/{api_id}/channelNamespace/{name}"
        ),
        "subscribeAuthModes": params.get("subscribeAuthModes", []),
        "publishAuthModes": params.get("publishAuthModes", []),
        "codeHandlers": params.get("codeHandlers", ""),
        "tags": params.get("tags", {}),
        "created": time.time(),
        "lastModified": time.time(),
    }

    with store.lock:
        store.channel_namespaces[api_id][name] = ns
    return {"channelNamespace": ns}


def _get_channel_namespace(
    store: AppSyncStore, api_id: str, name: str, region: str, account_id: str
) -> dict:
    _require_event_api(store, api_id)
    with store.lock:
        ns = store.channel_namespaces.get(api_id, {}).get(name)
    if not ns:
        raise AppSyncError("NotFoundException", f"Channel namespace {name} not found.", 404)
    return {"channelNamespace": ns}


def _list_channel_namespaces(store: AppSyncStore, api_id: str) -> dict:
    _require_event_api(store, api_id)
    with store.lock:
        namespaces = list(store.channel_namespaces.get(api_id, {}).values())
    return {"channelNamespaces": namespaces}


def _update_channel_namespace(
    store: AppSyncStore, api_id: str, name: str, params: dict, region: str, account_id: str
) -> dict:
    _require_event_api(store, api_id)
    with store.lock:
        ns = store.channel_namespaces.get(api_id, {}).get(name)
        if not ns:
            raise AppSyncError("NotFoundException", f"Channel namespace {name} not found.", 404)
        if "subscribeAuthModes" in params:
            ns["subscribeAuthModes"] = params["subscribeAuthModes"]
        if "publishAuthModes" in params:
            ns["publishAuthModes"] = params["publishAuthModes"]
        if "codeHandlers" in params:
            ns["codeHandlers"] = params["codeHandlers"]
        ns["lastModified"] = time.time()
    return {"channelNamespace": ns}


def _delete_channel_namespace(store: AppSyncStore, api_id: str, name: str) -> dict:
    _require_event_api(store, api_id)
    with store.lock:
        namespaces = store.channel_namespaces.get(api_id, {})
        if name not in namespaces:
            raise AppSyncError("NotFoundException", f"Channel namespace {name} not found.", 404)
        del namespaces[name]
    return {}


def _require_event_api(store: AppSyncStore, api_id: str) -> None:
    with store.lock:
        if api_id not in store.event_apis:
            raise AppSyncError("NotFoundException", f"Event API {api_id} not found.", 404)


# ---------------------------------------------------------------------------
# API Cache
# ---------------------------------------------------------------------------

_api_caches: dict[str, dict[str, dict]] = {}  # region -> api_id -> cache


def _create_api_cache(store: AppSyncStore, api_id: str, params: dict) -> dict:
    _require_api(store, api_id)
    cache = {
        "apiCachingBehavior": params.get("apiCachingBehavior", "FULL_REQUEST_CACHING"),
        "atRestEncryptionEnabled": params.get("atRestEncryptionEnabled", False),
        "healthMetricsConfig": params.get("healthMetricsConfig", "DISABLED"),
        "status": "AVAILABLE",
        "transitEncryptionEnabled": params.get("transitEncryptionEnabled", False),
        "ttl": params.get("ttl", 3600),
        "type": params.get("type", "T2_SMALL"),
    }
    # Store per api_id in the store object (reuse store.lock)
    with store.lock:
        if not hasattr(store, "api_caches"):
            store.api_caches = {}
        store.api_caches[api_id] = cache
    return {"apiCache": cache}


def _get_api_cache(store: AppSyncStore, api_id: str) -> dict:
    _require_api(store, api_id)
    with store.lock:
        caches = getattr(store, "api_caches", {})
        cache = caches.get(api_id)
    if not cache:
        raise AppSyncError("NotFoundException", f"No API cache for {api_id}.", 404)
    return {"apiCache": cache}


def _update_api_cache(store: AppSyncStore, api_id: str, params: dict) -> dict:
    _require_api(store, api_id)
    with store.lock:
        caches = getattr(store, "api_caches", {})
        cache = caches.get(api_id)
        if not cache:
            raise AppSyncError("NotFoundException", f"No API cache for {api_id}.", 404)
        for key in (
            "apiCachingBehavior",
            "ttl",
            "type",
            "transitEncryptionEnabled",
            "atRestEncryptionEnabled",
            "healthMetricsConfig",
        ):
            if key in params:
                cache[key] = params[key]
    return {"apiCache": cache}


def _delete_api_cache(store: AppSyncStore, api_id: str) -> None:
    _require_api(store, api_id)
    with store.lock:
        caches = getattr(store, "api_caches", {})
        if api_id not in caches:
            raise AppSyncError("NotFoundException", f"No API cache for {api_id}.", 404)
        del caches[api_id]


def _flush_api_cache(store: AppSyncStore, api_id: str) -> None:
    _require_api(store, api_id)
    # No-op: just verify the API and cache exist
    with store.lock:
        caches = getattr(store, "api_caches", {})
        if api_id not in caches:
            raise AppSyncError("NotFoundException", f"No API cache for {api_id}.", 404)


# ---------------------------------------------------------------------------
# Introspection Schema
# ---------------------------------------------------------------------------


def _get_introspection_schema_response(
    store: AppSyncStore, api_id: str, fmt: str, include_directives: bool
) -> Response:
    _require_api(store, api_id)
    with store.lock:
        schema_info = store.schemas.get(api_id)
    if not schema_info:
        raise AppSyncError("NotFoundException", "Schema not found.", 404)

    definition = schema_info.get("definition", "")
    # Return the schema definition as-is (simplified)
    if fmt == "JSON":
        content = json.dumps({"__schema": {"types": [], "queryType": {"name": "Query"}}})
        return Response(content=content, status_code=200, media_type="application/json")
    # SDL format — return definition text
    if isinstance(definition, bytes):
        import base64

        try:
            definition = base64.b64decode(definition).decode("utf-8")
        except Exception:
            definition = definition.decode("utf-8", errors="replace")
    return Response(content=definition, status_code=200, media_type="text/plain")
