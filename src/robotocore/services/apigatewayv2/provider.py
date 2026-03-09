"""Native API Gateway v2 (HTTP API + WebSocket API) provider.

Implements full CRUD for APIs, routes, integrations, stages, authorizers,
and deployments. Uses REST-JSON protocol.
"""

import json
import re
import threading
import time
import uuid
from urllib.parse import unquote

from starlette.requests import Request
from starlette.responses import Response

_lock = threading.RLock()

# In-memory stores keyed by region
_apis: dict[str, dict[str, dict]] = {}  # region -> api_id -> api
_routes: dict[str, dict[str, dict[str, dict]]] = {}  # region -> api_id -> route_id -> route
_integrations: dict[str, dict[str, dict[str, dict]]] = {}  # region -> api -> integ_id -> integ
_stages: dict[str, dict[str, dict[str, dict]]] = {}  # region -> api_id -> stage_name -> stage
_authorizers: dict[str, dict[str, dict[str, dict]]] = {}  # region -> api -> auth_id -> auth
_deployments: dict[str, dict[str, dict[str, dict]]] = {}  # region -> api -> deploy_id -> deploy
_vpc_links: dict[str, dict[str, dict]] = {}  # region -> vpc_link_id -> vpc_link
_domain_names: dict[str, dict[str, dict]] = {}  # region -> domain_name -> domain
_api_mappings: dict[
    str, dict[str, dict[str, dict]]
] = {}  # region -> domain -> mapping_id -> mapping
_models: dict[str, dict[str, dict[str, dict]]] = {}  # region -> api_id -> model_id -> model
_integration_responses: dict[
    str, dict[str, dict[str, dict[str, dict]]]
] = {}  # region -> api_id -> integ_id -> ir_id -> ir
_route_responses: dict[
    str, dict[str, dict[str, dict[str, dict]]]
] = {}  # region -> api_id -> route_id -> rr_id -> rr
# WebSocket connection tracking
_connections: dict[str, dict[str, dict]] = {}  # api_id -> connection_id -> conn_info


def _store(top: dict, *keys) -> dict:
    """Navigate nested dicts, creating missing intermediates."""
    current = top
    for k in keys:
        with _lock:
            if k not in current:
                current[k] = {}
            current = current[k]
    return current


# ---------------------------------------------------------------------------
# REST-JSON path patterns
# ---------------------------------------------------------------------------

_API_PATH = re.compile(r"^/v2/apis/([^/]+)$")
_APIS_LIST = re.compile(r"^/v2/apis/?$")

_ROUTE_PATH = re.compile(r"^/v2/apis/([^/]+)/routes/([^/]+)$")
_ROUTES_LIST = re.compile(r"^/v2/apis/([^/]+)/routes/?$")

_INTEGRATION_PATH = re.compile(r"^/v2/apis/([^/]+)/integrations/([^/]+)$")
_INTEGRATIONS_LIST = re.compile(r"^/v2/apis/([^/]+)/integrations/?$")

_STAGE_PATH = re.compile(r"^/v2/apis/([^/]+)/stages/([^/]+)$")
_STAGES_LIST = re.compile(r"^/v2/apis/([^/]+)/stages/?$")

_AUTHORIZER_PATH = re.compile(r"^/v2/apis/([^/]+)/authorizers/([^/]+)$")
_AUTHORIZERS_LIST = re.compile(r"^/v2/apis/([^/]+)/authorizers/?$")

_DEPLOYMENT_PATH = re.compile(r"^/v2/apis/([^/]+)/deployments/([^/]+)$")
_DEPLOYMENTS_LIST = re.compile(r"^/v2/apis/([^/]+)/deployments/?$")

_TAGS_PATH = re.compile(r"^/v2/tags/(.+)$")

_VPC_LINK_PATH = re.compile(r"^/v2/vpclinks/([^/]+)$")
_VPC_LINKS_LIST = re.compile(r"^/v2/vpclinks/?$")

_DOMAIN_NAME_PATH = re.compile(r"^/v2/domainnames/([^/]+)$")
_DOMAIN_NAMES_LIST = re.compile(r"^/v2/domainnames/?$")
_API_MAPPING_PATH = re.compile(r"^/v2/domainnames/([^/]+)/apimappings/([^/]+)$")
_API_MAPPINGS_LIST = re.compile(r"^/v2/domainnames/([^/]+)/apimappings/?$")

_MODEL_PATH = re.compile(r"^/v2/apis/([^/]+)/models/([^/]+)$")
_MODELS_LIST = re.compile(r"^/v2/apis/([^/]+)/models/?$")

_INTEGRATION_RESPONSE_PATH = re.compile(
    r"^/v2/apis/([^/]+)/integrations/([^/]+)/integrationresponses/([^/]+)$"
)
_INTEGRATION_RESPONSES_LIST = re.compile(
    r"^/v2/apis/([^/]+)/integrations/([^/]+)/integrationresponses/?$"
)
_ROUTE_RESPONSE_PATH = re.compile(r"^/v2/apis/([^/]+)/routes/([^/]+)/routeresponses/([^/]+)$")
_ROUTE_RESPONSES_LIST = re.compile(r"^/v2/apis/([^/]+)/routes/([^/]+)/routeresponses/?$")
_CORS_PATH = re.compile(r"^/v2/apis/([^/]+)/cors/?$")
_ROUTE_REQUEST_PARAM_PATH = re.compile(r"^/v2/apis/([^/]+)/routes/([^/]+)/requestparameters/(.+)$")


class ApiGatewayV2Error(Exception):
    def __init__(self, code: str, message: str, status: int = 400):
        self.code = code
        self.message = message
        self.status = status


async def handle_apigatewayv2_request(
    request: Request,
    region: str,
    account_id: str,
) -> Response:
    """Handle an API Gateway V2 API request."""
    path = request.url.path
    method = request.method.upper()
    body = await request.body()
    raw_params = json.loads(body) if body else {}
    # Normalize incoming params to PascalCase (boto3 sends camelCase on the wire)
    params = _pascal_keys(raw_params)

    try:
        # APIs
        m = _APIS_LIST.match(path)
        if m:
            if method == "POST":
                return _json_response(_create_api(params, region, account_id), 201)
            if method == "GET":
                return _json_response(_get_apis(region))

        # CORS configuration
        m = _CORS_PATH.match(path)
        if m:
            api_id = m.group(1)
            if method == "DELETE":
                _delete_cors_configuration(api_id, region)
                return Response(status_code=204)

        m = _API_PATH.match(path)
        if m:
            api_id = m.group(1)
            if method == "GET":
                return _json_response(_get_api(api_id, region))
            if method == "PATCH":
                return _json_response(_update_api(api_id, params, region))
            if method == "DELETE":
                _delete_api(api_id, region)
                return Response(status_code=204)
            if method == "PUT":
                # reimport_api
                return _json_response(_reimport_api(api_id, params, region))

        # Route Responses (must match before Routes — longer path)
        m = _ROUTE_RESPONSES_LIST.match(path)
        if m:
            api_id, route_id = m.group(1), m.group(2)
            if method == "POST":
                return _json_response(_create_route_response(api_id, route_id, params, region), 201)
            if method == "GET":
                return _json_response(_get_route_responses(api_id, route_id, region))

        m = _ROUTE_RESPONSE_PATH.match(path)
        if m:
            api_id, route_id, rr_id = m.group(1), m.group(2), m.group(3)
            if method == "GET":
                return _json_response(_get_route_response(api_id, route_id, rr_id, region))
            if method == "PATCH":
                return _json_response(
                    _update_route_response(api_id, route_id, rr_id, params, region)
                )
            if method == "DELETE":
                _delete_route_response(api_id, route_id, rr_id, region)
                return Response(status_code=204)

        # Route Request Parameters
        m = _ROUTE_REQUEST_PARAM_PATH.match(path)
        if m:
            api_id, route_id, param_key = m.group(1), m.group(2), unquote(m.group(3))
            if method == "DELETE":
                _delete_route_request_parameter(api_id, route_id, param_key, region)
                return Response(status_code=204)

        # Routes
        m = _ROUTES_LIST.match(path)
        if m:
            api_id = m.group(1)
            if method == "POST":
                return _json_response(_create_route(api_id, params, region, account_id), 201)
            if method == "GET":
                return _json_response(_get_routes(api_id, region))

        m = _ROUTE_PATH.match(path)
        if m:
            api_id, route_id = m.group(1), m.group(2)
            if method == "GET":
                return _json_response(_get_route(api_id, route_id, region))
            if method == "PATCH":
                return _json_response(_update_route(api_id, route_id, params, region))
            if method == "DELETE":
                _delete_route(api_id, route_id, region)
                return Response(status_code=204)

        # Integration Responses (must match before Integrations — longer path)
        m = _INTEGRATION_RESPONSES_LIST.match(path)
        if m:
            api_id, integ_id = m.group(1), m.group(2)
            if method == "POST":
                return _json_response(
                    _create_integration_response(api_id, integ_id, params, region), 201
                )
            if method == "GET":
                return _json_response(_get_integration_responses(api_id, integ_id, region))

        m = _INTEGRATION_RESPONSE_PATH.match(path)
        if m:
            api_id, integ_id, ir_id = m.group(1), m.group(2), m.group(3)
            if method == "GET":
                return _json_response(_get_integration_response(api_id, integ_id, ir_id, region))
            if method == "PATCH":
                return _json_response(
                    _update_integration_response(api_id, integ_id, ir_id, params, region)
                )
            if method == "DELETE":
                _delete_integration_response(api_id, integ_id, ir_id, region)
                return Response(status_code=204)

        # Integrations
        m = _INTEGRATIONS_LIST.match(path)
        if m:
            api_id = m.group(1)
            if method == "POST":
                return _json_response(_create_integration(api_id, params, region, account_id), 201)
            if method == "GET":
                return _json_response(_get_integrations(api_id, region))

        m = _INTEGRATION_PATH.match(path)
        if m:
            api_id, integ_id = m.group(1), m.group(2)
            if method == "GET":
                return _json_response(_get_integration(api_id, integ_id, region))
            if method == "PATCH":
                return _json_response(_update_integration(api_id, integ_id, params, region))
            if method == "DELETE":
                _delete_integration(api_id, integ_id, region)
                return Response(status_code=204)

        # Stages
        m = _STAGES_LIST.match(path)
        if m:
            api_id = m.group(1)
            if method == "POST":
                return _json_response(_create_stage(api_id, params, region, account_id), 201)
            if method == "GET":
                return _json_response(_get_stages(api_id, region))

        m = _STAGE_PATH.match(path)
        if m:
            api_id, stage_name = m.group(1), m.group(2)
            if method == "GET":
                return _json_response(_get_stage(api_id, stage_name, region))
            if method == "PATCH":
                return _json_response(_update_stage(api_id, stage_name, params, region))
            if method == "DELETE":
                _delete_stage(api_id, stage_name, region)
                return Response(status_code=204)

        # Authorizers
        m = _AUTHORIZERS_LIST.match(path)
        if m:
            api_id = m.group(1)
            if method == "POST":
                return _json_response(_create_authorizer(api_id, params, region, account_id), 201)
            if method == "GET":
                return _json_response(_get_authorizers(api_id, region))

        m = _AUTHORIZER_PATH.match(path)
        if m:
            api_id, auth_id = m.group(1), m.group(2)
            if method == "GET":
                return _json_response(_get_authorizer(api_id, auth_id, region))
            if method == "PATCH":
                return _json_response(_update_authorizer(api_id, auth_id, params, region))
            if method == "DELETE":
                _delete_authorizer(api_id, auth_id, region)
                return Response(status_code=204)

        # Deployments
        m = _DEPLOYMENTS_LIST.match(path)
        if m:
            api_id = m.group(1)
            if method == "POST":
                return _json_response(_create_deployment(api_id, params, region, account_id), 201)
            if method == "GET":
                return _json_response(_get_deployments(api_id, region))

        m = _DEPLOYMENT_PATH.match(path)
        if m:
            api_id, deploy_id = m.group(1), m.group(2)
            if method == "GET":
                return _json_response(_get_deployment(api_id, deploy_id, region))
            if method == "DELETE":
                _delete_deployment(api_id, deploy_id, region)
                return Response(status_code=204)

        # VPC Links
        m = _VPC_LINKS_LIST.match(path)
        if m:
            if method == "POST":
                return _json_response(_create_vpc_link(params, region, account_id), 201)
            if method == "GET":
                return _json_response(_get_vpc_links(region))

        m = _VPC_LINK_PATH.match(path)
        if m:
            vpc_link_id = m.group(1)
            if method == "GET":
                return _json_response(_get_vpc_link(vpc_link_id, region))
            if method == "PATCH":
                return _json_response(_update_vpc_link(vpc_link_id, params, region))
            if method == "DELETE":
                _delete_vpc_link(vpc_link_id, region)
                return Response(status_code=204)

        # Domain Names
        m = _DOMAIN_NAMES_LIST.match(path)
        if m:
            if method == "POST":
                return _json_response(_create_domain_name(params, region, account_id), 201)
            if method == "GET":
                return _json_response(_get_domain_names(region))

        m = _API_MAPPINGS_LIST.match(path)
        if m:
            domain = m.group(1)
            if method == "POST":
                return _json_response(_create_api_mapping(domain, params, region), 201)
            if method == "GET":
                return _json_response(_get_api_mappings(domain, region))

        m = _API_MAPPING_PATH.match(path)
        if m:
            domain, mapping_id = m.group(1), m.group(2)
            if method == "GET":
                return _json_response(_get_api_mapping(domain, mapping_id, region))
            if method == "DELETE":
                _delete_api_mapping(domain, mapping_id, region)
                return Response(status_code=204)

        m = _DOMAIN_NAME_PATH.match(path)
        if m:
            domain = m.group(1)
            if method == "GET":
                return _json_response(_get_domain_name(domain, region))
            if method == "PATCH":
                return _json_response(_update_domain_name(domain, params, region))
            if method == "DELETE":
                _delete_domain_name(domain, region)
                return Response(status_code=204)

        # Models
        m = _MODELS_LIST.match(path)
        if m:
            api_id = m.group(1)
            if method == "POST":
                return _json_response(_create_model(api_id, params, region), 201)
            if method == "GET":
                return _json_response(_get_models(api_id, region))

        m = _MODEL_PATH.match(path)
        if m:
            api_id, model_id = m.group(1), m.group(2)
            if method == "GET":
                return _json_response(_get_model(api_id, model_id, region))
            if method == "PATCH":
                return _json_response(_update_model(api_id, model_id, params, region))
            if method == "DELETE":
                _delete_model(api_id, model_id, region)
                return Response(status_code=204)

        # Tags
        m = _TAGS_PATH.match(path)
        if m:
            resource_arn = unquote(m.group(1))
            if method == "GET":
                return _json_response({"Tags": _list_tags_v2(resource_arn, region)})
            if method == "POST":
                new_tags = params.get("Tags", {})
                _tag_resource_v2(resource_arn, new_tags, region)
                return _json_response({})
            if method == "DELETE":
                tag_keys = request.query_params.getlist("tagKeys")
                _untag_resource_v2(resource_arn, tag_keys, region)
                return Response(status_code=204)

        return _error("NotFoundException", f"Unknown path: {method} {path}", 404)

    except ApiGatewayV2Error as e:
        return _error(e.code, e.message, e.status)
    except Exception as e:
        return _error("InternalServerException", str(e), 500)


# ---------------------------------------------------------------------------
# API CRUD
# ---------------------------------------------------------------------------


def _create_api(params: dict, region: str, account_id: str) -> dict:
    apis = _store(_apis, region)
    api_id = _short_id()
    protocol_type = params.get("ProtocolType", "HTTP")

    api = {
        "ApiId": api_id,
        "ApiEndpoint": f"https://{api_id}.execute-api.{region}.amazonaws.com",
        "Name": params.get("Name", ""),
        "ProtocolType": protocol_type,
        "RouteSelectionExpression": params.get(
            "RouteSelectionExpression",
            "$request.body.action"
            if protocol_type == "WEBSOCKET"
            else "${request.method} ${request.path}",
        ),
        "Description": params.get("Description", ""),
        "DisableSchemaValidation": params.get("DisableSchemaValidation", False),
        "DisableExecuteApiEndpoint": params.get("DisableExecuteApiEndpoint", False),
        "ApiKeySelectionExpression": params.get(
            "ApiKeySelectionExpression", "$request.header.x-api-key"
        ),
        "CorsConfiguration": params.get("CorsConfiguration"),
        "Version": params.get("Version", ""),
        "Tags": params.get("Tags", {}),
        "CreatedDate": _iso_time(),
    }

    with _lock:
        apis[api_id] = api

    # Auto-create $default stage for HTTP APIs if requested
    if protocol_type == "HTTP":
        _store(_stages, region, api_id)

    return api


def _get_api(api_id: str, region: str) -> dict:
    apis = _store(_apis, region)
    with _lock:
        api = apis.get(api_id)
    if not api:
        raise ApiGatewayV2Error("NotFoundException", f"API {api_id} not found", 404)
    return api


def _get_apis(region: str) -> dict:
    apis = _store(_apis, region)
    with _lock:
        items = list(apis.values())
    return {"Items": items}


def _update_api(api_id: str, params: dict, region: str) -> dict:
    apis = _store(_apis, region)
    with _lock:
        api = apis.get(api_id)
        if not api:
            raise ApiGatewayV2Error("NotFoundException", f"API {api_id} not found", 404)
        for key in (
            "Name",
            "Description",
            "RouteSelectionExpression",
            "CorsConfiguration",
            "Version",
            "DisableSchemaValidation",
            "DisableExecuteApiEndpoint",
        ):
            if key in params:
                api[key] = params[key]
    return api


def _delete_api(api_id: str, region: str) -> None:
    apis = _store(_apis, region)
    with _lock:
        if api_id not in apis:
            raise ApiGatewayV2Error("NotFoundException", f"API {api_id} not found", 404)
        del apis[api_id]
    # Clean up related resources
    for store in (_routes, _integrations, _stages, _authorizers, _deployments):
        region_store = store.get(region, {})
        region_store.pop(api_id, None)


# ---------------------------------------------------------------------------
# Route CRUD
# ---------------------------------------------------------------------------


def _create_route(api_id: str, params: dict, region: str, account_id: str) -> dict:
    _require_api(api_id, region)
    routes = _store(_routes, region, api_id)
    route_id = _short_id()

    route = {
        "RouteId": route_id,
        "RouteKey": params.get("RouteKey", ""),
        "Target": params.get("Target", ""),
        "AuthorizationType": params.get("AuthorizationType", "NONE"),
        "AuthorizerId": params.get("AuthorizerId"),
        "ApiKeyRequired": params.get("ApiKeyRequired", False),
        "ModelSelectionExpression": params.get("ModelSelectionExpression"),
        "OperationName": params.get("OperationName"),
        "RequestModels": params.get("RequestModels"),
        "RequestParameters": params.get("RequestParameters"),
        "RouteResponseSelectionExpression": params.get("RouteResponseSelectionExpression"),
    }

    with _lock:
        routes[route_id] = route

    _auto_deploy_if_needed(api_id, region, account_id)
    return route


def _get_route(api_id: str, route_id: str, region: str) -> dict:
    _require_api(api_id, region)
    routes = _store(_routes, region, api_id)
    with _lock:
        route = routes.get(route_id)
    if not route:
        raise ApiGatewayV2Error("NotFoundException", f"Route {route_id} not found", 404)
    return route


def _get_routes(api_id: str, region: str) -> dict:
    _require_api(api_id, region)
    routes = _store(_routes, region, api_id)
    with _lock:
        items = list(routes.values())
    return {"Items": items}


def _update_route(api_id: str, route_id: str, params: dict, region: str) -> dict:
    _require_api(api_id, region)
    routes = _store(_routes, region, api_id)
    with _lock:
        route = routes.get(route_id)
        if not route:
            raise ApiGatewayV2Error("NotFoundException", f"Route {route_id} not found", 404)
        for key in (
            "RouteKey",
            "Target",
            "AuthorizationType",
            "AuthorizerId",
            "ApiKeyRequired",
            "OperationName",
        ):
            if key in params:
                route[key] = params[key]
    return route


def _delete_route(api_id: str, route_id: str, region: str) -> None:
    _require_api(api_id, region)
    routes = _store(_routes, region, api_id)
    with _lock:
        if route_id not in routes:
            raise ApiGatewayV2Error("NotFoundException", f"Route {route_id} not found", 404)
        del routes[route_id]


# ---------------------------------------------------------------------------
# Integration CRUD
# ---------------------------------------------------------------------------


def _create_integration(
    api_id: str,
    params: dict,
    region: str,
    account_id: str,
) -> dict:
    _require_api(api_id, region)
    integrations = _store(_integrations, region, api_id)
    integ_id = _short_id()

    integ = {
        "IntegrationId": integ_id,
        "IntegrationType": params.get("IntegrationType", "AWS_PROXY"),
        "IntegrationUri": params.get("IntegrationUri", ""),
        "IntegrationMethod": params.get("IntegrationMethod", "POST"),
        "PayloadFormatVersion": params.get("PayloadFormatVersion", "2.0"),
        "ConnectionType": params.get("ConnectionType", "INTERNET"),
        "Description": params.get("Description", ""),
        "TimeoutInMillis": params.get("TimeoutInMillis", 30000),
        "RequestParameters": params.get("RequestParameters"),
        "RequestTemplates": params.get("RequestTemplates"),
        "ResponseParameters": params.get("ResponseParameters"),
        "TemplateSelectionExpression": params.get("TemplateSelectionExpression"),
        "CredentialsArn": params.get("CredentialsArn"),
    }

    with _lock:
        integrations[integ_id] = integ

    _auto_deploy_if_needed(api_id, region, account_id)
    return integ


def _get_integration(api_id: str, integ_id: str, region: str) -> dict:
    _require_api(api_id, region)
    integrations = _store(_integrations, region, api_id)
    with _lock:
        integ = integrations.get(integ_id)
    if not integ:
        raise ApiGatewayV2Error("NotFoundException", f"Integration {integ_id} not found", 404)
    return integ


def _get_integrations(api_id: str, region: str) -> dict:
    _require_api(api_id, region)
    integrations = _store(_integrations, region, api_id)
    with _lock:
        items = list(integrations.values())
    return {"Items": items}


def _update_integration(
    api_id: str,
    integ_id: str,
    params: dict,
    region: str,
) -> dict:
    _require_api(api_id, region)
    integrations = _store(_integrations, region, api_id)
    with _lock:
        integ = integrations.get(integ_id)
        if not integ:
            raise ApiGatewayV2Error("NotFoundException", f"Integration {integ_id} not found", 404)
        for key in (
            "IntegrationType",
            "IntegrationUri",
            "IntegrationMethod",
            "PayloadFormatVersion",
            "Description",
            "TimeoutInMillis",
            "RequestParameters",
            "RequestTemplates",
        ):
            if key in params:
                integ[key] = params[key]
    return integ


def _delete_integration(api_id: str, integ_id: str, region: str) -> None:
    _require_api(api_id, region)
    integrations = _store(_integrations, region, api_id)
    with _lock:
        if integ_id not in integrations:
            raise ApiGatewayV2Error("NotFoundException", f"Integration {integ_id} not found", 404)
        del integrations[integ_id]


# ---------------------------------------------------------------------------
# Integration Response CRUD
# ---------------------------------------------------------------------------


def _create_integration_response(api_id: str, integ_id: str, params: dict, region: str) -> dict:
    _require_api(api_id, region)
    _get_integration(api_id, integ_id, region)  # ensure integration exists
    irs = _store(_integration_responses, region, api_id, integ_id)
    ir_id = _short_id()
    ir = {
        "IntegrationResponseId": ir_id,
        "IntegrationResponseKey": params.get("IntegrationResponseKey", "$default"),
        "ContentHandlingStrategy": params.get("ContentHandlingStrategy"),
        "ResponseParameters": params.get("ResponseParameters"),
        "ResponseTemplates": params.get("ResponseTemplates"),
        "TemplateSelectionExpression": params.get("TemplateSelectionExpression"),
    }
    with _lock:
        irs[ir_id] = ir
    return ir


def _get_integration_response(api_id: str, integ_id: str, ir_id: str, region: str) -> dict:
    _require_api(api_id, region)
    irs = _store(_integration_responses, region, api_id, integ_id)
    with _lock:
        ir = irs.get(ir_id)
    if not ir:
        raise ApiGatewayV2Error("NotFoundException", f"Integration response {ir_id} not found", 404)
    return ir


def _get_integration_responses(api_id: str, integ_id: str, region: str) -> dict:
    _require_api(api_id, region)
    irs = _store(_integration_responses, region, api_id, integ_id)
    with _lock:
        items = list(irs.values())
    return {"Items": items}


def _update_integration_response(
    api_id: str, integ_id: str, ir_id: str, params: dict, region: str
) -> dict:
    _require_api(api_id, region)
    irs = _store(_integration_responses, region, api_id, integ_id)
    with _lock:
        ir = irs.get(ir_id)
        if not ir:
            raise ApiGatewayV2Error(
                "NotFoundException", f"Integration response {ir_id} not found", 404
            )
        for key in (
            "IntegrationResponseKey",
            "ContentHandlingStrategy",
            "ResponseParameters",
            "ResponseTemplates",
            "TemplateSelectionExpression",
        ):
            if key in params:
                ir[key] = params[key]
    return ir


def _delete_integration_response(api_id: str, integ_id: str, ir_id: str, region: str) -> None:
    _require_api(api_id, region)
    irs = _store(_integration_responses, region, api_id, integ_id)
    with _lock:
        if ir_id not in irs:
            raise ApiGatewayV2Error(
                "NotFoundException", f"Integration response {ir_id} not found", 404
            )
        del irs[ir_id]


# ---------------------------------------------------------------------------
# Route Response CRUD
# ---------------------------------------------------------------------------


def _create_route_response(api_id: str, route_id: str, params: dict, region: str) -> dict:
    _require_api(api_id, region)
    _get_route(api_id, route_id, region)  # ensure route exists
    rrs = _store(_route_responses, region, api_id, route_id)
    rr_id = _short_id()
    rr = {
        "RouteResponseId": rr_id,
        "RouteResponseKey": params.get("RouteResponseKey", "$default"),
        "ModelSelectionExpression": params.get("ModelSelectionExpression"),
        "ResponseModels": params.get("ResponseModels"),
        "ResponseParameters": params.get("ResponseParameters"),
    }
    with _lock:
        rrs[rr_id] = rr
    return rr


def _get_route_response(api_id: str, route_id: str, rr_id: str, region: str) -> dict:
    _require_api(api_id, region)
    rrs = _store(_route_responses, region, api_id, route_id)
    with _lock:
        rr = rrs.get(rr_id)
    if not rr:
        raise ApiGatewayV2Error("NotFoundException", f"Route response {rr_id} not found", 404)
    return rr


def _get_route_responses(api_id: str, route_id: str, region: str) -> dict:
    _require_api(api_id, region)
    rrs = _store(_route_responses, region, api_id, route_id)
    with _lock:
        items = list(rrs.values())
    return {"Items": items}


def _update_route_response(
    api_id: str, route_id: str, rr_id: str, params: dict, region: str
) -> dict:
    _require_api(api_id, region)
    rrs = _store(_route_responses, region, api_id, route_id)
    with _lock:
        rr = rrs.get(rr_id)
        if not rr:
            raise ApiGatewayV2Error("NotFoundException", f"Route response {rr_id} not found", 404)
        for key in (
            "RouteResponseKey",
            "ModelSelectionExpression",
            "ResponseModels",
            "ResponseParameters",
        ):
            if key in params:
                rr[key] = params[key]
    return rr


def _delete_route_response(api_id: str, route_id: str, rr_id: str, region: str) -> None:
    _require_api(api_id, region)
    rrs = _store(_route_responses, region, api_id, route_id)
    with _lock:
        if rr_id not in rrs:
            raise ApiGatewayV2Error("NotFoundException", f"Route response {rr_id} not found", 404)
        del rrs[rr_id]


# ---------------------------------------------------------------------------
# CORS, Route Request Parameters, Reimport
# ---------------------------------------------------------------------------


def _delete_cors_configuration(api_id: str, region: str) -> None:
    _require_api(api_id, region)
    apis = _store(_apis, region)
    with _lock:
        api = apis.get(api_id)
        if api:
            api.pop("CorsConfiguration", None)


def _delete_route_request_parameter(
    api_id: str, route_id: str, param_key: str, region: str
) -> None:
    _require_api(api_id, region)
    routes = _store(_routes, region, api_id)
    with _lock:
        route = routes.get(route_id)
        if not route:
            raise ApiGatewayV2Error("NotFoundException", f"Route {route_id} not found", 404)
        params = route.get("RequestParameters")
        if params and param_key in params:
            del params[param_key]


def _reimport_api(api_id: str, params: dict, region: str) -> dict:
    """Reimport an API from an OpenAPI spec. Simplified implementation."""
    _require_api(api_id, region)
    apis = _store(_apis, region)
    with _lock:
        api = apis.get(api_id)
        if not api:
            raise ApiGatewayV2Error("NotFoundException", f"API {api_id} not found", 404)
    # Parse the body if it's an OpenAPI spec
    body = params.get("Body", "{}")
    try:
        spec = json.loads(body) if isinstance(body, str) else body
    except (json.JSONDecodeError, TypeError):
        spec = {}
    # Update API name from spec info title if present
    info = spec.get("info", {})
    if "title" in info:
        with _lock:
            api["Name"] = info["title"]
    return api


# ---------------------------------------------------------------------------
# Stage CRUD
# ---------------------------------------------------------------------------


def _create_stage(
    api_id: str,
    params: dict,
    region: str,
    account_id: str,
) -> dict:
    _require_api(api_id, region)
    stages = _store(_stages, region, api_id)
    stage_name = params.get("StageName", "$default")

    with _lock:
        if stage_name in stages:
            raise ApiGatewayV2Error("ConflictException", f"Stage {stage_name} already exists", 409)

    stage = {
        "StageName": stage_name,
        "AutoDeploy": params.get("AutoDeploy", False),
        "Description": params.get("Description", ""),
        "StageVariables": params.get("StageVariables", {}),
        "DeploymentId": params.get("DeploymentId", ""),
        "DefaultRouteSettings": params.get("DefaultRouteSettings", {}),
        "RouteSettings": params.get("RouteSettings", {}),
        "AccessLogSettings": params.get("AccessLogSettings"),
        "Tags": params.get("Tags", {}),
        "CreatedDate": _iso_time(),
        "LastUpdatedDate": _iso_time(),
    }

    with _lock:
        stages[stage_name] = stage
    return stage


def _get_stage(api_id: str, stage_name: str, region: str) -> dict:
    _require_api(api_id, region)
    stages = _store(_stages, region, api_id)
    with _lock:
        stage = stages.get(stage_name)
    if not stage:
        raise ApiGatewayV2Error("NotFoundException", f"Stage {stage_name} not found", 404)
    return stage


def _get_stages(api_id: str, region: str) -> dict:
    _require_api(api_id, region)
    stages = _store(_stages, region, api_id)
    with _lock:
        items = list(stages.values())
    return {"Items": items}


def _update_stage(
    api_id: str,
    stage_name: str,
    params: dict,
    region: str,
) -> dict:
    _require_api(api_id, region)
    stages = _store(_stages, region, api_id)
    with _lock:
        stage = stages.get(stage_name)
        if not stage:
            raise ApiGatewayV2Error("NotFoundException", f"Stage {stage_name} not found", 404)
        for key in (
            "AutoDeploy",
            "Description",
            "StageVariables",
            "DeploymentId",
            "DefaultRouteSettings",
            "RouteSettings",
        ):
            if key in params:
                stage[key] = params[key]
        stage["LastUpdatedDate"] = _iso_time()
    return stage


def _delete_stage(api_id: str, stage_name: str, region: str) -> None:
    _require_api(api_id, region)
    stages = _store(_stages, region, api_id)
    with _lock:
        if stage_name not in stages:
            raise ApiGatewayV2Error("NotFoundException", f"Stage {stage_name} not found", 404)
        del stages[stage_name]


# ---------------------------------------------------------------------------
# Authorizer CRUD
# ---------------------------------------------------------------------------


def _create_authorizer(
    api_id: str,
    params: dict,
    region: str,
    account_id: str,
) -> dict:
    _require_api(api_id, region)
    authorizers = _store(_authorizers, region, api_id)
    auth_id = _short_id()

    auth = {
        "AuthorizerId": auth_id,
        "AuthorizerType": params.get("AuthorizerType", "JWT"),
        "Name": params.get("Name", ""),
        "IdentitySource": params.get("IdentitySource", "$request.header.Authorization"),
        "JwtConfiguration": params.get("JwtConfiguration"),
        "AuthorizerUri": params.get("AuthorizerUri"),
        "AuthorizerCredentialsArn": params.get("AuthorizerCredentialsArn"),
        "AuthorizerResultTtlInSeconds": params.get("AuthorizerResultTtlInSeconds", 300),
        "AuthorizerPayloadFormatVersion": params.get("AuthorizerPayloadFormatVersion"),
        "EnableSimpleResponses": params.get("EnableSimpleResponses", False),
    }

    with _lock:
        authorizers[auth_id] = auth
    return auth


def _get_authorizer(api_id: str, auth_id: str, region: str) -> dict:
    _require_api(api_id, region)
    authorizers = _store(_authorizers, region, api_id)
    with _lock:
        auth = authorizers.get(auth_id)
    if not auth:
        raise ApiGatewayV2Error("NotFoundException", f"Authorizer {auth_id} not found", 404)
    return auth


def _get_authorizers(api_id: str, region: str) -> dict:
    _require_api(api_id, region)
    authorizers = _store(_authorizers, region, api_id)
    with _lock:
        items = list(authorizers.values())
    return {"Items": items}


def _update_authorizer(
    api_id: str,
    auth_id: str,
    params: dict,
    region: str,
) -> dict:
    _require_api(api_id, region)
    authorizers = _store(_authorizers, region, api_id)
    with _lock:
        auth = authorizers.get(auth_id)
        if not auth:
            raise ApiGatewayV2Error("NotFoundException", f"Authorizer {auth_id} not found", 404)
        for key in (
            "AuthorizerType",
            "Name",
            "IdentitySource",
            "JwtConfiguration",
            "AuthorizerUri",
            "AuthorizerResultTtlInSeconds",
        ):
            if key in params:
                auth[key] = params[key]
    return auth


def _delete_authorizer(api_id: str, auth_id: str, region: str) -> None:
    _require_api(api_id, region)
    authorizers = _store(_authorizers, region, api_id)
    with _lock:
        if auth_id not in authorizers:
            raise ApiGatewayV2Error("NotFoundException", f"Authorizer {auth_id} not found", 404)
        del authorizers[auth_id]


# ---------------------------------------------------------------------------
# Deployment CRUD
# ---------------------------------------------------------------------------


def _create_deployment(
    api_id: str,
    params: dict,
    region: str,
    account_id: str,
) -> dict:
    _require_api(api_id, region)
    deployments = _store(_deployments, region, api_id)
    deploy_id = _short_id()

    deployment = {
        "DeploymentId": deploy_id,
        "Description": params.get("Description", ""),
        "DeploymentStatus": "DEPLOYED",
        "StageName": params.get("StageName"),
        "CreatedDate": _iso_time(),
        "AutoDeployed": False,
    }

    with _lock:
        deployments[deploy_id] = deployment

    # Update stage deployment ID if specified
    stage_name = params.get("StageName")
    if stage_name:
        stages = _store(_stages, region, api_id)
        with _lock:
            stage = stages.get(stage_name)
            if stage:
                stage["DeploymentId"] = deploy_id

    return deployment


def _get_deployment(api_id: str, deploy_id: str, region: str) -> dict:
    _require_api(api_id, region)
    deployments = _store(_deployments, region, api_id)
    with _lock:
        deploy = deployments.get(deploy_id)
    if not deploy:
        raise ApiGatewayV2Error("NotFoundException", f"Deployment {deploy_id} not found", 404)
    return deploy


def _get_deployments(api_id: str, region: str) -> dict:
    _require_api(api_id, region)
    deployments = _store(_deployments, region, api_id)
    with _lock:
        items = list(deployments.values())
    return {"Items": items}


def _delete_deployment(api_id: str, deploy_id: str, region: str) -> None:
    _require_api(api_id, region)
    deployments = _store(_deployments, region, api_id)
    with _lock:
        if deploy_id not in deployments:
            raise ApiGatewayV2Error("NotFoundException", f"Deployment {deploy_id} not found", 404)
        del deployments[deploy_id]


# ---------------------------------------------------------------------------
# VPC Link CRUD
# ---------------------------------------------------------------------------


def _create_vpc_link(params: dict, region: str, account_id: str) -> dict:
    links = _store(_vpc_links, region)
    vpc_link_id = _short_id()
    link = {
        "VpcLinkId": vpc_link_id,
        "Name": params.get("Name", ""),
        "SubnetIds": params.get("SubnetIds", []),
        "SecurityGroupIds": params.get("SecurityGroupIds", []),
        "VpcLinkStatus": "AVAILABLE",
        "VpcLinkStatusMessage": "VPC link is ready to route traffic",
        "VpcLinkVersion": "V2",
        "Tags": params.get("Tags", {}),
        "CreatedDate": _iso_time(),
    }
    with _lock:
        links[vpc_link_id] = link
    return link


def _get_vpc_link(vpc_link_id: str, region: str) -> dict:
    links = _store(_vpc_links, region)
    with _lock:
        link = links.get(vpc_link_id)
    if not link:
        raise ApiGatewayV2Error("NotFoundException", f"VPC link {vpc_link_id} not found", 404)
    return link


def _get_vpc_links(region: str) -> dict:
    links = _store(_vpc_links, region)
    with _lock:
        items = list(links.values())
    return {"Items": items}


def _update_vpc_link(vpc_link_id: str, params: dict, region: str) -> dict:
    links = _store(_vpc_links, region)
    with _lock:
        link = links.get(vpc_link_id)
        if not link:
            raise ApiGatewayV2Error("NotFoundException", f"VPC link {vpc_link_id} not found", 404)
        for key in ("Name", "SecurityGroupIds"):
            if key in params:
                link[key] = params[key]
    return link


def _delete_vpc_link(vpc_link_id: str, region: str) -> None:
    links = _store(_vpc_links, region)
    with _lock:
        if vpc_link_id not in links:
            raise ApiGatewayV2Error("NotFoundException", f"VPC link {vpc_link_id} not found", 404)
        del links[vpc_link_id]


# ---------------------------------------------------------------------------
# Domain Name CRUD
# ---------------------------------------------------------------------------


def _create_domain_name(params: dict, region: str, account_id: str) -> dict:
    domains = _store(_domain_names, region)
    domain_name = params.get("DomainName", "")
    domain = {
        "DomainName": domain_name,
        "DomainNameConfigurations": params.get("DomainNameConfigurations", []),
        "MutualTlsAuthentication": params.get("MutualTlsAuthentication"),
        "Tags": params.get("Tags", {}),
        "ApiMappingSelectionExpression": "$request.basepath",
    }
    with _lock:
        domains[domain_name] = domain
    return domain


def _get_domain_name(domain: str, region: str) -> dict:
    domains = _store(_domain_names, region)
    with _lock:
        d = domains.get(domain)
    if not d:
        raise ApiGatewayV2Error("NotFoundException", f"Domain {domain} not found", 404)
    return d


def _get_domain_names(region: str) -> dict:
    domains = _store(_domain_names, region)
    with _lock:
        items = list(domains.values())
    return {"Items": items}


def _update_domain_name(domain: str, params: dict, region: str) -> dict:
    domains = _store(_domain_names, region)
    with _lock:
        d = domains.get(domain)
        if not d:
            raise ApiGatewayV2Error("NotFoundException", f"Domain {domain} not found", 404)
        for key in ("DomainNameConfigurations", "MutualTlsAuthentication"):
            if key in params:
                d[key] = params[key]
    return d


def _delete_domain_name(domain: str, region: str) -> None:
    domains = _store(_domain_names, region)
    with _lock:
        if domain not in domains:
            raise ApiGatewayV2Error("NotFoundException", f"Domain {domain} not found", 404)
        del domains[domain]
    # Clean up api mappings
    _api_mappings.get(region, {}).pop(domain, None)


# ---------------------------------------------------------------------------
# API Mapping CRUD
# ---------------------------------------------------------------------------


def _create_api_mapping(domain: str, params: dict, region: str) -> dict:
    # Verify domain exists
    _get_domain_name(domain, region)
    mappings = _store(_api_mappings, region, domain)
    mapping_id = _short_id()
    mapping = {
        "ApiMappingId": mapping_id,
        "ApiId": params.get("ApiId", ""),
        "ApiMappingKey": params.get("ApiMappingKey", ""),
        "Stage": params.get("Stage", ""),
    }
    with _lock:
        mappings[mapping_id] = mapping
    return mapping


def _get_api_mapping(domain: str, mapping_id: str, region: str) -> dict:
    mappings = _store(_api_mappings, region, domain)
    with _lock:
        mapping = mappings.get(mapping_id)
    if not mapping:
        raise ApiGatewayV2Error("NotFoundException", f"API mapping {mapping_id} not found", 404)
    return mapping


def _get_api_mappings(domain: str, region: str) -> dict:
    mappings = _store(_api_mappings, region, domain)
    with _lock:
        items = list(mappings.values())
    return {"Items": items}


def _delete_api_mapping(domain: str, mapping_id: str, region: str) -> None:
    mappings = _store(_api_mappings, region, domain)
    with _lock:
        if mapping_id not in mappings:
            raise ApiGatewayV2Error("NotFoundException", f"API mapping {mapping_id} not found", 404)
        del mappings[mapping_id]


# ---------------------------------------------------------------------------
# Model CRUD
# ---------------------------------------------------------------------------


def _create_model(api_id: str, params: dict, region: str) -> dict:
    _require_api(api_id, region)
    models = _store(_models, region, api_id)
    model_id = _short_id()
    model = {
        "ModelId": model_id,
        "ContentType": params.get("ContentType", "application/json"),
        "Description": params.get("Description", ""),
        "Name": params.get("Name", ""),
        "Schema": params.get("Schema", ""),
    }
    with _lock:
        models[model_id] = model
    return model


def _get_model(api_id: str, model_id: str, region: str) -> dict:
    _require_api(api_id, region)
    models = _store(_models, region, api_id)
    with _lock:
        model = models.get(model_id)
    if not model:
        raise ApiGatewayV2Error("NotFoundException", f"Model {model_id} not found", 404)
    return model


def _get_models(api_id: str, region: str) -> dict:
    _require_api(api_id, region)
    models = _store(_models, region, api_id)
    with _lock:
        items = list(models.values())
    return {"Items": items}


def _update_model(api_id: str, model_id: str, params: dict, region: str) -> dict:
    _require_api(api_id, region)
    models = _store(_models, region, api_id)
    with _lock:
        model = models.get(model_id)
        if not model:
            raise ApiGatewayV2Error("NotFoundException", f"Model {model_id} not found", 404)
        for key in ("ContentType", "Description", "Name", "Schema"):
            if key in params:
                model[key] = params[key]
    return model


def _delete_model(api_id: str, model_id: str, region: str) -> None:
    _require_api(api_id, region)
    models = _store(_models, region, api_id)
    with _lock:
        if model_id not in models:
            raise ApiGatewayV2Error("NotFoundException", f"Model {model_id} not found", 404)
        del models[model_id]


# ---------------------------------------------------------------------------
# Auto-deploy
# ---------------------------------------------------------------------------


def _auto_deploy_if_needed(api_id: str, region: str, account_id: str) -> None:
    """If any stage has AutoDeploy=True, create a deployment."""
    stages = _store(_stages, region, api_id)
    with _lock:
        for stage in stages.values():
            if stage.get("AutoDeploy"):
                deploy_id = _short_id()
                deployments = _store(_deployments, region, api_id)
                deployments[deploy_id] = {
                    "DeploymentId": deploy_id,
                    "Description": "Auto-deployed",
                    "DeploymentStatus": "DEPLOYED",
                    "CreatedDate": _iso_time(),
                    "AutoDeployed": True,
                }
                stage["DeploymentId"] = deploy_id
                break


# ---------------------------------------------------------------------------
# WebSocket connection management
# ---------------------------------------------------------------------------


def create_connection(api_id: str, connection_id: str | None = None) -> str:
    """Register a new WebSocket connection. Returns connection_id."""
    conn_id = connection_id or str(uuid.uuid4()).replace("-", "")[:12]
    conns = _store(_connections, api_id)
    with _lock:
        conns[conn_id] = {
            "connectionId": conn_id,
            "connectedAt": int(time.time() * 1000),
        }
    return conn_id


def delete_connection(api_id: str, connection_id: str) -> bool:
    """Remove a WebSocket connection. Returns True if found."""
    conns = _store(_connections, api_id)
    with _lock:
        if connection_id in conns:
            del conns[connection_id]
            return True
    return False


def get_connection(api_id: str, connection_id: str) -> dict | None:
    """Get WebSocket connection info."""
    conns = _store(_connections, api_id)
    with _lock:
        return conns.get(connection_id)


def list_connections(api_id: str) -> list[dict]:
    """List active WebSocket connections."""
    conns = _store(_connections, api_id)
    with _lock:
        return list(conns.values())


def post_to_connection(api_id: str, connection_id: str, data: bytes) -> bool:
    """Send data to a WebSocket connection. Returns True if connection exists."""
    conns = _store(_connections, api_id)
    with _lock:
        conn = conns.get(connection_id)
        if not conn:
            return False
        # In a real implementation, this would push through a WebSocket.
        # For emulation, store the last message.
        conn["lastMessage"] = data.decode() if isinstance(data, bytes) else data
        conn["lastMessageAt"] = int(time.time() * 1000)
    return True


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _require_api(api_id: str, region: str) -> dict:
    """Verify API exists, raise if not."""
    apis = _store(_apis, region)
    with _lock:
        api = apis.get(api_id)
    if not api:
        raise ApiGatewayV2Error("NotFoundException", f"API {api_id} not found", 404)
    return api


def _short_id() -> str:
    """Generate a short random ID like AWS uses."""
    return uuid.uuid4().hex[:10]


def _iso_time() -> str:
    return time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())


def _find_resource_by_arn_v2(resource_arn: str, region: str) -> dict | None:
    """Find any v2 resource by ARN. Searches APIs, stages, routes, etc."""
    apis = _store(_apis, region)
    with _lock:
        for api in apis.values():
            if f"/apis/{api.get('ApiId', '')}" in resource_arn:
                return api
    return None


def _list_tags_v2(resource_arn: str, region: str) -> dict:
    resource = _find_resource_by_arn_v2(resource_arn, region)
    if resource is None:
        return {}
    with _lock:
        return dict(resource.get("Tags", {}))


def _tag_resource_v2(resource_arn: str, new_tags: dict, region: str) -> None:
    resource = _find_resource_by_arn_v2(resource_arn, region)
    if resource is None:
        return
    with _lock:
        existing = resource.setdefault("Tags", {})
        existing.update(new_tags)


def _untag_resource_v2(resource_arn: str, tag_keys: list[str], region: str) -> None:
    resource = _find_resource_by_arn_v2(resource_arn, region)
    if resource is None:
        return
    with _lock:
        tags = resource.get("Tags", {})
        for key in tag_keys:
            tags.pop(key, None)


def _to_camel(key: str) -> str:
    """Convert PascalCase to camelCase (e.g., ApiId -> apiId)."""
    if not key:
        return key
    return key[0].lower() + key[1:]


def _to_pascal(key: str) -> str:
    """Convert camelCase to PascalCase (e.g., apiId -> ApiId)."""
    if not key:
        return key
    return key[0].upper() + key[1:]


# Keys whose values are user-defined dicts/content and should NOT have
# their sub-keys case-converted.
_PASSTHROUGH_KEYS = frozenset(
    {
        "Tags",
        "tags",
        "StageVariables",
        "stageVariables",
        "ResponseParameters",
        "responseParameters",
        "RequestParameters",
        "requestParameters",
    }
)


def _camel_keys(obj, _parent_key=None):
    """Recursively convert all dict keys from PascalCase to camelCase."""
    if isinstance(obj, dict):
        if _parent_key in _PASSTHROUGH_KEYS:
            return obj
        return {_to_camel(k): _camel_keys(v, _parent_key=k) for k, v in obj.items()}
    if isinstance(obj, list):
        return [_camel_keys(item, _parent_key=_parent_key) for item in obj]
    return obj


def _pascal_keys(obj, _parent_key=None):
    """Recursively convert all dict keys from camelCase to PascalCase."""
    if isinstance(obj, dict):
        if _parent_key in _PASSTHROUGH_KEYS:
            return obj
        return {_to_pascal(k): _pascal_keys(v, _parent_key=_to_pascal(k)) for k, v in obj.items()}
    if isinstance(obj, list):
        return [_pascal_keys(item, _parent_key=_parent_key) for item in obj]
    return obj


def _json_response(data: dict, status: int = 200) -> Response:
    return Response(
        content=json.dumps(_camel_keys(data), default=str),
        status_code=status,
        media_type="application/json",
    )


def _error(code: str, message: str, status: int) -> Response:
    body = json.dumps({"Message": message, "Code": code})
    return Response(content=body, status_code=status, media_type="application/json")


# ---------------------------------------------------------------------------
# Public accessor for executor
# ---------------------------------------------------------------------------


def get_api_store(region: str) -> dict:
    """Get the API store for a region (used by executor)."""
    return _store(_apis, region)


def get_route_store(region: str, api_id: str) -> dict:
    """Get routes for an API (used by executor)."""
    return _store(_routes, region, api_id)


def get_integration_store(region: str, api_id: str) -> dict:
    """Get integrations for an API (used by executor)."""
    return _store(_integrations, region, api_id)


def get_stage_store(region: str, api_id: str) -> dict:
    """Get stages for an API (used by executor)."""
    return _store(_stages, region, api_id)


def get_authorizer_store(region: str, api_id: str) -> dict:
    """Get authorizers for an API (used by executor)."""
    return _store(_authorizers, region, api_id)
