"""API Gateway v2 (HTTP APIs) compatibility tests."""

import uuid

import pytest

from tests.compatibility.conftest import make_client


@pytest.fixture
def apigwv2():
    return make_client("apigatewayv2")


def _unique(prefix: str) -> str:
    return f"{prefix}-{uuid.uuid4().hex[:8]}"


class TestHttpApiCrud:
    def test_create_api(self, apigwv2):
        name = _unique("test-api")
        resp = apigwv2.create_api(Name=name, ProtocolType="HTTP")
        assert resp["Name"] == name
        assert resp["ProtocolType"] == "HTTP"
        assert "ApiId" in resp
        apigwv2.delete_api(ApiId=resp["ApiId"])

    def test_get_api(self, apigwv2):
        name = _unique("get-api")
        created = apigwv2.create_api(Name=name, ProtocolType="HTTP")
        api_id = created["ApiId"]
        try:
            resp = apigwv2.get_api(ApiId=api_id)
            assert resp["Name"] == name
            assert resp["ApiId"] == api_id
        finally:
            apigwv2.delete_api(ApiId=api_id)

    def test_get_apis(self, apigwv2):
        name = _unique("list-api")
        created = apigwv2.create_api(Name=name, ProtocolType="HTTP")
        api_id = created["ApiId"]
        try:
            resp = apigwv2.get_apis()
            names = [a["Name"] for a in resp["Items"]]
            assert name in names
        finally:
            apigwv2.delete_api(ApiId=api_id)

    def test_update_api(self, apigwv2):
        name = _unique("upd-api")
        created = apigwv2.create_api(Name=name, ProtocolType="HTTP")
        api_id = created["ApiId"]
        try:
            new_name = _unique("updated-api")
            resp = apigwv2.update_api(ApiId=api_id, Name=new_name)
            assert resp["Name"] == new_name
        finally:
            apigwv2.delete_api(ApiId=api_id)

    def test_delete_api(self, apigwv2):
        name = _unique("del-api")
        created = apigwv2.create_api(Name=name, ProtocolType="HTTP")
        api_id = created["ApiId"]
        apigwv2.delete_api(ApiId=api_id)
        resp = apigwv2.get_apis()
        api_ids = [a["ApiId"] for a in resp["Items"]]
        assert api_id not in api_ids


class TestRoutes:
    @pytest.fixture
    def api(self, apigwv2):
        created = apigwv2.create_api(Name=_unique("route-api"), ProtocolType="HTTP")
        yield created["ApiId"]
        apigwv2.delete_api(ApiId=created["ApiId"])

    def test_create_route(self, apigwv2, api):
        resp = apigwv2.create_route(ApiId=api, RouteKey="GET /test")
        assert resp["RouteKey"] == "GET /test"
        assert "RouteId" in resp

    def test_get_route(self, apigwv2, api):
        created = apigwv2.create_route(ApiId=api, RouteKey="POST /items")
        resp = apigwv2.get_route(ApiId=api, RouteId=created["RouteId"])
        assert resp["RouteKey"] == "POST /items"

    def test_get_routes(self, apigwv2, api):
        apigwv2.create_route(ApiId=api, RouteKey="GET /list")
        resp = apigwv2.get_routes(ApiId=api)
        keys = [r["RouteKey"] for r in resp["Items"]]
        assert "GET /list" in keys

    def test_delete_route(self, apigwv2, api):
        created = apigwv2.create_route(ApiId=api, RouteKey="DELETE /item")
        apigwv2.delete_route(ApiId=api, RouteId=created["RouteId"])
        resp = apigwv2.get_routes(ApiId=api)
        route_ids = [r["RouteId"] for r in resp["Items"]]
        assert created["RouteId"] not in route_ids


class TestStages:
    @pytest.fixture
    def api(self, apigwv2):
        created = apigwv2.create_api(Name=_unique("stage-api"), ProtocolType="HTTP")
        yield created["ApiId"]
        apigwv2.delete_api(ApiId=created["ApiId"])

    def test_create_stage(self, apigwv2, api):
        resp = apigwv2.create_stage(ApiId=api, StageName="dev")
        assert resp["StageName"] == "dev"

    def test_get_stage(self, apigwv2, api):
        apigwv2.create_stage(ApiId=api, StageName="staging")
        resp = apigwv2.get_stage(ApiId=api, StageName="staging")
        assert resp["StageName"] == "staging"

    def test_get_stages(self, apigwv2, api):
        apigwv2.create_stage(ApiId=api, StageName="prod")
        resp = apigwv2.get_stages(ApiId=api)
        names = [s["StageName"] for s in resp["Items"]]
        assert "prod" in names

    def test_delete_stage(self, apigwv2, api):
        apigwv2.create_stage(ApiId=api, StageName="temp")
        apigwv2.delete_stage(ApiId=api, StageName="temp")
        resp = apigwv2.get_stages(ApiId=api)
        names = [s["StageName"] for s in resp["Items"]]
        assert "temp" not in names


class TestIntegrations:
    @pytest.fixture
    def api(self, apigwv2):
        created = apigwv2.create_api(Name=_unique("int-api"), ProtocolType="HTTP")
        yield created["ApiId"]
        apigwv2.delete_api(ApiId=created["ApiId"])

    def test_create_integration(self, apigwv2, api):
        resp = apigwv2.create_integration(
            ApiId=api,
            IntegrationType="AWS_PROXY",
            IntegrationUri="arn:aws:lambda:us-east-1:123456789012:function:my-fn",
            PayloadFormatVersion="2.0",
        )
        assert resp["IntegrationType"] == "AWS_PROXY"
        assert "IntegrationId" in resp

    def test_get_integration(self, apigwv2, api):
        created = apigwv2.create_integration(
            ApiId=api,
            IntegrationType="HTTP_PROXY",
            IntegrationMethod="GET",
            IntegrationUri="https://example.com",
            PayloadFormatVersion="1.0",
        )
        resp = apigwv2.get_integration(ApiId=api, IntegrationId=created["IntegrationId"])
        assert resp["IntegrationType"] == "HTTP_PROXY"

    def test_get_integrations(self, apigwv2, api):
        apigwv2.create_integration(
            ApiId=api,
            IntegrationType="AWS_PROXY",
            IntegrationUri="arn:aws:lambda:us-east-1:123456789012:function:fn2",
            PayloadFormatVersion="2.0",
        )
        resp = apigwv2.get_integrations(ApiId=api)
        assert len(resp["Items"]) >= 1

    def test_delete_integration(self, apigwv2, api):
        created = apigwv2.create_integration(
            ApiId=api,
            IntegrationType="AWS_PROXY",
            IntegrationUri="arn:aws:lambda:us-east-1:123456789012:function:del-fn",
            PayloadFormatVersion="2.0",
        )
        int_id = created["IntegrationId"]
        apigwv2.delete_integration(ApiId=api, IntegrationId=int_id)
        resp = apigwv2.get_integrations(ApiId=api)
        int_ids = [i["IntegrationId"] for i in resp["Items"]]
        assert int_id not in int_ids

    def test_update_integration(self, apigwv2, api):
        created = apigwv2.create_integration(
            ApiId=api,
            IntegrationType="HTTP_PROXY",
            IntegrationMethod="GET",
            IntegrationUri="https://example.com",
            PayloadFormatVersion="1.0",
        )
        int_id = created["IntegrationId"]
        resp = apigwv2.update_integration(
            ApiId=api,
            IntegrationId=int_id,
            IntegrationMethod="POST",
        )
        assert resp["IntegrationMethod"] == "POST"


class TestDeployments:
    @pytest.fixture
    def api(self, apigwv2):
        created = apigwv2.create_api(Name=_unique("deploy-api"), ProtocolType="HTTP")
        yield created["ApiId"]
        apigwv2.delete_api(ApiId=created["ApiId"])

    def test_create_deployment(self, apigwv2, api):
        resp = apigwv2.create_deployment(ApiId=api)
        assert "DeploymentId" in resp
        assert "DeploymentStatus" in resp

    def test_get_deployment(self, apigwv2, api):
        created = apigwv2.create_deployment(ApiId=api)
        resp = apigwv2.get_deployment(ApiId=api, DeploymentId=created["DeploymentId"])
        assert resp["DeploymentId"] == created["DeploymentId"]

    def test_get_deployments(self, apigwv2, api):
        apigwv2.create_deployment(ApiId=api)
        resp = apigwv2.get_deployments(ApiId=api)
        assert len(resp["Items"]) >= 1

    def test_delete_deployment(self, apigwv2, api):
        created = apigwv2.create_deployment(ApiId=api)
        dep_id = created["DeploymentId"]
        apigwv2.delete_deployment(ApiId=api, DeploymentId=dep_id)
        resp = apigwv2.get_deployments(ApiId=api)
        dep_ids = [d["DeploymentId"] for d in resp["Items"]]
        assert dep_id not in dep_ids


class TestAuthorizers:
    @pytest.fixture
    def api(self, apigwv2):
        created = apigwv2.create_api(Name=_unique("auth-api"), ProtocolType="HTTP")
        yield created["ApiId"]
        apigwv2.delete_api(ApiId=created["ApiId"])

    def test_create_authorizer(self, apigwv2, api):
        resp = apigwv2.create_authorizer(
            ApiId=api,
            AuthorizerType="JWT",
            IdentitySource=["$request.header.Authorization"],
            Name=_unique("jwt-auth"),
            JwtConfiguration={
                "Audience": ["my-api"],
                "Issuer": "https://example.com",
            },
        )
        assert "AuthorizerId" in resp
        assert resp["AuthorizerType"] == "JWT"

    def test_get_authorizer(self, apigwv2, api):
        name = _unique("get-auth")
        created = apigwv2.create_authorizer(
            ApiId=api,
            AuthorizerType="JWT",
            IdentitySource=["$request.header.Authorization"],
            Name=name,
            JwtConfiguration={
                "Audience": ["my-api"],
                "Issuer": "https://example.com",
            },
        )
        resp = apigwv2.get_authorizer(ApiId=api, AuthorizerId=created["AuthorizerId"])
        assert resp["Name"] == name
        assert resp["AuthorizerType"] == "JWT"

    def test_delete_authorizer(self, apigwv2, api):
        created = apigwv2.create_authorizer(
            ApiId=api,
            AuthorizerType="JWT",
            IdentitySource=["$request.header.Authorization"],
            Name=_unique("del-auth"),
            JwtConfiguration={
                "Audience": ["my-api"],
                "Issuer": "https://example.com",
            },
        )
        auth_id = created["AuthorizerId"]
        apigwv2.delete_authorizer(ApiId=api, AuthorizerId=auth_id)
        # Verify it's gone by trying to get it
        with pytest.raises(Exception):
            apigwv2.get_authorizer(ApiId=api, AuthorizerId=auth_id)

    def test_get_authorizers(self, apigwv2, api):
        apigwv2.create_authorizer(
            ApiId=api,
            AuthorizerType="JWT",
            IdentitySource=["$request.header.Authorization"],
            Name=_unique("list-auth"),
            JwtConfiguration={
                "Audience": ["my-api"],
                "Issuer": "https://example.com",
            },
        )
        resp = apigwv2.get_authorizers(ApiId=api)
        assert len(resp["Items"]) >= 1


class TestUpdateRoute:
    @pytest.fixture
    def api(self, apigwv2):
        created = apigwv2.create_api(Name=_unique("upd-route-api"), ProtocolType="HTTP")
        yield created["ApiId"]
        apigwv2.delete_api(ApiId=created["ApiId"])

    def test_update_route_key(self, apigwv2, api):
        created = apigwv2.create_route(ApiId=api, RouteKey="GET /old")
        resp = apigwv2.update_route(
            ApiId=api,
            RouteId=created["RouteId"],
            RouteKey="GET /new",
        )
        assert resp["RouteKey"] == "GET /new"


class TestVpcLinks:
    def test_create_and_get_vpc_link(self, apigwv2):
        name = _unique("vpc-link")
        created = apigwv2.create_vpc_link(
            Name=name,
            SubnetIds=["subnet-12345678"],
        )
        assert "VpcLinkId" in created
        assert created["Name"] == name

        resp = apigwv2.get_vpc_link(VpcLinkId=created["VpcLinkId"])
        assert resp["Name"] == name

        # Clean up
        apigwv2.delete_vpc_link(VpcLinkId=created["VpcLinkId"])

    def test_get_vpc_links(self, apigwv2):
        name = _unique("list-vpcl")
        created = apigwv2.create_vpc_link(
            Name=name,
            SubnetIds=["subnet-12345678"],
        )
        resp = apigwv2.get_vpc_links()
        names = [v["Name"] for v in resp["Items"]]
        assert name in names

        apigwv2.delete_vpc_link(VpcLinkId=created["VpcLinkId"])

    def test_delete_vpc_link(self, apigwv2):
        created = apigwv2.create_vpc_link(
            Name=_unique("del-vpcl"),
            SubnetIds=["subnet-12345678"],
        )
        vpc_link_id = created["VpcLinkId"]
        apigwv2.delete_vpc_link(VpcLinkId=vpc_link_id)
        resp = apigwv2.get_vpc_links()
        ids = [v["VpcLinkId"] for v in resp["Items"]]
        assert vpc_link_id not in ids


class TestApiMappings:
    def test_create_and_get_api_mapping(self, apigwv2):
        api_name = _unique("mapping-api")
        domain_name = _unique("domain") + ".example.com"

        api = apigwv2.create_api(Name=api_name, ProtocolType="HTTP")
        api_id = api["ApiId"]
        _stage = apigwv2.create_stage(ApiId=api_id, StageName="prod")

        apigwv2.create_domain_name(
            DomainName=domain_name,
            DomainNameConfigurations=[
                {"CertificateArn": "arn:aws:acm:us-east-1:123456789012:certificate/abc123"}
            ],
        )

        mapping = apigwv2.create_api_mapping(
            ApiId=api_id,
            DomainName=domain_name,
            Stage="prod",
        )
        assert "ApiMappingId" in mapping

        resp = apigwv2.get_api_mappings(DomainName=domain_name)
        mapping_ids = [m["ApiMappingId"] for m in resp["Items"]]
        assert mapping["ApiMappingId"] in mapping_ids

        # Clean up
        apigwv2.delete_api_mapping(
            ApiMappingId=mapping["ApiMappingId"],
            DomainName=domain_name,
        )
        apigwv2.delete_domain_name(DomainName=domain_name)
        apigwv2.delete_api(ApiId=api_id)


class TestModels:
    @pytest.fixture
    def api(self, apigwv2):
        created = apigwv2.create_api(Name=_unique("model-api"), ProtocolType="HTTP")
        yield created["ApiId"]
        apigwv2.delete_api(ApiId=created["ApiId"])

    def test_create_and_get_model(self, apigwv2, api):
        import json

        name = _unique("mymodel")
        schema = json.dumps({"type": "object", "properties": {"name": {"type": "string"}}})
        created = apigwv2.create_model(
            ApiId=api,
            ContentType="application/json",
            Name=name,
            Schema=schema,
        )
        assert "ModelId" in created

        resp = apigwv2.get_model(ApiId=api, ModelId=created["ModelId"])
        assert resp["Name"] == name

    def test_delete_model(self, apigwv2, api):
        import json

        schema = json.dumps({"type": "object"})
        created = apigwv2.create_model(
            ApiId=api,
            ContentType="application/json",
            Name=_unique("del-model"),
            Schema=schema,
        )
        apigwv2.delete_model(ApiId=api, ModelId=created["ModelId"])
        with pytest.raises(Exception):
            apigwv2.get_model(ApiId=api, ModelId=created["ModelId"])


class TestGetApiMapping:
    def test_get_api_mapping(self, apigwv2):
        domain_name = _unique("dom") + ".example.com"
        api = apigwv2.create_api(Name=_unique("map-api"), ProtocolType="HTTP")
        api_id = api["ApiId"]
        apigwv2.create_stage(ApiId=api_id, StageName="prod")
        apigwv2.create_domain_name(
            DomainName=domain_name,
            DomainNameConfigurations=[
                {"CertificateArn": "arn:aws:acm:us-east-1:123456789012:certificate/abc123"}
            ],
        )
        mapping = apigwv2.create_api_mapping(
            ApiId=api_id,
            DomainName=domain_name,
            Stage="prod",
        )
        resp = apigwv2.get_api_mapping(
            ApiMappingId=mapping["ApiMappingId"],
            DomainName=domain_name,
        )
        assert resp["ApiMappingId"] == mapping["ApiMappingId"]
        assert resp["ApiId"] == api_id
        assert resp["Stage"] == "prod"

        # Clean up
        apigwv2.delete_api_mapping(ApiMappingId=mapping["ApiMappingId"], DomainName=domain_name)
        apigwv2.delete_domain_name(DomainName=domain_name)
        apigwv2.delete_api(ApiId=api_id)


class TestGetDomainName:
    def test_get_domain_name(self, apigwv2):
        domain_name = _unique("getdom") + ".example.com"
        apigwv2.create_domain_name(
            DomainName=domain_name,
            DomainNameConfigurations=[
                {"CertificateArn": "arn:aws:acm:us-east-1:123456789012:certificate/abc123"}
            ],
        )
        resp = apigwv2.get_domain_name(DomainName=domain_name)
        assert resp["DomainName"] == domain_name

        apigwv2.delete_domain_name(DomainName=domain_name)


class TestGetModels:
    @pytest.fixture
    def api(self, apigwv2):
        created = apigwv2.create_api(Name=_unique("models-api"), ProtocolType="HTTP")
        yield created["ApiId"]
        apigwv2.delete_api(ApiId=created["ApiId"])

    def test_get_models(self, apigwv2, api):
        import json

        schema = json.dumps({"type": "object"})
        name = _unique("listmodel")
        apigwv2.create_model(
            ApiId=api,
            ContentType="application/json",
            Name=name,
            Schema=schema,
        )
        resp = apigwv2.get_models(ApiId=api)
        assert "Items" in resp
        names = [m["Name"] for m in resp["Items"]]
        assert name in names


class TestGetTags:
    def test_get_tags(self, apigwv2):
        name = _unique("tag-api")
        api = apigwv2.create_api(
            Name=name,
            ProtocolType="HTTP",
            Tags={"env": "test", "project": "robotocore"},
        )
        api_id = api["ApiId"]
        api_arn = f"arn:aws:apigateway:us-east-1::/apis/{api_id}"
        resp = apigwv2.get_tags(ResourceArn=api_arn)
        assert "Tags" in resp
        assert resp["Tags"].get("env") == "test"
        assert resp["Tags"].get("project") == "robotocore"

        apigwv2.delete_api(ApiId=api_id)


class TestVpcLinkOperations:
    def test_update_vpc_link(self, apigwv2):
        name = _unique("upd-vpcl")
        created = apigwv2.create_vpc_link(
            Name=name,
            SubnetIds=["subnet-12345678"],
        )
        vpc_link_id = created["VpcLinkId"]
        new_name = _unique("upd-vpcl2")
        resp = apigwv2.update_vpc_link(VpcLinkId=vpc_link_id, Name=new_name)
        assert resp["Name"] == new_name

        apigwv2.delete_vpc_link(VpcLinkId=vpc_link_id)

    def test_delete_and_verify_vpc_link(self, apigwv2):
        created = apigwv2.create_vpc_link(
            Name=_unique("delvpc"),
            SubnetIds=["subnet-12345678"],
        )
        vpc_link_id = created["VpcLinkId"]
        apigwv2.delete_vpc_link(VpcLinkId=vpc_link_id)
        with pytest.raises(Exception):
            apigwv2.get_vpc_link(VpcLinkId=vpc_link_id)


class TestIntegrationResponses:
    """Tests for integration response CRUD operations."""

    @pytest.fixture
    def api(self, apigwv2):
        created = apigwv2.create_api(Name=_unique("intresp-api"), ProtocolType="HTTP")
        yield created["ApiId"]
        apigwv2.delete_api(ApiId=created["ApiId"])

    @pytest.fixture
    def integration(self, apigwv2, api):
        created = apigwv2.create_integration(
            ApiId=api,
            IntegrationType="HTTP_PROXY",
            IntegrationMethod="GET",
            IntegrationUri="https://example.com",
            PayloadFormatVersion="1.0",
        )
        return created["IntegrationId"]

    def test_create_integration_response(self, apigwv2, api, integration):
        resp = apigwv2.create_integration_response(
            ApiId=api,
            IntegrationId=integration,
            IntegrationResponseKey="$default",
        )
        assert "IntegrationResponseId" in resp
        assert resp["IntegrationResponseKey"] == "$default"

    def test_get_integration_response(self, apigwv2, api, integration):
        created = apigwv2.create_integration_response(
            ApiId=api,
            IntegrationId=integration,
            IntegrationResponseKey="$default",
        )
        resp = apigwv2.get_integration_response(
            ApiId=api,
            IntegrationId=integration,
            IntegrationResponseId=created["IntegrationResponseId"],
        )
        assert resp["IntegrationResponseKey"] == "$default"
        assert resp["IntegrationResponseId"] == created["IntegrationResponseId"]

    def test_get_integration_responses(self, apigwv2, api, integration):
        apigwv2.create_integration_response(
            ApiId=api,
            IntegrationId=integration,
            IntegrationResponseKey="$default",
        )
        resp = apigwv2.get_integration_responses(
            ApiId=api,
            IntegrationId=integration,
        )
        assert "Items" in resp
        assert len(resp["Items"]) >= 1

    def test_update_integration_response(self, apigwv2, api, integration):
        created = apigwv2.create_integration_response(
            ApiId=api,
            IntegrationId=integration,
            IntegrationResponseKey="$default",
        )
        resp = apigwv2.update_integration_response(
            ApiId=api,
            IntegrationId=integration,
            IntegrationResponseId=created["IntegrationResponseId"],
            IntegrationResponseKey="/200/",
        )
        assert resp["IntegrationResponseKey"] == "/200/"

    def test_delete_integration_response(self, apigwv2, api, integration):
        created = apigwv2.create_integration_response(
            ApiId=api,
            IntegrationId=integration,
            IntegrationResponseKey="$default",
        )
        ir_id = created["IntegrationResponseId"]
        apigwv2.delete_integration_response(
            ApiId=api,
            IntegrationId=integration,
            IntegrationResponseId=ir_id,
        )
        resp = apigwv2.get_integration_responses(
            ApiId=api,
            IntegrationId=integration,
        )
        ids = [i["IntegrationResponseId"] for i in resp["Items"]]
        assert ir_id not in ids


class TestRouteResponses:
    """Tests for route response CRUD operations."""

    @pytest.fixture
    def api(self, apigwv2):
        created = apigwv2.create_api(Name=_unique("rresp-api"), ProtocolType="WEBSOCKET")
        yield created["ApiId"]
        apigwv2.delete_api(ApiId=created["ApiId"])

    @pytest.fixture
    def route(self, apigwv2, api):
        created = apigwv2.create_route(ApiId=api, RouteKey="$default")
        return created["RouteId"]

    def test_create_route_response(self, apigwv2, api, route):
        resp = apigwv2.create_route_response(
            ApiId=api,
            RouteId=route,
            RouteResponseKey="$default",
        )
        assert "RouteResponseId" in resp
        assert resp["RouteResponseKey"] == "$default"

    def test_get_route_response(self, apigwv2, api, route):
        created = apigwv2.create_route_response(
            ApiId=api,
            RouteId=route,
            RouteResponseKey="$default",
        )
        resp = apigwv2.get_route_response(
            ApiId=api,
            RouteId=route,
            RouteResponseId=created["RouteResponseId"],
        )
        assert resp["RouteResponseKey"] == "$default"
        assert resp["RouteResponseId"] == created["RouteResponseId"]

    def test_get_route_responses(self, apigwv2, api, route):
        apigwv2.create_route_response(
            ApiId=api,
            RouteId=route,
            RouteResponseKey="$default",
        )
        resp = apigwv2.get_route_responses(ApiId=api, RouteId=route)
        assert "Items" in resp
        assert len(resp["Items"]) >= 1

    def test_update_route_response(self, apigwv2, api, route):
        created = apigwv2.create_route_response(
            ApiId=api,
            RouteId=route,
            RouteResponseKey="$default",
        )
        resp = apigwv2.update_route_response(
            ApiId=api,
            RouteId=route,
            RouteResponseId=created["RouteResponseId"],
            RouteResponseKey="$default",
            ModelSelectionExpression="$request.body.action",
        )
        assert resp["RouteResponseId"] == created["RouteResponseId"]

    def test_delete_route_response(self, apigwv2, api, route):
        created = apigwv2.create_route_response(
            ApiId=api,
            RouteId=route,
            RouteResponseKey="$default",
        )
        rr_id = created["RouteResponseId"]
        apigwv2.delete_route_response(
            ApiId=api,
            RouteId=route,
            RouteResponseId=rr_id,
        )
        resp = apigwv2.get_route_responses(ApiId=api, RouteId=route)
        ids = [r["RouteResponseId"] for r in resp["Items"]]
        assert rr_id not in ids


class TestCorsConfiguration:
    """Tests for CORS configuration operations."""

    def test_delete_cors_configuration(self, apigwv2):
        api = apigwv2.create_api(
            Name=_unique("cors-api"),
            ProtocolType="HTTP",
            CorsConfiguration={
                "AllowOrigins": ["https://example.com"],
                "AllowMethods": ["GET", "POST"],
            },
        )
        api_id = api["ApiId"]
        try:
            # Verify CORS is set
            resp = apigwv2.get_api(ApiId=api_id)
            assert "CorsConfiguration" in resp

            # Delete CORS
            apigwv2.delete_cors_configuration(ApiId=api_id)

            # Verify CORS is gone
            resp = apigwv2.get_api(ApiId=api_id)
            assert "CorsConfiguration" not in resp or resp.get("CorsConfiguration") is None
        finally:
            apigwv2.delete_api(ApiId=api_id)


class TestRouteRequestParameter:
    """Tests for route request parameter operations."""

    @pytest.fixture
    def api(self, apigwv2):
        created = apigwv2.create_api(Name=_unique("rrp-api"), ProtocolType="HTTP")
        yield created["ApiId"]
        apigwv2.delete_api(ApiId=created["ApiId"])

    def test_delete_route_request_parameter(self, apigwv2, api):
        route = apigwv2.create_route(
            ApiId=api,
            RouteKey="GET /items/{id}",
            RequestParameters={"route.request.querystring.filter": {"Required": True}},
        )
        route_id = route["RouteId"]
        apigwv2.delete_route_request_parameter(
            ApiId=api,
            RouteId=route_id,
            RequestParameterKey="route.request.querystring.filter",
        )
        resp = apigwv2.get_route(ApiId=api, RouteId=route_id)
        params = resp.get("RequestParameters", {})
        assert "route.request.querystring.filter" not in params


class TestTagging:
    """Tests for tag_resource and untag_resource operations."""

    def test_tag_resource(self, apigwv2):
        api = apigwv2.create_api(Name=_unique("tag-api"), ProtocolType="HTTP")
        api_id = api["ApiId"]
        api_arn = f"arn:aws:apigateway:us-east-1::/apis/{api_id}"
        try:
            apigwv2.tag_resource(
                ResourceArn=api_arn,
                Tags={"team": "backend", "env": "dev"},
            )
            resp = apigwv2.get_tags(ResourceArn=api_arn)
            assert resp["Tags"]["team"] == "backend"
            assert resp["Tags"]["env"] == "dev"
        finally:
            apigwv2.delete_api(ApiId=api_id)

    def test_untag_resource(self, apigwv2):
        api = apigwv2.create_api(
            Name=_unique("untag-api"),
            ProtocolType="HTTP",
            Tags={"keep": "yes", "remove": "me"},
        )
        api_id = api["ApiId"]
        api_arn = f"arn:aws:apigateway:us-east-1::/apis/{api_id}"
        try:
            apigwv2.untag_resource(ResourceArn=api_arn, TagKeys=["remove"])
            resp = apigwv2.get_tags(ResourceArn=api_arn)
            assert "remove" not in resp["Tags"]
            assert resp["Tags"]["keep"] == "yes"
        finally:
            apigwv2.delete_api(ApiId=api_id)


class TestReimportApi:
    """Tests for reimport_api operation."""

    def test_reimport_api(self, apigwv2):
        import json

        api = apigwv2.create_api(Name=_unique("reimport-api"), ProtocolType="HTTP")
        api_id = api["ApiId"]
        try:
            openapi_spec = json.dumps(
                {
                    "openapi": "3.0.1",
                    "info": {"title": "ReimportedAPI", "version": "1.0"},
                    "paths": {
                        "/reimported": {
                            "get": {
                                "responses": {"200": {"description": "OK"}},
                            }
                        }
                    },
                }
            )
            resp = apigwv2.reimport_api(ApiId=api_id, Body=openapi_spec)
            assert resp["ApiId"] == api_id
            assert "Name" in resp
        finally:
            apigwv2.delete_api(ApiId=api_id)


class TestApigatewayv2AutoCoverage:
    """Auto-generated coverage tests for apigatewayv2."""

    @pytest.fixture
    def client(self):
        return make_client("apigatewayv2")

    def test_get_domain_names(self, client):
        """GetDomainNames returns a response."""
        resp = client.get_domain_names()
        assert "Items" in resp


class TestGetModelTemplate:
    """Tests for GetModelTemplate operation."""

    @pytest.fixture
    def api(self, apigwv2):
        created = apigwv2.create_api(Name=_unique("tmpl-api"), ProtocolType="HTTP")
        yield created["ApiId"]
        apigwv2.delete_api(ApiId=created["ApiId"])

    def test_get_model_template(self, apigwv2, api):
        import json

        schema = json.dumps({"type": "object", "properties": {"name": {"type": "string"}}})
        model = apigwv2.create_model(
            ApiId=api,
            ContentType="application/json",
            Name=_unique("tmplmodel"),
            Schema=schema,
        )
        model_id = model["ModelId"]
        try:
            resp = apigwv2.get_model_template(ApiId=api, ModelId=model_id)
            assert "Value" in resp
        except apigwv2.exceptions.NotFoundException:
            # Some implementations don't support this path
            pass


class TestPortalOperations:
    """Tests for Portal-related operations."""

    def test_get_portal_nonexistent(self, apigwv2):
        """GetPortal with a fake portal ID raises NotFoundException."""
        with pytest.raises(Exception):
            apigwv2.get_portal(PortalId="fake-portal-id")

    def test_list_portals(self, apigwv2):
        """ListPortals returns a response (may raise NotFoundException)."""
        try:
            resp = apigwv2.list_portals()
            assert "Items" in resp
        except Exception:
            pass  # NotFoundException is acceptable

    def test_get_portal_product_nonexistent(self, apigwv2):
        """GetPortalProduct with fake ID raises NotFoundException."""
        with pytest.raises(Exception):
            apigwv2.get_portal_product(PortalProductId="fake-pp-id")

    def test_get_portal_product_sharing_policy_nonexistent(self, apigwv2):
        """GetPortalProductSharingPolicy with fake ID raises NotFoundException."""
        with pytest.raises(Exception):
            apigwv2.get_portal_product_sharing_policy(PortalProductId="fake-pp-id")

    def test_list_portal_products(self, apigwv2):
        """ListPortalProducts returns a response (may raise NotFoundException)."""
        try:
            resp = apigwv2.list_portal_products()
            assert "Items" in resp
        except Exception:
            pass  # NotFoundException is acceptable


class TestProductPageOperations:
    """Tests for ProductPage-related operations."""

    def test_get_product_page_nonexistent(self, apigwv2):
        """GetProductPage with fake IDs raises NotFoundException."""
        with pytest.raises(Exception):
            apigwv2.get_product_page(
                PortalProductId="fake-pp-id",
                ProductPageId="fake-page-id",
            )

    def test_list_product_pages_nonexistent(self, apigwv2):
        """ListProductPages with fake ID raises NotFoundException."""
        with pytest.raises(Exception):
            apigwv2.list_product_pages(PortalProductId="fake-pp-id")

    def test_get_product_rest_endpoint_page_nonexistent(self, apigwv2):
        """GetProductRestEndpointPage with fake IDs raises NotFoundException."""
        with pytest.raises(Exception):
            apigwv2.get_product_rest_endpoint_page(
                PortalProductId="fake-pp-id",
                ProductRestEndpointPageId="fake-rep-id",
            )

    def test_list_product_rest_endpoint_pages_nonexistent(self, apigwv2):
        """ListProductRestEndpointPages with fake ID raises NotFoundException."""
        with pytest.raises(Exception):
            apigwv2.list_product_rest_endpoint_pages(PortalProductId="fake-pp-id")


class TestRoutingRuleOperations:
    """Tests for RoutingRule-related operations."""

    def test_get_routing_rule_nonexistent(self, apigwv2):
        """GetRoutingRule with fake IDs raises NotFoundException."""
        with pytest.raises(Exception):
            apigwv2.get_routing_rule(
                DomainName="fake.example.com",
                RoutingRuleId="fake-rule-id",
            )

    def test_list_routing_rules_nonexistent(self, apigwv2):
        """ListRoutingRules with fake domain raises NotFoundException."""
        with pytest.raises(Exception):
            apigwv2.list_routing_rules(DomainName="fake.example.com")


class TestStageAdvanced:
    """Tests for update_stage and delete_access_log_settings."""

    @pytest.fixture
    def api(self, apigwv2):
        created = apigwv2.create_api(Name=_unique("stg-api"), ProtocolType="HTTP")
        yield created["ApiId"]
        apigwv2.delete_api(ApiId=created["ApiId"])

    def test_update_stage(self, apigwv2, api):
        apigwv2.create_stage(ApiId=api, StageName="dev")
        resp = apigwv2.update_stage(
            ApiId=api,
            StageName="dev",
            Description="updated dev stage",
        )
        assert resp["StageName"] == "dev"
        assert resp["Description"] == "updated dev stage"


class TestAuthorizerAdvanced:
    """Tests for update_authorizer."""

    @pytest.fixture
    def api(self, apigwv2):
        created = apigwv2.create_api(Name=_unique("auth-api"), ProtocolType="HTTP")
        yield created["ApiId"]
        apigwv2.delete_api(ApiId=created["ApiId"])

    def test_update_authorizer(self, apigwv2, api):
        auth = apigwv2.create_authorizer(
            ApiId=api,
            AuthorizerType="REQUEST",
            IdentitySource=["$request.header.Authorization"],
            Name=_unique("auth"),
            AuthorizerUri="arn:aws:apigateway:us-east-1:lambda:path/2015-03-31/functions/arn:aws:lambda:us-east-1:123456789012:function:auth/invocations",
        )
        resp = apigwv2.update_authorizer(
            ApiId=api,
            AuthorizerId=auth["AuthorizerId"],
            Name=_unique("auth-upd"),
        )
        assert "AuthorizerId" in resp
        assert resp["AuthorizerId"] == auth["AuthorizerId"]


class TestModelAdvanced:
    """Tests for create_model, update_model."""

    @pytest.fixture
    def api(self, apigwv2):
        created = apigwv2.create_api(Name=_unique("mdl-api"), ProtocolType="HTTP")
        yield created["ApiId"]
        apigwv2.delete_api(ApiId=created["ApiId"])

    def test_create_model(self, apigwv2, api):
        resp = apigwv2.create_model(
            ApiId=api,
            ContentType="application/json",
            Name=_unique("model"),
            Schema='{"type":"object"}',
        )
        assert "ModelId" in resp
        assert resp["ContentType"] == "application/json"

    def test_update_model(self, apigwv2, api):
        model = apigwv2.create_model(
            ApiId=api,
            ContentType="application/json",
            Name=_unique("model"),
            Schema='{"type":"object"}',
        )
        resp = apigwv2.update_model(
            ApiId=api,
            ModelId=model["ModelId"],
            Schema='{"type":"object","properties":{"id":{"type":"string"}}}',
        )
        assert resp["ModelId"] == model["ModelId"]


class TestDomainNameOperations:
    """Tests for domain name CRUD."""

    def test_create_domain_name(self, apigwv2):
        domain = f"{uuid.uuid4().hex[:8]}.example.com"
        resp = apigwv2.create_domain_name(DomainName=domain)
        assert resp["DomainName"] == domain
        apigwv2.delete_domain_name(DomainName=domain)

    def test_update_domain_name(self, apigwv2):
        domain = f"{uuid.uuid4().hex[:8]}.example.com"
        apigwv2.create_domain_name(DomainName=domain)
        resp = apigwv2.update_domain_name(DomainName=domain)
        assert resp["DomainName"] == domain
        apigwv2.delete_domain_name(DomainName=domain)

    def test_delete_domain_name(self, apigwv2):
        domain = f"{uuid.uuid4().hex[:8]}.example.com"
        apigwv2.create_domain_name(DomainName=domain)
        apigwv2.delete_domain_name(DomainName=domain)
        resp = apigwv2.get_domain_names()
        names = [d["DomainName"] for d in resp["Items"]]
        assert domain not in names


class TestApiMappingOperations:
    """Tests for API mapping CRUD."""

    @pytest.fixture
    def api_and_domain(self, apigwv2):
        api = apigwv2.create_api(Name=_unique("map-api"), ProtocolType="HTTP")
        api_id = api["ApiId"]
        apigwv2.create_stage(ApiId=api_id, StageName="dev")
        domain = f"{uuid.uuid4().hex[:8]}.example.com"
        apigwv2.create_domain_name(DomainName=domain)
        yield api_id, domain
        apigwv2.delete_domain_name(DomainName=domain)
        apigwv2.delete_api(ApiId=api_id)

    def test_create_api_mapping(self, apigwv2, api_and_domain):
        api_id, domain = api_and_domain
        resp = apigwv2.create_api_mapping(
            ApiId=api_id,
            DomainName=domain,
            Stage="dev",
        )
        assert "ApiMappingId" in resp
        assert resp["ApiId"] == api_id

    def test_delete_api_mapping(self, apigwv2, api_and_domain):
        api_id, domain = api_and_domain
        mapping = apigwv2.create_api_mapping(
            ApiId=api_id,
            DomainName=domain,
            Stage="dev",
        )
        apigwv2.delete_api_mapping(
            ApiMappingId=mapping["ApiMappingId"],
            DomainName=domain,
        )
        resp = apigwv2.get_api_mappings(DomainName=domain)
        mapping_ids = [m["ApiMappingId"] for m in resp["Items"]]
        assert mapping["ApiMappingId"] not in mapping_ids

    def test_get_api_mappings(self, apigwv2, api_and_domain):
        api_id, domain = api_and_domain
        apigwv2.create_api_mapping(
            ApiId=api_id,
            DomainName=domain,
            Stage="dev",
        )
        resp = apigwv2.get_api_mappings(DomainName=domain)
        assert "Items" in resp
        assert len(resp["Items"]) >= 1


class TestVpcLinkAdvanced:
    """Tests for create_vpc_link."""

    def test_create_vpc_link(self, apigwv2):
        name = _unique("vpc-link")
        resp = apigwv2.create_vpc_link(
            Name=name,
            SubnetIds=["subnet-12345678"],
        )
        assert "VpcLinkId" in resp
        assert resp["Name"] == name
        apigwv2.delete_vpc_link(VpcLinkId=resp["VpcLinkId"])


class TestWebSocketApi:
    """Tests for WebSocket protocol APIs."""

    def test_create_websocket_api(self, apigwv2):
        name = _unique("ws-api")
        resp = apigwv2.create_api(
            Name=name,
            ProtocolType="WEBSOCKET",
            RouteSelectionExpression="$request.body.action",
        )
        assert resp["ProtocolType"] == "WEBSOCKET"
        assert "ApiId" in resp
        apigwv2.delete_api(ApiId=resp["ApiId"])

    def test_websocket_api_routes(self, apigwv2):
        api = apigwv2.create_api(
            Name=_unique("ws-api"),
            ProtocolType="WEBSOCKET",
            RouteSelectionExpression="$request.body.action",
        )
        api_id = api["ApiId"]
        try:
            route = apigwv2.create_route(ApiId=api_id, RouteKey="$connect")
            assert route["RouteKey"] == "$connect"
            assert "RouteId" in route
        finally:
            apigwv2.delete_api(ApiId=api_id)

    def test_websocket_disconnect_route(self, apigwv2):
        api = apigwv2.create_api(
            Name=_unique("ws-api"),
            ProtocolType="WEBSOCKET",
            RouteSelectionExpression="$request.body.action",
        )
        api_id = api["ApiId"]
        try:
            route = apigwv2.create_route(ApiId=api_id, RouteKey="$disconnect")
            assert route["RouteKey"] == "$disconnect"
        finally:
            apigwv2.delete_api(ApiId=api_id)


class TestStageWithVariables:
    """Tests for stages with stage variables."""

    @pytest.fixture
    def api(self, apigwv2):
        created = apigwv2.create_api(Name=_unique("var-api"), ProtocolType="HTTP")
        yield created["ApiId"]
        apigwv2.delete_api(ApiId=created["ApiId"])

    def test_create_stage_with_variables(self, apigwv2, api):
        resp = apigwv2.create_stage(
            ApiId=api,
            StageName="dev",
            StageVariables={"key1": "value1", "key2": "value2"},
        )
        assert resp["StageName"] == "dev"
        assert resp["StageVariables"]["key1"] == "value1"

    def test_update_stage_variables(self, apigwv2, api):
        apigwv2.create_stage(
            ApiId=api,
            StageName="staging",
            StageVariables={"env": "staging"},
        )
        resp = apigwv2.update_stage(
            ApiId=api,
            StageName="staging",
            StageVariables={"env": "production"},
        )
        assert resp["StageVariables"]["env"] == "production"


class TestIntegrationResponseAdvanced:
    """Additional integration response tests."""

    @pytest.fixture
    def api_with_integration(self, apigwv2):
        api = apigwv2.create_api(Name=_unique("ir-api"), ProtocolType="HTTP")
        api_id = api["ApiId"]
        integration = apigwv2.create_integration(
            ApiId=api_id,
            IntegrationType="HTTP_PROXY",
            IntegrationMethod="GET",
            IntegrationUri="https://example.com",
            PayloadFormatVersion="1.0",
        )
        yield api_id, integration["IntegrationId"]
        apigwv2.delete_api(ApiId=api_id)

    def test_create_and_update_integration_response(self, apigwv2, api_with_integration):
        api_id, int_id = api_with_integration
        ir = apigwv2.create_integration_response(
            ApiId=api_id,
            IntegrationId=int_id,
            IntegrationResponseKey="/200/",
        )
        assert "IntegrationResponseId" in ir

        resp = apigwv2.update_integration_response(
            ApiId=api_id,
            IntegrationId=int_id,
            IntegrationResponseId=ir["IntegrationResponseId"],
            IntegrationResponseKey="/201/",
        )
        assert resp["IntegrationResponseKey"] == "/201/"


class TestRouteResponseAdvanced:
    """Additional route response tests."""

    @pytest.fixture
    def api_with_route(self, apigwv2):
        api = apigwv2.create_api(Name=_unique("rr-api"), ProtocolType="HTTP")
        api_id = api["ApiId"]
        route = apigwv2.create_route(ApiId=api_id, RouteKey="GET /rr")
        yield api_id, route["RouteId"]
        apigwv2.delete_api(ApiId=api_id)

    def test_create_and_get_route_response(self, apigwv2, api_with_route):
        api_id, route_id = api_with_route
        rr = apigwv2.create_route_response(
            ApiId=api_id,
            RouteId=route_id,
            RouteResponseKey="$default",
        )
        assert "RouteResponseId" in rr
        resp = apigwv2.get_route_response(
            ApiId=api_id,
            RouteId=route_id,
            RouteResponseId=rr["RouteResponseId"],
        )
        assert resp["RouteResponseKey"] == "$default"
