"""Unit tests for the dashboard module."""

import os
import re
from unittest.mock import patch

from starlette.testclient import TestClient

from robotocore.dashboard.app import DASHBOARD_HTML, dashboard_endpoint


class TestDashboardEndpoint:
    """Test the dashboard HTTP endpoint."""

    def _make_client(self) -> TestClient:
        from starlette.applications import Starlette
        from starlette.routing import Route

        app = Starlette(routes=[Route("/_robotocore/dashboard", dashboard_endpoint)])
        return TestClient(app)

    def test_returns_200(self) -> None:
        client = self._make_client()
        resp = client.get("/_robotocore/dashboard")
        assert resp.status_code == 200

    def test_content_type_is_html(self) -> None:
        client = self._make_client()
        resp = client.get("/_robotocore/dashboard")
        assert "text/html" in resp.headers["content-type"]

    def test_contains_overview_section(self) -> None:
        assert 'id="overview"' in DASHBOARD_HTML or "Overview" in DASHBOARD_HTML

    def test_contains_resources_section(self) -> None:
        assert 'id="resources"' in DASHBOARD_HTML or "Resources" in DASHBOARD_HTML

    def test_contains_chaos_section(self) -> None:
        assert 'id="chaos"' in DASHBOARD_HTML or "Chaos" in DASHBOARD_HTML

    def test_contains_audit_section(self) -> None:
        assert 'id="audit"' in DASHBOARD_HTML or "Audit" in DASHBOARD_HTML

    def test_contains_config_section(self) -> None:
        assert 'id="config"' in DASHBOARD_HTML or "Config" in DASHBOARD_HTML

    def test_contains_services_section(self) -> None:
        assert 'id="services"' in DASHBOARD_HTML or "Services" in DASHBOARD_HTML

    def test_fetches_health_endpoint(self) -> None:
        assert "/_robotocore/health" in DASHBOARD_HTML

    def test_fetches_resources_endpoint(self) -> None:
        assert "/_robotocore/resources" in DASHBOARD_HTML

    def test_fetches_chaos_endpoint(self) -> None:
        assert "/_robotocore/chaos/rules" in DASHBOARD_HTML

    def test_fetches_audit_endpoint(self) -> None:
        assert "/_robotocore/audit" in DASHBOARD_HTML

    def test_fetches_config_endpoint(self) -> None:
        assert "/_robotocore/config" in DASHBOARD_HTML

    def test_fetches_services_endpoint(self) -> None:
        assert "/_robotocore/services" in DASHBOARD_HTML

    def test_navigation_sidebar(self) -> None:
        assert "<nav" in DASHBOARD_HTML

    def test_auto_refresh_toggle(self) -> None:
        assert "auto-refresh" in DASHBOARD_HTML.lower() or "autoRefresh" in DASHBOARD_HTML

    def test_dark_theme_styles(self) -> None:
        # Dark theme should have dark background colors
        html_lower = DASHBOARD_HTML.lower()
        assert "background" in html_lower
        # Should contain dark color values (e.g. #1a, #2a, #0d, rgb(1x,...)
        assert re.search(r"#[012][0-9a-f]{5}", html_lower) or "rgb(1" in html_lower

    def test_no_external_dependencies(self) -> None:
        # No CDN links or external script/stylesheet references
        assert "cdn." not in DASHBOARD_HTML.lower()
        assert "googleapis.com" not in DASHBOARD_HTML
        assert "unpkg.com" not in DASHBOARD_HTML
        assert "jsdelivr.net" not in DASHBOARD_HTML

    def test_no_unclosed_tags(self) -> None:
        # Basic check: count opening vs closing for key tags
        for tag in ["div", "table", "nav", "section", "script", "style"]:
            open_count = len(re.findall(rf"<{tag}[\s>]", DASHBOARD_HTML, re.IGNORECASE))
            close_count = len(re.findall(rf"</{tag}>", DASHBOARD_HTML, re.IGNORECASE))
            assert open_count == close_count, (
                f"Unclosed <{tag}>: {open_count} opens, {close_count} closes"
            )

    def test_disabled_via_env(self) -> None:
        with patch.dict(os.environ, {"DASHBOARD_DISABLED": "1"}):
            client = self._make_client()
            resp = client.get("/_robotocore/dashboard")
            assert resp.status_code == 404


class TestDashboardRouteRegistered:
    """Test that dashboard route is registered in the main app."""

    def test_route_in_management_routes(self) -> None:
        from robotocore.gateway.app import management_routes

        paths = [r.path for r in management_routes]
        assert "/_robotocore/dashboard" in paths
