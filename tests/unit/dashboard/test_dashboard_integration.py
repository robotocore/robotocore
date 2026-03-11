"""Semantic integration tests for the dashboard."""

import re

from starlette.testclient import TestClient

from robotocore.dashboard.app import DASHBOARD_HTML, dashboard_endpoint


class TestDashboardEndToEnd:
    """Test the full dashboard response structure."""

    def _make_client(self) -> TestClient:
        from starlette.applications import Starlette
        from starlette.routing import Route

        app = Starlette(routes=[Route("/_robotocore/dashboard", dashboard_endpoint)])
        return TestClient(app)

    def test_get_returns_html_with_structure(self) -> None:
        client = self._make_client()
        resp = client.get("/_robotocore/dashboard")
        assert resp.status_code == 200
        body = resp.text
        assert "<!DOCTYPE html>" in body or "<html" in body
        assert "</html>" in body
        assert "<head>" in body
        assert "<body>" in body

    def test_references_all_api_endpoints(self) -> None:
        """Dashboard must make AJAX calls to all management endpoints."""
        required_endpoints = [
            "/_robotocore/health",
            "/_robotocore/resources",
            "/_robotocore/chaos/rules",
            "/_robotocore/audit",
            "/_robotocore/config",
            "/_robotocore/services",
        ]
        for endpoint in required_endpoints:
            assert endpoint in DASHBOARD_HTML, f"Missing fetch for {endpoint}"

    def test_includes_fetch_calls(self) -> None:
        """Dashboard must use fetch() to call APIs."""
        assert "fetch(" in DASHBOARD_HTML

    def test_no_external_urls(self) -> None:
        """Dashboard must be fully self-contained -- no external URLs."""
        # Find all URLs in the HTML
        urls = re.findall(r'(?:href|src|url)\s*[=:(]\s*["\']?(https?://[^"\')\s]+)', DASHBOARD_HTML)
        for url in urls:
            # Allow localhost references only
            assert "localhost" in url or "127.0.0.1" in url, f"External URL found: {url}"

    def test_contains_inline_css(self) -> None:
        """All CSS must be inline in <style> tags."""
        assert "<style>" in DASHBOARD_HTML
        assert "</style>" in DASHBOARD_HTML

    def test_contains_inline_js(self) -> None:
        """All JS must be inline in <script> tags."""
        assert "<script>" in DASHBOARD_HTML
        assert "</script>" in DASHBOARD_HTML

    def test_no_external_stylesheet_links(self) -> None:
        """No <link rel="stylesheet" href="..."> to external resources."""
        links = re.findall(r'<link[^>]+rel=["\']stylesheet["\'][^>]*>', DASHBOARD_HTML)
        for link in links:
            assert "http" not in link, f"External stylesheet link: {link}"

    def test_no_external_script_src(self) -> None:
        """No <script src="http..."> external scripts."""
        scripts = re.findall(r'<script[^>]+src=["\']([^"\']+)["\']', DASHBOARD_HTML)
        for src in scripts:
            assert not src.startswith("http"), f"External script src: {src}"
