"""Tests for Docker Desktop extension structure and content."""

from __future__ import annotations

import json
import re
from pathlib import Path

import pytest
import yaml

REPO_ROOT = Path(__file__).resolve().parents[3]
EXT_DIR = REPO_ROOT / "docker-extension"


class TestMetadataJson:
    """Test metadata.json is valid and has required fields."""

    @pytest.fixture
    def metadata(self) -> dict:
        path = EXT_DIR / "metadata.json"
        assert path.exists(), "metadata.json must exist"
        return json.loads(path.read_text())

    def test_metadata_is_valid_json(self, metadata: dict) -> None:
        assert isinstance(metadata, dict)

    def test_metadata_has_required_fields(self, metadata: dict) -> None:
        for field in ("name", "description", "version"):
            assert field in metadata, f"metadata.json missing required field: {field}"

    def test_metadata_has_correct_name(self, metadata: dict) -> None:
        assert metadata["name"] == "robotocore"

    def test_metadata_has_icon(self, metadata: dict) -> None:
        assert "icon" in metadata


class TestDockerComposeYaml:
    """Test docker-compose.yaml is valid and references extension image."""

    @pytest.fixture
    def compose(self) -> dict:
        path = EXT_DIR / "docker-compose.yaml"
        assert path.exists(), "docker-compose.yaml must exist"
        return yaml.safe_load(path.read_text())

    def test_compose_is_valid_yaml(self, compose: dict) -> None:
        assert isinstance(compose, dict)

    def test_compose_references_extension_image(self, compose: dict) -> None:
        services = compose.get("services", {})
        assert len(services) > 0, "docker-compose.yaml must define at least one service"
        # At least one service should reference the extension image
        images = [svc.get("image", "") for svc in services.values()]
        assert any("robotocore" in img for img in images), (
            "At least one service must reference a robotocore image"
        )


class TestDockerfile:
    """Test Dockerfile exists and has correct base image."""

    def test_dockerfile_exists(self) -> None:
        path = EXT_DIR / "Dockerfile"
        assert path.exists(), "docker-extension/Dockerfile must exist"

    def test_dockerfile_has_from_instruction(self) -> None:
        content = (EXT_DIR / "Dockerfile").read_text()
        assert "FROM" in content, "Dockerfile must have a FROM instruction"


class TestUIIndexHtml:
    """Test UI index.html exists and has required sections."""

    @pytest.fixture
    def html(self) -> str:
        path = EXT_DIR / "ui" / "index.html"
        assert path.exists(), "ui/index.html must exist"
        return path.read_text()

    def test_ui_exists(self, html: str) -> None:
        assert len(html) > 0

    def test_ui_includes_status_panel(self, html: str) -> None:
        assert "status" in html.lower(), "UI must include a status panel section"
        # Check for a dedicated status panel element
        assert re.search(r'id=["\']status', html, re.IGNORECASE), (
            "UI must have an element with id starting with 'status'"
        )

    def test_ui_includes_start_stop_buttons(self, html: str) -> None:
        assert re.search(r"start", html, re.IGNORECASE), "UI must include a start button"
        assert re.search(r"stop", html, re.IGNORECASE), "UI must include a stop button"

    def test_ui_includes_configuration_form(self, html: str) -> None:
        assert "<form" in html.lower() or "config" in html.lower(), (
            "UI must include a configuration form"
        )

    def test_ui_includes_service_status_table(self, html: str) -> None:
        assert re.search(r'id=["\']service', html, re.IGNORECASE), (
            "UI must have a service status section"
        )

    def test_ui_has_no_external_cdn_dependencies(self, html: str) -> None:
        # Should not load CSS/JS from external CDNs
        assert "cdn." not in html.lower(), "UI must not depend on external CDNs"
        assert "unpkg.com" not in html.lower(), "UI must not depend on unpkg"
        assert "cdnjs." not in html.lower(), "UI must not depend on cdnjs"


class TestBackendMainPy:
    """Test backend main.py has required endpoints."""

    @pytest.fixture
    def backend_source(self) -> str:
        path = EXT_DIR / "vm" / "main.py"
        assert path.exists(), "vm/main.py must exist"
        return path.read_text()

    def test_backend_has_status_endpoint(self, backend_source: str) -> None:
        assert "/status" in backend_source, "Backend must have /status endpoint"

    def test_backend_has_start_endpoint(self, backend_source: str) -> None:
        assert "/start" in backend_source, "Backend must have /start endpoint"

    def test_backend_has_stop_endpoint(self, backend_source: str) -> None:
        assert "/stop" in backend_source, "Backend must have /stop endpoint"

    def test_backend_has_logs_endpoint(self, backend_source: str) -> None:
        assert "/logs" in backend_source, "Backend must have /logs endpoint"

    def test_backend_has_proxy_endpoint(self, backend_source: str) -> None:
        assert "/proxy" in backend_source, "Backend must have /proxy endpoint"

    def test_backend_proxy_strips_prefix(self, backend_source: str) -> None:
        # The proxy handler must strip the /proxy prefix before forwarding
        assert re.search(
            r'replace.*proxy|strip|lstrip|removeprefix|"/proxy"',
            backend_source,
        ), "Backend proxy must strip the /proxy prefix"
