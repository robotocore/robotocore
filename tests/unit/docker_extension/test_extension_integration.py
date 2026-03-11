"""Semantic integration tests for Docker Desktop extension."""

from __future__ import annotations

import re
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[3]
EXT_DIR = REPO_ROOT / "docker-extension"


class TestExtensionStructure:
    """Test extension has all required files."""

    REQUIRED_FILES = [
        "Dockerfile",
        "docker-compose.yaml",
        "metadata.json",
        "ui/index.html",
        "vm/main.py",
    ]

    def test_all_required_files_exist(self) -> None:
        missing = [f for f in self.REQUIRED_FILES if not (EXT_DIR / f).exists()]
        assert not missing, f"Missing required extension files: {missing}"


class TestUIJavaScriptEndpoints:
    """Test UI JavaScript references correct API endpoints."""

    def test_ui_references_status_api(self) -> None:
        html = (EXT_DIR / "ui" / "index.html").read_text()
        assert re.search(r"/status", html), "UI JavaScript must reference /status endpoint"

    def test_ui_references_start_api(self) -> None:
        html = (EXT_DIR / "ui" / "index.html").read_text()
        assert re.search(r"/start", html), "UI JavaScript must reference /start endpoint"

    def test_ui_references_stop_api(self) -> None:
        html = (EXT_DIR / "ui" / "index.html").read_text()
        assert re.search(r"/stop", html), "UI JavaScript must reference /stop endpoint"

    def test_ui_references_logs_api(self) -> None:
        html = (EXT_DIR / "ui" / "index.html").read_text()
        assert re.search(r"/logs", html), "UI JavaScript must reference /logs endpoint"


class TestConfigurationForm:
    """Test configuration form includes expected env var fields."""

    def test_form_has_services_field(self) -> None:
        html = (EXT_DIR / "ui" / "index.html").read_text()
        assert re.search(r"SERVICES", html, re.IGNORECASE), (
            "Config form must include SERVICES field"
        )

    def test_form_has_enforce_iam_field(self) -> None:
        html = (EXT_DIR / "ui" / "index.html").read_text()
        assert re.search(r"ENFORCE_IAM", html, re.IGNORECASE), (
            "Config form must include ENFORCE_IAM field"
        )

    def test_form_has_persistence_field(self) -> None:
        html = (EXT_DIR / "ui" / "index.html").read_text()
        assert re.search(r"PERSISTENCE", html, re.IGNORECASE), (
            "Config form must include PERSISTENCE field"
        )

    def test_form_has_log_level_field(self) -> None:
        html = (EXT_DIR / "ui" / "index.html").read_text()
        assert re.search(r"LOG_LEVEL", html, re.IGNORECASE), (
            "Config form must include LOG_LEVEL field"
        )
