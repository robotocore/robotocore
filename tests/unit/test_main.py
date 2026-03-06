"""Tests for robotocore.main — entry point, CLI args, server startup config."""

import os
from unittest.mock import patch

from robotocore.main import main


class TestMain:
    """Test the main() entry point with various environment configurations."""

    @patch("robotocore.main.uvicorn")
    def test_default_config(self, mock_uvicorn):
        """Without env vars, defaults to 127.0.0.1:4566, no reload, info log."""
        with patch.dict(os.environ, {}, clear=True):
            # Remove any robotocore env vars that might be set
            env = {k: v for k, v in os.environ.items() if not k.startswith("ROBOTOCORE_")}
            with patch.dict(os.environ, env, clear=True):
                main()

        mock_uvicorn.run.assert_called_once_with(
            "robotocore.gateway.app:app",
            host="127.0.0.1",
            port=4566,
            reload=False,
            log_level="info",
        )

    @patch("robotocore.main.uvicorn")
    def test_custom_host(self, mock_uvicorn):
        with patch.dict(
            os.environ,
            {"ROBOTOCORE_HOST": "0.0.0.0"},
            clear=False,
        ):
            # Clear other vars
            os.environ.pop("ROBOTOCORE_PORT", None)
            os.environ.pop("ROBOTOCORE_DEBUG", None)
            main()

        call_kwargs = mock_uvicorn.run.call_args
        assert call_kwargs[1]["host"] == "0.0.0.0" or call_kwargs[0][0] is not None
        # Check positional/keyword args
        args, kwargs = call_kwargs
        if kwargs:
            assert kwargs.get("host") == "0.0.0.0"
        else:
            assert args[1] == "0.0.0.0"

    @patch("robotocore.main.uvicorn")
    def test_custom_port(self, mock_uvicorn):
        with patch.dict(os.environ, {"ROBOTOCORE_PORT": "5000"}, clear=False):
            os.environ.pop("ROBOTOCORE_HOST", None)
            os.environ.pop("ROBOTOCORE_DEBUG", None)
            main()

        mock_uvicorn.run.assert_called_once()
        _, kwargs = mock_uvicorn.run.call_args
        assert kwargs["port"] == 5000

    @patch("robotocore.main.uvicorn")
    def test_debug_mode_enabled(self, mock_uvicorn):
        with patch.dict(
            os.environ,
            {"ROBOTOCORE_DEBUG": "1"},
            clear=False,
        ):
            os.environ.pop("ROBOTOCORE_HOST", None)
            os.environ.pop("ROBOTOCORE_PORT", None)
            main()

        _, kwargs = mock_uvicorn.run.call_args
        assert kwargs["reload"] is True
        assert kwargs["log_level"] == "debug"

    @patch("robotocore.main.uvicorn")
    def test_debug_mode_disabled_with_zero(self, mock_uvicorn):
        with patch.dict(
            os.environ,
            {"ROBOTOCORE_DEBUG": "0"},
            clear=False,
        ):
            os.environ.pop("ROBOTOCORE_HOST", None)
            os.environ.pop("ROBOTOCORE_PORT", None)
            main()

        _, kwargs = mock_uvicorn.run.call_args
        assert kwargs["reload"] is False
        assert kwargs["log_level"] == "info"

    @patch("robotocore.main.uvicorn")
    def test_debug_mode_disabled_with_other_value(self, mock_uvicorn):
        """Any value other than '1' should not enable debug."""
        with patch.dict(
            os.environ,
            {"ROBOTOCORE_DEBUG": "true"},
            clear=False,
        ):
            os.environ.pop("ROBOTOCORE_HOST", None)
            os.environ.pop("ROBOTOCORE_PORT", None)
            main()

        _, kwargs = mock_uvicorn.run.call_args
        assert kwargs["reload"] is False
        assert kwargs["log_level"] == "info"

    @patch("robotocore.main.uvicorn")
    def test_all_custom_config(self, mock_uvicorn):
        with patch.dict(
            os.environ,
            {
                "ROBOTOCORE_HOST": "192.168.1.1",
                "ROBOTOCORE_PORT": "8080",
                "ROBOTOCORE_DEBUG": "1",
            },
            clear=False,
        ):
            main()

        mock_uvicorn.run.assert_called_once_with(
            "robotocore.gateway.app:app",
            host="192.168.1.1",
            port=8080,
            reload=True,
            log_level="debug",
        )

    @patch("robotocore.main.uvicorn")
    def test_app_module_string(self, mock_uvicorn):
        """The app module path should always be robotocore.gateway.app:app."""
        with patch.dict(os.environ, {}, clear=True):
            env = {k: v for k, v in os.environ.items() if not k.startswith("ROBOTOCORE_")}
            with patch.dict(os.environ, env, clear=True):
                main()

        args, _ = mock_uvicorn.run.call_args
        assert args[0] == "robotocore.gateway.app:app"
