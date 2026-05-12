"""Tests for the .NET runtime executor."""

import shutil
from unittest.mock import MagicMock, patch

import pytest

from robotocore.services.lambda_.runtimes.dotnet import (
    DotnetExecutor,
    _dotnet_compile_env,
)
from tests.unit.services.lambda_.helpers import make_zip

_DOTNET_AVAILABLE = shutil.which("dotnet") is not None


class TestDotnetExecutor:
    pytestmark = pytest.mark.skipif(not _DOTNET_AVAILABLE, reason=".NET SDK not installed")

    def setup_method(self):
        self.executor = DotnetExecutor()

    def test_missing_assembly(self):
        code_zip = make_zip({"readme.txt": "nothing useful"})
        result, error_type, logs = self.executor.execute(
            code_zip=code_zip,
            handler="MyAssembly::MyNamespace.MyClass::MyMethod",
            event={},
            function_name="fn",
            timeout=10,
        )
        assert error_type == "Runtime.ImportModuleError"
        assert "MyAssembly.dll" in logs

    def test_bad_handler_format(self):
        code_zip = make_zip({"dummy.txt": ""})
        result, error_type, logs = self.executor.execute(
            code_zip=code_zip,
            handler="bad-format",
            event={},
            function_name="fn",
            timeout=10,
        )
        assert error_type == "Runtime.HandlerNotFound"
        assert "Assembly::Type::Method" in logs


class TestDotnetCompileEnv:
    """Verify that all dotnet subprocess calls include DOTNET_SYSTEM_GLOBALIZATION_INVARIANT.

    These tests use mocks and do not require .NET to be installed. They exist to
    catch regressions where the env var is dropped from subprocess calls, which
    causes dotnet to crash on slim Debian images that don't ship libicu.
    """

    def test_dotnet_compile_env_includes_globalization_invariant(self):
        env = _dotnet_compile_env()
        assert env.get("DOTNET_SYSTEM_GLOBALIZATION_INVARIANT") == "1"

    def test_dotnet_compile_env_includes_standard_suppressions(self):
        env = _dotnet_compile_env()
        assert env["DOTNET_CLI_TELEMETRY_OPTOUT"] == "1"
        assert env["DOTNET_NOLOGO"] == "1"
        assert env["DOTNET_SKIP_FIRST_TIME_EXPERIENCE"] == "1"

    def test_detect_tfm_passes_invariant_env_to_subprocess(self):
        import robotocore.services.lambda_.runtimes.dotnet as dotnet_mod

        dotnet_mod._cached_tfm = None  # reset module-level cache
        mock_proc = MagicMock()
        mock_proc.returncode = 0
        mock_proc.stdout = "Microsoft.NETCore.App 8.0.1 [/usr/share/dotnet]\n"

        with patch("subprocess.run", return_value=mock_proc) as mock_run:
            dotnet_mod._detect_tfm()

        passed_env = mock_run.call_args.kwargs.get("env") or mock_run.call_args[1].get("env")
        assert passed_env is not None, "_detect_tfm did not pass env= to subprocess.run"
        assert passed_env.get("DOTNET_SYSTEM_GLOBALIZATION_INVARIANT") == "1", (
            "_detect_tfm subprocess call is missing DOTNET_SYSTEM_GLOBALIZATION_INVARIANT; "
            "dotnet --list-runtimes will crash on slim images without libicu"
        )

    def test_executor_sets_invariant_mode_on_lambda_execution_env(self):
        """DotnetExecutor.execute() must inject the invariant flag into the Lambda env
        so dotnet exec doesn't crash on slim images, even when user env_vars don't include it."""
        executor = DotnetExecutor()
        code_zip = make_zip({"Handler.dll": b"\x00" * 16})  # fake DLL

        captured_envs: list[dict] = []

        def fake_run(cmd, **kwargs):
            captured_envs.append(dict(kwargs.get("env") or {}))
            proc = MagicMock()
            proc.returncode = 0
            proc.stdout = '{"ok": true}'
            proc.stderr = ""
            return proc

        with (
            patch("shutil.which", return_value="/usr/bin/dotnet"),
            patch("subprocess.run", side_effect=fake_run),
            patch(
                "robotocore.services.lambda_.runtimes.dotnet.extract_code",
                return_value="/tmp/fake",
            ),
            patch("os.path.exists", return_value=True),
            patch("os.listdir", return_value=["Handler.dll"]),
        ):
            executor.execute(
                code_zip=code_zip,
                handler="Handler::Handler::HandleRequest",
                event={},
                function_name="fn",
                timeout=3,
                env_vars={},
            )

        assert captured_envs, "No subprocess.run calls were captured"
        for env in captured_envs:
            assert env.get("DOTNET_SYSTEM_GLOBALIZATION_INVARIANT") == "1", (
                f"A subprocess call is missing DOTNET_SYSTEM_GLOBALIZATION_INVARIANT: {env}"
            )
