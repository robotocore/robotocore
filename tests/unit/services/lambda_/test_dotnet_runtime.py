"""Tests for the .NET runtime executor."""

import shutil
from unittest.mock import patch

import pytest

from robotocore.services.lambda_.runtimes import clear_executor_cache, get_executor_for_runtime
from robotocore.services.lambda_.runtimes.dotnet import (
    _RUNTIME_BINARY,
    DotnetExecutor,
)
from tests.unit.services.lambda_.helpers import make_zip

pytestmark = pytest.mark.skipif(shutil.which("dotnet") is None, reason=".NET SDK not installed")


class TestDotnetExecutor:
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


class TestDotnetVersionRouting:
    """Verify each Lambda dotnet runtime resolves to the correct TFM."""

    def test_runtime_binary_map_covers_known_versions(self):
        assert "dotnet6" in _RUNTIME_BINARY
        assert "dotnet8" in _RUNTIME_BINARY
        assert "dotnet9" in _RUNTIME_BINARY

    def test_get_executor_for_runtime_returns_versioned_instance(self):
        clear_executor_cache()
        e6 = get_executor_for_runtime("dotnet6")
        e8 = get_executor_for_runtime("dotnet8")
        e9 = get_executor_for_runtime("dotnet9")
        assert isinstance(e6, DotnetExecutor)
        assert e6 is not e8
        assert e8 is not e9
        assert get_executor_for_runtime("dotnet8") is e8

    def test_executor_records_runtime(self):
        executor = DotnetExecutor(runtime="dotnet8")
        assert executor._runtime == "dotnet8"

    def test_detect_tfm_prefers_matching_runtime_when_sdk_installed(self):
        # With SDKs for net6/net8/net9 all installed, _detect_tfm picks the
        # TFM matching the requested runtime — real per-version dispatch.
        from robotocore.services.lambda_.runtimes import dotnet as dotnet_mod

        with patch.object(dotnet_mod, "_installed_majors", {6, 8, 9}):
            with patch.object(dotnet_mod, "_cached_tfm", None):
                assert dotnet_mod._detect_tfm("dotnet6") == "net6.0"
            with patch.object(dotnet_mod, "_cached_tfm", None):
                assert dotnet_mod._detect_tfm("dotnet8") == "net8.0"
            with patch.object(dotnet_mod, "_cached_tfm", None):
                assert dotnet_mod._detect_tfm("dotnet9") == "net9.0"

    def test_detect_tfm_matching_runtime_does_not_warn(self):
        # SDK installed for the requested runtime → no warning.
        from robotocore.services.lambda_.runtimes import dotnet as dotnet_mod

        with patch.object(dotnet_mod, "_installed_majors", {6, 8, 9}):
            with patch.object(dotnet_mod, "_cached_tfm", None):
                with patch.object(dotnet_mod.logger, "warning") as mock_warn:
                    tfm = dotnet_mod._detect_tfm("dotnet6")
        assert tfm == "net6.0"
        mock_warn.assert_not_called()

    def test_detect_tfm_falls_back_when_requested_missing(self):
        # SDK for dotnet6 not installed → fall back to host max and warn.
        from robotocore.services.lambda_.runtimes import dotnet as dotnet_mod

        with patch.object(dotnet_mod, "_installed_majors", {8, 9}):
            with patch.object(dotnet_mod, "_cached_tfm", None):
                with patch.object(dotnet_mod.logger, "warning") as mock_warn:
                    tfm = dotnet_mod._detect_tfm("dotnet6")
        assert tfm == "net9.0"
        mock_warn.assert_called_once()
        assert any("dotnet6" in str(arg) for arg in mock_warn.call_args.args)

    def test_detect_tfm_no_runtime_arg_returns_host_max(self):
        # Legacy callers (or bootstrap paths without runtime context) get the
        # host's max-installed major.
        from robotocore.services.lambda_.runtimes import dotnet as dotnet_mod

        with patch.object(dotnet_mod, "_installed_majors", {6, 8, 9}):
            with patch.object(dotnet_mod, "_cached_tfm", None):
                assert dotnet_mod._detect_tfm() == "net9.0"
