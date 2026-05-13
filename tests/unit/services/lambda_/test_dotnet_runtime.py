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

    def test_detect_tfm_returns_host_max_even_when_runtime_specified(self):
        # The bootstrap TFM follows the host's max installed major, never the
        # requested runtime — building below the user DLL's TFM produces a
        # silent "Type not found" at handler load. See _detect_tfm() docstring.
        from robotocore.services.lambda_.runtimes import dotnet as dotnet_mod

        # net8 and net9 both installed; runtime=dotnet8 must still pick net9.0
        # because the user DLL is almost certainly compiled at host max.
        with patch.object(dotnet_mod, "_installed_majors", {8, 9}):
            with patch.object(dotnet_mod, "_cached_tfm", None):
                tfm = dotnet_mod._detect_tfm("dotnet8")
        assert tfm == "net9.0"

    def test_detect_tfm_warns_when_requested_below_host_max(self):
        # On a host with {6, 8, 9}, requesting dotnet6 still builds at net9.0
        # because we can't go lower than host max without breaking the
        # cross-TFM reference. The user must see a warning about this
        # divergence — otherwise the runtime mismatch is silent.
        from robotocore.services.lambda_.runtimes import dotnet as dotnet_mod

        with patch.object(dotnet_mod, "_installed_majors", {6, 8, 9}):
            with patch.object(dotnet_mod, "_cached_tfm", None):
                with patch.object(dotnet_mod.logger, "warning") as mock_warn:
                    tfm = dotnet_mod._detect_tfm("dotnet6")
        assert tfm == "net9.0"
        mock_warn.assert_called_once()
        # The warning should name the requested runtime and the actual TFM.
        assert any("dotnet6" in str(arg) for arg in mock_warn.call_args.args)

    def test_detect_tfm_falls_back_when_requested_missing(self):
        from robotocore.services.lambda_.runtimes import dotnet as dotnet_mod

        # Only net9 installed; requesting dotnet6 still gives net9.0 and warns.
        with patch.object(dotnet_mod, "_installed_majors", {9}):
            with patch.object(dotnet_mod, "_cached_tfm", None):
                with patch.object(dotnet_mod.logger, "warning") as mock_warn:
                    tfm = dotnet_mod._detect_tfm("dotnet6")
        assert tfm == "net9.0"
        # The divergence warning fired.
        assert mock_warn.called

    def test_detect_tfm_with_matching_runtime_no_warning(self):
        # Requested runtime equals host max → no warning.
        from robotocore.services.lambda_.runtimes import dotnet as dotnet_mod

        with patch.object(dotnet_mod, "_installed_majors", {8}):
            with patch.object(dotnet_mod, "_cached_tfm", None):
                with patch.object(dotnet_mod.logger, "warning") as mock_warn:
                    tfm = dotnet_mod._detect_tfm("dotnet8")
        assert tfm == "net8.0"
        mock_warn.assert_not_called()

    def test_detect_tfm_with_runtime_equal_to_host_max_no_warning(self):
        # dotnet9 requested on {6, 8, 9} — requested IS the max, so no warning.
        from robotocore.services.lambda_.runtimes import dotnet as dotnet_mod

        with patch.object(dotnet_mod, "_installed_majors", {6, 8, 9}):
            with patch.object(dotnet_mod, "_cached_tfm", None):
                with patch.object(dotnet_mod.logger, "warning") as mock_warn:
                    tfm = dotnet_mod._detect_tfm("dotnet9")
        assert tfm == "net9.0"
        mock_warn.assert_not_called()
