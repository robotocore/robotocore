"""Tests for versioned plugin API: api_version, manifest, dependencies, capabilities."""

from __future__ import annotations

from robotocore.extensions.api_version import (
    CURRENT_API_VERSION,
    DEPRECATED_VERSIONS,
    SUPPORTED_VERSIONS,
    CompatResult,
    PluginAPIVersion,
)
from robotocore.extensions.base import PluginManifest, RobotocorePlugin
from robotocore.extensions.registry import ExtensionRegistry

# ======================================================================
# Version compatibility
# ======================================================================


class TestVersionCompatibility:
    def test_exact_match_current(self):
        result = PluginAPIVersion.check_compatibility(CURRENT_API_VERSION)
        assert result.compatible is True
        assert result.errors == []

    def test_supported_version(self):
        for v in SUPPORTED_VERSIONS:
            result = PluginAPIVersion.check_compatibility(v)
            assert result.compatible is True

    def test_unsupported_version(self):
        result = PluginAPIVersion.check_compatibility("99.99")
        assert result.compatible is False
        assert len(result.errors) == 1
        assert "99.99" in result.errors[0]

    def test_empty_version_string(self):
        result = PluginAPIVersion.check_compatibility("")
        assert result.compatible is False
        assert "did not declare" in result.errors[0]

    def test_deprecated_version_compatible_with_warning(self):
        SUPPORTED_VERSIONS.add("0.9")
        DEPRECATED_VERSIONS["0.9"] = "Migrate to 1.0: use get_capabilities()"
        try:
            result = PluginAPIVersion.check_compatibility("0.9")
            assert result.compatible is True
            assert len(result.warnings) == 1
            assert "deprecated" in result.warnings[0].lower()
            assert "0.9" in result.warnings[0]
        finally:
            SUPPORTED_VERSIONS.discard("0.9")
            DEPRECATED_VERSIONS.pop("0.9", None)

    def test_compat_result_dataclass_defaults(self):
        r = CompatResult(compatible=True)
        assert r.warnings == []
        assert r.errors == []

    def test_migration_guide_empty_when_no_deprecations(self):
        guide = PluginAPIVersion.get_migration_guide()
        assert guide == []

    def test_migration_guide_with_deprecations(self):
        DEPRECATED_VERSIONS["0.8"] = "Old hooks removed"
        try:
            guide = PluginAPIVersion.get_migration_guide()
            assert len(guide) == 1
            assert guide[0]["from_version"] == "0.8"
            assert guide[0]["to_version"] == CURRENT_API_VERSION
            assert guide[0]["message"] == "Old hooks removed"
        finally:
            DEPRECATED_VERSIONS.pop("0.8", None)


# ======================================================================
# PluginManifest validation
# ======================================================================


class TestPluginManifest:
    def test_valid_manifest(self):
        m = PluginManifest(name="test", version="1.0.0", api_version="1.0")
        assert m.validate() == []

    def test_missing_name(self):
        m = PluginManifest(name="", version="1.0.0", api_version="1.0")
        errors = m.validate()
        assert any("name" in e for e in errors)

    def test_missing_version(self):
        m = PluginManifest(name="test", version="", api_version="1.0")
        errors = m.validate()
        assert any("version" in e for e in errors)

    def test_missing_api_version(self):
        m = PluginManifest(name="test", version="1.0.0", api_version="")
        errors = m.validate()
        assert any("api_version" in e for e in errors)

    def test_optional_fields_default(self):
        m = PluginManifest(name="test", version="1.0.0", api_version="1.0")
        assert m.description == ""
        assert m.author == ""
        assert m.capabilities == set()
        assert m.dependencies == []
        assert m.config_schema is None

    def test_optional_fields_set(self):
        m = PluginManifest(
            name="test",
            version="1.0.0",
            api_version="1.0",
            description="A test plugin",
            author="Test Author",
            capabilities={"custom_routes"},
            dependencies=["other-plugin"],
            config_schema={"type": "object"},
        )
        assert m.description == "A test plugin"
        assert m.author == "Test Author"
        assert "custom_routes" in m.capabilities
        assert m.dependencies == ["other-plugin"]

    def test_to_dict(self):
        m = PluginManifest(
            name="test",
            version="1.0.0",
            api_version="1.0",
            capabilities={"b", "a"},
        )
        d = m.to_dict()
        assert d["name"] == "test"
        assert d["capabilities"] == ["a", "b"]  # sorted
        assert "config_schema" not in d  # None is omitted

    def test_to_dict_with_config_schema(self):
        m = PluginManifest(
            name="test",
            version="1.0.0",
            api_version="1.0",
            config_schema={"type": "object"},
        )
        d = m.to_dict()
        assert d["config_schema"] == {"type": "object"}

    def test_bad_capabilities_type(self):
        m = PluginManifest(
            name="test",
            version="1.0.0",
            api_version="1.0",
            capabilities="not_a_set",  # type: ignore[arg-type]
        )
        errors = m.validate()
        assert any("capabilities" in e for e in errors)


# ======================================================================
# Dependency resolution
# ======================================================================


def _make_plugin(name: str, deps: list[str] | None = None) -> RobotocorePlugin:
    """Create a minimal plugin with a name and optional dependencies."""
    p = RobotocorePlugin()
    p.name = name
    p.dependencies = deps or []
    return p


class TestDependencyResolution:
    def test_no_dependencies(self):
        a = _make_plugin("a")
        b = _make_plugin("b")
        ordered, warnings = ExtensionRegistry.resolve_load_order([a, b])
        assert len(ordered) == 2
        assert warnings == []

    def test_linear_dependencies(self):
        a = _make_plugin("a")
        b = _make_plugin("b", deps=["a"])
        c = _make_plugin("c", deps=["b"])
        ordered, warnings = ExtensionRegistry.resolve_load_order([c, b, a])
        names = [p.name for p in ordered]
        assert names.index("a") < names.index("b")
        assert names.index("b") < names.index("c")
        assert warnings == []

    def test_diamond_dependencies(self):
        a = _make_plugin("a")
        b = _make_plugin("b", deps=["a"])
        c = _make_plugin("c", deps=["a"])
        d = _make_plugin("d", deps=["b", "c"])
        ordered, warnings = ExtensionRegistry.resolve_load_order([d, c, b, a])
        names = [p.name for p in ordered]
        assert names.index("a") < names.index("b")
        assert names.index("a") < names.index("c")
        assert names.index("b") < names.index("d")
        assert names.index("c") < names.index("d")
        assert warnings == []

    def test_circular_dependency_detected(self):
        a = _make_plugin("a", deps=["b"])
        b = _make_plugin("b", deps=["a"])
        ordered, warnings = ExtensionRegistry.resolve_load_order([a, b])
        # Should detect cycle and produce warnings
        assert len(warnings) > 0
        assert any("circular" in w[1].lower() or "Circular" in w[1] for w in warnings)

    def test_missing_dependency_warning(self):
        a = _make_plugin("a", deps=["nonexistent"])
        ordered, warnings = ExtensionRegistry.resolve_load_order([a])
        assert len(warnings) == 1
        assert "nonexistent" in warnings[0][1]
        # Plugin is still included despite missing dep
        assert len(ordered) == 1

    def test_empty_plugin_list(self):
        ordered, warnings = ExtensionRegistry.resolve_load_order([])
        assert ordered == []
        assert warnings == []

    def test_self_dependency(self):
        a = _make_plugin("a", deps=["a"])
        ordered, warnings = ExtensionRegistry.resolve_load_order([a])
        # Should detect the self-cycle
        assert len(warnings) > 0


# ======================================================================
# Capability queries
# ======================================================================


class _CapPlugin(RobotocorePlugin):
    def __init__(self, name: str, caps: set[str]):
        self.name = name
        self._caps = caps

    def get_capabilities(self) -> set[str]:
        return self._caps


class TestCapabilityQueries:
    def test_single_capability(self):
        reg = ExtensionRegistry()
        reg.register(_CapPlugin("a", {"custom_routes"}))
        reg.register(_CapPlugin("b", {"state_hooks"}))
        result = reg.plugins_with_capability("custom_routes")
        assert len(result) == 1
        assert result[0].name == "a"

    def test_multiple_plugins_same_capability(self):
        reg = ExtensionRegistry()
        reg.register(_CapPlugin("a", {"custom_routes"}))
        reg.register(_CapPlugin("b", {"custom_routes", "state_hooks"}))
        result = reg.plugins_with_capability("custom_routes")
        assert len(result) == 2

    def test_no_matching_capability(self):
        reg = ExtensionRegistry()
        reg.register(_CapPlugin("a", {"custom_routes"}))
        result = reg.plugins_with_capability("nonexistent")
        assert result == []

    def test_empty_registry(self):
        reg = ExtensionRegistry()
        result = reg.plugins_with_capability("anything")
        assert result == []

    def test_plugin_with_no_capabilities(self):
        reg = ExtensionRegistry()
        p = RobotocorePlugin()
        p.name = "bare"
        reg.register(p)
        result = reg.plugins_with_capability("custom_routes")
        assert result == []


# ======================================================================
# Config validation
# ======================================================================


class _ConfigPlugin(RobotocorePlugin):
    name = "config-test"

    def get_config_schema(self) -> dict | None:
        return {
            "type": "object",
            "required": ["host", "port"],
            "properties": {
                "host": {"type": "string"},
                "port": {"type": "integer"},
                "debug": {"type": "boolean"},
            },
        }


class TestConfigValidation:
    def test_valid_config(self):
        p = _ConfigPlugin()
        errors = p.validate_config({"host": "localhost", "port": 8080})
        assert errors == []

    def test_missing_required_key(self):
        p = _ConfigPlugin()
        errors = p.validate_config({"host": "localhost"})
        assert len(errors) == 1
        assert "port" in errors[0]

    def test_wrong_type(self):
        p = _ConfigPlugin()
        errors = p.validate_config({"host": "localhost", "port": "not-an-int"})
        assert len(errors) == 1
        assert "port" in errors[0]
        assert "integer" in errors[0]

    def test_no_schema(self):
        p = RobotocorePlugin()
        p.name = "no-schema"
        errors = p.validate_config({"any": "thing"})
        assert errors == []

    def test_extra_keys_allowed(self):
        p = _ConfigPlugin()
        errors = p.validate_config({"host": "localhost", "port": 8080, "extra": "ok"})
        assert errors == []

    def test_all_required_missing(self):
        p = _ConfigPlugin()
        errors = p.validate_config({})
        assert len(errors) == 2  # host and port


# ======================================================================
# Registry integration
# ======================================================================


class TestRegistryIntegration:
    def test_version_check_on_register(self):
        reg = ExtensionRegistry()
        p = RobotocorePlugin()
        p.name = "good"
        p.api_version = "1.0"
        reg.register(p)
        assert len(reg.plugins) == 1

    def test_incompatible_version_rejected(self):
        reg = ExtensionRegistry()
        p = RobotocorePlugin()
        p.name = "bad"
        p.api_version = "99.0"
        reg.register(p)
        assert len(reg.plugins) == 0  # rejected
        # But compat result is tracked
        assert "bad" in reg._compat_results
        assert reg._compat_results["bad"].compatible is False

    def test_list_plugins_includes_api_version(self):
        reg = ExtensionRegistry()
        p = _CapPlugin("cap-test", {"custom_routes"})
        p.api_version = "1.0"
        reg.register(p)
        info = reg.list_plugins()
        assert len(info) == 1
        assert info[0]["api_version"] == "1.0"
        assert info[0]["capabilities"] == ["custom_routes"]

    def test_list_plugins_includes_compat(self):
        reg = ExtensionRegistry()
        p = RobotocorePlugin()
        p.name = "compat-check"
        p.api_version = "1.0"
        reg.register(p)
        info = reg.list_plugins()
        assert info[0]["api_compat"]["compatible"] is True

    def test_dependency_graph(self):
        reg = ExtensionRegistry()
        a = _make_plugin("a")
        b = _make_plugin("b", deps=["a"])
        reg.register(a)
        reg.register(b)
        graph = reg.get_dependency_graph()
        assert graph == {"a": [], "b": ["a"]}

    def test_unregister_cleans_compat(self):
        reg = ExtensionRegistry()
        p = RobotocorePlugin()
        p.name = "ephemeral"
        reg.register(p)
        assert "ephemeral" in reg._compat_results
        reg.unregister("ephemeral")
        assert "ephemeral" not in reg._compat_results


# ======================================================================
# Plugin base class new methods
# ======================================================================


class TestPluginBaseMethods:
    def test_get_capabilities_default(self):
        p = RobotocorePlugin()
        assert p.get_capabilities() == set()

    def test_get_config_schema_default(self):
        p = RobotocorePlugin()
        assert p.get_config_schema() is None

    def test_get_manifest(self):
        p = _CapPlugin("manifest-test", {"state_hooks"})
        p.version = "2.0.0"
        p.api_version = "1.0"
        p.description = "Test"
        m = p.get_manifest()
        assert m.name == "manifest-test"
        assert m.version == "2.0.0"
        assert m.api_version == "1.0"
        assert "state_hooks" in m.capabilities

    def test_on_api_version_change_noop(self):
        p = RobotocorePlugin()
        # Should not raise
        p.on_api_version_change("0.9", "1.0")

    def test_validate_config_boolean_type(self):
        p = _ConfigPlugin()
        errors = p.validate_config({"host": "h", "port": 1, "debug": "yes"})
        assert len(errors) == 1
        assert "debug" in errors[0]

    def test_default_api_version(self):
        p = RobotocorePlugin()
        assert p.api_version == "1.0"
