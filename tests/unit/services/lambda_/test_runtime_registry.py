"""Tests for the Lambda runtime registry and mapping."""

import pytest

from robotocore.services.lambda_.runtimes import (
    clear_executor_cache,
    get_executor_for_runtime,
    runtime_to_family,
)
from robotocore.services.lambda_.runtimes.custom import CustomRuntimeExecutor
from robotocore.services.lambda_.runtimes.dotnet import DotnetExecutor
from robotocore.services.lambda_.runtimes.java import JavaExecutor
from robotocore.services.lambda_.runtimes.node import NodejsExecutor
from robotocore.services.lambda_.runtimes.python import PythonExecutor
from robotocore.services.lambda_.runtimes.ruby import RubyExecutor


class TestRuntimeToFamily:
    @pytest.mark.parametrize(
        "runtime,expected",
        [
            ("python3.8", "python"),
            ("python3.9", "python"),
            ("python3.10", "python"),
            ("python3.11", "python"),
            ("python3.12", "python"),
            ("python3.13", "python"),
        ],
    )
    def test_python_runtimes(self, runtime, expected):
        assert runtime_to_family(runtime) == expected

    @pytest.mark.parametrize(
        "runtime,expected",
        [
            ("nodejs16.x", "nodejs"),
            ("nodejs18.x", "nodejs"),
            ("nodejs20.x", "nodejs"),
            ("nodejs22.x", "nodejs"),
        ],
    )
    def test_nodejs_runtimes(self, runtime, expected):
        assert runtime_to_family(runtime) == expected

    @pytest.mark.parametrize(
        "runtime,expected",
        [
            ("ruby3.2", "ruby"),
            ("ruby3.3", "ruby"),
        ],
    )
    def test_ruby_runtimes(self, runtime, expected):
        assert runtime_to_family(runtime) == expected

    @pytest.mark.parametrize(
        "runtime,expected",
        [
            ("java8", "java"),
            ("java8.al2", "java"),
            ("java11", "java"),
            ("java17", "java"),
            ("java21", "java"),
        ],
    )
    def test_java_runtimes(self, runtime, expected):
        assert runtime_to_family(runtime) == expected

    @pytest.mark.parametrize(
        "runtime,expected",
        [
            ("dotnet6", "dotnet"),
            ("dotnet8", "dotnet"),
        ],
    )
    def test_dotnet_runtimes(self, runtime, expected):
        assert runtime_to_family(runtime) == expected

    @pytest.mark.parametrize(
        "runtime,expected",
        [
            ("provided", "custom"),
            ("provided.al2", "custom"),
            ("provided.al2023", "custom"),
            ("go1.x", "custom"),
        ],
    )
    def test_custom_runtimes(self, runtime, expected):
        assert runtime_to_family(runtime) == expected

    def test_empty_runtime_is_custom(self):
        assert runtime_to_family("") == "custom"

    def test_unknown_runtime_is_custom(self):
        assert runtime_to_family("cobol42") == "custom"


class TestGetExecutorForRuntime:
    def setup_method(self):
        clear_executor_cache()

    def test_python_executor(self):
        executor = get_executor_for_runtime("python3.12")
        assert isinstance(executor, PythonExecutor)

    def test_nodejs_executor(self):
        executor = get_executor_for_runtime("nodejs20.x")
        assert isinstance(executor, NodejsExecutor)

    def test_ruby_executor(self):
        executor = get_executor_for_runtime("ruby3.3")
        assert isinstance(executor, RubyExecutor)

    def test_java_executor(self):
        executor = get_executor_for_runtime("java21")
        assert isinstance(executor, JavaExecutor)

    def test_dotnet_executor(self):
        executor = get_executor_for_runtime("dotnet8")
        assert isinstance(executor, DotnetExecutor)

    def test_custom_executor(self):
        executor = get_executor_for_runtime("provided.al2023")
        assert isinstance(executor, CustomRuntimeExecutor)

    def test_executor_is_cached(self):
        e1 = get_executor_for_runtime("python3.12")
        e2 = get_executor_for_runtime("python3.12")
        assert e1 is e2

    def test_different_versions_same_family_share_executor(self):
        e1 = get_executor_for_runtime("python3.12")
        e2 = get_executor_for_runtime("python3.11")
        assert e1 is e2

    def test_clear_cache(self):
        e1 = get_executor_for_runtime("python3.12")
        clear_executor_cache()
        e2 = get_executor_for_runtime("python3.12")
        assert e1 is not e2
