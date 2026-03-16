""".NET runtime executor -- runs handler via dotnet subprocess.

.NET Lambda handlers are typically compiled assemblies. The handler format is:
    "Assembly::Namespace.Class::Method"

This executor supports two modes:
1. Pre-compiled: zip contains Assembly.dll + Assembly.runtimeconfig.json -> dotnet exec
2. Source compilation: zip contains .cs files -> compile with dotnet build, then run

The bootstrap is a small console app that loads the handler assembly via reflection,
invokes the specified method with the event JSON from stdin, and writes the result
to stdout.
"""

import logging
import os
import re
import shutil
import subprocess
import tempfile

from robotocore.services.lambda_.runtimes.base import (
    build_env,
    extract_code,
    run_subprocess,
)

logger = logging.getLogger(__name__)

# Cache the detected target framework moniker
_cached_tfm: str | None = None


def _detect_tfm() -> str:
    """Detect the best available .NET target framework moniker (e.g., 'net9.0').

    Inspects installed runtimes via `dotnet --list-runtimes` and picks the latest
    Microsoft.NETCore.App version available. Falls back to 'net8.0' if detection fails.
    """
    global _cached_tfm
    if _cached_tfm is not None:
        return _cached_tfm

    try:
        proc = subprocess.run(
            ["dotnet", "--list-runtimes"],
            capture_output=True,
            text=True,
            timeout=10,
        )
        if proc.returncode == 0:
            # Parse lines like: Microsoft.NETCore.App 9.0.12 [/path]
            versions = []
            for line in proc.stdout.splitlines():
                if "Microsoft.NETCore.App" in line:
                    m = re.search(r"(\d+)\.\d+\.\d+", line)
                    if m:
                        versions.append(int(m.group(1)))
            if versions:
                major = max(versions)
                _cached_tfm = f"net{major}.0"
                logger.debug("Detected .NET TFM: %s", _cached_tfm)
                return _cached_tfm
    except Exception as exc:
        logger.debug("_detect_tfm: run failed (non-fatal): %s", exc)

    _cached_tfm = "net8.0"
    return _cached_tfm


# Project file template for the bootstrap console app.
# References the user's assembly DLL and System.Text.Json for serialization.
BOOTSTRAP_CSPROJ = """\
<Project Sdk="Microsoft.NET.Sdk">
  <PropertyGroup>
    <OutputType>Exe</OutputType>
    <TargetFramework>{tfm}</TargetFramework>
    <ImplicitUsings>enable</ImplicitUsings>
  </PropertyGroup>
  <ItemGroup>
    <Reference Include="{assembly_name}">
      <HintPath>{dll_path}</HintPath>
    </Reference>
  </ItemGroup>
</Project>
"""

BOOTSTRAP_CS = r"""using System;
using System.IO;
using System.Reflection;
using System.Text.Json;

class Bootstrap {
    static int Main(string[] args) {
        string handler = Environment.GetEnvironmentVariable("_HANDLER") ?? "";
        string[] parts = handler.Split("::");
        if (parts.Length < 3) {
            Console.Write(JsonSerializer.Serialize(new {
                errorMessage = "Bad handler format: " + handler,
                errorType = "Runtime.HandlerNotFound"
            }));
            return 1;
        }

        string assemblyName = parts[0];
        string typeName = parts[1];
        string methodName = parts[2];

        string eventJson = Console.In.ReadToEnd();

        try {
            // Try loading from the working directory first
            string dllPath = Path.Combine(Directory.GetCurrentDirectory(), assemblyName + ".dll");
            if (!File.Exists(dllPath)) {
                // Try parent directory (bootstrap runs from its own build dir)
                string parentDir = Environment.GetEnvironmentVariable("LAMBDA_CODE_DIR") ?? "";
                if (!string.IsNullOrEmpty(parentDir))
                    dllPath = Path.Combine(parentDir, assemblyName + ".dll");
            }

            Assembly assembly = Assembly.LoadFrom(dllPath);
            Type type = assembly.GetType(typeName);
            if (type == null) {
                Console.Write(JsonSerializer.Serialize(new {
                    errorMessage = "Type not found: " + typeName,
                    errorType = "Runtime.ImportModuleError"
                }));
                return 1;
            }

            MethodInfo method = type.GetMethod(methodName);
            if (method == null) {
                Console.Write(JsonSerializer.Serialize(new {
                    errorMessage = "Method '" + methodName + "' not found in '" + typeName + "'",
                    errorType = "Runtime.HandlerNotFound"
                }));
                return 1;
            }

            object instance = Activator.CreateInstance(type);
            ParameterInfo[] parameters = method.GetParameters();

            object result;
            if (parameters.Length == 0) {
                result = method.Invoke(instance, null);
            } else if (parameters.Length == 1) {
                result = method.Invoke(instance, new object[] { eventJson });
            } else {
                result = method.Invoke(instance, new object[] { eventJson, null });
            }
            Console.Write(result?.ToString() ?? "null");
            return 0;
        } catch (TargetInvocationException e) {
            Exception cause = e.InnerException ?? e;
            Console.Error.WriteLine(cause.ToString());
            Console.Write(JsonSerializer.Serialize(new {
                errorMessage = cause.Message,
                errorType = cause.GetType().Name
            }));
            return 1;
        } catch (Exception e) {
            Console.Error.WriteLine(e.ToString());
            Console.Write(JsonSerializer.Serialize(new {
                errorMessage = e.Message,
                errorType = e.GetType().Name
            }));
            return 1;
        }
    }
}
"""

# Minimal csproj for compiling user source files into a class library
USER_LIB_CSPROJ = """\
<Project Sdk="Microsoft.NET.Sdk">
  <PropertyGroup>
    <TargetFramework>{tfm}</TargetFramework>
    <ImplicitUsings>enable</ImplicitUsings>
    <AssemblyName>{assembly_name}</AssemblyName>
  </PropertyGroup>
</Project>
"""


class DotnetExecutor:
    def execute(
        self,
        code_zip: bytes,
        handler: str,
        event: dict,
        function_name: str,
        timeout: int = 3,
        memory_size: int = 128,
        env_vars: dict | None = None,
        region: str = "us-east-1",
        account_id: str = "123456789012",
        layer_zips: list[bytes] | None = None,
        code_dir: str | None = None,
        hot_reload: bool = False,
    ) -> tuple[dict | str | list | None, str | None, str]:
        dotnet_bin = shutil.which("dotnet")
        if not dotnet_bin:
            return None, "Runtime.InvalidRuntime", ".NET SDK not installed"

        tmpdir = extract_code(code_zip, layer_zips, code_dir=code_dir, function_name=function_name)
        env = build_env(function_name, region, account_id, timeout, memory_size, handler, env_vars)

        # Parse handler: "Assembly::Type::Method"
        parts = handler.split("::")
        if len(parts) < 3:
            return (
                None,
                "Runtime.HandlerNotFound",
                f"Bad .NET handler format: {handler}. Expected Assembly::Type::Method",
            )

        assembly_name = parts[0]

        # Look for the assembly DLL
        dll_path = os.path.join(tmpdir, assembly_name + ".dll")
        if not os.path.exists(dll_path):
            # Try in publish/ subdirectory
            dll_path = os.path.join(tmpdir, "publish", assembly_name + ".dll")

        if not os.path.exists(dll_path):
            # No compiled DLL -- check for .cs source files to compile
            cs_files = [f for f in os.listdir(tmpdir) if f.endswith(".cs")]
            if not cs_files:
                return (
                    None,
                    "Runtime.ImportModuleError",
                    f"Assembly '{assembly_name}.dll' not found and no .cs source files",
                )
            dll_path = self._compile_source(dotnet_bin, tmpdir, assembly_name, env, timeout)
            if dll_path is None:
                return (
                    None,
                    "Runtime.ImportModuleError",
                    f"Failed to compile .cs source files into '{assembly_name}.dll'",
                )

        # Check for self-contained app (has runtimeconfig.json)
        rc_name = assembly_name + ".runtimeconfig.json"
        runtimeconfig = os.path.join(tmpdir, rc_name)
        if not os.path.exists(runtimeconfig):
            runtimeconfig = os.path.join(os.path.dirname(dll_path), rc_name)

        if os.path.exists(runtimeconfig):
            # Self-contained app: run directly
            cmd = [dotnet_bin, "exec", dll_path]
            return run_subprocess(cmd, event, tmpdir, env, timeout)

        # Class library: use bootstrap to load via reflection
        return self._run_with_bootstrap(
            dotnet_bin,
            tmpdir,
            dll_path,
            assembly_name,
            event,
            env,
            timeout,
        )

    def _compile_source(
        self,
        dotnet_bin: str,
        code_dir: str,
        assembly_name: str,
        env: dict,
        timeout: int,
    ) -> str | None:
        """Compile .cs source files into a class library DLL. Returns DLL path or None."""
        # Create a temporary project for compilation
        tfm = _detect_tfm()
        proj_content = USER_LIB_CSPROJ.format(assembly_name=assembly_name, tfm=tfm)
        proj_path = os.path.join(code_dir, f"{assembly_name}.csproj")
        with open(proj_path, "w") as f:
            f.write(proj_content)

        # Build a clean env for dotnet that inherits system essentials.
        # The Lambda env may override HOME/DOTNET_ROOT etc. in ways that
        # break dotnet CLI tooling, so we merge carefully.
        compile_env = os.environ.copy()
        # Suppress .NET CLI telemetry and first-run experience which can
        # hang or write to unexpected locations in CI.
        compile_env["DOTNET_CLI_TELEMETRY_OPTOUT"] = "1"
        compile_env["DOTNET_NOLOGO"] = "1"
        compile_env["DOTNET_SKIP_FIRST_TIME_EXPERIENCE"] = "1"

        try:
            proc = subprocess.run(
                [dotnet_bin, "build", "-c", "Release", "-o", code_dir, "--nologo", "-v", "quiet"],
                capture_output=True,
                text=True,
                timeout=timeout + 30,  # generous timeout for compilation
                cwd=code_dir,
                env=compile_env,
            )
            if proc.returncode != 0:
                logger.warning(
                    "dotnet build failed (rc=%d): stderr=%s stdout=%s",
                    proc.returncode,
                    proc.stderr[:500],
                    proc.stdout[:500],
                )
                return None
        except subprocess.TimeoutExpired:
            logger.warning("dotnet build timed out after %ds", timeout + 30)
            return None
        except FileNotFoundError:
            logger.warning("dotnet binary not found at %s", dotnet_bin)
            return None

        dll_path = os.path.join(code_dir, assembly_name + ".dll")
        return dll_path if os.path.exists(dll_path) else None

    def _run_with_bootstrap(
        self,
        dotnet_bin: str,
        code_dir: str,
        dll_path: str,
        assembly_name: str,
        event: dict,
        env: dict,
        timeout: int,
    ) -> tuple[dict | str | list | None, str | None, str]:
        """Build and run the bootstrap app that loads the handler via reflection."""
        bootstrap_dir = tempfile.mkdtemp(prefix="dotnet_bootstrap_")
        try:
            # Write bootstrap project
            tfm = _detect_tfm()
            csproj = BOOTSTRAP_CSPROJ.format(
                assembly_name=assembly_name,
                dll_path=os.path.abspath(dll_path),
                tfm=tfm,
            )
            with open(os.path.join(bootstrap_dir, "Bootstrap.csproj"), "w") as f:
                f.write(csproj)

            with open(os.path.join(bootstrap_dir, "Bootstrap.cs"), "w") as f:
                f.write(BOOTSTRAP_CS)

            # Build the bootstrap using a clean env (not the Lambda env)
            # to avoid issues with DOTNET_ROOT, HOME, etc.
            compile_env = os.environ.copy()
            compile_env["DOTNET_CLI_TELEMETRY_OPTOUT"] = "1"
            compile_env["DOTNET_NOLOGO"] = "1"
            compile_env["DOTNET_SKIP_FIRST_TIME_EXPERIENCE"] = "1"
            try:
                build_proc = subprocess.run(
                    [
                        dotnet_bin,
                        "build",
                        "-c",
                        "Release",
                        "-o",
                        os.path.join(bootstrap_dir, "out"),
                        "--nologo",
                        "-v",
                        "quiet",
                    ],
                    capture_output=True,
                    text=True,
                    timeout=timeout + 30,
                    cwd=bootstrap_dir,
                    env=compile_env,
                )
                if build_proc.returncode != 0:
                    return (
                        {
                            "errorMessage": build_proc.stderr or build_proc.stdout,
                            "errorType": "Runtime.CompilationError",
                        },
                        "Unhandled",
                        build_proc.stderr,
                    )
            except subprocess.TimeoutExpired:
                return None, "Task.TimedOut", "Bootstrap compilation timed out"

            # Copy the handler DLL to the bootstrap output dir so it can be found
            out_dir = os.path.join(bootstrap_dir, "out")
            target_dll = os.path.join(out_dir, os.path.basename(dll_path))
            if not os.path.exists(target_dll):
                shutil.copy2(dll_path, target_dll)
            # Also copy any deps.json if present
            deps_file = dll_path.replace(".dll", ".deps.json")
            if os.path.exists(deps_file):
                shutil.copy2(deps_file, os.path.join(out_dir, os.path.basename(deps_file)))

            bootstrap_dll = os.path.join(out_dir, "Bootstrap.dll")
            if not os.path.exists(bootstrap_dll):
                return (
                    None,
                    "Runtime.CompilationError",
                    "Bootstrap compilation produced no output",
                )

            # Set LAMBDA_CODE_DIR so bootstrap can find the assembly
            env["LAMBDA_CODE_DIR"] = code_dir

            cmd = [dotnet_bin, "exec", bootstrap_dll]
            return run_subprocess(cmd, event, out_dir, env, timeout)
        finally:
            shutil.rmtree(bootstrap_dir, ignore_errors=True)
