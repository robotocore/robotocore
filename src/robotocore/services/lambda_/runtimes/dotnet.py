""".NET runtime executor — runs handler via dotnet subprocess.

.NET Lambda handlers are typically compiled assemblies. The handler format is:
    "Assembly::Namespace.Class::Method"

This executor looks for the assembly DLL in the extracted code and runs it
with the dotnet CLI.
"""

import os
import shutil

from robotocore.services.lambda_.runtimes.base import (
    build_env,
    extract_code,
    run_subprocess,
)

# .NET bootstrap: a small C# script that loads the assembly and invokes the handler.
# For simplicity, we look for a compiled assembly and use `dotnet exec` to run it.
# Real .NET Lambda uses a custom runtime bootstrap, but for emulation we use a simpler approach.

BOOTSTRAP_TEMPLATE = """\
using System;
using System.IO;
using System.Reflection;
using System.Text.Json;

class Bootstrap {{
    static int Main(string[] args) {{
        string handler = Environment.GetEnvironmentVariable("_HANDLER") ?? "";
        string[] parts = handler.Split("::");
        if (parts.Length < 3) {{
            Console.Write(JsonSerializer.Serialize(new {{
                errorMessage = $"Bad handler format: {{handler}}. Expected Assembly::Type::Method",
                errorType = "Runtime.HandlerNotFound"
            }}));
            return 1;
        }}

        string assemblyName = parts[0];
        string typeName = parts[1];
        string methodName = parts[2];

        string eventJson = Console.In.ReadToEnd();

        try {{
            Assembly assembly = Assembly.LoadFrom(assemblyName + ".dll");
            Type type = assembly.GetType(typeName);
            if (type == null) {{
                Console.Write(JsonSerializer.Serialize(new {{
                    errorMessage = $"Type '{{typeName}}' not found in assembly '{{assemblyName}}'",
                    errorType = "Runtime.ImportModuleError"
                }}));
                return 1;
            }}

            MethodInfo method = type.GetMethod(methodName);
            if (method == null) {{
                Console.Write(JsonSerializer.Serialize(new {{
                    errorMessage = $"Method '{{methodName}}' not found in '{{typeName}}'",
                    errorType = "Runtime.HandlerNotFound"
                }}));
                return 1;
            }}

            object instance = Activator.CreateInstance(type);
            object result = method.Invoke(instance, new object[] {{ eventJson, null }});
            Console.Write(result?.ToString() ?? "null");
            return 0;
        }} catch (TargetInvocationException e) {{
            Exception cause = e.InnerException ?? e;
            Console.Error.WriteLine(cause.ToString());
            Console.Write(JsonSerializer.Serialize(new {{
                errorMessage = cause.Message,
                errorType = cause.GetType().Name
            }}));
            return 1;
        }} catch (Exception e) {{
            Console.Error.WriteLine(e.ToString());
            Console.Write(JsonSerializer.Serialize(new {{
                errorMessage = e.Message,
                errorType = e.GetType().Name
            }}));
            return 1;
        }}
    }}
}}
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
            return (
                None,
                "Runtime.ImportModuleError",
                f"Assembly '{assembly_name}.dll' not found",
            )

        # Write bootstrap C# file
        bootstrap_path = os.path.join(tmpdir, "Bootstrap.cs")
        with open(bootstrap_path, "w") as f:
            f.write(BOOTSTRAP_TEMPLATE)

        # Use dotnet-script or compile and run
        # Simpler approach: use `dotnet exec` on the DLL directly if it has an entry point,
        # or compile our bootstrap
        cmd = [dotnet_bin, "script", bootstrap_path]

        # If dotnet-script isn't available, fall back to dotnet exec on the DLL
        # For compiled Lambda packages, they include a runtimeconfig.json
        runtimeconfig = os.path.join(tmpdir, assembly_name + ".runtimeconfig.json")
        if os.path.exists(runtimeconfig):
            # The DLL is a complete app — run it directly
            # Pass the event via stdin and hope it reads it
            cmd = [dotnet_bin, "exec", dll_path]

        return run_subprocess(cmd, event, tmpdir, env, timeout)
