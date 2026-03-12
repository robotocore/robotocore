---
session: "a09ac01b"
timestamp: "2026-03-12T06:00:53Z"
model: claude-opus-4-6
---

## Human

Write comprehensive compat tests for .NET Lambda execution. Create Lambda functions
with C# code, invoke them via the server on port 4566, and assert results. Fix
the DotnetExecutor if needed. Cover: basic handler, JSON return, event processing,
env vars, error handling, multiple invocations, handler not found, timeout, and
built-in .NET classes.

## Assistant

## Key decisions

**DotnetExecutor rewrite**: The existing executor relied on `dotnet script` (a
third-party tool not installed by default). Rewrote to use `dotnet build` with
a temporary bootstrap console app project. Three execution modes:

1. Pre-compiled DLL with runtimeconfig.json -> `dotnet exec` directly
2. Pre-compiled DLL (class library) -> bootstrap project loads via reflection
3. Raw .cs source files -> compile to class library first, then bootstrap

**Bootstrap.cs used raw string (r-string)**: The original template used Python
`{{` format escaping but was never passed through `.format()`, so literal `{{`
ended up in the .cs file causing 57 compilation errors. Switched to `r"""..."""`
with plain C# braces.

**Runtime detection**: Instead of hardcoding `net8.0` as target framework (which
would fail if only .NET 9/10 runtimes are installed), added `_detect_tfm()` that
inspects `dotnet --list-runtimes` and picks the highest available major version.

**15 tests, 100% effective rate**: All tests compile real C# code, deploy to
the server, invoke via boto3, and assert on response payloads. Covers string
return, JSON serialization, event parsing, environment variables, exceptions,
bad handler/type names, invocation isolation, System.Text/Crypto/DateTime, timeout,
and server-side source compilation.
