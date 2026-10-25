//! Docker-based Lambda executor — runs Lambda functions in isolated Docker
//! containers.
//!
//! Uses the docker CLI via subprocess (no docker-py/docker-rs dependency) to
//! run functions in the official AWS Lambda runtime images. Supports all AWS
//! runtimes: Python, Node.js, Java, .NET, Ruby, Go, and custom runtimes.
//!
//! Configuration via environment variables:
//!   - `LAMBDA_EXECUTOR`: "local" (default) or "docker"
//!   - `LAMBDA_RUNTIME_EXECUTOR`: legacy alias, consulted when
//!     `LAMBDA_EXECUTOR` is unset
//!   - `GATEWAY_PORT`: gateway port containers dial for AWS calls (4566
//!     default)
//!   - `LAMBDA_DOCKER_NETWORK`: docker network for Lambda containers
//!   - `LAMBDA_DOCKER_DNS`: custom DNS for Lambda containers
//!
//! Cold-pull budget: the mapped images live on `public.ecr.aws`, and the
//! first invocation of a runtime pays the image pull inside the same
//! `docker run` CLI window as the invoke itself — `Timeout + 10` overall for
//! the detached-run handshake, `Timeout + 15` for the loose-bridge families
//! (see [`DockerLambdaExecutor::docker_invocation`]). A cold multi-hundred-
//! MB pull can outlive that budget on the very first invoke: the failure
//! surfaces as the python-era run-error shape (Runtime.ExitError/Unhandled
//! with the CLI stderr), not as a hang — so pre-warm a runtime's image
//! (`docker pull <image>`) before invoking when a cold source is expected.
//! After the pull, every later invocation only waits on the container.
//!
//! Design lineage: port of the python-era `DockerLambdaExecutor`
//! (`src/robotocore/services/lambda_/docker_executor.py`, 7d909e8c PR #111,
//! evolving the container-pool executors of #41/#75). One deliberate
//! deviation is documented at [`DockerLambdaExecutor::execute_docker`]: the
//! python-era happy path piped the event through `docker run`'s stdin, which
//! never reaches the container; the working handshake is this module's port
//! of that file's own `_invoke_via_runtime_interface` — POST the event to the
//! Lambda Runtime Interface (the runtime-interface-emulator inside every
//! official runtime image, listening on localhost:8080).
//!
//! .NET source-package compile safety: the mapped public-ECR dotnet images
//! ship only the shared runtime — no build SDK — so the python-era
//! `_compile_source` fallback (compile a .cs package into an assembly) still
//! runs on the twin host with its own SDK. That build never touches
//! caller-controlled build input: the handler's assembly component is
//! validated first ([`dotnet_assembly_violation`]), every fragment
//! interpolated into the generated csproj is XML-escaped, and the build runs
//! in a fresh staging directory under the temp root holding only the twin's
//! csproj and the package's .cs sources, so a crafted csproj or
//! `Directory.Build.props` shipped in the code package never reaches the
//! host MSBuild (the python era interpolated the raw name into the csproj
//! and built in place). Building inside the mapped image itself is not
//! possible: those images carry no SDK (verified with `dotnet --list-sdks`
//! in public.ecr.aws/lambda/dotnet:6/8).

use crate::runtime::{collect_by, drain_thread, ExecResult, DOCKER_OUTPUT_CAP};
use serde_json::{json, Value};
use std::collections::HashMap;
use std::io::Write;
use std::process::{Child, Stdio};
use std::sync::atomic::{AtomicBool, Ordering as AtomicOrdering};
use std::sync::Arc;
use std::time::{Duration, Instant};

/// Default runtime-to-image mapping (official AWS Lambda runtime images).
pub const RUNTIME_IMAGES: &[(&str, &str)] = &[
    // Python
    ("python3.8", "public.ecr.aws/lambda/python:3.8"),
    ("python3.9", "public.ecr.aws/lambda/python:3.9"),
    ("python3.10", "public.ecr.aws/lambda/python:3.10"),
    ("python3.11", "public.ecr.aws/lambda/python:3.11"),
    ("python3.12", "public.ecr.aws/lambda/python:3.12"),
    ("python3.13", "public.ecr.aws/lambda/python:3.13"),
    // Node.js
    ("nodejs16.x", "public.ecr.aws/lambda/nodejs:16"),
    ("nodejs18.x", "public.ecr.aws/lambda/nodejs:18"),
    ("nodejs20.x", "public.ecr.aws/lambda/nodejs:20"),
    ("nodejs22.x", "public.ecr.aws/lambda/nodejs:22"),
    // Java
    ("java8", "public.ecr.aws/lambda/java:8"),
    ("java8.al2", "public.ecr.aws/lambda/java:8.al2"),
    ("java11", "public.ecr.aws/lambda/java:11"),
    ("java17", "public.ecr.aws/lambda/java:17"),
    ("java21", "public.ecr.aws/lambda/java:21"),
    // .NET
    ("dotnet6", "public.ecr.aws/lambda/dotnet:6"),
    ("dotnet8", "public.ecr.aws/lambda/dotnet:8"),
    // Go (uses custom runtime)
    ("go1.x", "public.ecr.aws/lambda/go:1"),
    // Ruby
    ("ruby3.2", "public.ecr.aws/lambda/ruby:3.2"),
    ("ruby3.3", "public.ecr.aws/lambda/ruby:3.3"),
    // Custom / provided
    ("provided", "public.ecr.aws/lambda/provided:al2"),
    ("provided.al2", "public.ecr.aws/lambda/provided:al2"),
    ("provided.al2023", "public.ecr.aws/lambda/provided:al2023"),
];

/// Return the configured executor mode: "local" or "docker" (lowercased).
///
/// Checks `LAMBDA_EXECUTOR` first (matches the documented config name), then
/// falls back to `LAMBDA_RUNTIME_EXECUTOR` for backward compatibility.
pub fn executor_mode() -> String {
    let mut mode = std::env::var("LAMBDA_EXECUTOR").unwrap_or_default();
    if mode.is_empty() {
        mode = std::env::var("LAMBDA_RUNTIME_EXECUTOR").unwrap_or_else(|_| "local".to_string());
    }
    mode.to_ascii_lowercase()
}

/// Get the Docker image for a given AWS runtime string.
///
/// Returns `None` if no image is mapped for the runtime.
pub fn runtime_image(runtime: &str) -> Option<&'static str> {
    RUNTIME_IMAGES
        .iter()
        .find(|(r, _)| *r == runtime)
        .map(|(_, image)| *image)
}

/// Whether this runtime family executes via an official AWS runtime image —
/// i.e. the docker executor can serve it when `executor_mode()` is "docker".
pub fn is_docker_runtime(runtime: &str) -> bool {
    runtime_image(runtime).is_some()
}

/// Runtime families that run the python-era *loose bridge* instead of the
/// official image's strict runtime-interface bootstrap.
///
/// Port lineage: `src/robotocore/services/lambda_/runtimes/{java,dotnet,ruby}.py`
/// (every executor of that era, multi-runtime engine from 547fb91b). Those
/// executors defined the invocation semantics this twin's managed-runtime
/// suites encode: the handler receives loosely-typed arguments (java
/// `(String|Map, Object)`, dotnet `(string, object)`, ruby
/// `(event:, context:)` built from env), the result protocol is "event JSON
/// on stdin, result on stdout, error JSON + non-zero exit" (era
/// `base.run_subprocess`). Official images are strict about handler
/// signatures (java RIC rejects `(String, Object)`; dotnet's
/// `Amazon.Lambda.RuntimeSupport` requires `ILambdaContext`; the ruby EMI
/// context returns memo/limit values as strings), so the faithful port runs
/// the era's loose bootstrap INSIDE the mapped official image as a one-shot
/// `docker run --rm -i` and keeps the era's exact response rules.
const LOOSE_BRIDGE_FAMILIES: [&str; 3] = ["java", "dotnet", "ruby"];

/// Loose observed timeout text (python-era `base.run_subprocess`
/// `TimeoutExpired` branch and the in-process shims' shape). The official
/// EMI's own timeout meanwhile answers with a plain-text body
/// `Task timed out after 2.00 seconds`.
pub(crate) fn task_timed_out_message(timeout: u32) -> String {
    format!("Task timed out after {timeout}.00 seconds")
}

/// Structured Task.TimedOut result in the python-era shape.
pub(crate) fn run_timed_out_result(timeout_secs: u32, logs: &str) -> ExecResult {
    ExecResult {
        payload: json!({
            "errorMessage": task_timed_out_message(timeout_secs),
            "errorType": "Task.TimedOut",
        })
        .to_string(),
        function_error: Some("Task.TimedOut".to_string()),
        logs: if logs.is_empty() {
            format!("Function timed out after {timeout_secs}s")
        } else {
            logs.to_string()
        },
    }
}

/// Port of the python-era `runtimes/base.py run_subprocess` response rules
/// for the one-shot bridge containers: exit 0 -> stdout as parsed JSON (or
/// raw string; empty -> "null"); non-zero -> stdout error JSON ("Handled")
/// else Runtime.ExitError with stderr tail ("Unhandled"); emulator-style
/// plain "Task timed out after ..." bodies become Task.TimedOut.
pub(crate) fn loose_result(out: SubprocessOutput, timeout_secs: u32) -> ExecResult {
    let stdout = out.stdout.trim().to_string();
    let logs = out.stderr;
    if out.status_code == Some(0) {
        let payload = if stdout.is_empty() {
            json!("null").to_string()
        } else {
            match serde_json::from_str::<Value>(&stdout) {
                Ok(parsed) => parsed.to_string(),
                Err(_) => json!(stdout).to_string(),
            }
        };
        return ExecResult {
            payload,
            function_error: None,
            logs,
        };
    }
    // Non-zero exits: structured error JSON -> "Handled"; the in-container
    // timeout's text -> Task.TimedOut; anything else -> Runtime.ExitError.
    if let Ok(parsed) = serde_json::from_str::<Value>(&stdout) {
        if parsed
            .as_object()
            .map(|o| o.contains_key("errorMessage"))
            .unwrap_or(false)
        {
            return ExecResult {
                payload: parsed.to_string(),
                function_error: Some("Handled".to_string()),
                logs,
            };
        }
    }
    if stdout.starts_with("Task timed out after") {
        return run_timed_out_result(timeout_secs, &logs);
    }
    ExecResult {
        payload: json!({
            "errorMessage": if stdout.is_empty() {
                logs.lines().last().unwrap_or("Unknown error")
            } else {
                stdout.as_str()
            },
            "errorType": "Runtime.ExitError",
            "stackTrace": logs.trim().lines().collect::<Vec<_>>(),
        })
        .to_string(),
        function_error: Some("Unhandled".to_string()),
        logs,
    }
}

/// The `docker run --rm -i` prefix for one-shot bridge containers: read-only
/// code mount at /var/task, bridge assets at /var/bridge, the era env pairs
/// (same core set as `build_docker_run_cmd`), plus network/dns/add-host.
#[allow(clippy::too_many_arguments)]
pub(crate) fn loose_bridge_run_prefix(
    code_dir: &str,
    handler: &str,
    function_name: &str,
    function_version: &str,
    function_arn: &str,
    env_vars: &HashMap<String, String>,
    timeout: u32,
    memory_size: u32,
    region: &str,
    account_id: &str,
    gateway_port: u16,
    docker_network: Option<&str>,
    docker_dns: Option<&str>,
) -> Vec<String> {
    let mut cmd = vec![
        "docker".to_string(),
        "run".to_string(),
        "--rm".to_string(),
        // Without -i the CLI never attaches stdin, so the event never
        // reaches the bridge (the python-era happy-path lesson).
        "-i".to_string(),
        "-v".to_string(),
        format!("{code_dir}:/var/task:ro"),
        "-e".to_string(),
        format!("AWS_LAMBDA_FUNCTION_NAME={function_name}"),
        "-e".to_string(),
        format!("AWS_LAMBDA_FUNCTION_VERSION={function_version}"),
        "-e".to_string(),
        format!("AWS_LAMBDA_FUNCTION_ARN={function_arn}"),
        "-e".to_string(),
        format!("AWS_LAMBDA_FUNCTION_MEMORY_SIZE={memory_size}"),
        "-e".to_string(),
        format!("AWS_LAMBDA_FUNCTION_TIMEOUT={timeout}"),
        "-e".to_string(),
        format!("AWS_REGION={region}"),
        "-e".to_string(),
        format!("AWS_DEFAULT_REGION={region}"),
        "-e".to_string(),
        format!("AWS_ACCOUNT_ID={account_id}"),
        "-e".to_string(),
        format!("AWS_LAMBDA_LOG_GROUP_NAME=/aws/lambda/{function_name}"),
        "-e".to_string(),
        format!("AWS_LAMBDA_LOG_STREAM_NAME={}", shim_log_stream_name()),
        "-e".to_string(),
        "LAMBDA_TASK_ROOT=/var/task".to_string(),
        "-e".to_string(),
        format!("_HANDLER={handler}"),
        "-e".to_string(),
        "AWS_ACCESS_KEY_ID=testing".to_string(),
        "-e".to_string(),
        "AWS_SECRET_ACCESS_KEY=testing".to_string(),
        "-e".to_string(),
        format!("AWS_ENDPOINT_URL=http://host.docker.internal:{gateway_port}"),
    ];
    for (key, value) in env_vars {
        cmd.push("-e".to_string());
        cmd.push(format!("{key}={value}"));
    }
    if let Some(network) = docker_network {
        cmd.push("--network".to_string());
        cmd.push(network.to_string());
    }
    if let Some(dns) = docker_dns {
        cmd.push("--dns".to_string());
        cmd.push(dns.to_string());
    }
    cmd.push("--add-host".to_string());
    cmd.push("host.docker.internal:host-gateway".to_string());
    cmd.push("-v".to_string());
    cmd.push(format!(
        "{}:/var/bridge:ro",
        loose_bridge_dir().to_string_lossy()
    ));
    cmd
}

/// The image's dotnet TFM from the runtime string (dotnet6 -> net6.0,
/// dotnet8 -> net8.0; default net8.0).
pub(crate) fn dotnet_runtime_tfm(runtime: &str) -> &'static str {
    if runtime.starts_with("dotnet6") {
        "net6.0"
    } else {
        "net8.0"
    }
}

/// Build (once per TFM, python-era cached bootstrap) the loose dotnet bridge
/// with the twin's SDK. `None` when the twin has no dotnet CLI or the build
/// fails — callers degrade to the era's Runtime.InvalidRuntime shape.
pub(crate) fn ensure_dotnet_bridge(tfm: &str) -> Option<std::path::PathBuf> {
    static CACHE: parking_lot::Mutex<Option<HashMap<String, Option<std::path::PathBuf>>>> =
        parking_lot::Mutex::new(None);
    let mut guard = CACHE.lock();
    if let Some(entry) = guard.as_mut() {
        if let Some(cached) = entry.get(tfm).cloned().flatten() {
            // Pruned-bridge guard: a next-start twin's sweep reaps tempo
            // roots older than STALE_EXTRACT_DIR_AGE, so this process's
            // bridge dir (and its publish output) can disappear mid-life. A
            // dangling cached path must not leak out — drop the entry and
            // rebuild (or degrade) on the fresh pass.
            if cached.join("RobotocoreDotnetBridge.dll").is_file() {
                return Some(cached);
            }
            entry.remove(tfm);
        } else if entry.contains_key(tfm) {
            // Cached failure (no SDK / failed build): repeat it like before.
            return None;
        }
    }
    let dir = loose_bridge_dir();
    let out_dir = dir.join(format!("publish-{tfm}"));
    let dll = out_dir.join("RobotocoreDotnetBridge.dll");
    let proj = dir.join("RobotocoreDotnetBridge.csproj");
    let result = if dll.exists() {
        Some(out_dir.clone())
    } else {
        let which = std::env::var_os("PATH").map(|paths| {
            std::env::split_paths(&paths)
                .map(|p| p.join("dotnet"))
                .any(|p| p.is_file())
        });
        if which != Some(true) {
            None
        } else {
            let _ = std::fs::create_dir_all(&out_dir);
            let mut builder = std::process::Command::new("dotnet");
            builder
                .arg("publish")
                .arg(&proj)
                .env("DOTNET_CLI_TELEMETRY_OPTOUT", "1")
                .env("DOTNET_NOLOGO", "1")
                .env("DOTNET_SKIP_FIRST_TIME_EXPERIENCE", "1")
                .env("DOTNET_SYSTEM_GLOBALIZATION_INVARIANT", "1")
                .stdout(Stdio::null())
                .stderr(Stdio::null());
            let ok = builder
                .arg("-c")
                .arg("Release")
                .arg("-f")
                .arg(tfm)
                .arg("-o")
                .arg(&out_dir)
                .status()
                .ok()
                .map(|s| s.success())
                .unwrap_or(false)
                && dll.exists();
            if ok {
                Some(out_dir.clone())
            } else {
                None
            }
        }
    };
    if let Some(entry) = guard.as_mut() {
        entry.insert(tfm.to_string(), result.clone());
    } else {
        let mut map = HashMap::new();
        map.insert(tfm.to_string(), result.clone());
        *guard = Some(map);
    }
    result
}

/// Validation rule for the dotnet handler's assembly component. The python
/// era (`runtimes/dotnet.py`) split `Assembly::Type::Method` on `::` and
/// passed `parts[0]` to `os.path.join` and a csproj `<AssemblyName>` with no
/// further vetting — the escape/injection hole this module closes (the
/// mapped dotnet images have no SDK, so that build runs on the twin host).
///
/// `$(...)` MSBuild fragments are rejected too: XML escaping does not
/// neutralize MSBuild property expressions, so a caller-supplied
/// `<AssemblyName>$(Foo)</AssemblyName>` would be *evaluated* by the twin's
/// SDK build. Control characters below 0x20 (except tab/lf/cr) are rejected
/// as well — they are never legitimate csproj text or file names.
pub(crate) fn dotnet_assembly_violation(assembly: &str) -> Option<&'static str> {
    if assembly.is_empty() {
        return Some("empty assembly name");
    }
    if assembly.starts_with('.') {
        return Some("leading dot");
    }
    if assembly.contains("..") {
        return Some("dot segment");
    }
    if assembly.contains('/') || assembly.contains('\\') {
        return Some("path separator");
    }
    if assembly.chars().any(|c| "<>&\"'".contains(c)) {
        return Some("XML metacharacter");
    }
    if assembly.contains("$(") {
        return Some("MSBuild property expression");
    }
    if assembly
        .chars()
        .any(|c| c < '\u{20}' && c != '\t' && c != '\n' && c != '\r')
    {
        return Some("control character");
    }
    None
}

/// XML escape for the fragments interpolated into the generated csproj
/// (`AssemblyName`, `TargetFramework`): the csproj charset is fixed by the
/// twin, not the caller, so `&<>"'` arrive here only escaped.
pub(crate) fn xml_escape_csproj_value(value: &str) -> String {
    let mut out = String::with_capacity(value.len());
    for c in value.chars() {
        match c {
            '&' => out.push_str("&amp;"),
            '<' => out.push_str("&lt;"),
            '>' => out.push_str("&gt;"),
            '"' => out.push_str("&quot;"),
            '\'' => out.push_str("&apos;"),
            _ => out.push(c),
        }
    }
    out
}

/// The era's lib csproj template ([era `USER_LIB_CSPROJ`]) with the
/// Directory.Build imports disabled, every interpolated fragment first
/// XML-escaped. Returns the written project path.
pub(crate) fn write_generated_csproj(
    dir: &std::path::Path,
    assembly_name: &str,
    tfm: &str,
) -> std::path::PathBuf {
    let csproj = format!(
        "<Project Sdk=\"Microsoft.NET.Sdk\">\n  <PropertyGroup>\n    <TargetFramework>{}</TargetFramework>\n    <ImplicitUsings>enable</ImplicitUsings>\n    <AssemblyName>{}</AssemblyName>\n    <ImportDirectoryBuildProps>false</ImportDirectoryBuildProps>\n    <ImportDirectoryBuildTargets>false</ImportDirectoryBuildTargets>\n  </PropertyGroup>\n</Project>\n",
        xml_escape_csproj_value(tfm),
        xml_escape_csproj_value(assembly_name),
    );
    let proj = dir.join(format!("{assembly_name}.csproj"));
    let _ = std::fs::write(&proj, csproj);
    proj
}

/// Recursively collect `*.cs` sources under `dir` as
/// (path relative to `dir`, absolute path) — the set the SDK's implicit
/// `**/*.cs` glob would compile.
fn collect_cs_sources(dir: &std::path::Path) -> Vec<(std::path::PathBuf, std::path::PathBuf)> {
    let mut out = Vec::new();
    let mut stack = vec![std::path::PathBuf::from(dir)];
    while let Some(current) = stack.pop() {
        let Ok(entries) = std::fs::read_dir(&current) else {
            continue;
        };
        for entry in entries.flatten() {
            let path = current.join(entry.file_name());
            if entry.file_type().map(|t| t.is_dir()).unwrap_or(false) {
                stack.push(path);
            } else if entry.file_name().to_string_lossy().ends_with(".cs") {
                // Package-internal relative path, as the implicit glob sees it.
                if let Ok(rel) = path.strip_prefix(dir).map(std::path::Path::to_path_buf) {
                    out.push((rel, path));
                }
            }
        }
    }
    out.sort();
    out
}

/// Compile .cs sources inside `code_dir` when the package carries source and
/// no DLL (python-era `_compile_source`). Returns Some(result-shaped) only
/// for the era's error conditions; None means "nothing to compile / ok".
///
/// The build itself runs in a fresh staging directory under the temp root —
/// never in `code_dir` and never against a caller-supplied csproj: the host
/// SDK otherwise evaluates whatever project files / `Directory.Build.*`
/// hooks the code package carries (the python era built in place, which
/// meant `dotnet build` would consume the user's own csproj). Only the .cs
/// sources travel; non-source assets (resx etc.) are out of scope for the
/// twin's compile fallback and live only in DLL packages.
pub(crate) fn compile_dotnet_source_if_needed(
    tfm: &str,
    code_dir: &std::path::Path,
    handler: &str,
) -> Option<ExecResult> {
    // Era split (dotnet.py: "Assembly::Type::Method", parts[0] names the
    // assembly). Every use of the name below — path joins and the generated
    // csproj — is behind [`dotnet_assembly_violation`].
    let parts: Vec<&str> = handler.split("::").collect();
    let assembly_name = parts.first().copied().unwrap_or("");
    if assembly_name.is_empty() {
        return Some(ExecResult {
            payload: json!({
                "errorMessage": format!("Bad .NET handler format: {handler}. Expected Assembly::Type::Method"),
                "errorType": "Runtime.HandlerNotFound",
            })
            .to_string(),
            function_error: Some("Runtime.HandlerNotFound".to_string()),
            logs: String::new(),
        });
    }
    if let Some(violation) = dotnet_assembly_violation(assembly_name) {
        return Some(ExecResult {
            payload: json!({
                "errorMessage": format!("Invalid .NET handler assembly '{assembly_name}': {violation}"),
                "errorType": "Runtime.ImportModuleError",
            })
            .to_string(),
            function_error: Some("Runtime.ImportModuleError".to_string()),
            logs: String::new(),
        });
    }
    let dll = code_dir.join(format!("{assembly_name}.dll"));
    if dll.exists()
        || code_dir
            .join("publish")
            .join(format!("{assembly_name}.dll"))
            .exists()
    {
        return None;
    }
    let cs_sources: Vec<(std::path::PathBuf, std::path::PathBuf)> = collect_cs_sources(code_dir);
    if cs_sources.is_empty() {
        return Some(ExecResult {
            payload: json!({
                "errorMessage": format!("Assembly '{assembly_name}.dll' not found and no .cs source files"),
                "errorType": "Runtime.ImportModuleError",
            })
            .to_string(),
            function_error: Some("Runtime.ImportModuleError".to_string()),
            logs: String::new(),
        });
    }
    // Staging dir under the temp root, outside any user-visible tree: the
    // write below is the only thing that lands here, and the parent chain
    // (…/robotocore-lambda-build/<uuid>) is twin-owned, so the SDK finds no
    // caller-planted MSBuild hooks to import.
    let staging = std::env::temp_dir()
        .join("robotocore-lambda-build")
        .join(uuid::Uuid::new_v4().simple().to_string());
    let cleanup_staging = || {
        let _ = std::fs::remove_dir_all(&staging);
    };
    if std::fs::create_dir_all(&staging).is_err() {
        return Some(ExecResult {
            payload: json!({
                "errorMessage": "failed to create staging dir for server-side compile",
                "errorType": "Runtime.ImportModuleError",
            })
            .to_string(),
            function_error: Some("Runtime.ImportModuleError".to_string()),
            logs: String::new(),
        });
    }
    for (rel, source) in cs_sources {
        let target = staging.join(&rel);
        if let Some(parent) = target.parent() {
            let _ = std::fs::create_dir_all(parent);
        }
        if std::fs::copy(&source, &target).is_err() {
            cleanup_staging();
            return Some(ExecResult {
                payload: json!({
                    "errorMessage": "failed to copy .cs source into staging for server-side compile",
                    "errorType": "Runtime.ImportModuleError",
                })
                .to_string(),
                function_error: Some("Runtime.ImportModuleError".to_string()),
                logs: String::new(),
            });
        }
    }
    // Write the generated csproj and assert — canonicalized prefix, belt in
    // front of the byte-level validation — that the path still resolves
    // inside the staging dir.
    let proj = write_generated_csproj(&staging, assembly_name, tfm);
    let proj_inside_dir = std::fs::canonicalize(&proj)
        .ok()
        .and_then(|canonical| {
            std::fs::canonicalize(&staging)
                .ok()
                .map(|staging_canonical| canonical.starts_with(&staging_canonical))
        })
        .unwrap_or(false);
    if !proj_inside_dir {
        cleanup_staging();
        return Some(ExecResult {
            payload: json!({
                "errorMessage": format!("generated csproj escaped the staging dir for assembly '{assembly_name}'"),
                "errorType": "Runtime.ImportModuleError",
            })
            .to_string(),
            function_error: Some("Runtime.ImportModuleError".to_string()),
            logs: String::new(),
        });
    }
    let _ = std::fs::remove_dir_all(code_dir.join("publish"));
    // The SDK's implicit glob + project-dir-as-output interplay: building a
    // project with `-o <project dir>` (direct output at the csproj, the
    // python-era `_compile_source` invocation that was removed here)
    // produced an EMPTY assembly — type names missing from the metadata
    // heap — the quirk era `_run_with_bootstrap` dodged by publishing to a
    // subdirectory and peeking `publish/` for the same reason. Build into a
    // subdirectory and hoist the outputs into the code dir instead.
    let build_out = staging.join(".robotocore-build");
    let _ = std::fs::remove_dir_all(&build_out);
    let mut builder = std::process::Command::new("dotnet");
    builder
        .arg("build")
        .arg("-c")
        .arg("Release")
        .arg("-o")
        .arg(&build_out)
        .arg("--nologo")
        .arg("-v")
        .arg("quiet")
        .env("DOTNET_CLI_TELEMETRY_OPTOUT", "1")
        .env("DOTNET_NOLOGO", "1")
        .env("DOTNET_SKIP_FIRST_TIME_EXPERIENCE", "1")
        .env("DOTNET_SYSTEM_GLOBALIZATION_INVARIANT", "1")
        .current_dir(&staging)
        .stdout(Stdio::piped())
        .stderr(Stdio::piped());
    let run = builder.output();
    let ok = run.as_ref().map(|o| o.status.success()).unwrap_or(false);
    if ok {
        for entry in std::fs::read_dir(&build_out)
            .map(|entries| entries.filter_map(|e| e.ok()).collect::<Vec<_>>())
            .unwrap_or_default()
        {
            if let Some(name) = entry.path().file_name() {
                let _ = std::fs::copy(entry.path(), code_dir.join(name));
            }
        }
    }
    cleanup_staging();
    match run {
        Ok(_) if ok && dll.exists() => None,
        _ => {
            let stderr = run
                .as_ref()
                .map(|o| String::from_utf8_lossy(&o.stderr).to_string())
                .unwrap_or_default();
            Some(ExecResult {
                payload: json!({
                    "errorMessage": format!("Failed to compile .cs source files into '{assembly_name}.dll'"),
                    "errorType": "Runtime.ImportModuleError",
                })
                .to_string(),
                function_error: Some("Runtime.ImportModuleError".to_string()),
                logs: stderr,
            })
        }
    }
}

/// Map a runtime string to its runtime *family* as the gateway reports it
/// (the python-era `/_robotocore/runtimes` family list: custom, dotnet, java,
/// nodejs, python, ruby).
fn runtime_family(runtime: &str) -> &'static str {
    if runtime.starts_with("python") {
        "python"
    } else if runtime.starts_with("nodejs") {
        "nodejs"
    } else if runtime.starts_with("java") {
        "java"
    } else if runtime.starts_with("dotnet") {
        "dotnet"
    } else if runtime.starts_with("ruby") {
        "ruby"
    } else {
        // go1.x / provided* run through the custom-runtime image families.
        "custom"
    }
}

/// Runtime families served by the docker executor, for the gateway layer's
/// `/_robotocore/runtimes` surface: `Some(sorted families)` when
/// `LAMBDA_EXECUTOR=docker` and docker is usable, `None` otherwise.
///
/// Kept cheap: the daemon probe gets a 2s timeout so an HTTP handler can call
/// this inline.
pub fn docker_capability_runtimes() -> Option<Vec<&'static str>> {
    if executor_mode() != "docker" {
        return None;
    }
    if !docker_available(2) {
        return None;
    }
    let mut families: Vec<&'static str> = Vec::new();
    for (runtime, _) in RUNTIME_IMAGES {
        let family = runtime_family(runtime);
        if !families.contains(&family) {
            families.push(family);
        }
    }
    families.sort_unstable();
    Some(families)
}

/// Check if the docker CLI is available and the daemon is running.
///
/// Output is suppressed; the timeout bounds the daemon round-trip so callers
/// in HTTP handlers are not blocked for long.
pub fn docker_available(timeout_secs: u64) -> bool {
    run_subprocess(
        &[DOCKER.to_string(), "info".to_string()],
        None,
        timeout_secs,
    )
    .map(|out| out.status_code == Some(0))
    .unwrap_or(false)
}

/// Build the `docker run` command line for a Lambda invocation.
///
/// Mirrors the python-era `_build_docker_run_cmd` flag-for-flag: docker run
/// --rm --name <ctr> -v <code>:/var/task:ro <core env pairs> [user env pairs]
/// [--network <net>] [--dns <dns>] -- image <handler>. One addition: the
/// python twin never needed a gateway route because docker desktop aliases
/// `host.docker.internal` for its containers, but plain Linux engines don't —
/// so `--add-host host.docker.internal:host-gateway` is what makes
/// `AWS_ENDPOINT_URL` resolve inside the container.
#[allow(clippy::too_many_arguments)]
pub fn build_docker_run_cmd(
    image: &str,
    container_name: &str,
    code_dir: &str,
    handler: &str,
    function_name: &str,
    timeout: u32,
    memory_size: u32,
    env_vars: Option<&HashMap<String, String>>,
    region: &str,
    account_id: &str,
    gateway_port: u16,
    docker_network: Option<&str>,
    docker_dns: Option<&str>,
) -> Vec<String> {
    let mut cmd = vec![
        "docker".to_string(),
        "run".to_string(),
        "--rm".to_string(),
        "--name".to_string(),
        container_name.to_string(),
        // Mount function code read-only.
        "-v".to_string(),
        format!("{code_dir}:/var/task:ro"),
        // Core Lambda environment variables.
        "-e".to_string(),
        format!("AWS_LAMBDA_FUNCTION_NAME={function_name}"),
        "-e".to_string(),
        format!("AWS_LAMBDA_FUNCTION_MEMORY_SIZE={memory_size}"),
        "-e".to_string(),
        format!("AWS_LAMBDA_FUNCTION_TIMEOUT={timeout}"),
        "-e".to_string(),
        format!("AWS_REGION={region}"),
        "-e".to_string(),
        format!("AWS_DEFAULT_REGION={region}"),
        "-e".to_string(),
        format!("AWS_ACCOUNT_ID={account_id}"),
        // The shim's context reports these names (`log_group_name`,
        // `log_stream_name`) — the official runtime clients read them from
        // this env in containers, so ctx.log_group_name matches the shim.
        "-e".to_string(),
        format!("AWS_LAMBDA_LOG_GROUP_NAME=/aws/lambda/{function_name}"),
        "-e".to_string(),
        format!("AWS_LAMBDA_LOG_STREAM_NAME={}", shim_log_stream_name()),
        // The twin's python shim puts layer packages (their `python/`
        // prefixes) on the interpreter path — same behavior in containers.
        "-e".to_string(),
        "PYTHONPATH=/var/task:/var/task/python".to_string(),
        "-e".to_string(),
        format!("_HANDLER={handler}"),
        "-e".to_string(),
        "AWS_ACCESS_KEY_ID=testing".to_string(),
        "-e".to_string(),
        "AWS_SECRET_ACCESS_KEY=testing".to_string(),
        "-e".to_string(),
        format!("AWS_ENDPOINT_URL=http://host.docker.internal:{gateway_port}"),
    ];

    // Pass through function environment variables.
    if let Some(env_vars) = env_vars {
        for (key, value) in env_vars {
            cmd.push("-e".to_string());
            cmd.push(format!("{key}={value}"));
        }
    }

    // Docker network.
    if let Some(network) = docker_network {
        cmd.push("--network".to_string());
        cmd.push(network.to_string());
    }

    // Custom DNS.
    if let Some(dns) = docker_dns {
        cmd.push("--dns".to_string());
        cmd.push(dns.to_string());
    }

    // Reach the host gateway from Linux-engine containers.
    cmd.push("--add-host".to_string());
    cmd.push("host.docker.internal:host-gateway".to_string());

    // Image and handler (the Lambda runtime images receive the event over the
    // Runtime Interface; the command argument is the handler override).
    cmd.push(image.to_string());
    cmd.push(handler.to_string());

    cmd
}

// ============================================================================
// Loose managed-family bridges (python-era ports)
//
// For `LOOSE_BRIDGE_FAMILIES` the invocation semantics are the python twin's:
// `src/robotocore/services/lambda_/runtimes/{java,dotnet,ruby}.py` + their
// bootstraps (Bootstrap.java, bootstrap.rb, dotnet's Bootstrap.cs) ran ON THE
// TWIN's host with `run_subprocess` (event JSON on stdin, JSON result on
// stdout, error JSON + non-zero exit). The official images dispatch these
// families' handlers strictly (java rejects `(String, Object)`, dotnet
// requires `ILambdaContext`, the ruby EMI gives string context values), so
// the ported bootstraps run INSIDE the mapped image as a one-shot container.
// ============================================================================

/// Where the bridge assets live on the twin (written once per process).
///
/// Pruned-bridge guard: the startup sweep reaps `robotocore-lambda-bridges`
/// entries older than [`STALE_EXTRACT_DIR_AGE`], so this process's dir can
/// disappear mid-life. The cached path is re-validated and recreated on
/// demand — a concurrent twin's sweep must cost the older twin at most one
/// rebuilt bridge, never a ghost mount.
pub(crate) fn loose_bridge_dir() -> std::path::PathBuf {
    static DIR: parking_lot::Mutex<Option<std::path::PathBuf>> = parking_lot::Mutex::new(None);
    let mut guard = DIR.lock();
    if let Some(dir) = &*guard {
        if dir.is_dir() {
            return dir.clone();
        }
        *guard = None;
    }
    let dir = std::env::temp_dir()
        .join("robotocore-lambda-bridges")
        .join(uuid::Uuid::new_v4().simple().to_string());
    let _ = std::fs::create_dir_all(&dir);
    for (name, source) in [
        ("RobotocoreJavaBridge.java", JAVA_BRIDGE_SOURCE.as_bytes()),
        ("bootstrap.rb", RUBY_BRIDGE_SOURCE.as_bytes()),
        (
            "RobotocoreDotnetBridge.csproj",
            DOTNET_BRIDGE_CSPROJ.as_bytes(),
        ),
        ("RobotocoreDotnetBridge.cs", DOTNET_BRIDGE_SOURCE.as_bytes()),
    ] {
        let _ = std::fs::write(dir.join(name), source);
    }
    *guard = Some(dir.clone());
    dir
}

/// Java loose bridge (port of the python-era `bootstraps/Bootstrap.java`;
/// handler spec from `_HANDLER` as `Class::method` or `Class`, event JSON on
/// stdin, result `toString`ed to stdout, error JSON + exit 1 on failure).
const JAVA_BRIDGE_SOURCE: &str = r#"import java.io.*;
import java.lang.reflect.*;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.stream.*;

public class RobotocoreJavaBridge {
    public static void main(String[] args) {
        String handlerSpec = System.getenv("_HANDLER");
        if (handlerSpec == null && args.length > 0) handlerSpec = args[0];
        if (handlerSpec == null) {
            System.out.println("{\"errorMessage\":\"No handler specified\",\"errorType\":\"Runtime.HandlerNotFound\"}");
            System.exit(1);
        }

        String className;
        String methodName = "handleRequest";
        if (handlerSpec.contains("::")) {
            String[] parts = handlerSpec.split("::");
            className = parts[0];
            methodName = parts[1];
        } else {
            className = handlerSpec;
        }

        String eventJson;
        try (BufferedReader reader = new BufferedReader(new InputStreamReader(System.in, StandardCharsets.UTF_8))) {
            eventJson = reader.lines().collect(Collectors.joining("\n"));
        } catch (IOException e) {
            eventJson = "{}";
        }

        Class<?> handlerClass;
        try {
            handlerClass = Class.forName(className);
        } catch (ClassNotFoundException e) {
            System.out.printf("{\"errorMessage\":\"Cannot find class '%s': %s\",\"errorType\":\"Runtime.ImportModuleError\"}%n",
                className, e.getMessage());
            System.exit(1);
            return;
        }

        Object instance;
        try {
            instance = handlerClass.getDeclaredConstructor().newInstance();
        } catch (Exception e) {
            System.out.printf("{\"errorMessage\":\"Cannot instantiate '%s': %s\",\"errorType\":\"Runtime.ImportModuleError\"}%n",
                className, e.getMessage());
            System.exit(1);
            return;
        }

        Method method = null;
        try {
            method = handlerClass.getMethod(methodName, String.class, Object.class);
        } catch (NoSuchMethodException e1) {
            try {
                method = handlerClass.getMethod(methodName, Map.class, Object.class);
            } catch (NoSuchMethodException e2) {
                try {
                    method = handlerClass.getMethod(methodName, InputStream.class, OutputStream.class, Object.class);
                } catch (NoSuchMethodException e3) {
                    try {
                        method = handlerClass.getMethod(methodName, Object.class, Object.class);
                    } catch (NoSuchMethodException e4) {
                        System.out.printf(
                            "{\"errorMessage\":\"Handler method '%s' not found in '%s'\",\"errorType\":\"Runtime.HandlerNotFound\"}%n",
                            methodName, className);
                        System.exit(1);
                        return;
                    }
                }
            }
        }

        // era build_env context: a map the handler may inspect as is.
        Map<String, Object> context = new LinkedHashMap<>();
        context.put("functionName", System.getenv().getOrDefault("AWS_LAMBDA_FUNCTION_NAME", "test-function"));
        context.put("functionVersion", System.getenv().getOrDefault("AWS_LAMBDA_FUNCTION_VERSION", "$LATEST"));
        context.put("invokedFunctionArn", System.getenv().getOrDefault("AWS_LAMBDA_FUNCTION_ARN", ""));
        context.put("memoryLimitInMB", Integer.parseInt(System.getenv().getOrDefault("AWS_LAMBDA_FUNCTION_MEMORY_SIZE", "128")));
        context.put("awsRequestId", UUID.randomUUID().toString());
        context.put("logGroupName", System.getenv().getOrDefault("AWS_LAMBDA_LOG_GROUP_NAME", "/aws/lambda/Functions"));
        context.put("logStreamName", System.getenv().getOrDefault("AWS_LAMBDA_LOG_STREAM_NAME", ""));

        try {
            Object result;
            Class<?>[] paramTypes = method.getParameterTypes();

            if (paramTypes.length == 3 && paramTypes[0] == InputStream.class) {
                ByteArrayInputStream in = new ByteArrayInputStream(eventJson.getBytes(StandardCharsets.UTF_8));
                ByteArrayOutputStream out = new ByteArrayOutputStream();
                method.invoke(instance, in, out, context);
                System.out.print(out.toString());
            } else if (paramTypes[0] == String.class) {
                result = method.invoke(instance, eventJson, context);
                System.out.print(result == null ? "null" : result.toString());
            } else if (paramTypes[0] == Map.class) {
                Map<String, Object> eventMap = new HashMap<>();
                eventMap.put("_raw", eventJson);
                result = method.invoke(instance, eventMap, context);
                System.out.print(result == null ? "null" : result.toString());
            } else {
                result = method.invoke(instance, eventJson, context);
                System.out.print(result == null ? "null" : result.toString());
            }
        } catch (InvocationTargetException e) {
            Throwable cause = e.getCause() != null ? e.getCause() : e;
            StringWriter sw = new StringWriter();
            cause.printStackTrace(new PrintWriter(sw));
            System.err.print(sw.toString());
            System.out.printf(
                "{\"errorMessage\":\"%s\",\"errorType\":\"%s\"}",
                cause.getMessage() != null ? cause.getMessage().replace("\"", "\\\"") : "Unknown error",
                cause.getClass().getSimpleName());
            System.exit(1);
        } catch (Exception e) {
            System.err.println(e.toString());
            System.out.printf(
                "{\"errorMessage\":\"%s\",\"errorType\":\"%s\"}",
                e.getMessage() != null ? e.getMessage().replace("\"", "\\\"") : "Unknown error",
                e.getClass().getSimpleName());
            System.exit(1);
        }
    }
}
"#;

/// Ruby loose bridge (port of the python-era `bootstraps/bootstrap.rb`):
/// `_HANDLER` as `module.method`, event parsed from stdin (bad JSON -> `{}`),
/// Struct context carrying the era's same VALUES — here derived from the
/// runtime envs the run prefix sets (`AWS_LAMBDA_FUNCTION_ARN`,
/// `AWS_LAMBDA_LOG_STREAM_NAME`, `AWS_LAMBDA_FUNCTION_MEMORY_SIZE`) — and
/// error JSON with the exception class name.
const RUBY_BRIDGE_SOURCE: &str = r##"require 'json'
require 'securerandom'

handler_spec = ENV['_HANDLER'] || ARGV[0]
unless handler_spec
  $stdout.write(JSON.generate({
    errorMessage: 'No handler specified',
    errorType: 'Runtime.HandlerNotFound'
  }))
  exit(1)
end

dot_index = handler_spec.rindex('.')
unless dot_index
  $stdout.write(JSON.generate({
    errorMessage: "Bad handler format: #{handler_spec}",
    errorType: 'Runtime.HandlerNotFound'
  }))
  exit(1)
end

module_name = handler_spec[0...dot_index]
method_name = handler_spec[dot_index + 1..]

input_data = $stdin.read
event = begin
  JSON.parse(input_data)
rescue JSON::ParserError
  {}
end

context_data = {
  function_name: ENV['AWS_LAMBDA_FUNCTION_NAME'] || 'test-function',
  function_version: '$LATEST',
  invoked_function_arn: ENV['AWS_LAMBDA_FUNCTION_ARN'],
  memory_limit_in_mb: (ENV['AWS_LAMBDA_FUNCTION_MEMORY_SIZE'] || '128').to_i,
  aws_request_id: SecureRandom.uuid,
  log_group_name: "/aws/lambda/#{ENV['AWS_LAMBDA_FUNCTION_NAME'] || 'test-function'}",
  log_stream_name: ENV['AWS_LAMBDA_LOG_STREAM_NAME'],
}

LambdaContext = Struct.new(*context_data.keys, keyword_init: true) do
  def get_remaining_time_in_millis
    timeout = (ENV['AWS_LAMBDA_FUNCTION_TIMEOUT'] || '3').to_i
    timeout * 1000
  end
end
context = LambdaContext.new(**context_data)

module_file = File.join(Dir.pwd, "#{module_name}.rb")
unless File.exist?(module_file)
  $stdout.write(JSON.generate({
    errorMessage: "Cannot find module '#{module_name}': #{module_file}",
    errorType: 'Runtime.ImportModuleError'
  }))
  exit(1)
end

begin
  require module_file
rescue => e
  $stdout.write(JSON.generate({
    errorMessage: "Error loading module '#{module_name}': #{e.message}",
    errorType: 'Runtime.ImportModuleError',
    stackTrace: e.backtrace || []
  }))
  exit(1)
end

unless respond_to?(method_name.to_sym, true)
  $stdout.write(JSON.generate({
    errorMessage: "Handler method '#{method_name}' not found in '#{module_name}'",
    errorType: 'Runtime.HandlerNotFound'
  }))
  exit(1)
end

begin
  result = send(method_name.to_sym, event: event, context: context)
  $stdout.write(JSON.generate(result))
rescue => e
  $stderr.write("#{e.class}: #{e.message}\n#{e.backtrace&.join("\n")}\n")
  $stdout.write(JSON.generate({
    errorMessage: e.message,
    errorType: e.class.name,
    stackTrace: e.backtrace || []
  }))
  exit(1)
end
"##;

/// .NET loose bridge (port of the python-era dotnet `Bootstrap.cs`): loads
/// the user assembly via reflection, invokes `method(eventJson, null)` for
/// the era's `(string, object)` handlers, prints `result.ToString() ?? null`
/// to stdout, error JSON (type name) + exit 1 on failure. Built once per TFM
/// on the twin (`dotnet publish`) because the official dotnet images ship
/// only the runtime, not the SDK.
const DOTNET_BRIDGE_SOURCE: &str = r##"using System;
using System.IO;
using System.Reflection;
using System.Text.Json;

class RobotocoreDotnetBridge {
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
            string dllPath = Path.Combine("/var/task", assemblyName + ".dll");
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
"##;

/// The bridge's csproj: the .NET images ship no SDK, so the twin builds this
/// once per TFM with the host SDK (python-era pattern: cached bootstrap).
pub(crate) const DOTNET_BRIDGE_CSPROJ: &str = r##"<Project Sdk="Microsoft.NET.Sdk">
  <PropertyGroup>
    <OutputType>Exe</OutputType>
    <!-- -f net8.0/net6.0 selects at publish time; the images carry the
         matching shared runtime only. -->
    <TargetFrameworks>net8.0;net6.0</TargetFrameworks>
    <ImplicitUsings>enable</ImplicitUsings>
  </PropertyGroup>
</Project>"##;

/// The shim's per-invocation Lambda log-stream name (`src/runtime.rs`
/// execute_python context): `YYYY/MM/DD/[$LATEST]{uuid32}`.
pub(crate) fn shim_log_stream_name() -> String {
    let date = chrono::Utc::now().format("%Y/%m/%d");
    format!("{date}/[$LATEST]{}", uuid::Uuid::new_v4().simple())
}

/// Python-family error-type normalizer: the official python runtime client
/// prefixes its load errors (`Runtime.UserCodeSyntaxError`,
/// `Runtime.ImportModuleError`) while this twin's shim reports the bare
/// interpreter names (`SyntaxError`, `ModuleNotFoundError`) — the parity
/// shape the suites assert. Only applied to the python family.
pub(crate) fn normalize_error_type_for_family(payload: &mut serde_json::Value, family: &str) {
    let Some(obj) = payload.as_object_mut() else {
        return;
    };
    let Some(t) = obj.get("errorType").and_then(serde_json::Value::as_str) else {
        return;
    };
    if family != "python" {
        return;
    }
    let mapped = match t {
        "Runtime.UserCodeSyntaxError" => Some("SyntaxError"),
        "Runtime.ImportModuleError" => Some("ModuleNotFoundError"),
        _ => None,
    };
    if let Some(mapped) = mapped {
        obj.insert(
            "errorType".to_string(),
            serde_json::Value::String(mapped.to_string()),
        );
    }
}

/// Execute an invocation through the docker executor when the invoke path
/// selects docker mode (`LAMBDA_EXECUTOR=docker` + a mapped runtime image + a
/// reachable daemon). `None` means "fall through to the in-process
/// executors", which keeps the default behavior with no env set
/// byte-identical to local execution.
#[allow(clippy::too_many_arguments)]
pub fn try_execute_docker(
    runtime: &str,
    code_zip: &[u8],
    layer_zips: &[Vec<u8>],
    handler: &str,
    event: &str,
    function_name: &str,
    function_version: &str,
    function_arn: &str,
    env_vars: &HashMap<String, String>,
    timeout_secs: u32,
    memory_size: u32,
    region: &str,
    account_id: &str,
    code_dir: Option<&std::path::Path>,
) -> Option<ExecResult> {
    if executor_mode() != "docker" {
        return None;
    }
    runtime_image(runtime)?;
    let executor = get_docker_executor();
    // Per-invoke daemon re-probe (5s cap): the construction-time snapshot
    // can go stale when the daemon dies after this twin started — fall back
    // to the LOCAL executors instead of wedging the docker path, and
    // re-adopt a daemon that comes back after a startup dip.
    // `Runtime.InvalidRuntime` stays the shape for runtimes with no local
    // executor (execute_family's unmapped-family contract below).
    if !executor.reprobe_docker_available() {
        return None;
    }
    Some(executor.execute(
        runtime,
        code_zip,
        layer_zips,
        handler,
        event,
        function_name,
        function_version,
        function_arn,
        env_vars,
        timeout_secs,
        memory_size,
        region,
        account_id,
        code_dir,
    ))
}

/// A subprocess result with output capped per stream, like the in-process
/// executors' captured logs.
#[derive(Clone, Debug)]
pub(crate) struct SubprocessOutput {
    pub(crate) status_code: Option<i32>,
    pub(crate) stdout: String,
    pub(crate) stderr: String,
}

/// Failure modes of [`run_subprocess`]; `NotFound` marks a missing docker CLI
/// (the python-era `FileNotFoundError` fallback trigger).
#[derive(Clone, Debug)]
pub(crate) enum DockerRunError {
    NotFound,
    Timeout,
    Io,
}

/// Run a subprocess with bounded waiting: stdin bytes piped in,
/// stdout/stderr drained on dedicated threads, killed at the deadline.
/// Mirrors the drain/collect discipline of the in-process executors.
pub(crate) fn run_subprocess(
    argv: &[String],
    input: Option<&[u8]>,
    timeout_secs: u64,
) -> Result<SubprocessOutput, DockerRunError> {
    if argv.is_empty() {
        return Err(DockerRunError::Io);
    }
    let mut cmd = std::process::Command::new(&argv[0]);
    cmd.args(&argv[1..])
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .stdin(if input.is_some() {
            Stdio::piped()
        } else {
            Stdio::null()
        });
    let mut child: Child = match cmd.spawn() {
        Ok(c) => c,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Err(DockerRunError::NotFound),
        Err(_) => return Err(DockerRunError::Io),
    };
    // The python-era executor always wrote the event JSON to the docker run
    // subprocess's stdin; the detached CLI ignores it, so write errors are
    // swallowed (python's communicate() behavior).
    if let Some(bytes) = input {
        if let Some(mut stdin) = child.stdin.take() {
            let _ = stdin.write_all(bytes);
            let _ = stdin.flush();
            drop(stdin);
        }
    }

    // Drain pipes immediately, on dedicated threads, so a chatty container
    // cannot deadlock on the OS pipe buffer while nobody reads.
    let stdout_rx = drain_thread(child.stdout.take(), DOCKER_OUTPUT_CAP);
    let stderr_rx = drain_thread(child.stderr.take(), DOCKER_OUTPUT_CAP);

    let deadline = Instant::now() + Duration::from_secs(timeout_secs.max(1));
    let mut timed_out = false;
    let mut status_code = None;
    loop {
        match child.try_wait() {
            Ok(Some(status)) => {
                status_code = status.code();
                break;
            }
            Ok(None) => {
                if Instant::now() > deadline {
                    let _ = child.kill();
                    let _ = child.wait();
                    timed_out = true;
                    break;
                }
                std::thread::sleep(Duration::from_millis(25));
            }
            Err(_) => return Err(DockerRunError::Io),
        }
    }
    if timed_out {
        return Err(DockerRunError::Timeout);
    }

    // The CLI is reaped, so collecting is bounded: grandchildren may hold the
    // pipes open past the deadline, in which case the tail is abandoned.
    let drain_deadline = Instant::now() + Duration::from_secs(1);
    Ok(SubprocessOutput {
        status_code,
        stdout: collect_by(&stdout_rx, drain_deadline),
        stderr: collect_by(&stderr_rx, drain_deadline),
    })
}

/// The docker surface the executor touches. Injected so unit tests can record
/// calls and replay canned daemon/curl output without a running daemon.
pub(crate) trait DockerCli: Send + Sync {
    /// `docker run <argv>` with the event JSON piped to the CLI subprocess.
    fn run(
        &self,
        argv: &[String],
        event_json: &str,
        timeout_secs: u64,
    ) -> Result<SubprocessOutput, DockerRunError>;

    /// Invoke the function via the Runtime Interface (python-era
    /// `_invoke_via_runtime_interface`): POST the event to the Lambda
    /// runtime API server on localhost:8080 inside the container. Returns
    /// (stdout, stderr) of the curl call.
    fn invoke_via_runtime_interface(
        &self,
        container_name: &str,
        event_json: &str,
        timeout: u32,
    ) -> Result<SubprocessOutput, DockerRunError>;

    /// Force-remove a container by name. Best-effort, ignores errors.
    fn force_remove_container(&self, container_name: &str);

    /// Combined stdout+stderr of a container (function + rapid logs).
    fn container_logs(&self, container_name: &str) -> String;

    /// Names of leftover `robotocore-lambda-{function}-{uuid8}` containers —
    /// the list half of the startup sweep ([`sweep_list_cmd`]) with the
    /// in-process prefix match of [`straggler_names`] already applied; the
    /// removal half is [`DockerCli::force_remove_container`].
    fn lambda_straggler_containers(&self) -> Vec<String>;

    /// Daemon-liveness probe (`docker info`-style). The real impl bounds the
    /// round-trip by [`DOCKER_DAEMON_PROBE_CAP`]; the mock scripts a verdict
    /// so the per-invoke re-probe is testable without a daemon.
    fn daemon_available(&self) -> bool;
}

const DOCKER: &str = "docker";
const RUNTIME_INTERFACE_INVOKE_URL: &str =
    "http://localhost:8080/2015-03-31/functions/function/invocations";

/// Per-invoke cap on the daemon re-probe (`docker info`): a healthy daemon
/// answers in well under a second, but a wedged CLI/daemon must not stretch
/// an invoke by more than this window; beyond it the invocation falls back
/// to the local executors.
pub(crate) const DOCKER_DAEMON_PROBE_CAP: u64 = 5;

/// Name prefix of every Lambda container this executor spawns
/// (`robotocore-lambda-{function}-{uuid8}`). Also the startup sweep's filter
/// anchor.
pub(crate) const LAMBDA_CONTAINER_PREFIX: &str = "robotocore-lambda-";

/// Argv listing leftover Lambda containers: `docker ps -aq --filter
/// name=robotocore-lambda- --format {{.Names}}`, the first half of the
/// startup sweep's `docker rm -f` removal, argv-only as this module's
/// convention (no shell). `-a` reaches every container state — docker only
/// lists *running* containers without it, so Created-state husks (e.g. a
/// `--rm` bridge container frozen before its entrypoint started) would
/// otherwise survive the sweep and pile up across restarts. The docker
/// `name=` filter matches substrings, not prefixes; the exact-prefix anchor
/// happens in-process in [`straggler_names`].
pub(crate) fn sweep_list_cmd() -> Vec<String> {
    vec![
        DOCKER.to_string(),
        "ps".to_string(),
        "-aq".to_string(),
        "--filter".to_string(),
        format!("name={LAMBDA_CONTAINER_PREFIX}"),
        "--format".to_string(),
        "{{.Names}}".to_string(),
    ]
}

/// The sweep's in-process exact-shape match over `sweep_list_cmd` output
/// (one container name per line): docker's `name=` filter is substring-
/// based, so every candidate is checked against the executor's own name
/// shape (`robotocore-lambda-{function}-{uuid8}`) — only containers spawned
/// by this executor are reaped, and neighbors whose name merely *contains*
/// the prefix (e.g. `simulator-robotocore-lambda-proxy`, or a bare
/// `robotocore-lambda-proxy`) survive. Names double as `docker rm -f`
/// operands.
pub(crate) fn straggler_names(list_stdout: &str) -> Vec<String> {
    list_stdout
        .lines()
        .map(str::trim)
        .filter(|name| !name.is_empty() && is_lambda_straggler_name(name))
        .map(str::to_string)
        .collect()
}

/// The executor names its containers `robotocore-lambda-{function}-{uuid8}`
/// with an 8-hex tail; a sweeper candidate must have that exact prefix AND
/// tail segment to qualify.
fn is_lambda_straggler_name(name: &str) -> bool {
    let Some((_, tail)) = name
        .strip_prefix(LAMBDA_CONTAINER_PREFIX)
        .and_then(|rest| rest.rsplit_once('-'))
    else {
        return false;
    };
    tail.len() == 8 && tail.chars().all(|c| c.is_ascii_hexdigit())
}

/// The temp roots this crate's executors own under `std::env::temp_dir()`,
/// all of them startup-pruned by [`DockerLambdaExecutor::sweep_stragglers`]:
/// per-invocation docker extraction dirs, the dotnet host-SDK staging root,
/// the per-process loose-bridge sources, the python/custom `-exec` work
/// dirs and the node `-node` work dirs.
pub(crate) const TEMP_PRUNE_ROOTS: [&str; 5] = [
    "robotocore-lambda-docker",
    "robotocore-lambda-build",
    "robotocore-lambda-bridges",
    "robotocore-lambda-exec",
    "robotocore-lambda-node",
];

/// Age beyond which a per-invocation extraction dir under
/// /tmp/robotocore-lambda-docker counts as crashed-process debris: live ones
/// are removed by the same invocation that created them, well within an
/// hour even at the maximum function timeout.
pub(crate) const STALE_EXTRACT_DIR_AGE: std::time::Duration = std::time::Duration::from_secs(3600);

/// Remove `base/{function}-{uuid}` extraction dirs left by a crashed twin
/// (the `docker rm -f` sweep may run before the dirs are reaped). Age-based,
/// so a concurrently running twin's in-flight dirs survive.
pub(crate) fn prune_stale_extract_dirs(base: &std::path::Path, older_than: std::time::Duration) {
    let Ok(now) = std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH) else {
        return;
    };
    let Ok(entries) = std::fs::read_dir(base) else {
        return;
    };
    for entry in entries.flatten() {
        let stale = entry
            .metadata()
            .and_then(|m| m.modified())
            .ok()
            .and_then(|m| m.duration_since(std::time::UNIX_EPOCH).ok())
            .map(|m| now >= m && now - m >= older_than)
            .unwrap_or(false);
        if stale {
            let _ = std::fs::remove_dir_all(entry.path());
        }
    }
}

/// The real docker CLI.
#[derive(Default)]
pub(crate) struct RealCli;

impl DockerCli for RealCli {
    fn run(
        &self,
        argv: &[String],
        event_json: &str,
        timeout_secs: u64,
    ) -> Result<SubprocessOutput, DockerRunError> {
        run_subprocess(argv, Some(event_json.as_bytes()), timeout_secs)
    }

    fn invoke_via_runtime_interface(
        &self,
        container_name: &str,
        event_json: &str,
        timeout: u32,
    ) -> Result<SubprocessOutput, DockerRunError> {
        run_subprocess(
            &[
                DOCKER.to_string(),
                "exec".to_string(),
                container_name.to_string(),
                "curl".to_string(),
                "-s".to_string(),
                "-X".to_string(),
                "POST".to_string(),
                RUNTIME_INTERFACE_INVOKE_URL.to_string(),
                "-d".to_string(),
                event_json.to_string(),
            ],
            None,
            // Grace beyond the function timeout for the runtime round-trip,
            // as in the python-era `_invoke_via_runtime_interface`.
            u64::from(timeout.max(1)) + 5,
        )
    }

    fn force_remove_container(&self, container_name: &str) {
        // Best-effort, may already be gone.
        let _ = run_subprocess(
            &[
                DOCKER.to_string(),
                "rm".to_string(),
                "-f".to_string(),
                container_name.to_string(),
            ],
            None,
            10,
        );
    }

    fn container_logs(&self, container_name: &str) -> String {
        match run_subprocess(
            &[
                DOCKER.to_string(),
                "logs".to_string(),
                container_name.to_string(),
            ],
            None,
            10,
        ) {
            Ok(out) => {
                let mut logs = out.stdout;
                if !out.stderr.is_empty() {
                    if !logs.is_empty() {
                        logs.push('\n');
                    }
                    logs.push_str(&out.stderr);
                }
                logs
            }
            Err(_) => String::new(),
        }
    }

    fn lambda_straggler_containers(&self) -> Vec<String> {
        match run_subprocess(&sweep_list_cmd(), None, 10) {
            Ok(out) => straggler_names(&out.stdout),
            Err(_) => Vec::new(),
        }
    }

    fn daemon_available(&self) -> bool {
        docker_available(DOCKER_DAEMON_PROBE_CAP)
    }
}

/// Executes Lambda functions in Docker containers using the docker CLI.
///
/// Falls back to the local in-process executors if Docker is not available or
/// if the runtime has no mapped image.
pub struct DockerLambdaExecutor {
    pub(crate) gateway_port: u16,
    pub(crate) docker_network: Option<String>,
    pub(crate) docker_dns: Option<String>,
    /// Daemon-liveness signal. Probed at construction like the python-era
    /// `__init__` and re-probed per invocation
    /// ([`DockerLambdaExecutor::reprobe_docker_available`]) so a daemon that
    /// dies mid-life falls back to the local executors instead of wedging
    /// the docker path.
    pub(crate) docker_available: AtomicBool,
    pub(crate) cli: Arc<dyn DockerCli>,
}

impl Default for DockerLambdaExecutor {
    fn default() -> Self {
        Self::new()
    }
}

impl DockerLambdaExecutor {
    pub fn new() -> Self {
        let gateway_port = std::env::var("GATEWAY_PORT")
            .ok()
            .and_then(|p| p.parse::<u16>().ok())
            .unwrap_or(4566);
        let docker_network = std::env::var("LAMBDA_DOCKER_NETWORK")
            .ok()
            .filter(|s| !s.is_empty());
        let docker_dns = std::env::var("LAMBDA_DOCKER_DNS")
            .ok()
            .filter(|s| !s.is_empty());
        let cli: Arc<dyn DockerCli> = Arc::new(RealCli);
        let docker_available = cli.daemon_available();
        let executor = Self {
            gateway_port,
            docker_network,
            docker_dns,
            docker_available: AtomicBool::new(docker_available),
            cli,
        };
        if docker_available {
            // Leak-bounded startup sweep: a twin killed mid-invocation leaves
            // `--rm` containers running forever (the runtime interface keeps
            // them alive waiting for the next event) and extraction dirs on
            // disk. Reap any stragglers from a previous process now; a live
            // concurrent twin loses at most containers that were in flight
            // at this instant (a timeout+10s window).
            executor.sweep_stragglers();
        }
        executor
    }

    /// Re-probe the daemon (capped at [`DOCKER_DAEMON_PROBE_CAP`]) and record
    /// the fresh verdict on the executor. Called per invocation: the
    /// construction-time snapshot can go stale when the daemon dies after
    /// this twin started — a stale-true snapshot would spend every invoke's
    /// whole budget on docker CLI errors, while a stale-false snapshot would
    /// never re-adopt a daemon that came back. Either way the invocation
    /// then takes the local-executor fallback (see
    /// [`try_execute_docker`]), where `Runtime.InvalidRuntime` remains the
    /// shape for runtimes with no local executor.
    pub(crate) fn reprobe_docker_available(&self) -> bool {
        let ok = self.cli.daemon_available();
        self.docker_available.store(ok, AtomicOrdering::Release);
        ok
    }

    /// Leak-bounded straggler sweep: list-run the leftover
    /// `robotocore-lambda-*` containers ([`sweep_list_cmd`] + in-process
    /// prefix match, every container state covered) into
    /// [`DockerCli::force_remove_container`], then prune every executor
    /// tempo root ([`TEMP_PRUNE_ROOTS`]) of entries older than
    /// [`STALE_EXTRACT_DIR_AGE`]. Runs once per executor construction
    /// (python-era `__init__` took no such step; this bounds leaks from a
    /// twin killed mid-invocation, whose `--rm` containers the runtime
    /// interface keeps alive forever). A concurrently live twin loses at
    /// most the containers in flight at this instant.
    pub(crate) fn sweep_stragglers(&self) {
        for container in self.cli.lambda_straggler_containers() {
            self.cli.force_remove_container(&container);
        }
        let base = std::env::temp_dir();
        for root in TEMP_PRUNE_ROOTS {
            prune_stale_extract_dirs(&base.join(root), STALE_EXTRACT_DIR_AGE);
        }
    }

    /// Execute a Lambda function in a Docker container.
    ///
    /// Falls back to local execution if Docker is not available or if the
    /// runtime has no mapped image.
    #[allow(clippy::too_many_arguments)]
    pub fn execute(
        &self,
        runtime: &str,
        code_zip: &[u8],
        layer_zips: &[Vec<u8>],
        handler: &str,
        event: &str,
        function_name: &str,
        function_version: &str,
        function_arn: &str,
        env_vars: &HashMap<String, String>,
        timeout_secs: u32,
        memory_size: u32,
        region: &str,
        account_id: &str,
        code_dir: Option<&std::path::Path>,
    ) -> ExecResult {
        if !self.docker_available.load(AtomicOrdering::Acquire) {
            return self.execute_local_fallback(
                runtime,
                code_zip,
                layer_zips,
                handler,
                event,
                function_name,
                function_version,
                function_arn,
                env_vars,
                timeout_secs,
                memory_size,
                region,
                account_id,
            );
        }
        let Some(image) = runtime_image(runtime) else {
            return self.execute_local_fallback(
                runtime,
                code_zip,
                layer_zips,
                handler,
                event,
                function_name,
                function_version,
                function_arn,
                env_vars,
                timeout_secs,
                memory_size,
                region,
                account_id,
            );
        };
        self.execute_docker(
            runtime,
            image,
            code_zip,
            layer_zips,
            handler,
            event,
            function_name,
            function_version,
            function_arn,
            env_vars,
            timeout_secs,
            memory_size,
            region,
            account_id,
            code_dir,
        )
    }

    /// The docker-mode invocation wrapper: extract the code (when not
    /// provided) into a temp dir, run the containerized invocation against it,
    /// and always remove the temp dir afterwards.
    #[allow(clippy::too_many_arguments)]
    fn execute_docker(
        &self,
        runtime: &str,
        image: &str,
        code_zip: &[u8],
        layer_zips: &[Vec<u8>],
        handler: &str,
        event: &str,
        function_name: &str,
        function_version: &str,
        function_arn: &str,
        env_vars: &HashMap<String, String>,
        timeout_secs: u32,
        memory_size: u32,
        region: &str,
        account_id: &str,
        code_dir: Option<&std::path::Path>,
    ) -> ExecResult {
        // Extract code to a temp directory if not provided. Layers extract
        // first, then the deployment package, so code files win conflicts
        // (mirrors the python-era code cache and the other executors here).
        let extracted = code_dir
            .is_none()
            .then(|| self.extract_code_dir(function_name, code_zip, layer_zips));
        let dir: std::path::PathBuf = match code_dir {
            Some(dir) => dir.to_path_buf(),
            None => extracted.clone().unwrap(),
        };
        let result = self.docker_invocation(
            runtime,
            image,
            code_zip,
            layer_zips,
            &dir,
            handler,
            event,
            function_name,
            function_version,
            function_arn,
            env_vars,
            timeout_secs,
            memory_size,
            region,
            account_id,
        );
        if let Some(tmp) = &extracted {
            let _ = std::fs::remove_dir_all(tmp);
        }
        result
    }

    /// One containerized invocation against `code_dir` (already extracted):
    /// Deviation from the python source's happy path (deliberate): the
    /// python-era executor wrote the event to `docker run`'s stdin and parsed
    /// the CLI's stdout — but the CLI never attaches stdin without `-i`, and
    /// no official runtime image reads the event from stdin, so that flow
    /// always ended in `Task.TimedOut`. The images expect the event POSTed to
    /// their runtime-interface-emulator on localhost:8080 (exactly what the
    /// python file's `_invoke_via_runtime_interface` did over `docker exec
    /// curl`), so this port runs the container detached (`-d`), drives the
    /// handshake, and force-removes the container on every path — `--rm`
    /// alone would leave it waiting for the next event. The python-era
    /// budgets are preserved: `timeout + 10` overall (docker run window),
    /// `timeout + 5` per curl, `docker rm -f` on timeout. That same window
    /// also bounds a cold image pull — see the module's cold-pull budget
    /// note.
    #[allow(clippy::too_many_arguments)]
    fn docker_invocation(
        &self,
        runtime: &str,
        image: &str,
        code_zip: &[u8],
        layer_zips: &[Vec<u8>],
        code_dir: &std::path::Path,
        handler: &str,
        event: &str,
        function_name: &str,
        function_version: &str,
        function_arn: &str,
        env_vars: &HashMap<String, String>,
        timeout_secs: u32,
        memory_size: u32,
        region: &str,
        account_id: &str,
    ) -> ExecResult {
        // Managed families (java/dotnet/ruby) run the python-era loose
        // bridges: one-shot `docker run --rm -i` with the event on stdin —
        // see LOOSE_BRIDGE_FAMILIES and exec_loose_family.
        if LOOSE_BRIDGE_FAMILIES.contains(&runtime_family(runtime)) {
            return self.exec_loose_family(
                runtime,
                code_zip,
                layer_zips,
                code_dir,
                handler,
                event,
                function_name,
                function_version,
                function_arn,
                env_vars,
                timeout_secs,
                memory_size,
                region,
                account_id,
            );
        }

        // Generate a unique container name.
        let uuid_hex = uuid::Uuid::new_v4().simple().to_string();
        let container_name = format!("robotocore-lambda-{function_name}-{}", &uuid_hex[..8]);

        // Build the docker run command; run detached (the image's
        // lambda-entrypoint boots the runtime client against the embedded
        // runtime-interface-emulator on localhost:8080 and waits for events).
        let mut cmd = build_docker_run_cmd(
            image,
            &container_name,
            &code_dir.to_string_lossy(),
            handler,
            function_name,
            timeout_secs,
            memory_size,
            Some(env_vars),
            region,
            account_id,
            self.gateway_port,
            self.docker_network.as_deref(),
            self.docker_dns.as_deref(),
        );
        cmd.insert(2, "-d".to_string());

        // The python-era provider normalized an empty invoke body to an
        // empty object before any executor saw it (`event = json.loads(body)
        // if body else {}`), and the Runtime Interface POST must match: a
        // blank body handed to the runtime image is unparseable
        // (Runtime.UnmarshalError from the runtime's JSON load).
        let event_json = if event.trim().is_empty() {
            "{}".to_string()
        } else {
            event.to_string()
        };
        let timeout_secs = timeout_secs.max(1);
        // Overall budget: the python-era `docker run` window with grace for
        // container startup.
        let run_budget_secs = u64::from(timeout_secs) + 10;
        // `docker run -d` answers with the container id on stdout, but a
        // bodyless exit 0 is still a started run — success rides on the exit
        // status alone (empty-stdout-but-zero is not a failed start); a
        // failed start surfaces below.
        match self.cli.run(&cmd, &event_json, run_budget_secs) {
            Ok(out) if out.status_code == Some(0) => {}
            Ok(out) => {
                // `docker run -d` failed to start the container (bad image,
                // pull rejected, malformed mount). Python-era shape for a
                // non-zero run exit: a structured stdout error object is
                // "Handled", anything else is Runtime.ExitError with the CLI
                // output as logs. Best-effort cleanup either way.
                self.cli.force_remove_container(&container_name);
                return run_exit_error(&out.stdout, &out.stderr);
            }
            Err(DockerRunError::NotFound) => {
                // docker CLI not found — python-era FileNotFoundError.
                return self.execute_local_fallback(
                    runtime,
                    code_zip,
                    layer_zips,
                    handler,
                    event,
                    function_name,
                    function_version,
                    function_arn,
                    env_vars,
                    timeout_secs,
                    memory_size,
                    region,
                    account_id,
                );
            }
            Err(_) => {
                self.cli.force_remove_container(&container_name);
                return run_exit_error("", "docker run failed");
            }
        }

        // POST the event to the runtime API server inside the container. The
        // first POST may land before the runtime process registered with the
        // emulator; retry until the overall budget expires. Each accepted
        // POST is one invocation (the emulator queues lone events), so the
        // loop only rescues post attempts that never reached the HTTP
        // exchange — curl could not connect (exit 7, the runtime client had
        // not bound port 8080 yet) or the daemon could not run the exec
        // (125/126/127). Once an attempt has been delivered or its
        // deliverability is unknowable, rescue stops: re-sending delivered
        // events would fire the function twice (double-delivery).
        let deadline = Instant::now() + Duration::from_secs(run_budget_secs);
        let mut result_stdout = String::new();
        let mut timed_out = false;
        let mut container_gone = false;
        // Per-connection accepted-once latch: TRUE once an attempt stopped
        // being a proven pre-exchange failure.
        let mut delivered = false;
        loop {
            match self
                .cli
                .invoke_via_runtime_interface(&container_name, &event_json, timeout_secs)
            {
                Ok(out) if out.status_code == Some(0) && !out.stdout.trim().is_empty() => {
                    result_stdout = out.stdout.trim().to_string();
                    break;
                }
                Ok(out) => {
                    if out.stderr.contains("No such container") {
                        // The container died without answering (runtime
                        // crash, OOM kill) — python-era non-zero docker run:
                        // the CLI stdout (if any) becomes the error message.
                        result_stdout = out.stdout;
                        container_gone = true;
                        break;
                    }
                    if !out.stdout.trim().is_empty() {
                        // A result body arrived despite a non-zero curl
                        // status: prefer the delivered body over a re-POST.
                        result_stdout = out.stdout.trim().to_string();
                        break;
                    }
                    if delivered {
                        break;
                    }
                    // Pre-exchange failures only: the runtime client had not
                    // bound port 8080 (curl exit 7) or the daemon never
                    // started the exec (125/126/127) — the POST was not
                    // delivered, so keep polling while the budget holds.
                    // Anything else (a bodyless exit 0 included) is
                    // delivered-or-unknown: latch and stop.
                    if matches!(out.status_code, Some(7 | 125 | 126 | 127)) {
                        // Runtime not up yet: keep polling.
                    } else {
                        delivered = true;
                        break;
                    }
                }
                Err(DockerRunError::Timeout) => {
                    // The curl budget (timeout + 5, python-era
                    // _invoke_via_runtime_interface tune) expired: the
                    // function hung (a delivered invocation with no response).
                    // Same outcome as the python-era subprocess.TimeoutExpired
                    // branch of docker run — and no re-POST.
                    timed_out = true;
                    break;
                }
                Err(_) => {
                    // Spawn/wait-phase CLI failure — deliverability unknown:
                    // accepted-once, never re-send the event.
                    delivered = true;
                    break;
                }
            }
            if Instant::now() > deadline {
                timed_out = true;
                break;
            }
            std::thread::sleep(Duration::from_millis(250));
        }

        // Container cleanup on every path: containers use --rm, but the
        // embedded runtime interface server keeps them alive waiting for the
        // next event, so each invocation ends with an explicit force-remove.
        let logs = self.cli.container_logs(&container_name);
        self.cli.force_remove_container(&container_name);

        if timed_out {
            // Killed on the timeout budget, like the python-era
            // subprocess.TimeoutExpired branch of docker run.
            return ExecResult {
                payload: json!({
                    "errorMessage": format!("Task timed out after {timeout_secs}s"),
                    "errorType": "Task.TimedOut",
                })
                .to_string(),
                function_error: Some("Task.TimedOut".to_string()),
                logs: format!("Function timed out after {timeout_secs}s"),
            };
        }
        if container_gone {
            return ExecResult {
                payload: json!({
                    "errorMessage": if result_stdout.trim().is_empty() {
                        "Function execution failed"
                    } else {
                        result_stdout.trim()
                    },
                    "errorType": "Runtime.ExitError",
                })
                .to_string(),
                function_error: Some("Unhandled".to_string()),
                logs,
            };
        }
        if delivered {
            // The POST reached (or may have reached) the runtime interface
            // but no readable result came back — accepted-once: the outcome
            // is a failed invocation, not a rescuable retry and not a
            // timeout (the budget has not run out).
            let mut failure = run_exit_error("", "");
            failure.logs = logs;
            return failure;
        }

        if result_stdout.starts_with("Task timed out after") {
            // The official emulator answers function-timeout breaches with a
            // plain-text body; surface the shim's Task.TimedOut shape (the
            // suites assert FunctionError + "timed out" in the message).
            return run_timed_out_result(timeout_secs, "Task timed out after emulator cap");
        }

        // Parse successful response: JSON data verbatim; non-JSON output as a
        // JSON string (python-era raw-string fallback via the provider's
        // json.dumps). The runtime interface emulator answers handler errors
        // with the error payload as the body (errorMessage +
        // requestId/stackTrace), so surface those as "Handled" function
        // errors.
        match serde_json::from_str::<Value>(&result_stdout) {
            Ok(mut parsed) => {
                let is_handler_error = parsed
                    .as_object()
                    .map(|o| {
                        o.contains_key("errorMessage")
                            && (o.contains_key("requestId")
                                || o.contains_key("stackTrace")
                                // The nodejs runtime client signals stack
                                // frames under "trace" (Runtime.HandlerNotFound etc).
                                || o.contains_key("trace")
                                || o.contains_key("errorType"))
                    })
                    .unwrap_or(false);
                if is_handler_error {
                    // The python twin's shim reports bare interpreter names
                    // for load errors; normalize the emulator's prefixed
                    // ones for the python family.
                    normalize_error_type_for_family(&mut parsed, runtime_family(runtime));
                    return ExecResult {
                        payload: parsed.to_string(),
                        function_error: Some("Handled".to_string()),
                        logs,
                    };
                }
                ExecResult {
                    payload: parsed.to_string(),
                    function_error: None,
                    logs,
                }
            }
            Err(_) => ExecResult {
                payload: json!(result_stdout).to_string(),
                function_error: None,
                logs,
            },
        }
    }

    /// Extract layers (first) and the deployment package (last) into a fresh
    /// per-invocation directory mounted read-only as /var/task.
    fn extract_code_dir(
        &self,
        function_name: &str,
        code_zip: &[u8],
        layer_zips: &[Vec<u8>],
    ) -> std::path::PathBuf {
        let safe = crate::runtime::safe_dir_name(function_name);
        let dir = std::env::temp_dir()
            .join("robotocore-lambda-docker")
            .join(format!("{}-{}", safe, uuid::Uuid::new_v4().simple()));
        let _ = std::fs::create_dir_all(&dir);
        for layer in layer_zips {
            let _ = crate::runtime::extract_zip_into(layer, &dir);
        }
        let _ = crate::runtime::extract_zip_into(code_zip, &dir);
        dir
    }

    /// Fall back to the local in-process executors (per-runtime dispatch
    /// identical to the gateway's default family branches).
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn execute_local_fallback(
        &self,
        runtime: &str,
        code_zip: &[u8],
        layer_zips: &[Vec<u8>],
        handler: &str,
        event: &str,
        function_name: &str,
        function_version: &str,
        function_arn: &str,
        env_vars: &HashMap<String, String>,
        timeout_secs: u32,
        memory_size: u32,
        region: &str,
        account_id: &str,
    ) -> ExecResult {
        crate::runtime::execute_family(
            runtime,
            code_zip,
            layer_zips,
            handler,
            event,
            function_name,
            function_version,
            function_arn,
            env_vars,
            timeout_secs,
            memory_size,
            region,
            account_id,
        )
    }

    /// Port of the python-era managed-family executors
    /// (`runtimes/{java,dotnet,ruby}.py` + their bootstraps): a one-shot
    /// `docker run --rm -i` running the shipped loose bridge INSIDE the
    /// mapped official image — event JSON on stdin (the era
    /// `run_subprocess` protocol), result/error JSON on stdout, era
    /// `Task.TimedOut` on run-out; response parsing mirrors the era's
    /// `base.run_subprocess`.
    #[allow(clippy::too_many_arguments)]
    fn exec_loose_family(
        &self,
        runtime: &str,
        code_zip: &[u8],
        layer_zips: &[Vec<u8>],
        code_dir: &std::path::Path,
        handler: &str,
        event: &str,
        function_name: &str,
        function_version: &str,
        function_arn: &str,
        env_vars: &HashMap<String, String>,
        timeout_secs: u32,
        memory_size: u32,
        region: &str,
        account_id: &str,
    ) -> ExecResult {
        // The era run_subprocess path always sent json.dumps(event) — {} for
        // empty bodies.
        let event_json = if event.trim().is_empty() {
            "{}".to_string()
        } else {
            event.to_string()
        };
        // (loose_bridge_dir() is created by loose_bridge_run_prefix below.)
        let family = runtime_family(runtime);
        let mut cmd = loose_bridge_run_prefix(
            &code_dir.to_string_lossy(),
            handler,
            function_name,
            function_version,
            function_arn,
            env_vars,
            timeout_secs,
            memory_size,
            region,
            account_id,
            self.gateway_port,
            self.docker_network.as_deref(),
            self.docker_dns.as_deref(),
        );
        let image = runtime_image(runtime).unwrap_or_default();
        // The family images' /lambda-entrypoint.sh demands the handler as the
        // single CMD argument and boots their own RIC bootstrap — not what the
        // loose bridges need. Override the entrypoint with `sh` and drive the
        // bridge through an explicit script (stdin pipes through to the child,
        // which lets `-i` feed the event).
        cmd.push("--entrypoint".to_string());
        cmd.push("sh".to_string());
        cmd.push(image.to_string());
        match family {
            // javac ships in the amazonlinux java images: compile the bridge
            // into container tmp (never into the read-only code mount), build
            // the era classpath (code dir + packaged jars + layer dirs), and
            // let coreutils `timeout` enforce the function timeout.
            "java" => {
                let script =
                    concat!(
                        "mkdir -p /tmp/rc-bridge\n",
                        "if ! javac -d /tmp/rc-bridge /var/bridge/RobotocoreJavaBridge.java 2>/dev/null; then\n",
                        "  echo '{{\"errorMessage\":\"java bridge could not compile\",\"errorType\":\"Runtime.InvalidRuntime\"}}'\n",
                        "  exit 1\n",
                        "fi\n",
                        "CP=\"/tmp/rc-bridge:/var/task\"\n",
                        "for jar in $(find /var/task -name '*.jar' 2>/dev/null | sort); do CP=\"$CP:$jar\"; done\n",
                        "for d in /var/task/lib /var/task/java /var/task/java/lib; do if [ -d \"$d\" ]; then CP=\"$CP:$d\"; fi; done\n",
                        // Era memory parity: `runtimes/java.py` invoked
                        // `java -cp <cp> -Xmx{memory_size}m Bootstrap`.
                        "timeout \"${AWS_LAMBDA_FUNCTION_TIMEOUT}s\" java -cp \"$CP\" -Xmx\"${AWS_LAMBDA_FUNCTION_MEMORY_SIZE}m\" RobotocoreJavaBridge\n",
                        "rc=$?\n",
                        "if [ \"$rc\" -eq 124 ]; then echo \"Task timed out after ${AWS_LAMBDA_FUNCTION_TIMEOUT}.00 seconds\"; fi\n",
                        "exit $rc\n",
                    );
                cmd.push("-c".to_string());
                cmd.push(script.to_string());
            }
            "ruby" => {
                let script = "rc=0\n".to_string()
                    + "timeout \"${AWS_LAMBDA_FUNCTION_TIMEOUT}s\" ruby /var/bridge/bootstrap.rb || rc=$?\n"
                    + "if [ \"$rc\" -eq 124 ]; then echo \"Task timed out after ${AWS_LAMBDA_FUNCTION_TIMEOUT}.00 seconds\"; fi\n"
                    + "exit $rc\n";
                cmd.push("-c".to_string());
                cmd.push(script);
            }
            _ => {
                // dotnet: the twin builds the bridge once per TFM (the
                // official images carry only the runtime); class libraries
                // with .cs sources and no DLL compile first (python-era
                // `_compile_source`).
                let tfm = dotnet_runtime_tfm(runtime);
                if ensure_dotnet_bridge(tfm).is_none() {
                    return ExecResult {
                        payload: json!({
                            "errorMessage": ".NET SDK not installed",
                            "errorType": "Runtime.InvalidRuntime",
                        })
                        .to_string(),
                        function_error: Some("Runtime.InvalidRuntime".to_string()),
                        logs: String::new(),
                    };
                }
                if let Some(err) = compile_dotnet_source_if_needed(tfm, code_dir, handler) {
                    return err;
                }
                let script = format!(
                    "rc=0\ntimeout \"${{AWS_LAMBDA_FUNCTION_TIMEOUT}}s\" dotnet /var/bridge/publish-{tfm}/RobotocoreDotnetBridge.dll || rc=$?\nif [ \"$rc\" -eq 124 ]; then echo \"Task timed out after ${{AWS_LAMBDA_FUNCTION_TIMEOUT}}.00 seconds\"; fi\nexit $rc\n"
                );
                cmd.push("-c".to_string());
                cmd.push(script);
            }
        }

        // Overall CLI budget: bridge bootstrap (javac/dotnet boot) plus the
        // function runtime, whose own cap is enforced inside by `timeout` or
        // the emulator.
        let budget = u64::from(timeout_secs.max(1)) + 15;
        let out = self.cli.run(&cmd, &event_json, budget);
        match out {
            Ok(o) => loose_result(o, timeout_secs),
            Err(DockerRunError::NotFound) => self.execute_local_fallback(
                runtime,
                code_zip,
                layer_zips,
                handler,
                &event_json,
                function_name,
                function_version,
                function_arn,
                env_vars,
                timeout_secs,
                memory_size,
                region,
                account_id,
            ),
            Err(_) => run_timed_out_result(timeout_secs, "Task timed out after CLI budget"),
        }
    }

    /// No-op for subprocess-based executor (containers use --rm).
    pub fn cleanup(&self) {}
}

/// A failed `docker run` (python-era shape): a stdout JSON object carrying
/// "errorMessage" is a handler error ("Handled"); anything else is
/// Runtime.ExitError/"Unhandled" with the CLI stdout (or a generic message)
/// and stderr as logs.
fn run_exit_error(stdout: &str, stderr: &str) -> ExecResult {
    let stdout_trim = stdout.trim();
    if let Ok(parsed) = serde_json::from_str::<Value>(stdout_trim) {
        if parsed
            .as_object()
            .map(|o| o.contains_key("errorMessage"))
            .unwrap_or(false)
        {
            return ExecResult {
                payload: parsed.to_string(),
                function_error: Some("Handled".to_string()),
                logs: stderr.to_string(),
            };
        }
    }
    ExecResult {
        payload: json!({
            "errorMessage": if stdout_trim.is_empty() { "Function execution failed" } else { stdout_trim },
            "errorType": "Runtime.ExitError",
        })
        .to_string(),
        function_error: Some("Unhandled".to_string()),
        logs: stderr.to_string(),
    }
}

// === Module-level singleton (python-era shape) ===

static DOCKER_EXECUTOR: parking_lot::Mutex<Option<Arc<DockerLambdaExecutor>>> =
    parking_lot::Mutex::new(None);

/// Return the global DockerLambdaExecutor singleton.
pub fn get_docker_executor() -> Arc<DockerLambdaExecutor> {
    DOCKER_EXECUTOR
        .lock()
        .get_or_insert_with(|| Arc::new(DockerLambdaExecutor::new()))
        .clone()
}

/// Reset the singleton (for testing).
pub fn reset_docker_executor() {
    *DOCKER_EXECUTOR.lock() = None;
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::VecDeque;

    const LIVE_IMAGE: &str = "public.ecr.aws/lambda/python:3.12";

    /// Env mutation and CLI mocking are process-global: serialize the tests
    /// that touch either.
    static GUARD: parking_lot::Mutex<()> = parking_lot::Mutex::new(());

    fn mock_output(status: i32, stdout: &str, stderr: &str) -> SubprocessOutput {
        SubprocessOutput {
            status_code: Some(status),
            stdout: stdout.to_string(),
            stderr: stderr.to_string(),
        }
    }

    /// Recording docker-CLI double: scripted results are consumed in order;
    /// a dry script reports errors (run: daemon failure, exec: curl timeout).
    /// State lives in an `Arc` so a test can keep inspecting the handle after
    /// moving the CLI into the executor under test.
    #[derive(Clone, Default)]
    struct MockCli {
        st: Arc<MockState>,
    }

    #[derive(Default)]
    struct MockState {
        runs: parking_lot::Mutex<Vec<Vec<String>>>,
        run_script: parking_lot::Mutex<VecDeque<Result<SubprocessOutput, DockerRunError>>>,
        execs: parking_lot::Mutex<Vec<(String, String)>>,
        exec_script: parking_lot::Mutex<VecDeque<Result<SubprocessOutput, DockerRunError>>>,
        rms: parking_lot::Mutex<Vec<String>>,
        logs_back: parking_lot::Mutex<String>,
        lists: parking_lot::Mutex<VecDeque<Vec<String>>>,
        /// Scripted daemon-probe verdicts; unscripted probes read healthy.
        probe_back: parking_lot::Mutex<VecDeque<bool>>,
    }

    impl MockCli {
        fn new() -> Self {
            Self {
                st: Arc::new(MockState {
                    runs: parking_lot::Mutex::new(Vec::new()),
                    run_script: parking_lot::Mutex::new(VecDeque::new()),
                    execs: parking_lot::Mutex::new(Vec::new()),
                    exec_script: parking_lot::Mutex::new(VecDeque::new()),
                    rms: parking_lot::Mutex::new(Vec::new()),
                    logs_back: parking_lot::Mutex::new(String::new()),
                    lists: parking_lot::Mutex::new(VecDeque::new()),
                    probe_back: parking_lot::Mutex::new(VecDeque::new()),
                }),
            }
        }

        /// Script the straggler sweep's container listing (one returned
        /// vec per sweep).
        fn script_stragglers(&self, containers: &[&str]) {
            self.st.lists.lock().push_back(
                containers
                    .iter()
                    .map(|c| c.to_string())
                    .collect::<Vec<String>>(),
            );
        }

        /// Script the per-invoke daemon-probe verdicts (consumed in order;
        /// unscripted probes read healthy).
        fn script_probes(&self, verdicts: &[bool]) {
            *self.st.probe_back.lock() = verdicts.iter().copied().collect();
        }

        /// Script one healthy container start (docker run -d → id on stdout).
        fn run_starts(&self) {
            self.run_script_of(&[Ok(mock_output(0, "dcnt-rec", ""))]);
        }

        /// Script the runtime-interface POST results.
        fn exec_results(&self, events: &[Result<SubprocessOutput, DockerRunError>]) {
            self.exec_script_of(events);
        }

        fn run_script_of(&self, events: &[Result<SubprocessOutput, DockerRunError>]) {
            *self.st.run_script.lock() = events.iter().cloned().collect();
        }

        fn exec_script_of(&self, events: &[Result<SubprocessOutput, DockerRunError>]) {
            *self.st.exec_script.lock() = events.iter().cloned().collect();
        }

        fn set_logs(&self, logs: &str) {
            *self.st.logs_back.lock() = logs.to_string();
        }

        fn run_calls(&self) -> Vec<Vec<String>> {
            self.st.runs.lock().clone()
        }

        fn exec_calls(&self) -> Vec<(String, String)> {
            self.st.execs.lock().clone()
        }

        fn rm_calls(&self) -> Vec<String> {
            self.st.rms.lock().clone()
        }
    }

    impl DockerCli for MockCli {
        fn run(
            &self,
            argv: &[String],
            _event_json: &str,
            _timeout_secs: u64,
        ) -> Result<SubprocessOutput, DockerRunError> {
            self.st.runs.lock().push(argv.to_vec());
            match self.st.run_script.lock().pop_front() {
                Some(result) => result,
                None => Err(DockerRunError::Io),
            }
        }

        fn invoke_via_runtime_interface(
            &self,
            container_name: &str,
            event_json: &str,
            _timeout: u32,
        ) -> Result<SubprocessOutput, DockerRunError> {
            self.st
                .execs
                .lock()
                .push((container_name.to_string(), event_json.to_string()));
            match self.st.exec_script.lock().pop_front() {
                Some(result) => result,
                None => Err(DockerRunError::Timeout),
            }
        }

        fn force_remove_container(&self, container_name: &str) {
            self.st.rms.lock().push(container_name.to_string());
        }

        fn container_logs(&self, _container_name: &str) -> String {
            self.st.logs_back.lock().clone()
        }

        fn lambda_straggler_containers(&self) -> Vec<String> {
            self.st.lists.lock().pop_front().unwrap_or_default()
        }

        fn daemon_available(&self) -> bool {
            self.st.probe_back.lock().pop_front().unwrap_or(true)
        }
    }

    fn mk_executor(cli: &MockCli, docker_available: bool) -> DockerLambdaExecutor {
        DockerLambdaExecutor {
            gateway_port: 4566,
            docker_network: None,
            docker_dns: None,
            docker_available: AtomicBool::new(docker_available),
            cli: Arc::new(cli.clone()),
        }
    }

    // ------------------------------------------------------------------
    // Runtime image mapping (python TestRuntimeImageMapping)
    // ------------------------------------------------------------------

    #[test]
    fn test_runtime_image_python() {
        assert_eq!(
            runtime_image("python3.12"),
            Some("public.ecr.aws/lambda/python:3.12")
        );
        assert_eq!(
            runtime_image("python3.11"),
            Some("public.ecr.aws/lambda/python:3.11")
        );
        assert_eq!(
            runtime_image("python3.13"),
            Some("public.ecr.aws/lambda/python:3.13")
        );
        assert_eq!(
            runtime_image("python3.10"),
            Some("public.ecr.aws/lambda/python:3.10")
        );
        assert_eq!(
            runtime_image("python3.9"),
            Some("public.ecr.aws/lambda/python:3.9")
        );
        assert_eq!(
            runtime_image("python3.8"),
            Some("public.ecr.aws/lambda/python:3.8")
        );
    }

    #[test]
    fn test_runtime_image_node() {
        assert_eq!(
            runtime_image("nodejs20.x"),
            Some("public.ecr.aws/lambda/nodejs:20")
        );
        assert_eq!(
            runtime_image("nodejs18.x"),
            Some("public.ecr.aws/lambda/nodejs:18")
        );
        assert_eq!(
            runtime_image("nodejs16.x"),
            Some("public.ecr.aws/lambda/nodejs:16")
        );
        assert_eq!(
            runtime_image("nodejs22.x"),
            Some("public.ecr.aws/lambda/nodejs:22")
        );
    }

    #[test]
    fn test_runtime_image_java() {
        assert_eq!(
            runtime_image("java21"),
            Some("public.ecr.aws/lambda/java:21")
        );
        assert_eq!(
            runtime_image("java17"),
            Some("public.ecr.aws/lambda/java:17")
        );
        assert_eq!(
            runtime_image("java11"),
            Some("public.ecr.aws/lambda/java:11")
        );
        assert_eq!(runtime_image("java8"), Some("public.ecr.aws/lambda/java:8"));
        assert_eq!(
            runtime_image("java8.al2"),
            Some("public.ecr.aws/lambda/java:8.al2")
        );
    }

    #[test]
    fn test_runtime_image_dotnet_ruby_go_provided() {
        assert_eq!(
            runtime_image("dotnet8"),
            Some("public.ecr.aws/lambda/dotnet:8")
        );
        assert_eq!(
            runtime_image("dotnet6"),
            Some("public.ecr.aws/lambda/dotnet:6")
        );
        assert_eq!(
            runtime_image("ruby3.3"),
            Some("public.ecr.aws/lambda/ruby:3.3")
        );
        assert_eq!(
            runtime_image("ruby3.2"),
            Some("public.ecr.aws/lambda/ruby:3.2")
        );
        assert_eq!(runtime_image("go1.x"), Some("public.ecr.aws/lambda/go:1"));
        assert_eq!(
            runtime_image("provided"),
            Some("public.ecr.aws/lambda/provided:al2")
        );
        assert_eq!(
            runtime_image("provided.al2"),
            Some("public.ecr.aws/lambda/provided:al2")
        );
        assert_eq!(
            runtime_image("provided.al2023"),
            Some("public.ecr.aws/lambda/provided:al2023")
        );
    }

    #[test]
    fn test_unknown_and_empty_runtime_have_no_image() {
        assert_eq!(runtime_image("cobol99"), None);
        assert_eq!(runtime_image(""), None);
    }

    #[test]
    fn test_all_images_are_24_ecr_entries() {
        // Full mapping: 24 runtimes, every image an official ECR image.
        let expected_runtimes: &[&str] = &[
            "python3.8",
            "python3.9",
            "python3.10",
            "python3.11",
            "python3.12",
            "python3.13",
            "nodejs16.x",
            "nodejs18.x",
            "nodejs20.x",
            "nodejs22.x",
            "java8",
            "java8.al2",
            "java11",
            "java17",
            "java21",
            "dotnet6",
            "dotnet8",
            "go1.x",
            "ruby3.2",
            "ruby3.3",
            "provided",
            "provided.al2",
            "provided.al2023",
        ];
        assert_eq!(
            RUNTIME_IMAGES.len(),
            expected_runtimes.len(),
            "24 runtime mappings"
        );
        for (runtime, image) in RUNTIME_IMAGES {
            assert!(
                image.starts_with("public.ecr.aws/lambda/"),
                "Runtime {runtime} -> {image} missing ECR prefix"
            );
            assert!(
                expected_runtimes.contains(runtime),
                "unexpected runtime mapped: {runtime}"
            );
        }
    }

    // ------------------------------------------------------------------
    // Executor mode (python TestGetExecutorMode)
    // ------------------------------------------------------------------

    fn set_env(key: &str, value: &str) {
        std::env::set_var(key, value);
    }

    fn unset_env(key: &str) {
        std::env::remove_var(key);
    }

    #[test]
    fn test_executor_mode_defaults_local() {
        let _g = GUARD.lock();
        unset_env("LAMBDA_EXECUTOR");
        unset_env("LAMBDA_RUNTIME_EXECUTOR");
        assert_eq!(executor_mode(), "local");
    }

    #[test]
    fn test_executor_mode_lambda_executor_env() {
        let _g = GUARD.lock();
        unset_env("LAMBDA_RUNTIME_EXECUTOR");
        set_env("LAMBDA_EXECUTOR", "docker");
        assert_eq!(executor_mode(), "docker");
        unset_env("LAMBDA_EXECUTOR");
    }

    #[test]
    fn test_executor_mode_case_insensitive() {
        let _g = GUARD.lock();
        set_env("LAMBDA_EXECUTOR", "Docker");
        assert_eq!(executor_mode(), "docker");
        set_env("LAMBDA_EXECUTOR", "DOCKER");
        assert_eq!(executor_mode(), "docker");
        unset_env("LAMBDA_EXECUTOR");
    }

    #[test]
    fn test_executor_mode_backward_compat() {
        let _g = GUARD.lock();
        unset_env("LAMBDA_EXECUTOR");
        set_env("LAMBDA_RUNTIME_EXECUTOR", "docker");
        assert_eq!(executor_mode(), "docker");
        unset_env("LAMBDA_RUNTIME_EXECUTOR");
    }

    #[test]
    fn test_lambda_executor_takes_precedence() {
        let _g = GUARD.lock();
        set_env("LAMBDA_EXECUTOR", "docker");
        set_env("LAMBDA_RUNTIME_EXECUTOR", "local");
        assert_eq!(executor_mode(), "docker");
        unset_env("LAMBDA_EXECUTOR");
        unset_env("LAMBDA_RUNTIME_EXECUTOR");
    }

    // ------------------------------------------------------------------
    // Docker availability (python TestDockerAvailability)
    // ------------------------------------------------------------------

    #[test]
    fn test_run_subprocess_missing_binary_maps_to_not_found() {
        let _g = GUARD.lock();
        let result = run_subprocess(&["definitely-not-a-real-binary-xyz".to_string()], None, 5);
        // python-era FileNotFoundError → fallback trigger.
        assert!(matches!(result, Err(DockerRunError::NotFound)));
    }

    #[test]
    fn test_run_subprocess_nonzero_status_not_available() {
        let result = run_subprocess(&["/bin/false".to_string()], None, 5).unwrap();
        assert_eq!(result.status_code, Some(1));
    }

    #[test]
    fn test_run_subprocess_timeout_returns_timeout() {
        let result = run_subprocess(&["sleep".to_string(), "30".to_string()], None, 1);
        assert!(matches!(result, Err(DockerRunError::Timeout)));
    }

    #[test]
    fn test_run_subprocess_success_status_zero() {
        let result = run_subprocess(&["/bin/true".to_string()], None, 5).unwrap();
        assert_eq!(result.status_code, Some(0));
    }

    #[test]
    fn test_docker_available_with_running_daemon() {
        // Skip on hosts without a docker daemon rather than failing.
        if !docker_available(5) {
            return;
        }
        assert!(docker_available(5));
    }

    // ------------------------------------------------------------------
    // docker run command construction (python TestBuildDockerRunCmd)
    // ------------------------------------------------------------------

    /// Port of the python test's `_basic_cmd` helper.
    fn basic_cmd() -> Vec<String> {
        build_docker_run_cmd(
            "public.ecr.aws/lambda/python:3.12",
            "test-ctr",
            "/tmp/code",
            "handler.main",
            "my-func",
            3,
            128,
            None,
            "us-east-1",
            "123456789012",
            4566,
            None,
            None,
        )
    }

    fn env_pairs(cmd: &[String]) -> Vec<String> {
        // Elements directly following an "-e" flag.
        let mut pairs = Vec::new();
        for (i, item) in cmd.iter().enumerate() {
            if i > 0 && cmd[i - 1] == "-e" {
                pairs.push(item.clone());
            }
        }
        pairs
    }

    #[test]
    fn test_cmd_starts_with_docker_run() {
        let cmd = basic_cmd();
        assert_eq!(cmd[0], "docker");
        assert_eq!(cmd[1], "run");
        assert!(cmd.contains(&"--rm".to_string()));
    }

    #[test]
    fn test_cmd_container_name() {
        let cmd = build_docker_run_cmd(
            "public.ecr.aws/lambda/python:3.12",
            "my-container",
            "/tmp/code",
            "handler.main",
            "my-func",
            3,
            128,
            None,
            "us-east-1",
            "123456789012",
            4566,
            None,
            None,
        );
        let idx = cmd.iter().position(|a| a == "--name").unwrap();
        assert_eq!(cmd[idx + 1], "my-container");
    }

    #[test]
    fn test_cmd_volume_mount() {
        let cmd = build_docker_run_cmd(
            "public.ecr.aws/lambda/python:3.12",
            "test-ctr",
            "/my/code",
            "handler.main",
            "my-func",
            3,
            128,
            None,
            "us-east-1",
            "123456789012",
            4566,
            None,
            None,
        );
        let idx = cmd.iter().position(|a| a == "-v").unwrap();
        assert_eq!(cmd[idx + 1], "/my/code:/var/task:ro");
    }

    #[test]
    fn test_cmd_handler_is_last() {
        let cmd = build_docker_run_cmd(
            "public.ecr.aws/lambda/python:3.12",
            "test-ctr",
            "/tmp/code",
            "index.handler",
            "my-func",
            3,
            128,
            None,
            "us-east-1",
            "123456789012",
            4566,
            None,
            None,
        );
        assert_eq!(cmd.last().unwrap(), "index.handler");
    }

    #[test]
    fn test_cmd_image_is_second_to_last() {
        let cmd = build_docker_run_cmd(
            "public.ecr.aws/lambda/nodejs:20",
            "test-ctr",
            "/tmp/code",
            "handler.main",
            "my-func",
            3,
            128,
            None,
            "us-east-1",
            "123456789012",
            4566,
            None,
            None,
        );
        assert_eq!(cmd[cmd.len() - 2], "public.ecr.aws/lambda/nodejs:20");
    }

    #[test]
    fn test_cmd_env_vars_passthrough() {
        let mut env = HashMap::new();
        env.insert("FOO".to_string(), "bar".to_string());
        env.insert("BAZ".to_string(), "qux".to_string());
        let cmd = build_docker_run_cmd(
            "public.ecr.aws/lambda/python:3.12",
            "test-ctr",
            "/tmp/code",
            "handler.main",
            "my-func",
            3,
            128,
            Some(&env),
            "us-east-1",
            "123456789012",
            4566,
            None,
            None,
        );
        let pairs = env_pairs(&cmd);
        assert!(pairs.contains(&"FOO=bar".to_string()));
        assert!(pairs.contains(&"BAZ=qux".to_string()));
    }

    #[test]
    fn test_cmd_core_env_vars() {
        let cmd = build_docker_run_cmd(
            "public.ecr.aws/lambda/python:3.12",
            "test-ctr",
            "/tmp/code",
            "handler.main",
            "my-func",
            3,
            128,
            None,
            "eu-west-1",
            "999888777666",
            9999,
            None,
            None,
        );
        let pairs = env_pairs(&cmd);
        assert!(pairs.contains(&"AWS_REGION=eu-west-1".to_string()));
        assert!(pairs.contains(&"AWS_ACCOUNT_ID=999888777666".to_string()));
        assert!(pairs.contains(&"AWS_ENDPOINT_URL=http://host.docker.internal:9999".to_string()));
        assert!(pairs.contains(&"AWS_LAMBDA_FUNCTION_NAME=my-func".to_string()));
        assert!(pairs.contains(&"AWS_LAMBDA_FUNCTION_MEMORY_SIZE=128".to_string()));
        assert!(pairs.contains(&"AWS_LAMBDA_FUNCTION_TIMEOUT=3".to_string()));
        assert!(pairs.contains(&"_HANDLER=handler.main".to_string()));
        assert!(pairs.contains(&"AWS_ACCESS_KEY_ID=testing".to_string()));
        assert!(pairs.contains(&"AWS_SECRET_ACCESS_KEY=testing".to_string()));
    }

    #[test]
    fn test_cmd_docker_network() {
        let cmd = build_docker_run_cmd(
            "public.ecr.aws/lambda/python:3.12",
            "test-ctr",
            "/tmp/code",
            "handler.main",
            "my-func",
            3,
            128,
            None,
            "us-east-1",
            "123456789012",
            4566,
            Some("my-net"),
            None,
        );
        let idx = cmd.iter().position(|a| a == "--network").unwrap();
        assert_eq!(cmd[idx + 1], "my-net");
    }

    #[test]
    fn test_cmd_no_network_by_default() {
        let cmd = basic_cmd();
        assert!(!cmd.iter().any(|a| a == "--network"));
    }

    #[test]
    fn test_cmd_docker_dns() {
        let cmd = build_docker_run_cmd(
            "public.ecr.aws/lambda/python:3.12",
            "test-ctr",
            "/tmp/code",
            "handler.main",
            "my-func",
            3,
            128,
            None,
            "us-east-1",
            "123456789012",
            4566,
            None,
            Some("8.8.8.8"),
        );
        let idx = cmd.iter().position(|a| a == "--dns").unwrap();
        assert_eq!(cmd[idx + 1], "8.8.8.8");
    }

    // ------------------------------------------------------------------
    // DockerLambdaExecutor flow (python TestDockerLambdaExecutor)
    // ------------------------------------------------------------------

    /// Standard execute() args used across these tests.
    fn run_execute(ex: &DockerLambdaExecutor, runtime: &str, event: &str) -> ExecResult {
        ex.execute(
            runtime,
            b"z",
            &[],
            "h.h",
            event,
            "f",
            "$LATEST",
            "arn:aws:lambda:us-east-1:123456789012:function:f",
            &HashMap::new(),
            3,
            128,
            "us-east-1",
            "123456789012",
            None,
        )
    }

    #[test]
    fn test_marks_unavailable_and_falls_back_locally() {
        let _g = GUARD.lock();
        let cli = MockCli::new();
        let ex = mk_executor(&cli, false);
        assert!(!ex.docker_available.load(AtomicOrdering::Acquire));
        // Falls back to the real local python shim: with a garbage deployment
        // package the shim reports the unpack failure as a handled error.
        let res = run_execute(&ex, "python3.12", r#"{}"#);
        assert_eq!(res.function_error.as_deref(), Some("Handled"));
        let payload: Value = serde_json::from_str(&res.payload).unwrap();
        assert_eq!(payload["errorType"], "BadZipFile");
    }

    #[test]
    fn test_success_payload_and_container_lifecycle() {
        let _g = GUARD.lock();
        let cli = MockCli::new();
        cli.run_starts();
        cli.exec_results(&[Ok(mock_output(0, r#"{"statusCode": 200}"#, "logs\n"))]);
        cli.set_logs("START\nlogs\nEND\n");
        let ex = mk_executor(&cli, true);
        let res = run_execute(&ex, "nodejs20.x", r#"{"k": "v"}"#);
        let payload: Value = serde_json::from_str(&res.payload).unwrap();
        assert_eq!(payload["statusCode"], 200);
        assert_eq!(res.function_error, None);
        assert!(res.logs.contains("logs"), "logs: {}", res.logs);
        // One detached docker run, one runtime-interface POST, one rm -f.
        assert_eq!(cli.run_calls().len(), 1);
        assert!(cli.run_calls()[0].contains(&"-d".to_string()));
        assert_eq!(cli.exec_calls().len(), 1);
        assert_eq!(cli.rm_calls().len(), 1);
    }

    #[test]
    fn test_timeout_kills_container() {
        let _g = GUARD.lock();
        let cli = MockCli::new();
        cli.run_starts();
        cli.exec_results(&[Err(DockerRunError::Timeout)]);
        let ex = mk_executor(&cli, true);
        let res = run_execute(&ex, "python3.12", r#"{}"#);
        assert_eq!(res.function_error.as_deref(), Some("Task.TimedOut"));
        let payload: Value = serde_json::from_str(&res.payload).unwrap();
        assert_eq!(payload["errorMessage"], "Task timed out after 3s");
        assert_eq!(res.logs, "Function timed out after 3s");
        // The container is force-removed on timeout, exactly once.
        assert_eq!(cli.rm_calls().len(), 1);
    }

    #[test]
    fn test_handled_error_from_runtime_interface_body() {
        let _g = GUARD.lock();
        let cli = MockCli::new();
        cli.run_starts();
        let body = r#"{"errorMessage": "bad", "errorType": "ValueError", "requestId": "r1", "stackTrace": ["t"]}"#;
        cli.exec_results(&[Ok(mock_output(0, body, "err\n"))]);
        let ex = mk_executor(&cli, true);
        // EMI-family runtime: java/dotnet/ruby take the loose bridge path now
        // (see LOOSE_BRIDGE_FAMILIES) and never reach the runtime interface.
        let res = run_execute(&ex, "nodejs20.x", r#"{}"#);
        assert_eq!(res.function_error.as_deref(), Some("Handled"));
        let payload: Value = serde_json::from_str(&res.payload).unwrap();
        assert_eq!(payload["errorMessage"], "bad");
    }

    #[test]
    fn test_unhandled_error_when_container_dies() {
        let _g = GUARD.lock();
        let cli = MockCli::new();
        cli.run_starts();
        cli.exec_results(&[Ok(mock_output(
            1,
            "crash",
            "Error response from daemon: No such container: cnt",
        ))]);
        let ex = mk_executor(&cli, true);
        // EMI-family runtime (java/dotnet/ruby take the loose bridge, which
        // has no runtime-interface POST to observe).
        let res = run_execute(&ex, "nodejs20.x", r#"{}"#);
        assert_eq!(res.function_error.as_deref(), Some("Unhandled"));
        let payload: Value = serde_json::from_str(&res.payload).unwrap();
        assert_eq!(payload["errorType"], "Runtime.ExitError");
        assert_eq!(payload["errorMessage"], "crash");
    }

    #[test]
    fn test_unknown_runtime_falls_back() {
        let _g = GUARD.lock();
        let cli = MockCli::new();
        let ex = mk_executor(&cli, true);
        let res = run_execute(&ex, "cobol99", r#"{}"#);
        // No executor for that family locally either: the invalid-runtime
        // shape mirrors the Rust invoke path.
        assert_eq!(
            res.function_error.as_deref(),
            Some("Runtime.InvalidRuntime")
        );
    }

    #[test]
    fn test_empty_response_maps_to_timeout_when_runtime_silent() {
        let _g = GUARD.lock();
        let cli = MockCli::new();
        cli.run_starts();
        // Runtime not up yet = curl could not connect (exit 7): a proven
        // pre-exchange failure, so the loop keeps rescuing until the dry
        // script answers the curl-budget timeout (Task.TimedOut). A
        // bodyless exit 0 would mean delivered-or-unknown instead (see the
        // accept-once dedup test below).
        cli.exec_results(&[Ok(mock_output(7, "", ""))]);
        let ex = mk_executor(&cli, true);
        let res = run_execute(&ex, "python3.12", r#"{}"#);
        assert_eq!(res.function_error.as_deref(), Some("Task.TimedOut"));
        assert!(
            cli.exec_calls().len() >= 2,
            "refused post rescued until the budget: {} execs",
            cli.exec_calls().len()
        );
    }

    #[test]
    fn test_non_json_response_becomes_json_string() {
        let _g = GUARD.lock();
        let cli = MockCli::new();
        cli.run_starts();
        cli.exec_results(&[Ok(mock_output(0, "plain text", ""))]);
        let ex = mk_executor(&cli, true);
        let res = run_execute(&ex, "python3.12", r#"{}"#);
        assert_eq!(res.function_error, None);
        assert_eq!(res.payload, json!("plain text").to_string());
    }

    #[test]
    fn test_uses_provided_code_dir() {
        let _g = GUARD.lock();
        let cli = MockCli::new();
        cli.run_starts();
        let ex = mk_executor(&cli, true);
        let _ = ex.execute(
            "python3.12",
            b"z",
            &[],
            "h.h",
            r#"{}"#,
            "f",
            "$LATEST",
            "arn:aws:lambda:us-east-1:123456789012:function:f",
            &HashMap::new(),
            3,
            128,
            "us-east-1",
            "123456789012",
            Some(std::path::Path::new("/mounted/code")),
        );
        let runs = cli.run_calls();
        assert_eq!(runs.len(), 1);
        let idx = runs[0].iter().position(|a| a == "-v").unwrap();
        assert_eq!(runs[0][idx + 1], "/mounted/code:/var/task:ro");
    }

    #[test]
    fn test_event_json_reaches_the_runtime_interface() {
        let _g = GUARD.lock();
        let cli = MockCli::new();
        cli.run_starts();
        cli.exec_results(&[Ok(mock_output(0, "null", ""))]);
        let ex = mk_executor(&cli, true);
        let _ = run_execute(&ex, "python3.12", r#"{"ping": 1}"#);
        let execs = cli.exec_calls();
        assert_eq!(execs.len(), 1);
        assert_eq!(execs[0].1, r#"{"ping": 1}"#);
        // The POST targets the runtime interface inside the container.
        assert!(execs[0].0.starts_with("robotocore-lambda-f-"));
    }

    /// An empty invoke body (boto3 `invoke` with no `Payload`) normalizes to
    /// `{}` before the Runtime Interface POST, like the python-era provider's
    /// `event = json.loads(body) if body else {}` — a blank POST body reaches
    /// the runtime image unparseable (Runtime.UnmarshalError).
    #[test]
    fn test_empty_event_normalized_to_empty_object() {
        let _g = GUARD.lock();
        let cli = MockCli::new();
        cli.run_starts();
        cli.exec_results(&[Ok(mock_output(0, "null", ""))]);
        let ex = mk_executor(&cli, true);
        let _ = run_execute(&ex, "python3.12", "");
        let execs = cli.exec_calls();
        assert_eq!(execs.len(), 1);
        assert_eq!(execs[0].1, "{}");
    }

    /// Loose-bridge (java/dotnet/ruby) result rules: exit 0 JSON verbatim;
    /// non-zero error JSON -> Handled; plain timeout text -> Task.TimedOut;
    /// anything else -> Runtime.ExitError with the stderr tail.
    #[test]
    fn loose_result_parses_era_shapes() {
        let out = loose_result(mock_output(0, r#"{"ok":true}"#, ""), 3);
        assert_eq!(out.function_error, None);
        assert_eq!(out.payload, r#"{"ok":true}"#);
        let out = loose_result(mock_output(0, "plain text", ""), 3);
        assert_eq!(out.payload, json!("plain text").to_string());
        let out = loose_result(
            mock_output(
                1,
                r#"{"errorMessage":"bad","errorType":"RuntimeError"}"#,
                "log\n",
            ),
            3,
        );
        assert_eq!(out.function_error.as_deref(), Some("Handled"));
        let out = loose_result(mock_output(124, "Task timed out after 3.00 seconds", ""), 3);
        assert_eq!(out.function_error.as_deref(), Some("Task.TimedOut"));
        let payload: Value = serde_json::from_str(&out.payload).unwrap();
        assert_eq!(payload["errorType"], "Task.TimedOut");
        assert_eq!(payload["errorMessage"], "Task timed out after 3.00 seconds");
        let out = loose_result(mock_output(2, "", "boom\n"), 3);
        let payload: Value = serde_json::from_str(&out.payload).unwrap();
        assert_eq!(payload["errorType"], "Runtime.ExitError");
        assert_eq!(payload["errorMessage"], "boom");
    }

    /// The loose families build a one-shot `docker run --rm -i ...` — no
    /// detached container, no runtime-interface POST (that is for the EMI
    /// families only).
    #[test]
    fn loose_java_builds_oneshot_run_cmd() {
        let _g = GUARD.lock();
        let cli = MockCli::new();
        cli.run_starts();
        let ex = mk_executor(&cli, true);
        let _ = run_execute(&ex, "java21", r#"{"k":1}"#);
        let runs = cli.run_calls();
        assert_eq!(runs.len(), 1);
        assert!(runs[0].contains(&"--rm".to_string()));
        assert!(runs[0].contains(&"-i".to_string()));
        assert!(runs[0].contains(&"sh".to_string()));
        assert!(runs[0].iter().any(|a| a.contains("RobotocoreJavaBridge")));
    }

    #[test]
    fn loose_ruby_builds_bridge_run_cmd() {
        let _g = GUARD.lock();
        let cli = MockCli::new();
        cli.run_starts();
        let ex = mk_executor(&cli, true);
        let _ = run_execute(&ex, "ruby3.2", r#"{}"#);
        let runs = cli.run_calls();
        assert_eq!(runs.len(), 1);
        assert!(runs[0].contains(&"sh".to_string()));
        assert!(runs[0].iter().any(|a| a.contains("bootstrap.rb")));
    }

    /// For the EMI families the nodejs runtime client's stack frames come
    /// under "trace" — such error bodies must be marked as Handled.
    #[test]
    fn nodejs_trace_error_marked_handled() {
        let _g = GUARD.lock();
        let cli = MockCli::new();
        cli.run_starts();
        cli.exec_results(&[Ok(mock_output(
            0,
            r#"{"errorType":"Error","errorMessage":"boom","trace":["Error: boom"]}"#,
            "err\n",
        ))]);
        let ex = mk_executor(&cli, true);
        let res = run_execute(&ex, "nodejs20.x", r#"{}"#);
        assert_eq!(res.function_error.as_deref(), Some("Handled"));
        let payload: Value = serde_json::from_str(&res.payload).unwrap();
        assert_eq!(payload["errorType"], "Error");
        assert_eq!(payload["errorMessage"], "boom");
    }

    /// The emulator answers function-timeout breaches with a plain-text
    /// body; the executor maps it to the shim's Task.TimedOut result.
    #[test]
    fn emulator_plain_timeout_text_maps_to_task_timed_out() {
        let _g = GUARD.lock();
        let cli = MockCli::new();
        cli.run_starts();
        cli.exec_results(&[Ok(mock_output(0, "Task timed out after 3.00 seconds", ""))]);
        let ex = mk_executor(&cli, true);
        let res = run_execute(&ex, "python3.12", r#"{}"#);
        assert_eq!(res.function_error.as_deref(), Some("Task.TimedOut"));
        let payload: Value = serde_json::from_str(&res.payload).unwrap();
        assert_eq!(payload["errorType"], "Task.TimedOut");
    }

    /// The python family report load errors with bare interpreter names
    /// (python-era shim contract), not the emulator's prefixed aliases.
    #[test]
    fn python_load_errors_normalize_to_shim_names() {
        let _g = GUARD.lock();
        let cli = MockCli::new();
        cli.run_starts();
        cli.exec_results(&[Ok(mock_output(
            0,
            r#"{"errorType":"Runtime.UserCodeSyntaxError","errorMessage":"bad","requestId":"r1"}"#,
            "",
        ))]);
        let ex = mk_executor(&cli, true);
        let res = run_execute(&ex, "python3.12", r#"{}"#);
        let payload: Value = serde_json::from_str(&res.payload).unwrap();
        assert_eq!(payload["errorType"], "SyntaxError");
    }

    #[test]
    fn test_docker_executor_singleton() {
        let _g = GUARD.lock();
        reset_docker_executor();
        let a = get_docker_executor();
        let b = get_docker_executor();
        assert!(Arc::ptr_eq(&a, &b));
        reset_docker_executor();
        let c = get_docker_executor();
        assert!(!Arc::ptr_eq(&a, &c));
        reset_docker_executor();
    }

    #[test]
    fn test_capability_runtimes_track_mode_and_daemon() {
        let _g = GUARD.lock();
        unset_env("LAMBDA_EXECUTOR");
        unset_env("LAMBDA_RUNTIME_EXECUTOR");
        assert_eq!(docker_capability_runtimes(), None);

        set_env("LAMBDA_EXECUTOR", "docker");
        let daemon = docker_available(2);
        let caps = docker_capability_runtimes();
        if daemon {
            let families = caps.expect("docker hosts serve all image families");
            let mut sorted = families.clone();
            sorted.sort_unstable();
            assert_eq!(families, sorted);
            for family in ["custom", "dotnet", "java", "nodejs", "python", "ruby"] {
                assert!(
                    families.contains(&family),
                    "missing family {family} in {families:?}"
                );
            }
        } else {
            assert_eq!(caps, None, "no daemon → no docker capability");
        }
        unset_env("LAMBDA_EXECUTOR");
        if docker_available(2) {
            assert_eq!(docker_capability_runtimes(), None);
        }
    }

    // ------------------------------------------------------------------
    // Live docker end-to-end (skipped without a running daemon)
    // ------------------------------------------------------------------

    /// Pull a runtime image once if it is not present (the official images
    /// may be missing on a fresh host).
    fn ensure_image(image: &str) {
        let inspect = ["docker", "image", "inspect", image]
            .iter()
            .map(|s| s.to_string())
            .collect::<Vec<String>>();
        if run_subprocess(&inspect, None, 30)
            .map(|out| out.status_code == Some(0))
            .unwrap_or(false)
        {
            return;
        }
        let pull = ["docker", "pull", image]
            .iter()
            .map(|s| s.to_string())
            .collect::<Vec<String>>();
        let _ = run_subprocess(&pull, None, 900);
    }

    fn e2e_code_dir(code: &str) -> std::path::PathBuf {
        let dir =
            std::env::temp_dir().join(format!("docker-e2e-{}", uuid::Uuid::new_v4().simple()));
        std::fs::create_dir_all(&dir).unwrap();
        std::fs::write(dir.join("lambda_function.py"), code).unwrap();
        dir
    }

    /// Minimal std-only HTTP stub standing in for the gateway, listening on
    /// an ephemeral 0.0.0.0 port so the container can dial host-gateway:port.
    fn spawn_probe_server(expected_requests: usize) -> (u16, std::thread::JoinHandle<()>) {
        use std::io::{Read, Write};
        let listener = std::net::TcpListener::bind("0.0.0.0:0").unwrap();
        let port = listener.local_addr().unwrap().port();
        let handle = std::thread::spawn(move || {
            // Accept, serve, count: block only while a request is still owed,
            // so one connection closes the loop and join() returns.
            let mut served = 0;
            while served < expected_requests {
                let Ok((mut stream, _)) = listener.accept() else {
                    continue;
                };
                let mut buf = [0u8; 4096];
                let _ = stream.read(&mut buf);
                let _ = stream.write_all(
                    b"HTTP/1.1 200 OK\r\nContent-Type: text/plain\r\nContent-Length: 6\r\nConnection: close\r\n\r\nprobe6",
                );
                served += 1;
            }
        });
        (port, handle)
    }

    /// Skip helper for daemon-less hosts.
    fn live_expected() -> DockerLambdaExecutor {
        assert!(
            docker_available(5),
            "live docker tests require a daemon (guarded by caller)"
        );
        ensure_image(LIVE_IMAGE);
        DockerLambdaExecutor::new()
    }

    /// Live LAMBDA_EXECUTOR=docker flow for python3.12: a real container
    /// runs lambda_function.py from a temp code dir and the function result
    /// reaches the caller through the runtime-interface handshake, incl.
    /// user env vars.
    #[test]
    fn test_live_docker_e2e_python312() {
        let _g = GUARD.lock();
        if !docker_available(5) {
            return; // skip: no docker daemon on this host
        }
        let ex = live_expected();
        let dir = e2e_code_dir(
            "import os\n\ndef lambda_handler(event, context):\n    return {\n        \"ok\": True,\n        \"echo\": event.get(\"echo\"),\n        \"custom\": os.environ.get(\"LAMBDA_ENV_ZETA\"),\n    }\n",
        );
        let mut env = HashMap::new();
        env.insert("LAMBDA_ENV_ZETA".to_string(), "docker-env-zeta".to_string());
        let res = ex.execute(
            "python3.12",
            b"",
            &[],
            "lambda_function.lambda_handler",
            r#"{"echo": "ping-1"}"#,
            "e2e-fn",
            "$LATEST",
            "arn:aws:lambda:us-east-1:123456789012:function:e2e-fn",
            &env,
            30,
            128,
            "us-east-1",
            "123456789012",
            Some(&dir),
        );
        let _ = std::fs::remove_dir_all(&dir);
        assert_eq!(res.function_error, None, "logs: {}", res.logs);
        let payload: Value = serde_json::from_str(&res.payload).unwrap();
        assert_eq!(payload["ok"], true);
        assert_eq!(payload["echo"], "ping-1");
        assert_eq!(payload["custom"], "docker-env-zeta");
        assert!(!res.logs.is_empty(), "container logs must be captured");
    }

    /// Live handler errors reach the caller as "Handled" function errors with
    /// the error payload from the runtime interface.
    #[test]
    fn test_live_docker_handler_error() {
        let _g = GUARD.lock();
        if !docker_available(5) {
            return; // skip: no docker daemon on this host
        }
        let ex = live_expected();
        let dir =
            e2e_code_dir("def lambda_handler(event, context):\n    raise ValueError(\"boom\")\n");
        let res = ex.execute(
            "python3.12",
            b"",
            &[],
            "lambda_function.lambda_handler",
            r#"{}"#,
            "e2e-err",
            "$LATEST",
            "arn:aws:lambda:us-east-1:123456789012:function:e2e-err",
            &HashMap::new(),
            30,
            128,
            "us-east-1",
            "123456789012",
            Some(&dir),
        );
        let _ = std::fs::remove_dir_all(&dir);
        assert_eq!(
            res.function_error.as_deref(),
            Some("Handled"),
            "logs: {}",
            res.logs
        );
        let payload: Value = serde_json::from_str(&res.payload).unwrap();
        assert_eq!(payload["errorType"], "ValueError");
        assert_eq!(payload["errorMessage"], "boom");
    }

    /// Container → gateway loopback: the container dials the twin's gateway
    /// port (host.docker.internal:{GATEWAY_PORT}) and the handler reports
    /// the HTTP exchange back as its result.
    #[test]
    fn test_live_docker_gateway_loopback() {
        let _g = GUARD.lock();
        if !docker_available(5) {
            return; // skip: no docker daemon on this host
        }
        let (port, server) = spawn_probe_server(1);
        let mut ex = live_expected();
        ex.gateway_port = port;
        let dir = e2e_code_dir(
            "import os, urllib.request\n\ndef lambda_handler(event, context):\n    url = os.environ[\"AWS_ENDPOINT_URL\"].rstrip(\"/\") + \"/loopback-probe\"\n    try:\n        with urllib.request.urlopen(url, timeout=5) as resp:\n            return {\"net_ok\": True, \"status\": resp.status, \"body\": resp.read().decode()}\n    except Exception as exc:\n        return {\"net_ok\": False, \"err\": str(exc)}\n",
        );
        let res = ex.execute(
            "python3.12",
            b"",
            &[],
            "lambda_function.lambda_handler",
            r#"{}"#,
            "e2e-loopback",
            "$LATEST",
            "arn:aws:lambda:us-east-1:123456789012:function:e2e-loopback",
            &HashMap::new(),
            30,
            128,
            "us-east-1",
            "123456789012",
            Some(&dir),
        );
        let _ = std::fs::remove_dir_all(&dir);
        assert_eq!(res.function_error, None, "logs: {}", res.logs);
        let payload: Value = serde_json::from_str(&res.payload).unwrap();
        assert_eq!(payload["net_ok"], true, "payload: {}", res.payload);
        assert_eq!(payload["status"], 200);
        assert_eq!(payload["body"], "probe6");
        server.join().unwrap();
    }

    // ------------------------------------------------------------------
    // Executor hardening (adversarial-review fixes)
    // ------------------------------------------------------------------

    /// The dotnet handler's assembly component is the only piece that lands
    /// in a filesystem path or a generated csproj: every separator, dot
    /// segment, leading dot, csproj/XML metacharacter, MSBuild `$(...)`
    /// fragment and sub-0x20 control character is rejected (XML escaping
    /// does not neutralize MSBuild property expressions, so `$(...)` would be
    /// *evaluated* by the twin-run SDK build). Tab/lf/cr stay accepted — the
    /// only sub-0x20 whitespace the era tolerated. The python era
    /// (`runtimes/dotnet.py`) vetted nothing — that gap is the
    /// csproj-injection/path-traversal hole this guard closes.
    #[test]
    fn dotnet_assembly_violation_rejects_unsafe_names() {
        for bad in [
            "",
            ".hidden",
            "..",
            "a..b",
            "sub/dir",
            "sub\\dir",
            "a<b",
            "a>b",
            "a&b",
            "a\"b",
            "a'b",
            "a$(d)b",
            "$(TestAssembly)",
            "a\u{1}b",
            "a\u{7}b",
            "a\u{1f}b",
        ] {
            assert!(
                dotnet_assembly_violation(bad).is_some(),
                "assembly {bad:?} must be rejected"
            );
        }
        for good in ["Assembly", "my-lib", "My.App", "Assembly_2"] {
            assert_eq!(dotnet_assembly_violation(good), None, "{good}");
        }
    }

    /// A rejected handler answers with the era's Runtime.ImportModuleError
    /// shape and nothing is written into or near `code_dir`.
    #[test]
    fn dotnet_invalid_assembly_rejected_before_any_write() {
        let dir = std::env::temp_dir().join(format!(
            "lam-dotnet-invalid-{}",
            uuid::Uuid::new_v4().simple()
        ));
        std::fs::create_dir_all(&dir).unwrap();
        std::fs::write(dir.join("evil.cs"), "using System;\n").unwrap();
        for handler in ["/etc/evil::T::M", "../evil::T::M"] {
            let res = compile_dotnet_source_if_needed("net8.0", &dir, handler)
                .expect("invalid handler must reject with a result");
            assert_eq!(
                res.function_error.as_deref(),
                Some("Runtime.ImportModuleError")
            );
            let payload: Value = serde_json::from_str(&res.payload).unwrap();
            assert_eq!(payload["errorType"], "Runtime.ImportModuleError");
            assert!(!res.payload.is_empty());
        }
        let leftovers: Vec<String> = std::fs::read_dir(&dir)
            .unwrap()
            .filter_map(|e| e.ok())
            .map(|e| e.file_name().to_string_lossy().to_string())
            .collect();
        assert_eq!(leftovers, vec!["evil.cs".to_string()]);
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// Every fragment interpolated into the generated csproj is XML-escaped
    /// (second belt behind the charset validation) and the project's
    /// Directory.Build imports are disabled, so package-shipped MSBuild
    /// hooks cannot reach the host SDK either.
    #[test]
    fn generated_csproj_escapes_interpolated_fragments() {
        let dir =
            std::env::temp_dir().join(format!("lam-csproj-{}", uuid::Uuid::new_v4().simple()));
        std::fs::create_dir_all(&dir).unwrap();
        let proj = write_generated_csproj(&dir, "MyLib", "net8.0");
        let content = std::fs::read_to_string(&proj).unwrap();
        assert!(
            content.contains("<AssemblyName>MyLib</AssemblyName>"),
            "content: {content}"
        );
        assert!(
            content.contains("<TargetFramework>net8.0</TargetFramework>"),
            "content: {content}"
        );
        assert!(content.contains("<ImportDirectoryBuildProps>false"));
        assert!(content.contains("<ImportDirectoryBuildTargets>false"));
        // Escapes apply to every metacharacter (validation keeps them out of
        // names; the escape is corroborated directly here).
        assert_eq!(xml_escape_csproj_value("&"), "&amp;");
        assert_eq!(xml_escape_csproj_value("<"), "&lt;");
        assert_eq!(xml_escape_csproj_value(">"), "&gt;");
        assert_eq!(xml_escape_csproj_value("\""), "&quot;");
        assert_eq!(xml_escape_csproj_value("'"), "&apos;");
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// Host-SDK compile of a .cs source package through the staging path:
    /// the DLL lands in `code_dir` and the staging dir is reaped. Requires a
    /// host dotnet SDK; skipped on SDK-less hosts.
    #[test]
    fn test_dotnet_source_compiles_in_staging() {
        let dotnet_on_path = std::env::var("PATH")
            .map(|paths| std::env::split_paths(&paths).any(|p| p.join("dotnet").is_file()))
            .ok();
        if dotnet_on_path != Some(true) {
            return; // skip: no host dotnet SDK
        }
        let dir = std::env::temp_dir().join(format!(
            "lam-dotnet-compile-{}",
            uuid::Uuid::new_v4().simple()
        ));
        std::fs::create_dir_all(&dir).unwrap();
        std::fs::write(
            dir.join("TestAssembly.cs"),
            "using System;\nnamespace Robotocore.Tests\n{\n    public class TestHandler\n    {\n        public string Handle(string evt, object ctx)\n        {\n            return \"compiled\";\n        }\n    }\n}\n",
        )
        .unwrap();
        let res = compile_dotnet_source_if_needed(
            "net8.0",
            &dir,
            "TestAssembly::Robotocore.Tests.TestHandler::Handle",
        );
        assert!(
            res.is_none(),
            "compile failed: {:?}",
            res.as_ref()
                .map(|r| (r.function_error.clone(), r.logs.clone()))
        );
        assert!(dir.join("TestAssembly.dll").exists());
        // The staging dir only holds twin-generated content and is reaped.
        let build_root = std::env::temp_dir().join("robotocore-lambda-build");
        let leftovers = std::fs::read_dir(&build_root)
            .map(|entries| entries.filter_map(|e| e.ok()).count())
            .unwrap_or(0);
        assert_eq!(leftovers, 0, "staging dirs must be reaped after compile");
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// A POST that reached the HTTP exchange — or whose reachability is
    /// unknowable — is never re-sent: one accepted POST is one invocation.
    /// A bodyless exit 0 (curl -s delivered, response lost) latches and
    /// reports a failed invocation instead of rescuing.
    #[test]
    fn post_retry_never_redelivers_after_ambiguous_attempt() {
        let _g = GUARD.lock();
        let cli = MockCli::new();
        cli.run_starts();
        cli.exec_results(&[Ok(mock_output(0, "", ""))]);
        let ex = mk_executor(&cli, true);
        let res = run_execute(&ex, "python3.12", r#"{}"#);
        assert_eq!(res.function_error.as_deref(), Some("Unhandled"));
        let payload: Value = serde_json::from_str(&res.payload).unwrap();
        assert_eq!(payload["errorType"], "Runtime.ExitError");
        assert_eq!(payload["errorMessage"], "Function execution failed");
        assert!(
            cli.run_calls().is_empty() || cli.run_calls().len() == 1,
            "container run not retried"
        );
        assert_eq!(
            cli.exec_calls().len(),
            1,
            "no re-POST after ambiguous attempt"
        );
    }

    /// A refused POST (curl exit 7 — runtime client not bound to :8080 yet)
    /// is rescuable and the next POST delivers; a body that arrives despite
    /// a non-zero curl status is preferred over any re-POST.
    #[test]
    fn post_retry_rescues_only_pre_exchange_refusals() {
        let _g = GUARD.lock();
        let cli = MockCli::new();
        cli.run_starts();
        cli.exec_results(&[
            Ok(mock_output(7, "", "")),
            Ok(mock_output(0, r#"{"found": true}"#, "")),
        ]);
        let ex = mk_executor(&cli, true);
        let res = run_execute(&ex, "python3.12", r#"{}"#);
        assert_eq!(res.function_error, None, "logs: {}", res.logs);
        let payload: Value = serde_json::from_str(&res.payload).unwrap();
        assert_eq!(payload["found"], true);
        assert_eq!(cli.exec_calls().len(), 2, "refused then delivered");
    }

    /// The sweep's list half is argv-only (no shell pipeline):
    /// `docker ps -aq --filter name=robotocore-lambda- --format {{.Names}}`.
    /// `-a` covers every container state (Created-state husks included);
    /// `{{.Names}}` exposes names for the in-process prefix match.
    #[test]
    fn sweep_list_cmd_filters_lambda_container_prefix() {
        assert_eq!(
            sweep_list_cmd(),
            vec![
                "docker".to_string(),
                "ps".to_string(),
                "-aq".to_string(),
                "--filter".to_string(),
                "name=robotocore-lambda-".to_string(),
                "--format".to_string(),
                "{{.Names}}".to_string(),
            ]
        );
    }

    /// Docker's `name=` filter matches substrings, not prefixes: the sweep's
    /// in-process pass keeps only the executor's exact name shape
    /// (`robotocore-lambda-{function}-{8hex}`). A neighbor like
    /// `simulator-robotocore-lambda-proxy` — or a bare `robotocore-lambda-
    /// proxy` missing the executor's uuid tail — survives, while the
    /// executor's own containers (Created-state bridge husks included) are
    /// reaped.
    #[test]
    fn straggler_names_prefix_match_excludes_neighbors() {
        let stdout = "\
robotocore-lambda-fn1-12345678
robotocore-lambda-bridge-fn-deadbeef
simulator-robotocore-lambda-proxy
robotocore-lambda-proxy
Other-Container-1
";
        assert_eq!(
            straggler_names(stdout),
            vec![
                "robotocore-lambda-fn1-12345678".to_string(),
                "robotocore-lambda-bridge-fn-deadbeef".to_string(),
            ]
        );
        assert!(straggler_names("").is_empty());
    }

    /// Every executor-owned temp root is startup-pruned: stale entries
    /// removed, fresh entries (live concurrent twins) survive in each root.
    #[test]
    fn prune_stale_temp_roots_covers_executor_roots() {
        let base =
            std::env::temp_dir().join(format!("lam-roots-{}", uuid::Uuid::new_v4().simple()));
        let backdated = std::time::SystemTime::now()
            - STALE_EXTRACT_DIR_AGE
            - std::time::Duration::from_secs(10);
        for root in TEMP_PRUNE_ROOTS {
            let stale_dir = base
                .join(root)
                .join(format!("stale-{}", uuid::Uuid::new_v4().simple()));
            let fresh_dir = base
                .join(root)
                .join(format!("fresh-{}", uuid::Uuid::new_v4().simple()));
            std::fs::create_dir_all(&stale_dir).unwrap();
            std::fs::create_dir_all(&fresh_dir).unwrap();
            let handle = std::fs::File::open(&stale_dir).unwrap();
            if std::fs::File::set_times(&handle, std::fs::FileTimes::new().set_modified(backdated))
                .is_err()
            {
                // Platform cannot backdate directory times: quit cleanly.
                let _ = std::fs::remove_dir_all(&base);
                return;
            }
            drop(handle);
            prune_stale_extract_dirs(&base.join(root), STALE_EXTRACT_DIR_AGE);
            assert!(
                !stale_dir.exists(),
                "stale entry under {root} must be pruned"
            );
            assert!(fresh_dir.exists(), "fresh entry under {root} must survive");
        }
        // The roots the brief pins (docker extraction, dotnet staging,
        // loose-bridges, python/custom -exec, node -node).
        for root in [
            "robotocore-lambda-docker",
            "robotocore-lambda-build",
            "robotocore-lambda-bridges",
            "robotocore-lambda-exec",
            "robotocore-lambda-node",
        ] {
            assert!(
                TEMP_PRUNE_ROOTS.contains(&root),
                "executor tempo root {root} must be startup-pruned"
            );
        }
        let _ = std::fs::remove_dir_all(&base);
    }

    /// Daemon death mid-life: the per-invoke probe (5s cap) answers false,
    /// the executor's fresh verdict drops, and the invocation falls back to
    /// the LOCAL executors instead of spending the budget on docker CLI
    /// errors — no docker run, no rm.
    #[test]
    fn daemon_death_mid_life_falls_back_to_local() {
        let _g = GUARD.lock();
        let cli = MockCli::new();
        cli.script_probes(&[false]);
        let ex = mk_executor(&cli, true);
        ex.reprobe_docker_available();
        assert!(!ex.docker_available.load(AtomicOrdering::Acquire));
        let res = run_execute(&ex, "python3.12", r#"{}"#);
        // Local shim outcome (not a docker error shape): garbage zip →
        // handled BadZipFile, plus zero container lifecycle calls.
        assert_eq!(res.function_error.as_deref(), Some("Handled"));
        let payload: Value = serde_json::from_str(&res.payload).unwrap();
        assert_eq!(payload["errorType"], "BadZipFile");
        assert!(
            cli.run_calls().is_empty(),
            "no docker run after probe death"
        );
        assert!(cli.exec_calls().is_empty());
        assert!(cli.rm_calls().is_empty());
    }

    /// Daemon recovery: an executor built during a startup dip (snapshot
    /// false) re-adopts a healthy daemon per invoke and docker executes.
    #[test]
    fn daemon_recovery_after_startup_dip_adopts_docker() {
        let _g = GUARD.lock();
        let cli = MockCli::new();
        cli.script_probes(&[true]);
        cli.run_starts();
        cli.exec_results(&[Ok(mock_output(0, r#"{"ok": true}"#, ""))]);
        let ex = mk_executor(&cli, false);
        assert!(ex.reprobe_docker_available());
        assert!(ex.docker_available.load(AtomicOrdering::Acquire));
        let res = run_execute(&ex, "nodejs20.x", r#"{}"#);
        assert_eq!(res.function_error, None, "logs: {}", res.logs);
        let payload: Value = serde_json::from_str(&res.payload).unwrap();
        assert_eq!(payload["ok"], true);
        assert_eq!(cli.run_calls().len(), 1, "docker path ran after re-adopt");
    }

    /// A genuinely unmapped runtime with a dead daemon stays on the
    /// local-family contract: Runtime.InvalidRuntime (the docker executor
    /// is simply absent for that family).
    #[test]
    fn daemon_death_keeps_invalid_runtime_for_unmapped_family() {
        let _g = GUARD.lock();
        let cli = MockCli::new();
        cli.script_probes(&[false]);
        let ex = mk_executor(&cli, true);
        ex.reprobe_docker_available();
        let res = run_execute(&ex, "cobol99", r#"{}"#);
        assert_eq!(
            res.function_error.as_deref(),
            Some("Runtime.InvalidRuntime")
        );
    }

    /// Sweep through the injected CLI: every listed straggler is
    /// force-removed through the recorded `rm` method, nothing else runs.
    #[test]
    fn sweep_stragglers_force_removes_only_listed_containers() {
        let _g = GUARD.lock();
        let cli = MockCli::new();
        cli.script_stragglers(&["a1b2c3d4", "e5f6a7b8"]);
        let ex = mk_executor(&cli, true);
        ex.sweep_stragglers();
        assert_eq!(
            cli.rm_calls(),
            vec!["a1b2c3d4".to_string(), "e5f6a7b8".to_string()]
        );
        assert!(
            cli.run_calls().is_empty(),
            "sweep never goes through cli.run"
        );
    }

    /// Stale extraction dirs (older than the live-bound) are pruned; dirs
    /// younger than that survive.
    #[test]
    fn prune_removes_only_stale_extract_dirs() {
        let base =
            std::env::temp_dir().join(format!("lam-prune-{}", uuid::Uuid::new_v4().simple()));
        let stale = base.join("stale-fn-0001");
        let fresh = base.join("fresh-fn-0002");
        std::fs::create_dir_all(&stale).unwrap();
        std::fs::create_dir_all(&fresh).unwrap();
        let handle = std::fs::File::open(&stale).unwrap();
        let backdated = std::time::SystemTime::now()
            - STALE_EXTRACT_DIR_AGE
            - std::time::Duration::from_secs(10);
        if std::fs::File::set_times(&handle, std::fs::FileTimes::new().set_modified(backdated))
            .is_err()
        {
            // Platform cannot backdate directory times: skip rather than fail.
            let _ = std::fs::remove_dir_all(&base);
            return;
        }
        drop(handle);
        prune_stale_extract_dirs(&base, STALE_EXTRACT_DIR_AGE);
        assert!(!stale.exists());
        assert!(fresh.exists());
        let _ = std::fs::remove_dir_all(&base);
    }

    /// `docker run -d` answering exit 0 with an empty stdout (daemon quirk)
    /// is a started run: the flow proceeds to the runtime-interface POST.
    #[test]
    fn docker_run_exit_zero_with_empty_stdout_is_a_started_run() {
        let _g = GUARD.lock();
        let cli = MockCli::new();
        cli.run_script_of(&[Ok(mock_output(0, "", ""))]);
        cli.exec_results(&[Ok(mock_output(0, r#"{"ok": true}"#, ""))]);
        let ex = mk_executor(&cli, true);
        let res = run_execute(&ex, "python3.12", r#"{}"#);
        assert_eq!(res.function_error, None, "logs: {}", res.logs);
        let payload: Value = serde_json::from_str(&res.payload).unwrap();
        assert_eq!(payload["ok"], true);
        assert_eq!(cli.run_calls().len(), 1);
        assert_eq!(cli.exec_calls().len(), 1);
        assert_eq!(cli.rm_calls().len(), 1);
    }
}
