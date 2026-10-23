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
//! Design lineage: port of the python-era `DockerLambdaExecutor`
//! (`src/robotocore/services/lambda_/docker_executor.py`, 7d909e8c PR #111,
//! evolving the container-pool executors of #41/#75). One deliberate
//! deviation is documented at [`DockerLambdaExecutor::execute_docker`]: the
//! python-era happy path piped the event through `docker run`'s stdin, which
//! never reaches the container; the working handshake is this module's port
//! of that file's own `_invoke_via_runtime_interface` — POST the event to the
//! Lambda Runtime Interface (the runtime-interface-emulator inside every
//! official runtime image, listening on localhost:8080).

use crate::runtime::{collect_by, drain_thread, ExecResult};
use serde_json::{json, Value};
use std::collections::HashMap;
use std::io::Write;
use std::process::{Child, Stdio};
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
    if !executor.docker_available {
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
    let stdout_rx = drain_thread(child.stdout.take());
    let stderr_rx = drain_thread(child.stderr.take());

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
}

const DOCKER: &str = "docker";
const RUNTIME_INTERFACE_INVOKE_URL: &str =
    "http://localhost:8080/2015-03-31/functions/function/invocations";

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
}

/// Executes Lambda functions in Docker containers using the docker CLI.
///
/// Falls back to the local in-process executors if Docker is not available or
/// if the runtime has no mapped image.
pub struct DockerLambdaExecutor {
    pub(crate) gateway_port: u16,
    pub(crate) docker_network: Option<String>,
    pub(crate) docker_dns: Option<String>,
    /// Snapshotted at construction like the python-era `__init__`.
    pub(crate) docker_available: bool,
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
        let docker_available = docker_available(5);
        Self {
            gateway_port,
            docker_network,
            docker_dns,
            docker_available,
            cli: Arc::new(RealCli),
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
        if !self.docker_available {
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
    /// `timeout + 5` per curl, `docker rm -f` on timeout.
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

        let event_json = event.to_string();
        let timeout_secs = timeout_secs.max(1);
        // Overall budget: the python-era `docker run` window with grace for
        // container startup.
        let run_budget_secs = u64::from(timeout_secs) + 10;

        match self.cli.run(&cmd, &event_json, run_budget_secs) {
            Ok(out) if out.status_code == Some(0) && !out.stdout.trim().is_empty() => {}
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
        // POST is one invocation (the emulator queues lone events).
        let deadline = Instant::now() + Duration::from_secs(run_budget_secs);
        let mut result_stdout = String::new();
        let mut timed_out = false;
        let mut container_gone = false;
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
                    // Runtime not up yet or bodyless response: keep polling.
                }
                Err(DockerRunError::Timeout) => {
                    // The curl budget (timeout + 5, python-era
                    // _invoke_via_runtime_interface tune) expired: the
                    // function hung. Same outcome as the python-era docker
                    // run TimeoutExpired branch.
                    timed_out = true;
                    break;
                }
                Err(_) => {
                    // Transient CLI failure: keep polling until the budget.
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

        // Parse successful response: JSON data verbatim; non-JSON output as a
        // JSON string (python-era raw-string fallback via the provider's
        // json.dumps). The runtime interface emulator answers handler errors
        // with the error payload as the body (errorMessage +
        // requestId/stackTrace), so surface those as "Handled" function
        // errors.
        match serde_json::from_str::<Value>(&result_stdout) {
            Ok(parsed) => {
                let is_handler_error = parsed
                    .as_object()
                    .map(|o| {
                        o.contains_key("errorMessage")
                            && (o.contains_key("requestId") || o.contains_key("stackTrace"))
                    })
                    .unwrap_or(false);
                if is_handler_error {
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
                }),
            }
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
    }

    fn mk_executor(cli: &MockCli, docker_available: bool) -> DockerLambdaExecutor {
        DockerLambdaExecutor {
            gateway_port: 4566,
            docker_network: None,
            docker_dns: None,
            docker_available,
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
        assert!(!ex.docker_available);
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
        let res = run_execute(&ex, "java21", r#"{}"#);
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
        let res = run_execute(&ex, "dotnet8", r#"{}"#);
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
        // Bodyless 200 from the runtime interface = runtime never answered;
        // the next POST hits the dry script (curl-budget timeout).
        cli.exec_results(&[Ok(mock_output(0, "", ""))]);
        let ex = mk_executor(&cli, true);
        let res = run_execute(&ex, "python3.12", r#"{}"#);
        assert_eq!(res.function_error.as_deref(), Some("Task.TimedOut"));
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

    // ------------------------------------------------------------------
    // Singleton (python-era module-level singleton)
    // ------------------------------------------------------------------

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
}
