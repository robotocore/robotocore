//! Lambda handler runtime execution.
//!
//! Executes Python handler code by unpacking the deployment package into a
//! per-invocation temp directory and spawning `python3` with a small shim.
//! Mirrors robotocore's in-process Python executor: the handler receives the
//! event payload and a Lambda-like context, returns JSON-serializable data,
//! and raised exceptions surface as `FunctionError` payloads
//! (`{"errorMessage", "errorType", "stackTrace"}`) with captured stderr and
//! stdout available as function logs.
//!
//! Alongside the python shim, the node bootstrap and the custom-runtime
//! `bootstrap` executor, this module implements the managed families
//! (`ruby`/`java`/`dotnet`) — ports of the python-era
//! `runtimes/{ruby,java,dotnet}.py` executors that ran the era's bootstraps
//! on the twin host with the era `base.run_subprocess` protocol (event JSON
//! on stdin, result on stdout, error JSON + non-zero exit on failure).

use serde_json::{json, Value};
use std::collections::HashMap;
use std::fs;
use std::io::Write;
use std::path::PathBuf;
use std::process::{Child, Stdio};
use std::sync::mpsc;

/// Result of executing a function handler.
pub struct ExecResult {
    /// Raw payload body string (already JSON-serialized), or "null".
    pub payload: String,
    /// `Some("Handled")` when the handler raised; payload carries error info.
    pub function_error: Option<String>,
    /// Captured execution logs (shim stderr + handler stdout) for LogType=Tail.
    pub logs: String,
}

const SHIM_PY: &str = r#"#!/usr/bin/env python3
"""robotocore Lambda shim: unpack code, import handler, call it, emit result JSON."""
import importlib
import io
import json
import os
import sys
import time
import traceback
import uuid
import zipfile


# The running decompressed-bytes budget of one extraction (era limits.py:
# 250 MB unzipped per function). Without the bound a crafted 2 MB zip
# expands N-fold into the work dir on every invoke and fills the disk.
ZIP_UNPACK_BUDGET_BYTES = 250 * 1024 * 1024
ZIP_UNPACK_BUDGET_MB = ZIP_UNPACK_BUDGET_BYTES // (1024 * 1024)


class Ctx:
    def __init__(self, meta):
        self.meta = meta
        self.aws_request_id = str(uuid.uuid4())
        self.function_name = meta["function_name"]
        self.function_version = meta.get("function_version", "$LATEST")
        self.memory_limit_in_mb = str(meta.get("memory_size", 128))
        self.invoked_function_arn = meta.get("function_arn", "")
        self.log_group_name = f"/aws/lambda/{self.function_name}"
        self.log_stream_name = "{date}/[$LATEST]{rid}".format(date=time.strftime("%Y/%m/%d", time.gmtime()), rid=uuid.uuid4().hex[:32])
        self._timeout_ms = int(meta.get("timeout", 3)) * 1000
        self._started = time.time()

    def get_remaining_time_in_millis(self):
        elapsed_ms = int((time.time() - self._started) * 1000)
        return max(0, self._timeout_ms - elapsed_ms)


def render_budget_error():
    print(
        f"ZIP archive expands past the {ZIP_UNPACK_BUDGET_MB} MB unzipped limit",
        file=sys.stderr,
    )
    sys.stderr.flush()
    raise SystemExit(1)


def safe_extract(zippath, outdir, written=0):
    """Budgeted extraction, carrying `written` across calls: ONE per-function
    budget spans code + every layer (era limits.py: 250 MB unzipped per
    FUNCTION, not per archive — per-archive resets let N layers draw N×250 MB).
    Returns the running total."""
    with open(zippath, "rb") as fh:
        blob = fh.read()
    with zipfile.ZipFile(io.BytesIO(blob)) as zf:
        for name in zf.namelist():
            normalized = name.replace("\\", "/")
            if normalized.startswith("/") or ".." in normalized.split("/"):
                raise ValueError(f"Unsafe path in ZIP archive: {name}")
        for info in zf.infolist():
            written += info.file_size
            if written > ZIP_UNPACK_BUDGET_BYTES:
                render_budget_error()
            zf.extract(info, outdir)
    return written


class HandlerLoadError(Exception):
    """Handler-string/binding problems (era bootstrap.py: Runtime.HandlerNotFound)."""


class HandlerImportError(Exception):
    """Module resolution problems (era runtime: Runtime.ImportModuleError)."""


def main():
    work = sys.argv[1]
    result_path = os.path.join(work, "result.json")
    try:
        meta = json.load(open(os.path.join(work, "fn.json")))
        event = json.load(open(os.path.join(work, "event.json")))
        handler = meta.get("handler") or "lambda_function.handler"
        code_dir = os.path.join(work, "code")
        os.makedirs(code_dir, exist_ok=True)
        unpacked = safe_extract(os.path.join(work, "code.zip"), code_dir)

        # Unpack each layerN.zip into layerN/ so its python/ subtree (or
        # package dirs) lands there and the sys.path scan below finds it.
        # Layers carry the SAME running budget as the code zip (one
        # per-function total, like the node bootstrap).
        def layer_index(name):
            # Numeric-aware: layer2 must sort before layer10 (a plain string
            # sort put layer10 first). Attachment order after the numeric
            # sort is what the insert(1) loop below rides for precedence.
            digits = "".join(ch for ch in name if ch.isdigit())
            return (int(digits) if digits else -1, name)

        layer_zips = sorted(
            (p for p in os.listdir(work)
             if p.startswith("layer") and p.endswith(".zip")),
            key=layer_index,
        )
        for layer_zip in layer_zips:
            dest = os.path.join(work, layer_zip[:-4])
            os.makedirs(dest, exist_ok=True)
            unpacked = safe_extract(os.path.join(work, layer_zip), dest, unpacked)

        sys.path.insert(0, code_dir)
        # Each layer insert(1)s after the code dir in ATTACHMENT ORDER, so
        # the LAST attachment ends up highest on the scan order and keeps
        # winning — real-AWS layer precedence. The numeric layer_index above
        # already fixed the only pre-existing bug (a plain string sort put
        # layer10 before layer2).
        layer_dirs = [os.path.join(work, name[:-4], "python") for name in layer_zips]
        for layer_path in layer_dirs:
            if os.path.isdir(os.path.join(os.path.dirname(layer_path))):
                sys.path.insert(1, layer_path)

        if "." not in handler:
            raise HandlerLoadError(f"Bad handler format: {handler}")
        module_path, func_name = handler.rsplit(".", 1)
        ctx = Ctx(meta)
        try:
            module = importlib.import_module(module_path)
        except ModuleNotFoundError as exc:
            raise HandlerImportError(f"Cannot find module: {module_path}") from exc
        fn = getattr(module, func_name, None)
        if fn is None or not callable(fn):
            raise HandlerLoadError(
                f"Handler '{func_name}' not found in module '{module_path}'"
            )
        result = fn(event, ctx)
        open(result_path, "w").write(
            json.dumps({"restocore_result_raw": json.dumps(result, default=str)})
        )
    except SystemExit:
        # Budgeted-extraction aborts re-raise past the error recorder:
        # the process exits non-zero WITHOUT a result record, which the
        # executor wraps as Runtime.InternalError (like the other shim
        # failures). Recording it as handler data would mislabel the
        # extraction crash as a handler exception.
        raise
    except (HandlerLoadError, HandlerImportError) as exc:
        # Load-era failures carry the era bootstrap's Runtime.* types so the
        # local shim answers the same shape the docker renormalizer (and
        # every other runtime family) produces.
        open(result_path, "w").write(
            json.dumps(
                {
                    "restocore_error": {
                        "errorMessage": str(exc),
                        "errorType": (
                            "Runtime.HandlerNotFound"
                            if isinstance(exc, HandlerLoadError)
                            else "Runtime.ImportModuleError"
                        ),
                        "stackTrace": [],
                    }
                }
            )
        )
        sys.stderr.write(f"{exc}\n")
        sys.stderr.flush()
    except BaseException as exc:  # report handler errors as data, not as crashes
        tb = traceback.format_exc()
        open(result_path, "w").write(
            json.dumps(
                {
                    "restocore_error": {
                        "errorMessage": str(exc),
                        "errorType": type(exc).__name__,
                        "stackTrace": [l for l in tb.split("\n") if l.strip()],
                    }
                }
            )
        )
        sys.stderr.write(tb)
        sys.stderr.flush()


if __name__ == "__main__":
    main()
"#;

pub(crate) fn safe_dir_name(fn_name: &str) -> String {
    fn_name
        .chars()
        .map(|c| {
            if c.is_ascii_alphanumeric() || c == '-' || c == '_' {
                c
            } else {
                '_'
            }
        })
        .collect()
}

fn missing_runtime_result(msg: &str) -> ExecResult {
    ExecResult {
        payload: json!({"errorMessage": msg, "errorType": "Runtime.InternalError"}).to_string(),
        function_error: Some("Unhandled".to_string()),
        logs: String::new(),
    }
}

/// Cap on captured handler output per stream (stdout, stderr) for the LOCAL
/// (in-process) executors' LOG channels — stderr rides this; logs never
/// exceed the HTTP envelope. When a stream exceeds the cap only the last
/// bytes are kept (tail semantics).
const LOCAL_OUTPUT_CAP: usize = 128 * 1024;

/// Cap on the RESULT channel of the stdin-fed family executors
/// (`run_subprocess_family`: stdout carries the invocation's response
/// payload, not logs). The tail-truncating 128 KB log cap silently
/// corrupted handlers returning more than 128 KB of JSON (review round 2):
/// the kept tail no longer parses as JSON and the caller received a
/// garbage wrapped string with no function error. Real Lambda responses
/// reach 6 MB; keep the whole result window up to that budget.
const FAMILY_RESULT_CAP: usize = 6 * 1024 * 1024;

/// The moto ResponseSizeTooLarge failure (moto/awslambda/responses.py:66-68):
/// moto's exact message with its 6291556 bound, `Function.ResponseSizeTooLarge`
/// errorType and the `x-amz-function-error: Unhandled` header (the handler
/// maps `function_error` onto that header). Shared by every stdout-riding
/// result channel so a handler whose output hit the capture budget fails
/// loudly instead of delivering a tail fragment as data.
fn response_too_large_result(logs: String) -> ExecResult {
    ExecResult {
        payload: json!({
            "errorMessage": "Response payload size exceeded maximum allowed payload size (6291556 bytes).",
            "errorType": "Function.ResponseSizeTooLarge",
        })
        .to_string(),
        function_error: Some("Unhandled".to_string()),
        logs,
    }
}

/// Cap on captured docker-cli output per stream: the EMI-family
/// invocation answers (whole runtime-interface response bodies) ride the
/// `docker exec` stdout, so the docker pipes need more room than the local
/// executors — 4MB keeps multi-hundred-KB results intact while bounding the
/// buffer.
pub(crate) const DOCKER_OUTPUT_CAP: usize = 4 * 1024 * 1024;

/// Drain a spawned child pipe on a dedicated thread, keeping at most `cap`
/// bytes (tail semantics) and returning a channel the caller can wait on
/// with a deadline. Reading blocks until EOF, which never arrives while a
/// grandchild inherited the pipe, so the caller must bound how long it
/// waits and abandon the thread (and its text) on timeout.
pub(crate) fn drain_thread<R: std::io::Read + Send + 'static>(
    pipe: Option<R>,
    cap: usize,
) -> mpsc::Receiver<String> {
    let (tx, rx) = mpsc::channel();
    std::thread::spawn(move || {
        let mut kept: Vec<u8> = Vec::new();
        if let Some(mut p) = pipe {
            let mut chunk = [0u8; 8192];
            loop {
                match p.read(&mut chunk) {
                    Ok(0) | Err(_) => break,
                    Ok(n) => {
                        kept.extend_from_slice(&chunk[..n]);
                        if kept.len() > cap {
                            let excess = kept.len() - cap;
                            kept.drain(..excess);
                        }
                    }
                }
            }
        }
        let _ = tx.send(String::from_utf8_lossy(&kept).into_owned());
    });
    rx
}

/// Take the drained text, bounded by the absolute `deadline`; on timeout
/// abandon the drain thread and return nothing.
pub(crate) fn collect_by(rx: &mpsc::Receiver<String>, deadline: std::time::Instant) -> String {
    rx.recv_timeout(deadline.saturating_duration_since(std::time::Instant::now()))
        .unwrap_or_default()
}

/// Install the stdout/stderr pipe drains and feed the event JSON over stdin
/// from a dedicated thread — the era `communicate()` shape. A bootstrap that
/// defers (or never performs) its stdin read can no longer suspend the
/// parent's deadline loop: the parent kills the child at the deadline and
/// the writer thread unblocks with an EPIPE instead of wedging the invoke
/// thread forever behind an OS pipe buffer.
pub(crate) fn communicate_event(
    child: &mut Child,
    event: &str,
    stdout_cap: usize,
    stderr_cap: usize,
) -> (mpsc::Receiver<String>, mpsc::Receiver<String>) {
    let stdout_rx = drain_thread(child.stdout.take(), stdout_cap);
    let stderr_rx = drain_thread(child.stderr.take(), stderr_cap);
    if let Some(mut stdin) = child.stdin.take() {
        let event = event.to_string();
        std::thread::spawn(move || {
            let _ = stdin.write_all(event.as_bytes());
            let _ = stdin.flush();
        });
    }
    (stdout_rx, stderr_rx)
}

/// Execute a Python Lambda handler in a subprocess shim.
///
/// * `env_vars` — user-configured environment variables from the function config
#[allow(clippy::too_many_arguments)]
pub fn execute_python(
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
    let work = std::env::temp_dir().join(format!(
        "robotocore-lambda-exec/{}-{}",
        safe_dir_name(function_name),
        uuid::Uuid::new_v4().simple()
    ));
    let cleanup = |p: &PathBuf| {
        let _ = fs::remove_dir_all(p);
    };
    let err_result = |msg: String, logs: String| -> ExecResult {
        cleanup(&work);
        ExecResult {
            payload: json!({"errorMessage": msg, "errorType": "Runtime.InternalError"}).to_string(),
            function_error: Some("Unhandled".to_string()),
            logs,
        }
    };
    if let Err(e) = fs::create_dir_all(&work) {
        return missing_runtime_result(&e.to_string());
    }
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        let _ = fs::set_permissions(&work, fs::Permissions::from_mode(0o700));
    }
    if fs::write(work.join("code.zip"), code_zip).is_err() {
        return missing_runtime_result("failed to write deployment package");
    }
    let event_str = if event.trim().is_empty() { "{}" } else { event };
    let _ = fs::write(work.join("event.json"), event_str);
    let meta = json!({
        "function_name": function_name,
        "function_version": function_version,
        "function_arn": function_arn,
        "memory_size": memory_size,
        "timeout": timeout_secs,
        "handler": handler,
    });
    let _ = fs::write(work.join("fn.json"), meta.to_string());
    for (i, layer) in layer_zips.iter().enumerate() {
        let _ = fs::write(work.join(format!("layer{}.zip", i)), layer);
    }

    let shim = work.join("lambda_shim.py");
    if fs::write(&shim, SHIM_PY).is_err() {
        return missing_runtime_result("failed to write handler shim");
    }
    let mut cmd = std::process::Command::new(find_python3());
    cmd.arg(shim)
        .arg(work.as_os_str())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .stdin(Stdio::null());
    // Era build_env ordering via the shared helper: user env first,
    // built-ins after, credential trio setdefault.
    apply_era_env(
        &mut cmd,
        env_vars,
        &[
            ("AWS_LAMBDA_FUNCTION_NAME", function_name.to_string()),
            ("AWS_LAMBDA_FUNCTION_VERSION", function_version.to_string()),
            ("AWS_LAMBDA_FUNCTION_MEMORY_SIZE", memory_size.to_string()),
            ("AWS_LAMBDA_FUNCTION_TIMEOUT", timeout_secs.to_string()),
            ("AWS_REGION", region.to_string()),
            ("AWS_DEFAULT_REGION", region.to_string()),
            ("AWS_ACCOUNT_ID", account_id.to_string()),
            ("_HANDLER", handler.to_string()),
        ],
    );

    let mut child: Child = match cmd.spawn() {
        Ok(c) => c,
        Err(e) => {
            // Era parity: a NotFound interpreter (the spawn raised
            // FileNotFoundError) is Runtime.InvalidRuntime, not
            // Runtime.InternalError — and the work dir is cleaned up (the
            // err_result path leaks it previously).
            cleanup(&work);
            if e.kind() == std::io::ErrorKind::NotFound {
                return ExecResult {
                    payload: json!({
                        "errorMessage": format!("python3 interpreter not found: {e}"),
                        "errorType": "Runtime.InvalidRuntime",
                    })
                    .to_string(),
                    function_error: Some("Runtime.InvalidRuntime".to_string()),
                    logs: String::new(),
                };
            }
            return missing_runtime_result(&e.to_string());
        }
    };

    // Start draining pipes immediately, from dedicated threads. Output
    // beyond the OS pipe buffer blocks the child's writes until somebody
    // reads, so waiting here for child exit first would deadlock on any
    // handler that prints more than a pipe buffer.
    let stdout_rx = drain_thread(child.stdout.take(), LOCAL_OUTPUT_CAP);
    let stderr_rx = drain_thread(child.stderr.take(), LOCAL_OUTPUT_CAP);

    // Wait up to the function timeout plus the python-era 2s grace; kill on
    // overrun (all executors share the `+2` kill budget — the era's
    // `subprocess.run(..., timeout=timeout+2)`).
    let wait_secs = u64::from(timeout_secs.max(1)) + 2;
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(wait_secs);
    let mut timed_out = false;
    loop {
        match child.try_wait() {
            Ok(Some(_)) => break,
            Ok(None) => {
                if std::time::Instant::now() > deadline {
                    let _ = child.kill();
                    let _ = child.wait();
                    timed_out = true;
                    break;
                }
                std::thread::sleep(std::time::Duration::from_millis(25));
            }
            Err(e) => return err_result(e.to_string(), String::new()),
        }
    }

    // The child died or finished — drain threads run until EOF, but EOF
    // never arrives while a grandchild inherited the pipes, so collecting
    // is bounded. The killed path allows a short grace; the normal-exit
    // path also keeps only a short grace (the child is reaped, so nothing
    // new can arrive on a pipe whose producer is gone) instead of waiting
    // out the whole remaining timeout budget.
    let drain_deadline = std::time::Instant::now() + std::time::Duration::from_secs(1);
    let stdout = collect_by(&stdout_rx, drain_deadline);
    let stderr = collect_by(&stderr_rx, drain_deadline);

    let result_raw = fs::read_to_string(work.join("result.json")).unwrap_or_default();
    cleanup(&work);

    let mut logs = String::new();
    if !stdout.is_empty() {
        logs.push_str(&stdout);
    }
    if !stderr.is_empty() {
        if !logs.is_empty() {
            logs.push('\n');
        }
        logs.push_str(&stderr);
    }

    if timed_out {
        // Era TimeoutExpired contract (executor.py:771-776): the platform
        // timeout rides `x-amz-function-error: Task.TimedOut` with the
        // .00-formatted message — like every other executor's kill path.
        return crate::docker_runtime::run_timed_out_result(timeout_secs, &logs);
    }

    let parsed: Value = serde_json::from_str(&result_raw).unwrap_or(Value::Null);
    match parsed {
        Value::Object(ref map) if map.contains_key("restocore_error") => ExecResult {
            payload: map
                .get("restocore_error")
                .cloned()
                .unwrap_or(Value::Null)
                .to_string(),
            function_error: Some("Handled".to_string()),
            logs,
        },
        Value::Object(ref map) if map.contains_key("restocore_result_raw") => {
            let raw = map
                .get("restocore_result_raw")
                .and_then(Value::as_str)
                .unwrap_or("null")
                .to_string();
            ExecResult {
                payload: raw,
                function_error: None,
                logs,
            }
        }
        // No result record: the shim died before/while extracting (the
        // budgeted-extraction SystemExit aborts exactly here). The captured
        // logs carry the answer — the cap text or the crash — so they ride
        // the result instead of being dropped.
        _ => ExecResult {
            payload: json!({"errorMessage": "handler produced no result", "errorType": "Runtime.InternalError"}).to_string(),
            function_error: Some("Unhandled".to_string()),
            logs,
        },
    }
}

fn find_python3() -> String {
    if let Ok(path) = std::env::var("ROBOTOCORE_PY") {
        if !path.is_empty() {
            return path;
        }
    }
    "python3".to_string()
}

/// Whether this runtime family executes through the Python shim.
pub fn is_python_runtime(runtime: &str) -> bool {
    let r = runtime.trim().to_ascii_lowercase();
    r.is_empty() || r.starts_with("python") || r == "custom"
}

/// Whether this runtime family executes through the Node.js bootstrap.
pub fn is_node_runtime(runtime: &str) -> bool {
    runtime.trim().to_ascii_lowercase().starts_with("nodejs")
}

const BOOTSTRAP_JS: &str = r#"#!/usr/bin/env node
/**
 * robotocore Node.js Lambda bootstrap: extracts the deployment package and
 * layer zips into the current directory, reads the JSON event from stdin,
 * invokes the handler, and writes the JSON result to stdout (logs on stderr).
 *
 * Protocol (mirrors the Python-era twin): exit 0 = success with the JSON
 * result on stdout; non-zero exit = failure — a stdout object carrying
 * "errorMessage" maps to FunctionError "Handled", anything else becomes
 * FunctionError "Unhandled" ({errorType: Runtime.ExitError}).
 *
 * Usage: node lambda_bootstrap.js [layerN.zip...] code.zip
 */
const fs = require('fs');
const path = require('path');
const zlib = require('zlib');

// The running decompressed-bytes budget shared by every archive this
// process extracts (code + layers; era limits.py: 250 MB unzipped per
// function). Without the bound a crafted 2MB zip expands N-fold into the
// work dir on every invoke and fills the disk.
const ZIP_UNPACK_BUDGET_BYTES = 250 * 1024 * 1024;
let unpackBudgetWritten = 0;

/** Extract one zip archive into destDir (central-directory walk + inflateRaw). */
function extractZip(zipPath, destDir) {
    const buf = fs.readFileSync(zipPath);
    // Locate the End Of Central Directory record, scanning past the comment.
    let eocd = -1;
    for (let i = buf.length - 22; i >= 0 && i >= buf.length - 22 - 65535; i--) {
        if (buf.readUInt32LE(i) === 0x06054b50) {
            eocd = i;
            break;
        }
    }
    if (eocd < 0) throw new Error(`Invalid ZIP archive: ${zipPath}`);
    const entryCount = buf.readUInt16LE(eocd + 10);
    // EOCD stores a 16-bit count; 0xFFFF marks the zip64 overflow sentinel.
    // A >65,535-entry package previously SILENTLY extracted only the first
    // 65,535 files and answered a confusing module-not-found. The node
    // bootstrap has no zip64 reader — fail the invoke with the explicit
    // error instead of a lying partial extraction.
    if (entryCount === 0xffff) {
        throw new Error(
            'ZIP archive has more than 65535 entries (zip64); the node bootstrap cannot read it'
        );
    }
    let offset = buf.readUInt32LE(eocd + 16);
    for (let i = 0; i < entryCount; i++) {
        if (buf.readUInt32LE(offset) !== 0x02014b50) {
            throw new Error(`Corrupt ZIP central directory in ${zipPath}`);
        }
        const method = buf.readUInt16LE(offset + 10);
        const csize = buf.readUInt32LE(offset + 20);
        const usize = buf.readUInt32LE(offset + 24);
        const nameLen = buf.readUInt16LE(offset + 28);
        const extraLen = buf.readUInt16LE(offset + 30);
        const commentLen = buf.readUInt16LE(offset + 32);
        const lfhOffset = buf.readUInt32LE(offset + 42);
        const name = buf.slice(offset + 46, offset + 46 + nameLen).toString('utf8');
        offset += 46 + nameLen + extraLen + commentLen;

        const normalized = name.replace(/\\/g, '/');
        if (normalized.startsWith('/') || normalized.split('/').includes('..')) {
            throw new Error(`Unsafe path in ZIP archive: ${name}`);
        }
        // Payload location comes from the local header: name/extra lengths
        // can differ from the central directory (streamed archives).
        const lfhNameLen = buf.readUInt16LE(lfhOffset + 26);
        const lfhExtraLen = buf.readUInt16LE(lfhOffset + 28);
        const dataStart = lfhOffset + 30 + lfhNameLen + lfhExtraLen;
        const outPath = path.join(destDir, ...normalized.split('/'));
        if (normalized.endsWith('/')) {
            fs.mkdirSync(outPath, { recursive: true });
            continue;
        }
        // Budgeted extraction on BOTH the declaration and the real bytes:
        // the central-directory uncompressed size joins the running total
        // BEFORE the inflate starts (an honestly-declared bomb never
        // inflates), but the declaration is not trusted as the truth — the
        // inflate is clamped to the remaining budget and the running total
        // is reconciled to the REAL inflated size afterward, so a lying
        // central directory (claims 1 MB, actually inflates 300 MB) hits
        // the clamp and aborts non-zero with the cap text instead of
        // writing 300 MB to the work dir.
        unpackBudgetWritten += usize;
        if (unpackBudgetWritten > ZIP_UNPACK_BUDGET_BYTES) {
            throw new Error(
                `ZIP archive expands past the ${ZIP_UNPACK_BUDGET_BYTES / (1024 * 1024)} MB unzipped limit`
            );
        }
        fs.mkdirSync(path.dirname(outPath), { recursive: true });
        const data = buf.slice(dataStart, dataStart + csize);
        const remaining = Math.max(ZIP_UNPACK_BUDGET_BYTES - unpackBudgetWritten, 0);
        let content;
        if (method === 0) {
            content = data;
        } else if (method === 8) {
            try {
                // maxOutputLength=0 is a hard 0-byte limit in node: the
                // exactly-exhausted-budget edge (remaining == 0) would
                // false-abort even a legit last entry. Clamp to at least 1
                // so the non-zero-cap semantics hold; the real-bytes
                // reconciliation below still throws on overrun.
                content = zlib.inflateRawSync(data, { maxOutputLength: Math.max(remaining, 1) });
            } catch (e) {
                throw new Error(
                    `ZIP archive expands past the ${ZIP_UNPACK_BUDGET_BYTES / (1024 * 1024)} MB unzipped limit`
                );
            }
        } else {
            throw new Error(`Unsupported compression method ${method} for ${name}`);
        }
        unpackBudgetWritten += content.length - usize;
        if (unpackBudgetWritten > ZIP_UNPACK_BUDGET_BYTES) {
            throw new Error(
                `ZIP archive expands past the ${ZIP_UNPACK_BUDGET_BYTES / (1024 * 1024)} MB unzipped limit`
            );
        }
        fs.writeFileSync(outPath, content);
    }
}

function readStdin() {
    return new Promise((resolve, reject) => {
        let all = '';
        process.stdin.setEncoding('utf8');
        process.stdin.on('data', (chunk) => {
            all += chunk;
        });
        process.stdin.on('end', () => resolve(all));
        process.stdin.on('error', reject);
    });
}

/** Write one final JSON document to stdout, exit with `code` on flush. */
function send(text, code) {
    process.stdout.write(text, () => process.exit(code));
    // Belt-and-braces for cases where the flush callback never fires (EPIPE).
    setTimeout(() => process.exit(code), 5000).unref();
}

function fail(message, errorType) {
    send(JSON.stringify({ errorMessage: message, errorType }), 1);
}

async function main() {
    // argv[2..]: layer zips first, then the code zip, so code wins conflicts.
    const zips = process.argv.slice(2);
    for (const zipPath of zips) {
        extractZip(zipPath, process.cwd());
    }

    const handler = process.env._HANDLER || '';
    if (!handler) {
        return fail('No handler specified', 'Runtime.HandlerNotFound');
    }
    const dot = handler.lastIndexOf('.');
    if (dot < 0) {
        return fail(`Bad handler format: ${handler}`, 'Runtime.HandlerNotFound');
    }
    const modulePath = handler.substring(0, dot);
    const funcName = handler.substring(dot + 1);

    const eventText = await readStdin();
    let event;
    try {
        event = JSON.parse(eventText);
    } catch (e) {
        event = {};
    }

    const requestId = require('crypto').randomUUID();
    const context = {
        functionName: process.env.AWS_LAMBDA_FUNCTION_NAME,
        functionVersion: process.env.AWS_LAMBDA_FUNCTION_VERSION || '$LATEST',
        invokedFunctionArn: `arn:aws:lambda:${process.env.AWS_REGION || 'us-east-1'}:${
            process.env.AWS_ACCOUNT_ID || '123456789012'
        }:function:${process.env.AWS_LAMBDA_FUNCTION_NAME}`,
        memoryLimitInMB: process.env.AWS_LAMBDA_FUNCTION_MEMORY_SIZE || '128',
        awsRequestId: requestId,
        logGroupName: `/aws/lambda/${process.env.AWS_LAMBDA_FUNCTION_NAME}`,
        logStreamName: `${new Date().toISOString().split('T')[0].replace(/-/g, '/')}/$LATEST`,
        getRemainingTimeInMillis: () => {
            const timeout = parseInt(process.env.AWS_LAMBDA_FUNCTION_TIMEOUT || '3', 10);
            return timeout * 1000;
        },
        done: (err, result) => {
            if (err) throw err;
            return result;
        },
        succeed: (result) => result,
        fail: (err) => {
            throw err;
        },
    };

    let handlerModule;
    try {
        handlerModule = require(path.resolve(process.cwd(), modulePath));
    } catch (e) {
        return fail(`Cannot find module '${modulePath}': ${e.message}`, 'Runtime.ImportModuleError');
    }
    const handlerFunc = handlerModule[funcName];
    if (typeof handlerFunc !== 'function') {
        return fail(
            `Handler '${funcName}' is not a function in '${modulePath}'`,
            'Runtime.HandlerNotFound',
        );
    }

    try {
        let result;
        if (handlerFunc.length <= 2) {
            // async handler or 2-arg handler — awaited promise result
            result = await handlerFunc(event, context);
        } else {
            // callback-style handler(event, context, callback)
            result = await new Promise((resolve, reject) => {
                handlerFunc(event, context, (err, res) => {
                    if (err) reject(err);
                    else resolve(res);
                });
            });
        }
        send(JSON.stringify(result === undefined ? null : result), 0);
    } catch (e) {
        send(
            JSON.stringify({
                errorMessage: e.message || String(e),
                errorType: e.constructor ? e.constructor.name : 'Error',
                stackTrace: (e.stack || '').split('\n'),
            }),
            1,
        );
    }
}

main().catch((e) => {
    process.stderr.write(`Bootstrap error: ${e.message}\n`);
    process.exit(1);
});
"#;

/// Node binary discovery: ROBOTOCORE_NODE override, then $PATH, then the
/// newest nvm-managed node under ~/.nvm/versions/node. `runtime` selects a
/// version-specific binary first, mirroring the python-era
/// `runtimes/node.py::_RUNTIME_BINARY` map (nodejs18.x → node18 …): a
/// preferred versioned binary that is absent on the host falls back to the
/// plain `node` discovery with a warning so the divergence is visible.
fn find_node(runtime: &str) -> Option<String> {
    if let Ok(path) = std::env::var("ROBOTOCORE_NODE") {
        if !path.is_empty() {
            return Some(path);
        }
    }
    // Versioned preference (era _RUNTIME_BINARY): Lambda identifiers to the
    // versioned binary name in the image.
    let preferred = match runtime {
        "nodejs18.x" => Some("node18"),
        "nodejs20.x" => Some("node20"),
        "nodejs22.x" => Some("node22"),
        _ => None,
    };
    if let Some(pref) = preferred {
        if let Ok(paths) = std::env::var("PATH") {
            for dir in paths.split(':') {
                if dir.is_empty() {
                    continue;
                }
                let candidate = std::path::Path::new(dir).join(pref);
                if candidate.is_file() {
                    return Some(candidate.to_string_lossy().into_owned());
                }
            }
        }
        eprintln!(
            "node: {runtime} prefers {pref}, binary not found — running with the default node"
        );
    }
    if let Ok(paths) = std::env::var("PATH") {
        for dir in paths.split(':') {
            if dir.is_empty() {
                continue;
            }
            let candidate = std::path::Path::new(dir).join("node");
            if candidate.is_file() {
                return Some(candidate.to_string_lossy().into_owned());
            }
        }
    }
    if let Ok(home) = std::env::var("HOME") {
        let nvm = std::path::PathBuf::from(home)
            .join(".nvm")
            .join("versions")
            .join("node");
        if let Ok(entries) = fs::read_dir(&nvm) {
            let mut best: Option<(Vec<u64>, PathBuf)> = None;
            for entry in entries.flatten() {
                let bin = entry.path().join("bin").join("node");
                if !bin.is_file() {
                    continue;
                }
                let version: Vec<u64> = entry
                    .file_name()
                    .to_string_lossy()
                    .trim_start_matches('v')
                    .split('.')
                    .map(|part| part.parse::<u64>().unwrap_or(0))
                    .collect();
                if best
                    .as_ref()
                    .is_none_or(|(best_version, _): &(Vec<u64>, PathBuf)| &version > best_version)
                {
                    best = Some((version, bin));
                }
            }
            if let Some((_, bin)) = best {
                return Some(bin.to_string_lossy().into_owned());
            }
        }
    }
    None
}

/// Execute a Node.js Lambda handler in a subprocess bootstrap.
///
/// Mirrors the Python-era twin's `runtimes/node.py` + `runtimes/base.py`:
/// node reads the JSON event from stdin and prints the JSON result to
/// stdout (logs go to stderr), with `timeout + 2`s of grace before the
/// child is killed and a `Task.TimedOut` error is reported.
#[allow(clippy::too_many_arguments)]
pub fn execute_nodejs(
    runtime: &str,
    code_zip: &[u8],
    layer_zips: &[Vec<u8>],
    handler: &str,
    event: &str,
    function_name: &str,
    function_version: &str,
    env_vars: &HashMap<String, String>,
    timeout_secs: u32,
    memory_size: u32,
    region: &str,
    account_id: &str,
) -> ExecResult {
    if find_node(runtime).is_none() {
        return ExecResult {
            payload: "null".to_string(),
            function_error: Some("Runtime.InvalidRuntime".to_string()),
            logs: "Node.js not installed".to_string(),
        };
    }
    let work = std::env::temp_dir().join(format!(
        "robotocore-lambda-node/{}-{}",
        safe_dir_name(function_name),
        uuid::Uuid::new_v4().simple()
    ));
    let cleanup = |p: &PathBuf| {
        let _ = fs::remove_dir_all(p);
    };
    let runtime_broken = |msg: String, logs: String| -> ExecResult {
        cleanup(&work);
        ExecResult {
            payload: json!({"errorMessage": msg, "errorType": "Runtime.InvalidRuntime"})
                .to_string(),
            function_error: Some("Runtime.InvalidRuntime".to_string()),
            logs,
        }
    };
    if fs::create_dir_all(&work).is_err() {
        return runtime_broken("failed to create execution dir".to_string(), String::new());
    }
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        let _ = fs::set_permissions(&work, fs::Permissions::from_mode(0o700));
    }
    // Layers extract first, then the code, so code files win conflicts —
    // the bootstrap receives them in that order and unpacks into cwd.
    let mut extract_args: Vec<PathBuf> = Vec::new();
    for (i, layer) in layer_zips.iter().enumerate() {
        let path = work.join(format!("layer{}.zip", i));
        if fs::write(&path, layer).is_err() {
            return runtime_broken("failed to write layer package".to_string(), String::new());
        }
        extract_args.push(path);
    }
    if fs::write(work.join("code.zip"), code_zip).is_err() {
        return runtime_broken(
            "failed to write deployment package".to_string(),
            String::new(),
        );
    }
    extract_args.push(work.join("code.zip"));
    let event_str = if event.trim().is_empty() {
        "{}".to_string()
    } else {
        event.to_string()
    };
    let event_path = work.join("event.json");
    if fs::write(&event_path, event_str).is_err() {
        return runtime_broken("failed to write event".to_string(), String::new());
    }
    let bootstrap = work.join("lambda_bootstrap.js");
    if fs::write(&bootstrap, BOOTSTRAP_JS).is_err() {
        return runtime_broken(
            "failed to write handler bootstrap".to_string(),
            String::new(),
        );
    }
    // The bootstrap unpacks everything into its cwd, which doubles as
    // NODE_PATH so code/layers resolve bare requires.
    let code_dir = work.join("code");
    if fs::create_dir_all(&code_dir).is_err() {
        return runtime_broken("failed to create code dir".to_string(), String::new());
    }
    let node_path = [
        code_dir.clone(),
        code_dir.join("node_modules"),
        code_dir.join("nodejs"),
        code_dir.join("nodejs").join("node_modules"),
    ]
    .iter()
    .map(|p| p.to_string_lossy().into_owned())
    .collect::<Vec<String>>()
    .join(":");

    let event_file = match fs::File::open(&event_path) {
        Ok(f) => f,
        Err(e) => return runtime_broken(e.to_string(), String::new()),
    };
    let mut cmd =
        std::process::Command::new(find_node(runtime).unwrap_or_else(|| "node".to_string()));
    cmd.arg(bootstrap)
        .args(&extract_args)
        .current_dir(&code_dir)
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .stdin(Stdio::from(event_file));
    // Era build_env ordering via the shared helper (user env first,
    // built-ins after, credential trio setdefault); NODE_PATH rides the
    // built-ins block — the runtime picks the module resolution root.
    apply_era_env(
        &mut cmd,
        env_vars,
        &[
            ("NODE_PATH", node_path.clone()),
            ("AWS_LAMBDA_FUNCTION_NAME", function_name.to_string()),
            ("AWS_LAMBDA_FUNCTION_VERSION", function_version.to_string()),
            ("AWS_LAMBDA_FUNCTION_MEMORY_SIZE", memory_size.to_string()),
            ("AWS_LAMBDA_FUNCTION_TIMEOUT", timeout_secs.to_string()),
            ("AWS_REGION", region.to_string()),
            ("AWS_DEFAULT_REGION", region.to_string()),
            ("AWS_ACCOUNT_ID", account_id.to_string()),
            ("_HANDLER", handler.to_string()),
        ],
    );

    let mut child: Child = match cmd.spawn() {
        Ok(c) => c,
        Err(e) => return runtime_broken(format!("Runtime not found: {e}"), String::new()),
    };

    // Drain pipes on dedicated threads (bounded tail) so the child never
    // deadlocks on a full pipe buffer while nobody reads. The node result
    // rides stdout — that channel needs the 6MB result budget (the 128KB
    // log cap's tail semantics silently corrupt a larger JSON payload);
    // stderr is the log channel and stays at the log cap.
    let stdout_rx = drain_thread(child.stdout.take(), FAMILY_RESULT_CAP);
    let stderr_rx = drain_thread(child.stderr.take(), LOCAL_OUTPUT_CAP);

    // Grace period beyond the Lambda timeout, as in the Python-era twin.
    let wait_secs = u64::from(timeout_secs.max(1)) + 2;
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(wait_secs);
    let mut timed_out = false;
    let mut status: Option<std::process::ExitStatus> = None;
    loop {
        match child.try_wait() {
            Ok(Some(s)) => {
                status = Some(s);
                break;
            }
            Ok(None) => {
                if std::time::Instant::now() > deadline {
                    let _ = child.kill();
                    let _ = child.wait();
                    timed_out = true;
                    break;
                }
                std::thread::sleep(std::time::Duration::from_millis(25));
            }
            Err(e) => return runtime_broken(e.to_string(), String::new()),
        }
    }

    // Bounded drain grace: the child is reaped, but grandchildren may hold
    // the pipes open past our deadline, in which case abandon the text.
    let drain_deadline = std::time::Instant::now() + std::time::Duration::from_secs(1);
    let stdout = collect_by(&stdout_rx, drain_deadline);
    let stderr = collect_by(&stderr_rx, drain_deadline);
    cleanup(&work);

    if timed_out {
        return ExecResult {
            payload: json!({
                "errorMessage": format!("Task timed out after {timeout_secs}.00 seconds"),
                "errorType": "Task.TimedOut",
            })
            .to_string(),
            function_error: Some("Task.TimedOut".to_string()),
            logs: format!("Function timed out after {timeout_secs}s"),
        };
    }

    let stdout_trim = stdout.trim();
    if status.is_some_and(|s| s.success()) {
        if stdout_trim.is_empty() {
            return ExecResult {
                payload: "null".to_string(),
                function_error: None,
                logs: stderr,
            };
        }
        // Fail-loud at the capture budget — the same boundary
        // run_subprocess_family enforces: the drain keeps a bounded TAIL, so
        // a result at/over the 6MB window cannot be trusted as JSON and
        // must not re-encode as a fragment-per-data payload.
        if stdout.len() >= FAMILY_RESULT_CAP - 1 {
            return response_too_large_result(stderr);
        }
        // Handlers always emit JSON; non-JSON stdout is surfaced as a JSON
        // string, exactly like the Python-era twin's raw-string fallback.
        let payload = if serde_json::from_str::<Value>(stdout_trim).is_ok() {
            stdout_trim.to_string()
        } else {
            json!(stdout_trim).to_string()
        };
        return ExecResult {
            payload,
            function_error: None,
            logs: stderr,
        };
    }
    // Failure: a stdout JSON object announcing an errorMessage is a handler
    // error → "Handled"; anything else is a runtime crash → "Unhandled".
    let is_handler_error = serde_json::from_str::<Value>(stdout_trim)
        .ok()
        .filter(|obj| obj.is_object() && obj.get("errorMessage").is_some())
        .is_some();
    if is_handler_error {
        return ExecResult {
            payload: stdout_trim.to_string(),
            function_error: Some("Handled".to_string()),
            logs: stderr,
        };
    }
    // No structured error: fall back to stdout or the last log line.
    let error_message = if !stdout_trim.is_empty() {
        stdout_trim.to_string()
    } else {
        let last_log = stderr.lines().last().unwrap_or("");
        if last_log.is_empty() {
            "Unknown error".to_string()
        } else {
            last_log.to_string()
        }
    };
    ExecResult {
        payload: json!({
            "errorMessage": error_message,
            "errorType": "Runtime.ExitError",
            "stackTrace": stderr.lines().collect::<Vec<&str>>(),
        })
        .to_string(),
        function_error: Some("Unhandled".to_string()),
        logs: stderr,
    }
}

// === managed families: ruby / java / dotnet (ports of the python-era
// `runtimes/{ruby,java,dotnet}.py` + their bootstraps) ===
//
// The python-era twin ran these families on the twin's host with
// `run_subprocess` (event JSON on stdin, result JSON on stdout, logs on
// stderr). The docker executor re-runs the same era bridges INSIDE the
// mapped official images (see `docker_runtime::LOOSE_BRIDGE_FAMILIES`); these
// executors are the host-toolchain variant of the exact same flow, sharing the
// bridge sources through `docker_runtime::loose_bridge_dir()`.

/// Whether this runtime family executes through the Ruby bootstrap.
pub fn is_ruby_runtime(runtime: &str) -> bool {
    runtime.trim().to_ascii_lowercase().starts_with("ruby")
}

/// Whether this runtime family executes through the Java bridge.
pub fn is_java_runtime(runtime: &str) -> bool {
    runtime.trim().to_ascii_lowercase().starts_with("java")
}

/// Whether this runtime family executes through the .NET bridge.
pub fn is_dotnet_runtime(runtime: &str) -> bool {
    runtime.trim().to_ascii_lowercase().starts_with("dotnet")
}

/// `shutil.which` equivalent over PATH without spawning a process
/// (`find_node` scan style). Exec-bit variants of the probes are not
/// consulted; a file present on PATH is treated as executable, like the
/// sibling probes (`server.rs which()`).
fn which_bin(bin: &str) -> Option<String> {
    if let Some(paths) = std::env::var_os("PATH") {
        for dir in std::env::split_paths(&paths) {
            let candidate = dir.join(bin);
            if candidate.is_file() {
                return Some(candidate.to_string_lossy().into_owned());
            }
        }
    }
    None
}

/// Era `runtimes/ruby.py::_RUNTIME_BINARY`: Lambda identifiers preferred to
/// the versioned ruby binary present in the image, falling back to plain
/// `ruby` (the python-era fault-in installer was a download path — the twin
/// must stay offline-runnable, so only PATH-resolvable binaries surface).
const RUBY_RUNTIME_BINARIES: &[(&str, &str)] = &[
    ("ruby3.2", "ruby3.2"),
    ("ruby3.3", "ruby3.3"),
    ("ruby3.4", "ruby3.4"),
];

/// Era `runtimes/java.py::_RUNTIME_BINARY` (java8.al2 shares the java8
/// binary — JVM 21 runs older bytecode, matching the era's symlink posture).
const JAVA_RUNTIME_BINARIES: &[(&str, &str)] = &[
    ("java8", "java8"),
    ("java8.al2", "java8"),
    ("java11", "java11"),
    ("java17", "java17"),
    ("java21", "java21"),
];

fn java_home() -> Option<std::path::PathBuf> {
    std::env::var_os("JAVA_HOME")
        .map(std::path::PathBuf::from)
        .filter(|p| p.is_dir())
}

/// Ruby binary resolution, era `_resolve_binary`: versioned binary per the
/// runtime identifier first, plain `ruby` as the fallback. `ROBOTOCORE_RUBY`
/// pins the interpreter like `ROBOTOCORE_PY`/`ROBOTOCORE_NODE` do upstream.
fn find_ruby(runtime: &str) -> Option<String> {
    if let Ok(path) = std::env::var("ROBOTOCORE_RUBY") {
        if !path.is_empty() {
            return Some(path);
        }
    }
    for (rt, bin) in RUBY_RUNTIME_BINARIES {
        if *rt == runtime.trim() {
            if let Some(found) = which_bin(bin) {
                return Some(found);
            }
            break; // versioned binary missing — fall through to the default
        }
    }
    which_bin("ruby")
}

/// Java binary resolution, era `_resolve_binary`: versioned binary first, the
/// default `java` second, `$JAVA_HOME/bin/java` last (Dockerfile images export
/// JAVA_HOME; hosts with only a PATH `java` resolve through the which scan).
fn find_java(runtime: &str) -> Option<String> {
    if let Ok(path) = std::env::var("ROBOTOCORE_JAVA") {
        if !path.is_empty() {
            return Some(path);
        }
    }
    for (rt, bin) in JAVA_RUNTIME_BINARIES {
        if *rt == runtime.trim() {
            if let Some(found) = which_bin(bin) {
                return Some(found);
            }
            break;
        }
    }
    if let Some(home) = java_home() {
        let candidate = home.join("bin").join("java");
        if candidate.is_file() {
            return Some(candidate.to_string_lossy().into_owned());
        }
    }
    which_bin("java")
}

fn find_javac() -> Option<String> {
    if let Ok(path) = std::env::var("ROBOTOCORE_JAVAC") {
        if !path.is_empty() {
            return Some(path);
        }
    }
    if let Some(found) = which_bin("javac") {
        return Some(found);
    }
    if let Some(home) = java_home() {
        let candidate = home.join("bin").join("javac");
        if candidate.is_file() {
            return Some(candidate.to_string_lossy().into_owned());
        }
    }
    None
}

/// dotnet CLI resolution (era: `shutil.which("dotnet")`), with the
/// `$DOTNET_ROOT/dotnet` layout the install script produces as fallback.
fn find_dotnet() -> Option<String> {
    if let Ok(path) = std::env::var("ROBOTOCORE_DOTNET") {
        if !path.is_empty() {
            return Some(path);
        }
    }
    if let Some(found) = which_bin("dotnet") {
        return Some(found);
    }
    if let Some(root) = std::env::var_os("DOTNET_ROOT").map(std::path::PathBuf::from) {
        let candidate = root.join("dotnet");
        if candidate.is_file() {
            return Some(candidate.to_string_lossy().into_owned());
        }
    }
    None
}

/// Extract layers (first, so function code wins conflicts — the python-era
/// code-cache ordering every executor here mirrors) then the deployment
/// package into `work`.
fn extract_family_code(
    code_zip: &[u8],
    layer_zips: &[Vec<u8>],
    work: &std::path::Path,
) -> Result<(), String> {
    for layer in layer_zips {
        // A broken layer zip is tolerated, like the custom-runtime executor.
        let _ = extract_zip_into(layer, work);
    }
    extract_zip_into(code_zip, work)
}

/// Era `runtimes/base.py build_env` ordering shared by every executor:
/// user-configured env vars go down FIRST, Lambda built-ins are assigned
/// AFTER (the built-ins win for `AWS_LAMBDA_*`, region, `AWS_ACCOUNT_ID`,
/// `_HANDLER` — a spoofing user value can't lie in `context.function_name`),
/// and the credential trio follows `setdefault` semantics (user-configured
/// `AWS_ACCESS_KEY_ID`/`AWS_SECRET_ACCESS_KEY`/`AWS_SESSION_TOKEN` are
/// preserved; the `testing` defaults fill only the gaps).
fn apply_era_env(
    cmd: &mut std::process::Command,
    env_vars: &HashMap<String, String>,
    builtins: &[(&str, String)],
) {
    for (k, v) in env_vars {
        cmd.env(k, v);
    }
    for (k, v) in builtins {
        cmd.env(k, v);
    }
    for cred in [
        "AWS_ACCESS_KEY_ID",
        "AWS_SECRET_ACCESS_KEY",
        "AWS_SESSION_TOKEN",
    ] {
        if !env_vars.contains_key(cred) {
            cmd.env(cred, "testing");
        }
    }
}

/// Builtin Lambda env pairs for the managed-family executors — the same core
/// set the docker loose-bridge prefix sets (`loose_bridge_run_prefix`), so a
/// handler sees identical context inputs whether the invocation ran locally
/// or inside a mapped runtime image. Ordering follows era `build_env` via
/// [`apply_era_env`]: user overrides first, builtins last, credentials
/// setdefault.
#[allow(clippy::too_many_arguments)]
fn apply_family_env(
    cmd: &mut std::process::Command,
    function_name: &str,
    function_version: &str,
    function_arn: &str,
    handler: &str,
    env_vars: &HashMap<String, String>,
    timeout_secs: u32,
    memory_size: u32,
    region: &str,
    account_id: &str,
) {
    apply_era_env(
        cmd,
        env_vars,
        &[
            ("AWS_LAMBDA_FUNCTION_NAME", function_name.to_string()),
            ("AWS_LAMBDA_FUNCTION_VERSION", function_version.to_string()),
            ("AWS_LAMBDA_FUNCTION_ARN", function_arn.to_string()),
            ("AWS_LAMBDA_FUNCTION_MEMORY_SIZE", memory_size.to_string()),
            ("AWS_LAMBDA_FUNCTION_TIMEOUT", timeout_secs.to_string()),
            ("AWS_REGION", region.to_string()),
            ("AWS_DEFAULT_REGION", region.to_string()),
            ("AWS_ACCOUNT_ID", account_id.to_string()),
            (
                "AWS_LAMBDA_LOG_GROUP_NAME",
                format!("/aws/lambda/{function_name}"),
            ),
            ("AWS_LAMBDA_LOG_STREAM_NAME", shim_log_stream_name()),
            ("_HANDLER", handler.to_string()),
        ],
    );
}

/// The shim's per-invocation log-stream name (`YYYY/MM/DD/[$LATEST]{uuid32}`),
/// same env value the docker loose-bridge prefix sets via
/// `docker_runtime::shim_log_stream_name`.
fn shim_log_stream_name() -> String {
    let date = chrono::Utc::now().format("%Y/%m/%d");
    format!("{date}/[$LATEST]{}", uuid::Uuid::new_v4().simple())
}

/// Era `runtimes/base.py::run_subprocess` port for the managed-family
/// executors: spawn `cmd` (cwd + env already set), event JSON on stdin, JSON
/// result on stdout, logs on stderr. Deadlines: `timeout + 2` grace then kill
/// (python's `timeout=timeout+2`), bounded pipe drains. Response rules: exit
/// 0 → stdout parsed as JSON (raw string fallback, empty → "null");
/// non-zero → stdout error JSON maps to "Handled", anything else becomes
/// Runtime.ExitError/"Unhandled"; spawn `NotFound` (python
/// `FileNotFoundError`) is `Runtime.InvalidRuntime`.
fn run_subprocess_family(
    cmd: &mut std::process::Command,
    event: &str,
    timeout_secs: u32,
) -> ExecResult {
    let event_json = if event.trim().is_empty() {
        "{}".to_string()
    } else {
        match serde_json::from_str::<Value>(event) {
            Ok(v) => v.to_string(),
            Err(_) => event.to_string(),
        }
    };
    cmd.stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .stdin(Stdio::piped());
    let mut child: Child = match cmd.spawn() {
        Ok(c) => c,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
            // Era: FileNotFoundError → (None, "Runtime.InvalidRuntime", ...)
            return ExecResult {
                payload: "null".to_string(),
                function_error: Some("Runtime.InvalidRuntime".to_string()),
                logs: format!("Runtime not found: {e}"),
            };
        }
        Err(e) => return missing_runtime_result(&e.to_string()),
    };
    // Era parity: `subprocess.run(input=...)`'s communicate() feeds stdin
    // while draining BOTH pipes concurrently — install the pipe drains
    // BEFORE touching the (possibly blocking) write and feed stdin from a
    // dedicated thread, or a bootstrap that fills its stdout pipe (or never
    // reads stdin at all) would wedge the synchronous write and the
    // `timeout+2` deadline would never be consulted (review-found hang
    // class).
    let (stdout_rx, stderr_rx) =
        communicate_event(&mut child, &event_json, FAMILY_RESULT_CAP, LOCAL_OUTPUT_CAP);

    let deadline = std::time::Instant::now()
        + std::time::Duration::from_secs(u64::from(timeout_secs.max(1)) + 2);
    let mut timed_out = false;
    let mut exit_code: Option<i32> = None;
    loop {
        match child.try_wait() {
            Ok(Some(status)) => {
                exit_code = status.code();
                break;
            }
            Ok(None) => {
                if std::time::Instant::now() > deadline {
                    let _ = child.kill();
                    let _ = child.wait();
                    timed_out = true;
                    break;
                }
                std::thread::sleep(std::time::Duration::from_millis(25));
            }
            Err(e) => return missing_runtime_result(&e.to_string()),
        }
    }
    // The child is reaped; grandchildren may hold the pipes — collect bounded.
    let drain_deadline = std::time::Instant::now() + std::time::Duration::from_secs(1);
    let stdout_raw = collect_by(&stdout_rx, drain_deadline);
    let stderr = collect_by(&stderr_rx, drain_deadline);
    let stdout = stdout_raw.trim().to_string();
    // Fail loud on a budget-overrun capture: the drain keeps a bounded TAIL
    // (the last cap bytes), and a handler that ran past the budget would
    // otherwise deliver a garbage tail-fragment re-encoded as a JSON string
    // with no error. Real AWS fails the invoke over the response cap.
    let stdout_hit_cap = stdout_raw.len() >= FAMILY_RESULT_CAP - 1;

    if timed_out {
        // Era TimeoutExpired branch: (error_obj, "Task.TimedOut", "Function
        // timed out after Ns").
        return crate::docker_runtime::run_timed_out_result(timeout_secs, "");
    }

    if exit_code.unwrap_or(-1) != 0 {
        // Structured error object on stdout wins over the exit code → Handled.
        if let Ok(parsed) = serde_json::from_str::<Value>(&stdout) {
            if parsed
                .as_object()
                .map(|o| o.contains_key("errorMessage"))
                .unwrap_or(false)
            {
                return ExecResult {
                    payload: parsed.to_string(),
                    function_error: Some("Handled".to_string()),
                    logs: stderr,
                };
            }
        }
        let error_message = if !stdout.is_empty() {
            stdout
        } else if !stderr.is_empty() {
            stderr.lines().next_back().unwrap_or("").to_string()
        } else {
            "Unknown error".to_string()
        };
        let stack_trace: Vec<String> = if stderr.is_empty() {
            Vec::new()
        } else {
            stderr.trim().split('\n').map(str::to_string).collect()
        };
        return ExecResult {
            payload: json!({
                "errorMessage": error_message,
                "errorType": "Runtime.ExitError",
                "stackTrace": stack_trace,
            })
            .to_string(),
            function_error: Some("Unhandled".to_string()),
            logs: stderr,
        };
    }

    if stdout.is_empty() {
        return ExecResult {
            payload: "null".to_string(),
            function_error: None,
            logs: stderr,
        };
    }
    // (n) fail-loud: a handler whose stdout hit the capture budget cannot be
    // trusted — its tail was dropped, so JSON parses (if any) arrive in the
    // WRONG reads or fail. Real AWS/moto answer Function.ResponseSizeTooLarge;
    // surface that instead of silently wrapping a fragment as a string
    // (moto's exact message text, errorType and Unhandled error family).
    if stdout_hit_cap {
        return response_too_large_result(stderr);
    }
    let payload = match serde_json::from_str::<Value>(&stdout) {
        Ok(v) => v.to_string(),
        // Not valid JSON — the era json-dumped the raw string.
        Err(_) => json!(stdout).to_string(),
    };
    ExecResult {
        payload,
        function_error: None,
        logs: stderr,
    }
}

/// Execute a Ruby Lambda handler through the era bootstrap (host `ruby`).
///
/// Port of the python-era `runtimes/ruby.py`: unzip the package (and layers)
/// into a per-invocation dir, run `ruby bootstrap.rb` with the event JSON on
/// stdin and the era env/RUBYOPT load paths; the module file is resolved from
/// the cwd, handler as `module.method` raising `event:`/`context:` keyword
/// args; result JSON on stdout, error JSON + non-zero exit on failure.
#[allow(clippy::too_many_arguments)]
pub fn execute_ruby(
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
    let Some(ruby_bin) = find_ruby(runtime) else {
        return ExecResult {
            payload: "null".to_string(),
            function_error: Some("Runtime.InvalidRuntime".to_string()),
            logs: "Ruby not installed".to_string(),
        };
    };
    let work = std::env::temp_dir().join(format!(
        "robotocore-lambda-exec/{}-{}",
        safe_dir_name(function_name),
        uuid::Uuid::new_v4().simple()
    ));
    let cleanup = |p: &PathBuf| {
        let _ = fs::remove_dir_all(p);
    };
    if let Err(e) = fs::create_dir_all(&work) {
        return missing_runtime_result(&e.to_string());
    }
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        let _ = fs::set_permissions(&work, fs::Permissions::from_mode(0o700));
    }
    if let Err(e) = extract_family_code(code_zip, layer_zips, &work) {
        cleanup(&work);
        return missing_runtime_result(&e);
    }
    let mut cmd = std::process::Command::new(ruby_bin);
    // Era bootstrap path is fixed inside the twin-owned bridge dir.
    cmd.arg(crate::docker_runtime::loose_bridge_dir().join("bootstrap.rb"));
    cmd.current_dir(&work);
    apply_family_env(
        &mut cmd,
        function_name,
        function_version,
        function_arn,
        handler,
        env_vars,
        timeout_secs,
        memory_size,
        region,
        account_id,
    );
    // Era `runtimes/ruby.py`: layer `ruby/` subtrees and the code dir go on
    // the load path via RUBYOPT; the gems dir underneath rides GEM_PATH.
    let mut rubyopt_parts: Vec<String> = Vec::new();
    let ruby_dir = work.join("ruby");
    if ruby_dir.is_dir() {
        rubyopt_parts.push(format!("-I{}", ruby_dir.to_string_lossy()));
    }
    rubyopt_parts.push(format!("-I{}", work.to_string_lossy()));
    cmd.env("RUBYOPT", rubyopt_parts.join(" "));
    cmd.env("GEM_PATH", ruby_dir.join("gems"));

    let result = run_subprocess_family(&mut cmd, event, timeout_secs);
    cleanup(&work);
    result
}

/// Compile the era `Bootstrap.java`-derived bridge once per process (era
/// `_ensure_bootstrap_compiled`): `javac --release 8` so the class loads on
/// any JVM ≥ 8, cached in the twin-owned bridge dir. `None` when no javac or
/// the compile fails (era's "Cannot compile Java bootstrap" InvalidRuntime).
fn ensure_java_bootstrap() -> Option<std::path::PathBuf> {
    // The inner None memoizes a failed compile: without it a broken
    // toolchain re-ran `javac` twice under this process-wide mutex on EVERY
    // java invoke, serializing the pool behind 2×30s retries (review round
    // 2). Era retried per invoke too but never serialized (per-call
    // mkdtemp); a javac-broken host cannot self-heal mid-process, so the
    // memo only clears on restart.
    static CACHE: parking_lot::Mutex<Option<Option<std::path::PathBuf>>> =
        parking_lot::Mutex::new(None);
    // Held across the compile: two concurrent java invocations must not race
    // javac into the shared class dir (docker `ensure_dotnet_bridge`
    // serializes its build the same way).
    let mut guard = CACHE.lock();
    if let Some(cached) = guard.as_ref() {
        if let Some(dir) = cached {
            // Pruned-cache guard: a next-start twin's sweep reaps bridge
            // dirs older than an hour; revalidate before reuse.
            if dir.join("RobotocoreJavaBridge.class").is_file() {
                return Some(dir.clone());
            }
        } else {
            return None; // memoized compile failure
        }
    }
    let Some(javac) = find_javac() else {
        *guard = Some(None);
        return None;
    };
    let bridge_dir = crate::docker_runtime::loose_bridge_dir();
    let source = bridge_dir.join("RobotocoreJavaBridge.java");
    let out_dir = bridge_dir.join("bootstrap-classes");
    let _ = fs::create_dir_all(&out_dir);
    let dir_arg = out_dir.to_string_lossy().into_owned();
    let source_arg = source.to_string_lossy().into_owned();
    // `--release 8` (era) needs javac ≥ 9; JDK 8's javac predates it — retry
    // without the flag before giving up.
    let argv8 = vec![
        javac.clone(),
        "--release".to_string(),
        "8".to_string(),
        "-d".to_string(),
        dir_arg.clone(),
        source_arg.clone(),
    ];
    let mut ok = crate::docker_runtime::run_subprocess(&argv8, None, 30)
        .map(|o| o.status_code == Some(0))
        .unwrap_or(false);
    if !ok {
        let argv = vec![
            javac.clone(),
            "-d".to_string(),
            dir_arg.clone(),
            source_arg.clone(),
        ];
        ok = crate::docker_runtime::run_subprocess(&argv, None, 30)
            .map(|o| o.status_code == Some(0))
            .unwrap_or(false);
    }
    if ok && out_dir.join("RobotocoreJavaBridge.class").is_file() {
        *guard = Some(Some(out_dir.clone()));
        Some(out_dir)
    } else {
        *guard = Some(None);
        None
    }
}

/// Execute a Java Lambda handler through the era bridge (`Bootstrap.java`).
///
/// Port of the python-era `runtimes/java.py`: compile the bootstrap once
/// (`javac --release 8`), unzip the package, build the era classpath
/// (bootstrap dir, code dir, `lib/*.jar`, root `*.jar`, `java/` and
/// `java/lib/*.jar` layer dirs), then run `java -cp <cp> -Xmx{memory}m
/// RobotocoreJavaBridge` with the event JSON on stdin (handler spec from
/// `_HANDLER` as `Class::method`, `Map`-param handlers get the era `_raw`
/// wrapper). Era response rules via [`run_subprocess_family`].
#[allow(clippy::too_many_arguments)]
pub fn execute_java(
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
    let Some(java_bin) = find_java(runtime) else {
        return ExecResult {
            payload: "null".to_string(),
            function_error: Some("Runtime.InvalidRuntime".to_string()),
            logs: "Java not installed".to_string(),
        };
    };
    let Some(bootstrap_dir) = ensure_java_bootstrap() else {
        return ExecResult {
            payload: "null".to_string(),
            function_error: Some("Runtime.InvalidRuntime".to_string()),
            logs: "Cannot compile Java bootstrap (javac not found)".to_string(),
        };
    };
    let work = std::env::temp_dir().join(format!(
        "robotocore-lambda-exec/{}-{}",
        safe_dir_name(function_name),
        uuid::Uuid::new_v4().simple()
    ));
    let cleanup = |p: &PathBuf| {
        let _ = fs::remove_dir_all(p);
    };
    if let Err(e) = fs::create_dir_all(&work) {
        return missing_runtime_result(&e.to_string());
    }
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        let _ = fs::set_permissions(&work, fs::Permissions::from_mode(0o700));
    }
    if let Err(e) = extract_family_code(code_zip, layer_zips, &work) {
        cleanup(&work);
        return missing_runtime_result(&e);
    }

    // Era classpath order: bootstrap dir, code dir, lib/*.jar, root *.jar,
    // java/ (layer dir), java/lib/*.jar. Sorted for determinism (the era's
    // glob order was OS-defined and unrelied-on).
    let join_jars = |dir: &std::path::Path| -> Vec<String> {
        let mut out = Vec::new();
        if let Ok(entries) = fs::read_dir(dir) {
            for entry in entries.flatten() {
                let path = entry.path();
                if path.extension().and_then(|e| e.to_str()) == Some("jar") {
                    out.push(path.to_string_lossy().into_owned());
                }
            }
        }
        out.sort();
        out
    };
    let mut cp_parts: Vec<String> = vec![
        bootstrap_dir.to_string_lossy().into_owned(),
        work.to_string_lossy().into_owned(),
    ];
    cp_parts.extend(join_jars(&work.join("lib")));
    cp_parts.extend(join_jars(&work));
    let java_dir = work.join("java");
    if java_dir.is_dir() {
        cp_parts.push(java_dir.to_string_lossy().into_owned());
        cp_parts.extend(join_jars(&java_dir.join("lib")));
    }
    let classpath = cp_parts.join(":");

    let mut cmd = std::process::Command::new(java_bin);
    cmd.arg("-cp")
        .arg(classpath)
        .arg(format!("-Xmx{memory_size}m"))
        .arg("RobotocoreJavaBridge")
        .current_dir(&work);
    apply_family_env(
        &mut cmd,
        function_name,
        function_version,
        function_arn,
        handler,
        env_vars,
        timeout_secs,
        memory_size,
        region,
        account_id,
    );

    let result = run_subprocess_family(&mut cmd, event, timeout_secs);
    cleanup(&work);
    result
}

/// Port of the era dotnet reflection bootstrap (`BOOTSTRAP_CS`): loads the
/// user assembly from the working directory, falling back to
/// `LAMBDA_CODE_DIR`; invokes `method(eventJson, null)` / `(eventJson)` /
/// `()` per arity; prints the result `ToString` (or literal `null`) to
/// stdout; error JSON with the exception type name + exit 1 on failure.
const DOTNET_LOCAL_BOOTSTRAP_CS: &str = r##"using System;
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
                // Try the code directory (bootstrap runs from its own build dir)
                string codeDir = Environment.GetEnvironmentVariable("LAMBDA_CODE_DIR") ?? "";
                if (!string.IsNullOrEmpty(codeDir))
                    dllPath = Path.Combine(codeDir, assemblyName + ".dll");
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
"##;

/// Era `BOOTSTRAP_CSPROJ` minus the unused user-assembly `<Reference>` (the
/// bootstrap reflects, it does not compile against the handler — one cached
/// binary serves every function). `ImportDirectoryBuild*` are pinned off so
/// no host-level MSBuild hook can leak into the twin-owned bridge dir.
const DOTNET_LOCAL_BOOTSTRAP_CSPROJ: &str = "<Project Sdk=\"Microsoft.NET.Sdk\">\n  <PropertyGroup>\n    <OutputType>Exe</OutputType>\n    <TargetFramework>{tfm}</TargetFramework>\n    <ImplicitUsings>enable</ImplicitUsings>\n    <ImportDirectoryBuildProps>false</ImportDirectoryBuildProps>\n    <ImportDirectoryBuildTargets>false</ImportDirectoryBuildTargets>\n  </PropertyGroup>\n</Project>\n";

/// Era `_dotnet_compile_env`: the vars every dotnet CLI subprocess needs to
/// run headless/offline (telemetry off, first-run experience skipped,
/// globalization invariant for libicu-less images).
fn apply_dotnet_compile_env(cmd: &mut std::process::Command) {
    cmd.env("DOTNET_CLI_TELEMETRY_OPTOUT", "1")
        .env("DOTNET_NOLOGO", "1")
        .env("DOTNET_SKIP_FIRST_TIME_EXPERIENCE", "1")
        .env("DOTNET_SYSTEM_GLOBALIZATION_INVARIANT", "1");
}

/// Era `_detect_tfm`: the host's best installed .NET runtime major, cached
/// per process, `net8.0` when detection fails. Local builds compile against
/// what the host actually runs — never a NuGet restore of a foreign
/// targeting pack (the twin must stay offline-runnable). Public so the
/// bridge tests stage their helper assembly against the same TFM the
/// executor will use.
pub fn detect_dotnet_tfm() -> String {
    static CACHE: parking_lot::Mutex<Option<String>> = parking_lot::Mutex::new(None);
    if let Some(tfm) = CACHE.lock().as_ref() {
        return tfm.clone();
    }
    let mut tfm = "net8.0".to_string();
    if let Some(dotnet) = find_dotnet() {
        let mut cmd = std::process::Command::new(&dotnet);
        cmd.arg("--list-runtimes");
        apply_dotnet_compile_env(&mut cmd);
        let detected = run_bounded(cmd, 10).and_then(|(code, stdout, _)| {
            if code != Some(0) {
                return None;
            }
            let mut best = 0i64;
            for line in stdout.lines() {
                if !line.contains("Microsoft.NETCore.App") {
                    continue;
                }
                let major = line
                    .split_whitespace()
                    .nth(1)
                    .and_then(|v| v.split('.').next())
                    .and_then(|v| v.parse::<i64>().ok())
                    .unwrap_or(0);
                if major > best {
                    best = major;
                }
            }
            (best > 0).then(|| format!("net{best}.0"))
        });
        if let Some(detected) = detected {
            tfm = detected;
        }
    }
    *CACHE.lock() = Some(tfm.clone());
    tfm
}

/// Run a build/introspection subprocess with bounded waiting: stdout/stderr
/// drained on the executor's drain threads, killed at the deadline. Returns
/// `(status code, stdout, stderr)`.
pub(crate) fn run_bounded(
    mut cmd: std::process::Command,
    timeout_secs: u64,
) -> Option<(Option<i32>, String, String)> {
    cmd.stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .stdin(Stdio::null());
    let mut child: Child = cmd.spawn().ok()?;
    let stdout_rx = drain_thread(child.stdout.take(), LOCAL_OUTPUT_CAP);
    let stderr_rx = drain_thread(child.stderr.take(), LOCAL_OUTPUT_CAP);
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(timeout_secs.max(1));
    let status_code = loop {
        match child.try_wait() {
            Ok(Some(status)) => break status.code(),
            Ok(None) => {
                if std::time::Instant::now() > deadline {
                    let _ = child.kill();
                    let _ = child.wait();
                    return None;
                }
                std::thread::sleep(std::time::Duration::from_millis(25));
            }
            Err(_) => return None,
        }
    };
    let drain_deadline = std::time::Instant::now() + std::time::Duration::from_secs(1);
    Some((
        status_code,
        collect_by(&stdout_rx, drain_deadline),
        collect_by(&stderr_rx, drain_deadline),
    ))
}

/// Build the local .NET reflection bootstrap once per TFM (era
/// `_run_with_bootstrap` built per invocation; the era java/dotnet bootstrap
/// caches and the docker `ensure_dotnet_bridge` cache set the process-wide
/// precedent). Cached in the twin-owned bridge dir under
/// `dotnet-local-{tfm}`, revalidated (a next-start twin's sweep prunes stale
/// bridge dirs). `None` when no dotnet CLI or the build fails.
fn ensure_dotnet_local_bootstrap(tfm: &str) -> Option<std::path::PathBuf> {
    static CACHE: parking_lot::Mutex<Option<HashMap<String, Option<std::path::PathBuf>>>> =
        parking_lot::Mutex::new(None);
    // Held across the build: concurrent first invokes must not race msbuild
    // into the shared project dir (docker `ensure_dotnet_bridge` serializes
    // its build the same way).
    let mut guard = CACHE.lock();
    if let Some(map) = guard.as_ref() {
        match map.get(tfm) {
            Some(Some(dir)) => {
                // Pruned-cache guard: a next-start twin's sweep reaps bridge
                // dirs older than an hour; revalidate before use.
                if dir.join("Bootstrap.dll").is_file() {
                    return Some(dir.clone());
                }
            }
            Some(None) => return None, // cached failure repeats
            None => {}
        }
    }
    let Some(dotnet) = find_dotnet() else {
        // Cache the miss like a failed build (era surfaced "no SDK" fast).
        guard
            .get_or_insert_with(HashMap::new)
            .insert(tfm.to_string(), None);
        return None;
    };
    let dir = crate::docker_runtime::loose_bridge_dir().join(format!("dotnet-local-{tfm}"));
    let out_dir = dir.join("out");
    let _ = fs::create_dir_all(&out_dir);
    if fs::write(dir.join("Bootstrap.cs"), DOTNET_LOCAL_BOOTSTRAP_CS).is_err() {
        return None;
    }
    let csproj = DOTNET_LOCAL_BOOTSTRAP_CSPROJ.replace("{tfm}", tfm);
    if fs::write(dir.join("Bootstrap.csproj"), csproj).is_err() {
        return None;
    }
    let mut cmd = std::process::Command::new(&dotnet);
    cmd.arg("build")
        .arg(dir.join("Bootstrap.csproj"))
        .arg("-c")
        .arg("Release")
        .arg("-o")
        .arg(&out_dir)
        .arg("--nologo")
        .arg("-v")
        .arg("quiet")
        .current_dir(&dir);
    apply_dotnet_compile_env(&mut cmd);
    let built = run_bounded(cmd, 45)
        .map(|(code, _, _)| code == Some(0))
        .unwrap_or(false)
        && out_dir.join("Bootstrap.dll").is_file();
    let result = if built { Some(out_dir.clone()) } else { None };
    guard
        .get_or_insert_with(HashMap::new)
        .insert(tfm.to_string(), result.clone());
    result
}

/// Execute a .NET Lambda handler via the era `runtimes/dotnet.py` flow.
///
/// Handler format `Assembly::Type::Method`. Pre-compiled class libraries
/// (with or without `runtimeconfig.json`) run as built; a package carrying
/// `.cs` sources and no DLL compiles first through the hardened
/// [`docker_runtime::compile_dotnet_source_if_needed`] staging (csproj guard
/// + escape-proof staging dir + subdirectory build); a self-contained app
/// (own `runtimeconfig.json`) runs `dotnet exec <assembly.dll>` directly,
/// a class library runs `dotnet exec <bootstrap.dll>` from the cached
/// reflection-bootstrap dir with `LAMBDA_CODE_DIR` pointing at the code dir.
/// No NuGet/network at invoke time: build inputs are twin-owned csproj +
/// the package's sources only, compiled against the HOST's TFM
/// (`detect_dotnet_tfm`, era `_detect_tfm`) — the `runtime` identifier is
/// accepted for signature parity and otherwise informational.
#[allow(clippy::too_many_arguments)]
pub fn execute_dotnet(
    _runtime: &str,
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
    let Some(dotnet_bin) = find_dotnet() else {
        return ExecResult {
            payload: "null".to_string(),
            function_error: Some("Runtime.InvalidRuntime".to_string()),
            logs: ".NET SDK not installed".to_string(),
        };
    };
    let work = std::env::temp_dir().join(format!(
        "robotocore-lambda-exec/{}-{}",
        safe_dir_name(function_name),
        uuid::Uuid::new_v4().simple()
    ));
    let cleanup = |p: &PathBuf| {
        let _ = fs::remove_dir_all(p);
    };
    if let Err(e) = fs::create_dir_all(&work) {
        return missing_runtime_result(&e.to_string());
    }
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        let _ = fs::set_permissions(&work, fs::Permissions::from_mode(0o700));
    }
    if let Err(e) = extract_family_code(code_zip, layer_zips, &work) {
        cleanup(&work);
        return missing_runtime_result(&e);
    }

    // Era handler split: "Assembly::Type::Method" (parts[0] names the
    // assembly for the load path and generated csproj).
    let parts: Vec<&str> = handler.split("::").collect();
    if parts.len() < 3 {
        cleanup(&work);
        let message =
            format!("Bad .NET handler format: {handler}. Expected Assembly::Type::Method");
        return ExecResult {
            payload: json!({
                "errorMessage": message,
                "errorType": "Runtime.HandlerNotFound",
            })
            .to_string(),
            function_error: Some("Runtime.HandlerNotFound".to_string()),
            logs: message,
        };
    }
    let assembly_name = parts[0];
    // Same guard the docker-mode compile path enforces — applied to every
    // load path here (the name is joined into paths/csproj fragments).
    if let Some(violation) = crate::docker_runtime::dotnet_assembly_violation(assembly_name) {
        cleanup(&work);
        let message = format!("Invalid .NET handler assembly '{assembly_name}': {violation}");
        return ExecResult {
            payload: json!({
                "errorMessage": message,
                "errorType": "Runtime.ImportModuleError",
            })
            .to_string(),
            function_error: Some("Runtime.ImportModuleError".to_string()),
            logs: message,
        };
    }

    // Locate the assembly DLL: code dir first, then a publish/ subdir (era).
    let mut dll_path = work.join(format!("{assembly_name}.dll"));
    if !dll_path.exists() {
        dll_path = work.join("publish").join(format!("{assembly_name}.dll"));
    }
    if !dll_path.exists() {
        // No compiled DLL: compile .cs sources through the hardened staging
        // (era `_compile_source`). Some(result) is an era error condition.
        if let Some(err) = crate::docker_runtime::compile_dotnet_source_if_needed(
            &detect_dotnet_tfm(),
            &work,
            handler,
        ) {
            cleanup(&work);
            return err;
        }
        dll_path = work.join(format!("{assembly_name}.dll"));
    }

    let tfm = detect_dotnet_tfm();
    let runtimeconfig = dll_path
        .parent()
        .map(|dir| dir.join(format!("{assembly_name}.runtimeconfig.json")))
        .filter(|p| p.exists());
    let cmd = if runtimeconfig.is_some() {
        // Self-contained app: run directly (era `dotnet exec <assembly>`).
        let mut cmd = std::process::Command::new(&dotnet_bin);
        cmd.arg("exec").arg(&dll_path).current_dir(&work);
        cmd
    } else {
        // Class library: run the cached reflection bootstrap from its own
        // output dir, pointing LAMBDA_CODE_DIR at the code dir.
        let Some(bootstrap_dir) = ensure_dotnet_local_bootstrap(&tfm) else {
            cleanup(&work);
            return ExecResult {
                payload: json!({
                    "errorMessage": "Bootstrap compilation produced no output",
                    "errorType": "Runtime.CompilationError",
                })
                .to_string(),
                function_error: Some("Unhandled".to_string()),
                logs: "Bootstrap compilation produced no output".to_string(),
            };
        };
        let mut cmd = std::process::Command::new(&dotnet_bin);
        cmd.arg("exec")
            .arg(bootstrap_dir.join("Bootstrap.dll"))
            .current_dir(&bootstrap_dir);
        cmd
    };

    let mut cmd = cmd;
    apply_family_env(
        &mut cmd,
        function_name,
        function_version,
        function_arn,
        handler,
        env_vars,
        timeout_secs,
        memory_size,
        region,
        account_id,
    );
    // Era: invariant mode always wins over user env; LAMBDA_CODE_DIR after
    // the builtins.
    cmd.env("DOTNET_SYSTEM_GLOBALIZATION_INVARIANT", "1")
        .env("LAMBDA_CODE_DIR", &work);

    let result = run_subprocess_family(&mut cmd, event, timeout_secs);
    cleanup(&work);
    result
}

// === custom runtime execution (port/port-lambda-custom) ===
pub fn is_custom_runtime(runtime: &str) -> bool {
    let r = runtime.trim().to_ascii_lowercase();
    r.starts_with("provided") || r.starts_with("go")
}

/// Total decompressed-bytes bound per extraction (layer or code package).
/// Era `limits.py` allowed 250 MB unzipped per function; without the bound a
/// crafted 2 MB zip expands N-fold into /tmp on every invoke and fills the
/// disk (review round 2). The work dir is discarded on error so the prune
/// sweep reaps the partial files.
const ZIP_UNPACK_LIMIT_TOTAL: u64 = 250 * 1024 * 1024;

pub(crate) fn extract_zip_into(zip_bytes: &[u8], dest: &std::path::Path) -> Result<(), String> {
    let reader = std::io::Cursor::new(zip_bytes);
    let mut archive = zip::ZipArchive::new(reader).map_err(|e| e.to_string())?;
    let mut copied = 0u64;
    for i in 0..archive.len() {
        let mut entry = archive.by_index(i).map_err(|e| e.to_string())?;
        let Some(rel) = entry.enclosed_name() else {
            return Err(format!("unsafe path in ZIP archive entry {i}"));
        };
        let out = dest.join(rel);
        if entry.is_dir() {
            fs::create_dir_all(&out).map_err(|e| e.to_string())?;
        } else {
            if let Some(parent) = out.parent() {
                fs::create_dir_all(parent).map_err(|e| e.to_string())?;
            }
            if copy_entry_capped(&mut entry, &out, &mut copied)? {
                return Err(format!(
                    "ZIP archive expands past the {} MB unzipped limit",
                    ZIP_UNPACK_LIMIT_TOTAL / (1024 * 1024)
                ));
            }
            #[cfg(unix)]
            if let Some(mode) = entry.unix_mode() {
                use std::os::unix::fs::PermissionsExt;
                let _ = fs::set_permissions(&out, fs::Permissions::from_mode(mode & 0o777));
            }
        }
    }
    Ok(())
}

/// Copy `entry` into `out` while counting total bytes across the whole
/// archive; returns `true` the moment the shared budget would be exceeded
/// (partial write; the caller discards the extraction dir, so partial
/// files never reach an executor).
fn copy_entry_capped<R: std::io::Read>(
    entry: &mut R,
    out: &std::path::Path,
    copied: &mut u64,
) -> Result<bool, String> {
    let mut out_file = fs::File::create(out).map_err(|e| e.to_string())?;
    let mut chunk = [0u8; 8192];
    loop {
        let n = entry.read(&mut chunk).map_err(|e| e.to_string())?;
        if n == 0 {
            return Ok(false);
        }
        *copied += n as u64;
        if *copied > ZIP_UNPACK_LIMIT_TOTAL {
            return Ok(true);
        }
        out_file.write_all(&chunk[..n]).map_err(|e| e.to_string())?;
    }
}

/// The bound the Python shim (`safe_extract`) and the Node bootstrap
/// (`extractZip`) embed: the running decompressed total stays inside
/// [`ZIP_UNPACK_LIMIT_TOTAL`] or the extraction aborts with the shared cap
/// text before the offending entry writes anything. The Rust tests drive
/// this fn so the shim strings and this process's budget can never drift
/// apart on the limit's shape. (The shims implement the accumulation
/// themselves — they are process-external text — so this fn is the
/// test-controlled contract.)
#[cfg(test)]
pub(crate) fn zip_unpack_budget(running: u64, next_entry: u64) -> Result<u64, String> {
    let next = running.saturating_add(next_entry);
    if next > ZIP_UNPACK_LIMIT_TOTAL {
        return Err(format!(
            "ZIP archive expands past the {} MB unzipped limit",
            ZIP_UNPACK_LIMIT_TOTAL / (1024 * 1024)
        ));
    }
    Ok(next)
}

#[allow(clippy::too_many_arguments)]
pub fn execute_custom(
    code_zip: &[u8],
    layer_zips: &[Vec<u8>],
    handler: &str,
    event: &str,
    function_name: &str,
    function_version: &str,
    env_vars: &HashMap<String, String>,
    timeout_secs: u32,
    memory_size: u32,
    region: &str,
    account_id: &str,
) -> ExecResult {
    let work = std::env::temp_dir().join(format!(
        "robotocore-lambda-exec/{}-{}",
        safe_dir_name(function_name),
        uuid::Uuid::new_v4().simple()
    ));
    let cleanup = |p: &PathBuf| {
        let _ = fs::remove_dir_all(p);
    };
    let invalid_entrypoint = |work: Option<&PathBuf>| -> ExecResult {
        if let Some(p) = work {
            cleanup(p);
        }
        ExecResult {
            payload: "null".to_string(),
            function_error: Some("Runtime.InvalidEntrypoint".to_string()),
            logs: "No 'bootstrap' executable found in code package".to_string(),
        }
    };
    if let Err(e) = fs::create_dir_all(&work) {
        return missing_runtime_result(&e.to_string());
    }
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        let _ = fs::set_permissions(&work, fs::Permissions::from_mode(0o700));
    }
    // Layers first so function code overrides layer files, mirroring the
    // python-era code cache. A broken layer zip is tolerated (python-era
    // behavior warns and continues); a broken code zip is a hard failure.
    for layer in layer_zips {
        let _ = extract_zip_into(layer, &work);
    }
    if let Err(e) = extract_zip_into(code_zip, &work) {
        cleanup(&work);
        return missing_runtime_result(&e.to_string());
    }

    // Locate the bootstrap executable: `bootstrap` at the package root, else
    // named after the handler's base (some packages do that).
    let mut bootstrap_path = work.join("bootstrap");
    if !bootstrap_path.exists() {
        let base = if handler.contains('.') {
            handler.split('.').next().unwrap_or("")
        } else {
            handler
        };
        if base.is_empty() {
            return invalid_entrypoint(Some(&work));
        }
        bootstrap_path = work.join(base);
    }
    if !bootstrap_path.exists() {
        return invalid_entrypoint(Some(&work));
    }
    // The python-era executor chmods the entry even when the zip lost the
    // exec bit, so packages without +x still run.
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        if let Ok(st) = fs::metadata(&bootstrap_path) {
            let mode = st.permissions().mode() | 0o100; // S_IEXEC
            let _ = fs::set_permissions(&bootstrap_path, fs::Permissions::from_mode(mode));
        }
    }

    // Normalize the event the way the python-era provider did: it parsed the
    // request body and wrote it back with json.dumps (one compact line, which
    // the bootstrap scripts `read` as a single line).
    let event_str = if event.trim().is_empty() {
        "{}".to_string()
    } else {
        match serde_json::from_str::<Value>(event) {
            Ok(v) => v.to_string(),
            Err(_) => event.to_string(),
        }
    };

    let mut cmd = std::process::Command::new(&bootstrap_path);
    cmd.current_dir(&work)
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .stdin(Stdio::piped());
    // Era build_env ordering via the shared helper (user env first,
    // built-ins after, credential trio setdefault — a user-configured
    // AWS_ACCESS_KEY_ID survives, the python-era setdefault semantics).
    apply_era_env(
        &mut cmd,
        env_vars,
        &[
            ("AWS_LAMBDA_FUNCTION_NAME", function_name.to_string()),
            ("AWS_LAMBDA_FUNCTION_VERSION", function_version.to_string()),
            ("AWS_LAMBDA_FUNCTION_MEMORY_SIZE", memory_size.to_string()),
            ("AWS_LAMBDA_FUNCTION_TIMEOUT", timeout_secs.to_string()),
            ("AWS_REGION", region.to_string()),
            ("AWS_DEFAULT_REGION", region.to_string()),
            ("AWS_ACCOUNT_ID", account_id.to_string()),
            ("_HANDLER", handler.to_string()),
        ],
    );

    let mut child: Child = match cmd.spawn() {
        Ok(c) => c,
        // ETXTBSY (os error 26) is a known transient on concurrent
        // extraction storms: another thread still holds the freshly
        // extracted file open for write when exec(2) lands. One short
        // retry makes a legit twin resilient instead of 904-ing the
        // invoke with a misleading Runtime.InternalError.
        Err(e) if e.raw_os_error() == Some(26) => {
            std::thread::sleep(std::time::Duration::from_millis(50));
            match cmd.spawn() {
                Ok(c) => c,
                Err(e) => {
                    cleanup(&work);
                    return missing_runtime_result(&e.to_string());
                }
            }
        }
        Err(e) => {
            cleanup(&work);
            return missing_runtime_result(&e.to_string());
        }
    };

    // Communicate() discipline (see communicate_event): drains before any
    // write and the event fed from a writer thread, so a chatty bootstrap or
    // one that never reads stdin cannot wedge the invoke thread — the
    // deadline loop below kills the child either way. The custom runtime's
    // result rides stdout, so that channel takes the 6MB result budget and
    // only stderr keeps the 128KB log cap (same shape run_subprocess_family
    // uses, for the same tail-truncation reason).
    let (stdout_rx, stderr_rx) =
        communicate_event(&mut child, &event_str, FAMILY_RESULT_CAP, LOCAL_OUTPUT_CAP);

    // Wait up to the function timeout plus the python-era 2s grace; kill the
    // child on overrun.
    let wait_deadline = std::time::Instant::now()
        + std::time::Duration::from_secs(u64::from(timeout_secs.max(1)) + 2);
    let mut timed_out = false;
    let mut exit_code: Option<i32> = None;
    loop {
        match child.try_wait() {
            Ok(Some(status)) => {
                exit_code = status.code();
                break;
            }
            Ok(None) => {
                if std::time::Instant::now() > wait_deadline {
                    let _ = child.kill();
                    let _ = child.wait();
                    timed_out = true;
                    break;
                }
                std::thread::sleep(std::time::Duration::from_millis(25));
            }
            Err(e) => {
                cleanup(&work);
                return missing_runtime_result(&e.to_string());
            }
        }
    }

    // The child died or was killed — drain threads may never see EOF while a
    // grandchild inherited the pipes, so collecting is bounded.
    let drain_deadline = std::time::Instant::now() + std::time::Duration::from_secs(1);
    let stdout_raw = collect_by(&stdout_rx, drain_deadline);
    let stderr_raw = collect_by(&stderr_rx, drain_deadline);
    cleanup(&work);

    // python-era run_subprocess: stderr alone is the log stream.
    let logs = stderr_raw.clone();
    let stdout = stdout_raw.trim().to_string();

    if timed_out {
        return ExecResult {
            payload: json!({
                "errorMessage": format!("Task timed out after {timeout_secs}.00 seconds"),
                "errorType": "Task.TimedOut",
            })
            .to_string(),
            function_error: Some("Task.TimedOut".to_string()),
            logs: format!("Function timed out after {timeout_secs}s"),
        };
    }

    if exit_code.unwrap_or(-1) != 0 {
        // A structured error object on stdout wins over the exit code.
        if !stdout.is_empty() {
            if let Ok(v) = serde_json::from_str::<Value>(&stdout) {
                if v.as_object()
                    .map(|o| o.contains_key("errorMessage"))
                    .unwrap_or(false)
                {
                    return ExecResult {
                        payload: v.to_string(),
                        function_error: Some("Handled".to_string()),
                        logs,
                    };
                }
            }
        }
        let error_message = if !logs.is_empty() {
            if !stdout.is_empty() {
                stdout.clone()
            } else {
                logs.split('\n').next_back().unwrap_or("").to_string()
            }
        } else {
            "Unknown error".to_string()
        };
        let stack_trace: Vec<String> = if !logs.is_empty() {
            logs.trim().split('\n').map(str::to_string).collect()
        } else {
            Vec::new()
        };
        return ExecResult {
            payload: json!({
                "errorMessage": error_message,
                "errorType": "Runtime.ExitError",
                "stackTrace": stack_trace,
            })
            .to_string(),
            function_error: Some("Unhandled".to_string()),
            logs,
        };
    }

    if stdout.is_empty() {
        return ExecResult {
            payload: "null".to_string(),
            function_error: None,
            logs,
        };
    }
    // Fail-loud at the capture budget (the same mirror run_subprocess_family
    // and the node lane enforce): the drain keeps a bounded TAIL, so a
    // ≥6MB runtime-interface result cannot be trusted as JSON — moto/real
    // AWS answer Function.ResponseSizeTooLarge instead of fragmenting.
    if stdout_raw.len() >= FAMILY_RESULT_CAP - 1 {
        return response_too_large_result(logs);
    }
    let payload = match serde_json::from_str::<Value>(&stdout) {
        Ok(v) => v.to_string(),
        // Not valid JSON — the python-era provider json-dumped the raw string.
        Err(_) => json!(stdout).to_string(),
    };
    ExecResult {
        payload,
        function_error: None,
        logs,
    }
}

/// Family dispatch shared with the docker executor's local fallback: identical
/// branches to the handler's/bridge's invoke paths — provided./go runtimes run
/// the package `bootstrap`, nodejs through the node bootstrap, the python
/// family through the python shim, ruby/java/dotnet through the era family
/// executors, anything else reports an invalid runtime.
#[allow(clippy::too_many_arguments)]
pub fn execute_family(
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
    if is_custom_runtime(runtime) {
        return execute_custom(
            code_zip,
            layer_zips,
            handler,
            event,
            function_name,
            function_version,
            env_vars,
            timeout_secs,
            memory_size,
            region,
            account_id,
        );
    }
    if is_node_runtime(runtime) {
        return execute_nodejs(
            runtime,
            code_zip,
            layer_zips,
            handler,
            event,
            function_name,
            function_version,
            env_vars,
            timeout_secs,
            memory_size,
            region,
            account_id,
        );
    }
    if is_ruby_runtime(runtime) {
        return execute_ruby(
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
    if is_java_runtime(runtime) {
        return execute_java(
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
    if is_dotnet_runtime(runtime) {
        return execute_dotnet(
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
    if is_python_runtime(runtime) {
        return execute_python(
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
    // No executor in this backend for the runtime; mirror the ground-truth
    // shape used by the invoke paths: null payload + Runtime.InvalidRuntime.
    ExecResult {
        payload: "null".to_string(),
        function_error: Some("Runtime.InvalidRuntime".to_string()),
        logs: String::new(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_zip(code: &str) -> Vec<u8> {
        make_zip_named(code, "lambda_function.py")
    }

    /// Zip one source file for the shim/bootstrap exec tests.
    fn make_zip_named(code: &str, filename: &str) -> Vec<u8> {
        // The shim requires python3 anyway, so build the test zip with it.
        let dir = std::env::temp_dir().join(format!("lam-test-{}", uuid::Uuid::new_v4().simple()));
        fs::create_dir_all(&dir).unwrap();
        let zpath = dir.join("t.zip");
        let snippet = "import io, sys, zipfile\nif len(sys.argv) < 4:\n    sys.argv.append('lambda_function.py')\nbuf = io.BytesIO()\nze = zipfile.ZipFile(buf, 'w')\nze.writestr(sys.argv[3], sys.argv[2])\nze.close()\nopen(sys.argv[1], 'wb').write(buf.getvalue())\n";
        let out = std::process::Command::new("python3")
            .arg("-c")
            .arg(snippet)
            .arg(zpath.as_os_str())
            .arg(code)
            .arg(filename)
            .stdin(Stdio::null())
            .output()
            .expect("python3 failed");
        assert!(
            out.status.success(),
            "zip maker failed: {}",
            String::from_utf8_lossy(&out.stderr)
        );
        let z = fs::read(&zpath).unwrap();
        let _ = fs::remove_dir_all(&dir);
        z
    }

    #[test]
    fn test_execute_success() {
        let code = "def handler(e, c):\n    return {'ok': e.get('x')}";
        let zip = make_zip(code);
        let res = execute_python(
            &zip,
            &[],
            "lambda_function.handler",
            r#"{"x": 42}"#,
            "test-fn",
            "$LATEST",
            "arn:aws:lambda:us-east-1:123456789012:function:test-fn",
            &HashMap::new(),
            20,
            128,
            "us-east-1",
            "123456789012",
        );
        assert_eq!(res.function_error, None, "unexpected error: {}", res.logs);
        assert_eq!(
            serde_json::from_str::<Value>(&res.payload).unwrap()["ok"],
            json!(42)
        );
    }

    #[test]
    fn test_grandchild_holding_pipes_does_not_block() {
        // A grandchild that inherits stdout/stderr keeps the pipes open past
        // the handler process's exit; result delivery must stay bounded.
        let code = "import subprocess, sys\ndef handler(e, c):\n    subprocess.Popen([sys.executable, '-c', 'import time; time.sleep(30)'])\n    return 'spawned'\n";
        let zip = make_zip(code);
        let started = std::time::Instant::now();
        let res = execute_python(
            &zip,
            &[],
            "lambda_function.handler",
            r#"{}"#,
            "test-fn",
            "$LATEST",
            "arn:aws:lambda:us-east-1:123456789012:function:test-fn",
            &HashMap::new(),
            1,
            128,
            "us-east-1",
            "123456789012",
        );
        // The shim's own timeout budget is timeout_secs + 5; previously this
        // scenario blocked until the grandchild (30s sleep) exited.
        assert!(
            started.elapsed() < std::time::Duration::from_secs(10),
            "invocation blocked on grandchild-inherited pipes for {:?}",
            started.elapsed()
        );
        assert_eq!(res.function_error, None, "logs: {}", res.logs);
        assert_eq!(res.payload, json!("spawned").to_string());
    }

    #[test]
    fn test_execute_error() {
        let code = "def handler(e, c):\n    raise ValueError('boom')";
        let zip = make_zip(code);
        let res = execute_python(
            &zip,
            &[],
            "lambda_function.handler",
            r#"{}"#,
            "test-fn",
            "$LATEST",
            "arn:aws:lambda:us-east-1:123456789012:function:test-fn",
            &HashMap::new(),
            20,
            128,
            "us-east-1",
            "123456789012",
        );
        assert_eq!(res.function_error.as_deref(), Some("Handled"));
        let payload: Value = serde_json::from_str(&res.payload).unwrap();
        assert_eq!(payload["errorType"], "ValueError");
        assert_eq!(payload["errorMessage"], "boom");
    }

    /// Zip an existing directory tree (one entry per file, paths relative
    /// to the root) with python's zipfile, like a real layer package.
    fn make_zip_dir(root: &std::path::Path, out: &std::path::Path) -> Vec<u8> {
        let snippet = "import os, sys, zipfile\nroot, out = sys.argv[1], sys.argv[2]\nwith zipfile.ZipFile(out, 'w') as zf:\n    for dirpath, _, files in os.walk(root):\n        for f in files:\n            p = os.path.join(dirpath, f)\n            zf.write(p, os.path.relpath(p, root))\n";
        let result = std::process::Command::new("python3")
            .arg("-c")
            .arg(snippet)
            .arg(root.as_os_str())
            .arg(out.as_os_str())
            .stdin(Stdio::null())
            .output()
            .expect("python3 failed");
        assert!(
            result.status.success(),
            "zip dir maker failed: {}",
            String::from_utf8_lossy(&result.stderr)
        );
        fs::read(out).unwrap()
    }

    #[test]
    fn test_layers_extracted_and_importable() {
        // A layer zip with python/laymod.py; the handler imports it. Before
        // the shim extracted layers this raised ModuleNotFoundError.
        let laydir =
            std::env::temp_dir().join(format!("lam-lay-{}", uuid::Uuid::new_v4().simple()));
        fs::create_dir_all(laydir.join("python")).unwrap();
        fs::write(
            laydir.join("python").join("laymod.py"),
            "def val():\n    return 'from-layer'\n",
        )
        .unwrap();
        let zpath = laydir.parent().unwrap().join("lay.zip");
        let layer_zip = make_zip_dir(&laydir, &zpath);

        let code = "import laymod\ndef handler(e, c):\n    return {'val': laymod.val()}\n";
        let zip = make_zip(code);
        let res = execute_python(
            &zip,
            &[layer_zip],
            "lambda_function.handler",
            r#"{}"#,
            "test-fn",
            "$LATEST",
            "arn:aws:lambda:us-east-1:123456789012:function:test-fn",
            &HashMap::new(),
            20,
            128,
            "us-east-1",
            "123456789012",
        );
        assert_eq!(res.function_error, None, "logs: {}", res.logs);
        let payload: Value = serde_json::from_str(&res.payload).unwrap();
        assert_eq!(payload["val"], json!("from-layer"));
    }

    #[test]
    fn test_output_capture_is_capped_tail() {
        // ~600KB of stdout must not buffer unboundedly; the tail is kept.
        let code = "def handler(e, c):\n    for i in range(6000):\n        print('x' * 100, i)\n    return 'done'\n";
        let zip = make_zip(code);
        let res = execute_python(
            &zip,
            &[],
            "lambda_function.handler",
            r#"{}"#,
            "test-fn",
            "$LATEST",
            "arn:aws:lambda:us-east-1:123456789012:function:test-fn",
            &HashMap::new(),
            20,
            128,
            "us-east-1",
            "123456789012",
        );
        assert_eq!(
            res.function_error, None,
            "exec logs: {} | payload: {}",
            res.logs, res.payload
        );
        assert!(
            res.logs.len() <= LOCAL_OUTPUT_CAP + 8,
            "captured {} bytes of output",
            res.logs.len()
        );
        assert!(
            res.logs.contains("5999"),
            "tail semantics: end of stream must be kept, got ending {:?}",
            &res.logs[res.logs.len().saturating_sub(64)..]
        );
    }

    #[test]
    fn test_execute_nodejs_success() {
        let code = r#"exports.handler = async (event) => ({ ok: event.spec["x"] });"#;
        let zip = make_zip_named(code, "index.js");
        let res = execute_nodejs(
            "nodejs20.x",
            &zip,
            &[],
            "index.handler",
            r#"{"spec": {"x": 7}}"#,
            "node-fn",
            "$LATEST",
            &HashMap::new(),
            20,
            128,
            "us-east-1",
            "123456789012",
        );
        assert_eq!(res.function_error, None, "logs: {}", res.logs);
        assert_eq!(
            serde_json::from_str::<Value>(&res.payload).unwrap()["ok"],
            json!(7)
        );
    }

    #[test]
    fn test_execute_nodejs_error_and_context() {
        let code = r#"
exports.handler = async (event, context) => {
    return {
        name: context.functionName,
        mem: context.memoryLimitInMB,
        remaining: context.getRemainingTimeInMillis() > 0,
    };
};"#;
        let zip = make_zip_named(code, "index.js");
        let res = execute_nodejs(
            "nodejs20.x",
            &zip,
            &[],
            "index.handler",
            r#"{}"#,
            "node-fn",
            "$LATEST",
            &HashMap::new(),
            20,
            256,
            "us-east-1",
            "123456789012",
        );
        assert_eq!(res.function_error, None, "logs: {}", res.logs);
        let payload: Value = serde_json::from_str(&res.payload).unwrap();
        assert_eq!(payload["name"], json!("node-fn"));
        assert_eq!(payload["mem"], json!("256"));
        assert_eq!(payload["remaining"], json!(true));

        let throw_zip = make_zip_named(
            "exports.handler = async () => { throw new Error('boom'); };",
            "index.js",
        );
        let res = execute_nodejs(
            "nodejs20.x",
            &throw_zip,
            &[],
            "index.handler",
            r#"{}"#,
            "node-fn",
            "$LATEST",
            &HashMap::new(),
            20,
            128,
            "us-east-1",
            "123456789012",
        );
        assert_eq!(res.function_error.as_deref(), Some("Handled"));
        let payload: Value = serde_json::from_str(&res.payload).unwrap();
        assert_eq!(payload["errorType"], "Error");
        assert_eq!(payload["errorMessage"], "boom");
    }

    #[test]
    fn test_execute_nodejs_handler_not_found() {
        let zip = make_zip_named("exports.handler = async () => 'ok';", "index.js");
        let res = execute_nodejs(
            "nodejs20.x",
            &zip,
            &[],
            "index.wrong_name",
            r#"{}"#,
            "node-fn",
            "$LATEST",
            &HashMap::new(),
            20,
            128,
            "us-east-1",
            "123456789012",
        );
        assert_eq!(res.function_error.as_deref(), Some("Handled"));
        let payload: Value = serde_json::from_str(&res.payload).unwrap();
        assert!(payload["errorMessage"]
            .as_str()
            .unwrap_or_default()
            .contains("wrong_name"));
    }

    #[test]
    fn test_env_vars() {
        let code = "import os\ndef handler(e, c):\n    return os.environ.get('MY_VAR')";
        let zip = make_zip(code);
        let mut env = HashMap::new();
        env.insert("MY_VAR".to_string(), "hello123".to_string());
        let res = execute_python(
            &zip,
            &[],
            "lambda_function.handler",
            r#"{}"#,
            "test-fn",
            "$LATEST",
            "arn:aws:lambda:us-east-1:123456789012:function:test-fn",
            &env,
            20,
            128,
            "us-east-1",
            "123456789012",
        );
        assert_eq!(res.function_error, None);
        assert_eq!(res.payload, json!("hello123").to_string());
    }
    fn make_bootstrap_zip_bytes(script: &str, mode_bits: u32) -> Vec<u8> {
        let dir = std::env::temp_dir().join(format!("lam-crt-{}", uuid::Uuid::new_v4().simple()));
        fs::create_dir_all(&dir).unwrap();
        let zpath = dir.join("t.zip");
        let snippet = "import io, sys, zipfile\nbuf = io.BytesIO()\ninfo = zipfile.ZipInfo('bootstrap')\ninfo.external_attr = int(sys.argv[3], 8) << 16\nzipfile.ZipFile(buf, 'w').writestr(info, sys.argv[2])\nbuf.seek(0)\nopen(sys.argv[1], 'wb').write(buf.getvalue())\n";
        let out = std::process::Command::new("python3")
            .arg("-c")
            .arg(snippet)
            .arg(zpath.as_os_str())
            .arg(script)
            .arg(format!("{mode_bits:o}"))
            .stdin(Stdio::null())
            .output()
            .expect("python3 failed");
        assert!(
            out.status.success(),
            "zip maker failed: {}",
            String::from_utf8_lossy(&out.stderr)
        );
        let z = fs::read(&zpath).unwrap();
        let _ = fs::remove_dir_all(&dir);
        z
    }

    const CRT_ECHO: &str = "#!/bin/bash\nread event\necho \"$event\"\n";

    #[cfg(unix)]
    fn exec_custom(zip: &[u8], handler: &str, event: &str, timeout: u32) -> ExecResult {
        execute_custom(
            zip,
            &[],
            handler,
            event,
            "test-fn-crt",
            "$LATEST",
            &HashMap::new(),
            timeout,
            128,
            "us-east-1",
            "123456789012",
        )
    }

    #[test]
    fn test_is_custom_runtime() {
        assert!(is_custom_runtime("provided"));
        assert!(is_custom_runtime("provided.al2"));
        assert!(is_custom_runtime("provided.al2023"));
        assert!(is_custom_runtime(" provided.al2023 "));
        assert!(is_custom_runtime("go1.x"));
        assert!(!is_custom_runtime("python3.12"));
        assert!(!is_custom_runtime("nodejs20.x"));
        assert!(!is_custom_runtime(""));
    }

    #[cfg(unix)]
    #[test]
    fn test_custom_bootstrap_echo() {
        let zip = make_bootstrap_zip_bytes(CRT_ECHO, 0o755);
        let res = exec_custom(&zip, "bootstrap", r#"{"message":"hello"}"#, 10);
        assert_eq!(
            res.function_error, None,
            "logs: {} payload: {}",
            res.logs, res.payload
        );
        assert_eq!(res.payload, json!({"message": "hello"}).to_string());
    }

    #[cfg(unix)]
    #[test]
    fn test_custom_bootstrap_non_executable_runs_after_chmod() {
        // 0o644 external bits exactly like the compat suite's broken zip.
        let zip = make_bootstrap_zip_bytes(CRT_ECHO, 0o644);
        let res = exec_custom(&zip, "bootstrap", r#"{"fixed": true}"#, 10);
        assert_eq!(
            res.function_error, None,
            "logs: {} payload: {}",
            res.logs, res.payload
        );
        assert_eq!(res.payload, json!({"fixed": true}).to_string());
    }

    #[cfg(unix)]
    #[test]
    fn test_custom_bootstrap_missing_reports_invalid_entrypoint() {
        // The python-helper zips have no bootstrap file.
        let zip = make_zip("def handler(e, c):\n    return {}\n");
        let res = exec_custom(&zip, "bootstrap", r#"{}"#, 3);
        assert_eq!(
            res.function_error.as_deref(),
            Some("Runtime.InvalidEntrypoint")
        );
        assert_eq!(res.payload, "null");
    }

    #[cfg(unix)]
    #[test]
    fn test_custom_bootstrap_error_exit() {
        let script = "#!/bin/bash\necho \"something went wrong\" >&2\nexit 1\n";
        let zip = make_bootstrap_zip_bytes(script, 0o755);
        let res = exec_custom(&zip, "bootstrap", r#"{}"#, 10);
        assert_eq!(res.function_error.as_deref(), Some("Unhandled"));
        let payload: Value = serde_json::from_str(&res.payload).unwrap();
        assert_eq!(payload["errorType"], "Runtime.ExitError");
        assert!(payload["stackTrace"]
            .as_array()
            .unwrap()
            .iter()
            .any(|l| l.as_str().unwrap_or("").contains("something went wrong")));
    }

    #[cfg(unix)]
    #[test]
    fn test_custom_bootstrap_structured_stdout_error_is_handled() {
        let script = "#!/bin/bash\necho '{\"errorMessage\": \"failed\", \"errorType\": \"Runtime.Foo\"}'\nexit 2\n";
        let zip = make_bootstrap_zip_bytes(script, 0o755);
        let res = exec_custom(&zip, "bootstrap", r#"{}"#, 10);
        assert_eq!(res.function_error.as_deref(), Some("Handled"));
        let payload: Value = serde_json::from_str(&res.payload).unwrap();
        assert_eq!(payload["errorMessage"], "failed");
    }

    #[cfg(unix)]
    #[test]
    fn test_custom_bootstrap_timeout() {
        let script = "#!/bin/bash\nsleep 30\necho \"{}\"\n";
        let zip = make_bootstrap_zip_bytes(script, 0o755);
        let res = exec_custom(&zip, "bootstrap", r#"{}"#, 1);
        assert_eq!(res.function_error.as_deref(), Some("Task.TimedOut"));
        let payload: Value = serde_json::from_str(&res.payload).unwrap();
        assert_eq!(payload["errorType"], "Task.TimedOut");
        assert_eq!(payload["errorMessage"], "Task timed out after 1.00 seconds");
        assert_eq!(res.logs, "Function timed out after 1s");
    }

    #[cfg(unix)]
    #[test]
    fn test_custom_bootstrap_stderr_logs_are_captured() {
        let script = "#!/bin/bash\nread event\necho \"LOG: processing event\" >&2\necho '{\"status\": \"ok\"}'\n";
        let zip = make_bootstrap_zip_bytes(script, 0o755);
        let res = exec_custom(&zip, "bootstrap", r#"{}"#, 10);
        assert_eq!(
            res.function_error, None,
            "logs: {} payload: {}",
            res.logs, res.payload
        );
        assert!(
            res.logs.contains("LOG: processing event"),
            "logs: {}",
            res.logs
        );
    }

    #[cfg(unix)]
    #[test]
    fn test_custom_bootstrap_env_vars() {
        let script = "#!/bin/bash\nread event\nfn_name=\"$AWS_LAMBDA_FUNCTION_NAME\"\necho \"{\\\"fn\\\": \\\"$fn_name\\\", \\\"my_var\\\": \\\"$MY_VAR\\\", \\\"handler\\\": \\\"$_HANDLER\\\"}\"\n";
        let zip = make_bootstrap_zip_bytes(script, 0o755);
        let mut env = HashMap::new();
        env.insert("MY_VAR".to_string(), "zeta-9".to_string());
        let res = execute_custom(
            &zip,
            &[],
            "bootstrap.handler",
            r#"{}"#,
            "test-fn-crt",
            "$LATEST",
            &env,
            10,
            128,
            "us-east-1",
            "123456789012",
        );
        assert_eq!(
            res.function_error, None,
            "logs: {} payload: {}",
            res.logs, res.payload
        );
        let payload: Value = serde_json::from_str(&res.payload).unwrap();
        assert_eq!(payload["fn"], json!("test-fn-crt"));
        assert_eq!(payload["my_var"], json!("zeta-9"));
        assert_eq!(payload["handler"], json!("bootstrap.handler"));
    }

    // === managed families (ruby/java/dotnet): era unit-test ports ===

    fn has_bin(bin: &str) -> bool {
        which_bin(bin).is_some()
    }

    /// Zip arbitrary (text) files — the era `make_zip` helper shape.
    fn make_files_zip(files: &[(&str, &str)]) -> Vec<u8> {
        let dir = std::env::temp_dir().join(format!("lam-files-{}", uuid::Uuid::new_v4().simple()));
        fs::create_dir_all(&dir).unwrap();
        for (name, content) in files {
            let path = dir.join(name);
            if let Some(parent) = path.parent() {
                fs::create_dir_all(parent).unwrap();
            }
            fs::write(path, content).unwrap();
        }
        let out = dir.parent().unwrap().join(format!(
            "{}.zip",
            dir.file_name().unwrap().to_string_lossy()
        ));

        let zip = make_zip_dir(&dir, &out);
        let _ = fs::remove_dir_all(&dir);
        let _ = fs::remove_file(&out);
        zip
    }

    /// Compile java sources (stage + javac) and zip the staged tree, era
    /// `_compile_java_to_zip`.
    #[cfg(unix)]
    fn make_java_zip(sources: &[(&str, &str)]) -> Vec<u8> {
        assert!(has_bin("javac"), "javac required for java exec tests");
        let dir = std::env::temp_dir().join(format!("lam-java-{}", uuid::Uuid::new_v4().simple()));
        fs::create_dir_all(&dir).unwrap();
        let mut java_files: Vec<PathBuf> = Vec::new();
        for (name, source) in sources {
            let path = dir.join(name);
            if let Some(parent) = path.parent() {
                fs::create_dir_all(parent).unwrap();
            }
            fs::write(&path, source).unwrap();
            java_files.push(path);
        }
        let output = std::process::Command::new("javac")
            .arg("-d")
            .arg(&dir)
            .args(&java_files)
            .stdin(Stdio::null())
            .output()
            .expect("javac spawn failed");
        assert!(
            output.status.success(),
            "javac failed: {}",
            String::from_utf8_lossy(&output.stderr)
        );
        let out = dir.parent().unwrap().join(format!(
            "{}.zip",
            dir.file_name().unwrap().to_string_lossy()
        ));
        let zip = make_zip_dir(&dir, &out);
        let _ = fs::remove_dir_all(&dir);
        let _ = fs::remove_file(&out);
        zip
    }

    /// Build a C# class library (stage + dotnet build, host TFM) and zip the
    /// build output, mirroring the compat suite's `_compile_cs_to_zip`.
    #[cfg(unix)]
    fn make_dotnet_dll_zip(cs_source: &str) -> Vec<u8> {
        assert!(has_bin("dotnet"), "dotnet required for dotnet exec tests");
        let tfm = detect_dotnet_tfm();
        let dir =
            std::env::temp_dir().join(format!("lam-dotnet-{}", uuid::Uuid::new_v4().simple()));
        fs::create_dir_all(&dir).unwrap();
        fs::write(dir.join("Handler.cs"), cs_source).unwrap();
        fs::write(
            dir.join("MyLambda.csproj"),
            format!(
                "<Project Sdk=\"Microsoft.NET.Sdk\">\n  <PropertyGroup>\n    <TargetFramework>{tfm}</TargetFramework>\n    <ImplicitUsings>enable</ImplicitUsings>\n    <AssemblyName>MyLambda</AssemblyName>\n  </PropertyGroup>\n</Project>\n"
            ),
        )
        .unwrap();
        let out_dir = dir.join("out");
        let mut cmd = std::process::Command::new("dotnet");
        cmd.arg("build")
            .arg(dir.join("MyLambda.csproj"))
            .arg("-c")
            .arg("Release")
            .arg("-o")
            .arg(&out_dir)
            .arg("--nologo")
            .arg("-v")
            .arg("quiet");
        apply_dotnet_compile_env(&mut cmd);
        let output = cmd.output().expect("dotnet build failed");
        assert!(
            output.status.success() && out_dir.join("MyLambda.dll").is_file(),
            "dotnet build failed: {}",
            String::from_utf8_lossy(&output.stderr)
        );
        let zip = make_zip_dir(&out_dir, &dir.join("out.zip"));
        let _ = fs::remove_dir_all(&dir);
        zip
    }

    fn exec_ruby(zip: &[u8], handler: &str, event: &str, timeout: u32) -> ExecResult {
        execute_ruby(
            "ruby3.2",
            zip,
            &[],
            handler,
            event,
            "ruby-fn",
            "$LATEST",
            "arn:aws:lambda:us-east-1:123456789012:function:ruby-fn",
            &HashMap::new(),
            timeout,
            128,
            "us-east-1",
            "123456789012",
        )
    }

    #[test]
    fn test_is_ruby_java_dotnet_runtime() {
        assert!(is_ruby_runtime("ruby3.2"));
        assert!(is_ruby_runtime("ruby3.3"));
        assert!(is_ruby_runtime(" ruby3.2 "));
        assert!(!is_ruby_runtime("python3.12"));
        assert!(!is_ruby_runtime("nodejs20.x"));
        assert!(is_java_runtime("java8"));
        assert!(is_java_runtime("java8.al2"));
        assert!(is_java_runtime("java21"));
        assert!(!is_java_runtime("nodejs22.x"));
        assert!(!is_java_runtime(""));
        assert!(is_dotnet_runtime("dotnet6"));
        assert!(is_dotnet_runtime("dotnet8"));
        assert!(!is_dotnet_runtime("dot"));
    }

    #[test]
    fn test_ruby_binary_resolution_prefers_versioned_then_default() {
        // Era TestRubyVersionRouting: the versioned binary wins when present,
        // the default `ruby` is the fallback (fault-in installs are out of
        // scope for the offline twin). On this host both live at /usr/bin.
        if which_bin("ruby3.2").is_some() {
            assert_eq!(
                find_ruby("ruby3.2").as_deref(),
                which_bin("ruby3.2").as_deref()
            );
        } else {
            assert_eq!(
                find_ruby("ruby3.2").as_deref(),
                which_bin("ruby").as_deref()
            );
        }
        // Unknown identifiers go straight to the default binary.
        assert_eq!(
            find_ruby("ruby2.7").as_deref(),
            which_bin("ruby").as_deref()
        );
    }

    #[test]
    fn test_execute_ruby_success() {
        // Era test_simple_return + test_returns_string + test_returns_array.
        if !has_bin("ruby") {
            return;
        }
        let hash = exec_ruby(
            &make_files_zip(&[(
                "lambda_function.rb",
                "def handler(event:, context:)\n  { result: 'ok' }\nend\n",
            )]),
            "lambda_function.handler",
            r#"{"key": "value"}"#,
            10,
        );
        assert_eq!(hash.function_error, None, "logs: {}", hash.logs);
        assert_eq!(
            serde_json::from_str::<Value>(&hash.payload).unwrap()["result"],
            json!("ok")
        );

        let array = exec_ruby(
            &make_files_zip(&[(
                "lambda_function.rb",
                "def handler(event:, context:)\n  [1, 2, 3]\nend\n",
            )]),
            "lambda_function.handler",
            r#"{}"#,
            10,
        );
        assert_eq!(array.function_error, None, "logs: {}", array.logs);
        assert_eq!(array.payload, json!([1, 2, 3]).to_string());

        let string = exec_ruby(
            &make_files_zip(&[(
                "lambda_function.rb",
                "def handler(event:, context:)\n  'hello from ruby'\nend\n",
            )]),
            "lambda_function.handler",
            r#"{}"#,
            10,
        );
        assert_eq!(string.function_error, None, "logs: {}", string.logs);
        assert_eq!(string.payload, json!("hello from ruby").to_string());
    }

    #[test]
    fn test_execute_ruby_event_echo_and_context() {
        // Era test_returns_event + test_handler_with_context.
        if !has_bin("ruby") {
            return;
        }
        let echo = exec_ruby(
            &make_files_zip(&[(
                "lambda_function.rb",
                "def handler(event:, context:)\n  event\nend\n",
            )]),
            "lambda_function.handler",
            r#"{"hello": "ruby"}"#,
            10,
        );
        assert_eq!(echo.function_error, None, "logs: {}", echo.logs);
        assert_eq!(echo.payload, json!({"hello": "ruby"}).to_string());

        let ctx = exec_ruby(
            &make_files_zip(&[(
                "lambda_function.rb",
                "def handler(event:, context:)\n  {\n    fn_name: context.function_name,\n    mem: context.memory_limit_in_mb,\n    request_id: context.aws_request_id,\n    has_remaining: context.get_remaining_time_in_millis > 0\n  }\nend\n",
            )]),
            "lambda_function.handler",
            r#"{}"#,
            10,
        );
        assert_eq!(ctx.function_error, None, "logs: {}", ctx.logs);
        let payload: Value = serde_json::from_str(&ctx.payload).unwrap();
        assert_eq!(payload["fn_name"], json!("ruby-fn"));
        assert_eq!(payload["mem"], json!(128));
        assert_eq!(payload["has_remaining"], json!(true));
        assert_eq!(payload["request_id"].as_str().unwrap().len(), 36);
    }

    #[test]
    fn test_execute_ruby_error_and_not_found() {
        // Era test_handler_raises + test_missing_module + test_missing_method.
        if !has_bin("ruby") {
            return;
        }
        let raised = exec_ruby(
            &make_files_zip(&[(
                "lambda_function.rb",
                "def handler(event:, context:)\n  raise 'boom'\nend\n",
            )]),
            "lambda_function.handler",
            r#"{}"#,
            10,
        );
        assert_eq!(raised.function_error.as_deref(), Some("Handled"));
        let payload: Value = serde_json::from_str(&raised.payload).unwrap();
        assert_eq!(payload["errorType"], "RuntimeError");
        assert!(
            payload["errorMessage"]
                .as_str()
                .unwrap_or_default()
                .contains("boom"),
            "payload: {payload}"
        );

        let missing_module = exec_ruby(
            &make_files_zip(&[("other.rb", "def handler(event:, context:)\n  'ok'\nend\n")]),
            "lambda_function.handler",
            r#"{}"#,
            10,
        );
        assert_eq!(missing_module.function_error.as_deref(), Some("Handled"));
        let payload: Value = serde_json::from_str(&missing_module.payload).unwrap();
        assert_eq!(payload["errorType"], "Runtime.ImportModuleError");
        assert!(
            payload["errorMessage"]
                .as_str()
                .unwrap_or_default()
                .contains("Cannot find module"),
            "payload: {payload}"
        );

        let missing_method = exec_ruby(
            &make_files_zip(&[(
                "lambda_function.rb",
                "def other(event:, context:)\n  'ok'\nend\n",
            )]),
            "lambda_function.handler",
            r#"{}"#,
            10,
        );
        assert_eq!(missing_method.function_error.as_deref(), Some("Handled"));
        let payload: Value = serde_json::from_str(&missing_method.payload).unwrap();
        assert!(
            payload["errorMessage"]
                .as_str()
                .unwrap_or_default()
                .contains("HandlerNotFound")
                || payload["errorType"] == "Runtime.HandlerNotFound",
            "payload: {payload}"
        );
    }

    #[test]
    fn test_execute_ruby_env_vars() {
        // Era test_env_vars.
        if !has_bin("ruby") {
            return;
        }
        let zip = make_files_zip(&[(
            "lambda_function.rb",
            "def handler(event:, context:)\n  {\n    custom: ENV['MY_VAR'],\n    fn: ENV['AWS_LAMBDA_FUNCTION_NAME']\n  }\nend\n",
        )]);
        let mut env = HashMap::new();
        env.insert("MY_VAR".to_string(), "ruby-custom".to_string());
        let res = execute_ruby(
            "ruby3.2",
            &zip,
            &[],
            "lambda_function.handler",
            r#"{}"#,
            "env-fn",
            "$LATEST",
            "arn:aws:lambda:us-east-1:123456789012:function:env-fn",
            &env,
            10,
            128,
            "us-east-1",
            "123456789012",
        );
        assert_eq!(res.function_error, None, "logs: {}", res.logs);
        let payload: Value = serde_json::from_str(&res.payload).unwrap();
        assert_eq!(payload["custom"], json!("ruby-custom"));
        assert_eq!(payload["fn"], json!("env-fn"));
    }

    #[test]
    fn test_execute_ruby_layer_ruby_subtree_on_load_path() {
        // Era `RUBYOPT` layer load path: a layer zip carrying ruby/helper.rb
        // must be require-able from the handler.
        if !has_bin("ruby") {
            return;
        }
        let laydir =
            std::env::temp_dir().join(format!("lam-ruby-lay-{}", uuid::Uuid::new_v4().simple()));
        fs::create_dir_all(laydir.join("ruby")).unwrap();
        fs::write(
            laydir.join("ruby").join("laymod.rb"),
            "def layer_val\n  'from-layer'\nend\n",
        )
        .unwrap();
        let zpath = laydir.parent().unwrap().join("lay.zip");
        let layer_zip = make_zip_dir(&laydir, &zpath);

        let code = "require 'laymod'\ndef handler(event:, context:)\n  { val: layer_val }\nend\n";
        let zip = make_files_zip(&[("lambda_function.rb", code)]);
        let res = execute_ruby(
            "ruby3.2",
            &zip,
            &[layer_zip],
            "lambda_function.handler",
            r#"{}"#,
            "layered-fn",
            "$LATEST",
            "arn:aws:lambda:us-east-1:123456789012:function:layered-fn",
            &HashMap::new(),
            10,
            128,
            "us-east-1",
            "123456789012",
        );
        assert_eq!(
            res.function_error, None,
            "logs: {} payload: {}",
            res.logs, res.payload
        );
        let payload: Value = serde_json::from_str(&res.payload).unwrap();
        assert_eq!(payload["val"], json!("from-layer"));
    }

    #[test]
    fn test_execute_ruby_timeout() {
        // Compat test_timeout_exceeded semantics: a sleeping handler answers
        // Task.TimedOut with the era message text.
        if !has_bin("ruby") {
            return;
        }
        let zip = make_files_zip(&[(
            "lambda_function.rb",
            "def handler(event:, context:)\n  sleep 10\n  { ok: true }\nend\n",
        )]);
        let res = exec_ruby(&zip, "lambda_function.handler", r#"{}"#, 1);
        assert_eq!(res.function_error.as_deref(), Some("Task.TimedOut"));
        let payload: Value = serde_json::from_str(&res.payload).unwrap();
        assert_eq!(payload["errorType"], "Task.TimedOut");
        assert_eq!(payload["errorMessage"], "Task timed out after 1.00 seconds");
        assert_eq!(res.logs, "Function timed out after 1s");
    }

    fn exec_java(zip: &[u8], handler: &str, event: &str, timeout: u32) -> ExecResult {
        execute_java(
            "java21",
            zip,
            &[],
            handler,
            event,
            "java-fn",
            "$LATEST",
            "arn:aws:lambda:us-east-1:123456789012:function:java-fn",
            &HashMap::new(),
            timeout,
            256,
            "us-east-1",
            "123456789012",
        )
    }

    #[cfg(unix)]
    #[test]
    fn test_execute_java_echo_error_env() {
        // Era test_simple_echo + test_handler_throws + test_missing_class +
        // test_env_vars.
        if !has_bin("javac") || !has_bin("java") {
            return;
        }
        let echo_zip = make_java_zip(&[(
            "Handler.java",
            "public class Handler {\n    public String handleRequest(String event, Object context) {\n        return event;\n    }\n}\n",
        )]);
        let echo = exec_java(
            &echo_zip,
            "Handler::handleRequest",
            r#"{"hello": "java"}"#,
            30,
        );
        assert_eq!(echo.function_error, None, "logs: {}", echo.logs);
        let payload: Value = serde_json::from_str(&echo.payload).unwrap();
        assert_eq!(payload["hello"], json!("java"));

        let err_zip = make_java_zip(&[(
            "ErrorHandler.java",
            "public class ErrorHandler {\n    public String handleRequest(String event, Object context) {\n        throw new RuntimeException(\"java boom\");\n    }\n}\n",
        )]);
        let raised = exec_java(&err_zip, "ErrorHandler::handleRequest", r#"{}"#, 30);
        assert_eq!(raised.function_error.as_deref(), Some("Handled"));
        let payload: Value = serde_json::from_str(&raised.payload).unwrap();
        assert_eq!(payload["errorType"], "RuntimeException");
        assert!(
            payload["errorMessage"]
                .as_str()
                .unwrap_or_default()
                .contains("java boom"),
            "payload: {payload}"
        );

        let dummy_zip = make_files_zip(&[("dummy.txt", "nothing")]);
        let missing = exec_java(&dummy_zip, "NonExistent::handleRequest", r#"{}"#, 30);
        assert_eq!(missing.function_error.as_deref(), Some("Handled"));
        let payload: Value = serde_json::from_str(&missing.payload).unwrap();
        assert!(
            payload["errorMessage"]
                .as_str()
                .unwrap_or_default()
                .contains("Cannot find class"),
            "payload: {payload}"
        );

        let env_zip = make_java_zip(&[(
            "EnvHandler.java",
            "public class EnvHandler {\n    public String handleRequest(String event, Object context) {\n        String custom = System.getenv(\"MY_VAR\");\n        String fn = System.getenv(\"AWS_LAMBDA_FUNCTION_NAME\");\n        return \"{\\\"custom\\\": \\\"\" + custom + \"\\\", \\\"fn\\\": \\\"\" + fn + \"\\\"}\";\n    }\n}\n",
        )]);
        let mut env = HashMap::new();
        env.insert("MY_VAR".to_string(), "java-custom".to_string());
        let with_env = execute_java(
            "java21",
            &env_zip,
            &[],
            "EnvHandler::handleRequest",
            r#"{}"#,
            "env-fn",
            "$LATEST",
            "arn:aws:lambda:us-east-1:123456789012:function:env-fn",
            &env,
            30,
            256,
            "us-east-1",
            "123456789012",
        );
        assert_eq!(with_env.function_error, None, "logs: {}", with_env.logs);
        let payload: Value = serde_json::from_str(&with_env.payload).unwrap();
        assert_eq!(payload["custom"], json!("java-custom"));
        assert_eq!(payload["fn"], json!("env-fn"));
    }

    #[cfg(unix)]
    #[test]
    fn test_execute_java_map_and_default_and_stream_handlers() {
        // Compat semantics that live in the bridge: Map-parameter handlers
        // get the era `_raw` wrapper; a handler spec without :: defaults to
        // handleRequest; the (InputStream, OutputStream, context) stream
        // branch writes the result to the stream.
        if !has_bin("javac") || !has_bin("java") {
            return;
        }
        let map_zip = make_java_zip(&[(
            "MapHandler.java",
            "import java.util.Map;\npublic class MapHandler {\n    public String handleRequest(Map<String, Object> event, Object context) {\n        return \"{\\\"result\\\": \\\"map-handler-ok\\\"}\";\n    }\n}\n",
        )]);
        let map = exec_java(&map_zip, "MapHandler::handleRequest", r#"{"x": 1}"#, 30);
        assert_eq!(map.function_error, None, "logs: {}", map.logs);
        let payload: Value = serde_json::from_str(&map.payload).unwrap();
        assert_eq!(payload["result"], json!("map-handler-ok"));

        let default_zip = make_java_zip(&[(
            "Handler.java",
            "public class Handler {\n    public String handleRequest(String event, Object context) {\n        return \"default-method\";\n    }\n}\n",
        )]);
        let default_method = exec_java(&default_zip, "Handler", r#"{}"#, 30);
        assert_eq!(
            default_method.function_error, None,
            "logs: {}",
            default_method.logs
        );
        assert_eq!(default_method.payload, json!("default-method").to_string());

        let stream_zip = make_java_zip(&[(
            "StreamHandler.java",
            "import java.io.*;\npublic class StreamHandler {\n    public void handleRequest(InputStream input, OutputStream output, Object context) throws IOException {\n        output.write(\"{\\\"stream\\\": true}\".getBytes());\n    }\n}\n",
        )]);
        let stream = exec_java(&stream_zip, "StreamHandler::handleRequest", r#"{}"#, 30);
        assert_eq!(stream.function_error, None, "logs: {}", stream.logs);
        let payload: Value = serde_json::from_str(&stream.payload).unwrap();
        assert_eq!(payload["stream"], json!(true));
    }

    fn exec_dotnet(zip: &[u8], handler: &str, event: &str, timeout: u32) -> ExecResult {
        execute_dotnet(
            "dotnet8",
            zip,
            &[],
            handler,
            event,
            "dotnet-fn",
            "$LATEST",
            "arn:aws:lambda:us-east-1:123456789012:function:dotnet-fn",
            &HashMap::new(),
            timeout,
            256,
            "us-east-1",
            "123456789012",
        )
    }

    #[test]
    fn test_execute_dotnet_bad_handler_format_and_guard() {
        // Era test_bad_handler_format + the docker-mode assembly guard: no
        // interpreter runs for a malformed handler spec or a traversal-ish
        // assembly name.
        let bad_format = exec_dotnet(
            &make_files_zip(&[("dummy.txt", "")]),
            "bad-format",
            r#"{}"#,
            30,
        );
        assert_eq!(
            bad_format.function_error.as_deref(),
            Some("Runtime.HandlerNotFound")
        );
        let payload: Value = serde_json::from_str(&bad_format.payload).unwrap();
        assert_eq!(payload["errorType"], "Runtime.HandlerNotFound");
        assert!(
            payload["errorMessage"]
                .as_str()
                .unwrap_or_default()
                .contains("Assembly::Type::Method"),
            "payload: {payload}"
        );

        let guard = exec_dotnet(
            &make_files_zip(&[("dummy.txt", "")]),
            "../evil::A::B",
            r#"{}"#,
            30,
        );
        assert_eq!(
            guard.function_error.as_deref(),
            Some("Runtime.ImportModuleError")
        );
        let payload: Value = serde_json::from_str(&guard.payload).unwrap();
        assert_eq!(payload["errorType"], "Runtime.ImportModuleError");
        assert!(
            payload["errorMessage"]
                .as_str()
                .unwrap_or_default()
                .contains("../evil"),
            "payload: {payload}"
        );
    }

    #[test]
    fn test_execute_dotnet_missing_assembly_and_sources() {
        // Era test_missing_assembly: no DLL and no .cs sources answers
        // ImportModuleError naming the assembly. The message rides the
        // payload object (the docker-mode compile path's shape).
        let res = exec_dotnet(
            &make_files_zip(&[("readme.txt", "nothing useful")]),
            "MyAssembly::MyNamespace.MyClass::MyMethod",
            r#"{}"#,
            30,
        );
        assert_eq!(
            res.function_error.as_deref(),
            Some("Runtime.ImportModuleError")
        );
        let payload: Value = serde_json::from_str(&res.payload).unwrap();
        assert_eq!(payload["errorType"], "Runtime.ImportModuleError");
        assert!(
            payload["errorMessage"]
                .as_str()
                .unwrap_or_default()
                .contains("MyAssembly.dll"),
            "payload: {payload}"
        );
    }

    #[cfg(unix)]
    #[test]
    fn test_execute_dotnet_source_compile() {
        // Era + compat `TestDotnetSourceCompilation`: a .cs-only package
        // compiles server-side (csproj guard staging), then runs.
        if !has_bin("dotnet") {
            return;
        }
        let cs = "using System;\nnamespace MyLambda\n{\n    public class Function\n    {\n        public string Handler(string eventJson, object context)\n        {\n            return \"{\\\"compiled\\\": true}\";\n        }\n    }\n}\n";
        let zip = make_files_zip(&[("Handler.cs", cs)]);
        let res = exec_dotnet(&zip, "MyLambda::MyLambda.Function::Handler", r#"{}"#, 60);
        assert_eq!(
            res.function_error, None,
            "logs: {} payload: {}",
            res.logs, res.payload
        );
        let payload: Value = serde_json::from_str(&res.payload).unwrap();
        assert_eq!(payload["compiled"], json!(true));
    }

    #[cfg(unix)]
    #[test]
    fn test_execute_dotnet_prebuilt_dll() {
        // Era pre-compiled mode: zip carries MyLambda.dll (class library) —
        // the cached reflection bootstrap runs it via LAMBDA_CODE_DIR.
        if !has_bin("dotnet") {
            return;
        }
        let cs = "using System;\n\nnamespace MyLambda\n{\n    public class Function\n    {\n        public string Handler(string eventJson, object context)\n        {\n            return \"{\\\"prefab\\\": \\\"yes\\\"}\";\n        }\n    }\n}\n";
        let zip = make_dotnet_dll_zip(cs);
        let res = exec_dotnet(&zip, "MyLambda::MyLambda.Function::Handler", r#"{}"#, 60);
        assert_eq!(
            res.function_error, None,
            "logs: {} payload: {}",
            res.logs, res.payload
        );
        let payload: Value = serde_json::from_str(&res.payload).unwrap();
        assert_eq!(payload["prefab"], json!("yes"));

        // Compat not-found shapes against the same assembly: a wrong type
        // (ImportModuleError) and a wrong method (HandlerNotFound).
        let wrong_type = exec_dotnet(&zip, "MyLambda::MyLambda.WrongClass::Handler", r#"{}"#, 60);
        assert_eq!(wrong_type.function_error.as_deref(), Some("Handled"));
        let payload: Value = serde_json::from_str(&wrong_type.payload).unwrap();
        assert_eq!(payload["errorType"], "Runtime.ImportModuleError");
        assert!(
            payload["errorMessage"]
                .as_str()
                .unwrap_or_default()
                .contains("WrongClass"),
            "payload: {payload}"
        );

        let wrong_method = exec_dotnet(
            &zip,
            "MyLambda::MyLambda.Function::NonExistentMethod",
            r#"{}"#,
            60,
        );
        assert_eq!(wrong_method.function_error.as_deref(), Some("Handled"));
        let payload: Value = serde_json::from_str(&wrong_method.payload).unwrap();
        assert!(
            payload["errorMessage"]
                .as_str()
                .unwrap_or_default()
                .contains("NonExistentMethod"),
            "payload: {payload}"
        );
    }

    #[cfg(unix)]
    #[test]
    fn test_execute_dotnet_exception_is_handled() {
        // Compat test_exception_thrown: the bridge's TargetInvocationException
        // branch surfaces (message, type) as a Handled error payload.
        if !has_bin("dotnet") {
            return;
        }
        let cs = "using System;\n\nnamespace MyLambda\n{\n    public class Function\n    {\n        public string Handler(string eventJson, object context)\n        {\n            throw new InvalidOperationException(\"something went wrong\");\n        }\n    }\n}\n";
        let zip = make_dotnet_dll_zip(cs);
        let res = exec_dotnet(&zip, "MyLambda::MyLambda.Function::Handler", r#"{}"#, 60);
        assert_eq!(res.function_error.as_deref(), Some("Handled"));
        let payload: Value = serde_json::from_str(&res.payload).unwrap();
        assert!(
            payload["errorMessage"]
                .as_str()
                .unwrap_or_default()
                .contains("something went wrong"),
            "payload: {payload}"
        );
        assert_eq!(payload["errorType"], "InvalidOperationException");
    }

    // === zip-bomb budget (SHIM_PY + node bootstrap extraction) ===

    /// The running decompressed-bytes budget both extraction shims embed
    /// mirrors the custom-runtime path: exactly-at still passes, one byte
    /// past aborts with the shared cap text.
    #[test]
    fn zip_unpack_budget_bites_at_the_bound() {
        assert_eq!(
            zip_unpack_budget(ZIP_UNPACK_LIMIT_TOTAL, 0),
            Ok(ZIP_UNPACK_LIMIT_TOTAL),
            "exactly at the budget is still inside it"
        );
        let err = zip_unpack_budget(ZIP_UNPACK_LIMIT_TOTAL, 1).unwrap_err();
        assert_eq!(
            err,
            format!(
                "ZIP archive expands past the {} MB unzipped limit",
                ZIP_UNPACK_LIMIT_TOTAL / (1024 * 1024)
            )
        );
        // The shim sources carry the same constant and cap text — they are
        // process-external scripts, so the Rust fn is the shared contract.
        assert!(
            SHIM_PY.contains("ZIP_UNPACK_BUDGET_BYTES = 250 * 1024 * 1024"),
            "python shim carries the 250MB budget"
        );
        assert!(
            BOOTSTRAP_JS.contains("ZIP_UNPACK_BUDGET_BYTES = 250 * 1024 * 1024"),
            "node bootstrap carries the 250MB budget"
        );
        for shim in [SHIM_PY, BOOTSTRAP_JS] {
            assert!(
                shim.contains("ZIP archive expands past the"),
                "the cap text rides both shims"
            );
            assert!(
                shim.contains("raise SystemExit") || shim.contains("throw new Error"),
                "extraction aborts in both shims"
            );
        }
    }

    /// Bomb-fixture maker (via python3, streaming — no 251MB buffer): one
    /// deflated 251MB-of-zeros entry beside a working handler entry. The
    /// declared uncompressed sizes cross the budget; the compressed bytes
    /// stay tiny.
    fn make_zip_bomb() -> Vec<u8> {
        let dir = std::env::temp_dir().join(format!("lam-bomb-{}", uuid::Uuid::new_v4().simple()));
        fs::create_dir_all(&dir).unwrap();
        let zpath = dir.join("bomb.zip");
        let snippet = r#"import sys, zipfile
with zipfile.ZipFile(sys.argv[1], "w", compression=zipfile.ZIP_DEFLATED) as zf:
    zf.writestr("lambda_function.py", sys.argv[2])
    with zf.open("big.bin", "w") as fh:
        chunk = bytes([0]) * (1024 * 1024)
        for _ in range(251):
            fh.write(chunk)
"#;
        let out = std::process::Command::new("python3")
            .arg("-c")
            .arg(snippet)
            .arg(zpath.as_os_str())
            .arg("def handler(e, c):\n    return {'ok': True}")
            .stdin(Stdio::null())
            .output()
            .expect("python3 failed");
        assert!(
            out.status.success(),
            "bomb-zip maker failed: {}",
            String::from_utf8_lossy(&out.stderr)
        );
        let z = fs::read(&zpath).unwrap();
        let _ = fs::remove_dir_all(&dir);
        z
    }

    #[test]
    fn python_shim_rejects_a_zip_bomb_at_the_budget() {
        let zip = make_zip_bomb();
        let res = execute_python(
            &zip,
            &[],
            "lambda_function.handler",
            r#"{}"#,
            "bomb-fn",
            "$LATEST",
            "arn:aws:lambda:us-east-1:123456789012:function:bomb-fn",
            &HashMap::new(),
            20,
            128,
            "us-east-1",
            "123456789012",
        );
        assert_eq!(res.function_error.as_deref(), Some("Unhandled"));
        let payload: Value = serde_json::from_str(&res.payload).unwrap();
        assert_eq!(
            payload["errorType"], "Runtime.InternalError",
            "payload: {payload}"
        );
        assert!(
            res.logs
                .contains("ZIP archive expands past the 250 MB unzipped limit"),
            "the shim prints the cap text — logs: {}",
            res.logs
        );
    }

    #[test]
    fn node_bootstrap_rejects_a_zip_bomb_at_the_budget() {
        let zip = make_zip_bomb();
        let res = execute_nodejs(
            "nodejs20.x",
            &zip,
            &[],
            "lambda_function.handler",
            r#"{}"#,
            "bomb-node-fn",
            "$LATEST",
            &HashMap::new(),
            20,
            128,
            "us-east-1",
            "123456789012",
        );
        assert!(res.function_error.is_some(), "logs: {}", res.logs);
        let payload: Value = serde_json::from_str(&res.payload).unwrap();
        assert_eq!(payload["errorType"], "Runtime.ExitError");
        assert!(
            res.logs
                .contains("ZIP archive expands past the 250 MB unzipped limit"),
            "the bootstrap prints the cap text — logs: {}",
            res.logs
        );
    }

    // The node bootstrap's budget holds against a LYING central directory:
    // an entry declaring 1 MB while actually inflating 300 MB (patched
    // fixture, the review's verified 306KB→300MB write) is stopped by the
    // inflate clamp and aborts with the cap text instead of writing.
    #[test]
    fn node_bootstrap_rejects_a_lying_central_directory() {
        // Same fixture shape as make_zip_bomb, but the central directory's
        // (and local header's) uncompressed size gets patched down to 1 MB
        // so only the real-byte accounting can refuse it.
        let dir = std::env::temp_dir().join(format!("lam-lie-{}", uuid::Uuid::new_v4().simple()));
        fs::create_dir_all(&dir).unwrap();
        let zpath = dir.join("lying.zip");
        let snippet = r#"import sys, zipfile
with zipfile.ZipFile(sys.argv[1], "w", compression=zipfile.ZIP_DEFLATED) as zf:
    zf.writestr("lambda_function.py", sys.argv[2])
    with zf.open("big.bin", "w") as fh:
        chunk = bytes([0]) * (1024 * 1024)
        for _ in range(300):
            fh.write(chunk)
"#;
        let out = std::process::Command::new("python3")
            .arg("-c")
            .arg(snippet)
            .arg(zpath.as_os_str())
            .arg("def handler(e, c):\n    return {'ok': True}")
            .stdin(Stdio::null())
            .output()
            .expect("python3 failed");
        assert!(
            out.status.success(),
            "lying-zip maker failed: {}",
            String::from_utf8_lossy(&out.stderr)
        );
        let mut zip = fs::read(&zpath).unwrap();
        let _ = fs::remove_dir_all(&dir);

        // Patch every big.bin size declaration down to 1 MB: the central
        // directory entry's usize (offset +24) and the local header's usize
        // (+22).
        fn le32(buf: &[u8], at: usize) -> u32 {
            u32::from_le_bytes(buf[at..at + 4].try_into().unwrap())
        }
        let locate_eocd = |buf: &[u8]| -> usize {
            let lower = buf.len().saturating_sub(22 + 65535);
            // `..=len-22`: a comment-less archive ends exactly at the EOCD.
            (lower..buf.len().saturating_sub(21))
                .rev()
                .find(|&i| buf[i..i + 4] == [0x50, 0x4b, 0x05, 0x06])
                .expect("EOCD present")
        };
        let eocd = locate_eocd(&zip) as usize;
        let entry_count =
            u16::from_le_bytes(zip[eocd + 10..eocd + 12].try_into().unwrap()) as usize;
        let mut offset = le32(&zip, eocd + 16) as usize;
        let mut patched = false;
        for _ in 0..entry_count {
            assert_eq!(
                le32(&zip, offset),
                0x02014b50,
                "central directory entry signature"
            );
            let name_len =
                u16::from_le_bytes(zip[offset + 28..offset + 30].try_into().unwrap()) as usize;
            let extra_len =
                u16::from_le_bytes(zip[offset + 30..offset + 32].try_into().unwrap()) as usize;
            let comment_len =
                u16::from_le_bytes(zip[offset + 32..offset + 34].try_into().unwrap()) as usize;
            let lfh_offset = le32(&zip, offset + 42) as usize;
            let name = &zip[offset + 46..offset + 46 + name_len];
            if name == b"big.bin" {
                zip[offset + 24..offset + 28].copy_from_slice(&1_048_576u32.to_le_bytes());
                zip[lfh_offset + 22..lfh_offset + 26].copy_from_slice(&1_048_576u32.to_le_bytes());
                patched = true;
                break;
            }
            offset += 46 + name_len + extra_len + comment_len;
        }
        assert!(patched, "big.bin entry found in the fixture");

        let res = execute_nodejs(
            "nodejs20.x",
            &zip,
            &[],
            "lambda_function.handler",
            r#"{}"#,
            "bomb-node-fn",
            "$LATEST",
            &HashMap::new(),
            20,
            128,
            "us-east-1",
            "123456789012",
        );
        assert!(res.function_error.is_some(), "logs: {}", res.logs);
        let payload: Value = serde_json::from_str(&res.payload).unwrap();
        assert_eq!(payload["errorType"], "Runtime.ExitError");
        assert!(
            res.logs
                .contains("ZIP archive expands past the 250 MB unzipped limit"),
            "the declared budget pass must not smuggle a 300MB write: {}",
            res.logs
        );
    }
}
