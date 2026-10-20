//! Lambda handler runtime execution.
//!
//! Executes Python handler code by unpacking the deployment package into a
//! per-invocation temp directory and spawning `python3` with a small shim.
//! Mirrors robotocore's in-process Python executor: the handler receives the
//! event payload and a Lambda-like context, returns JSON-serializable data,
//! and raised exceptions surface as `FunctionError` payloads
//! (`{"errorMessage", "errorType", "stackTrace"}`) with captured stderr and
//! stdout available as function logs.

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

    def get_remaining_time_in_millis(self):
        return self._timeout_ms


def safe_extract(zippath, outdir):
    with open(zippath, "rb") as fh:
        blob = fh.read()
    with zipfile.ZipFile(io.BytesIO(blob)) as zf:
        for name in zf.namelist():
            normalized = name.replace("\\", "/")
            if normalized.startswith("/") or ".." in normalized.split("/"):
                raise ValueError(f"Unsafe path in ZIP archive: {name}")
        zf.extractall(outdir)


def main():
    work = sys.argv[1]
    result_path = os.path.join(work, "result.json")
    try:
        meta = json.load(open(os.path.join(work, "fn.json")))
        event = json.load(open(os.path.join(work, "event.json")))
        handler = meta.get("handler") or "lambda_function.handler"
        code_dir = os.path.join(work, "code")
        os.makedirs(code_dir, exist_ok=True)
        safe_extract(os.path.join(work, "code.zip"), code_dir)

        # Unpack each layerN.zip into layerN/ so its python/ subtree (or
        # package dirs) lands there and the sys.path scan below finds it.
        for layer_zip in sorted(
            p for p in os.listdir(work)
            if p.startswith("layer") and p.endswith(".zip")
        ):
            dest = os.path.join(work, layer_zip[:-4])
            os.makedirs(dest, exist_ok=True)
            safe_extract(os.path.join(work, layer_zip), dest)

        sys.path.insert(0, code_dir)
        for layer_name in sorted(
            p for p in os.listdir(work)
            if p.startswith("layer") and os.path.isdir(os.path.join(work, p))
        ):
            sys.path.insert(1, os.path.join(work, layer_name, "python"))

        if "." not in handler:
            raise ValueError(f"Bad handler: {handler}")
        module_path, func_name = handler.rsplit(".", 1)
        ctx = Ctx(meta)
        module = importlib.import_module(module_path)
        fn = getattr(module, func_name, None)
        if fn is None or not callable(fn):
            raise ValueError(
                f"Handler function '{func_name}' not found in {module_path}"
            )
        result = fn(event, ctx)
        open(result_path, "w").write(
            json.dumps({"restocore_result_raw": json.dumps(result, default=str)})
        )
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

fn safe_dir_name(fn_name: &str) -> String {
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

/// Cap on captured handler output per stream (stdout, stderr). When a
/// stream exceeds the cap only the last bytes are kept (tail semantics).
const OUTPUT_CAP: usize = 128 * 1024;

/// Drain a spawned child pipe on a dedicated thread, keeping at most
/// `OUTPUT_CAP` bytes (tail semantics) and returning a channel the caller
/// can wait on with a deadline. Reading blocks until EOF, which never
/// arrives while a grandchild inherited the pipe, so the caller must bound
/// how long it waits and abandon the thread (and its text) on timeout.
fn drain_thread<R: std::io::Read + Send + 'static>(pipe: Option<R>) -> mpsc::Receiver<String> {
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
                        if kept.len() > OUTPUT_CAP {
                            let excess = kept.len() - OUTPUT_CAP;
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
fn collect_by(rx: &mpsc::Receiver<String>, deadline: std::time::Instant) -> String {
    rx.recv_timeout(deadline.saturating_duration_since(std::time::Instant::now()))
        .unwrap_or_default()
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
    cmd.env("AWS_LAMBDA_FUNCTION_NAME", function_name)
        .env("AWS_LAMBDA_FUNCTION_VERSION", function_version)
        .env("AWS_LAMBDA_FUNCTION_MEMORY_SIZE", memory_size.to_string())
        .env("AWS_LAMBDA_FUNCTION_TIMEOUT", timeout_secs.to_string())
        .env("AWS_REGION", region)
        .env("AWS_DEFAULT_REGION", region)
        .env("AWS_ACCOUNT_ID", account_id)
        .env("AWS_ACCESS_KEY_ID", "testing")
        .env("AWS_SECRET_ACCESS_KEY", "testing")
        .env("AWS_SESSION_TOKEN", "testing")
        .env("_HANDLER", handler);
    for (k, v) in env_vars {
        cmd.env(k, v);
    }

    let mut child: Child = match cmd.spawn() {
        Ok(c) => c,
        Err(e) => return missing_runtime_result(&e.to_string()),
    };

    // Start draining pipes immediately, from dedicated threads. Output
    // beyond the OS pipe buffer blocks the child's writes until somebody
    // reads, so waiting here for child exit first would deadlock on any
    // handler that prints more than a pipe buffer.
    let stdout_rx = drain_thread(child.stdout.take());
    let stderr_rx = drain_thread(child.stderr.take());

    // Wait up to the function timeout; kill on overrun.
    let wait_secs = u64::from(timeout_secs.max(1)) + 5;
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
    let _ = timed_out; // still consulted below for the kill path
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
        return ExecResult {
            payload: json!({
                "errorMessage": format!("Task timed out after {timeout_secs} seconds"),
                "errorType": "Task.TimedOut",
            })
            .to_string(),
            function_error: Some("Handled".to_string()),
            logs,
        };
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
        _ => missing_runtime_result("handler produced no result"),
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

/// Whether this runtime family executes the `bootstrap` executable shipped in
/// the code package (provided, provided.al2, provided.al2023 and go runtimes,
/// mirroring the python-era `runtime_to_family` mapping).
pub fn is_custom_runtime(runtime: &str) -> bool {
    let r = runtime.trim().to_ascii_lowercase();
    r.starts_with("provided") || r.starts_with("go")
}

/// Extract a zip archive into `dest`, keeping unix permission bits so an
/// executable `bootstrap` comes out runnable. Archive entries carry their
/// path after the crate's own traversal sanitization (`enclosed_name`).
fn extract_zip_into(zip_bytes: &[u8], dest: &std::path::Path) -> Result<(), String> {
    let reader = std::io::Cursor::new(zip_bytes);
    let mut archive = zip::ZipArchive::new(reader).map_err(|e| e.to_string())?;
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
            let mut out_file = fs::File::create(&out).map_err(|e| e.to_string())?;
            std::io::copy(&mut entry, &mut out_file).map_err(|e| e.to_string())?;
            #[cfg(unix)]
            if let Some(mode) = entry.unix_mode() {
                use std::os::unix::fs::PermissionsExt;
                let _ = fs::set_permissions(&out, fs::Permissions::from_mode(mode & 0o777));
            }
        }
    }
    Ok(())
}

/// Execute a custom-runtime `bootstrap` executable (`provided.*`/`go` runtimes).
///
/// Mirrors the python-era twin's `CustomRuntimeExecutor`:
/// * layers are unpacked first and the code zip over them (function code
///   overrides layer files), and an executable `bootstrap` is located at the
///   package root (falling back to the handler's base name), chmod +x'd and
///   spawned with the JSON event on stdin, `cwd` at the package root and the
///   Lambda environment variables set;
/// * the result is the JSON on stdout (invalid JSON comes back as a
///   JSON-encoded string) and stderr becomes the function logs;
/// * non-zero exit yields a `Runtime.ExitError` payload (`Unhandled`) or
///   `Handled` when stdout already carries an `errorMessage` JSON object,
///   a missing bootstrap yields `Runtime.InvalidEntrypoint`, and running
///   past the function timeout (+2s subprocess grace) yields `Task.TimedOut`.
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
    // User-configured variables first, built-ins after (both mirror the
    // python-era build_env ordering).
    for (k, v) in env_vars {
        cmd.env(k, v);
    }
    cmd.env("AWS_LAMBDA_FUNCTION_NAME", function_name)
        .env("AWS_LAMBDA_FUNCTION_VERSION", function_version)
        .env("AWS_LAMBDA_FUNCTION_MEMORY_SIZE", memory_size.to_string())
        .env("AWS_LAMBDA_FUNCTION_TIMEOUT", timeout_secs.to_string())
        .env("AWS_REGION", region)
        .env("AWS_DEFAULT_REGION", region)
        .env("AWS_ACCOUNT_ID", account_id)
        .env("AWS_ACCESS_KEY_ID", "testing")
        .env("AWS_SECRET_ACCESS_KEY", "testing")
        .env("AWS_SESSION_TOKEN", "testing")
        .env("_HANDLER", handler);

    let mut child: Child = match cmd.spawn() {
        Ok(c) => c,
        Err(e) => {
            cleanup(&work);
            return missing_runtime_result(&e.to_string());
        }
    };

    // Event JSON goes in over stdin; a write error (a bootstrap that exited
    // without reading it all) is swallowed, mirroring python's communicate().
    if let Some(mut stdin) = child.stdin.take() {
        let _ = stdin.write_all(event_str.as_bytes());
        let _ = stdin.flush();
    }

    // Start draining pipes immediately, from dedicated threads, so a
    // chatty bootstrap cannot deadlock on the OS pipe buffer.
    let stdout_rx = drain_thread(child.stdout.take());
    let stderr_rx = drain_thread(child.stderr.take());

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

#[cfg(test)]
mod tests {
    use super::*;

    fn make_zip(code: &str) -> Vec<u8> {
        // The shim requires python3 anyway, so build the test zip with it.
        let dir = std::env::temp_dir().join(format!("lam-test-{}", uuid::Uuid::new_v4().simple()));
        fs::create_dir_all(&dir).unwrap();
        let zpath = dir.join("t.zip");
        let snippet = "import io, sys, zipfile\nbuf = io.BytesIO()\nze = zipfile.ZipFile(buf, 'w')\nze.writestr('lambda_function.py', sys.argv[2])\nze.close()\nopen(sys.argv[1], 'wb').write(buf.getvalue())\n";
        let out = std::process::Command::new("python3")
            .arg("-c")
            .arg(snippet)
            .arg(zpath.as_os_str())
            .arg(code)
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
            res.logs.len() <= OUTPUT_CAP + 8,
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

    // --- custom runtime (bootstrap) executor ---

    /// Build a zip with a single `bootstrap` file carrying the given unix
    /// permission bits, mirroring the compat suite's `_make_bootstrap_zip`.
    #[cfg(unix)]
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

    #[cfg(unix)]
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
}
