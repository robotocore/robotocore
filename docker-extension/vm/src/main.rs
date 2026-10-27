//! Backend service for the Robotocore Docker Desktop extension.
//!
//! Proxies between the Docker Desktop UI and the robotocore container,
//! providing start/stop/status/logs management endpoints plus a
//! transparent `/proxy/*` forward to the twin's management endpoints.
//!
//! Rust port of the former Python (uvicorn/starlette) backend — same
//! routes, same JSON response shapes.

use std::env;
use std::time::Duration;

use axum::body::Bytes;
use axum::extract::{Path, RawQuery, State};
use axum::http::{HeaderMap, Method, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::routing::{any, get, post};
use axum::{Json, Router};
use serde_json::{json, Value};
use tokio::process::Command;

/// subprocess.run(..., timeout=30) equivalent for docker CLI calls.
const DOCKER_TIMEOUT: Duration = Duration::from_secs(30);
/// httpx.AsyncClient(timeout=10.0) equivalent for the proxy.
const PROXY_TIMEOUT: Duration = Duration::from_secs(10);

#[derive(Clone)]
struct ExtConfig {
    image: String,
    container: String,
    port: u16,
}

impl ExtConfig {
    fn from_env() -> Self {
        Self {
            image: env::var("ROBOTOCORE_IMAGE")
                .unwrap_or_else(|_| "ghcr.io/robotocore/robotocore:latest".to_string()),
            container: env::var("ROBOTOCORE_CONTAINER")
                .unwrap_or_else(|_| "robotocore".to_string()),
            port: env::var("ROBOTOCORE_PORT")
                .ok()
                .and_then(|p| p.parse().ok())
                .unwrap_or(4566),
        }
    }
}

struct DockerOutput {
    ok: bool,
    stdout: String,
    stderr: String,
}

enum DockerResult {
    /// The docker CLI ran and produced exit status plus output.
    Completed(DockerOutput),
    /// The CLI could not run (not installed) or timed out — mirrors the
    /// Python backend, where such failures surfaced as 500s instead of
    /// being mistaken for "container not present".
    Unavailable(String),
}

/// Run a docker CLI command and capture its output.
async fn run_docker(args: &[String]) -> DockerResult {
    let result =
        tokio::time::timeout(DOCKER_TIMEOUT, Command::new("docker").args(args).output()).await;
    match result {
        Ok(Ok(out)) => DockerResult::Completed(DockerOutput {
            ok: out.status.success(),
            stdout: String::from_utf8_lossy(&out.stdout).into_owned(),
            stderr: String::from_utf8_lossy(&out.stderr).into_owned(),
        }),
        Ok(Err(e)) => DockerResult::Unavailable(format!("docker is unavailable: {e}")),
        Err(_) => DockerResult::Unavailable("docker command timed out".to_string()),
    }
}

fn json_result(value: Value) -> Response {
    Json(value).into_response()
}

fn error_result(status: StatusCode, message: String) -> Response {
    (status, Json(json!({ "error": message.trim() }))).into_response()
}

/// Inspect the robotocore container.
///
/// `Err` when the docker CLI is unavailable (like Python's 500s),
/// `Ok(None)` when the container does not exist, `Ok(Some(info))` when
/// it exists.
async fn container_info(cfg: &ExtConfig) -> Result<Option<Value>, String> {
    let args = vec![
        "inspect".to_string(),
        cfg.container.clone(),
        "--format".to_string(),
        "{{json .}}".to_string(),
    ];
    match run_docker(&args).await {
        DockerResult::Unavailable(e) => Err(e),
        DockerResult::Completed(out) => {
            if !out.ok {
                // Nonzero rc means "no such container" — the container
                // genuinely does not exist.
                return Ok(None);
            }
            Ok(serde_json::from_str(out.stdout.trim()).ok().flatten())
        }
    }
}

async fn status(State(cfg): State<ExtConfig>) -> Response {
    match container_info(&cfg).await {
        Err(e) => error_result(StatusCode::INTERNAL_SERVER_ERROR, e),
        Ok(None) => json_result(json!({
            "running": false,
            "container_id": Value::Null,
            "uptime": Value::Null,
            "port": cfg.port,
        })),
        Ok(Some(info)) => {
            let running = info
                .get("State")
                .and_then(|s| s.get("Running"))
                .and_then(Value::as_bool)
                .unwrap_or(false);
            let uptime = if running {
                info.get("State")
                    .and_then(|s| s.get("StartedAt"))
                    .and_then(Value::as_str)
                    .map(String::from)
            } else {
                None
            };
            // Python: info.get("Id", "")[:12] — always a string here.
            let container_id: String = info
                .get("Id")
                .and_then(Value::as_str)
                .unwrap_or("")
                .chars()
                .take(12)
                .collect();
            json_result(json!({
                "running": running,
                "container_id": container_id,
                "uptime": uptime,
                "port": cfg.port,
            }))
        }
    }
}

/// Any JSON-parse failure falls back to defaults, like the Python
/// handler (# noqa: BLE001 comment there).
fn env_flags(body: &[u8]) -> Vec<String> {
    let parsed: Value = if body.is_empty() {
        Value::Null
    } else {
        serde_json::from_slice(body).unwrap_or(Value::Null)
    };
    match parsed.get("env").and_then(Value::as_object) {
        Some(vars) => {
            let mut flags = Vec::new();
            for (key, value) in vars {
                let val = match value {
                    Value::String(s) => s.clone(),
                    other => other.to_string(),
                };
                flags.push("-e".to_string());
                flags.push(format!("{key}={val}"));
            }
            flags
        }
        None => Vec::new(),
    }
}

async fn start(State(cfg): State<ExtConfig>, body: Bytes) -> Response {
    let env_flags = env_flags(&body);

    // Remove existing container if present (result ignored, as in Python).
    run_docker(&["rm".into(), "-f".into(), cfg.container.clone()]).await;

    let port_map = format!("{}:{}", cfg.port, cfg.port);
    let mut args: Vec<String> = vec![
        "run".into(),
        "-d".into(),
        "--name".into(),
        cfg.container.clone(),
        "-p".into(),
        port_map,
    ];
    args.extend(env_flags);
    args.push(cfg.image.clone());
    match run_docker(&args).await {
        DockerResult::Unavailable(e) => error_result(StatusCode::INTERNAL_SERVER_ERROR, e),
        DockerResult::Completed(result) => {
            if !result.ok {
                return error_result(StatusCode::INTERNAL_SERVER_ERROR, result.stderr);
            }
            let container_id: String = result.stdout.trim().chars().take(12).collect();
            json_result(json!({
                "started": true,
                "container_id": container_id,
            }))
        }
    }
}

async fn stop(State(cfg): State<ExtConfig>) -> Response {
    let stop_arg = cfg.container.clone();
    let result = match run_docker(&["stop".into(), stop_arg]).await {
        DockerResult::Unavailable(e) => return error_result(StatusCode::INTERNAL_SERVER_ERROR, e),
        DockerResult::Completed(out) => out,
    };
    if !result.ok {
        return error_result(StatusCode::INTERNAL_SERVER_ERROR, result.stderr);
    }
    // Remove the container after stopping (result ignored).
    run_docker(&["rm".into(), cfg.container.clone()]).await;
    json_result(json!({ "stopped": true }))
}

/// `?tail=N` passthrough of `docker logs`, defaults to 100. The value is
/// percent-decoded like Python's `request.query_params.get`.
fn tail_param(query: Option<&str>) -> String {
    query
        .unwrap_or("")
        .split('&')
        .find_map(|pair| {
            let (k, v) = pair.split_once('=')?;
            (k == "tail").then(|| {
                urlencoding::decode(v)
                    .map(String::from)
                    .unwrap_or_else(|_| v.to_string())
            })
        })
        .unwrap_or_else(|| "100".to_string())
}

async fn logs(State(cfg): State<ExtConfig>, RawQuery(query): RawQuery) -> Response {
    let tail = tail_param(query.as_deref());
    let args = vec![
        "logs".to_string(),
        "--tail".to_string(),
        tail,
        cfg.container.clone(),
    ];
    // Python combined stdout+stderr and surfaced CLI failures as errors;
    // a successful call just streams stdout+stderr as text.
    match run_docker(&args).await {
        DockerResult::Unavailable(e) => error_result(StatusCode::INTERNAL_SERVER_ERROR, e),
        DockerResult::Completed(out) => (
            [(http::header::CONTENT_TYPE, "text/plain")],
            out.stdout + &out.stderr,
        )
            .into_response(),
    }
}

/// GET/POST/PUT/DELETE /proxy/* — forward to the robotocore management
/// endpoints with the /proxy prefix stripped and headers (except host)
/// preserved.
async fn proxy(
    State(cfg): State<ExtConfig>,
    method: Method,
    Path(rest): Path<String>,
    RawQuery(query): RawQuery,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    proxy_inner(cfg, method, rest, query, headers, body).await
}

/// The Python `{path:path}` converter also matched a bare `/proxy`.
async fn proxy_root(
    State(cfg): State<ExtConfig>,
    method: Method,
    RawQuery(query): RawQuery,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    proxy_inner(cfg, method, String::new(), query, headers, body).await
}

async fn proxy_inner(
    cfg: ExtConfig,
    method: Method,
    rest: String,
    query: Option<String>,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    let target = if rest.is_empty() {
        format!("http://127.0.0.1:{}/", cfg.port)
    } else {
        format!("http://127.0.0.1:{}/{rest}", cfg.port)
    };
    let target = match query {
        Some(q) if !q.is_empty() => format!("{target}?{q}"),
        _ => target,
    };

    let mut fwd: http::HeaderMap = http::HeaderMap::new();
    for (name, value) in headers.iter() {
        if name.as_str() != "host" {
            fwd.insert(name.clone(), value.clone());
        }
    }

    let client = match reqwest::Client::builder().timeout(PROXY_TIMEOUT).build() {
        Ok(c) => c,
        Err(e) => return error_result(StatusCode::INTERNAL_SERVER_ERROR, e.to_string()),
    };
    let resp = match client
        .request(method, &target)
        .headers(fwd)
        .body(body)
        .send()
        .await
    {
        Ok(r) => r,
        Err(e) => {
            if e.is_connect() {
                return error_result(
                    StatusCode::BAD_GATEWAY,
                    "Cannot connect to robotocore. Is it running?".to_string(),
                );
            }
            return error_result(StatusCode::INTERNAL_SERVER_ERROR, e.to_string());
        }
    };

    let status =
        StatusCode::from_u16(resp.status().as_u16()).unwrap_or(StatusCode::INTERNAL_SERVER_ERROR);
    let is_json = resp
        .headers()
        .get(http::header::CONTENT_TYPE)
        .and_then(|v| v.to_str().ok())
        .map(|v| v.starts_with("application/json"))
        .unwrap_or(false);
    let text = match resp.text().await {
        Ok(t) => t,
        Err(e) => return error_result(StatusCode::INTERNAL_SERVER_ERROR, e.to_string()),
    };
    if is_json {
        match serde_json::from_str::<Value>(&text) {
            Ok(v) => (status, Json(v)).into_response(),
            // Python surfaced parse errors as a 500 {"error": ...}.
            Err(e) => error_result(StatusCode::INTERNAL_SERVER_ERROR, e.to_string()),
        }
    } else {
        (status, Json(json!({ "body": text }))).into_response()
    }
}

#[tokio::main]
async fn main() {
    let cfg = ExtConfig::from_env();
    let port: u16 = env::var("EXTENSION_PORT")
        .ok()
        .and_then(|p| p.parse().ok())
        .unwrap_or(8080);

    let app = Router::new()
        .route("/status", get(status))
        .route("/start", post(start))
        .route("/stop", post(stop))
        .route("/logs", get(logs))
        .route("/proxy", any(proxy_root))
        .route("/proxy/*rest", any(proxy))
        .with_state(cfg);

    let listener = tokio::net::TcpListener::bind(("0.0.0.0", port))
        .await
        .expect("bind extension backend port");
    axum::serve(listener, app)
        .await
        .expect("extension backend server");
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn env_flags_from_body() {
        let body = br#"{"env": {"DEBUG": "1", "COUNT": 3}}"#;
        let flags = env_flags(body);
        // serde_json Maps iterate in sorted key order (preserve_order off).
        assert_eq!(flags, vec!["-e", "COUNT=3", "-e", "DEBUG=1"]);
    }

    #[test]
    fn env_flags_default() {
        assert!(env_flags(b"").is_empty());
        assert!(env_flags(b"not json").is_empty());
        assert!(env_flags(br#"{"other": 1}"#).is_empty());
    }

    #[test]
    fn tail_defaults_to_100() {
        assert_eq!(tail_param(None), "100");
        assert_eq!(tail_param(Some("tail=200")), "200");
        assert_eq!(tail_param(Some("other=1&tail=5")), "5");
        assert_eq!(tail_param(Some("other=1")), "100");
        // Percent-decoded like Python's query_params.
        assert_eq!(tail_param(Some("tail=%31%30")), "10");
    }
}
