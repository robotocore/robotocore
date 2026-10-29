//! Cross-service invocation bridge.
//!
//! Lets other service crates (SNS subscriptions, Step Functions Task states,
//! EventBridge targets) execute a Lambda function without owning a
//! `LambdaHandler`. Live `LambdaState`s register here on every handler
//! state resolution; because `LambdaState` wraps its maps in `Arc`s a
//! registered handle stays live, and `reset`/restore writes it in place so
//! the bridge never goes stale. Mirrors the `s3::bridge` pattern.

use crate::models::LambdaState;
use crate::runtime;
use parking_lot::RwLock;
use serde_json::Value;
use std::collections::HashMap;
use std::sync::{Arc, OnceLock};

fn registered() -> &'static RwLock<HashMap<(u64, String), LambdaState>> {
    static REGISTRY: OnceLock<RwLock<HashMap<(u64, String), LambdaState>>> = OnceLock::new();
    REGISTRY.get_or_init(|| RwLock::new(HashMap::new()))
}

/// Register (or re-register) the live state for (account, region). Called by
/// the handler whenever it resolves its per-account/region state.
pub fn register_state(account: u64, region: &str, state: &LambdaState) {
    registered()
        .write()
        .insert((account, region.to_string()), state.clone());
}

/// Live handle of the registered state for (account, region).
pub fn get_state(account: u64, region: &str) -> Option<LambdaState> {
    registered()
        .read()
        .get(&(account, region.to_string()))
        .cloned()
}

/// ESM attach hook: the gateway's ESM engine registers itself so
/// `CreateEventSourceMapping` can freeze a kinesis stream's `LATEST`
/// position at attach time. Mirrors moto, which registers the mapping on
/// its source object synchronously (`moto/kinesis/models.py`
/// KinesisStream holds `lambda_event_source_mappings`; `put_record` then
/// pushes to every registered mapping) — a mapping therefore only sees
/// records assigned after registration. The hook receives
/// (account, region, event_source_arn) and returns the per-shard
/// `(shard id, next sequence number)` watermark pairs to freeze; `None`
/// means the ARN is not a kinesis stream or the stream does not exist yet
/// (later shards then bind at first poll sight). No-op (None) until the
/// engine registers it at startup.
pub type EsmAttachHook =
    Arc<dyn Fn(u64, &str, &str) -> Option<Vec<(String, String)>> + Send + Sync>;

fn esm_attach_hook() -> &'static RwLock<Option<EsmAttachHook>> {
    static HOOK: OnceLock<RwLock<Option<EsmAttachHook>>> = OnceLock::new();
    HOOK.get_or_init(|| RwLock::new(None))
}

pub fn register_esm_attach_hook(hook: EsmAttachHook) {
    *esm_attach_hook().write() = Some(hook);
}

/// Attach-time per-shard watermarks for a kinesis-ARN mapping (None
/// otherwise). The caller stores them on the freshly built
/// `EventSourceMapping` (see `kinesis_bound_watermarks`) so the engine's
/// first sight reads them atomically with the mapping's publication.
pub fn kinesis_attach_watermarks(
    account: u64,
    region: &str,
    event_source_arn: &str,
) -> Option<Vec<(String, String)>> {
    let hook = esm_attach_hook().read().clone()?;
    hook(account, region, event_source_arn)
}

/// Successful = handler returned; failure = handler raised / runtime invalid.
pub struct InvokeOutcome {
    pub payload: String,
    pub function_error: Option<String>,
}

/// Whether the function addressed by `arn` exists (any qualifier).
pub fn function_arn_exists(arn: &str) -> bool {
    resolve_function(arn).is_some()
}

/// Resolve + synchronously execute a function addressed by its (unversioned
/// or versioned) ARN. `None` when no such function is registered.
///
/// Runtime semantics mirror the handler's `Invoke` path exactly (docker-mode
/// precedence, then the in-process executors family by family), including
/// the no-code "simple-mode" response and the `Runtime.InvalidRuntime`
/// error shape for runtimes no executor family claims.
pub fn invoke_by_arn(arn: &str, event: &Value) -> Option<InvokeOutcome> {
    let func = resolve_function(arn)?;
    let event_json = serde_json::to_string(event).unwrap_or_else(|_| "{}".to_string());

    let zip_bytes = func.code_zip.read().clone();
    if zip_bytes.is_empty() {
        return Some(InvokeOutcome {
            payload: "{}".to_string(),
            function_error: None,
        });
    }
    let state = resolve_state(arn)?;
    let mut layer_zips: Vec<Vec<u8>> = Vec::new();
    for layer_entry in func.layers.read().iter() {
        let arn = layer_entry.get("Arn").and_then(Value::as_str).unwrap_or("");
        if let Some(lv) = state.layers_versions_by_arn(arn) {
            let z = lv.code_zip.clone();
            if !z.is_empty() {
                layer_zips.push(z);
            }
        }
    }
    let region = arn.split(':').nth(3).unwrap_or("us-east-1").to_string();
    let account = arn.split(':').nth(4).unwrap_or("123456789012").to_string();
    let runtime_name = func.runtime.read().clone();
    // LAMBDA_EXECUTOR=docker: official runtime images via the docker CLI
    // when mode/daemon/image all check out (same dispatch as the REST path);
    // None keeps the in-process dispatch below.
    if let Some(out) = crate::docker_runtime::try_execute_docker(
        &runtime_name,
        &zip_bytes,
        &layer_zips,
        &func.handler.read().clone(),
        &event_json,
        &func.function_name,
        &func.version.read().clone(),
        &func.function_arn.clone(),
        &func.environment.read().clone().unwrap_or_default(),
        *func.timeout.read(),
        *func.memory_size.read(),
        &region,
        &account,
        None,
    ) {
        return Some(InvokeOutcome {
            payload: out.payload,
            function_error: out.function_error,
        });
    }
    let out = if runtime::is_custom_runtime(&runtime_name) {
        runtime::execute_custom(
            &zip_bytes,
            &layer_zips,
            &func.handler.read().clone(),
            &event_json,
            &func.function_name,
            &func.version.read().clone(),
            &func.environment.read().clone().unwrap_or_default(),
            *func.timeout.read(),
            *func.memory_size.read(),
            &region,
            &account,
        )
    } else if runtime::is_node_runtime(&runtime_name) {
        runtime::execute_nodejs(
            &runtime_name,
            &zip_bytes,
            &layer_zips,
            &func.handler.read().clone(),
            &event_json,
            &func.function_name,
            &func.version.read().clone(),
            &func.environment.read().clone().unwrap_or_default(),
            *func.timeout.read(),
            *func.memory_size.read(),
            &region,
            &account,
        )
    } else if runtime::is_ruby_runtime(&runtime_name) {
        runtime::execute_ruby(
            &runtime_name,
            &zip_bytes,
            &layer_zips,
            &func.handler.read().clone(),
            &event_json,
            &func.function_name,
            &func.version.read().clone(),
            &func.function_arn.clone(),
            &func.environment.read().clone().unwrap_or_default(),
            *func.timeout.read(),
            *func.memory_size.read(),
            &region,
            &account,
        )
    } else if runtime::is_java_runtime(&runtime_name) {
        runtime::execute_java(
            &runtime_name,
            &zip_bytes,
            &layer_zips,
            &func.handler.read().clone(),
            &event_json,
            &func.function_name,
            &func.version.read().clone(),
            &func.function_arn.clone(),
            &func.environment.read().clone().unwrap_or_default(),
            *func.timeout.read(),
            *func.memory_size.read(),
            &region,
            &account,
        )
    } else if runtime::is_dotnet_runtime(&runtime_name) {
        runtime::execute_dotnet(
            &runtime_name,
            &zip_bytes,
            &layer_zips,
            &func.handler.read().clone(),
            &event_json,
            &func.function_name,
            &func.version.read().clone(),
            &func.function_arn.clone(),
            &func.environment.read().clone().unwrap_or_default(),
            *func.timeout.read(),
            *func.memory_size.read(),
            &region,
            &account,
        )
    } else if runtime::is_python_runtime(&runtime_name) {
        runtime::execute_python(
            &zip_bytes,
            &layer_zips,
            &func.handler.read().clone(),
            &event_json,
            &func.function_name,
            &func.version.read().clone(),
            &func.function_arn.clone(),
            &func.environment.read().clone().unwrap_or_default(),
            *func.timeout.read(),
            *func.memory_size.read(),
            &region,
            &account,
        )
    } else {
        return Some(InvokeOutcome {
            payload: "null".to_string(),
            function_error: Some("Runtime.InvalidRuntime".to_string()),
        });
    };
    eprintln!(
        "invoke_by_arn outcome payload={} err={:?}",
        out.payload, out.function_error
    );
    Some(InvokeOutcome {
        payload: out.payload,
        function_error: out.function_error,
    })
}
/// Sharpest ARN form: (base function name, lambda state) for resolution.
fn resolve_function(arn: &str) -> Option<Arc<crate::models::LambdaFunction>> {
    let mut parts = arn.split(':');
    if parts.next()? != "arn" {
        return None;
    }
    if parts.next()? != "aws" {
        return None;
    }
    if parts.next()? != "lambda" {
        return None;
    }
    let _region = parts.next()?;
    let _account = parts.next()?;
    if parts.next()? != "function" {
        return None;
    }
    let name = parts.next()?.to_string();
    let qualifier = parts.next().map(String::from);

    let state = resolve_state(arn)?;
    let entry = state.functions.read().get(&name)?.clone();
    let target_version = qualifier.unwrap_or_else(|| "$LATEST".to_string());
    if target_version == "$LATEST" {
        return entry.latest;
    }
    entry
        .versions
        .iter()
        .find(|v| v.version.read().as_str() == target_version)
        .cloned()
        .or(entry.latest)
}

/// Registered state for the (region, account) encoded in a lambda ARN.
fn resolve_state(arn: &str) -> Option<LambdaState> {
    let parts: Vec<&str> = arn.split(':').collect();
    if parts.len() < 5 || parts[0] != "arn" || parts[2] != "lambda" {
        return None;
    }
    let account = parts[4].parse::<u64>().ok()?;
    registered()
        .read()
        .get(&(account, parts[3].to_string()))
        .cloned()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::models::LambdaState;
    use serde_json::json;

    fn make_zip(code: &str) -> Vec<u8> {
        // The runtime exercises a real zipfile: build one via python3 like
        // the handler tests do.
        let dir =
            std::env::temp_dir().join(format!("lam-bridge-{}", uuid::Uuid::new_v4().simple()));
        std::fs::create_dir_all(&dir).unwrap();
        let zpath = dir.join("t.zip");
        let snippet = "import io, sys, zipfile\nbuf = io.BytesIO()\nze = zipfile.ZipFile(buf, 'w')\nze.writestr(sys.argv[3], sys.argv[2])\nze.close()\nopen(sys.argv[1], 'wb').write(buf.getvalue())\n";
        let st = std::process::Command::new("python3")
            .arg("-c")
            .arg(snippet)
            .arg(zpath.as_os_str())
            .arg(code)
            .arg("lambda_function.py")
            .stdin(std::process::Stdio::null())
            .output()
            .expect("python3 failed");
        assert!(st.status.success(), "zip maker failed");
        let zip = std::fs::read(&zpath).unwrap();
        let _ = std::fs::remove_dir_all(&dir);
        zip
    }

    fn make_function(state: &LambdaState, account: u64, region: &str, name: &str, code: &str) {
        let func = crate::models::LambdaFunction::new(account, region, name.to_string());
        *func.code_zip.write() = make_zip(code);
        *func.handler.write() = "lambda_function.handler".into();
        state.functions.write().insert(
            name.to_string(),
            crate::models::FunctionEntry {
                latest: Some(Arc::new(func)),
                versions: Vec::new(),
            },
        );
    }

    #[test]
    fn invoke_by_arn_runs_registered_function() {
        let account = 434343434343u64;
        let region = "bridge-inv";
        let state = LambdaState::new();
        make_function(
            &state,
            account,
            region,
            "bridge-fn",
            "def handler(e,c): return e['n'] * 2",
        );
        register_state(account, region, &state);

        let out = invoke_by_arn(
            "arn:aws:lambda:bridge-inv:434343434343:function:bridge-fn",
            &json!({"n": 5}),
        )
        .expect("function registered");
        assert!(out.function_error.is_none(), "outcome: {}", out.payload);
        assert_eq!(out.payload.trim_matches('"'), "10");
    }

    #[test]
    fn unknown_function_resolves_none() {
        assert!(invoke_by_arn(
            "arn:aws:lambda:us-east-1:123456789012:function:no-such-fn-bridge",
            &json!({})
        )
        .is_none());
    }

    // === managed families through the bridge (SNS/EventBridge/FSM invokes) ===

    fn toolchain_missing(bin: &str) -> bool {
        std::env::var_os("PATH")
            .map(|paths| !std::env::split_paths(&paths).any(|dir| dir.join(bin).is_file()))
            .unwrap_or(true)
    }

    /// Stage bytes as one staged tree and zip it with python's zipfile
    /// (walk + relative paths, like a real deployment package) — works for
    /// text sources and compiled artifacts (.class/.dll) alike.
    fn make_staged_zip(files: &[(&str, &[u8])]) -> Vec<u8> {
        let dir =
            std::env::temp_dir().join(format!("lam-bridge-stage-{}", uuid::Uuid::new_v4().simple()));
        std::fs::create_dir_all(&dir).unwrap();
        for (name, content) in files {
            let path = dir.join(name);
            if let Some(parent) = path.parent() {
                std::fs::create_dir_all(parent).unwrap();
            }
            std::fs::write(path, content).unwrap();
        }
        let out_path =
            dir.parent().unwrap().join(format!("{}.zip", dir.file_name().unwrap().to_string_lossy()));
        let out_str = out_path.to_string_lossy().into_owned();
        let root_str = dir.to_string_lossy().into_owned();
        let snippet = "import os, sys, zipfile\nroot, out = sys.argv[1], sys.argv[2]\nwith zipfile.ZipFile(out, 'w') as zf:\n    for dirpath, _, names in os.walk(root):\n        for f in names:\n            p = os.path.join(dirpath, f)\n            zf.write(p, os.path.relpath(p, root))\n";
        let out = std::process::Command::new("python3")
            .arg("-c")
            .arg(snippet)
            .arg(&root_str)
            .arg(&out_str)
            .stdin(std::process::Stdio::null())
            .output()
            .expect("python3 failed");
        assert!(out.status.success(), "zip dir maker failed");
        let zip = std::fs::read(&out_path).unwrap();
        let _ = std::fs::remove_dir_all(&dir);
        let _ = std::fs::remove_file(&out_path);
        zip
    }

    /// Register a managed-family function the way `CreateFunction` would
    /// (declared runtime + handler, own code zip).
    fn make_managed_function(
        state: &LambdaState,
        account: u64,
        region: &str,
        name: &str,
        runtime: &str,
        handler: &str,
        code_zip: Vec<u8>,
    ) {
        let func = crate::models::LambdaFunction::new(account, region, name.to_string());
        *func.code_zip.write() = code_zip;
        *func.runtime.write() = runtime.into();
        *func.handler.write() = handler.into();
        state.functions.write().insert(
            name.to_string(),
            crate::models::FunctionEntry {
                latest: Some(Arc::new(func)),
                versions: Vec::new(),
            },
        );
    }

    #[test]
    fn invoke_by_arn_runs_ruby_function() {
        if toolchain_missing("ruby") {
            return;
        }
        let account = 454545454545u64;
        let region = "bridge-ruby";
        let state = LambdaState::new();
        let code = "def handler(event:, context:)\n  { ok: true, n: event['n'] }\nend\n";
        make_managed_function(
            &state,
            account,
            region,
            "bridge-ruby-fn",
            "ruby3.2",
            "lambda_function.handler",
            make_staged_zip(&[("lambda_function.rb", code.as_bytes())]),
        );
        register_state(account, region, &state);

        let out = invoke_by_arn(
            "arn:aws:lambda:bridge-ruby:454545454545:function:bridge-ruby-fn",
            &json!({"n": 7}),
        )
        .expect("ruby function registered");
        assert!(out.function_error.is_none(), "outcome: {}", out.payload);
        let payload: Value = serde_json::from_str(&out.payload).unwrap();
        assert_eq!(payload["ok"], json!(true));
        assert_eq!(payload["n"], json!(7));
    }

    #[test]
    fn invoke_by_arn_runs_java_function() {
        if toolchain_missing("javac") || toolchain_missing("java") {
            return;
        }
        let account = 464646464646u64;
        let region = "bridge-java";
        let state = LambdaState::new();
        let source = "public class Handler {\n    public String handleRequest(String event, Object context) {\n        return event;\n    }\n}\n";
        let dir =
            std::env::temp_dir().join(format!("lam-bridge-java-{}", uuid::Uuid::new_v4().simple()));
        std::fs::create_dir_all(&dir).unwrap();
        std::fs::write(dir.join("Handler.java"), source).unwrap();
        let compiled = std::process::Command::new("javac")
            .arg("-d")
            .arg(&dir)
            .arg(dir.join("Handler.java"))
            .stdin(std::process::Stdio::null())
            .output()
            .expect("javac spawn failed");
        assert!(
            compiled.status.success(),
            "javac failed: {}",
            String::from_utf8_lossy(&compiled.stderr)
        );
        let zip = make_staged_zip(&[(
            "Handler.class",
            &std::fs::read(dir.join("Handler.class")).unwrap(),
        )]);
        let _ = std::fs::remove_dir_all(&dir);
        make_managed_function(
            &state,
            account,
            region,
            "bridge-java-fn",
            "java21",
            "Handler::handleRequest",
            zip,
        );
        register_state(account, region, &state);

        let out = invoke_by_arn(
            "arn:aws:lambda:bridge-java:464646464646:function:bridge-java-fn",
            &json!({"n": 9}),
        )
        .expect("java function registered");
        assert!(out.function_error.is_none(), "outcome: {}", out.payload);
        let payload: Value = serde_json::from_str(&out.payload).unwrap();
        assert_eq!(payload["n"], json!(9));
    }

    #[test]
    fn invoke_by_arn_runs_dotnet_function() {
        if toolchain_missing("dotnet") {
            return;
        }
        let account = 474747474747u64;
        let region = "bridge-dotnet";
        let state = LambdaState::new();
        // Build the class library with the same host TFM the executor uses.
        let tfm = runtime::detect_dotnet_tfm();
        let dir = std::env::temp_dir()
            .join(format!("lam-bridge-dotnet-{}", uuid::Uuid::new_v4().simple()));
        std::fs::create_dir_all(&dir).unwrap();
        std::fs::write(dir.join("Function.cs"), "using System.Text.Json;\n\nnamespace MyLambda\n{\n    public class Function\n    {\n        public string Handler(string eventJson, object context)\n        {\n            return \"{\\\"ok\\\": true, \\\"n\\\": 3}\";\n        }\n    }\n}\n").unwrap();
        std::fs::write(
            dir.join("MyLambda.csproj"),
            format!(
                "<Project Sdk=\"Microsoft.NET.Sdk\">\n  <PropertyGroup>\n    <TargetFramework>{tfm}</TargetFramework>\n    <ImplicitUsings>enable</ImplicitUsings>\n    <AssemblyName>MyLambda</AssemblyName>\n  </PropertyGroup>\n</Project>\n"
            ),
        )
        .unwrap();
        let out_dir = dir.join("out");
        let mut builder = std::process::Command::new("dotnet");
        builder
            .arg("build")
            .arg(dir.join("MyLambda.csproj"))
            .arg("-c")
            .arg("Release")
            .arg("-o")
            .arg(&out_dir)
            .arg("--nologo")
            .arg("-v")
            .arg("quiet")
            .env("DOTNET_CLI_TELEMETRY_OPTOUT", "1")
            .env("DOTNET_NOLOGO", "1")
            .env("DOTNET_SKIP_FIRST_TIME_EXPERIENCE", "1")
            .env("DOTNET_SYSTEM_GLOBALIZATION_INVARIANT", "1");
        let built = builder.output().expect("dotnet build failed");
        assert!(
            built.status.success() && out_dir.join("MyLambda.dll").is_file(),
            "dotnet build failed: {}",
            String::from_utf8_lossy(&built.stderr)
        );
        let zip = make_staged_zip(&[(
            "MyLambda.dll",
            &std::fs::read(out_dir.join("MyLambda.dll")).unwrap(),
        )]);
        let _ = std::fs::remove_dir_all(&dir);
        make_managed_function(
            &state,
            account,
            region,
            "bridge-dotnet-fn",
            "dotnet8",
            "MyLambda::MyLambda.Function::Handler",
            zip,
        );
        register_state(account, region, &state);

        let out = invoke_by_arn(
            "arn:aws:lambda:bridge-dotnet:474747474747:function:bridge-dotnet-fn",
            &json!({}),
        )
        .expect("dotnet function registered");
        assert!(out.function_error.is_none(), "outcome: {}", out.payload);
        let payload: Value = serde_json::from_str(&out.payload).unwrap();
        assert_eq!(payload["ok"], json!(true));
        assert_eq!(payload["n"], json!(3));
    }
}
