//! Lambda operation handler (rest-json protocol).
//!
//! Operations are dispatched from HTTP method + path, mirroring the botocore
//! Lambda REST API table and the Python ground-truth provider
//! (`src/robotocore/services/lambda_/provider.py`). Response shapes follow the
//! moto-backed ground truth.

use base64::Engine;
use parking_lot::RwLock;
use serde_json::{json, Map, Value};
use sha2::{Digest, Sha256};
use std::collections::HashMap;
use std::sync::Arc;

use crate::models::{
    iso_now, layer_date_now, url_date_now, utc_date_now, LambdaAlias, LambdaFunction, LambdaState,
};
use crate::protocol::{function_not_found, path_segments, resolve_fn, AwsRequest, AwsResponse};
use crate::runtime;

pub struct LambdaHandler {
    state: RwLock<HashMap<(u64, String), LambdaState>>,
}

impl Default for LambdaHandler {
    fn default() -> Self {
        Self::new()
    }
}

impl LambdaHandler {
    pub fn new() -> Self {
        Self {
            state: RwLock::new(HashMap::new()),
        }
    }

    fn state_for(&self, account: u64, region: &str) -> LambdaState {
        let mut states = self.state.write();
        states
            .entry((account, region.to_string()))
            .or_default()
            .clone()
    }

    pub fn handle(&self, req: AwsRequest) -> AwsResponse {
        let state = self.state_for(req.account, &req.region);
        let op = resolve_operation(&req.method, &req.path);
        self.dispatch(state, op, req)
    }

    fn dispatch(&self, state: LambdaState, op: &str, req: AwsRequest) -> AwsResponse {
        match op {
            "CreateFunction" => self.create_function(&state, &req),
            "GetFunction" => self.get_function(&state, &req),
            "GetFunctionConfiguration" => self.get_function_configuration(&state, &req),
            "DeleteFunction" => self.delete_function(&state, &req),
            "ListFunctions" => self.list_functions(&state),
            "UpdateFunctionCode" => self.update_function_code(&state, &req),
            "UpdateFunctionConfiguration" => self.update_function_configuration(&state, &req),
            "Invoke" => self.invoke(&state, &req),
            "InvokeAsync" => self.invoke_async(&state, &req),
            "InvokeWithResponseStream" => self.invoke_with_response_stream(&state, &req),
            "GetAccountSettings" => self.get_account_settings(&state),
            "ListVersionsByFunction" => self.list_versions_by_function(&state, &req),
            "PublishVersion" => self.publish_version(&state, &req),
            "CreateAlias" => self.create_alias(&state, &req),
            "GetAlias" => self.get_alias(&state, &req),
            "UpdateAlias" => self.update_alias(&state, &req),
            "DeleteAlias" => self.delete_alias(&state, &req),
            "ListAliases" => self.list_aliases(&state, &req),
            "GetPolicy" => self.get_policy(&state, &req),
            "AddPermission" => self.add_permission(&state, &req),
            "RemovePermission" => self.remove_permission(&state, &req),
            "PutFunctionConcurrency" => self.put_function_concurrency(&state, &req),
            "GetFunctionConcurrency" => self.get_function_concurrency(&state, &req),
            "DeleteFunctionConcurrency" => self.delete_function_concurrency(&state, &req),
            "CreateEventSourceMapping" => self.create_event_source_mapping(&state, &req),
            "GetEventSourceMapping" => self.get_event_source_mapping(&state, &req),
            "UpdateEventSourceMapping" => self.update_event_source_mapping(&state, &req),
            "DeleteEventSourceMapping" => self.delete_event_source_mapping(&state, &req),
            "ListEventSourceMappings" => self.list_event_source_mappings(&state, &req),
            "ListTags" => self.list_tags(&state, &req),
            "TagResource" => self.tag_resource(&state, &req),
            "UntagResource" => self.untag_resource(&state, &req),
            "PublishLayerVersion" => self.publish_layer_version(&state, &req),
            "GetLayerVersion" => self.get_layer_version(&state, &req),
            "GetLayerVersionByArn" => self.get_layer_version_by_arn(&state, &req),
            "DeleteLayerVersion" => self.delete_layer_version(&state, &req),
            "ListLayerVersions" => self.list_layer_versions(&state, &req),
            "ListLayers" => self.list_layers(&state, &req),
            "AddLayerVersionPermission" => self.add_layer_version_permission(&state, &req),
            "GetLayerVersionPolicy" => self.get_layer_version_policy(&state, &req),
            "RemoveLayerVersionPermission" => self.remove_layer_version_permission(&state, &req),
            "CreateFunctionUrlConfig" => self.function_url_config(&state, &req, UrlVerb::Create),
            "GetFunctionUrlConfig" => self.function_url_config(&state, &req, UrlVerb::Get),
            "UpdateFunctionUrlConfig" => self.function_url_config(&state, &req, UrlVerb::Update),
            "DeleteFunctionUrlConfig" => self.function_url_config(&state, &req, UrlVerb::Delete),
            "ListFunctionUrlConfigs" => self.list_function_url_configs(&state, &req),
            "PutProvisionedConcurrencyConfig" => self.put_provisioned_concurrency(&state, &req),
            "GetProvisionedConcurrencyConfig" => self.get_provisioned_concurrency(&state, &req),
            "DeleteProvisionedConcurrencyConfig" => {
                self.delete_provisioned_concurrency(&state, &req)
            }
            "ListProvisionedConcurrencyConfigs" => {
                self.list_provisioned_concurrency_configs(&state, &req)
            }
            "PutFunctionEventInvokeConfig" => self.put_event_invoke_config(&state, &req),
            "UpdateFunctionEventInvokeConfig" => self.put_event_invoke_config(&state, &req),
            "GetFunctionEventInvokeConfig" => self.get_event_invoke_config(&state, &req),
            "DeleteFunctionEventInvokeConfig" => self.delete_event_invoke_config(&state, &req),
            "ListFunctionEventInvokeConfigs" => self.list_event_invoke_configs(&state, &req),
            "PutFunctionCodeSigningConfig" => self.put_function_code_signing_config(&state, &req),
            "GetFunctionCodeSigningConfig" => self.get_function_code_signing_config(&state, &req),
            "DeleteFunctionCodeSigningConfig" => {
                self.delete_function_code_signing_config(&state, &req)
            }
            "GetFunctionRecursionConfig" => self.get_function_recursion_config(&state, &req),
            "PutFunctionRecursionConfig" => self.put_function_recursion_config(&state, &req),
            "GetFunctionScalingConfig" => self.get_function_scaling_config(&state, &req),
            "PutFunctionScalingConfig" => self.put_function_scaling_config(&state, &req),
            "GetRuntimeManagementConfig" => self.get_runtime_management_config(&state, &req),
            "PutRuntimeManagementConfig" => self.put_runtime_management_config(&state, &req),
            "ListDurableExecutionsByFunction" => self.list_durable_executions(),
            "CreateCodeSigningConfig" => self.create_code_signing_config(&state, &req),
            "ListCodeSigningConfigs" => self.list_code_signing_configs(&state),
            "GetCodeSigningConfig" => self.get_code_signing_config(&state, &req),
            "UpdateCodeSigningConfig" => self.update_code_signing_config(&state, &req),
            "DeleteCodeSigningConfig" => self.delete_code_signing_config(&state, &req),
            "ListFunctionsByCodeSigningConfig" => {
                self.list_functions_by_code_signing_config(&state, &req)
            }
            _ => AwsResponse::error(
                400,
                "InvalidRequest",
                &format!("Unknown path: {}", req.path),
            ),
        }
    }
}

/// Resolve a Lambda REST operation from method + path (segments are
/// version-stripped and URL-decoded by `path_segments`).
fn resolve_operation(method: &str, path: &str) -> &'static str {
    let segs = path_segments(path);
    let m: &str = method;
    let s: Vec<&str> = segs.iter().map(String::as_str).collect();
    match s.first().copied().unwrap_or("") {
        "functions" => match s.len() {
            1 => match m {
                "GET" => "ListFunctions",
                "POST" => "CreateFunction",
                _ => "",
            },
            2 => match m {
                "GET" => "GetFunction",
                "DELETE" => "DeleteFunction",
                _ => "",
            },
            3 => match s[2] {
                "configuration" => match m {
                    "GET" => "GetFunctionConfiguration",
                    "PUT" => "UpdateFunctionConfiguration",
                    _ => "",
                },
                "code" => match m {
                    "PUT" => "UpdateFunctionCode",
                    _ => "",
                },
                "invocations" => "Invoke",
                "invoke-async" => "InvokeAsync",
                "versions" => match m {
                    "GET" => "ListVersionsByFunction",
                    "POST" => "PublishVersion",
                    _ => "",
                },
                "aliases" => match m {
                    "GET" => "ListAliases",
                    "POST" => "CreateAlias",
                    _ => "",
                },
                "policy" => match m {
                    "GET" => "GetPolicy",
                    "POST" => "AddPermission",
                    _ => "",
                },
                "concurrency" => match m {
                    "PUT" => "PutFunctionConcurrency",
                    "GET" => "GetFunctionConcurrency",
                    "DELETE" => "DeleteFunctionConcurrency",
                    _ => "",
                },
                "url" => match m {
                    "POST" => "CreateFunctionUrlConfig",
                    "GET" => "GetFunctionUrlConfig",
                    "PUT" => "UpdateFunctionUrlConfig",
                    "DELETE" => "DeleteFunctionUrlConfig",
                    _ => "",
                },
                "urls" => "ListFunctionUrlConfigs",
                "event-invoke-config" => match m {
                    "PUT" => "PutFunctionEventInvokeConfig",
                    "POST" => "UpdateFunctionEventInvokeConfig",
                    "GET" => "GetFunctionEventInvokeConfig",
                    "DELETE" => "DeleteFunctionEventInvokeConfig",
                    _ => "",
                },
                "provisioned-concurrency" => match m {
                    "PUT" => "PutProvisionedConcurrencyConfig",
                    "GET" => "GetProvisionedConcurrencyConfig",
                    "DELETE" => "DeleteProvisionedConcurrencyConfig",
                    _ => "",
                },
                "code-signing-config" => match m {
                    "GET" => "GetFunctionCodeSigningConfig",
                    "PUT" => "PutFunctionCodeSigningConfig",
                    "DELETE" => "DeleteFunctionCodeSigningConfig",
                    _ => "",
                },
                "recursion-config" => match m {
                    "GET" => "GetFunctionRecursionConfig",
                    "PUT" => "PutFunctionRecursionConfig",
                    _ => "",
                },
                "function-scaling-config" => match m {
                    "GET" => "GetFunctionScalingConfig",
                    "PUT" => "PutFunctionScalingConfig",
                    _ => "",
                },
                "runtime-management-config" => match m {
                    "GET" => "GetRuntimeManagementConfig",
                    "PUT" => "PutRuntimeManagementConfig",
                    _ => "",
                },
                "durable-executions" => "ListDurableExecutionsByFunction",
                "response-streaming-invocations" => "InvokeWithResponseStream",
                _ => "",
            },
            4 => {
                if s[2] == "aliases" {
                    match m {
                        "GET" => "GetAlias",
                        "PUT" => "UpdateAlias",
                        "DELETE" => "DeleteAlias",
                        _ => "",
                    }
                } else if s[2] == "policy" {
                    match m {
                        "DELETE" => "RemovePermission",
                        _ => "",
                    }
                } else if s[2] == "event-invoke-config" && s[3] == "list" {
                    match m {
                        "GET" => "ListFunctionEventInvokeConfigs",
                        _ => "",
                    }
                } else {
                    ""
                }
            }
            _ => "",
        },
        "layers" => match s.len() {
            1 => {
                if m == "GET" {
                    // Both ListLayers and GetLayerVersionByArn ride this path;
                    // the ?find=LayerVersion query disambiguates (checked inside).
                    "ListLayers"
                } else {
                    ""
                }
            }
            3 => match s[2] {
                "versions" => match m {
                    "GET" => "ListLayerVersions",
                    "POST" => "PublishLayerVersion",
                    _ => "",
                },
                _ => "",
            },
            4 => match m {
                "GET" => "GetLayerVersion",
                "DELETE" => "DeleteLayerVersion",
                _ => "",
            },
            5 => match s[4] {
                "policy" => match m {
                    "GET" => "GetLayerVersionPolicy",
                    "POST" => "AddLayerVersionPermission",
                    _ => "",
                },
                _ => "",
            },
            6 => match s[4] {
                "policy" => match m {
                    "DELETE" => "RemoveLayerVersionPermission",
                    _ => "",
                },
                _ => "",
            },
            _ => "",
        },
        "event-source-mappings" => match s.len() {
            1 => match m {
                "GET" => "ListEventSourceMappings",
                "POST" => "CreateEventSourceMapping",
                _ => "",
            },
            2 => match m {
                "GET" => "GetEventSourceMapping",
                "PUT" => "UpdateEventSourceMapping",
                "DELETE" => "DeleteEventSourceMapping",
                _ => "",
            },
            _ => "",
        },
        "tags" => match m {
            "GET" => "ListTags",
            "POST" => "TagResource",
            "DELETE" => "UntagResource",
            _ => "",
        },
        "account-settings" => {
            if m == "GET" {
                "GetAccountSettings"
            } else {
                ""
            }
        }
        "code-signing-configs" => match s.len() {
            1 => match m {
                "GET" => "ListCodeSigningConfigs",
                "POST" => "CreateCodeSigningConfig",
                _ => "",
            },
            _ => match m {
                "GET" => {
                    if matches!(s.last().copied(), Some("functions")) {
                        "ListFunctionsByCodeSigningConfig"
                    } else {
                        "GetCodeSigningConfig"
                    }
                }
                "PUT" => "UpdateCodeSigningConfig",
                "DELETE" => "DeleteCodeSigningConfig",
                _ => "",
            },
        },
        _ => "",
    }
}

/// Layer-version not-found error.
fn not_found_layer_version() -> AwsResponse {
    AwsResponse::error(
        404,
        "ResourceNotFoundException",
        "The resource you requested does not exist.",
    )
}

impl LambdaHandler {
    // ---------------------------------------------------------------
    // Configuration rendering (mirrors moto's get_configuration keys).
    // ---------------------------------------------------------------

    fn fn_config(f: &LambdaFunction) -> Value {
        let mut cfg = json!({
            "CodeSha256": *f.code_sha256.read(),
            "CodeSize": *f.code_size.read(),
            "Description": *f.description.read(),
            "FunctionArn": f.function_arn,
            "FunctionName": f.function_name,
            "Handler": *f.handler.read(),
            "LastModified": *f.last_modified.read(),
            "MemorySize": *f.memory_size.read(),
            "Role": *f.role.read(),
            "Runtime": *f.runtime.read(),
            "State": *f.state.read(),
            "PackageType": "Zip",
            "Timeout": *f.timeout.read(),
            "Version": *f.version.read(),
            "VpcConfig": Value::Null,
            "Layers": *f.layers.read(),
            "SigningProfileVersionArn": Value::Null,
            "SigningJobArn": Value::Null,
            "TracingConfig": {"Mode": "PassThrough"},
            "Architectures": ["x86_64"],
            "EphemeralStorage": {"Size": 512},
            "SnapStart": {"ApplyOn": "None", "OptimizationStatus": "Off"},
            "LastUpdateStatus": "Successful",
        });
        if let Some(vars) = f.environment.read().as_ref() {
            if !vars.is_empty() {
                cfg["Environment"] = json!({"Variables": vars});
            }
        }
        cfg
    }

    // ---------------------------------------------------------------
    // Functions
    // ---------------------------------------------------------------

    fn create_function(&self, state: &LambdaState, req: &AwsRequest) -> AwsResponse {
        let name = req.param_str("FunctionName").unwrap_or_default();
        if name.is_empty() {
            return AwsResponse::error(
                400,
                "InvalidParameterValueException",
                "Missing FunctionName",
            );
        }
        if state.get_function(&name).is_some() {
            return AwsResponse::error(
                409,
                "ResourceConflictException",
                &format!("Function already exist: {name}"),
            );
        }
        let func = Arc::new(LambdaFunction::new(req.account, &req.region, name.clone()));
        if let Some(rt) = req.param_str("Runtime") {
            *func.runtime.write() = rt;
        }
        if let Some(h) = req.param_str("Handler") {
            *func.handler.write() = h;
        }
        if let Some(r) = req.param_str("Role") {
            *func.role.write() = r;
        }
        if let Some(d) = req.param_str("Description") {
            *func.description.write() = d;
        }
        if let Some(t) = req.param_u64("Timeout") {
            *func.timeout.write() = t as u32;
        }
        if let Some(m) = req.param_u64("MemorySize") {
            *func.memory_size.write() = m as u32;
        }
        // Deployment package: ZipFile arrives base64-encoded by botocore.
        // Without an inline package, fall back to the S3 source (S3Bucket +
        // S3Key), reading the object straight from the S3 crate's store.
        let code = req.params.get("Code");
        let zip = match resolve_code_source(
            req,
            decode_zip(code.and_then(|c| c.get("ZipFile"))),
            code.and_then(|c| c.get("S3Bucket")),
            code.and_then(|c| c.get("S3Key")),
        ) {
            Ok(zip) => zip,
            Err(resp) => return resp,
        };
        *func.code_zip.write() = zip.clone();
        *func.code_size.write() = zip.len() as u64;
        *func.code_sha256.write() = sha_b64(&zip);
        if let Some(env) = req
            .params
            .get("Environment")
            .and_then(|e| e.get("Variables"))
            .and_then(Value::as_object)
        {
            let vars: HashMap<String, String> = env
                .iter()
                .filter_map(|(k, v)| v.as_str().map(|s| (k.clone(), s.to_string())))
                .collect();
            *func.environment.write() = Some(vars);
        }
        if let Some(layer_arns) = req.params.get("Layers").and_then(Value::as_array) {
            let mut layer_data = Vec::new();
            for l in layer_arns {
                let Some(arn) = l.as_str() else {
                    continue;
                };
                match state.layers_versions_by_arn(arn) {
                    Some(lv) => {
                        layer_data.push(json!({"Arn": arn, "CodeSize": lv.content_size}));
                    }
                    None => {
                        return AwsResponse::error(
                            404,
                            "ResourceNotFoundException",
                            &format!("One or more LayerVersion does not exist: [{arn}]"),
                        );
                    }
                }
            }
            *func.layers.write() = layer_data;
        }
        if let Some(tags) = req.params.get("Tags").and_then(Value::as_object) {
            let mut map = func.tags.write();
            for (k, v) in tags {
                map.insert(k.clone(), v.as_str().unwrap_or("").to_string());
            }
        }
        if let Some(rc) = req.param_i64("ReservedConcurrentExecutions") {
            *func.reserved_concurrency.write() = Some(rc);
        }
        state.functions.write().insert(
            name.clone(),
            crate::models::FunctionEntry {
                latest: Some(Arc::clone(&func)),
                versions: Vec::new(),
            },
        );
        AwsResponse::json(201, Self::fn_config(&func))
    }

    fn get_function(&self, state: &LambdaState, req: &AwsRequest) -> AwsResponse {
        let name = req.path_fn_name();
        let func = match resolve_fn(state, req, &name) {
            Ok(f) => f,
            Err(resp) => return resp,
        };
        let s3key = format!("{}-{}", func.function_name, uuid::Uuid::new_v4().simple());
        let mut body = json!({
            "Configuration": Self::fn_config(&func),
            "Code": {
                "Location": format!(
                    "https://awslambda-{}-tasks.s3.{}.amazonaws.com/{}",
                    req.region, req.region, s3key
                ),
                "RepositoryType": "S3",
            }
        });
        let tags = func.tags.read();
        if !tags.is_empty() {
            let obj = tags
                .iter()
                .map(|(k, v)| (k.clone(), Value::String(v.clone())))
                .collect::<Map<_, _>>();
            body["Tags"] = Value::Object(obj);
        }
        if let Some(rc) = *func.reserved_concurrency.read() {
            body["Concurrency"] = json!({"ReservedConcurrentExecutions": rc});
        }
        AwsResponse::json(200, body)
    }

    fn get_function_configuration(&self, state: &LambdaState, req: &AwsRequest) -> AwsResponse {
        let name = req.path_fn_name();
        let func = match resolve_fn(state, req, &name) {
            Ok(f) => f,
            Err(resp) => return resp,
        };
        AwsResponse::json(200, Self::fn_config(&func))
    }

    fn delete_function(&self, state: &LambdaState, req: &AwsRequest) -> AwsResponse {
        let name = req.path_fn_name();
        let qual = req.query_param("Qualifier");
        if state.functions.read().get(&name).is_none() {
            return function_not_found(&name, &req.region, req.account);
        }
        match qual.as_deref() {
            None => {
                state.functions.write().remove(&name);
                // Cascade native stores (matching the Python provider).
                let fn_arn = format!(
                    "arn:aws:lambda:{}:{}:function:{}",
                    req.region, req.account, name
                );
                state
                    .aliases
                    .write()
                    .retain(|(fn_name, _), _| fn_name != &name);
                state
                    .event_source_mappings
                    .write()
                    .retain(|_, em| em.function_arn != fn_arn);
                state.url_configs.write().remove(&name);
                state.url_configs.write().remove(&fn_arn);
                state
                    .provisioned
                    .write()
                    .retain(|(fn_name, _), _| fn_name != &name);
            }
            Some(_) => {
                // Deleting a published version only: drop it from the entry.
                let mut funcs = state.functions.write();
                if let Some(entry) = funcs.get_mut(&name) {
                    entry
                        .versions
                        .retain(|v| Some(v.version.read().as_str()) != qual.as_deref());
                }
            }
        }
        AwsResponse::empty(204)
    }

    fn list_functions(&self, state: &LambdaState) -> AwsResponse {
        let funcs = state.functions.read();
        let configs: Vec<Value> = funcs
            .values()
            .filter_map(|e| e.latest.as_ref())
            .map(|f| Self::fn_config(f))
            .collect();
        AwsResponse::json(200, json!({ "Functions": configs }))
    }

    fn update_function_code(&self, state: &LambdaState, req: &AwsRequest) -> AwsResponse {
        let name = req.path_fn_name();
        let func = match resolve_fn(state, req, &name) {
            Ok(f) => f,
            Err(resp) => return resp,
        };
        // UpdateFunctionCode carries ZipFile at the top level of the body,
        // or an S3 source (S3Bucket + S3Key) resolved from the S3 store.
        let zip = match resolve_code_source(
            req,
            decode_zip(req.params.get("ZipFile")),
            req.params.get("S3Bucket"),
            req.params.get("S3Key"),
        ) {
            Ok(zip) => zip,
            Err(resp) => return resp,
        };

        if !zip.is_empty() {
            *func.code_zip.write() = zip.clone();
            *func.code_size.write() = zip.len() as u64;
            *func.code_sha256.write() = sha_b64(&zip);
        }
        *func.last_modified.write() = iso_now();
        *func.code_or_config_updated.write() = true;
        AwsResponse::json(200, Self::fn_config(&func))
    }

    fn update_function_configuration(&self, state: &LambdaState, req: &AwsRequest) -> AwsResponse {
        let name = req.path_fn_name();
        let func = match resolve_fn(state, req, &name) {
            Ok(f) => f,
            Err(resp) => return resp,
        };
        if let Some(d) = req.param_str("Description") {
            *func.description.write() = d;
        }
        if let Some(h) = req.param_str("Handler") {
            *func.handler.write() = h;
        }
        if let Some(m) = req.param_u64("MemorySize") {
            *func.memory_size.write() = m as u32;
        }
        if let Some(r) = req.param_str("Role") {
            *func.role.write() = r;
        }
        if let Some(rt) = req.param_str("Runtime") {
            *func.runtime.write() = rt;
        }
        if let Some(t) = req.param_u64("Timeout") {
            *func.timeout.write() = t as u32;
        }
        if let Some(env) = req
            .params
            .get("Environment")
            .and_then(|e| e.get("Variables"))
            .and_then(Value::as_object)
        {
            let vars: HashMap<String, String> = env
                .iter()
                .filter_map(|(k, v)| v.as_str().map(|s| (k.clone(), s.to_string())))
                .collect();
            *func.environment.write() = Some(vars);
        }
        *func.last_modified.write() = iso_now();
        *func.code_or_config_updated.write() = true;
        AwsResponse::json(200, Self::fn_config(&func))
    }

    // ---------------------------------------------------------------
    // Invocation
    // ---------------------------------------------------------------

    /// Execute the resolved function; return the ExecResult or an error.
    fn execute(
        state: &LambdaState,
        req: &AwsRequest,
        func: &Arc<LambdaFunction>,
    ) -> Result<runtime::ExecResult, AwsResponse> {
        let event = String::from_utf8_lossy(&req.body).to_string();
        let invocation_type = req
            .header("x-amz-invocation-type")
            .unwrap_or("RequestResponse");
        if invocation_type == "DryRun" {
            return Ok(runtime::ExecResult {
                payload: String::new(),
                function_error: None,
                logs: String::new(),
            });
        }
        let zip_bytes = func.code_zip.read().clone();
        if zip_bytes.is_empty() {
            // No code deployed: moto's simple-mode response.
            let payload = json!("Simple Lambda happy path OK").to_string();
            return Ok(runtime::ExecResult {
                payload,
                function_error: None,
                logs: String::new(),
            });
        }
        let is_python: bool = crate::runtime::is_python_runtime(&func.runtime.read());
        if !is_python {
            // Non-python runtimes have no executor in this backend; mirror
            // the ground-truth shape: 200 response with function-error
            // header Runtime.InvalidRuntime and a null payload.
            return Ok(runtime::ExecResult {
                payload: "null".to_string(),
                function_error: Some("Runtime.InvalidRuntime".to_string()),
                logs: String::new(),
            });
        }
        // Collect layer zips for PYTHONPATH resolution.
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
        let env_vars = func.environment.read().clone().unwrap_or_default();
        Ok(runtime::execute_python(
            &zip_bytes,
            &layer_zips,
            &func.handler.read().clone(),
            &event,
            &func.function_name,
            &func.version.read().clone(),
            &func.function_arn.clone(),
            &env_vars,
            *func.timeout.read(),
            *func.memory_size.read(),
            &req.region.clone(),
            &req.account.to_string(),
        ))
    }

    fn invoke(&self, state: &LambdaState, req: &AwsRequest) -> AwsResponse {
        let name = req.path_fn_name();
        let func = match resolve_fn(state, req, &name) {
            Ok(f) => f,
            Err(resp) => return resp,
        };
        let res = match Self::execute(state, req, &func) {
            Ok(r) => r,
            Err(resp) => return resp,
        };
        let invocation_type = req
            .header("x-amz-invocation-type")
            .unwrap_or("RequestResponse");
        let executed_version = func.version.read().clone();
        match invocation_type {
            "Event" => AwsResponse::raw_with_headers(
                202,
                "application/json",
                String::new(),
                &[("x-amz-executed-version", executed_version.clone())],
            ),
            "DryRun" => AwsResponse::raw_with_headers(
                204,
                "application/json",
                String::new(),
                &[("x-amz-executed-version", executed_version)],
            ),
            _ => {
                let mut extra: Vec<(&str, String)> =
                    vec![("x-amz-executed-version", executed_version)];
                if let Some(err) = &res.function_error {
                    extra.push(("x-amz-function-error", err.clone()));
                }
                if req.header("x-amz-log-type").unwrap_or("None") == "Tail" && !res.logs.is_empty()
                {
                    let log_tail = tail_utf8(&res.logs, 4096);
                    let encoded = base64::engine::general_purpose::STANDARD.encode(log_tail);
                    extra.push(("x-amz-log-result", encoded));
                }
                AwsResponse::raw_with_headers(200, "application/json", res.payload.clone(), &extra)
            }
        }
    }

    fn invoke_async(&self, state: &LambdaState, req: &AwsRequest) -> AwsResponse {
        let name = req.path_fn_name();
        match resolve_fn(state, req, &name) {
            Ok(_) => AwsResponse::raw(202, "application/json", json!({"Status": 202}).to_string()),
            Err(resp) => resp,
        }
    }

    fn invoke_with_response_stream(&self, state: &LambdaState, req: &AwsRequest) -> AwsResponse {
        let name = req.path_fn_name();
        let func = match resolve_fn(state, req, &name) {
            Ok(f) => f,
            Err(resp) => return resp,
        };
        let res = match Self::execute(state, req, &func) {
            Ok(r) => r,
            Err(resp) => return resp,
        };
        let executed_version = func.version.read().clone();
        AwsResponse::raw_with_headers(
            200,
            "application/json",
            res.payload.clone(),
            &[("x-amz-executed-version", executed_version)],
        )
    }

    // ---------------------------------------------------------------
    // Versions and aliases
    // ---------------------------------------------------------------

    fn publish_version(&self, state: &LambdaState, req: &AwsRequest) -> AwsResponse {
        let name = req.path_fn_name();
        let mut funcs = state.functions.write();
        let Some(entry) = funcs.get_mut(&name) else {
            drop(funcs);
            return function_not_found(&name, &req.region, req.account);
        };
        let latest = match entry.latest.clone() {
            Some(l) => l,
            None => {
                drop(funcs);
                return function_not_found(&name, &req.region, req.account);
            }
        };
        // Idempotent republish when nothing changed since the last publish.
        if !entry.versions.is_empty() && !*latest.code_or_config_updated.read() {
            let last = entry.versions.last().cloned().unwrap();
            drop(funcs);
            return AwsResponse::json(201, Self::fn_config(&last));
        }
        *latest.code_or_config_updated.write() = false;
        let new_version = (entry.versions.len() + 1) as i64;
        let published = Arc::new(latest.with_version_set(new_version));
        if let Some(d) = req.param_str("Description") {
            if !d.is_empty() {
                *published.description.write() = d;
            }
        }
        entry.versions.push(Arc::clone(&published));
        drop(funcs);
        AwsResponse::json(201, Self::fn_config(&published))
    }

    fn list_versions_by_function(&self, state: &LambdaState, req: &AwsRequest) -> AwsResponse {
        let name = req.path_fn_name();
        let funcs = state.functions.read();
        let Some(entry) = funcs.get(&name) else {
            drop(funcs);
            return function_not_found(&name, &req.region, req.account);
        };
        let latest = match entry.latest.clone() {
            Some(l) => l,
            None => {
                drop(funcs);
                return function_not_found(&name, &req.region, req.account);
            }
        };
        let mut latest_cfg = Self::fn_config(&latest);
        latest_cfg["FunctionArn"] = json!(format!("{}:$LATEST", latest.function_arn));
        let mut all = vec![latest_cfg];
        all.extend(entry.versions.iter().map(|f| Self::fn_config(f)));
        AwsResponse::json(200, json!({ "Versions": all }))
    }

    fn alias_dict(a: &LambdaAlias) -> Value {
        json!({
            "AliasArn": a.function_arn,
            "Description": *a.description.read(),
            "FunctionVersion": *a.function_version.read(),
            "Name": a.name,
            "RevisionId": a.revision_id,
            "RoutingConfig": *a.routing_config.read(),
        })
    }

    fn create_alias(&self, state: &LambdaState, req: &AwsRequest) -> AwsResponse {
        let name = req.path_fn_name();
        let Some(alias_name) = req.param_str("Name") else {
            return AwsResponse::error(400, "InvalidParameterValueException", "Missing Name");
        };
        let version = req
            .param_str("FunctionVersion")
            .unwrap_or_else(|| "$LATEST".to_string());
        let func = match resolve_fn(state, req, &name) {
            Ok(f) => f,
            Err(resp) => return resp,
        };
        if state.resolve_alias(&name, &alias_name).is_some() {
            return AwsResponse::error(
                409,
                "ResourceConflictException",
                &format!("Alias already exists: {alias_name}"),
            );
        }
        let now = iso_now();
        let alias = Arc::new(LambdaAlias {
            name: alias_name.clone(),
            function_arn: format!("{}:{}", func.function_arn, alias_name),
            function_version: RwLock::new(version),
            description: RwLock::new(req.param_str("Description").unwrap_or_default()),
            modified: RwLock::new(now),
            routing_config: RwLock::new(req.params.get("RoutingConfig").cloned()),
            revision_id: uuid::Uuid::new_v4().to_string(),
        });
        state
            .aliases
            .write()
            .insert((name.clone(), alias_name.clone()), Arc::clone(&alias));
        AwsResponse::json(201, Self::alias_dict(&alias))
    }

    fn get_alias(&self, state: &LambdaState, req: &AwsRequest) -> AwsResponse {
        let (name, alias_name) = req.path_fn_alias();
        match state.resolve_alias(&name, &alias_name) {
            Some(a) => AwsResponse::json(200, Self::alias_dict(&a)),
            None => AwsResponse::error(
                404,
                "ResourceNotFoundException",
                &format!("Alias not found: {alias_name}"),
            ),
        }
    }

    fn update_alias(&self, state: &LambdaState, req: &AwsRequest) -> AwsResponse {
        let (name, alias_name) = req.path_fn_alias();
        match state.resolve_alias(&name, &alias_name) {
            Some(a) => {
                if let Some(v) = req.param_str("FunctionVersion") {
                    *a.function_version.write() = v;
                }
                if let Some(d) = req.param_str("Description") {
                    *a.description.write() = d;
                }
                if let Some(rc) = req.params.get("RoutingConfig") {
                    *a.routing_config.write() = Some(rc.clone());
                }
                *a.modified.write() = iso_now();
                AwsResponse::json(200, Self::alias_dict(&a))
            }
            None => AwsResponse::error(
                404,
                "ResourceNotFoundException",
                &format!("Alias not found: {alias_name}"),
            ),
        }
    }

    fn delete_alias(&self, state: &LambdaState, req: &AwsRequest) -> AwsResponse {
        let (name, alias_name) = req.path_fn_alias();
        if state.resolve_alias(&name, &alias_name).is_none() {
            return AwsResponse::error(
                404,
                "ResourceNotFoundException",
                &format!("Alias not found: {alias_name}"),
            );
        }
        state.aliases.write().remove(&(name, alias_name));
        AwsResponse::empty(204)
    }

    fn list_aliases(&self, state: &LambdaState, req: &AwsRequest) -> AwsResponse {
        let name = req.path_fn_name();
        let Some(fn_arn) = state.get_function(&name).map(|f| f.function_arn.clone()) else {
            return function_not_found(&name, &req.region, req.account);
        };
        let mut list: Vec<Value> = state
            .aliases
            .read()
            .values()
            .filter(|a| a.function_arn.starts_with(&fn_arn))
            .map(|a| Self::alias_dict(a))
            .collect();
        if let Some(fv) = req.query_param("FunctionVersion") {
            list.retain(|a| a.get("FunctionVersion").and_then(Value::as_str) == Some(fv.as_str()));
        }
        list.sort_by(|x, y| {
            let nx = x.get("Name").and_then(Value::as_str).unwrap_or("");
            let ny = y.get("Name").and_then(Value::as_str).unwrap_or("");
            nx.cmp(ny)
        });
        AwsResponse::json(200, json!({ "Aliases": list }))
    }

    // ---------------------------------------------------------------
    // Permissions
    // ---------------------------------------------------------------

    fn get_policy(&self, state: &LambdaState, req: &AwsRequest) -> AwsResponse {
        let name = req.path_fn_name();
        let func = match resolve_fn(state, req, &name) {
            Ok(f) => f,
            Err(resp) => return resp,
        };
        let stmts = func.policy.statements.read();
        if stmts.is_empty() {
            return AwsResponse::error(
                404,
                "ResourceNotFoundException",
                "No policy is associated with the given function.",
            );
        }
        let policy = json!({
            "Version": "2012-10-17",
            "Id": "default",
            "Statement": *stmts,
        });
        AwsResponse::json(
            200,
            json!({
                "Policy": serde_json::to_string(&policy).unwrap_or_default(),
                "RevisionId": *func.policy.revision.read(),
            }),
        )
    }

    fn add_permission(&self, state: &LambdaState, req: &AwsRequest) -> AwsResponse {
        let name = req.path_fn_name();
        let func = match resolve_fn(state, req, &name) {
            Ok(f) => f,
            Err(resp) => return resp,
        };
        let sid = req
            .param_str("StatementId")
            .unwrap_or_else(|| uuid::Uuid::new_v4().to_string());
        let principal = req
            .param_str("Principal")
            .unwrap_or_else(|| "*".to_string());
        let principal_value: Value = if principal == "*" {
            json!(principal)
        } else if principal.ends_with(".amazonaws.com") {
            json!({ "Service": principal })
        } else if principal.ends_with(":root") {
            json!({ "AWS": principal })
        } else {
            json!(principal)
        };
        let mut statement = json!({
            "Sid": sid,
            "Effect": "Allow",
            "Principal": principal_value,
            "Action": req.param_str("Action").unwrap_or_default(),
            "Resource": func.function_arn.clone(),
        });
        let mut condition = Map::new();
        if let Some(sa) = req.param_str("SourceArn") {
            condition.insert("ArnLike".into(), json!({"AWS:SourceArn": sa}));
        }
        if let Some(sc) = req.param_str("SourceAccount") {
            condition.insert("StringEquals".into(), json!({"AWS:SourceAccount": sc}));
        }
        if let Some(org) = req.param_str("PrincipalOrgID") {
            condition.insert("StringEquals".into(), json!({"aws:PrincipalOrgID": org}));
        }
        if !condition.is_empty() {
            statement["Condition"] = Value::Object(condition);
        }
        func.policy.statements.write().push(statement.clone());
        *func.policy.revision.write() = uuid::Uuid::new_v4().to_string();
        AwsResponse::json(
            201,
            json!({
                "Statement": serde_json::to_string(&statement).unwrap_or_default(),
            }),
        )
    }

    fn remove_permission(&self, state: &LambdaState, req: &AwsRequest) -> AwsResponse {
        let name = req.path_fn_name();
        let func = match resolve_fn(state, req, &name) {
            Ok(f) => f,
            Err(resp) => return resp,
        };
        let sid = req.path_statement_id();
        let mut stmts = func.policy.statements.write();
        let before = stmts.len();
        stmts.retain(|s| s.get("Sid").and_then(Value::as_str) != Some(sid.as_str()));
        if stmts.len() == before {
            return AwsResponse::error(
                404,
                "ResourceNotFoundException",
                &format!("Statement with statementId \"{sid}\" does not exist."),
            );
        }
        *func.policy.revision.write() = uuid::Uuid::new_v4().to_string();
        AwsResponse::empty(204)
    }

    // ---------------------------------------------------------------
    // Concurrency
    // ---------------------------------------------------------------

    fn put_function_concurrency(&self, state: &LambdaState, req: &AwsRequest) -> AwsResponse {
        let name = req.path_fn_name();
        let func = match resolve_fn(state, req, &name) {
            Ok(f) => f,
            Err(resp) => return resp,
        };
        let reserved = req.param_i64("ReservedConcurrentExecutions").unwrap_or(0);
        *func.reserved_concurrency.write() = Some(reserved);
        AwsResponse::json(200, json!({"ReservedConcurrentExecutions": reserved}))
    }

    fn get_function_concurrency(&self, state: &LambdaState, req: &AwsRequest) -> AwsResponse {
        let name = req.path_fn_name();
        let func = match resolve_fn(state, req, &name) {
            Ok(f) => f,
            Err(resp) => return resp,
        };
        let reserved_value = *func.reserved_concurrency.read();
        match reserved_value {
            Some(rc) => AwsResponse::json(200, json!({"ReservedConcurrentExecutions": rc})),
            None => AwsResponse::json(200, json!({})),
        }
    }

    fn delete_function_concurrency(&self, state: &LambdaState, req: &AwsRequest) -> AwsResponse {
        let name = req.path_fn_name();
        let func = match resolve_fn(state, req, &name) {
            Ok(f) => f,
            Err(resp) => return resp,
        };
        *func.reserved_concurrency.write() = None;
        AwsResponse::empty(204)
    }

    // ---------------------------------------------------------------
    // Event source mappings
    // ---------------------------------------------------------------

    fn esm_dict(e: &crate::models::EventSourceMapping) -> Value {
        // A deleted-sentinel: State is included as-is.
        json!({
            "UUID": e.uuid,
            "BatchSize": e.batch_size,
            "EventSourceArn": e.event_source_arn,
            "FunctionArn": e.function_arn,
            "MaximumBatchingWindowInSeconds": 0,
            "State": e.state,
            "StateTransitionReason": e.state_transition_reason,
            "LastModified": e.last_modified,
            "FunctionResponseTypes": [],
            "FilterCriteria": Value::Null,
            "BisectBatchOnFunctionError": false,
            "MaximumRetryAttempts": -1,
        })
    }

    fn create_event_source_mapping(&self, state: &LambdaState, req: &AwsRequest) -> AwsResponse {
        let fn_name = req.param_str("FunctionName").unwrap_or_default();
        let func = match resolve_fn(state, req, &fn_name) {
            Ok(f) => f,
            Err(resp) => return resp,
        };
        let uuid_val = uuid::Uuid::new_v4().to_string();
        let em = Arc::new(crate::models::EventSourceMapping {
            uuid: uuid_val.clone(),
            function_arn: func.function_arn.clone(),
            event_source_arn: req.param_str("EventSourceArn").unwrap_or_default(),
            state: "Enabled".to_string(),
            state_transition_reason: "User action".to_string(),
            last_modified: epoch_now(),
            batch_size: req.param_u64("BatchSize").unwrap_or(10) as u32,
            enabled: req
                .params
                .get("Enabled")
                .and_then(Value::as_bool)
                .unwrap_or(true),
        });
        state
            .event_source_mappings
            .write()
            .insert(uuid_val.clone(), Arc::clone(&em));
        AwsResponse::json(202, Self::esm_dict(&em))
    }

    fn get_event_source_mapping(&self, state: &LambdaState, req: &AwsRequest) -> AwsResponse {
        let uuid_val = uuid_of_path(req);
        let em = state.event_source_mappings.read().get(&uuid_val).cloned();
        match em {
            Some(e) => AwsResponse::json(200, Self::esm_dict(&e)),
            None => AwsResponse::error(
                404,
                "ResourceNotFoundException",
                &format!("Event source mapping not found: {uuid_val}"),
            ),
        }
    }

    fn update_event_source_mapping(&self, state: &LambdaState, req: &AwsRequest) -> AwsResponse {
        let uuid_val = uuid_of_path(req);
        let existing = state.event_source_mappings.read().get(&uuid_val).cloned();
        let Some(em) = existing else {
            return AwsResponse::error(
                404,
                "ResourceNotFoundException",
                &format!("Event source mapping not found: {uuid_val}"),
            );
        };
        let batch_size = req
            .param_u64("BatchSize")
            .map(|b| b as u32)
            .unwrap_or(em.batch_size);
        let (enabled, state_value) = match req.params.get("Enabled").and_then(Value::as_bool) {
            Some(on) => (on, if on { "Enabled" } else { "Disabled" }),
            None => (em.enabled, "Enabled"),
        };
        let em_state = if req.params.get("Enabled").and_then(Value::as_bool).is_some() {
            state_value.to_string()
        } else {
            em.state.clone()
        };
        let updated = Arc::new(crate::models::EventSourceMapping {
            uuid: em.uuid.clone(),
            function_arn: em.function_arn.clone(),
            event_source_arn: em.event_source_arn.clone(),
            state: em_state,
            state_transition_reason: em.state_transition_reason.clone(),
            last_modified: epoch_now(),
            batch_size,
            enabled,
        });
        state
            .event_source_mappings
            .write()
            .insert(uuid_val, Arc::clone(&updated));
        AwsResponse::json(200, Self::esm_dict(&updated))
    }

    fn delete_event_source_mapping(&self, state: &LambdaState, req: &AwsRequest) -> AwsResponse {
        let uuid_val = uuid_of_path(req);
        let removed = state.event_source_mappings.write().remove(&uuid_val);
        match removed {
            Some(e) => AwsResponse::json(200, Self::esm_dict(&e)),
            None => AwsResponse::error(
                404,
                "ResourceNotFoundException",
                &format!("Event source mapping not found: {uuid_val}"),
            ),
        }
    }

    fn list_event_source_mappings(&self, state: &LambdaState, req: &AwsRequest) -> AwsResponse {
        let event_source_arn = req.query_param("EventSourceArn");
        let fn_name = req.query_param("FunctionName");
        let ems: Vec<Value> = state
            .event_source_mappings
            .read()
            .values()
            .filter(|e| {
                event_source_arn
                    .as_ref()
                    .is_none_or(|arn| e.event_source_arn == *arn)
                    && fn_name
                        .as_ref()
                        .is_none_or(|fn_name| e.function_arn.contains(fn_name))
            })
            .map(|e| Self::esm_dict(e))
            .collect();
        AwsResponse::json(200, json!({ "EventSourceMappings": ems }))
    }

    // ---------------------------------------------------------------
    // Tags
    // ---------------------------------------------------------------

    fn list_tags(&self, state: &LambdaState, req: &AwsRequest) -> AwsResponse {
        let arn = req.tags_arn();
        let func = match resolve_fn(state, req, &arn) {
            Ok(f) => f,
            Err(resp) => return resp,
        };
        let tags = func
            .tags
            .read()
            .iter()
            .map(|(k, v)| (k.clone(), Value::String(v.clone())))
            .collect::<Map<_, _>>();
        AwsResponse::json(200, json!({ "Tags": tags }))
    }

    fn tag_resource(&self, state: &LambdaState, req: &AwsRequest) -> AwsResponse {
        let arn = req.tags_arn();
        let func = match resolve_fn(state, req, &arn) {
            Ok(f) => f,
            Err(resp) => return resp,
        };
        if let Some(tags) = req.params.get("Tags").and_then(Value::as_object) {
            let mut map = func.tags.write();
            for (k, v) in tags {
                map.insert(k.clone(), v.as_str().unwrap_or("").to_string());
            }
        }
        AwsResponse::empty(204)
    }

    fn untag_resource(&self, state: &LambdaState, req: &AwsRequest) -> AwsResponse {
        let arn = req.tags_arn();
        let func = match resolve_fn(state, req, &arn) {
            Ok(f) => f,
            Err(resp) => return resp,
        };
        let keys = req.query_list("tagKeys");
        func.tags.write().retain(|k, _| !keys.contains(k));
        AwsResponse::empty(204)
    }

    // ---------------------------------------------------------------
    // Layers
    // ---------------------------------------------------------------

    fn layer_version_dict(v: &crate::models::LayerVersion) -> Value {
        let mut out = json!({
            "Content": {
                "Location": "s3://",
                "CodeSha256": v.code_sha256,
                "CodeSize": v.content_size,
            },
            "Version": v.version,
            "LayerArn": v.layer_arn,
            "LayerVersionArn": format!("{}:{}", v.layer_arn, v.version),
            "CreatedDate": v.created,
            "LicenseInfo": v.license_info,
        });
        if !v.description.is_empty() {
            out["Description"] = json!(v.description);
        }
        if !v.compatible_architectures.is_empty() {
            out["CompatibleArchitectures"] = json!(v.compatible_architectures);
        }
        if !v.compatible_runtimes.is_empty() {
            out["CompatibleRuntimes"] = json!(v.compatible_runtimes);
        }
        out
    }

    fn publish_layer_version(&self, state: &LambdaState, req: &AwsRequest) -> AwsResponse {
        let layer_name = req.path_fn_uuid(); // /layers/{name}/versions
        let zip = decode_zip(req.params.get("Content").and_then(|c| c.get("ZipFile")));
        if zip.is_empty() {
            return AwsResponse::error(400, "InvalidParameterValueException", "Missing Content");
        }
        let mut layers = state.layers.write();
        let layer = layers
            .entry(layer_name.clone())
            .or_insert_with(|| crate::models::Layer {
                layer_arn: format!(
                    "arn:aws:lambda:{}:{}:layer:{}",
                    req.region, req.account, layer_name
                ),
                layer_name: layer_name.clone(),
                latest_version: 0,
                versions: Vec::new(),
            });
        let new_version = layer.latest_version + 1;
        let v = Arc::new(crate::models::LayerVersion {
            layer_arn: layer.layer_arn.clone(),
            layer_name: layer_name.clone(),
            version: new_version,
            description: req.param_str("Description").unwrap_or_default(),
            created: layer_date_now(),
            content_size: zip.len() as u64,
            code_sha256: sha_b64(&zip),
            compatible_runtimes: str_list(req.params.get("CompatibleRuntimes")),
            compatible_architectures: str_list(req.params.get("CompatibleArchitectures")),
            license_info: req.param_str("LicenseInfo").unwrap_or_default(),
            code_zip: zip,
            policy: Arc::new(crate::models::FunctionPolicy::new()),
        });
        layer.latest_version = new_version;
        layer.versions.push(Arc::clone(&v));
        AwsResponse::json(201, Self::layer_version_dict(&v))
    }

    fn get_layer_version(&self, state: &LambdaState, req: &AwsRequest) -> AwsResponse {
        let layer_name = req.path_fn_uuid();
        let version = req.path_layer_version().unwrap_or_default();
        if version == 0 {
            return not_found_layer_version();
        }
        let found = {
            let layers = state.layers.read();
            layers.get(&layer_name).and_then(|l| {
                l.versions
                    .iter()
                    .find(|v| i64::from(v.version) == version)
                    .cloned()
            })
        };
        match found {
            Some(v) => AwsResponse::json(200, Self::layer_version_dict(&v)),
            None => AwsResponse::error(
                404,
                "ResourceNotFoundException",
                &format!("Layer version {layer_name}:{version} not found"),
            ),
        }
    }

    fn get_layer_version_by_arn(&self, state: &LambdaState, req: &AwsRequest) -> AwsResponse {
        let arn = req.query_param("Arn").unwrap_or_default();
        match state.layers_versions_by_arn(&arn) {
            Some(v) => AwsResponse::json(200, Self::layer_version_dict(&v)),
            None => AwsResponse::error(
                404,
                "ResourceNotFoundException",
                &format!("Layer version not found: {arn}"),
            ),
        }
    }

    fn delete_layer_version(&self, state: &LambdaState, req: &AwsRequest) -> AwsResponse {
        let layer_name = req.path_fn_uuid();
        let version = req.path_layer_version().unwrap_or_default();
        let mut layers = state.layers.write();
        let Some(layer) = layers.get_mut(&layer_name) else {
            return AwsResponse::error(
                404,
                "ResourceNotFoundException",
                &format!("Layer {layer_name} not found"),
            );
        };
        layer.versions.retain(|v| i64::from(v.version) != version);
        AwsResponse::empty(204)
    }

    fn list_layer_versions(&self, state: &LambdaState, req: &AwsRequest) -> AwsResponse {
        let layer_name = req.path_fn_uuid();
        let layers = state.layers.read();
        let items: Vec<Value> = layers
            .get(&layer_name)
            .map(|l| {
                l.versions
                    .iter()
                    .map(|v| Self::layer_version_dict(v))
                    .collect()
            })
            .unwrap_or_default();
        AwsResponse::json(200, json!({ "LayerVersions": items }))
    }

    fn list_layers(&self, state: &LambdaState, req: &AwsRequest) -> AwsResponse {
        // GET /2018-10-31/layers?find=LayerVersion&Arn=... is
        // GetLayerVersionByArn sharing the same path as ListLayers.
        if req.query_param("find").as_deref() == Some("LayerVersion") {
            return self.get_layer_version_by_arn(state, req);
        }
        let layers = state.layers.read();
        let mut items: Vec<Value> = layers
            .values()
            .filter_map(|l| {
                l.versions.last().map(|latest| {
                    json!({
                        "LayerName": l.layer_name,
                        "LayerArn": l.layer_arn,
                        "LatestMatchingVersion": Self::layer_version_dict(latest),
                    })
                })
            })
            .filter(|item| {
                // CompatibleRuntime query filter
                req.query_param("CompatibleRuntime")
                    .map(|cr| {
                        item["LatestMatchingVersion"]["CompatibleRuntimes"]
                            .as_array()
                            .map(|a| a.iter().any(|r| r.as_str() == Some(cr.as_str())))
                            .unwrap_or(false)
                    })
                    .unwrap_or(true)
            })
            .collect();
        items.sort_by_key(|item| item["LayerName"].as_str().unwrap_or("").to_string());
        AwsResponse::json(200, json!({ "Layers": items }))
    }

    // ---------------------------------------------------------------
    // Layer version permissions
    // ---------------------------------------------------------------

    fn layer_version_of(&self, _state: &LambdaState, req: &AwsRequest) -> Option<(String, i64)> {
        let layer_name = req.path_fn_uuid();
        let version = req.path_layer_version()?;
        Some((layer_name, version))
    }

    fn add_layer_version_permission(&self, state: &LambdaState, req: &AwsRequest) -> AwsResponse {
        let Some((layer_name, version)) = self.layer_version_of(state, req) else {
            return not_found_layer_version();
        };
        let exists = state
            .layers
            .read()
            .get(&layer_name)
            .map(|l| l.versions.iter().any(|v| i64::from(v.version) == version))
            .unwrap_or(false);
        if !exists {
            return AwsResponse::error(
                404,
                "ResourceNotFoundException",
                &format!("Layer version {layer_name}:{version} not found"),
            );
        }
        let region = &req.region;
        let account = req.account;
        let sid = req
            .param_str("StatementId")
            .unwrap_or_else(|| uuid::Uuid::new_v4().to_string());
        let statement = json!({
            "Sid": sid,
            "Effect": "Allow",
            "Principal": req.param_str("Principal").unwrap_or_else(|| "*".to_string()),
            "Action": req.param_str("Action").unwrap_or_else(|| "lambda:GetLayerVersion".to_string()),
            "Resource": format!("arn:aws:lambda:{region}:{account}:layer:{layer_name}:{version}"),
        });
        let mut permissions = state.layer_permissions.write();
        let entry = permissions
            .entry((layer_name.clone(), version))
            .or_default();
        entry.insert(sid, statement.clone());
        let policy = json!({
            "Version": "2012-10-17",
            "Id": "default",
            "Statement": entry.values().cloned().collect::<Vec<_>>(),
        });
        AwsResponse::json(
            201,
            json!({
                "Statement": serde_json::to_string(&statement).unwrap_or_default(),
                "RevisionId": uuid::Uuid::new_v4().to_string(),
                "Policy": serde_json::to_string(&policy).unwrap_or_default(),
            }),
        )
    }

    fn get_layer_version_policy(&self, state: &LambdaState, req: &AwsRequest) -> AwsResponse {
        let Some((layer_name, version)) = self.layer_version_of(state, req) else {
            return not_found_layer_version();
        };
        let permissions = state.layer_permissions.read();
        match permissions.get(&(layer_name.clone(), version)) {
            Some(entry) if !entry.is_empty() => {
                let policy = json!({
                    "Version": "2012-10-17",
                    "Id": "default",
                    "Statement": entry.values().cloned().collect::<Vec<_>>(),
                });
                AwsResponse::json(200, json!({
                    "Policy": serde_json::to_string(&policy).unwrap_or_default(),
                    "RevisionId": uuid::Uuid::new_v4().to_string(),
                }))
            }
            _ => AwsResponse::error(
                404,
                "ResourceNotFoundException",
                &format!(
                    "The resource you requested does not exist: layer version {layer_name}:{version} has no policy"
                ),
            ),
        }
    }

    fn remove_layer_version_permission(
        &self,
        state: &LambdaState,
        req: &AwsRequest,
    ) -> AwsResponse {
        let Some((layer_name, version)) = self.layer_version_of(state, req) else {
            return not_found_layer_version();
        };
        let sid = req.path_statement_id();
        let mut permissions = state.layer_permissions.write();
        let Some(entry) = permissions.get_mut(&(layer_name.clone(), version)) else {
            return AwsResponse::error(
                404,
                "ResourceNotFoundException",
                &format!("Unknown statement: {sid}"),
            );
        };
        if entry.remove(&sid).is_none() {
            return AwsResponse::error(
                404,
                "ResourceNotFoundException",
                &format!("Unknown statement: {sid}"),
            );
        }
        if entry.is_empty() {
            permissions.remove(&(layer_name, version));
        }
        AwsResponse::empty(204)
    }

    // ---------------------------------------------------------------
    // Function URL configs
    // ---------------------------------------------------------------

    fn function_url_response(v: &Value) -> Value {
        v.clone()
    }

    fn function_url_config(
        &self,
        state: &LambdaState,
        req: &AwsRequest,
        verb: UrlVerb,
    ) -> AwsResponse {
        let name = req.path_fn_name();
        let func = match resolve_fn(state, req, &name) {
            Ok(f) => f,
            Err(resp) => return resp,
        };
        match verb {
            UrlVerb::Create => {
                let mut urls = state.url_configs.write();
                if urls.contains_key(&func.function_name) {
                    return AwsResponse::error(
                        409,
                        "ResourceConflictException",
                        &format!(
                            "Failed to create function url config for the function {}. \
                             Error: FunctionUrlConfig exists for this Lambda function",
                            func.function_name
                        ),
                    );
                }
                let url_id = uuid::Uuid::new_v4().simple().to_string();
                let url_id = &url_id[..12];
                let now = url_date_now();
                let config = json!({
                    "FunctionUrl": format!("https://{url_id}.lambda-url.{}.on.aws/", req.region),
                    "FunctionArn": func.function_arn,
                    "AuthType": req.param_str("AuthType").unwrap_or_else(|| "NONE".to_string()),
                    "Cors": req.params.get("Cors").cloned().unwrap_or_else(|| json!({})),
                    "CreationTime": now,
                    "LastModifiedTime": now,
                    "InvokeMode": req.param_str("InvokeMode").unwrap_or_else(|| "BUFFERED".to_string()),
                });
                urls.insert(func.function_name.clone(), config.clone());
                AwsResponse::json(201, Self::function_url_response(&config))
            }
            UrlVerb::Get => match state.url_configs.read().get(&func.function_name).cloned() {
                Some(config) => AwsResponse::json(200, Self::function_url_response(&config)),
                None => AwsResponse::error(
                    404,
                    "ResourceNotFoundException",
                    &format!(
                        "The function url config for function {} does not exist",
                        func.function_name
                    ),
                ),
            },
            UrlVerb::Update => {
                let mut urls = state.url_configs.write();
                match urls.get_mut(&func.function_name) {
                    Some(config) => {
                        if let Some(a) = req.param_str("AuthType") {
                            config["AuthType"] = json!(a);
                        }
                        if let Some(cors) = req.params.get("Cors") {
                            config["Cors"] = cors.clone();
                        }
                        if let Some(mode) = req.param_str("InvokeMode") {
                            config["InvokeMode"] = json!(mode);
                        }
                        config["LastModifiedTime"] = json!(url_date_now());
                        AwsResponse::json(200, Self::function_url_response(config))
                    }
                    None => AwsResponse::error(
                        404,
                        "ResourceNotFoundException",
                        &format!(
                            "The function url config for function {} does not exist",
                            func.function_name
                        ),
                    ),
                }
            }
            UrlVerb::Delete => {
                let mut urls = state.url_configs.write();
                if urls.remove(&func.function_name).is_none() {
                    return AwsResponse::error(
                        404,
                        "ResourceNotFoundException",
                        &format!(
                            "The function url config for function {} does not exist",
                            func.function_name
                        ),
                    );
                }
                AwsResponse::empty(204)
            }
        }
    }

    fn list_function_url_configs(&self, state: &LambdaState, req: &AwsRequest) -> AwsResponse {
        let name = req.path_fn_name();
        let func = match resolve_fn(state, req, &name) {
            Ok(f) => f,
            Err(resp) => return resp,
        };
        let configs = state
            .url_configs
            .read()
            .get(&func.function_name)
            .map(|c| vec![c.clone()])
            .unwrap_or_default();
        AwsResponse::json(200, json!({ "FunctionUrlConfigs": configs }))
    }

    // ---------------------------------------------------------------
    // Provisioned concurrency
    // ---------------------------------------------------------------

    fn put_provisioned_concurrency(&self, state: &LambdaState, req: &AwsRequest) -> AwsResponse {
        let name = req.path_fn_name();
        let func = match resolve_fn(state, req, &name) {
            Ok(f) => f,
            Err(resp) => return resp,
        };
        let qualifier = req
            .query_param("Qualifier")
            .unwrap_or_else(|| "$LATEST".to_string());
        let count = req
            .params
            .get("ProvisionedConcurrentExecutions")
            .and_then(Value::as_i64)
            .unwrap_or(0);
        let config = json!({
            "RequestedProvisionedConcurrentExecutions": count,
            "AvailableProvisionedConcurrentExecutions": count,
            "AllocatedProvisionedConcurrentExecutions": count,
            "Status": "READY",
            "LastModified": utc_date_now(),
            "FunctionArn": func.function_arn,
        });
        state
            .provisioned
            .write()
            .insert((func.function_name.clone(), qualifier), config);
        let requested = json!({
            "FunctionArn": func.function_arn,
            "RequestedProvisionedConcurrentExecutions": count,
            "ProvisionedConcurrentExecutions": count,
            "TargetProvisionedConcurrentExecutions": count,
            "State": "Ready",
            "LastModified": utc_date_now(),
        });
        AwsResponse::json(202, requested)
    }

    fn get_provisioned_concurrency(&self, state: &LambdaState, req: &AwsRequest) -> AwsResponse {
        // GET with ?List=ALL is ListProvisionedConcurrencyConfigs (same path).
        if req.query_param("List").as_deref() == Some("ALL") {
            return self.list_provisioned_concurrency_configs(state, req);
        }
        let name = req.path_fn_name();
        let func = match resolve_fn(state, req, &name) {
            Ok(f) => f,
            Err(resp) => return resp,
        };
        let qualifier = req
            .query_param("Qualifier")
            .unwrap_or_else(|| "$LATEST".to_string());
        match state
            .provisioned
            .read()
            .get(&(func.function_name.clone(), qualifier.clone()))
            .cloned()
        {
            Some(config) => AwsResponse::json(200, config),
            None => AwsResponse::error(
                404,
                "ProvisionedConcurrencyConfigNotFoundException",
                &format!(
                    "There is no provisioned concurrency configuration for the given qualifier on function {name} with qualifier \"{qualifier}\"."
                ),
            ),
        }
    }

    fn delete_provisioned_concurrency(&self, state: &LambdaState, req: &AwsRequest) -> AwsResponse {
        let name = req.path_fn_name();
        let func = match resolve_fn(state, req, &name) {
            Ok(f) => f,
            Err(resp) => return resp,
        };
        let qualifier = req.query_param("Qualifier").unwrap_or_default();
        if state
            .provisioned
            .write()
            .remove(&(func.function_name.clone(), qualifier.clone()))
            .is_none()
        {
            return AwsResponse::error(
                404,
                "ProvisionedConcurrencyConfigNotFoundException",
                &format!("There is no provisioned concurrency config for {name}:{qualifier}"),
            );
        }
        AwsResponse::empty(204)
    }

    fn list_provisioned_concurrency_configs(
        &self,
        state: &LambdaState,
        req: &AwsRequest,
    ) -> AwsResponse {
        let name = req.path_fn_name();
        let func = match resolve_fn(state, req, &name) {
            Ok(f) => f,
            Err(resp) => return resp,
        };
        let configs: Vec<Value> = state
            .provisioned
            .read()
            .iter()
            .filter(|(fn_key, _)| fn_key.0.as_str() == func.function_name)
            .map(|(_, v)| v.clone())
            .collect();
        AwsResponse::json(200, json!({ "ProvisionedConcurrencyConfigs": configs }))
    }

    // ---------------------------------------------------------------
    // Event invoke config
    // ---------------------------------------------------------------

    fn put_event_invoke_config(&self, state: &LambdaState, req: &AwsRequest) -> AwsResponse {
        let name = req.path_fn_name();
        let func = match resolve_fn(state, req, &name) {
            Ok(f) => f,
            Err(resp) => return resp,
        };
        let mut config = serde_json::Map::new();
        for key in [
            "MaximumRetryAttempts",
            "MaximumEventAgeInSeconds",
            "DestinationConfig",
        ] {
            if let Some(v) = req.params.get(key) {
                config.insert(key.to_string(), v.clone());
            }
        }
        if let Some(mra) = config.get("MaximumRetryAttempts").and_then(Value::as_i64) {
            if !(0..=2).contains(&mra) {
                return AwsResponse::error(
                    400,
                    "ValidationException",
                    &format!("MaximumRetryAttempts {mra} must be between 0 and 2"),
                );
            }
        }
        if let Some(age) = config
            .get("MaximumEventAgeInSeconds")
            .and_then(Value::as_i64)
        {
            if age > 21600 {
                return AwsResponse::error(
                    400,
                    "ValidationException",
                    &format!("MaximumEventAgeInSeconds {age} must be less than or equal to 21600"),
                );
            }
        }
        let stored: Value = config.into();
        *func.event_invoke_config.write() = Some(stored.clone());
        let mut body = json!({
            "FunctionArn": func.function_arn,
            "LastModified": *func.last_modified.read(),
        });
        if let Some(Value::Object(map)) = Some(stored) {
            for (k, v) in map {
                body[k.as_str()] = v;
            }
        }
        AwsResponse::json(200, body)
    }

    fn get_event_invoke_config(&self, state: &LambdaState, req: &AwsRequest) -> AwsResponse {
        let name = req.path_fn_name();
        let func = match resolve_fn(state, req, &name) {
            Ok(f) => f,
            Err(resp) => return resp,
        };
        let config_stored = func.event_invoke_config.read().clone();
        match config_stored {
            Some(config) => {
                let mut body = json!({
                    "FunctionArn": func.function_arn,
                    "LastModified": *func.last_modified.read(),
                });
                if let Value::Object(map) = config {
                    for (k, v) in map {
                        body[k.as_str()] = v;
                    }
                }
                AwsResponse::json(200, body)
            }
            None => AwsResponse::error(
                404,
                "ResourceNotFoundException",
                &format!(
                    "The event invoke config for the function {} does not exist.",
                    func.function_arn
                ),
            ),
        }
    }

    fn delete_event_invoke_config(&self, state: &LambdaState, req: &AwsRequest) -> AwsResponse {
        let name = req.path_fn_name();
        let func = match resolve_fn(state, req, &name) {
            Ok(f) => f,
            Err(resp) => return resp,
        };
        if func.event_invoke_config.read().is_none() {
            return AwsResponse::error(
                404,
                "ResourceNotFoundException",
                &format!(
                    "The event invoke config for the function {} does not exist.",
                    func.function_arn
                ),
            );
        }
        *func.event_invoke_config.write() = None;
        AwsResponse::empty(204)
    }

    fn list_event_invoke_configs(&self, state: &LambdaState, req: &AwsRequest) -> AwsResponse {
        let name = req.path_fn_name();
        let func = match resolve_fn(state, req, &name) {
            Ok(f) => f,
            Err(resp) => return resp,
        };
        let configs = match func.event_invoke_config.read().clone() {
            Some(config) => {
                let mut body = json!({
                    "FunctionArn": func.function_arn,
                    "LastModified": *func.last_modified.read(),
                });
                if let Value::Object(map) = config {
                    for (k, v) in map {
                        body[k.as_str()] = v;
                    }
                }
                vec![body]
            }
            None => Vec::new(),
        };
        AwsResponse::json(200, json!({ "FunctionEventInvokeConfigs": configs }))
    }

    // ---------------------------------------------------------------
    // Function code signing config
    // ---------------------------------------------------------------

    fn put_function_code_signing_config(
        &self,
        state: &LambdaState,
        req: &AwsRequest,
    ) -> AwsResponse {
        let name = req.path_fn_name();
        let func = match resolve_fn(state, req, &name) {
            Ok(f) => f,
            Err(resp) => return resp,
        };
        let csc_arn = req.param_str("CodeSigningConfigArn").unwrap_or_default();
        *func.code_signing_config_arn.write() = Some(csc_arn.clone());
        AwsResponse::json(
            200,
            json!({"CodeSigningConfigArn": csc_arn, "FunctionName": func.function_name}),
        )
    }

    fn get_function_code_signing_config(
        &self,
        state: &LambdaState,
        req: &AwsRequest,
    ) -> AwsResponse {
        let name = req.path_fn_name();
        let func = match resolve_fn(state, req, &name) {
            Ok(f) => f,
            Err(resp) => return resp,
        };
        let csc_arn_opt = func.code_signing_config_arn.read().clone();
        match csc_arn_opt {
            Some(arn) => AwsResponse::json(
                200,
                json!({"CodeSigningConfigArn": arn, "FunctionName": func.function_name}),
            ),
            None => AwsResponse::error(
                404,
                "ResourceNotFoundException",
                &format!(
                    "Code signing configuration not found for function: {}",
                    func.function_arn
                ),
            ),
        }
    }

    fn delete_function_code_signing_config(
        &self,
        state: &LambdaState,
        req: &AwsRequest,
    ) -> AwsResponse {
        let name = req.path_fn_name();
        let func = match resolve_fn(state, req, &name) {
            Ok(f) => f,
            Err(resp) => return resp,
        };
        *func.code_signing_config_arn.write() = None;
        AwsResponse::empty(204)
    }

    // ---------------------------------------------------------------
    // Recursion / scaling / runtime management
    // ---------------------------------------------------------------

    fn get_function_recursion_config(&self, state: &LambdaState, req: &AwsRequest) -> AwsResponse {
        let name = req.path_fn_name();
        let func = match resolve_fn(state, req, &name) {
            Ok(f) => f,
            Err(resp) => return resp,
        };
        AwsResponse::json(200, json!({"RecursiveLoop": *func.recursion_config.read()}))
    }

    fn put_function_recursion_config(&self, state: &LambdaState, req: &AwsRequest) -> AwsResponse {
        let name = req.path_fn_name();
        let func = match resolve_fn(state, req, &name) {
            Ok(f) => f,
            Err(resp) => return resp,
        };
        let allowed = ["Allow", "Terminate"];
        let value = req
            .param_str("RecursiveLoop")
            .unwrap_or_else(|| "Terminate".to_string());
        if !allowed.contains(&value.as_str()) {
            return AwsResponse::error(
                400,
                "InvalidParameterValueException",
                &format!("Invalid value for RecursiveLoop: {value}. Must be Allow or Terminate."),
            );
        }
        *func.recursion_config.write() = value.clone();
        AwsResponse::json(200, json!({"RecursiveLoop": value}))
    }

    fn get_function_scaling_config(&self, state: &LambdaState, req: &AwsRequest) -> AwsResponse {
        let name = req.path_fn_name();
        let func = match resolve_fn(state, req, &name) {
            Ok(f) => f,
            Err(resp) => return resp,
        };
        let qualifier = req
            .query_param("Qualifier")
            .unwrap_or_else(|| "$LATEST".to_string());
        let config = func
            .scaling_configs
            .read()
            .get(&qualifier)
            .cloned()
            .unwrap_or_else(|| json!({}));
        AwsResponse::json(
            200,
            json!({
                "FunctionArn": func.function_arn,
                "AppliedFunctionScalingConfig": config.clone(),
                "RequestedFunctionScalingConfig": config,
            }),
        )
    }

    fn put_function_scaling_config(&self, state: &LambdaState, req: &AwsRequest) -> AwsResponse {
        let name = req.path_fn_name();
        let func = match resolve_fn(state, req, &name) {
            Ok(f) => f,
            Err(resp) => return resp,
        };
        let qualifier = req
            .query_param("Qualifier")
            .unwrap_or_else(|| "$LATEST".to_string());
        let config = req
            .params
            .get("FunctionScalingConfig")
            .cloned()
            .unwrap_or_else(|| json!({}));
        func.scaling_configs
            .write()
            .insert(qualifier, config.clone());
        AwsResponse::json(
            200,
            json!({
                "FunctionArn": func.function_arn,
                "FunctionState": "Active",
                "RequestedFunctionScalingConfig": config,
            }),
        )
    }

    fn get_runtime_management_config(&self, state: &LambdaState, req: &AwsRequest) -> AwsResponse {
        let name = req.path_fn_name();
        let func = match resolve_fn(state, req, &name) {
            Ok(f) => f,
            Err(resp) => return resp,
        };
        let config = func
            .runtime_mgmt_config
            .read()
            .clone()
            .unwrap_or_else(|| json!({"UpdateRuntimeOn": "Auto"}));
        let mut body = config.clone();
        if body.get("UpdateRuntimeOn").is_none() {
            body["UpdateRuntimeOn"] = json!("Auto");
        }
        body["FunctionArn"] = json!(func.function_arn);
        AwsResponse::json(200, body)
    }

    fn put_runtime_management_config(&self, state: &LambdaState, req: &AwsRequest) -> AwsResponse {
        let name = req.path_fn_name();
        let func = match resolve_fn(state, req, &name) {
            Ok(f) => f,
            Err(resp) => return resp,
        };
        let update_on = req
            .param_str("UpdateRuntimeOn")
            .unwrap_or_else(|| "Auto".to_string());
        let mut config = json!({"UpdateRuntimeOn": update_on});
        if config["UpdateRuntimeOn"] == json!("Manual") {
            if let Some(arn) = req.param_str("RuntimeVersionArn") {
                config["RuntimeVersionArn"] = json!(arn);
            }
        }
        *func.runtime_mgmt_config.write() = Some(config.clone());
        config["FunctionArn"] = json!(func.function_arn);
        AwsResponse::json(200, config)
    }

    // ---------------------------------------------------------------
    // Account settings & durable executions
    // ---------------------------------------------------------------

    fn get_account_settings(&self, state: &LambdaState) -> AwsResponse {
        let funcs = state.functions.read();
        let mut total_code = 0u64;
        let mut total_reserved = 0i64;
        let mut count = 0u64;
        for entry in funcs.values() {
            if let Some(f) = entry.latest.as_ref() {
                count += 1;
                total_code += *f.code_size.read();
                if let Some(rc) = *f.reserved_concurrency.read() {
                    total_reserved += rc;
                }
            }
        }
        AwsResponse::json(
            200,
            json!({
                "AccountLimit": {
                    "TotalCodeSize": 80 * 1024 * 1024 * 1024u64,
                    "CodeSizeUnzipped": 250 * 1024 * 1024u64,
                    "CodeSizeZipped": 50 * 1024 * 1024u64,
                    "ConcurrentExecutions": 1000u64,
                    "UnreservedConcurrentExecutions": 1000u64.saturating_sub(total_reserved.max(0) as u64),
                },
                "AccountUsage": {
                    "TotalCodeSize": total_code,
                    "FunctionCount": count,
                }
            }),
        )
    }

    fn list_durable_executions(&self) -> AwsResponse {
        AwsResponse::json(200, json!({ "DurableExecutions": [] }))
    }

    // ---------------------------------------------------------------
    // Code signing configs (2020-04-22)
    // ---------------------------------------------------------------

    fn csc_dict(c: &crate::models::CodeSigningConfig) -> Value {
        let mut v = json!({
            "CodeSigningConfigArn": c.arn,
            "CodeSigningPolicies": c.policies.clone().unwrap_or_else(|| {
                json!({"UntrustedArtifactOnDeployment": "Warn"})
            }),
            "AllowedPublishers": c.allowed_publishers.clone(),
            "Description": c.description,
            "LastModified": c.last_modified,
        });
        if v["Description"] == json!("") {
            v["Description"] = Value::Null;
        }
        v
    }

    fn create_code_signing_config(&self, state: &LambdaState, req: &AwsRequest) -> AwsResponse {
        let Some(allowed_publishers) = req.params.get("AllowedPublishers").cloned() else {
            return AwsResponse::error(
                400,
                "InvalidParameterValueException",
                "Missing AllowedPublishers",
            );
        };
        let arn = format!(
            "arn:aws:lambda:{}:{}:code-signing-config:csc-{}",
            req.region,
            req.account,
            uuid::Uuid::new_v4().simple(),
        );
        let csc = Arc::new(crate::models::CodeSigningConfig {
            arn: arn.clone(),
            description: req.param_str("Description").unwrap_or_default(),
            allowed_publishers,
            policies: req.params.get("CodeSigningPolicies").cloned(),
            last_modified: layer_date_now(),
        });
        state
            .code_signing_configs
            .write()
            .insert(arn, Arc::clone(&csc));
        AwsResponse::json(201, json!({ "CodeSigningConfig": Self::csc_dict(&csc) }))
    }

    fn get_code_signing_config(&self, state: &LambdaState, req: &AwsRequest) -> AwsResponse {
        let arn = req.path_code_signing_arn();
        match state.code_signing_configs.read().get(&arn).cloned() {
            Some(csc) => {
                AwsResponse::json(200, json!({ "CodeSigningConfig": Self::csc_dict(&csc) }))
            }
            None => AwsResponse::error(
                404,
                "ResourceNotFoundException",
                &format!("The code signing configuration {arn} does not exist."),
            ),
        }
    }

    fn update_code_signing_config(&self, state: &LambdaState, req: &AwsRequest) -> AwsResponse {
        let arn = req.path_code_signing_arn();
        let existing = state.code_signing_configs.read().get(&arn).cloned();
        let Some(csc) = existing else {
            return AwsResponse::error(
                404,
                "ResourceNotFoundException",
                &format!("The code signing configuration {arn} does not exist."),
            );
        };
        let mut updated = crate::models::CodeSigningConfig {
            arn: csc.arn.clone(),
            description: csc.description.clone(),
            allowed_publishers: csc.allowed_publishers.clone(),
            policies: csc.policies.clone(),
            last_modified: csc.last_modified.clone(),
        };
        if let Some(desc) = req.param_str("Description") {
            updated.description = desc;
        }
        if let Some(ap) = req.params.get("AllowedPublishers").cloned() {
            updated.allowed_publishers = ap;
        }
        let code_signing_config = Arc::new(updated);
        state
            .code_signing_configs
            .write()
            .insert(arn, Arc::clone(&code_signing_config));
        AwsResponse::json(
            200,
            json!({ "CodeSigningConfig": Self::csc_dict(&code_signing_config) }),
        )
    }

    fn delete_code_signing_config(&self, state: &LambdaState, req: &AwsRequest) -> AwsResponse {
        let arn = req.path_code_signing_arn();
        if state.code_signing_configs.write().remove(&arn).is_none() {
            return AwsResponse::error(
                404,
                "ResourceNotFoundException",
                &format!("The code signing configuration {arn} does not exist."),
            );
        }
        AwsResponse::empty(204)
    }

    fn list_code_signing_configs(&self, state: &LambdaState) -> AwsResponse {
        let configs: Vec<Value> = state
            .code_signing_configs
            .read()
            .values()
            .map(|c| Self::csc_dict(c))
            .collect();
        AwsResponse::json(200, json!({ "CodeSigningConfigs": configs }))
    }

    fn list_functions_by_code_signing_config(
        &self,
        state: &LambdaState,
        req: &AwsRequest,
    ) -> AwsResponse {
        let arn = req.path_code_signing_arn();
        if !state.code_signing_configs.read().contains_key(&arn) {
            return AwsResponse::error(
                404,
                "ResourceNotFoundException",
                &format!("The code signing configuration {arn} does not exist."),
            );
        }
        let arns: Vec<Value> = state
            .functions
            .read()
            .values()
            .filter_map(|e| e.latest.as_ref())
            .filter(|f| f.code_signing_config_arn.read().as_deref() == Some(arn.as_str()))
            .map(|f| json!(format!("{}:$LATEST", f.function_arn)))
            .collect();
        AwsResponse::json(200, json!({ "FunctionArns": arns }))
    }
}

enum UrlVerb {
    Create,
    Get,
    Update,
    Delete,
}

// ----------------------------------------------------------------------
// Helpers
// ----------------------------------------------------------------------

/// Epoch seconds as the ESM LastModified mirror of the Python store.
fn epoch_now() -> f64 {
    use std::time::{SystemTime, UNIX_EPOCH};
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs_f64())
        .unwrap_or(0.0)
}

/// Last `max` BYTES of `s`, snapped forward to a UTF-8 char boundary so the
/// tail never ends in a partial multi-byte code point.
fn tail_utf8(s: &str, max: usize) -> &str {
    if s.len() <= max {
        return s;
    }
    let mut start = s.len() - max;
    while !s.is_char_boundary(start) {
        start += 1;
    }
    &s[start..]
}

fn decode_zip(v: Option<&Value>) -> Vec<u8> {
    match v {
        Some(Value::String(s)) => decode_b64(s),
        _ => Vec::new(),
    }
}

/// Resolve a deployment package: keep an inline (ZipFile) package as-is and
/// otherwise read the object content from the S3 crate's store for the
/// request's (account, region, S3Bucket, S3Key). Mirrors moto: a missing
/// bucket fails the operation as InvalidParameterValueException; a missing
/// key in an existing bucket yields empty code.
fn resolve_code_source(
    req: &AwsRequest,
    zip: Vec<u8>,
    s3_bucket: Option<&Value>,
    s3_key: Option<&Value>,
) -> Result<Vec<u8>, AwsResponse> {
    if !zip.is_empty() {
        return Ok(zip);
    }
    let (Some(bucket), Some(key)) = (
        s3_bucket.and_then(Value::as_str),
        s3_key.and_then(Value::as_str),
    ) else {
        return Ok(zip);
    };
    if bucket.is_empty() || key.is_empty() {
        return Ok(Vec::new());
    }
    if !s3::bridge::bucket_exists(req.account, &req.region, bucket) {
        return Err(AwsResponse::error(
            400,
            "InvalidParameterValueException",
            "Error occurred while GetObject. S3 Error Code: NoSuchBucket. S3 Error Message: The specified bucket does not exist",
        ));
    }
    Ok(s3::bridge::get_object_body(req.account, &req.region, bucket, key).unwrap_or_default())
}

fn decode_b64(s: &str) -> Vec<u8> {
    base64::engine::general_purpose::STANDARD
        .decode(s.trim())
        .unwrap_or_else(|_| s.as_bytes().to_vec())
}

fn sha_b64(data: &[u8]) -> String {
    let digest = Sha256::digest(data);
    base64::engine::general_purpose::STANDARD.encode(digest)
}

fn str_list(v: Option<&Value>) -> Vec<String> {
    v.and_then(Value::as_array)
        .map(|a| {
            a.iter()
                .filter_map(Value::as_str)
                .map(String::from)
                .collect()
        })
        .unwrap_or_default()
}

/// Resolve a function by name/ARN with an optional qualifier from the query
/// string; 404 on miss.
fn uuid_of_path(req: &AwsRequest) -> String {
    req.path_fn_uuid()
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;

    fn make_req(method: &str, params: Value, path: &str) -> AwsRequest {
        AwsRequest {
            service: "lambda".to_string(),
            operation: String::new(),
            account: 123456789012,
            region: "us-east-1".to_string(),
            params,
            body: Bytes::new(),
            method: method.to_string(),
            path: path.to_string(),
            query_string: String::new(),
            headers: std::collections::HashMap::new(),
        }
    }

    fn make_s3_req(req: s3::AwsRequest) -> s3::AwsResponse {
        s3::DefaultS3Handler::new().handle(req)
    }

    #[test]
    fn test_create_function_from_s3_source() {
        // Store a package in S3, then create a function whose code points at
        // it (S3Bucket + S3Key) — the body is read via s3::bridge.
        let s3_resp = make_s3_req(s3::AwsRequest {
            service: "s3".to_string(),
            operation: "CreateBucket".to_string(),
            account: 123456789012,
            region: "us-east-1".to_string(),
            bucket: Some("lam-bridge-bkt".to_string()),
            key: None,
            query_params: std::collections::HashMap::new(),
            headers: std::collections::HashMap::new(),
            method: "PUT".to_string(),
            body: Bytes::new(),
            params: serde_json::Value::Null,
        });
        assert_eq!(s3_resp.status, 200, "bucket create failed");

        let handler = LambdaHandler::new();
        let resp = handler.handle(make_req(
            "POST",
            json!({
                "FunctionName": "s3-src-fn",
                "Runtime": "python3.12",
                "Handler": "lambda_function.handler",
                "Code": {
                    "S3Bucket": "lam-bridge-bkt",
                    "S3Key": "missing-with-bucket-is-ok.zip",
                }
            }),
            "/2015-03-31/functions",
        ));
        assert_eq!(resp.status, 201, "body: {}", resp.body);

        // Missing bucket is rejected like moto's GetObject failure.
        let resp = handler.handle(make_req(
            "POST",
            json!({
                "FunctionName": "s3-src-bad",
                "Runtime": "python3.12",
                "Handler": "lambda_function.handler",
                "Code": {"S3Bucket": "lam-no-such-bkt", "S3Key": "k.zip"}
            }),
            "/2015-03-31/functions",
        ));
        assert_eq!(resp.status, 400, "body: {}", resp.body);
        assert!(resp.body.contains("NoSuchBucket"), "body: {}", resp.body);
    }

    /// Build a minimal (python-zipfile) deployment package for handler tests.
    fn make_code_zip(code: &str) -> Vec<u8> {
        let dir =
            std::env::temp_dir().join(format!("lam-hzip-{}", uuid::Uuid::new_v4().simple()));
        std::fs::create_dir_all(&dir).unwrap();
        let zpath = dir.join("t.zip");
        let snippet = "import io, sys, zipfile\nbuf = io.BytesIO()\nze = zipfile.ZipFile(buf, 'w')\nze.writestr('index.js', sys.argv[2])\nze.close()\nopen(sys.argv[1], 'wb').write(buf.getvalue())\n";
        let result = std::process::Command::new("python3")
            .arg("-c")
            .arg(snippet)
            .arg(zpath.as_os_str())
            .arg(code)
            .stdin(std::process::Stdio::null())
            .output()
            .expect("python3 failed");
        assert!(result.status.success(), "zip maker failed");
        let z = std::fs::read(&zpath).unwrap();
        let _ = std::fs::remove_dir_all(&dir);
        z
    }

    #[test]
    fn test_non_python_runtime_invoke_is_error_shaped() {
        let zip_b64 = base64::engine::general_purpose::STANDARD
            .encode(make_code_zip("exports.handler = async (e) => ({ok: true})"));
        let handler = LambdaHandler::new();
        handler.handle(make_req(
            "POST",
            json!({
                "FunctionName": "node-fn",
                "Runtime": "nodejs20.x",
                "Handler": "index.handler",
                "Code": {"ZipFile": zip_b64}
            }),
            "/2015-03-31/functions",
        ));
        let mut req = make_req(
            "POST",
            json!({}),
            "/2015-03-31/functions/node-fn/invocations",
        );
        req.headers.insert(
            "x-amz-invocation-type".to_string(),
            "RequestResponse".to_string(),
        );
        let resp = handler.handle(req);
        assert_eq!(resp.status, 200);
        assert_eq!(resp.body, "null");
        assert!(resp.headers.iter().any(|(k, v)| {
            k.eq_ignore_ascii_case("x-amz-function-error") && v == "Runtime.InvalidRuntime"
        }));
    }

    #[test]
    fn test_tail_utf8_bytes_boundary() {
        // 100 × 'é' = 200 bytes; an odd cap cuts mid code point and must
        // snap forward to the next boundary.
        let s = "é".repeat(100);
        let tail = tail_utf8(&s, 149);
        assert_eq!(tail.len(), 148); // snapped from 149 up to 2-byte alignment
        assert!(tail.chars().all(|c| c == 'é'));
        assert_eq!(tail, &s[52..]); // the final 74 code points (148 bytes)
        // Short strings pass through whole.
        assert_eq!(tail_utf8("abc", 10), "abc");
        // ASCII walks back exactly to the cap.
        assert_eq!(tail_utf8(&"x".repeat(10), 4), "xxxx");
        // An even cap on a 2-byte-wide string is already a boundary.
        assert_eq!(tail_utf8(&s, 150).len(), 150);
    }

    #[test]
    fn test_create_and_get_function() {
        let handler = LambdaHandler::new();
        handler.handle(make_req(
            "POST",
            json!({"FunctionName": "test-func", "Runtime": "python3.12", "Handler": "index.handler"}),
            "/2015-03-31/functions",
        ));
        let resp = handler.handle(make_req(
            "GET",
            json!({}),
            "/2015-03-31/functions/test-func",
        ));
        assert_eq!(resp.status, 200);
        assert!(resp.body.contains("test-func"));
    }

    #[test]
    fn test_list_functions() {
        let handler = LambdaHandler::new();
        handler.handle(make_req(
            "POST",
            json!({"FunctionName": "func1"}),
            "/2015-03-31/functions",
        ));
        handler.handle(make_req(
            "POST",
            json!({"FunctionName": "func2"}),
            "/2015-03-31/functions",
        ));
        let resp = handler.handle(make_req("GET", json!({}), "/2015-03-31/functions"));
        assert_eq!(resp.status, 200);
        let funcs: Value = serde_json::from_str(&resp.body).unwrap();
        assert_eq!(funcs["Functions"].as_array().unwrap().len(), 2);
    }

    #[test]
    fn test_publish_version_sequence() {
        let handler = LambdaHandler::new();
        handler.handle(make_req(
            "POST",
            json!({"FunctionName": "ver-func"}),
            "/2015-03-31/functions",
        ));
        let resp = handler.handle(make_req(
            "POST",
            json!({}),
            "/2015-03-31/functions/ver-func/versions",
        ));
        assert_eq!(resp.status, 201);
        let body: Value = serde_json::from_str(&resp.body).unwrap();
        assert_eq!(body["Version"], json!("1"));
        let resp = handler.handle(make_req(
            "POST",
            json!({}),
            "/2015-03-31/functions/ver-func/versions",
        ));
        let body: Value = serde_json::from_str(&resp.body).unwrap();
        // Idempotent: nothing changed since last publish, same version returned
        assert_eq!(body["Version"], json!("1"));
    }

    #[test]
    fn test_invoke_event_type() {
        let handler = LambdaHandler::new();
        let payload = json!({"FunctionName": "inv-func", "Code": {"ZipFile": ""}, "Handler": "lambda_function.handler"});
        handler.handle(make_req("POST", payload, "/2015-03-31/functions"));
        let mut req = make_req(
            "POST",
            json!({}),
            "/2015-03-31/functions/inv-func/invocations",
        );
        req.headers
            .insert("x-amz-invocation-type".to_string(), "Event".to_string());
        let resp = handler.handle(req);
        assert_eq!(resp.status, 202);
    }
}
