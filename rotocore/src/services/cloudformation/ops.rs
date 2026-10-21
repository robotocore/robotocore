//! CloudFormation operations: stacks, change sets, events, exports, stack
//! sets, and the registry/drift stubs. Ports
//! `src/robotocore/services/cloudformation/provider.py` plus the moto
//! wire shapes the compatibility suite pins.

use super::engine::{self, ResolveCtx};
use super::mirror;
use super::store;
use super::store::{
    with_store, CfnChangeSet, CfnResource, CfnStack, StackInstance, StackSet, StackSetOperation,
};
use super::xml;
use super::{account_str, form_params, indexed_members, key_values, param};
use crate::{Request, Response};
use serde_json::{json, Map, Value};
use std::collections::HashMap;
use uuid::Uuid;

const PSEUDO_KEYS: [&str; 6] = [
    "AWS::Region",
    "AWS::AccountId",
    "AWS::StackName",
    "AWS::StackId",
    "AWS::URLSuffix",
    "AWS::Partition",
];

fn is_pseudo(name: &str) -> bool {
    PSEUDO_KEYS.contains(&name) || name.starts_with("__")
}

struct CfnError {
    code: String,
    message: String,
    status: u16,
}

impl CfnError {
    fn new(code: &str, message: impl Into<String>) -> Self {
        Self {
            code: code.to_string(),
            message: message.into(),
            status: 400,
        }
    }
    fn status(mut self, status: u16) -> Self {
        self.status = status;
        self
    }
}

fn validation_error(message: impl Into<String>) -> CfnError {
    CfnError::new("ValidationError", message)
}

type OpResult = Result<Response, CfnError>;

pub fn handle(req: &Request) -> Response {
    let params = form_params(req);
    let action = param(&params, "Action").to_string();
    let account = account_str(req);
    let region = req.region.clone();
    match handle_action(&action, &params, region.as_str(), &account) {
        Ok(response) => response,
        Err(e) => xml::error(&e.code, &e.message, e.status),
    }
}

fn handle_action(
    action: &str,
    params: &HashMap<String, String>,
    region: &str,
    account: &str,
) -> OpResult {
    let account_num: u64 = account.parse().unwrap_or(123456789012);
    match action {
        "CreateStack" => create_stack(params, region, account),
        "UpdateStack" => update_stack(params, region, account),
        "DeleteStack" => delete_stack_action(params, region, account),
        "DescribeStacks" => describe_stacks(params, account_num, region),
        "ListStacks" => list_stacks(params, account_num, region),
        "DescribeStackResources" => describe_stack_resources(params, account_num, region),
        "DescribeStackResource" => describe_stack_resource(params, account_num, region),
        "DescribeStackEvents" => describe_stack_events(params, account_num, region),
        "ListStackResources" => list_stack_resources(params, account_num, region),
        "ListExports" => list_exports(account_num, region),
        "GetTemplate" => get_template(params, account_num, region),
        "ValidateTemplate" => validate_template(params),
        "CreateChangeSet" => create_change_set(params, region, account),
        "DescribeChangeSet" => describe_change_set(params, account_num, region),
        "DeleteChangeSet" => delete_change_set(params, account_num, region),
        "ExecuteChangeSet" => execute_change_set(params, region, account),
        _ => stub_action(action, params, account_num, region),
    }
}

// ---------------------------------------------------------------------------
// Shared helpers
// ---------------------------------------------------------------------------

fn scalar_text(v: &Value) -> String {
    match v {
        Value::String(s) => s.clone(),
        Value::Bool(b) => engine::py_str_bool(*b),
        Value::Number(n) => n.to_string(),
        Value::Null => String::new(),
        other => other.to_string(),
    }
}

fn user_parameters(params: &HashMap<String, String>) -> Vec<(String, String)> {
    let keys = indexed_members(params, "Parameters", "ParameterKey");
    let mut out = Vec::new();
    for (i, key) in keys.iter().enumerate() {
        let value = params
            .get(&format!("Parameters.member.{}.ParameterValue", i + 1))
            .cloned()
            .unwrap_or_default();
        out.push((key.clone(), value));
    }
    out
}

/// Events carry a fixed timestamp shape.
fn now_ts() -> String {
    xml::now_timestamp()
}

fn add_event(
    stack: &mut CfnStack,
    logical_id: &str,
    resource_type: &str,
    physical_id: &str,
    status: &str,
    reason: Option<String>,
) {
    stack.events.push(event_entry(
        &stack.stack_id,
        &stack.stack_name,
        logical_id,
        resource_type,
        physical_id,
        status,
        reason,
    ));
}

fn add_stack_event(stack: &mut CfnStack, status: &str, reason: Option<String>) {
    let physical = stack.stack_id.clone();
    let logical = stack.stack_name.clone();
    let rtype = "AWS::CloudFormation::Stack";
    let name = stack.stack_name.clone();
    let sid = stack.stack_id.clone();
    stack.events.push(event_entry(
        &sid, &name, &logical, rtype, &physical, status, reason,
    ));
}

fn event_entry(
    stack_id: &str,
    stack_name: &str,
    logical_id: &str,
    resource_type: &str,
    physical_id: &str,
    status: &str,
    reason: Option<String>,
) -> crate::services::cloudformation::store::CfnEvent {
    crate::services::cloudformation::store::CfnEvent {
        stack_id: stack_id.to_string(),
        stack_name: stack_name.to_string(),
        event_id: Uuid::new_v4().to_string(),
        logical_id: logical_id.to_string(),
        physical_id: physical_id.to_string(),
        resource_type: resource_type.to_string(),
        resource_status: status.to_string(),
        resource_status_reason: reason,
        timestamp: now_ts(),
    }
}

/// Build the resolve context, expanding template defaults and pseudo params.
fn make_ctx(
    stack: &CfnStack,
    template: &Value,
    region: &str,
    account: &str,
    imports: HashMap<String, String>,
) -> ResolveCtx {
    let mut parameters: HashMap<String, String> = stack.parameters.iter().cloned().collect();
    if let Some(pdefs) = template.get("Parameters").and_then(Value::as_object) {
        for (pname, pdef) in pdefs {
            if !parameters.contains_key(pname) {
                if let Some(d) = pdef.get("Default") {
                    parameters.insert(pname.clone(), scalar_text(d));
                }
            }
        }
    }
    parameters.insert("AWS::Region".to_string(), region.to_string());
    parameters.insert("AWS::AccountId".to_string(), account.to_string());
    parameters.insert("AWS::StackName".to_string(), stack.stack_name.clone());
    parameters.insert("AWS::StackId".to_string(), stack.stack_id.clone());
    engine::ResolveCtx {
        resources: HashMap::new(),
        parameters,
        mappings: template.get("Mappings").cloned().unwrap_or(Value::Null),
        conditions: HashMap::new(),
        imports,
        stack_name: stack.stack_name.clone(),
        stack_id: stack.stack_id.clone(),
        region: region.to_string(),
        account_id: account.to_string(),
    }
}

/// Expand AWS::Serverless functions into standard resources (SAM support).
fn expand_sam(template: &mut Value) {
    let is_sam = match template.get("Transform") {
        Some(Value::String(t)) => t == "AWS::Serverless-2016-10-31",
        Some(Value::Array(list)) => list
            .iter()
            .any(|t| t.as_str() == Some("AWS::Serverless-2016-10-31")),
        _ => false,
    };
    if !is_sam {
        return;
    }
    let Some(obj) = template.as_object_mut() else {
        return;
    };
    let mut resources = match obj.remove("Resources") {
        Some(Value::Object(m)) => m,
        Some(other) => {
            obj.insert("Resources".to_string(), other);
            return;
        }
        None => return,
    };
    let mut additions: Vec<(String, Value)> = Vec::new();
    for (logical_id, res_def) in resources.clone() {
        if res_def.get("Type").and_then(Value::as_str) != Some("AWS::Serverless::Function") {
            continue;
        }
        let props = res_def
            .get("Properties")
            .and_then(Value::as_object)
            .cloned()
            .unwrap_or_default();

        let role_ref: Value = if props.get("Role").is_none() {
            additions.push((
                format!("{logical_id}Role"),
                json!({
                    "Type": "AWS::IAM::Role",
                    "Properties": {
                        "AssumeRolePolicyDocument": {
                            "Version": "2012-10-17",
                            "Statement": [{
                                "Effect": "Allow",
                                "Principal": {"Service": "lambda.amazonaws.com"},
                                "Action": "sts:AssumeRole",
                            }],
                        },
                        "ManagedPolicyArns": [
                            "arn:aws:iam::aws:policy/service-role/AWSLambdaBasicExecutionRole",
                        ],
                    },
                }),
            ));
            json!({"Fn::GetAtt": [format!("{logical_id}Role"), "Arn"]})
        } else {
            props.get("Role").cloned().unwrap_or(json!({}))
        };

        let code: Value = if let Some(Value::String(inline)) = props.get("InlineCode") {
            json!({"ZipFile": inline})
        } else if let Some(code_uri) = props.get("CodeUri") {
            if let Some(uri) = code_uri.as_str() {
                json!({"S3Bucket": "sam-artifacts", "S3Key": uri})
            } else if let Some(obj) = code_uri.as_object() {
                let bucket = obj
                    .get("Bucket")
                    .and_then(Value::as_str)
                    .unwrap_or("sam-artifacts");
                let key = obj
                    .get("Key")
                    .and_then(Value::as_str)
                    .unwrap_or_else(|| obj.get("Uri").and_then(Value::as_str).unwrap_or(""));
                let mut c = json!({"S3Bucket": bucket, "S3Key": key});
                if let Some(v) = obj.get("Version") {
                    let _ = c
                        .as_object_mut()
                        .unwrap()
                        .insert("S3ObjectVersion".to_string(), v.clone());
                }
                c
            } else {
                json!({})
            }
        } else {
            json!({})
        };

        let mut lambda_props = json!({
            "Runtime": props.get("Runtime").and_then(Value::as_str).unwrap_or("python3.12"),
            "Handler": props.get("Handler").and_then(Value::as_str).unwrap_or("index.handler"),
            "Code": code,
            "Role": role_ref,
        });
        for key in [
            "FunctionName",
            "Timeout",
            "MemorySize",
            "Environment",
            "Layers",
            "Description",
        ] {
            if let Some(v) = props.get(key) {
                let _ = lambda_props
                    .as_object_mut()
                    .unwrap()
                    .insert(key.to_string(), v.clone());
            }
        }
        let mut fn_def = Map::new();
        fn_def.insert(
            "Type".to_string(),
            Value::String("AWS::Lambda::Function".to_string()),
        );
        fn_def.insert("Properties".to_string(), lambda_props);
        if roles_added(&additions, logical_id.as_str()) {
            fn_def.insert(
                "DependsOn".to_string(),
                Value::Array(vec![Value::String(format!("{logical_id}Role"))]),
            );
        }
        let _ = resources.insert(logical_id.clone(), Value::Object(fn_def));

        if let Some(events) = props.get("Events").and_then(Value::as_object) {
            for (event_name, event_def) in events {
                let event_type = event_def.get("Type").and_then(Value::as_str).unwrap_or("");
                let event_props = event_def
                    .get("Properties")
                    .cloned()
                    .unwrap_or_else(|| json!({}));
                match event_type {
                    "SQS" => additions.push((
                        format!("{logical_id}{event_name}ESM"),
                        json!({
                            "Type": "AWS::Lambda::EventSourceMapping",
                            "Properties": {
                                "FunctionName": {"Ref": logical_id},
                                "EventSourceArn": event_props.get("Queue").cloned().unwrap_or(Value::Null),
                                "BatchSize": event_props.get("BatchSize").and_then(Value::as_i64).unwrap_or(10),
                            },
                            "DependsOn": [logical_id],
                        }),
                    )),
                    "Api" => {
                        let api_id = format!("{logical_id}Api");
                        if !resources.contains_key(&api_id)
                            && !additions.iter().any(|(id, _)| id == &api_id)
                            && logical_id != api_id
                        {
                            additions.push((
                                api_id,
                                json!({
                                    "Type": "AWS::ApiGateway::RestApi",
                                    "Properties": {
                                        "Name": {"Fn::Sub": "${AWS::StackName}-api"},
                                    },
                                }),
                            ));
                        }
                    }
                    _ => {}
                }
            }
        }
    }
    for (id, val) in additions {
        let _ = resources.insert(id, val);
    }
    let _ = obj.insert("Resources".to_string(), Value::Object(resources));
    let _ = obj.remove("Transform");
}

fn roles_added(additions: &[(String, Value)], logical_id: &str) -> bool {
    additions
        .iter()
        .any(|(id, _)| id == &format!("{logical_id}Role"))
}

/// Validate template parameters (required + AllowedValues).
fn validate_parameters(
    template: &Value,
    cfn_params: &HashMap<String, String>,
) -> Result<(), CfnError> {
    let Some(pdefs) = template.get("Parameters").and_then(Value::as_object) else {
        return Ok(());
    };
    for (pname, pdef) in pdefs {
        if pdef.get("Default").is_none() && !cfn_params.contains_key(pname) {
            return Err(validation_error(format!(
                "Parameters: [{pname}] must have values"
            )));
        }
    }
    for (pname, pdef) in pdefs {
        let Some(allowed) = pdef.get("AllowedValues").and_then(Value::as_array) else {
            continue;
        };
        if allowed.is_empty() {
            continue;
        }
        let value = cfn_params
            .get(pname)
            .cloned()
            .unwrap_or_else(|| scalar_text(pdef.get("Default").unwrap_or(&Value::Null)));
        let all: Vec<String> = allowed.iter().map(scalar_text).collect();
        if !value.is_empty() && !all.contains(&value) {
            return Err(validation_error(format!(
                "Parameter '{pname}' must be one of AllowedValues: {}. Got: {value}",
                all.join(", ")
            )));
        }
    }
    Ok(())
}

/// Validate that Fn::ImportValue references resolve in the global exports.
fn find_imports(value: &Value, out: &mut Vec<String>) {
    match value {
        Value::Object(map) => {
            if let Some(Value::String(s)) = map.get("Fn::ImportValue") {
                out.push(s.clone());
            }
            for v in map.values() {
                find_imports(v, out);
            }
        }
        Value::Array(items) => {
            for v in items {
                find_imports(v, out);
            }
        }
        _ => {}
    }
}

/// Prevent deleting a stack whose exports other live stacks still import.
/// Takes the store it runs against (no nested lock acquisition).
fn check_import_references(store: &store::CfnStore, stack: &CfnStack) -> Result<(), CfnError> {
    if stack.exports.is_empty() {
        return Ok(());
    }
    let export_names: Vec<String> = stack.exports.iter().map(|(k, _)| k.clone()).collect();
    for other in &store.stacks {
        if other.stack_name == stack.stack_name || other.status == "DELETE_COMPLETE" {
            continue;
        }
        if other.template_body.is_empty() {
            continue;
        }
        for ename in &export_names {
            if other.template_body.contains(ename.as_str()) {
                return Err(validation_error(format!(
                    "Export '{ename}' cannot be deleted as it is in use by stack '{}'",
                    other.stack_name
                )));
            }
        }
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// Resource creation via the mirror
// ---------------------------------------------------------------------------

fn fallback_name(logical_id: &str) -> String {
    format!("cfn-{logical_id}-{}", uuid_head())
}

fn fallback_name_lower(logical_id: &str) -> String {
    format!("cfn-{}-{}", logical_id.to_lowercase(), uuid_head())
}

fn uuid_head() -> String {
    String::from(&Uuid::new_v4().to_string()[..8])
}

/// Create one resource through the mirror. Returns its physical id and the
/// attributes the Python provider published; unmirrored types get a
/// placeholder identity (the sidecar served moto-sized backends from its
/// own memory), and mirror failures roll the deployment back.
fn create_resource(
    logical_id: &str,
    rtype: &str,
    props: &Value,
    account: &str,
    region: &str,
) -> Result<(Option<String>, HashMap<String, Value>), String> {
    let (physical, attrs): (Option<String>, HashMap<String, Value>) = match rtype {
        "AWS::SQS::Queue" => {
            let name = props
                .get("QueueName")
                .and_then(Value::as_str)
                .map(String::from)
                .unwrap_or_else(|| fallback_name(logical_id));
            let (url, attrs) = mirror::create_sqs_queue(props, &name, account, region)
                .ok_or("sqs mirror failed")?;
            (Some(url), json_obj(&attrs))
        }
        "AWS::SNS::Topic" => {
            let name = props
                .get("TopicName")
                .and_then(Value::as_str)
                .map(String::from)
                .unwrap_or_else(|| fallback_name(logical_id));
            let (arn, attrs) =
                mirror::create_sns_topic(&name, account, region).ok_or("sns mirror failed")?;
            (Some(arn), json_obj(&attrs))
        }
        "AWS::SNS::Subscription" => {
            let topic_arn = props.get("TopicArn").and_then(Value::as_str).unwrap_or("");
            let protocol = props.get("Protocol").and_then(Value::as_str).unwrap_or("");
            let endpoint = props.get("Endpoint").and_then(Value::as_str).unwrap_or("");
            match mirror::subscribe_sns(topic_arn, protocol, endpoint, account, region) {
                Some((sub_arn, attrs)) => (Some(sub_arn), json_obj(&attrs)),
                // No confirmed subscription: keep the resource without one.
                None => (None, HashMap::new()),
            }
        }
        "AWS::DynamoDB::Table" => {
            let name = props
                .get("TableName")
                .and_then(Value::as_str)
                .map(String::from)
                .unwrap_or_else(|| fallback_name(logical_id));
            if !mirror::create_dynamodb_table(props, &name, account, region) {
                return Err("dynamodb mirror failed".into());
            }
            let attrs = json_obj(&json!({
                "Arn": format!("arn:aws:dynamodb:{region}:{account}:table/{name}"),
                "TableName": name,
            }));
            (Some(name), attrs)
        }
        "AWS::IAM::Role" => {
            let name = props
                .get("RoleName")
                .and_then(Value::as_str)
                .map(String::from)
                .unwrap_or_else(|| fallback_name(logical_id));
            let assume = props
                .get("AssumeRolePolicyDocument")
                .cloned()
                .unwrap_or_else(|| json!({}));
            let (_role_id, attrs) =
                mirror::create_iam_role(&name, &assume, account).ok_or("iam role mirror failed")?;
            (Some(name), json_obj(&attrs))
        }
        "AWS::IAM::Policy" => {
            let name = props
                .get("PolicyName")
                .and_then(Value::as_str)
                .map(String::from)
                .unwrap_or_else(|| fallback_name(logical_id));
            let doc = props.get("PolicyDocument").cloned().unwrap_or(json!({}));
            if let Some(roles) = props.get("Roles").and_then(Value::as_array) {
                for role in roles {
                    if let Some(role_name) = role.as_str() {
                        let _ = mirror::put_role_policy(role_name, &name, &doc, account);
                    }
                }
            }
            if let Some(users) = props.get("Users").and_then(Value::as_array) {
                for user in users {
                    if let Some(user_name) = user.as_str() {
                        let _ = mirror::put_user_policy(user_name, &name, &doc, account);
                    }
                }
            }
            let attrs = json_obj(&json!({"PolicyName": name}));
            (Some(name), attrs)
        }
        "AWS::SSM::Parameter" => {
            let name = props
                .get("Name")
                .and_then(Value::as_str)
                .map(String::from)
                .unwrap_or_else(|| format!("/cfn/{logical_id}"));
            if !mirror::put_ssm_parameter(props, &name, account, region) {
                return Err("ssm mirror failed".into());
            }
            let mut attrs = HashMap::new();
            if let Some(t) = props.get("Type") {
                let _ = attrs.insert("Type".to_string(), t.clone());
            }
            if let Some(v) = props.get("Value") {
                let _ = attrs.insert("Value".to_string(), v.clone());
            }
            (Some(name), attrs)
        }
        "AWS::S3::Bucket" => {
            let name = props
                .get("BucketName")
                .and_then(Value::as_str)
                .map(String::from)
                .unwrap_or_else(|| fallback_name_lower(logical_id));
            if !mirror::create_s3_bucket_full(props, &name, account, region) {
                return Err("s3 mirror failed".into());
            }
            let attrs = json_obj(&json!({
                "Arn": format!("arn:aws:s3:::{name}"),
                "DomainName": format!("{name}.s3.amazonaws.com"),
                "RegionalDomainName": format!("{name}.s3.{region}.amazonaws.com"),
                "WebsiteURL": format!("http://{name}.s3-website-{region}.amazonaws.com"),
            }));
            (Some(name), attrs)
        }
        "AWS::Logs::LogGroup" => {
            let name = props
                .get("LogGroupName")
                .and_then(Value::as_str)
                .map(String::from)
                .unwrap_or_else(|| format!("/cfn/{logical_id}"));
            if !mirror::create_log_group(&name, account, region) {
                return Err("logs mirror failed".into());
            }
            if let Some(days) = props.get("RetentionInDays").and_then(Value::as_u64) {
                mirror::put_log_retention(&name, days, account, region);
            }
            let attrs = json_obj(&json!({
                "Arn": format!("arn:aws:logs:{region}:{account}:log-group:{name}"),
            }));
            (Some(name), attrs)
        }
        "AWS::Events::Rule" => {
            let name = props
                .get("Name")
                .and_then(Value::as_str)
                .map(String::from)
                .unwrap_or_else(|| fallback_name(logical_id));
            let (_phys, attrs) = mirror::put_events_rule(props, &name, account, region)
                .ok_or("events mirror failed")?;
            (Some(name), json_obj(&attrs))
        }
        "AWS::KMS::Key" => {
            let (key_id, attrs) =
                mirror::create_kms_key(props, account, region).ok_or("kms mirror failed")?;
            (Some(key_id), json_obj(&attrs))
        }
        "AWS::Lambda::Function" => {
            let name = props
                .get("FunctionName")
                .and_then(Value::as_str)
                .map(String::from)
                .unwrap_or_else(|| fallback_name(logical_id));
            let (_phys, attrs) = mirror::create_lambda_function(props, &name, account, region)
                .ok_or("lambda mirror failed")?;
            (Some(name), json_obj(&attrs))
        }
        "AWS::SecretsManager::Secret" => {
            let fallback = fallback_name(logical_id);
            let (name, attrs) =
                mirror::create_secretsmanager_secret(props, &fallback, account, region)
                    .ok_or("secretsmanager mirror failed")?;
            (Some(name), json_obj(&attrs))
        }
        "AWS::Kinesis::Stream" => {
            let fallback = fallback_name(logical_id);
            let (name, attrs) = mirror::create_kinesis_stream(props, &fallback, account, region)
                .ok_or("kinesis mirror failed")?;
            (Some(name), json_obj(&attrs))
        }
        "AWS::StepFunctions::StateMachine" => {
            let fallback = fallback_name(logical_id);
            let (arn, attrs) = mirror::create_sfn_state_machine(props, &fallback, account, region)
                .ok_or("stepfunctions mirror failed")?;
            (Some(arn.clone()), json_obj(&attrs))
        }
        "AWS::ECS::Cluster" => {
            let name = props
                .get("ClusterName")
                .and_then(Value::as_str)
                .map(String::from)
                .unwrap_or_else(|| fallback_name(logical_id));
            let (name, attrs) =
                mirror::create_ecs_cluster(&name, account, region).ok_or("ecs mirror failed")?;
            (Some(name.clone()), json_obj(&attrs))
        }
        "AWS::EC2::VPC" => {
            let (vpc_id, attrs) =
                mirror::create_ec2_vpc(props, account, region).ok_or("ec2 vpc mirror failed")?;
            (Some(vpc_id), json_obj(&attrs))
        }
        "AWS::EC2::Subnet" => {
            let (subnet_id, attrs) = mirror::create_ec2_subnet(props, account, region)
                .ok_or("ec2 subnet mirror failed")?;
            (Some(subnet_id), json_obj(&attrs))
        }
        "AWS::EC2::InternetGateway" => {
            let (igw_id, attrs) = mirror::create_ec2_internet_gateway(props, account, region)
                .ok_or("ec2 igw mirror failed")?;
            (Some(igw_id), json_obj(&attrs))
        }
        "AWS::EC2::VPCGatewayAttachment" => {
            let (attach_id, attrs) =
                mirror::create_ec2_vpc_gateway_attachment(props, account, region)
                    .ok_or("ec2 attachment mirror failed")?;
            (Some(attach_id), json_obj(&attrs))
        }
        "AWS::EC2::RouteTable" => {
            let (rt_id, attrs) = mirror::create_ec2_route_table(props, account, region)
                .ok_or("ec2 route table mirror failed")?;
            (Some(rt_id), json_obj(&attrs))
        }
        "AWS::EC2::Route" => {
            let (phys, attrs) = mirror::create_ec2_route(props, account, region)
                .ok_or("ec2 route mirror failed")?;
            (Some(phys), json_obj(&attrs))
        }
        "AWS::EC2::SubnetRouteTableAssociation" => {
            let (assoc_id, attrs) =
                mirror::create_ec2_subnet_rtb_association(props, account, region)
                    .ok_or("ec2 subnet-rtb association mirror failed")?;
            (Some(assoc_id), json_obj(&attrs))
        }
        "AWS::EC2::SecurityGroup" => {
            // moto passes the GroupName property (or the generated physical
            // name) as the security group name.
            let name = mirror::value_str(props, "GroupName");
            let name = if name.is_empty() {
                fallback_name(logical_id)
            } else {
                name
            };
            let (sg_id, attrs) = mirror::create_ec2_security_group(props, &name, account, region)
                .ok_or("ec2 security group mirror failed")?;
            (Some(sg_id), json_obj(&attrs))
        }
        "AWS::Cognito::UserPool" => {
            let name = mirror::value_str(props, "UserPoolName");
            let name = if name.is_empty() {
                fallback_name(logical_id)
            } else {
                name
            };
            let (pool_id, attrs) = mirror::create_cognito_user_pool(props, &name, account, region)
                .ok_or("cognito user pool mirror failed")?;
            (Some(pool_id), json_obj(&attrs))
        }
        "AWS::Cognito::UserPoolClient" => {
            let fallback = fallback_name(logical_id);
            let (client_id, attrs) =
                mirror::create_cognito_user_pool_client(props, &fallback, account, region)
                    .ok_or("cognito user pool client mirror failed")?;
            (Some(client_id), json_obj(&attrs))
        }
        "AWS::CloudWatch::Alarm" => {
            let name = mirror::value_str(props, "AlarmName");
            let name = if name.is_empty() {
                fallback_name(logical_id)
            } else {
                name
            };
            let (phys, attrs) = mirror::create_cloudwatch_alarm(props, &name, account, region)
                .ok_or("cloudwatch alarm mirror failed")?;
            (Some(phys), json_obj(&attrs))
        }
        "AWS::S3::BucketPolicy" => {
            let name = mirror::value_str(props, "Bucket");
            if name.is_empty() {
                return Err("bucket policy mirror failed".into());
            }
            let Some(policy) = props.get("PolicyDocument").filter(|p| !p.is_null()) else {
                return Err("bucket policy mirror failed".into());
            };
            if !mirror::put_s3_bucket_policy(&name, policy, account, region) {
                return Err("s3 mirror failed".into());
            }
            let attrs = json_obj(&json!({}));
            (Some(name), attrs)
        }
        "AWS::Lambda::EventSourceMapping" => {
            let (uuid, attrs) = mirror::create_lambda_esm(props, account, region)
                .ok_or("lambda event source mapping mirror failed")?;
            (Some(uuid), json_obj(&attrs))
        }
        "AWS::ApiGateway::RestApi" => {
            let name = mirror::value_str(props, "Name");
            let name = if name.is_empty() {
                fallback_name(logical_id)
            } else {
                name
            };
            let (api_id, attrs) = mirror::create_apigw_rest_api(props, &name, account, region)
                .ok_or("apigateway rest api mirror failed")?;
            (Some(api_id), json_obj(&attrs))
        }
        "AWS::ApiGateway::Resource" => {
            let (resource_id, attrs) = mirror::create_apigw_resource(props, account, region)
                .ok_or("apigateway resource mirror failed")?;
            (Some(resource_id), json_obj(&attrs))
        }
        "AWS::ApiGateway::Method" => {
            let (phys, attrs) = mirror::create_apigw_method(props, account, region)
                .ok_or("apigateway method mirror failed")?;
            (Some(phys), json_obj(&attrs))
        }
        "AWS::ApiGateway::Deployment" => {
            let (phys, attrs) = mirror::create_apigw_deployment(props, account, region)
                .ok_or("apigateway deployment mirror failed")?;
            (Some(phys), json_obj(&attrs))
        }
        "AWS::ApiGateway::Stage" => {
            let (phys, attrs) = mirror::create_apigw_stage(props, account, region)
                .ok_or("apigateway stage mirror failed")?;
            (Some(phys), json_obj(&attrs))
        }
        _ => {
            return Err(format!("unsupported resource type: {rtype}"));
        }
    };
    Ok((physical, attrs))
}

/// Json object -> attribute map helper.
fn json_obj(v: &Value) -> HashMap<String, Value> {
    v.as_object()
        .map(|m| m.iter().map(|(k, val)| (k.clone(), val.clone())).collect())
        .unwrap_or_default()
}

/// Delete one mirrored resource from its recorded state.
fn delete_resource(resource: &CfnResource, account: &str, region: &str) {
    let rtype = resource.resource_type.as_str();
    let phys = resource.physical_id.clone().unwrap_or_default();
    let props = &resource.properties;
    let get = |key: &str| props.get(key).and_then(Value::as_str).map(String::from);
    match rtype {
        "AWS::SQS::Queue" => {
            let name = get("QueueName").unwrap_or_else(|| {
                phys.trim_end_matches('/')
                    .rsplit('/')
                    .next()
                    .unwrap_or("")
                    .to_string()
            });
            if !name.is_empty() {
                mirror::delete_sqs_queue(&name, account, region);
            }
        }
        "AWS::SNS::Topic" => {
            let arn = get("TopicArn")
                .or_else(|| {
                    resource
                        .attributes
                        .get("TopicArn")
                        .and_then(Value::as_str)
                        .map(String::from)
                })
                .unwrap_or_else(|| phys.clone());
            if !arn.is_empty() {
                mirror::delete_sns_topic(&arn, account, region);
            }
        }
        "AWS::SNS::Subscription" => {
            if !phys.is_empty() {
                mirror::unsubscribe_sns(&phys, account, region);
            }
        }
        "AWS::DynamoDB::Table" => {
            let name = get("TableName").unwrap_or_else(|| phys.clone());
            if !name.is_empty() {
                mirror::delete_dynamodb_table(&name, account, region);
            }
        }
        "AWS::IAM::Role" => {
            let name = get("RoleName").unwrap_or_else(|| phys.clone());
            if !name.is_empty() {
                mirror::delete_iam_role(&name, account);
            }
        }
        "AWS::SSM::Parameter" => {
            let name = get("Name").unwrap_or_else(|| phys.clone());
            if !name.is_empty() {
                mirror::delete_ssm_parameter(&name, account, region);
            }
        }
        "AWS::S3::Bucket" => {
            if !phys.is_empty() {
                mirror::delete_s3_bucket(&phys, account, region);
            }
        }
        "AWS::Logs::LogGroup" => {
            if !phys.is_empty() {
                mirror::delete_log_group(&phys, account, region);
            }
        }
        "AWS::Events::Rule" => {
            if !phys.is_empty() {
                mirror::delete_events_rule(&phys, account, region);
            }
        }
        "AWS::KMS::Key" => {
            if !phys.is_empty() {
                mirror::delete_kms_key(&phys, account, region);
            }
        }
        "AWS::Lambda::Function" => {
            if !phys.is_empty() {
                let name = phys.clone();
                mirror::delete_lambda_function(&name, account, region);
            }
        }
        "AWS::SecretsManager::Secret" => {
            let name = get("Name").unwrap_or_else(|| phys.clone());
            if !name.is_empty() {
                mirror::delete_secretsmanager_secret(&name, account, region);
            }
        }
        "AWS::Kinesis::Stream" => {
            if !phys.is_empty() {
                mirror::delete_kinesis_stream(&phys, account, region);
            }
        }
        "AWS::StepFunctions::StateMachine" => {
            if !phys.is_empty() {
                mirror::delete_sfn_state_machine(&phys, account, region);
            }
        }
        "AWS::ECS::Cluster" if !phys.is_empty() => {
            mirror::delete_ecs_cluster(&phys, account, region);
        }
        "AWS::EC2::VPC" if !phys.is_empty() => mirror::delete_ec2_vpc(&phys, account, region),
        "AWS::EC2::Subnet" if !phys.is_empty() => mirror::delete_ec2_subnet(&phys, account, region),
        "AWS::EC2::InternetGateway" if !phys.is_empty() => {
            mirror::delete_ec2_internet_gateway(&phys, account, region)
        }
        "AWS::EC2::VPCGatewayAttachment" => {
            mirror::delete_ec2_vpc_gateway_attachment(props, account, region)
        }
        "AWS::EC2::RouteTable" if !phys.is_empty() => {
            mirror::delete_ec2_route_table(&phys, account, region)
        }
        "AWS::EC2::Route" => mirror::delete_ec2_route(props, account, region),
        "AWS::EC2::SubnetRouteTableAssociation" => {
            let assoc_id = get("AssociationId").unwrap_or_else(|| phys.clone());
            if !assoc_id.is_empty() {
                mirror::delete_ec2_subnet_rtb_association(&assoc_id, account, region);
            }
        }
        "AWS::EC2::SecurityGroup" if !phys.is_empty() => {
            mirror::delete_ec2_security_group(&phys, account, region)
        }
        "AWS::Cognito::UserPool" if !phys.is_empty() => {
            mirror::delete_cognito_user_pool(&phys, account, region)
        }
        "AWS::Cognito::UserPoolClient" => mirror::delete_cognito_user_pool_client(
            &phys,
            &get("UserPoolId").unwrap_or_default(),
            account,
            region,
        ),
        "AWS::CloudWatch::Alarm" if !phys.is_empty() => {
            mirror::delete_cloudwatch_alarm(&phys, account, region)
        }
        "AWS::S3::BucketPolicy" => {
            let bucket = get("Bucket").unwrap_or_else(|| phys.clone());
            if !bucket.is_empty() {
                mirror::delete_s3_bucket_policy(&bucket, account, region);
            }
        }
        "AWS::Lambda::EventSourceMapping" if !phys.is_empty() => {
            mirror::delete_lambda_esm(&phys, account, region);
        }
        _ => {
            // Placeholder types have no side effects to unwind.
        }
    }
}

// ---------------------------------------------------------------------------
// Deployment pipeline
// ---------------------------------------------------------------------------

/// Deploy a stack: topological creation through the mirror, then resolve
/// outputs and register exports. Mirrors `_deploy_stack`; store access is
/// via the passed handle (never nested `with_store` — it is not reentrant).
fn deploy_stack(
    store: &mut store::CfnStore,
    stack: &mut CfnStack,
    template: &mut Value,
    region: &str,
    account: &str,
    account_num: u64,
) -> Result<(), String> {
    expand_sam(template);

    // Pre-validate Fn::ImportValue references against live exports.
    let imports = store.export_map();
    let mut import_refs: Vec<String> = Vec::new();
    find_imports(
        template.get("Resources").unwrap_or(&Value::Null),
        &mut import_refs,
    );
    for name in import_refs {
        if !imports.contains_key(&name) {
            return Err(format!(
                "No export named '{name}' found. Rollback requested by user."
            ));
        }
    }

    let base_ctx = make_ctx(stack, template, region, account, imports);
    let conditions = engine::evaluate_conditions(template, &base_ctx)?;
    let mut ctx = base_ctx.clone();
    ctx.conditions = conditions.clone();

    let order = engine::build_dependency_order(template)?;
    let resource_defs: Map<String, Value> = template
        .get("Resources")
        .and_then(Value::as_object)
        .cloned()
        .unwrap_or_default();

    for logical_id in order {
        if stack.resource(&logical_id).is_some() {
            continue; // in-place update: untouched
        }
        let Some(res_def) = resource_defs.get(&logical_id) else {
            continue;
        };
        if let Some(cond_name) = res_def.get("Condition").and_then(Value::as_str) {
            if !conditions.get(cond_name).copied().unwrap_or(true) {
                continue;
            }
        }
        let Some(rtype) = res_def.get("Type").and_then(Value::as_str) else {
            continue;
        };
        let raw_props = res_def
            .get("Properties")
            .cloned()
            .unwrap_or_else(|| json!({}));
        let resolved = engine::resolve_intrinsics(&raw_props, &ctx, 0)?;
        let deletion_policy = res_def
            .get("DeletionPolicy")
            .and_then(Value::as_str)
            .unwrap_or("Delete")
            .to_string();

        // AWS::CloudFormation::Stack: fetch the child template over the S3
        // mirror, deploy it as its own stack record, and surface its outputs
        // as attributes (moto's Stack create_from_cloudformation_json embeds
        // the child through the CFN backend).
        if rtype == "AWS::CloudFormation::Stack" {
            add_event(stack, &logical_id, rtype, "", "CREATE_IN_PROGRESS", None);
            let child_body = deploy_nested_stack(
                store,
                stack,
                &logical_id,
                &resolved,
                region,
                account,
                account_num,
            )?;
            let attrs = json_obj(&child_body);
            let phys_str = child_body
                .get("stack_name")
                .and_then(Value::as_str)
                .unwrap_or("")
                .to_string();
            add_event(
                stack,
                &logical_id,
                rtype,
                &phys_str,
                "CREATE_COMPLETE",
                None,
            );
            stack.resources.push(CfnResource {
                logical_id: logical_id.clone(),
                resource_type: rtype.to_string(),
                properties: resolved.clone(),
                physical_id: Some(phys_str.clone()),
                attributes: attrs.clone(),
                status: "CREATE_COMPLETE".to_string(),
                deletion_policy,
            });
            ctx.resources.insert(
                logical_id.clone(),
                engine::ResourceInfo {
                    resource_type: rtype.to_string(),
                    physical_id: phys_str,
                    attributes: attrs,
                },
            );
            continue;
        }

        add_event(stack, &logical_id, rtype, "", "CREATE_IN_PROGRESS", None);
        let (physical, attrs) = create_resource(&logical_id, rtype, &resolved, account, region)
            .map_err(|e| {
                // StatusReason-shaped like the real service (the inner text
                // keeps the mirror detail for debugging).
                format!("The following resource(s) failed to create: [{logical_id}]: {e}")
            })?;
        let phys_str = physical.clone().unwrap_or_default();
        add_event(
            stack,
            &logical_id,
            rtype,
            &phys_str,
            "CREATE_COMPLETE",
            None,
        );
        stack.resources.push(CfnResource {
            logical_id: logical_id.clone(),
            resource_type: rtype.to_string(),
            properties: resolved.clone(),
            physical_id: physical,
            attributes: attrs.clone(),
            status: "CREATE_COMPLETE".to_string(),
            deletion_policy,
        });
        ctx.resources.insert(
            logical_id.clone(),
            engine::ResourceInfo {
                resource_type: rtype.to_string(),
                physical_id: phys_str,
                attributes: attrs,
            },
        );
    }

    resolve_outputs(store, stack, template, &ctx, region);
    let _ = account_num;
    Ok(())
}

/// Parse a CFN TemplateURL into (bucket, key) — mirrors moto's
/// `get_stack_from_s3_url` parsing rules (bucket in path for localhost /
/// s3.amazon URLs, bucket in host otherwise).
fn template_url_bucket_key(url: &str) -> Option<(String, String)> {
    let without_scheme = url.split_once("://")?.1;
    let (host, path) = without_scheme.split_once('/')?;
    if host.contains("localhost")
        || host.starts_with("127.0.0.1")
        || (host.starts_with("s3.") && host.ends_with("amazonaws.com"))
        || (host.starts_with("s3-") && host.ends_with(".amazonaws.com"))
    {
        let (bucket, key) = path.split_once('/')?;
        Some((bucket.to_string(), key.to_string()))
    } else {
        let bucket = host.split('.').next()?;
        Some((bucket.to_string(), path.to_string()))
    }
}

#[allow(clippy::too_many_arguments)]
fn deploy_nested_stack(
    store: &mut store::CfnStore,
    stack: &mut CfnStack,
    logical_id: &str,
    props: &Value,
    region: &str,
    account: &str,
    account_num: u64,
) -> Result<Value, String> {
    let url = props
        .get("TemplateURL")
        .and_then(Value::as_str)
        .unwrap_or("");
    let (bucket, key) = template_url_bucket_key(url).ok_or_else(|| {
        format!("Nested stack ({logical_id}): TemplateURL '{url}' is not an S3 URL")
    })?;
    let body = mirror::s3_get_object(&bucket, &key, account, region).ok_or_else(|| {
        format!("Nested stack ({logical_id}): template s3://{bucket}/{key} could not be fetched")
    })?;
    let mut child_template = engine::parse_template(&body)
        .map_err(|e| format!("Nested stack ({logical_id}): child template invalid: {e}"))?;
    let child_name = format!("{}-{}-{}", stack.stack_name, logical_id, uuid_head());
    let child_id = store::new_stack_id(&child_name, region, account);
    let mut child = CfnStack::new(child_id.clone(), child_name.clone(), body.clone());
    if let Some(params) = props.get("Parameters").and_then(Value::as_object) {
        for (k, v) in params {
            let value = scalar_text(v);
            child.parameters.retain(|(ek, _)| ek != k);
            child.parameters.push((k.clone(), value));
        }
    }
    add_stack_event(&mut child, "CREATE_IN_PROGRESS", None);
    let deploy_result = deploy_stack(
        store,
        &mut child,
        &mut child_template,
        region,
        account,
        account_num,
    );
    // Deploy on the local record (deploy_stack holds the store); the child
    // row joins the store on BOTH exits — a mid-deploy failure must leave a
    // record behind, or the mirror-created resources (queues, buckets, VPCs)
    // exist with no stack record to unwind them through the cascade.
    let mut failed: Option<String> = None;
    match deploy_result {
        Ok(()) => {
            add_stack_event(&mut child, "CREATE_COMPLETE", None);
            child.status = "CREATE_COMPLETE".to_string();
        }
        Err(fail) => {
            let why = format!("Nested stack ({logical_id}) failed: {fail}");
            add_stack_event(&mut child, "ROLLBACK_IN_PROGRESS", None);
            child.status = "ROLLBACK_COMPLETE".to_string();
            child.status_reason = why.clone();
            failed = Some(why);
        }
    }
    // Publish the child's outputs as attributes so descriptions, ref curls
    // and deletions can reach them.
    let mut out = json!({"stack_name": child_name.clone(), "stack_id": child_id.clone()});
    for (k, v) in &child.outputs {
        if let Some(val) = v.get("OutputValue").and_then(Value::as_str) {
            out.as_object_mut().unwrap().insert(k.clone(), json!(val));
        }
    }
    store.stacks.push(child);
    if let Some(why) = failed {
        // Unwind what the child created through the same cascade helper the
        // delete path uses; the parent keeps its own rollback semantics.
        cascade_delete_nested_stack(
            store,
            &child_name,
            account,
            region,
            "ROLLBACK_COMPLETE",
            Some(&why),
        );
        return Err(why);
    }
    Ok(out)
}

/// Unwind one stack's own resources: delete in reverse creation order
/// (honoring DeletionPolicy: Retain), cascade into its AWS::CloudFormation::Stack
/// children, drop its registered exports, and clear the resources. The
/// record stays (tombstoning is the caller's terminal-status choice).
fn unwind_stack_resources(store: &mut store::CfnStore, pos: usize, account: &str, region: &str) {
    let snapshot: Vec<CfnResource> = store.stacks[pos].resources.clone();
    for resource in snapshot.iter().rev() {
        if resource.deletion_policy == "Retain" {
            continue;
        }
        // Nested stack: cascade the delete into the child stack.
        if resource.resource_type == "AWS::CloudFormation::Stack" {
            if let Some(child_name) = resource.physical_id.clone().filter(|s| !s.is_empty()) {
                cascade_delete_nested_stack(
                    store,
                    &child_name,
                    account,
                    region,
                    "DELETE_COMPLETE",
                    None,
                );
            }
            continue;
        }
        delete_resource(resource, account, region);
    }
    let export_names: Vec<String> = store.stacks[pos]
        .exports
        .iter()
        .map(|(k, _)| k.clone())
        .collect();
    for export_name in export_names {
        store.exports.retain(|(k, _)| k != &export_name);
    }
    store.stacks[pos].resources.clear();
}

/// Cascade-delete a nested stack after unwinding it: unrecorded children
/// cannot leak, so this is the single entry the delete path, the create
/// rollback loop, and a failed nested deploy all share. The child record
/// ends at `terminal` (DELETE_COMPLETE after a delete cascade,
/// ROLLBACK_COMPLETE after a failed create) with its status reason set and
/// its own stack event recorded.
fn cascade_delete_nested_stack(
    store: &mut store::CfnStore,
    child_name: &str,
    account: &str,
    region: &str,
    terminal: &str,
    reason: Option<&str>,
) {
    let Some(pos) = store.stacks.iter().position(|s| s.stack_name == child_name) else {
        return;
    };
    unwind_stack_resources(store, pos, account, region);
    let child = &mut store.stacks[pos];
    child.status = terminal.to_string();
    if let Some(r) = reason {
        child.status_reason = r.to_string();
    }
    add_stack_event(child, terminal, reason.map(String::from));
}

/// Resolve outputs (`Value` + optional `Export`), honoring `Condition`.
fn resolve_outputs(
    store: &mut store::CfnStore,
    stack: &mut CfnStack,
    template: &Value,
    ctx: &ResolveCtx,
    region: &str,
) {
    let Some(outputs) = template.get("Outputs").and_then(Value::as_object) else {
        return;
    };
    let region = region.to_string();
    for (out_name, out_def) in outputs {
        if let Some(cond_name) = out_def.get("Condition").and_then(Value::as_str) {
            if !ctx.conditions.get(cond_name).copied().unwrap_or(true) {
                continue;
            }
        }
        let Some(value_src) = out_def.get("Value") else {
            continue;
        };
        let value = engine::resolve_intrinsics(value_src, ctx, 0)
            .map(|v| scalar_text(&v))
            .unwrap_or_default();
        let mut member = Map::new();
        let _ = member.insert("OutputKey".to_string(), Value::String(out_name.clone()));
        let _ = member.insert("OutputValue".to_string(), Value::String(value.clone()));
        if let Some(desc) = out_def.get("Description").and_then(Value::as_str) {
            let _ = member.insert("Description".to_string(), Value::String(desc.to_string()));
        }
        if let Some(export) = out_def.get("Export").and_then(|e| e.get("Name")) {
            let export_name = engine::resolve_intrinsics(export, ctx, 0)
                .map(|v| scalar_text(&v))
                .unwrap_or_default();
            let _ = member.insert("ExportName".to_string(), Value::String(export_name.clone()));
            stack.exports.push((export_name.clone(), value.clone()));
            store.put_export(&export_name, &value, &stack.stack_id);
        }
        stack
            .outputs
            .push((out_name.clone(), Value::Object(member)));
        let _ = &region;
    }
}

// ---------------------------------------------------------------------------
// Stack CRUD
// ---------------------------------------------------------------------------

fn create_stack(params: &HashMap<String, String>, region: &str, account: &str) -> OpResult {
    let account_num: u64 = account.parse().unwrap_or(123456789012);
    let name = param(params, "StackName").to_string();
    let template_body = param(params, "TemplateBody").to_string();
    if name.is_empty() {
        return Err(validation_error("StackName is required"));
    }
    if template_body.is_empty() {
        return Err(validation_error("TemplateBody is required"));
    }

    let template = engine::parse_template(&template_body).map_err(validation_error)?;
    let mut template = template;
    expand_sam(&mut template);
    let description = template
        .get("Description")
        .and_then(Value::as_str)
        .unwrap_or("")
        .to_string();
    if template
        .get("Resources")
        .and_then(Value::as_object)
        .map(|m| m.is_empty())
        != Some(false)
    {
        return Err(validation_error(
            "Template must contain a Resources section",
        ));
    }
    let cfn_params: HashMap<String, String> = user_parameters(params).into_iter().collect();
    validate_parameters(&template, &cfn_params)?;
    let tags = key_values(params, "Tags");
    engine::build_dependency_order(&template).map_err(validation_error)?;
    let import_refs: Vec<String> = {
        let mut out = Vec::new();
        find_imports(template.get("Resources").unwrap_or(&Value::Null), &mut out);
        out
    };
    let region_owned = region.to_string();
    let account_owned = account.to_string();

    with_store(account_num, region, |store| {
        if let Some(existing) = store.find_stack(&name) {
            if existing.status != "DELETE_COMPLETE" {
                return Err(CfnError::new(
                    "AlreadyExistsException",
                    format!("Stack [{name}] already exists"),
                ));
            }
        }
        // Duplicate exports check (outputs resolved without resources).
        let ctx = make_ctx(
            &CfnStack::new(String::new(), name.clone(), String::new()),
            &template,
            region,
            account,
            HashMap::new(),
        );
        if let Some(outputs) = template.get("Outputs").and_then(Value::as_object) {
            for out_def in outputs.values() {
                if let Some(export) = out_def.get("Export").and_then(|e| e.get("Name")) {
                    let export_name = match export {
                        Value::String(s) => s.clone(),
                        other => engine::resolve_intrinsics(other, &ctx, 0)
                            .map(|v| scalar_text(&v))
                            .unwrap_or_default(),
                    };
                    if !export_name.is_empty() && store.export_exists(&export_name) {
                        return Err(validation_error(format!(
                            "Export with name '{export_name}' is already exported by another stack"
                        )));
                    }
                }
            }
        }
        for name in &import_refs {
            if !store.export_exists(name) {
                return Err(validation_error(format!(
                    "No export named '{name}' found. Rollback requested by user."
                )));
            }
        }

        let stack_id = crate::services::cloudformation::store::new_stack_id(
            &name,
            &region_owned,
            &account_owned,
        );
        let mut stack = CfnStack::new(stack_id.clone(), name.clone(), template_body.clone());
        stack.parameters = user_parameters(params);
        stack.tags = tags.clone();
        stack.description = description.clone();
        add_event(
            &mut stack,
            &name,
            "AWS::CloudFormation::Stack",
            &stack_id,
            "CREATE_IN_PROGRESS",
            None,
        );

        let mut template_clone = template.clone();
        match deploy_stack(
            store,
            &mut stack,
            &mut template_clone,
            region,
            account,
            account_num,
        ) {
            Ok(()) => {
                stack.status = "CREATE_COMPLETE".to_string();
                add_event(
                    &mut stack,
                    &name,
                    "AWS::CloudFormation::Stack",
                    &stack_id,
                    "CREATE_COMPLETE",
                    None,
                );
            }
            Err(reason) => {
                // Rollback: delete what was created (reverse order). Nested
                // stacks unwind through the shared cascade helper, so a
                // failing resource after a nested deploy cannot orphan the
                // child's resources (real CFN deletes nested stacks during
                // rollback).
                let created: Vec<CfnResource> = stack.resources.clone();
                stack.resources.clear();
                for resource in created.iter().rev() {
                    let phys = resource.physical_id.clone().unwrap_or_default();
                    add_event(
                        &mut stack,
                        &resource.logical_id,
                        &resource.resource_type,
                        &phys,
                        "DELETE_IN_PROGRESS",
                        None,
                    );
                    // Nested stack: cascade the delete into the child stack.
                    if resource.resource_type == "AWS::CloudFormation::Stack" {
                        if let Some(child_name) =
                            resource.physical_id.clone().filter(|s| !s.is_empty())
                        {
                            cascade_delete_nested_stack(
                                store,
                                &child_name,
                                account,
                                region,
                                "DELETE_COMPLETE",
                                None,
                            );
                        }
                    } else {
                        delete_resource(resource, account, region);
                    }
                    add_event(
                        &mut stack,
                        &resource.logical_id,
                        &resource.resource_type,
                        &phys,
                        "DELETE_COMPLETE",
                        None,
                    );
                }
                stack.resources.clear();
                stack.status = "ROLLBACK_COMPLETE".to_string();
                stack.status_reason = reason.clone();
                add_event(
                    &mut stack,
                    &name,
                    "AWS::CloudFormation::Stack",
                    &stack_id,
                    "ROLLBACK_COMPLETE",
                    Some(reason),
                );
            }
        }
        let stack_id_out = stack.stack_id.clone();
        store.stacks.push(stack);
        Ok(xml::ok(
            "CreateStackResponse",
            &format!("<StackId>{}</StackId>", stack_id_out),
        ))
    })
}

fn update_stack(params: &HashMap<String, String>, region: &str, account: &str) -> OpResult {
    let account_num: u64 = account.parse().unwrap_or(123456789012);
    let name = param(params, "StackName").to_string();
    let template_body = param(params, "TemplateBody").to_string();
    if name.is_empty() {
        return Err(validation_error("StackName is required"));
    }
    if template_body.is_empty() {
        return Err(validation_error("TemplateBody is required"));
    }
    let new_params = user_parameters(params);
    let tags = key_values(params, "Tags");

    with_store(account_num, region, |store| {
        let pos = store.stacks.iter().position(|s| {
            s.stack_id == name || (s.stack_name == name && s.status != "DELETE_COMPLETE")
        });
        let Some(pos) = pos else {
            return Err(validation_error(format!("Stack [{name}] does not exist")).status(404));
        };
        if store.stacks[pos].status == "DELETE_COMPLETE" {
            return Err(validation_error(format!("Stack [{name}] does not exist")).status(404));
        }

        let old_template_body = store.stacks[pos].template_body.clone();
        if old_template_body == template_body && new_params.is_empty() && tags.is_empty() {
            return Err(validation_error("No updates are to be performed."));
        }
        // Preserve previous non-pseudo parameters.
        let previous: Vec<(String, String)> = store.stacks[pos]
            .parameters
            .iter()
            .filter(|(k, _)| !is_pseudo(k))
            .cloned()
            .collect();
        let mut merged = previous.clone();
        for (k, v) in &new_params {
            merged.retain(|(ek, _)| ek != k);
            merged.push((k.clone(), v.clone()));
        }

        // Parse both templates for add/remove/modify.
        let old_template_body = store.stacks[pos].template_body.clone();
        let old_resources: Map<String, Value> = engine::parse_template(&old_template_body)
            .ok()
            .and_then(|t| {
                let mut t2 = t;
                expand_sam(&mut t2);
                t2.get("Resources").and_then(Value::as_object).cloned()
            })
            .unwrap_or_default();
        let mut new_template_val =
            engine::parse_template(&template_body).map_err(validation_error)?;
        let new_resources: Map<String, Value> = new_template_val
            .get("Resources")
            .and_then(Value::as_object)
            .cloned()
            .unwrap_or_default();

        let stack_id_cached = store.stacks[pos].stack_id.clone();
        let mut stack = std::mem::replace(
            &mut store.stacks[pos],
            CfnStack::new(String::new(), String::new(), String::new()),
        );
        stack.status = "UPDATE_IN_PROGRESS".to_string();
        add_event(
            &mut stack,
            &name,
            "AWS::CloudFormation::Stack",
            &stack_id_cached.clone(),
            "UPDATE_IN_PROGRESS",
            None,
        );

        let result = (|| -> Result<(), String> {
            // Diff by resource definition equality (old parse done above).
            swap_remove_changed(
                store,
                &mut stack,
                &old_resources,
                &new_resources,
                region,
                account,
            )?;
            stack.template_body = template_body.clone();
            stack.parameters = merged.clone();
            if !tags.is_empty() {
                stack.tags = tags.clone();
            }
            stack.outputs.clear();
            stack.exports.clear();
            deploy_stack(
                store,
                &mut stack,
                &mut new_template_val,
                region,
                account,
                account_num,
            )
        })();

        match result {
            Ok(()) => {
                stack.status = "UPDATE_COMPLETE".to_string();
                add_stack_event(&mut stack, "UPDATE_COMPLETE", None);
            }
            Err(reason) => {
                stack.status = "UPDATE_ROLLBACK_COMPLETE".to_string();
                stack.status_reason = reason.clone();
                add_stack_event(&mut stack, "UPDATE_ROLLBACK_COMPLETE", Some(reason));
            }
        }
        let stack_id = stack.stack_id.clone();
        store.stacks[pos] = stack;
        Ok(xml::ok(
            "UpdateStackResponse",
            &format!("<StackId>{stack_id}</StackId>"),
        ))
    })
}

/// Delete definitions that changed or vanished, then let the deploy step
/// (re)create them. `swap_remove_changed` mirrors the Python diff loop.
fn swap_remove_changed(
    _store: &mut store::CfnStore,
    stack: &mut CfnStack,
    old_defs: &Map<String, Value>,
    new_defs: &Map<String, Value>,
    region: &str,
    account: &str,
) -> Result<(), String> {
    let removed: Vec<String> = old_defs
        .keys()
        .filter(|k| !new_defs.contains_key(*k))
        .cloned()
        .collect();
    let changed: Vec<String> = old_defs
        .iter()
        .filter(|(k, v)| new_defs.get(*k).map(|nv| nv != *v).unwrap_or(false))
        .map(|(k, _)| k.clone())
        .collect();
    for logical_id in removed.into_iter().chain(changed) {
        if let Some(index) = stack
            .resources
            .iter()
            .position(|r| r.logical_id == logical_id)
        {
            let resource = stack.resources.remove(index);
            delete_resource(&resource, account, region);
        }
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// Delete + describes
// ---------------------------------------------------------------------------

fn delete_stack_action(params: &HashMap<String, String>, region: &str, account: &str) -> OpResult {
    let account_num: u64 = account.parse().unwrap_or(123456789012);
    let name = param(params, "StackName").to_string();
    with_store(account_num, region, |store| {
        // Prefer a live record over a stale DELETE_COMPLETE one, matching
        // `find_stack`'s name resolution — otherwise a deleted-then-remade
        // stack delete hits the tombstone and the live stack leaks.
        let pos = store
            .stacks
            .iter()
            .position(|s| {
                (s.stack_id == name || s.stack_name == name) && s.status != "DELETE_COMPLETE"
            })
            .or_else(|| {
                store
                    .stacks
                    .iter()
                    .position(|s| s.stack_id == name || s.stack_name == name)
            });
        let Some(pos) = pos else {
            // Python provider: deleting an unknown stack is a no-op.
            return Ok(xml::empty("DeleteStackResponse"));
        };

        // Block deletion when other live stacks import these exports — the
        // parent's own and its nested stacks' children alike.
        check_import_references(store, &store.stacks[pos])?;
        let child_names: Vec<String> = store.stacks[pos]
            .resources
            .iter()
            .filter(|r| r.resource_type == "AWS::CloudFormation::Stack")
            .filter_map(|r| r.physical_id.clone().filter(|s| !s.is_empty()))
            .collect();
        for child_name in &child_names {
            if let Some(cpos) = store
                .stacks
                .iter()
                .position(|s| s.stack_name == *child_name)
            {
                check_import_references(store, &store.stacks[cpos])?;
            }
        }

        // Unwind through the shared cascade core: reverse-order resource
        // deletes, nested children cascaded, the stack's own exports
        // removed, resources cleared. Then tombstone the record.
        unwind_stack_resources(store, pos, account, region);
        store.stacks[pos].status = "DELETE_COMPLETE".to_string();
        Ok(xml::empty("DeleteStackResponse"))
    })
}

fn stack_json(stack: &CfnStack) -> Value {
    let mut member = Map::new();
    let _ = member.insert("StackId".to_string(), Value::String(stack.stack_id.clone()));
    let _ = member.insert(
        "StackName".to_string(),
        Value::String(stack.stack_name.clone()),
    );
    let _ = member.insert(
        "StackStatus".to_string(),
        Value::String(stack.status.clone()),
    );
    let _ = member.insert(
        "CreationTime".to_string(),
        Value::String(xml::timestamp_from(stack.created)),
    );
    if !stack.description.is_empty() {
        let _ = member.insert(
            "Description".to_string(),
            Value::String(stack.description.clone()),
        );
    }
    if !stack.status_reason.is_empty() {
        let _ = member.insert(
            "StackStatusReason".to_string(),
            Value::String(stack.status_reason.clone()),
        );
    }
    if !stack.outputs.is_empty() {
        let arr: Vec<Value> = stack.outputs.iter().map(|(_, v)| v.clone()).collect();
        let _ = member.insert("Outputs".to_string(), Value::Array(arr));
    }
    let visible: Vec<Value> = stack
        .parameters
        .iter()
        .filter(|(k, _)| !is_pseudo(k))
        .map(|(k, v)| json!({"ParameterKey": k, "ParameterValue": v}))
        .collect();
    if !visible.is_empty() {
        let _ = member.insert("Parameters".to_string(), Value::Array(visible));
    }
    if !stack.tags.is_empty() {
        let tags: Vec<Value> = stack
            .tags
            .iter()
            .map(|(k, v)| json!({"Key": k, "Value": v}))
            .collect();
        let _ = member.insert("Tags".to_string(), Value::Array(tags));
    }
    let _ = member.insert(
        "EnableTerminationProtection".to_string(),
        Value::String(stack.enable_termination_protection.to_string()),
    );
    Value::Object(member)
}

fn describe_stacks(params: &HashMap<String, String>, account_num: u64, region: &str) -> OpResult {
    let name = param(params, "StackName");
    with_store(account_num, region, |store| {
        let members: Vec<Value> = if !name.is_empty() {
            let stack = store
                .find_stack(name)
                .filter(|s| {
                    // Only the ARN (stack id) resolves deleted stacks.
                    !(s.status == "DELETE_COMPLETE" && s.stack_id != name)
                })
                .ok_or_else(|| validation_error(format!("Stack with id {name} does not exist")))?;
            vec![stack_json(stack)]
        } else {
            store
                .stacks
                .iter()
                .filter(|s| s.status != "DELETE_COMPLETE")
                .map(stack_json)
                .collect()
        };
        Ok(xml::ok(
            "DescribeStacksResponse",
            &xml::dict_to_xml(&json!({"Stacks": members})),
        ))
    })
}

fn list_stacks(params: &HashMap<String, String>, account_num: u64, region: &str) -> OpResult {
    let mut filter: Vec<String> = Vec::new();
    let mut i = 1;
    while let Some(v) = params.get(&format!("StackStatusFilter.member.{i}")) {
        filter.push(v.clone());
        i += 1;
    }
    with_store(account_num, region, |store| {
        // moto order: live records first (`self.stacks`), then the deleted
        // ones (`self.deleted_stacks`) — so name lookups over the list see
        // the live stack before any DELETE_COMPLETE tombstone.
        let mut live_summaries: Vec<Value> = Vec::new();
        let mut deleted_summaries: Vec<Value> = Vec::new();
        for s in store.stacks.iter() {
            if !filter.is_empty() && !filter.contains(&s.status) {
                continue;
            }
            let summary = json!({
                "StackId": s.stack_id,
                "StackName": s.stack_name,
                "StackStatus": s.status,
                "CreationTime": xml::timestamp_from(s.created),
            });
            if s.status == "DELETE_COMPLETE" {
                deleted_summaries.push(summary);
            } else {
                live_summaries.push(summary);
            }
        }
        live_summaries.extend(deleted_summaries);
        let summaries = live_summaries;
        Ok(xml::ok(
            "ListStacksResponse",
            &xml::dict_to_xml(&json!({"StackSummaries": summaries})),
        ))
    })
}

fn resource_json(stack: &CfnStack, resource: &CfnResource) -> Value {
    json!({
        "LogicalResourceId": resource.logical_id,
        "PhysicalResourceId": resource.physical_id.clone().unwrap_or_default(),
        "ResourceType": resource.resource_type,
        "ResourceStatus": resource.status,
        "Timestamp": xml::timestamp_from(stack.created),
    })
}

fn describe_stack_resources(
    params: &HashMap<String, String>,
    account_num: u64,
    region: &str,
) -> OpResult {
    let name = param(params, "StackName");
    with_store(account_num, region, |store| {
        let stack = store
            .find_stack(name)
            .ok_or_else(|| validation_error(format!("Stack [{name}] does not exist")))?;
        let resources: Vec<Value> = stack
            .resources
            .iter()
            .map(|r| resource_json(stack, r))
            .collect();
        Ok(xml::ok(
            "DescribeStackResourcesResponse",
            &xml::dict_to_xml(&json!({"StackResources": resources})),
        ))
    })
}

fn describe_stack_resource(
    params: &HashMap<String, String>,
    account_num: u64,
    region: &str,
) -> OpResult {
    let name = param(params, "StackName");
    let logical_id = param(params, "LogicalResourceId").to_string();
    with_store(account_num, region, |store| {
        let stack = store
            .find_stack(name)
            .ok_or_else(|| validation_error(format!("Stack [{name}] does not exist")))?;
        let resource = stack.resource(&logical_id).ok_or_else(|| {
            validation_error(format!(
                "Resource [{logical_id}] does not exist in stack [{name}]"
            ))
        })?;
        let detail = resource_json(stack, resource);
        Ok(xml::ok(
            "DescribeStackResourceResponse",
            &xml::dict_to_xml(&json!({"StackResourceDetail": detail})),
        ))
    })
}

fn describe_stack_events(
    params: &HashMap<String, String>,
    account_num: u64,
    region: &str,
) -> OpResult {
    let name = param(params, "StackName");
    with_store(account_num, region, |store| {
        let stack = store
            .find_stack(name)
            .ok_or_else(|| validation_error(format!("Stack [{name}] does not exist")))?;
        let events: Vec<Value> = stack.events.iter().rev().map(|e| e.to_json()).collect();
        Ok(xml::ok(
            "DescribeStackEventsResponse",
            &xml::dict_to_xml(&json!({"StackEvents": events})),
        ))
    })
}

fn list_stack_resources(
    params: &HashMap<String, String>,
    account_num: u64,
    region: &str,
) -> OpResult {
    let name = param(params, "StackName");
    with_store(account_num, region, |store| {
        let stack = store
            .find_stack(name)
            .ok_or_else(|| validation_error(format!("Stack [{name}] does not exist")))?;
        let summaries: Vec<Value> = stack
            .resources
            .iter()
            .map(|r| {
                let mut m = resource_json(stack, r)
                    .as_object()
                    .cloned()
                    .unwrap_or_default();
                let _ = m.clone();
                m.insert(
                    "LastUpdatedTimestamp".to_string(),
                    Value::String(xml::timestamp_from(stack.created)),
                );
                Value::Object(m)
            })
            .collect();
        Ok(xml::ok(
            "ListStackResourcesResponse",
            &xml::dict_to_xml(&json!({"StackResourceSummaries": summaries})),
        ))
    })
}

fn list_exports(account_num: u64, region: &str) -> OpResult {
    with_store(account_num, region, |store| {
        let exports: Vec<Value> = store
            .exports
            .iter()
            .map(|(name, entry)| {
                json!({
                    "ExportingStackId": entry.stack_id,
                    "Name": name,
                    "Value": entry.value,
                })
            })
            .collect();
        Ok(xml::ok(
            "ListExportsResponse",
            &xml::dict_to_xml(&json!({"Exports": exports})),
        ))
    })
}

fn get_template(params: &HashMap<String, String>, account_num: u64, region: &str) -> OpResult {
    let name = param(params, "StackName");
    with_store(account_num, region, |store| {
        let stack = store
            .find_stack(name)
            .ok_or_else(|| validation_error(format!("Stack [{name}] does not exist")))?;
        Ok(xml::ok(
            "GetTemplateResponse",
            &xml::dict_to_xml(&json!({ "TemplateBody": stack.template_body })),
        ))
    })
}

fn validate_template(params: &HashMap<String, String>) -> OpResult {
    let template_body = param(params, "TemplateBody").to_string();
    if template_body.is_empty() {
        return Err(validation_error("TemplateBody is required"));
    }
    let template = engine::parse_template(&template_body).map_err(validation_error)?;
    if template
        .get("Resources")
        .and_then(Value::as_object)
        .map(|m| m.is_empty())
        != Some(false)
    {
        return Err(validation_error(
            "Template must contain a Resources section",
        ));
    }
    let mut members: Vec<Value> = Vec::new();
    if let Some(pdefs) = template.get("Parameters").and_then(Value::as_object) {
        for (pname, pdef) in pdefs {
            let mut m = Map::new();
            let _ = m.insert("ParameterKey".to_string(), Value::String(pname.clone()));
            if let Some(d) = pdef.get("Default") {
                let _ = m.insert("DefaultValue".to_string(), Value::String(scalar_text(d)));
            }
            if let Some(t) = pdef.get("Type").and_then(Value::as_str) {
                let _ = m.insert("ParameterType".to_string(), Value::String(t.to_string()));
            }
            if let Some(d) = pdef.get("Description").and_then(Value::as_str) {
                let _ = m.insert("Description".to_string(), Value::String(d.to_string()));
            }
            let _ = m.insert("NoEcho".to_string(), Value::String("false".to_string()));
            members.push(Value::Object(m));
        }
    }
    let mut result = json!({"Parameters": members});
    if let Some(Value::String(desc)) = template.get("Description") {
        let _ = result
            .as_object_mut()
            .unwrap()
            .insert("Description".to_string(), Value::String(desc.clone()));
    }
    Ok(xml::ok(
        "ValidateTemplateResponse",
        &xml::dict_to_xml(&result),
    ))
}

// ---------------------------------------------------------------------------
// Change sets
// ---------------------------------------------------------------------------

fn create_change_set(params: &HashMap<String, String>, region: &str, account: &str) -> OpResult {
    let account_num: u64 = account.parse().unwrap_or(123456789012);
    let stack_name = param(params, "StackName").to_string();
    let cs_name = param(params, "ChangeSetName").to_string();
    let template_body = param(params, "TemplateBody").to_string();
    let cs_type = param(params, "ChangeSetType").to_string();
    let cs_type = if cs_type.is_empty() {
        "UPDATE".to_string()
    } else {
        cs_type
    };
    if stack_name.is_empty() {
        return Err(validation_error("StackName is required"));
    }
    if cs_name.is_empty() {
        return Err(validation_error("ChangeSetName is required"));
    }
    let region_owned = region.to_string();
    let account_owned = account.to_string();
    with_store(account_num, region, |store| {
        let cs_id = crate::services::cloudformation::store::new_change_set_id(
            &cs_name,
            &region_owned,
            &account_owned,
        );
        let existing = store.find_stack(&stack_name).map(|s| s.stack_id.clone());
        let stack_id = existing.clone().unwrap_or_else(|| {
            crate::services::cloudformation::store::new_stack_id(
                &stack_name,
                &region_owned,
                &account_owned,
            )
        });
        if existing.is_none() && cs_type == "CREATE" {
            let mut stub =
                CfnStack::new(stack_id.clone(), stack_name.clone(), template_body.clone());
            stub.status = "REVIEW_IN_PROGRESS".to_string();
            store.stacks.push(stub);
        }
        store.change_sets.push(CfnChangeSet {
            change_set_id: cs_id.clone(),
            change_set_name: cs_name,
            stack_name: stack_name.clone(),
            stack_id: stack_id.clone(),
            template_body,
            status: "CREATE_COMPLETE".to_string(),
            status_reason: String::new(),
            change_set_type: cs_type,
            parameters: user_parameters(params),
            created: chrono::Utc::now().timestamp(),
        });
        Ok(xml::ok(
            "CreateChangeSetResponse",
            &xml::dict_to_xml(&json!({"Id": cs_id, "StackId": stack_id})),
        ))
    })
}

fn store_find_change_set(
    store: &store::CfnStore,
    params: &HashMap<String, String>,
) -> Result<CfnChangeSet, String> {
    let cs_name = param(params, "ChangeSetName");
    let stack_name = param(params, "StackName");
    store
        .find_change_set(cs_name, stack_name)
        .cloned()
        .ok_or_else(|| format!("ChangeSet [{cs_name}] does not exist"))
}

fn describe_change_set(
    params: &HashMap<String, String>,
    account_num: u64,
    region: &str,
) -> OpResult {
    with_store(account_num, region, |store| {
        let cs = store_find_change_set(store, params)
            .map_err(|m| CfnError::new("ChangeSetNotFoundException", m).status(404))?;
        // Compute changes by diffing the templates (Add/Remove/Modify).
        let mut changes: Vec<Value> = Vec::new();
        if !cs.template_body.is_empty() {
            if let Ok(new_template) = engine::parse_template(&cs.template_body) {
                let new_resources: Map<String, Value> = new_template
                    .get("Resources")
                    .and_then(Value::as_object)
                    .cloned()
                    .unwrap_or_default();
                let old_resources: Map<String, Value> = store
                    .find_stack(&cs.stack_name)
                    .filter(|s| !s.template_body.is_empty())
                    .and_then(|s| engine::parse_template(&s.template_body).ok())
                    .and_then(|t| t.get("Resources").and_then(Value::as_object).cloned())
                    .unwrap_or_default();
                for lid in new_resources.keys() {
                    if !old_resources.contains_key(lid) {
                        changes.push(json!({
                            "Type": "Resource",
                            "ResourceChange": {
                                "Action": "Add",
                                "LogicalResourceId": lid,
                                "ResourceType": new_resources[lid].get("Type").and_then(Value::as_str).unwrap_or(""),
                            },
                        }));
                    }
                }
                for lid in old_resources.keys() {
                    if !new_resources.contains_key(lid) {
                        changes.push(json!({
                            "Type": "Resource",
                            "ResourceChange": {
                                "Action": "Remove",
                                "LogicalResourceId": lid,
                                "ResourceType": old_resources[lid].get("Type").and_then(Value::as_str).unwrap_or(""),
                            },
                        }));
                    }
                }
                for (lid, old_def) in &old_resources {
                    if let Some(new_def) = new_resources.get(lid) {
                        if new_def != old_def {
                            changes.push(json!({
                                "Type": "Resource",
                                "ResourceChange": {
                                    "Action": "Modify",
                                    "LogicalResourceId": lid,
                                    "ResourceType": new_def.get("Type").and_then(Value::as_str).unwrap_or(""),
                                },
                            }));
                        }
                    }
                }
            }
        }
        let result = json!({
            "ChangeSetId": cs.change_set_id,
            "ChangeSetName": cs.change_set_name,
            "StackId": cs.stack_id,
            "StackName": cs.stack_name,
            "Status": cs.status,
            "StatusReason": cs.status_reason,
            "Changes": changes,
        });
        Ok(xml::ok(
            "DescribeChangeSetResponse",
            &xml::dict_to_xml(&result),
        ))
    })
}

fn delete_change_set(params: &HashMap<String, String>, account_num: u64, region: &str) -> OpResult {
    with_store(account_num, region, |store| {
        let found = store
            .find_change_set(param(params, "ChangeSetName"), param(params, "StackName"))
            .map(|c| c.change_set_id.clone());
        if let Some(id) = found {
            store.change_sets.retain(|c| c.change_set_id != id);
        }
        Ok(xml::empty("DeleteChangeSetResponse"))
    })
}

fn execute_change_set(params: &HashMap<String, String>, region: &str, account: &str) -> OpResult {
    let account_num: u64 = account.parse().unwrap_or(123456789012);
    with_store(account_num, region, |store| {
        let cs = store_find_change_set(store, params)
            .map_err(|m| CfnError::new("ChangeSetNotFoundException", m).status(404))?;
        if cs.status == "EXECUTE_COMPLETE" {
            return Err(CfnError::new(
                "InvalidChangeSetStatusException",
                format!(
                    "ChangeSet [{cs_id}] is in EXECUTE_COMPLETE state and cannot be executed.",
                    cs_id = cs.change_set_id
                ),
            ));
        }
        let stack_pos = store
            .stacks
            .iter()
            .position(|s| s.stack_id == cs.stack_id || s.stack_name == cs.stack_name);
        let Some(stack_pos) = stack_pos else {
            return Err(validation_error(format!(
                "Stack [{}] does not exist",
                cs.stack_name
            )));
        };
        let mut stack = std::mem::replace(
            &mut store.stacks[stack_pos],
            CfnStack::new(String::new(), String::new(), String::new()),
        );

        if cs.change_set_type == "CREATE" {
            stack.template_body = cs.template_body.clone();
            stack.parameters = cs.parameters.clone();
            stack.status = "CREATE_IN_PROGRESS".to_string();
            add_stack_event(&mut stack, "CREATE_IN_PROGRESS", None);
            let mut template = match engine::parse_template(&cs.template_body) {
                Ok(t) => t,
                Err(e) => {
                    store.stacks[stack_pos] = stack;
                    return Err(validation_error(e));
                }
            };
            match deploy_stack(
                store,
                &mut stack,
                &mut template,
                region,
                account,
                account_num,
            ) {
                Ok(()) => {
                    stack.status = "CREATE_COMPLETE".to_string();
                    add_stack_event(&mut stack, "CREATE_COMPLETE", None);
                }
                Err(reason) => {
                    for resource in stack.resources.iter().rev() {
                        delete_resource(resource, account, region);
                    }
                    stack.resources.clear();
                    stack.status = "ROLLBACK_COMPLETE".to_string();
                    stack.status_reason = reason.clone();
                    add_stack_event(&mut stack, "ROLLBACK_COMPLETE", Some(reason));
                }
            }
        } else {
            stack.status = "UPDATE_IN_PROGRESS".to_string();
            add_stack_event(&mut stack, "UPDATE_IN_PROGRESS", None);
            // Replace: delete everything, then deploy the new template.
            for resource in stack.resources.iter().rev() {
                delete_resource(resource, account, region);
            }
            stack.resources.clear();
            stack.outputs.clear();
            stack.exports.clear();
            stack.template_body = cs.template_body.clone();
            stack.parameters = cs.parameters.clone();
            let mut template = match engine::parse_template(&cs.template_body) {
                Ok(t) => t,
                Err(e) => {
                    store.stacks[stack_pos] = stack;
                    return Err(validation_error(e));
                }
            };
            match deploy_stack(
                store,
                &mut stack,
                &mut template,
                region,
                account,
                account_num,
            ) {
                Ok(()) => {
                    stack.status = "UPDATE_COMPLETE".to_string();
                    add_stack_event(&mut stack, "UPDATE_COMPLETE", None);
                }
                Err(reason) => {
                    stack.status = "UPDATE_FAILED".to_string();
                    stack.status_reason = reason.clone();
                    add_stack_event(&mut stack, "UPDATE_FAILED", Some(reason));
                }
            }
        }
        store.stacks[stack_pos] = stack;
        let cs_id = cs.change_set_id.clone();
        if let Some(change_set) = store
            .change_sets
            .iter_mut()
            .find(|c| c.change_set_id == cs_id)
        {
            change_set.status = "EXECUTE_COMPLETE".to_string();
        }
        Ok(xml::empty("ExecuteChangeSetResponse"))
    })
}

// ---------------------------------------------------------------------------
// Stack sets + registry/drift stubs (moto wire shapes)
// ---------------------------------------------------------------------------

fn stack_set_json(ss: &StackSet) -> Value {
    let tags: Vec<Value> = ss
        .tags
        .iter()
        .map(|(k, v)| json!({"Key": k, "Value": v}))
        .collect();
    json!({
        "StackSetId": ss.id,
        "StackSetName": ss.name,
        "Description": if ss.description.is_empty() { Value::Null } else { Value::String(ss.description.clone()) },
        "Status": ss.status.clone(),
        "PermissionModel": if ss.permission_model.is_empty() { Value::Null } else { Value::String(ss.permission_model.clone()) },
        "Tags": tags,
    })
}

/// Stack sets are stored in the (account, region) that issued the call.
fn with_stack_set<T>(
    account_num: u64,
    region: &str,
    name: &str,
    f: impl FnOnce(&mut stack_mut_alias::CfnStore, &mut StackSet) -> Result<T, CfnError>,
) -> Result<T, CfnError> {
    with_store(account_num, region, |store| {
        let pos = store
            .stack_sets
            .iter()
            .position(|ss| ss.name == name || ss.id == name)
            .filter(|&pos| store.stack_sets[pos].status != "DELETED")
            .or_else(|| {
                store
                    .stack_sets
                    .iter()
                    .position(|ss| (ss.name == name || ss.id == name) && ss.status != "DELETED")
            });
        let Some(pos) = pos else {
            return Err(CfnError::new(
                "StackSetNotFoundException",
                format!("StackSet {name} not found"),
            ));
        };
        let mut ss = std::mem::take(&mut store.stack_sets[pos]);
        let outcome = f(store, &mut ss);
        store.stack_sets[pos] = ss;
        outcome
    })
}

mod stack_mut_alias {
    pub type CfnStore = crate::services::cloudformation::store::CfnStore;
}

fn create_operation(
    ss: &mut StackSet,
    action: &str,
    accounts: &[String],
    regions: &[String],
) -> String {
    let operation_id = Uuid::new_v4().to_string();
    let instances: Vec<(String, String)> = accounts
        .iter()
        .flat_map(|a| regions.iter().map(move |r| (a.clone(), r.clone())))
        .collect();
    ss.operations.push(StackSetOperation {
        operation_id: operation_id.clone(),
        action: action.to_string(),
        status: "SUCCEEDED".to_string(),
        creation_timestamp: now_ts(),
        instances,
    });
    operation_id
}

fn parse_accounts_regions(params: &HashMap<String, String>) -> (Vec<String>, Vec<String>) {
    let mut accounts = Vec::new();
    let mut i = 1;
    while let Some(v) = params.get(&format!("Accounts.member.{i}")) {
        accounts.push(v.clone());
        i += 1;
    }
    let mut regions = Vec::new();
    let mut i = 1;
    while let Some(v) = params.get(&format!("Regions.member.{i}")) {
        regions.push(v.clone());
        i += 1;
    }
    (accounts, regions)
}

fn create_stack_set(params: &HashMap<String, String>, region: &str, account: &str) -> OpResult {
    let account_num: u64 = account.parse().unwrap_or(123456789012);
    let name = param(params, "StackSetName").to_string();
    let tag_list = key_values(params, "Tags");
    with_store(account_num, region, |store| {
        let ss = StackSet {
            id: format!("stackset-{name}-{}", account_num),
            name: name.clone(),
            template_body: param(params, "TemplateBody").to_string(),
            description: param(params, "Description").to_string(),
            tags: tag_list,
            permission_model: param(params, "PermissionModel").to_string(),
            parameters: user_parameters(params),
            status: "ACTIVE".to_string(),
            instances: Vec::new(),
            operations: Vec::new(),
        };
        let id = ss.id.clone();
        store.stack_sets.push(ss);
        Ok(xml::ok(
            "CreateStackSetResponse",
            &xml::dict_to_xml(&json!({"StackSetId": id})),
        ))
    })
}

fn update_stack_set(params: &HashMap<String, String>, account_num: u64, region: &str) -> OpResult {
    let name = param(params, "StackSetName").to_string();
    with_stack_set(account_num, region, &name, |_store, ss| {
        if let Some(desc) = params.get("Description") {
            ss.description = desc.clone();
        }
        if let Some(body) = params.get("TemplateBody") {
            if !body.is_empty() {
                ss.template_body = body.clone();
            }
        }
        let tags = key_values(params, "Tags");
        if !tags.is_empty() {
            ss.tags = tags;
        }
        let op = create_operation(ss, "UPDATE", &[], &[]);
        Ok(xml::ok(
            "UpdateStackSetResponse",
            &xml::dict_to_xml(&json!({"OperationId": op})),
        ))
    })
}

fn delete_stack_set(params: &HashMap<String, String>, account_num: u64, region: &str) -> OpResult {
    let name = param(params, "StackSetName").to_string();
    with_store(account_num, region, |store| {
        if let Some(pos) = store
            .stack_sets
            .iter()
            .position(|ss| ss.name == name || ss.id == name)
        {
            if !store.stack_sets[pos].instances.is_empty() {
                return Err(CfnError::new(
                    "StackSetNotEmptyException",
                    "StackSet is not empty",
                ));
            }
            store.stack_sets[pos].status = "DELETED".to_string();
        }
        Ok(xml::empty("DeleteStackSetResponse"))
    })
}

fn describe_stack_set(
    params: &HashMap<String, String>,
    account_num: u64,
    region: &str,
) -> OpResult {
    let name = param(params, "StackSetName");
    with_store(account_num, region, |store| {
        let ss = store
            .stack_sets
            .iter()
            .find(|ss| (ss.name == name || ss.id == name) && ss.status != "DELETED")
            .ok_or_else(|| {
                CfnError::new(
                    "StackSetNotFoundException",
                    format!("StackSet {name} not found"),
                )
            })?;
        Ok(xml::ok(
            "DescribeStackSetResponse",
            &xml::dict_to_xml(&json!({"StackSet": stack_set_json(ss)})),
        ))
    })
}

fn list_stack_sets(params: &HashMap<String, String>, account_num: u64, region: &str) -> OpResult {
    let status_filter = param(params, "Status");
    with_store(account_num, region, |store| {
        let summaries: Vec<Value> = store
            .stack_sets
            .iter()
            .filter(|ss| status_filter.is_empty() || status_filter == ss.status.as_str())
            .map(|ss| {
                json!({
                    "StackSetId": ss.id,
                    "StackSetName": ss.name,
                    "Status": ss.status,
                    "Description": ss.description,
                })
            })
            .collect();
        Ok(xml::ok(
            "ListStackSetsResponse",
            &xml::dict_to_xml(&json!({"Summaries": summaries})),
        ))
    })
}

fn list_stack_instances(
    params: &HashMap<String, String>,
    account_num: u64,
    region: &str,
) -> OpResult {
    let name = param(params, "StackSetName");
    with_store(account_num, region, |store| {
        let found = store
            .stack_sets
            .iter()
            .find(|ss| ss.name == name || ss.id == name);
        let summaries: Vec<Value> = found
            .map(|ss| {
                ss.instances
                    .iter()
                    .map(|inst| inst.to_json(&ss.id, &ss.name))
                    .collect()
            })
            .unwrap_or_default();
        let _ = found;
        Ok(xml::ok(
            "ListStackInstancesResponse",
            &xml::dict_to_xml(&json!({"Summaries": summaries})),
        ))
    })
}

fn create_stack_instances(
    params: &HashMap<String, String>,
    account_num: u64,
    region: &str,
) -> OpResult {
    let name = param(params, "StackSetName").to_string();
    let (accounts, regions) = parse_accounts_regions(params);
    let accounts = if accounts.is_empty() {
        vec![account_num.to_string()]
    } else {
        accounts
    };
    let regions = if regions.is_empty() {
        vec![region.to_string()]
    } else {
        regions
    };
    with_stack_set(account_num, region, &name, |_store, ss| {
        for a in &accounts {
            for r in &regions {
                let stack_id = crate::services::cloudformation::store::new_stack_id(
                    &format!("StackSet-{}", ss.id),
                    r,
                    a,
                );
                let exists = ss
                    .instances
                    .iter()
                    .any(|inst| inst.account == *a && inst.region == *r);
                if !exists {
                    ss.instances.push(StackInstance {
                        account: a.clone(),
                        region: r.clone(),
                        stack_id,
                        parameter_overrides: json!([]),
                    });
                }
            }
        }
        let op = create_operation(ss, "CREATE", &accounts, &regions);
        Ok(xml::ok(
            "CreateStackInstancesResponse",
            &xml::dict_to_xml(&json!({"OperationId": op})),
        ))
    })
}

fn update_stack_instances(
    params: &HashMap<String, String>,
    account_num: u64,
    region: &str,
) -> OpResult {
    let name = param(params, "StackSetName").to_string();
    let (accounts, regions) = parse_accounts_regions(params);
    with_stack_set(account_num, region, &name, |_store, ss| {
        let op = create_operation(ss, "UPDATE", &accounts, &regions);
        Ok(xml::ok(
            "UpdateStackInstancesResponse",
            &xml::dict_to_xml(&json!({"OperationId": op})),
        ))
    })
}

fn delete_stack_instances(
    params: &HashMap<String, String>,
    account_num: u64,
    region: &str,
) -> OpResult {
    let name = param(params, "StackSetName").to_string();
    let (accounts, regions) = parse_accounts_regions(params);
    with_stack_set(account_num, region, &name, |_store, ss| {
        let op = create_operation(ss, "DELETE", &accounts, &regions);
        ss.instances
            .retain(|inst| !accounts.contains(&inst.account) || !regions.contains(&inst.region));
        Ok(xml::ok(
            "DeleteStackInstancesResponse",
            &xml::dict_to_xml(&json!({"OperationId": op})),
        ))
    })
}

fn describe_stack_instance(
    params: &HashMap<String, String>,
    account_num: u64,
    region: &str,
) -> OpResult {
    let name = param(params, "StackSetName");
    let account_filter = param(params, "StackInstanceAccount");
    let region_filter = param(params, "StackInstanceRegion");
    with_store(account_num, region, |store| {
        let ss = store
            .stack_sets
            .iter()
            .find(|ss| ss.name == name || ss.id == name)
            .ok_or_else(|| {
                CfnError::new(
                    "StackSetNotFoundException",
                    format!("StackSet {name} not found"),
                )
            })?;
        let inst = ss
            .instances
            .iter()
            .find(|inst| inst.account == account_filter && inst.region == region_filter)
            .ok_or_else(|| {
                CfnError::new(
                    "StackInstanceNotFoundException",
                    format!("StackInstance {account_filter}/{region_filter} not found"),
                )
            })?;
        Ok(xml::ok(
            "DescribeStackInstanceResponse",
            &xml::dict_to_xml(&json!({"StackInstance": inst.to_json(&ss.id, &ss.name)})),
        ))
    })
}

fn describe_stack_set_operation(
    params: &HashMap<String, String>,
    account_num: u64,
    region: &str,
) -> OpResult {
    let name = param(params, "StackSetName").to_string();
    let op_id = param(params, "OperationId").to_string();
    with_stack_set(account_num, region, &name, |_store, ss| {
        let op = ss
            .operations
            .iter()
            .find(|op| op.operation_id == op_id)
            .ok_or_else(|| validation_error(&op_id))?;
        Ok(xml::ok(
            "DescribeStackSetOperationResponse",
            &xml::dict_to_xml(&json!({
                "StackSetOperation": {
                    "OperationId": op.operation_id,
                    "Action": op.action,
                    "Status": op.status,
                    "CreationTimestamp": op.creation_timestamp,
                },
            })),
        ))
    })
}

fn list_stack_set_operations(
    params: &HashMap<String, String>,
    account_num: u64,
    region: &str,
) -> OpResult {
    let name = param(params, "StackSetName").to_string();
    with_store(account_num, region, |store| {
        let ss = store
            .stack_sets
            .iter()
            .find(|ss| ss.name == name || ss.id == name);
        let summaries: Vec<Value> = ss
            .map(|ss| {
                ss.operations
                    .iter()
                    .map(|op| {
                        json!({
                            "OperationId": op.operation_id,
                            "Action": op.action,
                            "Status": op.status,
                            "CreationTimestamp": op.creation_timestamp,
                        })
                    })
                    .collect()
            })
            .unwrap_or_default();
        Ok(xml::ok(
            "ListStackSetOperationsResponse",
            &xml::dict_to_xml(&json!({"Summaries": summaries})),
        ))
    })
}

fn list_stack_set_operation_results(
    params: &HashMap<String, String>,
    account_num: u64,
    region: &str,
) -> OpResult {
    let name = param(params, "StackSetName").to_string();
    let op_id = param(params, "OperationId").to_string();
    with_store(account_num, region, |store| {
        let ss = store
            .stack_sets
            .iter()
            .find(|ss| ss.name == name || ss.id == name);
        let summaries: Vec<Value> = ss
            .and_then(|ss| ss.operations.iter().find(|op| op.operation_id == op_id))
            .map(|op| {
                op.instances
                    .iter()
                    .map(|(account, region_part)| {
                        json!({
                            "AccountGateResult": {
                                "StatusReason": format!(
                                    "Function not found: arn:aws:lambda:us-west-2:{account}:function:AWSCloudFormationStackSetAccountGate"
                                ),
                                "Status": "SKIPPED",
                            },
                            "Region": region_part,
                            "Account": account,
                            "Status": op.status,
                        })
                    })
                    .collect()
            })
            .unwrap_or_default();
        Ok(xml::ok(
            "ListStackSetOperationResultsResponse",
            &xml::dict_to_xml(&json!({"Summaries": summaries})),
        ))
    })
}

fn stop_stack_set_operation(
    params: &HashMap<String, String>,
    account_num: u64,
    region: &str,
) -> OpResult {
    let name = param(params, "StackSetName").to_string();
    let op_id = param(params, "OperationId").to_string();
    let _ = account_num;
    with_stack_set(account_num, region, &name, |_store, ss| {
        let op = ss
            .operations
            .iter_mut()
            .find(|op| op.operation_id == op_id)
            .ok_or_else(|| validation_error(format!("StackSet operation {op_id} not found")))?;
        op.status = "STOPPED".to_string();
        Ok(xml::empty("StopStackSetOperationResponse"))
    })
}

fn import_stacks_to_stack_set(
    params: &HashMap<String, String>,
    account_num: u64,
    region: &str,
) -> OpResult {
    let name = param(params, "StackSetName").to_string();
    with_stack_set(account_num, region, &name, |_store, ss| {
        let op = create_operation(ss, "IMPORT", &[], &[]);
        Ok(xml::ok(
            "ImportStacksToStackSetResponse",
            &xml::dict_to_xml(&json!({"OperationId": op})),
        ))
    })
}

// --- Stack-scoped validations + stubs requiring the stack -------------------

fn get_stack_policy(params: &HashMap<String, String>, account_num: u64, region: &str) -> OpResult {
    let name = param(params, "StackName");
    with_store(account_num, region, |store| {
        let stack = store
            .find_stack(name)
            .ok_or_else(|| validation_error(format!("Stack: {name} does not exist")))?;
        let body = stack.policy.clone().unwrap_or_default();
        Ok(xml::ok(
            "GetStackPolicyResponse",
            &xml::dict_to_xml(
                &json!({"StackPolicyBody": if body.is_empty() { Value::Null } else { Value::String(body) }}),
            ),
        ))
    })
}

fn set_stack_policy(params: &HashMap<String, String>, account_num: u64, region: &str) -> OpResult {
    let name = param(params, "StackName").to_string();
    let body = param(params, "StackPolicyBody").to_string();
    with_store(account_num, region, |store| {
        let pos = find_stack_pos(store, &name);
        let Some(pos) = pos else {
            return Err(validation_error(format!("Stack: {name} does not exist")));
        };
        store.stacks[pos].policy = Some(body);
        Ok(xml::empty("SetStackPolicyResponse"))
    })
}

fn update_termination_protection(
    params: &HashMap<String, String>,
    account_num: u64,
    region: &str,
) -> OpResult {
    let name = param(params, "StackName");
    let enable = matches!(
        param(params, "EnableTerminationProtection"),
        "true" | "True"
    );
    with_store(account_num, region, |store| {
        let pos = store
            .stacks
            .iter()
            .position(|s| s.stack_id == name || s.stack_name == name);
        let Some(pos) = pos else {
            return Err(validation_error(format!("Stack: {name} does not exist")));
        };
        store.stacks[pos].enable_termination_protection = enable;
        let stack_id = store.stacks[pos].stack_id.clone();
        Ok(xml::ok(
            "UpdateTerminationProtectionResponse",
            &xml::dict_to_xml(&json!({"StackId": stack_id})),
        ))
    })
}

fn find_stack_pos(store: &store::CfnStore, name: &str) -> Option<usize> {
    store
        .stacks
        .iter()
        .position(|s| s.stack_id == name || s.stack_name == name)
}

macro_rules! require_stack {
    ($store:expr, $name:expr) => {
        match find_stack_pos($store, $name) {
            Some(pos) => pos,
            None => {
                return Err(validation_error(format!(
                    "Stack with id {} does not exist",
                    $name
                )))
            }
        }
    };
}

fn continue_update_rollback(
    params: &HashMap<String, String>,
    account_num: u64,
    region: &str,
) -> OpResult {
    let name = param(params, "StackName").to_string();
    with_store(account_num, region, |store| {
        let pos = require_stack!(store, &name);
        store.stacks[pos].status = "UPDATE_ROLLBACK_COMPLETE".to_string();
        add_stack_event(&mut store.stacks[pos], "UPDATE_ROLLBACK_COMPLETE", None);
        Ok(xml::empty("ContinueUpdateRollbackResponse"))
    })
}

fn rollback_stack(params: &HashMap<String, String>, account_num: u64, region: &str) -> OpResult {
    let name = param(params, "StackName").to_string();
    with_store(account_num, region, |store| {
        let pos = require_stack!(store, &name);
        store.stacks[pos].status = "ROLLBACK_COMPLETE".to_string();
        add_stack_event(
            &mut store.stacks[pos],
            "ROLLBACK_COMPLETE",
            Some("Rollback requested".to_string()),
        );
        let stack_id = store.stacks[pos].stack_id.clone();
        Ok(xml::ok(
            "RollbackStackResponse",
            &xml::dict_to_xml(&json!({"StackId": stack_id})),
        ))
    })
}

fn signal_resource(params: &HashMap<String, String>, account_num: u64, region: &str) -> OpResult {
    let name = param(params, "StackName");
    with_store(account_num, region, |store| {
        if find_stack_pos(store, name).is_none() {
            return Err(validation_error(format!("Stack: {name} does not exist")));
        }
        Ok(xml::empty("SignalResourceResponse"))
    })
}

fn detect_stack_drift(
    params: &HashMap<String, String>,
    account_num: u64,
    region: &str,
) -> OpResult {
    let name = param(params, "StackName");
    with_store(account_num, region, |store| {
        if find_stack_pos(store, name).is_none() {
            return Err(validation_error(format!("Stack: {name} does not exist")));
        }
        Ok(xml::ok(
            "DetectStackDriftResponse",
            &xml::dict_to_xml(&json!({"StackDriftDetectionId": Uuid::new_v4().to_string()})),
        ))
    })
}

fn detect_stack_resource_drift(
    params: &HashMap<String, String>,
    account_num: u64,
    region: &str,
) -> OpResult {
    let name = param(params, "StackName");
    with_store(account_num, region, |store| {
        if find_stack_pos(store, name).is_none() {
            return Err(validation_error(format!("Stack: {name} does not exist")));
        }
        Ok(xml::ok(
            "DetectStackResourceDriftResponse",
            &xml::dict_to_xml(
                &json!({"StackResourceDrift": {"StackId": name, "LogicalResourceId": param(params, "LogicalResourceId"), "StackResourceDriftStatus": "IN_SYNC"}}),
            ),
        ))
    })
}

fn describe_stack_resource_drifts(
    params: &HashMap<String, String>,
    account_num: u64,
    region: &str,
) -> OpResult {
    let name = param(params, "StackName");
    with_store(account_num, region, |store| {
        if find_stack_pos(store, name).is_none() {
            return Err(validation_error(format!("Stack: {name} does not exist")));
        }
        Ok(xml::ok(
            "DescribeStackResourceDriftsResponse",
            &xml::dict_to_xml(&json!({"StackResourceDrifts": []})),
        ))
    })
}

fn cancel_update_stack(
    params: &HashMap<String, String>,
    account_num: u64,
    region: &str,
) -> OpResult {
    let name = param(params, "StackName").to_string();
    with_store(account_num, region, |store| {
        if find_stack_pos(store, &name).is_none() {
            return Err(validation_error(format!(
                "Stack with id {name} does not exist"
            )));
        }
        Ok(xml::empty("CancelUpdateStackResponse"))
    })
}

fn list_change_sets(params: &HashMap<String, String>, account_num: u64, region: &str) -> OpResult {
    let name = param(params, "StackName");
    with_store(account_num, region, |store| {
        let summaries: Vec<Value> = store
            .change_sets
            .iter()
            .filter(|cs| cs.stack_name == name)
            .map(|cs| {
                json!({
                    "ChangeSetId": cs.change_set_id,
                    "ChangeSetName": cs.change_set_name,
                    "StackId": cs.stack_id,
                    "StackName": cs.stack_name,
                    "Status": cs.status,
                })
            })
            .collect();
        Ok(xml::ok(
            "ListChangeSetsResponse",
            &xml::dict_to_xml(&json!({"Summaries": summaries})),
        ))
    })
}

fn get_template_summary(
    params: &HashMap<String, String>,
    account_num: u64,
    region: &str,
) -> OpResult {
    let stack_name = param(params, "StackName");
    let body = if !stack_name.is_empty() {
        with_store(account_num, region, |store| {
            let stack = store.find_stack(stack_name).ok_or_else(|| {
                validation_error(format!("Stack with id {stack_name} does not exist"))
            })?;
            if stack.status == "REVIEW_IN_PROGRESS" {
                return Err(validation_error(
                    "GetTemplateSummary cannot be called on REVIEW_IN_PROGRESS stacks.",
                ));
            }
            Ok(stack.template_body.clone())
        })?
    } else {
        param(params, "TemplateBody").to_string()
    };
    let template = engine::parse_template(&body).map_err(validation_error)?;
    let mut resource_types: Vec<Value> = Vec::new();
    if let Some(resources) = template.get("Resources").and_then(Value::as_object) {
        for v in resources.values() {
            if let Some(t) = v.get("Type").and_then(Value::as_str) {
                resource_types.push(Value::String(t.to_string()));
            }
        }
    }
    let mut members: Vec<Value> = Vec::new();
    if let Some(pdefs) = template.get("Parameters").and_then(Value::as_object) {
        for (pname, pdef) in pdefs {
            let mut m = Map::new();
            let _ = m.insert("ParameterKey".to_string(), Value::String(pname.clone()));
            let _ = m.insert(
                "Description".to_string(),
                Value::String(
                    pdef.get("Description")
                        .and_then(Value::as_str)
                        .unwrap_or("")
                        .to_string(),
                ),
            );
            let _ = m.insert(
                "DefaultValue".to_string(),
                pdef.get("Default").cloned().unwrap_or(Value::Null),
            );
            let _ = m.insert(
                "NoEcho".to_string(),
                Value::Bool(pdef.get("NoEcho").and_then(Value::as_bool).unwrap_or(false)),
            );
            let _ = m.insert(
                "ParameterType".to_string(),
                Value::String(
                    pdef.get("Type")
                        .and_then(Value::as_str)
                        .unwrap_or("String")
                        .to_string(),
                ),
            );
            let _ = m.insert("ParameterConstraints".to_string(), json!({}));
            members.push(Value::Object(m));
        }
    }
    let mut result = json!({
        "ResourceTypes": resource_types,
        "Version": template.get("AWSTemplateFormatVersion").cloned().unwrap_or(Value::String("2010-09-09".to_string())),
        "Parameters": members,
    });
    if let Some(Value::String(desc)) = template.get("Description") {
        let _ = result
            .as_object_mut()
            .unwrap()
            .insert("Description".to_string(), Value::String(desc.clone()));
    }
    let _ = region;
    Ok(xml::ok(
        "GetTemplateSummaryResponse",
        &xml::dict_to_xml(&result),
    ))
}

fn describe_type(params: &HashMap<String, String>, region: &str, account: &str) -> OpResult {
    let type_name = param(params, "TypeName");
    let type_name = if type_name.is_empty() {
        "AWS::CloudFormation::Stack"
    } else {
        type_name
    };
    let type_arn = param(params, "Arn");
    let arn = if type_arn.is_empty() {
        format!(
            "arn:aws:cloudformation:{region}:{account}:type/resource/{}/00000001",
            type_name.replace("::", "-")
        )
    } else {
        type_arn.to_string()
    };
    let body = format!(
        "<Arn>{arn}</Arn><Type>RESOURCE</Type><TypeName>{tn}</TypeName><Description>Schema for {tn}</Description><Schema></Schema><ProvisioningType>FULLY_MUTABLE</ProvisioningType><DeprecatedStatus>LIVE</DeprecatedStatus><Visibility>PUBLIC</Visibility>",
        tn = type_name
    );
    Ok(xml::ok("DescribeTypeResponse", &body))
}

// --- Generated templates -----------------------------------------------------

fn create_generated_template(
    params: &HashMap<String, String>,
    region: &str,
    account: &str,
) -> OpResult {
    let account_num: u64 = account.parse().unwrap_or(123456789012);
    let name = param(params, "GeneratedTemplateName").to_string();
    with_store(account_num, region, |store| {
        let id = format!(
            "arn:aws:cloudformation:{region}:{account}:generatedtemplate/{}",
            Uuid::new_v4()
        );
        store
            .generated_templates
            .push(crate::services::cloudformation::store::GeneratedTemplate {
                name: name.clone(),
                id: id.clone(),
                resources: json!([]),
                created: chrono::Utc::now().timestamp(),
            });
        Ok(xml::ok(
            "CreateGeneratedTemplateResponse",
            &xml::dict_to_xml(&json!({"GeneratedTemplateId": id})),
        ))
    })
}

fn describe_generated_template(
    params: &HashMap<String, String>,
    region: &str,
    account: &str,
) -> OpResult {
    let account_num: u64 = account.parse().unwrap_or(123456789012);
    let name = param(params, "GeneratedTemplateName").to_string();
    with_store(account_num, region, |store| {
        // Moto answers DescribeGeneratedTemplate with a synthesized record
        // even for unknown names (never 404).
        let gt_name = store
            .generated_templates
            .iter()
            .find(|g| g.name == name)
            .map(|g| (g.id.clone(), g.created))
            .unwrap_or_else(|| {
                (
                    format!(
                        "arn:aws:cloudformation:{region}:{account}:generatedtemplate/{}",
                        Uuid::new_v4()
                    ),
                    chrono::Utc::now().timestamp(),
                )
            });
        let result = json!({
            "GeneratedTemplateId": gt_name.0,
            "GeneratedTemplateName": name,
            "Status": "COMPLETE",
            "TemplateBody": "{}",
            "TotalWarnings": 0,
            "CreationTime": xml::timestamp_from(gt_name.1),
            "LastUpdatedTime": xml::timestamp_from(gt_name.1),
        });
        let _ = account;
        Ok(xml::ok(
            "DescribeGeneratedTemplateResponse",
            &xml::dict_to_xml(&result),
        ))
    })
}

fn update_generated_template(
    params: &HashMap<String, String>,
    region: &str,
    account: &str,
) -> OpResult {
    let account_num: u64 = account.parse().unwrap_or(123456789012);
    let name = param(params, "GeneratedTemplateName").to_string();
    with_store(account_num, region, |store| {
        let pos = store
            .generated_templates
            .iter()
            .position(|g| g.name == name);
        let Some(pos) = pos else {
            return Err(CfnError::new(
                "ResourceNotFoundException",
                format!("Generated template {name} not found"),
            ));
        };
        let new_name = param(params, "NewGeneratedTemplateName");
        if !new_name.is_empty() {
            store.generated_templates[pos].name = new_name.to_string();
        }
        let id = store.generated_templates[pos].id.clone();
        Ok(xml::ok(
            "UpdateGeneratedTemplateResponse",
            &xml::dict_to_xml(&json!({"GeneratedTemplateId": id})),
        ))
    })
}

fn verify_stack_refactor(
    _params: &HashMap<String, String>,
    _region: &str,
    _account: &str,
) -> OpResult {
    Ok(xml::ok(
        "CreateStackRefactorResponse",
        &xml::dict_to_xml(&json!({"StackRefactorId": Uuid::new_v4().to_string()})),
    ))
}

// ---------------------------------------------------------------------------
// stub_action: route the moto-shaped registry/drift/stack-set surface.
// ---------------------------------------------------------------------------

#[allow(clippy::too_many_lines)]
fn stub_action(
    action: &str,
    params: &HashMap<String, String>,
    account_num: u64,
    region: &str,
) -> OpResult {
    let account = account_num.to_string();
    match action {
        "CreateStackSet" => create_stack_set(params, region, &account),
        "UpdateStackSet" => update_stack_set(params, account_num, region),
        "DeleteStackSet" => delete_stack_set(params, account_num, region),
        "DescribeStackSet" => describe_stack_set(params, account_num, region),
        "ListStackSets" => list_stack_sets(params, account_num, region),
        "ListStackInstances" => list_stack_instances(params, account_num, region),
        "CreateStackInstances" => create_stack_instances(params, account_num, region),
        "UpdateStackInstances" => update_stack_instances(params, account_num, region),
        "DeleteStackInstances" => delete_stack_instances(params, account_num, region),
        "DescribeStackInstance" => describe_stack_instance(params, account_num, region),
        "DescribeStackSetOperation" => describe_stack_set_operation(params, account_num, region),
        "ListStackSetOperations" => list_stack_set_operations(params, account_num, region),
        "ListStackSetOperationResults" => list_stack_set_operation_results(params, account_num, region),
        "StopStackSetOperation" => stop_stack_set_operation(params, account_num, region),
        "ImportStacksToStackSet" => import_stacks_to_stack_set(params, account_num, region),
        "GetStackPolicy" => get_stack_policy(params, account_num, region),
        "SetStackPolicy" => set_stack_policy(params, account_num, region),
        "UpdateTerminationProtection" => update_termination_protection(params, account_num, region),
        "ContinueUpdateRollback" => continue_update_rollback(params, account_num, region),
        "RollbackStack" => rollback_stack(params, account_num, region),
        "SignalResource" => signal_resource(params, account_num, region),
        "DetectStackDrift" => detect_stack_drift(params, account_num, region),
        "DetectStackResourceDrift" => detect_stack_resource_drift(params, account_num, region),
        "DescribeStackResourceDrifts" => describe_stack_resource_drifts(params, account_num, region),
        "CancelUpdateStack" => cancel_update_stack(params, account_num, region),
        "ListChangeSets" => list_change_sets(params, account_num, region),
        "GetTemplateSummary" => get_template_summary(params, account_num, region),
        "DescribeType" => describe_type(params, region, &account),
        "CreateGeneratedTemplate" => create_generated_template(params, region, &account),
        "DescribeGeneratedTemplate" => describe_generated_template(params, region, &account),
        "UpdateGeneratedTemplate" => update_generated_template(params, region, &account),
        "CreateStackRefactor" => verify_stack_refactor(params, region, &account),
        // moto's handler: a canned result echoing the (optional) refactor id.
        "DescribeStackRefactor" => Ok(xml::ok(
            "DescribeStackRefactorResponse",
            &xml::dict_to_xml(&json!({
                "StackRefactorId": params
                    .get("StackRefactorId")
                    .cloned()
                    .unwrap_or_else(|| Uuid::new_v4().to_string()),
                "Status": "CREATE_COMPLETE",
                "StatusReason": "",
                "ExecutionStatus": "AVAILABLE",
            })),
        )),
        "ListTypes" => Ok(xml::ok(action, "<TypeSummaries/>")),
        "ListTypeRegistrations" => Ok(xml::ok(action, "<RegistrationTokenList/>")),
        "ListGeneratedTemplates" => Ok(xml::ok(action, "<Summaries/>")),
        "ListResourceScans" => Ok(xml::ok(action, "<ResourceScanSummaries/>")),
        "ListHookResults" => Ok(xml::ok(action, "<HookResults/>")),
        "ListImports" => Ok(xml::ok(action, "<Imports/>")),
        "ListStackRefactors" => Ok(xml::ok(action, "<StackRefactorSummaries/>")),
        "ListTypeVersions" => Ok(xml::ok(action, "<TypeVersionSummaries/>")),
        "ListResourceScanResources" => Ok(xml::ok(action, "<Resources/>")),
        "ListResourceScanRelatedResources" => Ok(xml::ok(action, "<RelatedResources/>")),
        "ListStackInstanceResourceDrifts" => Ok(xml::ok(action, "<Summaries/>")),
        "ListStackSetAutoDeploymentTargets" => Ok(xml::ok(action, "<Summaries/>")),
        "ListStackRefactorActions" => Ok(xml::ok(action, "<StackRefactorActions/>")),
        "BatchDescribeTypeConfigurations" => Ok(xml::ok(
            action,
            "<Errors/><UnprocessedTypeConfigurations/><TypeConfigurations/>",
        )),
        "DescribeAccountLimits" => Ok(xml::ok(
            action,
            "<AccountLimits><member><Name>StackLimit</Name><Value>200</Value></member><member><Name>StackOutputsLimit</Name><Value>60</Value></member><member><Name>ConcurrentResourcesLimit</Name><Value>2500</Value></member></AccountLimits>",
        )),
        "DescribeOrganizationsAccess" => Ok(xml::ok(action, "<Status>DISABLED</Status>")),
        "ActivateOrganizationsAccess" | "DeactivateOrganizationsAccess" | "DeactivateType"
        | "DeregisterType" | "DeleteGeneratedTemplate" | "ExecuteStackRefactor"
        | "RecordHandlerProgress" | "SetTypeDefaultVersion" => Ok(xml::empty(action)),
        // Moto-shaped: GetGeneratedTemplate always answers with a template.
        "GetGeneratedTemplate" => Ok(xml::ok(action, "<Status>COMPLETE</Status><TemplateBody>{}</TemplateBody>")),
        "DescribePublisher" => Ok(xml::ok(
            action,
            "<PublisherId>000000000000</PublisherId><PublisherStatus>VERIFIED</PublisherStatus><IdentityProvider>AWS_Marketplace</IdentityProvider>",
        )),
        "DescribeEvents" => Ok(xml::ok(action, "<StackEvents/>")),
        "GetHookResult" => Ok(xml::ok(action, "<HookStatus>HOOK_COMPLETE_SUCCEEDED</HookStatus>")),
        "EstimateTemplateCost" => Ok(xml::ok(
            action,
            "<Url>http://calculator.s3.amazonaws.com/calc5.html</Url>",
        )),
        "ActivateType" => Ok(xml::ok(
            action,
            &format!("<Arn>arn:aws:cloudformation:{region}:{account}:type/resource/AWS-Example-Resource/00000001</Arn>"),
        )),
        "PublishType" => Ok(xml::ok(
            action,
            &format!("<PublicTypeArn>arn:aws:cloudformation:{region}::type/resource/AWS-Example-Resource/00000001</PublicTypeArn>"),
        )),
        "RegisterPublisher" => Ok(xml::ok(action, &format!("<PublisherId>{account}</PublisherId>"))),
        "StartResourceScan" => Ok(xml::ok(
            action,
            &format!(
                "<ResourceScanId>arn:aws:cloudformation:{region}:{account}:resourceScan/{}</ResourceScanId>",
                Uuid::new_v4()
            ),
        )),
        "TestType" => Ok(xml::ok(
            action,
            &format!("<TypeVersionArn>arn:aws:cloudformation:{region}:{account}:type/resource/AWS-Example-Resource/00000001</TypeVersionArn>"),
        )),
        "DescribeStackDriftDetectionStatus" => Ok(xml::ok(
            action,
            &format!(
                "<StackId></StackId><StackDriftDetectionId>{}</StackDriftDetectionId><StackDriftStatus>IN_SYNC</StackDriftStatus><DetectionStatus>DETECTION_COMPLETE</DetectionStatus><DriftedStackResourceCount>0</DriftedStackResourceCount>",
                param(params, "StackDriftDetectionId")
            ),
        )),
        "DetectStackSetDrift" => Ok(xml::ok(
            action,
            &format!("<OperationId>{}</OperationId>", Uuid::new_v4()),
        )),
        "DescribeChangeSetHooks" => Ok(xml::ok(
            action,
            &format!(
                "<ChangeSetId>{}</ChangeSetId><ChangeSetName>{}</ChangeSetName><Hooks/><Status>PLANNING_COMPLETE</Status>",
                param(params, "ChangeSetName"),
                param(params, "ChangeSetName")
            ),
        )),
        "DescribeResourceScan" => Err(validation_error(format!(
            "ResourceScan [{}] does not exist",
            param(params, "ResourceScanId")
        ))),
        "DescribeTypeRegistration" | "RegisterType" | "SetTypeConfiguration" => Err(
            CfnError::new(
                "NotImplemented",
                format!("The {action} action has not been implemented"),
            )
            .status(501),
        ),
        _ => Err(CfnError::new(
            "NotImplemented",
            format!("The {action} action has not been implemented"),
        )
        .status(501)),
    }
}
