//! Amazon Pinpoint backend — moto-equivalent (REST-JSON).
//!
//! Port of moto's `pinpoint` backend (`moto/pinpoint/models.py`): apps,
//! campaigns, segments, journeys, templates (email/sms/push/voice/inapp),
//! channels (10 platforms), endpoints, export/import jobs, recommender
//! configurations, tagging, events, messages and the phone/OTP/KPI stubs.
//! State is per-(account, region).
//!
//! Requests enter with an empty `operation` (the gateway keeps no REST op
//! table for pinpoint), so this module resolves the operation from the
//! HTTP method + botocore requestUri (`pinpoint-2016-12-01` model) and
//! captures the `{param}` placeholders from the path. Path segments arrive
//! percent-encoded (tag ARNs travel as `arn%3Aaws…%3Aapps%2F<id>`) and are
//! decoded on capture, matching moto's `unquote(self.path)`.
//!
//! moto's quirks kept verbatim:
//! - creations answer HTTP 201 (moto's `return 201, …, json.dumps(...)`).
//! - tags for a resource are a `key -> value` dict; `TagResource` merges
//!   (a repeated key overwrites in place), `UntagResource` removes by key.
//! - `Get{Campaign,Segment}Version` indexes the stored version history
//!   1-based; out-of-range raises the type's `NotFoundException`.
//! - `UpdateApplicationSettings` replaces the settings object wholesale
//!   and answers `ApplicationSettingsResource` (with ApplicationId).
//! - `UpdateTemplate` replaces the params and bumps the string version;
//!   `ListTemplateVersions` answers stored snapshots in insertion order.
//! - `Get{type}Channel` invents a disabled channel on first read, and
//!   `Delete{type}Channel` answers a disabled channel for a type never
//!   created instead of erroring.
//! - campaigns always report `State.CampaignStatus: PAUSED`; journeys
//!   track their state (`DRAFT` until updated).
//! - probe-era moto 500s (non-numeric `Version` path params, tag delete
//!   without `tagKeys`, deleting an unset event stream) answer the modeled
//!   404/200 here instead of the crash — only 501 means absent.

use crate::proto::{json_error, parse_json_body, query_map, url_decode};
use crate::store::{self, ScopeKey};
use crate::{Request, Response};
use parking_lot::RwLock;
use serde_json::{json, Map, Value};
use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;

// ---------------------------------------------------------------------------
// State
// ---------------------------------------------------------------------------

type Stores = Arc<RwLock<HashMap<ScopeKey, PinpointStore>>>;

fn stores() -> Stores {
    store::slot::<PinpointStore>("pinpoint")
}

/// Channel types in moto's `get_channels` output order.
const CHANNEL_TYPES: [&str; 10] = [
    "ADM",
    "APNS",
    "APNS_SANDBOX",
    "APNS_VOIP",
    "APNS_VOIP_SANDBOX",
    "BAIDU",
    "EMAIL",
    "GCM",
    "SMS",
    "VOICE",
];

/// Per-(account, region) in-memory state for Amazon Pinpoint.
#[derive(Default)]
struct PinpointStore {
    apps: BTreeMap<String, App>,
    /// resource ARN -> (tag key -> tag value), mirroring moto's TaggingService.
    tags: BTreeMap<String, BTreeMap<String, Value>>,
    /// "name/TYPE" -> template — moto's `_template_key`.
    templates: BTreeMap<String, Template>,
    recommenders: BTreeMap<String, Recommender>,
    /// per-app collections, keyed by application id.
    apps_resources: BTreeMap<String, AppResources>,
}

#[derive(Default)]
struct AppResources {
    campaigns: BTreeMap<String, Campaign>,
    segments: BTreeMap<String, Segment>,
    journeys: BTreeMap<String, Journey>,
    /// channel type (ADM, APNS, …) -> channel
    channels: BTreeMap<String, Channel>,
    endpoints: BTreeMap<String, Endpoint>,
    export_jobs: Vec<Job>,
    import_jobs: Vec<Job>,
}

struct App {
    id: String,
    arn: String,
    name: Value,
    created: f64,
    settings: Value,
    settings_modified: String,
    event_stream: Option<EventStream>,
}

impl App {
    fn to_json(&self) -> Value {
        json!({
            "Arn": self.arn,
            "Id": self.id,
            "Name": self.name,
            "CreationDate": self.created,
        })
    }
}

struct EventStream {
    stream_arn: String,
    role_arn: String,
    last_modified: String,
}

impl EventStream {
    /// moto adds `ApplicationId` to every event-stream body.
    fn to_json(&self, app_id: &str) -> Value {
        json!({
            "DestinationStreamArn": self.stream_arn,
            "RoleArn": self.role_arn,
            "LastModifiedDate": self.last_modified,
            "ApplicationId": app_id,
        })
    }
}

struct Campaign {
    id: String,
    app_id: String,
    arn: String,
    params: Value,
    version: i64,
    created: String,
    last_modified: String,
    versions: Vec<Value>,
}

impl Campaign {
    /// moto's `Campaign.update`: params replaced, version bumps and the
    /// new snapshot joins the history.
    fn update(&mut self, params: &Value) {
        self.params = params.clone();
        self.version += 1;
        self.last_modified = now_iso();
        self.versions.push(self.to_json());
    }

    /// 1-based lookup into the stored history (`GetCampaignVersion`).
    fn version_at(&self, version: i64) -> Option<&Value> {
        if version < 1 || version as usize > self.versions.len() {
            return None;
        }
        Some(&self.versions[(version - 1) as usize])
    }

    /// moto's `to_json`: params with the resolved fields overriding.
    fn to_json(&self) -> Value {
        merged(
            &self.params,
            &[
                ("Id", json!(self.id)),
                ("ApplicationId", json!(self.app_id)),
                ("Arn", json!(self.arn)),
                ("CreationDate", json!(self.created)),
                ("LastModifiedDate", json!(self.last_modified)),
                ("Version", json!(self.version)),
                // moto fixes campaigns to paused until started elsewhere.
                ("State", json!({ "CampaignStatus": "PAUSED" })),
            ],
        )
    }
}

struct Segment {
    id: String,
    app_id: String,
    arn: String,
    params: Value,
    segment_type: String,
    version: i64,
    created: String,
    last_modified: String,
    versions: Vec<Value>,
}

impl Segment {
    fn update(&mut self, params: &Value) {
        self.params = params.clone();
        self.version += 1;
        self.last_modified = now_iso();
        self.versions.push(self.to_json());
    }

    fn version_at(&self, version: i64) -> Option<&Value> {
        if version < 1 || version as usize > self.versions.len() {
            return None;
        }
        Some(&self.versions[(version - 1) as usize])
    }

    fn to_json(&self) -> Value {
        merged(
            &self.params,
            &[
                ("Id", json!(self.id)),
                ("ApplicationId", json!(self.app_id)),
                ("Arn", json!(self.arn)),
                ("CreationDate", json!(self.created)),
                ("LastModifiedDate", json!(self.last_modified)),
                ("Version", json!(self.version)),
                ("SegmentType", json!(self.segment_type)),
            ],
        )
    }
}

struct Journey {
    id: String,
    app_id: String,
    arn: String,
    params: Value,
    state: String,
    created: String,
    last_modified: String,
}

impl Journey {
    /// moto's `Journey.update`: params replaced; a `State` member wins.
    fn update(&mut self, params: &Value) {
        self.params = params.clone();
        if let Some(state) = self.params.get("State").and_then(Value::as_str) {
            self.state = state.to_string();
        }
        self.last_modified = now_iso();
    }

    fn update_state(&mut self, state: &str) {
        self.state = state.to_string();
        self.last_modified = now_iso();
    }

    fn to_json(&self) -> Value {
        merged(
            &self.params,
            &[
                ("Id", json!(self.id)),
                ("ApplicationId", json!(self.app_id)),
                ("Arn", json!(self.arn)),
                ("CreationDate", json!(self.created)),
                ("LastModifiedDate", json!(self.last_modified)),
                ("State", json!(self.state)),
            ],
        )
    }
}

struct Template {
    name: String,
    template_type: String,
    params: Value,
    version: String,
    created: String,
    last_modified: String,
    /// stored snapshots (version, json) in insertion order — moto keeps a
    /// python dict, so `list_template_versions` answers insertion order.
    versions: Vec<(String, Value)>,
    active_version: String,
}

impl Template {
    /// moto's `Template.update`: params replaced, version bumps as a string
    /// and the new snapshot joins the history.
    fn update(&mut self, params: &Value) {
        self.params = params.clone();
        self.version = (self.version.parse::<i64>().unwrap_or(0) + 1).to_string();
        self.last_modified = now_iso();
        let snapshot = self.to_json();
        self.versions.push((self.version.clone(), snapshot));
    }

    fn to_json(&self) -> Value {
        merged(
            &self.params,
            &[
                ("TemplateName", json!(self.name)),
                ("TemplateType", json!(self.template_type)),
                ("Version", json!(self.version)),
                ("CreationDate", json!(self.created)),
                ("LastModifiedDate", json!(self.last_modified)),
            ],
        )
    }
}

struct Channel {
    id: String,
    channel_type: String,
    params: Value,
    enabled: bool,
    version: i64,
    created: String,
    last_modified: String,
    app_id: String,
}

impl Channel {
    fn new(app_id: &str, channel_type: &str, params: Value) -> Channel {
        Channel {
            id: gen_id(),
            channel_type: channel_type.to_string(),
            // moto: `self.enabled = params.get("Enabled", True)`.
            enabled: params
                .get("Enabled")
                .and_then(Value::as_bool)
                .unwrap_or(true),
            params,
            version: 1,
            created: now_iso(),
            last_modified: now_iso(),
            app_id: app_id.to_string(),
        }
    }

    /// moto's `Channel.update`: params replaced, `Enabled` sticks.
    fn update(&mut self, params: &Value) {
        self.params = params.clone();
        if let Some(enabled) = self.params.get("Enabled").and_then(Value::as_bool) {
            self.enabled = enabled;
        }
        self.last_modified = now_iso();
        self.version += 1;
    }

    fn to_json(&self) -> Value {
        merged(
            &self.params,
            &[
                ("Id", json!(self.id)),
                ("ApplicationId", json!(self.app_id)),
                ("CreationDate", json!(self.created)),
                ("LastModifiedDate", json!(self.last_modified)),
                ("Enabled", json!(self.enabled)),
                ("Version", json!(self.version)),
                ("Platform", json!(self.channel_type.to_uppercase())),
            ],
        )
    }
}

struct Endpoint {
    id: String,
    app_id: String,
    params: Value,
    created: String,
    effective_date: String,
    user_id: String,
}

impl Endpoint {
    /// moto's `Endpoint.update`: params replaced wholesale; user id resets
    /// unless the new params carry `User.UserId`.
    fn update(&mut self, params: &Value) {
        self.params = params.clone();
        self.effective_date = params
            .get("EffectiveDate")
            .and_then(Value::as_str)
            .unwrap_or(&now_iso())
            .to_string();
        self.user_id = params
            .get("User")
            .and_then(|u| u.get("UserId"))
            .and_then(Value::as_str)
            .unwrap_or("")
            .to_string();
    }

    fn to_json(&self) -> Value {
        merged(
            &self.params,
            &[
                ("Id", json!(self.id)),
                ("ApplicationId", json!(self.app_id)),
                ("CreationDate", json!(self.created)),
                ("EffectiveDate", json!(self.effective_date)),
                ("CohortId", json!("1")),
            ],
        )
    }
}

struct Job {
    id: String,
    app_id: String,
    params: Value,
    created: String,
    export: bool,
}

impl Job {
    fn to_json(&self) -> Value {
        json!({
            "Id": self.id,
            "ApplicationId": self.app_id,
            "CreationDate": self.created,
            "CompletedPieces": 1,
            "CompletionDate": self.created,
            "Definition": self.params.clone(),
            "FailedPieces": 0,
            "JobStatus": "COMPLETED",
            "TotalFailures": 0,
            "TotalPieces": 1,
            "TotalProcessed": 0,
            "Type": if self.export { "EXPORT" } else { "IMPORT" },
        })
    }
}

struct Recommender {
    id: String,
    params: Value,
    created: String,
    last_modified: String,
}

impl Recommender {
    /// moto's `RecommenderConfig.update`: params replaced, no version bump.
    fn update(&mut self, params: &Value) {
        self.params = params.clone();
        self.last_modified = now_iso();
    }

    fn to_json(&self) -> Value {
        merged(
            &self.params,
            &[
                ("Id", json!(self.id)),
                ("CreationDate", json!(self.created)),
                ("LastModifiedDate", json!(self.last_modified)),
            ],
        )
    }
}

// ---------------------------------------------------------------------------
// moto utility mirrors
// ---------------------------------------------------------------------------

/// moto's `get_partition` for ARN partitions.
fn partition(region: &str) -> &'static str {
    if region.starts_with("cn-") {
        "aws-cn"
    } else if region.starts_with("us-gov-") {
        "aws-us-gov"
    } else if region.starts_with("us-iso-") {
        "aws-iso"
    } else if region.starts_with("us-isob-") {
        "aws-iso-b"
    } else if region.starts_with("eusc-") {
        "aws-eusc"
    } else {
        "aws"
    }
}

fn mobile_targeting_arn(region: &str, account: u64, resource: &str) -> String {
    format!(
        "arn:{}:mobiletargeting:{region}:{account}:{resource}",
        partition(region)
    )
}

/// moto's `_gen_id`: a uuid4 hex without dashes.
fn gen_id() -> String {
    uuid::Uuid::new_v4().simple().to_string()
}

/// moto's `_now_iso`: `%Y-%m-%dT%H:%M:%S.%fZ` in local time.
fn now_iso() -> String {
    chrono::Local::now()
        .format("%Y-%m-%dT%H:%M:%S.%fZ")
        .to_string()
}

/// moto's `unix_time` (epoch seconds as a float) for App CreationDate.
fn now_unix() -> f64 {
    chrono::Utc::now().timestamp_millis() as f64 / 1000.0
}

/// Merge stored params with resolved fields — fixed keys win (moto's
/// `result = dict(params); result.update({...})`).
fn merged(params: &Value, extra: &[(&str, Value)]) -> Value {
    let mut obj = params.as_object().cloned().unwrap_or_default();
    for (k, v) in extra {
        obj.insert((*k).to_string(), v.clone());
    }
    Value::Object(obj)
}

fn str_field(body: &Value, key: &str) -> String {
    body.get(key)
        .and_then(Value::as_str)
        .unwrap_or("")
        .to_string()
}

/// moto's TaggingService: keys keep their first-insertion position; a
/// repeated key updates its value in place.
fn tag_resource(st: &mut PinpointStore, arn: &str, tags: &Value) {
    let Some(map) = tags.as_object() else { return };
    let entry = st.tags.entry(arn.to_string()).or_default();
    for (k, v) in map {
        entry.insert(k.clone(), v.clone());
    }
}

/// moto's `untag_resource_using_names`.
fn untag_resource(st: &mut PinpointStore, arn: &str, keys: &[Value]) {
    let Some(entry) = st.tags.get_mut(arn) else {
        return;
    };
    for key in keys {
        if let Some(k) = key.as_str() {
            entry.remove(k);
        }
    }
}

fn app_resources<'a>(st: &'a mut PinpointStore, app_id: &str) -> &'a mut AppResources {
    st.apps_resources.entry(app_id.to_string()).or_default()
}

/// moto's get_app guard: everything app-owned needs a real application.
fn app_guard(st: &mut PinpointStore, app_id: &str) -> Result<(), Response> {
    if st.apps.contains_key(app_id) {
        Ok(())
    } else {
        Err(app_not_found())
    }
}

// ---------------------------------------------------------------------------
// Operation resolution: (method, requestUri) -> operation, per botocore's
// pinpoint-2016-12-01 model — one entry per operation, path placeholders
// preserved from the spec.
// ---------------------------------------------------------------------------

const RULES: &[(&str, &str, &str)] = &[
    ("DELETE", "/v1/apps/{application-id}/channels/adm", "DeleteAdmChannel"),
    ("DELETE", "/v1/apps/{application-id}/channels/apns", "DeleteApnsChannel"),
    ("DELETE", "/v1/apps/{application-id}/channels/apns_sandbox", "DeleteApnsSandboxChannel"),
    ("DELETE", "/v1/apps/{application-id}/channels/apns_voip", "DeleteApnsVoipChannel"),
    ("DELETE", "/v1/apps/{application-id}/channels/apns_voip_sandbox", "DeleteApnsVoipSandboxChannel"),
    ("DELETE", "/v1/apps/{application-id}/channels/baidu", "DeleteBaiduChannel"),
    ("DELETE", "/v1/apps/{application-id}/channels/email", "DeleteEmailChannel"),
    ("DELETE", "/v1/apps/{application-id}/channels/gcm", "DeleteGcmChannel"),
    ("DELETE", "/v1/apps/{application-id}/channels/sms", "DeleteSmsChannel"),
    ("DELETE", "/v1/apps/{application-id}/channels/voice", "DeleteVoiceChannel"),
    ("DELETE", "/v1/apps/{application-id}/campaigns/{campaign-id}", "DeleteCampaign"),
    ("DELETE", "/v1/apps/{application-id}/endpoints/{endpoint-id}", "DeleteEndpoint"),
    ("DELETE", "/v1/apps/{application-id}/eventstream", "DeleteEventStream"),
    ("DELETE", "/v1/apps/{application-id}/journeys/{journey-id}", "DeleteJourney"),
    ("DELETE", "/v1/apps/{application-id}/segments/{segment-id}", "DeleteSegment"),
    ("DELETE", "/v1/apps/{application-id}/users/{user-id}", "DeleteUserEndpoints"),
    ("DELETE", "/v1/templates/{template-name}/email", "DeleteEmailTemplate"),
    ("DELETE", "/v1/templates/{template-name}/inapp", "DeleteInAppTemplate"),
    ("DELETE", "/v1/templates/{template-name}/push", "DeletePushTemplate"),
    ("DELETE", "/v1/templates/{template-name}/sms", "DeleteSmsTemplate"),
    ("DELETE", "/v1/templates/{template-name}/voice", "DeleteVoiceTemplate"),
    ("DELETE", "/v1/apps/{application-id}", "DeleteApp"),
    ("DELETE", "/v1/recommenders/{recommender-id}", "DeleteRecommenderConfiguration"),
    ("DELETE", "/v1/tags/{resource-arn}", "UntagResource"),
    ("GET", "/v1/apps/{application-id}/journeys/{journey-id}/runs/{run-id}/activities/{journey-activity-id}/execution-metrics", "GetJourneyRunExecutionActivityMetrics"),
    ("GET", "/v1/apps/{application-id}/campaigns/{campaign-id}/kpis/daterange/{kpi-name}", "GetCampaignDateRangeKpi"),
    ("GET", "/v1/apps/{application-id}/journeys/{journey-id}/activities/{journey-activity-id}/execution-metrics", "GetJourneyExecutionActivityMetrics"),
    ("GET", "/v1/apps/{application-id}/journeys/{journey-id}/kpis/daterange/{kpi-name}", "GetJourneyDateRangeKpi"),
    ("GET", "/v1/apps/{application-id}/journeys/{journey-id}/runs/{run-id}/execution-metrics", "GetJourneyRunExecutionMetrics"),
    ("GET", "/v1/apps/{application-id}/segments/{segment-id}/jobs/export", "GetSegmentExportJobs"),
    ("GET", "/v1/apps/{application-id}/segments/{segment-id}/jobs/import", "GetSegmentImportJobs"),
    ("GET", "/v1/apps/{application-id}/campaigns/{campaign-id}/activities", "GetCampaignActivities"),
    ("GET", "/v1/apps/{application-id}/campaigns/{campaign-id}/versions", "GetCampaignVersions"),
    ("GET", "/v1/apps/{application-id}/campaigns/{campaign-id}/versions/{version}", "GetCampaignVersion"),
    ("GET", "/v1/apps/{application-id}/channels/adm", "GetAdmChannel"),
    ("GET", "/v1/apps/{application-id}/channels/apns", "GetApnsChannel"),
    ("GET", "/v1/apps/{application-id}/channels/apns_sandbox", "GetApnsSandboxChannel"),
    ("GET", "/v1/apps/{application-id}/channels/apns_voip", "GetApnsVoipChannel"),
    ("GET", "/v1/apps/{application-id}/channels/apns_voip_sandbox", "GetApnsVoipSandboxChannel"),
    ("GET", "/v1/apps/{application-id}/channels/baidu", "GetBaiduChannel"),
    ("GET", "/v1/apps/{application-id}/channels/email", "GetEmailChannel"),
    ("GET", "/v1/apps/{application-id}/channels/gcm", "GetGcmChannel"),
    ("GET", "/v1/apps/{application-id}/channels/sms", "GetSmsChannel"),
    ("GET", "/v1/apps/{application-id}/channels/voice", "GetVoiceChannel"),
    ("GET", "/v1/apps/{application-id}/endpoints/{endpoint-id}/inappmessages", "GetInAppMessages"),
    ("GET", "/v1/apps/{application-id}/jobs/export", "GetExportJobs"),
    ("GET", "/v1/apps/{application-id}/jobs/export/{job-id}", "GetExportJob"),
    ("GET", "/v1/apps/{application-id}/jobs/import", "GetImportJobs"),
    ("GET", "/v1/apps/{application-id}/jobs/import/{job-id}", "GetImportJob"),
    ("GET", "/v1/apps/{application-id}/journeys/{journey-id}/execution-metrics", "GetJourneyExecutionMetrics"),
    ("GET", "/v1/apps/{application-id}/journeys/{journey-id}/runs", "GetJourneyRuns"),
    ("GET", "/v1/apps/{application-id}/kpis/daterange/{kpi-name}", "GetApplicationDateRangeKpi"),
    ("GET", "/v1/apps/{application-id}/segments/{segment-id}/versions", "GetSegmentVersions"),
    ("GET", "/v1/apps/{application-id}/segments/{segment-id}/versions/{version}", "GetSegmentVersion"),
    ("GET", "/v1/apps/{application-id}/campaigns", "GetCampaigns"),
    ("GET", "/v1/apps/{application-id}/campaigns/{campaign-id}", "GetCampaign"),
    ("GET", "/v1/apps/{application-id}/channels", "GetChannels"),
    ("GET", "/v1/apps/{application-id}/endpoints/{endpoint-id}", "GetEndpoint"),
    ("GET", "/v1/apps/{application-id}/eventstream", "GetEventStream"),
    ("GET", "/v1/apps/{application-id}/journeys", "ListJourneys"),
    ("GET", "/v1/apps/{application-id}/journeys/{journey-id}", "GetJourney"),
    ("GET", "/v1/apps/{application-id}/segments", "GetSegments"),
    ("GET", "/v1/apps/{application-id}/segments/{segment-id}", "GetSegment"),
    ("GET", "/v1/apps/{application-id}/settings", "GetApplicationSettings"),
    ("GET", "/v1/apps/{application-id}/users/{user-id}", "GetUserEndpoints"),
    ("GET", "/v1/templates/{template-name}/email", "GetEmailTemplate"),
    ("GET", "/v1/templates/{template-name}/inapp", "GetInAppTemplate"),
    ("GET", "/v1/templates/{template-name}/push", "GetPushTemplate"),
    ("GET", "/v1/templates/{template-name}/sms", "GetSmsTemplate"),
    ("GET", "/v1/templates/{template-name}/voice", "GetVoiceTemplate"),
    ("GET", "/v1/templates/{template-name}/{template-type}/versions", "ListTemplateVersions"),
    ("GET", "/v1/apps", "GetApps"),
    ("GET", "/v1/apps/{application-id}", "GetApp"),
    ("GET", "/v1/recommenders", "GetRecommenderConfigurations"),
    ("GET", "/v1/recommenders/{recommender-id}", "GetRecommenderConfiguration"),
    ("GET", "/v1/tags/{resource-arn}", "ListTagsForResource"),
    ("GET", "/v1/templates", "ListTemplates"),
    ("POST", "/v1/apps/{application-id}/jobs/export", "CreateExportJob"),
    ("POST", "/v1/apps/{application-id}/jobs/import", "CreateImportJob"),
    ("POST", "/v1/phone/number/validate", "PhoneNumberValidate"),
    ("POST", "/v1/apps/{application-id}/campaigns", "CreateCampaign"),
    ("POST", "/v1/apps/{application-id}/events", "PutEvents"),
    ("POST", "/v1/apps/{application-id}/eventstream", "PutEventStream"),
    ("POST", "/v1/apps/{application-id}/journeys", "CreateJourney"),
    ("POST", "/v1/apps/{application-id}/messages", "SendMessages"),
    ("POST", "/v1/apps/{application-id}/otp", "SendOTPMessage"),
    ("POST", "/v1/apps/{application-id}/segments", "CreateSegment"),
    ("POST", "/v1/apps/{application-id}/users-messages", "SendUsersMessages"),
    ("POST", "/v1/apps/{application-id}/verify-otp", "VerifyOTPMessage"),
    ("POST", "/v1/templates/{template-name}/email", "CreateEmailTemplate"),
    ("POST", "/v1/templates/{template-name}/inapp", "CreateInAppTemplate"),
    ("POST", "/v1/templates/{template-name}/push", "CreatePushTemplate"),
    ("POST", "/v1/templates/{template-name}/sms", "CreateSmsTemplate"),
    ("POST", "/v1/templates/{template-name}/voice", "CreateVoiceTemplate"),
    ("POST", "/v1/apps", "CreateApp"),
    ("POST", "/v1/recommenders", "CreateRecommenderConfiguration"),
    ("POST", "/v1/tags/{resource-arn}", "TagResource"),
    ("PUT", "/v1/apps/{application-id}/channels/adm", "UpdateAdmChannel"),
    ("PUT", "/v1/apps/{application-id}/channels/apns", "UpdateApnsChannel"),
    ("PUT", "/v1/apps/{application-id}/channels/apns_sandbox", "UpdateApnsSandboxChannel"),
    ("PUT", "/v1/apps/{application-id}/channels/apns_voip", "UpdateApnsVoipChannel"),
    ("PUT", "/v1/apps/{application-id}/channels/apns_voip_sandbox", "UpdateApnsVoipSandboxChannel"),
    ("PUT", "/v1/apps/{application-id}/channels/baidu", "UpdateBaiduChannel"),
    ("PUT", "/v1/apps/{application-id}/channels/email", "UpdateEmailChannel"),
    ("PUT", "/v1/apps/{application-id}/channels/gcm", "UpdateGcmChannel"),
    ("PUT", "/v1/apps/{application-id}/channels/sms", "UpdateSmsChannel"),
    ("PUT", "/v1/apps/{application-id}/channels/voice", "UpdateVoiceChannel"),
    ("PUT", "/v1/apps/{application-id}/journeys/{journey-id}/state", "UpdateJourneyState"),
    ("PUT", "/v1/apps/{application-id}/attributes/{attribute-type}", "RemoveAttributes"),
    ("PUT", "/v1/apps/{application-id}/campaigns/{campaign-id}", "UpdateCampaign"),
    ("PUT", "/v1/apps/{application-id}/endpoints", "UpdateEndpointsBatch"),
    ("PUT", "/v1/apps/{application-id}/endpoints/{endpoint-id}", "UpdateEndpoint"),
    ("PUT", "/v1/apps/{application-id}/journeys/{journey-id}", "UpdateJourney"),
    ("PUT", "/v1/apps/{application-id}/segments/{segment-id}", "UpdateSegment"),
    ("PUT", "/v1/apps/{application-id}/settings", "UpdateApplicationSettings"),
    ("PUT", "/v1/templates/{template-name}/email", "UpdateEmailTemplate"),
    ("PUT", "/v1/templates/{template-name}/inapp", "UpdateInAppTemplate"),
    ("PUT", "/v1/templates/{template-name}/push", "UpdatePushTemplate"),
    ("PUT", "/v1/templates/{template-name}/sms", "UpdateSmsTemplate"),
    ("PUT", "/v1/templates/{template-name}/voice", "UpdateVoiceTemplate"),
    ("PUT", "/v1/templates/{template-name}/{template-type}/active-version", "UpdateTemplateActiveVersion"),
    ("PUT", "/v1/recommenders/{recommender-id}", "UpdateRecommenderConfiguration"),
];

/// Channel type (ADM, APNS, APNS_SANDBOX, …) behind each `XChannel` op.
/// Longer platform tokens first — `ApnsVoipSandbox` also contains `Apns`.
fn channel_type_for_op(op: &str) -> &'static str {
    for (token, ctype) in [
        ("ApnsVoipSandbox", "APNS_VOIP_SANDBOX"),
        ("ApnsSandbox", "APNS_SANDBOX"),
        ("ApnsVoip", "APNS_VOIP"),
        ("Adm", "ADM"),
        ("Apns", "APNS"),
        ("Baidu", "BAIDU"),
        ("Email", "EMAIL"),
        ("Gcm", "GCM"),
        ("Sms", "SMS"),
        ("Voice", "VOICE"),
    ] {
        if op.contains(token) {
            return ctype;
        }
    }
    ""
}

/// Template type (EMAIL/SMS/PUSH/VOICE/INAPP) behind each `XTemplate` op.
fn template_type_for_op(op: &str) -> &'static str {
    for (token, ttype) in [
        ("InApp", "INAPP"),
        ("Email", "EMAIL"),
        ("Sms", "SMS"),
        ("Push", "PUSH"),
        ("Voice", "VOICE"),
    ] {
        if op.contains(token) {
            return ttype;
        }
    }
    ""
}

/// Resolve (method, path) to `(operation, {name -> decoded value})`.
/// Literals must match exactly; `{Param}` captures one decoded segment.
/// Earlier rules win, so the table is ordered literal-heavy first.
fn route(method: &str, raw_path: &str) -> Option<(&'static str, BTreeMap<String, String>)> {
    let path = raw_path.split('?').next().unwrap_or(raw_path);
    let segs: Vec<&str> = path.split('/').filter(|s| !s.is_empty()).collect();
    for (rule_method, uri, op) in RULES {
        if *rule_method != method {
            continue;
        }
        let tsegs: Vec<&str> = uri.split('/').filter(|s| !s.is_empty()).collect();
        if tsegs.len() != segs.len() {
            continue;
        }
        let mut params = BTreeMap::new();
        let mut matched = true;
        for (t, s) in tsegs.iter().zip(segs.iter()) {
            let name = t.strip_prefix('{').and_then(|x| x.strip_suffix('}'));
            match name {
                Some(name) => {
                    params.insert(name.to_string(), url_decode(s));
                }
                None if t != s => {
                    matched = false;
                    break;
                }
                None => {}
            }
        }
        if matched {
            return Some((op, params));
        }
    }
    None
}

fn uri_param(params: &BTreeMap<String, String>, name: &str) -> String {
    params.get(name).cloned().unwrap_or_default()
}

/// moto runs `int(parts[-1])` on the version path segment.
fn version_int(raw: &str) -> Option<i64> {
    raw.parse::<i64>().ok()
}

// ---------------------------------------------------------------------------
// dispatch
// ---------------------------------------------------------------------------

pub fn dispatch(req: &Request) -> Option<Response> {
    if req.service != "pinpoint" {
        return None;
    }
    let Some((op, params)) = route(&req.method, &req.path) else {
        let e = json!({
            "__type": "NotImplemented",
            "message": format!("The {} action has not been implemented", req.operation),
        });
        return Some(Response::json(
            501,
            serde_json::to_vec(&e).unwrap_or_default(),
        ));
    };
    let slot = stores();
    let mut map = slot.write();
    let st = map.entry((req.account, req.region.clone())).or_default();
    Some(handle(req, st, op, &params))
}

fn handle(
    req: &Request,
    st: &mut PinpointStore,
    op: &str,
    params: &BTreeMap<String, String>,
) -> Response {
    let body = parse_json_body(&req.body);
    let app_id = uri_param(params, "application-id");
    match op {
        // --- App --------------------------------------------------------------
        "CreateApp" => create_app(req, st, &body),
        "GetApp" => match st.apps.get(&app_id) {
            Some(app) => ok(app.to_json()),
            None => app_not_found(),
        },
        "DeleteApp" => {
            let Some(app) = st.apps.get(&app_id) else {
                return app_not_found();
            };
            let dict = app.to_json();
            st.apps.remove(&app_id);
            st.apps_resources.remove(&app_id);
            ok(dict)
        }
        "GetApps" => {
            let items: Vec<Value> = st.apps.values().map(App::to_json).collect();
            ok(json!({ "Item": items }))
        }
        "GetApplicationSettings" | "UpdateApplicationSettings" => {
            let Some(app) = st.apps.get_mut(&app_id) else {
                return app_not_found();
            };
            if op == "UpdateApplicationSettings" {
                app.settings = body.clone();
                app.settings_modified = now_iso();
            }
            let settings = json!({
                "CampaignHook": app.settings.get("CampaignHook").cloned().unwrap_or(json!({})),
                "CloudWatchMetricsEnabled": app
                    .settings
                    .get("CloudWatchMetricsEnabled")
                    .cloned()
                    .unwrap_or(json!(false)),
                "LastModifiedDate": app.settings_modified,
                "Limits": app.settings.get("Limits").cloned().unwrap_or(json!({})),
                "QuietTime": app.settings.get("QuietTime").cloned().unwrap_or(json!({})),
                "ApplicationId": app.id,
            });
            ok(settings)
        }
        // --- Tags ---------------------------------------------------------------
        "ListTagsForResource" => {
            let tags = st
                .tags
                .get(&uri_param(params, "resource-arn"))
                .cloned()
                .unwrap_or_default();
            ok(json!({ "tags": tags }))
        }
        "TagResource" => {
            tag_resource(
                st,
                &uri_param(params, "resource-arn"),
                &body.get("tags").cloned().unwrap_or(json!({})),
            );
            ok_empty()
        }
        "UntagResource" => {
            let keys = query_map(&req.query_string)
                .get("tagKeys")
                .cloned()
                .unwrap_or_default();
            let values: Vec<Value> = keys.into_iter().map(Value::String).collect();
            untag_resource(st, &uri_param(params, "resource-arn"), &values);
            ok_empty()
        }
        // --- Event stream ---------------------------------------------------------
        "PutEventStream" | "GetEventStream" | "DeleteEventStream" => {
            let Some(app) = st.apps.get_mut(&app_id) else {
                return app_not_found();
            };
            if op == "PutEventStream" {
                EventStreamPut::apply(app, &body);
                let stream = app.event_stream.as_ref().expect("put sets a stream");
                return ok(stream.to_json(&app_id));
            }
            match app.event_stream.take() {
                Some(stream) => {
                    let dict = stream.to_json(&app_id);
                    if op == "GetEventStream" {
                        // Only delete removes the stream.
                        app.event_stream = Some(stream);
                    }
                    ok(dict)
                }
                // Get/Delete without a stream answer the modeled "Resource
                // not found" (moto 500s with an AttributeError instead).
                None => not_found("Resource not found"),
            }
        }
        // --- Campaigns ------------------------------------------------------------
        "CreateCampaign" => {
            let Some(res) = app_resources_guarded(st, &app_id) else {
                return app_not_found();
            };
            let id = gen_id();
            let mut campaign = Campaign {
                app_id: app_id.clone(),
                arn: mobile_targeting_arn(
                    &req.region,
                    req.account,
                    &format!("apps/{app_id}/campaigns/{id}"),
                ),
                id: id.clone(),
                version: 1,
                created: now_iso(),
                last_modified: now_iso(),
                params: body.clone(),
                versions: Vec::new(),
            };
            let dict = campaign.to_json();
            let campaign_arn = dict["Arn"].as_str().unwrap_or("").to_string();
            campaign.versions.push(dict.clone());
            res.campaigns.insert(id, campaign);
            tag_resource(
                st,
                &campaign_arn,
                &body.get("tags").cloned().unwrap_or(json!({})),
            );
            ok_created(dict, 201)
        }
        "GetCampaign" => {
            let Some(res) = app_resources_guarded(st, &app_id) else {
                return app_not_found();
            };
            match res.campaigns.get(&uri_param(params, "campaign-id")) {
                Some(campaign) => {
                    let dict = campaign.to_json();
                    ok(dict)
                }
                None => campaign_not_found(),
            }
        }
        "GetCampaigns" => {
            let Some(res) = app_resources_guarded(st, &app_id) else {
                return app_not_found();
            };
            let items: Vec<Value> = res.campaigns.values().map(Campaign::to_json).collect();
            ok(json!({ "Item": items }))
        }
        "UpdateCampaign" => {
            let Some(res) = app_resources_guarded(st, &app_id) else {
                return app_not_found();
            };
            match res.campaigns.get_mut(&uri_param(params, "campaign-id")) {
                Some(campaign) => {
                    campaign.update(&body);
                    let dict = campaign.to_json();
                    ok(dict)
                }
                None => campaign_not_found(),
            }
        }
        "DeleteCampaign" => {
            let Some(res) = app_resources_guarded(st, &app_id) else {
                return app_not_found();
            };
            match res.campaigns.remove(&uri_param(params, "campaign-id")) {
                Some(campaign) => {
                    let dict = campaign.to_json();
                    ok(dict)
                }
                None => campaign_not_found(),
            }
        }
        "GetCampaignVersions" => {
            let Some(res) = app_resources_guarded(st, &app_id) else {
                return app_not_found();
            };
            match res.campaigns.get(&uri_param(params, "campaign-id")) {
                Some(campaign) => ok(json!({ "Item": campaign.versions.clone() })),
                None => campaign_not_found(),
            }
        }
        "GetCampaignVersion" => {
            let Some(res) = app_resources_guarded(st, &app_id) else {
                return app_not_found();
            };
            let cid = uri_param(params, "campaign-id");
            let version = version_int(&uri_param(params, "version"));
            match res
                .campaigns
                .get(&cid)
                .and_then(|c| c.version_at(version.unwrap_or(-1)))
            {
                Some(snapshot) => {
                    let dict = snapshot.clone();
                    ok(dict)
                }
                // Non-numeric or out-of-range versions answer the modeled
                // NotFound (moto 500s on the int() failure).
                None => campaign_not_found(),
            }
        }
        "GetCampaignActivities" => {
            let Some(res) = app_resources_guarded(st, &app_id) else {
                return app_not_found();
            };
            if !res
                .campaigns
                .contains_key(&uri_param(params, "campaign-id"))
            {
                return campaign_not_found();
            }
            // Activities are execution metrics moto does not record.
            ok(json!({ "Item": [] }))
        }
        "GetCampaignDateRangeKpi" => {
            let Some(res) = app_resources_guarded(st, &app_id) else {
                return app_not_found();
            };
            let cid = uri_param(params, "campaign-id");
            if !res.campaigns.contains_key(&cid) {
                return campaign_not_found();
            }
            let kpi = kpi_json("CampaignId", &cid, &app_id, &uri_param(params, "kpi-name"));
            ok(kpi)
        }
        // Application KPI stub — moto's `get_application_date_range_kpi`.
        "GetApplicationDateRangeKpi" => {
            if app_guard(st, &app_id).is_err() {
                return app_not_found();
            }
            ok(json!({
                "ApplicationId": app_id,
                "KpiName": uri_param(params, "kpi-name"),
                "KpiResult": { "Rows": [] },
                "StartTime": now_iso(),
                "EndTime": now_iso(),
            }))
        }
        // In-app messages stub (moto's `get_in_app_messages`).
        "GetInAppMessages" => {
            if app_guard(st, &app_id).is_err() {
                return app_not_found();
            }
            ok(json!({ "InAppMessageCampaigns": [] }))
        }
        // --- Segments ---------------------------------------------------------------
        "CreateSegment" => {
            let Some(res) = app_resources_guarded(st, &app_id) else {
                return app_not_found();
            };
            let id = gen_id();
            let mut segment = Segment {
                app_id: app_id.clone(),
                arn: mobile_targeting_arn(
                    &req.region,
                    req.account,
                    &format!("apps/{app_id}/segments/{id}"),
                ),
                id: id.clone(),
                segment_type: body
                    .get("SegmentType")
                    .and_then(Value::as_str)
                    .unwrap_or("DIMENSIONAL")
                    .to_string(),
                version: 1,
                created: now_iso(),
                last_modified: now_iso(),
                params: body.clone(),
                versions: Vec::new(),
            };
            let dict = segment.to_json();
            let segment_arn = dict["Arn"].as_str().unwrap_or("").to_string();
            segment.versions.push(dict.clone());
            res.segments.insert(id, segment);
            tag_resource(
                st,
                &segment_arn,
                &body.get("tags").cloned().unwrap_or(json!({})),
            );
            ok_created(dict, 201)
        }
        "GetSegment" => {
            let Some(res) = app_resources_guarded(st, &app_id) else {
                return app_not_found();
            };
            match res.segments.get(&uri_param(params, "segment-id")) {
                Some(segment) => {
                    let dict = segment.to_json();
                    ok(dict)
                }
                None => segment_not_found(),
            }
        }
        "GetSegments" => {
            let Some(res) = app_resources_guarded(st, &app_id) else {
                return app_not_found();
            };
            let items: Vec<Value> = res.segments.values().map(Segment::to_json).collect();
            ok(json!({ "Item": items }))
        }
        "UpdateSegment" => {
            let Some(res) = app_resources_guarded(st, &app_id) else {
                return app_not_found();
            };
            match res.segments.get_mut(&uri_param(params, "segment-id")) {
                Some(segment) => {
                    segment.update(&body);
                    let dict = segment.to_json();
                    ok(dict)
                }
                None => segment_not_found(),
            }
        }
        "DeleteSegment" => {
            let Some(res) = app_resources_guarded(st, &app_id) else {
                return app_not_found();
            };
            match res.segments.remove(&uri_param(params, "segment-id")) {
                Some(segment) => {
                    let dict = segment.to_json();
                    ok(dict)
                }
                None => segment_not_found(),
            }
        }
        "GetSegmentVersions" => {
            let Some(res) = app_resources_guarded(st, &app_id) else {
                return app_not_found();
            };
            match res.segments.get(&uri_param(params, "segment-id")) {
                Some(segment) => ok(json!({ "Item": segment.versions.clone() })),
                None => segment_not_found(),
            }
        }
        "GetSegmentVersion" => {
            let Some(res) = app_resources_guarded(st, &app_id) else {
                return app_not_found();
            };
            let sid = uri_param(params, "segment-id");
            let version = version_int(&uri_param(params, "version"));
            match res
                .segments
                .get(&sid)
                .and_then(|s| s.version_at(version.unwrap_or(-1)))
            {
                Some(snapshot) => {
                    let dict = snapshot.clone();
                    ok(dict)
                }
                None => segment_not_found(),
            }
        }
        "GetSegmentExportJobs" | "GetSegmentImportJobs" => {
            let is_export = op.ends_with("ExportJobs");
            {
                let Some(res) = app_resources_guarded(st, &app_id) else {
                    return app_not_found();
                };
                if !res.segments.contains_key(&uri_param(params, "segment-id")) {
                    return segment_not_found();
                }
            }
            let res = app_resources(st, &app_id);
            let sid = uri_param(params, "segment-id");
            let jobs = if is_export {
                &res.export_jobs
            } else {
                &res.import_jobs
            };
            let items: Vec<Value> = jobs
                .iter()
                .filter(|j| j.params.get("SegmentId").and_then(Value::as_str) == Some(sid.as_str()))
                .map(Job::to_json)
                .collect();
            ok(json!({ "Item": items }))
        }
        // --- Journeys -----------------------------------------------------------------
        "CreateJourney" => {
            let Some(res) = app_resources_guarded(st, &app_id) else {
                return app_not_found();
            };
            let id = gen_id();
            let journey = Journey {
                app_id: app_id.clone(),
                arn: mobile_targeting_arn(
                    &req.region,
                    req.account,
                    &format!("apps/{app_id}/journeys/{id}"),
                ),
                id: id.clone(),
                state: body
                    .get("State")
                    .and_then(Value::as_str)
                    .unwrap_or("DRAFT")
                    .to_string(),
                created: now_iso(),
                last_modified: now_iso(),
                params: body.clone(),
            };
            let dict = journey.to_json();
            let journey_arn = dict["Arn"].as_str().unwrap_or("").to_string();
            res.journeys.insert(id, journey);
            tag_resource(
                st,
                &journey_arn,
                &body.get("tags").cloned().unwrap_or(json!({})),
            );
            ok_created(dict, 201)
        }
        "GetJourney" => {
            let Some(res) = app_resources_guarded(st, &app_id) else {
                return app_not_found();
            };
            match res.journeys.get(&uri_param(params, "journey-id")) {
                Some(journey) => {
                    let dict = journey.to_json();
                    ok(dict)
                }
                None => journey_not_found(),
            }
        }
        "ListJourneys" => {
            let Some(res) = app_resources_guarded(st, &app_id) else {
                return app_not_found();
            };
            let items: Vec<Value> = res.journeys.values().map(Journey::to_json).collect();
            ok(json!({ "Item": items }))
        }
        "UpdateJourney" => {
            let Some(res) = app_resources_guarded(st, &app_id) else {
                return app_not_found();
            };
            match res.journeys.get_mut(&uri_param(params, "journey-id")) {
                Some(journey) => {
                    journey.update(&body);
                    let dict = journey.to_json();
                    ok(dict)
                }
                None => journey_not_found(),
            }
        }
        "UpdateJourneyState" => {
            let Some(res) = app_resources_guarded(st, &app_id) else {
                return app_not_found();
            };
            match res.journeys.get_mut(&uri_param(params, "journey-id")) {
                Some(journey) => {
                    let state = body
                        .get("State")
                        .and_then(Value::as_str)
                        .unwrap_or("ACTIVE");
                    journey.update_state(state);
                    let dict = journey.to_json();
                    ok(dict)
                }
                None => journey_not_found(),
            }
        }
        "DeleteJourney" => {
            let Some(res) = app_resources_guarded(st, &app_id) else {
                return app_not_found();
            };
            match res.journeys.remove(&uri_param(params, "journey-id")) {
                Some(journey) => {
                    let dict = journey.to_json();
                    ok(dict)
                }
                None => journey_not_found(),
            }
        }
        "GetJourneyDateRangeKpi" => {
            let Some(res) = app_resources_guarded(st, &app_id) else {
                return app_not_found();
            };
            let jid = uri_param(params, "journey-id");
            if !res.journeys.contains_key(&jid) {
                return journey_not_found();
            }
            let kpi = kpi_json("JourneyId", &jid, &app_id, &uri_param(params, "kpi-name"));
            ok(kpi)
        }
        "GetJourneyExecutionMetrics" => {
            let Some(res) = app_resources_guarded(st, &app_id) else {
                return app_not_found();
            };
            let jid = uri_param(params, "journey-id");
            if !res.journeys.contains_key(&jid) {
                return journey_not_found();
            }
            ok(json!({
                "ApplicationId": app_id,
                "JourneyId": jid,
                "LastEvaluatedTime": now_iso(),
                "Metrics": {},
            }))
        }
        "GetJourneyExecutionActivityMetrics" => {
            let Some(res) = app_resources_guarded(st, &app_id) else {
                return app_not_found();
            };
            let jid = uri_param(params, "journey-id");
            if !res.journeys.contains_key(&jid) {
                return journey_not_found();
            }
            ok(json!({
                "ApplicationId": app_id,
                "JourneyId": jid,
                "JourneyActivityId": uri_param(params, "journey-activity-id"),
                "LastEvaluatedTime": now_iso(),
                "Metrics": {},
            }))
        }
        "GetJourneyRuns" => {
            let Some(res) = app_resources_guarded(st, &app_id) else {
                return app_not_found();
            };
            if !res.journeys.contains_key(&uri_param(params, "journey-id")) {
                return journey_not_found();
            }
            ok(json!({ "Item": [] }))
        }
        "GetJourneyRunExecutionMetrics" => {
            let Some(res) = app_resources_guarded(st, &app_id) else {
                return app_not_found();
            };
            let jid = uri_param(params, "journey-id");
            if !res.journeys.contains_key(&jid) {
                return journey_not_found();
            }
            let run_id = uri_param(params, "run-id");
            ok(json!({
                "ApplicationId": app_id,
                "JourneyId": jid,
                "RunId": run_id,
                "LastEvaluatedTime": now_iso(),
                "Metrics": {},
            }))
        }
        "GetJourneyRunExecutionActivityMetrics" => {
            let Some(res) = app_resources_guarded(st, &app_id) else {
                return app_not_found();
            };
            let jid = uri_param(params, "journey-id");
            if !res.journeys.contains_key(&jid) {
                return journey_not_found();
            }
            ok(json!({
                "ApplicationId": app_id,
                "JourneyId": jid,
                "RunId": uri_param(params, "run-id"),
                "JourneyActivityId": uri_param(params, "journey-activity-id"),
                "LastEvaluatedTime": now_iso(),
                "Metrics": {},
            }))
        }
        // --- Templates --------------------------------------------------------------------
        "CreateEmailTemplate"
        | "CreateSmsTemplate"
        | "CreatePushTemplate"
        | "CreateVoiceTemplate"
        | "CreateInAppTemplate" => {
            let name = uri_param(params, "template-name");
            let ttype = template_type_for_op(op);
            let mut template = Template {
                name: name.clone(),
                template_type: ttype.to_string(),
                params: body.clone(),
                version: "1".to_string(),
                created: now_iso(),
                last_modified: now_iso(),
                versions: Vec::new(),
                active_version: String::new(),
            };
            let snapshot = template.to_json();
            template.versions.push(("1".to_string(), snapshot));
            st.templates.insert(template_key(&name, ttype), template);
            let resp_body = json!({
                "CreateTemplateMessageBody": {
                    "Arn": "",
                    "Message": "Created",
                    "RequestID": name,
                }
            });
            ok_created(resp_body, 201)
        }
        "GetEmailTemplate" | "GetSmsTemplate" | "GetPushTemplate" | "GetVoiceTemplate"
        | "GetInAppTemplate" => {
            let name = uri_param(params, "template-name");
            let ttype = template_type_for_op(op);
            match st.templates.get(&template_key(&name, ttype)) {
                Some(template) => {
                    let dict = template.to_json();
                    ok(dict)
                }
                None => template_not_found(),
            }
        }
        "UpdateEmailTemplate"
        | "UpdateSmsTemplate"
        | "UpdatePushTemplate"
        | "UpdateVoiceTemplate"
        | "UpdateInAppTemplate" => {
            let name = uri_param(params, "template-name");
            let ttype = template_type_for_op(op);
            match st.templates.get_mut(&template_key(&name, ttype)) {
                Some(template) => {
                    template.update(&body);
                    let accepted = accepted_message_body(&name);
                    ok(accepted)
                }
                None => template_not_found(),
            }
        }
        "DeleteEmailTemplate"
        | "DeleteSmsTemplate"
        | "DeletePushTemplate"
        | "DeleteVoiceTemplate"
        | "DeleteInAppTemplate" => {
            let name = uri_param(params, "template-name");
            let ttype = template_type_for_op(op);
            if st.templates.remove(&template_key(&name, ttype)).is_some() {
                ok(accepted_message_body(&name))
            } else {
                template_not_found()
            }
        }
        "ListTemplates" => {
            let items: Vec<Value> = st.templates.values().map(Template::to_json).collect();
            ok(json!({ "Item": items }))
        }
        "ListTemplateVersions" => {
            let name = uri_param(params, "template-name");
            let ttype = uri_param(params, "template-type").to_uppercase();
            match st.templates.get(&template_key(&name, &ttype)) {
                Some(template) => {
                    let versions: Vec<Value> =
                        template.versions.iter().map(|(_, v)| v.clone()).collect();
                    ok(json!({ "Item": versions }))
                }
                None => template_not_found(),
            }
        }
        "UpdateTemplateActiveVersion" => {
            let name = uri_param(params, "template-name");
            let ttype = uri_param(params, "template-type").to_uppercase();
            match st.templates.get_mut(&template_key(&name, &ttype)) {
                Some(template) => {
                    template.active_version = body
                        .get("Version")
                        .and_then(Value::as_str)
                        .unwrap_or("1")
                        .to_string();
                    ok(accepted_message_body(&name))
                }
                None => template_not_found(),
            }
        }
        // --- Channels ------------------------------------------------------------------------
        "GetChannels" => {
            if app_guard(st, &app_id).is_err() {
                return app_not_found();
            }
            let res = app_resources(st, &app_id);
            let mut channels = Map::new();
            for ctype in CHANNEL_TYPES {
                if let Some(c) = res.channels.get(ctype) {
                    channels.insert((*ctype).to_string(), c.to_json());
                }
            }
            ok(json!({ "Channels": Value::Object(channels) }))
        }
        op if op.starts_with("Get") && op.ends_with("Channel") => {
            if app_guard(st, &app_id).is_err() {
                return app_not_found();
            }
            let ctype = channel_type_for_op(op);
            let res = app_resources(st, &app_id);
            // moto invents a disabled channel on first read, and stores it.
            if !res.channels.contains_key(ctype) {
                let mut channel = Channel::new(&app_id, ctype, json!({}));
                channel.enabled = false;
                res.channels.insert(ctype.to_string(), channel);
            }
            let dict = res
                .channels
                .get(ctype)
                .map(Channel::to_json)
                .unwrap_or(json!({}));
            ok(dict)
        }
        op if op.starts_with("Update") && op.ends_with("Channel") => {
            if app_guard(st, &app_id).is_err() {
                return app_not_found();
            }
            let ctype = channel_type_for_op(op);
            let res = app_resources(st, &app_id);
            if res.channels.contains_key(ctype) {
                let channel = res.channels.get_mut(ctype).unwrap();
                channel.update(&body);
                let dict = channel.to_json();
                ok(dict)
            } else {
                let channel = Channel::new(&app_id, ctype, body.clone());
                let dict = channel.to_json();
                res.channels.insert(ctype.to_string(), channel);
                ok(dict)
            }
        }
        op if op.starts_with("Delete") && op.ends_with("Channel") => {
            if app_guard(st, &app_id).is_err() {
                return app_not_found();
            }
            let ctype = channel_type_for_op(op);
            let res = app_resources(st, &app_id);
            match res.channels.remove(ctype) {
                Some(channel) => {
                    let dict = channel.to_json();
                    ok(dict)
                }
                // Deleting a channel that never existed answers a disabled
                // default channel instead of erroring (moto constructs one).
                None => {
                    let dict = Channel::new(&app_id, ctype, json!({ "Enabled": false })).to_json();
                    ok(dict)
                }
            }
        }
        // --- Endpoints -----------------------------------------------------------------------
        "UpdateEndpoint" => {
            if app_guard(st, &app_id).is_err() {
                return app_not_found();
            }
            let eid = uri_param(params, "endpoint-id");
            let res = app_resources(st, &app_id);
            if let Some(endpoint) = res.endpoints.get_mut(&eid) {
                endpoint.update(&body);
            } else {
                res.endpoints
                    .insert(eid.clone(), new_endpoint(&app_id, &eid, &body));
            }
            let endpoint_body =
                json!({ "MessageBody": { "Message": "Accepted", "RequestID": eid } });
            ok(endpoint_body)
        }
        "UpdateEndpointsBatch" => {
            if app_guard(st, &app_id).is_err() {
                return app_not_found();
            }
            let res = app_resources(st, &app_id);
            let mut results = Map::new();
            let items = body
                .get("Item")
                .and_then(Value::as_array)
                .cloned()
                .unwrap_or_default();
            for item in items {
                let eid = item
                    .get("Id")
                    .and_then(Value::as_str)
                    .map(str::to_string)
                    .unwrap_or_else(gen_id);
                if let Some(endpoint) = res.endpoints.get_mut(&eid) {
                    endpoint.update(&item);
                } else {
                    res.endpoints
                        .insert(eid.clone(), new_endpoint(&app_id, &eid, &item));
                }
                results.insert(eid, json!({ "StatusCode": 202, "Message": "Accepted" }));
            }
            ok(json!({
                "MessageBody": { "Message": "Accepted" },
                "Result": Value::Object(results),
            }))
        }
        "GetEndpoint" => {
            if app_guard(st, &app_id).is_err() {
                return app_not_found();
            }
            let res = app_resources(st, &app_id);
            match res.endpoints.get(&uri_param(params, "endpoint-id")) {
                Some(endpoint) => {
                    let dict = endpoint.to_json();
                    ok(dict)
                }
                None => endpoint_not_found(),
            }
        }
        "DeleteEndpoint" => {
            if app_guard(st, &app_id).is_err() {
                return app_not_found();
            }
            let res = app_resources(st, &app_id);
            match res.endpoints.remove(&uri_param(params, "endpoint-id")) {
                Some(endpoint) => {
                    let dict = endpoint.to_json();
                    ok(dict)
                }
                None => endpoint_not_found(),
            }
        }
        "GetUserEndpoints" | "DeleteUserEndpoints" => {
            if app_guard(st, &app_id).is_err() {
                return app_not_found();
            }
            let user_id = uri_param(params, "user-id");
            let res = app_resources(st, &app_id);
            let targets: Vec<String> = res
                .endpoints
                .iter()
                .filter(|(_, ep)| ep.user_id == user_id)
                .map(|(eid, _)| eid.clone())
                .collect();
            let items: Vec<Value> = targets
                .iter()
                .map(|eid| {
                    if op == "DeleteUserEndpoints" {
                        res.endpoints
                            .remove(eid)
                            .map_or(json!({}), |ep| ep.to_json())
                    } else {
                        res.endpoints.get(eid).map_or(json!({}), |ep| ep.to_json())
                    }
                })
                .collect();
            ok(json!({ "Item": items }))
        }
        // --- Export / import jobs ---------------------------------------------------------------
        "CreateExportJob" | "CreateImportJob" => {
            let is_export = op == "CreateExportJob";
            if app_guard(st, &app_id).is_err() {
                return app_not_found();
            }
            let res = app_resources(st, &app_id);
            let job = Job {
                id: gen_id(),
                app_id: app_id.clone(),
                params: body.clone(),
                created: now_iso(),
                export: is_export,
            };
            let dict = job.to_json();
            if is_export {
                res.export_jobs.push(job);
            } else {
                res.import_jobs.push(job);
            }
            ok_created(dict, 201)
        }
        "GetExportJob" | "GetImportJob" => {
            let is_export = op == "GetExportJob";
            if app_guard(st, &app_id).is_err() {
                return app_not_found();
            }
            let job_id = uri_param(params, "job-id");
            let res = app_resources(st, &app_id);
            let jobs = if is_export {
                &res.export_jobs
            } else {
                &res.import_jobs
            };
            match jobs.iter().find(|job| job.id == job_id) {
                Some(job) => {
                    let dict = job.to_json();
                    ok(dict)
                }
                None => job_not_found(is_export),
            }
        }
        "GetExportJobs" | "GetImportJobs" => {
            let is_export = op == "GetExportJobs";
            if app_guard(st, &app_id).is_err() {
                return app_not_found();
            }
            let res = app_resources(st, &app_id);
            let jobs = if is_export {
                &res.export_jobs
            } else {
                &res.import_jobs
            };
            let items: Vec<Value> = jobs.iter().map(Job::to_json).collect();
            ok(json!({ "Item": items }))
        }
        // --- Recommenders -------------------------------------------------------------------------
        "CreateRecommenderConfiguration" => {
            let rec = Recommender {
                id: gen_id(),
                params: body.clone(),
                created: now_iso(),
                last_modified: now_iso(),
            };
            let dict = rec.to_json();
            st.recommenders.insert(rec.id.clone(), rec);
            ok_created(dict, 201)
        }
        "GetRecommenderConfiguration" => {
            match st.recommenders.get(&uri_param(params, "recommender-id")) {
                Some(rec) => {
                    let dict = rec.to_json();
                    ok(dict)
                }
                None => recommender_not_found(),
            }
        }
        "GetRecommenderConfigurations" => {
            let items: Vec<Value> = st.recommenders.values().map(Recommender::to_json).collect();
            ok(json!({ "Item": items }))
        }
        "UpdateRecommenderConfiguration" | "DeleteRecommenderConfiguration" => {
            let rid = uri_param(params, "recommender-id");
            let Some(rec) = st.recommenders.get_mut(&rid) else {
                return recommender_not_found();
            };
            if op == "UpdateRecommenderConfiguration" {
                rec.update(&body);
            }
            let dict = rec.to_json();
            if op == "DeleteRecommenderConfiguration" {
                st.recommenders.remove(&rid);
            }
            ok(dict)
        }
        // --- Events / messages ----------------------------------------------------------------------
        "PutEvents" => {
            if app_guard(st, &app_id).is_err() {
                return app_not_found();
            }
            let mut results = Map::new();
            if let Some(batch) = body.get("BatchItem").and_then(Value::as_object) {
                for (batch_id, item) in batch {
                    let mut endpoint_results = Map::new();
                    if let Some(events) = item.get("Events").and_then(Value::as_object) {
                        for event_id in events.keys() {
                            endpoint_results.insert(
                                event_id.clone(),
                                json!({ "StatusCode": 202, "Message": "Accepted" }),
                            );
                        }
                    }
                    results.insert(
                        batch_id.clone(),
                        json!({
                            "EndpointItemResponse": { "StatusCode": 202, "Message": "Accepted" },
                            "EventsItemResponse": Value::Object(endpoint_results),
                        }),
                    );
                }
            }
            ok(json!({ "Results": Value::Object(results) }))
        }
        "SendMessages" | "SendUsersMessages" | "SendOTPMessage" | "VerifyOTPMessage" => {
            if app_guard(st, &app_id).is_err() {
                return app_not_found();
            }
            let result = match op {
                "SendMessages" => {
                    let mut result = Map::new();
                    for id in body_key_names(&body, "Addresses") {
                        result.insert(id, message_status());
                    }
                    for id in body_key_names(&body, "Endpoints") {
                        result.insert(id, message_status());
                    }
                    json!({
                        "ApplicationId": app_id,
                        "RequestId": gen_id(),
                        "Result": Value::Object(result),
                    })
                }
                "SendUsersMessages" => {
                    let mut result = Map::new();
                    for id in body_key_names(&body, "Users") {
                        result.insert(id, json!({ "default": message_status() }));
                    }
                    json!({
                        "ApplicationId": app_id,
                        "RequestId": gen_id(),
                        "Result": Value::Object(result),
                    })
                }
                "SendOTPMessage" => json!({ "MessageId": gen_id() }),
                _ => json!({ "Valid": true }),
            };
            ok(result)
        }
        "PhoneNumberValidate" => ok(phone_number_validate(&body)),
        "RemoveAttributes" => {
            if app_guard(st, &app_id).is_err() {
                return app_not_found();
            }
            let blacklist = body.get("Blacklist").cloned().unwrap_or(json!([]));
            ok(json!({
                "ApplicationId": app_id,
                "AttributeType": uri_param(params, "attribute-type"),
                "Attributes": blacklist,
            }))
        }
        other => json_error(
            501,
            "NotImplemented",
            &format!("The {other} action has not been implemented"),
        ),
    }
}

// ---------------------------------------------------------------------------
// Small helpers used by the match above
// ---------------------------------------------------------------------------

fn template_key(name: &str, template_type: &str) -> String {
    format!("{name}/{template_type}")
}

/// Accepted-message body for Update/Delete template responses.
fn accepted_message_body(name: &str) -> Value {
    json!({ "MessageBody": { "Message": "Accepted", "RequestID": name } })
}

/// The event-stream put helper (keeps the big match readable).
struct EventStreamPut;

impl EventStreamPut {
    fn apply(app: &mut App, body: &Value) {
        app.event_stream = Some(EventStream {
            stream_arn: str_field(body, "DestinationStreamArn"),
            role_arn: str_field(body, "RoleArn"),
            last_modified: now_iso(),
        });
    }
}

/// app-owned resource slot only when the app exists; AppNotFound otherwise.
fn app_resources_guarded<'a>(
    st: &'a mut PinpointStore,
    app_id: &str,
) -> Option<&'a mut AppResources> {
    if st.apps.contains_key(app_id) {
        Some(app_resources(st, app_id))
    } else {
        None
    }
}

/// moto's `create_app`: invents the app id and tags its ARN.
fn create_app(req: &Request, st: &mut PinpointStore, body: &Value) -> Response {
    let id = gen_id();
    let app = App {
        id: id.clone(),
        arn: mobile_targeting_arn(&req.region, req.account, &format!("apps/{id}")),
        name: body.get("Name").cloned().unwrap_or(Value::Null),
        created: now_unix(),
        settings: json!({}),
        settings_modified: now_iso(),
        event_stream: None,
    };
    st.apps.insert(id.clone(), app);
    let dict = st.apps[&id].to_json();
    let arn = dict["Arn"].as_str().unwrap_or("").to_string();
    if let Some(tags) = body.get("tags") {
        tag_resource(st, &arn, tags);
    }
    ok_created(dict, 201)
}

/// The top-level member names of a body member (moto iterates `dict.keys()`).
fn body_key_names(body: &Value, member: &str) -> Vec<String> {
    body.get(member)
        .and_then(Value::as_object)
        .map(|m| m.keys().cloned().collect())
        .unwrap_or_default()
}

/// Message status dict repeated for every sent address/endpoint.
fn message_status() -> Value {
    json!({
        "DeliveryStatus": "SUCCESSFUL",
        "StatusCode": 200,
        "StatusMessage": "Message sent",
    })
}

// ---------------------------------------------------------------------------
// Response helpers
// ---------------------------------------------------------------------------

fn ok(body: Value) -> Response {
    Response::json(200, serde_json::to_vec(&body).unwrap_or_default())
}

fn ok_created(body: Value, status: u16) -> Response {
    Response::json(status, serde_json::to_vec(&body).unwrap_or_default())
}

fn ok_empty() -> Response {
    ok(json!({}))
}

/// moto's exception wire form (`JsonRESTError`): 404 + `NotFoundException`.
fn not_found(message: &str) -> Response {
    json_error(404, "NotFoundException", message)
}

fn app_not_found() -> Response {
    not_found("Application not found")
}

fn campaign_not_found() -> Response {
    not_found("Campaign not found")
}

fn segment_not_found() -> Response {
    not_found("Segment not found")
}

fn journey_not_found() -> Response {
    not_found("Journey not found")
}

fn template_not_found() -> Response {
    not_found("Template not found")
}

fn endpoint_not_found() -> Response {
    not_found("Endpoint not found")
}

fn job_not_found(is_export: bool) -> Response {
    if is_export {
        not_found("Export job not found")
    } else {
        not_found("Import job not found")
    }
}

fn recommender_not_found() -> Response {
    not_found("Recommender configuration not found")
}

/// KPI stub — moto's `get_*_date_range_kpi` shape.
fn kpi_json(id_field: &str, id_value: &str, app_id: &str, kpi_name: &str) -> Value {
    json!({
        "ApplicationId": app_id,
        id_field: id_value,
        "KpiName": kpi_name,
        "KpiResult": { "Rows": [] },
        "StartTime": now_iso(),
        "EndTime": now_iso(),
    })
}

/// moto's `phone_number_validate` fixed response.
fn phone_number_validate(body: &Value) -> Value {
    let number = str_field(body, "PhoneNumber");
    let iso = body
        .get("IsoCountryCode")
        .and_then(Value::as_str)
        .unwrap_or("US");
    json!({
        "Carrier": "Unknown",
        "City": "Unknown",
        "CleansedPhoneNumberE164": number,
        "CleansedPhoneNumberNational": number,
        "Country": iso,
        "CountryCodeIso2": iso,
        "CountryCodeNumeric": "1",
        "County": "Unknown",
        "OriginalCountryCodeIso2": iso,
        "OriginalPhoneNumber": number,
        "PhoneType": "MOBILE",
        "PhoneTypeCode": 0,
        "Timezone": "America/New_York",
        "ZipCode": "00000",
    })
}

/// A new endpoint (moto's `Endpoint` model constructor).
fn new_endpoint(app_id: &str, id: &str, params: &Value) -> Endpoint {
    let user_id = params
        .get("User")
        .and_then(|u| u.get("UserId"))
        .and_then(Value::as_str)
        .unwrap_or("")
        .to_string();
    let created = now_iso();
    let effective = params
        .get("EffectiveDate")
        .and_then(Value::as_str)
        .unwrap_or(&created)
        .to_string();
    Endpoint {
        id: id.to_string(),
        app_id: app_id.to_string(),
        params: params.clone(),
        created,
        effective_date: effective,
        user_id,
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------
#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;
    use std::collections::HashMap;
    use std::sync::atomic::{AtomicU64, Ordering};

    static ACCOUNTS: AtomicU64 = AtomicU64::new(300_000_000_001);

    /// Every test takes a fresh account so the shared store stays isolated.
    fn next_account() -> u64 {
        ACCOUNTS.fetch_add(1, Ordering::SeqCst)
    }

    fn call(account: u64, method: &str, path: &str, body: Value) -> (u16, Value) {
        call_query(account, method, path, "", body)
    }

    fn call_query(
        account: u64,
        method: &str,
        path: &str,
        query: &str,
        body: Value,
    ) -> (u16, Value) {
        let request = Request {
            service: "pinpoint".to_string(),
            operation: String::new(),
            method: method.to_string(),
            path: path.to_string(),
            query_string: query.to_string(),
            headers: HashMap::new(),
            body: Bytes::from(serde_json::to_vec(&body).unwrap()),
            account,
            region: "us-east-1".to_string(),
        };
        let resp = dispatch(&request).expect("pinpoint dispatch");
        let value = serde_json::from_slice(&resp.body).unwrap_or(json!({}));
        (resp.status, value)
    }

    fn create_app(account: u64, name: &str) -> String {
        let (status, body) = call(
            account,
            "POST",
            "/v1/apps",
            json!({ "Name": name, "tags": { "env": "dev" } }),
        );
        assert_eq!(status, 201, "create app failed: {body}");
        // The raw body *is* the ApplicationResponse — botocore unfolds the
        // payload member; these unit tests index the raw body.
        body["Id"].as_str().unwrap().to_string()
    }

    /// The URL encoding the AWS SDKs apply to path parameters.
    fn param_encode(value: &str) -> String {
        urlencoding::encode(value).to_string()
    }

    #[test]
    fn route_resolves_botocore_uris_and_params() {
        let cases = [
            ("POST", "/v1/apps", "CreateApp", ""),
            ("GET", "/v1/apps/abc123", "GetApp", "application-id=abc123"),
            (
                "DELETE",
                "/v1/apps/abc123",
                "DeleteApp",
                "application-id=abc123",
            ),
            (
                "GET",
                "/v1/apps/abc/campaigns/def/versions/2",
                "GetCampaignVersion",
                "application-id=abc|campaign-id=def|version=2",
            ),
            (
                "GET",
                "/v1/templates/foo/EMAIL/versions",
                "ListTemplateVersions",
                "template-name=foo|template-type=EMAIL",
            ),
            (
                "PUT",
                "/v1/templates/foo/email/active-version",
                "UpdateTemplateActiveVersion",
                "template-name=foo|template-type=email",
            ),
            (
                "GET",
                "/v1/tags/arn%3Aaws%3Amobiletargeting%3Aus-east-1%3A1%3Aapps%2Fxy",
                "ListTagsForResource",
                "resource-arn=arn:aws:mobiletargeting:us-east-1:1:apps/xy",
            ),
            (
                "POST",
                "/v1/tags/arn%3Aaws%3Amobiletargeting%3Aus-east-1%3A1%3Aapps%2Fxy",
                "TagResource",
                "resource-arn=arn:aws:mobiletargeting:us-east-1:1:apps/xy",
            ),
            (
                "GET",
                "/v1/apps/abc/settings",
                "GetApplicationSettings",
                "application-id=abc",
            ),
            (
                "GET",
                "/v1/apps/abc/campaigns",
                "GetCampaigns",
                "application-id=abc",
            ),
            ("GET", "/v1/templates", "ListTemplates", ""),
            (
                "POST",
                "/v1/apps/abc/events",
                "PutEvents",
                "application-id=abc",
            ),
            (
                "DELETE",
                "/v1/tags/x%2Fyz",
                "UntagResource",
                "resource-arn=x/yz",
            ),
        ];
        for (method, path, expected_op, expected_params) in cases {
            let got = route(method, path);
            if expected_op.is_empty() {
                assert!(got.is_none(), "expected no route for {method} {path}");
                continue;
            }
            let (op, params) = got.unwrap_or_else(|| panic!("no route for {method} {path}"));
            assert_eq!(op, expected_op, "op for {method} {path}");
            let expected: BTreeMap<String, String> = expected_params
                .split('|')
                .filter(|s| !s.is_empty())
                .map(|pair| {
                    let (k, v) = pair.split_once('=').unwrap();
                    (k.to_string(), v.to_string())
                })
                .collect();
            assert_eq!(params, expected, "params for {method} {path}");
        }
    }

    #[test]
    fn app_lifecycle_and_settings() {
        let account = next_account();
        let (status, body) = call(account, "POST", "/v1/apps", json!({ "Name": "my-app" }));
        assert_eq!(status, 201);
        assert_eq!(body["Name"], "my-app");
        assert!(body["Arn"]
            .as_str()
            .unwrap()
            .starts_with("arn:aws:mobiletargeting:us-east-1:"));
        let app_id = body["Id"].as_str().unwrap().to_string();

        let (_, body) = call(account, "GET", &format!("/v1/apps/{app_id}"), json!({}));
        assert_eq!(body["Name"], "my-app");

        let (status, body) = call(
            account,
            "PUT",
            &format!("/v1/apps/{app_id}/settings"),
            json!({ "Limits": { "Daily": 200 } }),
        );
        assert_eq!(status, 200);
        assert_eq!(body["Limits"]["Daily"], 200);
        assert_eq!(body["ApplicationId"], app_id.as_str());

        let (_, body) = call(
            account,
            "GET",
            &format!("/v1/apps/{app_id}/settings"),
            json!({}),
        );
        assert_eq!(body["Limits"]["Daily"], 200);
        assert_eq!(body["CloudWatchMetricsEnabled"], false);

        let (status, body) = call(account, "GET", "/v1/apps/gone-app", json!({}));
        assert_eq!(status, 404);
        assert_eq!(body["__type"], "NotFoundException");
        assert_eq!(body["message"], "Application not found");

        let (status, body) = call(account, "DELETE", &format!("/v1/apps/{app_id}"), json!({}));
        assert_eq!(status, 200);
        assert_eq!(body["Id"], app_id.as_str());
        let (status, _) = call(account, "GET", &format!("/v1/apps/{app_id}"), json!({}));
        assert_eq!(status, 404);
    }

    #[test]
    fn tags_merge_overwrite_and_untag() {
        let account = next_account();
        let app_id = create_app(account, "tagged");
        let (_, body) = call(account, "GET", &format!("/v1/apps/{app_id}"), json!({}));
        let arn = body["Arn"].as_str().unwrap().to_string();
        let arn_path = format!("/v1/tags/{}", param_encode(&arn));

        // tags set at create time are visible
        let (_, body) = call(account, "GET", &arn_path, json!({}));
        assert_eq!(body["tags"]["env"], "dev");

        // merge semantics: add one, overwrite one
        call(
            account,
            "POST",
            &arn_path,
            json!({ "tags": { "env": "prod", "team": "qa" } }),
        );
        let (_, body) = call(account, "GET", &arn_path, json!({}));
        assert_eq!(body["tags"]["env"], "prod");
        assert_eq!(body["tags"]["team"], "qa");

        // untag without tagKeys must not crash (moto 500s here)
        let (status, _) = call_query(account, "DELETE", &arn_path, "", json!({}));
        assert_eq!(status, 200);

        let (status, _) = call_query(account, "DELETE", &arn_path, "tagKeys=env", json!({}));
        assert_eq!(status, 200);
        let (_, body) = call(account, "GET", &arn_path, json!({}));
        assert_eq!(body["tags"]["team"], "qa");
        assert!(body["tags"].get("env").is_none());
    }

    #[test]
    fn campaign_lifecycle_versioning_and_errors() {
        let account = next_account();
        let app_id = create_app(account, "campaigns");

        // unknown app → ApplicationNotFound; unknown campaign → CampaignNotFound
        let (status, body) = call(
            account,
            "POST",
            "/v1/apps/gone/campaigns",
            json!({ "Name": "x" }),
        );
        assert_eq!(status, 404);
        assert_eq!(body["message"], "Application not found");
        let (status, body) = call(
            account,
            "GET",
            &format!("/v1/apps/{app_id}/campaigns/nope"),
            json!({}),
        );
        assert_eq!(status, 404);
        assert_eq!(body["message"], "Campaign not found");

        let (status, body) = call(
            account,
            "POST",
            &format!("/v1/apps/{app_id}/campaigns"),
            json!({ "Name": "first", "SegmentId": "seg-1" }),
        );
        assert_eq!(status, 201);
        assert_eq!(body["Name"], "first");
        assert_eq!(body["SegmentId"], "seg-1");
        assert_eq!(body["State"]["CampaignStatus"], "PAUSED");
        assert_eq!(body["Version"], 1);
        let cid = body["Id"].as_str().unwrap().to_string();

        // update bumps the version; the state stays paused
        let (_, body) = call(
            account,
            "PUT",
            &format!("/v1/apps/{app_id}/campaigns/{cid}"),
            json!({ "Name": "second" }),
        );
        assert_eq!(body["Version"], 2);
        assert_eq!(body["Name"], "second");

        let (_, body) = call(
            account,
            "GET",
            &format!("/v1/apps/{app_id}/campaigns/{cid}/versions"),
            json!({}),
        );
        assert_eq!(body["Item"].as_array().unwrap().len(), 2);

        let (_, body) = call(
            account,
            "GET",
            &format!("/v1/apps/{app_id}/campaigns/{cid}/versions/1"),
            json!({}),
        );
        assert_eq!(body["Name"], "first");

        // out-of-range and non-numeric versions answer NotFound
        let (status, _) = call(
            account,
            "GET",
            &format!("/v1/apps/{app_id}/campaigns/{cid}/versions/9"),
            json!({}),
        );
        assert_eq!(status, 404);
        let (status, _) = call(
            account,
            "GET",
            &format!("/v1/apps/{app_id}/campaigns/{cid}/versions/wat"),
            json!({}),
        );
        assert_eq!(status, 404);

        let (_, body) = call(
            account,
            "GET",
            &format!("/v1/apps/{app_id}/campaigns/{cid}/activities"),
            json!({}),
        );
        assert_eq!(body["Item"].as_array().unwrap().len(), 0);

        let (status, body) = call(
            account,
            "DELETE",
            &format!("/v1/apps/{app_id}/campaigns/{cid}"),
            json!({}),
        );
        assert_eq!(status, 200);
        assert_eq!(body["Id"], cid.as_str());
    }

    #[test]
    fn segment_lifecycle_with_versions() {
        let account = next_account();
        let app_id = create_app(account, "segments");
        let (status, body) = call(
            account,
            "POST",
            &format!("/v1/apps/{app_id}/segments"),
            json!({ "Name": "s1", "Dimensions": { "Demographic": {} } }),
        );
        assert_eq!(status, 201);
        let sid = body["Id"].as_str().unwrap().to_string();
        assert_eq!(body["SegmentType"], "DIMENSIONAL");

        let (_, body) = call(
            account,
            "PUT",
            &format!("/v1/apps/{app_id}/segments/{sid}"),
            json!({ "Name": "s2", "SegmentType": "IMPORT" }),
        );
        assert_eq!(body["Name"], "s2");
        assert_eq!(body["SegmentType"], "DIMENSIONAL");

        let (_, body) = call(
            account,
            "GET",
            &format!("/v1/apps/{app_id}/segments/{sid}/versions"),
            json!({}),
        );
        assert_eq!(body["Item"].as_array().unwrap().len(), 2);

        let (_, body) = call(
            account,
            "GET",
            &format!("/v1/apps/{app_id}/segments/{sid}/versions/1"),
            json!({}),
        );
        assert_eq!(body["Name"], "s1");

        let (status, body) = call(
            account,
            "DELETE",
            &format!("/v1/apps/{app_id}/segments/{sid}"),
            json!({}),
        );
        assert_eq!(status, 200);
        assert_eq!(body["Id"], sid.as_str());
    }

    #[test]
    fn journey_lifecycle_and_state() {
        let account = next_account();
        let app_id = create_app(account, "journeys");
        let (status, body) = call(
            account,
            "POST",
            &format!("/v1/apps/{app_id}/journeys"),
            json!({ "Name": "j1" }),
        );
        assert_eq!(status, 201);
        let jid = body["Id"].as_str().unwrap().to_string();
        assert_eq!(body["State"], "DRAFT");

        let (_, body) = call(
            account,
            "PUT",
            &format!("/v1/apps/{app_id}/journeys/{jid}/state"),
            json!({ "State": "CANCELLED" }),
        );
        assert_eq!(body["State"], "CANCELLED");

        let (_, body) = call(
            account,
            "GET",
            &format!("/v1/apps/{app_id}/journeys/{jid}/runs/run-9/execution-metrics"),
            json!({}),
        );
        assert_eq!(body["RunId"], "run-9");
        assert_eq!(body["JourneyId"], jid.as_str());

        // metric lookups on nonexistent journeys raise NotFound
        let (status, body) = call(
            account,
            "GET",
            &format!("/v1/apps/{app_id}/journeys/gone/execution-metrics"),
            json!({}),
        );
        assert_eq!(status, 404);
        assert_eq!(body["message"], "Journey not found");

        let (status, _) = call(
            account,
            "DELETE",
            &format!("/v1/apps/{app_id}/journeys/{jid}"),
            json!({}),
        );
        assert_eq!(status, 200);
    }

    #[test]
    fn template_round_trip_per_type() {
        let account = next_account();
        let (status, body) = call(
            account,
            "POST",
            "/v1/templates/my-email/email",
            json!({ "Subject": "Hello", "TextPart": "Body" }),
        );
        assert_eq!(status, 201);
        assert_eq!(body["CreateTemplateMessageBody"]["Message"], "Created");
        assert_eq!(body["CreateTemplateMessageBody"]["RequestID"], "my-email");

        let (_, body) = call(account, "GET", "/v1/templates/my-email/email", json!({}));
        assert_eq!(body["TemplateName"], "my-email");
        assert_eq!(body["TemplateType"], "EMAIL");
        assert_eq!(body["Version"], "1");

        let (_, body) = call(
            account,
            "PUT",
            "/v1/templates/my-email/email",
            json!({ "Subject": "New" }),
        );
        assert_eq!(body["MessageBody"]["Message"], "Accepted");

        let (_, body) = call(
            account,
            "GET",
            "/v1/templates/my-email/EMAIL/versions",
            json!({}),
        );
        let versions = body["Item"].as_array().unwrap();
        assert_eq!(versions.len(), 2);
        assert_eq!(versions[0]["Version"], "1");
        assert_eq!(versions[1]["Version"], "2");

        let (status, _) = call(
            account,
            "PUT",
            "/v1/templates/my-email/EMAIL/active-version",
            json!({ "Version": "2" }),
        );
        assert_eq!(status, 200);

        // in-app templates echo moto's wrapper keys
        let (status, body) = call(account, "POST", "/v1/templates/my-inapp/inapp", json!({}));
        assert_eq!(status, 201);
        assert!(body.get("CreateTemplateMessageBody").is_some());
        let (_, body) = call(account, "GET", "/v1/templates/my-inapp/inapp", json!({}));
        assert_eq!(body["TemplateName"], "my-inapp");

        let (status, _) = call(account, "DELETE", "/v1/templates/my-email/email", json!({}));
        assert_eq!(status, 200);
        let (status, _) = call(account, "GET", "/v1/templates/my-email/email", json!({}));
        assert_eq!(status, 404);
        assert_eq!(
            call(account, "GET", "/v1/templates/none/voice", json!({})).0,
            404
        );
    }

    #[test]
    fn channels_get_update_delete_respond_like_moto() {
        let account = next_account();
        let app_id = create_app(account, "channels");

        // first read invents a disabled channel (and stores it)
        let (_, body) = call(
            account,
            "GET",
            &format!("/v1/apps/{app_id}/channels/adm"),
            json!({}),
        );
        assert_eq!(body["Platform"], "ADM");
        assert_eq!(body["Enabled"], false);
        assert_eq!(body["ApplicationId"], app_id.as_str());

        let (_, body) = call(
            account,
            "GET",
            &format!("/v1/apps/{app_id}/channels/apns_voip_sandbox"),
            json!({}),
        );
        assert_eq!(body["Platform"], "APNS_VOIP_SANDBOX");

        // update creates with default enabled = true
        let (_, body) = call(
            account,
            "PUT",
            &format!("/v1/apps/{app_id}/channels/email"),
            json!({ "FromAddress": "a@b.com" }),
        );
        assert_eq!(body["Platform"], "EMAIL");
        assert_eq!(body["Enabled"], true);
        assert_eq!(body["FromAddress"], "a@b.com");

        // the channels listing includes stored types
        let (_, body) = call(
            account,
            "GET",
            &format!("/v1/apps/{app_id}/channels"),
            json!({}),
        );
        let channels = body["Channels"].as_object().unwrap();
        assert!(channels.contains_key("ADM"));
        assert!(channels.contains_key("EMAIL"));

        let (status, body) = call(
            account,
            "DELETE",
            &format!("/v1/apps/{app_id}/channels/email"),
            json!({}),
        );
        assert_eq!(status, 200);
        assert_eq!(body["ApplicationId"], app_id.as_str());

        // deleting a never-created type answers a disabled channel
        let (_, body) = call(
            account,
            "DELETE",
            &format!("/v1/apps/{app_id}/channels/baidu"),
            json!({}),
        );
        assert_eq!(body["Enabled"], false);
    }

    #[test]
    fn endpoints_crud_batch_and_users() {
        let account = next_account();
        let app_id = create_app(account, "endpoints");

        let (status, body) = call(
            account,
            "PUT",
            &format!("/v1/apps/{app_id}/endpoints/ep-1"),
            json!({ "Address": "a@b.com", "User": { "UserId": "u-9" } }),
        );
        assert_eq!(status, 200);
        assert_eq!(body["MessageBody"]["Message"], "Accepted");
        assert_eq!(body["MessageBody"]["RequestID"], "ep-1");

        let (_, body) = call(
            account,
            "GET",
            &format!("/v1/apps/{app_id}/endpoints/ep-1"),
            json!({}),
        );
        assert_eq!(body["Id"], "ep-1");
        assert_eq!(body["Address"], "a@b.com");
        assert_eq!(body["CohortId"], "1");

        let (status, body) = call(
            account,
            "PUT",
            &format!("/v1/apps/{app_id}/endpoints"),
            json!({ "Item": [{ "Id": "ep-2" }, { "Id": "ep-3" }] }),
        );
        assert_eq!(status, 200);
        assert_eq!(body["Result"]["ep-2"]["StatusCode"], 202);

        let (_, body) = call(
            account,
            "GET",
            &format!("/v1/apps/{app_id}/users/u-9"),
            json!({}),
        );
        let items = body["Item"].as_array().unwrap();
        assert_eq!(items.len(), 1);
        assert_eq!(items[0]["Id"], "ep-1");

        let (status, body) = call(
            account,
            "DELETE",
            &format!("/v1/apps/{app_id}/users/u-9"),
            json!({}),
        );
        assert_eq!(status, 200);
        assert_eq!(body["Item"].as_array().unwrap().len(), 1);
        let (status, _) = call(
            account,
            "GET",
            &format!("/v1/apps/{app_id}/endpoints/ep-1"),
            json!({}),
        );
        assert_eq!(status, 404);

        let (status, _) = call(
            account,
            "GET",
            &format!("/v1/apps/{app_id}/endpoints/gone"),
            json!({}),
        );
        assert_eq!(status, 404);
    }

    #[test]
    fn jobs_recommenders_and_messaging() {
        let account = next_account();
        let app_id = create_app(account, "jobs");

        // the segment filter needs a real segment; create one
        let (_, body) = call(
            account,
            "POST",
            &format!("/v1/apps/{app_id}/segments"),
            json!({ "Name": "jobs-seg" }),
        );
        let seg_id = body["Id"].as_str().unwrap().to_string();

        let (status, body) = call(
            account,
            "POST",
            &format!("/v1/apps/{app_id}/jobs/export"),
            json!({
                "RoleArn": "arn:aws:iam::1:role/x",
                "S3UrlPrefix": "s3://b/p",
                "SegmentId": seg_id,
            }),
        );
        assert_eq!(status, 201);
        assert_eq!(body["Type"], "EXPORT");
        assert_eq!(body["JobStatus"], "COMPLETED");
        let job_id = body["Id"].as_str().unwrap().to_string();

        let (_, body) = call(
            account,
            "GET",
            &format!("/v1/apps/{app_id}/jobs/export/{job_id}"),
            json!({}),
        );
        assert_eq!(body["Id"], job_id.as_str());

        let (_, body) = call(
            account,
            "GET",
            &format!("/v1/apps/{app_id}/segments/{seg_id}/jobs/export"),
            json!({}),
        );
        assert_eq!(body["Item"].as_array().unwrap().len(), 1);

        let (status, _) = call(
            account,
            "GET",
            &format!("/v1/apps/{app_id}/jobs/import/none"),
            json!({}),
        );
        assert_eq!(status, 404);

        let (status, body) = call(
            account,
            "POST",
            "/v1/recommenders",
            json!({ "RecommendationProviderUri": "arn:aws:personalize:us-east-1:1:campaign/x" }),
        );
        assert_eq!(status, 201);
        let rid = body["Id"].as_str().unwrap().to_string();
        let (status, body) = call(
            account,
            "PUT",
            &format!("/v1/recommenders/{rid}"),
            json!({ "RecommendationProviderUri": "arn:aws:personalize:us-east-1:1:campaign/y" }),
        );
        assert_eq!(status, 200);
        assert_eq!(
            body["RecommendationProviderUri"],
            "arn:aws:personalize:us-east-1:1:campaign/y"
        );
        let (status, _) = call(
            account,
            "DELETE",
            &format!("/v1/recommenders/{rid}"),
            json!({}),
        );
        assert_eq!(status, 200);

        let (status, body) = call(
            account,
            "POST",
            &format!("/v1/apps/{app_id}/messages"),
            json!({ "Addresses": { "a@b.com": {} } }),
        );
        assert_eq!(status, 200);
        assert_eq!(body["Result"]["a@b.com"]["DeliveryStatus"], "SUCCESSFUL");

        let (status, body) = call(
            account,
            "POST",
            &format!("/v1/apps/{app_id}/otp"),
            json!({ "Channel": "SMS", "ReferenceId": "ref" }),
        );
        assert_eq!(status, 200);
        assert!(body["MessageId"].as_str().is_some());

        let (_, body) = call(
            account,
            "POST",
            &format!("/v1/apps/{app_id}/verify-otp"),
            json!({ "Otp": "123456" }),
        );
        assert_eq!(body["Valid"], true);

        let (_, body) = call(
            account,
            "POST",
            "/v1/phone/number/validate",
            json!({ "PhoneNumber": "+12065551234" }),
        );
        assert_eq!(body["PhoneType"], "MOBILE");

        let (_, body) = call(
            account,
            "POST",
            &format!("/v1/apps/{app_id}/events"),
            json!({ "BatchItem": { "ep1": { "Events": { "evt1": {} } } } }),
        );
        assert_eq!(
            body["Results"]["ep1"]["EventsItemResponse"]["evt1"]["StatusCode"],
            202
        );

        let (_, body) = call(
            account,
            "PUT",
            &format!("/v1/apps/{app_id}/attributes/endpoint-custom-attributes"),
            json!({ "Blacklist": ["attr1"] }),
        );
        assert_eq!(body["Attributes"][0], "attr1");
    }

    #[test]
    fn event_stream_round_trip_and_normalized_missing() {
        let account = next_account();
        let app_id = create_app(account, "streams");

        // no stream yet: Get/Delete answer the modeled 404 (moto 500s)
        let (status, body) = call(
            account,
            "GET",
            &format!("/v1/apps/{app_id}/eventstream"),
            json!({}),
        );
        assert_eq!(status, 404);
        assert_eq!(body["message"], "Resource not found");

        let (status, body) = call(
            account,
            "POST",
            &format!("/v1/apps/{app_id}/eventstream"),
            json!({
                "DestinationStreamArn": "arn:aws:kinesis:us-east-1:1:stream/s",
                "RoleArn": "arn:aws:iam::1:role/r",
            }),
        );
        assert_eq!(status, 200);
        assert_eq!(
            body["DestinationStreamArn"],
            "arn:aws:kinesis:us-east-1:1:stream/s"
        );
        assert_eq!(body["ApplicationId"], app_id.as_str());

        let (status, _) = call(
            account,
            "DELETE",
            &format!("/v1/apps/{app_id}/eventstream"),
            json!({}),
        );
        assert_eq!(status, 200);
        let (status, _) = call(
            account,
            "GET",
            &format!("/v1/apps/{app_id}/eventstream"),
            json!({}),
        );
        assert_eq!(status, 404);
    }

    #[test]
    fn unknown_operations_answer_not_implemented() {
        let account = next_account();
        let (status, body) = call(account, "POST", "/v1/definitely-not-real", json!({}));
        assert_eq!(status, 501);
        assert_eq!(body["__type"], "NotImplemented");
    }

    #[test]
    fn foreign_service_is_untouched() {
        let account = next_account();
        let request = Request {
            service: "s3".to_string(),
            operation: String::new(),
            method: "GET".to_string(),
            path: "/v1/apps".to_string(),
            query_string: String::new(),
            headers: HashMap::new(),
            body: Bytes::new(),
            account,
            region: "us-east-1".to_string(),
        };
        assert!(dispatch(&request).is_none());
    }

    #[test]
    fn partition_follows_moto_region_prefixes() {
        assert_eq!(partition("us-east-1"), "aws");
        assert_eq!(partition("cn-north-1"), "aws-cn");
        assert_eq!(partition("us-gov-west-1"), "aws-us-gov");
        assert_eq!(partition("us-iso-east-1"), "aws-iso");
        assert_eq!(partition("us-isob-east-1"), "aws-iso-b");
        assert_eq!(partition("eusc-de-east-1"), "aws-eusc");
        assert_eq!(partition(""), "aws");
    }
}
