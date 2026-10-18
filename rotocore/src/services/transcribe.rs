//! AWS Transcribe backend — ported from moto's `transcribe` service.
//!
//! Wire behavior mirrors the moto sidecar (x-amz-json-1.1):
//! - transcription/medical-job statuses advance one tick per describe
//!   (None → QUEUED → IN_PROGRESS → COMPLETED), vocabularies advance
//!   None → PENDING → READY (moto's state_manager "manual, times=1");
//!   call-analytics and medical-scribe jobs are immediately COMPLETED;
//! - timestamps serialize as epoch-second f64s and null-valued members are
//!   pruned (moto's botocore-shape-sensitive serializer, reproduced).

use crate::proto::parse_json_body;
use crate::store;
use crate::{Request, Response};
use once_cell::sync::Lazy;
use serde_json::{json, Map, Value};
use std::collections::HashMap;
use uuid::Uuid;

static SLOT: Lazy<store::Slot<TranscribeState>> =
    Lazy::new(|| store::slot::<TranscribeState>("transcribe"));

#[derive(Default)]
struct TranscribeState {
    transcription_jobs: HashMap<String, TranscriptionJob>,
    medical_transcription_jobs: HashMap<String, TranscriptionJob>,
    vocabularies: HashMap<String, Vocabulary>,
    medical_vocabularies: HashMap<String, Vocabulary>,
    vocabulary_filters: HashMap<String, VocabularyFilter>,
    language_models: HashMap<String, LanguageModel>,
    call_analytics_categories: HashMap<String, CallAnalyticsCategory>,
    call_analytics_jobs: HashMap<String, SimpleJob>,
    medical_scribe_jobs: HashMap<String, SimpleJob>,
    /// Resource tags keyed by full resource ARN (moto's `_resource_tags`).
    resource_tags: HashMap<String, Vec<Value>>,
    region: String,
    account: String,
}

struct TranscriptionJob {
    name: String,
    medical: bool,
    status: Option<&'static str>,
    language_code: Option<String>,
    language_codes: Option<Vec<Value>>,
    media_sample_rate_hertz: Option<Value>,
    media_format: Option<String>,
    media: Value,
    transcript: Option<Value>,
    start_time: Option<f64>,
    completion_time: Option<f64>,
    creation_time: f64,
    settings: Value,
    job_execution_settings: Option<Value>,
    identify_language: Option<bool>,
    identify_multiple_languages: Option<bool>,
    language_options: Option<Value>,
    identified_language_score: Option<f64>,
    output_bucket_name: Option<String>,
    output_key: Option<String>,
    output_location_type: &'static str,
    subtitles: Value,
    /// Only used by medical jobs.
    specialty: String,
    job_type: String,
}

#[derive(Clone)]
struct Vocabulary {
    language_code: String,
    status: Option<&'static str>,
    last_modified_time: Option<f64>,
    download_uri: String,
}

struct VocabularyFilter {
    language_code: Option<String>,
    file_uri: Option<Value>,
    data_access_role_arn: Option<Value>,
    last_modified_time: f64,
}

struct LanguageModel {
    language_code: String,
    base_model_name: String,
    input_data_config: Value,
    create_time: f64,
    last_modified_time: f64,
}

struct CallAnalyticsCategory {
    category_name: String,
    rules: Value,
    input_type: Option<Value>,
    tags: Option<Value>,
    create_time: f64,
    last_update_time: f64,
}

/// Call-analytics and medical-scribe jobs: immediately COMPLETED states.
#[derive(Clone)]
struct SimpleJob {
    media: Value,
    creation_time: f64,
    settings: Option<Value>,
    data_access_role_arn: Option<Value>,
    channel_definitions: Option<Value>,
}

pub fn dispatch(req: &Request) -> Response {
    let params = parse_json_body(&req.body);
    let mut guard = SLOT.write();
    let st = guard.entry((req.account, req.region.clone())).or_default();
    if st.region.is_empty() {
        st.region = req.region.clone();
        st.account = req.account.to_string();
    }
    match req.operation.as_str() {
        "StartTranscriptionJob" => st.op_start_transcription_job(&params),
        "GetTranscriptionJob" => st.op_get_transcription_job(&params),
        "ListTranscriptionJobs" => st.op_list_transcription_jobs(&params),
        "DeleteTranscriptionJob" => st.op_delete_transcription_job(&params),
        "StartMedicalTranscriptionJob" => st.op_start_medical_transcription_job(&params),
        "GetMedicalTranscriptionJob" => st.op_get_medical_transcription_job(&params),
        "ListMedicalTranscriptionJobs" => st.op_list_medical_transcription_jobs(&params),
        "DeleteMedicalTranscriptionJob" => st.op_delete_medical_transcription_job(&params),
        "CreateVocabulary" => st.op_create_vocabulary(&params, false),
        "CreateMedicalVocabulary" => st.op_create_vocabulary(&params, true),
        "GetVocabulary" | "GetMedicalVocabulary" => st.op_get_vocabulary(&params),
        "ListVocabularies" => st.op_list_vocabularies(&params, false),
        "ListMedicalVocabularies" => st.op_list_vocabularies(&params, true),
        "DeleteVocabulary" => st.op_delete_vocabulary(&params, false),
        "DeleteMedicalVocabulary" => st.op_delete_vocabulary(&params, true),
        "UpdateMedicalVocabulary" => st.op_update_medical_vocabulary(&params),
        "CreateVocabularyFilter" => st.op_create_vocabulary_filter(&params),
        "GetVocabularyFilter" => st.op_get_vocabulary_filter(&params),
        "UpdateVocabularyFilter" => st.op_update_vocabulary_filter(&params),
        "ListVocabularyFilters" => st.op_list_vocabulary_filters(&params),
        "DeleteVocabularyFilter" => st.op_delete_vocabulary_filter(&params),
        "CreateLanguageModel" => st.op_create_language_model(&params),
        "DescribeLanguageModel" => st.op_describe_language_model(&params),
        "ListLanguageModels" => st.op_list_language_models(&params),
        "DeleteLanguageModel" => st.op_delete_language_model(&params),
        "CreateCallAnalyticsCategory" => st.op_create_call_analytics_category(&params),
        "GetCallAnalyticsCategory" => st.op_get_call_analytics_category(&params),
        "UpdateCallAnalyticsCategory" => st.op_update_call_analytics_category(&params),
        "DeleteCallAnalyticsCategory" => st.op_delete_call_analytics_category(&params),
        "ListCallAnalyticsCategories" => st.op_list_call_analytics_categories(&params),
        "StartCallAnalyticsJob" => st.op_start_call_analytics_job(&params),
        "GetCallAnalyticsJob" => st.op_get_call_analytics_job(&params),
        "DeleteCallAnalyticsJob" => st.op_delete_call_analytics_job(&params),
        "ListCallAnalyticsJobs" => st.op_list_call_analytics_jobs(&params),
        "StartMedicalScribeJob" => st.op_start_medical_scribe_job(&params),
        "GetMedicalScribeJob" => st.op_get_medical_scribe_job(&params),
        "DeleteMedicalScribeJob" => st.op_delete_medical_scribe_job(&params),
        "ListMedicalScribeJobs" => st.op_list_medical_scribe_jobs(&params),
        "TagResource" => st.op_tag_resource(&params),
        "UntagResource" => st.op_untag_resource(&params),
        "ListTagsForResource" => st.op_list_tags_for_resource(&params),
        _ => crate::proto::json_error(
            501,
            "NotImplemented",
            &format!("The {} action has not been implemented", req.operation),
        ),
    }
}

// --- Shared helpers --------------------------------------------------------

fn now_ts() -> f64 {
    chrono::Utc::now().timestamp_millis() as f64 / 1000.0
}

fn bad_request(message: &str) -> Response {
    Response::json_status(
        400,
        &json!({ "__type": "BadRequestException", "message": message }),
    )
}

fn conflict(message: &str) -> Response {
    Response::json_status(
        400,
        &json!({ "__type": "ConflictException", "message": message }),
    )
}

fn ok(body: Value) -> Response {
    Response::json_status(200, &body)
}

fn empty() -> Response {
    ok(json!({}))
}

/// Prune null-valued members of an object (moto serializer behavior).
fn without_nulls(v: &Value) -> Value {
    match v {
        Value::Object(map) => {
            let mut out = Map::new();
            for (k, val) in map {
                if !val.is_null() {
                    out.insert(k.clone(), val.clone());
                }
            }
            Value::Object(out)
        }
        other => other.clone(),
    }
}

/// Offset pagination shared by every list op: (page items, NextToken?).
fn paginate(
    items: &[Value],
    next_token: Option<&Value>,
    max_results: Option<&Value>,
) -> (Vec<Value>, Option<String>) {
    let start = next_token
        .and_then(Value::as_str)
        .and_then(|s| s.parse::<usize>().ok())
        .unwrap_or(0);
    let end = start
        + max_results
            .and_then(Value::as_u64)
            .map(|m| m as usize)
            .unwrap_or(100);
    let end = end.min(items.len());
    let token = if end < items.len() {
        Some(end.to_string())
    } else {
        None
    };
    (items.get(start..end).unwrap_or(&[]).to_vec(), token)
}

/// "path/to/file.json" with a single trailing ".json" removed.
fn strip_json_suffix(s: &str) -> String {
    match s.strip_suffix(".json") {
        Some(base) => base.to_string(),
        None => s.to_string(),
    }
}

impl TranscriptionJob {
    #[allow(clippy::too_many_arguments)]
    fn regular(
        name: &str,
        language_code: Option<String>,
        media_sample_rate_hertz: Option<Value>,
        media_format: Option<String>,
        media: Value,
        output_bucket_name: Option<String>,
        output_key: Option<String>,
        settings: Option<Value>,
        job_execution_settings: Option<Value>,
        identify_language: Option<bool>,
        identify_multiple_languages: Option<bool>,
        language_options: Option<Value>,
        subtitles: Value,
    ) -> Self {
        let has_customer_bucket = output_bucket_name.is_some();
        Self {
            name: name.to_string(),
            medical: false,
            status: None,
            language_code,
            language_codes: None,
            media_sample_rate_hertz,
            media_format,
            media,
            transcript: None,
            start_time: None,
            completion_time: None,
            creation_time: now_ts(),
            settings: settings.unwrap_or_else(|| {
                json!({"ChannelIdentification": false, "ShowAlternatives": false, "ShowSpeakerLabels": false})
            }),
            job_execution_settings,
            identify_language,
            identify_multiple_languages,
            language_options,
            identified_language_score: None,
            output_bucket_name,
            output_key,
            output_location_type: if has_customer_bucket {
                "CUSTOMER_BUCKET"
            } else {
                "SERVICE_BUCKET"
            },
            subtitles,
            specialty: String::new(),
            job_type: String::new(),
        }
    }

    #[allow(clippy::too_many_arguments)]
    fn medical(
        name: &str,
        language_code: Option<String>,
        media_sample_rate_hertz: Option<Value>,
        media_format: Option<String>,
        media: Value,
        output_bucket_name: Option<String>,
        specialty: String,
        job_type: String,
        settings: Option<Value>,
    ) -> Self {
        let medical_settings = settings
            .unwrap_or_else(|| json!({"ChannelIdentification": false, "ShowAlternatives": false}));
        let mut job = Self::regular(
            name,
            language_code,
            media_sample_rate_hertz,
            media_format,
            media,
            Some(output_bucket_name.unwrap_or_default()),
            None,
            Some(medical_settings),
            None,
            None,
            None,
            None,
            json!({"Formats": [], "OutputStartIndex": 0}),
        );
        job.medical = true;
        job.output_location_type = "CUSTOMER_BUCKET";
        job.specialty = specialty;
        job.job_type = job_type;
        job
    }

    /// One describe tick: None → QUEUED → IN_PROGRESS → COMPLETED, mirroring
    /// moto's ManagedState with progression "manual", times=1.
    fn advance(&mut self, region: &str, account: &str) {
        let next = match self.status {
            None => Some("QUEUED"),
            Some("QUEUED") => Some("IN_PROGRESS"),
            Some("IN_PROGRESS") => Some("COMPLETED"),
            Some(_) => return,
        };
        self.status = next;
        match next {
            Some("IN_PROGRESS") => {
                self.start_time = Some(now_ts());
                if self
                    .media_sample_rate_hertz
                    .as_ref()
                    .map(Value::is_null)
                    .unwrap_or(true)
                {
                    self.media_sample_rate_hertz = Some(json!(44100));
                }
                if self.media_format.is_none() {
                    let ext = self
                        .media
                        .get("MediaFileUri")
                        .and_then(Value::as_str)
                        .and_then(|uri| uri.rsplit('.').next())
                        .unwrap_or("")
                        .to_lowercase();
                    self.media_format =
                        Some(if ["mp3", "mp4", "wav", "flac"].contains(&ext.as_str()) {
                            ext
                        } else {
                            "mp3".to_string()
                        });
                }
                if self.identify_language == Some(true) {
                    self.identified_language_score = Some(0.999645948);
                    self.language_code = Some(self.language_any(0, "en-US"));
                }
                if self.identify_multiple_languages == Some(true) {
                    self.identified_language_score = Some(0.999645948);
                    let first = self.language_any(0, "en-US");
                    let mut codes =
                        vec![json!({"LanguageCode": first, "DurationInSeconds": 123.0})];
                    if let Some(second) = self
                        .language_options
                        .as_ref()
                        .and_then(Value::as_array)
                        .and_then(|a| a.get(1))
                        .and_then(Value::as_str)
                    {
                        codes.push(json!({"LanguageCode": second, "DurationInSeconds": 321.0}));
                    }
                    self.language_codes = Some(codes);
                }
            }
            Some("COMPLETED") => {
                self.completion_time = Some(now_ts() + 10.0);
                let prefix = if self.medical {
                    format!(
                        "https://s3.{region}.amazonaws.com/{bucket}/medical/{name}.json",
                        bucket = self.output_bucket_name.clone().unwrap_or_default(),
                        name = self.name
                    )
                } else {
                    match &self.output_bucket_name {
                        Some(bucket) => {
                            let key_base =
                                strip_json_suffix(self.output_key.as_deref().unwrap_or(&self.name));
                            format!("https://s3.{region}.amazonaws.com/{bucket}/{key_base}")
                        }
                        None => format!(
                            "https://s3.{region}.amazonaws.com/aws-transcribe-{region}-prod/{account}/{}/{}/asrOutput",
                            self.name,
                            Uuid::new_v4()
                        ),
                    }
                };
                self.transcript = Some(json!({"TranscriptFileUri": format!("{prefix}.json")}));
                let formats = self
                    .subtitles
                    .get("Formats")
                    .and_then(Value::as_array)
                    .cloned()
                    .unwrap_or_default();
                let uris: Vec<Value> = formats
                    .iter()
                    .filter_map(Value::as_str)
                    .map(|f| json!(format!("{prefix}.{f}")))
                    .collect();
                self.subtitles["SubtitleFileUris"] = json!(uris);
            }
            _ => {}
        }
    }

    fn language_any(&self, index: usize, fallback: &str) -> String {
        self.language_options
            .as_ref()
            .and_then(Value::as_array)
            .and_then(|a| a.get(index))
            .and_then(Value::as_str)
            .unwrap_or(fallback)
            .to_string()
    }

    /// StartTranscriptionJob response payload (`TranscriptionJob` value).
    fn create_summary(&self) -> Value {
        let job_execution = self
            .job_execution_settings
            .clone()
            .unwrap_or_else(|| json!({"AllowDeferredExecution": false}));
        let mut body = json!({
            "TranscriptionJobName": self.name,
            "LanguageCode": self.language_code.clone(),
            "MediaFormat": self.media_format.clone(),
            "Media": self.media.clone(),
            "CreationTime": self.creation_time,
            "Settings": without_nulls(&self.settings),
            "JobExecutionSettings": without_nulls(&job_execution),
            "Subtitles": self.subtitles.clone(),
        });
        if self.identify_language.is_some() {
            body["IdentifyLanguage"] = json!(self.identify_language);
        }
        if self.identify_multiple_languages.is_some() {
            body["IdentifyMultipleLanguages"] = json!(self.identify_multiple_languages);
        }
        if let Some(opts) = &self.language_options {
            body["LanguageOptions"] = opts.clone();
        }
        without_nulls(&body)
    }

    /// GetTranscriptionJob response payload.
    fn get_detail(&self) -> Value {
        without_nulls(&json!({
            "TranscriptionJobName": self.name,
            "TranscriptionJobStatus": self.status,
            "LanguageCode": self.language_code.clone(),
            "LanguageCodes": self.language_codes.clone(),
            "MediaSampleRateHertz": self.media_sample_rate_hertz.clone(),
            "MediaFormat": self.media_format.clone(),
            "Media": self.media.clone(),
            "Settings": without_nulls(&self.settings),
            "Transcript": self.transcript.clone(),
            "StartTime": self.start_time,
            "CreationTime": self.creation_time,
            "CompletionTime": self.completion_time,
            "IdentifyLanguage": self.identify_language,
            "IdentifyMultipleLanguages": self.identify_multiple_languages,
            "LanguageOptions": self.language_options.clone(),
            "IdentifiedLanguageScore": self.identified_language_score,
            "Subtitles": self.subtitles.clone(),
        }))
    }

    /// ListTranscriptionJobs summary entry.
    fn list_summary(&self) -> Value {
        without_nulls(&json!({
            "TranscriptionJobName": self.name,
            "CreationTime": self.creation_time,
            "StartTime": self.start_time,
            "CompletionTime": self.completion_time,
            "LanguageCode": self.language_code.clone(),
            "LanguageCodes": self.language_codes.clone(),
            "TranscriptionJobStatus": self.status,
            "IdentifyLanguage": self.identify_language,
            "IdentifyMultipleLanguages": self.identify_multiple_languages,
            "IdentifiedLanguageScore": self.identified_language_score,
            "OutputLocationType": self.output_location_type,
        }))
    }

    /// StartMedicalTranscriptionJob response payload (`MedicalTranscriptionJob`).
    fn medical_create_summary(&self) -> Value {
        without_nulls(&json!({
            "MedicalTranscriptionJobName": self.name,
            "TranscriptionJobStatus": self.status,
            "LanguageCode": self.language_code.clone(),
            "MediaFormat": self.media_format.clone(),
            "Media": self.media.clone(),
            "StartTime": self.start_time,
            "CreationTime": self.creation_time,
            "Specialty": self.specialty.clone(),
            "Type": self.job_type.clone(),
        }))
    }

    /// GetMedicalTranscriptionJob response payload.
    fn medical_get_detail(&self) -> Value {
        without_nulls(&json!({
            "MedicalTranscriptionJobName": self.name,
            "TranscriptionJobStatus": self.status,
            "LanguageCode": self.language_code.clone(),
            "MediaSampleRateHertz": self.media_sample_rate_hertz.clone(),
            "MediaFormat": self.media_format.clone(),
            "Media": self.media.clone(),
            "Transcript": self.transcript.clone(),
            "StartTime": self.start_time,
            "CreationTime": self.creation_time,
            "CompletionTime": self.completion_time,
            "Settings": without_nulls(&self.settings),
            "Specialty": self.specialty.clone(),
            "Type": self.job_type.clone(),
        }))
    }

    /// ListMedicalTranscriptionJobs summary entry.
    fn medical_list_summary(&self) -> Value {
        without_nulls(&json!({
            "MedicalTranscriptionJobName": self.name,
            "CreationTime": self.creation_time,
            "StartTime": self.start_time,
            "CompletionTime": self.completion_time,
            "LanguageCode": self.language_code.clone(),
            "TranscriptionJobStatus": self.status,
            "OutputLocationType": self.output_location_type,
            "Specialty": self.specialty.clone(),
            "Type": self.job_type.clone(),
        }))
    }
}

impl Vocabulary {
    /// One describe tick: None → PENDING → READY, stamping last-modified on
    /// each observed transition (moto's manual, times=1 progression).
    fn advance(&mut self) {
        self.status = match self.status {
            None => {
                self.last_modified_time = Some(now_ts());
                Some("PENDING")
            }
            Some("PENDING") => {
                self.last_modified_time = Some(now_ts());
                Some("READY")
            }
            other => other,
        };
    }
}

impl TranscribeState {
    fn region(&self) -> &str {
        if self.region.is_empty() {
            "us-east-1"
        } else {
            &self.region
        }
    }

    fn account_id(&self) -> u64 {
        self.account.parse::<u64>().unwrap_or(123456789012)
    }

    // ------------------------------------------------------------------
    // Transcription jobs
    // ------------------------------------------------------------------

    fn op_start_transcription_job(&mut self, p: &Value) -> Response {
        let Some(name) = p.get("TranscriptionJobName").and_then(Value::as_str) else {
            return bad_request("A transcription job name is required.");
        };
        if self.transcription_jobs.contains_key(name) {
            return conflict("The requested job name already exists. Use a different job name.");
        }
        let settings = p.get("Settings").cloned();
        if let Some(vocab) = settings.as_ref().and_then(|s| s.get("VocabularyName")) {
            if !self
                .vocabularies
                .contains_key(vocab.as_str().unwrap_or_default())
            {
                return bad_request(
                    "The requested vocabulary couldn't be found. Check the vocabulary name and try your request again.",
                );
            }
        }
        let job = TranscriptionJob::regular(
            name,
            p.get("LanguageCode")
                .and_then(Value::as_str)
                .map(String::from),
            p.get("MediaSampleRateHertz").cloned(),
            p.get("MediaFormat")
                .and_then(Value::as_str)
                .map(String::from),
            p.get("Media").cloned().unwrap_or_else(|| json!({})),
            p.get("OutputBucketName")
                .and_then(Value::as_str)
                .map(String::from),
            p.get("OutputKey").and_then(Value::as_str).map(String::from),
            settings,
            p.get("JobExecutionSettings").cloned(),
            p.get("IdentifyLanguage").and_then(Value::as_bool),
            p.get("IdentifyMultipleLanguages").and_then(Value::as_bool),
            p.get("LanguageOptions").cloned(),
            p.get("Subtitles")
                .cloned()
                .unwrap_or_else(|| json!({"Formats": [], "OutputStartIndex": 0})),
        );
        self.transcription_jobs.insert(name.to_string(), job);
        let job = &self.transcription_jobs[name];
        ok(json!({"TranscriptionJob": without_nulls(&job.create_summary())}))
    }

    fn op_get_transcription_job(&mut self, p: &Value) -> Response {
        let name = p
            .get("TranscriptionJobName")
            .and_then(Value::as_str)
            .unwrap_or("");
        let (region, account) = (self.region().to_string(), self.account_id().to_string());
        let Some(job) = self.transcription_jobs.get_mut(name) else {
            return bad_request(
                "The requested job couldn't be found. Check the job name and try your request again.",
            );
        };
        job.advance(&region, &account);
        let detail = job.get_detail();
        ok(json!({"TranscriptionJob": detail}))
    }

    fn op_delete_transcription_job(&mut self, p: &Value) -> Response {
        let name = p
            .get("TranscriptionJobName")
            .and_then(Value::as_str)
            .unwrap_or("");
        if self.transcription_jobs.remove(name).is_none() {
            return bad_request(
                "The requested job couldn't be found. Check the job name and try your request again.",
            );
        }
        empty()
    }

    fn op_list_transcription_jobs(&mut self, p: &Value) -> Response {
        let status_filter = p.get("Status").and_then(Value::as_str);
        let contains = p.get("JobNameContains").and_then(Value::as_str);
        let mut items: Vec<Value> = Vec::new();
        for job in self.transcription_jobs.values() {
            // moto's status property does not advance on filter reads.
            if let Some(f) = status_filter.map(String::from) {
                if job.status.map(|s| s != f).unwrap_or(true) {
                    continue;
                }
            }
            if let Some(c) = contains {
                if !job.name.contains(c) {
                    continue;
                }
            }
            items.push(job.list_summary());
        }
        let (page, token) = paginate(&items, p.get("NextToken"), p.get("MaxResults"));
        let mut body = json!({"TranscriptionJobSummaries": page});
        if let Some(t) = token {
            body["NextToken"] = json!(t);
        }
        if let Some(f) = status_filter {
            body["Status"] = json!(f);
        }
        ok(body)
    }

    // ------------------------------------------------------------------
    // Medical transcription jobs
    // ------------------------------------------------------------------

    fn op_start_medical_transcription_job(&mut self, p: &Value) -> Response {
        let Some(name) = p.get("MedicalTranscriptionJobName").and_then(Value::as_str) else {
            return bad_request("A medical transcription job name is required.");
        };
        if self.medical_transcription_jobs.contains_key(name) {
            return conflict("The requested job name already exists. Use a different job name.");
        }
        let settings = p.get("Settings").cloned();
        if let Some(vocab) = settings.as_ref().and_then(|s| s.get("VocabularyName")) {
            if !self
                .medical_vocabularies
                .contains_key(vocab.as_str().unwrap_or_default())
            {
                return bad_request(
                    "The requested vocabulary couldn't be found. Check the vocabulary name and try your request again.",
                );
            }
        }
        let job = TranscriptionJob::medical(
            name,
            p.get("LanguageCode")
                .and_then(Value::as_str)
                .map(String::from),
            p.get("MediaSampleRateHertz").cloned(),
            p.get("MediaFormat")
                .and_then(Value::as_str)
                .map(String::from),
            p.get("Media").cloned().unwrap_or_else(|| json!({})),
            p.get("OutputBucketName")
                .and_then(Value::as_str)
                .map(String::from),
            p.get("Specialty")
                .and_then(Value::as_str)
                .unwrap_or("")
                .to_string(),
            p.get("Type")
                .and_then(Value::as_str)
                .unwrap_or("")
                .to_string(),
            settings,
        );
        self.medical_transcription_jobs
            .insert(name.to_string(), job);
        let job = &self.medical_transcription_jobs[name];
        ok(json!({"MedicalTranscriptionJob": job.medical_create_summary()}))
    }

    fn op_get_medical_transcription_job(&mut self, p: &Value) -> Response {
        let name = p
            .get("MedicalTranscriptionJobName")
            .and_then(Value::as_str)
            .unwrap_or("");
        let (region, account) = (self.region().to_string(), self.account_id().to_string());
        let Some(job) = self.medical_transcription_jobs.get_mut(name) else {
            return bad_request(
                "The requested job couldn't be found. Check the job name and try your request again.",
            );
        };
        job.advance(&region, &account);
        let detail = job.medical_get_detail();
        ok(json!({"MedicalTranscriptionJob": detail}))
    }

    fn op_delete_medical_transcription_job(&mut self, p: &Value) -> Response {
        let name = p
            .get("MedicalTranscriptionJobName")
            .and_then(Value::as_str)
            .unwrap_or("");
        if self.medical_transcription_jobs.remove(name).is_none() {
            return bad_request(
                "The requested job couldn't be found. Check the job name and try your request again.",
            );
        }
        empty()
    }

    fn op_list_medical_transcription_jobs(&mut self, p: &Value) -> Response {
        let status_filter = p.get("Status").and_then(Value::as_str);
        let contains = p.get("JobNameContains").and_then(Value::as_str);
        let mut items: Vec<Value> = Vec::new();
        for (name, job) in self.medical_transcription_jobs.iter() {
            if let Some(f) = status_filter.map(String::from) {
                if job.status.map(|s| s != f).unwrap_or(true) {
                    continue;
                }
            }
            if let Some(c) = contains {
                if !name.contains(c) {
                    continue;
                }
            }
            items.push(job.medical_list_summary());
        }
        let (page, token) = paginate(&items, p.get("NextToken"), p.get("MaxResults"));
        let mut body = json!({"MedicalTranscriptionJobSummaries": page});
        if let Some(t) = token {
            body["NextToken"] = json!(t);
        }
        if let Some(f) = status_filter {
            body["Status"] = json!(f);
        }
        ok(body)
    }

    // ------------------------------------------------------------------
    // Vocabularies
    // ------------------------------------------------------------------

    fn op_create_vocabulary(&mut self, p: &Value, medical: bool) -> Response {
        let phrases = p.get("Phrases");
        let file_uri = p.get("VocabularyFileUri");
        if !medical {
            if phrases.is_some() == file_uri.is_some() {
                // exactly one of the two must be provided
                return bad_request(
                    "Either Phrases or VocabularyFileUri field should be provided.",
                );
            }
            if let Some(list) = phrases.and_then(Value::as_array) {
                if list.is_empty() {
                    return bad_request(
                        "1 validation error detected: Value '[]' at 'phrases' failed to satisfy constraint: Member must have length greater than or equal to 1",
                    );
                }
            }
        }
        let Some(name) = p.get("VocabularyName").and_then(Value::as_str) else {
            return bad_request("A vocabulary name is required.");
        };
        let (region, account) = (self.region().to_string(), self.account_id());
        let store = if medical {
            &mut self.medical_vocabularies
        } else {
            &mut self.vocabularies
        };
        if store.contains_key(name) {
            return conflict(
                "The requested vocabulary name already exists. Use a different vocabulary name.",
            );
        }
        let download_uri = if medical {
            format!(
                "https://s3.us-east-1.amazonaws.com/aws-transcribe-dictionary-model-{region}-prod/{account}/medical/{name}/{}/input.txt",
                Uuid::new_v4()
            )
        } else {
            format!(
                "https://s3.{region}.amazonaws.com/aws-transcribe-dictionary-model-{region}-prod/{account}/{name}/{}/input.txt",
                Uuid::new_v4()
            )
        };
        store.insert(
            name.to_string(),
            Vocabulary {
                language_code: p
                    .get("LanguageCode")
                    .and_then(Value::as_str)
                    .unwrap_or("")
                    .to_string(),
                status: None,
                last_modified_time: None,
                download_uri,
            },
        );
        // VocabularyState is None at CREATE — the moto serializer omits it.
        ok(json!({
            "VocabularyName": name,
            "LanguageCode": store[name].language_code.clone(),
        }))
    }

    fn op_get_vocabulary(&mut self, p: &Value) -> Response {
        let name = p
            .get("VocabularyName")
            .and_then(Value::as_str)
            .unwrap_or("");
        let store = if self.vocabularies.contains_key(name) {
            self.vocabularies.get_mut(name)
        } else {
            self.medical_vocabularies.get_mut(name)
        };
        let Some(v) = store else {
            return bad_request(
                "The requested vocabulary couldn't be found. Check the vocabulary name and try your request again.",
            );
        };
        v.advance();
        let detail = without_nulls(&json!({
            "VocabularyName": name,
            "LanguageCode": v.language_code.clone(),
            "VocabularyState": v.status,
            "LastModifiedTime": v.last_modified_time,
            "FailureReason": None::<String>,
            "DownloadUri": v.download_uri.clone(),
        }));
        ok(detail)
    }

    fn op_list_vocabularies(&mut self, p: &Value, medical: bool) -> Response {
        let state_filter = p.get("StateEquals").and_then(Value::as_str);
        let contains = p.get("NameContains").and_then(Value::as_str);
        let store = if medical {
            &self.medical_vocabularies as &HashMap<String, Vocabulary>
        } else {
            &self.vocabularies
        };
        let mut items = Vec::new();
        for (name, v) in store.iter() {
            if let Some(f) = state_filter.map(String::from) {
                if v.status.map(|s| s != f).unwrap_or(true) {
                    continue;
                }
            }
            if let Some(c) = contains {
                if !name.contains(c) {
                    continue;
                }
            }
            items.push(without_nulls(&json!({
                "VocabularyName": name,
                "LanguageCode": v.language_code.clone(),
                "LastModifiedTime": v.last_modified_time,
                "VocabularyState": v.status,
            })));
        }
        let (page, token) = paginate(&items, p.get("NextToken"), p.get("MaxResults"));
        let mut body = json!({"Vocabularies": page});
        if let Some(t) = token {
            body["NextToken"] = json!(t);
        }
        if let Some(f) = state_filter {
            body["Status"] = json!(f);
        }
        ok(body)
    }

    fn op_delete_vocabulary(&mut self, p: &Value, medical: bool) -> Response {
        let name = p
            .get("VocabularyName")
            .and_then(Value::as_str)
            .unwrap_or("");
        let removed = if medical {
            self.medical_vocabularies.remove(name)
        } else {
            self.vocabularies.remove(name)
        };
        if removed.is_none() {
            return bad_request(
                "The requested vocabulary couldn't be found. Check the vocabulary name and try your request again.",
            );
        }
        empty()
    }

    fn op_update_medical_vocabulary(&mut self, p: &Value) -> Response {
        let name = p
            .get("VocabularyName")
            .and_then(Value::as_str)
            .unwrap_or("");
        let Some(v) = self.medical_vocabularies.get_mut(name) else {
            return bad_request(
                "The requested vocabulary couldn't be found. Check the vocabulary name and try your request again.",
            );
        };
        if let Some(code) = p.get("LanguageCode").and_then(Value::as_str) {
            v.language_code = code.to_string();
        }
        v.advance();
        let detail = without_nulls(&json!({
            "VocabularyName": name,
            "LanguageCode": v.language_code.clone(),
            "LastModifiedTime": v.last_modified_time,
            "VocabularyState": v.status,
        }));
        ok(detail)
    }

    // ------------------------------------------------------------------
    // Vocabulary filters
    // ------------------------------------------------------------------

    fn op_create_vocabulary_filter(&mut self, p: &Value) -> Response {
        let Some(name) = p.get("VocabularyFilterName").and_then(Value::as_str) else {
            return bad_request("A vocabulary filter name is required.");
        };
        if self.vocabulary_filters.contains_key(name) {
            return conflict(
                "The requested vocabulary filter name already exists. Use a different vocabulary filter name.",
            );
        }
        if p.get("Words").is_none() && p.get("VocabularyFilterFileUri").is_none() {
            return bad_request(
                "Either Words or VocabularyFilterFileUri field should be provided.",
            );
        }
        self.vocabulary_filters.insert(
            name.to_string(),
            VocabularyFilter {
                language_code: p
                    .get("LanguageCode")
                    .and_then(Value::as_str)
                    .map(String::from),
                file_uri: p
                    .get("VocabularyFilterFileUri")
                    .filter(|x| !x.is_null())
                    .cloned(),
                data_access_role_arn: p.get("DataAccessRoleArn").filter(|x| !x.is_null()).cloned(),
                last_modified_time: now_ts(),
            },
        );
        let created = &self.vocabulary_filters[name];
        ok(without_nulls(&json!({
            "VocabularyFilterName": name,
            "LanguageCode": created.language_code.clone(),
            "LastModifiedTime": created.last_modified_time,
        })))
    }

    fn op_get_vocabulary_filter(&mut self, p: &Value) -> Response {
        let name = p
            .get("VocabularyFilterName")
            .and_then(Value::as_str)
            .unwrap_or("");
        let Some(f) = self.vocabulary_filters.get(name) else {
            return bad_request(
                "The requested vocabulary filter couldn't be found. Check the vocabulary filter name and try your request again.",
            );
        };
        ok(without_nulls(&json!({
            "VocabularyFilterName": name,
            "LanguageCode": f.language_code.clone(),
            "LastModifiedTime": f.last_modified_time,
            "DownloadUri": None::<String>,
        })))
    }

    fn op_update_vocabulary_filter(&mut self, p: &Value) -> Response {
        let name = p
            .get("VocabularyFilterName")
            .and_then(Value::as_str)
            .unwrap_or("");
        let Some(f) = self.vocabulary_filters.get_mut(name) else {
            return bad_request(
                "The requested vocabulary filter couldn't be found. Check the vocabulary filter name and try your request again.",
            );
        };
        if p.get("Words").filter(|x| !x.is_null()).is_some() {
            f.file_uri = None;
        }
        if let Some(uri) = p.get("VocabularyFilterFileUri").filter(|x| !x.is_null()) {
            f.file_uri = Some(uri.clone());
        }
        if let Some(arn) = p.get("DataAccessRoleArn").filter(|x| !x.is_null()) {
            f.data_access_role_arn = Some(arn.clone());
        }
        f.last_modified_time = now_ts();
        ok(without_nulls(&json!({
            "VocabularyFilterName": name,
            "LanguageCode": f.language_code.clone(),
            "LastModifiedTime": f.last_modified_time,
        })))
    }

    fn op_list_vocabulary_filters(&mut self, p: &Value) -> Response {
        let contains = p.get("NameContains").and_then(Value::as_str);
        let mut items = Vec::new();
        for (name, f) in self.vocabulary_filters.iter() {
            if let Some(c) = contains {
                if !name.contains(c) {
                    continue;
                }
            }
            items.push(without_nulls(&json!({
                "VocabularyFilterName": name,
                "LanguageCode": f.language_code.clone(),
                "LastModifiedTime": f.last_modified_time,
            })));
        }
        let (page, token) = paginate(&items, p.get("NextToken"), p.get("MaxResults"));
        let mut body = json!({"VocabularyFilters": page});
        if let Some(t) = token {
            body["NextToken"] = json!(t);
        }
        ok(body)
    }

    fn op_delete_vocabulary_filter(&mut self, p: &Value) -> Response {
        let name = p
            .get("VocabularyFilterName")
            .and_then(Value::as_str)
            .unwrap_or("");
        if self.vocabulary_filters.remove(name).is_none() {
            return bad_request(
                "The requested vocabulary filter couldn't be found. Check the vocabulary filter name and try your request again.",
            );
        }
        empty()
    }

    // ------------------------------------------------------------------
    // Language models
    // ------------------------------------------------------------------

    fn op_create_language_model(&mut self, p: &Value) -> Response {
        let Some(name) = p.get("ModelName").and_then(Value::as_str) else {
            return bad_request("A model name is required.");
        };
        if self.language_models.contains_key(name) {
            return conflict(
                "The requested language model name already exists. Use a different language model name.",
            );
        }
        let now = now_ts();
        self.language_models.insert(
            name.to_string(),
            LanguageModel {
                language_code: p
                    .get("LanguageCode")
                    .and_then(Value::as_str)
                    .unwrap_or("")
                    .to_string(),
                base_model_name: p
                    .get("BaseModelName")
                    .and_then(Value::as_str)
                    .unwrap_or("")
                    .to_string(),
                input_data_config: p.get("InputDataConfig").cloned().unwrap_or(json!({})),
                create_time: now,
                last_modified_time: now,
            },
        );
        let m = &self.language_models[name];
        ok(json!({
            "LanguageCode": m.language_code.clone(),
            "BaseModelName": m.base_model_name.clone(),
            "ModelName": name,
            "InputDataConfig": without_nulls(&m.input_data_config),
            "ModelStatus": "COMPLETED",
        }))
    }

    fn op_describe_language_model(&mut self, p: &Value) -> Response {
        let name = p.get("ModelName").and_then(Value::as_str).unwrap_or("");
        let Some(m) = self.language_models.get(name) else {
            return bad_request(
                "The requested language model couldn't be found. Check the language model name and try your request again.",
            );
        };
        ok(without_nulls(&json!({
            "LanguageModel": {
                "ModelName": name,
                "CreateTime": m.create_time,
                "LastModifiedTime": m.last_modified_time,
                "LanguageCode": m.language_code.clone(),
                "BaseModelName": m.base_model_name.clone(),
                "ModelStatus": "COMPLETED",
                "UpgradeAvailability": false,
                "InputDataConfig": without_nulls(&m.input_data_config),
            }
        })))
    }

    fn op_list_language_models(&mut self, p: &Value) -> Response {
        let status_filter = p.get("StatusEquals").and_then(Value::as_str);
        let contains = p.get("NameContains").and_then(Value::as_str);
        let mut items = Vec::new();
        for (name, m) in self.language_models.iter() {
            if let Some(f) = status_filter {
                if f != "COMPLETED" {
                    continue;
                }
            }
            if let Some(c) = contains {
                if !name.contains(c) {
                    continue;
                }
            }
            items.push(without_nulls(&json!({
                "ModelName": name,
                "CreateTime": m.create_time,
                "LastModifiedTime": m.last_modified_time,
                "LanguageCode": m.language_code.clone(),
                "BaseModelName": m.base_model_name.clone(),
                "ModelStatus": "COMPLETED",
                "UpgradeAvailability": false,
                "InputDataConfig": without_nulls(&m.input_data_config),
            })));
        }
        let (page, token) = paginate(&items, p.get("NextToken"), p.get("MaxResults"));
        let mut body = json!({"Models": page});
        if let Some(t) = token {
            body["NextToken"] = json!(t);
        }
        ok(body)
    }

    fn op_delete_language_model(&mut self, p: &Value) -> Response {
        let name = p.get("ModelName").and_then(Value::as_str).unwrap_or("");
        if self.language_models.remove(name).is_none() {
            return bad_request(
                "The requested language model couldn't be found. Check the language model name and try your request again.",
            );
        }
        empty()
    }

    // ------------------------------------------------------------------
    // Call analytics categories
    // ------------------------------------------------------------------

    fn op_create_call_analytics_category(&mut self, p: &Value) -> Response {
        let Some(name) = p.get("CategoryName").and_then(Value::as_str) else {
            return bad_request("A category name is required.");
        };
        if self.call_analytics_categories.contains_key(name) {
            return conflict(
                "The requested category name already exists. Use a different category name.",
            );
        }
        let now = now_ts();
        self.call_analytics_categories.insert(
            name.to_string(),
            CallAnalyticsCategory {
                category_name: name.to_string(),
                rules: p.get("Rules").cloned().unwrap_or(json!([])),
                input_type: p.get("InputType").filter(|x| !x.is_null()).cloned(),
                tags: p
                    .get("Tags")
                    .filter(|t| t.as_array().is_some_and(|a| !a.is_empty()))
                    .cloned(),
                create_time: now,
                last_update_time: now,
            },
        );
        ok(json!({"CategoryProperties": self.category_to_value(name)}))
    }

    fn op_get_call_analytics_category(&mut self, p: &Value) -> Response {
        let name = p.get("CategoryName").and_then(Value::as_str).unwrap_or("");
        if !self.call_analytics_categories.contains_key(name) {
            return bad_request(
                "The requested category couldn't be found. Check the category name and try your request again.",
            );
        }
        ok(json!({"CategoryProperties": self.category_to_value(name)}))
    }

    fn op_update_call_analytics_category(&mut self, p: &Value) -> Response {
        let name = p.get("CategoryName").and_then(Value::as_str).unwrap_or("");
        let Some(c) = self.call_analytics_categories.get_mut(name) else {
            return bad_request(
                "The requested category couldn't be found. Check the category name and try your request again.",
            );
        };
        c.rules = p.get("Rules").cloned().unwrap_or(json!([]));
        if let Some(it) = p.get("InputType").filter(|x| !x.is_null()) {
            c.input_type = Some(it.clone());
        }
        c.last_update_time = now_ts();
        ok(json!({"CategoryProperties": self.category_to_value(name)}))
    }

    fn op_delete_call_analytics_category(&mut self, p: &Value) -> Response {
        let name = p.get("CategoryName").and_then(Value::as_str).unwrap_or("");
        if self.call_analytics_categories.remove(name).is_none() {
            return bad_request(
                "The requested category couldn't be found. Check the category name and try your request again.",
            );
        }
        empty()
    }

    fn op_list_call_analytics_categories(&mut self, p: &Value) -> Response {
        let names: Vec<String> = self.call_analytics_categories.keys().cloned().collect();
        let items: Vec<Value> = names.iter().map(|n| self.category_to_value(n)).collect();
        let (page, token) = paginate(&items, p.get("NextToken"), p.get("MaxResults"));
        let mut body = json!({"Categories": page});
        if let Some(t) = token {
            body["NextToken"] = json!(t);
        }
        ok(body)
    }

    fn category_to_value(&self, name: &str) -> Value {
        let c = &self.call_analytics_categories[name];
        without_nulls(&json!({
            "CategoryName": c.category_name.clone(),
            "Rules": c.rules.clone(),
            "CreateTime": c.create_time,
            "LastUpdateTime": c.last_update_time,
            "InputType": c.input_type.clone(),
            "Tags": c.tags.clone(),
        }))
    }

    // ------------------------------------------------------------------
    // Call analytics + medical scribe jobs
    // ------------------------------------------------------------------

    fn op_start_call_analytics_job(&mut self, p: &Value) -> Response {
        let Some(name) = p.get("CallAnalyticsJobName").and_then(Value::as_str) else {
            return bad_request("A call analytics job name is required.");
        };
        if self.call_analytics_jobs.contains_key(name) {
            return conflict("The requested job name already exists. Use a different job name.");
        }
        self.call_analytics_jobs
            .insert(name.to_string(), SimpleJob::from(p));
        let job = self.call_analytics_jobs[name].clone();
        ok(json!({
            "CallAnalyticsJob": without_nulls(&json!({
                "CallAnalyticsJobName": name,
                "CallAnalyticsJobStatus": "COMPLETED",
                "Media": job.media.clone(),
                "CreationTime": job.creation_time,
                "Settings": job.settings.clone(),
                "DataAccessRoleArn": job.data_access_role_arn.clone(),
                "ChannelDefinitions": job.channel_definitions.clone(),
            }))
        }))
    }

    fn op_get_call_analytics_job(&mut self, p: &Value) -> Response {
        let name = p
            .get("CallAnalyticsJobName")
            .and_then(Value::as_str)
            .unwrap_or("");
        let Some(job) = self.call_analytics_jobs.get(name) else {
            return bad_request(
                "The requested job couldn't be found. Check the job name and try your request again.",
            );
        };
        let job = job.clone();
        ok(json!({
            "CallAnalyticsJob": without_nulls(&json!({
                "CallAnalyticsJobName": name,
                "CallAnalyticsJobStatus": "COMPLETED",
                "Media": job.media.clone(),
                "CreationTime": job.creation_time,
                "Settings": job.settings.clone(),
                "DataAccessRoleArn": job.data_access_role_arn.clone(),
                "ChannelDefinitions": job.channel_definitions.clone(),
            }))
        }))
    }

    fn op_delete_call_analytics_job(&mut self, p: &Value) -> Response {
        let name = p
            .get("CallAnalyticsJobName")
            .and_then(Value::as_str)
            .unwrap_or("");
        if self.call_analytics_jobs.remove(name).is_none() {
            return bad_request(
                "The requested job couldn't be found. Check the job name and try your request again.",
            );
        }
        empty()
    }

    fn op_list_call_analytics_jobs(&mut self, p: &Value) -> Response {
        let status_filter = p.get("Status").and_then(Value::as_str);
        let contains = p.get("JobNameContains").and_then(Value::as_str);
        let mut items = Vec::new();
        for name in self.call_analytics_jobs.keys() {
            if let Some(f) = status_filter {
                if f != "COMPLETED" {
                    continue;
                }
            }
            if let Some(c) = contains {
                if !name.contains(c) {
                    continue;
                }
            }
            items.push(without_nulls(&json!({
                "CallAnalyticsJobName": name,
                "CreationTime": self.call_analytics_jobs[name].creation_time,
                "CallAnalyticsJobStatus": "COMPLETED",
            })));
        }
        let (page, token) = paginate(&items, p.get("NextToken"), p.get("MaxResults"));
        let mut body = json!({"CallAnalyticsJobSummaries": page});
        if let Some(t) = token {
            body["NextToken"] = json!(t);
        }
        if let Some(f) = status_filter {
            body["Status"] = json!(f);
        }
        ok(body)
    }

    fn op_start_medical_scribe_job(&mut self, p: &Value) -> Response {
        let Some(name) = p.get("MedicalScribeJobName").and_then(Value::as_str) else {
            return bad_request("A medical scribe job name is required.");
        };
        if self.medical_scribe_jobs.contains_key(name) {
            return conflict("The requested job name already exists. Use a different job name.");
        }
        self.medical_scribe_jobs
            .insert(name.to_string(), SimpleJob::from(p));
        let job = self.medical_scribe_jobs[name].clone();
        ok(json!({
            "MedicalScribeJob": without_nulls(&json!({
                "MedicalScribeJobName": name,
                "MedicalScribeJobStatus": "COMPLETED",
                "LanguageCode": "en-US",
                "Media": job.media.clone(),
                "CreationTime": job.creation_time,
                "Settings": job.settings.clone(),
                "DataAccessRoleArn": job.data_access_role_arn.clone(),
                "ChannelDefinitions": job.channel_definitions.clone(),
            }))
        }))
    }

    fn op_get_medical_scribe_job(&mut self, p: &Value) -> Response {
        let name = p
            .get("MedicalScribeJobName")
            .and_then(Value::as_str)
            .unwrap_or("");
        let Some(job) = self.medical_scribe_jobs.get(name) else {
            return bad_request(
                "The requested job couldn't be found. Check the job name and try your request again.",
            );
        };
        let job = job.clone();
        ok(json!({
            "MedicalScribeJob": without_nulls(&json!({
                "MedicalScribeJobName": name,
                "MedicalScribeJobStatus": "COMPLETED",
                "LanguageCode": "en-US",
                "Media": job.media.clone(),
                "CreationTime": job.creation_time,
                "Settings": job.settings.clone(),
                "DataAccessRoleArn": job.data_access_role_arn.clone(),
                "ChannelDefinitions": job.channel_definitions.clone(),
            }))
        }))
    }

    fn op_delete_medical_scribe_job(&mut self, p: &Value) -> Response {
        let name = p
            .get("MedicalScribeJobName")
            .and_then(Value::as_str)
            .unwrap_or("");
        if self.medical_scribe_jobs.remove(name).is_none() {
            return bad_request(
                "The requested job couldn't be found. Check the job name and try your request again.",
            );
        }
        empty()
    }

    fn op_list_medical_scribe_jobs(&mut self, p: &Value) -> Response {
        let status_filter = p.get("Status").and_then(Value::as_str);
        let contains = p.get("JobNameContains").and_then(Value::as_str);
        let mut items = Vec::new();
        for name in self.medical_scribe_jobs.keys() {
            if let Some(f) = status_filter {
                if f != "COMPLETED" {
                    continue;
                }
            }
            if let Some(c) = contains {
                if !name.contains(c) {
                    continue;
                }
            }
            items.push(without_nulls(&json!({
                "MedicalScribeJobName": name,
                "CreationTime": self.medical_scribe_jobs[name].creation_time,
                "LanguageCode": "en-US",
                "MedicalScribeJobStatus": "COMPLETED",
            })));
        }
        let (page, token) = paginate(&items, p.get("NextToken"), p.get("MaxResults"));
        let mut body = json!({"MedicalScribeJobSummaries": page});
        if let Some(t) = token {
            body["NextToken"] = json!(t);
        }
        if let Some(f) = status_filter {
            body["Status"] = json!(f);
        }
        ok(body)
    }

    // ------------------------------------------------------------------
    // Resource tags
    // ------------------------------------------------------------------

    fn op_tag_resource(&mut self, p: &Value) -> Response {
        let Some(arn) = p.get("ResourceArn").and_then(Value::as_str) else {
            return bad_request("A resource ARN is required.");
        };
        if !self.find_resource_by_arn(arn) {
            return bad_request(
                "The requested resource couldn't be found. Check the ARN and try your request again.",
            );
        }
        let tags = p
            .get("Tags")
            .and_then(Value::as_array)
            .cloned()
            .unwrap_or_default();
        let entry = self.resource_tags.entry(arn.to_string()).or_default();
        for tag in tags {
            let key = tag.get("Key").and_then(Value::as_str).unwrap_or("");
            if !key.is_empty() {
                entry.retain(|t| t.get("Key").and_then(Value::as_str) != Some(key));
            }
            entry.push(tag);
        }
        empty()
    }

    fn op_untag_resource(&mut self, p: &Value) -> Response {
        let Some(arn) = p.get("ResourceArn").and_then(Value::as_str) else {
            return bad_request("A resource ARN is required.");
        };
        if !self.find_resource_by_arn(arn) {
            return bad_request(
                "The requested resource couldn't be found. Check the ARN and try your request again.",
            );
        }
        let keys: Vec<String> = p
            .get("TagKeys")
            .and_then(Value::as_array)
            .map(|a| {
                a.iter()
                    .filter_map(Value::as_str)
                    .map(String::from)
                    .collect()
            })
            .unwrap_or_default();
        if let Some(entry) = self.resource_tags.get_mut(arn) {
            entry.retain(|t| {
                let k = t.get("Key").and_then(Value::as_str).unwrap_or("");
                !keys.contains(&k.to_string())
            });
        }
        empty()
    }

    fn op_list_tags_for_resource(&mut self, p: &Value) -> Response {
        let Some(arn) = p.get("ResourceArn").and_then(Value::as_str) else {
            return bad_request("A resource ARN is required.");
        };
        if !self.find_resource_by_arn(arn) {
            return bad_request(
                "The requested resource couldn't be found. Check the ARN and try your request again.",
            );
        }
        let tags = self.resource_tags.get(arn).cloned().unwrap_or_default();
        ok(json!({"ResourceArn": arn, "Tags": tags}))
    }

    /// Resolve a resource ARN to a store hit; ARN format
    /// `arn:aws:transcribe:{region}:{account}:{type}/{name}`.
    fn find_resource_by_arn(&self, arn: &str) -> bool {
        let parts: Vec<&str> = arn.split(':').collect();
        if parts.len() < 6 {
            return false;
        }
        let Some((kind, name)) = parts[5].split_once('/') else {
            return false;
        };
        match kind {
            "vocabulary" => self.vocabularies.contains_key(name),
            "medical-vocabulary" => self.medical_vocabularies.contains_key(name),
            "transcription-job" => self.transcription_jobs.contains_key(name),
            "medical-transcription-job" => self.medical_transcription_jobs.contains_key(name),
            "language-model" => self.language_models.contains_key(name),
            "vocabulary-filter" => self.vocabulary_filters.contains_key(name),
            "call-analytics-category" => self.call_analytics_categories.contains_key(name),
            "call-analytics-job" => self.call_analytics_jobs.contains_key(name),
            "medical-scribe-job" => self.medical_scribe_jobs.contains_key(name),
            _ => false,
        }
    }
}

impl SimpleJob {
    fn from(p: &Value) -> Self {
        Self {
            media: p.get("Media").cloned().unwrap_or(json!({})),
            creation_time: now_ts(),
            settings: p
                .get("Settings")
                .filter(|x| x.as_object().is_some_and(|o| !o.is_empty()))
                .cloned(),
            data_access_role_arn: p.get("DataAccessRoleArn").filter(|x| !x.is_null()).cloned(),
            channel_definitions: p
                .get("ChannelDefinitions")
                .filter(|x| x.as_array().is_some_and(|a| !a.is_empty()))
                .cloned(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::AtomicU64;

    static ACCOUNT: AtomicU64 = AtomicU64::new(970_000_000_001);

    fn next_account() -> u64 {
        ACCOUNT.fetch_add(1, std::sync::atomic::Ordering::Relaxed)
    }

    fn call(account: u64, op: &str, params: Value) -> (u16, Value) {
        let req = Request {
            service: "transcribe".to_string(),
            operation: op.to_string(),
            method: "POST".to_string(),
            path: "/".to_string(),
            query_string: String::new(),
            headers: HashMap::new(),
            body: serde_json::to_vec(&params).unwrap().into(),
            account,
            region: "us-east-1".to_string(),
        };
        let resp = dispatch(&req);
        (
            resp.status,
            serde_json::from_slice::<Value>(&resp.body).unwrap(),
        )
    }

    fn media(job: &str) -> Value {
        json!({"MediaFileUri": format!("s3://b/{job}.wav")})
    }

    #[test]
    fn transcription_job_lifecycle() {
        let acct = next_account();
        let (s, r) = call(
            acct,
            "StartTranscriptionJob",
            json!({"TranscriptionJobName": "j1", "LanguageCode": "en-US", "Media": media("j1")}),
        );
        assert_eq!(s, 200);
        let j = &r["TranscriptionJob"];
        assert_eq!(j["TranscriptionJobName"], "j1");
        assert_eq!(j["LanguageCode"], "en-US");
        assert!(j.get("TranscriptionJobStatus").is_none());
        assert_eq!(j["Settings"]["ShowSpeakerLabels"], false);
        assert!(j["CreationTime"].as_f64().is_some());

        let (s, r) = call(
            acct,
            "GetTranscriptionJob",
            json!({"TranscriptionJobName": "j1"}),
        );
        assert_eq!(s, 200);
        assert_eq!(r["TranscriptionJob"]["TranscriptionJobStatus"], "QUEUED");
        let (_s, r) = call(
            acct,
            "GetTranscriptionJob",
            json!({"TranscriptionJobName": "j1"}),
        );
        assert_eq!(
            r["TranscriptionJob"]["TranscriptionJobStatus"],
            "IN_PROGRESS"
        );
        assert_eq!(r["TranscriptionJob"]["MediaSampleRateHertz"], 44100);
        assert_eq!(r["TranscriptionJob"]["MediaFormat"], "wav");
        let (_s, r) = call(
            acct,
            "GetTranscriptionJob",
            json!({"TranscriptionJobName": "j1"}),
        );
        assert_eq!(r["TranscriptionJob"]["TranscriptionJobStatus"], "COMPLETED");
        assert!(r["TranscriptionJob"]["Transcript"]["TranscriptFileUri"]
            .as_str()
            .unwrap()
            .contains("aws-transcribe-us-east-1-prod"));

        let (_s, r) = call(acct, "ListTranscriptionJobs", json!({}));
        let names: Vec<&str> = r["TranscriptionJobSummaries"]
            .as_array()
            .unwrap()
            .iter()
            .map(|i| i["TranscriptionJobName"].as_str().unwrap())
            .collect();
        assert!(names.contains(&"j1"));

        let (s, r) = call(
            acct,
            "DeleteTranscriptionJob",
            json!({"TranscriptionJobName": "j1"}),
        );
        assert_eq!(s, 200);
        assert_eq!(r, json!({}));
        let (s, _) = call(
            acct,
            "GetTranscriptionJob",
            json!({"TranscriptionJobName": "j1"}),
        );
        assert_eq!(s, 400);
    }

    #[test]
    fn transcription_job_conflict_and_missing() {
        let acct = next_account();
        let (s, _r) = call(
            acct,
            "StartTranscriptionJob",
            json!({"TranscriptionJobName": "dup", "LanguageCode": "en-US", "Media": media("dup")}),
        );
        assert_eq!(s, 200);
        let (s, r) = call(
            acct,
            "StartTranscriptionJob",
            json!({"TranscriptionJobName": "dup", "LanguageCode": "en-US", "Media": media("dup")}),
        );
        assert_eq!(s, 400);
        assert_eq!(r["__type"], "ConflictException");

        let (s, r) = call(
            acct,
            "GetTranscriptionJob",
            json!({"TranscriptionJobName": "zzz"}),
        );
        assert_eq!(s, 400);
        assert_eq!(r["__type"], "BadRequestException");
        // delete-missing is also BadRequest
        let (s, _) = call(
            acct,
            "DeleteTranscriptionJob",
            json!({"TranscriptionJobName": "zzz"}),
        );
        assert_eq!(s, 400);
    }

    #[test]
    fn list_filters_and_next_token() {
        let acct = next_account();
        for n in ["lm-a", "lm-b"] {
            call(
                acct,
                "StartTranscriptionJob",
                json!({"TranscriptionJobName": n, "LanguageCode": "en-US", "Media": media(n)}),
            );
        }
        // Status filter has no matches (created jobs are still None at list time).
        let (s, r) = call(
            acct,
            "ListTranscriptionJobs",
            json!({"Status": "IN_PROGRESS"}),
        );
        assert_eq!(s, 200);
        assert_eq!(r["Status"], "IN_PROGRESS");
        assert_eq!(r["TranscriptionJobSummaries"].as_array().unwrap().len(), 0);

        let (_s, r) = call(
            acct,
            "ListTranscriptionJobs",
            json!({"JobNameContains": "lm-"}),
        );
        assert_eq!(r["TranscriptionJobSummaries"].as_array().unwrap().len(), 2);

        let (_s, r) = call(acct, "ListTranscriptionJobs", json!({"MaxResults": 1}));
        assert!(r["NextToken"].is_string());
        assert_eq!(r["TranscriptionJobSummaries"].as_array().unwrap().len(), 1);
    }

    #[test]
    fn vocabulary_lifecycle_and_errors() {
        let acct = next_account();
        let (s, r) = call(
            acct,
            "CreateVocabulary",
            json!({"VocabularyName": "v1", "LanguageCode": "en-US", "Phrases": ["hi"]}),
        );
        assert_eq!(r["VocabularyName"], "v1");
        assert_eq!(r["LanguageCode"], "en-US");
        assert_eq!(s, 200);

        // duplicate create conflicts
        let (s, r) = call(
            acct,
            "CreateVocabulary",
            json!({"VocabularyName": "v1", "LanguageCode": "en-US", "Phrases": ["hi"]}),
        );
        assert_eq!(s, 400);
        assert_eq!(r["__type"], "ConflictException");

        // neither phrases nor file uri -> BadRequest
        let (s, _) = call(
            acct,
            "CreateVocabulary",
            json!({"VocabularyName": "v2", "LanguageCode": "en-US"}),
        );
        assert_eq!(s, 400);

        // get advances PENDING -> READY
        let (_s, r) = call(acct, "GetVocabulary", json!({"VocabularyName": "v1"}));
        assert_eq!(r["VocabularyState"], "PENDING");
        assert!(r["DownloadUri"]
            .as_str()
            .unwrap()
            .contains("aws-transcribe-dictionary-model"));
        let (_s, r) = call(acct, "GetVocabulary", json!({"VocabularyName": "v1"}));
        assert_eq!(r["VocabularyState"], "READY");

        let (_s, r) = call(acct, "ListVocabularies", json!({}));
        assert!(!r["Vocabularies"].as_array().unwrap().is_empty());

        let (s, _) = call(acct, "DeleteVocabulary", json!({"VocabularyName": "v1"}));
        assert_eq!(s, 200);
        let (s, _) = call(acct, "DeleteVocabulary", json!({"VocabularyName": "v1"}));
        assert_eq!(s, 400);
    }

    #[test]
    fn medical_vocabulary_update_returns_ready() {
        let acct = next_account();
        call(
            acct,
            "CreateMedicalVocabulary",
            json!({
                "VocabularyName": "mv1",
                "LanguageCode": "en-US",
                "VocabularyFileUri": "s3://b/v.txt"
            }),
        );
        let (_s, r) = call(
            acct,
            "GetMedicalVocabulary",
            json!({"VocabularyName": "mv1"}),
        );
        assert_eq!(r["VocabularyState"], "PENDING");
        let (_s, r) = call(
            acct,
            "GetMedicalVocabulary",
            json!({"VocabularyName": "mv1"}),
        );
        assert_eq!(r["VocabularyState"], "READY");
        let (_s, r) = call(
            acct,
            "UpdateMedicalVocabulary",
            json!({"VocabularyName": "mv1", "LanguageCode": "en-US"}),
        );
        assert_eq!(r["VocabularyName"], "mv1");
        assert_eq!(r["LanguageCode"], "en-US");
        assert_eq!(r["VocabularyState"], "READY");
    }

    #[test]
    fn vocabulary_filter_crud() {
        let acct = next_account();
        let (s, _) = call(
            acct,
            "CreateVocabularyFilter",
            json!({"VocabularyFilterName": "f1", "LanguageCode": "en-US", "Words": ["x"]}),
        );
        assert_eq!(s, 200);
        let (_s, r) = call(
            acct,
            "GetVocabularyFilter",
            json!({"VocabularyFilterName": "f1"}),
        );
        assert_eq!(r["VocabularyFilterName"], "f1");
        let (_s, r) = call(
            acct,
            "UpdateVocabularyFilter",
            json!({"VocabularyFilterName": "f1", "Words": ["y"]}),
        );
        assert_eq!(r["VocabularyFilterName"], "f1");
        let (s, _) = call(
            acct,
            "DeleteVocabularyFilter",
            json!({"VocabularyFilterName": "f1"}),
        );
        assert_eq!(s, 200);
        let (s, _) = call(
            acct,
            "GetVocabularyFilter",
            json!({"VocabularyFilterName": "f1"}),
        );
        assert_eq!(s, 400);
    }

    #[test]
    fn language_model_crud() {
        let acct = next_account();
        let input =
            json!({"S3Uri": "s3://b/d/", "DataAccessRoleArn": "arn:aws:iam::123456789012:role/t"});
        let (_s, r) = call(
            acct,
            "CreateLanguageModel",
            json!({
                "LanguageCode": "en-US",
                "BaseModelName": "NarrowBand",
                "ModelName": "m1",
                "InputDataConfig": input.clone()
            }),
        );
        assert_eq!(r["ModelName"], "m1");
        assert_eq!(r["ModelStatus"], "COMPLETED");
        let (_s, r) = call(acct, "DescribeLanguageModel", json!({"ModelName": "m1"}));
        let lm = &r["LanguageModel"];
        assert_eq!(lm["BaseModelName"], "NarrowBand");
        assert_eq!(lm["UpgradeAvailability"], false);
        let (_s, r) = call(acct, "ListLanguageModels", json!({}));
        assert!(!r["Models"].as_array().unwrap().is_empty());
        let (s, _) = call(acct, "DeleteLanguageModel", json!({"ModelName": "m1"}));
        assert_eq!(s, 200);
        let (s, _r) = call(acct, "DescribeLanguageModel", json!({"ModelName": "m1"}));
        assert_eq!(s, 400);
    }

    #[test]
    fn call_analytics_category_crud() {
        let acct = next_account();
        let rules = json!([{"NonTalkTimeFilter": {"Threshold": 60}}]);
        let (_s, r) = call(
            acct,
            "CreateCallAnalyticsCategory",
            json!({"CategoryName": "c1", "Rules": rules}),
        );
        let cat = &r["CategoryProperties"];
        assert_eq!(cat["CategoryName"], "c1");
        assert_eq!(cat["Rules"], rules);
        assert!(cat["CreateTime"].as_f64().is_some());
        let (_s, r) = call(
            acct,
            "UpdateCallAnalyticsCategory",
            json!({"CategoryName": "c1", "Rules": [{"NonTalkTimeFilter": {"Threshold": 90}}]}),
        );
        assert_eq!(
            r["CategoryProperties"]["Rules"][0]["NonTalkTimeFilter"]["Threshold"],
            90
        );
        let (_s, r) = call(acct, "ListCallAnalyticsCategories", json!({}));
        assert!(!r["Categories"].as_array().unwrap().is_empty());
        let (s, _) = call(
            acct,
            "DeleteCallAnalyticsCategory",
            json!({"CategoryName": "c1"}),
        );
        assert_eq!(s, 200);
        let (s, _r) = call(
            acct,
            "GetCallAnalyticsCategory",
            json!({"CategoryName": "c1"}),
        );
        assert_eq!(s, 400);
    }

    #[test]
    fn call_analytics_job_immediately_completed() {
        let acct = next_account();
        let (_s, r) = call(
            acct,
            "StartCallAnalyticsJob",
            json!({
                "CallAnalyticsJobName": "ca1",
                "Media": media("ca1"),
                "DataAccessRoleArn": "arn:aws:iam::123456789012:role/t"
            }),
        );
        let j = &r["CallAnalyticsJob"];
        assert_eq!(j["CallAnalyticsJobStatus"], "COMPLETED");
        assert_eq!(j["DataAccessRoleArn"], "arn:aws:iam::123456789012:role/t");
        let (_s, r) = call(acct, "ListCallAnalyticsJobs", json!({}));
        let summary = &r["CallAnalyticsJobSummaries"][0];
        assert_eq!(summary["CallAnalyticsJobName"], "ca1");
        // Media is shape-pruned from summaries
        assert!(summary.get("Media").is_none());
        let (s, _) = call(
            acct,
            "DeleteCallAnalyticsJob",
            json!({"CallAnalyticsJobName": "ca1"}),
        );
        assert_eq!(s, 200);
    }

    #[test]
    fn medical_scribe_job_state() {
        let acct = next_account();
        let (_s, r) = call(
            acct,
            "StartMedicalScribeJob",
            json!({
                "MedicalScribeJobName": "ms1",
                "Media": media("ms1"),
                "OutputBucketName": "out",
                "DataAccessRoleArn": "arn:aws:iam::123456789012:role/t",
                "Settings": {"ShowSpeakerLabels": false, "MaxSpeakerLabels": 2}
            }),
        );
        let j = &r["MedicalScribeJob"];
        assert_eq!(j["MedicalScribeJobStatus"], "COMPLETED");
        assert_eq!(j["LanguageCode"], "en-US");
        let (_s, r) = call(acct, "ListMedicalScribeJobs", json!({}));
        assert_eq!(
            r["MedicalScribeJobSummaries"][0]["MedicalScribeJobName"],
            "ms1"
        );
    }

    #[test]
    fn medical_job_lifecycle() {
        let acct = next_account();
        call(
            acct,
            "StartMedicalTranscriptionJob",
            json!({
                "MedicalTranscriptionJobName": "mj1",
                "LanguageCode": "en-US",
                "Media": media("mj1"),
                "OutputBucketName": "out",
                "Specialty": "PRIMARYCARE",
                "Type": "CONVERSATION"
            }),
        );
        let (_s, r) = call(
            acct,
            "GetMedicalTranscriptionJob",
            json!({"MedicalTranscriptionJobName": "mj1"}),
        );
        assert_eq!(
            r["MedicalTranscriptionJob"]["TranscriptionJobStatus"],
            "QUEUED"
        );
        call(
            acct,
            "GetMedicalTranscriptionJob",
            json!({"MedicalTranscriptionJobName": "mj1"}),
        );
        let (_s, r) = call(
            acct,
            "GetMedicalTranscriptionJob",
            json!({"MedicalTranscriptionJobName": "mj1"}),
        );
        let job = &r["MedicalTranscriptionJob"];
        assert_eq!(job["TranscriptionJobStatus"], "COMPLETED");
        assert_eq!(job["Specialty"], "PRIMARYCARE");
        assert_eq!(job["Type"], "CONVERSATION");
        assert!(job["Transcript"]["TranscriptFileUri"]
            .as_str()
            .unwrap()
            .contains("out/medical/mj1.json"));
        let (_s, r) = call(acct, "ListMedicalTranscriptionJobs", json!({}));
        let items = r["MedicalTranscriptionJobSummaries"].as_array().unwrap();
        assert_eq!(items[0]["OutputLocationType"], "CUSTOMER_BUCKET");
        let (s, _) = call(
            acct,
            "DeleteMedicalTranscriptionJob",
            json!({"MedicalTranscriptionJobName": "mj1"}),
        );
        assert_eq!(s, 200);
    }

    #[test]
    fn tags_roundtrip() {
        let acct = next_account();
        call(
            acct,
            "CreateVocabulary",
            json!({"VocabularyName": "tv", "LanguageCode": "en-US", "Phrases": ["i"]}),
        );
        let arn = format!("arn:aws:transcribe:us-east-1:{acct}:vocabulary/tv");
        let (s, _) = call(
            acct,
            "TagResource",
            json!({"ResourceArn": arn, "Tags": [{"Key": "env", "Value": "test"}]}),
        );
        assert_eq!(s, 200);
        let (_s, r) = call(acct, "ListTagsForResource", json!({"ResourceArn": arn}));
        assert_eq!(r["Tags"][0]["Key"], "env");
        call(
            acct,
            "UntagResource",
            json!({"ResourceArn": arn, "TagKeys": ["env"]}),
        );
        let (_s, r) = call(acct, "ListTagsForResource", json!({"ResourceArn": arn}));
        assert_eq!(r["Tags"].as_array().unwrap().len(), 0);
        // unknown ARN -> BadRequest
        let (s, r) = call(
            acct,
            "TagResource",
            json!({"ResourceArn": "arn:aws:transcribe:us-east-1:123456789012:vocabulary/zzz", "Tags": []}),
        );
        assert_eq!(s, 400);
        assert_eq!(r["__type"], "BadRequestException");
    }

    #[test]
    fn unknown_operation_is_not_implemented() {
        let acct = next_account();
        let (s, r) = call(acct, "FooBar", json!({}));
        assert_eq!(s, 501);
        assert_eq!(r["__type"], "NotImplemented");
    }
}
