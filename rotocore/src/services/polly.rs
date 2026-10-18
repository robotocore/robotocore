//! Amazon Polly backend — ported from moto's `polly` service (REST-JSON).
//!
//! Routes (moto's polly url table): GET /v1/voices,
//! GET/PUT/DELETE /v1/lexicons[/{name}], POST /v1/speech. Async synthesis
//! tasks are not modeled by moto; those routes return the sidecar's
//! route-miss error verbatim ("Service polly is not yet implemented", 501),
//! which mirrors the py-lane baseline failures.

use crate::store;
use crate::{Request, Response};
use once_cell::sync::Lazy;
use serde_json::{json, Value};
use std::collections::HashMap;

include!("polly_voice_data.rs");

static SLOT: Lazy<store::Slot<PollyState>> = Lazy::new(|| store::slot::<PollyState>("polly"));

#[derive(Default)]
struct PollyState {
    lexicons: HashMap<String, Lexicon>,
    region: String,
    account: String,
}

struct Lexicon {
    content: String,
    size: usize,
    last_modified: i64,
    alphabet: Option<String>,
    language_code: Option<String>,
}

impl Lexicon {
    /// moto's Lexicon.update(): parse XML; failure panics via ValueError in
    /// Python (500 there); root attribs ending in alphabet/lang are picked.
    fn parse(content: &str) -> (usize, i64, Option<String>, Option<String>) {
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_secs() as i64)
            .unwrap_or(0);
        let trimmed = content.trim_start();
        if !trimmed.starts_with('<') {
            panic!("Failure parsing XML: not well-formed");
        }
        // root element attributes
        let root_end = trimmed.find('>').unwrap_or(0);
        let root_tag = &trimmed[..root_end];
        let mut alphabet = None;
        let mut language_code = None;
        for part in root_tag.split_whitespace() {
            let Some((k, v)) = part.split_once('=') else {
                continue;
            };
            let val = v.trim().trim_matches('"').to_string();
            if k.ends_with("alphabet") && alphabet.is_none() {
                alphabet = Some(val);
            } else if k.ends_with("lang") && language_code.is_none() {
                language_code = Some(val);
            }
        }
        (content.len(), now, alphabet, language_code)
    }

    fn new(content: &str) -> Self {
        let (size, last_modified, alphabet, language_code) = Self::parse(content);
        Self {
            content: content.to_string(),
            size,
            last_modified,
            alphabet,
            language_code,
        }
    }

    fn attributes_json(&self, account: &str, region: &str, name: &str) -> Value {
        json!({
            "Alphabet": self.alphabet,
            "LanguageCode": self.language_code,
            "LastModified": self.last_modified,
            "LexemesCount": 1, // moto: len(root.findall(".")) -> [root]
            "LexiconArn": format!("arn:aws:polly:{region}:{account}:lexicon/{name}"),
            "Size": self.size,
        })
    }
}

pub fn dispatch(req: &Request) -> Response {
    let path = req.path.trim_end_matches('/');
    let segments: Vec<&str> = path.trim_start_matches('/').split('/').collect();
    let mut guard = SLOT.write();
    let st = guard.entry((req.account, req.region.clone())).or_default();
    if st.region.is_empty() {
        st.region = req.region.clone();
        st.account = req.account.to_string();
    }
    let region = st.region.clone();
    let account = st.account.clone();

    let rest_err = || {
        Response::json_status(
            501,
            &json!({
                "__type": "NotImplemented",
                "message": format!("Service {} is not yet implemented", req.service)
            }),
        )
    };

    match (req.method.as_str(), segments.as_slice()) {
        ("GET", ["v1", "voices"]) => describe_voices(&req.query_string),
        ("GET", ["v1", "lexicons"]) => st.list_lexicons(),
        ("GET", ["v1", "lexicons", name]) => st.get_lexicon(name, &account, &region),
        ("PUT", ["v1", "lexicons", name]) => st.put_lexicon(name, &req.body),
        ("DELETE", ["v1", "lexicons", name]) => st.delete_lexicon(name),
        ("POST", ["v1", "speech"]) => synthesize_speech(&req.body, st),
        _ => rest_err(),
    }
}

fn json_error(status: u16, code: &str, message: &str) -> Response {
    Response::json_status(status, &json!({ "__type": code, "message": message }))
}

fn describe_voices(query: &str) -> Response {
    let mut language_code: Option<String> = None;
    for kv in query.split('&') {
        if let Some((k, v)) = kv.split_once('=') {
            if k == "LanguageCode" || k == "languageCode" {
                language_code = Some(urldecode(v));
            }
        }
    }
    let voices: Vec<Value> = VOICE_DATA
        .iter()
        .filter(|v| {
            language_code
                .as_deref()
                .map(|l| v.language_code == l)
                .unwrap_or(true)
        })
        .map(|v| {
            let mut m = serde_json::Map::new();
            if !v.additional_language_codes.is_empty() {
                m.insert(
                    "AdditionalLanguageCodes".into(),
                    json!(v.additional_language_codes),
                );
            }
            m.insert("Gender".into(), json!(v.gender));
            m.insert("Id".into(), json!(v.id));
            m.insert("LanguageCode".into(), json!(v.language_code));
            m.insert("LanguageName".into(), json!(v.language_name));
            m.insert("Name".into(), json!(v.name));
            m.insert("SupportedEngines".into(), json!(v.supported_engines));
            Value::Object(m)
        })
        .collect();
    Response::json_status(200, &json!({"Voices": voices}))
}

fn urldecode(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    let bytes = s.as_bytes();
    let mut i = 0;
    while i < bytes.len() {
        match bytes[i] {
            b'%' if i + 2 < bytes.len() => {
                let hex = std::str::from_utf8(&bytes[i + 1..i + 3]).unwrap_or("");
                if let Ok(b) = u8::from_str_radix(hex, 16) {
                    out.push(b as char);
                    i += 3;
                } else {
                    out.push('%');
                    i += 1;
                }
            }
            b'+' => {
                out.push(' ');
                i += 1;
            }
            b => {
                out.push(b as char);
                i += 1;
            }
        }
    }
    out
}

impl PollyState {
    fn list_lexicons(&self) -> Response {
        let items: Vec<Value> = self
            .lexicons
            .iter()
            .map(|(name, lex)| {
                json!({
                    "Name": name,
                    "Attributes": lex.attributes_json(&self.account, &self.region, name),
                })
            })
            .collect();
        Response::json_status(200, &json!({"Lexicons": items}))
    }

    fn get_lexicon(&self, name: &str, account: &str, region: &str) -> Response {
        match self.lexicons.get(name) {
            Some(lex) => Response::json_status(
                200,
                &json!({
                    "Lexicon": {"Name": name, "Content": lex.content},
                    "LexiconAttributes": lex.attributes_json(account, region, name),
                }),
            ),
            None => json_error(400, "LexiconNotFoundException", "Lexicon not found"),
        }
    }

    fn put_lexicon(&mut self, name: &str, body: &[u8]) -> Response {
        let valid_name =
            !name.is_empty() && name.len() <= 20 && name.chars().all(|c| c.is_ascii_alphanumeric());
        if !valid_name {
            return json_error(
                400,
                "InvalidParameterValue",
                "Lexicon name must match [0-9A-Za-z]{1,20}",
            );
        }
        let params: Value = serde_json::from_slice(body).unwrap_or(json!({}));
        let Some(content) = params.get("Content").and_then(Value::as_str) else {
            return json_error(400, "MissingParameter", "Content is missing from the body");
        };
        let lex = Lexicon::new(content);
        self.lexicons.insert(name.to_string(), lex);
        Response::new(200, "application/json", "")
    }

    fn delete_lexicon(&mut self, name: &str) -> Response {
        if self.lexicons.remove(name).is_none() {
            return json_error(400, "LexiconNotFoundException", "Lexicon not found");
        }
        Response::new(200, "application/json", "")
    }
}

/// POST /v1/speech — SynthesizeSpeech with moto's validation order.
fn synthesize_speech(body: &[u8], st: &PollyState) -> Response {
    let params: Value = serde_json::from_slice(body).unwrap_or(json!({}));

    let mut lexicon_names: Option<&Vec<Value>> = None;
    if let Some(names) = params.get("LexiconNames").and_then(Value::as_array) {
        for n in names {
            let name = n.as_str().unwrap_or_default();
            if !st.lexicons.contains_key(name) {
                return json_error(400, "LexiconNotFoundException", "Lexicon not found");
            }
        }
        lexicon_names = Some(names);
    }
    let _ = lexicon_names;

    let Some(output_format) = params.get("OutputFormat").and_then(Value::as_str) else {
        return json_error(400, "MissingParameter", "Missing parameter OutputFormat");
    };
    if !["json", "mp3", "ogg_vorbis", "pcm"].contains(&output_format) {
        return json_error(
            400,
            "InvalidParameterValue",
            "Not one of json, mp3, ogg_vorbis, pcm",
        );
    }

    let mut sample_rate: i64 = 22050;
    if let Some(sr) = params.get("SampleRate") {
        let sr = sr
            .as_i64()
            .or_else(|| sr.as_str().and_then(|s| s.parse().ok()));
        match sr {
            Some(v) if [8000, 16000, 22050].contains(&v) => sample_rate = v,
            _ => {
                return json_error(
                    400,
                    "InvalidSampleRateException",
                    "The specified sample rate is not valid.",
                )
            }
        }
    }

    let mut speech_marks: Option<&Vec<Value>> = None;
    if let Some(marks) = params.get("SpeechMarkTypes").and_then(Value::as_array) {
        for value in marks {
            if !["sentance", "ssml", "viseme", "word"].contains(&value.as_str().unwrap_or_default())
            {
                return json_error(
                    400,
                    "InvalidParameterValue",
                    "Not one of sentance, ssml, viseme, word",
                );
            }
        }
        speech_marks = Some(marks);
    }

    let Some(text) = params.get("Text").and_then(Value::as_str) else {
        return json_error(400, "MissingParameter", "Missing parameter Text");
    };

    let mut text_type = "text";
    if let Some(tt) = params.get("TextType").and_then(Value::as_str) {
        if !["ssml", "text"].contains(&tt) {
            return json_error(400, "InvalidParameterValue", "Not one of ssml, text");
        }
        text_type = tt;
    }

    let Some(voice_id) = params.get("VoiceId").and_then(Value::as_str) else {
        return json_error(400, "MissingParameter", "Missing parameter VoiceId");
    };
    if !VOICE_IDS.contains(&voice_id) {
        return json_error(
            400,
            "InvalidParameterValue",
            "Not one of the supported voices",
        );
    }

    if text.len() > 3000 {
        return json_error(400, "TextLengthExceededException", "Text too long");
    }
    if speech_marks.is_some() && output_format != "json" {
        return json_error(
            400,
            "MarksNotSupportedForFormatException",
            "OutputFormat must be json",
        );
    }
    if speech_marks.is_some() && text_type == "text" {
        return json_error(
            400,
            "SsmlMarksNotSupportedForTextTypeException",
            "TextType must be ssml",
        );
    }

    let content_type = match output_format {
        "mp3" => "audio/mpeg",
        "ogg_vorbis" => "audio/ogg",
        "pcm" => "audio/pcm",
        _ => "audio/json",
    };
    let _ = sample_rate;
    Response::new(200, content_type, "\0\0\0\0\0\0\0\0")
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;
    use std::collections::HashMap as Map2;

    fn req(method: &str, path: &str, body: &str) -> Request {
        let (path_part, query) = match path.split_once('?') {
            Some((p, q)) => (p.to_string(), q.to_string()),
            None => (path.to_string(), String::new()),
        };
        Request {
            service: "polly".to_string(),
            operation: String::new(),
            method: method.to_string(),
            path: path_part,
            query_string: query,
            headers: Map2::new(),
            body: Bytes::from(body.to_string()),
            account: 123456789012,
            region: "us-east-1".to_string(),
        }
    }

    #[test]
    fn voices_list_and_filter() {
        let r = dispatch(&req("GET", "/v1/voices", ""));
        let body: Value = serde_json::from_slice(&r.body).unwrap();
        assert!(!body["Voices"].as_array().unwrap().is_empty());
        assert!(body["Voices"][0]["Id"].is_string());

        let r = dispatch(&req("GET", "/v1/voices?LanguageCode=en-US", ""));
        let body: Value = serde_json::from_slice(&r.body).unwrap();
        let voices = body["Voices"].as_array().unwrap();
        assert!(!voices.is_empty());
        for v in voices {
            assert_eq!(v["LanguageCode"], "en-US");
        }
    }

    #[test]
    fn lexicon_lifecycle() {
        let content = r#"<?xml version="1.0" encoding="UTF-8"?><lexicon version="1.0" xmlns="x" alphabet="ipa" xml:lang="en-US"><lexeme><grapheme>W3C</grapheme><alias>WWW</alias></lexeme></lexicon>"#;
        let body = format!(
            r#"{{"Content": {}}}"#,
            serde_json::to_string(content).unwrap()
        );
        let r = dispatch(&req("PUT", "/v1/lexicons/compat01", &body));
        assert_eq!(r.status, 200, "{}", String::from_utf8_lossy(&r.body));

        let r = dispatch(&req("GET", "/v1/lexicons/compat01", ""));
        assert_eq!(r.status, 200);
        let body: Value = serde_json::from_slice(&r.body).unwrap();
        assert_eq!(body["Lexicon"]["Name"], "compat01");
        assert!(body["Lexicon"]["Content"].as_str().is_some());
        assert!(Value::Object(
            body["LexiconAttributes"]
                .as_object()
                .expect("attrs")
                .clone()
        )["LexiconArn"]
            .as_str()
            .is_some());

        let r = dispatch(&req("GET", "/v1/lexicons", ""));
        let body: Value = serde_json::from_slice(&r.body).unwrap();
        assert!(body["Lexicons"]
            .as_array()
            .unwrap()
            .iter()
            .any(|l| l["Name"] == "compat01"));

        let r = dispatch(&req("DELETE", "/v1/lexicons/compat01", ""));
        assert_eq!(r.status, 200);
        let r = dispatch(&req("DELETE", "/v1/lexicons/compat01", ""));
        assert_eq!(r.status, 400);
        let body: Value = serde_json::from_slice(&r.body).unwrap();
        assert_eq!(body["__type"], "LexiconNotFoundException");
    }

    #[test]
    fn synthesize_speech_mp3() {
        let r = dispatch(&req(
            "POST",
            "/v1/speech",
            r#"{"OutputFormat": "mp3", "Text": "Hello", "VoiceId": "Joanna"}"#,
        ));
        assert_eq!(r.status, 200);
        assert_eq!(r.headers[0].1, "audio/mpeg");
        assert!(!r.body.is_empty());
    }

    #[test]
    fn unknown_route_is_not_implemented() {
        let r = dispatch(&req("POST", "/v1/synthesisTasks", "{}"));
        assert_eq!(r.status, 501);
        let body: Value = serde_json::from_slice(&r.body).unwrap();
        assert_eq!(body["__type"], "NotImplemented");
        assert_eq!(body["message"], "Service polly is not yet implemented");
    }
}
