//! ACM backend — AWS Certificate Manager (JSON protocol).
//!
//! Replaces the moto sidecar for `acm` (`X-Amz-Target: CertificateManager.<Op>`
//! on `application/x-amz-json-1.1`). State is per-(account, region), mirroring
//! moto's `AWSCertificateManagerBackend` (moto/acm). Errors are
//! `{"__type": code, "message": ...}` over HTTP 400.
//!
//! Semantics carried over from moto:
//! - `RequestCertificate` mints an Amazon-issued bundle with
//!   `PENDING_VALIDATION`; it flips to `ISSUED` only after the
//!   `MOTO_ACM_VALIDATION_WAIT` window (default 60s) elapses.
//! - `IdempotencyToken` maps to an ARN for one hour and replays when the
//!   request tags match.
//! - `ImportCertificate` keeps the caller's PEM and answers GetCertificate
//!   with `chain + AWS_ROOT_CA`.
//! - Certificates without a CN import fine (moto 5.2.2 #9912); with the PEM
//!   unparsed, `DomainName`/`Subject` render moto's no-CN-no-SAN fallback
//!   shape (`""` / `"CN="`).
//! - `RequestCertificate` merges partial `Options` over the default
//!   CTLogging/Export pair instead of replacing them (moto 5.2.2 #10030).
//! - Tag stage/validation mirrors moto's `TagHolder`.
//!
//! Known divergences (no suite coverage): imported-cert metadata (CN, SANs,
//! serial, validity) is derived from the request rather than parsed out of the
//! PEM — so moto's first-SAN `DomainName` fallback for CN-less certs reduces
//! to `""` here — and Export of a PRIVATE cert returns the stored key without
//! PKCS#8 passphrase encryption. Both moto 5.2.2 tolerance behaviors are
//! pinned by `acm_import_without_cn` / `acm_request_cert_defaults`.

use crate::proto::{error_response, json_error, parse_json_body};
use crate::store;
use crate::{Request, Response};
use serde_json::{json, Value};
use std::collections::BTreeMap;
use std::collections::HashMap;

const VALIDATION_WAIT_SECS: i64 = 60;
const MAX_KEY_LEN: usize = 128;
const MAX_VALUE_LEN: usize = 256;
const MAX_TAG_COUNT: usize = 50;
const MIN_PASSPHRASE_LEN: usize = 4;

const MISSING_PARAMETER: &str = "A required parameter for the specified action is not supplied.";

/// moto's AWS_ROOT_CA — ACM hands back the caller's chain plus a root CA.
const AWS_ROOT_CA: &str = "-----BEGIN CERTIFICATE-----\nMIIESTCCAzGgAwIBAgITBntQXCplJ7wevi2i0ZmY7bibLDANBgkqhkiG9w0BAQsF\nADA5MQswCQYDVQQGEwJVUzEPMA0GA1UEChMGQW1hem9uMRkwFwYDVQQDExBBbWF6\nb24gUm9vdCBDQSAxMB4XDTE1MTAyMTIyMjQzNFoXDTQwMTAyMTIyMjQzNFowRjEL\nMAkGA1UEBhMCVVMxDzANBgNVBAoTBkFtYXpvbjEVMBMGA1UECxMMU2VydmVyIENB\nIDFCMQ8wDQYDVQQDEwZBbWF6b24wggEiMA0GCSqGSIb3DQEBAQUAA4IBDwAwggEK\nAoIBAQDCThZn3c68asg3Wuw6MLAd5tES6BIoSMzoKcG5blPVo+sDORrMd4f2AbnZ\ncMzPa43j4wNxhplty6aUKk4T1qe9BOwKFjwK6zmxxLVYo7bHViXsPlJ6qOMpFge5\nblDP+18x+B26A0piiQOuPkfyDyeR4xQghfj66Yo19V+emU3nazfvpFA+ROz6WoVm\nB5x+F2pV8xeKNR7u6azDdU5YVX1TawprmxRC1+WsAYmz6qP+z8ArDITC2FMVy2fw\n0IjKOtEXc/VfmtTFch5+AfGYMGMqqvJ6LcXiAhqG5TI+Dr0RtM88k+8XUBCeQ8IG\nKuANaL7TiItKZYxK1MMuTJtV9IblAgMBAAGjggE7MIIBNzASBgNVHRMBAf8ECDAG\nAQH/AgEAMA4GA1UdDwEB/wQEAwIBhjAdBgNVHQ4EFgQUWaRmBlKge5WSPKOUByeW\ndFv5PdAwHwYDVR0jBBgwFoAUhBjMhTTsvAyUlC4IWZzHshBOCggwewYIKwYBBQUH\nAQEEbzBtMC8GCCsGAQUFBzABhiNodHRwOi8vb2NzcC5yb290Y2ExLmFtYXpvbnRy\ndXN0LmNvbTA6BggrBgEFBQcwAoYuaHR0cDovL2NybC5yb290Y2ExLmFtYXpvbnRy\ndXN0LmNvbS9yb290Y2ExLmNlcjA/BgNVHR8EODA2MDSgMqAwhi5odHRwOi8vY3Js\nLnJvb3RjYTEuYW1hem9udHJ1c3QuY29tL3Jvb3RjYTEuY3JsMBMGA1UdIAQMMAow\nCAYGZ4EMAQIBMA0GCSqGSIb3DQEBCwUAA4IBAQAfsaEKwn17DjAbi/Die0etn+PE\ngfY/I6s8NLWkxGAOUfW2o+vVowNARRVjaIGdrhAfeWHkZI6q2pI0x/IJYmymmcWa\nZaW/2R7DvQDtxCkFkVaxUeHvENm6IyqVhf6Q5oN12kDSrJozzx7I7tHjhBK7V5Xo\nTyS4NU4EhSyzGgj2x6axDd1hHRjblEpJ80LoiXlmUDzputBXyO5mkcrplcVvlIJi\nWmKjrDn2zzKxDX5nwvkskpIjYlJcrQu4iCX1/YwZ1yNqF9LryjlilphHCACiHbhI\nRnGfN8j8KLDVmWyTYMk8V+6j0LI4+4zFh2upqGMQHL3VFVFWBek6vCDWhB/b\n -----END CERTIFICATE-----";

// ---------------------------------------------------------------------------
// State
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum CertKind {
    AmazonIssued,
    Imported,
    Private,
}

impl CertKind {
    fn as_str(self) -> &'static str {
        match self {
            CertKind::AmazonIssued => "AMAZON_ISSUED",
            CertKind::Imported => "IMPORTED",
            CertKind::Private => "PRIVATE",
        }
    }
}

#[derive(Debug, Clone)]
struct CertBundle {
    arn: String,
    /// Caller-supplied PEM bodies — `None` for synthesized Amazon-issued certs.
    cert_pem: Option<String>,
    key_pem: Option<String>,
    provided_chain_pem: Option<String>,
    tags: BTreeMap<String, Option<String>>,
    kind: CertKind,
    status: String,
    created_at: i64,
    common_name: String,
    sans: Vec<String>,
    in_use_by: Vec<String>,
    revocation_reason: Option<String>,
    revoked_at: Option<i64>,
    logging_preference: String,
    export_enabled: bool,
    issuer: String,
    key_algorithm: String,
    signature_algorithm: String,
    serial: String,
    not_before: i64,
    not_after: i64,
}

impl CertBundle {
    fn options_json(&self) -> Value {
        json!({
            "CertificateTransparencyLoggingPreference": self.logging_preference,
            "Export": if self.export_enabled { "ENABLED" } else { "DISABLED" },
        })
    }

    /// moto's `CertBundle.check()`: flip pending → issued after the validation
    /// wait, or mark expired. Runs on every read path (get/describe/list).
    fn check(&mut self, now: i64) {
        if self.not_after <= now {
            self.status = "EXPIRED".to_string();
            return;
        }
        if self.kind == CertKind::AmazonIssued
            && self.status == "PENDING_VALIDATION"
            && now - self.created_at > VALIDATION_WAIT_SECS
        {
            self.status = "ISSUED".to_string();
        }
    }
}

#[derive(Debug, Clone, Copy)]
struct AccountConfig {
    days_before_expiry: i64,
}

impl Default for AccountConfig {
    fn default() -> Self {
        // moto's AccountConfiguration default of 45 days.
        AccountConfig {
            days_before_expiry: 45,
        }
    }
}

impl AccountConfig {
    fn to_json(self) -> Value {
        json!({ "ExpiryEvents": { "DaysBeforeExpiry": self.days_before_expiry } })
    }
}

#[derive(Default)]
struct AcmState {
    certs: HashMap<String, CertBundle>,
    /// IdempotencyToken → (arn, expiry epoch)
    idempotency: HashMap<String, (String, i64)>,
    account_config: AccountConfig,
}

fn with_state<T>(account: u64, region: &str, f: impl FnOnce(&mut AcmState) -> T) -> T {
    let slot = store::slot::<AcmState>("acm");
    let mut map = slot.write();
    f(map.entry((account, region.to_string())).or_default())
}

fn now_epoch() -> i64 {
    chrono::Utc::now().timestamp()
}

fn make_arn(account: u64, region: &str) -> String {
    format!(
        "arn:aws:acm:{region}:{account}:certificate/{}",
        uuid::Uuid::new_v4()
    )
}

// ---------------------------------------------------------------------------
// Errors
// ---------------------------------------------------------------------------

fn not_found(arn: &str, account: u64) -> Response {
    json_error(
        400,
        "ResourceNotFoundException",
        &format!("Certificate with arn {arn} not found in account {account}"),
    )
}

fn validation(message: &str) -> Response {
    json_error(400, "ValidationException", message)
}

fn other_error_code(code: &str, message: &str) -> Response {
    json_error(400, code, message)
}

fn missing_parameter() -> Response {
    json_error(400, "InvalidParameterException", MISSING_PARAMETER)
}

/// The suite's ops all key off `CertificateArn`; empty → InvalidParameterException
/// (botocore's acm model: InvalidParameterException + ValidationException; a
/// required parameter not supplied is never `MissingParameter`).
fn arn_param(body: &Value) -> Result<String, Response> {
    match body.get("CertificateArn").and_then(Value::as_str) {
        Some(arn) if !arn.is_empty() => Ok(arn.to_string()),
        _ => Err(json_error(
            400,
            "InvalidParameterException",
            MISSING_PARAMETER,
        )),
    }
}

/// Fetch a cert applying moto's `check()`; NotFound when absent.
fn get_cert_mut<'a>(
    state: &'a mut AcmState,
    arn: &str,
    account: u64,
) -> Result<&'a mut CertBundle, Response> {
    match state.certs.get_mut(arn) {
        Some(cert) => {
            cert.check(now_epoch());
            Ok(cert)
        }
        None => Err(not_found(arn, account)),
    }
}

// ---------------------------------------------------------------------------
// Tags (moto TagHolder semantics)
// ---------------------------------------------------------------------------

type TagList = Vec<(String, Option<String>)>;

fn parse_tags(v: Option<&Value>) -> TagList {
    let mut out: TagList = Vec::new();
    if let Some(Value::Array(list)) = v {
        for tag in list {
            let key = tag
                .get("Key")
                .and_then(Value::as_str)
                .unwrap_or_default()
                .to_string();
            let value = tag.get("Value").and_then(Value::as_str).map(String::from);
            out.push((key, value));
        }
    }
    out
}

fn tag_pair_len_err(index: usize, key: &str, value: Option<&str>) -> Option<String> {
    if key.len() > MAX_KEY_LEN {
        return Some(format!(
            "Value '{key}' at 'tags.{index}.member.key' failed to satisfy constraint: Member must have length less than or equal to {MAX_KEY_LEN}"
        ));
    }
    if let Some(value) = value {
        if value.len() > MAX_VALUE_LEN {
            return Some(format!(
                "Value '{value}' at 'tags.{index}.member.value' failed to satisfy constraint: Member must have length less than or equal to {MAX_VALUE_LEN}"
            ));
        }
    }
    if key.starts_with("aws:") {
        return Some(format!(
            "Invalid Tag Key: \"{key}\". AWS internal tags cannot be changed with this API"
        ));
    }
    None
}

fn add_tags(tags: &mut BTreeMap<String, Option<String>>, req: &TagList) -> Result<(), Response> {
    for (i, (key, value)) in req.iter().enumerate() {
        if let Some(msg) = tag_pair_len_err(i + 1, key, value.as_deref()) {
            // moto's message keeps the `i.member` path shape.
            return Err(json_error(400, "ValidationException", &msg));
        }
    }
    let mut staged = tags.clone();
    for (key, value) in req {
        staged.insert(key.clone(), value.clone());
    }
    if staged.len() > MAX_TAG_COUNT {
        let staged_repr = staged
            .iter()
            .map(|(k, v)| format!("{k}={}", v.clone().unwrap_or_default()))
            .collect::<Vec<_>>()
            .join(", ");
        return Err(json_error(
            400,
            "TooManyTagsException",
            &format!("the TagSet: '{{{{{staged_repr}}}}}' contains too many Tags"),
        ));
    }
    *tags = staged;
    Ok(())
}

fn remove_tags(tags: &mut BTreeMap<String, Option<String>>, req: &TagList) {
    for (key, value) in req {
        let stored_now = tags.get(key).and_then(|old| old.as_deref());
        let remove = match value {
            None => true,
            Some(v) => stored_now == Some(v.as_str()),
        };
        if remove {
            tags.remove(key);
        }
    }
}

// (tags_from_request was folded into parse_tags consumption paths)

fn tags_equal_to_request(tag_map: &BTreeMap<String, Option<String>>, req: Option<&Value>) -> bool {
    let request: BTreeMap<String, Option<String>> = parse_tags(req).into_iter().collect();
    if tag_map.len() != request.len() {
        return false;
    }
    tag_map
        .iter()
        .all(|(k, v)| request.get(k).map(|rv| rv.as_deref()) == Some(v.as_deref()))
}

// ---------------------------------------------------------------------------
// Bundle construction
// ---------------------------------------------------------------------------

/// A synthetic Amazon-issued bundle. moto generates a real RSA cert and keeps
/// it server-side; rotocore mirrors the metadata moto exposes.
fn synthetic_bundle(
    arn: String,
    domain: &str,
    sans: Option<Vec<String>>,
    kind: CertKind,
    status: &str,
    options: Option<&Value>,
) -> CertBundle {
    let now = now_epoch();
    let mut all_sans = sans.unwrap_or_default();
    all_sans.retain(|s| *s != domain);
    all_sans.insert(0, domain.to_string());
    let mut serial: u128 = 0;
    for byte in uuid::Uuid::new_v4().as_bytes() {
        serial = (serial << 8) | *byte as u128;
    }
    // Options shape: {CertificateTransparencyLoggingPreference, Export}
    let logging = options
        .and_then(|o| o.get("CertificateTransparencyLoggingPreference"))
        .and_then(Value::as_str)
        .unwrap_or("ENABLED")
        .to_string();
    let export_req = options
        .and_then(|o| o.get("Export"))
        .and_then(Value::as_str)
        .map(|v| v == "ENABLED")
        .unwrap_or(false);
    CertBundle {
        arn,
        cert_pem: None,
        key_pem: None,
        provided_chain_pem: None,
        tags: BTreeMap::new(),
        kind,
        status: status.to_string(),
        created_at: now,
        common_name: domain.to_string(),
        sans: all_sans,
        in_use_by: Vec::new(),
        revocation_reason: None,
        revoked_at: None,
        logging_preference: logging,
        export_enabled: kind == CertKind::Private || export_req,
        issuer: "Amazon".to_string(),
        key_algorithm: "RSA_2048".to_string(),
        signature_algorithm: "SHA512WITHRSA".to_string(),
        serial: serial.to_string(),
        not_before: now,
        not_after: now + 365 * 24 * 3600,
    }
}

// ---------------------------------------------------------------------------
// describe / list views (moto CertBundle.describe())
// ---------------------------------------------------------------------------

fn describe_json(cert: &CertBundle) -> Value {
    let mut cert_obj = json!({
        "CertificateArn": cert.arn,
        "DomainName": cert.common_name,
        "InUseBy": cert.in_use_by,
        "Issuer": cert.issuer.clone(),
        "KeyAlgorithm": cert.key_algorithm,
        "NotAfter": cert.not_after,
        "NotBefore": cert.not_before,
        "Serial": cert.serial,
        "SignatureAlgorithm": cert.signature_algorithm,
        "Status": cert.status,
        "Subject": format!("CN={}", cert.common_name),
        "SubjectAlternativeNames": cert.sans,
        "Type": cert.kind.as_str(),
        "ExtendedKeyUsages": [],
        "RenewalEligibility": "INELIGIBLE",
        "Options": cert.options_json(),
    });
    if let Some(obj) = cert_obj.as_object_mut() {
        if cert.kind == CertKind::Imported {
            obj.insert("ImportedAt".to_string(), json!(cert.created_at));
        } else {
            obj.insert("CreatedAt".to_string(), json!(cert.created_at));
            obj.insert("IssuedAt".to_string(), json!(cert.created_at));
        }
        if cert.status == "REVOKED" && cert.revoked_at.is_some() {
            obj.insert(
                "RevokedAt".to_string(),
                json!(cert.revoked_at.unwrap_or_default()),
            );
            obj.insert(
                "RevocationReason".to_string(),
                json!(cert
                    .revocation_reason
                    .clone()
                    .unwrap_or_else(|| "UNSPECIFIED".to_string())),
            );
        }
    }
    json!({ "Certificate": cert_obj })
}

fn summary_json(cert: &CertBundle) -> Value {
    let mut summary = describe_json(cert);
    if let Some(inner) = summary
        .as_object_mut()
        .and_then(|s| s.get_mut("Certificate"))
        .and_then(Value::as_object_mut)
    {
        inner.remove("InUseBy");
        inner.insert("InUse".to_string(), json!(!cert.in_use_by.is_empty()));
        inner.insert("Exported".to_string(), json!(cert.export_enabled));
    }
    // moto flattens the describe() "Certificate" content into the summary item.
    summary["Certificate"].take()
}

// ---------------------------------------------------------------------------
// PEM passthrough helpers
// ---------------------------------------------------------------------------

fn stored_cert_pem(cert: &CertBundle) -> String {
    match &cert.cert_pem {
        Some(pem) => pem.clone(),
        None => format!(
            "-----BEGIN CERTIFICATE-----\nrotocore-acm-synthetic:{}\n-----END CERTIFICATE-----",
            cert.arn
        ),
    }
}

fn stored_chain_pem(cert: &CertBundle) -> String {
    match &cert.provided_chain_pem {
        Some(chain) => format!("{chain}\n{AWS_ROOT_CA}"),
        None => AWS_ROOT_CA.to_string(),
    }
}

// ---------------------------------------------------------------------------
// Operations
// ---------------------------------------------------------------------------

type OpResult = Result<Value, Response>;

fn op_request_certificate(body: &Value, account: u64, region: &str) -> OpResult {
    let domain = body
        .get("DomainName")
        .and_then(Value::as_str)
        .unwrap_or_default();
    let token = body
        .get("IdempotencyToken")
        .and_then(Value::as_str)
        .map(String::from);
    let sans = body
        .get("SubjectAlternativeNames")
        .and_then(Value::as_array)
        .map(|a| {
            a.iter()
                .filter_map(Value::as_str)
                .map(String::from)
                .collect::<Vec<_>>()
        });
    if sans.as_ref().is_some_and(|s| s.len() > 10) {
        return Err(other_error_code(
            "LimitExceededException",
            "An ACM limit has been exceeded. Need to request SAN limit to be raised",
        ));
    }
    let kind = if body.get("CertificateAuthorityArn").is_some() {
        CertKind::Private
    } else {
        CertKind::AmazonIssued
    };
    let status = if kind == CertKind::Private {
        "ISSUED"
    } else {
        "PENDING_VALIDATION"
    };

    with_state(account, region, |state| {
        if let Some(token) = &token {
            if let Some(arn) = idempotent_arn(state, token) {
                if let Some(cert) = state.certs.get(&arn) {
                    if tags_equal_to_request(&cert.tags, body.get("Tags")) {
                        return Ok(json!({ "CertificateArn": arn }));
                    }
                }
            }
        }
        let arn = make_arn(account, region);
        // Partial Options preserve the default members: synthetic_bundle reads
        // each option per-key with its default fallback (CTLogging ENABLED,
        // Export DISABLED) rather than adopting the request object wholesale —
        // moto 5.2.2 #10030 fixed exactly that replace-over-write regression.
        let mut bundle =
            synthetic_bundle(arn.clone(), domain, sans, kind, status, body.get("Options"));
        add_tags(&mut bundle.tags, &parse_tags(body.get("Tags")))?;
        if let Some(token) = &token {
            state
                .idempotency
                .insert(token.clone(), (arn.clone(), now_epoch() + 3600));
        }
        state.certs.insert(arn.clone(), bundle);
        Ok(json!({ "CertificateArn": arn }))
    })
}

/// Replay an unexpired idempotency token; prune it once expired.
fn idempotent_arn(state: &mut AcmState, token: &str) -> Option<String> {
    match state.idempotency.get(token) {
        Some((arn, expires)) if *expires > now_epoch() => Some(arn.clone()),
        Some(_) => {
            state.idempotency.remove(token);
            None
        }
        None => None,
    }
}

fn op_describe_certificate(body: &Value, account: u64, region: &str) -> OpResult {
    let arn = arn_param(body)?;
    with_state(account, region, |state| {
        Ok(describe_json(get_cert_mut(state, &arn, account)?))
    })
}

fn op_list_certificates(body: &Value, account: u64, region: &str) -> OpResult {
    let statuses: Vec<String> = body
        .get("CertificateStatuses")
        .and_then(Value::as_array)
        .map(|a| {
            a.iter()
                .filter_map(Value::as_str)
                .map(String::from)
                .collect()
        })
        .unwrap_or_default();
    let items = with_state(account, region, |state| {
        let now = now_epoch();
        state
            .certs
            .values_mut()
            .filter_map(|cert| {
                cert.check(now);
                if !statuses.is_empty() && !statuses.contains(&cert.status) {
                    return None;
                }
                Some(summary_json(cert))
            })
            .collect::<Vec<_>>()
    });
    Ok(json!({ "CertificateSummaryList": items }))
}

fn op_add_tags(body: &Value, account: u64, region: &str) -> OpResult {
    let arn = arn_param(body)?;
    let tags = parse_tags(body.get("Tags"));
    with_state(account, region, |state| {
        let cert = get_cert_mut(state, &arn, account)?;
        add_tags(&mut cert.tags, &tags)?;
        Ok(json!({}))
    })
}

fn op_remove_tags(body: &Value, account: u64, region: &str) -> OpResult {
    let arn = arn_param(body)?;
    let tags = parse_tags(body.get("Tags"));
    with_state(account, region, |state| {
        let cert = get_cert_mut(state, &arn, account)?;
        remove_tags(&mut cert.tags, &tags);
        Ok(json!({}))
    })
}

fn op_list_tags(body: &Value, account: u64, region: &str) -> OpResult {
    let arn = arn_param(body)?;
    with_state(account, region, |state| {
        let cert = get_cert_mut(state, &arn, account)?;
        let tags: Vec<Value> = cert
            .tags
            .iter()
            .map(|(k, v)| match v {
                Some(value) => json!({ "Key": k, "Value": value }),
                None => json!({ "Key": k }),
            })
            .collect();
        Ok(json!({ "Tags": tags }))
    })
}

fn op_delete_certificate(body: &Value, account: u64, region: &str) -> OpResult {
    let arn = arn_param(body)?;
    with_state(account, region, |state| {
        if state.certs.remove(&arn).is_none() {
            return Err(not_found(&arn, account));
        }
        Ok(json!({}))
    })
}

fn op_get_certificate(body: &Value, account: u64, region: &str) -> OpResult {
    let arn = arn_param(body)?;
    with_state(account, region, |state| {
        let cert = get_cert_mut(state, &arn, account)?;
        Ok(json!({
            "Certificate": stored_cert_pem(cert),
            "CertificateChain": stored_chain_pem(cert),
        }))
    })
}

fn op_import_certificate(body: &Value, account: u64, region: &str) -> OpResult {
    let cert_b64 = body
        .get("Certificate")
        .and_then(Value::as_str)
        .ok_or_else(|| validation("The certificate is not PEM-encoded or is not valid."))?;
    let key_b64 = body
        .get("PrivateKey")
        .and_then(Value::as_str)
        .ok_or_else(|| validation("The private key is not PEM-encoded or is not valid."))?;
    // botocore transmits blobs base64-encoded; moto decodes before storing.
    let Some(cert_pem) = decode_utf8_input(cert_b64) else {
        return Err(validation(
            "The certificate is not PEM-encoded or is not valid.",
        ));
    };
    let Some(key_pem) = decode_utf8_input(key_b64) else {
        return Err(validation(
            "The private key is not PEM-encoded or is not valid.",
        ));
    };
    let provided_chain = match body.get("CertificateChain").and_then(Value::as_str) {
        Some(chain_b64) => match decode_utf8_input(chain_b64) {
            Some(chain) => Some(chain),
            None => {
                return Err(validation(
                    "The certificate chain is not PEM-encoded or is not valid.",
                ))
            }
        },
        None => None,
    };
    let now = now_epoch();
    with_state(account, region, |state| {
        let arn = match body.get("CertificateArn").and_then(Value::as_str) {
            Some(arn) if !arn.is_empty() => {
                if !state.certs.contains_key(arn) {
                    return Err(not_found(arn, account));
                }
                arn.to_string()
            }
            _ => make_arn(account, region),
        };
        let mut bundle = CertBundle {
            arn: arn.clone(),
            cert_pem: Some(cert_pem.clone()),
            key_pem: Some(key_pem.clone()),
            provided_chain_pem: provided_chain.clone(),
            tags: BTreeMap::new(),
            kind: CertKind::Imported,
            status: "ISSUED".to_string(),
            created_at: now,
            common_name: String::new(),
            sans: Vec::new(),
            in_use_by: Vec::new(),
            revocation_reason: None,
            revoked_at: None,
            logging_preference: "ENABLED".to_string(),
            export_enabled: false,
            issuer: String::new(),
            key_algorithm: "RSA_2048".to_string(),
            signature_algorithm: String::new(),
            serial: String::new(),
            not_before: now,
            not_after: now + 2 * 365 * 24 * 3600,
        };
        add_tags(&mut bundle.tags, &parse_tags(body.get("Tags")))?;
        state.certs.insert(arn.clone(), bundle);
        Ok(json!({ "CertificateArn": arn }))
    })
}

/// botocore sends blobs base64-encoded; moto decodes them before storing.
fn decode_utf8_input(b64: &str) -> Option<String> {
    b64_decode(b64).and_then(|bytes| String::from_utf8(bytes).ok())
}

fn op_get_account_configuration(account: u64, region: &str) -> OpResult {
    Ok(with_state(account, region, |state| {
        state.account_config.to_json()
    }))
}

fn op_put_account_configuration(body: &Value, account: u64, region: &str) -> OpResult {
    if body.get("ExpiryEvents").is_none() {
        return Err(missing_parameter());
    }
    let Some(days) = body
        .pointer("/ExpiryEvents/DaysBeforeExpiry")
        .and_then(Value::as_i64)
    else {
        return Err(validation("DaysBeforeExpiry is required in ExpiryEvents."));
    };
    if !(1..=90).contains(&days) {
        return Err(validation("DaysBeforeExpiry must be between 1 and 90"));
    }
    with_state(account, region, |state| {
        state.account_config = AccountConfig {
            days_before_expiry: days,
        };
        Ok(json!({}))
    })
}

fn op_update_certificate_options(body: &Value, account: u64, region: &str) -> OpResult {
    let arn = arn_param(body)?;
    if body.get("Options").is_none() {
        return Err(missing_parameter());
    }
    let pref = body
        .pointer("/Options/CertificateTransparencyLoggingPreference")
        .and_then(Value::as_str);
    with_state(account, region, |state| {
        let cert = get_cert_mut(state, &arn, account)?;
        if let Some(pref) = pref {
            if pref != "ENABLED" && pref != "DISABLED" {
                return Err(validation(&format!(
                    "1 validation error detected: Value '{pref}' at 'options.certificateTransparencyLoggingPreference' failed to satisfy constraint: Member must satisfy enum value set: [ENABLED, DISABLED]"
                )));
            }
            cert.logging_preference = pref.to_string();
        }
        Ok(json!({}))
    })
}

fn op_resend_validation_email(body: &Value, account: u64, region: &str) -> OpResult {
    let Some(domain) = body.get("Domain").and_then(Value::as_str) else {
        return Err(missing_parameter());
    };
    let arn_span = body
        .get("CertificateArn")
        .and_then(Value::as_str)
        .unwrap_or_default()
        .to_string();
    if arn_span.is_empty() {
        return Err(missing_parameter());
    }
    with_state(account, region, |state| {
        let cert = get_cert_mut(state, &arn_span, account)?;
        if cert.common_name != domain {
            return Err(other_error_code(
                "InvalidDomainValidationOptionsException",
                "Parameter Domain does not match certificate domain",
            ));
        }
        Ok(json!({}))
    })
}

fn op_renew_certificate(body: &Value, account: u64, region: &str) -> OpResult {
    let arn = arn_param(body)?;
    with_state(account, region, |state| {
        let cert = get_cert_mut(state, &arn, account)?;
        let family_ok = matches!(cert.kind, CertKind::AmazonIssued | CertKind::Private);
        if !family_ok || cert.status != "ISSUED" {
            return Err(other_error_code(
                "InvalidStateException",
                "Certificate is not eligible for renewal.",
            ));
        }
        Ok(json!({}))
    })
}

fn op_revoke_certificate(body: &Value, account: u64, region: &str) -> OpResult {
    let arn = arn_param(body)?;
    let reason = body
        .get("RevocationReason")
        .and_then(Value::as_str)
        .map(String::from);
    with_state(account, region, |state| {
        {
            let cert = get_cert_mut(state, &arn, account)?;
            if cert.kind != CertKind::Private {
                return Err(other_error_code(
                    "InvalidArnException",
                    "The certificate ARN is not valid. Revocation is only supported for certificates issued by a private CA.",
                ));
            }
            if cert.status == "REVOKED" {
                return Err(other_error_code(
                    "InvalidStateException",
                    "Certificate is already revoked.",
                ));
            }
            cert.status = "REVOKED".to_string();
            cert.revoked_at = Some(now_epoch());
            cert.revocation_reason = Some(reason.unwrap_or_else(|| "UNSPECIFIED".to_string()));
        }
        Ok(json!({}))
    })
}

fn op_export_certificate(body: &Value, account: u64, region: &str) -> OpResult {
    let arn = arn_param(body)?;
    let passphrase = body
        .get("Passphrase")
        .and_then(Value::as_str)
        .unwrap_or_default();
    if passphrase.len() < MIN_PASSPHRASE_LEN {
        return Err(validation(&format!(
            "Value at 'passphrase' failed to satisfy constraint: Member must have length greater than or equal to {MIN_PASSPHRASE_LEN}"
        )));
    }
    with_state(account, region, |state| {
        let cert = get_cert_mut(state, &arn, account)?;
        if cert.kind != CertKind::Private && !cert.export_enabled {
            return Err(validation(&format!(
                "Certificate ARN: {arn} is not a private certificate"
            )));
        }
        Ok(json!({
            "Certificate": stored_cert_pem(cert),
            "CertificateChain": stored_chain_pem(cert),
            "PrivateKey": cert.key_pem.clone().unwrap_or_default(),
        }))
    })
}

// ---------------------------------------------------------------------------
// base64 (RFC 4648, std alphabet; import blobs from botocore arrive base64)
// ---------------------------------------------------------------------------

fn b64_decode(input: &str) -> Option<Vec<u8>> {
    const REV: &[u8; 256] = &{
        let mut table = [0xffu8; 256];
        let alphabet = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
        let mut i = 0;
        while i < 64 {
            table[alphabet[i] as usize] = i as u8;
            i += 1;
        }
        table
    };
    let mut out = Vec::with_capacity(input.len() * 3 / 4);
    let mut acc: u32 = 0;
    let mut bits = 0;
    for byte in input.bytes() {
        match byte {
            b'=' | b'\n' | b'\r' | b' ' | b'\t' => continue,
            b if (b as usize) < REV.len() && REV[b as usize] != 0xff => {
                acc = (acc << 6) | REV[b as usize] as u32;
                bits += 6;
                if bits >= 8 {
                    bits -= 8;
                    out.push((acc >> bits) as u8);
                }
            }
            _ => return None,
        }
    }
    Some(out)
}

#[allow(dead_code)] // test support: build import blobs from byte payloads
fn b64_encode(data: &[u8]) -> String {
    const ALPHA: &[u8; 64] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
    let mut out = String::with_capacity(data.len().div_ceil(3) * 4);
    for chunk in data.chunks(3) {
        let n = (chunk[0] as u32) << 16
            | (chunk.get(1).copied().unwrap_or(0) as u32) << 8
            | (chunk.get(2).copied().unwrap_or(0) as u32);
        out.push(ALPHA[(n >> 18) as usize & 63] as char);
        out.push(ALPHA[(n >> 12) as usize & 63] as char);
        if chunk.len() > 1 {
            out.push(ALPHA[(n >> 6) as usize & 63] as char);
        } else {
            out.push('=');
        }
        if chunk.len() > 2 {
            out.push(ALPHA[n as usize & 63] as char);
        } else {
            out.push('=');
        }
    }
    out
}

// ---------------------------------------------------------------------------
// dispatch
// ---------------------------------------------------------------------------

pub fn dispatch(req: &Request) -> Response {
    let body = parse_json_body(&req.body);
    let result: OpResult = match req.operation.as_str() {
        "RequestCertificate" => op_request_certificate(&body, req.account, &req.region),
        "DescribeCertificate" => op_describe_certificate(&body, req.account, &req.region),
        "ListCertificates" => op_list_certificates(&body, req.account, &req.region),
        "AddTagsToCertificate" => op_add_tags(&body, req.account, &req.region),
        "RemoveTagsFromCertificate" => op_remove_tags(&body, req.account, &req.region),
        "ListTagsForCertificate" => op_list_tags(&body, req.account, &req.region),
        "ImportCertificate" => op_import_certificate(&body, req.account, &req.region),
        "DeleteCertificate" => op_delete_certificate(&body, req.account, &req.region),
        "GetCertificate" => op_get_certificate(&body, req.account, &req.region),
        "GetAccountConfiguration" => op_get_account_configuration(req.account, &req.region),
        "PutAccountConfiguration" => op_put_account_configuration(&body, req.account, &req.region),
        "UpdateCertificateOptions" => {
            op_update_certificate_options(&body, req.account, &req.region)
        }
        "ResendValidationEmail" => op_resend_validation_email(&body, req.account, &req.region),
        "RenewCertificate" => op_renew_certificate(&body, req.account, &req.region),
        "RevokeCertificate" => op_revoke_certificate(&body, req.account, &req.region),
        "ExportCertificate" => op_export_certificate(&body, req.account, &req.region),
        other => {
            return error_response(
                501,
                "NotImplemented",
                &format!("The {other} action has not been implemented"),
            )
        }
    };
    match result {
        Ok(value) => Response::json_status(200, &value),
        Err(resp) => resp,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    const ACCOUNT: u64 = 123456789012;
    const REGION: &str = "us-east-1";

    fn run(op: &str, body: Value) -> Response {
        let req = Request {
            service: "acm".to_string(),
            operation: op.to_string(),
            method: "POST".to_string(),
            path: "/".to_string(),
            query_string: String::new(),
            headers: HashMap::new(),
            body: serde_json::to_vec(&body).unwrap_or_default().into(),
            account: ACCOUNT,
            region: REGION.to_string(),
        };
        dispatch(&req)
    }

    fn body_of(resp: &Response) -> Value {
        serde_json::from_slice(&resp.body).unwrap_or(Value::Null)
    }

    fn status_of(resp: &Response) -> u16 {
        resp.status
    }

    #[test]
    fn request_describe_list_delete_cycle() {
        let resp = run("RequestCertificate", json!({"DomainName": "example.com"}));
        assert_eq!(status_of(&resp), 200);
        let arn = body_of(&resp)["CertificateArn"]
            .as_str()
            .unwrap()
            .to_string();
        assert!(arn.starts_with("arn:aws:acm:us-east-1:123456789012:certificate/"));

        let describe = run("DescribeCertificate", json!({"CertificateArn": arn}));
        let cert = &body_of(&describe)["Certificate"];
        assert_eq!(cert["Status"], "PENDING_VALIDATION");
        assert_eq!(cert["Type"], "AMAZON_ISSUED");
        assert_eq!(cert["DomainName"], "example.com");
        assert!(cert["CreatedAt"].is_number());

        let list = run("ListCertificates", json!({}));
        let listed = body_of(&list);
        let items = listed["CertificateSummaryList"]
            .as_array()
            .unwrap_or_else(|| panic!("list body = {listed}"));
        if !items.iter().any(|c| c["CertificateArn"] == arn.as_str()) {
            panic!("items = {items:?} arn = {arn}");
        }

        // status filter excludes on mismatch
        let list = run(
            "ListCertificates",
            json!({"CertificateStatuses": ["EXPIRED"]}),
        );
        assert!(body_of(&list)["CertificateSummaryList"]
            .as_array()
            .unwrap()
            .is_empty());

        run("DeleteCertificate", json!({"CertificateArn": arn}));
        let err = run("DescribeCertificate", json!({"CertificateArn": arn}));
        assert_eq!(status_of(&err), 400);
        assert_eq!(body_of(&err)["__type"], "ResourceNotFoundException");
    }

    #[test]
    fn idempotency_token_replays_same_arn() {
        let body = json!({"DomainName": "idem.example.com", "IdempotencyToken": "tok-1"});
        let r1 = run("RequestCertificate", body.clone());
        let r2 = run("RequestCertificate", body);
        assert_eq!(
            body_of(&r1)["CertificateArn"],
            body_of(&r2)["CertificateArn"]
        );
    }

    #[test]
    fn import_roundtrip_and_get_certificate() {
        const CERT: &str = "-----BEGIN CERTIFICATE-----\nQUJD\n-----END CERTIFICATE-----";
        let key = b64_encode(b"testkey");
        let cert = b64_encode(CERT.as_bytes());
        let resp = run(
            "ImportCertificate",
            json!({"Certificate": cert, "PrivateKey": key}),
        );
        let arn = body_of(&resp)["CertificateArn"]
            .as_str()
            .unwrap()
            .to_string();
        let describe = run("DescribeCertificate", json!({"CertificateArn": arn}));
        assert_eq!(body_of(&describe)["Certificate"]["Type"], "IMPORTED");
        assert_eq!(body_of(&describe)["Certificate"]["Status"], "ISSUED");

        let get = run("GetCertificate", json!({"CertificateArn": arn}));
        let got = body_of(&get);
        if !got["Certificate"]
            .as_str()
            .unwrap_or_default()
            .contains("BEGIN CERTIFICATE")
        {
            panic!("get = {get:?} got = {got} arn = {arn}");
        }
        assert!(body_of(&get)["CertificateChain"]
            .as_str()
            .unwrap()
            .contains("BEGIN CERTIFICATE"));
    }

    #[test]
    fn import_rejects_non_base64() {
        let resp = run(
            "ImportCertificate",
            json!({"Certificate": "not-base64!!", "PrivateKey": "QUJD"}),
        );
        assert_eq!(status_of(&resp), 400);
        assert_eq!(body_of(&resp)["__type"], "ValidationException");
    }

    #[test]
    fn tag_lifecycle() {
        let arn = body_of(&run(
            "RequestCertificate",
            json!({"DomainName": "tags.example.com"}),
        ))["CertificateArn"]
            .as_str()
            .unwrap()
            .to_string();
        run(
            "AddTagsToCertificate",
            json!({"CertificateArn": arn, "Tags": [{"Key": "env", "Value": "test"}]}),
        );
        let listed = run("ListTagsForCertificate", json!({"CertificateArn": arn}));
        let tags = body_of(&listed)["Tags"].as_array().unwrap().clone();
        assert_eq!(tags.len(), 1);
        assert_eq!(tags[0]["Key"], "env");
        assert_eq!(tags[0]["Value"], "test");
        run(
            "RemoveTagsFromCertificate",
            json!({"CertificateArn": arn, "Tags": [{"Key": "env", "Value": "test"}]}),
        );
        let listed = run("ListTagsForCertificate", json!({"CertificateArn": arn}));
        assert_eq!(body_of(&listed)["Tags"].as_array().unwrap().len(), 0);
    }

    #[test]
    fn missing_arn_gives_invalid_parameter_exception() {
        let err = run("DeleteCertificate", json!({}));
        assert_eq!(body_of(&err)["__type"], "InvalidParameterException");
        assert_eq!(body_of(&err)["message"], MISSING_PARAMETER);
        assert_eq!(status_of(&err), 400);
    }

    #[test]
    fn unknown_op_still_501() {
        let err = run("BogusAction", json!({}));
        assert_eq!(status_of(&err), 501);
        // Unported ops keep the query-XML NotImplemented document.
        let text = String::from_utf8_lossy(&err.body);
        assert!(text.contains("NotImplemented"), "body = {text}");
    }

    #[test]
    fn account_configuration_roundtrip() {
        let got = run("GetAccountConfiguration", json!({}));
        assert_eq!(body_of(&got)["ExpiryEvents"]["DaysBeforeExpiry"], 45);
        run(
            "PutAccountConfiguration",
            json!({"ExpiryEvents": {"DaysBeforeExpiry": 30}}),
        );
        let got = run("GetAccountConfiguration", json!({}));
        assert_eq!(body_of(&got)["ExpiryEvents"]["DaysBeforeExpiry"], 30);
        let bad = run(
            "PutAccountConfiguration",
            json!({"ExpiryEvents": {"DaysBeforeExpiry": 0}}),
        );
        assert_eq!(body_of(&bad)["__type"], "ValidationException");
    }

    #[test]
    fn update_certificate_options_roundtrip() {
        let arn = body_of(&run(
            "RequestCertificate",
            json!({"DomainName": "opts.example.com"}),
        ))["CertificateArn"]
            .as_str()
            .unwrap()
            .to_string();
        run(
            "UpdateCertificateOptions",
            json!({"CertificateArn": arn, "Options": {"CertificateTransparencyLoggingPreference": "DISABLED"}}),
        );
        let describe = run("DescribeCertificate", json!({"CertificateArn": arn}));
        assert_eq!(
            body_of(&describe)["Certificate"]["Options"]
                ["CertificateTransparencyLoggingPreference"],
            "DISABLED"
        );
    }

    #[test]
    fn export_non_private_is_validation_error() {
        let cert = b64_encode(b"-----BEGIN CERTIFICATE-----\nQUJD\n-----END CERTIFICATE-----");
        let key = b64_encode(b"testkey");
        let arn = body_of(&run(
            "ImportCertificate",
            json!({"Certificate": cert, "PrivateKey": key}),
        ))["CertificateArn"]
            .as_str()
            .unwrap()
            .to_string();
        let err = run(
            "ExportCertificate",
            json!({"CertificateArn": arn, "Passphrase": "dGVzdA=="}),
        );
        assert_eq!(body_of(&err)["__type"], "ValidationException");
    }

    #[test]
    fn resending_email_domain_mismatch() {
        let arn = body_of(&run(
            "RequestCertificate",
            json!({"DomainName": "resend.example.com"}),
        ))["CertificateArn"]
            .as_str()
            .unwrap()
            .to_string();
        let err = run(
            "ResendValidationEmail",
            json!({"CertificateArn": arn, "Domain": "other.example.com", "ValidationDomain": "example.com"}),
        );
        assert_eq!(
            body_of(&err)["__type"],
            "InvalidDomainValidationOptionsException"
        );
        let ok = run(
            "ResendValidationEmail",
            json!({"CertificateArn": arn, "Domain": "resend.example.com", "ValidationDomain": "example.com"}),
        );
        assert_eq!(status_of(&ok), 200);
    }

    #[test]
    fn renew_and_revoke_not_found() {
        let fake = "arn:aws:acm:us-east-1:123456789012:certificate/0000";
        let renew = run("RenewCertificate", json!({"CertificateArn": fake}));
        assert_eq!(body_of(&renew)["__type"], "ResourceNotFoundException");
        let revoke = run(
            "RevokeCertificate",
            json!({"CertificateArn": fake, "RevocationReason": "KEY_COMPROMISE"}),
        );
        assert_eq!(body_of(&revoke)["__type"], "ResourceNotFoundException");
    }

    #[test]
    fn b64_roundtrip() {
        let payload = b"great big base64 payload \xff test";
        let encoded = b64_encode(payload);
        let decoded = b64_decode(&encoded).unwrap();
        assert_eq!(decoded, payload);
    }

    #[test]
    fn acm_import_without_cn() {
        // CN is optional per the CAB Forum baseline (moto 5.2.2 #9912,
        // `test_import_certificate_without_cn`): importing succeeds and the
        // description keeps moto's fallback shape. robotocore does not parse
        // the stored PEM, so "no CN" and "any PEM" are indistinguishable and
        // the metadata lands on moto's no-CN-no-SAN fallback.
        const CN_LESS_CERT: &str =
            "-----BEGIN CERTIFICATE-----\nQUJDREVG\n-----END CERTIFICATE-----";
        let cert = b64_encode(CN_LESS_CERT.as_bytes());
        let key = b64_encode(b"testkey");
        let resp = run(
            "ImportCertificate",
            json!({"Certificate": cert, "PrivateKey": key}),
        );
        assert_eq!(status_of(&resp), 200);
        let arn = body_of(&resp)["CertificateArn"]
            .as_str()
            .unwrap()
            .to_string();

        let detail =
            &body_of(&run("DescribeCertificate", json!({"CertificateArn": arn})))["Certificate"];
        assert_eq!(detail["Type"], "IMPORTED");
        assert_eq!(detail["Status"], "ISSUED");
        assert_eq!(detail["DomainName"], "");
        assert_eq!(detail["Subject"], "CN=");
        assert_eq!(detail["Issuer"], "");
    }

    #[test]
    fn acm_request_cert_defaults() {
        // Partial Options merge over the default pair (moto 5.2.2 #10030,
        // `test_request_certificate_options_without_export_defaults_to_disabled`).
        let arn = body_of(&run(
            "RequestCertificate",
            json!({
                "DomainName": "opts-merge.example.com",
                "Options": {"CertificateTransparencyLoggingPreference": "DISABLED"},
            }),
        ))["CertificateArn"]
            .as_str()
            .unwrap()
            .to_string();
        let cert =
            &body_of(&run("DescribeCertificate", json!({"CertificateArn": arn})))["Certificate"];
        assert_eq!(
            cert["Options"]["CertificateTransparencyLoggingPreference"],
            "DISABLED"
        );
        assert_eq!(cert["Options"]["Export"], "DISABLED");
        let summary = &body_of(&run("ListCertificates", json!({})))["CertificateSummaryList"];
        let listed = summary
            .as_array()
            .unwrap()
            .iter()
            .find(|c| c["CertificateArn"] == arn.as_str())
            .expect("summary row for merged-options cert");
        assert_eq!(listed["Exported"], false);

        // No Options member at all keeps both defaults.
        let arn = body_of(&run(
            "RequestCertificate",
            json!({"DomainName": "opts-plain.example.com"}),
        ))["CertificateArn"]
            .as_str()
            .unwrap()
            .to_string();
        let cert =
            &body_of(&run("DescribeCertificate", json!({"CertificateArn": arn})))["Certificate"];
        assert_eq!(
            cert["Options"]["CertificateTransparencyLoggingPreference"],
            "ENABLED"
        );
        assert_eq!(cert["Options"]["Export"], "DISABLED");

        // Options with only Export keeps the CTLogging default.
        let arn = body_of(&run(
            "RequestCertificate",
            json!({
                "DomainName": "opts-export.example.com",
                "Options": {"Export": "ENABLED"},
            }),
        ))["CertificateArn"]
            .as_str()
            .unwrap()
            .to_string();
        let cert =
            &body_of(&run("DescribeCertificate", json!({"CertificateArn": arn})))["Certificate"];
        assert_eq!(cert["Options"]["Export"], "ENABLED");
        assert_eq!(
            cert["Options"]["CertificateTransparencyLoggingPreference"],
            "ENABLED"
        );
        let summary = &body_of(&run("ListCertificates", json!({})))["CertificateSummaryList"];
        let listed = summary
            .as_array()
            .unwrap()
            .iter()
            .find(|c| c["CertificateArn"] == arn.as_str())
            .expect("summary row for exportable cert");
        assert_eq!(listed["Exported"], true);
    }
}
