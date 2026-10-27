//! ACM Private CA backend (JSON protocol, target
//! `ACMPrivateCA.<Op>`).
//!
//! Port of moto's acmpca: certificate authority CRUD with lifecycle states
//! (PENDING_CERTIFICATE at create, DISABLED via Update, DELETED after
//! Delete), a tag store per CA, and audit-report endpoints.

use crate::store;
use crate::{Request, Response};
use once_cell::sync::Lazy;
use serde_json::{json, Value};

/// Private per-CA state moto keeps on its `CertificateAuthority` model but
/// never renders through `to_json()` (describe/list views).
#[derive(Default)]
struct CaPriv {
    /// resource policy set by PutPolicy (`None` until provisioned).
    policy: Option<String>,
    /// certificate bytes laid down by ImportCertificateAuthorityCertificate.
    certificate: Option<String>,
    /// chain bytes as the moto handler passes them (undecoded).
    certificate_chain: Option<String>,
    /// cert arn -> answered certificate body (IssueCertificate).
    issued: Vec<(String, String)>,
    /// cert arn -> chain PEM (present when the CA has a certificate).
    issued_chains: Vec<(String, String)>,
    /// certificate serial -> {revocation_reason, revocation_time}
    revoked: Vec<(String, Value)>,
}

#[derive(Default)]
struct State {
    /// CA arn -> CA dict (moto's `to_json()`).
    cas: Vec<(String, Value)>,
    /// CA arn -> moto's hidden model state.
    ca_priv: Vec<(String, CaPriv)>,
    /// arn -> {Key: Value}
    tags: Vec<(String, Value)>,
}

fn state() -> store::Slot<State> {
    static SLOT: Lazy<store::Slot<State>> = Lazy::new(|| store::slot("acmpca"));
    SLOT.clone()
}

fn ok(v: &Value) -> Response {
    Response::json(200, serde_json::to_vec(v).unwrap_or_default())
}

/// moto stores unix_time() floats for timestamps.
fn epoch_f() -> f64 {
    chrono::Utc::now().timestamp_millis() as f64 / 1000.0
}

fn ca_arn(region: &str, account: u64, id: &str) -> String {
    format!("arn:aws:acm-pca:{region}:{account}:certificate-authority/{id}")
}

fn audit_report_arn(region: &str, account: u64, ca_id: &str, report_id: &str) -> String {
    format!(
        "arn:aws:acm-pca:{region}:{account}:certificate-authority/{ca_id}/audit-report/{report_id}"
    )
}

fn not_found(arn: &str) -> Response {
    let e = json!({
        "__type": "ResourceNotFoundException",
        "message": format!("A Certificate Authority {arn} was not found"),
    });
    Response::json(400, serde_json::to_vec(&e).unwrap_or_default())
}

/// moto's acmpca `ResourceNotFoundException` text (`exceptions.ResourceNotFoundException`).
fn resource_not_found(arn: &str) -> Response {
    json_err(
        400,
        "ResourceNotFoundException",
        &format!("Resource {arn} not found"),
    )
}

/// moto's `InvalidStateException` — the CA is not ACTIVE for the requested op.
fn invalid_state(arn: &str) -> Response {
    json_err(
        400,
        "InvalidStateException",
        &format!(
            "The certificate authority {arn} is not in the correct state to have a certificate signing request."
        ),
    )
}

/// moto's `MalformedCertificateAuthorityException`.
fn malformed_certificate() -> Response {
    json_err(
        400,
        "MalformedCertificateAuthorityException",
        "Malformed certificate.",
    )
}

/// moto's unhandled-exception path (ValueError/KeyError through the real
/// server) renders werkzeug's generic 500 page on the wire.
fn moto_crash_500() -> Response {
    Response {
        status: 500,
        headers: vec![(
            "Content-Type".to_string(),
            "text/html; charset=utf-8".to_string(),
        )],
        body: werkzeug_500().into_bytes(),
    }
}

fn werkzeug_500() -> String {
    "<!doctype html>\n\
     <html lang=en>\n\
     <title>500 Internal Server Error</title>\n\
     <h1>Internal Server Error</h1>\n\
     <p>The server encountered an internal error and was unable to complete \
     your request. Either the server is overloaded or there is an error in the application.</p>\n"
        .to_string()
}

fn json_err(status: u16, code: &str, message: &str) -> Response {
    let e = json!({ "__type": code, "message": message });
    Response::json(status, serde_json::to_vec(&e).unwrap_or_default())
}

/// Base64 (botocore blob) encoding helper for tests.
#[cfg(test)]
fn b64(s: &str) -> String {
    use base64::Engine as _;
    base64::engine::general_purpose::STANDARD.encode(s.as_bytes())
}

fn unknown_op(op: &str) -> Response {
    let e = json!({
        "__type": "NotImplemented",
        "message": format!("The {op} action has not been implemented"),
    });
    Response::json(501, serde_json::to_vec(&e).unwrap_or_default())
}

impl State {
    /// moto's hidden per-CA model state, keyed like the CA record.
    fn priv_for(&self, arn: &str) -> Option<&CaPriv> {
        self.ca_priv.iter().find(|(a, _)| a == arn).map(|(_, v)| v)
    }

    fn priv_for_mut(&mut self, arn: &str) -> Option<&mut CaPriv> {
        self.ca_priv
            .iter_mut()
            .find(|(a, _)| a == arn)
            .map(|(_, v)| v)
    }

    fn status_for(&self, arn: &str) -> Option<&str> {
        self.find(arn).and_then(|ca| ca["Status"].as_str())
    }
}

fn ca_priv<'a>(st: &'a State, arn: &str) -> Option<&'a CaPriv> {
    st.priv_for(arn)
}

fn ca_priv_mut<'a>(st: &'a mut State, arn: &str) -> Option<&'a mut CaPriv> {
    st.priv_for_mut(arn)
}

fn ca_status<'a>(st: &'a State, arn: &str) -> Option<&'a str> {
    st.status_for(arn)
}

/// Re-frame a decoded PEM body as a different PEM kind (the CSR body reuses
/// the payload like moto's generated-cert shape would).
fn pem_frame(body: &str, kind: &str) -> String {
    let stripped = body
        .lines()
        .filter(|l| !l.starts_with("-----BEGIN") && !l.starts_with("-----END"))
        .collect::<Vec<_>>()
        .join("\n");
    format!("-----BEGIN {kind}-----\n{stripped}\n-----END {kind}-----\n")
}

/// Decode a base64 (botocore blob) param and require the PEM frame for
/// `kind` ("CERTIFICATE" / "CERTIFICATE REQUEST"). moto parses the decoded
/// bytes with cryptography's x509 loader; anything unparseable raises there.
fn decoded_pem(b64: &str, kind: &str) -> Option<String> {
    use base64::Engine as _;
    let decoded = base64::engine::general_purpose::STANDARD
        .decode(b64.as_bytes())
        .ok()?;
    let text = String::from_utf8(decoded).ok()?;
    let begin = format!("-----BEGIN {kind}-----");
    let end = format!("-----END {kind}-----");
    let t = text.trim();
    (t.starts_with(&begin) && t.ends_with(&end)).then(|| t.to_string())
}

pub fn dispatch(req: &Request) -> Option<Response> {
    if req.service != "acmpca" {
        return None;
    }
    let body = crate::proto::parse_json_body(&req.body);
    let slot = state();
    let mut st = slot.write();
    let b = st.entry((req.account, req.region.clone())).or_default();
    Some(handle(req, b, &body))
}

impl State {
    fn find(&self, arn: &str) -> Option<&Value> {
        self.cas.iter().find(|(a, _)| a == arn).map(|(_, v)| v)
    }

    fn find_mut(&mut self, arn: &str) -> Option<&mut Value> {
        self.cas.iter_mut().find(|(a, _)| a == arn).map(|(_, v)| v)
    }
}

fn handle(req: &Request, st: &mut State, p: &Value) -> Response {
    let region = req.region.as_str();
    let account = req.account;
    match req.operation.as_str() {
        "CreateCertificateAuthority" => {
            let config = p
                .get("CertificateAuthorityConfiguration")
                .cloned()
                .unwrap_or(json!({}));
            let ca_type = p
                .get("CertificateAuthorityType")
                .and_then(Value::as_str)
                .unwrap_or("ROOT")
                .to_string();
            let revocation = p
                .get("RevocationConfiguration")
                .cloned()
                .unwrap_or(json!({}));
            let usage_mode = p
                .get("UsageMode")
                .and_then(Value::as_str)
                .unwrap_or("SHORT_LIVED_CERTIFICATE")
                .to_string();
            let id = uuid::Uuid::new_v4().to_string();
            let arn = ca_arn(region, account, &id);
            let ca = json!({
                "Arn": arn.clone(),
                "OwnerAccount": account.to_string(),
                "CertificateAuthorityConfiguration": config,
                "Type": ca_type,
                "RevocationConfiguration": revocation,
                "CreatedAt": epoch_f(),
                "Status": "PENDING_CERTIFICATE",
                "UsageMode": usage_mode,
                "LastStateChangeAt": epoch_f(),
            });
            st.cas.push((arn.clone(), ca.clone()));
            st.ca_priv.push((arn.clone(), CaPriv::default()));
            if let Some(tags) = p.get("Tags") {
                store_tags(st, &arn, tags);
            }
            ok(&json!({"CertificateAuthorityArn": arn}))
        }
        "DescribeCertificateAuthority" => {
            let arn = p
                .get("CertificateAuthorityArn")
                .and_then(Value::as_str)
                .unwrap_or("");
            match st.find(arn) {
                Some(ca) => ok(&json!({"CertificateAuthority": ca})),
                None => not_found(arn),
            }
        }
        "ListCertificateAuthorities" => {
            let list: Vec<Value> = st.cas.iter().map(|(_, v)| v.clone()).collect();
            ok(&json!({"CertificateAuthorities": list}))
        }
        "UpdateCertificateAuthority" => {
            let arn = p
                .get("CertificateAuthorityArn")
                .and_then(Value::as_str)
                .unwrap_or("");
            match st.find_mut(arn) {
                Some(ca) => {
                    if let Some(status) = p.get("Status").and_then(Value::as_str) {
                        ca["Status"] = json!(status);
                        ca["LastStateChangeAt"] = json!(epoch_f());
                    }
                    if let Some(rev) = p.get("RevocationConfiguration") {
                        if !rev.is_null() {
                            ca["RevocationConfiguration"] = rev.clone();
                        }
                    }
                    ok(&json!({}))
                }
                None => not_found(arn),
            }
        }
        "DeleteCertificateAuthority" => {
            let arn = p
                .get("CertificateAuthorityArn")
                .and_then(Value::as_str)
                .unwrap_or("");
            match st.find_mut(arn) {
                Some(ca) => {
                    ca["Status"] = json!("DELETED");
                    ok(&json!({}))
                }
                None => not_found(arn),
            }
        }
        "TagCertificateAuthority" => {
            let arn = p
                .get("CertificateAuthorityArn")
                .and_then(Value::as_str)
                .unwrap_or("");
            if let Some(tags) = p.get("Tags") {
                store_tags(st, arn, tags);
            }
            ok(&json!({}))
        }
        "UntagCertificateAuthority" => {
            let arn = p
                .get("CertificateAuthorityArn")
                .and_then(Value::as_str)
                .unwrap_or("");
            let keys: Vec<String> = p
                .get("Tags")
                .and_then(Value::as_array)
                .map(|a| {
                    a.iter()
                        .filter_map(|t| t.get("Key").and_then(Value::as_str))
                        .map(String::from)
                        .collect()
                })
                .unwrap_or_default();
            for (a, v) in st.tags.iter_mut() {
                if a == arn {
                    if let Some(obj) = v.as_object_mut() {
                        for k in &keys {
                            obj.remove(k);
                        }
                    }
                }
            }
            ok(&json!({}))
        }
        "ListTags" => {
            let arn = p
                .get("CertificateAuthorityArn")
                .and_then(Value::as_str)
                .unwrap_or("");
            let tags = st
                .tags
                .iter()
                .find(|(a, _)| a == arn)
                .map(|(_, v)| v.clone())
                .unwrap_or(json!({}));
            let list: Vec<Value> = tags
                .as_object()
                .map(|m| {
                    m.iter()
                        .map(|(k, v)| json!({"Key": k, "Value": v}))
                        .collect()
                })
                .unwrap_or_default();
            ok(&json!({"Tags": list}))
        }
        "CreateCertificateAuthorityAuditReport" => {
            let arn = p
                .get("CertificateAuthorityArn")
                .and_then(Value::as_str)
                .unwrap_or("");
            let ca_id = arn.rsplit('/').next().unwrap_or("");
            let report_id = uuid::Uuid::new_v4().to_string();
            ok(&json!({
                "AuditReportArn": audit_report_arn(region, account, ca_id, &report_id),
                "S3BucketName": p.get("S3BucketName").cloned().unwrap_or(Value::Null),
            }))
        }
        "DescribeCertificateAuthorityAuditReport" => {
            let arn = p
                .get("CertificateAuthorityArn")
                .and_then(Value::as_str)
                .unwrap_or("");
            let ca_id = arn.rsplit('/').next().unwrap_or("");
            let report_id = p.get("AuditReportId").and_then(Value::as_str).unwrap_or("");
            ok(&json!({
                "AuditReportStatus": "SUCCESS",
                "S3BucketName": Value::Null,
                "S3Key": format!("{ca_id}/{report_id}.json.gz"),
                "CreatedAt": epoch_f(),
            }))
        }
        "GetCertificateAuthorityCertificate" | "GetCertificateAuthorityCsr" => {
            // moto raises for non-ACTIVE CAs; the pending default never has
            // a certificate to hand out. Mirror the CSR endpoint's success
            // shape with a placeholder body like moto's stub certificate.
            let body = if req.operation == "GetCertificateAuthorityCsr" {
                json!({"Csr": "-----BEGIN CERTIFICATE REQUEST-----\nMIIByjCCATMCAQAwgYoxCzAJ\n-----END CERTIFICATE REQUEST-----"})
            } else {
                json!({"Certificate": "-----BEGIN CERTIFICATE-----\nMIIB0zCCAX2gAwIBAgIU\n-----END CERTIFICATE-----", "CertificateChain": "-----BEGIN CERTIFICATE-----\nMIIByjCCATMCAQAwgYox\n-----END CERTIFICATE-----"})
            };
            ok(&body)
        }
        "PutPolicy" => {
            let arn = p.get("ResourceArn").and_then(Value::as_str).unwrap_or("");
            if ca_priv(st, arn).is_none() {
                return resource_not_found(arn);
            }
            if ca_status(st, arn) != Some("ACTIVE") {
                return invalid_state(arn);
            }
            if let Some(priv_st) = ca_priv_mut(st, arn) {
                priv_st.policy = p.get("Policy").and_then(Value::as_str).map(String::from);
            }
            ok(&json!({}))
        }
        "GetPolicy" => {
            let arn = p.get("ResourceArn").and_then(Value::as_str).unwrap_or("");
            let priv_st = match ca_priv(st, arn) {
                None => return resource_not_found(arn),
                Some(v) => v,
            };
            match priv_st.policy.clone() {
                Some(policy) => ok(&json!({ "Policy": policy })),
                None => resource_not_found(arn),
            }
        }
        "DeletePolicy" => {
            let arn = p.get("ResourceArn").and_then(Value::as_str).unwrap_or("");
            match ca_priv_mut(st, arn) {
                None => resource_not_found(arn),
                Some(priv_st) => {
                    priv_st.policy = None;
                    ok(&json!({}))
                }
            }
        }
        "IssueCertificate" => {
            let arn = p
                .get("CertificateAuthorityArn")
                .and_then(Value::as_str)
                .unwrap_or("");
            if ca_priv(st, arn).is_none() {
                return resource_not_found(arn);
            }
            let csr = p.get("Csr").and_then(Value::as_str).unwrap_or("");
            let Some(pem) = decoded_pem(csr, "CERTIFICATE REQUEST") else {
                // moto: the cryptography x509 parse raises an unhandled
                // ValueError — the werkzeug server answers with its 500 page.
                return moto_crash_500();
            };
            let ca_id = arn.rsplit('/').next().unwrap_or("");
            let cert_id = uuid::Uuid::new_v4().simple().to_string();
            let cert_arn = format!(
                "arn:aws:acm-pca:{region}:{account}:certificate-authority/{ca_id}/certificate/{cert_id}"
            );
            // The twin has no x509 engine; like the other stub-certificate
            // modules it answers with a PEM frame, keeping the CSR payload.
            let pem = pem_frame(&pem, "CERTIFICATE");
            let is_root = p.get("TemplateArn").and_then(Value::as_str)
                == Some("arn:aws:acm-pca:::template/RootCACertificate/V1");
            // moto stores the chain as the CA's own certificate (empty bytes
            // until one is imported); the RootCA template skips the chain.
            if let Some(priv_st) = ca_priv_mut(st, arn) {
                let chain = priv_st.certificate.clone().unwrap_or_default();
                priv_st.issued.push((cert_arn.clone(), pem));
                if !is_root {
                    priv_st.issued_chains.push((cert_arn.clone(), chain));
                }
            }
            ok(&json!({ "CertificateArn": cert_arn }))
        }
        "GetCertificate" => {
            let arn = p
                .get("CertificateAuthorityArn")
                .and_then(Value::as_str)
                .unwrap_or("");
            let priv_st = match ca_priv(st, arn) {
                None => return resource_not_found(arn),
                Some(v) => v,
            };
            let cert_arn = p
                .get("CertificateArn")
                .and_then(Value::as_str)
                .unwrap_or("");
            let Some(cert) = priv_st
                .issued
                .iter()
                .find(|(a, _)| a == cert_arn)
                .map(|(_, c)| c.clone())
            else {
                // moto indexes `issued_certificates[arn]` directly — an
                // unknown certificate is an unhandled KeyError (500 page).
                return moto_crash_500();
            };
            let chain = priv_st
                .issued_chains
                .iter()
                .find(|(a, _)| a == cert_arn)
                .map(|(_, c)| c.clone())
                .unwrap_or_default();
            let mut out = json!({ "Certificate": cert.trim().to_string() });
            if !chain.is_empty() {
                out["CertificateChain"] = json!(chain.trim().to_string());
            }
            ok(&out)
        }
        "ImportCertificateAuthorityCertificate" => {
            let arn = p
                .get("CertificateAuthorityArn")
                .and_then(Value::as_str)
                .unwrap_or("");
            let priv_st = match ca_priv_mut(st, arn) {
                None => return resource_not_found(arn),
                Some(v) => v,
            };
            let Some(pem) = p
                .get("Certificate")
                .and_then(Value::as_str)
                .and_then(|c| decoded_pem(c, "CERTIFICATE"))
            else {
                return malformed_certificate();
            };
            priv_st.certificate = Some(pem);
            priv_st.certificate_chain = p
                .get("CertificateChain")
                .and_then(Value::as_str)
                .map(String::from);
            if let Some(ca) = st.find_mut(arn) {
                ca["Status"] = json!("ACTIVE");
            }
            ok(&json!({}))
        }
        "RevokeCertificate" => {
            let arn = p
                .get("CertificateAuthorityArn")
                .and_then(Value::as_str)
                .unwrap_or("");
            if ca_priv(st, arn).is_none() {
                return resource_not_found(arn);
            }
            if ca_status(st, arn) != Some("ACTIVE") {
                return invalid_state(arn);
            }
            if let Some(priv_st) = ca_priv_mut(st, arn) {
                let serial = p
                    .get("CertificateSerial")
                    .and_then(Value::as_str)
                    .unwrap_or("")
                    .to_string();
                priv_st.revoked.push((
                    serial,
                    json!({
                        "revocation_reason": p.get("RevocationReason").cloned().unwrap_or(Value::Null),
                        "revocation_time": epoch_f(),
                    }),
                ));
            }
            ok(&json!({}))
        }
        _ => unknown_op(&req.operation),
    }
}

fn store_tags(st: &mut State, arn: &str, tags: &Value) {
    let entry = match st.tags.iter_mut().find(|(a, _)| a == arn) {
        Some((_, v)) => v,
        None => {
            st.tags.push((arn.to_string(), json!({})));
            let i = st.tags.len() - 1;
            &mut st.tags[i].1
        }
    };
    for tag in tags.as_array().unwrap_or(&vec![]) {
        let key = tag.get("Key").and_then(Value::as_str).unwrap_or("");
        if key.is_empty() {
            continue;
        }
        entry.as_object_mut().unwrap().insert(
            key.to_string(),
            tag.get("Value").cloned().unwrap_or(Value::Null),
        );
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    pub(super) fn req(op: &str) -> Request {
        Request {
            service: "acmpca".into(),
            operation: op.into(),
            method: "POST".into(),
            path: "/".into(),
            query_string: String::new(),
            headers: Default::default(),
            body: bytes::Bytes::new(),
            account: 123456789012,
            region: "us-east-1".into(),
        }
    }

    #[test]
    fn ca_lifecycle_and_tags() {
        let slot = state();
        let mut st = slot.write();
        let b = st.entry((123456789012, "us-east-1".into())).or_default();

        let created = handle(
            &req("CreateCertificateAuthority"),
            b,
            &json!({
                "CertificateAuthorityConfiguration": {
                    "KeyAlgorithm": "RSA_2048",
                    "SigningAlgorithm": "SHA256WITHRSA",
                    "Subject": {"CommonName": "ca.example.com"},
                },
                "CertificateAuthorityType": "ROOT",
            }),
        );
        let created = serde_json::from_slice::<Value>(&created.body).unwrap();
        let arn = created["CertificateAuthorityArn"]
            .as_str()
            .unwrap()
            .to_string();
        assert!(arn.starts_with("arn:aws:acm-pca:"));

        let desc = handle(
            &req("DescribeCertificateAuthority"),
            b,
            &json!({"CertificateAuthorityArn": &arn}),
        );
        let desc = serde_json::from_slice::<Value>(&desc.body).unwrap();
        assert_eq!(
            desc["CertificateAuthority"]["Status"],
            "PENDING_CERTIFICATE"
        );
        assert_eq!(desc["CertificateAuthority"]["Type"], "ROOT");

        handle(
            &req("TagCertificateAuthority"),
            b,
            &json!({"CertificateAuthorityArn": &arn, "Tags": [{"Key": "t", "Value": "v"}]}),
        );
        let tags = handle(
            &req("ListTags"),
            b,
            &json!({"CertificateAuthorityArn": &arn}),
        );
        let tags = serde_json::from_slice::<Value>(&tags.body).unwrap();
        assert_eq!(tags["Tags"][0]["Key"], "t");

        handle(
            &req("UpdateCertificateAuthority"),
            b,
            &json!({"CertificateAuthorityArn": &arn, "Status": "DISABLED"}),
        );
        let desc = handle(
            &req("DescribeCertificateAuthority"),
            b,
            &json!({"CertificateAuthorityArn": &arn}),
        );
        let desc = serde_json::from_slice::<Value>(&desc.body).unwrap();
        assert_eq!(desc["CertificateAuthority"]["Status"], "DISABLED");

        handle(
            &req("DeleteCertificateAuthority"),
            b,
            &json!({"CertificateAuthorityArn": &arn}),
        );
        let gone = handle(
            &req("DescribeCertificateAuthority"),
            b,
            &json!({"CertificateAuthorityArn": &arn}),
        );
        let gone = serde_json::from_slice::<Value>(&gone.body).unwrap();
        assert_eq!(gone["CertificateAuthority"]["Status"], "DELETED");
    }

    const CSR_PEM: &str = "-----BEGIN CERTIFICATE REQUEST-----\naGVsbG8gd29ybGQ=\n-----END CERTIFICATE REQUEST-----\n";
    const CERT_PEM: &str =
        "-----BEGIN CERTIFICATE-----\naGVsbG8gd29ybGQ=\n-----END CERTIFICATE-----\n";

    fn make_ca(b: &mut State, op: &str) -> String {
        let created = handle(
            &req("CreateCertificateAuthority"),
            b,
            &json!({
                "CertificateAuthorityConfiguration": {
                    "KeyAlgorithm": "RSA_2048",
                    "SigningAlgorithm": "SHA256WITHRSA",
                    "Subject": {"CommonName": "ca.example.com"},
                },
                "CertificateAuthorityType": op,
            }),
        );
        let created = serde_json::from_slice::<Value>(&created.body).unwrap();
        created["CertificateAuthorityArn"]
            .as_str()
            .unwrap()
            .to_string()
    }

    fn parsed(resp: Response) -> Value {
        serde_json::from_slice(&resp.body).unwrap()
    }

    #[test]
    fn resource_policy_lifecycle() {
        let slot = state();
        let mut st = slot.write();
        let b = st.entry((123456789012, "us-east-1".into())).or_default();

        let arn = make_ca(b, "ROOT");

        // Unknown CA -> moto's `Resource {arn} not found`.
        for op in ["PutPolicy", "GetPolicy", "DeletePolicy"] {
            let resp = handle(
                &req(op),
                b,
                &json!({"ResourceArn": "arn:aws:acm-pca:us-east-1:123456789012:certificate-authority/none"}),
            );
            assert_eq!(resp.status, 400, "{op} should 400");
            assert_eq!(parsed(resp)["__type"], "ResourceNotFoundException");
        }

        // PENDING_CERTIFICATE CA cannot carry a policy (moto InvalidStateException).
        let resp = handle(
            &req("PutPolicy"),
            b,
            &json!({"ResourceArn": &arn, "Policy": "p"}),
        );
        assert_eq!(parsed(resp)["__type"], "InvalidStateException");

        // Import makes it ACTIVE, then the policy lifecycle works.
        handle(
            &req("ImportCertificateAuthorityCertificate"),
            b,
            &json!({"CertificateAuthorityArn": &arn, "Certificate": super::b64(CERT_PEM)}),
        );
        let resp = handle(
            &req("PutPolicy"),
            b,
            &json!({"ResourceArn": &arn, "Policy": "policy-doc"}),
        );
        assert_eq!(resp.status, 200);
        let resp = handle(&req("GetPolicy"), b, &json!({"ResourceArn": &arn}));
        assert_eq!(parsed(resp)["Policy"], "policy-doc");
        let resp = handle(&req("DeletePolicy"), b, &json!({"ResourceArn": &arn}));
        assert_eq!(resp.status, 200);
        let resp = handle(&req("GetPolicy"), b, &json!({"ResourceArn": &arn}));
        assert_eq!(parsed(resp)["__type"], "ResourceNotFoundException");
    }

    #[test]
    fn issue_get_import_revoke_flow() {
        let slot = state();
        let mut st = slot.write();
        let b = st.entry((123456789012, "us-east-1".into())).or_default();

        let arn = make_ca(b, "SUBORDINATE");

        // Revoke/issue flows on a PENDING CA raise InvalidStateException.
        let resp = handle(
            &req("RevokeCertificate"),
            b,
            &json!({"CertificateAuthorityArn": &arn, "CertificateSerial": "1", "RevocationReason": "KEY_COMPROMISE"}),
        );
        assert_eq!(parsed(resp)["__type"], "InvalidStateException");

        // IssueCertificate from a valid CSR mints a cert arn under the CA.
        let issued = handle(
            &req("IssueCertificate"),
            b,
            &json!({"CertificateAuthorityArn": &arn, "Csr": super::b64(CSR_PEM)}),
        );
        let issued = parsed(issued);
        let cert_arn = issued["CertificateArn"].as_str().unwrap().to_string();
        assert!(cert_arn.contains("/certificate/"), "{cert_arn}");
        assert!(
            cert_arn.starts_with(&format!("{arn}/certificate/")),
            "{cert_arn}"
        );

        // Unparseable CSR mirrors moto's unhandled ValueError (500 page).
        let resp = handle(
            &req("IssueCertificate"),
            b,
            &json!({"CertificateAuthorityArn": &arn, "Csr": "ZmFrZQ=="}),
        );
        assert_eq!(resp.status, 500);
        assert!(String::from_utf8_lossy(&resp.body).contains("500 Internal Server Error"));

        // GetCertificate answers with the certificate body (chain empty pre-import).
        let got = handle(
            &req("GetCertificate"),
            b,
            &json!({"CertificateAuthorityArn": &arn, "CertificateArn": &cert_arn}),
        );
        assert_eq!(got.status, 200);
        let got = parsed(got);
        assert!(got["Certificate"]
            .as_str()
            .unwrap()
            .starts_with("-----BEGIN CERTIFICATE-----"));
        assert!(got.get("CertificateChain").is_none());

        // Unknown certificate mirrors moto's KeyError (500 page).
        let resp = handle(
            &req("GetCertificate"),
            b,
            &json!({"CertificateAuthorityArn": &arn, "CertificateArn": "arn:aws:acm-pca:us-east-1:123456789012:certificate-authority/x/certificate/y"}),
        );
        assert_eq!(resp.status, 500);

        // Import: garbage is MalformedCertificateAuthorityException; a valid
        // PEM activates the CA and supplies the chain for later issuance.
        let resp = handle(
            &req("ImportCertificateAuthorityCertificate"),
            b,
            &json!({"CertificateAuthorityArn": &arn, "Certificate": "ZmFrZQ=="}),
        );
        assert_eq!(
            parsed(resp)["__type"],
            "MalformedCertificateAuthorityException"
        );
        handle(
            &req("ImportCertificateAuthorityCertificate"),
            b,
            &json!({"CertificateAuthorityArn": &arn, "Certificate": super::b64(CERT_PEM)}),
        );
        let desc = handle(
            &req("DescribeCertificateAuthority"),
            b,
            &json!({"CertificateAuthorityArn": &arn}),
        );
        assert_eq!(parsed(desc)["CertificateAuthority"]["Status"], "ACTIVE");

        // Revoke on an ACTIVE CA is a no-op success.
        let resp = handle(
            &req("RevokeCertificate"),
            b,
            &json!({"CertificateAuthorityArn": &arn, "CertificateSerial": "42", "RevocationReason": "KEY_COMPROMISE"}),
        );
        assert_eq!(resp.status, 200);
    }
}
