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

#[derive(Default)]
struct State {
    /// CA arn -> CA dict (moto's `to_json()`).
    cas: Vec<(String, Value)>,
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

fn unknown_op(op: &str) -> Response {
    let e = json!({
        "__type": "NotImplemented",
        "message": format!("The {op} action has not been implemented"),
    });
    Response::json(501, serde_json::to_vec(&e).unwrap_or_default())
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

    fn req(op: &str) -> Request {
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
}
