//! Amazon SimpleDB backend — port of the moto fork's `sdb` service
//! (botocore query protocol, SigV2 body auth).
//!
//! The moto surface (`moto/sdb/urls.py` + `responses.py`) is five
//! operations: CreateDomain, DeleteDomain, ListDomains, GetAttributes,
//! PutAttributes. The other five botocore operations — Select,
//! DomainMetadata, DeleteAttributes, BatchPutAttributes,
//! BatchDeleteAttributes — have no handler in the fork's responses class and
//! answer 501 the way moto does for a routed action without a method
//! (`moto/core/responses.py`: NotImplementedError → NotImplemented error
//! XML).
//!
//! Wire shapes mirror moto's serializer byte-for-byte: the sdb XML
//! namespace, no `<?xml?>` prologue, non-empty flattened lists serialize as
//! bare member elements while empty ones keep a self-closing wrapper
//! (`<DomainNames/>`), and error documents carry `<Type>Sender</Type>` plus
//! the fixed sdb `BoxUsage` field. Semantics mirror `moto/sdb/models.py`:
//! domain-name validation (`^[a-zA-Z0-9-_.]{3,}$`), DeleteDomain ignores
//! unknown domains, PutAttributes appends name/value pairs (dropping
//! existing values of the same name only when `Replace` is true), and
//! GetAttributes returns stored pairs in insertion order filtered by the
//! requested names.

use crate::store::{self, ScopeKey};
use crate::{Request, Response};
use once_cell::sync::Lazy;

pub const XMLNS: &str = "http://sdb.amazonaws.com/doc/2009-04-15/";
const BOX_USAGE: &str = "0.0055590278";

static SLOT: Lazy<store::Slot<SdbState>> = Lazy::new(|| store::slot::<SdbState>("sdb"));

#[derive(Default)]
struct SdbState {
    /// Insertion-ordered domains (moto's `self.domains` is a plain dict).
    domains: Vec<Domain>,
}

struct Domain {
    name: String,
    items: Vec<Item>,
}

struct Item {
    name: String,
    /// Stored name/value pairs, insertion order.
    attributes: Vec<StoredAttr>,
}

#[derive(Clone)]
struct StoredAttr {
    name: String,
    value: String,
}

pub fn dispatch(req: &Request) -> Option<Response> {
    match req.operation.as_str() {
        "CreateDomain" => Some(create_domain(req)),
        "DeleteDomain" => Some(delete_domain(req)),
        "ListDomains" => Some(list_domains(req)),
        "GetAttributes" => Some(get_attributes(req)),
        "PutAttributes" => Some(put_attributes(req)),
        // Routed service, but no handler in the moto fork: these raise moto's
        // NotImplementedError arm (`The {action} action has not been
        // implemented`).
        "DomainMetadata"
        | "Select"
        | "DeleteAttributes"
        | "BatchPutAttributes"
        | "BatchDeleteAttributes" => Some(not_implemented(req)),
        _ => Some(not_implemented(req)),
    }
}

fn scope_of(req: &Request) -> ScopeKey {
    (req.account, req.region.clone())
}

fn with_state<T>(req: &Request, f: impl FnOnce(&mut SdbState) -> T) -> T {
    let mut guard = SLOT.write();
    f(guard.entry(scope_of(req)).or_default())
}

// ---------------------------------------------------------------------------
// Operations
// ---------------------------------------------------------------------------

fn create_domain(req: &Request) -> Response {
    let params = form_params(req);
    let Some(domain_name) = first(&params, "DomainName") else {
        return missing_parameter("DomainName");
    };
    if let Err(resp) = validate_domain_name(&domain_name) {
        return resp;
    }
    with_state(req, |st| {
        // moto replaces any existing domain object wholesale.
        st.domains.retain(|d| d.name != domain_name);
        st.domains.push(Domain {
            name: domain_name,
            items: Vec::new(),
        });
    });
    empty_result("CreateDomain")
}

fn delete_domain(req: &Request) -> Response {
    let params = form_params(req);
    let Some(domain_name) = first(&params, "DomainName") else {
        return missing_parameter("DomainName");
    };
    if let Err(resp) = validate_domain_name(&domain_name) {
        return resp;
    }
    // moto ignores unknown domains — AWS does the same.
    with_state(req, |st| st.domains.retain(|d| d.name != domain_name));
    empty_result("DeleteDomain")
}

fn list_domains(req: &Request) -> Response {
    let names = with_state(req, |st| {
        st.domains
            .iter()
            .map(|d| d.name.clone())
            .collect::<Vec<_>>()
    });
    let body = match names.is_empty() {
        // moto serializes an empty flattened list as a self-closing wrapper.
        true => "<ListDomainsResult><DomainNames/></ListDomainsResult>".to_string(),
        false => {
            let mut body = String::from("<ListDomainsResult>");
            for name in &names {
                body.push_str(&format!(
                    "<DomainName>{}</DomainName>",
                    crate::proto::xml_escape(name)
                ));
            }
            body.push_str("</ListDomainsResult>");
            body
        }
    };
    wrap_result_xml("ListDomains", &body)
}

fn get_attributes(req: &Request) -> Response {
    let params = form_params(req);
    let Some(domain_name) = first(&params, "DomainName") else {
        return missing_parameter("DomainName");
    };
    if let Err(resp) = validate_domain_name(&domain_name) {
        return resp;
    }
    let Some(item_name) = first(&params, "ItemName") else {
        return missing_parameter("ItemName");
    };
    let attribute_names = indexed_strings(&params, "AttributeName");

    let attrs = with_state(req, |st| {
        let Some(domain) = st.domains.iter().find(|d| d.name == domain_name) else {
            return Err(unknown_domain());
        };
        let Some(item) = domain.items.iter().find(|i| i.name == item_name) else {
            // moto's defaultdict FakeItem: unknown items read as empty.
            return Ok(Vec::new());
        };
        Ok(match attribute_names.is_empty() {
            true => item.attributes.clone(),
            false => item
                .attributes
                .iter()
                .filter(|a| attribute_names.contains(&a.name))
                .cloned()
                .collect(),
        })
    });
    match attrs {
        Err(resp) => resp,
        Ok(attrs) => {
            let body = match attrs.is_empty() {
                true => "<GetAttributesResult><Attributes/></GetAttributesResult>".to_string(),
                false => {
                    let mut body = String::from("<GetAttributesResult>");
                    for attr in &attrs {
                        body.push_str(&format!(
                            "<Attribute><Name>{}</Name><Value>{}</Value></Attribute>",
                            crate::proto::xml_escape(&attr.name),
                            crate::proto::xml_escape(&attr.value),
                        ));
                    }
                    body.push_str("</GetAttributesResult>");
                    body
                }
            };
            wrap_result_xml("GetAttributes", &body)
        }
    }
}

fn put_attributes(req: &Request) -> Response {
    let params = form_params(req);
    let Some(domain_name) = first(&params, "DomainName") else {
        return missing_parameter("DomainName");
    };
    if let Err(resp) = validate_domain_name(&domain_name) {
        return resp;
    }
    let Some(item_name) = first(&params, "ItemName") else {
        return missing_parameter("ItemName");
    };
    let new_attrs = attribute_params(&params, "Attribute");
    with_state(req, |st| {
        let Some(domain) = st.domains.iter_mut().find(|d| d.name == domain_name) else {
            return unknown_domain();
        };
        let item_idx = match domain.items.iter().position(|i| i.name == item_name) {
            Some(idx) => idx,
            None => {
                domain.items.push(Item {
                    name: item_name.clone(),
                    attributes: Vec::new(),
                });
                domain.items.len() - 1
            }
        };
        let item = &mut domain.items[item_idx];
        for attr in new_attrs {
            // moto: Replace drops every stored pair of the same name, then
            // the new pair is appended.
            if attr.replace {
                item.attributes.retain(|a| a.name != attr.name);
            }
            item.attributes.push(StoredAttr {
                name: attr.name,
                value: attr.value,
            });
        }
        empty_result("PutAttributes")
    })
}

// ---------------------------------------------------------------------------
// Wire helpers
// ---------------------------------------------------------------------------

/// `<{Root}Response xmlns=...>{body}<ResponseMetadata>...` — moto's query
/// envelope: no XML prologue, metadata after the result body.
fn wrap_result_xml(root: &str, body: &str) -> Response {
    let xml = format!(
        r#"<{root}Response xmlns="{XMLNS}">{body}<ResponseMetadata><RequestId>{}</RequestId></ResponseMetadata></{root}Response>"#,
        uuid::Uuid::new_v4()
    );
    Response::xml(200, xml.into_bytes())
}

fn empty_result(root: &str) -> Response {
    wrap_result_xml(root, "")
}

/// moto's ServiceException rendering for sdb: Sender-type error with the
/// fixed BoxUsage field and the query-protocol `<RequestId>`.
fn sdb_error(status: u16, code: &str, message: &str) -> Response {
    let xml = format!(
        r#"<ErrorResponse xmlns="{XMLNS}"><Error><Type>Sender</Type><Code>{}</Code><Message>{}</Message><BoxUsage>{BOX_USAGE}</BoxUsage></Error><RequestId>{}</RequestId></ErrorResponse>"#,
        crate::proto::xml_escape(code),
        crate::proto::xml_escape(message),
        uuid::Uuid::new_v4()
    );
    Response::xml(status, xml.into_bytes())
}

fn unknown_domain() -> Response {
    sdb_error(400, "NoSuchDomain", "The specified domain does not exist.")
}

fn invalid_domain_name(name: &str) -> Response {
    sdb_error(
        400,
        "InvalidParameterValue",
        &format!("Value ({name}) for parameter DomainName is invalid. "),
    )
}

fn missing_parameter(name: &str) -> Response {
    sdb_error(
        400,
        "MissingParameter",
        &format!("The request must contain the parameter {name}"),
    )
}

/// Operations outside moto's sdb surface (`moto/core/responses.py`
/// NotImplementedError for a routed action with no method).
fn not_implemented(req: &Request) -> Response {
    sdb_error(
        501,
        "NotImplemented",
        &format!("The {} action has not been implemented", req.operation),
    )
}

// ---------------------------------------------------------------------------
// Form parsing (SigV2-signed query bodies: auth params ride in the body and
// are simply never read).
// ---------------------------------------------------------------------------

type Params = std::collections::BTreeMap<String, Vec<String>>;

fn form_params(req: &Request) -> Params {
    crate::proto::query_map(std::str::from_utf8(&req.body).unwrap_or(""))
}

fn first(params: &Params, key: &str) -> Option<String> {
    params.get(key).and_then(|v| v.first()).cloned()
}

/// Flattened form-list values `Key.1`, `Key.2`, ...
///
/// Note: botocore serializes flattened lists under the list *member's*
/// locationName on the wire (`AttributeName.1=...`), not the parent field
/// name (`AttributeNames`), so callers pass the member name.
fn indexed_strings(params: &Params, key: &str) -> Vec<String> {
    let mut out = Vec::new();
    for i in 1..10_000 {
        let Some(v) = first(params, &format!("{key}.{i}")) else {
            break;
        };
        out.push(v);
    }
    out
}

struct AttributeParam {
    name: String,
    value: String,
    replace: bool,
}

/// Struct attributes `Attributes.{i}.Name/.Value/.Replace`.
fn attribute_params(params: &Params, key: &str) -> Vec<AttributeParam> {
    let mut out = Vec::new();
    for i in 1..10_000 {
        let Some(name) = first(params, &format!("{key}.{i}.Name")) else {
            break;
        };
        let value = first(params, &format!("{key}.{i}.Value")).unwrap_or_default();
        let replace = first(params, &format!("{key}.{i}.Replace"))
            .map(|v| v.eq_ignore_ascii_case("true"))
            .unwrap_or(false);
        out.push(AttributeParam {
            name,
            value,
            replace,
        });
    }
    out
}

/// moto's domain-name rule: `^[a-zA-Z0-9-_.]{3,}$`.
fn validate_domain_name(name: &str) -> Result<(), Response> {
    let valid = name.len() >= 3
        && name
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || matches!(c, '_' | '-' | '.'));
    if valid {
        Ok(())
    } else {
        Err(invalid_domain_name(name))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

    /// Per-test dispatch harness: a unique region keeps the process-global
    /// state slots isolated between tests.
    struct Ops(String);

    impl Ops {
        fn new() -> Self {
            Ops(format!("sdb-test-{}", uuid::Uuid::new_v4()))
        }

        fn call(&self, operation: &str, form: &[(&str, &str)]) -> Response {
            let body = form
                .iter()
                .map(|(k, v)| format!("{k}={}", urlencoding::encode(v)))
                .collect::<Vec<_>>()
                .join("&");
            let req = Request {
                service: "sdb".to_string(),
                operation: operation.to_string(),
                method: "POST".to_string(),
                path: "/".to_string(),
                query_string: String::new(),
                headers: HashMap::from([
                    (
                        "content-type".to_string(),
                        "application/x-www-form-urlencoded".to_string(),
                    ),
                    ("host".to_string(), "localhost".to_string()),
                ]),
                body: body.into_bytes().into(),
                account: 123456789012,
                region: self.0.clone(),
            };
            dispatch(&req).unwrap()
        }
    }

    fn body_xml(resp: &Response) -> String {
        String::from_utf8(resp.body.clone()).unwrap()
    }

    #[test]
    fn create_list_delete_domains() {
        let ops = Ops::new();
        assert_eq!(
            ops.call("CreateDomain", &[("DomainName", "abc")]).status,
            200
        );

        let xml = body_xml(&ops.call("ListDomains", &[]));
        assert!(xml.contains("<DomainName>abc</DomainName>"), "{xml}");

        // moto: DeleteDomain ignores unknown domains and clears state.
        assert_eq!(
            ops.call("DeleteDomain", &[("DomainName", "gone")]).status,
            200
        );
        assert_eq!(
            ops.call("DeleteDomain", &[("DomainName", "abc")]).status,
            200
        );

        let xml = body_xml(&ops.call("ListDomains", &[]));
        assert!(
            xml.contains("<ListDomainsResult><DomainNames/></ListDomainsResult>"),
            "{xml}"
        );
    }

    #[test]
    fn invalid_domain_names() {
        let ops = Ops::new();
        for name in ["", "a", "ab", "bad!name"] {
            let resp = ops.call("CreateDomain", &[("DomainName", name)]);
            assert_eq!(resp.status, 400);
            assert!(
                body_xml(&resp).contains("<Code>InvalidParameterValue</Code>"),
                "{name}"
            );
        }
        for name in ["abc", "as-df", "jk_kl", "qw.rt"] {
            let resp = ops.call("CreateDomain", &[("DomainName", name)]);
            assert_eq!(resp.status, 200, "valid name {name}");
        }
    }

    #[test]
    fn put_get_attributes_with_replace() {
        let ops = Ops::new();
        assert_eq!(
            ops.call("CreateDomain", &[("DomainName", "put-get-dom")])
                .status,
            200
        );

        assert_eq!(
            ops.call(
                "PutAttributes",
                &[
                    ("DomainName", "put-get-dom"),
                    ("ItemName", "i1"),
                    ("Attribute.1.Name", "color"),
                    ("Attribute.1.Value", "red"),
                    ("Attribute.1.Replace", "true"),
                ]
            )
            .status,
            200
        );
        ops.call(
            "PutAttributes",
            &[
                ("DomainName", "put-get-dom"),
                ("ItemName", "i1"),
                ("Attribute.1.Name", "size"),
                ("Attribute.1.Value", "large"),
            ],
        );

        let got = body_xml(&ops.call(
            "GetAttributes",
            &[("DomainName", "put-get-dom"), ("ItemName", "i1")],
        ));
        assert!(
            got.contains("<Attribute><Name>color</Name><Value>red</Value></Attribute>")
                && got.contains("<Attribute><Name>size</Name><Value>large</Value></Attribute>"),
            "{got}"
        );
        assert!(!got.contains("Replace"), "{got}");

        // Replace removes prior values of the same name, then appends.
        ops.call(
            "PutAttributes",
            &[
                ("DomainName", "put-get-dom"),
                ("ItemName", "i1"),
                ("Attribute.1.Name", "size"),
                ("Attribute.1.Value", "small"),
                ("Attribute.1.Replace", "true"),
            ],
        );
        let got = body_xml(&ops.call(
            "GetAttributes",
            &[("DomainName", "put-get-dom"), ("ItemName", "i1")],
        ));
        assert!(
            got.contains("<Value>small</Value>") && !got.contains("<Value>large</Value>"),
            "{got}"
        );

        // Named filter returns exactly the color pair.
        let got = body_xml(&ops.call(
            "GetAttributes",
            &[
                ("DomainName", "put-get-dom"),
                ("ItemName", "i1"),
                ("AttributeName.1", "color"),
            ],
        ));
        assert!(
            got.contains("<Name>color</Name>") && !got.contains("<Name>size</Name>"),
            "{got}"
        );

        // Unknown domain.
        let gone = ops.call(
            "PutAttributes",
            &[
                ("DomainName", "no-such-domain-here"),
                ("ItemName", "i1"),
                ("Attribute.1.Name", "k"),
                ("Attribute.1.Value", "v"),
            ],
        );
        assert_eq!(gone.status, 400);
        assert!(
            body_xml(&gone).contains("<Code>NoSuchDomain</Code>"),
            "{}",
            body_xml(&gone)
        );
    }

    #[test]
    fn moto_absent_ops_answer_not_implemented() {
        let ops = Ops::new();
        for op in [
            "DomainMetadata",
            "Select",
            "DeleteAttributes",
            "BatchPutAttributes",
            "BatchDeleteAttributes",
        ] {
            let resp = ops.call(op, &[("DomainName", "d")]);
            assert_eq!(resp.status, 501, "{op}");
            let xml = body_xml(&resp);
            assert!(xml.contains("<Code>NotImplemented</Code>"), "{op}: {xml}");
            assert!(
                xml.contains(&format!("The {op} action has not been implemented")),
                "{op}: {xml}"
            );
        }
    }
}
