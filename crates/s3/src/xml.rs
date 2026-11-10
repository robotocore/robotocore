//! S3 XML serialization and error formatting.

/// The S3 XML namespace.
pub const S3_NS: &str = "http://s3.amazonaws.com/doc/2006-03-01/";

/// moto's canned HostId (live-captured: identical across every error
/// envelope moto emits, base64 constant).
pub const MOTON_HOST_ID: &str =
    "9Gjjt1m+cjU4OPvX9O9/8RuvnG41MRb/18Oux2o5H5MY7ISNTlXN+Dz9IG62/ILVxhAGI0qyPfg=";

/// XML-escape one client-controlled value interpolated into an S3 XML
/// response (bucket/key/error-message names come off the request path —
/// `GET /bucket-with-%3Cscript%3E` would otherwise land raw in `<Message>`).
/// moto's writer escapes ONLY `&`, `<`, `>` (live-probed: raw `'` rides
/// through in `Value 'q'q'` quotes) — `&quot;`/`&apos;` are twin-tells.
pub fn xml_escape(text: &str) -> String {
    text.replace('&', "&amp;")
        .replace('<', "&lt;")
        .replace('>', "&gt;")
}

/// moto's per-envelope RequestId: 52 random [A-Za-z0-9] chars
/// (live-probed: fresh per request, 52 bytes, base62-ish alphabet).
/// FNV-1a mixing over the uuid words indexed by position: the naive
/// `b[i % 16]` cycle repeats every 16 chars (a wire-visible fingerprint
/// of the kind this campaign hunts — review #491 note 1).
pub fn fresh_request_id() -> String {
    const ALPHA: &[u8] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789";
    let bytes = uuid::Uuid::new_v4().as_bytes().to_owned();
    let mut hash: u32 = 0x811C9DC5;
    (0..52)
        .map(|i| {
            hash = hash.wrapping_mul(0x01000193)
                ^ (bytes[i % 16] as u32).wrapping_add(i as u32).rotate_left(3);
            ALPHA[(hash % (ALPHA.len() as u32)) as usize] as char
        })
        .collect()
}

/// Format an S3 error response as XML (moto shape: compact one-liner,
/// lowercase utf-8, RequestId + canned HostId appended).
pub fn error_response(code: &str, message: &str) -> String {
    format!(
        concat!(
            r#"<?xml version="1.0" encoding="utf-8"?>"#,
            "\n<Error><Code>{code}</Code><Message>{message}</Message>",
            "<RequestId>{request_id}</RequestId><HostId>{host_id}</HostId></Error>"
        ),
        code = xml_escape(code),
        message = xml_escape(message),
        request_id = fresh_request_id(),
        host_id = MOTON_HOST_ID,
    )
}

/// Format an S3 error response with additional fields.
pub fn error_response_full(code: &str, message: &str, resource: &str, request_id: &str) -> String {
    format!(
        concat!(
            r#"<?xml version="1.0" encoding="utf-8"?>"#,
            "\n<Error><Code>{code}</Code><Message>{message}</Message>",
            "<Resource>{resource}</Resource><RequestId>{request_id}</RequestId>",
            "<HostId>{host_id}</HostId></Error>"
        ),
        code = xml_escape(code),
        message = xml_escape(message),
        resource = xml_escape(resource),
        request_id = xml_escape(request_id),
        host_id = MOTON_HOST_ID,
    )
}

/// Build a CreateBucketResult XML.
pub fn create_bucket_result(bucket: &str) -> String {
    format!(
        r#"<?xml version="1.0" encoding="UTF-8"?>
<CreateBucketResult xmlns="{ns}">
  <Location>http://{bucket}.s3.amazonaws.com</Location>
  <Bucket>{bucket}</Bucket>
</CreateBucketResult>"#,
        ns = S3_NS,
        bucket = bucket,
    )
}

/// Build a ListBuckets XML response.
pub fn list_buckets(buckets: &[(String, u64)]) -> String {
    let mut xml = format!(
        r#"<?xml version="1.0" encoding="UTF-8"?>
<ListAllMyBucketsResult xmlns="{ns}">
  <Owner>
    <ID>robotocore</ID>
    <DisplayName>robotocore</DisplayName>
  </Owner>
  <Buckets>"#,
        ns = S3_NS,
    );
    for (name, created) in buckets {
        let dt = chrono::DateTime::from_timestamp(*created as i64, 0)
            .map(|d| d.to_rfc3339())
            .unwrap_or_default();
        xml.push_str(&format!(
            "\n    <Bucket>\n      <Name>{}</Name>\n      <CreationDate>{}</CreationDate>\n    </Bucket>",
            name, dt
        ));
    }
    xml.push_str("\n  </Buckets>\n</ListAllMyBucketsResult>");
    xml
}

/// Build a GetBucketLocation XML response.
pub fn get_bucket_location(region: &str) -> String {
    if region == "us-east-1" {
        format!(
            r#"<?xml version="1.0" encoding="UTF-8"?>
<LocationConstraint xmlns="{ns}"/>"#,
            ns = S3_NS
        )
    } else {
        format!(
            r#"<?xml version="1.0" encoding="UTF-8"?>
<LocationConstraint xmlns="{ns}">{region}</LocationConstraint>"#,
            ns = S3_NS,
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_error_response() {
        let xml = error_response("NoSuchBucket", "The specified bucket does not exist");
        assert!(xml.contains("NoSuchBucket"));
        assert!(xml.contains("The specified bucket does not exist"));
        // Real S3 error XML carries no xmlns attribute on <Error>
        assert!(!xml.contains(S3_NS));
        // moto shape (live-captured): compact one-liner, lowercase
        // encoding, per-envelope RequestId + the canned HostId.
        assert!(xml.starts_with("<?xml version=\"1.0\" encoding=\"utf-8\"?>\n<Error><Code>"));
        assert!(xml.contains("<RequestId>"));
        assert!(xml.contains(MOTON_HOST_ID));
        // compact: single body line between the declaration and the tail
        assert_eq!(xml.matches('\n').count(), 1);
        // the new escape convention: raw quote rides through
        let quoted = error_response("InvalidTag", "Value 'q'q' failed");
        assert!(quoted.contains("Value 'q'q' failed"));
        assert!(!quoted.contains("&apos;"));
        assert!(!quoted.contains("&quot;"));
    }

    #[test]
    fn test_list_buckets() {
        let xml = list_buckets(&[("my-bucket".to_string(), 1700000000)]);
        assert!(xml.contains("my-bucket"));
        assert!(xml.contains("ListAllMyBucketsResult"));
    }

    #[test]
    fn test_get_bucket_location_other() {
        let xml = get_bucket_location("eu-west-1");
        assert!(xml.contains("eu-west-1"));
    }
}
