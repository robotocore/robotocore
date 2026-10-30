//! S3 XML serialization and error formatting.

/// The S3 XML namespace.
pub const S3_NS: &str = "http://s3.amazonaws.com/doc/2006-03-01/";

/// XML-escape one client-controlled value interpolated into an S3 XML
/// response (bucket/key/error-message names come off the request path —
/// `GET /bucket-with-%3Cscript%3E` would otherwise land raw in `<Message>`).
pub fn xml_escape(text: &str) -> String {
    text.replace('&', "&amp;")
        .replace('<', "&lt;")
        .replace('>', "&gt;")
        .replace('"', "&quot;")
        .replace('\'', "&apos;")
}

/// Format an S3 error response as XML.
pub fn error_response(code: &str, message: &str) -> String {
    format!(
        r#"<?xml version="1.0" encoding="UTF-8"?>
<Error>
  <Code>{code}</Code>
  <Message>{message}</Message>
</Error>"#,
        code = xml_escape(code),
        message = xml_escape(message),
    )
}

/// Format an S3 error response with additional fields.
pub fn error_response_full(code: &str, message: &str, resource: &str, request_id: &str) -> String {
    format!(
        r#"<?xml version="1.0" encoding="UTF-8"?>
<Error>
  <Code>{code}</Code>
  <Message>{message}</Message>
  <Resource>{resource}</Resource>
  <RequestId>{request_id}</RequestId>
</Error>"#,
        code = xml_escape(code),
        message = xml_escape(message),
        resource = xml_escape(resource),
        request_id = xml_escape(request_id),
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
