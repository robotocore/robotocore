//! S3 XML serialization and error formatting.

/// The S3 XML namespace.
pub const S3_NS: &str = "http://s3.amazonaws.com/doc/2006-03-01/";

/// Format an S3 error response as XML.
pub fn error_response(code: &str, message: &str) -> String {
    format!(
        r#"<?xml version="1.0" encoding="UTF-8"?>
<Error>
  <Code>{code}</Code>
  <Message>{message}</Message>
</Error>"#,
        code = code,
        message = message,
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
        code = code,
        message = message,
        resource = resource,
        request_id = request_id,
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

/// Build a ListObjectsV2 XML response.
pub fn list_objects_v2(
    bucket: &str,
    prefix: &str,
    marker: &str,
    max_keys: usize,
    is_truncated: bool,
    contents: &[(String, String, usize, u64, String)], // (key, etag, size, last_modified, storage_class)
    common_prefixes: &[String],
) -> String {
    let mut xml = format!(
        r#"<?xml version="1.0" encoding="UTF-8"?>
<ListBucketResult xmlns="{ns}">
  <Name>{bucket}</Name>
  <Prefix>{prefix}</Prefix>
  <Marker>{marker}</Marker>
  <MaxKeys>{max_keys}</MaxKeys>
  <IsTruncated>{is_truncated}</IsTruncated>"#,
        ns = S3_NS,
        bucket = bucket,
        prefix = prefix,
        marker = marker,
        max_keys = max_keys,
        is_truncated = is_truncated,
    );

    for (key, etag, size, last_modified, storage_class) in contents {
        let dt = chrono::DateTime::from_timestamp(*last_modified as i64, 0)
            .map(|d| d.to_rfc3339())
            .unwrap_or_default();
        xml.push_str(&format!(
            r#"
  <Contents>
    <Key>{}</Key>
    <LastModified>{}</LastModified>
    <ETag>"{}"</ETag>
    <Size>{}</Size>
    <StorageClass>{}</StorageClass>
  </Contents>"#,
            key, dt, etag, size, storage_class
        ));
    }

    for cp in common_prefixes {
        xml.push_str(&format!(
            "\n  <CommonPrefixes>\n    <Prefix>{}</Prefix>\n  </CommonPrefixes>",
            cp
        ));
    }

    xml.push_str("\n</ListBucketResult>");
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

/// Build a HeadObject response headers (no body).
pub fn head_object_headers(
    size: usize,
    etag: &str,
    content_type: &str,
    last_modified: u64,
) -> Vec<(String, String)> {
    let dt = chrono::DateTime::from_timestamp(last_modified as i64, 0)
        .map(|d| d.to_rfc3339())
        .unwrap_or_default();
    vec![
        ("Content-Length".to_string(), size.to_string()),
        ("ETag".to_string(), format!("\"{}\"", etag)),
        ("Content-Type".to_string(), content_type.to_string()),
        ("Last-Modified".to_string(), dt),
        (
            "x-amz-request-id".to_string(),
            uuid::Uuid::new_v4().to_string(),
        ),
    ]
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
    fn test_get_bucket_location_us_east_1() {
        let xml = get_bucket_location("us-east-1");
        assert!(xml.contains("LocationConstraint"));
        assert!(!xml.contains("us-east-1"));
    }

    #[test]
    fn test_get_bucket_location_other() {
        let xml = get_bucket_location("eu-west-1");
        assert!(xml.contains("eu-west-1"));
    }
}
