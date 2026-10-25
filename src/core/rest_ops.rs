// Auto-generated REST operation tables from botocore specs.

pub fn lambda_rest_ops() -> &'static [(&'static str, &'static str, &'static str)] {
    &[
        (
            "POST",
            "/2015-03-31/functions/{FunctionName}/invocations",
            "Invoke",
        ),
        (
            "POST",
            "/2015-03-31/functions/{FunctionName}/invoke-async/",
            "InvokeAsync",
        ),
        (
            "GET",
            "/2015-03-31/functions/{FunctionName}/configuration",
            "GetFunctionConfiguration",
        ),
        (
            "PUT",
            "/2015-03-31/functions/{FunctionName}/configuration",
            "UpdateFunctionConfiguration",
        ),
        (
            "GET",
            "/2015-03-31/event-source-mappings/{UUID}",
            "GetEventSource",
        ),
        (
            "DELETE",
            "/2015-03-31/event-source-mappings/{UUID}",
            "RemoveEventSource",
        ),
        (
            "DELETE",
            "/2015-03-31/functions/{FunctionName}",
            "DeleteFunction",
        ),
        ("GET", "/2015-03-31/functions/{FunctionName}", "GetFunction"),
        (
            "PUT",
            "/2015-03-31/functions/{FunctionName}",
            "UploadFunction",
        ),
        (
            "POST",
            "/2015-03-31/event-source-mappings/",
            "AddEventSource",
        ),
        (
            "GET",
            "/2015-03-31/event-source-mappings/",
            "ListEventSources",
        ),
        ("GET", "/2015-03-31/functions/", "ListFunctions"),
    ]
}

#[cfg(test)]
mod tests {
    use super::*;

    /// `POST …/functions/{FunctionName}/invocations` names the `Invoke`
    /// operation through the gateway's spec-table resolver (the inline path
    /// resolver in crates/lambda also maps it; the table row keeps the audit
    /// and introspection naming aligned for requests resolved here).
    #[test]
    fn invoke_row_resolves_for_post_invocations() {
        let row = lambda_rest_ops().iter().find(|(m, u, _)| {
            *m == "POST" && *u == "/2015-03-31/functions/{FunctionName}/invocations"
        });
        assert_eq!(row.map(|r| r.2), Some("Invoke"));
    }
}

pub fn s3_rest_ops() -> &'static [(&'static str, &'static str, &'static str)] {
    &[
        (
            "POST",
            "/{Bucket}/{Key+}?select&select-type=2",
            "SelectObjectContent",
        ),
        (
            "PUT",
            "/{Bucket}?metadataInventoryTable",
            "UpdateBucketMetadataInventoryTableConfiguration",
        ),
        (
            "POST",
            "/{Bucket}?metadataConfiguration",
            "CreateBucketMetadataConfiguration",
        ),
        (
            "DELETE",
            "/{Bucket}?metadataConfiguration",
            "DeleteBucketMetadataConfiguration",
        ),
        (
            "GET",
            "/{Bucket}?metadataConfiguration",
            "GetBucketMetadataConfiguration",
        ),
        (
            "PUT",
            "/{Bucket}?metadataJournalTable",
            "UpdateBucketMetadataJournalTableConfiguration",
        ),
        (
            "DELETE",
            "/{Bucket}?intelligent-tiering",
            "DeleteBucketIntelligentTieringConfiguration",
        ),
        (
            "GET",
            "/{Bucket}?intelligent-tiering",
            "GetBucketIntelligentTieringConfiguration",
        ),
        (
            "GET",
            "/{Bucket}?intelligent-tiering",
            "ListBucketIntelligentTieringConfigurations",
        ),
        (
            "PUT",
            "/{Bucket}?intelligent-tiering",
            "PutBucketIntelligentTieringConfiguration",
        ),
        ("PUT", "/{Bucket}/{Key+}?renameObject", "RenameObject"),
        (
            "DELETE",
            "/{Bucket}?ownershipControls",
            "DeleteBucketOwnershipControls",
        ),
        (
            "DELETE",
            "/{Bucket}?publicAccessBlock",
            "DeletePublicAccessBlock",
        ),
        (
            "GET",
            "/{Bucket}?ownershipControls",
            "GetBucketOwnershipControls",
        ),
        ("GET", "/{Bucket}/{Key+}?attributes", "GetObjectAttributes"),
        ("GET", "/{Bucket}/{Key+}?legal-hold", "GetObjectLegalHold"),
        ("GET", "/{Bucket}?publicAccessBlock", "GetPublicAccessBlock"),
        (
            "PUT",
            "/{Bucket}?ownershipControls",
            "PutBucketOwnershipControls",
        ),
        ("PUT", "/{Bucket}/{Key+}?legal-hold", "PutObjectLegalHold"),
        ("PUT", "/{Bucket}?publicAccessBlock", "PutPublicAccessBlock"),
        (
            "PUT",
            "/{Bucket}/{Key+}?encryption",
            "UpdateObjectEncryption",
        ),
        ("GET", "/{Bucket}/{Key+}?retention", "GetObjectRetention"),
        ("PUT", "/{Bucket}/{Key+}?retention", "PutObjectRetention"),
        ("POST", "/{Bucket}/{Key+}?uploads", "CreateMultipartUpload"),
        ("DELETE", "/{Bucket}/{Key+}?tagging", "DeleteObjectTagging"),
        ("GET", "/{Bucket}?requestPayment", "GetBucketRequestPayment"),
        ("GET", "/{Bucket}/{Key+}?tagging", "GetObjectTagging"),
        ("GET", "/{Bucket}/{Key+}?torrent", "GetObjectTorrent"),
        ("PUT", "/{Bucket}?requestPayment", "PutBucketRequestPayment"),
        ("PUT", "/{Bucket}/{Key+}?tagging", "PutObjectTagging"),
        ("POST", "/{Bucket}/{Key+}?restore", "RestoreObject"),
        (
            "POST",
            "/{Bucket}?metadataTable",
            "CreateBucketMetadataTableConfiguration",
        ),
        (
            "DELETE",
            "/{Bucket}?metadataTable",
            "DeleteBucketMetadataTableConfiguration",
        ),
        (
            "GET",
            "/{Bucket}?metadataTable",
            "GetBucketMetadataTableConfiguration",
        ),
        ("POST", "/WriteGetObjectResponse", "WriteGetObjectResponse"),
        ("GET", "/{Bucket}?notification", "GetBucketNotification"),
        (
            "GET",
            "/{Bucket}?notification",
            "GetBucketNotificationConfiguration",
        ),
        ("GET", "/{Bucket}?policyStatus", "GetBucketPolicyStatus"),
        ("PUT", "/{Bucket}?notification", "PutBucketNotification"),
        (
            "PUT",
            "/{Bucket}?notification",
            "PutBucketNotificationConfiguration",
        ),
        ("DELETE", "/{Bucket}?replication", "DeleteBucketReplication"),
        ("GET", "/{Bucket}?replication", "GetBucketReplication"),
        ("GET", "/{Bucket}?object-lock", "GetObjectLockConfiguration"),
        ("GET", "/{Bucket}?list-type=2", "ListObjectsV2"),
        ("PUT", "/{Bucket}?replication", "PutBucketReplication"),
        ("PUT", "/{Bucket}?object-lock", "PutObjectLockConfiguration"),
        ("DELETE", "/{Bucket}?encryption", "DeleteBucketEncryption"),
        (
            "GET",
            "/{Bucket}?accelerate",
            "GetBucketAccelerateConfiguration",
        ),
        ("GET", "/{Bucket}?encryption", "GetBucketEncryption"),
        ("GET", "/{Bucket}?versioning", "GetBucketVersioning"),
        ("GET", "/{Bucket}/{Key+}?acl", "GetObjectAcl"),
        (
            "PUT",
            "/{Bucket}?accelerate",
            "PutBucketAccelerateConfiguration",
        ),
        ("PUT", "/{Bucket}?encryption", "PutBucketEncryption"),
        ("PUT", "/{Bucket}?versioning", "PutBucketVersioning"),
        ("PUT", "/{Bucket}/{Key+}?acl", "PutObjectAcl"),
        (
            "DELETE",
            "/{Bucket}?analytics",
            "DeleteBucketAnalyticsConfiguration",
        ),
        (
            "DELETE",
            "/{Bucket}?inventory",
            "DeleteBucketInventoryConfiguration",
        ),
        ("DELETE", "/{Bucket}?lifecycle", "DeleteBucketLifecycle"),
        (
            "GET",
            "/{Bucket}?analytics",
            "GetBucketAnalyticsConfiguration",
        ),
        (
            "GET",
            "/{Bucket}?inventory",
            "GetBucketInventoryConfiguration",
        ),
        ("GET", "/{Bucket}?lifecycle", "GetBucketLifecycle"),
        (
            "GET",
            "/{Bucket}?lifecycle",
            "GetBucketLifecycleConfiguration",
        ),
        (
            "GET",
            "/{Bucket}?analytics",
            "ListBucketAnalyticsConfigurations",
        ),
        (
            "GET",
            "/{Bucket}?inventory",
            "ListBucketInventoryConfigurations",
        ),
        (
            "PUT",
            "/{Bucket}?analytics",
            "PutBucketAnalyticsConfiguration",
        ),
        (
            "PUT",
            "/{Bucket}?inventory",
            "PutBucketInventoryConfiguration",
        ),
        ("PUT", "/{Bucket}?lifecycle", "PutBucketLifecycle"),
        (
            "PUT",
            "/{Bucket}?lifecycle",
            "PutBucketLifecycleConfiguration",
        ),
        ("GET", "/{Bucket}?location", "GetBucketLocation"),
        ("GET", "/{Bucket}?versions", "ListObjectVersions"),
        ("GET", "/{Bucket}?session", "CreateSession"),
        (
            "DELETE",
            "/{Bucket}?metrics",
            "DeleteBucketMetricsConfiguration",
        ),
        ("DELETE", "/{Bucket}?tagging", "DeleteBucketTagging"),
        ("DELETE", "/{Bucket}?website", "DeleteBucketWebsite"),
        ("GET", "/{Bucket}?logging", "GetBucketLogging"),
        ("GET", "/{Bucket}?metrics", "GetBucketMetricsConfiguration"),
        ("GET", "/{Bucket}?tagging", "GetBucketTagging"),
        ("GET", "/{Bucket}?website", "GetBucketWebsite"),
        (
            "GET",
            "/{Bucket}?metrics",
            "ListBucketMetricsConfigurations",
        ),
        ("GET", "/{Bucket}?uploads", "ListMultipartUploads"),
        ("PUT", "/{Bucket}?logging", "PutBucketLogging"),
        ("PUT", "/{Bucket}?metrics", "PutBucketMetricsConfiguration"),
        ("PUT", "/{Bucket}?tagging", "PutBucketTagging"),
        ("PUT", "/{Bucket}?website", "PutBucketWebsite"),
        ("DELETE", "/{Bucket}/{Key+}", "AbortMultipartUpload"),
        ("POST", "/{Bucket}/{Key+}", "CompleteMultipartUpload"),
        ("PUT", "/{Bucket}/{Key+}", "CopyObject"),
        ("DELETE", "/{Bucket}?policy", "DeleteBucketPolicy"),
        ("DELETE", "/{Bucket}/{Key+}", "DeleteObject"),
        ("POST", "/{Bucket}?delete", "DeleteObjects"),
        ("GET", "/{Bucket}?policy", "GetBucketPolicy"),
        ("GET", "/{Bucket}/{Key+}", "GetObject"),
        ("HEAD", "/{Bucket}/{Key+}", "HeadObject"),
        ("GET", "/{Bucket}/{Key+}", "ListParts"),
        ("PUT", "/{Bucket}?policy", "PutBucketPolicy"),
        ("PUT", "/{Bucket}/{Key+}", "PutObject"),
        ("PUT", "/{Bucket}/{Key+}", "UploadPart"),
        ("PUT", "/{Bucket}/{Key+}", "UploadPartCopy"),
        ("DELETE", "/{Bucket}?cors", "DeleteBucketCors"),
        ("GET", "/{Bucket}?abac", "GetBucketAbac"),
        ("GET", "/{Bucket}?cors", "GetBucketCors"),
        ("PUT", "/{Bucket}?abac", "PutBucketAbac"),
        ("PUT", "/{Bucket}?cors", "PutBucketCors"),
        ("GET", "/{Bucket}?acl", "GetBucketAcl"),
        ("PUT", "/{Bucket}?acl", "PutBucketAcl"),
        ("PUT", "/{Bucket}", "CreateBucket"),
        ("DELETE", "/{Bucket}", "DeleteBucket"),
        ("HEAD", "/{Bucket}", "HeadBucket"),
        ("GET", "/{Bucket}", "ListObjects"),
        ("GET", "/", "ListBuckets"),
        ("GET", "/", "ListDirectoryBuckets"),
    ]
}
