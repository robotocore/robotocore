---
session: "a9ec4334"
timestamp: "2026-03-12T07:00:00Z"
model: claude-opus-4-6
---

## Human

Write tests that verify robotocore's error responses match AWS wire format exactly. Users switching from real AWS will break if our error XML/JSON doesn't match. Cover all five protocols (rest-xml, json, rest-json, query, ec2) with both boto3 ClientError checks and raw HTTP response structure validation.

## Assistant

## Key decisions

**Research-first approach**: Probed all error endpoints with raw HTTP before writing tests. This revealed the actual wire format for each protocol, including subtle differences like S3 bare `<Error>` vs query `<ErrorResponse><Error>` vs EC2 `<Response><Errors><Error>`.

**Dual validation strategy**: Each service gets both a boto3 ClientError test (verifying the SDK parses it correctly) and a raw HTTP test (verifying the XML/JSON body structure, root element names, and Content-Type headers). The raw tests catch wire format bugs that boto3 might paper over.

**x-robotocore-diag header**: Only appears on 500/501 errors from the error_normalizer and moto_bridge exception handlers. Moto's own error responses (NoSuchBucket, ResourceNotFoundException) pass through without it. The test uses an invalid action to trigger the error path rather than testing a Moto-generated error.

**Protocol-specific root elements**: Verified that S3 uses `<Error>` (bare), query services (SQS/IAM/SNS/CloudWatch) use `<ErrorResponse>`, and EC2 uses `<Response>` with an `<Errors>` wrapper. These distinctions matter because boto3 parses them differently.

**Tolerant assertions where AWS varies**: SecretsManager can return 400 or 404 for missing secrets. S3 HeadBucket can return 404 or 403. Tests accept both where AWS behavior allows variation.
