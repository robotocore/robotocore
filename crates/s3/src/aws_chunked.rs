//! Decoding for `aws-chunked` request bodies.
//!
//! SDKs send streaming uploads (`PutObject`, `UploadPart`) with
//! `Content-Encoding: aws-chunked` and an `x-amz-content-sha256` of
//! `STREAMING-*`: signed chunks (`STREAMING-AWS4-HMAC-SHA256-PAYLOAD[-TRAILER]`)
//! or unsigned chunks with a checksum trailer (`STREAMING-UNSIGNED-PAYLOAD-TRAILER`,
//! the default for most SDKs since the 2025 checksum change). The body is
//!
//! ```text
//! <hex-size>[;chunk-signature=<sig>]\r\n<data>\r\n ... 0[;chunk-signature=<sig>]\r\n[<trailer>\r\n]*\r\n
//! ```
//!
//! S3 stores only the concatenated chunk data. Chunk signatures are not
//! verified, and trailers (checksums, the trailer signature) are skipped.

use std::collections::HashMap;

use bytes::Bytes;

use crate::protocol::{AwsRequest, AwsResponse};

/// True when the headers declare an aws-chunked body.
pub fn is_aws_chunked(headers: &HashMap<String, String>) -> bool {
    headers
        .get("x-amz-content-sha256")
        .is_some_and(|v| v.starts_with("STREAMING-"))
        || headers.get("content-encoding").is_some_and(|v| {
            v.split(',')
                .any(|e| e.trim().eq_ignore_ascii_case("aws-chunked"))
        })
}

/// Returns the chunk data of an aws-chunked body, or why the framing is invalid.
pub fn decode(body: &[u8]) -> Result<Vec<u8>, &'static str> {
    let mut data = Vec::with_capacity(body.len());
    let mut pos = 0;
    loop {
        let header_end = find_crlf(body, pos).ok_or("a chunk header has no CRLF terminator")?;
        let size_field = body[pos..header_end]
            .split(|&b| b == b';')
            .next()
            .unwrap_or_default();
        let size_field =
            std::str::from_utf8(size_field).map_err(|_| "a chunk size is not ASCII")?;
        let size = usize::from_str_radix(size_field.trim(), 16)
            .map_err(|_| "a chunk size is not hexadecimal")?;
        pos = header_end + 2;
        if size == 0 {
            // The final chunk is followed by optional trailer lines and a blank
            // line; none of it is object data.
            return Ok(data);
        }
        let data_end = pos
            .checked_add(size)
            .filter(|&end| end <= body.len())
            .ok_or("a chunk is shorter than its declared size")?;
        data.extend_from_slice(&body[pos..data_end]);
        if body.get(data_end..data_end + 2) != Some(b"\r\n".as_slice()) {
            return Err("chunk data is not followed by CRLF");
        }
        pos = data_end + 2;
    }
}

/// Replaces an aws-chunked request body with its decoded data, as S3 does
/// before storing it. Leaves other requests untouched.
pub fn decode_request_body(req: &mut AwsRequest) -> Result<(), AwsResponse> {
    if req.body.is_empty() || !is_aws_chunked(&req.headers) {
        return Ok(());
    }
    let data = decode(&req.body).map_err(|reason| {
        AwsResponse::error(
            400,
            "InvalidRequest",
            &format!("Invalid aws-chunked request body: {reason}"),
        )
    })?;
    if let Some(expected) = req.headers.get("x-amz-decoded-content-length") {
        if expected.trim().parse::<usize>().ok() != Some(data.len()) {
            return Err(AwsResponse::error(
                400,
                "IncompleteBody",
                "You did not provide the number of bytes specified by the x-amz-decoded-content-length HTTP header",
            ));
        }
    }
    strip_aws_chunked_encoding(&mut req.headers);
    req.headers
        .insert("content-length".to_string(), data.len().to_string());
    req.body = Bytes::from(data);
    Ok(())
}

/// Drops `aws-chunked` from Content-Encoding: it describes the transfer, not
/// the stored object.
fn strip_aws_chunked_encoding(headers: &mut HashMap<String, String>) {
    let Some(value) = headers.get("content-encoding") else {
        return;
    };
    let remaining: Vec<&str> = value
        .split(',')
        .map(str::trim)
        .filter(|e| !e.is_empty() && !e.eq_ignore_ascii_case("aws-chunked"))
        .collect();
    if remaining.is_empty() {
        headers.remove("content-encoding");
    } else {
        let remaining = remaining.join(", ");
        headers.insert("content-encoding".to_string(), remaining);
    }
}

fn find_crlf(body: &[u8], from: usize) -> Option<usize> {
    body.get(from..)?
        .windows(2)
        .position(|w| w == b"\r\n")
        .map(|i| from + i)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn request(headers: &[(&str, &str)], body: &[u8]) -> AwsRequest {
        AwsRequest {
            service: "s3".to_string(),
            operation: "PutObject".to_string(),
            account: 123456789012,
            region: "us-east-1".to_string(),
            bucket: Some("bucket".to_string()),
            key: Some("key".to_string()),
            query_params: HashMap::new(),
            headers: headers
                .iter()
                .map(|(k, v)| (k.to_string(), v.to_string()))
                .collect(),
            method: "PUT".to_string(),
            body: Bytes::copy_from_slice(body),
            params: serde_json::Value::Null,
        }
    }

    #[test]
    fn decodes_signed_chunks() {
        let body = b"5;chunk-signature=aaaa\r\nhello\r\n6;chunk-signature=bbbb\r\n world\r\n0;chunk-signature=cccc\r\n\r\n";
        assert_eq!(decode(body).unwrap(), b"hello world");
    }

    #[test]
    fn decodes_unsigned_chunks_with_a_checksum_trailer() {
        let body = b"b\r\nhello world\r\n0\r\nx-amz-checksum-crc32:DUoRhQ==\r\n\r\n";
        assert_eq!(decode(body).unwrap(), b"hello world");
    }

    #[test]
    fn decodes_signed_chunks_with_a_signed_trailer() {
        let body = b"b;chunk-signature=aaaa\r\nhello world\r\n0;chunk-signature=bbbb\r\nx-amz-checksum-crc32:DUoRhQ==\r\nx-amz-trailer-signature:cccc\r\n\r\n";
        assert_eq!(decode(body).unwrap(), b"hello world");
    }

    #[test]
    fn keeps_crlf_inside_chunk_data() {
        let body = b"4\r\na\r\nb\r\n0\r\n\r\n";
        assert_eq!(decode(body).unwrap(), b"a\r\nb");
    }

    #[test]
    fn decodes_an_empty_object() {
        assert_eq!(decode(b"0;chunk-signature=aaaa\r\n\r\n").unwrap(), b"");
    }

    #[test]
    fn rejects_a_chunk_shorter_than_its_size() {
        assert!(decode(b"10\r\nhello\r\n0\r\n\r\n").is_err());
    }

    #[test]
    fn rejects_a_non_hex_size() {
        assert!(decode(b"zz\r\nhello\r\n0\r\n\r\n").is_err());
    }

    #[test]
    fn rejects_chunk_data_without_a_trailing_crlf() {
        assert!(decode(b"5\r\nhelloX\r\n0\r\n\r\n").is_err());
    }

    #[test]
    fn detects_chunked_bodies_from_either_header() {
        let by_sha = request(
            &[("x-amz-content-sha256", "STREAMING-UNSIGNED-PAYLOAD-TRAILER")],
            b"",
        );
        let by_encoding = request(&[("content-encoding", "gzip, aws-chunked")], b"");
        let plain = request(&[("x-amz-content-sha256", "UNSIGNED-PAYLOAD")], b"");
        assert!(is_aws_chunked(&by_sha.headers));
        assert!(is_aws_chunked(&by_encoding.headers));
        assert!(!is_aws_chunked(&plain.headers));
    }

    #[test]
    fn decode_request_body_replaces_the_body_and_cleans_the_headers() {
        let mut req = request(
            &[
                ("x-amz-content-sha256", "STREAMING-AWS4-HMAC-SHA256-PAYLOAD"),
                ("content-encoding", "aws-chunked"),
                ("x-amz-decoded-content-length", "11"),
                ("content-length", "75"),
            ],
            b"b;chunk-signature=aaaa\r\nhello world\r\n0;chunk-signature=bbbb\r\n\r\n",
        );
        decode_request_body(&mut req).unwrap();
        assert_eq!(&req.body[..], b"hello world");
        assert_eq!(
            req.headers.get("content-length").map(String::as_str),
            Some("11")
        );
        assert!(!req.headers.contains_key("content-encoding"));
    }

    #[test]
    fn decode_request_body_keeps_other_content_encodings() {
        let mut req = request(
            &[("content-encoding", "gzip,aws-chunked")],
            b"b\r\nhello world\r\n0\r\n\r\n",
        );
        decode_request_body(&mut req).unwrap();
        assert_eq!(
            req.headers.get("content-encoding").map(String::as_str),
            Some("gzip")
        );
    }

    #[test]
    fn decode_request_body_rejects_a_wrong_decoded_length() {
        let mut req = request(
            &[
                ("x-amz-content-sha256", "STREAMING-UNSIGNED-PAYLOAD-TRAILER"),
                ("x-amz-decoded-content-length", "12"),
            ],
            b"b\r\nhello world\r\n0\r\n\r\n",
        );
        let err = decode_request_body(&mut req).unwrap_err();
        assert_eq!(err.status, 400);
        assert!(String::from_utf8_lossy(&err.body).contains("IncompleteBody"));
    }

    #[test]
    fn decode_request_body_leaves_plain_bodies_alone() {
        let mut req = request(&[("content-length", "5")], b"5\r\nhi");
        decode_request_body(&mut req).unwrap();
        assert_eq!(&req.body[..], b"5\r\nhi");
    }
}
