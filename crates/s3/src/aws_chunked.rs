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
//! S3 stores only the concatenated chunk data. Chunk signatures (and the
//! trailer signature) are not verified. A checksum trailer the request
//! declared through `x-amz-trailer` IS validated against the decoded data
//! (`400 InvalidRequest` on a mismatch), except for algorithms the twin
//! cannot compute locally (crc32c, crc64nvme). Recording the checksum on
//! the stored object is still pending: `ObjectVersion` has no checksum
//! slot yet (metadata plumbing + GetObject/HeadObject rendering).

use std::collections::HashMap;

use base64::Engine;
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

/// A decoded aws-chunked body: the object bytes plus the key/value pairs
/// that followed the final chunk.
pub type DecodedBody = (Vec<u8>, Vec<(String, String)>);

/// Returns the chunk data and trailer key/value pairs of an aws-chunked
/// body, or why the framing is invalid.
pub fn decode_with_trailers(body: &[u8]) -> Result<DecodedBody, &'static str> {
    let mut data = Vec::with_capacity(body.len());
    let mut trailers: Vec<(String, String)> = Vec::new();
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
            // The final chunk is followed by optional trailer lines and a
            // blank line; none of it is object data. Trailer values are
            // captured for checksum validation in decode_request_body.
            loop {
                let Some(line_end) = find_crlf(body, pos) else {
                    // A body that ends without a final CRLF/blank line may
                    // still carry one last (unterminated) trailer line.
                    if pos < body.len() {
                        let line = std::str::from_utf8(&body[pos..])
                            .map_err(|_| "a trailer line is not ASCII")?;
                        if let Some((name, value)) = line.split_once(':') {
                            trailers
                                .push((name.trim().to_ascii_lowercase(), value.trim().to_string()));
                        }
                    }
                    return Ok((data, trailers));
                };
                if line_end == pos {
                    return Ok((data, trailers));
                }
                let line = std::str::from_utf8(&body[pos..line_end])
                    .map_err(|_| "a trailer line is not ASCII")?;
                if let Some((name, value)) = line.split_once(':') {
                    trailers.push((name.trim().to_ascii_lowercase(), value.trim().to_string()));
                }
                pos = line_end + 2;
            }
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

/// Data-only view of [`decode_with_trailers`].
pub fn decode(body: &[u8]) -> Result<Vec<u8>, &'static str> {
    decode_with_trailers(body).map(|(data, _)| data)
}

/// Replaces an aws-chunked request body with its decoded data, as S3 does
/// before storing it. Leaves other requests untouched.
pub fn decode_request_body(req: &mut AwsRequest) -> Result<(), AwsResponse> {
    if req.body.is_empty() || !is_aws_chunked(&req.headers) {
        return Ok(());
    }
    let (data, trailers) = decode_with_trailers(&req.body).map_err(|reason| {
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
    // A checksum the request declared with x-amz-trailer describes the
    // stored data, so validate it against what was decoded (real S3
    // validates; moto instead skips the trailer region entirely —
    // deliberate divergence). A declared trailer that never arrives and
    // algorithms without a local implementation (crc32c, crc64nvme) pass
    // unverified, both recorded in the module docs.
    if let Some(declared) = req.headers.get("x-amz-trailer") {
        for name in declared.split(',').map(str::trim) {
            let Some(algorithm) = name.strip_prefix("x-amz-checksum-") else {
                continue;
            };
            let Some(value) = trailers.iter().find_map(|(trailer_name, value)| {
                (trailer_name.eq_ignore_ascii_case(name)).then(|| value.clone())
            }) else {
                continue;
            };
            if declared_checksum_matches(algorithm, &value, &data) == Some(false) {
                return Err(AwsResponse::error(
                    400,
                    "InvalidRequest",
                    &format!("Value for {name} header is invalid."),
                ));
            }
        }
    }
    strip_aws_chunked_encoding(&mut req.headers);
    req.headers
        .insert("content-length".to_string(), data.len().to_string());
    req.body = Bytes::from(data);
    Ok(())
}

/// True when a declared `x-amz-checksum-<algorithm>` trailer value matches
/// the decoded data; None for algorithms the twin cannot compute locally
/// (crc32c, crc64nvme — no local CRC multiplier), which then pass
/// unverified. Base64 values are padded exactly as SDKs send them.
fn declared_checksum_matches(algorithm: &str, value: &str, data: &[u8]) -> Option<bool> {
    let matches = match algorithm {
        "crc32" => {
            let crc = crc32fast::hash(data) as u32;
            base64::engine::general_purpose::STANDARD.encode(crc.to_le_bytes()) == value
        }
        "sha1" => {
            use sha1::Digest;
            let mut hash = sha1::Sha1::new();
            hash.update(data);
            base64::engine::general_purpose::STANDARD.encode(hash.finalize()) == value
        }
        "sha256" => {
            use sha2::Digest;
            let mut hash = sha2::Sha256::new();
            hash.update(data);
            base64::engine::general_purpose::STANDARD.encode(hash.finalize()) == value
        }
        _ => return None,
    };
    Some(matches)
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
    fn captures_trailers_after_the_final_chunk() {
        // crc32("hello world") = hRFKDQ== padded base64 (little-endian u32).
        let (_, trailers) =
            decode_with_trailers(b"b\r\nhello world\r\n0\r\nx-amz-checksum-crc32:hRFKDQ==\r\n\r\n")
                .unwrap();
        assert_eq!(
            trailers,
            vec![("x-amz-checksum-crc32".to_string(), "hRFKDQ==".to_string())]
        );
        // The 13-byte trailer-only tail without a final blank line still parses.
        let (_, trailers) =
            decode_with_trailers(b"b\r\nhello world\r\n0\r\nx-amz-trailer-signature:zzzz").unwrap();
        assert_eq!(
            trailers,
            vec![("x-amz-trailer-signature".to_string(), "zzzz".to_string())]
        );
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

    #[test]
    fn decode_request_body_accepts_a_matching_declared_checksum() {
        // crc32("hello world") == hRFKDQ== (little-endian base64).
        let mut req = request(
            &[
                ("x-amz-content-sha256", "STREAMING-UNSIGNED-PAYLOAD-TRAILER"),
                ("x-amz-trailer", "x-amz-checksum-crc32"),
            ],
            b"b\r\nhello world\r\n0\r\nx-amz-checksum-crc32:hRFKDQ==\r\n\r\n",
        );
        decode_request_body(&mut req).unwrap();
        assert_eq!(&req.body[..], b"hello world");

        let mut req = request(
            &[
                ("x-amz-content-sha256", "STREAMING-UNSIGNED-PAYLOAD-TRAILER"),
                ("x-amz-trailer", "x-amz-checksum-sha256"),
            ],
            b"b\r\nhello world\r\n0\r\nx-amz-checksum-sha256:uU0nuZNNPgilLlLX2n2r+sSE7+N6U4DukIj3rOLvzek=\r\n\r\n",
        );
        decode_request_body(&mut req).unwrap();
    }

    #[test]
    fn decode_request_body_rejects_a_mismatching_declared_checksum() {
        let mut req = request(
            &[
                ("x-amz-content-sha256", "STREAMING-UNSIGNED-PAYLOAD-TRAILER"),
                ("x-amz-trailer", "x-amz-checksum-crc32"),
            ],
            b"b\r\nhello world\r\n0\r\nx-amz-checksum-crc32:AAAAAAAAAA==\r\n\r\n",
        );
        let err = decode_request_body(&mut req).unwrap_err();
        assert_eq!(err.status, 400);
        let body = String::from_utf8_lossy(&err.body);
        assert!(body.contains("InvalidRequest"), "{body}");
        assert!(
            body.contains("Value for x-amz-checksum-crc32 header is invalid."),
            "{body}"
        );
    }

    #[test]
    fn decode_request_body_skips_an_unverifiable_algorithm_and_undeclared_trailers() {
        // crc32c has no local implementation: passes unverified (module docs).
        let mut req = request(
            &[
                ("x-amz-content-sha256", "STREAMING-UNSIGNED-PAYLOAD-TRAILER"),
                ("x-amz-trailer", "x-amz-checksum-crc32c"),
            ],
            b"b\r\nhello world\r\n0\r\nx-amz-checksum-crc32c:ESIzRA==\r\n\r\n",
        );
        decode_request_body(&mut req).unwrap();

        // A checksum trailer NOT declared via x-amz-trailer is not validated.
        let mut req = request(
            &[("x-amz-content-sha256", "STREAMING-UNSIGNED-PAYLOAD-TRAILER")],
            b"b\r\nhello world\r\n0\r\nx-amz-checksum-crc32:AAAAAAAAAA==\r\n\r\n",
        );
        decode_request_body(&mut req).unwrap();
        assert_eq!(&req.body[..], b"hello world");
    }
}
