//! Ciphertext blob codec mirroring moto's KMS mock crypto (`moto/kms/utils.py`).
//!
//! Blob layout: a 36-byte NUL-padded key id, a 12-byte IV, a 16-byte GCM tag,
//! then the AES-256-GCM ciphertext. The encryption context is bound as AAD
//! (sorted key/value concatenation), so a ciphertext can only be decrypted
//! under its master key *and* its original encryption context. As moto
//! documents, this is not real KMS wire format — blobs are only meaningful
//! within this twin.

use aes_gcm::aead::{Aead, KeyInit, Payload};
use aes_gcm::Aes256Gcm;
use rand::Rng;
use serde_json::Value;

const KEY_ID_LEN: usize = 36;
const IV_LEN: usize = 12;
const TAG_LEN: usize = 16;
const HEADER_LEN: usize = KEY_ID_LEN + IV_LEN + TAG_LEN;

/// A parsed ciphertext blob.
pub(crate) struct Ciphertext {
    /// Master key id carried in the blob header (NUL padding trimmed).
    pub(crate) key_id: String,
    pub(crate) iv: [u8; IV_LEN],
    pub(crate) ciphertext: Vec<u8>,
    pub(crate) tag: [u8; TAG_LEN],
}

impl Ciphertext {
    /// Parse a blob; `None` when the header is truncated or the layout
    /// cannot be unpacked (moto raises `InvalidCiphertextException` for
    /// the same condition).
    pub(crate) fn parse(blob: &[u8]) -> Option<Self> {
        if blob.len() < HEADER_LEN {
            return None;
        }
        let (header, ciphertext) = blob.split_at(HEADER_LEN);
        let key_len = header[..KEY_ID_LEN]
            .iter()
            .rposition(|&b| b != 0)
            .map(|i| i + 1)
            .unwrap_or(0);
        let key_id = String::from_utf8(header[..key_len].to_vec()).ok()?;
        let mut iv = [0u8; IV_LEN];
        iv.copy_from_slice(&header[KEY_ID_LEN..KEY_ID_LEN + IV_LEN]);
        let mut tag = [0u8; TAG_LEN];
        tag.copy_from_slice(&header[KEY_ID_LEN + IV_LEN..]);
        Some(Self {
            key_id,
            iv,
            ciphertext: ciphertext.to_vec(),
            tag,
        })
    }
}

/// Sorted (key, value) pairs of an `EncryptionContext` map.
///
/// Non-string values are stringified the way JSON renders them; moto assumes
/// strings and would crash on anything else, so this stays strictly closer
/// to real KMS.
pub(crate) fn context_entries(context: Option<&Value>) -> Vec<(String, String)> {
    let mut entries: Vec<(String, String)> = context
        .and_then(|v| v.as_object())
        .map(|m| {
            m.iter()
                .map(|(k, v)| {
                    let val = match v {
                        Value::String(s) => s.clone(),
                        other => other.to_string(),
                    };
                    (k.clone(), val)
                })
                .collect()
        })
        .unwrap_or_default();
    entries.sort_by(|a, b| a.0.cmp(&b.0));
    entries
}

/// AAD bytes for an encryption context: sorted key/value concatenation
/// (moto's `_serialize_encryption_context`).
fn context_aad(entries: &[(String, String)]) -> Vec<u8> {
    let mut aad = Vec::new();
    for (key, value) in entries {
        aad.extend_from_slice(key.as_bytes());
        aad.extend_from_slice(value.as_bytes());
    }
    aad
}

fn pack_key_id(key_id: &str) -> [u8; KEY_ID_LEN] {
    let mut buf = [0u8; KEY_ID_LEN];
    let bytes = key_id.as_bytes();
    buf[..bytes.len().min(KEY_ID_LEN)].copy_from_slice(&bytes[..bytes.len().min(KEY_ID_LEN)]);
    buf
}

/// Encrypt `plaintext` under a master key; returns the serialized blob
/// (moto's `kms.utils.encrypt`, minus the key lookup which callers do).
pub(crate) fn encrypt(
    master_key: &[u8],
    key_id: &str,
    plaintext: &[u8],
    entries: &[(String, String)],
) -> Vec<u8> {
    let mut iv = [0u8; IV_LEN];
    rand::thread_rng().fill(&mut iv);
    let aead = Aes256Gcm::new_from_slice(master_key).expect("AES-256 master key length");
    let sealed = aead
        .encrypt(
            &iv.into(),
            Payload {
                msg: plaintext,
                aad: &context_aad(entries),
            },
        )
        .expect("AES-GCM seal cannot fail");
    // aead output is ciphertext || tag; the header carries the tag first.
    let (ciphertext, tag) = sealed.split_at(sealed.len() - TAG_LEN);
    let mut blob = Vec::with_capacity(HEADER_LEN + ciphertext.len());
    blob.extend_from_slice(&pack_key_id(key_id));
    blob.extend_from_slice(&iv);
    blob.extend_from_slice(tag);
    blob.extend_from_slice(ciphertext);
    blob
}

/// Decrypt a parsed blob under a master key and encryption context;
/// `None` on any cryptographic mismatch (moto raises
/// `InvalidCiphertextException` in every such case).
pub(crate) fn decrypt(
    master_key: &[u8],
    ct: &Ciphertext,
    entries: &[(String, String)],
) -> Option<Vec<u8>> {
    let aead = Aes256Gcm::new_from_slice(master_key).expect("AES-256 master key length");
    let mut sealed = Vec::with_capacity(ct.ciphertext.len() + TAG_LEN);
    sealed.extend_from_slice(&ct.ciphertext);
    sealed.extend_from_slice(&ct.tag);
    aead.decrypt(
        &ct.iv.into(),
        Payload {
            msg: sealed.as_slice(),
            aad: &context_aad(entries),
        },
    )
    .ok()
}

#[cfg(test)]
mod tests {
    use super::*;
    use base64::engine::general_purpose::STANDARD as B64;
    use base64::Engine as _;

    fn key_material() -> [u8; 32] {
        let mut k = [0u8; 32];
        rand::thread_rng().fill(&mut k);
        k
    }

    #[test]
    fn roundtrip_without_context() {
        let master = key_material();
        let blob = encrypt(
            &master,
            "somekey0-1111-2222-3333-444444444444",
            b"hello",
            &[],
        );
        let ct = Ciphertext::parse(&B64.decode(B64.encode(&blob)).unwrap()).unwrap();
        assert_eq!(ct.key_id, "somekey0-1111-2222-3333-444444444444");
        assert_eq!(decrypt(&master, &ct, &[]).unwrap(), b"hello");
    }

    #[test]
    fn blob_header_pads_and_trims_key_id() {
        let master = key_material();
        // 32-hex key id (32 chars) pads to the 36-byte header field.
        let blob = encrypt(&master, "a1b2c3d4e5f6a7b8c9d0e1f2a3b4c5d6", b"x", &[]);
        let ct = Ciphertext::parse(&blob).unwrap();
        assert_eq!(ct.key_id, "a1b2c3d4e5f6a7b8c9d0e1f2a3b4c5d6");
        // mrk- prefixed ids (36 chars) fill the field exactly.
        let blob = encrypt(&master, "mrk-a1b2c3d4e5f6a7b8c9d0e1f2a3b4c5d6", b"x", &[]);
        let ct = Ciphertext::parse(&blob).unwrap();
        assert_eq!(ct.key_id, "mrk-a1b2c3d4e5f6a7b8c9d0e1f2a3b4c5d6");
    }

    #[test]
    fn blob_matches_moto_layout() {
        let master = key_material();
        let blob = encrypt(
            &master,
            "somekey0-1111-2222-3333-444444444444",
            &[0u8; 5],
            &[],
        );
        // KEY_ID_LEN(36) + IV_LEN(12) + TAG_LEN(16) + plaintext
        assert_eq!(blob.len(), 36 + 12 + 16 + 5);
        // Header decodes as the moto struct: 36-byte key id, 12-byte IV,
        // 16-byte tag, then the ciphertext body.
        let ct = Ciphertext::parse(&blob).unwrap();
        assert_eq!(ct.iv.len(), 12);
        assert_eq!(ct.tag.len(), 16);
        assert_eq!(ct.ciphertext.len(), 5);
    }

    #[test]
    fn short_blob_is_unparseable() {
        let master = key_material();
        let blob = encrypt(&master, "somekey0-1111-2222-3333-444444444444", b"ab", &[]);
        assert!(Ciphertext::parse(&blob[..HEADER_LEN - 1]).is_none());
        assert!(Ciphertext::parse(&[]).is_none());
    }

    #[test]
    fn tampered_ciphertext_is_undecodable() {
        let master = key_material();
        let mut blob = encrypt(
            &master,
            "somekey0-1111-2222-3333-444444444444",
            b"hello",
            &[],
        );
        let last = blob.len() - 1;
        blob[last] ^= 0xff;
        let ct = Ciphertext::parse(&blob).unwrap();
        assert!(decrypt(&master, &ct, &[]).is_none());
    }

    #[test]
    fn other_master_key_is_undecodable() {
        let blob = encrypt(
            &key_material(),
            "somekey0-1111-2222-3333-444444444444",
            b"hello",
            &[],
        );
        let ct = Ciphertext::parse(&blob).unwrap();
        assert!(decrypt(&key_material(), &ct, &[]).is_none());
    }

    #[test]
    fn context_binds_matches_only() {
        let master = key_material();
        let ctx = vec![
            ("env".to_string(), "dev".to_string()),
            ("purpose".to_string(), "test".to_string()),
        ];
        let blob = encrypt(
            &master,
            "somekey0-1111-2222-3333-444444444444",
            b"hello",
            &ctx,
        );
        let ct = Ciphertext::parse(&blob).unwrap();
        assert_eq!(decrypt(&master, &ct, &ctx).unwrap(), b"hello");

        // Wrong value, subset, superset and empty context all fail (moto
        // answers InvalidCiphertextException on every one of these).
        let wrong_value = vec![("env".to_string(), "prod".to_string())];
        assert!(decrypt(&master, &ct, &wrong_value).is_none());
        let subset = vec![("env".to_string(), "dev".to_string())];
        assert!(decrypt(&master, &ct, &subset).is_none());
        assert!(decrypt(&master, &ct, &[]).is_none());
    }

    #[test]
    fn context_entries_sort_and_coerce() {
        let entries = context_entries(Some(&serde_json::json!({
            "b": "2",
            "a": "1",
            "n": 7
        })));
        assert_eq!(
            entries,
            vec![
                ("a".to_string(), "1".to_string()),
                ("b".to_string(), "2".to_string()),
                ("n".to_string(), "7".to_string()),
            ]
        );
        assert!(context_entries(None).is_empty());
        assert!(context_entries(Some(&serde_json::json!("scalar"))).is_empty());
    }
}
