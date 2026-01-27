// Copyright 2026 zerotable.
// Use of this source code is governed by the Apache 2.0 license that can be
// found in the LICENSE file.

use std::fmt;

/// Maximum length (bytes) for collection IDs and document IDs.
// firestore-like limit. Fjall enforces 65536 bytes for keys.
const MAX_ID_LENGTH: usize = 1500;

/// Separator byte between collection ID and document ID in storage keys.
const SEPARATOR: u8 = 0x00;

/// Errors that can occur during key encoding.
#[derive(Debug, PartialEq)]
pub enum KeyError {
    EmptyId,
    ContainsNullByte,
    ContainsSlash, 
    TooLong { len: usize, max: usize },
}

impl fmt::Display for KeyError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            KeyError::EmptyId => write!(f, "id must not be empty"),
            KeyError::ContainsNullByte => write!(f, "id must not contain null bytes"),
            KeyError::ContainsSlash => write!(f, "id must not contain forward slashes"),
            KeyError::TooLong { len, max } => {
                write!(f, "id too long: {len} bytes, max {max}")
            }
        }
    }
}

impl std::error::Error for KeyError {}

/// Validate a collection ID or document ID.
fn validate(id: &str) -> Result<(), KeyError> {
    // NOTE: we check emptiness at grpc boundary too. Is this defense in depth
    //       redundant? Related to the uuid validation question below.   
    if id.is_empty() {
        return Err(KeyError::EmptyId);
    }
    if id.as_bytes().contains(&SEPARATOR) {
        return Err(KeyError::ContainsNullByte);
    }
    if id.contains('/') {
        return Err(KeyError::ContainsSlash);
    }
    if id.len() > MAX_ID_LENGTH {
        return Err(KeyError::TooLong {
            len: id.len(),
            max: MAX_ID_LENGTH,
        });
    }
    Ok(())
}

/// Encode a collection id and document ID into a storage key.
///
/// Key format: `{collection_id}\x00{doc_id}`
///
/// Returns an error if either id is empty, contains a null byte,
/// contains a forward slash, or exceeds the maximum length.
pub fn encode(collection_id: &str, doc_id: &str) -> Result<Vec<u8>, KeyError> {
    // NOTE: should we have separate validation rules for collection vs doc ids?
    // NOTE: should we skip validation for server generated uuids? 
    validate(collection_id)?;
    validate(doc_id)?;

    let mut key = Vec::with_capacity(collection_id.len() + 1 + doc_id.len());
    key.extend_from_slice(collection_id.as_bytes());
    key.push(SEPARATOR);
    key.extend_from_slice(doc_id.as_bytes());
    Ok(key)
}

/// Decode a storage key back into (collection_id, doc_id).
///
/// Returns `None` if the key has no separator or contains invalid UTF-8.
// NOTE: should this return Result instead of Option for better error info? 
pub fn decode(key: &[u8]) -> Option<(&str, &str)> {
    let pos = key.iter().position(|&b| b == SEPARATOR)?;
    let collection_id = std::str::from_utf8(&key[..pos]).ok()?;
    let doc_id = std::str::from_utf8(&key[pos + 1..]).ok()?;
    Some((collection_id, doc_id))
}

/// Build a prefix for scanning all documents in a collection.
///
/// Use with `Keyspace::prefix()` to iterate over all documents in a collection.
pub fn collection_prefix(collection_id: &str) -> Result<Vec<u8>, KeyError> {
    validate(collection_id)?;

    let mut prefix = Vec::with_capacity(collection_id.len() + 1);
    prefix.extend_from_slice(collection_id.as_bytes());
    prefix.push(SEPARATOR);
    Ok(prefix)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_encode_decode() {
        let key = encode("users", "abc123").unwrap();
        let (collection_id, doc_id) = decode(&key).unwrap();
        assert_eq!(collection_id, "users");
        assert_eq!(doc_id, "abc123");
    }

    #[test]
    fn test_encode_empty_collection_id() {
        assert_eq!(encode("", "doc1"), Err(KeyError::EmptyId));
    }

    #[test]
    fn test_encode_empty_doc_id() {
        assert_eq!(encode("users", ""), Err(KeyError::EmptyId));
    }

    #[test]
    fn test_encode_null_byte_in_collection_id() {
        assert_eq!(encode("us\x00ers", "doc1"), Err(KeyError::ContainsNullByte));
    }

    #[test]
    fn test_encode_null_byte_in_doc_id() {
        assert_eq!(encode("users", "doc\x001"), Err(KeyError::ContainsNullByte));
    }

    #[test]
    fn test_encode_too_long_collection_id() {
        let long_name = "a".repeat(1501);
        assert_eq!(
            encode(&long_name, "doc1"),
            Err(KeyError::TooLong { len: 1501, max: 1500 })
        );
    }

    #[test]
    fn test_encode_max_length_is_valid() {
        let max_name = "a".repeat(1500);
        assert!(encode(&max_name, "doc1").is_ok());
    }

    #[test]
    fn test_decode_no_separator() {
        assert_eq!(decode(b"noseparator"), None);
    }

    #[test]
    fn test_decode_invalid_utf8_collection() {
        let mut key = Vec::new();
        key.extend_from_slice(&[0xFF, 0xFE]); // invalid UTF-8
        key.push(SEPARATOR);
        key.extend_from_slice(b"doc1");
        assert_eq!(decode(&key), None);
    }

    #[test]
    fn test_decode_invalid_utf8_document() {
        let mut key = Vec::new();
        key.extend_from_slice(b"users");
        key.push(SEPARATOR);
        key.extend_from_slice(&[0xFF, 0xFE]); // invalid UTF-8
        assert_eq!(decode(&key), None);
    }

    #[test]
    fn test_collection_prefix() {
        let prefix = collection_prefix("users").unwrap();
        let key = encode("users", "doc1").unwrap();
        assert!(key.starts_with(&prefix));
    }

    // lsm trees store keys in lexicographical order!
    #[test]
    fn test_keys_are_ordered_by_collection() {
        let key_a = encode("aaa", "doc1").unwrap();
        let key_b = encode("bbb", "doc1").unwrap();
        assert!(key_a < key_b);
    }

    #[test]
    fn test_same_collection_ordered_by_doc_id() {
        let key_a = encode("users", "alice").unwrap();
        let key_b = encode("users", "bob").unwrap();
        assert!(key_a < key_b);
    }

    #[test]
    fn test_collection_isolation_with_similar_names() {
        let key_a = encode("users", "zzzz").unwrap();
        let key_b = encode("users_backup", "aaaa").unwrap();

        // Even though "zzzz" is "larger" than "aaaa",
        // the "users" collection must come first because of the 0x00 separator.
        assert!(key_a < key_b);
    }
}
