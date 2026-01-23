// Copyright 2026 zerotable.
// Use of this source code is governed by the Apache 2.0 license that can be
// found in the LICENSE file.

/// Separator byte between collection and document ID in storage keys.
const SEPARATOR: u8 = 0x00;

/// Encode a collection name and document ID into a storage key.
///
/// Key format: `{collection}\x00{doc_id}`
pub fn encode(collection: &str, doc_id: &str) -> Vec<u8> {
    let mut key = Vec::with_capacity(collection.len() + 1 + doc_id.len());
    key.extend_from_slice(collection.as_bytes());
    key.push(SEPARATOR);
    key.extend_from_slice(doc_id.as_bytes());
    key
}

/// Decode a storage key back into (collection, doc_id).
///
/// Returns `None` if the key has no separator or contains invalid UTF-8.
pub fn decode(key: &[u8]) -> Option<(&str, &str)> {
    let pos = key.iter().position(|&b| b == SEPARATOR)?;
    let collection = std::str::from_utf8(&key[..pos]).ok()?;
    let doc_id = std::str::from_utf8(&key[pos + 1..]).ok()?;
    Some((collection, doc_id))
}

/// Build a prefix for scanning all documents in a collection.
///
/// Use with `Keyspace::prefix()` to iterate over all documents in a collection.
pub fn collection_prefix(collection: &str) -> Vec<u8> {
    let mut prefix = Vec::with_capacity(collection.len() + 1);
    prefix.extend_from_slice(collection.as_bytes());
    prefix.push(SEPARATOR);
    prefix
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_encode_decode() {
        let key = encode("users", "abc123");
        let (collection, doc_id) = decode(&key).unwrap();
        assert_eq!(collection, "users");
        assert_eq!(doc_id, "abc123");
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
        let prefix = collection_prefix("users");
        let key = encode("users", "doc1");
        assert!(key.starts_with(&prefix));
    }

    // lsm trees store keys in lexicographical order!
    #[test]
    fn test_keys_are_ordered_by_collection() {
        let key_a = encode("aaa", "doc1");
        let key_b = encode("bbb", "doc1");
        assert!(key_a < key_b);
    }

    #[test]
    fn test_same_collection_ordered_by_doc_id() {
        let key_a = encode("users", "alice");
        let key_b = encode("users", "bob");
        assert!(key_a < key_b);
    }

    #[test]
    fn test_collection_isolation_with_similar_names() {
        let key_a = encode("users", "zzzz");
        let key_b = encode("users_backup", "aaaa");

        // Even though "zzzz" is "larger" than "aaaa",
        // the "users" collection must come first because of the 0x00 separator.
        assert!(key_a < key_b);
    }
}
