// Copyright 2026 zerotable.
// Use of this source code is governed by the Apache 2.0 license that can be
// found in the LICENSE file.

//! Document ID generation utilities using UUID v7.

use std::time::{Duration, SystemTime};
use uuid::Uuid;

/// Generate a new UUID v7 and extract its embedded timestamp.
///
/// Returns `(uuid, timestamp)` where timestamp is extracted from the UUID itself,
/// ensuring consistency between the ID and any create_time/update_time fields.
pub fn generate_uuid_v7() -> (Uuid, SystemTime) {
    let uuid = Uuid::now_v7();
    let timestamp = extract_timestamp(&uuid);
    (uuid, timestamp)
}

/// Extract the timestamp embedded in a UUID v7.
pub fn extract_timestamp(uuid: &Uuid) -> SystemTime {
    let (secs, nanos) = uuid
        .get_timestamp()
        .expect("UUID v7 always has a timestamp")
        .to_unix();
    SystemTime::UNIX_EPOCH + Duration::new(secs, nanos)
}

/// Get current time truncated to millisecond precision.
///
/// Ensures consistency with UUID v7 timestamps that are millisecond precision!
pub fn now_millis() -> SystemTime {
    let millis = SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64;
    SystemTime::UNIX_EPOCH + Duration::from_millis(millis)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_now_millis_truncation() {
        let ts = now_millis();
        let duration = ts.duration_since(SystemTime::UNIX_EPOCH).unwrap();

        assert_eq!(
            duration.subsec_nanos() % 1_000_000,
            0,
            "now_millis should have 0 nanoseconds remainder beyond the millisecond"
        );
    }

    #[test]
    fn test_extraction_consistency() {
        let (id, ts_from_tuple) = generate_uuid_v7();
        let ts_manual = extract_timestamp(&id);

        assert_eq!(
            ts_from_tuple, ts_manual,
            "The extracted timestamp must match the one returned by the generator"
        );
    }

    #[test]
    fn test_timestamp_sanity() {
        let before = now_millis();
        let (_, ts) = generate_uuid_v7();
        let after = now_millis();

        // The ID timestamp should be between our two 'now' checks
        // (or equal to them since we are using millisecond precision)
        assert!(
            ts >= before,
            "ID timestamp {:?} is earlier than 'before' {:?}",
            ts,
            before
        );
        assert!(
            ts <= after,
            "ID timestamp {:?} is later than 'after' {:?}",
            ts,
            after
        );
    }
}
