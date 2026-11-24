//! Times as the stores keep them: epoch milliseconds. One home for the conversions the core,
//! the store add-ons and the tests share.

use std::time::{Duration, SystemTime, UNIX_EPOCH};

/// `at` as epoch milliseconds (before 1970: negative).
pub fn epoch_ms(at: SystemTime) -> i64 {
    match at.duration_since(UNIX_EPOCH) {
        Ok(d) => i64::try_from(d.as_millis()).unwrap_or(i64::MAX),
        Err(e) => -i64::try_from(e.duration().as_millis()).unwrap_or(i64::MAX),
    }
}

/// The system clock as epoch milliseconds: where no injected [`Clock`](crate::saga::Clock)
/// applies.
pub fn now_ms() -> i64 {
    epoch_ms(SystemTime::now())
}

/// `at_ms` plus `by`, saturating.
pub fn after(at_ms: i64, by: Duration) -> i64 {
    at_ms.saturating_add(i64::try_from(by.as_millis()).unwrap_or(i64::MAX))
}

/// `at_ms` as an RFC 3339 UTC instant at millisecond precision (`2026-01-01T00:00:00.000Z`).
pub fn rfc3339(at_ms: i64) -> String {
    chrono::DateTime::<chrono::Utc>::from_timestamp_millis(at_ms)
        .map(|t| t.to_rfc3339_opts(chrono::SecondsFormat::Millis, true))
        .unwrap_or_else(|| at_ms.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn conversions() {
        let at = UNIX_EPOCH + Duration::from_millis(1_793_700_000_250);
        assert_eq!(epoch_ms(at), 1_793_700_000_250);
        assert_eq!(epoch_ms(UNIX_EPOCH - Duration::from_millis(5)), -5);
        assert_eq!(after(1_000, Duration::from_secs(2)), 3_000);
        assert_eq!(after(i64::MAX, Duration::from_secs(1)), i64::MAX);
        assert_eq!(rfc3339(1_793_700_000_250), "2026-11-03T10:00:00.250Z");
    }
}
