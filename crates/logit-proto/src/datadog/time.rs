//! Datadog wire timestamps to and from `Event::timestamp` nanoseconds. Metrics, sketches, events,
//! and service checks carry Unix seconds (integers on the protobuf routes, doubles on the v1 JSON
//! ones); logs carry milliseconds. Saturating split arithmetic, the same idiom as
//! `crate::graphite`'s `resolve_timestamp`, because `secs * 1e9` in `f64` loses integer precision
//! past 2^53.

const NANOS_PER_SECOND: i64 = 1_000_000_000;
const NANOS_PER_MILLI: i64 = 1_000_000;

/// Whole seconds → ns, saturating.
pub fn seconds_to_nanos(seconds: i64) -> i64 {
    seconds.saturating_mul(NANOS_PER_SECOND)
}

/// Fractional seconds (a JSON double) → ns, `None` when not finite.
pub fn seconds_f64_to_nanos(seconds: f64) -> Option<i64> {
    if !seconds.is_finite() {
        return None;
    }
    let whole = seconds.trunc();
    let sub_nanos = ((seconds - whole) * 1e9).round() as i64;
    Some((whole as i64).saturating_mul(NANOS_PER_SECOND).saturating_add(sub_nanos))
}

/// Milliseconds → ns, saturating.
pub fn millis_to_nanos(millis: i64) -> i64 {
    millis.saturating_mul(NANOS_PER_MILLI)
}

/// ns → whole seconds, toward negative infinity.
pub fn nanos_to_seconds(nanos: i64) -> i64 {
    nanos.div_euclid(NANOS_PER_SECOND)
}

/// ns → whole milliseconds, toward negative infinity.
pub fn nanos_to_millis(nanos: i64) -> i64 {
    nanos.div_euclid(NANOS_PER_MILLI)
}

/// A wire timestamp that is absent or zero means "now" on every Datadog route; `received_at` is
/// what the decoder was handed for that.
pub fn or_received(nanos: Option<i64>, received_at: i64) -> i64 {
    match nanos {
        Some(n) if n > 0 => n,
        _ => received_at,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn seconds_and_millis_round_trip_through_nanos() {
        assert_eq!(nanos_to_seconds(seconds_to_nanos(1_700_000_000)), 1_700_000_000);
        assert_eq!(nanos_to_millis(millis_to_nanos(1_700_000_000_123)), 1_700_000_000_123);
        assert_eq!(seconds_f64_to_nanos(1475317847.5), Some(1_475_317_847_500_000_000));
        assert_eq!(seconds_f64_to_nanos(f64::NAN), None);
        assert_eq!(seconds_to_nanos(i64::MAX), i64::MAX);
        assert_eq!(or_received(Some(0), 7), 7);
        assert_eq!(or_received(None, 7), 7);
        assert_eq!(or_received(Some(5), 7), 5);
    }
}
