//! The per-frame decode budget: a ceiling on the heap one payload may decode into, charged as
//! the payload is read, so a batch that would expand far past its wire size is rejected before
//! it is built.
//!
//! A frame's byte caps bound what arrives, not what it becomes: one empty event is 1 wire byte
//! and a `size_of::<Event>()` slot. `docs/design/wire-protocol.md`'s "Decode amplification"
//! section has the measured ratio per element and what each one is charged.
//!
//! The budget is a counter, never an allocation. A reader charges a list's element size times
//! its count once, after checking the count against the bytes left (every element costs at least
//! one wire byte), and a dictionary entry its string bytes plus a `Symbol`. A `Str`/`Bytes`
//! value is a slice of the frame's own buffer, so it's charged nothing.
//!
//! The rule and the 4x multiplier are decided in ADR `untrusted-input-bounds`.

use std::cell::Cell;

use crate::frame::MAX_SANE_UNCOMPRESSED_LEN;
use crate::CodecError;

/// How many heap bytes a frame may decode into per byte of the frame cap it arrived under.
pub const DECODE_BUDGET_PER_FRAME_BYTE: u64 = 4;

/// The budget for a frame under the default 64 MiB cap: 256 MiB. [`crate::native::NativeDecoder`]
/// uses it.
pub const DEFAULT_DECODE_BUDGET: u64 =
    DECODE_BUDGET_PER_FRAME_BYTE * MAX_SANE_UNCOMPRESSED_LEN as u64;

/// A payload's remaining decode budget. One per payload: [`crate::native::decode_batch`] and
/// [`crate::native::decode_batch_v2`] charge it as they read.
#[derive(Debug)]
pub struct DecodeBudget {
    limit: u64,
    remaining: Cell<u64>,
}

impl DecodeBudget {
    pub const fn new(limit: u64) -> Self {
        DecodeBudget { limit, remaining: Cell::new(limit) }
    }

    /// The budget for a frame that arrived under `max_frame_bytes` (clamped to
    /// [`MAX_SANE_UNCOMPRESSED_LEN`]): [`DECODE_BUDGET_PER_FRAME_BYTE`] times it.
    pub fn for_frame_cap(max_frame_bytes: u32) -> Self {
        let cap = max_frame_bytes.min(MAX_SANE_UNCOMPRESSED_LEN) as u64;
        DecodeBudget::new(DECODE_BUDGET_PER_FRAME_BYTE * cap)
    }

    /// No ceiling. For a payload this process encoded itself, such as a disk-spool record: the
    /// batch was already that size in memory before it was written.
    pub const fn unlimited() -> Self {
        DecodeBudget::new(u64::MAX)
    }

    pub fn limit(&self) -> u64 {
        self.limit
    }

    /// The bytes charged so far.
    pub fn charged(&self) -> u64 {
        self.limit - self.remaining.get()
    }

    /// Takes `bytes` from the budget, or fails with [`CodecError::BudgetExceeded`].
    pub(crate) fn charge(&self, bytes: u64) -> Result<(), CodecError> {
        let remaining = self.remaining.get();
        if bytes > remaining {
            return Err(self.exceeded());
        }
        self.remaining.set(remaining - bytes);
        Ok(())
    }

    /// Charges `count` elements of `size` bytes, after checking `count` against `available`
    /// wire bytes at `min_wire` bytes per element, so a count the payload can't hold is rejected
    /// as such before it is charged or reserved.
    pub(crate) fn charge_list(
        &self,
        what: &str,
        count: usize,
        min_wire: usize,
        available: usize,
        size: usize,
    ) -> Result<(), CodecError> {
        if count > available / min_wire {
            return Err(CodecError::Malformed(format!(
                "{what} declares {count} entries but only {available} bytes remain"
            )));
        }
        self.charge((count as u64).saturating_mul(size as u64))
    }

    #[cold]
    fn exceeded(&self) -> CodecError {
        CodecError::BudgetExceeded { limit: self.limit }
    }
}

impl Default for DecodeBudget {
    fn default() -> Self {
        DecodeBudget::new(DEFAULT_DECODE_BUDGET)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn charges_until_the_limit_then_names_it() {
        let budget = DecodeBudget::new(100);
        budget.charge(60).unwrap();
        budget.charge(40).unwrap();
        assert_eq!(budget.charged(), 100);
        match budget.charge(1) {
            Err(err @ CodecError::BudgetExceeded { limit: 100 }) => {
                assert!(err.to_string().contains("100-byte decode budget"))
            }
            other => panic!("expected BudgetExceeded, got {other:?}"),
        }
        assert_eq!(budget.charged(), 100, "a refused charge takes nothing");
    }

    #[test]
    fn a_list_count_the_bytes_cannot_hold_is_rejected_before_it_is_charged() {
        let budget = DecodeBudget::new(u64::MAX);
        assert!(budget.charge_list("list", 5, 2, 10, 8).is_ok());
        assert_eq!(budget.charged(), 40);
        match budget.charge_list("list", 6, 2, 10, 8) {
            Err(CodecError::Malformed(msg)) => assert!(msg.contains("declares 6 entries")),
            other => panic!("expected Malformed, got {other:?}"),
        }
        assert_eq!(budget.charged(), 40);
    }

    #[test]
    fn a_huge_count_saturates_instead_of_wrapping() {
        let budget = DecodeBudget::new(u64::MAX - 1);
        assert!(budget.charge_list("list", usize::MAX, 1, usize::MAX, 1 << 20).is_err());
    }

    #[test]
    fn the_frame_cap_budget_is_clamped_to_the_sanity_cap() {
        assert_eq!(DecodeBudget::for_frame_cap(1024).limit(), 4096);
        assert_eq!(DecodeBudget::for_frame_cap(u32::MAX).limit(), DEFAULT_DECODE_BUDGET);
    }
}
