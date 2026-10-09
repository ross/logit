//! Rebuilds a zero-copy [`Bytes`] from a borrowed slice of the buffer it came from.
//!
//! A zero-copy decoder parses a `&str` or `&[u8]` view of the `Bytes` it was handed, then hands
//! out each field as a `Value::Str` slice of that same buffer rather than a copy
//! (`docs/design/data-model.md`'s "`bytes::Bytes` everywhere strings and blobs appear"). [`share`]
//! turns the borrowed view back into a `Bytes` sharing `base`'s allocation, and the one range
//! check here is the only place that offset arithmetic lives.
//!
//! The contract: if `sub` lies inside `base`, [`share`] returns `base.slice(..)` over the same
//! bytes; otherwise it copies `sub`. A copy is never wrong, only slower, so a caller that passes
//! an unescaped or otherwise rebuilt slice gets correct bytes and no panic. That's why this isn't
//! `Bytes::slice_ref`, which panics on a slice outside `base` and would take a node down over one
//! input. [`within`] is the same check as a predicate, for tests that pin a field as zero-copy.
//!
//! An empty `sub` counts as inside when its pointer is anywhere from `base`'s start to one past
//! its end, so `&base[base.len()..]` shares. An empty slice anywhere else, such as a `""`
//! literal, falls outside and copies, which allocates nothing. Either way the result is empty.
//!
//! A slice of a different live allocation can't pass the check by accident: two live allocations
//! never overlap, so any non-empty `sub` whose bytes sit inside `base`'s address range borrows
//! from `base` itself.

use bytes::Bytes;

/// Whether `sub` lies inside `base`'s address range (module doc).
#[inline]
pub fn within(base: &[u8], sub: &[u8]) -> bool {
    offset(base, sub).is_some()
}

/// `sub` as a [`Bytes`] sharing `base`'s allocation, or a copy of it when `sub` lies outside
/// `base` (module doc).
#[inline]
pub fn share(base: &Bytes, sub: &[u8]) -> Bytes {
    match offset(base, sub) {
        Some(off) => base.slice(off..off + sub.len()),
        None => copy(sub),
    }
}

/// `sub`'s byte offset into `base`, when it lies inside it. `wrapping_sub` turns a `sub` that
/// starts before `base` into a huge offset that fails the bound, so the check can't overflow.
#[inline]
fn offset(base: &[u8], sub: &[u8]) -> Option<usize> {
    let off = (sub.as_ptr() as usize).wrapping_sub(base.as_ptr() as usize);
    (sub.len() <= base.len() && off <= base.len() - sub.len()).then_some(off)
}

#[cold]
#[inline(never)]
fn copy(sub: &[u8]) -> Bytes {
    Bytes::copy_from_slice(sub)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn base() -> Bytes {
        Bytes::from_static(b"hello, world")
    }

    #[test]
    fn a_slice_inside_base_is_within_and_shares() {
        let base = base();
        let sub = &base[7..12];
        assert!(within(&base, sub));
        let shared = share(&base, sub);
        assert_eq!(shared, "world");
        assert_eq!(shared.as_ptr(), sub.as_ptr());
    }

    #[test]
    fn slices_at_either_end_are_within() {
        let base = base();
        assert!(within(&base, &base[..5]));
        assert!(within(&base, &base[7..]));
        assert_eq!(share(&base, &base[7..]).as_ptr(), base[7..].as_ptr());
    }

    #[test]
    fn the_whole_base_is_within() {
        let base = base();
        assert!(within(&base, &base[..]));
        assert_eq!(share(&base, &base[..]).as_ptr(), base.as_ptr());
    }

    #[test]
    fn empty_slices_at_the_start_and_end_are_within() {
        let base = base();
        assert!(within(&base, &base[..0]));
        assert!(within(&base, &base[base.len()..]));
        assert!(share(&base, &base[base.len()..]).is_empty());
    }

    #[test]
    fn an_empty_slice_two_past_the_end_is_outside() {
        let backing = *b"hello, world!";
        let base = &backing[..12];
        let past = &backing[13..];
        assert_eq!(past.as_ptr() as usize, base.as_ptr() as usize + base.len() + 1);
        assert!(!within(base, past));
    }

    #[test]
    fn a_slice_one_byte_past_the_end_is_outside() {
        let backing = *b"hello, world!";
        let base = &backing[..12];
        assert!(!within(base, &backing[8..13]));
        assert!(!within(base, &backing[12..13]));
    }

    #[test]
    fn a_disjoint_allocation_is_outside_and_copies() {
        let base = base();
        let other = Bytes::from(b"world".to_vec());
        assert!(!within(&base, &other));
        let copied = share(&base, &other);
        assert_eq!(copied, other);
        assert_ne!(copied.as_ptr(), other.as_ptr());
    }

    #[test]
    fn a_slice_starting_before_base_is_outside() {
        let backing = *b"hello, world";
        assert!(!within(&backing[3..], &backing[1..5]));
    }

    #[test]
    fn an_empty_literal_is_outside_and_copies_to_empty() {
        // A heap base, so a static `""` can't land at its end.
        let base = Bytes::from(b"hello, world".to_vec());
        assert!(!within(&base, b""));
        assert!(!within(&base, "".as_bytes()));
        assert!(share(&base, b"").is_empty());
    }
}
