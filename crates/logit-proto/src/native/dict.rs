//! The wire dictionary: every interned `Symbol` a batch uses, written once as a string and
//! referenced by `u32` index. See `docs/design/wire-protocol.md`'s "Payload: dictionary-first
//! batches".
//!
//! **A `Symbol` (`lasso::Spur`) is never written raw.** It's a process-local index into the global
//! interner (`docs/design/data-model.md`): the same string gets a different `Spur` in another
//! process, or in a later run reading an old file. [`DictBuilder`] resolves every `Symbol` to its
//! string at encode time; [`Dict`] re-interns every string at decode time.
//!
//! [`Dict::read`] interns each string as it reads it, before the rest of the batch validates, so
//! a batch rejected later still leaves its strings in the never-evicting interner. That is a
//! documented non-goal: `docs/known-gaps.md`'s interner entry, "`logit_in`'s native dictionary".

use std::collections::HashMap;

use bytes::{Bytes, BytesMut};
use logit_core::{interner, Symbol};

use crate::native::budget::DecodeBudget;
use crate::native::varint::{read_uvarint, write_uvarint};
use crate::CodecError;

/// Collects the distinct `Symbol`s a batch uses, indexed in first-use order, so the same batch
/// always encodes to the same dictionary.
#[derive(Default)]
pub struct DictBuilder {
    symbols: Vec<Symbol>,
    index: HashMap<Symbol, u32>,
}

impl DictBuilder {
    /// Returns `sym`'s index, adding it on first use.
    pub fn intern(&mut self, sym: Symbol) -> u32 {
        if let Some(&i) = self.index.get(&sym) {
            return i;
        }
        let i = self.symbols.len() as u32;
        self.symbols.push(sym);
        self.index.insert(sym, i);
        i
    }

    /// Writes a count, then each string in index order, so a decoded `Vec` position is its index.
    pub fn write(&self, out: &mut BytesMut) {
        write_uvarint(out, self.symbols.len() as u64);
        for &sym in &self.symbols {
            let s = interner::resolve(sym);
            write_uvarint(out, s.len() as u64);
            out.extend_from_slice(s.as_bytes());
        }
    }
}

/// The decode-side dictionary. Every string is re-interned once, up front, so `Dict::get` is a
/// plain index.
///
/// It also carries the payload's [`DecodeBudget`]: every reader that allocates already takes the
/// dictionary.
pub struct Dict<'b> {
    symbols: Vec<Symbol>,
    budget: &'b DecodeBudget,
}

/// Rejects a declared entry count this large (~64 MiB of 4-byte `Symbol`s) as corrupt or hostile.
/// The initial `Vec::with_capacity` is separately clamped to 4096, so a count under the cap
/// still can't force a large allocation before any content is validated.
const MAX_SANE_DICT_ENTRIES: usize = 16 * 1024 * 1024;

impl<'b> Dict<'b> {
    /// Reads the dictionary, charging `budget` each entry's string bytes plus a `Symbol`.
    pub fn read(bytes: &mut Bytes, budget: &'b DecodeBudget) -> Result<Self, CodecError> {
        let count = read_uvarint(bytes)? as usize;
        if count > MAX_SANE_DICT_ENTRIES {
            return Err(CodecError::Malformed(format!(
                "dictionary declares {count} entries, over the {MAX_SANE_DICT_ENTRIES} sanity cap"
            )));
        }
        let mut symbols = Vec::with_capacity(count.min(4096));
        for _ in 0..count {
            let len = read_uvarint(bytes)? as usize;
            if bytes.len() < len {
                return Err(CodecError::Malformed(format!(
                    "dictionary entry declares {len} bytes but only {} remain",
                    bytes.len()
                )));
            }
            budget.charge((len + std::mem::size_of::<Symbol>()) as u64)?;
            let raw = bytes.split_to(len);
            let s = std::str::from_utf8(&raw)
                .map_err(|e| CodecError::Malformed(format!("dictionary entry not utf-8: {e}")))?;
            symbols.push(interner::intern(s));
        }
        Ok(Dict { symbols, budget })
    }

    pub fn get(&self, idx: u32) -> Result<Symbol, CodecError> {
        self.symbols.get(idx as usize).copied().ok_or_else(|| {
            CodecError::Malformed(format!(
                "dictionary index {idx} out of range ({} entries)",
                self.symbols.len()
            ))
        })
    }

    pub(crate) fn budget(&self) -> &'b DecodeBudget {
        self.budget
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn interning_the_same_symbol_twice_returns_the_same_index() {
        let mut builder = DictBuilder::default();
        let sym = interner::intern("host");
        let a = builder.intern(sym);
        let b = builder.intern(sym);
        assert_eq!(a, b);
        assert_eq!(a, 0);
        assert_eq!(builder.symbols.len(), 1, "the dictionary should hold only one entry");
    }

    #[test]
    fn distinct_symbols_get_distinct_indices_in_first_use_order() {
        let mut builder = DictBuilder::default();
        let a = builder.intern(interner::intern("dict_test_zzz_first"));
        let b = builder.intern(interner::intern("dict_test_zzz_second"));
        assert_eq!(a, 0);
        assert_eq!(b, 1);
    }

    #[test]
    fn round_trips_through_write_and_read() {
        let mut builder = DictBuilder::default();
        let host = interner::intern("dict_roundtrip_host");
        let env = interner::intern("dict_roundtrip_env");
        let idx_host = builder.intern(host);
        let idx_env = builder.intern(env);

        let mut buf = BytesMut::new();
        builder.write(&mut buf);
        let mut bytes = buf.freeze();

        let budget = DecodeBudget::unlimited();
        let dict = Dict::read(&mut bytes, &budget).unwrap();
        assert!(bytes.is_empty());
        assert_eq!(dict.get(idx_host).unwrap(), host);
        assert_eq!(dict.get(idx_env).unwrap(), env);
    }

    #[test]
    fn get_rejects_an_out_of_range_index() {
        let budget = DecodeBudget::unlimited();
        let dict = Dict { symbols: vec![interner::intern("dict_range_test")], budget: &budget };
        assert!(dict.get(0).is_ok());
        assert!(matches!(dict.get(1), Err(CodecError::Malformed(_))));
    }

    #[test]
    fn read_rejects_a_declared_length_longer_than_the_remaining_bytes() {
        let mut buf = BytesMut::new();
        write_uvarint(&mut buf, 1); // one entry
        write_uvarint(&mut buf, 1000); // claims 1000 bytes, but none follow
        let mut bytes = buf.freeze();
        assert!(matches!(
            Dict::read(&mut bytes, &DecodeBudget::unlimited()),
            Err(CodecError::Malformed(_))
        ));
    }

    #[test]
    fn read_rejects_non_utf8_entries() {
        let mut buf = BytesMut::new();
        write_uvarint(&mut buf, 1);
        write_uvarint(&mut buf, 2);
        buf.extend_from_slice(&[0xff, 0xfe]); // invalid utf-8
        let mut bytes = buf.freeze();
        assert!(matches!(
            Dict::read(&mut bytes, &DecodeBudget::unlimited()),
            Err(CodecError::Malformed(_))
        ));
    }

    #[test]
    fn read_rejects_a_count_over_the_sanity_cap() {
        let mut buf = BytesMut::new();
        write_uvarint(&mut buf, MAX_SANE_DICT_ENTRIES as u64 + 1);
        let mut bytes = buf.freeze();
        assert!(matches!(
            Dict::read(&mut bytes, &DecodeBudget::unlimited()),
            Err(CodecError::Malformed(_))
        ));
    }

    #[test]
    fn read_rejects_a_large_count_with_no_content_behind_it() {
        let mut buf = BytesMut::new();
        write_uvarint(&mut buf, 1_000_000);
        let mut bytes = buf.freeze();
        assert!(matches!(
            Dict::read(&mut bytes, &DecodeBudget::unlimited()),
            Err(CodecError::Malformed(_))
        ));
    }
}
