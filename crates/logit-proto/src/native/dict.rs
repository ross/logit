//! The wire dictionary: every interned `Symbol` a batch uses, written once, referenced everywhere
//! else by `u32` index. See `docs/design/wire-protocol.md`'s "dictionary-first batches".
//!
//! **A `Symbol` (`lasso::Spur`) is never written raw.** It's a process-local index into a
//! never-evicting global interner (`docs/design/data-model.md`) -- the same key string gets a
//! different `Spur` in two processes, or in the same process on a later run reading an old file.
//! [`DictBuilder`] resolves every `Symbol` to its string at encode time; [`Dict`] re-interns every
//! string at decode time. This is the one correctness rule the whole codec exists to uphold.

use std::collections::HashMap;

use bytes::{Bytes, BytesMut};
use logit_core::{interner, Symbol};

use crate::native::varint::{read_uvarint, write_uvarint};
use crate::CodecError;

/// Collects the distinct `Symbol`s a batch actually uses, in first-use order, and assigns each a
/// stable index -- the same batch encoded twice produces the same dictionary, which matters for
/// tests and for two batches sharing an otherwise-identical shape compressing similarly.
#[derive(Default)]
pub struct DictBuilder {
    symbols: Vec<Symbol>,
    index: HashMap<Symbol, u32>,
}

impl DictBuilder {
    /// Interns `sym` into this batch's dictionary, returning its index. Idempotent: interning the
    /// same `Symbol` twice returns the same index without growing the dictionary again -- the
    /// same repetition this whole mechanism exists to avoid paying for twice, once in the
    /// process-wide interner and again here.
    pub fn intern(&mut self, sym: Symbol) -> u32 {
        if let Some(&i) = self.index.get(&sym) {
            return i;
        }
        let i = self.symbols.len() as u32;
        self.symbols.push(sym);
        self.index.insert(sym, i);
        i
    }

    /// Writes the dictionary section: a count, then each string in index order (so the decoder's
    /// `Vec` position matches the index this builder handed out).
    pub fn write(&self, out: &mut BytesMut) {
        write_uvarint(out, self.symbols.len() as u64);
        for &sym in &self.symbols {
            let s = interner::resolve(sym);
            write_uvarint(out, s.len() as u64);
            out.extend_from_slice(s.as_bytes());
        }
    }
}

/// The decode-side dictionary: every string read back and re-interned once, up front, so every
/// later `Dict::get` is a plain index into an already-resolved `Vec` rather than a repeated
/// interner round trip.
pub struct Dict(Vec<Symbol>);

/// A dictionary this large would mean over a gigabyte of index storage alone -- almost certainly a
/// corrupt or hostile length field, not a real batch. Bounds the up-front `Vec::with_capacity`
/// below so a bad count can't be used to force a huge allocation before a single byte of the
/// dictionary's actual content has even been validated.
const MAX_SANE_DICT_ENTRIES: usize = 16 * 1024 * 1024;

impl Dict {
    pub fn read(bytes: &mut Bytes) -> Result<Self, CodecError> {
        let count = read_uvarint(bytes)? as usize;
        if count > MAX_SANE_DICT_ENTRIES {
            return Err(CodecError::Malformed(format!(
                "dictionary declares {count} entries, over the {MAX_SANE_DICT_ENTRIES} sanity cap"
            )));
        }
        let mut symbols = Vec::with_capacity(count);
        for _ in 0..count {
            let len = read_uvarint(bytes)? as usize;
            if bytes.len() < len {
                return Err(CodecError::Malformed(format!(
                    "dictionary entry declares {len} bytes but only {} remain",
                    bytes.len()
                )));
            }
            let raw = bytes.split_to(len);
            let s = std::str::from_utf8(&raw)
                .map_err(|e| CodecError::Malformed(format!("dictionary entry not utf-8: {e}")))?;
            symbols.push(interner::intern(s));
        }
        Ok(Dict(symbols))
    }

    pub fn get(&self, idx: u32) -> Result<Symbol, CodecError> {
        self.0.get(idx as usize).copied().ok_or_else(|| {
            CodecError::Malformed(format!(
                "dictionary index {idx} out of range ({} entries)",
                self.0.len()
            ))
        })
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

        let dict = Dict::read(&mut bytes).unwrap();
        assert!(bytes.is_empty());
        assert_eq!(dict.get(idx_host).unwrap(), host);
        assert_eq!(dict.get(idx_env).unwrap(), env);
    }

    #[test]
    fn get_rejects_an_out_of_range_index() {
        let dict = Dict(vec![interner::intern("dict_range_test")]);
        assert!(dict.get(0).is_ok());
        assert!(matches!(dict.get(1), Err(CodecError::Malformed(_))));
    }

    #[test]
    fn read_rejects_a_declared_length_longer_than_the_remaining_bytes() {
        let mut buf = BytesMut::new();
        write_uvarint(&mut buf, 1); // one entry
        write_uvarint(&mut buf, 1000); // claims 1000 bytes, but none follow
        let mut bytes = buf.freeze();
        assert!(matches!(Dict::read(&mut bytes), Err(CodecError::Malformed(_))));
    }

    #[test]
    fn read_rejects_non_utf8_entries() {
        let mut buf = BytesMut::new();
        write_uvarint(&mut buf, 1);
        write_uvarint(&mut buf, 2);
        buf.extend_from_slice(&[0xff, 0xfe]); // invalid utf-8
        let mut bytes = buf.freeze();
        assert!(matches!(Dict::read(&mut bytes), Err(CodecError::Malformed(_))));
    }
}
