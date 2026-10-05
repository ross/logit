//! The start of a stream through `proxy::parse`, as a `proxy_protocol: true` listener reads it.
//! Oracles: a complete header lies within the input and parses the same on its own bytes; every
//! proper prefix of a complete header is incomplete, never an error, since the stream reader
//! consumes the bytes of an incomplete result as header; a known total length is past the input
//! and within the v2 bound.
#![no_main]

use libfuzzer_sys::fuzz_target;
use logit_proto::proxy::{parse, Parse, MIN_LEN, V2_FIXED_LEN};

fuzz_target!(|data: &[u8]| {
    match parse(data) {
        Ok(Parse::Complete { origin, len }) => {
            assert!((MIN_LEN..=data.len()).contains(&len), "len {len} of {}", data.len());
            assert_eq!(
                parse(&data[..len]),
                Ok(Parse::Complete { origin, len }),
                "the header alone parses as it did with the payload behind it"
            );
            // The reader's growing buffers: a short prefix, and the header less its last byte.
            for end in [0, 1, len / 2, len - 1] {
                assert!(
                    matches!(parse(&data[..end]), Ok(Parse::Incomplete { .. })),
                    "prefix {end} of a {len}-byte header"
                );
            }
        }
        Ok(Parse::Incomplete { len }) => {
            if let Some(len) = len {
                assert!(len > data.len() && len <= V2_FIXED_LEN + usize::from(u16::MAX));
            }
            if let Some(shorter) = data.len().checked_sub(1) {
                assert!(matches!(parse(&data[..shorter]), Ok(Parse::Incomplete { .. })));
            }
        }
        Err(_) => {}
    }
});
