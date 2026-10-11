//! One row through `message::csv::split_row` and `unescape`, as the `csv` transform hands one
//! over. Byte 0 picks the delimiter from the 125 graph rule 32 admits (every ASCII byte but `"`,
//! `\n`, and `\r`), as `byte 0 % 125` into them in byte order; the rest is the row.
//!
//! Oracles, each over every input:
//! - offsets: every `(start, end)` lies in the row with `start <= end`, each field starts after
//!   the previous one ends, and `needs_unescape` is set only on a field holding `""`;
//! - reference: a state machine written here from RFC 4180 and the module doc's rules reads the
//!   same fields, unescaped, and fails with the same `RowError`;
//! - re-serialization: quoting every field (each `"` doubled) and joining on the delimiter
//!   splits back to the same fields;
//! - no quotes: a row with no `"` splits as `row.split(delimiter)`;
//! - UTF-8: every field of a valid UTF-8 row is valid UTF-8, the reasoning the transform's one
//!   whole-message check rests on, for every delimiter rule 32 admits.
#![no_main]

use bytes::Bytes;
use libfuzzer_sys::fuzz_target;
use logit_proto::message::csv::{split_row, unescape, RowError};

/// The delimiters graph rule 32 admits, in byte order.
fn delimiter(selector: u8) -> u8 {
    let allowed: Vec<u8> = (0u8..128).filter(|b| !matches!(b, b'"' | b'\n' | b'\r')).collect();
    allowed[selector as usize % allowed.len()]
}

/// The split, unescaped.
fn split(row: &Bytes, delim: u8) -> Result<Vec<Bytes>, RowError> {
    let mut offsets = Vec::new();
    split_row(row, delim, &mut offsets)?;
    Ok(fields(row, &offsets))
}

fn fields(row: &Bytes, offsets: &[(u32, u32, bool)]) -> Vec<Bytes> {
    offsets
        .iter()
        .map(|&(start, end, needs_unescape)| {
            let field = row.slice(start as usize..end as usize);
            if needs_unescape {
                unescape(&field)
            } else {
                field
            }
        })
        .collect()
}

/// The reference reading: one byte at a time through four states.
fn reference(row: &[u8], delim: u8) -> Result<Vec<Vec<u8>>, RowError> {
    enum State {
        /// At a field's first byte.
        Start,
        Unquoted,
        Quoted,
        /// After a `"` inside a quoted field: a second `"` is a literal, anything else closes it.
        QuoteInQuoted,
    }
    let mut out = Vec::new();
    let mut field = Vec::new();
    let mut state = State::Start;
    for &b in row {
        state = match state {
            State::Start if b == b'"' => State::Quoted,
            State::Start | State::Unquoted if b == delim => {
                out.push(std::mem::take(&mut field));
                State::Start
            }
            State::Start | State::Unquoted => {
                field.push(b);
                State::Unquoted
            }
            State::Quoted if b == b'"' => State::QuoteInQuoted,
            State::Quoted => {
                field.push(b);
                State::Quoted
            }
            State::QuoteInQuoted if b == b'"' => {
                field.push(b'"');
                State::Quoted
            }
            State::QuoteInQuoted if b == delim => {
                out.push(std::mem::take(&mut field));
                State::Start
            }
            State::QuoteInQuoted => return Err(RowError::TrailingAfterQuote),
        };
    }
    match state {
        State::Quoted => Err(RowError::UnterminatedQuote),
        _ => {
            out.push(field);
            Ok(out)
        }
    }
}

fuzz_target!(|data: &[u8]| {
    let Some((&selector, row)) = data.split_first() else { return };
    let delim = delimiter(selector);
    let row = Bytes::copy_from_slice(row);

    let mut offsets = Vec::new();
    let result = split_row(&row, delim, &mut offsets);
    let want = reference(&row, delim);
    let offsets = match (result, want) {
        (Ok(()), Ok(_)) => offsets,
        (Err(got), Err(want)) => {
            assert_eq!(got, want, "reference: a different error");
            return;
        }
        (got, want) => panic!("reference: the split says {got:?}, the reference {want:?}"),
    };
    let want = reference(&row, delim).unwrap();

    let mut previous_end = None;
    for &(start, end, needs_unescape) in &offsets {
        let (start, end) = (start as usize, end as usize);
        assert!(start <= end && end <= row.len(), "offsets: {start}..{end} in {}", row.len());
        if let Some(previous) = previous_end {
            assert!(start > previous, "offsets: a field at {start} after one ending at {previous}");
        }
        previous_end = Some(end);
        if needs_unescape {
            assert!(
                row[start..end].windows(2).any(|w| w == b"\"\""),
                "offsets: nothing to unescape"
            );
        }
    }

    let got = fields(&row, &offsets);
    assert_eq!(got.len(), want.len(), "reference: field count");
    for (i, (g, w)) in got.iter().zip(&want).enumerate() {
        assert_eq!(&g[..], &w[..], "reference: field {i}");
    }

    let mut quoted = Vec::new();
    for (i, field) in got.iter().enumerate() {
        if i > 0 {
            quoted.push(delim);
        }
        quoted.push(b'"');
        for &b in field.iter() {
            if b == b'"' {
                quoted.push(b'"');
            }
            quoted.push(b);
        }
        quoted.push(b'"');
    }
    let again = split(&Bytes::from(quoted), delim).expect("re-serialization: the quoted row fails");
    assert_eq!(again, got, "re-serialization: different fields");

    if !row.contains(&b'"') {
        let plain: Vec<&[u8]> = row.split(|&b| b == delim).collect();
        assert_eq!(got.iter().map(|f| &f[..]).collect::<Vec<_>>(), plain, "no quotes");
    }

    if std::str::from_utf8(&row).is_ok() {
        for (i, field) in got.iter().enumerate() {
            assert!(std::str::from_utf8(field).is_ok(), "UTF-8: field {i}, delimiter {delim:#04x}");
        }
    }
});
