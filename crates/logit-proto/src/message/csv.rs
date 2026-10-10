//! The `csv` transform's parse core: one RFC 4180 row split into field offsets.
//!
//! The `csv` transform (`crates/logit-transforms/src/csv.rs`) wraps it with its column schema,
//! header-row and UTF-8 checks, field-count check, and diagnostics; the grammar is in
//! [ADR `csv-positional-columns`](../../../../docs/adr/csv-positional-columns.md). A row is one
//! message, so quoting never spans a record separator.

use bytes::Bytes;

/// Why a [`split_row`] call failed; the `csv` transform `Display`s it into its `parse_failure`
/// diagnostic.
#[derive(Debug, PartialEq, Eq)]
pub enum RowError {
    /// End of input inside a quoted field: a malformed row, or the first half of a record an
    /// embedded newline already split into two events.
    UnterminatedQuote,
    /// A closing `"` followed by something other than the delimiter or end of line (`"a"b,c`).
    TrailingAfterQuote,
}

impl std::fmt::Display for RowError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            RowError::UnterminatedQuote => write!(f, "unterminated quoted field"),
            RowError::TrailingAfterQuote => {
                write!(f, "unexpected content after a closing quote")
            }
        }
    }
}

/// Splits `line` on `delim` per RFC 4180 quoting, appending each field's `(start, end,
/// needs_unescape)` offsets to `out`.
///
/// `needs_unescape` marks a quoted field containing a doubled `""`, which needs [`unescape`]
/// rather than a slice. An empty `line` is one empty field. On `Err`, `out` may hold the fields
/// before the failure.
#[inline]
pub fn split_row(line: &Bytes, delim: u8, out: &mut Vec<(u32, u32, bool)>) -> Result<(), RowError> {
    let n = line.len();
    let mut i = 0usize;

    loop {
        let start;
        let end;
        let needs_unescape;
        let next;

        if line.get(i) == Some(&b'"') {
            // Quoted field. Everything until the closing quote is data, including `delim`.
            start = i + 1;
            let mut j = start;
            let mut esc = false;
            loop {
                if j >= n {
                    return Err(RowError::UnterminatedQuote);
                }
                if line[j] == b'"' {
                    if j + 1 < n && line[j + 1] == b'"' {
                        esc = true; // a doubled quote: one literal `"`, keep scanning
                        j += 2;
                        continue;
                    }
                    break; // j is the closing quote
                }
                j += 1;
            }
            end = j; // exclusive: the closing quote's own index
            needs_unescape = esc;
            next = j + 1; // index just past the closing quote
                          // A closing quote must be followed by the delimiter or end-of-line.
            if next < n && line[next] != delim {
                return Err(RowError::TrailingAfterQuote);
            }
        } else {
            // Unquoted field, to the next delimiter or end of line; a `"` inside it is data.
            start = i;
            let mut j = i;
            while j < n && line[j] != delim {
                j += 1;
            }
            end = j;
            needs_unescape = false;
            next = j;
        }

        out.push((start as u32, end as u32, needs_unescape));

        if next >= n {
            break; // end of line: that was the last field
        }
        debug_assert_eq!(line[next], delim);
        i = next + 1;
        if i == n {
            // a trailing delimiter means a final empty field
            out.push((n as u32, n as u32, false));
            break;
        }
    }
    Ok(())
}

/// Collapses each doubled `""` to one `"`: the only path in the `csv` transform that allocates.
///
/// It allocates once because the first pass sizes the `Vec` exactly: `Bytes::from(Vec<u8>)`
/// avoids a second allocation only when length equals capacity, and `field.len()` would
/// overestimate.
#[inline]
pub fn unescape(field: &Bytes) -> Bytes {
    let mut out_len = 0;
    let mut i = 0;
    while i < field.len() {
        if field[i] == b'"' && i + 1 < field.len() && field[i + 1] == b'"' {
            i += 2;
        } else {
            i += 1;
        }
        out_len += 1;
    }

    let mut out = Vec::with_capacity(out_len);
    let mut i = 0;
    while i < field.len() {
        if field[i] == b'"' && i + 1 < field.len() && field[i + 1] == b'"' {
            out.push(b'"');
            i += 2;
        } else {
            out.push(field[i]);
            i += 1;
        }
    }
    debug_assert_eq!(out.len(), out.capacity());
    Bytes::from(out)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn split(input: &str, delim: u8) -> Result<Vec<String>, RowError> {
        let bytes = Bytes::copy_from_slice(input.as_bytes());
        let mut out = Vec::new();
        split_row(&bytes, delim, &mut out)?;
        Ok(out
            .into_iter()
            .map(|(start, end, needs_unescape)| {
                let field = bytes.slice(start as usize..end as usize);
                let field = if needs_unescape { unescape(&field) } else { field };
                String::from_utf8(field.to_vec()).unwrap()
            })
            .collect())
    }

    #[test]
    fn split_row_worked_examples() {
        assert_eq!(split("a,b,c", b',').unwrap(), vec!["a", "b", "c"]);
        assert_eq!(split("a,,c", b',').unwrap(), vec!["a", "", "c"]);
        assert_eq!(split("a,b,", b',').unwrap(), vec!["a", "b", ""]);
        assert_eq!(split(r#""a,b",c"#, b',').unwrap(), vec!["a,b", "c"]);
        assert_eq!(split(r#""a""b",c"#, b',').unwrap(), vec!["a\"b", "c"]);
        assert_eq!(split(r#"""#, b',').unwrap_err(), RowError::UnterminatedQuote);
        assert_eq!(split(r#""",a"#, b',').unwrap(), vec!["", "a"]);
        assert_eq!(split(r#"he said "hi",b"#, b',').unwrap(), vec!["he said \"hi\"", "b"]);
        assert_eq!(split(r#""a"b,c"#, b',').unwrap_err(), RowError::TrailingAfterQuote);
        assert_eq!(split(r#"a,"b"#, b',').unwrap_err(), RowError::UnterminatedQuote);
    }

    #[test]
    fn split_row_single_unterminated_quote() {
        assert_eq!(split("\"", b',').unwrap_err(), RowError::UnterminatedQuote);
    }

    #[test]
    fn split_row_of_an_empty_line_is_one_empty_field() {
        assert_eq!(split("", b',').unwrap(), vec![""]);
    }
}
