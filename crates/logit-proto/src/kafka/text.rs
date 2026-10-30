//! The line form of a `kafka_out` `format: text` record under `record: batch`, which `kafka_in`
//! reads back (`docs/adr/kafka-input.md`, "11. Formats").
//!
//! Each body is escaped bytewise: `\` (0x5C) becomes `\\` and LF (0x0A) becomes `\n`, the
//! backslash and the letter `n`. Every other byte, invalid UTF-8 included, stays as it is, so a
//! UTF-8 body stays valid UTF-8. The escaped bodies are joined by LF with none after the last.
//!
//! Reading splits the value on LF and unescapes each line. A `\` followed by any byte other than
//! `\` or `n`, and a `\` that ends a line, are kept as they are, both bytes or the one. An empty
//! value is one empty body, because `kafka_out` writes an empty batch record only for a run of
//! one empty body.

/// Appends `body` to `out` in the escaped form.
pub fn escape_line_into(out: &mut Vec<u8>, body: &[u8]) {
    let mut rest = body;
    while let Some(i) = rest.iter().position(|&b| b == b'\\' || b == b'\n') {
        out.extend_from_slice(&rest[..i]);
        out.extend_from_slice(if rest[i] == b'\\' { b"\\\\" } else { b"\\n" });
        rest = &rest[i + 1..];
    }
    out.extend_from_slice(rest);
}

/// The length [`escape_line_into`] writes for `body`.
pub fn escaped_len(body: &[u8]) -> usize {
    body.len() + body.iter().filter(|&&b| b == b'\\' || b == b'\n').count()
}

/// The bodies of a batch record's `value`, in order, each unescaped.
pub fn unescape_lines(value: &[u8]) -> Vec<Vec<u8>> {
    value.split(|&b| b == b'\n').map(unescape_line).collect()
}

/// One line's body: `\\` as `\` and `\n` as LF, any other byte as it is.
pub fn unescape_line(line: &[u8]) -> Vec<u8> {
    let mut out = Vec::with_capacity(line.len());
    let mut bytes = line.iter().copied();
    while let Some(b) = bytes.next() {
        if b != b'\\' {
            out.push(b);
            continue;
        }
        match bytes.next() {
            Some(b'\\') => out.push(b'\\'),
            Some(b'n') => out.push(b'\n'),
            Some(other) => out.extend_from_slice(&[b'\\', other]),
            None => out.push(b'\\'),
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    fn join(bodies: &[&[u8]]) -> Vec<u8> {
        let mut out = Vec::new();
        for (i, body) in bodies.iter().enumerate() {
            if i > 0 {
                out.push(b'\n');
            }
            escape_line_into(&mut out, body);
        }
        out
    }

    #[test]
    fn backslash_and_newline_are_escaped_and_nothing_else() {
        let mut out = Vec::new();
        escape_line_into(&mut out, b"a\\b\nc\r\t\xff");
        assert_eq!(out, b"a\\\\b\\nc\r\t\xff");
        assert_eq!(escaped_len(b"a\\b\nc\r\t\xff"), out.len());
    }

    #[test]
    fn arbitrary_bodies_round_trip() {
        let bodies: &[&[u8]] = &[
            b"plain",
            b"",
            b"two\nlines",
            b"trailing backslash\\",
            b"\\n is not a newline",
            b"\\\\n",
            b"\n",
            b"",
            b"\xff\xfe not utf-8 \x00",
            "caf\u{e9}".as_bytes(),
        ];
        let value = join(bodies);
        assert_eq!(value.iter().filter(|&&b| b == b'\n').count(), bodies.len() - 1);
        let read = unescape_lines(&value);
        assert_eq!(read, bodies.iter().map(|b| b.to_vec()).collect::<Vec<_>>());
    }

    #[test]
    fn every_two_byte_body_round_trips() {
        for a in 0..=255u8 {
            for b in 0..=255u8 {
                let body = [a, b];
                assert_eq!(unescape_lines(&join(&[&body])), vec![body.to_vec()], "{body:?}");
            }
        }
    }

    #[test]
    fn an_empty_value_is_one_empty_body() {
        assert_eq!(unescape_lines(b""), vec![Vec::<u8>::new()]);
    }

    #[test]
    fn an_unknown_escape_and_a_lone_trailing_backslash_are_kept() {
        assert_eq!(unescape_lines(b"a\\tb\\"), vec![b"a\\tb\\".to_vec()]);
        assert_eq!(unescape_lines(b"\\\nx"), vec![b"\\".to_vec(), b"x".to_vec()]);
    }
}
