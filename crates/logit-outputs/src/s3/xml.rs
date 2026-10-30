//! The text of a leaf element in an S3 XML answer, with no XML crate
//! (`docs/adr/s3-object-archive.md`, decision 5). S3's error body is
//! `<Error><Code>…</Code><Message>…</Message>…</Error>`, each field a leaf element with no
//! attributes, and the sink reads its `<Code>` and `<Message>` over a bounded read
//! ([`ERROR_BODY_BYTES`]), so a body cut by that bound yields only the fields that ended in it.

/// How much of an error answer's body the sink reads: an S3 error names its `<Code>` first, and
/// its `<Message>` is a sentence.
pub(crate) const ERROR_BODY_BYTES: usize = 4096;

/// The text of the first `<tag>…</tag>` in `body`, its five predefined entity references and
/// numeric character references decoded, or `None` when `body` has no complete one. Bytes that
/// aren't UTF-8 are replaced.
pub(crate) fn element(body: &[u8], tag: &str) -> Option<String> {
    let open = format!("<{tag}>");
    let close = format!("</{tag}>");
    let start = find(body, open.as_bytes())? + open.len();
    let len = find(&body[start..], close.as_bytes())?;
    Some(unescape(&String::from_utf8_lossy(&body[start..start + len])))
}

fn find(haystack: &[u8], needle: &[u8]) -> Option<usize> {
    haystack.windows(needle.len()).position(|window| window == needle)
}

/// `text` with `&amp;`, `&lt;`, `&gt;`, `&quot;`, `&apos;`, and `&#N;`/`&#xN;` decoded. An
/// unknown or malformed reference stays as it is.
fn unescape(text: &str) -> String {
    let mut out = String::with_capacity(text.len());
    let mut rest = text;
    while let Some(at) = rest.find('&') {
        out.push_str(&rest[..at]);
        rest = &rest[at..];
        let decoded = rest.find(';').and_then(|end| {
            let name = &rest[1..end];
            let ch = match name {
                "amp" => Some('&'),
                "lt" => Some('<'),
                "gt" => Some('>'),
                "quot" => Some('"'),
                "apos" => Some('\''),
                _ => name
                    .strip_prefix("#x")
                    .map(|hex| u32::from_str_radix(hex, 16))
                    .or_else(|| name.strip_prefix('#').map(str::parse))
                    .and_then(Result::ok)
                    .and_then(char::from_u32),
            };
            ch.map(|ch| (ch, end))
        });
        match decoded {
            Some((ch, end)) => {
                out.push(ch);
                rest = &rest[end + 1..];
            }
            None => {
                out.push('&');
                rest = &rest[1..];
            }
        }
    }
    out.push_str(rest);
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    /// AWS's example error body, from "Error responses" in the Amazon S3 API reference.
    const AWS_ERROR: &[u8] = br#"<?xml version="1.0" encoding="UTF-8"?>
<Error>
  <Code>NoSuchKey</Code>
  <Message>The resource you requested does not exist</Message>
  <Resource>/mybucket/myfoto.jpg</Resource>
  <RequestId>4442587FB7D0A2F9</RequestId>
</Error>"#;

    #[test]
    fn a_leaf_element_is_read_and_its_references_decoded() {
        assert_eq!(element(AWS_ERROR, "Code").as_deref(), Some("NoSuchKey"));
        assert_eq!(
            element(AWS_ERROR, "Message").as_deref(),
            Some("The resource you requested does not exist")
        );
        assert_eq!(
            element(
                b"<Message>a &lt;b&gt; &amp; &quot;c&quot; &#65;&#x42; &bogus; &</Message>",
                "Message"
            )
            .as_deref(),
            Some(r#"a <b> & "c" AB &bogus; &"#)
        );
    }

    #[test]
    fn a_missing_or_cut_element_is_none() {
        assert_eq!(element(AWS_ERROR, "UploadId"), None);
        assert_eq!(element(b"<Error><Code>SlowDo", "Code"), None, "cut by the read bound");
        assert_eq!(element(b"<html>503 Service Unavailable</html>", "Code"), None);
        assert_eq!(element(b"", "Code"), None);
    }
}
