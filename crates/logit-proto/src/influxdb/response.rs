//! influxd's error body, `{"code":"<code>","message":"<message>"}`, written byte-exact with no
//! whitespace, in influxd's key order. There's no `line` member: influxd names a bad line by its
//! text inside `message` (probe section 13). Lives in the codec because only this crate depends
//! on `serde_json`.

use super::line::LineError;
use crate::json::{write_str, JsonObject};
use bytes::Bytes;

/// A parse error, a missing `org` and `orgID`, or a bad `precision` (`400`).
pub const CODE_INVALID: &str = "invalid";
/// A missing or unknown token (`401`).
pub const CODE_UNAUTHORIZED: &str = "unauthorized";
/// A missing bucket or an unknown route (`404`).
pub const CODE_NOT_FOUND: &str = "not found";
/// The pipeline stayed busy (`503`).
pub const CODE_UNAVAILABLE: &str = "unavailable";
/// A body over `max_request_bytes` (`413`).
pub const CODE_REQUEST_TOO_LARGE: &str = "request too large";
/// A `Content-Encoding` other than `gzip` or identity (`415`).
pub const CODE_UNSUPPORTED_MEDIA_TYPE: &str = "unsupported media type";

/// `{"code":<code>,"message":<message>}`, each a JSON string.
pub fn error_body(code: &str, message: &str) -> Bytes {
    let mut out = Vec::with_capacity(code.len() + message.len() + 24);
    let mut object = JsonObject::begin(&mut out);
    write_str(object.key("code"), code);
    write_str(object.key("message"), message);
    object.finish();
    Bytes::from(out)
}

/// The `400` body for a line that failed to parse: `unable to parse '<line>': <reason>`, as
/// influxd words it. A line that isn't UTF-8 is quoted lossily, as influxd's JSON encoder does.
///
/// influxd lists every bad line, joined by `\n`; the decoder stops at the first, so this names
/// one.
pub fn parse_error_body(err: &LineError) -> Bytes {
    let message =
        format!("unable to parse '{}': {}", String::from_utf8_lossy(&err.text), err.reason);
    error_body(CODE_INVALID, &message)
}

#[cfg(test)]
mod tests {
    use super::*;

    // probe: influxd 2.9.1, row 14.4
    #[test]
    fn an_error_body_is_byte_exact() {
        assert_eq!(
            error_body(CODE_UNAUTHORIZED, "unauthorized access"),
            Bytes::from_static(br#"{"code":"unauthorized","message":"unauthorized access"}"#)
        );
    }

    // probe: influxd 2.9.1, row 1.7: the message JSON-escapes the line's backslashes
    #[test]
    fn a_parse_error_body_quotes_the_line() {
        let err = LineError {
            line: 1,
            text: Bytes::from_static(br"p01g\\ f=1i"),
            reason: "missing fields".into(),
        };
        assert_eq!(
            parse_error_body(&err),
            Bytes::from_static(
                br#"{"code":"invalid","message":"unable to parse 'p01g\\\\ f=1i': missing fields"}"#
            )
        );
    }

    // probe: influxd 2.9.1, row 4.2: control bytes and quotes escape
    #[test]
    fn control_bytes_and_quotes_escape() {
        let err = LineError {
            line: 1,
            text: Bytes::from_static(b"p04d f=\"x\r"),
            reason: "unbalanced quotes".into(),
        };
        assert_eq!(
            parse_error_body(&err),
            Bytes::from_static(
                br#"{"code":"invalid","message":"unable to parse 'p04d f=\"x\r': unbalanced quotes"}"#
            )
        );
    }
}
