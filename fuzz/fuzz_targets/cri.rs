//! One CRI container log line through `cri::parse`, as `cri_in` reads it. Oracles: a parsed
//! line's content is the input's tail, the three fields before it are spaces apart and name the
//! stream and tag, and the timestamp field reads the same through the shared RFC 3339 parser.
#![no_main]

use libfuzzer_sys::fuzz_target;
use logit_core::time::parse_rfc3339_to_nanos;
use logit_proto::cri::parse;

fuzz_target!(|data: &[u8]| {
    let Ok(line) = parse(data) else { return };
    assert!(data.ends_with(line.content), "content is a suffix");
    let head = &data[..data.len() - line.content.len()];
    assert_eq!(head.last(), Some(&b' '));
    let mut fields = head[..head.len() - 1].split(|&b| b == b' ');
    let ts = fields.next().unwrap();
    assert_eq!(fields.next(), Some(line.stream.as_str().as_bytes()));
    let tag = fields.next().unwrap();
    assert!(fields.next().is_none(), "three fields precede the content");
    assert_eq!(line.partial, tag.split(|&b| b == b':').next() == Some(b"P"));
    let expected = std::str::from_utf8(ts).ok().and_then(|s| parse_rfc3339_to_nanos(s).ok());
    assert_eq!(line.timestamp_nanos.ok(), expected);
});
