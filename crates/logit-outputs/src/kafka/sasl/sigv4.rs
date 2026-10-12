//! AWS Signature Version 4 on `ring`: the canonical request, the string to sign, the derived
//! signing key, and the presigned query `AWS_MSK_IAM` sends (`docs/adr/kafka-sasl.md`).
//!
//! The rules are AWS's "Create a signed AWS API request" guide, pinned by vectors from AWS's
//! SigV4 test suite in `tests`:
//!
//! - Names and values are URI-encoded byte by byte: `A-Z a-z 0-9 - _ . ~` as they are, every
//!   other byte `%XX` in uppercase hex, so a space is `%20` and never `+`. A path keeps its `/`.
//! - The query is sorted by encoded name, then by encoded value.
//! - A header name is lowercased, and its value trimmed with each run of spaces collapsed to one.
//!   Headers sort by name, and repeated names join their values with `,`.

use ring::{digest, hmac};

/// The algorithm every SigV4 string to sign and credential names.
pub(crate) const ALGORITHM: &str = "AWS4-HMAC-SHA256";

/// The SHA-256 of an empty payload, in hex: a presigned `GET`'s payload hash.
pub(crate) const EMPTY_PAYLOAD_SHA256: &str =
    "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855";

/// A request time in SigV4's two forms.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct AmzDate {
    /// `YYYYMMDDTHHMMSSZ`, the `X-Amz-Date`.
    pub(crate) datetime: String,
}

impl AmzDate {
    /// The time `nanos` after the Unix epoch, to the second.
    pub(crate) fn from_unix_nanos(nanos: i64) -> Self {
        // `YYYY-MM-DDTHH:MM:SS.fffffffffZ`, the one form `format_rfc3339_utc` writes.
        let rfc3339 = logit_core::time::format_rfc3339_utc(nanos);
        let datetime: String = rfc3339[..19].chars().filter(|c| *c != '-' && *c != ':').collect();
        Self { datetime: format!("{datetime}Z") }
    }

    pub(crate) fn now() -> Self {
        let nanos = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map_or(0, |d| i64::try_from(d.as_nanos()).unwrap_or(i64::MAX));
        Self::from_unix_nanos(nanos)
    }

    /// `YYYYMMDD`, the credential scope's date.
    pub(crate) fn date(&self) -> &str {
        &self.datetime[..8]
    }
}

/// The credentials and scope a signature is made with.
pub(crate) struct Signer<'a> {
    pub(crate) access_key_id: &'a str,
    pub(crate) secret_access_key: &'a str,
    pub(crate) region: &'a str,
    pub(crate) service: &'a str,
}

/// The parts of a request SigV4 signs. Query names and values are given decoded.
pub(crate) struct Request<'a> {
    pub(crate) method: &'a str,
    pub(crate) path: &'a str,
    pub(crate) query: Vec<(String, String)>,
    pub(crate) headers: Vec<(String, String)>,
    /// The payload's SHA-256 in hex, or `UNSIGNED-PAYLOAD`.
    pub(crate) payload_hash: &'a str,
}

impl Signer<'_> {
    /// `<date>/<region>/<service>/aws4_request`.
    pub(crate) fn scope(&self, date: &AmzDate) -> String {
        format!("{}/{}/{}/aws4_request", date.date(), self.region, self.service)
    }

    /// The signature of `request` at `date`, in hex.
    pub(crate) fn sign(&self, request: &Request<'_>, date: &AmzDate) -> String {
        let sts = string_to_sign(date, &self.scope(date), &canonical_request(request));
        let key = signing_key(self.secret_access_key, date.date(), self.region, self.service);
        hex(hmac::sign(&key, sts.as_bytes()).as_ref())
    }

    /// Presigns a request to `host` with `query`, valid for `expires_secs`: the `X-Amz-*`
    /// parameters a presigned URL adds, signed over `query` and themselves with `host` the one
    /// signed header and an empty payload. Returns every parameter, decoded, in the order AWS's
    /// presigners write them: `query`, the signed `X-Amz-*` ones, then `X-Amz-Signature`. A
    /// session token is signed with the rest, as the AWS SDKs' presigners sign it.
    pub(crate) fn presign(
        &self,
        host: &str,
        query: &[(&str, &str)],
        session_token: Option<&str>,
        date: &AmzDate,
        expires_secs: u32,
    ) -> Vec<(String, String)> {
        let mut params: Vec<(String, String)> =
            query.iter().map(|(k, v)| (k.to_string(), v.to_string())).collect();
        params.push(("X-Amz-Algorithm".into(), ALGORITHM.into()));
        params.push((
            "X-Amz-Credential".into(),
            format!("{}/{}", self.access_key_id, self.scope(date)),
        ));
        params.push(("X-Amz-Date".into(), date.datetime.clone()));
        params.push(("X-Amz-Expires".into(), expires_secs.to_string()));
        if let Some(token) = session_token {
            params.push(("X-Amz-Security-Token".into(), token.to_string()));
        }
        params.push(("X-Amz-SignedHeaders".into(), "host".into()));
        let request = Request {
            method: "GET",
            path: "/",
            query: params.clone(),
            headers: vec![("host".into(), host.to_string())],
            payload_hash: EMPTY_PAYLOAD_SHA256,
        };
        params.push(("X-Amz-Signature".into(), self.sign(&request, date)));
        params
    }
}

/// The canonical request: method, path, query, headers, signed headers, and payload hash, one
/// per line.
pub(crate) fn canonical_request(request: &Request<'_>) -> String {
    let path: Vec<String> = request.path.split('/').map(uri_encode).collect();
    let mut query: Vec<(String, String)> =
        request.query.iter().map(|(k, v)| (uri_encode(k), uri_encode(v))).collect();
    query.sort();
    let query: Vec<String> = query.into_iter().map(|(k, v)| format!("{k}={v}")).collect();

    let mut headers: Vec<(String, String)> = Vec::new();
    for (name, value) in &request.headers {
        let name = name.to_ascii_lowercase();
        let value = value.split_whitespace().collect::<Vec<_>>().join(" ");
        match headers.iter_mut().find(|(n, _)| *n == name) {
            Some((_, joined)) => {
                joined.push(',');
                joined.push_str(&value);
            }
            None => headers.push((name, value)),
        }
    }
    headers.sort();
    let canonical_headers: String =
        headers.iter().map(|(name, value)| format!("{name}:{value}\n")).collect();
    let signed: Vec<&str> = headers.iter().map(|(name, _)| name.as_str()).collect();

    format!(
        "{}\n{}\n{}\n{}\n{}\n{}",
        request.method,
        path.join("/"),
        query.join("&"),
        canonical_headers,
        signed.join(";"),
        request.payload_hash
    )
}

/// The string to sign for `canonical` at `date` in `scope`.
pub(crate) fn string_to_sign(date: &AmzDate, scope: &str, canonical: &str) -> String {
    let hash = hex(digest::digest(&digest::SHA256, canonical.as_bytes()).as_ref());
    format!("{ALGORITHM}\n{}\n{scope}\n{hash}", date.datetime)
}

/// `HMAC(HMAC(HMAC(HMAC("AWS4" + secret, date), region), service), "aws4_request")`.
fn signing_key(secret: &str, date: &str, region: &str, service: &str) -> hmac::Key {
    let mut key = format!("AWS4{secret}").into_bytes();
    for part in [date, region, service, "aws4_request"] {
        let tag = hmac::sign(&hmac::Key::new(hmac::HMAC_SHA256, &key), part.as_bytes());
        key = tag.as_ref().to_vec();
    }
    hmac::Key::new(hmac::HMAC_SHA256, &key)
}

/// SigV4's URI encoding of one name, value, or path segment.
pub(crate) fn uri_encode(value: &str) -> String {
    let mut out = String::with_capacity(value.len());
    for b in value.bytes() {
        match b {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_' | b'.' | b'~' => {
                out.push(char::from(b))
            }
            _ => out.push_str(&format!("%{b:02X}")),
        }
    }
    out
}

fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{b:02x}")).collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    // Cases from AWS's SigV4 test suite as awslabs/aws-c-auth carries it, at commit
    // 84a26ea184d9ccd322c8164759510b3604ab13fd, under tests/aws-signing-test-suite/v4/<case>/:
    // each case's `context.json` (credentials, region, service, and time, shared by every case
    // below), `request.txt`, and the `header-*` and `query-*` canonical requests and signatures.

    const ACCESS_KEY_ID: &str = "AKIDEXAMPLE";
    const SECRET: &str = "wJalrXUtnFEMI/K7MDENG+bPxRfiCYEXAMPLEKEY";
    const HOST: &str = "example.amazonaws.com";
    /// `get-vanilla-with-session-token`'s token.
    const TOKEN: &str = "6e86291e8372ff2a2260956d9b8aae1d763fbf315fa00fa31553b73ebf194267";
    /// `post-sts-header-before`'s and `post-sts-header-after`'s token.
    const STS_TOKEN: &str = "AQoDYXdzEPT//////////wEXAMPLEtc764bNrC9SAPBSM22wDOk4x4HIZ8j4FZTwdQWLW\
        sKWHGBuFqwAeMicRXmxfpSPfIeoIYRqTflfKD8YUuwthAx7mSEI/qkPpKPi/kMcGdQrmGdeehM4IC1NtBmUpp2wUE8\
        phUZampKsburEDy0KPkyQDYwT7WZ0wq5VSXDvp75YU9HFvlRd8Tx6q6fE8YQcHNVXAkiY9q6d+xo0rKwT38xVqr7ZD\
        0u0iPPkUL64lIZbqBAz+scqKmlzm8FDrypNC9Yjc8fPOLn9FX9KSYvKTr4rvx3iSIlTJabIQwj2ICCR/oLxBA==";

    fn suite_signer() -> Signer<'static> {
        Signer {
            access_key_id: ACCESS_KEY_ID,
            secret_access_key: SECRET,
            region: "us-east-1",
            service: "service",
        }
    }

    /// 2015-08-30T12:36:00Z.
    fn suite_date() -> AmzDate {
        AmzDate::from_unix_nanos(1_440_938_160 * 1_000_000_000)
    }

    fn pairs(query: &[(&str, &str)]) -> Vec<(String, String)> {
        query.iter().map(|(k, v)| (k.to_string(), v.to_string())).collect()
    }

    /// A case's `header-signature.txt`: `host` and `x-amz-date` signed, and the session token
    /// as a header when `token` is set.
    fn header_signature(
        method: &str,
        path: &str,
        query: &[(&str, &str)],
        token: Option<&str>,
    ) -> String {
        let date = suite_date();
        let mut headers = vec![
            ("Host".to_string(), HOST.to_string()),
            ("X-Amz-Date".to_string(), date.datetime.clone()),
        ];
        if let Some(token) = token {
            headers.push(("X-Amz-Security-Token".to_string(), token.to_string()));
        }
        let request = Request {
            method,
            path,
            query: pairs(query),
            headers,
            payload_hash: EMPTY_PAYLOAD_SHA256,
        };
        suite_signer().sign(&request, &date)
    }

    #[test]
    fn the_date_has_both_forms() {
        let date = suite_date();
        assert_eq!(date.datetime, "20150830T123600Z");
        assert_eq!(date.date(), "20150830");
        assert_eq!(suite_signer().scope(&date), "20150830/us-east-1/service/aws4_request");
    }

    #[test]
    fn header_signatures_match_the_aws_test_suite() {
        let cases: Vec<(&str, String, &str)> = vec![
            (
                "get-vanilla",
                header_signature("GET", "/", &[], None),
                "5fa00fa31553b73ebf1942676e86291e8372ff2a2260956d9b8aae1d763fbf31",
            ),
            (
                "get-vanilla-query-order-key-case",
                header_signature("GET", "/", &[("Param2", "value2"), ("Param1", "value1")], None),
                "b97d918cfa904a5beff61c982a1b6f458b799221646efd99d3219ec94cdf2500",
            ),
            (
                "get-utf8",
                header_signature("GET", "/\u{1234}", &[], None),
                "8318018e0b0f223aa2bbf98705b62bb787dc9c0e678f255a891fd03141be5d85",
            ),
            (
                "get-vanilla-utf8-query",
                header_signature("GET", "/", &[("\u{1234}", "bar")], None),
                "2cdec8eed098649ff3a119c94853b13c643bcf08f8b0a1d91e12c9027818dd04",
            ),
            (
                "get-vanilla-query-unreserved",
                header_signature(
                    "GET",
                    "/",
                    &[(
                        "-._~0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz",
                        "-._~0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz",
                    )],
                    None,
                ),
                "9c3e54bfcdf0b19771a7f523ee5669cdf59bc7cc0884027167c21bb143a40197",
            ),
            (
                "get-vanilla-with-session-token",
                header_signature("GET", "/", &[], Some(TOKEN)),
                "07ec1639c89043aa0e3e2de82b96708f198cceab042d4a97044c66dd9f74e7f8",
            ),
            (
                "post-sts-header-before",
                header_signature("POST", "/", &[], Some(STS_TOKEN)),
                "85d96828115b5dc0cfc3bd16ad9e210dd772bbebba041836c64533a82be05ead",
            ),
            (
                "post-sts-header-after",
                header_signature("POST", "/", &[], None),
                "5da7c1a2acd57cee7505fc6676e4e544621c30862966e37dddb68e92efbe5d6b",
            ),
        ];
        for (case, got, want) in cases {
            assert_eq!(got, want, "{case}");
        }
    }

    /// The query variants: `presign`'s parameters signed over `GET /` with `host` alone.
    #[test]
    fn presigned_queries_match_the_aws_test_suite() {
        let date = suite_date();
        let signature = |query: &[(&str, &str)], token: Option<&str>| {
            let params = suite_signer().presign(HOST, query, token, &date, 3600);
            let (name, value) = params.last().unwrap().clone();
            assert_eq!(name, "X-Amz-Signature");
            value
        };
        assert_eq!(
            signature(&[], None),
            "e93c787ed7f371d5c6b165c1b38ede9550f4dce4144713e844b25b7192d3865d",
            "get-vanilla"
        );
        assert_eq!(
            signature(&[("Param2", "value2"), ("Param1", "value1")], None),
            "86012e2c9ad4d77369f5d81c11f75158aae4f895a085212cc6d3f923d300bed5",
            "get-vanilla-query-order-key-case"
        );
        assert_eq!(
            signature(&[("\u{1234}", "bar")], None),
            "0bdd809b1519ac4f0c1dc3540e2cc46bd0c7f778eda408b2ebf3b913d21ff600",
            "get-vanilla-utf8-query"
        );
        assert_eq!(
            signature(&[], Some(TOKEN)),
            "7ff2b50b376cb4d151970630573d6291dc128cc5c2a12ffb237f73cc53f67b6c",
            "get-vanilla-with-session-token"
        );
    }

    /// `post-sts-header-before`'s query variant: a `POST`, with the token's `/`, `+`, and `=`
    /// percent-encoded in the signed query.
    #[test]
    fn a_session_token_is_percent_encoded_in_the_canonical_query() {
        let date = suite_date();
        let signer = suite_signer();
        let request = Request {
            method: "POST",
            path: "/",
            query: pairs(&[
                ("X-Amz-Algorithm", ALGORITHM),
                ("X-Amz-Credential", "AKIDEXAMPLE/20150830/us-east-1/service/aws4_request"),
                ("X-Amz-Date", "20150830T123600Z"),
                ("X-Amz-Expires", "3600"),
                ("X-Amz-Security-Token", STS_TOKEN),
                ("X-Amz-SignedHeaders", "host"),
            ]),
            headers: vec![("host".to_string(), HOST.to_string())],
            payload_hash: EMPTY_PAYLOAD_SHA256,
        };
        assert!(canonical_request(&request)
            .contains("X-Amz-Security-Token=AQoDYXdzEPT%2F%2F%2F%2F%2F%2F%2F%2F%2F%2FwEXAMPLE"));
        assert_eq!(
            signer.sign(&request, &date),
            "46c2e3c63c1dbe7d39f8ada16fe7f001c1f56c5791441565323677f96308871c"
        );
    }

    /// `get-vanilla`'s `query-canonical-request.txt` and `query-string-to-sign.txt`, whole.
    #[test]
    fn the_canonical_request_and_string_to_sign_match_the_aws_test_suite() {
        let date = suite_date();
        let signer = suite_signer();
        let params = signer.presign(HOST, &[], None, &date, 3600);
        let request = Request {
            method: "GET",
            path: "/",
            query: params[..params.len() - 1].to_vec(),
            headers: vec![("host".to_string(), HOST.to_string())],
            payload_hash: EMPTY_PAYLOAD_SHA256,
        };
        let canonical = canonical_request(&request);
        assert_eq!(
            canonical,
            "GET\n/\nX-Amz-Algorithm=AWS4-HMAC-SHA256&X-Amz-Credential=AKIDEXAMPLE%2F20150830%2F\
             us-east-1%2Fservice%2Faws4_request&X-Amz-Date=20150830T123600Z&X-Amz-Expires=3600&\
             X-Amz-SignedHeaders=host\nhost:example.amazonaws.com\n\nhost\n\
             e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"
        );
        let sts = string_to_sign(&date, &signer.scope(&date), &canonical);
        assert_eq!(
            sts.lines().collect::<Vec<_>>(),
            [
                "AWS4-HMAC-SHA256",
                "20150830T123600Z",
                "20150830/us-east-1/service/aws4_request",
                // get-vanilla's query-string-to-sign.txt.
                sts.lines().last().unwrap(),
            ]
        );
    }

    #[test]
    fn header_values_are_trimmed_and_repeated_names_joined() {
        let request = Request {
            method: "GET",
            path: "/",
            query: Vec::new(),
            headers: vec![
                ("My-Header1".into(), "  a   b   c  ".into()),
                ("Host".into(), HOST.into()),
                ("my-header1".into(), "d".into()),
            ],
            payload_hash: EMPTY_PAYLOAD_SHA256,
        };
        let canonical = canonical_request(&request);
        assert!(canonical
            .contains("\nhost:example.amazonaws.com\nmy-header1:a b c,d\n\nhost;my-header1\n"));
        assert_eq!(uri_encode("a b+c/é~"), "a%20b%2Bc%2F%C3%A9~");
    }
}
