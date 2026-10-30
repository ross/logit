//! Where a request goes and how it's signed (`docs/adr/s3-object-archive.md`, decision 5).
//!
//! - **Addressing.** Without `endpoint:`, AWS's regional endpoint for `region:`,
//!   virtual-hosted (`https://<bucket>.s3.<region>.amazonaws.com`, `.amazonaws.com.cn` for a `cn-`
//!   region); with it, path-style (`<endpoint>/<bucket>/<key>`). `path_style:` overrides either.
//!   Another partition's region needs `endpoint:`.
//! - **The path.** S3 encodes a request path once, segment by segment, where other services
//!   encode it twice, so the URL carries `logit_aws::sigv4::uri_encode`'s form of each segment
//!   and the canonical request encodes the decoded path the same way.
//! - **SigV4 header auth.** Every header the sink sends but `Content-Length` and `Authorization`
//!   is signed, `host` among them; `x-amz-content-sha256` is the body's hex SHA-256, signed as the
//!   payload hash, and temporary credentials add `x-amz-security-token`. The query is signed
//!   decoded, a parameter with no value as `name=`, while the URL writes it as `?name`.

use logit_aws::creds::Credentials;
use logit_aws::sigv4::{uri_encode, AmzDate, Request, Signer};

/// Where every request of one sink goes: an origin, the `Host` it signs, and the path the key
/// follows.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct Addressing {
    /// `scheme://host[:port]`, with no trailing `/`.
    origin: String,
    /// The `Host` header `reqwest` sends for `origin`: the host, and the port unless it's the
    /// scheme's default.
    host: String,
    /// `/<bucket>` under path-style, else empty: decoded, as the canonical request takes it.
    bucket_path: String,
}

impl Addressing {
    /// The addressing `bucket`, `region`, `endpoint`, and `path_style` describe. An `endpoint`
    /// that isn't an `http://` or `https://` URL with a host is an error naming it redacted.
    pub(crate) fn new(
        bucket: &str,
        region: &str,
        endpoint: Option<&str>,
        path_style: Option<bool>,
    ) -> anyhow::Result<Self> {
        let path_style = path_style.unwrap_or(endpoint.is_some());
        let (scheme, host) = match endpoint {
            Some(endpoint) => {
                let url = reqwest::Url::parse(endpoint)
                    .ok()
                    .filter(|url| matches!(url.scheme(), "http" | "https"))
                    .filter(|url| url.host_str().is_some())
                    .ok_or_else(|| {
                        anyhow::anyhow!(
                            "s3_out 'endpoint' ({}) isn't an http:// or https:// URL with a host",
                            logit_core::redact::url(endpoint)
                        )
                    })?;
                let mut host = url.host_str().unwrap_or_default().to_string();
                if let Some(port) = url.port() {
                    host.push_str(&format!(":{port}"));
                }
                (url.scheme().to_string(), host)
            }
            None => {
                let domain =
                    if region.starts_with("cn-") { "amazonaws.com.cn" } else { "amazonaws.com" };
                ("https".to_string(), format!("s3.{region}.{domain}"))
            }
        };
        let (host, bucket_path) = if path_style {
            (host, format!("/{bucket}"))
        } else {
            (format!("{bucket}.{host}"), String::new())
        };
        Ok(Self { origin: format!("{scheme}://{host}"), host, bucket_path })
    }

    pub(crate) fn host(&self) -> &str {
        &self.host
    }

    /// The URL of a request on `key` with `query`, decoded pairs, and its path decoded, as the
    /// canonical request takes it. A pair with an empty value is written as its name alone, as
    /// `?uploads` is; SigV4 signs it as `uploads=` either way.
    pub(crate) fn request(&self, key: &str, query: &[(&str, &str)]) -> (String, String) {
        let path = format!("{}/{key}", self.bucket_path);
        let encoded: Vec<String> = path.split('/').map(uri_encode).collect();
        let mut url = format!("{}{}", self.origin, encoded.join("/"));
        for (n, (name, value)) in query.iter().enumerate() {
            url.push(if n == 0 { '?' } else { '&' });
            url.push_str(&uri_encode(name));
            if !value.is_empty() {
                url.push('=');
                url.push_str(&uri_encode(value));
            }
        }
        (url, path)
    }
}

/// The `Authorization` header of a request whose decoded path is `path` and decoded query is
/// `query`, carrying `headers`, every one of them signed, with a body whose hex SHA-256 is
/// `payload_hash`.
#[allow(clippy::too_many_arguments)]
pub(crate) fn authorization(
    credentials: &Credentials,
    region: &str,
    date: &AmzDate,
    method: &str,
    path: &str,
    query: &[(&str, &str)],
    headers: &[(String, String)],
    payload_hash: &str,
) -> String {
    let signer = Signer {
        access_key_id: &credentials.access_key_id,
        secret_access_key: &credentials.secret_access_key,
        region,
        service: "s3",
    };
    let query = query.iter().map(|(name, value)| (name.to_string(), value.to_string())).collect();
    let request = Request { method, path, query, headers: headers.to_vec(), payload_hash };
    let signature = signer.sign(&request, date);
    let mut names: Vec<String> =
        headers.iter().map(|(name, _)| name.to_ascii_lowercase()).collect();
    names.sort();
    names.dedup();
    format!(
        "AWS4-HMAC-SHA256 Credential={}/{},SignedHeaders={},Signature={signature}",
        credentials.access_key_id,
        signer.scope(date),
        names.join(";"),
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    fn credentials(access_key_id: &str, secret_access_key: &str) -> Credentials {
        Credentials {
            access_key_id: access_key_id.to_string(),
            secret_access_key: secret_access_key.to_string(),
            session_token: None,
            expires: None,
            source: "test".to_string(),
        }
    }

    /// "Example: PUT Object" in AWS's "Signature Calculations for the Authorization Header:
    /// Transferring Payload in a Single Chunk (AWS Signature Version 4)", the Amazon S3 API
    /// reference's SigV4 examples
    /// (<https://docs.aws.amazon.com/AmazonS3/latest/API/sig-v4-header-based-auth.html>): the
    /// key `test$file.text`, its `$` encoded once in the canonical URI, with the guide's example
    /// credentials, at 2013-05-24T00:00:00Z.
    #[test]
    fn the_authorization_matches_aws_put_object_example() {
        let date = AmzDate::from_unix_nanos(1_369_353_600 * 1_000_000_000);
        let addressing =
            Addressing { origin: String::new(), host: String::new(), bucket_path: String::new() };
        let (url, path) = addressing.request("test$file.text", &[]);
        assert_eq!((url.as_str(), path.as_str()), ("/test%24file.text", "/test$file.text"));
        let headers: Vec<(String, String)> = [
            ("Host", "examplebucket.s3.amazonaws.com"),
            ("Date", "Fri, 24 May 2013 00:00:00 GMT"),
            ("x-amz-date", "20130524T000000Z"),
            ("x-amz-storage-class", "REDUCED_REDUNDANCY"),
            (
                "x-amz-content-sha256",
                "44ce7dd67c959e0d3524ffac1771dfbba87d2b6b4b4e99e42034a8b803f8b072",
            ),
        ]
        .into_iter()
        .map(|(name, value)| (name.to_string(), value.to_string()))
        .collect();
        let hash = ring::digest::digest(&ring::digest::SHA256, b"Welcome to Amazon S3.");
        let payload_hash = crate::s3::object::hex(hash.as_ref());
        assert_eq!(payload_hash, headers[4].1);
        let authorization = authorization(
            &credentials("AKIAIOSFODNN7EXAMPLE", "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY"),
            "us-east-1",
            &date,
            "PUT",
            &path,
            &[],
            &headers,
            &payload_hash,
        );
        assert_eq!(
            authorization,
            "AWS4-HMAC-SHA256 Credential=AKIAIOSFODNN7EXAMPLE/20130524/us-east-1/s3/aws4_request,\
             SignedHeaders=date;host;x-amz-content-sha256;x-amz-date;x-amz-storage-class,\
             Signature=98ad721746da40c64f1a55b78f14c238d841ea1380cd77a1b5971af0ece108bd"
        );
    }

    /// "Example: GET Bucket Lifecycle" from the same AWS page: a query parameter with no value,
    /// `?lifecycle`, signed as `lifecycle=`, the form `?uploads` takes.
    #[test]
    fn the_authorization_matches_aws_get_bucket_lifecycle_example() {
        let date = AmzDate::from_unix_nanos(1_369_353_600 * 1_000_000_000);
        let headers: Vec<(String, String)> = [
            ("host", "examplebucket.s3.amazonaws.com"),
            ("x-amz-date", "20130524T000000Z"),
            (
                "x-amz-content-sha256",
                "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855",
            ),
        ]
        .into_iter()
        .map(|(name, value)| (name.to_string(), value.to_string()))
        .collect();
        let authorization = authorization(
            &credentials("AKIAIOSFODNN7EXAMPLE", "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY"),
            "us-east-1",
            &date,
            "GET",
            "/",
            &[("lifecycle", "")],
            &headers,
            &headers[2].1,
        );
        assert_eq!(
            authorization,
            "AWS4-HMAC-SHA256 Credential=AKIAIOSFODNN7EXAMPLE/20130524/us-east-1/s3/aws4_request,\
             SignedHeaders=host;x-amz-content-sha256;x-amz-date,\
             Signature=fea454ca298b7da1c68078a5d1bdbfbbe0d65c699e0f91ac7a200a0136783543"
        );
    }

    #[test]
    fn addressing_is_virtual_hosted_on_aws_and_path_style_on_an_endpoint() {
        let cases = [
            (
                "us-east-1",
                None,
                None,
                "https://b.s3.us-east-1.amazonaws.com/k",
                "b.s3.us-east-1.amazonaws.com",
                "/k",
            ),
            (
                "cn-north-1",
                None,
                None,
                "https://b.s3.cn-north-1.amazonaws.com.cn/k",
                "b.s3.cn-north-1.amazonaws.com.cn",
                "/k",
            ),
            (
                "us-east-1",
                None,
                Some(true),
                "https://s3.us-east-1.amazonaws.com/b/k",
                "s3.us-east-1.amazonaws.com",
                "/b/k",
            ),
            (
                "us-east-1",
                Some("http://minio:9000"),
                None,
                "http://minio:9000/b/k",
                "minio:9000",
                "/b/k",
            ),
            ("us-east-1", Some("https://minio:443/"), None, "https://minio/b/k", "minio", "/b/k"),
            (
                "us-east-1",
                Some("https://store.example"),
                Some(false),
                "https://b.store.example/k",
                "b.store.example",
                "/k",
            ),
        ];
        for (region, endpoint, path_style, url, host, path) in cases {
            let addressing = Addressing::new("b", region, endpoint, path_style).unwrap();
            assert_eq!(
                addressing.request("k", &[]),
                (url.to_string(), path.to_string()),
                "{endpoint:?}"
            );
            assert_eq!(addressing.host(), host);
        }
        let err = Addressing::new("b", "r", Some("ftp://u:secret@x"), None).unwrap_err();
        assert!(!format!("{err:#}").contains("secret"), "{err:#}");
    }

    #[test]
    fn a_key_is_encoded_once_per_segment_in_the_url() {
        let addressing =
            Addressing::new("my-bucket", "us-east-1", Some("http://s3:9000"), None).unwrap();
        let (url, path) = addressing.request("logs/dt=2026-10-30/a b+c~é.ndjson", &[]);
        assert_eq!(url, "http://s3:9000/my-bucket/logs/dt%3D2026-10-30/a%20b%2Bc~%C3%A9.ndjson");
        assert_eq!(path, "/my-bucket/logs/dt=2026-10-30/a b+c~é.ndjson");
    }

    #[test]
    fn a_query_is_encoded_and_an_empty_value_written_as_its_name() {
        let addressing = Addressing::new("b", "us-east-1", Some("http://s3:9000"), None).unwrap();
        assert_eq!(addressing.request("k", &[("uploads", "")]).0, "http://s3:9000/b/k?uploads");
        let (url, path) = addressing.request("k", &[("partNumber", "3"), ("uploadId", "a/b+c=")]);
        assert_eq!(url, "http://s3:9000/b/k?partNumber=3&uploadId=a%2Fb%2Bc%3D");
        assert_eq!(path, "/b/k");
    }
}
