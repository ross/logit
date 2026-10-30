//! SASL `AWS_MSK_IAM`: one client message, the JSON object `aws-msk-iam-auth`'s
//! `AWS4SignedPayloadGenerator` writes, and a broker answer that ends the exchange.
//!
//! The object is the query of a SigV4 presigned `GET https://<broker host>/?Action=
//! kafka-cluster:Connect` for service `kafka-cluster`, valid 900 seconds, with `host` the one
//! signed header and an empty payload ([`logit_aws::sigv4::Signer::presign`]). Each query parameter
//! becomes a key in lowercase with its value as it is, not percent-encoded, beside `version`
//! (`2020_10_22`), `host` (the broker's host, as the connection names it), and `user-agent`:
//!
//! ```text
//! {"action":"kafka-cluster:Connect","host":"b-1.c.kafka.us-east-1.amazonaws.com",
//!  "user-agent":"logit/0.1.0","version":"2020_10_22","x-amz-algorithm":"AWS4-HMAC-SHA256",
//!  "x-amz-credential":"AKID/20261012/us-east-1/kafka-cluster/aws4_request",
//!  "x-amz-date":"20261012T010203Z","x-amz-expires":"900","x-amz-security-token":"...",
//!  "x-amz-signature":"...","x-amz-signedheaders":"host"}
//! ```
//!
//! `x-amz-security-token` is there only for temporary credentials, and is signed with the rest,
//! as the Java SDK's presigner signs it. MSK answers an accepted payload with JSON naming a
//! `version` and a `request-id`; any answer that isn't empty ends the exchange, since the
//! signature is what the broker checks. It ties the session lifetime to the credentials' expiry,
//! and `super::authenticate`'s redial before then fetches fresh ones.
//!
//! The region is `region:` when set, else the label before an AWS domain at the end of the
//! broker's host (`b-1.c.kafka.us-east-1.amazonaws.com`,
//! `boot-x.c1.kafka-serverless.us-west-2.amazonaws.com`, `.amazonaws.com.cn`, `.api.aws`), which
//! are the `aws`, `aws-cn`, and `aws-us-gov` partitions' domains only
//! (`docs/known-gaps/kafka.md`), else `AWS_REGION` or `AWS_DEFAULT_REGION`. With none, the
//! exchange is `Refused`. Credentials come from [`logit_aws::creds::Chain`].

use std::sync::{Arc, Mutex};

use anyhow::{anyhow, Context};
use logit_pipeline::Fault;

use super::{Authenticator, Session, Step};
use logit_aws::creds::{Chain, Credentials};
use logit_aws::sigv4::{AmzDate, Signer};

/// The payload version `aws-msk-iam-auth` sends.
pub(crate) const VERSION: &str = "2020_10_22";

/// The action every payload asks for.
pub(crate) const ACTION: &str = "kafka-cluster:Connect";

/// The service the signature is scoped to.
pub(crate) const SERVICE: &str = "kafka-cluster";

/// How long the presigned query is valid, `aws-msk-iam-auth`'s 15 minutes.
pub(crate) const EXPIRES_SECS: u32 = 900;

/// The domains MSK brokers are named under, the region's label immediately before each.
const AWS_DOMAINS: &[&str] = &[".amazonaws.com", ".amazonaws.com.cn", ".api.aws"];

pub(crate) struct MskIam {
    region: Option<String>,
    chain: Arc<Chain>,
    /// Where the last credentials came from, for messages.
    source: Mutex<Option<String>>,
}

impl MskIam {
    pub(crate) fn new(region: Option<String>, chain: Chain) -> Self {
        Self { region, chain: Arc::new(chain), source: Mutex::new(None) }
    }

    /// The region to sign for, for a broker at `host`.
    fn region(&self, host: &str) -> anyhow::Result<String> {
        if let Some(region) = &self.region {
            return Ok(region.clone());
        }
        if let Some(region) = region_from_host(host) {
            return Ok(region);
        }
        ["AWS_REGION", "AWS_DEFAULT_REGION"]
            .iter()
            .find_map(|var| self.chain.var(var))
            .map(|region| region.trim().to_string())
            .ok_or_else(|| {
                anyhow!(
                    "no AWS region: 'sasl.region' is unset, the broker host {host:?} isn't an MSK \
                     host that names one, and AWS_REGION and AWS_DEFAULT_REGION are unset"
                )
            })
            .context(Fault::Refused)
    }
}

#[async_trait::async_trait]
impl Authenticator for MskIam {
    fn mechanism(&self) -> &'static str {
        "AWS_MSK_IAM"
    }

    fn principal(&self) -> String {
        match self.source.lock().unwrap().as_ref() {
            Some(source) => format!("AWS credentials from {source}"),
            None => "AWS credentials".to_string(),
        }
    }

    async fn start(&self, host: &str) -> anyhow::Result<Box<dyn Session>> {
        let region = self.region(host)?;
        let credentials = self.chain.credentials(&region).await?;
        *self.source.lock().unwrap() = Some(credentials.source.clone());
        let message = payload(&credentials, host, &region, &AmzDate::now());
        Ok(Box::new(MskSession { message: Some(message), credentials, chain: self.chain.clone() }))
    }
}

/// The client message for a broker at `host` in `region`, signed with `credentials` at `date`.
pub(crate) fn payload(
    credentials: &Credentials,
    host: &str,
    region: &str,
    date: &AmzDate,
) -> Vec<u8> {
    let signer = Signer {
        access_key_id: &credentials.access_key_id,
        secret_access_key: &credentials.secret_access_key,
        region,
        service: SERVICE,
    };
    let params = signer.presign(
        host,
        &[("Action", ACTION)],
        credentials.session_token.as_deref(),
        date,
        EXPIRES_SECS,
    );
    let mut object = serde_json::Map::new();
    object.insert("version".into(), VERSION.into());
    object.insert("host".into(), host.into());
    object.insert("user-agent".into(), concat!("logit/", env!("CARGO_PKG_VERSION")).into());
    for (name, value) in params {
        object.insert(name.to_ascii_lowercase(), value.into());
    }
    serde_json::to_vec(&object).expect("a map of strings serializes")
}

/// The region label immediately before an AWS domain at the end of `host`, when it has a
/// region's shape.
pub(crate) fn region_from_host(host: &str) -> Option<String> {
    let host = host.trim().trim_end_matches('.').to_ascii_lowercase();
    AWS_DOMAINS.iter().find_map(|domain| {
        let rest = host.strip_suffix(domain)?;
        let label = rest.rsplit('.').next()?;
        is_region(label).then(|| label.to_string())
    })
}

/// An AWS region's shape: a two-letter prefix, one or more words, and a number, such as
/// `us-east-1` or `us-gov-west-1`.
pub(crate) fn is_region(region: &str) -> bool {
    let parts: Vec<&str> = region.split('-').collect();
    parts.len() >= 3
        && parts[0].len() == 2
        && parts[0].bytes().all(|b| b.is_ascii_lowercase())
        && parts[1..parts.len() - 1]
            .iter()
            .all(|p| !p.is_empty() && p.bytes().all(|b| b.is_ascii_lowercase()))
        && parts[parts.len() - 1].bytes().all(|b| b.is_ascii_digit())
        && !parts[parts.len() - 1].is_empty()
}

struct MskSession {
    message: Option<Vec<u8>>,
    credentials: Arc<Credentials>,
    chain: Arc<Chain>,
}

impl Session for MskSession {
    fn first(&mut self) -> anyhow::Result<Vec<u8>> {
        self.message.take().ok_or_else(|| anyhow!("AWS_MSK_IAM sends one message"))
    }

    fn next(&mut self, server: &[u8]) -> anyhow::Result<Step> {
        anyhow::ensure!(!server.is_empty(), "the broker answered the payload with nothing");
        Ok(Step::Done)
    }

    /// A broker that refuses the credentials gets fresh ones on the next dial.
    fn refused(&mut self) {
        self.chain.forget(&self.credentials);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // The golden payloads' signatures come from botocore 1.40.40's `SigV4QueryAuth` (and
    // 1.43.112, identical) in python:3.12-slim@sha256:a6e34c598f2467ed0e9a8d349809fcd8b5c603269
    // 512df273a0bb1784edc11b1, its clock frozen at 2026-10-12T01:02:03Z, presigning
    // `GET https://b-1.example.abc123.c2.kafka.us-east-1.amazonaws.com/?Action=kafka-cluster%3A
    // Connect` for service `kafka-cluster` in `us-east-1`, expiring in 900 s, with AWS's example
    // key pair. A stdlib-only HMAC re-derivation agreed. The Java client presigns through the AWS
    // SDK for Java v2's `Aws4Signer.presign` with the same parameters and the same empty-payload
    // hash.

    const HOST: &str = "b-1.example.abc123.c2.kafka.us-east-1.amazonaws.com";
    const TOKEN: &str = "FwoGZXIvYXdzEXAMPLETOKEN/+=";

    fn credentials(session_token: Option<&str>) -> Credentials {
        Credentials {
            access_key_id: "AKIDEXAMPLE".into(),
            secret_access_key: "wJalrXUtnFEMI/K7MDENG+bPxRfiCYEXAMPLEKEY".into(),
            session_token: session_token.map(String::from),
            expires: None,
            source: "a test".into(),
        }
    }

    fn golden_date() -> AmzDate {
        AmzDate::from_unix_nanos(1_791_766_923 * 1_000_000_000)
    }

    fn object(bytes: &[u8]) -> serde_json::Map<String, serde_json::Value> {
        match serde_json::from_slice(bytes).unwrap() {
            serde_json::Value::Object(map) => map,
            other => panic!("not an object: {other}"),
        }
    }

    #[test]
    fn the_payload_matches_the_golden_signature_and_fields() {
        let payload = object(&payload(&credentials(None), HOST, "us-east-1", &golden_date()));
        let want = serde_json::json!({
            "version": "2020_10_22",
            "host": HOST,
            "user-agent": concat!("logit/", env!("CARGO_PKG_VERSION")),
            "action": "kafka-cluster:Connect",
            "x-amz-algorithm": "AWS4-HMAC-SHA256",
            "x-amz-credential": "AKIDEXAMPLE/20261012/us-east-1/kafka-cluster/aws4_request",
            "x-amz-date": "20261012T010203Z",
            "x-amz-expires": "900",
            "x-amz-signedheaders": "host",
            "x-amz-signature": "1d658fa3a79700fb64436c9a7cf194992f4577f03ac8026a4d62937086f5bc79",
        });
        assert_eq!(serde_json::Value::Object(payload), want);
    }

    /// The session token is signed, percent-encoded in the canonical query, and sent as it is.
    #[test]
    fn a_session_token_is_signed_and_sent_unencoded() {
        let payload =
            object(&payload(&credentials(Some(TOKEN)), HOST, "us-east-1", &golden_date()));
        assert_eq!(payload["x-amz-security-token"], TOKEN);
        assert_eq!(
            payload["x-amz-signature"],
            "7154c893ee452636cf3128707dd244aa1fcba90a66f7940e3074a42ba1329bf2"
        );
        assert_eq!(payload.len(), 11);
    }

    #[test]
    fn the_region_is_the_label_before_the_aws_domain() {
        for (host, want) in [
            ("b-1.demo-us-west-1-app.abc123.c2.kafka.us-west-2.amazonaws.com", Some("us-west-2")),
            ("b-1.us-west-1-qa.abcdef.c2.kafka.cn-north-1.amazonaws.com.cn", Some("cn-north-1")),
            ("b-1.eu-west-1-qa.abcdef.c3.kafka.us-gov-west-1.amazonaws.com", Some("us-gov-west-1")),
            ("b-1.demo.abc123.c2.kafka.us-west-2.api.aws", Some("us-west-2")),
            ("boot-us-west-1abc.c1.kafka-serverless.us-west-2.amazonaws.com", Some("us-west-2")),
            ("B-1.X.C2.KAFKA.EU-WEST-1.AMAZONAWS.COM.", Some("eu-west-1")),
            ("kafka.internal.example.com", None),
            ("127.0.0.1", None),
            ("s3.amazonaws.com", None),
        ] {
            assert_eq!(region_from_host(host).as_deref(), want, "{host}");
        }
    }

    #[test]
    fn a_non_empty_answer_ends_the_exchange() {
        let mut session = MskSession {
            message: Some(b"{}".to_vec()),
            credentials: Arc::new(credentials(None)),
            chain: Arc::new(Chain::with(
                Arc::new(|_| None),
                logit_aws::creds::Endpoints::default(),
                std::time::Duration::from_secs(1),
            )),
        };
        assert_eq!(session.first().unwrap(), b"{}");
        assert!(session.first().is_err());
        assert_eq!(
            session.next(br#"{"version":"2020_10_22","request-id":"r"}"#).unwrap(),
            Step::Done
        );
        assert_eq!(session.next(b"not json").unwrap(), Step::Done);
        assert!(session.next(b"").is_err());
    }
}
