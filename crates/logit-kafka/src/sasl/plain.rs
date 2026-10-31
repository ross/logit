//! SASL `PLAIN` (RFC 4616): one client message, `\0username\0password`, with an empty
//! authorization identity as the Java client sends it. The broker's answer carries nothing.

use super::{Authenticator, Session, Step};

pub struct Plain {
    pub username: String,
    pub password: String,
}

#[async_trait::async_trait]
impl Authenticator for Plain {
    fn mechanism(&self) -> &'static str {
        "PLAIN"
    }

    fn principal(&self) -> String {
        format!("user {:?}", self.username)
    }

    async fn start(&self, _host: &str) -> anyhow::Result<Box<dyn Session>> {
        Ok(Box::new(PlainSession { message: Some(message(&self.username, &self.password)) }))
    }
}

fn message(username: &str, password: &str) -> Vec<u8> {
    let mut out = Vec::with_capacity(2 + username.len() + password.len());
    out.push(0);
    out.extend_from_slice(username.as_bytes());
    out.push(0);
    out.extend_from_slice(password.as_bytes());
    out
}

struct PlainSession {
    message: Option<Vec<u8>>,
}

impl Session for PlainSession {
    fn first(&mut self) -> anyhow::Result<Vec<u8>> {
        self.message.take().ok_or_else(|| anyhow::anyhow!("PLAIN sends one message"))
    }

    fn next(&mut self, _server: &[u8]) -> anyhow::Result<Step> {
        Ok(Step::Done)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn the_message_is_an_empty_authzid_then_the_username_and_password() {
        let plain = Plain { username: "alice".into(), password: "pä55".into() };
        let mut session = plain.start("broker").await.unwrap();
        assert_eq!(session.first().unwrap(), b"\0alice\0p\xc3\xa455");
        assert_eq!(session.next(b"").unwrap(), Step::Done);
        assert_eq!(plain.principal(), "user \"alice\"");
    }
}
