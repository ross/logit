# TLS test fixtures

Self-signed, test-only certificates used by `logit-outputs`/`logit-inputs`/`logit-cli`'s TLS tests.
**Not used at runtime** — nothing under `crates/` reads this directory outside `#[cfg(test)]` code.
The private keys here are deliberately public; never reuse them for anything real.

Regenerate with `./regen.sh` (see that script's own comment for why and when). Every key is an
unencrypted PKCS#8 EC P-256 key, and every certificate is valid for 100 years (the current set from
2026-09-03 to 2126-08-10), so don't regenerate these for expiry; regenerate only when a test needs a
different shape, such as a new SAN or a new mutual-TLS case.

| File | What |
|---|---|
| `ca.pem` / `ca.key` | Test CA (`CN=logit-test-ca`). `ca_file` in TLS-client tests trusts this. |
| `other-ca.pem` / `other-ca.key` | An unrelated CA (`CN=logit-other-test-ca`) that signs nothing else here. Used to prove that a client trusting only this CA rejects the server leaf. |
| `server.pem` / `server.key` | Leaf signed by `ca.pem` (`CN=localhost`), SANs `DNS:localhost` + `IP:127.0.0.1`. Used by canned TLS servers in tests. |
| `client.pem` / `client.key` | Leaf signed by `ca.pem` (`CN=logit-test-client`), for mutual-TLS (`client_ca_file`) tests. |
