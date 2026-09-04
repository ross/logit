# TLS test fixtures

Self-signed, test-only certificates used by `logit-outputs`/`logit-inputs`/`logit-cli`'s TLS
tests. **Not used at runtime** — nothing under `crates/` reads this directory outside `#[cfg(test)]`
code. The private keys here are deliberately public; never reuse them for anything real.

Regenerate with `./regen.sh` (see that script's own comment for why and when).

| File | What |
|---|---|
| `ca.pem` / `ca.key` | Test CA. `ca_file` in TLS-client tests trusts this. |
| `other-ca.pem` / `other-ca.key` | An unrelated CA, signs nothing else here — used to prove a client that doesn't trust it rejects the server leaf. |
| `server.pem` / `server.key` | Leaf signed by `ca.pem`, SANs `DNS:localhost` + `IP:127.0.0.1`. Used by canned TLS servers in tests. |
| `client.pem` / `client.key` | Leaf signed by `ca.pem`, for mutual-TLS (`client_ca_file`) tests. |
