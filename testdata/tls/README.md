# TLS test fixtures

Self-signed, test-only certificates used by `logit-pipeline`/`logit-outputs`/`logit-inputs`/`logit-cli`'s
TLS tests.
**Not used at runtime** — nothing under `crates/` reads this directory outside `#[cfg(test)]` code.
The private keys here are deliberately public; never reuse them for anything real.

Regenerate with `./regen.sh` (see that script's own comment for why and when). It takes a group
name: `./regen.sh rotation` regenerates only the reload tests' set and leaves the rest alone.
Every key is an unencrypted PKCS#8 EC P-256 key, and every certificate except `utctime.pem` is
valid for 100 years (the `base` set from 2026-09-03 to 2126-08-10, the `rotation` set from
2026-10-07 to 2126-09-13), so don't regenerate these for expiry; regenerate only when a test needs
a different shape, such as a new SAN or a new mutual-TLS case.

| File | What |
|---|---|
| `ca.pem` / `ca.key` | Test CA (`CN=logit-test-ca`). `ca_file` in TLS-client tests trusts this. |
| `other-ca.pem` / `other-ca.key` | An unrelated CA (`CN=logit-other-test-ca`). Used to prove that a client trusting only this CA rejects the server leaf, and signs the `rotation` set's `*-other` leaves. |
| `server.pem` / `server.key` | Leaf signed by `ca.pem` (`CN=localhost`), SANs `DNS:localhost` + `IP:127.0.0.1`. Used by canned TLS servers in tests. |
| `client.pem` / `client.key` | Leaf signed by `ca.pem` (`CN=logit-test-client`), for mutual-TLS (`client_ca_file`) tests. |
| `server-b.pem` / `server-b.key` | A second leaf signed by `ca.pem`, same subject and SANs as `server.pem`: a renewed certificate, for reload tests. |
| `server-other.pem` / `server-other.key` | Leaf signed by `other-ca.pem`, same subject and SANs as `server.pem`, for a client whose trusted CA rotates. |
| `client-other.pem` / `client-other.key` | Leaf signed by `other-ca.pem` (`CN=logit-other-test-client`), for a listener whose `client_ca_file` rotates. |
| `utctime.pem` | Self-signed (`CN=logit-utctime`), valid from 2026-01-01 to 2049-12-31T23:59:59Z. A `notAfter` before 2050 is encoded as `UTCTime`, so it tests the expiry reader's `UTCTime` branch. Its key isn't kept. |
