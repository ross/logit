---
created: 2026-10-07
updated: 2026-10-07
---

# TLS certificate reload: swappable resolvers and verifiers, triggered by a file poll and SIGHUP

## Status
Accepted

## Context
Every TLS component reads its PEM files once, at `logit run` startup
(`docs/known-gaps/intake.md`, "Every TLS-capable component's certificates are loaded once at
startup"). A 90-day certificate from Let's Encrypt or cert-manager therefore needs a restart, and a
restart loses the in-memory sink queues and any partial `aggregate` window, and leaves a gap on
every UDP port.

Two rotation tools dominate, and neither tells the process anything:

- **cert-manager**, through a mounted Kubernetes Secret, rewrites the files with no signal. The
  kubelet writes a new timestamped directory and swaps a `..data` symlink to it, so the configured
  path stays the same and resolves to a new file.
- **certbot** renews into `archive/` and repoints the symlinks in `live/`. A `--deploy-hook` can
  send a signal, but nothing requires one.

So a signal alone can't be the trigger. [ADR `otlp-tls-and-pooled-grpc-client`](otlp-tls-and-pooled-grpc-client.md)
left rotation out of scope and named the two shapes to reach for: a `ResolvesServerCert` hook, or a
SIGHUP-triggered reload. [ADR `signal-handling`](signal-handling.md) defined SIGHUP as a reopen of
file targets and listed certificates as not reloaded.
`docs/plans/live-reload-and-socket-handover.md`'s "TLS certificate reload" section found that
rustls has a seam on both sides that lets a rotation happen without reconnecting anything.

## Decision
`logit` swaps certificate, key, and CA material inside TLS configs it builds once, and checks the
files for new content on a process-wide poll and on every SIGHUP.

### Mechanism: build once, swap the inner pieces
Nothing reconnects, and no `rustls::ServerConfig`, `rustls::ClientConfig`, `TlsAcceptor`, or HTTP
client is rebuilt. Each config is built once around wrappers whose inner material is swappable.
The current material sits in a `RwLock<Arc<_>>`, and a swap replaces the `Arc`.

- **Server certificate and key.** The listener's `ServerConfig` is built with
  `with_cert_resolver(Arc<ReloadingCert>)` instead of `with_single_cert`. `resolve()` returns the
  current `Arc<CertifiedKey>`. A new handshake gets the new certificate; an open connection keeps
  its session.
- **Server `client_ca_file`.** A delegating `ClientCertVerifier` wraps a swappable
  `WebPkiClientVerifier`. Its `root_hint_subjects()` returns a borrowed
  `&[DistinguishedName]`, which can't borrow through a lock guard. So the wrapper keeps every hint
  list it has loaded, in an append-only store that lives as long as the process, and returns the
  newest. That costs a few KB per rotation.
- **Client `ca_file`.** A delegating `ServerCertVerifier` wraps a swappable `WebPkiServerVerifier`.
- **Client `cert_file` and `key_file`.** The `ClientConfig` is built with
  `with_client_cert_resolver(Arc<ReloadingCert>)`.

`ClientConfig::clone` shares those `Arc`'d resolvers and verifiers. So an HTTP sink's `reqwest`
client built with `use_preconfigured_tls(cfg.clone())`, and `otlp_out`'s pooled gRPC client built
with `hyper-rustls`'s `with_tls_config(cfg.clone())`, see each rotation with no client rebuild.

### `prometheus_in`'s scrape client moves onto the shared config
`prometheus_in`'s `scrape_tls:` client is the one TLS path that doesn't build a rustls config. It
hands PEM to `reqwest`'s own `Certificate` and `Identity` loaders (`apply_client_tls`), so it has no
seam to swap through. It moves to the shared `build_client_config` and passes the result with
`reqwest::ClientBuilder::use_preconfigured_tls`, as every HTTP sink already does. That also fixes
`apply_client_tls` reading only the first certificate of a `ca_file` bundle. The config keys stay
`scrape_tls:` and `bind_tls:`.

### Trigger: a content poll, and SIGHUP
- **A top-level `tls_reload_interval:`** sets one process-wide poll. It defaults to `60s`, and
  `0s` turns polling off. One poller serves every TLS component.
- **SIGHUP triggers an immediate check.** The poller subscribes to the reopen generation that
  `crates/logit-cli/src/signals.rs` bumps on each SIGHUP ([ADR `signal-handling`](signal-handling.md)).
  A SIGHUP checks the files even with `tls_reload_interval: 0s`, so a certbot deploy hook works
  with polling off.
- **A change is a change in content.** Each check reads every file in a component's set and
  compares the bytes with what it last attempted to load. Opening the configured path follows
  whatever symlinks exist at that moment, which covers cert-manager's `..data` swap, certbot's
  `live/` to `archive/` links, and an in-place rewrite, and doesn't depend on mtime granularity. A
  set is a few KB, so a read per minute costs nothing measurable.

### Failure policy: keep serving the old material
- **Unchanged content does nothing.**
- **Changed content loads the whole set before any swap.** A certificate and key pair is checked
  with `CertifiedKey::from_der`, which runs `keys_match`, so a key that doesn't match its
  certificate fails the load rather than every later handshake.
- **A failed load keeps the old material.** It counts one failure and sends a throttled warning
  diagnostic. The failed bytes are remembered as attempted, so the same bad content counts once,
  not once per poll.
- **A mid-rotation read is retried.** A tool that writes the certificate and the key separately
  can be read between the two writes. That load fails, and the next check sees new content when
  the key lands and loads again.
- **Startup still fails hard** on a file that's missing or doesn't parse, as it does today.
- **Readiness is never affected.** The old certificate keeps serving, so `/readyz`
  ([ADR `admin-readiness-endpoint`](admin-readiness-endpoint.md)) has nothing to report. An
  operator watches the failure counter and the expiry gauge instead.

### Where the code lives
The shared builders, the wrappers, and the poller (`TlsReloader`) go in a new
`logit_pipeline::tls` module. `logit-inputs` and `logit-outputs` already depend on
`logit-pipeline`, rustls is already in its dependency graph, and no crate gains an edge.
`build_server_config` moves there from `crates/logit-inputs/src/tls.rs`, and
`build_client_config` from `crates/logit-outputs/src/tls.rs`. Each component registers its file
set with the reloader along with its own `Diagnostics` and `Telemetry`, so a reload's telemetry is
attributed to the component that owns the files.

### Telemetry
- `logit.tls.reloads{outcome=reloaded|failed}`, a counter.
- `logit.tls.certificate.not_after{role=server|client}`, a gauge in unix seconds for the leaf
  certificate in `cert_file`. It's emitted at registration and after every successful reload, so
  an alert on it catches a rotation that never happened.

The gauge's value comes from a narrow, hand-rolled DER walk: Certificate, then TBSCertificate,
skipping the optional `[0]` version, the serial, the signature algorithm, and the issuer, then
Validity, then notAfter, as either `UTCTime` or `GeneralizedTime`. It handles long-form lengths and
returns nothing on anything it doesn't expect. Then the gauge is left out and the reload goes ahead
unaffected, because rustls has already validated the certificate. If the walk proves fragile,
`x509-parser` replaces it.

## Alternatives considered
- **inotify as the trigger.** Rejected. It would still need a poll as a fallback, for the reasons
  [ADR `file-tailing-and-docker-json-logs`](file-tailing-and-docker-json-logs.md)'s "Wake source:
  poll always, `inotify` as a lower-latency addition" gives: NFS and FUSE mounts where it doesn't
  fire, and `IN_Q_OVERFLOW`. A symlink swap, which is how both cert-manager and certbot rotate,
  changes no watched file, so a file watch misses it without also watching the directories the
  links resolve through. And the latency it buys is worthless against a renewal that happens
  30 days before expiry. It could be added later as a wake hint for the same content check.
- **A per-`tls:`-block interval.** Rejected. Nothing about one component's certificate needs a
  different cadence from another's, and one poller is simpler to reason about than several.
- **Rebuild the HTTP clients behind a swap.** Rejected. Every client already shares the config's
  `Arc`'d verifier and resolver through `ClientConfig::clone`, so a rebuild buys nothing, and it
  would drop each client's connection pool on every rotation. Only `prometheus_in`'s scrape client
  lacked the seam, and moving it onto the shared config is smaller than a rebuild path.
- **`arc-swap` for the swappable pieces.** Rejected. A `RwLock<Arc<_>>` read once per handshake
  costs nothing measurable, and needs no new dependency.
- **`x509-parser` for the expiry gauge.** Rejected for now. One field doesn't justify a parser and
  its dependency tree, and a walk that returns nothing on surprise can't break a reload. It stays
  the fallback.
- **Fail readiness on a failed reload.** Rejected. A failed reload leaves the process serving a
  valid certificate, and a `503` from `/readyz` would pull a working instance out of rotation.
- **A dedicated `logit-tls` crate.** Rejected. It would add a crate and its edges for code that
  fits in `logit-pipeline`, which both I/O crates already depend on, and
  [ADR `crate-layout-and-build-speed`](crate-layout-and-build-speed.md) leans against reshaping
  crate edges.

## Consequences
- Each TLS listener's and sink's `with_tls` builds through `logit_pipeline::tls` and registers its
  file set. `crates/logit-inputs/src/tls.rs` and `crates/logit-outputs/src/tls.rs` shrink to
  nothing, and `apply_client_tls` goes away.
- `logit_config::Config` gains `tls_reload_interval`, and `schema/logit.schema.json` changes with
  it.
- `logit-cli` creates the reloader, threads it through to every component beside the reopen
  generation, spawns its task, and aborts it at shutdown. Tests call its `check_now()` instead of
  waiting out an interval.
- `docs/design/internal-telemetry.md` gains the two metrics and the reload diagnostics.
- A rotated certificate reaches new connections only. An open connection keeps the certificate it
  handshook with until it closes, on both the listener and the sink side.
- Some things still aren't reloaded: the config itself, `insecure_skip_verify` (its verifier
  accepts everything and has no material), and the `webpki-roots` default trust store, which is
  compiled in. A change to any of them still needs a restart.
- `docs/known-gaps/intake.md`'s "loaded once at startup" entry closes, and `docs/deploying.md`
  documents the knob, the metrics, and the cert-manager and certbot recipes, when the behavior
  lands.
