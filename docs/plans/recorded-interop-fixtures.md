---
created: 2026-09-10
updated: 2026-09-12
---

# Recorded interop fixtures: real producers, captured once, replayed as tests forever

This repo has already paid for the lesson this workstream systematizes, twice:

- `demo/app/pages/syslog_handler.py` exists solely because Python's stdlib `SysLogHandler` appends
  a trailing NUL byte to every UDP datagram, which broke `logit`'s `json` transform — found by
  hand, by running the real demo against a real Python syslog client. Read that file's docstring.
- `docs/known-gaps.md`'s HAProxy-native-CBOR entry (search that file for "CBOR") records a
  multi-paragraph investigation of a real format's real framing behavior against `logit`'s current
  transports — the kind of finding that only comes from checking a decoder against something real.

Both were discovered by accident, by someone running real software against `logit` and noticing it
broke. A recorded-fixture corpus is the systematic version of that: real producers' actual bytes,
captured once and committed, replayed as tests forever — so a decoder's assumptions get checked
against reality instead of only against this team's own reading of a spec, and instead of only
against `logit`'s own encoder, which will happily agree with itself even if both sides share the
same misunderstanding.

## What this PR ships vs. what's follow-on work

**Shipped, real, and passing `script/cibuild`:**

- `script/record-fixtures` — the recording harness (§1 below), plus `tools/record-fixtures/` (its
  Python/YAML/config helpers).
- `testdata/interop/` populated with real captures from **four** producers: util-linux `logger(1)`
  (three fixtures — RFC 3164, RFC 3164 with a UTF-8 multibyte body, and RFC 5424), Python's
  `logging.handlers.SysLogHandler` via `demo/app/pages/syslog_handler.py`'s `NoNulSysLogHandler`
  (two fixtures), rsyslog itself forwarding a real message (one fixture), and the OpenTelemetry
  Collector's `file` exporter fed by `telemetrygen` (three fixtures — traces, logs, metrics). See
  `testdata/interop/syslog/README.md` and `testdata/interop/otlp/README.md` for the full
  provenance table.
- Seven Rust tests in `crates/logit-inputs/src/syslog.rs` (`interop_fixture_*`) reading every
  syslog fixture and asserting on decoded message content, tags, and severity — not byte-for-byte
  fixture equality (§2 explains why).

**Explicitly deferred, not forgotten:**

- **A Rust test consuming the OTLP/JSON fixtures.** `crates/logit-proto/src/otlp/json/` (the
  hand-written OTLP/JSON decoder these fixtures are meant to back-fill, per this workstream's
  original brief) does not exist on `main` as of this writing — it's PR #113
  (`feat/otlp-json-decode`, "feat: OTLP/JSON decoding in otlp_in"), open and unmerged. The three
  `testdata/interop/otlp/*.json` fixtures are captured, real, and ready; once #113 lands, add
  `interop_fixture_*`-style tests near `crates/logit-proto/src/otlp/json/traces.rs`'s existing
  `OTLP_TRACE_REQUEST_JSON` literal (see that file's `#[cfg(test)] mod tests`) reading these files
  the same way `syslog.rs`'s tests do. This repo's own workflow docs (`AGENTS.md`'s "To bring a
  branch... up to date" section) treat unmerged sibling branches as independent, in-flight work —
  this workstream was scoped the same way rather than rebasing onto or waiting for #113.
- **syslog-ng.** Not attempted. `rsyslog` (below) turned out practical to script reliably within
  the time budget; syslog-ng wasn't tried at all, so there's no empirical finding to record here
  beyond "not done yet." A `record_syslog_ng` function following `record_rsyslog`'s shape (real
  package, no official Docker Hub image assumed) is the natural next step.
  <br>_Related but explicitly out of scope for this corpus entirely, per this workstream's
  brief — real future work, not overlooked:_ statsd/DogStatsD producer fixtures, Docker
  json-file cross-version fixtures, and `logit_in`/`logit_out` cross-version compatibility
  fixtures. None of these have any code or design started here.
- **OTLP/protobuf and OTLP/HTTP fixtures.** Every OTLP fixture here is JSON, by the design in §1 —
  capturing protobuf would mean a genuinely different (raw-byte, not `file`-exporter) capture
  shape. `crates/logit-proto/src/otlp/mod.rs`'s existing hand-built `OTLP_TRACE_REQUEST` literal
  already covers the protobuf decode path, so this wasn't judged worth the added harness
  complexity for a first PR — see `testdata/interop/otlp/README.md`'s own "not covered" section.
- **TCP-framed syslog fixtures.** `syslog_in` is UDP-only today
  (`crates/logit-inputs/src/syslog.rs`'s module doc), so there's nothing to exercise yet — but
  `raw_capture.py` already implements a `--proto tcp` capture mode for when that changes.

## Amendment (2026-09-12): collectd, recorded

A **fifth producer** landed with W4a of [`collectd-binary-relay.md`](collectd-binary-relay.md):
`record_collectd` in `script/record-fixtures` (plus `tools/record-fixtures/collectd.conf` and
`collectd-entrypoint.sh`), three captured datagrams in `testdata/interop/collectd/`, and five
`interop_fixture_*` tests in `crates/logit-inputs/src/collectd.rs` consuming them. It follows
`record_rsyslog`'s shape exactly — a real Debian package (`collectd-core`) installed fresh into
`debian:bookworm-slim` at record time — for the same verified reason: the collectd project's own
Docker Hub namespace holds a single repository, `collectd/ci`, which is a *build-environment* image
(one tag per distro, carrying what it takes to compile collectd), not a runnable daemon.

Two findings worth recording alongside §1's, both specific to how collectd's `network` plugin
behaves rather than to what its protocol says:

- **A datagram is not a read cycle, and `--count 3` is not "three measurements."** The `network`
  plugin packs value lists into a send buffer and only puts a datagram on the wire once the next
  list would not fit under `MaxPacketSize` (default 1452) — or at shutdown, which this corpus never
  sees, since the capture stops on its own datagram count seconds earlier. So the recorded fixtures
  are ~1.3 KB each, just under that cap, rather than the few hundred bytes every syslog fixture is,
  and each one carries
  ~25 value lists from several read cycles. This is what makes three datagrams a *good* size for
  this corpus rather than an accident: sender-side identity elision (one Host part for ~25 lists),
  a read cycle split across a packet boundary, and identity being re-stated in full at the start of
  the next datagram are all things only a packed datagram can show, and all three are exactly what
  `crates/logit-proto/src/collectd/decode.rs`'s sticky-identity state machine has to get right.
  `Interval 1` in the config is there to make the buffer fill in seconds instead of minutes.
- **Which lists land in which datagram is not stable across a re-record** — more so than for any
  other producer here, since it depends on plugin read-thread scheduling. §1's "captures aren't
  byte-reproducible, and that's fine" therefore applies with extra force: the consuming tests
  assert a list's data-source count and kinds, its `collectd.*` identity, its interval and that the
  whole datagram decodes with no diagnostics — never a measured value, a timestamp, or a per-file
  list count.

The follow-on list below is otherwise unchanged: syslog-ng is still not attempted, and
statsd/DogStatsD, Docker json-file and `logit_in`/`logit_out` cross-version fixtures are still not
started. One item is now *scheduled* rather than merely unstarted: a collectd `threshold`-plugin
capture, exercising the notification parts (`Message` 0x0100 / `Severity` 0x0101), lands with W5 of
`collectd-binary-relay.md`, which is the workstream that teaches the decoder to read them.
`testdata/interop/collectd/README.md`'s own "what isn't covered here (yet)" section carries the
rest (signed/encrypted traffic, multi-host forwarding, legacy second-resolution time parts,
COUNTER/ABSOLUTE data sources).

## 1. How captures are recorded, reproducibly

**The load-bearing design decision: `script/record-fixtures`, a script, not folklore** — someone
six months from now runs it and gets an equivalent (not necessarily byte-identical; see below)
capture, the same way `script/protogen` and `testdata/tls/regen.sh` already establish as this
repo's shape for "committed artifact, regenerated by a script, reviewed as a diff, never re-run by
CI" (`docs/adr/committed-pregenerated-otlp-protobuf.md` states that reasoning explicitly, and this
plan follows it rather than re-deriving it).

### Two different capture shapes, for two different reasons

**Syslog: a real raw-byte capture, via a purpose-built generic listener.**
`tools/record-fixtures/raw_capture.py` binds a UDP or TCP socket in a throwaway `python:3.12-slim`
container and writes whatever arrives straight to a file — no parsing, no validation, no
re-encoding. One file per UDP datagram (syslog/UDP has no framing beyond "one datagram is one
message"); one file per accepted TCP connection's full byte stream (for when `syslog_in` grows TCP
support, RFC 6587 octet-counting/non-transparent-framing needs exactly this). This is the same
listener for every syslog producer — the producer side is a `record_<name>()` bash function in
`script/record-fixtures` that starts the listener, generates traffic, and tears down; the *only*
"a small, obvious diff" a new producer needs is one more such function plus a name added to `all`.
An HTTP capture mode isn't implemented (nothing needs it yet); `raw_capture.py`'s own docstring
says where to add one.

**OTLP: the OpenTelemetry Collector's own `file` exporter, deliberately not a raw-byte capture.**
`tools/record-fixtures/otel-collector-config.yaml` configures a real
`otel/opentelemetry-collector-contrib` with an `otlp` receiver (gRPC + HTTP) and three `file`
exporters (one per signal), fed by `telemetrygen` — the OpenTelemetry project's own
load-generation tool, a real SDK-backed exporter, not a hand-built payload. The `file` exporter's
default marshaler **is** pdata's JSON marshaler, the normative implementation of OTLP/JSON — there
is no more "real" OTLP/JSON underneath it to capture instead, and recording it this way means the
fixture is valid regardless of whether the sender used gRPC or HTTP, protobuf or JSON, on the wire
to the Collector. A raw-byte capture of the gRPC leg would mean stripping gRPC's own 5-byte
message-framing prefix to get at the protobuf underneath, for no benefit `OTLP_TRACE_REQUEST`
(`crates/logit-proto/src/otlp/mod.rs`) doesn't already provide.

### Why captures aren't byte-reproducible, and why that's fine

Re-running `script/record-fixtures` will not reproduce the exact bytes committed here — container
hostnames land in RFC 3164's HOSTNAME field, timestamps are real wall-clock time, RFC 5424's
`syncAccuracy` is a real measured value, and telemetrygen's trace/span ids are randomly generated
every run. This is *expected*, not a flaw to fix: the corpus is "a real capture from a real
producer, checked into git," not a byte-stable golden file. `crates/logit-inputs/src/syslog.rs`'s
`interop_fixture_*` tests assert on decoded, identifiable values (message content, tag, severity)
for exactly this reason — see `testdata/interop/README.md`'s "Consuming these fixtures" section.

### Findings worth recording so they don't need rediscovering

Three real obstacles came up building this, each one specific enough to be worth naming rather
than leaving as an undocumented "just so" in the script:

- **Busybox's `logger` applet has no remote-server support at all.** `-n`/`-P` aren't recognized
  (confirmed against `busybox:1`'s own `--help`) — so the `logger(1)` producer uses
  `debian:bookworm-slim`'s util-linux `logger` instead, which is the actual `/usr/bin/logger` on
  essentially every real Linux distribution anyway, not a downgrade.
- **SELinux (this repo's dev machines can be Fedora) blocks an un-relabeled bind mount.**
  Confirmed empirically: a container reading or writing a plain `-v host:container` mount against
  an SELinux-enforcing host gets `Permission denied` even when the host directory is
  world-writable. `script/protogen` already carries the fix (`-v "${ROOT}:/work:z"`);
  `script/record-fixtures` follows the same `:z` convention on every bind mount.
- **`otel/opentelemetry-collector-contrib` runs as a fixed non-root uid (10001)**, unrelated to
  whatever uid runs the script — its `file` exporters fail closed (`open /output/logs.json:
  permission denied`) against a normally-created (`mkdir`, mode 755) output directory.
  `record_otlp` `chmod 777`s the output directory before starting the container.
- **There's no official `rsyslog/*` Docker Hub image** (verified: `rsyslog/rsyslog_deb12`, a name
  that shows up in some Stack Overflow answers, doesn't exist; nothing else under `rsyslog/` on
  Docker Hub is maintained by the rsyslog project either). `record_rsyslog` installs the real
  Debian-packaged `rsyslogd` fresh into `debian:bookworm-slim` at record time instead — the actual
  upstream-built binary Debian ships, just fetched at record time rather than baked into an image.
  Two sub-findings from getting this working: mounting a config file straight over the package's
  own `/etc/rsyslog.conf` makes `apt-get install` hang forever on an interactive dpkg conffile
  prompt (`DEBIAN_FRONTEND=noninteractive` does **not** suppress this — that env var reaches
  debconf, not dpkg's own conffile-conflict prompt), fixed by mounting at
  `/etc/rsyslog-fixture.conf` and passing `-f` explicitly; and the ruleset filters to
  `$programname == 'logit-fixture'` specifically, so the fixture captures the one test message,
  not rsyslogd's own startup notice (also delivered over the same `imuxsock` path).

## 2. Per-fixture provenance metadata

**A README.md table per fixture subdirectory** (`testdata/interop/syslog/README.md`,
`testdata/interop/otlp/README.md`), the same shape `testdata/tls/README.md` already uses for this
repo's other committed-artifact-with-provenance directory. Each row carries: producer name +
version (and, for the syslog producers, exactly which real binary produced it, down to the Debian
package version where relevant), the invocation used, the capture date, and — the column that
matters most for a corpus meant to show what's covered and what isn't — the specific construct
that fixture exercises (RFC 3164 vs. 5424, a UTF-8 multibyte body, RFC 5424 STRUCTURED-DATA, a
forwarder's own reformatting, an OTLP batch's `droppedAttributesCount`, ...). Each directory README
also carries an explicit "what isn't covered here (yet)" section, so a reader doesn't have to infer
absence from a table's silence.

**Why a table, not a per-fixture JSON/YAML sidecar file:** a sidecar means two files per fixture
(and a schema to keep them consistent) for a corpus this repo intends to stay small (see §3) — a
handful of producers, a handful of fixtures each. A markdown table in one README per subdirectory
keeps provenance next to a human-readable "what's covered" narrative, matches the one precedent
this repo already has (`testdata/tls/README.md`), and stays reviewable as a normal PR diff.
Reconsider this if the corpus grows into dozens of fixtures per producer, where a table becomes
unwieldy — not a concern yet.

## 3. Size discipline

**Rule of thumb: a few hundred bytes per syslog fixture, ~1.3 KB per collectd fixture (one packed
datagram, just under collectd's 1452-byte `MaxPacketSize` — the producer, not this corpus, chooses
that size), low single-digit KB per OTLP fixture, whole directory well under 100 KB.** It was ~4 KB
across nine files when this plan was written, and is ~8.9 KB across twelve at head, the three
collectd captures being the difference. Justification: these fixtures exist to exercise decoder
*paths* — one or two representative messages per construct — not to be a load-testing dataset or a
corpus of "everything a producer can possibly emit." `record_otlp` enforces this at capture time by
using `telemetrygen`'s fixed-count flags (`--traces=3`/`--logs=3`/`--metrics=3`) rather than an
open-ended `--duration`, and `record_collectd` by capturing a fixed count of *datagrams*, so a
re-record can't accidentally balloon the fixture based on how long the container happened to take
to start.
A per-fixture size creeping up over time (padding, verbose repeated attributes, an accidentally
large `--count`) is a signal something needless crept in, not a corpus that's naturally growing.

## 4. Scope and priority

Syslog first, matching this demo's real dependency surface: three different real producers
already feed `logit` syslog traffic in one form or another (rsyslog-style access logs via
HAProxy/nginx's `syslog:` writer into `syslog_in`, Python's `SysLogHandler` in `demo/app`, and
whatever `docker_in`'s json-file format effectively is), and RFC 3164 framing varies meaningfully
across real implementations — exactly the kind of variation a corpus like this is for. OTLP
second, backing the hand-written literals in `crates/logit-proto/src/otlp/mod.rs` (protobuf,
already on `main`) and, once #113 lands, `crates/logit-proto/src/otlp/json/*.rs` (JSON, not yet on
`main` — see "What this PR ships vs. what's follow-on work" above). HAProxy/nginx access-log
capture is deliberately **not** a fixture-corpus concern — that's already covered by the existing
`demo/`/`examples/nginx/` integration test path, and duplicating it here would just be a slower,
less realistic copy of what `demo/` already proves. statsd/DogStatsD, Docker json-file
cross-version fixtures, and `logit_in`/`logit_out` cross-version compatibility fixtures are real
future work, named explicitly above, not silently out of scope.

## 5. Fuzz seeds — positioned for, not wired up

`docs/known-gaps.md` already carries a `cargo-fuzz` entry (`crates/logit-proto/tests/robustness.rs`'s
seeded mutation tests are the interim substitute — nightly Rust is the blocker, per
`docs/adr/containerized-development.md`'s stable-only toolchain) and
`docs/plans/native-transport.md` names real `cargo-fuzz` targets as a `known-gaps.md` follow-up,
not built there either. No `fuzz/` directory exists anywhere in this repo yet (confirmed: no
`fuzz/`, no `cargo-fuzz` reference outside those two docs). This corpus is well-positioned to
double as a fuzz seed corpus once that lands — real, minimal, protocol-diverse inputs are exactly
what a fuzzer seeds from — but building that integration now would be speculative against a
toolchain gap this repo has already decided not to pay for yet. Noted here as intent, not started.

## Consuming tests, and what they assert

`crates/logit-inputs/src/syslog.rs`'s `interop_fixture_*` tests (seven, one per syslog fixture)
read `testdata/interop/syslog/*.raw` via a `testdata_dir()`-style helper (mirroring
`crates/logit-inputs/src/otlp.rs`'s existing TLS-fixture helper of the same shape) and assert on
`message_str`, `syslog.tag`, and severity — not on the fixture's raw bytes staying stable across a
re-record. One of them (`interop_fixture_python_syslog_handler_json_body_is_clean_for_the_json_transform`)
goes one step further and round-trips the decoded message through `serde_json::from_str`, which is
the actual empirical check that `NoNulSysLogHandler`'s fix still holds against Python's real
handler output today, not just against a hand-typed literal shaped like it.
