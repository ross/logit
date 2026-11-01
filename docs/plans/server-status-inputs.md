---
created: 2026-10-31
updated: 2026-10-31
---

# Enabling plan: server status inputs — `nginx_stub_status_in` and `apache_status_in` on a shared scrape driver

## Context

nginx's `stub_status` page and Apache httpd's `mod_status` `?auto` page each serve a server's
connection, worker, and request counters in a plain-text layout of their own, and `logit` reads
neither without an exporter sidecar. This plan will extract `prometheus_in`'s scrape client into a
shared driver and build one input kind per server on it, under stream key `status`. Every decision
the workstreams share is in [ADR
`server-status-scrape-inputs`](../adr/server-status-scrape-inputs.md), cited below by its section
numbers; this plan says which workstream will land each one and how it will be tested.

Goals:

- A scrape driver that `prometheus_in`'s scrape mode will run on with no change in behavior apart
  from one validation change (ADR section 3), and that a new scrape kind will use by supplying a
  decoder (ADR section 2).
- `nginx_stub_status_in` and `apache_status_in` will read their pages into the Collector's metric
  names, with `prometheus_in`'s config keys, synthetics, and telemetry (ADR sections 3 to 10).
- Both parsers will be fuzzed and checked against pages real servers served.

Non-goals:

- A generic HTTP fetch input, or a JSON status endpoint (Angie's `/status/`, NGINX Plus's API).
- A HAProxy kind: HAProxy's built-in exporter goes through `prometheus_in` (ADR section 14).
- An `_out` for either page, or a transit-spec source (ADR section 1).
- Closing the scrape gaps the new kinds inherit (ADR section 15).

## Workstreams

Each workstream will be one PR on branch `status/w<N>`, stacked on the one before it.

| WS | Branch | Scope | Depends on | State |
|---|---|---|---|---|
| W0 | `status/w0` | the ADR and this plan, and their index rows | — | Planned |
| W1 | `status/w1` | `crates/logit-inputs/src/scrape.rs`: `ScrapeInput<D: ScrapeDecoder>` and the driver's share of the work (ADR section 2); `prometheus_in`'s scrape mode on it with its dialect choice behind its decoder, and the kind's name in the `headers:` and `scrape_tls:` construction errors; graph rule 40's checks and the reserved-header list made shared (section 3); its one intentional change to `prometheus_in`'s validation (section 3): `accept-encoding` joins the reserved headers; `prometheus.rs`'s module doc pointing at the driver | W0 | Planned |
| W2 | `status/w2` | `nginx_stub_status_in` end to end: `logit_proto::nginx_status` (section 9) and its module doc; the `nginx_status` fuzz target, seeds, and `FUZZ_TARGETS` row (section 12); a `script/record-fixtures nginx-status` producer and `testdata/interop/nginx-status/`; the `ComponentKind` variant, config struct, and schema; rule 9's table and rule 40 (section 3); the resource (section 4); the registry arm; a `stub_status` location on an internal listener in `fixtures/nginx/nginx.conf` and `fixtures/nginx-stub-status.yaml`; `AGENTS.md`, `docs/deploying.md`'s new section (section 14), `docs/design/internal-telemetry.md`, and the known-gaps entries (section 15) | W1 | Planned |
| W3 | `status/w3` | `apache_status_in` end to end: `logit_proto::apache_status` (section 10) and its module doc, with the `?auto` hint in its HTML-body diagnostic; the `apache_status` fuzz target, seeds, and `FUZZ_TARGETS` row; a `script/record-fixtures apache-status` producer and `testdata/interop/apache-status/`; the kind, config, schema, and registry arm; rules 9 and 40; an `examples/` config covering both kinds, with its rendered graph; the Apache and HAProxy parts of `docs/deploying.md`'s section; `AGENTS.md` and the known-gaps entries | W2 | Planned |

W3 depends on W2 for the `deploying.md` section it extends.

## Test coverage

Each workstream's tests will wait on observables through `logit_pipeline::test_util`, never a sleep
([ADR `test-timing-and-observables`](../adr/test-timing-and-observables.md)), and none will need a
running service: a test will serve its page from an in-process server, as `prometheus_in`'s scrape
tests do. Each regression test will be run once with its fix reverted, and the PR body will say so.

- **W1, the driver.** The proof of no change in behavior will be `prometheus_in`'s existing scrape
  tests (synthetics, every status class, `oversize`, TLS and its reload, header precedence,
  redaction, `instance` defaults, the missed-tick behavior) and its rule 40 tests passing unchanged
  apart from paths and type names, except for W1's one intentional validation change, which
  gets a test: a `headers:` entry naming `accept-encoding` rejected. The PR body will list every test whose body changed and why. New tests
  will cover only what the seam adds: a stub decoder's `Accept` value and body cap reaching the
  request and the read, and its kind's name in a `scrape_failed` message and in each construction
  error.
- **W2, `nginx_stub_status_in`.** Decoder unit tests over committed literals: the four-line page
  with and without its trailing spaces and with `\r\n`; Tengine's four-column form, its fourth
  number read and not emitted; each `parse_error` shape (a missing final `\n`, a missing or extra
  line, a reordered line, a number count that doesn't match the header, a non-digit); and a field
  past `i64::MAX` skipped and counted `out_of_range`, with the other six points emitted. A test will
  read every body under `testdata/interop/nginx-status/` and check each table row of ADR section 9:
  name, kind, value, unit, and `state`. The fuzz target will feed arbitrary bytes and check that the
  decoder never panics and that an accepted page gives seven events whose values read back from the
  input's digits. Input tests against an in-process server: a page's seven events plus three
  synthetics on one batch with `server.address` and `server.port`; `up` at `0` and no decoded events
  on a `parse_error`, and `up` at `1` with an out-of-range field; the 64 KiB cap as `oversize`.
  Resource tests: `url.path` taken from the path without its query string, two targets differing
  only in path giving two resources, and an IPv6 target's `server.address` without brackets
  (`http://[::1]:8080/nginx_status` gives `::1` and port `8080`). Graph tests for the shared rule 40
  checks on the new kind and for rule 9.
- **W3, `apache_status_in`.** Decoder unit tests: each key of ADR section 10's table, mapped and not
  mapped; `%g` forms (`.0689655`, `0`, an exponent); a malformed value skipped and counted with the
  rest of the page read; a repeated key keeping its last value, counted
  `logit.input.metrics.normalized{reason="duplicate_key"}`; an unterminated last line discarded and
  counted `logit.input.lines.dropped{reason="truncated"}`, the lines before it read; a body with no
  server-name line, as httpd 2.2 serves; an unknown key and a line with no `": "` ignored uncounted;
  the HTML page and an empty body as `parse_error`, the HTML page's diagnostic naming `?auto`; a
  non-ASCII byte in an ignored value, such as a server version, leaving the page readable; every
  scoreboard character, an unknown one, and the zero states. A test will read every body under
  `testdata/interop/apache-status/` and check it against its MPM and `ExtendedStatus` variant: the
  keys each variant omits emit nothing and count nothing. The fuzz target will check no panic and
  that each emitted value came from a listed key's value. Input tests as for W2, with the 1 MiB cap.
  A target without `?auto` passes `logit validate`. The example config will load under
  `every_shipped_config_loads_and_validates`.

## Mechanics

- The stack will be managed with `gh stack` from W0 on, and Ross will merge it.
- The reviewer peer session will review each PR, and its findings will be addressed before the next
  workstream builds on it.
- Commit messages carry a conventional type (`feat`, `fix`, `docs`, `test`, `chore`, `perf`); PR
  titles are `status/w<N>: <summary>`.
- A decision a workstream makes that the ADR doesn't hold will go into the ADR in that PR, and each
  workstream will set its row's state to "Built" with its PR number.
