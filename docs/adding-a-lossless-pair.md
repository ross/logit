# Adding a lossless transit pair

A lossless pair is a `<proto>_in` listener and a `<proto>_out` sink for one wire protocol, built so
that `<proto>_in -> <proto>_out` hands a consumer of that protocol what the producer sent, modulo a
named list of normalizations ([ADR `lossless-transit`](adr/lossless-transit.md)). Eleven pairs
exist. This guide is the order of work that has landed them, the files each step touches, and the check
that fails when a step is missed. It's written for a contributor adding the twelfth. For a sink or
listener with no partner (`s3_out`, `cri_in`), the per-layer sections still apply; skip the pair
round trip and the like-pair flows.

The one thing to take away: "lossless" is a property proven by tests at four layers, not a design
claim. A pair is done when the fixed-point test, the pair round trip, the transit spec's like-pair
flows, and a run against the real system all agree, and every degradation the pair can't avoid is
counted and written down.

## Before any code

1. **Read the survey entry.** [`docs/design/telemetry-landscape.md`](design/telemetry-landscape.md)
   is the list of every wire feature a protocol can carry. A feature with no entry there gets one in
   the same PR as the ADR, because the fixed-point corpus is built from that list.
2. **Check what's decided.** [`docs/adr/README.md`](adr/README.md) and
   [`docs/known-gaps/`](known-gaps/README.md) may already hold a decision or a deliberate limit for
   this protocol. The relay ADRs for Datadog, Splunk, and InfluxDB each have a "Verified
   destination behavior" section; read the nearest one before designing a response-class table.
3. **Pick a stream key** (`[a-z0-9-]`, not in use) and plan the workstreams. See
   [Branches and PR titles](../AGENTS.md#branches-and-pr-titles) for the branch and title forms.
4. **W0 is an ADR and a plan**, nothing else. The ADR is copied from
   [`docs/adr/TEMPLATE.md`](adr/TEMPLATE.md) and named after the decision; the plan goes in
   [`docs/plans/`](plans/README.md). Both get an index row. A W0 that also carries code gets its
   design reviewed through the code, which is the wrong order.

The ADR has to settle these, each as a sentence a reviewer can answer yes or no to:

- **The mapping.** Every wire field to its home on `Event`: a typed field, a public attribute, or a
  private `_logit.<proto>.*` carrier ([ADR `private-attribute-namespace`](adr/private-attribute-namespace.md)).
  A fact only the pair's own family reads is a carrier. A fact another component must act on is a
  `_logit.metrics.*` flag ([ADR `metric-flags`](adr/metric-flags.md)), never a carrier read across
  families ([ADR `protocol-knowledge-stays-in-family`](adr/protocol-knowledge-stays-in-family.md)).
  The sender's host goes on `host.name` ([ADR `process-hostname`](adr/process-hostname.md)).
- **The permitted normalizations**, as a numbered list. This list is what the fixed-point test
  asserts against, so it has to be complete before the test is written. Expect review to find a
  row the code performs that the list doesn't name.
- **What the model can't carry.** A wire feature `Event` has no home for is tracked debt in
  `docs/known-gaps/<proto>.md`, not an accepted codec limit.
- **The acknowledgment rule.** What the listener's acknowledgment means, which is "in every
  downstream inbox", never "delivered" ([ADR `delivery-semantics`](adr/delivery-semantics.md), item 3).
  If the wire can't express a negative acknowledgment, the exception is an amendment to that ADR,
  as `kafka_in`'s commit past a dropped record is.
- **The sink's posture and response-class table.** Which responses are `Clean`, `Ambiguous`,
  `Rejected`, and `Refused` ([ADR `sink-fault-classes`](adr/sink-fault-classes.md)), whether a
  duplicate is absorbed at the destination, and the transport timeout that bounds every attempt.
- **The encoder shape.** `Encoder` (one blob per batch), `FramedEncoder` (one message per record
  into a `MessageBuf`, [ADR `framed-encoder`](adr/framed-encoder.md)), or `SignalEncoder` (one
  payload per signal). The choice follows where the wire's batch edges fall, not convenience.
- **What's shared.** Code a second kind will need (a broker connection, an auth scheme, a signing
  implementation) goes in its own crate from the start. `logit-kafka` and `logit-aws` each exist
  because a sink's module turned out to be a listener's dependency too.
- **Where the pair stops.** Which real systems the harness verifies against, and which topologies
  are out of scope. A leg that no run has verified is marked unverified in the docs, never implied.

The plan carries the workstream table (branch, PR title, size, depends on, status), the mechanics
paragraph (stacked PRs, review before fix rounds, the test-reverted rule, the prose sweep), and a
"Findings" section each workstream appends to as it lands.

## The workstreams that have worked

Every pair has landed through a shape like this, stacked as one `gh stack` with each PR based on
the one before it. Sizes are from the `kafka_in` plan.

| Workstream | Contents | Depends on |
|---|---|---|
| W0 | ADR, plan, survey entry, index rows | — |
| W1 | A shared crate or core type, with no behavior change | W0 |
| W2 | The codec in `logit-proto`: decoder, encoder, module-doc mapping table, fixed-point test, robustness test, fuzz target | W1 |
| W3 | The listener, with its graph rule, fixture, and round trip | W2 |
| W4 | The sink, with its response-class table, graph rule, fixture, and the pair round trip | W3 |
| W5 | The transit spec: source cases, sink column, like-pair flows, generated docs | W4 |
| W6a | The interop harness against the real system, and the recorded corpus | W5 |
| W6b | The soak scenario and its wire or stand-in shape | W5 |

W6a and W6b share no file, so they're siblings off W5. The codec and the two kinds split into
separate workstreams because each is a review in its own right; a listener and sink in one PR is
the shape review rounds multiply on. When the sink already exists and holds the encoder, W2 is its
move into `logit-proto` beside the new decoder, as `influxdb_out`'s was.

Review runs on each PR before its fix rounds, from the reviewer session, and the stack is brought
up to date with `main` by merging, never rebasing. A merge of `main` can move a graph rule number
out from under the branch (`s3_out` took rule 100 while `kafka_in` held it); the renumber touches
`graph.rs`, the registry's comments, the ADR, the plan, and
[`pipeline-graph.md`](design/pipeline-graph.md)'s rule list in one commit.

## Each layer, and the check that fails when it's missed

### Shared crate

A new crate is a workspace member in the root `Cargo.toml`, a row in
[AGENTS.md's crate list](../AGENTS.md#where-things-live), and a dependency the fuzz workspace can
see if any target needs it. The rule for what goes there: anything two kinds read. A fake peer
(`logit-kafka`'s fake broker) lives behind a `test-util` feature. `script/lint`'s `cargo tree` guard
checks only `logit-pipeline`'s test features, so a new crate's feature gets its own guard line
there.

### Codec (`crates/logit-proto/src/<proto>/`)

- The decoder implements `logit_proto::Decoder`, or, where records arrive through a client rather
  than a socket, a function the listener calls (`kafka_in`'s `decode_record_batches`). It never
  summarizes: raw timer values and set members stay raw until an `aggregate` decides.
- The encoder implements one of the three shapes. A `FramedEncoder` counts every per-message drop.
- `mod.rs`'s doc is the canonical copy of the mapping table, the permitted-normalization list, and
  a "Deviations from <reference>" section. Every other document summarizes it and links to it.
- Every length read from the wire is bounded before anything is reserved from it. A
  `Vec::with_capacity` from a wire count has been a blocking finding more than once; clamp to a
  constant and let the budget charge the rest.
- Tests: a fixed-point suite, `crates/logit-proto/tests/<proto>_fixed_point.rs`, over a corpus
  that exercises every survey feature; a robustness section in
  `crates/logit-proto/tests/robustness.rs` that pins the worst-case allocation under a hostile length; and an interop test that decodes
  the recorded corpus and asserts identifiable values, never whole-file bytes.
- A fuzz target: `fuzz/fuzz_targets/<proto>.rs`, a `[[bin]]` in `fuzz/Cargo.toml`, a row in
  `script/unsafe-check`'s `FUZZ_TARGETS` table with its `max_len` and allocation limit, a seed
  source in `fuzz/seedgen`, the regenerated `fuzz/seeds/<target>/`, and a bullet in
  [ADR `out-of-ci-fuzzing`](adr/out-of-ci-fuzzing.md)'s target list. The fuzz crate depends on
  `logit-core` and `logit-proto` only, which is why a record codec that started in a sink crate
  moves before its target can exist. `script/lint` fails on a stale seed and on a target that
  doesn't compile; nothing fails on a target missing from `FUZZ_TARGETS`, so check that row by hand.

### Config (`crates/logit-config/src/lib.rs`)

- One `ComponentKind` variant per kind, deriving `Serialize + Deserialize + JsonSchema`, with
  `deny_unknown_fields` on every fixed-key struct ([ADR `config-yaml-jsonschema`](adr/config-yaml-jsonschema.md)).
- Field docs are operator docs: they render into `schema/logit.schema.json`, so no rule numbers,
  ADR paths, or cross-references.
- TLS and auth keys on a two-mode kind are prefixed by the mode they serve, as the Prometheus kinds'
  are. No bare `tls:` on a kind that could later grow a second mode.
- Run `script/schema` and commit the result. `committed_schema_is_current` fails on any drift,
  including a doc-comment edit. Run it only on a tree that builds; on a broken tree it truncates
  the file.

### Listener (`crates/logit-inputs/src/<proto>*`)

- Implements `logit_pipeline::Input`. `bind` opens the socket in the pre-pass; `run` never binds.
- Acknowledges a batch only once it's in every downstream inbox, through `Fanout`'s
  deadline-carrying sends, and never acknowledges a batch no consumer took. An HTTP listener answers a bounded wait
  with the protocol's busy status and counts `input.batches.deferred`.
- A loss is `*.dropped` with a `reason`; an outcome the config asked for is `discarded`, `skipped`,
  `omitted`, or `evicted` ([ADR `telemetry-loss-vocabulary`](adr/telemetry-loss-vocabulary.md)).
  A loss reported only by a diagnostic gets a counter beside it.
- Takes the shared blocks where they apply: `receive:` on a datagram listener, `peer:` and
  `proxy_protocol:` on a socket listener, `forwarded:` and the family-prefixed `bind_auth:` on an
  HTTP listener, `tls:` through `logit_pipeline::tls` so the reloader covers it.
- Never prints its config, or any struct that could hold a `!env`-inlined secret, in an error or log.
- Tests wait on observables through `logit_pipeline::test_util`, never a sleep
  ([ADR `test-timing-and-observables`](adr/test-timing-and-observables.md)).
- Registration: the kind arm in `crates/logit-cli/src/pipeline.rs`, a graph rule in
  `crates/logit-pipeline/src/graph.rs` with a test that rejects each misconfiguration, the rule's
  row in [`pipeline-graph.md`](design/pipeline-graph.md)'s validation list, and a runnable
  `fixtures/<proto>-*.yaml`. `every_shipped_config_loads_and_validates` loads every fixture with a
  fake `!env` lookup; a new `!env` name goes in that lookup and in `script/validate`'s exports.

### Sink (`crates/logit-outputs/src/<proto>*`)

- Implements `logit_pipeline::Output`. A wrapper around another `Output` forwards `observe_batch`
  and `observe_posture`.
- The module doc's response-class table is the contract: one row per response the sink
  distinguishes, each backed by a test and the destination's documentation. A destination fault read
  as `Rejected` loses data; a bad batch read as `Refused` wedges the sink. The shared HTTP driver's
  status-only default is the starting point, and each row the sink refines is a test.
- Every write and acknowledgment wait has a timeout. There's no retry budget and no exit on a send
  failure.
- Encode-side counts go through `Telemetry::gated`/`Diagnostics::gated` over a `BatchAccounting`
  gate, or they're counted again on every retry
  ([ADR `sink-send-path-and-attempt-accounting`](adr/sink-send-path-and-attempt-accounting.md)).
  A counter that fires for an event no request writes is the same bug from the other side.
- A destination limit that's measured and detectable is pre-filtered and counted, not sent to fail.
- A cross-protocol degradation (a kind the wire can't carry) is counted `degraded` or `skipped`,
  named in the module doc, and given a row in [`docs/known-gaps/mappings.md`](known-gaps/mappings.md)
  through the transit spec's generated region, never silently.
- The same registration list as the listener: registry arm, graph rule and test, rule row, fixture.

### Pair round trip

`crates/logit-cli/tests/<proto>_pair_round_trip.rs` runs the recorded corpus through the real
listener decoder, the real sink encoder, and back, asserting equality modulo the ADR's list. Along
with the fixed-point test it's the only enforcement of "lossless" outside the transit spec, and
it's where a normalization the ADR didn't name shows up first.

### Transit spec (`spec/transit/`, `crates/logit-cli/src/transit/`)

[`spec/transit/README.md`](../spec/transit/README.md)'s "Adding a source", "Adding a sink column",
"Adding a case", and "Adding a flow" are the procedure; what follows is what a pair adds and what
fails without it.

- **A source** is a directory `spec/transit/cases/<proto>/` with a `features/<proto>.yaml`, a
  `.meta.yaml` per case naming the wire file and the features it exercises, and a decode arm in
  `spec.rs`'s `decode_wire` keyed on the directory name before its first `-`. A case that reads
  third-party bytes points its `wire:` at `testdata/interop/`. The `.json` beside each case is
  generated; `script/transit gen` rewrites it and `transit::spec::cases` fails when it's stale.
- **A sink column** is the sink's name in `mapping.yaml`'s `sinks:` list, a cell in every row
  (class, operator sentence, reason, counter), a `SinkEncoder` arm in `wire.rs` with the pair's own
  decoder as the read-back, and the sink's file and protocol name in `docs.rs`'s `FILES` and
  `PROTOCOLS`. The sink's module doc gets a region opened by `<!-- transit:doc:<sink> -->` and
  `mappings.md` one opened by `<!-- transit:gaps:<sink> -->`, each closed by `<!-- transit:end -->`;
  `script/transit docs` fills them and `transit::docs::transit_docs` fails on a missing or stale region.
- **The like-pair flows** are `flows/<proto>-relay*.yaml`, one per sink mode that changes the
  outcome, each registered in `spec.rs`'s `flow_tests!` list; `check_cases` fails naming the
  flow and the line to add. Every cross-protocol flow into the new sink changes its expected and
  ledger files too, so the diff is large and generated.
- **The class check** fails a flow where an observed outcome differs from the cell's class, or a
  counter moved that no reached cell names. `coverage.md` lists the cells no flow reaches; a cell
  with no case is a feature the spec doesn't yet cover.
- The amendment to [ADR `lossless-transit`](adr/lossless-transit.md) ("an Nth like pair") names the
  pair, points at the ADR's normalization list, and names the flows that check it.

### Recorded corpus (`testdata/interop/<proto>/`)

`script/record-fixtures <proto>` runs real clients in pinned images and writes what they sent, with
a README naming each image, its digest, the date, and what each file holds
([`testdata/interop/README.md`](../testdata/interop/README.md)). A new producer is a
`record_<proto>()` function and an entry in the script's `all` list. Record a corpus before writing
the decoder's edge cases, not after: a recurring blocking finding is a reference client doing
something the ADR said it wouldn't. Nothing fails if a corpus goes unused, so the tests that read
it are written in the same PR. A capture that fingerprints the recording machine is a finding;
check the bytes before committing them.

### Interop harness (`tools/<proto>-interop/`)

The shape every harness shares: `script/<proto>-interop` on the host driving docker, a
`compose.yaml` with images pinned by tag and digest and no host ports, one `logit-<leg>.yaml` per
leg, a stdlib-only `check.py` printing one `PASS`/`GAP`/`FAIL` row per leg, a README with a
"Recorded run" section, and runs under `perf/results/<proto>-interop/<timestamp>/`. A `GAP` is a
difference a known-gaps entry records. Each leg config joins `script/validate` and the shipped-config
test only if its directory is in both globs and the file is named `logit-*.yaml`; a leg with no
`check.py` row has no verdict and nothing says so. The harness is out of CI, and the PR body
carries the run's row counts.

### Soak (`tools/soak/`)

A pair that ingests over a wire the harness doesn't judge yet adds a `wire = "<name>"`:
`WIRES` and `LISTENER_WIRES` in `scenario.py`, a `Wire` entry in `checks.py`'s `WIRES` with the
G, W, and counted-loss counters and the replay band a killed life is allowed, an ingress predicate
in `faults.py`, and an end-of-run wait in `driver.py` where the wire is pull-style. A sink whose
response-class table the harness should cue adds a `[target] shape` in `tools/soak/standin/` with
a vectors file and a body codec in `soaklib/standin.py`, and a `standin-<proto>` scenario whose
`respond` steps walk every row of the table naming the class each must read; AGENTS.md's invariant
list makes that scenario part of the table's contract. Every self-test function is added to the
tuple in `selftest.py`, or it never runs. `script/soak self-test` is the only check, and it's manual.

### Docs sweep

The files that have described every pair, each hand-maintained:

- `AGENTS.md`: the Inputs and Outputs tables, the lossless-pairs count and per-pair bullet, the
  harness bullets, the Environment table row for a new script, the crate list.
- `README.md` and `docs/OVERVIEW.md` where the pair changes what `logit` claims to do.
- `docs/deploying.md`: an operator section where the pair has operational rules.
- `docs/<proto>.md`: an operator guide, where the pair has topologies to choose between
  (`datadog.md`, `splunk.md`).
- `docs/design/internal-telemetry.md`'s counter tables: every new counter and reason. This is the
  single most-missed list across every pair's reviews.
- `docs/design/pipeline-graph.md`: the rule list and, for a new cancellation point, that table.
- `docs/design/data-model.md`'s "Well-known attribute names": every new carrier and public key.
- `docs/known-gaps/<proto>.md`, indexed from `known-gaps/README.md`: the residual debt by the
  plan's closing assessment.
- The ADR index's `updated` date on every ADR the pair amends.

The relative links in all of them are checked by `doc_links.rs`; nothing checks the counts.

## What review finds

A catalogue of 203 findings across the pair streams, ranked. The first two categories are 70 of them,
most in files the diff didn't touch.

1. **Stale prose** (48): a doc, module doc, or comment that still describes the behavior before the
   change. The sweep before each PR is a grep for every statement of the old behavior, including
   in ADRs the branch didn't open.
2. **A list with a wrong or missing row** (22): a normalization the code performs that the ADR
   doesn't name, a counter the mapping table cites that the sink never counts, a counter the sink
   counts that `internal-telemetry.md` doesn't list. Check every row of a list the change touches,
   not the row the change was about.
3. **A harness check that can't fail** (18): a ledger row or interop leg that passes whether or not
   the property holds. For each new row, break the property and watch the row fail.
4. **Reference fidelity** (15): the codec disagrees with the real destination or client. A
   recorded corpus and a run against the real system catch these; reasoning from the protocol
   specification doesn't.
5. **A test that can't fail** (13): revert the fix, run the test, restore the fix, and say so in the
   PR body.
6. **An external fact stated past the evidence** (11): "the destination dedupes" when one version
   was tested. Write what was verified and name the version.

Below those, each a handful: the wrong fault class for a response (which either loses data or wedges
the sink), an acknowledgment or commit that covers a record not yet accepted, an allocation sized
from a wire length, state lost across a cancelled `await` (hold it in locals until the step
completes, then install it), loss counted under a non-loss name, and a carrier read outside its
family. The blocking findings cluster in the harness, acknowledgment, fault-class, fidelity, and
lifecycle categories, which are also the ones a unit test rarely covers.

Findings that took more than one round had a pattern: the fix addressed the instance, not the
class. A skip that could still land on an unverified base, a count that was then over-counted, a
key spelling the fix didn't cover. When a finding names an instance, look for the class before
fixing it.

## Closing the stream

- Append each workstream's findings to the plan as it lands. A finding that changes a decision is
  an amendment to the ADR, dated, never an edit of the decision text.
- Write the residual debt into `docs/known-gaps/<proto>.md` from the plan's closing assessment,
  and set each plan row's status.
- A completed plan is eventually removed, once its findings have moved into the ADR and
  known-gaps; the ADR is the record that stays.
