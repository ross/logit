# Differential corpora

This directory holds test cases, each committed beside a reference implementation's reading of
it. A test runs `logit`'s decoder over each case and compares its verdict with the
reading, so the decoder is checked against the software it has to agree with, with none of that
software installed at test time.

It differs from [`../interop/`](../interop/README.md) in three ways:

- **Generated or hand-built, not captured.** A generator script builds every case from fixed
  literals, or a case is a hand-built body committed as an input, so a case can be a shape no
  recorded sender happened to produce. A corpus can also read recorded captures under
  `../interop/` in place, beside its own cases.
- **Deterministic.** The generator runs in images pinned by tag and digest and writes the same
  bytes on every run, so a regenerated corpus that differs from the committed one is a finding.
- **It carries a reference reading.** Each case's JSON file holds what the reference made of the
  bytes, beside the verdict `logit` must give.

For the decision, see [ADR `out-of-ci-fuzzing`](../../docs/adr/out-of-ci-fuzzing.md)'s "Seeds and
differential corpora". The fuzz seed generator, `fuzz/seedgen`, also reads every case here as a
starting seed.

**Not used at runtime.** Nothing under `crates/` reads this directory outside tests.

## Corpora

| Directory | Decoder | Reference | Test |
|---|---|---|---|
| [`graphite-pickle/`](graphite-pickle/README.md) | `crates/logit-proto/src/graphite/pickle.rs`, carbon's pickle reader | CPython 3.12's and Python 2.7's `pickle`, and carbon 1.1.10's pickle receiver | `crates/logit-proto/tests/graphite_pickle_differential.rs` |
| [`prometheus-text/`](prometheus-text/README.md) | `crates/logit-proto/src/prometheus/text.rs` and `assemble.rs`, the text 0.0.4 and OpenMetrics decoder | Prometheus 3.14.0's `model/textparse`, over hand-built bodies and `../interop/prometheus-scrape/`'s recorded ones | `crates/logit-proto/tests/prometheus_text_differential.rs` |

## Regenerating and checking

Run `script/differential <corpus>` to regenerate a corpus in place, then review
`git diff --stat testdata/differential/` and commit. Run `script/differential <corpus> --check` to
regenerate into a temporary directory outside the repository and compare: it exits 1 when the
committed corpus is stale, that is, when a generator or the decoder source it reads has changed
since the corpus was written.

Both run on the host and drive docker, like `script/record-fixtures`. Neither is part of
`script/cibuild`. A generator writes only into its corpus's directory, or the temporary directory
under `--check`.

A generator self-checks before it writes anything: each corpus's README says what it checks.

## Size

The tree is about 480 KB, most of it JSON readings. The `graphite-pickle` payloads total about
80 KB, 68 KB of them in the one case that has to pass 64 KiB to get two `FRAME`s; the rest are a
few hundred bytes each. `prometheus-text` is about 220 KB: 40 KB of cases and 180 KB of readings,
49 KB of them the recorded node_exporter body's.
