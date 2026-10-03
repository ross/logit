---
created: 2026-10-03
updated: 2026-10-03
---

# Release profile: fat LTO and one codegen unit, measured against thin LTO and 16 units

## Status
Accepted

## Context
The root `Cargo.toml`'s `[profile.release]` sets `lto = true` (fat) and `codegen-units = 1`. That
makes it the slowest build in the repo:
- On a 24-core dev machine, a cold release build takes 146 s.
- 87 s of that is the single-threaded fat-LTO link of `logit-cli`.

The release profile builds the binary that `script/image`, `script/vm build`, and every
`script/perf` run measure. It builds nothing in the edit-and-test loop, which uses the dev
profile.

The obvious way to speed it up is thin LTO, more codegen units, or both. Four arms were measured
on the disposable perf VM ([ADR `disposable-azure-perf-vm`](disposable-azure-perf-vm.md)):
- **Machine:** `Standard_F8as_v6`, 8 cores, AMD EPYC 9V74, rustc 1.98.1.
- **Source:** every arm was built from `main` at `d1521c5f`, changing only the two profile keys.
  The result files record `ccd76db2535e` instead, because that's the harness checkout that ran
  them, not the source of the binaries.
- **Suite:** the full `logit-perf` suite, 22 scenarios, 3 repeats per arm.
- **Noise floor:** about ±4% for most scenarios. As a drift control, the fat-LTO binary was run a
  second time. It moved `native-relay` by +3.5% and `udp-statsd` by −4.0% with no code change.
  `udp-statsd-packed` spreads wider, about ±7%.

Cold release build time on the VM:

| `lto` | `codegen-units` | Build time |
|---|---|---:|
| `true` (fat) | 1 | 156 s |
| `"thin"` | 1 | 89 s |
| `"thin"` | 16 | 70 s |
| `true` (fat) | 16 | 157 s |

These are wall times around a container run. Cargo's own totals are 7–8 s lower in each case.

### Full suite, CPU per event against fat LTO with one unit

| Scenario | Thin, 1 unit | Thin, 16 units | Fat, 16 units |
|---|---:|---:|---:|
| `native-relay` | +7.9% | +12.1% | +8.0% |
| `aggregate` | −3.2% | +10.0% | +10.5% |
| `aggregate-groups` | +3.7% | −2.0% | −1.9% |
| `json-parse-x3` | −9.7% | −12.5% | −2.3% |
| other `json-parse*` | −2.1% to −3.2% | −6.6% to −11.1% | −1.3% to −5.8% |
| `udp-statsd` drop rate | −1.9 pts | +9.6 pts | +7.2 pts |
| `udp-statsd-packed` drop rate | −3.5 pts | +8.4 pts | +6.5 pts |

The table separates two effects:
- **16 codegen units** cost `aggregate` 10% and raise UDP statsd drops by 6.5–9.6 points, under
  thin and fat LTO alike.
- **Thin LTO with one unit** doesn't have those costs. It's the only real alternative.

### Interleaved rerun: thin LTO against fat LTO, one unit each
This rerun covered 5 scenarios, one repeat per binary per round, over 6 rounds. The order
alternated each round. The figures are medians of the six rounds.

| Scenario | Thin LTO against fat LTO | Rounds that agree |
|---|---|---|
| `native-relay` | +9% CPU per event, −11% events/s, +10% peak RSS | 5/6 for CPU, 6/6 for the rest |
| `aggregate-groups` | +7% CPU per event, −3.5% events/s | 6/6 |
| `logfmt-parse` | +2% CPU per event | 6/6 |
| `json-parse-x3` | −8% CPU per event | 6/6 |
| `udp-statsd` | −4% CPU per event (inside the noise floor); drop rate 3.2% → 0.4% | 6/6 |

`native-relay` regresses in every arm that moves off fat LTO with one unit, and it's the most
robust result here.

Two results are less robust:
- `aggregate-groups` regresses only in the thin-LTO, one-unit binary, and improves with 16 units.
- `json-parse-x3` and `logfmt-parse` move in opposite directions.

Both patterns match the code-layout sensitivity that `docs/design/performance.md` §1 ("What moved
since 2026-09-28") records for `json-parse-x3` and `logfmt-parse`. It's tracked in
[`docs/known-gaps/transforms.md`](../known-gaps/transforms.md#http-access-logs-nginx-haproxy-and-http_access). The UDP statsd drop-rate
improvement is robust: the ranges don't overlap, and it held in 6 of 6 rounds.

## Decision
`[profile.release]` stays `lto = true`, `codegen-units = 1`.

16 codegen units are out under either LTO mode:
- With fat LTO, they build no faster.
- With either mode, they cost `aggregate`, `native-relay`, and UDP statsd intake.

Thin LTO with one unit is a trade, not a win:
- **For:** 67 s off the build, a lower UDP statsd drop rate, and a cheaper `json-parse-x3`.
- **Against:** the native hop is 9% more CPU per event, 11% fewer events per second, and 10% more
  peak RSS, and `aggregate-groups` costs 7% more.

The native hop carries every event in a split-collection topology. The build runs only for
`script/image`, VM sessions, and perf runs. So the native hop decides it.

## Alternatives considered
- **Thin LTO, one codegen unit.** Builds in 89 s instead of 156 s. It loses on `native-relay` and
  `aggregate-groups`, and wins on UDP statsd drop rate and `json-parse-x3`. The trade is described
  under Decision.
- **Thin LTO, 16 codegen units.** The fastest build, at 70 s. It regresses `native-relay` by 12%
  and `aggregate` by 10%, and drops 8–10 points more UDP statsd events.
- **Fat LTO, 16 codegen units.** No faster to build than today, because the fat-LTO link
  dominates. It regresses `native-relay` by 8% and `aggregate` by 10%, and drops 7 points more
  UDP statsd events.

## Consequences
- A cold release build stays about 2.5 minutes, most of it one single-threaded link. Dev and test
  builds aren't affected.
- **Follow-up:** with one codegen unit, thin LTO drops UDP statsd events at about an eighth of
  fat LTO's rate. The two builds differ only in how LTO inlines and lays out code across crates, so
  that's the likely cause. A perf study of the `udp-statsd` intake path under both
  builds might recover that rate without giving up the native hop. The `json-parse-x3` difference
  is the known layout trade above, and it doesn't need a separate study.
- Don't rerun this comparison unless the toolchain or a major dependency changes enough to move
  it. To rerun it, build each arm with `script/vm build` and compare the arms with
  `logit-perf compare`. If an arm comes within a few percent of fat LTO, follow up with an
  interleaved rerun: the three-repeat suite alone can't separate effects that small from the
  drift control.
